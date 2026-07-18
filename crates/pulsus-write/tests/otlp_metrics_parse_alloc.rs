//! Allocation-regression guard for `otlp_metrics::parse` (issue #62),
//! modeled on `crates/pulsus-promql/tests/binop_range_alloc.rs`: a counting
//! global allocator — scoped to this one test binary — pins two
//! deterministic, scale-invariant bounds (no wall-time assert; allocation
//! counts depend only on the code under test and the pinned toolchain).
//!
//! This gate is the regression safeguard for the standing owner
//! query/ingest-performance directive, not a design-doc conformance check.
//! It fails if the expansion budget adds a per-sample hot-path allocation,
//! if an OVER-clone of the base pairs returns, or — its second arm — if a
//! pathological over-budget fan-out is materialized past its abort prefix
//! instead of aborting charge-before-allocate.
//!
//! Everything runs in the SINGLE `#[test]` below so no parallel test thread
//! can pollute the process-global counter.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};

struct CountingAlloc;

static ALLOCS: AtomicU64 = AtomicU64::new(0);

// SAFETY: delegates verbatim to the system allocator; the only side effect
// is a relaxed atomic increment, which allocates nothing and cannot
// re-enter the allocator.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;

use pulsus_write::error::LogsIngestError;
use pulsus_write::protocols::otlp_metrics::{MAX_EXPANDED_BYTES, parse};

fn kv(key: &str, value: String) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value)),
        }),
        key_strindex: 0,
    }
}

fn number_dp(value: f64, attributes: Vec<KeyValue>) -> NumberDataPoint {
    NumberDataPoint {
        attributes,
        start_time_unix_nano: 0,
        time_unix_nano: 1,
        exemplars: vec![],
        flags: 0,
        value: Some(number_data_point::Value::AsDouble(value)),
    }
}

fn gauge_request(
    resource: Resource,
    data_points: Vec<NumberDataPoint>,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(resource),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "cpu_usage".to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge { data_points })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Valid fixture (constant C): one gauge, base = 4 resource attrs, `N`
/// data points each with 2 distinct string attrs ⇒ `N` distinct
/// series/samples.
const N_VALID: usize = 1000;

#[test]
fn otlp_metrics_parse_allocations_stay_bounded() {
    // -- (a) valid request: allocations/sample ceiling (constant C) -------
    let resource = Resource {
        attributes: (0..4)
            .map(|i| kv(&format!("res.attr.{i}"), format!("resval{i}")))
            .collect(),
        dropped_attributes_count: 0,
        entity_refs: vec![],
    };
    let data_points: Vec<NumberDataPoint> = (0..N_VALID)
        .map(|i| {
            number_dp(
                i as f64,
                vec![kv("host", format!("h{i}")), kv("pid", format!("p{i}"))],
            )
        })
        .collect();
    let valid_req = gauge_request(resource, data_points);

    // Warm-up (allocator internals) + prove the shape is exercised.
    let warm = parse(&valid_req, 0).expect("valid request within budget");
    assert_eq!(warm.samples.len(), N_VALID, "one sample per data point");
    assert_eq!(
        warm.series.len(),
        N_VALID,
        "each data point is a distinct series"
    );
    drop(warm);

    let start = ALLOCS.load(Ordering::Relaxed);
    let out = parse(&valid_req, 0).expect("valid request within budget");
    let valid_allocs = ALLOCS.load(Ordering::Relaxed) - start;
    black_box(&out);

    // Post-#62 the valid path measures ~41.0 allocations/sample on this
    // fixture and toolchain (4 base pairs + 2 dp-attr pairs cloned into an
    // owned `LabelSet`, its normalized `BTreeMap`, plus `SeriesRef`/
    // `MetricPoint`) — the required-distinct-series-count ceiling of the
    // owned-label model (interning is deferred). Bound at 46/sample (~10%
    // headroom): above the measured cost, yet an added per-sample base
    // OVER-clone or any super-linear regression blows past it. The
    // expansion-budget charge itself is allocation-free (wire-length
    // arithmetic only), so it does not move this number.
    const C: u64 = 46;
    assert!(
        valid_allocs <= C * N_VALID as u64,
        "otlp_metrics::parse: {valid_allocs} allocations over {N_VALID} samples = {:.2}/sample \
         — the per-sample allocation discipline regressed (bound {C}/sample)",
        valid_allocs as f64 / N_VALID as f64
    );

    // -- (b) over-budget request: abort before mass materialization (B) ---
    const MIB: usize = 1024 * 1024;
    let big_resource = Resource {
        attributes: vec![kv("big.attr", "v".repeat(MIB))],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    };
    // 100k data points, each cloning the ~1 MiB base — the per-sample
    // charge (~1 MiB) trips the 256 MiB budget after ≈ 256 samples, so
    // parse must abort having materialized hundreds of samples, not 100k
    // (whose full output would be ≈ 100 GiB).
    let big_data_points: Vec<NumberDataPoint> = (0..100_000)
        .map(|i| number_dp(i as f64, vec![kv("i", format!("{i}"))]))
        .collect();
    let over_budget_req = gauge_request(big_resource, big_data_points);

    let start = ALLOCS.load(Ordering::Relaxed);
    let err = parse(&over_budget_req, 0).expect_err("over-budget request must abort");
    let abort_allocs = ALLOCS.load(Ordering::Relaxed) - start;

    assert!(
        matches!(
            err,
            LogsIngestError::OversizeMessage { limit, actual, .. }
                if limit == MAX_EXPANDED_BYTES && actual > MAX_EXPANDED_BYTES
        ),
        "unexpected error: {err}"
    );

    // The abort materializes ≈ 250 samples before tripping; each clones the
    // ~1 MiB base pair and builds a small `LabelSet` (a handful of allocs),
    // so the whole aborted parse measures ~4.1k allocations on this
    // toolchain. Bound at 5000 (~20% headroom) — comfortably above the
    // abort prefix, yet ORDERS of magnitude below the ~100k-sample full
    // materialization the budget prevents (which would be >= 100k
    // allocations). This is the charge-before-allocate proof: materialization
    // stops at hundreds, not 100k.
    const B: u64 = 5000;
    assert!(
        abort_allocs <= B,
        "over-budget parse made {abort_allocs} allocations — it must abort within its ~256-sample \
         prefix (bound {B}), not materialize all 100k data points"
    );
}
