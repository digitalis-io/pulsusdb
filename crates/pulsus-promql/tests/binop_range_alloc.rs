//! Allocation-regression guard for the range vector-vector binop hot
//! path (issue #93, finding 1). Modeled exactly on
//! `crates/pulsus-read/tests/logql_pipeline_alloc.rs`: a counting global
//! allocator — scoped to this one test binary — pins an
//! allocations-per-(series × step) bound for a `group_right` range
//! query, the profiled heaviest binop shape.
//!
//! **Deterministic, scale-invariant bound** (no wall-time assert):
//! allocation counts depend only on the code under test and the pinned
//! toolchain, so the bound is expressed PER CELL (per output series ×
//! step). The pre-#93 code cloned the full `(Labels, Option<String>)`
//! output identity into the many-to-one duplicate-detection set every
//! matched pair — a full deep label clone per (step × many-side series)
//! that the committed profile
//! (`docs/benchmarks/metrics-read-path.md`) showed to be the single
//! largest allocation source. The fix keys the INNER output-identity set
//! on a 64-bit hash instead (upstream `matchedSigs` inner `metric.Hash()`),
//! while the OUTER signature key stays the collision-free full `MatchKey`,
//! cloned only per distinct signature (round-2 review). This gate fails if
//! the per-pair output-identity clone returns, and by orders of magnitude
//! if a per-step operand rebuild (re-indexing the whole operand each step)
//! or any super-linear regression lands.
//!
//! Everything runs in the SINGLE `#[test]` below so no parallel test
//! thread can pollute the process-global counter.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

struct CountingAlloc;

static ALLOCS: AtomicU64 = AtomicU64::new(0);

// SAFETY: delegates verbatim to the system allocator; the only side
// effect is a relaxed atomic increment, which allocates nothing and
// cannot re-enter the allocator.
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

use pulsus_promql::{
    FetchedSeries, Labels, PlanParams, QueryPlan, Sample, SeriesData, evaluate, parse, plan,
};

const GROUPS: usize = 4;
const MANY_PER_GROUP: usize = 8;
const STEPS: i64 = 100;
const STEP_MS: i64 = 15_000;

/// Builds the pinned `group_right` range shape: `GROUPS` one-side (`foo`)
/// series, `GROUPS × MANY_PER_GROUP` many-side (`bar`) series each with a
/// 3-label set, over `STEPS` steps.
fn fixture() -> (QueryPlan, SeriesData) {
    let params = PlanParams {
        start_ms: 0,
        end_ms: (STEPS - 1) * STEP_MS,
        step_ms: STEP_MS,
        lookback_ms: pulsus_promql::DEFAULT_LOOKBACK_MS,
        experimental_functions: false,
    };
    let expr = parse("foo / on(g) group_right bar").expect("parse");
    let qp = plan(&expr, params).expect("plan");
    let samples = |base: f64| -> Vec<Sample> {
        (0..STEPS)
            .map(|k| Sample::float(k * STEP_MS, base + k as f64))
            .collect()
    };
    let mut data = SeriesData::new();
    for sel in &qp.selectors {
        let name = sel.metric_name.clone().expect("concrete-name selector");
        let mut series = Vec::new();
        if name == "foo" {
            for g in 0..GROUPS {
                series.push(FetchedSeries {
                    fingerprint: g as u64,
                    metric_name: Some("foo".to_string()),
                    labels: Labels::new([("g".to_string(), format!("g{g}"))]),
                    samples: samples(1.0),
                });
            }
        } else {
            let mut fp = 100_000u64;
            for g in 0..GROUPS {
                for m in 0..MANY_PER_GROUP {
                    series.push(FetchedSeries {
                        fingerprint: fp,
                        metric_name: Some("bar".to_string()),
                        labels: Labels::new([
                            ("g".to_string(), format!("g{g}")),
                            ("inst".to_string(), format!("i{m}")),
                            ("region".to_string(), "us-east-1".to_string()),
                        ]),
                        samples: samples(2.0),
                    });
                    fp += 1;
                }
            }
        }
        data.insert(sel.id, series);
    }
    (qp, data)
}

#[test]
fn range_binop_allocations_per_cell_stay_bounded() {
    let (qp, data) = fixture();

    // Warm-up (allocator internals) + prove the shape is exercised.
    let (warm, _annotations) = evaluate(&qp, &data).expect("evaluate");
    let out_series = match &warm {
        pulsus_promql::QueryValue::Matrix(m) => m.len(),
        other => panic!("expected Matrix, got {other:?}"),
    };
    assert_eq!(
        out_series,
        GROUPS * MANY_PER_GROUP,
        "group_right emits one output series per many-side series"
    );

    let start = ALLOCS.load(Ordering::Relaxed);
    let out = evaluate(&qp, &data).expect("evaluate");
    let total = ALLOCS.load(Ordering::Relaxed) - start;
    std::hint::black_box(&out);

    let cells = (GROUPS * MANY_PER_GROUP) as u64 * STEPS as u64;

    // Post-#93 the range binop measures ~20.4 allocations/cell on this
    // fixture (collision-free `MatchKey` outer signature key + hashed inner
    // output identity). Bound at 25/cell: comfortably above the current
    // cost, yet BELOW the pre-fix ~30/cell (reintroducing the per-pair
    // full-identity clone into the dup-detection set fails here — verified
    // by reverting the fix), and orders of magnitude below any per-step
    // operand rebuild / super-linear regression.
    const BOUND_PER_CELL: u64 = 25;
    assert!(
        total <= BOUND_PER_CELL * cells,
        "range group_right binop: {total} allocations over {cells} (series × step) cells \
         = {:.2}/cell — the per-step allocation discipline regressed (bound {BOUND_PER_CELL}/cell)",
        total as f64 / cells as f64
    );
}
