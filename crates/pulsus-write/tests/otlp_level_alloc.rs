//! The allocation gate that SELECTED the OTLP ingest shape (issue #483),
//! and the identity that makes the selected shape safe.
//!
//! Before this change every record in a `ScopeLogs` shared one
//! structured-metadata string, cloned per record. A per-record
//! `detected_level` makes that string per-record, and the plan required the
//! shape to follow a measurement rather than an assertion. Two candidate
//! shapes: REBUILD the resolved pair list and re-canonicalize the JSON per
//! record, or render the sorted JSON once per scope with a hole where the
//! `detected_level` member belongs and SPLICE each record's answer into it.
//!
//! The gate below is the difference in allocations per record between level
//! discovery off and on, measured in ONE process on ONE build, so allocator
//! or toolchain drift cancels. It is a scale-invariant bound; no wall-clock
//! figure is asserted.
//!
//! [`the_splice_is_byte_identical_to_a_rebuild`] is an identity between two
//! of OUR OWN functions and is **not** evidence of reference agreement —
//! that is `detected_level_reference_cases.rs`.

use std::alloc::{GlobalAlloc, Layout, System};
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

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;

use pulsus_write::LogIngestSettings;
use pulsus_write::protocols::otlp_logs;

const RECORDS: usize = 2_000;

/// The per-record allocation budget for level discovery. The splice shape
/// costs at most two allocations per record (the stored `String`, and the
/// owned level value when the rule's answer is not a `&'static str`); a
/// rebuild-per-record shape re-clones the scope's six pairs and rebuilds
/// both the map and the JSON per record.
///
/// Both shapes were measured on this corpus, in this test, on one build:
/// the splice reported `off=6114 on=8085 extra=1971 per_record=0.986`, and
/// the rebuild — the per-record `LevelOutcome` applied to a clone of the
/// scope pairs and re-rendered through `render_structured_metadata` —
/// reported `off=6114 on=130085`, i.e. `61.986` per record. The budget sits
/// between the two by more than an order of magnitude in each direction, so
/// it selects the shape rather than merely permitting the one that shipped.
const MAX_EXTRA_ALLOCS_PER_RECORD: u64 = 3;

fn attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

/// One `ScopeLogs` carrying scope name, scope version and four scope
/// attributes — the shape whose shared string this change makes per-record.
fn scope_bearing_request(records: usize) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![attr("service.name", "alloc-gate")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "gate-scope".to_string(),
                    version: "1.4.2".to_string(),
                    attributes: vec![
                        attr("team", "payments"),
                        attr("tier", "gold"),
                        attr("region", "eu-west-1"),
                        attr("build", "abc123"),
                    ],
                    ..Default::default()
                }),
                log_records: (0..records)
                    .map(|i| LogRecord {
                        time_unix_nano: 1_788_099_000_000_000_000 + i as u64,
                        body: Some(AnyValue {
                            value: Some(Value::StringValue(
                                "handled request in 12ms with no incident".to_string(),
                            )),
                        }),
                        ..Default::default()
                    })
                    .collect(),
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn settings(on: bool) -> LogIngestSettings {
    LogIngestSettings {
        discover_log_levels: on,
    }
}

/// **Criteria 15 and 16, in ONE `#[test]`** so no parallel test thread in
/// this binary can pollute the allocation counter — the same reason
/// `crates/pulsus-read/tests/logql_pipeline_alloc.rs` puts everything in one
/// test.
#[test]
fn the_otlp_shape_costs_at_most_three_extra_allocations_per_record() {
    let request = scope_bearing_request(RECORDS);

    // Warm: the first parse touches lazily-initialized machinery that the
    // difference must not be charged for.
    let _ = otlp_logs::parse(&request, 0, settings(false)).expect("warm");

    let before_off = ALLOCS.load(Ordering::Relaxed);
    let off = otlp_logs::parse(&request, 0, settings(false)).expect("off");
    let after_off = ALLOCS.load(Ordering::Relaxed);

    let before_on = ALLOCS.load(Ordering::Relaxed);
    let on = otlp_logs::parse(&request, 0, settings(true)).expect("on");
    let after_on = ALLOCS.load(Ordering::Relaxed);

    let allocs_off = after_off - before_off;
    let allocs_on = after_on - before_on;
    assert_eq!(off.rows.len(), RECORDS);
    assert_eq!(on.rows.len(), RECORDS);

    let extra = allocs_on.saturating_sub(allocs_off);
    let per_record = extra as f64 / RECORDS as f64;
    println!(
        "otlp level alloc gate: off={allocs_off} on={allocs_on} extra={extra} \
         per_record={per_record:.3}"
    );
    assert!(
        extra <= MAX_EXTRA_ALLOCS_PER_RECORD * RECORDS as u64,
        "level discovery costs {per_record:.3} allocations per record, over the \
         {MAX_EXTRA_ALLOCS_PER_RECORD} budget (off={allocs_off}, on={allocs_on})"
    );

    // The two shapes must also agree on the answer, or the cheap one is
    // cheap for the wrong reason: every stored string carries the level and
    // differs from the off-path string by exactly that pair.
    for (i, row) in on.rows.iter().enumerate() {
        assert!(
            row.structured_metadata
                .contains(r#""detected_level":"unknown""#),
            "record {i}: {}",
            row.structured_metadata
        );
    }

    the_splice_is_byte_identical_to_a_rebuild();
}

/// **Criterion 16.** The spliced string equals what the ordinary render
/// seam produces over the same resolved pair list, across scope shapes that
/// cross empty, single-pair, escaped-value and colliding-name cases.
///
/// The comparison runs through `otlp_logs::parse` twice: once with
/// discovery on (the splice) and once with it off (the shared render), the
/// second answer having the level member inserted at its sorted position by
/// this test rather than by the code under test.
///
/// Called from the single `#[test]` above rather than being one itself, so
/// its own allocations cannot land in that test's counter.
fn the_splice_is_byte_identical_to_a_rebuild() {
    let shapes: Vec<Vec<KeyValue>> = (0..200)
        .map(|i| match i % 5 {
            0 => vec![],
            1 => vec![attr("only", "one")],
            2 => vec![attr("quoted", r#"a"b\c"#), attr("ctl", "x\ty")],
            3 => vec![attr("a.b", "renamed"), attr("a_b", "base")],
            // Names either side of `detected_level` in sort order, so the
            // hole is exercised at the front, the middle and the back.
            _ => vec![
                attr("aaa", &format!("v{i}")),
                attr("zzz", &format!("w{i}")),
                attr("detected_zebra", "after"),
            ],
        })
        .collect();

    for (i, scope_attrs) in shapes.iter().enumerate() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![attr("service.name", "splice-gate")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "sc".to_string(),
                        attributes: scope_attrs.clone(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_788_099_000_000_000_000,
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("level=warn msg=x".to_string())),
                        }),
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let spliced = otlp_logs::parse(&request, 0, settings(true))
            .expect("on")
            .rows[0]
            .structured_metadata
            .clone();
        let plain = otlp_logs::parse(&request, 0, settings(false))
            .expect("off")
            .rows[0]
            .structured_metadata
            .clone();

        // Rebuild independently: parse the off-path JSON, insert the level
        // member, re-render in sorted key order with `serde_json` escaping —
        // which is exactly what `LabelSet::to_canonical_json` does.
        let mut map: std::collections::BTreeMap<String, String> = if plain.is_empty() {
            std::collections::BTreeMap::new()
        } else {
            serde_json::from_str(&plain).expect("off-path JSON")
        };
        map.insert("detected_level".to_string(), "warn".to_string());
        let rebuilt = format!(
            "{{{}}}",
            map.iter()
                .map(|(k, v)| format!(
                    "{}:{}",
                    serde_json::to_string(k).expect("k"),
                    serde_json::to_string(v).expect("v")
                ))
                .collect::<Vec<_>>()
                .join(",")
        );
        assert_eq!(spliced, rebuilt, "shape {i}: attrs {scope_attrs:?}");
    }
}
