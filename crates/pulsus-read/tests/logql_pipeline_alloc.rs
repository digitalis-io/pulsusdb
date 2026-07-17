//! Allocation-regression guard for the LogQL per-line evaluator (issue
//! #72 review rounds 1–2: the "allocation-lean hot path" plan AC gets a
//! MECHANICAL in-tree gate, not prose). A counting global allocator —
//! scoped to this one test binary — pins allocations-per-row bounds for
//! the evaluator fast paths (`run_into` with a reused scratch, the exact
//! shape `exec::run_pipeline_rows` drives) and for the transform/fan-out
//! assembly end to end.
//!
//! **Deterministic, scale-invariant bounds** (no wall-time asserts):
//! allocation counts depend only on the code under test and the pinned
//! allocator/regex/serde versions in `Cargo.lock`. Everything runs in
//! the SINGLE `#[test]` below so no parallel test thread can pollute the
//! counter; the "zero per row" gates allow a fixed sub-per-mille residue
//! rather than exact zero so an incidental harness-thread allocation can
//! never flake the build while a real per-row regression (>= 1 per row)
//! still fails by three orders of magnitude.

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

use std::borrow::Cow;

use pulsus_read::logql::exec::run_pipeline_rows;
use pulsus_read::logql::pipeline::CompiledPipeline;
use pulsus_read::logql::rows::{SampleRow, StreamMetaRow};

const ROWS: u64 = 20_000;
/// The "zero per row" residue budget: 20 stray allocations over 20k rows
/// (0.001/row) — three orders of magnitude under a real 1-per-row
/// regression, immune to one-off harness noise.
const ZERO_RESIDUE: u64 = 20;

fn compiled(query: &str) -> CompiledPipeline {
    let expr = pulsus_logql::parse(query).expect("parse");
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("expected a log query: {query}");
    };
    CompiledPipeline::compile(&log.pipeline).expect("compile")
}

/// Counts allocations across `ROWS` `run_into` evaluations with one
/// reused scratch — the `run_pipeline_rows` hot-loop shape.
fn count_run_into(query: &str, bodies: &[String], base: &[(String, String)]) -> u64 {
    let pipeline = compiled(query);
    let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
    // Warm-up: scratch capacity, allocator internals.
    for body in bodies {
        let out = pipeline.run_into(body, base, &mut scratch);
        std::hint::black_box(&out);
    }
    let start = ALLOCS.load(Ordering::Relaxed);
    for i in 0..ROWS {
        let body = &bodies[i as usize % bodies.len()];
        let out = pipeline.run_into(body, base, &mut scratch);
        std::hint::black_box(&out);
    }
    ALLOCS.load(Ordering::Relaxed) - start
}

/// Counts allocations across one full `run_pipeline_rows` assembly over
/// `rows` (pre-built OUTSIDE the window — fetching/decoding rows is the
/// ClickHouse client's cost, not the evaluator's).
fn count_assembly(
    query: &str,
    rows: &[SampleRow],
    meta: &std::collections::HashMap<u64, StreamMetaRow>,
) -> u64 {
    let pipeline = compiled(query);
    // Warm-up run (also proves the path is exercised).
    let warm = run_pipeline_rows(rows.to_vec(), &pipeline, meta, u32::MAX);
    assert!(!warm.is_empty(), "assembly fixture must produce output");
    let rows_clone = rows.to_vec(); // clone outside the window too
    let start = ALLOCS.load(Ordering::Relaxed);
    let out = run_pipeline_rows(rows_clone, &pipeline, meta, u32::MAX);
    let total = ALLOCS.load(Ordering::Relaxed) - start;
    std::hint::black_box(&out);
    total
}

/// One test on purpose: the counter is process-global, and a single test
/// per binary keeps every measurement window single-threaded.
#[test]
fn per_row_allocation_bounds_hold() {
    let base = vec![
        ("app".to_string(), "checkout".to_string()),
        ("env".to_string(), "prod".to_string()),
    ];
    let logfmt_bodies: Vec<String> = (0..64)
        .map(|i| {
            format!(
                "level=info took={}ms size={}kb msg=\"op {i}\"",
                100 + i,
                1 + i % 10
            )
        })
        .collect();
    let plain_bodies: Vec<String> = (0..64)
        .map(|i| format!("GET /api/items {} {}ms", 200 + i % 400, 100 + i))
        .collect();

    // --- Evaluator fast paths (`run_into` + reused scratch): ZERO
    // --- allocations per row.
    for (name, query, bodies) in [
        (
            "string label filter (drop path)",
            r#"{a="b"} | level = "error""#,
            &logfmt_bodies,
        ),
        (
            "logfmt + duration filter",
            r#"{a="b"} | logfmt | took > 250ms"#,
            &logfmt_bodies,
        ),
        (
            "pattern parser",
            r#"{a="b"} | pattern "<method> <path> <status> <took>""#,
            &plain_bodies,
        ),
    ] {
        let total = count_run_into(query, bodies, &base);
        assert!(
            total <= ZERO_RESIDUE,
            "{name}: {total} allocations over {ROWS} rows — the zero-per-row fast path regressed"
        );
    }

    // regexp: the regex crate's `captures()` allocates its capture-slot
    // storage internally — exactly one per matching row today. Pinned at
    // <= 1/row; a `CaptureLocations`-reuse pass may later drop it to 0.
    let total = count_run_into(
        r#"{a="b"} | regexp `^(?P<method>\w+) (?P<path>\S+) (?P<status>\d+)`"#,
        &plain_bodies,
        &base,
    );
    assert!(
        total <= ROWS + ZERO_RESIDUE,
        "regexp: {total} allocations over {ROWS} rows — must stay <= 1 per row"
    );

    // --- Assembly paths (`run_pipeline_rows` end to end). Output
    // --- materialization is inherent (owned `StreamResult`s), so these
    // --- pin small per-surviving-row constants, not zero.
    let meta = std::collections::HashMap::from([(
        1u64,
        StreamMetaRow {
            fingerprint: 1,
            service: "checkout".to_string(),
            labels: r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
        },
    )]);
    let assembly_rows: Vec<SampleRow> = (0..4096)
        .map(|i| SampleRow {
            fingerprint: 1,
            timestamp_ns: i as i64,
            body: logfmt_bodies[i % logfmt_bodies.len()].clone(),
        })
        .collect();

    // Transform path (drops/keeps, labels verbatim): the only per-
    // surviving-row allocation is the owned output line (+ amortized
    // entries growth). Bound: <= 2 per row.
    let n = assembly_rows.len() as u64;
    let total = count_assembly(
        r#"{a="b"} | line_format "L={{.env}} {{.service_name}}" |= "L=prod""#,
        &assembly_rows,
        &meta,
    );
    assert!(
        total <= 2 * n + ZERO_RESIDUE,
        "transform assembly: {total} allocations over {n} rows — must stay <= 2 per row"
    );

    // Fan-out path (logfmt parser regroups by final label set): per
    // surviving row exactly the canonical labels_json (the group key —
    // rendered straight from the sorted borrowed scratch, round-2
    // finding 1) + the owned output line + amortized group growth.
    // Bound: <= 4 per row.
    let total = count_assembly(r#"{a="b"} | logfmt"#, &assembly_rows, &meta);
    assert!(
        total <= 4 * n + ZERO_RESIDUE,
        "fan-out assembly: {total} allocations over {n} rows — must stay <= 4 per row"
    );

    // HIGH-CARDINALITY fan-out (review round 3): every row produces a
    // DISTINCT final label set, so the new-group path runs per row —
    // exactly the class where a per-new-group key clone becomes a
    // per-row allocation. Per row: labels_json (the map key, moved —
    // never cloned — into `StreamResult` at drain), the owned output
    // line, the group's `service` string, and its one-entry vec; map
    // growth amortized. Bound: <= 4.5 per row — the pre-fix
    // `e.key().clone()` shape measures ~5.0/row and fails this.
    let high_card_rows: Vec<SampleRow> = (0..4096)
        .map(|i| SampleRow {
            fingerprint: 1,
            timestamp_ns: i as i64,
            body: format!("id=r{i} level=info"),
        })
        .collect();
    let n = high_card_rows.len() as u64;
    let total = count_assembly(r#"{a="b"} | logfmt"#, &high_card_rows, &meta);
    assert!(
        total <= 4 * n + n / 2 + ZERO_RESIDUE,
        "high-cardinality fan-out assembly: {total} allocations over {n} rows — must stay \
         <= 4.5 per row (a per-new-group key clone would push this past 5)"
    );
}
