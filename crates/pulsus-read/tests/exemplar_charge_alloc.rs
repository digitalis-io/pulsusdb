//! U3g (issue #501 part 2): the structures an exemplar answer keeps take no
//! more bytes than the `_held` halves of its rows' charges.
//!
//! A counting global allocator, scoped to this one test binary and counting
//! per thread, as `crates/pulsus-write/tests/otlp_metrics_parse_alloc.rs`
//! does. The measured window turns the rows into the answer — each row
//! cloned inside the window, so the clone's buffers come and go within it
//! and the bytes still live when it closes are the answer's own.
//!
//! Everything runs in the single `#[test]` below so no parallel test thread
//! can share the counter.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;

use pulsus_model::Fingerprint;
use pulsus_read::metrics::exemplars::{
    assemble, exemplar_row_held, keep_exemplar, keep_series, series_row_held,
};
use pulsus_read::metrics::rows::{ExemplarRow, ExemplarSeriesRow};

struct CountingAlloc;

// Per thread, `const`-initialised and written through `try_with`, for the
// reasons `otlp_metrics_parse_alloc.rs` gives: a process-wide figure would
// charge other threads' allocations to the window, and the slot is gone
// during thread-local teardown.
thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static LIVE: Cell<i64> = const { Cell::new(0) };
}

fn add_live(bytes: i64) {
    let on = COUNTING.try_with(Cell::get).unwrap_or(false);
    if on {
        let _ = LIVE.try_with(|c| c.set(c.get() + bytes));
    }
}

// SAFETY: delegates verbatim to the system allocator; the only side effect
// is a thread-local integer update, which allocates nothing and cannot
// re-enter the allocator.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        add_live(layout.size() as i64);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        add_live(-(layout.size() as i64));
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        add_live(layout.size() as i64);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        add_live(new_size as i64 - layout.size() as i64);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// The bytes still live when `f` returns, its result included.
fn live_after<T>(f: impl FnOnce() -> T) -> (T, i64) {
    LIVE.with(|c| c.set(0));
    COUNTING.with(|c| c.set(true));
    let out = black_box(f());
    COUNTING.with(|c| c.set(false));
    (out, LIVE.with(Cell::get))
}

fn controls(n: u32) -> String {
    (0..n)
        .map(|i| char::from_u32(1 + i % 31).expect("a control character"))
        .collect()
}

fn row(fp: u128, ms: i64, labels: &str) -> ExemplarRow {
    ExemplarRow {
        fingerprint: Fingerprint::from_raw(fp),
        unix_milli: ms,
        value: -5e-324,
        trace_id: [0xab; 16],
        span_id: [0xcd; 8],
        labels: labels.to_string(),
    }
}

fn series(fp: u128, name: &str, labels: &str) -> ExemplarSeriesRow {
    ExemplarSeriesRow {
        fingerprint: Fingerprint::from_raw(fp),
        series_name: name.to_string(),
        series_labels: labels.to_string(),
    }
}

/// The bytes the answer for `rows` and `series` keeps, against the sum of
/// their `_held` halves.
fn check(case: &str, rows: &[ExemplarRow], series: &[ExemplarSeriesRow]) {
    let held: usize = rows.iter().map(exemplar_row_held).sum::<usize>()
        + series.iter().map(series_row_held).sum::<usize>();
    let (answer, live) = live_after(|| {
        assemble(
            rows.iter().cloned().map(keep_exemplar).collect(),
            series.iter().cloned().map(keep_series).collect(),
        )
    });
    assert_eq!(
        answer.iter().map(|s| s.exemplars.len()).sum::<usize>(),
        rows.len(),
        "{case}: every row is in the answer"
    );
    assert!(
        live > 0 && live as usize <= held,
        "{case}: the answer keeps {live} bytes against a held charge of {held}"
    );
    drop(answer);
}

#[test]
fn the_answer_never_allocates_more_than_its_held_charge() {
    // U3f's rows: labels `{}` with both ids, and labels of 1,000 control
    // characters; a series named with 100 control characters, and one
    // with the wide labels.
    let wide = serde_json::to_string(&serde_json::json!({ "k": controls(1_000) }))
        .expect("canonical labels");
    let rows: Vec<ExemplarRow> = (0..8)
        .map(|i| row(1 + (i % 2) as u128, i, if i < 4 { "{}" } else { &wide }))
        .collect();
    let named = vec![series(1, &controls(100), "{}"), series(2, "m", &wide)];
    check("U3f's rows", &rows, &named);

    // 1,000 rows of 30-pair label sets over 10 series of 30 pairs.
    let thirty = |prefix: &str| -> String {
        let map: serde_json::Map<String, serde_json::Value> = (0..30)
            .map(|k| {
                (
                    format!("{prefix}{k:02}"),
                    serde_json::Value::String(format!("v{k}")),
                )
            })
            .collect();
        serde_json::to_string(&serde_json::Value::Object(map)).expect("canonical labels")
    };
    let rows: Vec<ExemplarRow> = (0..1_000)
        .map(|i| row(1 + (i % 10) as u128, i, &thirty("e")))
        .collect();
    let many: Vec<ExemplarSeriesRow> = (1..=10).map(|fp| series(fp, "m", &thirty("s"))).collect();
    check("1,000 rows of 30 pairs", &rows, &many);
}
