//! Issue #241 wave 3 (formerly #265) — the `| json` per-ROW expansion
//! factor, measured against a rule fixed BEFORE the measurement.
//!
//! # What is left uncharged, and why the question is a measurement
//!
//! `MAX_JSON_FLATTEN_KEY_BYTES` already covers the flatten's key strings
//! and all four containers of the json-path capture; `pipeline.rs`'s
//! `JsonPaths` doc names what it does **not** cover, in the code's own
//! words: *"the row's label vector spine and the parsed
//! `serde_json::Value`"*. That residual is **per-ROW, not
//! query-lifetime** — one row is live at a time on this path — so the
//! only question it raises is how far one row's live heap can exceed the
//! body that produced it. Reading cannot answer that; it is a property
//! of `serde_json`'s `Value` representation and of the label spine's
//! growth. So it is measured.
//!
//! # THE PRE-COMMITTED RULE
//!
//! Fixed before the first measurement was taken (the #16/#34/#35 bar),
//! and applied below exactly as written:
//!
//! ```text
//! Ladder:    body = 1 KiB, 16 KiB, 256 KiB, 4 MiB  (one JSON object, m leaves, fixed shape)
//! Query:     {app="a"} | json     (the QUERY-path flatten; NOT /detected_fields' capture mode)
//! Statistic: f(B) = peak_window_bytes / B, 5 reps, warmup discarded, per-rep values reported
//! Decide:    F = max over the ladder of the median f(B).
//!            F * MAX_DECOMPRESSED_BYTES <= MAX_JSON_FLATTEN_KEY_BYTES
//!                -> RECORD the inequality here and charge NOTHING
//!            otherwise
//!                -> charge the residual, subject to the cost rule
//! Cost rule: any charge added is O(1) per emitted LABEL, never per body byte and never per
//!            point; A/B'd against tests/logql_pipeline_alloc.rs and
//!            tests/multi_metric_scan_alloc.rs with both suites unmoved.
//! ```
//!
//! `MAX_DECOMPRESSED_BYTES` is the ingest-side ceiling on a decompressed
//! push body (`crates/pulsus-write/src/ingest/decompress.rs:16`), i.e. an
//! upper bound on `B` for any line that can be stored, and
//! `MAX_JSON_FLATTEN_KEY_BYTES` is the per-row ledger the charged half
//! already runs on. Both are 64 MiB today, so the record-nothing branch
//! requires `F <= 1`.
//!
//! **The outcome is written into `the_measured_expansion_factor_is_recorded`'s
//! own output and asserted there, not summarised here** — a number in
//! prose beside a measurement is the thing that goes stale.
//!
//! # Scope, and what this does NOT establish
//!
//! * one shape only: a FLAT object of `m` equal-sized string leaves.
//!   Depth, arrays and mixed value types are not on the ladder; a deeper
//!   document expands differently and this file does not claim otherwise.
//! * the measurement is per ROW. It says nothing about a query's total,
//!   which is `MAX_JSON_FLATTEN_KEY_BYTES` per row times however many
//!   rows a scan feeds, one at a time.
//! * the instrument is the cohort window carried from
//!   `tests/logql_post_agg_witness.rs:83-324` (see that file for the
//!   attribution rule). It is duplicated here, again, because a
//!   `#[global_allocator]` is per-test-binary. **A defect fixed in one
//!   copy exists in the others.**
//!
//! # A read-path performance finding this ladder surfaced — since FIXED
//!
//! Kept as the record of how it was caught. As first measured here, the
//! wall times were quadratic in the emitted label count, and the
//! `LADDER-WALL` line proved it was not the instrument: one
//! UNINSTRUMENTED `run_into` over a flat `| json` body cost 60-116 us at
//! 977 B, 0.81-1.12 ms at 16 105 B, 127-196 ms at 257 909 B and
//! **34-42 s** at 4 126 651 B. Each of the top two steps is 16x the input
//! for over 100x the time — 113-242x then 183-331x, taking the ranges at
//! their most and least favourable ends, so the superlinearity holds
//! everywhere in them and is not an artefact of pairing two lucky runs.
//! The source
//! agreed: the flatten resolved each emitted key with
//! `labels.iter().position(..)`, a linear scan of the label vector, once
//! per leaf, so a row emitting `m` labels cost `O(m^2)` comparisons.
//! Flagged and not fixed at the time: it is a LATENCY property and issue
//! #241 is about bytes.
//!
//! Issue #447 fixed it — `pipeline.rs`'s `LabelIndex`, a positions-only
//! open-addressed table built once per flatten. The same uninstrumented
//! `LADDER-WALL` line now reads, across the same four rungs,
//! 76-310 us / 0.38-1.07 ms / 5.8-21.4 ms / **0.11-0.23 s**.
//!
//! **Every wall time in this file is a RANGE, and must stay one.** The
//! measuring machine is shared: several builds run on it at once, so a
//! single figure is one draw from a spread of roughly 2x, and quoting one
//! is how a lucky run becomes a claim. The ranges above are the observed
//! min and max over 60 runs of `zz_the_full_precommitted_ladder`, one
//! process per run; the pre-#447 range below is over 4. Both are wide,
//! two orders of magnitude apart, and it is the gap that is the finding —
//! not any value inside either range.
//!
//! The fix did move the byte ratio this file measures, by the index
//! table's doubling transient: `F` went 3.583 -> 3.934. That number is
//! NOT a wall time and NOT a range, and the reason it is exempt from the
//! rule above is that `apply_the_rule` asserts it by EQUALITY against
//! `F_MILLI` — likewise `264 006 270 B` against `WORST_CASE_BYTES`. They
//! are deterministic allocation counts under a pinned toolchain, so an
//! exact gate is available for them and a stale figure here fails the
//! test rather than sitting in prose. The rule's branch selection and the
//! linearity bounds are asserted as well, and all of them still hold.

use std::alloc::{GlobalAlloc, Layout, System};
use std::borrow::Cow;
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use pulsus_read::logql::MAX_JSON_FLATTEN_KEY_BYTES;
use pulsus_read::logql::pipeline::CompiledPipeline;
use pulsus_write::ingest::decompress::MAX_DECOMPRESSED_BYTES;

// =====================================================================
// 1. The instrument (carried from tests/logql_post_agg_witness.rs)
// =====================================================================

const COHORT_SLOTS: usize = 1 << 21;
const COHORT_MASK: usize = COHORT_SLOTS - 1;
const PROBE_BOUND: usize = 128;

static SLOT_KEY: [AtomicUsize; COHORT_SLOTS] = [const { AtomicUsize::new(0) }; COHORT_SLOTS];
static SLOT_SIZE: [AtomicU64; COHORT_SLOTS] = [const { AtomicU64::new(0) }; COHORT_SLOTS];
static SLOT_GEN: [AtomicU64; COHORT_SLOTS] = [const { AtomicU64::new(0) }; COHORT_SLOTS];
static GENERATION: AtomicU64 = AtomicU64::new(1);
static OVERFLOW: AtomicBool = AtomicBool::new(false);

static W_LIVE: AtomicU64 = AtomicU64::new(0);
static W_PEAK: AtomicU64 = AtomicU64::new(0);
static W_COUNT: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static IN_WINDOW: Cell<bool> = const { Cell::new(false) };
}

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn in_window() -> bool {
    IN_WINDOW.try_with(Cell::get).unwrap_or(false)
}

fn slot_of(ptr: usize) -> usize {
    ((ptr >> 4).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 44) & COHORT_MASK
}

fn table_insert(ptr: usize, size: u64) {
    let epoch = GENERATION.load(Ordering::Relaxed);
    let mut i = slot_of(ptr);
    for _ in 0..PROBE_BOUND {
        let occupied = SLOT_GEN[i].load(Ordering::Relaxed) == epoch
            && SLOT_KEY[i].load(Ordering::Relaxed) != 0;
        if !occupied || SLOT_KEY[i].load(Ordering::Relaxed) == ptr {
            SLOT_KEY[i].store(ptr, Ordering::Relaxed);
            SLOT_SIZE[i].store(size, Ordering::Relaxed);
            SLOT_GEN[i].store(epoch, Ordering::Relaxed);
            return;
        }
        i = (i + 1) & COHORT_MASK;
    }
    OVERFLOW.store(true, Ordering::Relaxed);
}

fn table_remove(ptr: usize) -> Option<u64> {
    let epoch = GENERATION.load(Ordering::Relaxed);
    let mut i = slot_of(ptr);
    for _ in 0..PROBE_BOUND {
        if SLOT_GEN[i].load(Ordering::Relaxed) != epoch || SLOT_KEY[i].load(Ordering::Relaxed) == 0
        {
            return None;
        }
        if SLOT_KEY[i].load(Ordering::Relaxed) == ptr {
            let size = SLOT_SIZE[i].load(Ordering::Relaxed);
            SLOT_KEY[i].store(0, Ordering::Relaxed);
            let mut j = (i + 1) & COHORT_MASK;
            while SLOT_GEN[j].load(Ordering::Relaxed) == epoch
                && SLOT_KEY[j].load(Ordering::Relaxed) != 0
            {
                let p = SLOT_KEY[j].load(Ordering::Relaxed);
                let s = SLOT_SIZE[j].load(Ordering::Relaxed);
                SLOT_KEY[j].store(0, Ordering::Relaxed);
                table_insert(p, s);
                j = (j + 1) & COHORT_MASK;
            }
            return Some(size);
        }
        i = (i + 1) & COHORT_MASK;
    }
    None
}

struct CohortAlloc;

// SAFETY: every method delegates verbatim to the system allocator and the
// bookkeeping runs only after the underlying call has produced (or is
// about to release) the pointer. The bookkeeping touches nothing but
// `static` atomics and a `Drop`-free, `const`-initialised thread-local,
// so it cannot re-enter the allocator.
unsafe impl GlobalAlloc for CohortAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() && in_window() {
            table_insert(p as usize, layout.size() as u64);
            W_COUNT.fetch_add(1, Ordering::Relaxed);
            let live =
                W_LIVE.fetch_add(layout.size() as u64, Ordering::Relaxed) + layout.size() as u64;
            W_PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if in_window()
            && let Some(owned) = table_remove(ptr as usize)
        {
            W_LIVE.fetch_sub(owned, Ordering::Relaxed);
        }
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if in_window() {
            if let Some(owned) = table_remove(ptr as usize) {
                W_LIVE.fetch_sub(owned, Ordering::Relaxed);
            }
            if !p.is_null() {
                table_insert(p as usize, new_size as u64);
                W_COUNT.fetch_add(1, Ordering::Relaxed);
                let live = W_LIVE.fetch_add(new_size as u64, Ordering::Relaxed) + new_size as u64;
                W_PEAK.fetch_max(live, Ordering::Relaxed);
            }
        }
        p
    }
}

#[global_allocator]
static ALLOCATOR: CohortAlloc = CohortAlloc;

/// Runs `f` with a fresh cohort open and returns `(value, peak, count)`.
fn measure<T>(f: impl FnOnce() -> T) -> (T, u64, u64) {
    GENERATION.fetch_add(1, Ordering::SeqCst);
    OVERFLOW.store(false, Ordering::SeqCst);
    W_LIVE.store(0, Ordering::SeqCst);
    W_PEAK.store(0, Ordering::SeqCst);
    W_COUNT.store(0, Ordering::SeqCst);
    IN_WINDOW.with(|c| c.set(true));
    let out = f();
    IN_WINDOW.with(|c| c.set(false));
    assert!(
        !OVERFLOW.load(Ordering::SeqCst),
        "cohort table overflowed — the instrument failed loudly rather than under-counting"
    );
    (
        out,
        W_PEAK.load(Ordering::SeqCst),
        W_COUNT.load(Ordering::SeqCst),
    )
}

// =====================================================================
// 2. The ladder
// =====================================================================

/// The pre-committed body sizes, in bytes.
const LADDER: [usize; 4] = [1024, 16 * 1024, 256 * 1024, 4 * 1024 * 1024];

/// The rungs the shorter test walks: the pre-committed ladder's prefix.
///
/// **A disclosed deviation, and the measurement that FORCED it has since
/// been repealed.** The plan chose 4 MiB as the top rung "so the whole
/// file stays inside the `ci` job", and at the time that was false: one
/// 4 MiB rung cost **228-264 s** measured and **34-42 s** with no window
/// open at all, because the `| json` flatten was then QUADRATIC in the
/// emitted label count. Ranges, not values, for the reason the module doc
/// gives — the machine is shared, so one figure is one draw.
///
/// The single run that first caught it, kept verbatim as the record of
/// how it was caught and NOT as a claim about what the rung costs
/// (`cargo test -p pulsus-read --test logql_row_expansion_ladder --
/// --nocapture --test-threads=1`):
///
/// ```text
/// B=      977  uninstrumented   129.8us   measured    1.18ms
/// B=   16 105  uninstrumented     1.21ms  measured   15.96ms
/// B=  257 909  uninstrumented   138.7ms   measured  858.70ms
/// B=4 126 651  uninstrumented    40.52s   measured  256.70s
/// ```
///
/// Issue #447 removed the quadratic term, so
/// `zz_the_full_precommitted_ladder` no longer needs `#[ignore]` and now
/// runs always-on — see its own doc for the measurement that repealed it.
/// This prefix is kept, and `the_measured_expansion_factor_is_recorded`
/// still walks it, for two reasons: it spans 256x on its own — four times
/// the `>= 64x` bar — and it yields the SAME `F` as the full ladder, so it
/// is the standing evidence that the rule's answer does not depend on the
/// top rung. Post-#447 medians: 3.055 / 3.934 / 3.934, against the full
/// ladder's 3.055 / 3.934 / 3.934 / 3.934.
const CI_LADDER: [usize; 3] = [1024, 16 * 1024, 256 * 1024];

/// Reps per rung; the first is discarded as warm-up.
const REPS: usize = 5;

/// The query-path `| json` flatten — **not** `/detected_fields`' capture
/// mode, which builds its parser with capture ON.
const QUERY: &str = r#"{app="a"} | json"#;

/// One flat JSON object of `m` equal-sized string leaves, sized so the
/// rendered document is as close to `target` bytes as the shape allows.
/// FIXED shape across the ladder: only `m` varies.
fn body_of(target: usize) -> String {
    // `"kNNNNNN":"vvvv…",` — a 6-digit key and a value padded to a fixed
    // width, so every leaf costs the same and `m` scales linearly.
    const VALUE_BYTES: usize = 48;
    const PER_LEAF: usize = 6 + 4 + VALUE_BYTES + 2 + 2; // key, quotes+colon, value, quotes, comma
    let m = ((target.saturating_sub(2)) / PER_LEAF).max(1);
    let mut s = String::with_capacity(target + PER_LEAF);
    s.push('{');
    for i in 0..m {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("\"k{i:06}\":\"{}\"", "v".repeat(VALUE_BYTES)));
    }
    s.push('}');
    s
}

fn compiled() -> CompiledPipeline {
    let expr = pulsus_logql::parse(QUERY).expect("parse");
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("expected a log query");
    };
    CompiledPipeline::compile(&log.pipeline).expect("compile")
}

/// `f(B)` for one body, over [`REPS`] reps with the first discarded.
/// Returns `(median, all reps)` as ratios scaled by 1000 so the report
/// carries three decimal places without floating-point formatting drift.
fn ratios_milli(pipeline: &CompiledPipeline, body: &str) -> (u64, Vec<u64>) {
    let base: Vec<(String, String)> = vec![
        ("app".to_string(), "a".to_string()),
        ("service_name".to_string(), "checkout".to_string()),
    ];
    // The scratch is caller-owned and reused across rows in production,
    // so it is built and WARMED outside every window — one warm-up run,
    // discarded, then `REPS` measured reps, exactly as the rule states.
    let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
    let t_warm = std::time::Instant::now();
    let warm = pipeline.run_into(body, &base, 0, &mut scratch);
    let warm_wall = t_warm.elapsed();
    println!(
        "LADDER-WALL uninstrumented run_into B={} wall={warm_wall:?}",
        body.len()
    );
    assert!(warm.is_ok(), "the ladder must not breach a row budget");
    assert!(
        !scratch.is_empty(),
        "the flatten emitted no labels — the fixture does not exercise `| json`"
    );
    scratch.clear();

    let mut reps: Vec<u64> = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let ((), peak, count) = measure(|| {
            let out = pipeline
                .run_into(body, &base, 0, &mut scratch)
                .expect("no budget breach");
            std::hint::black_box(&out);
        });
        assert!(count > 0, "the window observed no allocation at all");
        scratch.clear();
        reps.push(peak.saturating_mul(1000) / body.len() as u64);
    }
    let mut sorted = reps.clone();
    sorted.sort_unstable();
    (sorted[sorted.len() / 2], reps)
}

/// **The measurement, and the pre-committed rule applied to it as
/// written.**
///
/// The rule's two branches are both implemented: the assertion at the end
/// is on whichever branch the measured `F` selects, and the transcript
/// printed above it carries every rep so the decision is checkable rather
/// than reported.
#[test]
fn the_measured_expansion_factor_is_recorded() {
    apply_the_rule(&CI_LADDER);
}

/// The FULL pre-committed ladder, 4 MiB rung included.
///
/// **The `#[ignore]` is gone, and only because the wall time that forced
/// it is gone** (issue #447). It was `#[ignore]`d "for wall-time only" —
/// see [`CI_LADDER`] for the transcript — and after the flatten's O(m^2)
/// key lookup was replaced the whole test costs **1.2-2.0 s**.
///
/// Ranges over 60 runs of this test, one process per run,
/// `CARGO_INCREMENTAL=0 cargo test -p pulsus-read --test
/// logql_row_expansion_ladder -- --nocapture --test-threads=1`, on a
/// SHARED build machine — several builds run on it concurrently, so
/// loadavg was 0.88-1.28 across the 120 samples taken either side of the
/// 60 runs and no single figure below is reproducible on demand:
///
/// ```text
///                    uninstrumented run_into        rung total
/// B=      977          76 us -    310 us         0.71 ms -  2.61 ms
/// B=   16 105        0.38 ms -   1.07 ms         11.4 ms -  26.4 ms
/// B=  257 909         5.8 ms -   21.4 ms         62.5 ms - 248.2 ms
/// B=4 126 651        0.11 s  -    0.23 s          1.09 s -  1.73 s
/// ```
///
/// The pre-#447 top rung was 34-42 s uninstrumented over 4 runs of the
/// same test on the same machine, so the gap is roughly two orders of
/// magnitude at every point in both ranges. **Quote the range, never a
/// value from inside it** — a single figure from this machine is one
/// draw, and the 4 MiB rung's spread is about 2x.
///
/// Restore the attribute if the 4 MiB rung ever climbs back into the tens
/// of seconds — that would itself be the regression worth failing on.
#[test]
fn zz_the_full_precommitted_ladder() {
    apply_the_rule(&LADDER);
}

fn apply_the_rule(rungs: &[usize]) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let pipeline = compiled();
    let mut f_max_milli = 0u64;
    for &target in rungs {
        let body = body_of(target);
        let t0 = std::time::Instant::now();
        let (median, reps) = ratios_milli(&pipeline, &body);
        println!(
            "LADDER B={:>9} (rendered {:>9}) median_f={}.{:03} reps={:?} wall={:?}",
            target,
            body.len(),
            median / 1000,
            median % 1000,
            reps,
            t0.elapsed()
        );
        f_max_milli = f_max_milli.max(median);
    }
    println!(
        "LADDER F={}.{:03}  MAX_DECOMPRESSED_BYTES={MAX_DECOMPRESSED_BYTES}  \
         MAX_JSON_FLATTEN_KEY_BYTES={MAX_JSON_FLATTEN_KEY_BYTES}",
        f_max_milli / 1000,
        f_max_milli % 1000
    );

    // The rule, applied as written. `F * MAX_DECOMPRESSED_BYTES` is
    // computed in milli-units to keep it in integer arithmetic.
    let worst_case_bytes = f_max_milli.saturating_mul(MAX_DECOMPRESSED_BYTES as u64) / 1000;
    let charge_nothing = worst_case_bytes <= MAX_JSON_FLATTEN_KEY_BYTES;
    println!(
        "LADDER worst_case = F * MAX_DECOMPRESSED_BYTES = {worst_case_bytes} B; \
         charge_nothing = {charge_nothing}"
    );

    // **THE RECORDED OUTCOME, and where it stops.**
    //
    // `F = 3.934` (3.583 before issue #447 added the flatten's label
    // index, whose table carries a doubling transient), so
    // `F * MAX_DECOMPRESSED_BYTES = 264 006 270 B` (was 240 451 059 B)
    // exceeds `MAX_JSON_FLATTEN_KEY_BYTES = 67 108 864 B`. The rule's
    // record-nothing branch does NOT fire; the rule selects **charge the
    // residual**, and that selection is asserted below so it cannot be
    // read off prose.
    //
    // **The charge is NOT implemented here, and the reason is a gap in
    // the rule rather than a judgement about the answer.** The rule fixes
    // the ladder, the statistic and the branch; it does not fix the
    // COEFFICIENT a charge would use, and every candidate is the measured
    // ratio itself — a machine- and profile-dependent figure, which
    // `plan_recursive_control.rs:21-24` says in terms must never be
    // pinned as a threshold. Charging `alloc_block_bytes(body.len())`
    // (2x) or `grown_alloc_bytes(body.len())` (3x) both sit BELOW the
    // measured 3.934x, so they would under-charge; charging at the
    // measured ratio pins that ratio. Separately, any of them tightens
    // the existing `RowBudget::JsonFlattenKeys` 422 to refuse a `| json`
    // row the reference serves, which is a divergence and needs the
    // ledger entry and docs the parity mandate requires and this plan
    // does not provision. Referred with the measurement in hand rather
    // than guessed; see the issue's closeout comment.
    //
    // What IS asserted, so a regression cannot pass quietly: the rule's
    // branch, that the expansion stays a small constant multiple of the
    // body — `O(B)`, not `O(B^2)` — across a ladder spanning at least
    // 64x, and the two exact byte figures the docs quote.
    //
    // **The exact figures are asserted by EQUALITY, and that is why they
    // are the one pair of numbers in this file written as single values
    // rather than ranges.** They are deterministic allocation counts
    // under a pinned toolchain, not wall times, so equality is available
    // here in a way it is not for anything timed. The bounds below would
    // pass over a wide band; only the equality makes the quoted `3.934`
    // and `264 006 270 B` a claim a test can refute.
    assert!(
        f_max_milli >= 1000,
        "F fell below 1.0, which would put the rule on its record-nothing branch — re-run \
         the decision, do not adjust this assertion"
    );
    assert!(
        f_max_milli <= 32_000,
        "the `| json` row expansion factor is now {}.{:03}x the body, past the 32x ceiling \
         this ladder was linear under. The per-row residual named in `pipeline.rs`'s \
         `JsonPaths` doc has grown super-linearly and issue #241's decision must be re-taken.",
        f_max_milli / 1000,
        f_max_milli % 1000
    );
    assert!(
        !charge_nothing,
        "the measured F now satisfies `F * MAX_DECOMPRESSED_BYTES <= \
         MAX_JSON_FLATTEN_KEY_BYTES`, so the rule's record-nothing branch fires and the \
         residual note in `pipeline.rs` should carry the inequality"
    );
    assert_eq!(
        f_max_milli,
        F_MILLI,
        "F is no longer exactly {}.{:03}. This is a DETERMINISTIC allocation count, not a \
         wall time, so a move here is a real change in the flatten's allocation shape — not \
         machine noise. Re-derive it, then update this constant and every site that quotes \
         it — `the_quoted_figures_are_counted_so_a_new_quotation_cannot_go_unchecked` will \
         tell you how many there are; do not relax this assertion.",
        F_MILLI / 1000,
        F_MILLI % 1000
    );
    assert_eq!(
        worst_case_bytes, WORST_CASE_BYTES,
        "`F * MAX_DECOMPRESSED_BYTES` is no longer exactly {WORST_CASE_BYTES} B. Same rule as \
         for F: re-derive and update the constant and the docs, do not widen this."
    );
}

/// `F` in milli-units, as [`apply_the_rule`] computes it and asserts it
/// by equality.
///
/// **How many places quote it is COUNTED, not listed** — see
/// [`the_quoted_figures_are_counted_so_a_new_quotation_cannot_go_unchecked`].
/// A hand-written inventory of quotation sites has no failure mode: the
/// next edit that adds a mention silently falsifies it, which is exactly
/// what happened to the list that used to sit here, and the edit that
/// falsified it was the one that introduced it. The count fails instead.
///
/// The one quotation the count cannot reach is issue #241's closeout
/// comment, which is outside the tree and outside any test's reach. It
/// quotes this value too, and a re-derivation has to go and correct it by
/// hand; that is why it is named here rather than counted.
///
/// Both ladders yield it: the 4 MiB rung is not what sets it (the 16 KiB
/// rung already reaches it), which is [`CI_LADDER`]'s standing point. It
/// was 3.583 before issue #447's label index added the table's doubling
/// transient.
const F_MILLI: u64 = 3934;

/// `F * MAX_DECOMPRESSED_BYTES` in bytes (240 451 059 B before issue
/// #447), derived from [`F_MILLI`] and asserted separately in
/// [`apply_the_rule`] so the integer arithmetic that produces it is
/// pinned too. Its quotations are counted in the same test as
/// [`F_MILLI`]'s, and issue #241's closeout quotes it as well.
const WORST_CASE_BYTES: u64 = 264_006_270;

/// Renders `n` with a space every three digits, the way this file's prose
/// writes byte figures.
///
/// It exists so the needle
/// [`the_quoted_figures_are_counted_so_a_new_quotation_cannot_go_unchecked`]
/// searches for is DERIVED from [`WORST_CASE_BYTES`] rather than typed
/// out beside it. A typed needle is a second declaration of the same
/// value, free to disagree with the first — and it would also be found by
/// its own search, so the count would include the searcher.
fn digit_grouped(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.char_indices() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(' ');
        }
        out.push(c);
    }
    out
}

/// How many times this file's own source quotes `F`'s rendered value.
///
/// **Occurrences, not lines** — one line quoting it three times counts
/// three, because a line count would let a quotation be added to a line
/// that already has one and go unseen.
const F_QUOTATIONS: usize = 9;

/// How many times this file's own source quotes [`WORST_CASE_BYTES`] in
/// the space-grouped rendering [`digit_grouped`] produces. Occurrences,
/// not lines, for the reason [`F_QUOTATIONS`] gives.
const WORST_CASE_QUOTATIONS: usize = 3;

/// **The inventory of quotation sites, as a check rather than a
/// sentence.**
///
/// Both figures are exempt from this file's range-only rule because
/// [`apply_the_rule`] pins them by equality — but that pins the values,
/// not the prose repeating them, and prose repeating a number is how a
/// figure goes stale in one place while staying right in another. So the
/// file reads its own source and counts.
///
/// **If this fails you have added or removed a quotation.** Open the site
/// that moved, check it states the current value, and only then update
/// the constant. Updating the constant first turns the check back into
/// the sentence it replaced.
///
/// Scope, stated rather than implied: this counts one RENDERING of each
/// figure in THIS file — `F` as `{units}.{milli:03}` and the byte figure
/// space-grouped. The forms the code itself produces are deliberately
/// outside it: `F_MILLI`'s and `WORST_CASE_BYTES`'s own literals, and the
/// undelimited `worst_case` value the `LADDER` transcript prints. Those
/// are computed, not quoted, so they cannot go stale.
#[test]
fn the_quoted_figures_are_counted_so_a_new_quotation_cannot_go_unchecked() {
    // The file's own source, embedded at compile time. Needles are built
    // from the constants at run time, so neither literal appears here and
    // the count is of quotations elsewhere, never of the searcher.
    const SELF_SOURCE: &str = include_str!("logql_row_expansion_ladder.rs");

    let f_text = format!("{}.{:03}", F_MILLI / 1000, F_MILLI % 1000);
    let bytes_text = digit_grouped(WORST_CASE_BYTES);

    assert_eq!(
        SELF_SOURCE.matches(f_text.as_str()).count(),
        F_QUOTATIONS,
        "this file now quotes `{f_text}` a different number of times. Find the site that \
         changed, check it carries the value `apply_the_rule` asserts, then update \
         `F_QUOTATIONS` — in that order."
    );
    assert_eq!(
        SELF_SOURCE.matches(bytes_text.as_str()).count(),
        WORST_CASE_QUOTATIONS,
        "this file now quotes `{bytes_text}` a different number of times. Same order as for \
         `F_QUOTATIONS`: check the site first, update `WORST_CASE_QUOTATIONS` second."
    );
}

/// The ladder spans a >= 64x body range, so `F` being flat across it is
/// evidence of LINEARITY rather than of one lucky size. Asserted, because
/// "measured over a ladder" means nothing if the ladder is narrow.
#[test]
fn the_ladder_spans_at_least_sixty_four_times() {
    assert!(
        LADDER.starts_with(&CI_LADDER),
        "the always-on rungs are no longer a PREFIX of the pre-committed ladder, so the two \
         tests no longer apply the same rule to the same shapes"
    );
    for rungs in [&LADDER[..], &CI_LADDER[..]] {
        let lo = rungs.iter().min().copied().expect("non-empty");
        let hi = rungs.iter().max().copied().expect("non-empty");
        assert!(
            hi / lo >= 64,
            "a ladder spans only {}x — too narrow to distinguish linear from super-linear",
            hi / lo
        );
    }
    for target in LADDER {
        let body = body_of(target);
        // The rendered document must actually be near its target, or the
        // rungs are not the sizes the rule names.
        assert!(
            body.len() >= target * 9 / 10 && body.len() <= target + 128,
            "rung {target} rendered {} bytes",
            body.len()
        );
    }
}
