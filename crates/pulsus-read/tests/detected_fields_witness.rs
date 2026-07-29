//! The issue #244 allocation witness for `/detected_fields`: cohort-
//! attributed allocation gates over the charged `FieldAccumulator` (claim
//! C1), the streaming `DetectedRowFeeder`/`absorb_page` path (C1), and
//! the helper-level per-row before/after (claim **C2 (Q1+Q2)** — see the
//! #244 plan §A; the claim is scoped to the four row shapes measured here
//! at helper-transcription granularity, never "not worse in general").
//!
//! THE COHORT ATTRIBUTION RULE below is quoted EXACTLY from issue #236's
//! plan comment 5088719129 (v12) §5.1 — the supplied authority — and
//! ships here unchanged (the AC 5 quote guard pins the "Capacity is a
//! power of two" sentence so the quote cannot silently drift):
//!
//! ```text
//! /// THE COHORT ATTRIBUTION RULE (named so it can be cited: #245 and #281
//! /// adopt it as the standard for allocation witnesses in this repo, and
//! /// #281 exists because `logql_variants_alloc.rs` carries the same masking
//! /// class in a different spelling — a process-global `LIVE` reset at window
//! /// open, decremented on every dealloc including pointers it never
//! /// allocated, with `PEAK` as `fetch_max` over it):
//! /// * an allocation belongs to the window that was open, ON THE MEASURING
//! ///   THREAD, when its pointer was returned;
//! /// * a free is counted ONLY against the window that owns that pointer —
//! ///   a free of a pointer owned by no open window is ignored ENTIRELY, so
//! ///   dropping the stage's pre-window input neither raises nor lowers any
//! ///   measured quantity;
//! /// * a realloc is a free of the old pointer plus an allocation of the new
//! ///   one, attributed by the same rule (an in-place realloc that returns
//! ///   the same pointer is handled remove-then-insert);
//! /// * allocations from any other thread are ignored (`MEASURING_THREAD`).
//! /// Therefore `peak` is exactly "the maximum bytes this window itself held
//! /// live" and `retained` is exactly "the bytes this window allocated that
//! /// are still live at close" — neither can be masked by unrelated frees.
//! struct CohortAlloc;
//!
//! /// Fixed-capacity, allocation-free, open-addressed ptr -> size table.
//! /// The allocator hook must never allocate (re-entrancy), so this is a
//! /// `static` array of atomics, never a `HashMap`. Capacity is a power of
//! /// two; an insert that cannot find a free slot within the probe bound
//! /// sets `OVERFLOW`, which every gate asserts is clear — the instrument
//! /// FAILS LOUDLY rather than degrading silently.
//! const COHORT_SLOTS: usize = 1 << 20;      // 16 MiB of static table
//!
//! struct Window { bytes: u64, count: u64, peak: u64, retained: u64 }
//!
//! /// Opens a cohort, runs `f`, closes it, returns `f`'s value ALIVE (so
//! /// anything the call retains is inside `retained`). The fixture MUST be
//! /// built by the caller, outside `f`.
//! fn measure<T>(f: impl FnOnce() -> T) -> (T, Window);
//! ```
//!
//! The only local adaptation, labelled as such rather than folded into
//! the quote: `f`'s subject is this issue's feeder/accumulator, not
//! `apply_vector_aggs`. Tests serialize on a `SERIAL` mutex so no
//! parallel test pollutes a window. Nothing here uses
//! `logql_variants_alloc.rs`'s harness (#281) — a process-global `LIVE`
//! reset at window open, decremented on every dealloc including pointers
//! the window never allocated, under-reports peaks, and that defect is
//! not hypothetical here: `trim()` frees pre-window buffers *inside* a
//! measured window, and `feed_row` drops the previous row's owned strings
//! inside one too. This binary declares its own `#[global_allocator]`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use pulsus_logql::parse;
use pulsus_read::logql::rows::TailSampleRow;
use pulsus_read::logql::template::TemplateEnv;
use pulsus_read::logql::{
    CompiledPipeline, DetectedFieldsProbe, MAX_DETECTED_FIELD_BYTES, MAX_FEEDER_SCRATCH_BYTES,
    ReadError,
};

// ---------------------------------------------------------------------
// The instrument (see the module doc for the rule it implements).
// ---------------------------------------------------------------------

struct CohortAlloc;

const COHORT_SLOTS: usize = 1 << 20; // 16 MiB of static table
const SLOT_MASK: usize = COHORT_SLOTS - 1;
/// Linear-probe bound; exceeding it on insert sets `OVERFLOW` (the
/// instrument fails loudly rather than degrading silently).
const PROBE_BOUND: usize = 128;
/// Removed-slot marker: real pointers are >= 8-aligned, so `1` is never a
/// live pointer. Inserts may reuse tombstones; lookups probe past them.
const TOMBSTONE: usize = 1;

#[allow(clippy::declare_interior_mutable_const)]
static PTRS: [AtomicUsize; COHORT_SLOTS] = [const { AtomicUsize::new(0) }; COHORT_SLOTS];
static SIZES: [AtomicUsize; COHORT_SLOTS] = [const { AtomicUsize::new(0) }; COHORT_SLOTS];

static WINDOW_OPEN: AtomicBool = AtomicBool::new(false);
static OVERFLOW: AtomicBool = AtomicBool::new(false);
static BYTES: AtomicU64 = AtomicU64::new(0);
static COUNT: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);
/// EVERY free observed on the measuring thread while the window is open,
/// ownership-blind — the naive BULK quantity, tracked ONLY so I1 can
/// compute what a bulk instrument would report and assert it differs.
static BULK_FREED: AtomicU64 = AtomicU64::new(0);
static LAST_BULK_FREED: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Const-initialized so the allocator hook's read can never itself
    /// allocate (lazy TLS init would re-enter).
    static ON_MEASURING_THREAD: Cell<bool> = const { Cell::new(false) };
}

fn measuring_here() -> bool {
    WINDOW_OPEN.load(Ordering::SeqCst) && ON_MEASURING_THREAD.try_with(Cell::get).unwrap_or(false)
}

fn slot_hash(ptr: usize) -> usize {
    ((ptr >> 4).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 44) & SLOT_MASK
}

fn on_alloc(ptr: usize, size: usize) {
    if !measuring_here() {
        return;
    }
    let mut idx = slot_hash(ptr);
    for _ in 0..PROBE_BOUND {
        let cur = PTRS[idx].load(Ordering::SeqCst);
        if cur == 0 || cur == TOMBSTONE {
            SIZES[idx].store(size, Ordering::SeqCst);
            PTRS[idx].store(ptr, Ordering::SeqCst);
            BYTES.fetch_add(size as u64, Ordering::SeqCst);
            COUNT.fetch_add(1, Ordering::SeqCst);
            let live = LIVE.fetch_add(size as u64, Ordering::SeqCst) + size as u64;
            PEAK.fetch_max(live, Ordering::SeqCst);
            return;
        }
        idx = (idx + 1) & SLOT_MASK;
    }
    OVERFLOW.store(true, Ordering::SeqCst);
}

fn on_dealloc(ptr: usize, size: usize) {
    if measuring_here() {
        BULK_FREED.fetch_add(size as u64, Ordering::SeqCst);
    }
    if ptr == 0 {
        return;
    }
    // A free is counted ONLY against the window that owns the pointer:
    // the lookup consults the ownership table, never the window state, so
    // an unowned free is ignored ENTIRELY.
    let mut idx = slot_hash(ptr);
    for _ in 0..PROBE_BOUND {
        let cur = PTRS[idx].load(Ordering::SeqCst);
        if cur == 0 {
            return;
        }
        if cur == ptr {
            let sz = SIZES[idx].load(Ordering::SeqCst);
            PTRS[idx].store(TOMBSTONE, Ordering::SeqCst);
            LIVE.fetch_sub(sz as u64, Ordering::SeqCst);
            return;
        }
        idx = (idx + 1) & SLOT_MASK;
    }
}

// SAFETY: every hook path is allocation-free (static atomics + a
// const-initialized TLS flag), so the allocator can never re-enter
// itself; the underlying allocation itself is delegated to `System`
// unchanged.
unsafe impl GlobalAlloc for CohortAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            on_alloc(p as usize, layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        on_dealloc(ptr as usize, layout.size());
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            // A realloc is a free of the old pointer plus an allocation of
            // the new one, attributed by the same rule (an in-place
            // realloc that returns the same pointer is handled
            // remove-then-insert).
            on_dealloc(ptr as usize, layout.size());
            on_alloc(p as usize, new_size);
        }
        p
    }
}

#[global_allocator]
static COHORT: CohortAlloc = CohortAlloc;

/// One closed cohort window's totals (the quoted shape).
#[derive(Debug, Clone, Copy)]
struct Window {
    bytes: u64,
    count: u64,
    peak: u64,
    retained: u64,
}

/// Serializes every windowed test — a parallel test's allocations on
/// another thread are already ignored by the thread gate, but a parallel
/// test on THIS thread is impossible and windows must not nest.
static SERIAL: Mutex<()> = Mutex::new(());

fn lock_serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// Opens a cohort, runs `f`, closes it, returns `f`'s value ALIVE (so
/// anything the call retains is inside `retained`). The fixture MUST be
/// built by the caller, outside `f`. Caller holds `SERIAL`.
fn measure<T>(f: impl FnOnce() -> T) -> (T, Window) {
    assert!(
        !WINDOW_OPEN.load(Ordering::SeqCst),
        "windows must not nest (SERIAL)"
    );
    // Drain the table: entries retained past a previous window's close
    // are owned by NO open window, so their frees must be ignored.
    for i in 0..COHORT_SLOTS {
        PTRS[i].store(0, Ordering::SeqCst);
        SIZES[i].store(0, Ordering::SeqCst);
    }
    BYTES.store(0, Ordering::SeqCst);
    COUNT.store(0, Ordering::SeqCst);
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    BULK_FREED.store(0, Ordering::SeqCst);
    OVERFLOW.store(false, Ordering::SeqCst);
    ON_MEASURING_THREAD.with(|c| c.set(true));
    WINDOW_OPEN.store(true, Ordering::SeqCst);
    let out = f();
    WINDOW_OPEN.store(false, Ordering::SeqCst);
    ON_MEASURING_THREAD.with(|c| c.set(false));
    LAST_BULK_FREED.store(BULK_FREED.load(Ordering::SeqCst), Ordering::SeqCst);
    let w = Window {
        bytes: BYTES.load(Ordering::SeqCst),
        count: COUNT.load(Ordering::SeqCst),
        peak: PEAK.load(Ordering::SeqCst),
        retained: LIVE.load(Ordering::SeqCst),
    };
    (out, w)
}

/// The live in-window peak, readable at a checkpoint INSIDE an open
/// window (allocation-free).
fn cohort_peak_now() -> u64 {
    PEAK.load(Ordering::SeqCst)
}

fn overflow_now() -> bool {
    OVERFLOW.load(Ordering::SeqCst)
}

fn assert_no_overflow(ctx: &str) {
    assert!(
        !overflow_now(),
        "{ctx}: the cohort table overflowed — raise COHORT_SLOTS, never widen a bound"
    );
}

// ---------------------------------------------------------------------
// Instrument discrimination (AC 5) — the instrument is itself under test.
// ---------------------------------------------------------------------

const MIB: u64 = 1024 * 1024;

/// I1 — a pre-window 8 MiB fixture dropped INSIDE the window while 1 MiB
/// is allocated and RETAINED: a sound instrument reports
/// `peak == retained == 1 MiB`; the naive bulk quantity (in-window
/// allocated minus ALL in-window frees) is computed IN-TEST and asserted
/// to DIFFER, so reverting to bulk arithmetic reddens this named test
/// (M12's masking class, #281).
#[test]
fn i1_pre_window_free_neither_raises_nor_lowers_the_cohort_quantities() {
    let _guard = lock_serial();
    let fixture: Vec<u8> = vec![7u8; (8 * MIB) as usize];
    let (kept, w) = measure(move || {
        let kept: Vec<u8> = Vec::with_capacity(MIB as usize);
        drop(fixture); // an 8 MiB free of pointers this window never made
        kept
    });
    assert_no_overflow("I1");
    assert_eq!(w.peak, MIB, "peak is the window's own live maximum");
    assert_eq!(
        w.retained, MIB,
        "retained is the window's own live-at-close"
    );
    drop(kept);
    // The naive bulk quantity a LIVE-counter instrument would report:
    // in-window allocations minus every in-window free, ownership-blind.
    let bulk_live_at_close = (w.bytes as i64) - (LAST_BULK_FREED.load(Ordering::SeqCst) as i64);
    assert!(
        bulk_live_at_close < w.retained as i64,
        "the bulk quantity ({bulk_live_at_close}) must DIFFER from the cohort retained \
         ({}) — if these agree the instrument has regressed to bulk arithmetic (M12)",
        w.retained
    );
}

/// I2 — allocate and drop 1 MiB inside: `peak == 1 MiB`, `retained == 0`
/// (the case a bulk instrument gets right).
#[test]
fn i2_in_window_alloc_and_free_peaks_then_retains_nothing() {
    let _guard = lock_serial();
    let ((), w) = measure(|| {
        let v: Vec<u8> = Vec::with_capacity(MIB as usize);
        drop(v);
    });
    assert_no_overflow("I2");
    assert_eq!(w.peak, MIB);
    assert_eq!(w.retained, 0);
}

/// I3 — a `Vec` allocated BEFORE the window and grown INSIDE it: the new
/// buffer is charged, the old buffer's free is ignored (a bulk
/// instrument's peak is deflated by the old buffer's free).
#[test]
fn i3_growth_of_a_pre_window_buffer_charges_the_new_block_and_ignores_the_old() {
    let _guard = lock_serial();
    let mut v: Vec<u8> = Vec::with_capacity(MIB as usize);
    v.resize(MIB as usize, 3);
    let (_, w) = measure(|| {
        v.reserve_exact(3 * MIB as usize); // realloc 1 MiB -> 4 MiB
    });
    assert_no_overflow("I3");
    assert_eq!(w.peak, 4 * MIB, "the new 4 MiB buffer, old free ignored");
    assert_eq!(w.retained, 4 * MIB, "still live at close (v outlives)");
    drop(v);
}

/// I4 — more distinct live in-window allocations than the table can hold:
/// `OVERFLOW` fires loudly; the gate fails rather than under-counting.
#[test]
fn i4_exceeding_the_cohort_table_sets_overflow_loudly() {
    let _guard = lock_serial();
    let (boxes, _w) = measure(|| {
        let mut boxes: Vec<Box<u64>> = Vec::with_capacity(COHORT_SLOTS + 4096);
        for i in 0..(COHORT_SLOTS + 4096) {
            boxes.push(Box::new(i as u64));
        }
        boxes
    });
    assert!(
        overflow_now(),
        "more live allocations than COHORT_SLOTS must set OVERFLOW"
    );
    drop(boxes);
}

/// AC 5's quote guard: the cohort rule ships in this binary's module doc
/// quoted EXACTLY from #236 comment 5088719129 §5.1 — the "Capacity is a
/// power of two" sentence (dropped once by a prior adopter) and the
/// pre-window-input clause are pinned so the quote cannot silently drift.
#[test]
fn module_doc_carries_the_cohort_rule_quote_verbatim() {
    let source = include_str!("detected_fields_witness.rs");
    for needle in [
        "Capacity is a power of",
        "dropping the stage's pre-window input neither raises nor lowers any",
        "THE COHORT ATTRIBUTION RULE (named so it can be cited: #245 and #281",
        "the same pointer is handled remove-then-insert);",
    ] {
        assert!(
            source.contains(needle),
            "the module-doc quote lost the sentence {needle:?}"
        );
    }
}

// ---------------------------------------------------------------------
// Shared fixtures.
// ---------------------------------------------------------------------

const SLACK: u64 = 65_536;
const CHECK_EVERY: usize = 64;

fn compile(query: &str) -> CompiledPipeline {
    let expr = parse(query).expect("parse");
    let pulsus_logql::Expr::Log(le) = expr else {
        panic!("log expr expected: {query}");
    };
    CompiledPipeline::compile(&le.pipeline)
        .expect("compile")
        .with_template_env(TemplateEnv::default())
}

/// The A–D checkpoint (issue #244 AC 6): every `CHECK_EVERY`
/// observations, the window's live peak must sit under the model's
/// charged peak plus slack — under M1 (an uncharged budget) this fires
/// within the first 1 000 observations, naming the index.
fn checkpoint(probe: &DetectedFieldsProbe, i: usize, case: &str) {
    assert!(
        cohort_peak_now() <= probe.peak_charged() + SLACK,
        "{case}: cohort peak {} exceeded peak_charged {} + SLACK {SLACK} at observation {i}",
        cohort_peak_now(),
        probe.peak_charged(),
    );
}

/// AC 10 — non-vacuity: the charge model must stay within a fixed factor
/// of the REAL bytes (an inflated charge — M5 — trips this, a vacuous
/// always-huge model cannot pass it).
fn assert_non_vacuous(probe: &DetectedFieldsProbe, peak: u64, case: &str) {
    assert!(
        probe.charged() <= 16 * peak + 65_536,
        "{case}: charged {} is vacuously large vs cohort peak {peak}",
        probe.charged()
    );
}

/// The M4 discriminator for A–D: the window's TOTAL allocated bytes stay
/// within a fixed factor of the charge. The plan's gate table expects the
/// uncharged-transient mutant (M4: `let _t = value.to_owned()` per
/// observation) to fail the A–D gates, but the PEAK gate alone cannot see
/// it — the charge model's deliberate ~2x over-charge absorbs one
/// value-width transient — so the claim is backed here by the total,
/// which M4 inflates by the whole observation stream's width (a
/// width-independent BYTE ceiling, per the house alloc-test rule).
///
/// COVERAGE — which CASES catch M4 depends on WHERE M4 is placed, so it
/// is stated here rather than left for the next reader to rediscover.
/// Both placements were run and observed (issue #244, implementation and
/// review of `8caa4c1`):
///  * M4 at the TOP of `observe_pair`, so every observed pair pays the
///    transient: **A, B and D fail** (in-window totals 268 895 079 /
///    25 952 355 / 134 415 016 B). **C does not** — case C's values are
///    1 byte, so a value-width clone is structurally invisible to any
///    byte ceiling. The plan's "M4 must fail A–D" is over-broad for C,
///    and that is a property of C's axis (field NAMES), not a hole.
///  * M4 immediately BEFORE `observe_admitted`, i.e. after the
///    field-name charge: **only A and B fail**. In C and D the 64 KiB
///    field names exhaust the 1 MiB budget within the first handful of
///    observations, after which the name charge refuses and
///    `observe_pair` returns early — the transient never runs.
///
/// Every placement tried is caught, but A and B are the only cases that
/// catch both; C and D corroborate, they do not stand in for A/B.
fn assert_transient_total(probe: &DetectedFieldsProbe, w: &Window, case: &str) {
    assert!(
        w.bytes <= 4 * probe.charged() + MIB,
        "{case}: in-window total {} bytes exceeds 4 x charged {} + 1 MiB — an uncharged          per-observation transient (M4) is allocating outside the model",
        w.bytes,
        probe.charged()
    );
}

// ---------------------------------------------------------------------
// Cases A–D: the charged accumulator under the cohort instrument.
// ---------------------------------------------------------------------

/// Case A — the value-BYTE axis: one field, 4 096 distinct 64 KiB values
/// from ONE reused fixture `String`; budget 1 MiB. Retained under M1:
/// 4 096 x 65 536 = 268 435 456 B (256.0x the budget).
#[test]
fn case_a_value_byte_axis_is_charged_before_retention() {
    let _guard = lock_serial();
    let mut probe = DetectedFieldsProbe::with_byte_budget(100, 1000, MIB);
    let pad = "x".repeat(65_536 - 16);
    let mut value = String::with_capacity(65_536);
    let ((), w) = measure(|| {
        for i in 0..4096usize {
            // Non-digit-leading values: a digit-prefixed 64 KiB value pays
            // `determine_type`'s pre-existing width-sized transient
            // (`parse_bytes_value`'s unit `to_lowercase` copy — flagged in
            // the #244 notes), which would swamp the M4 discriminator.
            value.clear();
            value.push_str(&pad);
            write!(value, "{i:016}").expect("write");
            probe.observe_pair("payload", &value, None);
            if i > 0 && i % CHECK_EVERY == 0 {
                checkpoint(&probe, i, "case A");
            }
        }
    });
    assert_no_overflow("case A");
    checkpoint(&probe, 4096, "case A (close)");
    assert_non_vacuous(&probe, w.peak, "case A");
    assert_transient_total(&probe, &w, "case A");
    let (_, capped) = probe.finish();
    assert!(capped, "the 1 MiB budget must refuse 64 KiB values");
}

/// Case B — the value-COUNT axis: 400 000 distinct 64 B values; budget
/// 1 MiB. Retained under M1: 400 000 x 64 = 25 600 000 B (24.4x).
#[test]
fn case_b_value_count_axis_is_charged_before_retention() {
    let _guard = lock_serial();
    let mut probe = DetectedFieldsProbe::with_byte_budget(100, 1000, MIB);
    let mut value = String::with_capacity(64);
    let ((), w) = measure(|| {
        for i in 0..400_000usize {
            value.clear();
            write!(value, "{i:064}").expect("write");
            probe.observe_pair("uid", &value, None);
            if i > 0 && i % CHECK_EVERY == 0 {
                checkpoint(&probe, i, "case B");
            }
        }
    });
    assert_no_overflow("case B");
    checkpoint(&probe, 400_000, "case B (close)");
    assert_non_vacuous(&probe, w.peak, "case B");
    assert_transient_total(&probe, &w, "case B");
    let (_, capped) = probe.finish();
    assert!(capped, "400k distinct values must breach 1 MiB");
}

/// Case C — the field-NAME axis: 512 distinct 64 KiB names; budget 1 MiB.
/// Retained under M1: 512 x 65 536 = 33 554 432 B (32.0x).
#[test]
fn case_c_field_name_axis_is_charged_before_retention() {
    let _guard = lock_serial();
    let mut probe = DetectedFieldsProbe::with_byte_budget(100, 5000, MIB);
    let pad = "n".repeat(65_536 - 16);
    let mut name = String::with_capacity(65_536);
    let ((), w) = measure(|| {
        for i in 0..512usize {
            name.clear();
            write!(name, "{i:016}").expect("write");
            name.push_str(&pad);
            probe.observe_pair(&name, "v", None);
            if i > 0 && i % CHECK_EVERY == 0 {
                checkpoint(&probe, i, "case C");
            }
        }
    });
    assert_no_overflow("case C");
    checkpoint(&probe, 512, "case C (close)");
    assert_non_vacuous(&probe, w.peak, "case C");
    assert_transient_total(&probe, &w, "case C");
    let (_, capped) = probe.finish();
    assert!(capped, "512 x 64 KiB names must breach 1 MiB");
}

/// Case D — MIXED: `N_D` iterations of a (64 KiB name, 64 KiB value)
/// pair from two reused buffers (live harness input a constant
/// 131 072 B); budget 1 MiB. Retained under M1 at N_D = 2048:
/// 268 435 456 B (256.0x; the pre-committed floor is N_D = 512 at 64.0x).
/// Pre-committed runtime rule: if a run exceeds 10 s in the dev profile,
/// halve N_D — at most twice, never below 512 — recording each step.
#[test]
fn case_d_mixed_axes_are_charged_before_retention() {
    let _guard = lock_serial();
    let name_pad = "n".repeat(65_536 - 16);
    let value_pad = "x".repeat(65_536 - 16);
    let mut n_d = 2048usize;
    loop {
        let mut probe = DetectedFieldsProbe::with_byte_budget(100, 5000, MIB);
        let mut name = String::with_capacity(65_536);
        let mut value = String::with_capacity(65_536);
        let started = std::time::Instant::now();
        let ((), w) = measure(|| {
            for i in 0..n_d {
                // Non-digit-leading (see case A).
                name.clear();
                name.push_str(&name_pad);
                write!(name, "{i:016}").expect("write");
                value.clear();
                value.push_str(&value_pad);
                write!(value, "{i:016}").expect("write");
                probe.observe_pair(&name, &value, None);
                if i > 0 && i % CHECK_EVERY == 0 {
                    checkpoint(&probe, i, "case D");
                }
            }
        });
        let elapsed = started.elapsed();
        assert_no_overflow("case D");
        checkpoint(&probe, n_d, "case D (close)");
        assert_non_vacuous(&probe, w.peak, "case D");
        assert_transient_total(&probe, &w, "case D");
        let (_, capped) = probe.finish();
        assert!(capped, "the mixed axes must breach 1 MiB");
        if elapsed.as_secs() < 10 {
            eprintln!("case D: N_D = {n_d}, elapsed {elapsed:?}");
            break;
        }
        eprintln!("case D: N_D = {n_d} took {elapsed:?} (> 10 s) — halving (pre-committed rule)");
        assert!(
            n_d > 512,
            "case D still over 10 s at the N_D = 512 floor — ESCALATE, never shrink further"
        );
        n_d /= 2;
    }
}

// ---------------------------------------------------------------------
// Case E: the sampled-row axis is streamed (AC 11).
// ---------------------------------------------------------------------

/// Case E — `absorb_page` over 2 000 rows of 64 KiB non-parseable bodies,
/// constructed lazily INSIDE the window: one row is in flight at a time.
/// Ceiling, all three constant terms stated:
/// `MAX_FEEDER_SCRATCH_BYTES 1 196 032 + ROW_TRANSIENT 524 288 + SLACK
/// 65 536 = 1 785 856 B`; the runtime term `peak_charged()` is asserted
/// `== 0` (non-parseable bodies admit no field, so nothing is charged).
/// M3 (materialise the page first) retains 2 000 x 65 536 =
/// 131 072 000 B => >= 73.3x this ceiling.
#[test]
fn case_e_sampled_rows_are_streamed_one_row_live() {
    let _guard = lock_serial();
    const ROW_TRANSIENT: u64 = 8 * 65_536;
    const CEILING: u64 = 1_785_856;
    assert_eq!(
        MAX_FEEDER_SCRATCH_BYTES + ROW_TRANSIENT + SLACK,
        CEILING,
        "the three constant terms must sum to the stated ceiling"
    );
    let compiled = compile(r#"{app="x"}"#);
    let mut probe = DetectedFieldsProbe::new(5000, 1000);
    probe.add_stream(1, &[("app".to_string(), "x".to_string())]);
    // Warm the parsers (LazyLock) outside the window so their one-time
    // compilation is not read as streaming cost.
    probe
        .feed_row(&compiled, 1, 0, "zzzz", "")
        .expect("warm row");
    let (decision, w) = measure(|| {
        let rows = (0..2000u64).map(|i| {
            Ok::<TailSampleRow, ReadError>(TailSampleRow {
                fingerprint: 1,
                timestamp_ns: 2_000_000 - i as i64,
                body: "z".repeat(65_536),
                body_hash: i,
                structured_metadata: String::new(),
            })
        });
        let mut stream = futures::stream::iter(rows);
        futures::executor::block_on(probe.absorb_page(&compiled, &mut stream, 1))
    });
    assert_no_overflow("case E");
    let decision = decision.expect("absorb_page succeeds");
    assert_eq!(
        decision,
        Some(false),
        "2000 < page_size terminates COMPLETE"
    );
    assert_eq!(
        probe.peak_charged(),
        0,
        "non-parseable bodies admit no field, so nothing is charged"
    );
    assert!(
        w.peak <= probe.peak_charged() + CEILING,
        "case E: cohort peak {} exceeded the streaming ceiling {CEILING} — the page is being \
         re-materialised (M3 retains 131 072 000 B, >= 73.3x this bound)",
        w.peak
    );
    assert!(
        probe.scratch_capacity_bytes() <= MAX_FEEDER_SCRATCH_BYTES,
        "carried scratch {} over bound",
        probe.scratch_capacity_bytes()
    );
    eprintln!(
        "case E: cohort peak {} over {} allocations (ceiling {CEILING}), matched {}",
        w.peak,
        w.count,
        probe.matched()
    );
}

// ---------------------------------------------------------------------
// Cases F and G: the carried feeder bound (AC 12).
// ---------------------------------------------------------------------

/// AC 12's runtime-term cap: generous for 8 admitted fields with 6-byte
/// keys and 1-byte values, ASSERTED rather than assumed so the charge
/// model cannot drift out from under the M2 ratio (the F/G ceiling is
/// `1 196 032 + 65 536 + 65 536 = 1 327 104 B`).
const CHARGED_CAP: u64 = 65_536;

fn wide_sm_json(w: usize) -> String {
    let mut s = String::with_capacity(w * 14 + 2);
    s.push('{');
    for i in 0..w {
        if i > 0 {
            s.push(',');
        }
        write!(s, r#""k{i:05}":"1""#).expect("write");
    }
    s.push('}');
    s
}

fn assert_narrow_tail_bound(
    probe: &mut DetectedFieldsProbe,
    compiled: &CompiledPipeline,
    case: &str,
) {
    // The wide row has been fed OUTSIDE any window; the trim contract
    // must already hold before the tail begins.
    assert!(
        probe.scratch_capacity_bytes() <= MAX_FEEDER_SCRATCH_BYTES,
        "{case}: carried capacity {} exceeds MAX_FEEDER_SCRATCH_BYTES {MAX_FEEDER_SCRATCH_BYTES} \
         after the wide row (M2: an uncapped trim carries the wide spines forever)",
        probe.scratch_capacity_bytes()
    );
    let ((), w) = measure(|| {
        for i in 0..200i64 {
            probe
                .feed_row(compiled, 1, 1_000_000 + i, "!!! narrow", "")
                .expect("narrow row");
        }
    });
    assert_no_overflow(case);
    assert!(
        probe.peak_charged() <= CHARGED_CAP,
        "{case}: peak_charged {} exceeds CHARGED_CAP {CHARGED_CAP}",
        probe.peak_charged()
    );
    assert!(
        w.peak <= probe.peak_charged() + MAX_FEEDER_SCRATCH_BYTES + SLACK,
        "{case}: narrow-tail cohort peak {} exceeds the carried bound (ceiling {} = charged cap \
         {CHARGED_CAP} + MAX_FEEDER_SCRATCH_BYTES {MAX_FEEDER_SCRATCH_BYTES} + SLACK {SLACK})",
        w.peak,
        CHARGED_CAP + MAX_FEEDER_SCRATCH_BYTES + SLACK,
    );
    assert!(
        probe.scratch_capacity_bytes() <= MAX_FEEDER_SCRATCH_BYTES,
        "{case}: carried capacity {} exceeds the bound after the tail",
        probe.scratch_capacity_bytes()
    );
    eprintln!(
        "{case}: narrow-tail peak {}, peak_charged {}, carried {}",
        w.peak,
        probe.peak_charged(),
        probe.scratch_capacity_bytes()
    );
}

/// Case F — a wide STRUCTURED-METADATA row (W = 65 536 entries, far past
/// `MAX_FEEDER_SCRATCH_SLOTS = 4096`) then a 200-row narrow no-SM tail:
/// what the feeder carries into the tail is bounded, cohort-verified.
#[test]
fn case_f_wide_sm_row_does_not_bloat_the_carried_scratch() {
    let _guard = lock_serial();
    let compiled = compile(r#"{app="x"}"#);
    let mut probe = DetectedFieldsProbe::with_byte_budget(10_000, 8, MAX_DETECTED_FIELD_BYTES);
    probe.add_stream(1, &[("app".to_string(), "x".to_string())]);
    let sm = wide_sm_json(65_536);
    probe
        .feed_row(&compiled, 1, 2_000_000, "wide row body", &sm)
        .expect("wide row");
    drop(sm);
    assert_narrow_tail_bound(&mut probe, &compiled, "case F");
}

/// Case G — a wide flat-JSON BODY (W = 65 536 keys) under `| json` then
/// the same narrow tail: the pipeline-extraction scratch is bounded too.
#[test]
fn case_g_wide_json_body_does_not_bloat_the_carried_scratch() {
    let _guard = lock_serial();
    let compiled = compile(r#"{app="x"} | json"#);
    let mut probe = DetectedFieldsProbe::with_byte_budget(10_000, 8, MAX_DETECTED_FIELD_BYTES);
    probe.add_stream(1, &[("app".to_string(), "x".to_string())]);
    let body = wide_sm_json(65_536);
    probe
        .feed_row(&compiled, 1, 2_000_000, &body, "")
        .expect("wide row");
    drop(body);
    assert_narrow_tail_bound(&mut probe, &compiled, "case G");
}

// ---------------------------------------------------------------------
// AC 13 — claim C2 (Q1+Q2, plan §A): the helper-level before/after.
// ---------------------------------------------------------------------

const WARMUP: usize = 3;
/// One of the three per-row spines the legacy shape allocates, derived
/// from the fixture's declared width and `size_of` — floored at 65 536.
const K_SHAPE: usize = 2048;
const LEGACY_DELTA_FLOOR: u64 = {
    let derived = (K_SHAPE * size_of::<(String, String)>()) as u64;
    if derived > 65_536 { derived } else { 65_536 }
};

struct ShapeRun {
    peak_legacy: u64,
    peak_new_1: u64,
    peak_new_2: u64,
}

/// Runs one row shape through BOTH helpers over identically-prepared
/// probe state: `WARMUP` discarded feeds per path (initialising the
/// `LazyLock` parsers and bringing the buffers to steady-state capacity),
/// then one measured window each; `peak_new_2` is a SECOND window on the
/// SAME probe, used only by the 13(d) negative control.
fn run_shape(query: &str, body: &str, sm: &str, case: &str) -> ShapeRun {
    let compiled = compile(query);
    let base = [("app".to_string(), "x".to_string())];

    let mut legacy = DetectedFieldsProbe::new(1000, 5000);
    legacy.add_stream(1, &base);
    for _ in 0..WARMUP {
        legacy
            .feed_row_legacy_shape(&compiled, 1, 5, body, sm)
            .expect("legacy warm-up");
    }
    let (_, w_legacy) = measure(|| legacy.feed_row_legacy_shape(&compiled, 1, 5, body, sm));
    assert_no_overflow(case);

    let mut newp = DetectedFieldsProbe::new(1000, 5000);
    newp.add_stream(1, &base);
    for _ in 0..WARMUP {
        newp.feed_row(&compiled, 1, 5, body, sm)
            .expect("new warm-up");
    }
    let (_, w_new_1) = measure(|| newp.feed_row(&compiled, 1, 5, body, sm));
    assert_no_overflow(case);
    let (_, w_new_2) = measure(|| newp.feed_row(&compiled, 1, 5, body, sm));
    assert_no_overflow(case);

    eprintln!(
        "{case}: peak_legacy {} peak_new_1 {} peak_new_2 {}",
        w_legacy.peak, w_new_1.peak, w_new_2.peak
    );
    ShapeRun {
        peak_legacy: w_legacy.peak,
        peak_new_1: w_new_1.peak,
        peak_new_2: w_new_2.peak,
    }
}

fn flat_json_body(k: usize) -> String {
    let mut s = String::with_capacity(k * 16 + 2);
    s.push('{');
    for i in 0..k {
        if i > 0 {
            s.push(',');
        }
        write!(s, r#""f{i:05}":"w{i:05}""#).expect("write");
    }
    s.push('}');
    s
}

/// The 13(a)/(b) improvement gates for the three improving shapes.
fn assert_improved(r: &ShapeRun, case: &str) {
    assert!(
        r.peak_legacy > r.peak_new_1,
        "{case}: peak_legacy {} must strictly exceed peak_new_1 {} — the shape change removed \
         per-row owned copies (C2 (Q1+Q2), #244 plan §A)",
        r.peak_legacy,
        r.peak_new_1
    );
    assert!(
        r.peak_legacy - r.peak_new_1 >= LEGACY_DELTA_FLOOR,
        "{case}: delta {} under LEGACY_DELTA_FLOOR {LEGACY_DELTA_FLOOR}",
        r.peak_legacy - r.peak_new_1
    );
    assert_stable(r, case);
}

/// 13(d) — the negative control, and the harness fault it catches, named:
/// a difference produced by ONE-TIME initialisation (`LazyLock` parser
/// compilation and first-touch growth of the recycled buffers) being read
/// as a per-row shape difference — i.e. the harness having compared a
/// COLD run against a WARM one. Demonstrated firing under H1 (the
/// warm-up loops deleted).
fn assert_stable(r: &ShapeRun, case: &str) {
    let delta = r.peak_new_1.abs_diff(r.peak_new_2);
    assert!(
        delta < LEGACY_DELTA_FLOOR / 4,
        "{case}: |peak_new_1 - peak_new_2| = {delta} >= {} — the harness compared a cold run \
         against a warm one (one-time LazyLock/first-touch cost read as a shape difference)",
        LEGACY_DELTA_FLOOR / 4
    );
}

/// Shape (i) — flat escape-free JSON body of K = 2048 keys, no SM.
#[test]
fn ac13_shape_i_flat_json_is_not_worse_at_helper_granularity() {
    let _guard = lock_serial();
    let body = flat_json_body(K_SHAPE);
    let r = run_shape(r#"{app="x"} | json"#, &body, "", "shape (i)");
    assert_improved(&r, "shape (i)");
}

/// Shape (ii) — the same body with a 4 096-entry SM string (exercises the
/// D1 re-parse).
#[test]
fn ac13_shape_ii_json_with_wide_sm_is_not_worse_at_helper_granularity() {
    let _guard = lock_serial();
    let body = flat_json_body(K_SHAPE);
    let sm = wide_sm_json(4096);
    let r = run_shape(r#"{app="x"} | json"#, &body, &sm, "shape (ii)");
    assert_improved(&r, "shape (ii)");
}

/// Shape (iii) — an 8 KiB non-parseable body, no SM (D2's O(1) floor
/// path): EXACT equality, not improvement — on this shape the two paths
/// genuinely allocate the same amount (the legacy `Vec::new()` never
/// grows, its empty collect allocates nothing, the recycled scratch never
/// grows, and the json-attempt's boxed `serde_json::Error` is an ABSOLUTE
/// cost on both sides, not a legacy-minus-new delta). Pre-committed: an
/// inequality either way is recorded and ESCALATED — it would mean the
/// paths differ where the #244 plan says they do not.
#[test]
fn ac13_shape_iii_non_parseable_body_is_exactly_equal_at_helper_granularity() {
    let _guard = lock_serial();
    let body = "z".repeat(8192);
    let r = run_shape(r#"{app="x"}"#, &body, "", "shape (iii)");
    assert_eq!(
        r.peak_legacy, r.peak_new_1,
        "shape (iii): expected EXACT equality; peaks {} vs {} — ESCALATE with both numbers \
         (an inequality either way means the paths differ where the plan says they do not)",
        r.peak_legacy, r.peak_new_1
    );
    assert_stable(&r, "shape (iii)");
}

/// Shape (iv) — the same JSON body under a `line_format`-rewritten
/// pipeline: under the helper-level comparison it is simply another
/// input.
#[test]
fn ac13_shape_iv_line_format_rewrite_is_not_worse_at_helper_granularity() {
    let _guard = lock_serial();
    let body = flat_json_body(K_SHAPE);
    let r = run_shape(
        r#"{app="x"} | line_format "{{.app}} {{__line__}}""#,
        &body,
        "",
        "shape (iv)",
    );
    assert_improved(&r, "shape (iv)");
}

// ---------------------------------------------------------------------
// AC 19 — the registered cardinality divergence artifact.
// ---------------------------------------------------------------------

/// Every TSV row: (a) our cardinality is EXACT (`cardinality ==
/// pulsus_exact == n_distinct`, recomputed through the production
/// accumulator with the capture's own `"v0".."v{n-1}"` values); (b) the
/// captured reference estimate DIFFERS; (c) the `ledger_id` occurs
/// verbatim in the committed ledger, which names #244 and #261; (d) the
/// mandatory `5328 / 5327` first-divergence row is present.
#[test]
fn reference_divergence_tsv_rows_hold_and_the_ledger_names_them() {
    let tsv = include_str!("golden/detected_cardinality/reference_divergence.tsv");
    let ledger_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/benchmarks/logs-differential-ledger.md"
    );
    let ledger = std::fs::read_to_string(ledger_path).expect("ledger readable");
    assert!(ledger.contains("#244") && ledger.contains("#261"));
    let mut saw_mandatory = false;
    let mut rows = 0usize;
    for line in tsv.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        rows += 1;
        let cols: Vec<&str> = line.split('\t').collect();
        assert_eq!(cols.len(), 5, "row {line:?}");
        let n_distinct: u64 = cols[0].parse().expect("n_distinct");
        let reference_estimate: u64 = cols[1].parse().expect("reference_estimate");
        let pulsus_exact: u64 = cols[2].parse().expect("pulsus_exact");
        let ledger_id = cols[3];
        assert_eq!(pulsus_exact, n_distinct, "(a) we are exact: {line:?}");
        assert_ne!(
            reference_estimate, pulsus_exact,
            "(b) a kept row must DISAGREE: {line:?}"
        );
        assert!(
            ledger.contains(ledger_id),
            "(c) ledger is missing {ledger_id:?}"
        );
        // (a) recomputed through the production accumulator.
        let mut probe = DetectedFieldsProbe::with_byte_budget(100, 1000, MAX_DETECTED_FIELD_BYTES);
        let mut value = String::with_capacity(16);
        for i in 0..n_distinct {
            value.clear();
            write!(value, "v{i}").expect("write");
            probe.observe_pair("uid", &value, None);
        }
        let (fields, capped) = probe.finish();
        assert!(!capped);
        assert_eq!(fields.len(), 1);
        assert_eq!(
            fields[0].cardinality, n_distinct,
            "(a) exact through the accumulator: {line:?}"
        );
        if n_distinct == 5328 {
            assert_eq!(reference_estimate, 5327);
            saw_mandatory = true;
        }
    }
    assert!(rows >= 1, "the artifact must be non-empty");
    assert!(saw_mandatory, "(d) the mandatory 5328/5327 row is required");
}

// ---------------------------------------------------------------------
// AC 14 — the demoted frame census. EXPLANATORY ONLY: this proves
// nothing and no claim rests on it; it exists solely so the #244 plan's
// §6.1 where-the-allocation-arises account cannot silently rot — a
// change to any frame's callee multiset reddens this until §6.1 (and
// this pin) is updated. If the closure exceeds ~30 frames, report the
// list and ESCALATE the scope rather than trimming it.
// ---------------------------------------------------------------------

mod census {
    use std::collections::BTreeMap;

    #[derive(Default)]
    pub struct Census {
        pub callees: BTreeMap<String, usize>,
    }

    impl<'ast> syn::visit::Visit<'ast> for Census {
        fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
            if let syn::Expr::Path(p) = &*node.func {
                if let Some(seg) = p.path.segments.last() {
                    *self.callees.entry(seg.ident.to_string()).or_default() += 1;
                }
            } else {
                *self.callees.entry("<dyn-call>".to_string()).or_default() += 1;
            }
            syn::visit::visit_expr_call(self, node);
        }
        fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
            *self.callees.entry(format!(".{}", node.method)).or_default() += 1;
            syn::visit::visit_expr_method_call(self, node);
        }
        fn visit_macro(&mut self, node: &'ast syn::Macro) {
            if let Some(seg) = node.path.segments.last() {
                *self.callees.entry(format!("{}!", seg.ident)).or_default() += 1;
            }
            if let Ok(exprs) = node.parse_body_with(
                syn::punctuated::Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated,
            ) {
                for e in &exprs {
                    self.visit_expr(e);
                }
            }
        }
    }
}

/// `(file, impl type, fn name)` — a census frame's anchor.
type FrameKey = (&'static str, Option<&'static str>, &'static str);

/// The DECLARED closure of `DetectedRowFeeder::feed_row` (13 frames,
/// under the ~30 cap).
const FRAMES: [FrameKey; 13] = [
    ("exec.rs", Some("DetectedRowFeeder"), "feed_row"),
    ("exec.rs", Some("DetectedRowFeeder"), "trim"),
    ("exec.rs", None, "observe_detected_row"),
    ("exec.rs", None, "auto_parse_observe"),
    ("exec.rs", None, "merge_labels_with_structured_metadata"),
    ("exec.rs", None, "parse_flat_labels_into"),
    ("exec.rs", None, "recycle_label_scratch"),
    ("detected.rs", Some("FieldAccumulator"), "observe_pair"),
    ("detected.rs", None, "observe_admitted"),
    ("detected.rs", None, "field_entry_bytes"),
    ("detected.rs", None, "value_entry_bytes"),
    ("detected.rs", None, "auto_parse_into"),
    ("detected.rs", None, "determine_type"),
];

fn parse_frame_source(name: &str) -> syn::File {
    let src = match name {
        "exec.rs" => include_str!("../src/logql/exec.rs"),
        "detected.rs" => include_str!("../src/logql/detected.rs"),
        other => panic!("unknown frame file {other}"),
    };
    syn::parse_file(src).expect("the frame source parses")
}

fn frame_body<'a>(file: &'a syn::File, ty: Option<&str>, name: &str) -> &'a syn::Block {
    let mut hits: Vec<&syn::Block> = Vec::new();
    match ty {
        None => {
            for item in &file.items {
                if let syn::Item::Fn(func) = item
                    && func.sig.ident == name
                {
                    hits.push(&func.block);
                }
            }
        }
        Some(ty) => {
            for item in &file.items {
                if let syn::Item::Impl(imp) = item {
                    let self_ty = match &*imp.self_ty {
                        syn::Type::Path(p) => p.path.segments.last().map(|s| s.ident.to_string()),
                        _ => None,
                    };
                    if self_ty.as_deref() != Some(ty) {
                        continue;
                    }
                    for ii in &imp.items {
                        if let syn::ImplItem::Fn(func) = ii
                            && func.sig.ident == name
                        {
                            hits.push(&func.block);
                        }
                    }
                }
            }
        }
    }
    assert_eq!(hits.len(), 1, "frame {ty:?}::{name} must resolve uniquely");
    hits[0]
}

fn frame_census(file: &str, ty: Option<&str>, name: &str) -> String {
    use syn::visit::Visit;
    let ast = parse_frame_source(file);
    let mut c = census::Census::default();
    c.visit_block(frame_body(&ast, ty, name));
    let mut out = String::new();
    for (callee, count) in &c.callees {
        if !out.is_empty() {
            out.push(' ');
        }
        write!(out, "{callee}x{count}").expect("write");
    }
    out
}

/// Prints the current censuses in the pinned format (generator for the
/// table below).
#[test]
#[ignore = "generator: prints the frame censuses to pin"]
fn zz_print_frame_censuses() {
    for (file, ty, name) in FRAMES {
        println!(
            "(\"{file}\", {ty:?}, \"{name}\") => {:?}",
            frame_census(file, ty, name)
        );
    }
}

#[test]
fn ac14_frame_census_pins_the_explanatory_account() {
    let expected: HashMap<FrameKey, &str> = HashMap::from(EXPECTED_CENSUS);
    assert!(
        FRAMES.len() <= 30,
        "the declared closure exceeded ~30 frames — report the list and escalate the scope"
    );
    for (file, ty, name) in FRAMES {
        let got = frame_census(file, ty, name);
        let want = expected
            .get(&(file, ty, name))
            .unwrap_or_else(|| panic!("frame {ty:?}::{name} has no pinned census"));
        assert_eq!(
            &got.as_str(),
            want,
            "frame {ty:?}::{name} changed — update the #244 plan §6.1 account AND this pin \
             together (this census is EXPLANATORY ONLY; no claim rests on it)"
        );
    }
}

/// The pinned callee multisets (regenerate with `zz_print_frame_censuses`).
#[rustfmt::skip]
const EXPECTED_CENSUS: [(FrameKey, &str); 13] = [
    (("exec.rs", Some("DetectedRowFeeder"), "feed_row"),
     ".getx1 .is_emptyx1 .trimx1 Errx1 Okx2 Somex1 merge_labels_with_structured_metadatax1 observe_detected_rowx1 takex1"),
    (("exec.rs", Some("DetectedRowFeeder"), "trim"),
     ".capacityx2 .clearx2 newx2 trim_strx2 trim_vecx3"),
    (("exec.rs", None, "observe_detected_row"),
     ".anyx1 .as_refx4 .as_strx1 .clearx3 .intox1 .iterx3 .observe_pairx2 .run_into_with_smx1 Errx1 Okx2 auto_parse_observex1 parse_flat_labels_intox1 recycle_label_scratchx2"),
    (("exec.rs", None, "auto_parse_observe"),
     ".as_refx2 .clearx1 .iterx1 .observe_pairx1 Somex1 auto_parse_intox1 recycle_label_scratchx1"),
    (("exec.rs", None, "merge_labels_with_structured_metadata"),
     ".anyx1 .clearx4 .clonedx1 .drainx1 .extendx1 .findx1 .is_emptyx1 .iterx2 .iter_mutx1 .lenx1 .pushx1 .push_strx1 parse_flat_labels_intox1"),
    (("exec.rs", None, "parse_flat_labels_into"),
     ".charsx1 .nextx3 .peekx3 .peekablex1 .pushx1 Somex1 parse_json_stringx2 skip_wsx3"),
    (("exec.rs", None, "recycle_label_scratch"),
     ".clearx1 .collectx1 .into_iterx1 .into_ownedx2 .mapx1 Ownedx2"),
    (("detected.rs", Some("FieldAccumulator"), "observe_pair"),
     ".chargex1 .contains_keyx1 .get_mutx1 .insertx1 .lenx1 .to_stringx1 field_entry_bytesx1 newx1 observe_admittedx1 with_capacityx1"),
    (("detected.rs", None, "observe_admitted"),
     ".chargex1 .containsx2 .insertx1 .pushx1 .to_stringx1 determine_typex1 value_entry_bytesx1"),
    (("detected.rs", None, "field_entry_bytes"),
     ".lenx1 .saturating_addx2 alloc_block_bytesx1 grown_alloc_bytesx1 map_entry_bytesx1 size_ofx2"),
    (("detected.rs", None, "value_entry_bytes"),
     ".lenx1 .saturating_addx1 alloc_block_bytesx1 map_entry_bytesx1 size_ofx1"),
    (("detected.rs", None, "auto_parse_into"),
     ".clearx1 .is_somex1 .run_into_reporting_errx1 .unwrap_orx1 Somex1"),
    (("detected.rs", None, "determine_type"),
     ".containsx1 .is_okx2 .is_somex2 .parsex2 parse_bytes_valuex1 parse_duration_secondsx1"),
];
