//! Issue #236 §5 — the post-aggregation allocation **witness**.
//!
//! The byte model this file derives (`post_agg_peak_bytes` /
//! `binary_peak_bytes` in `logql/exec.rs`) is not an enumeration of
//! containers. Ten review rounds established that enumerating the
//! post-aggregation stage's live containers produces clean arithmetic
//! over the wrong set: seven separate revisions omitted something live (a
//! second concurrent stage buffer, a second point-scaled container, a
//! fourth/fifth/sixth/seventh simultaneously-live owned label vector, a
//! realloc chain). So the stage's byte behaviour is **measured** here and
//! the shipped coefficients are the measured per-unit rates times a
//! stated margin. A witness cannot omit a live value, because it does not
//! enumerate values — it observes the allocator.
//!
//! # THE COHORT ATTRIBUTION RULE
//!
//! (Named so it can be cited: issue #236 §8 C6 makes this the standard
//! for allocation witnesses in this repo, and #281 exists because
//! `logql_variants_alloc.rs` carries the masking class below in a
//! different spelling — a process-global `LIVE` reset at window open,
//! decremented on every dealloc including pointers it never allocated,
//! with `PEAK` as `fetch_max` over it.)
//!
//! * an allocation belongs to the window that was open, **on the
//!   measuring thread**, when its pointer was returned;
//! * a free is counted **only** against the window that owns that pointer
//!   — a free of a pointer owned by no open window is ignored ENTIRELY,
//!   so dropping the stage's pre-window input neither raises nor lowers
//!   any measured quantity;
//! * a realloc is a free of the old pointer plus an allocation of the new
//!   one, attributed by the same rule (an in-place realloc that returns
//!   the same pointer is handled remove-then-insert);
//! * allocations from any other thread are ignored.
//!
//! Therefore [`Window::peak`] is exactly "the maximum bytes this window
//! itself held live" and [`Window::retained`] is exactly "the bytes this
//! window allocated that are still live at close" — neither can be masked
//! by unrelated frees.
//!
//! **Why the obvious instrument is unsound here.** A naive
//! `LIVE = Σalloc − Σdealloc` high-water mark lets allocations made
//! *before* the window be freed *during* it, decrementing `LIVE` and
//! deflating the observed peak. `apply_vector_aggs` takes `QueryResult`
//! **by value** and drops its (large, pre-window) input inside the
//! window, so that is not a corner case here — it is the common case. The
//! same flaw invalidates any bulk `freed >= allocated − residue`
//! retention identity. [`Window::bulk_peak`]/[`Window::bulk_live_at_close`]
//! carry the naive quantities so the discrimination tests I1–I4 can show
//! the two instruments disagree, rather than asserting that they would.
//!
//! # What this file gates
//!
//! | gate | what it proves |
//! |---|---|
//! | I1–I4 | the instrument itself is sound and fails loudly |
//! | the cell matrix | every construct has a measured fixture, enforced by exhaustive `match`es with no `_` arm |
//! | the per-cell safety gate | `measured peak <= modelled bytes` for EVERY cell |
//! | the ladders | each coefficient is `WITNESS_MARGIN x rate_max` over a >= 64x span, and the stage is observed linear |
//! | the paired fixtures | each coefficient is NECESSARY — the model without it fails to cover the increment the pair causes |
//! | the derivation | `max(X_chain, X_bin) <= MAX_POST_AGG_BYTES` over the leaf-gated feasible region at the non-amplifying corner |
//!
//! **What the cap does NOT claim.** It is not a worst-case proof. It is
//! "bounded by a measured-and-margined rate over a compile-enforced
//! construct space, with a clean refusal instead of an OOM at the
//! boundary". The residual is named in §7 of the plan and in
//! `MAX_POST_AGG_BYTES`' doc comment.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use pulsus_logql::{BinOp, Grouping, GroupingKind, MatchGroup, VectorAggOp, VectorMatching};
use pulsus_read::logql::MAX_CLIENT_AGG_BUCKETS;
use pulsus_read::logql::plan::VectorAggSpec;
use pulsus_read::logql::{
    B_INCLUDE, B_LABEL, B_MANY, B_PAIR, B_POINT, B_SERIES, BinaryTerm, ChainTerm, LabelReplaceSpec,
    MAX_POST_AGG_BYTES, MatrixSeries, QueryResult, StageInput, VectorSample, W_APPROX_TOPK,
    W_GROUPNAME, W_LABEL_BYTE, W_PAIR, W_POINT, W_SERIES, W_STAGE_SERIES, apply_vector_aggs,
    apply_vector_aggs_capped, binary_peak_bytes, binary_peak_bytes_without, combine_binary,
    combine_binary_capped, group_name_bytes, include_bytes, label_replace_peak_bytes,
    leaf_min_entry_bytes, measure_matrix, measure_vector, post_agg_peak_bytes,
    post_agg_peak_bytes_without,
};
use pulsus_read::logql::{Direction, PlanCtx, QueryParams, QuerySpec, ReadError, TooBroadReason};
use pulsus_read::logql::{MAX_CLIENT_AGG_GROUP_BYTES, MAX_METRIC_RESULT_POINTS};

// =====================================================================
// 1. The instrument
// =====================================================================

/// Fixed-capacity, allocation-free, open-addressed `ptr -> size` table.
/// The allocator hook must never allocate (re-entrancy), so this is a
/// `static` array of atomics, never a `HashMap`.
const COHORT_SLOTS: usize = 1 << 21;
const COHORT_MASK: usize = COHORT_SLOTS - 1;
/// An insert that cannot find a free slot within this many probes sets
/// [`OVERFLOW`], which every gate asserts is clear — the instrument FAILS
/// LOUDLY rather than degrading silently (I4).
const PROBE_BOUND: usize = 128;

static SLOT_KEY: [AtomicUsize; COHORT_SLOTS] = [const { AtomicUsize::new(0) }; COHORT_SLOTS];
static SLOT_SIZE: [AtomicU64; COHORT_SLOTS] = [const { AtomicU64::new(0) }; COHORT_SLOTS];
/// A slot belongs to the CURRENT window only if its generation matches
/// [`GENERATION`]. Opening a window bumps the generation, so the reset is
/// `O(1)` rather than a 1 MiB-entry clear per measured cell — with ~600
/// cells that difference is the whole binary's time budget.
static SLOT_GEN: [AtomicU64; COHORT_SLOTS] = [const { AtomicU64::new(0) }; COHORT_SLOTS];
static GENERATION: AtomicU64 = AtomicU64::new(1);
static OVERFLOW: AtomicBool = AtomicBool::new(false);

static W_BYTES: AtomicU64 = AtomicU64::new(0);
static W_COUNT: AtomicU64 = AtomicU64::new(0);
static W_LIVE: AtomicU64 = AtomicU64::new(0);
static W_PEAK: AtomicU64 = AtomicU64::new(0);
/// The NAIVE quantities — kept only so I1/I3 can show the two
/// instruments disagree. Never used by a model gate.
static BULK_LIVE: AtomicU64 = AtomicU64::new(0);
static BULK_PEAK: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// `const`-initialised and `Drop`-free, so reading it registers no
    /// destructor and cannot allocate — a requirement, not an
    /// optimisation, since this is read from inside the allocator.
    static IN_WINDOW: Cell<bool> = const { Cell::new(false) };
}

/// Every test in this binary serialises here: the counters and the cohort
/// table are process-global statics (the `logql_variants_alloc.rs:210`
/// precedent).
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn in_window() -> bool {
    IN_WINDOW.try_with(Cell::get).unwrap_or(false)
}

fn slot_of(ptr: usize) -> usize {
    // Multiply-shift over the pointer with its allocation-alignment bits
    // dropped; no allocation, no division.
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

/// Removes `ptr` from the cohort, returning its size iff THIS window owns
/// it. Deletion re-inserts the rest of the probe cluster (the classic
/// linear-probing fix), so a lookup can never be cut short by a hole.
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

fn on_alloc(ptr: usize, size: u64) {
    table_insert(ptr, size);
    W_BYTES.fetch_add(size, Ordering::Relaxed);
    W_COUNT.fetch_add(1, Ordering::Relaxed);
    let live = W_LIVE.fetch_add(size, Ordering::Relaxed) + size;
    W_PEAK.fetch_max(live, Ordering::Relaxed);
    let bulk = BULK_LIVE.fetch_add(size, Ordering::Relaxed) + size;
    BULK_PEAK.fetch_max(bulk, Ordering::Relaxed);
}

fn on_free(ptr: usize, size: u64) {
    if let Some(owned) = table_remove(ptr) {
        W_LIVE.fetch_sub(owned, Ordering::Relaxed);
    }
    // The naive counter decrements on EVERY free, owned or not, and
    // saturates at zero — the masking class this instrument exists to
    // avoid, reproduced verbatim so I1/I3 can measure the difference.
    let mut cur = BULK_LIVE.load(Ordering::Relaxed);
    loop {
        let next = cur.saturating_sub(size);
        match BULK_LIVE.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(seen) => cur = seen,
        }
    }
}

struct CohortAlloc;

// SAFETY: every method delegates verbatim to the system allocator and the
// bookkeeping runs only after the underlying call has produced (or is
// about to release) the pointer. The bookkeeping itself touches nothing
// but `static` atomics and a `Drop`-free, `const`-initialised
// thread-local, so it cannot re-enter the allocator.
unsafe impl GlobalAlloc for CohortAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() && in_window() {
            on_alloc(p as usize, layout.size() as u64);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if in_window() {
            on_free(ptr as usize, layout.size() as u64);
        }
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if in_window() {
            // Remove-then-insert, so an IN-PLACE realloc returning the
            // same pointer is attributed correctly rather than dropped.
            on_free(ptr as usize, layout.size() as u64);
            if !p.is_null() {
                on_alloc(p as usize, new_size as u64);
            }
        }
        p
    }
}

#[global_allocator]
static ALLOCATOR: CohortAlloc = CohortAlloc;

/// One measured window.
#[derive(Clone, Copy, Debug)]
struct Window {
    /// Total bytes this window allocated (retained or not).
    bytes: u64,
    /// Allocation events attributed to this window.
    count: u64,
    /// **The load-bearing quantity**: the maximum bytes this window
    /// itself held live.
    peak: u64,
    /// Bytes this window allocated that are still live at close.
    retained: u64,
    /// The naive high-water mark (see the module doc) — never gates.
    bulk_peak: u64,
    /// The naive `LIVE` at close — never gates.
    bulk_live_at_close: u64,
    /// Set iff the cohort table ran out of probe room. Every gate asserts
    /// this is clear.
    overflow: bool,
}

/// Opens a cohort, runs `f`, closes it, and returns `f`'s value **alive**
/// (so anything the call retains is inside [`Window::retained`]). The
/// fixture MUST be built by the caller, outside `f`.
fn measure<T>(f: impl FnOnce() -> T) -> (T, Window) {
    GENERATION.fetch_add(1, Ordering::SeqCst);
    OVERFLOW.store(false, Ordering::SeqCst);
    W_BYTES.store(0, Ordering::SeqCst);
    W_COUNT.store(0, Ordering::SeqCst);
    W_LIVE.store(0, Ordering::SeqCst);
    W_PEAK.store(0, Ordering::SeqCst);
    BULK_LIVE.store(0, Ordering::SeqCst);
    BULK_PEAK.store(0, Ordering::SeqCst);

    IN_WINDOW.with(|c| c.set(true));
    let out = f();
    IN_WINDOW.with(|c| c.set(false));

    let w = Window {
        bytes: W_BYTES.load(Ordering::SeqCst),
        count: W_COUNT.load(Ordering::SeqCst),
        peak: W_PEAK.load(Ordering::SeqCst),
        retained: W_LIVE.load(Ordering::SeqCst),
        bulk_peak: BULK_PEAK.load(Ordering::SeqCst),
        bulk_live_at_close: BULK_LIVE.load(Ordering::SeqCst),
        overflow: OVERFLOW.load(Ordering::SeqCst),
    };
    (out, w)
}

const MIB: usize = 1024 * 1024;

// =====================================================================
// 2. Instrument discrimination — I1..I4 (AC 23)
// =====================================================================

/// I1 — an 8 MiB buffer allocated BEFORE the window is dropped INSIDE it,
/// while the window retains 1 MiB of its own. The cohort instrument
/// reports exactly the 1 MiB it owns; the naive one reports zero live at
/// close, so its `freed >= allocated - residue` retention identity
/// "passes" while 1 MiB is still retained.
#[test]
fn i1_a_pre_window_free_cannot_mask_what_the_window_retains() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let outside: Vec<u8> = vec![7u8; 8 * MIB];

    let (kept, w) = measure(|| {
        let inside: Vec<u8> = vec![3u8; MIB];
        drop(outside);
        inside
    });

    assert!(!w.overflow, "cohort table overflowed");
    assert!(
        w.retained >= MIB as u64 && w.retained < 2 * MIB as u64,
        "the cohort must retain exactly the window's own 1 MiB: {w:?}"
    );
    assert!(
        w.peak >= MIB as u64 && w.peak < 2 * MIB as u64,
        "the pre-window free must neither raise nor lower the peak: {w:?}"
    );
    // The naive quantity, computed by the same run, DIFFERS — so
    // reverting this instrument to bulk arithmetic reddens this test.
    assert_eq!(
        w.bulk_live_at_close, 0,
        "the naive counter is expected to saturate to zero here (that is the defect)"
    );
    assert_ne!(
        w.retained, w.bulk_live_at_close,
        "cohort and bulk retention must differ on this scenario: {w:?}"
    );
    assert_eq!(kept.len(), MIB);
}

/// I2 — the case a bulk instrument gets right: allocate inside, drop
/// inside. Both agree, and the cohort's `retained` is zero.
#[test]
fn i2_an_in_window_allocation_dropped_in_window_retains_nothing() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let ((), w) = measure(|| {
        let v: Vec<u8> = vec![1u8; MIB];
        assert_eq!(v.len(), MIB);
        drop(v);
    });
    assert!(!w.overflow, "cohort table overflowed");
    assert!(
        w.peak >= MIB as u64 && w.peak < 2 * MIB as u64,
        "peak must see the in-window MiB: {w:?}"
    );
    assert_eq!(w.retained, 0, "nothing may be retained: {w:?}");
    assert_eq!(w.bulk_peak, w.peak, "bulk and cohort agree here: {w:?}");
    assert!(
        w.count >= 1 && w.bytes >= w.peak,
        "the window's own totals must dominate its peak: {w:?}"
    );
}

/// I3 — a `Vec` allocated BEFORE the window and grown INSIDE it: the new
/// buffer is charged, the old one is ignored. The naive counter's
/// decrement for the old buffer deflates its peak below the cohort's.
#[test]
fn i3_growing_a_pre_window_buffer_charges_the_new_block_and_ignores_the_old() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut outside: Vec<u8> = Vec::with_capacity(8 * MIB);
    outside.push(1);

    let ((held, grown), w) = measure(|| {
        let held: Vec<u8> = vec![9u8; 4 * MIB];
        outside.reserve(16 * MIB);
        (held, outside)
    });

    assert!(!w.overflow, "cohort table overflowed");
    assert!(
        w.peak >= 20 * MIB as u64,
        "both in-window blocks (4 MiB held + the >=16 MiB new buffer) must be live at the peak: {w:?}"
    );
    assert!(
        w.bulk_peak < w.peak,
        "the naive counter's decrement for the pre-window buffer must deflate its peak \
         below the cohort's — that is the masking class: {w:?}"
    );
    assert_eq!(held.len(), 4 * MIB);
    assert!(grown.capacity() >= 16 * MIB);
}

/// I4 — more distinct live in-window allocations than the cohort table
/// can hold: `OVERFLOW` is set and the gate fails, rather than the
/// instrument silently under-counting.
#[test]
fn i4_exhausting_the_cohort_table_fails_loudly() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (boxes, w) = measure(|| {
        let mut boxes: Vec<Box<u64>> = Vec::with_capacity(COHORT_SLOTS + 16);
        for i in 0..(COHORT_SLOTS + 16) {
            boxes.push(Box::new(i as u64));
        }
        boxes
    });
    assert_eq!(boxes.len(), COHORT_SLOTS + 16);
    assert!(
        w.overflow,
        "more live allocations than COHORT_SLOTS must set OVERFLOW: {w:?}"
    );
}

// =====================================================================
// 3. The compile-enforced cell key (§5.3)
// =====================================================================

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Shape {
    Instant,
    Range,
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum GroupShape {
    NoGrouping,
    ByPresent,
    ByAbsent,
    Without,
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ChainShape {
    Single,
    NestedSameOp,
    NestedTwoOp,
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum KShape {
    NotParameterised,
    KOne,
    KAll,
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Driver {
    Direct,
    PerVariant,
}

/// Binary matching clause shape.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum MatchShape {
    NoMatching,
    On,
    Ignoring,
}
/// Binary group modifier.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BinGroup {
    NoGroup,
    GroupLeft,
    GroupRight,
}

/// Why a cell is not exercised. Every `Excluded` cell carries one of
/// these plus a source citation, and
/// [`every_exclusion_is_real`] checks the exclusion actually holds.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ExcludeReason {
    PlanRejectedForShape,
    PassthroughNoAllocation,
    OpTakesNoGrouping,
    OpTakesNoParameter,
    MatchingRequiredForGroupModifier,
}

// One exhaustive `match` per dimension enum, NO `_` arm — a new dimension
// variant is a BUILD FAILURE until it is dispositioned.
fn describe(s: Shape) -> &'static str {
    match s {
        Shape::Instant => "instant",
        Shape::Range => "range",
    }
}
fn describe_group(g: GroupShape) -> &'static str {
    match g {
        GroupShape::NoGrouping => "no grouping",
        GroupShape::ByPresent => "by(<present label>)",
        GroupShape::ByAbsent => "by(<absent label>)",
        GroupShape::Without => "without(<present label>)",
    }
}
fn describe_chain(c: ChainShape) -> &'static str {
    match c {
        ChainShape::Single => "single stage",
        ChainShape::NestedSameOp => "two stages, same op",
        ChainShape::NestedTwoOp => "two stages, sum over op",
    }
}
fn describe_k(k: KShape) -> &'static str {
    match k {
        KShape::NotParameterised => "no k",
        KShape::KOne => "k = 1",
        KShape::KAll => "k = N",
    }
}
fn describe_driver(d: Driver) -> &'static str {
    match d {
        Driver::Direct => "direct",
        Driver::PerVariant => "per variant",
    }
}
fn describe_matching(m: MatchShape) -> &'static str {
    match m {
        MatchShape::NoMatching => "no on/ignoring",
        MatchShape::On => "on(x)",
        MatchShape::Ignoring => "ignoring(pad)",
    }
}
fn describe_bin_group(g: BinGroup) -> &'static str {
    match g {
        BinGroup::NoGroup => "one-to-one",
        BinGroup::GroupLeft => "group_left",
        BinGroup::GroupRight => "group_right",
    }
}
fn describe_reason(r: ExcludeReason) -> &'static str {
    match r {
        ExcludeReason::PlanRejectedForShape => "the planner rejects this op for this shape",
        ExcludeReason::PassthroughNoAllocation => "the op is a passthrough for this shape",
        ExcludeReason::OpTakesNoGrouping => "the op rejects a grouping clause at parse",
        ExcludeReason::OpTakesNoParameter => "the op takes no parameter",
        ExcludeReason::MatchingRequiredForGroupModifier => {
            "a bare group_left/group_right without on/ignoring is a parse error"
        }
    }
}

const ALL_OPS: [VectorAggOp; 12] = [
    VectorAggOp::Sum,
    VectorAggOp::Avg,
    VectorAggOp::Min,
    VectorAggOp::Max,
    VectorAggOp::Count,
    VectorAggOp::Stddev,
    VectorAggOp::Stdvar,
    VectorAggOp::Topk,
    VectorAggOp::Bottomk,
    VectorAggOp::ApproxTopk,
    VectorAggOp::Sort,
    VectorAggOp::SortDesc,
];

const ALL_BIN_OPS: [BinOp; 15] = [
    BinOp::Add,
    BinOp::Sub,
    BinOp::Mul,
    BinOp::Div,
    BinOp::Mod,
    BinOp::Pow,
    BinOp::Eq,
    BinOp::Neq,
    BinOp::Gt,
    BinOp::Gte,
    BinOp::Lt,
    BinOp::Lte,
    BinOp::And,
    BinOp::Or,
    BinOp::Unless,
];

// =====================================================================
// 4. Fixtures
// =====================================================================

/// A stage-input shape, as plain data so the cell tables can be `const`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Fixture {
    /// Series in the operand.
    series: u64,
    /// Label pairs per series (>= 1: the `id` label).
    pairs: u64,
    /// Bytes per label value.
    value_bytes: u64,
    /// Points per series (a `Shape::Instant` operand always has 1).
    steps: u64,
    skew: Skew,
}

/// How the operand's mass is distributed across its series. `rate_max` is
/// taken over BOTH, because per-series `MIN_ALLOC` floors (uniform) and
/// large single buffers (concentrated) peak on different axes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Skew {
    /// Every series identically shaped.
    Uniform,
    /// The same TOTAL mass, all of it in series 0; the rest carry one
    /// pair and one point.
    Concentrated,
}

const CHAIN_BASE: Fixture = Fixture {
    series: 1024,
    pairs: 4,
    value_bytes: 8,
    steps: 8,
    skew: Skew::Uniform,
};

const BIN_BASE: Fixture = Fixture {
    series: 512,
    pairs: 4,
    value_bytes: 8,
    steps: 8,
    skew: Skew::Uniform,
};

/// A label value of EXACTLY `width` bytes. Truncating from the left
/// rather than letting a wide `n` overflow the width is load-bearing:
/// `label_bytes` must be exactly `series * pairs * (4 + value_bytes)` for
/// a paired fixture to hold it byte-identical while varying `pairs`.
fn pad(n: u64, width: u64) -> String {
    let w = width.max(1) as usize;
    let s = format!("{n:0w$}");
    if s.len() > w {
        s[s.len() - w..].to_string()
    } else {
        s
    }
}

/// One series' labels: `id00` plus `l001..`, all key-sorted (`i` < `l`),
/// all values series-distinct so `without(id00)` yields one group per
/// series.
///
/// Every key is FOUR bytes on purpose: `label_bytes` is then exactly
/// `series * pairs * (4 + value_bytes)`, so a paired fixture can hold it
/// byte-identical while varying `pairs` — with a shorter first key the
/// key mix shifts with the pair count and the isolation assertion fails
/// for a reason that has nothing to do with the term under test.
fn labels_for(
    series_ix: u64,
    pairs: u64,
    value_bytes: u64,
    variant: bool,
) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    if variant {
        out.push(("__variant__".to_string(), "0".to_string()));
    }
    out.push(("id00".to_string(), pad(series_ix, value_bytes)));
    for j in 1..pairs {
        // `j`-major, so the LAST digits carry the series index: `pad`
        // truncates from the left, and a series-major encoding would make
        // series 0 and series 10 share a value at four digits — which
        // silently collapses `without(...)` groups and flattens the very
        // axis the pair ladders vary.
        out.push((
            format!("l{j:03}"),
            pad(j.wrapping_mul(1_000).wrapping_add(series_ix), value_bytes),
        ));
    }
    out
}

/// The per-series `(pairs, steps)` shape at index `i` under `fx.skew`.
fn shape_at(fx: &Fixture, i: u64) -> (u64, u64) {
    match fx.skew {
        Skew::Uniform => (fx.pairs, fx.steps),
        Skew::Concentrated => {
            if i == 0 {
                (
                    fx.pairs.saturating_mul(fx.series),
                    fx.steps.saturating_mul(fx.series),
                )
            } else {
                (1, 1)
            }
        }
    }
}

fn build_vector(fx: &Fixture, variant: bool) -> Vec<VectorSample> {
    (0..fx.series)
        .map(|i| {
            let (pairs, _) = shape_at(fx, i);
            VectorSample {
                labels: labels_for(i, pairs, fx.value_bytes, variant),
                value: (i % 97) as f64 + 0.5,
            }
        })
        .collect()
}

fn build_matrix(fx: &Fixture, variant: bool) -> Vec<MatrixSeries> {
    (0..fx.series)
        .map(|i| {
            let (pairs, steps) = shape_at(fx, i);
            MatrixSeries {
                labels: labels_for(i, pairs, fx.value_bytes, variant),
                points: (0..steps)
                    .map(|k| (k as i64 * 60_000_000_000, (i % 97) as f64 + k as f64))
                    .collect(),
            }
        })
        .collect()
}

fn grouping_for(g: GroupShape) -> Option<Grouping> {
    match g {
        GroupShape::NoGrouping => None,
        GroupShape::ByPresent => Some(Grouping {
            kind: GroupingKind::By,
            labels: vec!["id00".to_string()],
        }),
        GroupShape::ByAbsent => Some(Grouping {
            kind: GroupingKind::By,
            labels: vec!["absent_from_the_data".to_string()],
        }),
        GroupShape::Without => Some(Grouping {
            kind: GroupingKind::Without,
            labels: vec!["id00".to_string()],
        }),
    }
}

fn param_for(k: KShape, series: u64) -> Option<f64> {
    match k {
        KShape::NotParameterised => None,
        KShape::KOne => Some(1.0),
        KShape::KAll => Some(series as f64),
    }
}

fn chain_aggs(op: VectorAggOp, key: &ChainKey, fx: &Fixture) -> Vec<VectorAggSpec> {
    let grouping = if key.driver == Driver::PerVariant {
        Some(Grouping {
            kind: GroupingKind::By,
            labels: vec!["__variant__".to_string()],
        })
    } else {
        grouping_for(key.group)
    };
    let param = param_for(key.k, fx.series);
    let inner = (op, grouping, param);
    match key.chain {
        ChainShape::Single => vec![inner],
        // `vector_aggs` is OUTER-first; `apply_vector_aggs` applies
        // `.rev()`, so the last element is the innermost stage.
        ChainShape::NestedSameOp => vec![inner.clone(), inner],
        ChainShape::NestedTwoOp => vec![(VectorAggOp::Sum, None, None), inner],
    }
}

/// Builds a chain cell's operand and aggregation chain. `scale` multiplies
/// the series count (the response check re-runs every cell at `2N`).
fn build_chain(
    op: VectorAggOp,
    key: &ChainKey,
    fx: &Fixture,
    scale: u64,
) -> (QueryResult, Vec<VectorAggSpec>) {
    let fx = Fixture {
        series: fx.series * scale,
        ..*fx
    };
    let variant = key.driver == Driver::PerVariant;
    let aggs = chain_aggs(op, key, &fx);
    let result = match key.shape {
        Shape::Instant => QueryResult::Vector(build_vector(&fx, variant)),
        Shape::Range => QueryResult::Matrix(build_matrix(&fx, variant)),
    };
    (result, aggs)
}

fn measure_result(result: &QueryResult) -> StageInput {
    match result {
        QueryResult::Vector(items) => measure_vector(items),
        QueryResult::Matrix(items) => measure_matrix(items),
        other => panic!("witness fixtures build only vectors and matrices, got {other:?}"),
    }
}

/// Measures one chain cell: the fixture is built OUTSIDE the window, the
/// stage input is measured outside it, and the returned result is dropped
/// after the window closes.
fn run_chain(
    op: VectorAggOp,
    key: &ChainKey,
    fx: &Fixture,
    scale: u64,
) -> (StageInput, Vec<VectorAggSpec>, Window) {
    let (result, aggs) = build_chain(op, key, fx, scale);
    let input = measure_result(&result);
    let (out, w) = measure(|| apply_vector_aggs(result, &aggs));
    drop(out.expect("a witness cell must be admitted; a refusal would read as a small peak"));
    (input, aggs, w)
}

// =====================================================================
// 5. The cells (§5.3) — exhaustive `match`es, no `_` arm
// =====================================================================

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct ChainKey {
    shape: Shape,
    group: GroupShape,
    chain: ChainShape,
    k: KShape,
    driver: Driver,
}

#[derive(Clone, Copy, Debug)]
enum CellKind {
    Exercised(Fixture),
    Excluded {
        reason: ExcludeReason,
        cite: &'static str,
    },
}

#[derive(Clone, Copy, Debug)]
struct ChainCell {
    key: ChainKey,
    cell: CellKind,
}

const fn ck(
    shape: Shape,
    group: GroupShape,
    chain: ChainShape,
    k: KShape,
    driver: Driver,
) -> ChainKey {
    ChainKey {
        shape,
        group,
        chain,
        k,
        driver,
    }
}

const fn exercised(key: ChainKey) -> ChainCell {
    ChainCell {
        key,
        cell: CellKind::Exercised(CHAIN_BASE),
    }
}

const fn excluded(key: ChainKey, reason: ExcludeReason, cite: &'static str) -> ChainCell {
    ChainCell {
        key,
        cell: CellKind::Excluded { reason, cite },
    }
}

const NO_PARAM_CITE: &str = "pulsus-logql/src/parser.rs — a parameter is parsed only for \
                             topk/bottomk/approx_topk/quantile_over_time";
const NO_GROUPING_CITE: &str = "pulsus-logql/src/ast.rs:907-912 (sort/sort_desc) and :901-906 \
                                (approx_topk): grouping is rejected at parse";
const APPROX_RANGE_CITE: &str = "crates/pulsus-read/src/logql/plan.rs:529 — approx_topk is \
                                 instant-only, rejected for a range query";
const SORT_RANGE_CITE: &str = "crates/pulsus-read/src/logql/post_agg.rs group_range — sort/sort_desc \
                               are a matrix passthrough (the reference does not value-order a \
                               matrix)";

/// The seven reduction operators (`sum`/`avg`/`min`/`max`/`count`/
/// `stddev`/`stdvar`) share one cell list: they differ only in `reduce`'s
/// arm, which allocates nothing input-scaled.
const REDUCTION_CELLS: &[ChainCell] = &[
    // GroupShape x Shape.
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::Single,
        KShape::NotParameterised,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Instant,
        GroupShape::ByPresent,
        ChainShape::Single,
        KShape::NotParameterised,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Instant,
        GroupShape::ByAbsent,
        ChainShape::Single,
        KShape::NotParameterised,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Instant,
        GroupShape::Without,
        ChainShape::Single,
        KShape::NotParameterised,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Range,
        GroupShape::NoGrouping,
        ChainShape::Single,
        KShape::NotParameterised,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Range,
        GroupShape::ByPresent,
        ChainShape::Single,
        KShape::NotParameterised,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Range,
        GroupShape::ByAbsent,
        ChainShape::Single,
        KShape::NotParameterised,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Range,
        GroupShape::Without,
        ChainShape::Single,
        KShape::NotParameterised,
        Driver::Direct,
    )),
    // ChainShape.
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::NestedSameOp,
        KShape::NotParameterised,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::NestedTwoOp,
        KShape::NotParameterised,
        Driver::Direct,
    )),
    // KShape.
    excluded(
        ck(
            Shape::Instant,
            GroupShape::NoGrouping,
            ChainShape::Single,
            KShape::KOne,
            Driver::Direct,
        ),
        ExcludeReason::OpTakesNoParameter,
        NO_PARAM_CITE,
    ),
    excluded(
        ck(
            Shape::Instant,
            GroupShape::NoGrouping,
            ChainShape::Single,
            KShape::KAll,
            Driver::Direct,
        ),
        ExcludeReason::OpTakesNoParameter,
        NO_PARAM_CITE,
    ),
    // Driver::PerVariant, once per Shape.
    exercised(ck(
        Shape::Instant,
        GroupShape::ByPresent,
        ChainShape::Single,
        KShape::NotParameterised,
        Driver::PerVariant,
    )),
    exercised(ck(
        Shape::Range,
        GroupShape::ByPresent,
        ChainShape::Single,
        KShape::NotParameterised,
        Driver::PerVariant,
    )),
];

/// `topk`/`bottomk` — parameterised and grouping-accepting.
const SELECT_CELLS: &[ChainCell] = &[
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::Single,
        KShape::KAll,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Instant,
        GroupShape::ByPresent,
        ChainShape::Single,
        KShape::KAll,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Instant,
        GroupShape::ByAbsent,
        ChainShape::Single,
        KShape::KAll,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Instant,
        GroupShape::Without,
        ChainShape::Single,
        KShape::KAll,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Range,
        GroupShape::NoGrouping,
        ChainShape::Single,
        KShape::KAll,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Range,
        GroupShape::ByPresent,
        ChainShape::Single,
        KShape::KAll,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Range,
        GroupShape::ByAbsent,
        ChainShape::Single,
        KShape::KAll,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Range,
        GroupShape::Without,
        ChainShape::Single,
        KShape::KAll,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::NestedSameOp,
        KShape::KAll,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::NestedTwoOp,
        KShape::KAll,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::Single,
        KShape::KOne,
        Driver::Direct,
    )),
];

/// `approx_topk` — instant-only and grouping-free.
const APPROX_CELLS: &[ChainCell] = &[
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::Single,
        KShape::KAll,
        Driver::Direct,
    )),
    excluded(
        ck(
            Shape::Instant,
            GroupShape::ByPresent,
            ChainShape::Single,
            KShape::KAll,
            Driver::Direct,
        ),
        ExcludeReason::OpTakesNoGrouping,
        NO_GROUPING_CITE,
    ),
    excluded(
        ck(
            Shape::Instant,
            GroupShape::ByAbsent,
            ChainShape::Single,
            KShape::KAll,
            Driver::Direct,
        ),
        ExcludeReason::OpTakesNoGrouping,
        NO_GROUPING_CITE,
    ),
    excluded(
        ck(
            Shape::Instant,
            GroupShape::Without,
            ChainShape::Single,
            KShape::KAll,
            Driver::Direct,
        ),
        ExcludeReason::OpTakesNoGrouping,
        NO_GROUPING_CITE,
    ),
    excluded(
        ck(
            Shape::Range,
            GroupShape::NoGrouping,
            ChainShape::Single,
            KShape::KAll,
            Driver::Direct,
        ),
        ExcludeReason::PlanRejectedForShape,
        APPROX_RANGE_CITE,
    ),
    excluded(
        ck(
            Shape::Range,
            GroupShape::ByPresent,
            ChainShape::Single,
            KShape::KAll,
            Driver::Direct,
        ),
        ExcludeReason::PlanRejectedForShape,
        APPROX_RANGE_CITE,
    ),
    excluded(
        ck(
            Shape::Range,
            GroupShape::ByAbsent,
            ChainShape::Single,
            KShape::KAll,
            Driver::Direct,
        ),
        ExcludeReason::PlanRejectedForShape,
        APPROX_RANGE_CITE,
    ),
    excluded(
        ck(
            Shape::Range,
            GroupShape::Without,
            ChainShape::Single,
            KShape::KAll,
            Driver::Direct,
        ),
        ExcludeReason::PlanRejectedForShape,
        APPROX_RANGE_CITE,
    ),
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::NestedSameOp,
        KShape::KAll,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::NestedTwoOp,
        KShape::KAll,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::Single,
        KShape::KOne,
        Driver::Direct,
    )),
];

/// `sort`/`sort_desc` — instant-only in effect, grouping-free,
/// parameter-free.
const SORT_CELLS: &[ChainCell] = &[
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::Single,
        KShape::NotParameterised,
        Driver::Direct,
    )),
    excluded(
        ck(
            Shape::Instant,
            GroupShape::ByPresent,
            ChainShape::Single,
            KShape::NotParameterised,
            Driver::Direct,
        ),
        ExcludeReason::OpTakesNoGrouping,
        NO_GROUPING_CITE,
    ),
    excluded(
        ck(
            Shape::Instant,
            GroupShape::ByAbsent,
            ChainShape::Single,
            KShape::NotParameterised,
            Driver::Direct,
        ),
        ExcludeReason::OpTakesNoGrouping,
        NO_GROUPING_CITE,
    ),
    excluded(
        ck(
            Shape::Instant,
            GroupShape::Without,
            ChainShape::Single,
            KShape::NotParameterised,
            Driver::Direct,
        ),
        ExcludeReason::OpTakesNoGrouping,
        NO_GROUPING_CITE,
    ),
    excluded(
        ck(
            Shape::Range,
            GroupShape::NoGrouping,
            ChainShape::Single,
            KShape::NotParameterised,
            Driver::Direct,
        ),
        ExcludeReason::PassthroughNoAllocation,
        SORT_RANGE_CITE,
    ),
    excluded(
        ck(
            Shape::Range,
            GroupShape::ByPresent,
            ChainShape::Single,
            KShape::NotParameterised,
            Driver::Direct,
        ),
        ExcludeReason::PassthroughNoAllocation,
        SORT_RANGE_CITE,
    ),
    excluded(
        ck(
            Shape::Range,
            GroupShape::ByAbsent,
            ChainShape::Single,
            KShape::NotParameterised,
            Driver::Direct,
        ),
        ExcludeReason::PassthroughNoAllocation,
        SORT_RANGE_CITE,
    ),
    excluded(
        ck(
            Shape::Range,
            GroupShape::Without,
            ChainShape::Single,
            KShape::NotParameterised,
            Driver::Direct,
        ),
        ExcludeReason::PassthroughNoAllocation,
        SORT_RANGE_CITE,
    ),
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::NestedSameOp,
        KShape::NotParameterised,
        Driver::Direct,
    )),
    exercised(ck(
        Shape::Instant,
        GroupShape::NoGrouping,
        ChainShape::NestedTwoOp,
        KShape::NotParameterised,
        Driver::Direct,
    )),
    excluded(
        ck(
            Shape::Instant,
            GroupShape::NoGrouping,
            ChainShape::Single,
            KShape::KOne,
            Driver::Direct,
        ),
        ExcludeReason::OpTakesNoParameter,
        NO_PARAM_CITE,
    ),
    excluded(
        ck(
            Shape::Instant,
            GroupShape::NoGrouping,
            ChainShape::Single,
            KShape::KAll,
            Driver::Direct,
        ),
        ExcludeReason::OpTakesNoParameter,
        NO_PARAM_CITE,
    ),
];

/// EXHAUSTIVE over all 12 `VectorAggOp` variants, NO `_` arm: a new
/// operator is a build failure until it has cells.
fn chain_cells(op: VectorAggOp) -> &'static [ChainCell] {
    match op {
        VectorAggOp::Sum
        | VectorAggOp::Avg
        | VectorAggOp::Min
        | VectorAggOp::Max
        | VectorAggOp::Count
        | VectorAggOp::Stddev
        | VectorAggOp::Stdvar => REDUCTION_CELLS,
        VectorAggOp::Topk | VectorAggOp::Bottomk => SELECT_CELLS,
        VectorAggOp::ApproxTopk => APPROX_CELLS,
        VectorAggOp::Sort | VectorAggOp::SortDesc => SORT_CELLS,
    }
}

fn is_parameterised(op: VectorAggOp) -> bool {
    matches!(
        op,
        VectorAggOp::Topk | VectorAggOp::Bottomk | VectorAggOp::ApproxTopk
    )
}

fn natural_k(op: VectorAggOp) -> KShape {
    if is_parameterised(op) {
        KShape::KAll
    } else {
        KShape::NotParameterised
    }
}

/// The tuples §5.3 REQUIRES a cell for. Computed, not listed, so the
/// requirement and the cells cannot be edited into agreement.
fn required_chain_tuples(op: VectorAggOp) -> Vec<ChainKey> {
    let mut v = Vec::new();
    for shape in [Shape::Instant, Shape::Range] {
        for group in [
            GroupShape::NoGrouping,
            GroupShape::ByPresent,
            GroupShape::ByAbsent,
            GroupShape::Without,
        ] {
            v.push(ck(
                shape,
                group,
                ChainShape::Single,
                natural_k(op),
                Driver::Direct,
            ));
        }
    }
    for chain in [
        ChainShape::Single,
        ChainShape::NestedSameOp,
        ChainShape::NestedTwoOp,
    ] {
        v.push(ck(
            Shape::Instant,
            GroupShape::NoGrouping,
            chain,
            natural_k(op),
            Driver::Direct,
        ));
    }
    let ks: &[KShape] = if is_parameterised(op) {
        &[KShape::KOne, KShape::KAll]
    } else {
        &[KShape::NotParameterised, KShape::KOne, KShape::KAll]
    };
    for k in ks {
        v.push(ck(
            Shape::Instant,
            GroupShape::NoGrouping,
            ChainShape::Single,
            *k,
            Driver::Direct,
        ));
    }
    v.sort_by_key(|k| format!("{k:?}"));
    v.dedup();
    v
}

// ---- binary cells ----

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct BinaryKey {
    matching: MatchShape,
    group: BinGroup,
    shape: Shape,
}

#[derive(Clone, Copy, Debug)]
struct BinaryCell {
    key: BinaryKey,
    cell: CellKind,
}

const fn bk(matching: MatchShape, group: BinGroup, shape: Shape) -> BinaryKey {
    BinaryKey {
        matching,
        group,
        shape,
    }
}

const fn bin_exercised(key: BinaryKey) -> BinaryCell {
    BinaryCell {
        key,
        cell: CellKind::Exercised(BIN_BASE),
    }
}

const fn bin_excluded(key: BinaryKey, reason: ExcludeReason, cite: &'static str) -> BinaryCell {
    BinaryCell {
        key,
        cell: CellKind::Excluded { reason, cite },
    }
}

const BARE_GROUP_CITE: &str = "pulsus-logql/src/ast.rs:732-736 — the parser populates `group` \
                               only when an on/ignoring clause precedes it; a bare \
                               group_left/group_right is a parse error";

/// One shared list for every operator: the join core (`instant_join` /
/// `set_op_join`) is selected by `is_set_op`, and the fixtures below
/// exercise both arms for every operator through the same key set.
const BINARY_CELLS: &[BinaryCell] = &[
    bin_exercised(bk(
        MatchShape::NoMatching,
        BinGroup::NoGroup,
        Shape::Instant,
    )),
    bin_exercised(bk(MatchShape::NoMatching, BinGroup::NoGroup, Shape::Range)),
    bin_excluded(
        bk(MatchShape::NoMatching, BinGroup::GroupLeft, Shape::Instant),
        ExcludeReason::MatchingRequiredForGroupModifier,
        BARE_GROUP_CITE,
    ),
    bin_excluded(
        bk(MatchShape::NoMatching, BinGroup::GroupLeft, Shape::Range),
        ExcludeReason::MatchingRequiredForGroupModifier,
        BARE_GROUP_CITE,
    ),
    bin_excluded(
        bk(MatchShape::NoMatching, BinGroup::GroupRight, Shape::Instant),
        ExcludeReason::MatchingRequiredForGroupModifier,
        BARE_GROUP_CITE,
    ),
    bin_excluded(
        bk(MatchShape::NoMatching, BinGroup::GroupRight, Shape::Range),
        ExcludeReason::MatchingRequiredForGroupModifier,
        BARE_GROUP_CITE,
    ),
    bin_exercised(bk(MatchShape::On, BinGroup::NoGroup, Shape::Instant)),
    bin_exercised(bk(MatchShape::On, BinGroup::NoGroup, Shape::Range)),
    bin_exercised(bk(MatchShape::On, BinGroup::GroupLeft, Shape::Instant)),
    bin_exercised(bk(MatchShape::On, BinGroup::GroupLeft, Shape::Range)),
    bin_exercised(bk(MatchShape::On, BinGroup::GroupRight, Shape::Instant)),
    bin_exercised(bk(MatchShape::On, BinGroup::GroupRight, Shape::Range)),
    bin_exercised(bk(MatchShape::Ignoring, BinGroup::NoGroup, Shape::Instant)),
    bin_exercised(bk(MatchShape::Ignoring, BinGroup::NoGroup, Shape::Range)),
    bin_exercised(bk(
        MatchShape::Ignoring,
        BinGroup::GroupLeft,
        Shape::Instant,
    )),
    bin_exercised(bk(MatchShape::Ignoring, BinGroup::GroupLeft, Shape::Range)),
    bin_exercised(bk(
        MatchShape::Ignoring,
        BinGroup::GroupRight,
        Shape::Instant,
    )),
    bin_exercised(bk(MatchShape::Ignoring, BinGroup::GroupRight, Shape::Range)),
];

/// EXHAUSTIVE over all 15 `BinOp` variants, NO `_` arm.
fn binary_cells(op: BinOp) -> &'static [BinaryCell] {
    match op {
        BinOp::Add
        | BinOp::Sub
        | BinOp::Mul
        | BinOp::Div
        | BinOp::Mod
        | BinOp::Pow
        | BinOp::Eq
        | BinOp::Neq
        | BinOp::Gt
        | BinOp::Gte
        | BinOp::Lt
        | BinOp::Lte
        | BinOp::And
        | BinOp::Or
        | BinOp::Unless => BINARY_CELLS,
    }
}

fn required_binary_tuples() -> Vec<BinaryKey> {
    let mut v = Vec::new();
    for matching in [MatchShape::NoMatching, MatchShape::On, MatchShape::Ignoring] {
        for group in [BinGroup::NoGroup, BinGroup::GroupLeft, BinGroup::GroupRight] {
            for shape in [Shape::Instant, Shape::Range] {
                v.push(bk(matching, group, shape));
            }
        }
    }
    v
}

fn matching_for(key: &BinaryKey, include: &[&str]) -> Option<VectorMatching> {
    let (on, labels) = match key.matching {
        MatchShape::NoMatching => return None,
        MatchShape::On => (true, vec!["id00".to_string()]),
        MatchShape::Ignoring => (false, vec!["l001".to_string()]),
    };
    let inc: Vec<String> = include.iter().map(|s| (*s).to_string()).collect();
    let group = match key.group {
        BinGroup::NoGroup => None,
        BinGroup::GroupLeft => Some(MatchGroup::Left(inc)),
        BinGroup::GroupRight => Some(MatchGroup::Right(inc)),
    };
    Some(VectorMatching { on, labels, group })
}

/// Measures one binary cell. Both operands carry the same `id` values, so
/// every matching shape pairs one-to-one and the join's output path is
/// genuinely exercised.
fn run_binary(
    op: BinOp,
    key: &BinaryKey,
    fx: &Fixture,
    scale: u64,
) -> (StageInput, StageInput, Option<VectorMatching>, Window) {
    let fx = Fixture {
        series: fx.series * scale,
        ..*fx
    };
    let matching = matching_for(key, &[]);
    let (lhs, rhs, li, ri) = match key.shape {
        Shape::Instant => {
            let l = build_vector(&fx, false);
            let r = build_vector(&fx, false);
            let li = measure_vector(&l);
            let ri = measure_vector(&r);
            (QueryResult::Vector(l), QueryResult::Vector(r), li, ri)
        }
        Shape::Range => {
            let l = build_matrix(&fx, false);
            let r = build_matrix(&fx, false);
            let li = measure_matrix(&l);
            let ri = measure_matrix(&r);
            (QueryResult::Matrix(l), QueryResult::Matrix(r), li, ri)
        }
    };
    let m = matching.clone();
    let (out, w) = measure(move || combine_binary(op, false, m.as_ref(), lhs, rhs));
    drop(out.expect("a witness binary fixture must combine cleanly"));
    (li, ri, matching, w)
}

// =====================================================================
// 6. Coverage (AC 22) and the exclusions
// =====================================================================

#[test]
fn every_required_tuple_has_a_cell_and_every_exclusion_is_cited() {
    for op in ALL_OPS {
        let cells = chain_cells(op);
        for want in required_chain_tuples(op) {
            assert!(
                cells.iter().any(|c| c.key == want),
                "{op:?}: no cell for {} / {} / {} / {} / {}",
                describe(want.shape),
                describe_group(want.group),
                describe_chain(want.chain),
                describe_k(want.k),
                describe_driver(want.driver),
            );
        }
        for c in cells {
            if let CellKind::Excluded { reason, cite } = c.cell {
                assert!(
                    !cite.is_empty(),
                    "{op:?}: {} carries no source citation",
                    describe_reason(reason)
                );
            }
        }
    }
    // `Driver::PerVariant` once per `Shape`, checked globally.
    for shape in [Shape::Instant, Shape::Range] {
        assert!(
            ALL_OPS.iter().any(|op| chain_cells(*op).iter().any(|c| {
                c.key.driver == Driver::PerVariant
                    && c.key.shape == shape
                    && matches!(c.cell, CellKind::Exercised(_))
            })),
            "no per-variant cell for the {} shape",
            describe(shape)
        );
    }
    for op in ALL_BIN_OPS {
        let cells = binary_cells(op);
        for want in required_binary_tuples() {
            assert!(
                cells.iter().any(|c| c.key == want),
                "{op:?}: no cell for {} / {} / {}",
                describe_matching(want.matching),
                describe_bin_group(want.group),
                describe(want.shape),
            );
        }
    }
}

/// Every `Excluded` cell's reason is checked against the behaviour it
/// claims, so an exclusion cannot outlive the constraint that justified
/// it. Some constraints live in the parser and some in the planner; each
/// is checked where it actually is.
#[test]
fn every_exclusion_is_real() {
    fn ctx() -> PlanCtx<'static> {
        PlanCtx {
            db: "pulsus",
            streams_idx: "log_streams_idx",
            streams: "log_streams",
            samples: "log_samples",
            rollup_table: "log_metrics_5s",
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 50 * 1024 * 1024 * 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        }
    }
    fn params(range: bool) -> QueryParams {
        QueryParams {
            spec: if range {
                QuerySpec::Range {
                    start_ns: 1_782_907_200_000_000_000,
                    end_ns: 1_782_928_800_000_000_000,
                    step_ns: 60_000_000_000,
                }
            } else {
                QuerySpec::Instant {
                    at_ns: 1_782_928_800_000_000_000,
                }
            },
            limit: 100,
            direction: Direction::Backward,
        }
    }
    let rejected = |q: &str, range: bool| -> bool {
        match pulsus_logql::parse(q) {
            Err(_) => true,
            Ok(expr) => pulsus_read::logql::plan(&expr, &params(range), &ctx()).is_err(),
        }
    };

    // PlanRejectedForShape — approx_topk is instant-only, and it is the
    // PLANNER that says so (it parses fine).
    let q = r#"approx_topk(3, count_over_time({app="a"}[5m]))"#;
    assert!(
        pulsus_logql::parse(q).is_ok(),
        "approx_topk must PARSE; the rejection is at plan time"
    );
    assert!(
        rejected(q, true),
        "approx_topk must be rejected for a range query"
    );
    assert!(
        !rejected(q, false),
        "approx_topk must plan for an instant query"
    );

    // OpTakesNoGrouping — sort/sort_desc and approx_topk reject a
    // grouping clause.
    for q in [
        r#"sort by (app) (count_over_time({app="a"}[5m]))"#,
        r#"sort_desc by (app) (count_over_time({app="a"}[5m]))"#,
        r#"approx_topk by (app) (3, count_over_time({app="a"}[5m]))"#,
    ] {
        assert!(rejected(q, false), "a grouping must be rejected: {q}");
    }

    // OpTakesNoParameter — a reduction/sort operator takes no parameter.
    for q in [
        r#"sum(3, count_over_time({app="a"}[5m]))"#,
        r#"sort(3, count_over_time({app="a"}[5m]))"#,
    ] {
        assert!(rejected(q, false), "a parameter must be rejected: {q}");
    }

    // MatchingRequiredForGroupModifier — a bare group_left is rejected.
    assert!(
        rejected(
            r#"count_over_time({app="a"}[5m]) / group_left count_over_time({app="b"}[5m])"#,
            false
        ),
        "a bare group_left must be rejected"
    );

    // PassthroughNoAllocation — `sort` over a matrix returns its input.
    let fx = Fixture {
        series: 8,
        ..CHAIN_BASE
    };
    let items = build_matrix(&fx, false);
    let before = items.clone();
    let out = apply_vector_aggs(
        QueryResult::Matrix(items),
        &[(VectorAggOp::Sort, None, None)],
    )
    .expect("a sort passthrough is always admitted");
    match out {
        QueryResult::Matrix(after) => {
            assert_eq!(
                after.len(),
                before.len(),
                "sort over a matrix is a passthrough"
            );
            for (a, b) in after.iter().zip(before.iter()) {
                assert_eq!(a.labels, b.labels);
                let ab: Vec<u64> = a.points.iter().map(|(_, v)| v.to_bits()).collect();
                let bb: Vec<u64> = b.points.iter().map(|(_, v)| v.to_bits()).collect();
                assert_eq!(ab, bb, "sort over a matrix must not move a point");
            }
        }
        other => panic!("expected a matrix, got {other:?}"),
    }
}

// =====================================================================
// 7. The per-cell gates (AC 22's response half, AC 25)
// =====================================================================

/// Every `Exercised` cell: the fixture drives real allocation
/// (`peak > 0`), the allocation RESPONDS to the input (`peak(2N) >
/// peak(N)`), and the measured peak is inside the model.
#[test]
fn every_cell_exercises_allocation_and_stays_inside_the_model() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut measured = 0usize;
    for op in ALL_OPS {
        for c in chain_cells(op) {
            let CellKind::Exercised(fx) = c.cell else {
                continue;
            };
            let (input, aggs, w) = run_chain(op, &c.key, &fx, 1);
            let (input2, aggs2, w2) = run_chain(op, &c.key, &fx, 2);
            let label = format!(
                "{op:?} {} / {} / {} / {} / {}",
                describe(c.key.shape),
                describe_group(c.key.group),
                describe_chain(c.key.chain),
                describe_k(c.key.k),
                describe_driver(c.key.driver)
            );
            assert!(
                !w.overflow && !w2.overflow,
                "{label}: cohort table overflowed"
            );
            assert!(w.peak > 0, "{label}: the cell allocates nothing: {w:?}");
            assert!(
                w2.peak > w.peak,
                "{label}: doubling N did not raise the peak ({} -> {}) — the fixture does not \
                 drive input-scaled allocation in the measured stage",
                w.peak,
                w2.peak
            );
            let modelled = post_agg_peak_bytes(&input, &aggs);
            assert!(
                w.peak <= modelled,
                "{label}: measured peak {} exceeds the model {modelled}",
                w.peak
            );
            let modelled2 = post_agg_peak_bytes(&input2, &aggs2);
            assert!(
                w2.peak <= modelled2,
                "{label} at 2N: measured peak {} exceeds the model {modelled2}",
                w2.peak
            );
            measured += 2;
        }
    }
    for op in ALL_BIN_OPS {
        for c in binary_cells(op) {
            let CellKind::Exercised(fx) = c.cell else {
                continue;
            };
            let (li, ri, matching, w) = run_binary(op, &c.key, &fx, 1);
            let (li2, ri2, _, w2) = run_binary(op, &c.key, &fx, 2);
            let label = format!(
                "{op:?} {} / {} / {}",
                describe_matching(c.key.matching),
                describe_bin_group(c.key.group),
                describe(c.key.shape)
            );
            assert!(
                !w.overflow && !w2.overflow,
                "{label}: cohort table overflowed"
            );
            assert!(w.peak > 0, "{label}: the cell allocates nothing: {w:?}");
            assert!(
                w2.peak > w.peak,
                "{label}: doubling N did not raise the peak ({} -> {})",
                w.peak,
                w2.peak
            );
            let modelled = binary_peak_bytes(op, matching.as_ref(), &li, &ri);
            assert!(
                w.peak <= modelled,
                "{label}: measured peak {} exceeds the model {modelled}",
                w.peak
            );
            let modelled2 = binary_peak_bytes(op, matching.as_ref(), &li2, &ri2);
            assert!(
                w2.peak <= modelled2,
                "{label} at 2N: measured peak {} exceeds the model {modelled2}",
                w2.peak
            );
            measured += 2;
        }
    }
    assert!(
        measured > 200,
        "the cell matrix shrank to {measured} measurements"
    );
}

// =====================================================================
// 8. Ladders and coefficient derivation (AC 26)
// =====================================================================

/// The stated safety factor applied to every measured rate. Not a
/// calibration knob: it is the margin covering "a distribution
/// adversarial in a dimension no ladder varies" (§7's named residual).
const WITNESS_MARGIN: u64 = 2;

fn ceil_div(a: u64, b: u64) -> u64 {
    if b == 0 { 0 } else { a.div_ceil(b) }
}

/// One ladder's measured points: `(axis value, peak)`, baseline first.
#[derive(Debug)]
struct LadderRun {
    name: &'static str,
    points: Vec<(u64, u64)>,
}

impl LadderRun {
    /// Secants from the BASELINE point (never adjacent differences —
    /// hashbrown/`Vec` doubling makes adjacent differences step-shaped).
    fn rates(&self) -> Vec<u64> {
        let (x0, p0) = self.points[0];
        self.points[1..]
            .iter()
            .map(|&(x, p)| ceil_div(p.saturating_sub(p0), x.saturating_sub(x0)))
            .collect()
    }

    fn rate_max(&self) -> u64 {
        self.rates().into_iter().max().unwrap_or(0)
    }

    /// The marginal cost never exceeds 1.25x the smallest-step marginal
    /// cost: a superlinear stage fails HERE rather than being
    /// extrapolated.
    fn assert_linear(&self) {
        let rates = self.rates();
        let r1 = rates[0];
        for (i, r) in rates.iter().enumerate().skip(1) {
            assert!(
                r.saturating_mul(4) <= r1.saturating_mul(5),
                "{}: rate[{i}] = {r} exceeds 1.25x the smallest-step rate {r1} — the stage is \
                 superlinear on this axis and must not be extrapolated ({:?})",
                self.name,
                self.points
            );
        }
        assert!(
            self.points.last().expect("ladder").0 >= self.points[0].0.saturating_mul(64),
            "{}: the ladder must span >= 64x ({:?})",
            self.name,
            self.points
        );
    }
}

/// A chain ladder: one axis varied, everything else at baseline.
struct ChainLadder {
    name: &'static str,
    term: ChainTerm,
    op: VectorAggOp,
    key: ChainKey,
    /// `(fixture, chain length)` per rung, baseline first.
    rungs: Vec<(Fixture, usize)>,
    axis: fn(&StageInput, &[VectorAggSpec]) -> u64,
}

fn axis_series(m: &StageInput, _: &[VectorAggSpec]) -> u64 {
    m.series()
}
fn axis_points(m: &StageInput, _: &[VectorAggSpec]) -> u64 {
    m.points()
}
fn axis_label_bytes(m: &StageInput, _: &[VectorAggSpec]) -> u64 {
    m.label_bytes()
}
fn axis_pairs(m: &StageInput, _: &[VectorAggSpec]) -> u64 {
    m.label_pairs()
}
fn axis_stage_series(m: &StageInput, aggs: &[VectorAggSpec]) -> u64 {
    m.series() * aggs.len() as u64
}
fn axis_group_names(m: &StageInput, aggs: &[VectorAggSpec]) -> u64 {
    m.series() * group_name_bytes(aggs)
}

fn nested(spec: VectorAggSpec, len: usize) -> Vec<VectorAggSpec> {
    (0..len).map(|_| spec.clone()).collect()
}

fn chain_ladders() -> Vec<ChainLadder> {
    let inst = |group: GroupShape, k: KShape| {
        ck(Shape::Instant, group, ChainShape::Single, k, Driver::Direct)
    };
    let rng = |group: GroupShape, k: KShape| {
        ck(Shape::Range, group, ChainShape::Single, k, Driver::Direct)
    };
    vec![
        // W_POINT — a SINGLE output group, so one `BTreeSet<i64>` step
        // union holds every point.
        ChainLadder {
            name: "W_POINT",
            term: ChainTerm::Point,
            op: VectorAggOp::Sum,
            key: rng(GroupShape::NoGrouping, KShape::NotParameterised),
            rungs: [4u64, 32, 128, 512]
                .into_iter()
                .map(|steps| {
                    (
                        Fixture {
                            series: 64,
                            steps,
                            ..CHAIN_BASE
                        },
                        1,
                    )
                })
                .collect(),
            axis: axis_points,
        },
        // W_SERIES — `topk(k = N)` retains everything, so BOTH stage
        // buffers are full; points and label mass are held constant.
        ChainLadder {
            name: "W_SERIES",
            term: ChainTerm::Series,
            op: VectorAggOp::Topk,
            key: rng(GroupShape::NoGrouping, KShape::KAll),
            rungs: [128u64, 512, 2048, 8192]
                .into_iter()
                .map(|series| {
                    (
                        Fixture {
                            series,
                            pairs: 8192 / series,
                            steps: 8192 / series,
                            ..CHAIN_BASE
                        },
                        1,
                    )
                })
                .collect(),
            axis: axis_series,
        },
        // W_LABEL_BYTE — `without(...)` keeps one group per series, so
        // the retained key mass is maximal.
        ChainLadder {
            name: "W_LABEL_BYTE",
            term: ChainTerm::LabelByte,
            op: VectorAggOp::Sum,
            key: inst(GroupShape::Without, KShape::NotParameterised),
            rungs: [4u64, 32, 256, 1024]
                .into_iter()
                .map(|value_bytes| {
                    (
                        Fixture {
                            series: 256,
                            value_bytes,
                            ..CHAIN_BASE
                        },
                        1,
                    )
                })
                .collect(),
            axis: axis_label_bytes,
        },
        // W_PAIR — same shape, per-pair width scaled down so the label
        // BYTE total stays near constant while the pair count spans 64x.
        ChainLadder {
            name: "W_PAIR",
            term: ChainTerm::Pair,
            op: VectorAggOp::Sum,
            key: inst(GroupShape::Without, KShape::NotParameterised),
            rungs: [4u64, 32, 128, 512]
                .into_iter()
                .map(|pairs| {
                    (
                        Fixture {
                            // 64 series, not the usual 256: the
                            // CONCENTRATED skew puts every pair on ONE
                            // series, and 256 x 512 pairs exceeds the
                            // cohort table's capacity. Reducing the
                            // widest ladder point is the sanctioned
                            // response (plan v14 §5.2); widening a probe
                            // bound or dropping entries is not.
                            series: 64,
                            pairs,
                            value_bytes: (2048 / pairs).max(1),
                            ..CHAIN_BASE
                        },
                        1,
                    )
                })
                .collect(),
            axis: axis_pairs,
        },
        // W_STAGE_SERIES — nested `topk(N, ...)`, every stage retaining,
        // so the previous stage's buffer is live while its successor is
        // collected (§6.1).
        ChainLadder {
            name: "W_STAGE_SERIES",
            term: ChainTerm::StageSeries,
            op: VectorAggOp::Topk,
            key: inst(GroupShape::NoGrouping, KShape::KAll),
            rungs: [1usize, 2, 4, 64]
                .into_iter()
                .map(|len| {
                    (
                        Fixture {
                            series: 512,
                            ..CHAIN_BASE
                        },
                        len,
                    )
                })
                .collect(),
            axis: axis_stage_series,
        },
        // W_GROUPNAME — `by(id, <q-1 absent names>)`. §5.4 named an
        // ALL-absent clause as the maximising shape; measurement refutes
        // it: with every name absent all series collapse to ONE group, so
        // exactly one key is RETAINED and the other N-1 are built and
        // dropped (peak flat at 8 KiB from q = 4 to q = 16). Keeping one
        // PRESENT distinguishing name retains one q-pair key per output
        // group, which is what the term `W_GROUPNAME * N * A` models. The
        // RULE §5.4 states — "the shape that maximises that axis's rate"
        // — is what is followed here.
        ChainLadder {
            name: "W_GROUPNAME",
            term: ChainTerm::GroupName,
            op: VectorAggOp::Sum,
            key: inst(GroupShape::ByPresent, KShape::NotParameterised),
            rungs: [4u64, 16, 64, 256]
                .into_iter()
                .map(|names| {
                    (
                        Fixture {
                            series: 256,
                            // `pairs` is reused as the by-name count for
                            // this ladder only; the fixture data is fixed.
                            pairs: names,
                            ..CHAIN_BASE
                        },
                        1,
                    )
                })
                .collect(),
            axis: axis_group_names,
        },
    ]
}

/// `by(<q names, none of them in the data>)`.
fn absent_by_names(q: u64) -> Grouping {
    Grouping {
        kind: GroupingKind::By,
        labels: (0..q).map(|i| format!("a{i:03}")).collect(),
    }
}

/// `by(id, <q-1 absent names>)` — one PRESENT distinguishing name, so the
/// grouping retains one `q`-pair key per output group instead of
/// collapsing every series into one group.
fn by_names_with_one_present(q: u64) -> Grouping {
    let mut labels = vec!["id00".to_string()];
    labels.extend((1..q).map(|i| format!("a{i:03}")));
    Grouping {
        kind: GroupingKind::By,
        labels,
    }
}

fn run_chain_ladder(l: &ChainLadder, skew: Skew) -> LadderRun {
    let mut points = Vec::new();
    for (fx, len) in &l.rungs {
        let fx = Fixture { skew, ..*fx };
        let (result, aggs) = if l.term == ChainTerm::GroupName {
            // This ladder varies the BY-NAME COUNT, not the data.
            let data = Fixture {
                pairs: CHAIN_BASE.pairs,
                ..fx
            };
            let items = build_vector(&data, false);
            (
                QueryResult::Vector(items),
                vec![(l.op, Some(by_names_with_one_present(fx.pairs)), None)],
            )
        } else {
            let spec = (
                l.op,
                grouping_for(l.key.group),
                param_for(l.key.k, fx.series),
            );
            let aggs = nested(spec, *len);
            let result = match l.key.shape {
                Shape::Instant => QueryResult::Vector(build_vector(&fx, false)),
                Shape::Range => QueryResult::Matrix(build_matrix(&fx, false)),
            };
            (result, aggs)
        };
        let input = measure_result(&result);
        let x = (l.axis)(&input, &aggs);
        let (out, w) = measure(|| apply_vector_aggs(result, &aggs));
        drop(out.expect("a witness ladder rung must be admitted"));
        assert!(!w.overflow, "{}: cohort table overflowed", l.name);
        points.push((x, w.peak));
    }
    LadderRun {
        name: l.name,
        points,
    }
}

/// A binary ladder.
struct BinLadder {
    name: &'static str,
    term: BinaryTerm,
    key: BinaryKey,
    rungs: Vec<(Fixture, usize)>,
    axis: fn(&StageInput, &StageInput, Option<&VectorMatching>) -> u64,
}

fn bin_axis_series(l: &StageInput, r: &StageInput, _: Option<&VectorMatching>) -> u64 {
    l.series() + r.series()
}
fn bin_axis_points(l: &StageInput, r: &StageInput, _: Option<&VectorMatching>) -> u64 {
    l.points() + r.points()
}
fn bin_axis_label(l: &StageInput, r: &StageInput, _: Option<&VectorMatching>) -> u64 {
    l.label_bytes() + r.label_bytes()
}
fn bin_axis_pairs(l: &StageInput, r: &StageInput, _: Option<&VectorMatching>) -> u64 {
    l.label_pairs() + r.label_pairs()
}
fn bin_axis_many(l: &StageInput, _: &StageInput, _: Option<&VectorMatching>) -> u64 {
    l.series()
}
fn bin_axis_include(l: &StageInput, r: &StageInput, m: Option<&VectorMatching>) -> u64 {
    l.series() * include_bytes(m, BinOp::Add, r)
}

fn bin_ladders() -> Vec<BinLadder> {
    vec![
        BinLadder {
            name: "B_SERIES",
            term: BinaryTerm::Series,
            key: bk(MatchShape::NoMatching, BinGroup::NoGroup, Shape::Instant),
            rungs: [64u64, 256, 1024, 4096]
                .into_iter()
                .map(|series| (Fixture { series, ..BIN_BASE }, 0))
                .collect(),
            axis: bin_axis_series,
        },
        BinLadder {
            name: "B_POINT",
            term: BinaryTerm::Point,
            key: bk(MatchShape::NoMatching, BinGroup::NoGroup, Shape::Range),
            rungs: [4u64, 32, 128, 512]
                .into_iter()
                .map(|steps| {
                    (
                        Fixture {
                            // 16 series: `combine_matrices` runs an
                            // INDEPENDENT per-step join, so its cost is
                            // `steps x series` and the widest rung
                            // dominates the whole binary's wall time.
                            series: 16,
                            steps,
                            ..BIN_BASE
                        },
                        0,
                    )
                })
                .collect(),
            axis: bin_axis_points,
        },
        BinLadder {
            name: "B_LABEL",
            term: BinaryTerm::Label,
            key: bk(MatchShape::NoMatching, BinGroup::NoGroup, Shape::Instant),
            rungs: [4u64, 32, 256, 1024]
                .into_iter()
                .map(|value_bytes| {
                    (
                        Fixture {
                            series: 256,
                            value_bytes,
                            ..BIN_BASE
                        },
                        0,
                    )
                })
                .collect(),
            axis: bin_axis_label,
        },
        BinLadder {
            name: "B_PAIR",
            term: BinaryTerm::Pair,
            key: bk(MatchShape::NoMatching, BinGroup::NoGroup, Shape::Instant),
            rungs: [4u64, 32, 128, 512]
                .into_iter()
                .map(|pairs| {
                    (
                        Fixture {
                            // See the `W_PAIR` ladder: 64 series keeps the
                            // concentrated rung inside the cohort table.
                            series: 64,
                            pairs,
                            value_bytes: (2048 / pairs).max(1),
                            ..BIN_BASE
                        },
                        0,
                    )
                })
                .collect(),
            axis: bin_axis_pairs,
        },
        BinLadder {
            name: "B_MANY",
            term: BinaryTerm::Many,
            key: bk(MatchShape::On, BinGroup::GroupLeft, Shape::Instant),
            rungs: [64u64, 256, 1024, 4096]
                .into_iter()
                .map(|series| (Fixture { series, ..BIN_BASE }, 0))
                .collect(),
            axis: bin_axis_many,
        },
        BinLadder {
            name: "B_INCLUDE",
            term: BinaryTerm::Include,
            key: bk(MatchShape::On, BinGroup::GroupLeft, Shape::Instant),
            rungs: [4usize, 16, 64, 256]
                .into_iter()
                .map(|q| {
                    (
                        Fixture {
                            series: 128,
                            ..BIN_BASE
                        },
                        q,
                    )
                })
                .collect(),
            axis: bin_axis_include,
        },
    ]
}

fn include_names(q: usize) -> Vec<String> {
    (0..q).map(|i| format!("inc{i:03}")).collect()
}

/// The one side carries EVERY include label in every fixture, so varying
/// the `group_left(...)` list moves `include_bytes` and nothing else.
fn one_side_with_includes(fx: &Fixture, q: usize) -> Vec<VectorSample> {
    (0..fx.series)
        .map(|i| {
            let mut labels = labels_for(i, fx.pairs, fx.value_bytes, false);
            for name in include_names(q) {
                labels.push((name, pad(i, fx.value_bytes)));
            }
            labels.sort();
            VectorSample {
                labels,
                value: (i % 89) as f64 + 1.5,
            }
        })
        .collect()
}

fn run_bin_ladder(l: &BinLadder, skew: Skew, max_includes: usize) -> LadderRun {
    let mut points = Vec::new();
    for (fx, q) in &l.rungs {
        let fx = Fixture { skew, ..*fx };
        let inc = if l.term == BinaryTerm::Include {
            include_names(*q)
        } else {
            Vec::new()
        };
        let inc_refs: Vec<&str> = inc.iter().map(String::as_str).collect();
        let matching = matching_for(&l.key, &inc_refs);
        let (lhs, rhs, li, ri) = match l.key.shape {
            Shape::Instant => {
                let a = build_vector(&fx, false);
                let b = if l.term == BinaryTerm::Include {
                    one_side_with_includes(&fx, max_includes)
                } else {
                    build_vector(&fx, false)
                };
                let li = measure_vector(&a);
                let ri = measure_vector(&b);
                (QueryResult::Vector(a), QueryResult::Vector(b), li, ri)
            }
            Shape::Range => {
                let a = build_matrix(&fx, false);
                let b = build_matrix(&fx, false);
                let li = measure_matrix(&a);
                let ri = measure_matrix(&b);
                (QueryResult::Matrix(a), QueryResult::Matrix(b), li, ri)
            }
        };
        let x = (l.axis)(&li, &ri, matching.as_ref());
        let m = matching.clone();
        let (out, w) = measure(move || combine_binary(BinOp::Add, false, m.as_ref(), lhs, rhs));
        drop(out.expect("a witness binary ladder must combine cleanly"));
        assert!(!w.overflow, "{}: cohort table overflowed", l.name);
        points.push((x, w.peak));
    }
    LadderRun {
        name: l.name,
        points,
    }
}

fn shipped(term: ChainTerm) -> u64 {
    match term {
        ChainTerm::None => 0,
        ChainTerm::Series => W_SERIES,
        ChainTerm::Point => W_POINT,
        ChainTerm::LabelByte => W_LABEL_BYTE,
        ChainTerm::Pair => W_PAIR,
        ChainTerm::StageSeries => W_STAGE_SERIES,
        ChainTerm::GroupName => W_GROUPNAME,
        ChainTerm::ApproxTopk => W_APPROX_TOPK,
    }
}

fn shipped_bin(term: BinaryTerm) -> u64 {
    match term {
        BinaryTerm::None => 0,
        BinaryTerm::Series => B_SERIES,
        BinaryTerm::Point => B_POINT,
        BinaryTerm::Label => B_LABEL,
        BinaryTerm::Pair => B_PAIR,
        BinaryTerm::Many => B_MANY,
        BinaryTerm::Include => B_INCLUDE,
    }
}

/// Every axis the model prices, NAMED. Deleting a `ChainTerm`/
/// `BinaryTerm` variant stops this literal compiling; adding one stops
/// [`shipped`]/[`shipped_bin`]'s exhaustive `match` compiling. So a term
/// cannot leave the model quietly — which matters for
/// `W_STAGE_SERIES`, whose coefficient is 0 and which a future reader
/// will otherwise be tempted to delete as dead weight (see its doc: the
/// zero is contingent on an in-place-collect specialisation, not on the
/// nature of the computation).
const ALL_CHAIN_TERMS: [ChainTerm; 7] = [
    ChainTerm::Series,
    ChainTerm::Point,
    ChainTerm::LabelByte,
    ChainTerm::Pair,
    ChainTerm::StageSeries,
    ChainTerm::GroupName,
    ChainTerm::ApproxTopk,
];

const ALL_BINARY_TERMS: [BinaryTerm; 6] = [
    BinaryTerm::Series,
    BinaryTerm::Point,
    BinaryTerm::Label,
    BinaryTerm::Pair,
    BinaryTerm::Many,
    BinaryTerm::Include,
];

/// Every priced axis is either derived by a ladder or is a DECLARED
/// exception with its reason. A term that is neither has no derivation
/// behind it, which is the state this issue exists to eliminate.
#[test]
fn every_model_term_is_derived_or_a_declared_exception() {
    // `W_STAGE_SERIES`: measured 0 across 8 shapes x 5 chain lengths
    // (in-place collect), guarded by
    // `chain_depth_does_not_multiply_peak_memory`.
    // `W_APPROX_TOPK`: a FLAT term, so it has no rate and is derived at
    // the minimal input by `the_flat_approx_topk_term_bounds_a_minimal_input`.
    // `B_MANY`: plan v14 §6.4's declared exception, discriminated by the
    // 2x2 difference-of-differences.
    let chain_exceptions = [ChainTerm::StageSeries, ChainTerm::ApproxTopk];
    let ladder_terms: Vec<ChainTerm> = chain_ladders().iter().map(|l| l.term).collect();
    for t in ALL_CHAIN_TERMS {
        assert!(
            ladder_terms.contains(&t) || chain_exceptions.contains(&t),
            "{t:?} is priced by the chain model but has no ladder and no declared exception"
        );
    }
    let bin_exceptions = [BinaryTerm::Many];
    let bin_ladder_terms: Vec<BinaryTerm> = bin_ladders().iter().map(|l| l.term).collect();
    for t in ALL_BINARY_TERMS {
        assert!(
            bin_ladder_terms.contains(&t) || bin_exceptions.contains(&t),
            "{t:?} is priced by the binary model but has no ladder and no declared exception"
        );
    }
    // `B_MANY` HAS a ladder as well as its 2x2 — both, not either.
    assert!(bin_ladder_terms.contains(&BinaryTerm::Many));
    // Suppressing a term must actually change the model for every term
    // whose coefficient is non-zero: a `_without` arm that quietly
    // stopped suppressing would make every necessity assertion vacuous.
    let m = StageInput::for_derivation(64, 128, 512, 512, 2, 8, 64, 1);
    let aggs = vec![
        (
            VectorAggOp::ApproxTopk,
            Some(by_clause_of_total_bytes(16)),
            Some(1.0),
        ),
        (VectorAggOp::Sum, None, None),
    ];
    for t in ALL_CHAIN_TERMS {
        let full = post_agg_peak_bytes(&m, &aggs);
        let without = post_agg_peak_bytes_without(&m, &aggs, t);
        assert_eq!(
            without < full,
            shipped(t) > 0,
            "{t:?}: suppressing the term must lower the model exactly when its coefficient is \
             non-zero (shipped = {})",
            shipped(t)
        );
    }
    let one = StageInput::for_derivation(64, 128, 512, 512, 2, 8, 64, 1);
    let matching = include_matching_of_names(4);
    for t in ALL_BINARY_TERMS {
        let full = binary_peak_bytes(BinOp::Add, Some(&matching), &m, &one);
        let without = binary_peak_bytes_without(BinOp::Add, Some(&matching), &m, &one, t);
        assert_eq!(
            without < full,
            shipped_bin(t) > 0,
            "{t:?}: suppressing the term must lower the model exactly when its coefficient is \
             non-zero (shipped = {})",
            shipped_bin(t)
        );
    }
}

/// Derives every coefficient from a fresh ladder run and asserts the
/// SHIPPED constant covers it. The measured `rate_max` is recorded in
/// each constant's doc comment; regeneration is `zz_witness_report`.
#[test]
fn coefficients_are_witness_derived_and_the_stage_is_linear() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for l in chain_ladders() {
        let mut rate_max = 0;
        for skew in [Skew::Uniform, Skew::Concentrated] {
            let run = run_chain_ladder(&l, skew);
            run.assert_linear();
            rate_max = rate_max.max(run.rate_max());
        }
        let want = rate_max.saturating_mul(WITNESS_MARGIN);
        assert!(
            shipped(l.term) >= want,
            "{}: shipped {} < WITNESS_MARGIN x rate_max = {want} (rate_max = {rate_max})",
            l.name,
            shipped(l.term)
        );
    }
    for l in bin_ladders() {
        let mut rate_max = 0;
        for skew in [Skew::Uniform, Skew::Concentrated] {
            let run = run_bin_ladder(&l, skew, 256);
            run.assert_linear();
            rate_max = rate_max.max(run.rate_max());
        }
        let want = rate_max.saturating_mul(WITNESS_MARGIN);
        assert!(
            shipped_bin(l.term) >= want,
            "{}: shipped {} < WITNESS_MARGIN x rate_max = {want} (rate_max = {rate_max})",
            l.name,
            shipped_bin(l.term)
        );
    }
}

// =====================================================================
// 9. Term isolation by paired fixtures (AC 27, AC 28)
// =====================================================================

/// A `lo`/`hi` pair for one chain coefficient. The matching shape and
/// operand orientation are fixed HERE, not by the fixture author.
struct ChainPair {
    name: &'static str,
    term: ChainTerm,
    op: VectorAggOp,
    shape: Shape,
    group: GroupShape,
    lo: (Fixture, usize, u64),
    hi: (Fixture, usize, u64),
}

fn chain_pairs() -> Vec<ChainPair> {
    let base = Fixture {
        series: 256,
        ..CHAIN_BASE
    };
    vec![
        // The `by(...)` list only; names absent from the data; the data
        // is byte-identical and `aggs.len()` is held equal.
        ChainPair {
            name: "W_GROUPNAME",
            term: ChainTerm::GroupName,
            op: VectorAggOp::Sum,
            shape: Shape::Instant,
            group: GroupShape::ByAbsent,
            lo: (base, 1, 1),
            hi: (base, 1, 256),
        },
        // Points per series; series and labels fixed.
        ChainPair {
            name: "W_POINT",
            term: ChainTerm::Point,
            op: VectorAggOp::Sum,
            shape: Shape::Range,
            group: GroupShape::NoGrouping,
            lo: (
                Fixture {
                    series: 64,
                    steps: 8,
                    ..base
                },
                1,
                0,
            ),
            hi: (
                Fixture {
                    series: 64,
                    steps: 128,
                    ..base
                },
                1,
                0,
            ),
        },
        // Series, with points-per-series and per-series label width
        // scaled down so points, label_bytes and label_pairs are CONSTANT
        // — asserted, not assumed.
        ChainPair {
            name: "W_SERIES",
            term: ChainTerm::Series,
            op: VectorAggOp::Topk,
            shape: Shape::Range,
            group: GroupShape::NoGrouping,
            // `series x pairs` and `series x steps` are both 4 096 on
            // each side, so `points`, `label_bytes` and `label_pairs` are
            // byte-identical and only `series` moves. An INSTANT operand
            // cannot express this pair at all — `points == series` there.
            lo: (
                Fixture {
                    series: 256,
                    pairs: 16,
                    steps: 16,
                    ..base
                },
                1,
                0,
            ),
            hi: (
                Fixture {
                    series: 4096,
                    pairs: 1,
                    steps: 1,
                    ..base
                },
                1,
                0,
            ),
        },
        // Label VALUE width; pairs and series fixed. A CHAIN fixture, so
        // `max_value_bytes` (which drifts) is not a model input.
        ChainPair {
            name: "W_LABEL_BYTE",
            term: ChainTerm::LabelByte,
            op: VectorAggOp::Sum,
            shape: Shape::Instant,
            group: GroupShape::Without,
            lo: (
                Fixture {
                    series: 256,
                    value_bytes: 8,
                    ..base
                },
                1,
                0,
            ),
            hi: (
                Fixture {
                    series: 256,
                    value_bytes: 256,
                    ..base
                },
                1,
                0,
            ),
        },
        // Pairs per series, per-pair width scaled down so `label_bytes`
        // is constant — asserted.
        ChainPair {
            name: "W_PAIR",
            term: ChainTerm::Pair,
            op: VectorAggOp::Sum,
            shape: Shape::Instant,
            group: GroupShape::Without,
            lo: (
                Fixture {
                    series: 256,
                    pairs: 4,
                    value_bytes: 124,
                    ..base
                },
                1,
                0,
            ),
            hi: (
                Fixture {
                    series: 256,
                    pairs: 64,
                    value_bytes: 4,
                    ..base
                },
                1,
                0,
            ),
        },
    ]
}

fn build_chain_pair_side(
    p: &ChainPair,
    side: &(Fixture, usize, u64),
) -> (QueryResult, Vec<VectorAggSpec>) {
    let (fx, len, names) = side;
    let grouping = if p.term == ChainTerm::GroupName {
        Some(absent_by_names(*names))
    } else {
        grouping_for(p.group)
    };
    let param = if matches!(p.op, VectorAggOp::Topk | VectorAggOp::Bottomk) {
        Some(fx.series as f64)
    } else {
        None
    };
    let aggs = nested((p.op, grouping, param), *len);
    let result = match p.shape {
        Shape::Instant => QueryResult::Vector(build_vector(fx, false)),
        Shape::Range => QueryResult::Matrix(build_matrix(fx, false)),
    };
    (result, aggs)
}

/// The NINE model-relevant inputs plan v14 §6 enumerates. `StageInput`
/// also measures `label_block_bytes`, `max_series_pairs` and
/// `max_series_points`, which NO model term reads — asserted by
/// [`the_model_reads_exactly_the_nine_isolated_inputs`] rather than
/// assumed, so this list cannot silently stop covering the model.
fn model_relevant(
    lhs: &StageInput,
    rhs: Option<&StageInput>,
    aggs: &[VectorAggSpec],
    matching: Option<&VectorMatching>,
    op: BinOp,
) -> Vec<(&'static str, u64)> {
    let mut v = vec![
        ("lhs.series", lhs.series()),
        ("lhs.points", lhs.points()),
        ("lhs.label_bytes", lhs.label_bytes()),
        ("lhs.label_pairs", lhs.label_pairs()),
        ("aggs.len()", aggs.len() as u64),
        ("group_name_bytes", group_name_bytes(aggs)),
    ];
    if let Some(r) = rhs {
        v.push(("rhs.series", r.series()));
        v.push(("rhs.points", r.points()));
        v.push(("rhs.label_bytes", r.label_bytes()));
        v.push(("rhs.label_pairs", r.label_pairs()));
        let one = match matching.and_then(|m| m.group.as_ref()) {
            Some(MatchGroup::Right(_)) => lhs,
            _ => r,
        };
        let many = match matching.and_then(|m| m.group.as_ref()) {
            Some(MatchGroup::Right(_)) => r,
            _ => lhs,
        };
        v.push(("many.series", many.series()));
        v.push(("include_bytes", include_bytes(matching, op, one)));
    }
    v
}

/// Every model-relevant input except the target must be byte-identical
/// across `lo` and `hi` — asserted over an explicit named list, failing
/// if any drifts. "Zero non-target increment" is asserted, never argued.
fn assert_only_target_moves(
    name: &str,
    lo: &[(&'static str, u64)],
    hi: &[(&'static str, u64)],
    allowed: &[&str],
) {
    assert_eq!(lo.len(), hi.len(), "{name}: input lists differ in shape");
    for (&(n, a), &(_, b)) in lo.iter().zip(hi.iter()) {
        if allowed.contains(&n) {
            continue;
        }
        assert_eq!(
            a, b,
            "{name}: non-target model input `{n}` drifted between the lo and hi fixtures"
        );
    }
}

/// The three measured-but-unread `StageInput` axes really are unread: the
/// isolation list above would otherwise be weaker than it claims.
#[test]
fn the_model_reads_exactly_the_nine_isolated_inputs() {
    let base = StageInput::for_derivation(7, 11, 13, 17, 19, 23, 29, 31);
    // Same nine inputs, different `label_block_bytes` / `max_series_pairs`
    // / `max_series_points`.
    let twin = StageInput::for_derivation(7, 11, 13, 999, 999, 23, 29, 999);
    let aggs = vec![(VectorAggOp::Sum, None, None)];
    assert_eq!(
        post_agg_peak_bytes(&base, &aggs),
        post_agg_peak_bytes(&twin, &aggs),
        "the chain model must read none of label_block_bytes/max_series_pairs/max_series_points"
    );
    let m = include_matching_of_names(3);
    assert_eq!(
        binary_peak_bytes(BinOp::Add, Some(&m), &base, &base),
        binary_peak_bytes(BinOp::Add, Some(&m), &twin, &twin),
        "the binary model must read none of them either"
    );
    // `max_value_bytes` IS read, but only through `include_bytes`.
    let wide = StageInput::for_derivation(7, 11, 13, 17, 19, 4096, 29, 31);
    assert_eq!(
        post_agg_peak_bytes(&wide, &aggs),
        post_agg_peak_bytes(&base, &aggs),
        "the chain model must not read max_value_bytes at all"
    );
    assert_ne!(
        binary_peak_bytes(BinOp::Add, Some(&m), &base, &wide),
        binary_peak_bytes(BinOp::Add, Some(&m), &base, &base),
        "the binary model must read max_value_bytes through include_bytes"
    );
    assert_eq!(
        binary_peak_bytes(BinOp::Add, None, &base, &wide),
        binary_peak_bytes(BinOp::Add, None, &base, &base),
        "with no group modifier there is no include amplification to read it through"
    );
}

fn allowed_drift(term: ChainTerm) -> &'static [&'static str] {
    match term {
        ChainTerm::None => &[],
        ChainTerm::Series => &["lhs.series"],
        ChainTerm::Point => &["lhs.points"],
        ChainTerm::LabelByte => &["lhs.label_bytes"],
        ChainTerm::Pair => &["lhs.label_pairs"],
        ChainTerm::StageSeries => &["aggs.len()"],
        ChainTerm::GroupName => &["group_name_bytes"],
        ChainTerm::ApproxTopk => &[],
    }
}

fn allowed_drift_bin(term: BinaryTerm) -> &'static [&'static str] {
    match term {
        BinaryTerm::None => &[],
        BinaryTerm::Series => &["lhs.series", "rhs.series", "many.series"],
        BinaryTerm::Point => &["lhs.points", "rhs.points"],
        BinaryTerm::Label => &["lhs.label_bytes", "rhs.label_bytes"],
        BinaryTerm::Pair => &["lhs.label_pairs", "rhs.label_pairs"],
        BinaryTerm::Many => &["many.series"],
        BinaryTerm::Include => &["include_bytes"],
    }
}

#[test]
fn each_chain_coefficient_is_necessary_by_its_paired_fixtures() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for p in chain_pairs() {
        let (lo_res, lo_aggs) = build_chain_pair_side(&p, &p.lo);
        let (hi_res, hi_aggs) = build_chain_pair_side(&p, &p.hi);
        let lo_in = measure_result(&lo_res);
        let hi_in = measure_result(&hi_res);
        assert_only_target_moves(
            p.name,
            &model_relevant(&lo_in, None, &lo_aggs, None, BinOp::Add),
            &model_relevant(&hi_in, None, &hi_aggs, None, BinOp::Add),
            allowed_drift(p.term),
        );

        let (o1, w_lo) = measure(|| apply_vector_aggs(lo_res, &lo_aggs));
        drop(o1.expect("a paired fixture must be admitted"));
        let (o2, w_hi) = measure(|| apply_vector_aggs(hi_res, &hi_aggs));
        drop(o2.expect("a paired fixture must be admitted"));
        assert!(!w_lo.overflow && !w_hi.overflow, "{}: overflow", p.name);

        let d_measured = w_hi.peak as i128 - w_lo.peak as i128;
        let d_with = post_agg_peak_bytes(&hi_in, &hi_aggs) as i128
            - post_agg_peak_bytes(&lo_in, &lo_aggs) as i128;
        let d_without = post_agg_peak_bytes_without(&hi_in, &hi_aggs, p.term) as i128
            - post_agg_peak_bytes_without(&lo_in, &lo_aggs, p.term) as i128;

        assert!(
            d_without < d_measured,
            "{}: NECESSITY — without the term the model covers {d_without} of the {d_measured} \
             incremental bytes the pair actually causes",
            p.name
        );
        assert!(
            d_measured <= d_with,
            "{}: SUFFICIENCY — the model covers only {d_with} of {d_measured} incremental bytes",
            p.name
        );
    }
}

/// `B_SERIES`/`B_POINT`/`B_LABEL`/`B_PAIR` — binary, **one-to-one,
/// `matching = on(id)`, no `group_left/right`**: `many` is `None` so the
/// `B_MANY` term is 0 in both fixtures and `include_bytes` is 0 in both,
/// which makes any `max_value_bytes` drift inert.
#[test]
fn each_binary_coefficient_is_necessary_by_its_paired_fixtures() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // §6's pre-commitment: one-to-one, `matching = None`, no group
    // modifier — so `B_MANY` is 0 in both fixtures and `include_bytes` is
    // 0 in both, which makes any `max_value_bytes` drift inert.
    let key = bk(MatchShape::NoMatching, BinGroup::NoGroup, Shape::Instant);
    let range_key = bk(MatchShape::NoMatching, BinGroup::NoGroup, Shape::Range);
    let base = Fixture {
        series: 256,
        ..BIN_BASE
    };
    let cases: Vec<(&str, BinaryTerm, BinaryKey, Fixture, Fixture)> = vec![
        // MATRIX operands: `points == series` on an instant vector, so an
        // instant `B_SERIES` pair cannot hold `points` constant and the
        // `B_POINT` term absorbs the whole increment.
        (
            "B_SERIES",
            BinaryTerm::Series,
            range_key,
            Fixture {
                series: 256,
                pairs: 16,
                steps: 16,
                ..base
            },
            Fixture {
                series: 4096,
                pairs: 1,
                steps: 1,
                ..base
            },
        ),
        (
            "B_POINT",
            BinaryTerm::Point,
            range_key,
            Fixture {
                series: 64,
                steps: 8,
                ..base
            },
            Fixture {
                series: 64,
                steps: 128,
                ..base
            },
        ),
        (
            "B_LABEL",
            BinaryTerm::Label,
            key,
            Fixture {
                value_bytes: 8,
                ..base
            },
            Fixture {
                value_bytes: 256,
                ..base
            },
        ),
        (
            "B_PAIR",
            BinaryTerm::Pair,
            key,
            Fixture {
                pairs: 4,
                value_bytes: 124,
                ..base
            },
            Fixture {
                pairs: 64,
                value_bytes: 4,
                ..base
            },
        ),
    ];

    for (name, term, k, lo, hi) in cases {
        let mut deltas = Vec::new();
        for fx in [lo, hi] {
            let matching = matching_for(&k, &[]);
            let (l, r, li, ri) = match k.shape {
                Shape::Instant => {
                    let a = build_vector(&fx, false);
                    let b = build_vector(&fx, false);
                    let li = measure_vector(&a);
                    let ri = measure_vector(&b);
                    (QueryResult::Vector(a), QueryResult::Vector(b), li, ri)
                }
                Shape::Range => {
                    let a = build_matrix(&fx, false);
                    let b = build_matrix(&fx, false);
                    let li = measure_matrix(&a);
                    let ri = measure_matrix(&b);
                    (QueryResult::Matrix(a), QueryResult::Matrix(b), li, ri)
                }
            };
            let m = matching.clone();
            let (out, w) = measure(move || combine_binary(BinOp::Add, false, m.as_ref(), l, r));
            drop(out.expect("binary pair fixture must combine"));
            assert!(!w.overflow, "{name}: overflow");
            deltas.push((li, ri, matching, w.peak));
        }
        let (llo, rlo, mlo, plo) = &deltas[0];
        let (lhi, rhi, mhi, phi) = &deltas[1];
        assert_eq!(
            include_bytes(mlo.as_ref(), BinOp::Add, rlo),
            0,
            "{name}: the one-to-one pair must carry no include amplification"
        );
        assert_eq!(
            include_bytes(mhi.as_ref(), BinOp::Add, rhi),
            0,
            "{name}: the one-to-one pair must carry no include amplification"
        );
        let no_aggs: [VectorAggSpec; 0] = [];
        assert_only_target_moves(
            name,
            &model_relevant(llo, Some(rlo), &no_aggs, mlo.as_ref(), BinOp::Add),
            &model_relevant(lhi, Some(rhi), &no_aggs, mhi.as_ref(), BinOp::Add),
            allowed_drift_bin(term),
        );

        let d_measured = *phi as i128 - *plo as i128;
        let d_with = binary_peak_bytes(BinOp::Add, mhi.as_ref(), lhi, rhi) as i128
            - binary_peak_bytes(BinOp::Add, mlo.as_ref(), llo, rlo) as i128;
        let d_without = binary_peak_bytes_without(BinOp::Add, mhi.as_ref(), lhi, rhi, term) as i128
            - binary_peak_bytes_without(BinOp::Add, mlo.as_ref(), llo, rlo, term) as i128;
        assert!(
            d_without < d_measured,
            "{name}: NECESSITY — without the term the model covers {d_without} of {d_measured}"
        );
        assert!(
            d_measured <= d_with,
            "{name}: SUFFICIENCY — the model covers {d_with} of {d_measured}"
        );
    }
}

/// §6.2 — `B_INCLUDE`: the `group_left(...)` LIST LENGTH is the only
/// thing that moves; the one side carries all `q` labels in BOTH
/// fixtures, so the measured inputs are byte-identical. Run in BOTH
/// orientations, because a many/one mix-up charges the wrong side.
#[test]
fn b_include_is_necessary_in_both_group_orientations() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let fx = Fixture {
        series: 256,
        ..BIN_BASE
    };
    for group in [BinGroup::GroupLeft, BinGroup::GroupRight] {
        let key = bk(MatchShape::On, group, Shape::Instant);
        let mut obs = Vec::new();
        for q in [1usize, 64] {
            let inc = include_names(q);
            let inc_refs: Vec<&str> = inc.iter().map(String::as_str).collect();
            let matching = matching_for(&key, &inc_refs);
            // The one side is the operand `instant_join` picks as `one`.
            let many = build_vector(&fx, false);
            let one = one_side_with_includes(&fx, 64);
            let (lhs, rhs, li, ri) = match group {
                BinGroup::GroupLeft => {
                    let li = measure_vector(&many);
                    let ri = measure_vector(&one);
                    (QueryResult::Vector(many), QueryResult::Vector(one), li, ri)
                }
                BinGroup::GroupRight | BinGroup::NoGroup => {
                    let li = measure_vector(&one);
                    let ri = measure_vector(&many);
                    (QueryResult::Vector(one), QueryResult::Vector(many), li, ri)
                }
            };
            let m = matching.clone();
            let (out, w) = measure(move || combine_binary(BinOp::Add, false, m.as_ref(), lhs, rhs));
            drop(out.expect("include pair must combine"));
            assert!(!w.overflow, "B_INCLUDE {group:?}: overflow");
            obs.push((li, ri, matching, w.peak));
        }
        let (llo, rlo, mlo, plo) = &obs[0];
        let (lhi, rhi, mhi, phi) = &obs[1];
        let no_aggs: [VectorAggSpec; 0] = [];
        assert_only_target_moves(
            "B_INCLUDE",
            &model_relevant(llo, Some(rlo), &no_aggs, mlo.as_ref(), BinOp::Add),
            &model_relevant(lhi, Some(rhi), &no_aggs, mhi.as_ref(), BinOp::Add),
            allowed_drift_bin(BinaryTerm::Include),
        );
        let d_measured = *phi as i128 - *plo as i128;
        let d_with = binary_peak_bytes(BinOp::Add, mhi.as_ref(), lhi, rhi) as i128
            - binary_peak_bytes(BinOp::Add, mlo.as_ref(), llo, rlo) as i128;
        let d_without =
            binary_peak_bytes_without(BinOp::Add, mhi.as_ref(), lhi, rhi, BinaryTerm::Include)
                as i128
                - binary_peak_bytes_without(BinOp::Add, mlo.as_ref(), llo, rlo, BinaryTerm::Include)
                    as i128;
        assert_eq!(
            d_without, 0,
            "B_INCLUDE {group:?}: the pair must make the without-model increment exactly zero"
        );
        assert!(
            d_measured > 0,
            "B_INCLUDE {group:?}: the include chain must cost measurable bytes"
        );
        assert!(
            d_measured <= d_with,
            "B_INCLUDE {group:?}: SUFFICIENCY — model covers {d_with} of {d_measured}"
        );
    }
}

/// §6.4 — `B_MANY` is a DECLARED EXCEPTION: no single pair can isolate
/// it, because `many` is SELECTED by the matching clause rather than
/// being an independent axis, and any pair that moves `many.series` also
/// moves `B_SERIES · (N_l + N_r)`.
///
/// The replacement is a 2x2 difference-of-differences whose cancellation
/// is ARITHMETIC, not a fixture property: four fixtures sharing one
/// identical `on(id)` clause, varying only the group modifier and the
/// many-side width.
///
/// Pre-committed decision rule: `ΔΔmeasured <= 0` means the grouped path
/// costs no more per many-side series than the one-to-one path, `B_MANY`
/// is not a necessary term, and it is DELETED from the model.
#[test]
fn b_many_is_discriminated_by_a_difference_of_differences() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut peaks = [0i128; 4];
    let mut models = [0i128; 4];
    let mut models_without = [0i128; 4];
    for (ix, (group, series)) in [
        (BinGroup::NoGroup, 512u64), // A
        (BinGroup::NoGroup, 1024),   // B
        (BinGroup::GroupLeft, 512),  // C
        (BinGroup::GroupLeft, 1024), // D
    ]
    .into_iter()
    .enumerate()
    {
        let fx = Fixture { series, ..BIN_BASE };
        let key = bk(MatchShape::On, group, Shape::Instant);
        // The group modifier carries an EMPTY include list, so
        // `B_INCLUDE` is zero in all four.
        let matching = matching_for(&key, &[]);
        let a = build_vector(&fx, false);
        let b = build_vector(&fx, false);
        let li = measure_vector(&a);
        let ri = measure_vector(&b);
        let m = matching.clone();
        let (out, w) = measure(move || {
            combine_binary(
                BinOp::Add,
                false,
                m.as_ref(),
                QueryResult::Vector(a),
                QueryResult::Vector(b),
            )
        });
        drop(out.expect("b_many fixture must combine"));
        assert!(!w.overflow, "B_MANY cell {ix}: overflow");
        peaks[ix] = w.peak as i128;
        models[ix] = binary_peak_bytes(BinOp::Add, matching.as_ref(), &li, &ri) as i128;
        models_without[ix] =
            binary_peak_bytes_without(BinOp::Add, matching.as_ref(), &li, &ri, BinaryTerm::Many)
                as i128;
    }
    let dd_measured = (peaks[3] - peaks[2]) - (peaks[1] - peaks[0]);
    let dd_with = (models[3] - models[2]) - (models[1] - models[0]);
    let dd_without =
        (models_without[3] - models_without[2]) - (models_without[1] - models_without[0]);

    assert_eq!(
        dd_without, 0,
        "the second difference must cancel every other term EXACTLY (arithmetic, not fixture)"
    );
    assert!(
        dd_measured > 0,
        "PRE-COMMITTED RULE: ΔΔmeasured = {dd_measured} <= 0 means the grouped path costs no \
         more per many-side series than the one-to-one path, so B_MANY is NOT a necessary term \
         and must be DELETED from the model and the constants"
    );
    assert!(
        dd_measured <= dd_with,
        "SUFFICIENCY: ΔΔmodel_with = {dd_with} must cover ΔΔmeasured = {dd_measured}"
    );
}

/// §6.3 — both operands are live: `l` stays live while `r` evaluates and
/// both enter `combine_binary`. The discriminating comparison is against
/// a model that reads only ONE operand.
#[test]
fn both_binary_operands_are_inside_the_bound() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let narrow = Fixture {
        series: 64,
        ..BIN_BASE
    };
    let wide = Fixture {
        series: 1024,
        ..BIN_BASE
    };
    let key = bk(MatchShape::NoMatching, BinGroup::NoGroup, Shape::Instant);
    let mut obs = Vec::new();
    for rhs_fx in [narrow, wide] {
        let matching = matching_for(&key, &[]);
        let a = build_vector(&wide, false);
        let b = build_vector(&rhs_fx, false);
        let li = measure_vector(&a);
        let ri = measure_vector(&b);
        let m = matching.clone();
        let (out, w) = measure(move || {
            combine_binary(
                BinOp::Add,
                false,
                m.as_ref(),
                QueryResult::Vector(a),
                QueryResult::Vector(b),
            )
        });
        drop(out.expect("both-operand fixture must combine"));
        assert!(!w.overflow, "6.3: overflow");
        obs.push((li, ri, matching, w.peak as i128));
    }
    let (llo, rlo, mlo, plo) = &obs[0];
    let (lhi, rhi, mhi, phi) = &obs[1];
    // A model reading only the LHS: both fixtures share the lhs, so its
    // increment is exactly zero.
    let one_operand = |l: &StageInput, m: Option<&VectorMatching>| {
        let empty = StageInput::default();
        binary_peak_bytes(BinOp::Add, m, l, &empty) as i128
    };
    let d_measured = phi - plo;
    let d_one = one_operand(lhi, mhi.as_ref()) - one_operand(llo, mlo.as_ref());
    let d_both = binary_peak_bytes(BinOp::Add, mhi.as_ref(), lhi, rhi) as i128
        - binary_peak_bytes(BinOp::Add, mlo.as_ref(), llo, rlo) as i128;
    assert_eq!(
        d_one, 0,
        "a one-operand model cannot see the rhs growing at all"
    );
    assert!(
        d_measured > 0,
        "growing only the rhs must cost measurable bytes"
    );
    assert!(
        d_measured <= d_both,
        "the two-operand model must cover the rhs increment ({d_measured} > {d_both})"
    );

    // The companion ARITHMETIC assertion (no measurement): two operands
    // each spending a WHOLE leaf budget bound strictly more than the same
    // two operands sharing one budget between them. Stated over budgets
    // rather than over a series count, because the operand's maximum is
    // NOT at `N = N_max` — at that width the leaf has spent its entire
    // budget on entries and has no label mass left.
    let independent = best_operand_within(MAX_CLIENT_AGG_GROUP_BYTES);
    let shared = best_operand_within(MAX_CLIENT_AGG_GROUP_BYTES / 2);
    let g = corner_matching(0);
    let both_full = binary_peak_bytes(BinOp::Add, Some(&g), &independent, &independent);
    let one_split = binary_peak_bytes(BinOp::Add, Some(&g), &shared, &shared);
    assert!(
        both_full > one_split,
        "each `MetricNode::Binary` child evaluates with its OWN AggCaps::DEFAULT, so one leaf \
         budget must not be shared across two operands ({both_full} vs {one_split})"
    );
}

/// §6.1 — the two-concurrent-all-retaining-stages workload. Its
/// discrimination is now `chain_depth_does_not_multiply_peak_memory`
/// (the pre-committed `topk(N, x) -> topk(N, topk(N, x))` pair yields
/// ZERO increment, measured); what stays load-bearing here is that the
/// workload is INSIDE the model at both depths and on both shapes.
#[test]
fn the_two_stage_all_retaining_workload_is_inside_the_model() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for shape in [Shape::Instant, Shape::Range] {
        let fx = Fixture {
            series: 512,
            ..CHAIN_BASE
        };
        for len in [1usize, 2] {
            let spec = (VectorAggOp::Topk, None, Some(fx.series as f64));
            let aggs = nested(spec, len);
            let result = match shape {
                Shape::Instant => QueryResult::Vector(build_vector(&fx, false)),
                Shape::Range => QueryResult::Matrix(build_matrix(&fx, false)),
            };
            let input = measure_result(&result);
            let (out, w) = measure(|| apply_vector_aggs(result, &aggs));
            drop(out.expect("a witness fixture must be admitted"));
            assert!(!w.overflow, "6.1 {shape:?}: overflow");
            let modelled = post_agg_peak_bytes(&input, &aggs);
            assert!(
                w.peak <= modelled,
                "6.1 {} at depth {len}: peak {} exceeds the model {modelled}",
                describe(shape),
                w.peak
            );
        }
    }
}

// =====================================================================
// 10. The cap derivation (AC 29, 31, 32)
// =====================================================================

/// A leaf-gated feasible-region corner at `n` series: the leaf's own
/// charge caps the entry cost at `s_min * n`, and everything it did not
/// spend there is available as label mass.
fn corner_operand(n: u64) -> StageInput {
    corner_operand_within(n, MAX_CLIENT_AGG_GROUP_BYTES)
}

/// The same at an arbitrary leaf budget — used by §6.3's companion
/// arithmetic assertion, which compares two whole budgets against one
/// budget split in half.
fn corner_operand_within(n: u64, budget: u64) -> StageInput {
    let smin = leaf_min_entry_bytes();
    let spent = smin.saturating_mul(n);
    let block = budget.saturating_sub(spent);
    // `label_bytes <= label_block_bytes`, and a pair's element buffer
    // alone costs `alloc_block_bytes(48 * p) / p = 96`, so
    // `label_pairs <= label_block_bytes / 96`.
    let pairs = block / 96;
    let points = MAX_METRIC_RESULT_POINTS.min(n.saturating_mul(MAX_ADMITTED_GRID_POINTS_LOCAL));
    StageInput::for_derivation(n, pairs, block, block, pairs, block, points, points)
}

/// `on(id00) group_left(<q single-character include names>)` — the
/// matching shape the binary derivation is maximised over. `q = 0` is the
/// non-amplifying corner (`include_bytes = 0`, `B_MANY` still on).
fn corner_matching(q: u64) -> VectorMatching {
    include_matching_of_names(q)
}

/// The single-operand model's maximum over every width a leaf of `budget`
/// bytes can produce. Walked exhaustively over the (bounded) width
/// domain.
fn best_operand_within(budget: u64) -> StageInput {
    let n_max = (budget / leaf_min_entry_bytes()).max(1);
    let mut best = corner_operand_within(1, budget);
    let mut best_value = 0u64;
    let empty = StageInput::default();
    for n in 1..=n_max {
        let m = corner_operand_within(n, budget);
        let v = binary_peak_bytes(BinOp::Add, Some(&corner_matching(0)), &empty, &m);
        if v > best_value {
            best_value = v;
            best = m;
        }
    }
    best
}

/// `MAX_ADMITTED_GRID_POINTS` is not re-exported by the crate root; this
/// mirrors its derivation (`2 * (MAX_CLIENT_AGG_BUCKETS + 1)`) and is
/// pinned against the shipped constant by
/// [`the_feasible_region_operands_are_read_from_the_shipped_constants`].
const MAX_ADMITTED_GRID_POINTS_LOCAL: u64 = 22_002;

/// The deepest chain the feasible region admits, carrying `names` bytes
/// of `by(...)` amplification on its outermost stage. `names == 0` is the
/// NON-AMPLIFYING corner the cap is derived over; the O6 threshold test
/// re-uses this so the chain it evaluates is the SAME one `X_chain(N)`
/// was maximised over.
fn corner_chain(names: u64, stages: usize) -> Vec<VectorAggSpec> {
    let mut aggs = nested((VectorAggOp::ApproxTopk, None, Some(1.0)), stages);
    if names > 0 {
        aggs[0].1 = Some(by_clause_of_total_bytes(names));
    }
    aggs
}

/// The feasible region's series ceiling.
fn n_max() -> u64 {
    MAX_CLIENT_AGG_GROUP_BYTES / leaf_min_entry_bytes()
}

/// The maximum chain depth the feasible region admits: every nesting
/// level costs at least `sum(` in the query text.
fn max_stages(query_bytes: u64, max_depth: u64) -> u64 {
    max_depth.min(query_bytes / 4)
}

struct Derivation {
    x_chain: u64,
    x_chain_argmax: u64,
    x_bin: u64,
    x_bin_argmax: u64,
    cap: u64,
    stages: u64,
    n_max: u64,
    s_min: u64,
    /// O6: the smallest TOTAL `by`-name byte count anywhere in `D` at
    /// which the chain funnel CAN refuse.
    a_min: Option<u64>,
    a_min_argmin: u64,
    /// O7: the smallest `N_many * include_bytes` PRODUCT anywhere in `D`
    /// at which the binary funnel CAN refuse.
    amp_min: Option<u64>,
    amp_min_argmin: u64,
}

/// The smallest by-name contribution a `by` name can make: a
/// one-character name plus its separator.
const A_NAME_MIN: u64 = 2;

/// `X_chain(N)` and `X_bin(N)` for every `N` in the feasible region,
/// computed ONCE per process. The domain is bounded (`N <=
/// MAX_CLIENT_AGG_GROUP_BYTES / s_min`), so it is walked EXHAUSTIVELY
/// rather than sampled at the corners the objective is "obviously"
/// maximised at — the same idiom that found the 24-permutation and
/// `i128`-narrowing defects earlier in this issue.
struct Region {
    /// `(x_chain(n), x_bin(n), the one side's best width <= n)` indexed
    /// by `n - 1`.
    per_n: Vec<(u64, u64, u64)>,
    stages: u64,
    n_max: u64,
    s_min: u64,
}

static REGION: std::sync::OnceLock<Region> = std::sync::OnceLock::new();

fn region() -> &'static Region {
    REGION.get_or_init(|| {
        let s_min = leaf_min_entry_bytes();
        let n_max = n_max();
        let stages = max_stages(pulsus_logql::MAX_QUERY_BYTES as u64, 64);
        // Hoisted: the chain at its deepest, with the flat `approx_topk`
        // term present (a legal chain member, so it belongs in the max).
        let aggs = corner_chain(0, stages as usize);
        let empty = StageInput::default();
        // The binary funnel's NON-AMPLIFYING corner still carries a group
        // modifier: `a / on(x) group_left() b` has `include_bytes = 0`
        // and is a perfectly legal query, and the grouped arm's
        // `many_matched` map is exactly what `B_MANY` prices. Deriving
        // `X_bin` without it would under-bound every grouped binary by
        // `B_MANY * many.series`.
        let grouped = corner_matching(0);

        let mut per_n = Vec::with_capacity(n_max as usize);
        // The one side's own best width, as a running prefix maximum over
        // `N_r <= N_l` — which makes the binary maximum EXACT in one pass
        // instead of quadratic (by symmetry, assume `N_l >= N_r`; the
        // `B_MANY` term then reads the lhs).
        let mut best_one = corner_operand(1);
        let mut best_one_n = 1u64;
        let mut best_one_value = 0u64;
        for n in 1..=n_max {
            let m = corner_operand(n);
            let x_chain = post_agg_peak_bytes(&m, &aggs);
            // The rhs slot carries no `B_MANY` term (many = lhs), so this
            // is exactly the one side's own contribution.
            let one_only = binary_peak_bytes(BinOp::Add, Some(&grouped), &empty, &m);
            if one_only > best_one_value {
                best_one_value = one_only;
                best_one = m;
                best_one_n = n;
            }
            let x_bin = binary_peak_bytes(BinOp::Add, Some(&grouped), &m, &best_one);
            per_n.push((x_chain, x_bin, best_one_n));
        }
        Region {
            per_n,
            stages,
            n_max,
            s_min,
        }
    })
}

/// Derives every published number from the shipped coefficients over the
/// cached feasible region.
fn derive(cap_override: Option<u64>) -> Derivation {
    let r = region();
    let (mut x_chain, mut x_chain_argmax) = (0u64, 1u64);
    let (mut x_bin, mut x_bin_argmax) = (0u64, 1u64);
    for (i, &(c, b, _)) in r.per_n.iter().enumerate() {
        let n = i as u64 + 1;
        if c > x_chain {
            x_chain = c;
            x_chain_argmax = n;
        }
        if b > x_bin {
            x_bin = b;
            x_bin_argmax = n;
        }
    }
    let cap = cap_override.unwrap_or_else(|| x_chain.max(x_bin).next_power_of_two());

    // D = { N >= 1 : X(N) <= CAP }. Empty D means the funnel refuses
    // everywhere and there is no threshold to publish.
    let mut a_min: Option<u64> = None;
    let mut a_min_argmin = 0u64;
    let mut amp_min: Option<u64> = None;
    let mut amp_min_argmin = 0u64;
    for (i, &(c, b, _)) in r.per_n.iter().enumerate() {
        let n = i as u64 + 1;
        if c <= cap && W_GROUPNAME > 0 {
            let head = (cap - c) / W_GROUPNAME.saturating_mul(n) + 1;
            if a_min.is_none_or(|cur| head < cur) {
                a_min = Some(head);
                a_min_argmin = n;
            }
        }
        if b <= cap && B_INCLUDE > 0 {
            let head = (cap - b) / B_INCLUDE + 1;
            if amp_min.is_none_or(|cur| head < cur) {
                amp_min = Some(head);
                amp_min_argmin = n;
            }
        }
    }

    Derivation {
        x_chain,
        x_chain_argmax,
        x_bin,
        x_bin_argmax,
        cap,
        stages: r.stages,
        n_max: r.n_max,
        s_min: r.s_min,
        a_min,
        a_min_argmin,
        amp_min,
        amp_min_argmin,
    }
}

#[test]
fn the_feasible_region_operands_are_read_from_the_shipped_constants() {
    assert_eq!(
        MAX_ADMITTED_GRID_POINTS_LOCAL,
        2 * (MAX_CLIENT_AGG_BUCKETS + 1),
        "the local grid-point ceiling must track the shipped fence"
    );
    // Every nesting level costs at least `sum(` (4 bytes) of query
    // text, so the region's stage operand is `min(MAX_DEPTH, Q/4)`.
    // Each term is exercised WHERE IT BINDS (fix round 5's `[medium]`:
    // the former `max_stages(q, 64) == 64.min(q / 4)` restated the
    // function body at a point where MAX_DEPTH dominates — `q/4 =
    // 32 768` and `q/8 = 16 384` both clamp to 64 — so a `/ 8` mutant
    // stayed green; a gate must have an input at which the guarded
    // thing changing gives a different answer).
    let q = pulsus_logql::MAX_QUERY_BYTES as u64;
    // Depth ceiling out of the way -> the DIVISOR decides, at the real
    // shipped `q`: 131 072 / 4 nesting levels. A `/ 8` body answers
    // 16 384 here (tripped for real, fix round 5).
    assert_eq!(max_stages(q, u64::MAX), 32_768, "Q / 4 where Q binds");
    // Divisor out of the way -> the DEPTH ceiling decides.
    assert_eq!(max_stages(q, 64), 64, "MAX_DEPTH where it binds");

    // The derivation's NON-AMPLIFYING corner is genuinely the zero
    // corner, gated on the BUILT values rather than the constructors'
    // word (fix round 5 re-examination: a corner that silently carried
    // amplifier bytes would shift the X baseline and the O-threshold
    // brackets EQUALLY, so the bracket tests alone cannot detect it —
    // the same same-shift blindness the divisor gate had).
    let stages = max_stages(q, 64) as usize;
    assert_eq!(
        group_name_bytes(&corner_chain(0, stages)),
        0,
        "the chain corner must carry zero by-name bytes"
    );
    assert_eq!(
        include_bytes(Some(&corner_matching(0)), BinOp::Add, &corner_operand(1)),
        0,
        "the binary corner must carry zero include bytes"
    );
    let l0 = lr_template_of_len(0);
    assert_eq!(
        l0.dst.len() + l0.replacement.len(),
        0,
        "the label_replace corner must carry a zero-length template"
    );
    assert!(
        !l0.replacement.contains('$'),
        "the label_replace corner must carry no `$` reference"
    );
}

/// **The cap is derived, and only the safety direction gates.** There is
/// deliberately NO upper-bound assertion on `MAX_POST_AGG_BYTES`: a
/// change that REDUCES peak memory (#245's Part C deletes two `BTreeMap`
/// indexes and a `BTreeSet` union from `combine_matrices`) must never
/// redden CI.
#[test]
fn the_cap_covers_the_feasible_region_at_the_non_amplifying_corner() {
    let d = derive(Some(MAX_POST_AGG_BYTES));
    assert!(
        d.x_chain <= MAX_POST_AGG_BYTES,
        "X_chain = {} exceeds MAX_POST_AGG_BYTES = {MAX_POST_AGG_BYTES}",
        d.x_chain
    );
    assert!(
        d.x_bin <= MAX_POST_AGG_BYTES,
        "X_bin = {} exceeds MAX_POST_AGG_BYTES = {MAX_POST_AGG_BYTES}",
        d.x_bin
    );
    assert_eq!(
        MAX_POST_AGG_BYTES,
        d.x_chain.max(d.x_bin).next_power_of_two(),
        "the shipped cap must be the smallest power of two >= max(X_chain, X_bin)"
    );

    // Both maxima are INTERIOR to the region (the point cap kinks the
    // objective at `MAX_METRIC_RESULT_POINTS / MAX_ADMITTED_GRID_POINTS`
    // and it falls away on both sides), which is why the derived cap is
    // insensitive to `s_min`: widening or narrowing the leaf-entry slot
    // moves `N_max`, and every `N` it adds or removes sits BELOW the
    // maximum. Stated as an assertion rather than as a note, because it
    // stops holding the moment a coefficient change pushes an argmax onto
    // the boundary — and then `s_min` becomes load-bearing again.
    // Both maxima sit exactly at the KINK where the point term stops
    // growing (`P(N) = min(MAX_METRIC_RESULT_POINTS, N x
    // MAX_ADMITTED_GRID_POINTS)`), which is a point no corner sample
    // visits — so this is what the exhaustive walk buys over evaluating
    // the region's endpoints.
    let kink = MAX_METRIC_RESULT_POINTS.div_ceil(MAX_ADMITTED_GRID_POINTS_LOCAL);
    assert_eq!(
        (d.x_chain_argmax, d.x_bin_argmax),
        (kink, kink),
        "both maxima must sit at the point-cap kink N = {kink}; if a coefficient change moved \
         one, the printed argmax says where, and the region must still be walked exhaustively"
    );
    for (name, argmax) in [("X_chain", d.x_chain_argmax), ("X_bin", d.x_bin_argmax)] {
        assert!(
            argmax > 1 && argmax < d.n_max,
            "{name}'s maximum is on the region BOUNDARY (argmax = {argmax}, N_max = {}); the \
             derivation is now sensitive to s_min and the leaf-entry slot sizing must be \
             re-checked",
            d.n_max
        );
    }

    // A client-leaf-sourced input at the leaf maxima with no grouping-name
    // and no include amplification is ADMITTED.
    let m = corner_operand(d.n_max);
    let aggs = nested((VectorAggOp::Sum, None, None), d.stages as usize);
    assert!(
        post_agg_peak_bytes(&m, &aggs) <= MAX_POST_AGG_BYTES,
        "a non-amplifying client-leaf input at the leaf maximum must be admitted"
    );
}

/// O6/O7 carry their COEFFICIENTS (round 12's `[high]`): the threshold
/// divides by `W_GROUPNAME * N`, not by `N`, and by `B_INCLUDE`, not by
/// nothing. Asserted at the MODEL level, unconditionally — the
/// end-to-end half is `#236`'s §4 funnel, which does not exist yet.
#[test]
fn o6_and_o7_are_numbers_that_bound_where_refusal_is_possible() {
    let d = derive(Some(MAX_POST_AGG_BYTES));
    let r = region();
    let a_min = d.a_min.expect(
        "an empty domain D means the funnel refuses everywhere, which would make the CAP \
         derivation wrong — a non-amplifying client-leaf input must be admitted",
    );
    let amp_min = d
        .amp_min
        .expect("an empty domain D for the binary funnel — see above");

    // ---- O6 ----
    let stages = d.stages as usize;
    let chain_below = corner_chain(a_min - 1, stages);
    let chain_at = corner_chain(a_min, stages);
    assert_eq!(
        group_name_bytes(&chain_below),
        a_min - 1,
        "the by-clause builder must produce EXACTLY the requested amplifier"
    );
    assert_eq!(group_name_bytes(&chain_at), a_min);

    let n = d.a_min_argmin;
    let m = corner_operand(n);
    assert!(
        post_agg_peak_bytes(&m, &chain_below) <= MAX_POST_AGG_BYTES,
        "O6: a by-clause of {} bytes must still be admitted at N = {n}",
        a_min - 1
    );
    assert!(
        post_agg_peak_bytes(&m, &chain_at) > MAX_POST_AGG_BYTES,
        "O6: a by-clause of {a_min} bytes must be refusable at N = {n}"
    );

    // Strictly below A_MIN, refusal is impossible ANYWHERE in D —
    // enumerated over the whole domain, not sampled at its corners.
    for i in 0..r.per_n.len() {
        let n = i as u64 + 1;
        assert!(
            post_agg_peak_bytes(&corner_operand(n), &chain_below) <= MAX_POST_AGG_BYTES,
            "O6: refusal must be impossible below A_MIN = {a_min} at any group count (N = {n})"
        );
    }

    // ---- O7 ----
    // The include amplifier's granularity is `N_many * (1 + V + 1)` per
    // name, so the achievable products are a lattice; the threshold is
    // BRACKETED by the two neighbouring lattice points rather than hit
    // exactly. `max_value_bytes` enters `binary_peak_bytes` ONLY through
    // `include_bytes`, so setting it to zero on the one side sharpens the
    // lattice without moving `X_bin(N)` — asserted below.
    let n = d.amp_min_argmin;
    let many = corner_operand(n);
    let one_wide = corner_operand(r.per_n[n as usize - 1].2);
    let one = with_max_value_bytes(&one_wide, 0);
    assert_eq!(
        binary_peak_bytes(BinOp::Add, Some(&corner_matching(0)), &many, &one),
        r.per_n[n as usize - 1].1,
        "narrowing the one side's longest VALUE must not move X_bin(N)"
    );

    let per_name = 1 + one.max_value_bytes() + 1;
    let stride = n.saturating_mul(per_name);
    let q_at = amp_min.div_ceil(stride).max(1);
    let q_below = q_at - 1;
    let product_at = stride.saturating_mul(q_at);
    let product_below = stride.saturating_mul(q_below);
    assert!(
        product_below < amp_min && product_at >= amp_min,
        "O7: the achievable products {product_below} and {product_at} must bracket \
         AMP_MIN = {amp_min}"
    );
    let at = corner_matching(q_at);
    assert!(
        binary_peak_bytes(BinOp::Add, Some(&at), &many, &one) > MAX_POST_AGG_BYTES,
        "O7: an include product of {product_at} (>= AMP_MIN = {amp_min}) must be refusable at \
         N = {n}"
    );
    if q_below > 0 {
        let below = corner_matching(q_below);
        assert!(
            binary_peak_bytes(BinOp::Add, Some(&below), &many, &one) <= MAX_POST_AGG_BYTES,
            "O7: an include product of {product_below} (< AMP_MIN = {amp_min}) must be admitted"
        );
    }
}

/// O8's derived numbers (issue #276 fix round 2): the `label_replace`
/// TEMPLATE threshold, in O6's vocabulary. `L = dst.len() +
/// replacement.len() + #'$' × max_value_bytes` is the per-series
/// template length `label_replace_peak_bytes` prices at `2 ×
/// W_LABEL_BYTE × L × N` exactly, so the smallest refusable `L` at each
/// `N` is `(CAP − X_lr(N)) / (2 × W_LABEL_BYTE × N) + 1`.
struct O8 {
    /// `max_N X_lr(N)` at the non-amplifying template corner (`L = 0`).
    x_lr: u64,
    x_lr_argmax: u64,
    /// The smallest `L` anywhere in `D` at which refusal is possible.
    l_min: Option<u64>,
    l_min_argmin: u64,
}

fn derive_o8(cap: u64) -> O8 {
    let l0 = lr_template_of_len(0);
    let mut o = O8 {
        x_lr: 0,
        x_lr_argmax: 1,
        l_min: None,
        l_min_argmin: 0,
    };
    for n in 1..=n_max() {
        let v = label_replace_peak_bytes(&corner_operand(n), &l0);
        if v > o.x_lr {
            o.x_lr = v;
            o.x_lr_argmax = n;
        }
        if v <= cap {
            let head = (cap - v) / (2 * W_LABEL_BYTE).saturating_mul(n) + 1;
            if o.l_min.is_none_or(|cur| head < cur) {
                o.l_min = Some(head);
                o.l_min_argmin = n;
            }
        }
    }
    o
}

/// A `label_replace` spec whose template length `L` is exactly `l`: an
/// empty destination and a `$`-free replacement of `l` bytes. The model
/// reads only the LENGTHS and the `$` count, so this stands for every
/// spec of that `L`.
fn lr_template_of_len(l: usize) -> LabelReplaceSpec {
    LabelReplaceSpec::compile("", &"r".repeat(l), "s", ".*").expect("a $-free template compiles")
}

/// O8 — `label_replace`'s template amplification, gated in O6's idiom
/// (issue #276 fix round 2, which found the published guarantee FALSE
/// as previously worded: the `2·per_series·series` term scales with a
/// quantity the ceiling's generator never varied). Three claims, each
/// the mechanism behind one sentence of `MAX_POST_AGG_BYTES`' doc:
/// at `L = 0` the WHOLE region is admitted (the guarantee's third
/// clause); strictly below `L_MIN` refusal is impossible anywhere in
/// `D`; at `L_MIN` it is possible. This funnel is WIRED
/// (`apply_label_replace` refuses live), so the divergence carries a
/// ledger row: `label-replace-template-amplification`. So are O6's and
/// O7's — `both_amplifiers_are_refused_end_to_end_from_query_text`
/// drives those two from real query text, and they carry rows (d) and
/// (e) of the issue #236 section.
#[test]
fn o8_the_label_replace_template_threshold_bounds_where_refusal_is_possible() {
    let o = derive_o8(MAX_POST_AGG_BYTES);
    assert!(
        o.x_lr <= MAX_POST_AGG_BYTES,
        "the guarantee's third clause: a template-free label_replace over every \
         client-leaf-sourced input must be admitted (X_lr = {}, argmax N = {})",
        o.x_lr,
        o.x_lr_argmax
    );
    let l_min = o.l_min.expect("an empty domain D — see O6");
    let at = lr_template_of_len(l_min as usize);
    let below = lr_template_of_len(l_min as usize - 1);
    assert!(
        label_replace_peak_bytes(&corner_operand(o.l_min_argmin), &at) > MAX_POST_AGG_BYTES,
        "O8: a template of {l_min} bytes must be refusable at N = {}",
        o.l_min_argmin
    );
    // Strictly below L_MIN, refusal is impossible ANYWHERE in D —
    // enumerated over the whole domain, not sampled at its corners.
    for n in 1..=n_max() {
        assert!(
            label_replace_peak_bytes(&corner_operand(n), &below) <= MAX_POST_AGG_BYTES,
            "O8: refusal must be impossible below L_MIN = {l_min} at any series count (N = {n})"
        );
    }
    // L is REACHABLE inside the query-text cap.
    assert!(
        l_min <= pulsus_logql::MAX_QUERY_BYTES as u64,
        "O8: L_MIN = {l_min} must fit inside MAX_QUERY_BYTES"
    );
    // The published `L` includes the `$` gearing: one `$` prices the
    // input's widest label VALUE per series, exactly.
    let m = corner_operand(1);
    let with_ref = LabelReplaceSpec::compile("", "$1", "s", "(.*)").expect("compiles");
    let plain = lr_template_of_len(2);
    assert_eq!(
        label_replace_peak_bytes(&m, &with_ref) - label_replace_peak_bytes(&m, &plain),
        2 * W_LABEL_BYTE * m.max_value_bytes(),
        "each `$` must add max_value_bytes to L"
    );
}

/// The same operand with a different longest label VALUE. Only
/// [`include_bytes`] reads that field, so this changes the include
/// amplifier's granularity and nothing else in the model.
fn with_max_value_bytes(m: &StageInput, v: u64) -> StageInput {
    StageInput::for_derivation(
        m.series(),
        m.label_pairs(),
        m.label_bytes(),
        m.label_block_bytes(),
        m.model_inputs()[4].1,
        v,
        m.points(),
        m.model_inputs()[7].1,
    )
}

/// `group_left(...)` with `q` single-character include names.
fn include_matching_of_names(q: u64) -> VectorMatching {
    VectorMatching {
        on: true,
        labels: vec!["id00".to_string()],
        group: Some(MatchGroup::Left((0..q).map(|_| "i".to_string()).collect())),
    }
}

// =====================================================================
// Fix round 3 (issue #276): the published figures are PINNED to the
// derivation
// =====================================================================

/// The `///` doc block of `MAX_POST_AGG_BYTES`, read from the SOURCE.
/// The published figures live in that prose and the derivation computes
/// the same quantities; until fix round 3 nothing asserted the two
/// equal, so the doc's `2 413` could drift to `2 414` with the suite
/// green — two sources of truth, and O6/O7 had carried the identical
/// gap since #236.
fn max_post_agg_doc() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/logql/post_agg.rs");
    let text = std::fs::read_to_string(&path).expect("read src/logql/post_agg.rs");
    let end = text
        .find("pub const MAX_POST_AGG_BYTES")
        .expect("MAX_POST_AGG_BYTES exists");
    let mut lines: Vec<&str> = Vec::new();
    for line in text[..end].lines().rev() {
        let Some(rest) = line.trim_start().strip_prefix("///") else {
            break;
        };
        lines.push(rest.strip_prefix(' ').unwrap_or(rest));
    }
    lines.reverse();
    lines.join("\n")
}

/// The O8 row of the divergence ledger — the SAME figures are published
/// there, so the same pins read it (a digit drifting in the ledger
/// alone would be the identical hole one file over).
fn ledger_o8_row() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/benchmarks/logs-differential-ledger.md");
    let text = std::fs::read_to_string(&path).expect("read the divergence ledger");
    let start = text
        .find("### `label-replace-template-amplification`")
        .expect("the O8 ledger row exists");
    let rest = &text[start..];
    let end = rest[4..].find("\n## ").map(|i| i + 4).unwrap_or(rest.len());
    rest[..end].to_string()
}

/// The section of `doc` under `header`, ending at the next `# ` header.
fn doc_section<'a>(doc: &'a str, header: &str) -> &'a str {
    let start = doc
        .find(header)
        .unwrap_or_else(|| panic!("doc section `{header}` is missing"));
    let rest = &doc[start + header.len()..];
    match rest.find("\n# ") {
        Some(end) => &rest[..end],
        None => rest,
    }
}

/// The first line of `scope` containing `key`.
fn doc_line<'a>(scope: &'a str, key: &str) -> &'a str {
    scope
        .lines()
        .find(|l| l.contains(key))
        .unwrap_or_else(|| panic!("no published line contains `{key}`"))
}

/// The first published figure after `key`: skips to the first ASCII
/// digit, then reads digit groups joined by single spaces (the prose
/// writes `2 847 288 941`).
fn figure(scope: &str, key: &str) -> u64 {
    let at = scope
        .find(key)
        .unwrap_or_else(|| panic!("`{key}` is not in the published text"));
    let bytes = &scope.as_bytes()[at + key.len()..];
    let mut i = 0;
    while i < bytes.len() && !bytes[i].is_ascii_digit() {
        i += 1;
    }
    let mut digits = String::new();
    while i < bytes.len() {
        if bytes[i].is_ascii_digit() {
            digits.push(bytes[i] as char);
            i += 1;
        } else if bytes[i] == b' ' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            i += 1; // a thousands separator inside one figure
        } else {
            break;
        }
    }
    digits
        .parse()
        .unwrap_or_else(|_| panic!("no figure after `{key}`"))
}

/// A figure in the prose's thousands-spaced rendering (`2 413`).
fn spaced(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(' ');
        }
        out.push(c);
    }
    out
}

/// Fix round 3's `[high]` (issue #276): every published O6/O7/O8 figure
/// — value AND argmin/argmax where the prose states one — plus the
/// generator's numbers block and O8's ledger row, asserted equal to
/// what the derivation computes. The figures are read OUT OF the
/// shipped prose, so a mutant changing any published digit reddens
/// here (verified by mutation for O6, O7, O8 and the ledger row).
///
/// Fix round 4 extended it to every REMAINING figure: the
/// `MAX_QUERY_BYTES` cross-references (pinned against the constant
/// itself, never a retyped number), the cap's GiB restatements, the
/// prose margin factor, the tightness ratio's VALUE, and a
/// stated-exactly-once pin on each threshold — a figure published twice
/// drifts eventually, and a drifted second copy beside a pinned first
/// would make the prose contradict itself while this test reports the
/// figure checked.
///
/// This does not conflict with "deliberately not pinned from above":
/// that note bars a TIGHTNESS assertion on the cap (`MAX_POST_AGG_BYTES
/// < k x max(X)`), which would punish a memory improvement. These are
/// EQUALITIES on the published figures: a coefficient change moves
/// prose and derivation in the same commit, and regeneration stays one
/// command (`zz_witness_report`). The tightness ratio follows the same
/// split: its printed VALUE is pinned, no bound on it is gated.
#[test]
fn the_published_figures_are_pinned_to_the_derivation() {
    let doc = max_post_agg_doc();
    let d = derive(Some(MAX_POST_AGG_BYTES));
    let o8 = derive_o8(MAX_POST_AGG_BYTES);

    // -- the generator's numbers block --
    let block = doc_section(&doc, "# The generator's numbers");
    assert_eq!(figure(block, "s_min"), d.s_min, "published s_min");
    assert_eq!(figure(block, "N_max"), d.n_max, "published N_max");
    assert_eq!(figure(block, "stages"), d.stages, "published stages");
    for (key, value, argmax) in [
        ("X_chain", d.x_chain, d.x_chain_argmax),
        ("X_bin", d.x_bin, d.x_bin_argmax),
        ("X_lr (L = 0)", o8.x_lr, o8.x_lr_argmax),
    ] {
        let line = doc_line(block, key);
        assert_eq!(figure(line, key), value, "published {key}");
        assert_eq!(figure(line, "argmax N ="), argmax, "published {key} argmax");
    }
    assert_eq!(
        figure(block, "MAX_POST_AGG_BYTES ="),
        MAX_POST_AGG_BYTES,
        "published cap"
    );

    // -- O6 --
    let o6 = doc_section(&doc, "# O6");
    let a_min = d.a_min.expect("O6 threshold exists");
    assert_eq!(figure(o6, "A_MIN ="), a_min, "published A_MIN");
    assert_eq!(
        figure(o6, "at `N ="),
        d.a_min_argmin,
        "published A_MIN argmin"
    );
    assert_eq!(
        figure(o6, "A_NAME_MIN ="),
        A_NAME_MIN,
        "published A_NAME_MIN"
    );
    assert_eq!(
        figure(o6, "at least"),
        a_min.div_ceil(A_NAME_MIN),
        "published one-character name count"
    );

    // -- O7 --
    let o7 = doc_section(&doc, "# O7");
    let amp_min = d.amp_min.expect("O7 threshold exists");
    assert_eq!(figure(o7, "AMP_MIN ="), amp_min, "published AMP_MIN");
    assert_eq!(
        figure(o7, "N_many ="),
        d.amp_min_argmin,
        "published AMP_MIN argmin"
    );

    // -- O8, in the doc --
    let o8s = doc_section(&doc, "# O8");
    let l_min = o8.l_min.expect("O8 threshold exists");
    let product_cap = MAX_POST_AGG_BYTES / (2 * W_LABEL_BYTE);
    let crossing_at_n_max = product_cap / d.n_max + 1;
    assert_eq!(figure(o8s, "L_MIN ="), l_min, "published L_MIN");
    assert_eq!(
        figure(o8s, "at `N ="),
        o8.l_min_argmin,
        "published L_MIN argmin"
    );
    assert_eq!(
        figure(o8s, "W_LABEL_BYTE) ="),
        product_cap,
        "published amplifier-alone product cap"
    );
    assert_eq!(figure(o8s, "N_max ="), d.n_max, "published O8 N_max");
    assert_eq!(
        figure(o8s, "replacement of"),
        crossing_at_n_max,
        "published crossing at N_max"
    );

    // -- O8, in the divergence ledger --
    let row = ledger_o8_row();
    assert_eq!(figure(&row, "below `L ="), l_min, "ledger L_MIN");
    assert_eq!(
        figure(&row, "`L_MIN`, at `N ="),
        o8.l_min_argmin,
        "ledger L_MIN argmin"
    );
    assert_eq!(
        figure(&row, "L × series >"),
        product_cap,
        "ledger product cap"
    );
    assert_eq!(
        figure(&row, "replacement of"),
        crossing_at_n_max,
        "ledger crossing at N_max"
    );
    assert_eq!(figure(&row, "N_max ="), d.n_max, "ledger N_max");

    // -- fix round 4: cross-referenced constants, restatements, the
    // tightness ratio, and stated-exactly-once --

    // `MAX_QUERY_BYTES` is a real constant; nothing may retype it.
    let q = pulsus_logql::MAX_QUERY_BYTES as u64;
    assert_eq!(figure(o6, "MAX_QUERY_BYTES ="), q, "O6 MAX_QUERY_BYTES");
    assert_eq!(figure(o8s, "MAX_QUERY_BYTES ="), q, "O8 MAX_QUERY_BYTES");
    assert_eq!(
        figure(&row, "bounded only by the"),
        q,
        "ledger query-text cap"
    );

    // The cap's `(8 GiB)` restatements track the pinned byte figure.
    let gib = MAX_POST_AGG_BYTES >> 30;
    assert_eq!(
        figure(doc_line(block, "MAX_POST_AGG_BYTES ="), "bytes"),
        gib,
        "published cap in GiB"
    );
    assert_eq!(
        figure(&row, "MAX_POST_AGG_BYTES` ("),
        gib,
        "ledger cap in GiB"
    );

    // The prose margin factor is the ladders' WITNESS_MARGIN.
    assert!(
        doc.contains(&format!("the {WITNESS_MARGIN}x")),
        "the doc's margin factor must be WITNESS_MARGIN = {WITNESS_MARGIN}"
    );

    // The tightness ratio's VALUE is pinned; no BOUND on it is gated —
    // bounding it is exactly what "deliberately not pinned from above"
    // forbids, because a memory improvement loosens it.
    let published_ratio = doc_line(block, "tightness ratio")
        .split('=')
        .nth(1)
        .and_then(|r| r.split_whitespace().next())
        .expect("a tightness ratio figure");
    let derived_ratio = format!(
        "{:.4}",
        MAX_POST_AGG_BYTES as f64 / d.x_chain.max(d.x_bin) as f64
    );
    assert_eq!(published_ratio, derived_ratio, "published tightness ratio");

    // Each threshold is stated EXACTLY once per document (fix round 4's
    // `[high]`: two second copies existed, and a drifted copy beside a
    // pinned one makes the prose contradict itself while this test
    // reports the figure checked).
    for (scope, name, value) in [
        (o6, "A_MIN in the O6 section", a_min),
        (o8s, "L_MIN in the O8 section", l_min),
        (row.as_str(), "L_MIN in the ledger row", l_min),
    ] {
        assert_eq!(
            scope.matches(&spaced(value)).count(),
            1,
            "{name} must be published exactly once"
        );
    }
}

/// **AC 31's END-TO-END half.** The generator's reachability verdict says
/// both amplifiers are expressible inside `MAX_QUERY_BYTES`, so the
/// verdict governs and this asserts the refusal from real QUERY TEXT —
/// parsed, planned, then driven through the public `apply_vector_aggs` /
/// `combine_binary` at their shipped cap.
///
/// The refusal is exercised at a HERMETIC group count, not at the
/// derivation's argmin: `A_MIN`/`AMP_MIN` are the smallest amplifiers at
/// which refusal is possible ANYWHERE in the region, and a wider
/// amplifier refuses at proportionally fewer series. The test asserts the
/// amplifier it builds is at or above the published threshold, so it is
/// anchored to the derivation rather than to a number chosen to pass.
#[test]
fn both_amplifiers_are_refused_end_to_end_from_query_text() {
    let d = derive(Some(MAX_POST_AGG_BYTES));
    let a_min = d.a_min.expect("O6 threshold");
    let amp_min = d.amp_min.expect("O7 threshold");
    let q = pulsus_logql::MAX_QUERY_BYTES as u64;

    // ---- O6: a `by(...)` clause read off the query text ----
    // One name per 6 bytes ("nnnnn,"), filling the text cap with room for
    // the rest of the expression.
    let names: Vec<String> = (0..18_000).map(|i| format!("n{i:05}")).collect();
    let text = format!(
        "sum by ({}) (count_over_time({{app=\"a\"}}[5m]))",
        names.join(",")
    );
    assert!(
        (text.len() as u64) < q,
        "the O6 probe query must fit inside MAX_QUERY_BYTES ({} vs {q})",
        text.len()
    );
    let expr = pulsus_logql::parse(&text).expect("the by-clause probe must parse");
    // Issue #272: E0509 — re-bind through a reference and clone.
    let aggs = match &expr {
        pulsus_logql::Expr::Metric(pulsus_logql::MetricExpr::Vector { op, grouping, .. }) => {
            vec![(*op, grouping.clone(), None)]
        }
        other => panic!("expected a vector aggregation, got {other:?}"),
    };
    let amplifier = group_name_bytes(&aggs);
    assert!(
        amplifier >= a_min,
        "the probe's by-clause carries {amplifier} bytes, below the published A_MIN = {a_min}"
    );

    let fx = Fixture {
        series: 4_096,
        ..CHAIN_BASE
    };
    let result = QueryResult::Vector(build_vector(&fx, false));
    match apply_vector_aggs(result, &aggs) {
        Err(ReadError::QueryTooBroad(TooBroadReason::MetricPostAggBytes { bytes, cap })) => {
            assert_eq!(cap, MAX_POST_AGG_BYTES);
            assert!(
                bytes > cap,
                "the refusal must name a breach: {bytes} vs {cap}"
            );
        }
        other => panic!("O6: expected MetricPostAggBytes, got {other:?}"),
    }

    // ---- O7: a `group_left(...)` include list read off the query text ----
    let inc: Vec<String> = (0..6_000).map(|i| format!("i{i:05}")).collect();
    let btext = format!(
        "count_over_time({{app=\"a\"}}[5m]) * on (id00) group_left ({}) \
         count_over_time({{app=\"b\"}}[5m])",
        inc.join(",")
    );
    assert!(
        (btext.len() as u64) < q,
        "the O7 probe query must fit inside MAX_QUERY_BYTES ({} vs {q})",
        btext.len()
    );
    let bexpr = pulsus_logql::parse(&btext).expect("the include probe must parse");
    // Issue #272: E0509 — re-bind through a reference and clone.
    let matching = match &bexpr {
        pulsus_logql::Expr::Metric(pulsus_logql::MetricExpr::Binary { modifier, .. }) => modifier
            .clone()
            .and_then(|m| m.matching)
            .expect("the probe carries an on/group_left clause"),
        other => panic!("expected a binary expression, got {other:?}"),
    };

    // The one side carries a wide label VALUE, which is what makes each
    // include name expensive; the many side is narrow.
    let many_fx = Fixture {
        series: 256,
        ..BIN_BASE
    };
    let many = build_vector(&many_fx, false);
    let one: Vec<VectorSample> = (0..many_fx.series)
        .map(|i| VectorSample {
            labels: vec![
                ("id00".to_string(), pad(i, many_fx.value_bytes)),
                ("wide".to_string(), "w".repeat(1_000)),
            ],
            value: 1.0,
        })
        .collect();
    let (lm, rm) = (measure_vector(&many), measure_vector(&one));
    let product = lm
        .series()
        .saturating_mul(include_bytes(Some(&matching), BinOp::Mul, &rm));
    assert!(
        product >= amp_min,
        "the probe's include amplification is {product}, below the published AMP_MIN = {amp_min}"
    );
    match combine_binary(
        BinOp::Mul,
        false,
        Some(&matching),
        QueryResult::Vector(many),
        QueryResult::Vector(one),
    ) {
        Err(ReadError::QueryTooBroad(TooBroadReason::MetricPostAggBytes { bytes, cap })) => {
            assert_eq!(cap, MAX_POST_AGG_BYTES);
            assert!(
                bytes > cap,
                "the refusal must name a breach: {bytes} vs {cap}"
            );
        }
        other => panic!("O7: expected MetricPostAggBytes, got {other:?}"),
    }
}

/// A `by(...)` clause whose TOTAL `group_name_bytes` is exactly `total`
/// (each name contributes `len + 1`).
fn by_clause_of_total_bytes(total: u64) -> Grouping {
    let mut labels = Vec::new();
    let mut left = total;
    while left >= A_NAME_MIN {
        let take = left.min(64);
        labels.push("n".repeat((take - 1) as usize));
        left -= take;
    }
    if left == 1 {
        // A name of length zero is not expressible; fold the odd byte
        // into the previous name.
        if let Some(last) = labels.last_mut() {
            last.push('n');
        } else {
            labels.push(String::new());
        }
    }
    Grouping {
        kind: GroupingKind::By,
        labels,
    }
}

/// A `group_left(...)` matching whose `include_bytes` is at least
/// `per_series`, given the one side's `max_value_bytes`.
fn include_matching(per_series: u64, max_value_bytes: u64) -> VectorMatching {
    let per_name = 1 + max_value_bytes + 1;
    let count = per_series.div_ceil(per_name.max(1)).max(1);
    VectorMatching {
        on: true,
        labels: vec!["id00".to_string()],
        group: Some(MatchGroup::Left(
            (0..count).map(|i| format!("i{i}")).collect(),
        )),
    }
}

/// A `group_left`/`group_right` on a set operation is a NO-OP
/// (`instant_join` returns through `set_op_join` before `include` is
/// read), so the model must charge no include amplification for one —
/// otherwise the same query is priced differently for `and` than for
/// `/`, and the binary cells cannot see it because they all carry an
/// EMPTY include list.
#[test]
fn set_operations_carry_no_include_amplification() {
    let one = StageInput::for_derivation(64, 128, 512, 512, 2, 32, 64, 1);
    let many = StageInput::for_derivation(64, 128, 512, 512, 2, 32, 64, 1);
    let m = include_matching_of_names(16);
    for op in ALL_BIN_OPS {
        let is_set = matches!(op, BinOp::And | BinOp::Or | BinOp::Unless);
        assert_eq!(
            include_bytes(Some(&m), op, &one) == 0,
            is_set,
            "{op:?}: include amplification must be zero for a set operation and non-zero \
             otherwise"
        );
        assert_eq!(
            binary_peak_bytes(op, Some(&m), &many, &one)
                == binary_peak_bytes_without(op, Some(&m), &many, &one, BinaryTerm::Include),
            is_set,
            "{op:?}: the include TERM must vanish for a set operation and only for one"
        );
    }
}

/// `binary_peak_bytes` must pick many/one EXACTLY as `instant_join` does
/// — `group_right` swaps the sides — or the amplification is charged
/// against the wrong operand.
#[test]
fn the_many_side_is_chosen_exactly_as_instant_join_chooses_it() {
    let wide = StageInput::for_derivation(1024, 2048, 8192, 8192, 2, 8, 1024, 1);
    let narrow = StageInput::for_derivation(4, 8, 32, 32, 2, 8, 4, 1);
    let inc = |g: MatchGroup| VectorMatching {
        on: true,
        labels: vec!["id00".to_string()],
        group: Some(g),
    };
    let names: Vec<String> = (0..8).map(|_| "i".to_string()).collect();
    let left = inc(MatchGroup::Left(names.clone()));
    let right = inc(MatchGroup::Right(names));
    // `group_left` => many = lhs; `group_right` => many = rhs. With the
    // WIDE operand on the left, `group_left` must charge the wide side
    // and `group_right` the narrow one.
    let with_left = binary_peak_bytes(BinOp::Add, Some(&left), &wide, &narrow);
    let with_right = binary_peak_bytes(BinOp::Add, Some(&right), &wide, &narrow);
    assert!(
        with_left > with_right,
        "group_left over a wide lhs must charge more many-side bytes than group_right \
         ({with_left} vs {with_right})"
    );
    // And symmetrically with the sides swapped.
    let swapped_left = binary_peak_bytes(BinOp::Add, Some(&left), &narrow, &wide);
    let swapped_right = binary_peak_bytes(BinOp::Add, Some(&right), &narrow, &wide);
    assert!(
        swapped_right > swapped_left,
        "with the wide operand on the RIGHT the roles invert ({swapped_right} vs {swapped_left})"
    );
}

/// The amplifying corner yields NO protection, verified from the shipped
/// numbers rather than from prose — and the arithmetic saturates rather
/// than wrapping.
#[test]
fn the_amplifying_corner_is_unusable_and_the_arithmetic_saturates() {
    let q = pulsus_logql::MAX_QUERY_BYTES as u64;
    let n_many_max = n_max();
    // `q <= Q/2` include names, each carrying a value bounded only by the
    // one side's whole label budget.
    let names = q / 2;
    let v = MAX_CLIENT_AGG_GROUP_BYTES;
    let amplified = (n_many_max as u128)
        .saturating_mul(q as u128 + names as u128 * v as u128)
        .saturating_mul(B_INCLUDE.max(1) as u128);
    assert!(
        amplified >= 1u128 << 62,
        "the amplified maximum {amplified} must exceed 2^62, so the next power of two is >= 2^63"
    );
    // A cap of 8 EiB admits every query that will ever exist on any
    // machine: deriving over the amplifying corner produces no
    // protection, whatever the coefficient turns out to be.
    const {
        assert!(
            B_INCLUDE >= 1,
            "the conclusion above is coefficient-independent at every B_INCLUDE >= 1"
        )
    };

    // Saturation: the model resolves an amplified query to u64::MAX,
    // never to a small wrapped value.
    let m = StageInput::for_derivation(u64::MAX / 2, 1, 1, 1, 1, 1, 1, 1);
    let aggs = vec![(
        VectorAggOp::Sum,
        Some(by_clause_of_total_bytes(1 << 20)),
        None,
    )];
    assert_eq!(
        post_agg_peak_bytes(&m, &aggs),
        u64::MAX,
        "an amplified chain must saturate"
    );
    let one = StageInput::for_derivation(1, 1, 1, 1, 1, u64::MAX / 4, 1, 1);
    let many = StageInput::for_derivation(u64::MAX / 2, 1, 1, 1, 1, 1, 1, 1);
    let matching = include_matching(1_000_000, u64::MAX / 4);
    assert_eq!(
        binary_peak_bytes(BinOp::Add, Some(&matching), &many, &one),
        u64::MAX,
        "an amplified binary combination must saturate"
    );
    const {
        assert!(
            u64::MAX > MAX_POST_AGG_BYTES,
            "a saturated model is above the cap, so the funnel refuses rather than admitting"
        )
    };
}

// =====================================================================
// 11. The generator (AC 33) and the large-scale cell (AC 25)
// =====================================================================

/// The witness report: run with
/// `CARGO_INCREMENTAL=0 cargo test -p pulsus-read --test logql_post_agg_witness
///  -- --ignored --nocapture zz_witness_report`.
///
/// Its stdout is the derivation of every shipped constant and is
/// reproduced in the issue's implementation notes. It is deliberately NOT
/// a byte-frozen fixture: a memory improvement must never redden a diff.
#[test]
#[ignore = "generator — run explicitly to re-derive the shipped constants"]
fn zz_witness_report() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let started = std::time::Instant::now();
    println!("== issue #236 §5 witness report ==");

    println!("\n-- chain coefficients --");
    for l in chain_ladders() {
        let started_ladder = std::time::Instant::now();
        let mut rate_max = 0;
        for skew in [Skew::Uniform, Skew::Concentrated] {
            let run = run_chain_ladder(&l, skew);
            println!("{}[{skew:?}] points = {:?}", l.name, run.points);
            println!("{}[{skew:?}] rates  = {:?}", l.name, run.rates());
            rate_max = rate_max.max(run.rate_max());
        }
        println!(
            "{}: rate_max = {rate_max}  =>  {} x {WITNESS_MARGIN} = {} [{:?}]",
            l.name,
            rate_max,
            rate_max * WITNESS_MARGIN,
            started_ladder.elapsed()
        );
    }

    println!("\n-- binary coefficients --");
    for l in bin_ladders() {
        let started_ladder = std::time::Instant::now();
        let mut rate_max = 0;
        for skew in [Skew::Uniform, Skew::Concentrated] {
            let run = run_bin_ladder(&l, skew, 256);
            println!("{}[{skew:?}] points = {:?}", l.name, run.points);
            println!("{}[{skew:?}] rates  = {:?}", l.name, run.rates());
            rate_max = rate_max.max(run.rate_max());
        }
        println!(
            "{}: rate_max = {rate_max}  =>  {} x {WITNESS_MARGIN} = {} [{:?}]",
            l.name,
            rate_max,
            rate_max * WITNESS_MARGIN,
            started_ladder.elapsed()
        );
    }

    println!("\n-- W_APPROX_TOPK (flat term) --");
    let mut flat = 0u64;
    for series in APPROX_FLAT_SERIES {
        for k in [KShape::KOne, KShape::KAll] {
            let key = ck(
                Shape::Instant,
                GroupShape::NoGrouping,
                ChainShape::Single,
                k,
                Driver::Direct,
            );
            let fx = Fixture {
                series,
                ..CHAIN_BASE
            };
            let (input, aggs, w) = run_chain(VectorAggOp::ApproxTopk, &key, &fx, 1);
            let without = post_agg_peak_bytes_without(&input, &aggs, ChainTerm::ApproxTopk);
            let excess = w.peak.saturating_sub(without);
            println!(
                "approx_topk[N = {series}, {}] peak = {}, model without the flat term = \
                 {without}, excess = {excess}",
                describe_k(k),
                w.peak
            );
            flat = flat.max(excess);
        }
    }
    println!(
        "W_APPROX_TOPK: excess = {flat}  =>  x {WITNESS_MARGIN} = {}",
        flat * WITNESS_MARGIN
    );

    println!("\n-- derivation --");
    let d = derive(None);
    println!("s_min                = {} bytes", d.s_min);
    println!("N_max                = {} series", d.n_max);
    println!("stages               = {}", d.stages);
    println!(
        "X_chain              = {} bytes (argmax N = {})",
        d.x_chain, d.x_chain_argmax
    );
    println!(
        "X_bin                = {} bytes (argmax N = {})",
        d.x_bin, d.x_bin_argmax
    );
    println!(
        "MAX_POST_AGG_BYTES   = {} bytes ({} GiB)",
        d.cap,
        d.cap / (1 << 30)
    );
    println!(
        "tightness ratio      = CAP / max(X) = {:.4} (printed, NOT gated)",
        d.cap as f64 / d.x_chain.max(d.x_bin) as f64
    );
    match d.a_min {
        Some(a) => println!(
            "O6  A_MIN            = {a} by-clause bytes (argmin N = {}), >= {} names of one char",
            d.a_min_argmin,
            a.div_ceil(A_NAME_MIN)
        ),
        None => println!("O6  A_MIN            = None (domain D is empty)"),
    }
    println!("O6  A_NAME_MIN       = {A_NAME_MIN}");
    match d.amp_min {
        Some(a) => println!(
            "O7  AMP_MIN          = {a} (N_many x include_bytes), argmin N = {}",
            d.amp_min_argmin
        ),
        None => println!("O7  AMP_MIN          = None (domain D is empty)"),
    }
    let q = pulsus_logql::MAX_QUERY_BYTES as u64;
    println!(
        "O6 reachability      = {} (A_MIN = {:?} vs Q = {q})",
        d.a_min.is_some_and(|a| a <= q),
        d.a_min
    );
    println!(
        "O7 reachability      = {} (AMP_MIN = {:?} vs the largest expressible product)",
        d.amp_min.is_some_and(|a| (a as u128)
            <= (d.n_max as u128)
                * (q as u128 + (q / 2) as u128 * MAX_CLIENT_AGG_GROUP_BYTES as u128)),
        d.amp_min
    );
    let o8 = derive_o8(d.cap);
    println!(
        "O8  X_lr(L = 0)      = {} bytes (argmax N = {})",
        o8.x_lr, o8.x_lr_argmax
    );
    match o8.l_min {
        Some(l) => println!(
            "O8  L_MIN            = {l} template bytes (argmin N = {}); amplifier-alone \
             product cap = {} byte-series",
            o8.l_min_argmin,
            d.cap / (2 * W_LABEL_BYTE)
        ),
        None => println!("O8  L_MIN            = None (domain D is empty)"),
    }
    println!(
        "O8 reachability      = {} (L_MIN = {:?} vs Q = {q})",
        o8.l_min.is_some_and(|l| l <= q),
        o8.l_min
    );
    println!("\nwall time = {:?}", started.elapsed());
}

/// The widths the flat `approx_topk` term is derived and gated at. A
/// FLAT term is masked at a large fixture (the input-scaled terms already
/// dominate the 1.5 MiB sketch), so it is derived where it is visible:
/// the SMALLEST inputs, which is also where under-bounding would be a
/// real safety hole.
const APPROX_FLAT_SERIES: [u64; 4] = [1, 2, 8, 64];

/// `approx_topk` allocates a fixed `CMS_DEPTH x CMS_WIDTH` counter grid
/// whatever its input size, so the model must cover it at ONE series.
#[test]
fn the_flat_approx_topk_term_bounds_a_minimal_input() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for series in APPROX_FLAT_SERIES {
        for k in [KShape::KOne, KShape::KAll] {
            let key = ck(
                Shape::Instant,
                GroupShape::NoGrouping,
                ChainShape::Single,
                k,
                Driver::Direct,
            );
            let fx = Fixture {
                series,
                ..CHAIN_BASE
            };
            let (input, aggs, w) = run_chain(VectorAggOp::ApproxTopk, &key, &fx, 1);
            assert!(!w.overflow, "approx_topk flat cell: overflow");
            let modelled = post_agg_peak_bytes(&input, &aggs);
            assert!(
                w.peak <= modelled,
                "approx_topk at N = {series} ({}): peak {} exceeds the model {modelled} — the \
                 flat sketch term does not cover a minimal input",
                describe_k(k),
                w.peak
            );
        }
    }
}

/// **Plan v14 §6.1's premise, corrected by measurement.** The chain does
/// NOT hold one buffer per stage: `select_k_instant`'s output collect is
/// in-place (`Zip`/`FilterMap` over `vec::IntoIter` are `SourceIter` +
/// `InPlaceIterable`), and every aggregation arm is non-expanding in both
/// series and points, so a later stage's input is never larger than an
/// earlier stage's. This gate pins BOTH halves of that, and reddens if a
/// future change ever makes chain depth accumulate — which is what would
/// make `W_STAGE_SERIES = 0` unsafe.
#[test]
fn chain_depth_does_not_multiply_peak_memory() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for (shape, group, op, k) in [
        (
            Shape::Instant,
            GroupShape::NoGrouping,
            VectorAggOp::Topk,
            KShape::KAll,
        ),
        (
            Shape::Instant,
            GroupShape::ByPresent,
            VectorAggOp::Topk,
            KShape::KAll,
        ),
        (
            Shape::Instant,
            GroupShape::ByPresent,
            VectorAggOp::Sum,
            KShape::NotParameterised,
        ),
        (
            Shape::Instant,
            GroupShape::Without,
            VectorAggOp::Sum,
            KShape::NotParameterised,
        ),
        (
            Shape::Range,
            GroupShape::NoGrouping,
            VectorAggOp::Topk,
            KShape::KAll,
        ),
        (
            Shape::Range,
            GroupShape::ByPresent,
            VectorAggOp::Topk,
            KShape::KAll,
        ),
        (
            Shape::Range,
            GroupShape::ByPresent,
            VectorAggOp::Sum,
            KShape::NotParameterised,
        ),
        (
            Shape::Range,
            GroupShape::Without,
            VectorAggOp::Sum,
            KShape::NotParameterised,
        ),
    ] {
        let fx = Fixture {
            series: 512,
            ..CHAIN_BASE
        };
        let mut peaks = Vec::new();
        for len in [1usize, 2, 4, 8, 64] {
            let spec = (op, grouping_for(group), param_for(k, fx.series));
            let aggs = nested(spec, len);
            let result = match shape {
                Shape::Instant => QueryResult::Vector(build_vector(&fx, false)),
                Shape::Range => QueryResult::Matrix(build_matrix(&fx, false)),
            };
            let (out, w) = measure(|| apply_vector_aggs(result, &aggs));
            drop(out.expect("a witness fixture must be admitted"));
            assert!(!w.overflow, "depth probe: overflow");
            peaks.push(w.peak);
        }
        let label = format!("{op:?} {} {}", describe(shape), describe_group(group));
        assert!(
            peaks[2] <= peaks[1] && peaks[3] <= peaks[1] && peaks[4] <= peaks[1],
            "{label}: chain depth beyond TWO stages must add nothing — peaks {peaks:?}"
        );
        assert!(
            peaks[1] <= peaks[0].saturating_mul(2),
            "{label}: at most two stage buffers may be concurrent — peaks {peaks:?}"
        );
    }
}

/// The nightly/dispatch leg's large-scale cell: `MAX_METRIC_RESULT_POINTS
/// / 8` points through the measured stage, extending the validated range
/// to ~10^6 and turning the linearity gate's extrapolation into an
/// observed property one order further out.
#[test]
#[ignore = "large-scale cell — nightly/dispatch leg only"]
fn zz_large_scale_cell_stays_inside_the_model() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let target = MAX_METRIC_RESULT_POINTS / 8;
    let series = 4096u64;
    let fx = Fixture {
        series,
        pairs: 2,
        value_bytes: 8,
        steps: target / series,
        skew: Skew::Uniform,
    };
    let key = ck(
        Shape::Range,
        GroupShape::NoGrouping,
        ChainShape::Single,
        KShape::NotParameterised,
        Driver::Direct,
    );
    let (input, aggs, w) = run_chain(VectorAggOp::Sum, &key, &fx, 1);
    assert!(
        !w.overflow,
        "cohort table overflowed at scale — raise COHORT_SLOTS, never the probe bound"
    );
    let modelled = post_agg_peak_bytes(&input, &aggs);
    println!(
        "large-scale cell: {} series x {} points, peak = {}, modelled = {modelled}",
        input.series(),
        input.points(),
        w.peak
    );
    assert!(
        w.peak <= modelled,
        "large-scale peak {} exceeds the model {modelled}",
        w.peak
    );
}

// =====================================================================
// 11b. The funnel itself (ACs 17, 19, 20, 21)
// =====================================================================

/// AC 17 — an empty chain returns its input BIT-IDENTICALLY and allocates
/// nothing: the early return happens before measurement and before the
/// `Vec -> BTreeMap -> Vec` conversion, which is a strict win on the
/// commonest metric shape.
#[test]
fn an_empty_aggregation_chain_is_a_zero_allocation_passthrough() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let fx = Fixture {
        series: 64,
        ..CHAIN_BASE
    };
    for result in [
        QueryResult::Vector(build_vector(&fx, false)),
        QueryResult::Matrix(build_matrix(&fx, false)),
    ] {
        let before = result.clone();
        let (out, w) = measure(|| apply_vector_aggs(result, &[]));
        let out = out.expect("an empty chain is always admitted");
        assert!(!w.overflow, "overflow");
        assert_eq!(w.peak, 0, "an empty chain must allocate nothing: {w:?}");
        assert_eq!(w.count, 0, "an empty chain must not allocate at all: {w:?}");
        match (&before, &out) {
            (QueryResult::Vector(a), QueryResult::Vector(b)) => {
                assert_eq!(a.len(), b.len());
                for (x, y) in a.iter().zip(b) {
                    assert_eq!(x.labels, y.labels);
                    assert_eq!(x.value.to_bits(), y.value.to_bits());
                }
            }
            (QueryResult::Matrix(a), QueryResult::Matrix(b)) => {
                assert_eq!(a.len(), b.len());
                for (x, y) in a.iter().zip(b) {
                    assert_eq!(x.labels, y.labels);
                    let xb: Vec<(i64, u64)> =
                        x.points.iter().map(|(t, v)| (*t, v.to_bits())).collect();
                    let yb: Vec<(i64, u64)> =
                        y.points.iter().map(|(t, v)| (*t, v.to_bits())).collect();
                    assert_eq!(xb, yb, "an empty chain must not move a point");
                }
            }
            _ => panic!("the shape changed"),
        }
    }
}

/// AC 20 — **refusal before allocation.** With the cap set below the
/// modelled value the call returns `MetricPostAggBytes` AND the window's
/// peak is under one series' worth: the conversion never ran, no group
/// map was built and no join index exists.
#[test]
fn a_refused_charge_allocates_nothing() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let fx = Fixture {
        series: 1024,
        ..CHAIN_BASE
    };
    let aggs = vec![(VectorAggOp::Sum, None, None)];

    for shape in [Shape::Instant, Shape::Range] {
        let result = match shape {
            Shape::Instant => QueryResult::Vector(build_vector(&fx, false)),
            Shape::Range => QueryResult::Matrix(build_matrix(&fx, false)),
        };
        let input = measure_result(&result);
        let modelled = post_agg_peak_bytes(&input, &aggs);
        let mut charged = 0u64;
        let (out, w) =
            measure(|| apply_vector_aggs_capped(&mut charged, result, &aggs, modelled - 1));
        assert!(!w.overflow, "overflow");
        match out {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricPostAggBytes { bytes, cap })) => {
                assert_eq!(bytes, modelled);
                assert_eq!(cap, modelled - 1);
            }
            other => panic!(
                "{}: expected MetricPostAggBytes, got {other:?}",
                describe(shape)
            ),
        }
        assert!(
            w.peak < 4096,
            "{}: a refused charge allocated {} bytes — the conversion ran before the refusal",
            describe(shape),
            w.peak
        );
        assert_eq!(
            charged, 0,
            "a failed acquire must leave the counter unmutated"
        );
    }
}

/// AC 21 — **charge symmetry is structural.** The stage-local counter is
/// 0 after every return path: `Ok`, a refused `acquire`, and an `Err`
/// raised by an Entry function partway down the chain. Because the
/// discharge is `Drop`, a leak is not expressible — this test pins the
/// property, it does not create it.
#[test]
fn the_stage_counter_returns_to_zero_on_every_return_path() {
    let fx = Fixture {
        series: 32,
        ..CHAIN_BASE
    };
    let aggs = vec![(VectorAggOp::Sum, None, None)];

    // (1) Ok.
    let mut charged = 0u64;
    let ok = apply_vector_aggs_capped(
        &mut charged,
        QueryResult::Vector(build_vector(&fx, false)),
        &aggs,
        MAX_POST_AGG_BYTES,
    );
    assert!(ok.is_ok());
    assert_eq!(charged, 0, "the Ok path must discharge");

    // (2) a refused acquire.
    let mut charged = 0u64;
    let refused = apply_vector_aggs_capped(
        &mut charged,
        QueryResult::Vector(build_vector(&fx, false)),
        &aggs,
        1,
    );
    assert!(refused.is_err());
    assert_eq!(charged, 0, "a refused acquire must not mutate the counter");

    // (3) an Err raised inside the funnel: a duplicate one-side
    // signature. Since issue #290 this is decided by the class-(P)
    // preflight, under the PREFLIGHT's charge and before
    // `Ledger::acquire_binary` exists — which is the point of that issue,
    // and which makes this case exercise the new `Drop`-free early
    // return rather than the old mid-chain one. The property it pins is
    // unchanged: an early `?` anywhere in the funnel still leaves the
    // caller's counter at zero. Case (4) below keeps a witness for the
    // "an error AFTER the stage charge is in force" half.
    let dup = vec![
        VectorSample {
            labels: vec![("id00".to_string(), "a".to_string())],
            value: 1.0,
        },
        VectorSample {
            labels: vec![("id00".to_string(), "a".to_string())],
            value: 2.0,
        },
    ];
    let one = vec![VectorSample {
        labels: vec![("id00".to_string(), "a".to_string())],
        value: 3.0,
    }];
    let mut charged = 0u64;
    let err = combine_binary_capped(
        &mut charged,
        BinOp::Div,
        false,
        Some(&VectorMatching {
            on: true,
            labels: vec!["id00".to_string()],
            group: Some(MatchGroup::Left(Vec::new())),
        }),
        QueryResult::Vector(one),
        QueryResult::Vector(dup),
        MAX_POST_AGG_BYTES,
    );
    assert!(err.is_err(), "a duplicate one-side signature must error");
    assert_eq!(
        charged, 0,
        "an early `?` inside the chain must still discharge"
    );

    // (4) an Err raised AFTER the stage charge is in force: the
    // post-charge `admit` shortfall of
    // `admit_refuses_a_collection_wider_than_its_charge` — charge for a
    // one-series shape, hand the stage a 64-series operand.
    let small = StageInput::for_derivation(1, 1, 4, 32, 1, 4, 1, 1);
    let bytes = post_agg_peak_bytes(&small, &aggs);
    let wide = Fixture {
        series: 64,
        ..CHAIN_BASE
    };
    let mut charged = 0u64;
    let admitted = apply_vector_aggs_capped(
        &mut charged,
        QueryResult::Vector(build_vector(&wide, false)),
        &aggs,
        bytes,
    );
    assert!(
        matches!(
            admitted,
            Err(ReadError::QueryTooBroad(
                TooBroadReason::MetricPostAggBytes { .. }
            ))
        ),
        "the admission shortfall must refuse cleanly, got {admitted:?}"
    );
    assert_eq!(
        charged, 0,
        "an `admit` refusal after the stage charge must still discharge"
    );
}

/// AC 19 — **`admit` is unconditional.** Driving an Entry function with a
/// collection larger than the charge covers yields a clean
/// `MetricPostAggBytes`, with no `debug_assert` anywhere in the path: the
/// same outcome in release as in debug. Reached here through the public
/// seam by charging for a SMALL operand and then handing the stage a
/// large one — which is exactly the shape a future uncharged call site
/// would take.
#[test]
fn admit_refuses_a_collection_wider_than_its_charge() {
    let small = StageInput::for_derivation(1, 1, 4, 32, 1, 4, 1, 1);
    let aggs = vec![(VectorAggOp::Sum, None, None)];
    let bytes = post_agg_peak_bytes(&small, &aggs);
    let wide = Fixture {
        series: 64,
        ..CHAIN_BASE
    };
    // The seam charges for what it measures, so an under-measured charge
    // is only reachable by handing `_capped` a cap big enough for the
    // small shape and an operand of the large one — `admit` is what
    // stops it, not the acquire.
    let mut charged = 0u64;
    let out = apply_vector_aggs_capped(
        &mut charged,
        QueryResult::Vector(build_vector(&wide, false)),
        &aggs,
        bytes,
    );
    match out {
        Err(ReadError::QueryTooBroad(TooBroadReason::MetricPostAggBytes { .. })) => {}
        other => panic!("expected MetricPostAggBytes, got {other:?}"),
    }
    assert_eq!(charged, 0);
}

/// The source-level half of AC 19: no `debug_assert` anywhere in the
/// enforcement path. A `debug_assert` is not an enforcement mechanism, it
/// is a comment that runs in CI — and release builds are what serve
/// queries.
#[test]
fn the_enforcement_path_contains_no_debug_assert() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/logql/post_agg.rs"),
    )
    .expect("read exec.rs");
    let start = src
        .find("mod ledger {")
        .expect("the ledger module must exist");
    let end = src[start..]
        .find("\nuse ledger::Ledger;")
        .expect("the ledger module's end")
        + start;
    // CODE only: `admit`'s own doc says "no `debug_assert`", and a census
    // that counted its own prose would fail on the sentence describing
    // the property it checks. The unit is a non-comment source LINE.
    let module: String = src[start..end]
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !module.contains("debug_assert"),
        "`mod ledger` contains a debug_assert — the charge and the admission must hold in \
         release exactly as in debug"
    );
    // Issue #290 adds a SECOND proof token to this module, so the pin
    // becomes a COUNT: `.contains` would have stayed green with
    // `Ledger::admit` mutated to `-> bool`, satisfied by
    // `PreflightCharge::admit`'s identical line. The count also asserts
    // the funnel's shape out loud — EVERY proof token admits, with that
    // `Result`-returning signature — so a token that does not admit
    // reddens here and needs a design decision, not a number edit.
    let admits = module
        .matches("fn admit(&self, series: u64, points: u64) -> Result<(), ReadError>")
        .count();
    assert_eq!(
        admits,
        PROOF_TOKENS.len(),
        "`mod ledger` declares {} proof tokens ({PROOF_TOKENS:?}) but {admits} `admit` \
         signatures returning a Result — every proof token must admit, and must do so by \
         returning rather than asserting",
        PROOF_TOKENS.len()
    );
    // The region is carved out by a `str::find` on the two `use` lines
    // below `mod ledger`, so a refactor that moved `PreflightCharge` out
    // of the module would silently stop the no-`debug_assert` ban
    // covering it. Since issue #290 deleted the second detector (v9's
    // subtraction), THIS ASSERT IS NOW THE ONLY ONE that catches that
    // move — do not remove it on the assumption something else does.
    assert!(
        module.contains("struct PreflightCharge"),
        "`PreflightCharge` is no longer declared inside `mod ledger`, so the enforcement-path \
         scan has silently stopped covering it"
    );
}

// =====================================================================
// 12. §4.1's call-graph census — the token-taking set is DERIVED
// =====================================================================
//
// The set of functions that must carry the funnel's proof token is
// COMPUTED from the call graph, not written down: a hand list omitted
// `select_k_range`/`select_k_instant` once already (round 12's `[high]`)
// and `apply_vector_aggs`/`combine_binary` themselves once after that
// (round 13's `[medium]`). This module recomputes the closure and the
// classification on every run; the published table is the expected
// ANSWER, and a mismatch fails here rather than at review.

mod region_census {
    use std::collections::{BTreeMap, BTreeSet};
    use syn::visit::Visit;

    /// One function definition in the region's source, with the callee
    /// names its body mentions.
    #[derive(Debug, Clone)]
    pub struct FnInfo {
        pub file: String,
        /// `foo` for a free fn, `Type::foo` for an inherent method.
        pub display: String,
        /// `None` for a free fn.
        pub owner: Option<String>,
        pub name: String,
        /// Every parameter's type, as the set of path idents it mentions,
        /// plus `("String","String")` tuples flattened to `StringPair`.
        pub param_types: BTreeSet<String>,
        /// Bare call names, `Type::name` paths, `.method` names,
        /// `name!` macros and `Type::Variant { .. }` struct expressions.
        pub callees: BTreeSet<String>,
        /// The `::`-joined chain of enclosing INLINE modules, `None` at
        /// file scope. Issue #290 needs it to scope a rule to
        /// `mod preflight` and its nested `mod points`, which
        /// `free_fns`/`closure` cannot express because they range over
        /// free functions by bare name.
        pub module: Option<String>,
    }

    #[derive(Default)]
    struct Body {
        callees: BTreeSet<String>,
    }

    impl Visit<'_> for Body {
        fn visit_expr_call(&mut self, node: &syn::ExprCall) {
            if let syn::Expr::Path(p) = &*node.func {
                let segs: Vec<String> = p
                    .path
                    .segments
                    .iter()
                    .map(|s| s.ident.to_string())
                    .collect();
                if let Some(last) = segs.last() {
                    self.callees.insert(last.clone());
                }
                if segs.len() >= 2 {
                    self.callees.insert(segs[segs.len() - 2..].join("::"));
                }
            }
            syn::visit::visit_expr_call(self, node);
        }
        fn visit_expr_method_call(&mut self, node: &syn::ExprMethodCall) {
            self.callees.insert(format!(".{}", node.method));
            syn::visit::visit_expr_method_call(self, node);
        }
        /// `ReadError::PipelineInvalid { .. }` is a STRUCT expression, not
        /// a call, so `visit_expr_call` cannot see it — and the semantic
        /// refusals of this region are all struct variants. Only the
        /// two-segment `Type::Variant` form is recorded: a bare
        /// `StageInput { .. }` would put a type name into a set the
        /// call-graph closure resolves against.
        fn visit_expr_struct(&mut self, node: &syn::ExprStruct) {
            let segs: Vec<String> = node
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();
            if segs.len() >= 2 {
                self.callees.insert(segs[segs.len() - 2..].join("::"));
            }
            syn::visit::visit_expr_struct(self, node);
        }
        fn visit_macro(&mut self, node: &syn::Macro) {
            if let Some(seg) = node.path.segments.last() {
                self.callees.insert(format!("{}!", seg.ident));
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

    fn type_idents(ty: &syn::Type, out: &mut BTreeSet<String>) {
        match ty {
            syn::Type::Path(p) => {
                for seg in &p.path.segments {
                    out.insert(seg.ident.to_string());
                    if let syn::PathArguments::AngleBracketed(a) = &seg.arguments {
                        for arg in &a.args {
                            match arg {
                                syn::GenericArgument::Type(t) => type_idents(t, out),
                                syn::GenericArgument::AssocType(t) => type_idents(&t.ty, out),
                                _ => {}
                            }
                        }
                    }
                }
            }
            syn::Type::Reference(r) => type_idents(&r.elem, out),
            syn::Type::Slice(s) => type_idents(&s.elem, out),
            syn::Type::Array(a) => type_idents(&a.elem, out),
            syn::Type::Paren(p) => type_idents(&p.elem, out),
            syn::Type::Group(g) => type_idents(&g.elem, out),
            syn::Type::Ptr(p) => type_idents(&p.elem, out),
            syn::Type::Tuple(t) => {
                let mut inner = BTreeSet::new();
                for e in &t.elems {
                    type_idents(e, &mut inner);
                }
                if t.elems.len() == 2 && inner.len() == 1 && inner.contains("String") {
                    out.insert("StringPair".to_string());
                }
                out.extend(inner);
            }
            syn::Type::ImplTrait(i) => {
                for b in &i.bounds {
                    if let syn::TypeParamBound::Trait(t) = b {
                        for seg in &t.path.segments {
                            out.insert(seg.ident.to_string());
                            if let syn::PathArguments::Parenthesized(p) = &seg.arguments {
                                for a in &p.inputs {
                                    type_idents(a, out);
                                }
                                if let syn::ReturnType::Type(_, r) = &p.output {
                                    type_idents(r, out);
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn sig_types(sig: &syn::Signature) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for arg in &sig.inputs {
            if let syn::FnArg::Typed(t) = arg {
                type_idents(&t.ty, &mut out);
            }
        }
        // A generic parameter's BOUNDS carry the stage-data types on a
        // generic helper (`F: Fn(usize) -> &LabelSet`), so they count.
        for p in &sig.generics.params {
            if let syn::GenericParam::Type(t) = p {
                for b in &t.bounds {
                    if let syn::TypeParamBound::Trait(tr) = b {
                        for seg in &tr.path.segments {
                            if let syn::PathArguments::Parenthesized(pa) = &seg.arguments {
                                for a in &pa.inputs {
                                    type_idents(a, &mut out);
                                }
                                if let syn::ReturnType::Type(_, r) = &pa.output {
                                    type_idents(r, &mut out);
                                }
                            }
                        }
                    }
                }
            }
        }
        if let Some(w) = &sig.generics.where_clause {
            for pred in &w.predicates {
                if let syn::WherePredicate::Type(t) = pred {
                    for b in &t.bounds {
                        if let syn::TypeParamBound::Trait(tr) = b {
                            for seg in &tr.path.segments {
                                if let syn::PathArguments::Parenthesized(pa) = &seg.arguments {
                                    for a in &pa.inputs {
                                        type_idents(a, &mut out);
                                    }
                                    if let syn::ReturnType::Type(_, r) = &pa.output {
                                        type_idents(r, &mut out);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        out
    }

    fn is_cfg_test(attrs: &[syn::Attribute]) -> bool {
        attrs.iter().any(|a| {
            let s = format!(
                "{:?}",
                a.meta.path().segments.last().map(|s| s.ident.to_string())
            );
            s.contains("cfg")
                && a.to_owned()
                    .parse_args::<syn::Meta>()
                    .is_ok_and(|m| m.path().is_ident("test"))
        })
    }

    fn walk_items(file: &str, module: Option<&str>, items: &[syn::Item], out: &mut Vec<FnInfo>) {
        for item in items {
            match item {
                syn::Item::Fn(f) => {
                    let mut b = Body::default();
                    b.visit_block(&f.block);
                    out.push(FnInfo {
                        file: file.to_string(),
                        display: f.sig.ident.to_string(),
                        owner: None,
                        name: f.sig.ident.to_string(),
                        param_types: sig_types(&f.sig),
                        callees: b.callees,
                        module: module.map(str::to_string),
                    });
                }
                syn::Item::Impl(i) => {
                    let owner = match &*i.self_ty {
                        syn::Type::Path(p) => p
                            .path
                            .segments
                            .last()
                            .map(|s| s.ident.to_string())
                            .unwrap_or_default(),
                        _ => "<impl>".to_string(),
                    };
                    for it in &i.items {
                        if let syn::ImplItem::Fn(f) = it {
                            let mut b = Body::default();
                            b.visit_block(&f.block);
                            out.push(FnInfo {
                                file: file.to_string(),
                                display: format!("{owner}::{}", f.sig.ident),
                                owner: Some(owner.clone()),
                                name: f.sig.ident.to_string(),
                                param_types: sig_types(&f.sig),
                                callees: b.callees,
                                module: module.map(str::to_string),
                            });
                        }
                    }
                }
                syn::Item::Mod(m) => {
                    if is_cfg_test(&m.attrs) {
                        continue; // PRODUCTION source only.
                    }
                    if let Some((_, inner)) = &m.content {
                        let name = m.ident.to_string();
                        let nested = match module {
                            Some(outer) => format!("{outer}::{name}"),
                            None => name,
                        };
                        walk_items(file, Some(&nested), inner, out);
                    }
                }
                _ => {}
            }
        }
    }

    /// Parses every production function in `src/logql`.
    ///
    /// **Scope, stated because an unscoped conclusion from a scoped
    /// census is worthless:** every `.rs` file **directly in**
    /// `crates/pulsus-read/src/logql/` — the flat level only, with
    /// `#[cfg(test)]` modules excluded. The walk is NON-RECURSIVE, so
    /// subdirectory modules such as `template/` are not scanned; making
    /// it recursive is #302. The directory is read at RUN TIME rather
    /// than a file list being written out, which is why issue #299's
    /// `exec.rs` split did not narrow this census — the region simply
    /// moved to other files the walk already covers. The unit is a
    /// function ITEM.
    pub fn collect() -> (Vec<String>, Vec<FnInfo>) {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/logql");
        let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
            .expect("the logql source directory")
            .map(|e| e.expect("dir entry").path())
            .filter(|p| p.extension().is_some_and(|x| x == "rs"))
            .collect();
        files.sort();
        assert!(
            files.len() >= 10,
            "only {} source files found in {dir:?} — the census is looking in the wrong place",
            files.len()
        );
        let mut out = Vec::new();
        let mut names = Vec::new();
        for path in &files {
            let name = path
                .file_name()
                .expect("file name")
                .to_string_lossy()
                .into_owned();
            let text = std::fs::read_to_string(path).expect("read source");
            let parsed = syn::parse_file(&text)
                .unwrap_or_else(|e| panic!("{name} does not parse as Rust: {e}"));
            walk_items(&name, None, &parsed.items, &mut out);
            names.push(name);
        }
        (names, out)
    }

    /// The names `mod.rs` re-exports from `post_agg` — the region's
    /// public boundary, read off the `pub use post_agg::{…}` source so
    /// the census cannot drift from it (fix round 1's `[low]`: a
    /// hand-maintained list of what exists cannot detect a new export
    /// nobody added to it).
    pub fn post_agg_exports() -> BTreeSet<String> {
        fn walk(tree: &syn::UseTree, in_post_agg: bool, out: &mut BTreeSet<String>) {
            match tree {
                syn::UseTree::Path(p) => {
                    walk(&p.tree, in_post_agg || p.ident == "post_agg", out);
                }
                syn::UseTree::Group(g) => {
                    for t in &g.items {
                        walk(t, in_post_agg, out);
                    }
                }
                // Outside a `post_agg::` prefix the name is another
                // module's export and out of this census's scope.
                syn::UseTree::Name(n) => {
                    if in_post_agg {
                        out.insert(n.ident.to_string());
                    }
                }
                // A rename would hide the DEFINED name the call-graph
                // resolves, so record the source ident, not the alias.
                syn::UseTree::Rename(r) => {
                    if in_post_agg {
                        out.insert(r.ident.to_string());
                    }
                }
                // A glob cannot be enumerated from the use tree alone —
                // that takes name resolution this census does not have —
                // and silently skipping it is exactly how an export
                // would escape (fix round 2's `[low]`: the former
                // `_ => {}` under-reported `pub use post_agg::*;`). A
                // parser must reject what it does not understand,
                // LOUDLY. The match is EXHAUSTIVE on purpose: a new
                // `syn` use-tree form stops this compiling instead of
                // falling through.
                syn::UseTree::Glob(_) => panic!(
                    "src/logql/mod.rs carries a glob re-export{} — the export census cannot \
                     enumerate a glob; write the names out",
                    if in_post_agg { " of post_agg::*" } else { "" }
                ),
            }
        }
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/logql/mod.rs");
        let text = std::fs::read_to_string(&path).expect("read src/logql/mod.rs");
        let parsed = syn::parse_file(&text).expect("mod.rs parses as Rust");
        let mut out = BTreeSet::new();
        for item in &parsed.items {
            if let syn::Item::Use(u) = item
                && matches!(u.vis, syn::Visibility::Public(_))
            {
                walk(&u.tree, false, &mut out);
            }
        }
        assert!(
            out.contains("apply_vector_aggs"),
            "no `pub use post_agg::{{…}}` block found in mod.rs — the boundary derivation is \
             reading the wrong file: {out:?}"
        );
        out
    }

    /// Every type owning an `acquire*` constructor in ANY file the
    /// census parses — not just `post_agg.rs` (issue #290).
    ///
    /// A FREE `acquire*` function has no owner to derive, and skipping it
    /// silently would be an escape: it panics instead, naming the
    /// function, because a derivation that cannot classify an input must
    /// say so rather than return a smaller set. `Some("<impl>")` —
    /// `walk_items`' fallback for a non-path `self_ty` — is not a second
    /// escape either: it inserts the literal `<impl>`, which equals no
    /// `PROOF_TOKENS` entry, so the equality reddens naming it.
    ///
    /// **What this closes and what it does not.** It closes "a proof
    /// token exists whose type the census's predicates do not know
    /// about", for a token minted by an `acquire*` constructor in any
    /// file `collect()` parses. It does NOT close a token minted by a
    /// constructor named something else, nor one declared in a
    /// subdirectory module the non-recursive walk never reads (#302);
    /// both are accepted open, and both are empty at this commit —
    /// `git grep -n "fn acquire" -- crates/pulsus-read/src/logql/`
    /// returns four hits, all inherent methods of `Ledger` or
    /// `PreflightCharge` in `post_agg.rs`.
    pub fn token_owners(all: &[FnInfo]) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for f in all.iter().filter(|f| f.name.starts_with("acquire")) {
            match &f.owner {
                Some(owner) => {
                    out.insert(owner.clone());
                }
                None => panic!(
                    "`{}` in {} is a FREE `acquire*` constructor: `token_owners` derives proof \
                     tokens from inherent-impl owners and cannot classify it. Give it an owner \
                     or extend this derivation — do not delete the assert.",
                    f.display, f.file
                ),
            }
        }
        out
    }

    /// Free functions by name. A name defined twice is a loud failure:
    /// the closure would otherwise resolve to whichever came first.
    pub fn free_fns(all: &[FnInfo]) -> BTreeMap<String, FnInfo> {
        let mut map: BTreeMap<String, Vec<&FnInfo>> = BTreeMap::new();
        for f in all.iter().filter(|f| f.owner.is_none()) {
            map.entry(f.name.clone()).or_default().push(f);
        }
        let mut out = BTreeMap::new();
        for (name, defs) in map {
            assert!(
                defs.len() == 1,
                "`{name}` is defined {} times across src/logql ({:?}) — the call-graph closure \
                 cannot resolve it, so the census would silently follow the wrong body",
                defs.len(),
                defs.iter().map(|d| &d.file).collect::<Vec<_>>()
            );
            out.insert(name, defs[0].clone());
        }
        out
    }
}

/// **Every proof-token type in `mod ledger`** (issue #290).
///
/// Each of the census's token-keyed predicates is generated from this
/// list — the minting roots, the `public_roots` "must MINT, never
/// receive" assert, `admit_helpers`, `Member.mints`,
/// `Member.takes_ledger` and `Member.calls_admit` — so adding a THIRD
/// token is one edit here rather than six edits nobody will find.
/// `the_enforcement_path_contains_no_debug_assert`'s `fn admit` pin is
/// keyed on its LENGTH, which asserts the funnel's shape out loud: every
/// proof token admits, with that identical `Result`-returning signature.
///
/// **NOT keyed by it:** that test's raw `"mod ledger {"` /
/// `"\nuse ledger::Ledger;"` delimiters. Those are strings about a
/// module's TEXT, not about a token type; token-keying them would make
/// the region end depend on import ordering for no gain, and the
/// `struct PreflightCharge` containment assert beside them is what turns
/// a silent scope loss into a loud failure instead.
///
/// The list itself is DERIVED-checked by `region_census::token_owners`,
/// so a token nobody added to it fails by name rather than going
/// invisible.
const PROOF_TOKENS: &[&str] = &["Ledger", "PreflightCharge"];

fn takes_a_proof_token(f: &region_census::FnInfo) -> bool {
    PROOF_TOKENS.iter().any(|t| f.param_types.contains(*t))
}

fn mints_a_proof_token(f: &region_census::FnInfo) -> bool {
    f.callees.iter().any(|c| {
        PROOF_TOKENS
            .iter()
            .any(|t| c.starts_with(&format!("{t}::acquire")))
    })
}

fn admits_directly(f: &region_census::FnInfo) -> bool {
    f.callees
        .iter()
        .any(|c| c == ".admit" || PROOF_TOKENS.iter().any(|t| *c == format!("{t}::admit")))
}

/// A body ALLOCATES if it mentions any of these. Deliberately
/// over-inclusive: a false positive only widens the set that must carry
/// the token, which is the safe direction.
const ALLOC_TOKENS: &[&str] = &[
    ".collect",
    ".to_vec",
    ".clone",
    ".push",
    ".insert",
    ".entry",
    ".extend",
    ".to_string",
    ".to_owned",
    ".sort",
    ".sort_by",
    ".sort_by_key",
    ".sort_unstable",
    ".sort_unstable_by",
    ".sort_unstable_by_key",
    ".with_capacity",
    "vec!",
    "format!",
    "Vec::new",
    "Vec::with_capacity",
    "HashMap::new",
    "HashMap::with_capacity",
    "HashSet::new",
    "BTreeMap::new",
    "BTreeSet::new",
    "String::new",
];

/// A parameter carries STAGE DATA if its type mentions one of these.
/// `QueryResult` is in the list (round 13's `[medium]`): `map_samples`
/// takes one and is an Entry, and without it the census could not derive
/// the set it publishes.
const STAGE_DATA_TYPES: &[&str] = &[
    "QueryResult",
    // Issue #290's borrowed operand view. A view type must not be able
    // to hide a helper from the census, so it joins the class rather
    // than sitting outside it.
    "SideSeries",
    "RangeSeries",
    "InstantSeries",
    "MatrixSeries",
    "VectorSample",
    "JoinItem",
    "LabelSet",
    "MatchSig",
    "StringPair",
];

/// **The `Shared` class — enumerated, not predicated** (task-manager
/// ruling on issue #236).
///
/// These two are Element-class members of the funnel's closure AND are
/// called by the Part B fold at the client leaf (`ReduceFold::push_series`,
/// `SelectFold::push_series`, `RangeSlideState::emit`), which charges
/// through `charge_group_bytes`/`charge_result_points` and holds no
/// funnel token. Requiring `&Ledger` on them would not compile there;
/// requiring it only sometimes would make the token optional, which is no
/// token at all.
///
/// **Why these two are safe to share, restated to what is TRUE** (review
/// round 1's `[high]`: the first version of this reason said `group_key`
/// returns a `LabelSet` "the caller owns and charges for", and the FOLD
/// caller did not charge for it — a property true of the funnel path and
/// false of the fold path, which is the path the class exists for).
///
/// * `pin_reduction_order` sorts a slice **in place** and allocates
///   nothing beyond the sort's own scratch. It is safe under any
///   charging regime because it adds no retained bytes to either.
/// * `group_key` **does** allocate — one owned `String` per `by` name,
///   read off the query text. It is safe to share because **every** route
///   to it charges first, in its own regime: the funnel's callers are
///   covered by `Ledger::acquire`'s `W_GROUPNAME · series ·
///   group_name_bytes` term, and the fold reaches it only through
///   `charged_group_key`, which charges `group_key_bytes(...) +
///   map_entry_bytes(FOLD_GROUP_SLOT)` against the leaf's
///   `MAX_CLIENT_AGG_GROUP_BYTES` **before** building the key
///   (`exec.rs`, pinned by `the_fold_charges_before_it_builds_a_group_key`
///   and `the_fold_charges_a_group_key_before_group_key_allocates_it`).
///
/// So the property a third candidate must demonstrate is not "allocates
/// nothing" — it is **"every calling regime charges for what it
/// allocates, before it allocates it"**, named per regime.
///
/// The list is a NAME LIST on purpose. A membership rule would let a
/// future function step out of the funnel simply by acquiring a second
/// caller; with an enumeration a third member fails the census loudly and
/// by name, and joins only by adjudication.
const SHARED_WITH_THE_LEAF: &[&str] = &["group_key", "pin_reduction_order"];

/// A member reached from the funnel roots, with everything the four
/// mechanical predicates need.
#[derive(Debug)]
struct Member {
    name: String,
    file: String,
    allocates: bool,
    stage_data: bool,
    /// Reaches `Ledger::acquire` — mints the charge.
    mints: bool,
    /// Carries the proof token.
    takes_ledger: bool,
    /// Calls `Ledger::admit` on its own input.
    calls_admit: bool,
    /// Callers OUTSIDE the closure — a member with one cannot be
    /// required to hold the funnel's token, because a legitimate caller
    /// has no funnel charge in force.
    external_callers: Vec<String>,
}

fn census_members() -> (Vec<String>, Vec<Member>) {
    let (files, all) = region_census::collect();
    let free = region_census::free_fns(&all);

    // **The roots are DERIVED, not listed**: every free function that
    // mints a charge. A hand list would silently miss a new one, which is
    // exactly how the SQL path's two inline chains would have escaped the
    // census when they started charging.
    let mut roots: Vec<String> = free
        .values()
        .filter(|f| mints_a_proof_token(f))
        .map(|f| f.name.clone())
        .collect();
    // Plus the region's PUBLIC boundary, DERIVED from `mod.rs`'s
    // `pub use post_agg::{…}` block (fix round 1's `[low]`: the former
    // hand list could not detect a new export nobody added to it). The
    // uncapped wrappers mint INDIRECTLY (through their `_capped` seams),
    // so the direct filter above cannot see them; here every exported
    // free fn that transitively reaches a minter joins the roots.
    // Coverage stated exactly: an export that neither mints nor
    // delegates to one — today the measure/peak-bytes model helpers,
    // which authorise no charge and are themselves callees of the
    // minting seams — is walked only if the closure reaches it.
    let reaches_a_minter: std::collections::BTreeSet<String> = {
        let mut set: std::collections::BTreeSet<String> = roots.iter().cloned().collect();
        loop {
            let before = set.len();
            for f in free.values() {
                if !set.contains(&f.name)
                    && f.callees
                        .iter()
                        .any(|c| set.contains(c.rsplit("::").next().unwrap_or(c)))
                {
                    set.insert(f.name.clone());
                }
            }
            if set.len() == before {
                break;
            }
        }
        set
    };
    let public_roots: Vec<String> = region_census::post_agg_exports()
        .into_iter()
        .filter(|n| free.contains_key(n) && reaches_a_minter.contains(n))
        .collect();
    assert!(
        public_roots.iter().any(|n| n == "apply_label_replace"),
        "the derived public boundary lost a known transform — the mod.rs parse or the \
         reaches-a-minter closure is wrong: {public_roots:?}"
    );
    for public in public_roots {
        let f = &free[&public];
        assert!(
            !takes_a_proof_token(f),
            "`{public}` is a public entry point of the region and must MINT the charge, never \
             receive one"
        );
        roots.push(public);
    }
    assert!(
        roots.len() >= 3,
        "only {} charge-minting roots found — the token is not wired where the census looks: \
         {roots:?}",
        roots.len()
    );

    // Transitive callee closure of the roots, to a fixpoint.
    let mut closure: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut frontier: Vec<String> = roots.clone();
    while let Some(name) = frontier.pop() {
        if !closure.insert(name.clone()) {
            continue;
        }
        let Some(info) = free.get(&name) else {
            continue;
        };
        for callee in &info.callees {
            let bare = callee.rsplit("::").next().unwrap_or(callee);
            if free.contains_key(bare) && !closure.contains(bare) {
                frontier.push(bare.to_string());
            }
        }
    }

    // The direct admission helpers, DERIVED: a free fn that takes A
    // PROOF TOKEN (issue #290: either of them) and whose whole job is to
    // call that token's `admit` (`admit_range`
    // computes `points` as one `O(series)` sum, `admit_instant`/
    // `admit_join` are `O(1)`). An Entry reaches `admit` through one of
    // these, and recognising them is what keeps Entry from reading as
    // Element.
    let admit_helpers: std::collections::BTreeSet<String> = free
        .values()
        .filter(|f| takes_a_proof_token(f) && admits_directly(f))
        .map(|f| f.name.clone())
        .collect();

    let members = closure
        .iter()
        .filter_map(|n| free.get(n))
        .map(|info| {
            let external_callers = all
                .iter()
                .filter(|f| !closure.contains(&f.name) || f.owner.is_some())
                .filter(|f| f.display != info.display)
                .filter(|f| {
                    f.callees
                        .iter()
                        .any(|c| c.rsplit("::").next() == Some(info.name.as_str()))
                })
                .map(|f| format!("{}::{}", f.file, f.display))
                .collect();
            Member {
                name: info.name.clone(),
                file: info.file.clone(),
                mints: mints_a_proof_token(info),
                takes_ledger: takes_a_proof_token(info),
                calls_admit: admits_directly(info)
                    || info.callees.iter().any(|c| admit_helpers.contains(c)),
                allocates: ALLOC_TOKENS.iter().any(|t| info.callees.contains(*t)),
                stage_data: STAGE_DATA_TYPES
                    .iter()
                    .any(|t| info.param_types.contains(*t)),
                external_callers,
            }
        })
        .collect();
    (files, members)
}

/// The five classes §4.1 admits, `Shared` being the fifth (task-manager
/// ruling): a member that matches none of them fails the census.
#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
enum RegionClass {
    /// Mints the charge: reaches some proof token's `acquire*`
    /// constructor and takes no proof token itself.
    Root,
    /// Receives a fresh stage-level collection; admits it unconditionally.
    Entry,
    /// Operates on ONE element of an already-admitted collection.
    Element,
    /// Element-class, but also reached from the client leaf's own
    /// charging regime, so it cannot be required to hold the token.
    Shared,
}

fn classify(m: &Member) -> Option<RegionClass> {
    if SHARED_WITH_THE_LEAF.contains(&m.name.as_str()) {
        return Some(RegionClass::Shared);
    }
    match (m.mints, m.takes_ledger, m.calls_admit) {
        (true, false, _) => Some(RegionClass::Root),
        (false, true, true) => Some(RegionClass::Entry),
        (false, true, false) => Some(RegionClass::Element),
        _ => None,
    }
}

/// **The census is the gate; the published table is the expected
/// answer.** Every member that both allocates and takes a stage-data
/// parameter must fall into exactly one of `Root`/`Entry`/`Element`/
/// `Shared` by the mechanical predicates, and the classification the
/// census computes must equal the one published here.
///
/// **§7's "which call sites" claim, restated to what it now covers**
/// (task-manager condition 3 — the sentence changes rather than being
/// softened in a footnote):
///
/// > Every allocating function in the census-derived set requires a
/// > `&Ledger`, whose only constructor charges, so an uncapped call site
/// > does not compile — **except for the two functions named in
/// > `SHARED_WITH_THE_LEAF`, which are also reached from the client
/// > leaf's own charging regime. For those two the coverage comes from
/// > whichever caller's charge is in force, and they are safe to share
/// > because each is a pure function of its arguments that allocates
/// > nothing into the funnel's accounting.** A third such function fails
/// > this census by name and joins only by adjudication.
#[test]
fn the_token_taking_set_is_derived_from_the_call_graph() {
    let (files, members) = census_members();
    assert!(
        files.contains(&"exec.rs".to_string()),
        "the census did not read exec.rs; files = {files:?}"
    );
    assert!(
        members.len() >= 20,
        "the closure collapsed to {} members — the roots or the resolver are wrong",
        members.len()
    );

    // The obligated set: allocates AND receives stage data.
    let obligated: Vec<&Member> = members
        .iter()
        .filter(|m| m.allocates && m.stage_data)
        .collect();

    // The obligated set is `allocates AND stage_data`. `charged_*_chain`
    // and `run_*_chain` are in the closure but allocate NOTHING
    // themselves — they measure, charge and delegate — so the census
    // does not obligate them, and publishing them here would be claiming
    // an obligation the predicates do not derive.
    //
    // Issue #290: `combine_binary_capped` LEFT this table. It is still a
    // Root — it still mints through `Ledger::acquire_binary` — but it no
    // longer allocates: its one allocating expression was the inline
    // `ReadError::PipelineInvalid { reason: "...".to_string() }` of the
    // incompatible-types arm, and that arm now calls
    // `incompatible_types_error()`, the single constructor the class-(P)
    // preflight shares with it so the two cannot drift. The obligated
    // set is `allocates AND stage_data`, so publishing it here would
    // claim an obligation the predicates no longer derive.
    const EXPECT_ROOT: &[&str] = &["apply_vector_aggs_capped"];
    const EXPECT_ENTRY: &[&str] = &[
        "approx_topk_instant",
        "combine_matrices",
        "combine_vectors",
        // Issue #290's class-(P) preflight: reserves its six scratch
        // buffers and admits the operand pair it was charged for through
        // `admit_operands` before any of them is filled.
        "decide_binary_refusals",
        "group_instant",
        "group_range",
        "instant_join",
        "map_samples",
        // Issue #276: the label_replace collision merge — admits the
        // relabeled operand (whose envelope is the measured input's)
        // before its rebuilt containers allocate.
        "merge_matrix_collisions",
        "select_k_instant",
        "select_k_range",
        "set_op_join",
        "sort_instant",
    ];
    const EXPECT_ELEMENT: &[&str] = &[
        // Issue #290's three preflight stage helpers. Each fills a
        // buffer the Entry reserved at a count it charged for.
        "collision_groups",
        "earliest_offending_step",
        "match_signature",
        "project_side",
        // Issue #276: one series' in-place `label_replace` rewrite; its
        // expansion/insert allocations are priced per series by
        // `label_replace_peak_bytes`.
        "relabel",
        "set_label_sorted",
        "sort_candidates",
    ];

    let mut got: Vec<&str> = obligated.iter().map(|m| m.name.as_str()).collect();
    got.sort_unstable();
    let mut want: Vec<&str> = EXPECT_ROOT
        .iter()
        .chain(EXPECT_ENTRY)
        .chain(EXPECT_ELEMENT)
        .chain(SHARED_WITH_THE_LEAF)
        .copied()
        .collect();
    want.sort_unstable();
    assert_eq!(
        got, want,
        "the census-derived obligation set differs from the published table; every difference \
         is either a function that must be dispositioned or a table that has gone stale"
    );

    // Every obligated member falls into EXACTLY one class, and into the
    // one published for it.
    for m in &obligated {
        let class = classify(m).unwrap_or_else(|| {
            panic!(
                "{} matches none of the five classes (mints = {}, takes_ledger = {}, \
                 calls_admit = {}) — it must be dispositioned, not left to allocate uncapped",
                m.name, m.mints, m.takes_ledger, m.calls_admit
            )
        });
        let published = if EXPECT_ROOT.contains(&m.name.as_str()) {
            RegionClass::Root
        } else if EXPECT_ENTRY.contains(&m.name.as_str()) {
            RegionClass::Entry
        } else if EXPECT_ELEMENT.contains(&m.name.as_str()) {
            RegionClass::Element
        } else {
            RegionClass::Shared
        };
        assert_eq!(
            class, published,
            "{}: the census computes {class:?} where the table publishes {published:?}",
            m.name
        );
    }

    for m in &members {
        if obligated.iter().any(|o| o.name == m.name) {
            continue;
        }
        assert!(
            !(m.allocates && m.stage_data),
            "{} is obligated but absent from the published table",
            m.name
        );
    }
}

/// Condition 2 of the `Shared` ruling: a THIRD member fails loudly and by
/// name rather than joining silently. `SHARED_WITH_THE_LEAF` is consulted
/// only for names that are genuinely shared, so a stale entry is a
/// failure too.
#[test]
fn the_shared_class_admits_exactly_its_two_named_members() {
    let (_, members) = census_members();
    let genuinely_shared: Vec<&str> = members
        .iter()
        .filter(|m| m.allocates && m.stage_data && !m.mints)
        .filter(|m| {
            // The client leaf's own charging regime, by its entry points:
            // the two fold states, the slider that drives them, and
            // `charged_group_key` — the fold's single charged route to
            // `group_key` (review round 1's `[high]`).
            const LEAF_REGIME: [&str; 4] = [
                "ReduceFold::",
                "SelectFold::",
                "RangeSlideState::",
                "charged_group_key",
            ];
            m.external_callers
                .iter()
                .any(|c| LEAF_REGIME.iter().any(|r| c.contains(r)))
        })
        .map(|m| m.name.as_str())
        .collect();
    assert_eq!(
        genuinely_shared, SHARED_WITH_THE_LEAF,
        "a function is shared between the funnel and the client leaf's charging regime that \
         `SHARED_WITH_THE_LEAF` does not name (or names one that is no longer shared). The \
         Shared class is ENUMERATED, not predicated: a third member needs adjudication, not a \
         list edit — it must be a pure function of its arguments that allocates nothing into \
         the funnel's accounting, as `group_key` and `pin_reduction_order` are"
    );
}

/// **The census's external-caller map, pinned whole.** Two members of
/// the funnel's closure are also called from OUTSIDE it, and the two
/// cases are not the same kind of thing:
///
/// * `group_range` / `group_instant` / `apply_vector_aggs` /
///   `combine_binary` — called from the engine's own metric entry points
///   (`run_metric_client`, `run_metric_node`, `run_metric_inner`,
///   `run_client_agg_rows_folded`, `VariantsAggState::finish_in_place`)
///   and from the SQL path's two INLINE chains. Those are the funnel's
///   production entry sites: each acquires a token when §4 is wired, so
///   sharing is expected and benign.
/// * **`group_key` and `pin_reduction_order` are called by the Part B
///   FOLD at the client leaf** (`ReduceFold::push_series`,
///   `SelectFold::push_series`, `RangeSlideState::emit`), which holds no
///   funnel token and must not: the fold's bytes are charged by the
///   leaf's own `charge_group_bytes` / `charge_result_points`, and plan
///   v14 §10.3 forbids threading one counter across the two regimes.
///   Plan v14 §4.1 was written at `d145ded`, before the fold existed, so
///   its four-class table has no disposition for a member shared across
///   two charging regimes.
///
/// The map is pinned WHOLE rather than filtered by a rule, because a rule
/// that decides which sharing is benign is exactly the judgement that
/// needs adjudicating rather than encoding.
#[test]
fn the_external_caller_map_of_the_funnel_closure_is_pinned() {
    let (_, members) = census_members();
    let mut got: Vec<(String, Vec<String>)> = members
        .iter()
        .filter(|m| m.allocates && m.stage_data && !m.external_callers.is_empty())
        .map(|m| {
            let mut c = m.external_callers.clone();
            c.sort();
            (m.name.clone(), c)
        })
        .collect();
    got.sort();

    let want: Vec<(String, Vec<String>)> = vec![
        (
            // Since review round 1's `[high]` the fold reaches `group_key`
            // through ONE charging route instead of two direct call
            // sites, which is why this entry shrank.
            "group_key".to_string(),
            vec!["fold.rs::charged_group_key".to_string()],
        ),
        (
            "pin_reduction_order".to_string(),
            vec!["client_agg.rs::RangeSlideState::emit".to_string()],
        ),
    ];
    assert_eq!(
        got, want,
        "the funnel closure's external-caller map has changed; every new entry is either a \
         production entry site that must acquire a token or a second charging regime that \
         cannot"
    );
}

/// **The proof-token list is derived-checked** (issue #290).
///
/// `PROOF_TOKENS` keys six of the census's predicates, so a token type
/// nobody added to it would go invisible in all six at once. This is what
/// says so, by name.
///
/// **Where it stops, said out loud.** It closes "a proof token exists
/// whose type the predicates do not know about" for a token minted by an
/// `acquire*` constructor in any file `collect()` parses, and it turns a
/// FREE such constructor into a named panic. It does NOT close a token
/// minted by a constructor named something else, nor one declared in
/// `src/logql/template/` or `src/logql/testkit/`, which the
/// non-recursive walk never reads (#302). Both are accepted open by
/// ruling, and both are empty at this commit.
#[test]
fn the_proof_token_list_is_derived_from_the_acquire_constructors() {
    let (_, all) = region_census::collect();
    let derived = region_census::token_owners(&all);
    let published: std::collections::BTreeSet<String> =
        PROOF_TOKENS.iter().map(|t| (*t).to_string()).collect();
    assert_eq!(
        derived, published,
        "a type mints a proof token that `PROOF_TOKENS` does not name (or names one that no \
         longer mints). Six census predicates are generated from that list, so the difference \
         is a hole in all six — it needs adjudication, not a list edit"
    );
}

/// **No semantic refusal may live below the charge** (issue #290 §7).
///
/// The set of functions under `combine_binary_capped` that construct a
/// NON-budget `ReadError` is derived from the call graph and compared
/// against the published answer. A sixth one is a refusal the class-(P)
/// preflight does not decide, which is the defect this issue closes,
/// reappearing one layer down.
///
/// **Limit, stated rather than implied.** The closure walks FREE
/// functions inside `src/logql/*.rs` only, so a refusal introduced inside
/// an inherent method, or in a subdirectory module, is invisible to it —
/// the same scope `region_census` has everywhere else. What covers the
/// wire is the runtime rows in `tests/logql_semantics_before_budget.rs`;
/// this covers the enumeration.
#[test]
fn every_semantic_refusal_under_the_binary_seam_is_decided_above_the_charge() {
    let (_, all) = region_census::collect();
    let free = region_census::free_fns(&all);

    // The callee closure of the binary seam, to a fixpoint.
    let mut closure: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut frontier = vec!["combine_binary_capped".to_string()];
    while let Some(name) = frontier.pop() {
        if !closure.insert(name.clone()) {
            continue;
        }
        let Some(info) = free.get(&name) else {
            continue;
        };
        for callee in &info.callees {
            let bare = callee.rsplit("::").next().unwrap_or(callee);
            if free.contains_key(bare) && !closure.contains(bare) {
                frontier.push(bare.to_string());
            }
        }
    }
    assert!(
        closure.contains("instant_join"),
        "the binary seam's closure lost the join: {closure:?}"
    );

    let mut got: Vec<&str> = closure
        .iter()
        .filter_map(|n| free.get(n))
        .filter(|f| {
            f.callees.iter().any(|c| {
                c.starts_with("ReadError::")
                    // The budget's own refusal is not a semantic one —
                    // it is the thing that must never preempt them.
                    && c != "ReadError::QueryTooBroad"
            })
        })
        .map(|f| f.name.as_str())
        .collect();
    got.sort_unstable();

    /// Every semantic refusal the binary funnel can raise, as the FIVE
    /// leaf constructors that build them. All five are class (P) — the
    /// operand-shape ones decided by `decide_shape` (P0, above every
    /// charge, unconditionally), the three join ones by
    /// `decide_binary_refusals` (P1, under its own charge) — so this
    /// funnel has no class-(A) member left.
    ///
    /// (P1) is decided above the stage charge whenever that charge would
    /// REFUSE, which is the only case in which its position matters:
    /// where the charge admits, `decide_binary`'s guard skips the
    /// preflight and `instant_join` raises the same three errors below
    /// the charge, with nothing to be preempted by. The one case where a
    /// budget breach still answers first is the scratch skip
    /// (`PreflightCharge::acquire` returning `None`), unreachable below
    /// ~6.75 million combined series and kept reproducible by
    /// `the_join_refusals_are_preempted_by_the_budget_when_the_preflight_is_skipped`.
    const EXPECT_SEMANTIC_REFUSALS: &[&str] = &[
        "duplicate_one_side_error",
        "grouping_unique_error",
        "incompatible_types_error",
        "multiple_matches_error",
        "set_op_scalar_error",
    ];
    assert_eq!(
        got, EXPECT_SEMANTIC_REFUSALS,
        "a semantic refusal under the binary seam is not in the published set. It must be \
         decided in `preflight::decide_shape` or `preflight::decide_binary_refusals` and added \
         here with the acceptance row that pins it — never added silently, and never left below \
         `Ledger::acquire_binary` where a budget breach can answer first"
    );
}

/// **`mod preflight` allocates only its six reserved buffers** — the
/// SOURCE half, and it is defence in depth, not the enforcement (issue
/// #290 §3).
///
/// `FnInfo.callees` records DIRECT calls, so what this can support is
/// exactly: *no function defined in `mod preflight` names an allocating
/// token outside the allowed set*. It cannot see a helper defined
/// elsewhere that the preflight calls. The byte bound itself is measured,
/// by `tests/logql_preflight_alloc_gate.rs`, which is closed over the
/// callee closure by construction.
///
/// The forbidden set is derived by SUBTRACTION, so a token added to
/// `ALLOC_TOKENS` later is automatically forbidden here.
#[test]
fn the_preflight_module_allocates_only_its_six_reserved_buffers() {
    /// Reserving a buffer at a count known up front, filling it, and
    /// sorting it in place are what the six-buffer charge prices.
    /// `sort_by` is absent on purpose: it allocates `n/2`.
    const PREFLIGHT_ALLOWED_ALLOC: &[&str] = &[
        ".push",
        ".sort_unstable",
        ".sort_unstable_by",
        ".sort_unstable_by_key",
        ".with_capacity",
        "Vec::with_capacity",
    ];
    let (_, all) = region_census::collect();
    let scanned: Vec<&region_census::FnInfo> = all
        .iter()
        .filter(|f| {
            f.module
                .as_deref()
                .is_some_and(|m| m == "preflight" || m.starts_with("preflight::"))
        })
        .collect();
    assert!(
        scanned.len() >= 8,
        "only {} functions found in `mod preflight` — the module scope is wrong",
        scanned.len()
    );
    for f in &scanned {
        for token in ALLOC_TOKENS {
            if PREFLIGHT_ALLOWED_ALLOC.contains(token) {
                continue;
            }
            assert!(
                !f.callees.contains(*token),
                "`{}` (module {:?}) names the allocating token `{token}`. The class-(P) \
                 preflight's charge prices SIX exactly-reserved buffers and nothing else; an \
                 allocation outside them is uncharged scratch above the stage charge",
                f.display,
                f.module
            );
        }
    }
}

/// The generator's companion for the census: prints the derived
/// classification so it can be pasted into a review.
#[test]
#[ignore = "generator — prints the census-derived region table"]
fn zz_print_region_census() {
    let (files, members) = census_members();
    println!("files scanned: {files:?}");
    println!(
        "{:<24} {:<10} {:<6} {:<6} external callers",
        "member", "file", "alloc", "stage"
    );
    for m in &members {
        println!(
            "{:<24} {:<10} {:<6} {:<6} {:?}",
            m.name, m.file, m.allocates, m.stage_data, m.external_callers
        );
    }
}

// =====================================================================
// 13. AC 34(a) — the half of the flat-`or` class that #236 owns
// =====================================================================

/// **The accumulated multi-leaf operand, bounded.** A flat `a or b or c …`
/// chain accumulates into one growing operand: before #236 the
/// accumulation returned NO error on this path and grew without bound
/// (plan v14 §11 measured `err = None` in every completed run). It is now
/// charged per `combine_binary`, so a chain wide enough to matter is a
/// clean `MetricPostAggBytes` instead.
///
/// **What this does NOT claim.** The binding failure mode for a flat
/// chain is a process ABORT in `plan()` at ≈49 KB of query text — an
/// order of magnitude before the memory becomes interesting — and that
/// belongs to the recursion-guard work (#255/#272), not here. This test
/// pins only the half #236 fixes, at a term count well inside the
/// planable range.
#[test]
fn a_flat_or_accumulation_is_refused_rather_than_grown() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    const TERMS: usize = 128; // far inside the ~1 200-term planable range
    const PER_TERM: u64 = 64;
    // A cap the accumulator crosses partway through, so the refusal
    // happens DURING accumulation rather than on the first operand: the
    // model grows ~125 KB per term at this shape, so 8 MiB is crossed
    // around term 66 of 128.
    let cap = 8 * 1024 * 1024u64;

    let term = |t: u64| -> Vec<VectorSample> {
        (0..PER_TERM)
            .map(|i| VectorSample {
                labels: vec![
                    ("id00".to_string(), pad(t * PER_TERM + i, 8)),
                    ("t000".to_string(), pad(t, 8)),
                ],
                value: (i + 1) as f64,
            })
            .collect()
    };

    let mut acc = Some(QueryResult::Vector(term(0)));
    let mut refused_at = None;
    let mut widest = 0usize;
    let (_, w) = measure(|| {
        for t in 1..TERMS as u64 {
            let mut charged = 0u64;
            let rhs = QueryResult::Vector(term(t));
            let lhs = acc
                .take()
                .expect("the accumulator survives until it is refused");
            match combine_binary_capped(&mut charged, BinOp::Or, false, None, lhs, rhs, cap) {
                Ok(next) => {
                    if let QueryResult::Vector(v) = &next {
                        widest = widest.max(v.len());
                    }
                    acc = Some(next);
                    assert_eq!(charged, 0, "term {t}: the counter must discharge");
                }
                Err(ReadError::QueryTooBroad(TooBroadReason::MetricPostAggBytes {
                    bytes,
                    cap: got,
                })) => {
                    assert_eq!(got, cap);
                    assert!(bytes > got);
                    assert_eq!(charged, 0, "a refused acquire must not mutate the counter");
                    // The accumulator was CONSUMED by the failed call —
                    // which is the point: the growth stops at the
                    // refusal, and there is nothing left to grow.
                    refused_at = Some(t);
                    break;
                }
                Err(other) => panic!("term {t}: unexpected error {other:?}"),
            }
        }
    });
    assert!(!w.overflow, "cohort table overflowed");
    let refused_at = refused_at.expect(
        "the accumulation must be REFUSED before it exhausts the term budget — an unbounded \
         growth path is exactly what issue #236 closes on this class",
    );
    assert!(
        refused_at > 1 && (refused_at as usize) < TERMS,
        "the refusal must happen DURING accumulation, not on the first or last term (at {refused_at})"
    );
    assert!(
        widest >= PER_TERM as usize * 2,
        "the accumulator must genuinely have grown before the refusal (widest = {widest})"
    );
}

/// AC 34(c): the flat-chain class carries **no** ledger row. It was
/// registered as "O9" in an earlier plan revision and withdrawn — a
/// process abort is a crash, not a divergence, and we do not register
/// crashes as divergences.
#[test]
fn the_flat_chain_class_has_no_ledger_row() {
    let ledger = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/benchmarks/logs-differential-ledger.md"),
    )
    .expect("read the differential ledger");
    assert!(
        !ledger.contains("O9"),
        "the flat-`or` accumulation class must carry no ledger entry: the binding failure mode \
         is a process abort in plan(), which belongs to #255/#272 as a crash, not here as a \
         divergence"
    );
}
