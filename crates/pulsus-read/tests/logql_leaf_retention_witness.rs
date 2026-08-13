//! Issue #241 wave 2 — the INSTANT client leaf's retained-byte witness.
//!
//! The #227/#260 censuses in `charge.rs` enumerate CHARGES, and
//! `MAX_LEAF_RETAINED_BYTES`' own doc says what that costs: *"A retained
//! structure governed by NO cap is outside it by construction."* A
//! registry of charges cannot see a retention nobody charged. So this
//! file does not build a list. It turns the audit table into an
//! **equation with a residual that must be zero**, measured on the
//! shipped leaf seam:
//!
//! ```text
//! measured_peak  <=  instant_leaf_charged_bytes(state)
//!                 +  instant_leaf_bounded_bytes(observed fixture quantities)
//! ```
//!
//! Every addend on the right is either a CHARGED counter of
//! `ClientAggState` or a row of `charge.rs`'s query-lifetime audit table.
//! If the identity does not close, the unexplained bytes name an
//! uncharged query-lifetime container — that is the deliverable, not a
//! number to tune.
//!
//! # Provenance of the instrument
//!
//! §1 below is carried from `tests/logql_post_agg_witness.rs:83-324`
//! (the cohort table, `OVERFLOW`, `IN_WINDOW`, `measure`). It is
//! DUPLICATED rather than shared because a `#[global_allocator]` is
//! per-test-binary, and this crate already runs a dozen such binaries.
//! **If you fix a defect in one, the other exists** — that is the whole
//! reason this paragraph is here. Hoisting it into `pulsus-testkit` would
//! touch a shared crate and the shipped witness in one change and was
//! deliberately not done.
//!
//! ## THE COHORT ATTRIBUTION RULE (issue #236 §8 C6)
//!
//! * an allocation belongs to the window that was open, **on the
//!   measuring thread**, when its pointer was returned;
//! * a free is counted only against the window that owns that pointer;
//! * a realloc is a free plus an allocation, attributed the same way;
//! * allocations from any other thread are ignored.
//!
//! So [`Window::peak`] is "the maximum bytes this window itself held
//! live" and [`Window::retained`] is "the bytes this window allocated
//! that are still live at close", and neither can be masked by unrelated
//! frees.
//!
//! # Scope — what this file claims, and what it does not
//!
//! **It claims the INSTANT leaf.** `RangeSlideState` discharges its
//! retention counters as the window slides, so a snapshot taken at finish
//! is below its own peak and the identity above would be unsound for it;
//! `run_client_agg_rows_folded_measured` reports `None` there rather than
//! a zero that reads as a fact. The variants fan-out is likewise out:
//! `AggCaps::divided(n)` splits every counter across sub-states and the
//! seam here drives one.
//!
//! **The post-aggregation path is excluded by CONSTRUCTION.** Every
//! fixture drives the seam with `aggs: &[]`, and
//! `an_empty_aggregation_chain_is_a_zero_allocation_passthrough`
//! (`tests/logql_post_agg_witness.rs`) measures `apply_vector_aggs(result,
//! &[])` at `peak == 0` and `count == 0`, so the call the seam makes
//! contributes exactly zero to these windows. A fixture that reintroduced
//! a chain fails `every_fixture_drives_an_empty_aggregation_chain` rather
//! than quietly widening the identity.
//!
//! # Four routes around this file's claim, in `RowBudgets`' own terms
//!
//! `charge.rs`'s `RowBudgets` doc states that its enumeration is *"a CONVENTION
//! with a compiler-checked back half"* and that *"the published figure is
//! sound for the ledgers listed and says nothing about a ledger nobody
//! listed."* The same sentence, with "ledger" replaced by
//! "fixture-driven path", is this file's claim. Concretely:
//!
//! 1. **Fixture coverage — the largest.** A retained allocation on a
//!    client-aggregation path no fixture exercises leaves every number
//!    here unchanged. `the_reachable_pair_coverage_table_is_generated`
//!    converts this from an unstated gap into a visible list; it does not
//!    close it.
//! 2. **Thread locality, by design.** `IN_WINDOW` is a thread-local;
//!    `client_agg.rs` spawns nothing today and a future spawn would be
//!    invisible here.
//! 3. **The two-place edit.** Changing the model and a pinned figure in
//!    one commit. `RowBudgets`' route 2 — a semantic choice no mechanism
//!    can see.
//! 4. **A term computed from a quantity the fixture does not vary.** The
//!    necessity sweep below reports exactly this and is honest about what
//!    it found.
//!
//! # ENUMERATION-CHECKLIST
//!
//! Every enumeration this file asserts over, classified against all three
//! clauses of issue #241's enumeration rule. The rule, once:
//!
//! 1. **restated?** — iterated from a macro-emitted `ALL`, answered by a
//!    wildcard-free `match`, or labelled input data / an argument;
//! 2. **wrongly scoped?** — the domain it is derived over, and the
//!    per-run assertion proving that domain is the SEAM's rather than the
//!    type's full variant list;
//! 3. **renamed parallel list?** — can two declarations of the same field
//!    set disagree and still compile? If yes, delete one.
//!
//! Discovery of this list's INPUT is syntax-aware, not a regex: every
//! enumeration carries a `/// ENUMERATION: <name>` marker on its
//! declaring item, `the_enumeration_checklist_covers_every_marked_item`
//! `syn::parse_file`s this file and walks it with `syn::visit::Visit`,
//! and the two sides are asserted equal in BOTH directions.
//!
//! - FIXTURES | restated: input data — it makes no completeness claim, which is precisely why the coverage table exists beside it | scoped: n/a, an argument | parallel: no second declaration; the coverage table names fixtures by this array's own `name` field and resolves each through it
//! - REACHABLE_PAIRS | restated: iterated from `RangeAggOp::ALL` x `ClientValue::ALL`, both macro-emitted beside their enums | scoped: derived over the pairs the PLANNER produces, proved per run against `MetricPlan.client.value` — not against `plan()`'s verdict, which a text that planned a different value also satisfies, and not against the types' variant lists | parallel: no — the row set, each row's `ReducerClass` and each row's status are all computed
//! - coverage_status | restated: wildcard-free `match` over `RangeAggOp` and, inside each arm, over `ClientValue` | scoped: only the planner-reachable pairs are asked for a status; the rest are answered `Unreachable` by the predicate, which the agreement gate ties to the planner | parallel: no second table; `Driven(name)` is resolved against FIXTURES and the fixture's own planned `(op, value)` is asserted
//! - reachable | restated: wildcard-free `match` over `RangeAggOp` mirroring `plan.rs:1760-1776` and `:1829-1835` | scoped: proved per run against the planner's derived value, row by row | parallel: `plan.rs` is the other declaration by necessity — the agreement gate is what stops the two drifting, and it is line-targeted at `plan.rs:1831`
//! - LeafBoundedTerm_ALL | restated: iterated from `LeafBoundedTerm::ALL`, macro-emitted beside the enum the model's own suppression `match` reads | scoped: the suppressible terms of `instant_leaf_bounded_bytes`, which is exactly the function swept | parallel: no — one declaration, in `client_agg.rs`
//! - QueryResult_at_the_instant_seam | restated: wildcard-free `match` in `client_agg.rs`'s `LeafBoundedInput::observe` | scoped: `finish_folded` constructs `Vector` on every path, and this file asserts it PER RUN rather than declaring it once | parallel: no
//!
//! Not a line here, and deliberately: `instant_leaf_charged_bytes`'
//! exhaustive destructure of `ClientAggState`. This file asserts nothing
//! over it — the COMPILER does, and a field added without a disposition
//! is `error[E0027]` in `client_agg.rs`, not a failure here.
//!
//! **The boundary, in one sentence:** the scan covers this file's own
//! declarations that carry the marker; an enumeration written without the
//! marker is invisible to it; that residual is accepted, and it is why
//! this block is a review aid rather than a proof.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use pulsus_logql::RangeAggOp;
use pulsus_read::logql::pipeline::CompiledPipeline;
use pulsus_read::logql::plan::VectorAggSpec;
use pulsus_read::logql::rows::{MetricScanRow, StreamMetaRow};
use pulsus_read::logql::{
    ClientValue, ClientWindow, Direction, LeafBoundedInput, LeafBoundedTerm, Plan, PlanCtx,
    QueryParams, QueryResult, QuerySpec, ReducerClass, instant_leaf_bounded_bytes,
    instant_leaf_bounded_bytes_without, plan, reducer_class, run_client_agg_rows_folded_measured,
};

// =====================================================================
// 1. The instrument (carried from tests/logql_post_agg_witness.rs)
// =====================================================================

const COHORT_SLOTS: usize = 1 << 21;
const COHORT_MASK: usize = COHORT_SLOTS - 1;
/// An insert that cannot find a free slot within this many probes sets
/// [`OVERFLOW`], which every gate asserts is clear — the instrument FAILS
/// LOUDLY rather than degrading silently.
const PROBE_BOUND: usize = 128;

static SLOT_KEY: [AtomicUsize; COHORT_SLOTS] = [const { AtomicUsize::new(0) }; COHORT_SLOTS];
static SLOT_SIZE: [AtomicU64; COHORT_SLOTS] = [const { AtomicU64::new(0) }; COHORT_SLOTS];
static SLOT_GEN: [AtomicU64; COHORT_SLOTS] = [const { AtomicU64::new(0) }; COHORT_SLOTS];
static GENERATION: AtomicU64 = AtomicU64::new(1);
static OVERFLOW: AtomicBool = AtomicBool::new(false);

static W_BYTES: AtomicU64 = AtomicU64::new(0);
static W_COUNT: AtomicU64 = AtomicU64::new(0);
static W_LIVE: AtomicU64 = AtomicU64::new(0);
static W_PEAK: AtomicU64 = AtomicU64::new(0);
/// The NAIVE quantities — kept only so the discrimination control can
/// show the two instruments disagree. Never used by a model gate.
static BULK_LIVE: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// `const`-initialised and `Drop`-free, so reading it registers no
    /// destructor and cannot allocate — a requirement, not an
    /// optimisation, since this is read from inside the allocator.
    static IN_WINDOW: Cell<bool> = const { Cell::new(false) };
}

/// Every test in this binary serialises here: the counters and the cohort
/// table are process-global statics.
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

/// Removes `ptr` from the cohort, returning its size iff THIS window owns
/// it. Deletion re-inserts the rest of the probe cluster, so a lookup can
/// never be cut short by a hole.
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
    BULK_LIVE.fetch_add(size, Ordering::Relaxed);
}

fn on_free(ptr: usize, size: u64) {
    if let Some(owned) = table_remove(ptr) {
        W_LIVE.fetch_sub(owned, Ordering::Relaxed);
    }
    // The naive counter decrements on EVERY free, owned or not, and
    // saturates at zero — the masking class this instrument exists to
    // avoid, reproduced verbatim so the control can measure the
    // difference.
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
    bytes: u64,
    count: u64,
    /// **The load-bearing quantity**: the maximum bytes this window
    /// itself held live.
    peak: u64,
    /// Bytes this window allocated that are still live at close.
    retained: u64,
    /// The naive `LIVE` at close — never gates; kept for the control.
    bulk_live_at_close: u64,
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

    IN_WINDOW.with(|c| c.set(true));
    let out = f();
    IN_WINDOW.with(|c| c.set(false));

    let w = Window {
        bytes: W_BYTES.load(Ordering::SeqCst),
        count: W_COUNT.load(Ordering::SeqCst),
        peak: W_PEAK.load(Ordering::SeqCst),
        retained: W_LIVE.load(Ordering::SeqCst),
        bulk_live_at_close: BULK_LIVE.load(Ordering::SeqCst),
        overflow: OVERFLOW.load(Ordering::SeqCst),
    };
    (out, w)
}

// =====================================================================
// 2. The fixtures — INPUT DATA
// =====================================================================

const START_NS: i64 = 1_782_907_200_000_000_000;

/// One fixture: a query and the shape of the scan it is driven over.
#[derive(Clone, Copy, Debug)]
struct Fixture {
    name: &'static str,
    query: &'static str,
    streams: usize,
    /// Extracted/base label pairs per stream.
    pairs: usize,
    /// Bytes in each stream label VALUE — the axis `hydrated_label_bytes`
    /// tracks.
    value_bytes: usize,
    rows_per_stream: usize,
    /// All of a stream's rows share ONE timestamp — the equal-timestamp
    /// staging path (`ClientAggState::pending`).
    ties: bool,
}

/// ENUMERATION: FIXTURES
///
/// **Input data, not a set claim.** This array makes no assertion about
/// covering the reachable `(RangeAggOp, ClientValue)` space; the coverage
/// table generated by `the_reachable_pair_coverage_table_is_generated` is
/// what makes the uncovered part of that space visible, and most of it
/// IS uncovered.
const FIXTURES: &[Fixture] = &[
    Fixture {
        name: "count_nonmutating",
        query: r#"count_over_time({app="a"} | env = "prod" [5m])"#,
        streams: 64,
        pairs: 6,
        value_bytes: 24,
        rows_per_stream: 16,
        ties: false,
    },
    Fixture {
        name: "count_mutating",
        query: r#"count_over_time({app="a"} | logfmt [5m])"#,
        streams: 64,
        pairs: 6,
        value_bytes: 24,
        rows_per_stream: 16,
        ties: false,
    },
    // The `hydrated_label_bytes` pair for `count_nonmutating`: same
    // shape, 8x the label value width.
    Fixture {
        name: "count_nonmutating_wide_labels",
        query: r#"count_over_time({app="a"} | env = "prod" [5m])"#,
        streams: 64,
        pairs: 6,
        value_bytes: 192,
        rows_per_stream: 16,
        ties: false,
    },
    // The `hydrated_streams` pair for `count_nonmutating`: same shape,
    // 8x the stream count.
    Fixture {
        name: "count_nonmutating_many_streams",
        query: r#"count_over_time({app="a"} | env = "prod" [5m])"#,
        streams: 512,
        pairs: 6,
        value_bytes: 24,
        rows_per_stream: 2,
        ties: false,
    },
    Fixture {
        name: "bytes_nonmutating",
        query: r#"bytes_over_time({app="a"} | env = "prod" [5m])"#,
        streams: 64,
        pairs: 6,
        value_bytes: 24,
        rows_per_stream: 16,
        ties: false,
    },
    Fixture {
        name: "rate_nonmutating",
        query: r#"rate({app="a"} | env = "prod" [5m])"#,
        streams: 64,
        pairs: 6,
        value_bytes: 24,
        rows_per_stream: 16,
        ties: false,
    },
    Fixture {
        name: "sum_unwrap",
        query: r#"sum_over_time({app="a"} | logfmt | unwrap v [5m])"#,
        streams: 64,
        pairs: 6,
        value_bytes: 24,
        rows_per_stream: 16,
        ties: false,
    },
    // The staging pair for `sum_unwrap`: a `CanonicalFold` reducer whose
    // rows all share one timestamp, so `ClientAggState::pending` is
    // non-empty for the whole scan.
    Fixture {
        name: "sum_unwrap_equal_timestamps",
        query: r#"sum_over_time({app="a"} | logfmt | unwrap v [5m])"#,
        streams: 64,
        pairs: 6,
        value_bytes: 24,
        rows_per_stream: 16,
        ties: true,
    },
    Fixture {
        name: "min_unwrap",
        query: r#"min_over_time({app="a"} | logfmt | unwrap v [5m])"#,
        streams: 64,
        pairs: 6,
        value_bytes: 24,
        rows_per_stream: 16,
        ties: false,
    },
    Fixture {
        name: "quantile_unwrap",
        query: r#"quantile_over_time(0.9, {app="a"} | logfmt | unwrap v [5m])"#,
        streams: 64,
        pairs: 6,
        value_bytes: 24,
        rows_per_stream: 16,
        ties: false,
    },
    Fixture {
        name: "rate_counter_unwrap",
        query: r#"rate_counter({app="a"} | logfmt | unwrap v [5m])"#,
        streams: 64,
        pairs: 6,
        value_bytes: 24,
        rows_per_stream: 16,
        ties: false,
    },
    // Zero rows on purpose: `absent_over_time` reports absence, so the
    // synthetic one-series emit is only reachable with nothing present.
    Fixture {
        name: "absent_no_rows",
        query: r#"absent_over_time({app="a"}[5m])"#,
        streams: 8,
        pairs: 6,
        value_bytes: 24,
        rows_per_stream: 0,
        ties: false,
    },
];

fn plan_ctx() -> PlanCtx<'static> {
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

fn instant_params() -> QueryParams {
    QueryParams {
        spec: QuerySpec::Instant { at_ns: START_NS },
        limit: 100,
        direction: Direction::Backward,
    }
}

fn build_meta(fx: &Fixture) -> HashMap<u64, StreamMetaRow> {
    let pad = "x".repeat(fx.value_bytes);
    (0..fx.streams as u64)
        .map(|fp| {
            let mut labels = String::from(r#"{"app":"a","env":"prod""#);
            for p in 0..fx.pairs.saturating_sub(2) {
                labels.push_str(&format!(r#","k{p:02}":"{pad}{fp}""#));
            }
            labels.push('}');
            (
                fp,
                StreamMetaRow {
                    fingerprint: fp,
                    service: format!("svc{fp:03}"),
                    labels,
                },
            )
        })
        .collect()
}

fn build_rows(fx: &Fixture) -> Vec<MetricScanRow> {
    let pad = "y".repeat(fx.value_bytes);
    let mut rows = Vec::with_capacity(fx.streams * fx.rows_per_stream);
    for fp in 0..fx.streams as u64 {
        for i in 0..fx.rows_per_stream {
            let ts = if fx.ties {
                START_NS - 1_000_000
            } else {
                START_NS - 1_000_000 - (i as i64) * 1_000
            };
            rows.push(MetricScanRow {
                fingerprint: fp,
                timestamp_ns: ts,
                body: format!("v={} env=prod w={pad}{fp} tag=t{i:03}", i + 1),
                structured_metadata: String::new(),
            });
        }
    }
    rows.sort_by_key(|r| (r.fingerprint, r.timestamp_ns));
    rows
}

/// One fixture, planned and driven — everything built OUTSIDE any window.
struct Driven {
    fx: Fixture,
    rows: Vec<MetricScanRow>,
    meta: HashMap<u64, StreamMetaRow>,
    compiled: CompiledPipeline,
    client: pulsus_read::logql::ClientAgg,
    window: ClientWindow,
    rate_window_ns: Option<u64>,
}

fn drive(fx: &Fixture) -> Driven {
    let expr = pulsus_logql::parse(fx.query).unwrap_or_else(|e| panic!("{}: {e}", fx.name));
    let Plan::Metric(mp) = plan(&expr, &instant_params(), &plan_ctx())
        .unwrap_or_else(|e| panic!("{}: {e:?}", fx.name))
    else {
        panic!("{}: expected a Metric plan", fx.name);
    };
    let client = mp
        .client
        .as_ref()
        .unwrap_or_else(|| panic!("{}: fixture must be client-aggregated", fx.name))
        .clone();
    assert!(
        mp.step_ns.is_none(),
        "{}: fixture must be an INSTANT plan — the range slider is out of this file's scope",
        fx.name
    );
    let compiled = CompiledPipeline::compile(&client.pipeline).expect("compile");
    Driven {
        fx: *fx,
        rows: build_rows(fx),
        meta: build_meta(fx),
        compiled,
        client,
        window: ClientWindow::Instant {
            start_ns: mp.grid_start_ns,
            end_ns: mp.end_ns,
        },
        rate_window_ns: mp.rate_window_ns,
    }
}

/// The empty chain every fixture drives — see the module doc's structural
/// exclusion of the post-aggregation path.
const NO_AGGS: &[VectorAggSpec] = &[];

impl Driven {
    fn run(&self) -> (QueryResult, u64) {
        let (result, charged) = run_client_agg_rows_folded_measured(
            &self.rows,
            &self.compiled,
            &self.meta,
            &self.client,
            self.window,
            self.rate_window_ns,
            NO_AGGS,
        )
        .unwrap_or_else(|e| panic!("{}: {e:?}", self.fx.name));
        let charged = charged.unwrap_or_else(|| {
            panic!(
                "{}: the seam reported no instant-leaf charge — the fixture is not on the \
                 instant path",
                self.fx.name
            )
        });
        (result, charged)
    }

    /// The measured cell: the SAME call, with its result released inside
    /// the window so `retained` means "allocated and not released".
    fn measured(&self) -> (u64, Window) {
        measure(|| {
            let (result, charged) = self.run();
            drop(result);
            charged
        })
    }
}

// =====================================================================
// 3. The residual identity
// =====================================================================

/// **The gate this file exists for.** For every fixture cell:
///
/// ```text
/// measured_peak <= instant_leaf_charged_bytes + instant_leaf_bounded_bytes
/// ```
///
/// with every addend on the right either a CHARGED counter of
/// `ClientAggState` (through the one exhaustive destructure that prices
/// them) or a row of `charge.rs`'s query-lifetime audit table.
///
/// If this fails, the unexplained bytes name an uncharged query-lifetime
/// container on the instant leaf. That is a stop-and-report finding, not
/// a number to widen.
#[test]
fn the_residual_identity_closes_for_every_fixture() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for fx in FIXTURES {
        let d = drive(fx);
        // (0) Observation run, UNMEASURED — the emitted quantities come
        // from a run whose own allocations are not in any window.
        let (result, charged_probe) = d.run();
        let observed = LeafBoundedInput::observe(&d.meta, &result);
        drop(result);

        // (A) The measured cell.
        let (charged, w) = d.measured();
        assert!(!w.overflow, "{}: cohort table overflowed: {w:?}", fx.name);
        assert_eq!(
            charged, charged_probe,
            "{}: the charged snapshot is not deterministic across runs",
            fx.name
        );
        assert!(
            w.count > 0 && w.bytes > 0,
            "{}: the window observed nothing — the fixture does not exercise the seam: {w:?}",
            fx.name
        );
        let bounded = instant_leaf_bounded_bytes(&observed);
        let modelled = charged + bounded;
        println!(
            "IDENTITY {:<32} peak={:>9} charged={:>9} bounded={:>9} modelled={:>9} \
             slack={:>9}",
            fx.name,
            w.peak,
            charged,
            bounded,
            modelled,
            modelled.saturating_sub(w.peak)
        );
        assert!(
            w.peak <= modelled,
            "{}: RESIDUAL DOES NOT CLOSE — measured peak {} B exceeds charged {} B + bounded \
             {} B = {} B. The excess names a query-lifetime container on the instant leaf \
             that nothing charges and no audit row bounds. observed={observed:?} window={w:?}",
            fx.name,
            w.peak,
            charged,
            bounded,
            modelled
        );
    }
}

/// **The release gate** (issue #241): the seam frees everything it
/// allocates. `retained` is exactly zero for every cell — a free of a
/// pointer the window never allocated cannot lower it, and a missed free
/// can only raise it, so both failure directions are loud.
#[test]
fn every_fixture_releases_everything_it_allocates() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for fx in FIXTURES {
        let d = drive(fx);
        let (_charged, w) = d.measured();
        assert!(!w.overflow, "{}: cohort table overflowed: {w:?}", fx.name);
        assert_eq!(
            w.retained, 0,
            "{}: the instant leaf retained {} B past the drop of its own result: {w:?}",
            fx.name, w.retained
        );
    }
}

/// The control that makes `retained == 0` worth asserting — the I5 shape
/// (`logql_post_agg_witness.rs`), moved onto this seam.
///
/// The same fixture is run twice: once released inside the window, once
/// `mem::forget`-ed. The leaking run ALSO releases a copy of the scan
/// rows inside the window — which is what the engine does with each
/// chunk after folding it, and which the naive counter cannot tell from
/// the leak because it decrements on every free including pointers it
/// never allocated.
///
/// Reported as **what stays green**: on run B the retired bulk identity
/// (`allocated - freed <= residue`, i.e. `bulk_live_at_close <= residue`)
/// is deflated below the leak it is supposed to catch, while `retained`
/// reports the leak in full.
#[test]
fn a_leaked_leaf_result_is_loud_where_the_retired_bulk_identity_is_silent() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let fx = FIXTURES
        .iter()
        .find(|f| f.name == "count_mutating")
        .expect("the mutating fixture is committed");
    let d = drive(fx);

    let (_charged_a, wa) = d.measured();
    assert!(!wa.overflow, "run A: cohort table overflowed");
    assert_eq!(wa.retained, 0, "run A must retain nothing: {wa:?}");

    // Built OUTSIDE the window, released INSIDE it — the pre-window mass.
    let chunk = d.rows.clone();
    let ((), wb) = measure(|| {
        let (result, _charged) = d.run();
        drop(chunk);
        std::mem::forget(result);
    });
    println!(
        "I5 run A retained={} bulk={} | run B retained={} bulk={}",
        wa.retained, wa.bulk_live_at_close, wb.retained, wb.bulk_live_at_close
    );
    assert!(!wb.overflow, "run B: cohort table overflowed");
    assert!(
        wb.retained > 0,
        "run B leaked its result and `retained` did not see it — the release gate is \
         asserting something it cannot fail: {wb:?}"
    );
    assert!(
        wb.retained > wa.retained,
        "the leak must be visible as a DIFFERENCE against run A: {wa:?} vs {wb:?}"
    );
    assert!(
        wb.retained >= 16 * 4096,
        "the leak ({} B) must dwarf a 4 KiB residue or the discrimination proves nothing",
        wb.retained
    );
    // The discriminating assertion: the naive counter, computed by the
    // SAME run, is deflated below the leak by frees it does not own.
    assert!(
        wb.bulk_live_at_close < wb.retained,
        "the naive counter did not under-report the leak on this fixture, so reverting this \
         instrument to bulk arithmetic would not redden anything here: {wb:?}"
    );
}

/// The structural exclusion of the post-aggregation path, asserted rather
/// than assumed: every fixture drives an EMPTY aggregation chain, so
/// `apply_vector_aggs`' measured zero-allocation passthrough
/// (`logql_post_agg_witness.rs::
/// an_empty_aggregation_chain_is_a_zero_allocation_passthrough`) is what
/// keeps the post-aggregation maps out of these windows.
#[test]
fn every_fixture_drives_an_empty_aggregation_chain() {
    assert!(
        NO_AGGS.is_empty(),
        "the fixtures' aggregation chain is no longer empty — the residual identity would \
         then have to price the post-aggregation maps, which it does not"
    );
}

/// ENUMERATION: QueryResult_at_the_instant_seam
///
/// The scope of `LeafBoundedInput::observe`'s `QueryResult` destructure,
/// **proved per run rather than declared once**: the instant leaf's
/// output is constructed by `finish_folded`, which returns `Vector` on
/// every path. If the seam ever produces another variant this fires, and
/// the wildcard-free `match` in `client_agg.rs` must answer that arm with
/// a real term before the suite is green again.
#[test]
fn every_fixture_returns_a_vector() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for fx in FIXTURES {
        let d = drive(fx);
        let (result, _charged) = d.run();
        assert!(
            matches!(result, QueryResult::Vector(_)),
            "{}: the instant leaf returned {result:?}, not a Vector — B5's domain is no \
             longer the seam's",
            fx.name
        );
    }
}

/// The bounded side is doing work, measured rather than asserted:
/// dropping **every** bounded term at once must redden the identity for at
/// least one fixture, so `peak <= charged` alone is not what is being
/// gated.
///
/// **What this establishes, and no more.** It prints which fixtures
/// redden. On the committed set that is a SHORT list, because the charge
/// counters over-cover the measured peak on most cells — the charge model
/// prices a `LabelSet` clone plus a map slot at `group_entry_bytes`'
/// `8x`-slot-plus-pad rate, several times what the allocator actually
/// hands back. Read the printed list, not this sentence.
///
/// **This replaces AC 8's per-term necessity check, and the substitution
/// is disclosed rather than quiet.** The per-term form — zero one term,
/// require the identity to go red — cannot pass for ANY term of this
/// model, because every coefficient is a deliberate over-approximation
/// (`alloc_block_bytes` is `2x` by construction, `map_entry_bytes` is
/// `8x` a slot plus a 128 B pad, and B4 is a flat 8 MiB shipped ceiling).
/// Each term alone therefore has enough slack to cover its neighbours,
/// and the remedy AC 8 prescribes for a term that fails necessity —
/// delete it — would leave a `ClientAggState` field with no disposition
/// in the exhaustive destructure the same plan requires. The per-term
/// sweep is still RUN, by `the_necessity_sweep_reports_every_term`, and
/// its result is reported instead of being turned into a pass/fail.
#[test]
fn the_bounded_side_is_load_bearing() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut reddened = Vec::new();
    for fx in FIXTURES {
        let d = drive(fx);
        let (result, _c) = d.run();
        let observed = LeafBoundedInput::observe(&d.meta, &result);
        drop(result);
        let (charged, w) = d.measured();
        if w.peak > charged {
            reddened.push((fx.name, w.peak, charged, observed));
        }
    }
    for (name, peak, charged, _observed) in &reddened {
        println!("BOUNDED-LOAD {name}: peak={peak} > charged={charged}");
    }
    println!(
        "BOUNDED-LOAD {} of {} fixtures need the bounded side at all",
        reddened.len(),
        FIXTURES.len()
    );
    assert!(
        !reddened.is_empty(),
        "no fixture's measured peak exceeds its CHARGED bytes alone, so the bounded terms \
         are never exercised and this file gates nothing about them"
    );
}

/// ENUMERATION: LeafBoundedTerm_ALL
///
/// The per-term necessity sweep, run and REPORTED. Every suppressible
/// term of the model is dropped in turn and the identity re-evaluated
/// over every fixture; the outcome is printed, and the only assertion is
/// the one that is true of a loose upper-bound model: suppressing a term
/// can never RAISE the modelled total.
///
/// See `the_bounded_side_is_load_bearing` for why this is a report and
/// not a gate.
#[test]
fn the_necessity_sweep_reports_every_term() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut cells = Vec::new();
    for fx in FIXTURES {
        let d = drive(fx);
        let (result, _c) = d.run();
        let observed = LeafBoundedInput::observe(&d.meta, &result);
        drop(result);
        let (charged, w) = d.measured();
        cells.push((fx.name, charged, w.peak, observed));
    }
    for term in LeafBoundedTerm::ALL {
        let mut red = Vec::new();
        for (name, charged, peak, observed) in &cells {
            let full = instant_leaf_bounded_bytes(observed);
            let without = instant_leaf_bounded_bytes_without(observed, *term);
            assert!(
                without <= full,
                "{name}: suppressing {term:?} RAISED the model ({without} > {full})"
            );
            if *peak > charged + without {
                red.push(*name);
            }
        }
        println!(
            "NECESSITY {term:?}: reddens {} of {} fixtures{}",
            red.len(),
            cells.len(),
            if red.is_empty() {
                String::new()
            } else {
                format!(" ({red:?})")
            }
        );
    }
}

// =====================================================================
// 4. AC 16 — the reachable-pair coverage table, generated
// =====================================================================

/// Whether a fixture drives this `(op, value)` pair, or why it cannot
/// exist.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Coverage {
    /// A committed fixture drives it; the payload is its `Fixture::name`.
    Driven(&'static str),
    /// Reachable by a client query, with no fixture. **The deliverable:**
    /// this table's job is to make the uncovered set visible, not small.
    Uncovered,
    /// The planner can never produce this pair.
    Unreachable,
}

/// ENUMERATION: reachable
///
/// Mirrors `plan.rs`'s three rules — `requires_unwrap` (`:1760-1772`),
/// `forbids_unwrap` (`:1773-1776`) and the derived-value ladder
/// (`:1829-1835`) — in a wildcard-free `match`, so a new `RangeAggOp`
/// fails to compile until it is answered.
///
/// This is a CLAIM about `plan.rs`; what makes it a measured agreement
/// rather than a restatement is
/// `the_reachability_predicate_agrees_with_the_planner`, which runs the
/// real planner and compares against the planned leaf's
/// `MetricPlan.client.value` — not against `plan()`'s verdict, which a
/// text that planned a DIFFERENT value also satisfies.
fn reachable(op: RangeAggOp, value: ClientValue) -> bool {
    match op {
        // `requires_unwrap`: `has_unwrap` is forced true, so the derived
        // value is always `Unwrap`.
        RangeAggOp::SumOverTime
        | RangeAggOp::AvgOverTime
        | RangeAggOp::MinOverTime
        | RangeAggOp::MaxOverTime
        | RangeAggOp::StddevOverTime
        | RangeAggOp::StdvarOverTime
        | RangeAggOp::QuantileOverTime
        | RangeAggOp::FirstOverTime
        | RangeAggOp::LastOverTime
        | RangeAggOp::RateCounter => value == ClientValue::Unwrap,
        // `forbids_unwrap`, and not a bytes op: only `Count`.
        RangeAggOp::CountOverTime => value == ClientValue::Count,
        // `forbids_unwrap`, and a bytes op: only `Bytes`.
        RangeAggOp::BytesRate | RangeAggOp::BytesOverTime => value == ClientValue::Bytes,
        // Neither: `unwrap` is optional, and without it neither is a
        // bytes op, so `Count`.
        RangeAggOp::Rate | RangeAggOp::AbsentOverTime => {
            value == ClientValue::Unwrap || value == ClientValue::Count
        }
    }
}

/// ENUMERATION: coverage_status
///
/// Wildcard-free over `RangeAggOp` and, inside each arm, over
/// `ClientValue` — a new variant of either enum fails to compile until it
/// is answered, and simultaneously adds its rows to the table because the
/// table iterates `ALL`.
///
/// Only the planner-reachable pairs are asked; the rest are answered
/// `Unreachable` by the predicate before this is called.
fn coverage_status(op: RangeAggOp, value: ClientValue) -> Coverage {
    if !reachable(op, value) {
        return Coverage::Unreachable;
    }
    let driven = |name: &'static str| Coverage::Driven(name);
    match op {
        RangeAggOp::CountOverTime => match value {
            ClientValue::Count => driven("count_nonmutating"),
            ClientValue::Bytes | ClientValue::Unwrap => Coverage::Uncovered,
        },
        RangeAggOp::BytesOverTime => match value {
            ClientValue::Bytes => driven("bytes_nonmutating"),
            ClientValue::Count | ClientValue::Unwrap => Coverage::Uncovered,
        },
        RangeAggOp::Rate => match value {
            ClientValue::Count => driven("rate_nonmutating"),
            ClientValue::Bytes | ClientValue::Unwrap => Coverage::Uncovered,
        },
        RangeAggOp::SumOverTime => match value {
            ClientValue::Unwrap => driven("sum_unwrap"),
            ClientValue::Count | ClientValue::Bytes => Coverage::Uncovered,
        },
        RangeAggOp::MinOverTime => match value {
            ClientValue::Unwrap => driven("min_unwrap"),
            ClientValue::Count | ClientValue::Bytes => Coverage::Uncovered,
        },
        RangeAggOp::QuantileOverTime => match value {
            ClientValue::Unwrap => driven("quantile_unwrap"),
            ClientValue::Count | ClientValue::Bytes => Coverage::Uncovered,
        },
        RangeAggOp::RateCounter => match value {
            ClientValue::Unwrap => driven("rate_counter_unwrap"),
            ClientValue::Count | ClientValue::Bytes => Coverage::Uncovered,
        },
        RangeAggOp::AbsentOverTime => match value {
            ClientValue::Count => driven("absent_no_rows"),
            ClientValue::Bytes | ClientValue::Unwrap => Coverage::Uncovered,
        },
        RangeAggOp::BytesRate
        | RangeAggOp::AvgOverTime
        | RangeAggOp::MaxOverTime
        | RangeAggOp::StddevOverTime
        | RangeAggOp::StdvarOverTime
        | RangeAggOp::FirstOverTime
        | RangeAggOp::LastOverTime => match value {
            ClientValue::Count | ClientValue::Bytes | ClientValue::Unwrap => Coverage::Uncovered,
        },
    }
}

/// The query text for one `(op, has_unwrap)` — the ONE shape AC 16 pins,
/// so the counts below mean one thing. A different shape (a pipeline
/// stage, a grouping) could route differently in other respects, but it
/// cannot change `client.value`, which reads only `op` and `has_unwrap`.
fn probe_text(op: RangeAggOp, value: ClientValue) -> String {
    let unwrap = value == ClientValue::Unwrap;
    let sel = if unwrap {
        r#"{app="a"} | logfmt | unwrap v [5m]"#
    } else {
        r#"{app="a"}[5m]"#
    };
    if op == RangeAggOp::QuantileOverTime {
        format!("quantile_over_time(0.9, {sel})")
    } else {
        format!("{op}({sel})")
    }
}

/// The planner's answer for one probe text: whether it planned at all,
/// and what `ClientValue` it derived.
fn planned_value(text: &str) -> Option<Option<ClientValue>> {
    let expr = pulsus_logql::parse(text).ok()?;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: START_NS,
            end_ns: START_NS + 3_600_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let p = plan(&expr, &params, &plan_ctx()).ok()?;
    let Plan::Metric(mp) = p else {
        return Some(None);
    };
    Some(mp.client.as_ref().map(|c| c.value))
}

/// The count of `(op, value)` pairs the planner can actually produce.
/// **Pinned AND recomputed**: the figure below is recomputed from
/// `reachable` over `ALL x ALL` on every run, and separately from the
/// real planner — the load-bearing gate is that those two agree ROW BY
/// ROW, and this literal is the review event when the planner's rules
/// move.
const PINNED_SEMANTIC_MATCHES: usize = 17;

/// The count of probe texts the planner merely ACCEPTS. Different from
/// `PINNED_SEMANTIC_MATCHES` because query text carries the op and the
/// presence of `unwrap` and nothing else — `Count` versus `Bytes` is
/// DERIVED (`plan.rs:1829-1835`) and is unrepresentable in the text.
const PINNED_SYNTAX_ACCEPTS: usize = 22;

/// ENUMERATION: REACHABLE_PAIRS
///
/// **AC 16.** The 45 rows are generated by iterating
/// `RangeAggOp::ALL x ClientValue::ALL`; no row is written by hand, and a
/// new variant of either enum grows the table because the table iterates
/// the slice its own macro emits.
///
/// The load-bearing assertion is the ROW-LEVEL iff:
///
/// ```text
/// reachable(op, value)  <=>  ( plan() succeeded
///                              AND the planned leaf's client.value == value )
/// ```
///
/// Syntax acceptance alone is explicitly NOT the predicate. A text that
/// planned a DIFFERENT value satisfies `plan().is_ok()`, so an
/// acceptance-only gate cannot go red on derivation drift — which is the
/// one failure mode this exists to catch.
#[test]
fn the_reachability_predicate_agrees_with_the_planner() {
    let mut accepts = 0usize;
    let mut direct_semantic = 0usize;
    let mut predicate_true = 0usize;
    let mut predicate_and_semantic = 0usize;
    let mut rows = 0usize;
    let mut rowfails: Vec<String> = Vec::new();

    for op in RangeAggOp::ALL {
        for value in ClientValue::ALL {
            rows += 1;
            let text = probe_text(*op, *value);
            let planned = planned_value(&text);
            let ok = planned.is_some();
            let semantic = planned.flatten() == Some(*value);
            let predicate = reachable(*op, *value);
            if ok {
                accepts += 1;
            }
            if semantic {
                direct_semantic += 1;
            }
            if predicate {
                predicate_true += 1;
            }
            if predicate && semantic {
                predicate_and_semantic += 1;
            }
            if predicate != semantic {
                rowfails.push(format!(
                    "{op:?}/{value:?} predicate={predicate} semantic={semantic} \
                     planned={:?} text={text}",
                    planned.flatten()
                ));
            }
        }
    }

    println!(
        "PROBE241 rows={rows} accepts={accepts} direct_semantic={direct_semantic} \
         predicate_true={predicate_true} predicate_and_semantic={predicate_and_semantic} \
         row_iff={}/{rows}",
        rows - rowfails.len()
    );
    assert_eq!(
        rows,
        RangeAggOp::ALL.len() * ClientValue::ALL.len(),
        "the table is not the full `ALL x ALL` product"
    );
    assert!(
        rowfails.is_empty(),
        "the reachability predicate disagrees with the planner on {} of {rows} rows:\n  {}",
        rowfails.len(),
        rowfails.join("\n  ")
    );
    assert_eq!(
        direct_semantic, PINNED_SEMANTIC_MATCHES,
        "the planner now derives {direct_semantic} of the 45 pairs, not \
         {PINNED_SEMANTIC_MATCHES} — re-derive the pin against `plan.rs`"
    );
    assert_eq!(
        accepts, PINNED_SYNTAX_ACCEPTS,
        "the planner now accepts {accepts} of the 45 probe texts, not \
         {PINNED_SYNTAX_ACCEPTS} — re-derive the pin against `plan.rs`"
    );
    // Kept for the case it was written for: a future edit that collapses
    // acceptance into derivation. It is BLIND to derivation drift — the
    // two counts stay unequal throughout — which is what the row-level
    // iff above covers.
    assert_ne!(
        accepts, direct_semantic,
        "acceptance and derivation have become the same quantity; the row-level iff is now \
         the only thing distinguishing them"
    );
    // Both `ALL` slices are sets of distinct variants.
    let ops: BTreeSet<String> = RangeAggOp::ALL.iter().map(|o| format!("{o:?}")).collect();
    assert_eq!(
        ops.len(),
        RangeAggOp::ALL.len(),
        "`RangeAggOp::ALL` repeats"
    );
    let vals: BTreeSet<String> = ClientValue::ALL.iter().map(|v| format!("{v:?}")).collect();
    assert_eq!(
        vals.len(),
        ClientValue::ALL.len(),
        "`ClientValue::ALL` repeats"
    );
}

/// The coverage table itself: 45 generated rows, each carrying its
/// COMPUTED `ReducerClass` and its status. Printed in full, because the
/// uncovered set is the deliverable.
///
/// `Driven(name)` must name a committed fixture whose query really plans
/// to that `(op, value)` — otherwise a table row could claim coverage
/// that no fixture provides.
#[test]
fn the_reachable_pair_coverage_table_is_generated() {
    let mut driven = 0usize;
    let mut uncovered = 0usize;
    let mut unreachable = 0usize;
    println!("op | value | reducer_class | status");
    for op in RangeAggOp::ALL {
        for value in ClientValue::ALL {
            let status = coverage_status(*op, *value);
            let class: ReducerClass = reducer_class(*op, *value);
            println!("{op:?} | {value:?} | {class:?} | {status:?}");
            match status {
                Coverage::Driven(name) => {
                    driven += 1;
                    let fx = FIXTURES.iter().find(|f| f.name == name).unwrap_or_else(|| {
                        panic!("{op:?}/{value:?} names no such fixture: {name}")
                    });
                    let d = drive(fx);
                    assert_eq!(
                        (d.client.range_op, d.client.value),
                        (*op, *value),
                        "fixture `{name}` is claimed for {op:?}/{value:?} but plans to \
                         {:?}/{:?}",
                        d.client.range_op,
                        d.client.value
                    );
                }
                Coverage::Uncovered => uncovered += 1,
                Coverage::Unreachable => unreachable += 1,
            }
        }
    }
    println!("COVERAGE driven={driven} uncovered={uncovered} unreachable={unreachable}");
    assert_eq!(
        driven + uncovered + unreachable,
        RangeAggOp::ALL.len() * ClientValue::ALL.len()
    );
    assert_eq!(
        driven + uncovered,
        PINNED_SEMANTIC_MATCHES,
        "the statuses disagree with the reachability predicate on how many pairs exist"
    );
    assert!(
        driven > 0,
        "no reachable pair is driven by a fixture — every measurement in this file would be \
         about a path no client can reach"
    );
}

// =====================================================================
// 5. The ENUMERATION-CHECKLIST scan
// =====================================================================

/// Collects the `<name>` of every item in this file carrying a
/// `/// ENUMERATION: <name>` doc line. Syntax-aware (`syn::visit::Visit`)
/// rather than a regex, so a marker inside a string or a comment cannot
/// register and a rustfmt-wrapped item cannot hide.
#[derive(Default)]
struct MarkerScan {
    found: BTreeSet<String>,
}

impl MarkerScan {
    fn take(&mut self, attrs: &[syn::Attribute]) {
        for a in attrs {
            if !a.path().is_ident("doc") {
                continue;
            }
            let syn::Meta::NameValue(nv) = &a.meta else {
                continue;
            };
            let syn::Expr::Lit(lit) = &nv.value else {
                continue;
            };
            let syn::Lit::Str(s) = &lit.lit else {
                continue;
            };
            if let Some(rest) = s.value().trim().strip_prefix("ENUMERATION: ") {
                self.found.insert(rest.trim().to_string());
            }
        }
    }
}

impl<'ast> syn::visit::Visit<'ast> for MarkerScan {
    fn visit_item_const(&mut self, n: &'ast syn::ItemConst) {
        self.take(&n.attrs);
        syn::visit::visit_item_const(self, n);
    }
    fn visit_item_fn(&mut self, n: &'ast syn::ItemFn) {
        self.take(&n.attrs);
        syn::visit::visit_item_fn(self, n);
    }
    fn visit_item_static(&mut self, n: &'ast syn::ItemStatic) {
        self.take(&n.attrs);
        syn::visit::visit_item_static(self, n);
    }
    fn visit_item_enum(&mut self, n: &'ast syn::ItemEnum) {
        self.take(&n.attrs);
        syn::visit::visit_item_enum(self, n);
    }
    fn visit_item_struct(&mut self, n: &'ast syn::ItemStruct) {
        self.take(&n.attrs);
        syn::visit::visit_item_struct(self, n);
    }
    fn visit_item_type(&mut self, n: &'ast syn::ItemType) {
        self.take(&n.attrs);
        syn::visit::visit_item_type(self, n);
    }
}

fn own_source() -> String {
    std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/logql_leaf_retention_witness.rs"),
    )
    .expect("this file is readable")
}

/// The checklist's names, read out of the module doc's
/// `ENUMERATION-CHECKLIST` block. Each line is `- <name> | restated: … |
/// scoped: … | parallel: …`.
fn checklist_entries(src: &str) -> Vec<(String, String)> {
    let block = src
        .split("//! # ENUMERATION-CHECKLIST")
        .nth(1)
        .expect("the module doc still carries the ENUMERATION-CHECKLIST block");
    block
        .lines()
        .filter_map(|l| l.trim().strip_prefix("//! - "))
        .map(|l| {
            let name = l.split('|').next().unwrap_or("").trim().to_string();
            (name, l.to_string())
        })
        .collect()
}

/// **Set equality in BOTH directions** between the marked items and the
/// checklist lines. One direction alone leaves exactly the hole this
/// replaces: a marked enumeration with no line, or a line whose item is
/// gone.
///
/// **Boundary, restated where it is enforced:** the scan covers this
/// file's own declarations that carry the marker. An enumeration written
/// without the marker is invisible to it. That residual is accepted, and
/// it is why the checklist is a review aid rather than a proof.
#[test]
fn the_enumeration_checklist_covers_every_marked_item() {
    let src = own_source();
    let file = syn::parse_file(&src).expect("this file parses");
    let mut scan = MarkerScan::default();
    syn::visit::visit_file(&mut scan, &file);
    assert!(
        !scan.found.is_empty(),
        "no `/// ENUMERATION:` markers found — the scan is not reading this file"
    );

    let entries = checklist_entries(&src);
    let listed: BTreeSet<String> = entries.iter().map(|(n, _)| n.clone()).collect();
    assert_eq!(
        listed.len(),
        entries.len(),
        "the checklist lists a name twice"
    );

    let missing: Vec<&String> = scan.found.difference(&listed).collect();
    assert!(
        missing.is_empty(),
        "marked enumerations with no checklist line: {missing:?} — add one line per clause \
         (restated / scoped / parallel)"
    );
    // No exemption: set equality, both directions. A checklist line
    // whose marked item is gone fails naming the line, exactly as a
    // marked item with no line fails naming the item.
    let orphans: Vec<&String> = listed.difference(&scan.found).collect();
    assert!(
        orphans.is_empty(),
        "checklist lines whose marked item does not exist in this file: {orphans:?} — the \
         item was renamed or deleted and its line outlived it"
    );
    assert_eq!(
        listed, scan.found,
        "the marked enumerations and the checklist are not the same set"
    );
    for (name, line) in &entries {
        for key in ["restated:", "scoped:", "parallel:"] {
            assert!(
                line.contains(key),
                "checklist entry `{name}` does not answer `{key}` — all three clauses are \
                 required, because an enumeration that passed one is not thereby correct"
            );
        }
    }
}
