//! Issue #221: the `variants(...) of (...)` allocation gates (G0–G3, the
//! Φ finish bands) and the G4 frame census — the mechanised half of the
//! W-MEM charge-before-allocate audit.
//!
//! # The instrument (Δ8.1)
//!
//! Three counters, three roles, one validity control:
//!
//! | counter | update | role |
//! |---|---|---|
//! | `TOTAL_BYTES` | `+size` on alloc, `+new` on realloc | sees EVERY allocation, retained or dropped — catches large uncharged per-variant work |
//! | `ALLOC_CALLS` | `+1` on alloc and realloc | one count per allocation, NO byte-model slack — catches a SMALL per-variant transient a byte gate would absorb. **A count band catches any non-zero-sized heap allocation, whatever its size** — the 32-byte mutation demonstrator is a sufficient choice, not a detection floor (Δ10.0). |
//! | `LIVE`/`PEAK` | `+`/`−`, `fetch_max`; realloc charges old+new | the OOM-relevant high-water mark the cap exists to bound |
//!
//! The v6 retained-delta quantity (`live_after − live_before`) appears
//! nowhere as a decision gate: it is structurally blind to anything
//! allocated and dropped inside the window (G0 demonstrates this
//! in-test).
//!
//! # Coverage statement (what each gate can and cannot see)
//!
//! G1a/G1c/G1d–G1h and G2b–G2h are tight count bands with no byte slack
//! and catch a small per-variant/per-sub-state transient of ANY size.
//! G1b/G2/G3 carry the charge model's 2–6× slack and catch only LARGE
//! uncharged work. PEAK cannot see a sequential transient by
//! construction. The classes that carry no count band are the R4
//! residuals below. F-q (the pre-sized finish concat, including the
//! per-variant staging vector) is explicitly NOT gated: a push-growth
//! regression there costs ≈ log₂N ≤ 7 allocations over 63 variants,
//! inside the ±20 residue — stated so no reader over-reads the bands.
//!
//! # R4 — the named residuals (no itemized count band is possible)
//!
//! Derived from the branch inventory (the `INVENTORY` const), not from
//! the fixtures that were written. Exactly three per-variant branches
//! carry no itemized allocation-count band:
//!
//! - **(i) meta hydration** (`C-e`) — the `base_labels`(+`hashes`) insert
//!   loops run only for non-empty `meta`; their count includes
//!   hashbrown's growth schedule plus `series_labels`' per-stream
//!   allocations. Not first-party-derivable.
//! - **(ii) an arena entry for a DISTINCT non-empty tail** (`C-l`) —
//!   `CompiledPipeline::extended_with`, a `pipeline.rs`-internal count
//!   that for a regex-bearing tail includes the regex crate's
//!   compile-time allocations.
//! - **(iii) `apply_vector_aggs` for a variant that CARRIES a vector
//!   aggregation** (`F-w`) — hashbrown growth plus std's
//!   in-place-collect specialization make the count non-derivable.
//!   Narrowed by construction: the call is SKIPPED when
//!   `spec.vector_aggs()` is empty (F-p), and its input is bounded by
//!   `AggCaps::divided(n)`'s series cap.
//!
//! Compensating controls: (1) the retained terms of (i)/(ii) are
//! isolated per-term by I2 and I4/I5 (in `exec.rs`'s unit tests); (2)
//! G2/G3's byte and PEAK slopes catch a LARGE member of any of the
//! three; (3) this issue adds no statement inside (i) or (iii), and
//! every statement it adds inside (ii) is enumerated by the charge-site
//! census. **A member of R4 must be found by inspection.**
//!
//! **Maintenance rule (mechanised as G4):** any new statement
//! conditional on a variant's op or shape MUST be classified in the
//! `INVENTORY` (BAND / NIL / NOT-EXEC / UNREACH / R4) — a frame that
//! gains a branch or a call fails the census until the pin and the
//! inventory move in the same commit.
//!
//! # What the corrected instrument would have caught (the round-by-round
//! visibility table, Δ8.5/Δ9.3)
//!
//! | member | class | retained Δ | PEAK | TOTAL_BYTES | ALLOC_CALLS |
//! |---|---|---|---|---|---|
//! | `absent_labels` clone in the range ctor | retained/variant | yes | yes | yes | yes |
//! | driver buffers `slot`/`subs`/`sub_charged` | retained | yes | yes | yes | yes |
//! | `ClientAgg::{pipeline,absent_labels}` | retained (plan) | after v6 | yes | yes | yes |
//! | `Grouping::labels` | retained + realloc peak | half | yes | yes | yes |
//! | duplicate grouping/param clones | transient/variant | **no** | **no** | yes | yes |
//! | `format!("{op} parameter")` | transient, in-statement | **no** | **no** | marginal | **yes** |
//! | pure-twin `rows.to_vec()` | transient, large | no | no | **yes** | **yes** (1 + 1/row — `MetricScanRow` is `Clone` with an owned `body: String`) |
//! | `Result<Vec<_>>` push-growth | transient realloc | no | no | yes | yes |
//! | small sequential per-sub-state transient (first-party fixture) | transient | no | no | no (slack) | **yes** (G2b) |
//! | same, meta-hydration / arena-compile / applied-aggs path | transient | — | — | — | **no gate — R4** |
//!
//! # The W-MEM inventory (a RENDERING of the `INVENTORY` const — G4
//! # asserts byte-identity; edit the const and re-paste, never the doc)
//!
//! ## W_plan
//!
//! | id | what | frames | site | disp | covered by |
//! | P-a | 0 vs 1 vector layers | ::unwrap_vector_aggs_into, ::build_variants_node | plan.rs build_variants_node loop | BAND | G1d/G1e (0) vs G1a/G1c (1) |
//! | P-b | parameterized vs parameterless aggregation | ::parse_vector_agg_params | plan.rs parse_vector_agg_params | BAND | G1a (param) vs G1c (none) |
//! | P-c | sort-grouping + approx_topk-in-range rejections | ::parse_vector_agg_params | plan.rs parse_vector_agg_params | NIL | error paths abort the plan - B1 |
//! | P-d | grouping created / by-cloned / without-cloned | ::build_variants_node | plan.rs VariantSpec::try_new injection | BAND | G1a / G1c / G1g |
//! | P-e1 | BARE variant: unwrap tail empty vs non-empty | ::build_variants_node | plan.rs variant_tail + try_new clone | BAND | G1a/G1d/G1e vs G1c |
//! | P-e2 | WRAPPED variant: whole pipeline as the tail (no prefix compile) | ::build_variants_node | plan.rs build_variants_node bare/wrapped arm | BAND | G1i (F_wrap) |
//! | P-f | arity class success side (forbids vs requires unwrap) | ::build_variants_node | plan.rs build_variants_node arity gates | BAND | G1a vs G1c |
//! | P-g | ClientValue Count / Bytes / Unwrap | ::build_variants_node | plan.rs build_variants_node value arm | BAND | G1a / G1f / G1c |
//! | P-h | quantile parameter parse | ::build_variants_node | plan.rs build_variants_node quantile arm | BAND | G1h |
//! | P-i | absent vs non-absent absent_labels | ::build_variants_node | plan.rs VariantSpec::try_new absent arm | BAND | G1e vs every other plan fixture |
//! | P-j | parse_plan_number success path (format! only on Err) | ::parse_plan_number | plan.rs parse_plan_number | NIL | executes under G1a/G1h |
//! | P-k | the reused raw-handle buffer (one growth, intercept) | ::unwrap_vector_aggs_into | plan.rs unwrap_vector_aggs_into | BAND | G1a upper band; G1d pins the 0-layer shape at 0 |
//! | P-l | the variant offset shift (integer arithmetic only) | ::build_variants_node | plan.rs build_variants_node offset arm | NIL | executes under every G1 fixture |
//!
//! ## W_ctor
//!
//! | id | what | frames | site | disp | covered by |
//! | C-a | state kind Range vs Instant | VariantsAggState::new, ClientAggState::new, RangeSlideState::new | exec.rs VariantsAggState::new kind dispatch | BAND | G2b/G2c/G2f (range) vs G2d/G2e (instant) |
//! | C-b | is_absent gates present_cover (branch-free multiplier) | RangeSlideState::new | exec.rs RangeSlideState::new present_cover | BAND | G2c (range) / G2e (instant: costs nothing) |
//! | C-c | absent_labels.clone() empty vs populated | RangeSlideState::new | exec.rs RangeSlideState::new | BAND | G2b (empty) vs G2c (2 Eq matchers) |
//! | C-d | arena: empty tail shares entry 0 | VariantArena::build | exec.rs VariantArena::build | BAND | G2b |
//! | C-e | meta empty vs populated (base_labels/hashes loops) | ClientAggState::new, RangeSlideState::new | exec.rs constructors + series_labels | R4 (i) | I4/I5 charge isolation; G2/G3 slopes |
//! | C-f | op-derived scalars (reducer_class etc.) | RangeSlideState::new | exec.rs RangeSlideState::new | NIL | executes under G2b + G2c |
//! | C-g | ensure_grid_resolution (integer arithmetic) | ClientAggState::new, RangeSlideState::new | exec.rs constructors | NIL | executes under G2b |
//! | C-h | tail-slice dedup backward scan (no key materialized) | VariantArena::build | exec.rs VariantArena::build | NIL | G2f pins a dedup HIT at zero |
//! | C-i | sizing walks over borrowed AST (no temporary container) | ::variant_pipeline_entry_bytes, ::stage_source_bytes, ::regex_stage_count, ::variant_state_bytes | exec.rs sizing helpers | NIL | execute under every G2 fixture |
//! | C-j | driver buffer reservations (with_capacity, intercept) | VariantsAggState::new, VariantArena::build | exec.rs build/new preambles | NIL | a per-variant realloc would fail G2b |
//! | C-k | arena: shared non-empty tail (dedup hit) | VariantArena::build | exec.rs VariantArena::build | BAND | G2f |
//! | C-l | arena: DISTINCT non-empty tail (extended_with compile) | VariantArena::build | exec.rs VariantArena::build | R4 (ii) | I2 charge isolation; G3 peak slope |
//!
//! ## W_fin
//!
//! | id | what | frames | site | disp | covered by |
//! | F-a | MetricAggState::push_rows kind dispatch | MetricAggState::push_rows | exec.rs MetricAggState::push_rows | NIL | Phi1/Phi4 |
//! | F-b | ClientAggState::push_rows prologue (Vec::new scratch) | ClientAggState::push_rows, ClientAggState::push_rows_inner | exec.rs ClientAggState::push_rows | NIL | Phi4-Phi7 |
//! | F-c | RangeSlideState::push_rows prologue/epilogue (mem::take) | RangeSlideState::push_rows | exec.rs RangeSlideState::push_rows | NIL | Phi1-Phi3 |
//! | F-d | the row loops and everything reachable only from them | ClientAggState::push_rows, ClientAggState::push_rows_inner, ClientAggState::push_one_row, RangeSlideState::push_one_row, ClientAggState::stage, ClientAggState::stage_bytes, ClientAggState::flush_pending, RangeSlideState::push_rows, RangeSlideState::flush_collision, FpSlide::finish | exec.rs row paths | NOT-EXEC | rows.is_empty(); the existing CLIENT_AGG_FLAT_BUDGET per-row gate |
//! | F-e | MetricAggState::finish kind dispatch (Box move) | MetricAggState::finish | exec.rs MetricAggState::finish | NIL | Phi1/Phi4 |
//! | F-f | ClientAggState::finish absent vs non-absent | ClientAggState::finish, ClientAggState::finish_folded | exec.rs ClientAggState::finish | BAND | Phi6/Phi7 vs Phi4/Phi5 |
//! | F-g | absent: the finish-time absent_labels clone (1 + 2k) | ClientAggState::finish, ClientAggState::finish_folded | exec.rs ClientAggState::finish | BAND | Phi6/Phi7 (k = 2 gives 5) |
//! | F-h | absent instant: present empty vec![sample] vs empty Vector | ClientAggState::finish, ClientAggState::finish_folded | exec.rs ClientAggState::finish | BAND | Phi6/Phi7 (empty-present arm; no rows) |
//! | F-j | non-absent fan_out label_groups vs fp_groups collects | ClientAggState::finish, ClientAggState::finish_folded | exec.rs ClientAggState::finish | BAND | Phi5 / Phi4 (0 each: empty maps reserve nothing) |
//! | F-k | non-absent instant emit | ClientAggState::finish, ClientAggState::finish_folded | exec.rs ClientAggState::finish | BAND | Phi4/Phi5 (0) |
//! | F-l | RangeSlideState finish prologue (flush early-return, cur None) | RangeSlideState::finish, RangeSlideState::finish_in_place, RangeSlideState::flush_collision | exec.rs RangeSlideState::finish_in_place | NIL | Phi1-Phi3 |
//! | F-m | is_absent routes to finish_absent (points + vec![series]) | RangeSlideState::finish_in_place, RangeSlideState::finish_absent | exec.rs RangeSlideState::finish_absent | BAND | Phi3 |
//! | F-n | fan_out group loop vs series_out take | RangeSlideState::finish_in_place | exec.rs RangeSlideState::finish_in_place | BAND | Phi2 / Phi1 (0 each) |
//! | F-o | append_variant_label insert vs override arm; SKIPPED for an absent variant's synthetic series (adjudicated capture correction) | ::append_variant_label, VariantsAggState::finish_in_place | exec.rs append_variant_label + the absent gate | BAND | Phi3/Phi6/Phi7 pin the absent SKIP (no append); the insert/override arms are pinned by the hermetic goldens |
//! | F-p | vector_aggs empty: apply_vector_aggs SKIPPED | VariantsAggState::finish_in_place | exec.rs VariantsAggState::finish_in_place | BAND | Phi1-Phi7 (skip arm, 0) |
//! | F-q | pre-sized concat + per-variant staging vec (intercept) | VariantsAggState::finish_in_place | exec.rs VariantsAggState::finish_in_place | NIL | explicitly NOT gated: a push-growth regression is inside the residue |
//! | F-r | range-only drop of __variant__-less series; instant keeps | VariantsAggState::finish_in_place | exec.rs VariantsAggState::finish_in_place | NIL | Phi3 (keep) + the instant/range asymmetry golden |
//! | F-s | per-variant charge release (integer arithmetic) | VariantsAggState::finish_in_place | exec.rs VariantsAggState::finish_in_place | NIL | finish asserts charged == base |
//! | F-t | the forwarding loop hands each sub-state the SAME slice | VariantsAggState::push_rows | exec.rs VariantsAggState::push_rows | NIL | Phi1-Phi7; row-bearing work is F-d's NOT-EXEC |
//! | F-u | VariantsAggState::finish delegation + post-condition | VariantsAggState::finish | exec.rs VariantsAggState::finish | NIL | Phi1-Phi7 |
//! | F-w | apply_vector_aggs for an aggregation-bearing variant | VariantsAggState::finish_in_place | exec.rs apply_vector_aggs | R4 (iii) | input bounded by AggCaps::divided(n).series; G2/G3 slopes |
//! | F-z | instant sub-state narrowing refusal (issue #236 Part D) | VariantsAggState::new | exec.rs VariantsAggState::new as_instant/ok_or_else | UNREACH | a stepped window routes to the Range arm above, and the witness cannot be minted from one |
//! | F-y | one mutating group's cell drain (expanded / delta / sample sweep) | RangeSlideState::drain_group | exec.rs RangeSlideState::drain_group | NOT-EXEC | rows.is_empty() => no mutating group exists => never called (F-d's premise) |
//! | F-x | issue #236 Part B fold containers (dense slots, live map) | RangeSlideState::finish_in_place | exec.rs RangeSlideState::emit + VectorAggFold | NOT-EXEC | run_variants never calls attach_fold; fold is None here |
//! | F-y2 | offset put back on the emitted grid (in-place, both arms) | RangeSlideState::finish | client_agg.rs shift_emitted_points | NIL | offset-free variants take the early return; the shift mutates points in place |
//!
//! # G4 — what this gate does NOT catch (eight gaps)
//!
//! - **G-1 item-level attributes.** `#[cfg]`/attribute macros on a frame
//!   item sit outside the walked `Block`; `syn::parse_file` does not
//!   expand them, so adding/removing one does not move the census.
//! - **G-2 macro-generated items and module-level `include!`.** A
//!   frame-shaped function produced by expansion is not in the frame
//!   list at all.
//! - **G-3 unpinned callee bodies.** Every non-frame callee's body — the
//!   15 boundary entries and the ~70 ordinary names — is unpinned; the
//!   "every function this issue creates or edits is a frame" rule is a
//!   diff-review criterion, not a mechanism.
//! - **G-4 macro-token residual.** A call added inside the tokens of a
//!   macro whose body does not parse as a comma-separated expression
//!   list is caught only by the `ident(` fallback, which sees `f(` but
//!   not `f::<T>(`.
//! - **G-5 name-based callee identity.** Callees are
//!   last-path-segment/method names, so same-named functions are
//!   indistinguishable and "this callee is a frame" is a name match,
//!   never a resolved binding.
//! - **G-6 syntactic branch set.** Control flow through
//!   `map_or`/`unwrap_or_else`/`bool::then`/`&&` is not a branch (it does
//!   move the callee set if the name is new); `#[cfg]`-gated code inside
//!   a block is censused whether or not it compiles.
//! - **G-7 structural, not semantic.** The gate proves each frame has ≥1
//!   row; it does NOT prove that row's disposition is the right one.
//! - **G-8 callee occurrence multiplicity.** `census` returns a
//!   `BTreeSet<String>`, so the NUMBER of calls to a name is not pinned:
//!   a second `check_surviving_error(...)` beside the existing one in
//!   `ClientAggState::push_rows` (the other site is in a different
//!   frame, `RangeSlideState::push_rows`) changes neither its branch
//!   count nor its callee set, and so passes G4. Live in the code
//!   today: `RangeSlideState::finish_in_place` calls
//!   `discharge_retention` TWICE and contributes it to the pinned set
//!   once. Distinct from G-5 (a name denoting several bindings) and G-7
//!   (disposition correctness). NOT mechanised by decision: a multiset
//!   would make every pin churn on ordinary refactors, and a duplicate
//!   allocation-bearing call is already bounded by the Φ bands and
//!   `CLIENT_AGG_FLAT_BUDGET`, not by the census.
//!
//! Compensating controls for all eight: the Φ1–Φ7 bands with
//! `EXEC_FIN_*` = 0/5/9/7, the by-hand mutation set, G2/G3's byte and
//! PEAK slopes, and `CLIENT_AGG_FLAT_BUDGET`. A gap here is a stale
//! TABLE, not an unbounded allocation.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

use pulsus_logql::{Grouping, GroupingKind, VectorAggOp, parse};
use pulsus_read::logql::rows::{MetricScanRow, StreamMetaRow};
use pulsus_read::logql::{
    Direction, MAX_VARIANT_FANOUT_STATE_BYTES, MetricNode, MetricPlan, Plan, PlanCtx, QueryParams,
    QueryResult, QuerySpec, VariantArena, VariantSpec, VariantsAggState, plan, run_variants_rows,
};

static TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Issue #297: **thread-confined**, where it used to be a process-wide
    /// `AtomicBool`. A measured window asks a question about the work the
    /// measuring thread does inside it; while the flag was global the
    /// counters answered a different question — "what did the whole
    /// process allocate during that interval" — and the harness's own
    /// per-test machinery, running on other threads, landed inside the
    /// window. That is the observed `--test-threads=2` flake (a stray 48
    /// bytes in G0's window, ~1 run in 7).
    ///
    /// Everything a window measures (`plan`, the state constructors,
    /// `push_rows`/`finish`, `run_variants_rows`) runs synchronously on
    /// the calling thread, so confining the flag removes interference
    /// without removing a single first-party allocation.
    ///
    /// `const`-initialised and non-`Drop`, so reading it from inside the
    /// global allocator is a plain TLS load: no lazy initialisation, no
    /// destructor registration, and therefore no re-entry into the
    /// allocator. `try_with` covers the one remaining edge — a thread
    /// allocating during TLS teardown — by reporting "not measuring".
    static MEASURING: Cell<bool> = const { Cell::new(false) };

    /// A per-thread identity for the [`SERIAL`] ownership check, since
    /// `ThreadId` has no stable integer projection. Assigned lazily from
    /// [`NEXT_THREAD_KEY`]; never read from inside the allocator.
    static THREAD_KEY: Cell<u64> = const { Cell::new(0) };
}

fn measuring() -> bool {
    MEASURING.try_with(Cell::get).unwrap_or(false)
}

static NEXT_THREAD_KEY: AtomicU64 = AtomicU64::new(1);

fn thread_key() -> u64 {
    THREAD_KEY.with(|k| {
        let mut key = k.get();
        if key == 0 {
            key = NEXT_THREAD_KEY.fetch_add(1, Ordering::Relaxed);
            k.set(key);
        }
        key
    })
}

/// The four counters are still process-global (only the *window flag* is
/// thread-confined), so two threads measuring at once would interleave
/// their totals. Every test that opens a window holds this lock.
///
/// Issue #297 defect (2): holding it was a convention, and a convention a
/// new test can forget is not a guarantee. [`serialize`] is now the only
/// way to obtain a window ([`measured`] asserts the current thread owns
/// the lock), so a test added without it fails loudly instead of racing.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// The thread that currently holds [`SERIAL`], as a [`thread_key`]; `0`
/// when the lock is free.
static SERIAL_OWNER: AtomicU64 = AtomicU64::new(0);

/// RAII proof that the calling thread holds [`SERIAL`]. Poison is
/// deliberately ignored: a panicking test leaves no state behind that a
/// later one could misread — the counters are reset at every window open.
struct Serialized {
    _guard: std::sync::MutexGuard<'static, ()>,
}

fn serialize() -> Serialized {
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    SERIAL_OWNER.store(thread_key(), Ordering::SeqCst);
    Serialized { _guard: guard }
}

impl Drop for Serialized {
    fn drop(&mut self) {
        SERIAL_OWNER.store(0, Ordering::SeqCst);
    }
}

struct TripleCounterAlloc;

fn on_alloc(size: u64) {
    if measuring() {
        TOTAL_BYTES.fetch_add(size, Ordering::Relaxed);
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        let now = LIVE.fetch_add(size, Ordering::Relaxed) + size;
        PEAK.fetch_max(now, Ordering::Relaxed);
    }
}

fn on_dealloc(size: u64) {
    if measuring() {
        // Saturating: a pre-window allocation freed inside the window
        // would push `LIVE` negative; clamping at 0 only ever over-states
        // the peak — the safe direction for a `≤ charge` assertion.
        let mut cur = LIVE.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(size);
            match LIVE.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }
}

// SAFETY: delegates verbatim to the system allocator; the only side
// effects are relaxed atomic updates (gated by the `MEASURING` TLS flag,
// whose `const`-init non-`Drop` slot is read without allocating) which
// allocate nothing and cannot re-enter the allocator.
unsafe impl GlobalAlloc for TripleCounterAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        on_alloc(layout.size() as u64);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        on_dealloc(layout.size() as u64);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Old+new both live during a non-in-place realloc; TOTAL charges
        // the new request, CALLS counts one allocation event.
        on_alloc(new_size as u64);
        on_dealloc(layout.size() as u64);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: TripleCounterAlloc = TripleCounterAlloc;

/// One measured window: `(alloc_calls, total_bytes, peak)`.
///
/// Refuses to open a window unless the calling thread holds [`SERIAL`]
/// (via [`serialize`]) — issue #297's "a test added without the mutex
/// must fail rather than silently race".
fn measured<T>(f: impl FnOnce() -> T) -> (u64, u64, u64, T) {
    assert_eq!(
        SERIAL_OWNER.load(Ordering::SeqCst),
        thread_key(),
        "measured() opens a window over process-global counters: the calling test must hold \
         SERIAL. Start the test with `let _serial = serialize();`"
    );
    TOTAL_BYTES.store(0, Ordering::SeqCst);
    ALLOC_CALLS.store(0, Ordering::SeqCst);
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    MEASURING.set(true);
    let out = f();
    MEASURING.set(false);
    (
        ALLOC_CALLS.load(Ordering::SeqCst),
        TOTAL_BYTES.load(Ordering::SeqCst),
        PEAK.load(Ordering::SeqCst),
        out,
    )
}

// ---------------------------------------------------------------------
// Itemized per-variant allocation constants — DERIVED, never calibrated.
// The correction rule: a constant may change ONLY by adding or removing
// an item, each carrying its source line and either the charge term
// covering its bytes or its named-residue entry; a growth-realloc item
// must be reproducible by a standalone Vec measurement in this file.
// Widening `STRAY_ALLOC_RESIDUE`, one-siding a band, or absorbing a
// discrepancy into an existing item is forbidden.
// ---------------------------------------------------------------------

/// F_bare `count_over_time({app="a"}[5m])` — no vector layer (the empty
/// `Result` collect reserves nothing), no tail, no absent labels: ZERO
/// allocations per variant. The single most sensitive gate in the file.
const PLAN_ALLOCS_PER_VARIANT_BARE: u64 = 0;

/// F_min `topk(3, count_over_time({app="a"}[5m]))`:
/// - item 1: the `Result<Vec<VectorAggSpec>>` collect block, one layer
///   (`plan.rs::parse_vector_agg_params`; charge: the `grown_alloc_bytes`
///   vector_aggs term, C1) = 1
/// - item 2: the CREATED `Grouping::labels` buffer — `topk` carries no
///   grouping, so the injection creates `by (__variant__)` from nothing
///   (member M3; `plan.rs::VariantSpec::try_new`; charge: the
///   `grown_alloc_bytes(len + 1)` grouping term) = 1
/// - item 3: `VARIANT_LABEL.to_string()` (same site; charge: the
///   `alloc_block_bytes(VARIANT_LABEL.len())` term) = 1
/// - the empty tail `to_vec()` and empty `absent_labels` collect
///   allocate nothing; the raw handle buffer is reused (M5, intercept).
const PLAN_ALLOCS_PER_VARIANT: u64 = 3;

/// F_rich `sum by (app) (sum_over_time({app="a"} | unwrap dur | dur > 1
/// [5m]))`:
/// - item 1: the `Result<Vec<VectorAggSpec>>` collect block = 1
/// - item 2: `grouping.cloned()` — the labels buffer = 1
/// - item 3: `grouping.cloned()` — the `"app"` label `String` = 1
/// - item 4: the `__variant__` injection push realloc (cap 1 → 2;
///   reproduced by `growth_realloc_reproduction` below) = 1
/// - item 5: `VARIANT_LABEL.to_string()` = 1
/// - item 6: the 2-stage tail `to_vec()` buffer = 1
/// - items 7–9: the tail's owned strings — `Unwrap.label` ("dur"),
///   `Compare.name` ("dur"), `NumericLiteral::Number` ("1") = 3
///   (charge: the `stage_source_bytes × 130` clone factor)
const PLAN_ALLOCS_PER_VARIANT_RICH: u64 = 9;

/// F_wrap `sum by (app) (count_over_time({app="a"} |= "z" | logfmt
/// [5m]))` (issue #397) — the WRAPPED arm carrying a real pre-`unwrap`
/// prefix, so its tail is the WHOLE 2-stage pipeline rather than an
/// unwrap tail. It is the only plan fixture that charges the new "clone
/// the whole pipeline into the tail" term; without it that term rides in
/// no band at all:
/// - item 1: the `Result<Vec<VectorAggSpec>>` collect block, one layer
///   (`plan.rs::parse_vector_agg_params`) = 1
/// - item 2: `grouping.cloned()` — the labels buffer = 1
/// - item 3: `grouping.cloned()` — the `"app"` label `String` = 1
/// - item 4: the `__variant__` injection push realloc (cap 1 → 2) = 1
/// - item 5: `VARIANT_LABEL.to_string()` = 1
/// - item 6: the 2-stage tail `to_vec()` buffer — `[LineFilter, Parser]`,
///   which under the BARE rule would have been empty = 1
/// - item 7: the tail's one owned string, `LineFilter.value` (`"z"`);
///   the `logfmt` stage carries no expressions and the filter's
///   `or_matches` is empty, so neither allocates (charge: the
///   `stage_source_bytes × 130` clone factor, the term F_rich's items
///   7–9 ride) = 1
///
/// The discarded-prefix `compile` that F_rich's constant absorbs is
/// absent here by construction — the wrapped arm does not run it — so
/// this band also reddens if that compile is reinstated unconditionally.
const PLAN_ALLOCS_PER_VARIANT_WRAPPED: u64 = 7;

/// F_abs `absent_over_time({app="a", env="prod"}[5m])`:
/// - item 1: the `filter().map().collect()` absent-labels buffer (the
///   `Filter` adapter reports `size_hint().0 == 0` ⇒ one push-grown
///   block at 2 elements; `plan.rs::VariantSpec::try_new`; charge: the
///   absent-labels buffer term) = 1
/// - items 2–5: `m.name.clone()` + `m.value.clone()` × 2 Eq matchers
///   (charge: the absent-labels string terms, I9) = 4
const PLAN_ALLOCS_PER_VARIANT_ABSENT: u64 = 5;

/// W_ctor, non-absent kinds (range AND instant; shared-tail dedup hits
/// add 0): the boxed sub-state slot (`exec.rs::VariantsAggState::new`;
/// charge: the `alloc_block_bytes(size_of::<state>())` slot term) = 1.
const EXEC_CTOR_ALLOCS_PER_VARIANT: u64 = 1;

/// W_ctor, `absent_over_time` RANGE kind: the box (1) + `present_cover`
/// (`exec.rs::RangeSlideState::new`; charge: the present_cover term) (1)
/// + the `absent_labels` clone — one exact buffer + 2 `String`s × 2 Eq
///   matchers (charge: the absent-labels term, I6) (5) = 7. The INSTANT
///   absent kind stays at 1 (no clone, no cover — pinned by G2e).
const EXEC_CTOR_ALLOCS_PER_VARIANT_ABSENT: u64 = 7;

/// W_fin, non-absent finish, all four kind × fan-out shapes: ZERO
/// allocations per variant (empty maps collect to empty vectors; the
/// pre-sized concat and per-variant staging vector are per-query
/// intercepts — F-q, explicitly ungated).
const EXEC_FIN_ALLOCS_PER_VARIANT: u64 = 0; // Φ1, Φ2, Φ4, Φ5

/// W_fin, `absent_over_time` RANGE at a 1-point grid: `points` first
/// push (1) + `vec![MatrixSeries]` (1) = 2 — and NO append: the
/// adjudicated absent correction (issue #221, capture-governed) skips
/// `append_variant_label` for an absent variant's synthetic series, so
/// the plan's pinned 5 (which itemized a 3-allocation append) was
/// derived from the WRONG semantics and is re-derived here by
/// measurement. Deliberately not carried forward.
const EXEC_FIN_ALLOCS_PER_VARIANT_ABS_RANGE: u64 = 2; // Φ3 (was 5 pre-correction)

/// W_fin, `absent_over_time` INSTANT: the finish-time `absent_labels`
/// clone (1 buffer + 4 strings) + `vec![VectorSample]` (1) = 6 — and NO
/// append (see Φ3's note; the plan's pinned 9 itemized the 3-allocation
/// insert arm of an append the reference never performs).
const EXEC_FIN_ALLOCS_PER_VARIANT_ABS_INSTANT: u64 = 6; // Φ6 (was 9 pre-correction)

/// Φ7 — as Φ6 but the variant's own selector already carries a
/// `__variant__` Eq matcher: the selector-sourced label survives in the
/// synthetic series UNTOUCHED (no override ever runs for an absent
/// variant), so the count equals Φ6's. The append OVERRIDE arm — which
/// this fixture pinned pre-correction at 7 — now runs only for
/// non-absent variants and is gated by the common-pipeline-collision
/// hermetic golden instead of a Φ band.
const EXEC_FIN_ALLOCS_PER_VARIANT_ABS_OVERRIDE: u64 = 6; // Φ7 (was 7 pre-correction)

/// The two-sided band residue (`logql_pipeline_alloc.rs` precedent).
const STRAY_ALLOC_RESIDUE: u64 = 20;

// G0's retained-delta ceiling is derived in-test (`bytes / calls`, one
// transient's cost) rather than declared here: a constant would be a byte
// literal, and the standing rule wants the ceiling scale-free — see the
// comment at the assertion.

fn assert_band(slope: u64, per_variant: u64, n: u64, what: &str) {
    let expected = per_variant * (n - 1);
    let lo = expected.saturating_sub(STRAY_ALLOC_RESIDUE);
    let hi = expected + STRAY_ALLOC_RESIDUE;
    assert!(
        (lo..=hi).contains(&slope),
        "{what}: allocation-call slope {slope} outside the two-sided band \
         [{lo}, {hi}] (= {per_variant}/variant × {} ± {STRAY_ALLOC_RESIDUE})",
        n - 1
    );
}

// ---------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------

const NS: i64 = 1_000_000_000;

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

fn range_params() -> QueryParams {
    QueryParams {
        spec: QuerySpec::Range {
            start_ns: 0,
            end_ns: 60 * NS,
            step_ns: 60 * NS as u64 as i64 as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    }
}

/// A range whose grid holds exactly ONE point (`end == start`) — the Φ3
/// finish itemization's grid shape.
fn one_point_range_params() -> QueryParams {
    QueryParams {
        spec: QuerySpec::Range {
            start_ns: 60 * NS,
            end_ns: 60 * NS,
            step_ns: 60 * NS as u64 as i64 as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    }
}

fn instant_params() -> QueryParams {
    QueryParams {
        spec: QuerySpec::Instant { at_ns: 60 * NS },
        limit: 100,
        direction: Direction::Backward,
    }
}

fn n_variant_query(n: usize, variant: &str, common: &str) -> String {
    format!("variants({}) of ({common})", vec![variant; n].join(", "))
}

fn plan_variants(query: &str, params: &QueryParams) -> (MetricPlan, Vec<VariantSpec>, u64) {
    let expr = parse(query).expect("parse");
    // Issue #272: `impl Drop for MetricNode` forbids moving out of a
    // field (E0509), so this re-binds through a reference and takes the
    // owned pieces out of the borrow. It sits OUTSIDE `count_plan`'s and
    // `count_ctor`'s measured windows, so no band's quantity moves.
    let mut planned = plan(&expr, params, &ctx()).expect("plan");
    match &mut planned {
        Plan::MetricBinary(MetricNode::Variants {
            scan,
            variants,
            spec_bytes,
        }) => ((**scan).clone(), std::mem::take(variants), *spec_bytes),
        other => panic!("expected a variants plan, got {other:?}"),
    }
}

/// W_plan: one `plan()` call inside the window (parse stays OUTSIDE — the
/// AST is linear in request text and carries no N amplification).
fn count_plan(query: &str, params: &QueryParams) -> (u64, u64, u64) {
    let expr = parse(query).expect("parse");
    let params = *params;
    let c = ctx();
    let (calls, bytes, peak, out) = measured(|| plan(&expr, &params, &c));
    let planned = out.expect("plan");
    drop(planned);
    (calls, bytes, peak)
}

struct ExecFixture {
    scan: MetricPlan,
    variants: Vec<VariantSpec>,
    spec_bytes: u64,
    meta: HashMap<u64, StreamMetaRow>,
}

fn exec_fixture(query: &str, params: &QueryParams, meta_streams: u64) -> ExecFixture {
    let (scan, variants, spec_bytes) = plan_variants(query, params);
    let meta: HashMap<u64, StreamMetaRow> = (0..meta_streams)
        .map(|i| {
            (
                i + 1,
                StreamMetaRow {
                    fingerprint: i + 1,
                    service: format!("svc{i}"),
                    labels: format!(r#"{{"env":"prod","idx":"{i}"}}"#),
                },
            )
        })
        .collect();
    ExecFixture {
        scan,
        variants,
        spec_bytes,
        meta,
    }
}

/// W_ctor: `VariantArena::build` + `VariantsAggState::new`.
fn count_ctor(f: &ExecFixture) -> (u64, u64, u64, u64) {
    let common = &f.scan.client.as_ref().expect("client scan").pipeline;
    let (calls, bytes, peak, charged) = measured(|| {
        let arena = VariantArena::build(
            common,
            &f.variants,
            MAX_VARIANT_FANOUT_STATE_BYTES,
            f.spec_bytes,
        )
        .expect("arena");
        let st =
            VariantsAggState::new(&arena, &f.variants, &f.meta, MAX_VARIANT_FANOUT_STATE_BYTES)
                .expect("state");
        st.charged_bytes()
    });
    (calls, bytes, peak, charged)
}

/// W_fin: `push_rows(&[])` + `finish()` (row-bearing work is NOT-EXEC
/// here and delegated to the existing per-row gate — F-d/F-t).
fn count_fin(f: &ExecFixture) -> (u64, u64, u64) {
    let common = &f.scan.client.as_ref().expect("client scan").pipeline;
    let arena = VariantArena::build(
        common,
        &f.variants,
        MAX_VARIANT_FANOUT_STATE_BYTES,
        f.spec_bytes,
    )
    .expect("arena");
    let mut st =
        VariantsAggState::new(&arena, &f.variants, &f.meta, MAX_VARIANT_FANOUT_STATE_BYTES)
            .expect("state");
    let (calls, bytes, peak, out) = measured(|| {
        st.push_rows(&[]).expect("empty push");
        st.finish().expect("finish")
    });
    drop(out);
    (calls, bytes, peak)
}

// ---------------------------------------------------------------------
// The single measurement test (one #[test]: the counters are
// process-global and the windows must not interleave).
// ---------------------------------------------------------------------

#[test]
fn variants_allocation_gates() {
    let _serial = serialize();
    // --- G0: instrument-validity control (direction-neutral). The new
    // instrument SEES a dropped transient; the v6 retained quantity
    // cannot.
    let n_ctl: u64 = 64;
    // The ceiling below is the heap cost of ONE transient, taken from a
    // probe built OUTSIDE the window (issue #320 review round 2, finding
    // 2). The previous attempt divided the window's own totals — a MEAN,
    // which the in-test `calls` guard bounds only from below, so a single
    // dropped 128-byte stray inside the window raised the divisor enough
    // (1408/65 = 21) to let a retained 20-byte transient through. Derived
    // from the fixture instead, nothing that happens inside the window can
    // move it. `{i:04}` is fixed-width, so every transient in the loop
    // allocates exactly what this probe does.
    let per_transient = {
        let probe = format!("transient-{:04}", 0);
        probe.capacity() as u64
    };
    assert!(
        per_transient > 0,
        "G0: the control transient must be heap-allocated"
    );
    let live_at_open = LIVE.load(Ordering::SeqCst);
    let (calls, bytes, _, _) = measured(|| {
        for i in 0..n_ctl {
            let s = format!("transient-{i:04}");
            std::hint::black_box(&s);
            drop(s);
        }
    });
    let live_at_close = LIVE.load(Ordering::SeqCst);
    assert!(
        calls >= n_ctl - STRAY_ALLOC_RESIDUE,
        "G0: ALLOC_CALLS must see {n_ctl} dropped transients, saw {calls}"
    );
    assert!(bytes > 0, "G0: TOTAL_BYTES must see dropped transients");
    // Issue #297: a CEILING, never exact equality against a process-global
    // counter. What G0 claims is that the retained delta cannot SEE the
    // transients while TOTAL_BYTES/ALLOC_CALLS see all of them.
    //
    // `per_transient` is scale-free (it is the `String` buffer the fixture
    // itself allocates, never a byte literal, so it carries no
    // pointer-width or allocator assumption) and window-independent, so
    // retaining a SINGLE one of the 64 lands the delta exactly ON the
    // ceiling and reddens, whatever else the window allocates.
    //
    // The interference the ceiling used to have to absorb is gone at the
    // source anyway — `MEASURING` is thread-confined (see its
    // declaration), so this delta measures only this thread's window and
    // observes 0.
    let retained = live_at_close.saturating_sub(live_at_open);
    assert!(
        retained < per_transient,
        "G0: the retained-delta quantity must stay blind to {calls} allocations totalling \
         {bytes} bytes — it observed {retained}, at or above the cost of a single transient \
         ({per_transient} bytes, taken from the fixture, not from this window). Either the \
         window now retains what it allocates, or the counters are no longer window-confined."
    );

    // --- G1-proof: every plan fixture is ADMITTED and provably executed
    // its per-variant success path (the sole-producer argument:
    // `parse_vector_agg_params` is the only `VectorAggSpec` producer, and
    // a `Some(3.0)` param can only come from its `(true, Some(raw))` arm
    // — F_min's proof therefore pins `plan.rs`'s parameter statement ran
    // N times; F_abs's sorted absent labels are producible only by the
    // `AbsentOverTime` planner arm).
    let f_min = |n: usize| {
        n_variant_query(
            n,
            r#"topk(3, count_over_time({app="a"}[5m]))"#,
            r#"{app="a"} | logfmt [5m]"#,
        )
    };
    let f_rich = |n: usize| {
        n_variant_query(
            n,
            r#"sum by (app) (sum_over_time({app="a"} | unwrap dur | dur > 1 [5m]))"#,
            r#"{app="a"} | logfmt [5m]"#,
        )
    };
    let f_bare = |n: usize| {
        n_variant_query(
            n,
            r#"count_over_time({app="a"}[5m])"#,
            r#"{app="a"} | logfmt [5m]"#,
        )
    };
    let f_abs = |n: usize| {
        n_variant_query(
            n,
            r#"absent_over_time({app="a", env="prod"}[5m])"#,
            r#"{app="a"} | logfmt [5m]"#,
        )
    };
    let f_bytes = |n: usize| {
        n_variant_query(
            n,
            r#"topk(3, bytes_over_time({app="a"}[5m]))"#,
            r#"{app="a"} | logfmt [5m]"#,
        )
    };
    let f_without = |n: usize| {
        n_variant_query(
            n,
            r#"sum without (env) (sum_over_time({app="a"} | unwrap dur | dur > 1 [5m]))"#,
            r#"{app="a"} | logfmt [5m]"#,
        )
    };
    let f_quant = |n: usize| {
        n_variant_query(
            n,
            r#"sum by (app) (quantile_over_time(0.9, {app="a"} | unwrap dur | dur > 1 [5m]))"#,
            r#"{app="a"} | logfmt [5m]"#,
        )
    };
    // Issue #397: a WRAPPED variant with a real pre-`unwrap` prefix —
    // the one shape whose tail is the WHOLE pipeline. Every other plan
    // fixture here is either bare or wrapped-with-nothing-before-the-
    // `unwrap`, so without this one the new "clone the whole pipeline
    // into the tail" term rides in no band at all.
    let f_wrap = |n: usize| {
        n_variant_query(
            n,
            r#"sum by (app) (count_over_time({app="a"} |= "z" | logfmt [5m]))"#,
            r#"{app="a"} | logfmt [5m]"#,
        )
    };
    for n in [1usize, 8, 64] {
        let (_, variants, _) = plan_variants(&f_min(n), &range_params());
        assert_eq!(variants.len(), n);
        for spec in &variants {
            assert_eq!(
                spec.vector_aggs(),
                &[(
                    VectorAggOp::Topk,
                    Some(Grouping {
                        kind: GroupingKind::By,
                        labels: vec!["__variant__".to_string()],
                    }),
                    Some(3.0),
                )],
                "F_min proof: the parameterized arm ran for every variant"
            );
        }
        let (_, variants, _) = plan_variants(&f_rich(n), &range_params());
        for spec in &variants {
            assert_eq!(
                spec.vector_aggs(),
                &[(
                    VectorAggOp::Sum,
                    Some(Grouping {
                        kind: GroupingKind::By,
                        labels: vec!["__variant__".to_string(), "app".to_string()],
                    }),
                    None,
                )]
            );
        }
        let (_, variants, _) = plan_variants(&f_bare(n), &range_params());
        for spec in &variants {
            assert!(spec.vector_aggs().is_empty());
            assert!(spec.client().pipeline.is_empty());
            assert!(spec.client().absent_labels.is_empty());
        }
        let (_, variants, _) = plan_variants(&f_abs(n), &range_params());
        for spec in &variants {
            assert_eq!(
                spec.client().absent_labels,
                vec![
                    ("app".to_string(), "a".to_string()),
                    ("env".to_string(), "prod".to_string()),
                ],
                "F_abs proof: the absent-only planner arm ran"
            );
        }
    }

    // --- G1a/G1c/G1d/G1e/G1f/G1g/G1h: W_plan allocation-count bands
    // (two-sided, zero allowance beyond the fixed residue).
    let plan_slope = |q1: &str, q64: &str| {
        let (c1, ..) = count_plan(q1, &range_params());
        let (c64, ..) = count_plan(q64, &range_params());
        c64 - c1
    };
    assert_band(
        plan_slope(&f_min(1), &f_min(64)),
        PLAN_ALLOCS_PER_VARIANT,
        64,
        "G1a (F_min)",
    );
    assert_band(
        plan_slope(&f_rich(1), &f_rich(64)),
        PLAN_ALLOCS_PER_VARIANT_RICH,
        64,
        "G1c (F_rich)",
    );
    assert_band(
        plan_slope(&f_bare(1), &f_bare(64)),
        PLAN_ALLOCS_PER_VARIANT_BARE,
        64,
        "G1d (F_bare)",
    );
    assert_band(
        plan_slope(&f_abs(1), &f_abs(64)),
        PLAN_ALLOCS_PER_VARIANT_ABSENT,
        64,
        "G1e (F_abs)",
    );
    // The three siblings deliberately reuse their base constants: if a
    // future change makes the Bytes / `without` / quantile arm allocate,
    // the sibling band fails while its base passes — the intended signal.
    assert_band(
        plan_slope(&f_bytes(1), &f_bytes(64)),
        PLAN_ALLOCS_PER_VARIANT,
        64,
        "G1f (F_bytes = F_min's constant)",
    );
    assert_band(
        plan_slope(&f_without(1), &f_without(64)),
        PLAN_ALLOCS_PER_VARIANT_RICH,
        64,
        "G1g (F_without = F_rich's constant)",
    );
    assert_band(
        plan_slope(&f_quant(1), &f_quant(64)),
        PLAN_ALLOCS_PER_VARIANT_RICH,
        64,
        "G1h (F_quant = F_rich's constant)",
    );
    assert_band(
        plan_slope(&f_wrap(1), &f_wrap(64)),
        PLAN_ALLOCS_PER_VARIANT_WRAPPED,
        64,
        "G1i (F_wrap)",
    );

    // --- G1b: W_plan byte slope ≤ the spec charge slope (the soundness
    // half — 2–6× model slack, catches only LARGE uncharged work).
    //
    // Issue #397 runs it over F_wrap as well as F_rich: the wrapped arm
    // hands `try_new` a LONGER tail slice to clone, and `variant_spec_bytes`
    // is slice-driven, so the charge is expected to keep up with the
    // clone. Expected, not assumed — hence the second fixture.
    let byte_slope_soundness = |query: &dyn Fn(usize) -> String, what: &str| {
        let expr1 = parse(&query(1)).expect("parse");
        let expr64 = parse(&query(64)).expect("parse");
        let params = range_params();
        let c = ctx();
        let (_, bytes1, _, out1) = measured(|| plan(&expr1, &params, &c));
        let sb1 = match out1.expect("plan") {
            Plan::MetricBinary(MetricNode::Variants { spec_bytes, .. }) => spec_bytes,
            _ => unreachable!(),
        };
        let (_, bytes64, _, out64) = measured(|| plan(&expr64, &params, &c));
        let sb64 = match out64.expect("plan") {
            Plan::MetricBinary(MetricNode::Variants { spec_bytes, .. }) => spec_bytes,
            _ => unreachable!(),
        };
        assert!(
            bytes64.saturating_sub(bytes1) <= sb64 - sb1,
            "{what}: plan-time byte slope {} exceeds the charged spec slope {}",
            bytes64.saturating_sub(bytes1),
            sb64 - sb1
        );
    };
    byte_slope_soundness(&f_rich, "G1b (F_rich)");
    byte_slope_soundness(&f_wrap, "G1b (F_wrap)");

    // --- W_ctor count bands: G2b (range min), G2c (range absent),
    // G2d (instant min), G2e (instant absent), G2f (shared non-empty
    // tail — a dedup HIT must allocate zero; the one `extended_with`
    // sits in the N = 1 intercept).
    let ctor_slope = |q1: &str, q64: &str, params: &QueryParams| {
        let f1 = exec_fixture(q1, params, 0);
        let f64x = exec_fixture(q64, params, 0);
        let (c1, ..) = count_ctor(&f1);
        let (c64, ..) = count_ctor(&f64x);
        c64 - c1
    };
    assert_band(
        ctor_slope(&f_bare(1), &f_bare(64), &range_params()),
        EXEC_CTOR_ALLOCS_PER_VARIANT,
        64,
        "G2b (F_exec_min)",
    );
    assert_band(
        ctor_slope(&f_abs(1), &f_abs(64), &range_params()),
        EXEC_CTOR_ALLOCS_PER_VARIANT_ABSENT,
        64,
        "G2c (F_exec_absent)",
    );
    assert_band(
        ctor_slope(&f_bare(1), &f_bare(64), &instant_params()),
        EXEC_CTOR_ALLOCS_PER_VARIANT,
        64,
        "G2d (F_exec_inst)",
    );
    assert_band(
        ctor_slope(&f_abs(1), &f_abs(64), &instant_params()),
        EXEC_CTOR_ALLOCS_PER_VARIANT,
        64,
        "G2e (F_exec_inst_abs: the instant kind has no absent clone and \
         no present_cover — this band pins C-b/E-b in the instant kind)",
    );
    let f_tail = |n: usize| {
        n_variant_query(
            n,
            r#"sum_over_time({app="a"} | unwrap dur [5m])"#,
            r#"{app="a"} | logfmt [5m]"#,
        )
    };
    assert_band(
        ctor_slope(&f_tail(1), &f_tail(64), &range_params()),
        EXEC_CTOR_ALLOCS_PER_VARIANT,
        64,
        "G2f (F_exec_tail: identical tails share ONE arena entry)",
    );

    // --- G2: W_exec byte slope ≤ charged slope (soundness).
    {
        let charged_and_bytes = |q: &str| {
            let f = exec_fixture(q, &range_params(), 0);
            let common = &f.scan.client.as_ref().expect("client scan").pipeline;
            let (_, bytes, _, charged) = measured(|| {
                let arena = VariantArena::build(
                    common,
                    &f.variants,
                    MAX_VARIANT_FANOUT_STATE_BYTES,
                    f.spec_bytes,
                )
                .expect("arena");
                let mut st = VariantsAggState::new(
                    &arena,
                    &f.variants,
                    &f.meta,
                    MAX_VARIANT_FANOUT_STATE_BYTES,
                )
                .expect("state");
                st.push_rows(&[]).expect("push");
                let charged = st.charged_bytes();
                let out = st.finish().expect("finish");
                drop(out);
                charged
            });
            (bytes, charged)
        };
        for q in [(f_bare(1), f_bare(64)), (f_abs(1), f_abs(64))] {
            let (b1, ch1) = charged_and_bytes(&q.0);
            let (b64, ch64) = charged_and_bytes(&q.1);
            assert!(
                b64.saturating_sub(b1) <= ch64 - ch1,
                "G2: exec byte slope {} exceeds the charged slope {}",
                b64.saturating_sub(b1),
                ch64 - ch1
            );
        }
    }

    // --- W_fin count bands Φ1–Φ7 (N ∈ {1, 64}).
    let fin_slope = |mk: &dyn Fn(usize) -> String, params: &QueryParams| {
        let f1 = exec_fixture(&mk(1), params, 0);
        let f64x = exec_fixture(&mk(64), params, 0);
        let (c1, ..) = count_fin(&f1);
        let (c64, ..) = count_fin(&f64x);
        c64 - c1
    };
    assert_band(
        fin_slope(&f_bare, &range_params()),
        EXEC_FIN_ALLOCS_PER_VARIANT,
        64,
        "Φ1 (F_fin_min)",
    );
    assert_band(
        fin_slope(&f_tail, &range_params()),
        EXEC_FIN_ALLOCS_PER_VARIANT,
        64,
        "Φ2 (F_fin_fanout)",
    );
    assert_band(
        fin_slope(&f_abs, &one_point_range_params()),
        EXEC_FIN_ALLOCS_PER_VARIANT_ABS_RANGE,
        64,
        "Φ3 (F_fin_abs_rng, 1-point grid)",
    );
    assert_band(
        fin_slope(&f_bare, &instant_params()),
        EXEC_FIN_ALLOCS_PER_VARIANT,
        64,
        "Φ4 (F_fin_inst)",
    );
    assert_band(
        fin_slope(&f_tail, &instant_params()),
        EXEC_FIN_ALLOCS_PER_VARIANT,
        64,
        "Φ5 (F_fin_inst_fanout)",
    );
    assert_band(
        fin_slope(&f_abs, &instant_params()),
        EXEC_FIN_ALLOCS_PER_VARIANT_ABS_INSTANT,
        64,
        "Φ6 (F_fin_inst_abs)",
    );
    let f_abs_override = |n: usize| {
        n_variant_query(
            n,
            r#"absent_over_time({__variant__="z", app="a"}[5m])"#,
            r#"{app="a"} | logfmt [5m]"#,
        )
    };
    assert_band(
        fin_slope(&f_abs_override, &instant_params()),
        EXEC_FIN_ALLOCS_PER_VARIANT_ABS_OVERRIDE,
        64,
        "Φ7 (F_fin_inst_abs_ov: the append OVERRIDE arm)",
    );

    // --- G3: PEAK slope ≤ charged slope over regex tails + K = 8 meta
    // streams (the shapes whose windows contain non-first-party counts —
    // no count band here: R4 (i)/(ii)) + the unordered fat-body case.
    {
        let f_regex = |n: usize| {
            // N DISTINCT regex-bearing tails (the label name varies), so
            // every variant charges an arena entry.
            let variants = (0..n)
                .map(|i| format!(r#"sum_over_time({{app="a"}} | unwrap dur | l{i} =~ "a.*" [5m])"#))
                .collect::<Vec<_>>()
                .join(", ");
            format!(r#"variants({variants}) of ({{app="a"}} | logfmt [5m])"#)
        };
        let peak_and_charged = |q: &str| {
            let f = exec_fixture(q, &range_params(), 8);
            let common = &f.scan.client.as_ref().expect("client scan").pipeline;
            let (_, _, peak, charged) = measured(|| {
                let arena = VariantArena::build(
                    common,
                    &f.variants,
                    MAX_VARIANT_FANOUT_STATE_BYTES,
                    f.spec_bytes,
                )
                .expect("arena");
                let mut st = VariantsAggState::new(
                    &arena,
                    &f.variants,
                    &f.meta,
                    MAX_VARIANT_FANOUT_STATE_BYTES,
                )
                .expect("state");
                st.push_rows(&[]).expect("push");
                let charged = st.charged_bytes();
                let out = st.finish().expect("finish");
                drop(out);
                charged
            });
            (peak, charged)
        };
        let (p1, ch1) = peak_and_charged(&f_regex(1));
        let (p8, ch8) = peak_and_charged(&f_regex(8));
        assert!(
            p8.saturating_sub(p1) <= ch8 - ch1,
            "G3: peak slope {} exceeds the charged slope {}",
            p8.saturating_sub(p1),
            ch8 - ch1
        );

        // The unordered fat-body case (AC 33): `run_variants_rows` sorts
        // ONCE into at most one local Vec — its TOTAL_BYTES cost is
        // N-independent (a per-sub-state re-sort would clone every body
        // N times), and the output is identical shuffled vs pre-sorted.
        let f2 = exec_fixture(&f_bare(2), &range_params(), 2);
        let f8 = exec_fixture(&f_bare(8), &range_params(), 2);
        let fat = "x".repeat(64 * 1024);
        let mk_rows = |shuffled: bool| -> Vec<MetricScanRow> {
            let mut rows: Vec<MetricScanRow> = (0..32)
                .map(|i| MetricScanRow {
                    fingerprint: 1 + (i % 2),
                    timestamp_ns: (i as i64 % 50) * NS,
                    body: fat.clone(),
                    structured_metadata: String::new(),
                })
                .collect();
            if shuffled {
                rows.reverse();
            }
            rows
        };
        let common2 = f2.scan.client.as_ref().expect("client").pipeline.clone();
        let common8 = f8.scan.client.as_ref().expect("client").pipeline.clone();
        let rows = mk_rows(true);
        let (_, bytes2, _, out2) =
            measured(|| run_variants_rows(&rows, &f2.meta, &common2, &f2.variants).expect("run"));
        let (_, bytes8, _, out8) =
            measured(|| run_variants_rows(&rows, &f8.meta, &common8, &f8.variants).expect("run"));
        let body_bytes = 32 * fat.len() as u64;
        assert!(
            bytes8.saturating_sub(bytes2) < body_bytes,
            "fat-body: the sort-once contract — the byte slope {} must not \
             grow by a body-clone per extra sub-state (bodies = {body_bytes})",
            bytes8.saturating_sub(bytes2)
        );
        let sorted_out =
            run_variants_rows(&mk_rows(false), &f8.meta, &common8, &f8.variants).expect("run");
        // Series order inside a fan-out result is map-iteration order
        // (the server encoder label-sorts on the wire) — canonicalize
        // before comparing.
        type CanonSeries = Vec<(Vec<(String, String)>, Vec<(i64, f64)>)>;
        let canon = |r: QueryResult| -> CanonSeries {
            let QueryResult::Matrix(items) = r else {
                panic!("expected a matrix");
            };
            let mut out: Vec<_> = items.into_iter().map(|s| (s.labels, s.points)).collect();
            out.sort_by(|a, b| a.0.cmp(&b.0));
            out
        };
        assert_eq!(
            canon(out8),
            canon(sorted_out),
            "shuffled and pre-sorted fixtures agree"
        );
        drop(out2);
    }

    // --- The growth-realloc sub-rule reproductions: every itemized
    // realloc names its container and is reproduced standalone.
    {
        // F_rich item 4 / Φ3+Φ6 append item: a `Vec` at capacity == len
        // grows exactly once on the next push/insert.
        let mut v: Vec<String> = Vec::with_capacity(1);
        v.push("app".to_string());
        let (calls, ..) = measured(|| v.push("__variant__".to_string()));
        assert_eq!(
            calls, 2,
            "one String + one growth realloc — the itemized push-realloc"
        );
        let mut labels: Vec<(String, String)> = vec![
            ("app".to_string(), "a".to_string()),
            ("env".to_string(), "prod".to_string()),
        ];
        labels.shrink_to_fit();
        let (calls, ..) = measured(|| {
            labels.insert(0, ("__variant__".to_string(), "0".to_string()));
        });
        assert_eq!(calls, 3, "two Strings + one insert growth realloc");
    }
}

// ---------------------------------------------------------------------
// G4 — the frame census (syntax-aware, `syn`-based: a token rule cannot
// see generic calls or macro bodies) + the machine-readable inventory.
// ---------------------------------------------------------------------

/// The five W-MEM dispositions. `R4(i|ii|iii)` carries its residual
/// index so the R4 rows and the module-doc prose cannot drift apart.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Disp {
    Band,
    Nil,
    NotExec,
    Unreach,
    R4(u8),
}

impl Disp {
    fn render(self) -> String {
        match self {
            Disp::Band => "BAND".to_string(),
            Disp::Nil => "NIL".to_string(),
            Disp::NotExec => "NOT-EXEC".to_string(),
            Disp::Unreach => "UNREACH".to_string(),
            Disp::R4(i) => format!("R4 ({})", ["i", "ii", "iii"][(i - 1) as usize]),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Win {
    Plan,
    Ctor,
    Fin,
}

/// One per-variant frame: a function invoked once or more per element of
/// `variants`/`subs` inside one of the three measured windows. One entry
/// per FUNCTION (a `Frame` has one anchor).
struct Frame {
    file: &'static str,
    ty: Option<&'static str>,
    anchor: &'static str,
    branches: u32,
    callees: &'static [&'static str],
}

fn frame_key(f: &Frame) -> String {
    match f.ty {
        Some(ty) => format!("{ty}::{}", f.anchor),
        None => format!("::{}", f.anchor),
    }
}

/// One inventory row: every op/shape-conditional per-variant branch,
/// classified with exactly ONE disposition.
struct Row {
    id: &'static str,
    window: Win,
    what: &'static str,
    frames: &'static [&'static str],
    site: &'static str,
    disp: Disp,
    covered_by: &'static str,
}

/// A per-variant callee whose BODY carries a row's DELEGATED claim
/// (NOT-EXEC / UNREACH / R4). Must be pre-existing and untouched by this
/// issue — every function it creates or edits is a frame (a diff-review
/// criterion, gap G-3, not a mechanism). The other ~70 ordinary callee
/// names are classified in bulk by the row(s) covering the calling
/// frame: the gate's teeth for them are the pinned per-frame callee
/// SETS — a call to a name not already in that frame's set fails the
/// pin, as does removing the LAST call of a name, or renaming a call to
/// a name not already present. A DUPLICATE call to a name the frame
/// already calls does NOT fail the pin — the census pins a `BTreeSet`,
/// so occurrence counts are discarded (gap G-8). Plus
/// `BOUNDARY_CALLEES.len() == 15`, so a new delegating entry cannot be
/// added silently.
struct Boundary {
    callee: &'static str,
    rows: &'static [&'static str],
    disp: Disp,
}

/// The Δ12.3 visitor: `branches` = `if` + `match` + `while` + `for` +
/// `loop` + `?` + let-else + any `matches!` invocation; `callees` = a
/// `BTreeSet<String>` of last-path-segment call names (`f`), `.method`
/// names, `name!` macros and `<dyn-call>`; macro bodies are re-parsed as
/// comma-separated expressions and visited on success, with an
/// `ident?`-marked token fallback otherwise.
mod census {
    use std::collections::BTreeSet;
    use syn::visit::Visit;

    #[derive(Default)]
    pub struct Census {
        pub branches: u32,
        pub callees: BTreeSet<String>,
    }

    impl Visit<'_> for Census {
        fn visit_expr_if(&mut self, node: &syn::ExprIf) {
            self.branches += 1;
            syn::visit::visit_expr_if(self, node);
        }
        fn visit_expr_match(&mut self, node: &syn::ExprMatch) {
            self.branches += 1;
            syn::visit::visit_expr_match(self, node);
        }
        fn visit_expr_while(&mut self, node: &syn::ExprWhile) {
            self.branches += 1;
            syn::visit::visit_expr_while(self, node);
        }
        fn visit_expr_for_loop(&mut self, node: &syn::ExprForLoop) {
            self.branches += 1;
            syn::visit::visit_expr_for_loop(self, node);
        }
        fn visit_expr_loop(&mut self, node: &syn::ExprLoop) {
            self.branches += 1;
            syn::visit::visit_expr_loop(self, node);
        }
        fn visit_expr_try(&mut self, node: &syn::ExprTry) {
            self.branches += 1;
            syn::visit::visit_expr_try(self, node);
        }
        fn visit_local(&mut self, node: &syn::Local) {
            if node.init.as_ref().is_some_and(|i| i.diverge.is_some()) {
                self.branches += 1; // let-else
            }
            syn::visit::visit_local(self, node);
        }
        fn visit_expr_call(&mut self, node: &syn::ExprCall) {
            match &*node.func {
                syn::Expr::Path(p) => {
                    if let Some(seg) = p.path.segments.last() {
                        self.callees.insert(seg.ident.to_string());
                    }
                }
                _ => {
                    self.callees.insert("<dyn-call>".to_string());
                }
            }
            syn::visit::visit_expr_call(self, node);
        }
        fn visit_expr_method_call(&mut self, node: &syn::ExprMethodCall) {
            self.callees.insert(format!(".{}", node.method));
            syn::visit::visit_expr_method_call(self, node);
        }
        fn visit_macro(&mut self, node: &syn::Macro) {
            if let Some(seg) = node.path.segments.last() {
                let name = seg.ident.to_string();
                if name == "matches" {
                    self.branches += 1;
                }
                self.callees.insert(format!("{name}!"));
            }
            // `syn::visit` does not descend into token streams: re-parse
            // the body as a comma-separated expression list and visit it
            // (so `vec![f()]` is caught); otherwise fall back to a token
            // scan recording `ident?` for every identifier immediately
            // followed by a parenthesized group (gap G-4: `f::<T>(`
            // inside an unparseable macro body is the one form missed).
            if let Ok(exprs) = node.parse_body_with(
                syn::punctuated::Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated,
            ) {
                for e in &exprs {
                    self.visit_expr(e);
                }
            } else {
                token_fallback(node.tokens.clone(), &mut self.callees);
            }
        }
    }

    fn token_fallback(tokens: proc_macro2::TokenStream, out: &mut BTreeSet<String>) {
        let toks: Vec<proc_macro2::TokenTree> = tokens.into_iter().collect();
        for pair in toks.windows(2) {
            if let (proc_macro2::TokenTree::Ident(id), proc_macro2::TokenTree::Group(g)) =
                (&pair[0], &pair[1])
                && g.delimiter() == proc_macro2::Delimiter::Parenthesis
            {
                out.insert(format!("{id}?"));
            }
        }
        for t in toks {
            if let proc_macro2::TokenTree::Group(g) = t {
                token_fallback(g.stream(), out);
            }
        }
    }
}

/// Resolves a frame's function item in `file` — a frame anchor that
/// resolves to anything but exactly ONE item is a G4 failure.
fn frame_body<'a>(file: &'a syn::File, f: &Frame) -> &'a syn::Block {
    let mut hits: Vec<&syn::Block> = Vec::new();
    match f.ty {
        None => {
            for item in &file.items {
                if let syn::Item::Fn(func) = item
                    && func.sig.ident == f.anchor
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
                            && func.sig.ident == f.anchor
                        {
                            hits.push(&func.block);
                        }
                    }
                }
            }
        }
    }
    assert_eq!(
        hits.len(),
        1,
        "frame {} must resolve to exactly one item, found {}",
        frame_key(f),
        hits.len()
    );
    hits[0]
}

fn census_of(file: &syn::File, f: &Frame) -> (u32, BTreeSet<String>) {
    let mut c = census::Census::default();
    use syn::visit::Visit;
    c.visit_block(frame_body(file, f));
    (c.branches, c.callees)
}

fn parse_source(name: &str) -> syn::File {
    let src = match name {
        "client_agg.rs" => include_str!("../src/logql/client_agg.rs"),
        "variants.rs" => include_str!("../src/logql/variants.rs"),
        "plan.rs" => include_str!("../src/logql/plan.rs"),
        other => panic!("unknown frame file {other}"),
    };
    syn::parse_file(src).expect("the frame source parses")
}

#[test]
#[ignore = "generator: prints the frame censuses to pin"]
fn zz_print_frame_censuses() {
    let frames: [(&str, Option<&str>, &str); 27] = [
        ("plan.rs", None, "build_variants_node"),
        ("plan.rs", None, "unwrap_vector_aggs_into"),
        ("plan.rs", None, "parse_vector_agg_params"),
        ("plan.rs", None, "parse_plan_number"),
        ("exec.rs", Some("VariantArena"), "build"),
        ("exec.rs", Some("VariantsAggState"), "new"),
        ("exec.rs", None, "variant_pipeline_entry_bytes"),
        ("exec.rs", None, "stage_source_bytes"),
        ("exec.rs", None, "regex_stage_count"),
        ("exec.rs", None, "variant_state_bytes"),
        ("exec.rs", Some("ClientAggState"), "new"),
        ("exec.rs", Some("RangeSlideState"), "new"),
        ("exec.rs", Some("VariantsAggState"), "push_rows"),
        ("exec.rs", Some("VariantsAggState"), "finish"),
        ("exec.rs", Some("VariantsAggState"), "finish_in_place"),
        ("exec.rs", None, "append_variant_label"),
        ("exec.rs", Some("MetricAggState"), "push_rows"),
        ("exec.rs", Some("MetricAggState"), "finish"),
        ("exec.rs", Some("ClientAggState"), "push_rows"),
        ("exec.rs", Some("ClientAggState"), "finish"),
        ("exec.rs", Some("RangeSlideState"), "push_rows"),
        ("exec.rs", Some("RangeSlideState"), "finish"),
        ("exec.rs", Some("RangeSlideState"), "finish_in_place"),
        ("exec.rs", Some("RangeSlideState"), "drain_group"),
        ("exec.rs", Some("RangeSlideState"), "finish_absent"),
        ("exec.rs", Some("RangeSlideState"), "flush_collision"),
        ("exec.rs", Some("FpSlide"), "finish"),
    ];
    for (file, ty, anchor) in frames {
        let f = Frame {
            file,
            ty,
            anchor,
            branches: 0,
            callees: &[],
        };
        let parsed = parse_source(file);
        let src = &parsed;
        let (br, callees) = census_of(src, &f);
        let list: Vec<String> = callees.into_iter().collect();
        println!(
            "FRAME {file} {} {br} {} :: {}",
            frame_key(&f),
            list.len(),
            list.join(" ")
        );
    }
}

/// The 27 per-variant frames (`W-MEM`): one entry per FUNCTION. The 12
/// frames this issue creates are pinned from the implementation-commit
/// census; the 14 pre-existing frames reproduce the plan's pinned
/// censuses except where the plan itself edits the body (deviations
/// reported in the implementation notes): `parse_vector_agg_params`
/// (M1's `.clone` → `.cloned` plus `format_args!`) and
/// `RangeSlideState::new` (the `vec![0; …]` macro body does not parse as
/// an expression list, so the token fallback records `max?`).
static PER_VARIANT_FRAMES: [Frame; 34] = [
    // --- W_plan (4) ---
    Frame {
        file: "plan.rs",
        ty: None,
        anchor: "build_variants_node",
        // Issue #344 (grammar half): 26 -> 27. The range-aggregation
        // grouping was parsed but not executed, so the per-variant arm
        // refused it by name (`if grouping.is_some()`) — one new branch,
        // NIL taken / BAND untaken, no new callee.
        //
        // Issue #344 (execution half): 29 -> 28, and `.as_ref` joins the
        // callee set. The refusal is DELETED — grouped variants now
        // execute — and nothing conditional replaces it: the clause is
        // handed to `VariantSpec::try_new` as `grouping.as_ref()`, an
        // unconditional borrow of a field the AST already owns. W-MEM
        // disposition of the new callee: **NIL** — `Option::as_ref` is a
        // reference reshuffle that allocates on neither path, so it adds
        // no term to any per-variant band. The normalization it feeds
        // (`RangeGrouping::from_ast`, one `Vec<String>` clone) happens
        // inside `try_new`, AFTER that frame's `charge_fanout_bytes`
        // gate, and is charged by `variant_spec_bytes`' new
        // range-grouping term — so the allocation is inside an existing
        // band, not a new one. Inventory row P-l.
        //
        // Issue #343: 27 branches — UNCHANGED — and two new callees, from
        // the `offset` window shift. The interim `if range.offset_ns
        // .is_some()` refusal came out and nothing replaced it: a variant
        // carrying a different offset from the common range's is PLANNED,
        // reading its own shifted window intersected with the one shared
        // scan, which is what the reference does (measured, v3.7.4, seeded
        // store). An earlier draft refused that shape by name and the
        // branch count stood at 28; the probe refuted the refusal, so the
        // net effect on this frame is a guard removed and no guard added.
        // W-MEM disposition of what remains: **NIL** — `.unwrap_or` on an
        // `Option<i64>` and `shift_by_offset` (one `i64::checked_sub`)
        // are integer arithmetic over `Copy` scalars, allocating on
        // neither path, so they add no term to any per-variant band.
        // Inventory row P-l.
        //
        // Issue #343 boundary fix: 27 -> 29, no callee change.
        // `shift_by_offset` became fallible (`checked_sub`), so each spec
        // arm now matches on its result and substitutes the degenerate
        // empty window when the shifted domain leaves the representable
        // timestamp axis — one new arm per spec shape, PER VARIANT (a
        // sibling variant is unaffected). W-MEM disposition: **NIL** on
        // both new arms — the substitution writes two `i64` literals into
        // the `Copy` `ClientWindow` that was being built anyway; no
        // allocation on either path, so no per-variant band term.
        //
        // Issue #247 round 2: 28 -> 29, and `compile` joins the callee
        // set. A variant's own pipeline is dead syntax at evaluation but
        // the reference VALIDATES it — `newVariantsEvaluator` builds the
        // variant's extractor purely to count it
        // (`pkg/logql/evaluator.go:1417`, `:1422 @ v3.7.4`) — so
        // `CompiledPipeline::compile(common_stages(pipeline))?` runs here
        // and its result is discarded. The new branch is that `?`.
        //
        // W-MEM disposition of the new callee: **BAND** — a compile
        // allocates per stage, once PER VARIANT, so it is a real
        // per-variant term and not a NIL. It is bounded to the DISCARDED
        // PREFIX (`common_stages`, everything before the first
        // `Stage::Unwrap`) precisely so it does not double the tail:
        // `VariantArena::build` already compiles `common ++ tail` before
        // any I/O, so compiling the whole variant pipeline here would
        // charge the tail's stages twice per variant. With the prefix
        // only, the G1c per-variant slope stays INSIDE the committed
        // `EXEC_*` band unchanged — measured, not assumed: compiling the
        // whole pipeline instead pushed G1c (F_rich) to 882 against the
        // band [547, 587], and the prefix-only form passes it. The band
        // constants are therefore untouched by this issue. Inventory row
        // P-l.
        //
        // Issue #397: 29 -> 30, and `.is_empty` joins the callee set.
        // The paragraph above holds for a BARE variant only. The
        // reference type-switches on the variant expression
        // (`pkg/logql/syntax/extractor.go:114 @ v3.7.4`), so a variant
        // wrapped in a vector aggregation runs its WHOLE pipeline; the
        // new branch is `if raw_buf.is_empty()`, choosing between the
        // bare arm (validate the discarded prefix; tail = `variant_tail`)
        // and the wrapped arm (tail = the whole pipeline, no validation
        // compile — `VariantArena::build` already compiles `common ++
        // tail` before any I/O).
        //
        // W-MEM disposition of the new callee: **NIL** — `<[_]>::is_empty`
        // is a length test on a slice already in hand. The BRANCH is
        // BAND on both arms, and they move the per-variant cost in
        // OPPOSITE directions, which is why the inventory splits P-e
        // into P-e1/P-e2: the bare arm keeps the discarded-prefix
        // `compile` (a per-stage allocation) and a tail starting at the
        // `unwrap`; the wrapped arm drops that compile and hands
        // `try_new` a LONGER tail slice to clone. Both terms are
        // slice-driven and already charged — `variant_spec_bytes(tail,
        // …)` and `variant_pipeline_entry_bytes(common, tail)` take the
        // tail BY SLICE, so a longer tail is charged more with no
        // formula change. F_wrap puts a band on the wrapped arm;
        // G1a/G1c/G1d/G1e keep the bare one.
        branches: 30,
        callees: &[
            ".any",
            ".as_nanos",
            // Issue #344: the per-variant grouping borrow, NIL.
            ".as_ref",
            ".as_u64",
            ".clone",
            ".enumerate",
            // Issue #397: the bare/wrapped type-switch test, NIL.
            ".is_empty",
            ".is_some",
            ".iter",
            ".len",
            ".map_err",
            // Issue #343: the offset shift, both NIL (see above).
            ".unwrap_or",
            "shift_by_offset",
            // Issue #272: `v.variants` is a `ChildVec`, so the variant
            // list is read as an inert handle opened by the driver
            // (`walk::slice_of(v.variants.peek())`) rather than by
            // `.iter()` on a `Vec`. Branch count unchanged at 26; two
            // callees join. W-MEM disposition: **NIL** — both are
            // `#[inline]` newtype/reference maps with no allocation.
            ".peek",
            ".push",
            "slice_of",
            ".to_string",
            ".to_vec",
            "Err",
            "Ok",
            "QueryTooBroad",
            "Some",
            "Unwrap",
            "charge_fanout_bytes",
            "common_stages",
            // Issue #247 round 2: the discarded-prefix validation, BAND.
            "compile",
            "format!",
            "format_args!",
            "matches!",
            "metric_plan",
            "new",
            "parse_plan_number",
            "reject",
            "try_new",
            "unwrap_vector_aggs_into",
            "validate_duration_ns",
            "variant_tail",
            "vec_buffer_bytes",
            "widen_scan_start",
            "with_capacity",
        ],
    },
    Frame {
        file: "plan.rs",
        ty: None,
        anchor: "unwrap_vector_aggs_into",
        // Issue #272: the `while let` became `walk::descend_spine` — one
        // closure `match` plus one `match` on the `Descent`, 1 -> 2
        // branches, and the driver plus its two `unreachable!()` arms
        // join the callee set. Regenerated with `zz_print_frame_censuses`.
        // W-MEM disposition: **NIL** — `descend_spine` holds one loop
        // variable and allocates nothing on any path, so the only
        // allocations left in this frame are `out`'s own growth, exactly
        // as before.
        branches: 2,
        callees: &[
            ".as_deref",
            ".as_ref",
            ".clear",
            ".push",
            "Break",
            "Continue",
            "Expr",
            "descend_spine",
            "unreachable!",
        ],
    },
    Frame {
        file: "plan.rs",
        ty: None,
        anchor: "parse_vector_agg_params",
        branches: 6,
        callees: &[
            ".cloned",
            ".collect",
            ".is_some",
            ".iter",
            ".map",
            ".takes_param",
            ".to_string",
            "Err",
            "Ok",
            "Some",
            "format!",
            "format_args!",
            "matches!",
            "parse_plan_number",
        ],
    },
    Frame {
        file: "plan.rs",
        ty: None,
        anchor: "parse_plan_number",
        branches: 1,
        callees: &[".is_finite", ".parse", "Err", "Ok", "format!"],
    },
    // --- W_ctor (8) ---
    Frame {
        file: "variants.rs",
        ty: Some("VariantArena"),
        anchor: "build",
        branches: 11,
        callees: &[
            ".client",
            ".enumerate",
            ".extended_with",
            ".is_empty",
            ".iter",
            ".len",
            ".map_err",
            ".push",
            "Err",
            "Ok",
            "QueryTooBroad",
            "Some",
            "charge_fanout_bytes",
            "compile",
            "variant_driver_buffer_bytes",
            "variant_pipeline_entry_bytes",
            "with_capacity",
        ],
    },
    Frame {
        file: "variants.rs",
        ty: Some("VariantsAggState"),
        anchor: "new",
        // Issue #236 Part D: the instant sub-state arm narrows the
        // window to an `InstantWindow` witness before constructing
        // (`.as_instant` + the `.ok_or_else` refusal), 11 -> 12
        // branches. Regenerated with `zz_print_frame_censuses`. W-MEM
        // disposition: **NIL** — a `match` on a `Copy` enum and an
        // `Option`; the `.to_string` is inside the refusal arm, which
        // is unreachable (a stepped window routes to `Range` above).
        branches: 12,
        callees: &[
            ".as_instant",
            ".as_u64",
            ".charged_bytes",
            ".client",
            ".divided",
            ".enumerate",
            ".get",
            ".iter",
            ".len",
            ".map_err",
            ".max",
            ".ok_or_else",
            ".push",
            ".rate_window_ns",
            ".to_string",
            ".window",
            "Instant",
            "Ok",
            "Range",
            "charge_fanout_bytes",
            "grid_point_count",
            "matches!",
            "new",
            "variant_meta_snapshot_bytes",
            "variant_state_bytes",
            "with_capacity",
        ],
    },
    Frame {
        file: "variants.rs",
        ty: None,
        anchor: "variant_pipeline_entry_bytes",
        branches: 0,
        callees: &[
            ".saturating_add",
            ".saturating_mul",
            "regex_stage_count",
            "stage_source_bytes",
        ],
    },
    Frame {
        file: "variants.rs",
        ty: None,
        anchor: "stage_source_bytes",
        // Issue #272: `label_filter_bytes` became a driver consumer
        // (`for_each_label_filter`) rather than a recursion over
        // `LabelFilterExpr`'s now-`Child` slots, so the driver joins the
        // callee set. Branch count unchanged at 6. Regenerated with
        // `zz_print_frame_censuses`. W-MEM disposition: **NIL** — the
        // driver descends a sole-child spine in-loop and touches a
        // `ChunkStack` only at `arity >= 2`; the sum it accumulates is
        // identical to the recursion's.
        branches: 6,
        callees: &[
            ".alternatives",
            ".as_deref",
            ".as_ref",
            ".fold",
            ".iter",
            ".len",
            ".map_or",
            ".saturating_add",
            "for_each_label_filter",
            "label_filter_bytes",
            "matcher_bytes",
        ],
    },
    Frame {
        file: "variants.rs",
        ty: None,
        anchor: "regex_stage_count",
        branches: 6,
        // Issue #272: `label_filter_regexes` became a driver consumer
        // (`for_each_label_filter`) rather than a recursion over
        // `LabelFilterExpr`'s now-`Child` slots. Branch count unchanged
        // at 6. Regenerated with `zz_print_frame_censuses`. W-MEM
        // disposition: **NIL** — the driver allocates nothing on an
        // `arity <= 1` tree and the count it accumulates is identical.
        callees: &[
            ".alternatives",
            ".as_ref",
            ".fold",
            ".is_some_and",
            ".iter",
            ".saturating_add",
            "for_each_label_filter",
            "from",
            "label_filter_regexes",
            "matches!",
        ],
    },
    Frame {
        file: "variants.rs",
        ty: None,
        anchor: "variant_state_bytes",
        branches: 3,
        callees: &[
            ".is_empty",
            ".max",
            ".saturating_add",
            "alloc_block_bytes",
            "label_set_bytes",
            "size_of",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("ClientAggState"),
        anchor: "new",
        // Issue #236 Part D: the stepped-grid guard is deleted with the
        // `InstantWindow` parameter — a stepped window can no longer
        // reach this constructor, so the branch it defended is gone
        // (3 -> 1 branches). Regenerated with `zz_print_frame_censuses`.
        // W-MEM disposition: **unchanged** — row C-e still prices the
        // `base_labels` snapshot, which is the only allocation here.
        //
        // Issue #344: 1 branch — UNCHANGED — and two new callees.
        // `.is_some` is the fan-out gate's new disjunct
        // (`metric_mutates_labels() || client.grouping.is_some()`, which
        // makes a grouped instant query group by final label set instead
        // of by fingerprint); `||` short-circuits, so the census counts it
        // as part of the same expression rather than a new branch. W-MEM
        // disposition: **NIL** — `Option::is_some` on a borrowed plan
        // field allocates on neither path.
        //
        // `stream_hash` is the SECOND new callee and the one with a real
        // charge: the instant state now builds a per-fingerprint `hashes`
        // map beside `base_labels`, because `first_over_time`/
        // `last_over_time` take the endpoints of Loki's `(timestamp,
        // stream_hash, tie_rank)` order on the instant path too (it
        // previously used a value tiebreak, which returned a wrong value
        // on a group merging two streams at one nanosecond). W-MEM
        // disposition: **BAND, row C-e** — the same row that prices the
        // `base_labels` snapshot, whose `variant_meta_snapshot_bytes`
        // hashes term is now charged for BOTH sub-state kinds rather than
        // the sliding one alone. `stream_hash` itself hashes into a
        // stack buffer and allocates nothing; the map entries it fills
        // are what C-e prices.
        //
        // Issue #344 review round 1: 1 -> 2 branches, and `matches!` +
        // `reducer_class` join the callee set. The new branch is the
        // `needs_ts_order` gate — `matches!(reducer_class(op, value),
        // CanonicalFold)` — decided ONCE here rather than per row, so the
        // equal-timestamp staging buffer exists only for the reducers
        // whose fold order matters. W-MEM disposition: **NIL** —
        // `reducer_class` is an exhaustive `match` over two `Copy` enums
        // returning a third, and `matches!` compiles to a discriminant
        // test; neither allocates on either path, and the staging buffer
        // the flag governs starts EMPTY (`Vec::new()` allocates nothing)
        // and is charged against the collision caps when it fills.
        //
        // Issue #249: 2 branches — UNCHANGED — and one new callee,
        // `slider_safe_fingerprints`. It decides ONCE per query which
        // fingerprints may keep the per-fingerprint accumulator, which is
        // what lets structured metadata make the final label set the
        // output-series key without moving the metadata-free hot path.
        // W-MEM disposition: **BAND, row C-e** — the same row that already
        // prices the `base_labels`/`hashes` snapshot. The helper's own
        // retained allocation is one `HashSet<u64>` of at most one entry
        // per resolved fingerprint (8 bytes of key), i.e. strictly
        // narrower than the `base_labels` entry C-e already charges for
        // the same fingerprint, and it scales on the same axis
        // (`meta.len()`) — so the row's slope covers it without a new
        // coefficient. It allocates nothing per ROW.
        branches: 2,
        callees: &[
            ".insert",
            // Issue #344: the fan-out gate's grouping disjunct, NIL.
            ".is_some",
            ".metric_mutates_labels",
            "Ok",
            // Issue #344 review round 1: the `needs_ts_order` gate, NIL.
            "matches!",
            "new",
            "reducer_class",
            "series_labels",
            // Issue #249: the per-query slider-safety set, BAND (C-e).
            "slider_safe_fingerprints",
            // Issue #344: the instant path's `hashes` map, BAND (C-e).
            "stream_hash",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("RangeSlideState"),
        anchor: "new",
        // Issue #344: 6 branches — UNCHANGED — and two new callees.
        // `.is_some` is the fan-out gate's grouping disjunct (a grouping
        // MERGES streams, so the state must group by final label set, not
        // by fingerprint) and `.as_deref` borrows the boxed clause into
        // the state's `grouping` field for the per-row projection. Both
        // are in the existing `let`/struct-literal expressions, so
        // neither is a new branch. W-MEM disposition: **NIL** for both —
        // `Option::is_some`/`Option::as_deref` over a borrowed plan field
        // allocate on neither path and add no per-variant band term (the
        // clause itself was cloned and charged at plan time, inside
        // `variant_spec_bytes`' range-grouping term). Inventory row C-e.
        //
        // Issue #249: 6 branches — UNCHANGED — and one new callee,
        // `slider_safe_fingerprints`, the twin of the `ClientAggState::new`
        // entry above and with the same disposition: **BAND, row C-e**. One
        // `HashSet<u64>` sized at most one 8-byte key per resolved
        // fingerprint, scaling on the `meta.len()` axis C-e already prices
        // for `base_labels`/`hashes` and strictly narrower per entry than
        // either; nothing per ROW.
        branches: 6,
        callees: &[
            // Issue #344: the borrowed grouping, NIL.
            ".as_deref",
            ".as_u64",
            ".clone",
            ".get",
            ".insert",
            // Issue #344: the fan-out gate's grouping disjunct, NIL.
            ".is_some",
            ".metric_mutates_labels",
            "Ok",
            "ensure_grid_resolution",
            "matches!",
            "max?",
            "new",
            "reducer_class",
            "retention_points_per_sample",
            "series_labels",
            // Issue #249: the per-query slider-safety set, BAND (C-e).
            "slider_safe_fingerprints",
            "stream_hash",
            "unreachable!",
            "vec!",
        ],
    },
    // --- W_fin (14) ---
    Frame {
        file: "variants.rs",
        ty: Some("VariantsAggState"),
        anchor: "push_rows",
        branches: 7,
        callees: &[
            ".admits_instant",
            ".enumerate",
            ".iter_mut",
            ".len",
            ".push_rows",
            "Ok",
        ],
    },
    Frame {
        file: "variants.rs",
        ty: Some("VariantsAggState"),
        anchor: "finish",
        branches: 1,
        callees: &[".finish_in_place", "Ok", "debug_assert_eq!"],
    },
    Frame {
        file: "variants.rs",
        ty: Some("VariantsAggState"),
        anchor: "finish_in_place",
        // Issue #236 (§10.5, predicted): the per-variant
        // `ensure_result_series(&out)?` adds one `syn::ExprTry` branch
        // (16 → 17) and one callee. Regenerated with
        // `zz_print_frame_censuses`, not hand-edited. W-MEM inventory
        // disposition: **NIL** — the call takes `&QueryResult`, reads a
        // `Vec::len`, and allocates nothing on either arm (the error arm
        // constructs a fixed-size `TooBroadReason`), so it adds no
        // per-variant allocation for the G-gates to bound.
        //
        // Issue #236 §4: `apply_vector_aggs` is fallible now (it charges
        // the stage's modelled bytes before allocating), so the
        // per-variant call gains a second `syn::ExprTry` branch
        // (17 -> 18). The callee set is unchanged. Regenerated with
        // `zz_print_frame_censuses`. W-MEM disposition: **NIL** — the
        // charge is integer arithmetic over an already-materialised
        // `StageInput` and the refusal arm builds one fixed-size
        // `TooBroadReason`, so the per-variant allocation count does not
        // move.
        branches: 18,
        callees: &[
            ".any",
            ".client",
            ".enumerate",
            ".extend",
            ".filter",
            ".finish",
            ".first",
            ".index",
            ".into_iter",
            ".is_empty",
            ".iter",
            ".iter_mut",
            ".len",
            ".map",
            ".push",
            ".sum",
            ".vector_aggs",
            "Matrix",
            "Ok",
            "Some",
            "Vector",
            "append_variant_label",
            "apply_vector_aggs",
            "discharge_fanout_bytes",
            "ensure_result_series",
            "matches!",
            "take",
            "with_capacity",
        ],
    },
    Frame {
        file: "variants.rs",
        ty: None,
        anchor: "append_variant_label",
        branches: 1,
        callees: &[
            ".as_str",
            ".binary_search_by",
            ".cmp",
            ".insert",
            ".to_string",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("MetricAggState"),
        anchor: "push_rows",
        branches: 1,
        callees: &[".push_rows"],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("MetricAggState"),
        anchor: "finish",
        branches: 1,
        // Issue #344 review round 1: `Ok` LEAVES the callee set.
        // `ClientAggState::finish` became fallible (it flushes the last
        // staged equal-timestamp run, which charges group bytes like any
        // other group creation), so the instant arm forwards its
        // `Result` instead of wrapping a `QueryResult` in `Ok`. One
        // callee fewer, branches unchanged, and no new allocation: the
        // flush's charge is `ClientAggState::group_bytes`, already
        // enumerated.
        callees: &[".finish"],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("ClientAggState"),
        anchor: "push_rows",
        // Issue #344 review round 1: this frame is now a THIN WRAPPER.
        // The batch-reused `scratch` borrows from `base_labels` through
        // its `Cow`s, so the row loop cannot also hold a `&mut self` for
        // the mid-loop staging flush; `base_labels` therefore moves to a
        // local for the batch (the move `RangeSlideState::push_rows`
        // already makes, for the same reason) and the body moved to
        // `push_rows_inner`, censused separately below. W-MEM
        // disposition: **NIL** — `mem::take` swaps a `HashMap` header,
        // allocating nothing, and the map is restored on every exit
        // including the error one. Inventory row F-b.
        branches: 0,
        callees: &[".push_rows_inner", "take"],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("ClientAggState"),
        anchor: "finish_folded",
        // Issue #236 Part D: the state is instant-only by construction,
        // so the two stepped arms — the `bucket_grid` absence walk and
        // the per-bucket `Matrix` emit — are DELETED, not merely unused
        // (10 -> 5 branches, `bucket_grid`/`Matrix`/`MatrixSeries`/
        // `INSTANT_BUCKET` leave the frame). Regenerated with
        // `zz_print_frame_censuses`. W-MEM disposition: rows F-i and
        // F-v classified exactly those two arms as UNREACH and are
        // deleted with them.
        //
        // Issue #249: 5 -> 4 branches, and `.extend` joins the callee set.
        // Routing is now per ROW, so a run can leave output in BOTH maps
        // and the `if self.fan_out { … } else { … }` that picked ONE of
        // them is deleted — that is the branch that left. Both maps are
        // now drained unconditionally into one vector, `label_groups`
        // through the existing `collect` and `fp_groups` through
        // `.extend` (a second STATEMENT, because both drains discharge
        // into the same `&mut group_bytes` and two closures cannot hold
        // it at once). W-MEM disposition: **NIL** — the drains and their
        // discharges are unchanged term for term; `.extend` reserves for
        // an iterator whose elements this frame already allocated and
        // charged, and on every non-mixed run one of the two drains is
        // empty, which is every run a variants sub-state can produce
        // (`VariantsAggState` builds one sub-state per extractor and each
        // is wholly fan-out or wholly not).
        branches: 4,
        callees: &[
            ".clone",
            ".collect",
            // Issue #249: the second drain's append, NIL.
            ".extend",
            ".filter_map",
            ".finish",
            ".get",
            ".into_iter",
            ".map",
            "Some",
            "Vector",
            "debug_assert_eq!",
            "discharge_group_bytes",
            "group_entry_bytes",
            "matches!",
            "new",
            "vec!",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("ClientAggState"),
        anchor: "push_rows_inner",
        // Issue #249: 25 -> 26 branches, and two new callees, `row_route`
        // + `.contains`. The state-level `if self.fan_out` that chose
        // between `label_groups` and `fp_groups` becomes a per-ROW
        // `if matches!(route, RowRoute::Labels)` — the `matches!` is the
        // one added branch — fed by `row_route(…, self.slider_safe
        // .contains(&row.fingerprint), &scratch, base)`. W-MEM
        // disposition: **NIL** — `row_route` is a length test plus a
        // zipped `&str` comparison over the label scratch and allocates
        // nothing on either arm, and `HashSet::<u64>::contains` hashes a
        // `Copy` key. Both run per row and both are allocation-free; the
        // per-row allocations this frame makes are unchanged, and which
        // map a row lands in was already priced by rows F-* of the
        // inventory for both destinations.
        //
        // Issue #344 review round 1: the former `push_rows` body, plus
        // the equal-timestamp staging route. 21 -> 22 branches (the
        // `needs_ts_order` arm) and two new callees: `.flush_pending`
        // closes the previous run and `.stage` opens the new one. W-MEM
        // disposition: **NIL in this frame** — the staging arm allocates
        // NOTHING here. Review round 2 moved the key render and the
        // `LabelSet` collect inside `ClientAggState::stage`, behind its
        // cap check, so this frame hands over the borrowed label scratch
        // and the `.then`/`.collect` that used to sit in the row loop are
        // gone from it. The allocations are censused where they now
        // happen (BAND, `ClientAggState::stage`). Inventory rows
        // F-b/F-d.
        branches: 5,
        callees: &[
            ".get",
            ".is_empty",
            ".push_one_row",
            "Ok",
            "default",
            "merge_labels_with_structured_metadata",
            "new",
            "recycle_label_scratch",
            "take",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("ClientAggState"),
        anchor: "stage",
        // Issue #344, re-shaped by review round 2. Size -> check BOTH
        // collision caps -> only then allocate, the same three-step order
        // `RangeSlideState::stage_member` uses. The previous revision
        // rendered the key and collected the `LabelSet` in the CALLER and
        // passed them in, so the cap was checked after the two
        // allocations it exists to bound; both now happen here, after the
        // check, which is why `.collect`/`.to_string`/
        // `render_labels_json_sorted` moved into this frame's callee set
        // and `.then` left `push_rows_inner`'s.
        //
        // W-MEM disposition: **BAND** — the rendered key, the cloned
        // `LabelSet` and one `Vec` slot per staged sample, all sized by
        // `stage_bytes` BEFORE they exist and charged against
        // `MAX_TS_COLLISION_GROUP`/`_BYTES`; a breach is the existing
        // named `TsCollisionGroup` 422 and leaves the buffer untouched.
        // The `Vec`'s capacity is returned to the state at flush, so the
        // steady state is one allocation per query. Inventory row F-d.
        branches: 2,
        callees: &[
            ".collect",
            ".contains_key",
            ".iter",
            ".len",
            ".map",
            ".push",
            ".saturating_add",
            ".stage_bytes",
            ".to_string",
            "Err",
            "Ok",
            "QueryTooBroad",
            "Some",
            "render_labels_json_sorted",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("ClientAggState"),
        anchor: "stage_bytes",
        // Issue #344 review round 2: the SIZING half of the staging
        // funnel, split out so the caller can charge before it allocates.
        // A provable upper bound on what `stage` is about to allocate —
        // the rendered JSON sized exactly by `rendered_labels_json_len`
        // and grown through `grown_alloc_bytes`, each label string and
        // the element buffer through `alloc_block_bytes`, and the
        // `PendingSample` slot at 8x to dominate a doubling `Vec`'s
        // realloc peak rather than charging one logical slot.
        //
        // W-MEM disposition: **NIL** — it walks the borrowed scratch and
        // returns a `u64`; it allocates on no path, which is the property
        // that lets it run before the cap check. Inventory row F-d.
        branches: 1,
        callees: &[
            ".len",
            ".saturating_add",
            ".saturating_mul",
            "alloc_block_bytes",
            "grown_alloc_bytes",
            "rendered_labels_json_len",
            "size_of",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("ClientAggState"),
        anchor: "flush_pending",
        // Issue #344 review round 1: folds the staged run in
        // `(stream_hash, tie_rank)` order and releases it. The sort is
        // STABLE, so equal hashes keep arrival order — which within one
        // stream is the scan's `body`-ascending sequence, i.e. exactly
        // `tie_rank`. W-MEM disposition: **BAND** — the group creations
        // it performs charge `group_bytes` in the same units the direct
        // arm does, and the buffer's capacity returns to the state so the
        // next run reuses it. Inventory row F-d.
        branches: 5,
        callees: &[
            ".add",
            ".clear",
            ".drain",
            ".entry",
            ".insert",
            ".into_mut",
            ".is_empty",
            ".is_err",
            ".is_ok",
            ".key",
            ".sort_by_key",
            ".unwrap_or_default",
            "Ok",
            "charge_group_bytes",
            "group_entry_bytes",
            "new",
            "take",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("ClientAggState"),
        anchor: "finish",
        // Issue #344 review round 1: `finish` became a two-step —
        // close the last staged equal-timestamp run, then read the
        // accumulators — so the emit body moved to `finish_folded`,
        // censused above. W-MEM disposition: **NIL** — the flush's own
        // allocations are `flush_pending`'s (BAND there); this frame adds
        // a `Result` wrap. Inventory row F-f.
        branches: 1,
        callees: &[".finish_folded", ".flush_pending", "Ok"],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("RangeSlideState"),
        anchor: "push_rows",
        // Issue #249: 8 branches — UNCHANGED — and two new callees.
        // `row_route(self.fan_out, self.slider_safe.contains(&row
        // .fingerprint), &scratch, base)` decides per ROW where the
        // sample belongs and is computed HERE, before `stage_member`
        // sorts the scratch in place (the comparison reads the
        // pipeline's output order). It sits inside the existing `let`,
        // so it adds no branch. W-MEM disposition: **NIL** for both —
        // a length test plus a zipped `&str` compare, and a
        // `HashSet<u64>` probe on a `Copy` key; neither allocates on
        // either path, and this frame's per-row allocation stays zero.
        branches: 6,
        // `.into` joined with issue #230: the render-budget breach
        // (`TemplateBudgetExceeded`) converts into `ReadError` on the
        // row path — F-d's NOT-EXEC disposition covers it.
        callees: &[
            ".flush_collision",
            ".get",
            ".is_empty",
            ".push_one_row",
            "Err",
            "Ok",
            "default",
            "merge_labels_with_structured_metadata",
            "new",
            "recycle_label_scratch",
            "take",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("RangeSlideState"),
        anchor: "finish",
        // Issue #343: one new callee, `shift_emitted_points` — the ONE
        // place a leaf's emitted matrix is put back on the caller's grid
        // after the whole evaluation ran `offset` earlier. Branch count
        // unchanged: it is a straight-line call, and the `offset_ns == 0`
        // early return inside it belongs to that function's own frame, not
        // this one. W-MEM disposition: **NIL** — it mutates the
        // already-allocated `points` vectors in place (`&mut` over
        // `(i64, f64)` tuples) and allocates nothing on either arm, so it
        // adds no per-variant term. Inventory row F-y2.
        branches: 1,
        callees: &[
            ".finish_in_place",
            "Ok",
            "debug_assert_eq!",
            "shift_emitted_points",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("RangeSlideState"),
        anchor: "finish_in_place",
        // Issue #236 P2 / Part B / Part C history above; the result-point
        // charge (issue #236 §4) made `finish_absent` fallible, so the
        // absent arm gains a `?` (10 -> 11 branches). Regenerated with
        // `zz_print_frame_censuses`. W-MEM disposition: **NIL** — a
        // propagated `Result`, no allocation.
        // Issue #249: 11 -> 12 branches, and five new callees
        // (`.chain`, `.map`, `.labels`, `Group`, plus the `Slider`/
        // `Group` match itself). Per-row routing lets one query leave
        // output in BOTH structures, so the fan-out arm's early return
        // becomes a union: `groups` and `series_out` are collected into
        // one `Vec<FinishItem>`, sorted on `labels()`, and emitted in
        // that single total order — which is what keeps a mixed run's
        // FOLDED value equal to its materialised value. The added
        // branch is the `FinishItem` match. W-MEM disposition:
        // **NIL** — issue #236 Part B's point-axis win is intact
        // because the `Group` arm stays LAZY (`drain_group` still runs
        // one group at a time inside the loop, nothing is materialised
        // up front), and the new `Vec<FinishItem>` replaces the
        // `Vec<(String, MutGroup)>` this frame already allocated and
        // already sorted — one element per output series either way,
        // on the axis rows F-* already price. A variants sub-state
        // leaves one of the two sources empty, so its element count is
        // unchanged.
        branches: 12,
        callees: &[
            ".as_mut",
            // Issue #249: the slider/group union, NIL.
            ".chain",
            ".cmp",
            ".collect",
            ".drain_group",
            ".emit",
            ".finish",
            ".finish_absent",
            ".flush_collision",
            ".into_iter",
            ".is_empty",
            // Issue #249: `FinishItem`'s sort key, NIL.
            ".labels",
            ".map",
            ".push",
            ".push_series",
            ".rotate_slider",
            ".sort_by",
            ".take",
            // Issue #249: the lazy `FinishItem::Group` arm, NIL.
            "Group",
            "Matrix",
            "Ok",
            "discharge_group_bytes",
            "drop",
            "group_entry_bytes",
            "new",
            "take",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("RangeSlideState"),
        anchor: "drain_group",
        // NEW with issue #236 Part C: one mutating group's cells drained
        // into grid points. Three arms — the class-A expanded map (kept
        // verbatim), the class-A difference array (C1) and the class-B/C
        // retained-sample sweep (C2). W-MEM disposition: **BAND** (row
        // F-y) — the measured windows push an EMPTY row slice, so no
        // mutating group is ever created and this body never runs, the
        // same premise F-d rests on. What it WOULD allocate is the
        // drained `Vec` and, on the C2 arm, the merged
        // covering-interval `Vec`, both bounded by the retention ALREADY
        // charged for the cells being drained.
        branches: 14,
        callees: &[
            ".checked_sub",
            ".collect",
            ".covering_k",
            ".get",
            ".grid_point",
            ".into_iter",
            ".last_mut",
            ".len",
            ".map_or",
            ".max",
            ".min",
            ".push",
            ".sort_by",
            ".sort_by_key",
            ".unwrap_or",
            "discharge_retention",
            "new",
            "reduce_int_cell",
            "reduce_window",
            "try_from",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("RangeSlideState"),
        anchor: "finish_absent",
        // Issue #236 Part B made this return `Vec<MatrixSeries>`; the
        // §4 result-point charge made it fallible and added the
        // reservation for the one series it may emit (3 -> 4 branches).
        // Regenerated with `zz_print_frame_censuses`. W-MEM disposition:
        // **BAND**, unchanged — row F-m already prices the points vector
        // and the one-element `vec![series]`; the charge is integer
        // arithmetic ahead of them.
        branches: 4,
        callees: &[
            ".grid_point",
            ".is_empty",
            ".push",
            "Ok",
            "charge_result_points",
            "grid_slot_count",
            "new",
            "take",
            "vec!",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("RangeSlideState"),
        anchor: "flush_collision",
        // Issue #236: Part A deleted the `series_count > caps.series`
        // rejection, P2 put a `charge_group_bytes` in its place, and the
        // §4 result-point charge reserves one grid width per slider
        // beside it (12 -> 13 branches). Regenerated with
        // `zz_print_frame_censuses`. W-MEM disposition: **NIL** —
        // integer arithmetic on the same once-per-fingerprint path the
        // deleted check occupied; it allocates nothing.
        // Issue #249: 13 -> 18 branches, and three new callees (`.any`,
        // `.iter`, `matches!`). The group's members are dispatched
        // INDIVIDUALLY now: `if self.fan_out { … return }` — one
        // whole-group decision — becomes two independent halves,
        // `if any_fp_route { …slider… }` then `if any_label_route
        // { …fan-out… } else { clear }`, plus the per-member
        // `matches!(m.route, …)` skips inside each. A collision group
        // can hold both routes once structured metadata makes the
        // final label set the output-series key. W-MEM disposition:
        // **NIL** — the two `.iter().any(…)` scans read a `Copy`
        // discriminant over a buffer bounded by
        // `AggCaps::collision_members` and allocate nothing, and each
        // half performs exactly the allocations it performed before,
        // for a subset of the members. A group that is wholly one
        // route — which is every group a variants sub-state can
        // produce, and every group of any metadata-free query — takes
        // byte-for-byte the path it took before.
        branches: 18,
        callees: &[
            // Issue #249: the per-member route partition, NIL.
            ".any",
            ".as_bytes",
            ".as_mut",
            ".clear",
            ".cloned",
            ".cmp",
            ".copied",
            ".covering_k",
            ".enumerate",
            ".expect",
            ".fan_out_sample",
            ".get",
            ".into_iter",
            ".is_empty",
            ".is_none",
            // Issue #249: the per-member route partition, NIL.
            ".iter",
            ".load_group",
            ".rotate_slider",
            ".sort_by",
            ".take",
            ".unwrap_or",
            ".unwrap_or_default",
            "Ok",
            "Some",
            "charge_group_bytes",
            "charge_result_points",
            "grid_slot_count",
            // Issue #249: the route discriminant tests, NIL.
            "matches!",
            "group_entry_bytes",
            "new",
            "take",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("FpSlide"),
        anchor: "finish",
        branches: 2,
        callees: &[
            ".emit_at",
            ".grid_point",
            ".is_empty",
            ".len",
            "Some",
            "discharge_retention",
        ],
    },
    // --- issue #249: the per-ROW frames the batch loops delegate to ---
    //
    // `push_rows_inner`/`push_rows` used to hold the whole row body. Issue
    // #249 moved it into `push_one_row` on each state so the metadata-free
    // and metadata-bearing rows share ONE implementation of valuing,
    // routing and staging a sample. The per-row allocations therefore live
    // HERE now, and both frames are censused so that move did not hide them
    // behind an un-censused helper.
    Frame {
        file: "client_agg.rs",
        ty: Some("ClientAggState"),
        anchor: "push_one_row",
        // The instant row body, moved here whole from `push_rows_inner`
        // (which kept 5 branches: the metadata split and its buffers).
        // W-MEM disposition: **NOT-EXEC, row F-d** — the disposition the
        // body carried inside `push_rows_inner`, unchanged by the move, and
        // still covered by `rows.is_empty()` plus the resident
        // `CLIENT_AGG_FLAT_BUDGET` per-row gate.
        branches: 24,
        callees: &[
            ".add",
            ".as_deref",
            ".collect",
            ".contains",
            ".contains_key",
            ".copied",
            ".entry",
            ".flush_pending",
            ".get",
            ".get_mut",
            ".insert",
            ".into_mut",
            ".iter",
            ".key",
            ".len",
            ".map",
            ".run_metric_into_with_sm",
            ".sort_unstable",
            ".stage",
            ".to_string",
            ".unwrap_or",
            "Err",
            "Ok",
            "QueryTooBroad",
            "charge_group_bytes",
            "check_surviving_error",
            "group_entry_bytes",
            "matches!",
            "new",
            "render_labels_json_sorted",
            "row_route",
        ],
    },
    Frame {
        file: "client_agg.rs",
        ty: Some("RangeSlideState"),
        anchor: "push_one_row",
        // The sliding row body, likewise moved whole out of `push_rows`.
        // W-MEM disposition: **NOT-EXEC, row F-d**, for the same reason.
        branches: 5,
        callees: &[
            ".contains",
            ".len",
            ".run_metric_into_with_sm",
            ".stage_member",
            "Ok",
            "check_surviving_error",
            "row_route",
        ],
    },
];

/// The complete op/shape-conditional branch inventory: 11 W_plan + 12
/// W_ctor + 23 W_fin = 46 rows, each with exactly ONE disposition. The
/// module-doc tables are a RENDERING of this const (assertion 7), never
/// the source.
static INVENTORY: [Row; 50] = [
    // --- W_plan (13) ---
    Row {
        id: "P-a",
        window: Win::Plan,
        what: "0 vs 1 vector layers",
        frames: &["::unwrap_vector_aggs_into", "::build_variants_node"],
        site: "plan.rs build_variants_node loop",
        disp: Disp::Band,
        covered_by: "G1d/G1e (0) vs G1a/G1c (1)",
    },
    Row {
        id: "P-b",
        window: Win::Plan,
        what: "parameterized vs parameterless aggregation",
        frames: &["::parse_vector_agg_params"],
        site: "plan.rs parse_vector_agg_params",
        disp: Disp::Band,
        covered_by: "G1a (param) vs G1c (none)",
    },
    Row {
        id: "P-c",
        window: Win::Plan,
        what: "sort-grouping + approx_topk-in-range rejections",
        frames: &["::parse_vector_agg_params"],
        site: "plan.rs parse_vector_agg_params",
        disp: Disp::Nil,
        covered_by: "error paths abort the plan - B1",
    },
    Row {
        id: "P-d",
        window: Win::Plan,
        what: "grouping created / by-cloned / without-cloned",
        frames: &["::build_variants_node"],
        site: "plan.rs VariantSpec::try_new injection",
        disp: Disp::Band,
        covered_by: "G1a / G1c / G1g",
    },
    // Issue #397 splits the old single P-e ("tail empty vs non-empty")
    // into its two axes, because the tail is now chosen by TWO
    // independent facts: whether the variant is bare or wrapped, and —
    // on the bare arm only — whether it carries an `unwrap`.
    Row {
        id: "P-e1",
        window: Win::Plan,
        what: "BARE variant: unwrap tail empty vs non-empty",
        frames: &["::build_variants_node"],
        site: "plan.rs variant_tail + try_new clone",
        disp: Disp::Band,
        covered_by: "G1a/G1d/G1e vs G1c",
    },
    Row {
        id: "P-e2",
        window: Win::Plan,
        what: "WRAPPED variant: whole pipeline as the tail (no prefix compile)",
        frames: &["::build_variants_node"],
        site: "plan.rs build_variants_node bare/wrapped arm",
        disp: Disp::Band,
        covered_by: "G1i (F_wrap)",
    },
    Row {
        id: "P-f",
        window: Win::Plan,
        what: "arity class success side (forbids vs requires unwrap)",
        frames: &["::build_variants_node"],
        site: "plan.rs build_variants_node arity gates",
        disp: Disp::Band,
        covered_by: "G1a vs G1c",
    },
    Row {
        id: "P-g",
        window: Win::Plan,
        what: "ClientValue Count / Bytes / Unwrap",
        frames: &["::build_variants_node"],
        site: "plan.rs build_variants_node value arm",
        disp: Disp::Band,
        covered_by: "G1a / G1f / G1c",
    },
    Row {
        id: "P-h",
        window: Win::Plan,
        what: "quantile parameter parse",
        frames: &["::build_variants_node"],
        site: "plan.rs build_variants_node quantile arm",
        disp: Disp::Band,
        covered_by: "G1h",
    },
    Row {
        id: "P-i",
        window: Win::Plan,
        what: "absent vs non-absent absent_labels",
        frames: &["::build_variants_node"],
        site: "plan.rs VariantSpec::try_new absent arm",
        disp: Disp::Band,
        covered_by: "G1e vs every other plan fixture",
    },
    Row {
        id: "P-j",
        window: Win::Plan,
        what: "parse_plan_number success path (format! only on Err)",
        frames: &["::parse_plan_number"],
        site: "plan.rs parse_plan_number",
        disp: Disp::Nil,
        covered_by: "executes under G1a/G1h",
    },
    Row {
        id: "P-k",
        window: Win::Plan,
        what: "the reused raw-handle buffer (one growth, intercept)",
        frames: &["::unwrap_vector_aggs_into"],
        site: "plan.rs unwrap_vector_aggs_into",
        disp: Disp::Band,
        covered_by: "G1a upper band; G1d pins the 0-layer shape at 0",
    },
    Row {
        id: "P-l",
        window: Win::Plan,
        what: "the variant offset shift (integer arithmetic only)",
        frames: &["::build_variants_node"],
        site: "plan.rs build_variants_node offset arm",
        disp: Disp::Nil,
        covered_by: "executes under every G1 fixture",
    },
    // --- W_ctor (12) ---
    Row {
        id: "C-a",
        window: Win::Ctor,
        what: "state kind Range vs Instant",
        frames: &[
            "VariantsAggState::new",
            "ClientAggState::new",
            "RangeSlideState::new",
        ],
        site: "exec.rs VariantsAggState::new kind dispatch",
        disp: Disp::Band,
        covered_by: "G2b/G2c/G2f (range) vs G2d/G2e (instant)",
    },
    Row {
        id: "C-b",
        window: Win::Ctor,
        what: "is_absent gates present_cover (branch-free multiplier)",
        frames: &["RangeSlideState::new"],
        site: "exec.rs RangeSlideState::new present_cover",
        disp: Disp::Band,
        covered_by: "G2c (range) / G2e (instant: costs nothing)",
    },
    Row {
        id: "C-c",
        window: Win::Ctor,
        what: "absent_labels.clone() empty vs populated",
        frames: &["RangeSlideState::new"],
        site: "exec.rs RangeSlideState::new",
        disp: Disp::Band,
        covered_by: "G2b (empty) vs G2c (2 Eq matchers)",
    },
    Row {
        id: "C-d",
        window: Win::Ctor,
        what: "arena: empty tail shares entry 0",
        frames: &["VariantArena::build"],
        site: "exec.rs VariantArena::build",
        disp: Disp::Band,
        covered_by: "G2b",
    },
    Row {
        id: "C-e",
        window: Win::Ctor,
        what: "meta empty vs populated (base_labels/hashes loops)",
        frames: &["ClientAggState::new", "RangeSlideState::new"],
        site: "exec.rs constructors + series_labels",
        disp: Disp::R4(1),
        covered_by: "I4/I5 charge isolation; G2/G3 slopes",
    },
    Row {
        id: "C-f",
        window: Win::Ctor,
        what: "op-derived scalars (reducer_class etc.)",
        frames: &["RangeSlideState::new"],
        site: "exec.rs RangeSlideState::new",
        disp: Disp::Nil,
        covered_by: "executes under G2b + G2c",
    },
    Row {
        id: "C-g",
        window: Win::Ctor,
        what: "ensure_grid_resolution (integer arithmetic)",
        frames: &["ClientAggState::new", "RangeSlideState::new"],
        site: "exec.rs constructors",
        disp: Disp::Nil,
        covered_by: "executes under G2b",
    },
    Row {
        id: "C-h",
        window: Win::Ctor,
        what: "tail-slice dedup backward scan (no key materialized)",
        frames: &["VariantArena::build"],
        site: "exec.rs VariantArena::build",
        disp: Disp::Nil,
        covered_by: "G2f pins a dedup HIT at zero",
    },
    Row {
        id: "C-i",
        window: Win::Ctor,
        what: "sizing walks over borrowed AST (no temporary container)",
        frames: &[
            "::variant_pipeline_entry_bytes",
            "::stage_source_bytes",
            "::regex_stage_count",
            "::variant_state_bytes",
        ],
        site: "exec.rs sizing helpers",
        disp: Disp::Nil,
        covered_by: "execute under every G2 fixture",
    },
    Row {
        id: "C-j",
        window: Win::Ctor,
        what: "driver buffer reservations (with_capacity, intercept)",
        frames: &["VariantsAggState::new", "VariantArena::build"],
        site: "exec.rs build/new preambles",
        disp: Disp::Nil,
        covered_by: "a per-variant realloc would fail G2b",
    },
    Row {
        id: "C-k",
        window: Win::Ctor,
        what: "arena: shared non-empty tail (dedup hit)",
        frames: &["VariantArena::build"],
        site: "exec.rs VariantArena::build",
        disp: Disp::Band,
        covered_by: "G2f",
    },
    Row {
        id: "C-l",
        window: Win::Ctor,
        what: "arena: DISTINCT non-empty tail (extended_with compile)",
        frames: &["VariantArena::build"],
        site: "exec.rs VariantArena::build",
        disp: Disp::R4(2),
        covered_by: "I2 charge isolation; G3 peak slope",
    },
    // --- W_fin (23) ---
    Row {
        id: "F-a",
        window: Win::Fin,
        what: "MetricAggState::push_rows kind dispatch",
        frames: &["MetricAggState::push_rows"],
        site: "exec.rs MetricAggState::push_rows",
        disp: Disp::Nil,
        covered_by: "Phi1/Phi4",
    },
    Row {
        id: "F-b",
        window: Win::Fin,
        what: "ClientAggState::push_rows prologue (Vec::new scratch)",
        frames: &[
            "ClientAggState::push_rows",
            "ClientAggState::push_rows_inner",
        ],
        site: "exec.rs ClientAggState::push_rows",
        disp: Disp::Nil,
        covered_by: "Phi4-Phi7",
    },
    Row {
        id: "F-c",
        window: Win::Fin,
        what: "RangeSlideState::push_rows prologue/epilogue (mem::take)",
        frames: &["RangeSlideState::push_rows"],
        site: "exec.rs RangeSlideState::push_rows",
        disp: Disp::Nil,
        covered_by: "Phi1-Phi3",
    },
    Row {
        id: "F-d",
        window: Win::Fin,
        what: "the row loops and everything reachable only from them",
        frames: &[
            "ClientAggState::push_rows",
            "ClientAggState::push_rows_inner",
            // Issue #249: the row bodies moved out of the two batch loops
            // into these, and they are reachable ONLY from them.
            "ClientAggState::push_one_row",
            "RangeSlideState::push_one_row",
            "ClientAggState::stage",
            "ClientAggState::stage_bytes",
            "ClientAggState::flush_pending",
            "RangeSlideState::push_rows",
            "RangeSlideState::flush_collision",
            "FpSlide::finish",
        ],
        site: "exec.rs row paths",
        disp: Disp::NotExec,
        covered_by: "rows.is_empty(); the existing CLIENT_AGG_FLAT_BUDGET per-row gate",
    },
    Row {
        id: "F-e",
        window: Win::Fin,
        what: "MetricAggState::finish kind dispatch (Box move)",
        frames: &["MetricAggState::finish"],
        site: "exec.rs MetricAggState::finish",
        disp: Disp::Nil,
        covered_by: "Phi1/Phi4",
    },
    Row {
        id: "F-f",
        window: Win::Fin,
        what: "ClientAggState::finish absent vs non-absent",
        frames: &["ClientAggState::finish", "ClientAggState::finish_folded"],
        site: "exec.rs ClientAggState::finish",
        disp: Disp::Band,
        covered_by: "Phi6/Phi7 vs Phi4/Phi5",
    },
    Row {
        id: "F-g",
        window: Win::Fin,
        what: "absent: the finish-time absent_labels clone (1 + 2k)",
        frames: &["ClientAggState::finish", "ClientAggState::finish_folded"],
        site: "exec.rs ClientAggState::finish",
        disp: Disp::Band,
        covered_by: "Phi6/Phi7 (k = 2 gives 5)",
    },
    Row {
        id: "F-h",
        window: Win::Fin,
        what: "absent instant: present empty vec![sample] vs empty Vector",
        frames: &["ClientAggState::finish", "ClientAggState::finish_folded"],
        site: "exec.rs ClientAggState::finish",
        disp: Disp::Band,
        covered_by: "Phi6/Phi7 (empty-present arm; no rows)",
    },
    Row {
        id: "F-j",
        window: Win::Fin,
        what: "non-absent fan_out label_groups vs fp_groups collects",
        frames: &["ClientAggState::finish", "ClientAggState::finish_folded"],
        site: "exec.rs ClientAggState::finish",
        disp: Disp::Band,
        covered_by: "Phi5 / Phi4 (0 each: empty maps reserve nothing)",
    },
    Row {
        id: "F-k",
        window: Win::Fin,
        what: "non-absent instant emit",
        frames: &["ClientAggState::finish", "ClientAggState::finish_folded"],
        site: "exec.rs ClientAggState::finish",
        disp: Disp::Band,
        covered_by: "Phi4/Phi5 (0)",
    },
    Row {
        id: "F-l",
        window: Win::Fin,
        what: "RangeSlideState finish prologue (flush early-return, cur None)",
        frames: &[
            "RangeSlideState::finish",
            "RangeSlideState::finish_in_place",
            "RangeSlideState::flush_collision",
        ],
        site: "exec.rs RangeSlideState::finish_in_place",
        disp: Disp::Nil,
        covered_by: "Phi1-Phi3",
    },
    Row {
        id: "F-m",
        window: Win::Fin,
        what: "is_absent routes to finish_absent (points + vec![series])",
        frames: &[
            "RangeSlideState::finish_in_place",
            "RangeSlideState::finish_absent",
        ],
        site: "exec.rs RangeSlideState::finish_absent",
        disp: Disp::Band,
        covered_by: "Phi3",
    },
    Row {
        id: "F-n",
        window: Win::Fin,
        what: "fan_out group loop vs series_out take",
        frames: &["RangeSlideState::finish_in_place"],
        site: "exec.rs RangeSlideState::finish_in_place",
        disp: Disp::Band,
        covered_by: "Phi2 / Phi1 (0 each)",
    },
    Row {
        id: "F-o",
        window: Win::Fin,
        what: "append_variant_label insert vs override arm; SKIPPED for an absent variant's synthetic series (adjudicated capture correction)",
        frames: &[
            "::append_variant_label",
            "VariantsAggState::finish_in_place",
        ],
        site: "exec.rs append_variant_label + the absent gate",
        disp: Disp::Band,
        covered_by: "Phi3/Phi6/Phi7 pin the absent SKIP (no append); the insert/override arms are pinned by the hermetic goldens",
    },
    Row {
        id: "F-p",
        window: Win::Fin,
        what: "vector_aggs empty: apply_vector_aggs SKIPPED",
        frames: &["VariantsAggState::finish_in_place"],
        site: "exec.rs VariantsAggState::finish_in_place",
        disp: Disp::Band,
        covered_by: "Phi1-Phi7 (skip arm, 0)",
    },
    Row {
        id: "F-q",
        window: Win::Fin,
        what: "pre-sized concat + per-variant staging vec (intercept)",
        frames: &["VariantsAggState::finish_in_place"],
        site: "exec.rs VariantsAggState::finish_in_place",
        disp: Disp::Nil,
        covered_by: "explicitly NOT gated: a push-growth regression is inside the residue",
    },
    Row {
        id: "F-r",
        window: Win::Fin,
        what: "range-only drop of __variant__-less series; instant keeps",
        frames: &["VariantsAggState::finish_in_place"],
        site: "exec.rs VariantsAggState::finish_in_place",
        disp: Disp::Nil,
        covered_by: "Phi3 (keep) + the instant/range asymmetry golden",
    },
    Row {
        id: "F-s",
        window: Win::Fin,
        what: "per-variant charge release (integer arithmetic)",
        frames: &["VariantsAggState::finish_in_place"],
        site: "exec.rs VariantsAggState::finish_in_place",
        disp: Disp::Nil,
        covered_by: "finish asserts charged == base",
    },
    Row {
        id: "F-t",
        window: Win::Fin,
        what: "the forwarding loop hands each sub-state the SAME slice",
        frames: &["VariantsAggState::push_rows"],
        site: "exec.rs VariantsAggState::push_rows",
        disp: Disp::Nil,
        covered_by: "Phi1-Phi7; row-bearing work is F-d's NOT-EXEC",
    },
    Row {
        id: "F-u",
        window: Win::Fin,
        what: "VariantsAggState::finish delegation + post-condition",
        frames: &["VariantsAggState::finish"],
        site: "exec.rs VariantsAggState::finish",
        disp: Disp::Nil,
        covered_by: "Phi1-Phi7",
    },
    Row {
        id: "F-w",
        window: Win::Fin,
        what: "apply_vector_aggs for an aggregation-bearing variant",
        frames: &["VariantsAggState::finish_in_place"],
        site: "exec.rs apply_vector_aggs",
        disp: Disp::R4(3),
        covered_by: "input bounded by AggCaps::divided(n).series; G2/G3 slopes",
    },
    Row {
        id: "F-z",
        window: Win::Fin,
        what: "instant sub-state narrowing refusal (issue #236 Part D)",
        frames: &["VariantsAggState::new"],
        site: "exec.rs VariantsAggState::new as_instant/ok_or_else",
        disp: Disp::Unreach,
        covered_by: "a stepped window routes to the Range arm above, and the witness cannot be minted from one",
    },
    Row {
        id: "F-y",
        window: Win::Fin,
        what: "one mutating group's cell drain (expanded / delta / sample sweep)",
        frames: &["RangeSlideState::drain_group"],
        site: "exec.rs RangeSlideState::drain_group",
        disp: Disp::NotExec,
        covered_by: "rows.is_empty() => no mutating group exists => never called (F-d's premise)",
    },
    Row {
        id: "F-x",
        window: Win::Fin,
        what: "issue #236 Part B fold containers (dense slots, live map)",
        frames: &["RangeSlideState::finish_in_place"],
        site: "exec.rs RangeSlideState::emit + VectorAggFold",
        disp: Disp::NotExec,
        covered_by: "run_variants never calls attach_fold; fold is None here",
    },
    Row {
        id: "F-y2",
        window: Win::Fin,
        what: "offset put back on the emitted grid (in-place, both arms)",
        frames: &["RangeSlideState::finish"],
        site: "client_agg.rs shift_emitted_points",
        disp: Disp::Nil,
        covered_by: "offset-free variants take the early return; the shift mutates points in place",
    },
];

/// The 15 delegating boundary callees (see [`Boundary`]).
static BOUNDARY_CALLEES: [Boundary; 13] = [
    Boundary {
        // Issue #249: the metric entrypoint became
        // `run_metric_into_with_sm`, which the metadata-FREE row reaches
        // through the same call with the shared empty context — one
        // implementation, so the boundary is one name.
        callee: ".run_metric_into_with_sm",
        rows: &["F-d"],
        disp: Disp::NotExec,
    },
    Boundary {
        callee: "check_surviving_error",
        rows: &["F-d"],
        disp: Disp::NotExec,
    },
    Boundary {
        callee: ".stage_member",
        rows: &["F-d"],
        disp: Disp::NotExec,
    },
    Boundary {
        callee: "charge_group_bytes",
        rows: &["F-d"],
        disp: Disp::NotExec,
    },
    Boundary {
        callee: "render_labels_json_sorted",
        rows: &["F-d"],
        disp: Disp::NotExec,
    },
    Boundary {
        callee: ".emit_at",
        rows: &["F-d"],
        disp: Disp::NotExec,
    },
    Boundary {
        callee: ".fan_out_sample",
        rows: &["F-d"],
        disp: Disp::NotExec,
    },
    Boundary {
        callee: ".load_group",
        rows: &["F-d"],
        disp: Disp::NotExec,
    },
    Boundary {
        callee: ".covering_k",
        rows: &["F-d", "F-y"],
        disp: Disp::NotExec,
    },
    Boundary {
        callee: "series_labels",
        rows: &["C-e"],
        disp: Disp::R4(1),
    },
    Boundary {
        callee: "stream_hash",
        rows: &["C-e"],
        disp: Disp::R4(1),
    },
    Boundary {
        callee: ".extended_with",
        rows: &["C-l"],
        disp: Disp::R4(2),
    },
    Boundary {
        callee: "apply_vector_aggs",
        rows: &["F-w"],
        disp: Disp::R4(3),
    },
];

fn render_table(win: Win) -> String {
    let mut out = String::from("| id | what | frames | site | disp | covered by |\n");
    for row in INVENTORY.iter().filter(|r| r.window == win) {
        for cell in [row.what, row.site, row.covered_by] {
            assert!(
                !cell.contains('|') && !cell.contains('\n'),
                "table cells must not contain `|` or newlines: {cell:?}"
            );
        }
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            row.id,
            row.what,
            row.frames.join(", "),
            row.site,
            row.disp.render(),
            row.covered_by
        ));
    }
    out
}

/// The module doc (`//!` lines of this very file, prefix stripped).
fn module_doc() -> String {
    include_str!("logql_variants_alloc.rs")
        .lines()
        .filter_map(|l| {
            l.strip_prefix("//!")
                .map(|r| r.strip_prefix(' ').unwrap_or(r))
        })
        .map(|l| format!("{l}\n"))
        .collect()
}

/// G4 — the closure + branch census (Δ13.4's eight assertions, plus the
/// Δ14.1 non-emptiness fix). What it does NOT catch is the enumerated
/// gap list G-1…G-8 at the bottom of this file's module doc; the claim
/// is bounded to structural table completeness plus the syntactic census
/// of the 26 frame bodies.
#[test]
fn g4_frame_census_and_inventory_closure() {
    let _serial = serialize();
    // (1) 26 unique frames, each resolving to exactly one item.
    assert_eq!(PER_VARIANT_FRAMES.len(), 34);
    let mut keys = BTreeSet::new();
    for f in &PER_VARIANT_FRAMES {
        assert!(
            keys.insert(frame_key(f)),
            "duplicate frame {}",
            frame_key(f)
        );
    }
    // (2) per-frame census == the pin.
    for f in &PER_VARIANT_FRAMES {
        let parsed = parse_source(f.file);
        let src = &parsed;
        let (branches, callees) = census_of(src, f);
        let pinned: BTreeSet<String> = f.callees.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            (branches, &callees),
            (f.branches, &pinned),
            "per-variant frame `{}` changed shape: classify the new branch/call \
             in the W-MEM inventory (BAND / NIL / NOT-EXEC / UNREACH / R4) and \
             update the pin in the same commit.\n  pinned: {} branches, {:?}\n  actual: {} branches, {:?}",
            frame_key(f),
            f.branches,
            pinned,
            branches,
            callees
        );
    }
    // (3) inventory size and per-window counts; unique ids.
    assert_eq!(INVENTORY.len(), 50);
    let count = |w: Win| INVENTORY.iter().filter(|r| r.window == w).count();
    assert_eq!(
        (count(Win::Plan), count(Win::Ctor), count(Win::Fin)),
        (13, 12, 25)
    );
    let mut ids = BTreeSet::new();
    for r in &INVENTORY {
        assert!(ids.insert(r.id), "duplicate inventory row {}", r.id);
    }
    // (4) three checks, not two (Δ14.1): non-emptiness, key validity,
    // bidirectional coverage — the first is what stops a row being
    // emptied while coverage still holds elsewhere.
    let mut covered: BTreeSet<&str> = BTreeSet::new();
    for row in &INVENTORY {
        assert!(
            !row.frames.is_empty(),
            "inventory row {} names no frame: every row must classify at least \
             one frame body",
            row.id
        );
        for key in row.frames {
            assert!(
                keys.contains(*key),
                "inventory row {} names unknown frame {key}",
                row.id
            );
            covered.insert(key);
        }
    }
    for key in &keys {
        assert!(
            covered.contains(key.as_str()),
            "frame {key} appears in no inventory row"
        );
    }
    // (5) exactly three R4 rows, indices i/ii/iii once each.
    let r4: Vec<(&str, u8)> = INVENTORY
        .iter()
        .filter_map(|r| match r.disp {
            Disp::R4(i) => Some((r.id, i)),
            _ => None,
        })
        .collect();
    assert_eq!(r4, vec![("C-e", 1), ("C-l", 2), ("F-w", 3)]);
    // (6) the boundary-callee closure: 15 entries; each name occurs in
    // ≥1 pinned callee set; its rows exist with the SAME disposition;
    // and every frame whose pinned set contains the name is listed in
    // one of those rows' frames.
    assert_eq!(BOUNDARY_CALLEES.len(), 13);
    for b in &BOUNDARY_CALLEES {
        let carriers: Vec<String> = PER_VARIANT_FRAMES
            .iter()
            .filter(|f| f.callees.contains(&b.callee))
            .map(frame_key)
            .collect();
        assert!(
            !carriers.is_empty(),
            "boundary callee {} occurs in no pinned set",
            b.callee
        );
        let mut row_frames: BTreeSet<&str> = BTreeSet::new();
        for rid in b.rows {
            let row = INVENTORY
                .iter()
                .find(|r| r.id == *rid)
                .unwrap_or_else(|| panic!("boundary {} names unknown row {rid}", b.callee));
            assert_eq!(
                row.disp, b.disp,
                "boundary {} disposition disagrees with row {rid}",
                b.callee
            );
            row_frames.extend(row.frames.iter().copied());
        }
        for carrier in &carriers {
            assert!(
                row_frames.contains(carrier.as_str()),
                "frame {carrier} calls boundary {} but is not in its rows' frames",
                b.callee
            );
        }
    }
    // (7) the module-doc tables are byte-identical to the rendering.
    let doc = module_doc();
    for win in [Win::Plan, Win::Ctor, Win::Fin] {
        let rendered = render_table(win);
        assert!(
            doc.contains(&rendered),
            "the module doc must carry this rendering verbatim (paste it):\n{rendered}"
        );
    }
    // (8) the per-row gate F-d/F-t delegate to still exists.
    assert!(
        include_str!("logql_pipeline_alloc.rs").contains("CLIENT_AGG_FLAT_BUDGET"),
        "the NOT-EXEC delegation target must exist"
    );
}

#[test]
#[ignore = "generator: prints the inventory renderings to paste into the module doc"]
fn zz_print_inventory_tables() {
    for (name, win) in [
        ("W_plan", Win::Plan),
        ("W_ctor", Win::Ctor),
        ("W_fin", Win::Fin),
    ] {
        println!("=== {name} ===");
        print!("{}", render_table(win));
    }
}
