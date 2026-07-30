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
//! | P-e | tail empty vs non-empty | ::build_variants_node | plan.rs variant_tail + try_new clone | BAND | G1a/G1d/G1e vs G1c |
//! | P-f | arity class success side (forbids vs requires unwrap) | ::build_variants_node | plan.rs build_variants_node arity gates | BAND | G1a vs G1c |
//! | P-g | ClientValue Count / Bytes / Unwrap | ::build_variants_node | plan.rs build_variants_node value arm | BAND | G1a / G1f / G1c |
//! | P-h | quantile parameter parse | ::build_variants_node | plan.rs build_variants_node quantile arm | BAND | G1h |
//! | P-i | absent vs non-absent absent_labels | ::build_variants_node | plan.rs VariantSpec::try_new absent arm | BAND | G1e vs every other plan fixture |
//! | P-j | parse_plan_number success path (format! only on Err) | ::parse_plan_number | plan.rs parse_plan_number | NIL | executes under G1a/G1h |
//! | P-k | the reused raw-handle buffer (one growth, intercept) | ::unwrap_vector_aggs_into | plan.rs unwrap_vector_aggs_into | BAND | G1a upper band; G1d pins the 0-layer shape at 0 |
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
//! | F-b | ClientAggState::push_rows prologue (Vec::new scratch) | ClientAggState::push_rows | exec.rs ClientAggState::push_rows | NIL | Phi4-Phi7 |
//! | F-c | RangeSlideState::push_rows prologue/epilogue (mem::take) | RangeSlideState::push_rows | exec.rs RangeSlideState::push_rows | NIL | Phi1-Phi3 |
//! | F-d | the row loops and everything reachable only from them | ClientAggState::push_rows, RangeSlideState::push_rows, RangeSlideState::flush_collision, FpSlide::finish | exec.rs row paths | NOT-EXEC | rows.is_empty(); the existing CLIENT_AGG_FLAT_BUDGET per-row gate |
//! | F-e | MetricAggState::finish kind dispatch (Box move) | MetricAggState::finish | exec.rs MetricAggState::finish | NIL | Phi1/Phi4 |
//! | F-f | ClientAggState::finish absent vs non-absent | ClientAggState::finish | exec.rs ClientAggState::finish | BAND | Phi6/Phi7 vs Phi4/Phi5 |
//! | F-g | absent: the finish-time absent_labels clone (1 + 2k) | ClientAggState::finish | exec.rs ClientAggState::finish | BAND | Phi6/Phi7 (k = 2 gives 5) |
//! | F-h | absent instant: present empty vec![sample] vs empty Vector | ClientAggState::finish | exec.rs ClientAggState::finish | BAND | Phi6/Phi7 (empty-present arm; no rows) |
//! | F-i | absent RANGE arm of ClientAggState::finish (bucket_grid) | ClientAggState::finish | exec.rs ClientAggState::finish | UNREACH | Instant states are built only for instant windows; a routing change breaks Phi1/Phi3's RangeSlideState-derived constants |
//! | F-j | non-absent fan_out label_groups vs fp_groups collects | ClientAggState::finish | exec.rs ClientAggState::finish | BAND | Phi5 / Phi4 (0 each: empty maps reserve nothing) |
//! | F-k | non-absent instant emit | ClientAggState::finish | exec.rs ClientAggState::finish | BAND | Phi4/Phi5 (0) |
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
//! | F-v | non-absent RANGE emit arm of ClientAggState::finish | ClientAggState::finish | exec.rs ClientAggState::finish | UNREACH | same routing citation as F-i |
//! | F-w | apply_vector_aggs for an aggregation-bearing variant | VariantsAggState::finish_in_place | exec.rs apply_vector_aggs | R4 (iii) | input bounded by AggCaps::divided(n).series; G2/G3 slopes |
//! | F-y | one mutating group's cell drain (expanded / delta / sample sweep) | RangeSlideState::drain_group | exec.rs RangeSlideState::drain_group | NOT-EXEC | rows.is_empty() => no mutating group exists => never called (F-d's premise) |
//! | F-x | issue #236 Part B fold containers (dense slots, live map) | RangeSlideState::finish_in_place | exec.rs RangeSlideState::emit + VectorAggFold | NOT-EXEC | run_variants never calls attach_fold; fold is None here |
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
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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
static MEASURING: AtomicBool = AtomicBool::new(false);

/// The counters are process-global: every test in this binary serializes
/// on this lock so no parallel test's allocations pollute a measured
/// window (the single-`#[test]` precedent, expressed as a lock because
/// G4 is a second, allocation-heavy test in the same binary).
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct TripleCounterAlloc;

fn on_alloc(size: u64) {
    if MEASURING.load(Ordering::Relaxed) {
        TOTAL_BYTES.fetch_add(size, Ordering::Relaxed);
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        let now = LIVE.fetch_add(size, Ordering::Relaxed) + size;
        PEAK.fetch_max(now, Ordering::Relaxed);
    }
}

fn on_dealloc(size: u64) {
    if MEASURING.load(Ordering::Relaxed) {
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
// effects are relaxed atomic updates (gated by `MEASURING`) which
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
fn measured<T>(f: impl FnOnce() -> T) -> (u64, u64, u64, T) {
    TOTAL_BYTES.store(0, Ordering::SeqCst);
    ALLOC_CALLS.store(0, Ordering::SeqCst);
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    MEASURING.store(true, Ordering::SeqCst);
    let out = f();
    MEASURING.store(false, Ordering::SeqCst);
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
    match plan(&expr, params, &ctx()).expect("plan") {
        Plan::MetricBinary(MetricNode::Variants {
            scan,
            variants,
            spec_bytes,
        }) => (*scan, variants, spec_bytes),
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
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // --- G0: instrument-validity control (direction-neutral). The new
    // instrument SEES a dropped transient; the v6 retained quantity
    // cannot.
    let n_ctl: u64 = 64;
    let live_at_open = LIVE.load(Ordering::SeqCst);
    let (calls, bytes, _, _) = measured(|| {
        for i in 0..n_ctl {
            let s = format!("transient-{i}");
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
    assert_eq!(
        live_at_close.saturating_sub(live_at_open),
        0,
        "G0: the retained-delta quantity is blind to transients — which is \
         exactly why it is no longer a decision gate"
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

    // --- G1b: W_plan byte slope ≤ the spec charge slope (the soundness
    // half — 2–6× model slack, catches only LARGE uncharged work).
    {
        let expr1 = parse(&f_rich(1)).expect("parse");
        let expr64 = parse(&f_rich(64)).expect("parse");
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
            "G1b: plan-time byte slope {} exceeds the charged spec slope {}",
            bytes64.saturating_sub(bytes1),
            sb64 - sb1
        );
    }

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
        "exec.rs" => include_str!("../src/logql/exec.rs"),
        "plan.rs" => include_str!("../src/logql/plan.rs"),
        other => panic!("unknown frame file {other}"),
    };
    syn::parse_file(src).expect("the frame source parses")
}

#[test]
#[ignore = "generator: prints the frame censuses to pin"]
fn zz_print_frame_censuses() {
    let exec = parse_source("exec.rs");
    let plan_f = parse_source("plan.rs");
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
        let src = if file == "exec.rs" { &exec } else { &plan_f };
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
static PER_VARIANT_FRAMES: [Frame; 27] = [
    // --- W_plan (4) ---
    Frame {
        file: "plan.rs",
        ty: None,
        anchor: "build_variants_node",
        branches: 26,
        callees: &[
            ".any",
            ".as_nanos",
            ".as_u64",
            ".clone",
            ".enumerate",
            ".is_some",
            ".iter",
            ".len",
            ".map_err",
            ".push",
            ".to_string",
            ".to_vec",
            "Err",
            "Ok",
            "QueryTooBroad",
            "Some",
            "Unwrap",
            "charge_fanout_bytes",
            "common_stages",
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
        branches: 1,
        callees: &[".as_deref", ".as_ref", ".clear", ".push"],
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
        file: "exec.rs",
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
        file: "exec.rs",
        ty: Some("VariantsAggState"),
        anchor: "new",
        branches: 11,
        callees: &[
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
            ".push",
            ".rate_window_ns",
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
        file: "exec.rs",
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
        file: "exec.rs",
        ty: None,
        anchor: "stage_source_bytes",
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
            "label_filter_bytes",
            "matcher_bytes",
        ],
    },
    Frame {
        file: "exec.rs",
        ty: None,
        anchor: "regex_stage_count",
        branches: 6,
        callees: &[
            ".alternatives",
            ".as_ref",
            ".fold",
            ".is_some_and",
            ".iter",
            ".saturating_add",
            "from",
            "label_filter_regexes",
            "matches!",
        ],
    },
    Frame {
        file: "exec.rs",
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
        file: "exec.rs",
        ty: Some("ClientAggState"),
        anchor: "new",
        branches: 3,
        callees: &[
            ".as_u64",
            ".end_ns",
            ".insert",
            ".map",
            ".metric_mutates_labels",
            ".start_ns",
            ".step_ns",
            "Ok",
            "ensure_grid_resolution",
            "new",
            "series_labels",
        ],
    },
    Frame {
        file: "exec.rs",
        ty: Some("RangeSlideState"),
        anchor: "new",
        branches: 6,
        callees: &[
            ".as_u64",
            ".clone",
            ".get",
            ".insert",
            ".metric_mutates_labels",
            "Ok",
            "ensure_grid_resolution",
            "matches!",
            "max?",
            "new",
            "reducer_class",
            "retention_points_per_sample",
            "series_labels",
            "stream_hash",
            "unreachable!",
            "vec!",
        ],
    },
    // --- W_fin (14) ---
    Frame {
        file: "exec.rs",
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
        file: "exec.rs",
        ty: Some("VariantsAggState"),
        anchor: "finish",
        branches: 1,
        callees: &[".finish_in_place", "Ok", "debug_assert_eq!"],
    },
    Frame {
        file: "exec.rs",
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
        branches: 17,
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
        file: "exec.rs",
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
        file: "exec.rs",
        ty: Some("MetricAggState"),
        anchor: "push_rows",
        branches: 1,
        callees: &[".push_rows"],
    },
    Frame {
        file: "exec.rs",
        ty: Some("MetricAggState"),
        anchor: "finish",
        branches: 1,
        callees: &[".finish", "Ok"],
    },
    Frame {
        file: "exec.rs",
        ty: Some("ClientAggState"),
        anchor: "push_rows",
        // 21st branch: issue #230's `?` on the now-fallible
        // `run_metric_into` (template render-budget breach → the
        // bounded 422). Row-path only — covered by F-d's NOT-EXEC
        // disposition (rows.is_empty() in every W_fin window).
        branches: 21,
        callees: &[
            ".add",
            ".as_u64",
            ".collect",
            ".contains_key",
            ".entry",
            ".get",
            ".get_mut",
            ".insert",
            ".into_mut",
            ".iter",
            ".key",
            ".len",
            ".map",
            ".or_default",
            ".run_metric_into",
            ".sort_unstable",
            ".step_ns",
            ".to_string",
            "Err",
            "Ok",
            "QueryTooBroad",
            "bucket_of",
            "charge_group_bytes",
            "check_surviving_error",
            "group_entry_bytes",
            "matches!",
            "new",
            "render_labels_json_sorted",
        ],
    },
    Frame {
        file: "exec.rs",
        ty: Some("ClientAggState"),
        anchor: "finish",
        // Issue #236 P1: the non-mutating `fp_groups` arm gained its
        // discharge leg, whose `base_labels.get(&fp)?` adds one
        // `syn::ExprTry` branch (7 → 8) and the `Some(...)` callee.
        // Regenerated with `zz_print_frame_censuses`. W-MEM disposition:
        // **NIL** — the `?` replaces the previous `.map(...)` on the same
        // `Option`, so the arm's allocation shape is unchanged (one
        // `labels.clone()` per surviving group, exactly as before); the
        // added work is one integer subtraction per group.
        branches: 8,
        callees: &[
            ".as_u64",
            ".clone",
            ".collect",
            ".contains",
            ".end_ns",
            ".filter",
            ".filter_map",
            ".finish",
            ".get",
            ".into_iter",
            ".is_empty",
            ".is_none",
            ".map",
            ".remove",
            ".start_ns",
            ".step_ns",
            ".unwrap_or",
            "Some",
            "Matrix",
            "Vector",
            "bucket_grid",
            "debug_assert_eq!",
            "discharge_group_bytes",
            "group_entry_bytes",
            "matches!",
            "new",
            "vec!",
        ],
    },
    Frame {
        file: "exec.rs",
        ty: Some("RangeSlideState"),
        anchor: "push_rows",
        branches: 8,
        // `.into` joined with issue #230: the render-budget breach
        // (`TemplateBudgetExceeded`) converts into `ReadError` on the
        // row path — F-d's NOT-EXEC disposition covers it.
        callees: &[
            ".flush_collision",
            ".get",
            ".into",
            ".len",
            ".run_metric_into",
            ".stage_member",
            "Err",
            "Ok",
            "check_surviving_error",
            "new",
            "take",
        ],
    },
    Frame {
        file: "exec.rs",
        ty: Some("RangeSlideState"),
        anchor: "finish",
        branches: 1,
        callees: &[".finish_in_place", "Ok", "debug_assert_eq!"],
    },
    Frame {
        file: "exec.rs",
        ty: Some("RangeSlideState"),
        anchor: "finish_in_place",
        // Issue #236 P2: the slider-retirement `if let` moved into
        // `rotate_slider`, and the non-mutating tail gained a discharge
        // loop over `series_out` (11 -> 12).
        //
        // Issue #236 Part B: the fan-out arm sorts its groups by label
        // set and routes each either to the fold or to `out`; both arms
        // finish through the fold, and the absent/non-mutating tails
        // route through `.emit`.
        //
        // Issue #236 Part C: the three-arm cell drain moved out to
        // `RangeSlideState::drain_group` (its own frame below, so the
        // census still reads the body rather than losing it behind a
        // delegating callee) — 15 -> 10 branches here. Regenerated with
        // `zz_print_frame_censuses`. W-MEM disposition: **NOT-EXEC**
        // (row F-x) for the fold arms; the group sort is **NIL** (an
        // in-place sort of a `Vec` the loop was going to drain anyway).
        branches: 10,
        callees: &[
            ".as_mut",
            ".cmp",
            ".collect",
            ".drain_group",
            ".emit",
            ".finish",
            ".finish_absent",
            ".flush_collision",
            ".into_iter",
            ".is_empty",
            ".push",
            ".push_series",
            ".rotate_slider",
            ".sort_by",
            ".take",
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
        file: "exec.rs",
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
        file: "exec.rs",
        ty: Some("RangeSlideState"),
        anchor: "finish_absent",
        // Issue #236 Part B: returns `Vec<MatrixSeries>` instead of a
        // `QueryResult` so the caller can route it through the fold like
        // every other emit path, so `Matrix` leaves this frame (7 → 6
        // callees, branch count unchanged). Regenerated with
        // `zz_print_frame_censuses`. W-MEM disposition: **BAND**,
        // unchanged — row F-m already prices the points vector and the
        // one-element `vec![series]`, and both are byte-identical.
        branches: 3,
        callees: &[".grid_point", ".is_empty", ".push", "new", "take", "vec!"],
    },
    Frame {
        file: "exec.rs",
        ty: Some("RangeSlideState"),
        anchor: "flush_collision",
        // Issue #236: Part A deleted the `series_count > caps.series`
        // rejection (so `Err`/`QueryTooBroad`/`.push` leave this frame);
        // P2 put a `charge_group_bytes(group_entry_bytes(...))?` in its
        // place before the `labels` clone, and the slider retirement moved
        // into `.rotate_slider`. Branch count is unchanged at 12 (one
        // rejection `if` traded for one charge `?`). Regenerated with
        // `zz_print_frame_censuses`. W-MEM disposition: **NIL** — the
        // charge is integer arithmetic over an already-materialised label
        // set, on the same once-per-fingerprint path the deleted check
        // occupied.
        branches: 12,
        callees: &[
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
            ".load_group",
            ".rotate_slider",
            ".sort_by",
            ".take",
            ".unwrap_or",
            ".unwrap_or_default",
            "Ok",
            "Some",
            "charge_group_bytes",
            "group_entry_bytes",
            "new",
            "take",
        ],
    },
    Frame {
        file: "exec.rs",
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
];

/// The complete op/shape-conditional branch inventory: 11 W_plan + 12
/// W_ctor + 23 W_fin = 46 rows, each with exactly ONE disposition. The
/// module-doc tables are a RENDERING of this const (assertion 7), never
/// the source.
static INVENTORY: [Row; 48] = [
    // --- W_plan (11) ---
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
    Row {
        id: "P-e",
        window: Win::Plan,
        what: "tail empty vs non-empty",
        frames: &["::build_variants_node"],
        site: "plan.rs variant_tail + try_new clone",
        disp: Disp::Band,
        covered_by: "G1a/G1d/G1e vs G1c",
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
        frames: &["ClientAggState::push_rows"],
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
        frames: &["ClientAggState::finish"],
        site: "exec.rs ClientAggState::finish",
        disp: Disp::Band,
        covered_by: "Phi6/Phi7 vs Phi4/Phi5",
    },
    Row {
        id: "F-g",
        window: Win::Fin,
        what: "absent: the finish-time absent_labels clone (1 + 2k)",
        frames: &["ClientAggState::finish"],
        site: "exec.rs ClientAggState::finish",
        disp: Disp::Band,
        covered_by: "Phi6/Phi7 (k = 2 gives 5)",
    },
    Row {
        id: "F-h",
        window: Win::Fin,
        what: "absent instant: present empty vec![sample] vs empty Vector",
        frames: &["ClientAggState::finish"],
        site: "exec.rs ClientAggState::finish",
        disp: Disp::Band,
        covered_by: "Phi6/Phi7 (empty-present arm; no rows)",
    },
    Row {
        id: "F-i",
        window: Win::Fin,
        what: "absent RANGE arm of ClientAggState::finish (bucket_grid)",
        frames: &["ClientAggState::finish"],
        site: "exec.rs ClientAggState::finish",
        disp: Disp::Unreach,
        covered_by: "Instant states are built only for instant windows; a routing change breaks Phi1/Phi3's RangeSlideState-derived constants",
    },
    Row {
        id: "F-j",
        window: Win::Fin,
        what: "non-absent fan_out label_groups vs fp_groups collects",
        frames: &["ClientAggState::finish"],
        site: "exec.rs ClientAggState::finish",
        disp: Disp::Band,
        covered_by: "Phi5 / Phi4 (0 each: empty maps reserve nothing)",
    },
    Row {
        id: "F-k",
        window: Win::Fin,
        what: "non-absent instant emit",
        frames: &["ClientAggState::finish"],
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
        id: "F-v",
        window: Win::Fin,
        what: "non-absent RANGE emit arm of ClientAggState::finish",
        frames: &["ClientAggState::finish"],
        site: "exec.rs ClientAggState::finish",
        disp: Disp::Unreach,
        covered_by: "same routing citation as F-i",
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
];

/// The 15 delegating boundary callees (see [`Boundary`]).
static BOUNDARY_CALLEES: [Boundary; 15] = [
    Boundary {
        callee: ".run_metric_into",
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
        callee: "bucket_of",
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
        callee: "bucket_grid",
        rows: &["F-i"],
        disp: Disp::Unreach,
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
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let exec = parse_source("exec.rs");
    let plan_src = parse_source("plan.rs");
    // (1) 26 unique frames, each resolving to exactly one item.
    assert_eq!(PER_VARIANT_FRAMES.len(), 27);
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
        let src = if f.file == "exec.rs" {
            &exec
        } else {
            &plan_src
        };
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
    assert_eq!(INVENTORY.len(), 48);
    let count = |w: Win| INVENTORY.iter().filter(|r| r.window == w).count();
    assert_eq!(
        (count(Win::Plan), count(Win::Ctor), count(Win::Fin)),
        (11, 12, 25)
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
    assert_eq!(BOUNDARY_CALLEES.len(), 15);
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
