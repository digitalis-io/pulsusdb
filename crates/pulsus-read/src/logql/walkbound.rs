//! The iterative walkers' memory bound (issue #272).
//!
//! Flattening the recursive LogQL walks moves what used to be machine
//! stack onto the heap, and that growth is a per-query allocation. This
//! module carries the chain that bounds it, in the shape of
//! [`crate::querytext`]'s sizing doc.
//!
//! **The accounting unit, stated once:** bytes **requested from the
//! global allocator** (`Layout::size()`), which is exactly what the
//! byte-counting allocator in `tests/logql_walk_bytes.rs` sums. Every
//! charge is an upper bound on that quantity, computed **before** the
//! request is made, in release as in debug. **No invariant in this
//! issue's code is enforced by a `debug_assert`** — a `debug_assert` is a
//! comment that runs in CI, and release builds serve the queries.
//!
//! # L1 — every allocation is charged before it is requested
//!
//! [`pulsus_logql::walk::ChunkStack::next_push_bytes`] is a conservative
//! upper bound on the next push's request, computed from values known
//! before the call: one element chunk (`WALK_CHUNK_ITEMS *
//! size_of::<T>()`) plus, when the chunk index grows, `2 x` the capacity
//! this type *chose* (`reserve_exact`, `0 -> INDEX_INIT`, else
//! `cap -> 2*cap`). Charging `2 x new_cap` against a true transient of
//! `old + new = 1.5 x new_cap` leaves >= 33 % headroom, which is what
//! makes the bound hold in release without checking the capacity the
//! allocator actually returned. **push** charges then allocates, **pop**
//! releases nothing (chunks are retained, so the charge is
//! cumulative-peak, not live). The RAII wrapper that carries that state
//! machine in production arrives with its only consumer, #293's
//! `try_dfs`; this issue's Leg B charges two exactly-sized reservations
//! and needs no release.
//!
//! # L2 — `N <= query.len() + 1`, structurally
//!
//! Every `MetricExpr`/`VariantsExpr`/`LabelFilterExpr` node is
//! constructed by consuming at least one token; every token consumes at
//! least one input byte; the token vector ends with exactly one `Eof`
//! (`crates/pulsus-logql/src/parser.rs`, `parse` -> `expect_eof`; the
//! `Cursor` doc states "Tokens always end with `Eof`").
//!
//! # L3 — [`WALK_BYTES_PER_NODE`] is a cumulative SUM, not a max
//!
//! One node can be simultaneously live in more than one structure, so the
//! per-node figure adds every stack entry a single node can occupy rather
//! than taking the largest. See the constant's own documentation for the
//! term-by-term breakdown.
//!
//! # L4 — the cap is derived, not chosen
//!
//! [`MAX_LOGQL_WALK_TRANSIENT_BYTES`] is `2 x
//! walk_transient_bound(REFERENCE_MAX_QUERY_BYTES)`, with `const`
//! assertions that it admits the largest query the reference itself
//! accepts and stays under 512 MiB. `REFERENCE_MAX_QUERY_BYTES` is
//! **imported, never restated** — it is `pulsus_logql::MAX_QUERY_BYTES`,
//! owned by #279.
//!
//! # L5 — two legs
//!
//! *Leg A* is [`admit_logql_walk`], an O(1) `str::len()` check covering
//! every walk whose signature cannot carry a budget or return a refusal
//! (`Drop`, `Clone`, `PartialEq`, `Hash`, `Debug` in both modes,
//! `Display`, `leaves()`, `produces_series()`). It is called
//! **immediately AFTER `pulsus_logql::parse`/`parse_selector` returns
//! `Ok`**, on input the reference's own cap has already admitted.
//!
//! **Leg A is EXECUTION-DEAD through the HTTP surface**, and that is
//! deliberate: #279 rejects every input at or above
//! `pulsus_logql::MAX_QUERY_BYTES` with the reference's own 400
//! `bad_data`, and Leg A's threshold has always sat above
//! `2 x MAX_QUERY_BYTES`, so no query `parse` accepts can reach the
//! refusal. It is kept for the `const` proof below and for any future
//! caller that does not route through `parse` — **not** for the runtime
//! path. Do not delete it for looking unused.
//!
//! *Leg B* is [`WalkBudget`], on the one walk in this issue whose error
//! type is already `ReadError`: `exec::LogQlEngine::run_metric_node`,
//! which reserves its post-order node list and its value stack at exact,
//! pre-computed sizes and charges both before reserving. A breach is
//! [`TooBroadReason::WalkTransientBytes`], which the existing wildcard
//! maps to 422 `query_too_broad`.
//!
//! # What this issue does NOT close, stated plainly
//!
//! `plan::build_metric_node` is the one walk #272 leaves recursive (by
//! ruling: converting it needs **#293**'s `Step::Only(SpineTo)`). A flat
//! `rate(…) or rate(…) or …` chain still aborts the process inside
//! `plan()` — measured on a 2 MiB stack, release: ok at 1,200 terms /
//! 44,486 bytes, **abort at 1,250** — which is inside the query-text cap,
//! so neither that cap nor the bound below prevents it. Tracked in #293
//! and #285. Nothing in this module should be read as closing it.
//!
//! # Not charged, stated plainly
//!
//! The `MetricExpr`/`MetricNode` trees themselves — `O(query text)` heap
//! with no budget, exactly as before this issue. The heap *behind* a
//! `QueryResult` value-stack entry is #245; SQL-path leaf
//! materialisation is #257; concurrency composition of a per-query
//! ceiling is #260; variants fan-out state is #221's own budget.

use std::mem::size_of;

use pulsus_logql::walk::{INDEX_INIT, WALK_ALLOC_ENVELOPE_BYTES, WALK_CHUNK_ITEMS};

use super::error::{ReadError, TooBroadReason};
use super::exec::QueryResult;
use super::plan::MetricNode;

/// The reference's LogQL query-text cap, **owned by #279 and imported,
/// never restated**: `pulsus_logql::MAX_QUERY_BYTES`
/// (`crates/pulsus-logql/src/limits.rs`; grafana/loki v3.7.4
/// `pkg/logql/syntax/parser.go`).
///
/// The bound is an **exclusive** maximum — `CheckedQuery::new` rejects on
/// `len >= MAX_QUERY_BYTES` — so the longest query the reference (and
/// PulsusDB) accepts is `MAX_QUERY_BYTES - 1`. This alias deliberately
/// uses the inclusive value, which is one byte conservative.
pub const REFERENCE_MAX_QUERY_BYTES: u64 = pulsus_logql::MAX_QUERY_BYTES as u64;

/// The cumulative index charge attributable to one node: the chunk index
/// grows by doubling from [`INDEX_INIT`], so its cumulative charge over a
/// stack of `C` chunks is `~4 * C * size_of::<Vec<T>>()`, i.e. this much
/// per element.
pub const WALK_INDEX_BYTES_PER_NODE: u64 =
    (4 * size_of::<Vec<usize>>() as u64).div_ceil(WALK_CHUNK_ITEMS as u64);

/// The per-allocation envelope amortised over a chunk's elements.
pub const WALK_ALLOC_OVERHEAD_PER_NODE: u64 =
    (WALK_ALLOC_ENVELOPE_BYTES as u64).div_ceil(WALK_CHUNK_ITEMS as u64);

/// The largest `ChunkStack` element any walk in this issue instantiates,
/// **derived** from `size_of` rather than chosen. A widening element type
/// raises it automatically, and the pinned ceiling below then fails the
/// **build** rather than letting the bound loosen silently.
pub const MAX_WALK_ENTRY_BYTES: u64 = max_of(&[
    size_of::<pulsus_logql::MeRef<'static>>() as u64,
    size_of::<&'static MetricNode>() as u64,
    size_of::<pulsus_logql::MeVal>() as u64,
    size_of::<MetricNode>() as u64,
    size_of::<(pulsus_logql::MeRef<'static>, u32, usize)>() as u64,
    2 * size_of::<pulsus_logql::MeRef<'static>>() as u64,
]);

/// The ceiling `MAX_WALK_ENTRY_BYTES` must stay under.
///
/// **No measured figure is repeated here.** The largest entry is
/// `MeVal`, whose larger arm is a `MetricExpr`; its size is asserted in
/// exactly one place — `the_fixed_slack_is_derived_from_the_measured_maximum_entry`
/// below — so no comment can drift away from the arithmetic it
/// describes. `WALK_FIXED_SLACK_BYTES` is computed from
/// `MAX_WALK_ENTRY_BYTES`, and that test re-derives it from the same
/// constant.
pub const WALK_ENTRY_CEILING_BYTES: u64 = 192;

const fn max_of(xs: &[u64]) -> u64 {
    let mut best = 0;
    let mut i = 0;
    while i < xs.len() {
        if xs[i] > best {
            best = xs[i];
        }
        i += 1;
    }
    best
}

/// Bytes one AST/plan node can put on the heap, **summed** across every
/// structure it can occupy at once (L3):
///
/// | term | what |
/// |---|---|
/// | `size_of::<MeRef>()` | SCC-2 pre/post-order work-stack entry |
/// | `size_of::<&MetricNode>()` | SCC-3 work-stack entry |
/// | `size_of::<&MetricNode>()` | `metric_node_postorder`'s node list |
/// | `size_of::<QueryResult>()` | `run_metric_node`'s value-stack entry |
/// | `size_of::<MeVal>()` | SCC-2 dismantle/clone value |
/// | `size_of::<MetricNode>()` | SCC-3 dismantle/clone value |
/// | `2 * size_of::<MeRef>()` | `PartialEq`'s pair-stack entry |
/// | `size_of::<(MeRef, u32, usize)>()` | the largest emitter step frame |
/// | [`WALK_INDEX_BYTES_PER_NODE`] | the amortised chunk index |
/// | [`WALK_ALLOC_OVERHEAD_PER_NODE`] | the amortised allocation envelope |
///
/// Wave 2 adds SCC-1's and SCC-4's own terms (`LfOp`, the label-filter
/// pair stack); this constant grows with them, and every `const`
/// assertion below re-derives rather than being re-chosen.
pub const WALK_BYTES_PER_NODE: u64 = size_of::<pulsus_logql::MeRef<'static>>() as u64
    + size_of::<&'static MetricNode>() as u64
    + size_of::<&'static MetricNode>() as u64
    + size_of::<QueryResult>() as u64
    + size_of::<pulsus_logql::MeVal>() as u64
    + size_of::<MetricNode>() as u64
    + 2 * size_of::<pulsus_logql::MeRef<'static>>() as u64
    + size_of::<(pulsus_logql::MeRef<'static>, u32, usize)>() as u64
    + WALK_INDEX_BYTES_PER_NODE
    + WALK_ALLOC_OVERHEAD_PER_NODE;

/// Fixed per-query slack: the first chunk of each of the four stacks a
/// single query can hold live at once, plus their first index buffers.
/// Independent of `N`, so it is an intercept rather than a slope.
pub const WALK_FIXED_SLACK_BYTES: u64 = 4
    * (WALK_CHUNK_ITEMS as u64 * MAX_WALK_ENTRY_BYTES
        + INDEX_INIT as u64 * size_of::<Vec<usize>>() as u64
        + WALK_ALLOC_ENVELOPE_BYTES as u64);

/// The transient heap an `N <= query_bytes + 1`-node walk can request.
/// Affine and non-decreasing in `query_bytes` — clause 3b sweeps the
/// whole reachable domain to keep it so.
pub const fn walk_transient_bound(query_bytes: u64) -> u64 {
    WALK_BYTES_PER_NODE.saturating_mul(query_bytes.saturating_add(1)) + WALK_FIXED_SLACK_BYTES
}

/// The refusal threshold: twice the bound for the largest query the
/// reference itself accepts (L4).
pub const MAX_LOGQL_WALK_TRANSIENT_BYTES: u64 = 2 * walk_transient_bound(REFERENCE_MAX_QUERY_BYTES);

// AC 6 clause 3c — unreachability is compile-time. With clause 3b's
// monotonicity these give: `len < MAX_QUERY_BYTES` implies
// `walk_transient_bound(len) <= walk_transient_bound(REFERENCE_MAX_QUERY_BYTES)
//  < MAX_LOGQL_WALK_TRANSIENT_BYTES`, so no query `parse` accepts can be
// refused by Leg A.
const _: () = assert!(walk_transient_bound(REFERENCE_MAX_QUERY_BYTES) > 0);
const _: () =
    assert!(walk_transient_bound(REFERENCE_MAX_QUERY_BYTES) < MAX_LOGQL_WALK_TRANSIENT_BYTES);
// AC 6 — the cap admits the reference's own largest query and stays well
// under half a gigabyte.
const _: () =
    assert!(MAX_LOGQL_WALK_TRANSIENT_BYTES >= walk_transient_bound(REFERENCE_MAX_QUERY_BYTES));
const _: () = assert!(MAX_LOGQL_WALK_TRANSIENT_BYTES < 512 * 1024 * 1024);
// A widening `ChunkStack` element must fail the BUILD, not loosen the
// bound silently.
const _: () = assert!(MAX_WALK_ENTRY_BYTES <= WALK_ENTRY_CEILING_BYTES);
const _: () = assert!(MAX_WALK_ENTRY_BYTES > 0);

/// Leg A: refuses a query whose walks could request more transient heap
/// than [`MAX_LOGQL_WALK_TRANSIENT_BYTES`]. O(1) — one `str::len()`, no
/// allocation, no round-trip.
///
/// **EXECUTION-DEAD through the HTTP surface, deliberately.** Callers run
/// it *after* `pulsus_logql::parse` has returned `Ok`, and #279 rejects
/// everything at or above `pulsus_logql::MAX_QUERY_BYTES` with the
/// reference's own 400 `bad_data`. Leg A's threshold has always sat above
/// `2 x MAX_QUERY_BYTES`, so no parse-accepted query can reach the
/// refusal below. **It is kept for the `const` proof in this module (AC 6
/// clause 3c) and for any future caller that does not route through
/// `parse` — not for the runtime path. Do not delete it for looking
/// unused.**
pub fn admit_logql_walk(query: &str) -> Result<(), TooBroadReason> {
    let estimate = walk_transient_bound(query.len() as u64);
    if estimate > MAX_LOGQL_WALK_TRANSIENT_BYTES {
        return Err(TooBroadReason::WalkTransientBytes {
            estimate,
            cap: MAX_LOGQL_WALK_TRANSIENT_BYTES,
        });
    }
    Ok(())
}

/// Leg B's counter. A plain `&mut` counter — **not** an RAII ledger and
/// **not** shared. #245 defines its own query-scoped result budget;
/// unification, if it is ever warranted, is #260's.
#[derive(Debug)]
pub(crate) struct WalkBudget {
    charged: u64,
    cap: u64,
}

impl WalkBudget {
    pub(crate) fn new() -> Self {
        WalkBudget {
            charged: 0,
            cap: MAX_LOGQL_WALK_TRANSIENT_BYTES,
        }
    }

    /// Check-then-add: a **failed charge does not mutate**, so a refusal
    /// leaves the counter usable and the reported estimate is the one
    /// that breached.
    pub(crate) fn charge(&mut self, bytes: usize) -> Result<(), ReadError> {
        let next = self.charged.saturating_add(bytes as u64);
        if next > self.cap {
            return Err(ReadError::QueryTooBroad(
                TooBroadReason::WalkTransientBytes {
                    estimate: next,
                    cap: self.cap,
                },
            ));
        }
        self.charged = next;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn charged(&self) -> u64 {
        self.charged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bound_is_affine_and_non_decreasing_over_the_reachable_domain() {
        // AC 6 clause 3b: 131,073 runtime comparisons / 262,146 calls to
        // `walk_transient_bound`, the largest argument being 131,073 —
        // one past the exclusive cap, so the rejection boundary is
        // covered. This is the lemma clause 3c's unreachability rests on.
        for n in 0..=REFERENCE_MAX_QUERY_BYTES {
            assert!(
                walk_transient_bound(n) <= walk_transient_bound(n + 1),
                "the bound dips at n = {n}"
            );
        }
    }

    #[test]
    fn admission_accepts_the_longest_query_the_reference_accepts() {
        // AC 6 clause 3a: the implementation half.
        let longest = "a".repeat(REFERENCE_MAX_QUERY_BYTES as usize - 1);
        assert!(admit_logql_walk(&longest).is_ok());
    }

    #[test]
    fn admission_refuses_a_query_far_past_the_walk_threshold() {
        let over = "a".repeat(4 * REFERENCE_MAX_QUERY_BYTES as usize);
        let err = admit_logql_walk(&over).expect_err("4x the cap must be refused");
        let TooBroadReason::WalkTransientBytes { estimate, cap } = err else {
            panic!("expected WalkTransientBytes, got {err:?}");
        };
        assert_eq!(cap, MAX_LOGQL_WALK_TRANSIENT_BYTES);
        assert!(
            estimate > cap,
            "the refusal must name a breach: {estimate} vs {cap}"
        );
    }

    /// The 4x probe clause 3d posts must sit above the model threshold,
    /// and it does so with `B*(2C-1)` of headroom. If a measured `B`/`S`
    /// ever made `B*(2C-1) <= S` this fails loudly rather than being
    /// papered over by widening the probe.
    #[test]
    fn the_four_times_probe_clears_the_threshold_with_stated_headroom() {
        let c = REFERENCE_MAX_QUERY_BYTES;
        assert!(
            WALK_BYTES_PER_NODE * (2 * c - 1) > WALK_FIXED_SLACK_BYTES,
            "B*(2C-1) = {} <= S = {WALK_FIXED_SLACK_BYTES} — report this, do not widen the probe",
            WALK_BYTES_PER_NODE * (2 * c - 1)
        );
        assert!(walk_transient_bound(4 * c) > MAX_LOGQL_WALK_TRANSIENT_BYTES);
        // 2x does NOT clear — exactly one integer multiple sits below the
        // guard, so shrinking the probe from 4x to 2x breaks it.
        assert!(walk_transient_bound(2 * c) < MAX_LOGQL_WALK_TRANSIENT_BYTES);
        // 3x is the first multiple that clears (conditional on
        // `B*(C-1) > S`, which is asserted rather than assumed).
        assert!(WALK_BYTES_PER_NODE * (c - 1) > WALK_FIXED_SLACK_BYTES);
        assert!(walk_transient_bound(3 * c) > MAX_LOGQL_WALK_TRANSIENT_BYTES);
    }

    /// The documented maximum entry and the fixed slack are the SAME
    /// number seen twice; asserting the identity is what stops the
    /// measurement note drifting from the arithmetic it describes.
    #[test]
    fn the_fixed_slack_is_derived_from_the_measured_maximum_entry() {
        // THE single source for the measured figure: it appears in no
        // comment, so no comment can go stale against it.
        assert_eq!(
            MAX_WALK_ENTRY_BYTES, 136,
            "the largest `ChunkStack` entry moved; re-derive \
             `WALK_FIXED_SLACK_BYTES`'s consequences before re-pinning"
        );
        assert_eq!(
            MAX_WALK_ENTRY_BYTES,
            size_of::<pulsus_logql::MeVal>() as u64,
            "`MeVal` is no longer the largest entry"
        );
        let per_stack = WALK_CHUNK_ITEMS as u64 * MAX_WALK_ENTRY_BYTES
            + INDEX_INIT as u64 * size_of::<Vec<usize>>() as u64
            + WALK_ALLOC_ENVELOPE_BYTES as u64;
        assert_eq!(WALK_FIXED_SLACK_BYTES, 4 * per_stack);
    }

    #[test]
    fn a_failed_charge_does_not_mutate_the_counter() {
        let mut b = WalkBudget::new();
        b.charge(1024).expect("under cap");
        assert_eq!(b.charged(), 1024);
        let err = b
            .charge(MAX_LOGQL_WALK_TRANSIENT_BYTES as usize)
            .expect_err("over cap");
        assert!(matches!(
            err,
            ReadError::QueryTooBroad(TooBroadReason::WalkTransientBytes { .. })
        ));
        assert_eq!(b.charged(), 1024, "a failed charge mutated the counter");
    }
}
