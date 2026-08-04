//! The pure planner: `Expr → Plan`. Normalizes matchers into the single-pass
//! stage 1 shape (docs/schemas.md §3.2, architect plan amendment §1),
//! decides rollup-vs-raw routing for metric queries, compiles line-filter
//! pushdown, and derives the partition months a range touches. Nothing here
//! talks to ClickHouse — [`plan`] is `Expr + QueryParams + PlanCtx →
//! Result<Plan, ReadError>`, fully deterministic and snapshot-testable.
//!
//! Stage 2 (hydration) and stage 3/metric reads depend on stage 1's
//! *runtime* fingerprint set, so only stage 1's SQL is fully static at plan
//! time; [`Plan`] carries the resolved table names, line filters, and
//! bucket/aggregate expressions [`super::exec::LogQlEngine`] needs to call
//! [`super::sql`]'s stage 2/3/metric builders once fingerprints are known.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::ControlFlow;

use pulsus_logql::walk;
use pulsus_logql::{
    BinModifier, BinOp, Expr, Grouping, GroupingKind, LineFilter, LineFilterOp, LogExpr, LogRange,
    MatchOp, Matcher, MetricExpr, RangeAggOp, Stage, StreamSelector, VariantsExpr, VectorAggOp,
    VectorMatching,
};

use super::charge::AggCaps;
use super::error::{ReadError, TooBroadReason};
use super::escape::{ch_regex_anchored_checked, ch_regex_unanchored_checked, ch_string};
use super::params::{
    Direction, PlanCtx, QueryParams, QuerySpec, ValidatedDuration, validate_duration_ns,
};
use super::pipeline::PipelineError;
use super::sql::ScanLowerBound;
use super::window::{ClientWindow, GridWindow};

/// A pure fetch plan for either query shape. See the module docs for why
/// stage 2/3 aren't pre-rendered here. `MetricBinary` (issue M6-10) is
/// the plan for a metric expression containing binary operations or
/// scalar literals — a tree whose leaves are ordinary [`MetricPlan`]s;
/// plain single-aggregation metric queries keep planning to
/// [`Plan::Metric`] byte-identically.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    Streams(StreamsPlan),
    Metric(MetricPlan),
    MetricBinary(MetricNode),
}

/// The static (runtime-fingerprint-independent) part of a stream-selector
/// query plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsPlan {
    pub stage1_sql: String,
    pub streams_table: String,
    pub samples_table: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub direction: Direction,
    /// The stage-3 SQL `LIMIT` — a *scan* bound (issue M6-09 plan v3
    /// delta 4). Equal to [`StreamsPlan::result_limit`] unless the
    /// pipeline contains an unpushed dropping stage (a label filter, or a
    /// line filter after `line_format`), in which case it is the
    /// **first-page fetch size** for fetch-until-limit paging (issue #90):
    /// `result_limit × PlanCtx::pipeline_scan_factor` (saturating). It is
    /// no longer a truncation ceiling — when [`StreamsPlan::fetch_until_limit`]
    /// is set the engine keyset-pages this many rows at a time through the
    /// pipeline until the limit fills, the window is exhausted, or the byte
    /// scan budget is spent. The byte scan budget is still the hard
    /// cumulative ceiling and aborts first.
    pub scan_limit: u32,
    /// The true response cap — re-applied in-engine to pipeline
    /// survivors, globally across streams. Responses never over-return.
    pub result_limit: u32,
    /// Fetch-until-limit paging is engaged (issue #90): set iff the
    /// pipeline has an unpushed dropping stage (label filter, or a line
    /// filter after `line_format`). The engine keyset-pages the dropping
    /// path exactly until `result_limit` survivors are collected (no more
    /// under-return), rather than truncating a single oversampled scan.
    /// Non-dropping plans leave this `false` and keep byte-identical
    /// single-`LIMIT` SQL (`scan_limit == result_limit`).
    pub fetch_until_limit: bool,
    /// One pre-rendered predicate fragment per **pushed-down** pipeline
    /// `LineFilter` stage — those positioned before the first
    /// `line_format` stage, which reference the original `body` — ANDed
    /// together by [`super::sql::stage3`]. Line filters after a
    /// `line_format` reference the rewritten line and evaluate in-engine
    /// instead ([`super::pipeline::CompiledPipeline`]).
    pub line_filters: Vec<String>,
    /// The full ordered pipeline, compiled per query by
    /// [`super::exec::LogQlEngine`] into the in-engine evaluator.
    pub pipeline: Vec<Stage>,
    pub probes: Vec<ProbePlan>,
}

/// Which physical table a metric read was routed to. See
/// [`RoutingDecision`] for the accompanying (deterministic, plan-derived)
/// reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteChoice {
    Rollup,
    Raw,
}

/// One vector-aggregation layer: `(op, grouping, parsed topk/bottomk k)`
/// — the parsed-parameter shape [`MetricPlan::vector_aggs`] and
/// [`MetricNode::VectorAgg`] carry (issue M6-10).
pub type VectorAggSpec = (VectorAggOp, Option<Grouping>, Option<f64>);

/// The raw-parameter shape straight off the AST, BORROWED — the walk that
/// produces it must not allocate (issue #221 review round 4: the owned
/// form cloned every grouping and parameter once per variant, before the
/// `variant_spec_bytes` charge could reject). [`parse_vector_agg_params`]
/// stays the sole producer of the OWNED [`VectorAggSpec`].
type RawVectorAggSpec<'a> = (VectorAggOp, Option<&'a Grouping>, Option<&'a str>);

/// The compiled `label_replace(...)` transform (issue #276), carried on
/// [`MetricNode::LabelReplace`] and applied by
/// [`super::post_agg::apply_label_replace`].
///
/// The regex is compiled ONCE, at plan time, with Loki's exact anchoring
/// — `^(?:regex)$`, NO dot-all flag (v3.7.4 `pkg/logql/syntax/ast.go`
/// `mustNewLabelReplaceExpr`; contrast Prometheus `label_replace`'s
/// `^(?s:…)$`) — after the #317 RE2→Rust rewrite
/// ([`pulsus_promql::re2_pattern_to_rust`]) so `\d`/`\w`/`\s` and the
/// class-set constructs keep RE2's meaning in-engine. The pattern never
/// reaches SQL: the transform runs over the already-evaluated result.
#[derive(Debug, Clone)]
pub struct LabelReplaceSpec {
    pub dst: String,
    pub replacement: String,
    pub src: String,
    /// The user's raw pattern — the `PartialEq` witness (`re` is derived
    /// from it deterministically) and the `Display`/explain text.
    pub regex: String,
    re: regex::Regex,
}

impl LabelReplaceSpec {
    /// Compiles the four raw `label_replace` arguments. An uncompilable
    /// regex is the reference's parse-time 400, and — the issue #240
    /// asymmetry, pinned as DELIBERATE — its message reports the
    /// **wrapped** `^(?:…)$` form, because the reference compiles exactly
    /// that string and surfaces the compiler's error verbatim
    /// (live-probed, v3.7.4: `invalid regex in label_replace: error
    /// parsing regexp: missing closing ): `^(?:()$``). Every other LogQL
    /// site reports the USER's pattern via `pipeline::bad_regex` — this
    /// constructor must NOT be "consistency fixed" to match them, and
    /// deliberately does not route through that seam. (The error wording
    /// after the prefix is rust-regex's, not Go's — the ledgered
    /// `template-error-wording-residuals` class.)
    pub fn compile(
        dst: &str,
        replacement: &str,
        src: &str,
        regex: &str,
    ) -> Result<Self, ReadError> {
        let translated = pulsus_promql::re2_pattern_to_rust(regex);
        let re = regex::Regex::new(&format!("^(?:{translated})$")).map_err(|e| {
            ReadError::PipelineInvalid {
                reason: format!("invalid regex in label_replace: {e}"),
            }
        })?;
        Ok(LabelReplaceSpec {
            dst: dst.to_string(),
            replacement: replacement.to_string(),
            src: src.to_string(),
            regex: regex.to_string(),
            re,
        })
    }

    /// The compiled, anchored matcher.
    pub(in crate::logql) fn re(&self) -> &regex::Regex {
        &self.re
    }
}

/// `re` is a deterministic function of `regex`, so field equality over
/// the four strings is full equality.
impl PartialEq for LabelReplaceSpec {
    fn eq(&self, other: &Self) -> bool {
        self.dst == other.dst
            && self.replacement == other.replacement
            && self.src == other.src
            && self.regex == other.regex
    }
}

/// The rollup-vs-raw routing decision for one metric query, computed once
/// in [`metric_plan`] and carried on both [`MetricPlan`] (for [`super::exec`]
/// to act on) and [`super::explain::PlanExplain`] (for #13's
/// `X-Pulsus-Explain` header to name). `reason` is entirely plan-derived —
/// an enum tag plus numeric nanosecond values, never user-controlled
/// data — so it is safe to surface verbatim in a response header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingDecision {
    pub chosen: RouteChoice,
    pub reason: String,
}

/// The static part of a metric query plan. `table`/`bucket_col`/`agg_expr`
/// encode the rollup-vs-raw routing decision (docs/schemas.md §3.2);
/// `rate_window_ns` is `Some` only for `rate`/`bytes_rate` (the divisor
/// [`super::exec`] applies), never for the `*_over_time` count ops.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricPlan {
    pub stage1_sql: String,
    pub streams_table: String,
    pub table: String,
    pub bucket_col: &'static str,
    pub agg_expr: &'static str,
    pub rollup: bool,
    /// The single routing decision `rollup` is derived from
    /// (`rollup == matches!(routing.chosen, RouteChoice::Rollup)`); kept
    /// alongside the plain bool so callers that only need the SQL shape
    /// (`exec.rs`) don't have to match on the enum.
    pub routing: RoutingDecision,
    /// Line-filter pushdown for the raw fallback (the rollup table has no
    /// `body` column — a metric query with a line filter can never be
    /// rollup-served, see [`metric_plan`]).
    pub extra_predicates: Vec<String>,
    /// The scan lower bound (`timestamp_ns > start_ns`, or `>=` when
    /// `scan_lower` is [`ScanLowerBound::Inclusive`]). For a range query
    /// this is widened to `query_start - range` (issue #227) so the first
    /// grid point's sliding window `(start-range, start]` sees its full
    /// lookback; the emit grid still begins at `query_start`.
    pub start_ns: i64,
    /// How `start_ns` compares in SQL — [`ScanLowerBound::Inclusive`]
    /// EXACTLY when the `start - range` widening underflowed i64
    /// ([`widen_scan_start`], issue #227 review round 11): the logical
    /// bound then sits below every representable timestamp, so the
    /// saturated `i64::MIN` in `start_ns` is vacuous, not exclusive — a
    /// sample stored at exactly `i64::MIN` is inside the reference's
    /// window. A legitimately-computed `i64::MIN` bound stays
    /// [`ScanLowerBound::Exclusive`].
    pub scan_lower: ScanLowerBound,
    pub end_ns: i64,
    /// `Some(step)` = [`QuerySpec::Range`]'s Loki sliding-window shape
    /// (issue #227: the `[range]` window `(t-range, t]` re-evaluated at the
    /// start-anchored grid `{start+k·step ≤ end}`); `None` =
    /// [`QuerySpec::Instant`]'s single-window aggregate. Carries the
    /// boundary-validated [`ValidatedDuration`] (issue #227 review round 3).
    pub step_ns: Option<ValidatedDuration>,
    /// The emit grid's lower bound — `query_start` (start-anchored,
    /// issue #227). Distinct from `start_ns`, which is the (range-widened)
    /// scan lower bound. Equals `start_ns` for instant queries.
    pub grid_start_ns: i64,
    /// The `[range]` selector width in nanoseconds — the sliding window's
    /// span and (for `rate`/`bytes_rate`) the per-second divisor.
    ///
    /// **Validated** at the planner boundary into `1 ..= MAX_DURATION_NS`
    /// ([`validate_duration_ns`]) and carried as the unforgeable
    /// [`ValidatedDuration`] (issue #227 review round 3), so the evaluator
    /// can neither narrow nor be handed unvalidated client input.
    pub range_ns: ValidatedDuration,
    /// `offset <duration>` in SIGNED nanoseconds, `0` when absent (issue
    /// #343). Every time bound above is ALREADY shifted by it — this
    /// field is what the emitted point timestamps add back so a matrix
    /// comes out on the CALLER's grid, exactly as Loki v3.7.4's
    /// `batchRangeVectorIterator` reports `current + offset` after
    /// starting at `start - offset`.
    ///
    /// Signed because the reference accepts a negative offset and shifts
    /// the window forward; an instant result carries no timestamp, so
    /// only the range/matrix shape ever reads it back.
    pub offset_ns: i64,
    /// The shifted evaluation domain left the representable timestamp axis
    /// (issue #343), so the query answers empty.
    ///
    /// # NEITHER MECHANISM MAY BE DELETED ON THE GROUNDS THAT THE OTHER COVERS IT
    ///
    /// Two mechanisms carry this, and each looks redundant given the other.
    /// Deleting either reintroduces the bug while reading as tidying, so the
    /// division of labour is written down here once:
    ///
    /// * **The DEGENERATE WINDOW is what makes the answer correct.** When
    ///   [`shift_by_offset`] returns `None` the planner substitutes
    ///   `grid_start_ns = 0`, `end_ns = -1` ([`EMPTY_DOMAIN_GRID_START_NS`] /
    ///   [`EMPTY_DOMAIN_END_NS`]), and `end < grid_start` makes every path
    ///   downstream produce nothing — the SQL predicate, the emit grid, the
    ///   fence, the partition list. It holds on EVERY path, which is why the
    ///   same substitution is repeated per variant in [`build_variants_node`]:
    ///   a variant's shift is its own, so a whole-plan flag could not express
    ///   one variant leaving the axis while its siblings stay on it.
    /// * **This FLAG is defence in depth plus one saved round trip, and is
    ///   deliberately NOT the correctness mechanism.** [`super::exec`] returns
    ///   empty on it before `resolve_fingerprints`, so an off-axis query costs
    ///   zero ClickHouse round trips instead of one.
    /// * **Why the defence in depth stays rather than being trimmed:** the
    ///   instant path re-filters nothing. [`super::window::ClientWindow::Instant`]
    ///   carries no residual `[range]` because the scan already bounded the
    ///   rows, so a row that reached the engine WOULD be returned regardless of
    ///   the window. MEASURED, not reasoned: an in-engine instant fixture over
    ///   the degenerate window returned `1` (issue #343, AC 6's
    ///   `r2_instant_…`, which is pinned at the plan for exactly this reason).
    /// * **The evidence they are not interchangeable** is issue #343's AC 8
    ///   finding: forcing this flag `false` leaves the WIRE unchanged — the
    ///   degenerate window still renders `'1970-01-01'`, stage 1 resolves zero
    ///   fingerprints, the pre-existing empty-fingerprint return fires, and the
    ///   explain payload is byte-identical. The flag therefore has no
    ///   distinguishing wire observable; it is asserted DIRECTLY by
    ///   `sql_snapshots.rs::an_offset_past_the_timestamp_axis_renders_the_degenerate_empty_window`,
    ///   this module's own
    ///   `tests::offset_off_the_timestamp_axis_plans_the_degenerate_empty_window`
    ///   and `logql_metric_agg_golden.rs::r2_instant_a_beyond_rail_instant_window_reaches_stored_data`.
    ///   Conversely the saturating mutant reddens the wire test and those three
    ///   alike. Two mechanisms, two different jobs, two different gates.
    pub empty_domain: bool,
    pub rate_window_ns: Option<u64>,
    pub op: RangeAggOp,
    /// Outer-to-inner vector-aggregation chain (`sum by (...) (avg(...))`
    /// nests outer-first); finished in Rust over the per-fingerprint series
    /// (docs/schemas.md §3.2: "the engine ... finishes the `sum by`").
    /// The third element is the parsed `topk`/`bottomk` `k` (issue
    /// M6-10), `None` for every other aggregation.
    pub vector_aggs: Vec<VectorAggSpec>,
    /// `Some` = client-aggregated (issue M6-10): raw-scan + in-engine
    /// pipeline/unwrap/reduce over `metric_raw_samples` (no `LIMIT` —
    /// complete-or-error). `None` = the existing SQL-aggregated
    /// (rollup-or-raw) path, byte-identical to pre-M6-10 plans.
    pub client: Option<ClientAgg>,
    pub probes: Vec<ProbePlan>,
}

/// The client-aggregated execution spec (issue M6-10): what
/// [`super::exec`] runs per surviving line after the full-window raw
/// scan.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientAgg {
    /// The full ordered pipeline (compiled once in exec via
    /// [`super::pipeline::CompiledPipeline`]); the line-filter prefix
    /// before the first `line_format` is ALSO pushed down as
    /// `extra_predicates` (plan v2 D3 — the pushdown-order invariant
    /// lives in [`compile_line_filters`]).
    pub pipeline: Vec<Stage>,
    /// Per-surviving-line sample value source.
    pub value: ClientValue,
    /// The over-time reducer.
    pub range_op: RangeAggOp,
    /// The `quantile_over_time` q, parsed from the AST's raw parameter.
    pub param: Option<f64>,
    /// `absent_over_time` only: the selector's `Eq`-matcher labels — the
    /// synthetic-absence series labels (oracle-probed; plan v2 D2).
    pub absent_labels: Vec<(String, String)>,
}

/// Where a client-aggregated sample's value comes from. `Unwrap` carries
/// no fields (plan v2 D1): the label/conversion live in the compiled
/// unwrap stage inside the pipeline; this is just the marker telling
/// exec to read the pipeline's extracted value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientValue {
    Count,
    Bytes,
    Unwrap,
}

/// One node of a binary-operation metric plan (issue M6-10): leaves are
/// ordinary [`MetricPlan`]s (each planned exactly as if it were the
/// whole query), scalars come from literal operands, and `VectorAgg`
/// covers a vector aggregation over a *binary* operand (`sum(a + b)`) —
/// a thin post-combination layer reusing the same reducer code as
/// [`MetricPlan::vector_aggs`].
/// **Issue #272 — no `#[derive]`.** `Binary::lhs`, `Binary::rhs` and
/// `VectorAgg::inner` are SCC-internal child slots spelled
/// [`walk::Child`], which implements no derivable trait, so a
/// `#[derive]` here is a compile error (C3). `Debug`, `Clone`,
/// `PartialEq` and `Drop` are hand-written below and iterative;
/// `Leaf`/`Variants`' boxed `MetricPlan` leaves the cycle and is
/// untouched.
pub enum MetricNode {
    Leaf(Box<MetricPlan>),
    Scalar(f64),
    /// `vector(<scalar>)` (issue #221): a constant promoted to a vector
    /// result (`{} => value`). Carries the resolved evaluation `window` so
    /// exec materializes an instant single-sample vector or a range
    /// constant `{}` matrix (reusing the shared bucket grid + cap).
    VectorLit {
        value: f64,
        window: GridWindow,
    },
    Binary {
        op: BinOp,
        /// The `bool` comparison modifier (0/1 instead of filtering).
        return_bool: bool,
        /// The `on`/`ignoring`/`group_left`/`group_right` vector-matching
        /// clause (issue #91); `None` = default full-label one-to-one
        /// matching (the pre-#91 behavior, byte-identical).
        matching: Option<VectorMatching>,
        lhs: walk::Child<MetricNode>,
        rhs: walk::Child<MetricNode>,
    },
    VectorAgg {
        aggs: Vec<VectorAggSpec>,
        inner: walk::Child<MetricNode>,
    },
    /// `variants(...) of (...)` (issue #221): ONE scan feeding N
    /// reducers. `scan` is planned from the COMMON log range alone
    /// (truncated at its first `unwrap` — dead syntax in the reference),
    /// so the SQL/index path is byte-identical to the equivalent
    /// single-extractor query and independent of N.
    Variants {
        scan: Box<MetricPlan>,
        variants: Vec<VariantSpec>,
        /// The plan-time fan-out charge (`Σ variant_spec_bytes` plus the
        /// spec vector's own buffer), carried so
        /// [`super::variants::VariantArena::build`] CONTINUES the same
        /// counter — one budget for plan-time + exec-time state, never
        /// two.
        spec_bytes: u64,
    },
    /// `label_replace(...)` (issue #276): a pure label transform over the
    /// inner node's evaluated result — no scan of its own, SQL/pushdown
    /// untouched.
    LabelReplace {
        spec: LabelReplaceSpec,
        inner: walk::Child<MetricNode>,
    },
}

impl MetricNode {
    /// Every [`MetricPlan`] leaf in the tree, left-to-right — the
    /// stage-1 resolution surface (`series`/explain walk these).
    pub fn leaves(&self) -> Vec<&MetricPlan> {
        let mut out = Vec::new();
        self.collect_leaves(&mut out);
        out
    }

    /// Issue #272: a driver consumer. Pre-order left-to-right visits the
    /// same nodes in the same order the recursion did, so the leaf
    /// sequence is unchanged.
    fn collect_leaves<'a>(&'a self, out: &mut Vec<&'a MetricPlan>) {
        walk::preorder::<MetricNodeScc>(self, |n| match n {
            MetricNode::Leaf(mp) => out.push(mp),
            MetricNode::Scalar(_) => {}
            // A `vector(n)` literal is series-producing but reads no DB
            // leaf (mirrors `Scalar` for leaf collection — see
            // `produces_series`).
            MetricNode::VectorLit { .. } => {}
            MetricNode::Binary { .. } => {}
            MetricNode::VectorAgg { .. } => {}
            MetricNode::Variants { scan, .. } => out.push(scan),
            // A pure label transform: its leaves are its inner's, which
            // the walk reaches through the child slot.
            MetricNode::LabelReplace { .. } => {}
        });
    }

    /// Whether the tree produces a series result (vector/matrix) rather
    /// than a pure scalar. True for any [`MetricNode::Leaf`] (a DB-read
    /// range aggregation) or any [`MetricNode::VectorLit`] (`vector(n)`,
    /// which yields `{} => n`); false for a pure-literal tree (`5`,
    /// `5+3`). Drives `binary_result_type`'s scalar-vs-vector/matrix
    /// classification (issue #221).
    ///
    /// Issue #272: a driver consumer with an early break, so the
    /// short-circuit the `||` gave is preserved — the walk stops at the
    /// first series-producing node.
    pub fn produces_series(&self) -> bool {
        walk::find_preorder::<MetricNodeScc, ()>(self, |n| match n {
            MetricNode::Leaf(_) | MetricNode::VectorLit { .. } | MetricNode::Variants { .. } => {
                ControlFlow::Break(())
            }
            MetricNode::Scalar(_) => ControlFlow::Continue(walk::Step::Descend),
            MetricNode::Binary { .. }
            | MetricNode::VectorAgg { .. }
            | MetricNode::LabelReplace { .. } => ControlFlow::Continue(walk::Step::Descend),
        })
        .is_some()
    }
}

// ---------------------------------------------------------------------
// SCC-3: `MetricNode` (issue #272)
// ---------------------------------------------------------------------

/// The zero-sized tag naming SCC-3 in [`walk::Scc`]'s type position.
/// SCC-3 is homogeneous — every SCC-internal child of a `MetricNode` is a
/// `MetricNode` — so its kinds are the trivial ones.
#[derive(Debug, Clone, Copy)]
pub struct MetricNodeScc;

/// A same-kind, non-allocating `MetricNode` the planner can never
/// produce, written over a stolen child so the emptied parent's own drop
/// is free.
#[inline]
fn mn_placeholder() -> MetricNode {
    MetricNode::Scalar(f64::NAN)
}

impl walk::Scc for MetricNodeScc {
    type Ref<'a> = walk::Ref<'a, MetricNode>;
    type Node<'a> = &'a MetricNode;
    type Slot<'a> = walk::Slot<'a, MetricNode>;
    type SlotNode<'a> = &'a mut MetricNode;
    type Val = MetricNode;

    #[inline]
    fn wrap<'a>(n: &'a MetricNode) -> walk::Ref<'a, MetricNode>
    where
        Self: 'a,
    {
        walk::Ref::new(n)
    }

    #[inline]
    fn open<'a>(r: walk::Ref<'a, MetricNode>, w: &walk::Walk) -> &'a MetricNode
    where
        Self: 'a,
    {
        r.open(w)
    }

    #[inline]
    fn open_slot<'a>(s: walk::Slot<'a, MetricNode>, w: &walk::Walk) -> &'a mut MetricNode
    where
        Self: 'a,
    {
        s.open(w)
    }

    #[inline]
    fn slot_node_ref<'x>(s: &'x &mut MetricNode) -> &'x MetricNode
    where
        Self: 'x,
    {
        s
    }

    #[inline]
    fn child<'a>(n: &'a MetricNode, i: usize) -> Option<walk::Ref<'a, MetricNode>>
    where
        Self: 'a,
    {
        match n {
            MetricNode::Leaf(_)
            | MetricNode::Scalar(_)
            | MetricNode::VectorLit { .. }
            | MetricNode::Variants { .. } => None,
            MetricNode::Binary { lhs, rhs, .. } => match i {
                0 => Some(lhs.peek()),
                1 => Some(rhs.peek()),
                _ => None,
            },
            MetricNode::VectorAgg { inner, .. } => match i {
                0 => Some(inner.peek()),
                _ => None,
            },
            MetricNode::LabelReplace { inner, .. } => match i {
                0 => Some(inner.peek()),
                _ => None,
            },
        }
    }

    fn shallow_eq(a: &MetricNode, b: &MetricNode) -> bool {
        match (a, b) {
            (MetricNode::Leaf(a), MetricNode::Leaf(b)) => a == b,
            (MetricNode::Scalar(a), MetricNode::Scalar(b)) => a == b,
            (
                MetricNode::VectorLit {
                    value: av,
                    window: aw,
                },
                MetricNode::VectorLit {
                    value: bv,
                    window: bw,
                },
            ) => av == bv && aw == bw,
            (
                MetricNode::Binary {
                    op: ao,
                    return_bool: ab,
                    matching: am,
                    ..
                },
                MetricNode::Binary {
                    op: bo,
                    return_bool: bb,
                    matching: bm,
                    ..
                },
            ) => ao == bo && ab == bb && am == bm,
            (MetricNode::VectorAgg { aggs: a, .. }, MetricNode::VectorAgg { aggs: b, .. }) => {
                a == b
            }
            (
                MetricNode::Variants {
                    scan: asc,
                    variants: av,
                    spec_bytes: ab,
                },
                MetricNode::Variants {
                    scan: bsc,
                    variants: bv,
                    spec_bytes: bb,
                },
            ) => asc == bsc && av == bv && ab == bb,
            (
                MetricNode::LabelReplace { spec: a, .. },
                MetricNode::LabelReplace { spec: b, .. },
            ) => a == b,
            (MetricNode::Leaf(_), _)
            | (MetricNode::Scalar(_), _)
            | (MetricNode::VectorLit { .. }, _)
            | (MetricNode::Binary { .. }, _)
            | (MetricNode::VectorAgg { .. }, _)
            | (MetricNode::Variants { .. }, _)
            | (MetricNode::LabelReplace { .. }, _) => false,
        }
    }

    fn rebuild(n: &MetricNode, kids: &mut Vec<MetricNode>) -> MetricNode {
        match n {
            MetricNode::Leaf(mp) => MetricNode::Leaf(mp.clone()),
            MetricNode::Scalar(v) => MetricNode::Scalar(*v),
            MetricNode::VectorLit { value, window } => MetricNode::VectorLit {
                value: *value,
                window: *window,
            },
            MetricNode::Binary {
                op,
                return_bool,
                matching,
                ..
            } => {
                // The tail of `kids` is [.., lhs, rhs].
                let rhs = drain_node(kids);
                let lhs = drain_node(kids);
                MetricNode::Binary {
                    op: *op,
                    return_bool: *return_bool,
                    matching: matching.clone(),
                    lhs: walk::Child::new(lhs),
                    rhs: walk::Child::new(rhs),
                }
            }
            MetricNode::VectorAgg { aggs, .. } => MetricNode::VectorAgg {
                aggs: aggs.clone(),
                inner: walk::Child::new(drain_node(kids)),
            },
            MetricNode::Variants {
                scan,
                variants,
                spec_bytes,
            } => MetricNode::Variants {
                scan: scan.clone(),
                variants: variants.clone(),
                spec_bytes: *spec_bytes,
            },
            MetricNode::LabelReplace { spec, .. } => MetricNode::LabelReplace {
                spec: spec.clone(),
                inner: walk::Child::new(drain_node(kids)),
            },
        }
    }

    fn take_own_fields(s: &mut &mut MetricNode) {
        match &mut **s {
            MetricNode::Leaf(_)
            | MetricNode::Scalar(_)
            | MetricNode::VectorLit { .. }
            | MetricNode::Variants { .. } => {}
            MetricNode::Binary { matching, .. } => {
                *matching = None;
            }
            MetricNode::VectorAgg { aggs, .. } => {
                aggs.clear();
                aggs.shrink_to_fit();
            }
            // The spec's own fields drop shallowly with the emptied node
            // (the `Leaf`/`Variants` precedent) — nothing to take.
            MetricNode::LabelReplace { .. } => {}
        }
    }

    fn steal_children(s: &mut &mut MetricNode, out: &mut walk::ChunkStack<MetricNode>) {
        match &mut **s {
            MetricNode::Leaf(_)
            | MetricNode::Scalar(_)
            | MetricNode::VectorLit { .. }
            | MetricNode::Variants { .. } => {}
            MetricNode::Binary { lhs, rhs, .. } => {
                // Right-to-left, so LIFO pop order is left-to-right.
                out.push(rhs.replace(mn_placeholder()));
                out.push(lhs.replace(mn_placeholder()));
            }
            MetricNode::VectorAgg { inner, .. } => {
                out.push(inner.replace(mn_placeholder()));
            }
            MetricNode::LabelReplace { inner, .. } => {
                out.push(inner.replace(mn_placeholder()));
            }
        }
    }

    #[inline]
    fn val_ref(v: &MetricNode) -> walk::Ref<'_, MetricNode> {
        walk::Ref::new(v)
    }

    #[inline]
    fn val_slot(v: &mut MetricNode) -> walk::Slot<'_, MetricNode> {
        walk::Slot::new(v)
    }
}

/// Drains one child value off the tail of a rebuild value stack.
#[inline]
fn drain_node(kids: &mut Vec<MetricNode>) -> MetricNode {
    match kids.pop() {
        Some(v) => v,
        // Unreachable by construction: post-order leaves exactly
        // `arity(n)` child values on the tail. Clone path, never a `Drop`
        // path, so a release panic is correct.
        None => unreachable!("expected a `MetricNode` child value on the rebuild stack"),
    }
}

/// The tree in post-order (children before parents, left to right) plus
/// the exact high-water mark a post-order value stack reaches over it —
/// which is what `run_metric_node` reserves, once, before evaluating.
/// Post-order (children before parents, left to right) plus the exact
/// high-water mark a post-order value stack reaches over it, with
/// **every allocating step charged before it happens** — the work stack's next chunk and the node
/// vector's next reallocation alike. Leg B's entry point: a refusal
/// stops the walk before the allocation it refused, rather than after
/// (issue #272 memory L1).
pub(crate) fn metric_node_postorder_charged<'a>(
    root: &'a MetricNode,
    budget: &mut super::walkbound::WalkBudget,
) -> Result<(Vec<&'a MetricNode>, usize), ReadError> {
    let mut nodes = Vec::new();
    walk::try_postorder_into::<MetricNodeScc, ReadError>(root, &mut nodes, |bytes| {
        budget.charge(bytes)
    })?;
    Ok(postorder_peak(nodes))
}

/// The exact high-water mark a post-order value stack reaches over
/// `nodes` — what `run_metric_node` reserves, once, before evaluating.
pub(crate) fn postorder_peak(nodes: Vec<&MetricNode>) -> (Vec<&MetricNode>, usize) {
    let mut live = 0usize;
    let mut peak = 0usize;
    for n in &nodes {
        let arity = walk::arity::<MetricNodeScc>(n);
        live -= arity;
        live += 1;
        peak = peak.max(live);
    }
    (nodes, peak)
}

impl Drop for MetricNode {
    fn drop(&mut self) {
        #[cfg(test)]
        drop_order::note_visited(self);
        walk::dismantle::<MetricNodeScc>(walk::Slot::new(self));
    }
}

impl Clone for MetricNode {
    fn clone(&self) -> Self {
        walk::clone_iter::<MetricNodeScc>(self)
    }
}

impl PartialEq for MetricNode {
    fn eq(&self, other: &Self) -> bool {
        walk::eq_iter::<MetricNodeScc>(self, other)
    }
}

/// Step indices for `MetricNode`'s `Debug` step machine.
mod mn_dbg_step {
    pub(super) const ENTER: u32 = 0;
    pub(super) const AFTER_FIRST: u32 = 1;
    pub(super) const AFTER_SECOND: u32 = 2;
}

impl fmt::Debug for MetricNode {
    /// Byte-equivalent to the deleted `#[derive(Debug)]` in both modes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let alt = f.alternate();
        walk::emit::<MetricNodeScc, fmt::Error>(self, |n, step, level| match n {
            MetricNode::Leaf(mp) => {
                walk::dbg_open_tuple(f, "Leaf", alt, level)?;
                walk::dbg_own(f, mp, alt, level + 1)?;
                walk::dbg_close_tuple(f, alt, level)?;
                Ok(walk::Emit::Done)
            }
            MetricNode::Scalar(v) => {
                walk::dbg_open_tuple(f, "Scalar", alt, level)?;
                walk::dbg_own(f, v, alt, level + 1)?;
                walk::dbg_close_tuple(f, alt, level)?;
                Ok(walk::Emit::Done)
            }
            MetricNode::VectorLit { value, window } => {
                walk::dbg_open_struct(f, "VectorLit", alt, level)?;
                f.write_str("value: ")?;
                walk::dbg_own(f, value, alt, level + 1)?;
                walk::dbg_sep(f, alt, level)?;
                f.write_str("window: ")?;
                walk::dbg_own(f, window, alt, level + 1)?;
                walk::dbg_close_struct(f, alt, level)?;
                Ok(walk::Emit::Done)
            }
            MetricNode::Variants {
                scan,
                variants,
                spec_bytes,
            } => {
                walk::dbg_open_struct(f, "Variants", alt, level)?;
                f.write_str("scan: ")?;
                walk::dbg_own(f, scan, alt, level + 1)?;
                walk::dbg_sep(f, alt, level)?;
                f.write_str("variants: ")?;
                walk::dbg_own(f, variants, alt, level + 1)?;
                walk::dbg_sep(f, alt, level)?;
                f.write_str("spec_bytes: ")?;
                walk::dbg_own(f, spec_bytes, alt, level + 1)?;
                walk::dbg_close_struct(f, alt, level)?;
                Ok(walk::Emit::Done)
            }
            MetricNode::VectorAgg { aggs, inner } => {
                if step == mn_dbg_step::ENTER {
                    walk::dbg_open_struct(f, "VectorAgg", alt, level)?;
                    f.write_str("aggs: ")?;
                    walk::dbg_own(f, aggs, alt, level + 1)?;
                    walk::dbg_sep(f, alt, level)?;
                    f.write_str("inner: ")?;
                    Ok(walk::Emit::Descend {
                        next_step: mn_dbg_step::AFTER_FIRST,
                        child: inner.peek(),
                        child_step: mn_dbg_step::ENTER,
                        child_level: level + 1,
                    })
                } else {
                    walk::dbg_close_struct(f, alt, level)?;
                    Ok(walk::Emit::Done)
                }
            }
            MetricNode::LabelReplace { spec, inner } => {
                if step == mn_dbg_step::ENTER {
                    walk::dbg_open_struct(f, "LabelReplace", alt, level)?;
                    f.write_str("spec: ")?;
                    walk::dbg_own(f, spec, alt, level + 1)?;
                    walk::dbg_sep(f, alt, level)?;
                    f.write_str("inner: ")?;
                    Ok(walk::Emit::Descend {
                        next_step: mn_dbg_step::AFTER_FIRST,
                        child: inner.peek(),
                        child_step: mn_dbg_step::ENTER,
                        child_level: level + 1,
                    })
                } else {
                    walk::dbg_close_struct(f, alt, level)?;
                    Ok(walk::Emit::Done)
                }
            }
            MetricNode::Binary {
                op,
                return_bool,
                matching,
                lhs,
                rhs,
            } => match step {
                mn_dbg_step::ENTER => {
                    walk::dbg_open_struct(f, "Binary", alt, level)?;
                    f.write_str("op: ")?;
                    walk::dbg_own(f, op, alt, level + 1)?;
                    walk::dbg_sep(f, alt, level)?;
                    f.write_str("return_bool: ")?;
                    walk::dbg_own(f, return_bool, alt, level + 1)?;
                    walk::dbg_sep(f, alt, level)?;
                    f.write_str("matching: ")?;
                    walk::dbg_own(f, matching, alt, level + 1)?;
                    walk::dbg_sep(f, alt, level)?;
                    f.write_str("lhs: ")?;
                    Ok(walk::Emit::Descend {
                        next_step: mn_dbg_step::AFTER_FIRST,
                        child: lhs.peek(),
                        child_step: mn_dbg_step::ENTER,
                        child_level: level + 1,
                    })
                }
                mn_dbg_step::AFTER_FIRST => {
                    walk::dbg_sep(f, alt, level)?;
                    f.write_str("rhs: ")?;
                    Ok(walk::Emit::Descend {
                        next_step: mn_dbg_step::AFTER_SECOND,
                        child: rhs.peek(),
                        child_step: mn_dbg_step::ENTER,
                        child_level: level + 1,
                    })
                }
                _ => {
                    walk::dbg_close_struct(f, alt, level)?;
                    Ok(walk::Emit::Done)
                }
            },
        })
    }
}

/// A selectivity `count()` probe over one matcher key's index prefix.
///
/// **Probe SQL is generated and surfaced in `PlanExplain`; probe
/// *execution* (matcher reordering / pre-flight estimate) is deferred**
/// (code-review fix-plan amendment §2, de-scoped rather than implemented).
/// Rationale: with the stage-1 scan itself now budget-capped
/// (`LogQlEngine::resolve_fingerprints`, `budget_settings()` +
/// `map_read_error`), the byte budget on the *actual* index scan already
/// provides the "abort past budget" guarantee docs/schemas.md §3.2
/// attributes to probes. The single grouped scan performs the whole
/// positive/negative intersection in one `GROUP BY ... HAVING` pass
/// (architect plan amendment §1), so OR-branch/matcher ordering inside that
/// one scan is cosmetic — it has no correctness or index-prefix dependency
/// the way, say, a sequential multi-pass plan would. Executing probes to
/// reorder branches or produce a pre-flight estimate is a pure
/// optimization, left for a later milestone; [`ProbePlan`] and its
/// `PlanExplain` wiring stay as-is so the SQL a probe *would* run is still
/// inspectable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbePlan {
    pub key: String,
    pub sql: String,
}

/// Plans `expr` into a [`Plan`]. See the module docs for the two-phase
/// split with stage 2/3 SQL generation.
pub fn plan(expr: &Expr, p: &QueryParams, ctx: &PlanCtx<'_>) -> Result<Plan, ReadError> {
    check_query_span(p)?;
    match expr {
        Expr::Log(log_expr) => Ok(Plan::Streams(streams_plan(log_expr, p, ctx)?)),
        Expr::Metric(metric_expr) => plan_metric_expr(metric_expr, p, ctx),
    }
}

/// **The 5-year rule, place 3 of 3** (issue #343, owner mandate): the
/// query's own `start`-to-`end` span may not exceed
/// [`pulsus_logql::MAX_QUERY_SPAN_NS`] — the SAME constant the parser
/// bounds `offset` and `[range]` against, read rather than restated.
///
/// This is the place the parser cannot reach, and the one that closes the
/// remaining hole: a `start` in 1677 with an ordinary `offset 1h` is
/// otherwise accepted and still walks off the representable timestamp
/// domain.
///
/// Run once in [`plan`], so it covers streams and metric queries alike.
/// An [`QuerySpec::Instant`] has no span to bound — its window is
/// `[at - range, at]`, and `range` is capped in the parser. `start > end`
/// is left alone here (an empty grid, handled downstream); only the
/// MAGNITUDE is bounded, computed in `i128` so the subtraction cannot
/// overflow before it is judged.
fn check_query_span(p: &QueryParams) -> Result<(), ReadError> {
    let QuerySpec::Range {
        start_ns, end_ns, ..
    } = p.spec
    else {
        return Ok(());
    };
    check_query_span_ns(start_ns, end_ns)
}

/// The 5-year span rule over a bare `start`/`end` pair — the form the HTTP
/// layer has, and the ONE implementation both callers share.
///
/// `logs_api::handlers::parse_bounds` calls this: it is the single
/// function every LogQL endpoint carrying `start`/`end` goes through, and
/// for three of those code paths it is the ONLY thing that caps the span.
/// The full division of labour is written out once, at `parse_bounds`
/// itself; the short version is that reaching [`plan`] is a property of
/// the ENGINE METHOD a route calls, not of whether the route takes a
/// selector — `/series` reaches it, `/labels` does not, and
/// `/detected_labels` reaches it only when `query` is present.
///
/// [`plan`] keeps its own call for the LIBRARY API, whose callers (tests,
/// e2e, any embedder) never pass through `parse_bounds`.
///
/// `start > end` is left alone — an empty grid, handled downstream. Only
/// the positive magnitude is bounded, computed in `i128` so the
/// subtraction cannot overflow before it is judged.
pub fn check_query_span_ns(start_ns: i64, end_ns: i64) -> Result<(), ReadError> {
    let span = i128::from(end_ns) - i128::from(start_ns);
    if span > i128::from(pulsus_logql::MAX_QUERY_SPAN_NS) {
        return Err(ReadError::QuerySpanTooLong {
            value: span,
            max: pulsus_logql::MAX_QUERY_SPAN_NS,
        });
    }
    Ok(())
}

/// Dispatches a metric expression: a (vector-agg-wrapped) range
/// aggregation keeps today's [`Plan::Metric`] shape byte-identically;
/// anything containing a binary operation or scalar literal plans to the
/// [`Plan::MetricBinary`] tree whose leaves are ordinary [`MetricPlan`]s
/// (issue M6-10).
fn plan_metric_expr(
    metric_expr: &MetricExpr,
    p: &QueryParams,
    ctx: &PlanCtx<'_>,
) -> Result<Plan, ReadError> {
    let (base, _) = unwrap_vector_aggs(metric_expr);
    match base {
        MetricExpr::Range { .. } => Ok(Plan::Metric(metric_plan(metric_expr, p, ctx, false)?)),
        _ => {
            // The step-domain guard normally lives in `metric_plan` (run per
            // leaf); a leaf-LESS tree (`2 + 2`, `vector(1)`) has no leaf to
            // run it, so the same boundary validation is applied here —
            // otherwise a hostile `step` would reach `window_from` ->
            // `materialize_vector_lit`'s grid math unvalidated (issue #227
            // review round 2). A zero step keeps its dedicated `InvalidStep`
            // error for request-shape consistency.
            if let QuerySpec::Range { step_ns, .. } = p.spec {
                if step_ns == 0 {
                    return Err(ReadError::InvalidStep);
                }
                validate_duration_ns(step_ns, "step")?;
            }
            Ok(Plan::MetricBinary(build_metric_node(metric_expr, p, ctx)?))
        }
    }
}

/// One step of the linearised planner program [`build_metric_node`]
/// emits (issue #293), in PRE-ORDER — the order the recursion it replaced
/// ran its fallible work in.
///
/// `pub(crate)` only so [`super::walkbound`] can price a slot; nothing
/// outside this module constructs or matches one.
pub(crate) enum PlanOp {
    /// A finished subtree — a leaf plan, a scalar, a `vector(n)` literal
    /// or a `variants(...)` node. Consumes no operand value.
    Node(MetricNode),
    /// Consumes the two values its operands left, `lhs` then `rhs`.
    Binary {
        op: BinOp,
        return_bool: bool,
        matching: Option<VectorMatching>,
    },
    /// Consumes the one value its operand left.
    VectorAgg { aggs: Vec<VectorAggSpec> },
    /// Consumes the one value its operand left (issue #276).
    LabelReplace { spec: LabelReplaceSpec },
}

/// Plans a binary/literal metric expression into a [`MetricNode`] tree.
/// Every (vector-agg-wrapped) range-aggregation operand becomes a
/// [`MetricNode::Leaf`] via the ordinary [`metric_plan`] path, so
/// per-leaf routing/rollup decisions are exactly what the same expression
/// would get standalone.
///
/// **Issue #293 — ITERATIVE.** This was the one LogQL walk #272 left
/// recursive, and it recursed on `lhs`: a flat `a or b or c …` chain
/// parses at depth 1 into a LEFT-DEEP `MetricExpr`, so query WIDTH is
/// tree DEPTH and a single request aborted the process (measured on a
/// 2 MiB stack, release: ok at 1,200 terms / 44,486 bytes, `fatal
/// runtime error: stack overflow` at 1,250 — well inside
/// [`pulsus_logql::MAX_QUERY_BYTES`]). It is now two loops: a
/// [`walk::find_preorder`] emission pass that runs every node's own
/// fallible work in the recursion's order, and a reverse fold over the
/// emitted program. Neither costs a machine-stack frame per node.
fn build_metric_node(
    metric_expr: &MetricExpr,
    p: &QueryParams,
    ctx: &PlanCtx<'_>,
) -> Result<MetricNode, ReadError> {
    let mut ops: Vec<PlanOp> = Vec::new();
    match emit_plan_ops(metric_expr, p, ctx, &mut ops) {
        Some(err) => Err(err),
        None => fold_plan_ops(ops),
    }
}

/// Emission pass. Pre-order, left-to-right — the exact order the
/// recursion evaluated in, so the FIRST error a malformed expression
/// produces is unchanged: a node's own fallible work runs before its
/// operands are visited, and `lhs` before `rhs`.
///
/// **The vector chain.** The recursion collapsed a whole
/// `sum(max(…))` spine in ONE step (`unwrap_vector_aggs`) and emitted a
/// single [`MetricNode::VectorAgg`] carrying every layer, so a walk that
/// visited each link and emitted per link would build a different tree.
/// `pending_aggs` carries the collapsed layers instead: it is non-empty
/// EXACTLY while the previously visited node was a `Vector` this walk
/// descended into. A `Vector` has exactly one child and a pre-order walk
/// visits a node's sole child immediately after the node, so the next
/// node is either the next link of the same chain (which emits nothing —
/// its layers are already collected) or the chain's base, which flushes.
fn emit_plan_ops(
    root: &MetricExpr,
    p: &QueryParams,
    ctx: &PlanCtx<'_>,
    ops: &mut Vec<PlanOp>,
) -> Option<ReadError> {
    let is_range = matches!(p.spec, QuerySpec::Range { .. });
    let mut pending_aggs: Vec<RawVectorAggSpec<'_>> = Vec::new();

    walk::find_preorder::<pulsus_logql::MetricScc, ReadError>(
        pulsus_logql::MeNode::Expr(root),
        |n| {
            let expr = match n {
                pulsus_logql::MeNode::Expr(e) => e,
                // Reached ONLY as the sole child of
                // `MetricExpr::Variants`, whose arm descends one link
                // rather than reaching into the slot itself — which is
                // why this walk needs no child accessor of its own.
                pulsus_logql::MeNode::Var(v) => {
                    return match build_variants_node(v, p, ctx) {
                        Ok(node) => {
                            ops.push(PlanOp::Node(node));
                            ControlFlow::Continue(walk::Step::Prune)
                        }
                        Err(err) => ControlFlow::Break(err),
                    };
                }
            };

            if let MetricExpr::Vector { .. } = expr {
                if pending_aggs.is_empty() {
                    // A chain HEAD: the collapsed base decides the shape,
                    // exactly as the recursion's `Vector` arm did.
                    let base = unwrap_vector_aggs_into(expr, &mut pending_aggs);
                    match base {
                        MetricExpr::Range { .. } => {
                            // The whole chain plans as one ordinary leaf.
                            pending_aggs.clear();
                            return match metric_plan(expr, p, ctx, false) {
                                Ok(mp) => {
                                    ops.push(PlanOp::Node(MetricNode::Leaf(Box::new(mp))));
                                    ControlFlow::Continue(walk::Step::Prune)
                                }
                                Err(err) => ControlFlow::Break(err),
                            };
                        }
                        MetricExpr::Literal(_) => {
                            return ControlFlow::Break(ReadError::PipelineInvalid {
                                reason: "a vector aggregation cannot aggregate a bare scalar \
                                         literal"
                                    .to_string(),
                            });
                        }
                        _ => {}
                    }
                }
                // A head with a planable base, or an inner link whose
                // layer the head already collected: descend to the base.
                return ControlFlow::Continue(walk::Step::Descend);
            }

            // Not a `Vector`, so this node is the base of the chain above
            // it, if any. The layers are validated HERE — before the
            // base's own work, exactly where the recursion validated them.
            if !pending_aggs.is_empty() {
                match parse_vector_agg_params(&pending_aggs, is_range) {
                    Ok(aggs) => ops.push(PlanOp::VectorAgg { aggs }),
                    Err(err) => return ControlFlow::Break(err),
                }
                pending_aggs.clear();
            }

            match expr {
                MetricExpr::Literal(raw) => {
                    match parse_plan_number(raw, format_args!("scalar literal")) {
                        Ok(value) => {
                            ops.push(PlanOp::Node(MetricNode::Scalar(value)));
                            ControlFlow::Continue(walk::Step::Prune)
                        }
                        Err(err) => ControlFlow::Break(err),
                    }
                }
                MetricExpr::VectorFn(raw) => {
                    let value = match parse_plan_number(raw, format_args!("vector() value")) {
                        Ok(v) => v,
                        Err(err) => return ControlFlow::Break(err),
                    };
                    match window_from(p) {
                        Ok(window) => {
                            ops.push(PlanOp::Node(MetricNode::VectorLit { value, window }));
                            ControlFlow::Continue(walk::Step::Prune)
                        }
                        Err(err) => ControlFlow::Break(err),
                    }
                }
                // Infallible here: the `VariantsExpr` child arm above does
                // the work, one link down.
                MetricExpr::Variants(_) => ControlFlow::Continue(walk::Step::Descend),
                MetricExpr::Binary { op, modifier, .. } => {
                    ops.push(PlanOp::Binary {
                        op: *op,
                        return_bool: matches!(
                            modifier,
                            Some(BinModifier {
                                return_bool: true,
                                ..
                            })
                        ),
                        matching: modifier.as_ref().and_then(|m| m.matching.clone()),
                    });
                    ControlFlow::Continue(walk::Step::Descend)
                }
                MetricExpr::Range { .. } => match metric_plan(expr, p, ctx, false) {
                    Ok(mp) => {
                        ops.push(PlanOp::Node(MetricNode::Leaf(Box::new(mp))));
                        ControlFlow::Continue(walk::Step::Prune)
                    }
                    Err(err) => ControlFlow::Break(err),
                },
                // `label_replace(...)` (issue #276): compile the regex
                // here in the pre-order pass — the reference surfaces its
                // parse-time regex error during validation, before the
                // evaluator factory ever sees a scalar operand
                // (live-probed ordering), and emission errors precede the
                // fold's scalar-operand rejection the same way.
                MetricExpr::LabelReplace {
                    dst,
                    replacement,
                    src,
                    regex,
                    ..
                } => match LabelReplaceSpec::compile(dst, replacement, src, regex) {
                    Ok(spec) => {
                        ops.push(PlanOp::LabelReplace { spec });
                        ControlFlow::Continue(walk::Step::Descend)
                    }
                    Err(err) => ControlFlow::Break(err),
                },
                // Unreachable: every `Vector` returns above, before the
                // chain flush.
                MetricExpr::Vector { .. } => {
                    unreachable!("a `Vector` node is handled by the chain arm above")
                }
            }
        },
    )
}

/// Fold pass. Consumes the pre-order program in REVERSE, which is a
/// bottom-up order: a parent's op precedes its operands' ops, so popping
/// from the tail reaches every operand before the op that combines them.
/// A `Binary`'s two operand values arrive `rhs` first (its right subtree
/// is the nearer tail), so the value popped first is `lhs`.
///
/// One loop, one value stack — no frame per node. The stack is reserved
/// at `ops.len()`, which is a hard upper bound on its depth (every op
/// pushes one value), so it allocates ONCE and never reallocates; a
/// three-op plan reserves three slots rather than a segmented stack's
/// whole first chunk.
///
/// Fallible since issue #276: each value carries its series-typing bit
/// (folded bottom-up in `O(ops)` total — never a per-node subtree walk),
/// and `PlanOp::LabelReplace` over a scalar-typed operand is the ONE
/// fold-time rejection. The reference 500s that operand (`unexpected
/// expr type (*syntax.LiteralExpr) for Evaluator type
/// (*logql.DefaultEvaluator)`, live-probed); PulsusDB keeps its
/// consistent plan-time 400 — the ledgered
/// `label-replace-scalar-operand-status` divergence, same class as
/// `variants-nonconforming-shape-status`.
fn fold_plan_ops(ops: Vec<PlanOp>) -> Result<MetricNode, ReadError> {
    // `(node, produces_series)` — the bool mirrors
    // `MetricNode::produces_series` incrementally: the `Node` ops are
    // childless prebuilt subtrees (`Leaf`/`Scalar`/`VectorLit`/
    // `Variants`), so their classification is O(1).
    let mut vals: Vec<(MetricNode, bool)> = Vec::with_capacity(ops.len());
    for op in ops.into_iter().rev() {
        match op {
            PlanOp::Node(node) => {
                let series = !matches!(node, MetricNode::Scalar(_));
                vals.push((node, series));
            }
            PlanOp::Binary {
                op,
                return_bool,
                matching,
            } => {
                let (lhs, ls) = pop_operand(&mut vals);
                let (rhs, rs) = pop_operand(&mut vals);
                vals.push((
                    MetricNode::Binary {
                        op,
                        return_bool,
                        matching,
                        lhs: walk::Child::new(lhs),
                        rhs: walk::Child::new(rhs),
                    },
                    ls || rs,
                ));
            }
            PlanOp::VectorAgg { aggs } => {
                let (inner, series) = pop_operand(&mut vals);
                vals.push((
                    MetricNode::VectorAgg {
                        aggs,
                        inner: walk::Child::new(inner),
                    },
                    series,
                ));
            }
            PlanOp::LabelReplace { spec } => {
                let (inner, series) = pop_operand(&mut vals);
                if !series {
                    return Err(ReadError::PipelineInvalid {
                        reason: "label_replace requires a vector operand, got a scalar \
                                 expression"
                            .to_string(),
                    });
                }
                vals.push((
                    MetricNode::LabelReplace {
                        spec,
                        inner: walk::Child::new(inner),
                    },
                    true,
                ));
            }
        }
    }
    match vals.pop() {
        Some((root, _)) if vals.is_empty() => Ok(root),
        // Unreachable by construction: every op pushes exactly one value
        // and consumes exactly the operands its emitting arm descended
        // into, so exactly one value survives. Not a `Drop` path, so a
        // release panic here is the correct failure.
        _ => unreachable!("the plan program leaves exactly one root value"),
    }
}

/// Pops one operand value off the fold's value stack.
#[inline]
fn pop_operand(vals: &mut Vec<(MetricNode, bool)>) -> (MetricNode, bool) {
    match vals.pop() {
        Some(v) => v,
        // Unreachable: see `fold_plan_ops`.
        None => unreachable!("expected an operand value on the plan fold stack"),
    }
}

/// The evaluation window for a leafless `vector(n)` node (issue #221),
/// taken from the same [`QuerySpec`] source `metric_plan` reads: instant
/// is `step_ns: None` (a single `{} => n` sample), range carries the
/// spec's `start_ns`/`end_ns`/`step_ns` so exec materializes the constant
/// `{}` matrix on the shared bucket grid.
fn window_from(p: &QueryParams) -> Result<GridWindow, ReadError> {
    // A `vector(n)` literal has no `[range]`; its constant `{}` series is
    // emitted at every start-anchored grid point regardless of window, so
    // `range_ns` is immaterial (0).
    match p.spec {
        QuerySpec::Instant { at_ns } => Ok(GridWindow {
            start_ns: at_ns,
            end_ns: at_ns,
            step_ns: None,
        }),
        QuerySpec::Range {
            start_ns,
            end_ns,
            step_ns,
        } => Ok(GridWindow {
            start_ns,
            end_ns,
            // The leafless `vector(n)` tree still routes its client `step`
            // through the boundary (issue #227 review round 3).
            step_ns: Some(validate_duration_ns(step_ns, "step")?),
        }),
    }
}

/// Parses a raw AST number the parser guaranteed to be `Number`-token
/// shaped; a non-finite/unparseable value is a named 400, never a NaN
/// smuggled into evaluation. `what` is formatted ONLY on the error path —
/// `format_args!` allocates nothing, so the happy path of a parameterized
/// vector aggregation costs zero allocations per variant (issue #221
/// review round 5, finding 1).
fn parse_plan_number(raw: &str, what: std::fmt::Arguments<'_>) -> Result<f64, ReadError> {
    match raw.parse::<f64>() {
        Ok(v) if v.is_finite() => Ok(v),
        _ => Err(ReadError::PipelineInvalid {
            reason: format!("invalid {what} {raw:?}"),
        }),
    }
}

/// Validates and parses each vector aggregation's parameter:
/// `topk`/`bottomk`/`approx_topk` require `k`; the parameterless
/// aggregations must not carry one (the parser already enforces both —
/// planner re-checks for defense in depth on programmatically-built
/// ASTs). `is_range` gates the instant-only `approx_topk` (issue #221):
/// this function is the SOLE producer of [`VectorAggSpec`] values (both
/// `metric_plan` and `build_metric_node` route through it), so the gate
/// cannot be bypassed by either plan shape.
fn parse_vector_agg_params(
    raw: &[RawVectorAggSpec<'_>],
    is_range: bool,
) -> Result<Vec<VectorAggSpec>, ReadError> {
    raw.iter()
        .map(|(op, grouping, param)| {
            // `sort`/`sort_desc` order the result vector by value; the
            // reference has no grouping form (`sort by(x)(...)` is a 400),
            // so reject a grouping rather than silently ignore it.
            if matches!(op, VectorAggOp::Sort | VectorAggOp::SortDesc) && grouping.is_some() {
                return Err(ReadError::PipelineInvalid {
                    reason: format!("`{op}` does not accept a grouping clause"),
                });
            }
            // Reference: `JoinCountMinSketchVector` refuses a non-instant
            // range type (pkg/logql/count_min_sketch.go, body verbatim —
            // the reference surfaces it as a 500, PulsusDB as a 400 per
            // the ledgered matching-error-status-divergence precedent).
            // Rejected at PLAN time, before any DB read, so no round-trip
            // is spent on a query that cannot be served.
            if matches!(op, VectorAggOp::ApproxTopk) && is_range {
                return Err(ReadError::PipelineInvalid {
                    reason: "count min sketches are only supported on instant queries".to_string(),
                });
            }
            let parsed = match (op.takes_param(), param) {
                (true, Some(raw)) => Some(parse_plan_number(raw, format_args!("{op} parameter"))?),
                (true, None) => {
                    return Err(ReadError::PipelineInvalid {
                        reason: format!("`{op}` requires a k parameter (e.g. {op}(5, ...))"),
                    });
                }
                (false, Some(_)) => {
                    return Err(ReadError::PipelineInvalid {
                        reason: format!("`{op}` takes no parameter"),
                    });
                }
                (false, None) => None,
            };
            Ok((*op, grouping.cloned().map(dedup_grouping), parsed))
        })
        .collect()
}

/// Deduplicates repeated grouping-label names (issue #288), preserving
/// first occurrence. The reference KEEPS duplicates in the parsed AST
/// (its `labels` grammar rule appends verbatim and `Grouping.String()`
/// renders them back) but they have no evaluation effect: the v3.7.4
/// `VectorAggEvaluator` builds each `by` group's label set from the
/// METRIC's own (sorted, unique) labels, taking a name at most once
/// (`metric.Range` + `break` on first membership hit), and `without`
/// deletes idempotently — live-probed, `sum by (fp, fp)` ==
/// `sum by (fp)` on the pinned container, for every aggregation and for
/// the grouping `variants` injects into. PulsusDB mirrors that split:
/// the parser keeps the AST faithful (duplicates render back), and this
/// SOLE `VectorAggSpec` producer normalizes what evaluation sees, so
/// `group_key` — whose absent-label `name=""` materialisation is owned
/// by #241 and deliberately untouched here — never receives a repeated
/// name. Names are matched byte-exactly: label names are case-sensitive
/// in the reference (`by (FP, fp)` probed as two distinct names, the
/// absent one omitted).
///
/// Two-tier, so BOTH standing constraints hold (fix round 1, U5):
/// * at or below [`DEDUP_LINEAR_SCAN_MAX`] names — every real-world
///   grouping — the pairwise scan allocates NOTHING, keeping the
///   `logql_variants_alloc` per-variant plan-time allocation bands
///   unmoved (its fixtures group by one label);
/// * above it, a hash-set pass keeps the whole thing `O(n)` expected:
///   the query-text cap admits ~20k grouping names, where the earlier
///   pairwise form cost ~80× the parse of the same query (measured,
///   release — the read-path-performance directive forbids that). The
///   probe `zz_print_dedup_grouping_timings` reproduces the
///   measurement; it prints, and never asserts wall time (CI gates stay
///   scale-invariant).
fn dedup_grouping(mut g: Grouping) -> Grouping {
    /// ≤ 16 names ⇒ ≤ 120 short-string comparisons — cheaper than one
    /// hash-set allocation, and allocation-free where the alloc gates
    /// look.
    const DEDUP_LINEAR_SCAN_MAX: usize = 16;
    fn first_occurrence_before(labels: &[String], i: usize) -> bool {
        labels[..i].iter().any(|l| *l == labels[i])
    }
    if g.labels.len() <= DEDUP_LINEAR_SCAN_MAX {
        if !(0..g.labels.len()).any(|i| first_occurrence_before(&g.labels, i)) {
            return g;
        }
        let mut i = 0;
        while i < g.labels.len() {
            if first_occurrence_before(&g.labels, i) {
                g.labels.remove(i);
            } else {
                i += 1;
            }
        }
        return g;
    }
    // The big-list tier: one keep-mask pass over a borrowed set (no
    // per-name clones), then an order-preserving in-place retain.
    let mut keep = Vec::with_capacity(g.labels.len());
    {
        let mut seen: HashSet<&str> = HashSet::with_capacity(g.labels.len());
        for name in &g.labels {
            keep.push(seen.insert(name.as_str()));
        }
    }
    if keep.iter().all(|k| *k) {
        return g;
    }
    let mut kept = keep.iter();
    g.labels.retain(|_| *kept.next().unwrap_or(&false));
    g
}

fn streams_plan(
    log_expr: &LogExpr,
    p: &QueryParams,
    ctx: &PlanCtx<'_>,
) -> Result<StreamsPlan, ReadError> {
    let (start_ns, end_ns) = window_bounds_for_streams(&p.spec);
    let normalized = normalize_matchers(&log_expr.selector)?;
    let months = months_overlapping(start_ns, end_ns);
    let stage1_sql = super::sql::stage1(
        ctx.streams_idx,
        &months,
        &normalized.positive_branches,
        &normalized.negative_branches,
    );
    let probes = build_probes(ctx, &months, &normalized.probe_keys);

    // A bare log query cannot evaluate `unwrap` — the unwrapped value
    // only means something inside a range aggregation (plan v3 delta 1).
    if log_expr
        .pipeline
        .iter()
        .any(|s| matches!(s, Stage::Unwrap(_)))
    {
        return Err(ReadError::PipelineInvalid {
            reason: "`unwrap` is only valid inside a range aggregation (e.g. \
                     sum_over_time({...} | unwrap x [5m]))"
                .to_string(),
        });
    }

    let line_filters = compile_line_filters(&log_expr.pipeline)?;
    let result_limit = p.limit;
    let fetch_until_limit = has_unpushed_dropping_stage(&log_expr.pipeline);
    let scan_limit = if fetch_until_limit {
        result_limit.saturating_mul(ctx.pipeline_scan_factor)
    } else {
        result_limit
    };

    Ok(StreamsPlan {
        stage1_sql,
        streams_table: ctx.streams.to_string(),
        samples_table: ctx.samples.to_string(),
        start_ns,
        end_ns,
        direction: p.direction,
        scan_limit,
        result_limit,
        fetch_until_limit,
        line_filters,
        pipeline: log_expr.pipeline.clone(),
        probes,
    })
}

/// Oversample eligibility (plan v3 delta 4): the pipeline contains a
/// stage that drops lines **in-engine** — a label filter, or a line
/// filter positioned after the first `line_format` (which references the
/// rewritten line and cannot push down). Parsers and `label_format` are
/// non-dropping (a parse failure keeps the line with an `__error__`
/// label; fan-out only regroups), so they alone never trigger the
/// oversample and parser-only pipelines keep byte-identical SQL.
fn has_unpushed_dropping_stage(pipeline: &[Stage]) -> bool {
    let mut seen_line_format = false;
    for stage in pipeline {
        match stage {
            Stage::LineFormat(_) => seen_line_format = true,
            // `decolorize`/`unpack` rewrite the line, so a following line
            // filter references the rewritten line and cannot push down (it
            // becomes an in-engine dropping stage — issue #200).
            Stage::Decolorize | Stage::Unpack => seen_line_format = true,
            Stage::LabelFilter(_) => return true,
            Stage::LineFilter(_) if seen_line_format => return true,
            // A non-pushable line filter (`ip(…)`/mixed-`or`) drops lines
            // in-engine (client pipeline), so it must oversample too.
            Stage::LineFilter(lf) if !is_pushable_line_filter(lf) => return true,
            _ => {}
        }
    }
    false
}

/// The first beyond-line-filter stage in pipeline order. Pre-M6-10 this
/// named the `PipelineUnsupportedInMetric` rejection; since M6-10 its
/// `Some` is the client-aggregation mode trigger — any beyond-line-filter
/// stage means the columnar store cannot express the aggregation and the
/// pipeline evaluates in-engine over the raw scan.
fn metric_pipeline_construct(pipeline: &[Stage]) -> Option<&'static str> {
    use pulsus_logql::ParserStage;
    pipeline.iter().find_map(|stage| match stage {
        // A pushable line filter is served by the columnar `sp.line_filters`
        // predicate; a non-pushable one (`ip(…)`/mixed-`or`) must force
        // in-engine client aggregation over the raw scan.
        Stage::LineFilter(lf) if is_pushable_line_filter(lf) => None,
        Stage::LineFilter(_) => Some("ip line filter"),
        Stage::Parser(ParserStage::Json { .. }) => Some("json"),
        Stage::Parser(ParserStage::Logfmt { .. }) => Some("logfmt"),
        Stage::Parser(ParserStage::Regexp(_)) => Some("regexp"),
        Stage::Parser(ParserStage::Pattern(_)) => Some("pattern"),
        Stage::LabelFilter(_) => Some("label filter"),
        Stage::LineFormat(_) => Some("line_format"),
        Stage::LabelFormat(_) => Some("label_format"),
        Stage::Unwrap(_) => Some("unwrap"),
        Stage::Unpack => Some("unpack"),
        Stage::Decolorize => Some("decolorize"),
        Stage::Drop(_) => Some("drop"),
        Stage::Keep(_) => Some("keep"),
    })
}

/// `force_client` (issue #221): `true` ONLY for the `variants(...)` scan
/// plan — the routing decision becomes `Raw` with its own named reason and
/// `client` is always `Some`, so the multi-extractor scan reads raw
/// `log_samples` and never the rollup table (which cannot serve unwraps,
/// per-variant `[range]` windows, or the common pipeline). Both
/// pre-existing call sites pass `false`, so every existing plan/SQL
/// snapshot is byte-identical.
/// The single wording for "the grammar accepts this, the engine does not
/// execute it yet" on a range-aggregation grouping (issue #344). One
/// constant so the top-level planner and the `variants(...)` arm cannot
/// drift apart, and so a grep finds every mention when execution lands.
const RANGE_GROUPING_UNSUPPORTED: &str =
    "range aggregation grouping is parsed but not yet executed (issue #344)";

fn metric_plan(
    metric_expr: &MetricExpr,
    p: &QueryParams,
    ctx: &PlanCtx<'_>,
    force_client: bool,
) -> Result<MetricPlan, ReadError> {
    // `0.is_multiple_of(_)` is trivially `true`, which would otherwise let
    // a zero step reach the routing decision below and pick rollup, then
    // render `intDiv(bucket_ns, 0)` — undefined in ClickHouse. The raw
    // fallback's own `intDiv(timestamp_ns, 0)` bucketing is equally
    // invalid, so this is checked before *any* routing choice is made,
    // making `intDiv(_, 0)` structurally unreachable regardless of what
    // request-level validation #13 later adds (task-manager resolution #4
    // on issue #12: "defense in depth, one cheap branch").
    if let QuerySpec::Range { step_ns: 0, .. } = p.spec {
        return Err(ReadError::InvalidStep);
    }

    let (base, raw_vector_aggs) = unwrap_vector_aggs(metric_expr);
    let MetricExpr::Range {
        op,
        range,
        param,
        grouping: range_grouping,
    } = base
    else {
        // `plan_metric_expr`/`build_metric_node` route every
        // `Literal`/`Binary`-bottomed expression to the node tree, so the
        // base reaching `metric_plan` is structurally always `Range`.
        unreachable!("metric_plan is only called on Vector-chains bottoming at MetricExpr::Range")
    };
    let vector_aggs =
        parse_vector_agg_params(&raw_vector_aggs, matches!(p.spec, QuerySpec::Range { .. }))?;

    // Issue M6-10: metric pipelines now execute in-engine. Classify the
    // query into the SQL-aggregated mode (the four count/bytes ops,
    // un-piped beyond line filters — byte-identical plans, rollup
    // auto-routing preserved) vs the client-aggregated mode (any
    // beyond-line-filter pipeline stage, any unwrap, or any of the new
    // over-time ops — full-window raw scan, complete-or-error).
    let pipeline = &range.selector.pipeline;
    let has_beyond_line_filter = metric_pipeline_construct(pipeline).is_some();
    let has_unwrap = pipeline.iter().any(|s| matches!(s, Stage::Unwrap(_))) ||
        // Defense in depth: the parser only emits the pipeline
        // `Stage::Unwrap` form and always leaves `LogRange::unwrap`
        // `None`.
        range.unwrap.is_some();

    // Unwrap arity — mirrors the oracle's parse errors verbatim
    // ("invalid aggregation X with/without unwrap", probed live).
    let requires_unwrap = matches!(
        op,
        RangeAggOp::SumOverTime
            | RangeAggOp::AvgOverTime
            | RangeAggOp::MinOverTime
            | RangeAggOp::MaxOverTime
            | RangeAggOp::StddevOverTime
            | RangeAggOp::StdvarOverTime
            | RangeAggOp::QuantileOverTime
            | RangeAggOp::FirstOverTime
            | RangeAggOp::LastOverTime
            | RangeAggOp::RateCounter
    );
    let forbids_unwrap = matches!(
        op,
        RangeAggOp::CountOverTime | RangeAggOp::BytesRate | RangeAggOp::BytesOverTime
    );
    if requires_unwrap && !has_unwrap {
        return Err(ReadError::PipelineInvalid {
            reason: format!("invalid aggregation {op} without unwrap"),
        });
    }
    if forbids_unwrap && has_unwrap {
        return Err(ReadError::PipelineInvalid {
            reason: format!("invalid aggregation {op} with unwrap"),
        });
    }
    // Issue #344: the grammar now accepts a range-aggregation grouping on
    // the eight ops the reference admits it on (the other seven are a
    // parse-time rejection carrying the reference's wording). EXECUTION is
    // a separate, larger piece of work: the grouping aggregates the group's
    // RAW SAMPLES — `stddev_over_time(...) by (fp)` over {1,5,7} is
    // 2.4944…, the population stddev of the merged samples, not a stddev of
    // per-series stddevs — so it re-keys the client aggregator's groups and
    // needs a total sample order across merged streams for
    // `first_over_time`/`last_over_time`. Until that lands the planner
    // refuses it by name rather than executing the ungrouped query and
    // silently returning the wrong series.
    //
    // Placed AFTER the unwrap-arity checks on purpose: those carry the
    // reference's own verbatim messages, and our not-yet-executed refusal
    // must never displace one. `max_over_time({a="b"}[5m]) by (fp)` answers
    // `invalid aggregation max_over_time without unwrap` on both systems.
    if range_grouping.is_some() {
        return Err(ReadError::PipelineInvalid {
            reason: RANGE_GROUPING_UNSUPPORTED.to_string(),
        });
    }
    let quantile = match (op, param) {
        (RangeAggOp::QuantileOverTime, Some(raw)) => {
            Some(parse_plan_number(raw, format_args!("quantile parameter"))?)
        }
        (RangeAggOp::QuantileOverTime, None) => {
            // The parser requires the parameter; re-checked for
            // programmatically-built ASTs.
            return Err(ReadError::PipelineInvalid {
                reason: "quantile_over_time requires a quantile parameter".to_string(),
            });
        }
        _ => None,
    };

    let client_only_op = requires_unwrap || matches!(op, RangeAggOp::AbsentOverTime);
    // Issue #227: EVERY range query is now client-aggregated with Loki's
    // sliding windows — the un-piped count/bytes/rate rollup fast-path is
    // retired for range reads (the 5s rollup cannot reproduce Loki's
    // per-event `(t-range, t]` boundary; only raw `log_samples` can).
    // Instant queries keep their existing routing (rollup/SQL-raw).
    let is_range = matches!(p.spec, QuerySpec::Range { .. });
    let client =
        if force_client || has_beyond_line_filter || has_unwrap || client_only_op || is_range {
            let value = if has_unwrap {
                ClientValue::Unwrap
            } else if matches!(op, RangeAggOp::BytesRate | RangeAggOp::BytesOverTime) {
                ClientValue::Bytes
            } else {
                ClientValue::Count
            };
            let absent_labels = if matches!(op, RangeAggOp::AbsentOverTime) {
                range
                    .selector
                    .selector
                    .matchers
                    .iter()
                    .filter(|m| m.op == MatchOp::Eq)
                    .map(|m| (m.name.clone(), m.value.clone()))
                    .collect()
            } else {
                Vec::new()
            };
            Some(ClientAgg {
                pipeline: pipeline.clone(),
                value,
                range_op: *op,
                param: quantile,
                absent_labels,
            })
        } else {
            None
        };

    // Issue #343: `offset d` evaluates the range selector over
    // `(T - d - range, T - d]` — the WHOLE time domain moves back by `d`,
    // measured against the pinned v3.7.4 container (3 old lines vs 7 recent
    // ones, with a `[70m]` control proving the window moved rather than the
    // data being absent).
    //
    // `offset 0s` is the identity, so an absent offset and a zero one are
    // the same window — `unwrap_or(0)` rather than a branch.
    let offset_ns = range.offset_ns.unwrap_or(0);
    // Issue #227 review round 2: VALIDATE the client-controlled `[range]`
    // duration at this boundary. Everything downstream carries the validated
    // `i64` — no `as i64` narrowing of client input exists past this line.
    let range_ns = validate_duration_ns(range.range.as_nanos(), "range selector")?;

    // Issue #227: `start_ns` is the (range-widened) SCAN lower bound;
    // `grid_start_ns` is the start-anchored emit grid's first point. Both
    // paths reproduce Loki: the window `(t-range, t]` is re-evaluated at
    // `t ∈ {grid_start + k·step ≤ end}`, so the scan must reach back a full
    // `range` before `grid_start`. `rate_window_ns = range` (never `step`)
    // is the `rate([1m]) ≠ rate([10m])` fix.
    //
    // Issue #343: every bound below is the OFFSET-SHIFTED evaluation
    // domain (`shift_by_offset`), so the scan, the partition months and
    // the emit grid all move together. The emitted point TIMESTAMPS are
    // put back on the caller's grid by `MetricPlan::offset_ns` (Loki
    // v3.7.4 `pkg/logql/range_vector.go` does exactly this: it starts the
    // iterator at `start-offset` and reports `current + offset`).
    // The SINGLE decision point for a shift that leaves the representable
    // timestamp axis (issue #343): on `None` from either bound, substitute
    // the degenerate empty evaluation domain and raise `empty_domain`.
    // Everything after this — SQL, routing, client window, probes — is
    // built from the substituted bounds unchanged, so a consumer that
    // never reads the flag still produces nothing.
    let (start_ns, scan_lower, end_ns, step_ns, grid_start_ns, rate_window_ns, empty_domain) =
        match p.spec {
            QuerySpec::Instant { at_ns } => match shift_by_offset(at_ns, offset_ns) {
                Some(at_ns) => {
                    let (start, lower) = widen_scan_start(at_ns, range_ns);
                    (start, lower, at_ns, None, start, Some(range_ns), false)
                }
                None => (
                    EMPTY_DOMAIN_GRID_START_NS,
                    ScanLowerBound::Exclusive,
                    EMPTY_DOMAIN_END_NS,
                    None,
                    EMPTY_DOMAIN_GRID_START_NS,
                    Some(range_ns),
                    true,
                ),
            },
            QuerySpec::Range {
                start_ns,
                end_ns,
                step_ns,
            } => {
                // Validate the client `step` at the same boundary as `[range]`
                // (issue #227 review round 2), so the whole evaluator works over
                // in-domain durations only: every later `step as i128` /
                // `step_ns > i64::MAX` guard is then provably never taken.
                // Ahead of the shift on purpose: a bad step is a 400 whether or
                // not the offset moves the window off the axis.
                let step = validate_duration_ns(step_ns, "step")?;
                match (
                    shift_by_offset(start_ns, offset_ns),
                    shift_by_offset(end_ns, offset_ns),
                ) {
                    (Some(start_ns), Some(end_ns)) => {
                        let (scan_start, lower) = widen_scan_start(start_ns, range_ns);
                        (
                            scan_start,
                            lower,
                            end_ns,
                            Some(step),
                            start_ns,
                            Some(range_ns),
                            false,
                        )
                    }
                    _ => (
                        EMPTY_DOMAIN_GRID_START_NS,
                        ScanLowerBound::Exclusive,
                        EMPTY_DOMAIN_END_NS,
                        Some(step),
                        EMPTY_DOMAIN_GRID_START_NS,
                        Some(range_ns),
                        true,
                    ),
                }
            }
        };

    let normalized = normalize_matchers(&range.selector.selector)?;
    let months = months_overlapping(start_ns, end_ns);
    let stage1_sql = super::sql::stage1(
        ctx.streams_idx,
        &months,
        &normalized.positive_branches,
        &normalized.negative_branches,
    );
    let probes = build_probes(ctx, &months, &normalized.probe_keys);

    let extra_predicates = compile_line_filters(&range.selector.pipeline)?;
    // A line filter constrains which log lines count; the rollup table
    // (`log_metrics_<res>`) has no `body` column to re-filter, so any
    // pipeline stage forces the raw fallback (docs/schemas.md §3.2: metric
    // reads "never touch samples for count-only rollup shapes" — that
    // guarantee only holds when there is nothing left for `log_samples` to
    // filter).
    //
    // Rollup eligibility is additionally gated on `client.is_none()`
    // (issue M6-10, removing the former guard-comment TODO): every
    // non-count op and every beyond-line-filter pipeline is client-
    // aggregated, always routed raw with its own named reason — the
    // rollup table can neither re-filter bodies nor produce unwrapped
    // values. `client.is_none()` count/bytes ops keep the pre-M6-10
    // routing and reasons byte-identically (the perf regression gate).
    // `Instant` is matched *first*, ahead of the line-filter/resolution
    // checks below: an instant window ([at - range, at]) has no step to
    // test against the resolution regardless of what else is true about
    // the query, so its reason must always be exactly "raw: instant query"
    // — never shadowed by an unrelated raw-fallback reason an instant query
    // also happens to satisfy (code review fix, issue #12: an instant query
    // that also carries a line filter, or runs with `rollup_res_ns == 0`,
    // must still report "raw: instant query", not "raw: line filter
    // present"/"raw: rollup resolution not configured"). schemas.md §3.2
    // ties eligibility strictly to "the query step is a multiple of the
    // resolution", and an unaligned window would silently diverge from raw
    // at bucket edges (task-manager resolution #1 on issue #12).
    let routing = if force_client {
        RoutingDecision {
            chosen: RouteChoice::Raw,
            reason: "raw: variants single-pass multi-extractor scan".to_string(),
        }
    } else if client.is_some() {
        // Issue #227: a plain (un-piped, non-unwrap) range aggregation now
        // reads raw and slides — name it distinctly from the pipeline/unwrap
        // client path so `X-Pulsus-Explain` stays truthful.
        let reason = if is_range && !(has_beyond_line_filter || has_unwrap || client_only_op) {
            "raw: sliding-window range aggregation (issue #227)".to_string()
        } else {
            "raw: client-side pipeline/unwrap aggregation".to_string()
        };
        RoutingDecision {
            chosen: RouteChoice::Raw,
            reason,
        }
    } else {
        match p.spec {
            QuerySpec::Instant { .. } => RoutingDecision {
                chosen: RouteChoice::Raw,
                reason: "raw: instant query".to_string(),
            },
            QuerySpec::Range { .. } if !extra_predicates.is_empty() => RoutingDecision {
                chosen: RouteChoice::Raw,
                reason: "raw: line filter present".to_string(),
            },
            QuerySpec::Range { .. } if ctx.rollup_res_ns == 0 => RoutingDecision {
                chosen: RouteChoice::Raw,
                reason: "raw: rollup resolution not configured".to_string(),
            },
            QuerySpec::Range { step_ns, .. } if step_ns.is_multiple_of(ctx.rollup_res_ns) => {
                RoutingDecision {
                    chosen: RouteChoice::Rollup,
                    reason: format!(
                        "rollup: step {step_ns} ns divisible by resolution {} ns",
                        ctx.rollup_res_ns
                    ),
                }
            }
            QuerySpec::Range { step_ns, .. } => RoutingDecision {
                chosen: RouteChoice::Raw,
                reason: format!(
                    "raw: step {step_ns} ns not a multiple of resolution {} ns",
                    ctx.rollup_res_ns
                ),
            },
        }
    };
    let rollup_eligible = matches!(routing.chosen, RouteChoice::Rollup);

    let is_bytes = matches!(op, RangeAggOp::BytesRate | RangeAggOp::BytesOverTime);
    // `rate_counter` divides its reset-aware increase by the window
    // seconds via the same `rate_window_ns` divisor as `rate`/`bytes_rate`.
    let is_rate = matches!(
        op,
        RangeAggOp::Rate | RangeAggOp::BytesRate | RangeAggOp::RateCounter
    );

    let (table, bucket_col, agg_expr) = if rollup_eligible {
        (
            ctx.rollup_table.to_string(),
            "bucket_ns",
            if is_bytes { "sum(bytes)" } else { "sum(count)" },
        )
    } else {
        (
            ctx.samples.to_string(),
            "timestamp_ns",
            if is_bytes {
                "sum(length(body))"
            } else {
                "count()"
            },
        )
    };

    Ok(MetricPlan {
        stage1_sql,
        streams_table: ctx.streams.to_string(),
        table,
        bucket_col,
        agg_expr,
        rollup: rollup_eligible,
        routing,
        extra_predicates,
        start_ns,
        scan_lower,
        end_ns,
        step_ns,
        grid_start_ns,
        range_ns,
        offset_ns,
        empty_domain,
        // Widening a boundary-validated positive `i64` to `u64` for
        // `apply_rate`'s divisor — provably lossless (issue #227 round 2).
        rate_window_ns: if is_rate {
            rate_window_ns.map(|ns| ns.as_u64())
        } else {
            None
        },
        op: *op,
        vector_aggs,
        client,
        probes,
    })
}

/// Widens a window's lower bound back by the `[range]` selector (Loki's
/// `(t-range, t]` lookback) and decides how the SQL predicate compares
/// against it (issue #227 review round 11). `checked_sub` is the crux:
///
/// * `Some(lo)` — the logical bound is representable, so the scan keeps
///   the reference's EXCLUSIVE `timestamp_ns > lo` — including a
///   legitimately-computed `lo == i64::MIN` (e.g. `start = i64::MIN + 1`,
///   `[1ns]`), where a sample stored at exactly `i64::MIN` is genuinely
///   outside the window.
/// * `None` — the subtraction UNDERFLOWED: the logical bound sits strictly
///   below `i64::MIN`, beneath every representable timestamp, so the
///   saturated `i64::MIN` is a VACUOUS bound and must render INCLUSIVELY
///   (`timestamp_ns >= i64::MIN`). The prior saturating form kept `>` and
///   silently dropped a sample stored at exactly `i64::MIN` that the
///   reference includes.
fn widen_scan_start(start_ns: i64, range_ns: ValidatedDuration) -> (i64, ScanLowerBound) {
    match start_ns.checked_sub(range_ns.get()) {
        Some(lo) => (lo, ScanLowerBound::Exclusive),
        None => (i64::MIN, ScanLowerBound::Inclusive),
    }
}

/// Moves one evaluation-domain instant back by `offset` ns (issue #343) —
/// ONCE, at the boundary, exactly as v3.7.4
/// `pkg/logql/range_vector.go:50-52` does (`start = start - offset;
/// end = end - offset`, tag `v3.7.4` /
/// `b318f2829f0ae2094ab3a1e90780450e9e4b03be`), so every comparison
/// downstream runs offset-free in the shifted domain and cannot forget it.
///
/// **SIGNED, and that is the whole point.** The reference accepts a
/// NEGATIVE offset and shifts the window FORWARD, into the future
/// (`rate({app="x"}[5m] offset -1h)` is a v3.7.4 **200**, returning empty
/// against a fixture with no future data). Written as an absolute-value
/// subtraction this reads correctly and silently evaluates the wrong
/// window for every negative offset — a quietly wrong time window, which
/// is the worst outcome this construct can produce. One subtraction, sign
/// included, is the reason this is a named function rather than an inline
/// `-`.
///
/// `checked_sub`, not `saturating_sub`. `None` means the shifted
/// evaluation domain has left the representable timestamp axis and the
/// query answers EMPTY. The reference's plain int64 subtraction WRAPS
/// there, relocating the window to an unrelated instant; saturating
/// CLAMPS, which is what shipped and what scanned from 1977-01-08 for a
/// 2026 query (`count_over_time({env="prod"}[2500000h] offset -2500000h)`
/// rendered `timestamp_ns > 223372036854775807`). The residual — the
/// handful of shapes where answering empty is not the exact answer either
/// — is ledgered as `offset-domain-edge-exact-arithmetic`.
///
/// There is still no new rejection surface: a domain-crossing offset is a
/// 200 answering empty, never a 400.
fn shift_by_offset(instant_ns: i64, offset_ns: i64) -> Option<i64> {
    instant_ns.checked_sub(offset_ns)
}

/// The grid lower bound of the DEGENERATE empty evaluation domain
/// substituted when [`shift_by_offset`] leaves the representable
/// timestamp axis (issue #343). Paired with [`EMPTY_DOMAIN_END_NS`],
/// `0 / -1` is the minimal pair with `end < grid_start`, which is what
/// makes [`super::window::grid_point_count`] return 0 and
/// `fence_intervals` return 0 (so `kmax = -1` and no grid point exists),
/// and what makes [`months_overlapping`] — which takes
/// `end_ns.max(start_ns)` — yield the single literal `'1970-01-01'`.
const EMPTY_DOMAIN_GRID_START_NS: i64 = 0;

/// The upper bound of the degenerate empty evaluation domain; see
/// [`EMPTY_DOMAIN_GRID_START_NS`].
const EMPTY_DOMAIN_END_NS: i64 = -1;

/// Unwraps every outer `MetricExpr::Vector` layer, returning the
/// innermost non-`Vector` expression and the aggregation chain (with raw
/// parameters) in outer-to-inner order (`sum by (svc) (avg(...))` yields
/// `[(Sum, Some(by(svc)), None)]` first, then deeper wrappers after).
fn unwrap_vector_aggs(expr: &MetricExpr) -> (&MetricExpr, Vec<RawVectorAggSpec<'_>>) {
    let mut aggs = Vec::new();
    let base = unwrap_vector_aggs_into(expr, &mut aggs);
    (base, aggs)
}

/// Fills `out` (cleared first) with borrowed handles — allocates only
/// when `out`'s capacity grows, so ONE buffer reused across a variants
/// query's variant loop is N-independent (issue #221, member M5). Depth
/// is parser-bounded (`MAX_DEPTH = 64`) for every parser-produced AST.
fn unwrap_vector_aggs_into<'a>(
    expr: &'a MetricExpr,
    out: &mut Vec<RawVectorAggSpec<'a>>,
) -> &'a MetricExpr {
    out.clear();
    // Issue #272: an ALLOCATION-FREE spine descent — one loop variable,
    // no `ChunkStack`, no heap on any path — so the only allocations in
    // this window remain `out`'s own growth.
    let d = walk::descend_spine::<pulsus_logql::MetricScc, &'a MetricExpr>(
        pulsus_logql::MeNode::Expr(expr),
        |n| match n {
            pulsus_logql::MeNode::Expr(MetricExpr::Vector {
                op,
                grouping,
                param,
                ..
            }) => {
                out.push((*op, grouping.as_ref(), param.as_deref()));
                ControlFlow::Continue(())
            }
            // Every other `MetricExpr` ends the vector chain — the base.
            pulsus_logql::MeNode::Expr(e) => ControlFlow::Break(e),
            // Unreachable: the only `Continue` arm is `Expr(Vector)`,
            // whose sole child is an `Expr`; a `Var` node is reached only
            // by descending through `MetricExpr::Variants`, which the arm
            // above breaks at.
            pulsus_logql::MeNode::Var(_) => {
                unreachable!("descent breaks at `Variants` before its child")
            }
        },
    );
    match d {
        walk::Descent::Broke(base) => base,
        // Unreachable: `Vector` always has arity 1, so descent cannot
        // exhaust while `f` continues. Not a `debug_assert` — a release
        // panic here is correct and this is not a `Drop` path.
        walk::Descent::Exhausted(_) => unreachable!("`Vector` always has exactly one child"),
    }
}

/// The number of sub-states (variants) a single `variants(...)` query may
/// declare — the DERIVED backstop (issue #221): the smallest
/// [`AggCaps::DEFAULT`] field, so every [`AggCaps::divided`] cap stays
/// ≥ 1 and the per-field TOTAL over sub-states remains exactly today's
/// single-query bound. Derived, not chosen.
///
/// Issue #236 deleted `AggCaps::series` (the mid-scan 500-group cap), so
/// the smallest field is no longer that 500: it is now
/// [`super::charge::MAX_TS_COLLISION_GROUP`] = **10 000**, a strictly
/// PERMISSIVE re-derivation in the direction the reference sits (which is
/// unbounded here — a recorded divergence,
/// `docs/benchmarks/logs-differential-ledger.md`).
pub const MAX_VARIANT_SUB_STATES: u64 = AggCaps::DEFAULT.min_field();

/// The COMMON pipeline the reference executes for a `variants(...)` scan:
/// everything BEFORE the first `Stage::Unwrap` (issue #221 Δ1). A
/// common-range `unwrap` and its post-`unwrap` label filters are dead
/// syntax inside `variants(...) of (...)` — the reference's common
/// extraction is `logRange.Left.Pipeline()`, and its `Unwrap` is a
/// sibling field it never reads. The parser guarantees only label filters
/// follow `unwrap`, so the truncated prefix IS `Left`.
fn common_stages(pipeline: &[Stage]) -> &[Stage] {
    match pipeline.iter().position(|s| matches!(s, Stage::Unwrap(_))) {
        Some(i) => &pipeline[..i],
        None => pipeline,
    }
}

/// A variant's own executable TAIL: its pipeline from its first
/// `Stage::Unwrap` (the unwrap plus the post-`unwrap` label filters the
/// reference honours — `ReduceAndLabelFilter(PostFilters)`); empty for
/// every non-unwrap reducer. Everything before it — the variant's own
/// selector, line filters, parsers, formatters — is dead syntax.
fn variant_tail(pipeline: &[Stage]) -> &[Stage] {
    match pipeline.iter().position(|s| matches!(s, Stage::Unwrap(_))) {
        Some(i) => &pipeline[i..],
        None => &[],
    }
}

pub use variant_spec::VariantSpec;

mod variant_spec {
    use super::*;

    /// One variant's reducer, derived from the variant metric expression
    /// (issue #221). The variant's own selector, line filters, parsers
    /// and formatters are DISCARDED (reference:
    /// `variantRangeAggExprExtractor` passes `nil` stages — live-probed).
    ///
    /// COMPILE-ENFORCED: every `VariantSpec` in existence was sized and
    /// charged, because the fields are private to this module and
    /// [`VariantSpec::try_new`] is the only constructor; and no
    /// per-variant owned payload can be built outside this module,
    /// because `try_new` takes borrowed inputs only. NOT
    /// compile-enforced: that no unrelated temporary is allocated before
    /// the charge — Rust cannot express that. That residual is policed by
    /// the `ALLOC_CALLS`/`TOTAL_BYTES` slope gates in
    /// `tests/logql_variants_alloc.rs` and narrowed by the census.
    #[derive(Debug, Clone, PartialEq)]
    pub struct VariantSpec {
        /// `__variant__`'s numeric source: the position in the source
        /// expression (`strconv.Itoa(i)` in the reference — plain
        /// decimal, no padding).
        index: usize,
        /// `client.pipeline` is this variant's UNWRAP TAIL ONLY (empty
        /// for every non-unwrap reducer) — NEVER the common pipeline,
        /// which lives once in the scan plan's `client.pipeline`. exec
        /// runs `common ++ tail` through the [`super::super::variants::VariantArena`];
        /// nothing may compile `client.pipeline` on its own.
        client: ClientAgg,
        /// This variant's OWN evaluation window (its `[range]`) on the
        /// SHARED grid. The SCAN window stays the common range's, so a
        /// wider variant range simply sees fewer rows (reference-probed).
        window: ClientWindow,
        /// `true` iff the instant window's lower bound UNDERFLOWED i64
        /// during widening ([`widen_scan_start`]) — the bound is then
        /// vacuous and compares inclusively, mirroring
        /// [`super::super::sql::ScanLowerBound::Inclusive`] on the scan
        /// path. Always `false` for a range window.
        instant_lower_inclusive: bool,
        rate_window_ns: Option<u64>,
        /// 0 or 1 outer vector aggregation with `__variant__` already
        /// injected into its grouping (BOTH `by` and `without`; under
        /// `without` this deliberately STRIPS it — the reference's
        /// observed behaviour) and the label list re-sorted.
        vector_aggs: Vec<VectorAggSpec>,
    }

    impl VariantSpec {
        /// The SOLE `VariantSpec` construction site in the crate. Order
        /// (normative, single body): size from borrowed inputs
        /// ([`super::super::variants::variant_spec_bytes`]) → charge
        /// (`charge_fanout_bytes`, a 422
        /// [`TooBroadReason::VariantSpecBytes`] on breach) → clone →
        /// construct. Allocates nothing before the charge returns `Ok`.
        /// The injected grouping list is sorted with `sort_unstable`
        /// (`sort` allocates a scratch buffer, `sort_unstable` does not).
        #[allow(clippy::too_many_arguments)]
        pub(crate) fn try_new(
            charged: &mut u64,
            cap: u64,
            index: usize,
            tail: &[Stage],
            selector: &StreamSelector,
            raw_aggs: &[RawVectorAggSpec<'_>],
            agg_chain: &MetricExpr,
            range_op: RangeAggOp,
            value: ClientValue,
            param: Option<f64>,
            is_range: bool,
            window: ClientWindow,
            instant_lower_inclusive: bool,
            rate_window_ns: Option<u64>,
        ) -> Result<Self, ReadError> {
            let is_absent = matches!(range_op, RangeAggOp::AbsentOverTime);
            let bytes =
                super::super::variants::variant_spec_bytes(tail, selector, is_absent, agg_chain);
            crate::logql::variants::charge_fanout_bytes(charged, bytes, cap).map_err(
                |(bytes, cap)| {
                    ReadError::QueryTooBroad(TooBroadReason::VariantSpecBytes { bytes, cap })
                },
            )?;
            // Charged — the clones below are what the charge paid for.
            let mut vector_aggs = parse_vector_agg_params(raw_aggs, is_range)?;
            for (_, grouping, _) in &mut vector_aggs {
                // `__variant__` is injected into the grouping for BOTH
                // `by` and `without` (under `without` this deliberately
                // STRIPS it), creating `by (__variant__)` when the
                // aggregation had none — the reference's non-nil default
                // `Grouping` (`mustNewVectorAggregationExpr`). Guarded
                // (issue #288): a variant that already groups
                // `by (__variant__)` must not gain a DUPLICATE entry —
                // the specs are deduplicated by `parse_vector_agg_params`
                // and re-duplicating here would re-open the emitted-dupe
                // defect (live-probed: the reference returns a single
                // `__variant__` for that shape).
                let g = grouping.get_or_insert_with(|| Grouping {
                    kind: GroupingKind::By,
                    labels: Vec::new(),
                });
                let variant_label = super::super::variants::VARIANT_LABEL;
                if !g.labels.iter().any(|l| l == variant_label) {
                    g.labels.push(variant_label.to_string());
                    g.labels.sort_unstable();
                }
            }
            // `absent_over_time`'s synthetic labels come from the
            // VARIANT'S OWN (otherwise dead) selector, not the common one
            // (issue #221 Δ2: `absentLabels(expr)` reads
            // `expr.Selector() = e.Left.Left`). Sorted so
            // `append_variant_label`'s key-sorted insertion holds.
            let mut absent_labels: Vec<(String, String)> = if is_absent {
                selector
                    .matchers
                    .iter()
                    .filter(|m| m.op == MatchOp::Eq)
                    .map(|m| (m.name.clone(), m.value.clone()))
                    .collect()
            } else {
                Vec::new()
            };
            absent_labels.sort_unstable();
            Ok(VariantSpec {
                index,
                client: ClientAgg {
                    pipeline: tail.to_vec(),
                    value,
                    range_op,
                    param,
                    absent_labels,
                },
                window,
                instant_lower_inclusive,
                rate_window_ns,
                vector_aggs,
            })
        }

        pub fn index(&self) -> usize {
            self.index
        }

        pub fn client(&self) -> &ClientAgg {
            &self.client
        }

        pub fn window(&self) -> ClientWindow {
            self.window
        }

        pub fn rate_window_ns(&self) -> Option<u64> {
            self.rate_window_ns
        }

        pub fn vector_aggs(&self) -> &[VectorAggSpec] {
            &self.vector_aggs
        }

        /// Whether a scanned row at `ts_ns` falls inside THIS variant's
        /// instant window `(at - range, at]` (issue #221): the scan is
        /// bounded by the COMMON range only, so a variant with a shorter
        /// `[range]` must exclude the older rows in-engine. Always `true`
        /// for a range window — the sliding evaluator's own `(t-range, t]`
        /// windows apply the variant range there.
        pub fn admits_instant(&self, ts_ns: i64) -> bool {
            match self.window {
                ClientWindow::Instant { start_ns, end_ns } => {
                    ts_ns <= end_ns
                        && if self.instant_lower_inclusive {
                            ts_ns >= start_ns
                        } else {
                            ts_ns > start_ns
                        }
                }
                ClientWindow::Range { .. } => true,
            }
        }
    }

    /// The W-MEM field-addition guard (issue #221): adding a field to
    /// `VariantSpec` breaks this test's compilation, forcing the
    /// type-closure walk (charged in [`super::super::variants::variant_spec_bytes`])
    /// to be re-run for it before it can ship. Lives INSIDE the module
    /// because the fields are deliberately private (the sole-constructor
    /// invariant); the indented `cfg(test)` stays out of the column-0
    /// census split.
    #[cfg(test)]
    mod field_guard {
        use super::*;

        #[test]
        fn every_variant_spec_field_is_accounted() {
            let expr = pulsus_logql::parse(r#"sum(count_over_time({a="b"}[5m]))"#).expect("parse");
            let pulsus_logql::Expr::Metric(me) = &expr else {
                panic!()
            };
            let (base, raw) = unwrap_vector_aggs(me);
            let MetricExpr::Range { op, range, .. } = base else {
                panic!()
            };
            let mut charged = 0u64;
            let spec = VariantSpec::try_new(
                &mut charged,
                u64::MAX,
                7,
                &[],
                &range.selector.selector,
                &raw,
                me,
                *op,
                ClientValue::Count,
                None,
                false,
                ClientWindow::Instant {
                    start_ns: 0,
                    end_ns: 60,
                },
                false,
                None,
            )
            .expect("try_new");
            // Exhaustive, no `..` — each binding annotated with its
            // W-MEM bucket and charge term.
            let VariantSpec {
                index,                        // S
                client,                       // H — tail/absent terms
                window,                       // S (Copy)
                instant_lower_inclusive: _il, // S
                rate_window_ns: _rw,          // S
                vector_aggs,                  // C+H — grown buffer + grouping terms
            } = spec;
            assert_eq!(index, 7);
            assert!(client.pipeline.is_empty());
            assert!(matches!(window, ClientWindow::Instant { .. }));
            assert_eq!(vector_aggs.len(), 1);
        }
    }
}

/// Validates and plans a `variants(...) of (...)` expression (issue
/// #221): ONE scan planned from the truncated common range, plus one
/// [`VariantSpec`] per variant. Order is normative — count gate (no
/// allocation) → spec-vector buffer charge → reservation → per variant:
/// shape gate (no allocation) → charge (`VariantSpec::try_new`) →
/// materialize → push.
fn build_variants_node(
    v: &VariantsExpr,
    p: &QueryParams,
    ctx: &PlanCtx<'_>,
) -> Result<MetricNode, ReadError> {
    let n = v.variants.len() as u64;
    if n > MAX_VARIANT_SUB_STATES {
        return Err(ReadError::QueryTooBroad(TooBroadReason::VariantSubStates {
            count: n,
            cap: MAX_VARIANT_SUB_STATES,
        }));
    }
    let cap = super::variants::MAX_VARIANT_FANOUT_STATE_BYTES;
    let mut charged: u64 = 0;
    // C2: the spec vector's own buffer, charged after the count gate and
    // before the exact reservation below — no P5 residue remains in the
    // plan-time list.
    crate::logql::variants::charge_fanout_bytes(
        &mut charged,
        super::variants::vec_buffer_bytes::<VariantSpec>(n),
        cap,
    )
    .map_err(|(bytes, cap)| {
        ReadError::QueryTooBroad(TooBroadReason::VariantSpecBytes { bytes, cap })
    })?;
    let mut variants: Vec<VariantSpec> = Vec::with_capacity(v.variants.len());

    // The ONE scan, planned from the COMMON log range alone (its pipeline
    // truncated at the first `unwrap` — Δ1). `Rate` is the deliberate
    // synthesis op: it is in neither `requires_unwrap` nor
    // `forbids_unwrap`, so a common range carrying `| unwrap` is not
    // spuriously rejected by an arity rule that belongs to the variants.
    // The emitted SQL depends only on table/services/fingerprints/window/
    // scan_lower/extra_predicates — all op-independent — so it is
    // byte-identical to the client-aggregated `count_over_time(<common
    // range>)` plan for the same range.
    let scan_expr = MetricExpr::Range {
        op: RangeAggOp::Rate,
        range: LogRange {
            selector: LogExpr {
                selector: v.range.selector.selector.clone(),
                pipeline: common_stages(&v.range.selector.pipeline).to_vec(),
            },
            range: v.range.range,
            unwrap: None,
            // Issue #343: the variants scan expression inherits the
            // source range selector's offset, so the guard above sees it
            // rather than this synthesised node silently dropping it.
            offset_ns: v.range.offset_ns,
        },
        param: None,
        // The synthesised common-range scan carries no grouping: the
        // emitted SQL is op- and grouping-independent (issue #344).
        grouping: None,
    };
    let scan = metric_plan(&scan_expr, p, ctx, /*force_client=*/ true)?;

    let is_range = matches!(p.spec, QuerySpec::Range { .. });
    // ONE raw handle buffer, reused (cleared) across the variant loop —
    // never the allocating wrapper here (issue #221 member M5).
    let mut raw_buf: Vec<RawVectorAggSpec<'_>> = Vec::new();
    for (index, variant) in walk::slice_of(v.variants.peek()).iter().enumerate() {
        let base = unwrap_vector_aggs_into(variant, &mut raw_buf);
        // One rejection for every non-conforming shape — the reference
        // 500s (three different texts plus a nil-pointer panic), which is
        // not a matchable contract; both sides reject (ledgered).
        let reject = |index: usize| ReadError::PipelineInvalid {
            reason: format!(
                "variant {index} must be a range aggregation, optionally wrapped in one \
                 vector aggregation (e.g. variants(count_over_time({{app=\"x\"}}[5m]), \
                 sum by (level) (rate({{app=\"x\"}}[5m]))) of ({{app=\"x\"}}[5m]))"
            ),
        };
        if raw_buf.len() > 1 {
            return Err(reject(index));
        }
        // `approx_topk` is rejected like every other non-conforming
        // variant (the reference 500s `expected aggregation operator but
        // got "approx_topk"`).
        if raw_buf
            .iter()
            .any(|(op, ..)| matches!(op, VectorAggOp::ApproxTopk))
        {
            return Err(reject(index));
        }
        let MetricExpr::Range {
            op,
            range,
            param,
            grouping,
        } = base
        else {
            return Err(reject(index));
        };
        // Issue #344: a range-aggregation grouping is parsed but not yet
        // executed; inside `variants(...)` it is rejected by the same
        // named error the top-level planner uses, never silently dropped.
        if grouping.is_some() {
            return Err(ReadError::PipelineInvalid {
                reason: RANGE_GROUPING_UNSUPPORTED.to_string(),
            });
        }

        // Arity is decided by the VARIANT's own expression, not the
        // common range (Δ1) — the reference messages verbatim.
        let pipeline = &range.selector.pipeline;
        let has_unwrap =
            pipeline.iter().any(|s| matches!(s, Stage::Unwrap(_))) || range.unwrap.is_some();
        let requires_unwrap = matches!(
            op,
            RangeAggOp::SumOverTime
                | RangeAggOp::AvgOverTime
                | RangeAggOp::MinOverTime
                | RangeAggOp::MaxOverTime
                | RangeAggOp::StddevOverTime
                | RangeAggOp::StdvarOverTime
                | RangeAggOp::QuantileOverTime
                | RangeAggOp::FirstOverTime
                | RangeAggOp::LastOverTime
                | RangeAggOp::RateCounter
        );
        let forbids_unwrap = matches!(
            op,
            RangeAggOp::CountOverTime | RangeAggOp::BytesRate | RangeAggOp::BytesOverTime
        );
        if requires_unwrap && !has_unwrap {
            return Err(ReadError::PipelineInvalid {
                reason: format!("invalid aggregation {op} without unwrap"),
            });
        }
        if forbids_unwrap && has_unwrap {
            return Err(ReadError::PipelineInvalid {
                reason: format!("invalid aggregation {op} with unwrap"),
            });
        }
        let quantile = match (op, param) {
            (RangeAggOp::QuantileOverTime, Some(raw)) => {
                Some(parse_plan_number(raw, format_args!("quantile parameter"))?)
            }
            (RangeAggOp::QuantileOverTime, None) => {
                return Err(ReadError::PipelineInvalid {
                    reason: "quantile_over_time requires a quantile parameter".to_string(),
                });
            }
            _ => None,
        };
        let tail = variant_tail(pipeline);
        let value = if has_unwrap {
            ClientValue::Unwrap
        } else if matches!(op, RangeAggOp::BytesRate | RangeAggOp::BytesOverTime) {
            ClientValue::Bytes
        } else {
            ClientValue::Count
        };
        // Issue #343: **the VARIANT's own offset**, not the common range's.
        //
        // `variants(...)` fetches ONCE, from the common range's window (that
        // range's own offset included), and evaluates each variant's whole
        // expression over exactly those rows — so a variant reads its
        // shifted window INTERSECTED with the common one. Measured against
        // the pinned v3.7.4 container with a seeded store: a variant
        // `[5m] offset 1h` under a common `[63m30s]` that reaches only two
        // of the three old lines answers **2** — not 3 (which a per-variant
        // scan would give) and not empty. Under a common `[70m]` the same
        // variant answers 3; under a common `[5m]` it answers 200-EMPTY.
        //
        // Two corrections are pinned by that measurement, both of which read
        // as improvements and are not:
        //
        // * The reference does NOT reject a variant whose offset differs
        //   from the common range's — every such shape is a 200. An earlier
        //   draft of this refused them by name, which 400s the query the
        //   reference answers 3.
        // * Widening the shared scan to COVER the union of the variants'
        //   shifted windows would answer 3 where the reference answers
        //   empty. It is not a fix; it is a divergence.
        //
        // Reading the variant's own offset here is the whole of it: the
        // intersection then falls out of the single shared scan, exactly as
        // it does there.
        let offset_ns = range.offset_ns.unwrap_or(0);
        // The variant's OWN `[range]`, validated at the boundary, on the
        // SHARED grid (`grid_start_ns`/`end_ns`/`step_ns` equal the scan
        // plan's; only `range_ns` differs).
        let range_ns = validate_duration_ns(range.range.as_nanos(), "range selector")?;
        // Issue #343: the degenerate-window substitution is PER VARIANT,
        // never whole-plan. A variant whose own shift leaves the axis gets
        // the empty window and its siblings are untouched — a whole-plan
        // refusal here would 400 a query the reference serves 200.
        let (window, instant_lower_inclusive) = match p.spec {
            QuerySpec::Instant { at_ns } => match shift_by_offset(at_ns, offset_ns) {
                Some(at_ns) => {
                    let (lo, bound) = widen_scan_start(at_ns, range_ns);
                    (
                        ClientWindow::Instant {
                            start_ns: lo,
                            end_ns: at_ns,
                        },
                        matches!(bound, ScanLowerBound::Inclusive),
                    )
                }
                // `instant_lower_inclusive = false` makes `admits_instant`
                // demand `ts > 0 && ts <= -1`, which admits no timestamp.
                None => (
                    ClientWindow::Instant {
                        start_ns: EMPTY_DOMAIN_GRID_START_NS,
                        end_ns: EMPTY_DOMAIN_END_NS,
                    },
                    false,
                ),
            },
            QuerySpec::Range {
                start_ns,
                end_ns,
                step_ns,
            } => {
                let step = validate_duration_ns(step_ns, "step")?;
                let (grid_start_ns, end_ns) = match (
                    shift_by_offset(start_ns, offset_ns),
                    shift_by_offset(end_ns, offset_ns),
                ) {
                    (Some(start_ns), Some(end_ns)) => (start_ns, end_ns),
                    _ => (EMPTY_DOMAIN_GRID_START_NS, EMPTY_DOMAIN_END_NS),
                };
                (
                    ClientWindow::Range {
                        grid_start_ns,
                        end_ns,
                        step_ns: step,
                        range_ns,
                        offset_ns,
                    },
                    false,
                )
            }
        };
        let rate_window_ns = if matches!(
            op,
            RangeAggOp::Rate | RangeAggOp::BytesRate | RangeAggOp::RateCounter
        ) {
            Some(range_ns.as_u64())
        } else {
            None
        };
        let spec = VariantSpec::try_new(
            &mut charged,
            cap,
            index,
            tail,
            &range.selector.selector,
            &raw_buf,
            variant,
            *op,
            value,
            quantile,
            is_range,
            window,
            instant_lower_inclusive,
            rate_window_ns,
        )?;
        variants.push(spec);
    }
    Ok(MetricNode::Variants {
        scan: Box::new(scan),
        variants,
        spec_bytes: charged,
    })
}

/// Streams (log-selector) queries always evaluate over an explicit
/// `[start, end]` window (`Range`'s bounds); an `Instant` spec has no
/// natural range for a bare selector (that concept only exists for range
/// aggregations, which carry their own `[duration]`) and degenerates to the
/// zero-width instant `[at, at]` — callers needing an instant *log* read
/// with lookback are expected to translate that at the #13 layer before
/// calling `plan` (task-manager resolution #3 is scoped to metric queries).
fn window_bounds_for_streams(spec: &QuerySpec) -> (i64, i64) {
    match *spec {
        QuerySpec::Range {
            start_ns, end_ns, ..
        } => (start_ns, end_ns),
        QuerySpec::Instant { at_ns } => (at_ns, at_ns),
    }
}

fn build_probes(ctx: &PlanCtx<'_>, months: &[String], probe_keys: &[String]) -> Vec<ProbePlan> {
    probe_keys
        .iter()
        .map(|key| ProbePlan {
            key: key.clone(),
            sql: super::sql::probe(ctx.streams_idx, months, &ch_string(key)),
        })
        .collect()
}

/// The result of normalizing a [`StreamSelector`]'s matchers into stage 1's
/// single-pass shape (architect plan amendment §1).
#[derive(Debug)]
struct NormalizedMatchers {
    /// One pre-rendered, parenthesized OR-branch per **distinct positive
    /// key** — the collapse that keeps `HAVING uniqExact(key, val) = n`
    /// (or its `If`-conditional form) valid (architect plan: "Matcher
    /// normalisation").
    positive_branches: Vec<String>,
    /// One pre-rendered, parenthesized OR-branch per negative matcher
    /// (`Neq`/`Nre`) — deliberately *not* collapsed per key: `countIf(...)
    /// = 0` is correct whether one or several negative branches target the
    /// same key.
    negative_branches: Vec<String>,
    /// Distinct label keys carrying a regex matcher (`Re` or `Nre`) — the
    /// only case that warrants a selectivity probe (architect plan:
    /// "Selectivity probes").
    probe_keys: Vec<String>,
}

/// One label key's positive matchers, collapsed to a single condition
/// (architect plan: "Eq+Eq same key/value dedups, Eq+Re same key ANDs
/// both, two different Eq values is `ContradictoryMatchers`").
struct PositiveGroup {
    key: String,
    eq_value: Option<String>,
    re_patterns: Vec<String>,
}

fn push_probe_key(probe_keys: &mut Vec<String>, key: &str) {
    if !probe_keys.iter().any(|k| k == key) {
        probe_keys.push(key.to_string());
    }
}

/// Partitions and collapses a selector's matchers per the architect plan's
/// normalization rules (see [`NormalizedMatchers`] field docs).
fn normalize_matchers(selector: &StreamSelector) -> Result<NormalizedMatchers, ReadError> {
    let mut positive_order: Vec<String> = Vec::new();
    let mut positive_groups: HashMap<String, PositiveGroup> = HashMap::new();
    let mut negative_branches: Vec<String> = Vec::new();
    let mut probe_keys: Vec<String> = Vec::new();

    for Matcher { name, op, value } in &selector.matchers {
        match op {
            MatchOp::Eq => {
                let group = positive_groups.entry(name.clone()).or_insert_with(|| {
                    positive_order.push(name.clone());
                    PositiveGroup {
                        key: name.clone(),
                        eq_value: None,
                        re_patterns: Vec::new(),
                    }
                });
                match &group.eq_value {
                    Some(existing) if existing != value => {
                        return Err(ReadError::ContradictoryMatchers);
                    }
                    _ => group.eq_value = Some(value.clone()),
                }
            }
            MatchOp::Re => {
                push_probe_key(&mut probe_keys, name);
                let group = positive_groups.entry(name.clone()).or_insert_with(|| {
                    positive_order.push(name.clone());
                    PositiveGroup {
                        key: name.clone(),
                        eq_value: None,
                        re_patterns: Vec::new(),
                    }
                });
                if !group.re_patterns.iter().any(|p| p == value) {
                    group.re_patterns.push(value.clone());
                }
            }
            MatchOp::Neq => {
                negative_branches.push(format!(
                    "(key = {} AND val = {})",
                    ch_string(name),
                    ch_string(value)
                ));
            }
            MatchOp::Nre => {
                push_probe_key(&mut probe_keys, name);
                negative_branches.push(format!(
                    "(key = {} AND match(val, {}))",
                    ch_string(name),
                    ch_regex_anchored_checked(value)?
                ));
            }
        }
    }

    if positive_order.is_empty() {
        return Err(ReadError::EmptyMatcherSet);
    }

    let mut positive_branches: Vec<String> = Vec::with_capacity(positive_order.len());
    for key in &positive_order {
        let group = &positive_groups[key];
        let mut conds = vec![format!("key = {}", ch_string(&group.key))];
        if let Some(v) = &group.eq_value {
            conds.push(format!("val = {}", ch_string(v)));
        }
        for pat in &group.re_patterns {
            conds.push(format!("match(val, {})", ch_regex_anchored_checked(pat)?));
        }
        positive_branches.push(format!("({})", conds.join(" AND ")));
    }

    Ok(NormalizedMatchers {
        positive_branches,
        negative_branches,
        probe_keys,
    })
}

/// Compiles the **pushed-down** pipeline `LineFilter` stages — those
/// positioned before the first `line_format` — into stage-3 predicate
/// fragments, in pipeline order (architect plan amendment: line filters
/// "ALWAYS paired with the exact predicate"). Filters before a parser
/// still push down (parsers read but never rewrite the line — the
/// M6-09 skip-index-preservation gate, `tests/explain_indexes.rs`);
/// filters after a `line_format` reference the rewritten line and are
/// deliberately absent here (evaluated in-engine instead).
///
/// Fallible since issue #240: a pushed-down regex is validated (by
/// compiling exactly the unanchored form the SQL emits) BEFORE any I/O,
/// so an uncompilable pattern is a 400 at plan time, never a ClickHouse
/// 500 mid-query. This is the right seam — not `pipeline.rs`'s pushdown
/// short-circuit — because `/api/logs/v1/stats` is pushdown-only and
/// never compiles a client pipeline (`exec.rs` plans via `plan::plan`),
/// so a validator behind the client-compile path would leave `stats`
/// still returning 500.
pub(crate) fn compile_line_filters(pipeline: &[Stage]) -> Result<Vec<String>, ReadError> {
    let mut out = Vec::new();
    for stage in pipeline {
        match stage {
            // Non-pushable line filters (`ip(…)` / any `or` alternative that
            // is an `ip`) have no literal/token prefilter and are evaluated
            // in the client pipeline — never emit SQL for them here (doing so
            // would drop lines the client scan must keep / re-test).
            Stage::LineFilter(lf) if is_pushable_line_filter(lf) => {
                out.push(compile_line_filter(lf)?)
            }
            Stage::LineFilter(_) => {}
            // `line_format`/`decolorize`/`unpack` rewrite the line — a line
            // filter after any of them references the rewritten text and must
            // NOT push down (issue #200).
            Stage::LineFormat(_) | Stage::Decolorize | Stage::Unpack => break,
            _ => {}
        }
    }
    Ok(out)
}

/// The single source of truth for "does this line filter push down to SQL,
/// or must it run in the client pipeline?" Consulted at every site that
/// decides SQL-vs-client for a `Stage::LineFilter` (this module's
/// `compile_line_filters`, `has_unpushed_dropping_stage`,
/// `metric_pipeline_construct`; `pipeline.rs`'s compile/`line_filter_only`;
/// `exec.rs`'s `stats` gate) so the two paths never drift.
///
/// An `ip(…)` alternative is a range test over IP-shaped substrings — it has
/// no `tokenbf_v1`/`hasToken` prefilter and cannot prune granules, so it (and
/// any `or` group containing one) evaluates client-side. A pure literal/regex
/// `or` group pushes down as a disjunction that preserves each disjunct's
/// token prefilter.
pub(crate) fn is_pushable_line_filter(lf: &LineFilter) -> bool {
    !lf.value_is_ip && lf.or_matches.iter().all(|m| !m.is_ip)
}

/// ClickHouse's `tokenbf_v1` splits on non-alphanumeric ASCII; a `hasToken`
/// prefilter must extract tokens the same way or it misses granules that
/// truly contain the phrase.
fn tokenize(literal: &str) -> Vec<String> {
    literal
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

const REGEX_METACHARS: &[char] = &[
    '.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '\\',
];

/// Conservative, safe-by-construction heuristic: a pattern with zero regex
/// metacharacters is a plain literal, so its tokens can seed a `hasToken`
/// prefilter exactly like a `|=` phrase. Anything else skips the prefilter
/// (never wrong, just less pruning) rather than attempting regex analysis
/// (out of scope — see the AST's own "regex not validated" contract).
fn is_plain_literal(pattern: &str) -> bool {
    !pattern.chars().any(|c| REGEX_METACHARS.contains(&c))
}

/// Compiles one `LineFilter`. Positive ops (`|=`, `|~`) render `hasToken`
/// prefilter(s) ANDed with the exact predicate. Negative ops (`!=`, `!~`)
/// wrap the *same* compound predicate in `NOT (...)` rather than negating
/// only the exact predicate: `hasToken` never has false negatives (a bloom
/// filter can only ever say "maybe present" or "definitely absent"), so
/// `hasToken(...) AND exact(...)` is exactly equivalent to `exact(...)`
/// alone — `NOT (hasToken(...) AND exact(...))` is therefore provably
/// equivalent to `NOT exact(...)`, the correct exclusion semantic, while
/// still surfacing the prefilter for ClickHouse's optimizer to exploit
/// where it can (architect plan: "Prefilter is always paired with the
/// exact predicate").
///
/// An `or` group (M8-LQ2 `linefilter.or`) is a disjunction of the same
/// per-alternative compound predicate: `((a) OR (b) …)` for positive ops,
/// `NOT ((a) OR (b) …)` for negative ops (each disjunct's `hasToken`
/// prefilter is preserved, so the `tokenbf_v1` skip index still prunes). A
/// single-value filter is left un-wrapped so its pushed-down SQL is
/// byte-identical to the pre-`or` output. Callers must gate on
/// [`is_pushable_line_filter`]: this only ever sees literal/regex
/// alternatives (`ip(…)` is served client-side).
pub(crate) fn compile_line_filter(lf: &LineFilter) -> Result<String, PipelineError> {
    let mut disjuncts: Vec<String> = Vec::new();
    for (value, _) in lf.alternatives() {
        disjuncts.push(match lf.op {
            LineFilterOp::Contains | LineFilterOp::NotContains => contains_predicate(value),
            LineFilterOp::Regex | LineFilterOp::NotRegex => regex_predicate(value)?,
        });
    }
    let core = if lf.or_matches.is_empty() {
        disjuncts
            .into_iter()
            .next()
            .expect("a line filter always has a head alternative")
    } else {
        disjuncts
            .iter()
            .map(|p| format!("({p})"))
            .collect::<Vec<_>>()
            .join(" OR ")
    };
    Ok(match lf.op {
        LineFilterOp::Contains | LineFilterOp::Regex => {
            if lf.or_matches.is_empty() {
                core
            } else {
                format!("({core})")
            }
        }
        LineFilterOp::NotContains | LineFilterOp::NotRegex => format!("NOT ({core})"),
    })
}

fn contains_predicate(phrase: &str) -> String {
    let mut parts: Vec<String> = tokenize(phrase)
        .iter()
        .map(|t| format!("hasToken(body, {})", ch_string(t)))
        .collect();
    parts.push(format!("position(body, {}) > 0", ch_string(phrase)));
    parts.join(" AND ")
}

fn regex_predicate(pattern: &str) -> Result<String, PipelineError> {
    let mut parts: Vec<String> = Vec::new();
    if is_plain_literal(pattern) {
        parts.extend(
            tokenize(pattern)
                .iter()
                .map(|t| format!("hasToken(body, {})", ch_string(t))),
        );
    }
    parts.push(format!(
        "match(body, {})",
        ch_regex_unanchored_checked(pattern)?
    ));
    Ok(parts.join(" AND "))
}

/// Days since the Unix epoch, per nanosecond. Local to this module rather
/// than a `pulsus-model` dependency (out of scope per the architect plan's
/// Cargo.toml deps list) — the civil-calendar conversion below is the same
/// public-domain algorithm `pulsus-model::time::Date` uses.
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// Howard Hinnant's public-domain civil-calendar algorithm
/// (<http://howardhinnant.github.io/date_algorithms.html#civil_from_days>),
/// correct for the full `i64` day range.
fn civil_from_days(z: i64) -> (i64, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m)
}

/// The `(year, month)` UTC calendar month a nanosecond instant falls in.
/// `pub(crate)` so the live-tail month-boundary refresh (issue #94 item 2,
/// [`super::exec::LogQlEngine::tail_refresh_months`]) can detect when a
/// poll's watermark crosses into a month the current plan doesn't cover.
/// Lexicographic `(i64, u32)` ordering is exactly year-then-month order.
pub(crate) fn year_month(ts_ns: i64) -> (i64, u32) {
    civil_from_days(ts_ns.div_euclid(NANOS_PER_DAY))
}

/// Every calendar month (UTC) overlapping `[start_ns, end_ns]`, ascending,
/// rendered as quoted ClickHouse `Date` literals (`'YYYY-MM-01'`).
/// `log_streams`/`log_streams_idx` partition monthly (docs/schemas.md
/// §3.1); a range spanning a month boundary must resolve every partition it
/// touches or streams silently vanish (architect plan edge case:
/// "Multi-month ranges"). `pub` (issue #170): the `detected_labels`
/// aggregation builder takes pre-rendered months, and the EXPLAIN gate
/// (`tests/explain_indexes.rs`) renders them the same way the engine does.
pub fn months_overlapping(start_ns: i64, end_ns: i64) -> Vec<String> {
    let (mut y, mut m) = year_month(start_ns);
    let (end_y, end_m) = year_month(end_ns.max(start_ns));
    let mut out = Vec::new();
    loop {
        out.push(format!("'{y:04}-{m:02}-01'"));
        if (y, m) == (end_y, end_m) {
            break;
        }
        if m == 12 {
            y += 1;
            m = 1;
        } else {
            m += 1;
        }
    }
    out
}

// NOTE: the file is a `plan_`-prefixed sibling, not `plan/drop_order.rs`.
// A `plan/` directory is swallowed by a common global gitignore rule, so
// the source would never be committed.
#[cfg(test)]
#[path = "plan_drop_order.rs"]
mod drop_order;

// Issue #293: the paired pinned-stack gate, including the BODY of the
// recursive `build_metric_node` this issue deleted, with two substituted
// child accessors — not the historical function itself. It has
// to be compiled in this module because it calls six `plan.rs`-private
// items; see that file's header.
#[cfg(test)]
#[path = "plan_recursive_control.rs"]
mod recursive_control;

#[cfg(test)]
mod tests {
    use pulsus_logql::{parse, parse_selector};

    use super::*;

    fn selector(src: &str) -> StreamSelector {
        parse_selector(src).expect("parse selector")
    }

    fn test_ctx() -> PlanCtx<'static> {
        PlanCtx {
            db: "pulsus",
            streams_idx: "log_streams_idx",
            streams: "log_streams",
            samples: "log_samples",
            rollup_table: "log_metrics_5s",
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        }
    }

    fn metric_mp(query: &str, spec: QuerySpec) -> Result<MetricPlan, ReadError> {
        let params = QueryParams {
            spec,
            limit: 100,
            direction: Direction::Backward,
        };
        let expr = parse(query).expect("parse");
        match plan(&expr, &params, &test_ctx())? {
            Plan::Metric(mp) => Ok(mp),
            Plan::Streams(_) | Plan::MetricBinary(_) => panic!("expected a Metric plan"),
        }
    }

    // --- Issue #221: approx_topk is instant-only, gated at the SOLE
    // --- VectorAggSpec producer (`parse_vector_agg_params`), so BOTH
    // --- plan routes (`metric_plan` for Range-bottomed chains,
    // --- `build_metric_node` for binary/literal trees) reject it.
    // --- (`range_spec()` is the shared helper defined further down.)

    fn plan_at(query: &str, spec: QuerySpec) -> Result<Plan, ReadError> {
        let params = QueryParams {
            spec,
            limit: 100,
            direction: Direction::Backward,
        };
        plan(&parse(query).expect("parse"), &params, &test_ctx())
    }

    // --- Issue #288: repeated grouping labels are deduplicated at the
    // --- SOLE `VectorAggSpec` producer (`parse_vector_agg_params`), so
    // --- both plan routes and the variants fan-out see unique names,
    // --- while the AST keeps the duplicates the reference's parser also
    // --- keeps (pinned in pulsus-logql's snapshots).

    /// Extracts the grouping label lists of a planned metric query's
    /// vector-aggregation specs, whichever plan shape it took.
    fn planned_grouping_labels(query: &str, spec: QuerySpec) -> Vec<Vec<String>> {
        let aggs: Vec<VectorAggSpec> = match plan_at(query, spec).expect("plan") {
            Plan::Metric(mp) => mp.vector_aggs,
            Plan::MetricBinary(ref node) => match node {
                MetricNode::VectorAgg { aggs, .. } => aggs.clone(),
                other => panic!("expected a VectorAgg node, got {other:?}"),
            },
            Plan::Streams(_) => panic!("expected a metric plan"),
        };
        aggs.into_iter()
            .map(|(_, grouping, _)| grouping.map(|g| g.labels).unwrap_or_default())
            .collect()
    }

    /// `by (fp, fp)`, `by (fp, fp, fp)` and the postfix form all plan to
    /// the single deduped name (reference-probed: identical results to
    /// `by (fp)` on grafana/loki:3.7.4); a repeat combined with a
    /// distinct name keeps first-occurrence order; `without` repeats
    /// collapse the same way. Case-sensitive: `FP` and `fp` are two
    /// distinct names, both kept.
    #[test]
    fn repeated_grouping_labels_are_deduplicated_in_the_planned_spec() {
        let instant = QuerySpec::Instant {
            at_ns: 1_000_000_000_000,
        };
        for (query, want) in [
            (
                r#"sum by (fp, fp) (count_over_time({service_name="x"}[5m]))"#,
                vec!["fp"],
            ),
            (
                r#"sum by (fp, fp, fp) (count_over_time({service_name="x"}[5m]))"#,
                vec!["fp"],
            ),
            (
                r#"sum (count_over_time({service_name="x"}[5m])) by (fp, fp)"#,
                vec!["fp"],
            ),
            (
                r#"sum by (fp, env, fp) (count_over_time({service_name="x"}[5m]))"#,
                vec!["fp", "env"],
            ),
            (
                r#"sum without (env, fp, env) (count_over_time({service_name="x"}[5m]))"#,
                vec!["env", "fp"],
            ),
            (
                r#"topk by (fp, fp) (2, count_over_time({service_name="x"}[5m]))"#,
                vec!["fp"],
            ),
            (
                r#"sum by (FP, fp) (count_over_time({service_name="x"}[5m]))"#,
                vec!["FP", "fp"],
            ),
        ] {
            assert_eq!(
                planned_grouping_labels(query, instant),
                vec![want.iter().map(|s| s.to_string()).collect::<Vec<_>>()],
                "{query}"
            );
        }
    }

    /// The hash-set tier (fix round 1, U5): lists past the linear-scan
    /// threshold dedupe identically — first occurrence kept, order
    /// preserved — and a duplicate-free big list passes through whole.
    #[test]
    fn big_grouping_lists_dedupe_through_the_hash_set_tier() {
        let dup: Vec<String> = (0..40).map(|i| format!("l{}", i % 10)).collect();
        let g = dedup_grouping(Grouping {
            kind: GroupingKind::By,
            labels: dup,
        });
        let want: Vec<String> = (0..10).map(|i| format!("l{i}")).collect();
        assert_eq!(g.labels, want, "first occurrence, order preserved");

        let distinct: Vec<String> = (0..40).map(|i| format!("l{i}")).collect();
        let g = dedup_grouping(Grouping {
            kind: GroupingKind::By,
            labels: distinct.clone(),
        });
        assert_eq!(g.labels, distinct, "a duplicate-free big list is untouched");
    }

    /// Fix round 1, U5 — the reproduction probe for the reviewer's
    /// measurement. PRINT-ONLY (wall-time asserts never gate CI): run
    /// with `cargo test --release -p pulsus-read --lib -- --ignored
    /// zz_print_dedup_grouping_timings --nocapture`. Builds the widest
    /// grouping lists the 131,072-byte query-text cap admits (distinct
    /// `l0..lN` names, and the all-duplicate `fp,fp,…` form), times the
    /// parse of the maximal query as the baseline, then times
    /// `dedup_grouping` on each shape.
    #[test]
    #[ignore = "generator: prints release-mode dedup timings for the U5 record"]
    fn zz_print_dedup_grouping_timings() {
        use std::time::Instant;

        // The widest distinct-name grouping the cap admits.
        let mut query = String::from("sum by (");
        let suffix = r#") (count_over_time({a="b"}[5m]))"#;
        let mut n_distinct = 0usize;
        loop {
            let name = format!("l{n_distinct}");
            if query.len() + name.len() + 1 + suffix.len() + 1 >= pulsus_logql::MAX_QUERY_BYTES {
                break;
            }
            if n_distinct > 0 {
                query.push(',');
            }
            query.push_str(&name);
            n_distinct += 1;
        }
        query.push_str(suffix);
        let t = Instant::now();
        let expr = parse(&query).expect("maximal query parses");
        let parse_ms = t.elapsed().as_secs_f64() * 1e3;
        let Expr::Metric(MetricExpr::Vector { ref grouping, .. }) = expr else {
            panic!("expected a vector aggregation");
        };
        let g = grouping.clone().expect("grouping");
        println!("distinct names: {n_distinct}; parse: {parse_ms:.1} ms");
        let t = Instant::now();
        let out = dedup_grouping(g);
        println!(
            "dedup (all-distinct): {:.3} ms -> {} names",
            t.elapsed().as_secs_f64() * 1e3,
            out.labels.len()
        );

        // The widest all-duplicate list the cap admits (`fp,` = 3 bytes).
        let n_dup = (pulsus_logql::MAX_QUERY_BYTES - 1 - 8 - suffix.len()) / 3;
        let g = Grouping {
            kind: GroupingKind::By,
            labels: vec!["fp".to_string(); n_dup],
        };
        let t = Instant::now();
        let out = dedup_grouping(g);
        println!(
            "dedup (all-duplicate, {n_dup} names): {:.3} ms -> {} names",
            t.elapsed().as_secs_f64() * 1e3,
            out.labels.len()
        );
    }

    /// The `MetricBinary` route dedupes through the same funnel: a
    /// vector-agg layer over a binary tree carries the deduped spec.
    #[test]
    fn repeated_grouping_labels_are_deduplicated_on_the_binary_route() {
        let got = planned_grouping_labels(
            r#"sum by (fp, fp) (count_over_time({service_name="x"}[5m]) + count_over_time({service_name="y"}[5m]))"#,
            QuerySpec::Instant {
                at_ns: 1_000_000_000_000,
            },
        );
        assert_eq!(got, vec![vec!["fp".to_string()]]);
    }

    /// The variants `__variant__` injection is guarded (issue #288): a
    /// variant that already groups `by (__variant__)` gains NO duplicate
    /// entry, and a deduped user grouping keeps exactly one of each name
    /// plus the injected index label.
    #[test]
    fn variants_injection_does_not_duplicate_the_variant_label() {
        let instant = QuerySpec::Instant {
            at_ns: 60_000_000_000,
        };
        for (query, want) in [
            (
                r#"variants(sum by (__variant__) (count_over_time({a="b"}[5m]))) of ({a="b"}[5m])"#,
                vec!["__variant__"],
            ),
            (
                r#"variants(sum by (fp, fp) (count_over_time({a="b"}[5m]))) of ({a="b"}[5m])"#,
                vec!["__variant__", "fp"],
            ),
        ] {
            let plan = plan_at(query, instant).expect("plan");
            let Plan::MetricBinary(MetricNode::Variants { ref variants, .. }) = plan else {
                panic!("expected a variants plan for {query}");
            };
            let got: Vec<Vec<String>> = variants[0]
                .vector_aggs()
                .iter()
                .map(|(_, g, _)| g.as_ref().map(|g| g.labels.clone()).unwrap_or_default())
                .collect();
            assert_eq!(
                got,
                vec![want.iter().map(|s| s.to_string()).collect::<Vec<_>>()],
                "{query}"
            );
        }
    }

    // --- issue #240 AC7(b): the §3 invariant on both LogQL paths — an
    // uncompilable pushed-down regex is a 400-class plan-time rejection
    // (`bad regex: …`), never SQL handed to ClickHouse.

    fn expect_bad_regex(query: &str, spec: QuerySpec) {
        match plan_at(query, spec) {
            Err(ReadError::PipelineInvalid { reason }) => {
                assert!(reason.starts_with("bad regex: "), "{query}: {reason}");
            }
            other => panic!("{query}: expected a bad-regex rejection, got {other:?}"),
        }
    }

    #[test]
    fn an_uncompilable_pushed_down_line_filter_regex_rejects_at_plan_time() {
        let instant = QuerySpec::Instant {
            at_ns: 1_000_000_000_000,
        };
        expect_bad_regex(r#"{service_name="x"} |~ "(""#, instant);
        expect_bad_regex(r#"{service_name="x"} !~ "(""#, instant);
        expect_bad_regex(r#"{service_name="x"} |~ "ok" or "(""#, instant);
    }

    /// The metric caller (`compile_line_filters` at its second call
    /// site) rejects identically.
    #[test]
    fn an_uncompilable_line_filter_regex_in_a_metric_query_rejects_at_plan_time() {
        expect_bad_regex(
            r#"count_over_time({service_name="x"} |~ "(" [5m])"#,
            QuerySpec::Instant {
                at_ns: 1_000_000_000_000,
            },
        );
    }

    #[test]
    fn an_uncompilable_stream_matcher_regex_rejects_at_plan_time() {
        let instant = QuerySpec::Instant {
            at_ns: 1_000_000_000_000,
        };
        expect_bad_regex(r#"{service_name="x", app=~"("}"#, instant);
        expect_bad_regex(r#"{service_name="x", app!~"("}"#, instant);
    }

    /// Deterministic rejection order: `normalize_matchers` runs before
    /// `compile_line_filters` at both call sites, so a query carrying
    /// BOTH a bad matcher regex and a bad line-filter regex reports the
    /// matcher one. The two patterns produce distinct regex errors, so
    /// the assertion discriminates.
    #[test]
    fn a_bad_matcher_regex_wins_over_a_bad_line_filter_regex() {
        match plan_at(
            r#"{service_name="x", app=~"("} |~ "[""#,
            QuerySpec::Instant {
                at_ns: 1_000_000_000_000,
            },
        ) {
            Err(ReadError::PipelineInvalid { reason }) => {
                assert!(reason.starts_with("bad regex: "), "{reason}");
                assert!(
                    reason.contains("missing closing )") || reason.contains('('),
                    "the MATCHER pattern's error, not the line filter's: {reason}"
                );
                assert!(
                    !reason.contains("[b"),
                    "must not be the line-filter pattern's error: {reason}"
                );
            }
            other => panic!("expected the matcher rejection, got {other:?}"),
        }
    }

    // --- issue #276: `label_replace(...)` planning ---------------------

    /// The #240 asymmetry, pinned as DELIBERATE: an uncompilable
    /// `label_replace` regex reports the WRAPPED `^(?:…)$` form — the
    /// reference compiles exactly that string and surfaces its compiler's
    /// error verbatim (live-probed, v3.7.4: `invalid regex in
    /// label_replace: error parsing regexp: missing closing ): `^(?:()$``)
    /// — while every other LogQL site reports the USER's raw pattern via
    /// `pipeline::bad_regex`. A "consistency fix" toward the raw pattern
    /// here would be a NEW divergence. Wording after the prefix is
    /// rust-regex's (the ledgered error-wording class); the STRUCTURE —
    /// prefix + wrapped pattern — is the pinned reference fact.
    #[test]
    fn label_replace_bad_regex_reports_the_wrapped_form_not_the_users_pattern() {
        match plan_at(
            r#"label_replace(count_over_time({service_name="x"}[5m]), "d", "r", "s", "(")"#,
            QuerySpec::Instant {
                at_ns: 1_000_000_000_000,
            },
        ) {
            Err(ReadError::PipelineInvalid { reason }) => {
                assert!(
                    reason.starts_with("invalid regex in label_replace: "),
                    "{reason}"
                );
                assert!(
                    reason.contains("^(?:()$"),
                    "must report the WRAPPED `^(?:…)$` form (issue #240 asymmetry): {reason}"
                );
                assert!(
                    !reason.starts_with("bad regex: "),
                    "must not route through the raw-pattern `bad_regex` seam: {reason}"
                );
            }
            other => panic!("expected the wrapped-regex rejection, got {other:?}"),
        }
    }

    /// A scalar-typed operand is a plan-time 400 (the reference 500s
    /// `unexpected expr type (*syntax.LiteralExpr) …` — ledgered
    /// `label-replace-scalar-operand-status`). Both the bare literal and
    /// a folded literal-only arithmetic tree reject; a series-producing
    /// operand anywhere in the tree (`vector(1) + 1`) plans.
    #[test]
    fn label_replace_over_a_scalar_typed_operand_is_rejected_at_plan_time() {
        let instant = QuerySpec::Instant {
            at_ns: 1_000_000_000_000,
        };
        for q in [
            r#"label_replace(2, "d", "r", "s", ".*")"#,
            r#"label_replace(1 + 1, "d", "r", "s", ".*")"#,
        ] {
            match plan_at(q, instant) {
                Err(ReadError::PipelineInvalid { reason }) => assert_eq!(
                    reason, "label_replace requires a vector operand, got a scalar expression",
                    "{q}"
                ),
                other => panic!("{q}: expected the scalar-operand rejection, got {other:?}"),
            }
        }
        assert!(
            plan_at(
                r#"label_replace(vector(1) + 1, "d", "r", "s", ".*")"#,
                instant
            )
            .is_ok(),
            "a series-producing operand must plan"
        );
    }

    /// Emission errors precede the fold's scalar-operand rejection — the
    /// reference's ordering (its parse-time regex error surfaces before
    /// the evaluator factory sees the literal operand).
    #[test]
    fn label_replace_regex_error_wins_over_the_scalar_operand_error() {
        match plan_at(
            r#"label_replace(2, "d", "r", "s", "(")"#,
            QuerySpec::Instant {
                at_ns: 1_000_000_000_000,
            },
        ) {
            Err(ReadError::PipelineInvalid { reason }) => {
                assert!(
                    reason.starts_with("invalid regex in label_replace: "),
                    "the regex error must win: {reason}"
                );
            }
            other => panic!("expected the regex rejection, got {other:?}"),
        }
    }

    /// Plan shape: `label_replace` over a range aggregation routes to
    /// `Plan::MetricBinary` with a `LabelReplace` node over an ordinary
    /// leaf, and composes under a vector aggregation.
    #[test]
    fn label_replace_plans_to_a_label_replace_node_over_the_ordinary_leaf() {
        let instant = QuerySpec::Instant {
            at_ns: 1_000_000_000_000,
        };
        match plan_at(
            r#"label_replace(count_over_time({service_name="x"}[5m]), "d", "r-$1", "s", "(.*)")"#,
            instant,
        ) {
            Ok(Plan::MetricBinary(MetricNode::LabelReplace { ref spec, .. })) => {
                assert_eq!(spec.dst, "d");
                assert_eq!(spec.replacement, "r-$1");
                assert_eq!(spec.src, "s");
                assert_eq!(spec.regex, "(.*)");
            }
            other => panic!("expected a LabelReplace plan, got {other:?}"),
        }
        match plan_at(
            r#"sum(label_replace(count_over_time({service_name="x"}[5m]), "d", "r", "s", ".*"))"#,
            instant,
        ) {
            Ok(Plan::MetricBinary(MetricNode::VectorAgg { .. })) => {}
            other => panic!("expected a VectorAgg over LabelReplace, got {other:?}"),
        }
    }

    /// The shape that bypassed the v1 design's gate: a vector chain
    /// bottoming at `MetricExpr::Range` goes through `metric_plan`, not
    /// `build_metric_node`.
    #[test]
    fn range_approx_topk_over_a_bare_range_agg_is_rejected() {
        match plan_at(r#"approx_topk(2, rate({app="x"}[5m]))"#, range_spec()) {
            Err(ReadError::PipelineInvalid { reason }) => assert_eq!(
                reason, "count min sketches are only supported on instant queries",
                "the reference's body, verbatim"
            ),
            other => panic!("expected the range approx_topk to be rejected, got {other:?}"),
        }
    }

    /// The `build_metric_node` route (a binary tree under the agg).
    #[test]
    fn range_approx_topk_over_a_binary_tree_is_rejected() {
        match plan_at(
            r#"approx_topk(2, rate({app="x"}[5m]) + rate({app="y"}[5m]))"#,
            range_spec(),
        ) {
            Err(ReadError::PipelineInvalid { reason }) => {
                assert_eq!(
                    reason,
                    "count min sketches are only supported on instant queries"
                );
            }
            other => panic!("expected the range approx_topk to be rejected, got {other:?}"),
        }
    }

    /// Guards over-rejection: the same queries at `Instant` plan fine.
    #[test]
    fn instant_approx_topk_over_a_bare_range_agg_plans() {
        let p = plan_at(
            r#"approx_topk(2, rate({app="x"}[5m]))"#,
            QuerySpec::Instant {
                at_ns: 1_000_000_000_000,
            },
        )
        .expect("instant approx_topk must plan");
        assert!(matches!(p, Plan::Metric(_)));
        let p = plan_at(
            r#"approx_topk(2, rate({app="x"}[5m]) + rate({app="y"}[5m]))"#,
            QuerySpec::Instant {
                at_ns: 1_000_000_000_000,
            },
        )
        .expect("instant approx_topk over a binary tree must plan");
        assert!(matches!(p, Plan::MetricBinary(_)));
    }

    /// Issue #221 AC 20: every `VectorAggSpec` carrying `ApproxTopk` has
    /// `grouping == None` — structurally guaranteed by the parse-time
    /// grouping rejection, so `select_k_instant`'s group map always
    /// holds a single empty key on this path (row 8 of the exec
    /// accounting table holds structurally, not by convention).
    #[test]
    fn approx_topk_specs_never_carry_a_grouping() {
        let mp = metric_mp(
            r#"approx_topk(3, sum by (lvl) (count_over_time({app="x"}[1m])))"#,
            QuerySpec::Instant {
                at_ns: 1_000_000_000_000,
            },
        )
        .expect("plan");
        let approx: Vec<_> = mp
            .vector_aggs
            .iter()
            .filter(|(op, ..)| matches!(op, VectorAggOp::ApproxTopk))
            .collect();
        assert_eq!(approx.len(), 1);
        for (_, grouping, param) in approx {
            assert!(grouping.is_none(), "grouping is rejected at parse time");
            assert_eq!(*param, Some(3.0));
        }
        // The binary-tree route carries the same invariant.
        let p = plan_at(
            r#"approx_topk(2, rate({app="x"}[5m]) + rate({app="y"}[5m]))"#,
            QuerySpec::Instant {
                at_ns: 1_000_000_000_000,
            },
        )
        .expect("plan");
        // Issue #272: E0509 — re-bind through a reference.
        let Plan::MetricBinary(MetricNode::VectorAgg { aggs, .. }) = &p else {
            panic!("expected a VectorAgg node");
        };
        assert!(
            aggs.iter()
                .all(|(op, grouping, _)| !matches!(op, VectorAggOp::ApproxTopk)
                    || grouping.is_none())
        );
    }

    /// Test-gap flagged by the architect-plan review: a zero-step `Range`
    /// query must be rejected before it ever reaches the routing decision
    /// — `0.is_multiple_of(_)` is trivially `true`, so without this guard
    /// it would silently route to rollup and render `intDiv(_, 0)`.
    #[test]
    fn a_zero_step_range_query_is_rejected_as_an_invalid_step() {
        let err = metric_mp(
            r#"rate({env="prod"}[5m])"#,
            QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000,
                step_ns: 0,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ReadError::InvalidStep));
    }

    /// Issue #227 (review round 2): a HOSTILE client `step` — one above the
    /// validated duration domain, including values that would narrow to a
    /// NEGATIVE `i64` — is rejected END-TO-END by the real planner with the
    /// named 400, never narrowed into the window/covering arithmetic.
    #[test]
    fn a_hostile_step_is_rejected_end_to_end_by_the_planner() {
        for hostile in [
            super::super::params::MAX_DURATION_NS as u64 + 1,
            i64::MAX as u64 + 1, // would narrow to i64::MIN
            u64::MAX,
        ] {
            let err = metric_mp(
                r#"rate({env="prod"}[5m])"#,
                QuerySpec::Range {
                    start_ns: 0,
                    end_ns: 1_000_000_000,
                    step_ns: hostile,
                },
            )
            .unwrap_err();
            assert!(
                matches!(
                    err,
                    ReadError::DurationOutOfRange { what: "step", value, .. } if value == hostile
                ),
                "hostile step {hostile} must be a named 400, got {err:?}"
            );
        }
    }

    /// The same boundary guards the LEAFLESS tree (`vector(1)`, `2 + 2`),
    /// which has no metric leaf to run `metric_plan`'s validation — a hostile
    /// step there must not reach `materialize_vector_lit`'s grid math.
    #[test]
    fn a_hostile_step_is_rejected_for_a_leafless_metric_tree() {
        let expr = parse("vector(1)").expect("parse");
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000,
                step_ns: u64::MAX,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        match plan(&expr, &params, &test_ctx()) {
            Err(ReadError::DurationOutOfRange { what: "step", .. }) => {}
            other => panic!("expected a named duration rejection, got {other:?}"),
        }
    }

    /// Issue #227 review round 11: when the `start - range` scan widening
    /// UNDERFLOWS i64, the plan carries `start_ns == i64::MIN` with an
    /// INCLUSIVE lower bound — the logical bound sits below the
    /// representable domain, so a sample stored at exactly `i64::MIN`
    /// must survive the SQL predicate (the reference includes it). Both
    /// the range path and its instant analogue.
    #[test]
    fn scan_widening_underflow_makes_the_lower_bound_inclusive() {
        let mp = metric_mp(
            r#"count_over_time({env="prod"}[1ns])"#,
            QuerySpec::Range {
                start_ns: i64::MIN,
                end_ns: i64::MIN,
                step_ns: 1,
            },
        )
        .unwrap();
        assert_eq!(mp.start_ns, i64::MIN);
        assert_eq!(mp.scan_lower, ScanLowerBound::Inclusive);
        assert_eq!(mp.grid_start_ns, i64::MIN, "the emit grid is unwidened");

        let mp = metric_mp(
            r#"count_over_time({env="prod"}[1ns])"#,
            QuerySpec::Instant { at_ns: i64::MIN },
        )
        .unwrap();
        assert_eq!(mp.start_ns, i64::MIN);
        assert_eq!(mp.scan_lower, ScanLowerBound::Inclusive);
    }

    /// The negative control (the crux of the round-11 distinction): a
    /// LEGITIMATELY-computed `i64::MIN` lower bound — `start = i64::MIN +
    /// 1` minus `[1ns]`, no underflow — keeps EXCLUSIVE semantics, so a
    /// sample at exactly `i64::MIN` stays outside the window, as in the
    /// reference.
    #[test]
    fn a_legitimately_computed_i64_min_scan_bound_stays_exclusive() {
        let mp = metric_mp(
            r#"count_over_time({env="prod"}[1ns])"#,
            QuerySpec::Range {
                start_ns: i64::MIN + 1,
                end_ns: i64::MIN + 1,
                step_ns: 1,
            },
        )
        .unwrap();
        assert_eq!(mp.start_ns, i64::MIN, "exactly representable: (MIN+1) - 1");
        assert_eq!(mp.scan_lower, ScanLowerBound::Exclusive);

        let mp = metric_mp(
            r#"count_over_time({env="prod"}[1ns])"#,
            QuerySpec::Instant {
                at_ns: i64::MIN + 1,
            },
        )
        .unwrap();
        assert_eq!(mp.start_ns, i64::MIN);
        assert_eq!(mp.scan_lower, ScanLowerBound::Exclusive);
    }

    // --- Issue #343: the `offset` window shift ------------------------
    //
    // These own the INSTANT half of the shift, which the hermetic corpus
    // cannot see: an instant window is a SQL predicate, so `b19_offset.
    // test` — which drives the pure evaluator over every loaded row —
    // reaches only the range/sliding half. What a user experiences on an
    // instant query IS `start_ns`/`end_ns`, the bounds asserted here.

    const HOUR_NS: i64 = 3_600_000_000_000;
    const FIVE_MIN_NS: i64 = 300_000_000_000;

    /// A positive offset moves the whole instant window BACK by `d`, and
    /// the emitted-grid shift is recorded so a matrix can be put back.
    #[test]
    fn offset_shifts_the_instant_scan_window_back() {
        let at = 100 * HOUR_NS;
        let mp = metric_mp(
            r#"count_over_time({env="prod"}[5m] offset 1h)"#,
            QuerySpec::Instant { at_ns: at },
        )
        .unwrap();
        assert_eq!(mp.end_ns, at - HOUR_NS, "the window's upper bound is T - d");
        assert_eq!(
            mp.start_ns,
            at - HOUR_NS - FIVE_MIN_NS,
            "and its lower bound is T - d - range"
        );
        assert_eq!(mp.grid_start_ns, mp.start_ns);
        assert_eq!(mp.scan_lower, ScanLowerBound::Exclusive);
        assert_eq!(mp.offset_ns, HOUR_NS);
    }

    /// `offset 0s` is the IDENTITY — byte-identical to the same query
    /// without an offset. It is a distinct SPELLING (bare `offset 0` is a
    /// reference 400), never a distinct plan.
    #[test]
    fn a_zero_offset_plans_identically_to_no_offset() {
        let spec = QuerySpec::Instant {
            at_ns: 100 * HOUR_NS,
        };
        let with = metric_mp(r#"count_over_time({env="prod"}[5m] offset 0s)"#, spec).unwrap();
        let without = metric_mp(r#"count_over_time({env="prod"}[5m])"#, spec).unwrap();
        assert_eq!(with, without);
        assert_eq!(with.offset_ns, 0);
    }

    /// **The sign, which is the whole trap.** A NEGATIVE offset is a
    /// reference 200 and shifts the window FORWARD. Written as an
    /// absolute-value subtraction the planner reads correctly and
    /// silently evaluates `(T-d-range, T-d]` instead — so this asserts
    /// the bounds are ABOVE `at`, and asserts they differ from the
    /// positive-offset plan, which an `|d|` implementation could not do.
    #[test]
    fn a_negative_offset_shifts_the_window_forward_not_back() {
        let at = 100 * HOUR_NS;
        let ahead = metric_mp(
            r#"count_over_time({env="prod"}[5m] offset -1h)"#,
            QuerySpec::Instant { at_ns: at },
        )
        .unwrap();
        assert_eq!(ahead.end_ns, at + HOUR_NS, "T - (-d) is T + d");
        assert_eq!(ahead.start_ns, at + HOUR_NS - FIVE_MIN_NS);
        assert_eq!(ahead.offset_ns, -HOUR_NS);

        let behind = metric_mp(
            r#"count_over_time({env="prod"}[5m] offset 1h)"#,
            QuerySpec::Instant { at_ns: at },
        )
        .unwrap();
        assert_ne!(
            ahead.end_ns, behind.end_ns,
            "an absolute-value shift would make these equal"
        );
        assert_eq!(
            ahead.end_ns - behind.end_ns,
            2 * HOUR_NS,
            "the two windows sit a full 2d apart, one either side of T"
        );
    }

    /// The range shape: the SCAN and the EMIT GRID both move, and
    /// `offset_ns` carries the shift the evaluator adds back so the
    /// matrix comes out on the caller's grid.
    #[test]
    fn offset_shifts_the_range_scan_and_grid_together() {
        let start = 100 * HOUR_NS;
        let end = start + HOUR_NS;
        let spec = QuerySpec::Range {
            start_ns: start,
            end_ns: end,
            step_ns: FIVE_MIN_NS as u64,
        };
        let mp = metric_mp(r#"count_over_time({env="prod"}[5m] offset 1h)"#, spec).unwrap();
        assert_eq!(mp.grid_start_ns, start - HOUR_NS);
        assert_eq!(mp.end_ns, end - HOUR_NS);
        assert_eq!(
            mp.start_ns,
            start - HOUR_NS - FIVE_MIN_NS,
            "the scan still reaches a full [range] below the grid"
        );
        assert_eq!(mp.offset_ns, HOUR_NS);

        // The SPAN is preserved: an offset translates the domain, it does
        // not widen it, so the byte budget of an offset query matches the
        // same query without one.
        let plain = metric_mp(r#"count_over_time({env="prod"}[5m])"#, spec).unwrap();
        assert_eq!(mp.end_ns - mp.start_ns, plain.end_ns - plain.start_ns);
    }

    /// The LARGEST offset that exists — `offset 43800h`, the 5-year cap —
    /// shifts both bounds by its exact amount and answers empty against
    /// 1970 data. No error, no wrap, no clamp.
    ///
    /// **This one is REPRESENTABLE** (`0 - 1.5768e17` is far inside
    /// `i64`), which is why it exercises no rail: the shipped
    /// `saturating_sub` and today's `checked_sub` agree here exactly. The
    /// rail behaviour is owned by
    /// [`offset_off_the_timestamp_axis_plans_the_degenerate_empty_window`]
    /// below, which needs a near-rail REQUEST rather than a large offset —
    /// the cap makes that the only way to reach it.
    #[test]
    fn the_largest_accepted_offset_shifts_exactly_rather_than_erroring() {
        let mp = metric_mp(
            r#"count_over_time({env="prod"}[5m] offset 43800h)"#,
            QuerySpec::Instant { at_ns: 0 },
        )
        .unwrap();
        assert!(!mp.empty_domain, "5 years back from 1970 is representable");
        assert_eq!(
            mp.end_ns,
            -pulsus_logql::MAX_QUERY_SPAN_NS,
            "T - d, exactly"
        );
        assert_eq!(mp.start_ns, mp.end_ns - FIVE_MIN_NS);
    }

    /// Issue #343 boundary fix: when the shift leaves the representable
    /// timestamp axis the planner substitutes the DEGENERATE empty
    /// evaluation domain and raises `empty_domain` — it does NOT clamp
    /// onto the `i64` rail (which relocated the window) and it does NOT
    /// refuse (an ordinary offset over a near-rail request is a legitimate
    /// query).
    ///
    /// **THE 5-YEAR CAPS DO NOT MAKE THIS UNREACHABLE, and that is the
    /// point of the cases chosen.** The caps bound `offset`, `[range]` and
    /// the query SPAN — none of them bounds where on the axis the request
    /// SITS. `start`/`end`/`time` are plain `i64` nanoseconds at the API
    /// (`logs_api::params::parse_ts` accepts the whole domain), so a
    /// request within 5 years of a rail plus an ORDINARY offset still
    /// leaves it. Both cases below use a 1-hour offset and a 60-second
    /// span; only the request's position is extreme.
    #[test]
    fn offset_off_the_timestamp_axis_plans_the_degenerate_empty_window() {
        // Instant, high rail: `at` one hour below MAX, shifted forward.
        let instant = metric_mp(
            r#"count_over_time({env="prod"}[5m] offset -1h)"#,
            QuerySpec::Instant {
                at_ns: i64::MAX - HOUR_NS + 1,
            },
        )
        .unwrap();
        assert!(instant.empty_domain);
        assert_eq!(
            (instant.start_ns, instant.grid_start_ns, instant.end_ns),
            (0, 0, -1),
            "grid_start = 0 and end = -1: `end < grid_start`, so no grid \
             point exists and no timestamp is admitted"
        );
        assert_eq!(instant.scan_lower, ScanLowerBound::Exclusive);
        assert_eq!(instant.step_ns, None, "still an instant plan");

        // Range, low rail: `i64::MIN - 1h` underflows on the START bound
        // alone — one failing bound is enough. The span is 60s.
        let range = metric_mp(
            r#"count_over_time({env="prod"}[5m] offset 1h)"#,
            QuerySpec::Range {
                start_ns: i64::MIN,
                end_ns: i64::MIN + 60_000_000_000,
                step_ns: FIVE_MIN_NS as u64,
            },
        )
        .unwrap();
        assert!(range.empty_domain);
        assert_eq!(
            (range.start_ns, range.grid_start_ns, range.end_ns),
            (0, 0, -1)
        );
        assert_eq!(
            range.step_ns.map(|d| d.get()),
            Some(FIVE_MIN_NS),
            "the validated step survives: the shape stays a matrix"
        );

        // The control: an ordinary shift is untouched by any of this.
        let ok = metric_mp(
            r#"count_over_time({env="prod"}[5m] offset 1h)"#,
            QuerySpec::Instant {
                at_ns: 100 * HOUR_NS,
            },
        )
        .unwrap();
        assert!(!ok.empty_domain);
        assert_eq!(ok.end_ns, 100 * HOUR_NS - HOUR_NS);
    }

    /// **The 5-year rule, place 3 of 3** (issue #343, owner mandate): the
    /// query's own `start`-to-`end` span. Checked once in [`plan`], so it
    /// covers streams and metric queries alike, against the SAME
    /// `pulsus_logql::MAX_QUERY_SPAN_NS` the parser bounds `offset` and
    /// `[range]` against.
    ///
    /// This is the place the parser cannot see and the one that closes the
    /// remaining hole: a 1677 `start` with an ordinary `offset 1h` was
    /// accepted and still walked off the representable domain. It bounds
    /// the SPAN only — a near-rail request of ordinary width stays legal,
    /// which is why the degenerate-window path above is still reachable.
    #[test]
    fn a_query_span_over_five_years_is_refused() {
        const CAP: i64 = pulsus_logql::MAX_QUERY_SPAN_NS;
        let spec = |start_ns: i64, end_ns: i64| QuerySpec::Range {
            start_ns,
            end_ns,
            step_ns: FIVE_MIN_NS as u64,
        };
        // At the cap: accepted.
        metric_mp(r#"count_over_time({env="prod"}[5m])"#, spec(0, CAP)).expect("exactly 5 years");
        // One nanosecond over: refused, for a metric query...
        assert!(matches!(
            metric_mp(r#"count_over_time({env="prod"}[5m])"#, spec(0, CAP + 1)),
            Err(ReadError::QuerySpanTooLong { .. })
        ));
        // ...and for a plain streams query, which never reaches
        // `metric_plan` at all.
        assert!(matches!(
            plan_at(r#"{env="prod"}"#, spec(0, CAP + 1)),
            Err(ReadError::QuerySpanTooLong { .. })
        ));
        // The 1677 hole, closed: the span is what refuses it, not the
        // offset (which is an ordinary hour).
        assert!(matches!(
            metric_mp(
                r#"count_over_time({env="prod"}[5m] offset 1h)"#,
                spec(i64::MIN, 0)
            ),
            Err(ReadError::QuerySpanTooLong { .. })
        ));
        // An INSTANT query has no span to bound, so a near-rail `at` is
        // still planned — the degenerate-window path stays live.
        metric_mp(
            r#"count_over_time({env="prod"}[5m])"#,
            QuerySpec::Instant { at_ns: i64::MAX },
        )
        .expect("an instant query carries no span");
    }

    /// The shared scan and the variant windows of one `variants(...)`
    /// plan: `(scan.start_ns, scan.end_ns, [variant window bounds])`.
    fn variants_windows(query: &str, spec: QuerySpec) -> (i64, i64, Vec<(i64, i64)>) {
        match &plan_at(query, spec).expect("plans") {
            Plan::MetricBinary(MetricNode::Variants { scan, variants, .. }) => (
                scan.start_ns,
                scan.end_ns,
                variants
                    .iter()
                    .map(|v| match v.window() {
                        ClientWindow::Instant { start_ns, end_ns } => (start_ns, end_ns),
                        ClientWindow::Range {
                            grid_start_ns,
                            end_ns,
                            ..
                        } => (grid_start_ns, end_ns),
                    })
                    .collect(),
            ),
            other => panic!("expected a variants plan, got {other:?}"),
        }
    }

    /// **A variant's window comes from its OWN offset, over the single
    /// shared scan of the COMMON range's window** — the reference's model,
    /// measured against the pinned v3.7.4 container with a seeded store
    /// (issue #343): a variant reads its shifted window INTERSECTED with
    /// the common one.
    ///
    /// So a variant offset the common range does not share is planned, not
    /// refused: the reference answers such a query 200 in every shape
    /// probed, `3` where the common window covers the shifted data and
    /// empty where it does not. This asserts the plan that produces that —
    /// the variant window moves, the scan does not.
    #[test]
    fn a_variant_carries_its_own_offset_over_the_common_ranges_scan() {
        let at = 100 * HOUR_NS;
        let (scan_start, scan_end, windows) = variants_windows(
            r#"variants(count_over_time({env="prod"}[5m] offset 1h)) of ({env="prod"}[5m])"#,
            QuerySpec::Instant { at_ns: at },
        );
        // The scan is the COMMON range's, unshifted and NOT widened to
        // cover the variant: union-widening would answer 3 where the
        // reference answers empty, which is a divergence, not a fix.
        assert_eq!((scan_start, scan_end), (at - FIVE_MIN_NS, at));
        // The variant's window IS shifted, by its own offset.
        assert_eq!(windows, vec![(at - HOUR_NS - FIVE_MIN_NS, at - HOUR_NS)]);
        assert!(
            windows[0].1 < scan_start,
            "this pair does not overlap at all, which is what makes the \
             reference's answer for it empty rather than partial"
        );
    }

    /// The other three arrangements, each pinned by the same probe.
    #[test]
    fn every_variant_offset_arrangement_plans_with_its_own_window() {
        let at = 100 * HOUR_NS;
        let spec = QuerySpec::Instant { at_ns: at };

        // UNIFORM: scan and variant move together, and the variant's
        // window sits inside the scan — the case that answers 3.
        let (s0, s1, w) = variants_windows(
            r#"variants(count_over_time({env="prod"}[5m] offset 1h)) of ({env="prod"}[5m] offset 1h)"#,
            spec,
        );
        assert_eq!((s0, s1), (at - HOUR_NS - FIVE_MIN_NS, at - HOUR_NS));
        assert_eq!(w, vec![(at - HOUR_NS - FIVE_MIN_NS, at - HOUR_NS)]);

        // The COMMON range carries the offset and the variant does not:
        // the scan moves, the variant window does not.
        let (s0, s1, w) = variants_windows(
            r#"variants(count_over_time({env="prod"}[5m])) of ({env="prod"}[5m] offset 1h)"#,
            spec,
        );
        assert_eq!((s0, s1), (at - HOUR_NS - FIVE_MIN_NS, at - HOUR_NS));
        assert_eq!(w, vec![(at - FIVE_MIN_NS, at)]);

        // TWO variants, only the first offset: each gets its own window
        // off ONE scan. The reference returns only the second one's
        // series for exactly this shape.
        let (s0, s1, w) = variants_windows(
            r#"variants(count_over_time({env="prod"}[5m] offset 1h), count_over_time({env="prod"}[5m])) of ({env="prod"}[5m])"#,
            spec,
        );
        assert_eq!((s0, s1), (at - FIVE_MIN_NS, at));
        assert_eq!(
            w,
            vec![
                (at - HOUR_NS - FIVE_MIN_NS, at - HOUR_NS),
                (at - FIVE_MIN_NS, at),
            ]
        );

        // `offset 0s` on the variant against no offset on the common
        // range is the identity — the same window, not merely the same
        // `Option` shape.
        let (_, _, zero) = variants_windows(
            r#"variants(count_over_time({env="prod"}[5m] offset 0s)) of ({env="prod"}[5m])"#,
            spec,
        );
        let (_, _, absent) = variants_windows(
            r#"variants(count_over_time({env="prod"}[5m])) of ({env="prod"}[5m])"#,
            spec,
        );
        assert_eq!(zero, absent);
    }

    /// Issue #227: a range query NO LONGER routes to the 5s rollup on a
    /// resolution-dividing step — the rollup cannot reproduce Loki's
    /// per-event sliding-window boundary, so every range read is the
    /// streaming raw path.
    #[test]
    fn a_range_query_routes_to_the_sliding_raw_path_regardless_of_step() {
        let mp = metric_mp(
            r#"rate({env="prod"}[5m])"#,
            QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
        )
        .unwrap();
        assert_eq!(mp.routing.chosen, RouteChoice::Raw);
        assert!(!mp.rollup);
        assert!(mp.client.is_some(), "range routes to the client slide");
        assert_eq!(
            mp.routing.reason,
            "raw: sliding-window range aggregation (issue #227)"
        );
    }

    /// Issue #227: a non-dividing step is also the sliding raw path (there is
    /// no longer a rollup-vs-raw distinction for range reads).
    #[test]
    fn a_range_query_with_a_non_dividing_step_still_slides_raw() {
        let mp = metric_mp(
            r#"rate({env="prod"}[5m])"#,
            QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 3_000_000_000,
            },
        )
        .unwrap();
        assert_eq!(mp.routing.chosen, RouteChoice::Raw);
        assert!(!mp.rollup);
        assert_eq!(
            mp.routing.reason,
            "raw: sliding-window range aggregation (issue #227)"
        );
    }

    /// Issue #227: a range query WITH a line filter is the pipeline/unwrap
    /// client path (a beyond-plain aggregation), still raw and sliding.
    #[test]
    fn a_range_line_filter_query_routes_to_raw_client_aggregation() {
        let mp = metric_mp(
            r#"count_over_time({env="prod"} |= "err" [5m])"#,
            QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
        )
        .unwrap();
        assert_eq!(mp.routing.chosen, RouteChoice::Raw);
        assert!(!mp.rollup);
        assert!(mp.client.is_some());
        // A plain line filter is pushed as a predicate, not a beyond-line
        // stage, so the reason is the sliding-range reason.
        assert_eq!(
            mp.routing.reason,
            "raw: sliding-window range aggregation (issue #227)"
        );
        assert_eq!(mp.extra_predicates.len(), 1, "the line filter is pushed");
    }

    #[test]
    fn an_instant_query_routes_to_raw_with_a_named_reason() {
        let mp = metric_mp(
            r#"rate({env="prod"}[5m])"#,
            QuerySpec::Instant {
                at_ns: 1_000_000_000,
            },
        )
        .unwrap();
        assert_eq!(mp.routing.chosen, RouteChoice::Raw);
        assert!(!mp.rollup);
        assert_eq!(mp.routing.reason, "raw: instant query");
    }

    /// Issue #227: rollup resolution is irrelevant to a range read now (it
    /// always slides raw) — an unconfigured resolution changes nothing.
    #[test]
    fn an_unconfigured_rollup_resolution_still_slides_raw_for_range() {
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let mut ctx = test_ctx();
        ctx.rollup_res_ns = 0;
        let expr = parse(r#"rate({env="prod"}[5m])"#).expect("parse");
        let mp = match plan(&expr, &params, &ctx).unwrap() {
            Plan::Metric(mp) => mp,
            Plan::Streams(_) | Plan::MetricBinary(_) => panic!("expected a Metric plan"),
        };
        assert_eq!(mp.routing.chosen, RouteChoice::Raw);
        assert_eq!(
            mp.routing.reason,
            "raw: sliding-window range aggregation (issue #227)"
        );
    }

    /// Precedence lock (code review fix, issue #12): `Instant` must win
    /// over every other raw-fallback reason an instant query also happens
    /// to satisfy — a line filter here would otherwise (wrongly) report
    /// "raw: line filter present" instead of "raw: instant query".
    #[test]
    fn an_instant_query_with_a_line_filter_still_reports_the_instant_reason() {
        let mp = metric_mp(
            r#"rate({env="prod"} |= "err" [5m])"#,
            QuerySpec::Instant {
                at_ns: 1_000_000_000,
            },
        )
        .unwrap();
        assert_eq!(mp.routing.chosen, RouteChoice::Raw);
        assert_eq!(mp.routing.reason, "raw: instant query");
    }

    /// Precedence lock (code review fix, issue #12): an unconfigured
    /// rollup resolution must not shadow the "raw: instant query" reason
    /// either.
    #[test]
    fn an_instant_query_with_an_unconfigured_rollup_resolution_still_reports_the_instant_reason() {
        let params = QueryParams {
            spec: QuerySpec::Instant {
                at_ns: 1_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let mut ctx = test_ctx();
        ctx.rollup_res_ns = 0;
        let expr = parse(r#"rate({env="prod"}[5m])"#).expect("parse");
        let mp = match plan(&expr, &params, &ctx).unwrap() {
            Plan::Metric(mp) => mp,
            Plan::Streams(_) | Plan::MetricBinary(_) => panic!("expected a Metric plan"),
        };
        assert_eq!(mp.routing.chosen, RouteChoice::Raw);
        assert_eq!(mp.routing.reason, "raw: instant query");
    }

    #[test]
    fn single_positive_matcher_collapses_to_one_branch() {
        let n = normalize_matchers(&selector(r#"{service_name="checkout"}"#)).unwrap();
        assert_eq!(
            n.positive_branches,
            vec!["(key = 'service_name' AND val = 'checkout')"]
        );
        assert!(n.negative_branches.is_empty());
        assert!(n.probe_keys.is_empty());
    }

    #[test]
    fn duplicate_eq_on_the_same_key_and_value_dedups_to_one_branch() {
        let n = normalize_matchers(&selector(
            r#"{service_name="checkout", service_name="checkout"}"#,
        ))
        .unwrap();
        assert_eq!(n.positive_branches.len(), 1);
    }

    #[test]
    fn conflicting_eq_values_on_the_same_key_are_contradictory() {
        let err = normalize_matchers(&selector(
            r#"{service_name="checkout", service_name="billing"}"#,
        ))
        .unwrap_err();
        assert!(matches!(err, ReadError::ContradictoryMatchers));
    }

    #[test]
    fn eq_and_re_on_the_same_key_and_both_conditions_into_one_branch() {
        let n = normalize_matchers(&selector(r#"{env="prod", env=~"prod|staging"}"#)).unwrap();
        assert_eq!(n.positive_branches.len(), 1);
        assert_eq!(
            n.positive_branches[0],
            "(key = 'env' AND val = 'prod' AND match(val, '^(?:prod|staging)$'))"
        );
        assert_eq!(n.probe_keys, vec!["env".to_string()]);
    }

    #[test]
    fn negative_only_selector_is_rejected_as_empty_matcher_set() {
        let err = normalize_matchers(&selector(r#"{env!="prod"}"#)).unwrap_err();
        assert!(matches!(err, ReadError::EmptyMatcherSet));
    }

    #[test]
    fn negative_matchers_are_not_collapsed_per_key() {
        let n = normalize_matchers(&selector(
            r#"{service_name="checkout", team!="qa", team!="staging"}"#,
        ))
        .unwrap();
        assert_eq!(n.negative_branches.len(), 2);
    }

    #[test]
    fn months_overlapping_a_single_month_yields_one_literal() {
        // 2026-07-10T00:00:00Z .. 2026-07-15T00:00:00Z.
        let start = 1_783_641_600_000_000_000;
        let end = 1_784_073_600_000_000_000;
        assert_eq!(
            months_overlapping(start, end),
            vec!["'2026-07-01'".to_string()]
        );
    }

    #[test]
    fn months_overlapping_a_boundary_yields_two_literals() {
        // 2026-07-31T23:00:00Z .. 2026-08-01T01:00:00Z.
        let start = 1_785_538_800_000_000_000;
        let end = 1_785_546_000_000_000_000;
        assert_eq!(
            months_overlapping(start, end),
            vec!["'2026-07-01'".to_string(), "'2026-08-01'".to_string()]
        );
    }

    #[test]
    fn months_overlapping_a_year_boundary_advances_the_year() {
        // 2026-12-15 .. 2027-01-15.
        let start = 1_797_292_800_000_000_000;
        let end = 1_799_971_200_000_000_000;
        assert_eq!(
            months_overlapping(start, end),
            vec!["'2026-12-01'".to_string(), "'2027-01-01'".to_string()]
        );
    }

    fn streams_sp(query: &str) -> StreamsPlan {
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let expr = parse(query).expect("parse");
        match plan(&expr, &params, &test_ctx()).expect("plan") {
            Plan::Streams(sp) => sp,
            Plan::Metric(_) | Plan::MetricBinary(_) => panic!("expected a Streams plan"),
        }
    }

    // --- AC9(i), issue M6-09: scan_limit oversample eligibility. ---

    #[test]
    fn a_label_filter_pipeline_oversamples_the_scan_limit_by_the_factor() {
        let sp = streams_sp(r#"{env="prod"} | json | status = "500""#);
        assert_eq!(sp.result_limit, 100);
        assert_eq!(
            sp.scan_limit, 1_000,
            "scan_limit must be the first-page size limit * factor"
        );
        // Issue #90 AC2: a dropping pipeline engages fetch-until-limit.
        assert!(sp.fetch_until_limit);
    }

    #[test]
    fn a_line_filter_after_line_format_oversamples_the_scan_limit() {
        let sp = streams_sp(r#"{env="prod"} | line_format "{{.x}}" |= "err""#);
        assert_eq!(sp.result_limit, 100);
        assert_eq!(sp.scan_limit, 1_000);
        assert!(sp.fetch_until_limit);
        // And the unpushable filter is absent from the stage-3 predicates.
        assert!(sp.line_filters.is_empty());
    }

    #[test]
    fn a_line_filter_only_pipeline_keeps_scan_limit_equal_to_the_limit() {
        let sp = streams_sp(r#"{env="prod"} |= "err" != "debug""#);
        assert_eq!(sp.result_limit, 100);
        assert_eq!(sp.scan_limit, 100, "fast path must stay byte-identical");
        // Issue #90 AC2: the fast path never pages.
        assert!(!sp.fetch_until_limit);
        assert_eq!(sp.line_filters.len(), 2);
    }

    #[test]
    fn a_parser_only_pipeline_keeps_scan_limit_equal_to_the_limit() {
        // Parsers are non-dropping (a parse failure keeps the line with
        // an `__error__` label) — no oversample, no paging.
        let sp = streams_sp(r#"{env="prod"} |= "err" | json"#);
        assert_eq!(sp.scan_limit, 100);
        assert!(!sp.fetch_until_limit);
        assert_eq!(
            sp.line_filters.len(),
            1,
            "the line filter still pushes down"
        );
    }

    #[test]
    fn scan_limit_saturates_instead_of_overflowing() {
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
            limit: u32::MAX,
            direction: Direction::Backward,
        };
        let expr = parse(r#"{env="prod"} | json | status = "500""#).expect("parse");
        let Plan::Streams(sp) = plan(&expr, &params, &test_ctx()).expect("plan") else {
            panic!("expected a Streams plan");
        };
        assert_eq!(sp.scan_limit, u32::MAX);
    }

    // --- AC6, issue M6-10: the former M6-09 deferral seam is REMOVED —
    // --- every parseable metric pipeline now plans successfully in
    // --- client mode (the exact query list the M6-09 rejection test
    // --- covered, flipped to success).

    fn range_spec() -> QuerySpec {
        QuerySpec::Range {
            start_ns: 0,
            end_ns: 1_000_000_000_000,
            step_ns: 60_000_000_000,
        }
    }

    #[test]
    fn every_formerly_deferred_metric_pipeline_now_plans_in_client_mode() {
        for query in [
            r#"count_over_time({env="prod"} | json [5m])"#,
            r#"count_over_time({env="prod"} | logfmt [5m])"#,
            r#"count_over_time({env="prod"} | regexp "(?P<x>.*)" [5m])"#,
            r#"count_over_time({env="prod"} | pattern "<x> y" [5m])"#,
            r#"count_over_time({env="prod"} | json | status = "500" [5m])"#,
            r#"rate({env="prod"} | level = "error" [5m])"#,
            r#"rate({env="prod"} | line_format "{{.x}}" [5m])"#,
            r#"rate({env="prod"} | label_format a=b [5m])"#,
            r#"rate({env="prod"} | json | unwrap latency [5m])"#,
            r#"rate({env="prod"} | unwrap latency [5m])"#,
        ] {
            let mp = metric_mp(query, range_spec())
                .unwrap_or_else(|e| panic!("expected {query:?} to plan in client mode, got {e}"));
            let client = mp
                .client
                .as_ref()
                .unwrap_or_else(|| panic!("expected {query:?} to carry a client-aggregation spec"));
            assert!(!mp.rollup, "client mode always routes raw: {query}");
            assert_eq!(
                mp.routing.reason, "raw: client-side pipeline/unwrap aggregation",
                "{query}"
            );
            assert_eq!(
                client.pipeline.len(),
                mp.client.as_ref().unwrap().pipeline.len()
            );
        }
    }

    #[test]
    fn client_value_source_follows_op_and_unwrap_presence() {
        let count = metric_mp(
            r#"count_over_time({env="prod"} | json | status = "500" [5m])"#,
            range_spec(),
        )
        .unwrap();
        assert_eq!(count.client.as_ref().unwrap().value, ClientValue::Count);

        let bytes =
            metric_mp(r#"bytes_over_time({env="prod"} | json [5m])"#, range_spec()).unwrap();
        assert_eq!(bytes.client.as_ref().unwrap().value, ClientValue::Bytes);

        let unwrap = metric_mp(
            r#"sum_over_time({env="prod"} | logfmt | unwrap took [5m])"#,
            range_spec(),
        )
        .unwrap();
        assert_eq!(unwrap.client.as_ref().unwrap().value, ClientValue::Unwrap);
        assert_eq!(
            unwrap.client.as_ref().unwrap().range_op,
            pulsus_logql::RangeAggOp::SumOverTime
        );
    }

    #[test]
    fn every_new_over_time_op_plans_in_client_mode_with_unwrap() {
        for (query, op) in [
            (
                r#"sum_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::SumOverTime,
            ),
            (
                r#"avg_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::AvgOverTime,
            ),
            (
                r#"min_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::MinOverTime,
            ),
            (
                r#"max_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::MaxOverTime,
            ),
            (
                r#"stddev_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::StddevOverTime,
            ),
            (
                r#"stdvar_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::StdvarOverTime,
            ),
            (
                r#"quantile_over_time(0.9, {e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::QuantileOverTime,
            ),
            (
                r#"first_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::FirstOverTime,
            ),
            (
                r#"last_over_time({e="p"} | logfmt | unwrap v [5m])"#,
                pulsus_logql::RangeAggOp::LastOverTime,
            ),
        ] {
            let mp = metric_mp(query, range_spec()).unwrap_or_else(|e| panic!("{query}: {e}"));
            let client = mp.client.as_ref().expect("client mode");
            assert_eq!(client.range_op, op, "{query}");
            assert_eq!(client.value, ClientValue::Unwrap, "{query}");
        }
    }

    #[test]
    fn quantile_over_time_param_is_parsed_onto_the_client_spec() {
        let mp = metric_mp(
            r#"quantile_over_time(0.95, {e="p"} | logfmt | unwrap v [5m])"#,
            range_spec(),
        )
        .unwrap();
        assert_eq!(mp.client.as_ref().unwrap().param, Some(0.95));
    }

    /// AC6: unwrap-required ops without `unwrap` are a NAMED
    /// `PipelineInvalid` (message mirrors the oracle's parse error).
    #[test]
    fn unwrap_required_ops_without_unwrap_are_named_pipeline_invalid() {
        for op in [
            "sum_over_time",
            "avg_over_time",
            "min_over_time",
            "max_over_time",
            "stddev_over_time",
            "stdvar_over_time",
            "first_over_time",
            "last_over_time",
        ] {
            let query = format!(r#"{op}({{e="p"}} | logfmt [5m])"#);
            match metric_mp(&query, range_spec()).unwrap_err() {
                ReadError::PipelineInvalid { reason } => {
                    assert_eq!(reason, format!("invalid aggregation {op} without unwrap"));
                }
                other => panic!("expected {query:?} to be PipelineInvalid, got {other:?}"),
            }
        }
    }

    #[test]
    fn unwrap_forbidding_ops_with_unwrap_are_named_pipeline_invalid() {
        for op in ["count_over_time", "bytes_rate", "bytes_over_time"] {
            let query = format!(r#"{op}({{e="p"}} | logfmt | unwrap v [5m])"#);
            match metric_mp(&query, range_spec()).unwrap_err() {
                ReadError::PipelineInvalid { reason } => {
                    assert_eq!(reason, format!("invalid aggregation {op} with unwrap"));
                }
                other => panic!("expected {query:?} to be PipelineInvalid, got {other:?}"),
            }
        }
    }

    #[test]
    fn absent_over_time_plans_selector_wide_with_eq_matcher_labels() {
        let mp = metric_mp(
            r#"absent_over_time({env="prod", team=~"a|b", region="eu"}[5m])"#,
            range_spec(),
        )
        .unwrap();
        let client = mp.client.as_ref().expect("client mode");
        assert_eq!(client.range_op, pulsus_logql::RangeAggOp::AbsentOverTime);
        assert_eq!(
            client.absent_labels,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("region".to_string(), "eu".to_string()),
            ],
            "only Eq matchers become the synthetic-absence labels"
        );
    }

    /// D3 (plan v2): only the line-filter prefix BEFORE the first
    /// `line_format` pushes to SQL; a post-`line_format` filter evaluates
    /// in-engine (it references the rewritten line).
    #[test]
    fn a_post_line_format_metric_line_filter_is_not_pushed_down() {
        let mp = metric_mp(
            r#"count_over_time({env="prod"} |= "a" | line_format "{{.x}}" |= "b" [5m])"#,
            range_spec(),
        )
        .unwrap();
        assert_eq!(
            mp.extra_predicates.len(),
            1,
            "only the pre-line_format filter pushes down"
        );
        assert!(mp.extra_predicates[0].contains("'a'"));
        assert!(!mp.extra_predicates[0].contains("'b'"));
        // The full ordered pipeline (including the unpushed filter) rides
        // the client spec for in-engine evaluation.
        assert_eq!(mp.client.as_ref().unwrap().pipeline.len(), 3);
    }

    // --- Binary-op planning (issue M6-10). ---

    fn plan_of(query: &str, spec: QuerySpec) -> Result<Plan, ReadError> {
        let params = QueryParams {
            spec,
            limit: 100,
            direction: Direction::Backward,
        };
        let expr = parse(query).expect("parse");
        plan(&expr, &params, &test_ctx())
    }

    #[test]
    fn a_binary_metric_expression_plans_to_a_node_tree_with_ordinary_leaves() {
        let p = plan_of(
            r#"rate({env="prod"}[5m]) + rate({env="staging"}[5m])"#,
            range_spec(),
        )
        .unwrap();
        let Plan::MetricBinary(node) = p else {
            panic!("expected a MetricBinary plan, got {p:?}");
        };
        let MetricNode::Binary {
            op, return_bool, ..
        } = &node
        else {
            panic!("expected a Binary root");
        };
        assert_eq!(*op, BinOp::Add);
        assert!(!return_bool);
        let leaves = node.leaves();
        assert_eq!(leaves.len(), 2);
        // Each leaf routes exactly as it would standalone — issue #227: a
        // range leaf slides raw (client-aggregated), never rollup.
        for leaf in leaves {
            assert!(!leaf.rollup);
            assert!(leaf.client.is_some());
        }
    }

    #[test]
    fn a_scalar_only_binary_expression_plans_leafless() {
        let p = plan_of("2 ^ 2 ^ 3", range_spec()).unwrap();
        let Plan::MetricBinary(node) = p else {
            panic!("expected a MetricBinary plan");
        };
        assert!(node.leaves().is_empty());
    }

    #[test]
    fn a_zero_step_range_is_rejected_even_for_a_leafless_binary_expression() {
        let err = plan_of(
            "2 + 2",
            QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000,
                step_ns: 0,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ReadError::InvalidStep));
    }

    #[test]
    fn a_vector_aggregation_over_a_binary_operand_plans_as_a_vector_agg_node() {
        let p = plan_of(
            r#"sum by (service_name) (rate({a="b"}[5m]) + rate({a="c"}[5m]))"#,
            range_spec(),
        )
        .unwrap();
        let Plan::MetricBinary(node) = p else {
            panic!("expected a MetricBinary plan");
        };
        let MetricNode::VectorAgg { aggs, inner } = &node else {
            panic!("expected a VectorAgg root, got {node:?}");
        };
        assert_eq!(aggs.len(), 1);
        assert_eq!(aggs[0].0, VectorAggOp::Sum);
        // Issue #272: reached through a driver. The single-opened-child
        // escape hatch this once went around is gone with #293 — census
        // check (j) now asserts its absence.
        let _ = inner;
        let mut kids: Vec<&MetricNode> = Vec::new();
        walk::postorder_into::<MetricNodeScc>(&node, &mut kids);
        assert!(
            kids.iter().any(|n| matches!(n, MetricNode::Binary { .. })),
            "the VectorAgg's inner node is the Binary"
        );
    }

    #[test]
    fn a_vector_aggregation_over_a_bare_scalar_is_rejected() {
        let err = plan_of("sum(2)", range_spec()).unwrap_err();
        assert!(matches!(err, ReadError::PipelineInvalid { .. }));
    }

    #[test]
    fn topk_k_is_parsed_onto_the_vector_agg_chain() {
        let mp = metric_mp(r#"topk(5, rate({env="prod"}[5m]))"#, range_spec()).unwrap();
        assert_eq!(mp.vector_aggs.len(), 1);
        assert_eq!(mp.vector_aggs[0].0, VectorAggOp::Topk);
        assert_eq!(mp.vector_aggs[0].2, Some(5.0));
        // topk/bottomk never disturb the inner query's routing — issue #227:
        // a range leaf slides raw (client-aggregated), never rollup.
        assert!(!mp.rollup);
        assert!(mp.client.is_some());
    }

    #[test]
    fn a_metric_query_with_only_line_filters_still_plans() {
        let mp = metric_mp(
            r#"count_over_time({env="prod"} |= "err" [5m])"#,
            QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
        )
        .expect("line-filter-only metric pipelines are in scope");
        assert_eq!(mp.extra_predicates.len(), 1);
    }

    // --- Bare-query unwrap: the planner-owned rejection (plan v3 D1). ---

    #[test]
    fn a_bare_log_query_with_unwrap_is_rejected_as_pipeline_invalid() {
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000_000,
                step_ns: 60_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let expr = parse(r#"{env="prod"} | json | unwrap latency"#).expect("parse");
        let err = plan(&expr, &params, &test_ctx()).unwrap_err();
        match err {
            ReadError::PipelineInvalid { reason } => {
                assert!(reason.contains("range aggregation"), "{reason}");
            }
            other => panic!("expected PipelineInvalid, got {other:?}"),
        }
    }

    #[test]
    fn tokenize_splits_on_non_alphanumeric_boundaries() {
        assert_eq!(
            tokenize("connection refused"),
            vec!["connection".to_string(), "refused".to_string()]
        );
    }

    #[test]
    fn is_plain_literal_rejects_regex_metacharacters() {
        assert!(is_plain_literal("connection refused"));
        assert!(!is_plain_literal("test.*"));
    }
    // -----------------------------------------------------------------
    // Issue #221: `variants(...) of (...)` — plan-time validation,
    // charges (I8–I13) and censuses.
    // -----------------------------------------------------------------

    fn variants_plan_of(query: &str, spec: QuerySpec) -> Result<Plan, ReadError> {
        let params = QueryParams {
            spec,
            limit: 100,
            direction: Direction::Backward,
        };
        let expr = parse(query).expect("parse");
        plan(&expr, &params, &test_ctx())
    }

    fn variants_parts(query: &str, spec: QuerySpec) -> (MetricPlan, Vec<VariantSpec>, u64) {
        // Issue #272: E0509 — re-bind through a reference and take the
        // owned pieces out of the borrow.
        let mut planned = variants_plan_of(query, spec).expect("plan");
        match &mut planned {
            Plan::MetricBinary(MetricNode::Variants {
                scan,
                variants,
                spec_bytes,
            }) => ((**scan).clone(), std::mem::take(variants), *spec_bytes),
            other => panic!("expected a variants plan, got {other:?}"),
        }
    }

    fn spec_bytes_of(query: &str) -> u64 {
        variants_parts(
            query,
            QuerySpec::Instant {
                at_ns: 60_000_000_000,
            },
        )
        .2
    }

    /// B1 / AC 4 — every non-conforming variant shape (binary, literal,
    /// `vector(1)`, doubly-nested vector agg, `approx_topk`, and — since
    /// issue #276 put it in the grammar — `label_replace`, on which the
    /// reference nil-panics like the binary shape) is rejected at PLAN
    /// time with the single named message.
    #[test]
    fn variants_rejects_every_nonconforming_variant_shape() {
        for q in [
            r#"variants(count_over_time({a="b"}[5m]) + 1) of ({a="b"}[5m])"#,
            r#"variants(1) of ({a="b"}[5m])"#,
            r#"variants(vector(1)) of ({a="b"}[5m])"#,
            r#"variants(sum(sum(count_over_time({a="b"}[5m])))) of ({a="b"}[5m])"#,
            r#"variants(approx_topk(1, count_over_time({a="b"}[5m]))) of ({a="b"}[5m])"#,
            r#"variants(label_replace(count_over_time({a="b"}[5m]), "d", "r", "s", ".*")) of ({a="b"}[5m])"#,
        ] {
            match variants_plan_of(
                q,
                QuerySpec::Instant {
                    at_ns: 60_000_000_000,
                },
            ) {
                Err(ReadError::PipelineInvalid { reason }) => assert!(
                    reason.contains("must be a range aggregation"),
                    "{q}: {reason}"
                ),
                other => panic!("{q}: expected the variant-shape rejection, got {other:?}"),
            }
        }
    }

    /// B2 / AC 4 — unwrap arity is decided by the VARIANT's own pipeline
    /// (the reference messages verbatim); a common-range unwrap alone
    /// trips NEITHER (it is dead syntax — Δ1).
    #[test]
    fn variants_unwrap_arity_is_decided_by_the_variants_own_pipeline() {
        match variants_plan_of(
            r#"variants(sum_over_time({a="b"}[5m])) of ({a="b"}[5m])"#,
            QuerySpec::Instant {
                at_ns: 60_000_000_000,
            },
        ) {
            Err(ReadError::PipelineInvalid { reason }) => {
                assert_eq!(reason, "invalid aggregation sum_over_time without unwrap");
            }
            other => panic!("expected the without-unwrap rejection, got {other:?}"),
        }
        match variants_plan_of(
            r#"variants(count_over_time({a="b"} | logfmt | unwrap v [5m])) of ({a="b"}[5m])"#,
            QuerySpec::Instant {
                at_ns: 60_000_000_000,
            },
        ) {
            Err(ReadError::PipelineInvalid { reason }) => {
                assert_eq!(reason, "invalid aggregation count_over_time with unwrap");
            }
            other => panic!("expected the with-unwrap rejection, got {other:?}"),
        }
        // A COMMON-range unwrap is dead syntax: no arity trip, and the
        // scan pipeline is truncated (B3 below).
        variants_plan_of(
            r#"variants(count_over_time({a="b"}[5m])) of ({a="b"} | logfmt | unwrap v [5m])"#,
            QuerySpec::Instant {
                at_ns: 60_000_000_000,
            },
        )
        .expect("a common-range unwrap must not trip the variant arity rule");
    }

    /// B3 / AC 5 — the scan's common pipeline is truncated at the first
    /// `Stage::Unwrap`, post-`unwrap` filters dropped: byte-identical to
    /// the same query written without the common unwrap.
    #[test]
    fn variants_scan_truncates_the_common_pipeline_at_unwrap() {
        let spec = QuerySpec::Instant {
            at_ns: 60_000_000_000,
        };
        let (with_unwrap, ..) = variants_parts(
            r#"variants(count_over_time({a="b"}[5m])) of ({a="b"} | logfmt | unwrap v | v > 1 [5m])"#,
            spec,
        );
        let (without, ..) = variants_parts(
            r#"variants(count_over_time({a="b"}[5m])) of ({a="b"} | logfmt [5m])"#,
            spec,
        );
        let wu = with_unwrap.client.expect("client scan");
        let wo = without.client.expect("client scan");
        assert_eq!(
            wu.pipeline, wo.pipeline,
            "truncated common == unwrap-free common"
        );
        assert_eq!(with_unwrap.stage1_sql, without.stage1_sql);
        // The variants scan routes raw with its own named reason and is
        // always client-aggregated (`force_client`).
        assert_eq!(
            with_unwrap.routing.reason,
            "raw: variants single-pass multi-extractor scan"
        );
    }

    /// B4 / AC 9+10, re-derived by issue #236 (AC 35).
    ///
    /// Deleting `AggCaps::series` (the mid-scan 500-group cap) makes
    /// `min_field()` land on `MAX_TS_COLLISION_GROUP`, so the DERIVED
    /// backstop moves **500 → 10 000**. Strictly permissive, in the
    /// direction the reference sits (it is unbounded here).
    ///
    /// **The `cap + 1` boundary is UNREACHABLE, and that is a finding
    /// against the plan, not a number to update.** Plan v14 pins to
    /// `d145ded`, which predates #279's query-text cap. At the shipped
    /// `pulsus_logql::MAX_QUERY_BYTES` (131 072, exclusive) the shortest
    /// legal variant expression is 28 bytes plus a 2-byte separator, so
    /// the largest expressible variant count is **4 368** and a
    /// 10 001-variant query is 300 055 bytes — rejected as `QueryTooLong`
    /// long before the backstop is consulted. The runtime rejection is
    /// therefore asserted **iff reachable**, the verdict computed from the
    /// two constants rather than assumed; this mirrors the reachability
    /// branch plan v14 §5.6 already prescribes for the O6/O7 thresholds.
    /// The arithmetic half of the derivation is asserted unconditionally
    /// (here and in `agg_caps_default_is_the_constants_and_divides_soundly`).
    #[test]
    fn variants_past_the_derived_backstop_reject_at_plan_time() {
        let cap = MAX_VARIANT_SUB_STATES;
        assert_eq!(cap, 10_000, "the #236 re-derivation");

        // The reachability verdict, computed — never assumed.
        const VARIANT: &str = r#"count_over_time({a="b"}[5m])"#;
        let text_bytes = |n: usize| {
            "variants(".len()
                + n * VARIANT.len()
                + n.saturating_sub(1) * 2
                + ") of ({a=\"b\"}[5m])".len()
        };
        let reachable = text_bytes(cap as usize + 1) < pulsus_logql::MAX_QUERY_BYTES;
        assert!(
            !reachable,
            "the backstop became reachable — assert the cap+1 rejection end-to-end \
             ({} bytes at n = {})",
            text_bytes(cap as usize + 1),
            cap + 1
        );

        // Largest expressible count, asserted so the verdict is a measured
        // property of the two constants and not a comment.
        let max_expressible = (1..)
            .take_while(|n| text_bytes(*n) < pulsus_logql::MAX_QUERY_BYTES)
            .last()
            .expect("at least one variant is expressible");
        assert_eq!(max_expressible, 4_368);
        assert!(
            (max_expressible as u64) < cap,
            "backstop sits above the parse cap"
        );

        // The acceptance win the re-derivation buys, and it IS reachable:
        // 600 variants was a `VariantSubStates` 422 before #236.
        let six_hundred = format!(
            "variants({}) of ({{a=\"b\"}}[5m])",
            vec![VARIANT; 600].join(", ")
        );
        variants_plan_of(
            &six_hundred,
            QuerySpec::Instant {
                at_ns: 60_000_000_000,
            },
        )
        .expect("600 variants must plan after the #236 re-derivation");

        // The largest expressible query still plans, so nothing between
        // the old backstop and the parse cap is refused by this guard.
        let widest = format!(
            "variants({}) of ({{a=\"b\"}}[5m])",
            vec![VARIANT; max_expressible].join(", ")
        );
        assert!(widest.len() < pulsus_logql::MAX_QUERY_BYTES);
        variants_plan_of(
            &widest,
            QuerySpec::Instant {
                at_ns: 60_000_000_000,
            },
        )
        .expect("the widest expressible variants query must plan");
    }

    /// AC 14 — a 1-variant query is admitted exactly when the equivalent
    /// single-extractor query is, and its only charge is `spec_bytes`
    /// (for a bare variant: exactly the spec-vector buffer term — every
    /// per-spec term is 0).
    #[test]
    fn one_variant_query_charges_only_the_spec_vector_buffer() {
        let spec = QuerySpec::Instant {
            at_ns: 60_000_000_000,
        };
        variants_plan_of(r#"count_over_time({a="b"}[5m])"#, spec)
            .expect("the plain query is admitted");
        let (_, variants, spec_bytes) = variants_parts(
            r#"variants(count_over_time({a="b"}[5m])) of ({a="b"}[5m])"#,
            spec,
        );
        assert_eq!(variants.len(), 1);
        assert_eq!(
            spec_bytes,
            crate::logql::variants::vec_buffer_bytes::<VariantSpec>(1)
        );
    }

    /// I8 — CHARGE: the spec's tail terms (buffer + clone factor). Pair:
    /// tail `[unwrap v]` vs `[unwrap v, v > 1, v < 9]` (single axis: the
    /// tail). Deleting either `pipeline` term zeroes half the delta.
    #[test]
    fn i8_spec_pipeline_terms_are_charged() {
        let short = spec_bytes_of(
            r#"variants(sum_over_time({a="b"} | unwrap v [5m])) of ({a="b"} | logfmt [5m])"#,
        );
        let (_, long_specs, long) = variants_parts(
            r#"variants(sum_over_time({a="b"} | unwrap v | v > 1 | v < 9 [5m])) of ({a="b"} | logfmt [5m])"#,
            QuerySpec::Instant {
                at_ns: 60_000_000_000,
            },
        );
        let (_, short_specs, _) = variants_parts(
            r#"variants(sum_over_time({a="b"} | unwrap v [5m])) of ({a="b"} | logfmt [5m])"#,
            QuerySpec::Instant {
                at_ns: 60_000_000_000,
            },
        );
        let long_tail = &long_specs[0].client().pipeline;
        let short_tail = &short_specs[0].client().pipeline;
        let expected = (crate::logql::variants::vec_buffer_bytes::<Stage>(long_tail.len() as u64)
            - crate::logql::variants::vec_buffer_bytes::<Stage>(short_tail.len() as u64))
            + (crate::logql::variants::stage_source_bytes(long_tail)
                - crate::logql::variants::stage_source_bytes(short_tail))
                * 130;
        assert!(expected > 0);
        assert_eq!(long - short, expected);
    }

    /// I9 — CHARGE: the spec's absent-label terms. Pair: an absent
    /// variant whose OWN selector carries 3 Eq matchers vs 1.
    #[test]
    fn i9_spec_absent_labels_terms_are_charged() {
        let three = spec_bytes_of(
            r#"variants(absent_over_time({a="1", b="2", c="3"}[5m])) of ({a="1"}[5m])"#,
        );
        let one = spec_bytes_of(r#"variants(absent_over_time({a="1"}[5m])) of ({a="1"}[5m])"#);
        let expected = (crate::logql::variants::vec_buffer_bytes::<(String, String)>(3)
            - crate::logql::variants::vec_buffer_bytes::<(String, String)>(1))
            + 2 * (crate::logql::charge::alloc_block_bytes(1)
                + crate::logql::charge::alloc_block_bytes(1));
        assert!(expected > 0);
        assert_eq!(three - one, expected);
    }

    /// I10 — CHARGE: the grouping terms, including the CREATED grouping
    /// (member M3: bare `sum` gets `by (__variant__)` from nothing — its
    /// low side must still charge the created buffer + injected label).
    #[test]
    fn i10_spec_grouping_terms_are_charged() {
        let declared = spec_bytes_of(
            r#"variants(sum by (aa, bb, cc) (count_over_time({a="b"}[5m]))) of ({a="b"}[5m])"#,
        );
        let bare = spec_bytes_of(r#"variants(sum(count_over_time({a="b"}[5m]))) of ({a="b"}[5m])"#);
        let ptr = size_of::<String>() as u64;
        let expected = (crate::logql::charge::grown_alloc_bytes(4 * ptr)
            - crate::logql::charge::grown_alloc_bytes(ptr))
            + 3 * crate::logql::charge::alloc_block_bytes(2);
        assert!(expected > 0);
        assert_eq!(declared - bare, expected);
    }

    /// I11 — CHARGE: the `vector_aggs` buffer term (grown — the
    /// `Result<Vec<_>>` collect grows by pushes, C1), isolated from I10:
    /// one layer vs none moves the buffer term PLUS the created-grouping
    /// terms, so I11 fails alone when only the buffer term is deleted.
    #[test]
    fn i11_spec_vector_agg_buffer_term_is_charged() {
        let one_layer =
            spec_bytes_of(r#"variants(sum(count_over_time({a="b"}[5m]))) of ({a="b"}[5m])"#);
        let bare = spec_bytes_of(r#"variants(count_over_time({a="b"}[5m])) of ({a="b"}[5m])"#);
        let ptr = size_of::<String>() as u64;
        let expected = crate::logql::charge::grown_alloc_bytes(size_of::<VectorAggSpec>() as u64)
            + crate::logql::charge::grown_alloc_bytes(ptr)
            + crate::logql::charge::alloc_block_bytes(
                crate::logql::variants::VARIANT_LABEL.len() as u64
            );
        assert_eq!(one_layer - bare, expected);
    }

    /// I12 — the spec charge GATES admission (an event, not a computed
    /// value): at exactly the sized bytes `try_new` admits; one byte
    /// under, it returns `VariantSpecBytes` — and the C2 spec-vector term
    /// is additive on top of the per-spec charges (deleting it collapses
    /// the N-delta to zero).
    #[test]
    fn i12_spec_charge_gates_admission_and_the_vec_term_is_additive() {
        let expr = parse(r#"sum by (env) (sum_over_time({a="1", t="x"} | unwrap v | v > 1 [5m]))"#)
            .expect("parse");
        let Expr::Metric(me) = &expr else { panic!() };
        let (base, raw) = unwrap_vector_aggs(me);
        let MetricExpr::Range { op, range, .. } = base else {
            panic!("range base")
        };
        let tail = variant_tail(&range.selector.pipeline);
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 60_000_000_000,
        };
        let sized =
            crate::logql::variants::variant_spec_bytes(tail, &range.selector.selector, false, me);
        assert!(sized > 0);
        let mut charged = 0u64;
        VariantSpec::try_new(
            &mut charged,
            sized,
            0,
            tail,
            &range.selector.selector,
            &raw,
            me,
            *op,
            ClientValue::Unwrap,
            None,
            false,
            window,
            false,
            None,
        )
        .expect("exactly the sized charge admits");
        assert_eq!(charged, sized);
        let mut charged = 0u64;
        match VariantSpec::try_new(
            &mut charged,
            sized - 1,
            0,
            tail,
            &range.selector.selector,
            &raw,
            me,
            *op,
            ClientValue::Unwrap,
            None,
            false,
            window,
            false,
            None,
        ) {
            Err(ReadError::QueryTooBroad(TooBroadReason::VariantSpecBytes { bytes, cap })) => {
                assert_eq!((bytes, cap), (sized, sized - 1));
            }
            other => panic!("expected VariantSpecBytes, got {other:?}"),
        }
        assert_eq!(charged, 0, "a refused spec charges nothing");
        // The C2 vec-buffer term is the only N-scaled plan-time residue,
        // charged once per query.
        let q2 = format!(
            "variants({}) of ({{a=\"b\"}}[5m])",
            [r#"count_over_time({a="b"}[5m])"#; 2].join(", ")
        );
        let q3 = format!(
            "variants({}) of ({{a=\"b\"}}[5m])",
            [r#"count_over_time({a="b"}[5m])"#; 3].join(", ")
        );
        assert_eq!(
            spec_bytes_of(&q3) - spec_bytes_of(&q2),
            crate::logql::variants::vec_buffer_bytes::<VariantSpec>(3)
                - crate::logql::variants::vec_buffer_bytes::<VariantSpec>(2)
        );
    }

    /// I13 — BEHAVIOUR only: the reused raw-aggregation buffer is
    /// CLEARED, not appended to (an append regression yields 3). The
    /// allocation claim for the reuse lives on the alloc-gate count
    /// bands, not on an end-state `capacity()` assertion.
    #[test]
    fn i13_unwrap_vector_aggs_into_clears_the_reused_buffer() {
        let two = parse(r#"sum(max(count_over_time({a="b"}[5m])))"#).expect("parse");
        let one = parse(r#"sum(count_over_time({a="b"}[5m]))"#).expect("parse");
        let (Expr::Metric(two), Expr::Metric(one)) = (&two, &one) else {
            panic!()
        };
        let mut buf = Vec::new();
        unwrap_vector_aggs_into(two, &mut buf);
        assert_eq!(buf.len(), 2);
        unwrap_vector_aggs_into(one, &mut buf);
        assert_eq!(buf.len(), 1, "the buffer is cleared, never appended to");
    }

    /// AC 32/59-style censuses over plan.rs production text (before the
    /// column-0 `#[cfg(test)]`, `//` text stripped — the `search_sql.rs`
    /// precedent).
    #[test]
    fn variants_plan_census() {
        let src = include_str!("plan.rs");
        let production = src
            .split("\n#[cfg(test)]")
            .next()
            .expect("split")
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        // Two plan-time charge sites: the C2 spec-vector buffer
        // (`build_variants_node`, `&mut charged`) and the per-spec charge
        // (`VariantSpec::try_new`, forwarding its `&mut u64` parameter).
        // (The v6 census text predates C2's second site — flagged in the
        // implementation notes.)
        let compact: String = production.chars().filter(|c| !c.is_whitespace()).collect();
        let charges = compact
            .matches("crate::logql::variants::charge_fanout_bytes(")
            .count();
        assert_eq!(charges, 2, "plan charge-site census");
        // The sole `VariantSpec { .. }` construction literal (the struct
        // declaration subtracted).
        // The declaration, the impl header and the field-guard's
        // DESTRUCTURING pattern (an indented `cfg(test)` module, kept out
        // of the column-0 split by design) are subtracted; what remains
        // is the construction literal.
        let literals = compact.matches("VariantSpec{").count()
            - compact.matches("structVariantSpec{").count()
            - compact.matches("implVariantSpec{").count()
            - compact.matches("letVariantSpec{").count();
        assert_eq!(literals, 1, "VariantSpec has exactly one construction site");
        // M1: the raw aggregation walk is borrowed — no grouping clone
        // outside the sole `grouping.cloned()` in `parse_vector_agg_params`.
        assert_eq!(compact.matches("grouping.clone()").count(), 0);
        assert_eq!(compact.matches("grouping.cloned()").count(), 1);
        // M5: the reused buffer's producer has exactly three production
        // call sites — the allocating single-shot wrapper, the variant
        // loop, and (issue #293) `emit_plan_ops`' vector-chain head,
        // which reuses ONE buffer across every chain in the expression
        // where the recursion it replaced allocated a fresh `Vec` per
        // chain. `build_variants_node` still never calls the wrapper.
        assert_eq!(compact.matches("unwrap_vector_aggs_into(").count(), 3);
        let bvn = {
            let start = production
                .find("fn build_variants_node(")
                .expect("build_variants_node");
            let tail = &production[start..];
            let end = tail.find("\n}").expect("fn end");
            &tail[..end]
        };
        assert!(
            !bvn.contains("unwrap_vector_aggs("),
            "the loop must reuse the buffer"
        );
        // M4: the injected grouping list sorts without a scratch buffer.
        assert!(!compact.contains("labels.sort()"), "sort_unstable only");
    }
}
