//! Issues #549 and #579: the aggregate nodes the database answers —
//! eligibility, group assignment, the threshold, and the reading of the
//! statements' rows back into each node's answer.
//!
//! Two shapes, at any node of the query:
//!
//! - **shape B** (#549): `min`/`max`/`count`/`group` over a plain instant
//!   selector, one run statement per fingerprint chunk;
//! - **shape A** (#579): `sum`/`avg`/`count`/`min`/`max` over
//!   `rate`/`irate`/`increase` of a plain range selector, one statement
//!   per node — split by time over [`PUSHED_SERIES_STEPS_PER_STATEMENT`] —
//!   returning one row per group per step.
//!
//! # Where the decision is taken
//!
//! ```text
//! query_inner
//!   |-- plan = pulsus_promql::plan(expr, params)
//!   |-- grouped::node_verdicts(&plan, &plan_params, &cfg)
//!   |     PURE, before the fetch loop. Every Aggregate node over a
//!   |     selector or a range function, with a PushShape or the
//!   |     NodeDecline that names why not. The flag is on.
//!   `-- the phase-1 loop, for each selector a pushed node owns
//!         resolution = resolver.resolve_labelled(...)   <- the ONE resolve
//!         grouped::decide / grouped::decide_range
//!           Series(pairs) -> gids from the pairs (shape B: then the
//!                            `series >= 2 * groups` threshold)
//!           SqlFallback   -> Err(ResolutionNotFingerprints)
//!         Ok  -> render the node's statements, skip the chunk builders
//!         Err -> fall through with the SAME resolution
//! ```
//!
//! The evaluator then takes each pushed node's vector at every step from
//! `SeriesData::pushed` and evaluates nothing below it; every other node
//! is evaluated over the pushed nodes' results as before.
//!
//! [`node_verdicts`] is the **sole owner** of the overflow guard and of
//! the feature flag: [`decide`] and [`decide_range`] re-check neither,
//! and neither statement builder re-checks either.
//!
//! # The group key never enters SQL
//!
//! A group id is assigned here, in this process, by
//! [`pulsus_promql::group_key_of`] — the same function the evaluator's
//! own aggregation uses, so the pushed route cannot partition series
//! differently from the unpushed one. `by`, `without` and bare therefore
//! produce byte-identical statement text for the same assignment; only
//! the `gids` array moves.
//!
//! # The threshold
//!
//! `series >= 2 * groups`, computed from the gid map before anything is
//! rendered.
//!
//! **It is a heuristic, and the quantity it stands in for is a TRANSITION
//! count, not a ratio.** A run ends where the group's answer changes, so
//! what a statement returns is how often that happens — which the series
//! and group counts do not determine. Measured on two corpora with the
//! same 100 series in the same 1 group:
//!
//! ```text
//!   100 one-sample series, evenly spaced, count    39 rows
//!   100 arrivals arranged to change the count at
//!     nearly every grid point, count              199 rows
//! ```
//!
//! The transition count is not knowable before the statement runs, so the
//! threshold uses what the resolver's answer alone supplies: it declines
//! where the query has as many groups as series, the shape with no
//! grouping work to do.
//!
//! **That is a proxy for the shape, and not a bound on rows on either
//! side of it.** Measured on a corpus the rule declines — three constant
//! series, one group each, 241 samples apiece — the push returns **3 rows
//! against the raw read's 723**, under every one of the four operations
//! (`crates/pulsus-read/tests/live_metrics_grouped.rs`, the corpus named
//! "one group per series, constant values (declined)"). So the threshold
//! turns away queries the push would have helped, and it claims nothing
//! about the ones it admits either.
//!
//! Rows ARE bounded either way: every run boundary is a sample's coverage
//! opening or closing, so a sample can begin at most two runs and
//! `pushed_rows <= 2 * raw_rows`. Bytes are not — the run row is wider
//! than a sample row and the wire size depends on framing.
//!
//! # What the charge counts
//!
//! The pushed route charges the per-statement **partial run rows** it
//! materialises, per row, inside the same drain loop and against the same
//! query-wide `SampleBudget` the unpushed route uses. That is a different
//! number from the source rows ClickHouse read, and deliberately so: the
//! budget's own contract is the query's total *materialized* rows, and
//! what this path materialises is runs. Owner ruling, 2026-09-17.

use std::collections::HashMap;

use pulsus_promql::{
    AggOp, Annotations, CancelToken, Grouping, InstantSample, Labels, PlanExpr, PlanParams, Point,
    PromqlError, PushedNode, QueryPlan, QueryValue, RangeSeries, SelectorId, group_key_of,
};

use super::exec::MetricsConfig;
use super::labels::LabelledResolution;
use pulsus_model::Fingerprint;

/// The four aggregations that are exactly reproducible in a statement
/// with no runtime condition.
///
/// `sum` and `avg` are not here: they need a per-group condition on
/// whether the group mixed floats and histograms. `stddev`/`stdvar`
/// accumulate a float through Welford's recurrence and are never pushed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupedOp {
    Min,
    Max,
    Count,
    Group,
}

impl GroupedOp {
    fn of(op: AggOp) -> Option<Self> {
        match op {
            AggOp::Min => Some(GroupedOp::Min),
            AggOp::Max => Some(GroupedOp::Max),
            AggOp::Count => Some(GroupedOp::Count),
            AggOp::Group => Some(GroupedOp::Group),
            _ => None,
        }
    }

    /// The name the ignored-histogram info annotation carries — the
    /// `Min`/`Max` arms of `pulsus-promql`'s own
    /// `ignored_in_aggregation_name`. `Count`/`Group` raise no
    /// annotation at all: they count a histogram member rather than
    /// ignoring it, so they have nothing to report.
    fn ignored_in_aggregation_name(self) -> Option<&'static str> {
        match self {
            GroupedOp::Min => Some("min"),
            GroupedOp::Max => Some("max"),
            GroupedOp::Count | GroupedOp::Group => None,
        }
    }
}

/// Why [`decide`] declined, and **only** what `decide` can produce.
///
/// [`node_verdicts`] consumes the aggregate shape, the plain-selector
/// checks, the grid arithmetic and the flag, so none of them has a variant
/// here. A refusal reason is written only
/// after naming the line that returns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclineReason {
    /// The resolver answered `SqlFallback` — a degraded or cold cache, so
    /// there is no fingerprint list to assign group ids over.
    ResolutionNotFingerprints,
    /// `series < 2 * groups` — as many groups as series, the shape with
    /// no grouping work to do. A proxy for that shape and not a row
    /// bound: see this module's header for a declined corpus that would
    /// have returned 3 rows against 723.
    TooFewSeriesPerGroup,
}

/// The evaluation grid the statement reduces onto.
///
/// `step_ms` is always `>= 1`: an instant query (`params.step_ms == 0`)
/// becomes a one-point grid with a step of 1, which the coverage
/// arithmetic divides by and the single `least(grid_n, …)` clamp makes
/// irrelevant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Grid {
    pub start_ms: i64,
    pub step_ms: i64,
    pub points: u32,
    pub lookback_ms: i64,
}

/// One partial run, as the reader holds it: a stretch of consecutive grid
/// indices over which one group's answer from ONE statement does not
/// change.
///
/// `flags` is `None` for `count`/`group`, whose templates carry no flags
/// column — the distinction is the row shape's, not a sentinel value.
#[derive(Debug, Clone, Copy)]
pub struct Run {
    pub gid: u32,
    pub gi_start: u32,
    pub gi_end: u32,
    pub agg: f64,
    pub flags: Option<u8>,
}

/// What [`node_verdicts`] establishes for a shape-B node without looking
/// at any resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupedShape {
    pub op: GroupedOp,
    pub selector: SelectorId,
    /// The selector's one concrete metric name, or `None` for a selector
    /// with no concrete name (issue #579 part 2). [`decide`] reads each
    /// member's own name into the group key's name channel.
    pub metric_name: Option<String>,
    pub grouping: Option<Grouping>,
    pub grid: Grid,
    /// The aggregated inner expression's start byte offset — the position
    /// `aggregate_reduce` gives its ignored-histogram info annotation
    /// (`e.Expr.PositionRange()`).
    pub expr_pos: usize,
    /// `true` for an instant query, whose answer is a vector; `false` for
    /// a range query, whose answer is a matrix.
    pub instant: bool,
}

/// What [`decide`] establishes once the resolution is in hand.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupedPush {
    pub op: GroupedOp,
    pub selector: SelectorId,
    pub metric_name: Option<String>,
    /// In member order, `(metric_name, fingerprint)`: the order the run
    /// statement's chunks and its NaN payload rule fold in.
    pub fingerprints: Vec<Fingerprint>,
    /// Parallel to `fingerprints`: `gids[i]` is the group id of
    /// `fingerprints[i]`.
    pub gids: Vec<u32>,
    /// Indexed by group id — the output identity of each group.
    pub groups: Vec<(Labels, Option<String>)>,
    pub grid: Grid,
    pub expr_pos: usize,
    pub instant: bool,
}

/// Issue #579: the largest `series x steps` one shape-A statement may
/// compute. A node over it is split by time (plan section 3.4): memory in
/// the database grows with series times steps, about 520 B each,
/// measured.
pub const PUSHED_SERIES_STEPS_PER_STATEMENT: usize = 1_048_576;

/// Issue #579: the group id of the row that carries the count of
/// histogram samples in a shape-A statement's window. No real group can
/// take it: a gid is below the series count, which the build-time
/// assertion below keeps under `u32::MAX`.
pub const HISTOGRAM_SENTINEL_GID: u32 = u32::MAX;

/// Issue #579: the aggregations shape A answers in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeAggOp {
    Sum,
    Avg,
    Count,
    Min,
    Max,
}

/// Issue #579: the range functions shape A computes in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushedRangeFn {
    Rate,
    Irate,
    Increase,
}

/// Issue #579: what [`pushed_nodes`] establishes for a shape-A node — an
/// aggregation over `rate`/`irate`/`increase` of a plain range selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeShape {
    pub op: RangeAggOp,
    pub func: PushedRangeFn,
    pub selector: SelectorId,
    pub metric_name: Option<String>,
    pub grouping: Option<Grouping>,
    /// The request's grid. `lookback_ms` is carried but unused: a range
    /// function's window is `range_ms`.
    pub grid: Grid,
    pub range_ms: i64,
    pub instant: bool,
}

/// Issue #579: the two kinds of node the database answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushKind {
    /// Shape A: plan section 3.1.
    Range(RangeShape),
    /// Shape B: issue #549's grouped instant read, at any node.
    Instant(GroupedShape),
}

/// Issue #579: one `Aggregate` node the database can answer, keyed by its
/// `self_pos` — the key the evaluator looks the answer up by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushShape {
    pub self_pos: usize,
    pub kind: PushKind,
}

impl PushShape {
    /// The one selector under this node.
    pub fn selector(&self) -> SelectorId {
        match &self.kind {
            PushKind::Range(r) => r.selector,
            PushKind::Instant(g) => g.selector,
        }
    }
}

/// Issue #579: why an `Aggregate` node over a selector or a range
/// function is not pushed, decided from the plan alone. The numbers are
/// the design's named fallbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeDecline {
    /// F1: an aggregation neither shape answers over this child.
    Aggregation,
    /// F2: a range function other than `rate`, `irate` or `increase`.
    RangeFunction,
    /// F3: the selector is not plain — `offset`, `@`, a subquery source,
    /// `anchored`/`smoothed`, the `info()` family, histogram statistics,
    /// or no single concrete metric name.
    Selector,
    /// F4: `by (__name__)` over a range function.
    NameGrouping,
    /// F8: the grid arithmetic would overflow in the database.
    GridOverflow,
}

/// Issue #579: what [`decide_range`] establishes once the resolution is in
/// hand.
#[derive(Debug, Clone, PartialEq)]
pub struct RangePush {
    pub op: RangeAggOp,
    pub func: PushedRangeFn,
    pub selector: SelectorId,
    /// In member order, `(metric_name, fingerprint)`.
    pub fingerprints: Vec<Fingerprint>,
    /// Parallel to `fingerprints`.
    pub gids: Vec<u32>,
    /// Indexed by group id.
    pub groups: Vec<(Labels, Option<String>)>,
    pub grid: Grid,
    pub range_ms: i64,
}

/// Issue #579: a shape-A node's rows, read back.
#[derive(Debug, Clone)]
pub enum RangeOutcome {
    /// The node's answer.
    Node(PushedNode),
    /// The sentinel row counted this many histogram samples in the
    /// window (F6): the answer is discarded and the selector is fetched
    /// as it is without the push.
    Histograms(u64),
}

/// Issue #579: every `Aggregate` node over a selector or a range function,
/// in written order, with its verdict. Nodes inside a subquery are not
/// visited: a subquery evaluates its inner expression on its own grid.
pub fn node_verdicts(
    plan: &QueryPlan,
    params: &PlanParams,
    cfg: &MetricsConfig,
) -> Vec<(usize, Result<PushShape, NodeDecline>)> {
    // The flag is read HERE and nowhere else in this module or in
    // `grouped_sql`.
    if !cfg.grouped_push {
        return Vec::new();
    }
    let mut out = Vec::new();
    walk(&plan.root, plan, params, &mut out);
    out
}

/// Visits `e` and its children in written order. A node that is pushed
/// is not descended into: everything below it is the database's.
fn walk(
    e: &PlanExpr,
    plan: &QueryPlan,
    params: &PlanParams,
    out: &mut Vec<(usize, Result<PushShape, NodeDecline>)>,
) {
    if let PlanExpr::Aggregate {
        op,
        expr,
        param,
        grouping,
        expr_pos,
        self_pos,
        ..
    } = e
        && let Some(verdict) = verdict_of(
            *op,
            expr,
            param.is_some(),
            grouping.as_ref(),
            *expr_pos,
            plan,
            params,
        )
    {
        let pushed = verdict.is_ok();
        out.push((
            *self_pos,
            verdict.map(|kind| PushShape {
                self_pos: *self_pos,
                kind,
            }),
        ));
        if pushed {
            return;
        }
    }
    for child in children(e) {
        walk(child, plan, params, out);
    }
}

/// A node's children in written order. **No `_` arm**, so a new
/// [`PlanExpr`] variant fails to compile here. A subquery's inner
/// expression is deliberately not a child: it is evaluated on the
/// subquery's own grid, never the request's.
fn children(e: &PlanExpr) -> Vec<&PlanExpr> {
    match e {
        PlanExpr::Selector(_)
        | PlanExpr::Time
        | PlanExpr::Scalar(_)
        | PlanExpr::StringLiteral(_)
        | PlanExpr::RangeVector { .. }
        | PlanExpr::RangeFn { .. }
        | PlanExpr::OverTime { .. }
        | PlanExpr::AbsentOverTime { .. } => Vec::new(),
        PlanExpr::OverTimeParam { args, .. } | PlanExpr::ScalarFn { args, .. } => {
            args.iter().map(Box::as_ref).collect()
        }
        PlanExpr::Absent { arg, .. }
        | PlanExpr::Sort { arg, .. }
        | PlanExpr::SortByLabel { arg, .. }
        | PlanExpr::LabelReplace { arg, .. }
        | PlanExpr::LabelJoin { arg, .. }
        | PlanExpr::HistogramAccessor { arg, .. }
        | PlanExpr::Timestamp { arg, .. }
        | PlanExpr::ScalarOf { arg }
        | PlanExpr::VectorOf { arg } => vec![arg.as_ref()],
        PlanExpr::HistogramQuantile { quantile, expr, .. } => vec![quantile, expr],
        PlanExpr::HistogramQuantiles {
            expr, quantiles, ..
        } => std::iter::once(expr.as_ref())
            .chain(quantiles.iter().map(Box::as_ref))
            .collect(),
        PlanExpr::HistogramFraction {
            lower, upper, expr, ..
        } => vec![lower, upper, expr],
        PlanExpr::Aggregate { expr, param, .. } => std::iter::once(expr.as_ref())
            .chain(param.iter().map(Box::as_ref))
            .collect(),
        PlanExpr::CountValues { expr, .. } => vec![expr.as_ref()],
        PlanExpr::Binary { lhs, rhs, .. } | PlanExpr::SetOp { lhs, rhs, .. } => vec![lhs, rhs],
        PlanExpr::MathFn {
            arg, scalar_args, ..
        } => std::iter::once(arg.as_ref())
            .chain(scalar_args.iter().map(Box::as_ref))
            .collect(),
        PlanExpr::DateFn { arg, .. } => arg.iter().map(Box::as_ref).collect(),
        PlanExpr::Info { base, .. } => vec![base.as_ref()],
    }
}

/// The verdict on one `Aggregate` node, or `None` when its child is
/// neither a selector nor a range function — not a candidate for either
/// shape, so there is nothing to report.
fn verdict_of(
    op: AggOp,
    expr: &PlanExpr,
    has_param: bool,
    grouping: Option<&Grouping>,
    expr_pos: usize,
    plan: &QueryPlan,
    params: &PlanParams,
) -> Option<Result<PushKind, NodeDecline>> {
    match expr {
        // Shape B: issue #549's checks, at any node.
        PlanExpr::Selector(id) => {
            let Some(op) = GroupedOp::of(op).filter(|_| !has_param) else {
                return Some(Err(NodeDecline::Aggregation));
            };
            let sel = plan.selectors.iter().find(|s| s.id == *id)?;
            let Some(metric_name) = plain_metric_name(sel).filter(|_| sel.range_ms.is_none())
            else {
                return Some(Err(NodeDecline::Selector));
            };
            let Some(grid) = grid_of(params) else {
                return Some(Err(NodeDecline::GridOverflow));
            };
            Some(Ok(PushKind::Instant(GroupedShape {
                op,
                selector: *id,
                metric_name,
                grouping: grouping.cloned(),
                grid,
                expr_pos,
                instant: params.step_ms == 0,
            })))
        }
        // Shape A.
        PlanExpr::RangeFn { func, source, .. } => {
            let Some(op) = RangeAggOp::of(op).filter(|_| !has_param) else {
                return Some(Err(NodeDecline::Aggregation));
            };
            let Some(func) = PushedRangeFn::of(*func) else {
                return Some(Err(NodeDecline::RangeFunction));
            };
            let pulsus_promql::plan::RangeSource::Selector(id) = source else {
                return Some(Err(NodeDecline::Selector));
            };
            let sel = plan.selectors.iter().find(|s| s.id == *id)?;
            let (Some(metric_name), Some(range_ms)) = (plain_metric_name(sel), sel.range_ms) else {
                return Some(Err(NodeDecline::Selector));
            };
            // The output's name channel is the group key's, which the gid
            // does not carry.
            if grouping.is_some_and(|g| !g.without && g.labels.iter().any(|l| l == "__name__")) {
                return Some(Err(NodeDecline::NameGrouping));
            }
            let Some(grid) = range_grid_of(params, range_ms) else {
                return Some(Err(NodeDecline::GridOverflow));
            };
            Some(Ok(PushKind::Range(RangeShape {
                op,
                func,
                selector: *id,
                metric_name,
                grouping: grouping.cloned(),
                grid,
                range_ms,
                instant: params.step_ms == 0,
            })))
        }
        PlanExpr::OverTime { .. } | PlanExpr::OverTimeParam { .. } => {
            Some(Err(if RangeAggOp::of(op).is_some() && !has_param {
                NodeDecline::RangeFunction
            } else {
                NodeDecline::Aggregation
            }))
        }
        _ => None,
    }
}

/// `None` when the selector is not PLAIN: an offset, an `@`, an enclosing
/// subquery context, the `info()` family, a histogram-stats reduction,
/// either extended-range modifier, or a concrete name that also carries
/// `__name__` matchers. Otherwise its concrete name, or `Some(None)` for a
/// selector with no concrete name — a `__name__` regex or label matchers
/// only, which the multi-name fan-out resolves (issue #579 part 2). The
/// range is the caller's to check.
fn plain_metric_name(sel: &pulsus_promql::SelectorSpec) -> Option<Option<String>> {
    let name = sel.metric_name.clone();
    if (name.is_some() && !sel.name_matchers.is_empty())
        || sel.offset_ms != 0
        || sel.at_ms.is_some()
        || sel.fetch != pulsus_promql::plan::FetchExtent::default()
        || sel.info_family
        || sel.histogram_stats
        || sel.anchored
        || sel.smoothed
    {
        return None;
    }
    Some(name)
}

/// Issue #579 part 2: one series a pushed node aggregates — its ID, its
/// own metric name, and its labels.
#[derive(Debug, Clone, PartialEq)]
pub struct Member {
    pub fingerprint: Fingerprint,
    pub metric_name: String,
    pub labels: pulsus_model::LabelSet,
}

/// Issue #579 part 2: a concrete-name selector's members. A `SqlFallback`
/// resolution has no fingerprint list to assign groups over.
pub fn members_of(
    resolution: &LabelledResolution,
    metric_name: &str,
) -> Result<Vec<Member>, DeclineReason> {
    let LabelledResolution::Series(pairs) = resolution else {
        return Err(DeclineReason::ResolutionNotFingerprints);
    };
    Ok(pairs
        .iter()
        .map(|(fp, labels)| Member {
            fingerprint: *fp,
            metric_name: metric_name.to_string(),
            labels: labels.clone(),
        })
        .collect())
}

/// Issue #579 part 2: a multi-name selector's members, from the name-keyed
/// fan-out's groups.
pub fn members_of_groups(groups: &[super::labels::MetricSeriesGroup]) -> Vec<Member> {
    groups
        .iter()
        .flat_map(|g| {
            g.series.iter().map(|(fp, labels)| Member {
                fingerprint: *fp,
                metric_name: g.metric_name.clone(),
                labels: labels.clone(),
            })
        })
        .collect()
}

/// Issue #579: every `Aggregate` node the database can answer.
pub fn pushed_nodes(plan: &QueryPlan, params: &PlanParams, cfg: &MetricsConfig) -> Vec<PushShape> {
    node_verdicts(plan, params, cfg)
        .into_iter()
        .filter_map(|(_, v)| v.ok())
        .collect()
}

/// Issue #579: assign every resolved fingerprint of a shape-A node to its
/// group. No threshold: one row per group per step is fewer rows than the
/// raw samples for every grouping.
pub fn decide_range(shape: &RangeShape, members: &[Member]) -> Result<RangePush, DeclineReason> {
    let (fingerprints, gids, groups) = assign_groups(members, shape.grouping.as_ref());
    Ok(RangePush {
        op: shape.op,
        func: shape.func,
        selector: shape.selector,
        fingerprints,
        gids,
        groups,
        grid: shape.grid,
        range_ms: shape.range_ms,
    })
}

/// Issue #579: splits a shape-A node's grid by time so no statement
/// computes more than `cap` series-steps. Each entry is the first grid
/// index of the chunk and the chunk's own grid.
pub fn time_chunks(grid: Grid, series: usize, cap: usize) -> Vec<(u32, Grid)> {
    let series = series.max(1);
    if series.saturating_mul(grid.points as usize) <= cap {
        return vec![(0, grid)];
    }
    // At least one step a statement, whatever the series count.
    let per = u32::try_from((cap / series).max(1)).unwrap_or(u32::MAX);
    let mut out = Vec::new();
    let mut g0 = 0u32;
    while g0 < grid.points {
        let points = per.min(grid.points - g0);
        out.push((
            g0,
            Grid {
                start_ms: grid.start_ms + i64::from(g0) * grid.step_ms,
                points,
                ..grid
            },
        ));
        g0 += points;
    }
    out
}

/// Issue #579: a shape-A node's rows, one `Vec` per time chunk with that
/// chunk's first grid index, read into the node's answer.
pub fn range_node(
    push: &RangePush,
    chunks: Vec<(u32, Vec<super::grouped_rows::RangeAggRow>)>,
    cancel: &CancelToken,
) -> Result<RangeOutcome, PromqlError> {
    let points = push.grid.points as usize;
    let mut cells: Vec<Vec<(u32, f64)>> = vec![Vec::new(); points];
    let mut histograms = 0.0f64;
    for (g0, rows) in chunks {
        if cancel.is_cancelled() {
            return Err(PromqlError::Cancelled);
        }
        for row in rows {
            // Picked out by its id, never by its position: the final
            // `ORDER BY` binds only to the last `SELECT` of the union.
            if row.gid == HISTOGRAM_SENTINEL_GID {
                histograms += row.agg.unwrap_or(0.0);
                continue;
            }
            let Some(agg) = row.agg else {
                return Err(PromqlError::Unsupported {
                    construct: "a NULL aggregate in a pushed node's row".to_string(),
                });
            };
            let gi = g0 as usize + row.gi as usize;
            // Unreachable: every fingerprint read is in `fps`, so every
            // gid indexes `groups`, and `gi` is below the chunk's points.
            if gi >= points || row.gid as usize >= push.groups.len() {
                continue;
            }
            cells[gi].push((row.gid, agg));
        }
    }
    if histograms > 0.0 {
        return Ok(RangeOutcome::Histograms(histograms as u64));
    }
    // `aggregate_reduce`'s output order: by labels, then name.
    let mut rank: Vec<u32> = (0..push.groups.len() as u32).collect();
    rank.sort_by(|a, b| {
        let (la, na) = &push.groups[*a as usize];
        let (lb, nb) = &push.groups[*b as usize];
        (la, na).cmp(&(lb, nb))
    });
    let mut position = vec![0u32; push.groups.len()];
    for (i, gid) in rank.iter().enumerate() {
        position[*gid as usize] = i as u32;
    }
    let mut steps = Vec::with_capacity(points);
    for (gi, mut cell) in cells.into_iter().enumerate() {
        if gi % 1_024 == 0 && cancel.is_cancelled() {
            return Err(PromqlError::Cancelled);
        }
        cell.sort_by_key(|(gid, _)| position[*gid as usize]);
        let t_ms = push.grid.start_ms + gi as i64 * push.grid.step_ms;
        steps.push(
            cell.into_iter()
                .map(|(gid, v)| {
                    let (labels, metric_name) = &push.groups[gid as usize];
                    InstantSample {
                        labels: labels.clone(),
                        metric_name: metric_name.clone(),
                        // A range function drops the name, and the
                        // group's verdict is the OR of its members'.
                        drop_name: true,
                        t_ms,
                        v,
                        h: None,
                    }
                })
                .collect(),
        );
    }
    Ok(RangeOutcome::Node(PushedNode {
        start_ms: push.grid.start_ms,
        step_ms: push.grid.step_ms,
        steps,
        annotations: Annotations::new(),
    }))
}

/// Issue #579: a shape-B node's runs, folded into the node's answer.
pub fn instant_node(
    push: &GroupedPush,
    chunks: Vec<Vec<Run>>,
    cancel: &CancelToken,
) -> Result<PushedNode, PromqlError> {
    let mut annotations = Annotations::new();
    let value = fold(push, chunks, &mut annotations, cancel)?;
    let mut steps: Vec<Vec<InstantSample>> = vec![Vec::new(); push.grid.points as usize];
    let index = |t_ms: i64| ((t_ms - push.grid.start_ms) / push.grid.step_ms) as usize;
    // `fold`'s output is already in `(labels, name)` order, so each step
    // receives its samples in that order.
    match value {
        QueryValue::Vector(v) => {
            for s in v {
                steps[index(s.t_ms)].push(s);
            }
        }
        QueryValue::Matrix(m) => {
            for series in m {
                for p in series.points {
                    steps[index(p.t_ms)].push(InstantSample {
                        labels: series.labels.clone(),
                        metric_name: series.metric_name.clone(),
                        drop_name: series.drop_name,
                        t_ms: p.t_ms,
                        v: p.v,
                        h: None,
                    });
                }
            }
        }
        // `fold` answers a vector or a matrix and nothing else.
        _ => {}
    }
    Ok(PushedNode {
        start_ms: push.grid.start_ms,
        step_ms: push.grid.step_ms,
        steps,
        annotations,
    })
}

impl RangeAggOp {
    fn of(op: AggOp) -> Option<Self> {
        match op {
            AggOp::Sum => Some(RangeAggOp::Sum),
            AggOp::Avg => Some(RangeAggOp::Avg),
            AggOp::Count => Some(RangeAggOp::Count),
            AggOp::Min => Some(RangeAggOp::Min),
            AggOp::Max => Some(RangeAggOp::Max),
            _ => None,
        }
    }
}

impl PushedRangeFn {
    fn of(f: pulsus_promql::RangeFn) -> Option<Self> {
        match f {
            pulsus_promql::RangeFn::Rate => Some(PushedRangeFn::Rate),
            pulsus_promql::RangeFn::Irate => Some(PushedRangeFn::Irate),
            pulsus_promql::RangeFn::Increase => Some(PushedRangeFn::Increase),
            pulsus_promql::RangeFn::Delta => None,
        }
    }
}

/// The request's grid, or `None` when the arithmetic the DATABASE
/// evaluates would overflow.
///
/// **The guard is the database's own expression, not a proxy for it.**
/// Measured on 26.3: `toInt64(i64::MAX) + 300000` wraps to
/// `-9223372036854475809`, silently. A request the API admits —
/// `start = 999999999699999`, `end = 9223372036854475806`,
/// `step = 1000000000000000`, lookback `300000` — is 9,222 step
/// intervals, under the grid-resolution cap, and both of the obvious
/// proxies fit exactly at `i64::MAX`:
///
/// ```text
/// end + lookback + 1                     = 9223372036854775807   fits
/// (end - start) + step                   = 9223372036854775807   fits
/// (end + lookback) - start + step - 1    = 9223372036855075806   WRAPS
/// ```
///
/// So the check below is the numerator the statement evaluates, in the
/// same association order, plus the `leadInFrame` default.
fn grid_of(params: &PlanParams) -> Option<Grid> {
    if params.step_ms < 0 || params.end_ms < params.start_ms {
        return None;
    }
    let (step_ms, points) = if params.step_ms == 0 {
        (1i64, 1u32)
    } else {
        let intervals = params
            .end_ms
            .checked_sub(params.start_ms)?
            .checked_div(params.step_ms)?;
        (
            params.step_ms,
            u32::try_from(intervals.checked_add(1)?).ok()?,
        )
    };
    // `intDiv(cover_end - grid_start + grid_step - 1, grid_step)` at
    // `cover_end`'s maximum, which is `end + lookback`.
    params
        .end_ms
        .checked_add(params.lookback_ms)?
        .checked_sub(params.start_ms)?
        .checked_add(step_ms)?
        .checked_sub(1)?;
    // `leadInFrame(ts, 1, toInt64(<upper>) + lookback + 1)`.
    params
        .end_ms
        .checked_add(params.lookback_ms)?
        .checked_add(1)?;
    Some(Grid {
        start_ms: params.start_ms,
        step_ms,
        points,
        lookback_ms: params.lookback_ms,
    })
}

/// Issue #579: the request's grid for a shape-A node, or `None` when the
/// arithmetic its statement evaluates would overflow: [`grid_of`]'s guard
/// with the range in place of the lookback — a sample covers grid indices
/// up to `ts + range` — and the window's lower bound, `start - range`.
fn range_grid_of(params: &PlanParams, range_ms: i64) -> Option<Grid> {
    params.start_ms.checked_sub(range_ms)?;
    let grid = grid_of(&PlanParams {
        lookback_ms: range_ms,
        ..*params
    })?;
    Some(Grid {
        lookback_ms: params.lookback_ms,
        ..grid
    })
}

/// A gid must fit the statement's `UInt32` column.
///
/// There is **no runtime guard** on the cast, and there does not need to
/// be one while the configuration ceiling holds: a selector cannot
/// resolve more series than `PULSUS_CACHE_MAX_SERIES` admits, and config
/// load refuses a value above this ceiling. Raising the ceiling past the
/// cast boundary fails the BUILD rather than a test — when this fires,
/// compilation stops and nothing in the test list could report it.
const _: () = assert!(
    pulsus_config::CACHE_MAX_SERIES_CEILING <= u32::MAX as u64,
    "a gid no longer fits u32: the grouped read's gid column must widen"
);

/// Assign every resolved fingerprint to a group, then apply the
/// threshold.
///
/// Borrows the resolution, so the caller's existing `match resolution`
/// still consumes it on the declining path: no clone, no second resolve.
pub fn decide(
    shape: &GroupedShape,
    members: &[Member],
    grid: Grid,
) -> Result<GroupedPush, DeclineReason> {
    let (fingerprints, gids, groups) = assign_groups(members, shape.grouping.as_ref());

    // The threshold. An empty resolution passes it (0 >= 0) and yields
    // zero chunks, zero statements and an empty answer — the same answer
    // today's route gives, by the same route through `chunk_fingerprints`.
    if fingerprints.len() < 2 * groups.len() {
        return Err(DeclineReason::TooFewSeriesPerGroup);
    }

    Ok(GroupedPush {
        op: shape.op,
        selector: shape.selector,
        metric_name: shape.metric_name.clone(),
        fingerprints,
        gids,
        groups,
        grid,
        expr_pos: shape.expr_pos,
        instant: shape.instant,
    })
}

/// The group of every member, by [`pulsus_promql::group_key_of`] over the
/// member's own labels and name: members in `(metric_name, fingerprint)`
/// order, their gids in parallel, and each group's key indexed by gid.
///
/// That order is the one the unpushed route evaluates in — the multi-name
/// fetch sorts its series by it, and a concrete name's series share their
/// name, so for them it is ascending fingerprint (issue #579 part 2). The
/// statements fold by a member's position in this list, never by its
/// fingerprint: the run statement's chunks and NaN payload rule, and the
/// shape-A statement's compensated `sum` and `avg`.
#[allow(clippy::type_complexity)]
fn assign_groups(
    members: &[Member],
    grouping: Option<&Grouping>,
) -> (Vec<Fingerprint>, Vec<u32>, Vec<(Labels, Option<String>)>) {
    let mut ordered: Vec<&Member> = members.iter().collect();
    ordered.sort_unstable_by(|a, b| {
        (a.metric_name.as_str(), a.fingerprint).cmp(&(b.metric_name.as_str(), b.fingerprint))
    });

    let mut groups: Vec<(Labels, Option<String>)> = Vec::new();
    let mut seen: HashMap<(Labels, Option<String>), u32> = HashMap::new();
    let mut fingerprints = Vec::with_capacity(ordered.len());
    let mut gids = Vec::with_capacity(ordered.len());
    for m in ordered {
        let key = group_key_of(
            &super::exec::to_promql_labels(&m.labels),
            Some(&m.metric_name),
            grouping,
        );
        let gid = match seen.get(&key) {
            Some(g) => *g,
            None => {
                // The cast is safe under the ceiling asserted above.
                let g = groups.len() as u32;
                groups.push(key.clone());
                seen.insert(key, g);
                g
            }
        };
        fingerprints.push(m.fingerprint);
        gids.push(gid);
    }
    (fingerprints, gids, groups)
}

/// One output series before it is given a value type: the group's
/// identity, and its points in ascending grid order.
type FoldedSeries = (Labels, Option<String>, Vec<(i64, f64)>);

/// One group's answer at one grid index, folded across chunks.
#[derive(Debug, Clone, Copy)]
struct Cell {
    seen: bool,
    v: f64,
    flags: u8,
}

const EMPTY_CELL: Cell = Cell {
    seen: false,
    v: 0.0,
    flags: 0,
};

/// Folds every statement's partial runs into the query's answer.
///
/// # The flags fold is bitwise, never additive
///
/// `flags` bit 0 says the group had a float member. Two chunks of one
/// float-only group each return `1`; `1 | 1 == 1` keeps bit 0 set, where
/// `1 + 1 == 2` reads as "histogram only" and DROPS a group that must be
/// emitted. That is not a hypothetical about a rare shape — it is every
/// `min`/`max` group whose members span a chunk boundary.
///
/// # The extremum fold is the reference's replacement rule
///
/// Replace when the comparison says to **or the accumulator is NaN**,
/// which is `aggregate_reduce`'s `Min | Max` arm verbatim. A NaN member
/// never displaces a number; a NaN accumulator is displaced by anything,
/// including a later NaN — so an all-NaN group answers the payload of its
/// last member in fold order, and fold order is chunk order, which is
/// member order, `(metric_name, fingerprint)`.
///
/// # It is CPU-bound, so it is cancellable and belongs off the reactor
///
/// The rows arrive already reduced, but reducing them further is not
/// free: a run covers a stretch of grid points and is EXPANDED into them
/// one cell at a time, so the work is the sum of the run widths, not the
/// row count. The sample budget bounds the rows and the grid bounds a
/// single run, so the worst case is (rows the budget admits) × (points
/// the grid holds) cell writes — large enough that a client who walks
/// away must not leave a runtime worker finishing it.
///
/// So this mirrors the ordinary evaluator exactly (issue #93, hardened by
/// issue #101): the caller runs it on the blocking pool behind the shared
/// `EvalGate`, and passes a [`CancelToken`] that the awaiting request
/// frame sets when it is dropped. The checkpoint is **once per run** and
/// **once per group**, never per cell: a run's expansion and a group's
/// emit are each O(points), so one relaxed atomic load per iteration is
/// not measurable against them, while a per-cell check would sit in the
/// innermost loop.
///
/// Cancelling answers [`PromqlError::Cancelled`], which is the same
/// variant the ordinary path raises and reaches the client as `503`
/// `timeout` through the existing mapping — no new error and no new knob.
pub fn fold(
    push: &GroupedPush,
    chunks: Vec<Vec<Run>>,
    annotations: &mut Annotations,
    cancel: &CancelToken,
) -> Result<QueryValue, PromqlError> {
    let points = push.grid.points as usize;
    let mut cells: Vec<Option<Vec<Cell>>> = vec![None; push.groups.len()];
    let mut any_histogram_member = false;

    for chunk in &chunks {
        for run in chunk {
            // Once per run: the body below writes up to `points` cells.
            if cancel.is_cancelled() {
                return Err(PromqlError::Cancelled);
            }
            let Some(slot) = cells.get_mut(run.gid as usize) else {
                // Unreachable: every fingerprint the statement reads is in
                // the `transform` source array, so the `CAST(0,'UInt32')`
                // default is never taken and every `gid` indexes `groups`.
                continue;
            };
            let lo = run.gi_start as usize;
            if lo >= points {
                continue;
            }
            let hi = (run.gi_end as usize).min(points - 1);
            if hi < lo {
                continue;
            }
            any_histogram_member |= run.flags.is_some_and(|f| f & 2 != 0);
            let row = slot.get_or_insert_with(|| vec![EMPTY_CELL; points]);
            for cell in &mut row[lo..=hi] {
                fold_one(push.op, cell, run);
            }
        }
    }

    // The reference raises this per ignored member and its annotation set
    // de-duplicates, so once per query is the same set. `count`/`group`
    // raise none.
    if any_histogram_member && let Some(name) = push.op.ignored_in_aggregation_name() {
        annotations.info_at(
            push.expr_pos,
            pulsus_promql::annotations::messages::histogram_ignored_in_aggregation_info(name),
        );
    }

    let mut out: Vec<FoldedSeries> = Vec::new();
    for (gid, (labels, name)) in push.groups.iter().enumerate() {
        // Once per group: the body below walks `points` cells.
        if cancel.is_cancelled() {
            return Err(PromqlError::Cancelled);
        }
        let Some(row) = cells[gid].as_ref() else {
            continue;
        };
        let mut pts: Vec<(i64, f64)> = Vec::new();
        for (i, cell) in row.iter().enumerate() {
            if !emits(push.op, cell) {
                continue;
            }
            pts.push((push.grid.start_ms + i as i64 * push.grid.step_ms, cell.v));
        }
        if pts.is_empty() {
            continue;
        }
        out.push((labels.clone(), name.clone(), pts));
    }
    // The evaluator's own output order for both shapes: `(labels,
    // metric_name)`.
    out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));

    Ok(if push.instant {
        QueryValue::Vector(
            out.into_iter()
                .map(|(labels, metric_name, pts)| InstantSample {
                    labels,
                    metric_name,
                    // The members are a raw selector's samples, which keep
                    // their name, so the group's OR of member verdicts is
                    // false and the terminal cleanup is a no-op.
                    drop_name: false,
                    t_ms: pts[0].0,
                    v: pts[0].1,
                    h: None,
                })
                .collect(),
        )
    } else {
        QueryValue::Matrix(
            out.into_iter()
                .map(|(labels, metric_name, pts)| RangeSeries {
                    labels,
                    metric_name,
                    drop_name: false,
                    points: pts
                        .into_iter()
                        .map(|(t_ms, v)| Point { t_ms, v, h: None })
                        .collect(),
                })
                .collect(),
        )
    })
}

fn fold_one(op: GroupedOp, cell: &mut Cell, run: &Run) {
    match op {
        GroupedOp::Min | GroupedOp::Max => {
            cell.flags |= run.flags.unwrap_or(0);
            if !cell.seen {
                cell.seen = true;
                cell.v = run.agg;
                return;
            }
            let replace = match op {
                GroupedOp::Max => cell.v < run.agg,
                _ => cell.v > run.agg,
            };
            if replace || cell.v.is_nan() {
                cell.v = run.agg;
            }
        }
        GroupedOp::Count => {
            if !cell.seen {
                cell.seen = true;
                cell.v = 0.0;
            }
            cell.v += run.agg;
        }
        GroupedOp::Group => {
            cell.seen = true;
            cell.v = 1.0;
        }
    }
}

/// Whether a folded cell reaches the client.
///
/// `min`/`max` drop a group whose members at this grid index were all
/// histograms — bit 0 clear — which is the reference's not-seen rule.
/// `count`/`group` count a histogram member, so every covered cell is
/// emitted.
fn emits(op: GroupedOp, cell: &Cell) -> bool {
    match op {
        GroupedOp::Min | GroupedOp::Max => cell.seen && cell.flags & 1 != 0,
        GroupedOp::Count | GroupedOp::Group => cell.seen,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_model::FpLiteral;

    use pulsus_promql::{DEFAULT_LOOKBACK_MS, parse};

    fn cfg(grouped_push: bool) -> MetricsConfig {
        MetricsConfig {
            db: "d".to_string(),
            samples_table: "metric_samples".to_string(),
            hist_samples_table: "metric_hist_samples".to_string(),
            series_table: "metric_series".to_string(),
            labels_table: "metric_labels".to_string(),
            label_index_table: "metric_label_index".to_string(),
            label_values_table: "metric_label_values".to_string(),
            metadata_table: "metric_metadata".to_string(),
            experimental_functions: true,
            max_metric_fanout: 1_000,
            max_cache_scan: 200_000,
            cache_max_series: 50_000,
            max_info_series: 100_000,
            max_samples: 50_000_000,
            distributed: false,
            read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
            grouped_push,
        }
    }

    fn params(start_ms: i64, end_ms: i64, step_ms: i64) -> PlanParams {
        PlanParams {
            start_ms,
            end_ms,
            step_ms,
            lookback_ms: DEFAULT_LOOKBACK_MS,
            experimental_functions: true,
        }
    }

    fn range_params() -> PlanParams {
        params(1_782_907_200_000, 1_782_910_800_000, 15_000)
    }

    /// The plan's one shape-B node, or `None` when it has none or more
    /// than one.
    fn shape(q: &str, p: PlanParams, on: bool) -> Option<GroupedShape> {
        let plan = pulsus_promql::plan(&parse(q).expect("parse"), p).expect("plan");
        let mut instant: Vec<GroupedShape> = pushed_nodes(&plan, &p, &cfg(on))
            .into_iter()
            .filter_map(|n| match n.kind {
                PushKind::Instant(g) => Some(g),
                PushKind::Range(_) => None,
            })
            .collect();
        (instant.len() == 1).then(|| instant.remove(0))
    }

    #[test]
    fn the_four_aggregations_over_a_plain_instant_selector_are_eligible() {
        for (q, op) in [
            ("max by (status) (m)", GroupedOp::Max),
            ("min without (instance) (m)", GroupedOp::Min),
            ("count(m)", GroupedOp::Count),
            ("group by (status) (m)", GroupedOp::Group),
        ] {
            let s = shape(q, range_params(), true).unwrap_or_else(|| panic!("{q} declined"));
            assert_eq!(s.op, op, "{q}");
            assert_eq!(s.metric_name.as_deref(), Some("m"), "{q}");
        }
    }

    /// The flag is read in `node_verdicts` and nowhere else, so turning it off
    /// makes every eligible query take today's route.
    #[test]
    fn the_flag_off_declines_every_query() {
        for q in [
            "max by (status) (m)",
            "min without (instance) (m)",
            "count(m)",
            "group by (status) (m)",
        ] {
            assert!(shape(q, range_params(), false).is_none(), "{q}");
        }
    }

    #[test]
    fn everything_out_of_scope_declines() {
        for q in [
            // not one of the four
            "sum by (status) (m)",
            "avg by (status) (m)",
            "stddev by (status) (m)",
            "stdvar by (status) (m)",
            "topk(3, m)",
            "quantile(0.5, m)",
            "count_values(\"v\", m)",
            // the child is not the selector
            "max by (status) (rate(m[5m]))",
            "max by (status) (max_over_time(m[5m]))",
            "max by (status) (m * 2)",
            "max by (status) (abs(m))",
            // the selector is not plain
            "max by (status) (m offset 5m)",
            "max by (status) (m @ 1782907200)",
            "max_over_time(max by (status) (m)[5m:1m])",
            // a concrete name with another `__name__` matcher (issue #579
            // part 2 pushes a selector with no concrete name)
            "max by (status) ({__name__=\"m\", __name__!~\"x\"})",
            // not an aggregate at all
            "m",
            "max_over_time(m[5m])",
        ] {
            assert!(shape(q, range_params(), true).is_none(), "{q} was eligible");
        }
    }

    /// An instant query is eligible and answers a one-point grid.
    #[test]
    fn an_instant_query_is_a_one_point_grid() {
        let s = shape("max by (status) (m)", params(1_000, 1_000, 0), true).expect("eligible");
        assert!(s.instant);
        assert_eq!(
            s.grid,
            Grid {
                start_ms: 1_000,
                step_ms: 1,
                points: 1,
                lookback_ms: DEFAULT_LOOKBACK_MS,
            }
        );
    }

    #[test]
    fn a_range_query_grid_counts_its_step_boundaries() {
        let s = shape("max by (status) (m)", range_params(), true).expect("eligible");
        assert!(!s.instant);
        assert_eq!(s.grid.points, 241);
        assert_eq!(s.grid.step_ms, 15_000);
    }

    /// Criterion 8: the overflow boundary, at a request the API admits.
    /// The first end value is the witness that both obvious proxy sums
    /// admit while the expression the database evaluates wraps.
    #[test]
    fn the_overflow_boundary_refuses_exactly_where_the_databases_numerator_wraps() {
        let start_ms = 999_999_999_699_999i64;
        let step_ms = 1_000_000_000_000_000i64;
        let end_ms = 9_223_372_036_854_475_806i64;
        let lookback_ms = 300_000i64;
        // The two proxies an earlier design guarded, both fitting exactly.
        assert_eq!(
            end_ms
                .checked_add(lookback_ms)
                .and_then(|x| x.checked_add(1)),
            Some(i64::MAX)
        );
        assert_eq!(
            end_ms
                .checked_sub(start_ms)
                .and_then(|x| x.checked_add(step_ms)),
            Some(i64::MAX)
        );
        let mut p = params(start_ms, end_ms, step_ms);
        p.lookback_ms = lookback_ms;
        assert_eq!(grid_of(&p), None, "the database's numerator wraps here");

        let mut q = params(start_ms, end_ms - step_ms, step_ms);
        q.lookback_ms = lookback_ms;
        let grid = grid_of(&q).expect("one step lower, the numerator fits");
        assert_eq!(grid.points, 9_222);
    }

    fn ls(pairs: &[(&str, &str)]) -> pulsus_model::LabelSet {
        pulsus_model::LabelSet::from_verbatim(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<_>>(),
        )
    }

    /// The members of one concrete name, `m`, as `members_of` builds them.
    fn resolution(pairs: &[(u128, &[(&str, &str)])]) -> Vec<Member> {
        members_of(
            &LabelledResolution::Series(
                pairs
                    .iter()
                    .map(|(fp, l)| (Fingerprint::from_raw(*fp), ls(l)))
                    .collect(),
            ),
            "m",
        )
        .expect("a fingerprint list")
    }

    fn shape_for(q: &str) -> GroupedShape {
        shape(q, range_params(), true).expect("eligible")
    }

    #[test]
    fn group_ids_follow_the_evaluators_own_group_key() {
        let s = shape_for("max by (status) (m)");
        let r = resolution(&[
            (3, &[("status", "500"), ("instance", "c")]),
            (1, &[("status", "200"), ("instance", "a")]),
            (4, &[("status", "500"), ("instance", "d")]),
            (2, &[("status", "200"), ("instance", "b")]),
        ]);
        let push = decide(&s, &r, s.grid).expect("pushed");
        // Ascending fingerprints, gids assigned in that order.
        assert_eq!(push.fingerprints, [1, 2, 3, 4].map(Fingerprint::from_raw));
        assert_eq!(push.gids, vec![0, 0, 1, 1]);
        assert_eq!(
            push.groups,
            vec![
                (
                    Labels::new([("status".to_string(), "200".to_string())]),
                    None
                ),
                (
                    Labels::new([("status".to_string(), "500".to_string())]),
                    None
                ),
            ]
        );
    }

    /// `without` and bare partition differently from `by`, and the
    /// difference reaches the statement only through `gids`.
    #[test]
    fn without_and_bare_partition_by_their_own_rule() {
        let r = resolution(&[
            (1, &[("status", "200"), ("instance", "a")]),
            (2, &[("status", "200"), ("instance", "a")]),
            (3, &[("status", "500"), ("instance", "a")]),
            (4, &[("status", "500"), ("instance", "a")]),
        ]);
        let bare = decide(&shape_for("count(m)"), &r, range_grid()).expect("pushed");
        assert_eq!(bare.gids, vec![0, 0, 0, 0]);
        assert_eq!(bare.groups.len(), 1);

        let without =
            decide(&shape_for("count without (status) (m)"), &r, range_grid()).expect("pushed");
        assert_eq!(without.gids, vec![0, 0, 0, 0]);

        let by = decide(&shape_for("count by (status) (m)"), &r, range_grid()).expect("pushed");
        assert_eq!(by.gids, vec![0, 0, 1, 1]);
    }

    /// `by (__name__)` reads the selector's concrete metric name into the
    /// group key's name channel, which is what the output's `__name__`
    /// comes from.
    #[test]
    fn by_dunder_name_carries_the_metric_name_into_the_group_key() {
        let s = shape_for("max by (__name__) (m)");
        let r = resolution(&[(1, &[("a", "x")]), (2, &[("a", "y")])]);
        let push = decide(&s, &r, s.grid).expect("pushed");
        assert_eq!(
            push.groups,
            vec![(Labels::default(), Some("m".to_string()))]
        );
    }

    fn range_grid() -> Grid {
        grid_of(&range_params()).expect("grid")
    }

    #[test]
    fn a_sql_fallback_resolution_declines() {
        let s = shape_for("max by (status) (m)");
        let r = LabelledResolution::SqlFallback {
            sql: "SELECT fingerprint FROM metric_series".to_string(),
            reason: crate::FallbackReason::ColdCache,
        };
        assert_eq!(
            members_of(&r, "m").and_then(|members| decide(&s, &members, s.grid)),
            Err(DeclineReason::ResolutionNotFingerprints)
        );
    }

    /// The threshold, at its boundary: four series in two groups is
    /// exactly `2 * groups` and is taken; three in two is not.
    #[test]
    fn the_threshold_is_series_at_least_twice_groups() {
        let s = shape_for("max by (status) (m)");
        let four = resolution(&[
            (1, &[("status", "200")]),
            (2, &[("status", "200")]),
            (3, &[("status", "500")]),
            (4, &[("status", "500")]),
        ]);
        assert!(decide(&s, &four, s.grid).is_ok());
        let three = resolution(&[
            (1, &[("status", "200")]),
            (2, &[("status", "200")]),
            (3, &[("status", "500")]),
        ]);
        assert_eq!(
            decide(&s, &three, s.grid),
            Err(DeclineReason::TooFewSeriesPerGroup)
        );
    }

    /// `max by (instance) (m)` over one series per instance is one group
    /// per series — the case the threshold exists to decline.
    #[test]
    fn one_group_per_series_declines() {
        let s = shape_for("max by (instance) (m)");
        let r = resolution(&[
            (1, &[("instance", "a")]),
            (2, &[("instance", "b")]),
            (3, &[("instance", "c")]),
        ]);
        assert_eq!(
            decide(&s, &r, s.grid),
            Err(DeclineReason::TooFewSeriesPerGroup)
        );
    }

    /// An empty resolution passes the threshold and yields an empty push:
    /// zero fingerprints, so zero chunks and zero statements.
    #[test]
    fn an_empty_resolution_pushes_nothing_and_declines_nothing() {
        let s = shape_for("max by (status) (m)");
        let push = decide(&s, &resolution(&[]), s.grid).expect("pushed");
        assert!(push.fingerprints.is_empty());
        assert!(push.groups.is_empty());
    }

    /// Criterion 5: **one statement per nonempty fingerprint chunk —
    /// strictly fewer than today's route at every nonzero chunk count,
    /// and zero against zero when the resolution is empty.**
    ///
    /// Structural, not measured: the pushed route sends `n` statements
    /// where today's sends `2n`, because today's route renders a float
    /// fetch AND a complementary histogram fetch per chunk. So "strictly
    /// fewer" holds exactly when `n > 0`, and an empty resolution yields
    /// zero chunks on both routes.
    ///
    /// Both counts come from the ONE chunker both routes use, so a
    /// change to the threshold moves them together.
    #[test]
    fn the_pushed_route_sends_one_statement_per_chunk_where_today_sends_two() {
        use crate::metrics::sample_sql::{CHUNK_THRESHOLD, chunk_fingerprints};
        let rows: [(usize, usize); 5] = [(0, 0), (1, 1), (500, 1), (501, 2), (1_200, 3)];
        for (series, chunks) in rows {
            let fps: Vec<FpLiteral> = (0..series as u128)
                .map(|v| Fingerprint::from_raw(v).sql_literal())
                .collect();
            let n = chunk_fingerprints(&fps, CHUNK_THRESHOLD).len();
            assert_eq!(n, chunks, "{series} fingerprints");
            // Today: a float statement and a histogram statement per chunk.
            let today = 2 * n;
            assert_eq!(
                today,
                2 * chunks,
                "{series} fingerprints: today's statement count"
            );
            if n == 0 {
                assert_eq!(n, today, "an empty resolution sends nothing either way");
            } else {
                assert!(n < today, "{series} fingerprints: {n} against {today}");
            }
        }
    }

    /// [`fold`] with a token that never fires — every test below is
    /// about the reduction, not about cancellation, and the one test
    /// that IS about cancellation calls `fold` directly with a live one.
    fn folded(
        push: &GroupedPush,
        chunks: Vec<Vec<Run>>,
        annotations: &mut Annotations,
    ) -> QueryValue {
        fold(push, chunks, annotations, &CancelToken::never()).expect("the token never fires")
    }

    /// The fold's cancellation checkpoint is REACHED **at each of its two
    /// positions independently** — and each case fails if only its own
    /// checkpoint is removed.
    ///
    /// The two checkpoints guard different loops, and a case that merely
    /// sets the flag and folds something ordinary cannot tell them apart:
    /// with the per-run check deleted the expansion finishes and the
    /// per-GROUP check answers the same `Cancelled`, so the test stays
    /// green while the expansion — the large half of the work, and the
    /// whole reason for the offload — has become uncancellable. Measured:
    /// that is exactly what happened to the first version of this test.
    ///
    /// So each case is shaped so the OTHER checkpoint cannot be reached:
    ///
    /// ```text
    ///   case            groups  runs   the other loop           removing
    ///                                                           its own check
    ///   per-run              0     1   emit walks 0 groups      -> Ok(empty)
    ///   per-group            1     0   no run to check          -> Ok(empty)
    /// ```
    ///
    /// **The zero-group and zero-run shapes are chosen to isolate a
    /// checkpoint, not because a query produces them.** A zero-run fold
    /// does ship — it is what a statement returning nothing folds to —
    /// while a zero-group push does not; both are legal inputs to this
    /// function, and what the case asserts is where the check sits.
    #[test]
    fn each_cancellation_checkpoint_is_reached_on_its_own() {
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let token = CancelToken::new(std::sync::Arc::clone(&flag));
        let cases: [(&str, usize, Vec<Vec<Run>>); 2] = [
            (
                "the per-run checkpoint",
                0,
                vec![vec![run(0, 0, 2, 1.0, Some(1))]],
            ),
            ("the per-group checkpoint", 1, Vec::new()),
        ];
        for (what, groups, chunks) in cases {
            let push = push_of(GroupedOp::Max, groups, 3, false);
            let mut annos = Annotations::new();
            match fold(&push, chunks.clone(), &mut annos, &token) {
                Err(PromqlError::Cancelled) => {}
                other => panic!("{what}: a live flag must stop the fold, got {other:?}"),
            }
            // The same input with the flag CLEAR answers normally, so the
            // case above stopped on the token and not on its shape.
            flag.store(false, std::sync::atomic::Ordering::Relaxed);
            let mut annos = Annotations::new();
            let v = fold(&push, chunks, &mut annos, &token)
                .unwrap_or_else(|e| panic!("{what}: a clear flag must answer, got {e:?}"));
            assert!(
                matrix_bits(v).is_empty(),
                "{what}: this shape answers an empty matrix when it is not cancelled"
            );
            flag.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// And the checkpoints do not fire when the token is clear, on an
    /// input that really does produce an answer — so the two isolating
    /// shapes above have not quietly become the only thing covered.
    #[test]
    fn a_clear_token_folds_the_whole_answer() {
        let push = push_of(GroupedOp::Max, 1, 3, false);
        let mut annos = Annotations::new();
        let v = fold(
            &push,
            vec![vec![run(0, 0, 2, 1.0, Some(1))]],
            &mut annos,
            &CancelToken::never(),
        )
        .expect("a token that never fires");
        assert_eq!(
            matrix_bits(v)[0].1.len(),
            3,
            "three points, one per grid index"
        );
    }

    // ---------------------------------------------------------- the fold

    fn push_of(op: GroupedOp, groups: usize, points: u32, instant: bool) -> GroupedPush {
        GroupedPush {
            op,
            selector: 0,
            metric_name: Some("m".to_string()),
            fingerprints: Vec::new(),
            gids: Vec::new(),
            groups: (0..groups)
                .map(|i| {
                    (
                        Labels::new([("g".to_string(), i.to_string())]),
                        None::<String>,
                    )
                })
                .collect(),
            grid: Grid {
                start_ms: 1_000,
                step_ms: 10,
                points,
                lookback_ms: DEFAULT_LOOKBACK_MS,
            },
            expr_pos: 7,
            instant,
        }
    }

    /// The one fixed histogram shape these tests need — a value, not a
    /// shape under test.
    fn one_histogram() -> pulsus_model::FloatHistogram {
        pulsus_model::FloatHistogram {
            counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 4.0,
            sum: 5.0,
            positive_spans: vec![pulsus_model::Span {
                offset: 0,
                length: 3,
            }],
            negative_spans: Vec::new(),
            positive_buckets: vec![1.0, 2.0, 1.0],
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
        }
    }

    fn run(gid: u32, gi_start: u32, gi_end: u32, agg: f64, flags: Option<u8>) -> Run {
        Run {
            gid,
            gi_start,
            gi_end,
            agg,
            flags,
        }
    }

    /// One matrix series' labels and its points, values as raw bit
    /// patterns so NaN compares equal to NaN.
    type MatrixBits = Vec<(Vec<(String, String)>, Vec<(i64, u64)>)>;

    fn matrix_bits(v: QueryValue) -> MatrixBits {
        let QueryValue::Matrix(m) = v else {
            panic!("expected a matrix, got {v:?}");
        };
        m.into_iter()
            .map(|s| {
                (
                    s.labels.0,
                    s.points
                        .into_iter()
                        .map(|p| (p.t_ms, p.v.to_bits()))
                        .collect(),
                )
            })
            .collect()
    }

    /// **The flags fold is bitwise.** One float-only group spanning two
    /// chunks returns `flags = 1` from each; `1 | 1` keeps bit 0 set and
    /// the group is emitted, where `1 + 1 = 2` reads as histogram-only
    /// and drops it.
    #[test]
    fn two_float_only_chunks_of_one_group_keep_the_group() {
        let push = push_of(GroupedOp::Max, 1, 1, false);
        let mut annos = Annotations::new();
        let v = folded(
            &push,
            vec![
                vec![run(0, 0, 0, 500.0, Some(1))],
                vec![run(0, 0, 0, 501.0, Some(1))],
            ],
            &mut annos,
        );
        assert_eq!(
            matrix_bits(v),
            vec![(
                vec![("g".to_string(), "0".to_string())],
                vec![(1_000, 501.0f64.to_bits())]
            )]
        );
    }

    /// The extremum fold is the reference's replacement rule: a NaN
    /// member never displaces a number, whichever order the chunks
    /// arrive in.
    #[test]
    fn a_nan_member_never_displaces_a_number() {
        for chunks in [
            vec![
                vec![run(0, 0, 0, f64::NAN, Some(1))],
                vec![run(0, 0, 0, 3.0, Some(1))],
            ],
            vec![
                vec![run(0, 0, 0, 3.0, Some(1))],
                vec![run(0, 0, 0, f64::NAN, Some(1))],
            ],
        ] {
            let push = push_of(GroupedOp::Max, 1, 1, false);
            let mut annos = Annotations::new();
            assert_eq!(
                matrix_bits(folded(&push, chunks, &mut annos))[0].1,
                vec![(1_000, 3.0f64.to_bits())]
            );
        }
    }

    /// An all-NaN group answers the payload its LAST member in fold order
    /// carried, which is the highest fingerprint — never a manufactured
    /// NaN.
    #[test]
    fn an_all_nan_group_answers_its_last_members_payload() {
        let first = f64::from_bits(0x7FF8_0000_0000_0001);
        let last = f64::from_bits(0x7FF8_0000_0000_0002);
        let push = push_of(GroupedOp::Max, 1, 1, false);
        let mut annos = Annotations::new();
        let v = folded(
            &push,
            vec![
                vec![run(0, 0, 0, first, Some(1))],
                vec![run(0, 0, 0, last, Some(1))],
            ],
            &mut annos,
        );
        assert_eq!(matrix_bits(v)[0].1, vec![(1_000, last.to_bits())]);
    }

    /// A group with no float member at a grid index is dropped under
    /// `min`/`max` — the reference's not-seen rule — and the
    /// ignored-histogram info is raised once.
    #[test]
    fn a_histogram_only_group_is_dropped_and_annotated_once() {
        let push = push_of(GroupedOp::Max, 2, 1, false);
        let mut annos = Annotations::new();
        let v = folded(
            &push,
            vec![vec![
                run(0, 0, 0, 42.0, Some(3)),
                run(1, 0, 0, 0.0, Some(2)),
            ]],
            &mut annos,
        );
        assert_eq!(
            matrix_bits(v),
            vec![(
                vec![("g".to_string(), "0".to_string())],
                vec![(1_000, 42.0f64.to_bits())]
            )]
        );
        let (warnings, infos) = annos.base_messages();
        assert!(warnings.is_empty());
        assert_eq!(
            infos,
            vec!["PromQL info: ignored histogram in max aggregation"]
        );
    }

    /// `count` sums its chunks' partial counts; `group` answers 1
    /// wherever any chunk covered the index; neither raises an
    /// annotation for a histogram member, because neither ignores one.
    #[test]
    fn count_sums_its_chunks_and_group_answers_one() {
        let push = push_of(GroupedOp::Count, 1, 1, false);
        let mut annos = Annotations::new();
        let v = folded(
            &push,
            vec![
                vec![run(0, 0, 0, 500.0, None)],
                vec![run(0, 0, 0, 1.0, None)],
            ],
            &mut annos,
        );
        assert_eq!(matrix_bits(v)[0].1, vec![(1_000, 501.0f64.to_bits())]);
        assert_eq!(annos.base_messages(), (Vec::new(), Vec::new()));

        let push = push_of(GroupedOp::Group, 1, 1, false);
        let mut annos = Annotations::new();
        let v = folded(&push, vec![vec![run(0, 0, 0, 1.0, None)]], &mut annos);
        assert_eq!(matrix_bits(v)[0].1, vec![(1_000, 1.0f64.to_bits())]);
    }

    /// A run covers every grid index between its ends, and a group absent
    /// from a stretch contributes no point there — the gap the unpushed
    /// route produces by the lookback expiring.
    #[test]
    fn a_run_expands_to_its_grid_indices_and_a_gap_stays_a_gap() {
        let push = push_of(GroupedOp::Max, 1, 5, false);
        let mut annos = Annotations::new();
        let v = folded(
            &push,
            vec![vec![run(0, 0, 1, 7.0, Some(1)), run(0, 3, 4, 9.0, Some(1))]],
            &mut annos,
        );
        assert_eq!(
            matrix_bits(v)[0].1,
            vec![
                (1_000, 7.0f64.to_bits()),
                (1_010, 7.0f64.to_bits()),
                (1_030, 9.0f64.to_bits()),
                (1_040, 9.0f64.to_bits()),
            ]
        );
    }

    /// An instant query's answer is a vector stamped at the grid's one
    /// point.
    #[test]
    fn an_instant_query_folds_to_a_vector() {
        let mut push = push_of(GroupedOp::Max, 1, 1, true);
        push.grid.step_ms = 1;
        let mut annos = Annotations::new();
        let QueryValue::Vector(v) =
            folded(&push, vec![vec![run(0, 0, 0, 5.0, Some(1))]], &mut annos)
        else {
            panic!("expected a vector");
        };
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].t_ms, 1_000);
        assert_eq!(v[0].v.to_bits(), 5.0f64.to_bits());
        assert!(!v[0].drop_name);
    }

    /// Our histogram-ignored annotation set is the one
    /// `pulsus_promql::eval::aggregation::aggregate` produces for the
    /// same shape — checked against that function rather than against a
    /// sentence in this file.
    #[test]
    fn the_annotation_set_matches_the_evaluators_own() {
        for (op, agg_op) in [
            (GroupedOp::Max, AggOp::Max),
            (GroupedOp::Min, AggOp::Min),
            (GroupedOp::Count, AggOp::Count),
            (GroupedOp::Group, AggOp::Group),
        ] {
            let vector = vec![
                InstantSample {
                    labels: Labels::new([("g".to_string(), "0".to_string())]),
                    metric_name: Some("m".to_string()),
                    drop_name: false,
                    t_ms: 1_000,
                    v: 42.0,
                    h: None,
                },
                InstantSample {
                    labels: Labels::new([("g".to_string(), "0".to_string())]),
                    metric_name: Some("m".to_string()),
                    drop_name: false,
                    t_ms: 1_000,
                    v: 0.0,
                    h: Some(Box::new(one_histogram())),
                },
            ];
            let mut reference = Annotations::new();
            pulsus_promql::eval::aggregation::aggregate(
                agg_op,
                &vector,
                None,
                None,
                pulsus_promql::eval::aggregation::AggPos {
                    expr_pos: 7,
                    param_pos: 0,
                    self_pos: 0,
                },
                &mut reference,
            )
            .expect("aggregate");

            let push = push_of(op, 1, 1, false);
            let mut ours = Annotations::new();
            // A group with a float member and a histogram member: flags
            // bit 0 AND bit 1 for the extremum templates, no flags column
            // at all for the counting ones.
            let flags = matches!(op, GroupedOp::Min | GroupedOp::Max).then_some(3);
            folded(&push, vec![vec![run(0, 0, 0, 42.0, flags)]], &mut ours);
            assert_eq!(ours.base_messages(), reference.base_messages(), "{op:?}");
        }
    }

    // ------------------------------------------------------ issue #579

    const ISSUE_QUERY: &str = "sum by (mode) (rate(node_cpu_seconds_total[5m])) / on() \
                               group_left count(count by (cpu)(node_cpu_seconds_total)) * 100";

    fn plan_of(q: &str, p: PlanParams) -> QueryPlan {
        pulsus_promql::plan(&parse(q).expect("parse"), p).expect("plan")
    }

    fn nodes(q: &str, p: PlanParams) -> Vec<PushShape> {
        pushed_nodes(&plan_of(q, p), &p, &cfg(true))
    }

    fn verdicts(q: &str, p: PlanParams) -> Vec<(usize, Result<PushShape, NodeDecline>)> {
        node_verdicts(&plan_of(q, p), &p, &cfg(true))
    }

    /// T5: the issue's query pushes two nodes — shape A at `sum`, shape B
    /// at the inner `count` — and the outer `count`, the division and the
    /// multiplication stay in the evaluator.
    #[test]
    fn the_issue_query_pushes_two_nodes() {
        let got = nodes(ISSUE_QUERY, range_params());
        assert_eq!(got.len(), 2, "{got:?}");
        assert_eq!(got[0].self_pos, 0, "sum is the first node in written order");
        let PushKind::Range(a) = &got[0].kind else {
            panic!("sum over rate is shape A: {got:?}");
        };
        assert_eq!(
            (a.op, a.func, a.selector, a.range_ms),
            (RangeAggOp::Sum, PushedRangeFn::Rate, 0, 300_000)
        );
        assert_eq!(a.metric_name.as_deref(), Some("node_cpu_seconds_total"));
        assert_eq!(a.grid, range_grid());
        let inner = ISSUE_QUERY.find("count by (cpu)").expect("inner count");
        assert_eq!(got[1].self_pos, inner);
        let PushKind::Instant(b) = &got[1].kind else {
            panic!("the inner count is shape B: {got:?}");
        };
        assert_eq!((b.op, b.selector), (GroupedOp::Count, 1));
    }

    /// Every one of the five aggregations over every one of the three
    /// functions, under every grouping form but `by (__name__)`, is shape A.
    #[test]
    fn every_covered_shape_is_a_range_node() {
        for (agg, op) in [
            ("sum", RangeAggOp::Sum),
            ("avg", RangeAggOp::Avg),
            ("count", RangeAggOp::Count),
            ("min", RangeAggOp::Min),
            ("max", RangeAggOp::Max),
        ] {
            for (f, func) in [
                ("rate", PushedRangeFn::Rate),
                ("irate", PushedRangeFn::Irate),
                ("increase", PushedRangeFn::Increase),
            ] {
                for grouping in ["by (mode) ", "by (cpu) ", "without (cpu) ", ""] {
                    let q = format!("{agg} {grouping}({f}(m[5m]))");
                    let got = nodes(&q, range_params());
                    assert_eq!(got.len(), 1, "{q}: {got:?}");
                    let PushKind::Range(r) = &got[0].kind else {
                        panic!("{q}: {got:?}");
                    };
                    assert_eq!((r.op, r.func), (op, func), "{q}");
                }
            }
        }
    }

    /// Shape B is pushed at any node, whatever else the plan holds.
    #[test]
    fn shape_b_is_pushed_at_any_node() {
        let got = nodes("max by (status) (m) + max by (status) (n)", range_params());
        assert_eq!(got.len(), 2, "{got:?}");
        assert!(got.iter().all(|n| matches!(
            n.kind,
            PushKind::Instant(GroupedShape {
                op: GroupedOp::Max,
                ..
            })
        )));
        assert_eq!((got[0].selector(), got[1].selector()), (0, 1));

        let got = nodes("count(count by (cpu) (m))", range_params());
        assert_eq!(got.len(), 1, "only the inner count: {got:?}");
        assert_eq!(got[0].self_pos, "count(".len());
    }

    /// A subquery evaluates its inner expression on its own grid, so no
    /// node inside one is pushed.
    #[test]
    fn nothing_inside_a_subquery_is_pushed() {
        for q in [
            "max_over_time(sum(rate(m[5m]))[10m:1m])",
            "max_over_time(max by (status) (m)[10m:1m])",
        ] {
            assert!(nodes(q, range_params()).is_empty(), "{q}");
        }
    }

    fn assert_declines(cases: &[&str], want: NodeDecline) {
        for q in cases {
            let p = range_params();
            assert!(nodes(q, p).is_empty(), "{q} was pushed");
            let v = verdicts(q, p);
            assert!(
                v.iter().any(|(_, r)| *r == Err(want)),
                "{q}: the verdicts {v:?} do not name {want:?}"
            );
        }
    }

    /// T5, F1: any other aggregation over a range function, and an
    /// aggregation neither shape answers over a plain selector.
    #[test]
    fn f1_other_aggregations_decline_and_name_it() {
        assert_declines(
            &[
                "stddev by (mode) (rate(m[5m]))",
                "stdvar by (mode) (rate(m[5m]))",
                "quantile by (mode) (0.5, rate(m[5m]))",
                "topk by (mode) (3, rate(m[5m]))",
                "bottomk by (mode) (3, rate(m[5m]))",
                "group by (mode) (rate(m[5m]))",
                "sum by (mode) (m)",
            ],
            NodeDecline::Aggregation,
        );
        assert!(nodes("count_values(\"v\", rate(m[5m]))", range_params()).is_empty());
    }

    /// T5, F2: any other range function.
    #[test]
    fn f2_other_range_functions_decline_and_name_it() {
        assert_declines(
            &[
                "sum by (mode) (delta(m[5m]))",
                "sum by (mode) (max_over_time(m[5m]))",
                "sum by (mode) (deriv(m[5m]))",
                "sum by (mode) (resets(m[5m]))",
                "sum by (mode) (quantile_over_time(0.5, m[5m]))",
            ],
            NodeDecline::RangeFunction,
        );
    }

    /// T5, F3: a selector that is not plain.
    #[test]
    fn f3_selectors_that_are_not_plain_decline_and_name_it() {
        assert_declines(
            &[
                "sum by (mode) (rate(m[5m] offset 5m))",
                "sum by (mode) (rate(m[5m] @ 1782907200))",
                "sum by (mode) (rate(m[5m:1m]))",
                "sum by (mode) (rate({__name__=\"m\", __name__!~\"x\"}[5m]))",
                "histogram_count(sum by (mode) (rate(m[5m])))",
                "max by (status) (m offset 5m)",
            ],
            NodeDecline::Selector,
        );
    }

    /// T5, F4: the name channel of a range function's output is not
    /// carried by the group id.
    #[test]
    fn f4_by_name_over_a_range_function_declines_and_names_it() {
        assert_declines(
            &[
                "sum by (__name__) (rate(m[5m]))",
                "max by (__name__, mode) (irate(m[5m]))",
            ],
            NodeDecline::NameGrouping,
        );
    }

    /// T5, F8: the database's grid arithmetic would wrap: a sample covers
    /// grid indices up to `ts + range`, and `end + range` passes
    /// `i64::MAX` here for a 200-year range but not for five minutes.
    #[test]
    fn f8_a_grid_that_would_overflow_declines_and_names_it() {
        let end_ms = i64::MAX - 5_000_000_000_000;
        let p = params(end_ms - 10_000_000_000_000, end_ms, 10_000_000_000);
        let q = "sum by (mode) (rate(m[200y]))";
        assert!(nodes(q, p).is_empty(), "{q} was pushed");
        assert!(
            verdicts(q, p)
                .iter()
                .any(|(_, r)| *r == Err(NodeDecline::GridOverflow)),
            "{q}: {:?}",
            verdicts(q, p)
        );
        // The same grid with a range the arithmetic holds is pushed.
        assert_eq!(nodes("sum by (mode) (rate(m[5m]))", p).len(), 1);
    }

    /// The flag gates both shapes, and with it off there is no verdict to
    /// report either.
    #[test]
    fn the_flag_off_pushes_no_node_of_either_shape() {
        let p = range_params();
        let plan = plan_of(ISSUE_QUERY, p);
        assert!(pushed_nodes(&plan, &p, &cfg(false)).is_empty());
        assert!(node_verdicts(&plan, &p, &cfg(false)).is_empty());
    }

    fn range_shape_for(q: &str) -> RangeShape {
        match nodes(q, range_params()).into_iter().next().map(|n| n.kind) {
            Some(PushKind::Range(r)) => r,
            other => panic!("{q} is not shape A: {other:?}"),
        }
    }

    /// T5, F5: a cold or degraded cache has no fingerprint list.
    #[test]
    fn f5_a_sql_fallback_resolution_declines_a_range_node() {
        let s = range_shape_for("sum by (mode) (rate(m[5m]))");
        let r = LabelledResolution::SqlFallback {
            sql: "SELECT fingerprint FROM metric_series".to_string(),
            reason: crate::FallbackReason::ColdCache,
        };
        assert_eq!(
            members_of(&r, "m").and_then(|members| decide_range(&s, &members)),
            Err(DeclineReason::ResolutionNotFingerprints)
        );
    }

    /// Shape A assigns groups as the evaluator keys them and applies no
    /// threshold: one group per series is still pushed.
    #[test]
    fn a_range_node_assigns_groups_and_has_no_threshold() {
        let s = range_shape_for("sum by (mode) (rate(m[5m]))");
        let r = resolution(&[
            (3, &[("mode", "user"), ("cpu", "1")]),
            (1, &[("mode", "idle"), ("cpu", "0")]),
            (2, &[("mode", "user"), ("cpu", "0")]),
        ]);
        let push = decide_range(&s, &r).expect("pushed");
        assert_eq!(push.fingerprints, [1, 2, 3].map(Fingerprint::from_raw));
        assert_eq!(push.gids, vec![0, 1, 1]);
        assert_eq!(
            push.groups,
            vec![
                (
                    Labels::new([("mode".to_string(), "idle".to_string())]),
                    None
                ),
                (
                    Labels::new([("mode".to_string(), "user".to_string())]),
                    None
                ),
            ]
        );
        assert_eq!(push.range_ms, 300_000);

        let s = range_shape_for("sum by (cpu, mode) (rate(m[5m]))");
        let push = decide_range(&s, &r).expect("one group per series is pushed");
        assert_eq!(push.groups.len(), 3);
    }

    /// Section 3.4: over the cap, the grid is split by time, never by
    /// series. 128 series at 5,761 steps under a cap of 262,144 is 2,048
    /// steps a chunk, three chunks.
    #[test]
    fn a_node_over_the_cap_is_split_by_time() {
        let grid = Grid {
            start_ms: 1_000,
            step_ms: 15_000,
            points: 5_761,
            lookback_ms: DEFAULT_LOOKBACK_MS,
        };
        let chunks = time_chunks(grid, 128, 262_144);
        let got: Vec<(u32, i64, u32)> = chunks
            .iter()
            .map(|(g0, g)| (*g0, g.start_ms, g.points))
            .collect();
        assert_eq!(
            got,
            vec![
                (0, 1_000, 2_048),
                (2_048, 1_000 + 2_048 * 15_000, 2_048),
                (4_096, 1_000 + 4_096 * 15_000, 1_665),
            ]
        );
        assert!(chunks.iter().all(|(_, g)| g.step_ms == 15_000));
        // Under the cap, one statement.
        assert_eq!(
            time_chunks(grid, 128, PUSHED_SERIES_STEPS_PER_STATEMENT),
            vec![(0, grid)]
        );
        // A node wider than the cap still gets one step a statement.
        assert_eq!(time_chunks(grid, 300_000, 262_144).len(), 5_761);
    }

    fn range_push(groups: &[&str], points: u32) -> RangePush {
        RangePush {
            op: RangeAggOp::Sum,
            func: PushedRangeFn::Rate,
            selector: 0,
            fingerprints: Vec::new(),
            gids: Vec::new(),
            groups: groups
                .iter()
                .map(|m| (Labels::new([("mode".to_string(), m.to_string())]), None))
                .collect(),
            grid: Grid {
                start_ms: 1_000,
                step_ms: 10,
                points,
                lookback_ms: DEFAULT_LOOKBACK_MS,
            },
            range_ms: 300_000,
        }
    }

    fn row(gid: u32, gi: u32, agg: f64) -> super::super::grouped_rows::RangeAggRow {
        super::super::grouped_rows::RangeAggRow {
            gid,
            gi,
            agg: Some(agg),
        }
    }

    /// Rows are placed by `(gid, gi)`, never by position; the sentinel is
    /// picked out by its id wherever it arrives; each step's vector is in
    /// label order, carries the step's time and `drop_name`; and a chunk's
    /// `gi` is offset by its first grid index.
    #[test]
    fn range_rows_are_placed_by_group_and_index() {
        // Group 0 is "user" and group 1 is "idle": the output order is the
        // labels', not the gids'.
        let push = range_push(&["user", "idle"], 4);
        let chunks = vec![
            (
                0,
                vec![
                    row(HISTOGRAM_SENTINEL_GID, 0, 0.0),
                    row(0, 1, 2.0),
                    row(1, 0, 3.0),
                    row(0, 0, 1.0),
                ],
            ),
            (2, vec![row(1, 1, 5.0), row(HISTOGRAM_SENTINEL_GID, 0, 0.0)]),
        ];
        let RangeOutcome::Node(node) =
            range_node(&push, chunks, &CancelToken::never()).expect("read")
        else {
            panic!("no histogram sample was counted");
        };
        assert_eq!((node.start_ms, node.step_ms), (1_000, 10));
        let got: Vec<Vec<(String, i64, u64, bool)>> = node
            .steps
            .iter()
            .map(|v| {
                v.iter()
                    .map(|s| {
                        (
                            s.labels.get("mode").unwrap_or("").to_string(),
                            s.t_ms,
                            s.v.to_bits(),
                            s.drop_name,
                        )
                    })
                    .collect()
            })
            .collect();
        assert_eq!(
            got,
            vec![
                vec![
                    ("idle".to_string(), 1_000, 3.0f64.to_bits(), true),
                    ("user".to_string(), 1_000, 1.0f64.to_bits(), true),
                ],
                vec![("user".to_string(), 1_010, 2.0f64.to_bits(), true)],
                vec![],
                vec![("idle".to_string(), 1_030, 5.0f64.to_bits(), true)],
            ]
        );
    }

    /// F6: any chunk's sentinel counting a histogram sample discards the
    /// node's answer.
    #[test]
    fn a_sentinel_counting_histograms_discards_the_answer() {
        let push = range_push(&["user"], 2);
        let chunks = vec![
            (0, vec![row(0, 0, 1.0), row(HISTOGRAM_SENTINEL_GID, 0, 0.0)]),
            (1, vec![row(HISTOGRAM_SENTINEL_GID, 0, 20.0)]),
        ];
        match range_node(&push, chunks, &CancelToken::never()).expect("read") {
            RangeOutcome::Histograms(n) => assert_eq!(n, 20),
            RangeOutcome::Node(n) => panic!("histograms were counted, got {n:?}"),
        }
    }

    /// A shape-B node's runs become one vector per grid index, with the
    /// fold's annotations carried on the node.
    #[test]
    fn instant_runs_become_one_vector_per_index() {
        let push = push_of(GroupedOp::Max, 2, 3, false);
        let node = instant_node(
            &push,
            vec![vec![run(1, 0, 2, 7.0, Some(1)), run(0, 1, 1, 9.0, Some(3))]],
            &CancelToken::never(),
        )
        .expect("fold");
        let got: Vec<Vec<(String, i64, u64)>> = node
            .steps
            .iter()
            .map(|v| {
                v.iter()
                    .map(|s| {
                        (
                            s.labels.get("g").unwrap_or("").to_string(),
                            s.t_ms,
                            s.v.to_bits(),
                        )
                    })
                    .collect()
            })
            .collect();
        assert_eq!(
            got,
            vec![
                vec![("1".to_string(), 1_000, 7.0f64.to_bits())],
                vec![
                    ("0".to_string(), 1_010, 9.0f64.to_bits()),
                    ("1".to_string(), 1_010, 7.0f64.to_bits()),
                ],
                vec![("1".to_string(), 1_020, 7.0f64.to_bits())],
            ]
        );
        assert!(node.steps.iter().flatten().all(|s| !s.drop_name));
        assert_eq!(
            node.annotations.base_messages().1,
            vec!["PromQL info: ignored histogram in max aggregation"]
        );
    }

    // ------------------------------------------------ issue #579 part 2

    /// T5: a selector with no concrete name is pushed by either shape; a
    /// concrete name with extra `__name__` matchers stays F3, and `by
    /// (__name__)` over a range function stays F4.
    #[test]
    fn multi_name_selectors_are_pushed() {
        let p = range_params();
        for q in [
            "count by (__name__) ({__name__=~\"a.+\"})",
            "count({job=\"x\"})",
        ] {
            let got = nodes(q, p);
            assert_eq!(got.len(), 1, "{q}: {got:?}");
            assert!(
                matches!(got[0].kind, PushKind::Instant(_)),
                "{q} is shape B: {got:?}"
            );
        }
        let q = "sum(rate({__name__=~\"a.+\"}[5m]))";
        let got = nodes(q, p);
        assert_eq!(got.len(), 1, "{q}: {got:?}");
        assert!(
            matches!(got[0].kind, PushKind::Range(_)),
            "{q} is shape A: {got:?}"
        );
        assert_declines(
            &["count({__name__=\"up\", __name__!~\"x\"})"],
            NodeDecline::Selector,
        );
        assert_declines(
            &["sum by (__name__) (rate({__name__=~\"a.+\"}[5m]))"],
            NodeDecline::NameGrouping,
        );
    }

    fn member(fp: u128, name: &str, pairs: &[(&str, &str)]) -> Member {
        Member {
            fingerprint: Fingerprint::from_raw(fp),
            metric_name: name.to_string(),
            labels: ls(pairs),
        }
    }

    /// T6: a node's members are ordered by `(metric_name, fingerprint)`,
    /// and `by (__name__)` keys each member by its own name.
    #[test]
    fn members_of_two_names_order_by_name_then_id() {
        let members = vec![
            member(1, "b", &[("k", "1")]),
            member(2, "a", &[("k", "1")]),
            member(3, "b", &[("k", "2")]),
            member(4, "a", &[("k", "2")]),
        ];
        let by_name = match nodes("count by (__name__) ({__name__=~\"a|b\"})", range_params())
            .into_iter()
            .next()
            .map(|n| n.kind)
        {
            Some(PushKind::Instant(g)) => g,
            other => panic!("not shape B: {other:?}"),
        };
        let push = decide(&by_name, &members, by_name.grid).expect("pushed");
        assert_eq!(push.fingerprints, [2, 4, 1, 3].map(Fingerprint::from_raw));
        assert_eq!(push.gids, vec![0, 0, 1, 1]);
        assert_eq!(
            push.groups,
            vec![
                (Labels::default(), Some("a".to_string())),
                (Labels::default(), Some("b".to_string())),
            ]
        );

        let s = match nodes("sum by (k) (rate({__name__=~\"a|b\"}[5m]))", range_params())
            .into_iter()
            .next()
            .map(|n| n.kind)
        {
            Some(PushKind::Range(r)) => r,
            other => panic!("not shape A: {other:?}"),
        };
        let push = decide_range(&s, &members).expect("pushed");
        assert_eq!(push.fingerprints, [2, 4, 1, 3].map(Fingerprint::from_raw));
        assert_eq!(push.gids, vec![0, 1, 0, 1]);
    }

    /// The fan-out's groups become members carrying their own name.
    #[test]
    fn the_fan_outs_groups_become_named_members() {
        let groups = vec![
            crate::metrics::labels::MetricSeriesGroup {
                metric_name: "a".to_string(),
                series: vec![(Fingerprint::from_raw(9), ls(&[("k", "1")]))],
            },
            crate::metrics::labels::MetricSeriesGroup {
                metric_name: "b".to_string(),
                series: vec![(Fingerprint::from_raw(1), ls(&[("k", "2")]))],
            },
        ];
        assert_eq!(
            members_of_groups(&groups),
            vec![member(9, "a", &[("k", "1")]), member(1, "b", &[("k", "2")])]
        );
    }
}
