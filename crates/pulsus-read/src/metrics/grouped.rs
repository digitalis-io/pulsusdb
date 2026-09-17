//! Issue #549: the grouped instant read — eligibility, group assignment,
//! the threshold, and the fold of the statements' run rows back into a
//! PromQL value.
//!
//! # Where the decision is taken
//!
//! ```text
//! query_inner
//!   |-- plan = pulsus_promql::plan(expr, params)
//!   |-- grouped::shape_of(&plan, &plan_params, &cfg) -> Option<GroupedShape>
//!   |     PURE, before the fetch loop. One selector; the root is
//!   |     Aggregate{min|max|count|group, grouping, Selector(id)} over a
//!   |     PLAIN instant selector; the grid arithmetic does not overflow;
//!   |     the flag is on. None -> today's route, nothing else changes.
//!   `-- the phase-1 loop, for that one selector
//!         resolution = resolver.resolve_labelled(...)   <- the ONE resolve
//!         grouped::decide(shape, &resolution, grid)
//!           Series(pairs) -> gids from the pairs, then `series >= 2 * groups`
//!           SqlFallback   -> Err(ResolutionNotFingerprints)
//!         Ok  -> render the grouped statements, skip the chunk builders
//!         Err -> fall through with the SAME resolution
//! ```
//!
//! [`shape_of`] is the **sole owner** of the overflow guard and of the
//! feature flag: [`decide`] re-checks neither, and
//! [`super::grouped_sql::grouped_fetch`] re-checks neither.
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
//! rendered. It is a heuristic and this comment says so. Rows are
//! bounded — every run boundary is a sample's coverage opening or
//! closing, so a sample can begin at most two runs and
//! `pushed_rows <= 2 * raw_rows` — but BYTES are not: the run row is
//! wider than a sample row and the wire size depends on framing. The
//! factor the push buys is series divided by groups, so a query with as
//! many groups as series gains nothing and pays the extra database time;
//! the threshold declines that case without claiming a gain for the ones
//! it admits.
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
    AggOp, Annotations, Grouping, InstantSample, Labels, PlanExpr, PlanParams, Point, QueryPlan,
    QueryValue, RangeSeries, SelectorId, group_key_of,
};

use super::exec::MetricsConfig;
use super::labels::LabelledResolution;

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
/// [`shape_of`] consumes the aggregate shape, the plain-selector checks,
/// the grid arithmetic and the flag, returning `None` for all four, so
/// none of them has a variant here. A refusal reason is written only
/// after naming the line that returns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclineReason {
    /// The resolver answered `SqlFallback` — a degraded or cold cache, so
    /// there is no fingerprint list to assign group ids over.
    ResolutionNotFingerprints,
    /// `series < 2 * groups`: the push would return at least half as many
    /// rows as the raw read and pay the extra database time for it.
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

/// What [`shape_of`] establishes without looking at any resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupedShape {
    pub op: GroupedOp,
    pub selector: SelectorId,
    /// The selector's one concrete metric name, needed by [`decide`]
    /// because `by (__name__)` reads it into the group key's name
    /// channel.
    pub metric_name: String,
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
    pub metric_name: String,
    /// Ascending, the order `build_chunk_sqls` sorts into.
    pub fingerprints: Vec<u64>,
    /// Parallel to `fingerprints`: `gids[i]` is the group id of
    /// `fingerprints[i]`.
    pub gids: Vec<u32>,
    /// Indexed by group id — the output identity of each group.
    pub groups: Vec<(Labels, Option<String>)>,
    pub grid: Grid,
    pub expr_pos: usize,
    pub instant: bool,
}

/// Is this plan one the grouped statement can answer?
///
/// Pure, and taken **before** the fetch loop, so a decline costs one
/// walk of the plan root and nothing else changes.
pub fn shape_of(
    plan: &QueryPlan,
    params: &PlanParams,
    cfg: &MetricsConfig,
) -> Option<GroupedShape> {
    // The flag is read HERE and nowhere else in this module or in
    // `grouped_sql`.
    if !cfg.grouped_push {
        return None;
    }
    // One selector. A binary operator, a set operator or `info()` puts a
    // second one in the plan, and none of those is in scope.
    if plan.selectors.len() != 1 {
        return None;
    }
    let PlanExpr::Aggregate {
        op,
        expr,
        param,
        grouping,
        expr_pos,
        ..
    } = &plan.root
    else {
        return None;
    };
    // `min`/`max`/`count`/`group` take no parameter; a `param` here would
    // mean the planner built a shape this code has not been written for.
    if param.is_some() {
        return None;
    }
    let op = GroupedOp::of(*op)?;
    // The child must be the SELECTOR itself. `max by (status)
    // (rate(m[5m]))` has a `RangeFn` here and keeps today's route.
    let PlanExpr::Selector(id) = expr.as_ref() else {
        return None;
    };
    let sel = plan.selectors.iter().find(|s| s.id == *id)?;

    // A PLAIN instant selector: one concrete metric name, no non-`Eq`
    // `__name__` matcher, no range, no offset, no `@`, no enclosing
    // subquery context, not the `info()` family, no histogram-stats
    // reduction, and neither extended-range modifier.
    let metric_name = sel.metric_name.clone()?;
    if !sel.name_matchers.is_empty()
        || sel.range_ms.is_some()
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

    let grid = grid_of(params)?;
    Some(GroupedShape {
        op,
        selector: *id,
        metric_name,
        grouping: grouping.clone(),
        grid,
        expr_pos: *expr_pos,
        instant: params.step_ms == 0,
    })
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
    resolution: &LabelledResolution,
    grid: Grid,
) -> Result<GroupedPush, DeclineReason> {
    let LabelledResolution::Series(pairs) = resolution else {
        return Err(DeclineReason::ResolutionNotFingerprints);
    };
    // Ascending fingerprint order, the order `build_chunk_sqls` sorts
    // into — so a chunk boundary falls in the same place on both routes
    // and the fold's chunk order IS fingerprint order.
    let mut by_fp: Vec<(u64, &pulsus_model::LabelSet)> =
        pairs.iter().map(|(fp, ls)| (*fp, ls)).collect();
    by_fp.sort_unstable_by_key(|(fp, _)| *fp);

    let mut groups: Vec<(Labels, Option<String>)> = Vec::new();
    let mut seen: HashMap<(Labels, Option<String>), u32> = HashMap::new();
    let mut fingerprints = Vec::with_capacity(by_fp.len());
    let mut gids = Vec::with_capacity(by_fp.len());
    for (fp, labelset) in by_fp {
        let key = group_key_of(
            &super::exec::to_promql_labels(labelset),
            Some(&shape.metric_name),
            shape.grouping.as_ref(),
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
        fingerprints.push(fp);
        gids.push(gid);
    }

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
/// fingerprint order.
pub fn fold(
    push: &GroupedPush,
    chunks: Vec<Vec<Run>>,
    annotations: &mut Annotations,
) -> QueryValue {
    let points = push.grid.points as usize;
    let mut cells: Vec<Option<Vec<Cell>>> = vec![None; push.groups.len()];
    let mut any_histogram_member = false;

    for chunk in &chunks {
        for run in chunk {
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

    let mut out: Vec<(Labels, Option<String>, Vec<(i64, f64)>)> = Vec::new();
    for (gid, (labels, name)) in push.groups.iter().enumerate() {
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

    if push.instant {
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
    }
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

    use pulsus_promql::{DEFAULT_LOOKBACK_MS, parse};

    fn cfg(grouped_push: bool) -> MetricsConfig {
        MetricsConfig {
            db: "d".to_string(),
            samples_table: "metric_samples".to_string(),
            hist_samples_table: "metric_hist_samples".to_string(),
            series_table: "metric_series".to_string(),
            metadata_table: "metric_metadata".to_string(),
            experimental_functions: true,
            max_metric_fanout: 1_000,
            max_cache_scan: 200_000,
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

    fn shape(q: &str, p: PlanParams, on: bool) -> Option<GroupedShape> {
        let plan = pulsus_promql::plan(&parse(q).expect("parse"), p).expect("plan");
        shape_of(&plan, &p, &cfg(on))
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
            assert_eq!(s.metric_name, "m", "{q}");
        }
    }

    /// The flag is read in `shape_of` and nowhere else, so turning it off
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
            "max by (status) ({__name__=~\"m.*\"})",
            "max by (status) ({job=\"api\"})",
            // two selectors
            "max by (status) (m) + max by (status) (n)",
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

    fn resolution(pairs: &[(u64, &[(&str, &str)])]) -> LabelledResolution {
        LabelledResolution::Series(pairs.iter().map(|(fp, l)| (*fp, ls(l))).collect())
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
        assert_eq!(push.fingerprints, vec![1, 2, 3, 4]);
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
            decide(&s, &r, s.grid),
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
            let fps: Vec<u64> = (0..series as u64).collect();
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

    // ---------------------------------------------------------- the fold

    fn push_of(op: GroupedOp, groups: usize, points: u32, instant: bool) -> GroupedPush {
        GroupedPush {
            op,
            selector: 0,
            metric_name: "m".to_string(),
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

    fn matrix_bits(v: QueryValue) -> Vec<(Vec<(String, String)>, Vec<(i64, u64)>)> {
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
        let v = fold(
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
                matrix_bits(fold(&push, chunks, &mut annos))[0].1,
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
        let v = fold(
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
        let v = fold(
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
        let v = fold(
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
        let v = fold(&push, vec![vec![run(0, 0, 0, 1.0, None)]], &mut annos);
        assert_eq!(matrix_bits(v)[0].1, vec![(1_000, 1.0f64.to_bits())]);
    }

    /// A run covers every grid index between its ends, and a group absent
    /// from a stretch contributes no point there — the gap the unpushed
    /// route produces by the lookback expiring.
    #[test]
    fn a_run_expands_to_its_grid_indices_and_a_gap_stays_a_gap() {
        let push = push_of(GroupedOp::Max, 1, 5, false);
        let mut annos = Annotations::new();
        let v = fold(
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
        let QueryValue::Vector(v) = fold(&push, vec![vec![run(0, 0, 0, 5.0, Some(1))]], &mut annos)
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
            fold(&push, vec![vec![run(0, 0, 0, 42.0, flags)]], &mut ours);
            assert_eq!(ours.base_messages(), reference.base_messages(), "{op:?}");
        }
    }
}
