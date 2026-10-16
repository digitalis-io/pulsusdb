//! The log rollup's read route (issue #614, part 2): which range queries
//! `log_metrics_<res>` answers exactly, and the raw plan a rollup plan falls
//! back to. The statement is `sql::metric_range_rollup`.

use std::borrow::Cow;
use std::collections::HashMap;

use pulsus_logql::{GroupingKind, RangeAggOp, Stage};
use pulsus_model::Fingerprint;

use super::labels::series_labels;
use super::params::ValidatedDuration;
use super::plan::{MetricPlan, RouteChoice, RoutingDecision, VectorAggSpec, parent_sum_grouping};
use super::rows::StreamMetaRow;
use super::sql::{MetricShape, MetricValue, ScanLowerBound};

/// Why a rollup plan reads the lines after all; each names its own routing
/// reason, so `X-Pulsus-Explain` reports the route that ran and its cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::logql) enum RawCause {
    /// A `sum by` name is not a stream label of every hydrated stream.
    Labels,
    /// The statement builder refused the grid.
    Refused,
    /// The statement failed for memory (server code 241).
    Memory,
}

impl RawCause {
    pub(in crate::logql) const fn reason(self) -> &'static str {
        match self {
            RawCause::Labels => {
                "raw: a sum by label is not a stream label of every selected stream (issue #614)"
            }
            RawCause::Refused => "raw: the rollup statement cannot render this grid (issue #614)",
            RawCause::Memory => "raw: the rollup statement exceeded its memory (issue #614)",
        }
    }
}

/// Whether the rollup answers a planned range leaf exactly, and the routing
/// reason when it does.
///
/// ```text
/// reducer      count_over_time, bytes_over_time, rate, bytes_rate
/// pipeline     no stage at all (the rollup has no line to filter)
/// parent       an innermost bare `sum`, or `sum by (L)` with no name in L
///              starting `__` or ending `_extracted`; [`rollup_or_raw`]
///              also requires every selected stream to carry every name in L
/// grid         grid start, step and range multiples of the resolution;
///              the scan bound exclusive
/// lowered      the clean bucketed range route, not instant, not empty
/// ```
///
/// Under such a `sum` the reference groups at the range step by the sum's
/// labels (`canInjectVectorGrouping`, `pkg/logql/syntax/ast.go:1612-1642 @
/// v3.7.4`), so no line's structured metadata reaches the answer — outside
/// one documented limit: a non-empty `__error__` in a line's metadata
/// fails the query on the raw route and is counted here (owner, issue #614).
pub(in crate::logql) fn rollup_route(
    op: RangeAggOp,
    pipeline: &[Stage],
    vector_aggs: &[VectorAggSpec],
    lowered: bool,
    res_ns: u64,
    (grid_start_ns, step_ns, range_ns, scan_lower): (
        i64,
        Option<ValidatedDuration>,
        i64,
        ScanLowerBound,
    ),
) -> Option<String> {
    if !lowered || !pipeline.is_empty() || scan_lower != ScanLowerBound::Exclusive {
        return None;
    }
    if !matches!(
        op,
        RangeAggOp::CountOverTime
            | RangeAggOp::BytesOverTime
            | RangeAggOp::Rate
            | RangeAggOp::BytesRate
    ) {
        return None;
    }
    match parent_sum_grouping(op, false, vector_aggs)? {
        None => {}
        Some(g) if g.kind == GroupingKind::By => {
            if g.labels
                .iter()
                .any(|l| l.starts_with("__") || l.ends_with("_extracted"))
            {
                return None;
            }
        }
        Some(_) => return None,
    }
    let res = i64::try_from(res_ns).ok().filter(|r| *r > 0)?;
    let step = step_ns?.get();
    let on_grid = |v: i64| v.rem_euclid(res) == 0;
    (on_grid(grid_start_ns) && on_grid(step) && on_grid(range_ns))
        .then(|| format!("rollup: sum over the {res} ns buckets (issue #614)"))
}

/// `mp` as the raw route plans it, when `mp` is a rollup plan: the samples
/// table, the raw column shape, the raw routing with `cause`'s reason. Any
/// other plan is itself.
pub(in crate::logql) fn raw_route_of<'a>(
    mp: &'a MetricPlan,
    samples: &str,
    cause: RawCause,
) -> Cow<'a, MetricPlan> {
    if !mp.rollup {
        return Cow::Borrowed(mp);
    }
    let is_bytes = matches!(mp.value, MetricValue::Shaped(MetricShape::RollupBytes));
    let mut raw = mp.clone();
    raw.table = samples.to_string();
    raw.value = MetricValue::Shaped(MetricShape::raw(is_bytes));
    raw.rollup = false;
    raw.routing = RoutingDecision {
        chosen: RouteChoice::Raw,
        reason: cause.reason().to_string(),
    };
    Cow::Owned(raw)
}

/// The plan that runs once the streams are hydrated: a rollup plan under
/// `sum by (L)` reads the lines unless every hydrated stream carries every
/// name in `L` as a stream label, because a line's structured metadata
/// supplies a name its stream lacks. Any other plan is itself.
pub(in crate::logql) fn rollup_or_raw<'a>(
    mp: &'a MetricPlan,
    meta: &HashMap<Fingerprint, StreamMetaRow>,
    samples: &str,
) -> Cow<'a, MetricPlan> {
    if !mp.rollup {
        return Cow::Borrowed(mp);
    }
    let names: &[String] = match mp.vector_aggs.last() {
        Some((_, Some(g), _)) => &g.labels,
        _ => &[],
    };
    let carried = meta.values().all(|m| {
        let labels = series_labels(m);
        names.iter().all(|n| labels.iter().any(|(k, _)| k == n))
    });
    if carried {
        Cow::Borrowed(mp)
    } else {
        raw_route_of(mp, samples, RawCause::Labels)
    }
}

#[cfg(test)]
mod tests {
    use super::super::params::{Direction, PlanCtx, QueryParams, QuerySpec};
    use super::super::plan::{Plan, plan};
    use super::*;

    fn route(query: &str, spec: QuerySpec) -> MetricPlan {
        let ctx = PlanCtx {
            tenant: Box::leak(Box::new(
                pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            )),
            db: "pulsus",
            streams_idx: "log_streams_idx",
            streams: "log_streams",
            samples: "log_samples",
            rollup_table: "log_metrics_5s",
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        };
        let params = QueryParams {
            spec,
            limit: 100,
            direction: Direction::Backward,
        };
        let expr = pulsus_logql::parse(query).expect("parse");
        match plan(&expr, &params, &ctx).expect("plan") {
            Plan::Metric(mp) => mp,
            Plan::Streams(_) | Plan::MetricBinary(_) => panic!("expected a Metric plan"),
        }
    }

    /// The rollup takes exactly the shapes its buckets answer, and every
    /// other shape keeps its route.
    #[test]
    fn the_rollup_route_takes_exactly_the_shapes_its_buckets_answer() {
        const S: i64 = 1_000_000_000;
        let range = |start_ns: i64, step_s: u64| QuerySpec::Range {
            start_ns,
            end_ns: start_ns + 3_600 * S,
            step_ns: step_s * 1_000_000_000,
        };
        let on = 1_800_000 * S;
        let mut wrong = Vec::new();
        for (query, spec, rollup) in [
            (r#"sum(count_over_time({a="b"}[1m]))"#, range(on, 15), true),
            (r#"sum(rate({a="b"}[5m]))"#, range(on, 60), true),
            (
                r#"sum by (x) (bytes_over_time({a="b"}[30s]))"#,
                range(on, 30),
                true,
            ),
            (
                r#"sum by (x, y) (bytes_rate({a="b"}[1m] offset 10s))"#,
                range(on, 15),
                true,
            ),
            (
                r#"topk(3, sum by (x) (count_over_time({a="b"}[1m])))"#,
                range(on, 15),
                true,
            ),
            (
                r#"sum(count_over_time({a="b"}[1m] offset 1s))"#,
                range(on, 15),
                false,
            ),
            (
                r#"sum(count_over_time({a="b"}[1m]))"#,
                range(on + 1, 15),
                false,
            ),
            (r#"sum(count_over_time({a="b"}[1m]))"#, range(on, 3), false),
            (
                r#"sum(count_over_time({a="b"}[62s]))"#,
                range(on, 15),
                false,
            ),
            (r#"count_over_time({a="b"}[1m])"#, range(on, 15), false),
            (r#"max(count_over_time({a="b"}[1m]))"#, range(on, 15), false),
            (
                r#"sum without (x) (count_over_time({a="b"}[1m]))"#,
                range(on, 15),
                false,
            ),
            (
                r#"sum by (__error__) (count_over_time({a="b"}[1m]))"#,
                range(on, 15),
                false,
            ),
            (
                r#"sum by (x_extracted) (count_over_time({a="b"}[1m]))"#,
                range(on, 15),
                false,
            ),
            (
                r#"sum(count_over_time({a="b"} |= "e" [1m]))"#,
                range(on, 15),
                false,
            ),
            (
                r#"sum(count_over_time({a="b"} | x="y" [1m]))"#,
                range(on, 15),
                false,
            ),
            (
                r#"sum(absent_over_time({a="b"}[1m]))"#,
                range(on, 15),
                false,
            ),
            (
                r#"sum(sum_over_time({a="b"} | unwrap x [1m]))"#,
                range(on, 15),
                false,
            ),
            (
                r#"sum(count_over_time({a="b"}[1m]))"#,
                QuerySpec::Instant { at_ns: on },
                false,
            ),
        ] {
            let mp = route(query, spec);
            let routed = (mp.routing.chosen == RouteChoice::Rollup, mp.rollup);
            if routed != (rollup, rollup) {
                wrong.push(format!("{query} {spec:?}: {:?}", mp.routing));
            }
            if rollup && mp.table != "log_metrics_5s" {
                wrong.push(format!("{query}: table {}", mp.table));
            }
        }
        assert_eq!(wrong, Vec::<String>::new());
    }
}
