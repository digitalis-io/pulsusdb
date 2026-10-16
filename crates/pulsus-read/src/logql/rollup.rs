//! The log rollup's read route (issue #614, part 2): which range queries
//! `log_metrics_<res>` answers exactly, and the raw plan a rollup plan falls
//! back to. The statement is `sql::metric_range_rollup`.

use pulsus_logql::{RangeAggOp, Stage};

use super::params::ValidatedDuration;
use super::plan::VectorAggSpec;
use super::sql::ScanLowerBound;

/// Stub: tests first (issue #614, part 2).
#[allow(dead_code)]
pub(in crate::logql) fn rollup_route(
    _op: RangeAggOp,
    _pipeline: &[Stage],
    _vector_aggs: &[VectorAggSpec],
    _lowered: bool,
    _res_ns: u64,
    _grid: (i64, Option<ValidatedDuration>, i64, ScanLowerBound),
) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::super::params::{Direction, PlanCtx, QueryParams, QuerySpec};
    use super::super::plan::{MetricPlan, Plan, RouteChoice, plan};

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
