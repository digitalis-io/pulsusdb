//! TraceQL metrics on `spans`, one statement per request (issue #596 part
//! 1, `docs/TraceQL/server-implementation.md` §3.2).
//!
//! [`plan_trace_metrics_routed`] decides which engine answers a metrics
//! query, and [`metrics_statement`] renders the one statement that answers
//! it from the span table: the occupied buckets, one row per series, the
//! trailing comparison and `topk`/`bottomk` per timestamp, and the
//! exemplars, each the longest span of its bucket and series, thinned to
//! the request's budget.

use pulsus_clickhouse::Row;
use pulsus_traceql::{FieldExpr, Query};
use serde::{Deserialize, Serialize};

use crate::traces::PlanError;
use crate::traces::metrics_plan::{
    MetricsCtx, MetricsParams, TraceMetricsPlan, plan_trace_metrics,
};

use super::attr_index::IndexedAttr;

/// The tables a metrics statement reads, and the attribute indexes its
/// filter may hint.
#[derive(Debug, Clone, Copy)]
pub struct StatementTables<'a> {
    pub spans: &'a str,
    pub resources: &'a str,
    pub indexed: &'a [IndexedAttr],
}

/// The range and instant forms of one request's statement.
#[derive(Debug, Clone)]
pub struct MetricsStatement {
    range_sql: String,
    instant_sql: String,
}

impl MetricsStatement {
    pub fn range_sql(&self) -> &str {
        &self.range_sql
    }

    pub fn instant_sql(&self) -> &str {
        &self.instant_sql
    }
}

/// Today's plan, and the statement that answers it from `spans` when the
/// query is one this route serves.
#[derive(Debug, Clone)]
pub struct RoutedMetricsPlan {
    plan: TraceMetricsPlan,
    statement: Option<MetricsStatement>,
}

impl RoutedMetricsPlan {
    pub fn plan(&self) -> &TraceMetricsPlan {
        &self.plan
    }

    pub fn statement(&self) -> Option<&MetricsStatement> {
        self.statement.as_ref()
    }
}

/// One series of the statement's answer: its label, its samples, its
/// exemplars (trace id, start in nanoseconds, value) and the fill flag.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct MetricStatementRow {
    pub g: String,
    pub points: Vec<(i64, f64)>,
    pub exemplars: Vec<([u8; 16], i64, f64)>,
    pub f: u8,
}

/// Plans one metrics request.
pub fn plan_trace_metrics_routed(
    query: &Query,
    params: &MetricsParams,
    ctx: &MetricsCtx<'_>,
    _tables: &StatementTables<'_>,
) -> Result<RoutedMetricsPlan, PlanError> {
    Ok(RoutedMetricsPlan {
        plan: plan_trace_metrics(query, params, ctx)?,
        statement: None,
    })
}

/// The statement for `plan`, whose filter body is `body`.
pub fn metrics_statement(
    _plan: &TraceMetricsPlan,
    _body: Option<&FieldExpr>,
    _max_series: u64,
    _tables: &StatementTables<'_>,
) -> Option<MetricsStatement> {
    None
}

/// Whether the engine fills a series' empty buckets with `0` (D14).
fn fills(_plan: &TraceMetricsPlan) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use pulsus_traceql::parse;

    use super::*;
    use crate::traces::filter::SpanFilterCtx;

    fn ctx<'a>() -> MetricsCtx<'a> {
        MetricsCtx {
            filter: SpanFilterCtx {
                spans_table: "trace_spans",
                attrs_table: "trace_attrs_idx",
            },
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            distributed: false,
            skip_unavailable_shards: false,
        }
    }

    const PARAMS: MetricsParams = MetricsParams {
        start_ns: 1_700_000_000_000_000_000,
        end_ns: 1_700_010_800_000_000_000,
        step_ms: 60_000,
        exemplars: None,
    };

    const TABLES: StatementTables<'static> = StatementTables {
        spans: "spans",
        resources: "resources",
        indexed: &[],
    };

    fn routed(q: &str, params: &MetricsParams) -> Result<RoutedMetricsPlan, PlanError> {
        plan_trace_metrics_routed(
            &parse(q).unwrap_or_else(|e| panic!("{q} must parse: {e:?}")),
            params,
            &ctx(),
            &TABLES,
        )
    }

    fn today(q: &str, params: &MetricsParams) -> Result<TraceMetricsPlan, PlanError> {
        plan_trace_metrics(
            &parse(q).unwrap_or_else(|e| panic!("{q} must parse: {e:?}")),
            params,
            &ctx(),
        )
    }

    /// D1: a field against a field, arithmetic and an absence check are
    /// served from the statement, where today's planner refuses them.
    #[test]
    fn a_field_against_a_field_is_served() {
        for q in [
            "{ span.a = span.b } | rate()",
            "{ duration * 2 > 700ms } | rate()",
            "{ .a = nil } | rate()",
        ] {
            let r = routed(q, &PARAMS).unwrap_or_else(|e| panic!("{q}: routed refused: {e}"));
            assert!(r.statement().is_some(), "{q}: no statement");
            assert!(today(q, &PARAMS).is_err(), "{q}: today's planner serves it");
        }
    }

    /// D1: the seven trace-level and nested-set fields, a child count and
    /// a `!` demand keep today's answer; a numbered comparison today's
    /// planner serves keeps today's plan, with no statement.
    #[test]
    fn the_seven_and_demands_keep_todays_answer() {
        for q in [
            "{ trace:duration > 1s } | rate()",
            "{ duration > trace:duration / 2 } | rate()",
            "{ nestedSetLeft > 0 } | rate()",
            "{ span:childCount > 1 } | rate()",
            "{ !span.a } | rate()",
        ] {
            let want = today(q, &PARAMS).expect_err("today's planner refuses it");
            match routed(q, &PARAMS) {
                Err(got) => assert_eq!(got, want, "{q}"),
                Ok(r) => panic!("{q}: routed accepted it, statement {:?}", r.statement()),
            }
        }
        let q = "{ nestedSetParent >= 1 } | rate()";
        let r = routed(q, &PARAMS).unwrap_or_else(|e| panic!("{q}: routed refused: {e}"));
        assert!(r.statement().is_none(), "{q}: a statement");
    }

    /// D1: `compare()` and a `by()` on another key stay on today's planner.
    #[test]
    fn compare_and_other_keys_stay_on_todays_planner() {
        let q = "{} | compare({ status = error })";
        let r = routed(q, &PARAMS).unwrap_or_else(|e| panic!("{q}: routed refused: {e}"));
        assert!(r.statement().is_none(), "{q}: a statement");
        let q = "{} | rate() by(span.http.route)";
        let want = today(q, &PARAMS).expect_err("today's planner refuses it");
        assert_eq!(routed(q, &PARAMS).err(), Some(want), "{q}");
    }

    /// D2: a window error is today's, word for word.
    #[test]
    fn a_window_error_is_todays() {
        let q = "{ span.a = span.b } | rate()";
        for params in [
            MetricsParams {
                step_ms: 0,
                ..PARAMS
            },
            MetricsParams {
                end_ns: PARAMS.start_ns,
                ..PARAMS
            },
            MetricsParams {
                end_ns: PARAMS.start_ns + 1_000_000 * 60_000_000_000,
                ..PARAMS
            },
        ] {
            let want = today("{} | rate()", &params).expect_err("today's planner refuses it");
            assert_eq!(routed(q, &params).err(), Some(want), "{params:?}");
        }
    }

    /// D14: the fill flag is decided once, from the comparison and the
    /// ranking.
    #[test]
    fn zero_passing_is_decided_once() {
        let flag = |q: &str| fills(&today(q, &PARAMS).unwrap_or_else(|e| panic!("{q}: {e}")));
        for (q, want) in [
            ("{} | count_over_time() > 0", false),
            ("{} | count_over_time() >= 0", true),
            ("{} | count_over_time() < 1", true),
            ("{} | count_over_time() <= 0", true),
            ("{} | count_over_time() != 0", false),
            ("{} | count_over_time() = 0", true),
            ("{} | count_over_time()", true),
            (
                "{} | count_over_time() by(resource.service.name) | topk(2)",
                false,
            ),
            (
                "{} | count_over_time() by(resource.service.name) | bottomk(2)",
                false,
            ),
            (
                "{} | count_over_time() by(resource.service.name) >= 0 | topk(2)",
                false,
            ),
            ("{} | avg_over_time(duration)", false),
            ("{} | avg_over_time(duration) >= 0", false),
            (
                "{} | avg_over_time(duration) by(resource.service.name)",
                false,
            ),
        ] {
            assert_eq!(flag(q), want, "{q}");
        }
    }
}
