//! TraceQL metrics on `spans`, one statement per request (issue #596 part
//! 1, `docs/TraceQL/server-implementation.md` §3.2).
//!
//! [`plan_trace_metrics_routed`] decides which engine answers a metrics
//! query, and [`metrics_statement`] renders the one statement that answers
//! it from the span table, in five levels:
//!
//! ```text
//!   S       the occupied buckets: per series and bucket, the value and
//!           the longest span (the exemplar candidate)
//!   SER     one row per series, its cells in an array; the series cap
//!   ROWS    one row per cell: the trailing comparison, then topk/bottomk
//!           per timestamp over every series, empty buckets as 0 (the
//!           ranked form fills them WITH FILL)
//!   answer  one row per series: its points, the exemplars the budget
//!           keeps, and the fill flag `f`
//! ```
//!
//! A series' empty buckets are `0` by definition, so the engine fills them
//! from the flag rather than the statement sending them: `f = 1` exactly
//! when the series is a count, rate, quantile or histogram, the query does
//! not rank, and `0` passes the comparison ([`fills`]).

use pulsus_clickhouse::Row;
use pulsus_traceql::{ComparisonOp, FieldExpr, PipelineStage, Query, SpansetExpr, SpansetFilter};
use serde::{Deserialize, Serialize};

use crate::traces::PlanError;
use crate::traces::metrics_plan::{
    MetricsCtx, MetricsParams, PlanKind, SeriesReduce, TraceMetricsPlan, plan_trace_metrics,
};
use crate::traces::metrics_sql::AggFn;
use crate::traces::window_sql::WindowSql;

use super::attr_index::{IndexedAttr, index_hints};
use super::predicate::{PredicateCtx, compile_span_predicate_in};

/// The `throwIf` message of the series cap. The engine turns a `Code: 395`
/// carrying it into the series cap's `422`.
pub const SERIES_CAP_MESSAGE: &str = "metrics series cap";

/// The exemplar of a cell that has none: start `0`.
const EX0: &str = "CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Int64)')";

/// An empty exemplar column.
const NO_EXEMPLARS: &str = "CAST([], 'Array(Tuple(FixedString(16), Int64, Float64))')";

/// A bucket's exemplar: its longest span, the larger span id on a tie.
const LONGEST: &str = "argMax((trace_id, start_ns, duration_ns), (duration_ns, span_id))";

/// The tables a metrics statement reads, and the attribute indexes its
/// filter may hint.
#[derive(Debug, Clone, Copy)]
pub struct StatementTables<'a> {
    pub spans: &'a str,
    pub resources: &'a str,
    pub traces: &'a str,
    pub indexed: &'a [IndexedAttr],
}

/// The unit a target's values are in (part 2's D7). A stub: part 2's
/// tests commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetUnit {
    Seconds,
    Raw,
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

    /// The label names of the query's `by()` keys. A stub: none.
    pub fn labels(&self) -> &[String] {
        &[]
    }

    /// The unit of the query's target. A stub: seconds.
    pub fn unit(&self) -> TargetUnit {
        TargetUnit::Seconds
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
    /// With a statement, today's plan of the query with its filter body
    /// removed: the window, step, axis, kind, labels, ranking, comparison
    /// and exemplar budget, its own SQL never issued. Without, today's
    /// plan of the query itself.
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
    pub g: Vec<(String, String)>,
    pub o: u64,
    pub points: Vec<(i64, f64)>,
    pub exemplars: Vec<([u8; 16], i64, f64)>,
    pub f: u8,
}

/// Plans one metrics request: the statement on `spans` when the query is
/// not `compare()`, its spanset is one `{ … }`, today's planner accepts it
/// with that filter's body removed, and [`metrics_statement`] serves the
/// body; otherwise today's planner on the query itself, its answer and its
/// refusals unchanged.
pub fn plan_trace_metrics_routed(
    query: &Query,
    params: &MetricsParams,
    ctx: &MetricsCtx<'_>,
    tables: &StatementTables<'_>,
) -> Result<RoutedMetricsPlan, PlanError> {
    let today = || {
        plan_trace_metrics(query, params, ctx).map(|plan| RoutedMetricsPlan {
            plan,
            statement: None,
        })
    };
    let SpansetExpr::Filter(filter) = &query.spanset else {
        return today();
    };
    if query
        .pipeline
        .iter()
        .any(|stage| matches!(stage, PipelineStage::Compare { .. }))
    {
        return today();
    }
    let shell = Query {
        spanset: SpansetExpr::Filter(SpansetFilter { body: None }),
        pipeline: query.pipeline.clone(),
        hints: query.hints.clone(),
    };
    let Ok(plan) = plan_trace_metrics(&shell, params, ctx) else {
        return today();
    };
    match metrics_statement(&plan, filter.body.as_ref(), ctx.max_series, tables) {
        Some(statement) => Ok(RoutedMetricsPlan {
            plan,
            statement: Some(statement),
        }),
        None => today(),
    }
}

/// The statement for `plan`, whose filter body is `body`; `None` when the
/// plan is `compare()`, or the body does not compile to a predicate over
/// the span row alone — a trace-level leaf or value, a child count, a
/// numbered leaf or a `!` demand.
pub fn metrics_statement(
    plan: &TraceMetricsPlan,
    body: Option<&FieldExpr>,
    max_series: u64,
    tables: &StatementTables<'_>,
) -> Option<MetricsStatement> {
    let shape = Shape::of(plan)?;
    let (rlo, rhi) = plan.range_window_ns();
    let (ilo, ihi) = plan.snapped_window_ns();
    let range_window = WindowSql::start_closed_end_open(rlo, rhi);
    let instant_window = WindowSql::start_closed_end_open(ilo, ihi);
    let range_predicate = predicate(body, range_window, tables)?;
    let instant_predicate = predicate(body, instant_window, tables)?;
    Some(MetricsStatement {
        range_sql: range_sql(
            plan,
            &shape,
            &from_clause(tables.spans, range_window, &range_predicate, &shape),
            max_series,
        ),
        instant_sql: instant_sql(
            plan,
            &shape,
            &from_clause(tables.spans, instant_window, &instant_predicate, &shape),
            max_series,
        ),
    })
}

/// Whether the engine fills a series' empty buckets with `0` (D14).
#[cfg(test)]
fn fills(plan: &TraceMetricsPlan) -> bool {
    Shape::of(plan).is_some_and(|s| s.fill)
}

/// The filter body compiled for `window`, behind its index hints; `true`
/// for `{}`.
fn predicate(
    body: Option<&FieldExpr>,
    window: WindowSql,
    tables: &StatementTables<'_>,
) -> Option<String> {
    let Some(body) = body else {
        return Some("true".to_string());
    };
    let ctx = PredicateCtx {
        window,
        spans_table: tables.spans,
        resources_table: tables.resources,
        indexed: tables.indexed,
    };
    let p = compile_span_predicate_in(body, &ctx).ok()?;
    if !p.demand_messages().is_empty()
        || !p.trace_leaves().is_empty()
        || !p.trace_values().is_empty()
        || p.reads_child_counts()
        || p.reads_numbering()
    {
        return None;
    }
    Some(
        p.with_hints(&index_hints(body, tables.indexed))
            .sql()
            .to_string(),
    )
}

/// What a plan's statement is made of.
struct Shape {
    kind: PlanKind,
    /// `by(resource.service.name)`.
    grouped: bool,
    /// More than one series can answer: grouped, quantile or histogram.
    many: bool,
    /// A count, rate, quantile or histogram: its empty buckets are `0`.
    dense: bool,
    /// The ranking, on a shape with more than one series.
    reduce: Option<SeriesReduce>,
    /// The trailing comparison, `(operator, threshold)` as text.
    comparison: Option<(&'static str, String)>,
    fill: bool,
}

impl Shape {
    fn of(plan: &TraceMetricsPlan) -> Option<Shape> {
        let kind = plan.kind();
        if kind == PlanKind::Compare {
            return None;
        }
        let grouped = plan.group_label().is_some();
        let many = grouped || matches!(kind, PlanKind::Quantile | PlanKind::Histogram);
        let dense = !matches!(kind, PlanKind::Agg(_));
        let reduce = plan.reduce().filter(|_| many);
        let (comparison, zero_passes) = match plan.result_filter() {
            None => (None, true),
            Some((op, threshold)) => {
                let (text, passes) = match op {
                    ComparisonOp::Eq => ("=", 0.0 == threshold),
                    ComparisonOp::Neq => ("!=", 0.0 != threshold),
                    ComparisonOp::Gt => (">", 0.0 > threshold),
                    ComparisonOp::Gte => (">=", 0.0 >= threshold),
                    ComparisonOp::Lt => ("<", 0.0 < threshold),
                    ComparisonOp::Lte => ("<=", 0.0 <= threshold),
                    // The parser refuses a regular expression after a
                    // metrics stage; today's engine keeps no sample.
                    ComparisonOp::Re | ComparisonOp::Nre => return None,
                };
                (Some((text, f64_text(threshold))), passes)
            }
        };
        Some(Shape {
            kind,
            grouped,
            many,
            dense,
            reduce,
            comparison,
            fill: dense && zero_passes && reduce.is_none(),
        })
    }

    /// The comparison applied to `x`.
    fn compare(&self, x: &str) -> Option<String> {
        self.comparison
            .as_ref()
            .map(|(op, threshold)| format!("{x} {op} {threshold}"))
    }

    /// `(G, O)`: the series label and its order.
    fn series(&self) -> (&'static str, &'static str) {
        if self.kind == PlanKind::Histogram {
            (
                "toString(toUInt64(roundToExp2(duration_ns - 1)) * 2)",
                "toUInt64(roundToExp2(duration_ns - 1)) * 2",
            )
        } else if self.grouped {
            ("toString(service)", "0")
        } else {
            ("''", "0")
        }
    }

    /// `V`: the value, a rate divided by `seconds`.
    fn value(&self, seconds: f64) -> String {
        match self.kind {
            PlanKind::Count { is_rate: true } => {
                format!("toFloat64(count()) / {}", f64_text(seconds))
            }
            PlanKind::Count { is_rate: false } | PlanKind::Histogram => {
                "toFloat64(count())".to_string()
            }
            PlanKind::Agg(AggFn::Sum) => "toFloat64(sum(duration_ns)) / 1000000000".to_string(),
            PlanKind::Agg(AggFn::Min) => "toFloat64(min(duration_ns)) / 1000000000".to_string(),
            PlanKind::Agg(AggFn::Max) => "toFloat64(max(duration_ns)) / 1000000000".to_string(),
            PlanKind::Agg(AggFn::Avg) => "avg(duration_ns) / 1000000000".to_string(),
            PlanKind::Quantile | PlanKind::Compare => {
                unreachable!("a quantile's value is its own level; compare() has no statement")
            }
        }
    }

    /// `DIR` and `n` of the ranking.
    fn ranking(&self) -> Option<(&'static str, u64)> {
        self.reduce.map(|r| match r {
            SeriesReduce::TopK(n) => ("DESC", n),
            SeriesReduce::BottomK(n) => ("ASC", n),
        })
    }
}

/// An `f64` as ClickHouse reads it back to the same value: Rust's shortest
/// round-trip text, a non-finite value by name.
fn f64_text(v: f64) -> String {
    if v.is_nan() {
        "nan".to_string()
    } else if v == f64::INFINITY {
        "inf".to_string()
    } else if v == f64::NEG_INFINITY {
        "-inf".to_string()
    } else {
        format!("{v}")
    }
}

/// The span rows of `window` the filter keeps: the window's three bounds,
/// the predicate, and a histogram's `duration_ns >= 2`.
fn from_clause(spans: &str, window: WindowSql, predicate: &str, shape: &Shape) -> String {
    let histogram = if shape.kind == PlanKind::Histogram {
        " AND duration_ns >= 2"
    } else {
        ""
    };
    format!(
        "FROM {spans}\nWHERE {}\n  AND {}\n  AND {}\n  AND ({predicate}){histogram}",
        window.span_time_clause(),
        window.span_bucket_clause(),
        window.span_day_clause()
    )
}

/// The requested quantile levels, as `quantilesTDigest`'s parameters.
fn levels(plan: &TraceMetricsPlan) -> String {
    plan.quantiles()
        .iter()
        .map(|q| f64_text(*q))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The range statement (section 3 of the design).
fn range_sql(plan: &TraceMetricsPlan, shape: &Shape, from: &str, max_series: u64) -> String {
    let axis = plan.range_axis();
    let step_ms = plan.step_ms();
    let first = axis.first_ms / step_ms;
    let step_ns = i128::from(step_ms) * 1_000_000;
    let budget = plan.exemplar_budget();
    let ex = if budget > 0 { LONGEST } else { EX0 };
    let bucket = format!("intDiv(start_ns - 1, {step_ns}) + 1 - {first}");
    let s = if shape.kind == PlanKind::Quantile {
        let indented = from.replace('\n', "\n      ");
        format!(
            "SELECT toString(qi) AS g, qi AS o, p, q AS v,\n       \
             if(qi = indexOf(arrayMap(x -> abs(x - x0.3 / 1000000000), qq), \
             arrayMin(arrayMap(x -> abs(x - x0.3 / 1000000000), qq))), x0, {EX0}) AS ex\n\
             FROM (SELECT {bucket} AS p,\n             \
             arrayMap(x -> if(isFinite(x), x / 1000000000, 0), \
             CAST(quantilesTDigest({levels})(duration_ns) AS Array(Float64))) AS qq,\n             \
             {ex} AS x0\n      {indented}\n      GROUP BY p)\n\
             ARRAY JOIN qq AS q, arrayEnumerate(qq) AS qi",
            levels = levels(plan)
        )
    } else {
        let (g, o) = shape.series();
        format!(
            "SELECT {g} AS g, {o} AS o, {bucket} AS p, {v} AS v, {ex} AS ex\n{from}\nGROUP BY {by}",
            v = shape.value(plan.step_seconds()),
            by = if shape.many { "g, o, p" } else { "p" }
        )
    };
    let cap = if shape.grouped {
        format!(", throwIf(count() OVER () > {max_series}, '{SERIES_CAP_MESSAGE}') AS over")
    } else {
        String::new()
    };
    let ser = if shape.many {
        format!(
            "SELECT g, any(o) AS o, groupArray((p, v, ex)) AS cells{cap}\nFROM ({s})\nGROUP BY g"
        )
    } else {
        format!("SELECT '' AS g, 0 AS o, groupArray((p, v, ex)) AS cells{cap}\nFROM ({s})")
    };
    let over: Vec<String> = if shape.grouped {
        vec!["over = 0".to_string()]
    } else {
        Vec::new()
    };
    let rows = match shape.ranking() {
        None => {
            let mut wheres = over;
            let v = match shape.compare("c.2") {
                Some(c) if shape.fill => format!("if({c}, c.2, nan)"),
                Some(c) => {
                    wheres.push(c);
                    "c.2".to_string()
                }
                None => "c.2".to_string(),
            };
            format!(
                "SELECT g, o, empty(cells) AS none, c.1 AS p, {v} AS v, if(isNaN(v), {EX0}, c.3) AS ex\n\
                 FROM ({ser})\nLEFT ARRAY JOIN cells AS c{}",
                where_clause(&wheres)
            )
        }
        Some((dir, n)) => {
            let mut cells = format!(
                "SELECT g, o, c.1 AS p, c.2 AS v, c.3 AS ex\nFROM ({ser})\nARRAY JOIN cells AS c{}",
                where_clause(&over)
            );
            if shape.dense {
                cells = format!(
                    "SELECT g, o, p, v, ex FROM ({cells})\nORDER BY o, g, p WITH FILL FROM 0 TO {}",
                    axis.points
                );
            }
            let compared = shape
                .compare("v")
                .map(|c| format!("\nWHERE {c}"))
                .unwrap_or_default();
            format!(
                "SELECT 0 AS none, g, o, p, v, ex FROM (SELECT g, o, p, v, ex, \
                 row_number() OVER (PARTITION BY p ORDER BY v {dir}, o, g) AS r\n\
                 FROM ({cells}){compared}) WHERE r <= {n}"
            )
        }
    };
    let (selected, exemplars) = if budget > 0 {
        let value = if shape.kind == PlanKind::Quantile {
            "ex.3 / 1000000000"
        } else {
            "v"
        };
        let keep = format!(
            "has AND (T <= {budget} OR (intDiv(x * {budget} + T - 1, T) < {budget} AND \
             intDiv(intDiv(x * {budget} + T - 1, T) * T, {budget}) = x))"
        );
        (
            format!(
                "SELECT none, g, o, p, v, ex, ex.2 != 0 AS has,\n       \
                 row_number() OVER (PARTITION BY has ORDER BY p, o, g) - 1 AS x,\n       \
                 countIf(has) OVER () AS T\nFROM ({rows})"
            ),
            format!("arraySort(e -> e.2, groupArrayIf((ex.1, ex.2, {value}), {keep}))"),
        )
    } else {
        (
            format!("SELECT none, g, o, p, v, ex FROM ({rows})"),
            NO_EXEMPLARS.to_string(),
        )
    };
    format!(
        "SELECT g, points, exemplars, {f} AS f\n\
         FROM (SELECT g, any(o) AS o, arraySort(groupArrayIf((({first} + p) * {step_ms}, v), NOT none)) AS points,\n             \
         {exemplars} AS exemplars\n      FROM ({selected})\n      GROUP BY g)\n{only}ORDER BY o, g",
        f = u8::from(shape.fill),
        only = if shape.fill {
            ""
        } else {
            "WHERE length(points) > 0\n"
        }
    )
}

/// The instant statement (section 3 of the design).
fn instant_sql(plan: &TraceMetricsPlan, shape: &Shape, from: &str, max_series: u64) -> String {
    let mut sql = if shape.kind == PlanKind::Quantile {
        format!(
            "SELECT toString(qi) AS g, qi AS o, q AS v FROM (SELECT arrayMap(x -> if(isFinite(x), \
             x / 1000000000, 0), CAST(quantilesTDigest({levels})(duration_ns) AS Array(Float64))) AS qq\n\
             {from}\nHAVING count() > 0)\nARRAY JOIN qq AS q, arrayEnumerate(qq) AS qi",
            levels = levels(plan)
        )
    } else {
        let (g, o) = shape.series();
        let tail = if shape.many {
            "\nGROUP BY g, o"
        } else if matches!(shape.kind, PlanKind::Agg(_)) {
            "\nHAVING count() > 0"
        } else {
            ""
        };
        format!(
            "SELECT {g} AS g, {o} AS o, {v} AS v\n{from}{tail}",
            v = shape.value(plan.window_seconds())
        )
    };
    if shape.grouped {
        sql = format!(
            "SELECT g, o, v FROM (SELECT g, o, v, throwIf(count() OVER () > {max_series}, \
             '{SERIES_CAP_MESSAGE}') AS over FROM ({sql})) WHERE over = 0"
        );
    }
    if let Some(c) = shape.compare("v") {
        sql = format!("SELECT g, o, v FROM ({sql})\nWHERE {c}");
    }
    if let Some((dir, n)) = shape.ranking() {
        sql = format!(
            "SELECT g, o, v FROM (SELECT g, o, v, row_number() OVER (ORDER BY v {dir}, o, g) AS r \
             FROM ({sql})) WHERE r <= {n}"
        );
    }
    format!(
        "SELECT g, [(toInt64({end}), v)] AS points, {NO_EXEMPLARS} AS exemplars, 0 AS f\n\
         FROM ({sql})\nORDER BY o, g",
        end = plan.snapped_end_ms()
    )
}

/// `\nWHERE a AND b`, or nothing.
fn where_clause(conditions: &[String]) -> String {
    if conditions.is_empty() {
        String::new()
    } else {
        format!("\nWHERE {}", conditions.join(" AND "))
    }
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
        traces: "traces",
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

    /// D1: `compare()` stays on today's planner, and so does an intrinsic
    /// target other than `duration` and `span:childCount` (part 2's D8).
    #[test]
    fn compare_and_intrinsic_targets_stay_on_todays_planner() {
        let q = "{} | compare({ status = error })";
        let r = routed(q, &PARAMS).unwrap_or_else(|e| panic!("{q}: routed refused: {e}"));
        assert!(r.statement().is_none(), "{q}: a statement");
        for q in [
            "{} | sum_over_time(nestedSetLeft)",
            "{} | avg_over_time(trace:duration)",
        ] {
            let want = today(q, &PARAMS).expect_err("today's planner refuses it");
            assert_eq!(routed(q, &PARAMS).err(), Some(want), "{q}");
        }
    }

    /// Part 2: any key, two keys, a keyed quantile, an attribute target
    /// and `span:childCount` as a key or a target are served by the
    /// statement; the child counts read the `ch` join.
    #[test]
    fn any_key_routes_to_the_statement() {
        for q in [
            "{} | rate() by(span.http.route)",
            "{} | rate() by(trace:rootService)",
            "{} | rate() by(nestedSetParent)",
            "{} | rate() by(resource.service.name, span.http.route)",
            "{} | quantile_over_time(duration, .9) by(span.http.route)",
            "{} | avg_over_time(span.http.request.body.size)",
            "{} | rate() by(span:childCount)",
            "{} | avg_over_time(span:childCount)",
        ] {
            let r = routed(q, &PARAMS);
            assert!(r.is_ok(), "{q}: routed refused: {r:?}");
            let r = r.expect("checked");
            let Some(st) = r.statement() else {
                panic!("{q}: no statement");
            };
            let children = q.contains("span:childCount");
            for sql in [st.range_sql(), st.instant_sql()] {
                assert_eq!(sql.contains(") AS ch USING"), children, "{q}: the ch join");
            }
        }
    }

    /// D3: a missing value of each of the twelve well-known keys is typed
    /// `'N'`, of any other key `'n'`.
    #[test]
    fn a_well_known_key_keeps_nil() {
        let well_known = [
            "resource.service.name",
            "resource.cluster",
            "resource.namespace",
            "resource.pod",
            "resource.container",
            "resource.k8s.cluster.name",
            "resource.k8s.namespace.name",
            "resource.k8s.pod.name",
            "resource.k8s.container.name",
            "span.http.status_code",
            "span.http.method",
            "span.http.url",
        ];
        let statement = |key: &str| {
            let q = format!("{{}} | count_over_time() by({key})");
            let r = routed(&q, &PARAMS);
            assert!(r.is_ok(), "{q}: routed refused: {r:?}");
            r.expect("checked")
                .statement()
                .unwrap_or_else(|| panic!("{q}: no statement"))
                .range_sql()
                .to_string()
        };
        for key in well_known {
            assert!(statement(key).contains("'N'"), "{key}: no 'N'");
        }
        for key in ["resource.foo", "span.foo"] {
            let sql = statement(key);
            assert!(sql.contains("'n'"), "{key}: no 'n'");
            assert!(!sql.contains("'N'"), "{key}: an 'N'");
        }
    }

    /// Section 3's `{traces}`, `{spans}` and `{resources}` are the caller's
    /// tables, never a name written into the SQL.
    #[test]
    fn the_tables_come_from_the_caller() {
        let tables = StatementTables {
            spans: "sp_t",
            resources: "rs_t",
            traces: "tr_t",
            indexed: &[],
        };
        let sql = |q: &str| {
            let r = plan_trace_metrics_routed(
                &parse(q).unwrap_or_else(|e| panic!("{q} must parse: {e:?}")),
                &PARAMS,
                &ctx(),
                &tables,
            );
            assert!(r.is_ok(), "{q}: routed refused: {r:?}");
            let r = r.expect("checked");
            let st = r.statement().unwrap_or_else(|| panic!("{q}: no statement"));
            [st.range_sql().to_string(), st.instant_sql().to_string()]
        };
        for q in [
            "{} | rate() by(trace:rootService)",
            "{} | rate() by(nestedSetParent)",
            "{} | rate() by(span:childCount)",
        ] {
            for text in sql(q) {
                assert!(text.contains("FROM tr_t"), "{q}: no FROM tr_t");
                assert!(text.contains("FROM sp_t"), "{q}: no FROM sp_t");
                for written in ["FROM traces", "FROM spans", "FROM resources"] {
                    assert!(!text.contains(written), "{q}: {written}");
                }
            }
        }
        for text in sql("{} | rate() by(resource.k8s.pod.name)") {
            assert!(text.contains("rs_t"), "no rs_t");
            for written in ["FROM traces", "FROM spans", "FROM resources"] {
                assert!(!text.contains(written), "{written}");
            }
        }
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
