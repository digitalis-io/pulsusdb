//! TraceQL metrics on `spans`, one statement per request (issue #596,
//! `docs/TraceQL/server-implementation.md` §3.2).
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
//! A series is `g`, its `by()` key values in order, each `(text, type)`
//! (part 2's D2), and `o`: a quantile's index, a histogram's bucket, else
//! `0`. A series' empty buckets are `0` by definition, so the engine fills
//! them from the flag rather than the statement sending them: `f = 1`
//! exactly when the series is a count, rate, quantile or histogram, the
//! query does not rank, and `0` passes the comparison.

use pulsus_clickhouse::Row;
use pulsus_traceql::{
    AttrScope, ComparisonOp, Field, FieldExpr, Intrinsic, MetricFn, PipelineStage, Query,
    SpansetExpr, SpansetFilter,
};
use serde::{Deserialize, Serialize};

use crate::traces::PlanError;
use crate::traces::metrics_plan::{
    MetricsCtx, MetricsParams, PlanKind, SeriesReduce, TraceMetricsPlan, plan_trace_metrics,
};
use crate::traces::metrics_sql::AggFn;
use crate::traces::window_sql::WindowSql;

use super::attr_index::{IndexedAttr, index_hints};
use super::numbering::numbering_sql;
use super::predicate::{PredicateCtx, attr_path, compile_span_predicate_in, resource_value_in};

/// The `throwIf` message of the series cap. The engine turns a `Code: 395`
/// carrying it into the series cap's `422`.
pub const SERIES_CAP_MESSAGE: &str = "metrics series cap";

/// The exemplar of a cell that has none: start `0`.
const EX0: &str = "CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Int64)')";

/// An empty exemplar column.
const NO_EXEMPLARS: &str = "CAST([], 'Array(Tuple(FixedString(16), Int64, Float64))')";

/// The key array of a series with no `by()` key.
const NO_KEYS: &str = "CAST([], 'Array(Tuple(String, String))')";

/// A bucket's exemplar: its longest span, the larger span id on a tie.
const LONGEST: &str = "argMax((trace_id, start_ns, duration_ns), (duration_ns, span_id))";

/// The keys the reference stores in their own columns
/// (`block_traceql.go:1015-1036` @ v3.0.2): a missing value of one is a
/// `nil` label, of any other key no label (part 2's D3).
const WELL_KNOWN_RESOURCE: [&str; 9] = [
    "service.name",
    "cluster",
    "namespace",
    "pod",
    "container",
    "k8s.cluster.name",
    "k8s.namespace.name",
    "k8s.pod.name",
    "k8s.container.name",
];
const WELL_KNOWN_SPAN: [&str; 3] = ["http.status_code", "http.method", "http.url"];

/// The tables a metrics statement reads, and the attribute indexes its
/// filter may hint.
#[derive(Debug, Clone, Copy)]
pub struct StatementTables<'a> {
    pub spans: &'a str,
    pub resources: &'a str,
    pub traces: &'a str,
    pub indexed: &'a [IndexedAttr],
}

/// The unit a target's values are in: `duration`'s nanoseconds, read as
/// seconds, or an attribute's or a child count's own number (part 2's D7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetUnit {
    Seconds,
    Raw,
}

/// The range and instant forms of one request's statement, the label
/// names of its `by()` keys, in order, and its target's unit.
#[derive(Debug, Clone)]
pub struct MetricsStatement {
    range_sql: String,
    instant_sql: String,
    labels: Vec<String>,
    unit: TargetUnit,
}

impl MetricsStatement {
    pub fn range_sql(&self) -> &str {
        &self.range_sql
    }

    pub fn instant_sql(&self) -> &str {
        &self.instant_sql
    }

    /// The label names of the query's `by()` keys, in `by()` order.
    pub fn labels(&self) -> &[String] {
        &self.labels
    }

    /// The unit of the query's target.
    pub fn unit(&self) -> TargetUnit {
        self.unit
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
    /// removed, its `by()` emptied and its target `duration`: the window,
    /// step, axis, kind, quantiles, ranking, comparison and exemplar
    /// budget, its own SQL never issued. Without, today's plan of the
    /// query itself.
    pub fn plan(&self) -> &TraceMetricsPlan {
        &self.plan
    }

    pub fn statement(&self) -> Option<&MetricsStatement> {
        self.statement.as_ref()
    }
}

/// One series of the statement's answer: its key values as `(text,
/// type)`, its order `o`, its samples, its exemplars (trace id, start in
/// nanoseconds, value) and the fill flag.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct MetricStatementRow {
    pub g: Vec<(String, String)>,
    pub o: u64,
    pub points: Vec<(i64, f64)>,
    pub exemplars: Vec<([u8; 16], i64, f64)>,
    pub f: u8,
}

/// What a query asks the statement for beyond today's plan: its `by()`
/// keys and its target.
struct Asked {
    keys: Vec<Field>,
    target: Target,
}

/// What the function aggregates.
#[derive(Debug, Clone)]
enum Target {
    /// `duration`, or no target at all (a count).
    Duration,
    /// An attribute's numeric value.
    Attribute(Field),
    /// `span:childCount`.
    Children,
}

impl Target {
    fn unit(&self) -> TargetUnit {
        match self {
            Target::Duration => TargetUnit::Seconds,
            Target::Attribute(_) | Target::Children => TargetUnit::Raw,
        }
    }
}

/// Plans one metrics request: the statement on `spans` when the query is
/// not `compare()`, its spanset is one `{ … }`, its target is `duration`,
/// an attribute or `span:childCount`, today's planner accepts it with the
/// filter's body removed, its `by()` emptied and its target `duration`,
/// and [`metrics_statement`] serves the body; otherwise today's planner on
/// the query itself, its answer and its refusals unchanged. A query the
/// validator refuses is refused with its text.
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
    // The shell empties `by()`, so the validator's arity rule is checked
    // here, before the shell can accept what the query may not ask.
    if let Err(e) = pulsus_traceql::validate(query) {
        return Err(PlanError::TypeMismatch(e.to_string()));
    }
    let Some((shell_pipeline, asked)) = shell_pipeline(&query.pipeline) else {
        return today();
    };
    let shell = Query {
        spanset: SpansetExpr::Filter(SpansetFilter { body: None }),
        pipeline: shell_pipeline,
        hints: query.hints.clone(),
    };
    let Ok(plan) = plan_trace_metrics(&shell, params, ctx) else {
        return today();
    };
    match statement_for(&plan, &asked, filter.body.as_ref(), ctx.max_series, tables) {
        Some(statement) => Ok(RoutedMetricsPlan {
            plan,
            statement: Some(statement),
        }),
        None => today(),
    }
}

/// The pipeline with its metric stage's `by()` emptied and its target
/// `duration`, and what the stage asked; `None` when the target is an
/// intrinsic other than `duration` and `span:childCount` (part 2's D8).
fn shell_pipeline(pipeline: &[PipelineStage]) -> Option<(Vec<PipelineStage>, Asked)> {
    let mut asked = Asked {
        keys: Vec::new(),
        target: Target::Duration,
    };
    let mut out = Vec::with_capacity(pipeline.len());
    for stage in pipeline {
        let PipelineStage::Metric(metric) = stage else {
            out.push(stage.clone());
            continue;
        };
        let mut metric = metric.clone();
        asked.keys = std::mem::take(&mut metric.by);
        let field = match &mut metric.func {
            MetricFn::Rate | MetricFn::CountOverTime => None,
            MetricFn::SumOverTime(f)
            | MetricFn::MinOverTime(f)
            | MetricFn::MaxOverTime(f)
            | MetricFn::AvgOverTime(f)
            | MetricFn::HistogramOverTime(f)
            | MetricFn::QuantileOverTime { field: f, .. } => Some(f),
        };
        if let Some(f) = field {
            asked.target = match &*f {
                Field::Intrinsic(Intrinsic::Duration) => Target::Duration,
                Field::Intrinsic(Intrinsic::ChildCount) => Target::Children,
                Field::Intrinsic(_) => return None,
                Field::Attribute { .. } => Target::Attribute(f.clone()),
            };
            *f = Field::Intrinsic(Intrinsic::Duration);
        }
        out.push(PipelineStage::Metric(metric));
    }
    Some((out, asked))
}

/// The statement for `plan`, whose filter body is `body` and which groups
/// by nothing and aggregates `duration`: part 1's entry point.
pub fn metrics_statement(
    plan: &TraceMetricsPlan,
    body: Option<&FieldExpr>,
    max_series: u64,
    tables: &StatementTables<'_>,
) -> Option<MetricsStatement> {
    let asked = Asked {
        keys: plan
            .group_label()
            .map(|_| {
                vec![Field::Attribute {
                    scope: AttrScope::Resource,
                    key: "service.name".to_string(),
                }]
            })
            .unwrap_or_default(),
        target: Target::Duration,
    };
    statement_for(plan, &asked, body, max_series, tables)
}

/// The statement for the shell `plan` and what the query asked; `None`
/// when the plan is `compare()`, or the body does not compile to a
/// predicate over the span row alone — a trace-level leaf or value, a child
/// count, a numbered leaf or a `!` demand.
fn statement_for(
    plan: &TraceMetricsPlan,
    asked: &Asked,
    body: Option<&FieldExpr>,
    max_series: u64,
    tables: &StatementTables<'_>,
) -> Option<MetricsStatement> {
    let shape = Shape::of(plan, !asked.keys.is_empty())?;
    let (rlo, rhi) = plan.range_window_ns();
    let (ilo, ihi) = plan.snapped_window_ns();
    let range_window = WindowSql::start_closed_end_open(rlo, rhi);
    let instant_window = WindowSql::start_closed_end_open(ilo, ihi);
    let range = Read::of(plan, asked, body, range_window, tables)?;
    let instant = Read::of(plan, asked, body, instant_window, tables)?;
    Some(MetricsStatement {
        range_sql: range_sql(plan, &shape, &range, max_series),
        instant_sql: instant_sql(plan, &shape, &instant, max_series),
        labels: asked.keys.iter().map(label_of_key).collect(),
        unit: asked.target.unit(),
    })
}

/// Whether the engine fills a series' empty buckets with `0` (D14).
#[cfg(test)]
fn fills(plan: &TraceMetricsPlan) -> bool {
    Shape::of(plan, plan.group_label().is_some()).is_some_and(|s| s.fill)
}

/// The filter body compiled for `window`, behind its index hints; `true`
/// for `{}`.
fn predicate(body: Option<&FieldExpr>, ctx: &PredicateCtx<'_>) -> Option<String> {
    let Some(body) = body else {
        return Some("true".to_string());
    };
    let p = compile_span_predicate_in(body, ctx).ok()?;
    if !p.demand_messages().is_empty()
        || !p.trace_leaves().is_empty()
        || !p.trace_values().is_empty()
        || p.reads_child_counts()
        || p.reads_numbering()
    {
        return None;
    }
    Some(
        p.with_hints(&index_hints(body, ctx.indexed))
            .sql()
            .to_string(),
    )
}

/// A key's label name: the reference's spelling (part 2's D3).
fn label_of_key(field: &Field) -> String {
    match field {
        Field::Attribute { scope, key } => format!("{scope}{key}"),
        Field::Intrinsic(i) => i.to_string(),
    }
}

/// Which per-trace or per-tree join a key or the target reads.
#[derive(Debug, Clone, Copy, Default)]
struct Joins {
    trace: bool,
    numbering: bool,
    children: bool,
}

/// What the statement reads over one window: the `FROM` clause, its joins
/// and the window's three bounds, the predicate and the target's
/// condition; the key array `g`; the target's `X` (a double) and `I` (an
/// integer).
struct Read {
    from: String,
    g: String,
    x: Option<String>,
    i: Option<String>,
}

impl Read {
    fn of(
        plan: &TraceMetricsPlan,
        asked: &Asked,
        body: Option<&FieldExpr>,
        window: WindowSql,
        tables: &StatementTables<'_>,
    ) -> Option<Read> {
        let ctx = PredicateCtx {
            window,
            spans_table: tables.spans,
            resources_table: tables.resources,
            indexed: tables.indexed,
        };
        let predicate = predicate(body, &ctx)?;
        let mut joins = Joins::default();
        let mut texts = Vec::with_capacity(asked.keys.len());
        for key in &asked.keys {
            texts.push(key_text(key, &ctx, &mut joins).ok()?);
        }
        let g = if texts.is_empty() {
            NO_KEYS.to_string()
        } else {
            format!("[{}]", texts.join(", "))
        };
        let (x, i) = match &asked.target {
            Target::Duration => (None, None),
            Target::Children => {
                joins.children = true;
                (
                    Some("toFloat64(ch.cc)".to_string()),
                    Some("toInt64(ch.cc)".to_string()),
                )
            }
            Target::Attribute(Field::Attribute { scope, key }) => {
                let a = value_read(*scope, key, &ctx).ok()?;
                (
                    Some(format!(
                        "if(dynamicType({a}) = 'Int64', toFloat64(dynamicElement({a}, 'Int64')), \
                         if(dynamicType({a}) = 'Float64', dynamicElement({a}, 'Float64'), NULL))"
                    )),
                    Some(format!("dynamicElement({a}, 'Int64')")),
                )
            }
            Target::Attribute(Field::Intrinsic(_)) => return None,
        };
        let condition = match (plan.kind(), &asked.target) {
            (PlanKind::Histogram, Target::Duration) => " AND duration_ns >= 2".to_string(),
            (PlanKind::Histogram, Target::Children) => " AND toInt64(ch.cc) >= 2".to_string(),
            (PlanKind::Histogram, _) => {
                let i = i.as_deref().unwrap_or_default();
                format!(" AND {i} IS NOT NULL AND {i} >= 2")
            }
            (PlanKind::Quantile, Target::Attribute(_)) => {
                format!(" AND {} IS NOT NULL", i.as_deref().unwrap_or_default())
            }
            (PlanKind::Agg(_), Target::Attribute(_)) => {
                format!(" AND {} IS NOT NULL", x.as_deref().unwrap_or_default())
            }
            _ => String::new(),
        };
        Some(Read {
            from: from_clause(tables, joins, window, &predicate, &condition),
            g,
            x,
            i,
        })
    }

    /// The target's double `X`, non-null where the statement reads it.
    fn x(&self) -> String {
        format!(
            "assumeNotNull({})",
            self.x.as_deref().unwrap_or("duration_ns")
        )
    }

    /// The target's integer `I`, non-null where the statement reads it.
    fn i(&self) -> String {
        match &self.i {
            Some(i) => format!("assumeNotNull({i})"),
            None => "duration_ns".to_string(),
        }
    }
}

/// `D(x)`: a `Dynamic` read as `(text, type)`, `missing` the type of an
/// absent value — `'N'` for a well-known key, `'n'` for any other.
fn dynamic_key(x: &str, missing: char) -> String {
    format!(
        "(toString({x}), multiIf(dynamicType({x}) = 'Int64', 'i', dynamicType({x}) = 'Float64', \
         'd', dynamicType({x}) = 'Bool', 'b', dynamicType({x}) = 'String', 's', \
         dynamicType({x}) = 'None', '{missing}', 'a'))"
    )
}

/// The first element of an event's or link's attribute `key` that holds
/// it.
fn first_element(root: &str, key: &str) -> String {
    format!(
        "arrayFirst(x -> dynamicType(x) != 'None', {})",
        attr_path(root, key)
    )
}

/// Whether an element of an event's or link's attribute `key` holds it.
fn any_element(root: &str, key: &str) -> String {
    format!(
        "arrayExists(x -> dynamicType(x) != 'None', {})",
        attr_path(root, key)
    )
}

/// Whether a missing value of `scope.key` is a `nil` label (D3).
fn well_known(scope: AttrScope, key: &str) -> bool {
    let resource = WELL_KNOWN_RESOURCE.contains(&key);
    let span = WELL_KNOWN_SPAN.contains(&key);
    match scope {
        AttrScope::Resource => resource,
        AttrScope::Span => span,
        AttrScope::Unscoped => resource || span,
        AttrScope::Instrumentation | AttrScope::Event | AttrScope::Link => false,
    }
}

/// An attribute target's `Dynamic` read in `scope` (section 3): the first
/// scope holding it, for an unscoped key.
fn value_read(scope: AttrScope, key: &str, ctx: &PredicateCtx<'_>) -> Result<String, PlanError> {
    Ok(match scope {
        AttrScope::Span => attr_path("attrs", key),
        AttrScope::Resource => resource_value_in(key, ctx)?,
        AttrScope::Instrumentation => attr_path("scope_attrs", key),
        AttrScope::Event => first_element("events.attrs", key),
        AttrScope::Link => first_element("links.attrs", key),
        AttrScope::Unscoped => {
            let span = attr_path("attrs", key);
            let resource = resource_value_in(key, ctx)?;
            format!(
                "multiIf(dynamicType({span}) != 'None', {span}, dynamicType({resource}) != 'None', \
                 {resource}, {}, {}, {}, {}, {})",
                any_element("events.attrs", key),
                first_element("events.attrs", key),
                any_element("links.attrs", key),
                first_element("links.attrs", key),
                attr_path("scope_attrs", key)
            )
        }
    })
}

/// A key's `(text, type)` (section 3's table), and the joins it reads.
fn key_text(field: &Field, ctx: &PredicateCtx<'_>, joins: &mut Joins) -> Result<String, PlanError> {
    let string = |column: &str| format!("(toString({column}), 's')");
    let hex = |column: &str| format!("(lower(hex({column})), 's')");
    Ok(match field {
        Field::Attribute { scope, key } => {
            let missing = if well_known(*scope, key) { 'N' } else { 'n' };
            match scope {
                AttrScope::Resource if key == "service.name" => {
                    "if(service_type = 'string', (toString(service), 's'), ('', 'N'))".to_string()
                }
                AttrScope::Span => dynamic_key(&attr_path("attrs", key), missing),
                AttrScope::Resource => dynamic_key(&resource_value_in(key, ctx)?, missing),
                AttrScope::Instrumentation => dynamic_key(&attr_path("scope_attrs", key), missing),
                AttrScope::Event => dynamic_key(&first_element("events.attrs", key), missing),
                AttrScope::Link => dynamic_key(&first_element("links.attrs", key), missing),
                AttrScope::Unscoped => {
                    let span = attr_path("attrs", key);
                    let resource_arm = if key == "service.name" {
                        "service_type = 'string', (toString(service), 's')".to_string()
                    } else {
                        let resource = resource_value_in(key, ctx)?;
                        format!(
                            "dynamicType({resource}) != 'None', {}",
                            dynamic_key(&resource, missing)
                        )
                    };
                    format!(
                        "multiIf(dynamicType({span}) != 'None', {}, {resource_arm}, {}, {}, {}, \
                         {}, {})",
                        dynamic_key(&span, missing),
                        any_element("events.attrs", key),
                        dynamic_key(&first_element("events.attrs", key), missing),
                        any_element("links.attrs", key),
                        dynamic_key(&first_element("links.attrs", key), missing),
                        dynamic_key(&attr_path("scope_attrs", key), missing)
                    )
                }
            }
        }
        Field::Intrinsic(intrinsic) => match intrinsic {
            Intrinsic::Name => string("name"),
            Intrinsic::StatusMessage => string("status_message"),
            Intrinsic::InstrumentationName => string("scope_name"),
            Intrinsic::InstrumentationVersion => string("scope_version"),
            Intrinsic::Status => "(['unset', 'ok', 'error'][status_code + 1], 's')".to_string(),
            Intrinsic::Kind => "(['unspecified', 'internal', 'server', 'client', 'producer', \
                                'consumer'][kind + 1], 's')"
                .to_string(),
            Intrinsic::Duration => "(toString(duration_ns), 'u')".to_string(),
            Intrinsic::SpanId => hex("span_id"),
            Intrinsic::ParentId => hex("parent_span_id"),
            Intrinsic::TraceId => hex("trace_id"),
            Intrinsic::EventName => "(toString(events.name[1]), 's')".to_string(),
            Intrinsic::EventTimeSinceStart => "(toString(if(empty(events), 0, \
                                               toInt64(events.time_ns[1]) - start_ns)), 'u')"
                .to_string(),
            Intrinsic::LinkTraceId => hex("links.trace_id[1]"),
            Intrinsic::LinkSpanId => "(if(empty(links), '0000000000000000', \
                                      lower(hex(links.span_id[1]))), 's')"
                .to_string(),
            Intrinsic::TraceDuration => {
                joins.trace = true;
                "(toString(tv.td), 'u')".to_string()
            }
            Intrinsic::RootName => {
                joins.trace = true;
                "(tv.rn, 's')".to_string()
            }
            Intrinsic::RootServiceName => {
                joins.trace = true;
                "(tv.rs, 's')".to_string()
            }
            Intrinsic::NestedSetLeft => {
                joins.numbering = true;
                "(toString(nb.nl), 'i')".to_string()
            }
            Intrinsic::NestedSetRight => {
                joins.numbering = true;
                "(toString(nb.nr), 'i')".to_string()
            }
            Intrinsic::NestedSetParent => {
                joins.numbering = true;
                "(toString(nb.np), 'i')".to_string()
            }
            Intrinsic::ChildCount => {
                joins.children = true;
                "(toString(ch.cc), 'i')".to_string()
            }
        },
    })
}

/// The `(bucket, trace_id)` pairs of every trace reaching `window`, from
/// the per-trace rows of its days ± 1.
fn trace_keys(traces: &str, window: WindowSql) -> String {
    format!(
        "(SELECT groupArray((k, trace_id)) FROM (SELECT trace_id, arrayJoin(bk) AS k \
         FROM (SELECT trace_id, groupUniqArrayArray(buckets) AS bk FROM {traces} \
         WHERE {} GROUP BY trace_id \
         HAVING max(last_start_ns) >= {} AND min(start_ns) <= {})))",
        window.per_trace_day_clause(),
        window.first_included_ns(),
        window.last_included_ns()
    )
}

/// The span rows of `window` the filter keeps, with the joins the keys and
/// the target read: the window's three bounds, the predicate, and the
/// target's condition.
fn from_clause(
    tables: &StatementTables<'_>,
    joins: Joins,
    window: WindowSql,
    predicate: &str,
    condition: &str,
) -> String {
    let mut join = String::new();
    if joins.trace {
        join.push_str(&format!(
            "\nLEFT JOIN (SELECT trace_id, max(end_ns) - min(start_ns) AS td, min(root) AS r, \
             if(r.1 = 0, r.5, '') AS rn, if(r.1 = 0, r.4, '') AS rs \
             FROM {} WHERE {} GROUP BY trace_id) AS tv USING trace_id",
            tables.traces,
            window.per_trace_day_clause()
        ));
    }
    if joins.numbering {
        join.push_str(&format!(
            "\nLEFT JOIN (WITH {} AS nested_keys SELECT trace_id, sp.1 AS span_id, nl, nr, np \
             FROM ({}) ARRAY JOIN s AS sp, lft AS nl, rgt AS nr, par AS np) AS nb \
             USING (trace_id, span_id)",
            trace_keys(tables.traces, window),
            numbering_sql(tables.spans, "nested_keys")
        ));
    }
    if joins.children {
        join.push_str(&format!(
            "\nLEFT JOIN (WITH {} AS child_keys SELECT trace_id, parent_span_id AS span_id, \
             count() AS cc FROM {} WHERE (intDiv(start_ns, 300000000000), trace_id) IN \
             (SELECT arrayJoin(child_keys)) AND parent_span_id != toFixedString('', 8) \
             GROUP BY trace_id, parent_span_id) AS ch USING (trace_id, span_id)",
            trace_keys(tables.traces, window),
            tables.spans
        ));
    }
    format!(
        "FROM {}{join}\nWHERE {}\n  AND {}\n  AND {}\n  AND ({predicate}){condition}",
        tables.spans,
        window.span_time_clause(),
        window.span_bucket_clause(),
        window.span_day_clause()
    )
}

/// What a plan's statement is made of.
struct Shape {
    kind: PlanKind,
    /// The query has `by()` keys.
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
    fn of(plan: &TraceMetricsPlan, grouped: bool) -> Option<Shape> {
        let kind = plan.kind();
        if kind == PlanKind::Compare {
            return None;
        }
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

    /// `O`: the series' order — a histogram's bucket, else `0`.
    fn order(&self, read: &Read) -> String {
        if self.kind == PlanKind::Histogram {
            format!("toUInt64(roundToExp2({} - 1)) * 2", read.i())
        } else {
            "toUInt64(0)".to_string()
        }
    }

    /// `V`: the value, a rate divided by `seconds`; a duration scaled to
    /// seconds, any other target unscaled.
    fn value(&self, read: &Read, seconds: f64) -> String {
        let duration = read.x.is_none();
        let agg = |f: &str| {
            if duration {
                format!("toFloat64({f}(duration_ns)) / 1000000000")
            } else {
                format!("{f}({})", read.x())
            }
        };
        match self.kind {
            PlanKind::Count { is_rate: true } => {
                format!("toFloat64(count()) / {}", f64_text(seconds))
            }
            PlanKind::Count { is_rate: false } | PlanKind::Histogram => {
                "toFloat64(count())".to_string()
            }
            PlanKind::Agg(AggFn::Sum) => agg("sum"),
            PlanKind::Agg(AggFn::Min) => agg("min"),
            PlanKind::Agg(AggFn::Max) => agg("max"),
            PlanKind::Agg(AggFn::Avg) if duration => "avg(duration_ns) / 1000000000".to_string(),
            PlanKind::Agg(AggFn::Avg) => agg("avg"),
            PlanKind::Quantile | PlanKind::Compare => {
                unreachable!("a quantile's value is its own level; compare() has no statement")
            }
        }
    }

    /// The quantile levels over the target, a duration scaled to seconds.
    fn levels(&self, plan: &TraceMetricsPlan, read: &Read) -> String {
        let scale = if read.x.is_none() {
            " / 1000000000"
        } else {
            ""
        };
        format!(
            "arrayMap(x -> if(isFinite(x), x{scale}, 0), CAST(quantilesTDigest({levels})({i}) AS \
             Array(Float64)))",
            levels = levels(plan),
            i = read.i()
        )
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

/// The requested quantile levels, as `quantilesTDigest`'s parameters.
fn levels(plan: &TraceMetricsPlan) -> String {
    plan.quantiles()
        .iter()
        .map(|q| f64_text(*q))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The range statement (section 3 of the design).
fn range_sql(plan: &TraceMetricsPlan, shape: &Shape, read: &Read, max_series: u64) -> String {
    let axis = plan.range_axis();
    let step_ms = plan.step_ms();
    let first = axis.first_ms / step_ms;
    let step_ns = i128::from(step_ms) * 1_000_000;
    let budget = plan.exemplar_budget();
    let bucket = format!("intDiv(start_ns - 1, {step_ns}) + 1 - {first}");
    let g = &read.g;
    let from = &read.from;
    let s = if shape.kind == PlanKind::Quantile {
        let indented = from.replace('\n', "\n      ");
        let ex = if budget == 0 {
            EX0.to_string()
        } else if read.x.is_none() {
            LONGEST.to_string()
        } else {
            format!(
                "argMax((trace_id, start_ns, {}), (duration_ns, span_id))",
                read.i()
            )
        };
        let scale = if read.x.is_none() {
            " / 1000000000"
        } else {
            ""
        };
        let by = if shape.grouped { "g, p" } else { "p" };
        format!(
            "SELECT g, toUInt64(qi) AS o, p, q AS v,\n       \
             if(qi = indexOf(arrayMap(x -> abs(x - x0.3{scale}), qq), \
             arrayMin(arrayMap(x -> abs(x - x0.3{scale}), qq))), x0, {EX0}) AS ex\n\
             FROM (SELECT {g} AS g, {bucket} AS p,\n             {levels} AS qq,\n             \
             {ex} AS x0\n      {indented}\n      GROUP BY {by})\n\
             ARRAY JOIN qq AS q, arrayEnumerate(qq) AS qi",
            levels = shape.levels(plan, read)
        )
    } else {
        let ex = if budget > 0 { LONGEST } else { EX0 };
        format!(
            "SELECT {g} AS g, {o} AS o, {bucket} AS p, {v} AS v, {ex} AS ex\n{from}\nGROUP BY {by}",
            o = shape.order(read),
            v = shape.value(read, plan.step_seconds()),
            by = if shape.many { "g, o, p" } else { "p" }
        )
    };
    let cap = if shape.grouped {
        format!(", throwIf(count() OVER () > {max_series}, '{SERIES_CAP_MESSAGE}') AS over")
    } else {
        String::new()
    };
    let ser = if shape.many {
        format!("SELECT g, o, groupArray((p, v, ex)) AS cells{cap}\nFROM ({s})\nGROUP BY g, o")
    } else {
        format!(
            "SELECT {NO_KEYS} AS g, toUInt64(0) AS o, groupArray((p, v, ex)) AS cells{cap}\n\
             FROM ({s})"
        )
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
                    "SELECT g, o, p, v, ex FROM ({cells})\nORDER BY g, o, p WITH FILL FROM 0 TO {}",
                    axis.points
                );
            }
            let compared = shape
                .compare("v")
                .map(|c| format!("\nWHERE {c}"))
                .unwrap_or_default();
            format!(
                "SELECT 0 AS none, g, o, p, v, ex FROM (SELECT g, o, p, v, ex, \
                 row_number() OVER (PARTITION BY p ORDER BY v {dir}, g, o) AS r\n\
                 FROM ({cells}){compared}) WHERE r <= {n}"
            )
        }
    };
    let (selected, exemplars) = if budget > 0 {
        let value = match (shape.kind, read.x.is_none()) {
            (PlanKind::Quantile, true) => "ex.3 / 1000000000",
            (PlanKind::Quantile, false) => "toFloat64(ex.3)",
            _ => "v",
        };
        let keep = format!(
            "has AND (T <= {budget} OR (intDiv(x * {budget} + T - 1, T) < {budget} AND \
             intDiv(intDiv(x * {budget} + T - 1, T) * T, {budget}) = x))"
        );
        (
            format!(
                "SELECT none, g, o, p, v, ex, ex.2 != 0 AS has,\n       \
                 row_number() OVER (PARTITION BY has ORDER BY p, g, o) - 1 AS x,\n       \
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
        "SELECT g, o, points, exemplars, {f} AS f\n\
         FROM (SELECT g, o, arraySort(groupArrayIf((({first} + p) * {step_ms}, v), NOT none)) AS points,\n             \
         {exemplars} AS exemplars\n      FROM ({selected})\n      GROUP BY g, o)\n{only}ORDER BY g, o",
        f = u8::from(shape.fill),
        only = if shape.fill {
            ""
        } else {
            "WHERE length(points) > 0\n"
        }
    )
}

/// The instant statement (section 3 of the design).
fn instant_sql(plan: &TraceMetricsPlan, shape: &Shape, read: &Read, max_series: u64) -> String {
    let g = &read.g;
    let from = &read.from;
    let mut sql = if shape.kind == PlanKind::Quantile {
        let tail = if shape.grouped {
            "\nGROUP BY g"
        } else {
            "\nHAVING count() > 0"
        };
        format!(
            "SELECT g, toUInt64(qi) AS o, q AS v FROM (SELECT {g} AS g, {levels} AS qq\n\
             {from}{tail})\nARRAY JOIN qq AS q, arrayEnumerate(qq) AS qi",
            levels = shape.levels(plan, read)
        )
    } else {
        let tail = if shape.many {
            "\nGROUP BY g, o"
        } else if matches!(shape.kind, PlanKind::Agg(_)) {
            "\nHAVING count() > 0"
        } else {
            ""
        };
        format!(
            "SELECT {g} AS g, {o} AS o, {v} AS v\n{from}{tail}",
            o = shape.order(read),
            v = shape.value(read, plan.window_seconds())
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
            "SELECT g, o, v FROM (SELECT g, o, v, row_number() OVER (ORDER BY v {dir}, g, o) AS r \
             FROM ({sql})) WHERE r <= {n}"
        );
    }
    format!(
        "SELECT g, o, [(toInt64({end}), v)] AS points, {NO_EXEMPLARS} AS exemplars, 0 AS f\n\
         FROM ({sql})\nORDER BY g, o",
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

    /// #594 part 5 (D1, D2): the seven trace-level and nested-set fields
    /// take the statement, which binds what each reads — the per-trace
    /// scalar for a trace leaf or a child count, the operand's pairs, the
    /// numbered traces — in both forms; a `!` demand keeps today's answer.
    #[test]
    fn the_seven_take_the_statement() {
        for (q, binding) in [
            ("{ trace:duration > 1s } | rate()", "AS per_trace"),
            ("{ rootServiceName = \"a\" } | rate()", "AS per_trace"),
            ("{ trace:rootName = \"a\" } | rate()", "AS per_trace"),
            (
                "{ duration > trace:duration / 2 } | rate()",
                "AS tv_duration",
            ),
            ("{ span:childCount > 1 } | rate()", "AS per_trace"),
            ("{ nestedSetLeft > 0 } | rate()", "AS nested_keys"),
            ("{ nestedSetRight < 100 } | rate()", "AS nested_keys"),
            ("{ nestedSetParent >= 1 } | rate()", "AS nested_keys"),
            ("{ nestedSetParent != -1 } | rate()", "AS nested_keys"),
            (
                "{ nestedSetRight - nestedSetLeft = 1 } | rate()",
                "AS nested_keys",
            ),
        ] {
            let r = routed(q, &PARAMS).unwrap_or_else(|e| panic!("{q}: routed refused: {e}"));
            let s = r.statement().unwrap_or_else(|| panic!("{q}: no statement"));
            for (form, sql) in [("range", s.range_sql()), ("instant", s.instant_sql())] {
                assert!(
                    sql.contains(binding),
                    "{q}: the {form} form lacks {binding}"
                );
            }
        }
        let q = "{ !span.a } | rate()";
        let want = today(q, &PARAMS).expect_err("today's planner refuses it");
        match routed(q, &PARAMS) {
            Err(got) => assert_eq!(got, want, "{q}"),
            Ok(r) => panic!("{q}: routed accepted it, statement {:?}", r.statement()),
        }
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
