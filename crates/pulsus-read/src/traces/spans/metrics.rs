//! TraceQL metrics on `spans`, one statement per request (issue #596,
//! `docs/TraceQL/server-implementation.md` §3.2).
//!
//! [`plan_trace_metrics_routed`] plans a metrics query's shell with the
//! shell planner and renders the one statement that answers it from the
//! span table, in five levels:
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
    SpansetExpr,
};
use serde::{Deserialize, Serialize};

use crate::traces::PlanError;
use crate::traces::metrics_plan::{
    AggFn, MetricsCtx, MetricsParams, PlanKind, STRUCTURAL_METRICS, SeriesReduce, TraceMetricsPlan,
    plan_trace_metrics,
};
use crate::traces::window_sql::WindowSql;

use super::attr_index::{IndexedAttr, index_hints};
use super::numbering::numbering_sql;
use super::predicate::{
    HELD, PredicateCtx, SpanPredicate, any_element_held, attr_path, compile_span_predicate_in,
    first_element, resource_label_in, scope_value,
};
use super::tracelevel::{CHILDREN, TraceLeaf, TraceValue, per_trace_sql, trace_values_sql};

/// The `throwIf` message of the series cap. The engine turns a `Code: 395`
/// carrying it into the series cap's `422`.
pub const SERIES_CAP_MESSAGE: &str = "metrics series cap";

/// The exemplar of a cell that has none: start `0`.
const EX0: &str = "CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Int64)')";

/// The exemplar of a cell that has none, for a `sum`/`min`/`max`/
/// `avg_over_time`, whose exemplar carries its span's own target value as
/// a double (issue #600 part 3): start `0`.
const EX0_OWN: &str =
    "CAST((toFixedString('', 16), 0, 0), 'Tuple(FixedString(16), Int64, Float64)')";

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
    /// The error-span table (issue #601 part 4b): the statement's span
    /// table when [`super::error_spans::admits`] the query.
    pub error_spans: &'a str,
    pub resources: &'a str,
    pub traces: &'a str,
    pub indexed: &'a [IndexedAttr],
    /// The request's tenant (issue #738), the first conjunct of every read
    /// of a trace table the statement renders.
    pub tenant: &'a crate::traces::tenant_scope::TenantTerm,
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
    demands: Vec<String>,
    compare: bool,
    /// Whether the statement runs under `final` (issue #601 part 4a).
    keeps_final: bool,
}

impl MetricsStatement {
    /// Whether this is a `compare()` statement (issue #597), whose rows are
    /// [`CompareStatementRow`]s.
    pub fn is_compare(&self) -> bool {
        self.compare
    }

    /// Whether the statement runs under `final` (issue #601 part 4a): it
    /// reads the nested-set numbering, through a key, the target or the
    /// filter. Every other statement keeps one row per span identity itself.
    pub fn keeps_final(&self) -> bool {
        self.keeps_final
    }

    /// The messages the filter's `!` demands raise (issue #602 part 2a): a
    /// `Code: 395` carrying one is the request's `400`.
    pub fn demands(&self) -> &[String] {
        &self.demands
    }

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

/// The shell planner's plan of a metrics query, and the statement that
/// answers it from `spans`.
#[derive(Debug, Clone)]
pub struct RoutedMetricsPlan {
    plan: TraceMetricsPlan,
    statement: MetricsStatement,
}

impl RoutedMetricsPlan {
    /// The shell planner's plan of the query with its `by()` emptied and
    /// its target `duration`: the window, step, axis, kind, quantiles,
    /// ranking, comparison and exemplar budget.
    pub fn plan(&self) -> &TraceMetricsPlan {
        &self.plan
    }

    /// The statement that answers the query.
    pub fn statement(&self) -> &MetricsStatement {
        &self.statement
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

/// What a query asks the statement for beyond the shell plan: its `by()`
/// keys and its target, and the refusal of a target the statement does
/// not aggregate.
struct Asked {
    keys: Vec<Field>,
    target: Target,
    refused_target: Option<String>,
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

/// Plans one metrics request: the validator's refusal, else the shell
/// planner's plan of the query with its `by()` emptied and its target
/// `duration` and the statement that answers it, else the first refusal
/// of the shell planner (window, pipeline shape, spanset shape), an
/// intrinsic target, or the statement compiler.
pub fn plan_trace_metrics_routed(
    query: &Query,
    params: &MetricsParams,
    ctx: &MetricsCtx,
    tables: &StatementTables<'_>,
) -> Result<RoutedMetricsPlan, PlanError> {
    // The shell empties `by()`, so the validator's arity rule is checked
    // here, before the shell can accept what the query may not ask.
    pulsus_traceql::validate(query).map_err(|e| PlanError::TypeMismatch(e.to_string()))?;
    let (pipeline, asked) = shell_pipeline(&query.pipeline);
    let plan = plan_trace_metrics(
        &Query {
            spanset: query.spanset.clone(),
            pipeline,
            hints: query.hints.clone(),
        },
        params,
        ctx,
    )?;
    let SpansetExpr::Filter(filter) = &query.spanset else {
        return Err(PlanError::TypeMismatch(STRUCTURAL_METRICS.to_string()));
    };
    if let [
        PipelineStage::Compare {
            selection,
            start_ns,
            end_ns,
            ..
        },
    ] = query.pipeline.as_slice()
    {
        let window = (*start_ns != 0 || *end_ns != 0).then_some((*start_ns, *end_ns));
        let statement = compare_statement(
            &plan,
            filter.body.as_ref(),
            selection.body.as_ref(),
            window,
            ctx.max_series,
            tables,
        )?;
        return Ok(RoutedMetricsPlan { plan, statement });
    }
    if let Some(refusal) = &asked.refused_target {
        return Err(PlanError::TypeMismatch(refusal.clone()));
    }
    // Issue #601 part 4b: an admitted query's statement reads the error-span
    // table as its span table.
    let tables = if super::error_spans::admits(query) {
        StatementTables {
            spans: tables.error_spans,
            ..*tables
        }
    } else {
        *tables
    };
    let statement = statement_for(&plan, &asked, filter.body.as_ref(), ctx.max_series, &tables)?;
    Ok(RoutedMetricsPlan { plan, statement })
}

/// The pipeline with its metric stage's `by()` emptied and its target
/// `duration`, and what the stage asked. An intrinsic target other than
/// `duration` and `span:childCount` is kept as its refusal (part 2's D8),
/// the shell's target `duration`, so the shell planner's refusals come
/// first.
fn shell_pipeline(pipeline: &[PipelineStage]) -> (Vec<PipelineStage>, Asked) {
    let mut asked = Asked {
        keys: Vec::new(),
        target: Target::Duration,
        refused_target: None,
    };
    let mut out = Vec::with_capacity(pipeline.len());
    for stage in pipeline {
        let PipelineStage::Metric(metric) = stage else {
            out.push(stage.clone());
            continue;
        };
        let mut metric = metric.clone();
        asked.keys = std::mem::take(&mut metric.by);
        let name = metric.func.name();
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
                Field::Intrinsic(i) => {
                    asked.refused_target = Some(format!(
                        "{name}() over {i} is not supported: its target must be duration, \
                         span:childCount or an attribute"
                    ));
                    Target::Duration
                }
                Field::Attribute { .. } => Target::Attribute(f.clone()),
            };
            *f = Field::Intrinsic(Intrinsic::Duration);
        }
        out.push(PipelineStage::Metric(metric));
    }
    (out, asked)
}

/// The statement for the shell `plan` and what the query asked, or the
/// statement compiler's refusal.
fn statement_for(
    plan: &TraceMetricsPlan,
    asked: &Asked,
    body: Option<&FieldExpr>,
    max_series: u64,
    tables: &StatementTables<'_>,
) -> Result<MetricsStatement, PlanError> {
    let shape = Shape::of(plan, !asked.keys.is_empty()).ok_or_else(|| {
        PlanError::TypeMismatch("a metrics comparison takes = != < <= > >=".to_string())
    })?;
    let (rlo, rhi) = plan.range_window_ns();
    let (ilo, ihi) = plan.snapped_window_ns();
    let range_window = WindowSql::start_closed_end_open(rlo, rhi);
    let instant_window = WindowSql::start_closed_end_open(ilo, ihi);
    let range = Read::of(plan, asked, body, range_window, tables)?;
    let instant = Read::of(plan, asked, body, instant_window, tables)?;
    Ok(MetricsStatement {
        range_sql: range_sql(plan, &shape, &range, max_series),
        instant_sql: instant_sql(plan, &shape, &instant, max_series),
        labels: asked.keys.iter().map(label_of_key).collect(),
        unit: asked.target.unit(),
        demands: demands_of(body, range_window, tables),
        compare: false,
        keeps_final: range.keeps_final,
    })
}

/// Whether the engine fills a series' empty buckets with `0` (D14).
#[cfg(test)]
fn fills(plan: &TraceMetricsPlan, grouped: bool) -> bool {
    Shape::of(plan, grouped).is_some_and(|s| s.fill)
}

/// The filter body compiled for `ctx`'s window, behind its index hints,
/// and the `WITH` binding what it reads beside the span row (#594 part 5):
/// the search statement's per-trace scalar, operand pairs and numbered
/// traces over that window, or nothing. `("true", "")` for `{}`; the
/// compiler's refusal when the body does not compile. A `!` demand is kept in the predicate
/// and carried as the statement's demands (issue #602 part 2a). The flag
/// is whether the predicate reads the nested-set numbering.
fn predicate(
    body: Option<&FieldExpr>,
    ctx: &PredicateCtx<'_>,
    traces: &str,
) -> Result<(String, String, bool), PlanError> {
    let Some(body) = body else {
        return Ok(("true".to_string(), String::new(), false));
    };
    let p = compile_span_predicate_in(body, ctx)?;
    let w = ctx.window;
    let leaves: Vec<&TraceLeaf> = p.trace_leaves().iter().collect();
    let mut bound = per_trace_sql(&leaves, p.reads_child_counts(), w, traces);
    bound.push_str(&trace_values_sql(p.trace_values(), w, w, traces));
    if p.reads_numbering() {
        bound.push_str(&format!("{} AS nested_keys", trace_keys(traces, w)));
    }
    let bound = bound.trim_end().trim_end_matches(',');
    let with = if bound.is_empty() {
        String::new()
    } else {
        format!("WITH {bound} ")
    };
    Ok((
        p.with_hints(&index_hints(body, ctx.indexed))
            .sql()
            .to_string(),
        with,
        p.reads_numbering(),
    ))
}

/// The `!` demands of `body`'s predicate over `window`.
fn demands_of(
    body: Option<&FieldExpr>,
    window: WindowSql,
    tables: &StatementTables<'_>,
) -> Vec<String> {
    let ctx = PredicateCtx {
        window,
        spans_table: tables.spans,
        resources_table: tables.resources,
        indexed: tables.indexed,
        tenant: tables.tenant,
    };
    body.and_then(|b| compile_span_predicate_in(b, &ctx).ok())
        .map(|p| p.demand_messages().to_vec())
        .unwrap_or_default()
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

/// What the statement reads over one window: the `WITH` the predicate
/// binds; the `FROM` clause, its joins and the window's three bounds, the
/// predicate and the target's condition; the key array `g`; the target's
/// `X` (a double) and `I` (an integer).
struct Read {
    with: String,
    from: String,
    g: String,
    x: Option<String>,
    i: Option<String>,
    /// Whether the read keeps `final`: it reads the numbering.
    keeps_final: bool,
}

impl Read {
    fn of(
        plan: &TraceMetricsPlan,
        asked: &Asked,
        body: Option<&FieldExpr>,
        window: WindowSql,
        tables: &StatementTables<'_>,
    ) -> Result<Read, PlanError> {
        let ctx = PredicateCtx {
            window,
            spans_table: tables.spans,
            resources_table: tables.resources,
            indexed: tables.indexed,
            tenant: tables.tenant,
        };
        let (predicate, with, numbered) = predicate(body, &ctx, tables.traces)?;
        let mut joins = Joins::default();
        let mut texts = Vec::with_capacity(asked.keys.len());
        for key in &asked.keys {
            texts.push(key_text(key, &ctx, &mut joins)?);
        }
        let g = if texts.is_empty() {
            NO_KEYS.to_string()
        } else {
            folded_keys(&texts.join(", "))
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
                let a = scope_value(*scope, key, &ctx)?;
                (
                    Some(format!(
                        "if(dynamicType({a}) = 'Int64', toFloat64(dynamicElement({a}, 'Int64')), \
                         if(dynamicType({a}) = 'Float64', dynamicElement({a}, 'Float64'), NULL))"
                    )),
                    Some(format!("dynamicElement({a}, 'Int64')")),
                )
            }
            // Unreachable: `shell_pipeline` builds an attribute target from
            // an attribute only.
            Target::Attribute(Field::Intrinsic(i)) => {
                return Err(PlanError::TypeMismatch(format!(
                    "{i} is not an attribute target"
                )));
            }
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
        // Issue #601 part 4a: a count and a rate count each span identity
        // once in the statement itself; any other kind reads one row per
        // span identity; a read of the numbering keeps `final` for both.
        let keeps_final = joins.numbering || numbered;
        let dedup = !keeps_final && !matches!(plan.kind(), PlanKind::Count { .. });
        // Issue #601 part 4c (D1): a read through the dedup wrap computes
        // the key array and the target inside it and reads them by alias,
        // so the wrap reads each subcolumn they name and not its whole
        // column. An ungrouped statement's key array is a constant, which
        // reads no column and stays outside, where it is not grouped by.
        let (inner, g, x, i) = if dedup {
            let keyed = !texts.is_empty();
            let mut inner = if keyed {
                format!(", {g} AS w_g")
            } else {
                String::new()
            };
            if let Some(x) = &x {
                inner.push_str(&format!(", {x} AS w_x"));
            }
            if let Some(i) = &i {
                inner.push_str(&format!(", {i} AS w_i"));
            }
            (
                inner,
                if keyed { "w_g".to_string() } else { g },
                x.map(|_| "w_x".to_string()),
                i.map(|_| "w_i".to_string()),
            )
        } else {
            (String::new(), g, x, i)
        };
        Ok(Read {
            with,
            from: from_clause(tables, joins, window, &predicate, &condition, dedup, &inner),
            g,
            x,
            i,
            keeps_final,
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

/// The key array `[{texts}]` with the text of every value the labels read
/// as missing — an absent value, or one with no TraceQL type — blanked,
/// `'n'` for an ordinary key and `'N'` for a well-known one each kept, so a
/// mixed array and a span without the key are one series, as the reference
/// answers (issue #600 part 1b-3). An array keeps its own text and type, so
/// each distinct array is its own `nil` series, as the reference's is
/// (`docs/api.md` §4.4).
fn folded_keys(texts: &str) -> String {
    format!("arrayMap(x -> if(x.2 IN ('n', 'N'), ('', x.2), x), [{texts}])")
}

/// `D(x)`: a `Dynamic` read as `(text, type)`, `missing` the type of an
/// absent value — `'N'` for a well-known key, `'n'` for any other.
fn dynamic_key(x: &str, missing: char) -> String {
    format!(
        "(toString({x}), multiIf(dynamicType({x}) = 'Int64', 'i', dynamicType({x}) = 'Float64', \
         'd', dynamicType({x}) = 'Bool', 'b', dynamicType({x}) = 'String', 's', \
         NOT (dynamicType({x}) {HELD}), '{missing}', 'a'))"
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

/// A key's `(text, type)` (section 3's table), and the joins it reads.
fn key_text(field: &Field, ctx: &PredicateCtx<'_>, joins: &mut Joins) -> Result<String, PlanError> {
    let string = |column: &str| format!("(toString({column}), 's')");
    let hex = |column: &str| format!("(lower(hex({column})), 's')");
    Ok(match field {
        Field::Attribute { scope, key } => {
            let missing = if well_known(*scope, key) { 'N' } else { 'n' };
            match scope {
                // The span row's string, else the resource row's label
                // (issue #493).
                AttrScope::Resource if key == "service.name" => {
                    let (present, label) = resource_label_in(key, ctx)?;
                    format!(
                        "if(service_type = 'string', (toString(service), 's'), if({present}, {label}, ('', 'N')))"
                    )
                }
                AttrScope::Span => dynamic_key(&attr_path("attrs", key), missing),
                // Issue #601 part 4c (D2): the label the lookup computes
                // once per resource row.
                AttrScope::Resource => {
                    let (present, label) = resource_label_in(key, ctx)?;
                    format!("if({present}, {label}, ('', '{missing}'))")
                }
                AttrScope::Instrumentation => dynamic_key(&attr_path("scope_attrs", key), missing),
                AttrScope::Event => dynamic_key(&first_element("events.attrs", key), missing),
                AttrScope::Link => dynamic_key(&first_element("links.attrs", key), missing),
                AttrScope::Unscoped => {
                    let span = attr_path("attrs", key);
                    let (present, label) = resource_label_in(key, ctx)?;
                    let resource_arm = if key == "service.name" {
                        format!(
                            "service_type = 'string', (toString(service), 's'), {present}, {label}"
                        )
                    } else {
                        format!("{present}, {label}")
                    };
                    format!(
                        "multiIf(dynamicType({span}) {HELD}, {}, {resource_arm}, {}, {}, {}, \
                         {}, {})",
                        dynamic_key(&span, missing),
                        any_element_held("events.attrs", key),
                        dynamic_key(&first_element("events.attrs", key), missing),
                        any_element_held("links.attrs", key),
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
/// target's condition. With `dedup` (issue #601 part 4a), one row per span
/// identity, the joins inside and each joined column the statement reads
/// named as it reads it, then `inner`, the columns the wrap computes for
/// the statement (part 4c); `inner` is ignored without `dedup`.
fn from_clause(
    tables: &StatementTables<'_>,
    joins: Joins,
    window: WindowSql,
    predicate: &str,
    condition: &str,
    dedup: bool,
    inner: &str,
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
             {CHILDREN} AS cc FROM {} WHERE (intDiv(start_ns, 300000000000), trace_id) IN \
             (SELECT arrayJoin(child_keys)) AND parent_span_id != toFixedString('', 8) \
             GROUP BY trace_id, parent_span_id) AS ch USING (trace_id, span_id)",
            trace_keys(tables.traces, window),
            tables.spans
        ));
    }
    let read = format!(
        "{}{join}\nWHERE {}\n  AND {}\n  AND {}\n  AND ({predicate}){condition}",
        tables.spans,
        window.span_time_clause(),
        window.span_bucket_clause(),
        window.span_day_clause()
    );
    if !dedup {
        return format!("FROM {read}");
    }
    let mut cols = String::new();
    if joins.trace {
        cols.push_str(", tv.td AS `tv.td`, tv.rn AS `tv.rn`, tv.rs AS `tv.rs`");
    }
    if joins.children {
        cols.push_str(", ch.cc AS `ch.cc`");
    }
    cols.push_str(inner);
    format!("FROM {}", super::distinct_spans(&read, &cols))
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
                    // metrics stage, so none reaches here.
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
            // Issue #601 part 4a: a count and a rate count each span
            // identity once, unless the statement keeps `final`.
            PlanKind::Count { is_rate: true } => {
                format!("toFloat64({}) / {}", count_of(read), f64_text(seconds))
            }
            PlanKind::Count { is_rate: false } => format!("toFloat64({})", count_of(read)),
            PlanKind::Histogram => "toFloat64(count())".to_string(),
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

/// A count and a rate's count (issue #601 part 4a): each span identity once,
/// or every row under `final`.
fn count_of(read: &Read) -> &'static str {
    if read.keeps_final {
        "count()"
    } else {
        "uniqExact(trace_id, start_ns, span_id, kind)"
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
    // Issue #600 part 3: a `sum`/`min`/`max`/`avg_over_time` exemplar's
    // value is its span's own target value, as the reference's
    // (`exemplarFnFor`, `pkg/traceql/ast_metrics.go:111-124`,
    // `engine_metrics_average.go:50`), not the series' sample.
    let own = matches!(shape.kind, PlanKind::Agg(_));
    let ex0 = if own { EX0_OWN } else { EX0 };
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
             FROM ({with}SELECT {g} AS g, {bucket} AS p,\n             {levels} AS qq,\n             \
             {ex} AS x0\n      {indented}\n      GROUP BY {by})\n\
             ARRAY JOIN qq AS q, arrayEnumerate(qq) AS qi",
            with = read.with,
            levels = shape.levels(plan, read)
        )
    } else {
        let ex = match (budget > 0, own) {
            (false, _) => ex0.to_string(),
            (true, false) => LONGEST.to_string(),
            (true, true) => {
                let value = if read.x.is_none() {
                    "toFloat64(duration_ns) / 1000000000".to_string()
                } else {
                    read.x()
                };
                format!("argMax((trace_id, start_ns, {value}), (duration_ns, span_id))")
            }
        };
        format!(
            "{with}SELECT {g} AS g, {o} AS o, {bucket} AS p, {v} AS v, {ex} AS ex\n{from}\nGROUP BY {by}",
            with = read.with,
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
                "SELECT g, o, empty(cells) AS none, c.1 AS p, {v} AS v, if(isNaN(v), {ex0}, c.3) AS ex\n\
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
            (PlanKind::Agg(_), _) => "ex.3",
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
            "SELECT g, toUInt64(qi) AS o, q AS v FROM ({with}SELECT {g} AS g, {levels} AS qq\n\
             {from}{tail})\nARRAY JOIN qq AS q, arrayEnumerate(qq) AS qi",
            with = read.with,
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
            "{with}SELECT {g} AS g, {o} AS o, {v} AS v\n{from}{tail}",
            with = read.with,
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

// ---------------------------------------------------------------------
// Issue #597: `compare()` in one statement
// ---------------------------------------------------------------------

/// The intrinsic keys a comparison counts, the reference's (D2), in the
/// statement's order.
pub const COMPARE_INTRINSIC_KEYS: [&str; 8] = [
    "name",
    "kind",
    "status",
    "statusMessage",
    "instrumentation:name",
    "instrumentation:version",
    "rootName",
    "rootServiceName",
];

/// One row of the `compare()` statement: its side `s` (`1` selection), its
/// grouping `g` (`0` a kept value; `1` a key's untrimmed sums and its
/// exemplars, or with `k` empty the side's population), key `k`, value
/// `v`, type `y`, per-bucket counts, per-bucket exemplars `(bucket, (trace
/// id, start ns))` and the cap's flag `c`.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct CompareStatementRow {
    pub s: u8,
    pub g: u8,
    pub k: String,
    pub v: String,
    pub y: String,
    pub pts: Vec<(i64, u64)>,
    pub ex: Vec<(i64, ([u8; 16], i64))>,
    pub c: u8,
}

/// The `compare()` statement (section 3 of the plan) for the shell `plan`:
/// `outer` the spanset's body, `selection` the comparison's, `window` its
/// `(start, end]` selection window. The compiler's refusal when either
/// body does not compile.
fn compare_statement(
    plan: &TraceMetricsPlan,
    outer: Option<&FieldExpr>,
    selection: Option<&FieldExpr>,
    window: Option<(i64, i64)>,
    max_series: u64,
    tables: &StatementTables<'_>,
) -> Result<MetricsStatement, PlanError> {
    let (rlo, rhi) = plan.range_window_ns();
    let (ilo, ihi) = plan.snapped_window_ns();
    let range_window = WindowSql::start_closed_end_open(rlo, rhi);
    let instant_window = WindowSql::start_closed_end_open(ilo, ihi);
    let step_ms = plan.step_ms();
    let step_ns = i128::from(step_ms) * 1_000_000;
    let range_p = format!("(intDiv(start_ns - 1, {step_ns}) + 1) * {step_ms}");
    let instant_p = format!("toInt64({})", plan.snapped_end_ms());
    let read = |w: WindowSql, p: &str| {
        compare_sql(plan, outer, selection, window, max_series, tables, w, p)
    };
    let (range_sql, keeps_final) = read(range_window, &range_p)?;
    let (instant_sql, _) = read(instant_window, &instant_p)?;
    let mut demands = demands_of(outer, range_window, tables);
    for d in demands_of(selection, range_window, tables) {
        if !demands.contains(&d) {
            demands.push(d);
        }
    }
    Ok(MetricsStatement {
        range_sql,
        instant_sql,
        labels: Vec::new(),
        unit: TargetUnit::Raw,
        demands,
        compare: true,
        keeps_final,
    })
}

/// `{J(pfx, col)}`: every top-level path of the JSON column `col` as
/// `(<pfx><decoded path>, text, type)`, the type from the column (D3).
fn compare_pairs(prefix: &str, column: &str) -> String {
    format!(
        "arrayMap(x -> ('{prefix}' || replaceAll(replaceAll(x.1, '%2E', '.'), '%25', '%'), \
         if(startsWith(x.2, '\"'), JSONExtractString(x.2), x.2), \
         transform(JSONAllPathsWithTypes({column})[x.1], ['Int64', 'Float64', 'Bool', 'String'], \
         ['i', 'd', 'b', 's'], '')), JSONExtractKeysAndValuesRaw(toString({column})))"
    )
}

/// An intrinsic key's text in the span row (D2).
fn compare_intrinsic(key: &str) -> &'static str {
    match key {
        "name" => "toString(name)",
        "kind" => {
            "['unspecified', 'internal', 'server', 'client', 'producer', 'consumer'][kind + 1]"
        }
        "status" => "['unset', 'ok', 'error'][status_code + 1]",
        "statusMessage" => "status_message",
        "instrumentation:name" => "toString(scope_name)",
        "instrumentation:version" => "toString(scope_version)",
        "rootName" => "tv.rn",
        "rootServiceName" => "tv.rs",
        other => unreachable!("{other} is not a compare intrinsic"),
    }
}

/// The section 3 statement over `w`, its bucket `p`, and whether it keeps
/// `final` (issue #601 part 4a): a body reads the nested-set numbering.
#[allow(clippy::too_many_arguments)]
fn compare_sql(
    plan: &TraceMetricsPlan,
    outer: Option<&FieldExpr>,
    selection: Option<&FieldExpr>,
    window: Option<(i64, i64)>,
    max_series: u64,
    tables: &StatementTables<'_>,
    w: WindowSql,
    p: &str,
) -> Result<(String, bool), PlanError> {
    let ctx = PredicateCtx {
        window: w,
        spans_table: tables.spans,
        resources_table: tables.resources,
        indexed: tables.indexed,
        tenant: tables.tenant,
    };
    let outer_p = outer
        .map(|b| compile_span_predicate_in(b, &ctx))
        .transpose()?;
    let selection_p = selection
        .map(|b| compile_span_predicate_in(b, &ctx))
        .transpose()?;
    // D12: both bodies' trace-level bindings in one `WITH`.
    let both: Vec<&SpanPredicate> = outer_p.iter().chain(selection_p.iter()).collect();
    let leaves: Vec<&TraceLeaf> = both.iter().flat_map(|p| p.trace_leaves()).collect();
    let children = both.iter().any(|p| p.reads_child_counts());
    let mut values: Vec<TraceValue> = Vec::new();
    for v in both.iter().flat_map(|p| p.trace_values()) {
        if !values.contains(v) {
            values.push(*v);
        }
    }
    let mut bound = per_trace_sql(&leaves, children, w, tables.traces);
    bound.push_str(&trace_values_sql(&values, w, w, tables.traces));
    let keeps_final = both.iter().any(|p| p.reads_numbering());
    if keeps_final {
        bound.push_str(&format!("{} AS nested_keys", trace_keys(tables.traces, w)));
    }
    let bound = bound.trim_end().trim_end_matches(',');
    let with = if bound.is_empty() {
        String::new()
    } else {
        format!("WITH {bound} ")
    };
    let outer_sql = match (&outer_p, outer) {
        (Some(p), Some(body)) => p
            .with_hints(&index_hints(body, ctx.indexed))
            .sql()
            .to_string(),
        _ => "true".to_string(),
    };
    let sel = match (&selection_p, window) {
        (None, _) => "(true)".to_string(),
        (Some(p), None) => format!("({})", p.sql()),
        (Some(p), Some((start, end))) => format!(
            "(({}) AND {})",
            p.sql(),
            WindowSql::start_open_end_closed(start, end).span_time_clause()
        ),
    };
    let intrinsics = COMPARE_INTRINSIC_KEYS
        .iter()
        .map(|k| format!("('{k}', {}, 's')", compare_intrinsic(k)))
        .collect::<Vec<_>>()
        .join(", ");
    let kvs = format!(
        "arrayConcat({}, {}, if(service_type = 'string', [('resource.service.name', \
         toString(service), 's')], []), [{intrinsics}, ('', '', 't'), ('', \
         concat(hex(resource_id), toString(service)), 'r')])",
        compare_pairs("span.", "attrs"),
        compare_pairs("instrumentation.", "scope_attrs")
    );
    let res = format!(
        "(SELECT concat(hex(resource_id), toString(service)) AS rk, any(kvs) AS rkv FROM \
         (SELECT service, resource_id, arrayFilter(t -> t.3 != '', {}) AS kvs FROM {} WHERE {}) \
         GROUP BY rk)",
        compare_pairs("resource.", "attrs"),
        tables.resources,
        w.resources_day_clause()
    );
    let from = from_clause(
        tables,
        Joins {
            trace: true,
            ..Joins::default()
        },
        w,
        &outer_sql,
        "",
        !keeps_final,
        // The outer read takes `kvs`, the whole `attrs`, which the wrap
        // supplies whole.
        "",
    )
    .replace('\n', &format!("\n{}", " ".repeat(36)));
    let exb = u8::from(plan.exemplar_budget() > 0);
    let top_n = plan.compare_top_n();
    Ok((format!(
        "{with}SELECT s, g, k, v, y, pts, ex, throwIf(countIf(g = 0 AND k != '') OVER () + 4 * uniqExactIf(k, g = 1 AND k != '') OVER () > {max_series}, '{SERIES_CAP_MESSAGE}') AS c
FROM (SELECT s, g, k, v, y, pts, ex, row_number() OVER (PARTITION BY s, g, k ORDER BY tot DESC, v, y) AS rn
      FROM (SELECT s, g, k, v, y, arraySort(groupArray((p, n))) AS pts, sum(n) AS tot,
                   if(g = 1 AND {exb}, arraySort(groupArray((p, (x.1, x.2)))), []) AS ex
            FROM (SELECT s, e.1 AS k, e.2 AS v, e.3 AS y, p, sum(n) AS n, argMax(x, (x.3, x.4)) AS x, toUInt8(grouping(e.2)) AS g
                  FROM (SELECT s, k, v, y, p, count() AS n, argMax((trace_id, st, du, sid), (du, sid)) AS x
                        FROM (SELECT s, p, e.1 AS k, e.2 AS v, e.3 AS y, trace_id, st, du, sid
                              FROM (SELECT toUInt8({sel}) AS s, {p} AS p, trace_id, start_ns AS st, duration_ns AS du, span_id AS sid,
                                           {kvs} AS kvs
                                    {from})
                              ARRAY JOIN kvs AS e
                              WHERE e.3 != '')
                        GROUP BY s, k, v, y, p) AS c
                  LEFT JOIN {res} AS rv ON c.y = 'r' AND c.v = rv.rk
                  ARRAY JOIN if(c.y = 'r', rv.rkv, [(c.k, c.v, c.y)]) AS e
                  GROUP BY GROUPING SETS ((s, e.1, e.2, e.3, p), (s, e.1, p)))
            GROUP BY s, g, k, v, y))
WHERE g = 1 OR (k != '' AND rn <= {top_n})
ORDER BY s, g, k, v, y"
        ),
        keeps_final,
    ))
}

#[cfg(test)]
mod tests {
    use pulsus_traceql::{SpansetFilter, parse};

    use super::*;

    fn ctx() -> MetricsCtx {
        MetricsCtx {
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
        floor_ns: i64::MIN,
    };

    static TABLES: std::sync::LazyLock<StatementTables<'static>> =
        std::sync::LazyLock::new(|| StatementTables {
            spans: "spans",
            error_spans: "error_spans",
            resources: "resources",
            traces: "traces",
            indexed: &[],
            tenant: crate::traces::tenant_scope::empty_term(),
        });

    fn routed(q: &str, params: &MetricsParams) -> Result<RoutedMetricsPlan, PlanError> {
        plan_trace_metrics_routed(
            &parse(q).unwrap_or_else(|e| panic!("{q} must parse: {e:?}")),
            params,
            &ctx(),
            &TABLES,
        )
    }

    fn shell(q: &str, params: &MetricsParams) -> Result<TraceMetricsPlan, PlanError> {
        plan_trace_metrics(
            &parse(q).unwrap_or_else(|e| panic!("{q} must parse: {e:?}")),
            params,
            &ctx(),
        )
    }

    /// D1: a field against a field, arithmetic and an absence check are
    /// served from the statement.
    #[test]
    fn a_field_against_a_field_is_served() {
        for q in [
            "{ span.a = span.b } | rate()",
            "{ duration * 2 > 700ms } | rate()",
            "{ .a = nil } | rate()",
        ] {
            routed(q, &PARAMS).unwrap_or_else(|e| panic!("{q}: routed refused: {e}"));
        }
    }

    /// #594 part 5 (D1, D2): the seven trace-level and nested-set fields
    /// take the statement, which binds what each reads — the per-trace
    /// scalar for a trace leaf or a child count, the operand's pairs, the
    /// numbered traces — in both forms; so does a `!` demand.
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
            let s = r.statement();
            for (form, sql) in [("range", s.range_sql()), ("instant", s.instant_sql())] {
                assert!(
                    sql.contains(binding),
                    "{q}: the {form} form lacks {binding}"
                );
            }
        }
        // Issue #602 part 2a: a `!` demand takes the statement too, which
        // carries the demand for its `400`.
        let q = "{ !span.a } | rate()";
        let r = routed(q, &PARAMS).unwrap_or_else(|e| panic!("{q}: routed refused: {e}"));
        let s = r.statement();
        assert!(
            !s.demands().is_empty(),
            "{q}: the statement carries no demand"
        );
    }

    /// The refused text of `q`, which must be a `TypeMismatch`.
    fn refusal(q: &str) -> String {
        match routed(q, &PARAMS) {
            Err(PlanError::TypeMismatch(text)) => text,
            other => panic!("{q}: not a TypeMismatch refusal: {other:?}"),
        }
    }

    /// Issue #602 part 3a (D2): a filter or key the statement compiler
    /// refuses is refused in its words; the previous planner's filter
    /// compiler answers nothing.
    #[test]
    fn a_refusal_is_the_statement_compilers() {
        for (q, want) in [
            (
                "{ name != nil } | histogram_over_time(span:childCount)",
                "existence checks are only supported on attributes",
            ),
            (
                "{ !instrumentation:name } | rate()",
                "expression (!instrumentation:name) expected a boolean",
            ),
        ] {
            assert_eq!(refusal(q), want, "{q}");
        }
    }

    /// Issue #602 part 3a (§2.1): an intrinsic target other than `duration`
    /// and `span:childCount` is refused by name.
    #[test]
    fn an_intrinsic_target_is_refused_by_name() {
        for (q, want) in [
            (
                "{} | sum_over_time(nestedSetLeft)",
                "sum_over_time() over nestedSetLeft is not supported: its target must be \
                 duration, span:childCount or an attribute",
            ),
            (
                "{} | avg_over_time(trace:duration)",
                "avg_over_time() over traceDuration is not supported: its target must be \
                 duration, span:childCount or an attribute",
            ),
        ] {
            assert_eq!(refusal(q), want, "{q}");
        }
    }

    /// Issue #602 part 3a: `compare()` beside another stage is the
    /// validator's refusal.
    #[test]
    fn compare_with_another_stage_is_the_validators_refusal() {
        let q = "{ } | by(name) | compare({ status = error })";
        assert_eq!(
            refusal(q),
            "compare() cannot be combined with any other pipeline stage",
            "{q}"
        );
    }

    /// An invalid regular expression on either side is refused when the
    /// request is planned, not when it runs.
    #[test]
    fn an_invalid_regex_is_refused_at_plan_time() {
        for q in [
            r#"{ name =~ "(" } | rate()"#,
            r#"{ .a =~ "(" } | compare({ .b = 1 })"#,
            r#"{ } | compare({ .b =~ "(" })"#,
        ] {
            assert_eq!(
                refusal(q),
                r#"invalid regex "(": not a valid RE2 regular expression"#,
                "{q}"
            );
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
            let st = r.statement();
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
            r.expect("checked").statement().range_sql().to_string()
        };
        // The key's own missing arm (`dynamic_key`, the service column's, or
        // a resource label's, issue #601 part 4c), not `folded_keys`, which
        // names both codes in every keyed statement (#600 part 1b-3).
        let missing = |sql: &str, code: char| {
            let label = format!("), ('', '{code}'))");
            sql.contains(&format!("{HELD}), '{code}', 'a')"))
                || (code == 'N' && sql.contains("(toString(service), 's'), ('', 'N'))"))
                || sql
                    .match_indices("ri_")
                    .any(|(at, _)| sql.get(at + 19..).is_some_and(|r| r.starts_with(&label)))
        };
        for key in well_known {
            assert!(missing(&statement(key), 'N'), "{key}: no 'N'");
        }
        for key in ["resource.foo", "span.foo"] {
            let sql = statement(key);
            assert!(missing(&sql, 'n'), "{key}: no 'n'");
            assert!(!missing(&sql, 'N'), "{key}: an 'N'");
        }
    }

    /// Section 3's `{traces}`, `{spans}` and `{resources}` are the caller's
    /// tables, never a name written into the SQL.
    #[test]
    fn the_tables_come_from_the_caller() {
        let tables = StatementTables {
            spans: "sp_t",
            error_spans: "es_t",
            resources: "rs_t",
            traces: "tr_t",
            indexed: &[],
            tenant: crate::traces::tenant_scope::empty_term(),
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
            let st = r.statement();
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

    /// D2: a window error is the shell planner's, word for word.
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
            let want = shell("{} | rate()", &params).expect_err("the shell planner refuses it");
            assert_eq!(routed(q, &params).err(), Some(want), "{params:?}");
        }
    }

    /// D14: the fill flag is decided once, from the comparison and the
    /// ranking.
    #[test]
    fn zero_passing_is_decided_once() {
        let flag = |q: &str| {
            let plan = shell(q, &PARAMS).unwrap_or_else(|e| panic!("{q}: {e}"));
            fills(&plan, q.contains("by("))
        };
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

    // ---- issue #597: compare() in one statement ------------------------

    const CMP_PARAMS: MetricsParams = MetricsParams {
        start_ns: 1_700_000_000_000_000_000,
        end_ns: 1_700_003_600_000_000_000,
        step_ms: 60_000,
        exemplars: Some(100),
        floor_ns: i64::MIN,
    };

    /// §3 of the plan, typed out: the range form; the instant form differs
    /// in `{P}` and the window.
    const COMPARE_TEMPLATE: &str = "{WITH}SELECT s, g, k, v, y, pts, ex, throwIf(countIf(g = 0 AND k != '') OVER () + 4 * uniqExactIf(k, g = 1 AND k != '') OVER () > {CAP}, 'metrics series cap') AS c
FROM (SELECT s, g, k, v, y, pts, ex, row_number() OVER (PARTITION BY s, g, k ORDER BY tot DESC, v, y) AS rn
      FROM (SELECT s, g, k, v, y, arraySort(groupArray((p, n))) AS pts, sum(n) AS tot,
                   if(g = 1 AND {EXB}, arraySort(groupArray((p, (x.1, x.2)))), []) AS ex
            FROM (SELECT s, e.1 AS k, e.2 AS v, e.3 AS y, p, sum(n) AS n, argMax(x, (x.3, x.4)) AS x, toUInt8(grouping(e.2)) AS g
                  FROM (SELECT s, k, v, y, p, count() AS n, argMax((trace_id, st, du, sid), (du, sid)) AS x
                        FROM (SELECT s, p, e.1 AS k, e.2 AS v, e.3 AS y, trace_id, st, du, sid
                              FROM (SELECT toUInt8({SEL}) AS s, {P} AS p, trace_id, start_ns AS st, duration_ns AS du, span_id AS sid,
                                           {KVS} AS kvs
                                    {FROM})
                              ARRAY JOIN kvs AS e
                              WHERE e.3 != '')
                        GROUP BY s, k, v, y, p) AS c
                  LEFT JOIN {RES} AS rv ON c.y = 'r' AND c.v = rv.rk
                  ARRAY JOIN if(c.y = 'r', rv.rkv, [(c.k, c.v, c.y)]) AS e
                  GROUP BY GROUPING SETS ((s, e.1, e.2, e.3, p), (s, e.1, p)))
            GROUP BY s, g, k, v, y))
WHERE g = 1 OR (k != '' AND rn <= {TOPN})
ORDER BY s, g, k, v, y";

    /// `{J(pfx, col)}`, typed out.
    fn typed_j(pfx: &str, col: &str) -> String {
        format!(
            "arrayMap(x -> ('{pfx}' || replaceAll(replaceAll(x.1, '%2E', '.'), '%25', '%'), \
             if(startsWith(x.2, '\"'), JSONExtractString(x.2), x.2), \
             transform(JSONAllPathsWithTypes({col})[x.1], ['Int64', 'Float64', 'Bool', 'String'], \
             ['i', 'd', 'b', 's'], '')), JSONExtractKeysAndValuesRaw(toString({col})))"
        )
    }

    /// §3's statement for `{} | compare(<selection>…)` over `w`, typed out.
    fn typed_compare(sel: &str, p: &str, w: WindowSql, top_n: u64) -> String {
        let kvs = format!(
            "arrayConcat({}, {}, if(service_type = 'string', [('resource.service.name', \
             toString(service), 's')], []), [('name', toString(name), 's'), ('kind', \
             ['unspecified', 'internal', 'server', 'client', 'producer', 'consumer'][kind + 1], \
             's'), ('status', ['unset', 'ok', 'error'][status_code + 1], 's'), ('statusMessage', \
             status_message, 's'), ('instrumentation:name', toString(scope_name), 's'), \
             ('instrumentation:version', toString(scope_version), 's'), ('rootName', tv.rn, 's'), \
             ('rootServiceName', tv.rs, 's'), ('', '', 't'), ('', concat(hex(resource_id), \
             toString(service)), 'r')])",
            typed_j("span.", "attrs"),
            typed_j("instrumentation.", "scope_attrs")
        );
        let res = format!(
            "(SELECT concat(hex(resource_id), toString(service)) AS rk, any(kvs) AS rkv FROM \
             (SELECT service, resource_id, arrayFilter(t -> t.3 != '', {}) AS kvs FROM resources \
             WHERE {}) GROUP BY rk)",
            typed_j("resource.", "attrs"),
            w.resources_day_clause()
        );
        // Issue #601 part 4a: one row per span identity, the join inside
        // and its three columns named as the statement reads them.
        let from = format!(
            "FROM (SELECT *, tv.td AS `tv.td`, tv.rn AS `tv.rn`, tv.rs AS `tv.rs` FROM spans\n\
             LEFT JOIN (SELECT trace_id, max(end_ns) - min(start_ns) AS td, min(root) \
             AS r, if(r.1 = 0, r.5, '') AS rn, if(r.1 = 0, r.4, '') AS rs FROM traces WHERE {} \
             GROUP BY trace_id) AS tv USING trace_id\nWHERE {}\n  AND {}\n  AND {}\n  AND (true)\n\
             LIMIT 1 BY trace_id, start_ns, span_id, kind)",
            w.per_trace_day_clause(),
            w.span_time_clause(),
            w.span_bucket_clause(),
            w.span_day_clause()
        )
        .replace('\n', &format!("\n{}", " ".repeat(36)));
        COMPARE_TEMPLATE
            .replace("{WITH}", "")
            .replace("{CAP}", "1000")
            .replace("{EXB}", "1")
            .replace("{SEL}", sel)
            .replace("{P}", p)
            .replace("{KVS}", &kvs)
            .replace("{FROM}", &from)
            .replace("{RES}", &res)
            .replace("{TOPN}", &top_n.to_string())
    }

    /// The SQL of `status = error` compiled over `w`.
    fn status_error_sql(w: WindowSql) -> String {
        let q = parse("{ status = error } | rate()").expect("parse");
        let SpansetExpr::Filter(SpansetFilter { body: Some(body) }) = &q.spanset else {
            panic!("a filter body");
        };
        let ctx = PredicateCtx {
            window: w,
            spans_table: "spans",
            resources_table: "resources",
            indexed: &[],
            tenant: crate::traces::tenant_scope::empty_term(),
        };
        compile_span_predicate_in(body, &ctx)
            .expect("compiles")
            .sql()
            .to_string()
    }

    /// U1 (issue #597): the range and instant statements are §3's, byte for
    /// byte; a selection window wraps the selection and `topN` bounds `rn`.
    #[test]
    fn the_compare_statement_is_byte_exact() {
        let routed =
            |q: &str| routed(q, &CMP_PARAMS).unwrap_or_else(|e| panic!("{q}: routed refused: {e}"));
        let r = routed("{} | compare({ status = error })");
        let st = r.statement();
        let (rlo, rhi) = r.plan().range_window_ns();
        let range_w = WindowSql::start_closed_end_open(rlo, rhi);
        let p = status_error_sql(range_w);
        let bucket = "(intDiv(start_ns - 1, 60000000000) + 1) * 60000";
        assert_eq!(
            st.range_sql(),
            typed_compare(&format!("({p})"), bucket, range_w, 10)
        );
        let (ilo, ihi) = r.plan().snapped_window_ns();
        let instant_w = WindowSql::start_closed_end_open(ilo, ihi);
        let ip = status_error_sql(instant_w);
        assert_eq!(
            st.instant_sql(),
            typed_compare(
                &format!("({ip})"),
                &format!("toInt64({})", r.plan().snapped_end_ms()),
                instant_w,
                10
            )
        );
        let r =
            routed("{} | compare({ status = error }, 3, 1700000600000000000, 1700001200000000000)");
        let st = r.statement();
        assert_eq!(
            st.range_sql(),
            typed_compare(
                &format!(
                    "(({p}) AND start_ns > 1700000600000000000 AND start_ns <= \
                     1700001200000000000)"
                ),
                bucket,
                range_w,
                3
            )
        );
    }

    /// U2 (issue #597): a comparison whose spanset is one filter takes the
    /// statement; a structural spanset is the shell planner's refusal.
    #[test]
    fn compare_takes_the_statement_where_its_spanset_is_one_filter() {
        let q = "{ .a = 1 } | compare({ .b = 2 })";
        let r = routed(q, &PARAMS).unwrap_or_else(|e| panic!("{q}: routed refused: {e}"));
        let st = r.statement();
        assert!(st.is_compare(), "{q}: not a compare statement");
        // A structural spanset is the shell planner's refusal.
        let q = "{ .a = 1 } >> { .b = 2 } | compare({ .c = 3 })";
        let r = routed(q, &PARAMS);
        assert!(r.is_err(), "{q}: a statement");
        assert_eq!(
            r.as_ref().err(),
            shell(q, &PARAMS).err().as_ref(),
            "{q}: the shell planner's answer"
        );
    }

    /// U3 (issue #597): the eight intrinsic keys are D2's, each in the
    /// statement, and no other intrinsic, event or link key is.
    #[test]
    fn the_compare_keys_are_the_references_eight() {
        assert_eq!(
            COMPARE_INTRINSIC_KEYS,
            [
                "name",
                "kind",
                "status",
                "statusMessage",
                "instrumentation:name",
                "instrumentation:version",
                "rootName",
                "rootServiceName",
            ]
        );
        let q = "{} | compare({ status = error })";
        let r = routed(q, &PARAMS).unwrap_or_else(|e| panic!("{q}: routed refused: {e}"));
        let sql = r.statement().range_sql().to_string();
        for key in COMPARE_INTRINSIC_KEYS {
            assert!(sql.contains(&format!("('{key}', ")), "{key} missing");
        }
        for absent in [
            "'span:id'",
            "'duration'",
            "'traceDuration'",
            "'event:",
            "'link:",
            "'event.",
            "'link.",
            "'trace:id'",
            "'span:parentID'",
            "'nestedSetLeft'",
            "'span:childCount'",
        ] {
            assert!(!sql.contains(absent), "{absent} present");
        }
        for column in ["events", "links"] {
            let named = sql
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .any(|w| w == column);
            assert!(!named, "the column {column} is read");
        }
    }

    /// U4 (issue #597): both bodies' trace-level bindings merge into one
    /// `WITH`, so a leaf both read is bound once.
    #[test]
    fn two_bodies_with_one_trace_leaf_bind_it_once() {
        let q = "{ traceDuration > 1s } | compare({ traceDuration > 1s && status = error })";
        let r = routed(q, &PARAMS).unwrap_or_else(|e| panic!("{q}: routed refused: {e}"));
        let st = r.statement();
        for sql in [st.range_sql(), st.instant_sql()] {
            assert_eq!(sql.matches("WITH ").count(), 1, "one WITH: {sql}");
            let aliases: Vec<&str> = sql
                .match_indices(" AS tl_")
                .map(|(i, _)| {
                    let rest = &sql[i + 4..];
                    let end = rest
                        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                        .unwrap_or(rest.len());
                    &rest[..end]
                })
                .collect();
            assert_eq!(
                aliases.len(),
                1,
                "the leaf's alias defined once: {aliases:?}"
            );
        }
    }
}
