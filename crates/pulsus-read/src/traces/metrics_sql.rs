//! Pure SQL string builders for the TraceQL metrics endpoints (issue
//! #59; docs/schemas.md §4.2, docs/api.md §4.4) — the byte-frozen golden
//! surface (`tests/traces_metrics_sql.rs`), same convention as
//! [`super::search_sql`]: pre-escaped fragments → `String`, no
//! `ChClient`, no I/O, no randomness.
//!
//! Metrics is a **single fully-pushed-down aggregation**, not the
//! two-phase candidate model: one time-bucketed query per request.
//! Counting is always `uniqExact(trace_id, span_id)` (plan v2 delta 1:
//! at-least-once replays must never inflate a bucket — this is exactly
//! T5's `(trace_id, span_id)` logical-span identity, carried flat here
//! because `span_id` is trace-local). Buckets are left-closed epoch
//! intervals `[b, b + step)` over the **snapped** window `[S, E)` (plan
//! v2 delta 2 — [`super::metrics_plan`] does the snapping; every emitted
//! bucket is full-width, so the client-side rate division always uses
//! the full `step_s`). The time filter is left-closed/right-open
//! (`>= S`, `< E`), deliberately different from search's `> start`,
//! `<= end`.
//!
//! Leaf lowering reuses T5's shared compiler ([`super::filter`]):
//! physical leaves inline on `trace_spans` columns; attribute leaves
//! become index-served `(trace_id, span_id) [NOT] IN (SELECT … FROM
//! trace_attrs_idx …)` semi-joins confined to the `(key[, val][, scope])`
//! prefix plus date/time pruning (`NOT IN` with the positive predicate is
//! the ratified `!=`/`!~` absent-key rule: a span with no positive index
//! row is counted). A `resource.service.name = "…"` comparison sitting as
//! a direct conjunct on the **root AND spine** — never inside or under
//! any `||` — is hoisted to `PREWHERE service = '…'` to select the
//! `service_time` projection (plan v2 delta 4: `Or` nodes are opaque,
//! rendered wholesale in `WHERE`, no hoist).

use pulsus_traceql::{AttrScope, BoolOp, ComparisonOp, Field, FieldExpr, Value};

use crate::logql::escape;

use super::filter::{self, AttrProbe, LeafEval, PlanError, ValuePred};
use super::search_plan::compile_anchored;
use super::search_sql::{byte_cap_expr, date_literal, root_ordering_tuple};

/// The snapped, left-closed/right-open metrics evaluation window
/// `[start_ns, end_ns)` — produced by `metrics_plan`'s epoch snapping,
/// deliberately a distinct type from `TimeWindow` (whose consumers render
/// the search-side `> start AND <= end` bound).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnappedWindow {
    pub start_ns: i64,
    pub end_ns: i64,
}

const NS_PER_DAY: i64 = 86_400_000_000_000;

/// The `trace_attrs_idx` daily-partition pruning clause for a right-open
/// window: the end day comes from the last **included** nanosecond
/// (`end_ns - 1`), so a window ending exactly at midnight never drags in
/// an extra day's partition.
fn date_clause(w: SnappedWindow) -> String {
    let start_days = w.start_ns.div_euclid(NS_PER_DAY);
    let end_days = (w.end_ns - 1).div_euclid(NS_PER_DAY);
    format!(
        "date >= {} AND date <= {}",
        date_literal(start_days),
        date_literal(end_days)
    )
}

/// The left-closed/right-open metrics time bound.
fn time_clause(w: SnappedWindow) -> String {
    format!(
        "timestamp_ns >= {} AND timestamp_ns < {}",
        w.start_ns, w.end_ns
    )
}

/// One compiled spanset filter, rendered for the single-query metrics
/// pushdown: an optional `PREWHERE` fragment (the hoisted root-AND-spine
/// service equality) and an optional residual `WHERE` boolean expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterSql {
    pub prewhere: Option<String>,
    pub where_expr: Option<String>,
}

/// Compiles one `{...}` filter body into its metrics `PREWHERE`/`WHERE`
/// fragments. `body: None` is the `{}` match-all (time-only) filter.
/// Regexes are validated here at plan time (`compile_anchored`) so a bad
/// pattern is a `400`, never a mid-query server error.
pub fn compile_filter_predicate(
    body: Option<&FieldExpr>,
    attrs_table: &str,
    window: SnappedWindow,
) -> Result<FilterSql, PlanError> {
    let Some(body) = body else {
        return Ok(FilterSql {
            prewhere: None,
            where_expr: None,
        });
    };
    let (prewhere, remainder) = extract_root_service_eq(body);
    let where_expr = match &remainder {
        Some(expr) => Some(render_expr(expr, attrs_table, window)?),
        None => None,
    };
    Ok(FilterSql {
        prewhere,
        where_expr,
    })
}

/// Hoists the first `resource.service.name = "…"` conjunct found on the
/// root AND spine (plan v2 delta 4). Traversal descends through
/// `Binary { And, .. }` nodes only — an `Or` node is opaque and never
/// descended — and the remainder is the tree with that one leaf removed
/// (`None` when the whole body was the hoisted leaf).
fn extract_root_service_eq(expr: &FieldExpr) -> (Option<String>, Option<FieldExpr>) {
    match expr {
        FieldExpr::Comparison {
            field: Field::Attribute { scope, key },
            op: ComparisonOp::Eq,
            value: Value::String(s),
        } if *scope == AttrScope::Resource && key == "service.name" => {
            (Some(format!("service = {}", escape::ch_string(s))), None)
        }
        FieldExpr::Binary {
            op: BoolOp::And,
            lhs,
            rhs,
        } => {
            let (hoisted, lhs_rem) = extract_root_service_eq(lhs);
            if hoisted.is_some() {
                return (hoisted, recombine(lhs_rem, Some((**rhs).clone())));
            }
            let (hoisted, rhs_rem) = extract_root_service_eq(rhs);
            if hoisted.is_some() {
                return (hoisted, recombine(Some((**lhs).clone()), rhs_rem));
            }
            (None, Some(expr.clone()))
        }
        other => (None, Some(other.clone())),
    }
}

/// Rejoins the two sides of an AND node after a hoist removed a leaf.
fn recombine(lhs: Option<FieldExpr>, rhs: Option<FieldExpr>) -> Option<FieldExpr> {
    match (lhs, rhs) {
        (Some(l), Some(r)) => Some(FieldExpr::Binary {
            op: BoolOp::And,
            lhs: Box::new(l),
            rhs: Box::new(r),
        }),
        (Some(one), None) | (None, Some(one)) => Some(one),
        (None, None) => None,
    }
}

/// Compiles a filter body into a single boolean SQL expression (issue
/// #182, compare's `selection`): no `PREWHERE` hoisting — the whole filter
/// renders as one per-span predicate (`service = '…'` inline, attribute
/// leaves as `[NOT] IN` semi-joins). `None` (the `{}` match-all) is `1`.
/// Regexes are validated at plan time.
pub fn compile_filter_bool(
    body: Option<&FieldExpr>,
    attrs_table: &str,
    window: SnappedWindow,
) -> Result<String, PlanError> {
    match body {
        None => Ok("1".to_string()),
        Some(expr) => render_expr(expr, attrs_table, window),
    }
}

/// Renders one filter subtree as a boolean SQL expression: binary nodes
/// are always parenthesized (`(lhs AND rhs)`), physical leaves render via
/// the shared compiler's pre-escaped fragments, attribute leaves become
/// `[NOT] IN` semi-joins.
fn render_expr(
    expr: &FieldExpr,
    attrs_table: &str,
    window: SnappedWindow,
) -> Result<String, PlanError> {
    match expr {
        FieldExpr::Comparison { field, op, value } => {
            let leaf = filter::compile_leaf(field, *op, value)?;
            match &leaf.eval {
                LeafEval::Physical(p) => {
                    validate_physical_regex(p)?;
                    Ok(filter::physical_sql(p))
                }
                LeafEval::Attr { probe, negated } => {
                    validate_probe_regex(probe)?;
                    Ok(semi_join_sql(probe, *negated, attrs_table, window))
                }
                // Nested-set intrinsics (issue #181) are query-time
                // structural properties with no SQL column — unsupported
                // on the metrics filter path (a clean 400, tracked as a
                // follow-up). Search remains the surface for nested-set.
                LeafEval::NestedSet { .. } => Err(PlanError::TypeMismatch(
                    "nested-set intrinsics are not supported in metrics filters".to_string(),
                )),
                // Trace-level intrinsics (issue #184) resolve from the
                // search engine's per-trace co-load — no per-span SQL
                // column exists on the metrics filter path (a clean 400,
                // mirroring nested-set; search remains their surface).
                LeafEval::TraceCtx(_) => Err(PlanError::TypeMismatch(
                    "trace-level intrinsics are not supported in metrics filters".to_string(),
                )),
                // `compile_leaf` never yields a field-vs-field or arithmetic
                // leaf (those come via the `FieldCompare`/`ArithCompare` AST
                // arms) — keep the match exhaustive.
                LeafEval::FieldCompare { .. } => Err(PlanError::TypeMismatch(
                    "field-vs-field comparisons are not supported in metrics filters".to_string(),
                )),
                LeafEval::Arith { .. } => Err(PlanError::TypeMismatch(
                    "arithmetic comparisons are not supported in metrics filters".to_string(),
                )),
            }
        }
        // Attribute existence (issue #185 `existence.*`): a key-only
        // membership semi-join. `resource.service.name != nil` and the
        // like are answerable on the metrics surface (the grafana
        // `rate() by(service)` case). The absent form (`= nil`) parses to
        // `Not(Exists)` and is rejected below with the other negations.
        FieldExpr::Exists(field) => {
            let probe = match field {
                Field::Attribute { scope, key } => AttrProbe {
                    key: key.clone(),
                    scope: match scope {
                        AttrScope::Span => Some("span"),
                        AttrScope::Resource => Some("resource"),
                        AttrScope::Unscoped => None,
                    },
                    pred: ValuePred::KeyExists,
                },
                Field::Intrinsic(_) => {
                    return Err(PlanError::TypeMismatch(
                        "existence checks are only supported on attributes".to_string(),
                    ));
                }
            };
            Ok(semi_join_sql(&probe, false, attrs_table, window))
        }
        // Arithmetic comparisons (issue #185) are a search-surface
        // construct; the metrics filter path does not support them yet
        // (a clean 400, mirroring the field-vs-field rejection).
        FieldExpr::ArithCompare { .. } => Err(PlanError::TypeMismatch(
            "arithmetic comparisons are not supported in metrics filters".to_string(),
        )),
        // Field-vs-field comparison, bare boolean statics and unary field
        // negation (issue #183) are search-surface constructs; the metrics
        // filter path does not support them yet (a clean 400, tracked as a
        // follow-up — mirrors the nested-set metrics rejection above).
        FieldExpr::FieldCompare { .. } => Err(PlanError::TypeMismatch(
            "field-vs-field comparisons are not supported in metrics filters".to_string(),
        )),
        FieldExpr::BoolStatic(_) => Err(PlanError::TypeMismatch(
            "bare boolean statics are not supported in metrics filters".to_string(),
        )),
        FieldExpr::Not(_) => Err(PlanError::TypeMismatch(
            "field negation is not supported in metrics filters".to_string(),
        )),
        FieldExpr::Binary { op, lhs, rhs } => {
            let l = render_expr(lhs, attrs_table, window)?;
            let r = render_expr(rhs, attrs_table, window)?;
            let sym = match op {
                BoolOp::And => "AND",
                BoolOp::Or => "OR",
            };
            Ok(format!("({l} {sym} {r})"))
        }
    }
}

fn validate_physical_regex(p: &filter::PhysicalPredicate) -> Result<(), PlanError> {
    let (op, value) = match p {
        filter::PhysicalPredicate::Name { op, value }
        | filter::PhysicalPredicate::Service { op, value }
        | filter::PhysicalPredicate::StatusMessage { op, value }
        | filter::PhysicalPredicate::SpanIdHex { op, value }
        | filter::PhysicalPredicate::ParentIdHex { op, value } => (op, value),
        _ => return Ok(()),
    };
    if matches!(op, ComparisonOp::Re | ComparisonOp::Nre) {
        compile_anchored(value)?;
    }
    Ok(())
}

fn validate_probe_regex(probe: &AttrProbe) -> Result<(), PlanError> {
    if let ValuePred::Regex(pat) = &probe.pred {
        compile_anchored(pat)?;
    }
    Ok(())
}

/// One attribute leaf's index-served membership semi-join, confined to
/// its `(key[, val][, scope])` prefix plus the window's date/time
/// pruning. `negated` renders `NOT IN` around the **positive** predicate
/// — the ratified absent-key rule.
fn semi_join_sql(
    probe: &AttrProbe,
    negated: bool,
    attrs_table: &str,
    window: SnappedWindow,
) -> String {
    let mut predicate = format!("key = {}", escape::ch_string(&probe.key));
    predicate.push_str(&format!(" AND {}", filter::value_pred_sql(&probe.pred)));
    if let Some(scope) = probe.scope {
        predicate.push_str(&format!(" AND scope = {}", escape::ch_string(scope)));
    }
    let membership = if negated { "NOT IN" } else { "IN" };
    format!(
        "(trace_id, span_id) {membership} (SELECT trace_id, span_id FROM {attrs_table} \
         WHERE {} AND {} AND {predicate})",
        date_clause(window),
        time_clause(window)
    )
}

/// The range query — one fully-pushed-down, time-bucketed, replay-deduped
/// conditional aggregation (docs/schemas.md §4.2). `toUnixTimestamp64Milli(...)`
/// pins the bucket column to a deterministic `Int64` epoch-milliseconds wire
/// type (plan v1 edge 2: `toStartOfInterval(DateTime64(9), …)`'s own
/// type/scale is version-sensitive; `Int64` ms also covers pre-1970/post-2106
/// buckets that a `UInt32` epoch-seconds column would wrap — issue #59
/// re-audit). The interval is rendered in **milliseconds**
/// (`INTERVAL {step_ms} MILLISECOND`), not seconds: live ClickHouse 24.8
/// evaluates `toStartOfInterval(DateTime64, INTERVAL n SECOND)` (and
/// MINUTE/HOUR/…) as a 32-bit `DateTime`, silently wrapping/clamping
/// pre-1970/post-2106 instants (verified live — the SQL then also fails
/// `toUnixTimestamp64Milli`'s strict `DateTime64` argument outright, for
/// every window, not only extreme ones); the millisecond-unit form is the
/// documented ClickHouse boundary at which `toStartOfInterval` keeps its
/// `DateTime64` precision/range. `step_ms = step_s * 1000` never overflows
/// `i64`: `metrics_plan::plan_trace_metrics` already requires the snapped
/// window (which is at least one whole step) to fit in `i64` nanoseconds,
/// so `step_s <= i64::MAX / NS_PER_S`.
pub fn metrics_range_sql(
    spans_table: &str,
    filter: &FilterSql,
    window: SnappedWindow,
    step_s: i64,
) -> String {
    let step_ms = step_s * 1000;
    let mut sql = format!(
        "SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), \
         INTERVAL {step_ms} MILLISECOND)) AS t,\n       uniqExact(trace_id, span_id) AS n\n\
         FROM {spans_table}\n"
    );
    if let Some(prewhere) = &filter.prewhere {
        sql.push_str(&format!("PREWHERE {prewhere}\n"));
    }
    sql.push_str(&format!("WHERE {}", time_clause(window)));
    if let Some(where_expr) = &filter.where_expr {
        sql.push_str(&format!("\n  AND {where_expr}"));
    }
    sql.push_str("\nGROUP BY t\nORDER BY t ASC");
    sql
}

/// The instant query — the same body over the whole snapped window
/// `[S, E)` with no `GROUP BY`: exactly one row (`uniqExact` over an
/// empty set is a single `n = 0` row, the documented empty-DB vector
/// oracle). The rate division by the window width happens client-side at
/// the encode boundary, like the range path's division by `step_s`.
pub fn metrics_instant_sql(spans_table: &str, filter: &FilterSql, window: SnappedWindow) -> String {
    let mut sql = format!("SELECT uniqExact(trace_id, span_id) AS n\nFROM {spans_table}\n");
    if let Some(prewhere) = &filter.prewhere {
        sql.push_str(&format!("PREWHERE {prewhere}\n"));
    }
    sql.push_str(&format!("WHERE {}", time_clause(window)));
    if let Some(where_expr) = &filter.where_expr {
        sql.push_str(&format!("\n  AND {where_expr}"));
    }
    sql
}

// ---------------------------------------------------------------------------
// Issue #182: grouped (`by(...)`) counting + first-stage value aggregation
// (`sum/min/max/avg_over_time`). All aggregations nest a per-`(trace_id,
// span_id)` dedup inner query so at-least-once replays never inflate `sum`/
// `avg` (the replay-dedup invariant); `min`/`max`/`count(uniqExact)` are
// replay-idempotent by construction. Counting stays `uniqExact(trace_id,
// span_id)`. This pass lowers the `by(resource.service.name)` grouping to
// the physical `service` column (always present); attribute by-keys and
// attribute value targets route to a follow-up.
// ---------------------------------------------------------------------------

/// One resolved `by(...)` grouping key. `col_expr` is the SQL scalar the
/// query groups on; `label_key` is the Tempo series-label key it becomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupKeySql {
    pub col_expr: String,
    pub label_key: String,
}

/// The first-stage value-aggregation functions (`*_over_time`), issue
/// #182. `count`/`rate` are not here — they are the `uniqExact` count
/// path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    Sum,
    Min,
    Max,
    Avg,
}

impl AggFn {
    fn sql(self) -> &'static str {
        match self {
            AggFn::Sum => "sum",
            AggFn::Min => "min",
            AggFn::Max => "max",
            AggFn::Avg => "avg",
        }
    }
}

/// Renders the `SELECT`-list group columns (`, <expr> AS g0, …`) and the
/// trailing `GROUP BY`/`ORDER BY` group tails for a set of by-keys. Group
/// columns are aliased `g0..gN` so the outer query and the decode-row
/// order are positional and deterministic.
fn group_fragments(keys: &[GroupKeySql]) -> (String, String, String) {
    let mut select = String::new();
    let mut group_by = String::new();
    let mut order_by = String::new();
    for (i, k) in keys.iter().enumerate() {
        select.push_str(&format!(", {} AS g{i}", k.col_expr));
        group_by.push_str(&format!(", g{i}"));
        order_by.push_str(&format!(", g{i}"));
    }
    (select, group_by, order_by)
}

/// The grouped/ungrouped replay-deduped **count** range query (rate and
/// count_over_time). With no by-keys this is the ungrouped
/// [`metrics_range_sql`] shape plus the group columns; `uniqExact` is
/// replay-safe so no inner dedup subquery is needed.
pub fn metrics_count_range_sql(
    spans_table: &str,
    filter: &FilterSql,
    window: SnappedWindow,
    step_s: i64,
    keys: &[GroupKeySql],
) -> String {
    let step_ms = step_s * 1000;
    let (gsel, ggroup, gorder) = group_fragments(keys);
    let mut sql = format!(
        "SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), \
         INTERVAL {step_ms} MILLISECOND)) AS t{gsel},\n       uniqExact(trace_id, span_id) AS n\n\
         FROM {spans_table}\n"
    );
    push_prewhere_where(&mut sql, filter, window);
    sql.push_str(&format!("\nGROUP BY t{ggroup}\nORDER BY t ASC{gorder}"));
    sql
}

/// The grouped/ungrouped **count** instant query (whole snapped window,
/// no time bucket). With by-keys this yields one row per label-set.
pub fn metrics_count_instant_sql(
    spans_table: &str,
    filter: &FilterSql,
    window: SnappedWindow,
    keys: &[GroupKeySql],
) -> String {
    let (gsel, ggroup, gorder) = group_fragments(keys);
    if keys.is_empty() {
        return metrics_instant_sql(spans_table, filter, window);
    }
    let cols = gsel.trim_start_matches(", ");
    let mut sql = format!("SELECT {cols}, uniqExact(trace_id, span_id) AS n\nFROM {spans_table}\n");
    push_prewhere_where(&mut sql, filter, window);
    sql.push_str(&format!(
        "\nGROUP BY {}\nORDER BY {}",
        ggroup.trim_start_matches(", "),
        gorder.trim_start_matches(", ")
    ));
    sql
}

/// The grouped/ungrouped value-aggregation range query
/// (`sum/min/max/avg_over_time`). The inner subquery deduplicates to one
/// value per `(t, group…, trace_id, span_id)` (`any(duration_ns)`); the
/// outer aggregates per `(t, group…)`. Duration is the physical
/// `duration_ns`; the engine scales ns→seconds at the encode boundary.
pub fn metrics_agg_range_sql(
    spans_table: &str,
    filter: &FilterSql,
    window: SnappedWindow,
    step_s: i64,
    agg: AggFn,
    keys: &[GroupKeySql],
) -> String {
    let step_ms = step_s * 1000;
    let (gsel, ggroup, gorder) = group_fragments(keys);
    let mut inner = format!(
        "SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), \
         INTERVAL {step_ms} MILLISECOND)) AS t{gsel}, trace_id, span_id,\n         \
         any(duration_ns) AS val\n  FROM {spans_table}\n  "
    );
    push_prewhere_where_indented(&mut inner, filter, window);
    inner.push_str(&format!("\n  GROUP BY t{ggroup}, trace_id, span_id"));
    format!(
        "SELECT t{ggroup}, toFloat64({}(val)) AS v\nFROM (\n  {inner}\n)\nGROUP BY t{ggroup}\nORDER BY t ASC{gorder}",
        agg.sql()
    )
}

/// The grouped/ungrouped value-aggregation instant query — the same
/// dedup-then-aggregate over the whole snapped window, no time bucket.
pub fn metrics_agg_instant_sql(
    spans_table: &str,
    filter: &FilterSql,
    window: SnappedWindow,
    agg: AggFn,
    keys: &[GroupKeySql],
) -> String {
    let (gsel, ggroup, gorder) = group_fragments(keys);
    if keys.is_empty() {
        let mut inner =
            format!("SELECT trace_id, span_id, any(duration_ns) AS val\n  FROM {spans_table}\n  ");
        push_prewhere_where_indented(&mut inner, filter, window);
        inner.push_str("\n  GROUP BY trace_id, span_id");
        return format!(
            "SELECT toFloat64({}(val)) AS v\nFROM (\n  {inner}\n)",
            agg.sql()
        );
    }
    let cols = gsel.trim_start_matches(", ");
    let group = ggroup.trim_start_matches(", ");
    let order = gorder.trim_start_matches(", ");
    let mut inner = format!(
        "SELECT {cols}, trace_id, span_id, any(duration_ns) AS val\n  FROM {spans_table}\n  "
    );
    push_prewhere_where_indented(&mut inner, filter, window);
    inner.push_str(&format!("\n  GROUP BY {group}, trace_id, span_id"));
    format!(
        "SELECT {group}, toFloat64({}(val)) AS v\nFROM (\n  {inner}\n)\nGROUP BY {group}\nORDER BY {order}",
        agg.sql()
    )
}

/// Renders the `quantilesTDigest(q, …)` argument list from quantile
/// literals (already validated to `[0, 1]` at plan time), each formatted
/// with `ryu`-style shortest round-trip via `f64` `Display`.
fn quantile_args(quantiles: &[f64]) -> String {
    quantiles
        .iter()
        .map(|q| format!("{q}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The ungrouped `quantile_over_time` range query (issue #182, OQ4):
/// `quantilesTDigest(q…)` over the per-`(t, trace_id, span_id)`-deduped
/// physical `duration_ns`, yielding one `Array(Float64)` per bucket
/// (`[q0, q1, …]`, ordered as requested). The engine scales ns→seconds and
/// emits one series per quantile (`p=<q>` label). Grouped quantiles route
/// to a follow-up.
pub fn metrics_quantile_range_sql(
    spans_table: &str,
    filter: &FilterSql,
    window: SnappedWindow,
    step_s: i64,
    quantiles: &[f64],
) -> String {
    let step_ms = step_s * 1000;
    let mut inner = format!(
        "SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), \
         INTERVAL {step_ms} MILLISECOND)) AS t, trace_id, span_id,\n         \
         any(duration_ns) AS val\n  FROM {spans_table}\n  "
    );
    push_prewhere_where_indented(&mut inner, filter, window);
    inner.push_str("\n  GROUP BY t, trace_id, span_id");
    format!(
        "SELECT t, CAST(quantilesTDigest({})(val) AS Array(Float64)) AS qs\nFROM (\n  {inner}\n)\nGROUP BY t\nORDER BY t ASC",
        quantile_args(quantiles)
    )
}

/// The ungrouped `quantile_over_time` instant query — the same
/// dedup-then-TDigest over the whole snapped window, no time bucket.
pub fn metrics_quantile_instant_sql(
    spans_table: &str,
    filter: &FilterSql,
    window: SnappedWindow,
    quantiles: &[f64],
) -> String {
    let mut inner =
        format!("SELECT trace_id, span_id, any(duration_ns) AS val\n  FROM {spans_table}\n  ");
    push_prewhere_where_indented(&mut inner, filter, window);
    inner.push_str("\n  GROUP BY trace_id, span_id");
    format!(
        "SELECT CAST(quantilesTDigest({})(val) AS Array(Float64)) AS qs\nFROM (\n  {inner}\n)",
        quantile_args(quantiles)
    )
}

/// The ungrouped `histogram_over_time` range query (issue #182, OQ4):
/// pushed-down conditional cumulative counts over fixed exponential
/// power-of-two nanosecond `le` boundaries, one column per bucket. The
/// engine emits one cumulative-count series per bucket (`__bucket=<le
/// seconds>` label). Exact bucket-boundary/value parity vs Tempo is
/// Tier-2 (issue #25); this pins the exp-`le` shape.
pub fn metrics_histogram_range_sql(
    spans_table: &str,
    filter: &FilterSql,
    window: SnappedWindow,
    step_s: i64,
    le_bounds_ns: &[i64],
) -> String {
    let step_ms = step_s * 1000;
    let cols = histogram_bucket_cols(le_bounds_ns);
    let mut inner = format!(
        "SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), \
         INTERVAL {step_ms} MILLISECOND)) AS t, trace_id, span_id,\n         \
         any(duration_ns) AS val\n  FROM {spans_table}\n  "
    );
    push_prewhere_where_indented(&mut inner, filter, window);
    inner.push_str("\n  GROUP BY t, trace_id, span_id");
    format!("SELECT t, {cols} AS bkts\nFROM (\n  {inner}\n)\nGROUP BY t\nORDER BY t ASC")
}

/// The ungrouped `histogram_over_time` instant query.
pub fn metrics_histogram_instant_sql(
    spans_table: &str,
    filter: &FilterSql,
    window: SnappedWindow,
    le_bounds_ns: &[i64],
) -> String {
    let cols = histogram_bucket_cols(le_bounds_ns);
    let mut inner =
        format!("SELECT trace_id, span_id, any(duration_ns) AS val\n  FROM {spans_table}\n  ");
    push_prewhere_where_indented(&mut inner, filter, window);
    inner.push_str("\n  GROUP BY trace_id, span_id");
    format!("SELECT {cols} AS bkts\nFROM (\n  {inner}\n)")
}

/// Renders the cumulative per-bucket count array
/// `[countIf(val <= le0), …]` for the histogram's `le` boundaries — one
/// `Array(UInt64)` column, decoded positionally against the boundary
/// list.
fn histogram_bucket_cols(le_bounds_ns: &[i64]) -> String {
    let items = le_bounds_ns
        .iter()
        .map(|le| format!("countIf(val <= {le})"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{items}]")
}

/// The per-bucket exemplar collection query (issue #182 P5): a bounded
/// `groupArraySample(K, seed)` of `(trace_id, timestamp_ns)` per time
/// bucket, pushed down alongside the count aggregation. Rendered only for
/// an ungrouped rate/count query under `with(exemplars=…)`. The fixed
/// seed keeps the sample deterministic (test-stable); exact
/// exemplar-count/selection parity vs Tempo is Tier-2 (issue #25).
pub fn metrics_exemplar_range_sql(
    spans_table: &str,
    filter: &FilterSql,
    window: SnappedWindow,
    step_s: i64,
    k: u32,
) -> String {
    let step_ms = step_s * 1000;
    let mut sql = format!(
        "SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), \
         INTERVAL {step_ms} MILLISECOND)) AS t,\n       \
         groupArraySample({k}, 1)(tuple(trace_id, timestamp_ns)) AS ex\nFROM {spans_table}\n"
    );
    push_prewhere_where(&mut sql, filter, window);
    sql.push_str("\nGROUP BY t\nORDER BY t ASC");
    sql
}

/// The distinct-by-key series-cardinality probe (issue #182, review Fix
/// 2): counts DISTINCT label-sets (never bucket rows) under the same
/// predicate, bounded by `LIMIT cap+1`. The engine issues it before the
/// main query; a result of `cap+1` is a static `422 query_too_broad`.
/// Only rendered when there is at least one by-key.
pub fn metrics_series_probe_sql(
    spans_table: &str,
    filter: &FilterSql,
    window: SnappedWindow,
    keys: &[GroupKeySql],
    cap: u64,
) -> String {
    let cols: Vec<String> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| format!("{} AS g{i}", k.col_expr))
        .collect();
    let group: Vec<String> = (0..keys.len()).map(|i| format!("g{i}")).collect();
    let mut inner = format!("SELECT {}\n  FROM {spans_table}\n  ", cols.join(", "));
    push_prewhere_where_indented(&mut inner, filter, window);
    inner.push_str(&format!(
        "\n  GROUP BY {}\n  LIMIT {}",
        group.join(", "),
        cap + 1
    ));
    format!("SELECT count() AS n FROM (\n  {inner}\n)")
}

/// The spanset-level search `| by(...)` cardinality pre-flight probe
/// (issue #185): the SAME distinct-by-key `GROUP BY <keys> LIMIT cap+1`
/// mechanism as the metric `by()` cap, over the search filter + window.
/// `group_col` is the grouping column expression (currently `service` for
/// `resource.service.name`). The engine counts its rows; `cap+1` is a
/// static `422 query_too_broad` before the main search runs.
pub fn search_by_probe_sql(
    spans_table: &str,
    attrs_table: &str,
    body: Option<&FieldExpr>,
    window: SnappedWindow,
    group_col: &str,
    cap: u64,
) -> Result<String, PlanError> {
    // `trace_spans` prunes on `timestamp_ns` only (no `date` column — that
    // partition column lives on `trace_attrs_idx`, and each attr semi-join
    // inside `filter_bool` carries its own date/time pruning internally).
    let filter_bool = compile_filter_bool(body, attrs_table, window)?;
    let inner = format!(
        "SELECT {group_col} AS g0\n  FROM {spans_table}\n  WHERE {} AND ({filter_bool})\
         \n  GROUP BY g0\n  LIMIT {}",
        time_clause(window),
        cap + 1
    );
    Ok(format!("SELECT count() AS n FROM (\n  {inner}\n)"))
}

/// The three SQL forms `compare()` needs (issue #182 P6b): the
/// per-`(bucket, attr_key, attr_value)` baseline/selection cross-tab, the
/// per-bucket totals (the `*_total` denominators + the `key=nil`
/// complement), and the distinct-`(key, value)` cardinality probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompareSql {
    pub cross_tab: String,
    pub totals: String,
    pub probe: String,
}

/// Maps the physical `kind`/`status_code` `Int8` columns to the TraceQL
/// intrinsic string values (Tempo's `kind`/`status` intrinsic rendering).
const KIND_MAP: &str = "transform(i_kind, [0, 1, 2, 3, 4, 5], ['unspecified', 'internal', 'server', 'client', \
     'producer', 'consumer'], 'unspecified')";
const STATUS_MAP: &str = "transform(i_status, [0, 1, 2], ['unset', 'ok', 'error'], 'unset')";

/// The inputs to [`metrics_compare_sql`] — bundled to keep the builder's
/// signature within the argument limit.
#[derive(Debug, Clone, Copy)]
pub struct CompareSqlInput<'a> {
    pub spans_table: &'a str,
    pub attrs_table: &'a str,
    pub outer: &'a FilterSql,
    /// The pre-compiled selection predicate (`compile_filter_bool`).
    pub inner_bool: &'a str,
    pub window: SnappedWindow,
    /// The bucket-start expression aliased `t` (the `toStartOfInterval`
    /// form for range, a literal ms for instant).
    pub bucket_expr: &'a str,
    /// The distinct-series cap (`reader.traceql_max_series`).
    pub cap: u64,
    /// The fixed well-known-attribute series count folded into the cap.
    pub fixed_series: u64,
}

/// Builds the compare() SQL trio. The cross-tab enumerates the present
/// attributes — the `name`/`kind`/`status`/`resource.service.name`
/// intrinsics plus every scoped `trace_attrs_idx` `(scope.key, val)` — and
/// counts them in the baseline complement (`countIf(is_sel = 0)`) and the
/// selection (`countIf(is_sel)`). The well-known-absent universe is folded
/// in engine-side (`frame_compare`); the cap probe bounds the true output
/// series count.
pub fn metrics_compare_sql(input: &CompareSqlInput<'_>) -> CompareSql {
    let CompareSqlInput {
        spans_table,
        attrs_table,
        outer,
        inner_bool,
        window,
        bucket_expr,
        cap,
        fixed_series,
    } = *input;
    let mut raw = format!(
        "SELECT {bucket_expr} AS t, trace_id, span_id, name AS i_name, kind AS i_kind, \
         status_code AS i_status, service AS i_service, status_message AS i_status_message, \
         ({inner_bool}) AS is_sel\n    FROM {spans_table}\n    "
    );
    push_prewhere_where_indented(&mut raw, outer, window);
    // Replay-dedup: one row per (t, trace_id, span_id) so at-least-once
    // replays never inflate the baseline/selection counts (mirrors the
    // `uniqExact` rule on the count path).
    let base = format!(
        "SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, \
         any(i_status) AS i_status, any(i_service) AS i_service, \
         any(i_status_message) AS i_status_message, max(is_sel) AS is_sel\n  FROM (\n  {raw}\n  )\n  GROUP BY t, trace_id, span_id"
    );
    // Issue #189: `rootName`/`rootServiceName` are trace-level intrinsics —
    // resolved WINDOW-FREE (trace-wide) so they never disagree with the
    // #184 search path's roots, then LEFT JOINed on trace_id into the
    // intrinsics branch only. `statusMessage` sources the per-span
    // `status_message` PHYSICAL column already carried in `base`: every span
    // has a `""`-or-value (there is no absent case), and Tempo v3.0.2's
    // compare() emits an empty `statusMessage` as a DISTINCT `""` value (not
    // folded into the `key=nil` complement — verified against the pinned
    // reference, #185). So it is emitted verbatim like every other
    // intrinsic; `name`/`kind`/`status`/`resource.service.name` and the
    // window-free roots are byte-unchanged.
    let roots_cte = compare_roots_cte(spans_table, &base);
    let intrinsics = format!(
        "SELECT t, is_sel, kv.1 AS akey, kv.2 AS aval FROM (\n    \
         SELECT t, is_sel, arrayJoin([\
         ('name', i_name), ('kind', {KIND_MAP}), ('status', {STATUS_MAP}), \
         ('resource.service.name', i_service), ('statusMessage', i_status_message), \
         ('rootName', r.root_name), ('rootServiceName', r.root_service)]) AS kv\n    \
         FROM (\n  {base}\n    ) b\n    LEFT JOIN (\n  {roots_cte}\n    ) r ON b.trace_id = r.trace_id\n  )"
    );
    let index_attrs = format!(
        "SELECT b.t AS t, b.is_sel AS is_sel, concat(a.scope, '.', a.key) AS akey, a.val AS aval\n  \
         FROM (\n  {base}\n  ) b\n  INNER JOIN (\n    SELECT DISTINCT trace_id, span_id, scope, key, val \
         FROM {attrs_table} WHERE {} AND {}\n  ) a ON b.trace_id = a.trace_id AND b.span_id = a.span_id",
        date_clause(window),
        time_clause(window)
    );
    let union = format!("{intrinsics}\n  UNION ALL\n  {index_attrs}");
    // `baseline` is the COMPLEMENT of the selection (spans NOT matching the
    // inner filter), `selection` the matching spans — the captured Tempo
    // convention (a selection value never appears under `baseline`). The
    // `_total` denominators count each population.
    let cross_tab = format!(
        "SELECT t, akey, aval, countIf(is_sel = 0) AS base_n, countIf(is_sel) AS sel_n\nFROM (\n  {union}\n)\n\
         GROUP BY t, akey, aval\nORDER BY t ASC, akey, aval"
    );
    let totals = format!(
        "SELECT t, countIf(is_sel = 0) AS base_total, countIf(is_sel) AS sel_total\nFROM (\n  {base}\n)\n\
         GROUP BY t\nORDER BY t ASC"
    );
    // The cap must bound the ACTUAL materialized output-series count, not
    // just distinct (key,value) pairs (issue #182 review Fix 2): framing
    // emits 2 series/pair (baseline + selection) + 4 series/key
    // (baseline/selection `key=nil` + `*_total`). The probe computes
    // `2·pairs + 4·keys`, bounding the scan by `LIMIT cap+1` on the
    // distinct pairs so `pairs > cap` short-circuits to a reject. (The
    // fixed well-known-absent-attribute set adds a bounded ≤ 4·25 series
    // on top — a small constant, not attacker-controlled.)
    let probe = format!(
        "SELECT toUInt64(pairs * 2 + keys * 4 + {fixed_series}) AS n FROM (\n  SELECT count() AS pairs, \
         uniqExact(akey) AS keys FROM (\n  SELECT akey, aval FROM (\n  {union}\n) GROUP BY akey, aval \
         LIMIT {}\n)\n)",
        cap + 1
    );
    CompareSql {
        cross_tab,
        totals,
        probe,
    }
}

/// The window-free per-trace roots read for `compare()` (issue #189): one
/// `argMin(byte_cap_expr(col), root_ordering_tuple())` per trace over
/// `spans_table`, restricted to the in-window `SELECT DISTINCT trace_id
/// FROM base` IN-set but carrying **no date/time predicate** — the whole
/// point of the trace-wide contract (docs/schemas.md §Phase-2
/// trace-context co-load; [`super::search_sql::trace_ctx_sql`]). Both
/// projections reuse the search path's [`byte_cap_expr`] and its
/// [`root_ordering_tuple`], so `rootName`/`rootServiceName` here are
/// byte-identical to what search returns. Bounded by the metrics IN-set
/// (`max_rows_in_set`) and scan (`max_rows_to_read`) throw budgets; scale
/// routes to #25.
fn compare_roots_cte(spans_table: &str, base: &str) -> String {
    let ordering = root_ordering_tuple();
    format!(
        "SELECT trace_id, argMin({}, {ordering}) AS root_name, \
         argMin({}, {ordering}) AS root_service\n  FROM {spans_table}\n  \
         WHERE trace_id IN (SELECT DISTINCT trace_id FROM (\n  {base}\n  ))\n  GROUP BY trace_id",
        byte_cap_expr("name"),
        byte_cap_expr("service"),
    )
}

/// The range-form bucket-start expression (`toStartOfInterval` → ms) for
/// compare().
pub fn compare_range_bucket_expr(step_s: i64) -> String {
    let step_ms = step_s * 1000;
    format!(
        "toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), \
         INTERVAL {step_ms} MILLISECOND))"
    )
}

/// Appends the `PREWHERE`/`WHERE` fragments at the top-level indentation.
fn push_prewhere_where(sql: &mut String, filter: &FilterSql, window: SnappedWindow) {
    if let Some(prewhere) = &filter.prewhere {
        sql.push_str(&format!("PREWHERE {prewhere}\n"));
    }
    sql.push_str(&format!("WHERE {}", time_clause(window)));
    if let Some(where_expr) = &filter.where_expr {
        sql.push_str(&format!("\n  AND {where_expr}"));
    }
}

/// Appends the `PREWHERE`/`WHERE` fragments at the nested (2-space)
/// indentation used inside the dedup/probe subqueries.
fn push_prewhere_where_indented(sql: &mut String, filter: &FilterSql, window: SnappedWindow) {
    if let Some(prewhere) = &filter.prewhere {
        sql.push_str(&format!("PREWHERE {prewhere}\n  "));
    }
    sql.push_str(&format!("WHERE {}", time_clause(window)));
    if let Some(where_expr) = &filter.where_expr {
        sql.push_str(&format!("\n    AND {where_expr}"));
    }
}

#[cfg(test)]
mod tests {
    use pulsus_traceql::parse;

    use super::*;

    const W: SnappedWindow = SnappedWindow {
        start_ns: 1_699_999_980_000_000_000,
        end_ns: 1_700_010_840_000_000_000,
    };

    fn body(q: &str) -> FieldExpr {
        match parse(q).expect("parse").spanset {
            pulsus_traceql::SpansetExpr::Filter(f) => f.body.expect("non-empty filter"),
            other => panic!("expected a single spanset filter, got {other:?}"),
        }
    }

    fn compile(q: &str) -> FilterSql {
        compile_filter_predicate(Some(&body(q)), "trace_attrs_idx", W).expect("compiles")
    }

    #[test]
    fn match_all_compiles_to_no_fragments() {
        let f = compile_filter_predicate(None, "trace_attrs_idx", W).unwrap();
        assert_eq!(f.prewhere, None);
        assert_eq!(f.where_expr, None);
    }

    #[test]
    fn a_root_spine_service_equality_hoists_to_prewhere() {
        let f = compile(r#"{ resource.service.name = "checkout" && duration > 2s }"#);
        assert_eq!(f.prewhere.as_deref(), Some("service = 'checkout'"));
        assert_eq!(f.where_expr.as_deref(), Some("duration_ns > 2000000000"));
    }

    #[test]
    fn a_lone_service_equality_hoists_with_no_residual_where() {
        let f = compile(r#"{ resource.service.name = "checkout" }"#);
        assert_eq!(f.prewhere.as_deref(), Some("service = 'checkout'"));
        assert_eq!(f.where_expr, None);
    }

    #[test]
    fn a_deep_root_and_spine_service_leaf_still_hoists() {
        let f = compile(r#"{ (resource.service.name = "a" && duration > 1s) && status = error }"#);
        assert_eq!(f.prewhere.as_deref(), Some("service = 'a'"));
        assert_eq!(
            f.where_expr.as_deref(),
            Some("(duration_ns > 1000000000 AND status_code = 2)")
        );
    }

    fn compile_bool(q: &str) -> String {
        compile_filter_bool(Some(&body(q)), "trace_attrs_idx", W).expect("compiles")
    }

    #[test]
    fn attribute_existence_renders_a_key_only_semi_join_on_the_metrics_surface() {
        // Issue #185: `resource.service.name != nil` (the grafana
        // `rate() by(service)` idiom, the code path the replay-ledger
        // deletion depends on) renders a key-only membership semi-join into
        // the attr index — NOT a value predicate.
        let sql = compile_bool(r#"{ resource.service.name != nil }"#);
        assert!(sql.contains("(trace_id, span_id) IN"), "{sql}");
        assert!(sql.contains("FROM trace_attrs_idx"), "{sql}");
        assert!(sql.contains("key = 'service.name'"), "{sql}");
        assert!(sql.contains("scope = 'resource'"), "{sql}");
        assert!(
            sql.contains("AND 1"),
            "the key-only (no value) predicate: {sql}"
        );
        // Unscoped existence: no scope predicate.
        let unscoped = compile_bool(r#"{ .a != nil }"#);
        assert!(unscoped.contains("key = 'a' AND 1"), "{unscoped}");
        assert!(!unscoped.contains("scope ="), "{unscoped}");
    }

    #[test]
    fn absent_existence_and_intrinsic_existence_are_metrics_filter_type_mismatches() {
        // `= nil` is `Not(Exists)` — negation is unsupported on the metrics
        // filter path (a clean 400).
        assert!(matches!(
            compile_filter_bool(Some(&body(r#"{ .a = nil }"#)), "trace_attrs_idx", W),
            Err(PlanError::TypeMismatch(_))
        ));
        // Intrinsic existence is not an attribute — rejected.
        assert!(matches!(
            compile_filter_bool(Some(&body(r#"{ name != nil }"#)), "trace_attrs_idx", W),
            Err(PlanError::TypeMismatch(_))
        ));
    }

    #[test]
    fn a_service_equality_under_an_or_is_never_hoisted() {
        // Plan v2 delta 4: Or nodes are opaque — hoisting either side
        // would drop matches of the other.
        let f = compile(
            r#"{ (resource.service.name = "a" || resource.service.name = "b") && duration > 1s }"#,
        );
        assert_eq!(f.prewhere, None);
        assert_eq!(
            f.where_expr.as_deref(),
            Some("((service = 'a' OR service = 'b') AND duration_ns > 1000000000)")
        );
    }

    #[test]
    fn only_the_first_spine_service_leaf_hoists_the_rest_render_inline() {
        let f = compile(r#"{ resource.service.name = "a" && resource.service.name = "b" }"#);
        assert_eq!(f.prewhere.as_deref(), Some("service = 'a'"));
        assert_eq!(f.where_expr.as_deref(), Some("service = 'b'"));
    }

    #[test]
    fn service_inequality_is_never_prewhere_eligible() {
        let f = compile(r#"{ resource.service.name != "a" }"#);
        assert_eq!(f.prewhere, None);
        assert_eq!(f.where_expr.as_deref(), Some("service != 'a'"));
    }

    #[test]
    fn an_attr_leaf_renders_an_index_served_semi_join() {
        let f = compile("{ span.http.status_code >= 500 }");
        let expr = f.where_expr.expect("where");
        assert!(
            expr.starts_with(
                "(trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx"
            )
        );
        assert!(expr.contains("date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')"));
        assert!(expr.contains(
            "timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000"
        ));
        assert!(expr.contains("key = 'http.status_code' AND val_num >= 500 AND scope = 'span'"));
    }

    #[test]
    fn a_negated_attr_renders_not_in_around_the_positive_predicate() {
        let f = compile(r#"{ .env != "prod" }"#);
        let expr = f.where_expr.expect("where");
        assert!(expr.contains("(trace_id, span_id) NOT IN (SELECT"));
        assert!(expr.contains("key = 'env' AND val = 'prod'"));
        assert!(
            !expr.contains("scope ="),
            "the unscoped form carries no scope clause (dual-scope negation): {expr}"
        );
    }

    #[test]
    fn the_date_clause_end_day_comes_from_the_last_included_nanosecond() {
        // A window ending exactly at midnight must not include the next
        // day's partition.
        let w = SnappedWindow {
            start_ns: 1_699_920_000_000_000_000, // 2023-11-14 00:00:00
            end_ns: 1_700_006_400_000_000_000,   // 2023-11-15 00:00:00 (excluded)
        };
        assert_eq!(
            date_clause(w),
            "date >= toDate('2023-11-14') AND date <= toDate('2023-11-14')"
        );
    }

    #[test]
    fn an_invalid_attr_regex_fails_at_compile_time() {
        let expr = body(r#"{ .k =~ "(" }"#);
        let err = compile_filter_predicate(Some(&expr), "trace_attrs_idx", W).unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch(_)));
    }

    #[test]
    fn an_invalid_service_regex_fails_at_compile_time() {
        let expr = body(r#"{ resource.service.name =~ "(" }"#);
        let err = compile_filter_predicate(Some(&expr), "trace_attrs_idx", W).unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch(_)));
    }

    #[test]
    fn injection_in_a_hoisted_service_value_is_neutralized() {
        let f = compile(
            r#"{ resource.service.name = "x'; DROP TABLE trace_spans; --" && duration > 1s }"#,
        );
        assert_eq!(
            f.prewhere.as_deref(),
            Some(r"service = 'x\'; DROP TABLE trace_spans; --'")
        );
    }

    #[test]
    fn range_sql_pins_the_bucket_wrapper_dedup_count_and_bounds() {
        let f = compile(r#"{ resource.service.name = "checkout" && duration > 2s }"#);
        let sql = metrics_range_sql("trace_spans", &f, W, 60);
        assert!(sql.starts_with(
            "SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), \
             INTERVAL 60000 MILLISECOND)) AS t,\n       uniqExact(trace_id, span_id) AS n\n\
             FROM trace_spans\nPREWHERE service = 'checkout'\n"
        ));
        assert!(sql.contains(
            "WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000"
        ));
        assert!(sql.ends_with("GROUP BY t\nORDER BY t ASC"));
        assert!(!sql.contains("count()"), "counting is always uniqExact");
    }

    #[test]
    fn instant_sql_is_the_same_body_without_bucketing() {
        let f = compile(r#"{ resource.service.name = "checkout" }"#);
        let sql = metrics_instant_sql("trace_spans", &f, W);
        assert!(sql.starts_with("SELECT uniqExact(trace_id, span_id) AS n\nFROM trace_spans\n"));
        assert!(!sql.contains("GROUP BY"));
        assert!(!sql.contains("toStartOfInterval"));
    }
}
