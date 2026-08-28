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

use pulsus_traceql::{AttrScope, BoolOp, ComparisonOp, Field, FieldExpr, FieldOp, Value};

use crate::logql::escape;

use super::filter::{
    self, AttrProbe, CompiledLeaf, LeafEval, NestedSetField, PlanError, ValuePred,
};
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
/// Regexes are validated at plan time — since issue #282 by the act of
/// rendering them (`filter.rs`'s checked escaper), not by a separate
/// pre-check — so a bad pattern is a `400`, never a mid-query server
/// error.
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
        // The hoistable leaf, now expressed over the collapsed node:
        // `resource.service.name = "…"` is a `Cmp(Eq)` whose sides are a
        // resource-scoped attribute and a string literal.
        FieldExpr::Binary {
            op: FieldOp::Cmp(ComparisonOp::Eq),
            lhs,
            rhs,
        } if matches!(
            (lhs.as_ref(), rhs.as_ref()),
            (
                FieldExpr::Field(Field::Attribute {
                    scope: AttrScope::Resource,
                    key,
                }),
                FieldExpr::Literal(Value::String(_)),
            ) if key == "service.name"
        ) =>
        {
            let FieldExpr::Literal(Value::String(s)) = rhs.as_ref() else {
                unreachable!("guarded by the match arm above");
            };
            (Some(format!("service = {}", escape::ch_string(s))), None)
        }
        FieldExpr::Binary {
            op: FieldOp::Bool(BoolOp::And),
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
            op: FieldOp::Bool(BoolOp::And),
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

/// A constant-folded filter subtree's SQL (issue #351): the same `1` the
/// `{ }` match-all renders, or `0` for the empty one.
fn static_bool_sql(value: bool) -> String {
    if value { "1" } else { "0" }.to_string()
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
        // The comparison arms, now dispatched on operand SHAPE (the
        // collapse moved this out of the parser). Only the
        // field-vs-literal shape has metrics SQL; the others are the same
        // clean 400s they were before.
        FieldExpr::Binary {
            op: FieldOp::Cmp(op),
            lhs,
            rhs,
        } => {
            // Issue #351: two STATIC operands fold to a constant, exactly
            // as they do on the search route (`filter::compile_leaf`'s
            // sibling arm) — `{ "x" = "y" } | rate()` is a reference 200
            // with no series and `{ "x" = "x" } | rate()` the same series
            // as `{ } | rate()`.
            if let (FieldExpr::Literal(l), FieldExpr::Literal(r)) = (lhs.as_ref(), rhs.as_ref()) {
                return Ok(static_bool_sql(filter::fold_static_compare(l, *op, r)?));
            }
            let (FieldExpr::Field(field), FieldExpr::Literal(value)) = (lhs.as_ref(), rhs.as_ref())
            else {
                return Err(PlanError::TypeMismatch(
                    "field-vs-field and arithmetic comparisons are not supported in metrics \
                     filters"
                        .to_string(),
                ));
            };
            let leaf = filter::compile_leaf(field, *op, value)?;
            lower_leaf(&leaf, attrs_table, window)
        }
        // Attribute existence (issue #185 `existence.*`): a key-only
        // membership semi-join. `resource.service.name != nil` and the
        // like are answerable on the metrics surface (the grafana
        // `rate() by(service)` case). The absent form (`= nil`) parses to
        // `Not(Exists)` and is rejected below with the other negations.
        FieldExpr::Exists {
            field,
            negated: false,
        } => {
            let probe = match field {
                Field::Attribute { scope, key } => AttrProbe {
                    key: key.clone(),
                    scope: match scope {
                        AttrScope::Span => Some("span"),
                        AttrScope::Resource => Some("resource"),
                        AttrScope::Unscoped => None,
                        AttrScope::Instrumentation => Some("instrumentation"),
                        AttrScope::Event => Some("event"),
                        AttrScope::Link => Some("link"),
                    },
                    pred: ValuePred::KeyExists,
                },
                Field::Intrinsic(_) => {
                    return Err(PlanError::TypeMismatch(
                        "existence checks are only supported on attributes".to_string(),
                    ));
                }
            };
            semi_join_sql(&probe, false, attrs_table, window)
        }
        // `= nil` (absence) has no positive membership semi-join — the
        // same rejection the `Not(Exists)` shape used to take.
        FieldExpr::Exists { negated: true, .. } => Err(PlanError::TypeMismatch(
            "absence checks are not supported in metrics filters".to_string(),
        )),
        // Issue #458: `{ .a }` IS `.a = true` — plain equality against the
        // boolean literal, which `filter::compile_leaf` already lowers to
        // the ordinary index-served attribute semi-join, so this arm
        // inherits that lowering rather than inventing one. `filter.rs`'s
        // `LeafEval::BoolTruth` doc records the measured asymmetry that
        // makes this sound: `{ .a }` is equality and only `{ !.a }` demands
        // a boolean operand (the reference answers `{ .a }` 200 with no
        // match against a string-valued `a`, and `{ !.a }` **500**).
        //
        // **No client sends this; it is included because the block is
        // open.** The refusal that a live client hit is the nested-set one
        // below; this one was found by enumerating the block against the
        // reference and costs three lines inside a `match` already being
        // changed.
        FieldExpr::Field(field @ Field::Attribute { .. }) => {
            let leaf = filter::compile_leaf(field, ComparisonOp::Eq, &Value::Bool(true))?;
            lower_leaf(&leaf, attrs_table, window)
        }
        // A bare INTRINSIC at predicate position (`{ name }`) never reaches
        // here: `pulsus_traceql::validate` rejects it with the reference's
        // own rule, message and 400 (`span filter field expressions must
        // resolve to a boolean`). Kept as a clean refusal for the
        // AST-constructed path.
        FieldExpr::Field(Field::Intrinsic(_)) => Err(PlanError::TypeMismatch(
            "bare field truthiness is not supported in metrics filters".to_string(),
        )),
        // Issue #351: `{ true }` is the corpus's canonical "match
        // everything" filter and is EXACTLY `{ }` in the reference —
        // `pkg/traceql/ast.go:459-469` @ v3.0.2 matches a span iff the
        // filter expression executes to boolean `true`, and a `Static`
        // executes to itself (`ast_execute.go:885-887`); the fetch layer
        // even special-cases it, appending a match-all condition when the
        // body is a `Static` whose `Bool()` is true
        // (`ast_conditions.go:13-31`, comment: "For empty spansets { }
        // ensure there is something that matches all spans"). So `{ true }`
        // renders `1` and `{ false }` renders `0`, exactly as
        // `compile_filter_bool(None)` renders `1` for `{ }`.
        //
        // A NON-boolean static (`{ 1 }`, `{ "x" }`) never reaches here:
        // `pulsus_traceql::validate` rejects it as `span filter field
        // expressions must resolve to a boolean`, the reference's own
        // message and its own 400.
        FieldExpr::Literal(Value::Bool(b)) => Ok(static_bool_sql(*b)),
        FieldExpr::Literal(_) => Err(PlanError::TypeMismatch(
            "bare boolean statics are not supported in metrics filters".to_string(),
        )),
        FieldExpr::Unary { .. } => Err(PlanError::TypeMismatch(
            "field negation is not supported in metrics filters".to_string(),
        )),
        // Arithmetic at predicate position is likewise unsupported here.
        FieldExpr::Binary {
            op: FieldOp::Arith(_),
            ..
        } => Err(PlanError::TypeMismatch(
            "arithmetic comparisons are not supported in metrics filters".to_string(),
        )),
        FieldExpr::Binary {
            op: FieldOp::Bool(op),
            lhs,
            rhs,
        } => {
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

/// Lowers ONE compiled leaf to its metrics SQL fragment. Extracted from
/// `render_expr`'s comparison arm (issue #458) so the bare-truthiness
/// arm reaches the same dispatch — one leaf lowering, not two that can
/// drift.
///
/// The six `LeafEval` variants that are not lowered here are
/// **unreachable from `filter::compile_leaf`**, which constructs only
/// `Physical`, `Attr`, `NestedSet` and `TraceCtx`. That claim is a gate,
/// not a reading: `dead_leaf_eval_variants_are_unreachable_from_compile_leaf`
/// below enumerates the whole `Field × ComparisonOp × Value` product with
/// exhaustive matches, so adding a variant to any of those enums fails to
/// compile rather than silently narrowing the checked domain.
fn lower_leaf(
    leaf: &CompiledLeaf,
    attrs_table: &str,
    window: SnappedWindow,
) -> Result<String, PlanError> {
    match &leaf.eval {
        // Issue #282: the renderers below validate every regex as they
        // escape it, so the two separate pre-render validators this arm
        // used to call are gone — one act, no second opinion to drift
        // from the emitted SQL.
        LeafEval::Physical(p) => filter::physical_sql(p),
        LeafEval::Attr { probe, negated } => semi_join_sql(probe, *negated, attrs_table, window),
        // Issue #458: the root/non-root region of `nestedSetParent` has an
        // exact per-span SQL form; everything else in the family keeps a
        // clean 400 (see `nested_set_metrics_sql`).
        LeafEval::NestedSet { field, op, value } => nested_set_metrics_sql(*field, *op, *value),
        // Trace-level intrinsics (issue #184) resolve from the search
        // engine's per-trace co-load — no per-span SQL column exists on
        // the metrics filter path (a clean 400; search remains their
        // surface). Wave 2 of issue #458 owns this one.
        LeafEval::TraceCtx(_) => Err(PlanError::TypeMismatch(
            "trace-level intrinsics are not supported in metrics filters".to_string(),
        )),
        // Issue #458 collapsed six separately-worded refusals into this
        // one arm. Every variant it covers is unreachable from
        // `filter::compile_leaf` (gated, see the doc comment above), so
        // the wording no longer pretends to describe a query a user can
        // write.
        LeafEval::BoolTruth { .. }
        | LeafEval::FieldCompare { .. }
        | LeafEval::Arith { .. }
        | LeafEval::Const(_)
        | LeafEval::BoolCompare { .. }
        | LeafEval::EventSetCompare { .. } => Err(PlanError::TypeMismatch(
            "unsupported metrics filter leaf".to_string(),
        )),
    }
}

/// The `nestedSetParent == -1` root sentinel, as a per-span SQL predicate.
///
/// The reference materialises the nested-set numbering per span at block
/// write time and assigns the root sentinel `-1` from `span.IsRoot()`
/// alone — an empty `ParentSpanID`
/// (`tempodb/encoding/vparquet4/nested_set_model.go:11-12,57` @ v3.0.2).
/// Our writer stores the same "no parent" convention as the all-zero
/// `parent_id` (`otlp_traces.rs`), so the root test is one
/// `FixedString(8)` column comparison — no join, no subquery, no window
/// dependence, and nothing for the planner to hoist away from the
/// `service_time` PREWHERE.
fn is_root_sql() -> String {
    format!("parent_id = {}", filter::ZERO_PARENT_SQL)
}

/// Whether `x OP value` is CONSTANT over the whole non-root domain.
///
/// `nestedSetParent` takes values in `{-1} ∪ [1, ∞)`: `-1` for a root
/// (`nested_set_model.go:11-12` @ v3.0.2) and otherwise the parent's Euler
/// `left`, which is at least 1 and whose exact value is not computable in
/// SQL. A comparison is therefore decidable per span iff its truth value
/// does not depend on WHICH non-root value the span carries. `None` = not
/// decidable = the comparison keeps its 400.
///
/// Non-finite values never reach here through `filter::compile_leaf`
/// (`parse_num` rejects them first), but the function is total anyway:
/// with `v = NaN` every comparison against `1.0` is false, so every arm
/// yields `None` — a conservative refusal, never a wrong lowering.
fn nonroot_constant(op: ComparisonOp, v: f64) -> Option<bool> {
    match op {
        // x == v is constant-false over x >= 1 iff v < 1.
        ComparisonOp::Eq => (v < 1.0).then_some(false),
        ComparisonOp::Neq => (v < 1.0).then_some(true),
        // x < v is constant-false over x >= 1 iff v <= 1.
        ComparisonOp::Lt => (v <= 1.0).then_some(false),
        ComparisonOp::Lte => (v < 1.0).then_some(false),
        ComparisonOp::Gt => (v < 1.0).then_some(true),
        ComparisonOp::Gte => (v <= 1.0).then_some(true),
        // `compile_leaf` rejects these first, at `compile_nested_set_leaf`
        // (`filter.rs`), with `nested-set intrinsics do not support regex
        // operators` — this arm is the total-function tail, not a
        // reachable refusal.
        ComparisonOp::Re | ComparisonOp::Nre => None,
    }
}

/// `nestedSetLeft`/`nestedSetRight` have no SQL form at all (the Euler
/// numbering is a per-trace tree walk over the hydrated forest, which the
/// single-query metrics pushdown does not build); `nestedSetParent` lowers
/// when and only when [`nonroot_constant`] decides.
///
/// The root-side truth comes from the search engine's OWN comparator
/// (`search_eval::cmp_f64`) applied to the sentinel, so the metrics and
/// search answers cannot drift by a re-spelled operator.
fn nested_set_metrics_sql(
    field: NestedSetField,
    op: ComparisonOp,
    value: f64,
) -> Result<String, PlanError> {
    if field != NestedSetField::Parent {
        return Err(PlanError::TypeMismatch(
            "nestedSetLeft and nestedSetRight are not supported in metrics filters".to_string(),
        ));
    }
    let root_true = super::search_eval::cmp_f64(op, -1.0, value);
    let Some(nonroot_true) = nonroot_constant(op, value) else {
        return Err(PlanError::TypeMismatch(
            "nestedSetParent comparisons inside the numbering range are not supported in \
             metrics filters"
                .to_string(),
        ));
    };
    Ok(match (root_true, nonroot_true) {
        (true, true) => static_bool_sql(true),
        (false, false) => static_bool_sql(false),
        (true, false) => is_root_sql(),
        (false, true) => format!("NOT ({})", is_root_sql()),
    })
}

/// One attribute leaf's index-served membership semi-join, confined to
/// its `(key[, val][, scope])` prefix plus the window's date/time
/// pruning. `negated` renders `NOT IN` around the **positive** predicate
/// — the ratified absent-key rule. Fallible since issue #282: the
/// positive predicate is rendered by the checked escaper (a negated regex
/// leaf still renders — and therefore still validates — its positive
/// form).
fn semi_join_sql(
    probe: &AttrProbe,
    negated: bool,
    attrs_table: &str,
    window: SnappedWindow,
) -> Result<String, PlanError> {
    let mut predicate = format!("key = {}", escape::ch_string(&probe.key));
    predicate.push_str(&format!(" AND {}", filter::value_pred_sql(&probe.pred)?));
    if let Some(scope) = probe.scope {
        predicate.push_str(&format!(" AND scope = {}", escape::ch_string(scope)));
    }
    let membership = if negated { "NOT IN" } else { "IN" };
    Ok(format!(
        "(trace_id, span_id) {membership} (SELECT trace_id, span_id FROM {attrs_table} \
         WHERE {} AND {} AND {predicate})",
        date_clause(window),
        time_clause(window)
    ))
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

/// The pushed-down `Log2Bucketize` — see
/// [`metrics_log2_bucket_range_sql`] for why each piece is what it is.
/// Its Rust twin is [`super::log2_histogram::log2_bucketize_ns`].
const LOG2_BUCKET_EXPR: &str = "toUInt64(roundToExp2(val - 1)) * 2";

/// The ungrouped `histogram_over_time` log2-bucket tally range query
/// (issue #252): the reference's `Log2Bucketize`
/// (`pkg/traceql/engine_metrics.go:2038-2046 @ v3.0.2` — the smallest
/// power of two `>= v`) pushed down as
/// `toUInt64(roundToExp2(val - 1)) * 2` over the per-`(t, trace_id,
/// span_id)` replay-deduped `duration_ns`, one row per OCCUPIED
/// `(t, bucket)`. There is no bucket ladder: the reference's `__bucket`
/// is a plain `by`-key whose series is created on first observation
/// (`engine_metrics.go:788-793`), and each tally is a plain count
/// (`CountOverTimeAggregator`, `:471-477`), never cumulative.
///
/// **`WHERE val >= 2` sits on the OUTER query, after the dedup**, for
/// two reasons: it reproduces the reference's sub-2ns drop
/// (`ast_metrics.go:181-188`) against the deduped value rather than the
/// raw rows, and it keeps the inner subquery byte-identical to
/// [`metrics_agg_range_sql`]'s, so the PREWHERE hoist, `service_time`
/// projection selection and `trace_attrs_idx` granule pruning are
/// unchanged. The guard is also what excludes negatives, and it fails
/// SILENTLY without it: measured on 24.8.14.39, `roundToExp2` over
/// `Int64` returns `0` for every argument `<= 0`, so a `-1`, `0` or
/// `1` ns duration lands in a spurious `__bucket = 0` series rather than
/// raising anything.
///
/// `roundToExp2` rounds DOWN and `roundUpToPowerOfTwo` does not exist on
/// ClickHouse 24.8, hence the `(val - 1) * 2` form. The `toUInt64`
/// before the doubling is required, not cosmetic: for `val` in
/// `2^62 + 1 ..= i64::MAX` the reference's bucket is `2^63`, which the
/// signed form wraps to a NEGATIVE `__bucket` label. Verified on
/// 24.8.14.39 at `2`, `3`, `536870912`, `2^62`, `2^62 + 1` and
/// `i64::MAX`; the domain is `2..=i64::MAX` because `duration_ns` is
/// `Int64` (`crates/pulsus-schema/src/catalog.rs:347`), and nothing is
/// claimed above it.
///
/// The returned row count per step is bounded by the bit width of the
/// duration, not by any property of the corpus: an `Int64` nanosecond
/// duration reaches exactly 63 distinct buckets (`2^1 .. 2^63`), and the
/// gates use 64 as the static ceiling. So the bucket axis can never
/// breach `traceql_max_series` (which in any case gates `by(...)` keys,
/// and grouping stays rejected for this function).
pub fn metrics_log2_bucket_range_sql(
    spans_table: &str,
    filter: &FilterSql,
    window: SnappedWindow,
    step_s: i64,
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
        "SELECT t, {LOG2_BUCKET_EXPR} AS bucket, count() AS n\nFROM (\n  {inner}\n)\n\
         WHERE val >= 2\nGROUP BY t, bucket\nORDER BY t ASC, bucket ASC"
    )
}

/// The ungrouped `histogram_over_time` instant form — the same tally
/// over the whole snapped window, no time bucket.
pub fn metrics_log2_bucket_instant_sql(
    spans_table: &str,
    filter: &FilterSql,
    window: SnappedWindow,
) -> String {
    let mut inner =
        format!("SELECT trace_id, span_id, any(duration_ns) AS val\n  FROM {spans_table}\n  ");
    push_prewhere_where_indented(&mut inner, filter, window);
    inner.push_str("\n  GROUP BY trace_id, span_id");
    format!(
        "SELECT {LOG2_BUCKET_EXPR} AS bucket, count() AS n\nFROM (\n  {inner}\n)\n\
         WHERE val >= 2\nGROUP BY bucket\nORDER BY bucket ASC"
    )
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
    /// The `compare(f, n, start, end)` selection window, unix nanoseconds,
    /// half-open as `(start, end]` — lower bound EXCLUSIVE, upper bound
    /// INCLUSIVE (`engine_metrics_compare.go:98-110` @ v3.0.2:
    /// `spanStartTime > uint64(m.start) && spanStartTime <= uint64(m.end)`).
    ///
    /// The window REPARTITIONS, it does not filter: a span the outer
    /// filter and the request window admit but the selection window
    /// excludes is still counted — it simply lands in `baseline`. That is
    /// why it renders as a conjunct on the `is_sel` SELECT-list
    /// expression and never as a `WHERE`/`PREWHERE` predicate; moving it
    /// into the filter would drop those spans and change every total.
    ///
    /// `None` renders `is_sel` byte-identically to the pre-#460 string
    /// (pinned by `golden/traces_metrics/compare_status.sql`).
    pub sel_window: Option<(i64, i64)>,
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
        sel_window,
    } = *input;
    // `(start, end]` on the span's own start time, ANDed into the
    // selection predicate — never into the filter. See
    // [`CompareSqlInput::sel_window`].
    let is_sel = match sel_window {
        Some((start_ns, end_ns)) => format!(
            "(({inner_bool}) AND timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns})"
        ),
        None => format!("({inner_bool})"),
    };
    let mut raw = format!(
        "SELECT {bucket_expr} AS t, trace_id, span_id, name AS i_name, kind AS i_kind, \
         status_code AS i_status, service AS i_service, status_message AS i_status_message, \
         scope_name AS i_scope_name, scope_version AS i_scope_version, \
         {is_sel} AS is_sel\n    FROM {spans_table}\n    "
    );
    push_prewhere_where_indented(&mut raw, outer, window);
    // Replay-dedup: one row per (t, trace_id, span_id) so at-least-once
    // replays never inflate the baseline/selection counts (mirrors the
    // `uniqExact` rule on the count path).
    let base = format!(
        "SELECT t, trace_id, span_id, any(i_name) AS i_name, any(i_kind) AS i_kind, \
         any(i_status) AS i_status, any(i_service) AS i_service, \
         any(i_status_message) AS i_status_message, any(i_scope_name) AS i_scope_name, \
         any(i_scope_version) AS i_scope_version, max(is_sel) AS is_sel\n  FROM (\n  {raw}\n  )\n  GROUP BY t, trace_id, span_id"
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
    // window-free roots are byte-unchanged. Issue #192: the
    // `instrumentation:name`/`instrumentation:version` intrinsics source the
    // per-span `scope_name`/`scope_version` PHYSICAL columns the same way —
    // every span has a `""`-or-value, emitted verbatim (the `statusMessage`
    // precedent), so a scopeless span contributes a DISTINCT `""` value.
    let roots_cte = compare_roots_cte(spans_table, &base);
    let intrinsics = format!(
        "SELECT t, is_sel, kv.1 AS akey, kv.2 AS aval FROM (\n    \
         SELECT t, is_sel, arrayJoin([\
         ('name', i_name), ('kind', {KIND_MAP}), ('status', {STATUS_MAP}), \
         ('resource.service.name', i_service), ('statusMessage', i_status_message), \
         ('instrumentation:name', i_scope_name), ('instrumentation:version', i_scope_version), \
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
    use pulsus_traceql::{Intrinsic, SpanKindValue, StatusValue, parse};

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

    /// Issue #351: `{ true }` is the corpus's canonical "match
    /// everything" filter, and on the metrics route it must be EXACTLY
    /// `{ }` — `pkg/traceql/ast.go:459-469` @ v3.0.2 keeps a span iff the
    /// filter expression executes to boolean `true`, and a `Static`
    /// executes to itself. `{ false }` is its empty counterpart.
    /// Measured: `{ true } | rate()` returns the same series as
    /// `{ } | rate()` against the pinned container, and `{ false }` /
    /// `{ "x" = "y" }` return no series at all.
    #[test]
    fn a_boolean_static_filter_is_the_match_all_or_match_none_constant() {
        for (q, expected) in [
            ("{ true }", "1"),
            ("{ false }", "0"),
            (r#"{ "x" = "x" }"#, "1"),
            (r#"{ "x" = "y" }"#, "0"),
            ("{ 1s = 1000000000 }", "1"),
        ] {
            let f = compile(q);
            assert_eq!(f.prewhere, None, "{q}");
            assert_eq!(f.where_expr.as_deref(), Some(expected), "{q}");
        }
        // Composed with a real leaf, the constant is just another
        // conjunct — `{ true && X }` is `X` in the reference.
        let f = compile(r#"{ true && .a = "1" }"#);
        assert!(
            f.where_expr
                .as_deref()
                .unwrap_or_default()
                .starts_with("(1 AND "),
            "{:?}",
            f.where_expr
        );
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

    // ------------------------------------------------------------------
    // Issue #458 AC 8: the six `LeafEval` variants `lower_leaf` refuses in
    // one collapsed arm are UNREACHABLE from `filter::compile_leaf`.
    //
    // Before this gate that claim was a reading of 25 `match` arms — the
    // "claimed domain vs checked domain" shape: a statement about a whole
    // product space, verified by inspecting part of it. Here it is the
    // literal product `Field × ComparisonOp × Value`, enumerated from
    // exhaustive `match`es so that **adding a variant to any of those
    // enums fails to compile** rather than silently shrinking the domain
    // this test claims to cover.
    //
    // What it does NOT cover, stated so nobody credits it with more: the
    // enumeration is over TYPES. A `compile_leaf` arm that dispatched on a
    // particular attribute KEY or a particular string VALUE would have
    // only its representative probed. That gap is real and unclosed.
    // ------------------------------------------------------------------

    /// Every `Intrinsic` variant. Exhaustive `match`, no wildcard: a new
    /// intrinsic is a compile error here, not a silently unprobed arm.
    fn intrinsic_ordinal(i: Intrinsic) -> usize {
        match i {
            Intrinsic::Name => 0,
            Intrinsic::Duration => 1,
            Intrinsic::Status => 2,
            Intrinsic::Kind => 3,
            Intrinsic::NestedSetParent => 4,
            Intrinsic::NestedSetLeft => 5,
            Intrinsic::NestedSetRight => 6,
            Intrinsic::StatusMessage => 7,
            Intrinsic::ChildCount => 8,
            Intrinsic::SpanId => 9,
            Intrinsic::ParentId => 10,
            Intrinsic::TraceId => 11,
            Intrinsic::TraceDuration => 12,
            Intrinsic::RootName => 13,
            Intrinsic::RootServiceName => 14,
            Intrinsic::InstrumentationName => 15,
            Intrinsic::InstrumentationVersion => 16,
            Intrinsic::EventName => 17,
            Intrinsic::EventTimeSinceStart => 18,
            Intrinsic::LinkSpanId => 19,
            Intrinsic::LinkTraceId => 20,
        }
    }

    const ALL_INTRINSICS: [Intrinsic; 21] = [
        Intrinsic::Name,
        Intrinsic::Duration,
        Intrinsic::Status,
        Intrinsic::Kind,
        Intrinsic::NestedSetParent,
        Intrinsic::NestedSetLeft,
        Intrinsic::NestedSetRight,
        Intrinsic::StatusMessage,
        Intrinsic::ChildCount,
        Intrinsic::SpanId,
        Intrinsic::ParentId,
        Intrinsic::TraceId,
        Intrinsic::TraceDuration,
        Intrinsic::RootName,
        Intrinsic::RootServiceName,
        Intrinsic::InstrumentationName,
        Intrinsic::InstrumentationVersion,
        Intrinsic::EventName,
        Intrinsic::EventTimeSinceStart,
        Intrinsic::LinkSpanId,
        Intrinsic::LinkTraceId,
    ];

    /// Every `AttrScope` variant. Exhaustive, no wildcard.
    fn scope_ordinal(s: AttrScope) -> usize {
        match s {
            AttrScope::Span => 0,
            AttrScope::Resource => 1,
            AttrScope::Unscoped => 2,
            AttrScope::Instrumentation => 3,
            AttrScope::Event => 4,
            AttrScope::Link => 5,
        }
    }

    const ALL_SCOPES: [AttrScope; 6] = [
        AttrScope::Span,
        AttrScope::Resource,
        AttrScope::Unscoped,
        AttrScope::Instrumentation,
        AttrScope::Event,
        AttrScope::Link,
    ];

    /// Every `ComparisonOp` variant. Exhaustive, no wildcard.
    fn op_ordinal(op: ComparisonOp) -> usize {
        match op {
            ComparisonOp::Eq => 0,
            ComparisonOp::Neq => 1,
            ComparisonOp::Gt => 2,
            ComparisonOp::Gte => 3,
            ComparisonOp::Lt => 4,
            ComparisonOp::Lte => 5,
            ComparisonOp::Re => 6,
            ComparisonOp::Nre => 7,
        }
    }

    const ALL_OPS: [ComparisonOp; 8] = [
        ComparisonOp::Eq,
        ComparisonOp::Neq,
        ComparisonOp::Gt,
        ComparisonOp::Gte,
        ComparisonOp::Lt,
        ComparisonOp::Lte,
        ComparisonOp::Re,
        ComparisonOp::Nre,
    ];

    /// Every `Value` variant, by discriminant. Exhaustive, no wildcard.
    fn value_ordinal(v: &Value) -> usize {
        match v {
            Value::String(_) => 0,
            Value::Number(_) => 1,
            Value::Duration(_) => 2,
            Value::Bool(_) => 3,
            Value::Status(_) => 4,
            Value::Kind(_) => 5,
        }
    }

    /// Every `Field` variant, by discriminant. Exhaustive, no wildcard —
    /// a third `Field` shape would be a compile error here.
    fn field_ordinal(f: &Field) -> usize {
        match f {
            Field::Intrinsic(_) => 0,
            Field::Attribute { .. } => 1,
        }
    }

    /// `Duration` cannot be constructed from outside `pulsus-traceql`
    /// (`from_nanos` is crate-private), so the representative comes from
    /// the parser — the same route a real query takes.
    fn parsed_duration_value() -> Value {
        let FieldExpr::Binary { rhs, .. } = body("{ duration > 1s }") else {
            panic!("the fixture query is a binary comparison");
        };
        let FieldExpr::Literal(v @ Value::Duration(_)) = *rhs else {
            panic!("the fixture query's rhs is a duration literal");
        };
        v
    }

    fn all_values() -> Vec<Value> {
        vec![
            Value::String("x".to_string()),
            Value::Number("1".to_string()),
            parsed_duration_value(),
            Value::Bool(true),
            Value::Status(StatusValue::Error),
            Value::Kind(SpanKindValue::Server),
        ]
    }

    fn all_fields() -> Vec<Field> {
        let mut out: Vec<Field> = ALL_INTRINSICS
            .iter()
            .copied()
            .map(Field::Intrinsic)
            .collect();
        out.extend(ALL_SCOPES.iter().map(|scope| Field::Attribute {
            scope: *scope,
            key: "a".to_string(),
        }));
        out
    }

    /// The three const arrays feed every variant the exhaustive ordinal
    /// matches know about — the gap the compiler alone cannot close.
    #[test]
    fn the_enumerated_arrays_cover_every_variant_of_their_enums() {
        fn covered<T: Copy>(items: &[T], ordinal: impl Fn(T) -> usize) -> Vec<usize> {
            let mut v: Vec<usize> = items.iter().copied().map(ordinal).collect();
            v.sort_unstable();
            v.dedup();
            v
        }
        assert_eq!(
            covered(&ALL_INTRINSICS, intrinsic_ordinal),
            (0..ALL_INTRINSICS.len()).collect::<Vec<_>>(),
            "ALL_INTRINSICS must list every Intrinsic variant exactly once"
        );
        assert_eq!(
            covered(&ALL_SCOPES, scope_ordinal),
            (0..ALL_SCOPES.len()).collect::<Vec<_>>(),
            "ALL_SCOPES must list every AttrScope variant exactly once"
        );
        assert_eq!(
            covered(&ALL_OPS, op_ordinal),
            (0..ALL_OPS.len()).collect::<Vec<_>>(),
            "ALL_OPS must list every ComparisonOp variant exactly once"
        );
        let values = all_values();
        let mut seen: Vec<usize> = values.iter().map(value_ordinal).collect();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(
            seen,
            (0..values.len()).collect::<Vec<_>>(),
            "all_values() must carry one representative of every Value variant"
        );
        let mut field_shapes: Vec<usize> = all_fields().iter().map(field_ordinal).collect();
        field_shapes.sort_unstable();
        field_shapes.dedup();
        assert_eq!(
            field_shapes,
            vec![0, 1],
            "all_fields() must carry both Field shapes"
        );
    }

    #[test]
    fn dead_leaf_eval_variants_are_unreachable_from_compile_leaf() {
        let values = all_values();
        let fields = all_fields();
        let mut probed = 0usize;
        let mut reached = 0usize;
        let mut escapes = Vec::new();
        for field in &fields {
            for op in ALL_OPS {
                for value in &values {
                    probed += 1;
                    let Ok(leaf) = filter::compile_leaf(field, op, value) else {
                        continue;
                    };
                    reached += 1;
                    // Exhaustive over `LeafEval`, no wildcard: a new
                    // variant is a compile error, so this classification
                    // cannot silently admit one.
                    let dead = match &leaf.eval {
                        LeafEval::Physical(_)
                        | LeafEval::Attr { .. }
                        | LeafEval::NestedSet { .. }
                        | LeafEval::TraceCtx(_) => false,
                        LeafEval::BoolTruth { .. } => true,
                        LeafEval::FieldCompare { .. } => true,
                        LeafEval::Arith { .. } => true,
                        LeafEval::Const(_) => true,
                        LeafEval::BoolCompare { .. } => true,
                        LeafEval::EventSetCompare { .. } => true,
                    };
                    if dead {
                        escapes.push(format!("{field} {op} {value} => {:?}", leaf.eval));
                    }
                }
            }
        }
        assert_eq!(
            probed,
            fields.len() * ALL_OPS.len() * values.len(),
            "the product must be enumerated in full"
        );
        assert!(
            escapes.is_empty(),
            "compile_leaf constructed a LeafEval variant `lower_leaf` treats as unreachable, in \
             {} of {probed} probes:\n{}",
            escapes.len(),
            escapes.join("\n")
        );
        // A gate that reached nothing would pass vacuously.
        assert!(
            reached > 0,
            "no probe compiled at all — the enumeration proves nothing"
        );
    }
}
