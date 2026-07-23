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
use super::search_sql::date_literal;

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
            }
        }
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
        | filter::PhysicalPredicate::Service { op, value } => (op, value),
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
