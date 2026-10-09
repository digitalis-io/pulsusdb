//! Trace-level intrinsics and `span:childCount` on the search statement
//! (issue #594 part 1).
//!
//! A leaf on `trace:duration`, `trace:rootName` or `trace:rootService` is
//! `trace_id IN (SELECT arrayJoin(tl_<h>))`, where `tl_<h>` is one element
//! of the statement's per-trace scalar, [`per_trace_sql`]: the traces whose
//! `traces` rows, in the window's days and the day either side, satisfy the
//! leaf's condition. A `span:childCount` leaf is an inline `IN` over the
//! spans' `(trace_id, parent_span_id)` groups, reading the window's buckets
//! and, through the scalar's first element, each trace's buckets outside
//! them.

use pulsus_traceql::{ComparisonOp, Intrinsic, Value};
use xxhash_rust::xxh3::xxh3_128;

use super::projection::ceiling_sql;
use crate::traces::filter::{PlanError, parse_num, sql_op};
use crate::traces::window_sql::{RECENT_BUCKET_NS, WindowSql};

/// One trace leaf's element of the per-trace scalar: the alias the leaf
/// reads, and the condition over one trace's values that selects it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceLeaf {
    /// `tl_` and `xxh3_128(cond)` as 32 lowercase hex digits, so leaves
    /// with one condition share one alias.
    pub alias: String,
    /// Over the scalar's `duration`, `root_name` and `root_service`.
    pub cond: String,
}

/// The predicate text of a trace leaf and the element it reads.
///
/// # Errors
///
/// [`type_refusal`]'s, for a literal of another type or an operator the
/// intrinsic does not take.
pub fn trace_leaf(
    intrinsic: Intrinsic,
    op: ComparisonOp,
    value: &Value,
) -> Result<(String, TraceLeaf), PlanError> {
    let cond = match (intrinsic, value) {
        (Intrinsic::TraceDuration, Value::Duration(d)) => {
            let sym = sql_op(op).ok_or_else(|| type_refusal(intrinsic, op, value))?;
            let nanos = i64::try_from(d.as_nanos()).map_err(|_| {
                PlanError::TypeMismatch("duration literal exceeds the i64 range".to_string())
            })?;
            format!("duration {sym} {nanos}")
        }
        (Intrinsic::RootName | Intrinsic::RootServiceName, Value::String(s))
            if matches!(
                op,
                ComparisonOp::Eq | ComparisonOp::Neq | ComparisonOp::Re | ComparisonOp::Nre
            ) =>
        {
            let column = if intrinsic == Intrinsic::RootName {
                "root_name"
            } else {
                "root_service"
            };
            super::predicate::string_column_text(column, op, s)?
        }
        _ => return Err(type_refusal(intrinsic, op, value)),
    };
    let alias = format!("tl_{:032x}", xxh3_128(cond.as_bytes()));
    Ok((
        format!("trace_id IN (SELECT arrayJoin({alias}))"),
        TraceLeaf { alias, cond },
    ))
}

/// The predicate text of `span:childCount op n`, `rendered` being `n`'s
/// literal. A span with no child counts 0, which no group carries, so when
/// `0 op n` holds the leaf is `NOT IN` the groups where the count fails.
///
/// # Errors
///
/// [`type_refusal`]'s, and `parse_num`'s for a literal it cannot read.
pub fn child_leaf(
    op: ComparisonOp,
    value: &Value,
    rendered: &str,
    w: WindowSql,
    spans_table: &str,
) -> Result<String, PlanError> {
    let (Value::Number(raw), Some(sym)) = (value, sql_op(op)) else {
        return Err(type_refusal(Intrinsic::ChildCount, op, value));
    };
    let zero_holds = crate::traces::search_eval::cmp_f64(op, 0.0, parse_num(raw)?);
    let (not, cond) = if zero_holds {
        ("NOT ", format!("NOT (count() {sym} {rendered})"))
    } else {
        ("", format!("count() {sym} {rendered}"))
    };
    Ok(format!(
        "(trace_id, span_id) {not}IN (SELECT trace_id, parent_span_id FROM {spans_table} \
         WHERE ({} OR (intDiv(start_ns, {RECENT_BUCKET_NS}), trace_id) IN \
         (SELECT arrayJoin(per_trace.1))) AND parent_span_id != toFixedString('', 8) \
         GROUP BY trace_id, parent_span_id HAVING {cond})",
        w.span_bucket_clause()
    ))
}

/// The statement's per-trace scalar and its aliases, ending in `,\n     `
/// so it sits before the statement's `top`; empty when no predicate holds
/// a trace leaf or a child leaf. Each alias is defined once. With
/// `child_counts`, element 1 is every read trace's buckets outside the
/// window, as `(bucket, trace_id)`.
pub fn per_trace_sql(
    trace_leaves: &[&TraceLeaf],
    child_counts: bool,
    w: WindowSql,
    traces_table: &str,
) -> String {
    let mut leaves: Vec<&TraceLeaf> = Vec::new();
    for l in trace_leaves {
        if !leaves.iter().any(|x| x.alias == l.alias) {
            leaves.push(l);
        }
    }
    if leaves.is_empty() && !child_counts {
        return String::new();
    }
    let mut elements = Vec::new();
    if child_counts {
        elements.push(format!(
            "arrayFlatten(groupArray(arrayMap(k -> (k, trace_id), arrayFilter(k -> {}, bk))))",
            w.bucket_outside("k")
        ));
    }
    let mut aliases = String::new();
    for l in &leaves {
        elements.push(format!("groupArrayIf(trace_id, {})", l.cond));
        aliases.push_str(&format!(
            "per_trace.{} AS {},\n     ",
            elements.len(),
            l.alias
        ));
    }
    let buckets = if child_counts {
        ",\n              groupUniqArrayArray(buckets) AS bk"
    } else {
        ""
    };
    format!(
        "(SELECT tuple({elements})\n FROM (SELECT trace_id, max(end_ns) - min(start_ns) AS duration, min(root) AS r,\n              if(r.1 = 0, {service}, '') AS root_service, if(r.1 = 0, {name}, '') AS root_name{buckets}\n       FROM {traces_table}\n       WHERE {days}\n       GROUP BY trace_id\n       HAVING max(last_start_ns) >= {first} AND min(start_ns) <= {last})) AS per_trace,\n     {aliases}",
        elements = elements.join(", "),
        service = ceiling_sql("r.4"),
        name = ceiling_sql("r.5"),
        days = w.per_trace_day_clause(),
        first = w.first_included_ns(),
        last = w.last_included_ns(),
    )
}

/// Today's planner's refusal of a literal of another type or an operator
/// the intrinsic does not take (`filter.rs`'s `compile_trace_num_leaf` and
/// `string_op_leaf`), in its text.
pub fn type_refusal(intrinsic: Intrinsic, op: ComparisonOp, value: &Value) -> PlanError {
    PlanError::TypeMismatch(match intrinsic {
        Intrinsic::TraceDuration | Intrinsic::ChildCount => {
            let f = if intrinsic == Intrinsic::ChildCount {
                "span:childCount"
            } else {
                "traceDuration"
            };
            if sql_op(op).is_none() {
                format!("{f} does not support regex operators")
            } else if intrinsic == Intrinsic::ChildCount {
                "span:childCount requires a numeric value".to_string()
            } else {
                "traceDuration requires a duration literal".to_string()
            }
        }
        _ => {
            let f = if intrinsic == Intrinsic::RootName {
                "rootName"
            } else {
                "rootServiceName"
            };
            if matches!(value, Value::String(_)) {
                format!("{f} supports only = != =~ !~")
            } else {
                format!("{f} requires a string value")
            }
        }
    })
}
