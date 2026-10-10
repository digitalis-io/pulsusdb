//! The nested-set intrinsics on the search statement (issue #594 part 2).
//!
//! `nestedSetLeft`, `nestedSetRight` and `nestedSetParent` are the
//! reference's numbering: an Euler tour
//! that starts at each span with an empty parent id, `left` on the way
//! down, `right` on the way up, `parent` the parent's `left`, `-1` for a
//! root. A span no root reaches (an orphan, a cycle member, a descendant
//! of either) is not numbered: all three are `0`. Siblings, and roots, are
//! toured in `(start_ns, span_id)` order.
//!
//! [`NUMBERING`] computes it per trace in the database, without
//! recursion: the tour's successor of every enter and exit event, then
//! list ranking by pointer jumping, `ceil(log2(2n + 2))` doublings, each a
//! sort, never a lookup through a captured array.
//!
//! A comparison is `(trace_id, span_id) IN` the spans of the read's traces
//! whose number satisfies it. The traces are `nested_keys`, which each read
//! binds: the traces whose span starts reach the slice the top read reads,
//! or the window when the statement is not sliced (`trace_keys`), the returned traces in the detail read (`detail_keys`). A `nestedSetParent`
//! comparison that holds for `-1` alone is `parent_span_id` empty, with no
//! numbering.
//!
//! A comparison reading a number or a child count as an operand (issue
//! #594 part 4) is `(trace_id, span_id) IN` the read's spans joined to
//! them ([`span_values_membership`]).

use pulsus_traceql::{ComparisonOp, Intrinsic, Value};

use crate::traces::filter::{PlanError, parse_num, sql_op};
use crate::traces::search_eval::cmp_f64;
use crate::traces::window_sql::WindowSql;

/// Per trace over `{spans}` rows whose `(bucket, trace_id)` is in
/// `{keys}`: `trace_id`, `s` (the spans as `(span_id, parent_span_id,
/// start_ns)` in `(start_ns, span_id)` order), `lft`, `rgt` and `par`, one
/// element per span of `s`.
const NUMBERING: &str = r"SELECT trace_id, s, n, lft, rgt,
       arrayMap((l, p) -> if(l = 0, 0, p), lft,
                arrayMap(x -> x.2, arraySort(x -> x.1, arrayFilter(x -> x.1 > 0,
                  arrayMap((a, b) -> if(a.2 = 1, (a.3, b.4), (0, 0)), pl, arrayFill(z -> z.2 = 0, pl)))))) AS par
FROM (SELECT trace_id, s, n, pidx, lft, rgt,
             arraySort(arrayConcat([(toUInt32(0), 0, toUInt32(0), toInt64(-1))],
                                   arrayMap((v, j) -> (j, 0, j, v), lft, arrayEnumerate(lft)),
                                   arrayMap((k, j) -> (k, 1, j, toInt64(0)), pidx, arrayEnumerate(pidx)))) AS pl
      FROM (SELECT trace_id, s, n, pidx,
                   arraySlice(tour, 1, n) AS enter,
                   toInt64(2 * arrayCount(t -> t.1 = 2 * n + 1, enter)) AS m2,
                   arrayMap(t -> if(t.1 = 2 * n + 1, m2 - t.2 + 1, 0), enter) AS lft,
                   arrayMap((t, e) -> if(t.1 = 2 * n + 1, m2 - e.2 + 1, 0), enter, arraySlice(tour, n + 1, n)) AS rgt
            FROM (SELECT trace_id, s, n, pidx,
                         arrayFold((acc, x) -> arrayMap(c -> arrayMap((t, y) -> (y.1, toUInt32(t.2 + y.2)), acc,
                                                        arrayMap(y -> (y.2, y.3), arraySort(y -> y.1, arrayFilter((y, k) -> k.2 = 1,
                                                          arrayMap((k, f) -> (k.3, f.4, f.5), c, arrayFill(z -> z.2 = 0, c)), c)))),
                                              [arraySort((v, k) -> k,
                                                 arrayConcat(arrayMap((t, j) -> (j, 0, toUInt32(0), t.1, t.2), acc, arrayEnumerate(acc)),
                                                             arrayMap((t, j) -> (t.1, 1, j, toUInt32(0), toUInt32(0)), acc, arrayEnumerate(acc))),
                                                 arrayConcat(arrayMap(j -> toUInt64(j) * 2, arrayEnumerate(acc)),
                                                             arrayMap(t -> toUInt64(t.1) * 2 + 1, acc)))])[1],
                                   range(toUInt32(ceil(log2(2 * n + 2)))),
                                   arrayConcat(arrayMap((f, i) -> (toUInt32(if(f > 0, f, n + i)), toUInt32(1)), fc, arrayEnumerate(fc)),
                                               arrayMap((sb, p, root) -> (toUInt32(if(sb > 0, sb, if(p > 0, n + p, if(root, 2 * n + 1, 2 * n + 2)))), toUInt32(1)), nsib, pidx, isroot),
                                               [(toUInt32(2 * n + 1), toUInt32(0)), (toUInt32(2 * n + 2), toUInt32(0))])) AS tour
                  FROM (SELECT trace_id, s, n, pidx, isroot,
                               arraySort((g, i) -> (g, i), grp, arrayEnumerate(grp)) AS qg,
                               arraySort((i, g) -> (g, i), arrayEnumerate(grp), grp) AS q,
                               arraySort((v, k) -> k,
                                         arrayMap((g, g2, nq) -> if(g = g2, nq, 0), qg,
                                                  arrayPushBack(arrayPopFront(qg), toInt64(-9223372036854775807)),
                                                  arrayPushBack(arrayPopFront(q), toUInt32(0))), q) AS nsib,
                               arraySort(arrayConcat(arrayMap(i -> (toInt64(i), 0, i), arrayEnumerate(grp)),
                                                     arrayMap((k, j) -> (k, 1, j), grp, arrayEnumerate(grp)))) AS fcc,
                               arrayMap(x -> x.2, arraySort(x -> x.1, arrayFilter(x -> x.1 > 0,
                                 arrayMap((a, b) -> if(a.2 = 0, (a.3, if(b.2 = 1 AND b.1 = a.1, b.3, 0)), (0, 0)),
                                          fcc, arrayReverseFill(x -> x.2 = 1, fcc))))) AS fc
                        FROM (SELECT trace_id, s, n, pidx, isroot,
                                     arrayMap((p, root, i) -> if(p > 0, toInt64(p), if(root, 0, -toInt64(i))), pidx, isroot, arrayEnumerate(pidx)) AS grp
                              FROM (SELECT trace_id, s, toUInt32(length(s)) AS n,
                                           arrayMap(x -> x.2 = toFixedString('', 8), s) AS isroot,
                                           arraySort(arrayConcat(arrayMap((x, j) -> (x.2, 0, j), s, arrayEnumerate(s)),
                                                                 arrayMap((x, j) -> (x.1, 1, j), s, arrayEnumerate(s)))) AS pc,
                                           arrayMap(x -> x.2, arraySort(x -> x.1, arrayFilter(x -> x.1 > 0,
                                             arrayMap((a, b) -> if(a.2 = 0, (a.3, if(b.2 = 1 AND b.1 = a.1, b.3, 0)), (0, 0)),
                                                      pc, arrayReverseFill(x -> x.2 = 1, pc))))) AS pidx0,
                                           arrayMap((v, root) -> if(root, 0, v), pidx0, isroot) AS pidx
                                    FROM (SELECT trace_id, arraySort(x -> (x.3, x.1), groupArray((span_id, parent_span_id, start_ns))) AS s
                                          FROM {spans}
                                          WHERE (intDiv(start_ns, 300000000000), trace_id) IN (SELECT arrayJoin({keys}))
                                          GROUP BY trace_id
                                          SETTINGS max_block_size = 256)))))))";

/// The numbering over the traces of `keys`, an alias holding
/// `(bucket, trace_id)` pairs.
pub fn numbering_sql(spans_table: &str, keys: &str) -> String {
    NUMBERING
        .replace("{spans}", spans_table)
        .replace("{keys}", keys)
}

/// The scope the top read binds: the traces reaching its slice.
pub const SCOPE_TOP: &str = "WITH trace_keys AS nested_keys ";
/// The scope the detail read binds: the returned traces.
pub const SCOPE_DETAIL: &str = "WITH detail_keys AS nested_keys ";

/// The numbering's column for `intrinsic`, and its element of
/// `nested_values`.
fn column(intrinsic: Intrinsic) -> (&'static str, usize) {
    match intrinsic {
        Intrinsic::NestedSetLeft => ("nl", 2),
        Intrinsic::NestedSetRight => ("nr", 3),
        _ => ("np", 4),
    }
}

/// Whether `nestedSetParent op n` holds for `-1` and for no other value a
/// span can take (`0`, or a `left` from 1 up).
pub fn root_only(op: ComparisonOp, value: &Value) -> bool {
    let Value::Number(raw) = value else {
        return false;
    };
    let Ok(n) = parse_num(raw) else {
        return false;
    };
    sql_op(op).is_some() && cmp_f64(op, -1.0, n) && !cmp_f64(op, 0.0, n) && !cmp_f64(op, 1.0, n)
}

/// A nested-set leaf: its predicate text, and whether it reads the
/// numbering. `rendered` is the literal as the predicate renders numbers.
///
/// # Errors
///
/// Today's planner's two refusals, in its text.
pub fn nested_leaf(
    intrinsic: Intrinsic,
    op: ComparisonOp,
    value: &Value,
    rendered: &str,
    spans_table: Option<&str>,
) -> Result<(String, bool), PlanError> {
    let Some(sym) = sql_op(op) else {
        return Err(PlanError::TypeMismatch(
            "nested-set intrinsics do not support regex operators".to_string(),
        ));
    };
    if !matches!(value, Value::Number(_)) {
        return Err(PlanError::TypeMismatch(
            "nested-set intrinsics require a numeric value".to_string(),
        ));
    }
    if intrinsic == Intrinsic::NestedSetParent && root_only(op, value) {
        return Ok(("parent_span_id = toFixedString('', 8)".to_string(), false));
    }
    let spans_table = spans_table.ok_or_else(|| {
        PlanError::UnsupportedField(format!(
            "{intrinsic} needs the request window: compile it with compile_span_predicate_in \
             (issue #594)"
        ))
    })?;
    Ok((
        format!(
            "(trace_id, span_id) IN (SELECT trace_id, sp.1 FROM ({}) ARRAY JOIN s AS sp, lft AS nl, \
             rgt AS nr, par AS np WHERE {} {sym} {rendered})",
            numbering_sql(spans_table, "nested_keys"),
            column(intrinsic).0
        ),
        true,
    ))
}

/// The scalar before `top` when a predicate reads the numbering, ending in
/// `,\n     `: `trace_keys`, the `(bucket, trace_id)` pairs of every
/// `traces` row, within `w`'s per-trace days (part 1's D2), of the traces
/// with a row in those days whose span starts reach `top` — the traces the
/// top-K numbers, each whole within those days.
pub fn keys_sql(top: WindowSql, w: WindowSql, traces_table: &str) -> String {
    let days = w.per_trace_day_clause();
    format!(
        "(SELECT groupArray((k, trace_id)) FROM (SELECT trace_id, arrayJoin(buckets) AS k \
         FROM {traces_table} WHERE {days} AND trace_id IN (SELECT trace_id FROM {traces_table} \
         WHERE {days} AND last_start_ns >= {first} AND start_ns <= {last}))) AS trace_keys,\n     ",
        first = top.first_included_ns(),
        last = top.last_included_ns(),
    )
}

/// The scalars after `top` when a predicate reads the numbering: the
/// returned traces' `(bucket, trace_id)` pairs, and their numbering as
/// `(concat(trace_id, span_id), left, right, parent)` arrays.
pub fn after_top_sql(spans_table: &str) -> String {
    format!(
        ",\n     (SELECT groupArray(x) FROM (SELECT arrayJoin(trace_keys) AS x) \
         WHERE x.2 IN (SELECT arrayJoin(top.1))) AS detail_keys,\n     \
         (SELECT (groupArray(concat(trace_id, sp.1)), groupArray(nl), groupArray(nr), groupArray(np))\n      \
         FROM ({})\n      ARRAY JOIN s AS sp, lft AS nl, rgt AS nr, par AS np) AS nested_values",
        numbering_sql(spans_table, "detail_keys")
    )
}

/// The value a matched span projects for `intrinsic`: `-1` for a root
/// comparison, else its number from `nested_values`.
pub fn projected_sql(intrinsic: Intrinsic, op: ComparisonOp, value: &Value) -> String {
    if intrinsic == Intrinsic::NestedSetParent && root_only(op, value) {
        return "'-1'".to_string();
    }
    format!(
        "toString(transform(concat(trace_id, span_id), nested_values.1, nested_values.{}, \
         toInt64(0)))",
        column(intrinsic).1
    )
}

/// The per-span values one comparison reads as operands (issue #594 part
/// 4): the numbering, the child counts, or both.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SpanValues {
    pub numbered: bool,
    pub children: bool,
}

impl SpanValues {
    /// Notes `intrinsic`'s value and returns its joined column.
    pub fn read(&mut self, intrinsic: Intrinsic) -> &'static str {
        match intrinsic {
            Intrinsic::ChildCount => {
                self.children = true;
                "pv_children"
            }
            Intrinsic::NestedSetLeft => {
                self.numbered = true;
                "pv_left"
            }
            Intrinsic::NestedSetRight => {
                self.numbered = true;
                "pv_right"
            }
            _ => {
                self.numbered = true;
                "pv_parent"
            }
        }
    }
}

/// A comparison `cond` over per-span values: the spans of the read's
/// traces (`nested_keys`) joined to their numbers and child counts, a span
/// without a child count or a number reading `0`.
pub fn span_values_membership(cond: &str, v: SpanValues, spans_table: &str) -> String {
    let mut joins = String::new();
    if v.numbered {
        joins.push_str(&format!(
            " LEFT JOIN (SELECT trace_id, sp.1 AS span_id, nl AS pv_left, nr AS pv_right, \
             np AS pv_parent FROM ({}) ARRAY JOIN s AS sp, lft AS nl, rgt AS nr, par AS np) \
             AS pv_n USING (trace_id, span_id)",
            numbering_sql(spans_table, "nested_keys")
        ));
    }
    if v.children {
        joins.push_str(&format!(
            " LEFT JOIN (SELECT trace_id, parent_span_id AS span_id, count() AS pv_children \
             FROM {spans_table} WHERE (intDiv(start_ns, 300000000000), trace_id) IN \
             (SELECT arrayJoin(nested_keys)) AND parent_span_id != toFixedString('', 8) \
             GROUP BY trace_id, parent_span_id) AS pv_c USING (trace_id, span_id)"
        ));
    }
    format!(
        "(trace_id, span_id) IN (SELECT trace_id, span_id FROM {spans_table}{joins} WHERE \
         (intDiv(start_ns, 300000000000), trace_id) IN (SELECT arrayJoin(nested_keys)) AND ({cond}))"
    )
}

/// The number `select(<intrinsic>)` projects (issue #594 part 4): the
/// span's, from `nested_values`.
pub fn selected_sql(intrinsic: Intrinsic) -> String {
    format!(
        "toString(transform(concat(trace_id, span_id), nested_values.1, nested_values.{}, \
         toInt64(0)))",
        column(intrinsic).1
    )
}

/// The scalars after `top` when the projection reads `nested_values` and
/// no predicate numbers: the returned traces' pairs within `w`'s per-trace
/// days, and their numbering.
pub fn returned_numbers_sql(spans_table: &str, traces_table: &str, w: WindowSql) -> String {
    format!(
        ",\n     (SELECT groupArray((k, trace_id)) FROM (SELECT trace_id, arrayJoin(buckets) AS k \
         FROM {traces_table} WHERE {} AND trace_id IN (SELECT arrayJoin(top.1)))) AS detail_keys,\n     \
         (SELECT (groupArray(concat(trace_id, sp.1)), groupArray(nl), groupArray(nr), groupArray(np))\n      \
         FROM ({})\n      ARRAY JOIN s AS sp, lft AS nl, rgt AS nr, par AS np) AS nested_values",
        w.per_trace_day_clause(),
        numbering_sql(spans_table, "detail_keys")
    )
}
