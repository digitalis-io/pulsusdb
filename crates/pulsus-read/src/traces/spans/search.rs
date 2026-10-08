//! The TraceQL search statement (issue #590): `docs/TraceQL/sql-schema.md`
//! §5.2, without `by()` and without the newest-slice loop. One pure
//! builder, `validated inputs -> String`, as [`super::fetch`]'s are; no
//! route calls it yet.
//!
//! ## The three reads, in one statement
//!
//! | read | what | rows read |
//! |---|---|---|
//! | `top` | the newest `limit` traces with a matching span, `last DESC, trace_id ASC`, and the five-minute buckets of their matching spans | the window, once |
//! | `m` | those traces' matching spans, read by the sort key's prefix `(bucket, trace_id)`; `matched` before the cap; the first `spss` by `(start_ns, span_id)` | whole granules around the keys |
//! | `t` | each trace's root, extent and duration from `traces`, grouped because a trace has a row per day | one granule per trace per part |
//!
//! **`top` is a scalar subquery, `WITH (SELECT …) AS top`.** The engine
//! computes a scalar once per statement and caches it; the common-table
//! form, `WITH top AS (SELECT …)`, is inlined at each use and reads the
//! window once per use. Measured on corpus g1 (2,000,064 spans, 26.3.29.7,
//! `max_threads = 4`): 2,047,170 rows read for `{}` in this form against
//! 4,047,232 in the other. A scalar alias also carries no `WithElement`,
//! so ADR 0008 D2's check passes it.
//!
//! **The detail read is keyed on `(bucket, trace_id)`, not `trace_id`
//! alone.** Both are the sort key's prefix, so the tuple `IN` becomes a key
//! condition; without the bucket the read is a near full scan — 2,961,259
//! rows on g1 against 2,047,170.
//!
//! **`LEFT JOIN traces`**: a trace the per-trace table has not indexed
//! still returns, with an empty root and a zero extent. ADR 0008's rule
//! against joins covers the statements the old compile core plans; this
//! builder is not one of them, and its goldens are on the join gate's
//! named list.
//!
//! **The root is one span** (issue #591 part 1, decision 4): `traces`
//! stores `root = (not a root, start_ns, span_id, service, name)` and the
//! read takes its `min`, so a trace's root service and name come from the
//! same span — the earliest parentless one by `(start_ns, span_id)` — and
//! a trace with no parentless span reports both empty.
//!
//! ## `&&` and `||` across spansets (issue #591 part 1)
//!
//! A spanset tree is one window read with one flag per filter and a
//! `HAVING` over each trace's flags, then the detail read keeps each
//! trace's spans by the same flags (section 4.3): `&&` is "both sides
//! non-empty, the union of their spans", `||` the union (`docs/api.md`
//! §4.2). Measured on g1, 2,034,612 rows and 50 ms, against 10,034,868
//! rows and 130 ms for the design's catalogue form, which builds a
//! `trace_id IN` set per filter in each of the two reads.
//!
//! ## The projection
//!
//! The detail read carries each matched span's projected values,
//! [`super::projection`]'s, and [`decode_search`] turns the rows into
//! today's [`SearchOutput`].

use std::collections::HashMap;

use pulsus_traceql::{
    BoolOp, Field, FieldExpr, FieldOp, PipelineStage, Query, SpansetExpr, SpansetFilter, Value,
};

use super::predicate::{PredicateCtx, SpanPredicate, compile_span_predicate_in};
use super::projection::{
    GroupKeySql, Projection, ceiling_sql, group_key_sql, rendered_array, rendered_array_len,
};
use super::rows::{SearchGroupTuple, SearchGroupedRow, SearchTraceRow};
use crate::logql::error::ReadError;
use crate::traces::PlanError;
use crate::traces::exec::{
    ByteBudget, RootSummary, SearchOutput, TraceSearchResult, output_reserve_bytes,
};
use crate::traces::search_eval::{
    GroupCardinalityCounter, GroupValue, SpanSetGroup, SpanSummary, group_double_bits,
    group_reserve_bytes, groups_reserve_bytes, match_reserve_bytes, summary_reserve_bytes,
};
use crate::traces::search_plan::SearchPlan;
use crate::traces::window_sql::WindowSql;
use pulsus_clickhouse::ChError;

/// The per-trace read, shared by both statements: each returned trace's
/// extent and its root, one span's service and name, cut at the response
/// ceiling.
const TRACE_READ: &str = r"LEFT JOIN (SELECT trace_id, min(start_ns) AS start_ns, max(end_ns) AS end_ns,
                  min(root) AS r, if(r.1 = 0, {root_service}, '') AS root_service, if(r.1 = 0, {root_name}, '') AS root_name
           FROM {traces}
           WHERE trace_id IN (SELECT arrayJoin(top.1))
           GROUP BY trace_id) AS t USING trace_id
ORDER BY last DESC, trace_id ASC";

/// One filter (section 4.2): #590's statement, the detail read carrying
/// the projection. A `const` at column zero, filled by [`fill`], so no
/// source indentation can leak into it.
const SEARCH: &str = r"WITH (SELECT (groupArray(trace_id), groupArray(keys))
      FROM (SELECT trace_id, max(start_ns) AS last,
                   groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
            FROM {spans}
            WHERE {time}
              AND {bucket}
              AND {day}
              AND ({predicate})
            GROUP BY trace_id
            ORDER BY last DESC, trace_id ASC
            LIMIT {limit})) AS top
SELECT m.trace_id AS trace_id, t.root_service AS root_service, t.root_name AS root_name,
       t.start_ns AS start_ns, t.end_ns - t.start_ns AS duration_ns,
       m.last AS last, m.matched AS matched, m.spans AS spans
FROM (SELECT trace_id, max(start_ns) AS last, count() AS matched,
             arraySlice(arraySort(x -> (x.2, x.1),
                        groupArray((span_id, start_ns, duration_ns, service, {projection}))), 1, {spss}) AS spans
      FROM {spans}
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN
            (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
        AND {time}
        AND {bucket}
        AND {day}
        AND ({predicate})
      GROUP BY trace_id) AS m
{trace_read}";

/// One filter followed by later `{…}` filters (issue #592 part 1): today's
/// engine ranks a trace by the newest span the selector matched, before
/// any stage runs, and then keeps the spans the later filters keep. So
/// both reads filter by the selector alone; the top-K keeps a trace only
/// when a span satisfies the later filters, `matched` counts those spans,
/// and the spans returned are theirs.
const SEARCH_LATER: &str = r"WITH (SELECT (groupArray(trace_id), groupArray(keys))
      FROM (SELECT trace_id, max(start_ns) AS last,
                   groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
            FROM {spans}
            WHERE {time}
              AND {bucket}
              AND {day}
              AND ({predicate})
            GROUP BY trace_id
            HAVING countIf({later}) > 0
            ORDER BY last DESC, trace_id ASC
            LIMIT {limit})) AS top
SELECT m.trace_id AS trace_id, t.root_service AS root_service, t.root_name AS root_name,
       t.start_ns AS start_ns, t.end_ns - t.start_ns AS duration_ns,
       m.last AS last, m.matched AS matched, m.spans AS spans
FROM (SELECT trace_id, max(start_ns) AS last, countIf({later}) AS matched,
             arraySlice(arraySort(x -> (x.2, x.1),
                        groupArrayIf((span_id, start_ns, duration_ns, service, {projection}), {later})), 1, {spss}) AS spans
      FROM {spans}
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN
            (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
        AND {time}
        AND {bucket}
        AND {day}
        AND ({predicate})
      GROUP BY trace_id) AS m
{trace_read}";

/// One filter, the selector, then one `by()` with filters before and after
/// it (issue #592 part 2). The top-K is part 1's, over traces: a trace is
/// kept when a span passes every filter (`{later}`). The detail read groups
/// each trace's spans by `(trace_id, key, key type)`: a group exists when a
/// span reached `by()` (`g_pre`, the filters before it), it is ordered by
/// the first such span, and its members are the spans every filter kept
/// (`g_matched`, `g_spans`). The trace's own `matched` and spans are the
/// union of its groups'.
const SEARCH_GROUPED: &str = r"WITH (SELECT (groupArray(trace_id), groupArray(keys))
      FROM (SELECT trace_id, max(start_ns) AS last,
                   groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
            FROM {spans}
            WHERE {time}
              AND {bucket}
              AND {day}
              AND ({predicate})
            GROUP BY trace_id
            HAVING countIf({later}) > 0
            ORDER BY last DESC, trace_id ASC
            LIMIT {limit})) AS top
SELECT m.trace_id AS trace_id, t.root_service AS root_service, t.root_name AS root_name,
       t.start_ns AS start_ns, t.end_ns - t.start_ns AS duration_ns,
       m.last AS last, m.matched AS matched, m.spans AS spans, m.groups AS groups
FROM (SELECT trace_id, max(g_last) AS last, sum(g_matched) AS matched,
             arraySlice(arraySort(x -> (x.2, x.1), arrayFlatten(groupArray(g_spans))), 1, {spss}) AS spans,
             arrayMap(x -> (x.2, x.3, x.4, x.5),
                      arraySort(x -> x.1, groupArrayIf((g_first, grp, grp_type, g_matched, g_spans), g_pre > 0))) AS groups
      FROM (SELECT trace_id, {grp} AS grp, {grp_type} AS grp_type,
                   max(start_ns) AS g_last, countIf({pre}) AS g_pre, countIf({later}) AS g_matched,
                   minIf((start_ns, span_id), {pre}) AS g_first,
                   arraySlice(arraySort(x -> (x.2, x.1),
                              groupArrayIf((span_id, start_ns, duration_ns, service, {projection}), {later})), 1, {spss}) AS g_spans
            FROM {spans}
            WHERE (intDiv(start_ns, 300000000000), trace_id) IN
                  (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
              AND {time}
              AND {bucket}
              AND {day}
              AND ({predicate}){demand}
            GROUP BY trace_id, grp, grp_type)
      GROUP BY trace_id) AS m
{trace_read}";

/// One filter, the selector, then one aggregate stage with filters before
/// and after it (issue #592 part 3). The top-K is part 1's, over traces,
/// keeping a trace when its spans that reached the aggregate (`{before}`)
/// pass it (`{pass}`) and a span passes every filter (`{later}`). The
/// detail read carries the aggregate's response value as one group
/// holding the trace's surviving spans.
const SEARCH_AGGREGATED: &str = r"WITH (SELECT (groupArray(trace_id), groupArray(keys))
      FROM (SELECT trace_id, max(start_ns) AS last,
                   groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
            FROM {spans}
            WHERE {time}
              AND {bucket}
              AND {day}
              AND ({predicate}){demand}
            GROUP BY trace_id
            HAVING countIf({before}) > 0 AND ({pass}) AND countIf({later}) > 0
            ORDER BY last DESC, trace_id ASC
            LIMIT {limit})) AS top
SELECT m.trace_id AS trace_id, t.root_service AS root_service, t.root_name AS root_name,
       t.start_ns AS start_ns, t.end_ns - t.start_ns AS duration_ns,
       m.last AS last, m.matched AS matched, m.spans AS spans,
       [(m.agg_text, m.agg_type, m.matched, m.spans)] AS groups
FROM (SELECT trace_id, max(start_ns) AS last, countIf({later}) AS matched,
             arraySlice(arraySort(x -> (x.2, x.1),
                        groupArrayIf((span_id, start_ns, duration_ns, service, {projection}), {later})), 1, {spss}) AS spans,
             {agg_text} AS agg_text, {agg_type} AS agg_type
      FROM {spans}
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN
            (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
        AND {time}
        AND {bucket}
        AND {day}
        AND ({predicate}){demand}
      GROUP BY trace_id) AS m
{trace_read}";

/// The message of an aggregate argument's off-path demand (issue #592
/// part 3).
pub const AGGREGATE_OFF_PATH_DEMAND: &str =
    "an aggregate value held off its own path is answered by the old engine (issue #592)";

/// The message of a projected value's off-path demand (issue #592 part
/// 3): `select(k)` or `{ k != nil }` where a span holds `k` as a key-value
/// list or bytes, whose JSON or base64 today's engine returns.
pub const SELECT_OFF_PATH_DEMAND: &str =
    "a selected value held off its own path is answered by the old engine (issue #592)";

/// The message of a `by()` key's off-path demand (issue #592 part 2): the
/// statement cannot read a key-value list's or bytes' value as the key's,
/// so the request goes to today's engine.
pub const OFF_PATH_DEMAND: &str =
    "a by() value held off its own path is answered by the old engine (issue #592)";

/// A spanset tree (section 4.3): one flag per filter, a `HAVING` over each
/// trace's flags, and the detail read keeping each trace's spans by them.
const SEARCH_TREE: &str = r"WITH (SELECT (groupArray(trace_id), groupArray(keys))
      FROM (SELECT trace_id, {counts}, greatest({lasts}) AS last,
                   groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
            FROM (SELECT trace_id, start_ns, {flags}
                  FROM {spans}
                  WHERE {time}
                    AND {bucket}
                    AND {day}
                    AND ({any}))
            GROUP BY trace_id
            HAVING {holds}
            ORDER BY last DESC, trace_id ASC
            LIMIT {limit})) AS top
SELECT m.trace_id AS trace_id, t.root_service AS root_service, t.root_name AS root_name,
       t.start_ns AS start_ns, t.end_ns - t.start_ns AS duration_ns,
       m.last AS last, m.matched AS matched, m.spans AS spans
FROM (SELECT trace_id, arrayMax(arrayMap(x -> x.2, sp)) AS last, length(sp) AS matched,
             arraySlice(arraySort(x -> (x.2, x.1), sp), 1, {spss}) AS spans
      FROM (SELECT trace_id, {counts},
                   arrayMap(x -> (x.1, x.2, x.3, x.4, x.5),
                            arrayFilter(x -> {member},
                                        groupArray((span_id, start_ns, duration_ns, service, proj, {fnames})))) AS sp
            FROM (SELECT trace_id, span_id, start_ns, duration_ns, service, {projection} AS proj, {flags}
                  FROM {spans}
                  WHERE (intDiv(start_ns, 300000000000), trace_id) IN
                        (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
                    AND {time}
                    AND {bucket}
                    AND {day}
                    AND ({any}))
            GROUP BY trace_id)) AS m
{trace_read}";

/// Fills `template`'s `{name}` placeholders in ONE pass over the template:
/// a value is never scanned again, so a client's string literal spelled
/// like a placeholder — `{limit}` inside a predicate — stays as written.
fn fill(template: &str, values: &[(&str, &str)]) -> String {
    let by_name: HashMap<&str, &str> = values.iter().copied().collect();
    let mut out = String::with_capacity(template.len() * 2);
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        match after.find('}') {
            Some(close) if by_name.contains_key(&after[..close]) => {
                out.push_str(by_name[&after[..close]]);
                rest = &after[close + 1..];
            }
            _ => {
                out.push('{');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// What the statement filters spans by.
pub enum SearchFilter {
    /// One `{ … }`.
    One(SpanPredicate),
    /// One `{ … }`, the selector, followed by later `{…}` filters whose
    /// bodies are ANDed in written order (issue #592 part 1).
    Later {
        selector: SpanPredicate,
        later: SpanPredicate,
    },
    /// Two or more filters joined by `&&`/`||`, in pre-order, with the
    /// texts of section 4.3's `{holds}` and each filter's guard (`None` =
    /// true).
    Tree {
        filters: Vec<SpanPredicate>,
        holds: String,
        guards: Vec<Option<String>>,
    },
    /// One `{ … }`, the selector, then one `by()` (issue #592 part 2):
    /// `before` is the AND of the filters before it, `all` of every later
    /// filter, before and after it (`None` = true), and `key` how the
    /// statement reads its key.
    Grouped {
        selector: SpanPredicate,
        before: Option<SpanPredicate>,
        all: Option<SpanPredicate>,
        key: GroupKeySql,
    },
    /// One `{ … }`, the selector, then one aggregate stage (issue #592
    /// part 3): `before` is the AND of the filters before it, `all` of
    /// every later filter, and `agg` the aggregate's SQL over `before`.
    Aggregated {
        selector: SpanPredicate,
        before: Option<SpanPredicate>,
        all: Option<SpanPredicate>,
        agg: AggregateSql,
    },
}

/// One aggregate stage's SQL over a trace's spans (issue #592 part 3): the
/// pass, and the response value as `(text, type)` for
/// [`aggregate_value`]; `off_path` as [`GroupKeySql::off_path`].
#[derive(Debug, Clone)]
pub struct AggregateSql {
    pub pass: String,
    pub text: String,
    pub value_type: String,
    pub off_path: Option<String>,
}

impl SearchFilter {
    fn predicates(&self) -> Vec<&SpanPredicate> {
        match self {
            SearchFilter::One(p) => vec![p],
            SearchFilter::Later { selector, later } => vec![selector, later],
            SearchFilter::Tree { filters, .. } => filters.iter().collect(),
            SearchFilter::Grouped {
                selector,
                before,
                all,
                ..
            }
            | SearchFilter::Aggregated {
                selector,
                before,
                all,
                ..
            } => std::iter::once(selector)
                .chain(before.iter())
                .chain(all.iter())
                .collect(),
        }
    }
}

/// A filter's body as a predicate expression: `{}` is `{ true }`.
fn body_of(filter: &SpansetFilter) -> FieldExpr {
    filter
        .body
        .clone()
        .unwrap_or(FieldExpr::Literal(Value::Bool(true)))
}

/// The filters of `spanset` in pre-order, through structural operators
/// too (issue #593): the projection reads every filter's body.
fn filters_of<'a>(
    spanset: &'a SpansetExpr,
    out: &mut Vec<&'a SpansetFilter>,
) -> Result<(), PlanError> {
    match spanset {
        SpansetExpr::Filter(f) => {
            out.push(f);
            Ok(())
        }
        SpansetExpr::Binary { lhs, rhs, .. } => {
            filters_of(lhs, out)?;
            filters_of(rhs, out)
        }
        SpansetExpr::Structural { lhs, rhs, .. } => {
            filters_of(lhs, out)?;
            filters_of(rhs, out)
        }
    }
}

/// `C(E)` and each filter's guard (section 4.3's table), filters numbered
/// in pre-order after `next`. A guard is the conjunction, outermost
/// first, of the `C` of every `&&` above the filter.
fn holds_and_guards(
    spanset: &SpansetExpr,
    next: &mut usize,
    guards: &mut Vec<Option<String>>,
) -> String {
    match spanset {
        SpansetExpr::Filter(_) => {
            *next += 1;
            guards.push(None);
            format!("c{next}")
        }
        SpansetExpr::Binary { op, lhs, rhs } => {
            let mut inner: Vec<Option<String>> = Vec::new();
            let l = holds_and_guards(lhs, next, &mut inner);
            let r = holds_and_guards(rhs, next, &mut inner);
            let joined = match op {
                BoolOp::And => format!("({l} AND {r})"),
                BoolOp::Or => format!("({l} OR {r})"),
            };
            for g in inner {
                guards.push(match (op, g) {
                    (BoolOp::And, Some(g)) => Some(format!("{joined} AND {g}")),
                    (BoolOp::And, None) => Some(joined.clone()),
                    (BoolOp::Or, g) => g,
                });
            }
            joined
        }
        SpansetExpr::Structural { .. } => {
            unreachable!("a structural spanset compiles to its membership")
        }
    }
}

/// Compiles `spanset` to the statement's filter: one `{ … }` is
/// [`SearchFilter::One`], a tree of them joined by `&&`/`||` a
/// [`SearchFilter::Tree`]. A structural operator is #593's; everything the
/// predicate compiler refuses is returned as it refused.
pub fn compile_search_filter(
    spanset: &SpansetExpr,
    ctx: &PredicateCtx<'_>,
) -> Result<SearchFilter, PlanError> {
    let mut filters = Vec::new();
    filters_of(spanset, &mut filters)?;
    let predicates: Vec<SpanPredicate> = filters
        .iter()
        .map(|f| compile_span_predicate_in(&body_of(f), ctx))
        .collect::<Result<_, _>>()?;
    if let (SpansetExpr::Filter(_), [_]) = (spanset, predicates.as_slice()) {
        let p = predicates.into_iter().next().expect("one filter");
        return Ok(SearchFilter::One(p));
    }
    let mut next = 0;
    let mut guards = Vec::new();
    let holds = holds_and_guards(spanset, &mut next, &mut guards);
    Ok(SearchFilter::Tree {
        filters: predicates,
        holds,
        guards,
    })
}

/// The search statement (`sql-schema.md` §5.2, ungrouped, without the
/// newest-slice loop): the newest `limit` traces with a span `f` keeps,
/// their matched spans capped at `spss`, each carrying `proj`'s values,
/// and their roots. `limit` and `spss` are the request's, already
/// validated positive by the caller. Issue it with `final = 1`, as every
/// read of these tables is; clustered, pass the distributed table names —
/// spans are sharded by trace, so each trace's rows are on one shard.
///
/// # Panics
///
/// If a filter was compiled for another window, as
/// [`super::predicate::span_membership_sql`] does: a resource subquery
/// bounded by one window's days inside a statement bounded by another is a
/// wrong answer.
pub fn search_sql(
    spans_table: &str,
    traces_table: &str,
    w: WindowSql,
    f: &SearchFilter,
    proj: &Projection,
    limit: u32,
    spss: u32,
) -> String {
    for p in f.predicates() {
        assert!(
            p.composes_with(w),
            "search_sql: the predicate was compiled for a different window"
        );
    }
    let trace_read = fill(
        TRACE_READ,
        &[
            ("traces", traces_table),
            ("root_service", &ceiling_sql("r.4")),
            ("root_name", &ceiling_sql("r.5")),
        ],
    );
    let (time, bucket, day) = (
        w.span_time_clause(),
        w.span_bucket_clause(),
        w.span_day_clause(),
    );
    let (limit, spss, projection) = (limit.to_string(), spss.to_string(), proj.sql());
    let common: [(&str, &str); 9] = [
        ("spans", spans_table),
        ("time", &time),
        ("bucket", &bucket),
        ("day", &day),
        ("limit", &limit),
        ("spss", &spss),
        ("projection", &projection),
        ("trace_read", &trace_read),
        ("traces", traces_table),
    ];
    match f {
        SearchFilter::One(p) => {
            let mut values: Vec<(&str, &str)> = common.to_vec();
            values.push(("predicate", p.sql()));
            fill(SEARCH, &values)
        }
        SearchFilter::Later { selector, later } => {
            let mut values: Vec<(&str, &str)> = common.to_vec();
            values.push(("predicate", selector.sql()));
            values.push(("later", later.sql()));
            fill(SEARCH_LATER, &values)
        }
        SearchFilter::Tree {
            filters,
            holds,
            guards,
        } => {
            let n = filters.len();
            let join = |each: &dyn Fn(usize) -> String, sep: &str| -> String {
                (1..=n).map(each).collect::<Vec<_>>().join(sep)
            };
            let flags = join(&|i| format!("({}) AS f{i}", filters[i - 1].sql()), ", ");
            let any = join(&|i| format!("f{i}"), " OR ");
            let counts = join(&|i| format!("max(f{i}) AS c{i}"), ", ");
            let lasts = join(
                &|i| match &guards[i - 1] {
                    None => format!("maxIf(start_ns, f{i})"),
                    Some(g) => format!("if({g}, maxIf(start_ns, f{i}), 0)"),
                },
                ", ",
            );
            let member = join(
                &|i| match &guards[i - 1] {
                    None => format!("x.{}", 5 + i),
                    Some(g) => format!("(x.{} AND {g})", 5 + i),
                },
                " OR ",
            );
            let fnames = join(&|i| format!("f{i}"), ", ");
            let mut values: Vec<(&str, &str)> = common.to_vec();
            values.extend([
                ("flags", flags.as_str()),
                ("any", any.as_str()),
                ("counts", counts.as_str()),
                ("lasts", lasts.as_str()),
                ("holds", holds.as_str()),
                ("member", member.as_str()),
                ("fnames", fnames.as_str()),
            ]);
            fill(SEARCH_TREE, &values)
        }
        SearchFilter::Grouped {
            selector,
            before,
            all,
            key,
        } => {
            fn text(p: &Option<SpanPredicate>) -> &str {
                p.as_ref().map_or("true", |p| p.sql())
            }
            let (pre, later) = (text(before), text(all));
            let demand = key.off_path.as_ref().map_or_else(String::new, |off| {
                format!(
                    "\n              AND throwIf(({pre}) AND ({off}), {}) = 0",
                    crate::logql::escape::ch_string(OFF_PATH_DEMAND)
                )
            });
            let grp_type = key.value_type.as_deref().unwrap_or("''");
            let mut values: Vec<(&str, &str)> = common.to_vec();
            values.extend([
                ("predicate", selector.sql()),
                ("pre", pre),
                ("later", later),
                ("grp", key.value.as_str()),
                ("grp_type", grp_type),
                ("demand", demand.as_str()),
            ]);
            fill(SEARCH_GROUPED, &values)
        }
        SearchFilter::Aggregated {
            selector,
            before,
            all,
            agg,
        } => {
            let pre = before.as_ref().map_or("true", |p| p.sql());
            let later = all.as_ref().map_or("true", |p| p.sql());
            let demand = agg.off_path.as_ref().map_or_else(String::new, |off| {
                format!(
                    "\n              AND throwIf(({pre}) AND ({off}), {}) = 0",
                    crate::logql::escape::ch_string(AGGREGATE_OFF_PATH_DEMAND)
                )
            });
            let mut values: Vec<(&str, &str)> = common.to_vec();
            values.extend([
                ("predicate", selector.sql()),
                ("before", pre),
                ("later", later),
                ("pass", agg.pass.as_str()),
                ("agg_text", agg.text.as_str()),
                ("agg_type", agg.value_type.as_str()),
                ("demand", demand.as_str()),
            ]);
            fill(SEARCH_AGGREGATED, &values)
        }
    }
}

/// A compiled search: the statement, the projection its rows decode by,
/// and the messages its `!` demands raise.
pub struct SearchStatement {
    sql: String,
    projection: Projection,
    demands: Vec<String>,
    grouping: Option<Grouping>,
}

/// A statement's one `by()` (issue #592 part 2): the group key's display,
/// `by(<field>)`, and whether a `coalesce()` after it merges the groups
/// back, so the response carries none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grouping {
    pub display: String,
    pub coalesced: bool,
    /// An aggregate stage's value rather than a `by()` key (issue #592
    /// part 3): decoded by [`aggregate_value`] and never counted against
    /// the distinct-group cap.
    pub aggregate: bool,
}

impl SearchStatement {
    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn projection(&self) -> &Projection {
        &self.projection
    }

    /// The messages the statement's `throwIf` demands raise: the server
    /// reports any `throwIf` as `Code: 395`, so a route turns that code
    /// into a `400` only when the message is one of these.
    pub fn demands(&self) -> &[String] {
        &self.demands
    }

    /// The statement's `by()`, when it has one; its rows are then
    /// [`SearchGroupedRow`]s.
    pub fn grouping(&self) -> Option<&Grouping> {
        self.grouping.as_ref()
    }
}

/// A query's `|` stages, as the search statement serves them (issues #592
/// parts 1 and 2): the later `{…}` filters' bodies, in written order — all
/// of them, and those before `by()` — the one `by()` key, whether a
/// `coalesce()` after it merges its groups back, and the `select()` fields.
struct Pipeline<'a> {
    before: Vec<&'a FieldExpr>,
    all: Vec<&'a FieldExpr>,
    by: Option<(&'a Field, GroupKeySql)>,
    coalesced: bool,
    selected: Vec<Field>,
    /// The one aggregate stage (issue #592 part 3); `before` then holds the
    /// filters written before it.
    aggregate: Option<&'a PipelineStage>,
}

/// [`Pipeline`] of `query`. Only a single `{…}` selector followed by later
/// `{…}` filters, `select()`, one `by()` on a key [`group_key_sql`] serves
/// and `coalesce()` is the statement's; a `coalesce()` with no `by()`
/// before it changes nothing. A second `by()`, a `by()` after
/// `coalesce()`, any other key or any other shape is refused naming #592,
/// an aggregate naming part 3.
fn pipeline_of(query: &Query) -> Result<Pipeline<'_>, PlanError> {
    let refused = |what: &str, target: &str| {
        PlanError::UnsupportedField(format!(
            "{what} is not supported by the search statement yet (issue {target})"
        ))
    };
    let mut out = Pipeline {
        before: Vec::new(),
        all: Vec::new(),
        by: None,
        coalesced: false,
        selected: Vec::new(),
        aggregate: None,
    };
    let mut coalesce_seen = false;
    for stage in &query.pipeline {
        match stage {
            PipelineStage::Filter(SpansetExpr::Filter(f)) => {
                if let Some(body) = f.body.as_ref() {
                    if out.by.is_none() && out.aggregate.is_none() {
                        out.before.push(body);
                    }
                    out.all.push(body);
                }
            }
            PipelineStage::Filter(_) => {
                return Err(refused("a spanset operation as a pipeline stage", "#592"));
            }
            PipelineStage::Select { fields } => out.selected.extend(fields.iter().cloned()),
            PipelineStage::By { key } => {
                if out.aggregate.is_some() {
                    return Err(refused("a by() stage with an aggregate", "#592"));
                }
                if out.by.is_some() {
                    return Err(refused("a second by() stage", "#592"));
                }
                if coalesce_seen {
                    return Err(refused("a by() stage after coalesce()", "#592"));
                }
                let FieldExpr::Field(field) = key else {
                    return Err(refused("a by() over an expression", "#592"));
                };
                let Some(sql) = group_key_sql(field) else {
                    return Err(refused(&format!("by({field})"), "#592"));
                };
                out.by = Some((field, sql));
            }
            PipelineStage::Coalesce => {
                coalesce_seen = true;
                if out.by.is_some() || out.aggregate.is_some() {
                    out.coalesced = true;
                }
            }
            PipelineStage::Aggregate { .. } => {
                if out.by.is_some() {
                    return Err(refused("an aggregate with a by() stage", "#592"));
                }
                if out.aggregate.is_some() {
                    return Err(refused("a second aggregate stage", "#592"));
                }
                out.aggregate = Some(stage);
            }
            PipelineStage::Metric(_)
            | PipelineStage::MetricSecondStage(_)
            | PipelineStage::Compare { .. } => {
                return Err(refused("a metrics stage", "#592"));
            }
        }
    }
    if !query.pipeline.is_empty()
        && !matches!(query.spanset, SpansetExpr::Filter(_))
        && !super::structural::holds_structural(&query.spanset)
    {
        return Err(refused(
            "a pipeline stage after a spanset operation",
            "#592",
        ));
    }
    Ok(out)
}

/// The AND of the later filters' bodies, in written order; `None` when
/// there is none.
fn later_body(later: &[&FieldExpr]) -> Option<FieldExpr> {
    let mut bodies = later.iter().map(|b| (*b).clone());
    let first = bodies.next()?;
    Some(bodies.fold(first, |acc, b| FieldExpr::Binary {
        op: FieldOp::Bool(BoolOp::And),
        lhs: Box::new(acc),
        rhs: Box::new(b),
    }))
}

/// A number as today's writer reads one from an attribute's stored text:
/// Rust's `f64` grammar, finite (`numeric_val_num`).
const NUMBER_TEXT: &str = "[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?";

/// Issue #592 part 3: one aggregate stage's SQL over the spans `cond`
/// keeps, by today's `aggregate_value` (`search_eval.rs`): the pass is the
/// scalar compared with the planner's own threshold; the response value
/// is `count()` an `Int64`, a duration in nanoseconds (`DurationI` exact,
/// `DurationF` from the `f64` sum), an attribute's `min`/`max` `AggInt`
/// when the first span holding the extreme stored an `Int64`, its `sum`
/// `AggInt` when every contributor did, and otherwise `AggDouble`.
fn aggregate_sql(
    op: pulsus_traceql::AggregateOp,
    field: Option<&FieldExpr>,
    cmp: pulsus_traceql::ComparisonOp,
    value: &pulsus_traceql::Value,
    cond: &str,
    ctx: &PredicateCtx<'_>,
) -> Result<AggregateSql, PlanError> {
    use pulsus_traceql::{AggregateOp as A, ComparisonOp as C};
    let threshold = crate::traces::search_plan::aggregate_threshold(op, &field.cloned(), value)?;
    let sql_op = match cmp {
        C::Eq => "=",
        C::Neq => "!=",
        C::Gt => ">",
        C::Gte => ">=",
        C::Lt => "<",
        C::Lte => "<=",
        C::Re | C::Nre => {
            return Err(PlanError::TypeMismatch(
                "aggregate filters do not support regex operators".to_string(),
            ));
        }
    };
    let t = format!("toFloat64('{threshold:?}')");
    let refuse = |what: String| {
        PlanError::UnsupportedField(format!(
            "{what} is not supported by the search statement (issue #592)"
        ))
    };
    let out = |scalar: String, contributors: String, text: String, value_type: String, off_path| {
        AggregateSql {
            pass: format!("{contributors} > 0 AND ({scalar}) {sql_op} {t}"),
            text,
            value_type,
            off_path,
        }
    };
    Ok(match (op, field) {
        (A::Count, None) => out(
            format!("toFloat64(countIf({cond}))"),
            format!("countIf({cond})"),
            format!("toString(countIf({cond}))"),
            "'Int64'".to_string(),
            None,
        ),
        (_, Some(FieldExpr::Field(Field::Intrinsic(pulsus_traceql::Intrinsic::Duration)))) => {
            let fsum = ordered_sum("toFloat64(duration_ns)", cond);
            let (scalar, text, ty) = match op {
                A::Sum => (fsum.clone(), format!("toString({fsum})"), "'DurationF'"),
                A::Avg => (
                    format!("{fsum} / countIf({cond})"),
                    format!("toString(intDiv(sumIf(duration_ns, {cond}), countIf({cond})))"),
                    "'DurationI'",
                ),
                A::Min => (
                    format!("toFloat64(minIf(duration_ns, {cond}))"),
                    format!("toString(minIf(duration_ns, {cond}))"),
                    "'DurationI'",
                ),
                A::Max => (
                    format!("toFloat64(maxIf(duration_ns, {cond}))"),
                    format!("toString(maxIf(duration_ns, {cond}))"),
                    "'DurationI'",
                ),
                A::Count => return Err(refuse(format!("{op}(duration)"))),
            };
            out(
                scalar,
                format!("countIf({cond})"),
                text,
                ty.to_string(),
                None,
            )
        }
        (_, Some(FieldExpr::Field(f @ Field::Attribute { scope, key }))) => {
            let Some((text_sql, kind)) = super::projection::aggregate_argument_sql(f, ctx)? else {
                return Err(refuse(format!("{op}({f})")));
            };
            let plain = format!("replaceRegexpOne({text_sql}, '^[+]', '')");
            let number = crate::traces::filter::anchored_regex_sql(NUMBER_TEXT)?;
            let v = format!(
                "if(match({text_sql}, {number}) AND isFinite(toFloat64OrZero({plain})), \
                 toFloat64OrZero({plain}), NULL)"
            );
            let contrib = format!("({cond}) AND isNotNull({v})");
            let (scalar, ty) = match op {
                A::Sum => (
                    ordered_sum(&format!("assumeNotNull({v})"), &contrib),
                    format!(
                        "if(countIf(({contrib}) AND ({kind}) != 'Int64') = 0, 'AggInt', 'AggDouble')"
                    ),
                ),
                A::Avg => (
                    format!(
                        "{} / countIf({contrib})",
                        ordered_sum(&format!("assumeNotNull({v})"), &contrib)
                    ),
                    "'AggDouble'".to_string(),
                ),
                A::Min => (
                    format!("minIf(assumeNotNull({v}), {contrib})"),
                    format!(
                        "if(argMinIf({kind}, (assumeNotNull({v}), start_ns, span_id), {contrib}) = 'Int64', 'AggInt', 'AggDouble')"
                    ),
                ),
                A::Max => (
                    format!("maxIf(assumeNotNull({v}), {contrib})"),
                    format!(
                        "if(argMinIf({kind}, (-assumeNotNull({v}), start_ns, span_id), {contrib}) = 'Int64', 'AggInt', 'AggDouble')"
                    ),
                ),
                A::Count => return Err(refuse(format!("{op}({f})"))),
            };
            let off_path = Some(off_path_at(*scope, key, ctx));
            out(
                scalar.clone(),
                format!("countIf({contrib})"),
                format!("toString({scalar})"),
                ty,
                off_path,
            )
        }
        _ => {
            return Err(refuse(format!(
                "{op}({})",
                field.map_or_else(String::new, |f| f.to_string())
            )));
        }
    })
}

/// Issue #592 part 3: `key` held off its typed path at `scope` — a
/// key-value list flattened under the key (its sub-object is not empty) or
/// a value in that scope's `attrs_other`. The unscoped key tests every
/// scope of the chain: the span, its resource row, an event, a link, the
/// instrumentation scope. Today's engine reads the key's holder whatever
/// its kind, so any such holder hands the request over.
pub(super) fn off_path_at(
    scope: pulsus_traceql::AttrScope,
    key: &str,
    ctx: &PredicateCtx<'_>,
) -> String {
    use pulsus_traceql::AttrScope as S;
    let lit = crate::logql::escape::ch_string(key);
    let path = pulsus_clickhouse::json_column::escape_json_path(key);
    let ident = crate::logql::escape::ch_ident(&path);
    let one = |root: &str, other: &str| {
        format!(
            "(dynamicType({root}.{ident}) = 'None' AND (position({other}, {lit}) > 0 \
             OR toString({root}.^{ident}) != '{{}}'))"
        )
    };
    let set = |arr: &str| {
        format!(
            "arrayExists((d, s, o) -> dynamicType(d) = 'None' AND (position(o, {lit}) > 0 \
             OR toString(s) != '{{}}'), {arr}.attrs.{ident}, {arr}.attrs.^{ident}, {arr}.attrs_other)"
        )
    };
    let resource = format!(
        "resource_id IN (SELECT resource_id FROM {} WHERE {} AND {})",
        ctx.resources_table,
        ctx.window.resources_day_clause(),
        one("attrs", "attrs_other"),
    );
    match scope {
        S::Span => one("attrs", "attrs_other"),
        S::Resource => resource,
        S::Event => set("events"),
        S::Link => set("links"),
        S::Instrumentation => one("scope_attrs", "scope_attrs_other"),
        S::Unscoped => format!(
            "({} OR {} OR {} OR {} OR {})",
            off_path_at(S::Span, key, ctx),
            off_path_at(S::Resource, key, ctx),
            off_path_at(S::Event, key, ctx),
            off_path_at(S::Link, key, ctx),
            off_path_at(S::Instrumentation, key, ctx),
        ),
    }
}

/// The `f64` sum of `x` over the spans `cond` keeps, added in today's
/// order — the spanset's, ascending `(start_ns, span_id)` — from `-0.0`,
/// as `Iterator::sum` folds it: float addition is not associative, so a
/// sum the database adds in its own order can differ in the last bits.
fn ordered_sum(x: &str, cond: &str) -> String {
    format!(
        "arrayFold((acc, e) -> acc + e.1, arraySort(e -> (e.2, e.3), \
         groupArrayIf(({x}, start_ns, span_id), {cond})), toFloat64('-0'))"
    )
}

/// An aggregate's response value (issue #592 part 3), as today's
/// `aggregate_value` builds its wire: a count and an `AggInt` as an `Int`
/// (an `f64` cast as today casts it), an `AggDouble` as its raw bits, a
/// duration as Go's duration text.
pub(crate) fn aggregate_value(text: &str, value_type: &str) -> Result<GroupValue, String> {
    let f = || {
        text.parse::<f64>()
            .map_err(|e| format!("aggregate {text:?}: {e}"))
    };
    Ok(match value_type {
        "Int64" => GroupValue::Int(text.parse().map_err(|e| format!("count {text:?}: {e}"))?),
        "AggInt" => GroupValue::Int(f()? as i64),
        "AggDouble" => GroupValue::Double(f()?.to_bits()),
        "DurationI" => GroupValue::Str(crate::traces::search_eval::go_duration_string(
            text.parse()
                .map_err(|e| format!("duration {text:?}: {e}"))?,
        )),
        "DurationF" => GroupValue::Str(crate::traces::search_eval::go_duration_string(f()? as i64)),
        other => return Err(format!("an aggregate value of type {other} is not decoded")),
    })
}

/// Compiles `query` to the search statement. Its `|` stages are
/// [`pipeline_of`]'s; a structural operator is #593's; a set field projected from a
/// comparison that holds it twice beside arithmetic, or under `!` or inside
/// a boolean-valued operand, is refused (`projection.rs`); everything the
/// predicate compiler refuses is returned as it refused. `query.hints` are
/// accepted and ignored, as today.
pub fn compile_search(
    query: &Query,
    ctx: &PredicateCtx<'_>,
    spans_table: &str,
    traces_table: &str,
    limit: u32,
    spss: u32,
) -> Result<SearchStatement, PlanError> {
    let pipeline = pipeline_of(query)?;
    // Issue #593: a spanset holding a structural operator is one
    // membership predicate, the selector of every template.
    let mut filter = if super::structural::holds_structural(&query.spanset) {
        SearchFilter::One(super::structural::compile_membership_in(
            &query.spanset,
            ctx,
            spans_table,
        )?)
    } else {
        compile_search_filter(&query.spanset, ctx)?
    };
    let compiled = |bodies: &[&FieldExpr]| -> Result<Option<SpanPredicate>, PlanError> {
        later_body(bodies)
            .map(|body| compile_span_predicate_in(&body, ctx))
            .transpose()
    };
    let mut grouping = None;
    if let Some(PipelineStage::Aggregate {
        op,
        field,
        cmp,
        value,
    }) = pipeline.aggregate
    {
        let SearchFilter::One(selector) = filter else {
            unreachable!("pipeline_of refuses a stage after anything but one filter")
        };
        let before = compiled(&pipeline.before)?;
        let cond = before.as_ref().map_or("true", |p| p.sql()).to_string();
        let agg = aggregate_sql(*op, field.as_ref(), *cmp, value, &cond, ctx)?;
        filter = SearchFilter::Aggregated {
            selector,
            before,
            all: compiled(&pipeline.all)?,
            agg,
        };
        grouping = Some(Grouping {
            display: match field {
                Some(FieldExpr::Field(f)) => format!("{op}({f})"),
                _ => format!("{op}()"),
            },
            coalesced: pipeline.coalesced,
            aggregate: true,
        });
    } else if let Some((field, key)) = &pipeline.by {
        let SearchFilter::One(selector) = filter else {
            unreachable!("pipeline_of refuses a stage after anything but one filter")
        };
        filter = SearchFilter::Grouped {
            selector,
            before: compiled(&pipeline.before)?,
            all: compiled(&pipeline.all)?,
            key: key.clone(),
        };
        grouping = Some(Grouping {
            display: format!("by({field})"),
            coalesced: pipeline.coalesced,
            aggregate: false,
        });
    } else if let Some(later) = compiled(&pipeline.all)? {
        let SearchFilter::One(selector) = filter else {
            unreachable!("pipeline_of refuses a stage after anything but one filter")
        };
        filter = SearchFilter::Later { selector, later };
    }
    let mut filters = Vec::new();
    filters_of(&query.spanset, &mut filters)?;
    let mut bodies: Vec<&FieldExpr> = filters.iter().filter_map(|f| f.body.as_ref()).collect();
    bodies.extend(pipeline.all.iter().copied());
    let projection = Projection::of_filters(&bodies, &pipeline.selected, ctx)?;
    let mut demands: Vec<String> = Vec::new();
    for p in filter.predicates() {
        for m in p.demand_messages() {
            if !demands.contains(m) {
                demands.push(m.clone());
            }
        }
    }
    if let SearchFilter::Grouped { key, .. } = &filter
        && key.off_path.is_some()
    {
        demands.push(OFF_PATH_DEMAND.to_string());
    }
    if let SearchFilter::Aggregated { agg, .. } = &filter
        && agg.off_path.is_some()
    {
        demands.push(AGGREGATE_OFF_PATH_DEMAND.to_string());
    }
    if projection.has_off_path() {
        demands.push(SELECT_OFF_PATH_DEMAND.to_string());
    }
    let sql = search_sql(
        spans_table,
        traces_table,
        ctx.window,
        &filter,
        &projection,
        limit,
        spss,
    );
    Ok(SearchStatement {
        sql,
        projection,
        demands,
        grouping,
    })
}

/// Issue #591 part 3's coverage: the search statement for `plan`, when
/// `compile_search` accepts its spanset and its `|` stages (issue #592
/// part 1) under its own window, `limit` and `spss`; `None` sends it to
/// today's engine, which answers it unchanged.
pub fn plan_statement(
    plan: &SearchPlan,
    spans_table: &str,
    traces_table: &str,
    resources_table: &str,
) -> Option<SearchStatement> {
    let window = WindowSql::start_closed_end_open(plan.window.start_ns, plan.window.end_ns);
    let ctx = PredicateCtx {
        window,
        resources_table,
    };
    let query = Query {
        spanset: plan.spanset.clone(),
        pipeline: plan.pipeline.clone(),
        hints: Vec::new(),
    };
    compile_search(
        &query,
        &ctx,
        spans_table,
        traces_table,
        plan.limit,
        plan.spss,
    )
    .ok()
}

/// [`decode_search`], charging every retained entry against `budget`
/// before the allocation it pays for, with today's engine's own charges in
/// today's order (issue #591 part 3, section 3.2): the trace buffer, then
/// per trace its span buffer, per span its attribute buffer and values,
/// and the trace's root strings. A breach is today's
/// `QueryTooBroad(ScanBudgetBytes)`; a malformed value a decode error.
pub(crate) fn decode_search_charged(
    rows: Vec<SearchTraceRow>,
    proj: &Projection,
    limit: u32,
    budget: &mut ByteBudget,
) -> Result<SearchOutput, ReadError> {
    let decode = |m: String| ReadError::Clickhouse(ChError::Decode(m));
    let capacity = proj.attribute_capacity();
    budget.charge(output_reserve_bytes(rows.len()))?;
    let mut traces = Vec::with_capacity(rows.len());
    for row in rows {
        budget.charge(match_reserve_bytes(row.spans.len()))?;
        let mut spans = Vec::with_capacity(row.spans.len());
        for s in row.spans {
            budget.charge(summary_reserve_bytes(capacity))?;
            let mut attributes = Vec::with_capacity(capacity);
            let name = proj.decode_charged(&s.projected, &mut attributes, budget)?;
            spans.push(SpanSummary::new(
                s.span_id,
                name,
                s.start_ns,
                s.duration_ns,
                attributes,
            ));
        }
        // The strings move from the row: nothing is allocated for them.
        budget.charge(row.root_service.len() + row.root_name.len())?;
        traces.push(TraceSearchResult {
            trace_id: row.trace_id,
            root: RootSummary {
                service: row.root_service,
                name: row.root_name,
                start_ns: 0,
                duration_ns: 0,
            },
            trace_start_ns: row.start_ns,
            trace_duration_ns: row.duration_ns,
            matched: u32::try_from(row.matched)
                .map_err(|_| decode(format!("matched {} exceeds u32", row.matched)))?,
            spans,
            groups: None,
        });
    }
    let returned = u32::try_from(traces.len())
        .map_err(|_| decode("more traces than u32 counts".to_string()))?;
    Ok(SearchOutput {
        traces,
        partial: false,
        returned,
        limit,
    })
}

/// A `by()` key's value from the statement's text and stored type (issue
/// #592 part 2), as today's engine types it: `Int64`, `Float64` (its group
/// bits), `Bool` and `String` in their own arms, an array as the JSON the
/// writer rendered it to, a span without the key the `nil` group, and a
/// column key, which has no type, its text.
pub(crate) fn group_value(text: &str, value_type: &str) -> Result<GroupValue, String> {
    Ok(match value_type {
        "" | "String" => GroupValue::Str(text.to_string()),
        "Int64" => GroupValue::Int(
            text.parse::<i64>()
                .map_err(|e| format!("a by() Int64 {text:?}: {e}"))?,
        ),
        "Float64" => GroupValue::Double(group_double_bits(
            text.parse::<f64>()
                .map_err(|e| format!("a by() Float64 {text:?}: {e}"))?,
        )),
        "Bool" => GroupValue::Bool(text == "true"),
        "None" => GroupValue::Nil,
        t if t.starts_with("Array(") => GroupValue::Str(rendered_array(text)?),
        other => return Err(format!("a by() key stored as {other} is not decoded")),
    })
}

/// The payload a [`group_value`] holds, learned before it is built.
fn group_value_payload(text: &str, value_type: &str) -> Result<usize, String> {
    Ok(match value_type {
        "" | "String" => text.len(),
        t if t.starts_with("Array(") => rendered_array_len(text)?,
        _ => 0,
    })
}

/// [`decode_search_charged`] for a grouped statement (issue #592 part 2),
/// in today's engine's order:
/// 1. every group of every row goes to `counter`, so a breach is today's
///    `TraceSearchSeriesCap` before anything is built;
/// 2. the flat response, as [`decode_search_charged`] builds it;
/// 3. unless `coalesce()` merged them, each trace's groups that kept a
///    span, charged as `build_span_set_groups` charges them: the group
///    vector, per group its attribute, its span buffer and each span;
/// 4. the counter's charge is released, as on today's success path.
pub(crate) fn decode_search_grouped_charged(
    rows: Vec<SearchGroupedRow>,
    proj: &Projection,
    grouping: &Grouping,
    limit: u32,
    budget: &mut ByteBudget,
    counter: &mut GroupCardinalityCounter,
) -> Result<SearchOutput, ReadError> {
    let decode = |m: String| ReadError::Clickhouse(ChError::Decode(m));
    let value_of = |g: &SearchGroupTuple| {
        if grouping.aggregate {
            aggregate_value(&g.value, &g.value_type)
        } else {
            group_value(&g.value, &g.value_type)
        }
    };
    if !grouping.aggregate {
        for row in &rows {
            for g in &row.groups {
                let value = group_value(&g.value, &g.value_type).map_err(decode)?;
                counter.observe(&vec![value], budget)?;
            }
        }
    }
    let mut trace_rows = Vec::with_capacity(rows.len());
    let mut row_groups = Vec::with_capacity(rows.len());
    for row in rows {
        trace_rows.push(SearchTraceRow {
            trace_id: row.trace_id,
            root_service: row.root_service,
            root_name: row.root_name,
            start_ns: row.start_ns,
            duration_ns: row.duration_ns,
            last: row.last,
            matched: row.matched,
            spans: row.spans,
        });
        row_groups.push(row.groups);
    }
    let mut out = decode_search_charged(trace_rows, proj, limit, budget)?;
    if !grouping.coalesced {
        let capacity = proj.attribute_capacity();
        for (trace, groups) in out.traces.iter_mut().zip(row_groups) {
            let kept: Vec<SearchGroupTuple> =
                groups.into_iter().filter(|g| g.matched > 0).collect();
            budget.charge(groups_reserve_bytes(kept.len()))?;
            let mut built = Vec::with_capacity(kept.len());
            for g in kept {
                // An aggregate's value is built, then charged, as today's
                // `run_pipeline` charges it after `aggregate_value`.
                let value = value_of(&g).map_err(decode)?;
                let payload = if grouping.aggregate {
                    value.payload_bytes()
                } else {
                    group_value_payload(&g.value, &g.value_type).map_err(decode)?
                };
                budget.charge(
                    std::mem::size_of::<(String, GroupValue)>() + grouping.display.len() + payload,
                )?;
                // One slot, as `groups_retained_bytes` counts it.
                let attributes = vec![(grouping.display.clone(), value)];
                budget.charge(group_reserve_bytes(g.spans.len()))?;
                let mut spans = Vec::with_capacity(g.spans.len());
                for s in g.spans {
                    budget.charge(summary_reserve_bytes(capacity))?;
                    let mut attrs = Vec::with_capacity(capacity);
                    let name = proj.decode_charged(&s.projected, &mut attrs, budget)?;
                    spans.push(SpanSummary::new(
                        s.span_id,
                        name,
                        s.start_ns,
                        s.duration_ns,
                        attrs,
                    ));
                }
                built.push(SpanSetGroup {
                    attributes,
                    matched: u32::try_from(g.matched)
                        .map_err(|_| decode(format!("matched {} exceeds u32", g.matched)))?,
                    spans,
                });
            }
            trace.groups = Some(built);
        }
    }
    counter.release(budget);
    Ok(out)
}

/// A row the decode could not read: a projected value that does not parse
/// as its kind, or a count out of range. Never a default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchDecodeError(pub String);

/// The statement's rows as today's [`SearchOutput`] (section 5.4), in the
/// order they arrive, which is the answer's. The root span's own start
/// and duration are `0`: `traces` stores no root span window, and neither
/// reaches the wire. The statement has no candidate cap, so the output is
/// never partial. It is [`decode_search_charged`] under an unbounded
/// budget.
pub fn decode_search(
    rows: Vec<SearchTraceRow>,
    proj: &Projection,
    limit: u32,
) -> Result<SearchOutput, SearchDecodeError> {
    decode_search_charged(rows, proj, limit, &mut ByteBudget::new(usize::MAX))
        .map_err(|e| SearchDecodeError(e.to_string()))
}

#[cfg(test)]
mod charge_tests {
    use super::*;
    use crate::traces::exec::{ByteBudget, RETAINED_ENTRY_OVERHEAD, output_reserve_bytes};
    use crate::traces::search_eval::TraceMatch;
    use crate::traces::spans::rows::{SearchProjected, SearchSpanTuple};

    /// Section 6.1's four groups: a string, an int, `name` and an array.
    fn statement() -> SearchStatement {
        let q = pulsus_traceql::parse(
            r#"{ span.foo = "bar" || span.n = 5 || name = "x" || span.tags = "g" }"#,
        )
        .expect("parses");
        let w = WindowSql::start_closed_end_open(1_000_000_000_000, 2_000_000_000_000);
        let ctx = PredicateCtx {
            window: w,
            resources_table: "resources",
        };
        compile_search(&q, &ctx, "spans", "traces", 20, 3).expect("compiles")
    }

    fn projected(group: u8, value: &str, kind: &str) -> SearchProjected {
        SearchProjected {
            group,
            value: value.to_string(),
            kind: kind.to_string(),
        }
    }

    fn span(id: u8, projected: Vec<SearchProjected>) -> SearchSpanTuple {
        SearchSpanTuple {
            span_id: [id; 8],
            start_ns: 1,
            duration_ns: 1,
            service: "svc".to_string(),
            projected,
        }
    }

    /// Two traces: a span with a string and an int, a span with only
    /// `name`, then, last, a span with an array, so no later charge can
    /// hide an over-charge on it.
    fn rows() -> Vec<SearchTraceRow> {
        vec![
            SearchTraceRow {
                trace_id: [1; 16],
                root_service: "frontend".to_string(),
                root_name: "GET /".to_string(),
                start_ns: 1,
                duration_ns: 2,
                last: 1,
                matched: 5,
                spans: vec![
                    span(
                        1,
                        vec![projected(1, "bar", "String"), projected(2, "5", "Int64")],
                    ),
                    span(2, vec![projected(3, "x", "String")]),
                ],
            },
            SearchTraceRow {
                trace_id: [2; 16],
                root_service: "cart".to_string(),
                root_name: "op".to_string(),
                start_ns: 1,
                duration_ns: 2,
                last: 1,
                matched: 1,
                spans: vec![span(
                    3,
                    vec![projected(
                        4,
                        r#"[["String","g"],["Int64","7"]]"#,
                        "Array(Nullable(String))",
                    )],
                )],
            },
        ]
    }

    /// Everything today's engine charges for the same response: the trace
    /// buffer, then per trace `TraceMatch::retained_bytes` and the root
    /// strings.
    fn todays_charge(out: &SearchOutput) -> usize {
        output_reserve_bytes(out.traces.len())
            + out
                .traces
                .iter()
                .map(|t| {
                    std::mem::size_of::<TraceMatch>()
                        + RETAINED_ENTRY_OVERHEAD
                        + t.spans.capacity() * std::mem::size_of::<SpanSummary>()
                        + t.spans
                            .iter()
                            .map(SpanSummary::heap_payload_bytes)
                            .sum::<usize>()
                        + t.root.service.len()
                        + t.root.name.len()
                })
                .sum::<usize>()
    }

    /// Section 6.1: the decode charges exactly what today's engine charges
    /// for the response it builds; a budget of exactly that decodes, and
    /// one byte less is refused.
    #[test]
    fn the_decode_charges_what_todays_engine_charges() {
        let s = statement();
        let mut unbounded = ByteBudget::new(usize::MAX);
        let out = decode_search_charged(rows(), s.projection(), 20, &mut unbounded)
            .expect("decodes unbounded");
        let used = unbounded.used();
        eprintln!(
            "used {used}; attributes (len, capacity) {:?}",
            out.traces
                .iter()
                .flat_map(|t| t
                    .spans
                    .iter()
                    .map(|s| (s.attributes.len(), s.attributes.capacity())))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            used,
            todays_charge(&out),
            "the decode's charge against today's"
        );
        assert!(
            decode_search_charged(rows(), s.projection(), 20, &mut ByteBudget::new(used)).is_ok(),
            "a budget of exactly the charge decodes"
        );
        let refused =
            decode_search_charged(rows(), s.projection(), 20, &mut ByteBudget::new(used - 1));
        assert!(
            matches!(
                refused,
                Err(crate::logql::error::ReadError::QueryTooBroad(_))
            ),
            "one byte less is refused: {refused:?}"
        );
    }

    // ---------------------------------------------- issue #592 part 2

    /// A grouped trace: the flat spans of the union, and three groups — a
    /// string with two spans, an integer the filter after `by()` emptied,
    /// and the `nil` group with one span.
    fn grouped_rows() -> Vec<SearchGroupedRow> {
        let bar = || vec![projected(1, "bar", "String")];
        vec![SearchGroupedRow {
            trace_id: [3; 16],
            root_service: "frontend".to_string(),
            root_name: "GET /".to_string(),
            start_ns: 1,
            duration_ns: 2,
            last: 1,
            matched: 3,
            spans: vec![span(1, bar()), span(2, bar()), span(3, bar())],
            groups: vec![
                SearchGroupTuple {
                    value: "x".to_string(),
                    value_type: "String".to_string(),
                    matched: 2,
                    spans: vec![span(1, bar()), span(2, bar())],
                },
                SearchGroupTuple {
                    value: "5".to_string(),
                    value_type: "Int64".to_string(),
                    matched: 0,
                    spans: Vec::new(),
                },
                SearchGroupTuple {
                    value: String::new(),
                    value_type: "None".to_string(),
                    matched: 1,
                    spans: vec![span(3, bar())],
                },
            ],
        }]
    }

    /// Issue #592 part 2, section 7.1: the grouped decode charges what
    /// today's engine charges for the same response, its groups included
    /// (`TraceMatch::retained_bytes` with `groups_retained_bytes`); it
    /// fits a budget of that plus the distinct group tuples the cap's
    /// counter holds while it runs, and one byte less is refused.
    #[test]
    fn the_grouped_decode_charges_what_todays_engine_charges() {
        use crate::traces::search_eval::{GroupValue, group_tuple_bytes, groups_retained_bytes};
        let q = pulsus_traceql::parse(r#"{ span.foo = "bar" } | by(span.k)"#).expect("parses");
        let w = WindowSql::start_closed_end_open(1_000_000_000_000, 2_000_000_000_000);
        let ctx = PredicateCtx {
            window: w,
            resources_table: "resources",
        };
        let s = compile_search(&q, &ctx, "spans", "traces", 20, 3).expect("compiles");
        let grouping = s.grouping().expect("grouped");
        let decode = |budget: &mut ByteBudget| {
            let mut counter = GroupCardinalityCounter::new(1_000);
            decode_search_grouped_charged(
                grouped_rows(),
                s.projection(),
                grouping,
                20,
                budget,
                &mut counter,
            )
        };
        let mut unbounded = ByteBudget::new(usize::MAX);
        let out = decode(&mut unbounded).expect("decodes unbounded");
        let used = unbounded.used();
        let groups = out.traces[0].groups.as_deref().expect("the trace's groups");
        assert_eq!(groups.len(), 2, "the emptied group is not in the response");
        let want = todays_charge(&out) + groups_retained_bytes(groups);
        assert_eq!(used, want, "the decode's charge against today's");
        let tuples: usize = [
            GroupValue::Str("x".to_string()),
            GroupValue::Int(5),
            GroupValue::Nil,
        ]
        .into_iter()
        .map(|v| group_tuple_bytes(&vec![v]))
        .sum();
        assert!(
            decode(&mut ByteBudget::new(used + tuples)).is_ok(),
            "a budget of the charge and the counter's tuples decodes"
        );
        let refused = decode(&mut ByteBudget::new(used + tuples - 1));
        assert!(
            matches!(
                refused,
                Err(crate::logql::error::ReadError::QueryTooBroad(_))
            ),
            "one byte less is refused: {refused:?}"
        );
    }

    /// Issue #592 part 3: an aggregate's one group charges what today's
    /// engine charges — `TraceMatch::retained_bytes` with
    /// `groups_retained_bytes` — and no distinct-group tuple: one byte less
    /// than that is refused.
    #[test]
    fn the_aggregated_decode_charges_what_todays_engine_charges() {
        use crate::traces::search_eval::groups_retained_bytes;
        let q =
            pulsus_traceql::parse(r#"{ span.foo = "bar" } | avg(duration) > 1ms"#).expect("parses");
        let w = WindowSql::start_closed_end_open(1_000_000_000_000, 2_000_000_000_000);
        let ctx = PredicateCtx {
            window: w,
            resources_table: "resources",
        };
        let s = compile_search(&q, &ctx, "spans", "traces", 20, 3).expect("compiles");
        let grouping = s.grouping().expect("an aggregate is one group");
        let bar = || vec![projected(1, "bar", "String")];
        let rows = || {
            vec![SearchGroupedRow {
                trace_id: [3; 16],
                root_service: "frontend".to_string(),
                root_name: "GET /".to_string(),
                start_ns: 1,
                duration_ns: 2,
                last: 1,
                matched: 2,
                spans: vec![span(1, bar()), span(2, bar())],
                groups: vec![SearchGroupTuple {
                    value: "2166666666".to_string(),
                    value_type: "DurationI".to_string(),
                    matched: 2,
                    spans: vec![span(1, bar()), span(2, bar())],
                }],
            }]
        };
        let decode = |budget: &mut ByteBudget| {
            let mut counter = GroupCardinalityCounter::new(1_000);
            decode_search_grouped_charged(
                rows(),
                s.projection(),
                grouping,
                20,
                budget,
                &mut counter,
            )
        };
        let mut unbounded = ByteBudget::new(usize::MAX);
        let out = decode(&mut unbounded).expect("decodes");
        let used = unbounded.used();
        let groups = out.traces[0]
            .groups
            .as_deref()
            .expect("the aggregate's group");
        assert_eq!(groups[0].attributes[0].0, "avg(duration)");
        assert_eq!(
            groups[0].attributes[0].1,
            GroupValue::Str("2.166666666s".to_string())
        );
        assert_eq!(used, todays_charge(&out) + groups_retained_bytes(groups));
        assert!(decode(&mut ByteBudget::new(used)).is_ok());
        assert!(matches!(
            decode(&mut ByteBudget::new(used - 1)),
            Err(crate::logql::error::ReadError::QueryTooBroad(_))
        ));
    }
}
