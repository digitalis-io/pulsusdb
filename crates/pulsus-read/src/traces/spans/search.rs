//! The TraceQL search statement (issue #590): `docs/TraceQL/sql-schema.md`
//! §5.2. One pure builder, `validated inputs -> String`, as
//! [`super::fetch`]'s are. The route (`exec.rs`'s `search_routed`) issues
//! it for every search, planned by [`plan_search_request`] (issue #602 part
//! 2b). Every `|` stage is [`super::fold`]'s fold
//! (issue #602 part 2a). The newest slice is [`search_sql_at`]'s; the loop
//! that widens it is `exec/newest_first.rs`'s (issue #595).
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
//! A spanset tree reads its spans once with one flag per filter, takes each
//! flag's maximum over the trace by a window function, and marks a span a
//! member when its flag and its `&&` guards hold (section 4.3): `&&` is
//! "both sides non-empty, the union of their spans", `||` the union
//! (`docs/api.md` §4.2). Since issue #602 part 2a that relation is the
//! fold's selector, so a tree reads the newest slice first and takes any
//! `|` stage. The earlier form, a `HAVING` over each trace's flags,
//! measured 2,034,612 rows and 50 ms on g1, against 10,034,868 rows and
//! 130 ms for the design's catalogue form, which builds a `trace_id IN` set
//! per filter in each of the two reads.
//!
//! ## The projection
//!
//! The detail read carries each matched span's projected values,
//! [`super::projection`]'s, and [`decode_search`] turns the rows into
//! today's [`SearchOutput`].

use std::collections::HashMap;

use pulsus_traceql::{
    BoolOp, Field, FieldExpr, Intrinsic, PipelineStage, Query, SpansetExpr, SpansetFilter, Value,
};

use super::fold::{Agg, AggArg, Contributor, Fold, Key, Src, Stage};
use super::numbering::{SCOPE_DETAIL, SCOPE_TOP, after_top_sql, keys_sql, returned_numbers_sql};
use super::predicate::{PredicateCtx, SpanPredicate, compile_span_predicate_in};
use super::projection::{
    Projection, ceiling_sql, group_key_sql, rendered_array, rendered_array_len,
};
use super::rows::{SearchGroupTuple, SearchGroupedRow, SearchTraceRow};
use super::tracelevel::{TraceLeaf, TraceValue, group_values_sql, per_trace_sql, trace_values_sql};
use crate::logql::error::ReadError;
use crate::traces::PlanError;
use crate::traces::exec::{
    ByteBudget, RootSummary, SearchOutput, TraceSearchResult, output_reserve_bytes,
};
use crate::traces::values::{
    GroupCardinalityCounter, GroupValue, SpanSetGroup, SpanSummary, group_double_bits,
    group_reserve_bytes, groups_reserve_bytes, match_reserve_bytes, summary_reserve_bytes,
};
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
const SEARCH: &str = r"WITH {scalars}(SELECT (groupArray(trace_id), groupArray(keys))
      FROM ({scope_top}SELECT trace_id, max(start_ns) AS last,
                   groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
            FROM {spans}
            WHERE {top_time}
              AND {top_bucket}
              AND {top_day}
              AND ({predicate})
            GROUP BY trace_id
            ORDER BY last DESC, trace_id ASC
            LIMIT {limit})) AS top{window_keys}{after_top}
SELECT m.trace_id AS trace_id, t.root_service AS root_service, t.root_name AS root_name,
       t.start_ns AS start_ns, t.end_ns - t.start_ns AS duration_ns,
       m.last AS last, m.matched AS matched, m.spans AS spans
FROM ({scope_detail}SELECT trace_id, max(start_ns) AS last, count() AS matched,
             arraySlice(arraySort(x -> (x.2, x.1),
                        groupArray((span_id, start_ns, duration_ns, service, {projection}))), 1, {spss}) AS spans
      FROM {spans}
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN
            {keys}
        AND {time}
        AND {bucket}
        AND {day}
        AND ({predicate})
      GROUP BY trace_id) AS m
{trace_read}";

/// The newest slice a statement's top-K reads first, and the
/// unit it doubles in (issue #595): the span table's bucket.
pub const SLICE_NS: i64 = 300_000_000_000;

/// The detail read's key set when the top-K read the whole window: the
/// buckets each chosen trace matched in.
const KEYS_WHOLE: &str = "(SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))";

/// A sliced statement's second scalar (issue #595): each chosen trace's
/// buckets in the window, from `traces`. A scalar, so a clustered
/// statement reads `traces` once on the initiator, never as a subquery of
/// the span read.
const WINDOW_KEYS: &str = ",
     (SELECT arrayFlatten(groupArray(arrayMap(k -> (k, trace_id), arrayFilter(k -> {within}, bk))))
      FROM (SELECT trace_id, groupUniqArrayArray(buckets) AS bk
            FROM {traces}
            WHERE {days} AND trace_id IN (SELECT arrayJoin(top.1))
            GROUP BY trace_id)) AS tb";

/// The detail read's key set when the top-K read a slice (issue #595):
/// the buckets each chosen trace matched in the slice and its buckets in
/// the window, so a trace `traces` has no row for still answers from the
/// slice.
const KEYS_SLICED: &str = "(SELECT arrayJoin(arrayConcat(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2)), tb)))";

/// Every search with a later filter, a `by()` or an aggregate (issue #602
/// part 2a): the top-K and the detail read are both [`super::fold`]'s
/// fold, the top-K over the window, a slice's candidates or, with no later
/// filter and no aggregate, the selector's spans alone.
const SEARCH_PIPELINE: &str = r"WITH {scalars}{candidates}(SELECT (groupArray(trace_id), groupArray(keys))
      FROM ({top})) AS top{window_keys}{after_top}{group_values}
SELECT m.trace_id AS trace_id, t.root_service AS root_service, t.root_name AS root_name,
       t.start_ns AS start_ns, t.end_ns - t.start_ns AS duration_ns,
       m.last AS last, m.matched AS matched, m.spans AS spans{groups}
FROM ({detail}) AS m
{trace_read}";

/// The plain top-K: the newest `limit` traces with a selector span in the
/// part of the window it reads.
const TOP_PLAIN: &str = r"{scope_top}SELECT trace_id, max(start_ns) AS last,
                   groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
            FROM {spans}
            WHERE {top_time}
              AND {top_bucket}
              AND {top_day}
              AND ({predicate})
            GROUP BY trace_id
            ORDER BY last DESC, trace_id ASC
            LIMIT {limit}";

/// A sliced fold's candidates: every trace with a selector span in the
/// slice and its buckets there, then each one's buckets in the window from
/// `traces`.
const CANDIDATES: &str = r"(SELECT (groupArray(trace_id), groupArray(keys))
      FROM ({scope_top}SELECT trace_id, groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
            FROM {spans}
            WHERE {top_time}
              AND {top_bucket}
              AND {top_day}
              AND ({predicate})
            GROUP BY trace_id)) AS cand,
     (SELECT arrayFlatten(groupArray(arrayMap(k -> (k, trace_id), arrayFilter(k -> {within}, bk))))
      FROM (SELECT trace_id, groupUniqArrayArray(buckets) AS bk
            FROM {traces}
            WHERE {days} AND trace_id IN (SELECT arrayJoin(cand.1))
            GROUP BY trace_id)) AS cand_keys,
     ";

/// The candidates' keys: their slice buckets and their window buckets.
const KEYS_CANDIDATES: &str = "(SELECT arrayJoin(arrayConcat(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), cand.1, cand.2)), cand_keys)))";

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
    /// Two or more filters joined by `&&`/`||`, in pre-order, with the
    /// texts of section 4.3's `{holds}` and each filter's guard (`None` =
    /// true).
    Tree {
        filters: Vec<SpanPredicate>,
        holds: String,
        guards: Vec<Option<String>>,
    },
    /// A spanset holding a structural operator (issue #602 part 2b):
    /// `leaves`, the OR of its filters, picks a slice's candidates, the
    /// traces with a span they keep in the slice; `member` is its
    /// membership, every relation reading the candidates' keys when sliced
    /// and the window when not.
    Structural {
        leaves: String,
        member: SpanPredicate,
    },
    /// A selector, `One`, `Tree` or `Structural`, then the `|` stages
    /// [`super::fold`] folds (issue #602 part 2a).
    Pipeline {
        selector: Box<SearchFilter>,
        fold: Fold,
    },
}

impl SearchFilter {
    fn predicates(&self) -> Vec<&SpanPredicate> {
        match self {
            SearchFilter::One(p) => vec![p],
            SearchFilter::Tree { filters, .. } => filters.iter().collect(),
            SearchFilter::Structural { member, .. } => vec![member],
            SearchFilter::Pipeline { selector, fold } => {
                let mut out = selector.predicates();
                out.extend(fold.filters.iter());
                out
            }
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
        .map(|f| {
            let body = body_of(f);
            let p = compile_span_predicate_in(&body, ctx)?;
            Ok(p.with_hints(&super::attr_index::index_hints(&body, ctx.indexed)))
        })
        .collect::<Result<_, PlanError>>()?;
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

/// The search statement (`sql-schema.md` §5.2, ungrouped, its top-K over
/// the whole window; [`search_sql_at`] reads a slice): the newest `limit`
/// traces with a span `f` keeps, their matched spans capped at `spss`,
/// each carrying `proj`'s values, and their roots. `limit` and `spss` are
/// the request's, already validated positive by the caller. Issue it with
/// `final = 1`, as every read of these tables is; clustered, pass the
/// distributed table names — spans are sharded by trace, so each trace's
/// rows are on one shard.
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
    search_sql_at(spans_table, traces_table, w, None, f, proj, limit, spss)
}

/// [`search_sql`], a [`SearchFilter::One`]'s top-K reading `slice`, the
/// newest part of `w`, when one is given (issue #595).
#[allow(clippy::too_many_arguments)]
pub fn search_sql_at(
    spans_table: &str,
    traces_table: &str,
    w: WindowSql,
    slice: Option<WindowSql>,
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
    let (limit_text, spss_text, projection) = (limit.to_string(), spss.to_string(), proj.sql());
    // Issue #594 part 1: one per-trace scalar for every filter's leaves.
    let predicates = f.predicates();
    let trace_leaves: Vec<&TraceLeaf> = predicates.iter().flat_map(|p| p.trace_leaves()).collect();
    let child_counts = predicates.iter().any(|p| p.reads_child_counts());
    // Issue #594 part 2: the numbering, scoped per read.
    let numbered = predicates.iter().any(|p| p.reads_numbering());
    let top_w = slice.unwrap_or(w);
    let mut scalars = per_trace_sql(&trace_leaves, child_counts, w, traces_table);
    // Issue #594 part 3: the per-trace values read as operands, over the
    // traces reaching the part of the window the top-K reads.
    let mut values: Vec<TraceValue> = Vec::new();
    for v in predicates.iter().flat_map(|p| p.trace_values()) {
        if !values.contains(v) {
            values.push(*v);
        }
    }
    scalars.push_str(&trace_values_sql(&values, top_w, w, traces_table));
    if numbered {
        scalars.push_str(&keys_sql(top_w, w, traces_table));
    }
    let (scope_top, scope_detail, after_top) = if numbered {
        (SCOPE_TOP, SCOPE_DETAIL, after_top_sql(spans_table))
    } else if proj.reads_numbers() {
        ("", "", returned_numbers_sql(spans_table, traces_table, w))
    } else {
        ("", "", String::new())
    };
    let common: [(&str, &str); 13] = [
        ("scalars", &scalars),
        ("scope_top", scope_top),
        ("scope_detail", scope_detail),
        ("after_top", &after_top),
        ("spans", spans_table),
        ("time", &time),
        ("bucket", &bucket),
        ("day", &day),
        ("limit", &limit_text),
        ("spss", &spss_text),
        ("projection", &projection),
        ("trace_read", &trace_read),
        ("traces", traces_table),
    ];
    match f {
        SearchFilter::One(p) => {
            let top = slice.unwrap_or(w);
            let (top_time, top_bucket, top_day) = (
                top.span_time_clause(),
                top.span_bucket_clause(),
                top.span_day_clause(),
            );
            let (window_keys, keys) = match slice {
                None => (String::new(), KEYS_WHOLE),
                Some(_) => (
                    fill(
                        WINDOW_KEYS,
                        &[
                            ("within", &w.bucket_within("k")),
                            ("traces", traces_table),
                            ("days", &w.per_trace_day_clause()),
                        ],
                    ),
                    KEYS_SLICED,
                ),
            };
            let mut values: Vec<(&str, &str)> = common.to_vec();
            values.push(("predicate", p.sql()));
            values.push(("top_time", &top_time));
            values.push(("top_bucket", &top_bucket));
            values.push(("top_day", &top_day));
            values.push(("window_keys", &window_keys));
            values.push(("keys", keys));
            fill(SEARCH, &values)
        }
        SearchFilter::Tree { .. } | SearchFilter::Structural { .. } => {
            unreachable!("a spanset tree or structural spanset is compiled as the fold's selector")
        }
        SearchFilter::Pipeline { selector, fold } => {
            let tree = matches!(**selector, SearchFilter::Tree { .. });
            let structural = matches!(**selector, SearchFilter::Structural { .. });
            let candidate = tree || structural || fold.filters_traces();
            let top = slice.unwrap_or(w);
            let (top_time, top_bucket, top_day) = (
                top.span_time_clause(),
                top.span_bucket_clause(),
                top.span_day_clause(),
            );
            let (sel, flags, member) = match &**selector {
                SearchFilter::One(p) => (p.sql().to_string(), String::new(), None),
                SearchFilter::Tree {
                    filters, guards, ..
                } => {
                    let n = filters.len();
                    let flags: Vec<String> = (1..=n)
                        .map(|i| format!("({}) AS f{i}", filters[i - 1].sql()))
                        .collect();
                    let any_sql: Vec<String> =
                        filters.iter().map(|p| format!("({})", p.sql())).collect();
                    let counts: Vec<String> = (1..=n)
                        .map(|i| format!("max(f{i}) OVER (PARTITION BY trace_id) AS c{i}"))
                        .collect();
                    let member: Vec<String> = (1..=n)
                        .map(|i| match &guards[i - 1] {
                            None => format!("f{i}"),
                            Some(g) => format!("(f{i} AND {g})"),
                        })
                        .collect();
                    (
                        any_sql.join(" OR "),
                        format!(", {}", flags.join(", ")),
                        Some((counts.join(", "), member.join(" OR "))),
                    )
                }
                SearchFilter::Structural { member, .. } => {
                    (member.sql().to_string(), String::new(), None)
                }
                SearchFilter::Pipeline { .. } => unreachable!("a selector is not a pipeline"),
            };
            // A slice's candidates are the traces with a span the selector
            // keeps there, or, for a structural spanset, its leaves keep
            // (issue #602 part 2b).
            let cand_sel = match &**selector {
                SearchFilter::Structural { leaves, .. } => leaves.clone(),
                _ => sel.clone(),
            };
            // The fold's span rows: the span table itself for one filter,
            // so a clustered statement groups on each shard; a tree's flags
            // and membership as a relation.
            let base = |is_top: bool, keys_cond: &str| -> Src {
                let scope = if is_top { scope_top } else { scope_detail };
                let from = format!(
                    "{spans_table}\nWHERE {keys_cond}{time}\n  AND {bucket}\n  AND {day}\n  AND ({sel})"
                );
                let mut named = fold.columns(is_top);
                if !is_top {
                    named.push(("proj".to_string(), projection.clone()));
                }
                match &member {
                    None => {
                        named.push(("s0".to_string(), "true".to_string()));
                        Src::table(scope, from, named)
                    }
                    Some((counts, member)) => {
                        let mut cols = String::from("trace_id, span_id, start_ns, duration_ns");
                        if !is_top {
                            cols.push_str(", service");
                        }
                        cols.push_str(&flags);
                        for (n, e) in &named {
                            cols.push_str(&format!(", ({e}) AS {n}"));
                        }
                        Src::rel(&format!(
                            "SELECT *, ({member}) AS s0\nFROM (SELECT *, {counts}\nFROM ({scope}SELECT {cols}\nFROM {from}))"
                        ))
                    }
                }
            };
            let mut candidates = String::new();
            let top_sql = if !candidate {
                fill(
                    TOP_PLAIN,
                    &[
                        ("scope_top", scope_top),
                        ("spans", spans_table),
                        ("top_time", &top_time),
                        ("top_bucket", &top_bucket),
                        ("top_day", &top_day),
                        ("predicate", &sel),
                        ("limit", &limit_text),
                    ],
                )
            } else if slice.is_some() {
                candidates = fill(
                    CANDIDATES,
                    &[
                        ("scope_top", scope_top),
                        ("spans", spans_table),
                        ("top_time", &top_time),
                        ("top_bucket", &top_bucket),
                        ("top_day", &top_day),
                        ("predicate", &cand_sel),
                        ("within", &w.bucket_within("k")),
                        ("traces", traces_table),
                        ("days", &w.per_trace_day_clause()),
                    ],
                );
                fold.render(
                    base(true, &format!("(intDiv(start_ns, 300000000000), trace_id) IN\n{KEYS_CANDIDATES}\n  AND ")),
                    true,
                    limit,
                    spss,
                    Some(top.first_included_ns()),
                )
            } else {
                fold.render(base(true, ""), true, limit, spss, None)
            };
            // A key an aggregate partitions by in the top-K, read through a
            // lookup over the traces the top-K reads.
            let top_set = if candidate && slice.is_some() {
                "(SELECT arrayJoin(cand.1))".to_string()
            } else {
                format!(
                    "(SELECT trace_id FROM {traces_table} WHERE {} AND last_start_ns >= {} AND start_ns <= {})",
                    w.per_trace_day_clause(),
                    top.first_included_ns(),
                    top.last_included_ns()
                )
            };
            for (j, k) in fold.keys.iter().enumerate() {
                if let Some(v) = k.sql.values
                    && fold.top_reads_lookup(j)
                {
                    let text = group_values_sql(v, w, spans_table, traces_table)
                        .replace("(SELECT arrayJoin(top.1))", &top_set)
                        .replace("group_values", &format!("top_group_values{j}"))
                        .replace("group_keys", &format!("top_group_keys{j}"));
                    candidates.push_str(text.trim_start_matches(",\n     "));
                    candidates.push_str(",\n     ");
                }
            }
            let (window_keys, keys) = match (candidate, slice) {
                (false, Some(_)) => (
                    fill(
                        WINDOW_KEYS,
                        &[
                            ("within", &w.bucket_within("k")),
                            ("traces", traces_table),
                            ("days", &w.per_trace_day_clause()),
                        ],
                    ),
                    KEYS_SLICED,
                ),
                _ => (String::new(), KEYS_WHOLE),
            };
            let mut group_values = String::new();
            for (j, k) in fold.keys.iter().enumerate() {
                if let Some(v) = k.sql.values {
                    group_values.push_str(
                        &group_values_sql(v, w, spans_table, traces_table)
                            .replace("group_values", &format!("group_values{j}"))
                            .replace("group_keys", &format!("group_keys{j}")),
                    );
                }
            }
            let detail = fold.render(
                base(
                    false,
                    &format!("(intDiv(start_ns, 300000000000), trace_id) IN\n{keys}\n  AND "),
                ),
                false,
                limit,
                spss,
                None,
            );
            let groups = if fold.contributors().is_empty() {
                ""
            } else {
                ", m.groups AS groups"
            };
            fill(
                SEARCH_PIPELINE,
                &[
                    ("scalars", &scalars),
                    ("candidates", &candidates),
                    ("top", &top_sql),
                    ("window_keys", &window_keys),
                    ("after_top", &after_top),
                    ("group_values", &group_values),
                    ("groups", groups),
                    ("detail", &detail),
                    ("trace_read", &trace_read),
                ],
            )
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
    /// `Some(max_depth)` when the statement climbs (`>>`, `<<`, issue
    /// #593 part 2): it is issued with `max_recursive_cte_evaluation_depth
    /// = max_depth + 1`.
    climb_depth: Option<u32>,
    /// Whether a predicate reads the nested-set numbering (issue #594 part
    /// 2).
    numbered: bool,
    /// Whether the selector holds a structural operator, whose relations
    /// read `spans` inside a read of `spans` (issue #602 part 2b).
    structural: bool,
    /// Whether a predicate reads a span's child count, whose `(trace_id,
    /// span_id) IN` subquery reads `spans` inside a read of `spans`
    /// (issue #600 part 2).
    child_counts: bool,
}

/// The response attributes every surviving spanset carries (issue #602
/// part 2a): the last segment's `by()` keys and aggregates, in written
/// order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grouping {
    pub contributors: Vec<Contributor>,
}

impl SearchStatement {
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The climb bound the statement carries, when it climbs.
    pub fn climb_depth(&self) -> Option<u32> {
        self.climb_depth
    }

    /// Whether a predicate reads the nested-set numbering (issue #594 part
    /// 2): the newest-slice loop keeps doubling for it.
    pub fn reads_numbering(&self) -> bool {
        self.numbered
    }

    /// Whether the statement reads a table inside a read of the same
    /// table: the numbering, a structural operator's relations or a span's
    /// child count.
    pub fn nests_reads(&self) -> bool {
        self.numbered || self.structural || self.child_counts
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

/// A query's `|` stages, as the search statement serves them: the fold,
/// the later filters' bodies the projection reads, and the `select()`
/// fields.
struct Pipeline<'a> {
    fold: Fold,
    bodies: Vec<&'a FieldExpr>,
    selected: Vec<Field>,
}

/// [`Pipeline`] of `query`: every stage at its written position (issue
/// #602 part 2a). Refused, with the previous planner's texts: a spanset
/// operation as a stage, a `by()` over an expression, an event or link
/// intrinsic key, and the aggregates [`agg_of`] refuses.
fn pipeline_of<'a>(query: &'a Query, ctx: &PredicateCtx<'_>) -> Result<Pipeline<'a>, PlanError> {
    let mut out = Pipeline {
        fold: Fold::default(),
        bodies: Vec::new(),
        selected: Vec::new(),
    };
    for stage in &query.pipeline {
        match stage {
            PipelineStage::Filter(SpansetExpr::Filter(f)) => {
                let Some(body) = f.body.as_ref() else {
                    continue;
                };
                let p = compile_span_predicate_in(body, ctx)?;
                out.bodies.push(body);
                out.fold.stages.push(Stage::Filter(out.fold.filters.len()));
                out.fold.filters.push(p);
            }
            PipelineStage::Filter(expr) => {
                return Err(PlanError::TypeMismatch(format!(
                    "{expr} is not executable as a pipeline stage: a `|` stage must be a \
                     single {{ ... }} filter, not a cross-spanset or structural operation"
                )));
            }
            PipelineStage::Select { fields } => out.selected.extend(fields.iter().cloned()),
            PipelineStage::By { key } => {
                let FieldExpr::Field(field) = key else {
                    return Err(PlanError::TypeMismatch(format!(
                        "by({key}) is not a group key this engine can execute: a grouping key \
                         must resolve to a single per-span value, so it must be an attribute or \
                         an intrinsic"
                    )));
                };
                let Some(sql) = group_key_sql(field, ctx)? else {
                    return Err(PlanError::UnsupportedField(format!(
                        "by({field}): grouping by a span-event / span-link intrinsic is not \
                         supported (a span carries a collection of events/links, so there is \
                         no single group value)"
                    )));
                };
                let per_trace = matches!(sql.values, Some(super::tracelevel::KeyValues::Trace(_)));
                out.fold.stages.push(Stage::By(out.fold.keys.len()));
                out.fold.keys.push(Key {
                    display: format!("by({field})"),
                    sql,
                    per_trace,
                });
            }
            PipelineStage::Coalesce => out.fold.stages.push(Stage::Coalesce),
            PipelineStage::Aggregate {
                op,
                field,
                cmp,
                value,
            } => {
                let agg = agg_of(*op, field.as_ref(), *cmp, value, ctx)?;
                out.fold.stages.push(Stage::Agg(out.fold.aggs.len()));
                out.fold.aggs.push(agg);
            }
            PipelineStage::Metric(_)
            | PipelineStage::MetricSecondStage(_)
            | PipelineStage::Compare { .. } => {
                return Err(PlanError::UnsupportedField(
                    "a metrics stage is not a search pipeline stage".to_string(),
                ));
            }
        }
    }
    Ok(out)
}

/// A number as today's writer reads one from an attribute's stored text:
/// Rust's `f64` grammar, finite (`numeric_val_num`).
const NUMBER_TEXT: &str = "[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?";

/// One aggregate stage, checked in the previous planner's order: the
/// operator, the argument, then the threshold.
fn agg_of(
    op: pulsus_traceql::AggregateOp,
    field: Option<&FieldExpr>,
    cmp: pulsus_traceql::ComparisonOp,
    value: &pulsus_traceql::Value,
    ctx: &PredicateCtx<'_>,
) -> Result<Agg, PlanError> {
    use pulsus_traceql::{AggregateOp as A, ComparisonOp as C};
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
    let arg = match (op, field) {
        (A::Count, None) => AggArg::Count,
        (A::Count, Some(_)) => {
            return Err(PlanError::TypeMismatch(
                "count() takes no field".to_string(),
            ));
        }
        (_, None) => return Err(PlanError::TypeMismatch(format!("{op}() requires a field"))),
        (_, Some(FieldExpr::Field(Field::Intrinsic(Intrinsic::Duration)))) => AggArg::Duration,
        (_, Some(FieldExpr::Field(Field::Intrinsic(other)))) => {
            return Err(PlanError::TypeMismatch(format!(
                "{other} is not numerically aggregatable"
            )));
        }
        (_, Some(FieldExpr::Field(f @ Field::Attribute { .. }))) => {
            let Some((text_sql, kind)) = super::projection::aggregate_argument_sql(f, ctx)? else {
                return Err(PlanError::UnsupportedField(format!(
                    "{op}({f}) is not supported by the search statement"
                )));
            };
            let plain = format!("replaceRegexpOne({text_sql}, '^[+]', '')");
            let number = crate::traces::filter::anchored_regex_sql(NUMBER_TEXT)?;
            AggArg::Attr {
                v: format!(
                    "if(match({text_sql}, {number}) AND isFinite(toFloat64OrZero({plain})), \
                     toFloat64OrZero({plain}), NULL)"
                ),
                kind,
            }
        }
        (_, Some(expr)) => {
            return Err(PlanError::TypeMismatch(format!(
                "{op}({expr}) is not an executable aggregation source: only a bare \
                 duration or attribute can be aggregated"
            )));
        }
    };
    let threshold = aggregate_threshold(op, &field.cloned(), value)?;
    Ok(Agg {
        display: match field {
            Some(FieldExpr::Field(f)) => format!("{op}({f})"),
            _ => format!("{op}()"),
        },
        op,
        arg,
        sql_op,
        threshold: format!("toFloat64('{threshold:?}')"),
    })
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
        "DurationI" => GroupValue::Str(crate::traces::values::go_duration_string(
            text.parse()
                .map_err(|e| format!("duration {text:?}: {e}"))?,
        )),
        "DurationF" => GroupValue::Str(crate::traces::values::go_duration_string(f()? as i64)),
        other => return Err(format!("an aggregate value of type {other} is not decoded")),
    })
}

/// Compiles `query` to the search statement. Its `|` stages are
/// [`pipeline_of`]'s; a structural operator is #593's; a set field projected from a
/// comparison that holds it twice beside arithmetic is refused
/// (`projection.rs`); everything the
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
    compile_search_at_depth(
        query,
        ctx,
        spans_table,
        traces_table,
        limit,
        spss,
        super::structural::DEFAULT_MAX_DEPTH,
    )
}

/// [`compile_search`], with `>>` and `<<` following at most `max_depth`
/// parent links (`PULSUS_TRACEQL_MAX_DEPTH`, issue #593 part 2).
pub fn compile_search_at_depth(
    query: &Query,
    ctx: &PredicateCtx<'_>,
    spans_table: &str,
    traces_table: &str,
    limit: u32,
    spss: u32,
    max_depth: u32,
) -> Result<SearchStatement, PlanError> {
    compile_search_sliced(
        query,
        ctx,
        spans_table,
        traces_table,
        limit,
        spss,
        max_depth,
        None,
    )
}

/// [`compile_search_at_depth`], the top-K reading `slice` (issue #595).
#[allow(clippy::too_many_arguments)]
pub fn compile_search_sliced(
    query: &Query,
    ctx: &PredicateCtx<'_>,
    spans_table: &str,
    traces_table: &str,
    limit: u32,
    spss: u32,
    max_depth: u32,
    slice: Option<WindowSql>,
) -> Result<SearchStatement, PlanError> {
    let pipeline = pipeline_of(query, ctx)?;
    let structural = super::structural::holds_structural(&query.spanset);
    // Issue #593: a spanset holding a structural operator is one
    // membership predicate. Issue #602 part 2b: sliced, its relations read
    // the slice's candidates' keys alone.
    let mut climb_depth = None;
    let selector = if structural {
        let m = super::structural::compile_membership_in(
            &query.spanset,
            ctx,
            spans_table,
            max_depth,
            slice.map(|_| KEYS_CANDIDATES),
        )?;
        if m.climbs {
            climb_depth = Some(max_depth);
        }
        SearchFilter::Structural {
            leaves: m.leaves,
            member: m.predicate,
        }
    } else {
        compile_search_filter(&query.spanset, ctx)?
    };
    let Pipeline {
        fold,
        bodies: later,
        selected,
    } = pipeline;
    let contributors = fold.contributors();
    // Issue #602 part 2a: a spanset tree is always the fold's selector, so
    // it reads the newest slice first as every other statement does; part
    // 2b: so is a structural spanset.
    let filter = if fold.is_empty()
        && !matches!(
            selector,
            SearchFilter::Tree { .. } | SearchFilter::Structural { .. }
        ) {
        selector
    } else {
        SearchFilter::Pipeline {
            selector: Box::new(selector),
            fold,
        }
    };
    let grouping = (!contributors.is_empty()).then_some(Grouping { contributors });
    let mut filters = Vec::new();
    filters_of(&query.spanset, &mut filters)?;
    let mut bodies: Vec<&FieldExpr> = filters.iter().filter_map(|f| f.body.as_ref()).collect();
    bodies.extend(later.iter().copied());
    let projection = Projection::of_filters(&bodies, &selected, ctx)?;
    let mut demands: Vec<String> = Vec::new();
    for p in filter.predicates() {
        for m in p.demand_messages() {
            if !demands.contains(m) {
                demands.push(m.clone());
            }
        }
    }
    let numbered: usize = filter
        .predicates()
        .iter()
        .map(|p| p.numbered_leaves())
        .sum();
    let sql = search_sql_at(
        spans_table,
        traces_table,
        ctx.window,
        slice,
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
        climb_depth,
        numbered: numbered > 0,
        structural,
        child_counts: filter.predicates().iter().any(|p| p.reads_child_counts()),
    })
}

/// The caller-validated request window and response caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchParams {
    pub start_ns: i64,
    pub end_ns: i64,
    /// Result cap (`limit` param, docs/api.md §4.2).
    pub limit: u32,
    /// Spans-per-spanset cap (`spss` param).
    pub spss: u32,
}

/// A search the route answers (issue #602 part 2b): its window, `limit`,
/// `spss` and query, accepted by [`compile_search`].
#[derive(Debug, Clone, PartialEq)]
pub struct SearchRequest {
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
    pub(crate) limit: u32,
    pub(crate) spss: u32,
    pub(crate) query: Query,
}

/// The route's planner (issue #602 part 2b): `query` over `params` as a
/// [`SearchRequest`], refused with [`compile_search`]'s own error. Its
/// `hints` are accepted and ignored.
pub fn plan_search_request(
    query: &Query,
    params: &SearchParams,
    tables: &super::metrics::StatementTables<'_>,
    max_depth: u32,
) -> Result<SearchRequest, PlanError> {
    let request = SearchRequest {
        start_ns: params.start_ns,
        end_ns: params.end_ns,
        limit: params.limit,
        spss: params.spss,
        query: Query {
            spanset: query.spanset.clone(),
            pipeline: query.pipeline.clone(),
            hints: Vec::new(),
        },
    };
    plan_statement_sliced(
        &request,
        tables.spans,
        tables.traces,
        tables.resources,
        max_depth,
        tables.indexed,
        None,
    )?;
    Ok(request)
}

/// The search statement for `request`, its top-K reading the newest
/// `slice_ns` of the window when given (issue #595).
pub fn plan_statement_sliced(
    request: &SearchRequest,
    spans_table: &str,
    traces_table: &str,
    resources_table: &str,
    max_depth: u32,
    indexed: &[super::attr_index::IndexedAttr],
    slice_ns: Option<i64>,
) -> Result<SearchStatement, PlanError> {
    let window = WindowSql::start_closed_end_open(request.start_ns, request.end_ns);
    let slice = slice_ns.map(|len| {
        WindowSql::start_closed_end_open(
            request.end_ns.saturating_sub(len).max(request.start_ns),
            request.end_ns,
        )
    });
    let ctx = PredicateCtx {
        window,
        spans_table,
        resources_table,
        indexed,
    };
    compile_search_sliced(
        &request.query,
        &ctx,
        spans_table,
        traces_table,
        request.limit,
        request.spss,
        max_depth,
        slice,
    )
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
        "Duration" => GroupValue::Str(crate::traces::values::go_duration_string(
            text.parse::<i64>()
                .map_err(|e| format!("a by() duration {text:?}: {e}"))?,
        )),
        t if t.starts_with("Array(") => GroupValue::Str(rendered_array(text)?),
        other => return Err(format!("a by() key stored as {other} is not decoded")),
    })
}

/// The payload a [`group_value`] holds, learned before it is built.
fn group_value_payload(text: &str, value_type: &str) -> Result<usize, String> {
    Ok(match value_type {
        "" | "String" => text.len(),
        "Duration" => match group_value(text, value_type)? {
            GroupValue::Str(s) => s.len(),
            _ => 0,
        },
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
    let stages = &grouping.contributors;
    let check = |g: &SearchGroupTuple| -> Result<(), ReadError> {
        if g.attrs.len() == stages.len() {
            Ok(())
        } else {
            Err(decode(format!(
                "a group carries {} attributes where the pipeline writes {}",
                g.attrs.len(),
                stages.len()
            )))
        }
    };
    let by_count = stages.iter().filter(|c| !c.aggregate).count();
    if by_count > 0 {
        for row in &rows {
            for g in &row.groups {
                check(g)?;
                let mut key = Vec::with_capacity(by_count);
                for (c, (text, ty)) in stages.iter().zip(&g.attrs) {
                    if !c.aggregate {
                        key.push(group_value(text, ty).map_err(decode)?);
                    }
                }
                counter.observe(&key, budget)?;
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
    let capacity = proj.attribute_capacity();
    for (trace, groups) in out.traces.iter_mut().zip(row_groups) {
        let kept: Vec<SearchGroupTuple> = groups.into_iter().filter(|g| g.matched > 0).collect();
        budget.charge(groups_reserve_bytes(kept.len()))?;
        let mut built = Vec::with_capacity(kept.len());
        for g in kept {
            check(&g)?;
            let mut attributes = Vec::with_capacity(stages.len());
            for (c, (text, ty)) in stages.iter().zip(&g.attrs) {
                let (value, payload) = if c.aggregate {
                    let v = aggregate_value(text, ty).map_err(decode)?;
                    let p = v.payload_bytes();
                    (v, p)
                } else {
                    let p = group_value_payload(text, ty).map_err(decode)?;
                    (group_value(text, ty).map_err(decode)?, p)
                };
                budget.charge(
                    std::mem::size_of::<(String, GroupValue)>() + c.display.len() + payload,
                )?;
                attributes.push((c.display.clone(), value));
            }
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

/// The numeric threshold one aggregate stage compares against: the
/// stage's literal as an `f64`, which [`agg_of`] renders into the
/// statement as `toFloat64('…')`. A duration literal is a threshold only
/// for a duration aggregate other than `count()`. Moved here from the
/// previous search engine's planner when #602 part 2c-2b-2 deleted it.
fn aggregate_threshold(
    op: pulsus_traceql::AggregateOp,
    field: &Option<FieldExpr>,
    value: &Value,
) -> Result<f64, PlanError> {
    match value {
        Value::Number(raw) => raw
            .parse::<f64>()
            .ok()
            .filter(|n| n.is_finite())
            .ok_or_else(|| PlanError::TypeMismatch(format!("not a finite number: {raw:?}"))),
        // A duration threshold is meaningful only against a duration
        // aggregate (nanosecond scale).
        Value::Duration(d)
            if matches!(
                field,
                Some(FieldExpr::Field(Field::Intrinsic(Intrinsic::Duration)))
            ) && op != pulsus_traceql::AggregateOp::Count =>
        {
            Ok(d.as_nanos() as f64)
        }
        _ => Err(PlanError::TypeMismatch(
            "aggregate comparisons require a numeric (or duration, for duration aggregates) \
             threshold"
                .to_string(),
        )),
    }
}

#[cfg(test)]
mod charge_tests {
    use super::*;
    use crate::traces::exec::{ByteBudget, RETAINED_ENTRY_OVERHEAD, output_reserve_bytes};
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
            spans_table: "spans",
            resources_table: "resources",
            indexed: &[],
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

    /// The expected charge for the response: the trace buffer, then per
    /// trace an entry's overhead, the summaries buffer and its payload, and
    /// the root strings. (The previous search engine also charged each
    /// trace's own match struct here; #602 part 2c-2b-2 dropped it with that
    /// engine, the trace's slot being charged with the output buffer.)
    fn todays_charge(out: &SearchOutput) -> usize {
        output_reserve_bytes(out.traces.len())
            + out
                .traces
                .iter()
                .map(|t| {
                    RETAINED_ENTRY_OVERHEAD
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

    /// Section 6.1: the decode charges exactly the expected charge
    /// ([`todays_charge`]) for the response it builds, the previous search
    /// engine's extra struct charge gone with that engine; a budget of
    /// exactly that decodes, and one byte less is refused.
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
                    attrs: vec![("x".to_string(), "String".to_string())],
                    matched: 2,
                    spans: vec![span(1, bar()), span(2, bar())],
                },
                SearchGroupTuple {
                    attrs: vec![("5".to_string(), "Int64".to_string())],
                    matched: 0,
                    spans: Vec::new(),
                },
                SearchGroupTuple {
                    attrs: vec![(String::new(), "None".to_string())],
                    matched: 1,
                    spans: vec![span(3, bar())],
                },
            ],
        }]
    }

    /// Issue #592 part 2, section 7.1: the grouped decode charges the
    /// expected charge for the same response, its groups included
    /// ([`todays_charge`] with `groups_retained_bytes`, the previous search
    /// engine's extra struct charge gone with that engine); it
    /// fits a budget of that plus the distinct group tuples the cap's
    /// counter holds while it runs, and one byte less is refused.
    #[test]
    fn the_grouped_decode_charges_what_todays_engine_charges() {
        use crate::traces::values::{GroupValue, group_tuple_bytes, groups_retained_bytes};
        let q = pulsus_traceql::parse(r#"{ span.foo = "bar" } | by(span.k)"#).expect("parses");
        let w = WindowSql::start_closed_end_open(1_000_000_000_000, 2_000_000_000_000);
        let ctx = PredicateCtx {
            window: w,
            spans_table: "spans",
            resources_table: "resources",
            indexed: &[],
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

    /// Issue #592 part 3: an aggregate's one group charges the expected
    /// amount — [`todays_charge`] with `groups_retained_bytes`, the previous
    /// search engine's extra struct charge gone with that engine — and no
    /// distinct-group tuple: one byte less than that is refused.
    #[test]
    fn the_aggregated_decode_charges_what_todays_engine_charges() {
        use crate::traces::values::groups_retained_bytes;
        let q =
            pulsus_traceql::parse(r#"{ span.foo = "bar" } | avg(duration) > 1ms"#).expect("parses");
        let w = WindowSql::start_closed_end_open(1_000_000_000_000, 2_000_000_000_000);
        let ctx = PredicateCtx {
            window: w,
            spans_table: "spans",
            resources_table: "resources",
            indexed: &[],
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
                    attrs: vec![("2166666666".to_string(), "DurationI".to_string())],
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

    /// Issue #600 part 2, D11: a child-count read nests a read of `spans`
    /// inside one, as the numbering and a structural operator do, so a
    /// clustered statement holding one is issued with `'local'`.
    #[test]
    fn a_child_count_statement_nests_reads() {
        let w = WindowSql::start_closed_end_open(1_000_000_000_000, 2_000_000_000_000);
        let ctx = PredicateCtx {
            window: w,
            spans_table: "spans",
            resources_table: "resources",
            indexed: &[],
        };
        let nests = |query: &str| {
            let q = pulsus_traceql::parse(query).expect("parses");
            compile_search(&q, &ctx, "spans", "traces", 20, 3)
                .expect("compiles")
                .nests_reads()
        };
        let got: Vec<(&str, bool)> = [
            "{ span:childCount > 2 }",
            r#"{ span:childCount > 0 || name = "x" }"#,
            r#"{ trace:rootService = "x" || span:childCount > 0 }"#,
            "{ nestedSetLeft > 0 }",
            "{ } > { }",
            r#"{ name = "x" }"#,
        ]
        .into_iter()
        .map(|q| (q, nests(q)))
        .collect();
        assert_eq!(
            got,
            vec![
                ("{ span:childCount > 2 }", true),
                (r#"{ span:childCount > 0 || name = "x" }"#, true),
                (
                    r#"{ trace:rootService = "x" || span:childCount > 0 }"#,
                    true
                ),
                ("{ nestedSetLeft > 0 }", true),
                ("{ } > { }", true),
                (r#"{ name = "x" }"#, false),
            ]
        );
    }
}
