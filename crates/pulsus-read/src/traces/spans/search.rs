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

use pulsus_traceql::{BoolOp, FieldExpr, Query, SpansetExpr, SpansetFilter, Value};

use super::predicate::{PredicateCtx, SpanPredicate, compile_span_predicate_in};
use super::projection::{Projection, ceiling_sql};
use super::rows::SearchTraceRow;
use crate::traces::PlanError;
use crate::traces::exec::{RootSummary, SearchOutput, TraceSearchResult};
use crate::traces::search_eval::SpanSummary;
use crate::traces::window_sql::WindowSql;

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
    /// Two or more filters joined by `&&`/`||`, in pre-order, with the
    /// texts of section 4.3's `{holds}` and each filter's guard (`None` =
    /// true).
    Tree {
        filters: Vec<SpanPredicate>,
        holds: String,
        guards: Vec<Option<String>>,
    },
}

impl SearchFilter {
    fn predicates(&self) -> Vec<&SpanPredicate> {
        match self {
            SearchFilter::One(p) => vec![p],
            SearchFilter::Tree { filters, .. } => filters.iter().collect(),
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

/// The filters of `spanset` in pre-order; a structural operator anywhere
/// is #593's.
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
        SpansetExpr::Structural { .. } => Err(PlanError::UnsupportedField(
            "a structural operator is not supported by the search statement yet (issue #593)"
                .to_string(),
        )),
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
        SpansetExpr::Structural { .. } => unreachable!("refused by filters_of first"),
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
    }
}

/// A compiled search: the statement, the projection its rows decode by,
/// and the messages its `!` demands raise.
pub struct SearchStatement {
    sql: String,
    projection: Projection,
    demands: Vec<String>,
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
}

/// Compiles `query` to the search statement. A pipeline stage is #592's, a
/// structural operator #593's, and a projection off the span row "#591
/// part 2"'s; everything the predicate compiler refuses is returned as it
/// refused. `query.hints` are accepted and ignored, as today.
pub fn compile_search(
    query: &Query,
    ctx: &PredicateCtx<'_>,
    spans_table: &str,
    traces_table: &str,
    limit: u32,
    spss: u32,
) -> Result<SearchStatement, PlanError> {
    if !query.pipeline.is_empty() {
        return Err(PlanError::UnsupportedField(
            "a pipeline stage is not supported by the search statement yet (issue #592)"
                .to_string(),
        ));
    }
    let filter = compile_search_filter(&query.spanset, ctx)?;
    let mut filters = Vec::new();
    filters_of(&query.spanset, &mut filters)?;
    let bodies: Vec<&FieldExpr> = filters.iter().filter_map(|f| f.body.as_ref()).collect();
    let projection = Projection::of_filters(&bodies, ctx)?;
    let mut demands: Vec<String> = Vec::new();
    for p in filter.predicates() {
        for m in p.demand_messages() {
            if !demands.contains(m) {
                demands.push(m.clone());
            }
        }
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
    })
}

/// A row the decode could not read: a projected value that does not parse
/// as its kind, or a count out of range. Never a default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchDecodeError(pub String);

/// The statement's rows as today's [`SearchOutput`] (section 5.4), in the
/// order they arrive, which is the answer's. The root span's own start
/// and duration are `0`: `traces` stores no root span window, and neither
/// reaches the wire. The statement has no candidate cap, so the output is
/// never partial.
pub fn decode_search(
    rows: Vec<SearchTraceRow>,
    proj: &Projection,
    limit: u32,
) -> Result<SearchOutput, SearchDecodeError> {
    let mut traces = Vec::with_capacity(rows.len());
    for row in rows {
        let mut spans = Vec::with_capacity(row.spans.len());
        for s in row.spans {
            let (name, attributes) = proj.decode(&s.projected).map_err(SearchDecodeError)?;
            spans.push(SpanSummary::new(
                s.span_id,
                name,
                s.start_ns,
                s.duration_ns,
                attributes,
            ));
        }
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
                .map_err(|_| SearchDecodeError(format!("matched {} exceeds u32", row.matched)))?,
            spans,
            groups: None,
        });
    }
    let returned = u32::try_from(traces.len())
        .map_err(|_| SearchDecodeError("more traces than u32 counts".to_string()))?;
    Ok(SearchOutput {
        traces,
        partial: false,
        returned,
        limit,
    })
}
