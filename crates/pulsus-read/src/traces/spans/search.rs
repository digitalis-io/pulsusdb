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

use super::predicate::SpanPredicate;
use crate::traces::window_sql::WindowSql;

/// The statement, byte for byte (the issue #590 design's section 3.2). A
/// `const` at column zero, filled by `str::replace`, as [`super::fetch`]
/// does, so no source indentation can leak into it.
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
                        groupArray((span_id, start_ns, duration_ns, service))), 1, {spss}) AS spans
      FROM {spans}
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN
            (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
        AND {time}
        AND {bucket}
        AND {day}
        AND ({predicate})
      GROUP BY trace_id) AS m
LEFT JOIN (SELECT trace_id, min(start_ns) AS start_ns, max(end_ns) AS end_ns,
                  max(root_service) AS root_service, max(root_name) AS root_name
           FROM {traces}
           WHERE trace_id IN (SELECT arrayJoin(top.1))
           GROUP BY trace_id) AS t USING trace_id
ORDER BY last DESC, trace_id ASC";

/// The search statement (`sql-schema.md` §5.2, ungrouped). `limit` and
/// `spss` are the request's, already validated positive by the caller.
/// Issue it with `final = 1`, as every read of these tables is; clustered,
/// pass the distributed table names — spans are sharded by trace, so each
/// trace's rows are on one shard.
///
/// Every output column carries an alias, read by name into
/// [`super::rows::SearchTraceRow`].
///
/// # Panics
///
/// If `p` was compiled for another window, as
/// [`super::predicate::span_membership_sql`] does: a resource subquery
/// bounded by one window's days inside a statement bounded by another is a
/// wrong answer.
pub fn search_sql(
    spans_table: &str,
    traces_table: &str,
    w: WindowSql,
    p: &SpanPredicate,
    limit: u32,
    spss: u32,
) -> String {
    assert!(
        p.composes_with(w),
        "search_sql: the predicate was compiled for a different window"
    );
    // The predicate is substituted LAST: it carries the client's string
    // literals, and a literal spelled `{limit}` must not be rewritten by a
    // later substitution.
    SEARCH
        .replace("{spans}", spans_table)
        .replace("{traces}", traces_table)
        .replace("{time}", &w.span_time_clause())
        .replace("{bucket}", &w.span_bucket_clause())
        .replace("{day}", &w.span_day_clause())
        .replace("{limit}", &limit.to_string())
        .replace("{spss}", &spss.to_string())
        .replace("{predicate}", p.sql())
}
