//! The service-graph statement over the per-minute edge totals and the span
//! table (issue #599).
//!
//! A scheduled job in the database (`service_edges_mv`, `schema/schema.sql`)
//! pairs the spans of each finished 5-minute interval and appends one row
//! per `(minute, client_minute, client, server, conn_type)` to
//! `service_edges`, ending every run with a **marker row** (`conn_type =
//! ''`) at the last minute of its interval. This statement, per shard and on
//! one replica:
//!
//! * takes its coverage from the markers: from the start of the oldest
//!   marker's interval (`A`) to the end of the **complete prefix** of
//!   markers (`B`), the markers in order from the oldest up to the first
//!   that is not five minutes after the one before it;
//! * sums the totals whose `minute` and `client_minute` are both whole
//!   minutes inside the window and inside `[A, B)`;
//! * pairs every other call of the window from the spans of the two raw
//!   regions, `[start, A + 60 s)` and `[B − 60 s, end)`, or the whole window
//!   when nothing is covered, keeping only calls not wholly inside `[A, B)`.
//!
//! A call counts only when both halves start in `[start, end)`, the rule
//! the old ledger read kept (docs/api.md §4.5).
//!
//! Pure SQL builder: no `ChClient`, no I/O. The window is integers and the
//! target is the database's own catalogue entry, read by
//! [`crate::traces::exec::graph_target_from_engine`]; neither is client
//! text.

use crate::traces::exec::GraphTarget;
use crate::traces::tenant_scope::{MARKER_TERM, TenantTerm};

/// Response cap on distinct `(client, server, conn_type)` edges the
/// service-graph read returns (docs/api.md §4.5; promoted to config only on
/// evidence). The SQL carries `LIMIT SERVICE_GRAPH_MAX_EDGES + 1`: the extra
/// row is the truncation probe (the search path's `cap + 1` convention) —
/// the engine returns at most `SERVICE_GRAPH_MAX_EDGES` edges plus a
/// non-silent `truncated` flag.
pub const SERVICE_GRAPH_MAX_EDGES: u64 = 1_000;

/// The snapped, left-closed/right-open service-graph window `[start_ns,
/// end_ns)` — an edge is reported iff BOTH its halves' own timestamps fall
/// in this window (the normative boundary rule, docs/api.md §4.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphWindow {
    pub start_ns: i64,
    pub end_ns: i64,
}

const NS_PER_MINUTE: i64 = 60_000_000_000;
const NS_PER_BUCKET: i64 = 300_000_000_000;

/// `x` rounded up to a whole minute.
fn ceil60(x: i64) -> i64 {
    -((-x).div_euclid(NS_PER_MINUTE)) * NS_PER_MINUTE
}

/// `x` rounded down to a whole minute.
fn floor60(x: i64) -> i64 {
    x.div_euclid(NS_PER_MINUTE) * NS_PER_MINUTE
}

/// The 5-minute bucket index `x` falls in, the span table's leading key.
fn bucket(x: i64) -> i64 {
    x.div_euclid(NS_PER_BUCKET)
}

/// The per-shard statement `I`: markers, totals and the raw regions, on
/// `edges` and `spans` as written.
fn per_shard(
    tenant: &TenantTerm,
    w: GraphWindow,
    markers: &str,
    edges: &str,
    spans: &str,
) -> String {
    let (s, e) = (w.start_ns, w.end_ns);
    format!(
        "{markers}\n\
        \x20    ms[1] AS first,\n\
        \x20    ms[arrayFirstIndex(i -> i = length(ms) OR ms[i + 1] - ms[i] != 300, arrayEnumerate(ms))] AS last,\n\
        \x20    greatest({ceil_s}, toInt64(toUnixTimestamp(first)) * 1000000000 - 240000000000) AS A,\n\
        \x20    least({floor_e}, toInt64(toUnixTimestamp(last)) * 1000000000 + 60000000000) AS B,\n\
        \x20    if(A < B, if(A > {s}, A + 60000000000, {s}), {e}) AS h1,\n\
        \x20    if(A < B, if(B < {e}, B - 60000000000, {e}), {e}) AS t0\n\
        SELECT client, server, conn_type, calls, failed, q\n\
        FROM {edges} FINAL\n\
        WHERE {tenant} AND conn_type IN ('rpc', 'messaging')\n\
        \x20 AND minute >= toDateTime(intDiv(A, 1000000000), 'UTC') AND minute < toDateTime(intDiv(B, 1000000000), 'UTC')\n\
        \x20 AND client_minute >= toDateTime(intDiv(A, 1000000000), 'UTC') AND client_minute < toDateTime(intDiv(B, 1000000000), 'UTC')\n\
        UNION ALL\n\
        SELECT c.1 AS client, s.2 AS server, if(c.3 = 3, 'rpc', 'messaging') AS conn_type,\n\
        \x20      count() AS calls, countIf(s.3 = 2 OR c.2 = 2) AS failed,\n\
        \x20      quantilesTDigestState(0.5, 0.95, 0.99)(s.4) AS q\n\
        FROM (SELECT trace_id,\n\
        \x20            if(kind IN (3, 4) OR attrs.`zipkin%2Eshared`.:String = 'true', span_id, parent_span_id) AS k,\n\
        \x20            anyIf((service, status_code, kind, start_ns), kind IN (3, 4)) AS c,\n\
        \x20            countIf(kind IN (3, 4)) AS nc,\n\
        \x20            groupArrayIf((span_id, service, status_code, duration_ns, kind, start_ns), kind IN (2, 5)) AS ss\n\
        \x20     FROM {spans}\n\
        \x20     WHERE {tenant} AND ((start_ns >= {s} AND start_ns < h1\n\
        \x20             AND intDiv(start_ns, 300000000000) >= {bucket_s} AND intDiv(start_ns, 300000000000) <= intDiv(h1 - 1, 300000000000))\n\
        \x20         OR (start_ns >= t0 AND start_ns < {e}\n\
        \x20             AND intDiv(start_ns, 300000000000) >= intDiv(t0, 300000000000) AND intDiv(start_ns, 300000000000) <= {bucket_e}))\n\
        \x20       AND kind IN (2, 3, 4, 5)\n\
        \x20     GROUP BY trace_id, k\n\
        \x20     HAVING nc > 0)\n\
        ARRAY JOIN arrayDistinct(ss) AS s\n\
        WHERE ((c.3 = 3 AND s.5 = 2) OR (c.3 = 4 AND s.5 = 5))\n\
        \x20 AND NOT (s.6 >= A AND s.6 < B AND c.4 >= A AND c.4 < B)\n\
        GROUP BY client, server, conn_type",
        ceil_s = ceil60(s),
        floor_e = floor60(e),
        bucket_s = bucket(s),
        bucket_e = bucket(e - 1),
    )
}

/// `I`'s marker list on a single node: the current database's own.
fn local_markers(edges: &str) -> String {
    format!(
        "WITH (SELECT arraySort(groupArray(minute)) FROM {edges} FINAL WHERE {MARKER_TERM}) AS ms,"
    )
}

/// `I`'s marker list on a cluster (issue #599 part A2): every shard's list,
/// keyed by shard number and host, read by a fan-out of the same cluster,
/// and each shard's own entry picked with `shardNum()` and `hostName()`.
///
/// ClickHouse evaluates a scalar subquery inside `cluster(…, view(…))`
/// once, on the initiator, and sends its value to every shard, so a scalar
/// over the local table would hand every shard the initiator's markers. A
/// shard or replica with no entry gets an empty list: it covers nothing and
/// pairs its whole window from spans.
fn cluster_markers(cluster: &str, edges: &str) -> String {
    format!(
        "WITH (SELECT groupArray((s, h, m)) FROM (SELECT s, h, arraySort(groupArray(minute)) AS m\n\
        \x20     FROM cluster('{cluster}', view(SELECT shardNum() AS s, hostName() AS h, minute\n\
        \x20                              FROM {edges} FINAL WHERE {MARKER_TERM}))\n\
        \x20     GROUP BY s, h)) AS mall,\n\
        \x20    arrayFirst(x -> x.1 = shardNum() AND x.2 = hostName(), mall).3 AS ms,"
    )
}

/// The service-graph statement (issue #599): `I` per shard, summed and
/// merged by the initiator, ordered and capped at `max_edges + 1` rows (the
/// truncation probe).
///
/// `target` is `None` on a single node, where `I` reads the current
/// database's bare `service_edges` and `spans`. On a cluster it is the
/// cluster and database the reader's `spans` routing table names, and `I`
/// runs on one replica of every shard through `cluster('<c>', view(…))`
/// with both tables qualified by that database, so each shard computes its
/// own coverage from its own markers.
pub fn spans_graph_sql(
    tenant: &TenantTerm,
    w: GraphWindow,
    target: Option<&GraphTarget>,
    max_edges: u64,
) -> String {
    let src = match target {
        None => format!(
            "(\n{}\n)",
            per_shard(
                tenant,
                w,
                &local_markers("service_edges"),
                "service_edges",
                "spans"
            )
        ),
        Some(t) => {
            let cluster = escape_quoted(&t.cluster);
            let edges = format!("{}.service_edges", t.database);
            format!(
                "cluster('{cluster}', view(\n{}\n))",
                per_shard(
                    tenant,
                    w,
                    &cluster_markers(&cluster, &edges),
                    &edges,
                    &format!("{}.spans", t.database),
                )
            )
        }
    };
    format!(
        "SELECT client, server, conn_type, sum(calls) AS calls, sum(failed) AS failed,\n\
        \x20      CAST(quantilesTDigestMerge(0.5, 0.95, 0.99)(q) AS Array(Float64)) AS quantiles_ns\n\
        FROM {src}\n\
        GROUP BY client, server, conn_type\n\
        ORDER BY calls DESC, client ASC, server ASC, conn_type ASC\n\
        LIMIT {limit}",
        limit = max_edges + 1,
    )
}

/// The statement that reads the routing table's definition, the target's
/// one source (issue #599): `table` is the configured `spans` routing
/// table name, never client text.
pub fn graph_target_sql(table: &str) -> String {
    format!(
        "SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = '{}'",
        escape_quoted(table)
    )
}

/// The failure of a target read whose routing table names no target: the
/// route's error for a failed statement (issue #599). Nothing is kept, so
/// the next request reads the definition again.
pub(crate) fn no_graph_target(
    table: &str,
    engine_full: Option<&str>,
) -> pulsus_clickhouse::ChError {
    pulsus_clickhouse::ChError::Decode(match engine_full {
        None => format!("the service graph's target: no table {table} in the current database"),
        Some(def) => format!(
            "the service graph's target: {table} is not Distributed('<cluster>', '<database>', …): {def}"
        ),
    })
}

/// A single-quoted literal's body: backslash and quote escaped.
fn escape_quoted(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_minute_and_bucket_rounding_goes_the_named_way() {
        assert_eq!(ceil60(60_000_000_001), 120_000_000_000);
        assert_eq!(ceil60(60_000_000_000), 60_000_000_000);
        assert_eq!(floor60(119_999_999_999), 60_000_000_000);
        assert_eq!(bucket(299_999_999_999), 0);
        assert_eq!(bucket(300_000_000_000), 1);
    }
}
