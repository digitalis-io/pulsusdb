//! Pure SQL string builder for the service-graph endpoint (issue #173,
//! M7-E1; docs/schemas.md §4.2, docs/api.md §4.5) — the byte-frozen golden
//! surface (`tests/traces_graph_sql.rs`), same convention as
//! [`super::search_sql`]/[`super::metrics_sql`]: no `ChClient`, no I/O, no
//! randomness, no user-controlled fragments (the window is pure integers,
//! the table name is config-derived).
//!
//! The query is a **single fully-pushed-down two-level aggregation** over
//! the `trace_edges` half-row ledger. The inner level dedups each side's
//! `ReplacingMergeTree` replays at read time (an explicit per-side
//! `GROUP BY` — background-merge state is never load-bearing, so the result
//! is byte-identical before and after `OPTIMIZE ... FINAL`); the two deduped
//! sides are joined **within `conn_type`** on the client-side span id
//! (`pair_id`), so only CLIENT->SERVER and PRODUCER->CONSUMER pairs form an
//! edge (issue #173 plan v3). The outer level rolls the joined edge
//! instances up per `(client, server, conn_type)`. Both the daily-partition
//! `date` prune (Tier-1 MinMax) and the leading-`side` PrimaryKey prune keep
//! each half-scan index-served.
//!
//! Clustered mode reuses the ratified §4.2/§4.4 semi-join pattern: the SQL
//! text names the `_dist` table on both sides (the caller resolves
//! `edges_table`), and the engine injects `distributed_product_mode='local'`
//! (`super::exec::graph_settings`) so the join executes per shard — halves
//! co-shard on `cityHash64(trace_id)`, so each shard's local join is
//! complete and the initiator merges only per-`(client, server, conn_type)`
//! partial states.

use super::search_sql::date_literal;

/// Response cap on distinct `(client, server, conn_type)` edges the
/// service-graph read returns (docs/api.md §4.5; promoted to config only on
/// evidence). The SQL carries `LIMIT SERVICE_GRAPH_MAX_EDGES + 1`: the extra
/// row is the truncation probe (the search path's `cap + 1` convention) —
/// the engine returns at most `SERVICE_GRAPH_MAX_EDGES` edges plus a
/// non-silent `truncated` flag.
pub const SERVICE_GRAPH_MAX_EDGES: u64 = 1_000;

const NS_PER_DAY: i64 = 86_400_000_000_000;

/// The snapped, left-closed/right-open service-graph window `[start_ns,
/// end_ns)` — an edge is reported iff BOTH its halves' own timestamps fall
/// in this window (the normative boundary rule, docs/api.md §4.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphWindow {
    pub start_ns: i64,
    pub end_ns: i64,
}

/// The `trace_edges` daily-partition pruning clause for a right-open
/// window: the end day comes from the last **included** nanosecond
/// (`end_ns - 1`), so a window ending exactly at midnight never drags in an
/// extra day's partition (the `metrics_sql` convention).
fn date_clause(w: GraphWindow) -> String {
    let start_days = w.start_ns.div_euclid(NS_PER_DAY);
    let end_days = (w.end_ns - 1).div_euclid(NS_PER_DAY);
    format!(
        "date >= {} AND date <= {}",
        date_literal(start_days),
        date_literal(end_days)
    )
}

/// The shared per-half `WHERE` body: the leading-`side` PK prune, the
/// daily-partition prune, and the left-closed/right-open time bound (each
/// half's own plain `timestamp_ns` — window membership is merge-invariant).
fn half_where(side: u8, w: GraphWindow) -> String {
    format!(
        "WHERE side = {side} AND {}\n      AND timestamp_ns >= {} AND timestamp_ns < {}",
        date_clause(w),
        w.start_ns,
        w.end_ns
    )
}

/// The service-graph query (byte-frozen golden; both aggregation levels
/// pushed down). The inner per-side `GROUP BY`s dedup the ledger's
/// at-least-once/`ReplacingMergeTree` replays (`any`/`max` over the group);
/// the within-`conn_type` equi-join on `pair_id` (the client-side span id)
/// pairs server halves to their client twin, preserving client fan-out (one
/// client parenting N servers yields N server-keyed rows, hence N edges);
/// the outer level counts edge instances and aggregates latency/failure per
/// `(client, server, conn_type)`. `quantilesTDigest` is `CAST` to
/// `Array(Float64)` so the wire type is pinned independent of the server's
/// internal default (issue #173 Fix 2) and decodes into `Vec<f64>`.
pub fn service_graph_sql(w: GraphWindow, edges_table: &str, max_edges: u64) -> String {
    format!(
        "SELECT\n\
        \x20   c.service AS client,\n\
        \x20   s.service AS server,\n\
        \x20   s.conn_type AS conn_type,\n\
        \x20   count() AS calls,\n\
        \x20   countIf(greatest(s.failed, c.failed) = 1) AS failed,\n\
        \x20   CAST(quantilesTDigest(0.5, 0.95, 0.99)(s.duration_ns) AS Array(Float64)) AS quantiles_ns\n\
        FROM\n\
        (\n\
        \x20   SELECT trace_id, span_id, any(pair_id) AS pair_id, any(conn_type) AS conn_type,\n\
        \x20          any(service) AS service, max(duration_ns) AS duration_ns, max(failed) AS failed\n\
        \x20   FROM {edges_table}\n\
        \x20   {server_where}\n\
        \x20   GROUP BY trace_id, span_id\n\
        ) AS s\n\
        INNER JOIN\n\
        (\n\
        \x20   SELECT trace_id, pair_id, any(conn_type) AS conn_type,\n\
        \x20          any(service) AS service, max(failed) AS failed\n\
        \x20   FROM {edges_table}\n\
        \x20   {client_where}\n\
        \x20   GROUP BY trace_id, pair_id\n\
        ) AS c\n\
        ON c.trace_id = s.trace_id AND c.pair_id = s.pair_id AND c.conn_type = s.conn_type\n\
        GROUP BY client, server, conn_type\n\
        ORDER BY calls DESC, client ASC, server ASC\n\
        LIMIT {limit}",
        server_where = half_where(1, w),
        client_where = half_where(0, w),
        limit = max_edges + 1,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const W: GraphWindow = GraphWindow {
        start_ns: 1_699_999_980_000_000_000,
        end_ns: 1_700_010_840_000_000_000,
    };

    #[test]
    fn date_clause_end_day_comes_from_the_last_included_nanosecond() {
        let w = GraphWindow {
            start_ns: 1_699_920_000_000_000_000, // 2023-11-14 00:00:00
            end_ns: 1_700_006_400_000_000_000,   // 2023-11-15 00:00:00 (excluded)
        };
        assert_eq!(
            date_clause(w),
            "date >= toDate('2023-11-14') AND date <= toDate('2023-11-14')"
        );
    }

    #[test]
    fn service_graph_sql_pins_the_two_level_pushdown_and_the_within_type_join() {
        let sql = service_graph_sql(W, "trace_edges", SERVICE_GRAPH_MAX_EDGES);
        // Both aggregation levels present; counting is never a bare count on
        // the raw ledger — always over the deduped/joined edge instances.
        assert!(sql.contains("count() AS calls"));
        assert!(sql.contains("countIf(greatest(s.failed, c.failed) = 1) AS failed"));
        // Fix 2: the quantile wire type is pinned to Array(Float64), no f32.
        assert!(sql.contains(
            "CAST(quantilesTDigest(0.5, 0.95, 0.99)(s.duration_ns) AS Array(Float64)) AS quantiles_ns"
        ));
        // Server half deduped by (trace_id, span_id); client half by
        // (trace_id, pair_id); within-conn_type join on the client-side id.
        assert!(sql.contains("WHERE side = 1 AND"));
        assert!(sql.contains("WHERE side = 0 AND"));
        assert!(sql.contains("GROUP BY trace_id, span_id"));
        assert!(sql.contains("GROUP BY trace_id, pair_id"));
        assert!(sql.contains(
            "ON c.trace_id = s.trace_id AND c.pair_id = s.pair_id AND c.conn_type = s.conn_type"
        ));
        // Daily-partition prune + left-closed/right-open bound on each half.
        assert!(sql.contains("date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')"));
        assert!(sql.contains(
            "timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000"
        ));
        assert!(sql.ends_with("LIMIT 1001"));
        assert!(
            !sql.contains("Float32"),
            "no f32 anywhere on the decode path"
        );
    }

    #[test]
    fn service_graph_sql_targets_the_dist_table_when_given_it() {
        let sql = service_graph_sql(W, "trace_edges_dist", SERVICE_GRAPH_MAX_EDGES);
        assert_eq!(sql.matches("FROM trace_edges_dist").count(), 2);
        assert!(!sql.contains("FROM trace_edges\n"));
    }
}
