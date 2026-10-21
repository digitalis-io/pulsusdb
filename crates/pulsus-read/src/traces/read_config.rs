//! The trace read configuration a process's [`pulsus_config::Config`]
//! implies (issue #600): one mapping, which the server and the catalogue
//! command both use.

use super::exec::TraceReadConfig;

impl TraceReadConfig {
    /// The tables and budgets every trace read uses: the `_dist` twins when
    /// clustered, except `resources`, which is `Replication::Global` and read
    /// from the local replica.
    pub fn from_config(config: &pulsus_config::Config) -> Self {
        let dist = if config.cluster.is_some() {
            config.dist_suffix.as_str()
        } else {
            ""
        };
        TraceReadConfig {
            spans_table: format!("trace_spans{dist}"),
            attrs_table: format!("trace_attrs_idx{dist}"),
            // `trace_edges` (M7-E1, issue #173) co-shards with `trace_spans` on
            // `cityHash64(trace_id)`, so it is `_dist`-aware exactly like
            // `spans_table`/`attrs_table` (the dist-suffix rule, not the catalog
            // carve-out) — the query-time edge join stays shard-local.
            edges_table: format!("trace_edges{dist}"),
            // Issue #560: the two derived trace tables co-shard with
            // `trace_spans` on `cityHash64(trace_id)` — the same rule.
            recent_table: format!("trace_recent{dist}"),
            errors_table: format!("trace_error_spans{dist}"),
            // Issue #587: the fetch's three tables. `spans` and `traces`
            // co-shard with the landing table on `cityHash64(trace_id)`, so
            // they take the same `_dist` rule as every other routed trace
            // table. `resources` does NOT: it is `Replication::Global`, one
            // cluster-wide replica set read from the local replica, the same
            // carve-out the tag catalog has — a `_dist` wrapper would fan a
            // read out to every shard for rows every shard already holds.
            spans_v2_table: format!("spans{dist}"),
            error_spans_table: format!("error_spans{dist}"),
            traces_table: format!("traces{dist}"),
            resources_table: "resources".to_string(),
            scan_budget_rows: config.reader.traceql_scan_budget_rows,
            max_depth: config.reader.traceql_max_depth,
            max_series: config.reader.traceql_max_series,
            // Issue #398: the TraceQL per-query ClickHouse memory ceiling's
            // production carrier — `ReaderConfig -> TraceReadConfig ->
            // traces::exec::{search_settings, catalog_settings}` (and, through
            // `catalog_settings`, the §4.2 point read).
            read_max_memory_bytes: config.reader.traceql_read_max_memory_bytes,
            distributed: config.cluster.is_some(),
            skip_unavailable_shards: config.skip_unavailable_shards,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every field, unclustered, clustered and clustered under a suffix of
    /// its own, from a `Config` whose every read setting is off its default:
    /// the server and the catalogue command read through this one mapping,
    /// so a field it drops, misnames or hard-codes reaches both.
    #[test]
    fn from_config_maps_every_field() {
        for (cluster, suffix, dist) in [
            (None, "_dist", ""),
            (Some("c".to_string()), "_dist", "_dist"),
            (Some("c".to_string()), "_shards", "_shards"),
        ] {
            let mut config = pulsus_config::Config {
                cluster: cluster.clone(),
                dist_suffix: suffix.to_string(),
                skip_unavailable_shards: true,
                ..pulsus_config::Config::default()
            };
            config.reader.traceql_scan_budget_rows = 7;
            config.reader.traceql_max_depth = 9;
            config.reader.traceql_max_series = 11;
            config.reader.traceql_read_max_memory_bytes = 13;
            let c = TraceReadConfig::from_config(&config);
            assert_eq!(c.spans_table, format!("trace_spans{dist}"));
            assert_eq!(c.attrs_table, format!("trace_attrs_idx{dist}"));
            assert_eq!(c.edges_table, format!("trace_edges{dist}"));
            assert_eq!(c.recent_table, format!("trace_recent{dist}"));
            assert_eq!(c.errors_table, format!("trace_error_spans{dist}"));
            assert_eq!(c.spans_v2_table, format!("spans{dist}"));
            assert_eq!(c.error_spans_table, format!("error_spans{dist}"));
            assert_eq!(c.traces_table, format!("traces{dist}"));
            assert_eq!(c.resources_table, "resources");
            assert_eq!(c.scan_budget_rows, 7);
            assert_eq!(c.max_depth, 9);
            assert_eq!(c.max_series, 11);
            assert_eq!(c.read_max_memory_bytes, 13);
            assert_eq!(c.distributed, cluster.is_some());
            assert!(c.skip_unavailable_shards);
        }
    }
}
