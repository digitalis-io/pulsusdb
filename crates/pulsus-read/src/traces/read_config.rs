//! The trace read configuration a process's [`pulsus_config::Config`]
//! implies (issue #600): one mapping, which the server and the catalogue
//! command both use.

use super::exec::TraceReadConfig;

impl TraceReadConfig {
    /// The tables and budgets every trace read uses.
    pub fn from_config(_config: &pulsus_config::Config) -> Self {
        TraceReadConfig {
            spans_table: String::new(),
            attrs_table: String::new(),
            edges_table: String::new(),
            recent_table: String::new(),
            errors_table: String::new(),
            spans_v2_table: String::new(),
            traces_table: String::new(),
            resources_table: String::new(),
            scan_budget_rows: 0,
            max_depth: 0,
            max_series: 0,
            read_max_memory_bytes: 0,
            distributed: false,
            skip_unavailable_shards: false,
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
