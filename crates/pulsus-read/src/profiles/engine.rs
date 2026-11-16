//! Stub.

use pulsus_clickhouse::{ChError, QuerySettings};

use super::selector::WindowLimits;

/// A profiles read's failure, by the Connect code it answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfilesReadError {
    InvalidArgument(String),
    TooBroad(String),
    DeadlineExceeded,
    Unavailable(String),
    Internal(String),
}

/// The tables and ceilings every profiles read uses.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProfilesReadConfig {
    pub series_table: String,
    pub dist_suffix: String,
    pub limits: WindowLimits,
    pub scan_budget_rows: u64,
    pub max_tree_nodes: u64,
    pub max_result_bytes: u64,
    pub query_memory_bytes: u64,
    pub distributed: bool,
    pub skip_unavailable_shards: bool,
}

impl ProfilesReadConfig {
    pub fn from_config(_config: &pulsus_config::Config) -> Self {
        Self::default()
    }
}

fn read_settings(_config: &ProfilesReadConfig, _log_comment: Option<&str>) -> QuerySettings {
    QuerySettings::new()
}

pub fn map_read_error(e: ChError) -> ProfilesReadError {
    ProfilesReadError::Internal(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn off_default() -> ProfilesReadConfig {
        ProfilesReadConfig {
            series_table: "profile_series".to_string(),
            dist_suffix: String::new(),
            limits: WindowLimits::default(),
            scan_budget_rows: 11,
            max_tree_nodes: 13,
            max_result_bytes: 17,
            query_memory_bytes: 19,
            distributed: false,
            skip_unavailable_shards: true,
        }
    }

    /// Q5: every ceiling reaches the statement's settings, and the
    /// clustered-only setting appears only when clustered.
    #[test]
    fn q5_the_settings_carry_every_ceiling() {
        let single = format!("{:?}", read_settings(&off_default(), Some("abc")));
        for expected in [
            r#"("max_rows_to_read", "11")"#,
            r#"("max_result_rows", "13")"#,
            r#"("max_result_bytes", "17")"#,
            r#"("max_memory_usage", "19")"#,
            r#"("max_bytes_before_external_group_by", "0")"#,
            r#"("max_query_size", "8388608")"#,
            r#"("read_overflow_mode", "throw")"#,
            r#"("result_overflow_mode", "throw")"#,
            r#"("skip_unavailable_shards", "1")"#,
            r#"("log_comment", "abc")"#,
        ] {
            assert!(
                single.contains(expected),
                "{expected} missing from {single}"
            );
        }
        assert!(!single.contains("distributed_product_mode"), "{single}");
        let no_comment = format!("{:?}", read_settings(&off_default(), None));
        assert!(!no_comment.contains("log_comment"), "{no_comment}");
        let clustered = ProfilesReadConfig {
            distributed: true,
            ..off_default()
        };
        let clustered = format!("{:?}", read_settings(&clustered, None));
        assert!(
            clustered.contains(r#"("distributed_product_mode", "local")"#),
            "{clustered}"
        );
    }

    /// Q6: each server code and transport failure maps to its class.
    #[test]
    fn q6_each_error_maps_to_its_class() {
        let server = |code: i32| ChError::Server {
            code,
            message: format!("Code: {code}. DB::Exception: x"),
        };
        for (code, setting) in [
            (158, "PULSUS_PROFILES_SCAN_BUDGET_ROWS"),
            (
                396,
                "PULSUS_PROFILES_MAX_TREE_NODES or PULSUS_PROFILES_MAX_RESULT_BYTES",
            ),
            (241, "PULSUS_PROFILES_QUERY_MEMORY_BYTES"),
        ] {
            assert_eq!(
                map_read_error(server(code)),
                ProfilesReadError::TooBroad(format!("query_too_broad: {setting}")),
                "{code}"
            );
        }
        let regex = ChError::Server {
            code: 427,
            message: "Code: 427. DB::Exception: OptimizedRegularExpression: cannot compile re2: \
                      (?-s)^(?:()$, error: missing ): (?-s)^(?:()$. Look at the syntax page."
                .to_string(),
        };
        match map_read_error(regex) {
            ProfilesReadError::InvalidArgument(d) => {
                assert!(d.starts_with("^(?:()$, error: missing )"), "{d}")
            }
            other => panic!("427: {other:?}"),
        }
        assert_eq!(
            map_read_error(server(159)),
            ProfilesReadError::DeadlineExceeded
        );
        assert_eq!(
            map_read_error(ChError::Timeout("t".to_string())),
            ProfilesReadError::DeadlineExceeded
        );
        assert!(matches!(
            map_read_error(ChError::Connect("refused".to_string())),
            ProfilesReadError::Unavailable(_)
        ));
        assert!(matches!(
            map_read_error(ChError::Io("reset".to_string())),
            ProfilesReadError::Unavailable(_)
        ));
        assert!(matches!(
            map_read_error(server(60)),
            ProfilesReadError::Internal(_)
        ));
    }

    /// Q7: every field, unclustered, clustered and clustered under a suffix
    /// of its own, from a `Config` whose every setting is off its default.
    #[test]
    fn q7_from_config_maps_every_field() {
        use std::time::Duration;
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
            config.reader.profiles_max_query_length =
                pulsus_config::HumanDuration(Duration::from_secs(2 * 3_600));
            config.reader.profiles_max_query_lookback =
                pulsus_config::HumanDuration(Duration::from_secs(3 * 86_400));
            config.reader.profiles_scan_budget_rows = 7;
            config.reader.profiles_max_tree_nodes = 9;
            config.reader.profiles_max_result_bytes = 11;
            config.reader.profiles_query_memory_bytes = 13;
            let c = ProfilesReadConfig::from_config(&config);
            assert_eq!(c.series_table, format!("profile_series{dist}"));
            assert_eq!(c.dist_suffix, dist);
            assert_eq!(
                c.limits,
                WindowLimits {
                    max_length_ms: 2 * 3_600_000,
                    max_lookback_ms: 3 * 86_400_000,
                }
            );
            assert_eq!(c.scan_budget_rows, 7);
            assert_eq!(c.max_tree_nodes, 9);
            assert_eq!(c.max_result_bytes, 11);
            assert_eq!(c.query_memory_bytes, 13);
            assert_eq!(c.distributed, cluster.is_some());
            assert!(c.skip_unavailable_shards);
        }
    }
}
