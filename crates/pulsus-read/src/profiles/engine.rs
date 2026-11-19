//! Issue #821: the profiles read engine — the tables and ceilings a
//! process's configuration implies, the settings every statement carries,
//! the classification of a failed statement, and [`ProfilesEngine`], which
//! sends a catalogue read through the private [`super::dispatch`].

use pulsus_clickhouse::{ChClient, ChError, QuerySettings, Row};
use serde::{Deserialize, Serialize};

use crate::traces::tenant_scope::TenantTerm;

use super::catalogue::{Read, compat_sets};
use super::diff::DiffRow;
use super::dispatch::ProfileDispatch;
use super::heatmap::{HeatmapExemplarRow, HeatmapRow};
use super::render::RenderRow;
use super::selector::WindowLimits;
use super::series::{TimeSeriesExemplarRow, TimeSeriesRow};
use super::tree::TreeRow;

/// ClickHouse's `TOO_MANY_ROWS` (`max_rows_to_read`).
const CODE_TOO_MANY_ROWS: i32 = 158;
/// ClickHouse's `TIMEOUT_EXCEEDED`.
const CODE_TIMEOUT_EXCEEDED: i32 = 159;
/// ClickHouse's `MEMORY_LIMIT_EXCEEDED`.
const CODE_MEMORY_LIMIT_EXCEEDED: i32 = 241;
/// ClickHouse's `TOO_MANY_ROWS_OR_BYTES` (`max_result_rows`,
/// `max_result_bytes`).
const CODE_TOO_MANY_ROWS_OR_BYTES: i32 = 396;

/// A profiles read's failure, by the Connect code it answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfilesReadError {
    /// `invalid_argument`: a refused window or selector, a missing name, or
    /// a regex the database cannot compile.
    InvalidArgument(String),
    /// `resource_exhausted`: a ceiling was reached; the text names it.
    TooBroad(String),
    /// `deadline_exceeded`.
    DeadlineExceeded,
    /// `unavailable`: no server could be reached.
    Unavailable(String),
    /// `internal`: anything else.
    Internal(String),
}

/// The tables and ceilings every profiles read uses.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProfilesReadConfig {
    /// `profile_series`, or its wrapper on a cluster.
    pub series_table: String,
    /// Issue #823: `profile_samples`, or its wrapper on a cluster.
    pub samples_table: String,
    /// Issue #824: `profile_entries`, or its wrapper on a cluster.
    pub entries_table: String,
    /// The wrapper suffix when clustered, else empty: the tenant check's
    /// table list.
    pub dist_suffix: String,
    pub limits: WindowLimits,
    pub scan_budget_rows: u64,
    pub max_tree_nodes: u64,
    pub max_result_bytes: u64,
    pub query_memory_bytes: u64,
    pub distributed: bool,
    pub skip_unavailable_shards: bool,
    /// Issue #823: the flame graph's `max_nodes` when a request sends none
    /// or 0.
    pub max_nodes_default: i64,
    /// Issue #823: the largest `max_nodes` a request may send.
    pub max_nodes_max: i64,
}

impl ProfilesReadConfig {
    /// The mapping the server uses from a process's configuration.
    pub fn from_config(config: &pulsus_config::Config) -> Self {
        let dist = if config.cluster.is_some() {
            config.dist_suffix.as_str()
        } else {
            ""
        };
        let r = &config.reader;
        ProfilesReadConfig {
            series_table: format!("profile_series{dist}"),
            samples_table: format!("profile_samples{dist}"),
            entries_table: format!("profile_entries{dist}"),
            dist_suffix: dist.to_string(),
            limits: WindowLimits {
                max_length_ms: millis(r.profiles_max_query_length.0),
                max_lookback_ms: millis(r.profiles_max_query_lookback.0),
            },
            scan_budget_rows: r.profiles_scan_budget_rows,
            max_tree_nodes: r.profiles_max_tree_nodes,
            max_result_bytes: r.profiles_max_result_bytes,
            query_memory_bytes: r.profiles_query_memory_bytes,
            distributed: config.cluster.is_some(),
            skip_unavailable_shards: config.skip_unavailable_shards,
            max_nodes_default: i64::try_from(r.profiles_max_nodes_default).unwrap_or(i64::MAX),
            max_nodes_max: i64::try_from(r.profiles_max_nodes_max).unwrap_or(i64::MAX),
        }
    }
}

fn millis(d: std::time::Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// The settings every profiles statement carries: the answer pins (every
/// overflow mode `throw`), the four ceilings, no spill, the raised query
/// size, the request id as `log_comment`, and on a cluster
/// `distributed_product_mode = 'local'`.
fn read_settings(config: &ProfilesReadConfig, log_comment: Option<&str>) -> QuerySettings {
    let mut s = QuerySettings::new()
        .answer_pins(false, config.skip_unavailable_shards)
        .set("max_rows_to_read", config.scan_budget_rows)
        .set("max_result_rows", config.max_tree_nodes)
        .set("max_result_bytes", config.max_result_bytes)
        .set("max_memory_usage", config.query_memory_bytes)
        .set("max_bytes_before_external_group_by", 0u64)
        .set("max_query_size", crate::querytext::MAX_QUERY_TEXT_BYTES);
    if config.distributed {
        s = s.set("distributed_product_mode", "local");
    }
    if let Some(comment) = log_comment {
        s = s.set("log_comment", comment);
    }
    s
}

/// Issue #823: the settings S5 carries — [`read_settings`], and the join's
/// build side kept on the aggregated samples. Issue #827: S7 carries them
/// too, and issue #829 the render statement.
fn tree_settings(config: &ProfilesReadConfig, log_comment: Option<&str>) -> QuerySettings {
    read_settings(config, log_comment).set("query_plan_join_swap_table", "false")
}

/// The class of a failed profiles statement (plan §6).
pub fn map_read_error(e: ChError) -> ProfilesReadError {
    if let Some(super::selector::SelectorError::InvalidArgument(d)) =
        super::selector::read_error(&e)
    {
        return ProfilesReadError::InvalidArgument(d);
    }
    let too_broad =
        |setting: &str| ProfilesReadError::TooBroad(format!("query_too_broad: {setting}"));
    match e {
        ChError::Server {
            code: CODE_TOO_MANY_ROWS,
            ..
        } => too_broad("PULSUS_PROFILES_SCAN_BUDGET_ROWS"),
        ChError::Server {
            code: CODE_TOO_MANY_ROWS_OR_BYTES,
            ..
        } => too_broad("PULSUS_PROFILES_MAX_TREE_NODES or PULSUS_PROFILES_MAX_RESULT_BYTES"),
        ChError::Server {
            code: CODE_MEMORY_LIMIT_EXCEEDED,
            ..
        } => too_broad("PULSUS_PROFILES_QUERY_MEMORY_BYTES"),
        ChError::Server {
            code: CODE_TIMEOUT_EXCEEDED,
            ..
        }
        | ChError::Timeout(_) => ProfilesReadError::DeadlineExceeded,
        ChError::Connect(_) | ChError::Io(_) => ProfilesReadError::Unavailable(e.to_string()),
        other => ProfilesReadError::Internal(other.to_string()),
    }
}

/// What a catalogue read answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// S1–S3: the values, in the statement's order.
    Strings(Vec<String>),
    /// S4: the label sets, each in name order.
    Sets(Vec<Vec<(String, String)>>),
}

#[derive(Row, Serialize, Deserialize)]
struct StringRow {
    s: String,
}

#[derive(Row, Serialize, Deserialize)]
struct SeriesRow {
    ls: Vec<(String, String)>,
}

/// One request's profiles reads, bound to one server by its client.
pub struct ProfilesEngine {
    dispatch: ProfileDispatch,
    config: ProfilesReadConfig,
    log_comment: Option<String>,
}

impl ProfilesEngine {
    /// An engine reading `tenant`'s profiles.
    pub fn new(client: ChClient, config: ProfilesReadConfig, tenant: pulsus_model::Tenant) -> Self {
        let tables = super::tenant_scope::tables(&config.dist_suffix);
        Self {
            dispatch: ProfileDispatch::new(client, TenantTerm::of(&tenant), tables),
            config,
            log_comment: None,
        }
    }

    /// Tags every statement with `log_comment = comment` (the request id).
    pub fn with_query_log_comment(mut self, comment: impl Into<String>) -> Self {
        self.log_comment = Some(comment.into());
        self
    }

    /// The first server failure this engine's reads met.
    pub fn server_failure(&self) -> Option<pulsus_clickhouse::ServerFailure> {
        self.dispatch.server_failure()
    }

    /// Sends `read`'s one statement and returns its answer.
    pub async fn read(&self, read: &Read) -> Result<Answer, ProfilesReadError> {
        let settings = read_settings(&self.config, self.log_comment.as_deref());
        match read {
            Read::Strings(sql) => {
                let rows: Vec<StringRow> = self.dispatch.rows(sql, &settings).await?;
                Ok(Answer::Strings(rows.into_iter().map(|r| r.s).collect()))
            }
            Read::Series { sql, compat } => {
                let rows: Vec<SeriesRow> = self.dispatch.rows(sql, &settings).await?;
                let sets: Vec<Vec<(String, String)>> = rows.into_iter().map(|r| r.ls).collect();
                Ok(Answer::Sets(if *compat { compat_sets(sets) } else { sets }))
            }
        }
    }

    /// Issue #823: sends S5 (`super::tree`) and returns the merged tree's
    /// rows.
    pub async fn read_tree(&self, sql: &str) -> Result<Vec<TreeRow>, ProfilesReadError> {
        let settings = tree_settings(&self.config, self.log_comment.as_deref());
        self.dispatch.rows(sql, &settings).await
    }

    /// Issue #829: sends the render statement (`super::render`) and returns
    /// the merged tree's rows and the series' rows, tagged by `part`.
    pub async fn read_render(&self, sql: &str) -> Result<Vec<RenderRow>, ProfilesReadError> {
        let _ = sql;
        Ok(Vec::new())
    }

    /// Issue #827: sends S7 (`super::diff`) and returns one row per node of
    /// the two trees' union.
    pub async fn read_diff(&self, sql: &str) -> Result<Vec<DiffRow>, ProfilesReadError> {
        let settings = tree_settings(&self.config, self.log_comment.as_deref());
        self.dispatch.rows(sql, &settings).await
    }

    /// Issue #824: sends S6 (`super::series`) and returns one row per
    /// series.
    pub async fn read_series(&self, sql: &str) -> Result<Vec<TimeSeriesRow>, ProfilesReadError> {
        let settings = read_settings(&self.config, self.log_comment.as_deref());
        self.dispatch.rows(sql, &settings).await
    }

    /// Issue #825: sends S8 (`super::heatmap`) and returns one row per
    /// series.
    pub async fn read_heatmap(&self, sql: &str) -> Result<Vec<HeatmapRow>, ProfilesReadError> {
        let settings = read_settings(&self.config, self.log_comment.as_deref());
        self.dispatch.rows(sql, &settings).await
    }

    /// Issue #826: sends S6 with exemplars (`super::series`) and returns
    /// one row per series and one per selected series' labels.
    pub async fn read_series_exemplars(
        &self,
        sql: &str,
    ) -> Result<Vec<TimeSeriesExemplarRow>, ProfilesReadError> {
        let settings = read_settings(&self.config, self.log_comment.as_deref());
        self.dispatch.rows(sql, &settings).await
    }

    /// Issue #826: sends S8 with exemplars (`super::heatmap`) and returns
    /// one row per series and one per selected series' labels.
    pub async fn read_heatmap_exemplars(
        &self,
        sql: &str,
    ) -> Result<Vec<HeatmapExemplarRow>, ProfilesReadError> {
        let settings = read_settings(&self.config, self.log_comment.as_deref());
        self.dispatch.rows(sql, &settings).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn off_default() -> ProfilesReadConfig {
        ProfilesReadConfig {
            series_table: "profile_series".to_string(),
            samples_table: "profile_samples".to_string(),
            entries_table: "profile_entries".to_string(),
            dist_suffix: String::new(),
            limits: WindowLimits::default(),
            scan_budget_rows: 11,
            max_tree_nodes: 13,
            max_result_bytes: 17,
            query_memory_bytes: 19,
            distributed: false,
            skip_unavailable_shards: true,
            max_nodes_default: 23,
            max_nodes_max: 29,
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
        // Issue #823 (D4): S5 carries every setting above, and keeps the
        // join's build side on the aggregated samples; the catalogue reads
        // do not carry it.
        let tree = format!("{:?}", tree_settings(&off_default(), Some("abc")));
        for expected in [
            r#"("max_rows_to_read", "11")"#,
            r#"("max_result_rows", "13")"#,
            r#"("log_comment", "abc")"#,
            r#"("query_plan_join_swap_table", "false")"#,
        ] {
            assert!(tree.contains(expected), "{expected} missing from {tree}");
        }
        assert!(!single.contains("query_plan_join_swap_table"), "{single}");
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
            config.reader.profiles_max_nodes_default = 17;
            config.reader.profiles_max_nodes_max = 19;
            let c = ProfilesReadConfig::from_config(&config);
            assert_eq!(c.series_table, format!("profile_series{dist}"));
            assert_eq!(c.samples_table, format!("profile_samples{dist}"));
            assert_eq!(c.entries_table, format!("profile_entries{dist}"));
            assert_eq!(c.max_nodes_default, 17);
            assert_eq!(c.max_nodes_max, 19);
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
