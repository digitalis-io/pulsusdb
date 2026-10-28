//! The view values each admin page and its JSON endpoint are built from
//! (design D14): a handler builds one, the page renders it and the API
//! returns it as JSON.

use pulsus_config::{Config, Mode};
use serde::Serialize;

use crate::app::AppState;

/// The effective configuration as JSON, secrets `"***"` by type. The one
/// rendering the configuration page and its endpoint share.
pub(crate) fn config_json(cfg: &Config) -> Result<serde_json::Value, String> {
    serde_json::to_value(cfg).map_err(|e| e.to_string())
}

/// `GET /admin/` and `/admin/api/v1/status`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StatusView {
    version: String,
    revision: String,
    built_at: String,
    rustc: String,
    mode: Mode,
    uptime_seconds: u64,
    ready: bool,
    not_ready_reason: Option<String>,
}

impl StatusView {
    /// Each JSON key with its page label, in page order.
    pub(crate) const FIELDS: &[(&str, &str)] = &[
        ("version", "Version"),
        ("revision", "Revision"),
        ("builtAt", "Built at"),
        ("rustc", "Compiler"),
        ("mode", "Mode"),
        ("uptimeSeconds", "Uptime (s)"),
        ("ready", "Ready"),
        ("notReadyReason", "Not ready because"),
    ];

    pub(crate) async fn build(state: &AppState) -> Self {
        let readiness = crate::ops::readiness(state).await;
        StatusView {
            version: state.build.version.clone(),
            revision: state.build.revision.clone(),
            built_at: state.build.built_at.clone(),
            rustc: state.build.rustc.clone(),
            mode: state.config.mode,
            uptime_seconds: state.started_at.elapsed().map(|d| d.as_secs()).unwrap_or(0),
            ready: readiness.is_ok(),
            not_ready_reason: readiness.err(),
        }
    }
}

/// One rollup level as the database view shows it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LevelView {
    pub(crate) table: String,
    pub(crate) res_ms: i64,
    pub(crate) from_ms: u64,
}

/// `GET /admin/database` and `/admin/api/v1/database`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DatabaseView {
    database: String,
    cluster: Option<String>,
    retention_days: Option<u32>,
    pub(crate) levels: Option<Vec<LevelView>>,
    levels_note: Option<&'static str>,
    shards: Option<u32>,
    counter_buckets: Option<u32>,
    discovery_failures: u64,
}

impl DatabaseView {
    /// Each JSON key with its page label, in page order.
    pub(crate) const FIELDS: &[(&str, &str)] = &[
        ("database", "Database"),
        ("cluster", "Cluster"),
        ("retentionDays", "Retention (days)"),
        ("levels", "Levels"),
        ("levelsNote", "Levels note"),
        ("shards", "Shards"),
        ("counterBuckets", "Counter buckets"),
        ("discoveryFailures", "Discovery failures"),
    ];

    pub(crate) async fn build(state: &AppState) -> Self {
        // A writer never starts level discovery (`serve.rs`), so its slot
        // stays empty for a reason other than "not yet".
        let (levels, levels_note, shards, counter_buckets) =
            if !crate::serve::reader_enabled(&state.config) {
                (None, Some("not read in writer mode"), None, None)
            } else {
                match state.rollup_levels.get() {
                    None => (None, Some("not yet discovered"), None, None),
                    Some(l) => (
                        Some(
                            l.levels
                                .iter()
                                .map(|level| LevelView {
                                    table: level.table.clone(),
                                    res_ms: level.res_ms,
                                    from_ms: level.from_ms,
                                })
                                .collect(),
                        ),
                        None,
                        Some(l.shards),
                        Some(l.counter_buckets),
                    ),
                }
            };
        DatabaseView {
            database: state.config.clickhouse.database.clone(),
            cluster: state.config.cluster.clone(),
            retention_days: state.retention_days(),
            levels,
            levels_note,
            shards,
            counter_buckets,
            discovery_failures: state.rollup_levels.failures(),
        }
    }
}
