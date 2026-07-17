//! Environment-variable overlay: `apply_env` maps every documented
//! variable (docs/configuration.md §§1–8) onto a [`Config`] — YAML-then-env
//! precedence, env wins. A variable set to the empty string counts as
//! unset (docs/configuration.md intro) and is skipped, leaving the
//! YAML/default value in place.

use crate::error::ConfigError;
use crate::model::{ChAuth, Config};
use crate::secret::Secret;
use crate::units::{ByteSize, HumanDuration, parse_bytes, parse_duration};

/// Every environment variable documented in docs/configuration.md §§1–8, in
/// table order. `apply_env` covers this set 1:1 — `tests/env_matrix.rs`
/// asserts set-equality against it, so an undocumented or unwired variable
/// is a red build.
pub const ALL_ENV_VARS: &[&str] = &[
    "PULSUS_MODE",
    "PULSUS_HOST",
    "PULSUS_PORT",
    "PULSUS_LOG_LEVEL",
    "PULSUS_AUTH_USER",
    "PULSUS_AUTH_PASSWORD",
    "PULSUS_COMPAT_ENDPOINTS",
    "PULSUS_CORS_ORIGIN",
    "PULSUS_QUERY_TIMEOUT",
    "CLICKHOUSE_SERVER",
    "CLICKHOUSE_PORT",
    "CLICKHOUSE_HTTP_PORT",
    "CLICKHOUSE_DB",
    "CLICKHOUSE_AUTH",
    "CLICKHOUSE_PROTO",
    "CLICKHOUSE_TLS_SKIP_VERIFY",
    "PULSUS_CH_POOL_SIZE",
    "PULSUS_SKIP_DDL",
    "PULSUS_RETENTION_DAYS",
    "PULSUS_STORAGE_POLICY",
    "PULSUS_ROTATION_INTERVAL",
    "PULSUS_LOG_ROLLUP_RESOLUTION",
    "PULSUS_CLUSTER",
    "PULSUS_DIST_SUFFIX",
    "PULSUS_SKIP_UNAVAILABLE_SHARDS",
    "PULSUS_BATCH_BYTES",
    "PULSUS_BATCH_MS",
    "PULSUS_INSERT_MODE",
    "PULSUS_INGEST_QUEUE_BYTES",
    "PULSUS_CACHE_TTL",
    "PULSUS_CACHE_MAX_SERIES",
    "PULSUS_SERIES_ACTIVITY_BUCKET",
    "PULSUS_CACHE_WINDOW",
    "PULSUS_PROMQL_MAX_SAMPLES",
    "PULSUS_PROMQL_LOOKBACK",
    "PULSUS_PROMQL_EXPERIMENTAL_FUNCTIONS",
    "PULSUS_PROMQL_MAX_METRIC_FANOUT",
    "PULSUS_LOGQL_SCAN_BUDGET_BYTES",
    "PULSUS_LOGQL_PIPELINE_SCAN_FACTOR",
    "PULSUS_TRACEQL_MAX_CANDIDATES",
    "PULSUS_TRACEQL_SCAN_BUDGET_ROWS",
    "PULSUS_TIER_POLICY",
    "PULSUS_RULER_ENABLED",
    "PULSUS_RULER_POLL_INTERVAL",
    "PULSUS_RULER_MAX_RESULT_BYTES",
];

/// Reads `name` from the process environment. A variable set to the empty
/// string is treated as unset.
fn read(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => None,
    }
}

fn env_err(var: &str, msg: impl Into<String>) -> ConfigError {
    ConfigError::Env {
        var: var.to_string(),
        msg: msg.into(),
    }
}

fn parse_bool(var: &str, v: &str) -> Result<bool, ConfigError> {
    match v {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => Err(env_err(
            var,
            format!("invalid boolean {other:?} (expected true, false, 1, or 0)"),
        )),
    }
}

fn parse_int<T>(var: &str, v: &str) -> Result<T, ConfigError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    v.parse()
        .map_err(|e| env_err(var, format!("invalid integer {v:?}: {e}")))
}

/// Parses one of the `Mode`/`LogLevel`/`ChProto`/`InsertMode`/`TierPolicy`
/// enums, whose `FromStr::Err` is just the valid-value list (e.g. `"one
/// of: all, writer, reader, init"`) — this builds the single, complete
/// error sentence.
fn parse_enum<T>(var: &str, v: &str) -> Result<T, ConfigError>
where
    T: std::str::FromStr<Err = String>,
{
    v.parse()
        .map_err(|expected| env_err(var, format!("invalid value {v:?} (expected {expected})")))
}

fn parse_dur(var: &str, v: &str) -> Result<HumanDuration, ConfigError> {
    parse_duration(v)
        .map(HumanDuration)
        .map_err(|e| env_err(var, e.to_string()))
}

fn parse_size(var: &str, v: &str) -> Result<ByteSize, ConfigError> {
    parse_bytes(v)
        .map(ByteSize)
        .map_err(|e| env_err(var, e.to_string()))
}

/// Overlays every documented environment variable onto `cfg` (YAML < env
/// precedence). Variables absent or set to the empty string are skipped.
pub fn apply_env(cfg: &mut Config) -> Result<(), ConfigError> {
    if let Some(v) = read("PULSUS_MODE") {
        cfg.mode = parse_enum("PULSUS_MODE", &v)?;
    }
    if let Some(v) = read("PULSUS_HOST") {
        cfg.host = v;
    }
    if let Some(v) = read("PULSUS_PORT") {
        cfg.port = parse_int("PULSUS_PORT", &v)?;
    }
    if let Some(v) = read("PULSUS_LOG_LEVEL") {
        cfg.log_level = parse_enum("PULSUS_LOG_LEVEL", &v)?;
    }
    if let Some(v) = read("PULSUS_AUTH_USER") {
        cfg.auth_user = Some(v);
    }
    if let Some(v) = read("PULSUS_AUTH_PASSWORD") {
        cfg.auth_password = Some(Secret::new(v));
    }
    if let Some(v) = read("PULSUS_COMPAT_ENDPOINTS") {
        cfg.compat_endpoints = parse_bool("PULSUS_COMPAT_ENDPOINTS", &v)?;
    }
    if let Some(v) = read("PULSUS_CORS_ORIGIN") {
        cfg.cors_origin = v;
    }
    if let Some(v) = read("PULSUS_QUERY_TIMEOUT") {
        cfg.query_timeout = parse_dur("PULSUS_QUERY_TIMEOUT", &v)?;
    }
    if let Some(v) = read("CLICKHOUSE_SERVER") {
        cfg.clickhouse.server = v;
    }
    if let Some(v) = read("CLICKHOUSE_PORT") {
        cfg.clickhouse.port = parse_int("CLICKHOUSE_PORT", &v)?;
    }
    if let Some(v) = read("CLICKHOUSE_HTTP_PORT") {
        cfg.clickhouse.http_port = parse_int("CLICKHOUSE_HTTP_PORT", &v)?;
    }
    if let Some(v) = read("CLICKHOUSE_DB") {
        cfg.clickhouse.database = v;
    }
    if let Some(v) = read("CLICKHOUSE_AUTH") {
        cfg.clickhouse.auth = v
            .parse::<ChAuth>()
            .map_err(|msg| env_err("CLICKHOUSE_AUTH", msg))?;
    }
    if let Some(v) = read("CLICKHOUSE_PROTO") {
        cfg.clickhouse.proto = parse_enum("CLICKHOUSE_PROTO", &v)?;
    }
    if let Some(v) = read("CLICKHOUSE_TLS_SKIP_VERIFY") {
        cfg.clickhouse.tls_skip_verify = parse_bool("CLICKHOUSE_TLS_SKIP_VERIFY", &v)?;
    }
    if let Some(v) = read("PULSUS_CH_POOL_SIZE") {
        cfg.clickhouse.pool_size = parse_int("PULSUS_CH_POOL_SIZE", &v)?;
    }
    if let Some(v) = read("PULSUS_SKIP_DDL") {
        cfg.skip_ddl = parse_bool("PULSUS_SKIP_DDL", &v)?;
    }
    if let Some(v) = read("PULSUS_RETENTION_DAYS") {
        cfg.retention_days = parse_int("PULSUS_RETENTION_DAYS", &v)?;
    }
    if let Some(v) = read("PULSUS_STORAGE_POLICY") {
        cfg.storage_policy = Some(v);
    }
    if let Some(v) = read("PULSUS_ROTATION_INTERVAL") {
        cfg.rotation_interval = parse_dur("PULSUS_ROTATION_INTERVAL", &v)?;
    }
    if let Some(v) = read("PULSUS_LOG_ROLLUP_RESOLUTION") {
        cfg.log_rollup_resolution = parse_dur("PULSUS_LOG_ROLLUP_RESOLUTION", &v)?;
    }
    if let Some(v) = read("PULSUS_CLUSTER") {
        cfg.cluster = Some(v);
    }
    if let Some(v) = read("PULSUS_DIST_SUFFIX") {
        cfg.dist_suffix = v;
    }
    if let Some(v) = read("PULSUS_SKIP_UNAVAILABLE_SHARDS") {
        cfg.skip_unavailable_shards = parse_bool("PULSUS_SKIP_UNAVAILABLE_SHARDS", &v)?;
    }
    if let Some(v) = read("PULSUS_BATCH_BYTES") {
        cfg.writer.batch_bytes = parse_size("PULSUS_BATCH_BYTES", &v)?;
    }
    if let Some(v) = read("PULSUS_BATCH_MS") {
        cfg.writer.batch_ms = parse_int("PULSUS_BATCH_MS", &v)?;
    }
    if let Some(v) = read("PULSUS_INSERT_MODE") {
        cfg.writer.insert_mode = parse_enum("PULSUS_INSERT_MODE", &v)?;
    }
    if let Some(v) = read("PULSUS_INGEST_QUEUE_BYTES") {
        cfg.writer.ingest_queue_bytes = parse_size("PULSUS_INGEST_QUEUE_BYTES", &v)?;
    }
    if let Some(v) = read("PULSUS_CACHE_TTL") {
        cfg.reader.cache_ttl = parse_dur("PULSUS_CACHE_TTL", &v)?;
    }
    if let Some(v) = read("PULSUS_CACHE_MAX_SERIES") {
        cfg.reader.cache_max_series = parse_int("PULSUS_CACHE_MAX_SERIES", &v)?;
    }
    if let Some(v) = read("PULSUS_SERIES_ACTIVITY_BUCKET") {
        cfg.reader.series_activity_bucket = parse_dur("PULSUS_SERIES_ACTIVITY_BUCKET", &v)?;
    }
    if let Some(v) = read("PULSUS_CACHE_WINDOW") {
        cfg.reader.cache_window = parse_dur("PULSUS_CACHE_WINDOW", &v)?;
    }
    if let Some(v) = read("PULSUS_PROMQL_MAX_SAMPLES") {
        cfg.reader.promql_max_samples = parse_int("PULSUS_PROMQL_MAX_SAMPLES", &v)?;
    }
    if let Some(v) = read("PULSUS_PROMQL_LOOKBACK") {
        cfg.reader.promql_lookback = parse_dur("PULSUS_PROMQL_LOOKBACK", &v)?;
    }
    if let Some(v) = read("PULSUS_PROMQL_EXPERIMENTAL_FUNCTIONS") {
        cfg.reader.promql_experimental_functions =
            parse_bool("PULSUS_PROMQL_EXPERIMENTAL_FUNCTIONS", &v)?;
    }
    if let Some(v) = read("PULSUS_PROMQL_MAX_METRIC_FANOUT") {
        cfg.reader.promql_max_metric_fanout = parse_int("PULSUS_PROMQL_MAX_METRIC_FANOUT", &v)?;
    }
    if let Some(v) = read("PULSUS_LOGQL_SCAN_BUDGET_BYTES") {
        cfg.reader.logql_scan_budget_bytes = parse_size("PULSUS_LOGQL_SCAN_BUDGET_BYTES", &v)?;
    }
    if let Some(v) = read("PULSUS_LOGQL_PIPELINE_SCAN_FACTOR") {
        cfg.reader.logql_pipeline_scan_factor = parse_int("PULSUS_LOGQL_PIPELINE_SCAN_FACTOR", &v)?;
    }
    if let Some(v) = read("PULSUS_TRACEQL_MAX_CANDIDATES") {
        cfg.reader.traceql_max_candidates = parse_int("PULSUS_TRACEQL_MAX_CANDIDATES", &v)?;
    }
    if let Some(v) = read("PULSUS_TRACEQL_SCAN_BUDGET_ROWS") {
        cfg.reader.traceql_scan_budget_rows = parse_int("PULSUS_TRACEQL_SCAN_BUDGET_ROWS", &v)?;
    }
    if let Some(v) = read("PULSUS_TIER_POLICY") {
        cfg.downsampling.tier_policy = parse_enum("PULSUS_TIER_POLICY", &v)?;
    }
    if let Some(v) = read("PULSUS_RULER_ENABLED") {
        cfg.ruler.enabled = parse_bool("PULSUS_RULER_ENABLED", &v)?;
    }
    if let Some(v) = read("PULSUS_RULER_POLL_INTERVAL") {
        cfg.ruler.poll_interval = parse_dur("PULSUS_RULER_POLL_INTERVAL", &v)?;
    }
    if let Some(v) = read("PULSUS_RULER_MAX_RESULT_BYTES") {
        cfg.ruler.max_result_bytes = parse_size("PULSUS_RULER_MAX_RESULT_BYTES", &v)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_env_vars_has_no_duplicates_and_the_documented_count() {
        let mut sorted = ALL_ENV_VARS.to_vec();
        sorted.sort_unstable();
        let mut deduped = sorted.clone();
        deduped.dedup();
        assert_eq!(sorted, deduped, "ALL_ENV_VARS must not contain duplicates");
        assert_eq!(
            ALL_ENV_VARS.len(),
            45,
            "docs/configuration.md §§1-8 document exactly 45 variables"
        );
    }

    #[test]
    fn parse_bool_accepts_true_false_1_0() {
        assert!(parse_bool("X", "true").unwrap());
        assert!(parse_bool("X", "1").unwrap());
        assert!(!parse_bool("X", "false").unwrap());
        assert!(!parse_bool("X", "0").unwrap());
    }

    #[test]
    fn parse_bool_rejects_other_values() {
        assert!(parse_bool("X", "yes").is_err());
    }

    /// Issue #85 (M6-08c): a non-integer `PULSUS_PROMQL_MAX_METRIC_FANOUT`
    /// is rejected at config load through the shared `parse_int` path.
    #[test]
    fn invalid_promql_max_metric_fanout_is_rejected_at_load() {
        let err = parse_int::<u64>("PULSUS_PROMQL_MAX_METRIC_FANOUT", "not-a-number").unwrap_err();
        assert!(err.to_string().contains("PULSUS_PROMQL_MAX_METRIC_FANOUT"));
    }
}
