//! Typed configuration model. Every struct and field here matches
//! docs/configuration.md §9's complete YAML schema 1:1 — root-level
//! scalars stay flat (not nested) so that a documented `retention_days: 7`
//! at the YAML root deserializes correctly under `deny_unknown_fields`
//! (see the issue #2 architect amendment). Only the genuinely-grouped
//! subsystems (`clickhouse`, `writer`, `reader`, `downsampling`, `ruler`)
//! are nested objects.
//!
//! Every struct carries `#[serde(default, deny_unknown_fields)]` plus a
//! hand-written `Default` impl encoding the documented default — this is
//! the single source of truth for defaults. `#[serde(default)]` at the
//! container level fills any field missing from the input with the value
//! from `Default::default()`, so a partial YAML object (only some keys
//! present) still resolves every other key to its documented default.
//! `deny_unknown_fields` turns a typo'd YAML key into an actionable parse
//! error instead of silently ignoring it.

use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::ConfigError;
use crate::secret::Secret;
use crate::units::{ByteSize, HumanDuration};

/// The complete effective configuration (docs/configuration.md §9).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    // §1 Core
    pub mode: Mode,
    pub host: String,
    pub port: u16,
    pub log_level: LogLevel,
    pub auth_user: Option<String>,
    pub auth_password: Option<Secret>,
    pub compat_endpoints: bool,
    pub cors_origin: String,
    pub query_timeout: HumanDuration,
    // §3 Schema & retention
    pub skip_ddl: bool,
    pub retention_days: u32,
    pub storage_policy: Option<String>,
    pub rotation_interval: HumanDuration,
    pub log_rollup_resolution: HumanDuration,
    // §4 Clustering
    pub cluster: Option<String>,
    pub dist_suffix: String,
    pub skip_unavailable_shards: bool,
    // Nested subsystem objects
    pub clickhouse: ClickHouseConfig,
    pub writer: WriterConfig,
    pub reader: ReaderConfig,
    pub downsampling: DownsamplingConfig,
    pub ruler: RulerConfig,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            mode: Mode::All,
            host: "0.0.0.0".to_string(),
            port: 3100,
            log_level: LogLevel::Info,
            auth_user: None,
            auth_password: None,
            compat_endpoints: false,
            cors_origin: "*".to_string(),
            query_timeout: HumanDuration(Duration::from_secs(120)),
            skip_ddl: false,
            retention_days: 7,
            storage_policy: None,
            rotation_interval: HumanDuration(Duration::from_secs(3_600)),
            log_rollup_resolution: HumanDuration(Duration::from_secs(5)),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            skip_unavailable_shards: false,
            clickhouse: ClickHouseConfig::default(),
            writer: WriterConfig::default(),
            reader: ReaderConfig::default(),
            downsampling: DownsamplingConfig::default(),
            ruler: RulerConfig::default(),
        }
    }
}

impl Config {
    /// Serialises the effective configuration as YAML with all secrets
    /// redacted to `"***"`. Redaction is a type-level property of
    /// [`Secret`]/[`ChAuth`], not a runtime scrub — safe to expose via the
    /// `/config` endpoint (issue #6 mounts the route; this is the dump).
    pub fn to_redacted_yaml(&self) -> Result<String, ConfigError> {
        serde_norway::to_string(self).map_err(|source| ConfigError::Yaml {
            path: "<redacted config dump>".to_string(),
            source,
        })
    }
}

/// ClickHouse connection settings (docs/configuration.md §2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClickHouseConfig {
    pub server: String,
    pub port: u16,
    pub http_port: u16,
    pub database: String,
    pub auth: ChAuth,
    pub proto: ChProto,
    pub tls_skip_verify: bool,
    pub pool_size: u32,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        ClickHouseConfig {
            server: "localhost".to_string(),
            port: 9_000,
            http_port: 8_123,
            database: "pulsus".to_string(),
            auth: ChAuth::default(),
            proto: ChProto::default(),
            tls_skip_verify: false,
            pool_size: 8,
        }
    }
}

/// Writer (ingestion) settings (docs/configuration.md §5).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WriterConfig {
    pub batch_bytes: ByteSize,
    pub batch_ms: u64,
    pub insert_mode: InsertMode,
    pub ingest_queue_bytes: ByteSize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        WriterConfig {
            batch_bytes: ByteSize(16 * 1024 * 1024),
            batch_ms: 200,
            insert_mode: InsertMode::Sync,
            ingest_queue_bytes: ByteSize(256 * 1024 * 1024),
        }
    }
}

/// Reader (query) settings (docs/configuration.md §6).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReaderConfig {
    pub cache_ttl: HumanDuration,
    pub cache_max_series: u64,
    pub series_activity_bucket: HumanDuration,
    pub cache_window: HumanDuration,
    pub promql_max_samples: u64,
    pub promql_lookback: HumanDuration,
    pub logql_scan_budget_bytes: ByteSize,
    pub traceql_max_candidates: u64,
    pub traceql_scan_budget_rows: u64,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        ReaderConfig {
            cache_ttl: HumanDuration(Duration::from_secs(60)),
            cache_max_series: 50_000,
            series_activity_bucket: HumanDuration(Duration::from_secs(3_600)),
            cache_window: HumanDuration(Duration::from_secs(24 * 3_600)),
            promql_max_samples: 50_000_000,
            promql_lookback: HumanDuration(Duration::from_secs(300)),
            logql_scan_budget_bytes: ByteSize(50u64 * 1024 * 1024 * 1024),
            traceql_max_candidates: 100_000,
            traceql_scan_budget_rows: 50_000_000,
        }
    }
}

/// Downsampling tier settings (docs/configuration.md §7, M3). Tier layout
/// is YAML-only — the shape doesn't flatten into env vars.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DownsamplingConfig {
    pub enabled: bool,
    pub raw_retention: Option<HumanDuration>,
    pub tier_policy: TierPolicy,
    pub tiers: Vec<Tier>,
}

impl Default for DownsamplingConfig {
    fn default() -> Self {
        DownsamplingConfig {
            enabled: false,
            raw_retention: None,
            tier_policy: TierPolicy::Exact,
            tiers: Vec::new(),
        }
    }
}

/// A single downsampling tier (docs/configuration.md §7). All fields are
/// required when a tier object is present — there is no sensible default
/// for a tier's name, table, or resolution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tier {
    pub name: String,
    pub resolution: HumanDuration,
    pub table: String,
    pub retention: HumanDuration,
    pub min_step: HumanDuration,
}

/// Ruler settings (docs/configuration.md §8, M7).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RulerConfig {
    pub enabled: bool,
    pub poll_interval: HumanDuration,
    pub max_result_bytes: ByteSize,
}

impl Default for RulerConfig {
    fn default() -> Self {
        RulerConfig {
            enabled: false,
            poll_interval: HumanDuration(Duration::from_secs(30)),
            max_result_bytes: ByteSize(10 * 1024 * 1024),
        }
    }
}

/// `PULSUS_MODE` / `--mode` (docs/configuration.md §1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    All,
    Writer,
    Reader,
    Init,
}

impl std::str::FromStr for Mode {
    type Err = String;

    /// On failure, returns just the valid-value list (e.g. `"one of: all,
    /// writer, reader, init"`) — callers wrap this with the offending
    /// input and their own error context (`ConfigError::Env` /
    /// `ConfigError::Value`), so the "expected" clause is stated once.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "all" => Ok(Mode::All),
            "writer" => Ok(Mode::Writer),
            "reader" => Ok(Mode::Reader),
            "init" => Ok(Mode::Init),
            _ => Err("one of: all, writer, reader, init".to_string()),
        }
    }
}

/// `PULSUS_LOG_LEVEL` (docs/configuration.md §1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

impl std::str::FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "error" => Ok(LogLevel::Error),
            "warn" => Ok(LogLevel::Warn),
            "info" => Ok(LogLevel::Info),
            "debug" => Ok(LogLevel::Debug),
            "trace" => Ok(LogLevel::Trace),
            _ => Err("one of: error, warn, info, debug, trace".to_string()),
        }
    }
}

/// `CLICKHOUSE_PROTO` (docs/configuration.md §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ChProto {
    Native,
    #[default]
    Http,
    Https,
}

impl std::str::FromStr for ChProto {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "native" => Ok(ChProto::Native),
            "http" => Ok(ChProto::Http),
            "https" => Ok(ChProto::Https),
            _ => Err("one of: native, http, https".to_string()),
        }
    }
}

/// `PULSUS_INSERT_MODE` (docs/configuration.md §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum InsertMode {
    #[default]
    Sync,
    Async,
}

impl std::str::FromStr for InsertMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sync" => Ok(InsertMode::Sync),
            "async" => Ok(InsertMode::Async),
            _ => Err("one of: sync, async".to_string()),
        }
    }
}

/// `PULSUS_TIER_POLICY` (docs/configuration.md §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TierPolicy {
    #[default]
    Exact,
    Fast,
}

impl std::str::FromStr for TierPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "exact" => Ok(TierPolicy::Exact),
            "fast" => Ok(TierPolicy::Fast),
            _ => Err("one of: exact, fast".to_string()),
        }
    }
}

/// `CLICKHOUSE_AUTH` / `clickhouse.auth`: a `user:password` string split on
/// the **first** colon (passwords may themselves contain `:`, e.g.
/// `user:pa:ss` — splitting on the last colon would corrupt such a
/// credential). Serialises as `user:***`; the password is a [`Secret`].
#[derive(Debug, Clone, PartialEq)]
pub struct ChAuth {
    pub user: String,
    pub password: Secret,
}

impl Default for ChAuth {
    fn default() -> Self {
        ChAuth {
            user: "default".to_string(),
            password: Secret::new(String::new()),
        }
    }
}

impl std::str::FromStr for ChAuth {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once(':') {
            Some((user, password)) => Ok(ChAuth {
                user: user.to_string(),
                password: Secret::new(password.to_string()),
            }),
            None => Err(format!(
                "invalid value {s:?} (expected \"user:password\", missing ':')"
            )),
        }
    }
}

impl Serialize for ChAuth {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}:***", self.user))
    }
}

impl<'de> Deserialize<'de> for ChAuth {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_default_matches_documented_defaults() {
        let cfg = Config::default();
        assert_eq!(cfg.mode, Mode::All);
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.port, 3100);
        assert_eq!(cfg.log_level, LogLevel::Info);
        assert_eq!(cfg.cors_origin, "*");
        assert_eq!(cfg.retention_days, 7);
        assert_eq!(cfg.dist_suffix, "_dist");
        assert_eq!(cfg.clickhouse.server, "localhost");
        assert_eq!(cfg.clickhouse.auth.user, "default");
        assert!(cfg.clickhouse.auth.password.is_empty());
    }

    #[test]
    fn mode_from_str_rejects_unknown_values_with_the_valid_set() {
        let err = "bogus".parse::<Mode>().unwrap_err();
        assert!(err.contains("all, writer, reader, init"), "{err}");
    }

    #[test]
    fn ch_auth_splits_on_first_colon_only() {
        let auth: ChAuth = "user:pa:ss".parse().unwrap();
        assert_eq!(auth.user, "user");
        assert_eq!(auth.password.expose(), "pa:ss");
    }

    #[test]
    fn ch_auth_without_colon_is_a_parse_error() {
        assert!("no-colon-here".parse::<ChAuth>().is_err());
    }

    #[test]
    fn ch_auth_serializes_password_as_redacted() {
        let auth = ChAuth {
            user: "admin".to_string(),
            password: Secret::new("s3cret"),
        };
        let yaml = serde_norway::to_string(&auth).unwrap();
        assert!(yaml.contains("admin:***"));
        assert!(!yaml.contains("s3cret"));
    }
}
