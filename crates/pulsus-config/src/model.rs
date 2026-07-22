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
    /// `PULSUS_TLS_CERT` (issue #174): path to a PEM certificate chain
    /// (leaf first) for the HTTP listener. Both `tls_cert` and `tls_key`
    /// set ⇒ the single `host:port` listener terminates TLS; both unset ⇒
    /// plaintext; exactly one set ⇒ hard startup error (fail closed, the
    /// `auth_user`/`auth_password` pairing precedent). A plain path — key
    /// material never enters `Config`, so no `Secret` wrapper is needed
    /// and the redacted `/config` dump shows it verbatim.
    pub tls_cert: Option<String>,
    /// `PULSUS_TLS_KEY` (issue #174): path to the matching PEM private key
    /// (PKCS#8/PKCS#1/SEC1). See [`Config::tls_cert`] for the pairing rule.
    pub tls_key: Option<String>,
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
    /// This PulsusDB node's own availability zone (issue #43). When set,
    /// the ClickHouse connection pool prefers endpoints (see
    /// `clickhouse.servers`) whose `zone` matches, failing over to other
    /// zones only when no local endpoint answers. Left unset (and
    /// `az_detect: off`), the pool spreads evenly across all endpoints.
    pub availability_zone: Option<String>,
    /// `PULSUS_AZ_DETECT` (issue #43): when `availability_zone` is not set
    /// explicitly, how to determine this node's zone from cloud instance
    /// metadata at startup (`off` — the default — leaves it unset).
    pub az_detect: AzDetect,
    /// `PULSUS_METRICS_EXP_HISTOGRAM_MODE` (M7-A4, issue #120): how OTLP
    /// exponential-histogram data points are stored. `classic` (default,
    /// current behavior unchanged) flattens to `_bucket`/`_sum`/`_count`
    /// float series; `native` stores the sparse native histogram in
    /// `metric_hist_samples`; `dual` emits both (disjoint fingerprints).
    pub exp_histogram_mode: ExpHistogramMode,
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
            tls_cert: None,
            tls_key: None,
            skip_ddl: false,
            retention_days: 7,
            storage_policy: None,
            rotation_interval: HumanDuration(Duration::from_secs(3_600)),
            log_rollup_resolution: HumanDuration(Duration::from_secs(5)),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            skip_unavailable_shards: false,
            availability_zone: None,
            az_detect: AzDetect::default(),
            exp_histogram_mode: ExpHistogramMode::default(),
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
    /// Multi-endpoint connection list (issue #43). **Empty** (the default)
    /// keeps the single-endpoint behavior driven by `server`/`http_port`.
    /// When populated, the connection pool holds one client per entry and
    /// spreads requests across them (availability-zone aware). An entry that
    /// omits `http_port` inherits `clickhouse.http_port`.
    pub servers: Vec<ChServerEntry>,
    /// `CLICKHOUSE_INSERT_QUORUM` (issue #114, default `0` = off): the
    /// number of replicas that must confirm a block before the insert is
    /// acknowledged. `0` = off, `1` = rejected at startup (a silent no-op in
    /// ClickHouse, which disables quorum below 2 — use `0` to disable),
    /// `>= 2` = active quorum. Integer-only — ClickHouse's `auto` (majority)
    /// value is unsupported. Only meaningful on `Replicated*` engines; adds
    /// latency, so it is off by default (strong consistency is opt-in).
    pub insert_quorum: u64,
    /// `CLICKHOUSE_INSERT_QUORUM_PARALLEL` (issue #114, default `true`).
    /// Only emitted when `insert_quorum > 0`.
    pub insert_quorum_parallel: bool,
    /// `CLICKHOUSE_INSERT_QUORUM_TIMEOUT` (issue #114, default `120s` —
    /// reconciled to equal the default `query_timeout`, which bounds the
    /// whole insert). Must be `0 < timeout <= query_timeout` when
    /// `insert_quorum > 0`, else the insert deadline preempts the quorum
    /// wait. Only emitted when `insert_quorum > 0`.
    pub insert_quorum_timeout: HumanDuration,
    /// `CLICKHOUSE_SELECT_SEQUENTIAL_CONSISTENCY` (issue #114, default
    /// `false`): when set, reads see all prior quorum-committed writes
    /// (read-your-writes). Adds latency, so it is off by default.
    pub select_sequential_consistency: bool,
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
            servers: Vec::new(),
            insert_quorum: 0,
            insert_quorum_parallel: true,
            insert_quorum_timeout: HumanDuration(Duration::from_secs(120)),
            select_sequential_consistency: false,
        }
    }
}

/// One entry in `clickhouse.servers` / `CLICKHOUSE_SERVERS` (issue #43): a
/// ClickHouse endpoint the connection pool may dial. `http_port` falls back
/// to `clickhouse.http_port` when omitted; `zone` names the endpoint's
/// availability zone for the pool's zone-preferring selection policy.
///
/// The flat env form parses `host[:port][=zone]` via [`FromStr`] (e.g.
/// `ch1:8123=az-a`); YAML uses object entries. IPv6 literals (which contain
/// `:`) are only expressible via the YAML `servers:` objects, not the flat
/// env string.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChServerEntry {
    pub host: String,
    #[serde(default)]
    pub http_port: Option<u16>,
    #[serde(default)]
    pub zone: Option<String>,
}

impl std::str::FromStr for ChServerEntry {
    type Err = String;

    /// Parses `host[:port][=zone]`. The zone (after `=`) is split off first,
    /// then an optional `:port` from the right of the host (so a bare host,
    /// `host:port`, `host=zone`, and `host:port=zone` all parse). An empty
    /// zone (`host=`) is treated as no zone.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hostport, zone) = match s.split_once('=') {
            Some((hp, z)) if !z.is_empty() => (hp, Some(z.to_string())),
            Some((hp, _)) => (hp, None),
            None => (s, None),
        };
        let (host, http_port) = match hostport.rsplit_once(':') {
            Some((h, p)) => {
                let port = p
                    .parse::<u16>()
                    .map_err(|e| format!("invalid port {p:?} in server entry {s:?}: {e}"))?;
                (h.to_string(), Some(port))
            }
            None => (hostport.to_string(), None),
        };
        if host.trim().is_empty() {
            return Err(format!("empty host in server entry {s:?}"));
        }
        Ok(ChServerEntry {
            host,
            http_port,
            zone,
        })
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
    /// Mirrors upstream Prometheus's
    /// `--enable-feature=promql-experimental-functions`: when `true`, the
    /// experimental slice of the v3.13 function registry (17 functions;
    /// `limitk`/`limit_ratio` aggregators) is permitted. Issue #64 (M6-01)
    /// lands the config surface only — no experimental function is
    /// implemented yet, so the flag is inert until the first M6 issue
    /// that implements one threads it into planning (the #64 Q2
    /// adjudication: no unconsumed `plan()` signature change today). The
    /// coverage authority for what "experimental" covers is
    /// `crates/pulsus-promql/tests/promqltest/coverage/function-coverage.json`.
    pub promql_experimental_functions: bool,
    /// Issue #85 (M6-08c): the cap on how many metric names one
    /// name-less/regex-`__name__` PromQL selector may fan out to before
    /// the query is rejected as too broad (default 1000, the #85
    /// adjudication) — the bound on the flat `PREWHERE metric_name IN
    /// (…)` fetch's `IN`-set width. Operator-scale tuning routes to #25.
    pub promql_max_metric_fanout: u64,
    /// Issue #89 (retroactive re-review): the independent bound on how
    /// many cache entries (metric names plus candidate fingerprints) one
    /// name-less/regex-`__name__` PromQL selector's resolution may
    /// **examine** before it is rejected as too broad (default 200_000 —
    /// above `cache_max_series` so no legitimate warm resolution
    /// false-rejects). Distinct from `promql_max_metric_fanout`, which
    /// bounds only the *matched* result: a selector whose matchers yield
    /// few or no matches can still examine the whole resident cache
    /// without tripping the fan-out or `cache_max_series` guards.
    /// Operator-scale tuning routes to issue #25.
    pub promql_max_cache_scan: u64,
    /// Issue #82 (retroactive re-review): the pathological-cardinality
    /// backstop on a PromQL `info()` node's synthetic `*_info`
    /// metadata-family selector — how many series that family may
    /// resolve to before the query is rejected as too broad (default
    /// 100_000, above realistic scrape-target-count fleets — a backstop,
    /// not the narrowing). Enforced BEFORE any sample fetch is issued.
    /// Distinct from `promql_max_metric_fanout` (bounds distinct metric
    /// *names*, not series) and `promql_max_cache_scan` (bounds
    /// examined, not matched, cache entries). Identifying-label VALUE
    /// narrowing of the fetch (closing the gap this cap merely
    /// backstops) routes to issue #25.
    pub promql_max_info_series: u64,
    pub logql_scan_budget_bytes: ByteSize,
    /// Issue M6-09 / #90 (LogQL pipelines): the **first-page fetch-size
    /// hint** for fetch-until-limit paging, applied when a query pipeline
    /// contains an in-engine dropping stage that cannot push down (a label
    /// filter, or a line filter after `line_format`). The engine
    /// keyset-pages `limit * factor` rows at a time through the pipeline
    /// until the true `limit` fills, the window is exhausted, or the byte
    /// scan budget is spent — so responses fill exactly to `limit` (no
    /// under-return) and never over-return. This is no longer an
    /// oversample-and-truncate ceiling; a larger factor only sizes the
    /// first page (fewer round-trips), it does not change the result.
    /// `logql_scan_budget_bytes` is an approximate best-effort scan guard,
    /// not a hard byte ceiling: if the first page alone exceeds the budget
    /// the query fails `QueryTooBroad`, but once at least one page has
    /// returned a spent budget (or a later page tripping its positive cap)
    /// returns the survivors so far signaled via `stats.pulsus_partial` —
    /// never issuing a zero/unlimited cap. Because ClickHouse enforces the
    /// cap per block per reader (per thread, per shard), actual bytes can
    /// exceed the budget, growing with parallelism and shard count. Must be
    /// `>= 1` (validated at startup); the container `Default` (not a
    /// field-level serde default, which would
    /// resolve a partial YAML object to 0) supplies 10.
    pub logql_pipeline_scan_factor: u32,
    pub traceql_max_candidates: u64,
    pub traceql_scan_budget_rows: u64,
    /// Issue #57 re-audit (sub-problem B): the trace-search phase-1
    /// candidate-generator query's `max_memory_usage` ceiling (throw) —
    /// bounds a dense common-value prefix's `GROUP BY trace_id`
    /// aggregation state; exceeding it is a `422 query_too_broad`
    /// (server code 241 `MEMORY_LIMIT_EXCEEDED`), never an OOM. Applied
    /// only to the generator read, never phase-2 hydration/membership/
    /// value/root reads. Default 512 MiB — well above the ~21 MB
    /// measured for a 500k-row/500k-key aggregation, well below the
    /// server's 10 GiB default.
    pub traceql_generator_max_memory_bytes: u64,
    /// Issue #101: process-wide bound on concurrent CPU-bound PromQL
    /// evaluations offloaded onto tokio's blocking pool (the read path's
    /// one `spawn_blocking(evaluate)` site). A query past the limit waits
    /// (bounded by `query_timeout`, 408), never a hard rejection. Default
    /// 256 — below tokio's 512 blocking-pool ceiling (so evals cannot
    /// monopolize the pool) yet above realistic heavy-query fan-in (so the
    /// uncontended fast path is the norm). Must be >= 1 (validated at
    /// startup).
    pub query_eval_concurrency: usize,
    /// Issue #74 (M6-11) live tail: how often an idle (caught-up) tail
    /// connection re-polls ClickHouse for new rows. Must be > 0
    /// (validated at startup) — a zero interval would busy-spin the poll
    /// loop.
    pub tail_poll_interval: HumanDuration,
    /// Issue #74: the ceiling on a tail client's `delay_for` request
    /// param (seconds tolerated for late arrivals); values above it are
    /// clamped, never rejected (docs/api.md §2.4).
    pub tail_max_delay: HumanDuration,
    /// Issue #74: process-wide cap on concurrent tail WebSocket
    /// connections; the next connection past it is rejected `429` before
    /// the upgrade. Must be >= 1 (validated at startup).
    pub tail_max_connections: usize,
    /// Issue #74: the bound on the per-frame `dropped_entries`
    /// representative sample a slow tail consumer is sent (the exact
    /// cumulative count always arrives as `dropped_total` — docs/api.md
    /// §2.4).
    pub tail_max_entries_per_frame: usize,
    /// Issue #74: how many undelivered tail frames may queue between the
    /// poll loop and a slow WebSocket writer before the OLDEST frame is
    /// evicted into `dropped_entries`/`dropped_total`. Must be >= 1
    /// (validated at startup — a zero-capacity buffer could never hold
    /// the frame just produced).
    pub tail_channel_depth: usize,
    /// Issue #74: per-send deadline on a tail WebSocket write; a client
    /// that stops reading past this is disconnected rather than pinning
    /// the connection (and its slot) forever.
    pub tail_send_timeout: HumanDuration,
    /// Issue #74: the hard cap on one tail poll's fetched-row `LIMIT`;
    /// a client `limit` above it is clamped BEFORE the query is built
    /// (the row allocation is bounded pre-query). Must be >= 1
    /// (validated at startup).
    pub tail_max_fetch_limit: u32,
    /// Issue #74: the maximum time window one tail poll may scan/sort.
    /// Catch-up over a long backlog proceeds one slice per query (the
    /// loop re-polls immediately until caught up), so no single query
    /// ever sorts an unbounded backlog. Must be > 0 (validated at
    /// startup).
    pub tail_catchup_slice: HumanDuration,
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
            promql_experimental_functions: false,
            promql_max_metric_fanout: 1_000,
            promql_max_cache_scan: 200_000,
            promql_max_info_series: 100_000,
            logql_scan_budget_bytes: ByteSize(50u64 * 1024 * 1024 * 1024),
            logql_pipeline_scan_factor: 10,
            traceql_max_candidates: 100_000,
            traceql_scan_budget_rows: 50_000_000,
            traceql_generator_max_memory_bytes: 536_870_912,
            query_eval_concurrency: 256,
            tail_poll_interval: HumanDuration(Duration::from_secs(1)),
            tail_max_delay: HumanDuration(Duration::from_secs(5)),
            tail_max_connections: 100,
            tail_max_entries_per_frame: 1_000,
            tail_channel_depth: 4,
            tail_send_timeout: HumanDuration(Duration::from_secs(30)),
            tail_max_fetch_limit: 5_000,
            tail_catchup_slice: HumanDuration(Duration::from_secs(60)),
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

/// `PULSUS_AZ_DETECT` (docs/configuration.md §4, issue #43): how this node's
/// own availability zone is determined when `availability_zone` is not set
/// explicitly. `off` (default) leaves it unset (spread evenly); the cloud
/// variants read the provider's instance-metadata service at startup; `auto`
/// tries each provider in turn. An explicitly-set `availability_zone` always
/// wins over any detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AzDetect {
    #[default]
    Off,
    Aws,
    Gcp,
    Azure,
    Auto,
}

impl std::str::FromStr for AzDetect {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "off" => Ok(AzDetect::Off),
            "aws" => Ok(AzDetect::Aws),
            "gcp" => Ok(AzDetect::Gcp),
            "azure" => Ok(AzDetect::Azure),
            "auto" => Ok(AzDetect::Auto),
            _ => Err("one of: off, aws, gcp, azure, auto".to_string()),
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

/// `PULSUS_METRICS_EXP_HISTOGRAM_MODE` (docs/configuration.md §5, M7-A4
/// issue #120): how OTLP exponential-histogram data points are stored.
/// `Classic` (default) keeps the current flatten-to-`_bucket`/`_sum`/
/// `_count` behavior byte-unchanged; `Native` stores the sparse native
/// histogram in `metric_hist_samples`; `Dual` emits both (the classic
/// float series under suffixed names AND one base-name native row — their
/// fingerprints are disjoint, so they never collide).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ExpHistogramMode {
    #[default]
    Classic,
    Native,
    Dual,
}

impl std::str::FromStr for ExpHistogramMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "classic" => Ok(ExpHistogramMode::Classic),
            "native" => Ok(ExpHistogramMode::Native),
            "dual" => Ok(ExpHistogramMode::Dual),
            _ => Err("one of: classic, native, dual".to_string()),
        }
    }
}

impl std::fmt::Display for ExpHistogramMode {
    /// Canonical lowercase rendering, round-tripping with [`FromStr`]
    /// (`FromStr::from_str(&mode.to_string()) == Ok(mode)`).
    ///
    /// [`FromStr`]: std::str::FromStr
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ExpHistogramMode::Classic => "classic",
            ExpHistogramMode::Native => "native",
            ExpHistogramMode::Dual => "dual",
        })
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
    fn ch_server_entry_parses_host_port_and_zone() {
        let e: ChServerEntry = "ch1:8123=az-a".parse().unwrap();
        assert_eq!(e.host, "ch1");
        assert_eq!(e.http_port, Some(8123));
        assert_eq!(e.zone.as_deref(), Some("az-a"));
    }

    #[test]
    fn ch_server_entry_bare_host_has_no_port_or_zone() {
        let e: ChServerEntry = "ch2".parse().unwrap();
        assert_eq!(e.host, "ch2");
        assert_eq!(e.http_port, None);
        assert_eq!(e.zone, None);
    }

    #[test]
    fn ch_server_entry_host_with_zone_only() {
        let e: ChServerEntry = "ch3=az-b".parse().unwrap();
        assert_eq!(e.host, "ch3");
        assert_eq!(e.http_port, None);
        assert_eq!(e.zone.as_deref(), Some("az-b"));
    }

    #[test]
    fn ch_server_entry_host_with_port_only() {
        let e: ChServerEntry = "ch4:9000".parse().unwrap();
        assert_eq!(e.host, "ch4");
        assert_eq!(e.http_port, Some(9000));
        assert_eq!(e.zone, None);
    }

    #[test]
    fn ch_server_entry_empty_zone_is_none() {
        let e: ChServerEntry = "ch5:8123=".parse().unwrap();
        assert_eq!(e.zone, None);
    }

    #[test]
    fn ch_server_entry_rejects_bad_port_and_empty_host() {
        assert!("ch:notaport".parse::<ChServerEntry>().is_err());
        assert!(":8123".parse::<ChServerEntry>().is_err());
    }

    #[test]
    fn az_detect_from_str_rejects_unknown_with_valid_set() {
        assert_eq!("auto".parse::<AzDetect>().unwrap(), AzDetect::Auto);
        assert_eq!("off".parse::<AzDetect>().unwrap(), AzDetect::Off);
        let err = "bogus".parse::<AzDetect>().unwrap_err();
        assert!(err.contains("off, aws, gcp, azure, auto"), "{err}");
    }

    #[test]
    fn exp_histogram_mode_defaults_to_classic_and_parses_each_value() {
        assert_eq!(ExpHistogramMode::default(), ExpHistogramMode::Classic);
        assert_eq!(
            Config::default().exp_histogram_mode,
            ExpHistogramMode::Classic
        );
        assert_eq!(
            "classic".parse::<ExpHistogramMode>().unwrap(),
            ExpHistogramMode::Classic
        );
        assert_eq!(
            "native".parse::<ExpHistogramMode>().unwrap(),
            ExpHistogramMode::Native
        );
        assert_eq!(
            "dual".parse::<ExpHistogramMode>().unwrap(),
            ExpHistogramMode::Dual
        );
        let err = "bogus".parse::<ExpHistogramMode>().unwrap_err();
        assert!(err.contains("classic, native, dual"), "{err}");
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
