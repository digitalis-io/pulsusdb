//! Cross-field startup validation (docs/configuration.md §7's tier rules,
//! plus sensible guards enumerated in the issue #2 architect plan). Kept
//! separate from [`crate::parse`] so environment/YAML/CLI parsing can be
//! tested independently of validation — in particular, the one-sided
//! basic-auth rule (#12) would otherwise block env-matrix parse coverage
//! for `PULSUS_AUTH_USER`/`PULSUS_AUTH_PASSWORD` (issue #2 plan amendment
//! 2).

use crate::error::ConfigError;
use crate::model::{ChProto, Config};
use crate::units::{ByteSize, HumanDuration};

fn value_err(field: &str, msg: &str, expected: &str) -> ConfigError {
    ConfigError::Value {
        field: field.to_string(),
        msg: msg.to_string(),
        expected: expected.to_string(),
    }
}

fn positive_duration(field: &str, d: HumanDuration) -> Result<(), ConfigError> {
    if d.0.is_zero() {
        return Err(value_err(field, "must be greater than zero", "> 0"));
    }
    Ok(())
}

fn positive_bytes(field: &str, b: ByteSize) -> Result<(), ConfigError> {
    if b.0 == 0 {
        return Err(value_err(field, "must be greater than zero", "> 0"));
    }
    Ok(())
}

fn positive_u64(field: &str, v: u64) -> Result<(), ConfigError> {
    if v == 0 {
        return Err(value_err(field, "must be greater than zero", "> 0"));
    }
    Ok(())
}

/// Issue #96 (retroactive re-review): `reader.promql_max_metric_fanout`
/// bounds a returned distinct-metric-name set (`metrics/exec.rs`'s
/// `rows.len() as u64 > cap`) and a resolved-group count (`metrics/
/// labels.rs`'s `groups.len() as u64 >= fanout_cap`). A value at/near
/// `u64::MAX` makes both comparisons unreachable, silently DISABLING the
/// too-broad guard. This is an explicit metrics-fanout policy ceiling
/// (1000x the default of 1_000, above `cache_max_series` (50_000) and
/// `promql_max_cache_scan` (200_000) so no legitimate warm/probe
/// resolution false-rejects, and small enough that `cap + 1` is always
/// representable) — not derived from any ClickHouse session setting.
pub const PROMQL_MAX_METRIC_FANOUT_CEILING: u64 = 1_000_000;

/// Validates cross-field startup invariants on an already-parsed [`Config`].
/// Enum values are already rejected at parse time (invalid `--mode`,
/// `PULSUS_LOG_LEVEL`, etc.); this only covers rules that need more than
/// one field, or a check that isn't expressible in the type itself.
pub fn validate(cfg: &Config) -> Result<(), ConfigError> {
    // Rule 9: bind/target ports must be non-zero.
    if cfg.port == 0 {
        return Err(value_err("port", "must not be 0", "1-65535"));
    }
    if cfg.clickhouse.port == 0 {
        return Err(value_err("clickhouse.port", "must not be 0", "1-65535"));
    }
    if cfg.clickhouse.http_port == 0 {
        return Err(value_err(
            "clickhouse.http_port",
            "must not be 0",
            "1-65535",
        ));
    }

    // Rule 10: at least one ClickHouse connection per process.
    if cfg.clickhouse.pool_size < 1 {
        return Err(value_err("clickhouse.pool_size", "must be >= 1", ">= 1"));
    }

    // Issue #43: each multi-endpoint entry must name a non-empty host and,
    // when it pins its own port, a non-zero one (an omitted port inherits
    // clickhouse.http_port, already validated above).
    for (i, entry) in cfg.clickhouse.servers.iter().enumerate() {
        if entry.host.trim().is_empty() {
            return Err(value_err(
                &format!("clickhouse.servers[{i}].host"),
                "must not be empty",
                "a non-empty host",
            ));
        }
        if entry.http_port == Some(0) {
            return Err(value_err(
                &format!("clickhouse.servers[{i}].http_port"),
                "must not be 0",
                "1-65535",
            ));
        }
    }

    // Rule 11: `native` is a reserved variant, not an accepted transport —
    // the M0 spike selected the HTTP-only `clickhouse` crate
    // (docs/decisions/0001-clickhouse-client.md, "ADR 0001"), which
    // `pulsus-clickhouse::ChConnConfig::validate` also rejects with the
    // same wording (issue #3 fix plan, finding 3).
    if cfg.clickhouse.proto == ChProto::Native {
        return Err(value_err(
            "clickhouse.proto",
            "native transport is not supported: the M0 client is HTTP-only \
             (docs/decisions/0001-clickhouse-client.md, ADR 0001)",
            "http | https",
        ));
    }

    // Rule 12: one-sided basic auth is a hard startup error (fail closed).
    match (&cfg.auth_user, &cfg.auth_password) {
        (Some(_), None) => {
            return Err(value_err(
                "auth_password",
                "PULSUS_AUTH_USER is set but PULSUS_AUTH_PASSWORD is not",
                "both PULSUS_AUTH_USER and PULSUS_AUTH_PASSWORD set to enable HTTP Basic auth",
            ));
        }
        (None, Some(_)) => {
            return Err(value_err(
                "auth_user",
                "PULSUS_AUTH_PASSWORD is set but PULSUS_AUTH_USER is not",
                "both PULSUS_AUTH_USER and PULSUS_AUTH_PASSWORD set to enable HTTP Basic auth",
            ));
        }
        (None, None) | (Some(_), Some(_)) => {}
    }

    // Rule 13: retention_days.
    if cfg.retention_days < 1 {
        return Err(value_err("retention_days", "must be >= 1", ">= 1"));
    }

    // Rule 15: readers target `<table><dist_suffix>`; an empty suffix would
    // silently point reads at base tables.
    if cfg.dist_suffix.is_empty() {
        return Err(value_err(
            "dist_suffix",
            "must not be empty",
            "a non-empty string",
        ));
    }

    // Rule 14: positive-value guards.
    positive_duration("query_timeout", cfg.query_timeout)?;
    positive_bytes("writer.batch_bytes", cfg.writer.batch_bytes)?;
    positive_u64("writer.batch_ms", cfg.writer.batch_ms)?;
    positive_bytes("writer.ingest_queue_bytes", cfg.writer.ingest_queue_bytes)?;
    positive_u64("reader.cache_max_series", cfg.reader.cache_max_series)?;
    // The `metric_series` activity-bucket floor (docs/schemas.md §2.1) is a
    // divisor in `pulsus_model::floor_to_activity_bucket` — a zero bucket
    // would panic that function's `debug_assert!` (or divide by zero in a
    // release build), so it is validated at startup like every other
    // positive-value config guard, issue #26 open question #4.
    positive_duration(
        "reader.series_activity_bucket",
        cfg.reader.series_activity_bucket,
    )?;
    positive_u64("reader.promql_max_samples", cfg.reader.promql_max_samples)?;
    positive_u64(
        "reader.promql_max_metric_fanout",
        cfg.reader.promql_max_metric_fanout,
    )?;
    // Issue #96 (retroactive re-review): reject values above the ceiling so
    // the fan-out guard (metrics/exec.rs, metrics/labels.rs) can never be
    // configured off.
    if cfg.reader.promql_max_metric_fanout > PROMQL_MAX_METRIC_FANOUT_CEILING {
        return Err(value_err(
            "reader.promql_max_metric_fanout",
            "exceeds the maximum fan-out ceiling (1_000_000): a larger value disables the too-broad guard",
            "1..=1000000",
        ));
    }
    // Issue #89 (retroactive re-review): a zero scan budget would reject
    // every regex/negated-`__name__` selector's resolution before it could
    // examine a single cache entry.
    positive_u64(
        "reader.promql_max_cache_scan",
        cfg.reader.promql_max_cache_scan,
    )?;
    // Issue #82 (retroactive re-review): a zero cap would reject every
    // `info()` query before a single `*_info` series could resolve.
    positive_u64(
        "reader.promql_max_info_series",
        cfg.reader.promql_max_info_series,
    )?;
    positive_bytes(
        "reader.logql_scan_budget_bytes",
        cfg.reader.logql_scan_budget_bytes,
    )?;
    // Issue M6-09 plan v3 delta 2: a zero factor would render the stage-3
    // SQL as `LIMIT 0` whenever a pipeline oversamples — silently empty
    // responses. Floor of 1, catching both YAML and env (`0`).
    positive_u64(
        "reader.logql_pipeline_scan_factor",
        u64::from(cfg.reader.logql_pipeline_scan_factor),
    )?;
    positive_u64(
        "reader.traceql_max_candidates",
        cfg.reader.traceql_max_candidates,
    )?;
    // Issue #101: a zero eval-concurrency bound would admit no eval at all
    // (the semaphore starts with 0 permits — every query would queue
    // forever until the 408 timeout).
    positive_u64(
        "reader.query_eval_concurrency",
        cfg.reader.query_eval_concurrency as u64,
    )?;
    // Issue #74 (M6-11) plan v3 delta 6 (+ v4's slice floor): the live-tail
    // floors. A zero poll interval busy-spins the poll loop; a zero
    // connection cap makes every tail a 429; a zero channel depth is a
    // frame buffer that cannot hold the frame just produced; a zero fetch
    // limit renders `LIMIT 0` (a tail that can never deliver); a zero
    // catch-up slice renders an empty scan window (catch-up never
    // progresses).
    positive_duration("reader.tail_poll_interval", cfg.reader.tail_poll_interval)?;
    positive_u64(
        "reader.tail_max_connections",
        cfg.reader.tail_max_connections as u64,
    )?;
    positive_u64(
        "reader.tail_channel_depth",
        cfg.reader.tail_channel_depth as u64,
    )?;
    positive_u64(
        "reader.tail_max_fetch_limit",
        u64::from(cfg.reader.tail_max_fetch_limit),
    )?;
    positive_duration("reader.tail_catchup_slice", cfg.reader.tail_catchup_slice)?;
    positive_duration("rotation_interval", cfg.rotation_interval)?;
    positive_duration("log_rollup_resolution", cfg.log_rollup_resolution)?;
    positive_duration("ruler.poll_interval", cfg.ruler.poll_interval)?;
    positive_bytes("ruler.max_result_bytes", cfg.ruler.max_result_bytes)?;

    // Issue #114: consistency guards. `insert_quorum_timeout` must be
    // positive, and — when quorum is enabled — must not exceed
    // `query_timeout`, which bounds the whole insert (both the client tokio
    // deadline and the server `max_execution_time`); a larger quorum wait
    // could never be observed, the insert deadline fires first. These
    // config-layer guards are defense-in-depth with better-located
    // `ConfigError::Value{field}` messages; authoritative enforcement lives
    // in `pulsus_clickhouse::ConsistencyConfig::validate_for_deadline`. The
    // cross-field rule is inert when `insert_quorum == 0` (off).
    positive_duration(
        "clickhouse.insert_quorum_timeout",
        cfg.clickhouse.insert_quorum_timeout,
    )?;
    if cfg.clickhouse.insert_quorum > 0
        && cfg.clickhouse.insert_quorum_timeout.0 > cfg.query_timeout.0
    {
        return Err(value_err(
            "clickhouse.insert_quorum_timeout",
            "must not exceed query_timeout when insert_quorum is enabled: the insert \
             deadline (query_timeout) preempts the quorum wait",
            "<= query_timeout",
        ));
    }

    // Issue #114 (code review round 1, finding 2): `insert_quorum == 1` is a
    // silent no-op in ClickHouse — quorum writes are disabled below 2, so a
    // config asking for a 1-replica quorum promises a guarantee that is never
    // applied. Reject it: `0` disables quorum, `>= 2` is an active quorum.
    if cfg.clickhouse.insert_quorum == 1 {
        return Err(value_err(
            "clickhouse.insert_quorum",
            "1 is a silent no-op in ClickHouse (quorum writes are disabled below \
             2): use 0 to disable quorum, or >= 2 for an active quorum",
            "0 (off) or >= 2",
        ));
    }

    // Issue #114 (code review round 1, finding 1): `select_sequential_consistency`
    // (read-your-writes) only holds when quorum inserts are enabled AND
    // non-parallel — ClickHouse cannot deliver the guarantee with parallel
    // quorum or quorum off. Reject the combination fail-fast rather than
    // silently forcing `insert_quorum_parallel = false`, so the operator's
    // stated intent and the delivered behaviour never diverge.
    if cfg.clickhouse.select_sequential_consistency
        && (cfg.clickhouse.insert_quorum == 0 || cfg.clickhouse.insert_quorum_parallel)
    {
        return Err(value_err(
            "clickhouse.select_sequential_consistency",
            "read-your-writes requires quorum inserts enabled and non-parallel: set \
             clickhouse.insert_quorum >= 2 and clickhouse.insert_quorum_parallel = false, \
             or disable clickhouse.select_sequential_consistency",
            "insert_quorum >= 2 and insert_quorum_parallel = false",
        ));
    }

    // Rule 8: raw_retention, if set, must be > 0.
    if let Some(raw_retention) = cfg.downsampling.raw_retention {
        positive_duration("downsampling.raw_retention", raw_retention)?;
    }

    validate_tiers(cfg)?;

    Ok(())
}

/// Rules 1-7 (docs/configuration.md §7): each tier's `min_step >=
/// resolution`; `resolution`/`min_step`/`retention` strictly increasing
/// across tiers in listed order; non-empty names/tables, unique across
/// tiers; `enabled` requires at least one tier.
fn validate_tiers(cfg: &Config) -> Result<(), ConfigError> {
    let tiers = &cfg.downsampling.tiers;

    if cfg.downsampling.enabled && tiers.is_empty() {
        return Err(ConfigError::Tier(
            "downsampling.enabled is true but no tiers are configured".to_string(),
        ));
    }

    let mut names = std::collections::HashSet::new();
    let mut tables = std::collections::HashSet::new();

    for tier in tiers {
        if tier.name.is_empty() {
            return Err(ConfigError::Tier("tier name must not be empty".to_string()));
        }
        if tier.table.is_empty() {
            return Err(ConfigError::Tier(format!(
                "tier {:?}: table must not be empty",
                tier.name
            )));
        }
        if tier.resolution.0.is_zero() {
            return Err(ConfigError::Tier(format!(
                "tier {:?}: resolution must be > 0",
                tier.name
            )));
        }
        if tier.retention.0.is_zero() {
            return Err(ConfigError::Tier(format!(
                "tier {:?}: retention must be > 0",
                tier.name
            )));
        }
        if tier.min_step.0.is_zero() {
            return Err(ConfigError::Tier(format!(
                "tier {:?}: min_step must be > 0",
                tier.name
            )));
        }
        if tier.min_step.0 < tier.resolution.0 {
            return Err(ConfigError::Tier(format!(
                "tier {:?}: min_step ({:?}) must be >= resolution ({:?})",
                tier.name, tier.min_step.0, tier.resolution.0
            )));
        }
        if !names.insert(tier.name.clone()) {
            return Err(ConfigError::Tier(format!(
                "duplicate tier name {:?}",
                tier.name
            )));
        }
        if !tables.insert(tier.table.clone()) {
            return Err(ConfigError::Tier(format!(
                "duplicate tier table {:?}",
                tier.table
            )));
        }
    }

    for pair in tiers.windows(2) {
        let (prev, next) = (&pair[0], &pair[1]);
        if next.resolution.0 <= prev.resolution.0 {
            return Err(ConfigError::Tier(format!(
                "tier {:?} resolution must be strictly greater than tier {:?} resolution",
                next.name, prev.name
            )));
        }
        if next.min_step.0 <= prev.min_step.0 {
            return Err(ConfigError::Tier(format!(
                "tier {:?} min_step must be strictly greater than tier {:?} min_step",
                next.name, prev.name
            )));
        }
        if next.retention.0 <= prev.retention.0 {
            return Err(ConfigError::Tier(format!(
                "tier {:?} retention must be strictly greater than tier {:?} retention",
                next.name, prev.name
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        assert!(validate(&Config::default()).is_ok());
    }

    #[test]
    fn zero_port_is_rejected() {
        let cfg = Config {
            port: 0,
            ..Config::default()
        };
        assert!(matches!(validate(&cfg), Err(ConfigError::Value { field, .. }) if field == "port"));
    }

    #[test]
    fn zero_clickhouse_port_is_rejected() {
        let mut cfg = Config::default();
        cfg.clickhouse.port = 0;
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn zero_clickhouse_http_port_is_rejected() {
        let mut cfg = Config::default();
        cfg.clickhouse.http_port = 0;
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn zero_pool_size_is_rejected() {
        let mut cfg = Config::default();
        cfg.clickhouse.pool_size = 0;
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn native_clickhouse_proto_is_rejected_citing_adr_0001() {
        let mut cfg = Config::default();
        cfg.clickhouse.proto = ChProto::Native;
        let err = validate(&cfg).expect_err("native must be rejected at startup");
        match &err {
            ConfigError::Value { field, msg, .. } => {
                assert_eq!(field, "clickhouse.proto");
                assert!(
                    msg.contains("ADR 0001"),
                    "error must cite ADR 0001, got: {msg}"
                );
            }
            other => panic!("expected ConfigError::Value, got {other:?}"),
        }
    }

    #[test]
    fn default_config_proto_is_http_and_passes_validation() {
        // The default deployment (docs/configuration.md §2) must be
        // startup-valid: `native` is rejected, but the default is `Http`.
        assert_eq!(Config::default().clickhouse.proto, ChProto::Http);
        assert!(validate(&Config::default()).is_ok());
    }

    #[test]
    fn one_sided_auth_user_only_is_a_hard_error() {
        let cfg = Config {
            auth_user: Some("alice".to_string()),
            ..Config::default()
        };
        assert!(
            matches!(validate(&cfg), Err(ConfigError::Value { field, .. }) if field == "auth_password")
        );
    }

    #[test]
    fn one_sided_auth_password_only_is_a_hard_error() {
        let cfg = Config {
            auth_password: Some(crate::secret::Secret::new("hunter2")),
            ..Config::default()
        };
        assert!(
            matches!(validate(&cfg), Err(ConfigError::Value { field, .. }) if field == "auth_user")
        );
    }

    #[test]
    fn both_auth_fields_set_is_valid() {
        let cfg = Config {
            auth_user: Some("alice".to_string()),
            auth_password: Some(crate::secret::Secret::new("hunter2")),
            ..Config::default()
        };
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn zero_retention_days_is_rejected() {
        let cfg = Config {
            retention_days: 0,
            ..Config::default()
        };
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn empty_dist_suffix_is_rejected() {
        let cfg = Config {
            dist_suffix: String::new(),
            ..Config::default()
        };
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn zero_query_timeout_is_rejected() {
        let cfg = Config {
            query_timeout: HumanDuration(std::time::Duration::ZERO),
            ..Config::default()
        };
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn zero_writer_batch_bytes_is_rejected() {
        let mut cfg = Config::default();
        cfg.writer.batch_bytes = ByteSize(0);
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn zero_series_activity_bucket_is_rejected() {
        let mut cfg = Config::default();
        cfg.reader.series_activity_bucket = HumanDuration(std::time::Duration::ZERO);
        assert!(validate(&cfg).is_err());
    }

    /// Issue M6-09 plan v3 delta 2 / AC9(iii): a zero pipeline scan
    /// factor — from YAML or env — is rejected as `ConfigError::Value`
    /// naming the field (a zero factor would render `LIMIT 0`), and the
    /// container default is 10.
    #[test]
    fn zero_logql_pipeline_scan_factor_is_rejected_and_the_default_is_10() {
        assert_eq!(Config::default().reader.logql_pipeline_scan_factor, 10);
        let mut cfg = Config::default();
        cfg.reader.logql_pipeline_scan_factor = 0;
        match validate(&cfg) {
            Err(ConfigError::Value { field, .. }) => {
                assert_eq!(field, "reader.logql_pipeline_scan_factor");
            }
            other => panic!("expected a Value error for the zero factor, got {other:?}"),
        }
    }

    /// Issue #85 (M6-08c): the name-less-selector fan-out cap follows the
    /// sibling u64 caps' validation shape — zero is an invalid value,
    /// rejected at config load, and the documented default is the
    /// adjudicated 1000.
    #[test]
    fn zero_promql_max_metric_fanout_is_rejected_and_the_default_is_1000() {
        assert_eq!(Config::default().reader.promql_max_metric_fanout, 1_000);
        let mut cfg = Config::default();
        cfg.reader.promql_max_metric_fanout = 0;
        assert!(validate(&cfg).is_err());
    }

    /// Issue #96 (retroactive re-review): a `promql_max_metric_fanout` at
    /// or near `u64::MAX` makes the returned-row/group-count fan-out
    /// guards (`metrics/exec.rs`, `metrics/labels.rs`) unreachable —
    /// silently disabling them. Config load must reject anything above
    /// the ceiling while still accepting the ceiling itself and the
    /// documented default.
    #[test]
    fn promql_max_metric_fanout_ceiling_rejects_absurd_and_accepts_the_max() {
        let mut cfg = Config::default();
        cfg.reader.promql_max_metric_fanout = u64::MAX;
        match validate(&cfg) {
            Err(ConfigError::Value { field, .. }) => {
                assert_eq!(field, "reader.promql_max_metric_fanout");
            }
            other => panic!("expected a Value error for u64::MAX, got {other:?}"),
        }

        cfg.reader.promql_max_metric_fanout = PROMQL_MAX_METRIC_FANOUT_CEILING + 1;
        match validate(&cfg) {
            Err(ConfigError::Value { field, .. }) => {
                assert_eq!(field, "reader.promql_max_metric_fanout");
            }
            other => panic!("expected a Value error for ceiling+1, got {other:?}"),
        }

        cfg.reader.promql_max_metric_fanout = PROMQL_MAX_METRIC_FANOUT_CEILING;
        assert!(validate(&cfg).is_ok());

        cfg.reader.promql_max_metric_fanout = 1_000;
        assert!(validate(&cfg).is_ok());
    }

    /// Issue #89 (retroactive re-review): the cache-scan budget follows the
    /// sibling u64 caps' validation shape — zero is an invalid value,
    /// rejected at config load, and the documented default is 200_000.
    #[test]
    fn zero_promql_max_cache_scan_is_rejected_and_the_default_is_200_000() {
        assert_eq!(Config::default().reader.promql_max_cache_scan, 200_000);
        let mut cfg = Config::default();
        cfg.reader.promql_max_cache_scan = 0;
        assert!(validate(&cfg).is_err());
    }

    /// Issue #82 (retroactive re-review): the info() cardinality cap
    /// follows the sibling u64 caps' validation shape — zero is an
    /// invalid value, rejected at config load, and the documented
    /// default is 100_000.
    #[test]
    fn zero_promql_max_info_series_is_rejected_and_the_default_is_100_000() {
        assert_eq!(Config::default().reader.promql_max_info_series, 100_000);
        let mut cfg = Config::default();
        cfg.reader.promql_max_info_series = 0;
        assert!(validate(&cfg).is_err());
    }

    /// Issue #101: the eval-concurrency bound follows the sibling u64 caps'
    /// validation shape — zero is rejected at config load as
    /// `ConfigError::Value` naming the field (a zero bound admits no eval),
    /// and the documented container default is 256.
    #[test]
    fn zero_query_eval_concurrency_is_rejected_and_the_default_is_256() {
        assert_eq!(Config::default().reader.query_eval_concurrency, 256);
        let mut cfg = Config::default();
        cfg.reader.query_eval_concurrency = 0;
        match validate(&cfg) {
            Err(ConfigError::Value { field, .. }) => {
                assert_eq!(field, "reader.query_eval_concurrency");
            }
            other => panic!("expected a Value error for the zero bound, got {other:?}"),
        }
    }

    /// Issue #74 (M6-11) plan v3 delta 6 + v4: each live-tail floor is
    /// rejected at load as `ConfigError::Value` naming its field, and the
    /// container defaults match the documented values.
    #[test]
    fn tail_floors_are_rejected_and_defaults_match_the_documented_values() {
        let d = Config::default();
        assert_eq!(
            d.reader.tail_poll_interval.0,
            std::time::Duration::from_secs(1)
        );
        assert_eq!(d.reader.tail_max_delay.0, std::time::Duration::from_secs(5));
        assert_eq!(d.reader.tail_max_connections, 100);
        assert_eq!(d.reader.tail_max_entries_per_frame, 1_000);
        assert_eq!(d.reader.tail_channel_depth, 4);
        assert_eq!(
            d.reader.tail_send_timeout.0,
            std::time::Duration::from_secs(30)
        );
        assert_eq!(d.reader.tail_max_fetch_limit, 5_000);
        assert_eq!(
            d.reader.tail_catchup_slice.0,
            std::time::Duration::from_secs(60)
        );

        type Mutator = fn(&mut Config);
        let cases: &[(&str, Mutator)] = &[
            ("reader.tail_poll_interval", |c| {
                c.reader.tail_poll_interval = HumanDuration(std::time::Duration::ZERO)
            }),
            ("reader.tail_max_connections", |c| {
                c.reader.tail_max_connections = 0
            }),
            ("reader.tail_channel_depth", |c| {
                c.reader.tail_channel_depth = 0
            }),
            ("reader.tail_max_fetch_limit", |c| {
                c.reader.tail_max_fetch_limit = 0
            }),
            ("reader.tail_catchup_slice", |c| {
                c.reader.tail_catchup_slice = HumanDuration(std::time::Duration::ZERO)
            }),
        ];
        for (field, mutate) in cases {
            let mut cfg = Config::default();
            mutate(&mut cfg);
            match validate(&cfg) {
                Err(ConfigError::Value { field: got, .. }) => {
                    assert_eq!(&got, field, "wrong field named for {field}")
                }
                other => panic!("{field}: expected a Value error, got {other:?}"),
            }
        }
    }

    /// Issue #43: a multi-endpoint entry with an empty host is rejected at
    /// load, naming the indexed field.
    #[test]
    fn empty_server_entry_host_is_rejected() {
        use crate::model::ChServerEntry;
        let mut cfg = Config::default();
        cfg.clickhouse.servers = vec![ChServerEntry {
            host: "  ".to_string(),
            http_port: Some(8123),
            zone: None,
        }];
        match validate(&cfg) {
            Err(ConfigError::Value { field, .. }) => {
                assert_eq!(field, "clickhouse.servers[0].host");
            }
            other => panic!("expected a Value error, got {other:?}"),
        }
    }

    /// Issue #43: an entry that pins a zero port is rejected; an entry that
    /// omits its port (inheriting clickhouse.http_port) is valid.
    #[test]
    fn zero_server_entry_port_is_rejected_but_omitted_port_is_ok() {
        use crate::model::ChServerEntry;
        let mut cfg = Config::default();
        cfg.clickhouse.servers = vec![ChServerEntry {
            host: "ch1".to_string(),
            http_port: Some(0),
            zone: None,
        }];
        assert!(matches!(
            validate(&cfg),
            Err(ConfigError::Value { field, .. }) if field == "clickhouse.servers[0].http_port"
        ));

        cfg.clickhouse.servers = vec![ChServerEntry {
            host: "ch1".to_string(),
            http_port: None,
            zone: Some("az-a".to_string()),
        }];
        assert!(validate(&cfg).is_ok());
    }

    /// AC11 (issue #114): the config-layer consistency guards. An enabled
    /// quorum with `insert_quorum_timeout > query_timeout` is rejected as
    /// `ConfigError::Value{field:"clickhouse.insert_quorum_timeout"}`; the
    /// same values with quorum OFF pass (inert); a zero timeout is rejected;
    /// the default (120s == default query_timeout 120s) passes.
    #[test]
    fn insert_quorum_timeout_guards_reject_over_deadline_and_zero_when_enabled() {
        // > query_timeout with quorum enabled -> rejected, naming the field.
        let mut cfg = Config::default();
        cfg.clickhouse.insert_quorum = 2;
        cfg.clickhouse.insert_quorum_timeout = HumanDuration(std::time::Duration::from_secs(300));
        cfg.query_timeout = HumanDuration(std::time::Duration::from_secs(120));
        match validate(&cfg) {
            Err(ConfigError::Value { field, .. }) => {
                assert_eq!(field, "clickhouse.insert_quorum_timeout");
            }
            other => panic!("expected a Value error for the over-deadline timeout, got {other:?}"),
        }

        // Same values, quorum off -> inert, passes.
        let mut cfg = Config::default();
        cfg.clickhouse.insert_quorum = 0;
        cfg.clickhouse.insert_quorum_timeout = HumanDuration(std::time::Duration::from_secs(300));
        cfg.query_timeout = HumanDuration(std::time::Duration::from_secs(120));
        assert!(validate(&cfg).is_ok());

        // Zero timeout -> rejected by the positive-duration guard.
        let mut cfg = Config::default();
        cfg.clickhouse.insert_quorum_timeout = HumanDuration(std::time::Duration::ZERO);
        match validate(&cfg) {
            Err(ConfigError::Value { field, .. }) => {
                assert_eq!(field, "clickhouse.insert_quorum_timeout");
            }
            other => panic!("expected a Value error for the zero timeout, got {other:?}"),
        }

        // Default is self-consistent (120s == 120s).
        let d = Config::default();
        assert_eq!(d.clickhouse.insert_quorum, 0);
        assert!(d.clickhouse.insert_quorum_parallel);
        assert_eq!(
            d.clickhouse.insert_quorum_timeout.0,
            std::time::Duration::from_secs(120)
        );
        assert!(!d.clickhouse.select_sequential_consistency);
        assert!(validate(&d).is_ok());
    }

    /// Issue #114 (code review round 1, finding 2): `insert_quorum == 1` is a
    /// silent no-op in ClickHouse and is rejected at startup naming the field;
    /// `0` (off) and `2` (active quorum) are accepted.
    #[test]
    fn insert_quorum_of_one_is_rejected_as_a_no_op() {
        // 0 = off -> OK.
        let mut cfg = Config::default();
        cfg.clickhouse.insert_quorum = 0;
        assert!(validate(&cfg).is_ok());

        // 1 = silent no-op -> rejected, naming the field.
        let mut cfg = Config::default();
        cfg.clickhouse.insert_quorum = 1;
        match validate(&cfg) {
            Err(ConfigError::Value { field, .. }) => {
                assert_eq!(field, "clickhouse.insert_quorum");
            }
            other => panic!("expected a Value error for insert_quorum == 1, got {other:?}"),
        }

        // 2 = active quorum -> OK (timeout 120s <= query_timeout 120s).
        let mut cfg = Config::default();
        cfg.clickhouse.insert_quorum = 2;
        assert!(validate(&cfg).is_ok());
    }

    /// Issue #114 (code review round 1, finding 1): `select_sequential_consistency`
    /// (read-your-writes) is only honoured by ClickHouse with quorum inserts
    /// enabled AND non-parallel. Reject it with quorum off or parallel quorum;
    /// accept it with `insert_quorum >= 2` and `insert_quorum_parallel = false`;
    /// leave it untouched when disabled.
    #[test]
    fn select_sequential_consistency_requires_nonparallel_quorum() {
        // seq-consistency on, quorum off -> rejected, naming the field.
        let mut cfg = Config::default();
        cfg.clickhouse.select_sequential_consistency = true;
        cfg.clickhouse.insert_quorum = 0;
        match validate(&cfg) {
            Err(ConfigError::Value { field, .. }) => {
                assert_eq!(field, "clickhouse.select_sequential_consistency");
            }
            other => {
                panic!("expected a Value error for seq-consistency + quorum off, got {other:?}")
            }
        }

        // seq-consistency on, quorum >= 2 but parallel -> rejected.
        let mut cfg = Config::default();
        cfg.clickhouse.select_sequential_consistency = true;
        cfg.clickhouse.insert_quorum = 2;
        cfg.clickhouse.insert_quorum_parallel = true;
        match validate(&cfg) {
            Err(ConfigError::Value { field, .. }) => {
                assert_eq!(field, "clickhouse.select_sequential_consistency");
            }
            other => {
                panic!(
                    "expected a Value error for seq-consistency + parallel quorum, got {other:?}"
                )
            }
        }

        // seq-consistency on, quorum >= 2, non-parallel -> OK.
        let mut cfg = Config::default();
        cfg.clickhouse.select_sequential_consistency = true;
        cfg.clickhouse.insert_quorum = 2;
        cfg.clickhouse.insert_quorum_parallel = false;
        assert!(validate(&cfg).is_ok());

        // seq-consistency off -> any quorum shape passes this rule.
        let mut cfg = Config::default();
        cfg.clickhouse.select_sequential_consistency = false;
        cfg.clickhouse.insert_quorum = 0;
        cfg.clickhouse.insert_quorum_parallel = true;
        assert!(validate(&cfg).is_ok());

        let mut cfg = Config::default();
        cfg.clickhouse.select_sequential_consistency = false;
        cfg.clickhouse.insert_quorum = 2;
        cfg.clickhouse.insert_quorum_parallel = true;
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn zero_raw_retention_is_rejected_when_set() {
        let mut cfg = Config::default();
        cfg.downsampling.raw_retention = Some(HumanDuration(std::time::Duration::ZERO));
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn unset_raw_retention_is_valid() {
        let cfg = Config::default();
        assert!(cfg.downsampling.raw_retention.is_none());
        assert!(validate(&cfg).is_ok());
    }
}
