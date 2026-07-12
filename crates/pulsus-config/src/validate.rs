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
    positive_u64("reader.promql_max_samples", cfg.reader.promql_max_samples)?;
    positive_bytes(
        "reader.logql_scan_budget_bytes",
        cfg.reader.logql_scan_budget_bytes,
    )?;
    positive_u64(
        "reader.traceql_max_candidates",
        cfg.reader.traceql_max_candidates,
    )?;
    positive_duration("rotation_interval", cfg.rotation_interval)?;
    positive_duration("log_rollup_resolution", cfg.log_rollup_resolution)?;
    positive_duration("ruler.poll_interval", cfg.ruler.poll_interval)?;
    positive_bytes("ruler.max_result_bytes", cfg.ruler.max_result_bytes)?;

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
