//! The `Config` → `ChConnConfig` / `SchemaParams` mappings, in exactly one
//! place (task-manager resolution on issue #6, carried over from issue #5's
//! resolution #4): `pulsus-clickhouse` and `pulsus-schema` stay config-free,
//! and every caller in this binary — the `--mode init` schema controller
//! (`schema_init.rs`) and the serving startup path (`serve.rs`, which runs
//! the same schema reconcile before flipping readiness — issue #6 review
//! fix) — derives its connection settings and schema parameters from these
//! functions.

use pulsus_clickhouse::{ChConnConfig, ChProto};
use pulsus_config::Config;
use pulsus_schema::{RenderCtx, SchemaParams};

/// Maps `Config` to the connection settings any part of this binary dials
/// ClickHouse with, targeting the configured `clickhouse.database`. Callers
/// that need a bootstrap connection instead (the target database does not
/// exist yet) use [`bootstrap_conn_config_from`]; the field-by-field mapping
/// itself still lives here exactly once.
pub(crate) fn conn_config_from(config: &Config) -> ChConnConfig {
    ChConnConfig {
        server: config.clickhouse.server.clone(),
        native_port: config.clickhouse.port,
        http_port: config.clickhouse.http_port,
        database: config.clickhouse.database.clone(),
        user: config.clickhouse.auth.user.clone(),
        password: config.clickhouse.auth.password.expose().to_string(),
        proto: match config.clickhouse.proto {
            pulsus_config::ChProto::Native => ChProto::Native,
            pulsus_config::ChProto::Http => ChProto::Http,
            pulsus_config::ChProto::Https => ChProto::Https,
        },
        tls_skip_verify: config.clickhouse.tls_skip_verify,
        pool_size: config.clickhouse.pool_size as usize,
        query_timeout: config.query_timeout.0,
    }
}

/// [`conn_config_from`]'s mapping, with `.database` overridden to
/// ClickHouse's built-in `default` database, never `config.clickhouse.database`
/// directly: on a fresh server the target database does not exist yet, and
/// every ClickHouse HTTP request — even a bare `SELECT 1` health probe —
/// fails with `UNKNOWN_DATABASE` if `?database=` names a database that isn't
/// there yet. `pulsus-schema`'s DDL is fully `{{db}}.`-qualified, so this
/// binding never affects which database the DDL actually targets
/// (docs/schemas.md's "CREATE DATABASE chicken-and-egg" edge case). Used by
/// both `--mode init` (`schema_init.rs`) and the serving reconnect loop's
/// schema-reconcile step (`serve.rs`).
pub(crate) fn bootstrap_conn_config_from(config: &Config) -> ChConnConfig {
    ChConnConfig {
        database: "default".to_string(),
        ..conn_config_from(config)
    }
}

/// Maps `Config` to the schema controller's rendering/reconcile parameters
/// (`pulsus_schema::run_init`/`reconcile`'s `SchemaParams`). Used by both
/// `--mode init` and the serving reconnect loop's schema-reconcile step.
pub(crate) fn schema_params_from(config: &Config) -> SchemaParams {
    RenderCtx {
        db: config.clickhouse.database.clone(),
        cluster: config.cluster.clone(),
        dist_suffix: config.dist_suffix.clone(),
        storage_policy: config.storage_policy.clone(),
        retention_days: config.retention_days,
        log_rollup: config.log_rollup_resolution.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conn_config_from_targets_the_configured_database() {
        let mut config = Config::default();
        config.clickhouse.database = "pulsus".to_string();
        let ch_cfg = conn_config_from(&config);
        assert_eq!(ch_cfg.database, "pulsus");
    }

    #[test]
    fn conn_config_from_maps_proto_variants() {
        let mut config = Config::default();
        config.clickhouse.proto = pulsus_config::ChProto::Https;
        assert_eq!(conn_config_from(&config).proto, ChProto::Https);
    }

    #[test]
    fn conn_config_from_maps_pool_size_and_timeout() {
        let mut config = Config::default();
        config.clickhouse.pool_size = 16;
        let ch_cfg = conn_config_from(&config);
        assert_eq!(ch_cfg.pool_size, 16);
        assert_eq!(ch_cfg.query_timeout, config.query_timeout.0);
    }

    #[test]
    fn bootstrap_conn_config_from_binds_the_default_database_not_the_target_one() {
        let mut config = Config::default();
        config.clickhouse.database = "pulsus".to_string();
        let ch_cfg = bootstrap_conn_config_from(&config);
        assert_eq!(ch_cfg.database, "default");
    }

    #[test]
    fn bootstrap_conn_config_from_still_maps_the_shared_fields() {
        let mut config = Config::default();
        config.clickhouse.proto = pulsus_config::ChProto::Https;
        let ch_cfg = bootstrap_conn_config_from(&config);
        assert_eq!(ch_cfg.proto, ChProto::Https);
        assert_eq!(ch_cfg.server, config.clickhouse.server);
    }

    #[test]
    fn schema_params_from_maps_the_target_database_and_cluster() {
        let mut config = Config::default();
        config.clickhouse.database = "pulsus".to_string();
        config.cluster = Some("prod".to_string());
        let params = schema_params_from(&config);
        assert_eq!(params.db, "pulsus");
        assert_eq!(params.cluster.as_deref(), Some("prod"));
    }
}
