//! `--mode init` wiring (issue #5): the `Config` → `ChConnConfig` /
//! `SchemaParams` mapping and the `pulsus-schema` call, kept in one place
//! per the issue #5 task-manager resolution — `pulsus-clickhouse` stays
//! config-free and `pulsus-schema` takes only an already-built client plus
//! plain, `Config`-derived data, so this mapping exists exactly once.

use std::process::ExitCode;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto};
use pulsus_config::Config;
use pulsus_schema::{RenderCtx, SchemaParams};

/// Runs `--mode init` to completion: refuse contradictory flags, connect,
/// version-gate, reconcile the schema, apply TTL, and map the outcome to a
/// process exit code. `0` on success (including the idempotent "already
/// initialized" case); `1` on any refusal or failure, with the specific
/// reason on stderr (docs/schemas.md's version-refusal and
/// `PULSUS_SKIP_DDL`-refusal requirements need a *nonzero* exit and a clear
/// message, not a specific code per failure kind — matching this binary's
/// existing `ConfigError` handling in `main.rs`).
pub async fn run(config: &Config) -> ExitCode {
    if let Err(err) = pulsus_schema::guard_skip_ddl_in_init(config.skip_ddl) {
        eprintln!("pulsusdb: {err}");
        return ExitCode::FAILURE;
    }

    let ch_cfg = conn_config_from(config);
    let client = match ChClient::new(ch_cfg).await {
        Ok(client) => client,
        Err(err) => {
            eprintln!("pulsusdb: {err}");
            return ExitCode::FAILURE;
        }
    };

    let params = schema_params_from(config);
    match pulsus_schema::run_init(&client, &params).await {
        Ok(()) => {
            println!("pulsusdb: schema initialized (database {:?})", params.db);
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("pulsusdb: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Maps `Config` to the connection settings `--mode init` dials with. The
/// connection is deliberately bound to ClickHouse's built-in `default`
/// database, never `config.clickhouse.database` directly: on a fresh
/// server the target database does not exist yet, and every ClickHouse
/// HTTP request — even a bare `SELECT 1` health probe — fails with
/// `UNKNOWN_DATABASE` if `?database=` names a database that isn't there
/// yet. `pulsus-schema`'s DDL is fully `{{db}}.`-qualified, so this binding
/// never affects which database the DDL actually targets (docs/schemas.md's
/// "CREATE DATABASE chicken-and-egg" edge case).
fn conn_config_from(config: &Config) -> ChConnConfig {
    ChConnConfig {
        server: config.clickhouse.server.clone(),
        native_port: config.clickhouse.port,
        http_port: config.clickhouse.http_port,
        database: "default".to_string(),
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

/// Maps `Config` to the schema controller's rendering/reconcile parameters.
fn schema_params_from(config: &Config) -> SchemaParams {
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
    fn conn_config_from_binds_the_default_database_not_the_target_one() {
        let mut config = Config::default();
        config.clickhouse.database = "pulsus".to_string();
        let ch_cfg = conn_config_from(&config);
        assert_eq!(ch_cfg.database, "default");
    }

    #[test]
    fn conn_config_from_maps_proto_variants() {
        let mut config = Config::default();
        config.clickhouse.proto = pulsus_config::ChProto::Https;
        assert_eq!(conn_config_from(&config).proto, ChProto::Https);
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
