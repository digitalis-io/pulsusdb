//! `--mode init` wiring (issue #5): runs the schema controller to
//! completion and exits. The `Config` → `ChConnConfig` / `SchemaParams`
//! mappings live in [`crate::chconfig`] (issue #6 task-manager resolution —
//! exactly once in this binary), shared with the serving reconnect loop's
//! own schema-reconcile step in `serve.rs` (issue #6 review fix — a serving
//! process must create the schema too, not just `--mode init`).

use std::process::ExitCode;

use pulsus_clickhouse::ChClient;
use pulsus_config::Config;
use pulsus_schema::NameCatalogue;

use crate::chconfig::{bootstrap_conn_config_from, schema_params_from};

/// Runs `--mode init` to completion: refuse contradictory flags, connect,
/// version-gate, reconcile the schema, apply TTL, and map the outcome to a
/// process exit code. `0` on success (including the idempotent "already
/// initialized" case); `1` on any refusal or failure, with the specific
/// reason on stderr (docs/schemas.md's version-refusal and
/// `PULSUS_SKIP_DDL`-refusal requirements need a *nonzero* exit and a clear
/// message, not a specific code per failure kind — matching this binary's
/// existing `ConfigError` handling in `main.rs`).
pub async fn run(config: &Config, required: &'static [(&'static str, NameCatalogue)]) -> ExitCode {
    match run_checked(config, required).await {
        Ok(msg) => {
            println!("pulsusdb: {msg}");
            ExitCode::SUCCESS
        }
        Err(msg) => {
            eprintln!("pulsusdb: {msg}");
            ExitCode::FAILURE
        }
    }
}

/// [`run`]'s whole body, with the outcome as a message rather than an
/// `ExitCode`. The seam exists because `ExitCode` has no `PartialEq`, so a
/// test cannot assert the refusal through `run`; every message is the one
/// `run` printed before (issue #603).
pub(crate) async fn run_checked(
    config: &Config,
    required: &'static [(&'static str, NameCatalogue)],
) -> Result<String, String> {
    pulsus_schema::guard_skip_ddl_in_init(config.skip_ddl).map_err(|e| e.to_string())?;

    let _ = required;
    let client = ChClient::new(bootstrap_conn_config_from(config))
        .await
        .map_err(|e| e.to_string())?;

    let params = schema_params_from(config);
    pulsus_schema::run_init(&client, &params)
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("schema initialized (database {:?})", params.db))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #603: init mode refuses a missing name **before** the `CREATE` —
    /// a check placed inside or after `run_init` would leave the schema
    /// created. Afterwards the target database does not exist at all, and the
    /// message names the absent name.
    #[tokio::test]
    async fn a_missing_server_name_refuses_init_mode_before_any_ddl() {
        if !pulsus_testkit::live_clickhouse_enabled() {
            eprintln!("skipping: PULSUS_TEST_CLICKHOUSE is not set");
            return;
        }
        const MADE_UP: &[(&str, NameCatalogue)] =
            &[("pulsus_not_a_setting", NameCatalogue::Setting)];

        let db = pulsus_testkit::test_db("pulsus_server_it_init_name_check");
        let mut config = Config::default();
        config.clickhouse.database = db.clone();
        config.clickhouse.server =
            std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string());
        config.clickhouse.http_port = std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123);

        let err = run_checked(&config, MADE_UP)
            .await
            .expect_err("a missing name refuses init mode");
        assert!(
            err.contains("pulsus_not_a_setting"),
            "the refusal names the absent name: {err}"
        );

        // Not even the database exists: the refusal came before any DDL.
        let bootstrap = ChClient::new(bootstrap_conn_config_from(&config))
            .await
            .expect("bootstrap connect");
        let tables = count_tables(&bootstrap, &db).await;
        assert_eq!(tables, 0, "init mode created nothing before refusing");

        // The same call with the real list initialises the schema.
        run_checked(&config, pulsus_schema::REQUIRED_SERVER_NAMES)
            .await
            .expect("the real list initialises the schema");
        assert!(
            count_tables(&bootstrap, &db).await > 0,
            "the landing table and the rest are there now"
        );
        let landing = count_named_table(&bootstrap, &db, "metric_landing").await;
        assert_eq!(landing, 1, "metric_landing is one of them");

        bootstrap
            .execute(
                &format!("DROP DATABASE IF EXISTS {db}"),
                &pulsus_clickhouse::QuerySettings::new(),
                pulsus_clickhouse::Idempotency::Idempotent,
            )
            .await
            .expect("drop the test database");
    }

    #[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct CountRow {
        n: u64,
    }

    async fn scalar(client: &ChClient, sql: &str) -> u64 {
        use futures::StreamExt;
        let mut stream = client
            .query_stream::<CountRow>(sql, &pulsus_clickhouse::QuerySettings::new())
            .await
            .expect("query system.tables");
        stream
            .next()
            .await
            .expect("one row")
            .expect("decode CountRow")
            .n
    }

    async fn count_tables(client: &ChClient, db: &str) -> u64 {
        scalar(
            client,
            &format!("SELECT count() AS n FROM system.tables WHERE database = '{db}'"),
        )
        .await
    }

    async fn count_named_table(client: &ChClient, db: &str, name: &str) -> u64 {
        scalar(
            client,
            &format!(
                "SELECT count() AS n FROM system.tables \
                 WHERE database = '{db}' AND name = '{name}'"
            ),
        )
        .await
    }
}
