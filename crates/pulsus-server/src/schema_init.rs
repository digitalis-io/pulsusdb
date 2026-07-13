//! `--mode init` wiring (issue #5): runs the schema controller to
//! completion and exits. The `Config` → `ChConnConfig` / `SchemaParams`
//! mappings live in [`crate::chconfig`] (issue #6 task-manager resolution —
//! exactly once in this binary), shared with the serving reconnect loop's
//! own schema-reconcile step in `serve.rs` (issue #6 review fix — a serving
//! process must create the schema too, not just `--mode init`).

use std::process::ExitCode;

use pulsus_clickhouse::ChClient;
use pulsus_config::Config;

use crate::chconfig::{bootstrap_conn_config_from, schema_params_from};

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

    let ch_cfg = bootstrap_conn_config_from(config);
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
