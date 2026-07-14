//! `pulsus-e2e` — dedicated e2e harness binary (issue #7 architect plan):
//! owns the whole compose-stack lifecycle (up -> poll `/ready` -> run
//! scenarios -> tear down) for the single-node and 2-shard cluster
//! variants. Talks HTTP to `pulsusdb` only; no ClickHouse client.
//!
//! ```text
//! cargo run -p pulsus-e2e -- --variant single
//! cargo run -p pulsus-e2e -- --variant cluster --engine podman --keep
//! ```

mod corpus;
mod engine;
mod harness;
mod metrics;
mod scenarios;

use std::process::ExitCode;

use clap::Parser;

use engine::EngineKind;
use scenarios::Variant;

#[derive(Parser, Debug)]
#[command(name = "pulsus-e2e", about = "PulsusDB e2e harness")]
struct Cli {
    /// Which compose stack to exercise.
    #[arg(long, value_enum)]
    variant: Variant,

    /// Compose-capable runtime to drive. Defaults to `PULSUS_E2E_ENGINE`,
    /// falling back to probing `docker` then `podman`/`podman-compose` on
    /// `PATH`.
    #[arg(long, value_enum)]
    engine: Option<EngineKind>,

    /// Leave the compose stack running after the scenarios finish (or
    /// fail) instead of tearing it down — useful for local debugging.
    #[arg(long)]
    keep: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let engine = match EngineKind::resolve(cli.engine) {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("pulsus-e2e: {err:#}");
            return ExitCode::FAILURE;
        }
    };

    let opts = harness::RunOptions {
        variant: cli.variant,
        engine,
        keep: cli.keep,
        base_url: "http://127.0.0.1:3100".to_string(),
        // Both compose variants publish the collector's OTLP/HTTP receiver
        // on this same host port (issue #15 architect plan) — see
        // `deploy/e2e/compose.{single,cluster}.yaml`.
        collector_url: "http://127.0.0.1:4318".to_string(),
        // Both compose variants publish the reference Prometheus on this
        // same host port too (issue #33 architect plan) — see
        // `deploy/e2e/compose.{single,cluster}.yaml`.
        prometheus_url: "http://127.0.0.1:9090".to_string(),
    };

    match harness::run(opts).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("pulsus-e2e: {err:#}");
            ExitCode::FAILURE
        }
    }
}
