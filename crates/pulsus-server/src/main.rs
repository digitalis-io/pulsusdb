//! PulsusDB server binary (`pulsusdb`). See docs/architecture.md §1 — process
//! model, config load, mode dispatch, and router assembly. Wires
//! --version/--help, config load/validation (issue #2), the `--mode init`
//! schema-controller hook (issue #5), and full all/writer/reader mode
//! dispatch + router assembly (issue #6).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use pulsus_config::Mode;

mod app;
mod chconfig;
mod compat;
mod ingest;
mod logs_api;
mod middleware;
mod modes;
mod ops;
mod prom_api;
mod schema_init;
mod serve;
mod subsystems;
mod traces_api;

/// Long version string: build version + build git SHA (issue #23:
/// `PULSUS_VERSION` is `build.rs`'s `PULSUS_BUILD_VERSION`-overridable
/// stamp — `CARGO_PKG_VERSION` for local/dev builds, the release tag for a
/// published image — so `--version` and `/status/buildinfo` agree).
const VERSION: &str = concat!(env!("PULSUS_VERSION"), " (", env!("PULSUS_GIT_SHA"), ")");

#[derive(Parser, Debug)]
#[command(name = "pulsusdb", version = VERSION, about = "PulsusDB observability database")]
struct Cli {
    /// Path to a YAML configuration file (docs/configuration.md §9).
    #[arg(long)]
    config: Option<PathBuf>,

    /// Process role override, beating `PULSUS_MODE` (docs/configuration.md
    /// §1). Not a clap `ValueEnum` — validated by `pulsus-config` so the
    /// valid-values list lives in exactly one place.
    #[arg(long)]
    mode: Option<String>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse(); // clap handles --version/--help and exits.

    match pulsus_config::load(cli.config.as_deref(), cli.mode.as_deref()) {
        Ok(config) if config.mode == Mode::Init => schema_init::run(&config).await,
        Ok(config) => serve::run(config).await,
        Err(err) => {
            eprintln!("pulsusdb: {err}");
            ExitCode::FAILURE
        }
    }
}
