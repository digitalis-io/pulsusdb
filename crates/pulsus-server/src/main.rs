//! PulsusDB server binary (`pulsusdb`). See docs/architecture.md §1 — process
//! model, config load, mode dispatch, and router assembly. M0 wires
//! --version/--help, config load/validation (issue #2), and the `--mode
//! init` schema-controller hook (issue #5); full all/writer/reader mode
//! dispatch and router assembly land in issue #6.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use pulsus_config::Mode;

mod schema_init;

/// Long version string: crate version + build git SHA.
const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), " (", env!("PULSUS_GIT_SHA"), ")");

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
        Ok(_config) => {
            // M0 scaffold: config loaded and validated, but no subsystems
            // mounted yet beyond `--mode init` (full mode dispatch and
            // router assembly are issue #6).
            println!("pulsusdb {VERSION} — scaffold build; no subsystems mounted (M0)");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("pulsusdb: {err}");
            ExitCode::FAILURE
        }
    }
}
