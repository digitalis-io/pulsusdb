//! PulsusDB server binary (`pulsusdb`). See docs/architecture.md §1 — process
//! model, config load, mode dispatch, and router assembly. M0 wires only
//! --version/--help; mode dispatch (issue #2) and API mounting land later.

use clap::Parser;

/// Long version string: crate version + build git SHA.
const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), " (", env!("PULSUS_GIT_SHA"), ")");

#[derive(Parser, Debug)]
#[command(name = "pulsusdb", version = VERSION, about = "PulsusDB observability database")]
struct Cli {}

fn main() {
    let _cli = Cli::parse(); // clap handles --version/--help and exits.
    // M0 scaffold: no subsystems mounted yet.
    println!("pulsusdb {VERSION} — scaffold build; no subsystems mounted (M0)");
}
