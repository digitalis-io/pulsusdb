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
mod azdetect;
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
mod tls;
mod traces_api;

// ---- issue #312: the thread-local allocation gate (TEST BUILDS ONLY) ----
#[cfg(test)]
pub(crate) mod probe_alloc {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    // The house convention gives allocator gates their own test binary
    // because a PROCESS-GLOBAL counter races parallel tests
    // (`logql_retained_ordering_gate.rs`, `logql_preflight_alloc_gate.rs`).
    // That reason is about the COUNTER, not the hook: these counters are
    // thread-local, so a `#[test]` arms them on its own thread and no
    // other test thread can perturb them — which is what lets this live
    // in a bin-only crate with no `--test-threads=1` requirement.
    // `const`-initialised `Cell`s allocate nothing and register no
    // destructor, so the hook cannot re-enter the allocator.
    thread_local! {
        /// `-1` = disarmed; `>= 0` = allocations counted on this thread.
        static N: Cell<i64> = const { Cell::new(-1) };
        /// Bytes currently live inside the armed window.
        static LIVE: Cell<i64> = const { Cell::new(0) };
        /// High-water mark of `LIVE` — the quantity issue #312 bounds.
        static PEAK: Cell<i64> = const { Cell::new(0) };
    }

    fn armed() -> bool {
        N.try_with(|n| n.get() >= 0).unwrap_or(false)
    }

    fn grew(size: i64) {
        if !armed() {
            return;
        }
        let _ = N.try_with(|n| n.set(n.get() + 1));
        let _ = LIVE.try_with(|l| {
            let live = l.get() + size;
            l.set(live);
            let _ = PEAK.try_with(|pk| {
                if live > pk.get() {
                    pk.set(live);
                }
            });
        });
    }

    fn shrank(size: i64) {
        if !armed() {
            return;
        }
        let _ = LIVE.try_with(|l| l.set(l.get() - size));
    }

    #[derive(Debug)]
    pub struct Counting;

    // SAFETY: delegates verbatim to the system allocator; the hooks only
    // touch `const`-init thread-local `Cell`s, which cannot re-enter it.
    // `try_with` keeps TLS teardown from panicking.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            grew(l.size() as i64);
            unsafe { System.alloc(l) }
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            shrank(l.size() as i64);
            unsafe { System.dealloc(p, l) }
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
            // Both blocks are live for the copy, then the old one is
            // freed — so the high-water mark records the realloc peak
            // honestly.
            grew(new as i64);
            shrank(l.size() as i64);
            unsafe { System.realloc(p, l, new) }
        }
    }

    /// Runs `f` with this thread's counters armed. Returns
    /// `(value, allocations, peak live bytes)`. A `realloc` counts as an
    /// allocation (a grow IS one) and contributes BOTH blocks to the
    /// peak, since both are live for the copy.
    ///
    /// The counters measure REQUESTED layout sizes, not allocator-rounded
    /// blocks — a model of peak in exactly the sense
    /// `pulsus_read::logql::charge::alloc_block_bytes` is one.
    pub fn measure<T>(f: impl FnOnce() -> T) -> (T, u64, u64) {
        LIVE.with(|l| l.set(0));
        PEAK.with(|pk| pk.set(0));
        N.with(|n| n.set(0));
        let out = f();
        let c = N.with(|n| {
            let v = n.get();
            n.set(-1);
            v
        });
        let peak = PEAK.with(|pk| pk.get());
        (out, c.max(0) as u64, peak.max(0) as u64)
    }
}

/// Test builds only: production binaries keep the platform allocator
/// untouched.
#[cfg(test)]
#[global_allocator]
static PROBE_ALLOC: probe_alloc::Counting = probe_alloc::Counting;

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
        Ok(mut config) => {
            // Issue #43: resolve this node's availability zone before any
            // ClickHouse connection is built. No-op unless the operator
            // opted into auto-detection and did not hard-set the zone; the
            // resolved `local_zone` then flows through `conn_config_from`.
            azdetect::resolve_local_zone(&mut config).await;
            if config.mode == Mode::Init {
                schema_init::run(&config).await
            } else {
                serve::run(config).await
            }
        }
        Err(err) => {
            eprintln!("pulsusdb: {err}");
            ExitCode::FAILURE
        }
    }
}
