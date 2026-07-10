//! Test-only helpers shared by the integration tests in this directory.
//! Not compiled as its own test binary — cargo only treats files placed
//! directly under `tests/` as separate integration-test crates, so a
//! `tests/support/mod.rs` submodule is invisible to the test harness except
//! via `mod support;`.
//!
//! `std::env` is process-global, so tests that set environment variables
//! must serialize with every other test in the same binary that also
//! touches the environment (different `tests/*.rs` files are already
//! separate processes and don't interfere with each other).
//!
//! Each `tests/*.rs` file compiles this module independently (as its own
//! integration-test crate) and uses a different subset of these helpers;
//! `dead_code` is allowed here rather than duplicating helpers per file.
#![allow(dead_code)]

use std::sync::{Mutex, MutexGuard, OnceLock};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Acquires the process-wide environment lock for this test binary. Hold
/// the returned guard for the entire duration of a test that reads or
/// writes environment variables.
pub fn lock_env() -> MutexGuard<'static, ()> {
    ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Clears every documented pulsus-config environment variable, so a test
/// starts from a clean slate regardless of what an earlier test (or the
/// host shell) left behind. Caller must hold `lock_env()`.
pub fn clear_all() {
    for var in pulsus_config::ALL_ENV_VARS {
        // SAFETY: `std::env::remove_var` is only unsound if called
        // concurrently with another thread reading/writing the
        // environment; the caller holds `lock_env()`, which every test in
        // this binary that touches the environment also acquires.
        unsafe { std::env::remove_var(var) };
    }
}

/// Sets a single environment variable. Caller must hold `lock_env()`.
pub fn set(var: &str, value: &str) {
    // SAFETY: see `clear_all`.
    unsafe { std::env::set_var(var, value) };
}

/// Writes `contents` to a uniquely-named file under the OS temp directory
/// and returns its path. The caller is responsible for removing it.
pub fn write_temp_yaml(name: &str, contents: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "pulsus-config-test-{name}-{}-{:?}.yaml",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::write(&path, contents).expect("write temp yaml fixture");
    path
}
