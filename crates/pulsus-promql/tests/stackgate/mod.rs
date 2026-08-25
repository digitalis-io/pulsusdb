//! Paired pinned-stack harness for the PromQL depth cap (issue #262).
//!
//! A module under `tests/<dir>/` is not itself a test target; it is
//! `#[path]`-included by the binaries that need it (repo precedent:
//! `crates/pulsus-server/tests/support/manifest.rs`,
//! `crates/pulsus-promql/tests/promqltest/mod.rs`).
//!
//! **The sentence each pair asserts, in full and nothing wider:** *this
//! shape, at depth `MAX_EXPR_DEPTH`, parses, plans and evaluates on an
//! `S`-byte stack, and `(S, 4000)` is inside the overflow regime for the
//! same shape.* The control is what keeps the positive leg from passing
//! because frames got smaller rather than because the cap holds.
//!
//! This is the **generic** half of `crates/pulsus-logql/tests/stackgate/
//! mod.rs:1-95`, copied rather than shared. A cross-crate
//! `#[path = "../../pulsus-logql/tests/…"]` would make a `pulsus-promql`
//! test fail when an unrelated crate's test tree moves; ninety lines of
//! process-spawning helper is the cheaper coupling.

#![allow(dead_code)]

use std::process::Command;

/// Child-mode dispatch env var. When set, the child entry point runs the
/// named mode and every parent test returns early so the child never
/// re-forks.
pub const CHILD_ENV: &str = "PULSUS_PROMQL_DEPTH_STACK_CHILD";

/// The suite's own gate. Unset, every test in the binary skips cleanly:
/// the control legs deliberately abort the process, so this suite is
/// nightly/dispatch-only and never rides a PR.
///
/// Set it to exactly `1` — that is `pulsus_testkit::live_gate`'s
/// contract, not a local convention.
pub const GATE_ENV: &str = "PULSUS_PROMQL_DEPTH_STACK";

pub fn child_mode() -> Option<String> {
    std::env::var(CHILD_ENV).ok()
}

/// `true` when the pinned-stack legs should run.
///
/// Routed through `pulsus_testkit` rather than reading the variable
/// directly, and the difference is the whole point: an absent gate on a
/// laptop or in the hermetic `ci` lane is a clean skip, but an absent
/// gate in the `promql-depth-stack-release` job — which exists to run
/// exactly this — **panics** instead of reporting green having executed
/// nothing. This suite is by its own doc the only gate that measures the
/// shipped configuration, and it already never runs on a PR; a silent
/// green is the one failure it cannot afford. `HERMETIC_CI_JOBS` is
/// `["ci"]` and the discriminator is fail-closed, so every other job id
/// is treated as live (issue #320).
pub fn gate_is_open() -> bool {
    pulsus_testkit::live_gate_enabled(GATE_ENV)
}

/// Runs `f` on a thread whose stack is pinned to exactly `bytes`. An
/// overflow of that stack aborts the whole process — which is why every
/// control leg runs in an out-of-process child.
pub fn on_stack(bytes: usize, f: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(bytes)
        .spawn(f)
        .expect("spawn pinned-stack thread")
        .join()
        .expect("pinned-stack thread panicked");
}

fn spawn_child(child_test: &str, mode: &str) -> std::process::Output {
    let exe = std::env::current_exe().expect("current_exe");
    Command::new(exe)
        .args([child_test, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, mode)
        .output()
        .expect("spawn child test process")
}

/// The positive leg: the shape at the cap's own boundary completes on the
/// pinned stack.
///
/// Also asserts the child reported **exactly one test run**, so a
/// mis-typed name filter cannot silently run the whole suite (or nothing)
/// in the child and pass.
pub fn assert_child_ok(child_test: &str, mode: &str) {
    let out = spawn_child(child_test, mode);
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success(),
        "child mode {mode:?} failed with {}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        out.status,
    );
    assert!(
        stdout.contains("1 passed"),
        "child mode {mode:?} did not run exactly one test\n--- stdout ---\n{stdout}"
    );
}

/// The control leg: the SAME shape, well past the cap, must **overflow**
/// on the same pinned stack. This is a **non-vacuity** claim — it proves
/// `(S, 4000)` is inside the overflow regime — not a frame-size claim.
pub fn assert_child_overflowed(child_test: &str, mode: &str) {
    let out = spawn_child(child_test, mode);
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        !out.status.success(),
        "control mode {mode:?} was expected to overflow but exited successfully\n{stderr}"
    );
    assert!(
        stderr.contains("stack overflow"),
        "control mode {mode:?} failed without a stack overflow: {}\n{stderr}",
        out.status
    );
}
