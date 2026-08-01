//! Shared test support for PulsusDB's environment-gated ("live") suites.
//!
//! # The problem this crate exists to solve (issue #320)
//!
//! Every suite that needs a real ClickHouse is admitted by an environment
//! variable — `PULSUS_TEST_CLICKHOUSE=1` — and returns early when it is
//! absent. On a developer laptop with no container that is exactly right.
//! In CI it is not: there, an absent variable means the workflow step lost
//! its `env:` block, and the suite reports **green without running
//! anything**. This project has already shipped one such step (#272's
//! provenance check) that never executed.
//!
//! [`live_gate`] separates the two readings, and [`require_live_gate`]
//! turns the CI one into a panic.
//!
//! # Why `GITHUB_JOB` and not `CI`
//!
//! `CI=true` is set for *every* GitHub Actions job, including the hermetic
//! `cargo test --workspace` lane, which legitimately runs these binaries
//! with no container anywhere. Keying on `CI` would redden every push.
//! `GITHUB_JOB` carries the job id, which distinguishes the hermetic lane
//! ([`HERMETIC_CI_JOBS`]) from a job that exists to stand up ClickHouse.
//!
//! The discriminator is deliberately **fail-closed**: any job id not in
//! [`HERMETIC_CI_JOBS`] is treated as a live job. If the hermetic job is
//! ever renamed, the hermetic lane starts failing *loudly* instead of
//! quietly excusing a live job — a wrong guess about which job we are in
//! can only ever produce noise, never a silent pass.
//!
//! # How a suite uses this
//!
//! ```no_run
//! # fn body() {}
//! fn should_run() -> bool {
//!     pulsus_testkit::live_clickhouse_enabled()
//! }
//!
//! #[test]
//! fn a_live_test() {
//!     if !should_run() {
//!         return;
//!     }
//!     body();
//! }
//!
//! #[test]
//! fn a_hermetic_test_in_the_same_binary() {
//!     // Reaches no gate of its own, so it calls the guard directly:
//!     // otherwise `--test <suite> hermetic` would still exit 0 in a live
//!     // CI job with the gate missing.
//!     pulsus_testkit::require_live_gate(pulsus_testkit::CLICKHOUSE_GATE);
//!     body();
//! }
//! ```
//!
//! # Exactly what this covers — and, precisely, what it does not
//!
//! The guard is a **per-test** hook, so the claim it supports is bounded
//! to paths that *execute a test body*:
//!
//! > In a live CI job with the gate absent, every test **that runs** in a
//! > migrated binary panics. An invocation therefore exits non-zero if it
//! > executes at least one such test **and that test lets the panic
//! > escape** — i.e. it is not `#[should_panic]` and does not wrap the
//! > guard in `catch_unwind`.
//!
//! The qualification is not decoration: a `#[should_panic]` test absorbs
//! the guard and reports `ok`, so without it the sentence would be false
//! (measured on #320 with a temporary such test). No gated suite absorbs
//! it today; nothing here prevents one being added.
//!
//! What remains is still stronger than an entry-point check — filtered
//! invocations and `--skip`ped ones are covered, and #309 measured a first
//! attempt that guarded only its live test and let `--skip <live test>`
//! exit `0`. It is **not** "every reachable path is noisy", which no
//! per-test hook can deliver. The uncovered paths, named rather than
//! implied:
//!
//! * **`--list`** — nextest's list phase, and `cargo test -- --list`, run
//!   no test body. The guard never fires.
//! * **A filter that matches no test** — nothing executes. `cargo test`
//!   exits `0`; nextest exits `4` (`no tests to run`).
//! * **Skip-all** — a `--skip` set covering every test in the binary is
//!   the same case as the one above.
//! * **`#[should_panic]` and `catch_unwind`** — the exclusion carried in
//!   the sentence above. Measured on #320: a live-job-no-gate run of all
//!   54 binaries produced zero tests reporting `ok`, so no suite absorbs
//!   the guard today.
//!
//! The only mechanism that would close the first three is a per-process
//! hook, i.e. `harness = false` — and nextest cannot list a non-harness
//! binary (measured on #309: `creating test list failed`), which would
//! break the hermetic lane this guard exists to keep honest. Carried as a
//! documented residual, deliberately not re-litigated.
//!
//! # What this crate does not police at all
//!
//! A gated suite with **no CI step** never runs anywhere, so no per-test
//! guard can notice it. Two exist today (`pulsus-clickhouse/live_tls`,
//! `pulsus-read/nestedset_value_differential`); the checked
//! suite-to-CI-step inventory that would close that class is issue #323.

use std::fmt;

/// The gate on the ClickHouse-backed suites.
pub const CLICKHOUSE_GATE: &str = "PULSUS_TEST_CLICKHOUSE";

/// The gate on the suites that additionally need a TLS-enabled ClickHouse.
pub const CLICKHOUSE_TLS_GATE: &str = "PULSUS_TEST_CLICKHOUSE_TLS";

/// GitHub Actions job ids that may execute a gated test binary with no
/// gate set. Only the hermetic lane qualifies: it runs
/// `cargo test --workspace` with no container, so the gated suites are
/// *meant* to skip there.
///
/// Adding an entry here is how a new hermetic job announces itself. Every
/// other job id is treated as live — see the fail-closed note in the
/// module docs.
pub const HERMETIC_CI_JOBS: &[&str] = &["ci"];

/// The environment variable GitHub Actions sets to the running job's id.
const GITHUB_JOB: &str = "GITHUB_JOB";

/// Why this run is, or is not, allowed to skip a gated suite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveGate {
    /// The gate is set: run against the container.
    Run,
    /// No gate, and nothing claims there should be one — a developer
    /// machine, or the hermetic CI lane. Skip cleanly.
    SkipUngated,
    /// No gate, but this is a CI job that exists to provide the container.
    /// Skipping here is the silent failure this variant exists to prevent.
    MissingInLiveCiJob {
        /// The `GITHUB_JOB` id that was observed.
        job: String,
        /// The gate variable that should have been set.
        var: String,
    },
}

impl LiveGate {
    /// `true` only for [`LiveGate::Run`].
    pub fn is_running(&self) -> bool {
        matches!(self, LiveGate::Run)
    }
}

impl fmt::Display for LiveGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LiveGate::Run => write!(f, "gate set: running against a live ClickHouse"),
            LiveGate::SkipUngated => write!(f, "gate absent and not in a live CI job: skipping"),
            LiveGate::MissingInLiveCiJob { job, var } => {
                write!(f, "{var} absent in live CI job {job:?}")
            }
        }
    }
}

/// The classification, as a pure function of the two environment readings
/// — the whole decision, testable without mutating process environment
/// (`std::env::set_var` is `unsafe` in edition 2024 and racy under a
/// threaded harness besides).
fn classify(var: &str, gate: Option<&str>, github_job: Option<&str>) -> LiveGate {
    if gate == Some("1") {
        return LiveGate::Run;
    }
    match github_job {
        Some(job) if !HERMETIC_CI_JOBS.contains(&job) => LiveGate::MissingInLiveCiJob {
            job: job.to_string(),
            var: var.to_string(),
        },
        _ => LiveGate::SkipUngated,
    }
}

/// Classify the current process against gate variable `var`.
pub fn live_gate(var: &str) -> LiveGate {
    let gate = std::env::var(var).ok();
    let job = std::env::var(GITHUB_JOB).ok();
    classify(var, gate.as_deref(), job.as_deref())
}

/// The running test binary's name, for the panic message — the operator
/// needs to know *which* workflow step lost its `env:` block, and deriving
/// it from `current_exe` means no suite has to hand-write its own name.
///
/// `target/debug/deps/live_schema-1a2b3c4d` -> `live_schema`.
fn suite_name() -> String {
    let unknown = || "<unknown test binary>".to_string();
    let Ok(exe) = std::env::current_exe() else {
        return unknown();
    };
    let Some(stem) = exe.file_stem().and_then(|s| s.to_str()) else {
        return unknown();
    };
    match stem.rsplit_once('-') {
        Some((name, hash)) if !name.is_empty() && hash.chars().all(|c| c.is_ascii_hexdigit()) => {
            name.to_string()
        }
        _ => stem.to_string(),
    }
}

/// Panic when the gate is absent in a live CI job.
///
/// Call this **first in every test** of a gated binary, including the
/// hermetic ones — see the coverage note in the module docs for why a
/// per-binary check is not enough, and for the paths a per-test hook
/// cannot reach. Suites whose tests all route through
/// [`live_gate_enabled`]/[`live_clickhouse_enabled`] get it for free.
///
/// Do not wrap a call to this in `catch_unwind`, and do not put it in a
/// `#[should_panic]` test: either absorbs the panic and turns the guard
/// back into a silent pass.
///
/// Returns the classification so a caller can reuse it.
pub fn require_live_gate(var: &str) -> LiveGate {
    let gate = live_gate(var);
    if let LiveGate::MissingInLiveCiJob { job, .. } = &gate {
        panic!(
            "{var} is not set, but this is CI job {job:?}, which exists to provide ClickHouse — \
             the `{}` suite would have skipped silently and reported green. Restore the `env:` \
             block on this suite's step in .github/workflows/ci.yml, or add {job:?} to \
             pulsus_testkit::HERMETIC_CI_JOBS if it is genuinely a hermetic lane (issue #320).",
            suite_name()
        );
    }
    gate
}

/// `true` when the gated half of a suite should execute; panics when the
/// gate is missing in a live CI job.
pub fn live_gate_enabled(var: &str) -> bool {
    require_live_gate(var).is_running()
}

/// [`live_gate_enabled`] for the common [`CLICKHOUSE_GATE`].
pub fn live_clickhouse_enabled() -> bool {
    live_gate_enabled(CLICKHOUSE_GATE)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VAR: &str = CLICKHOUSE_GATE;

    #[test]
    fn the_gate_being_set_runs_regardless_of_the_job() {
        assert_eq!(classify(VAR, Some("1"), None), LiveGate::Run);
        assert_eq!(classify(VAR, Some("1"), Some("ci")), LiveGate::Run);
        assert_eq!(classify(VAR, Some("1"), Some("schema-it")), LiveGate::Run);
    }

    #[test]
    fn a_developer_machine_with_no_gate_skips_cleanly() {
        assert_eq!(classify(VAR, None, None), LiveGate::SkipUngated);
    }

    #[test]
    fn the_hermetic_ci_lane_with_no_gate_skips_cleanly() {
        assert_eq!(classify(VAR, None, Some("ci")), LiveGate::SkipUngated);
    }

    #[test]
    fn a_live_ci_job_with_no_gate_is_a_wiring_failure() {
        assert_eq!(
            classify(VAR, None, Some("schema-it")),
            LiveGate::MissingInLiveCiJob {
                job: "schema-it".to_string(),
                var: VAR.to_string(),
            }
        );
    }

    /// Fail-closed: an unrecognised job id is a live job, so renaming the
    /// hermetic lane reddens it loudly instead of excusing a live one.
    #[test]
    fn an_unrecognised_job_id_is_treated_as_live_not_hermetic() {
        assert!(matches!(
            classify(VAR, None, Some("ci-renamed")),
            LiveGate::MissingInLiveCiJob { .. }
        ));
    }

    /// The gate is `=1`, not "present": `PULSUS_TEST_CLICKHOUSE=0` must
    /// not admit a suite, and in a live job it is still a wiring failure.
    #[test]
    fn a_non_one_gate_value_does_not_admit_the_suite() {
        assert_eq!(classify(VAR, Some("0"), None), LiveGate::SkipUngated);
        assert!(matches!(
            classify(VAR, Some(""), Some("schema-it")),
            LiveGate::MissingInLiveCiJob { .. }
        ));
    }

    /// The message names the variable that was missing, so a suite gated
    /// on the TLS variant does not send the reader to the wrong step.
    #[test]
    fn the_classification_carries_the_variable_that_was_missing() {
        let gate = classify(CLICKHOUSE_TLS_GATE, None, Some("schema-it"));
        assert_eq!(
            gate,
            LiveGate::MissingInLiveCiJob {
                job: "schema-it".to_string(),
                var: CLICKHOUSE_TLS_GATE.to_string(),
            }
        );
        assert!(gate.to_string().contains(CLICKHOUSE_TLS_GATE));
    }

    #[test]
    fn only_run_reports_as_running() {
        assert!(LiveGate::Run.is_running());
        assert!(!LiveGate::SkipUngated.is_running());
        assert!(
            !LiveGate::MissingInLiveCiJob {
                job: "schema-it".to_string(),
                var: VAR.to_string(),
            }
            .is_running()
        );
    }

    /// The panic message has to name a suite, and it is derived from
    /// `current_exe` rather than hand-written 50-odd times.
    #[test]
    fn the_suite_name_strips_cargos_binary_hash_suffix() {
        assert!(!suite_name().is_empty());
        assert!(!suite_name().contains('/'));
    }
}
