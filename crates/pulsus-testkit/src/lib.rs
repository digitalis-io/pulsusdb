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
//!
//! # Naming a test database
//!
//! [`test_db`] is the single place a live suite's throwaway database name
//! is composed, so that several checkouts can share one ClickHouse server.
//! See its documentation, and the guard
//! `crates/pulsus-server/tests/live_db_naming.rs`, which enforces that no
//! suite composes such a name by hand.

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

// ---------------------------------------------------------------------
// Test database naming
// ---------------------------------------------------------------------

/// Prepended to every name [`test_db`] composes, so that two checkouts
/// running the same live suite against **one** ClickHouse server do not
/// pick the same throwaway database and `DROP DATABASE` each other's data
/// mid-run.
///
/// Unset — the default everywhere, including CI — the composed name is the
/// bare one the suite asked for, so behaviour is exactly what it was
/// before this variable existed.
pub const DATABASE_PREFIX_VAR: &str = "PULSUS_TEST_CH_DATABASE_PREFIX";

/// The longest database name ClickHouse will accept here. Its on-disk
/// path component is escaped, so the real ceiling depends on the
/// characters used; 200 is comfortably under it for the `[A-Za-z0-9_]`
/// names this function admits, and the point of the check is to turn "an
/// over-long prefix produces an opaque server-side error deep inside a
/// live suite" into a message that names the variable.
const MAX_DATABASE_NAME_LEN: usize = 200;

/// Composes the name of a throwaway test database: `name`, prefixed with
/// [`DATABASE_PREFIX_VAR`] when that variable is set.
///
/// ```text
/// PULSUS_TEST_CH_DATABASE_PREFIX unset  -> "pulsus_read_it_s1_single"
/// PULSUS_TEST_CH_DATABASE_PREFIX=wt3    -> "wt3_pulsus_read_it_s1_single"
/// ```
///
/// Every live suite composes its database name through here and nowhere
/// else; `crates/pulsus-server/tests/live_db_naming.rs` fails the build if
/// a suite writes one itself.
///
/// # Panics
///
/// The result is interpolated straight into `CREATE DATABASE {db}` /
/// `DROP DATABASE IF EXISTS {db}` by every caller, unquoted. So both parts
/// are checked rather than trusted: `name` must be a non-empty
/// `[A-Za-z0-9_]` word starting with a letter or `_`, the prefix must be
/// the same shape, and the composed name must not exceed
/// [`MAX_DATABASE_NAME_LEN`]. A bad value panics naming the offender —
/// during a test, which is the only place this crate is ever linked.
///
/// Two prefix values are handled without panicking, and both are
/// deliberate:
///
/// * **A blank prefix reads as unset.** `PULSUS_TEST_CH_DATABASE_PREFIX=`
///   composes the bare name rather than `_pulsus_…`, so unsetting the
///   variable and emptying it mean the same thing
///   (`an_empty_or_blank_prefix_reads_as_unset`).
/// * **Surrounding whitespace is trimmed.** `" wt3 "` and `"wt3"` compose
///   the same database, so trimming costs no isolation — that is the
///   whole justification, and no claim is made here about how such a
///   value would arise. (A previous revision of this comment blamed
///   `export …=$(cat .prefix)`; that is false — bash command substitution
///   strips trailing newlines, measured: `$(printf 'wt3\n')` yields the
///   bytes `77 74 33`. Recorded so the story is not reinvented.) The trim
///   is not dead code: a quoted assignment does carry whitespace into a
///   child's environment, measured — `C="wt3 "` reaches `env` as
///   `C=wt3 `.
///
/// Whitespace *inside* the prefix is not covered by either: `"wt 3"` and
/// `"wt\n3"` panic like any other bad character.
pub fn test_db(name: &str) -> String {
    test_ident(name)
}

/// [`test_db`]'s composition for the other server-side names a live test
/// creates inside a database it does **not** own — a table in `default`,
/// or the `query_id` it later looks up in `system.query_log`. Those
/// collide between two concurrent checkouts exactly as a database name
/// does, and they are prefixed by the same variable for the same reason.
///
/// # Panics
///
/// As [`test_db`].
pub fn test_ident(name: &str) -> String {
    let prefix = std::env::var(DATABASE_PREFIX_VAR).ok();
    compose_db_name(prefix.as_deref(), name)
}

/// [`test_db`]'s whole decision as a pure function of the prefix reading —
/// testable without mutating process environment (`std::env::set_var` is
/// `unsafe` in edition 2024 and racy under a threaded harness besides).
fn compose_db_name(prefix: Option<&str>, name: &str) -> String {
    if let Err(why) = check_identifier(name) {
        panic!(
            "test database name {name:?} is not usable unquoted in `CREATE DATABASE`: {why}. \
             Name it with ASCII letters, digits and underscores only."
        );
    }
    // Trim, and treat blank as unset — both deliberate, both justified in
    // [`test_db`]'s `# Panics` section. Surrounding whitespace cannot
    // change which database is named, so absorbing it costs no isolation.
    let composed = match prefix.map(str::trim).filter(|p| !p.is_empty()) {
        None => name.to_string(),
        Some(prefix) => {
            if let Err(why) = check_identifier(prefix) {
                panic!(
                    "{DATABASE_PREFIX_VAR}={prefix:?} is not a usable database-name prefix: \
                     {why}. Set it to a short ASCII word such as `wt3`."
                );
            }
            format!("{prefix}_{name}")
        }
    };
    assert!(
        composed.len() <= MAX_DATABASE_NAME_LEN,
        "composed test database name {composed:?} is {} characters, over the {MAX_DATABASE_NAME_LEN} \
         this project allows — shorten {DATABASE_PREFIX_VAR}",
        composed.len()
    );
    composed
}

/// A suite-wide test database name, composed once on first use.
///
/// The `const DB: &str = "pulsus_…_it";` that several suites used before
/// the prefix existed cannot survive as a `const`, because the name is now
/// a function of the environment. `static DB: TestDb = TestDb::new("…");`
/// replaces it without disturbing the interpolations: `TestDb` is
/// [`Display`](fmt::Display), so `format!("DROP DATABASE {DB}")` and
/// `DB.to_string()` keep working, and it derefs to `str`, so `&DB` passes
/// anywhere a `&str` was passed before.
#[derive(Debug)]
pub struct TestDb {
    base: &'static str,
    composed: std::sync::OnceLock<String>,
}

impl TestDb {
    /// Declares the suite's database. `base` is the unprefixed name; the
    /// prefix is read the first time [`TestDb::name`] is called, never at
    /// declaration time.
    pub const fn new(base: &'static str) -> Self {
        Self {
            base,
            composed: std::sync::OnceLock::new(),
        }
    }

    /// The composed name. Panics on the same inputs [`test_db`] does.
    pub fn name(&self) -> &str {
        self.composed.get_or_init(|| test_db(self.base))
    }
}

impl fmt::Display for TestDb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl std::ops::Deref for TestDb {
    type Target = str;

    fn deref(&self) -> &str {
        self.name()
    }
}

/// `Ok` iff `s` is a non-empty `[A-Za-z_][A-Za-z0-9_]*`. A leading digit
/// is refused as well as punctuation: ClickHouse needs such a name
/// backquoted, and every call site interpolates the result bare.
fn check_identifier(s: &str) -> Result<(), String> {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return Err("it is empty".to_string());
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(format!("it starts with {first:?}, not a letter or `_`"));
    }
    if let Some(bad) = chars.find(|c| !(c.is_ascii_alphanumeric() || *c == '_')) {
        return Err(format!("it contains {bad:?}"));
    }
    Ok(())
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

    // -----------------------------------------------------------------
    // Test database naming
    // -----------------------------------------------------------------

    /// The default has to be byte-for-byte what the suites used before the
    /// prefix existed, or every committed EXPLAIN/SQL expectation that
    /// interpolates the database name would have to move with it.
    #[test]
    fn no_prefix_leaves_the_name_exactly_as_the_suite_wrote_it() {
        assert_eq!(
            compose_db_name(None, "pulsus_read_it_s1_single"),
            "pulsus_read_it_s1_single"
        );
    }

    /// An empty or whitespace-only value reads as "unset": exporting
    /// `PULSUS_TEST_CH_DATABASE_PREFIX=` must not produce `_pulsus_…`.
    #[test]
    fn an_empty_or_blank_prefix_reads_as_unset() {
        assert_eq!(compose_db_name(Some(""), "pulsus_x_it"), "pulsus_x_it");
        assert_eq!(compose_db_name(Some("   "), "pulsus_x_it"), "pulsus_x_it");
    }

    #[test]
    fn a_prefix_is_joined_to_the_name_with_one_underscore() {
        assert_eq!(
            compose_db_name(Some("wt3"), "pulsus_x_it"),
            "wt3_pulsus_x_it"
        );
        assert_eq!(
            compose_db_name(Some("agent_b"), "pulsus_x_it"),
            "agent_b_pulsus_x_it"
        );
    }

    /// Surrounding whitespace is trimmed rather than refused: `" wt3 "`
    /// and `"wt3"` name the same database, so accepting both isolates two
    /// checkouts exactly as well. Deliberate — see the note in
    /// `test_db`'s docs, which also records the *false* justification a
    /// previous revision gave for it.
    #[test]
    fn a_prefix_is_trimmed_before_use_rather_than_refused() {
        for spelling in [" wt3 ", "wt3\n", "\twt3\r\n", "wt3 "] {
            assert_eq!(
                compose_db_name(Some(spelling), "pulsus_x_it"),
                "wt3_pulsus_x_it",
                "{spelling:?} must compose the same database as \"wt3\""
            );
        }
    }

    /// Trimming reaches the ends and nothing else: whitespace *inside* a
    /// prefix would change the composed name, so it panics like any other
    /// bad character. One case per whitespace character that
    /// [`str::trim`] would have removed at an end, so the two behaviours
    /// cannot silently converge.
    #[test]
    fn whitespace_inside_a_prefix_is_still_refused() {
        for spelling in ["wt 3", "wt\t3", "wt\n3", "wt\r\n3"] {
            let err = std::panic::catch_unwind(|| compose_db_name(Some(spelling), "pulsus_x_it"))
                .expect_err("whitespace inside a prefix must panic");
            let msg = err
                .downcast_ref::<String>()
                .map(String::as_str)
                .unwrap_or("<non-string panic payload>");
            assert!(
                msg.contains("is not a usable database-name prefix"),
                "{spelling:?}: {msg}"
            );
        }
    }

    /// Two different prefixes never compose to the same database — the
    /// whole point of the variable.
    #[test]
    fn two_prefixes_never_collide_on_one_suite_name() {
        assert_ne!(
            compose_db_name(Some("a"), "pulsus_x_it"),
            compose_db_name(Some("b"), "pulsus_x_it")
        );
    }

    #[test]
    #[should_panic(expected = "is not a usable database-name prefix")]
    fn a_prefix_with_punctuation_is_refused() {
        // `DROP DATABASE x; DROP DATABASE pulsus_x_it` would otherwise be
        // two statements.
        let _ = compose_db_name(Some("x; DROP DATABASE y"), "pulsus_x_it");
    }

    #[test]
    #[should_panic(expected = "is not a usable database-name prefix")]
    fn a_prefix_starting_with_a_digit_is_refused() {
        let _ = compose_db_name(Some("3wt"), "pulsus_x_it");
    }

    #[test]
    #[should_panic(expected = "not usable unquoted")]
    fn a_name_with_punctuation_is_refused() {
        let _ = compose_db_name(None, "pulsus-x-it");
    }

    #[test]
    #[should_panic(expected = "not usable unquoted")]
    fn an_empty_name_is_refused() {
        let _ = compose_db_name(Some("wt3"), "");
    }

    /// A name left un-substituted by a missing `format!` argument (`{}`)
    /// must not reach ClickHouse as a database name.
    #[test]
    #[should_panic(expected = "not usable unquoted")]
    fn an_unsubstituted_format_placeholder_is_refused() {
        let _ = compose_db_name(None, "pulsus_x_it_{nonce}");
    }

    #[test]
    #[should_panic(expected = "over the 200")]
    fn an_over_long_composed_name_is_refused() {
        let _ = compose_db_name(Some(&"p".repeat(190)), "pulsus_x_it");
    }

    /// `TestDb` has to interpolate and coerce exactly like the `&str`
    /// constant it replaces, or migrating a suite to it would mean
    /// rewriting every `format!("… {DB} …")` in the file.
    #[test]
    fn a_test_db_interpolates_and_derefs_like_the_str_constant_it_replaces() {
        static DB: TestDb = TestDb::new("pulsus_x_it");
        fn takes_str(s: &str) -> usize {
            s.len()
        }
        let expected = test_db("pulsus_x_it");
        assert_eq!(
            format!("DROP DATABASE {DB}"),
            format!("DROP DATABASE {expected}")
        );
        assert_eq!(DB.to_string(), expected);
        assert_eq!(DB.name(), expected);
        assert_eq!(takes_str(&DB), expected.len());
    }

    /// The name is composed once and then stable, so a suite that
    /// interpolates it in twenty places names one database.
    #[test]
    fn a_test_db_composes_its_name_once() {
        static DB: TestDb = TestDb::new("pulsus_y_it");
        assert_eq!(DB.name(), DB.name());
        assert!(std::ptr::eq(DB.name(), DB.name()));
    }

    /// The public entry point reads the documented variable. Asserted
    /// without mutating the environment: whatever the ambient value is,
    /// `test_db` must agree with `compose_db_name` on it.
    #[test]
    fn test_db_reads_the_documented_variable() {
        let ambient = std::env::var(DATABASE_PREFIX_VAR).ok();
        assert_eq!(
            test_db("pulsus_x_it"),
            compose_db_name(ambient.as_deref(), "pulsus_x_it")
        );
    }
}
