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
//! guard can notice it. That class is closed by issue #323, in
//! `tests/gated_suite_inventory.rs`: every gated suite must either have a
//! `--test <name>` step or be named in a committed `DELIBERATELY_UNWIRED`
//! list with the reason it cannot have one — and an entry whose suite has
//! since been wired up fails as stale, so the list cannot only grow.
//! Which absences are deliberate is not decided there; it is forced into
//! a reason string, which is what "deliberate versus forgotten" means
//! mechanically.
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

/// The trace id the matched-span PROJECTION differential
/// (`crates/pulsus-read/tests/traces_search_projection_differential.rs`,
/// issue #479) pushes into its own reference instance.
///
/// It lives here, and not in that suite, because a SECOND suite has to be
/// able to recognise the residue: that corpus enters a `compare()` top-N
/// and the resulting failure surfaces in the other suite as an ordinary
/// value mismatch against the reference. `compare_value_differential`
/// refuses to run against an instance holding this trace, and naming the
/// constant once is what stops the two suites drifting to different ids.
pub const PROJECTION_DIFFERENTIAL_TRACE_HEX: &str = "a4790000000000000000000000000001";

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

/// What a gate variable's VALUE has to look like for the gate to count as
/// set. The two kinds exist because two kinds of gate exist, and one
/// rule cannot serve both (issue #458 review round 3).
///
/// The distinction is not cosmetic: widening the single rule to
/// "non-empty" so that a URL would pass would silently reclassify
/// `PULSUS_TEST_CLICKHOUSE=0` from a skip into a run, across every suite
/// in the workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GateValue {
    /// `=1` and nothing else. `0`, `true`, `yes` and the empty string are
    /// all "not set". Every gate in this repo but one is this kind.
    Boolean,
    /// Any non-blank value. For a gate whose value IS the thing the suite
    /// needs — an endpoint URL — there is no boolean spelling to demand,
    /// and demanding one would mean carrying a second variable whose only
    /// job is to say "the first one is set", which can then disagree with
    /// it.
    Endpoint,
}

impl GateValue {
    fn is_set(self, gate: Option<&str>) -> bool {
        match (self, gate) {
            (GateValue::Boolean, g) => g == Some("1"),
            (GateValue::Endpoint, Some(v)) => !v.trim().is_empty(),
            (GateValue::Endpoint, None) => false,
        }
    }
}

/// The classification, as a pure function of the two environment readings
/// — the whole decision, testable without mutating process environment
/// (`std::env::set_var` is `unsafe` in edition 2024 and racy under a
/// threaded harness besides).
///
/// The `kind` parameter changes ONLY which values count as set; the
/// job-discriminator half below is shared, which is the point of putting
/// them in one function rather than two. Adding [`GateValue::Endpoint`]
/// therefore cannot alter any existing caller's behaviour: the entry
/// points they use still pass [`GateValue::Boolean`], and that arm's
/// predicate is the same `gate == Some("1")` it always was.
fn classify_kind(
    kind: GateValue,
    var: &str,
    gate: Option<&str>,
    github_job: Option<&str>,
) -> LiveGate {
    if kind.is_set(gate) {
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

/// The boolean classification — unchanged, and the one every `=1` gate in
/// the workspace takes.
fn classify(var: &str, gate: Option<&str>, github_job: Option<&str>) -> LiveGate {
    classify_kind(GateValue::Boolean, var, gate, github_job)
}

/// Classify the current process against boolean gate variable `var`.
pub fn live_gate(var: &str) -> LiveGate {
    let gate = std::env::var(var).ok();
    let job = std::env::var(GITHUB_JOB).ok();
    classify(var, gate.as_deref(), job.as_deref())
}

/// Classify the current process against an ENDPOINT gate variable — one
/// whose value is the address the suite talks to (e.g.
/// `PULSUSDB_TEMPO_DIFF_URL=http://localhost:13200`) rather than a `1`.
///
/// Same fail-closed guarantee as [`live_gate`], and the same
/// `HERMETIC_CI_JOBS` discriminator: an absent endpoint in a live CI job
/// is a wiring failure, not a skip (issue #320). Only the "is it set"
/// test differs, because a URL is not a `1`.
pub fn live_endpoint_gate(var: &str) -> LiveGate {
    let gate = std::env::var(var).ok();
    let job = std::env::var(GITHUB_JOB).ok();
    classify_kind(GateValue::Endpoint, var, gate.as_deref(), job.as_deref())
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
    panic_if_missing(var, live_gate(var))
}

/// [`require_live_gate`] for an ENDPOINT gate — one whose value is the
/// address the suite talks to rather than a `1`. Same guarantee, same
/// message; only "is it set" differs. See [`live_endpoint_gate`].
///
/// Routing a URL-valued gate through [`require_live_gate`] instead is the
/// defect this exists to stop, and it is not a quiet one: the boolean
/// rule reads the URL as "not `1`", so the guard panics saying the
/// variable is not set while the `env:` block is right there in the same
/// log (issue #458 review round 3, `schema-it` red).
pub fn require_live_endpoint_gate(var: &str) -> LiveGate {
    panic_if_missing(var, live_endpoint_gate(var))
}

/// The shared fail-closed panic. One message for both kinds, so the two
/// entry points cannot drift into saying different things about the same
/// wiring failure.
fn panic_if_missing(var: &str, gate: LiveGate) -> LiveGate {
    if let LiveGate::MissingInLiveCiJob { job, .. } = &gate {
        panic!(
            "{var} is not set, but this is CI job {job:?}, which exists to provide the live \
             dependency — the `{}` suite would have skipped silently and reported green. \
             Restore the `env:` block on this suite's step in .github/workflows/ci.yml, or add \
             {job:?} to pulsus_testkit::HERMETIC_CI_JOBS if it is genuinely a hermetic lane \
             (issue #320).",
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

/// **The one endpoint read.** `Some(value)` when endpoint gate `var` is
/// set, `None` when the suite should skip — and a panic, before anything
/// is returned, when `var` is absent inside a CI job that exists to supply
/// it (see [`require_live_endpoint_gate`]).
///
/// ## Why this exists rather than `env::var` behind a guard
///
/// Every reference-facing differential suite needs the same two things:
/// the endpoint's value, and the fail-closed decision about its absence.
/// Written by hand that is
///
/// ```text
/// let Ok(base) = std::env::var("PULSUSDB_X_URL") else { return };   // fail-OPEN
/// ```
///
/// which reports a pass having compared nothing, and the corrected form is
/// three lines that have to be repeated identically at every site. Issue
/// #458 recorded the hole, #492 part 3 and #523 closed it on three suites
/// one at a time, and a review of #523 then found sixteen more carrying
/// it. Sixteen copies of a three-line correction is how this repository
/// acquired its other duplicated decisions, so the read and the decision
/// live here together and every suite makes one call.
///
/// ## What enforces "every suite", and how far that reaches
///
/// Two hermetic checks in `crates/pulsus-testkit/tests/gated_suite_inventory.rs`,
/// both in the workspace lane on every push. Their scope is `.rs` files
/// under `crates/*/tests`, recursively, so `tests/common/` modules count.
///
/// * `no_test_source_reads_an_endpoint_variable_directly` — no file may
///   read a `PULSUSDB_*` variable with the name written inside the
///   `env::var`/`env::var_os` call.
/// * `every_endpoint_name_written_in_a_test_source_is_routed_through_a_gate`
///   — a `PULSUSDB_*` name written out as a complete string literal
///   anywhere in a file must also be handed, in that file, to one of this
///   crate's gate entry points. This is the one that reaches an
///   indirection: `const V: &str = "PULSUSDB_X_URL"; env::var(V)` defeats
///   the first check and is caught by the second, because the constant's
///   DEFINITION is a literal. Measured on the whole binary:
///   `5 tests run: 4 passed, 1 failed`, exit 100 — and the `4` is the
///   load-bearing figure, because it says the first check stayed green.
///
/// # What the pair does not reach
///
/// Stated as ONE rule with measured instances, rather than a list that
/// grows an entry per review. Property (5) can see exactly one thing:
///
/// > a COMPLETE `PULSUSDB_*` name, spelled as a string literal, in the
/// > same file that reads it, with every character after the prefix an
/// > ASCII capital, digit or underscore.
///
/// Anything that stops the name being that is invisible to it. Each of
/// these was run against the whole inventory binary and left every test
/// green — `5 tests run: 5 passed, 0 skipped`, exit 0:
///
/// * assembled at run time — `["PULSUS", "DB_X_URL"].concat()`, `format!`;
/// * assembled at compile time — `concat!`;
/// * produced by a macro expansion — `stringify!(PULSUSDB_X_URL)` inside
///   a `macro_rules!` (issue #523 review round 3);
/// * defined outside `crates/*/tests` and imported;
/// * not all upper case — `const V: &str = "PULSUSDB_X_Url";`. Note the
///   halves differ here: the same name read DIRECTLY is still caught, by
///   property (4), which puts no case constraint on the name
///   (`5 tests run: 4 passed, 1 failed`, exit 100).
///
/// Two more, of a different kind:
///
/// * a file that keeps its routed call and ALSO reads the same name some
///   other way.
///
/// **Line comments are skipped, in both directions** (issue #523 review
/// round 4). Until that round they were scanned like code, and that was
/// wrong twice over: a comment showing the recommended form excused a real
/// unrouted read (`5 tests run: 5 passed`, exit 0), and a name written
/// ONLY in a comment, in a file with no read and no routed call, made (5)
/// fail (`5 tests run: 4 passed, 1 failed`, exit 100). An earlier revision
/// of this paragraph claimed the second could not happen; it was false,
/// and the second is the one that matters, because naming these variables
/// in a comment is the house style — **69 comment mentions across 30
/// files**, all in backticks. One editor writing `"PULSUSDB_X_URL"`
/// instead would have reddened the build for nothing, and the repair a
/// person reaches for then is an exemption.
///
/// Skipping them changed no verdict here: of the 46 complete name
/// literals in scope, 0 sat in a comment position. What remains, stated
/// because this is a line-scan and not a lexer: a `//` inside a string
/// earlier on the same line hides the rest of that line (a MISS, the safe
/// direction; 0 such lines carry a name today, and a probe of that shape
/// is not reported — `5 tests run: 5 passed`, exit 0), and a `/* … */`
/// block comment still counts as code both ways (0 in scope today; the
/// same probe inside `/* … */` IS reported, `5 tests run: 4 passed,
/// 1 failed`, exit 100).
///
/// And the scope, which is not a weakness but is part of the claim: only
/// `PULSUSDB_`-prefixed names, only `.rs` under `crates/*/tests`. `xtask/`
/// and `e2e/` name no `PULSUSDB_*` variable at all today, measured with
/// `git grep -n 'PULSUSDB_' -- e2e xtask`.
///
/// So the claim, exactly: **closed against every form a person writes
/// without meaning to bypass the check, and open to a deliberate one.**
/// A `macro_rules!` that stringifies an endpoint name, like
/// `std::mem::forget` on a database guard, is not something anyone
/// reaches for by accident, and following either would mean a parser.
///
/// ## The value is returned exactly as the environment holds it
///
/// No trimming. A gate counts as set when its value is non-blank
/// ([`GateValue::Endpoint`]), so `" "` skips, but `" http://x "` runs and
/// yields the padded string — the same value a bare `env::var` yielded
/// before this helper existed. Changing that here would change what every
/// converted suite sends.
pub fn live_endpoint(var: &str) -> Option<String> {
    if require_live_endpoint_gate(var).is_running() {
        // `Run` is only reachable through `GateValue::Endpoint::is_set`,
        // which already matched `Some(v)` with a non-blank `v`.
        Some(std::env::var(var).expect("the gate classified this variable as set"))
    } else {
        None
    }
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
/// characters used; the value below is comfortably under it for the
/// `[A-Za-z0-9_]` names this function admits, and the point of the check
/// is to turn "an over-long prefix produces an opaque server-side error
/// deep inside a live suite" into a message that names the variable.
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
/// Some prefix values are accepted rather than refused. Each is
/// deliberate:
///
/// * **A blank prefix reads as unset.** `PULSUS_TEST_CH_DATABASE_PREFIX=`
///   composes the bare name rather than `_pulsus_…`, so unsetting the
///   variable and emptying it mean the same thing
///   (`an_empty_or_blank_prefix_reads_as_unset`).
/// * **Surrounding whitespace is trimmed.** `" wt3 "` and `"wt3"` compose
///   the same database, so trimming costs no isolation — that is the
///   whole justification. (A previous revision of this comment blamed
///   `export …=$(cat .prefix)`; that is false — bash command substitution
///   strips trailing newlines, measured: `$(printf 'wt3\n')` yields the
///   bytes `77 74 33`. Recorded so the story is not reinvented.) The trim
///   is not dead code: a quoted assignment does carry whitespace into a
///   child's environment, measured — `C="wt3 "` reaches `env` as
///   `C=wt3 `.
///
/// "Whitespace" here means whatever [`str::trim`] strips, whatever that
/// is in the toolchain you are building with — the tests do not enumerate
/// it, they derive it by running `trim` over every Unicode scalar value.
/// Both halves are checked across that whole derived set: each such
/// character is absorbed at an end
/// (`every_character_trim_strips_is_absorbed_at_a_prefix_end`) and
/// refused *inside* a prefix, where it would change the composed name
/// (`every_character_trim_strips_is_refused_inside_a_prefix`). So this
/// paragraph cannot go stale when Rust's definition of whitespace grows.
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

// ---------------------------------------------------------------------------
// The live-poll settle rule (issue #460, lifted from issue #458's fix)
// ---------------------------------------------------------------------------

/// Consecutive IDENTICAL non-empty reads required before a live poll is
/// considered settled.
///
/// Three reads two seconds apart span more than one Tempo block-cut
/// period (`max_block_duration: 2s`, `flush_check_period: 1s` in
/// `ci/tempo/tempo-compare.yaml`), so a batch that is still landing shows
/// a different payload in at least one of them. **Two is not enough**: a
/// batch can pause mid-flush for one interval, which is why the rule is
/// three and why `StabilityWait`'s own test scripts that case.
///
/// ONE definition, so two suites cannot drift apart on it.
pub const STABLE_READS: usize = 3;

/// The stability rule for a live poll: an answer is settled only once the
/// SAME payload has been observed [`STABLE_READS`] times running.
///
/// **Not the first non-empty response.** A reference's live store cuts
/// blocks on a timer, so an already-finalised time bucket can be read
/// before every span in it has flushed — a partial view that reddened
/// `schema-it` four times over a month (CI runs 30258308527,
/// 30610626381, 31510222855, 33110647702, every one on attempt 1 with an
/// identical five-cell divergence). Two of the four (30610626381,
/// 33110647702) were re-run and passed on attempt 2 with nothing changed;
/// the other two were never re-run. Partial views are attested by all
/// four; the re-run behaviour by two.
///
/// A `None` read — a failed poll OR one the caller judged empty —
/// neither settles a run nor resets one in progress.
///
/// **The emptiness decision belongs to the CALLER**, not to this type: it
/// is domain knowledge (an empty count map is not an answer; an empty
/// list might be), so a caller passes
/// `read.filter(|payload| !payload.is_empty())`.
#[derive(Debug)]
pub struct StabilityWait<T> {
    last: Option<T>,
    run: usize,
}

// Hand-written rather than derived: `#[derive(Default)]` would require
// `T: Default`, which a payload type has no reason to satisfy (and which
// would bound `settle_by` for nothing — an empty wait holds no payload).
impl<T> Default for StabilityWait<T> {
    fn default() -> Self {
        Self { last: None, run: 0 }
    }
}

impl<T: Clone + PartialEq> StabilityWait<T> {
    /// Feeds one poll result. Returns `Some(payload)` only once the same
    /// payload has been observed [`STABLE_READS`] times running.
    pub fn observe(&mut self, read: Option<T>) -> Option<T> {
        let payload = read?;
        self.run = if self.last.as_ref() == Some(&payload) {
            self.run + 1
        } else {
            1
        };
        self.last = Some(payload);
        (self.run >= STABLE_READS).then(|| self.last.clone().expect("just set"))
    }

    /// The most recent observation, for a timeout message that says what
    /// the poll was actually seeing.
    pub fn last(&self) -> Option<&T> {
        self.last.as_ref()
    }
}

/// Polls `read` until [`StabilityWait`] settles, or `deadline` passes.
///
/// **The deadline is WALL CLOCK, checked before each poll**, so the worst
/// case is `deadline` plus one in-flight poll — NOT
/// `polls x (request timeout + interval)`. A poll-count budget with a
/// 20 s request timeout and a 2 s sleep is a 22 s worst case per
/// iteration, and 90 of those is 33 minutes: a CI step that sits for half
/// an hour before failing reads as a hang, gets killed, and the panic
/// message this function exists to print is never seen.
///
/// Panics on timeout, naming the last observation.
pub fn settle_by<T: Clone + PartialEq + std::fmt::Debug>(
    deadline: std::time::Instant,
    interval: std::time::Duration,
    what: &str,
    mut read: impl FnMut() -> Option<T>,
) -> T {
    let mut wait = StabilityWait::default();
    while std::time::Instant::now() < deadline {
        if let Some(settled) = wait.observe(read()) {
            return settled;
        }
        std::thread::sleep(interval);
    }
    panic!(
        "{what}: never returned a STABLE non-empty view before the deadline \
         (last observation {:?})",
        wait.last()
    );
}

/// The trace reference's build, as its own build-info route reports it.
///
/// Every trace differential leg in this workspace is pointed at ONE pinned
/// container image (`.github/workflows/ci.yml` starts three instances of
/// it), and this is the build that image reports. Asserted, not recorded:
/// see [`assert_traces_reference_identity`].
pub const TRACES_REFERENCE_BUILD_VERSION: &str = "v3.0.2";

/// The source revision the same route reports for that build — the commit
/// this workspace's trace conformance is written against.
pub const TRACES_REFERENCE_BUILD_REVISION: &str = "0c4b926d0";

/// Every field the reference's build-info envelope carries. All six must
/// be present and be JSON strings; two of them are also pinned to a value.
const TRACES_REFERENCE_BUILD_INFO_FIELDS: [&str; 6] = [
    "version",
    "revision",
    "branch",
    "buildUser",
    "buildDate",
    "goVersion",
];

/// The one string-valued field `name` of a flat JSON object, or `None`.
///
/// A deliberate hand parse rather than a JSON crate: this crate is
/// dependency-free by design (see the note on `[dependencies]` in its
/// manifest), and every test binary in the workspace links it. It reads
/// only what it needs — `"name"`, optional space, `:`, optional space, a
/// quoted string with `\`-escapes — and returns `None` for anything else,
/// so a body that merely MENTIONS the field name does not satisfy it.
fn json_string_field(body: &str, name: &str) -> Option<String> {
    let key = format!("\"{name}\"");
    let mut from = 0usize;
    loop {
        let at = body[from..].find(&key)? + from;
        let rest = body[at + key.len()..].trim_start();
        from = at + key.len();
        let Some(rest) = rest.strip_prefix(':') else {
            continue;
        };
        let rest = rest.trim_start();
        let Some(rest) = rest.strip_prefix('"') else {
            continue;
        };
        let mut out = String::new();
        let mut chars = rest.chars();
        while let Some(c) = chars.next() {
            match c {
                '"' => return Some(out),
                '\\' => out.push(chars.next()?),
                other => out.push(other),
            }
        }
        return None;
    }
}

/// Establish that `api_base` is answered by the PINNED TRACE REFERENCE and
/// not by something else listening on that port (issue #479, code review
/// wave 3).
///
/// **Why a guard needs this.** The two isolation guards below and in
/// `crates/pulsus-read/tests/traces_search_projection_differential.rs`
/// conclude "this instance holds no other suite's data" from an empty
/// search envelope, empty tag arrays, or a `404`. Every one of those
/// answers is trivially produced by a service that is not the reference at
/// all — a stale container on a reused port, another suite's server, a
/// proxy — so before wave 3 an unrelated service on the endpoint SATISFIED
/// both guards. Emptiness only means "free of other data" once the
/// responder is known to be the instance the suite thinks it is talking to.
///
/// **What it establishes.** `GET {api_base}/api/status/buildinfo` answers
/// `200` with a flat JSON object carrying all six of
/// [`TRACES_REFERENCE_BUILD_INFO_FIELDS`] as strings, whose `version` and
/// `revision` are [`TRACES_REFERENCE_BUILD_VERSION`] and
/// [`TRACES_REFERENCE_BUILD_REVISION`]. Fail-closed throughout: a
/// transport failure, a non-200, an unparseable body or a missing field is
/// a panic, never an accepted answer.
///
/// **What it does NOT establish, stated so it is not read as more.**
///
/// * It authenticates the SERVICE AND BUILD, not the INSTANCE. Two
///   containers of the same pinned image are indistinguishable here; that
///   is what the endpoint-identity checks in the projection differential
///   are for, and they are a separate mechanism.
/// * A responder that deliberately replays this envelope passes. The
///   hazard it is built for is an accidental one — some other service
///   answering on a port this run assumed was the reference's — not an
///   adversary.
/// * It says nothing about what the instance HOLDS. That is the caller's
///   own check, which runs after this one.
pub fn assert_traces_reference_identity(api_base: &str) {
    let url = format!("{}/api/status/buildinfo", api_base.trim_end_matches('/'));
    let out = std::process::Command::new("curl")
        .args(["-s", "-w", "\n%{http_code}", "--max-time", "20"])
        .arg(&url)
        .output()
        .unwrap_or_else(|e| panic!("curl {url} could not be run: {e}"));
    assert!(
        out.status.success(),
        "the reference identity check could not reach {url}: curl exited {:?}: {}\nA suite that \
         cannot confirm WHAT is answering on this endpoint cannot conclude anything from what it \
         answers.",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    let body = String::from_utf8_lossy(&out.stdout);
    let (payload, status) = body
        .rsplit_once('\n')
        .unwrap_or_else(|| panic!("curl {url} wrote no status line: {body:?}"));
    assert_eq!(
        status.trim(),
        "200",
        "{url} answered HTTP {} — body {payload:?}. The pinned reference answers this route; \
         whatever is on this endpoint is not it.",
        status.trim()
    );
    let fields: Vec<(&str, String)> = TRACES_REFERENCE_BUILD_INFO_FIELDS
        .into_iter()
        .map(|field| {
            let value = json_string_field(payload, field).unwrap_or_else(|| {
                panic!(
                    "{url} returned {payload:?}, which carries no string field {field:?}. The \
                     pinned reference's build-info envelope carries all of \
                     {TRACES_REFERENCE_BUILD_INFO_FIELDS:?}; whatever is on this endpoint is \
                     not it."
                )
            });
            (field, value)
        })
        .collect();
    let pinned = |field: &str| -> &str {
        fields
            .iter()
            .find(|(f, _)| *f == field)
            .map(|(_, v)| v.as_str())
            .unwrap_or_else(|| panic!("{field} was just read from {url}"))
    };
    assert_eq!(
        (pinned("version"), pinned("revision")),
        (
            TRACES_REFERENCE_BUILD_VERSION,
            TRACES_REFERENCE_BUILD_REVISION
        ),
        "{url} reports build {fields:?}, not the pinned \
         {TRACES_REFERENCE_BUILD_VERSION}/{TRACES_REFERENCE_BUILD_REVISION}. This suite's \
         expectations are that build's behaviour."
    );
}

/// Refuse to run against a reference instance that already holds
/// `trace_hex` (issue #479, code review wave 2).
///
/// The matched-span projection differential fills its instance with a
/// corpus that enters a `compare()` top-N. If a suite that reads such an
/// aggregate is then pointed at the same instance, it fails with a value
/// mismatch against the reference — a failure that reads exactly like a
/// parity defect in the code under review, and cost one review round. The
/// projection suite refuses an instance that is not empty; this is the
/// other direction, the one it cannot check for itself, because its
/// corpus is only resident AFTER it has run.
///
/// Fail-closed: only an explicit `404` from the AUTHENTICATED reference
/// counts as absent. A transport error, any other status, or a `200` are
/// all refusals — a guard that reads "not present" from a request that
/// failed passes precisely when it cannot see.
///
/// **The response classes this guard treats as free, measured — exactly
/// one of them (issue #479, code review wave 3).** Rows 1-4 and 7-8 are
/// answered by a loopback responder; rows 1-4 and 7 serve the reference's
/// own build-info envelope on the identity route, so the row isolates the
/// TRACE route, and row 8 does not.
///
/// | # | what answers `/api/traces/{trace_hex}` | verdict | rejected at |
/// |---|---|---|---|
/// | 1 | `200` carrying the trace | REJECTED | trace route |
/// | 2 | `200` with an empty envelope | REJECTED | trace route |
/// | 3 | `404` | **FREE — the only one** | — |
/// | 4 | `503` | REJECTED | trace route |
/// | 5 | no answer within `--max-time` | REJECTED | identity route |
/// | 6 | connection refused | REJECTED | identity route |
/// | 7 | `200` with a malformed body | REJECTED | trace route |
/// | 8 | a DIFFERENT service on the port | REJECTED | identity route |
///
/// Row 8 is the wave-3 finding: before
/// [`assert_traces_reference_identity`] ran first, an unrelated service
/// answering `404` — the ordinary answer for an unknown path — satisfied
/// this guard outright.
pub fn assert_reference_instance_is_free_of(api_base: &str, trace_hex: &str, owner: &str) {
    // WHO is answering, before anything is read into what it answers.
    assert_traces_reference_identity(api_base);
    let url = format!("{}/api/traces/{trace_hex}", api_base.trim_end_matches('/'));
    let out = std::process::Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "--max-time",
            "20",
        ])
        .arg(&url)
        .output()
        .unwrap_or_else(|e| panic!("curl {url} could not be run: {e}"));
    assert!(
        out.status.success(),
        "curl {url} exited {:?}: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    let code = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert_eq!(
        code, "404",
        "the reference at {api_base} answered HTTP {code} for trace {trace_hex}, the corpus \
         {owner} pushes. Anything but 404 means this instance is not exclusively this suite's: \
         {owner}'s spans enter compare()'s top-N and this suite would then report a value \
         mismatch against the reference that looks like a parity defect. Give each leg its own \
         instance."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const VAR: &str = CLICKHOUSE_GATE;

    /// The pinned reference's build-info body, captured verbatim from the
    /// pinned image on 2026-09-02 (`curl -s
    /// http://localhost:<port>/api/status/buildinfo`).
    const BUILD_INFO_BODY: &str = concat!(
        r#"{"version":"v3.0.2","revision":"0c4b926d0","branch":"HEAD","#,
        r#""buildUser":"","buildDate":"","goVersion":"go1.26.3"}"#
    );

    #[test]
    fn every_pinned_build_info_field_is_read_out_of_the_captured_body() {
        let read: Vec<(&str, Option<String>)> = TRACES_REFERENCE_BUILD_INFO_FIELDS
            .into_iter()
            .map(|f| (f, json_string_field(BUILD_INFO_BODY, f)))
            .collect();
        assert_eq!(
            read,
            vec![
                ("version", Some("v3.0.2".to_string())),
                ("revision", Some("0c4b926d0".to_string())),
                ("branch", Some("HEAD".to_string())),
                ("buildUser", Some(String::new())),
                ("buildDate", Some(String::new())),
                ("goVersion", Some("go1.26.3".to_string())),
            ]
        );
        assert_eq!(
            json_string_field(BUILD_INFO_BODY, "version").as_deref(),
            Some(TRACES_REFERENCE_BUILD_VERSION)
        );
        assert_eq!(
            json_string_field(BUILD_INFO_BODY, "revision").as_deref(),
            Some(TRACES_REFERENCE_BUILD_REVISION)
        );
    }

    /// A body that MENTIONS the field name does not supply it. This is the
    /// whole reason the identity check parses rather than searching for a
    /// substring: a different service's error page quoting the word is not
    /// a build-info envelope.
    #[test]
    fn a_field_name_that_is_not_a_string_valued_key_is_not_read() {
        for body in [
            r#"{"note":"version is not reported here"}"#,
            r#"{"version":3}"#,
            r#"{"version":null}"#,
            r#"{"version":["v3.0.2"]}"#,
            r#"{"version"}"#,
            r#"{"version":"unterminated"#,
            "not json at all",
            "",
        ] {
            assert_eq!(json_string_field(body, "version"), None, "{body:?}");
        }
    }

    #[test]
    fn a_later_occurrence_is_used_when_an_earlier_one_is_not_a_key() {
        assert_eq!(
            json_string_field(r#"{"a":"version","version":"v3.0.2"}"#, "version").as_deref(),
            Some("v3.0.2")
        );
    }

    #[test]
    fn an_escaped_quote_inside_the_value_is_kept() {
        assert_eq!(
            json_string_field(r#"{"branch":"re\"lease","version":"v1"}"#, "branch").as_deref(),
            Some("re\"lease")
        );
    }

    #[test]
    fn whitespace_around_the_colon_is_accepted() {
        assert_eq!(
            json_string_field("{\"version\" :   \"v3.0.2\"}", "version").as_deref(),
            Some("v3.0.2")
        );
    }

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

    // ------------------------------------------------------------------
    // Endpoint gates (issue #458 review round 3). A gate whose VALUE is
    // the address the suite talks to needs the same #320 guarantee, and
    // cannot get it from the boolean rule: `http://localhost:13200` is
    // not `1`, so the boolean rule reads a fully-wired step as unwired
    // and panics saying the variable is not set.
    //
    // All four states are asserted for the endpoint kind, and the fourth
    // — present-but-not-`1` under its own job — is the one the boolean
    // rule gets wrong.
    // ------------------------------------------------------------------

    const URL_VAR: &str = "PULSUSDB_TEMPO_DIFF_URL";
    const URL: &str = "http://localhost:13200";

    /// State 1: present and valid → runs, in any job.
    #[test]
    fn an_endpoint_gate_with_a_url_runs() {
        for job in [None, Some("ci"), Some("schema-it")] {
            assert_eq!(
                classify_kind(GateValue::Endpoint, URL_VAR, Some(URL), job),
                LiveGate::Run,
                "job {job:?}"
            );
        }
    }

    /// State 2: absent under its own job → fails loudly.
    #[test]
    fn an_absent_endpoint_gate_in_a_live_ci_job_is_a_wiring_failure() {
        assert_eq!(
            classify_kind(GateValue::Endpoint, URL_VAR, None, Some("schema-it")),
            LiveGate::MissingInLiveCiJob {
                job: "schema-it".to_string(),
                var: URL_VAR.to_string(),
            }
        );
        // A blank or whitespace-only value is absent, not present: an
        // `env:` block that expanded to nothing must not be read as
        // wiring.
        for blank in ["", "   ", "\t"] {
            assert!(
                matches!(
                    classify_kind(GateValue::Endpoint, URL_VAR, Some(blank), Some("schema-it")),
                    LiveGate::MissingInLiveCiJob { .. }
                ),
                "{blank:?}"
            );
        }
    }

    /// State 3: absent under a hermetic job (and on a laptop) → skips.
    #[test]
    fn an_absent_endpoint_gate_outside_a_live_ci_job_skips_cleanly() {
        assert_eq!(
            classify_kind(GateValue::Endpoint, URL_VAR, None, Some("ci")),
            LiveGate::SkipUngated
        );
        assert_eq!(
            classify_kind(GateValue::Endpoint, URL_VAR, None, None),
            LiveGate::SkipUngated
        );
    }

    /// State 4 — the one the boolean rule gets wrong. A present,
    /// non-`"1"` value under a live job RUNS. Asserted beside what the
    /// boolean rule does with the same input, so the difference between
    /// the two kinds is visible in one place rather than inferred.
    #[test]
    fn a_present_non_one_endpoint_value_runs_where_the_boolean_rule_would_panic() {
        assert_eq!(
            classify_kind(GateValue::Endpoint, URL_VAR, Some(URL), Some("schema-it")),
            LiveGate::Run
        );
        assert_eq!(
            classify_kind(GateValue::Boolean, URL_VAR, Some(URL), Some("schema-it")),
            LiveGate::MissingInLiveCiJob {
                job: "schema-it".to_string(),
                var: URL_VAR.to_string(),
            },
            "this is the CI failure that motivated the split: a URL is not a 1"
        );
    }

    /// The two kinds differ ONLY in which values count as set. Adding the
    /// endpoint kind must not have moved the boolean kind, and this is
    /// the assertion that says so rather than the reader trusting the
    /// refactor: every existing gate value, both kinds, side by side.
    #[test]
    fn the_endpoint_kind_did_not_move_the_boolean_kind() {
        for (value, boolean_runs, endpoint_runs) in [
            (Some("1"), true, true),
            (Some("0"), false, true),
            (Some("true"), false, true),
            (Some(""), false, false),
            (Some(URL), false, true),
            (None, false, false),
        ] {
            assert_eq!(
                classify_kind(GateValue::Boolean, VAR, value, None).is_running(),
                boolean_runs,
                "boolean {value:?}"
            );
            assert_eq!(
                classify_kind(GateValue::Endpoint, VAR, value, None).is_running(),
                endpoint_runs,
                "endpoint {value:?}"
            );
        }
        // And the boolean entry point still goes through the boolean
        // rule: `classify` is what every existing caller reaches.
        assert_eq!(classify(VAR, Some("0"), None), LiveGate::SkipUngated);
        assert_eq!(classify(VAR, Some(URL), None), LiveGate::SkipUngated);
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

    // -----------------------------------------------------------------
    // Trimming, over the whole set of characters `str::trim` strips.
    //
    // Review finding (PR #424, round 3): the previous version of these
    // tests wrote out a handful of spellings by hand and claimed to cover
    // "every whitespace character `str::trim` would have removed". It did
    // not, and a hand-written list beside an "every" is the defect this
    // branch had already produced once before. So the set is derived
    // instead of enumerated: [`characters_trim_strips`] computes it by
    // running `trim`, and a character added to Rust's definition in a
    // future toolchain is picked up on the next run.
    //
    // The derivation is the mechanism, and it is what is verified. To see
    // which characters are in the set, run the tests below.
    // -----------------------------------------------------------------

    /// The characters [`str::trim`] strips, **derived by running `trim`**
    /// rather than copied out of its documentation.
    ///
    /// Every Unicode scalar value is tried; `c` is kept when the
    /// one-character string `c` trims to nothing, which is exactly the
    /// condition under which `trim` removes `c` from an end. The scan is
    /// the whole scalar range — `char::from_u32` skips the surrogate gap
    /// — so there is no boundary for a character to hide behind.
    fn characters_trim_strips() -> Vec<char> {
        (0..=u32::from(char::MAX))
            .filter_map(char::from_u32)
            .filter(|c| c.to_string().trim().is_empty())
            .collect()
    }

    /// Checks the derivation, **not** the set it produced.
    ///
    /// A sweep over an empty (or universal) set passes vacuously, so this
    /// asserts that [`characters_trim_strips`] found the characters any
    /// working `trim` must strip and did not classify an ordinary letter
    /// as whitespace.
    ///
    /// The characters below are named on purpose: they are the probe, and
    /// a probe has to be written down.
    fn assert_derivation_is_sane(ws: &[char]) {
        for anchor in [' ', '\t', '\n', '\r'] {
            assert!(
                ws.contains(&anchor),
                "deriving the trim set found {} characters and not {anchor:?} — the derivation is \
                 broken, so anything it appears to prove is vacuous",
                ws.len()
            );
        }
        assert!(
            !ws.contains(&'w'),
            "the derivation classified an ordinary letter as whitespace"
        );
    }

    /// Every character `trim` strips is absorbed at a prefix end: the
    /// composed database is the same one the bare prefix names, and a
    /// prefix made only of such characters reads as unset. `" wt3 "` and
    /// `"wt3"` naming the same database is the whole justification for
    /// trimming rather than refusing — see the note in `test_db`'s docs,
    /// which also records the *false* justification a previous revision
    /// gave for it.
    #[test]
    fn every_character_trim_strips_is_absorbed_at_a_prefix_end() {
        let ws = characters_trim_strips();
        assert_derivation_is_sane(&ws);
        for c in &ws {
            assert_eq!(
                compose_db_name(Some(&format!("{c}wt3{c}")), "pulsus_x_it"),
                "wt3_pulsus_x_it",
                "a prefix wrapped in {c:?} must name the same database as \"wt3\""
            );
            assert_eq!(
                compose_db_name(Some(&c.to_string()), "pulsus_x_it"),
                "pulsus_x_it",
                "a prefix of nothing but {c:?} must read as unset"
            );
        }
    }

    /// …and every character `trim` strips is refused *inside* a prefix,
    /// where it would change the composed name. Iterated from the same
    /// derivation as the test above, so absorbing at an end and refusing
    /// in the middle cannot silently converge on a character neither test
    /// happens to mention.
    #[test]
    fn every_character_trim_strips_is_refused_inside_a_prefix() {
        let ws = characters_trim_strips();
        assert_derivation_is_sane(&ws);
        for c in &ws {
            let spelling = format!("wt{c}3");
            let err = std::panic::catch_unwind(|| compose_db_name(Some(&spelling), "pulsus_x_it"))
                .expect_err("whitespace inside a prefix must panic");
            let msg = err
                .downcast_ref::<String>()
                .map(String::as_str)
                .unwrap_or("<non-string panic payload>");
            assert!(
                msg.contains("is not a usable database-name prefix"),
                "{c:?} inside a prefix: {msg}"
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

    /// The length ceiling, driven from the constant rather than from a
    /// literal repeated in prose: a prefix long enough to breach it
    /// whatever the constant is set to.
    #[test]
    fn an_over_long_composed_name_is_refused() {
        let err = std::panic::catch_unwind(|| {
            compose_db_name(Some(&"p".repeat(MAX_DATABASE_NAME_LEN)), "pulsus_x_it")
        })
        .expect_err("a name over the ceiling must panic");
        let msg = err
            .downcast_ref::<String>()
            .map(String::as_str)
            .unwrap_or("<non-string panic payload>");
        assert!(
            msg.contains(&format!("over the {MAX_DATABASE_NAME_LEN}")),
            "{msg}"
        );
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
    /// interpolates it throughout a suite names one database.
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
