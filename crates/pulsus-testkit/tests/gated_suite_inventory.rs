//! Issue #320 review, finding 3: the migration of the 54 environment-gated
//! suites onto [`pulsus_testkit`] was verified by a one-time sweep, and a
//! one-time sweep protects nothing. This is the standing version — it runs
//! in the hermetic `cargo test --workspace` lane on every push.
//!
//! # What it checks — stated as narrowly as the mechanism allows
//!
//! Three **substring** properties of `crates/*/tests/*.rs`, and nothing
//! more:
//!
//! 1. no file contains the exact byte sequence `env::var("<GATE>`, for
//!    each of this crate's two gate constants;
//! 2. at least [`MIGRATED_SUITE_FLOOR`] files mention `pulsus_testkit::`;
//! 3. every file that READS a gate is invoked by a `--test <name>` step
//!    in `ci.yml`, or is named in `DELIBERATELY_UNWIRED` with the reason
//!    it is not (issue #323).
//!
//! (1) and (2) are issue #320's and are unchanged.
//!
//! Property (1) is **one spelling**, not a rule about `env::var` calls.
//! This is a substring scan over source text, not a parse, so it sees
//! neither the syntax tree nor any equivalent way of writing the same
//! call. That is *not* the same as "every gated suite reads its gate
//! through this crate", and the gap is large enough to be worth naming
//! rather than leaving the reader to infer it.
//!
//! What establishes the real property is the **runtime census**: execute
//! every gated binary with the gate unset and `GITHUB_JOB` set to a live
//! job id, and require that no test reports `ok`. That is what found the
//! three unguarded hermetic tests during #320, and it is the thing to
//! re-run after touching a gated suite —
//!
//! ```text
//! env -u PULSUS_TEST_CLICKHOUSE -u PULSUS_TEST_CLICKHOUSE_TLS GITHUB_JOB=live-it \
//!   cargo test -p <crate> --test <suite> -- --test-threads=1
//! ```
//!
//! — which must fail. This check is the cheap standing tripwire for the
//! regression actually observed in the wild (a new suite hand-writing the
//! obvious four-word idiom, which is what the original sweep found and
//! what reads most naturally); it is not a bypass-proof gate, and
//! teaching it more spellings would not make it one.
//!
//! # What it cannot see (stated, not implied)
//!
//! * **The same literal read, spelled differently** — a raw string
//!   (`env::var(r#"PULSUS_TEST_CLICKHOUSE"#)`) or any interposed token
//!   (`env::var( /* note */ "PULSUS_TEST_CLICKHOUSE")`, a newline after
//!   the parenthesis). Deliberately not chased: neither form occurs
//!   anywhere in the workspace today (measured on #320 round 3: 0 of 171
//!   `env::var` call sites), and a substring scan cannot be made
//!   exhaustive by adding cases to it.
//! * **A gate read through an indirection** — `env::var` applied to a
//!   constant, a `&str` variable, or a name assembled at run time.
//!   `std::env::var(pulsus_testkit::CLICKHOUSE_GATE)` reads the gate
//!   directly, evades (1) because the literal never appears, and
//!   satisfies (2) because the file does mention `pulsus_testkit::`.
//! * **A gate moved out of `tests/`** — into a crate's `src/`, a
//!   `tests/common/` module, or a dev-dependency helper. Only top-level
//!   `crates/*/tests/*.rs` files are test *binaries*, so that is the scope
//!   here; a gate read from anywhere else is invisible.
//! * **A different variable.** A new suite gated on, say,
//!   `PULSUS_TEST_TEMPO` is not matched. The two variables this crate
//!   knows about are its own constants.
//! * **`#[ignore]`d tests**, which the gate never runs for anyway.
//! * **A test inside a gated binary that reaches no guard** — the three
//!   found during #320. Only the runtime census above sees these.
//! * **A gated suite with no CI step at all** — closed by issue #323,
//!   the third property below. What that one still cannot see is
//!   narrower and is stated at the test itself: a step that exists but
//!   never runs, and a gated TEST inside a binary whose hermetic half
//!   gives the binary a step.
//! * **Anything outside `crates/`**: `xtask/` and `e2e/` are not scanned.

use std::path::{Path, PathBuf};

/// The floor on the migrated population. Issue #320 migrated 54; this is a
/// floor and not an equality so that adding a live suite does not churn an
/// unrelated PR, while a suite silently losing its gate still reddens.
const MIGRATED_SUITE_FLOOR: usize = 54;

fn workspace_root() -> PathBuf {
    // crates/pulsus-testkit -> crates -> <root>
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(Path::parent)
        .expect("the manifest dir is crates/pulsus-testkit")
        .to_path_buf()
}

/// Every `crates/*/tests/*.rs` — the integration-test **binaries** —
/// except this crate's own, which provides the helper rather than
/// consuming it (scope, not an exemption: there is no gated suite here).
///
/// Files nested deeper (`tests/common/…`) are modules of those binaries,
/// not binaries themselves, and are deliberately out of scope.
fn test_binaries() -> Vec<PathBuf> {
    let mut out = Vec::new();
    let crates = workspace_root().join("crates");
    let entries = std::fs::read_dir(&crates).unwrap_or_else(|e| panic!("read {crates:?}: {e}"));
    for krate in entries {
        let krate = krate.expect("crate dir entry").path();
        if krate.file_name().is_some_and(|n| n == "pulsus-testkit") {
            continue;
        }
        let dir = krate.join("tests");
        let Ok(files) = std::fs::read_dir(&dir) else {
            continue; // a crate with no integration tests
        };
        for f in files {
            let path = f.expect("test dir entry").path();
            if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    assert!(
        out.len() > 100,
        "the scan found only {} test binaries — the workspace layout moved and this check is \
         no longer looking where the suites are",
        out.len()
    );
    out
}

fn rel(path: &Path) -> String {
    path.strip_prefix(workspace_root())
        .unwrap_or(path)
        .display()
        .to_string()
}

/// The **one spelling** this check knows, assembled at run time from two
/// fragments so that **this file does not itself contain it** — a literal
/// here would either match itself or need an exemption list, and exemption
/// lists rot.
///
/// An exact byte sequence, deliberately: a raw string or any interposed
/// token writes the same call and is not matched. See the module docs for
/// why that is stated rather than chased.
fn gate_read_spelling_needles() -> [String; 2] {
    let call = concat!("env::", "var(\"");
    [
        format!("{call}{}", pulsus_testkit::CLICKHOUSE_GATE),
        format!("{call}{}", pulsus_testkit::CLICKHOUSE_TLS_GATE),
    ]
}

/// Property (1): no test binary contains the exact byte sequence
/// `env::var("<GATE>`. Prose mentions of the variable (module docs, skip
/// messages) are untouched.
///
/// The name says `contains_the_exact_…_spelling` rather than anything
/// about reading the gate, because that is all it establishes: an
/// indirect read (`env::var(pulsus_testkit::CLICKHOUSE_GATE)`), a raw
/// string, or an interposed comment all read the gate directly and all
/// pass this test. The runtime census in the module docs is what closes
/// those.
#[test]
fn no_test_binary_contains_the_exact_gate_read_spelling() {
    let needles = gate_read_spelling_needles();
    let mut offenders: Vec<String> = Vec::new();
    for path in test_binaries() {
        let src = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        if needles.iter().any(|n| src.contains(n.as_str())) {
            offenders.push(rel(&path));
        }
    }
    assert!(
        offenders.is_empty(),
        "these test binaries spell out `env::var(\"<GATE>` instead of calling \
         pulsus_testkit, so with the gate absent they exit 0 in CI and report green \
         (issue #320): {offenders:?}. Use pulsus_testkit::live_clickhouse_enabled() (or \
         live_gate_enabled/require_live_gate) — and if the suite also has hermetic tests, \
         call require_live_gate first in those too."
    );
}

/// Property (2): a change that strips the guard out of the gated suites
/// wholesale should not pass quietly just because no file contains the
/// one spelling property (1) knows.
///
/// This counts **mentions of `pulsus_testkit::`**, which is weaker than
/// "calls the guard": a file could reference one of this crate's constants
/// and never call `require_live_gate`. It is a floor against mass removal,
/// not a proof of routing.
#[test]
fn at_least_the_migrated_population_still_mentions_the_testkit() {
    let migrated: Vec<String> = test_binaries()
        .into_iter()
        .filter(|p| {
            std::fs::read_to_string(p)
                .unwrap_or_else(|e| panic!("read {p:?}: {e}"))
                .contains("pulsus_testkit::")
        })
        .map(|p| rel(&p))
        .collect();
    assert!(
        migrated.len() >= MIGRATED_SUITE_FLOOR,
        "only {} test binaries route through pulsus_testkit, below the {MIGRATED_SUITE_FLOOR} \
         issue #320 migrated. A gated suite has lost its live-CI-job guard: {migrated:?}",
        migrated.len()
    );
}

// ---------------------------------------------------------------------
// Issue #323 — property (3): a gated suite with NO CI step at all.
//
// This is the gap the module doc above already named as out of scope.
// #320 built the derived walk and the population floor; what was missing
// is that a suite nobody wired up looks exactly like a suite deliberately
// left unwired, and "someone forgot" is then indistinguishable from a
// decision. Same class as #272's provenance step, which shipped and never
// ran.
//
// The check does not decide which absence is which — it forces the
// decision into a committed reason string, which is what "deliberate
// versus forgotten" means mechanically.
// ---------------------------------------------------------------------

/// Gated suites with **no** `--test <name>` step in `ci.yml`, each with
/// the reason it has none.
///
/// A reason amounting to "forgotten" is not admissible: wire the suite
/// up instead. The staleness half of the check below is what stops this
/// becoming a list that only ever grows — an entry whose suite has since
/// been wired up, or has been deleted, fails.
const DELIBERATELY_UNWIRED: &[(&str, &str)] = &[
    (
        "live_tls",
        "needs a TLS-enabled ClickHouse; no job starts one. The hermetic half of TLS is \
         covered by pulsus-server/tests/tls_live.rs, which does have a step.",
    ),
    (
        "nestedset_value_differential",
        "needs a live ClickHouse AND a pinned grafana/tempo:3.0.2 in the same job; no job \
         runs both. It is the #185 closeout hook and runs by hand against that pair.",
    ),
    (
        "re2_reject_classes",
        "MIXED, and the reason is the unit rather than the suite: its four hermetic tests do \
         run, in the workspace lane, and only its one PULSUSDB_LOGQL_DIFF_URL probe does not. \
         Found by this check rather than known — the reference container it wants is the \
         logql-diff one, which lives in the nightly job, and that job runs no pulsus-re2 \
         step. Recorded here rather than wired up because pulsus-re2 has a suite that takes \
         minutes and the nightly job is not the place to discover that; the probe carries its \
         own by-hand invocation in its doc comment.",
    ),
];

/// Every suite name `ci.yml` invokes as `--test <name>`, however the
/// command is spelled — a bare `cargo test`, or through
/// `ci/test-summary.sh run <label> … --test <name>`.
fn suites_with_a_ci_step() -> std::collections::BTreeSet<String> {
    let path = workspace_root().join(".github/workflows/ci.yml");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    let mut out = std::collections::BTreeSet::new();
    for (i, part) in text.split("--test").enumerate() {
        // `split` yields the text BEFORE the first separator first.
        if i == 0 {
            continue;
        }
        if let Some(name) = part.split_whitespace().next() {
            out.insert(name.to_string());
        }
    }
    assert!(
        out.len() > 50,
        "only {} `--test <name>` invocations found in the workflow — the scan is no longer \
         reading the steps, so every suite would look unwired",
        out.len()
    );
    out
}

/// The spellings that make a suite GATED — i.e. that make some test in
/// it SKIP when the gate is absent.
///
/// **Deliberately not the #320 population.** That one is "the source
/// mentions `pulsus_testkit::`", which is the right floor for property
/// (2) and the wrong population here: it includes suites that merely
/// NAME the crate. Measured — with the #320 population this check
/// reported `live_db_naming` as an unwired gated suite, and it is a
/// hermetic source-scanner whose whole subject is the literal text
/// `pulsus_testkit::test_db(`, which it carries as a search NEEDLE. It
/// runs in the workspace lane on every push. Excusing it would have been
/// a committed reason for a suite that never needed one.
///
/// So the population is the suites that READ a gate: this crate's three
/// gate-reading entry points, plus the one gate it does not own.
const GATE_READ_SPELLINGS: &[&str] = &[
    "require_live_gate",
    "live_gate_enabled",
    "live_clickhouse_enabled",
    "PULSUSDB_LOGQL_DIFF_URL",
];

/// Property (3): every gated test binary either has a CI step, or is
/// named in [`DELIBERATELY_UNWIRED`] with a reason — **and** no entry in
/// that list is stale.
///
/// **Stated limits**, inherited and new:
///
/// * the population is a SUBSTRING property over `crates/*/tests/*.rs`,
///   exactly [`test_binaries`]'s scope — a suite gated on some third
///   variable, or living outside `crates/`, is invisible;
/// * the step scan sees the workflow's LITERAL `--test <name>`
///   invocations. A suite reached through a script that composes the
///   name, or run by a different workflow file, reads as unwired;
/// * it says nothing about whether a step that exists actually RUNS —
///   a step behind an `if:` that is never true passes this check. That
///   is #272's failure and is not what this closes;
/// * the unit is the BINARY. A binary that mixes hermetic tests with one
///   gated test has a step as soon as the hermetic half is run by the
///   workspace lane, so a gated test inside it can still be unreached.
///   `re2_reject_classes` is exactly that shape and is recorded in
///   [`DELIBERATELY_UNWIRED`] for it.
#[test]
fn every_gated_suite_has_a_ci_step_or_a_committed_reason() {
    let with_step = suites_with_a_ci_step();
    let excused: std::collections::BTreeMap<&str, &str> =
        DELIBERATELY_UNWIRED.iter().copied().collect();

    let mut gated: Vec<String> = Vec::new();
    for path in test_binaries() {
        let src = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        if GATE_READ_SPELLINGS.iter().any(|n| src.contains(n)) {
            gated.push(
                path.file_stem()
                    .expect("a .rs file has a stem")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    gated.sort();
    assert!(
        gated.len() > 40,
        "only {} gated suites found — the population predicate stopped matching and every \
         absence below would read as fine: {gated:?}",
        gated.len()
    );

    let unwired: Vec<&String> = gated
        .iter()
        .filter(|s| !with_step.contains(s.as_str()))
        .collect();
    let unexcused: Vec<&&String> = unwired
        .iter()
        .filter(|s| !excused.contains_key(s.as_str()))
        .collect();
    assert!(
        unexcused.is_empty(),
        "these gated suites have no `--test <name>` step in ci.yml, so they never run \
         anywhere and a regression in them is invisible: {unexcused:?}. Wire each up, or add \
         it to DELIBERATELY_UNWIRED with the reason it cannot be — a reason amounting to \
         `forgotten` is not admissible."
    );

    // The other half, and the one that keeps this from becoming a list
    // that only grows: an excused suite that HAS a step, or that no
    // longer exists, is a stale excuse.
    for (suite, reason) in DELIBERATELY_UNWIRED {
        assert!(
            !reason.trim().is_empty(),
            "{suite} is excused with an empty reason, which excuses nothing"
        );
        assert!(
            gated.iter().any(|g| g == suite),
            "DELIBERATELY_UNWIRED names {suite}, which is not a gated suite in this workspace \
             — it was renamed or deleted, and the entry is now excusing nothing"
        );
        assert!(
            !with_step.contains(*suite),
            "DELIBERATELY_UNWIRED still excuses {suite}, but ci.yml now runs it. Delete the \
             entry — a list of exemptions that can only grow stops meaning anything"
        );
    }
}
