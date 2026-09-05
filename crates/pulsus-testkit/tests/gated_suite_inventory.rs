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

// ---------------------------------------------------------------------
// Issue #523 — property (4): the ENDPOINT read.
//
// Properties (1)-(3) are about the BOOLEAN gates. The endpoint gates —
// the ones whose value is the address of the reference container a
// differential compares against — carried the identical hole, and it was
// closed one suite at a time: #492 part 3 on one, #523 on two more. A
// code review of #523 then found sixteen more still reading their address
// with a bare `env::var` and taking absence as a skip, which reports a
// pass having compared nothing.
//
// Sixteen copies of the same three-line correction is how this repository
// acquired its other duplicated decisions, so the read and the fail-closed
// decision now live together in `pulsus_testkit::live_endpoint` and every
// suite makes one call. THIS check is what makes that "every suite"
// rather than "the seventeen we happened to convert".
// ---------------------------------------------------------------------

/// `PULSUSDB_*` variables that are NOT a reference address, and so are not
/// [`pulsus_testkit::live_endpoint`]'s business.
///
/// One entry, and the two assertions in [`no_test_source_reads_an_endpoint_variable_directly`]
/// are what stop it becoming the place a real endpoint hides: an entry
/// that no longer occurs anywhere is a stale excuse and fails, and an
/// entry whose name ends in `_URL` is refused outright, because that is
/// the shape every endpoint in this workspace has.
const NON_ENDPOINT_PULSUSDB_VARS: &[(&str, &str)] = &[(
    "PULSUSDB_PROMQLTEST_CACHE_DIR",
    "a directory the corpus fetcher caches downloads in, not an address to \
     compare answers against — absent, it downloads instead of skipping",
)];

/// Floors on the converted population, set below the counts at the time of
/// writing (43 call sites in 27 files) with slack for ordinary deletion.
/// Without these, a change that deleted every `live_endpoint` call would
/// satisfy property (4)'s absence half and pass green.
const ENDPOINT_CALL_FLOOR: usize = 35;
const ENDPOINT_CALL_FILE_FLOOR: usize = 20;

/// Every `.rs` file under `crates/*/tests/`, **recursively**.
///
/// Deliberately wider than [`test_binaries`], which stops at the top level
/// because only top-level files are test binaries. An endpoint read moved
/// into a `tests/common/mod.rs` reads the address just as directly, and
/// that is a boundary properties (1)-(3) name as out of scope. This one
/// does not have it.
fn test_sources() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    let crates = workspace_root().join("crates");
    let entries = std::fs::read_dir(&crates).unwrap_or_else(|e| panic!("read {crates:?}: {e}"));
    for krate in entries {
        walk(
            &krate.expect("crate dir entry").path().join("tests"),
            &mut out,
        );
    }
    out.sort();
    assert!(
        out.len() > 100,
        "the recursive scan found only {} test sources — the workspace layout moved and this \
         check is no longer looking where the suites are",
        out.len()
    );
    out
}

/// The two spellings of a direct environment read, assembled at run time
/// so that **this file does not itself contain either** — the same reason
/// [`gate_read_spelling_needles`] is built the same way.
fn endpoint_read_needles() -> [String; 2] {
    [
        concat!("env::", "var(\"").to_string(),
        concat!("env::", "var_os(\"").to_string(),
    ]
}

/// Every `PULSUSDB_*` variable read directly by `path`, as
/// `(line, variable)`. Reads the name that follows the needle up to the
/// closing quote, so the check can distinguish an endpoint from the one
/// exempt non-endpoint variable rather than treating the prefix as the
/// whole rule.
fn direct_pulsusdb_reads(src: &str) -> Vec<(usize, String)> {
    let mut found = Vec::new();
    for needle in endpoint_read_needles() {
        let mut from = 0usize;
        while let Some(rel) = src[from..].find(needle.as_str()) {
            let name_at = from + rel + needle.len();
            from = name_at;
            let Some(close) = src[name_at..].find('"') else {
                continue;
            };
            let name = &src[name_at..name_at + close];
            if name.starts_with("PULSUSDB_") {
                let line = src[..name_at].bytes().filter(|&b| b == b'\n').count() + 1;
                found.push((line, name.to_string()));
            }
        }
    }
    found.sort();
    found
}

/// Property (4): no test source reads a `PULSUSDB_*` endpoint variable
/// itself. The address and the fail-closed decision come together, from
/// [`pulsus_testkit::live_endpoint`], or they do not come at all.
///
/// Same substring nature as property (1), and the same boundary: a raw
/// string, an interposed comment, or a name held in a constant all read
/// the variable and all pass this. What that leaves is the runtime census
/// in the module docs — run the suite with its address removed and
/// `GITHUB_JOB` set to a live job id, and require a nonzero exit.
#[test]
fn no_test_source_reads_an_endpoint_variable_directly() {
    let sources = test_sources();
    let mut offenders: Vec<String> = Vec::new();
    let mut exempt_seen: Vec<&str> = Vec::new();
    let mut call_sites = 0usize;
    let mut call_files = 0usize;

    for path in &sources {
        let src = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        let n = src.matches("pulsus_testkit::live_endpoint(").count();
        call_sites += n;
        if n > 0 {
            call_files += 1;
        }
        for (line, var) in direct_pulsusdb_reads(&src) {
            match NON_ENDPOINT_PULSUSDB_VARS.iter().find(|(v, _)| *v == var) {
                Some((v, _)) => exempt_seen.push(v),
                None => offenders.push(format!("{}:{line}: {var}", rel(path))),
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "these test sources read a reference address themselves instead of calling \
         pulsus_testkit::live_endpoint, so with the address absent they return success in a \
         live CI job having compared nothing (issue #523): {offenders:?}. Replace the read \
         with `pulsus_testkit::live_endpoint(\"<VAR>\")`, which yields `None` on a developer \
         machine and panics in a job that exists to supply the address."
    );

    // The floors. Property (4)'s absence half is satisfied by a tree with
    // no endpoint reads at all, including one where every call was
    // deleted, so the population is asserted too.
    assert!(
        call_sites >= ENDPOINT_CALL_FLOOR,
        "only {call_sites} pulsus_testkit::live_endpoint call sites remain, below the \
         {ENDPOINT_CALL_FLOOR} floor — a differential suite has lost its endpoint guard"
    );
    assert!(
        call_files >= ENDPOINT_CALL_FILE_FLOOR,
        "only {call_files} test sources call pulsus_testkit::live_endpoint, below the \
         {ENDPOINT_CALL_FILE_FLOOR} floor"
    );

    // Exemption hygiene, both halves: an exemption for a variable that is
    // gone excuses nothing and hides the next one, and no exemption may
    // wear an endpoint's name.
    for (var, reason) in NON_ENDPOINT_PULSUSDB_VARS {
        assert!(
            !reason.trim().is_empty(),
            "{var} is exempt with an empty reason, which exempts nothing"
        );
        assert!(
            !var.ends_with("_URL"),
            "{var} is exempt from the endpoint check but is named like an endpoint — every \
             reference address in this workspace ends in _URL, so this is the shape the \
             exemption list must never admit"
        );
        assert!(
            exempt_seen.contains(var),
            "NON_ENDPOINT_PULSUSDB_VARS exempts {var}, which no test source reads any more — \
             the entry is stale and is now only a hole for the next endpoint to hide in"
        );
    }
}

// ---------------------------------------------------------------------
// Issue #523 review round 2 — property (5): the NAME, not just the call.
//
// Property (4) scans for the read spelled out with the variable's name
// inside the call. (The spelling is not written here: this file is
// scanned too, and a comment is text like any other.) A code review showed
// (4) is dead for an equivalent read that names the variable in a constant
// first:
//
//     const ENDPOINT_VAR: &str = "PULSUSDB_LOGQL_DIFF_URL";
//     std::env::var(ENDPOINT_VAR)
//
// — every test in the binary green, and the suite fail-open again. (That
// measurement was taken when the binary held four tests; with this one
// added, the same break gives `5 tests run: 4 passed, 1 failed`, and the
// `4` is the point: property (4) is still green.) Unlike a deliberate
// bypass, naming a constant is ordinary style, so the check has to see it.
//
// Reaching it through the CALL would need to follow a value across a
// binding, an array and a closure parameter, which is a parser. Property
// (5) goes at the other end instead: it is about where the NAME may be
// WRITTEN. A `PULSUSDB_*` variable name spelled as a complete string
// literal in a test source must also be handed, in that same file, to one
// of this crate's gate entry points. A constant holding the name and
// nothing else then has nowhere to live, because the file that defines it
// no longer routes it.
// ---------------------------------------------------------------------

/// This file. Its `PULSUSDB_*` literals are the check's own needles and
/// its exemption entry, and there is no live comparison here for one of
/// them to leave unguarded. Named as a constant, and asserted to resolve,
/// so the exemption cannot quietly stop matching (the convention is
/// `crates/pulsus-server/tests/live_db_naming.rs:547`).
const SELF_PATH: &str = "crates/pulsus-testkit/tests/gated_suite_inventory.rs";

/// The entry points a `PULSUSDB_*` name may be handed to.
///
/// Four, not one, because two kinds of gate exist: an ENDPOINT gate whose
/// value is an address ([`pulsus_testkit::live_endpoint`]) and a BOOLEAN
/// gate whose value is `1`. `PULSUSDB_TEMPO_VECTORS` is the second kind
/// and is correctly routed through `require_live_gate`; demanding
/// `live_endpoint` for it would be demanding the wrong classifier.
const GATE_ENTRY_POINTS: &[&str] = &[
    "live_endpoint(",
    "require_live_endpoint_gate(",
    "require_live_gate(",
    "live_gate_enabled(",
];

/// Floor on the routed population, below the count at the time of writing
/// (46 routed occurrences) with slack for ordinary deletion. Property (5)
/// is an absence property, and an absence property over an empty
/// population passes.
const ROUTED_NAME_FLOOR: usize = 30;

/// Every COMPLETE `PULSUSDB_*` variable name written as a string literal
/// in `src`, as `(line, name)`.
///
/// "Complete" is what keeps the skip messages out: every converted suite
/// prints something like `"PULSUSDB_X_URL unset; skipping …"`, and that
/// literal's content is a sentence, not a name. A literal counts only
/// when everything between the quotes is `PULSUSDB_` followed by at least
/// one more character, all of them `A-Z`, `0-9` or `_`.
fn endpoint_name_literals(src: &str) -> Vec<(usize, String)> {
    let opening = concat!("\"", "PULSUSDB_");
    let mut found = Vec::new();
    let mut from = 0usize;
    while let Some(rel) = src[from..].find(opening) {
        let quote_at = from + rel;
        let name_at = quote_at + 1;
        from = name_at;
        let Some(close) = src[name_at..].find('"') else {
            continue;
        };
        let name = &src[name_at..name_at + close];
        let rest = &name["PULSUSDB_".len()..];
        if !rest.is_empty()
            && rest
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        {
            let line = src[..name_at].bytes().filter(|&b| b == b'\n').count() + 1;
            found.push((line, name.to_string()));
        }
    }
    found.sort();
    found
}

/// `true` when `src` hands `name` to one of [`GATE_ENTRY_POINTS`].
fn is_routed(src: &str, name: &str) -> bool {
    GATE_ENTRY_POINTS
        .iter()
        .any(|entry| src.contains(&format!("{entry}\"{name}\"")))
}

/// Property (5): a `PULSUSDB_*` name written out in a test source is also
/// handed to a gate entry point in that file.
///
/// **What this adds over property (4).** (4) sees one spelling of the
/// read. (5) sees the name wherever it is written, so the constant, the
/// array element and the raw string are all reached — the definition is
/// what they have in common, and the definition is a literal.
///
/// **What still escapes.** One rule, then its measured instances — a list
/// enumerated once beats one that grows an entry per review. This check
/// sees exactly:
///
/// > a COMPLETE `PULSUSDB_*` name, spelled as a string literal, in the
/// > same file that reads it, with every character after the prefix an
/// > ASCII capital, digit or underscore.
///
/// Anything that stops the name being that is invisible here. Each of
/// these was run against the whole binary and left every test green —
/// `5 tests run: 5 passed, 0 skipped`, exit 0:
///
/// * **assembled at run time** — `["PULSUS", "DB_X_URL"].concat()`,
///   `format!("PULSUSDB_{kind}_URL")`. Not hypothetical: two such splits
///   exist in the tree on purpose, in files whose own sweeps must not
///   match themselves, and this check is why one of them had to move its
///   split point.
/// * **assembled at compile time** — `concat!`.
/// * **produced by a macro expansion** — `stringify!(PULSUSDB_X_URL)`
///   inside a `macro_rules!` (issue #523 review round 3). Deliberately not
///   chased: reaching an expansion means expanding, which is a parser, and
///   nobody generates an environment variable's name this way by accident.
/// * **defined in another file or crate** and imported. The scan is per
///   file; a `pub const` in a `tests/common/` module would be seen where
///   it is DEFINED, so it is invisible only when it comes from outside
///   `crates/*/tests`.
/// * **not all upper case** — `const V: &str = "PULSUSDB_X_Url";`. The two
///   properties differ here, which is worth knowing rather than
///   generalising: the same name read DIRECTLY is still caught by (4),
///   which puts no case constraint on the name (`5 tests run: 4 passed,
///   1 failed`, exit 100).
///
/// Two of a different kind:
///
/// * **the routing evidence is text as well.** A file whose only
///   occurrence of `live_endpoint("NAME")` is inside a COMMENT counts as
///   routed, so a comment showing the recommended form excuses a constant
///   read in the same file — measured, `5 tests run: 5 passed`, exit 0.
///   Stripping comments would mean a second copy of a lexer this crate
///   does not have. The direction is at least safe: a comment can only
///   make this check MISS something, never accuse a file wrongly.
/// * **a file that keeps its routed call AND adds an unrouted read of the
///   same name.** The name is routed somewhere, so this passes; (4) then
///   catches it only if the second read spells the literal.
///
/// And the scope, which is part of the claim rather than a hole in it:
/// only `PULSUSDB_`-prefixed names, only `.rs` under `crates/*/tests`,
/// and `#[ignore]`d tests are scanned like any other text, as for (1)-(4).
#[test]
fn every_endpoint_name_written_in_a_test_source_is_routed_through_a_gate() {
    let root = workspace_root();
    assert!(
        root.join(SELF_PATH).is_file(),
        "{SELF_PATH} does not resolve — the one scan exemption names a file that is not \
         there, so it is exempting nothing and this check is scanning itself"
    );

    let mut offenders: Vec<String> = Vec::new();
    let mut routed = 0usize;
    let mut skipped_self = false;

    for path in test_sources() {
        let relative = rel(&path).replace('\\', "/");
        if relative == SELF_PATH {
            skipped_self = true;
            continue;
        }
        let src = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        for (line, name) in endpoint_name_literals(&src) {
            if NON_ENDPOINT_PULSUSDB_VARS.iter().any(|(v, _)| *v == name) {
                continue;
            }
            if is_routed(&src, &name) {
                routed += 1;
            } else {
                offenders.push(format!("{relative}:{line}: {name}"));
            }
        }
    }

    assert!(
        skipped_self,
        "the scan never reached {SELF_PATH}, so its walk no longer covers this crate and \
         every name in it would be unexamined"
    );
    assert!(
        offenders.is_empty(),
        "these test sources write out the name of a gate variable without handing it to a \
         pulsus_testkit gate entry point, so the value can be read some other way and the \
         suite is fail-open again (issue #523 review round 2): {offenders:?}. Pass the name \
         directly — `pulsus_testkit::live_endpoint(\"<VAR>\")` for an address, \
         `require_live_gate(\"<VAR>\")` for a `=1` gate — rather than by way of a constant."
    );
    assert!(
        routed >= ROUTED_NAME_FLOOR,
        "only {routed} gate-variable names are routed through an entry point, below the \
         {ROUTED_NAME_FLOOR} floor — an absence property over an empty population passes, \
         and this is what stops that"
    );
}
