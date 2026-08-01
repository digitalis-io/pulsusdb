//! Issue #320 review, finding 3: the migration of the 54 environment-gated
//! suites onto [`pulsus_testkit`] was verified by a one-time sweep, and a
//! one-time sweep protects nothing. This is the standing version — it runs
//! in the hermetic `cargo test --workspace` lane on every push.
//!
//! # What it checks — stated as narrowly as the mechanism allows
//!
//! Two **substring** properties of `crates/*/tests/*.rs`, and nothing
//! more:
//!
//! 1. no file contains the exact byte sequence `env::var("<GATE>`, for
//!    each of this crate's two gate constants;
//! 2. at least [`MIGRATED_SUITE_FLOOR`] files mention `pulsus_testkit::`.
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
//! * **A gated suite with no CI step at all** — issue #323.
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
