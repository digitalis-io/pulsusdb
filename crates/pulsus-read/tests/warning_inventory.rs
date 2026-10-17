//! Issue #277 AC 17 — **the scoping decision is pinned; the reference
//! inventory is a read.**
//!
//! PulsusDB emits two of the reference's three warning message families:
//! family 1 since issue #277, family 2 since issue #605. This suite commits
//! the eleven `AddWarning`/`AddWarnings` call sites that inventory rests on
//! as DATA, asserts our two formatters produce families 1 and 2, and asserts
//! that entry `(d)` of the differential ledger records family 2 as
//! implemented and family 3 as a measured non-emission — parsed out of that
//! entry, not substring-matched against the whole document.
//!
//! # What this pins, and what it does not
//!
//! **It pins OUR decision, not the reference's completeness.** The bound on
//! the reference side has two halves, both stated so the next reader can
//! re-check them rather than trust this file:
//!
//! * a client's `warnings` array is populated only from `metadata.Context`'s
//!   `warnings map[string]struct{}` (`pkg/logqlmodel/metadata/context.go:34
//!   @ grafana/loki v3.7.4 b318f2829f0ae2094ab3a1e90780450e9e4b03be`), or
//!   propagated from an upstream response that was itself populated that
//!   way. That field is **unexported**, so Go's package privacy excludes
//!   every OTHER package;
//! * inside that package the bound is a **hand read**, not a compiler one —
//!   privacy does not exclude the package from itself. At `v3.7.4` the
//!   package is a single non-test file (`git ls-tree -r --name-only v3.7.4
//!   pkg/logqlmodel/metadata` lists `context.go` and `context_test.go`)
//!   carrying exactly two insertion statements, `context.go:84`
//!   (`AddWarning`) and `:140` (`AddWarnings`).
//!
//! **Residue, accepted and recorded.** [`CALL_SITES`] is a read enumeration
//! of a FROZEN tree — our reference is pinned at
//! `b318f2829f0ae2094ab3a1e90780450e9e4b03be`, so nothing can "appear"
//! without a deliberate version bump, but equally **nothing on our side
//! would catch a mis-read of that tree.** This file stores what we
//! concluded; it does not re-derive it. A check asserting the two insertion
//! statements are still the only ones would, against a pinned commit, be
//! asserting its own input.

use std::path::PathBuf;

/// Every `AddWarning`/`AddWarnings` call site under `pkg/`, tests and
/// generated files excluded, at `grafana/loki v3.7.4`
/// `b318f2829f0ae2094ab3a1e90780450e9e4b03be`. Read with:
///
/// ```text
/// git -C <loki> grep -nE '\.(AddWarning|AddWarnings)\(' v3.7.4 -- pkg/ \
///     | grep -v '_test.go' | grep -v '\.pb\.go'
/// ```
///
/// `family` is `0` for a PROPAGATION site — one that forwards an upstream
/// response's already-built array (`batch.Warnings`, `res.Warnings`) and
/// mints no string of its own.
const CALL_SITES: &[(&str, u32, u8)] = &[
    ("pkg/engine/basic_engine.go", 265, 3),
    ("pkg/engine/engine.go", 298, 3),
    ("pkg/engine/handler.go", 532, 3),
    ("pkg/iter/entry_iterator.go", 383, 0),
    ("pkg/iter/sample_iterator.go", 500, 0),
    ("pkg/logql/downstream.go", 487, 0),
    ("pkg/logql/engine.go", 506, 1),
    ("pkg/logql/engine.go", 542, 2),
    ("pkg/logql/engine.go", 582, 2),
    ("pkg/querier/queryrange/limits.go", 507, 1),
    ("pkg/querier/queryrange/limits.go", 512, 2),
];

/// The three message families, by the format string each site builds.
const FAMILIES: &[(u8, &str)] = &[
    (1, "maximum of series (%d) reached for variant (%s)"),
    (
        2,
        "maximum number of series (%d) reached for a single query; returning partial results",
    ),
    (
        3,
        "Query was executed using the new experimental query engine[ and dataobj storage.]",
    ),
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

/// The body of the differential ledger's
/// `variants-label-collision-and-fanout-bounds` entry `(d)`: everything
/// from the `- **(d)` bullet up to the next `###` heading. Sliced so the
/// assertions below cannot be satisfied by text sitting somewhere else in
/// a 2 000-line document.
fn ledger_entry_d() -> String {
    let text =
        std::fs::read_to_string(repo_root().join("docs/benchmarks/logs-differential-ledger.md"))
            .expect("the differential ledger is readable");
    let section_at = text
        .find("### `variants-label-collision-and-fanout-bounds`")
        .expect("the variants section exists");
    let section = &text[section_at..];
    let section_end = section[1..]
        .find("\n### ")
        .map(|i| i + 1)
        .unwrap_or(section.len());
    let section = &section[..section_end];
    let d_at = section
        .find("- **(d)")
        .expect("entry (d) exists in the variants section");
    section[d_at..].to_string()
}

#[test]
fn the_reference_warning_inventory_is_three_families() {
    // The committed enumeration is exactly eleven sites, and every one is
    // classified into a family or marked as propagation.
    assert_eq!(CALL_SITES.len(), 11, "the read enumeration is eleven sites");
    for (file, line, family) in CALL_SITES {
        assert!(
            *family == 0 || FAMILIES.iter().any(|(f, _)| f == family),
            "{file}:{line} names family {family}, which is not in the inventory"
        );
    }
    // Every family is reached by at least one MINTING site — an inventory
    // row nothing produces would be a claim about nothing.
    for (family, message) in FAMILIES {
        assert!(
            CALL_SITES.iter().any(|(_, _, f)| f == family),
            "family {family} ({message}) has no call site"
        );
    }
    // Three propagation sites, which mint no string at all.
    assert_eq!(
        CALL_SITES.iter().filter(|(_, _, f)| *f == 0).count(),
        3,
        "the propagation sites forward an upstream array and mint nothing"
    );

    // OUR implemented families are 1 and 2. Each formatter is the single
    // production source of its text, so this compares against what
    // actually ships.
    let ours = pulsus_read::logql::variant_series_warning(500, 0);
    assert_eq!(ours, "maximum of series (500) reached for variant (0)");
    let family1 = FAMILIES
        .iter()
        .find(|(f, _)| *f == 1)
        .map(|(_, m)| *m)
        .expect("family 1");
    // Family 1's shape with its two verbs substituted.
    assert_eq!(
        ours,
        family1.replace("(%d)", "(500)").replace("(%s)", "(0)")
    );
    // …and specifically NOT family 2's wording, which differs by exactly
    // the word this project has already mistaken once.
    assert!(
        !ours.contains("maximum number of series"),
        "family 1 is `maximum of series`, family 2 is `maximum number of series`"
    );

    // Family 2 (issue #605): the drilldown partial-results warning.
    let ours2 = pulsus_read::logql::warnings::series_cap_partial_warning(500);
    let family2 = FAMILIES
        .iter()
        .find(|(f, _)| *f == 2)
        .map(|(_, m)| *m)
        .expect("family 2");
    assert_eq!(ours2, family2.replace("(%d)", "(500)"));

    // Each family's numbered record in ledger entry (d) carries its own
    // name and its own status: family 1 implemented by #277, family 2 by
    // #605, family 3 a measured NON-EMISSION. Each record is sliced out on
    // its own, so a status written against the wrong family fails.
    let d = ledger_entry_d();
    let flat = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
    for (family, key, status, not) in [
        (
            1u8,
            "maximum of series (%d) reached for variant (%s)",
            "**implemented**",
            "NOT emitted",
        ),
        (
            2,
            "maximum number of series (%d) reached for a single query;",
            "**implemented by #605.**",
            "NOT emitted",
        ),
        (
            3,
            "Query was executed using the new experimental query engine",
            "**NOT emitted.**",
            "implemented",
        ),
    ] {
        let record = flat(ledger_record(&d, family));
        assert!(
            record.contains(&flat(key)),
            "record {family} of entry (d) no longer names family {family}"
        );
        assert!(
            record.contains(status),
            "record {family} of entry (d) must say {status}"
        );
        assert!(
            !record.contains(not),
            "record {family} of entry (d) must not say {not}"
        );
    }
    let family2 = ledger_record(&d, 2);
    // The gating header for family 2.
    assert!(
        family2.contains("X-Query-Tags") && family2.contains("grafana-lokiexplore-app"),
        "record 2 must record what gates family 2"
    );
    // The evidence that the reference keeps no stable 500 where no sort
    // defines them: the dropped ids differed run to run.
    for id in ["184", "396", "370", "42", "303"] {
        assert!(
            family2.contains(id),
            "record 2 must keep the measured dropped ids ({id} missing) — a future \
             reader who sees only \"not implemented\" will otherwise re-derive them"
        );
    }
}

/// Record `n` of entry (d)'s numbered family list: from its `  <n>. `
/// line up to the next record, or to the paragraph after the last one.
fn ledger_record(d: &str, n: u8) -> &str {
    let start = d
        .find(&format!("\n  {n}. `"))
        .unwrap_or_else(|| panic!("entry (d) has no record {n}"));
    let rest = &d[start + 1..];
    let end = rest[1..]
        .find(&format!("\n  {}. `", n + 1))
        .or_else(|| rest.find("\n\n"))
        .map(|i| i + 1)
        .unwrap_or(rest.len());
    &rest[..end]
}

/// The inventory is only meaningful if the ledger entry it points at is
/// the one carrying the range over-acceptance too — the two halves were
/// adjudicated together and a split would let either be deleted alone.
///
/// **Its strength, measured rather than claimed.** Each assertion is a
/// PRESENCE test over entry `(d)`'s body, and that is the whole claim:
/// "entry (d) names X". Verified by breaking it — replacing every
/// ``!ok`` in the ledger with prose reddens the second assertion;
/// editing ONE of the entry's two mentions does not, because the entry
/// still names it. Anyone wanting a stronger property (each mention in
/// its own sentence, say) has to state which sentence, and that is a
/// claim about prose this test deliberately does not make.
#[test]
fn ledger_entry_d_carries_the_range_over_acceptance_as_well() {
    let d = ledger_entry_d();
    assert!(
        d.contains("multiVariantVectorsToSeries"),
        "entry (d) must name the defect by function"
    );
    assert!(
        d.contains("`!ok`"),
        "entry (d) must name the MISSING GUARD, not just call it an off-by-one — \
         that is what stops someone later 'correcting' us toward it"
    );
    assert!(
        d.contains("vectorsToSeriesWithLimit"),
        "entry (d) must name the sibling that has the guard"
    );
    assert!(
        d.contains("over-acceptance"),
        "entry (d) must state the direction of the divergence"
    );
}

// ---------------------------------------------------------------------
// Issue #278 (AC11) — the four traces suites `docs/features.md` names as
// the ANSWER-LEVEL comparison against the reference.
// ---------------------------------------------------------------------

/// The four suites the TraceQL-coverage paragraph names, with the
/// disposition it claims for each. `enforced` is the CI claim: `true`
/// means the paragraph says a workflow runs it with its gate supplied.
const TRACES_ANSWER_LEVEL_SUITES: &[(&str, bool)] = &[
    ("compare_value_differential.rs", true),
    ("traces_search_grouping_differential.rs", true),
    ("traces_log2_reference.rs", false),
];

/// The `docs/features.md` paragraph carrying the `interim == 0` clause and
/// the sentence this test guards — sliced out, so the assertions below
/// cannot be satisfied by text sitting elsewhere in the document.
fn traces_coverage_paragraph() -> String {
    let text = std::fs::read_to_string(repo_root().join("docs/features.md"))
        .expect("docs/features.md is readable");
    let at = text
        .find("**TraceQL conformance carries no tracked interim constructs")
        .expect("the interim == 0 clause exists in docs/features.md");
    // A markdown paragraph: up to the next blank line.
    let start = text[..at].rfind("\n\n").map(|i| i + 2).unwrap_or(0);
    let end = text[at..]
        .find("\n\n")
        .map(|i| at + i)
        .unwrap_or(text.len());
    text[start..end].to_string()
}

/// Issue #278 AC11: the sentence `docs/features.md` gained about
/// answer-level traces comparison names three existing suites, and its
/// **wiring** claim is true of the tree (the nested-set differential and
/// its two gate-variable checks went with that suite, #602 part 2c-2b-1).
///
/// # What this checks, and what it deliberately cannot
///
/// It checks the names and the wiring claim, and nothing about the
/// answers the suites compare.
#[test]
fn the_traces_answer_level_claim_names_only_real_suites() {
    let paragraph = traces_coverage_paragraph();
    let repo = repo_root();

    // 1. Every suite the paragraph names exists under
    //    crates/pulsus-read/tests/.
    for (name, _) in TRACES_ANSWER_LEVEL_SUITES {
        assert!(
            paragraph.contains(name),
            "the TraceQL coverage paragraph no longer names {name} — this test guards a \
             sentence that is not there"
        );
        assert!(
            repo.join("crates/pulsus-read/tests").join(name).is_file(),
            "docs/features.md names {name} as an answer-level traces gate, but \
             crates/pulsus-read/tests/{name} does not exist"
        );
    }

    // 2. The two the paragraph calls CI-enforced appear as `run:` steps —
    //    asserted on the test-binary selector in the `run:` line, not on
    //    the step's `name:`, because the former is what selects the suite.
    let workflow = std::fs::read_to_string(repo.join(".github/workflows/ci.yml"))
        .expect(".github/workflows/ci.yml is readable");
    for (name, enforced) in TRACES_ANSWER_LEVEL_SUITES {
        if !enforced {
            continue;
        }
        let binary = name.trim_end_matches(".rs");
        let needle = format!("--test {binary}");
        let on_a_run_line = workflow
            .lines()
            .any(|l| l.contains(&needle) && l.trim_start().starts_with("run:"));
        assert!(
            on_a_run_line,
            "docs/features.md calls {name} CI-enforced, but no `run:` line in \
             .github/workflows/ci.yml selects it with `{needle}`"
        );
    }
}
