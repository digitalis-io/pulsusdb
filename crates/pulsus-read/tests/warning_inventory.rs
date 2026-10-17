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
    ("nestedset_value_differential.rs", false),
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
/// answer-level traces comparison names four real suites, and its two
/// **wiring** claims are true of the tree.
///
/// # What this checks, and what it deliberately cannot
///
/// It checks the wiring claims, not the disposition claim. Nothing in CI
/// can assert "fails today by design" without running a red suite in CI,
/// which is precisely what we are not doing; that assertion rests on the
/// #278 transcript and on the #185 reference in the paragraph itself.
///
/// The third assertion is a **set equality**, not an absence grep. The
/// earlier form asserted the string `nestedset` appeared nowhere under
/// `.github/` — which passes while a workflow supplies either
/// `PULSUSDB_NESTEDSET_*` variable, because that spelling is upper case.
/// The set form names the offending path instead. (The two variable names
/// are never written out in this file: it sweeps the whole tracked tree
/// including itself, so a literal here would make it a second match and
/// the test would fail on its own source. They are assembled from halves
/// below.)
///
/// **Related mechanism, not duplicated here.**
/// `crates/pulsus-testkit/tests/gated_suite_inventory.rs` already carries
/// `nestedset_value_differential` in its `DELIBERATELY_UNWIRED` list and
/// already reddens both ways on "has a `--test` step". This test asserts
/// the thing that check cannot see: that the GATE VARIABLES are never
/// supplied.
///
/// **Coupling, deliberate.** If #185 lands and wires the suite in, this
/// test reddens — which is the correct signal, because the docs sentence
/// must then be updated.
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

    // 3. The paragraph's claim, checked exactly as it is written: no
    //    WORKFLOW supplies the gate.
    //
    //    This runs BEFORE the broad tracked-tree sweep below, and the order
    //    is the point. A workflow file is also a tracked file, so with the
    //    sweep first this assertion could never be the one to fire and
    //    would be a gate nobody could break — the shape this repo keeps
    //    losing to. Checked first, it fails on its own terms, and the sweep
    //    that follows is the wider net for the suppliers a workflow scan
    //    cannot see (a script a `run:` step invokes, a composite action).
    //
    //    The split point moved in issue #523 review round 2: it used to
    //    fall after `PULSUSDB_NESTED`, which left this file carrying a
    //    fragment that reads as a complete variable name, and
    //    `crates/pulsus-testkit/tests/gated_suite_inventory.rs`'s property
    //    (5) — a gate variable's name may not be written in a test source
    //    without being handed to a gate entry point — reported it. Moving
    //    the point serves the same purpose: neither piece is a name.
    let diff_url = ["PULSUS", "DB_NESTEDSET_DIFF_URL"].concat();
    let otlp_url = ["PULSUS", "DB_NESTEDSET_OTLP_URL"].concat();
    let workflows = repo.join(".github/workflows");
    let mut workflow_suppliers: Vec<String> = Vec::new();
    let mut workflow_files = 0usize;
    for entry in std::fs::read_dir(&workflows).expect(".github/workflows is readable") {
        let path = entry.expect("workflow dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("yml")
            && path.extension().and_then(|e| e.to_str()) != Some("yaml")
        {
            continue;
        }
        workflow_files += 1;
        let text = std::fs::read_to_string(&path).expect("workflow readable");
        if text.contains(diff_url.as_str()) || text.contains(otlp_url.as_str()) {
            workflow_suppliers.push(
                path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("<unnamed>")
                    .to_string(),
            );
        }
    }
    assert!(
        workflow_files > 0,
        "no workflow files were read, so this assertion checked nothing"
    );
    assert!(
        workflow_suppliers.is_empty(),
        "docs/features.md says no workflow supplies the nested-set differential's gate, but \
         these workflows name one of its variables: {workflow_suppliers:?}"
    );

    // 4. The nested-set suite's two gate variables are named by a CLOSED
    //    set of tracked files. Any other mention anywhere in the tree (a
    //    workflow `env:` block, a composite action, a script a `run:` step
    //    invokes) falsifies the paragraph's "no workflow supplies its
    //    gate" and is named here.
    //
    //    Two files are sanctioned, not one (issue #458). The suite's own
    //    source, and the differential ledger, whose
    //    `traceql-differential-legs-skip-green-on-a-missing-endpoint` entry
    //    names both variables in full so the wiring hole it records can be
    //    acted on without going and looking them up. **Documenting a
    //    variable is not supplying it**, and the distinction is not left to
    //    the reader: the workflow-scoped assertion below checks the
    //    paragraph's actual claim directly, so widening this list by one
    //    documentation file cannot admit a workflow.
    //
    //    Needles built by concatenation so this file is not itself a
    //    match — the precedent is
    //    crates/pulsus-read/tests/detected_labels_cardinality.rs:409-423.
    let expected = [
        "crates/pulsus-read/tests/nestedset_value_differential.rs",
        "docs/benchmarks/traces-differential-ledger.md",
    ];

    // Every TRACKED file, from git: a directory walk would also read a
    // developer's untracked scratch files. A failure to list is a hard
    // failure, never a quiet empty sweep.
    let listing = std::process::Command::new("git")
        .arg("-C")
        .arg(&repo)
        .args(["ls-files", "-z"])
        .output()
        .expect("git ls-files must run: this gate's domain is the tracked tree");
    assert!(
        listing.status.success(),
        "git ls-files failed: {}",
        String::from_utf8_lossy(&listing.stderr)
    );
    let mut unreadable: Vec<String> = Vec::new();
    let mut suppliers_diff: Vec<String> = Vec::new();
    let mut suppliers_otlp: Vec<String> = Vec::new();
    let mut scanned = 0usize;
    for rel in String::from_utf8_lossy(&listing.stdout).split('\0') {
        if rel.is_empty() {
            continue;
        }
        let Ok(bytes) = std::fs::read(repo.join(rel)) else {
            unreadable.push(rel.to_string());
            continue;
        };
        scanned += 1;
        let text = String::from_utf8_lossy(&bytes);
        if text.contains(diff_url.as_str()) {
            suppliers_diff.push(rel.to_string());
        }
        if text.contains(otlp_url.as_str()) {
            suppliers_otlp.push(rel.to_string());
        }
    }
    assert!(
        unreadable.is_empty(),
        "git listed paths this sweep could not open, so its domain is not the tracked tree: \
         {unreadable:?}"
    );
    assert!(
        scanned > 1_000,
        "the sweep read only {scanned} tracked files"
    );
    let sanctioned: Vec<String> = expected.iter().map(|p| (*p).to_string()).collect();
    assert_eq!(
        suppliers_diff, sanctioned,
        "docs/features.md says no workflow supplies the nested-set differential's gate, but \
         {diff_url} appears in these tracked files (only the suite's own source and the \
         differential ledger may name it)"
    );
    assert_eq!(
        suppliers_otlp, sanctioned,
        "docs/features.md says no workflow supplies the nested-set differential's gate, but \
         {otlp_url} appears in these tracked files (only the suite's own source and the \
         differential ledger may name it)"
    );
}
