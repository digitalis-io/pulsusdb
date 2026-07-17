//! Issue #64 (M6-01): replays the promqltest corpus — the hand-authored
//! proof files (must be 100% green against today's M2 surface) and the
//! vendored upstream Prometheus v3.13 `.test` files (integrity-gated;
//! every failing case must be classified by the coverage-manifest oracle
//! or by `corpus/eval-divergences.jsonl`; every deferred-directive file
//! must be skip-manifested loudly — never a silent skip).
//!
//! Run with `--nocapture` for the full per-file/per-case table:
//!
//! ```text
//! cargo test -p pulsus-promql --test promqltest_corpus -- --nocapture
//! ```

#[path = "promqltest/mod.rs"]
mod driver;

use std::collections::BTreeSet;

use driver::grammar::{DeferredDirective, scan_deferred_directives};
use driver::runner::{DirectiveCounts, run_file};
use driver::{CoverageManifest, SkipManifest, load_upstream_verified};

/// The committed proof corpus, in replay order. A file added on disk but
/// not listed here fails `proof_corpus_files_match_the_directory`.
const PROOF_FILES: &[&str] = &[
    "grammar.test",
    "m2_functions.test",
    "m2_aggregations.test",
    "m2_binops.test",
    "m6_02_math_trig.test",
    "m6_03_time_date.test",
    "m6_04_range_functions.test",
    "m6_05_label_sort_absence.test",
    "m6_05b_info.test",
    "m6_06_aggregation_operators.test",
    "m6_07_operator_matrix.test",
    "m6_08a_at_subquery.test",
    "m6_08b_duration_expressions.test",
    "m6_08c_utf8_selectors.test",
    "m6_08d_directives_delayed_name.test",
    "m6_08e_step_invariant.test",
];

fn proof_dir() -> std::path::PathBuf {
    driver::base_dir().join("corpus").join("proof")
}

#[test]
fn proof_corpus_files_match_the_directory() {
    let mut on_disk: Vec<String> = std::fs::read_dir(proof_dir())
        .expect("corpus/proof exists")
        .map(|e| e.expect("readable dir entry").file_name())
        .filter_map(|n| {
            let n = n.to_string_lossy().to_string();
            n.ends_with(".test").then_some(n)
        })
        .collect();
    on_disk.sort();
    let mut listed: Vec<String> = PROOF_FILES.iter().map(|s| s.to_string()).collect();
    listed.sort();
    assert_eq!(
        on_disk, listed,
        "proof .test files on disk must exactly match PROOF_FILES"
    );
}

/// Fast, isolated integrity signal (the replay test re-runs the same gate
/// itself, first thing — filtering this test out cannot bypass it, the
/// #29 F1 pattern).
#[test]
fn upstream_corpus_matches_its_integrity_manifest() {
    load_upstream_verified();
}

/// AC1: the proof corpus is 100% green and exercises every executed
/// directive — `clear`, `load` (all value notations live in
/// `grammar.test`), `eval instant`, `eval range`, `eval_ordered`,
/// `eval_fail` with both `expected_fail_message` and
/// `expected_fail_regexp`.
#[test]
fn proof_corpus_is_fully_green_and_exercises_every_executed_directive() {
    let mut totals = DirectiveCounts::default();
    let mut failures: Vec<String> = Vec::new();

    for name in PROOF_FILES {
        let path = proof_dir().join(name);
        let text = driver::read_file(&path);
        assert!(
            scan_deferred_directives(&text).is_empty(),
            "proof file {name} must not use deferred directives"
        );
        let run = run_file(&format!("proof/{name}"), &text).unwrap_or_else(|e| panic!("{e}"));
        for case in &run.cases {
            if !case.passed {
                failures.push(format!(
                    "proof/{name}:{} `{}` — {}",
                    case.line, case.query, case.detail
                ));
            }
        }
        totals.clear += run.counts.clear;
        totals.load += run.counts.load;
        totals.eval_instant += run.counts.eval_instant;
        totals.eval_instant_bare += run.counts.eval_instant_bare;
        totals.eval_range += run.counts.eval_range;
        totals.eval_ordered += run.counts.eval_ordered;
        totals.eval_fail += run.counts.eval_fail;
        totals.fail_message += run.counts.fail_message;
        totals.fail_regexp += run.counts.fail_regexp;
        totals.expect_fail += run.counts.expect_fail;
        totals.expect_fail_tagged += run.counts.expect_fail_tagged;
        totals.expect_string += run.counts.expect_string;
    }

    assert!(
        failures.is_empty(),
        "the proof corpus must be 100% green against the M2 surface:\n{}",
        failures.join("\n")
    );

    assert!(totals.clear > 0, "proof corpus never exercised `clear`");
    assert!(totals.load > 0, "proof corpus never exercised `load`");
    assert!(
        totals.eval_instant > 0,
        "proof corpus never exercised `eval instant`"
    );
    assert!(
        totals.eval_instant_bare > 0,
        "proof corpus never exercised the bare `eval instant <expr>` form (no `at` clause)"
    );
    assert!(
        totals.eval_range > 0,
        "proof corpus never exercised `eval range`"
    );
    assert!(
        totals.eval_ordered > 0,
        "proof corpus never exercised `eval_ordered`"
    );
    assert!(
        totals.eval_fail > 0,
        "proof corpus never exercised `eval_fail`"
    );
    assert!(
        totals.fail_message > 0,
        "proof corpus never exercised `expected_fail_message`"
    );
    assert!(
        totals.fail_regexp > 0,
        "proof corpus never exercised `expected_fail_regexp`"
    );
    assert!(
        totals.expect_fail > 0,
        "proof corpus never exercised the block `expect fail` directive (issue #86)"
    );
    assert!(
        totals.expect_fail_tagged > 0,
        "proof corpus never exercised `expect fail` with an inline msg:/regex: tail"
    );
    assert!(
        totals.expect_string > 0,
        "proof corpus never exercised `expect string`"
    );
}

/// AC2 + AC9: the vendored upstream corpus replays with zero unclassified
/// failures, the skip-manifest matches reality in both directions, and
/// the eval-divergence ledger carries no stale entries.
#[test]
fn upstream_corpus_replay_classifies_every_failure() {
    // Integrity first, inside this test body (the #29 F1 rule: test
    // filtering can never run the replay without the gate).
    let (manifest, contents) = load_upstream_verified();
    println!(
        "upstream corpus provenance: prometheus_tag={} prometheus_sha={}",
        manifest.prometheus_tag, manifest.prometheus_sha
    );

    let skip_manifest = SkipManifest::load();
    let coverage = CoverageManifest::load();
    let ledger = driver::load_ledger();

    let mut problems: Vec<String> = Vec::new();

    // Ledger structural rules: entries only ever point at vendored
    // upstream files (the proof corpus must be green outright, never
    // allowlisted), at files that exist.
    for entry in &ledger {
        let Some(name) = entry.file.strip_prefix("upstream/") else {
            problems.push(format!(
                "eval-divergences.jsonl entry for {:?} — only upstream/ files may carry \
                 residual divergences",
                entry.file
            ));
            continue;
        };
        if !contents.contains_key(name) {
            problems.push(format!(
                "eval-divergences.jsonl entry for {:?} — no such vendored file",
                entry.file
            ));
        }
    }

    // Skip-manifest structural rules: known files, known directive names,
    // non-empty activation issues.
    for entry in &skip_manifest.files {
        if !contents.contains_key(&entry.file) {
            problems.push(format!(
                "skip-manifest.json lists {:?}, which is not a vendored file",
                entry.file
            ));
        }
        if entry.blocking_directives.is_empty() {
            problems.push(format!(
                "skip-manifest.json entry {:?} lists no blocking directives",
                entry.file
            ));
        }
        for d in &entry.blocking_directives {
            if DeferredDirective::from_name(&d.directive).is_none() {
                problems.push(format!(
                    "skip-manifest.json entry {:?} names unknown directive {:?}",
                    entry.file, d.directive
                ));
            }
            if d.activation_issue.trim().is_empty() {
                problems.push(format!(
                    "skip-manifest.json entry {:?} directive {:?} has no activation issue",
                    entry.file, d.directive
                ));
            }
        }
    }

    let mut used_ledger_entries: BTreeSet<usize> = BTreeSet::new();
    let find_ledger = |file: &str, line: usize, query: &str| -> Option<usize> {
        ledger
            .iter()
            .position(|e| e.file == file && e.line == line && e.query == query)
    };

    let mut executed_files = 0usize;
    let mut skipped_files = 0usize;
    let mut passed = 0usize;
    let mut expected_fail = 0usize;
    let mut allowlisted = 0usize;

    for (name, text) in &contents {
        let deferred = scan_deferred_directives(text);
        match skip_manifest.entry(name) {
            Some(entry) => {
                // AC9 drift rule: a skip-manifested file whose blocking
                // directives are gone must be de-listed and activated.
                let listed: BTreeSet<&str> = entry
                    .blocking_directives
                    .iter()
                    .map(|d| d.directive.as_str())
                    .collect();
                let present: BTreeSet<&str> = deferred.iter().map(|d| d.name()).collect();
                if listed != present {
                    problems.push(format!(
                        "skip-manifest drift for {name}: listed blocking directives \
                         {listed:?} but the file actually uses {present:?} — de-list and \
                         activate (or re-classify) this file"
                    ));
                    continue;
                }
                skipped_files += 1;
                println!(
                    "SKIPPED (loud, wholesale) {name}: blocked by {}",
                    entry
                        .blocking_directives
                        .iter()
                        .map(|d| format!("{} (activates in {})", d.directive, d.activation_issue))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            None => {
                if !deferred.is_empty() {
                    problems.push(format!(
                        "{name} uses deferred directives {:?} but is not in \
                         skip-manifest.json — a file must be executed or loudly skipped, \
                         never silently dropped",
                        deferred.iter().map(|d| d.name()).collect::<Vec<_>>()
                    ));
                    continue;
                }
                executed_files += 1;
                let ledger_file = format!("upstream/{name}");
                let run = match run_file(&ledger_file, text) {
                    Ok(run) => run,
                    Err(e) => {
                        problems.push(format!("grammar error replaying {name}: {e}"));
                        continue;
                    }
                };
                for case in &run.cases {
                    let ledger_hit = find_ledger(&ledger_file, case.line, &case.query);
                    if case.passed {
                        passed += 1;
                        if let Some(idx) = ledger_hit {
                            problems.push(format!(
                                "stale eval-divergences.jsonl entry: {ledger_file}:{} `{}` \
                                 now passes (construct {:?}) — drop or re-classify it",
                                case.line, case.query, ledger[idx].construct
                            ));
                        }
                        continue;
                    }
                    // Failing case: the manifest oracle classifies first;
                    // the ledger is the residual escape valve.
                    let oracle = case
                        .constructs
                        .as_ref()
                        .and_then(|c| coverage.classify_expected_fail(c));
                    if let Some(reason) = oracle {
                        expected_fail += 1;
                        println!(
                            "expected-fail {ledger_file}:{} `{}` — {reason}",
                            case.line, case.query
                        );
                        if let Some(idx) = ledger_hit {
                            problems.push(format!(
                                "redundant eval-divergences.jsonl entry: {ledger_file}:{} \
                                 `{}` is already classified by the manifest ({}) — drop the \
                                 ledger entry",
                                case.line, case.query, ledger[idx].construct
                            ));
                        }
                        continue;
                    }
                    match ledger_hit {
                        Some(idx) => {
                            allowlisted += 1;
                            used_ledger_entries.insert(idx);
                            println!(
                                "allowlisted divergence {ledger_file}:{} `{}` — construct={:?} \
                                 reason={:?}",
                                case.line, case.query, ledger[idx].construct, ledger[idx].reason
                            );
                        }
                        None => {
                            problems.push(format!(
                                "UNCLASSIFIED divergence {ledger_file}:{} `{}` — {}",
                                case.line, case.query, case.detail
                            ));
                        }
                    }
                }
            }
        }
    }

    // Ledger staleness (the #29 rule 2): every committed entry must have
    // been consumed by a failing, otherwise-unclassified case this run.
    for (idx, entry) in ledger.iter().enumerate() {
        if !used_ledger_entries.contains(&idx) && entry.file.starts_with("upstream/") {
            let already_reported = problems.iter().any(|p| {
                p.contains("stale eval-divergences") && p.contains(&entry.query)
                    || p.contains("redundant eval-divergences") && p.contains(&entry.query)
            });
            if !already_reported {
                problems.push(format!(
                    "stale eval-divergences.jsonl entry: {}:{} `{}` did not match any \
                     failing case this run",
                    entry.file, entry.line, entry.query
                ));
            }
        }
    }

    println!("=== upstream replay summary ===");
    println!(
        "files: executed={executed_files} skipped={skipped_files} (of {})",
        contents.len()
    );
    println!(
        "cases: passed={passed} expected-fail(manifest)={expected_fail} \
         allowlisted(ledger)={allowlisted}"
    );

    assert!(
        problems.is_empty(),
        "\n{} problem(s):\n{}",
        problems.len(),
        problems.join("\n")
    );
}
