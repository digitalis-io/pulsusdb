//! Issue #64 (M6-01): replays the promqltest corpus — the hand-authored
//! proof files (must be 100% green against today's M2 surface) and the
//! pinned upstream Prometheus v3.13 `.test` files (fetched at test time
//! into a checksum-verified local cache, issue #156; integrity-gated;
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
    "m6_08f_subquery_step_invariant.test",
    "m6_08g_sparse_subquery_union.test",
    "m7_a7_ordered_rangevector_nhcb.test",
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
/// #29 F1 pattern). Since #156 this is also the cache pre-warm command:
/// on a cold cache it fetches every corpus file from the pinned upstream
/// commit and verifies it against the committed manifest.
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
        totals.expect_ordered += run.counts.expect_ordered;
        totals.expect_range_vector += run.counts.expect_range_vector;
        totals.load_with_nhcb += run.counts.load_with_nhcb;
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
    assert!(
        totals.expect_ordered > 0,
        "proof corpus never exercised the block `expect ordered` directive (issue #154)"
    );
    assert!(
        totals.expect_range_vector > 0,
        "proof corpus never exercised `expect range vector` (issue #154)"
    );
    assert!(
        totals.load_with_nhcb > 0,
        "proof corpus never exercised `load_with_nhcb` (issue #154)"
    );
}

/// Issue #154 (plan v2 Δ1): `expect ordered` in a RANGE block PARSES —
/// upstream's expect branch is not instant-gated — and the enforcement
/// happens at COMPARE time: the case (not the file) fails with the
/// oracle's exact wording (`compareResult`, test.go:1252-1254).
#[test]
fn expect_ordered_in_a_range_block_fails_the_case_with_the_pinned_message() {
    let text = "load 1m\n\tm 1 2 3\n\n\
                eval range from 0 to 2m step 1m m\n\texpect ordered\n\tm 1 2 3\n";
    let run = run_file("inline/ordered_range.test", text)
        .expect("a range block with `expect ordered` must parse (oracle-faithful, plan v2 Δ1)");
    assert_eq!(run.cases.len(), 1);
    let case = &run.cases[0];
    assert!(
        !case.passed,
        "a matrix result under ordered must fail the case"
    );
    assert_eq!(
        case.detail, "expected ordered result, but query returned a matrix",
        "the failure must carry the oracle's exact wording"
    );
}

/// Issue #154 (code-review round 1): `expect range vector` in a RANGE
/// block PARSES — upstream's prefix branch (test.go:723-737) is not
/// instant-gated; it REPLACES the block's own from/to/step, so the case
/// executes and compares on the DIRECTIVE grid end-to-end.
#[test]
fn expect_range_vector_in_a_range_block_replaces_the_grid_and_executes() {
    let text = "load 1m\n\tm 0 1 2 3\n\n\
                eval range from 0 to 3m step 3m m\n\
                \texpect range vector from 1m to 2m step 1m\n\tm 1 2\n";
    let run = run_file("inline/range_vector_range_block.test", text)
        .expect("a range block with `expect range vector` must parse (oracle-faithful)");
    assert_eq!(run.cases.len(), 1);
    assert!(
        run.cases[0].passed,
        "the directive grid (1m..2m step 1m) governs both the eval and the compare: {}",
        run.cases[0].detail
    );
    assert_eq!(run.counts.expect_range_vector, 1);
}

/// Issue #154 (plan v2 Δ1): the `msg:`/`regex:` tails of `expect ordered`
/// parse and are DISCARDED (upstream stores an `expectCmd` nothing
/// reads); an INVALID `regex:` pattern is the oracle's own parse error
/// (`parseExpect` → `regexp.Compile`, test.go:543-547).
#[test]
fn expect_ordered_tails_parse_and_run_and_an_invalid_regex_is_a_parse_error() {
    let text = "load 1m\n\tm{env=\"a\"} 1\n\n\
                eval instant at 0 sort(m)\n\texpect ordered msg:ignored tail\n\tm{env=\"a\"} 1\n";
    let run = run_file("inline/ordered_msg.test", text)
        .expect("`expect ordered msg:…` must parse and execute");
    assert!(run.cases[0].passed, "got: {}", run.cases[0].detail);
    assert_eq!(run.counts.expect_ordered, 1);

    let bad = "eval instant at 0 vector(1)\n\texpect ordered regex:(\n\t{} 1\n";
    let err = run_file("inline/ordered_bad_regex.test", bad)
        .expect_err("an invalid regex tail must be a parse error, like the oracle's");
    assert!(err.contains("invalid regex"), "got {err:?}");
}

/// AC5 (issue #86): a non-UTF-8 `expect string` literal now EXECUTES
/// instead of hard-erroring the whole file — before the fix,
/// `go_unquote`'s `String::from_utf8` gate turned this snippet into a
/// grammar (file-fatal) error, never reaching a case report. The case
/// itself still fails: the vendored engine parser decodes the
/// query-text `\xff` escape to a code point (U+00FF `ÿ`), not the raw
/// byte the corpus literal channel now carries losslessly — a normal,
/// ledger-classifiable per-case mismatch, not a driver defect.
#[test]
fn expect_string_with_non_utf8_literal_executes_as_a_classifiable_mismatch() {
    let text = "eval instant at 0 \"\\xff\"\n\texpect string \"\\xff\"\n";
    let run = run_file("inline/non_utf8_expect_string.test", text)
        .expect("go_unquote must no longer hard-error on a non-UTF-8 byte literal (issue #86)");
    assert_eq!(run.cases.len(), 1, "one eval block expected");
    let case = &run.cases[0];
    assert!(
        !case.passed,
        "a non-UTF-8 expect-string literal can never byte-match the engine's UTF-8 \
         QueryValue::String (recorded vendored-parser divergence) — it must fail honestly, \
         not pass"
    );
    assert!(
        case.detail.contains("b\"\\xff\""),
        "mismatch detail must render the non-UTF-8 want losslessly via escape_ascii: {}",
        case.detail
    );
}

/// Issue #155 (AC1): `start_timestamps.test` — the `@st` loader grammar
/// plus the rate/irate/increase/resets start-timestamp semantics —
/// executes from the checksum-verified upstream cache with exactly 18
/// eval cases, ALL passing (a pinpoint diagnostic independent of the
/// aggregate replay below).
#[test]
fn start_timestamps_all_18_rows_pass() {
    let (_, contents) = load_upstream_verified();
    let text = contents
        .get("start_timestamps.test")
        .expect("start_timestamps.test is part of the pinned upstream corpus");
    assert!(
        scan_deferred_directives(text).is_empty(),
        "@st is executable since issue #155 — no deferred directive may remain in the file"
    );
    let run = run_file("upstream/start_timestamps.test", text).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        run.cases.len(),
        18,
        "start_timestamps.test carries exactly 18 eval rows at the pin"
    );
    let failures: Vec<String> = run
        .cases
        .iter()
        .filter(|c| !c.passed)
        .map(|c| {
            format!(
                "upstream/start_timestamps.test:{} `{}` — {}",
                c.line, c.query, c.detail
            )
        })
        .collect();
    assert!(
        failures.is_empty(),
        "all 18 start-timestamp rows must pass:\n{}",
        failures.join("\n")
    );
}

/// AC2 + AC9: the pinned upstream corpus replays with zero unclassified
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

    // Ledger structural rules: entries only ever point at upstream
    // corpus files (the proof corpus must be green outright, never
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
                "eval-divergences.jsonl entry for {:?} — no such upstream corpus file",
                entry.file
            ));
        }
    }

    // Skip-manifest structural rules: known files, known directive names,
    // non-empty activation issues.
    for entry in &skip_manifest.files {
        if !contents.contains_key(&entry.file) {
            problems.push(format!(
                "skip-manifest.json lists {:?}, which is not an upstream corpus file",
                entry.file
            ));
        }
        if entry.blocking_directives.is_empty() && entry.manual_skip.is_none() {
            problems.push(format!(
                "skip-manifest.json entry {:?} lists no blocking directives and no manual_skip",
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
        if let Some(m) = &entry.manual_skip {
            if m.reason.trim().is_empty() {
                problems.push(format!(
                    "skip-manifest.json entry {:?} manual_skip has an empty reason",
                    entry.file
                ));
            }
            if m.activation_issue.trim().is_empty() {
                problems.push(format!(
                    "skip-manifest.json entry {:?} manual_skip has no activation issue",
                    entry.file
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
                let mut reasons: Vec<String> = entry
                    .blocking_directives
                    .iter()
                    .map(|d| format!("{} (activates in {})", d.directive, d.activation_issue))
                    .collect();
                if let Some(m) = &entry.manual_skip {
                    reasons.push(format!(
                        "manual: {} (activates in {})",
                        m.reason, m.activation_issue
                    ));
                }
                println!(
                    "SKIPPED (loud, wholesale) {name}: blocked by {}",
                    reasons.join(", ")
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

// ---------------------------------------------------------------------------
// Issue #156: hermetic fetch-layer guards — scratch cache dir + synthetic
// manifest + `file://` base (curl reads the local path; zero network).
// ---------------------------------------------------------------------------

/// A synthetic pin "sha" — only a cache-subdirectory / URL-path
/// component in these tests, never dereferenced upstream.
const SYNTH_PIN_SHA: &str = "1111111111111111111111111111111111111111";

/// A fresh per-test scratch directory under the OS temp dir (no tempfile
/// dep in this crate; the name is unique per test + process).
fn scratch_dir(test: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir()
        .join("pulsusdb-promqltest-fetch-tests")
        .join(format!("{test}-{}", std::process::id()));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("clean stale scratch dir");
    }
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// Lays out `<root>/<SYNTH_PIN_SHA>/promql/promqltest/testdata/<name>`
/// (the exact URL shape `ensure_cached` fetches) and returns the
/// `file://` base plus a manifest whose entries hash the SOURCE bytes.
fn synthetic_source(
    root: &std::path::Path,
    files: &[(&str, &str)],
) -> (String, driver::UpstreamManifest) {
    let testdata = root
        .join(SYNTH_PIN_SHA)
        .join("promql")
        .join("promqltest")
        .join("testdata");
    std::fs::create_dir_all(&testdata).expect("create synthetic testdata dir");
    let mut entries = Vec::new();
    for (name, text) in files {
        std::fs::write(testdata.join(name), text).expect("write synthetic source file");
        entries.push(driver::UpstreamFileEntry {
            name: (*name).to_string(),
            sha256: driver::sha256_hex(text.as_bytes()),
            lines: text.lines().count(),
        });
    }
    let manifest = driver::UpstreamManifest {
        prometheus_tag: "v-synthetic".to_string(),
        prometheus_sha: SYNTH_PIN_SHA.to_string(),
        files: entries,
        excluded: Vec::new(),
    };
    (format!("file://{}", root.display()), manifest)
}

const SYNTH_FILES: &[(&str, &str)] = &[
    (
        "alpha.test",
        "load 5m\n  alpha 1 2 3\n\neval instant at 5m alpha\n  alpha 2\n",
    ),
    ("beta.test", "load 1m\n  beta 7\n"),
];

/// #156 guard 1: a corrupted cache entry self-heals — `ensure_cached_in`
/// refetches it from the (still-honest) source and returns
/// manifest-verified bytes.
#[test]
fn fetch_self_heals_a_corrupted_cache_entry() {
    let root = scratch_dir("self-heal");
    let (base, manifest) = synthetic_source(&root, SYNTH_FILES);
    let pin_dir = root.join("cache").join(&manifest.prometheus_sha);

    // Cold fill.
    let contents = driver::fetch::ensure_cached_in(&pin_dir, &manifest, &base);
    assert_eq!(contents["alpha.test"], SYNTH_FILES[0].1);

    // Corrupt one cached entry.
    std::fs::write(pin_dir.join("alpha.test"), "corrupted garbage\n").expect("corrupt cache");

    // Self-heal: the corrupted entry is refetched once and verifies.
    let healed = driver::fetch::ensure_cached_in(&pin_dir, &manifest, &base);
    assert_eq!(healed["alpha.test"], SYNTH_FILES[0].1);
    assert_eq!(healed["beta.test"], SYNTH_FILES[1].1);
    assert_eq!(
        driver::sha256_hex(
            std::fs::read_to_string(pin_dir.join("alpha.test"))
                .expect("healed cache file readable")
                .as_bytes()
        ),
        manifest.files[0].sha256,
        "the on-disk cache entry must be restored to manifest-verified bytes"
    );
}

/// #156 guard 2: when the SOURCE bytes do not match the committed
/// manifest (truncation / tampering / upstream ref rewrite), the fetch
/// panics loudly with the URL and both hashes — it never installs the
/// bad bytes.
#[test]
fn fetch_fails_loudly_on_source_checksum_mismatch() {
    let root = scratch_dir("checksum-mismatch");
    let (base, mut manifest) = synthetic_source(&root, SYNTH_FILES);
    let pin_dir = root.join("cache").join(&manifest.prometheus_sha);

    // The manifest expects different bytes than the source serves.
    let expected_sha = driver::sha256_hex(b"the bytes the trust anchor pinned");
    manifest.files[0].sha256 = expected_sha.clone();

    let actual_sha = driver::sha256_hex(SYNTH_FILES[0].1.as_bytes());
    let panic = std::panic::catch_unwind(|| {
        driver::fetch::ensure_cached_in(&pin_dir, &manifest, &base);
    })
    .expect_err("a source/manifest checksum mismatch must panic");
    let msg = panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .expect("panic payload is a string");
    let url = format!(
        "{base}/{SYNTH_PIN_SHA}/promql/promqltest/testdata/{}",
        SYNTH_FILES[0].0
    );
    assert!(msg.contains(&url), "panic must name the URL: {msg}");
    assert!(
        msg.contains(&expected_sha),
        "panic must carry the expected sha: {msg}"
    );
    assert!(
        msg.contains(&actual_sha),
        "panic must carry the actual sha: {msg}"
    );
    assert!(
        !pin_dir.join(SYNTH_FILES[0].0).exists(),
        "mismatching bytes must never be installed into the cache"
    );
}

/// #156 code-review fix (comment 5036732106): a manifest carrying a
/// DUPLICATED entry name passes the bare `files.len() == 21` count check
/// (and would silently dedup in the name-keyed cache map, replaying
/// fewer than 21 distinct files) — the pinned name-set guard must reject
/// it loudly. Hermetic: fixture manifest only, no filesystem, no
/// network.
#[test]
fn manifest_with_a_duplicated_file_name_is_rejected_loudly() {
    let mut files: Vec<driver::UpstreamFileEntry> = driver::UPSTREAM_FILE_NAMES
        .iter()
        .map(|name| driver::UpstreamFileEntry {
            name: (*name).to_string(),
            sha256: "0".repeat(64),
            lines: 1,
        })
        .collect();
    // Duplicate the first pinned name over the second: the count stays
    // 21, so the length check alone cannot catch it.
    files[1].name = files[0].name.clone();
    let manifest = driver::UpstreamManifest {
        prometheus_tag: "v-synthetic".to_string(),
        prometheus_sha: SYNTH_PIN_SHA.to_string(),
        files,
        excluded: Vec::new(),
    };
    assert_eq!(
        manifest.files.len(),
        21,
        "fixture precondition: the duplicate must be invisible to the count check"
    );

    let panic = std::panic::catch_unwind(|| {
        driver::assert_upstream_manifest_file_set(&manifest);
    })
    .expect_err("a duplicated manifest file name must be rejected loudly");
    let msg = panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .expect("panic payload is a string");
    assert!(
        msg.contains("exactly the 21 pinned upstream .test file names"),
        "rejection must name the pinned-set guard: {msg}"
    );
}

/// #156 AC-11 (plan v2 Δ1): concurrent fetchers inside ONE process
/// (plain multi-threaded `cargo test`, not just nextest's
/// process-per-test) never corrupt the cache — per-writer-unique temp
/// names + atomic rename. All writers return manifest-verified contents
/// and the pin dir ends up holding exactly the manifest file set with
/// zero leftover `.tmp-*` files.
#[test]
fn fetch_concurrent_writers_in_one_process_do_not_corrupt_the_cache() {
    let root = scratch_dir("concurrent-writers");
    let (base, manifest) = synthetic_source(&root, SYNTH_FILES);
    let pin_dir = root.join("cache").join(&manifest.prometheus_sha);

    std::thread::scope(|s| {
        let handles: Vec<_> = (0..8)
            .map(|_| s.spawn(|| driver::fetch::ensure_cached_in(&pin_dir, &manifest, &base)))
            .collect();
        for handle in handles {
            let contents = handle.join().expect("writer thread must not panic");
            for (name, text) in SYNTH_FILES {
                assert_eq!(contents[*name], *text, "writer returned corrupted {name}");
            }
        }
    });

    let mut on_disk: Vec<String> = std::fs::read_dir(&pin_dir)
        .expect("pin dir listable")
        .map(|e| {
            e.expect("readable dir entry")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .collect();
    on_disk.sort();
    let mut expected: Vec<String> = manifest.files.iter().map(|f| f.name.clone()).collect();
    expected.sort();
    assert_eq!(
        on_disk, expected,
        "pin dir must contain exactly the manifest file set — no leftover .tmp-* files"
    );
    for entry in &manifest.files {
        let text = std::fs::read_to_string(pin_dir.join(&entry.name)).expect("cache file readable");
        assert_eq!(
            driver::sha256_hex(text.as_bytes()),
            entry.sha256,
            "{} cache bytes must verify against the manifest",
            entry.name
        );
    }
}
