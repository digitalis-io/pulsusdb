//! Issue #29 (M2 `promql-parser` validation spike): replays the vendored
//! upstream Prometheus parser test corpus
//! (`tests/corpus/prometheus-v3.13-parse-cases.jsonl`, see
//! `tests/corpus/PROVENANCE.md`) against `promql_parser::parser::parse` as
//! an **accept/reject** gate, plus a whole-corpus **round-trip invariant**
//! (`parse -> Display -> parse` must yield an equal AST for every case the
//! crate accepts — plan amendment F2). Comparison is deliberately
//! accept/reject only: `promql-parser`'s error text never matches
//! Prometheus's verbatim, so `err_substr` is carried informationally and
//! never gates (architect plan edge-case-3).
//!
//! **Never a silent skip:** every divergence (wrong accept/reject, or a
//! round-trip AST inequality) must have a matching entry in
//! `tests/corpus/expected-divergences.jsonl`, classified into one of three
//! buckets (`irrelevant_to_m2` / `patchable` / `requires_fallback`) — see
//! `docs/decisions/0002-promql-parser-selection.md` for what those buckets
//! mean and the measured decision. An unclassified divergence fails the
//! test; so does an allowlisted case that no longer diverges (stale
//! allowlist / drift, forcing re-classification instead of quietly rotting).
//!
//! **Corpus integrity (plan amendment F1):** before running any case, every
//! test in this file that reads the corpus calls [`assert_corpus_matches_manifest`]
//! *first* — it recomputes the corpus file's SHA-256 and line count and
//! asserts both against `tests/corpus/manifest.json`, so a truncated,
//! edited, or reordered committed corpus fails loudly there rather than
//! silently producing a wrong pass-rate. This is a plain in-test call (not
//! a separate `#[test]` a `--test`/`cargo test <filter>` invocation could
//! skip while still running the replay) — `corpus_matches_its_committed_manifest`
//! below is kept as its own test purely for a fast, isolated failure
//! signal; it does not carry the gate's only enforcement.
//!
//! Run locally to get the full per-case table (transcribed into ADR 0002):
//!
//! ```text
//! cargo test -p pulsus-promql --test upstream_parser_corpus -- --nocapture
//! ```

use std::collections::HashSet;
use std::fmt::Write as _;

use sha2::{Digest, Sha256};

const CORPUS_JSONL: &str = include_str!("corpus/prometheus-v3.13-parse-cases.jsonl");
const MANIFEST_JSON: &str = include_str!("corpus/manifest.json");
const EXPECTED_DIVERGENCES_JSONL: &str = include_str!("corpus/expected-divergences.jsonl");

#[derive(serde::Deserialize)]
struct Case {
    input: String,
    should_fail: bool,
    /// Informational only (architect plan edge-case-3): `promql-parser`'s
    /// error text never matches Prometheus's verbatim, so this never gates
    /// a divergence — it is only ever printed in the per-case table.
    err_substr: Option<String>,
}

#[derive(serde::Deserialize)]
struct Manifest {
    prometheus_tag: String,
    prometheus_sha: String,
    promql_parser_version: String,
    case_count: usize,
    sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum Bucket {
    IrrelevantToM2,
    Patchable,
    RequiresFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum DivergenceKind {
    /// The crate's accept/reject outcome disagrees with `should_fail`.
    AcceptReject,
    /// The crate accepted the input, but `parse -> Display -> parse` did
    /// not reproduce an equal AST (plan amendment F2).
    RoundTrip,
}

#[derive(serde::Deserialize)]
struct AllowedDivergence {
    input: String,
    kind: DivergenceKind,
    bucket: Bucket,
    /// Not machine-checked — printed in the per-case table so the
    /// human-reviewed classification is visible next to each divergence,
    /// not only in `expected-divergences.jsonl` itself.
    construct: String,
    reason: String,
}

fn parse_jsonl<T: serde::de::DeserializeOwned>(text: &str) -> Vec<T> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            serde_json::from_str(l).unwrap_or_else(|e| panic!("invalid JSONL line {l:?}: {e}"))
        })
        .collect()
}

/// Plan amendment F1: a truncated/edited/reordered committed corpus must
/// fail loudly, before a single case is run, rather than silently producing
/// a wrong pass-rate that looks like a real result. This is a plain helper
/// (not a `#[test]`) precisely so it cannot be bypassed by test filtering:
/// every test below that reads [`CORPUS_JSONL`] calls this *first*, as an
/// ordinary function call inside the test body, not as a separate test
/// case a `cargo test <filter>` invocation could select around.
fn assert_corpus_matches_manifest() -> Manifest {
    let manifest: Manifest = serde_json::from_str(MANIFEST_JSON).expect("valid manifest.json");

    let line_count = CORPUS_JSONL
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count();
    assert_eq!(
        line_count, manifest.case_count,
        "corpus line count does not match manifest.json's case_count — the \
         committed corpus was truncated, edited, or reordered; re-run \
         tests/corpus/extract-upstream-cases.py and recommit both files together"
    );

    let mut hasher = Sha256::new();
    hasher.update(CORPUS_JSONL.as_bytes());
    let sha256 = format!("{:x}", hasher.finalize());
    assert_eq!(
        sha256, manifest.sha256,
        "corpus SHA-256 does not match manifest.json — the committed corpus \
         bytes changed without re-running the extractor and updating the manifest"
    );

    manifest
}

/// Standalone, fast, isolated-failure-signal test for the same gate
/// [`assert_corpus_matches_manifest`] enforces inside every other test in
/// this file — kept for a quick "just the integrity check" run, but it is
/// not the gate's only enforcement (see the module doc): filtering this
/// test out (e.g. `cargo test --test upstream_parser_corpus -- replay`)
/// does not skip the check, because [`corpus_replay_and_round_trip_invariant`]
/// calls the same helper itself, first thing.
#[test]
fn corpus_matches_its_committed_manifest() {
    assert_corpus_matches_manifest();
}

/// One row of the full per-case table printed to stdout (transcribed once
/// into ADR 0002 — see the module doc for the `--nocapture` invocation).
struct Row {
    input: String,
    outcome: &'static str,
    detail: String,
}

/// Renders an allowlist entry's bucket/construct/reason for the per-case
/// table — the same classification recorded in `expected-divergences.jsonl`,
/// surfaced next to the case it applies to.
fn describe(d: &AllowedDivergence) -> String {
    format!(
        "bucket={:?} construct={:?} reason={:?}",
        d.bucket, d.construct, d.reason
    )
}

#[test]
fn corpus_replay_and_round_trip_invariant() {
    // Plan amendment F1, gate closed per code review finding 1: the
    // integrity check runs first, as a plain call inside this test body —
    // no test-filtering invocation can select this test while skipping it.
    let manifest = assert_corpus_matches_manifest();
    println!(
        "corpus provenance: prometheus_tag={} prometheus_sha={} promql_parser_version={}",
        manifest.prometheus_tag, manifest.prometheus_sha, manifest.promql_parser_version
    );

    let cases: Vec<Case> = parse_jsonl(CORPUS_JSONL);
    let allowlist: Vec<AllowedDivergence> = parse_jsonl(EXPECTED_DIVERGENCES_JSONL);

    let mut seen_allowlist_entries: HashSet<(String, DivergenceKind)> = HashSet::new();
    let mut unclassified: Vec<String> = Vec::new();
    let mut rows: Vec<Row> = Vec::new();

    let mut agree = 0usize;
    let mut divergent_by_bucket: [usize; 3] = [0; 3]; // irrelevant/patchable/requires_fallback
    let mut round_trip_failures_by_bucket: [usize; 3] = [0; 3];

    let find_allowed = |input: &str, kind: DivergenceKind| {
        allowlist
            .iter()
            .find(|d| d.kind == kind && d.input == input)
    };
    let bucket_idx = |b: Bucket| match b {
        Bucket::IrrelevantToM2 => 0,
        Bucket::Patchable => 1,
        Bucket::RequiresFallback => 2,
    };

    for case in &cases {
        let parsed = promql_parser::parser::parse(&case.input);
        let crate_accepted = parsed.is_ok();

        if crate_accepted != case.should_fail {
            agree += 1;
            rows.push(Row {
                input: case.input.clone(),
                outcome: "agree",
                detail: String::new(),
            });
        } else {
            let direction = if crate_accepted {
                "crate_accepts_upstream_rejects"
            } else {
                "crate_rejects_upstream_accepts"
            };
            // Informational only (never gates — see `Case::err_substr`'s
            // doc comment): included in the table purely for transcription.
            let err_note = case
                .err_substr
                .as_deref()
                .map(|e| format!(", upstream_err={e:?}"))
                .unwrap_or_default();
            match find_allowed(&case.input, DivergenceKind::AcceptReject) {
                Some(d) => {
                    seen_allowlist_entries
                        .insert((case.input.clone(), DivergenceKind::AcceptReject));
                    divergent_by_bucket[bucket_idx(d.bucket)] += 1;
                    rows.push(Row {
                        input: case.input.clone(),
                        outcome: "divergent (allowlisted)",
                        detail: format!("{direction}{err_note} — {}", describe(d)),
                    });
                }
                None => {
                    unclassified.push(format!(
                        "accept_reject divergence not in expected-divergences.jsonl: \
                         input={:?} direction={direction}",
                        case.input
                    ));
                    rows.push(Row {
                        input: case.input.clone(),
                        outcome: "DIVERGENT (UNCLASSIFIED)",
                        detail: format!("{direction}{err_note}"),
                    });
                }
            }
        }

        // Round-trip invariant (F2): every case the crate accepts, however
        // it compared to `should_fail` above, must round-trip through
        // `Display` to an equal AST. Not cancel-safe-relevant (pure, sync,
        // no I/O) — just a plain assertion loop.
        if let Ok(expr) = parsed {
            let rendered = expr.to_string();
            let round_trips = match promql_parser::parser::parse(&rendered) {
                Ok(expr2) => expr2 == expr,
                Err(_) => false,
            };
            if !round_trips {
                match find_allowed(&case.input, DivergenceKind::RoundTrip) {
                    Some(d) => {
                        seen_allowlist_entries
                            .insert((case.input.clone(), DivergenceKind::RoundTrip));
                        round_trip_failures_by_bucket[bucket_idx(d.bucket)] += 1;
                        writeln_row(
                            &mut rows,
                            &case.input,
                            "round-trip FAIL (allowlisted)",
                            &format!("rendered={rendered:?} — {}", describe(d)),
                        );
                    }
                    None => {
                        unclassified.push(format!(
                            "round_trip divergence not in expected-divergences.jsonl: \
                             input={:?} rendered={rendered:?}",
                            case.input
                        ));
                        writeln_row(
                            &mut rows,
                            &case.input,
                            "ROUND-TRIP FAIL (UNCLASSIFIED)",
                            &format!("rendered={rendered:?}"),
                        );
                    }
                }
            }
        }
    }

    // Gate rule 2 (stale allowlist / drift): every allowlisted input must
    // actually have been observed diverging this run, on the axis
    // (`kind`) it's allowlisted for.
    let mut stale: Vec<String> = Vec::new();
    for d in &allowlist {
        if !seen_allowlist_entries.contains(&(d.input.clone(), d.kind)) {
            stale.push(format!(
                "allowlisted {:?} divergence no longer reproduces: input={:?} bucket={:?} \
                 (crate/corpus behaviour changed — re-classify or drop this entry)",
                d.kind, d.input, d.bucket
            ));
        }
    }

    // Full per-case table, for transcription into ADR 0002.
    println!("=== upstream_parser_corpus: full per-case table ===");
    for row in &rows {
        println!(
            "{:<28} {:<45} {}",
            row.outcome,
            truncate(&row.input, 45),
            row.detail
        );
    }
    println!("=== summary ===");
    println!("total cases: {}", cases.len());
    println!("agree: {agree}");
    println!(
        "accept/reject divergences: irrelevant_to_m2={} patchable={} requires_fallback={}",
        divergent_by_bucket[0], divergent_by_bucket[1], divergent_by_bucket[2]
    );
    println!(
        "round-trip failures: irrelevant_to_m2={} patchable={} requires_fallback={}",
        round_trip_failures_by_bucket[0],
        round_trip_failures_by_bucket[1],
        round_trip_failures_by_bucket[2]
    );

    let mut report = String::new();
    if !unclassified.is_empty() {
        writeln!(report, "{} unclassified divergence(s):", unclassified.len()).unwrap();
        for u in &unclassified {
            writeln!(report, "  - {u}").unwrap();
        }
    }
    if !stale.is_empty() {
        writeln!(report, "{} stale allowlist entr(y/ies):", stale.len()).unwrap();
        for s in &stale {
            writeln!(report, "  - {s}").unwrap();
        }
    }
    assert!(report.is_empty(), "\n{report}");
}

fn writeln_row(rows: &mut Vec<Row>, input: &str, outcome: &'static str, detail: &str) {
    rows.push(Row {
        input: input.to_string(),
        outcome,
        detail: detail.to_string(),
    });
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let head: String = s.chars().take(max.saturating_sub(1)).collect();
        format!("{head}…")
    }
}
