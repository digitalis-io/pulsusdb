//! Issue #352 step 2/3: which corpus rows a live replay may compare, and
//! the coverage figure that keeps `captured` from being read as
//! `replayed`.
//!
//! **What a replay does and does not establish.** It verifies the VALUE:
//! it pushes a row's `load` dataset to the digest-pinned reference, runs
//! the row's query, and compares. It does NOT verify the row's
//! provenance marker — it *reads* the marker to decide what it may
//! compare. A hand-authored value that happens to be correct passes; a
//! genuine capture that has gone stale fails. See
//! `logqltest/PROVENANCE.md` §"Provenance markers" for why no mechanism
//! can close that gap.
//!
//! **`captured` is not `replayed`.** 1135 directives claim capture; far
//! fewer can be replayed, and the difference is enumerated here by
//! reason rather than left as a shortfall. A coverage number that omits
//! its own exclusions is the defect this issue is about.

mod logqltest;

use std::collections::BTreeMap;

use logqltest::runner::{Command, EvalMode};

/// Why a directive is not replayable against the live reference.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Unreplayed {
    /// The expectation records what PulsusDB does INSTEAD of the
    /// reference (a ledgered divergence). Replaying it would compare the
    /// reference against a value defined as not-the-reference; "fixing"
    /// a mismatch would erase the divergence. Replay and comparator
    /// would be one source.
    PinnedDivergence(String),
    /// `eval_fail`: the expectation is PulsusDB's own error text, not a
    /// reference value. The reference's rejection is pinned by the
    /// registry disposition and the status-only differential instead.
    OurErrorText,
    /// The file's capture needed a container config the CI oracle does
    /// not run, so a replay against that oracle would be inconclusive.
    ConfigDelta(&'static str),
    /// Not a capture claim at all.
    NotCaptured(&'static str),
}

/// A directive's replay disposition.
#[derive(Debug)]
struct Row {
    file: String,
    line: usize,
    verdict: Result<(), Unreplayed>,
}

/// Files whose committed capture required a container config beyond the
/// bare image, with whether `ci/logql/config.yaml` — the config the CI
/// differential oracle runs — actually supplies it.
const CONFIG_DELTA_FILES: &[(&str, &str)] = &[
    (
        "b10_approx_topk.test",
        "shard_aggregations + protobuf frontend (in ci/logql/config.yaml)",
    ),
    (
        "b13_variants.test",
        "enable_multi_variant_queries (in ci/logql/config.yaml)",
    ),
    (
        "b17_grouping_dedup.test",
        "enable_multi_variant_queries (in ci/logql/config.yaml)",
    ),
    (
        "b12_error_pair_model.test",
        "discover_log_levels: false (NOT in ci/logql/config.yaml)",
    ),
    (
        "b14_detected_fields.test",
        "discover_log_levels: false + /detected_fields (NOT in ci/logql/config.yaml)",
    ),
];

/// Sorts every corpus directive into replayable / not, by the rules in
/// [`Unreplayed`].
///
/// **A known unquantified class sits INSIDE the replayable set.** Some
/// rows are ones the reference cannot reproduce against itself — Go
/// map-order in its own output, same-timestamp tie order, and
/// `approx_topk` above its retention cap, where the corpus deliberately
/// pins a deterministic choice the reference does not make. Those rows
/// are counted as replayable here and should not be, because a replay of
/// them compares against something the reference will not repeat.
///
/// **No list of them exists, deliberately.** Nothing in the corpus marks
/// them and they cannot be identified by inspection; fabricating a list
/// would be worse than admitting there is none, because a wrong
/// exclusion list reads exactly like a right one. The live replay is
/// expected to surface them as flapping failures — a row that passes and
/// fails across runs with no change to either side — and they should be
/// marked from that evidence, not from a guess. Until then the 948 is an
/// upper bound on what is genuinely replayable, not a measurement of it.
fn classify() -> Vec<Row> {
    let mut out = Vec::new();
    let dir = logqltest::corpus_dir();
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("corpus dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "test"))
        .collect();
    files.sort();
    for path in files {
        let name = path
            .file_name()
            .expect("name")
            .to_string_lossy()
            .into_owned();
        let text = logqltest::read_file(&path);
        let cmds = logqltest::runner::parse_file(&name, &text).expect("corpus parses");
        // Provenance in force per directive, in file order.
        let marks = provenance_by_directive(&text);
        let mut mi = 0usize;
        let delta = CONFIG_DELTA_FILES.iter().find(|(f, _)| *f == name);
        for c in &cmds {
            let Command::Eval(e) = c else { continue };
            let mark = marks.get(mi).cloned().unwrap_or_default();
            mi += 1;
            let verdict = if let Some(id) = mark
                .strip_prefix("divergence(")
                .and_then(|s| s.strip_suffix(')'))
            {
                Err(Unreplayed::PinnedDivergence(id.to_string()))
            } else if e.mode == EvalMode::Fail {
                Err(Unreplayed::OurErrorText)
            } else if mark == "derived" {
                Err(Unreplayed::NotCaptured("derived"))
            } else if mark.starts_with("ported(") {
                Err(Unreplayed::NotCaptured("ported"))
            } else if let Some((_, why)) = delta {
                Err(Unreplayed::ConfigDelta(why))
            } else {
                Ok(())
            };
            out.push(Row {
                file: name.clone(),
                line: e.line,
                verdict,
            });
        }
    }
    out
}

/// The provenance marker in force for each `eval` directive, in file
/// order — the same rule `logqltest_provenance.rs` check E applies.
fn provenance_by_directive(text: &str) -> Vec<String> {
    let lines: Vec<&str> = text.lines().collect();
    let mut file_default = String::new();
    let mut pending: Option<String> = None;
    let mut out = Vec::new();
    for (i, raw) in lines.iter().enumerate() {
        if let Some(rest) = raw.trim().strip_prefix("# provenance:") {
            let v = rest.trim().to_string();
            if lines.get(i + 1).is_some_and(|l| l.starts_with("eval")) {
                pending = Some(v);
            } else if file_default.is_empty() {
                file_default = v;
            }
            continue;
        }
        if raw.starts_with("eval") {
            out.push(pending.take().unwrap_or_else(|| file_default.clone()));
        }
    }
    out
}

fn reason_key(u: &Unreplayed) -> String {
    match u {
        Unreplayed::PinnedDivergence(_) => "pinned-divergence".to_string(),
        Unreplayed::OurErrorText => "our-error-text (eval_fail)".to_string(),
        Unreplayed::ConfigDelta(_) => "config-delta file".to_string(),
        Unreplayed::NotCaptured(k) => format!("not a capture claim ({k})"),
    }
}

/// **The coverage figure, with its exclusions named.** Pinned, so
/// `replayable` cannot drift and cannot be quoted as if it were
/// `captured`.
#[test]
fn replay_coverage_is_pinned_and_names_every_exclusion() {
    let rows = classify();
    assert!(
        rows.len() > 900,
        "expected the whole corpus, got {}",
        rows.len()
    );

    let replayable = rows.iter().filter(|r| r.verdict.is_ok()).count();
    let mut by_reason: BTreeMap<String, usize> = BTreeMap::new();
    for r in &rows {
        if let Err(u) = &r.verdict {
            *by_reason.entry(reason_key(u)).or_default() += 1;
        }
    }
    // Name one example row per reason: a count with no locator is the
    // kind of figure this issue exists to stop.
    let mut example: BTreeMap<String, String> = BTreeMap::new();
    for r in &rows {
        if let Err(u) = &r.verdict {
            let k = reason_key(u);
            example
                .entry(k)
                .or_insert_with(|| format!("{}:{}", r.file, r.line));
        }
    }
    let summary: Vec<String> = by_reason
        .iter()
        .map(|(k, v)| format!("{k}={v} (e.g. {})", example[k]))
        .collect();
    assert_eq!(
        (replayable, rows.len()),
        (REPLAYABLE, TOTAL_DIRECTIVES),
        "replay coverage moved — update the pins AND the figure quoted on issue #352. \
         Unreplayed by reason: {}",
        summary.join(", ")
    );
    assert_eq!(
        by_reason
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", "),
        UNREPLAYED_BY_REASON,
        "the unreplayed breakdown moved"
    );
}

/// Every file named as needing a config delta must actually exist, and
/// every file whose header mentions a config requirement must be named
/// here — so the exclusion list cannot silently miss one.
#[test]
fn the_config_delta_file_list_matches_the_corpus_headers() {
    let dir = logqltest::corpus_dir();
    let mut headers_mentioning: Vec<String> = Vec::new();
    for e in std::fs::read_dir(&dir).expect("corpus dir").flatten() {
        let p = e.path();
        if p.extension().is_none_or(|x| x != "test") {
            continue;
        }
        let text = logqltest::read_file(&p);
        let head: String = text
            .lines()
            .take_while(|l| l.starts_with('#'))
            .collect::<Vec<_>>()
            .join(" ");
        if head.contains("shard_aggregations")
            || head.contains("enable_multi_variant_queries")
            || head.contains("discover_log_levels")
            || head.contains("config delta")
            || head.contains("CONFIG DELTA")
        {
            headers_mentioning.push(p.file_name().expect("name").to_string_lossy().into_owned());
        }
    }
    headers_mentioning.sort();
    let mut named: Vec<String> = CONFIG_DELTA_FILES
        .iter()
        .map(|(f, _)| f.to_string())
        .collect();
    named.sort();
    assert_eq!(
        headers_mentioning, named,
        "a corpus header declares a config requirement that the exclusion list does not name \
         (or vice versa)"
    );
}

/// Pinned replay coverage (issue #352 steps 2-3).
const TOTAL_DIRECTIVES: usize = 1_200;
const REPLAYABLE: usize = 948;
const UNREPLAYED_BY_REASON: &str = "config-delta file=121, not a capture claim (derived)=14, \
not a capture claim (ported)=29, our-error-text (eval_fail)=71, pinned-divergence=17";
