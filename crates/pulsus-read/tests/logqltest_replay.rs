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
//! **Three different numbers, and none of them is the others.** This
//! issue exists because a coverage figure got read as stronger than it
//! was, so the vocabulary is split and each figure is pinned separately:
//!
//! | figure | means | pinned by |
//! |---|---|---|
//! | `captured` | directives claiming container capture | `CAPTURED` in `logqltest_provenance.rs` |
//! | [`PROVENANCE_PERMITS`] | rows the marker classification ALLOWS a replay to compare | this file |
//! | [`REACHABLE`] | rows a live replay can PHYSICALLY compare today | this file |
//!
//! The values live ONLY on those constants, each asserted against a figure
//! recomputed from the corpus. This table used to restate them as
//! literals, and two of the three had drifted before issue #248 noticed —
//! a hand-copied number beside a machine-checked one is a false claim
//! waiting to happen, so the copies were removed rather than re-synced,
//! and none is quoted here. Read the constants.
//!
//! `PROVENANCE_PERMITS` was called `REPLAYABLE` until the live leg was
//! built, and the name was wrong: the [`Unreachable::AbsoluteTimestamp`]
//! bucket — the large majority of them — is pinned to an
//! absolute date the reference will not serve, so no replay can ever
//! reach them. That is the SAME conflation this issue opened with,
//! reproduced one level further in — we corrected `captured` vs
//! `replayable` and immediately built `replayable` vs `reached`. Hence
//! three names, three constants, and the gap enumerated by reason.

mod logqltest;

use std::collections::BTreeMap;

use std::time::Duration;

use logqltest::runner::{Command, EvalMode, Labels, StreamEntry, StreamSpec};
use pulsus_logql::{Expr, parse};

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

/// Why a row the markers PERMIT still cannot be reached by a live
/// replay. Distinct from [`Unreplayed`] on purpose: that one is about
/// what the corpus says a row is, this one is about what the reference
/// container can physically serve.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Unreachable {
    /// The case's samples carry an ABSOLUTE timestamp (the template
    /// files `t1`-`t6`, pinned to 2026-07-27), and neither way of
    /// replaying it works:
    ///
    /// - it cannot be time-SHIFTED, because the expected value is a
    ///   function of the timestamp — `t5_time.test` is 258 rows of
    ///   exactly that;
    /// - it cannot be pushed AS-IS, because the reference serves only
    ///   the last ~3 hours (see [`INGESTION_WINDOW`]).
    ///
    /// **This is the single largest lever on reachability — 678 rows, far
    /// more than the live leg reaches in total — and it is
    /// blocked by the CORPUS, not by this harness.** Unblocking it means
    /// re-capturing those files against RELATIVE time, which is corpus
    /// work with its own capture procedure and its own review, not a
    /// slice of the replay. Recorded here so whoever picks it up knows
    /// where the 678 went.
    AbsoluteTimestamp,
    /// A metric query. The first slice replays log (streams) queries
    /// only; metric results need vector/scalar comparison against the
    /// reference's own JSON shapes.
    MetricQuery,
    /// A `range`/matrix eval — needs the step grid replayed too.
    RangeMatrix,
    /// An `eval detected` directive. Zero today (all six sit in a
    /// config-delta file and never reach this classification), matched
    /// so a future one cannot be silently mis-bucketed as reachable.
    DetectedFields,
    /// The case has no `load` in force, so there is nothing to push.
    NoLoadSet,
}

/// **The reference's ingestion window, measured — not read off a
/// default.** Pushes are accepted (`204`) at every age below; only the
/// young ones are then queryable, because `querier.query_ingesters_within`
/// (3h) bounds what is served from the ingester and a short-lived
/// container has no flushed store chunks behind it.
///
/// | sample age | push | queryable |
/// |---|---|---|
/// | 5m, 1h, 1.5h, 2h, 2.5h | 204 | yes |
/// | 3h, 4h, 6h, 24h | 204 | **no** |
///
/// **A too-old push SUCCEEDS and then answers nothing**, which reads
/// exactly like a replay finding a mismatch. Every slot this leg uses
/// must sit inside the window, and the assertion that they do is
/// [`slots_fit_inside_the_measured_ingestion_window`].
const INGESTION_WINDOW: Duration = Duration::from_secs(3 * 3600);

/// One row's replay inputs, captured during classification so the live
/// leg does not walk the corpus a second time with its own rules.
#[derive(Debug, Clone)]
struct ReplayCase {
    load: Vec<StreamSpec>,
    query: String,
    expected: Vec<String>,
}

/// A directive's replay disposition, at BOTH levels.
#[derive(Debug)]
struct Row {
    file: String,
    line: usize,
    /// What the provenance markers permit — [`PROVENANCE_PERMITS`].
    provenance: Result<(), Unreplayed>,
    /// Whether a live replay can physically reach it — [`REACHABLE`].
    /// Only meaningful when `provenance` is `Ok`.
    reach: Result<ReplayCase, Unreachable>,
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
/// marked from that evidence, not from a guess. Until then
/// [`PROVENANCE_PERMITS`] is an upper bound on what is genuinely
/// replayable, not a measurement of it.
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
        // The load set in force, exactly as the runner maintains it:
        // `load` accumulates, `clear` resets.
        let mut load: Vec<StreamSpec> = Vec::new();
        for c in &cmds {
            match c {
                Command::Clear => {
                    load.clear();
                    continue;
                }
                Command::Load(specs) => {
                    load.extend(specs.iter().cloned());
                    continue;
                }
                Command::Eval(_) => {}
            }
            let Command::Eval(e) = c else { unreachable!() };
            let mark = marks.get(mi).cloned().unwrap_or_default();
            mi += 1;
            let provenance = if let Some(id) = mark
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
            let reach = reachability(e, &load);
            out.push(Row {
                file: name.clone(),
                line: e.line,
                provenance,
                reach,
            });
        }
    }
    out
}

/// Whether a live replay can physically compare this row, and its inputs
/// if so. Ordered most-specific first so each row lands in exactly one
/// bucket.
fn reachability(
    e: &logqltest::runner::EvalCmd,
    load: &[StreamSpec],
) -> Result<ReplayCase, Unreachable> {
    if e.detected.is_some() {
        return Err(Unreachable::DetectedFields);
    }
    if e.range.is_some() {
        return Err(Unreachable::RangeMatrix);
    }
    // The result kind is decided by the query, exactly as the hermetic
    // runner decides it (`evaluate`): `Expr::Log` is the streams class.
    if !matches!(parse(&e.query), Ok(Expr::Log(_))) {
        return Err(Unreachable::MetricQuery);
    }
    if load.is_empty() || load.iter().all(|s| s.samples.is_empty()) {
        return Err(Unreachable::NoLoadSet);
    }
    // An absolute timestamp cannot be shifted (the expectation depends on
    // it) and cannot be pushed as-is (outside the ingestion window). One
    // day is far above every relative corpus offset (max 10s) and far
    // below the 2026 absolute stamps, so it separates the two classes
    // without needing to know either exactly.
    const RELATIVE_CEILING_NS: i64 = 86_400_000_000_000;
    if load
        .iter()
        .flat_map(|s| s.samples.iter())
        .any(|(ts, _)| *ts > RELATIVE_CEILING_NS)
    {
        return Err(Unreachable::AbsoluteTimestamp);
    }
    Ok(ReplayCase {
        load: load.to_vec(),
        query: e.query.clone(),
        expected: e.expected.clone(),
    })
}

fn unreachable_key(u: &Unreachable) -> &'static str {
    match u {
        Unreachable::AbsoluteTimestamp => "absolute-timestamp (template corpus)",
        Unreachable::MetricQuery => "metric query (slice: streams only)",
        Unreachable::RangeMatrix => "range/matrix eval (slice: instant only)",
        Unreachable::DetectedFields => "detected-fields eval (slice: streams only)",
        Unreachable::NoLoadSet => "no load set",
    }
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

/// **The coverage figures, with every exclusion named at BOTH levels.**
/// Pinned, so neither can drift and neither can be quoted as the other.
#[test]
fn coverage_is_pinned_and_names_every_exclusion_at_both_levels() {
    let rows = classify();
    assert!(
        rows.len() > 900,
        "expected the whole corpus, got {}",
        rows.len()
    );

    let permits = rows.iter().filter(|r| r.provenance.is_ok()).count();
    // Reachability is only asked of rows the markers already permit: a
    // row excluded by provenance is not "unreachable", it is out of
    // scope, and counting it twice would inflate both figures.
    let reachable = rows
        .iter()
        .filter(|r| r.provenance.is_ok() && r.reach.is_ok())
        .count();

    let mut by_reason: BTreeMap<String, usize> = BTreeMap::new();
    let mut example: BTreeMap<String, String> = BTreeMap::new();
    let mut note = |k: String, r: &Row, by: &mut BTreeMap<String, usize>| {
        *by.entry(k.clone()).or_default() += 1;
        example
            .entry(k)
            .or_insert_with(|| format!("{}:{}", r.file, r.line));
    };
    let mut unreach: BTreeMap<String, usize> = BTreeMap::new();
    for r in &rows {
        match &r.provenance {
            Err(u) => note(reason_key(u), r, &mut by_reason),
            Ok(()) => {
                if let Err(u) = &r.reach {
                    note(unreachable_key(u).to_string(), r, &mut unreach);
                }
            }
        }
    }
    let fmt = |m: &BTreeMap<String, usize>| {
        m.iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    // A count with no locator is the kind of figure this issue exists to
    // stop, so every reason carries an example row.
    let detail = |m: &BTreeMap<String, usize>| {
        m.iter()
            .map(|(k, v)| format!("{k}={v} (e.g. {})", example[k]))
            .collect::<Vec<_>>()
            .join(", ")
    };

    assert_eq!(
        (permits, rows.len()),
        (PROVENANCE_PERMITS, TOTAL_DIRECTIVES),
        "provenance coverage moved — update the pins AND the figure quoted on issue #352. \
         Excluded by provenance: {}",
        detail(&by_reason)
    );
    assert_eq!(
        fmt(&by_reason),
        EXCLUDED_BY_PROVENANCE,
        "the provenance exclusion breakdown moved"
    );
    assert_eq!(
        reachable,
        REACHABLE,
        "live reachability moved. Permitted but UNREACHABLE: {}",
        detail(&unreach)
    );
    assert_eq!(
        fmt(&unreach),
        UNREACHABLE_BY_REASON,
        "the unreachable breakdown moved"
    );
    // The three figures must compose, so none can be quoted as another:
    // permitted = reached + unreachable, exactly.
    let unreachable_total: usize = unreach.values().sum();
    assert_eq!(
        reachable + unreachable_total,
        permits,
        "reached + unreachable must account for every permitted row"
    );
}

/// **A ledgered divergence row must never enter the replayed set.**
///
/// Replaying one would compare the reference against a value defined as
/// NOT-the-reference, so it would always "fail" — and the obvious repair
/// is to re-capture the row, which silently erases the divergence the
/// ledger exists to record. The exclusion is the point of the marker
/// scheme, so it is pinned rather than trusted.
///
/// This was verified once by hand — corrupting an excluded row and
/// confirming the leg does NOT catch it, which is what proves the
/// exclusion real rather than decorative — but a manual check protects
/// nothing after the day it was run. The row named here is the
/// byte-literal quantization divergence from issue #350: the corpus's
/// worked example of a capture that must NOT be re-captured.
#[test]
fn a_ledgered_divergence_row_is_never_replayed() {
    const LEDGERED: &str = "byte-literal-render-quantization";
    let rows = classify();
    let excluded: Vec<&Row> = rows
        .iter()
        .filter(
            |r| matches!(&r.provenance, Err(Unreplayed::PinnedDivergence(id)) if id == LEDGERED),
        )
        .collect();
    assert!(
        !excluded.is_empty(),
        "the {LEDGERED:?} divergence row has left the corpus; this test pins a row that must \
         exist to pin anything"
    );
    // The set the live leg actually iterates — same filter, so this
    // cannot drift away from what is replayed.
    for r in &excluded {
        assert!(
            r.provenance.is_err(),
            "{}:{} is marked divergence({LEDGERED}) yet the provenance filter admits it — \
             replaying it would compare the reference against a value defined as not-the-\
             reference, and a re-capture would erase the divergence",
            r.file,
            r.line
        );
    }
    let replayed: Vec<String> = rows
        .iter()
        .filter(|r| r.provenance.is_ok() && r.reach.is_ok())
        .map(|r| format!("{}:{}", r.file, r.line))
        .collect();
    for r in &excluded {
        let key = format!("{}:{}", r.file, r.line);
        assert!(
            !replayed.contains(&key),
            "{key} carries a ledgered divergence and is in the replayed set"
        );
    }
}

/// Every slot the live leg uses must sit inside the reference's measured
/// [`INGESTION_WINDOW`], or its pushes succeed and answer nothing — which
/// reads exactly like a replay finding a mismatch.
///
/// Checked arithmetically, so widening the slice cannot outgrow the
/// window unnoticed.
#[test]
fn slots_fit_inside_the_measured_ingestion_window() {
    let span = SLOT_SECS * REACHABLE as u64;
    assert!(
        FIRST_SLOT_AGE.as_secs() + SLOT_SECS <= INGESTION_WINDOW.as_secs(),
        "the oldest slot ({}s) must sit inside the {}s window",
        FIRST_SLOT_AGE.as_secs(),
        INGESTION_WINDOW.as_secs()
    );
    assert!(
        span <= FIRST_SLOT_AGE.as_secs(),
        "{REACHABLE} slots of {SLOT_SECS}s span {span}s, which marches past `now` from a \
         start {}s back — either shorten the slot or start further back (bounded by the \
         {}s window)",
        FIRST_SLOT_AGE.as_secs(),
        INGESTION_WINDOW.as_secs()
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

/// Pinned coverage (issue #352 steps 2-3). THREE figures, never one.
///
/// Issue #343 added `b19_offset.test`'s 9 rows, and its boundary fix 6
/// more (the domain-edge rows). They move the TOTAL and the `derived`
/// exclusion only: `derived` is not a capture claim, so
/// [`PROVENANCE_PERMITS`] does not move, and they are metric queries,
/// which this slice cannot reach in any case.
///
/// Issue #344's execution half moves all three provenance figures.
/// `b18_range_agg_grouping.test` gained 28 captured `eval` rows and
/// converted 10 `eval_fail` rows (the interim "not yet executed"
/// refusals) into `eval`s, and its instant `first`/`last`
/// delivery-order fix added the two cross-stream tie rows: TOTAL
/// 1_215 -> 1_245, `our-error-text (eval_fail)` 71 -> 61, and so
/// `PROVENANCE_PERMITS` 948 -> 993 (the rows added across both rounds
/// plus the 10 that stopped being our own error text; review round 1 net
/// +5 permitted — seven captured rows added, one grouped-`avg` row
/// removed, and two of the new ones are `eval_fail`s carrying our own
/// error text, which the markers do not permit a replay to compare).
/// [`REACHABLE`] does NOT move: every newly-permitted row is a metric
/// queries (30 instant, 8 on a step grid) and this slice replays log
/// (streams) queries at a single instant — so they land in the
/// enumerated gap, not in the coverage.
///
/// Issue #248 adds `b20_nested_ip.test`: TOTAL 1_252 -> 1_277 (20 `eval`
/// rows and 5 `eval_fail` rows), `our-error-text (eval_fail)` 63 -> 68,
/// and so [`PROVENANCE_PERMITS`] 993 -> 1_013. Thirteen of the permitted
/// rows are streams queries at a single instant over a relative-offset
/// load set, so [`REACHABLE`] moves for the first time since the leg was
/// built: 77 -> 90. The other seven are metric queries (228 -> 235).
///
/// Its second round adds eleven more `eval` rows to that same file — the
/// error-ordering block, where a numeric conversion fails to the LEFT of
/// a leaf that reads the error state. All eleven are streams queries at a
/// single instant over a relative-offset load set, so all three figures
/// move together: TOTAL 1_277 -> 1_288, [`PROVENANCE_PERMITS`]
/// 1_013 -> 1_024, [`REACHABLE`] 90 -> 101.
const TOTAL_DIRECTIVES: usize = 1_288;

/// What the provenance markers ALLOW a replay to compare. Named
/// `REPLAYABLE` until the live leg existed, which was wrong: most of
/// these cannot be reached at all. See the module docs.
const PROVENANCE_PERMITS: usize = 1_024;

/// What the live leg can PHYSICALLY compare today. The gap to
/// `PROVENANCE_PERMITS` is enumerated by
/// [`UNREACHABLE_BY_REASON`] — it is not a shortfall to be quietly
/// absorbed into one number.
const REACHABLE: usize = 101;

const EXCLUDED_BY_PROVENANCE: &str = "config-delta file=121, not a capture claim (derived)=29, \
not a capture claim (ported)=29, our-error-text (eval_fail)=68, pinned-divergence=17";

/// Issue #344: `metric query` 191 -> 221 and `range/matrix eval`
/// 2 -> 10. All 38 of `b18_range_agg_grouping.test`'s newly-permitted
/// rows are metric queries, 8 of them on a step grid, and this slice
/// replays log (streams) queries at a single instant — so they enlarge
/// the ENUMERATED gap rather than the coverage. Both are levers the
/// module docs already name.
const UNREACHABLE_BY_REASON: &str = "absolute-timestamp (template corpus)=678, \
metric query (slice: streams only)=235, range/matrix eval (slice: instant only)=10";

/// How far back the first slot sits. Bounded above by
/// [`INGESTION_WINDOW`] and below by the total slot span; both are
/// asserted by [`slots_fit_inside_the_measured_ingestion_window`].
const FIRST_SLOT_AGE: Duration = Duration::from_secs(150 * 60);

/// One case's exclusive time slot. Every corpus case spans at most 10s,
/// so 60s leaves ample separation between neighbours — needed because
/// the corpus reuses stream labels constantly (`clear` scopes them, and
/// the reference has no equivalent), so time is what keeps two cases
/// apart.
const SLOT_SECS: u64 = 60;

// ---------------------------------------------------------------------
// The live replay (issue #352 step 3).
//
// Gated on `PULSUSDB_LOGQL_DIFF_URL`, skipping cleanly when unset, like
// every other leg on the differential oracle. Nightly, not per-PR: it is
// drift detection on committed captures, and drift is a property of time
// passing, not of the commit under test.
// ---------------------------------------------------------------------

/// What a replay establishes, restated where the test runs so a green
/// line in a log cannot be read as more than it is.
const WHAT_THIS_VERIFIES: &str = "A replay verifies the VALUE: it pushes each case's `load` to \
the reference and compares the reference's answer against the committed expectation. It does NOT \
verify the provenance MARKER — it only READS the marker to decide which rows it may compare. A \
hand-authored value that happens to be right passes; a genuine capture gone stale fails.";

/// Loki injects this label when `discover_log_levels` is on, which it is
/// in `ci/logql/config.yaml` — the config the differential oracle runs.
/// The corpus expectations were captured with it OFF, so it is stripped
/// before comparison.
///
/// **This is a normalisation, and it is the only one.** The honest fix is
/// `discover_log_levels: false` in that config, which would also unblock
/// the two config-delta files that need exactly that setting
/// (`b12_error_pair_model`, `b14_detected_fields` — 121 rows). It is
/// deliberately NOT done here: that config is shared with the syntax leg
/// and the json-key leg, and changing what two other suites measure from
/// inside a third suite's first slice is how a shared oracle drifts.
/// Follow-up, with both legs re-verified.
const INJECTED_LABEL: &str = "detected_level";

fn curl(args: &[&str]) -> String {
    let out = std::process::Command::new("curl")
        .args(["-s", "--max-time", "30"])
        .args(args)
        .output()
        .expect("curl must be on PATH");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

/// One case's answer from the reference, in CORPUS coordinates: the slot
/// offset is removed so a mismatch reads against the committed
/// expectation rather than against wall-clock nanoseconds.
fn query_case(
    base: &str,
    slot_start_ns: i64,
    case_min_ns: i64,
    span_ns: i64,
    query: &str,
) -> Vec<StreamEntry> {
    // The window brackets THIS case's samples exactly, in nanoseconds
    // (`start` inclusive, `end` exclusive). Not the whole slot: a slot
    // only separates cases within ONE run, and a second run against the
    // same container lands its slots at a different offset, so a wider
    // window pulls in a previous run's lines under the same reused
    // labels. Measured — a +26s line from an earlier run appeared inside
    // a +30s window and read as a corpus mismatch. CI always has a fresh
    // container, which is exactly why this could have shipped unnoticed.
    let body = curl(&[
        "-G",
        "--data-urlencode",
        &format!("query={query}"),
        "--data-urlencode",
        &format!("start={slot_start_ns}"),
        "--data-urlencode",
        &format!("end={}", slot_start_ns + span_ns + 1),
        "--data-urlencode",
        "limit=5000",
        "--data-urlencode",
        "direction=forward",
        &format!("{base}/loki/api/v1/query_range"),
    ]);
    let parsed: serde_json::Value = serde_json::from_str(&body)
        .unwrap_or_else(|e| panic!("unparseable query_range body for {query:?}: {e}\n{body}"));
    let mut out = Vec::new();
    let Some(result) = parsed["data"]["result"].as_array() else {
        panic!("no data.result for {query:?}: {body}");
    };
    for stream in result {
        let mut labels: Labels = stream["stream"]
            .as_object()
            .expect("stream labels")
            .iter()
            .filter(|(k, _)| k.as_str() != INJECTED_LABEL)
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
            .collect();
        labels.sort();
        for v in stream["values"].as_array().expect("values") {
            let ts: i64 = v[0]
                .as_str()
                .expect("ts string")
                .parse()
                .expect("ts is an integer");
            out.push((
                labels.clone(),
                ts - slot_start_ns + case_min_ns,
                v[1].as_str().expect("line").to_string(),
            ));
        }
    }
    out.sort();
    out
}

#[test]
fn live_replay_of_the_reachable_rows_against_the_reference() {
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!(
            "skipping: set PULSUSDB_LOGQL_DIFF_URL to replay the corpus against the reference"
        );
        return;
    };
    let base = base.trim_end_matches('/').to_string();

    let rows = classify();
    let cases: Vec<(&Row, &ReplayCase)> = rows
        .iter()
        .filter_map(|r| match (&r.provenance, &r.reach) {
            (Ok(()), Ok(c)) => Some((r, c)),
            _ => None,
        })
        .collect();
    assert_eq!(
        cases.len(),
        REACHABLE,
        "the reachable set moved; the coverage test explains it"
    );

    // **The container must be FRESH.** Cross-run isolation is not
    // possible after the fact: two runs push the same corpus labels at
    // overlapping absolute times, so the reference sees ONE stream and
    // merges the entries — no query window can separate them, because
    // the stale line lands inside the case's own span. Measured: a
    // second run against a used container reported four corpus rows as
    // mismatches, with an extra line at an offset the first run had
    // written.
    //
    // So the precondition is checked rather than assumed. CI always
    // starts a fresh container, which is precisely why an unchecked
    // assumption here would have held in CI and misled every local run.
    let prior = curl(&[
        "-G",
        "--data-urlencode",
        r#"query={replay_sentinel=~".+"}"#,
        "--data-urlencode",
        &format!(
            "start={}",
            (unix_now_secs() - INGESTION_WINDOW.as_secs()) * 1_000_000_000
        ),
        "--data-urlencode",
        &format!("end={}", unix_now_secs() * 1_000_000_000),
        "--data-urlencode",
        "limit=1",
        &format!("{base}/loki/api/v1/query_range"),
    ]);
    let prior: serde_json::Value = serde_json::from_str(&prior).unwrap_or_else(|e| {
        panic!("unparseable query_range body while probing for a prior run: {e}")
    });
    assert!(
        prior["data"]["result"]
            .as_array()
            .is_none_or(|r| r.is_empty()),
        "PRECONDITION FAILURE, NOT A CORPUS FAILURE. This reference container already holds a \
         previous replay run.\n\n\
         THE COMMITTED CORPUS VALUES ARE NOT IMPLICATED — do not go looking at them. Two runs \
         push the same corpus labels at overlapping absolute times, so the reference merges \
         them into ONE stream and a stale line lands inside a case's own span. No query window \
         can separate them. Had this gone undetected it would have surfaced as `N of \
         {REACHABLE} replayed rows disagree with the reference`, sending you after captures \
         that are fine.\n\n\
         CI always starts a fresh container, so this check exists for LOCAL runs — which also \
         means CI can never exercise it.\n\n\
         Fix: restart the container, then re-run.\n  \
         podman rm -f pulsus-logql-diff && podman run -d --name pulsus-logql-diff -p 13100:3100 \
         -v $PWD/ci/logql/config.yaml:/etc/loki/local-config.yaml:ro <pinned image> \
         -config.file=/etc/loki/local-config.yaml"
    );

    // Push everything first: one exclusive slot per case, marching
    // forward from `FIRST_SLOT_AGE` back. Slots exist because the corpus
    // reuses stream labels across cases (`clear` scopes them and the
    // reference has no equivalent), so time is the only thing separating
    // two otherwise identical streams.
    let base_slot_s = unix_now_secs() - FIRST_SLOT_AGE.as_secs();
    let mut plan = Vec::new();
    for (i, (row, case)) in cases.iter().enumerate() {
        let slot_start_ns = (base_slot_s + i as u64 * SLOT_SECS) as i64 * 1_000_000_000;
        let case_min_ns = case
            .load
            .iter()
            .flat_map(|s| s.samples.iter())
            .map(|(ts, _)| *ts)
            .min()
            .expect("a reachable case has samples");
        let mut streams = Vec::new();
        for spec in &case.load {
            if spec.samples.is_empty() {
                continue;
            }
            let labels: serde_json::Map<String, serde_json::Value> = spec
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect();
            let values: Vec<serde_json::Value> = spec
                .samples
                .iter()
                .map(|(ts, body)| {
                    serde_json::json!([
                        (slot_start_ns + (ts - case_min_ns)).to_string(),
                        body.clone()
                    ])
                })
                .collect();
            streams.push(serde_json::json!({"stream": labels, "values": values}));
        }
        // The ingestion barrier for every case: a push returns 204 before
        // its data is queryable, so something must be waited on.
        //
        // It is a SEPARATE STREAM, not the case's own query, and that is
        // what makes the barrier sound rather than merely adequate: the
        // sentinel's visibility is independent of whether the case
        // expects any results, so "not yet ingested" and "correctly no
        // results" can never be the same observation. A case whose
        // expectation is empty is therefore handled by construction — no
        // ingestion lag can present as a pass, whatever the slice grows
        // to contain.
        streams.push(serde_json::json!({
            "stream": {"replay_sentinel": i.to_string()},
            "values": [[slot_start_ns.to_string(), "sentinel"]],
        }));
        let payload = serde_json::json!({"streams": streams}).to_string();
        let status = curl(&[
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-H",
            "Content-Type: application/json",
            "-X",
            "POST",
            "--data-binary",
            &payload,
            &format!("{base}/loki/api/v1/push"),
        ]);
        assert_eq!(
            status.trim(),
            "204",
            "{}:{}: push rejected",
            row.file,
            row.line
        );
        let span_ns = case
            .load
            .iter()
            .flat_map(|s| s.samples.iter())
            .map(|(ts, _)| *ts)
            .max()
            .expect("a reachable case has samples")
            - case_min_ns;
        plan.push((slot_start_ns, case_min_ns, span_ns));
    }

    let mut mismatches = Vec::new();
    for (i, (row, case)) in cases.iter().enumerate() {
        let (slot_start_ns, case_min_ns, span_ns) = plan[i];
        // Barrier: wait for THIS case's sentinel before reading its
        // answer. A push returns 204 before the data is queryable.
        let mut visible = false;
        for _ in 0..40 {
            if !query_case(
                &base,
                slot_start_ns,
                case_min_ns,
                0,
                &format!(r#"{{replay_sentinel="{i}"}}"#),
            )
            .is_empty()
            {
                visible = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        assert!(
            visible,
            "{}:{}: the case's sentinel never became visible — the push succeeded but the data \
             is not being served. Check the slot is inside the {}s ingestion window.",
            row.file,
            row.line,
            INGESTION_WINDOW.as_secs()
        );

        let mut want: Vec<StreamEntry> = case
            .expected
            .iter()
            .map(|l| {
                logqltest::runner::parse_expected_stream(l)
                    .unwrap_or_else(|e| panic!("{}:{}: bad expectation: {e}", row.file, row.line))
            })
            .collect();
        want.sort();
        let got = query_case(&base, slot_start_ns, case_min_ns, span_ns, &case.query);
        if want != got {
            mismatches.push(format!(
                "{}:{} {:?}\n     want ({}): {}\n     got  ({}): {}",
                row.file,
                row.line,
                case.query,
                want.len(),
                fmt_entries(&want),
                got.len(),
                fmt_entries(&got),
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "{} of {REACHABLE} replayed rows disagree with the reference.\n\n{WHAT_THIS_VERIFIES}\n\n\
         A mismatch is EITHER a capture that has gone stale OR a row the reference does not \
         reproduce deterministically (map order, same-timestamp ties, a sketch above its \
         retention cap). Those are unlisted deliberately — a wrong exclusion list reads exactly \
         like a right one — so a row that flaps across runs with no change to either side is \
         the second kind, and gets marked from THAT evidence, never from a guess.\n\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
    eprintln!("replayed {REACHABLE} rows against the reference. {WHAT_THIS_VERIFIES}");
}

fn fmt_entries(items: &[StreamEntry]) -> String {
    items
        .iter()
        .map(|(l, ts, line)| {
            let ls = l
                .iter()
                .map(|(k, v)| format!("{k}={v:?}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{{ls}}} @{ts} {line:?}")
        })
        .collect::<Vec<_>>()
        .join("; ")
}
