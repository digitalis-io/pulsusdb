// corpus-counts: none (replay-module-doc) — this module doc is where the
// class started: it carried a table of hand-copied coverage figures, and
// they had drifted by the time anyone looked. The region keeps them from
// coming back.
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
//! **Separate numbers, and none of them is the others.** This issue
//! exists because a coverage figure got read as stronger than it was, so
//! the vocabulary is split and each figure is pinned separately:
//!
//! | figure | means | pinned by |
//! |---|---|---|
//! | `captured` | directives claiming container capture | `CAPTURED` in `logqltest_provenance.rs` |
//! | [`PROVENANCE_PERMITS`] | rows the marker classification ALLOWS a replay to compare | this file |
//! | [`REACHABLE`] | rows a live replay can PHYSICALLY compare today | this file |
//!
//! The values live ONLY on those constants, each asserted against a figure
//! recomputed from the corpus. This table used to restate them as
//! literals, and they had drifted before issue #248 noticed — a
//! hand-copied number beside a machine-checked figure is a false claim
//! waiting to happen, so the copies were removed rather than re-synced,
//! and none is quoted here. Read the constants.
//!
//! **`corpus-counts: none` regions.** Issue #248 corrected a stale copy
//! in round after round and the class survived every correction, so the
//! rule is now enforced rather than restated: inside such a region,
//! comment text carries no digit and no number word at all — the
//! narrower "a number word in front of a counting noun" rule kept
//! meeting a word it did not know. That ban covers digits, the standard
//! spelling of every cardinal the check's speller emits, and a listed
//! set of variants (`nought` and friends); it is NOT closed against
//! archaic, dialect or function-word forms, and the check's own doc
//! names which it declines and why.
//! `check_f_marked_regions_state_no_corpus_count`
//! (`logqltest_provenance.rs`) fails on any; `logqltest/PROVENANCE.md`
//! §"Counts live on the constants" carries the rule itself. Each marker
//! names its region — the id in parentheses, repeated on the `end` — and
//! that id is pinned in `NO_COUNT_REQUIRED`, so a region that disappears
//! fails by name rather than leaving the file's other regions to cover
//! for it.
//!
//! `PROVENANCE_PERMITS` was called `REPLAYABLE` until the live leg was
//! built, and the name was wrong: the [`Unreachable::AbsoluteTimestamp`]
//! bucket — the large majority of them — is pinned to an
//! absolute date the reference will not serve, so no replay can ever
//! reach them. That is the SAME conflation this issue opened with,
//! reproduced a level further in — we corrected `captured` vs
//! `replayable` and immediately built `replayable` vs `reached`. Hence a
//! separate name and a separate constant for each, and the gap
//! enumerated by reason.
// corpus-counts: end (replay-module-doc)

mod logqltest;

use std::collections::BTreeMap;

use std::time::Duration;

use logqltest::runner::{Command, EvalMode, Labels, StreamEntry, StreamSpec, parse_labelset};
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
    // corpus-counts: none (replay-absolute-timestamp)
    /// The case's samples carry an ABSOLUTE timestamp (the template
    /// files `t1`-`t6`, pinned to 2026-07-27), and neither way of
    /// replaying it works:
    ///
    /// - it cannot be time-SHIFTED, because the expected value is a
    ///   function of the timestamp — `t5_time.test` is rows of exactly
    ///   that and nothing else;
    /// - it cannot be pushed AS-IS, because the reference serves only a
    ///   recent window (see [`INGESTION_WINDOW`]).
    ///
    /// **This is the single largest lever on reachability — bigger than
    /// everything the live leg reaches put together, which
    /// [`the_template_corpus_is_the_largest_unreachable_bucket`] asserts
    /// rather than claims — and it is blocked by the CORPUS, not by this
    /// harness.** Unblocking it means re-capturing those files against
    /// RELATIVE time, which is corpus work with its own capture
    /// procedure and its own review, not a slice of the replay. Recorded
    /// here so whoever picks it up knows where the bucket went.
    AbsoluteTimestamp,
    // corpus-counts: end (replay-absolute-timestamp)
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
/// [`the_placement_fits_the_measured_served_horizon`].
///
/// **This is the coarse BRACKET, not the boundary, and slot placement
/// is governed by [`SERVED_HORIZON`] instead** (issue #278). The ladder
/// above steps from 2.5 h straight to 3 h and this constant took the
/// far end; the wall was later measured to sit between 9640 s and
/// 9700 s. Reading this figure as the space available cost issue #278's
/// plan a factor of twenty on the run margin. It stays as it is because
/// it is still the right bound for the prior-run probe below, where a
/// wider window can only find MORE evidence of a previous run.
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
    // Issue #249. The injected `detected_level` is itself STRUCTURED
    // METADATA, and this file's whole subject is what structured metadata
    // does to the metric label set — so with level discovery on, every
    // expected set here would carry an extra pair and the ANSWERS change,
    // as they do for `b12`/`b14`. (Contrast `b20`, where the injection
    // only adds a stream label the replay strips.)
    (
        "b25_structured_metadata.test",
        "discover_log_levels: false (NOT in ci/logql/config.yaml)",
    ),
    // Issue #277: the per-variant series cap's skip-and-warn corpus.
    (
        "b21_variant_series_cap.test",
        "enable_multi_variant_queries (in ci/logql/config.yaml)",
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
        .any(|(ts, _, _)| *ts > RELATIVE_CEILING_NS)
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
    // The figures must compose, so none can be quoted as another:
    // permitted = reached + unreachable, exactly.
    let unreachable_total: usize = unreach.values().sum();
    assert_eq!(
        reachable + unreachable_total,
        permits,
        "reached + unreachable must account for every permitted row"
    );
}

/// The claim [`Unreachable::AbsoluteTimestamp`] makes in prose — "the
/// single largest lever on reachability, bigger than everything the live
/// leg reaches put together" — asserted instead of asserted-in-prose.
///
/// A comparative claim drifts exactly like a copied count does, and this
/// one is load-bearing: it is the reason the template corpus is named as
/// the thing to fix first. The prose now carries no figure at all
/// (issue #248 round 3), so this is what keeps it honest.
#[test]
fn the_template_corpus_is_the_largest_unreachable_bucket() {
    let rows = classify();
    let mut unreach: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut reachable = 0usize;
    for r in &rows {
        if r.provenance.is_ok() {
            match &r.reach {
                Ok(_) => reachable += 1,
                Err(u) => *unreach.entry(unreachable_key(u)).or_default() += 1,
            }
        }
    }
    let template = unreach
        .remove(unreachable_key(&Unreachable::AbsoluteTimestamp))
        .expect("the template corpus must still be an unreachable bucket");
    for (k, v) in &unreach {
        assert!(
            template > *v,
            "{k} ({v}) is no longer smaller than the template-corpus bucket ({template}) — \
             the 'single largest lever' claim in Unreachable::AbsoluteTimestamp's docs and in \
             PROVENANCE.md is now false"
        );
    }
    assert!(
        template > reachable,
        "the template-corpus bucket ({template}) no longer exceeds everything the live leg \
         reaches ({reachable}) — correct the claim, do not widen it"
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

/// How many times the capacity prose quotes the placement RATIO, the
/// occupancy PERCENTAGE and the free-budget ROW COUNT — across this file
/// and `PROVENANCE.md` together.
///
/// **Occurrences, not lines** (the #447 rule): one line quoting a figure
/// twice counts twice, because a line count would let a second quotation
/// hide on a line that already has one.
///
/// Three needles, not the four the fixed-slot scheme had. The fourth was
/// the no-margin quotient `SERVED_HORIZON / SLOT_SECS` and its
/// percentage — a warning ABOUT the retired mechanism, which retires
/// with it. Its sentence stays in the record in the past tense and is no
/// longer counted.
const CAPACITY_QUOTATIONS: [usize; 3] = [2, 2, 2];

/// **The quoted capacity figures, as a check rather than a sentence.**
///
/// [`PLACEMENT_BUDGET_SECS`], [`REMAINING_SECS`] and
/// [`REMAINING_ZERO_SPAN_ROWS`] are computed, so the VALUES cannot go
/// stale — but prose repeating a value is how a figure goes wrong in one
/// place while staying right in another, and issue #278 did exactly that
/// twice: round 1 found the no-margin quotient and its percentage typed
/// beside a correct constant, and round 2 found their corrected
/// replacements typed as well, drift-provable by perturbing a digit and
/// building clean.
///
/// So the two artefacts are read at compile time and the quotations
/// counted. **The needles are built at RUN TIME from the constants**,
/// which is the load-bearing part of the #447 shape
/// (`logql_row_expansion_ladder.rs`): a typed needle would be a second
/// declaration of the same value, free to disagree with the first, and
/// would also match itself so the count would include the searcher.
///
/// **This is a SOURCE-SPELLING check, not a behavioural one** (issue
/// #388's rebase, review round 6). It asserts how the two artefacts are
/// WRITTEN. A stale figure worded any other way — "roughly three fifths
/// occupied", the ratio with a comma, the row count spelled out — is
/// invisible to it, and no amount of tightening changes that: the check
/// counts exact byte sequences. What covers the same ground
/// behaviourally is
/// [`the_placement_fits_the_measured_served_horizon`], which derives
/// every one of these figures from the live corpus and reddens if the
/// placement moves; this test only ensures the PROSE that repeats them
/// moved too. If prose and code disagree, that test is the one that
/// tells you which is right.
///
/// **If this fails you have added, removed or changed a quotation.**
/// Open the site that moved, check it states the current value, and only
/// then update [`CAPACITY_QUOTATIONS`]. Updating the constant first
/// turns the check back into the sentence it replaced.
///
/// **Scope, stated rather than implied.** It counts ONE rendering of
/// each figure: the ratio as placed-slash-budget followed by the unit,
/// the occupancy percentage followed by the word `occupied`, and the
/// free row count followed by the words that name what it counts. A
/// quotation spelled any other way is invisible to it, which is why
/// those renderings are used consistently across both files.
///
/// **This doc deliberately spells none of them.** A literal here would
/// be found by the search it describes, so the count would include the
/// searcher — the same trap the run-time needles avoid.
///
/// The DELIMITED forms are also deliberate. A bare budget figure would
/// match inside a larger number, and a bare row count would match
/// unrelated accounting in the coverage constants above.
///
/// **The per-artefact floor** (issue #388's rebase). Each needle must
/// appear at least once in EACH file before the totals are compared.
/// Without it the gate was satisfiable by deleting every quotation from
/// one artefact and adding one to the other — the totals would still
/// match and the record in `PROVENANCE.md` would be gone.
#[test]
fn the_quoted_capacity_figures_are_counted_so_a_stale_one_cannot_compile() {
    const SELF_SOURCE: &str = include_str!("logqltest_replay.rs");
    // `PROVENANCE.md` is not Rust, so it cannot carry a check of its
    // own; it is read from here instead, which is what keeps its
    // capacity paragraph inside the same guarantee as the code's.
    const PROVENANCE: &str = include_str!("logqltest/PROVENANCE.md");

    let needles = [
        format!("{PLACED_SPAN_SECS}/{PLACEMENT_BUDGET_SECS} s"),
        format!(
            "{:.1}% occupied",
            100.0 * PLACED_SPAN_SECS as f64 / PLACEMENT_BUDGET_SECS as f64
        ),
        format!("{REMAINING_ZERO_SPAN_ROWS} more zero-span rows"),
    ];
    for (needle, want) in needles.iter().zip(CAPACITY_QUOTATIONS) {
        let here = SELF_SOURCE.matches(needle.as_str()).count();
        let there = PROVENANCE.matches(needle.as_str()).count();
        assert!(
            here >= 1 && there >= 1,
            "`{needle}` appears {here} time(s) in logqltest_replay.rs and {there} time(s) in \
             logqltest/PROVENANCE.md. Each artefact must state the figure at least once — a \
             total alone is satisfiable by deleting every quotation from one file."
        );
        assert_eq!(
            here + there,
            want,
            "the capacity prose now quotes `{needle}` {} time(s) across \
             logqltest_replay.rs and logqltest/PROVENANCE.md, not {want}. Find the site that \
             moved, check it states the value the constants produce, and update \
             CAPACITY_QUOTATIONS after that — in that order.",
            here + there,
        );
    }
}

/// The placement is squeezed from both sides, and BOTH sides are checked
/// here — the upper one against the measured horizon, the lower one
/// against the corpus itself. Without the lower half the gap after a
/// case could shrink until neighbouring cases interleaved in one stream,
/// which the reference would merge and the replay would report as a
/// value mismatch.
///
/// Checked arithmetically and from the corpus, never from prose, so
/// neither a widening slice nor a widening case can outgrow the layout
/// unnoticed.
///
/// **This is one of [`place`]'s two callers**, the other being
/// [`live_replay_of_the_reachable_rows_against_the_reference`]. The
/// budget checked here and the timestamps pushed there come from the
/// same function on purpose: under the retired fixed slot they were two
/// separate expressions that agreed only by inspection, and either could
/// have been changed alone.
#[test]
fn the_placement_fits_the_measured_served_horizon() {
    let rows = classify();
    let cases: Vec<&ReplayCase> = rows
        .iter()
        .filter_map(|r| match (&r.provenance, &r.reach) {
            (Ok(()), Ok(c)) => Some(c),
            _ => None,
        })
        .collect();
    assert_eq!(
        cases.len(),
        REACHABLE,
        "the reachable set moved; the coverage test explains it"
    );

    // BELOW: the separation floor, derived from the corpus rather than
    // restated as a number in a comment, because it is the corpus that
    // moves it — the widest case is whatever the widest case happens to
    // be.
    //
    // This is the retired `2 * widest <= SLOT_SECS` in the new
    // coordinates and nothing weaker: `SLOT_SECS` was `2 *
    // MIN_GAP_SECS`, so the two inequalities are the same one.
    let widest_s = cases
        .iter()
        .map(|c| case_span_secs(c))
        .max()
        .expect("the reachable slice is non-empty");
    assert!(
        widest_s <= MIN_GAP_SECS,
        "the widest reachable case spans {widest_s}s and the separation floor is \
         {MIN_GAP_SECS}s — the gap after a case must be at least as wide as the case itself, \
         or two neighbours interleave in one stream and the reference merges them. Narrow the \
         case; MIN_GAP_SECS is defence in depth and is not the lever."
    );

    // THE DRIFT GATE. `PLACED_SPAN_SECS` is a committed measurement of a
    // walked corpus, exactly as `REACHABLE` is, so any corpus edit that
    // moves a load span reddens here with both figures in the message.
    let (slots, placed_total) = place(&cases);
    assert_eq!(
        placed_total, PLACED_SPAN_SECS,
        "the placement now spans {placed_total}s, not the committed {PLACED_SPAN_SECS}s. A \
         corpus edit moved a load span. Commit what this test prints — never edit a corpus \
         row to make the constant match."
    );

    // `FIRST_SLOT_AGE` against the MEASURED total, not against the
    // constant beside it. Writing `assert_eq!(FIRST_SLOT_AGE.as_secs(),
    // PLACED_SPAN_SECS)` would pass on a hand-typed age the moment
    // someone replaced the derivation with a literal, because both sides
    // would be that literal. Comparing against `place()`'s own output is
    // what makes the derivation load-bearing.
    assert_eq!(
        FIRST_SLOT_AGE.as_secs(),
        placed_total,
        "FIRST_SLOT_AGE ({}s) is no longer the placement's own total ({placed_total}s). It is \
         derived from PLACED_SPAN_SECS precisely so it cannot be hand-set: raising it is what \
         spent the run margin under the retired fixed-slot scheme.",
        FIRST_SLOT_AGE.as_secs(),
    );

    // The separation the gap exists to give, realised rather than
    // assumed: between two consecutive cases the distance from the
    // earlier one's LAST entry to the later one's FIRST is at least
    // MIN_GAP_SECS, and at least the earlier case's own span.
    for pair in slots.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        let gap = b.offset_secs - (a.offset_secs + a.span_secs);
        assert!(
            gap >= MIN_GAP_SECS && gap >= a.span_secs,
            "a case spanning {}s at offset {}s is followed by a gap of only {gap}s — the \
             separation rule is max(MIN_GAP_SECS, the case's own span)",
            a.span_secs,
            a.offset_secs,
        );
    }

    // ABOVE, the bound that actually bites: the oldest entry must sit
    // inside what the reference will SERVE, with enough left over for
    // the run itself. This bounds where the slots are PLACED, where the
    // `const _` beside `PLACED_SPAN_SECS` bounds how much the budget
    // HOLDS. Neither subsumes the other.
    assert!(
        FIRST_SLOT_AGE + Duration::from_secs(MIN_GAP_SECS) + RUN_MARGIN <= SERVED_HORIZON,
        "the oldest entry sits {}s back and the run is allowed {}s, which together pass the \
         {}s the reference still serves. Placement span and run margin come out of the SAME \
         budget.",
        FIRST_SLOT_AGE.as_secs(),
        RUN_MARGIN.as_secs(),
        SERVED_HORIZON.as_secs()
    );

    // ABOVE, the coarse bracket, kept: `INGESTION_WINDOW` is what the
    // window was first measured on and is looser than the wall by around
    // nineteen minutes. It still bounds the prior-run probe in the live
    // leg, so it is still asserted here.
    assert!(
        FIRST_SLOT_AGE.as_secs() + MIN_GAP_SECS <= INGESTION_WINDOW.as_secs(),
        "the oldest entry ({}s) must sit inside the {}s window",
        FIRST_SLOT_AGE.as_secs(),
        INGESTION_WINDOW.as_secs()
    );

    // THE REPORT, which is the part a constant cannot give: the
    // occupancy, printed on every run so the figure reaches a human
    // without anyone re-deriving it. The needles in
    // `the_quoted_capacity_figures_are_counted_so_a_stale_one_cannot_compile`
    // are built from these same constants.
    eprintln!(
        "replay placement: {PLACED_SPAN_SECS}/{PLACEMENT_BUDGET_SECS} s ({:.1}% occupied, \
         {REMAINING_SECS} s free ≈ {REMAINING_ZERO_SPAN_ROWS} more zero-span rows)",
        100.0 * PLACED_SPAN_SECS as f64 / PLACEMENT_BUDGET_SECS as f64,
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

// corpus-counts: none (replay-coverage-constants) — every figure below is
// recomputed from the corpus and asserted by
// `coverage_is_pinned_and_names_every_exclusion_at_both_levels`. The VALUES live in the code; this prose says which rows moved
// them and why, never how many (issue #248, third round).
/// Pinned coverage (issue #352 steps 2-3). Separate figures, never merged.
///
/// Issue #343 added `b19_offset.test`, and its boundary fix the
/// domain-edge rows. They move the TOTAL and the `derived` exclusion
/// only: `derived` is not a capture claim, so [`PROVENANCE_PERMITS`]
/// does not move, and they are metric queries, which this slice cannot
/// reach in any case.
///
/// Issue #344's execution half moves all the provenance figures.
/// `b18_range_agg_grouping.test` gained captured `eval` rows and
/// converted its interim "not yet executed" `eval_fail` refusals into
/// `eval`s, and its instant `first`/`last` delivery-order fix added the
/// cross-stream tie rows; the first review round added captured rows and
/// a grouped-`avg` row whose captured value was a frontend-dependent
/// `sum/count`. [`REACHABLE`] does NOT move: every newly-permitted row
/// is a metric query, some on a step grid, and this slice replays log
/// (streams) queries at a single instant — so they land in the
/// enumerated gap, not in the coverage.
///
/// Issue #248 adds `b20_nested_ip.test` (`eval` rows plus reject-parity
/// `eval_fail` rows, which carry our own error text and so are not
/// permitted). Most of the permitted rows are streams queries at a
/// single instant over a relative-offset load set, so [`REACHABLE`]
/// moves for the first time since the leg was built; the rest are metric
/// queries. Its second round adds the error-ordering block to the same
/// file — a numeric conversion failing to the LEFT of a leaf that reads
/// the error state — and every row of it is a streams query at a single
/// instant, so all the figures move together.
///
/// Issue #241 adopts the formerly-EXCLUDED `by`-over-a-missing-label
/// sub-cases and the general shapes they instance, across the variants,
/// `label_replace` and grouping-dedup files. The TOTAL moves by all of
/// them; [`PROVENANCE_PERMITS`] and the `config-delta file` exclusion by
/// the share that lands outside a config-delta file. [`REACHABLE`] does
/// NOT move — they are all metric queries.
///
/// Issue #334 adds `b21_key_collisions.test`, captured against the same
/// pinned image with no config delta: every row is a streams query at a
/// single instant over a relative-offset load set, so the figures here
/// move with it together.
///
/// Issue #247 adds `b22_logfmt_expr_reject.test`, captured against the
/// same pinned image with no config delta. Its `eval_fail` rows carry
/// PulsusDB's own error text, so they enlarge the `our-error-text`
/// exclusion rather than [`PROVENANCE_PERMITS`]; the `eval` rows are all
/// streams queries at a single instant over a relative-offset load set,
/// so they move every figure here together.
///
/// Issue #389 adds `b23_json_raw_read.test`, captured against the same
/// pinned image with no config delta. Its captured rows are all streams
/// queries at a single instant over a relative-offset load set, so they
/// move every figure here together; its mid-line-malformed rows carry a
/// `divergence` marker and so enlarge the `pinned-divergence` exclusion
/// instead, leaving [`PROVENANCE_PERMITS`] and [`REACHABLE`] to move by
/// the captured share alone.
///
/// Issue #397 adds the `W` section to `b13_variants.test`, which is a
/// CONFIG-DELTA file (`enable_multi_variant_queries`). So its captured
/// rows enlarge the `config-delta file` exclusion; its `eval_fail` rows
/// marked `divergence(variants-surviving-error-status)` enlarge
/// `pinned-divergence`; and its unmarked `eval_fail` row enlarges
/// `our-error-text`. [`PROVENANCE_PERMITS`] and [`REACHABLE`] therefore
/// do not move at all: the whole section is excluded before
/// reachability is even asked.
///
/// Issue #393 adds `b24_logfmt_expr_eval.test`, captured against the
/// same pinned image with no config delta. Its captured rows are streams
/// queries at a single instant over a relative-offset load set, plus a
/// single instant metric row (the grouping consequence of an
/// empty-valued extracted label), so the streams share moves every
/// figure here together and the metric row enlarges the `metric query`
/// reason instead. Its rows where several extraction identifiers share a
/// source key carry
/// `divergence(logfmt-expression-duplicate-source-key-tiebreak)` and so
/// enlarge `pinned-divergence`.
/// Issue #400 adds `b24_string_escapes.test`, captured against the same
/// pinned image with no config delta. Its `eval_fail` rows carry
/// PulsusDB's own error text and so enlarge the `our-error-text`
/// exclusion; its `eval` rows are streams queries at a single instant
/// over a relative-offset load set, so they move [`PROVENANCE_PERMITS`]
/// and [`REACHABLE`] together.
///
/// Issue #249 adds `b25_structured_metadata.test`, whose every row is
/// excluded as a CONFIG-DELTA file: its capture needs
/// `discover_log_levels: false`, and unlike `b20`'s case the injected
/// `detected_level` is itself structured metadata, so with level
/// discovery on the answers — not merely a stripped label — change. So
/// this figure moves and [`PROVENANCE_PERMITS`] does not.
///
/// Issue #277 adds `b21_variant_series_cap.test`, which is a CONFIG-DELTA
/// file (`enable_multi_variant_queries`), so its captured `eval` rows
/// enlarge the `config-delta file` exclusion. Its root-only `eval_fail`
/// row carries PulsusDB's own error text and the classifier assigns each
/// row a SINGLE reason, so it lands under `our-error-text` instead — the
/// same split issue #397's `b13` section already has. This figure moves
/// by the whole file; [`PROVENANCE_PERMITS`] and [`REACHABLE`] do not
/// move at all.
///
/// Issue #400's second stage adds `b25_re2_reject_parity.test`, which
/// needs no config delta: its `eval` control rows are permitted AND
/// reachable, so this figure, [`PROVENANCE_PERMITS`] and [`REACHABLE`]
/// all move together by that share, while its `eval_fail` rows carry
/// PulsusDB's own error text and land under `our-error-text`.
///
/// Issue #278 adds `b26_line_filter_pushdown.test` and issue #388 adds
/// `b25_pattern_expr_reject.test` and `b26_json_expr.test`, none of
/// which needs a config delta. Their `eval` rows are streams queries at
/// a single instant over a relative-offset load set, so they move this
/// figure, [`PROVENANCE_PERMITS`] and [`REACHABLE`] together; #388's
/// `eval_fail` rows carry PulsusDB's own error text and land under
/// `our-error-text`, which the markers never permitted in the first
/// place.
/// Issue #294 adds `t6_errors_edges.test` rows. They are template-corpus
/// rows, so they move this figure and [`PROVENANCE_PERMITS`] together
/// and leave [`REACHABLE`] alone: the template files carry an absolute
/// timestamp, which is the largest unreachable bucket.
const TOTAL_DIRECTIVES: usize = 1_604;

/// What the provenance markers ALLOW a replay to compare. Named
/// `REPLAYABLE` until the live leg existed, which was wrong: most of
/// these cannot be reached at all. See the module docs.
const PROVENANCE_PERMITS: usize = 1_186;

/// What the live leg can PHYSICALLY compare today. The gap to
/// `PROVENANCE_PERMITS` is enumerated by
/// [`UNREACHABLE_BY_REASON`] — it is not a shortfall to be quietly
/// absorbed into a single figure.
///
/// Every `b21_key_collisions.test` row (issue #334) is reachable: a
/// streams query at a single instant over a relative-offset load set,
/// and so is every permitted `b22_logfmt_expr_reject.test` row (issue
/// #247) — its metric rows are all `eval_fail`, which the markers do not
/// permit in the first place. `b23_json_raw_read.test` (issue #389) is
/// the same shape throughout: every permitted row is a streams query at
/// a single instant over a relative-offset load set, so its whole
/// captured share lands here. `b24_logfmt_expr_eval.test` (issue #393)
/// is that shape too, apart from its instant metric row, which
/// [`TOTAL_DIRECTIVES`] names.
///
/// `b24_string_escapes.test` (issue #400) is
/// that shape too: every `eval` row is a streams query at a single
/// instant over a relative-offset load set, so its whole permitted share
/// lands here and its `eval_fail` rows never enter the question.
/// `b25_re2_reject_parity.test` (issue #400's second stage) is that
/// shape as well, and it was authored to be: its load spans well inside
/// the retired `SLOT_SECS`'s half, which the widest-case assertion
/// below checks rather than takes on trust.
///
/// Issue #278 adds `b26_line_filter_pushdown.test`, the rows that
/// exercise the line filters the planner renders into SQL — which the
/// hermetic runner could not execute until that issue taught it to. It
/// is that same shape throughout and was authored to be: every row is a
/// streams query at a single instant over a relative-offset load set
/// with no config delta, so its whole share moves this figure,
/// [`PROVENANCE_PERMITS`] and [`TOTAL_DIRECTIVES`] together.
///
/// Issue #388's `b25_pattern_expr_reject.test` and `b26_json_expr.test`
/// are that shape too, and were authored to be: every `eval` row is a
/// streams query at a single instant over a relative-offset load set, so
/// their whole permitted share lands here and their `eval_fail` rows
/// never enter the question. **Issue #278's share and issue #388's are
/// DISJOINT over the merge base**, which is why this figure is their sum
/// rather than either alone: taking a side of that rebase conflict whole
/// would have dropped the other issue's rows and still looked plausible.
/// The arithmetic is deliberately not spelled out here — this region
/// states no quantity in prose, by
/// `check_f_marked_regions_state_no_corpus_count` — and lives in
/// `PROVENANCE.md`'s issue #388 section instead.
const REACHABLE: usize = 247;

/// Issue #406 moved `differential_metric_reducers.test`'s `eval_ordered`
/// rows off that file's `ported(...)` default onto
/// `divergence(sort-tie-order)`, so the same rows leave the
/// `not a capture claim (ported)` bucket and enter `pinned-divergence`.
/// The total is unchanged; nothing became replayable or stopped being so.
///
/// Issue #249 adds `b25_structured_metadata.test` to `CONFIG_DELTA_FILES`
/// (its capture needs `discover_log_levels: false`, and the injected
/// `detected_level` is itself structured metadata, so it changes the
/// answers rather than adding a strippable label). Its rows are counted
/// there. Its `eval_fail` row is counted under `config-delta file` and NOT
/// under `our-error-text`, because the classifier assigns each row a
/// single reason and no row lands in both buckets.
///
/// `b25_pattern_expr_reject.test` and `b26_json_expr.test` (issue #388)
/// need no config delta, so their `eval_fail` rows enlarge
/// `our-error-text` alone and their `eval` rows are permitted.
const EXCLUDED_BY_PROVENANCE: &str = "config-delta file=184, not a capture claim (derived)=29, \
not a capture claim (ported)=27, our-error-text (eval_fail)=148, pinned-divergence=30";

/// Issue #344: all of `b18_range_agg_grouping.test`'s newly-permitted
/// rows are metric queries, some of them on a step grid, and this slice
/// replays log (streams) queries at a single instant — so they enlarge
/// the ENUMERATED gap rather than the coverage. Both are levers the
/// module docs already name.
const UNREACHABLE_BY_REASON: &str = "absolute-timestamp (template corpus)=689, \
metric query (slice: streams only)=240, range/matrix eval (slice: instant only)=10";
// corpus-counts: end (replay-coverage-constants)

/// **How far back the reference will still SERVE a pushed entry** —
/// the bound that actually governs slot placement, and it is much
/// tighter than [`INGESTION_WINDOW`].
///
/// `INGESTION_WINDOW` was measured on a coarse ladder (2.5 h served,
/// 3 h not) and the constant took the 3 h end. Issue #278 needed the
/// boundary rather than the bracket, because it moves
/// [`FIRST_SLOT_AGE`] and therefore spends this margin. Measured on a
/// FRESH container from the pinned digest and `ci/logql/config.yaml`,
/// one push carrying every age, queried ten seconds later:
///
/// | age at push | 9000 | 9240 | 9300 | 9360 | 9500 | 9600 | 9640 | 9700 | 9800 |
/// |---|---|---|---|---|---|---|---|---|---|
/// | served | yes | yes | yes | yes | yes | yes | **yes** | **no** | no |
///
/// So the wall sits between 9640 s and 9700 s, not at 10800 s — the
/// window is over-stated by around nineteen minutes. Reproduced on a
/// used container and on a fresh one, and confirmed dynamically: an
/// entry pushed at age 9600 s stopped being served between 30 s and
/// 60 s of wall clock, while one pushed at 9000 s was still served
/// after 421 s. **It behaves as a fixed age**, so an entry placed at
/// age `A` survives roughly `SERVED_HORIZON - A` seconds.
///
/// Taken as 9600 s: below the last age measured served, so it is a
/// floor on the wall rather than an estimate of it.
const SERVED_HORIZON: Duration = Duration::from_secs(9_600);

/// Wall clock the leg is allowed to spend between placing slot 0 and
/// re-reading it, asserted against [`SERVED_HORIZON`] rather than left
/// as a hope. Measured: the whole leg takes about 7 s locally against a
/// fresh container, so this is a factor of twenty-five, not a guess.
const RUN_MARGIN: Duration = Duration::from_secs(180);

/// The separation floor after every case — the minimum gap between one
/// case's last entry and the next case's first.
///
/// It is what the retired `SLOT_SECS` used to buy: a gap at least as
/// wide as the case it follows. The difference is that `SLOT_SECS`
/// charged that price to EVERY case at the WIDEST case's rate, and 146
/// of the 247 reachable cases have a load span of 0s — a single entry,
/// nothing to separate from itself.
///
/// A constant, and deliberately not the corpus's own widest span: the
/// floor is defence in depth against a window-arithmetic error, and that
/// risk does not shrink when the corpus's widest case does.
/// [`the_placement_fits_the_measured_served_horizon`] asserts
/// `widest_s <= MIN_GAP_SECS`, which is literally the retired
/// `2 * widest <= SLOT_SECS` at `SLOT_SECS == 2 * MIN_GAP_SECS`. The
/// property is carried over unweakened, not relaxed — and per case the
/// realised gap is `max(MIN_GAP_SECS, span_i) >= span_i`, which is at
/// least as strong again.
///
/// **The record of the mechanism this replaces, kept rather than
/// deleted** (issue #388's rebase). `SLOT_SECS` gave every case one
/// exclusive fixed-width slot: 60s until issue #389, 45s until issue
/// #393, 40s from then on. It was squeezed from both sides — below by
/// `2 * widest reachable case`, which issue #400's
/// `b24_string_escapes.test` sections spend exactly at 20s, and above by
/// `SLOT_SECS * REACHABLE <= FIRST_SLOT_AGE`. Both levers were spent at
/// once when issue #388's sixteen reachable rows landed on top of issue
/// #278's eight, and the merged tree did not compile: `REACHABLE` came
/// out at 247 against a ceiling of 234, thirteen over. That is the
/// argument for changing the UNIT rather than the numbers, and it is
/// why the ceiling and its headroom constant are gone.
///
/// **`SLOT_SECS`, `MAX_REACHABLE_ROWS` and `REMAINING_SLOTS` survive as
/// PROSE only** — here, in the successor constants' docs below, and in
/// `PROVENANCE.md`'s retired section. That they have no live reference
/// left is a SOURCE-SPELLING property: it is checked by grepping the
/// tree, not by anything that runs. The behavioural half is that the
/// scheme they named is gone — `place()` is the only placement, and
/// [`the_placement_fits_the_measured_served_horizon`] would redden if
/// anything reintroduced a fixed stride. A dead constant reappearing
/// under a different name is exactly what that grep cannot see, and
/// nothing else covers it either.
const MIN_GAP_SECS: u64 = 20;

/// Total wall clock the placement occupies: from the oldest entry to the
/// end of the last case's trailing gap.
///
/// Committed exactly as [`REACHABLE`] is, and asserted against the
/// classifier by BOTH [`the_placement_fits_the_measured_served_horizon`]
/// and the live leg, so it cannot be read as current after the corpus
/// moves. Any corpus edit that moves a load span moves this, and it goes
/// red on the PR with the new figure in the message.
///
/// Measured on the merged tree by [`place`] over the reachable slice:
/// the 247 cases' own spans sum to 885s and their gaps
/// `max(MIN_GAP_SECS, span_i)` sum to 4940s. Against
/// [`PLACEMENT_BUDGET_SECS`] that is 5825/9400 s, i.e. 62.0% occupied.
///
/// **What it replaces, and why the unit changed.** The retired
/// `MAX_REACHABLE_ROWS` was a ROW ceiling,
/// `(SERVED_HORIZON - SLOT_SECS - RUN_MARGIN) / SLOT_SECS` = 234 rows,
/// and issue #278 sat at 231/234 = 98.7% of it. Rows were never the
/// scarce resource: seconds were, and a fixed slot billed 146 zero-span
/// cases at the widest case's rate. Counting the seconds directly is
/// what turned thirteen rows over a ceiling into 38% of a budget free.
const PLACED_SPAN_SECS: u64 = 5_825;

/// What the measured horizon holds once the run's own wall clock and one
/// trailing gap are set aside — the same shape as the retired
/// `MAX_REACHABLE_ROWS`, in the unit that is actually scarce.
const PLACEMENT_BUDGET_SECS: u64 = SERVED_HORIZON.as_secs() - RUN_MARGIN.as_secs() - MIN_GAP_SECS;

/// The occupancy invariant, at COMPILE time. Replaces the retired
/// `REACHABLE <= MAX_REACHABLE_ROWS` and is checked the same way: every
/// term is a constant, so there is nothing to defer to a test run, and a
/// crate that will not build is a harder failure than a red test.
///
/// It fires before any test executes and it still fires if the runtime
/// assertions in [`the_placement_fits_the_measured_served_horizon`] are
/// deleted or that test stops being run. What it cannot see is WHERE the
/// slots are placed — that is the runtime half, and neither subsumes the
/// other.
const _: () = assert!(
    PLACED_SPAN_SECS <= PLACEMENT_BUDGET_SECS,
    "the corpus placement no longer fits the measured served horizon. The budget is \
     SERVED_HORIZON - RUN_MARGIN - MIN_GAP_SECS, in SECONDS: rows are not the scarce \
     resource, corpus span is. A zero-span row costs MIN_GAP_SECS; a wide one costs twice \
     its own span. Narrow a wide case, or re-measure SERVED_HORIZON."
);

/// Free budget at the current corpus — what the next addition is
/// spending. Derived, so it cannot go stale.
const REMAINING_SECS: u64 = PLACEMENT_BUDGET_SECS - PLACED_SPAN_SECS;

/// The free budget expressed in the cheapest row there is, because that
/// is the question a corpus author actually asks. A zero-span row costs
/// [`MIN_GAP_SECS`] and nothing else, so the free budget buys
/// 178 more zero-span rows; a wide row costs twice its own span
/// and buys fewer.
///
/// The retired `REMAINING_SLOTS` doc said, of the fixed-slot scheme:
/// **"The next addition that does not fit needs a narrower widest case,
/// not a bigger number here."** That was true of fixed-width slots,
/// where every row paid the widest case's price, and it is FALSE of what
/// replaces them. The sentence is kept attributed to the scheme it
/// described. What is true now: a wide case still costs twice its own
/// span, but a zero-span case costs [`MIN_GAP_SECS`] instead of twice
/// it, and the next addition spends [`REMAINING_SECS`] rather than
/// forcing a lever.
const REMAINING_ZERO_SPAN_ROWS: u64 = REMAINING_SECS / MIN_GAP_SECS;

/// How far back the oldest entry sits: exactly the placement's own span.
///
/// **No longer a hand-set number to be raised when rows are added.**
/// Raising it is what spent the run margin under the fixed-slot scheme —
/// it was `150 * 60` until issue #278 pushed `SLOT_SECS * REACHABLE`
/// past it, then `156 * 60`, and issue #388's branch had taken it to
/// `160 * 60` before this rework reverted that edit. At `160 * 60` the
/// oldest entry sits at 9600s, which is [`SERVED_HORIZON`] itself: the
/// horizon was re-measured live as served at 9640s and not at 9700s, so
/// an entry placed there has tens of seconds of life, not the 1160s the
/// edit's own comment claimed against [`INGESTION_WINDOW`].
///
/// Derived from [`PLACED_SPAN_SECS`], so the lever no longer exists.
/// [`the_placement_fits_the_measured_served_horizon`] asserts it equals
/// the total [`place`] MEASURES over the live corpus, not merely the
/// constant beside it — otherwise typing a literal here would satisfy
/// every check while the derivation was broken.
const FIRST_SLOT_AGE: Duration = Duration::from_secs(PLACED_SPAN_SECS);

/// One case's slot, in seconds — every bound this leg has is in seconds,
/// and a nanosecond ladder would only invite rounding arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Slot {
    /// Start, relative to the oldest entry.
    offset_secs: u64,
    /// The case's own sample span, rounded up.
    span_secs: u64,
}

/// A case's own sample span in whole seconds, rounded UP — a 4.5s case
/// is charged 5s, never 4. The same derivation the retired widest-case
/// assertion used (`div_ceil(1_000_000_000)` over the min and max sample
/// timestamps), lifted into a function so the widest-case bound and the
/// placement cannot disagree about what a span is.
fn case_span_secs(case: &ReplayCase) -> u64 {
    let mut lo = i64::MAX;
    let mut hi = i64::MIN;
    for (ts, _, _) in case.load.iter().flat_map(|s| s.samples.iter()) {
        lo = lo.min(*ts);
        hi = hi.max(*ts);
    }
    if lo > hi {
        return 0;
    }
    ((hi - lo) as u64).div_ceil(1_000_000_000)
}

/// **The ONE placement definition.**
///
/// [`the_placement_fits_the_measured_served_horizon`] and the live leg
/// both call this, so the budget that is CHECKED and the timestamps that
/// are PUSHED cannot drift apart. Under the fixed slot they were two
/// separate expressions — `SLOT_SECS * REACHABLE` in the test and
/// `base + i * SLOT_SECS` in the leg — which agreed only by inspection.
///
/// Each case starts where the previous one's gap ended; the gap after a
/// case is `max(MIN_GAP_SECS, its own span)`. The returned total
/// INCLUDES the last case's trailing gap, so [`FIRST_SLOT_AGE`] can be
/// that total and the newest entry still lands at least
/// [`MIN_GAP_SECS`] before `now`.
fn place(cases: &[&ReplayCase]) -> (Vec<Slot>, u64) {
    let mut slots = Vec::with_capacity(cases.len());
    let mut cursor = 0u64;
    for case in cases {
        let span_secs = case_span_secs(case);
        slots.push(Slot {
            offset_secs: cursor,
            span_secs,
        });
        cursor += span_secs + span_secs.max(MIN_GAP_SECS);
    }
    (slots, cursor)
}

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
/// the config-delta files that need exactly that setting
/// (`b12_error_pair_model`, `b14_detected_fields`; their share of the
/// exclusion is in [`EXCLUDED_BY_PROVENANCE`]). It is
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
    //
    // **The placement comes from `place()`, the same function
    // `the_placement_fits_the_measured_served_horizon` checks the budget
    // with.** These are its only two callers. Under the retired fixed
    // slot the two were separate expressions — `SLOT_SECS * REACHABLE`
    // there and `base + i * SLOT_SECS` here — so either could be changed
    // without the other noticing; now a change to one IS a change to the
    // other.
    let placed: Vec<&ReplayCase> = cases.iter().map(|(_, c)| *c).collect();
    let (slots, placed_total) = place(&placed);
    // A live run must not push against a stale placement: the layout
    // test is a DIFFERENT test and may not have run in this session.
    assert_eq!(
        placed_total, PLACED_SPAN_SECS,
        "the placement spans {placed_total}s, not the committed {PLACED_SPAN_SECS}s — the          corpus moved. Run the hermetic lane first; this leg is about to push against a          layout nothing has checked."
    );
    let base_slot_s = unix_now_secs() - FIRST_SLOT_AGE.as_secs();
    let mut plan = Vec::new();
    for (i, (row, case)) in cases.iter().enumerate() {
        let slot_start_ns = (base_slot_s + slots[i].offset_secs) as i64 * 1_000_000_000;
        let case_min_ns = case
            .load
            .iter()
            .flat_map(|s| s.samples.iter())
            .map(|(ts, _, _)| *ts)
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
                .map(|(ts, sm, body)| {
                    let ts = (slot_start_ns + (ts - case_min_ns)).to_string();
                    // Issue #249: a `sm{...}` sample replays as the push
                    // API's THIRD element, so the container sees the same
                    // per-entry structured metadata the hermetic store
                    // does. A metadata-free sample keeps the two-element
                    // form byte for byte.
                    if sm.is_empty() {
                        serde_json::json!([ts, body.clone()])
                    } else {
                        let map: serde_json::Map<String, serde_json::Value> = parse_labelset(sm)
                            .expect("the runner rendered this metadata JSON")
                            .0
                            .into_iter()
                            .map(|(k, v)| (k, serde_json::Value::String(v)))
                            .collect();
                        serde_json::json!([ts, body.clone(), map])
                    }
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
            .map(|(ts, _, _)| *ts)
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

    // **The oldest entry must still be inside the window.** This leg
    // spends real wall clock between placing the slots and reading them
    // — `SERVED_HORIZON - FIRST_SLOT_AGE` is all the margin there is,
    // and it is now 3775s because `FIRST_SLOT_AGE` is the placement's
    // own total rather than a hand-set age. If the run overran, slot 0's
    // entries have aged out of what the reference will serve and EVERY
    // row that reads like a mismatch is one, so the diagnosis has to be
    // made here rather than left to whoever reads the failure.
    //
    // Slot 0 is the oldest by construction — `place()` returns offsets
    // that only increase, and the slots march FORWARD from
    // `FIRST_SLOT_AGE` back.
    //
    // **What it cannot see:** a MIDDLE slot that was never served (it
    // checks the oldest, which is the one that ages out first), and
    // anything at all in the hermetic lane — this is the nightly leg.
    if !cases.is_empty() {
        let (slot_zero_ns, _, _) = plan[0];
        let alive = query_case(&base, slot_zero_ns, 0, 0, r#"{replay_sentinel="0"}"#);
        assert!(
            !alive.is_empty(),
            "PRECONDITION FAILURE, NOT A CORPUS FAILURE. The oldest slot's sentinel has \
             vanished, so slot 0 aged past the {}s of history the reference still SERVES \
             while this run was still going.\n\n\
             THE COMMITTED CORPUS VALUES ARE NOT IMPLICATED — do not go looking at them. The \
             run took longer than the {}s the oldest entry had ({}s served horizon minus the \
             {}s FIRST_SLOT_AGE, which is the placement's own span), so its rows were \
             compared against a reference that had stopped serving them. Without this check \
             that surfaces as `N of {REACHABLE} replayed rows disagree with the \
             reference`.\n\n\
             The budget is SERVED_HORIZON, not INGESTION_WINDOW: the latter is the coarse \
             bracket the window was first measured on and is looser by around nineteen \
             minutes.\n\n\
             The lever is the CORPUS, not a constant: the placement charges each case its own \
             span plus max(MIN_GAP_SECS, that span), so narrowing a wide case shortens \
             FIRST_SLOT_AGE directly. Both bounds are asserted by \
             `the_placement_fits_the_measured_served_horizon`.",
            SERVED_HORIZON.as_secs(),
            SERVED_HORIZON.as_secs() - FIRST_SLOT_AGE.as_secs(),
            SERVED_HORIZON.as_secs(),
            FIRST_SLOT_AGE.as_secs(),
        );
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
