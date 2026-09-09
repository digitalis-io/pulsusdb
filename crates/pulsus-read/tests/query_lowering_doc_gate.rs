//! Issue #492: the gates that hold the compile core's inspectable
//! projection to the document that publishes it.
//!
//! # What is here, and what is not
//!
//! The design record for this work nominates **eleven** document gates.
//! **The records they parse are committed** — `8f3d0c6d` (#517) added
//! `docs/query-lowering.md`, `docs/query-to-sql.md` and both diagrams to
//! the tree, and `git ls-files docs/` lists all four. The paragraph that
//! used to stand here said they were untracked working notes a committed
//! test could not read; that was true when it was written and is not
//! true now.
//!
//! **Three of the eleven exist.** One reads
//! [`docs/api.md`](../../../docs/api.md), and it is the one that matters
//! most for a wire surface: its two sides are genuinely independent
//! producers — the keys come from a serializer and the expectation from a
//! document in another directory, so neither can produce the other. The
//! second is `the_documented_plan_example_round_trips_through_the_renderer_shape`.
//! The third is `the_hops_diagram_and_the_document_agree_on_the_lowered_request`,
//! added by issue #492 part 8's first landing. The other **nine remain
//! owed by part 8's second landing** (item 3 of that issue's scope
//! enumeration).
//!
//! **Issue #492 part 8, first landing: §9.2 stops being unreproducible.**
//! `the_lowering_evidence_has_a_row_per_read`,
//! `every_figure_section_9_2_states_is_the_one_the_artefact_holds` and
//! `every_ratio_in_section_9_2_is_the_quotient_of_two_printed_figures`
//! read [`docs/benchmarks/data/traces-lowering-92.json`], which holds one
//! `system.query_log` row per statement, and compare every figure §9.2
//! and §9.2b publish against a total over those rows. Before that
//! landing the section's figures came from a run whose corpus was
//! committed nowhere and whose rows had been discarded.
//!
//! **Two further tests in this file are not among the eleven** and are
//! not claimed to be: `the_hops_diagram_marks_its_superseded_figures_on_its_own_face`
//! and `the_record_flags_the_survivor_nobody_re_measured` assert that a
//! superseded figure carries its marker, which is a different question
//! from whether two artefacts agree. The first of those **returns early
//! now that the drawing is redrawn** — it is a conditional rule and its
//! condition is false. What holds the drawing today is
//! `the_hops_diagram_and_the_document_agree_on_the_lowered_request`,
//! which lands in the same commit as the redraw, and
//! `no_superseded_lowered_cost_figure_survives_the_re_measurement`, which
//! asserts unconditionally that no superseded lowered figure is left on
//! its face.

use std::collections::BTreeSet;

use pulsus_read::compile::plan::{
    BoundShape, CutShape, EnginePartShape, HandoffCost, LinkShape, PartShape, PlanShape, SeedShape,
    SqlPartShape,
};

/// The line in `docs/api.md` the documented example follows. Anchoring on
/// the sentence rather than on a line number is what stops this gate
/// drifting silently when the section moves.
const ANCHOR: &str = "**The `plan` key's complete shape** (issue #492):";

fn api_md() -> String {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    std::fs::read_to_string(root.join("docs/api.md")).expect("read docs/api.md")
}

/// The first fenced block after [`ANCHOR`].
fn documented_example(md: &str) -> String {
    let after = md
        .split_once(ANCHOR)
        .unwrap_or_else(|| panic!("docs/api.md must carry the anchor {ANCHOR:?}"))
        .1;
    let body = after
        .split_once("```json\n")
        .unwrap_or_else(|| panic!("no fenced JSON block after {ANCHOR:?}"))
        .1;
    body.split_once("\n```")
        .unwrap_or_else(|| panic!("unterminated fence after {ANCHOR:?}"))
        .0
        .to_string()
}

/// Every object key in a JSON value, at any depth.
fn keys(v: &serde_json::Value, out: &mut BTreeSet<String>) {
    match v {
        serde_json::Value::Object(m) => {
            for (k, child) in m {
                out.insert(k.clone());
                keys(child, out);
            }
        }
        serde_json::Value::Array(a) => {
            for child in a {
                keys(child, out);
            }
        }
        _ => {}
    }
}

/// A `PlanShape` exercising **every variant the renderer can emit**, so
/// that the key set it serialises to is the renderer's whole vocabulary
/// and not the vocabulary of one lucky plan.
fn maximal_shape() -> PlanShape {
    PlanShape {
        parts: vec![
            PartShape::Sql(Box::new(SqlPartShape {
                kind: "sql",
                name: "log_streams_idx".to_string(),
                issue: "once",
                cut: None,
                seed: None,
                yields: "exact",
            })),
            PartShape::Sql(Box::new(SqlPartShape {
                kind: "sql",
                name: "log_samples".to_string(),
                issue: "per_seed:keyset",
                cut: Some(CutShape {
                    why: "source_handoff",
                    source: Some("log_samples".to_string()),
                    key: Some("fingerprint".to_string()),
                    sources: Vec::new(),
                    cost: None,
                }),
                seed: Some(SeedShape {
                    from: vec![0],
                    bound: BoundShape {
                        kind: "constant",
                        name: Some("DEFAULT_MAX_STREAMS"),
                        value: 100_000,
                    },
                }),
                yields: "candidates",
            })),
            PartShape::Sql(Box::new(SqlPartShape {
                kind: "sql",
                name: "trace_attrs_idx".to_string(),
                issue: "per_seed:chunks",
                cut: Some(CutShape {
                    why: "handoff_exceeds_bound",
                    source: None,
                    key: None,
                    sources: Vec::new(),
                    cost: Some(HandoffCost {
                        text_bytes: 1_409_081,
                        ast_elements: 65_540,
                    }),
                }),
                seed: Some(SeedShape {
                    from: vec![0],
                    bound: BoundShape {
                        kind: "request_limit",
                        name: None,
                        value: 20,
                    },
                }),
                yields: "candidates",
            })),
            PartShape::Sql(Box::new(SqlPartShape {
                kind: "sql",
                name: "trace_spans".to_string(),
                issue: "once",
                cut: Some(CutShape {
                    why: "disjoint_sources",
                    source: None,
                    key: None,
                    sources: vec!["trace_spans".to_string(), "trace_attrs_idx".to_string()],
                    cost: None,
                }),
                seed: None,
                yields: "reduced",
            })),
            PartShape::Engine(EnginePartShape {
                kind: "engine",
                links: vec![2, 3],
            }),
        ],
        links: vec![
            LinkShape {
                i: 0,
                part: 0,
                stage: "Source".to_string(),
                how: "lowered",
                fidelity: Some("equivalent"),
                why: None,
            },
            LinkShape {
                i: 1,
                part: 0,
                stage: "LineFilter".to_string(),
                how: "lowered",
                fidelity: Some("wider"),
                why: None,
            },
            LinkShape {
                i: 2,
                part: 4,
                stage: "Parser(Json)".to_string(),
                how: "residual",
                fidelity: None,
                why: Some("not_yet_lowered"),
            },
        ],
    }
}

/// Every key `QueryPlan::shape()` renders is a key `docs/api.md`
/// documents for `data.explain.plan`, and **no other**.
///
/// Both directions are asserted, because they catch different mistakes: a
/// key the renderer emits and the document omits is an undocumented wire
/// field, and a key the document promises and the renderer never emits is
/// a promise to a client that will never be kept.
#[test]
fn the_plan_shape_json_keys_match_the_api_document() {
    let rendered = serde_json::to_value(maximal_shape()).expect("the plan shape serialises");
    let mut ours = BTreeSet::new();
    keys(&rendered, &mut ours);

    let md = api_md();
    let example = documented_example(&md);
    let documented: serde_json::Value = serde_json::from_str(&example)
        .unwrap_or_else(|e| panic!("the documented example must be valid JSON: {e}\n{example}"));
    let mut theirs = BTreeSet::new();
    keys(&documented, &mut theirs);

    assert!(
        !ours.is_empty(),
        "the renderer emits no keys at all — the maximal shape is wrong, not the document"
    );
    assert_eq!(
        ours,
        theirs,
        "docs/api.md's `data.explain.plan` example and the renderer disagree.\n  \
         only in the renderer: {:?}\n  only in the document: {:?}",
        ours.difference(&theirs).collect::<Vec<_>>(),
        theirs.difference(&ours).collect::<Vec<_>>()
    );
}

/// The documented example is not merely key-compatible: it PARSES as the
/// shape it documents, so a client generated from it reads the same
/// fields the renderer writes.
#[test]
fn the_documented_plan_example_round_trips_through_the_renderer_shape() {
    let md = api_md();
    let example = documented_example(&md);
    let documented: serde_json::Value =
        serde_json::from_str(&example).expect("the documented example is valid JSON");
    let parts = documented["parts"]
        .as_array()
        .expect("the example carries parts");
    assert_eq!(parts.len(), 5, "four statements and one engine part");
    assert_eq!(
        parts.iter().filter(|p| p["kind"] == "engine").count(),
        1,
        "exactly one engine part in the example"
    );
    // Every `why` in the example is a cut word the renderer can produce.
    let mut whys: Vec<&str> = parts
        .iter()
        .filter_map(|p| p["cut"]["why"].as_str())
        .collect();
    whys.sort_unstable();
    assert_eq!(
        whys,
        vec![
            "disjoint_sources",
            "handoff_exceeds_bound",
            "source_handoff"
        ],
        "the example shows three of the four cuts; the fourth carries no extra key and is \
         described in the sentence beneath it"
    );
}

// ---------------------------------------------------------------------
// Issue #492 part 4 — the superseded lowered-cost figure and its markers
// ---------------------------------------------------------------------

const QUERY_LOWERING: &str = "docs/query-lowering.md";
const QUERY_TO_SQL: &str = "docs/query-to-sql.md";
const HOPS_SVG: &str = "docs/diagrams/query-lowering-hops.svg";

/// The sentence both markers open with, so the record and the drawing
/// cannot drift apart silently.
const MARKER_SENTENCE: &str = "The same answer lowered is four statements, not one: 4 round trips.";
/// The tag the superseded figure used to carry while it was still the
/// document's live lowered total.
const MARKER_TAG: &str = "seed + root only";
/// The superseded figure itself.
const SUPERSEDED_FIGURE: &str = "43,636";
/// Issue #492 part 8. **The tag a retirement paragraph must carry.** The
/// re-measurement replaced the figure, so it may still be NAMED — a
/// reader who meets `43,636` elsewhere should find it accounted for —
/// but only inside a paragraph that says it has been retired.
const RETIREMENT_TAG: &str = "superseded by the §9.2 re-measurement";
/// The wordings the correction replaces. Each was checked against both
/// records: together they matched the seven changing sites and nothing
/// else, so a contributor who edits an unrelated "two statements" line
/// does not have to touch them.
const SUPERSEDED_WORDING: [&str; 5] = [
    "lowered is two statements",
    "**Lowered** — **two statements**",
    "Two round trips, not one",
    "two statements in total",
    "search is two statements, not one",
];

/// The strings on the DRAWING that say it still carries the
/// two-statement model. While any of them is present the marker must be
/// too; when part 8 redraws and none is, the gate passes trivially.
const TWO_STATEMENT_WORDING: [&str; 8] = [
    "2 round trips",
    "2 SQL parts",
    "43,636 B",
    "43,636 result bytes",
    "9,871,360 rows",
    "1,205 granules",
    "1,756x fewer bytes",
    "555x fewer round trips",
];

const UNVERIFIED_TAG: &str = "Unverified survivors, nobody re-measured them";
/// Issue #492 part 8 narrowed this from three figures to one. The
/// peak-memory pair was re-measured on C1 (§9.2b: `169,061,322` /
/// `193,209,406` with a `1.14×`), so it is no longer a survivor; the
/// client's `11,340 B` was measured on **corpus C2**, which part 8 did
/// not rebuild, so it is carried forward still flagged.
const UNVERIFIED_FIGURES: [&str; 1] = ["11,340"];

fn repo_file(rel: &str) -> String {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    std::fs::read_to_string(root.join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
}

/// Blocks separated by blank lines. Paragraph-scoped rather than
/// line-scoped because both records are hard-wrapped: a line rule would
/// turn a re-wrap into a failure.
fn paragraphs(md: &str) -> Vec<&str> {
    md.split("\n\n").collect()
}

/// **REPLACES `every_superseded_lowered_cost_figure_carries_its_marker`
/// (issue #492 part 4), which part 8's re-measurement retired the
/// subject of.**
///
/// Part 4's gate asserted that every paragraph quoting the two-statement
/// lowered total also carried a `seed + root only` tag, and that the
/// figure was quoted somewhere — a rule about a LIVE figure. §9.2b now
/// measures all four lowered statements, so that figure is not this
/// document's total any more and part 4's gate could not pass: its
/// `seen > 0` clause is about a figure that is no longer stated. The
/// gate is replaced rather than deleted, and the state it asserts is the
/// new one.
///
/// Four rules:
///
/// 1. the drawing carries **neither** the superseded figure nor the tag,
///    and none of the eight two-statement wordings — it is redrawn from
///    the artefact, so there is nothing left to mark;
/// 2. in BOTH records, every paragraph naming the figure or the tag also
///    carries [`RETIREMENT_TAG`], so the number cannot be re-quoted as a
///    live one;
/// 3. `docs/query-lowering.md` names it at least once, so rule 2 is
///    checking something;
/// 4. neither record carries any of the five superseded wordings.
///
/// Whitespace is normalised before matching, because both records are
/// hard-wrapped and a re-wrap must not turn into a failure.
#[test]
fn no_superseded_lowered_cost_figure_survives_the_re_measurement() {
    let svg = repo_file(HOPS_SVG);
    for stale in [SUPERSEDED_FIGURE, MARKER_TAG] {
        assert!(
            !svg.contains(stale),
            "{HOPS_SVG} still carries {stale:?}: part 8 redrew it from \
             docs/benchmarks/data/traces-lowering-92.json, so no superseded lowered figure \
             survives on its face"
        );
    }
    let still_drawn: Vec<&str> = TWO_STATEMENT_WORDING
        .into_iter()
        .filter(|w| svg.contains(w))
        .collect();
    assert!(
        still_drawn.is_empty(),
        "{HOPS_SVG} still carries two-statement figures {still_drawn:?} after the redraw"
    );

    let mut seen = 0usize;
    for rel in [QUERY_LOWERING, QUERY_TO_SQL] {
        let text = repo_file(rel);
        for para in paragraphs(&text) {
            let flat = para.split_whitespace().collect::<Vec<_>>().join(" ");
            if !flat.contains(SUPERSEDED_FIGURE) && !flat.contains(MARKER_TAG) {
                continue;
            }
            if rel == QUERY_LOWERING {
                seen += 1;
            }
            assert!(
                flat.contains(RETIREMENT_TAG),
                "{rel}: a paragraph names {SUPERSEDED_FIGURE:?} or {MARKER_TAG:?} without saying \
                 it is {RETIREMENT_TAG:?}. It opens: {:?}",
                para.lines().next().unwrap_or("")
            );
        }
        for wording in SUPERSEDED_WORDING {
            assert!(
                !text.contains(wording),
                "{rel} still carries the superseded wording {wording:?}: the lowered form is four \
                 statements, not two"
            );
        }
    }
    assert!(
        seen > 0,
        "{QUERY_LOWERING} names {SUPERSEDED_FIGURE:?} nowhere — rule 2 is checking nothing, and a \
         reader who meets the number elsewhere has no way to learn it was retired"
    );
}

/// Issue #492 part 4. **The drawing says on its own face that its lowered
/// figures are superseded.**
///
/// A marker in a paragraph of the record does not reach the person who
/// opens the picture, and a picture asserts a design more confidently
/// than a sentence does. Six assertions, in order:
///
/// 1. the conditional — while the drawing carries any two-statement
///    figure it must carry the marker sentence; when part 8 redraws and
///    none is left, this returns early;
/// 2. the tag is present, so a reader can tell which figures are meant;
/// 3. the marker appears AFTER `</desc>`, i.e. in drawn content — a
///    marker only in the metadata is grep-visible and invisible to a
///    person opening the file;
/// 4. the marker's last baseline is inside the `viewBox` height;
/// 5. the `.supersede` rule renders at 13px or more, and `<text>` /
///    `<tspan>` open and close counts balance (no XML parser is in this
///    workspace's lock file and part 4 does not add one);
/// 6. the drawn marker carries the unverified-survivor line and names all
///    three of its figures.
///
/// **What this cannot see is whether the marker is VISIBLE.** Painting
/// `.supersede` and the banner's stroke in the background colour leaves
/// every string above in place and this test green. That is criterion
/// 19's job: a headless render and a bounding-box measurement, run on a
/// host shell at part 4's landing and recorded in the implementation
/// notes.
#[test]
fn the_hops_diagram_marks_its_superseded_figures_on_its_own_face() {
    let svg = repo_file(HOPS_SVG);
    let stale: Vec<&str> = TWO_STATEMENT_WORDING
        .into_iter()
        .filter(|w| svg.contains(w))
        .collect();
    if stale.is_empty() {
        // Part 8 has redrawn: there is nothing left to mark.
        return;
    }
    assert!(
        svg.contains(MARKER_SENTENCE),
        "{HOPS_SVG} still carries two-statement figures {stale:?} but no superseded marker: the \
         sentence {MARKER_SENTENCE:?} appears nowhere in the file"
    );
    assert!(
        svg.contains(MARKER_TAG),
        "{HOPS_SVG} carries the marker sentence but not the tag {MARKER_TAG:?}"
    );

    let drawn = svg
        .split_once("</desc>")
        .unwrap_or_else(|| panic!("{HOPS_SVG} must carry a <desc>"))
        .1;
    assert!(
        drawn.contains(MARKER_SENTENCE),
        "the marker is only in <desc>: it is grep-visible but nobody opening {HOPS_SVG} sees it"
    );

    let node = drawn
        .split_once("<text id=\"supersede\"")
        .unwrap_or_else(|| panic!("{HOPS_SVG} must carry a <text id=\"supersede\"> node"))
        .1
        .split_once("</text>")
        .expect("the marker node is closed")
        .0;
    let baseline = attr_number(node, " y=\"").expect("the marker node carries a baseline")
        + node
            .match_indices(" dy=\"")
            .filter_map(|(i, _)| number_after(&node[i + 5..]))
            .sum::<f64>();
    let view_height = attr_number(&svg, "viewBox=\"0 0 1120 ").expect("the viewBox names a height");
    assert!(
        baseline <= view_height,
        "the marker's last baseline is at y={baseline} but the viewBox ends at {view_height}: it \
         is drawn off the canvas and is invisible when the file is opened"
    );

    let size = svg
        .split_once(".supersede { font: ")
        .map(|(_, rest)| rest)
        .and_then(number_after)
        .expect("the .supersede rule names a font size");
    assert!(
        size >= 13.0,
        "the marker renders at {size}px; below 13px it is not legible beside 12px body text"
    );
    assert_eq!(
        svg.matches("<tspan").count(),
        svg.matches("</tspan>").count(),
        "unbalanced <tspan> elements in {HOPS_SVG}: a hand edit left the file malformed"
    );
    assert_eq!(
        svg.matches("<text").count(),
        svg.matches("</text>").count(),
        "unbalanced <text> elements in {HOPS_SVG}: a hand edit left the file malformed"
    );

    assert!(
        node.contains(UNVERIFIED_TAG),
        "the drawn marker does not carry {UNVERIFIED_TAG:?}: part 8 re-measures what the marker \
         lists, and a survivor nobody checked is not on the list"
    );
    let line = node
        .lines()
        .find(|l| l.contains(UNVERIFIED_TAG))
        .expect("the tag sits on one line");
    let missing: Vec<&str> = UNVERIFIED_FIGURES
        .into_iter()
        .filter(|f| !line.contains(f))
        .collect();
    assert!(
        missing.is_empty(),
        "the drawn marker's unverified-survivor line does not name {missing:?}"
    );
}

/// **The record flags the figure nobody re-measured.**
///
/// Issue #492 part 4 wrote this over three figures. Part 8 re-measured
/// two of them — the peak-memory pair, on corpus C1 — and did **not**
/// rebuild corpus C2, so the client's `11,340 B` is the one that is
/// still carried on argument alone. The gate is narrowed to what is
/// still true rather than deleted: a figure marked *survives* is never
/// looked at again unless something keeps the flag in the record.
#[test]
fn the_record_flags_the_survivor_nobody_re_measured() {
    let text = repo_file(QUERY_LOWERING);
    let para = paragraphs(&text)
        .into_iter()
        .find(|p| p.to_lowercase().contains("unverified survivor"))
        .unwrap_or_else(|| {
            panic!(
                "{QUERY_LOWERING} carries no paragraph containing \"unverified survivor\": part 8 \
                 re-measures what the marker lists"
            )
        });
    for figure in UNVERIFIED_FIGURES {
        assert!(
            para.contains(figure),
            "the unverified-survivor paragraph in {QUERY_LOWERING} does not name {figure:?}; it \
             reads: {:?}",
            &para[..para.len().min(160)]
        );
    }
}

/// The first decimal number in `text`, or `None`.
fn number_after(text: &str) -> Option<f64> {
    let digits: String = text
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    digits.parse().ok()
}

/// The number following the first occurrence of `key`.
fn attr_number(text: &str, key: &str) -> Option<f64> {
    text.split_once(key)
        .and_then(|(_, rest)| number_after(rest))
}

// ---------------------------------------------------------------------
// Issue #492 part 8 — §9.2 and §9.2b against the retained measurement
//
// Before part 8, §9.2's figures rested on a run whose corpus was
// committed nowhere and whose `system.query_log` rows had been discarded,
// so nothing in this repository could re-derive one of them. The
// re-measurement retains ONE ROW PER STATEMENT
// (`docs/benchmarks/data/traces-lowering-92.json`, written by
// `cargo xtask bench traces-lowering`), and the three checks below total
// those rows and compare every published cell against its total.
// ---------------------------------------------------------------------

/// The retained measurement: one object per `query_id`, never a summary.
const LOWERING_EVIDENCE: &str = "docs/benchmarks/data/traces-lowering-92.json";

const S92_HEADING: &str = "### 9.2 The worked query, per stage";
const S92B_HEADING: &str = "### 9.2b The lowered request, per stage";
const S93_HEADING: &str = "### 9.3 The correctness consequence, measured";

/// Which request a statement belongs to.
#[derive(serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum Form {
    Current,
    Lowered,
}

/// Which KIND of read a statement is.
#[derive(serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum Stage {
    Generator,
    Hydration,
    Membership,
    RootRead,
}

/// The artefact's row, mirrored here because the producer type lives in
/// `xtask`, which depends on this crate — importing it would pull the
/// benchmark crate's two ClickHouse clients into the `ci` job's build.
///
/// `deny_unknown_fields` is load-bearing: it is what makes a producer
/// field that reaches the file impossible to ignore. A key the mirror
/// does not know is a hard deserialisation error, not a dropped field.
#[derive(serde::Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
struct LoweringStageRow {
    form: Form,
    stage: Stage,
    #[allow(dead_code)]
    query_id: String,
    seq: u32,
    read_rows: u64,
    read_bytes: u64,
    read_compressed_bytes: u64,
    fd_read_bytes: u64,
    result_bytes: u64,
    selected_marks: u64,
    memory_usage: u64,
    max_block_size_submitted: u64,
    settings_max_block_size_logged: String,
}

#[derive(serde::Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
struct LoweringEvidence {
    #[allow(dead_code)]
    clickhouse_version: String,
    #[allow(dead_code)]
    corpus: String,
    #[allow(dead_code)]
    query: String,
    #[allow(dead_code)]
    limit: u32,
    rows: Vec<LoweringStageRow>,
}

/// Every `u64` field of the row schema, one variant each. The partition
/// below is over THIS list, not over the document's columns: two
/// accumulators share the `off file system †` header cell, so a
/// header-keyed closure stays green when one of them stops summing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RowField {
    ReadRows,
    ReadBytes,
    ReadCompressedBytes,
    FdReadBytes,
    SelectedMarks,
    ResultBytes,
    MemoryUsage,
    MaxBlockSizeSubmitted,
}

impl RowField {
    const ALL: [RowField; 8] = [
        RowField::ReadRows,
        RowField::ReadBytes,
        RowField::ReadCompressedBytes,
        RowField::FdReadBytes,
        RowField::SelectedMarks,
        RowField::ResultBytes,
        RowField::MemoryUsage,
        RowField::MaxBlockSizeSubmitted,
    ];

    /// No `_` arm: a new field fails to build until it names itself.
    fn name(self) -> &'static str {
        match self {
            RowField::ReadRows => "read_rows",
            RowField::ReadBytes => "read_bytes",
            RowField::ReadCompressedBytes => "read_compressed_bytes",
            RowField::FdReadBytes => "fd_read_bytes",
            RowField::SelectedMarks => "selected_marks",
            RowField::ResultBytes => "result_bytes",
            RowField::MemoryUsage => "memory_usage",
            RowField::MaxBlockSizeSubmitted => "max_block_size_submitted",
        }
    }

    /// No `_` arm: a new field fails to build until it names its accessor.
    fn get(self, row: &LoweringStageRow) -> u64 {
        match self {
            RowField::ReadRows => row.read_rows,
            RowField::ReadBytes => row.read_bytes,
            RowField::ReadCompressedBytes => row.read_compressed_bytes,
            RowField::FdReadBytes => row.fd_read_bytes,
            RowField::SelectedMarks => row.selected_marks,
            RowField::ResultBytes => row.result_bytes,
            RowField::MemoryUsage => row.memory_usage,
            RowField::MaxBlockSizeSubmitted => row.max_block_size_submitted,
        }
    }
}

/// Row keys that carry a JSON number and are not a quantity.
const NON_QUANTITY_ROW_KEYS: [(&str, &str); 1] = [(
    "seq",
    "a 0-based index within (form, stage), not a measurement",
)];

/// Why a field is deliberately not totalled. A closed set of two, never a
/// free string: a rationale that lives only in prose cannot be checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NotSummed {
    /// §9.2 publishes this as a maximum over a named subset of rows,
    /// never a total. A maximum cannot overflow, so it needs no
    /// accumulator and gets none.
    MaximumOverRows,
    /// The value every statement was submitted with. Requirement (h) of
    /// the criterion checks it: one distinct value across the artefact's
    /// rows, equal to `equals`.
    InstrumentConstant { equals: u64 },
}

impl NotSummed {
    fn rendered(self) -> String {
        match self {
            NotSummed::MaximumOverRows => "maximum over rows".to_string(),
            NotSummed::InstrumentConstant { equals } => {
                format!("instrument constant = {equals}")
            }
        }
    }
}

const DECLARED_NOT_SUMMED: [(RowField, NotSummed); 2] = [
    (RowField::MemoryUsage, NotSummed::MaximumOverRows),
    (
        RowField::MaxBlockSizeSubmitted,
        NotSummed::InstrumentConstant {
            equals: pulsus_read::TRACE_SEARCH_MAX_BLOCK_ROWS,
        },
    ),
];

/// One accumulator, each carrying the header cell it answers to. Seven
/// variants over six per-stage header cells, because `off file system †`
/// has two candidate instruments and the harness records both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SummedColumn {
    Queries,
    RowsRead,
    Decoded,
    OffFileSystemCompressed,
    OffFileSystemFd,
    GranuleMarks,
    ResultBytes,
}

impl SummedColumn {
    const ALL: [SummedColumn; 7] = [
        SummedColumn::Queries,
        SummedColumn::RowsRead,
        SummedColumn::Decoded,
        SummedColumn::OffFileSystemCompressed,
        SummedColumn::OffFileSystemFd,
        SummedColumn::GranuleMarks,
        SummedColumn::ResultBytes,
    ];

    /// `(per-stage header, comparison header)`. No `_` arm: a new variant
    /// fails to build until it declares its headers.
    fn headers(self) -> (&'static str, Option<&'static str>) {
        match self {
            SummedColumn::Queries => ("queries", Some("round trips")),
            SummedColumn::RowsRead => ("rows read", Some("rows read")),
            SummedColumn::Decoded => ("decoded †", None),
            SummedColumn::OffFileSystemCompressed => ("off file system †", None),
            SummedColumn::OffFileSystemFd => ("off file system †", None),
            SummedColumn::GranuleMarks => ("granules (avg)", Some("granules")),
            SummedColumn::ResultBytes => ("result bytes", Some("result bytes")),
        }
    }

    /// The row field this accumulator sums; `None` for the counter, which
    /// sums one per row. No `_` arm: a new variant fails to build until
    /// it names its field.
    fn field(self) -> Option<RowField> {
        match self {
            SummedColumn::Queries => None,
            SummedColumn::RowsRead => Some(RowField::ReadRows),
            SummedColumn::Decoded => Some(RowField::ReadBytes),
            SummedColumn::OffFileSystemCompressed => Some(RowField::ReadCompressedBytes),
            SummedColumn::OffFileSystemFd => Some(RowField::FdReadBytes),
            SummedColumn::GranuleMarks => Some(RowField::SelectedMarks),
            SummedColumn::ResultBytes => Some(RowField::ResultBytes),
        }
    }

    /// Expressed through [`Self::field`], so what a variant DECLARES it
    /// sums and what it DOES sum are one fact rather than two hand-kept
    /// ones.
    fn value(self, row: &LoweringStageRow) -> u64 {
        match self.field() {
            None => 1,
            Some(f) => f.get(row),
        }
    }
}

/// A running total that refuses rather than wrapping.
///
/// `Default` is hand-written: the derived one is `Total(None)`, which
/// would make a stage with zero rows refuse instead of totalling zero —
/// a silent inversion of the whole rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Total(Option<u64>);

impl Default for Total {
    fn default() -> Self {
        Total(Some(0))
    }
}

impl Total {
    fn add(&mut self, v: u64) {
        self.0 = self.0.and_then(|t| t.checked_add(v));
    }
    fn get(self, column: &str) -> Result<u64, String> {
        self.0.ok_or_else(|| {
            format!(
                "docs/query-lowering.md §9.2: the total of column \"{column}\" over the \
                 artefact's rows exceeds u64; this check refuses rather than summing wrapped"
            )
        })
    }
}

/// Totals one column over a set of rows. **The only producer of a total
/// in this file**; nothing else writes `+=` on a `u64` total.
fn total_of(col: SummedColumn, rows: &[&LoweringStageRow]) -> Result<u64, String> {
    let mut t = Total::default();
    for r in rows {
        t.add(col.value(r));
    }
    t.get(col.headers().0)
}

/// Accepts `m / 10^p` as a rendering of the exact quotient `n / d` iff it
/// is within half of its last printed place:
///
/// ```text
/// |n/d - m/10^p| <= 1/(2*10^p)   <=>   |2*n*10^p - 2*m*d| <= d
/// ```
///
/// Evaluated exactly in `u128`. **`None` is a refusal**, not a verdict:
/// the exact comparison does not fit, so the check fails rather than
/// approximating. No floating point anywhere.
fn accepts(n: u64, d: u64, m: u64, p: u32) -> Option<bool> {
    if d == 0 {
        return None;
    }
    let pow = 10u128.checked_pow(p)?;
    let lhs = 2u128.checked_mul(n as u128)?.checked_mul(pow)?;
    let rhs = 2u128.checked_mul(m as u128)?.checked_mul(d as u128)?;
    Some(lhs.abs_diff(rhs) <= d as u128)
}

/// The three groups every row-schema field falls into.
#[derive(Debug)]
struct Partition {
    summed: Vec<(&'static str, Vec<&'static str>)>,
    declared_not_summed: Vec<(&'static str, String)>,
    uncovered: Vec<&'static str>,
}

impl Partition {
    /// The committed rendering. A field moving between groups changes a
    /// line here, so taking the exclusion escape hatch cannot be silent.
    fn rendered(&self) -> String {
        let mut out = String::new();
        for (field, headers) in &self.summed {
            out.push_str(&format!(
                "{:<19} {} <- {}\n",
                "summed",
                field,
                headers.join(", ")
            ));
        }
        for (field, why) in &self.declared_not_summed {
            out.push_str(&format!(
                "{:<19} {} ({})\n",
                "declared_not_summed", field, why
            ));
        }
        for field in &self.uncovered {
            out.push_str(&format!("{:<19} {}\n", "UNCOVERED", field));
        }
        out
    }
}

/// Puts every `RowField::ALL` entry in exactly one of three groups.
/// A field that is BOTH summed and declared is `uncovered`, because two
/// answers to "is this totalled?" is not an answer.
fn partition(accumulators: &[SummedColumn], excluded: &[(RowField, NotSummed)]) -> Partition {
    let mut p = Partition {
        summed: Vec::new(),
        declared_not_summed: Vec::new(),
        uncovered: Vec::new(),
    };
    for field in RowField::ALL {
        let headers: Vec<&'static str> = accumulators
            .iter()
            .filter(|a| a.field() == Some(field))
            .map(|a| a.headers().0)
            .collect();
        let declared = excluded.iter().find(|(f, _)| *f == field).map(|(_, w)| *w);
        match (headers.is_empty(), declared) {
            (false, None) => p.summed.push((field.name(), headers)),
            (true, Some(why)) => p.declared_not_summed.push((field.name(), why.rendered())),
            _ => p.uncovered.push(field.name()),
        }
    }
    p
}

/// The committed partition, byte for byte.
const EXPECTED_PARTITION: &str = "\
summed              read_rows <- rows read
summed              read_bytes <- decoded †
summed              read_compressed_bytes <- off file system †
summed              fd_read_bytes <- off file system †
summed              selected_marks <- granules (avg)
summed              result_bytes <- result bytes
declared_not_summed memory_usage (maximum over rows)
declared_not_summed max_block_size_submitted (instrument constant = 4096)
";

/// The document cell each `(form, stage)` pair is checked against.
/// No `_` arm: adding a form or a stage fails to build here.
fn queries_cell(form: Form, stage: Stage) -> (&'static str, &'static str) {
    match (form, stage) {
        (Form::Current, Stage::Generator) => ("§9.2", "phase-1 generator"),
        (Form::Current, Stage::Hydration) => ("§9.2", "phase-2 hydration"),
        (Form::Current, Stage::Membership) => ("§9.2", "phase-2 membership"),
        (Form::Current, Stage::RootRead) => ("§9.2", "winners' root read"),
        (Form::Lowered, Stage::Generator) => ("§9.2b", "lowered generator"),
        (Form::Lowered, Stage::Hydration) => ("§9.2b", "lowered hydration"),
        (Form::Lowered, Stage::Membership) => ("§9.2b", "lowered membership"),
        (Form::Lowered, Stage::RootRead) => ("§9.2b", "winners' root read"),
    }
}

fn lowering_evidence() -> LoweringEvidence {
    let text = repo_file(LOWERING_EVIDENCE);
    serde_json::from_str(&text).unwrap_or_else(|e| {
        panic!("{LOWERING_EVIDENCE} must parse as the retained measurement: {e}")
    })
}

/// The slice of `md` from `heading` up to `end`.
///
/// **Sections are sliced before any header row is matched.** The header
/// shape `| | round trips |` occurs twice in this document, in §9.2's
/// comparison table and again in §9.6 with different columns, so a check
/// that finds a table by matching the header against the whole file gets
/// the right answer only because §9.2 comes first.
fn section<'a>(md: &'a str, heading: &str, end: &str) -> &'a str {
    let start = md
        .find(heading)
        .unwrap_or_else(|| panic!("{QUERY_LOWERING} must carry the heading {heading:?}"));
    assert_eq!(
        md.matches(heading).count(),
        1,
        "{QUERY_LOWERING} carries {heading:?} more than once; the slice would be ambiguous"
    );
    let rest = &md[start..];
    let len = rest
        .find(end)
        .unwrap_or_else(|| panic!("{QUERY_LOWERING}: {heading:?} is not followed by {end:?}"));
    &rest[..len]
}

/// Every markdown table in `slice`, as rows of trimmed cells. The
/// `|---|` separator row is dropped.
fn tables(slice: &str) -> Vec<Vec<Vec<String>>> {
    let mut out: Vec<Vec<Vec<String>>> = Vec::new();
    let mut current: Vec<Vec<String>> = Vec::new();
    for line in slice.lines() {
        let line = line.trim();
        if line.starts_with('|') {
            let cells: Vec<String> = line
                .trim_matches('|')
                .split('|')
                .map(|c| c.trim().to_string())
                .collect();
            if cells
                .iter()
                .all(|c| c.chars().all(|ch| ch == '-' || ch == ':') && !c.is_empty())
            {
                continue;
            }
            current.push(cells);
        } else if !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// Strips the markdown a figure can be dressed in — bold, code ticks,
/// thousands separators, the `×` a ratio carries — and nothing else.
fn bare(cell: &str) -> String {
    cell.replace("**", "")
        .replace(['`', ',', '×'], "")
        .trim()
        .to_string()
}

/// A figure as `u64`. Never through `f64`: a figure at or above 2^53
/// would compare equal to its neighbour there, which is a false green.
fn figure(cell: &str, what: &str) -> u64 {
    let b = bare(cell);
    b.parse::<u64>().unwrap_or_else(|_| {
        panic!("docs/query-lowering.md §9.2: {cell:?} is not a u64 figure ({what})")
    })
}

/// A printed decimal as `(m, p)`: `149.9` is `(1499, 1)`, `1,106` is
/// `(1106, 0)`, `282×` is `(282, 0)`.
fn rendering(cell: &str, what: &str) -> (u64, u32) {
    let b = bare(cell);
    match b.split_once('.') {
        None => (
            b.parse::<u64>().unwrap_or_else(|_| {
                panic!("docs/query-lowering.md §9.2: {cell:?} is not a printed number ({what})")
            }),
            0,
        ),
        Some((whole, frac)) => {
            let joined = format!("{whole}{frac}");
            (
                joined.parse::<u64>().unwrap_or_else(|_| {
                    panic!("docs/query-lowering.md §9.2: {cell:?} is not a printed number ({what})")
                }),
                u32::try_from(frac.len()).expect("a printed place count fits u32"),
            )
        }
    }
}

/// The granules cell: `avg` optionally followed by ` (min A, max B)`.
fn granules_cell(cell: &str) -> ((u64, u32), Option<(u64, u64)>) {
    match cell.split_once(" (min ") {
        None => (rendering(cell, "granules average"), None),
        Some((avg, rest)) => {
            let inner = rest.trim_end_matches(')').trim_end_matches("**");
            let (lo, hi) = inner
                .split_once(", max ")
                .unwrap_or_else(|| panic!("granules cell {cell:?} must read `avg (min A, max B)`"));
            (
                rendering(avg, "granules average"),
                Some((figure(lo, "granules min"), figure(hi, "granules max"))),
            )
        }
    }
}

/// **The artefact holds one row per read, and the counts come from the
/// document rather than from the file's own length.**
///
/// For each of the eight `(form, stage)` pairs the corresponding §9.2 or
/// §9.2b `queries` cell states a count; the artefact must hold exactly
/// that many rows with `seq` contiguous from 0. The total is the sum of
/// the eight cells, and every `query_id` is distinct — so a row that
/// summarises another row cannot hide inside the file.
///
/// *RED when:* a row is deleted (the pair's count and the missing `seq`
/// are both named), or a row's `form` is changed (both halves of the move
/// are named, which is the edit a single `stage` string could not even
/// express).
#[test]
fn the_lowering_evidence_has_a_row_per_read() {
    let evidence = lowering_evidence();
    let md = repo_file(QUERY_LOWERING);
    let s92 = section(&md, S92_HEADING, S92B_HEADING);
    let s92b = section(&md, S92B_HEADING, S93_HEADING);

    let mut failures: Vec<String> = Vec::new();
    let mut expected_total = 0u64;
    for form in [Form::Current, Form::Lowered] {
        for stage in [
            Stage::Generator,
            Stage::Hydration,
            Stage::Membership,
            Stage::RootRead,
        ] {
            let (section_name, label) = queries_cell(form, stage);
            let slice = if section_name == "§9.2" { s92 } else { s92b };
            let stated = per_stage_row(slice, section_name, label);
            let stated = figure(&stated[1], "queries");
            expected_total += stated;

            let mut held: Vec<&LoweringStageRow> = evidence
                .rows
                .iter()
                .filter(|r| r.form == form && r.stage == stage)
                .collect();
            held.sort_by_key(|r| r.seq);
            if held.len() as u64 != stated {
                failures.push(format!(
                    "{form:?}/{stage:?}: docs/query-lowering.md {section_name} states {stated} \
                     reads for {label:?}, the artefact holds {}",
                    held.len()
                ));
            }
            for (i, row) in held.iter().enumerate() {
                if row.seq as usize != i {
                    failures.push(format!(
                        "{form:?}/{stage:?}: seq {i} is missing; the artefact must hold every \
                         read, not a summary"
                    ));
                    break;
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    assert_eq!(
        evidence.rows.len() as u64,
        expected_total,
        "{LOWERING_EVIDENCE} holds {} rows; the eight (form, stage) cells of §9.2 and §9.2b state \
         {expected_total} between them",
        evidence.rows.len()
    );
    let ids: BTreeSet<&str> = evidence.rows.iter().map(|r| r.query_id.as_str()).collect();
    assert_eq!(
        ids.len(),
        evidence.rows.len(),
        "{LOWERING_EVIDENCE} carries a repeated query_id: {} rows, {} distinct ids — one row per \
         statement is the contract",
        evidence.rows.len(),
        ids.len()
    );
}

/// The data row of `slice`'s per-stage table whose first cell is `label`.
fn per_stage_row(slice: &str, section_name: &str, label: &str) -> Vec<String> {
    let table = tables(slice)
        .into_iter()
        .find(|t| {
            t.first()
                .is_some_and(|h| h.first().is_some_and(|c| c == "stage"))
        })
        .unwrap_or_else(|| panic!("{section_name} must carry a per-stage table"));
    table
        .into_iter()
        .find(|r| bare(&r[0]) == label)
        .unwrap_or_else(|| panic!("{section_name}'s per-stage table has no {label:?} row"))
}

/// **Every figure §9.2 and §9.2b state is the one the artefact holds.**
///
/// Nine requirements, in the order the criterion lists them:
///
/// (a) every total is produced by [`total_of`], which refuses rather than
///     wrapping and names the column when it does;
/// (b) the header set closes both ways against the three tables;
/// (c) the granules column is checked through its numerator with
///     [`accepts`], never in floating point;
/// (d) `off file system †` is checked against whichever instrument §9.2
///     NAMES, with both totalled through `total_of` regardless;
/// (e) every numeric key the artefact holds is classified;
/// (f) every row-schema field is summed or declared, never neither and
///     never both, and the three-group partition is committed;
/// (g) each accessor returns its own field, and there is exactly one
///     counter;
/// (h) the `InstrumentConstant` declaration is checked, not asserted;
/// (i) both tables are located inside their own section first.
///
/// (e), (f) and (g) close the coverage over three different domains — the
/// artefact's keys, the schema's fields, and the document's columns —
/// because no one of them subsumes the others. Two accumulators declare
/// the same header cell, so deleting either leaves the header closure
/// green while a field silently stops being totalled.
#[test]
fn every_figure_section_9_2_states_is_the_one_the_artefact_holds() {
    // **Closure A runs BEFORE the typed parse, and the order is the
    // check.** The typed mirror carries `deny_unknown_fields`, so a
    // producer field the mirror does not know fails the parse — and if
    // the parse ran first, a new numeric key would fail with serde's
    // message about an unknown field rather than with this gate's
    // message about a key no `RowField` covers. The break would then be
    // passing for a reason that has nothing to do with the closure it
    // was written to exercise. Reading the raw JSON first makes the
    // closure the thing that catches it; `deny_unknown_fields` is the
    // second, independent guard, and `the_lowering_evidence_has_a_row_
    // per_read` is where it shows.
    let md = repo_file(QUERY_LOWERING);
    // (i) — slice first, match header rows inside the slice.
    let s92 = section(&md, S92_HEADING, S92B_HEADING);
    let s92b = section(&md, S92B_HEADING, S93_HEADING);
    let s92_to_93 = section(&md, S92_HEADING, S93_HEADING);

    // ---- (e) closure A: the artefact's numeric keys -----------------
    let raw: serde_json::Value =
        serde_json::from_str(&repo_file(LOWERING_EVIDENCE)).expect("the artefact is valid JSON");
    let mut numeric_keys: BTreeSet<String> = BTreeSet::new();
    for row in raw["rows"].as_array().expect("the artefact carries rows") {
        for (k, v) in row.as_object().expect("each row is an object") {
            if v.is_number() {
                numeric_keys.insert(k.clone());
            }
        }
    }
    let covered: BTreeSet<String> = RowField::ALL
        .iter()
        .map(|f| f.name().to_string())
        .chain(NON_QUANTITY_ROW_KEYS.iter().map(|(k, _)| k.to_string()))
        .collect();
    let unknown: Vec<&String> = numeric_keys.difference(&covered).collect();
    let absent: Vec<&String> = covered.difference(&numeric_keys).collect();
    assert!(
        unknown.is_empty(),
        "docs/query-lowering.md §9.2 gate: the artefact's rows carry numeric key {:?} that no \
         RowField covers",
        unknown.first().map(|s| s.as_str()).unwrap_or("")
    );
    assert!(
        absent.is_empty(),
        "docs/query-lowering.md §9.2 gate: RowField {absent:?} names a key the artefact's rows do \
         not carry"
    );

    // Only now the typed read.
    let evidence = lowering_evidence();

    // ---- (f) closure B: the row schema's fields ---------------------
    let part = partition(&SummedColumn::ALL, &DECLARED_NOT_SUMMED);
    assert!(
        part.uncovered.is_empty(),
        "docs/query-lowering.md §9.2 gate: LoweringStageRow field(s) {:?} are summed by no \
         accumulator and are not in DECLARED_NOT_SUMMED; a field that is deliberately not \
         totalled must say so and say why",
        part.uncovered
    );
    assert_eq!(
        part.rendered(),
        EXPECTED_PARTITION,
        "docs/query-lowering.md §9.2 gate: the summed/not-summed partition of the row schema moved"
    );

    // ---- (g) the accessors, and exactly one counter -----------------
    let counters: Vec<SummedColumn> = SummedColumn::ALL
        .into_iter()
        .filter(|c| c.field().is_none())
        .collect();
    assert_eq!(
        counters.len(),
        1,
        "exactly one SummedColumn counts rows rather than summing a field; found {counters:?}"
    );
    for target in RowField::ALL {
        let sentinel = 700 + RowField::ALL.iter().position(|f| *f == target).unwrap() as u64;
        let mut obj = serde_json::Map::new();
        obj.insert("form".into(), serde_json::json!("current"));
        obj.insert("stage".into(), serde_json::json!("generator"));
        obj.insert("query_id".into(), serde_json::json!("sentinel"));
        obj.insert("seq".into(), serde_json::json!(0));
        obj.insert(
            "settings_max_block_size_logged".into(),
            serde_json::json!(""),
        );
        for f in RowField::ALL {
            obj.insert(
                f.name().into(),
                serde_json::json!(if f == target { sentinel } else { 0 }),
            );
        }
        let row: LoweringStageRow = serde_json::from_value(serde_json::Value::Object(obj))
            .expect("the sentinel row deserialises");
        assert_eq!(
            target.get(&row),
            sentinel,
            "RowField::{target:?}'s accessor does not return its own field"
        );
    }

    // ---- (h) the instrument constant --------------------------------
    let submitted: BTreeSet<u64> = evidence
        .rows
        .iter()
        .map(|r| r.max_block_size_submitted)
        .collect();
    assert_eq!(
        submitted.len(),
        1,
        "docs/query-lowering.md §9.2 gate: max_block_size_submitted is declared an instrument \
         constant but the artefact holds {} distinct values {submitted:?}",
        submitted.len()
    );
    let submitted = *submitted.iter().next().expect("one value");
    assert_eq!(
        submitted,
        pulsus_read::TRACE_SEARCH_MAX_BLOCK_ROWS,
        "the artefact was measured at max_block_size {submitted}; the search path submits {}",
        pulsus_read::TRACE_SEARCH_MAX_BLOCK_ROWS
    );
    let logged: Vec<&LoweringStageRow> = evidence
        .rows
        .iter()
        .filter(|r| !r.settings_max_block_size_logged.is_empty())
        .collect();
    assert!(
        !logged.is_empty(),
        "no row carries settings_max_block_size_logged: the server logged the setting on none of \
         the {} statements, so the artefact has no server-side witness of what was applied",
        evidence.rows.len()
    );
    for row in &logged {
        let parsed = row
            .settings_max_block_size_logged
            .parse::<u64>()
            .unwrap_or_else(|e| {
                panic!(
                    "settings_max_block_size_logged {:?} does not parse as u64: {e}",
                    row.settings_max_block_size_logged
                )
            });
        assert_eq!(
            parsed, submitted,
            "the server logged max_block_size {parsed} for {}, the harness submitted {submitted}",
            row.query_id
        );
    }

    // ---- (b) closure C: the document's columns ----------------------
    let per_stage_headers = |slice: &str, name: &str| -> Vec<String> {
        tables(slice)
            .into_iter()
            .find(|t| {
                t.first()
                    .is_some_and(|h| h.first().is_some_and(|c| c == "stage"))
            })
            .unwrap_or_else(|| panic!("{name} must carry a per-stage table"))[0]
            .clone()
    };
    let s92_headers = per_stage_headers(s92, "§9.2");
    let s92b_headers = per_stage_headers(s92b, "§9.2b");
    let comparison = tables(s92_to_93)
        .into_iter()
        .find(|t| {
            t.first()
                .is_some_and(|h| h.first().is_some_and(|c| c.is_empty()))
        })
        .expect("§9.2 must carry a comparison table whose header row opens with an empty cell");
    let comparison_headers = comparison[0].clone();

    let declared_per_stage: BTreeSet<&str> =
        SummedColumn::ALL.iter().map(|c| c.headers().0).collect();
    let declared_comparison: BTreeSet<&str> = SummedColumn::ALL
        .iter()
        .filter_map(|c| c.headers().1)
        .collect();
    // **Every column of BOTH per-stage tables, before the two are
    // compared with each other.** An earlier revision asserted that §9.2
    // and §9.2b publish the same columns first, so renaming ONE table's
    // header reported the two tables disagreeing rather than the column
    // no accumulator covers — a different message from the one the plan
    // approved for that edit, and a less useful one: the reader is told
    // two tables differ when the fact is that a named column is
    // uncovered. The cross-table equality is still asserted, after.
    // Each table is checked SEPARATELY and the message names the one
    // the uncovered column is in. An earlier revision unioned the two
    // header sets, so renaming §9.2b's header reported the column as
    // being in §9.2 — a diagnostic that points at the wrong place costs
    // more than none, because somebody goes and looks there.
    for (name, headers) in [("§9.2", &s92_headers), ("§9.2b", &s92b_headers)] {
        let cells: BTreeSet<&str> = headers.iter().skip(1).map(|s| s.as_str()).collect();
        let uncovered: Vec<&&str> = cells.difference(&declared_per_stage).collect();
        assert!(
            uncovered.is_empty(),
            "docs/query-lowering.md {name} has a column {:?} that no accumulator covers",
            uncovered.first().map(|s| **s).unwrap_or("")
        );
    }
    let per_stage_cells: BTreeSet<&str> = s92_headers
        .iter()
        .skip(1)
        .chain(s92b_headers.iter().skip(1))
        .map(|s| s.as_str())
        .collect();
    let comparison_cells: BTreeSet<&str> = comparison_headers
        .iter()
        .skip(1)
        .map(|s| s.as_str())
        .collect();
    // Kept, but it can no longer be the first to fire: the per-table
    // loop above reports the same column and names its table.
    let per_stage_uncovered: Vec<&&str> = per_stage_cells.difference(&declared_per_stage).collect();
    let comparison_uncovered: Vec<&&str> =
        comparison_cells.difference(&declared_comparison).collect();
    assert!(
        per_stage_uncovered.is_empty(),
        "docs/query-lowering.md §9.2 has a column {:?} that no accumulator covers",
        per_stage_uncovered.first().map(|s| **s).unwrap_or("")
    );
    assert!(
        comparison_uncovered.is_empty(),
        "docs/query-lowering.md §9.2's comparison table has a column {:?} that no accumulator \
         covers",
        comparison_uncovered.first().map(|s| **s).unwrap_or("")
    );
    // **These two assertions are NOT alike, and an earlier revision of
    // this comment said they were.** It claimed no break reddens either,
    // and a code review showed that was false for the second one, which
    // is worse than saying nothing: a reader would have discounted
    // coverage that exists. Both were then re-measured, one edit at a
    // time, and this is what each produced.
    //
    // `comparison_absent` is REACHABLE. Give a variant whose comparison
    // header is `None` one the comparison table does not carry —
    // `SummedColumn::Decoded => ("decoded †", Some("ghost comparison"))`
    // — and every table column stays covered while the declaration is
    // unmatched, so this assertion is the one that fires:
    //   `an accumulator declares the comparison header
    //    ["ghost comparison"], which §9.2's comparison table does not
    //    carry`
    // The partition does not move, because the partition prints
    // `headers().0` and this edit changes `headers().1`.
    //
    // `per_stage_absent` is not reached by any edit I could construct,
    // and three were tried, one at a time:
    //   * change a field-bearing variant's per-stage header
    //     (`Decoded`, and `OffFileSystemFd`, which shares its cell with
    //     another variant) — the partition assertion above fires first,
    //     because the partition prints that header beside the field;
    //   * change the counter's per-stage header (`Queries`) — the cell
    //     it vacated becomes uncovered and `per_stage_uncovered` fires
    //     first;
    //   * add a second field-less variant — the "exactly one counter"
    //     assertion fires first.
    // It stays because it costs nothing and would matter if the
    // partition assertion were ever weakened. **`No break reddens
    // per_stage_absent`**, and a later reader must not count it as live
    // coverage; `comparison_absent` is live and the break above is how
    // to reproduce it.
    assert_eq!(
        s92_headers, s92b_headers,
        "§9.2 and §9.2b must publish the same per-stage columns"
    );
    let per_stage_absent: Vec<&&str> = declared_per_stage.difference(&per_stage_cells).collect();
    let comparison_absent: Vec<&&str> = declared_comparison.difference(&comparison_cells).collect();
    assert!(
        per_stage_absent.is_empty(),
        "an accumulator declares the per-stage header {per_stage_absent:?}, which §9.2 does not \
         carry"
    );
    assert!(
        comparison_absent.is_empty(),
        "an accumulator declares the comparison header {comparison_absent:?}, which §9.2's \
         comparison table does not carry"
    );

    // ---- (d) which instrument the document names --------------------
    let named: Vec<SummedColumn> = [
        (SummedColumn::OffFileSystemCompressed, "ReadCompressedBytes"),
        (
            SummedColumn::OffFileSystemFd,
            "ReadBufferFromFileDescriptorReadBytes",
        ),
    ]
    .into_iter()
    .filter(|(_, counter)| s92.contains(counter))
    .map(|(c, _)| c)
    .collect();
    assert_eq!(
        named.len(),
        1,
        "§9.2 must name exactly one instrument for its `off file system †` column; it names \
         {named:?}"
    );
    let off_file_system = named[0];

    // ---- (a) + (c) the cells themselves -----------------------------
    let mut checked = 0usize;
    for (form, slice, section_name) in
        [(Form::Current, s92, "§9.2"), (Form::Lowered, s92b, "§9.2b")]
    {
        let mut form_rows: Vec<&LoweringStageRow> = Vec::new();
        for stage in [
            Stage::Generator,
            Stage::Hydration,
            Stage::Membership,
            Stage::RootRead,
        ] {
            let (_, label) = queries_cell(form, stage);
            let rows: Vec<&LoweringStageRow> = evidence
                .rows
                .iter()
                .filter(|r| r.form == form && r.stage == stage)
                .collect();
            form_rows.extend(rows.iter().copied());
            let cells = per_stage_row(slice, section_name, label);
            checked += check_per_stage_row(
                &cells,
                &s92_headers,
                &rows,
                off_file_system,
                section_name,
                label,
            );
        }
        let cells = per_stage_row(slice, section_name, "total");
        checked += check_per_stage_row(
            &cells,
            &s92_headers,
            &form_rows,
            off_file_system,
            section_name,
            "total",
        );
    }

    // The comparison table's two operand rows.
    for (label, form) in [("today", Form::Current), ("lowered", Form::Lowered)] {
        let cells = comparison
            .iter()
            .find(|r| bare(&r[0]) == label)
            .unwrap_or_else(|| panic!("§9.2's comparison table has no {label:?} row"));
        let rows: Vec<&LoweringStageRow> =
            evidence.rows.iter().filter(|r| r.form == form).collect();
        for (i, header) in comparison_headers.iter().enumerate().skip(1) {
            let col = SummedColumn::ALL
                .into_iter()
                .find(|c| c.headers().1 == Some(header.as_str()))
                .unwrap_or_else(|| panic!("no accumulator for comparison column {header:?}"));
            let total = total_of(col, &rows).unwrap_or_else(|e| panic!("{e}"));
            let stated = figure(&cells[i], header);
            assert_eq!(
                stated, total,
                "docs/query-lowering.md §9.2's comparison table states {stated} for {label} \
                 {header}; the artefact totals to {total}"
            );
            checked += 1;
        }
    }

    assert!(
        checked >= 60,
        "only {checked} cells were compared; §9.2 and §9.2b carry ten table rows of six figures \
         plus eight comparison cells, so a much smaller number means the tables were not found"
    );

    // Criterion 4: the total is derived from rows that exist, so the
    // phrase that flagged it as underived is gone.
    assert!(
        !s92.contains("not independent evidence"),
        "§9.2 still calls a total \"not independent evidence\"; the artefact holds every read, so \
         the total and the per-read unit are two readings of the same rows"
    );
    assert!(
        s92.contains(LOWERING_EVIDENCE),
        "§9.2 must name the artefact its figures are totals over ({LOWERING_EVIDENCE})"
    );
}

/// One per-stage table row against the rows it summarises. Returns the
/// number of cells compared, so the caller can assert it found a table
/// rather than an empty one.
fn check_per_stage_row(
    cells: &[String],
    headers: &[String],
    rows: &[&LoweringStageRow],
    off_file_system: SummedColumn,
    section_name: &str,
    label: &str,
) -> usize {
    let mut checked = 0usize;
    for (i, header) in headers.iter().enumerate().skip(1) {
        let cell = &cells[i];
        match header.as_str() {
            "granules (avg)" => {
                let numerator =
                    total_of(SummedColumn::GranuleMarks, rows).unwrap_or_else(|e| panic!("{e}"));
                let queries =
                    total_of(SummedColumn::Queries, rows).unwrap_or_else(|e| panic!("{e}"));
                let ((m, p), range) = granules_cell(cell);
                if label == "total" {
                    // The total row states the SUM, not a mean.
                    assert_eq!(
                        m, numerator,
                        "{section_name} states {m} total granules; the artefact totals to \
                         {numerator}"
                    );
                    assert_eq!(p, 0, "{section_name}'s granule total is a whole number");
                } else {
                    assert_eq!(
                        accepts(numerator, queries, m, p),
                        Some(true),
                        "docs/query-lowering.md {section_name}: {numerator} / {queries} is not \
                         within half of the last printed place of {cell:?} for {label} granules \
                         (avg)"
                    );
                    if let Some((lo, hi)) = range {
                        let observed_lo = rows.iter().map(|r| r.selected_marks).min().unwrap_or(0);
                        let observed_hi = rows.iter().map(|r| r.selected_marks).max().unwrap_or(0);
                        assert_eq!(
                            (lo, hi),
                            (observed_lo, observed_hi),
                            "{section_name} states granules (min {lo}, max {hi}) for {label}; the \
                             artefact holds (min {observed_lo}, max {observed_hi})"
                        );
                    }
                }
            }
            "off file system †" => {
                // BOTH instruments are totalled through `total_of`,
                // whichever one the document names.
                let compressed = total_of(SummedColumn::OffFileSystemCompressed, rows)
                    .unwrap_or_else(|e| panic!("{e}"));
                let fd =
                    total_of(SummedColumn::OffFileSystemFd, rows).unwrap_or_else(|e| panic!("{e}"));
                let total = match off_file_system {
                    SummedColumn::OffFileSystemCompressed => compressed,
                    _ => fd,
                };
                let stated = figure(cell, header);
                assert_eq!(
                    stated, total,
                    "docs/query-lowering.md {section_name} states {stated} for {label} {header}; \
                     the artefact totals to {total}"
                );
            }
            other => {
                let col = SummedColumn::ALL
                    .into_iter()
                    .find(|c| c.headers().0 == other)
                    .unwrap_or_else(|| panic!("no accumulator for per-stage column {other:?}"));
                let total = total_of(col, rows).unwrap_or_else(|e| panic!("{e}"));
                let stated = figure(cell, header);
                assert_eq!(
                    stated, total,
                    "docs/query-lowering.md {section_name} states {stated} for {label} {header}; \
                     the artefact totals to {total}"
                );
            }
        }
        checked += 1;
    }
    checked
}

/// **Every ratio §9.2 prints is the quotient of two figures printed
/// beside it**, and the printed value is within half of its last printed
/// place. Evaluated exactly in `u128`; `accepts` returning `None` is a
/// refusal, which fails the check rather than approximating.
///
/// Two families:
///
/// * the comparison table's `ratio` row — four ratios whose operands are
///   the two rows immediately above, in the same table;
/// * the byte renderings §9.2 and §9.2b print for reading, each checked
///   against the raw byte figure in the table above it with
///   `d = 1024^k`. A one-place GiB rendering admits a wide band, so the
///   rendering is checked as a rounding of the raw figure and never
///   instead of it — the raw `u64` is what the figure check gates.
#[test]
fn every_ratio_in_section_9_2_is_the_quotient_of_two_printed_figures() {
    let md = repo_file(QUERY_LOWERING);
    let s92 = section(&md, S92_HEADING, S92B_HEADING);
    let s92b = section(&md, S92B_HEADING, S93_HEADING);
    let s92_to_93 = section(&md, S92_HEADING, S93_HEADING);

    let comparison = tables(s92_to_93)
        .into_iter()
        .find(|t| {
            t.first()
                .is_some_and(|h| h.first().is_some_and(|c| c.is_empty()))
        })
        .expect("§9.2 must carry a comparison table");
    let headers = comparison[0].clone();
    let row = |label: &str| -> Vec<String> {
        comparison
            .iter()
            .find(|r| bare(&r[0]) == label)
            .unwrap_or_else(|| panic!("§9.2's comparison table has no {label:?} row"))
            .clone()
    };
    let today = row("today");
    let lowered = row("lowered");
    let ratio = row("ratio");

    let mut checked = 0usize;
    for (i, header) in headers.iter().enumerate().skip(1) {
        let n = figure(&today[i], header);
        let d = figure(&lowered[i], header);
        let (m, p) = rendering(&ratio[i], header);
        assert_eq!(
            accepts(n, d, m, p),
            Some(true),
            "docs/query-lowering.md §9.2: {n} / {d} is not within half of the last printed place \
             of {} for the {header} ratio",
            ratio[i]
        );
        checked += 1;
    }
    assert_eq!(
        checked,
        headers.len() - 1,
        "every column of the comparison table carries a ratio"
    );

    // The byte renderings, each against the raw total in its own table.
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * 1024 * 1024;
    let rendered_bytes = |slice: &str, name: &str, column: &str, unit: u64, unit_name: &str| {
        let table = tables(slice)
            .into_iter()
            .find(|t| {
                t.first()
                    .is_some_and(|h| h.first().is_some_and(|c| c == "stage"))
            })
            .unwrap_or_else(|| panic!("{name} must carry a per-stage table"));
        let col = table[0]
            .iter()
            .position(|h| h == column)
            .unwrap_or_else(|| panic!("{name} has no {column:?} column"));
        let total_row = table
            .iter()
            .find(|r| bare(&r[0]) == "total")
            .unwrap_or_else(|| panic!("{name} has no total row"));
        let raw = figure(&total_row[col], column);
        // The sentence that renders it: "**102.12 GiB** decoded".
        let needle = format!(" {unit_name}**");
        let printed: Vec<(u64, u32)> = slice
            .match_indices(&needle)
            .map(|(i, _)| {
                let head = &slice[..i];
                let start = head.rfind("**").expect("the rendering is bold");
                rendering(&head[start + 2..], column)
            })
            .collect();
        assert!(
            !printed.is_empty(),
            "{name} states no {unit_name} rendering, so this rule is checking nothing"
        );
        let ok = printed
            .iter()
            .any(|(m, p)| accepts(raw, unit, *m, *p) == Some(true));
        assert!(
            ok,
            "{name}: none of the printed {unit_name} renderings {printed:?} is within half of its \
             last printed place of {raw} / {unit} for {column:?}"
        );
    };
    rendered_bytes(s92, "§9.2", "decoded †", GIB, "GiB");
    rendered_bytes(s92, "§9.2", "off file system †", GIB, "GiB");
    rendered_bytes(s92b, "§9.2b", "decoded †", MIB, "MiB");
    rendered_bytes(s92b, "§9.2b", "off file system †", MIB, "MiB");

    // Every ratio §9.2 or §9.2b prints in PROSE is written as
    // `**<m>×** = <n> / <d>` — the value, then the division it comes
    // from. Scraping numbers out of a paragraph and taking the largest
    // over the smallest is not a check: it silently picks the wrong
    // operands. This form names them.
    let mut prose_ratios = 0usize;
    for (slice, name) in [(s92, "§9.2"), (s92b, "§9.2b")] {
        for (i, _) in slice.match_indices("×** = ") {
            let head = &slice[..i];
            let open = head
                .rfind("**")
                .unwrap_or_else(|| panic!("{name}: a prose ratio is not opened with `**`"));
            let (m, p) = rendering(&head[open + 2..], "prose ratio");
            let tail = &slice[i + "×** = ".len()..];
            let expr: &str = tail.split(['.', ';', '\n']).next().unwrap_or("");
            let (n_text, d_text) = expr
                .split_once(" / ")
                .unwrap_or_else(|| panic!("{name}: prose ratio {m}/10^{p} does not print `n / d`"));
            let n = figure(n_text, "prose ratio numerator");
            let d = figure(
                d_text.split_whitespace().next().unwrap_or(""),
                "prose ratio denominator",
            );
            assert_eq!(
                accepts(n, d, m, p),
                Some(true),
                "docs/query-lowering.md {name}: {n} / {d} is not within half of the last printed \
                 place of the ratio it is printed beside"
            );
            prose_ratios += 1;
        }
    }
    assert!(
        prose_ratios > 0,
        "neither §9.2 nor §9.2b prints a ratio in prose, so this rule is checking nothing"
    );
}

/// **The hops drawing's lowered figures are §9.2b's.**
///
/// A picture asserts a design more confidently than a sentence, and this
/// one carried a two-statement lowered model for three rounds while the
/// prose beside it said four. The drawing now names the two counters it
/// states about the lowered request in `id` attributes, and this check
/// reads them by id and compares them with the document.
///
/// **What it does not compare:** the drawing's TODAY band, its rows and
/// granules, and every word of prose on its face. Those are not this
/// gate's subject, and §11.3 records that limit.
#[test]
fn the_hops_diagram_and_the_document_agree_on_the_lowered_request() {
    let svg = repo_file(HOPS_SVG);
    let md = repo_file(QUERY_LOWERING);
    let s92_to_93 = section(&md, S92_HEADING, S93_HEADING);
    let comparison = tables(s92_to_93)
        .into_iter()
        .find(|t| {
            t.first()
                .is_some_and(|h| h.first().is_some_and(|c| c.is_empty()))
        })
        .expect("§9.2 must carry a comparison table");
    let headers = comparison[0].clone();
    let lowered = comparison
        .iter()
        .find(|r| bare(&r[0]) == "lowered")
        .expect("§9.2's comparison table has a lowered row");
    let cell = |header: &str| -> u64 {
        let i = headers
            .iter()
            .position(|h| h == header)
            .unwrap_or_else(|| panic!("§9.2's comparison table has no {header:?} column"));
        figure(&lowered[i], header)
    };

    let drawn = |id: &str| -> u64 {
        let node = svg
            .split_once(&format!("<text id=\"{id}\""))
            .unwrap_or_else(|| panic!("{HOPS_SVG} must carry a <text id=\"{id}\"> node"))
            .1
            .split_once('>')
            .expect("the node's tag is closed")
            .1
            .split_once("</text>")
            .expect("the node is closed")
            .0;
        figure(node.split_whitespace().next().unwrap_or(""), id)
    };

    let drawn_trips = drawn("lowered-round-trips");
    let stated_trips = cell("round trips");
    assert_eq!(
        drawn_trips, stated_trips,
        "the hops diagram counts {drawn_trips} lowered round trips; docs/query-lowering.md §9.2 \
         counts {stated_trips}"
    );
    let drawn_bytes = drawn("lowered-result-bytes");
    let stated_bytes = cell("result bytes");
    assert_eq!(
        drawn_bytes, stated_bytes,
        "the hops diagram states {drawn_bytes} lowered result bytes; docs/query-lowering.md §9.2 \
         states {stated_bytes}"
    );
}

// ---------------------------------------------------------------------
// Issue #492 part 8, landing 2 — the record's tables against the enums
// they claim to enumerate, and the boundary diagram against the tables.
//
// Every one of these checks has TWO INDEPENDENT PRODUCERS: a variant list
// parsed out of the compiler's own source, and a table written by hand in
// `docs/query-lowering.md`. Neither can produce the other, which is what
// makes a missing row visible. §3.1 called itself "the complete TraceQL
// link set" while carrying 5 of `TqlLink`'s 15 variants, and §7.1 called
// itself the complete LogQL link set with no `Source` row at all; both
// were found by writing these checks, not by reading the tables.
// ---------------------------------------------------------------------

const S31_HEADING: &str = "### 3.1 The complete TraceQL link set";
const S32_HEADING: &str = "### 3.2 Group 1 — cannot be lowered";
const S71_HEADING: &str = "### 7.1 The complete LogQL link set";
const S72_HEADING: &str = "### 7.2 Groups 1, 2 and 3";
const S27_HEADING: &str = "### 2.7 The compiler's output is a PLAN, not a statement";
const S28_HEADING: &str = "## 3. TraceQL against the model";
const BOUNDARY_SVG: &str = "docs/diagrams/query-lowering-boundary.svg";

/// The variant identifiers of `pub enum <name>` in `rel`, in declaration
/// order.
///
/// **This is the second producer.** The check does not hold a hand list
/// of variants that a contributor must remember to extend: it reads the
/// enum out of the file that declares it, so a variant added there and
/// not added to the document makes the check name the variant.
fn enum_variants(rel: &str, name: &str) -> Vec<String> {
    let src = repo_file(rel);
    let head = format!("pub enum {name} {{");
    let start = src
        .find(&head)
        .unwrap_or_else(|| panic!("{rel} must declare `{head}`"))
        + head.len();
    let mut depth = 1usize;
    let mut end = start;
    for (i, c) in src[start..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = start + i;
                    break;
                }
            }
            _ => {}
        }
    }
    assert!(end > start, "{rel}: `{head}` is not closed");
    let body = &src[start..end];
    // Variants are the identifiers at brace depth 1 that open a line.
    let mut depth = 0usize;
    let mut out = Vec::new();
    for line in body.lines() {
        let t = line.trim();
        if depth == 0 && !t.starts_with("///") && !t.starts_with("//") && !t.starts_with('#') {
            let ident: String = t
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if ident.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
                out.push(ident);
            }
        }
        depth = depth + t.matches('{').count() + t.matches('(').count() + t.matches('[').count();
        depth = depth.saturating_sub(
            t.matches('}').count() + t.matches(')').count() + t.matches(']').count(),
        );
    }
    assert!(!out.is_empty(), "{rel}: parsed no variants out of `{head}`");
    out
}

/// The leading backticked token of every table row in `slice`, reduced to
/// its leading identifier — `` `Aggregate { op, field, cmp, value }` ``
/// becomes `Aggregate`, `` `Limit(n)` `` becomes `Limit`.
///
/// Rows of the rejection tables (four columns) are excluded: they list a
/// refused payload, not a chain link.
fn link_row_labels(slice: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for row in link_table_rows(slice) {
        if let Some(label) = leading_backtick(&row[0]) {
            out.insert(label);
        }
    }
    out
}

/// The data rows of `slice`'s LINK tables — the tables whose header row
/// opens with `link` and carries a `continuation` column. A rejection
/// table also opens with `link`, and is excluded by that second test.
fn link_table_rows(slice: &str) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for table in tables(slice) {
        let header = &table[0];
        if header.first().map(String::as_str) != Some("link") {
            continue;
        }
        if header.last().map(String::as_str) != Some("continuation") {
            continue;
        }
        out.extend(table.into_iter().skip(1));
    }
    out
}

/// The leading identifier of a cell's first backticked token.
fn leading_backtick(cell: &str) -> Option<String> {
    let inner = cell.split('`').nth(1)?;
    let ident: String = inner
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == ':')
        .collect();
    (!ident.is_empty()).then_some(ident)
}

/// A variant whose row in the document is carried under another name,
/// with the reason. **Every alias is asserted to be USED**, so an entry
/// that stops applying is an error rather than a silent widening of what
/// counts as covered.
const LINK_ALIASES: [(&str, &str, &str); 2] = [
    (
        "TqlLink",
        "Pipe",
        "the `Pipe` arm carries `PipelineStage`, whose eight variants have eight rows of their own; \
         `every_traceql_pipeline_stage_variant_has_a_row_in_the_lowering_document` is the check \
         over those",
    ),
    (
        "LqlLink",
        "Pipe",
        "the `Pipe` arm carries `pulsus_logql::Stage`, whose ten variants have thirteen rows of \
         their own (`Parser` has four forms); \
         `every_logql_stage_variant_has_a_row_in_the_lowering_document` is the check over those",
    ),
];

fn assert_every_variant_has_a_row(
    enum_name: &str,
    variants: &[String],
    rows: &BTreeSet<String>,
    section: &str,
) {
    let aliased: BTreeSet<&str> = LINK_ALIASES
        .iter()
        .filter(|(e, _, _)| *e == enum_name)
        .map(|(_, v, _)| *v)
        .collect();
    for alias in &aliased {
        assert!(
            variants.iter().any(|v| v == alias),
            "{enum_name} has no variant {alias:?}, so the alias declared for it is dead"
        );
        assert!(
            !rows.contains(*alias),
            "{section} carries a row for {enum_name}::{alias}, so the alias is no longer needed \
             and must be removed rather than kept as a standing exemption"
        );
    }
    let missing: Vec<&String> = variants
        .iter()
        .filter(|v| !rows.contains(*v) && !aliased.contains(v.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "{enum_name}::{} has no row in docs/query-lowering.md {section}",
        missing
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
}

/// Every `pulsus_logql::Stage` variant has a row in §7.1.
#[test]
fn every_logql_stage_variant_has_a_row_in_the_lowering_document() {
    let md = repo_file(QUERY_LOWERING);
    let s71 = section(&md, S71_HEADING, S72_HEADING);
    let variants = enum_variants("crates/pulsus-logql/src/ast.rs", "Stage");
    assert_eq!(variants.len(), 10, "pulsus_logql::Stage: {variants:?}");
    assert_every_variant_has_a_row("Stage", &variants, &link_row_labels(s71), "§7.1");
}

/// Every `pulsus_traceql::PipelineStage` variant has a row in §3.1.
#[test]
fn every_traceql_pipeline_stage_variant_has_a_row_in_the_lowering_document() {
    let md = repo_file(QUERY_LOWERING);
    let s31 = section(&md, S31_HEADING, S32_HEADING);
    let variants = enum_variants("crates/pulsus-traceql/src/ast.rs", "PipelineStage");
    assert_eq!(
        variants.len(),
        8,
        "pulsus_traceql::PipelineStage: {variants:?}"
    );
    assert_every_variant_has_a_row("PipelineStage", &variants, &link_row_labels(s31), "§3.1");
}

/// Every `LqlLink` variant has a row in §7.1.
///
/// **`Source` had none until part 8.** §7.1 named the seed in its chain
/// diagram and in one prose sentence and gave it no row, so the table
/// that calls itself the complete LogQL link set was missing a variant —
/// the same defect §3.1 carried at ten times the size.
#[test]
fn every_lql_link_variant_has_a_row_in_the_lowering_document() {
    let md = repo_file(QUERY_LOWERING);
    let s71 = section(&md, S71_HEADING, S72_HEADING);
    let variants = enum_variants("crates/pulsus-read/src/logql/compile.rs", "LqlLink");
    assert_eq!(variants.len(), 9, "LqlLink: {variants:?}");
    assert_every_variant_has_a_row("LqlLink", &variants, &link_row_labels(s71), "§7.1");
}

/// Every `TqlLink` variant has a row in §3.1.
///
/// **Ten of the fifteen had none until part 8**, and three of the ten —
/// `Hydrate`, `Membership(n)` and `SelectValues(n)` — appear in every
/// rendered plan on the search route. The gate the record originally
/// nominated was a hand list of twelve names, which would have passed
/// over exactly that table.
#[test]
fn every_traceql_chain_link_has_a_row_in_the_lowering_document() {
    let md = repo_file(QUERY_LOWERING);
    let s31 = section(&md, S31_HEADING, S32_HEADING);
    let variants = enum_variants("crates/pulsus-read/src/traces/compile.rs", "TqlLink");
    assert_eq!(variants.len(), 15, "TqlLink: {variants:?}");
    assert_every_variant_has_a_row("TqlLink", &variants, &link_row_labels(s31), "§3.1");
}

/// The residual-state-effect group a link row states, read off the row's
/// own effect cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectGroup {
    /// The row states an effect on the accumulated relation.
    Stated,
    /// The row states the effect IS the identity, so the exemption is
    /// itself a check rather than a silence.
    Identity,
    /// The row is marked *not in the chain*, so it has no effect to
    /// state.
    NotInChain,
}

fn effect_group(cell: &str) -> EffectGroup {
    let flat = cell.replace("**", "");
    if flat.starts_with("n/a") {
        // `Source` in §3.1 reads "n/a — the seed is always applied",
        // which is the identity case, not a not-in-the-chain marking.
        if flat.contains("the seed is always applied") {
            EffectGroup::Identity
        } else {
            EffectGroup::NotInChain
        }
    } else if flat.starts_with("none") {
        EffectGroup::Identity
    } else {
        EffectGroup::Stated
    }
}

/// Counts a section's link rows by effect group.
fn effect_counts(slice: &str) -> (usize, usize, usize) {
    let (mut stated, mut identity, mut not_in_chain) = (0, 0, 0);
    for row in link_table_rows(slice) {
        // The effect cell is third from the right: … | effect |
        // disposition | continuation |.
        let cell = &row[row.len() - 3];
        match effect_group(cell) {
            EffectGroup::Stated => stated += 1,
            EffectGroup::Identity => identity += 1,
            EffectGroup::NotInChain => not_in_chain += 1,
        }
    }
    (stated, identity, not_in_chain)
}

/// The `**N**` figures a sentence states, in order.
fn bold_numbers(text: &str) -> Vec<u64> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some((_, tail)) = rest.split_once("**") {
        let (inner, after) = match tail.split_once("**") {
            Some(p) => p,
            None => break,
        };
        if let Ok(n) = inner.replace(',', "").parse::<u64>() {
            out.push(n);
        }
        rest = after;
    }
    out
}

/// **The counts the record states about its own tables are the counts
/// those tables carry**, and the TraceQL one is also the shipped gate's
/// row count.
///
/// Three claims, all in `docs/query-lowering.md`, all with two sides:
///
/// 1. §11.3's gate row states §3.1's and §7.1's `stated` and `without`
///    counts; the tables are counted here;
/// 2. §11.2b states the same two `stated` counts and their sum;
/// 3. §11.2b states that the shipped TraceQL residual-effect gate carries
///    §3.1's row count plus one, and
///    `assert_every_residual_state_effect::<Tql>(&rows, 21)` is read out
///    of the source that calls it — so the derivation and the shipped
///    number are two producers, not one sentence.
#[test]
fn the_document_states_the_residual_effect_counts_the_gates_assert() {
    let md = repo_file(QUERY_LOWERING);
    let s31 = section(&md, S31_HEADING, S32_HEADING);
    let s71 = section(&md, S71_HEADING, S72_HEADING);
    let (t_stated, t_identity, t_na) = effect_counts(s31);
    let (l_stated, l_identity, l_na) = effect_counts(s71);

    let gate_row = md
        .lines()
        .find(|l| l.starts_with("| §3.1 carries exactly "))
        .expect("§11.3 must carry the residual-effect count row");
    // The first cell only: the `at base` cell of the same row carries a
    // bold exit code, and a rule that scanned the whole row would read
    // it as a fifth count.
    let claim = gate_row
        .trim_start_matches('|')
        .split(" | ")
        .next()
        .expect("the row has a first cell");
    let stated = bold_numbers(claim);
    assert_eq!(
        stated,
        vec![
            t_stated as u64,
            (t_identity + t_na) as u64,
            l_stated as u64,
            (l_identity + l_na) as u64
        ],
        "§11.3's residual-effect row states {stated:?}; §3.1 carries {t_stated} with a stated \
         effect and {} without ({t_identity} identity + {t_na} not-in-the-chain), and §7.1 carries \
         {l_stated} and {} ({l_identity} + {l_na})",
        t_identity + t_na,
        l_identity + l_na
    );

    let para = paragraphs(&md)
        .into_iter()
        .find(|p| p.contains("effects in all."))
        .expect("§11.2b must state the effect total");
    let n = bold_numbers(para);
    assert!(
        n.len() >= 3,
        "§11.2b's effect paragraph must state §3.1's count, §7.1's and their sum; it states {n:?}"
    );
    assert_eq!(
        (n[0], n[1], n[2]),
        (
            t_stated as u64,
            l_stated as u64,
            (t_stated + l_stated) as u64
        ),
        "§11.2b states {n:?}; the tables carry {t_stated} and {l_stated}, summing to {}",
        t_stated + l_stated
    );

    // The shipped TraceQL gate's row count, read from the call that
    // asserts it rather than from a sentence.
    let src = repo_file("crates/pulsus-read/src/traces/compile.rs");
    let shipped: u64 = src
        .split_once("assert_every_residual_state_effect::<Tql>(&rows, ")
        .expect("traces/compile.rs must assert its residual-effect row count")
        .1
        .split(')')
        .next()
        .expect("the call is closed")
        .trim()
        .parse()
        .expect("the row count is a number");
    let derived = (t_stated + t_identity) as u64;
    assert_eq!(
        shipped,
        derived + 1,
        "§3.1 derives {derived} residual-effect rows and the shipped gate asserts {shipped}; the \
         difference must be exactly the one extra `By` row the shipped gate carries, one per key \
         branch"
    );
}

/// **The boundary diagram names only links this document defines.**
///
/// Each drawn link box carries a `data-links` attribute naming the
/// document link (or links, for a compressed box) it stands for, and
/// every one of those names must be the leading identifier of a row in
/// §3.1 or §7.1. Reading a machine-readable attribute rather than the
/// drawn caption is deliberate: the captions are query text
/// (`|= "CONN_REFUSED"`) and abbreviations (`Count(> 2)`), so matching
/// them against row labels would need a guessed mapping, and a guessed
/// mapping that happens to work is indistinguishable from a right one.
#[test]
fn the_boundary_diagram_names_only_links_the_document_defines() {
    let md = repo_file(QUERY_LOWERING);
    let mut defined = link_row_labels(section(&md, S31_HEADING, S32_HEADING));
    defined.extend(link_row_labels(section(&md, S71_HEADING, S72_HEADING)));

    let svg = repo_file(BOUNDARY_SVG);
    let drawn = boundary_pipelines(&svg);
    assert!(
        !drawn.is_empty(),
        "{BOUNDARY_SVG} draws no annotated link box"
    );
    let mut seen = 0usize;
    for (pipeline, links) in &drawn {
        for link in links {
            assert!(
                defined.contains(link),
                "the boundary diagram's pipeline {pipeline} names the link {link:?}, which neither \
                 §3.1 nor §7.1 defines"
            );
            seen += 1;
        }
    }
    assert!(
        seen >= 20,
        "only {seen} link names were checked; the diagram draws four pipelines"
    );
}

/// **Every pipeline the boundary diagram draws ends in `Order`, `Limit`
/// and `Emit`**, because every chain does — the three are synthesised by
/// the chain builder and are not optional. A pipeline drawn without them
/// is the truncation §11.3 records as one of the diagram's four
/// contradictions.
#[test]
fn every_boundary_diagram_pipeline_carries_the_three_synthesised_links() {
    let svg = repo_file(BOUNDARY_SVG);
    let drawn = boundary_pipelines(&svg);
    assert!(
        drawn.len() >= 4,
        "{BOUNDARY_SVG} draws {} annotated pipelines; it has four panels",
        drawn.len()
    );
    for (pipeline, links) in &drawn {
        let tail: Vec<&str> = links
            .iter()
            .rev()
            .take(3)
            .rev()
            .map(String::as_str)
            .collect();
        assert_eq!(
            tail,
            vec!["Order", "Limit", "Emit"],
            "the boundary diagram's pipeline {pipeline} ends {tail:?}; every chain ends Order, \
             Limit, Emit"
        );
    }
}

/// `(pipeline, links in drawn order)` for every annotated pipeline.
fn boundary_pipelines(svg: &str) -> Vec<(String, Vec<String>)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    for (i, _) in svg.match_indices("<text class=\"label\" data-pipeline=\"") {
        let rest = &svg[i + "<text class=\"label\" data-pipeline=\"".len()..];
        let (pipeline, rest) = rest.split_once('"').expect("data-pipeline is quoted");
        let links = rest
            .split_once("data-links=\"")
            .expect("an annotated box carries data-links")
            .1
            .split_once('"')
            .expect("data-links is quoted")
            .0;
        let links: Vec<String> = links.split_whitespace().map(str::to_string).collect();
        match out.last_mut() {
            Some((p, acc)) if p == pipeline => acc.extend(links),
            _ => out.push((pipeline.to_string(), links)),
        }
    }
    out
}

/// **Every `Cut` variant has a section of its own in §2.7**, matched by
/// an exhaustive list parsed out of the enum rather than by a hand list.
#[test]
fn every_cut_variant_has_a_row_in_the_design_record() {
    let md = repo_file(QUERY_LOWERING);
    let s27 = section(&md, S27_HEADING, S28_HEADING);
    let variants = enum_variants("crates/pulsus-read/src/compile/plan.rs", "Cut");
    assert_eq!(variants.len(), 4, "Cut: {variants:?}");
    let headings: BTreeSet<String> = s27
        .lines()
        .filter(|l| l.starts_with("#### 2.7."))
        .filter_map(leading_backtick)
        .filter_map(|h| h.strip_prefix("Cut::").map(str::to_string))
        .collect();
    for v in &variants {
        assert!(
            headings.contains(v),
            "Cut::{v} has no section in docs/query-lowering.md §2.7; §2.7 names {headings:?}"
        );
    }
    let extra: Vec<&String> = headings.iter().filter(|h| !variants.contains(h)).collect();
    assert!(
        extra.is_empty(),
        "§2.7 gives a section to {extra:?}, which is not a `Cut` variant"
    );
}

/// **Every chain-link row states a continuation, and a continuation that
/// names a cut names one of the four.**
///
/// The continuation column is what says whether a residual link is served
/// by a second SQL part or by the evaluator. A row with an empty cell is
/// a link whose answer to that question the reader has to infer.
#[test]
fn every_chain_link_row_states_a_continuation() {
    let md = repo_file(QUERY_LOWERING);
    let cuts: BTreeSet<String> = enum_variants("crates/pulsus-read/src/compile/plan.rs", "Cut")
        .into_iter()
        .collect();
    let mut checked = 0usize;
    for (name, heading, end) in [
        ("§3.1", S31_HEADING, S32_HEADING),
        ("§7.1", S71_HEADING, S72_HEADING),
    ] {
        for row in link_table_rows(section(&md, heading, end)) {
            let label = &row[0];
            let cell = row.last().expect("a row has cells");
            assert!(
                !cell.is_empty(),
                "{name}: the row {label:?} states no continuation"
            );
            for (i, _) in cell.match_indices("Cut::") {
                let named: String = cell[i + 5..]
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric())
                    .collect();
                assert!(
                    cuts.contains(&named),
                    "{name}: the row {label:?} names a continuation `Cut::{named}`, which is not \
                     one of the four cuts {cuts:?}"
                );
            }
            checked += 1;
        }
    }
    assert!(
        checked >= 40,
        "only {checked} link rows were checked; §3.1 carries 23 and §7.1 carries 24"
    );
}

const S12_HEADING: &str = "## 12. The end state";

/// **Every `NeverReason` variant is named in the end state**, matched
/// against the enum parsed out of the file that declares it rather than
/// against a hand list.
///
/// `Never` is a permanence claim: it says a construct is not lowerable in
/// any state, ever, as against `No`, which says *not here*. A ninth
/// permanent reason added to the compiler without a row in §12 is a
/// permanence claim nobody had to justify, which is the thing §12 exists
/// to stop.
#[test]
fn every_never_reason_variant_is_named_in_the_end_state() {
    let md = repo_file(QUERY_LOWERING);
    let start = md
        .find(S12_HEADING)
        .unwrap_or_else(|| panic!("{QUERY_LOWERING} must carry {S12_HEADING:?}"));
    let end_state = &md[start..];
    let variants = enum_variants("crates/pulsus-read/src/compile/fold.rs", "NeverReason");
    assert_eq!(variants.len(), 8, "NeverReason: {variants:?}");

    let table: Vec<Vec<String>> = tables(end_state)
        .into_iter()
        .find(|t| t[0].first().map(String::as_str) == Some("`NeverReason`"))
        .expect("§12.1 must carry a table whose first column is `NeverReason`");
    let named: BTreeSet<String> = table
        .iter()
        .skip(1)
        .filter_map(|r| leading_backtick(&r[0]))
        .collect();
    let missing: Vec<&String> = variants.iter().filter(|v| !named.contains(*v)).collect();
    assert!(
        missing.is_empty(),
        "NeverReason::{} is not named in docs/query-lowering.md §12; §12 names {named:?}",
        missing
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    let extra: Vec<&String> = named.iter().filter(|n| !variants.contains(n)).collect();
    assert!(
        extra.is_empty(),
        "§12 gives a row to {extra:?}, which is not a `NeverReason` variant"
    );
    for row in table.iter().skip(1) {
        let name = leading_backtick(&row[0]).unwrap_or_default();
        assert!(
            row.len() == 3 && !row[1].is_empty() && !row[2].is_empty(),
            "§12's row for {name} must state both what the reason rules out and why no state can \
             change it; it reads {row:?}"
        );
    }
}

const REBUILDS_TSV: &str = "docs/benchmarks/data/traces-lowering-92-rebuilds.tsv";
const REBUILD_BLOCK_BEGIN: &str = "<!-- generated from traces-lowering-92-rebuilds.tsv -->";
const REBUILD_BLOCK_END: &str = "<!-- end generated -->";

/// One observation of one column, read from [`REBUILDS_TSV`].
#[derive(Debug, Clone)]
struct Observation {
    id: String,
    scope: String,
    provenance: String,
    column: String,
    statements_moved: String,
    largest_change: String,
    total_change: String,
}

fn observations() -> Vec<Observation> {
    let tsv = repo_file(REBUILDS_TSV);
    let mut out = Vec::new();
    for (n, line) in tsv.lines().enumerate() {
        if n == 0 {
            assert_eq!(
                line,
                "observation\tscope\tprovenance\tcolumn\tstatements_moved\tlargest_change\t\
                 total_change",
                "{REBUILDS_TSV} header"
            );
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        assert_eq!(f.len(), 7, "{REBUILDS_TSV}:{}: seven columns", n + 1);
        assert!(
            !f[2].trim().is_empty(),
            "{REBUILDS_TSV}:{}: every observation states where it came from",
            n + 1
        );
        assert!(
            f[1] == "per_statement" || f[1] == "group_total",
            "{REBUILDS_TSV}:{}: unknown scope {:?}",
            n + 1,
            f[1]
        );
        out.push(Observation {
            id: f[0].into(),
            scope: f[1].into(),
            provenance: f[2].into(),
            column: f[3].into(),
            statements_moved: f[4].into(),
            largest_change: f[5].into(),
            total_change: f[6].into(),
        });
    }
    assert!(!out.is_empty(), "{REBUILDS_TSV} is empty");
    out
}

/// **The whole block — table AND the sentences that state numbers about
/// it — rendered from the dataset.**
///
/// An earlier revision gated the table's cells and left the sentences
/// beside it as prose. A code review changed a prose column count and
/// the suite stayed green: the check covered what it parsed and the
/// English beside it was untouched, which is this part's own defect one
/// layer down.
///
/// **Gating prose by pattern would not have fixed it.** Numbers in
/// English are unbounded — digits, words, ordinals, ranges, "a third of"
/// — so a pattern that catches today's sentences misses tomorrow's and
/// looks like coverage while doing it. The sentences are generated
/// instead: there is nothing here for a person to write a number into.
fn rebuild_block() -> String {
    let obs = observations();
    let per_statement: Vec<&Observation> =
        obs.iter().filter(|o| o.scope == "per_statement").collect();
    let mut ids: Vec<&str> = Vec::new();
    for o in &per_statement {
        if !ids.contains(&o.id.as_str()) {
            ids.push(&o.id);
        }
    }
    let mut columns: Vec<&str> = Vec::new();
    for o in &per_statement {
        if !columns.contains(&o.column.as_str()) {
            columns.push(&o.column);
        }
    }
    let cell = |id: &str, col: &str| -> Option<&Observation> {
        per_statement
            .iter()
            .copied()
            .find(|o| o.id == id && o.column == col)
    };

    let mut out = String::from(REBUILD_BLOCK_BEGIN);
    out.push_str(
        "\n\nEach cell reads *statements moved, of 1,132* / *largest per-statement \
                  change*.\n\n| column |",
    );
    for id in &ids {
        out.push_str(&format!(" {id} |"));
    }
    out.push_str("\n|---|");
    for _ in &ids {
        out.push_str("---|");
    }
    out.push('\n');
    for col in &columns {
        out.push_str(&format!("| `{col}` |"));
        for id in &ids {
            match cell(id, col) {
                Some(o) => {
                    out.push_str(&format!(" {} / {} |", o.statements_moved, o.largest_change))
                }
                None => out.push_str(" not recorded |"),
            }
        }
        out.push('\n');
    }
    out.push('\n');

    for id in &ids {
        let p = per_statement
            .iter()
            .find(|o| o.id == *id)
            .expect("an id has rows")
            .provenance
            .clone();
        out.push_str(&format!("**{id}** is {p}.\n\n"));
    }

    // Observations that recorded group totals rather than per-statement
    // figures get their own sentence, because they cannot appear in the
    // table above without inventing a per-statement number.
    let group: Vec<&Observation> = obs.iter().filter(|o| o.scope == "group_total").collect();
    for id in group
        .iter()
        .map(|o| o.id.clone())
        .collect::<BTreeSet<String>>()
    {
        let rows: Vec<&&Observation> = group.iter().filter(|o| o.id == id).collect();
        let p = &rows[0].provenance;
        let listed: Vec<String> = rows
            .iter()
            .map(|o| format!("`{}` {}", o.column, o.total_change))
            .collect();
        out.push_str(&format!(
            "**{id}** is {p}, so it appears in no column above: {}.\n\n",
            listed.join("; ")
        ));
    }

    let totals: Vec<String> = obs
        .iter()
        .filter(|o| o.scope == "per_statement" && o.total_change != "—")
        .map(|o| format!("{} `{}` {}", o.id, o.column, o.total_change))
        .collect();
    if totals.is_empty() {
        out.push_str("No observation published a change over the whole unlowered request.\n\n");
    } else {
        out.push_str(&format!(
            "Changes over the whole unlowered request, where an observation published one: {}. \
             Every other cell above was published per statement only.\n\n",
            totals.join("; ")
        ));
    }

    // The two derived sentences a person kept getting wrong.
    let unmoved: Vec<String> = columns
        .iter()
        .filter(|c| cell("S", c).is_some_and(|o| o.statements_moved == "0"))
        .map(|c| format!("`{c}`"))
        .collect();
    out.push_str(&format!(
        "On the same corpus, {} of the {} columns did not move on a single statement: {}.\n\n",
        unmoved.len(),
        columns.len(),
        unmoved.join(", ")
    ));

    let differing: Vec<String> = columns
        .iter()
        .filter(|c| match (cell("A", c), cell("C", c)) {
            (Some(a), Some(b)) => {
                (&a.statements_moved, &a.largest_change) != (&b.statements_moved, &b.largest_change)
            }
            _ => false,
        })
        .map(|c| format!("`{c}`"))
        .collect();
    let same: Vec<String> = columns
        .iter()
        .filter(|c| !differing.contains(&format!("`{c}`")))
        .map(|c| format!("`{c}`"))
        .collect();
    out.push_str(&format!(
        "Rebuild C differs from rebuild A on {} of the {} columns; the {} it does not differ on \
         {} {}.\n\n",
        differing.len(),
        columns.len(),
        if same.len() == 1 { "one" } else { "ones" },
        if same.len() == 1 { "is" } else { "are" },
        same.join(", ")
    ));

    // Which columns EVERY rebuild that recorded them found unmoved.
    let rebuilds: BTreeSet<String> = obs
        .iter()
        .filter(|o| o.id != "S")
        .map(|o| o.id.clone())
        .collect();
    let agreed: Vec<String> = columns
        .iter()
        .filter(|c| {
            rebuilds.iter().all(|id| {
                obs.iter()
                    .filter(|o| o.id == *id && o.column == **c)
                    .all(|o| {
                        (o.scope == "per_statement" && o.statements_moved == "0")
                            || (o.scope == "group_total" && o.total_change == "—")
                    })
            })
        })
        .map(|c| format!("`{c}`"))
        .collect();
    if agreed.is_empty() {
        out.push_str(&format!(
            "Of the {} columns, **none** is one every rebuild that recorded it found unmoved.\n\n",
            columns.len()
        ));
    } else {
        out.push_str(&format!(
            "Of the {} columns, {} {} unmoved by every rebuild that recorded {}: {}.\n\n",
            columns.len(),
            agreed.len(),
            if agreed.len() == 1 { "is" } else { "are" },
            if agreed.len() == 1 { "it" } else { "them" },
            agreed.join(", ")
        ));
    }
    // **The withdrawal sentence is generated too, and it did not used
    // to be.** It sat one line outside the marker and stated a count of
    // observations; a code review changed that count and every suite
    // stayed green. A number that satisfies the letter of "the block is
    // generated" by living just outside it is the same defect one line
    // further out, so the region was extended rather than the sentence
    // reworded. It names the observations rather than counting them in
    // order, so there is no ordinal to go stale when a fifth arrives.
    let all_ids: Vec<String> = {
        let mut v: Vec<String> = obs.iter().map(|o| o.id.clone()).collect();
        v.sort();
        v.dedup();
        v
    };
    out.push_str(&format!(
        "An earlier revision of this section said a rebuild is expected to land within rebuild \
         A's figures. That expectation was written before rebuild C, and rebuild C did not meet \
         it. {} observations exist now — {} — and no band is established across them: what they \
         establish is that these columns vary, not by how much. A re-runner should expect their \
         numbers to differ from the committed artefact without reading the difference as a \
         defect.\n\n",
        all_ids.len(),
        all_ids.join(", ")
    ));
    out.push_str(REBUILD_BLOCK_END);
    out
}

fn block_in(md: &str, begin: &str, end: &str) -> String {
    let a = md
        .find(begin)
        .unwrap_or_else(|| panic!("{QUERY_LOWERING} must carry {begin}"));
    let b = md[a..]
        .find(end)
        .unwrap_or_else(|| panic!("{begin} is not closed by {end}"));
    md[a..a + b + end.len()].to_string()
}

/// **The rebuild block in §9.2b is the one the dataset produces**, byte
/// for byte, table and sentences alike.
#[test]
fn the_rebuild_block_is_the_one_the_dataset_produces() {
    let md = repo_file(QUERY_LOWERING);
    assert_eq!(
        block_in(&md, REBUILD_BLOCK_BEGIN, REBUILD_BLOCK_END),
        rebuild_block(),
        "the rebuild block in {QUERY_LOWERING} is not what {REBUILDS_TSV} renders. It is \
         GENERATED — run the ignored `regenerate_the_rebuild_block` and read the diff, rather \
         than editing the document"
    );
}

/// Writes the generated block into `docs/query-lowering.md`. Ignored, so
/// it never runs in CI.
///
/// ```text
/// cargo test -p pulsus-read --test query_lowering_doc_gate -- --ignored
/// ```
#[test]
#[ignore = "writes the generated block in docs/query-lowering.md"]
fn regenerate_the_rebuild_block() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .join(QUERY_LOWERING);
    let md = repo_file(QUERY_LOWERING);
    let old = block_in(&md, REBUILD_BLOCK_BEGIN, REBUILD_BLOCK_END);
    std::fs::write(&root, md.replace(&old, &rebuild_block())).expect("write the document");
}
