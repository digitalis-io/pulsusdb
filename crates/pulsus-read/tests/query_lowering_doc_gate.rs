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
//! **Two of the eleven exist.** One reads
//! [`docs/api.md`](../../../docs/api.md), and it is the one that matters
//! most for a wire surface: its two sides are genuinely independent
//! producers — the keys come from a serializer and the expectation from a
//! document in another directory, so neither can produce the other. The
//! second is `every_superseded_lowered_cost_figure_carries_its_marker`,
//! added by issue #492 part 4. The other **nine remain owed by part 8**
//! (item 3 of that issue's scope enumeration).
//!
//! **Two further tests in this file are not among the eleven** and are
//! not claimed to be: `the_hops_diagram_marks_its_superseded_figures_on_its_own_face`
//! and `the_record_flags_the_two_survivors_nobody_re_measured` assert
//! that a superseded figure carries its marker, which is a different
//! question from whether two artefacts agree.

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
/// The tag every surviving copy of the figure carries.
const MARKER_TAG: &str = "seed + root only";
/// The superseded figure itself.
const SUPERSEDED_FIGURE: &str = "43,636";
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
const UNVERIFIED_FIGURES: [&str; 3] = ["11,340", "169,311,055", "190,353,655"];

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

/// Issue #492 part 4. The two design records quote a lowered-cost figure
/// that covers two of the four statements a lowered search issues. The
/// figure stays until part 8 re-measures §9.2; what may not stay is an
/// UNMARKED copy of it.
///
/// Three mechanical rules:
///
/// 1. `docs/query-lowering.md` carries the marker sentence and the tag;
/// 2. in BOTH records, every paragraph naming the figure also carries the
///    tag — which is what makes the marker findable by someone reading
///    only one of them;
/// 3. neither record still carries any of the five superseded wordings.
#[test]
fn every_superseded_lowered_cost_figure_carries_its_marker() {
    let lowering = repo_file(QUERY_LOWERING);
    assert!(
        lowering.contains(MARKER_SENTENCE),
        "{QUERY_LOWERING} carries no superseded marker: the sentence {MARKER_SENTENCE:?} appears \
         nowhere in it"
    );
    assert!(
        lowering.contains(MARKER_TAG),
        "{QUERY_LOWERING} carries the marker sentence but not the tag {MARKER_TAG:?}, so a reader \
         cannot tell WHICH figures are superseded"
    );
    for rel in [QUERY_LOWERING, QUERY_TO_SQL] {
        let text = repo_file(rel);
        let mut seen = 0usize;
        for para in paragraphs(&text) {
            if !para.contains(SUPERSEDED_FIGURE) {
                continue;
            }
            seen += 1;
            assert!(
                para.contains(MARKER_TAG),
                "{rel}: a paragraph quotes {SUPERSEDED_FIGURE} without the tag {MARKER_TAG:?}. It \
                 opens: {:?}",
                para.lines().next().unwrap_or("")
            );
        }
        assert!(
            seen > 0,
            "{rel} quotes {SUPERSEDED_FIGURE} nowhere — this rule is checking nothing"
        );
        for wording in SUPERSEDED_WORDING {
            assert!(
                !text.contains(wording),
                "{rel} still carries the superseded wording {wording:?}: the lowered form is four \
                 statements, not two"
            );
        }
    }
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

/// Issue #492 part 4. **The record flags the two figures nobody
/// re-measured.**
///
/// The client's `11,340 B` and the peak-memory pair are not superseded by
/// the statement-count correction — and nobody has re-measured them
/// either, and neither corpus is standing, so the verdict rests on
/// argument alone. A figure marked *survives* is never looked at again;
/// this keeps the flag in the record so part 8 either re-measures both or
/// says it did not.
#[test]
fn the_record_flags_the_two_survivors_nobody_re_measured() {
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
