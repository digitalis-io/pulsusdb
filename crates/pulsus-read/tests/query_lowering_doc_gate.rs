//! Issue #492: the gates that hold the compile core's inspectable
//! projection to the document that publishes it.
//!
//! # What is here, and what is not
//!
//! The design record for this work nominates **eleven** document gates.
//! Ten of them parse artefacts that are **not in this repository**: the
//! query-lowering design record, the query-to-SQL table, ADR 0008 and the
//! two diagrams are untracked working notes, so a committed test that
//! read one would fail on every checkout that does not happen to have
//! them on disk — including CI. They are recorded as owed rather than
//! written against a file the build cannot see.
//!
//! The eleventh reads [`docs/api.md`](../../../docs/api.md), which IS
//! committed, and it is the one that matters most for a wire surface: it
//! is the only gate of the eleven whose two sides are genuinely
//! independent producers — the keys come from a serializer and the
//! expectation from a document in another directory, so neither can
//! produce the other.

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
