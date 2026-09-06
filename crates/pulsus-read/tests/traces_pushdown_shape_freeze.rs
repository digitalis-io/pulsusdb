//! Issue #492 part 5: **every pipeline shape's push status, frozen.**
//!
//! # What this is for
//!
//! Part 5 changes WHICH queries send a pushed `HAVING`, in both
//! directions, and the changes are not confined to the queries the plan
//! names. Five mechanisms move a query's push status, and they compose:
//!
//! ```text
//!   E   the ServiceEq family becomes an exact generator
//!   W   AggregateLower::fidelity -> Wider, and the having-is-empty guard
//!   G   a renderable by() key becomes a grouped fragment
//!   C   coalesce() frees the grouping slot
//!   S6  the six-cell (aggregate, operator) rule
//! ```
//!
//! A list of example queries cannot say that the set is closed. An
//! enumeration can: five selectors, every pipeline word of length at
//! most three over a seven-symbol alphabet, 2,000 queries, each planned
//! and each row frozen.
//!
//! # Two tests over one file, and only one of them is criterion 28
//!
//! [`the_push_status_of_every_enumerated_shape_is_the_frozen_one`]
//! compares the `PUSH` column and nothing else. That is criterion 28.
//!
//! [`every_enumerated_shape_matches_the_frozen_row_byte_for_byte`]
//! compares whole rows, covering `EXACT`, `STMT_HAVING`, `YIELDS0` and
//! `LINKS`. It is the freeze for those four columns and is deliberately
//! NOT what criterion 28 cites, because it cannot distinguish a moved
//! push status from a moved disposition word. Measured on the base tree
//! with the two halves of the `W` rule disabled separately:
//!
//! ```text
//!   break                                  ROWS moved  PUSH moved
//!   Fidelity::Equivalent restored             1,734          0
//!   the having-is-empty guard deleted             0          0
//!   both (the complete W rule disabled)       1,734        477
//! ```
//!
//! The byte freeze gives the SAME message on the first and third rows.
//! So a reviewer who sees only the byte diff go red must look at the
//! other test — the "do not edit by hand" note in the golden says so.
//!
//! Hermetic — planning only, no ClickHouse. Regenerate deliberately with
//! the `#[ignore]`d [`regenerate_shape_freeze`] and review the diff.

use std::path::PathBuf;

use pulsus_read::SpanFilterCtx;
use pulsus_read::traces::search_plan::{SearchCtx, SearchParams, plan_search};

/// One symbol per rule-relevant CLASS, not one per spelling. The
/// enumeration establishes the class ("a renderable key" against "a key
/// that refuses"), never the membership of the accept list — that is the
/// refusal table's job.
const ALPHABET: [(&str, &str); 7] = [
    ("a1", "count() > 2"),        // a pushable cell
    ("a2", "count() < 2"),        // an anti-monotone cell
    ("a6", "avg(duration) > 1s"), // a refused aggregate FAMILY
    ("b1", "by(name)"),           // a renderable key
    ("b2", "by(status)"),         // a key that refuses
    ("c", "coalesce()"),
    ("s", "select(span.foo)"),
];

const SELECTORS: [(&str, &str); 5] = [
    ("S1", r#"{ span.http.method = "GET" }"#), // exact today
    ("S2", r#"{ resource.service.name = "svc" }"#), // exact only after part 5
    ("S3", r#"{ span.http.method = "GET" && duration > 1s }"#), // never exact (multi-leaf)
    ("S4", r#"{ name =~ "a" }"#),              // never exact (regex)
    ("S5", r#"{ .k = "v" }"#),                 // exact today, unscoped
];

/// The longest pipeline the enumeration walks. Three is where the last
/// of the five mechanisms first appears (`b1|c|a1` — take the slot, free
/// it, take it again), so a shorter word cannot witness the set.
const MAX_PIPELINE_LEN: usize = 3;

/// `-` for "nothing pushed". The regeneration asserts no fragment can
/// equal it and none contains a tab, so the sentinel cannot be forged by
/// data.
const NONE_SENTINEL: &str = "-";

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join("traces_pushdown_shapes.txt")
}

/// Every `(key, query)` the enumeration covers, in a fixed order: for
/// each selector, the empty pipeline first, then words of length one,
/// two and three in alphabet order.
fn enumerated() -> Vec<(String, String)> {
    let mut words: Vec<Vec<usize>> = vec![Vec::new()];
    let mut frontier: Vec<Vec<usize>> = vec![Vec::new()];
    for _ in 0..MAX_PIPELINE_LEN {
        let mut next = Vec::new();
        for w in &frontier {
            for i in 0..ALPHABET.len() {
                let mut w2 = w.clone();
                w2.push(i);
                next.push(w2);
            }
        }
        words.extend(next.iter().cloned());
        frontier = next;
    }
    let mut out = Vec::new();
    for (sel_name, sel) in SELECTORS {
        for w in &words {
            let key = format!(
                "{sel_name}/{}",
                w.iter()
                    .map(|i| ALPHABET[*i].0)
                    .collect::<Vec<_>>()
                    .join("|")
            );
            let mut q = sel.to_string();
            for i in w {
                q.push_str(" | ");
                q.push_str(ALPHABET[*i].1);
            }
            out.push((key, q));
        }
    }
    out
}

/// One frozen line. Tab-delimited, five fields, the key first.
fn row(key: &str, q: &str) -> String {
    let query = pulsus_traceql::parse(q).unwrap_or_else(|e| panic!("{key} ({q}): {e}"));
    let plan = plan_search(
        &query,
        &SearchParams {
            start_ns: 1_700_000_000_000_000_000,
            end_ns: 1_700_010_800_000_000_000,
            limit: 20,
            spss: 3,
        },
        &SearchCtx {
            filter: SpanFilterCtx {
                spans_table: "trace_spans",
                attrs_table: "trace_attrs_idx",
            },
            max_candidates: 100_000,
            max_series: 1_000,
            distributed: false,
        },
    )
    .unwrap_or_else(|e| panic!("{key} ({q}): {e:?}"));

    let exact = match plan.generator_exact() {
        Ok(()) => "ok".to_string(),
        Err(why) => format!("{why:?}"),
    };
    let push = plan
        .pushed_having()
        .map_or(NONE_SENTINEL.to_string(), str::to_string);
    // Whether the STATEMENT carries a `HAVING`, which is a different
    // question from whether the PLAN recorded one: a plan that records a
    // fragment and a statement that does not carry it is precisely the
    // drift the pushdown's own gates exist to catch.
    let stmt_having = if plan.generator_sqls.iter().any(|s| s.contains("\nHAVING ")) {
        "yes"
    } else {
        "no"
    };
    let shape = plan.plan_shape();
    let yields0 = match shape.parts.first() {
        Some(pulsus_read::compile::PartShape::Sql(s)) => s.yields,
        _ => "-",
    };
    let links = shape
        .links
        .iter()
        .filter(|l| l.stage == "Source" || l.stage.starts_with("Pipe(") || l.stage == "Order")
        .map(|l| {
            format!(
                "{}:{}:{}",
                l.stage,
                l.how,
                l.fidelity.or(l.why).unwrap_or("-")
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{key}\tEXACT={exact}\tPUSH={push}\tSTMT_HAVING={stmt_having}\tYIELDS0={yields0}\tLINKS={links}"
    )
}

fn rendered() -> Vec<String> {
    enumerated().iter().map(|(key, q)| row(key, q)).collect()
}

fn frozen() -> Vec<String> {
    let path = fixture_path();
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "read {}: {e} — regenerate with the #[ignore]d regenerate_shape_freeze and review \
             the diff",
            path.display()
        )
    });
    raw.lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .map(str::to_string)
        .collect()
}

/// One field of a frozen line, by name; `""` when absent.
fn column<'a>(line: &'a str, name: &str) -> &'a str {
    line.split('\t')
        .find_map(|f| f.strip_prefix(&format!("{name}=")))
        .unwrap_or("")
}

fn key_of(line: &str) -> &str {
    line.split('\t').next().unwrap_or("")
}

/// Asserts the two lists cover the same shapes in the same order, so
/// every column comparison below is between corresponding rows.
fn aligned(frozen: &[String], rendered: &[String]) {
    assert_eq!(
        frozen.len(),
        rendered.len(),
        "the enumeration is {} shapes and the freeze holds {} — the ALPHABET, the SELECTORS or \
         MAX_PIPELINE_LEN moved, which is a deliberate act and moves the fixture with it",
        rendered.len(),
        frozen.len()
    );
    let f: Vec<&str> = frozen.iter().map(|l| key_of(l)).collect();
    let r: Vec<&str> = rendered.iter().map(|l| key_of(l)).collect();
    assert_eq!(f, r, "the enumerated shape order moved");
}

/// **Issue #492 part 5 criterion 28: the enumerated push status is the
/// frozen one, and that is the whole of this test.**
///
/// It compares the `PUSH` column and nothing else, so it reddens for a
/// query that started or stopped pushing and for nothing that merely
/// renders a different word.
///
/// *RED when:* any enumerated shape's `pushed_having()` moves.
#[test]
fn the_push_status_of_every_enumerated_shape_is_the_frozen_one() {
    let frozen = frozen();
    let rendered = rendered();
    aligned(&frozen, &rendered);
    let moved: Vec<String> = frozen
        .iter()
        .zip(&rendered)
        .filter(|(f, r)| column(f, "PUSH") != column(r, "PUSH"))
        .map(|(f, r)| {
            format!(
                "{}: frozen {:?}, now {:?}",
                key_of(f),
                column(f, "PUSH"),
                column(r, "PUSH")
            )
        })
        .collect();
    assert!(
        moved.is_empty(),
        "{} enumerated shape(s) changed PUSH STATUS: {:?}",
        moved.len(),
        moved
    );
}

/// The freeze for the other four columns: `EXACT`, `STMT_HAVING`,
/// `YIELDS0` and `LINKS`.
///
/// **Not criterion 28's evidence.** Its message is identical whether a
/// push status moved or only a disposition word did, which is exactly
/// the distinction criterion 28 is about.
#[test]
fn every_enumerated_shape_matches_the_frozen_row_byte_for_byte() {
    let frozen = frozen();
    let rendered = rendered();
    aligned(&frozen, &rendered);
    let drifted: Vec<&str> = frozen
        .iter()
        .zip(&rendered)
        .filter(|(f, r)| f != r)
        .map(|(f, _)| key_of(f))
        .collect();
    assert!(
        drifted.is_empty(),
        "{} enumerated row(s) drifted. First: {}. The PUSH column is a SEPARATE gate — if it is \
         green, what moved is a disposition word, not which queries push",
        drifted.len(),
        drifted[0]
    );
}

/// The enumeration is the set it claims: 2,000 shapes, five selectors,
/// every word of length at most three over seven symbols.
///
/// Without it a `enumerated()` that silently returned fewer shapes would
/// make both freezes pass over a shorter list.
#[test]
fn the_enumeration_is_the_set_it_claims() {
    let all = enumerated();
    assert_eq!(
        all.len(),
        SELECTORS.len() * (1 + 7 + 49 + 343),
        "five selectors x every word of length <= 3 over seven symbols"
    );
    assert_eq!(all.len(), 2_000);
    let keys: std::collections::BTreeSet<&str> = all.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys.len(), all.len(), "every shape key is distinct");
    // Every mechanism has a witness in the set.
    for needed in ["S2/a1", "S2/b1|a1", "S2/b1|c|b1|a1", "S1/a1|a1", "S1/a2"] {
        assert!(
            keys.contains(needed) || needed.matches('|').count() >= MAX_PIPELINE_LEN,
            "{needed} must be enumerated"
        );
    }
}

/// Regenerates the committed fixture. `#[ignore]`d: run explicitly after
/// an intentional change, review the diff, and say so in the notes
/// (byte-frozen-artifact rule).
#[test]
#[ignore = "regenerates the committed enumeration freeze; run explicitly, see doc comment"]
fn regenerate_shape_freeze() {
    let rows = rendered();
    // The sentinel cannot be forged by data, and the delimiter cannot
    // appear inside a field.
    for line in &rows {
        let push = column(line, "PUSH");
        assert!(
            push == NONE_SENTINEL || (push.contains(' ') && push.contains('(')),
            "{}: {push:?} is neither the None sentinel nor a fragment",
            key_of(line)
        );
        assert_eq!(
            line.matches('\t').count(),
            5,
            "{}: a field carries a tab",
            key_of(line)
        );
    }
    let path = fixture_path();
    std::fs::create_dir_all(path.parent().expect("golden dir")).expect("create golden dir");
    let header = "\
# Issue #492 part 5 — the enumerated pipeline shapes and their push status.
#
# DO NOT EDIT BY HAND. Regenerate with the #[ignore]d
# `regenerate_shape_freeze` in tests/traces_pushdown_shape_freeze.rs and
# review the diff.
#
# TWO gates read this file. `the_push_status_of_every_enumerated_shape_is_the_frozen_one`
# compares the PUSH column ALONE; `every_enumerated_shape_matches_the_frozen_row_byte_for_byte`
# compares whole rows. If only the byte gate is red, what moved is a
# disposition word and not which queries push — look at both before
# regenerating.
#
# Key: <selector>/<pipeline word>, over the alphabet and selector list
# fixed in the test file.
";
    std::fs::write(&path, format!("{header}{}\n", rows.join("\n")))
        .unwrap_or_else(|e| panic!("write {path:?}: {e}"));
    eprintln!("wrote {} ({} shapes)", path.display(), rows.len());
}
