//! Issue #479 AC11 — the documentation and the ledger say what the code
//! does, checked BY COMMAND rather than by reading.
//!
//! Two claims, both relational and both checked over their whole stated
//! domain rather than over a sample:
//!
//! * each of the three ledger ids this issue adds exists AND names BOTH
//!   mounted routes (`/api/traces/v1/search` and its `/api/search`
//!   alias) — a row that names one route goes stale invisibly when the
//!   two answer differently;
//! * `docs/api.md` §4.2's span-summary paragraph states every clause of
//!   the projection rule the code implements.
//!
//! Hermetic: reads two committed files, runs no query and needs no
//! container. Built on the `entry_body`/`squash` pattern
//! `tests/traces_metrics_ledger.rs` already uses — the ledger is
//! hard-wrapped prose, so a needle must be a property of the sentence and
//! not of the column somebody's editor chose.

use std::path::PathBuf;

/// The three ids this issue adds. The count is in the type: a row added
/// to the ledger without a clause here, or removed while a clause
/// survives, moves this array.
const LEDGER_IDS: [&str; 3] = [
    "traceql-matched-span-attribute-order",
    "traceql-matched-span-negated-attribute-value-absent",
    "traceql-matched-span-multi-field-leaf-not-projected",
];

/// Both mounted routes. The search handler is one function behind two
/// paths, so a divergence recorded for one is a divergence on both — and
/// a reader who greps only the alias must still find the row.
const ROUTES: [&str; 2] = ["/api/traces/v1/search", "/api/search"];

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    let path = workspace_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"))
}

/// Collapses every run of whitespace to one space, so a needle is a
/// property of the sentence rather than of the hard wrap.
fn squash(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The body of one `### \`<id>\`` section, up to the next `### `.
fn entry_body<'a>(ledger: &'a str, id: &str) -> &'a str {
    let marker = format!("### `{id}`");
    let start = ledger
        .find(&marker)
        .unwrap_or_else(|| panic!("docs/benchmarks/traces-differential-ledger.md has no {marker}"));
    let rest = &ledger[start + marker.len()..];
    match rest.find("\n### ") {
        Some(end) => &rest[..end],
        None => rest,
    }
}

#[test]
fn every_projection_ledger_row_names_both_mounted_routes() {
    let ledger = read("docs/benchmarks/traces-differential-ledger.md");
    for id in LEDGER_IDS {
        let body = squash(entry_body(&ledger, id));
        for route in ROUTES {
            assert!(
                body.contains(route),
                "{id} does not name the route {route} — a ledger row scoped to one of the two \
                 mounted paths goes stale invisibly when they answer differently.\nbody: {body}"
            );
        }
    }
}

#[test]
fn the_api_doc_states_every_clause_of_the_projection_rule() {
    let doc = squash(&read("docs/api.md"));
    // Each clause is a SEPARATE property of the rule, so each is checked
    // separately: a paragraph that states four of five is a paragraph a
    // reader would act on wrongly.
    let clauses: [(&str, &str); 8] = [
        (
            "the condition half",
            "the fields the query filtered on with a **single-field condition that matched THAT \
             span**",
        ),
        // The clause the wave-1 code review found missing: the paragraph
        // listed "field-vs-field" flatly under the classes that project
        // nothing, so it said the opposite of what the code now does for
        // the same-field form.
        (
            "what makes a condition single-field",
            "**A condition is single-field when exactly ONE DISTINCT field appears across BOTH \
             its operands**",
        ),
        (
            "the same-field comparisons that DO project",
            "the degenerate same-field comparisons `{.a = .a}`, `{name = name}`, \
             `{nestedSetLeft = nestedSetLeft}` and `{resource.service.name = \
             resource.service.name}` all project that one field",
        ),
        ("the select half", "plus every `select()`ed field"),
        (
            "the bare key",
            "keyed by the **bare** attribute name — `http.method`, never `span.http.method`",
        ),
        (
            "the conditional name",
            "The `name` key is present **only** when the query referenced `name`",
        ),
        (
            "the seven envelope fields",
            "**Seven fields the response envelope already carries are never projected as \
             attributes**",
        ),
        (
            "span:childCount",
            "**`span:childCount` is never projected** in either position",
        ),
    ];
    // The multi-field clause must name the DISTINCT-field restriction, not
    // "field-vs-field" flatly — the wording the review found wrong.
    assert!(
        !doc.contains(&squash(
            "a **multi-field** condition (field-vs-field, cross-attribute arithmetic"
        )),
        "docs/api.md §4.2 still lists field-vs-field flatly as a class that projects nothing;          a same-field comparison DOES project"
    );
    for (what, needle) in clauses {
        assert!(
            doc.contains(&squash(needle)),
            "docs/api.md §4.2 does not state {what}: {needle:?}"
        );
    }
}
