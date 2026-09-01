//! Issue #479 AC11 — the documentation and the ledger say what the code
//! does, checked BY COMMAND rather than by reading.
//!
//! Two claims, both relational and both checked over their whole stated
//! domain rather than over a sample:
//!
//! * each of the four ledger ids this issue adds exists AND names BOTH
//!   mounted routes (`/api/traces/v1/search` and its `/api/search`
//!   alias) — a row that names one route goes stale invisibly when the
//!   two answer differently;
//! * `docs/api.md` §4.2's matched-span projection paragraph states every
//!   clause of the projection rule the code implements.
//!
//! **Why every assertion here is scoped and counted** (code review wave
//! 2). The previous version asked whether a substring occurred SOMEWHERE
//! in a file, and every one of its eleven assertions could be satisfied
//! by accident: a route named anywhere in a row satisfied "this row names
//! both routes" — demonstrated by moving `/api/search` out of the scope
//! sentence into an unrelated one, which still passed — and each API
//! clause was searched over the whole of `docs/api.md` rather than the
//! section it is claimed to be in. A claim about a RELATION cannot be
//! tested by presence. So each check below fixes the REGION the claim is
//! about (the row's `**What:**` bullet; §4.2's projection paragraph) and
//! asserts a COUNT in it — one occurrence, in that region and nowhere
//! else in the document. Position and uniqueness are what an incidental
//! mention cannot supply.
//!
//! What this still does not establish: that the sentence in the region is
//! TRUE of the code. It establishes that the exact sentence exists, once,
//! where the rule is stated — the code side of each clause is pinned by
//! the planner unit tests and the live differential named beside it.
//!
//! Hermetic: reads two committed files, runs no query and needs no
//! container.

use std::path::PathBuf;

/// The four ids this issue adds. The count is in the type: a row added
/// to the ledger without a clause here, or removed while a clause
/// survives, moves this array.
const LEDGER_IDS: [&str; 4] = [
    "traceql-matched-span-attribute-order",
    "traceql-matched-span-negated-attribute-value-absent",
    "traceql-matched-span-multi-field-leaf-not-projected",
    "traceql-matched-span-nil-condition-instance-state",
];

/// Both mounted routes. The search handler is one function behind two
/// paths, so a divergence recorded for one is a divergence on both — and
/// a reader who greps only the alias must still find the row.
const ROUTES: [&str; 2] = ["/api/traces/v1/search", "/api/search"];

/// The §4.2 heading whose body carries the projection rule.
const API_SECTION: &str = "### 4.2 `GET /api/traces/v1/search`";

/// The projection paragraph's bounds inside §4.2. Both must occur exactly
/// once in the section: if the document is reorganised this test fails
/// loudly instead of silently widening its own scope back to the whole
/// file, which is the defect it was written to remove.
const REGION_START: &str = "**The matched-span projection** (issue #479):";
const REGION_END: &str = "**The two duration fields differ by level and by unit**";

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

/// Non-overlapping occurrences of `needle` in `hay`.
fn count(hay: &str, needle: &str) -> usize {
    hay.matches(needle).count()
}

/// The lines of a markdown document that are OUTSIDE every fenced code
/// block, as `(index, line)`.
///
/// A heading-shaped line inside a fence is an example, not a heading;
/// selecting one as a row's body is how a lookup finds text that is not
/// the row.
fn lines_outside_fences(doc: &str) -> Vec<(usize, &str)> {
    let mut fenced = false;
    let mut out = Vec::new();
    for (i, line) in doc.lines().enumerate() {
        if line.trim_start().starts_with("```") {
            fenced = !fenced;
            continue;
        }
        if !fenced {
            out.push((i, line));
        }
    }
    out
}

/// The body under the UNIQUE heading line beginning with `marker`, up to
/// the next heading line — both taken outside fenced code blocks.
///
/// Uniqueness is asserted, not assumed: `find` on the raw text picks the
/// first match wherever it is, so a heading-shaped token in prose or a
/// code block could be selected as the row (code review wave 2).
fn unique_section(doc: &str, marker: &str, what: &str) -> String {
    let lines = lines_outside_fences(doc);
    let heads: Vec<usize> = lines
        .iter()
        .filter(|(_, l)| l.starts_with(marker))
        .map(|(i, _)| *i)
        .collect();
    assert_eq!(
        heads.len(),
        1,
        "{what} must contain exactly one heading line starting {marker:?}; found {} at lines \
         {heads:?}",
        heads.len()
    );
    let start = heads[0];
    let end = lines
        .iter()
        .find(|(i, l)| *i > start && l.starts_with('#'))
        .map_or(usize::MAX, |(i, _)| *i);
    lines
        .iter()
        .filter(|(i, _)| *i > start && *i < end)
        .map(|(_, l)| *l)
        .collect::<Vec<_>>()
        .join("\n")
}

/// A ledger row's SCOPE sentence: its `- **What:**` bullet, up to the next
/// sibling bullet.
///
/// This is where a row states what it is about, and therefore the only
/// place a route name scopes the row. The rest of the row may mention a
/// route in passing — the previous check counted such a mention, so a row
/// whose scope named one route only still passed.
fn what_bullet(body: &str, id: &str) -> String {
    let lines: Vec<&str> = body.lines().collect();
    let starts: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.starts_with("- **What:**"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        starts.len(),
        1,
        "the ledger row {id} must have exactly one `- **What:**` bullet; found {}",
        starts.len()
    );
    let start = starts[0];
    let end = lines
        .iter()
        .enumerate()
        .find(|(i, l)| *i > start && l.starts_with("- **"))
        .map_or(lines.len(), |(i, _)| i);
    squash(&lines[start..end].join("\n"))
}

#[test]
fn every_projection_ledger_row_names_both_mounted_routes() {
    let ledger = read("docs/benchmarks/traces-differential-ledger.md");
    for id in LEDGER_IDS {
        let body = unique_section(
            &ledger,
            &format!("### `{id}`"),
            "docs/benchmarks/traces-differential-ledger.md",
        );
        let scope = what_bullet(&body, id);
        for route in ROUTES {
            assert_eq!(
                count(&scope, route),
                1,
                "the ledger row {id} names the route {route} {} time(s) in its `**What:**` \
                 bullet, not once. A row scoped to one of the two mounted paths goes stale \
                 invisibly when they answer differently, and a mention further down the row is \
                 not a scope.\nscope sentence: {scope}",
                count(&scope, route)
            );
        }
    }
}

#[test]
fn the_api_doc_states_every_clause_of_the_projection_rule() {
    let raw = read("docs/api.md");
    let doc = squash(&raw);
    let section = squash(&unique_section(&raw, API_SECTION, "docs/api.md"));

    // The region the rule is stated in, bounded by two markers that must
    // each occur exactly once inside §4.2.
    for marker in [REGION_START, REGION_END] {
        assert_eq!(
            count(&section, marker),
            1,
            "docs/api.md §4.2 must contain {marker:?} exactly once; found {}",
            count(&section, marker)
        );
    }
    let from = section.find(REGION_START).expect("counted above");
    let to = section.find(REGION_END).expect("counted above");
    assert!(
        from < to,
        "docs/api.md §4.2's projection paragraph starts after it ends"
    );
    let region = &section[from..to];

    // Each clause is a SEPARATE property of the rule, so each is checked
    // separately: a paragraph that states seven of eight is a paragraph a
    // reader would act on wrongly. Each must occur ONCE in the region and
    // ONCE in the whole document — together that says the document's only
    // statement of the clause is the one inside the rule, which no
    // incidental mention elsewhere can supply.
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
    for (what, needle) in clauses {
        let needle = squash(needle);
        assert_eq!(
            count(region, &needle),
            1,
            "docs/api.md §4.2's projection paragraph states {what} {} time(s), not once: \
             {needle:?}",
            count(region, &needle)
        );
        assert_eq!(
            count(&doc, &needle),
            1,
            "docs/api.md states {what} {} time(s) across the whole document; the rule is stated \
             in one place so a second copy can go stale unnoticed",
            count(&doc, &needle)
        );
    }

    // The multi-field clause, checked as a POSITION rather than as the
    // absence of the old wording. A negative needle is evaded by
    // paraphrase; this says instead that the region mentions
    // "field-vs-field" exactly once and that the one mention carries the
    // DISTINCT-field restriction — the wording the wave-1 review found
    // wrong cannot be added without a second mention.
    assert_eq!(
        count(region, "field-vs-field"),
        1,
        "docs/api.md §4.2's projection paragraph mentions field-vs-field {} time(s); the rule \
         distinguishes the same-field form from the different-field one in exactly one place",
        count(region, "field-vs-field")
    );
    assert_eq!(
        count(region, "field-vs-field between two DIFFERENT fields"),
        1,
        "the projection paragraph's one mention of field-vs-field does not carry the \
         DISTINCT-field restriction: a same-field comparison DOES project, and listing \
         field-vs-field flatly says the opposite"
    );
}
