//! Issue #544 — the hermetic gates for the structured-metadata label
//! filter that need the tree rather than one module.
//!
//! Three things live here because none of them is a property of a single
//! function: the frozen planner record's delta, the set of writers that
//! reach `log_samples.structured_metadata`, and the dependency this work
//! is built on.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root")
}

fn read(rel: &str) -> String {
    let p = repo_root().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
}

/// The merge base of issue #544 — the revision whose committed planner
/// golden this change's delta is measured against.
///
/// **A commit, not a branch or a tag.** The blob is reachable by its hash
/// for as long as the history is, so this stays checkable after the merge;
/// a branch name would move and a tag would need making.
const MERGE_BASE: &str = "008826cd7379720ad2dee833e426990acd14b99f";

const PLANNER_GOLDEN: &str = "crates/pulsus-read/tests/golden/plan_build_differential.txt";

fn blob_at(commit: &str, rel: &str) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo_root())
        .arg("show")
        .arg(format!("{commit}:{rel}"))
        .output()
        .expect("git show");
    assert!(
        out.status.success(),
        "git show {commit}:{rel} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf-8")
}

/// Issue #544 AC18 — **the frozen planner record's delta is additive and
/// nothing else.**
///
/// `MetricPlan`'s field set is pinned by
/// `tests/golden/plan_build_differential.txt`, which `Debug`-prints it in
/// full across 178 occurrences, so the new `metadata_lowering` field moves
/// those bytes and the golden has to be regenerated. This is what makes
/// the regeneration reviewable: the diff between the committed golden at
/// the merge base and the one in the tree consists **only of added
/// lines**, every one of them the new field printing its empty value, with
/// no line removed and no line changed.
///
/// The corpus can carry that claim because **no entry in it carries a
/// label filter** — asserted below, so the day someone adds one this test
/// says so rather than waving the regeneration through.
#[test]
fn the_regenerated_planner_golden_differs_only_by_the_new_field() {
    let before = blob_at(MERGE_BASE, PLANNER_GOLDEN);
    let after = read(PLANNER_GOLDEN);
    let old: Vec<&str> = before.lines().collect();
    let new: Vec<&str> = after.lines().collect();

    // A line-by-line walk that only ever ADVANCES, so a removed or
    // changed line is a mismatch rather than a resynchronisation.
    let mut i = 0usize;
    let mut added: Vec<&str> = Vec::new();
    for line in &new {
        if i < old.len() && old[i] == *line {
            i += 1;
        } else {
            added.push(line);
        }
    }
    assert_eq!(
        i,
        old.len(),
        "the golden lost or changed a line: {} of {} committed lines survive, first unmatched: \
         {:?}",
        i,
        old.len(),
        old.get(i)
    );
    let trimmed: BTreeSet<&str> = added.iter().map(|l| l.trim()).collect();
    assert_eq!(
        trimmed,
        BTreeSet::from(["metadata_lowering: None,"]),
        "every added line must be the new field printing its empty value; the added lines, \
         trimmed, are {trimmed:?}"
    );
    assert_eq!(
        added.len(),
        178,
        "the golden `Debug`-prints `MetricPlan` 178 times, so 178 lines are added"
    );

    // Non-vacuity: the delta is behaviour-free because no corpus entry
    // carries a label filter. Decided by the PARSER, not by a pattern: a
    // selector and a binary operator both print a `|`.
    let entries: Vec<&str> = after.lines().filter(|l| l.starts_with("=== ")).collect();
    assert_eq!(entries.len(), 144, "the corpus size the delta rests on");
    let mut with_filter: Vec<&str> = Vec::new();
    for entry in &entries {
        let query = entry
            .strip_prefix("=== ")
            .and_then(|r| r.split_once(" | "))
            .map(|(_, q)| q)
            .unwrap_or_else(|| panic!("a corpus entry line with no query: {entry}"));
        let Ok(expr) = pulsus_logql::parse(query) else {
            // The corpus deliberately carries uncompilable regexes; they
            // parse, so a parse failure here is a corpus line this walk
            // does not understand and is worth failing on.
            panic!("a corpus entry no longer parses: {query}");
        };
        if holds_a_label_filter(&expr) {
            with_filter.push(entry);
        }
    }
    assert!(
        with_filter.is_empty(),
        "a corpus entry now carries a label filter, so the new field no longer prints its \
         empty value on every occurrence: {with_filter:?}"
    );
}

/// Does any pipeline in this expression carry a `| name = "v"` stage?
///
/// Walks the metric tree with the parser's own iterative walk, so a
/// binary or `variants(...)` entry is covered by the same rule as a leaf.
fn holds_a_label_filter(expr: &pulsus_logql::Expr) -> bool {
    use pulsus_logql::{Expr, MeNode, MetricExpr, Stage};
    let holds = |pipeline: &[Stage]| pipeline.iter().any(|s| matches!(s, Stage::LabelFilter(_)));
    match expr {
        Expr::Log(l) => holds(&l.pipeline),
        Expr::Metric(m) => {
            let mut found = false;
            pulsus_logql::for_each_metric_expr(m, |n| match n {
                MeNode::Expr(MetricExpr::Range { range, .. }) => {
                    if holds(&range.selector.pipeline) {
                        found = true;
                    }
                }
                MeNode::Expr(_) => {}
                MeNode::Var(v) => {
                    if holds(&v.range.selector.pipeline) {
                        found = true;
                    }
                }
            });
            found
        }
    }
}

/// Issue #544 AC0 — **the dependency is present**, checked as the eight
/// properties rather than as a branch name.
///
/// The lowered predicate is exact only if our reader and the database read
/// the same value out of the bytes our encoder writes. That is what the
/// shared decoder (issue #539) provides, and these are its eight
/// properties in the merged result. Two of the names moved between the
/// branch the plan inspected and the merge, which is what the plan's
/// mapping rule was for; the names asserted here are the merged ones.
#[test]
fn the_reader_dependency_is_satisfied() {
    let module = read("crates/pulsus-read/src/canonical_labels.rs");
    for entry in [
        "fn parse_canonical_labels_into",
        "fn parse_canonical_labels(",
        "fn parse_canonical_label_set",
    ] {
        assert!(
            module.contains(entry),
            "the shared decoder must export {entry}"
        );
    }
    for (arm, produces) in [
        (r#"'"' => out.push('"')"#, "a quote"),
        (r"'\\' => out.push('\\')", "a backslash"),
        (r"'/' => out.push('/')", "a solidus"),
        (r"'b' => out.push('\u{8}')", "a backspace"),
        (r"'f' => out.push('\u{c}')", "a form feed"),
        (r"'n' => out.push('\n')", "a line feed"),
        (r"'r' => out.push('\r')", "a carriage return"),
        (r"'t' => out.push('\t')", "a tab"),
    ] {
        assert!(
            module.contains(arm),
            "JSON defines eight two-character escapes; the table has no arm decoding \
             {produces} ({arm})"
        );
    }
    assert!(module.contains("'u' =>"), "the `\\uXXXX` arm");
    for test in [
        "fn every_unicode_scalar_value_survives_the_writer_then_the_reader",
        "fn the_named_escapes_decode_to_the_characters_json_defines",
        "fn the_escape_table_lists_all_eight_of_jsons_two_character_escapes",
        "fn the_boundary_neighbours_of_the_two_added_escapes_round_trip",
        "fn the_crate_defines_the_json_string_parser_exactly_once",
        "fn this_modules_source_carries_no_raw_control_bytes",
    ] {
        assert!(
            module.contains(test),
            "the shared decoder must carry {test}"
        );
    }
    // One decoder, three callers: no caller keeps a private parser.
    let out = Command::new("git")
        .arg("-C")
        .arg(repo_root())
        .args(["grep", "-l", "fn parse_json_string", "--", "crates"])
        .output()
        .expect("git grep");
    let files: Vec<String> = String::from_utf8(out.stdout)
        .expect("utf-8")
        .lines()
        .filter(|f| f.contains("/src/"))
        .map(str::to_string)
        .collect();
    assert_eq!(
        files,
        vec!["crates/pulsus-read/src/canonical_labels.rs".to_string()],
        "a caller has grown a second JSON string parser"
    );
}

/// Issue #544 AC10 — **every field source that reaches
/// `log_samples.structured_metadata`, and the serializer each one uses.**
///
/// The lowered predicate is exact for every byte string a WRITER can
/// produce, so the set of writers has to stay closed. There are two field
/// sources and two serializers, and the two serializers are pinned
/// byte-identical to each other by a test that already ships.
///
/// This is a source-text enumeration on purpose: it fails when a third
/// construction appears, which is the event that would invalidate the
/// claim, and it names the file and line it found.
#[test]
fn every_stored_metadata_field_source_uses_a_pinned_escaper() {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo_root())
        .args(["grep", "-n", "LogRow {", "--", "crates"])
        .output()
        .expect("git grep");
    let hits: Vec<(String, usize)> = String::from_utf8(out.stdout)
        .expect("utf-8")
        .lines()
        .filter_map(|l| {
            let mut parts = l.splitn(3, ':');
            let file = parts.next()?.to_string();
            let line: usize = parts.next()?.parse().ok()?;
            Some((file, line))
        })
        .collect();
    // A construction below a file's `#[cfg(test)]` module header is a
    // test helper. The header sits at column 0, so its own line is the
    // boundary.
    let mut production: Vec<(String, usize)> = Vec::new();
    for (file, line) in hits {
        // Only the crates' own sources: an integration test under
        // `tests/` builds rows for a fixture and writes no column.
        if !file.contains("/src/") {
            continue;
        }
        // The type's own declaration is not a construction of it.
        if read(&file)
            .lines()
            .nth(line - 1)
            .is_some_and(|l| l.contains("struct LogRow {"))
        {
            continue;
        }
        let body = read(&file);
        let boundary = body
            .lines()
            .position(|l| l == "#[cfg(test)]")
            .map(|i| i + 1)
            .unwrap_or(usize::MAX);
        if line < boundary {
            production.push((file, line));
        }
    }
    production.sort();
    // The FILES, not their line numbers: a doc comment added above one of
    // them must not redden a check about how many writers there are.
    let files: Vec<String> = production.iter().map(|(f, _)| f.clone()).collect();
    assert_eq!(
        files,
        vec![
            "crates/pulsus-write/src/protocols/loki_push.rs".to_string(),
            "crates/pulsus-write/src/protocols/otlp_logs.rs".to_string(),
        ],
        "the field sources that reach `log_samples.structured_metadata` are two; a third one \
         writes bytes no serializer here is pinned to. Found: {production:?}"
    );

    // What each one's `structured_metadata` field takes, read at the
    // construction rather than asserted from the call graph.
    let push = read("crates/pulsus-write/src/protocols/loki_push.rs");
    assert!(
        push.contains("    Ok(render_structured_metadata(resolved))"),
        "the push transport's field is `canonical_structured_metadata`'s answer, and that \
         function's last expression is the shared serializer"
    );
    assert!(
        push.contains("fn render_structured_metadata(resolved: Vec<(String, String)>)"),
        "the shared serializer"
    );
    let otlp = read("crates/pulsus-write/src/protocols/otlp_logs.rs");
    assert!(
        otlp.contains("structured_metadata: record_metadata,"),
        "the other transport's field is the scope's rendered metadata"
    );
    for serializer in [
        "ScopeMetadata::Shared(render_structured_metadata(scope_pairs))",
        "splice.render(level)",
    ] {
        assert!(
            otlp.contains(serializer),
            "the other transport has two serializers, one per discovery setting: {serializer}"
        );
    }
    assert!(
        otlp.contains("fn push_json_string"),
        "the splice serializer's own escaper"
    );

    // The pin that makes the two serializers ONE escaper, and the
    // enclosing `#[test]` — selecting the helper by name runs nothing and
    // reports success.
    let pin = read("crates/pulsus-write/tests/otlp_level_alloc.rs");
    assert!(
        pin.contains("fn the_splice_is_byte_identical_to_a_rebuild"),
        "the byte-identity pin between the two serializers"
    );
    assert!(
        pin.contains("fn the_otlp_shape_costs_at_most_three_extra_allocations_per_record"),
        "the ENCLOSING `#[test]`"
    );
    // The two C0 values the pin was blind to: with them absent, widening
    // the splice escaper's fast path to copy those bytes raw leaves the
    // enclosing test GREEN.
    for value in [r#"a\u{8}b"#, r#"a\u{c}b"#] {
        assert!(
            pin.contains(value),
            "the pinning corpus must carry {value}, or the escaper pin is blind to it"
        );
    }
}

/// The eight passages issue #544 moves, one row each: the file, the
/// sentence that must be GONE, and the sentence that must be PRESENT.
///
/// Three assertions per row, not one. A revision that deletes the prose
/// fails the second, and one that states the new rule absolutely — with
/// no mention of the budget fallback — fails the third, because a
/// document that says the filter compiles into the statement without
/// saying what happens past the budget is false on the fallback path.
const MOVED_PASSAGES: &[(&str, &str, &str)] = &[
    (
        "docs/schemas.md",
        "**`structured_metadata` is projected, never predicated.**",
        "**`structured_metadata` is predicated for an equality or inequality over",
    ),
    (
        "docs/architecture.md",
        "**Pipeline evaluation** in the engine: parsers (`json`, `logfmt`, `regexp`, \
         `pattern`), label filters, `line_format`/`label_format`, unwrap, and range/vector \
         aggregations. Range aggregations",
        "One label filter is the exception (issue #544)",
    ),
    (
        "docs/features.md",
        "A filter on a metadata\nlabel is evaluated in the compiled pipeline, never pushed \
         into SQL",
        "an **equality or\ninequality** over a metadata name pushes into SQL exactly (issue \
         #544)",
    ),
    (
        "docs/configuration.md",
        "the engine keyset-pages `limit × factor` rows at a time through the pipeline until \
         the true `limit` fills, the window is exhausted, or the byte scan budget is spent — \
         responses fill exactly to `limit` (no under-return) and never over-return. This is no \
         longer an oversample-and-truncate ceiling; a larger factor only sizes the first page \
         (fewer round trips on a selective pipeline, more bytes scanned per page). |",
        "**One label filter is no longer such a stage (issue #544):**",
    ),
    (
        "docs/api.md",
        "is served by fetch-until-limit keyset paging that fills exactly to `limit`. ",
        "which compiles into the statement and takes the request `LIMIT` with it, so the read \
         is one statement and no paging happens (issue #544)",
    ),
    (
        "docs/api.md",
        "engages the §2.1 fetch-until-limit keyset paging under the **sam",
        "**with the same exception (issue #544): an equality or inequality over a \
         structured-metadata name is in the statement",
    ),
    (
        "docs/api.md",
        "Server-side structured-metadata filter pushdown is a deferred optimization",
        "Server-side structured-metadata filter pushdown is no longer deferred for **equality \
         and inequality** (issue #544)",
    ),
    (
        "docs/benchmarks/logs-differential-ledger.md",
        "There is **no SM predicate pushdown**",
        "**SM predicate pushdown exists for the equality\n  and inequality forms since issue \
         #544**",
    ),
];

/// The budget caveat every replacement must carry: the rule is true
/// *unless* the rendered fragments exceed the budget, and a document that
/// states the first half without the second is false on the fallback
/// path.
const BUDGET_CAVEAT: &str = "MAX_METADATA_FRAGMENT_BYTES";

/// Issue #544 AC13 — **eight passages, three assertions each.**
#[test]
fn the_design_documents_state_the_metadata_predicate() {
    let mut problems: Vec<String> = Vec::new();
    for (file, withdrawn, replacement) in MOVED_PASSAGES {
        let text = read(file);
        if text.contains(withdrawn) {
            problems.push(format!(
                "{file}: the withdrawn sentence is still there: {withdrawn:?}"
            ));
        }
        if !text.contains(replacement) {
            problems.push(format!(
                "{file}: the replacement is absent: {replacement:?}"
            ));
            continue;
        }
        // The caveat must sit in the same PARAGRAPH as the replacement,
        // not merely somewhere in the file: a caveat three sections away
        // does not qualify the sentence a reader is reading.
        let at = text.find(replacement).expect("checked above");
        let para_start = text[..at].rfind("\n\n").map(|i| i + 2).unwrap_or(0);
        let para_end = text[at..]
            .find("\n\n")
            .map(|i| at + i)
            .unwrap_or(text.len());
        if !text[para_start..para_end].contains(BUDGET_CAVEAT) {
            problems.push(format!(
                "{file}: the replacement states the new rule absolutely — its paragraph does \
                 not name {BUDGET_CAVEAT}, so it is false on the fallback path"
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "{} of {} passages:\n  {}",
        problems.len(),
        MOVED_PASSAGES.len(),
        problems.join("\n  ")
    );
}
