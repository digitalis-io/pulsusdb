//! Hermetic anti-rot guard for `docs/production-readiness.md` (#177).
//!
//! Parses every evidence-citation token in the checklist and asserts each
//! resolves against the tree, so a renamed/removed test suite, code file, test
//! function, CI step, or doc section turns this test RED. No live deps — this
//! rides the `ci` job's workspace `Test` step. The `workspace_root()` idiom is
//! borrowed from `route_inventory.rs`.
//!
//! Citation grammar (tokens live inside single backticks unless noted):
//! - integration suite: `cargo test -p <crate> --test <suite>` → `crates/<crate>/tests/<suite>.rs`.
//! - lib unit test / fn ref: `crates/<crate>/src/<file>.rs::<fn>` → file exists AND contains `fn <fn>`.
//! - plain code ref: `crates/<crate>/src/<file>.rs` (optional `:LINE`, ignored) → file exists.
//! - CI step: `ci.yml: "<name>"` (scanned from raw text) → name follows a `name:` key in ci.yml.
//! - doc section: `<doc>.md §N[.M[.K]]` → `docs/<doc>.md` has a heading line for that exact number.
//!
//! Placeholder spans (containing `<`/`>`) are grammar documentation, not
//! citations, and are skipped.

use std::fs;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root resolves")
}

fn read_or_panic(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn doc_text() -> String {
    read_or_panic(&workspace_root().join("docs/production-readiness.md"))
}

/// Every non-empty single-backtick inline-code span, line by line.
fn backtick_spans(text: &str) -> Vec<String> {
    let mut spans = Vec::new();
    for line in text.lines() {
        let mut rest = line;
        while let Some(start) = rest.find('`') {
            let after = &rest[start + 1..];
            let Some(end) = after.find('`') else {
                break;
            };
            let span = &after[..end];
            if !span.is_empty() {
                spans.push(span.to_string());
            }
            rest = &after[end + 1..];
        }
    }
    spans
}

/// First whitespace-delimited token appearing after `key` in `s`.
fn token_after<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    let idx = s.find(key)?;
    s[idx + key.len()..].split_whitespace().next()
}

/// Strip a trailing `:LINE` (all-digit) suffix from a code ref, if present.
fn strip_line_suffix(span: &str) -> &str {
    if let Some((path, tail)) = span.rsplit_once(':')
        && !tail.is_empty()
        && tail.chars().all(|c| c.is_ascii_digit())
    {
        return path;
    }
    span
}

/// Parse a `<doc>.md §N[.M[.K]]` token into (`<doc>.md`, dotted section number).
fn parse_doc_section(span: &str) -> Option<(String, String)> {
    let (left, right) = span.split_once(" §")?;
    if !left.ends_with(".md") {
        return None;
    }
    let sec: String = right
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    if sec.is_empty() {
        return None;
    }
    Some((left.to_string(), sec))
}

/// Hand-rolled equivalent of `^#{1,6}\s+SEC(?:[^0-9.]|\.\s|\.$)` with `SEC` the
/// literal (dot-escaped) section number. The consuming boundary after the number
/// requires one of: a non-`[0-9.]` char, `.` then whitespace, or `.` at line end
/// — so `§2.6` matches `### 2.6 Drilldown` but never `#### 2.6.1 …`.
fn heading_matches_section(line: &str, sec: &str) -> bool {
    let line = line.trim_end();
    let t = line.trim_start();
    let hashes = t.chars().take_while(|c| *c == '#').count();
    if hashes == 0 || hashes > 6 {
        return false;
    }
    let after_hash = &t[hashes..];
    if !after_hash.starts_with(char::is_whitespace) {
        return false;
    }
    let rest = after_hash.trim_start();
    let Some(after) = rest.strip_prefix(sec) else {
        return false;
    };
    let mut chars = after.chars();
    match chars.next() {
        None => false,
        Some(c) if c != '.' && !c.is_ascii_digit() => true,
        Some('.') => match chars.next() {
            None => true,
            Some(next) if next.is_whitespace() => true,
            _ => false,
        },
        _ => false,
    }
}

/// Integer of a `## N. Title` top-level numbered heading, or `None` if the line
/// is not one. Uses the same heading grammar as `heading_matches_section`
/// (top-level = exactly two `#`, then the number, then a `.`+whitespace/end
/// boundary), restricted to top-level so nested `### N.M` headings are ignored.
fn top_level_section_number(line: &str) -> Option<u32> {
    let line = line.trim_end();
    let t = line.trim_start();
    let hashes = t.chars().take_while(|c| *c == '#').count();
    if hashes != 2 {
        return None;
    }
    let after_hash = &t[hashes..];
    if !after_hash.starts_with(char::is_whitespace) {
        return None;
    }
    let rest = after_hash.trim_start();
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    let after = &rest[digits.len()..];
    // Same boundary as `heading_matches_section`, minus the `.<digit>` nested case
    // (which `take_while` already consumed): `.` then whitespace, or `.` at line end.
    let mut chars = after.chars();
    match chars.next() {
        Some('.') => match chars.next() {
            None => {}
            Some(next) if next.is_whitespace() => {}
            _ => return None,
        },
        _ => return None,
    }
    digits.parse().ok()
}

/// All `ci.yml: "<name>"` citations scanned from the raw doc text.
fn ci_step_citations(text: &str) -> Vec<String> {
    let needle = "ci.yml: \"";
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(pos) = rest.find(needle) {
        let after = &rest[pos + needle.len()..];
        let Some(end) = after.find('"') else {
            break;
        };
        let name = &after[..end];
        if !name.contains('<') && !name.contains('>') {
            out.push(name.to_string());
        }
        rest = &after[end + 1..];
    }
    out
}

/// True if `name` appears as a `name:` step value anywhere in `ci`.
fn ci_has_step(ci: &str, name: &str) -> bool {
    ci.lines().any(|line| {
        line.find("name:")
            .map(|idx| line[idx + "name:".len()..].trim().trim_matches('"') == name)
            .unwrap_or(false)
    })
}

#[test]
fn every_evidence_pointer_in_the_readiness_doc_resolves() {
    let root = workspace_root();
    let text = doc_text();

    let mut test_citations = 0usize;
    let mut resolved_any = false;

    for span in backtick_spans(&text) {
        if span.contains('<') || span.contains('>') {
            // Grammar-documentation placeholder, not a citation.
            continue;
        }

        if let Some(rest) = span.strip_prefix("cargo test ") {
            if !rest.contains("--test ") {
                continue;
            }
            let crate_name =
                token_after(rest, "-p ").unwrap_or_else(|| panic!("no `-p <crate>` in `{span}`"));
            let suite = token_after(rest, "--test ")
                .unwrap_or_else(|| panic!("no `--test <suite>` in `{span}`"));
            let path = root.join(format!("crates/{crate_name}/tests/{suite}.rs"));
            assert!(
                path.exists(),
                "integration suite cited in the doc is missing: {}",
                path.display()
            );
            test_citations += 1;
            resolved_any = true;
        } else if span.starts_with("crates/") && span.contains(".rs") {
            if let Some((file, func)) = span.split_once(".rs::") {
                let path = root.join(format!("{file}.rs"));
                let src = read_or_panic(&path);
                assert!(
                    src.contains(&format!("fn {func}")),
                    "fn {func} not found in cited file {}",
                    path.display()
                );
                test_citations += 1;
                resolved_any = true;
            } else {
                let path = root.join(strip_line_suffix(&span));
                assert!(
                    path.exists(),
                    "code ref cited in the doc is missing: {}",
                    path.display()
                );
                resolved_any = true;
            }
        } else if let Some((doc, sec)) = parse_doc_section(&span) {
            let path = root.join(format!("docs/{doc}"));
            let src = read_or_panic(&path);
            assert!(
                src.lines().any(|line| heading_matches_section(line, &sec)),
                "doc section §{sec} not found as a heading in {}",
                path.display()
            );
            resolved_any = true;
        }
    }

    let ci = read_or_panic(&root.join(".github/workflows/ci.yml"));
    for name in ci_step_citations(&text) {
        assert!(
            ci_has_step(&ci, &name),
            "CI step cited in the doc is missing from ci.yml: {name:?}"
        );
        resolved_any = true;
    }

    assert!(
        resolved_any,
        "no evidence citations parsed — doc grammar may have drifted"
    );
    assert!(
        test_citations >= 8,
        "expected at least 8 test-citations, parsed {test_citations} — \
         an under-cited doc must fail loudly"
    );
}

#[test]
fn the_readiness_doc_has_all_nine_sections() {
    use std::collections::BTreeSet;

    let text = doc_text();
    // Collect every top-level numbered heading (`## N. Title`) as its integer.
    let numbers: Vec<u32> = text.lines().filter_map(top_level_section_number).collect();
    let sections: BTreeSet<u32> = numbers.iter().copied().collect();
    let expected: BTreeSet<u32> = (1..=9).collect();
    // Exact set-equality catches ANY spurious (§10, §11, …) or missing top-level
    // section, not just a spurious §10.
    assert_eq!(
        sections, expected,
        "production-readiness.md top-level numbered sections must be exactly {{1..=9}}, \
         got {sections:?}"
    );
    // A set collapses duplicates; the length check reds on a repeated `## N.`.
    assert_eq!(
        numbers.len(),
        sections.len(),
        "production-readiness.md has a duplicate top-level section number: {numbers:?}"
    );
}

#[test]
fn doc_section_predicate_matches_parent_but_not_nested_child() {
    // Real repo heading styles (api.md / configuration.md).
    assert!(heading_matches_section("### 2.6 Drilldown (M7)", "2.6"));
    assert!(heading_matches_section("## 2. ClickHouse connection", "2"));
    assert!(heading_matches_section("### 3.6 Retention", "3.6"));

    // Nested child must NOT satisfy the parent citation (the round-2 finding).
    assert!(!heading_matches_section("#### 2.6.1 Volume", "2.6"));
    assert!(!heading_matches_section("### 2.1 Tables", "2"));
    // `§1` must not resolve to `## 10.`.
    assert!(!heading_matches_section("## 10. Quickstart", "1"));
    // Bare number with no title does not resolve.
    assert!(!heading_matches_section("## 2", "2"));
}

#[test]
fn fabricated_doc_section_does_not_resolve() {
    let root = workspace_root();
    let src = read_or_panic(&root.join("docs/api.md"));
    assert!(
        !src.lines().any(|line| heading_matches_section(line, "99")),
        "a fabricated §99 citation must never resolve"
    );
}
