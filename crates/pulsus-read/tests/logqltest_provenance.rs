//! Issue #240: the provenance / surface gates behind the LogQL error
//! envelope change (shaped on `traces_alloc_audit.rs`'s source-census
//! idiom). Four checks, all fail-closed:
//!
//! - **A/B** — the `msg_exact:` corpus values and the committed
//!   `pulsus-240-bodies` capture table in `logqltest/PROVENANCE.md` are
//!   the SAME set, both directions (a body nobody captured cannot be
//!   pinned; a captured body cannot silently drop out of the corpus).
//!   The table carries exactly the rows B1–B3: the fourth candidate
//!   (B4, the `variants` unwrap-arity body) is provenance-BLOCKED —
//!   the reference nil-panics on that exact query, and a capture of a
//!   different, non-`variants` query must not be substituted (issue
//!   #240 AC10; see PROVENANCE.md §#240).
//! - **C** — `ReadError::PipelineInvalid` construction census over
//!   `src/logql/*.rs` production regions (every construction is the
//!   canonical multi-line form the sweep counted, with pinned per-file
//!   totals), plus the §3.4 anchoring guard: the interpolating anchored
//!   template `^(?:{` exists at exactly two committed sites.
//! - **D** — `escape.rs`'s constrained surface: CONSTRAIN (D1 no
//!   `impl`/`trait`/`extern`/`macro_rules!`/`include!`; D2 attribute
//!   allowlist; D3 top-level items ∈ {`use`, `fn`, `mod`}), THEN
//!   ENUMERATE (D4 the `ESCAPE_ITEMS` table by visibility and full
//!   signature, both directions; D5 no re-exports; D6 test-region
//!   kinds), plus D7's exemption call-site table.
//!
//! Layering (so no check is mistaken for another): Check D bounds
//! `escape.rs`'s **surface**; cross-module reach of the private raw
//! escapers is **rustc**'s job (E0603/E0624, measured on this layout);
//! the `_checked` **bodies** are the mutation tests' job (AC7(e)).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

fn manifest_path(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(rel)
}

fn read(rel: &str) -> String {
    let p = manifest_path(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {p:?}: {e}"))
}

// ---------------------------------------------------------------------
// Checks A + B — capture table <-> corpus `msg_exact:` values.
// ---------------------------------------------------------------------

/// One row of the `pulsus-240-bodies` fenced block:
/// `| id | corpus-file | source | value |`.
struct BodyRow {
    id: String,
    file: String,
    value: String,
}

fn provenance_body_rows() -> Vec<BodyRow> {
    let text = read("tests/logqltest/PROVENANCE.md");
    let start = text
        .find("```pulsus-240-bodies")
        .expect("PROVENANCE.md must carry the pulsus-240-bodies fenced block");
    let block = &text[start..];
    let end = block[3..].find("```").expect("unterminated fenced block") + 3;
    let mut rows = Vec::new();
    for line in block[..end].lines().skip(1) {
        let line = line.trim();
        if !line.starts_with('|') || line.starts_with("| id") || line.starts_with("|--") {
            continue;
        }
        let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
        assert_eq!(cells.len(), 4, "malformed pulsus-240-bodies row: {line:?}");
        assert!(
            cells[2].starts_with("wave0 ") || cells[2].starts_with("probe:https://"),
            "row {} source must be `wave0 <date>` or `probe:<immutable URL>`: {:?}",
            cells[0],
            cells[2]
        );
        rows.push(BodyRow {
            id: cells[0].to_string(),
            file: cells[1].to_string(),
            value: cells[3].to_string(),
        });
    }
    // Exactly B1–B3. B4 is provenance-BLOCKED (issue #240 AC10): the
    // reference nil-panics on the variants-form query, so no applicable
    // capture exists, and a different query's capture must not be
    // substituted. Re-adding a B4 row is the review event — it requires
    // an applicable v3.7.4 capture (or immutable probe URL) first.
    let ids: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(
        ids,
        ["B1", "B2", "B3"],
        "pulsus-240-bodies must carry exactly the rows B1-B3 (B4 is blocked, \
         see PROVENANCE.md, issue #240 section)"
    );
    rows
}

fn corpus_msg_exact_values() -> Vec<(String, usize, String)> {
    let dir = manifest_path("tests/logqltest/corpus");
    let mut out = Vec::new();
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("corpus dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "test"))
        .collect();
    files.sort();
    for f in &files {
        let text = std::fs::read_to_string(f).expect("read corpus file");
        for (i, line) in text.lines().enumerate() {
            if let Some(v) = line.trim().strip_prefix("msg_exact:") {
                out.push((
                    f.file_name().unwrap().to_string_lossy().to_string(),
                    i + 1,
                    v.trim().to_string(),
                ));
            }
        }
    }
    out
}

#[test]
fn check_a_and_b_capture_rows_and_msg_exact_values_are_the_same_set() {
    let rows = provenance_body_rows();
    let corpus = corpus_msg_exact_values();
    let mut errors = String::new();
    // A (forward): every row's value appears verbatim as a `msg_exact:`
    // value in the corpus file the row names.
    for row in &rows {
        if !corpus
            .iter()
            .any(|(file, _, v)| *file == row.file && *v == row.value)
        {
            let _ = writeln!(
                errors,
                "check A: row {} names no matching `msg_exact:` in {} (value {:?})",
                row.id, row.file, row.value
            );
        }
    }
    // B (reverse): every corpus `msg_exact:` value appears as some row.
    for (file, line, v) in &corpus {
        if !rows.iter().any(|r| r.value == *v && r.file == *file) {
            let _ = writeln!(
                errors,
                "check B: {file}:{line} pins a `msg_exact:` value with no \
                 pulsus-240-bodies capture row: {v:?}"
            );
        }
    }
    assert!(errors.is_empty(), "{errors}");
}

// ---------------------------------------------------------------------
// Check C — construction census + the §3.4 anchoring guard.
// ---------------------------------------------------------------------

/// Blanks `//`-family comments and string literals. A `/*` that closes
/// on the same line is allowed (one committed occurrence:
/// `plan.rs`'s `/*force_client=*/` inline marker); an unterminated one
/// FAILS — this scanner is line-oriented by design.
fn blank_comments_and_strings(file: &str, src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    for (i, line) in src.lines().enumerate() {
        let mut blanked = String::with_capacity(line.len());
        let mut chars = line.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '/' if chars.peek() == Some(&'/') => break, // line comment
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    // must close on the SAME line
                    let mut closed = false;
                    while let Some(d) = chars.next() {
                        if d == '*' && chars.peek() == Some(&'/') {
                            chars.next();
                            closed = true;
                            break;
                        }
                    }
                    assert!(
                        closed,
                        "{file}:{}: unterminated `/*` — this census is line-oriented; \
                         close the block comment on the same line or restructure",
                        i + 1
                    );
                }
                '"' => {
                    blanked.push('_');
                    while let Some(d) = chars.next() {
                        if d == '\\' {
                            chars.next();
                        } else if d == '"' {
                            break;
                        }
                    }
                }
                other => blanked.push(other),
            }
        }
        out.push_str(&blanked);
        out.push('\n');
    }
    out
}

fn logql_source_files() -> Vec<(String, String)> {
    let dir = manifest_path("src/logql");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("logql src dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .collect();
    files.sort();
    assert!(files.len() >= 10, "logql sources: {files:?}");
    files
        .into_iter()
        .map(|p| {
            let name = p.file_name().unwrap().to_string_lossy().to_string();
            let text = std::fs::read_to_string(&p).expect("read");
            (name, text)
        })
        .collect()
}

#[test]
fn check_c_pipeline_invalid_constructions_are_canonical_and_counted() {
    // Per-file production construction totals, pinned (issue #240 §2 A1:
    // 31 production sites, by file — 35 after issue #236, which added
    // `fold_off_grid` (Part B's off-grid internal-invariant breach) and
    // the three instant-window narrowing refusals Part D introduced at
    // `run_metric_client`, `run_client_agg_rows_folded` and
    // `VariantsAggState::new` — 37 after issue #276, which added
    // `label_replace`'s two rejections in `plan.rs`: the wrapped-form
    // regex error in `LabelReplaceSpec::compile` and the scalar-operand
    // rejection in `fold_plan_ops`).
    //
    // Issue #299 split `exec.rs` into ten flat modules, so `exec.rs`'s 20
    // are now spread over five of them. The TOTAL is unchanged at 20, and
    // #240's sweep numbers stand: this is a redistribution, not a new
    // construction site.
    let expected: BTreeMap<&str, usize> = BTreeMap::from([
        // Issue #272 moved `build_metric_node` — and with it ONE
        // `PipelineInvalid` construction, the `Vector`-over-`Literal`
        // rejection — into `plan_legacy_descent.rs`. Issue #293 converted
        // that walk and deleted the module, moving the construction back:
        // 13 + 1 -> 14, total unchanged both times.
        ("plan.rs", 16),
        ("exec.rs", 12),
        ("client_agg.rs", 1),
        ("fold.rs", 1),
        ("post_agg.rs", 5),
        ("variants.rs", 1),
        ("error.rs", 1),
    ]);
    let mut errors = String::new();
    let mut anchored_hits: Vec<String> = Vec::new();
    for (file, text) in logql_source_files() {
        // The §3.4 anchoring guard runs BEFORE blanking: the escaper and
        // the validator legitimately hold the template inside strings.
        for (i, line) in text.lines().enumerate() {
            if line.contains("^(?:{") {
                anchored_hits.push(format!("{file}:{}", i + 1));
            }
        }
        // Exactly one `mod tests {`, trailing.
        let test_marks: Vec<usize> = text.match_indices("mod tests {").map(|(i, _)| i).collect();
        assert!(
            test_marks.len() <= 1,
            "{file}: more than one `mod tests {{`"
        );
        let truncated = match test_marks.first() {
            Some(&idx) => {
                for tail_line in text[idx..].lines().skip(1) {
                    let first = tail_line.chars().next();
                    assert!(
                        !matches!(first, Some(c) if c.is_alphanumeric() || c == '#'),
                        "{file}: `mod tests` must be the trailing region                          (found a column-0 item after it: {tail_line:?})"
                    );
                }
                &text[..idx]
            }
            None => &text[..],
        };
        let blanked = blank_comments_and_strings(&file, truncated);
        for (i, line) in blanked.lines().enumerate() {
            let mut rest = line;
            while let Some(pos) = rest.find("ReadError::PipelineInvalid") {
                let after = &rest[pos + "ReadError::PipelineInvalid".len()..];
                // Canonical construction form: ` {` ends the line (the
                // A1 sweep's `ReadError::PipelineInvalid \{$` shape).
                if after.trim() != "{" {
                    let _ = writeln!(
                        errors,
                        "{file}:{}: non-canonical `ReadError::PipelineInvalid` occurrence in a \
                         production region — construct it as the multi-line struct literal \
                         (`ReadError::PipelineInvalid {{` ending the line) so the census can \
                         count it, or move it below `mod tests`",
                        i + 1
                    );
                }
                rest = after;
            }
        }
        let count = blanked
            .lines()
            .filter(|l| l.trim_end().ends_with("ReadError::PipelineInvalid {"))
            .count();
        let want = expected.get(file.as_str()).copied().unwrap_or(0);
        if count != want {
            let _ = writeln!(
                errors,
                "{file}: {count} production `ReadError::PipelineInvalid` constructions, \
                 pinned {want} — re-derive the census and update BOTH this pin and the \
                 issue-#240 sweep numbers"
            );
        }
    }
    // §3.4: the interpolating anchored template may exist at exactly the
    // three committed sites — the escaper, the validator, and (issue
    // #276) `plan.rs`'s `LabelReplaceSpec::compile`. The third is
    // deliberately OUTSIDE the checked-escape seam: `label_replace`'s
    // pattern never reaches SQL (the transform runs over the evaluated
    // result), the anchored text it compiles is the #317 RE2→Rust
    // REWRITE of the user's pattern (not the bytes the SQL seam
    // validates), and its compile error must surface the WRAPPED form —
    // the #240 asymmetry — which the `bad_regex`-routed seams must never
    // produce.
    anchored_hits.sort();
    let allowed = ["escape.rs", "pipeline.rs", "plan.rs"];
    if anchored_hits.len() != allowed.len()
        || !allowed
            .iter()
            .all(|f| anchored_hits.iter().filter(|h| h.starts_with(f)).count() == 1)
    {
        let _ = writeln!(
            errors,
            "anchoring guard: `^(?:{{` must occur at exactly three sites (escape.rs's \
             escaper, pipeline.rs's validator, and plan.rs's `LabelReplaceSpec::compile`), \
             found {anchored_hits:?} — every OTHER site must build the anchored form \
             through `escape::ch_regex_anchored_checked`, never by hand"
        );
    }
    assert!(errors.is_empty(), "{errors}");
}

// ---------------------------------------------------------------------
// Check D — escape.rs: CONSTRAIN, THEN ENUMERATE. Fail-closed.
// ---------------------------------------------------------------------

/// Committed verbatim; a diff to this array is the review event. EVERY
/// top-level item, private ones included.
const ESCAPE_ITEMS: &[&str] = &[
    "use super::pipeline::PipelineError",
    "pub fn ch_string(s: &str) -> String",
    "pub fn ch_ident(s: &str) -> String",
    "fn ch_regex_anchored(pat: &str) -> String",
    "fn ch_regex_unanchored(pat: &str) -> String",
    "pub(crate) fn ch_regex_anchored_checked(pat: &str) -> Result<String, PipelineError>",
    "pub(crate) fn ch_regex_unanchored_checked(pat: &str) -> Result<String, PipelineError>",
    "pub(crate) fn ch_regex_anchored_promql_re2(_authority: crate::metrics::PromqlRe2Fallback, pat: &str) -> String",
    "mod tests",
];

const D1_RULE: &str = "escape.rs is a leaf escaping module. These constructs can expose an item \
with no `pub` on its own line, or inject an item this text does not contain — measured: a \
foreign trait implemented here for a type another module owns is callable from `logql/` with \
zero `pub` tokens in this file. If you have a genuine need for one, this gate is the \
conversation. (Check D bounds escape.rs's SURFACE; cross-module reach of the private escapers \
is rustc's job, and the `_checked` bodies are the AC7(e) mutation tests' job.)";

#[test]
fn check_d_escape_rs_surface_is_allowlisted_and_fail_closed() {
    let file = "escape.rs";
    // NOT truncated at `mod tests` — a `pub` item in a `#[cfg(test)]`
    // module is reachable from another module's tests in a test build.
    let text = read("src/logql/escape.rs");
    let mut errors = String::new();

    // --- D1: no visibility-hiding or item-injecting constructs.
    let blanked_full = blank_comments_and_strings(file, &text);
    for (i, line) in blanked_full.lines().enumerate() {
        // Strip any leading visibility/unsafety so `pub(crate) unsafe
        // impl …` cannot hide the keyword, then match it as the first
        // delimited token of the line.
        let mut head = line.trim_start();
        loop {
            if let Some(rest) = head.strip_prefix("pub(") {
                head = rest
                    .split_once(')')
                    .map(|(_, r)| r.trim_start())
                    .unwrap_or("");
            } else if let Some(rest) = head.strip_prefix("pub ") {
                head = rest.trim_start();
            } else if let Some(rest) = head.strip_prefix("unsafe ") {
                head = rest.trim_start();
            } else {
                break;
            }
        }
        for kw in ["impl", "trait", "extern"] {
            let hit = head == kw
                || head.starts_with(&format!("{kw} "))
                || head.starts_with(&format!("{kw}<"))
                || head.starts_with(&format!("{kw}{{"));
            if hit {
                let _ = writeln!(
                    errors,
                    "D1 {file}:{}: `{kw}` is forbidden. {D1_RULE}",
                    i + 1
                );
            }
        }
        for token in ["macro_rules!", "include!"] {
            if line.contains(token) {
                let _ = writeln!(
                    errors,
                    "D1 {file}:{}: `{token}` is forbidden. {D1_RULE}",
                    i + 1
                );
            }
        }
    }

    // --- D2: attribute allowlist (excludes derive/macro_export/no_mangle/
    // export_name/path and every proc-macro attribute WITHOUT enumerating
    // them).
    for (i, line) in text.lines().enumerate() {
        let t = line.trim();
        if (t.starts_with("#[") || t.starts_with("#![")) && t != "#[cfg(test)]" && t != "#[test]" {
            let _ = writeln!(
                errors,
                "D2 {file}:{}: attribute {t:?} is not on the allowlist \
                     (#[cfg(test)] / #[test]). {D1_RULE}",
                i + 1
            );
        }
    }

    // --- D3/D4/D5/D6: item scan over the blanked text, bracket-aware.
    #[derive(Debug)]
    struct Item {
        line: usize,
        normalized: String, // "<vis> <kind> <name+signature>"
        kind: &'static str,
        vis: String,
        in_tests: bool,
    }
    let mut items: Vec<Item> = Vec::new();
    let mut depth: i64 = 0;
    let mut pending: Option<(usize, String)> = None; // multi-line signature
    let mut tests_depth: Option<i64> = None; // depth at which `mod tests` opened
    for (i, raw) in blanked_full.lines().enumerate() {
        let t = raw.trim();
        let depth_before = depth;
        for c in raw.chars() {
            match c {
                '{' | '(' | '[' => depth += 1,
                '}' | ')' | ']' => depth -= 1,
                _ => {}
            }
        }
        if let Some(td) = tests_depth
            && depth < td
        {
            tests_depth = None;
        }
        let in_tests = tests_depth.is_some();
        let item_scope_depth = if in_tests { 1 } else { 0 };
        if pending.is_none() {
            if depth_before == item_scope_depth
                && !t.is_empty()
                && !t.starts_with("#[")
                && !t.starts_with('}')
            {
                pending = Some((i + 1, String::new()));
            } else {
                continue;
            }
        }
        if let Some((start, sig)) = &mut pending {
            if !sig.is_empty() {
                sig.push(' ');
            }
            sig.push_str(t);
            // The item's signature ends where its body opens (`{`), the
            // declaration ends (`;`), or a one-line item closes its own
            // body (`… { … }` returning to the scope depth).
            let done = t.ends_with('{')
                || t.ends_with(';')
                || (t.ends_with('}') && depth == item_scope_depth);
            if done {
                let start = *start;
                let mut normalized = sig.trim_end_matches(['{', ';']).trim().to_string();
                // Collapse whitespace and rustfmt wrapping artifacts
                // (`( `, ` )`, trailing `,` before `)`) so a re-wrapped
                // signature normalizes identically — AC7(d)(vi).
                normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
                normalized = normalized
                    .replace("( ", "(")
                    .replace(", )", ")")
                    .replace(" )", ")");
                let after_vis = normalized
                    .strip_prefix("pub(crate) ")
                    .or_else(|| normalized.strip_prefix("pub(super) "))
                    .or_else(|| normalized.strip_prefix("pub "))
                    .unwrap_or(&normalized);
                let vis = normalized[..normalized.len() - after_vis.len()]
                    .trim()
                    .to_string();
                let kind = if after_vis.starts_with("use ") || after_vis == "use" {
                    "use"
                } else if after_vis.starts_with("fn ") {
                    "fn"
                } else if after_vis.starts_with("mod ") {
                    "mod"
                } else {
                    "other"
                };
                items.push(Item {
                    line: start,
                    normalized,
                    kind,
                    vis,
                    in_tests,
                });
                // Only a mod whose body is still OPEN starts a test
                // region (a one-line `mod x { … }` has already closed).
                if kind == "mod" && !in_tests && depth == item_scope_depth + 1 {
                    tests_depth = Some(depth);
                }
                pending = None;
            }
        }
    }

    let mut top_level: Vec<&Item> = Vec::new();
    for item in &items {
        if item.in_tests {
            // D6: the test region holds only visibility-free `fn`s (and
            // plain `use` imports — the pre-existing `use super::*;`;
            // a type/impl/const would need reclassifying here first).
            if !(item.kind == "fn" || item.kind == "use") || !item.vis.is_empty() {
                let _ = writeln!(
                    errors,
                    "D6 {file}:{}: test-region item must be a visibility-free `fn` \
                     (or a plain `use`): {:?}",
                    item.line, item.normalized
                );
            }
            continue;
        }
        // D3: top-level kind allowlist.
        if !matches!(item.kind, "use" | "fn" | "mod") {
            let _ = writeln!(
                errors,
                "D3 {file}:{}: top-level item kind must be use/fn/mod: {:?}. {D1_RULE}",
                item.line, item.normalized
            );
            continue;
        }
        // D5: no re-exports.
        if item.kind == "use" && !item.vis.is_empty() {
            let _ = writeln!(
                errors,
                "D5 {file}:{}: re-export (`{} use`) is forbidden: {:?}",
                item.line, item.vis, item.normalized
            );
        }
        top_level.push(item);
    }

    // D4: item table equality, both directions, visibility + signature.
    let found: Vec<&str> = top_level.iter().map(|i| i.normalized.as_str()).collect();
    if found != ESCAPE_ITEMS {
        let _ = writeln!(
            errors,
            "D4 {file}: top-level items do not match ESCAPE_ITEMS.\n  found:\n    {}\n  \
             pinned:\n    {}\n  (private items are in the table too — an escaper silently \
             gaining `pub`, or a signature losing its `Result`, fails here)",
            found.join("\n    "),
            ESCAPE_ITEMS.join("\n    ")
        );
    }

    assert!(errors.is_empty(), "{errors}");
}

/// D7 (secondary; D1–D6 cannot see other files): the exemption
/// call-site table. One entry since issue #282 retired TraceQL's
/// placeholder token — `traces/filter.rs` renders through
/// `ch_regex_anchored_checked` and holds no capability at all.
///
/// Issue #315 moved the entry from `metrics/sql.rs` to
/// `metrics/series_where.rs`. The replaced row pinned `sql.rs` at
/// `(4, 2)`: 4 production spellings — the `Re`/`Nre` arms of its two
/// predicate renderers, `matcher_predicate` and `metric_name_predicate` —
/// plus 2 in its tests. This table pins the single production call inside
/// the leaf's `anchored_re2_literal`; the test sites are gone because
/// tests now obtain the rendering through the `_for_test` seam. A rise,
/// or a second file appearing, is still the review event.
#[test]
fn check_d7_exemption_call_sites_match_the_committed_table() {
    let cases = [(
        "src/metrics/series_where.rs",
        "ch_regex_anchored_promql_re2(",
        1,
        0,
    )];
    // The builders' own file must hold NO escaper spelling. This is a
    // textual CENSUS, deliberately: it catches drift — the call pasted
    // back — never capability, since a `use … as` alias evades any grep.
    // The capability claim is rustc's (issue #315 review round 2): the
    // escaper's token has a private field and private `new` inside
    // series_where.rs, so the aliased call this census cannot see fails
    // to compile — `E0624` — from sql.rs and every other module alike
    // (series_where.rs's module doc carries the measured battery).
    assert_eq!(
        read("src/metrics/sql.rs")
            .matches("ch_regex_anchored_promql_re2(")
            .count(),
        0,
        "src/metrics/sql.rs must reach the escaper only through \
         series_where.rs's sealed renderer"
    );
    for (rel, needle, want_prod, want_test) in cases {
        let text = read(rel);
        let split = text.find("mod tests {").unwrap_or(text.len());
        let count_in = |s: &str| {
            s.lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .map(|l| l.matches(needle).count())
                .sum::<usize>()
        };
        // Import lines never match: the needle includes the call paren.
        let prod: usize = count_in(&text[..split]);
        let test = count_in(&text[split..]);
        assert_eq!(
            (prod, test),
            (want_prod, want_test),
            "{rel}: {needle} production/test call sites drifted from the committed table"
        );
    }
}
