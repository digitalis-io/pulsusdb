//! Issue #240: the provenance / surface gates behind the LogQL error
//! envelope change (shaped on `traces_alloc_audit.rs`'s source-census
//! idiom). Every check here is fail-closed:
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
//! - **F** — a `corpus-counts: none` region states no count the corpus
//!   derives. Issue #248 corrected one stale copy per round for three
//!   rounds without the class ever failing a test; this is the test.
//!
//! Layering (so no check is mistaken for another): Check D bounds
//! `escape.rs`'s **surface**; cross-module reach of the private raw
//! escapers is **rustc**'s job (E0603/E0624, measured on this layout);
//! the `_checked` **bodies** are the mutation tests' job (AC7(e)).

use std::collections::{BTreeMap, BTreeSet};
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
        // Issue #344's grammar half added TWO: the range-aggregation
        // grouping refusal in `metric_plan` and its `variants(...)`-arm
        // twin, both built from one constant so they could not drift.
        // 16 -> 18. Its EXECUTION half takes both back out — grouped
        // range aggregations now run — and adds none: 18 -> 16, the
        // pre-#344 figure, with the constant deleted. #240's sweep
        // numbers stand across both moves, since neither refusal carried
        // a regex or reference-verbatim text.
        // Issue #343 is net ZERO here, and the zero is the finding. The
        // two interim `offset is parsed but not yet evaluated` refusals
        // were replaced by the planner's window shift, and an earlier
        // draft added one back — a `variants(...)` refusal for a variant
        // whose offset differed from the common range's. Probing the
        // pinned v3.7.4 container refuted it: the reference PLANS that
        // shape (200, the shifted data where the common window covers it,
        // empty where it does not), so refusing it was a divergence
        // dressed as safety. Both out, none in: 18 stands, and so do
        // #240's sweep numbers.
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
    // four committed sites — the escaper, the byte-identity replica in
    // the escaper's OWN tests (issue #331 fix round 1: the corpus-wide
    // crossing that pins `ch_regex_anchored`'s Verbatim output against
    // the pre-#331 construction has to spell that construction, and it
    // lives beside the module-private escaper because nothing outside
    // the module can call it), the validator, and (issue #276)
    // `plan.rs`'s `LabelReplaceSpec::compile`. The last is deliberately
    // OUTSIDE the checked-escape seam: `label_replace`'s pattern never
    // reaches SQL (the transform runs over the evaluated result), the
    // anchored text it compiles is the #317 RE2→Rust REWRITE of the
    // user's pattern (not the bytes the SQL seam validates), and its
    // compile error must surface the WRAPPED form — the #240 asymmetry
    // — which the `bad_regex`-routed seams must never produce.
    anchored_hits.sort();
    let allowed: &[(&str, usize)] = &[("escape.rs", 2), ("pipeline.rs", 1), ("plan.rs", 1)];
    let total: usize = allowed.iter().map(|(_, n)| n).sum();
    if anchored_hits.len() != total
        || !allowed
            .iter()
            .all(|(f, n)| anchored_hits.iter().filter(|h| h.starts_with(f)).count() == *n)
    {
        let _ = writeln!(
            errors,
            "anchoring guard: `^(?:{{` must occur at exactly four sites (escape.rs's \
             escaper and its tests' pre-#331 replica, pipeline.rs's validator, and \
             plan.rs's `LabelReplaceSpec::compile`), found {anchored_hits:?} — every \
             OTHER site must build the anchored form through \
             `escape::ch_regex_anchored_checked`, never by hand"
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
    "use pulsus_re2::ClickhouseMatchStrategy",
    "pub fn ch_string(s: &str) -> String",
    "pub fn ch_ident(s: &str) -> String",
    "fn anchored_match_regex(pat: &str) -> String",
    "fn unanchored_match_regex(pat: &str) -> String",
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

// ---------------------------------------------------------------------
// Check E (issue #352): every corpus directive carries a provenance
// marker, and the counts are PINNED.
// ---------------------------------------------------------------------

/// **A replay validates the VALUE, not the provenance.** That distinction
/// is the reason this check exists and the reason it is not a replay.
///
/// "This expectation was captured from the reference, not hand-authored"
/// is a fact about what happened at capture time. No mechanism we can
/// build re-establishes it: a hand-authored value that happens to be
/// correct passes any replay, and a genuine capture that has gone stale
/// fails one. Issue #352's two known instances sit on opposite sides of
/// that line — the never-true one was a PROVENANCE failure caught only
/// incidentally, because its value was also wrong, and had the false
/// capture been accidentally right nothing would have flagged it.
///
/// So this check does the one thing that is actually checkable: it makes
/// every row's claim EXPLICIT and COUNTED, so an unmarked row cannot
/// enter the corpus unnoticed. A future live replay leg (issue #352 step
/// 2) rests on these markers to decide what it may compare; it does not
/// and cannot verify them.
///
/// **`unmarked` is asserted at zero, not printed.** A census of what we
/// have cannot detect what nobody marked, so the count has to be a gate:
/// a new corpus file lands unmarked and this fails. That is deliberate —
/// the issue's own count went stale between filing and pickup (26 of 31
/// became 27 of 32) because a new file arrived carrying an unchecked
/// claim, which is the defect reproducing itself while being described.
#[derive(Debug, PartialEq, Eq, Clone)]
enum Provenance {
    Captured,
    Derived,
    Divergence(String),
    Ported(String),
}

fn parse_provenance(value: &str) -> Result<Provenance, String> {
    let v = value.trim();
    if v == "captured" {
        return Ok(Provenance::Captured);
    }
    if v == "derived" {
        return Ok(Provenance::Derived);
    }
    for (prefix, ctor) in [
        (
            "divergence(",
            Provenance::Divergence as fn(String) -> Provenance,
        ),
        ("ported(", Provenance::Ported as fn(String) -> Provenance),
    ] {
        if let Some(rest) = v.strip_prefix(prefix)
            && let Some(inner) = rest.strip_suffix(')')
            && !inner.is_empty()
        {
            return Ok(ctor(inner.to_string()));
        }
    }
    Err(format!(
        "unknown provenance {v:?} (expected captured | derived | \
         divergence(<ledger-id>) | ported(<source>))"
    ))
}

/// Every `eval`/`eval_fail` directive in the corpus, with the provenance
/// in force for it: its own preceding `# provenance:` line when present,
/// else the file-level default, else `None` (unmarked).
fn corpus_directive_provenance() -> Vec<(String, usize, String, Option<Provenance>)> {
    let dir = manifest_path("tests/logqltest/corpus");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("corpus dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "test"))
        .collect();
    files.sort();
    let mut out = Vec::new();
    for path in files {
        let name = path
            .file_name()
            .expect("file name")
            .to_string_lossy()
            .into_owned();
        let text = std::fs::read_to_string(&path).expect("corpus file");
        let lines: Vec<&str> = text.lines().collect();
        // File-level default: the first `# provenance:` line that is not
        // immediately followed by a directive it would be overriding.
        let mut file_default: Option<Provenance> = None;
        let mut pending: Option<Provenance> = None;
        for (i, raw) in lines.iter().enumerate() {
            if let Some(rest) = raw.trim().strip_prefix("# provenance:") {
                let p = parse_provenance(rest).unwrap_or_else(|e| panic!("{name}:{}: {e}", i + 1));
                let next_is_directive = lines.get(i + 1).is_some_and(|l| l.starts_with("eval"));
                if next_is_directive {
                    pending = Some(p);
                } else if file_default.is_none() {
                    file_default = Some(p);
                } else {
                    panic!("{name}:{}: a second file-level provenance marker", i + 1);
                }
                continue;
            }
            if raw.starts_with("eval") {
                let p = pending.take().or_else(|| file_default.clone());
                out.push((name.clone(), i + 1, raw.trim().to_string(), p));
            }
        }
    }
    out
}

/// The ledger ids a `divergence(...)` marker may name — the `### \`id\``
/// headings of the logs differential ledger.
fn ledger_ids() -> Vec<String> {
    let text = read("../../docs/benchmarks/logs-differential-ledger.md");
    text.lines()
        .filter_map(|l| l.strip_prefix("### `"))
        .filter_map(|l| l.split('`').next())
        .map(|s| s.to_string())
        .collect()
}

#[test]
fn check_e_every_corpus_directive_carries_a_counted_provenance_marker() {
    let rows = corpus_directive_provenance();

    // The scan must be finding the corpus: an empty or tiny result would
    // make every assertion below vacuous.
    assert!(
        rows.len() > 900,
        "expected the whole corpus, found {} directives",
        rows.len()
    );

    // (1) UNMARKED IS A GATE, NOT A REPORT. A new corpus file — or a new
    // row in a file whose default does not apply — fails here rather
    // than joining the corpus with an unchecked claim.
    let unmarked: Vec<String> = rows
        .iter()
        .filter(|(_, _, _, p)| p.is_none())
        .map(|(f, n, d, _)| format!("{f}:{n}: {}", &d[..d.len().min(70)]))
        .collect();
    assert!(
        unmarked.is_empty(),
        "{} corpus directives carry no provenance marker — add a file-level \
         `# provenance:` line or a per-row override (issue #352):\n{}",
        unmarked.len(),
        unmarked.join("\n")
    );

    // (2) Pinned totals, so a class silently changing size fails.
    let count = |f: &dyn Fn(&Provenance) -> bool| {
        rows.iter()
            .filter(|(_, _, _, p)| p.as_ref().is_some_and(f))
            .count()
    };
    let captured = count(&|p| matches!(p, Provenance::Captured));
    let derived = count(&|p| matches!(p, Provenance::Derived));
    let divergence = count(&|p| matches!(p, Provenance::Divergence(_)));
    let ported = count(&|p| matches!(p, Provenance::Ported(_)));
    assert_eq!(
        (captured, derived, divergence, ported, rows.len()),
        (CAPTURED, DERIVED, DIVERGENCE, PORTED, TOTAL),
        "corpus provenance counts moved (captured, derived, divergence, ported, total) \
         — update these pins and the figure quoted on issue #352"
    );

    // (3) Every `divergence(<id>)` names a REAL ledger row. A marker
    // pointing at nothing is the drift this check exists to stop.
    let ids = ledger_ids();
    for (f, n, _, p) in &rows {
        if let Some(Provenance::Divergence(id)) = p {
            assert!(
                ids.contains(id),
                "{f}:{n}: provenance names ledger row {id:?}, which does not exist \
                 in docs/benchmarks/logs-differential-ledger.md (have: {ids:?})"
            );
        }
    }
}

/// The reverse direction of check E(3): a ledger row that says a corpus
/// file gates it must be named by a marker in that file. Without this,
/// a ledger row could claim corpus coverage that no row provides.
#[test]
fn check_e_ledger_rows_claiming_corpus_gating_are_named_by_a_marker() {
    let ledger = read("../../docs/benchmarks/logs-differential-ledger.md");
    let rows = corpus_directive_provenance();
    let marked: Vec<(&String, &String)> = rows
        .iter()
        .filter_map(|(f, _, _, p)| match p {
            Some(Provenance::Divergence(id)) => Some((f, id)),
            _ => None,
        })
        .collect();

    // A ledger section that says "Gated by `<file>.test`" must have a
    // divergence marker in that file naming this section's id.
    let mut current: Option<String> = None;
    let mut missing = Vec::new();
    for line in ledger.lines() {
        if let Some(rest) = line.strip_prefix("### `") {
            current = rest.split('`').next().map(|s| s.to_string());
        }
        if let (Some(id), Some(at)) = (current.as_ref(), line.find("Gated by `")) {
            let tail = &line[at + "Gated by `".len()..];
            if let Some(file) = tail.split('`').next()
                && file.ends_with(".test")
                && !marked.iter().any(|(f, mid)| *f == file && *mid == id)
            {
                missing.push(format!(
                    "ledger `{id}` says it is gated by {file}, but no \
                                      `# provenance: divergence({id})` marker exists there"
                ));
            }
        }
    }
    assert!(missing.is_empty(), "{}", missing.join("\n"));
}

// ---------------------------------------------------------------------
// Check F — a `corpus-counts: none` region states no corpus count.
// ---------------------------------------------------------------------

/// Where a marked region may live: (directory relative to this crate,
/// extensions scanned). Directories, not a file list, so a new file next
/// to the ones below is covered the day it is written.
const NO_COUNT_ROOTS: &[(&str, &[&str])] = &[
    ("tests", &["rs", "md"]),
    ("../../.github/workflows", &["yml"]),
    ("../../docs", &["md"]),
];

/// Every region that must still exist, as `(file, region id)`. A
/// renamed, mistyped or deleted marker would otherwise turn this check
/// into a green no-op — the exact failure mode that let three stale
/// counts through.
///
/// This lists REGIONS, not files, and that distinction is the finding it
/// was rewritten for: `PROVENANCE.md` carries two regions, so a
/// file-name list stayed GREEN when the first marker pair was deleted
/// and the second one kept the file in the set. Each open marker now
/// names its own id (`corpus-counts: none (<id>)`) and its `end` repeats
/// it, so losing, renaming or gutting any single region fails here by
/// name.
const NO_COUNT_REQUIRED: &[(&str, &str)] = &[
    ("PROVENANCE.md", "provenance-replay-coverage"),
    ("PROVENANCE.md", "provenance-template-corpus"),
    ("logqltest_replay.rs", "replay-module-doc"),
    ("logqltest_replay.rs", "replay-absolute-timestamp"),
    ("logqltest_replay.rs", "replay-coverage-constants"),
    ("logqltest_provenance.rs", "provenance-corpus-constants"),
    ("ci.yml", "ci-replay-leg"),
];

/// **The STANDARD spelling of every English cardinal below 10^21.**
///
/// Where it comes from: spell any non-negative integer below 10^21 the
/// ordinary way and the result is built out of exactly these tokens,
/// combined with hyphens and juxtaposition — the units and teens below
/// twenty, the eight tens, and the scale words.
/// [`spelling_uses_only_this_set`] re-derives that from an independent
/// speller, so this set is complete *for what a speller emits* and
/// carries nothing dead.
///
/// **That is a narrower claim than "the closed lexicon of English
/// cardinals", which is what this doc said in issue #248 round 5 and
/// which was false by the next review round: `nought` is an English
/// cardinal zero, it is not a token any speller emits, and `nought rows`
/// went straight through.** English keeps more than one spelling for the
/// same cardinal, so a speller cannot enumerate them; the extra ones are
/// listed in [`NUMERAL_VARIANTS`], by hand and WITHOUT a completeness
/// claim. What is closed here is the standard-spelling side alone.
///
/// Where it stops on the other axis: 10^21. `sextillion` and the
/// Latinate series above it are not here, on the ground that a corpus
/// count reaching 10^21 is not the failure this guard exists for.
/// Extending the set upward is mechanical if that ever stops being true.
const CARDINAL_NUMERALS: &[&str] = &[
    // Units and teens — every integer below twenty.
    "zero",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
    "eleven",
    "twelve",
    "thirteen",
    "fourteen",
    "fifteen",
    "sixteen",
    "seventeen",
    "eighteen",
    "nineteen",
    // Tens.
    "twenty",
    "thirty",
    "forty",
    "fifty",
    "sixty",
    "seventy",
    "eighty",
    "ninety",
    // Scales, ascending to 10^18.
    "hundred",
    "thousand",
    "million",
    "billion",
    "trillion",
    "quadrillion",
    "quintillion",
];

/// Non-standard spellings of a cardinal — mostly of zero.
///
/// **Not complete, and deliberately so.** These are the words that name
/// an exact cardinal but that no speller produces, so
/// [`spelling_uses_only_this_set`] cannot derive them and cannot check
/// them: each is here because somebody wrote it down. `nought` arrived
/// as a review finding on issue #248 (round 6) after the round-5 doc
/// claimed the numeral side was closed.
///
/// **What is deliberately NOT here, and why.** `ought` is a dialect zero
/// ("ought-six") and a very common modal verb; `none`, `both`, `single`
/// and `a` name exact quantities and are ordinary English function
/// words. Adding any of them would redden prose that states no count at
/// all, and a guard that reddens on "we ought to" gets switched off
/// within a week — so the guard keeps the false-negative instead, and
/// says so. Archaic and dialect spellings beyond this list are out of
/// scope for the same reason.
const NUMERAL_VARIANTS: &[&str] = &["nought", "naught", "aught", "nil", "zilch"];

/// Quantity words that are NOT cardinal numerals.
///
/// **This list is not complete and cannot be made complete**, and that is
/// stated here rather than implied: English has an open supply of vague
/// quantifiers (`a handful`, `a bunch`, `plenty`, `umpteen`), so no
/// enumeration terminates. It carries the ones already known to have
/// evaded the guard, and the residual is named in
/// [`check_f_marked_regions_state_no_corpus_count`]'s doc.
const INEXACT_QUANTITY_WORDS: &[&str] = &["dozen", "score", "couple", "handful"];

/// Whether a token spells a quantity.
///
/// A hyphenated compound counts when EVERY part does: `twenty-one` and
/// `thirty-five` are numbers, `one-off` and `well-formed` are not. A
/// trailing plural `s` is stripped first, so `hundreds` and `dozens`
/// resolve like their singulars.
fn is_number_word(tok: &str) -> bool {
    let lower = tok.to_ascii_lowercase();
    let known = |p: &str| {
        let p = p.strip_suffix('s').unwrap_or(p);
        CARDINAL_NUMERALS.contains(&p)
            || NUMERAL_VARIANTS.contains(&p)
            || INEXACT_QUANTITY_WORDS.contains(&p)
    };
    let mut parts = lower.split('-').filter(|p| !p.is_empty()).peekable();
    parts.peek().is_some() && parts.all(known)
}

/// Every count claim on one line of prose, as `(token, why)`.
///
/// The rule is INVERTED from the one this replaces. That one asked
/// whether a number word was followed by a noun from a `COUNT_NOUNS`
/// list — an AND over two open-ended enumerations, so a miss in either
/// one shipped as a silent pass, and four rounds of review each found
/// the next missing word. This one asks only whether the token is a
/// number: a marked region may contain no digit and no numeral, whatever
/// follows it. There is nothing left to enumerate on the noun side; on
/// the numeral side [`CARDINAL_NUMERALS`] is complete for standard
/// spellings and nothing more, with [`NUMERAL_VARIANTS`] a hand list of
/// non-standard ones that is labelled incomplete and is not claimed to
/// cover the remainder.
///
/// The cost is deliberate: prose inside a marked region cannot say "one
/// level up" or "the two halves" either. That is the contract — these
/// regions describe WHICH rows moved and why, and the quantities live on
/// the constants that recompute them.
fn count_claims(prose: &str) -> Vec<(String, &'static str)> {
    let cleaned = strip_code_spans(prose);
    let toks: Vec<&str> = cleaned.split_whitespace().map(trimmed_token).collect();
    let mut out = Vec::new();
    for (i, tok) in toks.iter().enumerate() {
        if tok.chars().any(|c| c.is_ascii_digit())
            && !is_allowed_numeric(tok, i.checked_sub(1).map(|p| toks[p]))
        {
            out.push((
                (*tok).to_string(),
                "a count belongs on the constant that recomputes it, not in prose",
            ));
        } else if is_number_word(tok) {
            out.push((
                (*tok).to_string(),
                "spell out no quantity here; name the rows and point at the constant",
            ));
        }
    }
    out
}

fn files_under(dir: &Path, exts: &[&str], out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        panic!("check F root {dir:?} does not exist");
    };
    let mut entries: Vec<PathBuf> = rd.flatten().map(|e| e.path()).collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            files_under(&p, exts, out);
        } else if p
            .extension()
            .is_some_and(|e| exts.iter().any(|x| *x == e.to_string_lossy()))
        {
            out.push(p);
        }
    }
}

/// One marked region: the id its markers carry, where it opens, and its
/// comment/prose lines with the marker lines themselves dropped. Code is
/// not scanned: the counts BELONG in code.
#[derive(Debug)]
struct MarkedRegion {
    id: String,
    open_line: usize,
    /// Every scanned line, the two marker lines' own tails included.
    prose: Vec<(usize, String)>,
    /// Of those, the lines BETWEEN the markers. Markers around nothing
    /// would otherwise satisfy the "this region still has prose" floor.
    body_lines: usize,
}

/// The id a marker line carries, e.g. `corpus-counts: none (ci-replay-leg)`.
///
/// Required, and required to match on the region's `end`: an id is what
/// makes a LOST region visible to [`NO_COUNT_REQUIRED`], which a file
/// name cannot do for the second region in a file.
fn marker_id(path: &Path, line_no: usize, kind: &str, rest: &str) -> String {
    let rest = rest.trim();
    let id = rest
        .strip_prefix('(')
        .and_then(|r| r.split(')').next())
        .unwrap_or_default();
    assert!(
        !id.is_empty()
            && id
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
        "{path:?}:{line_no}: `corpus-counts: {kind}` must name its region, as \
         `corpus-counts: {kind} (<id>)` with a lowercase-and-dashes id — an unnamed region \
         cannot be missed when it disappears (see NO_COUNT_REQUIRED)"
    );
    id.to_string()
}

/// What a marker line says after its id, scanned like any other prose —
/// otherwise the one line the parser consumes is the one line a count
/// could hide on.
fn marker_prose(line_no: usize, rest: &str) -> Vec<(usize, String)> {
    let tail = rest
        .trim()
        .split_once(')')
        .map(|(_, tail)| tail.trim())
        .unwrap_or("");
    if tail.is_empty() {
        Vec::new()
    } else {
        vec![(line_no, tail.to_string())]
    }
}

fn marked_regions(path: &Path, text: &str) -> Vec<MarkedRegion> {
    let is_md = path.extension().is_some_and(|e| e == "md");
    let is_yml = path.extension().is_some_and(|e| e == "yml");
    let mut out: Vec<MarkedRegion> = Vec::new();
    let mut open: Option<MarkedRegion> = None;
    let mut in_fence = false;
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if is_md && line.starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        // A marker OPENS a line, after whatever comment syntax the file
        // uses; a mention of one mid-sentence (this file's own prose, and
        // PROVENANCE.md's statement of the rule) is not a marker.
        let bare = line
            .trim_start_matches("<!--")
            .trim_start_matches("//!")
            .trim_start_matches("///")
            .trim_start_matches("//")
            .trim_start_matches('#')
            .trim();
        if let Some(rest) = bare.strip_prefix("corpus-counts: none") {
            assert!(
                open.is_none(),
                "{path:?}:{}: a `corpus-counts: none` region is already open",
                i + 1
            );
            // The marker line's OWN tail is prose too: regions open with
            // `none (<id>) — why this region exists`, and skipping the
            // whole line would leave exactly one line nobody scans.
            open = Some(MarkedRegion {
                id: marker_id(path, i + 1, "none", rest),
                open_line: i + 1,
                prose: marker_prose(i + 1, rest),
                body_lines: 0,
            });
            continue;
        }
        if let Some(rest) = bare.strip_prefix("corpus-counts: end") {
            let mut region = open.take().unwrap_or_else(|| {
                panic!("{path:?}:{}: `corpus-counts: end` closes nothing", i + 1)
            });
            region.prose.extend(marker_prose(i + 1, rest));
            let id = marker_id(path, i + 1, "end", rest);
            assert_eq!(
                id,
                region.id,
                "{path:?}:{}: `corpus-counts: end ({id})` closes the region opened at line {} as \
                 `{}` — the ids must match, or a region can be silently re-labelled out of \
                 NO_COUNT_REQUIRED",
                i + 1,
                region.open_line,
                region.id
            );
            out.push(region);
            continue;
        }
        let Some(region) = open.as_mut() else {
            continue;
        };
        if in_fence {
            continue;
        }
        let prose = if is_md {
            Some(line)
        } else if is_yml {
            line.strip_prefix('#')
        } else {
            line.strip_prefix("//!")
                .or_else(|| line.strip_prefix("///"))
                .or_else(|| line.strip_prefix("//"))
        };
        // Blank lines are not prose: a region gutted down to its markers
        // and a blank line would otherwise clear the "still has text"
        // floor below (it did, on the first run of the mutant battery).
        if let Some(p) = prose.map(str::trim).filter(|p| !p.is_empty()) {
            region.prose.push((i + 1, p.to_string()));
            region.body_lines += 1;
        }
    }
    if let Some(region) = open {
        panic!(
            "{path:?}: `corpus-counts: none ({})` opened at line {} is never closed",
            region.id, region.open_line
        );
    }
    out
}

/// Drops the spans a count never hides in — backticked code, HTML
/// comments — so what remains is prose a reader reads as a claim.
fn strip_code_spans(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut in_tick = false;
    let mut rest = line;
    while let Some(at) = rest.find("<!--") {
        out.push_str(&rest[..at]);
        match rest[at..].find("-->") {
            Some(end) => rest = &rest[at + end + 3..],
            None => {
                rest = "";
                break;
            }
        }
    }
    out.push_str(rest);
    let mut cleaned = String::with_capacity(out.len());
    for c in out.chars() {
        if c == '`' {
            in_tick = !in_tick;
            cleaned.push(' ');
        } else if !in_tick {
            cleaned.push(c);
        }
    }
    cleaned
}

fn trimmed_token(t: &str) -> &str {
    t.trim_start_matches(|c: char| !c.is_alphanumeric() && c != '#')
        .trim_end_matches(|c: char| !c.is_alphanumeric())
}

/// A token carrying digits that is NOT a count: an issue reference, an
/// ISO date, a version, or a plan step. Everything else with a digit in
/// it is a claim about how many, which is what the region forbids.
fn is_allowed_numeric(tok: &str, prev: Option<&str>) -> bool {
    // `#344's` is the same reference as `#344`.
    let tok = tok.split('\'').next().unwrap_or(tok);
    if let Some(rest) = tok.strip_prefix('#') {
        return !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit());
    }
    if tok.len() == 10
        && tok.as_bytes()[4] == b'-'
        && tok.as_bytes()[7] == b'-'
        && tok.chars().filter(|c| c.is_ascii_digit()).count() == 8
    {
        return true;
    }
    if let Some(rest) = tok.strip_prefix('v')
        && rest.chars().all(|c| c.is_ascii_digit() || c == '.')
    {
        return true;
    }
    matches!(
        prev.map(str::to_ascii_lowercase).as_deref(),
        Some("step") | Some("steps")
    )
}

/// **F: inside a `corpus-counts: none` region, prose states no count.**
///
/// Issue #248 corrected a stale corpus count in each of its three
/// rounds — the `today` column, the sentence explaining why that column
/// went, then a delta restated in two more places. Every round fixed the
/// number and none fixed the class, because nothing failed when the next
/// copy drifted. This is that thing.
///
/// **The rule is a prohibition, not a pattern match** (issue #248 round
/// 5). It used to be "a number word followed within three tokens by a
/// noun from `COUNT_NOUNS`" — an AND over two open-ended lists, so a
/// word missing from either shipped as a silent pass. `zero rows` was
/// the fifth such hole found by review, after filename-keyed regions,
/// compound number words, the marker line's own tail and blank lines
/// counting as prose. Adding `zero` would have fixed the instance and
/// left the shape, so the shape went instead: a marked region may carry
/// **no digit and no numeral**, full stop. The noun side no longer
/// exists to be incomplete.
///
/// **The numeral side is complete for standard spellings and no
/// further** (issue #248 round 6). Round 5 said it was closed outright;
/// the next review round injected `nought rows` and it passed, which is
/// the sixth hole. [`CARDINAL_NUMERALS`] is exactly what a speller
/// emits, and [`spelling_uses_only_this_set`] proves it. A spelling
/// OUTSIDE that set is caught only if somebody has written it down:
/// [`NUMERAL_VARIANTS`] holds the ones somebody has (`nought`, `naught`,
/// `aught`, `nil`, `zilch`), is hand-written, is labelled incomplete,
/// and names the words it leaves out on purpose (`ought`, `none`, `a`:
/// exact quantities that are also ordinary function words, whose
/// inclusion would redden count-free prose). It is a sample of the
/// non-standard spellings, never the set of them. So
/// the honest sentence is: **this covers digits, the standard cardinal
/// spellings in full, and the listed variants; it is not closed against
/// archaic or dialect forms.**
///
/// **The guard's own failure surface, bounded on the side that can be.**
/// A guard whose sentence is wider than its assertion is the same defect
/// one level up. Every STRUCTURAL way this one could go quietly green
/// has a mutant that reddens it, and that side is closed (the split
/// below says why); the COUNT-FORM side is a word list, so its rows are
/// the forms already known and not every form there is.
/// Every row below was applied to a real file, run and reverted on issue
/// #248 rounds 5 and 7 (the structural ones on round 4 as well); the
/// count-form rows are additionally COMMITTED as
/// [`every_count_form_known_to_have_evaded_this_guard_is_detected`], so
/// they are re-proved on every run rather than on the day someone ran
/// them:
///
/// | mutant | what fails |
/// |---|---|
/// | a digit count inside a region | [`count_claims`] (committed regression) |
/// | a spelled count, `nine rows` | the same (committed) |
/// | a COMPOUND spelled count, `twenty-one rows` | the same, via [`is_number_word`] (committed) |
/// | a spelled ZERO, `zero rows` | the same (committed) |
/// | a VARIANT zero, `nought rows` | the same, via [`NUMERAL_VARIANTS`] (committed) |
/// | a bare numeral with no noun after it | the same (committed) |
/// | a count on a MARKER line's own tail | the same — the marker's tail is scanned, not skipped |
/// | a marker PAIR deleted | [`NO_COUNT_REQUIRED`] misses the region id |
/// | the open marker alone deleted | `end` closes nothing |
/// | the `end` alone deleted | the next `none` finds a region already open |
/// | an id renamed on one side | open/end ids must match |
/// | an id renamed on both sides | the id is missing from [`NO_COUNT_REQUIRED`] |
/// | an id dropped entirely | a marker must name its region |
/// | a duplicated id | ids must be unique |
/// | a region's prose deleted, markers kept | a required region must still carry prose |
///
/// **Where the table IS exhaustive, and where it is not.** Round 5
/// claimed "there is no fifteenth" over the whole table and the next
/// round found one, so the claim is now split.
///
/// STRUCTURE — exhaustive. A region reaching the scan is exactly four
/// parts (an open marker, an id, a body, an end marker) and the
/// structural rows mutate each part in each way it can go wrong:
/// present/absent, renamed on one side/both, and (for the body)
/// emptied. The marker tokens are two, both above. Blank lines do NOT
/// count as prose, which the mutant table itself found: `M10` first
/// passed because markers around a single empty line cleared the "still
/// has text" floor.
///
/// COUNT FORMS — not exhaustive, and the residual is a WORD LIST, not a
/// shape. Digits are closed by `is_ascii_digit`; standard cardinal
/// spellings are closed by [`spelling_uses_only_this_set`]; variant
/// spellings are a hand list. So the mutant that still exists is
/// "another spelling of a number nobody wrote down", and the next one
/// found extends [`NUMERAL_VARIANTS`] rather than reshaping the rule.
/// Naming that is the point: five rounds tried to close this by adding
/// the word review had just found, and the sixth found the next one.
///
/// What remains is not a hole in the guard but a hole in its INPUT, and
/// it is the next paragraph.
///
/// **What it cannot see, stated rather than implied:** a count inside
/// backticks, a count in a file outside [`NO_COUNT_ROOTS`], a count in a
/// region nobody marked, an ORDINAL standing in for a count ("the third
/// block landed"), a vague quantifier outside the deliberately
/// incomplete [`INEXACT_QUANTITY_WORDS`] ("a bunch", "plenty",
/// "umpteen"), and a spelling of a cardinal outside
/// [`NUMERAL_VARIANTS`] — including the ones that list excludes on
/// purpose ("we ought to", "none of the rows"). Those are the residual
/// the inversion does not remove. The marker is the contract — prose
/// that carries counts must be inside one — and [`NO_COUNT_REQUIRED`]
/// keeps the known regions from evaporating.
#[test]
fn check_f_marked_regions_state_no_corpus_count() {
    let mut files = Vec::new();
    for (root, exts) in NO_COUNT_ROOTS {
        files_under(&manifest_path(root), exts, &mut files);
    }
    let mut violations: Vec<String> = Vec::new();
    // (file name, region id, prose lines) for every region found.
    let mut found: Vec<(String, String, usize)> = Vec::new();
    let mut scanned_lines = 0usize;

    for path in &files {
        let text = std::fs::read_to_string(path).unwrap_or_default();
        if !text.contains("corpus-counts: none") {
            continue;
        }
        let name = path
            .file_name()
            .expect("file name")
            .to_string_lossy()
            .into_owned();
        for region in marked_regions(path, &text) {
            found.push((name.clone(), region.id.clone(), region.body_lines));
            scanned_lines += region.body_lines;
            for (line_no, prose) in region.prose {
                for (tok, why) in count_claims(&prose) {
                    violations.push(format!("{name}:{line_no} ({}): `{tok}` — {why}", region.id));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "a `corpus-counts: none` region states {} count(s):\n{}",
        violations.len(),
        violations.join("\n")
    );
    // A region id must be unique repo-wide, or `NO_COUNT_REQUIRED` would
    // be satisfied by whichever copy survived.
    let mut ids: Vec<&str> = found.iter().map(|(_, id, _)| id.as_str()).collect();
    ids.sort_unstable();
    let dupes: Vec<&&str> = ids
        .windows(2)
        .filter(|w| w[0] == w[1])
        .map(|w| &w[0])
        .collect();
    assert!(
        dupes.is_empty(),
        "`corpus-counts` region ids must be unique; duplicated: {dupes:?}"
    );

    for (file, id) in NO_COUNT_REQUIRED {
        let region = found.iter().find(|(f, i, _)| f == file && i == id);
        let Some((_, _, prose_lines)) = region else {
            panic!(
                "{file} no longer carries the `corpus-counts: none ({id})` region — a mistyped, \
                 renamed or deleted marker turns this check into a no-op, which is how the class \
                 survived three corrections. A file-name list could not see this: {file} may \
                 hold other regions and stay in the set. Found: {found:?}"
            );
        };
        // Markers around nothing pass every assertion above vacuously.
        assert!(
            *prose_lines > 0,
            "{file}: the `corpus-counts: none ({id})` region has no prose left — the markers \
             survived and the text they govern did not"
        );
    }
    // Coarse backstop on the scan as a whole, above the per-region floor.
    assert!(
        scanned_lines > 60,
        "check F scanned only {scanned_lines} prose lines — the regions have collapsed"
    );
}

/// The count-form mutants of check F, committed instead of hand-applied.
///
/// The name is the honest one: these are the forms KNOWN to have got
/// past this guard, not every form a count can take. The count-form side
/// of the rule is a word list ([`NUMERAL_VARIANTS`]) and cannot claim
/// more — the earlier name said "in every form it can take", which
/// contradicted the exhaustiveness split in
/// [`check_f_marked_regions_state_no_corpus_count`]'s own doc.
///
/// Every entry in the first list is a sentence that either shipped
/// inside a marked region or was injected there by a reviewer and passed
/// — `nought rows` is the round-6 finding, `zero rows` the round-5 one,
/// `twenty-one rows` the round-4 one, `nine rows` the round-3 one. A
/// hand-applied mutant proves the guard catches it on the day it is run;
/// this proves it on every run.
///
/// The first list also carries one sentence per [`NUMERAL_VARIANTS`]
/// entry, so that list cannot shrink silently — the same shape
/// [`spelling_uses_only_this_set`] gives [`CARDINAL_NUMERALS`], which is
/// the only kind of completeness either side can honestly claim.
///
/// The second list is the other direction, and it is the one that keeps
/// the rule usable: an issue reference, a version, a date, a plan step
/// and anything inside backticks are NOT counts, and a guard that
/// reddened on them would be turned off within a week. Its last three
/// entries pin the words [`NUMERAL_VARIANTS`] refuses on that ground.
#[test]
fn every_count_form_known_to_have_evaded_this_guard_is_detected() {
    for prose in [
        // Digits, in the shape that shipped three times.
        "The latest block added 11 rows.",
        // Spelled, plain.
        "The latest block added nine rows.",
        // Spelled, compound (round 4).
        "The latest block added twenty-one rows.",
        // Spelled zero (round 5) — the fifth hole, and the reason the
        // rule became "no numeral" rather than "no known numeral".
        "The latest block added zero rows.",
        // A VARIANT spelling of zero (round 6) — the sixth hole, and the
        // reason the rule no longer claims the numeral side is closed.
        // One per NUMERAL_VARIANTS entry, so deleting any of them reddens
        // here rather than shipping as a silent pass.
        "The latest block added nought rows.",
        "The latest block added naught rows.",
        "The latest block added aught rows.",
        "The latest block added nil rows.",
        "The latest block added zilch rows.",
        // A plural scale word.
        "Hundreds of directives replay against the container.",
        // Quantity words that are not numerals — the deliberately
        // incomplete half of the rule, carrying the ones already known
        // to have evaded it.
        "A dozen more landed in the same capture run.",
        "A handful of rows landed in the same capture run.",
        // No count NOUN anywhere: the old rule needed one within three
        // tokens and this shape walked straight past it.
        "The corpus grew by three.",
        "Rows added: seven",
        // A number word on the far side of the old three-token window.
        "Nine of the rows that the round added were captured.",
    ] {
        assert!(
            !count_claims(prose).is_empty(),
            "check F would not catch {prose:?} inside a marked region"
        );
    }

    for prose in [
        "Issue #248 corrected a stale copy in each round.",
        "Measured on v3.7.4 against the digest-pinned oracle.",
        "Captured 2026-08-06 on the pinned container.",
        "Step 2 of the plan renames the marker.",
        "The value lives on `CAPTURED = 1_208` and is recomputed there.",
        "A one-off mistake is not a numeral, nor is a well-formed pattern.",
        "The prose says which rows moved a figure and why.",
        // The words NUMERAL_VARIANTS leaves out on purpose. Each names an
        // exact quantity and each is an ordinary function word, so the
        // guard keeps the false negative rather than reddening prose that
        // states no count. Pinned here so "just add it" fails loudly.
        "A reviewer ought to read the constant instead.",
        "None of the rows in that block moved.",
        "Both halves of the marker must carry the id.",
    ] {
        assert!(
            count_claims(prose).is_empty(),
            "check F reddens on {prose:?}, which states no count: {:?}",
            count_claims(prose)
        );
    }
}

/// [`CARDINAL_NUMERALS`] is complete for exactly what it claims: every
/// non-negative integer below 10^21 spells out of it and nothing in it
/// is dead.
///
/// It does NOT claim to hold every English word for a cardinal —
/// `nought` is one and is not derivable from a speller. That half lives
/// in [`NUMERAL_VARIANTS`] and is checked by example only.
///
/// The speller carries its OWN literals, duplicated on purpose, so this
/// is a cross-check between two independent listings and not a
/// tautology: deleting `seventy` from [`CARDINAL_NUMERALS`] leaves the
/// speller emitting it and reddens the first assertion, while a typo in
/// the set (`nintey`) leaves a token nothing can spell and reddens the
/// second.
#[test]
fn spelling_uses_only_this_set() {
    fn spell(n: u128, out: &mut Vec<&'static str>) {
        const UNITS: [&str; 20] = [
            "zero",
            "one",
            "two",
            "three",
            "four",
            "five",
            "six",
            "seven",
            "eight",
            "nine",
            "ten",
            "eleven",
            "twelve",
            "thirteen",
            "fourteen",
            "fifteen",
            "sixteen",
            "seventeen",
            "eighteen",
            "nineteen",
        ];
        const TENS: [&str; 10] = [
            "", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety",
        ];
        const SCALES: [(u128, &str); 7] = [
            (1_000_000_000_000_000_000, "quintillion"),
            (1_000_000_000_000_000, "quadrillion"),
            (1_000_000_000_000, "trillion"),
            (1_000_000_000, "billion"),
            (1_000_000, "million"),
            (1_000, "thousand"),
            (100, "hundred"),
        ];
        for (value, name) in SCALES {
            if n >= value {
                spell(n / value, out);
                out.push(name);
                if !n.is_multiple_of(value) {
                    spell(n % value, out);
                }
                return;
            }
        }
        if n < 20 {
            out.push(UNITS[n as usize]);
        } else {
            out.push(TENS[(n / 10) as usize]);
            if !n.is_multiple_of(10) {
                out.push(UNITS[(n % 10) as usize]);
            }
        }
    }

    // Below a thousand exercises every unit, teen and ten in every
    // combination; the anchors reach each scale word above it.
    let mut reached: BTreeSet<&'static str> = BTreeSet::new();
    let anchors = [
        1_000_000u128,
        1_000_000_000,
        1_000_000_000_000,
        1_000_000_000_000_000,
        1_000_000_000_000_000_000,
        999_999_999_999_999_999_999,
    ];
    for n in (0u128..=1_000).chain(anchors) {
        let mut spelled = Vec::new();
        spell(n, &mut spelled);
        for tok in spelled {
            assert!(
                CARDINAL_NUMERALS.contains(&tok),
                "{n} spells with `{tok}`, which CARDINAL_NUMERALS does not carry"
            );
            reached.insert(tok);
        }
    }
    let dead: Vec<&&str> = CARDINAL_NUMERALS
        .iter()
        .filter(|w| !reached.contains(*w))
        .collect();
    assert!(
        dead.is_empty(),
        "CARDINAL_NUMERALS carries {dead:?}, which no integer below 10^21 spells with — a typo, \
         or a variant spelling that belongs in NUMERAL_VARIANTS instead"
    );
}

/// The other half of the rule: **a count that must appear in prose is
/// gated, not trusted.**
///
/// `docs/features.md` and the logs differential ledger both quote the
/// template-engine corpus's size, and a reader of either would take the
/// figure at face value. They are the same class as the copies issue
/// #248 kept correcting — except that deleting them would remove a scale
/// claim the docs exist to make. So they stay, and this recomputes them
/// from `t1…t6_*.test` and fails when either doc drifts from the corpus,
/// in either direction.
///
/// (The re-derivation recipe in PROVENANCE.md is `grep -ac`, with the
/// `-a`: `t5_time.test` trips GNU grep's binary heuristic and a plain
/// `grep -c '^eval '` silently prints nothing for it, which reads as
/// zero. This check reads the files directly and cannot be fooled that
/// way.)
#[test]
fn check_f_quoted_template_corpus_counts_match_the_corpus() {
    let dir = manifest_path("tests/logqltest/corpus");
    let mut per_file: Vec<(String, usize)> = Vec::new();
    let (mut evals, mut fails) = (0usize, 0usize);
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("corpus dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with('t'))
                && p.extension().is_some_and(|e| e == "test")
        })
        .collect();
    entries.sort();
    for p in &entries {
        let text = std::fs::read_to_string(p).expect("read t-file");
        let e = text
            .lines()
            .filter(|l| l.starts_with("eval") && !l.starts_with("eval_fail"))
            .count();
        let f = text.lines().filter(|l| l.starts_with("eval_fail")).count();
        per_file.push((
            p.file_name().expect("name").to_string_lossy().into_owned(),
            e,
        ));
        evals += e;
        fails += f;
    }
    assert!(
        per_file.len() >= 6,
        "expected the whole template corpus, found {per_file:?}"
    );
    let total = evals + fails;
    let breakdown = per_file
        .iter()
        .map(|(_, e)| e.to_string())
        .collect::<Vec<_>>()
        .join("+");

    let features = read("../../docs/features.md");
    let ledger = read("../../docs/benchmarks/logs-differential-ledger.md");
    let pins: &[(&str, &str, String)] = &[
        (
            "docs/features.md",
            &features,
            format!(
                "{total} container-captured corpus directives ({evals} `eval` + {fails} `eval_fail`"
            ),
        ),
        (
            "docs/benchmarks/logs-differential-ledger.md",
            &ledger,
            format!("{total} container-captured corpus directives — {evals} `eval`"),
        ),
        (
            "docs/benchmarks/logs-differential-ledger.md",
            &ledger,
            format!("({breakdown} across"),
        ),
        (
            "docs/benchmarks/logs-differential-ledger.md",
            &ledger,
            format!("{fails} `eval_fail` reject-parity cases"),
        ),
    ];
    for (name, text, want) in pins {
        assert!(
            text.contains(want.as_str()),
            "{name} no longer says {want:?}. The template corpus now holds {evals} `eval` + \
             {fails} `eval_fail` = {total} directives ({breakdown}); update the doc, or — if \
             you reworded rather than re-counted — update this pin."
        );
    }
}

// corpus-counts: none (provenance-corpus-constants) — the values below are
// recomputed from the corpus and asserted by `check_e_...`; the prose says which rows moved them and
// why, never how many. Issue #248 restated a delta here and got it wrong
// while the arithmetic beside it was right, which is the whole reason the
// rule is now mechanical (`check_f_marked_regions_state_no_corpus_count`).
/// Pinned corpus provenance counts (issue #352 step 1).
///
/// Issue #344 (execution half): `b18_range_agg_grouping.test` gained
/// `eval` rows — the accepted operations executed, the
/// `by`/`without`/empty-list/duplicate/absent-name shapes, `by` on the
/// unwrapped label, and the `eval range` rows for the sliding path
/// (which include the cross-stream `StableHash` tie). Every value came
/// from a fresh capture against the pinned v3.7.4 container, so they
/// carry the file's `captured` default; no pre-existing `eval_fail` row
/// was removed, but its "not yet executed" refusals became `eval` rows.
/// More landed with the instant `first`/`last` delivery-order fix in the
/// same issue — the cross-stream tie rows that were briefly excluded
/// while our instant reducer still used a value tiebreak. The first
/// added the fingerprint-vs-`StableHash` inversion section,
/// `avg_over_time`'s ungrouped recurrence pin and the post-unwrap-filter
/// error pair, and removed the grouped `avg_over_time` row whose
/// captured value was a frontend-dependent `sum/count` rather than the
/// reducer's.
///
/// Issue #248 added `b20_nested_ip.test` — `eval` plus `eval_fail` rows,
/// in a single capture run against the pinned v3.7.4 container (accept/reject
/// disposition AND every value). They carry the file's `captured`
/// default; as elsewhere, an `eval_fail`'s `msg:` gate is PulsusDB's own
/// wording while the REJECTION it pins was captured. Its second round
/// added the error-ordering block to the same file, captured in the same
/// way from the same image: a numeric conversion failing to the LEFT of a
/// leaf that reads the error state, plus the chained-short-circuit rows.
///
/// Issue #241 adopted the formerly-EXCLUDED `by`-over-a-missing-label
/// sub-cases held open in the variants, `label_replace` and
/// grouping-dedup files, and pinned the general shape they are instances
/// of alongside them: the raw-stream-label forms, the
/// present-name-plus-absent-name and all-absent `by` forms, and the
/// `without` forms that the divergence never touched. Re-captured against
/// the pinned v3.7.4 container in the same run, so they carry each file's
/// `captured` default.
const CAPTURED: usize = 1_219;
/// Issue #343 added `b19_offset.test`: hand-derived from the semantics
/// measured on that issue, over a fixture authored here rather than taken
/// from the container, so they are `derived` and not `captured`. Its
/// boundary fix added the domain-edge rows (each off-axis row with its
/// on-axis control), same file default.
const DERIVED: usize = 31;
const DIVERGENCE: usize = 18;
const PORTED: usize = 32;
const TOTAL: usize = 1_300;
// corpus-counts: end (provenance-corpus-constants)
