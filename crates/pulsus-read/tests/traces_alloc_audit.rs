//! Issue #57 round-4: the MECHANICAL allocation-audit guard (the #36
//! drift-guard pattern — a test that detects change, not prose). It
//! scans the two budget-bearing search modules for collection-allocation
//! tokens on a comment-stripped, string-blanked view of the source and
//! asserts the hits match a pinned allowlist of
//! `(file, enclosing fn, token) → count` entries, each annotated with
//! WHERE its budget charge lives. Any new allocation site — a `Vec`, a
//! map/set, a `.collect`, a `format!` — fails this test until it is
//! added here **with its charge documented**, ending the
//! materialize-then-charge findings class by construction.
//!
//! Deliberately crude: token counting per enclosing `fn`, not semantic
//! analysis — drift detection is the goal. `#[cfg(test)] mod tests`
//! regions are exempt (test allocations need no budget).

use std::collections::BTreeMap;

const TOKENS: &[&str] = &[
    "Vec::new",
    "Vec::with_capacity",
    "HashSet::",
    "HashMap::",
    ".collect",
    ".to_vec",
    "String::with_capacity",
    "format!",
];

/// `(file, enclosing fn, token, count, charge site)` — the pinned
/// allowlist. The charge-site column is documentation the guard forces
/// you to write; the audit tables in the module docs carry the prose.
#[rustfmt::skip]
const ALLOWLIST: &[(&str, &str, &str, usize, &str)] = &[
    // ---- exec.rs ------------------------------------------------------
    ("exec.rs", "merge_candidates", ".collect", 1,
     "map+output pre-charged in search_inner (total_rows x tuple cost) before the call; reconciled to survivors after"),
    ("exec.rs", "merge_candidates", "HashMap::", 1,
     "same pre-charge as above"),
    ("exec.rs", "charge_explain", "format!", 1,
     "charge_explain charges sql+note+overhead before the to_string/format"),
    ("exec.rs", "collect_rows_charged", "Vec::new", 1,
     "the row vec fills only through the per-row cost closure charge (charged as it streams)"),
    ("exec.rs", "fetch_by_id", "Vec::new", 1,
     "trace-by-ID point read - deliberately outside the search budget (issue #55 scope; no SearchPlan/ByteBudget on this path)"),
    ("exec.rs", "list_tag_names", "Vec::new", 1,
     "tag-names catalog read (issue #58) - outside the search budget by design (no SearchPlan/ByteBudget on this path); hard-bounded by the SQL LIMIT to TAG_NAMES_MAX + 1 short catalog rows"),
    ("exec.rs", "list_tag_values", "Vec::new", 1,
     "tag-values catalog read (issue #58) - same class as list_tag_names; hard-bounded by the SQL LIMIT to TAG_VALUES_MAX + 1 rows"),
    ("exec.rs", "search_inner", "HashMap::", 1,
     "the empty-winners roots arm: HashMap::new() with zero entries - nothing to charge"),
    ("exec.rs", "pick_roots", "HashMap::", 1,
     "root rows charged per row during streaming; map retained via roots_retained_bytes charge before row release"),
    ("exec.rs", "pick_roots", ".collect", 1,
     "rebinds the same map's entries (into_iter map collect); covered by roots_retained_bytes"),
    ("exec.rs", "search_inner", "Vec::new", 1,
     "per_generator outer vec - slots covered by per-row CANDIDATE_TUPLE_BYTES overhead"),
    ("exec.rs", "search_inner", ".collect", 4,
     "generator tuples (per-row charge), batch ids (id_list_charge), heap->winners (retained_bytes already charged), winner ids (winner_ids_charge)"),
    ("exec.rs", "search_inner", "Vec::with_capacity", 1,
     "output slots pre-charged (winners.len x size_of<TraceSearchResult> + overhead) before the reservation"),
    ("exec.rs", "group_hydrated_rows", "Vec::new", 2,
     "outer traces vec + per-group inner vec: initial reservations (VEC_INITIAL_RESERVATION_SLOTS) + 2x slot doubling slack charged before each push; exact accounting unit-tested (round 5)"),
    ("exec.rs", "group_hydrated_rows", "HashSet::", 1,
     "dedup set entries at the standard hash cost ([u8;8] + RETAINED_ENTRY_OVERHEAD) charged before insert; replays contains-checked first, charge nothing"),
    ("exec.rs", "batch_attrs", ".collect", 2,
     "membership set + agg map entries charged per row during streaming (MEMBERSHIP/NUM_VALUE_ENTRY_BYTES)"),
    ("exec.rs", "batch_attrs", "HashMap::", 1,
     "select-value map entries charged per row during streaming (entry + value length)"),
    // ---- search_eval.rs -------------------------------------------------
    ("search_eval.rs", "charged_set", "HashSet::", 1,
     "the ChargedSet constructor itself: capacity pre-charged before with_capacity"),
    ("search_eval.rs", "aggregate_value", ".collect", 2,
     "Vec<f64> buffers covered by the per-trace transients envelope charged before the aggregates loop"),
    ("search_eval.rs", "build_summary", "Vec::with_capacity", 1,
     "attributes buffer at full capacity charged in the envelope before allocation"),
    ("search_eval.rs", "evaluate_batch", "Vec::new", 1,
     "out vec slots covered by each match's size_of<TraceMatch> base charge + overhead"),
    ("search_eval.rs", "evaluate_batch", ".collect", 1,
     "matched_spans ref list covered by the transients envelope (ref width per matched id)"),
    ("search_eval.rs", "evaluate_batch", "Vec::with_capacity", 1,
     "summaries buffer: base charge (take x size_of<SpanSummary>) before the reservation"),
];

fn source(file: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/traces")
        .join(file);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    // Exempt the trailing `mod tests` region (test allocations need no
    // budget). Both files keep their test module last.
    match text.find("mod tests {") {
        Some(idx) => text[..idx].to_string(),
        None => text,
    }
}

/// Blanks `//` comments and string literals so tokens in prose or SQL
/// text never count. Crude by design (no block comments exist in these
/// files — asserted below).
fn blank_comments_and_strings(src: &str) -> String {
    assert!(
        !src.contains("/*"),
        "block comments would need a smarter scanner"
    );
    let mut out = String::with_capacity(src.len());
    let mut chars = src.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '/' if chars.peek() == Some(&'/') => {
                for d in chars.by_ref() {
                    if d == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            '"' => {
                out.push('_');
                while let Some(d) = chars.next() {
                    if d == '\\' {
                        chars.next();
                    } else if d == '"' {
                        break;
                    } else if d == '\n' {
                        out.push('\n');
                    }
                }
            }
            other => out.push(other),
        }
    }
    out
}

/// The enclosing `fn` name per line (last `fn name(` seen).
fn scan(file: &str) -> BTreeMap<(String, String), usize> {
    let blanked = blank_comments_and_strings(&source(file));
    let mut hits: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut current_fn = "<module>".to_string();
    for line in blanked.lines() {
        if let Some(pos) = line.find("fn ") {
            let rest = &line[pos + 3..];
            let name: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() && rest.contains('(') {
                current_fn = name;
            }
        }
        for token in TOKENS {
            // Identifier-boundary check for tokens ending in an ident
            // char (`.collect` must not match `.collect_rows_charged`);
            // tokens ending in `::`/`!` are boundaries already.
            let needs_boundary = token
                .chars()
                .last()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_');
            let count = line
                .match_indices(token)
                .filter(|(at, _)| {
                    if !needs_boundary {
                        return true;
                    }
                    line[at + token.len()..]
                        .chars()
                        .next()
                        .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'))
                })
                .count();
            if count > 0 {
                *hits
                    .entry((current_fn.clone(), token.to_string()))
                    .or_insert(0) += count;
            }
        }
    }
    hits
}

#[test]
fn every_collection_allocation_site_is_on_the_charge_allowlist() {
    let mut drift = String::new();
    for file in ["exec.rs", "search_eval.rs"] {
        let actual = scan(file);
        let expected: BTreeMap<(String, String), usize> = ALLOWLIST
            .iter()
            .filter(|(f, _, _, count, _)| *f == file && *count > 0)
            .map(|(_, func, token, count, _)| ((func.to_string(), token.to_string()), *count))
            .collect();
        if actual != expected {
            drift.push_str(&format!("---- {file}: actual sites ----\n"));
            for ((func, token), count) in &actual {
                drift.push_str(&format!(
                    "    (\"{file}\", \"{func}\", \"{token}\", {count}, \"<document the charge site>\"),\n"
                ));
            }
        }
    }
    assert!(
        drift.is_empty(),
        "allocation sites drifted from the pinned allowlist.\n\
         A new collection allocation needs a budget charge BEFORE it (docs: \
         the module's allocation-charge audit table) and an allowlist entry \
         documenting that charge.\n{drift}"
    );
}
