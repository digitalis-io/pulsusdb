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
    ("exec.rs", "collect_rows_charged", "Vec::new", 1,
     "the row vec fills only through the per-row cost closure charge (charged as it streams)"),
    ("exec.rs", "list_tag_names", "Vec::new", 1,
     "tag-names catalog read (issue #58) - outside the search budget by design (no ByteBudget on this path); hard-bounded by the SQL LIMIT to TAG_NAMES_MAX + 1 short catalog rows"),
    ("exec.rs", "stream_tag_values", "Vec::new", 1,
     "tag-values read (issues #58 and #478) - same class as list_tag_names; hard-bounded by the SQL LIMIT to TAG_VALUES_MAX + 1 rows, on BOTH shapes (the catalog one and the q-narrowed index one), because the one streaming helper is what both go through"),
    ("exec.rs", "list_span_name_values", "Vec::new", 1,
     "span-name values read (issue #478) - same class again: outside the search budget by design (no ByteBudget on this path), hard-bounded by the SQL LIMIT to TAG_VALUES_MAX + 1 rows, each at most the 8192-byte string-column cap"),
    ("exec.rs", "new", "Vec::new", 1,
     "the engine's indexed attributes (issue #595 part 2): an empty Vec::new() at construction, once per engine and never per query - nothing to charge"),
    ("exec.rs", "frame_range_series", "Vec::new", 4,
     "metrics range series/samples (issue #59/#182) - outside the search ByteBudget by design (no ByteBudget on this path); hard-bounded by MAX_METRICS_POINTS + 1 emitted buckets x the series count (grouped series pre-capped by reader.traceql_max_series via the distinct-by-key probe; the quantile series count is the requested quantile list, and since issue #252 the histogram's is data-dependent but hard-bounded by the bit width of Int64 (63 reachable buckets)). Issue #477 split this function out of frame_range so the densify post-pass has its own charge row below; the bound is unchanged"),
    ("exec.rs", "frame_range_series", ".collect", 3,
     "same bound as frame_range_series Vec::new: quantile per-series init, the issue #252 histogram's per-bucket tally map -> series map, and grouped by-partition framing, all bounded by the fixed/probe-capped/64-bucket series count x MAX_METRICS_POINTS + 1 buckets"),
    ("exec.rs", "densify", "Vec::with_capacity", 1,
     "issue #477: the densified sample vector, allocated at exactly RangeAxis::points, which the planner capped at MAX_METRICS_POINTS + 1 before any SQL ran - the fill is computed from the window and the step, so it reads no row and adds no scan; same metrics carve-out as frame_range_series"),
    ("exec.rs", "densify", ".collect", 1,
     "same bound as densify Vec::with_capacity: the framed (label -> value) lookup map, bounded by the already-materialized sample count of the series being densified"),
    ("exec.rs", "frame_instant", "Vec::new", 2,
     "metrics instant series/samples - one sample per series; series count fixed (quantile), <= 63 reachable buckets (issue #252 histogram) or probe-capped (grouped), same design carve-out as frame_range_series"),
    ("exec.rs", "frame_instant", ".collect", 2,
     "same bound as frame_instant Vec::new: per-series instant framing over the fixed/probe-capped/64-bucket series count"),
    ("exec.rs", "apply_series_reduce", ".collect", 2,
     "topk/bottomk client-side reduction (issue #182 P5) over the ALREADY-materialized (probe-capped) series set; the per-timestamp rank/keep buffers are bounded by that series count"),
    ("exec.rs", "attach_range_exemplars", "Vec::new", 1,
     "exemplar list (issue #182 P5) - bounded by the total exemplar budget (issue #477: MAX_EXEMPLARS for the whole response, thinned to it after collection), and DURING collection by EXEMPLAR_COLLECTION_CEILING, which decimate_if_full enforces by halving the list in place (issue #477 wave 2: the statement now returns one row per bucket AND series identity - a log2 duration bucket, or a comparison side and attribute key - so the row count is no longer bounded by the bucket grid alone); outside the search ByteBudget by design"),
    ("exec.rs", "attach_range_exemplars", ".collect", 5,
     "issue #477 wave 2: the per-series value-at-bucket lookup maps (one Vec of BTreeMaps over the ALREADY-materialized series, each bounded by MAX_METRICS_POINTS buckets) plus ONE series-identity index per shape - group value, log2 bucket bound, or (meta type, attribute key) - each bounded by the same probe-capped series count. Exactly one of the three index maps is built per query (the match arms are exclusive); all are built from the framed answer and read no row; same metrics carve-out"),
    ("exec.rs", "thin_collected_exemplars", ".collect", 1,
     "issue #477: the even-stride reduction of an already-materialized exemplar list down to the total exemplar budget - the kept vector is bounded by that budget (MAX_EXEMPLARS), strictly smaller than the list it replaces, and the stride is taken over the WHOLE response so a grouped answer cannot multiply the budget by its series count; same metrics carve-out"),
    ("exec.rs", "frame_compare", "Vec::new", 7,
     "compare() meta-series (issue #182 P6b) - outside the search ByteBudget by design; the cross-tab (key,value) cardinality is pre-capped by reader.traceql_max_series via the distinct-(key,value) probe (enforce_series_cap runs it first), and the per-key nil/total buffers are bounded by the bucket count (MAX_METRICS_POINTS). Issue #460 adds the two topN rank-and-keep buffers (rank_base/rank_sel), allocated ONCE outside the per-key loop and cleared per key, each bounded by that key's distinct value count and so by the same probe cap"),
    ("exec.rs", "frame_compare", ".collect", 4,
     "same bound as frame_compare Vec::new: per-(key,value) baseline/selection sample vectors + the fixed well-known-absent key=nil/total vectors (WELL_KNOWN_COMPARE_KEYS, a bounded 25-key constant), all bounded by the probe-capped series count x MAX_METRICS_POINTS buckets"),
    ("exec.rs", "keep_top_n", ".collect", 1,
     "compare()'s topN kept-value set (issue #460): built once per attribute PER SIDE and dropped before the next attribute, bounded by min(top_n, that attribute's distinct value count) - and that distinct count is itself pre-capped by reader.traceql_max_series via the distinct-(key,value) probe enforce_series_cap runs first; same metrics carve-out from the search ByteBudget as frame_compare"),
    ("exec.rs", "hex16", "String::with_capacity", 1,
     "a single 32-char trace-id hex string per exemplar (bounded set, see attach_range_exemplars); metrics carve-out"),
    ("exec.rs", "hex16", "format!", 1,
     "per-byte hex formatting of the 16-byte trace id (32 chars total); metrics carve-out"),
    ("exec.rs", "service_graph", "Vec::new", 1,
     "service-graph edges (issue #173) - same class as list_tag_names/metrics_range: outside the search ByteBudget by design (no ByteBudget on this path); hard-bounded by the SQL LIMIT to SERVICE_GRAPH_MAX_EDGES + 1 rows"),
    // ---- values.rs: the value types moved out of the deleted search_eval.rs ----
    ("values.rs", "new", "HashSet::", 1,
     "GroupCardinalityCounter::new: the empty distinct-group set (HashSet::new allocates nothing; each distinct tuple is charged group_tuple_bytes in observe() BEFORE insert, released on the success path)"),
    ("values.rs", "go_duration_frac", "Vec::new", 1,
     "the fractional-digit scratch buffer (<= 9 ASCII bytes) for a by(duration) group value's Go-duration render - a bounded scalar render whose result .len() is charged by charged_str at the group-value site before retention (the same residual class as build_summary's duration/status/kind scalar renders)"),
    ("values.rs", "go_duration_string", "format!", 5,
     "bounded duration-string assembly (<= ~32 bytes, e.g. '2540400h10m10.000000000s') for a by(duration)/by(traceDuration) group value; charged_str charges its .len() before it is cloned into the retained tuple/attributes - a scalar-render residual"),
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
    for file in ["exec.rs", "values.rs"] {
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

/// AC10(i-b), second half (issue #477): no charge rationale still
/// describes a PER-BUCKET exemplar cap.
///
/// This is a prose check and it exists because prose is the residue the
/// compiler cannot reach. Renaming the constants moves every code
/// occurrence; the sentence in this table explaining what bounds the
/// exemplar list is not a code occurrence, and it said the wrong thing
/// until issue #477 changed the unit under it.
#[test]
fn no_charge_rationale_describes_a_per_bucket_exemplar_cap() {
    for (file, func, token, _n, charge) in ALLOWLIST {
        let low = charge.to_ascii_lowercase();
        assert!(
            !charge.contains("EXEMPLARS_PER_BUCKET"),
            "{file}/{func}/{token}: names a constant that no longer exists"
        );
        assert!(
            !low.contains("per-bucket exemplar") && !low.contains("per_bucket exemplar"),
            "{file}/{func}/{token}: describes a per-bucket exemplar bound, and the bound is a total"
        );
    }
    let (_, _, _, _, charge) = ALLOWLIST
        .iter()
        .find(|(_, f, t, _, _)| *f == "attach_range_exemplars" && *t == "Vec::new")
        .expect("the exemplar allocation row exists");
    assert!(
        charge.contains("total exemplar budget"),
        "the exemplar list's rationale must state the bound it actually has: {charge}"
    );
}
