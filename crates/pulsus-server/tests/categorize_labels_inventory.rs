//! Issue #463 — the enumeration behind the categorised wire shape's
//! all-or-nothing switch, turned into a check.
//!
//! Hermetic — no server, no ClickHouse. Runs under the plain `ci` job's
//! `cargo test --workspace` / `cargo nextest run --workspace`.
//!
//! ## What this closes, and what it does not
//!
//! The categorised `values` shape is all-or-nothing: a three-element
//! entry in a body that does not advertise `categorize-labels`
//! desynchronises the datasource's streaming decoder, and so does a
//! two-element entry in one that does. The design makes that unreachable
//! by construction — the decision has one implementation behind a
//! private-field type, and the envelope bytes and the per-item renderer
//! come out of ONE call to it — but "one call" is only true while nobody
//! adds a second. This file counts.
//!
//! **What it proves:** the call-site counts below, exactly. Nothing more.
//! In particular it does NOT prove that no bypass renderer exists — a
//! second manual renderer added *inside* an already-scanned file changes
//! no count here, and generated or `include!`d same-crate source outside
//! `src` is invisible to a `src` scan. That property is carried instead
//! by the checks that read the shipped bytes: the header table exercised
//! end to end at both handlers, the arity matrix on both envelopes, the
//! capture replay, and the advertisement-placement gate. A bypass that
//! ships bytes changes one of those; a bypass that ships no bytes is not
//! a bug.
//!
//! The enumeration on the read side — the `StreamResult` construction
//! sites — matters for a different reason: a constructor that fails to
//! populate `categories` yields `WireArity::Two`, which turns the whole
//! response's decision off. That is a downgrade, never a mixed body, so a
//! fifth constructor cannot break a client; it can only lose the feature
//! silently, which is what the count is here to stop.

#[path = "support/source_scan.rs"]
mod source_scan;

use std::path::{Path, PathBuf};

use source_scan::{line_of, preprocess_views, rs_files_under, workspace_root};

/// Counts non-overlapping occurrences of `needle` in the CODE view of
/// every `.rs` file under `dir`, returning `(file, line)` for each.
///
/// **Production scope**: each file is truncated at its `#[cfg(test)] mod
/// tests {` line, so a call from a gate does not count as an emission
/// path. The truncation is exact for this crate's one-test-module-per-
/// file layout and would UNDER-count a production item written after
/// that module — which `logqltest_provenance`'s trailing-region check
/// already forbids across `params.rs`, and which would show up here as a
/// count going DOWN rather than as a silent pass.
fn occurrences(dir: &Path, needle: &str) -> Vec<(String, usize)> {
    let root = workspace_root();
    let mut out = Vec::new();
    for path in rs_files_under(dir) {
        let src = std::fs::read_to_string(&path).expect("read source");
        // The CODE view: comments blanked, string literals kept — a
        // symbol named in a doc comment must not count as a call site,
        // and this file's own prose names every symbol it counts.
        let (stripped, _) = preprocess_views(&src);
        let end = stripped
            .find("#[cfg(test)]\nmod tests {")
            .unwrap_or(stripped.len());
        let stripped = &stripped[..end];
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned();
        let mut from = 0usize;
        while let Some(at) = stripped[from..].find(needle) {
            let abs = from + at;
            out.push((rel.clone(), line_of(&src, abs)));
            from = abs + needle.len();
        }
    }
    out
}

fn server_src() -> PathBuf {
    workspace_root().join("crates/pulsus-server/src")
}

fn read_src() -> PathBuf {
    workspace_root().join("crates/pulsus-read/src")
}

/// **Criterion 8 — the emission paths are enumerated, not remembered.**
///
/// Each count is `definition + call sites`, so the numbers below read as
/// "one definition and N callers".
#[test]
fn the_categorised_emission_paths_are_exactly_the_enumerated_ones() {
    // `render_stream_item_into` — one definition, one production call
    // site (inside `ItemWriter::write`). Every renderer that emits a
    // stream item has to be handed an `ItemWriter`, and `streams_render`
    // is the only source of one.
    let sites = occurrences(&server_src(), "render_stream_item_into(");
    assert_eq!(
        sites.len(),
        2,
        "render_stream_item_into must have exactly one definition and one call site: {sites:?}"
    );

    // `item_chunk` — one definition, two call sites: the `Streams` arm's
    // `stream_array` closure, and the AC-19 allocation probe. The probe
    // is a call site ON PURPOSE: a gate that measures a function nothing
    // else calls measures a transcription of the shipped path.
    let sites = occurrences(&server_src(), "item_chunk(");
    assert_eq!(
        sites.len(),
        2,
        "item_chunk must have one definition and one production call site (the Streams arm's \
         `stream_array` closure): {sites:?}"
    );

    // `streams_render` — one definition, two production call sites, one
    // per `StreamsEnvelope` variant, plus the criterion-16 probes.
    let sites = occurrences(&server_src(), "streams_render(");
    assert_eq!(
        sites.len(),
        3,
        "streams_render must have one definition and exactly two production call sites, one \
         per StreamsEnvelope variant: {sites:?}"
    );

    // `StreamsEnvelope` has exactly two variants: a third response shape
    // carrying log lines has to come through here.
    let src = std::fs::read_to_string(server_src().join("logs_api/encode.rs")).expect("read");
    let (code, _) = preprocess_views(&src);
    let start = code
        .find("enum StreamsEnvelope<'a> {")
        .expect("StreamsEnvelope is defined in logs_api/encode.rs");
    let body = &code[start..start + code[start..].find("\n}").expect("enum closes")];
    let variants: Vec<&str> = ["Query {", "Tail {"]
        .into_iter()
        .filter(|v| body.contains(v))
        .collect();
    assert_eq!(variants.len(), 2, "the two known variants must be present");
    // Any other capitalised item at variant indentation is a third
    // variant.
    for line in body.lines().skip(1) {
        let t = line.trim();
        if t.is_empty() || t.starts_with("//") || t.starts_with('}') {
            continue;
        }
        assert!(
            t.starts_with("Query {")
                || t.starts_with("Tail {")
                || !t.chars().next().is_some_and(|c| c.is_ascii_uppercase()),
            "StreamsEnvelope grew a third variant: {t:?} — it needs its own row in the \
             criterion-4 matrix before it can ship"
        );
    }

    // The `StreamResult` construction sites in the read path. One that
    // forgets `categories` reports `WireArity::Two`, which turns the
    // WHOLE response's decision off — a silent feature loss, which is
    // what this count exists to stop.
    //
    // **Five, not four.** Issue #463 added the fifth: the categorised
    // fast path accumulates into its own label-keyed map, so
    // `FastPathGroups::into_streams` drains two maps rather than one.
    // The four that predate it are the fast path's by-fingerprint
    // insert, the accumulator's by-fingerprint insert, the accumulator's
    // fan-out drain, and the structured-metadata fan-out drain.
    let sites: Vec<(String, usize)> = occurrences(&read_src(), "StreamResult {")
        .into_iter()
        .filter(|(f, line)| {
            // The type definition and its `impl` block are not
            // constructions.
            let src = std::fs::read_to_string(workspace_root().join(f)).expect("read");
            let text = src.lines().nth(line - 1).unwrap_or("");
            !text.contains("struct StreamResult {") && !text.contains("impl StreamResult {")
        })
        .collect();
    assert_eq!(
        sites.len(),
        5,
        "expected exactly five StreamResult construction sites: {sites:?}"
    );
    let files: Vec<&str> = sites.iter().map(|(f, _)| f.as_str()).collect();
    assert_eq!(
        files
            .iter()
            .filter(|f| f.ends_with("logql/exec.rs"))
            .count(),
        4,
        "exec.rs holds four constructors: {sites:?}"
    );
    assert_eq!(
        files
            .iter()
            .filter(|f| f.ends_with("logql/detected_probe.rs"))
            .count(),
        1,
        "detected_probe.rs holds the fan-out drain's constructor: {sites:?}"
    );
}

/// **The decision has one implementation.** `WireArity::Three` is the
/// predicate `categorize::decide` is built on; a renderer that
/// re-derived it would compare against that variant somewhere else.
///
/// This is a LEXICAL check and it is worth exactly what a lexical check
/// is worth: a natural rewording — `!= WireArity::Two`, or a length
/// comparison — slips past it. It is kept because it costs nothing and
/// catches the literal copy-paste; the property it gestures at is
/// carried by `Categorize`'s private field, which the compiler enforces.
#[test]
fn the_wire_arity_predicate_appears_once() {
    let sites = occurrences(&server_src(), "WireArity::Three");
    assert_eq!(
        sites.len(),
        1,
        "the categorised-arity comparison must live only in `categorize::decide`: {sites:?}"
    );
    assert!(
        sites[0].0.ends_with("logs_api/encode.rs"),
        "unexpected location: {sites:?}"
    );
}
