//! Shared, hermetic Rust-source scanning primitives for this crate's
//! source-scan guards: workspace-root resolution, `.rs` file enumeration,
//! and the one comment/string-literal lexer both guards preprocess with.
//!
//! Included by `route_inventory.rs` (issue #36's route-drift guard) and
//! `live_port_uniqueness.rs` (the live-suite fixed-port guard) via
//! `#[path = "support/source_scan.rs"] mod source_scan;` — a `tests/`
//! subdirectory, so cargo never builds this file as its own test binary
//! (same layout as `support/manifest.rs`).
//!
//! There is deliberately ONE lexer here rather than one per guard: a
//! second, hand-rolled comment stripper is exactly the defect
//! `preprocess_views`' own doc comment records being fixed (a raw string
//! containing `//` desynchronized a two-stage pipeline). A guard that
//! mis-lexes its input reports a clean pass over the wrong text.

#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root resolves")
}

pub fn rs_files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk(dir, &mut out);
    out.sort();
    out
}

pub fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Byte offset -> 1-based line number, against `src`.
pub fn line_of(src: &str, byte_offset: usize) -> usize {
    src.as_bytes()[..byte_offset.min(src.len())]
        .iter()
        .filter(|&&b| b == b'\n')
        .count()
        + 1
}

/// Advances past a Rust comment or string/char/raw/byte-string literal
/// starting at byte `i`, if `i` is the start of one — returns the index
/// just past it, so a brace/paren *inside* a literal (a `.route(` call's
/// JSON fixture strings are full of unbalanced `{`/`}` split across
/// several `push_str` literals — very real in this codebase's own
/// `#[cfg(test)]` blocks) is never mistaken for a structural token. `None`
/// when `i` starts none of these (including a lifetime like `'a`, which
/// looks like the start of a char literal but never closes).
pub fn skip_literal_or_comment(bytes: &[u8], i: usize) -> Option<usize> {
    match bytes.get(i) {
        Some(b'/') if bytes.get(i + 1) == Some(&b'/') => {
            let mut j = i + 2;
            while j < bytes.len() && bytes[j] != b'\n' {
                j += 1;
            }
            Some(j)
        }
        Some(b'/') if bytes.get(i + 1) == Some(&b'*') => {
            let mut depth = 1i32;
            let mut j = i + 2;
            while j < bytes.len() && depth > 0 {
                if bytes[j] == b'/' && bytes.get(j + 1) == Some(&b'*') {
                    depth += 1;
                    j += 2;
                } else if bytes[j] == b'*' && bytes.get(j + 1) == Some(&b'/') {
                    depth -= 1;
                    j += 2;
                } else {
                    j += 1;
                }
            }
            Some(j)
        }
        Some(b'"') => Some(skip_quoted(bytes, i + 1)),
        Some(b'\'') => skip_char_literal(bytes, i),
        Some(b'r') => skip_raw_string(bytes, i),
        Some(b'b') => match bytes.get(i + 1) {
            Some(b'"') => Some(skip_quoted(bytes, i + 2)),
            Some(b'\'') => skip_char_literal(bytes, i + 1),
            Some(b'r') => skip_raw_string(bytes, i + 1),
            _ => None,
        },
        _ => None,
    }
}

/// `j` starts just past the opening `"`; returns the index just past the
/// matching (backslash-escape-aware) closing `"`.
pub fn skip_quoted(bytes: &[u8], mut j: usize) -> usize {
    while j < bytes.len() && bytes[j] != b'"' {
        if bytes[j] == b'\\' {
            j += 1;
        }
        j += 1;
    }
    (j + 1).min(bytes.len())
}

/// `i` points at the opening `'`. Only a genuine char literal (`'c'`,
/// `'\n'`, `'\''`, `'\u{2603}'`, ...) closes within a short lookahead;
/// a lifetime (`'a`) never does, and is left unconsumed (`None`) so the
/// caller advances past just the `'` normally.
pub fn skip_char_literal(bytes: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    if bytes.get(j) == Some(&b'\\') {
        j += 1;
        if bytes.get(j) == Some(&b'u') && bytes.get(j + 1) == Some(&b'{') {
            j += 2;
            while j < bytes.len() && bytes[j] != b'}' {
                j += 1;
            }
            j += 1;
        } else {
            j += 1;
        }
    } else {
        j += 1;
    }
    if bytes.get(j) == Some(&b'\'') {
        Some(j + 1)
    } else {
        None
    }
}

/// `i` points at `r` (or, via the `b` case above, the `r` of `br"..."`).
/// Handles any hash count (`r"..."`, `r#"..."#`, `r##"..."##`, ...).
pub fn skip_raw_string(bytes: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    let mut hashes = 0usize;
    while bytes.get(j) == Some(&b'#') {
        hashes += 1;
        j += 1;
    }
    if bytes.get(j) != Some(&b'"') {
        return None;
    }
    j += 1;
    loop {
        if j >= bytes.len() {
            return Some(j);
        }
        if bytes[j] == b'"' {
            let mut k = j + 1;
            let mut matched = 0usize;
            while matched < hashes && bytes.get(k) == Some(&b'#') {
                matched += 1;
                k += 1;
            }
            if matched == hashes {
                return Some(k);
            }
        }
        j += 1;
    }
}

/// One single lexer pass over `src` emitting **both** preprocessed views
/// simultaneously (round-10 adjudication: the previous two-stage pipeline
/// — a hand-rolled comment stripper with its own ordinary-quote loop,
/// followed by a separate lexer-based literal blanker — could
/// desynchronize on a raw string containing `//`, e.g.
/// `r#"{"url":"https://x"}"#`: stage one's quote loop mis-lexed the raw
/// string's delimiters, exposed the interior `//` as a "comment", and
/// blanked the closing delimiter plus real code after it. One pass over
/// the one shared [`skip_literal_or_comment`] lexer makes a stage desync
/// structurally impossible — there is no second stage to disagree with
/// the first):
///
/// - `.0` (`stripped`): comments blanked, string/char literals intact —
///   route-path extraction's view (literals are exactly what it extracts).
///   Doc comments (`///`, `//!`) are ordinary comments to the lexer and
///   are blanked too — load-bearing: this codebase's doc comments
///   illustrate real code with snippets like `.merge(authed)` that a
///   comment-blind scan would mistake for real call sites.
/// - `.1` (`blanked`): comments AND string/char literals blanked — every
///   policy pass's view (a token inside a log message/format string must
///   never trip a policy check).
///
/// Both views are byte-length-identical to `src` (non-newline bytes
/// replaced with spaces), so offsets/line numbers stay valid across all
/// three.
pub fn preprocess_views(src: &str) -> (String, String) {
    let bytes = src.as_bytes();
    let mut stripped = bytes.to_vec();
    let mut blanked = bytes.to_vec();
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(next) = skip_literal_or_comment(bytes, i) {
            let end = next.min(bytes.len());
            let is_comment = bytes[i] == b'/';
            for j in i..end {
                if bytes[j] != b'\n' {
                    if is_comment {
                        stripped[j] = b' ';
                    }
                    blanked[j] = b' ';
                }
            }
            i = end;
        } else {
            i += 1;
        }
    }
    (
        String::from_utf8(stripped).expect("blanking preserves UTF-8 validity"),
        String::from_utf8(blanked).expect("blanking preserves UTF-8 validity"),
    )
}

/// Every `#[cfg(test)]\nmod <ident> { ... }` block's inclusive byte span
/// in `stripped` (the comment-blanked, literals-intact view — comments
/// are already gone, so a `#[cfg(test)]` inside a doc comment cannot
/// false-match; the brace-depth scan skips literals via
/// [`skip_literal_or_comment`], since this codebase's own test modules
/// build JSON fixture strings with plenty of unbalanced `{`/`}` inside
/// string literals). The caller blanks these spans in *both* views, so
/// test-only code is invisible to route extraction and policy passes
/// alike (plan v2 finding 1: "the only `.route(` calls in
/// `pulsus-write/src/ingest/http.rs` today are test-only routers" — this
/// is what excludes them).
pub fn cfg_test_mod_spans(stripped: &str) -> Vec<(usize, usize)> {
    let marker = "#[cfg(test)]";
    let bytes = stripped.as_bytes();
    let mut spans = Vec::new();
    let mut search_from = 0usize;
    while let Some(rel) = stripped[search_from..].find(marker) {
        let attr_start = search_from + rel;
        let Some(brace_rel) = stripped[attr_start..].find('{') else {
            break;
        };
        let open = attr_start + brace_rel;
        let between = &stripped[attr_start + marker.len()..open];
        if !between.trim_start().starts_with("mod ") {
            // Not a `#[cfg(test)] mod ... {` shape (e.g. `#[cfg(test)]` on
            // a single item) — this codebase does not use that pattern
            // today; skip past this attribute rather than mis-stripping.
            search_from = attr_start + marker.len();
            continue;
        }
        let mut depth = 0i32;
        let mut close = None;
        let mut i = open;
        while i < bytes.len() {
            if let Some(next) = skip_literal_or_comment(bytes, i) {
                i = next;
                continue;
            }
            match bytes[i] {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        close = Some(i);
                        break;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        let close = close.unwrap_or_else(|| {
            panic!(
                "unterminated #[cfg(test)] mod block starting at line {}",
                line_of(stripped, attr_start)
            )
        });
        spans.push((attr_start, close));
        search_from = close + 1;
    }
    spans
}

/// Blanks each inclusive `(start, end)` span in `text` (non-newline bytes
/// to spaces), preserving byte offsets.
pub fn blank_spans(text: String, spans: &[(usize, usize)]) -> String {
    let mut bytes = text.into_bytes();
    for &(start, end) in spans {
        for b in &mut bytes[start..=end] {
            if *b != b'\n' {
                *b = b' ';
            }
        }
    }
    String::from_utf8(bytes).expect("blanking preserves UTF-8 validity")
}
