//! Issue #475: the scope vocabulary the trace WRITER emits and the scope
//! constants the READER serves are one vocabulary, asserted as an
//! EQUALITY rather than as two presence checks.
//!
//! Why an equality and not two `contains` assertions. The defect this
//! guards is drift in either direction, and each direction is invisible
//! to the other check: a scope the writer starts emitting that the reader
//! does not list becomes rows no lookup can reach, and a scope the reader
//! lists that the writer never emits becomes an advertised scope with no
//! rows. Set equality is the only form that fails on both.
//!
//! The reader half is taken from the LINKED constants, not from text:
//! `pulsus-server` depends on both `pulsus-read` and `pulsus-write`
//! (`crates/pulsus-server/Cargo.toml`), so this suite can name
//! `ATTR_SCOPES`/`RESERVED_INTRINSIC_SCOPES` directly and only the
//! writer half needs scanning. Textual is required on that half and only
//! that half: a newly added `const SCOPE_*` is invisible to any exported
//! array, which is exactly the drift being guarded.
//!
//! **Stated boundary.** The scan sees a `const SCOPE_*: &str = "…";`
//! declaration in one file. It cannot see a scope string written inline
//! at an emission site, and it cannot see a scope emitted from another
//! file — `AttrRecord` is constructed in exactly one production file
//! today (`git grep -n 'AttrRecord' -- crates`), and if trace ingest ever
//! grows a second parser this gate goes quietly incomplete.

use std::collections::BTreeSet;

use pulsus_read::traces::tags_sql::{ATTR_SCOPES, RESERVED_INTRINSIC_SCOPES};

#[path = "support/source_scan.rs"]
mod source_scan;

use source_scan::{blank_spans, cfg_test_mod_spans, preprocess_views, workspace_root};

/// Every `const SCOPE_<NAME>: &str = "<value>";` the trace writer
/// declares, read through the shared lexer (comments blanked,
/// `#[cfg(test)] mod` blocks blanked) so a scope named in a doc comment
/// or a test fixture cannot enter the set.
fn writer_scope_literals() -> BTreeSet<String> {
    let path = workspace_root().join("crates/pulsus-write/src/protocols/otlp_traces.rs");
    let src = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    let (stripped, _blanked) = preprocess_views(&src);
    let spans = cfg_test_mod_spans(&stripped);
    let stripped = blank_spans(stripped, &spans);

    let mut out = BTreeSet::new();
    for line in stripped.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("const SCOPE_") else {
            continue;
        };
        let Some((_name, tail)) = rest.split_once(": &str = ") else {
            continue;
        };
        let value = tail
            .trim_end_matches(';')
            .trim_matches('"')
            .trim()
            .to_string();
        assert!(
            !value.is_empty() && !value.contains('"'),
            "unparsed scope declaration: {line}"
        );
        assert!(
            out.insert(value.clone()),
            "two scope constants carry the same value {value:?}"
        );
    }
    assert!(
        !out.is_empty(),
        "the scan found no scope constants — it is matching nothing, not passing"
    );
    out
}

#[test]
fn the_writer_scope_vocabulary_equals_the_reader_scope_constants() {
    let writer = writer_scope_literals();
    assert_eq!(
        writer.len(),
        7,
        "the writer declares seven scope constants; found {writer:?}"
    );

    let reader: BTreeSet<String> = ATTR_SCOPES
        .iter()
        .chain(RESERVED_INTRINSIC_SCOPES.iter())
        .map(|s| (*s).to_string())
        .collect();
    assert_eq!(
        reader.len(),
        ATTR_SCOPES.len() + RESERVED_INTRINSIC_SCOPES.len(),
        "the reader's two scope lists must be disjoint"
    );

    assert_eq!(
        writer, reader,
        "writer scope vocabulary vs reader scope constants"
    );
}
