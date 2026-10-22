//! Issue #475: the scope vocabulary the trace WRITER catalogues and the
//! scope constant the READER serves are one vocabulary, asserted as an
//! EQUALITY rather than as two presence checks.
//!
//! Why an equality and not two `contains` assertions. The defect this
//! guards is drift in either direction, and each direction is invisible
//! to the other check: a scope the writer starts cataloguing that the
//! reader does not list becomes rows no lookup can reach, and a scope the
//! reader lists that the writer never catalogues becomes an advertised
//! scope with no rows. Set equality is the only form that fails on both.
//!
//! Both halves are LINKED values: `pulsus-server` depends on both
//! `pulsus-read` and `pulsus-write`. The writer half is every
//! `pulsus_write::TagScope` variant, listed through an exhaustive `match`,
//! so a variant added to the enum fails to compile here until it is listed.

use std::collections::BTreeSet;

use pulsus_read::traces::spans::tags::ATTR_SCOPES;
use pulsus_write::TagScope;

/// Every scope the trace writer catalogues, by its stored spelling.
fn writer_scopes() -> BTreeSet<&'static str> {
    let all = [
        TagScope::Span,
        TagScope::Resource,
        TagScope::Event,
        TagScope::Link,
        TagScope::Instrumentation,
    ];
    for scope in all {
        // Exhaustive on purpose: a new variant is a compile error here.
        match scope {
            TagScope::Span
            | TagScope::Resource
            | TagScope::Event
            | TagScope::Link
            | TagScope::Instrumentation => {}
        }
    }
    all.iter().map(|s| s.as_str()).collect()
}

#[test]
fn the_writer_scope_vocabulary_equals_the_reader_scope_constants() {
    let writer = writer_scopes();
    assert_eq!(writer.len(), 5, "five distinct spellings; found {writer:?}");
    let reader: BTreeSet<&'static str> = ATTR_SCOPES.iter().copied().collect();
    assert_eq!(reader.len(), ATTR_SCOPES.len(), "no repeated reader scope");
    assert_eq!(
        writer, reader,
        "writer scope vocabulary vs reader scope constants"
    );
}
