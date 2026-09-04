//! Issue #510 — the read side's `val_type` spellings, sealed against the
//! writer's.
//!
//! `trace_attrs_idx.val_type` is written by
//! `pulsus_write::ingest::traces::AttrValueType::as_str` and read by
//! `pulsus_read::StoredType::from_stored`. The two live in different
//! crates and share nothing but four string literals, so a rename on
//! either side is a silent typing regression: every value would fall to
//! `StoredType::Unknown`, every attribute would render in the pre-#510
//! arm, and no compiler error would say so.
//!
//! **The seal is an EXHAUSTIVE match over the writer's enum**, not a list
//! of four names transcribed here. Adding a variant to `AttrValueType`
//! stops this file compiling until the new spelling is mapped, which is
//! the case a hand-written list cannot see: a fifth OTLP kind would
//! otherwise be written to the column, read back as `Unknown`, and
//! rendered in the fallback arm with nothing red.
//!
//! Hermetic — no ClickHouse, no container. `pulsus-write` is a DEV-only
//! dependency of this crate (see `Cargo.toml`), so this test does not put
//! it on the production graph.

use pulsus_read::StoredType;
use pulsus_write::ingest::traces::AttrValueType;

/// Every writer kind, its stored spelling, and the read-side type that
/// spelling must resolve to.
///
/// The `match` is what makes this exhaustive: `AttrValueType` has no
/// `_` arm here, so the compiler enumerates the domain rather than this
/// file claiming to.
fn read_side_of(kind: AttrValueType) -> (&'static str, StoredType) {
    match kind {
        AttrValueType::String => ("string", StoredType::String),
        AttrValueType::Int => ("int", StoredType::Int),
        AttrValueType::Float => ("float", StoredType::Float),
        AttrValueType::Bool => ("bool", StoredType::Bool),
    }
}

/// Every kind the writer can store reads back as the matching read-side
/// type, through the writer's OWN spelling rather than a copy of it.
///
/// *RED when:* either side renames a spelling. The writer's `as_str` is
/// the input, so a rename there changes what `from_stored` is handed and
/// the assertion fails on the pair that moved.
#[test]
fn every_written_value_type_reads_back_as_its_own_kind() {
    for kind in [
        AttrValueType::String,
        AttrValueType::Int,
        AttrValueType::Float,
        AttrValueType::Bool,
    ] {
        let (spelling, want) = read_side_of(kind);
        assert_eq!(
            kind.as_str(),
            spelling,
            "the writer's stored spelling for {kind:?} moved; the read side maps {spelling:?}"
        );
        assert_eq!(
            StoredType::from_stored(kind.as_str()),
            want,
            "{kind:?} is written as {:?} and must read back as {want:?}",
            kind.as_str()
        );
    }
}

/// A row written before the column existed reads back `''`, and that is
/// the ONLY value that may resolve to `Unknown`.
///
/// The `Unknown` arm is deliberately unreachable end to end — every row a
/// test or a live ingest writes carries a type — so it is exercised here
/// and claimed nowhere else. An unrecognised non-empty spelling also
/// lands on `Unknown` rather than panicking: a read must not fail on data
/// a future writer stored.
#[test]
fn only_an_unrecognised_spelling_reads_back_unknown() {
    assert_eq!(StoredType::from_stored(""), StoredType::Unknown);
    for other in ["INT", "Int", "integer", "double", "bytes", "null", " int"] {
        assert_eq!(
            StoredType::from_stored(other),
            StoredType::Unknown,
            "{other:?} is not a spelling the writer produces"
        );
    }
}
