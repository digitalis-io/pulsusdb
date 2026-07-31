//! Issue #272 AC 5 — the content-hash freeze.
//!
//! The characterization golden is a **frozen artifact**: it was captured
//! from the derived impls the conversion replaced, so it is the record of
//! what those impls produced. This guard recomputes its digest and
//! compares it with the committed `.sha256`, so the golden cannot be
//! silently refreshed to accommodate a representation change — an
//! accidental edit reddens a named test instead.

use sha2::{Digest, Sha256};

const GOLDEN: &str = include_str!("golden/ast_walk_characterization.txt");
const PINNED: &str = include_str!("golden/ast_walk_characterization.sha256");

#[test]
fn the_ast_characterization_golden_matches_its_committed_digest() {
    let digest = Sha256::digest(GOLDEN.as_bytes());
    assert_eq!(
        format!("{digest:x}"),
        PINNED.trim(),
        "crates/pulsus-logql/tests/golden/ast_walk_characterization.txt was edited without \
         its digest (issue #272 AC 5). If the change is intended, regenerate BOTH with \
         `--ignored zz_regenerate_golden` and say in the notes what observable byte moved."
    );
}
