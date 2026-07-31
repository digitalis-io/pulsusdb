//! Issue #272 AC 5 — the plan-side content-hash freeze.
//!
//! Same disposition as `pulsus-logql`'s: the golden records what the
//! derived impls produced before the conversion, and what
//! `CompiledPipeline`'s `Debug` produced before Wave 2 flattens
//! `CompiledLabelFilter`. It cannot be refreshed silently.

use sha2::{Digest, Sha256};

const GOLDEN: &str = include_str!("golden/plan_walk_characterization.txt");
const PINNED: &str = include_str!("golden/plan_walk_characterization.sha256");

#[test]
fn the_plan_characterization_golden_matches_its_committed_digest() {
    let digest = Sha256::digest(GOLDEN.as_bytes());
    assert_eq!(
        format!("{digest:x}"),
        PINNED.trim(),
        "crates/pulsus-read/tests/golden/plan_walk_characterization.txt was edited without \
         its digest (issue #272 AC 5). If the change is intended, regenerate BOTH with \
         `--ignored zz_regenerate_golden` and say in the notes what observable byte moved."
    );
}
