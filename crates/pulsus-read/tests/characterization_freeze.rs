//! Issue #272 AC 5 — the plan-side content-hash freeze.
//!
//! Same disposition as `pulsus-logql`'s: the golden records what the
//! derived impls produced before the conversion, and what
//! `CompiledPipeline`'s `Debug` produced before Wave 2 flattens
//! `CompiledLabelFilter`. It cannot be refreshed silently.

use sha2::{Digest, Sha256};

const GOLDEN: &str = include_str!("golden/plan_walk_characterization.txt");
const PINNED: &str = include_str!("golden/plan_walk_characterization.sha256");

/// Issue #293's differential golden, captured from the RECURSIVE
/// `build_metric_node` at `ae66648` and replayed against the iterative
/// one by `logql_plan_build_differential.rs`. Freezing it is what stops
/// a conversion defect being "fixed" by regenerating the oracle.
const BUILD_GOLDEN: &str = include_str!("golden/plan_build_differential.txt");
const BUILD_PINNED: &str = include_str!("golden/plan_build_differential.sha256");

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

#[test]
fn the_plan_build_differential_golden_matches_its_committed_digest() {
    let digest = Sha256::digest(BUILD_GOLDEN.as_bytes());
    assert_eq!(
        format!("{digest:x}"),
        BUILD_PINNED.trim(),
        "crates/pulsus-read/tests/golden/plan_build_differential.txt was edited without its \
         digest (issue #293). This golden is the PRE-conversion planner's output; regenerating \
         it against the post-conversion planner destroys the differential rather than fixing it."
    );
}
