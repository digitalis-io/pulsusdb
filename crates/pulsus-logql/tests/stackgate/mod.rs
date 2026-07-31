//! Shared paired pinned-stack harness (issue #272).
//!
//! A module under `tests/<dir>/` is not itself a test target; it is
//! `#[path]`-included by the binaries that need it (repo precedent:
//! `crates/pulsus-server/tests/support/manifest.rs`,
//! `crates/pulsus-read/tests/logqltest/mod.rs`). Included **only** from
//! top-level `tests/*.rs`, where the base directory is `tests/`.
//!
//! **The sentence each gate asserts, in full, and nothing wider:** *this
//! walk completes over an `N`-node tree on an `S`-byte stack, and
//! `(S, N)` is inside the overflow regime for a per-node-recursive walk
//! of the same shape over the same tree.* It does **not** assert that no
//! recursion exists anywhere in the walker — that claim belongs to the
//! compiler (`Child`/`ChildVec` implement nothing, so per-child dispatch
//! cannot be written), never here.
//!
//! The control is a **duplicated** mirror, not a reused one: the drop
//! oracles' mirrors live in in-crate `#[cfg(test)] mod`s, which do not
//! exist when an integration-test crate compiles the lib. The
//! duplication is counted rather than discovered, and every gate asserts
//! shape agreement between the real tree and its mirror before the stack
//! legs run.

#![allow(dead_code)]

use std::process::Command;

/// Child-mode dispatch env var. When set, the child entry point runs the
/// named mode and every parent test returns early so the child never
/// re-forks.
pub const CHILD_ENV: &str = "PULSUS_WALK_STACK_CHILD";

pub fn child_mode() -> Option<String> {
    std::env::var(CHILD_ENV).ok()
}

/// Runs `f` on a thread whose stack is pinned to exactly `bytes`. An
/// overflow of that stack aborts the whole process — which is why every
/// control leg runs in an out-of-process child.
pub fn on_stack(bytes: usize, f: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(bytes)
        .spawn(f)
        .expect("spawn pinned-stack thread")
        .join()
        .expect("pinned-stack thread panicked");
}

fn spawn_child(child_test: &str, mode: &str) -> std::process::Output {
    let exe = std::env::current_exe().expect("current_exe");
    Command::new(exe)
        .args([child_test, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, mode)
        .output()
        .expect("spawn child test process")
}

/// The positive leg: the converted walk completes on the pinned stack.
///
/// Also asserts the child reported **exactly one test run**, so a
/// mis-typed name filter cannot silently run the whole suite (or nothing)
/// in the child and pass.
pub fn assert_child_ok(child_test: &str, mode: &str) {
    let out = spawn_child(child_test, mode);
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success(),
        "child mode {mode:?} failed with {}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        out.status,
    );
    assert!(
        stdout.contains("1 passed"),
        "child mode {mode:?} did not run exactly one test\n--- stdout ---\n{stdout}"
    );
}

/// The control leg: a per-node-recursive walk of the same shape over the
/// same tree must **overflow** on the same pinned stack. This is a
/// **non-vacuity** claim — it proves `(S, N)` is inside the overflow
/// regime — not a frame-size equality claim against the real walker.
pub fn assert_child_overflowed(child_test: &str, mode: &str) {
    let out = spawn_child(child_test, mode);
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        !out.status.success(),
        "control mode {mode:?} was expected to overflow but exited successfully\n{stderr}"
    );
    assert!(
        stderr.contains("stack overflow"),
        "control mode {mode:?} failed without a stack overflow: {} \n{stderr}",
        out.status
    );
}

// ---------------------------------------------------------------------
// SCC-2's duplicated mirror
// ---------------------------------------------------------------------

/// A plain `Box`-child mirror of `MetricExpr` with the same variant
/// arities. The control's per-node recursion runs over this, so the
/// overflow regime is measured on the shape the real walker walks.
pub enum MeShadow {
    Leaf,
    One(Box<MeShadow>),
    Two(Box<MeShadow>, Box<MeShadow>),
    /// `MetricExpr::Variants` -> `VariantsExpr` -> N children.
    Var(Box<VeShadow>),
}

pub struct VeShadow {
    pub variants: Vec<MeShadow>,
}

pub fn shadow_arity(n: &MeShadow) -> usize {
    match n {
        MeShadow::Leaf => 0,
        MeShadow::One(_) | MeShadow::Var(_) => 1,
        MeShadow::Two(..) => 2,
    }
}

/// A left-deep `Two` spine of `n` branching nodes — the shape a flat
/// `a or b or c …` chain parses into.
pub fn build_me_shadow(n: usize) -> MeShadow {
    let mut e = MeShadow::Leaf;
    for _ in 0..n {
        e = MeShadow::Two(Box::new(e), Box::new(MeShadow::Leaf));
    }
    e
}

/// The control: one frame per node. Deliberately `#[inline(never)]` so a
/// release build cannot turn the recursion into a loop and make the
/// control vacuous.
#[inline(never)]
pub fn walk_me_shadow_recursive(n: &MeShadow, depth: usize) -> usize {
    match n {
        MeShadow::Leaf => depth,
        MeShadow::One(inner) => walk_me_shadow_recursive(inner, depth + 1),
        MeShadow::Two(l, r) => {
            let a = walk_me_shadow_recursive(l, depth + 1);
            let b = walk_me_shadow_recursive(r, depth + 1);
            a.max(b)
        }
        MeShadow::Var(v) => v
            .variants
            .iter()
            .map(|c| walk_me_shadow_recursive(c, depth + 1))
            .max()
            .unwrap_or(depth),
    }
}

/// Drops the mirror without the recursion the control measures — the
/// mirror has no `impl Drop`, so a deep one would abort on the way out
/// of the child even when the leg passed.
pub fn dismantle_me_shadow(mut n: MeShadow) {
    loop {
        n = match n {
            MeShadow::Two(l, r) => {
                drop(r);
                *l
            }
            MeShadow::One(inner) => *inner,
            MeShadow::Var(_) | MeShadow::Leaf => return,
        };
    }
}
