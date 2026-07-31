//! Shared paired pinned-stack harness for SCC-3 (issue #272).
//!
//! A second copy of `pulsus-logql`'s harness, deliberately: the two
//! crates' integration tests are separate compilation units and no
//! `#[path]` include reaches across a package boundary. The duplication
//! is counted here rather than discovered later — three copies exist
//! (this one, `pulsus-logql`'s, and `e2e`'s inline one), and each gate
//! asserts shape agreement between the real tree and its mirror before
//! the stack legs run.
//!
//! **The sentence each gate asserts, in full, and nothing wider:** *this
//! walk completes over an `N`-node tree on an `S`-byte stack, and
//! `(S, N)` is inside the overflow regime for a per-node-recursive walk
//! of the same shape over the same tree.*

#![allow(dead_code)]

use std::process::Command;

pub const CHILD_ENV: &str = "PULSUS_WALK_STACK_CHILD";

pub fn child_mode() -> Option<String> {
    std::env::var(CHILD_ENV).ok()
}

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

pub fn assert_child_overflowed(child_test: &str, mode: &str) {
    let out = spawn_child(child_test, mode);
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        !out.status.success(),
        "control mode {mode:?} was expected to overflow but exited successfully\n{stderr}"
    );
    assert!(
        stderr.contains("stack overflow"),
        "control mode {mode:?} failed without a stack overflow: {}\n{stderr}",
        out.status
    );
}

/// A plain `Box`-child mirror of `MetricNode` with the same arities.
pub enum MnShadow {
    Leaf,
    One(Box<MnShadow>),
    Two(Box<MnShadow>, Box<MnShadow>),
}

pub fn shadow_arity(n: &MnShadow) -> usize {
    match n {
        MnShadow::Leaf => 0,
        MnShadow::One(_) => 1,
        MnShadow::Two(..) => 2,
    }
}

pub fn build_mn_shadow(k: usize) -> MnShadow {
    let mut e = MnShadow::Leaf;
    for _ in 0..k {
        e = MnShadow::Two(Box::new(e), Box::new(MnShadow::Leaf));
    }
    e
}

/// One frame per node. `#[inline(never)]` so a release build cannot turn
/// the recursion into a loop and make the control vacuous.
#[inline(never)]
pub fn walk_mn_shadow_recursive(n: &MnShadow, depth: usize) -> usize {
    match n {
        MnShadow::Leaf => depth,
        MnShadow::One(inner) => walk_mn_shadow_recursive(inner, depth + 1),
        MnShadow::Two(l, r) => {
            let a = walk_mn_shadow_recursive(l, depth + 1);
            let b = walk_mn_shadow_recursive(r, depth + 1);
            a.max(b)
        }
    }
}

/// Drops the mirror iteratively — it has no `impl Drop`, so a deep one
/// would abort on the way out of the child even when the leg passed.
pub fn dismantle_mn_shadow(mut n: MnShadow) {
    loop {
        n = match n {
            MnShadow::Two(l, r) => {
                drop(r);
                *l
            }
            MnShadow::One(inner) => *inner,
            MnShadow::Leaf => return,
        };
    }
}
