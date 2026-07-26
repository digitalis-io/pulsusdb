//! Out-of-process regression gates for the label-filter parenthesis
//! recursion guard (issue #255). A stack overflow in Rust aborts the
//! process and cannot be caught in-process, so the crash-shaped cases
//! re-exec this test binary (`std::env::current_exe`) and assert the
//! child's exit status; the child pins the stack it parses on via
//! `std::thread::Builder::stack_size`, so every assertion is
//! stack-size-relative and scale-invariant — never wall-time.
//!
//! Three gates defend the derived `LABEL_FILTER_MAX_DEPTH = 91`:
//! - Gate A (`margin_label_only`): nesting `limit - 1` parses on a
//!   512 KiB stack — the label-filter walk at the limit costs less than
//!   a quarter of tokio's default 2 MiB worker stack by itself.
//! - Gate B (`margin_composed`): the same nesting under the deepest
//!   legal metric prefix (`sum(`×63, the pre-existing `MAX_DEPTH = 64`
//!   guard's worst case) parses on a 1,280 KiB stack — ≥1.6× total-stack
//!   headroom against the 2 MiB production worker.
//! - Gate C (also in `margin_composed`): `sum(`×64 is rejected, pinning
//!   ×63 as the deepest reachable prefix, so Gate B's prefix is the
//!   worst legal one.
//!
//! The guard bounds paren nesting ONLY: a flat `or`/`and` chain parses
//! at depth 1 and is not counted (surviving vectors tracked in #272).

use std::process::Command;

use pulsus_logql::LogQlError;

/// Child-mode dispatch env var. When set, `depth_guard_child` runs the
/// named mode and every parent test returns early so the child never
/// re-forks.
const CHILD_ENV: &str = "PULSUS_LOGQL_DEPTH_CHILD";

/// tokio's default worker-thread stack size — the production budget the
/// guard is derived against.
const WORKER_STACK: usize = 2 * 1024 * 1024;

/// Gate A stack: a quarter of [`WORKER_STACK`].
const LABEL_ONLY_STACK: usize = 512 * 1024;

/// Gate B stack: 0.625 × [`WORKER_STACK`].
const COMPOSED_STACK: usize = 1_280 * 1024;

/// `{a="b"} | ` + `(`×depth + `x="1"` + `)`×depth — paren nesting
/// `depth` inside a label-filter stage (15 + 2·depth bytes).
fn nested(depth: usize) -> String {
    let mut q = String::with_capacity(15 + 2 * depth);
    q.push_str(r#"{a="b"} | "#);
    for _ in 0..depth {
        q.push('(');
    }
    q.push_str(r#"x="1""#);
    for _ in 0..depth {
        q.push(')');
    }
    q
}

/// `sum(`×prefix + `count_over_time(` + [`nested`]`(depth)` + `[1s])` +
/// `)`×prefix — the label-filter nesting under a metric-aggregation
/// prefix, the composed worst case the limit is derived from.
fn composed(prefix: usize, depth: usize) -> String {
    let inner = nested(depth);
    let mut q = String::with_capacity(inner.len() + 5 * prefix + 21);
    for _ in 0..prefix {
        q.push_str("sum(");
    }
    q.push_str("count_over_time(");
    q.push_str(&inner);
    q.push_str("[1s])");
    for _ in 0..prefix {
        q.push(')');
    }
    q
}

/// Runs `f` on a thread whose stack is pinned to exactly `bytes`, and
/// propagates its panics. An overflow of that stack still aborts the
/// whole process — which is why the callers of the crash-shaped cases
/// are out-of-process children.
fn on_stack(bytes: usize, f: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(bytes)
        .spawn(f)
        .expect("spawn pinned-stack thread")
        .join()
        .expect("pinned-stack thread panicked");
}

/// Discovers the configured limit from the guard itself (never
/// hard-coded here): nesting 4,096 is far past the limit, so the error
/// carries it. Cheap — the parse descends `limit` levels, then rejects.
fn discovered_limit() -> usize {
    match pulsus_logql::parse(&nested(4096)) {
        Err(LogQlError::RecursionLimitExceeded { limit, .. }) => limit,
        other => panic!("expected RecursionLimitExceeded probing the limit, got {other:?}"),
    }
}

/// Re-execs this test binary filtered to `depth_guard_child` with
/// `CHILD_ENV=mode` and asserts the child exited successfully — the
/// libtest args are passed directly, which works under both `cargo
/// test` and cargo-nextest (nextest binaries are ordinary libtest
/// binaries). A regression aborts the child (SIGABRT) instead.
fn assert_child_ok(mode: &str) {
    let exe = std::env::current_exe().expect("current_exe");
    let output = Command::new(exe)
        .args([
            "depth_guard_child",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ENV, mode)
        .output()
        .expect("spawn child test process");
    assert!(
        output.status.success(),
        "child mode {mode:?} failed with {}\n--- child stdout ---\n{}\n--- child stderr ---\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// The child body. A no-op unless [`CHILD_ENV`] is set; dispatches on
/// its value. Each mode pins the stack it parses on so the assertion is
/// about a known budget, independent of the harness thread's own size.
#[test]
fn depth_guard_child() {
    let Ok(mode) = std::env::var(CHILD_ENV) else {
        return;
    };
    match mode.as_str() {
        // On a build without the guard this parse overflows the 2 MiB
        // stack and SIGABRTs the child — the parent sees the non-success
        // status. With the guard it returns the rejection error.
        "reject" => on_stack(WORKER_STACK, || match pulsus_logql::parse(&nested(4096)) {
            Err(LogQlError::RecursionLimitExceeded { .. }) => {}
            other => panic!("expected RecursionLimitExceeded at nesting 4096, got {other:?}"),
        }),
        // Gate A: the deepest accepted label-only nesting parses on a
        // 512 KiB stack (a quarter of the 2 MiB production worker).
        "margin_label_only" => {
            let limit = discovered_limit();
            on_stack(LABEL_ONLY_STACK, move || {
                let query = nested(limit - 1);
                if let Err(err) = pulsus_logql::parse(&query) {
                    panic!(
                        "nesting {} must parse on a {} KiB stack, got {err}",
                        limit - 1,
                        LABEL_ONLY_STACK / 1024,
                    );
                }
            });
        }
        // Gate B: the composed worst case — the deepest accepted
        // nesting under the deepest legal `sum(` prefix — parses on a
        // 1,280 KiB stack (0.625 × the 2 MiB production worker).
        // Gate C: one more `sum(` is rejected, pinning ×63 as maximal.
        "margin_composed" => {
            let limit = discovered_limit();
            on_stack(COMPOSED_STACK, move || {
                let query = composed(63, limit - 1);
                if let Err(err) = pulsus_logql::parse(&query) {
                    panic!(
                        "composed(63, {}) must parse on a {} KiB stack, got {err}",
                        limit - 1,
                        COMPOSED_STACK / 1024,
                    );
                }
            });
            on_stack(WORKER_STACK, move || {
                match pulsus_logql::parse(&composed(64, limit - 1)) {
                    Err(LogQlError::RecursionLimitExceeded { .. }) => {}
                    other => {
                        panic!(
                            "expected RecursionLimitExceeded for a sum(x64 prefix, got {other:?}"
                        )
                    }
                }
            });
        }
        other => panic!("unknown {CHILD_ENV} mode {other:?}"),
    }
}

/// The regression gate: deep label-filter paren nesting is a parse
/// error, never a process abort. Fails (child SIGABRT) on any build
/// where the label-filter grammar lacks its depth guard.
#[test]
fn deep_label_filter_nesting_is_rejected_not_aborted() {
    if std::env::var(CHILD_ENV).is_ok() {
        return;
    }
    assert_child_ok("reject");
}

/// Gate A — see [`depth_guard_child`]'s `margin_label_only` mode.
#[test]
fn limit_fits_in_a_quarter_of_the_worker_stack() {
    if std::env::var(CHILD_ENV).is_ok() {
        return;
    }
    assert_child_ok("margin_label_only");
}

/// Gates B and C — see [`depth_guard_child`]'s `margin_composed` mode.
#[test]
fn composed_worst_case_fits_in_five_eighths_of_the_worker_stack() {
    if std::env::var(CHILD_ENV).is_ok() {
        return;
    }
    assert_child_ok("margin_composed");
}

/// Pins the depth convention in-process, on the production stack size:
/// nesting `limit - 1` is the deepest accepted, `limit` is rejected with
/// a message naming the limit and a span inside the query.
#[test]
fn boundary_accepts_limit_minus_one_and_rejects_limit() {
    if std::env::var(CHILD_ENV).is_ok() {
        return;
    }
    let limit = discovered_limit();
    on_stack(WORKER_STACK, move || {
        if let Err(err) = pulsus_logql::parse(&nested(limit - 1)) {
            panic!("nesting {} must be accepted, got {err}", limit - 1);
        }
        match pulsus_logql::parse(&nested(limit)) {
            Err(err @ LogQlError::RecursionLimitExceeded { .. }) => {
                assert!(
                    err.to_string().contains(&limit.to_string()),
                    "message must name the limit: {err}"
                );
                assert!(err.span().start > 0, "span must point into the query");
            }
            other => panic!("expected RecursionLimitExceeded at nesting {limit}, got {other:?}"),
        }
    });
}
