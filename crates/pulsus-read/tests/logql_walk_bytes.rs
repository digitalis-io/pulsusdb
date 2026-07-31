//! Issue #272 — the walker's allocation model, measured.
//!
//! The accounting unit, stated once: **bytes requested from the global
//! allocator** (`Layout::size()`), which is exactly what the counting
//! allocator below sums. `ChunkStack::next_push_bytes` is a
//! **conservative upper bound** on that quantity computed before the
//! request is made, in release as well as debug, and **no invariant here
//! is enforced by a `debug_assert`**.
//!
//! Charged bytes and requested bytes are different quantities and are
//! never used as each other's expected value.
//!
//! Everything runs inside one `#[test]` so no parallel test thread can
//! pollute the process-global counters.

use std::alloc::{GlobalAlloc, Layout, System};
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static CALLS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static ON: AtomicBool = AtomicBool::new(false);

struct CountingAlloc;

// SAFETY: delegates verbatim to the system allocator; the only side
// effects are relaxed atomic increments, which allocate nothing and
// cannot re-enter the allocator.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ON.load(Ordering::Relaxed) {
            CALLS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ON.load(Ordering::Relaxed) {
            CALLS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

use pulsus_logql::walk::{self, Child, ChunkStack, INDEX_INIT, Scc, Step, WALK_CHUNK_ITEMS};
use pulsus_logql::{MeNode, MetricExpr, MetricScc};

/// Runs `f` with the counters scoped to it, returning `(calls, bytes)`.
fn measured<T>(f: impl FnOnce() -> T) -> (T, u64, u64) {
    CALLS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    ON.store(true, Ordering::Relaxed);
    let out = f();
    ON.store(false, Ordering::Relaxed);
    (
        out,
        CALLS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}

fn leaf() -> MetricExpr {
    let expr = pulsus_logql::parse(r#"rate({app="x"}[5m])"#).expect("fixture parses");
    match &expr {
        pulsus_logql::Expr::Metric(MetricExpr::Range { range, param, .. }) => MetricExpr::Range {
            op: pulsus_logql::RangeAggOp::Rate,
            range: range.clone(),
            param: param.clone(),
        },
        other => panic!("unexpected fixture shape: {other:?}"),
    }
}

/// A `Vector` chain of `depth` layers over a `Range` base.
fn vector_chain(depth: usize) -> MetricExpr {
    let mut e = leaf();
    for _ in 0..depth {
        e = MetricExpr::Vector {
            op: pulsus_logql::VectorAggOp::Sum,
            grouping: None,
            param: None,
            inner: Child::new(e),
        };
    }
    e
}

/// A left-deep `Binary` spine of `k` branching nodes over `Literal`
/// leaves. Its peak DFS frontier is `k + 1`.
fn binary_spine(k: usize) -> MetricExpr {
    let mut e = MetricExpr::Literal("0".to_string());
    for i in 0..k {
        e = MetricExpr::Binary {
            op: pulsus_logql::BinOp::Add,
            modifier: None,
            lhs: Child::new(e),
            rhs: Child::new(MetricExpr::Literal(i.to_string())),
        };
    }
    e
}

/// Delta 4's closed form, evaluated from `size_of` rather than hard-coded.
fn closed_form<T>(frontier: usize) -> (u64, u64) {
    if frontier == 0 {
        return (0, 0);
    }
    let chunks = frontier.div_ceil(WALK_CHUNK_ITEMS);
    let mut cap = INDEX_INIT;
    let mut index_bytes = 0usize;
    let mut index_calls = 0u64;
    loop {
        index_bytes += cap * std::mem::size_of::<Vec<T>>();
        index_calls += 1;
        if cap >= chunks {
            break;
        }
        cap *= 2;
    }
    let bytes = chunks * WALK_CHUNK_ITEMS * std::mem::size_of::<T>() + index_bytes;
    (chunks as u64 + index_calls, bytes as u64)
}

/// The production shape of `plan::unwrap_vector_aggs_into`'s descent,
/// transcribed so the route can be measured from an integration test
/// (the function itself is private to `plan.rs`).
fn unwrap_via_spine<'a>(
    expr: &'a MetricExpr,
    out: &mut Vec<(
        pulsus_logql::VectorAggOp,
        Option<&'a pulsus_logql::Grouping>,
        Option<&'a str>,
    )>,
) -> &'a MetricExpr {
    out.clear();
    let d = walk::descend_spine::<MetricScc, &'a MetricExpr>(MeNode::Expr(expr), |n| match n {
        MeNode::Expr(MetricExpr::Vector {
            op,
            grouping,
            param,
            ..
        }) => {
            out.push((*op, grouping.as_ref(), param.as_deref()));
            ControlFlow::Continue(())
        }
        MeNode::Expr(e) => ControlFlow::Break(e),
        MeNode::Var(_) => unreachable!("descent breaks at `Variants` before its child"),
    });
    match d {
        walk::Descent::Broke(base) => base,
        walk::Descent::Exhausted(_) => unreachable!("`Vector` always has exactly one child"),
    }
}

/// The AC 20(d) mutant route: the same body driven by a stack-backed
/// pre-order walk instead of the allocation-free spine descent.
#[allow(dead_code)]
fn unwrap_via_stack<'a>(
    expr: &'a MetricExpr,
    out: &mut Vec<(
        pulsus_logql::VectorAggOp,
        Option<&'a pulsus_logql::Grouping>,
        Option<&'a str>,
    )>,
) {
    out.clear();
    let mut stack: ChunkStack<<MetricScc as Scc>::Ref<'a>> = ChunkStack::new();
    stack.push(MetricScc::wrap(MeNode::Expr(expr)));
    walk::preorder::<MetricScc>(MeNode::Expr(expr), |n| {
        if let MeNode::Expr(MetricExpr::Vector {
            op,
            grouping,
            param,
            ..
        }) = n
        {
            out.push((*op, grouping.as_ref(), param.as_deref()));
        }
    });
}

#[test]
fn the_walk_allocation_model_holds() {
    // ---- AC 8: `ChunkStack`'s declared growth policy ----------------
    {
        let mut s: ChunkStack<u64> = ChunkStack::new();
        let ((), calls, bytes) = measured(|| {
            // A never-pushed stack allocates nothing.
        });
        assert_eq!((calls, bytes), (0, 0), "an empty ChunkStack allocates");
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
        assert_eq!(s.chunk_count(), 0);
        assert_eq!(s.index_capacity(), 0);

        // Peak element capacity is exactly ceil(len/C) * C, and the index
        // follows 0 -> INDEX_INIT -> 2x.
        let mut charged: u64 = 0;
        let ((), calls, bytes) = measured(|| {
            for i in 0..(WALK_CHUNK_ITEMS * 5 + 1) {
                charged += s.next_push_bytes() as u64;
                s.push(i as u64);
            }
        });
        assert_eq!(s.len(), WALK_CHUNK_ITEMS * 5 + 1);
        assert_eq!(s.chunk_count(), 6);
        assert_eq!(s.index_capacity(), 8);
        // The charge is an UPPER BOUND on what was requested — never an
        // equality, and never the other way round.
        assert!(
            charged >= bytes,
            "charged {charged} < requested {bytes} (calls {calls})"
        );
        let (want_calls, want_bytes) = closed_form::<u64>(WALK_CHUNK_ITEMS * 5 + 1);
        assert_eq!((calls, bytes), (want_calls, want_bytes));

        // `pop` releases nothing — chunks are retained, so a length
        // oscillating across a boundary cannot thrash.
        let ((), calls, bytes) = measured(|| {
            for _ in 0..WALK_CHUNK_ITEMS {
                s.pop();
            }
            for i in 0..WALK_CHUNK_ITEMS {
                s.push(i as u64);
            }
        });
        assert_eq!((calls, bytes), (0, 0), "an oscillating length reallocated");
    }

    // ---- AC 31(1): `descend_spine` is allocation-free ----------------
    for depth in [0usize, 1, 2, 8, 64] {
        let chain = vector_chain(depth);
        // Pre-grown to capacity >= the chain length, so `out`'s own
        // growth cannot appear inside the window (AC 20(a)).
        let mut out = Vec::with_capacity(128);
        let (layers, calls, bytes) = measured(|| {
            let base = unwrap_via_spine(&chain, &mut out);
            assert!(matches!(base, MetricExpr::Range { .. }));
            out.len()
        });
        assert_eq!(
            (calls, bytes),
            (0, 0),
            "descend_spine allocated at depth {depth}"
        );
        assert_eq!(layers, depth, "layer count at depth {depth}");
    }

    // ---- AC 20(c): the returned sequence, including both
    //      `unreachable!()` arms staying unreached ---------------------
    {
        let mut out = Vec::with_capacity(128);
        let variants_terminated = MetricExpr::Vector {
            op: pulsus_logql::VectorAggOp::Sum,
            grouping: None,
            param: None,
            inner: Child::new(
                match &pulsus_logql::parse(
                    r#"variants(count_over_time({app="x"}[5m])) of ({app="x"}[5m])"#,
                )
                .expect("variants fixture parses")
                {
                    pulsus_logql::Expr::Metric(m) => m.clone(),
                    other => panic!("unexpected fixture: {other:?}"),
                },
            ),
        };
        let base = unwrap_via_spine(&variants_terminated, &mut out);
        assert!(matches!(base, MetricExpr::Variants(_)));
        assert_eq!(out.len(), 1);

        let binary_terminated = MetricExpr::Vector {
            op: pulsus_logql::VectorAggOp::Sum,
            grouping: None,
            param: None,
            inner: Child::new(binary_spine(1)),
        };
        let base = unwrap_via_spine(&binary_terminated, &mut out);
        assert!(matches!(base, MetricExpr::Binary { .. }));
        assert_eq!(out.len(), 1);
    }

    // ---- AC 31(2): the pre-order drivers are allocation-free on an
    //      all-`arity <= 1` tree --------------------------------------
    {
        let spine = vector_chain(1_000);
        let (count, calls, bytes) = measured(|| {
            let mut n = 0usize;
            walk::preorder::<MetricScc>(MeNode::Expr(&spine), |_| n += 1);
            n
        });
        assert_eq!(count, 1_001);
        assert_eq!((calls, bytes), (0, 0), "preorder allocated on a spine");

        let (found, calls, bytes) = measured(|| {
            walk::find_preorder::<MetricScc, ()>(MeNode::Expr(&spine), |n| match n {
                MeNode::Expr(MetricExpr::Range { .. }) => ControlFlow::Break(()),
                _ => ControlFlow::Continue(Step::Descend),
            })
        });
        assert!(found.is_some());
        assert_eq!((calls, bytes), (0, 0), "find_preorder allocated on a spine");
    }

    // ---- Finding 4: the post-order walk charges BEFORE it allocates --
    //      Both allocating steps are inside the charged region: the work
    //      stack's next chunk AND the node vector's next reallocation.
    //      The identity asserted is CHARGED >= REQUESTED, measured over
    //      the same window, which is what makes the caller's ceiling
    //      cover this walk rather than merely precede it.
    for k in [1usize, 100, 1_000] {
        let tree = binary_spine(k);
        let mut nodes: Vec<MeNode<'_>> = Vec::new();
        let mut charged = 0usize;
        let ((), calls, bytes) = measured(|| {
            let done: Result<(), ()> =
                walk::try_postorder_into::<MetricScc, ()>(MeNode::Expr(&tree), &mut nodes, |d| {
                    charged += d;
                    Ok(())
                });
            done.expect("an unrefused charge cannot fail");
        });
        assert_eq!(nodes.len(), 2 * k + 1, "post-order node count at k={k}");
        assert!(
            charged as u64 >= bytes,
            "charged {charged} < requested {bytes} at k={k} ({calls} calls) — the ceiling \
             does not cover this walk"
        );
    }

    // A refusal must stop the walk BEFORE the allocation it refused, so
    // a tiny ceiling leaves the node vector far short of the tree.
    {
        let tree = binary_spine(1_000);
        let mut nodes: Vec<MeNode<'_>> = Vec::new();
        let mut left = 4_096usize;
        let refused =
            walk::try_postorder_into::<MetricScc, ()>(MeNode::Expr(&tree), &mut nodes, |d| {
                if d > left {
                    return Err(());
                }
                left -= d;
                Ok(())
            });
        assert!(
            refused.is_err(),
            "a 4 KiB ceiling must refuse a 2,001-node walk"
        );
        assert!(
            nodes.len() < 2 * 1_000 + 1,
            "the walk continued past the refusal: {} nodes",
            nodes.len()
        );
    }

    // ---- AC 31(3): the closed form, on both sides of both boundaries -
    //      F = k + 1 for a left-deep `Binary` spine of k branching nodes
    for (k, frontier) in [(1usize, 2usize), (255, 256), (256, 257), (1_024, 1_025)] {
        let tree = binary_spine(k);
        let (count, calls, bytes) = measured(|| {
            let mut n = 0usize;
            walk::preorder::<MetricScc>(MeNode::Expr(&tree), |_| n += 1);
            n
        });
        assert_eq!(count, 2 * k + 1, "node count at k={k}");
        let (want_calls, want_bytes) = closed_form::<<MetricScc as Scc>::Ref<'_>>(frontier);
        assert_eq!(
            (calls, bytes),
            (want_calls, want_bytes),
            "closed form at F={frontier}"
        );
    }
}
