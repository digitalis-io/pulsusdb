//! The PromQL parsed-expression **depth** cap (issue #262).
//!
//! # Why a cap exists here at all
//!
//! A Rust stack overflow aborts the process; it cannot be caught. Every
//! walk over a parsed `Expr` that recurses once per level — this crate's
//! `plan`/`evaluate`, and the compiler-generated `Drop`/`Clone`/`Debug`
//! glue on the vendored AST — therefore turns a deep-enough query into a
//! **process abort**, killing every other in-flight query on the node
//! rather than returning an error to the caller who sent it.
//!
//! The vectors that reach that point are ordinary query WIDTH, not
//! exotic nesting. The parser is LR and walks `a or b or c …` in a loop
//! at grammar-nesting depth 1, but it *builds* a left-deep `Binary`
//! spine, so a flat chain of N terms is a tree of depth N. Measured on
//! the release binary over HTTP at `f916d7f` (`PULSUS_MODE=all`, empty
//! ClickHouse), bisected on whether the server process survived:
//!
//! | shape | survives | aborts at |
//! |---|---|---|
//! | `label_replace(…)` nesting | 887 | **888** (34,634 B, POST) |
//! | `1 + 1 + …` | 1,219 | **1,220** (4,877 B — a plain GET) |
//!
//! 888 is the floor at the user layer across the fifteen shapes measured;
//! the same bisection on a bare 2 MiB pinned thread (tokio's worker
//! default; `pulsus-server`'s `#[tokio::main]` sets no `stack_size`) puts
//! the floor at 905. Both are *properties of this toolchain, target and
//! profile* — a compiler bump moves them.
//!
//! # This is a deliberate divergence, not a parity fix
//!
//! **Prometheus imposes no depth or length limit at all.** Read at
//! `40af9c2cdc0eda00f3622e867a27f6359f7295f3` (= `v3.13.0`, the tag this
//! repo pins): `promql/parser/lex.go:309`'s `parenDepth` is an
//! unbalanced-paren counter tested only for `< 0` (`:525`, `:1230`),
//! never an upper bound; `promql/parser/parse.go` carries no input-length
//! guard; `web/api/v1/api.go` has no `MaxBytesReader` and no query-text
//! cap; `promql/engine.go:303`'s `MaxSamples` bounds samples, not
//! expression size. Go grows its stacks, so the reference answers the
//! exact query that aborts this engine: `prom/prometheus:v3.13.0`
//! returns `200` for `1 + 1 + …` at 1,221 terms and at 20,000 terms, and
//! only stops at 50,000 by hitting its ordinary two-minute query timeout.
//!
//! So there is no reference value and no reference message to match.
//! Ledgered as `promql-expression-depth-cap` in `docs/api.md` §3.5, which
//! also records what the cap does **not** cover.
//!
//! # The guard must not have the defect it guards against
//!
//! Both functions here are iterative — one explicit `Vec` worklist, and
//! no `fn` on either path calls itself. That is load-bearing twice over:
//!
//! * [`depth_and_capacity`] measures a tree that may be far deeper than
//!   [`MAX_EXPR_DEPTH`], so a per-node-recursive walk would abort while
//!   measuring. Measured: this walk completes over a depth-1,000,001
//!   spine in release and depth-100,001 in debug on a 2 MiB stack.
//! * [`dismantle`] exists because *throwing the rejected tree away also
//!   recurses*. `Expr` has no manual `Drop`, so the compiler's drop glue
//!   descends one frame per level: measured abort at depth **43,564**
//!   release and **21,718** debug on 2 MiB. `parse` can be handed a tree
//!   deeper than that — a 60,000-term chain (239,997 bytes) parses
//!   cleanly, and a 2 MiB POST body buys far more — so rejecting without
//!   an iterative teardown would simply move the abort from `evaluate`
//!   onto the reject path.
//!
//! Mirrors the vendored parser's own `ast::dismantle` (PATCHES.md #6),
//! which is `pub(crate)` there and so cannot be reused.

use crate::parser::Expr;

/// The maximum **depth** of a parsed PromQL expression tree. Reject iff
/// `depth > MAX_EXPR_DEPTH`, so the deepest ACCEPTED expression has depth
/// exactly `MAX_EXPR_DEPTH`.
///
/// **A DELIBERATE DIVERGENCE** — Prometheus has no such bound (module doc
/// above, and `docs/api.md` §3.5).
///
/// **Why 250.** The margin is the argument: 888 (the measured
/// user-layer floor) / 250 = **3.55×**, against a Prometheus conformance
/// corpus whose deepest expression across all 2,183 `eval` queries is
/// **10**. The fifteen measured shapes are not the whole space, so an
/// unmeasured construct could cost more per level than any of them; the
/// margin is what absorbs that. The closest real counter-datapoint is a
/// 177-term machine-generated `or` chain (issue #255 — LogQL rather than
/// PromQL, and a derived figure rather than a captured query), which is
/// 71% of this cap. Because observed machine-generated width is that
/// close, the rejection message names **both** the limit and the depth
/// measured, so anyone who hits it can tell a cap from a bug.
///
/// One number, both profiles: the accept boundary is a public API
/// contract, and a per-profile limit would make CI exercise an
/// accept/reject surface the shipped binary does not have.
pub const MAX_EXPR_DEPTH: usize = 250;

/// The **full** depth of `expr`, plus the worklist `Vec`'s final
/// capacity.
///
/// ITERATIVE: one explicit `Vec<(&Expr, usize)>` worklist; no `fn` on
/// this path calls itself.
///
/// Does **not** short-circuit at [`MAX_EXPR_DEPTH`]: the rejection
/// message has to name the depth that was measured, not `limit + 1`.
/// The cost of that is structural rather than empirical — the walk
/// pushes each `Expr`-typed slot exactly once, so it is one O(nodes)
/// pass added to a reject path that already carries two mandatory ones
/// (the parser's construction, and [`dismantle`]); it cannot exceed a
/// constant fraction of work already committed.
///
/// The capacity is a high-water mark for free — a `Vec` never shrinks —
/// so the query-performance gate measures the **real** walk rather than
/// an instrumented copy of it, and there is no second walk in this crate
/// for a later refactor to let drift.
pub(crate) fn depth_and_capacity(expr: &Expr) -> (usize, usize) {
    let mut work: Vec<(&Expr, usize)> = Vec::new();
    work.push((expr, 1));
    let mut max_depth = 0usize;
    while let Some((node, depth)) = work.pop() {
        if depth > max_depth {
            max_depth = depth;
        }
        let child = depth + 1;
        // Exhaustive on purpose, and shaped exactly like the vendored
        // `ast::dismantle` (`vendor/promql-parser/src/parser/ast.rs:2328`)
        // so a vendored AST change breaks compilation here rather than
        // silently skipping a slot. `agg.param` is the one slot
        // `dismantle` handles that the enum's shape does not make
        // obvious.
        match node {
            Expr::Aggregate(agg) => {
                work.push((agg.expr.as_ref(), child));
                if let Some(param) = &agg.param {
                    work.push((param.as_ref(), child));
                }
            }
            Expr::Unary(u) => work.push((u.expr.as_ref(), child)),
            Expr::Binary(b) => {
                // lhs first, rhs second: the pop order that follows makes
                // the worklist O(1) on a left-deep spine — the shape every
                // flat chain parses into. Swapping these two pushes makes
                // it O(nodes) (measured: capacity 4 -> 131,072 at depth
                // 100,000), which is what the capacity gate detects.
                work.push((b.lhs.as_ref(), child));
                work.push((b.rhs.as_ref(), child));
            }
            Expr::Paren(p) => work.push((p.expr.as_ref(), child)),
            Expr::Subquery(sq) => work.push((sq.expr.as_ref(), child)),
            Expr::Call(c) => work.extend(c.args.args.iter().map(|b| (b.as_ref(), child))),
            Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::VectorSelector(_)
            | Expr::MatrixSelector(_)
            | Expr::Extension(_) => {}
        }
    }
    (max_depth, work.capacity())
}

/// Iterative teardown for the reject path: children are MOVED out of
/// their boxes onto an explicit worklist so every shell drops shallowly.
///
/// See the module doc for why letting a rejected tree fall out of scope
/// is not an option. Only the `Expr` spine needs this — every other
/// field (matchers, durations, literals) has depth bounded by its own
/// written form, independent of expression nesting.
pub(crate) fn dismantle(expr: Expr) {
    let mut work: Vec<Expr> = vec![expr];
    while let Some(node) = work.pop() {
        match node {
            Expr::Aggregate(agg) => {
                work.push(*agg.expr);
                if let Some(param) = agg.param {
                    work.push(*param);
                }
            }
            Expr::Unary(u) => work.push(*u.expr),
            Expr::Binary(b) => {
                work.push(*b.lhs);
                work.push(*b.rhs);
            }
            Expr::Paren(p) => work.push(*p.expr),
            Expr::Subquery(sq) => work.push(*sq.expr),
            Expr::Call(c) => work.extend(c.args.args.into_iter().map(|b| *b)),
            Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::VectorSelector(_)
            | Expr::MatrixSelector(_)
            | Expr::Extension(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::PromqlError;
    use crate::parser::{BinaryExpr, token};

    /// tokio's worker default, and the stack `pulsus-server`'s bare
    /// `#[tokio::main]` gives every request handler.
    const PINNED_STACK: usize = 2 * 1024 * 1024;

    /// Above BOTH measured drop-glue floors (43,564 release / 21,718
    /// debug), so replacing [`dismantle`] with `drop` aborts the process
    /// in either profile rather than merely being slower.
    const DEEP: usize = 100_000;

    /// A left-deep `Binary` spine of the given depth, built directly.
    ///
    /// Never by parsing: 100,000 iterations of `parse("1 + 1")` takes
    /// 203 s in debug (measured), which is why every deep fixture here
    /// is constructed instead.
    fn spine(depth: usize) -> Expr {
        let mut acc = Expr::from(1.0);
        for _ in 1..depth {
            acc = Expr::Binary(BinaryExpr {
                op: token::TokenType::new(token::T_ADD),
                lhs: Box::new(acc),
                rhs: Box::new(Expr::from(1.0)),
                modifier: None,
            });
        }
        acc
    }

    /// Runs `f` on a thread whose stack is pinned to exactly `bytes` and
    /// returns its value. An overflow of that stack aborts the whole
    /// process — which is precisely the failure these gates exist to
    /// make visible.
    fn on_stack<T: Send + 'static>(bytes: usize, f: impl FnOnce() -> T + Send + 'static) -> T {
        std::thread::Builder::new()
            .stack_size(bytes)
            .spawn(f)
            .expect("spawn pinned-stack thread")
            .join()
            .expect("pinned-stack thread panicked")
    }

    /// Iterative node count — a test-only second opinion on tree size,
    /// for AC 5 leg 2b's `capacity <= nodes.next_power_of_two()`.
    fn node_count(expr: &Expr) -> usize {
        let mut work = vec![expr];
        let mut n = 0usize;
        while let Some(node) = work.pop() {
            n += 1;
            match node {
                Expr::Aggregate(agg) => {
                    work.push(agg.expr.as_ref());
                    if let Some(param) = &agg.param {
                        work.push(param.as_ref());
                    }
                }
                Expr::Unary(u) => work.push(u.expr.as_ref()),
                Expr::Binary(b) => {
                    work.push(b.lhs.as_ref());
                    work.push(b.rhs.as_ref());
                }
                Expr::Paren(p) => work.push(p.expr.as_ref()),
                Expr::Subquery(sq) => work.push(sq.expr.as_ref()),
                Expr::Call(c) => work.extend(c.args.args.iter().map(|b| b.as_ref())),
                Expr::NumberLiteral(_)
                | Expr::StringLiteral(_)
                | Expr::VectorSelector(_)
                | Expr::MatrixSelector(_)
                | Expr::Extension(_) => {}
            }
        }
        n
    }

    // -----------------------------------------------------------------
    // Query-text generators for the pinned shapes — generated, never
    // retyped.
    // -----------------------------------------------------------------

    fn bin_chain(terms: usize) -> String {
        format!("1{}", " + 1".repeat(terms - 1))
    }

    fn named_chain(terms: usize) -> String {
        let mut q = "m0".to_string();
        for i in 1..terms {
            q.push_str(&format!(" + m{i}"));
        }
        q
    }

    fn or_chain(terms: usize) -> String {
        let mut q = r#"up{i="0"}"#.to_string();
        for i in 1..terms {
            q.push_str(&format!(r#" or up{{i="{i}"}}"#));
        }
        q
    }

    fn sum_chain(terms: usize) -> String {
        let mut q = "sum(m0)".to_string();
        for i in 1..terms {
            q.push_str(&format!(" + sum(m{i})"));
        }
        q
    }

    fn paren_nesting(levels: usize) -> String {
        format!("{}1{}", "(".repeat(levels), ")".repeat(levels))
    }

    fn label_replace_nesting(levels: usize) -> String {
        format!(
            "{}up{}",
            "label_replace(".repeat(levels),
            r#", "d", "$1", "s", "(.*)")"#.repeat(levels)
        )
    }

    fn right_deep_nesting(levels: usize) -> String {
        format!("{}1{}", "1 + (".repeat(levels), ")".repeat(levels))
    }

    /// `label_join(up, "d", "-", "s0", "s1", …)` — a **depth-2**
    /// expression with an unbounded argument count, and therefore the
    /// counter-example that refutes any universal ceiling on the
    /// worklist. Well inside the cap: it is ACCEPTED.
    fn label_join_variadic(sources: usize) -> String {
        let mut q = r#"label_join(up, "d", "-""#.to_string();
        for i in 0..sources {
            q.push_str(&format!(r#", "s{i}""#));
        }
        q.push(')');
        q
    }

    // -----------------------------------------------------------------
    // AC 1 — the boundary, on directly-constructed trees
    // -----------------------------------------------------------------

    /// **AC 1.** Reject iff `depth > MAX_EXPR_DEPTH`, so the deepest
    /// ACCEPTED expression has depth exactly `MAX_EXPR_DEPTH`. Measured
    /// on constructed spines so no parse cost is involved; the
    /// parse-level twin over real query text lives in `parser.rs`.
    #[test]
    fn the_walk_reports_exactly_the_constructed_depth_at_the_boundary() {
        let at = spine(MAX_EXPR_DEPTH);
        assert_eq!(depth_and_capacity(&at).0, MAX_EXPR_DEPTH);
        let over = spine(MAX_EXPR_DEPTH + 1);
        assert_eq!(depth_and_capacity(&over).0, MAX_EXPR_DEPTH + 1);
    }

    // -----------------------------------------------------------------
    // AC 2 — the depth measured is the TREE's, not the parser's
    // -----------------------------------------------------------------

    /// **AC 2.** The August ruling's premise as a standing check rather
    /// than a plan claim: a flat chain has grammar-nesting depth 1 and
    /// the parser walks it in a loop, but it BUILDS a left-deep spine,
    /// so query WIDTH becomes tree DEPTH 1:1. `sum(m0) + …` is `N + 1`
    /// because each term is itself an `Aggregate` over a selector.
    ///
    /// At `N = 250` the `sum` chain is depth 251 and is therefore
    /// REJECTED — asserted through the error's own `depth` field, which
    /// is the same quantity the walk reports.
    #[test]
    fn tree_depth_equals_query_width_for_every_flat_chain() {
        for n in [5usize, 100, 250] {
            for (shape, query, expected) in [
                ("bin", bin_chain(n), n),
                ("named", named_chain(n), n),
                ("or", or_chain(n), n),
                ("sumchain", sum_chain(n), n + 1),
            ] {
                match crate::parse(&query) {
                    Ok(expr) => {
                        assert!(
                            expected <= MAX_EXPR_DEPTH,
                            "{shape} n={n}: accepted, but depth {expected} is over the cap"
                        );
                        assert_eq!(depth_and_capacity(&expr).0, expected, "{shape} n={n}");
                    }
                    Err(PromqlError::ExprTooDeep { depth, limit }) => {
                        assert!(
                            expected > MAX_EXPR_DEPTH,
                            "{shape} n={n}: rejected at depth {expected}, inside the cap"
                        );
                        assert_eq!(depth, expected, "{shape} n={n}");
                        assert_eq!(limit, MAX_EXPR_DEPTH);
                    }
                    Err(other) => panic!("{shape} n={n}: {other}"),
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // AC 3 / AC 4 — the instrument and the teardown are bounded
    // -----------------------------------------------------------------

    /// **AC 3.** The guard must not have the defect it guards against.
    /// The walk runs over a spine of depth 100,000 on a pinned 2 MiB
    /// stack — tokio's worker default — and completes.
    ///
    /// **The break this criterion exists for:** replacing the worklist
    /// loop with a per-node-recursive walk of the same shape aborts the
    /// process at this same pinned `(2 MiB, 100,000)`. Demonstrated once
    /// on issue #262 with the transcript recorded; the number alone is
    /// not the property.
    ///
    /// The teardown at the end is not incidental — dropping a
    /// depth-100,000 spine would abort, which is AC 4's subject.
    #[test]
    fn the_depth_walk_survives_a_hundred_thousand_levels_on_a_pinned_stack() {
        on_stack(PINNED_STACK, || {
            let deep = spine(DEEP);
            let (depth, capacity) = depth_and_capacity(&deep);
            assert_eq!(depth, DEEP);
            assert!(depth > MAX_EXPR_DEPTH);
            assert!(capacity < 64, "worklist capacity {capacity}");
            dismantle(deep);
        });
    }

    /// **AC 4.** Throwing the rejected tree away also recurses: `Expr`
    /// has no manual `Drop`, so the compiler's drop glue descends one
    /// frame per level and aborts at depth 43,564 (release) / 21,718
    /// (debug) on 2 MiB. 100,000 is above both, so replacing
    /// `dismantle(deep)` below with `drop(deep)` makes this test ABORT —
    /// exit 134, `fatal runtime error: stack overflow` — in either
    /// profile. Demonstrated once on issue #262 with the transcript
    /// recorded.
    #[test]
    fn the_teardown_survives_a_hundred_thousand_levels_on_a_pinned_stack() {
        on_stack(PINNED_STACK, || {
            let deep = spine(DEEP);
            dismantle(deep);
        });
    }

    // -----------------------------------------------------------------
    // AC 5 — the worklist peak, pinned on the REAL walk
    // -----------------------------------------------------------------

    /// **AC 5 leg 1.** The query-performance claim as a scale-invariant
    /// identity rather than an adjective: on the left-deep spine every
    /// flat chain parses into, the worklist's high-water mark does not
    /// grow with depth at all. `Vec` never shrinks its capacity, so the
    /// final capacity IS the high-water mark — measured on the real
    /// walk, with no instrumented copy for a later edit to weaken.
    ///
    /// **The break:** swapping the two pushes in the `Binary` arm (`rhs`
    /// before `lhs`) makes these 1,024 / 16,384 / 131,072 — the gate
    /// reddens by 32,768× at the largest depth. Demonstrated once on
    /// issue #262.
    #[test]
    fn the_worklist_is_depth_independent_on_a_left_deep_spine() {
        let caps: Vec<usize> = [1_000usize, 10_000, 100_000]
            .into_iter()
            .map(|depth| {
                on_stack(PINNED_STACK, move || {
                    let tree = spine(depth);
                    let (measured, capacity) = depth_and_capacity(&tree);
                    assert_eq!(measured, depth);
                    dismantle(tree);
                    capacity
                })
            })
            .collect();
        assert_eq!(
            caps,
            vec![caps[0]; 3],
            "worklist capacity must not vary with spine depth"
        );
        assert!(caps[0] < 64, "worklist capacity {caps:?}");
    }

    /// **AC 5 leg 2a — pinned capacities at the accept boundary,
    /// spelling-independent only.**
    ///
    /// **The admission rule for this table, and it is load-bearing:** a
    /// row belongs here only if BOTH spellings of the `Call` arm — the
    /// vendored `extend` this walk copies, and a naive `push` loop —
    /// give the same number. The `label_join` variadic row fails exactly
    /// that rule (`extend` 1,003 vs `push` 1,024 at 1,000 sources), so
    /// it is NOT here; it lives in leg 2b, where the relation holds
    /// under both. A literal that silently encodes which spelling the
    /// `Call` arm uses would be a trap laid on the one line the
    /// exhaustive-match design expects to be revisited.
    ///
    /// No universal ceiling is claimed. An earlier draft asserted
    /// `capacity < 4096` for every accepted expression; that is FALSE —
    /// see leg 2b.
    #[test]
    fn worklist_capacity_is_pinned_at_the_accept_boundary() {
        // (shape, query, expected depth, expected capacity)
        let rows: Vec<(&str, String, usize, usize)> = vec![
            ("bin_chain", bin_chain(250), 250, 4),
            ("or_chain", or_chain(250), 250, 4),
            ("sum_chain", sum_chain(249), 250, 4),
            ("paren_nesting", paren_nesting(249), 250, 4),
            ("label_replace_nesting", label_replace_nesting(249), 250, 8),
            ("right_deep_nesting", right_deep_nesting(124), 249, 128),
        ];
        for (shape, query, want_depth, want_capacity) in rows {
            let expr = crate::parse(&query)
                .unwrap_or_else(|e| panic!("{shape} must be ACCEPTED by the cap: {e}"));
            let (depth, capacity) = depth_and_capacity(&expr);
            assert_eq!(depth, want_depth, "{shape} depth");
            assert_eq!(capacity, want_capacity, "{shape} capacity");
            assert!(depth <= MAX_EXPR_DEPTH, "{shape} must be inside the cap");
        }
    }

    /// **AC 5 leg 2b — the general relation**, asserted on every leg-2a
    /// shape PLUS the three variadic rows leg 2a cannot admit:
    /// `capacity <= nodes.next_power_of_two()`, and
    /// `next_power_of_two(n) < 2n`, so the worklist never exceeds twice
    /// the node count — and the parser has already allocated a far
    /// larger `Expr` per node. This is the general query-performance
    /// claim; leg 2a is the push-order and boundary pin.
    ///
    /// The variadic rows are the counter-example that killed the old
    /// `< 4096` universal: `label_join` is variadic, so a **depth-2**
    /// expression, accepted by the cap with 248 levels to spare,
    /// produces a worklist of 20,003.
    #[test]
    fn the_worklist_never_exceeds_the_node_count_rounded_up() {
        let mut queries: Vec<(&str, String)> = vec![
            ("bin_chain", bin_chain(250)),
            ("or_chain", or_chain(250)),
            ("sum_chain", sum_chain(249)),
            ("paren_nesting", paren_nesting(249)),
            ("label_replace_nesting", label_replace_nesting(249)),
            ("right_deep_nesting", right_deep_nesting(124)),
        ];
        for n in [1_000usize, 5_000, 20_000] {
            queries.push(("label_join_variadic", label_join_variadic(n)));
        }

        let mut saw_variadic_over_4096 = false;
        for (shape, query) in queries {
            let expr = crate::parse(&query)
                .unwrap_or_else(|e| panic!("{shape} must be ACCEPTED by the cap: {e}"));
            let (depth, capacity) = depth_and_capacity(&expr);
            let nodes = node_count(&expr);
            assert!(
                capacity <= nodes.next_power_of_two(),
                "{shape}: capacity {capacity} > nodes {nodes} rounded up"
            );
            if shape == "label_join_variadic" {
                assert_eq!(depth, 2, "{shape} is a DEPTH-2 expression");
                assert!(depth <= MAX_EXPR_DEPTH, "{shape} is accepted by the cap");
                saw_variadic_over_4096 |= capacity > 4096;
            }
        }
        assert!(
            saw_variadic_over_4096,
            "the variadic counter-example must exceed the refuted `< 4096` ceiling, \
             or leg 2b is not exercising the case it exists for"
        );
    }
}
