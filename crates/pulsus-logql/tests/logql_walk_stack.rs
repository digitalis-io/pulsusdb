//! Issue #272 — SCC-2's paired pinned-stack gates (AC 14/15).
//!
//! Every row is a **pair**: the converted walk completes over an
//! `N`-node tree on a `Builder::stack_size(S)` thread inside a
//! name-filtered re-exec'd child, and a per-node-recursive walk of the
//! same shape over the same tree **overflows** at `N/4` on the same `S`
//! (non-success exit **and** `stack overflow` in stderr).
//!
//! Pairing matters because the toolchain is pinned but not frozen: a
//! bump, target or profile change can shrink debug frames until an `N`
//! chosen today overflows nothing, making a bare "no overflow" assertion
//! vacuously true. The control is what keeps `(S, N)` inside the
//! overflow regime.
//!
//! `Debug`/`Display` rows render into a byte-counting sink, never a
//! `String` of the whole tree, so peak RSS is the tree.

#[path = "stackgate/mod.rs"]
mod stackgate;

use std::fmt::Write as _;

use pulsus_logql::walk::Child;
use pulsus_logql::{BinOp, LabelFilterExpr, MatchOp, Matcher, MeNode, MetricExpr, MetricScc};

/// The pinned stack every row runs on. The smallest of
/// {128 KiB, 256 KiB, 512 KiB} at which both legs hold.
const S: usize = 256 * 1024;

/// Nodes in the positive leg's tree. The control runs at `N / 4`.
const N: usize = 20_000;

/// A byte-counting `fmt::Write` sink.
struct Counter(usize);

impl std::fmt::Write for Counter {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0 += s.len();
        Ok(())
    }
}

/// A left-deep `Binary` spine of `k` branching nodes over `Literal`
/// leaves — `2k + 1` nodes. The shape a flat `a or b or c …` chain parses
/// into at parse depth 1, which is why query WIDTH becomes tree DEPTH.
fn build(k: usize) -> MetricExpr {
    let mut e = MetricExpr::Literal("0".to_string());
    for i in 0..k {
        e = MetricExpr::Binary {
            op: BinOp::Add,
            modifier: None,
            lhs: Child::new(e),
            rhs: Child::new(MetricExpr::Literal(i.to_string())),
        };
    }
    e
}

/// A left-deep `Or` chain of `k` boolean nodes over `Match` leaves —
/// `2k + 1` nodes. A flat `x="0" or x="1" or …` label filter parses to
/// exactly this at parse depth 1, so the parenthesis-depth guard never
/// sees it.
fn build_lf(k: usize) -> LabelFilterExpr {
    let leaf = |i: usize| {
        LabelFilterExpr::Match(Matcher {
            name: "x".to_string(),
            op: MatchOp::Eq,
            value: i.to_string(),
        })
    };
    let mut e = leaf(0);
    for i in 0..k {
        e = LabelFilterExpr::Or(Child::new(e), Child::new(leaf(i + 1)));
    }
    e
}

fn node_count(root: &MetricExpr) -> usize {
    let mut n = 0;
    pulsus_logql::for_each_metric_expr(root, |_| n += 1);
    n
}

/// The child entry point. One `#[test]`, dispatched by `CHILD_ENV`, so
/// the parent can name-filter it exactly.
#[test]
fn walk_stack_child() {
    let Some(mode) = stackgate::child_mode() else {
        return;
    };
    match mode.as_str() {
        // ---- positive legs: the converted walks, at N on S ----
        "debug_compact" | "debug_alternate" | "display" | "hash" | "clone" | "eq" | "drop"
        | "preorder" => {
            let k = (N - 1) / 2;
            stackgate::on_stack(S, move || {
                let tree = build(k);
                assert_eq!(node_count(&tree), 2 * k + 1, "built node count");
                match mode.as_str() {
                    "debug_compact" => {
                        let mut c = Counter(0);
                        write!(c, "{tree:?}").expect("compact Debug");
                        assert!(c.0 > k);
                    }
                    "debug_alternate" => {
                        // O(N x depth) *work*, exactly as the derive is,
                        // so this row pins a smaller tree on the SAME
                        // stack rather than a smaller stack.
                        let small = build(500);
                        let mut c = Counter(0);
                        write!(c, "{small:#?}").expect("alternate Debug");
                        assert!(c.0 > 500);
                        drop(small);
                    }
                    "display" => {
                        let mut c = Counter(0);
                        write!(c, "{tree}").expect("Display");
                        assert!(c.0 > k);
                    }
                    "hash" => {
                        use std::hash::{Hash, Hasher};
                        let mut h = std::collections::hash_map::DefaultHasher::new();
                        tree.hash(&mut h);
                        assert!(h.finish() != 0 || h.finish() == 0);
                    }
                    "clone" => {
                        let copy = tree.clone();
                        assert_eq!(node_count(&copy), 2 * k + 1);
                        drop(copy);
                    }
                    "eq" => {
                        let other = build(k);
                        assert!(tree == other);
                        drop(other);
                    }
                    "preorder" => {
                        let mut n = 0usize;
                        pulsus_logql::for_each_metric_expr(&tree, |x| {
                            if matches!(x, MeNode::Expr(MetricExpr::Literal(_))) {
                                n += 1;
                            }
                        });
                        assert_eq!(n, k + 1);
                    }
                    // The tree is dropped INSIDE the pinned thread on
                    // every row, so `drop` is covered by all of them;
                    // this row makes it the only thing measured.
                    "drop" => {}
                    other => panic!("unknown positive mode {other}"),
                }
                drop(tree);
            });
        }
        // ---- SCC-1: the label-filter chain, the vector that bypassed
        //      the parenthesis-depth guard entirely ----
        "lf_debug" | "lf_display" | "lf_hash" | "lf_clone" | "lf_eq" | "lf_drop" => {
            let k = (N - 1) / 2;
            stackgate::on_stack(S, move || {
                let tree = build_lf(k);
                let mut n = 0usize;
                pulsus_logql::for_each_label_filter(&tree, |_| n += 1);
                assert_eq!(n, 2 * k + 1, "built label-filter node count");
                match mode.as_str() {
                    "lf_debug" => {
                        let mut c = Counter(0);
                        write!(c, "{tree:?}").expect("compact Debug");
                        assert!(c.0 > k);
                    }
                    "lf_display" => {
                        let mut c = Counter(0);
                        write!(c, "{tree}").expect("Display");
                        assert!(c.0 > k);
                    }
                    "lf_hash" => {
                        use std::hash::{Hash, Hasher};
                        let mut h = std::collections::hash_map::DefaultHasher::new();
                        tree.hash(&mut h);
                        std::hint::black_box(h.finish());
                    }
                    "lf_clone" => {
                        let copy = tree.clone();
                        assert!(copy == tree);
                        drop(copy);
                    }
                    "lf_eq" => {
                        let other = build_lf(k);
                        assert!(tree == other);
                        drop(other);
                    }
                    "lf_drop" => {}
                    other => panic!("unknown label-filter mode {other}"),
                }
                drop(tree);
            });
        }
        // ---- control legs: a per-node recursion over the mirror ----
        "control" => {
            let k = ((N / 4) - 1) / 2;
            stackgate::on_stack(S, move || {
                let shadow = stackgate::build_me_shadow(k);
                // Shape agreement, asserted BEFORE the stack leg: the
                // mirror must have the SCC's arities, or it is a weaker
                // control than it looks.
                let real = build(k);
                assert_eq!(
                    pulsus_logql::walk::arity::<MetricScc>(MeNode::Expr(&real)),
                    stackgate::shadow_arity(&shadow),
                    "the mirror drifted from SCC-2's arities"
                );
                drop(real);
                let depth = stackgate::walk_me_shadow_recursive(&shadow, 0);
                println!("control reached depth {depth}");
                stackgate::dismantle_me_shadow(shadow);
            });
        }
        other => panic!("unknown child mode {other}"),
    }
}

macro_rules! positive_row {
    ($name:ident, $mode:literal) => {
        #[test]
        fn $name() {
            if stackgate::child_mode().is_some() {
                return;
            }
            stackgate::assert_child_ok("walk_stack_child", $mode);
        }
    };
}

positive_row!(
    compact_debug_survives_a_wide_tree_on_a_pinned_stack,
    "debug_compact"
);
positive_row!(
    alternate_debug_survives_a_wide_tree_on_a_pinned_stack,
    "debug_alternate"
);
positive_row!(display_survives_a_wide_tree_on_a_pinned_stack, "display");
positive_row!(hash_survives_a_wide_tree_on_a_pinned_stack, "hash");
positive_row!(clone_survives_a_wide_tree_on_a_pinned_stack, "clone");
positive_row!(partial_eq_survives_a_wide_tree_on_a_pinned_stack, "eq");
positive_row!(drop_survives_a_wide_tree_on_a_pinned_stack, "drop");
positive_row!(preorder_survives_a_wide_tree_on_a_pinned_stack, "preorder");
// SCC-1 — the flat `and`/`or` label-filter chain. Before #272 this shape
// aborted a 2 MiB stack at 3,000 terms / 33,696 bytes, inside #279's
// admission cap; the parenthesis-depth guard bounds paren nesting only
// and never saw it.
positive_row!(label_filter_compact_debug_survives_a_wide_chain, "lf_debug");
positive_row!(label_filter_display_survives_a_wide_chain, "lf_display");
positive_row!(label_filter_hash_survives_a_wide_chain, "lf_hash");
positive_row!(label_filter_clone_survives_a_wide_chain, "lf_clone");
positive_row!(label_filter_partial_eq_survives_a_wide_chain, "lf_eq");
positive_row!(label_filter_drop_survives_a_wide_chain, "lf_drop");

/// The non-vacuity control. Without this, every row above could pass
/// because the frames got smaller rather than because the walks became
/// iterative.
#[test]
fn a_per_node_recursion_over_the_same_shape_overflows_at_a_quarter_the_size() {
    if stackgate::child_mode().is_some() {
        return;
    }
    stackgate::assert_child_overflowed("walk_stack_child", "control");
}
