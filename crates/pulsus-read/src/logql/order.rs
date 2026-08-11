//! The instant-vector wire-order predicate (issue #406 R2).
//!
//! One question: **is the order of an instant vector result set by a
//! `sort`/`sort_desc`, or is it ours to choose?** The server encoder
//! label-re-sorts a vector result unless told otherwise
//! (`logs_api::encode::query_response`), so this module decides when that
//! re-sort would throw away an order the user asked for.
//!
//! # Why this is narrower than the reference's rule, and why deliberately
//!
//! The reference suppresses its re-sort whenever a `sort`/`sort_desc`
//! appears **anywhere** in the tree — `Sortable` walks the whole AST
//! (`pkg/logql/evaluator.go:242-260 @ grafana/loki v3.7.4
//! b318f2829f0ae2094ab3a1e90780450e9e4b03be`; call sites
//! `pkg/logql/engine.go:564` and `:627`, the guarded `sort.Slice` at
//! `:569`/`:632`).
//!
//! Copying that rule would be a **correctness** defect here, not a parity
//! one. Under a vector aggregation the surviving order is a hash walk on
//! both sides: the reference emits `for _, aggr := range result` over a
//! `map[uint64]*groupedAggregation` (`evaluator.go:584 @ v3.7.4`), and
//! [`post_agg::group_instant`](super::post_agg) collects out of a
//! `HashMap` (`post_agg.rs:1122-1135`). So suppressing the re-sort for
//! `sum by (svc) (sort(…))` would put **our own** `HashMap` collection
//! order on the wire — an arbitrary answer to a deterministic question,
//! and it would be wrong even if the reference happened to be stable.
//!
//! It is not. Measured 2026-08-11 on a 2/1/3 fixture, 20 instant queries
//! per store with the eval time nudged +1 s per repeat:
//! `sum by (svc) (sort(…))` returned two arrangements in 20 runs at
//! `grafana/loki@sha256:87f0a067…` (buildinfo `3.7.4`/`b318f282`) and
//! three in 20 at `grafana/loki@sha256:58a6c186…` (`3.4.2`/`4fa045d3`),
//! while PulsusDB was stable 20/20. Registered as `nested-sort-order` in
//! docs/benchmarks/logs-differential-ledger.md.
//!
//! So the rule implemented here is: **the sort's own total order, or our
//! deterministic label order — never a hash walk.** A wrapper carries the
//! order only when it demonstrably reproduces its input's sequence.
//!
//! # Iterative (issue #272)
//!
//! [`sorted_order_reaches_the_wire`] is a
//! [`walk::postorder_into`](pulsus_logql::walk::postorder_into) fold over
//! a value stack, never a per-node recursion —
//! `crates/pulsus-read/tests/recursion_census.rs` sweeps this file
//! automatically.

use pulsus_logql::{BinOp, Expr, MatchGroup, MeNode, MetricExpr, MetricScc, VectorAggOp, walk};

/// One node's two bits.
///
/// `produces_series` mirrors [`plan::MetricNode::produces_series`](super::plan)
/// — whether the sub-tree yields a vector/matrix rather than a bare
/// scalar. It is not decoration: `1 * sort(X)` carries the sort (the
/// scalar operand is mapped over the vector, in order) while
/// `Y * sort(X)` does not (the join ranges the vector `Y`, whose own
/// order is a hash walk). One bit cannot tell those apart.
///
/// `sorted` is the property this module exists to decide: the node's
/// output sequence is a function of a `sort`/`sort_desc`'s totally
/// ordered output and of nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Verdict {
    produces_series: bool,
    sorted: bool,
}

/// Whether the wire order of an INSTANT vector result is set by a
/// `sort`/`sort_desc` in `expr`.
///
/// `true` means the server encoder must **skip** its default label
/// re-sort (`logs_api::encode::query_response`); `false` means our
/// deterministic label order is the answer. See the module doc for why
/// this is deliberately narrower than the reference's whole-AST
/// `Sortable`.
///
/// A streams query has no vector to order, so it is `false` by shape.
pub fn sorted_order_reaches_the_wire(expr: &Expr) -> bool {
    let Expr::Metric(root) = expr else {
        return false;
    };
    let mut nodes: Vec<MeNode<'_>> = Vec::new();
    walk::postorder_into::<MetricScc>(MeNode::Expr(root), &mut nodes);
    let mut vals: Vec<Verdict> = Vec::with_capacity(nodes.len());
    for n in nodes {
        let arity = walk::arity::<MetricScc>(n);
        // Post-order leaves this node's children on the tail of the
        // value stack, in declaration order.
        let at = vals.len() - arity;
        let v = verdict(n, &vals[at..]);
        vals.truncate(at);
        vals.push(v);
    }
    vals.pop().map(|v| v.sorted).unwrap_or(false)
}

/// The classification of one node, given its children's verdicts in
/// declaration order (`Binary` ⇒ `[lhs, rhs]`).
///
/// **The `match`es below have no `_` arm.** A new [`MetricExpr`] variant
/// or a new [`VectorAggOp`] operator is a compile error *here*, in the
/// read path, until someone decides its wire-order property — and the
/// answer a hurried author would then reach for (`sorted: false`) is the
/// safe one: it costs a missing sort, never a nondeterministic response.
/// The property is classified here rather than in `pulsus-logql` because
/// parsing there stays purely syntactic (`super`'s module doc), and a
/// wire-order property is a read-path concern. Same shape as
/// [`agg::VectorAccum::is_reduction`](super::agg) and
/// [`fold::VectorAggFold::new`](super::fold).
fn verdict(node: MeNode<'_>, kids: &[Verdict]) -> Verdict {
    let expr = match node {
        // `VariantsExpr` is reached only as `MetricExpr::Variants`' sole
        // child. Its own children are the variant extractors, whose
        // verdicts are discarded: the reference short-circuits a variants
        // root to `false` before its walk begins (`Sortable`'s
        // `case syntax.VariantsExpr` at `evaluator.go:244-245 @ v3.7.4`),
        // and ours is a fan-out with a synthesized `__variant__` label,
        // so no inner sort's order reaches the root.
        MeNode::Var(_) => {
            return Verdict {
                produces_series: true,
                sorted: false,
            };
        }
        MeNode::Expr(e) => e,
    };
    match expr {
        // A leaf DB read: a set of series with no value order.
        MetricExpr::Range { .. } => Verdict {
            produces_series: true,
            sorted: false,
        },
        // A bare scalar operand.
        MetricExpr::Literal(_) => Verdict {
            produces_series: false,
            sorted: false,
        },
        // `vector(n)` is exactly one sample, so there is no order to set.
        MetricExpr::VectorFn(_) => Verdict {
            produces_series: true,
            sorted: false,
        },
        MetricExpr::Vector { op, .. } => Verdict {
            produces_series: kids[0].produces_series,
            sorted: match op {
                // `sort_instant` is a TOTAL order — value, then label set
                // (`post_agg.rs:743-766`) — so its output sequence is
                // deterministic whatever fed it, and it is the sequence
                // the user asked for.
                VectorAggOp::Sort | VectorAggOp::SortDesc => true,
                // Everything else groups through a `HashMap` on both
                // sides (`post_agg::group_instant`; the reference's
                // `evaluator.go:584`), or truncates through a selection
                // whose surviving order is that same walk. Our
                // deterministic label order is the answer.
                VectorAggOp::Sum
                | VectorAggOp::Avg
                | VectorAggOp::Min
                | VectorAggOp::Max
                | VectorAggOp::Count
                | VectorAggOp::Stddev
                | VectorAggOp::Stdvar
                | VectorAggOp::Topk
                | VectorAggOp::Bottomk
                | VectorAggOp::ApproxTopk => false,
            },
        },
        // A pure label rewrite in place over the same slice:
        // `LabelReplaceEvaluator.Next` rewrites `vec[i].Metric` and
        // returns the same `vec` (`evaluator.go:1275-1309 @ v3.7.4`);
        // `apply_label_replace_capped` iterates `&mut items`
        // (`post_agg.rs:3226-3233`).
        MetricExpr::LabelReplace { .. } => kids[0],
        MetricExpr::Variants(_) => Verdict {
            produces_series: true,
            sorted: false,
        },
        MetricExpr::Binary { op, modifier, .. } => {
            let (lhs, rhs) = (kids[0], kids[1]);
            let group_right = matches!(
                modifier
                    .as_ref()
                    .and_then(|m| m.matching.as_ref())
                    .and_then(|vm| vm.group.as_ref()),
                Some(MatchGroup::Right(_))
            );
            let sorted = match op {
                // `vectorOr` appends the whole lhs in order, then the
                // rhs entries with no lhs match, in rhs order
                // (`evaluator.go:1052-1073 @ v3.7.4`); `set_op_join`'s
                // `Or` arm does the same (`post_agg.rs:615-627`). So the
                // result is ordered iff BOTH sides are.
                BinOp::Or => lhs.sorted && rhs.sorted,
                // `vectorAnd`/`vectorUnless` filter the lhs in place and
                // only hash the rhs (`evaluator.go:1033-1091`), as
                // `set_op_join` does: a subsequence of the lhs.
                BinOp::And | BinOp::Unless => lhs.sorted,
                // Arithmetic and comparison.
                _ => {
                    if !lhs.produces_series {
                        // scalar ⊗ vector: `LiteralStepEvaluator.Next`
                        // ranges the vector and appends
                        // (`evaluator.go:1160-1194`), as `map_samples`
                        // does (`post_agg.rs:327-338`).
                        rhs.sorted
                    } else if !rhs.produces_series {
                        lhs.sorted
                    } else if group_right {
                        // `vectorBinop` hashes the ONE side and ranges
                        // the MANY side, swapping the operands under
                        // `CardOneToMany` (`evaluator.go:955-961`);
                        // `instant_join` assigns the same roles
                        // (`post_agg.rs:495-499`). Under `group_right`
                        // the many side is the rhs, so the rhs carries
                        // the order. Getting this backwards is invisible
                        // in every other shape.
                        rhs.sorted
                    } else {
                        lhs.sorted
                    }
                }
            };
            Verdict {
                produces_series: lhs.produces_series || rhs.produces_series,
                sorted,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_logql::parse;

    /// The inner metric expression every row below is built around — one
    /// vector-valued sub-tree, spelled once.
    const X: &str = r#"sum by (svc) (count_over_time({app="x"}[5m]))"#;
    const Y: &str = r#"sum by (svc) (count_over_time({app="y"}[5m]))"#;

    fn carries(query: &str) -> bool {
        sorted_order_reaches_the_wire(&parse(query).unwrap_or_else(|e| {
            panic!("query does not parse: {query:?}: {e}");
        }))
    }

    fn verdict_of(query: &str) -> Verdict {
        let expr = parse(query).unwrap_or_else(|e| panic!("query does not parse: {query:?}: {e}"));
        let Expr::Metric(root) = &expr else {
            panic!("{query:?} is not a metric query");
        };
        let mut nodes: Vec<MeNode<'_>> = Vec::new();
        walk::postorder_into::<MetricScc>(MeNode::Expr(root), &mut nodes);
        let mut vals: Vec<Verdict> = Vec::new();
        for n in nodes {
            let arity = walk::arity::<MetricScc>(n);
            let at = vals.len() - arity;
            let v = verdict(n, &vals[at..]);
            vals.truncate(at);
            vals.push(v);
        }
        vals.pop().expect("a metric root leaves one verdict")
    }

    /// AC 1 — the six queries measured against the digest-pinned
    /// reference on 2026-08-11 (issue #406, comment 5251687569 and this
    /// change's re-measurement at `e69d3f7`). Every one returned value
    /// order there, 20/20, and label order here.
    #[test]
    fn the_six_measured_wrapper_queries_carry_the_sort_to_the_wire() {
        for query in [
            format!(r#"label_replace(sort({X}), "tag", "$1", "svc", "(.*)")"#),
            format!(r#"label_replace(sort_desc({X}), "tag", "$1", "svc", "(.*)")"#),
            format!("sort({X}) * 1"),
            format!("sort_desc({X}) * 1"),
            format!("sort({X}) + on(svc) ({X} * 0)"),
            format!("sort_desc({X}) + on(svc) ({X} * 0)"),
        ] {
            assert!(carries(&query), "{query}");
        }
    }

    /// AC 2 — the discriminating table. Each `false` row rules out one
    /// wrong implementation that satisfies AC 1 on its own: the
    /// reference's whole-AST `Sortable` (rows 1-2 and 5), the carrier
    /// inversion under `group_right` (row 4 against its positive
    /// counterpart), and collapsing `produces_series` into `sorted`
    /// (row 3 against `1 * sort(X)`).
    #[test]
    fn an_order_that_the_root_cannot_inherit_is_not_carried() {
        for query in [
            // The reference's own rule says `true` here; ours must not,
            // because what survives is a `HashMap` walk.
            format!("sum by (svc) (sort({X}))"),
            format!("topk(2, sort({X}))"),
            // A vector lhs: the join ranges IT, and its order is a hash
            // walk.
            format!("{Y} * sort({X})"),
            // `group_right` makes the RHS the many side, so a sorted LHS
            // no longer sets the order. (No parentheses after
            // `group_right`: a parenthesised list there is its INCLUDE
            // labels, not its operand.)
            format!("sort({X}) + on(svc) group_right {Y}"),
            // `or` keeps the lhs order and then appends the rhs's own —
            // which here is a hash walk.
            format!("sort({X}) or {Y}"),
            r#"variants(sort(count_over_time({app="x"}[5m]))) of ({app="x"}[5m])"#.to_string(),
            format!("sum({X})"),
            r#"{app="x"}"#.to_string(),
        ] {
            assert!(!carries(&query), "{query}");
        }
        for query in [
            // A scalar operand is not a non-sorted one.
            format!("1 * sort({X})"),
            format!("{Y} + on(svc) group_right sort({X})"),
            format!("sort({X}) and {Y}"),
            format!("sort({X}) unless {Y}"),
            // MEASURED, not derived (ruling §2): both stores returned the
            // lhs in order followed by the rhs-only entries in order,
            // 20/20 on 3.7.4 and on 3.4.2.
            format!("sort({X}) or sort({Y})"),
            format!(
                r#"label_replace(label_replace(sort({X}), "a", "$1", "svc", "(.*)"), "b", "$1", "svc", "(.*)")"#
            ),
        ] {
            assert!(carries(&query), "{query}");
        }
    }

    /// AC 3 — the `Vector` arm is an exhaustive `match VectorAggOp` with
    /// no `_` arm, so a new operator is a BUILD failure in this file
    /// until someone classifies its wire-order property. This test then
    /// drives [`VectorAggOp::ALL`] — never a hand-written list — and
    /// pins the partition the arm implements.
    #[test]
    fn every_vector_aggregation_operator_is_classified() {
        for op in VectorAggOp::ALL {
            let query = if op.takes_param() {
                format!("{op}(2, sort({X}))")
            } else {
                format!("{op}(sort({X}))")
            };
            let carrying = matches!(op, VectorAggOp::Sort | VectorAggOp::SortDesc);
            assert_eq!(
                carries(&query),
                carrying,
                "{op} classified wrongly: {query}"
            );
        }
        // The partition is exactly {Sort, SortDesc} — asserted over the
        // same enumeration, so a twelfth operator cannot slip in on
        // either side of it.
        let carrying: Vec<String> = VectorAggOp::ALL
            .iter()
            .filter(|op| {
                let query = if op.takes_param() {
                    format!("{op}(2, sort({X}))")
                } else {
                    format!("{op}(sort({X}))")
                };
                carries(&query)
            })
            .map(|op| op.to_string())
            .collect();
        assert_eq!(carrying, vec!["sort".to_string(), "sort_desc".to_string()]);
    }

    /// AC 4 — one parsed query per [`MetricExpr`] variant, asserting the
    /// documented `Verdict` pair. The compile-time half is the
    /// wildcard-free `match` in [`verdict`]; this is the behavioural
    /// half.
    #[test]
    fn every_metric_expr_variant_reaches_an_intentional_arm() {
        let rows: &[(&str, Verdict)] = &[
            // Range
            (
                r#"count_over_time({app="x"}[5m])"#,
                Verdict {
                    produces_series: true,
                    sorted: false,
                },
            ),
            // Literal
            (
                "5",
                Verdict {
                    produces_series: false,
                    sorted: false,
                },
            ),
            // VectorFn
            (
                "vector(3)",
                Verdict {
                    produces_series: true,
                    sorted: false,
                },
            ),
            // Vector, carrying
            (
                r#"sort(count_over_time({app="x"}[5m]))"#,
                Verdict {
                    produces_series: true,
                    sorted: true,
                },
            ),
            // Vector, not carrying
            (
                r#"sum(count_over_time({app="x"}[5m]))"#,
                Verdict {
                    produces_series: true,
                    sorted: false,
                },
            ),
            // LabelReplace, inheriting
            (
                r#"label_replace(sort(count_over_time({app="x"}[5m])), "t", "$1", "app", "(.*)")"#,
                Verdict {
                    produces_series: true,
                    sorted: true,
                },
            ),
            // Binary
            (
                r#"sort(count_over_time({app="x"}[5m])) * 1"#,
                Verdict {
                    produces_series: true,
                    sorted: true,
                },
            ),
            // Variants
            (
                r#"variants(sort(count_over_time({app="x"}[5m]))) of ({app="x"}[5m])"#,
                Verdict {
                    produces_series: true,
                    sorted: false,
                },
            ),
        ];
        for (query, want) in rows {
            assert_eq!(verdict_of(query), *want, "{query}");
        }
    }

    /// A pure-scalar tree produces no series and carries no order — the
    /// `produces_series` half of the fold, observed at the root.
    #[test]
    fn a_pure_scalar_tree_produces_no_series() {
        assert_eq!(
            verdict_of("5 + 3"),
            Verdict {
                produces_series: false,
                sorted: false,
            }
        );
    }

    /// Nesting: the order survives an arbitrary stack of order-preserving
    /// wrappers, and one non-preserving node anywhere above the sort
    /// ends it.
    #[test]
    fn the_order_survives_a_stack_of_wrappers_and_dies_above_an_aggregation() {
        assert!(carries(&format!(
            r#"label_replace(sort({X}) * 1, "t", "$1", "svc", "(.*)") + 0"#
        )));
        assert!(!carries(&format!(
            r#"label_replace(sum by (svc) (sort({X})) * 1, "t", "$1", "svc", "(.*)") + 0"#
        )));
    }
}
