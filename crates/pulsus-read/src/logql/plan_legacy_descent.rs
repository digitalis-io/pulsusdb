//! The one LogQL walk issue #272 leaves recursive, and the only two
//! functions allowed to reach an SCC child outside a `walk.rs` driver.
//!
//! # Why this module exists at all
//!
//! #272 converts every recursive walk over the LogQL AST and plan types
//! to iterative form — except [`build_metric_node`], whose conversion
//! needs the `Step::Only(SpineTo)` primitive that **#293** owns. But
//! retyping SCC-2's slots to `Child` forces every `MetricExpr` consumer
//! to change, and this function is one, so it cannot both stay recursive
//! and stay untouched. [`scc2_child`]/[`scc2_variants`] are the result.
//!
//! # Where the guarantee lives, and where it does NOT
//!
//! The reviewed condition was "the hatch has exactly one user, and that
//! is asserted rather than intended." **A lexical source scan cannot
//! establish that** — it does not see a caller in a nested module, under
//! an import alias, behind a macro expansion, or in a file the scan's
//! own directory walk missed. Two rounds of patching a scanner did not
//! change that, so the guarantee moved here instead:
//!
//! * [`scc2_child`] and [`scc2_variants`] are **module-private `fn`s in
//!   a module whose only other item is [`build_metric_node`]**. The
//!   COMPILER, not a grep, guarantees that nothing else in `pulsus-read`
//!   can call them — including from a nested module, an alias or a
//!   macro.
//! * `plan.rs` re-exports [`build_metric_node`] alone.
//!
//! What is NOT compiler-held: a *new* direct call to
//! `pulsus_logql::walk::child_of` from somewhere else in this crate. A
//! `pub fn` in a dependency cannot be restricted to one caller, and no
//! amount of scanning changes that. It grants nothing new, though, and
//! this is the honest reason the condition is no longer load-bearing:
//! `walk::child_of` is reproducible from `walk::find_preorder`, which
//! every consumer already has and which #272's C1 already records as an
//! accepted residue ("a consumer can pass a yielded node to a helper
//! that re-enters a driver"). `child_of` differs only by allocating
//! nothing at `arity >= 2`. `walk_child_of_grants_nothing_find_preorder_
//! does_not` in `crates/pulsus-read/tests/recursion_census.rs` proves
//! the equivalence over every fixture node.
//!
//! **#293 deletes this whole module**: `build_metric_node` becomes a
//! `try_dfs` consumer and both accessors go with it.

use pulsus_logql::walk;
use pulsus_logql::{BinModifier, MetricExpr, VariantsExpr};

use super::super::error::ReadError;
use super::super::params::{QueryParams, QuerySpec};
use super::{
    MetricNode, PlanCtx, build_variants_node, metric_plan, parse_plan_number,
    parse_vector_agg_params, unwrap_vector_aggs, window_from,
};

/// The single-child accessor [`build_metric_node`] needs while it stays
/// recursive. **Module-private**: the compiler, not a source scan, is
/// what guarantees its callers are the ones in this file. Panics are
/// unreachable — every call site is inside a `match` arm that already
/// established the variant's arity.
#[inline]
fn scc2_child(n: &MetricExpr, i: usize) -> &MetricExpr {
    match walk::child_of::<pulsus_logql::MetricScc>(pulsus_logql::MeNode::Expr(n), i) {
        Some(pulsus_logql::MeNode::Expr(e)) => e,
        // Unreachable: callers pass an index the matched variant has, and
        // every `MetricExpr` child of a `MetricExpr` is a `MetricExpr`
        // (only `Variants` reaches a `VariantsExpr`, and it is handled by
        // its own arm).
        _ => unreachable!("`build_metric_node` indexes a child its arm declared"),
    }
}

/// The `VariantsExpr` behind `MetricExpr::Variants`. Same disposition as
/// [`scc2_child`]: module-private, deleted by #293.
#[inline]
fn scc2_variants(n: &MetricExpr) -> &VariantsExpr {
    match walk::child_of::<pulsus_logql::MetricScc>(pulsus_logql::MeNode::Expr(n), 0) {
        Some(pulsus_logql::MeNode::Var(v)) => v,
        // Unreachable: the sole child of `MetricExpr::Variants` is its
        // `VariantsExpr`.
        _ => unreachable!("`MetricExpr::Variants` has exactly one `VariantsExpr` child"),
    }
}

/// Recursively plans a binary/literal metric expression into a
/// [`MetricNode`] tree. Every (vector-agg-wrapped) range-aggregation
/// operand becomes a [`MetricNode::Leaf`] via the ordinary
/// [`metric_plan`] path, so per-leaf routing/rollup decisions are exactly
/// what the same expression would get standalone.
pub(super) fn build_metric_node(
    metric_expr: &MetricExpr,
    p: &QueryParams,
    ctx: &PlanCtx<'_>,
) -> Result<MetricNode, ReadError> {
    match metric_expr {
        MetricExpr::Literal(raw) => Ok(MetricNode::Scalar(parse_plan_number(
            raw,
            format_args!("scalar literal"),
        )?)),
        MetricExpr::VectorFn(raw) => Ok(MetricNode::VectorLit {
            value: parse_plan_number(raw, format_args!("vector() value"))?,
            window: window_from(p)?,
        }),
        MetricExpr::Variants(_) => build_variants_node(scc2_variants(metric_expr), p, ctx),
        MetricExpr::Binary { op, modifier, .. } => Ok(MetricNode::Binary {
            op: *op,
            return_bool: matches!(
                modifier,
                Some(BinModifier {
                    return_bool: true,
                    ..
                })
            ),
            matching: modifier.as_ref().and_then(|m| m.matching.clone()),
            lhs: walk::Child::new(build_metric_node(scc2_child(metric_expr, 0), p, ctx)?),
            rhs: walk::Child::new(build_metric_node(scc2_child(metric_expr, 1), p, ctx)?),
        }),
        MetricExpr::Range { .. } => Ok(MetricNode::Leaf(Box::new(metric_plan(
            metric_expr,
            p,
            ctx,
            false,
        )?))),
        MetricExpr::Vector { .. } => {
            let (base, raw_aggs) = unwrap_vector_aggs(metric_expr);
            match base {
                MetricExpr::Range { .. } => Ok(MetricNode::Leaf(Box::new(metric_plan(
                    metric_expr,
                    p,
                    ctx,
                    false,
                )?))),
                MetricExpr::Literal(_) => Err(ReadError::PipelineInvalid {
                    reason: "a vector aggregation cannot aggregate a bare scalar literal"
                        .to_string(),
                }),
                inner => Ok(MetricNode::VectorAgg {
                    aggs: parse_vector_agg_params(
                        &raw_aggs,
                        matches!(p.spec, QuerySpec::Range { .. }),
                    )?,
                    inner: walk::Child::new(build_metric_node(inner, p, ctx)?),
                }),
            }
        }
    }
}
