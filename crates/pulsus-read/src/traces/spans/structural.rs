//! The non-transitive structural operators on the search statement (issue
//! #593 part 1): `>`, `<`, `~`, each plain, `!` and `&`, over any spanset
//! operands, as one per-span membership predicate.
//!
//! A spanset holding a structural operator compiles to `M(s)`: whether a
//! span `s` of the window is in the spanset's result, as today's
//! `eval_spanset` (`search_eval.rs`) decides it per trace. Each relation
//! reads the other side through a `(trace_id, …) IN (…)` subquery over the
//! same window, so the result is a predicate on one span row and every
//! search template serves it as its selector.

use pulsus_traceql::{BoolOp, FieldExpr, SpansetExpr, StructuralModifier, StructuralOp, Value};

use super::predicate::{PredicateCtx, SpanPredicate, compile_span_predicate_in};
use crate::traces::PlanError;

/// The all-zero parent: a root.
const ZERO: &str = "toFixedString('', 8)";

/// Whether `spanset` holds a structural operator anywhere.
pub fn holds_structural(spanset: &SpansetExpr) -> bool {
    match spanset {
        SpansetExpr::Filter(_) => false,
        SpansetExpr::Binary { lhs, rhs, .. } => holds_structural(lhs) || holds_structural(rhs),
        SpansetExpr::Structural { .. } => true,
    }
}

/// `M(spanset)` for a spanset holding a structural operator, over
/// `spans_table` in `ctx`'s window. `>>` and `<<` are part 2's.
pub fn compile_membership_in(
    spanset: &SpansetExpr,
    ctx: &PredicateCtx<'_>,
    spans_table: &str,
) -> Result<SpanPredicate, PlanError> {
    let mut c = Membership {
        ctx,
        spans_table,
        demands: Vec::new(),
    };
    let sql = c.member(spanset)?;
    // Every relation and every `&&` reads a window-bounded subquery.
    Ok(SpanPredicate::composed(sql, Some(ctx.window), c.demands))
}

struct Membership<'a, 'b> {
    ctx: &'a PredicateCtx<'b>,
    spans_table: &'a str,
    demands: Vec<String>,
}

impl Membership<'_, '_> {
    /// `SELECT <cols> FROM spans WHERE <window> AND (<m>)`.
    fn over(&self, cols: &str, m: &str, extra: &str) -> String {
        let w = self.ctx.window;
        format!(
            "SELECT {cols} FROM {} WHERE {} AND {} AND {} AND ({m}){extra}",
            self.spans_table,
            w.span_time_clause(),
            w.span_bucket_clause(),
            w.span_day_clause(),
        )
    }

    fn member(&mut self, e: &SpansetExpr) -> Result<String, PlanError> {
        match e {
            SpansetExpr::Filter(f) => {
                let body = f
                    .body
                    .clone()
                    .unwrap_or(FieldExpr::Literal(Value::Bool(true)));
                let p = compile_span_predicate_in(&body, self.ctx)?;
                for d in p.demand_messages() {
                    if !self.demands.contains(d) {
                        self.demands.push(d.clone());
                    }
                }
                Ok(format!("({})", p.sql()))
            }
            SpansetExpr::Binary { op, lhs, rhs } => {
                let l = self.member(lhs)?;
                let r = self.member(rhs)?;
                Ok(match op {
                    BoolOp::Or => format!("({l} OR {r})"),
                    BoolOp::And => format!(
                        "(({l} OR {r}) AND trace_id IN ({}) AND trace_id IN ({}))",
                        self.over("trace_id", &l, ""),
                        self.over("trace_id", &r, "")
                    ),
                })
            }
            SpansetExpr::Structural {
                op,
                modifier,
                lhs,
                rhs,
            } => {
                let a = self.member(lhs)?;
                let b = self.member(rhs)?;
                let rhs_part = self.relation(*op, &a)?;
                Ok(match modifier {
                    StructuralModifier::Plain => format!("({b} AND {rhs_part})"),
                    StructuralModifier::Negated => format!("({b} AND NOT {rhs_part})"),
                    StructuralModifier::Union => {
                        let lhs_part = self.relation(mirror(*op), &b)?;
                        format!("(({b} AND {rhs_part}) OR ({a} AND {lhs_part}))")
                    }
                })
            }
        }
    }

    /// Whether the span holds `op` with some span of `seed`, as today's
    /// `rel_children` / `rel_parents` / `rel_siblings` (`search_eval.rs`)
    /// decide it with `seed` as the seed set — the candidate's own
    /// membership is the caller's.
    fn relation(&self, op: StructuralOp, seed: &str) -> Result<String, PlanError> {
        Ok(match op {
            // The candidate's parent is a seed span: not a root, not itself.
            StructuralOp::Child => format!(
                "(parent_span_id != {ZERO} AND parent_span_id != span_id \
                 AND (trace_id, parent_span_id) IN ({}))",
                self.over("trace_id, span_id", seed, "")
            ),
            // The candidate is the parent of a seed span that is not a root
            // and not its own parent.
            StructuralOp::Parent => format!(
                "((trace_id, span_id) IN ({}))",
                self.over(
                    "trace_id, parent_span_id",
                    seed,
                    &format!(" AND parent_span_id != {ZERO} AND parent_span_id != span_id")
                )
            ),
            // A seed span other than the candidate shares its non-root
            // parent: two seed spans under it, or one and the candidate
            // not a seed span.
            StructuralOp::Sibling => {
                let under = format!(" AND parent_span_id != {ZERO}");
                format!(
                    "(parent_span_id != {ZERO} AND ((trace_id, parent_span_id) IN ({}) \
                     OR (NOT {seed} AND (trace_id, parent_span_id) IN ({}))))",
                    self.over(
                        "trace_id, parent_span_id",
                        seed,
                        &format!("{under} GROUP BY trace_id, parent_span_id HAVING count() >= 2")
                    ),
                    self.over("trace_id, parent_span_id", seed, &under)
                )
            }
            StructuralOp::Descendant | StructuralOp::Ancestor => {
                return Err(PlanError::UnsupportedField(format!(
                    "the structural operator {op} is not supported by the search statement yet \
                     (issue #593 part 2)"
                )));
            }
        })
    }
}

/// The relation whose seed is the other side: the union's left-hand
/// participants (`lhs_participants`).
fn mirror(op: StructuralOp) -> StructuralOp {
    match op {
        StructuralOp::Child => StructuralOp::Parent,
        StructuralOp::Parent => StructuralOp::Child,
        other => other,
    }
}
