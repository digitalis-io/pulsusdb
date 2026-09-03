//! `PlanExplain` — the structured trace surfaced to #13's
//! `X-Pulsus-Explain` header. Built incrementally by
//! [`super::exec::LogQlEngine::explain`] as each stage's SQL becomes known
//! (stage 2/3 depend on stage 1's runtime fingerprint set, so the full
//! trace can only be assembled during execution, not at pure-plan time —
//! see `plan.rs`'s module docs).

use super::plan::RoutingDecision;
use crate::compile::plan::PlanShape;

/// One executed (or about-to-execute) stage's SQL, named for the response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainStage {
    pub name: &'static str,
    pub sql: String,
    pub note: Option<String>,
}

/// The full plan trace for one query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanExplain {
    /// `"streams"`, `"vector"`, or `"matrix"` — the LogQL/query-API result
    /// type this plan produces.
    pub result_type: &'static str,
    pub stages: Vec<ExplainStage>,
    /// The rollup-vs-raw routing decision, `Some` only for metric plans
    /// ([`super::plan::MetricPlan`]) — a streams (log-selector) plan never
    /// routes between tables, so it leaves this `None`.
    pub routing: Option<RoutingDecision>,
    /// The compiled plan's shape (issue #492), rendered as one ADDITIVE
    /// sibling key `data.explain.plan`.
    ///
    /// `None` on every path today, and deliberately: no read path calls
    /// the compile core yet, so every explain response is byte-identical
    /// to the one it produced before this field existed. The three
    /// existing fields do not move.
    pub plan: Option<PlanShape>,
}

impl PlanExplain {
    pub fn new(result_type: &'static str) -> Self {
        Self {
            result_type,
            stages: Vec::new(),
            routing: None,
            plan: None,
        }
    }

    pub fn push(&mut self, name: &'static str, sql: impl Into<String>, note: Option<String>) {
        self.stages.push(ExplainStage {
            name,
            sql: sql.into(),
            note,
        });
    }

    pub fn set_routing(&mut self, decision: RoutingDecision) {
        self.routing = Some(decision);
    }

    /// Records the compiled plan's shape. Nothing calls this on a served
    /// path yet — the compile core is unwired — so the key it would add
    /// is absent from every response this tree sends.
    pub fn set_plan(&mut self, shape: PlanShape) {
        self.plan = Some(shape);
    }
}

#[cfg(test)]
mod tests {
    use super::super::plan::RouteChoice;
    use super::*;

    #[test]
    fn push_appends_a_stage_in_order() {
        let mut explain = PlanExplain::new("streams");
        explain.push("stage1", "SELECT 1", None);
        explain.push("stage2", "SELECT 2", Some("note".to_string()));
        assert_eq!(explain.stages.len(), 2);
        assert_eq!(explain.stages[0].name, "stage1");
        assert_eq!(explain.stages[1].note.as_deref(), Some("note"));
    }

    #[test]
    fn a_new_explain_has_no_routing_decision_yet() {
        let explain = PlanExplain::new("matrix");
        assert!(explain.routing.is_none());
    }

    /// Issue #492: the plan key is additive and absent by default, which
    /// is what keeps every explain response byte-identical.
    #[test]
    fn a_new_explain_carries_no_plan_shape() {
        let mut explain = PlanExplain::new("streams");
        assert!(explain.plan.is_none());
        explain.set_plan(crate::compile::plan::PlanShape {
            parts: Vec::new(),
            links: Vec::new(),
        });
        assert!(explain.plan.is_some());
    }

    #[test]
    fn set_routing_records_the_decision() {
        let mut explain = PlanExplain::new("matrix");
        explain.set_routing(RoutingDecision {
            chosen: RouteChoice::Rollup,
            reason: "rollup: step 60000000000 ns divisible by resolution 5000000000 ns".to_string(),
        });
        let routing = explain.routing.expect("routing set");
        assert_eq!(routing.chosen, RouteChoice::Rollup);
        assert!(routing.reason.starts_with("rollup:"));
    }
}
