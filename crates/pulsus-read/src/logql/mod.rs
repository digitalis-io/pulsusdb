//! LogQL planner and SQL generator — the three-stage read path
//! (docs/architecture.md §5.3, docs/schemas.md §3). Consumes the
//! `pulsus-logql` [`pulsus_logql::Expr`] AST (parsing stays in
//! `pulsus-logql`, purely syntactic); this module owns everything
//! downstream of a parsed query: matcher normalization, SQL generation,
//! rollup routing, execution, and vector aggregation.
//!
//! **Module layout** mirrors the plan/execute split: [`params`] (query
//! shape contracts), [`error`] (the `ReadError` taxonomy), [`escape`] (the
//! injection boundary), [`plan`] (pure `Expr → Plan`), [`sql`] (pure
//! per-stage SQL string builders — the snapshot-testing surface),
//! [`explain`] (`PlanExplain`, surfaced to #13's `X-Pulsus-Explain`),
//! [`rows`] (`ChClient` result-row shapes), and [`exec`] (`LogQlEngine`,
//! the only module here that talks to ClickHouse).
//!
//! **M1 rate semantic — documented divergence from Loki/Prometheus:**
//! range-query rate/count bucketing is *fixed, step-aligned, non-overlapping*
//! (`intDiv(ts, step) * step` tumbling windows), not a sliding `[range]`
//! window re-evaluated at every step (task-manager resolution #4 on issue
//! #11). See [`params::QuerySpec::Range`]'s doc comment for the precise
//! contract. Sliding-window parity is an M6 concern.
//!
//! **Scan budget applies to every stage.** `ClickHouse
//! max_bytes_to_read` (from `reader.logql_scan_budget_bytes`) and the
//! 307→`ScanBudgetBytes` error mapping cover stage 1 (stream resolution),
//! stage 2 (hydration), stage 3 (samples), and every metric read — not
//! just the sample-heavy stages (code-review fix-plan amendment §1: a
//! broad `log_streams_idx` scan must abort structured, never run uncapped).
//!
//! **Selectivity probes are plan-only in M1.** [`plan::ProbePlan`] SQL is
//! generated and surfaced in [`PlanExplain`], but never *executed* to
//! reorder matchers or produce a pre-flight budget estimate — see
//! [`plan::ProbePlan`]'s doc comment for the deferral rationale
//! (code-review fix-plan amendment §2).

pub mod error;
pub mod escape;
pub mod exec;
pub mod explain;
pub mod params;
pub mod plan;
pub mod rows;
pub mod sql;

pub use error::{ReadError, TooBroadReason};
pub use exec::{EngineConfig, LogQlEngine, MatrixSeries, QueryResult, StreamResult, VectorSample};
pub use explain::{ExplainStage, PlanExplain};
pub use params::{DEFAULT_MAX_STREAMS, Direction, PlanCtx, QueryParams, QuerySpec};
pub use plan::{MetricPlan, Plan, ProbePlan, RouteChoice, RoutingDecision, StreamsPlan, plan};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
