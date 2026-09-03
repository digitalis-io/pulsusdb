//! The shared engine that compiles a query pipeline to SQL (issue #492,
//! wave 1) — the language-independent core both read paths lower through.
//!
//! **Named `compile`, not `lower`.** The module name is settled by owner
//! ruling on [#492]; a word kept out of the prose has no business
//! entering the tree as a path and as module identifiers.
//!
//! Two halves, and the split is the whole point:
//!
//! - [`fold`] — the per-link traversal. A link either lowers, or becomes
//!   **residual and the fold continues**, applying its state effect on
//!   the way past. Blocking is emergent from accumulated state (column
//!   provenance, shape, `exact`), never from position: a prefix model —
//!   stop at the first refusal — is a measured regression against what
//!   `logql::plan::compile_line_filters` ships today.
//! - [`plan`] — partitioning a completed fold into an ordered list of
//!   **parts**, each part either one SQL statement or work in our own
//!   process, with the value set crossing between two parts named, typed
//!   and bounded. The RULES live here; the FACTS come from
//!   [`fold::Lang`].
//!
//! **Wave 1 emits no new SQL and moves none.** Neither read path calls
//! this module: `logql::plan` and `traces::search_plan` are untouched, so
//! every statement the tree sends is byte-identical to the statement it
//! sent before. What lands here is the core, the two per-language
//! [`fold::Lang`] impls, and the gates that hold them to the design.
//!
//! **The core introduces no new dependency edge and exports no derive
//! macro.** `pulsus-read` already depends on `pulsus-logql` and
//! `pulsus-traceql`; this module depends on neither, being generic over
//! [`fold::Lang`]. The macro rule is the lesson of this crate's own
//! direct `clickhouse` dependency: a derive macro expands to unqualified
//! paths, so every future consumer would inherit a dependency it did not
//! ask for.
//!
//! [#492]: https://github.com/digitalis-io/pulsusdb/issues/492

pub mod fold;
pub mod plan;
#[cfg(test)]
pub mod testkit;

pub use fold::{
    BlockReason, BoundaryOutput, Capability, Col, ColSet, Disposition, Fidelity, Grouping, Lang,
    Lower, LowerCx, Lowering, Name, NeverReason, OpenSource, OpenSourceId, Ordering, Pred,
    Provenance, Relation, RequestBounds, ResidualReason, Shape, SortDir, SourceName, SourceTerm,
    SqlExpr, lower_chain,
};
pub use plan::{
    Cut, Driver, HandoffCost, Issue, LinkOutcome, Part, PlanConfig, PlanCx, PlanShape, QueryPlan,
    Seed, SeedBound, SourceRef, SqlPart, plan_of,
};
