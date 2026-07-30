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
//! **Range-query semantics (issue #227): Loki-exact SLIDING windows.**
//! A range metric query re-evaluates the `[range]` window `(t - range, t]` at
//! every start-anchored grid point `{start + k·step ≤ end}`, streamed off raw
//! `log_samples` — NOT the former fixed, step-aligned, non-overlapping
//! `intDiv(ts, step) * step` tumbling buckets, and not the 5s rollup (which
//! cannot reproduce Loki's per-event boundary). So `rate({}[1m])` and
//! `rate({}[10m])` differ: the window width AND the rate divisor both track
//! the `[range]`, never `step`. See [`params::QuerySpec::Range`]'s doc
//! comment for the precise contract.
//!
//! **Scan budget applies to every stage.** `ClickHouse
//! max_bytes_to_read` (from `reader.logql_scan_budget_bytes`) and the
//! 307→`ScanBudgetBytes` error mapping cover stage 1 (stream resolution),
//! stage 2 (hydration), stage 3 (samples), and every metric read — not
//! just the sample-heavy stages (code-review fix-plan amendment §1: a
//! broad `log_streams_idx` scan must abort structured, never run uncapped).
//!
//! **LIMIT vs a filtering pipeline (issue M6-09, documented divergence):**
//! stage 3's SQL `LIMIT` bounds *scanned* rows, not surviving entries.
//! When the pipeline contains an in-engine dropping stage that cannot
//! push down (a label filter, or a line filter after `line_format`), the
//! plan oversamples the scan (`scan_limit = limit ×
//! reader.logql_pipeline_scan_factor`, default 10) and re-applies the
//! true `limit` to survivors — a response never over-returns. If the
//! oversampled scan hits its own `LIMIT` ceiling and the pipeline drops
//! more than `(factor-1)/factor` of the scanned lines, the response may
//! return fewer than `limit` entries; exact fetch-until-limit (iterative
//! top-up) is a named follow-up. The scan stays bounded either way:
//! `max_bytes_to_read` is untouched and aborts first.
//!
//! **Client-aggregated metric queries never truncate (issue M6-10):** a
//! metric query whose range carries a beyond-line-filter pipeline, an
//! `unwrap`, or a non-count over-time op raw-scans `(fingerprint,
//! timestamp_ns, body)` over the FULL window with **no `LIMIT`** — an
//! aggregation is either complete or aborts on the byte scan budget as
//! `QueryTooBroad` (complete-or-error, the adjudicated design; distinct
//! from the streams path's scan-bound `LIMIT` above). Un-piped
//! count/bytes aggregations keep the SQL-aggregated rollup-or-raw path
//! byte-identically.
//!
//! **Selectivity probes are plan-only in M1.** [`plan::ProbePlan`] SQL is
//! generated and surfaced in [`PlanExplain`], but never *executed* to
//! reorder matchers or produce a pre-flight budget estimate — see
//! [`plan::ProbePlan`]'s doc comment for the deferral rationale
//! (code-review fix-plan amendment §2).

use pulsus_logql::{Expr, MetricExpr, VectorAggOp};

mod cms;
pub mod detected;
pub mod error;
pub mod escape;
pub mod exec;
pub mod explain;
mod ip;
pub mod params;
pub mod pipeline;
pub mod plan;
pub mod rows;
pub mod sql;
pub mod template;

/// True iff the OUTERMOST node of `expr` is a terminal `sort`/`sort_desc`
/// vector aggregation (issue M8-LQ3). Mirrors PromQL's
/// `pulsus_promql::expr_is_sort_root` (root-only): only a terminal sort's
/// value order survives to the wire, so the server encoder skips its
/// deterministic label re-sort for a sort-rooted **instant** query. A sort
/// nested under a binary op or an inner aggregation has its order destroyed
/// upstream (matches the reference), so this returns false there. LogQL's
/// `MetricExpr` has no `Paren` variant, so no paren-stripping is needed.
pub fn terminal_sort(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Metric(MetricExpr::Vector {
            op: VectorAggOp::Sort | VectorAggOp::SortDesc,
            ..
        })
    )
}

pub use detected::{DetectedFieldOut, DetectedFields, DetectedLabelOut, MAX_DETECTED_FIELD_BYTES};
pub use error::{ReadError, TooBroadReason};
pub use exec::{
    ClientWindow, DetectedFieldsProbe, EngineConfig, GridWindow, HistMatrixSeries, HistOrFloat,
    HistVectorSample, LogQlEngine, LogStats, MAX_FEEDER_SCRATCH_BYTES,
    MAX_VARIANT_FANOUT_STATE_BYTES, MatrixSeries, PatternSeries, QueryResult, StreamResult,
    TAIL_REGISTRATION_GRACE_NS, TailCursor, TailLower, TailPage, TailSetup, VARIANT_LABEL,
    VariantArena, VariantsAggState, VectorSample, VolumeAggregateBy, VolumeEntry, VolumeQuery,
    append_variant_label, apply_vector_aggs, combine_binary, ensure_result_series,
    materialize_vector_lit, read_query_settings, run_client_agg_rows, run_variants_rows,
};
pub use explain::{ExplainStage, PlanExplain};
pub use params::{
    DEFAULT_MAX_STREAMS, Direction, MAX_DURATION_NS, PlanCtx, QueryParams, QuerySpec, TimeBounds,
    ValidatedDuration, validate_duration_ns,
};
pub use pipeline::{CompiledPipeline, EntryOut, MetricRun, PipelineError, SAMPLE_EXTRACTION_ERROR};
pub use plan::{
    ClientAgg, ClientValue, MAX_VARIANT_SUB_STATES, MetricNode, MetricPlan, Plan, ProbePlan,
    RouteChoice, RoutingDecision, StreamsPlan, VariantSpec, plan,
};

#[cfg(test)]
mod tests {
    use super::terminal_sort;
    use pulsus_logql::parse;

    /// `terminal_sort` is true only when the OUTERMOST node is a
    /// `sort`/`sort_desc` (mirrors PromQL's root-only `expr_is_sort_root`).
    #[test]
    fn terminal_sort_matches_only_a_root_sort() {
        for query in [
            r#"sort(rate({app="x"}[5m]))"#,
            r#"sort_desc(rate({app="x"}[5m]))"#,
        ] {
            assert!(terminal_sort(&parse(query).unwrap()), "{query}");
        }
    }

    /// A sort nested under a binary op / an inner aggregation, a non-sort
    /// aggregation, and a bare log query are all false — their order is
    /// destroyed upstream (or there is none).
    #[test]
    fn terminal_sort_is_false_when_not_at_the_root() {
        for query in [
            r#"sum(sort(rate({app="x"}[5m])))"#,
            r#"sort(rate({app="x"}[5m])) + 1"#,
            r#"topk(3, rate({app="x"}[5m]))"#,
            r#"{app="x"}"#,
        ] {
            assert!(!terminal_sort(&parse(query).unwrap()), "{query}");
        }
    }

    /// Issue #221 (§risk pin): a variants root is NEVER a terminal sort,
    /// even when a variant is `sort(...)` — the reference's `Sortable`
    /// returns false for a variants expression, so an instant variants
    /// result is always label-sorted on the wire.
    #[test]
    fn terminal_sort_is_false_for_a_variants_root() {
        let query = r#"variants(sort(count_over_time({app="x"}[5m]))) of ({app="x"}[5m])"#;
        assert!(!terminal_sort(&parse(query).unwrap()));
    }
}
