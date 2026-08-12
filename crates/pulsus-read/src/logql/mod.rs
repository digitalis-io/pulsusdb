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
//! **The execution region** (issue #299) is ten flat sibling modules cut
//! along the subsystems the code already was, not one 19k-line file:
//! [`exec`] (the engine and its ClickHouse-facing helpers), [`window`]
//! (window/grid arithmetic), [`charge`] (the byte-charge model and the
//! admission caps), [`agg`] (the shared aggregation vocabulary),
//! [`labels`] (the label/JSON codec), [`client_agg`] (the streaming
//! per-row aggregation state — the only O(rows) path), [`fold`] (the
//! bounded grid fold), [`variants`] (`variants(...) of (...)`),
//! [`detected_probe`] (detected-fields probing and the tail cursor) and
//! [`post_agg`] (post-aggregation and its byte ledger), plus [`order`]
//! (issue #406: whether a `sort`/`sort_desc`'s order sets the wire order
//! of an instant vector). Flat, never a subdirectory: the censuses over
//! this directory are non-recursive, so a nested module would be
//! invisible to them.
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

pub(crate) mod agg;
pub(crate) mod charge;
pub(crate) mod client_agg;
mod cms;
pub mod detected;
pub(crate) mod detected_probe;
pub mod error;
pub mod escape;
pub mod exec;
pub mod explain;
pub(crate) mod fold;
mod ip;
pub(crate) mod labels;
/// Issue #247: the `| logfmt <id>="<expr>"` extraction-expression
/// sub-grammar — a module of its own rather than more of [`pipeline`],
/// which is already over the file-size limit.
mod logfmt_expr;
pub mod order;
pub mod params;
pub mod pipeline;
pub mod plan;
pub(crate) mod post_agg;
/// Issue #286: the LEAF module holding `CheckedFragment`/`CheckedLiteral`/
/// `MonthLiteral` and every LogQL renderer that can emit a ClickHouse
/// `match(…)`. `pub`, not `pub(crate)`: the types appear in `pub fn`
/// signatures of the `pub` [`sql`] module, and `pulsus-server` passes
/// fragments through.
pub mod predicate;
pub mod rows;
pub mod sql;
pub mod template;
/// Test-only helpers shared by more than one region module's
/// `#[cfg(test)] mod tests` (issue #299). A SUBDIRECTORY, never a flat
/// `.rs`: both directory censuses over `src/logql/` are non-recursive and
/// filter on `.rs`, so a flat test-only file would be walked as production
/// source while a subdirectory is invisible to them by construction.
#[cfg(test)]
mod testkit;
pub(crate) mod variants;
pub(crate) mod walkbound;
/// Issue #277: the per-query response-warning accumulator. Its own module
/// rather than a corner of [`charge`], because it is a WIRE contract (the
/// reference's dedup-and-sort set) and not a resource bound.
pub mod warnings;
pub(crate) mod window;

pub use charge::{
    MAX_CLIENT_AGG_GROUP_BYTES, MAX_LEAF_RETAINED_BYTES, MAX_METRIC_RESULT_POINTS,
    MAX_QUERY_RETAINED_BYTES, MAX_STREAMS_RESULT_BYTES, RESULT_BUDGETS, ResultBudgets,
    StreamsResultBudget, ensure_result_series, result_series_breach,
};
pub use client_agg::{run_client_agg_rows, run_client_agg_rows_folded};
pub use detected::{DetectedFieldOut, DetectedFields, DetectedLabelOut, MAX_DETECTED_FIELD_BYTES};
pub use detected_probe::{DetectedFieldsProbe, MAX_FEEDER_SCRATCH_BYTES};
pub use error::{ReadError, TooBroadReason};
pub use exec::{
    EngineConfig, HistMatrixSeries, HistOrFloat, HistVectorSample, LogQlEngine, LogStats,
    MatrixSeries, PatternSeries, QueryResult, STREAM_FEED_CHUNK_BYTES, StreamAccumulator,
    StreamResult, StreamsFastPathProbe, StreamsPagedProbe, TAIL_REGISTRATION_GRACE_NS, TailCursor,
    TailLower, TailPage, TailSetup, VectorSample, VolumeAggregateBy, VolumeEntry, VolumeQuery,
    final_series_gate_applies, read_query_settings, run_pipeline_rows,
};
pub use explain::{ExplainStage, PlanExplain};
/// The structured-metadata context [`pipeline::CompiledPipeline::run_into_with_sm`]
/// takes. Re-exported (issue #334) because that entrypoint is `pub` while
/// its module is not, so the parameter type had no nameable path outside
/// the crate — and the stream/structured-metadata split it now carries is
/// what the `| json` collision matrix has to drive to reach the reference's
/// LIVE-category rule.
pub use labels::{EMPTY_STRUCTURED_METADATA, StructuredMetadataCtx};
/// Issue #406 R2, replacing the ROOT-ONLY predicate this module carried
/// until `e69d3f7` (deleted here; `git log -S` finds it): an instant
/// vector keeps the engine's order when a `sort`/`sort_desc`'s order
/// reaches the root through order-PRESERVING wrappers, and only then.
/// That predicate's doc justified being root-only by saying a nested
/// sort had its order destroyed upstream, and called that a match for
/// the reference. Measured 2026-08-11, the premise was false in both
/// halves: the reference's `Sortable` walks the WHOLE tree and does
/// suppress its re-sort under an aggregation — what it returns there is a
/// Go map walk, so there was never a stable order to match — and the
/// order is NOT destroyed under `label_replace`, a scalar operand or a
/// vector binary operand, where both engines carry it through. See
/// [`order`]'s module doc for the rule we implement instead and the
/// `nested-sort-order` ledger entry for what we deliberately do not.
pub use order::sorted_order_reaches_the_wire;
pub use params::{
    DEFAULT_MAX_STREAMS, Direction, MAX_DURATION_NS, PlanCtx, QueryParams, QuerySpec, TimeBounds,
    ValidatedDuration, validate_duration_ns,
};
pub use pipeline::{
    CompiledPipeline, EntryOut, MAX_JSON_FLATTEN_KEY_BYTES, MetricRun, PipelineError,
    RangeGrouping, RowBudget, RowBudgetExceeded, SAMPLE_EXTRACTION_ERROR,
};
pub use walkbound::{
    MAX_LOGQL_WALK_TRANSIENT_BYTES, REFERENCE_MAX_QUERY_BYTES, admit_logql_walk,
    walk_transient_bound,
};

pub use plan::{
    ClientAgg, ClientValue, LabelReplaceSpec, MAX_VARIANT_SUB_STATES, MetricNode, MetricNodeScc,
    MetricPlan, Plan, ProbePlan, RouteChoice, RoutingDecision, StreamsPlan, VariantSpec,
    check_query_span_ns, plan,
};
pub use post_agg::{
    B_INCLUDE, B_LABEL, B_MANY, B_PAIR, B_POINT, B_SERIES, BinaryTerm, ChainTerm,
    MAX_BINARY_PREFLIGHT_BYTES, MAX_POST_AGG_BYTES, PREFLIGHT_BYTES_PER_SERIES,
    PREFLIGHT_FLAT_BYTES, StageInput, W_APPROX_TOPK, W_GROUPNAME, W_LABEL_BYTE, W_PAIR, W_POINT,
    W_SERIES, W_STAGE_SERIES, apply_label_replace, apply_label_replace_capped, apply_vector_aggs,
    apply_vector_aggs_capped, binary_peak_bytes, binary_peak_bytes_without, combine_binary,
    combine_binary_capped, group_name_bytes, include_bytes, label_replace_peak_bytes,
    leaf_min_entry_bytes, measure_matrix, measure_vector, post_agg_peak_bytes,
    post_agg_peak_bytes_without, preflight_alloc_probe, preflight_scratch_bytes,
};
pub use variants::{
    MAX_VARIANT_FANOUT_STATE_BYTES, VARIANT_LABEL, VariantArena, VariantsAggState,
    append_variant_label, run_variants_rows,
};
pub use warnings::{Warnings, variant_series_warning};
pub use window::MAX_ADMITTED_GRID_POINTS;
pub use window::{ClientWindow, GridWindow, MAX_CLIENT_AGG_BUCKETS, materialize_vector_lit};
