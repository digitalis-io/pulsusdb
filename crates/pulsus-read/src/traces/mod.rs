//! Trace read path: the docs/schemas.md §4.2 trace-by-ID point read
//! (issue #55) and the two-phase TraceQL search (issue #57). Deliberately
//! **OTLP-agnostic** (task-manager adjudication on issue #55, open
//! question 1): this module speaks SQL and streamed rows only — no
//! `prost`/`opentelemetry-proto` dependency enters this crate. Decoding
//! stored per-span payloads and shaping API responses live server-side
//! (`pulsus-server/src/traces_api`), mirroring the logs layering.
//!
//! **Module layout** mirrors [`crate::logql`]'s plan/execute split:
//! [`filter`] (the shared span-filter compiler — the leaf-level surface
//! the metrics endpoints consume too), [`search_plan`] (pure `Query →
//! SearchPlan`), [`search_sql`] (the pure, byte-frozen SQL builders),
//! [`search_eval`] (the pure Phase-2 exact evaluator), [`metrics_plan`]/
//! [`metrics_sql`] (the issue #59 TraceQL metrics planner + byte-frozen
//! single-query pushdown builders), [`tags_sql`] (the pure §4.3
//! tag-discovery builders — catalog-only, issue #58), [`sql`]/[`rows`]
//! (point-read builder + `ChClient` result-row shapes), and [`exec`]
//! (`TraceEngine`, the only module here that talks to ClickHouse).

pub mod exec;
pub mod filter;
pub mod metrics_plan;
pub mod metrics_sql;
pub mod rows;
pub mod search_eval;
pub mod search_plan;
pub mod search_sql;
pub mod sql;
pub mod tags_sql;

pub use exec::{
    BATCH_TRACES, CANDIDATE_TUPLE_BYTES, HYDRATION_BYTE_BUDGET, MAX_SPANS_PER_TRACE,
    RETAINED_ENTRY_OVERHEAD, RootSummary, SearchOutput, TAG_NAMES_MAX, TAG_VALUES_MAX,
    TRACE_METRICS_MAX_SET_BYTES, TRACE_METRICS_MAX_SET_ROWS, TRACE_SEARCH_MAX_BLOCK_ROWS, TagNames,
    TagValues, TraceEngine, TraceReadConfig, TraceSearchResult,
};
pub use filter::{CompiledLeaf, CompiledSpanFilter, PlanError, SpanFilterCtx, compile_span_filter};
pub use metrics_plan::{
    DEFAULT_METRICS_POINTS, MAX_METRICS_POINTS, MetricFunc, MetricsCtx, MetricsParams,
    TraceMetricsPlan, plan_trace_metrics,
};
pub use rows::{StoredSpan, StoredSpanRow, TagNameRow, TagValueRow};
pub use search_eval::SpanSummary;
pub use search_plan::{SearchCtx, SearchParams, SearchPlan, plan_search};
