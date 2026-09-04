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
//! single-query pushdown builders), [`log2_histogram`] (the reference's
//! pure `histogram_over_time` bucket rule, issue #252), [`tags_sql`]
//! (the pure §4.3
//! tag-discovery builders — the two catalog-only ones of issue #58 and
//! the two store-backed ones of issue #478), [`tag_narrow`] (the issue
//! #478 `q`-to-terms lowering), [`sql`]/[`rows`]
//! (point-read builder + `ChClient` result-row shapes), `dispatch` (the
//! private issue #509 choke point that owns the `ChClient` and is the
//! only place a `?` in query text is doubled), and [`exec`]
//! (`TraceEngine`, which plans and frames every read but reaches
//! ClickHouse only through `dispatch`).

// Issue #509: private to `traces`. The whole point is that `exec.rs`
// cannot obtain the `ChClient` any other way, so this must not be `pub`.
mod dispatch;

pub mod compile;
pub mod exec;
pub mod filter;
pub mod graph_sql;
pub mod log2_histogram;
pub mod metrics_plan;
pub mod metrics_result;
pub mod metrics_sql;
pub mod rows;
pub mod search_eval;
pub mod search_plan;
pub mod search_sql;

// Issue #282 deleted this module's `TraceqlPrevalidated` capability token
// (issue #240). `filter.rs` now renders every user regex through
// `logql::escape::ch_regex_anchored_checked`, which compiles the exact
// string it escapes, so nothing in `traces/` needs — or can obtain —
// access to the raw escapers: they are private to `logql::escape` and a
// call from here is an `E0603`. The exemption list is PromQL only, and
// that one is permanent by design.

pub mod sql;
pub mod tag_narrow;
pub mod tags_sql;

pub use exec::{
    BATCH_TRACES, CANDIDATE_TUPLE_BYTES, HYDRATION_BYTE_BUDGET, MAX_SPANS_PER_TRACE,
    RETAINED_ENTRY_OVERHEAD, RootSummary, SPAN_NAME_VALUE_TYPE, SearchOutput, ServiceGraph,
    TAG_NAMES_MAX, TAG_VALUES_MAX, TRACE_METRICS_MAX_SET_BYTES, TRACE_METRICS_MAX_SET_ROWS,
    TRACE_SEARCH_MAX_BLOCK_ROWS, TagNames, TagValue, TagValues, TagValuesRequest, TraceContext,
    TraceEngine, TraceReadConfig, TraceSearchResult,
};
pub use filter::{CompiledLeaf, CompiledSpanFilter, PlanError, SpanFilterCtx, compile_span_filter};
pub use graph_sql::{GraphWindow, SERVICE_GRAPH_MAX_EDGES, service_graph_sql};
pub use metrics_plan::{
    DEFAULT_METRICS_POINTS, MAX_METRICS_POINTS, MetricFunc, MetricsCtx, MetricsParams,
    TraceMetricsPlan, plan_trace_metrics,
};
pub use metrics_result::{
    MetricExemplar, MetricLabel, MetricLabelValue, TraceMetricSeries, TraceMetricsResult,
};
pub use rows::{GraphEdgeRow, SpanNameRow, StoredSpan, StoredSpanRow, TagNameRow, TagValueRow};
pub use search_eval::{
    GroupValue, ProjectedAttribute, SpanSetGroup, SpanSummary, StoredType,
    non_finite_double_spelling, wire_arm,
};
pub use search_plan::{SearchCtx, SearchParams, SearchPlan, WireKey, plan_search};
