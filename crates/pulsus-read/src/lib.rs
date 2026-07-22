//! Query HTTP APIs, response encoders, and live tail. See
//! docs/architecture.md §5.

pub mod eval_gate;
pub mod logql;
pub mod metrics;
pub mod querytext;
pub mod traces;

pub use eval_gate::{DEFAULT_EVAL_CONCURRENCY, EvalGate, EvalGateSnapshot};
pub use logql::{
    DEFAULT_MAX_STREAMS, DetectedFieldOut, DetectedFields, DetectedLabelOut, Direction,
    EngineConfig, ExplainStage, HistMatrixSeries, HistOrFloat, HistVectorSample, LogQlEngine,
    LogStats, MatrixSeries, PatternSeries, PlanCtx, PlanExplain, QueryParams, QueryResult,
    QuerySpec, ReadError, RouteChoice, RoutingDecision, StreamResult, TAIL_REGISTRATION_GRACE_NS,
    TailCursor, TailLower, TailPage, TailSetup, TimeBounds, VectorSample, VolumeAggregateBy,
    VolumeEntry, VolumeQuery,
};
pub use metrics::{
    CacheMetricsSnapshot, DEFAULT_STALENESS_MULTIPLIER, DataWindow, DiscoveryFilter,
    FallbackReason, FetchProbe, LabelCache, LabelCacheConfig, LabelMatcher, LabelledResolution,
    MatchOp, MetricMeta, MetricQueryParams, MetricsConfig, MetricsEngine, Resolution,
    SeriesResolver, TSDB_TOP_METRIC_NAMES, TsdbCacheSnapshot, TsdbStatus, spawn_refresh_loop,
};
pub use querytext::{MAX_QUERY_TEXT_BYTES, ensure_query_text_fits};
pub use traces::{
    BATCH_TRACES, CANDIDATE_TUPLE_BYTES, DEFAULT_METRICS_POINTS, HYDRATION_BYTE_BUDGET,
    MAX_METRICS_POINTS, MAX_SPANS_PER_TRACE, MetricFunc, MetricsCtx, MetricsParams,
    PlanError as TracePlanError, RETAINED_ENTRY_OVERHEAD, RootSummary, SearchCtx, SearchOutput,
    SearchParams, SearchPlan, SpanFilterCtx, SpanSummary, StoredSpan, TAG_NAMES_MAX,
    TAG_VALUES_MAX, TRACE_METRICS_MAX_SET_BYTES, TRACE_METRICS_MAX_SET_ROWS,
    TRACE_SEARCH_MAX_BLOCK_ROWS, TagNames, TagValues, TraceEngine, TraceMetricsPlan,
    TraceReadConfig, TraceSearchResult, compile_span_filter, plan_search, plan_trace_metrics,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
