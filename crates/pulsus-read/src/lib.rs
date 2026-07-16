//! Query HTTP APIs, response encoders, and live tail. See
//! docs/architecture.md §5.

pub mod logql;
pub mod metrics;
pub mod traces;

pub use logql::{
    DEFAULT_MAX_STREAMS, Direction, EngineConfig, ExplainStage, LogQlEngine, MatrixSeries, PlanCtx,
    PlanExplain, QueryParams, QueryResult, QuerySpec, ReadError, RouteChoice, RoutingDecision,
    StreamResult, TimeBounds, VectorSample,
};
pub use metrics::{
    CacheMetricsSnapshot, DEFAULT_STALENESS_MULTIPLIER, DataWindow, DiscoveryFilter,
    FallbackReason, LabelCache, LabelCacheConfig, LabelMatcher, LabelledResolution, MatchOp,
    MetricMeta, MetricQueryParams, MetricsConfig, MetricsEngine, Resolution, SeriesResolver,
    TSDB_TOP_METRIC_NAMES, TsdbCacheSnapshot, TsdbStatus, spawn_refresh_loop,
};
pub use traces::{
    BATCH_TRACES, DEFAULT_METRICS_POINTS, HYDRATION_BYTE_BUDGET, MAX_METRICS_POINTS,
    MAX_SPANS_PER_TRACE, MetricFunc, MetricsCtx, MetricsParams, PlanError as TracePlanError,
    RootSummary, SearchCtx, SearchOutput, SearchParams, SearchPlan, SpanFilterCtx, SpanSummary,
    StoredSpan, TAG_NAMES_MAX, TAG_VALUES_MAX, TRACE_METRICS_MAX_SET_BYTES,
    TRACE_METRICS_MAX_SET_ROWS, TagNames, TagValues, TraceEngine, TraceMetricsPlan,
    TraceReadConfig, TraceSearchResult, compile_span_filter, plan_search, plan_trace_metrics,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
