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
    BATCH_TRACES, HYDRATION_BYTE_BUDGET, MAX_SPANS_PER_TRACE, PlanError as TracePlanError,
    RootSummary, SearchCtx, SearchOutput, SearchParams, SearchPlan, SpanFilterCtx, SpanSummary,
    StoredSpan, TraceEngine, TraceReadConfig, TraceSearchResult, compile_span_filter, plan_search,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
