//! Query HTTP APIs, response encoders, and live tail. See
//! docs/architecture.md §5.

pub mod logql;
pub mod metrics;

pub use logql::{
    DEFAULT_MAX_STREAMS, Direction, EngineConfig, ExplainStage, LogQlEngine, MatrixSeries, PlanCtx,
    PlanExplain, QueryParams, QueryResult, QuerySpec, ReadError, RouteChoice, RoutingDecision,
    StreamResult, TimeBounds, VectorSample,
};
pub use metrics::{
    CacheMetricsSnapshot, DEFAULT_STALENESS_MULTIPLIER, DataWindow, FallbackReason, LabelCache,
    LabelCacheConfig, LabelMatcher, LabelledResolution, MatchOp, MetricQueryParams, MetricsConfig,
    MetricsEngine, Resolution, SeriesResolver, spawn_refresh_loop,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
