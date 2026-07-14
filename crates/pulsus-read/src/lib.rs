//! Query HTTP APIs, response encoders, and live tail. See
//! docs/architecture.md §5.

pub mod logql;

pub use logql::{
    DEFAULT_MAX_STREAMS, Direction, EngineConfig, ExplainStage, LogQlEngine, MatrixSeries, PlanCtx,
    PlanExplain, QueryParams, QueryResult, QuerySpec, ReadError, RouteChoice, RoutingDecision,
    StreamResult, TimeBounds, VectorSample,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
