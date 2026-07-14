//! Query HTTP APIs, response encoders, and live tail. See
//! docs/architecture.md §5.

pub mod logql;

pub use logql::{
    Direction, EngineConfig, LogQlEngine, PlanCtx, PlanExplain, QueryParams, QueryResult,
    QuerySpec, ReadError,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
