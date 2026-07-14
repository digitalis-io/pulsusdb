//! PromQL planner and hybrid evaluation engine. See docs/architecture.md
//! §5.1. Pure — no I/O, no ClickHouse; `pulsus-read::metrics::exec` is the
//! only crate that resolves/fetches and then calls into this one
//! (`plan` -> `evaluate`).

pub mod error;
pub mod eval;
pub mod math;
pub mod parser;
pub mod plan;
pub mod value;

pub use error::PromqlError;
pub use eval::evaluate;
pub use math::KahanSum;
pub use parser::parse;
pub use plan::{
    AggOp, BinOp, CacheAnswerable, DEFAULT_LOOKBACK_MS, Grouping, Matching, OverTimeFn, PlanExpr,
    PlanParams, QueryPlan, RangeFn, SelectorId, SelectorSpec, plan, series_selector,
};
pub use value::{
    FetchedSeries, InstantSample, Labels, QueryValue, RangeSeries, Sample, SeriesData,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
