//! Time-aware label cache: activity buckets, cache window, and the SQL/JOIN
//! fallback (docs/architecture.md §5.2, docs/schemas.md §2.1, issue #30).
//! Mirrors [`super::logql`]'s plan/sql/exec split, but the public surface is
//! narrower and deliberately synchronous where it can be: a resident,
//! atomically-swapped snapshot (`fingerprint -> LabelSet` +
//! `metric_name -> sorted [fingerprint]`) is rebuilt every `PULSUS_CACHE_TTL`
//! by the §5.2 `LIMIT 1 BY` sweep over `PULSUS_CACHE_WINDOW`, and
//! [`labels::SeriesResolver::resolve`] is a **pure, synchronous** function
//! over the current snapshot — the only async/ClickHouse-touching code in
//! this module is the refresh sweep ([`refresh`]).
//!
//! **Module layout:**
//! - [`matcher`] — the issue #31 -> resolver contract: re-exports
//!   [`pulsus_model::LabelMatcher`]/[`pulsus_model::MatchOp`] (owned by
//!   `pulsus-model` per the #31 plan amendment's lands-second-rebases rule,
//!   #30 landing first) plus [`matcher::DataWindow`], the resolver-boundary
//!   window type that stays local to this crate.
//! - [`labels`] — the resolver core: [`labels::LabelCache`],
//!   [`labels::LabelCacheConfig`], [`labels::CacheSnapshot`],
//!   [`labels::SeriesResolver`], [`labels::Resolution`],
//!   [`labels::LabelledResolution`] (issue #31's labelled variant),
//!   [`labels::FallbackReason`]. In-process matcher evaluation (incl. a
//!   bounded compiled-regex cache) lives here.
//! - [`sql`] — pure fallback SQL builders, the snapshot-testing surface for
//!   the `metric_series` historical/JOIN fallback and (issue #31) the
//!   `SqlFallback` sample-fetch path's label hydration query.
//! - [`refresh`] — the only ClickHouse-touching code: the §5.2 sweep and
//!   [`refresh::spawn_refresh_loop`].
//! - [`rows`] — `ChClient` result-row shapes for the sweep.
//! - [`stats`] — [`stats::CacheMetrics`] atomics + a plain-value snapshot,
//!   mirroring `pulsus-write`'s `WriterMetrics` precedent.
//! - [`exec`] *(issue #31)* — [`exec::MetricsEngine`]: `pulsus_promql::plan`
//!   -> resolve/fetch -> `pulsus_promql::evaluate` orchestration, the only
//!   async/ClickHouse-touching code #31 added.
//! - [`sample_sql`] *(issue #31)* — pure `metric_samples` fetch SQL
//!   builders (the §2.3 fetch shape), snapshot-testable without a
//!   database.
//! - [`sample_rows`] *(issue #31)* — the sample fetch's `ChClient`
//!   result-row shape.
//!
//! **Time-awareness invariant (correctness, not optimization):** the cache
//! answers only queries whose full data window lies inside the cache
//! window. A series alive last week but silent today is absent from the
//! window-bounded snapshot; a historical query for it must resolve via
//! `metric_series` with bucket-floored bounds
//! ([`pulsus_model::floor_to_activity_bucket`]), never from the cache
//! (docs/architecture.md §5.2).
//!
//! **Cardinality guard is per-selector**, not a resident-cache cap: the
//! cache itself is bounded by the *time window*
//! ([`labels::LabelCacheConfig::window_ms`]); `PULSUS_CACHE_MAX_SERIES`
//! bounds how many fingerprints one in-process match may return before
//! degrading to the SQL/JOIN fallback (task-manager resolution #1 on issue
//! #30 — see the architecture.md §5.2 amendment for both roles stated
//! explicitly).

pub mod exec;
pub mod labels;
pub mod matcher;
pub mod refresh;
pub mod rows;
pub mod sample_rows;
pub mod sample_sql;
pub mod sql;
pub mod stats;

pub use exec::{MetricMeta, MetricQueryParams, MetricsConfig, MetricsEngine, TsdbStatus};
pub use labels::{
    CacheSnapshot, DEFAULT_STALENESS_MULTIPLIER, FallbackReason, LabelCache, LabelCacheConfig,
    LabelledResolution, MetricSeriesGroup, MultiMetricResolution, Resolution, SeriesResolver,
    TSDB_TOP_METRIC_NAMES, TsdbCacheSnapshot,
};
pub use matcher::{DataWindow, DiscoveryFilter, LabelMatcher, MatchOp};
pub use refresh::spawn_refresh_loop;
pub use rows::SeriesRow;
pub use sample_rows::SampleRow;
pub use stats::{CacheMetrics, CacheMetricsSnapshot};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
