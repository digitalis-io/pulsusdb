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
//! - [`re2_authority`] *(issue #309)* — the conservative screen deciding
//!   which matcher patterns the warm cache may evaluate in-process at all;
//!   anything whose Rust-vs-RE2 acceptance is undecidable here degrades to
//!   the storage path, where issue #280's classifier owns the verdict.
//! - [`sql`] — pure fallback SQL builders, the snapshot-testing surface for
//!   the `metric_series` historical/JOIN fallback and (issue #31) the
//!   `SqlFallback` sample-fetch path's label hydration query.
//! - [`series_where`] *(issue #315)* — the leaf renderer for
//!   `metric_series` window bounds and matcher predicates. [`sql`]'s
//!   builders obtain the two only together, so the sanctioned components
//!   cannot render a user regex without the RE2 compile probe that makes
//!   an invalid pattern a 400 even when the window holds no rows. The
//!   #240 capability token is defined there too (review round 2), so no
//!   other module — descendants of `metrics` included — can construct it
//!   and reach the escaper directly. One boundary crossing is NOT
//!   sealed: the `_for_test` literal seam (the leaf's boundary inventory
//!   states what it permits; issue #328's D1 retires it).
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

mod dispatch;
pub mod exec;
pub mod labels;
pub mod matcher;
mod re2_authority;
pub mod refresh;
pub mod rows;
pub mod sample_rows;
pub mod sample_sql;
mod series_where;
pub mod sql;
pub mod stats;

/// The issue #240 capability token, re-exported so
/// [`crate::logql::escape::ch_regex_anchored_promql_re2`]'s pinned
/// signature can keep naming it as `crate::metrics::PromqlRe2Fallback`.
/// Only the NAME travels: the definition — private tuple field, private
/// `new` — lives in [`series_where`], so the token is constructible in
/// that leaf alone (issue #315, review round 2). Its full sealing
/// argument, with the measured rustc rejections for every other module,
/// is on the type itself.
pub(crate) use series_where::PromqlRe2Fallback;

pub use exec::{
    FetchProbe, MetricMeta, MetricQueryParams, MetricsConfig, MetricsEngine, TsdbStatus,
};
pub use labels::{
    CacheSnapshot, DEFAULT_STALENESS_MULTIPLIER, FallbackReason, LabelCache, LabelCacheConfig,
    LabelledResolution, MetricSeriesGroup, MultiMetricResolution, MultiMetricScanProbe, Resolution,
    SeriesResolver, TSDB_TOP_METRIC_NAMES, TsdbCacheSnapshot,
};
pub use matcher::{DataWindow, DiscoveryFilter, LabelMatcher, MatchOp};
#[doc(hidden)]
pub use re2_authority::pattern_requires_re2_authority_for_test;
pub use refresh::spawn_refresh_loop;
pub use rows::SeriesRow;
pub use sample_rows::SampleRow;
// Unsealed by design — exists because `tests/re2_screen_differential.rs`
// is an external binary. Issue #328's D1 extraction moved the SCREEN to
// `pulsus-re2` but this seam stays: the differential's SQL-meaning leg
// (#324) crosses the RENDERED literal, which only `series_where` can
// produce.
#[doc(hidden)]
pub use series_where::anchored_re2_literal_for_test;
pub use stats::{CacheMetrics, CacheMetricsSnapshot};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
