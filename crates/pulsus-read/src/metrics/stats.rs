//! [`CacheMetrics`]: the label cache's atomics inventory. Structurally
//! mirrors `pulsus-write::writer::metrics::WriterMetrics`'s precedent
//! (atomics + a plain-value [`CacheMetrics::snapshot`]) — but, unlike
//! `WriterMetrics` (deferred under its own issue), this snapshot **is**
//! bridged into the Prometheus `/metrics` exposition: the issue #30 AC
//! explicitly names cache hit/size/age metrics on `/metrics`, so the
//! bridge (`pulsus_server::ops::metrics_handler`, reader-mode only) is in
//! scope for this issue even though the `WriterMetrics` precedent's own
//! bridge is not. `hits`/`miss_*` cover every [`super::labels::Resolution`]
//! branch (a hit is `Resolution::Fingerprints`, each miss variant maps 1:1
//! to a `super::labels::FallbackReason`); `series_count`/`oversize` are
//! gauges updated once per successful refresh sweep (task-manager
//! resolution #1 on issue #30: `series_count` "feeds" the #34 scale
//! benchmark, `oversize` is the advisory degraded/oversize signal for a
//! sweep whose resident size exceeds `PULSUS_CACHE_MAX_SERIES`). Cache
//! *age* is not stored here — it is scrape-time-derived
//! ([`super::labels::LabelCache::age_ms`]), since it changes continuously
//! and a stored atomic would go stale the instant it was written.

use std::sync::atomic::{AtomicU64, Ordering};

/// The whole label cache's atomics.
#[derive(Debug, Default)]
pub struct CacheMetrics {
    /// `Resolution::Fingerprints` — answered entirely in-process.
    pub hits_total: AtomicU64,
    /// `FallbackReason::ColdCache`.
    pub miss_cold_total: AtomicU64,
    /// `FallbackReason::StaleCache`.
    pub miss_stale_total: AtomicU64,
    /// `FallbackReason::OutOfWindow`.
    pub miss_out_of_window_total: AtomicU64,
    /// `FallbackReason::OverCardinality`.
    pub miss_over_cardinality_total: AtomicU64,
    /// `FallbackReason::RegexUnsupported`.
    pub miss_regex_unsupported_total: AtomicU64,
    /// Issue #89 (retroactive re-review): a multi-metric resolution's
    /// cache-enumeration walk examined more entries (names plus candidate
    /// fingerprints) than `ReaderConfig::promql_max_cache_scan` before it
    /// could finish — [`super::labels::MultiMetricResolution::
    /// ScanBudgetExceeded`], never a [`FallbackReason`] (a warm cache that
    /// reaches this bound is not degraded).
    pub miss_scan_budget_total: AtomicU64,
    /// Successful sweeps (each one swaps in a new snapshot).
    pub refreshes_total: AtomicU64,
    /// Sweeps that failed (transient `ChError`) — the last good snapshot
    /// keeps serving, rising in age until `StaleCache` takes over.
    pub refresh_failures_total: AtomicU64,
    /// Gauge: resident fingerprint count as of the most recent successful
    /// sweep.
    pub series_count: AtomicU64,
    /// Gauge (0/1, read as `bool`): whether the most recent successful
    /// sweep's `series_count` exceeded `PULSUS_CACHE_MAX_SERIES` — advisory
    /// only, the cache still serves everything it holds (docs/architecture.md
    /// §5.2 amendment: `PULSUS_CACHE_MAX_SERIES` is a per-selector guard,
    /// not a resident-cache cap; this gauge is the "over the recommended
    /// size" signal, not a correctness gate).
    pub oversize: AtomicU64,
}

/// A point-in-time, plain-value copy of [`CacheMetrics`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheMetricsSnapshot {
    pub hits_total: u64,
    pub miss_cold_total: u64,
    pub miss_stale_total: u64,
    pub miss_out_of_window_total: u64,
    pub miss_over_cardinality_total: u64,
    pub miss_regex_unsupported_total: u64,
    pub miss_scan_budget_total: u64,
    pub refreshes_total: u64,
    pub refresh_failures_total: u64,
    pub series_count: u64,
    pub oversize: bool,
}

impl CacheMetrics {
    /// Records one successful sweep: bumps `refreshes_total` and updates
    /// the `series_count`/`oversize` gauges from the freshly built
    /// snapshot's size.
    pub(crate) fn record_refresh(&self, series_count: u64, cache_max_series: u64) {
        self.refreshes_total.fetch_add(1, Ordering::Relaxed);
        self.series_count.store(series_count, Ordering::Relaxed);
        self.oversize.store(
            u64::from(series_count > cache_max_series),
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_refresh_failure(&self) {
        self.refresh_failures_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            hits_total: self.hits_total.load(Ordering::Relaxed),
            miss_cold_total: self.miss_cold_total.load(Ordering::Relaxed),
            miss_stale_total: self.miss_stale_total.load(Ordering::Relaxed),
            miss_out_of_window_total: self.miss_out_of_window_total.load(Ordering::Relaxed),
            miss_over_cardinality_total: self.miss_over_cardinality_total.load(Ordering::Relaxed),
            miss_regex_unsupported_total: self.miss_regex_unsupported_total.load(Ordering::Relaxed),
            miss_scan_budget_total: self.miss_scan_budget_total.load(Ordering::Relaxed),
            refreshes_total: self.refreshes_total.load(Ordering::Relaxed),
            refresh_failures_total: self.refresh_failures_total.load(Ordering::Relaxed),
            series_count: self.series_count.load(Ordering::Relaxed),
            oversize: self.oversize.load(Ordering::Relaxed) != 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_refresh_updates_the_series_count_and_refreshes_counter() {
        let metrics = CacheMetrics::default();
        metrics.record_refresh(100, 50_000);
        let snap = metrics.snapshot();
        assert_eq!(snap.refreshes_total, 1);
        assert_eq!(snap.series_count, 100);
        assert!(!snap.oversize);
    }

    #[test]
    fn record_refresh_sets_the_oversize_gauge_when_series_count_exceeds_the_cap() {
        let metrics = CacheMetrics::default();
        metrics.record_refresh(60_000, 50_000);
        assert!(metrics.snapshot().oversize);
    }

    #[test]
    fn record_refresh_clears_the_oversize_gauge_on_a_later_smaller_sweep() {
        let metrics = CacheMetrics::default();
        metrics.record_refresh(60_000, 50_000);
        assert!(metrics.snapshot().oversize);
        metrics.record_refresh(10, 50_000);
        assert!(!metrics.snapshot().oversize);
    }

    #[test]
    fn record_refresh_failure_bumps_the_failure_counter_only() {
        let metrics = CacheMetrics::default();
        metrics.record_refresh_failure();
        let snap = metrics.snapshot();
        assert_eq!(snap.refresh_failures_total, 1);
        assert_eq!(snap.refreshes_total, 0);
    }

    #[test]
    fn hit_and_miss_counters_are_independently_addressable() {
        let metrics = CacheMetrics::default();
        metrics.hits_total.fetch_add(5, Ordering::Relaxed);
        metrics.miss_cold_total.fetch_add(1, Ordering::Relaxed);
        metrics.miss_stale_total.fetch_add(2, Ordering::Relaxed);
        metrics
            .miss_out_of_window_total
            .fetch_add(3, Ordering::Relaxed);
        metrics
            .miss_over_cardinality_total
            .fetch_add(4, Ordering::Relaxed);
        metrics
            .miss_regex_unsupported_total
            .fetch_add(6, Ordering::Relaxed);
        metrics
            .miss_scan_budget_total
            .fetch_add(7, Ordering::Relaxed);
        let snap = metrics.snapshot();
        assert_eq!(snap.hits_total, 5);
        assert_eq!(snap.miss_cold_total, 1);
        assert_eq!(snap.miss_stale_total, 2);
        assert_eq!(snap.miss_out_of_window_total, 3);
        assert_eq!(snap.miss_over_cardinality_total, 4);
        assert_eq!(snap.miss_regex_unsupported_total, 6);
        assert_eq!(snap.miss_scan_budget_total, 7);
    }
}
