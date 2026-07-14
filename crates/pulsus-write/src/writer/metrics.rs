//! `WriterMetrics`: the atomics inventory the architect plan enumerates
//! (rows/bytes/flushes/flush-latency/retries/inflight per table; a
//! `queue_bytes` gauge; `backpressure_total`; `spool_total{poison,
//! uncertain}`; `stream_registrations_total`; `lru_hits/misses_total`;
//! `collisions_total`/`rejected_total`). `/metrics` exposition is the
//! server's job (architect plan, "out of scope") — this crate only
//! maintains the atomics and exposes a plain-value [`WriterMetrics::snapshot`]
//! for whatever renders them.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Per-table counters (one instance each for `log_samples`/`log_streams`).
#[derive(Debug, Default)]
pub struct TableMetrics {
    pub rows_total: AtomicU64,
    pub bytes_total: AtomicU64,
    pub flushes_total: AtomicU64,
    pub flush_latency_sum_ns: AtomicU64,
    pub flush_latency_count: AtomicU64,
    pub retries_total: AtomicU64,
    /// Number of generations currently being inserted (0 or 1 per table
    /// with today's single-flush-task-per-table design; a gauge, not a
    /// counter, so a snapshot can read it without inferring from
    /// flushes-in-progress).
    pub inflight: AtomicU64,
}

/// A point-in-time, plain-value copy of [`TableMetrics`] — cheap to pass
/// around/compare in tests, unlike the atomics themselves.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableMetricsSnapshot {
    pub rows_total: u64,
    pub bytes_total: u64,
    pub flushes_total: u64,
    pub flush_latency_sum_ns: u64,
    pub flush_latency_count: u64,
    pub retries_total: u64,
    pub inflight: u64,
}

impl TableMetrics {
    /// Records one successful flush: rows/bytes/flushes bump by one
    /// generation's worth, plus its latency into the running sum/count
    /// (a caller renders `sum / count` for the mean, or exports both
    /// raw for a histogram-style aggregation upstream).
    pub fn record_flush(&self, rows: u64, bytes: u64, latency: Duration) {
        self.rows_total.fetch_add(rows, Ordering::Relaxed);
        self.bytes_total.fetch_add(bytes, Ordering::Relaxed);
        self.flushes_total.fetch_add(1, Ordering::Relaxed);
        self.flush_latency_sum_ns
            .fetch_add(latency.as_nanos() as u64, Ordering::Relaxed);
        self.flush_latency_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TableMetricsSnapshot {
        TableMetricsSnapshot {
            rows_total: self.rows_total.load(Ordering::Relaxed),
            bytes_total: self.bytes_total.load(Ordering::Relaxed),
            flushes_total: self.flushes_total.load(Ordering::Relaxed),
            flush_latency_sum_ns: self.flush_latency_sum_ns.load(Ordering::Relaxed),
            flush_latency_count: self.flush_latency_count.load(Ordering::Relaxed),
            retries_total: self.retries_total.load(Ordering::Relaxed),
            inflight: self.inflight.load(Ordering::Relaxed),
        }
    }
}

/// The whole writer's atomics. `samples`/`streams` are behind their own
/// `Arc` so each table's flush task can hold a cheap clone without
/// needing the rest of this struct.
#[derive(Debug, Default)]
pub struct WriterMetrics {
    pub samples: Arc<TableMetrics>,
    pub streams: Arc<TableMetrics>,
    pub backpressure_total: AtomicU64,
    pub spool_poison_total: AtomicU64,
    pub spool_uncertain_total: AtomicU64,
    pub stream_registrations_total: AtomicU64,
    pub lru_hits_total: AtomicU64,
    pub lru_misses_total: AtomicU64,
    pub collisions_total: AtomicU64,
    pub rejected_total: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriterMetricsSnapshot {
    pub samples: TableMetricsSnapshot,
    pub streams: TableMetricsSnapshot,
    /// The live `queued_bytes` gauge — passed in by the caller
    /// ([`crate::writer::LogWriter::metrics`]), which owns the
    /// authoritative `AtomicU64` (not duplicated here).
    pub queue_bytes: u64,
    pub backpressure_total: u64,
    pub spool_poison_total: u64,
    pub spool_uncertain_total: u64,
    pub stream_registrations_total: u64,
    pub lru_hits_total: u64,
    pub lru_misses_total: u64,
    pub collisions_total: u64,
    pub rejected_total: u64,
}

impl WriterMetrics {
    pub fn snapshot(&self, queue_bytes: u64) -> WriterMetricsSnapshot {
        WriterMetricsSnapshot {
            samples: self.samples.snapshot(),
            streams: self.streams.snapshot(),
            queue_bytes,
            backpressure_total: self.backpressure_total.load(Ordering::Relaxed),
            spool_poison_total: self.spool_poison_total.load(Ordering::Relaxed),
            spool_uncertain_total: self.spool_uncertain_total.load(Ordering::Relaxed),
            stream_registrations_total: self.stream_registrations_total.load(Ordering::Relaxed),
            lru_hits_total: self.lru_hits_total.load(Ordering::Relaxed),
            lru_misses_total: self.lru_misses_total.load(Ordering::Relaxed),
            collisions_total: self.collisions_total.load(Ordering::Relaxed),
            rejected_total: self.rejected_total.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_flush_updates_rows_bytes_flushes_and_latency() {
        let metrics = TableMetrics::default();
        metrics.record_flush(10, 1024, Duration::from_millis(5));
        let snap = metrics.snapshot();
        assert_eq!(snap.rows_total, 10);
        assert_eq!(snap.bytes_total, 1024);
        assert_eq!(snap.flushes_total, 1);
        assert_eq!(snap.flush_latency_count, 1);
        assert!(snap.flush_latency_sum_ns > 0);
    }

    #[test]
    fn writer_metrics_snapshot_carries_the_passed_in_queue_bytes() {
        let metrics = WriterMetrics::default();
        let snap = metrics.snapshot(4096);
        assert_eq!(snap.queue_bytes, 4096);
    }

    #[test]
    fn writer_metrics_snapshot_reflects_backpressure_and_spool_counters() {
        let metrics = WriterMetrics::default();
        metrics.backpressure_total.fetch_add(2, Ordering::Relaxed);
        metrics.spool_poison_total.fetch_add(1, Ordering::Relaxed);
        metrics
            .spool_uncertain_total
            .fetch_add(3, Ordering::Relaxed);
        let snap = metrics.snapshot(0);
        assert_eq!(snap.backpressure_total, 2);
        assert_eq!(snap.spool_poison_total, 1);
        assert_eq!(snap.spool_uncertain_total, 3);
    }
}
