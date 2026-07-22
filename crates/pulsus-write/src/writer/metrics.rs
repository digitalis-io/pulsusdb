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

use crate::writer::spool::SpoolCounters;

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
    /// Spool *I/O* failures (issue #134, residual R5): a poison/uncertain
    /// batch whose audit-file write itself failed — no durable record
    /// exists for that batch. Bumped in both spool-error branches of
    /// `writer::table::finish_generation`; the batch's settlement is
    /// unaffected (spool writes are best-effort by design).
    pub spool_write_failures_total: AtomicU64,
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
    pub spool_write_failures_total: u64,
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
            spool_write_failures_total: self.spool_write_failures_total.load(Ordering::Relaxed),
        }
    }
}

/// Per-backlog registration-backfill counters (issues #134/#139): rows
/// entering the in-memory backlog after a Poisoned registration flush
/// (`enqueued`), rows rejected by the backlog byte cap (`dropped`),
/// re-insert attempts kept-for-retry on a pre-send retryable failure
/// (`retries`), rows confirmed re-inserted (`healed`), rows terminally
/// abandoned on a deterministic or uncertain re-insert outcome
/// (`abandoned`), and the `pending` gauge (rows currently in the backlog,
/// updated under the backlog lock). One instance per backlog:
/// `log_streams`, `metric_series`, `metric_metadata`, `trace_attrs_idx`.
#[derive(Debug, Default)]
pub struct BackfillMetrics {
    pub enqueued_total: AtomicU64,
    pub dropped_total: AtomicU64,
    pub retries_total: AtomicU64,
    pub healed_total: AtomicU64,
    pub abandoned_total: AtomicU64,
    pub pending: AtomicU64,
}

/// A point-in-time, plain-value copy of [`BackfillMetrics`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackfillMetricsSnapshot {
    pub enqueued_total: u64,
    pub dropped_total: u64,
    pub retries_total: u64,
    pub healed_total: u64,
    pub abandoned_total: u64,
    pub pending: u64,
}

impl BackfillMetrics {
    pub fn snapshot(&self) -> BackfillMetricsSnapshot {
        BackfillMetricsSnapshot {
            enqueued_total: self.enqueued_total.load(Ordering::Relaxed),
            dropped_total: self.dropped_total.load(Ordering::Relaxed),
            retries_total: self.retries_total.load(Ordering::Relaxed),
            healed_total: self.healed_total.load(Ordering::Relaxed),
            abandoned_total: self.abandoned_total.load(Ordering::Relaxed),
            pending: self.pending.load(Ordering::Relaxed),
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
    /// The `log_streams` registration-backfill counters (issue #134;
    /// generalized to the [`BackfillMetrics`] embed by issue #139 — the
    /// snapshot keeps the original flat `backfill_*` fields, filled from
    /// this embed, so #134's committed assertions are unchanged). Its own
    /// `Arc` so the backfill task holds a cheap clone.
    pub backfill: Arc<BackfillMetrics>,
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
    pub backfill_enqueued_total: u64,
    pub backfill_dropped_total: u64,
    pub backfill_retries_total: u64,
    pub backfill_healed_total: u64,
    pub backfill_abandoned_total: u64,
    pub backfill_pending: u64,
}

impl SpoolCounters for WriterMetrics {
    fn spool_poison_total(&self) -> &AtomicU64 {
        &self.spool_poison_total
    }

    fn spool_uncertain_total(&self) -> &AtomicU64 {
        &self.spool_uncertain_total
    }
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
            backfill_enqueued_total: self.backfill.enqueued_total.load(Ordering::Relaxed),
            backfill_dropped_total: self.backfill.dropped_total.load(Ordering::Relaxed),
            backfill_retries_total: self.backfill.retries_total.load(Ordering::Relaxed),
            backfill_healed_total: self.backfill.healed_total.load(Ordering::Relaxed),
            backfill_abandoned_total: self.backfill.abandoned_total.load(Ordering::Relaxed),
            backfill_pending: self.backfill.pending.load(Ordering::Relaxed),
        }
    }
}

/// [`WriterMetrics`]'s three-table counterpart for
/// [`crate::writer::MetricWriter`] (issue #26 architect plan): the same
/// shape, generalized from two tables (`samples`/`streams`) to three
/// (`samples`/`series`/`metadata`), plus metrics-specific counters
/// (`series_registrations_total`, `series_lru_hits/misses_total`,
/// `metadata_upserts_total`) replacing `WriterMetrics`'s log-specific
/// `stream_registrations_total`/`lru_hits/misses_total`. Reuses
/// `TableMetrics`/`TableMetricsSnapshot` unchanged — the per-table counters
/// mean the same thing regardless of family.
#[derive(Debug, Default)]
pub struct MetricWriterMetrics {
    pub samples: Arc<TableMetrics>,
    pub series: Arc<TableMetrics>,
    pub metadata: Arc<TableMetrics>,
    /// `metric_hist_samples` per-table counters (M7-A4, issue #120).
    pub hist_samples: Arc<TableMetrics>,
    pub backpressure_total: AtomicU64,
    pub spool_poison_total: AtomicU64,
    pub spool_uncertain_total: AtomicU64,
    pub series_registrations_total: AtomicU64,
    pub series_lru_hits_total: AtomicU64,
    pub series_lru_misses_total: AtomicU64,
    pub metadata_upserts_total: AtomicU64,
    pub collisions_total: AtomicU64,
    pub rejected_total: AtomicU64,
    /// `metric_series` registration-backfill counters (issue #139) —
    /// their own `Arc`s so each backfill task holds a cheap clone.
    pub series_backfill: Arc<BackfillMetrics>,
    /// `metric_metadata` registration-backfill counters (issue #139).
    pub metadata_backfill: Arc<BackfillMetrics>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricWriterMetricsSnapshot {
    pub samples: TableMetricsSnapshot,
    pub series: TableMetricsSnapshot,
    pub metadata: TableMetricsSnapshot,
    /// `metric_hist_samples` per-table counters (M7-A4, issue #120).
    pub hist_samples: TableMetricsSnapshot,
    /// The live `queued_bytes` gauge — passed in by the caller
    /// ([`crate::writer::MetricWriter::metrics`]), which owns the
    /// authoritative `AtomicU64` (not duplicated here).
    pub queue_bytes: u64,
    pub backpressure_total: u64,
    pub spool_poison_total: u64,
    pub spool_uncertain_total: u64,
    pub series_registrations_total: u64,
    pub series_lru_hits_total: u64,
    pub series_lru_misses_total: u64,
    pub metadata_upserts_total: u64,
    pub collisions_total: u64,
    pub rejected_total: u64,
    /// `metric_series` registration-backfill counters (issue #139 —
    /// additive fields; no pre-existing consumer reads this snapshot's
    /// full shape).
    pub series_backfill: BackfillMetricsSnapshot,
    /// `metric_metadata` registration-backfill counters (issue #139).
    pub metadata_backfill: BackfillMetricsSnapshot,
}

impl SpoolCounters for MetricWriterMetrics {
    fn spool_poison_total(&self) -> &AtomicU64 {
        &self.spool_poison_total
    }

    fn spool_uncertain_total(&self) -> &AtomicU64 {
        &self.spool_uncertain_total
    }
}

impl MetricWriterMetrics {
    pub fn snapshot(&self, queue_bytes: u64) -> MetricWriterMetricsSnapshot {
        MetricWriterMetricsSnapshot {
            samples: self.samples.snapshot(),
            series: self.series.snapshot(),
            metadata: self.metadata.snapshot(),
            hist_samples: self.hist_samples.snapshot(),
            queue_bytes,
            backpressure_total: self.backpressure_total.load(Ordering::Relaxed),
            spool_poison_total: self.spool_poison_total.load(Ordering::Relaxed),
            spool_uncertain_total: self.spool_uncertain_total.load(Ordering::Relaxed),
            series_registrations_total: self.series_registrations_total.load(Ordering::Relaxed),
            series_lru_hits_total: self.series_lru_hits_total.load(Ordering::Relaxed),
            series_lru_misses_total: self.series_lru_misses_total.load(Ordering::Relaxed),
            metadata_upserts_total: self.metadata_upserts_total.load(Ordering::Relaxed),
            collisions_total: self.collisions_total.load(Ordering::Relaxed),
            rejected_total: self.rejected_total.load(Ordering::Relaxed),
            series_backfill: self.series_backfill.snapshot(),
            metadata_backfill: self.metadata_backfill.snapshot(),
        }
    }
}

/// [`WriterMetrics`]'s two-table counterpart for
/// [`crate::writer::TraceWriter`] (issue #54): `spans`/`attrs` per-table
/// counters plus the shared backpressure/spool/rejected atomics. No
/// registration-cache counters (traces have no LRU/metadata caches —
/// `trace_tag_catalog` is MV-populated) and no `collisions_total`
/// (`ParsedTraces` has no label sets, so nothing collides).
#[derive(Debug, Default)]
pub struct TraceWriterMetrics {
    pub spans: Arc<TableMetrics>,
    pub attrs: Arc<TableMetrics>,
    pub backpressure_total: AtomicU64,
    pub spool_poison_total: AtomicU64,
    pub spool_uncertain_total: AtomicU64,
    pub rejected_total: AtomicU64,
    /// `trace_attrs_idx` registration-backfill counters (issue #139) —
    /// its own `Arc` so the backfill task holds a cheap clone.
    pub attrs_backfill: Arc<BackfillMetrics>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TraceWriterMetricsSnapshot {
    pub spans: TableMetricsSnapshot,
    pub attrs: TableMetricsSnapshot,
    /// The live `queued_bytes` gauge — passed in by the caller
    /// ([`crate::writer::TraceWriter::metrics`]), which owns the
    /// authoritative `AtomicU64` (not duplicated here).
    pub queue_bytes: u64,
    pub backpressure_total: u64,
    pub spool_poison_total: u64,
    pub spool_uncertain_total: u64,
    pub rejected_total: u64,
    /// `trace_attrs_idx` registration-backfill counters (issue #139 —
    /// additive field).
    pub attrs_backfill: BackfillMetricsSnapshot,
}

impl SpoolCounters for TraceWriterMetrics {
    fn spool_poison_total(&self) -> &AtomicU64 {
        &self.spool_poison_total
    }

    fn spool_uncertain_total(&self) -> &AtomicU64 {
        &self.spool_uncertain_total
    }
}

impl TraceWriterMetrics {
    pub fn snapshot(&self, queue_bytes: u64) -> TraceWriterMetricsSnapshot {
        TraceWriterMetricsSnapshot {
            spans: self.spans.snapshot(),
            attrs: self.attrs.snapshot(),
            queue_bytes,
            backpressure_total: self.backpressure_total.load(Ordering::Relaxed),
            spool_poison_total: self.spool_poison_total.load(Ordering::Relaxed),
            spool_uncertain_total: self.spool_uncertain_total.load(Ordering::Relaxed),
            rejected_total: self.rejected_total.load(Ordering::Relaxed),
            attrs_backfill: self.attrs_backfill.snapshot(),
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

    #[test]
    fn metric_writer_metrics_snapshot_carries_the_passed_in_queue_bytes() {
        let metrics = MetricWriterMetrics::default();
        let snap = metrics.snapshot(4096);
        assert_eq!(snap.queue_bytes, 4096);
    }

    #[test]
    fn trace_writer_metrics_snapshot_reflects_queue_bytes_and_counters() {
        let metrics = TraceWriterMetrics::default();
        metrics.backpressure_total.fetch_add(2, Ordering::Relaxed);
        metrics.rejected_total.fetch_add(5, Ordering::Relaxed);
        let snap = metrics.snapshot(4096);
        assert_eq!(snap.queue_bytes, 4096);
        assert_eq!(snap.backpressure_total, 2);
        assert_eq!(snap.rejected_total, 5);
    }

    #[test]
    fn backfill_metrics_snapshot_reflects_every_counter_and_the_gauge() {
        let metrics = BackfillMetrics::default();
        metrics.enqueued_total.fetch_add(1, Ordering::Relaxed);
        metrics.dropped_total.fetch_add(2, Ordering::Relaxed);
        metrics.retries_total.fetch_add(3, Ordering::Relaxed);
        metrics.healed_total.fetch_add(4, Ordering::Relaxed);
        metrics.abandoned_total.fetch_add(5, Ordering::Relaxed);
        metrics.pending.store(6, Ordering::Relaxed);
        assert_eq!(
            metrics.snapshot(),
            BackfillMetricsSnapshot {
                enqueued_total: 1,
                dropped_total: 2,
                retries_total: 3,
                healed_total: 4,
                abandoned_total: 5,
                pending: 6,
            }
        );
    }

    /// Issue #139: `WriterMetricsSnapshot`'s flat `backfill_*` fields are
    /// PRESERVED (filled from the embedded `BackfillMetrics`) so #134's
    /// committed snapshot assertions do not churn.
    #[test]
    fn writer_metrics_snapshot_flat_backfill_fields_mirror_the_embed() {
        let metrics = WriterMetrics::default();
        metrics
            .backfill
            .enqueued_total
            .fetch_add(7, Ordering::Relaxed);
        metrics
            .backfill
            .healed_total
            .fetch_add(3, Ordering::Relaxed);
        metrics.backfill.pending.store(4, Ordering::Relaxed);
        let snap = metrics.snapshot(0);
        assert_eq!(snap.backfill_enqueued_total, 7);
        assert_eq!(snap.backfill_healed_total, 3);
        assert_eq!(snap.backfill_pending, 4);
        assert_eq!(snap.backfill_dropped_total, 0);
    }

    #[test]
    fn metric_and_trace_writer_snapshots_carry_their_backfill_embeds() {
        let metrics = MetricWriterMetrics::default();
        metrics
            .series_backfill
            .healed_total
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .metadata_backfill
            .abandoned_total
            .fetch_add(2, Ordering::Relaxed);
        let snap = metrics.snapshot(0);
        assert_eq!(snap.series_backfill.healed_total, 1);
        assert_eq!(snap.metadata_backfill.abandoned_total, 2);

        let trace_metrics = TraceWriterMetrics::default();
        trace_metrics
            .attrs_backfill
            .enqueued_total
            .fetch_add(3, Ordering::Relaxed);
        assert_eq!(trace_metrics.snapshot(0).attrs_backfill.enqueued_total, 3);
    }

    #[test]
    fn metric_writer_metrics_snapshot_reflects_series_and_metadata_counters() {
        let metrics = MetricWriterMetrics::default();
        metrics
            .series_registrations_total
            .fetch_add(2, Ordering::Relaxed);
        metrics
            .series_lru_hits_total
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .series_lru_misses_total
            .fetch_add(2, Ordering::Relaxed);
        metrics
            .metadata_upserts_total
            .fetch_add(3, Ordering::Relaxed);
        let snap = metrics.snapshot(0);
        assert_eq!(snap.series_registrations_total, 2);
        assert_eq!(snap.series_lru_hits_total, 1);
        assert_eq!(snap.series_lru_misses_total, 2);
        assert_eq!(snap.metadata_upserts_total, 3);
    }
}
