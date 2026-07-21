//! The seam between a metrics receiver (OTLP `/v1/metrics`, Prometheus
//! remote-write `/api/v1/write` — issue #27/#28, not built here) and the
//! writer core (issue #26): [`MetricSink`] plus the types an admitted batch
//! carries. Pure data + trait, no I/O — mirrors `ingest/mod.rs`'s
//! `ParsedLogs`/`LogSink` split (issue #8/#9's precedent).

use std::sync::Arc;

use pulsus_model::{Fingerprint, LabelSet, NativeHistogram};

use crate::ingest::{Backpressure, FlushWait};

/// One `metric_samples` row's source data (docs/schemas.md §2.1), produced
/// by a metrics receiver. No label data on this hot path — a fingerprint's
/// labels are resolved separately via [`SeriesRef`], mirroring
/// `pulsus_model::MetricSample`'s "no string data" shape.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricPoint {
    pub metric_name: Arc<str>,
    pub fingerprint: Fingerprint,
    /// Milliseconds since the Unix epoch, stored verbatim — never rounded
    /// or bucketed (docs/schemas.md §2.1: "resolution-agnostic"). The
    /// writer derives the `metric_series` activity bucket from this value;
    /// it never mutates it.
    pub unix_milli: i64,
    pub value: f64,
}

/// One `metric_hist_samples` row's source data (M7-A4, issue #120): a native
/// (sparse) histogram sample produced by the OTLP exponential-histogram
/// ingest path. Parallels [`MetricPoint`] (the float row's source), carrying
/// the base metric name, series fingerprint, and verbatim `unix_milli`; the
/// histogram value is the A3 [`NativeHistogram`] validated at the ingest
/// seam.
///
/// Hand-written [`PartialEq`] (so [`ParsedMetrics`] keeps its `derive`):
/// [`NativeHistogram`] deliberately has no `PartialEq` — its `f64` fields
/// (`sum`, `zero_threshold`, `custom_values`) may be NaN markers, so they are
/// compared by `to_bits()` here, everything else structurally.
#[derive(Debug, Clone)]
pub struct HistogramPoint {
    pub metric_name: Arc<str>,
    pub fingerprint: Fingerprint,
    /// Milliseconds since the Unix epoch, verbatim (same contract as
    /// [`MetricPoint::unix_milli`]).
    pub unix_milli: i64,
    pub histogram: NativeHistogram,
}

impl PartialEq for HistogramPoint {
    fn eq(&self, other: &Self) -> bool {
        let a = &self.histogram;
        let b = &other.histogram;
        self.metric_name == other.metric_name
            && self.fingerprint == other.fingerprint
            && self.unix_milli == other.unix_milli
            && a.schema == b.schema
            && a.zero_threshold.to_bits() == b.zero_threshold.to_bits()
            && a.zero_count == b.zero_count
            && a.count == b.count
            && a.sum.to_bits() == b.sum.to_bits()
            && a.positive_spans == b.positive_spans
            && a.negative_spans == b.negative_spans
            && a.positive_buckets == b.positive_buckets
            && a.negative_buckets == b.negative_buckets
            && a.custom_values.len() == b.custom_values.len()
            && a.custom_values
                .iter()
                .zip(&b.custom_values)
                .all(|(x, y)| x.to_bits() == y.to_bits())
    }
}

/// A `(metric_name, fingerprint)` series' resolved label set — the carrier
/// [`MetricWriter`](crate::writer::MetricWriter) reads to build a
/// `metric_series` registration row when that series is due one (docs/
/// schemas.md §2.1). One per distinct `(metric_name, fingerprint)` a
/// request touches, not one per sample.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesRef {
    pub metric_name: Arc<str>,
    pub fingerprint: Fingerprint,
    pub labels: LabelSet,
}

/// A `metric_metadata` descriptor (docs/schemas.md §2.1): remote-write
/// `MetricMetadata` / OTLP metric descriptors (name, type, help, unit).
#[derive(Debug, Clone, PartialEq)]
pub struct MetricMetadata {
    pub metric_name: Arc<str>,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
    /// The `ReplacingMergeTree(updated_ns)` version column (docs/schemas.md
    /// §2.1, issue #26 fix) — receiver-injected receive time (`now_ns`),
    /// the same source/role as `otlp_logs::StreamRow::updated_ns`.
    pub updated_ns: i64,
}

/// The normalized output a metrics receiver hands a [`MetricSink`]: rows
/// destined for `metric_samples`, candidate `metric_series` registrations,
/// candidate `metric_metadata` upserts, plus per-request accounting the
/// writer surfaces either as a metric (`collisions`) or as a partial-success
/// response (`rejected`, `rejected_message`) — mirrors `ParsedLogs`'s shape.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ParsedMetrics {
    /// One per data point.
    pub samples: Vec<MetricPoint>,
    /// One per distinct `(metric_name, fingerprint)` this request's samples
    /// touch — a labels carrier, not a per-sample registration; the writer
    /// derives the actual per-bucket `metric_series` rows from
    /// [`Self::samples`]' timestamps (docs/schemas.md §2.1's "one row per
    /// series per activity bucket").
    pub series: Vec<SeriesRef>,
    pub metadata: Vec<MetricMetadata>,
    /// Native-histogram samples destined for `metric_hist_samples` (M7-A4).
    /// Empty unless the request carried OTLP exponential histograms under a
    /// `native`/`dual` [`ExpHistogramMode`](pulsus_config::ExpHistogramMode).
    /// Their series are registered from [`Self::series`] just like float
    /// samples, but stamped `value_type = 1`.
    pub hist_samples: Vec<HistogramPoint>,
    /// Sum of every label set's normalized-key collision count across the
    /// whole request — never swallowed, surfaced for the writer's collision
    /// metric.
    pub collisions: u64,
    /// Count of individual data points dropped during parsing (not
    /// requests — a malformed/truncated payload is a whole-request error,
    /// never a `rejected` count).
    pub rejected: u64,
    /// The first rejection's error message, surfaced verbatim in a
    /// receiver's partial-success response.
    pub rejected_message: Option<String>,
}

/// The boundary a metrics receiver hands parsed batches across: admission
/// only, no batching/flush/ClickHouse-write logic lives on this side (issue
/// #26's domain). `Send + Sync` because a server holds an implementor
/// behind `axum::extract::State`, shared across concurrently-handled
/// requests — mirrors [`crate::ingest::LogSink`] exactly.
///
/// Reuses [`FlushWait`] (whose `Output` is `Result<(), LogsIngestError>`,
/// issue #8's fixed seam type) rather than a metrics-specific error type
/// (task-manager resolution, issue #26 open question #3): a metrics sink
/// only ever resolves `Ok` or a generic `FlushFailed`, never a
/// payload-shape-specific variant, so the existing type is sufficient for
/// now; a neutral `IngestError` rename is deferred to the M6 cleanup pass.
pub trait MetricSink: Send + Sync {
    /// Admits `batch` for async-mode requests: the caller responds
    /// immediately once this returns `Ok`, without waiting for the batch to
    /// be flushed.
    fn admit(&self, batch: ParsedMetrics) -> Result<(), Backpressure>;

    /// Admits `batch` for sync-mode requests: the caller `.await`s the
    /// returned [`FlushWait`] before responding.
    fn admit_flush(&self, batch: ParsedMetrics) -> Result<FlushWait, Backpressure>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_point_stores_fields_verbatim() {
        let point = MetricPoint {
            metric_name: Arc::from("http_requests_total"),
            fingerprint: 42,
            unix_milli: 1_700_000_000_000,
            value: 1.5,
        };
        assert_eq!(&*point.metric_name, "http_requests_total");
        assert_eq!(point.fingerprint, 42);
        assert_eq!(point.unix_milli, 1_700_000_000_000);
        assert_eq!(point.value, 1.5);
    }

    #[test]
    fn parsed_metrics_default_is_empty() {
        let parsed = ParsedMetrics::default();
        assert!(parsed.samples.is_empty());
        assert!(parsed.series.is_empty());
        assert!(parsed.metadata.is_empty());
        assert!(parsed.hist_samples.is_empty());
        assert_eq!(parsed.collisions, 0);
        assert_eq!(parsed.rejected, 0);
        assert_eq!(parsed.rejected_message, None);
    }
}
