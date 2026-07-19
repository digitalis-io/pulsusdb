//! Result-row shape for the issue #31 sample fetch, mirroring
//! `metrics/rows.rs`'s `#[derive(Row)]` convention: deserialized straight
//! off `ChClient::query_stream`.

use pulsus_clickhouse::Row;
use pulsus_model::HistogramColumns;
use serde::{Deserialize, Serialize};

/// One `metric_samples` row from [`super::sample_sql::sample_fetch`] /
/// [`super::sample_sql::sample_fetch_subquery`] (docs/schemas.md §2.3):
/// `SELECT fingerprint, unix_milli, value FROM metric_samples PREWHERE
/// metric_name = ... WHERE ... ORDER BY fingerprint, unix_milli`.
#[derive(Debug, Clone, Copy, PartialEq, Row, Serialize, Deserialize)]
pub struct SampleRow {
    pub fingerprint: u64,
    pub unix_milli: i64,
    pub value: f64,
}

/// One `metric_samples` row from [`super::sample_sql::sample_fetch_multi`]
/// (issue #85, M6-08c): the multi-metric fan-out fetch additionally
/// selects `metric_name`, because a fingerprint can exist under more than
/// one metric name (`metric_fingerprint` excludes `__name__`,
/// docs/schemas.md §2.1) — rows must group into per-`(metric_name,
/// fingerprint)` series, not per-fingerprint alone.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct MultiSampleRow {
    pub metric_name: String,
    pub fingerprint: u64,
    pub unix_milli: i64,
    pub value: f64,
}

/// One `metric_hist_samples` row from
/// [`super::sample_sql::hist_sample_fetch`] /
/// [`super::sample_sql::hist_sample_fetch_subquery`] (M7-A5a dual-read):
/// `SELECT fingerprint, unix_milli, <12 histogram value columns> FROM
/// metric_hist_samples …`. The value-column order is locked to the
/// catalog `CREATE` (id-23, `schema … custom_values`) and the writer row
/// (`MetricHistSampleRow`, minus its `metric_name`) — the read row is a
/// **separate** struct (a `MetricNameRow`-vs-`SeriesRow`-style column
/// subset, never the writer struct: reusing it would couple read to
/// write). No `Copy` (it owns `Vec`s) and no `PartialEq` derive
/// (`sum`/`zero_threshold`/`custom_values` may be NaN markers). `schema`
/// is `i8` — the physical `Int8` column width, widened on decode.
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct HistSampleRow {
    pub fingerprint: u64,
    pub unix_milli: i64,
    pub schema: i8,
    pub zero_threshold: f64,
    pub zero_count: u64,
    pub count: u64,
    pub sum: f64,
    pub pos_span_offsets: Vec<i32>,
    pub pos_span_lengths: Vec<u32>,
    pub pos_bucket_deltas: Vec<i64>,
    pub neg_span_offsets: Vec<i32>,
    pub neg_span_lengths: Vec<u32>,
    pub neg_bucket_deltas: Vec<i64>,
    pub custom_values: Vec<f64>,
}

impl HistSampleRow {
    /// Projects the value columns into [`HistogramColumns`] for
    /// `NativeHistogram::from_columns` (the trusted-storage decode; no
    /// re-validate — validation ran at the A4 ingest seam).
    pub fn to_columns(&self) -> HistogramColumns {
        HistogramColumns {
            schema: self.schema,
            zero_threshold: self.zero_threshold,
            zero_count: self.zero_count,
            count: self.count,
            sum: self.sum,
            pos_span_offsets: self.pos_span_offsets.clone(),
            pos_span_lengths: self.pos_span_lengths.clone(),
            pos_bucket_deltas: self.pos_bucket_deltas.clone(),
            neg_span_offsets: self.neg_span_offsets.clone(),
            neg_span_lengths: self.neg_span_lengths.clone(),
            neg_bucket_deltas: self.neg_bucket_deltas.clone(),
            custom_values: self.custom_values.clone(),
        }
    }
}

/// One `metric_hist_samples` row from
/// [`super::sample_sql::hist_sample_fetch_multi`] — the multi-metric
/// fan-out's histogram half, mirroring [`MultiSampleRow`]: a leading
/// `metric_name` so rows group into per-`(metric_name, fingerprint)`
/// series (a fingerprint can exist under more than one metric name).
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MultiHistSampleRow {
    pub metric_name: String,
    pub fingerprint: u64,
    pub unix_milli: i64,
    pub schema: i8,
    pub zero_threshold: f64,
    pub zero_count: u64,
    pub count: u64,
    pub sum: f64,
    pub pos_span_offsets: Vec<i32>,
    pub pos_span_lengths: Vec<u32>,
    pub pos_bucket_deltas: Vec<i64>,
    pub neg_span_offsets: Vec<i32>,
    pub neg_span_lengths: Vec<u32>,
    pub neg_bucket_deltas: Vec<i64>,
    pub custom_values: Vec<f64>,
}

impl MultiHistSampleRow {
    /// Projects the value columns into [`HistogramColumns`] — the
    /// multi-metric counterpart of [`HistSampleRow::to_columns`].
    pub fn to_columns(&self) -> HistogramColumns {
        HistogramColumns {
            schema: self.schema,
            zero_threshold: self.zero_threshold,
            zero_count: self.zero_count,
            count: self.count,
            sum: self.sum,
            pos_span_offsets: self.pos_span_offsets.clone(),
            pos_span_lengths: self.pos_span_lengths.clone(),
            pos_bucket_deltas: self.pos_bucket_deltas.clone(),
            neg_span_offsets: self.neg_span_offsets.clone(),
            neg_span_lengths: self.neg_span_lengths.clone(),
            neg_bucket_deltas: self.neg_bucket_deltas.clone(),
            custom_values: self.custom_values.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_row_derives_are_usable() {
        let a = SampleRow {
            fingerprint: 1,
            unix_milli: 1_000,
            value: 1.5,
        };
        let b = a;
        assert_eq!(a, b);
    }
}
