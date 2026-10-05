//! Result-row shape for the issue #31 sample fetch, mirroring
//! `metrics/rows.rs`'s `#[derive(Row)]` convention: deserialized straight
//! off `ChClient::query_stream`.

use pulsus_clickhouse::Row;
use pulsus_model::{Fingerprint, HistogramColumns};
use serde::{Deserialize, Serialize};

/// One `metric_samples` row from [`super::sample_sql::sample_fetch`] /
/// [`super::sample_sql::sample_fetch_subquery`] (docs/schemas.md §2.3):
/// `SELECT fingerprint, unix_milli, value FROM metric_samples PREWHERE
/// metric_name = ... WHERE ... ORDER BY fingerprint, unix_milli`.
#[derive(Debug, Clone, Copy, PartialEq, Row, Serialize, Deserialize)]
pub struct SampleRow {
    pub fingerprint: Fingerprint,
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
    pub fingerprint: Fingerprint,
    pub unix_milli: i64,
    pub value: f64,
}

/// One `metric_hist_samples` row from
/// [`super::sample_sql::hist_sample_fetch`] /
/// [`super::sample_sql::hist_sample_fetch_subquery`] (M7-A5a dual-read):
/// `SELECT fingerprint, unix_milli, <13 histogram value columns> FROM
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
    pub fingerprint: Fingerprint,
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
    /// `counter_reset_hint` column (issue #125, migrations 27/28) — LAST,
    /// matching the SELECT list's trailing position (RowBinary is
    /// positional); pre-#125 rows read back the column `DEFAULT 0`
    /// (= Unknown).
    pub counter_reset_hint: u8,
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
            counter_reset_hint: self.counter_reset_hint,
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
    pub fingerprint: Fingerprint,
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
    /// See [`HistSampleRow::counter_reset_hint`] — LAST, matching the
    /// SELECT list (issue #125).
    pub counter_reset_hint: u8,
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
            counter_reset_hint: self.counter_reset_hint,
        }
    }
}

/// A histogram row's 13 value columns as the one-statement fetch carries
/// them (issue #623, [`super::sample_sql::sample_fetch`]): the tuple in
/// `metric_hist_samples`' column order.
pub type HistColumnsTuple = (
    i8,
    f64,
    u64,
    u64,
    f64,
    Vec<i32>,
    Vec<u32>,
    Vec<i64>,
    Vec<i32>,
    Vec<u32>,
    Vec<i64>,
    Vec<f64>,
    u8,
);

/// One row of the one-statement fetch over both sample tables (issue #623,
/// [`super::sample_sql::sample_fetch`] /
/// [`super::sample_sql::sample_fetch_subquery`]): a float row carries its
/// `value` and an empty `hist`; a histogram row carries `value = 0` and its
/// columns as the one element of `hist`. [`Self::split`] hands the merge the
/// two streams it had before.
/// No `Debug` or `Clone`: a 13-element tuple has neither.
#[derive(Row, Serialize, Deserialize)]
pub struct UnionSampleRow {
    pub fingerprint: Fingerprint,
    pub unix_milli: i64,
    pub hist: Vec<HistColumnsTuple>,
    pub value: f64,
}

/// The histogram row `h` stands for, at `(fingerprint, unix_milli)`.
fn hist_sample_row(
    fingerprint: Fingerprint,
    unix_milli: i64,
    h: HistColumnsTuple,
) -> HistSampleRow {
    HistSampleRow {
        fingerprint,
        unix_milli,
        schema: h.0,
        zero_threshold: h.1,
        zero_count: h.2,
        count: h.3,
        sum: h.4,
        pos_span_offsets: h.5,
        pos_span_lengths: h.6,
        pos_bucket_deltas: h.7,
        neg_span_offsets: h.8,
        neg_span_lengths: h.9,
        neg_bucket_deltas: h.10,
        custom_values: h.11,
        counter_reset_hint: h.12,
    }
}

impl UnionSampleRow {
    /// The float rows and the histogram rows, each in the order the
    /// statement returned them — ascending `(fingerprint, unix_milli)`.
    pub fn split(rows: Vec<Self>) -> (Vec<SampleRow>, Vec<HistSampleRow>) {
        let mut float = Vec::new();
        let mut hist = Vec::new();
        for r in rows {
            match r.hist.into_iter().next() {
                Some(h) => hist.push(hist_sample_row(r.fingerprint, r.unix_milli, h)),
                None => float.push(SampleRow {
                    fingerprint: r.fingerprint,
                    unix_milli: r.unix_milli,
                    value: r.value,
                }),
            }
        }
        (float, hist)
    }
}

/// [`UnionSampleRow`] for the multi-metric fan-out
/// ([`super::sample_sql::sample_fetch_multi`]), with a leading
/// `metric_name`.
/// No `Debug` or `Clone`: a 13-element tuple has neither.
#[derive(Row, Serialize, Deserialize)]
pub struct MultiUnionSampleRow {
    pub metric_name: String,
    pub fingerprint: Fingerprint,
    pub unix_milli: i64,
    pub hist: Vec<HistColumnsTuple>,
    pub value: f64,
}

impl MultiUnionSampleRow {
    /// [`UnionSampleRow::split`] for the fan-out: each stream in the order
    /// the statement returned it, ascending `(metric_name, fingerprint,
    /// unix_milli)`.
    pub fn split(rows: Vec<Self>) -> (Vec<MultiSampleRow>, Vec<MultiHistSampleRow>) {
        let mut float = Vec::new();
        let mut hist = Vec::new();
        for r in rows {
            match r.hist.into_iter().next() {
                Some(h) => {
                    let row = hist_sample_row(r.fingerprint, r.unix_milli, h);
                    hist.push(MultiHistSampleRow {
                        metric_name: r.metric_name,
                        fingerprint: row.fingerprint,
                        unix_milli: row.unix_milli,
                        schema: row.schema,
                        zero_threshold: row.zero_threshold,
                        zero_count: row.zero_count,
                        count: row.count,
                        sum: row.sum,
                        pos_span_offsets: row.pos_span_offsets,
                        pos_span_lengths: row.pos_span_lengths,
                        pos_bucket_deltas: row.pos_bucket_deltas,
                        neg_span_offsets: row.neg_span_offsets,
                        neg_span_lengths: row.neg_span_lengths,
                        neg_bucket_deltas: row.neg_bucket_deltas,
                        custom_values: row.custom_values,
                        counter_reset_hint: row.counter_reset_hint,
                    });
                }
                None => float.push(MultiSampleRow {
                    metric_name: r.metric_name,
                    fingerprint: r.fingerprint,
                    unix_milli: r.unix_milli,
                    value: r.value,
                }),
            }
        }
        (float, hist)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_row_derives_are_usable() {
        let a = SampleRow {
            fingerprint: Fingerprint::from_raw(1),
            unix_milli: 1_000,
            value: 1.5,
        };
        let b = a;
        assert_eq!(a, b);
    }
}
