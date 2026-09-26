//! ClickHouse row shapes for `log_samples`/`log_streams` (docs/schemas.md
//! §3.1), their conversions from the parser's `LogRow`/`StreamRow` (issue
//! #8), and a byte-size estimate used for both the `PULSUS_BATCH_BYTES`
//! flush threshold and the `PULSUS_INGEST_QUEUE_BYTES` admission
//! reservation (architect plan).

use pulsus_clickhouse::Row;
use pulsus_model::{Fingerprint, LabelSet};
use serde::{Deserialize, Serialize};

use crate::ingest::metrics::{HistogramPoint, MetricMetadata, MetricPoint, SeriesRef};
use crate::ingest::traces::{AttrRecord, SpanRecord};
use crate::protocols::otlp_logs::{LogRow, StreamRow};
use crate::writer::backfill::BackfillRow;
use crate::writer::registration::StreamKey;
use crate::writer::spool::SpoolEncode;

/// One `log_samples` row (docs/schemas.md §3.1). `structured_metadata` is a
/// canonical sorted-key JSON String (issue #97), the LAST field so the
/// clickhouse-0.15.1 explicit-column INSERT column list stays append-only and
/// aligns with the additive `ADD COLUMN` migration (catalog id 21). Empty
/// string = no structured metadata (matches the column's `DEFAULT ''`, so
/// pre-column rows read back identically).
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct LogSampleRow {
    pub service: String,
    pub fingerprint: Fingerprint,
    pub timestamp_ns: i64,
    pub severity: i8,
    pub body: String,
    pub structured_metadata: String,
}

impl From<&LogRow> for LogSampleRow {
    fn from(row: &LogRow) -> Self {
        LogSampleRow {
            service: row.service.clone(),
            fingerprint: row.fingerprint,
            timestamp_ns: row.timestamp_ns.0,
            severity: row.severity,
            body: row.body.clone(),
            structured_metadata: row.structured_metadata.clone(),
        }
    }
}

impl LogSampleRow {
    /// A conservative in-memory byte estimate (own-fields, not a
    /// RowBinary wire-format size): every `String` field's byte length
    /// plus the fixed-width columns' true size. Good enough to bound
    /// memory for both the flush-size threshold and the queue-bytes
    /// admission gate without modelling RowBinary encoding exactly.
    pub fn est_bytes(&self) -> u64 {
        Self::estimate(&self.service, &self.body, self.structured_metadata.len())
    }

    /// Estimates a `LogRow`'s footprint *before* it is materialized into a
    /// `LogSampleRow` — the reserve-before-materialize hardening
    /// (architect plan amendment 3, finding 2): identical accounting to
    /// [`Self::est_bytes`], read straight off the source row so
    /// `writer::LogWriter::admit_batch`'s `fetch_add` reservation can
    /// happen before the clone that builds the target row.
    pub fn est_source_bytes(row: &LogRow) -> u64 {
        Self::estimate(&row.service, &row.body, row.structured_metadata.len())
    }

    fn estimate(service: &str, body: &str, structured_metadata_len: usize) -> u64 {
        (service.len() + body.len() + structured_metadata_len
            + 16 /* fingerprint */ + 8 /* timestamp_ns */ + 1/* severity */) as u64
    }
}

impl SpoolEncode for LogSampleRow {
    /// No non-finite-float hazard (no `f64` field) — a plain `serde_json`
    /// value is exact.
    fn to_spool_value(&self) -> serde_json::Value {
        serde_json::to_value(self)
            .expect("LogSampleRow has no non-finite float fields: JSON encoding cannot fail")
    }
}

/// One `log_streams` row (docs/schemas.md §3.1). `month` is stored as the
/// bare `u16` days-since-epoch ClickHouse's `Date` column uses on the
/// wire (task-manager resolution, issue #9:
/// `pulsus_model::Date::days_since_epoch`), not the `Date` newtype itself
/// — the writer is the sole RowBinary insertion boundary, so that
/// conversion happens exactly once, here.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct LogStreamRow {
    pub month: u16,
    pub fingerprint: Fingerprint,
    pub service: String,
    pub labels: String,
    pub updated_ns: i64,
}

impl From<&StreamRow> for LogStreamRow {
    fn from(row: &StreamRow) -> Self {
        LogStreamRow {
            month: row.month.days_since_epoch(),
            fingerprint: row.fingerprint,
            service: row.service.clone(),
            labels: row.labels.to_canonical_json(),
            updated_ns: row.updated_ns,
        }
    }
}

impl LogStreamRow {
    /// See [`LogSampleRow::est_bytes`]'s doc comment for the estimate's
    /// intent and limits.
    pub fn est_bytes(&self) -> u64 {
        Self::estimate(&self.service, self.labels.len())
    }

    /// Estimates a `StreamRow`'s footprint *before* it is materialized
    /// into a `LogStreamRow` (reserve-before-materialize, architect plan
    /// amendment 3, finding 2): approximates the eventual canonical-JSON
    /// length ([`estimate_canonical_json_len`]) from the label set's raw
    /// key/value bytes plus per-entry JSON punctuation, *without*
    /// building the string — the real canonicalization (the cost this
    /// hardening keeps off the rejected/over-limit path) only happens
    /// once the reservation has succeeded, in `From<&StreamRow>` above.
    pub fn est_source_bytes(row: &StreamRow) -> u64 {
        Self::estimate(&row.service, estimate_canonical_json_len(&row.labels))
    }

    fn estimate(service: &str, labels_len: usize) -> u64 {
        (service.len() + labels_len + 2 /* month */ + 16 /* fingerprint */ + 8/* updated_ns */)
            as u64
    }
}

impl SpoolEncode for LogStreamRow {
    /// No non-finite-float hazard (no `f64` field) — a plain `serde_json`
    /// value is exact.
    fn to_spool_value(&self) -> serde_json::Value {
        serde_json::to_value(self)
            .expect("LogStreamRow has no non-finite float fields: JSON encoding cannot fail")
    }
}

/// `log_streams` backfill identity (issue #134, unchanged semantics under
/// the #139 generalization): keyed `(fingerprint, month)` — the
/// `ReplacingMergeTree(updated_ns)` dedup key — versioned on `updated_ns`
/// (larger wins, mirroring the merge's winner).
impl BackfillRow for LogStreamRow {
    type Key = StreamKey;

    fn backfill_key(&self) -> StreamKey {
        (self.fingerprint, self.month)
    }

    fn backfill_version(&self) -> i64 {
        self.updated_ns
    }

    fn backfill_bytes(&self) -> u64 {
        self.est_bytes()
    }
}

/// Approximates [`LabelSet::to_canonical_json`]'s output length without
/// building the string: `{"k":"v","k2":"v2"}` — two enclosing braces, a
/// comma between entries, and per entry two quoted strings plus a colon
/// (`"k":"v"` = `k.len() + v.len() + 5`). Ignores JSON escaping, so this
/// is a lower-bound estimate, not an exact length — consistent with
/// [`LogSampleRow::est_bytes`]'s "conservative, not RowBinary-exact"
/// intent.
fn estimate_canonical_json_len(labels: &LabelSet) -> usize {
    let mut len = 2; // the enclosing `{`/`}`
    for (i, (k, v)) in labels.iter().enumerate() {
        if i > 0 {
            len += 1; // the separating `,`
        }
        len += k.len() + v.len() + 5; // `"`,`"`,`:`,`"`,`"` around key/value
    }
    len
}

/// One `log_patterns` row (docs/schemas.md §3.1, M7-C3 issue #171). Field
/// names/order match the DDL column list exactly (`fingerprint`, `bucket_ns`,
/// `pattern`, `count`). Produced by [`crate::patterns::aggregate_patterns`]
/// (batch pre-aggregation), never from a per-line `From` — one row is already
/// a `(fingerprint, bucket_ns, template) -> count` aggregate over the batch.
/// `count` inserts as a plain `UInt64` into the table's
/// `SimpleAggregateFunction(sum, UInt64)` column (the `log_metrics` idiom).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct LogPatternRow {
    pub fingerprint: Fingerprint,
    pub bucket_ns: i64,
    pub pattern: String,
    pub count: u64,
}

impl LogPatternRow {
    /// A conservative in-memory byte estimate (own-fields, not RowBinary): the
    /// `pattern` String's byte length plus the fixed-width columns — the same
    /// "conservative, not wire-exact" intent as [`LogSampleRow::est_bytes`].
    /// Used by the flush-size threshold; the admission reservation charges the
    /// [`crate::patterns::est_template_bound`] upper bound instead (the pattern
    /// String cannot be measured before extraction).
    pub fn est_bytes(&self) -> u64 {
        (self.pattern.len() + 16 /* fingerprint */ + 8 /* bucket_ns */ + 8/* count */) as u64
    }
}

impl SpoolEncode for LogPatternRow {
    /// No non-finite-float hazard (no `f64` field) — a plain `serde_json`
    /// value is exact.
    fn to_spool_value(&self) -> serde_json::Value {
        serde_json::to_value(self)
            .expect("LogPatternRow has no non-finite float fields: JSON encoding cannot fail")
    }
}

/// One `metric_samples` row (docs/schemas.md §2.1). `value` is a raw `f64`
/// carried verbatim from [`MetricPoint`] — never routed through plain
/// `serde_json` (which would destroy a stale-NaN payload's exact bit
/// pattern, e.g. `0x7FF0000000000002` — `Number::from_f64(NaN)` collapses
/// to `null`). This governs both the ClickHouse RowBinary wire encoding
/// (`value` flows through `Serialize`/`Row` untouched, never JSON) and the
/// spool audit-file encoding — see this type's `SpoolEncode` impl below,
/// and `writer::spool`'s module doc, for how the latter preserves the
/// exact bits despite writing JSON. No `PartialEq` derive here (unlike
/// `LogSampleRow`): `f64`'s `PartialEq` makes `NaN != NaN`, so a derived
/// equality would silently mislead a test into asserting the wrong thing
/// about a stale-NaN sample — compare `.value.to_bits()` explicitly
/// instead.
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MetricSampleRow {
    pub metric_name: String,
    pub fingerprint: Fingerprint,
    pub unix_milli: i64,
    pub value: f64,
}

impl From<&MetricPoint> for MetricSampleRow {
    fn from(row: &MetricPoint) -> Self {
        MetricSampleRow {
            metric_name: row.metric_name.to_string(),
            fingerprint: row.fingerprint,
            unix_milli: row.unix_milli,
            value: row.value,
        }
    }
}

impl MetricSampleRow {
    /// See [`LogSampleRow::est_bytes`]'s doc comment for the estimate's
    /// intent and limits.
    pub fn est_bytes(&self) -> u64 {
        Self::estimate(&self.metric_name)
    }

    /// Estimates a `MetricPoint`'s footprint *before* it is materialized
    /// into a `MetricSampleRow` (reserve-before-materialize, architect plan
    /// amendment 3) — identical accounting to [`Self::est_bytes`], read
    /// straight off the source point.
    pub fn est_source_bytes(row: &MetricPoint) -> u64 {
        Self::estimate(&row.metric_name)
    }

    fn estimate(metric_name: &str) -> u64 {
        (metric_name.len() + 16 /* fingerprint */ + 8 /* unix_milli */ + 8/* value */) as u64
    }
}

impl SpoolEncode for MetricSampleRow {
    /// Issue #26 code-review fix: plain `serde_json::to_value(self)` would
    /// silently collapse a non-finite `value` (the stale-NaN marker
    /// `0x7FF0000000000002`, or +-Infinity) to JSON `null`
    /// (`Number::from_f64` returns `None` for non-finite floats), losing
    /// the exact bit pattern a human/tool auditing a poisoned or
    /// insert-uncertain spool file needs. Always emits `value_bits` (the
    /// raw `f64::to_bits()`, an exact `u64`) alongside the best-effort
    /// human-readable `value` (a JSON number when finite, `null`
    /// otherwise) — see `writer::spool`'s module doc for the schema note a
    /// human audits against.
    ///
    /// **`value_bits` is a JSON STRING, not a bare number** (issue #26
    /// second review-cycle fix): `0x7FF0000000000002` is ~9.2e18, which
    /// exceeds `2^53` — every consumer that parses JSON numbers as
    /// IEEE-754 doubles (JavaScript's `JSON.parse`, `jq` arithmetic by
    /// default, ...) would silently round a bare-integer `value_bits` to
    /// the nearest representable double, defeating the entire point of
    /// this field. A decimal string (`u64::to_string()`) round-trips
    /// exactly through any JSON parser, string or numeric.
    fn to_spool_value(&self) -> serde_json::Value {
        serde_json::json!({
            "metric_name": self.metric_name,
            "fingerprint": self.fingerprint,
            "unix_milli": self.unix_milli,
            "value": if self.value.is_finite() {
                serde_json::json!(self.value)
            } else {
                serde_json::Value::Null
            },
            "value_bits": self.value.to_bits().to_string(),
        })
    }
}

/// One `metric_series` row (docs/schemas.md §2.1): `unix_milli` here is the
/// **activity-bucket floor** (`pulsus_model::floor_to_activity_bucket`), not
/// a raw sample timestamp — the caller ([`crate::writer::MetricWriter`])
/// supplies it explicitly rather than through a plain `From<&SeriesRef>`
/// conversion, because [`SeriesRef`] carries no timestamp of its own (the
/// bucket is derived per-sample, from `ParsedMetrics::samples`, not from the
/// series identity — docs/schemas.md §2.1's cross-bucket-in-one-request
/// rule).
///
/// `value_type` (M7-A4, issue #120) is the LAST field, matching the additive
/// `ALTER TABLE ... ADD COLUMN value_type UInt8 DEFAULT 0` (catalog id 25/26)
/// column order — `0` = float, `1` = histogram. The writer registers one row
/// per `(metric_name, fingerprint, bucket, value_type)`, so a series that
/// carries both a float and a histogram sample in one bucket registers two
/// rows (the per-series float/histogram discriminator A5 rolls up).
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MetricSeriesRow {
    pub metric_name: String,
    pub fingerprint: Fingerprint,
    pub unix_milli: i64,
    pub labels: String,
    pub value_type: u8,
}

impl MetricSeriesRow {
    /// Builds a row for `series`, bucket-floored to `bucket_unix_milli`
    /// (already computed by the caller via
    /// `pulsus_model::floor_to_activity_bucket`), stamped with `value_type`
    /// (`0` = float, `1` = histogram — issue #120).
    pub fn from_series_at_bucket(
        series: &SeriesRef,
        bucket_unix_milli: i64,
        value_type: u8,
    ) -> Self {
        MetricSeriesRow {
            metric_name: series.metric_name.to_string(),
            fingerprint: series.fingerprint,
            unix_milli: bucket_unix_milli,
            labels: series.labels.to_canonical_json(),
            value_type,
        }
    }

    /// See [`LogSampleRow::est_bytes`]'s doc comment for the estimate's
    /// intent and limits.
    pub fn est_bytes(&self) -> u64 {
        Self::estimate(&self.metric_name, self.labels.len())
    }

    /// Estimates a `SeriesRef`'s footprint *before* it is materialized into
    /// a `MetricSeriesRow` (reserve-before-materialize, architect plan
    /// amendment 3) — the bucket floor never changes the byte estimate, so
    /// unlike [`Self::from_series_at_bucket`] this needs no bucket
    /// argument.
    pub fn est_source_bytes(series: &SeriesRef) -> u64 {
        Self::estimate(
            &series.metric_name,
            estimate_canonical_json_len(&series.labels),
        )
    }

    fn estimate(metric_name: &str, labels_len: usize) -> u64 {
        (metric_name.len() + labels_len
            + 16 /* fingerprint */ + 8 /* unix_milli */ + 1/* value_type */) as u64
    }
}

impl SpoolEncode for MetricSeriesRow {
    /// No non-finite-float hazard (no `f64` field) — a plain `serde_json`
    /// value is exact.
    fn to_spool_value(&self) -> serde_json::Value {
        serde_json::to_value(self)
            .expect("MetricSeriesRow has no non-finite float fields: JSON encoding cannot fail")
    }
}

/// One `metric_hist_samples` row (docs/schemas.md §2.4, catalog id 23,
/// M7-A4 issue #120). Field names/order match the DDL column list EXACTLY
/// (identity triplet first, then the A3 histogram value columns). `schema`
/// is `i8` — the physical `Int8` column width. No `PartialEq` derive
/// (like [`MetricSampleRow`]): `sum`/`zero_threshold`/`custom_values` may be
/// NaN markers, so equality must compare `.to_bits()` explicitly.
///
/// Built from a validated A3 [`NativeHistogram`](pulsus_model::NativeHistogram)
/// via `to_columns()` — the ingest seam
/// (`otlp_metrics::emit_native_exponential_histogram`) already ran
/// `validate()`, so `to_columns` cannot fail here (the only failure is a
/// schema outside `Int8`, unreachable for a validated exponential/NHCB
/// schema).
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MetricHistSampleRow {
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
    /// `counter_reset_hint` column (issue #125, migrations 27/28) — the
    /// Prometheus hint byte from [`HistogramColumns`] (0 = Unknown; the
    /// only value today's OTLP ingest ever produces, see
    /// `otlp_metrics.rs`'s exponential-histogram seam).
    pub counter_reset_hint: u8,
}

impl From<&HistogramPoint> for MetricHistSampleRow {
    fn from(point: &HistogramPoint) -> Self {
        let cols = point
            .histogram
            .to_columns()
            .expect("histogram validated at the ingest seam: to_columns cannot fail");
        MetricHistSampleRow {
            metric_name: point.metric_name.to_string(),
            fingerprint: point.fingerprint,
            unix_milli: point.unix_milli,
            schema: cols.schema,
            zero_threshold: cols.zero_threshold,
            zero_count: cols.zero_count,
            count: cols.count,
            sum: cols.sum,
            pos_span_offsets: cols.pos_span_offsets,
            pos_span_lengths: cols.pos_span_lengths,
            pos_bucket_deltas: cols.pos_bucket_deltas,
            neg_span_offsets: cols.neg_span_offsets,
            neg_span_lengths: cols.neg_span_lengths,
            neg_bucket_deltas: cols.neg_bucket_deltas,
            custom_values: cols.custom_values,
            counter_reset_hint: cols.counter_reset_hint,
        }
    }
}

impl MetricHistSampleRow {
    /// See [`LogSampleRow::est_bytes`]'s doc comment for the estimate's
    /// intent and limits.
    pub fn est_bytes(&self) -> u64 {
        Self::estimate(
            self.metric_name.len(),
            self.pos_span_offsets.len() + self.neg_span_offsets.len(),
            self.pos_bucket_deltas.len() + self.neg_bucket_deltas.len(),
            self.custom_values.len(),
        )
    }

    /// Estimates a [`HistogramPoint`]'s footprint *before* it is
    /// materialized into a `MetricHistSampleRow` (reserve-before-materialize,
    /// the established `est_source_bytes` pattern) — read straight off the
    /// source histogram (span/bucket lengths are preserved by `to_columns`,
    /// so this matches [`Self::est_bytes`] on the materialized row).
    pub fn est_source_bytes(point: &HistogramPoint) -> u64 {
        let h = &point.histogram;
        Self::estimate(
            point.metric_name.len(),
            h.positive_spans.len() + h.negative_spans.len(),
            h.positive_buckets.len() + h.negative_buckets.len(),
            h.custom_values.len(),
        )
    }

    fn estimate(
        metric_name_len: usize,
        span_count: usize,
        bucket_count: usize,
        custom_count: usize,
    ) -> u64 {
        (metric_name_len
            + 16 /* fingerprint */ + 8 /* unix_milli */ + 1 /* schema */
            + 8 /* zero_threshold */ + 8 /* zero_count */ + 8 /* count */ + 8 /* sum */
            + 1 /* counter_reset_hint */
            + span_count * (4 /* offset */ + 4 /* length */)
            + bucket_count * 8 /* delta */
            + custom_count * 8/* custom bound */) as u64
    }
}

impl SpoolEncode for MetricHistSampleRow {
    /// Like [`MetricSampleRow`]'s impl (issue #26): the value columns that
    /// may carry a non-finite `f64` — `sum` (stale/absent-NaN marker) and
    /// `zero_threshold`/`custom_values` — are emitted with their exact bit
    /// pattern via a decimal-string `*_bits` field, since plain
    /// `serde_json` collapses a non-finite float to `null`. The best-effort
    /// human-readable value is a JSON number when finite, `null` otherwise.
    /// The integer span/delta arrays are exact as plain JSON.
    fn to_spool_value(&self) -> serde_json::Value {
        serde_json::json!({
            "metric_name": self.metric_name,
            "fingerprint": self.fingerprint,
            "unix_milli": self.unix_milli,
            "schema": self.schema,
            "zero_threshold": finite_or_null(self.zero_threshold),
            "zero_threshold_bits": self.zero_threshold.to_bits().to_string(),
            "zero_count": self.zero_count,
            "count": self.count,
            "sum": finite_or_null(self.sum),
            "sum_bits": self.sum.to_bits().to_string(),
            "pos_span_offsets": self.pos_span_offsets,
            "pos_span_lengths": self.pos_span_lengths,
            "pos_bucket_deltas": self.pos_bucket_deltas,
            "neg_span_offsets": self.neg_span_offsets,
            "neg_span_lengths": self.neg_span_lengths,
            "neg_bucket_deltas": self.neg_bucket_deltas,
            "custom_values": self.custom_values.iter().copied().map(finite_or_null).collect::<Vec<_>>(),
            "custom_values_bits": self.custom_values.iter().map(|v| v.to_bits().to_string()).collect::<Vec<_>>(),
            "counter_reset_hint": self.counter_reset_hint,
        })
    }
}

/// A finite `f64` as a JSON number, or JSON `null` for a non-finite value
/// (NaN/±Inf are not JSON-representable — the exact bits travel in the
/// paired `*_bits` string field). Shared by [`MetricHistSampleRow`]'s spool
/// audit encoding.
fn finite_or_null(v: f64) -> serde_json::Value {
    if v.is_finite() {
        serde_json::json!(v)
    } else {
        serde_json::Value::Null
    }
}

/// One `metric_metadata` row (docs/schemas.md §2.1, issue #26 fix: gained
/// `updated_ns` — the `ReplacingMergeTree(updated_ns)` version column,
/// **last field**, matching the DDL column order).
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MetricMetadataRow {
    pub metric_name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
    pub updated_ns: i64,
}

impl From<&MetricMetadata> for MetricMetadataRow {
    fn from(row: &MetricMetadata) -> Self {
        MetricMetadataRow {
            metric_name: row.metric_name.to_string(),
            metric_type: row.metric_type.clone(),
            help: row.help.clone(),
            unit: row.unit.clone(),
            updated_ns: row.updated_ns,
        }
    }
}

impl MetricMetadataRow {
    /// See [`LogSampleRow::est_bytes`]'s doc comment for the estimate's
    /// intent and limits.
    pub fn est_bytes(&self) -> u64 {
        Self::estimate(&self.metric_name, &self.metric_type, &self.help, &self.unit)
    }

    /// Estimates a `MetricMetadata`'s footprint *before* it is materialized
    /// into a `MetricMetadataRow` (reserve-before-materialize, architect
    /// plan amendment 3).
    pub fn est_source_bytes(row: &MetricMetadata) -> u64 {
        Self::estimate(&row.metric_name, &row.metric_type, &row.help, &row.unit)
    }

    fn estimate(metric_name: &str, metric_type: &str, help: &str, unit: &str) -> u64 {
        (metric_name.len() + metric_type.len() + help.len() + unit.len() + 8/* updated_ns */) as u64
    }
}

impl SpoolEncode for MetricMetadataRow {
    /// No non-finite-float hazard (no `f64` field) — a plain `serde_json`
    /// value is exact.
    fn to_spool_value(&self) -> serde_json::Value {
        serde_json::to_value(self)
            .expect("MetricMetadataRow has no non-finite float fields: JSON encoding cannot fail")
    }
}

/// One `metric_landing` row — one landed metrics event, whatever kind
/// (issue #603). A metrics push becomes exactly one block of these, and the
/// four derived metric tables are maintained from it by materialized view,
/// so the writer inserts into none of them.
///
/// `kind` says which event the row is; a row sets that kind's columns and
/// leaves the rest at the type's default. The fields are the landing
/// table's 26 columns **less `event_id`**, in the table's own declaration
/// order: the insert's column list is exactly this type's `COLUMN_NAMES`,
/// so leaving the column out is what makes the server fill it from
/// `DEFAULT generateUUIDv7()`. A row type carrying the column would store
/// whatever the writer put there — the nil UUID on every row, for an
/// explicit zero.
///
/// No `PartialEq` derive, for [`MetricSampleRow`]'s reason: `value`,
/// `hist_sum`, `hist_zero_threshold` and `hist_custom_values` may be NaN
/// markers, so equality must compare `.to_bits()`.
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MetricLandingRow {
    pub received_ms: i64,
    pub kind: u8,
    pub metric_name: String,
    pub fingerprint: Fingerprint,
    pub unix_milli: i64,
    pub value: f64,
    pub labels: String,
    pub value_type: u8,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
    pub updated_ns: i64,
    pub hist_schema: i8,
    pub hist_zero_threshold: f64,
    pub hist_zero_count: u64,
    pub hist_count: u64,
    pub hist_sum: f64,
    pub hist_pos_span_offsets: Vec<i32>,
    pub hist_pos_span_lengths: Vec<u32>,
    pub hist_pos_bucket_deltas: Vec<i64>,
    pub hist_neg_span_offsets: Vec<i32>,
    pub hist_neg_span_lengths: Vec<u32>,
    pub hist_neg_bucket_deltas: Vec<i64>,
    pub hist_custom_values: Vec<f64>,
    pub hist_counter_reset_hint: u8,
}

/// One landing row's own inline footprint, in the shape the WRITER QUEUE
/// holds it. Taken from `size_of` rather than written down, so it cannot drift
/// from the fields it prices — the same rule [`ARRAY_ELEMENT_SLOT_BYTES`]
/// follows.
///
/// A landing row is the union of the four target rows' columns, so it is far
/// wider than any one of them: three `i64`s, three `f64`s, two `u64`s, a
/// 16-byte fingerprint, four `String` headers, seven `Vec` headers and four
/// small integers. What a target row costs prices only the text and array
/// elements the row owns; the queue also holds all of those slots, every one
/// of them whether or not its kind uses it.
pub const LANDING_ROW_SLOT_BYTES: u64 = std::mem::size_of::<MetricLandingRow>() as u64;

impl MetricLandingRow {
    /// What the ingest queue is charged for one landing row whose kind's
    /// target estimator priced its owned text and arrays at `target_bytes`.
    ///
    /// The queue reservation must count what we HOLD (issue #556's rule):
    /// `PULSUS_INGEST_QUEUE_BYTES` names the buffered bytes, and a landing row
    /// is held as a whole [`MetricLandingRow`] until its block is encoded, so
    /// the row's inline slots are charged beside the buffers it owns.
    pub fn est_landing_bytes(target_bytes: u64) -> u64 {
        target_bytes + LANDING_ROW_SLOT_BYTES
    }

    /// A float sample, whose target is `metric_samples`.
    pub const KIND_FLOAT: u8 = 0;
    /// A native-histogram sample, whose target is `metric_hist_samples`.
    pub const KIND_HIST: u8 = 1;
    /// A series registration, whose target is `metric_series`.
    pub const KIND_SERIES: u8 = 2;
    /// A metadata descriptor, whose target is `metric_metadata`.
    pub const KIND_METADATA: u8 = 3;

    /// A row of `kind` with every kind-specific column at its default.
    fn of_kind(received_ms: i64, kind: u8) -> Self {
        MetricLandingRow {
            received_ms,
            kind,
            metric_name: String::new(),
            fingerprint: Fingerprint::from_raw(0),
            unix_milli: 0,
            value: 0.0,
            labels: String::new(),
            value_type: 0,
            metric_type: String::new(),
            help: String::new(),
            unit: String::new(),
            updated_ns: 0,
            hist_schema: 0,
            hist_zero_threshold: 0.0,
            hist_zero_count: 0,
            hist_count: 0,
            hist_sum: 0.0,
            hist_pos_span_offsets: Vec::new(),
            hist_pos_span_lengths: Vec::new(),
            hist_pos_bucket_deltas: Vec::new(),
            hist_neg_span_offsets: Vec::new(),
            hist_neg_span_lengths: Vec::new(),
            hist_neg_bucket_deltas: Vec::new(),
            hist_custom_values: Vec::new(),
            hist_counter_reset_hint: 0,
        }
    }

    /// A kind-0 row: the columns `metric_samples_mv` reads, and no others.
    pub fn float_sample(received_ms: i64, point: &MetricPoint) -> Self {
        MetricLandingRow {
            metric_name: point.metric_name.to_string(),
            fingerprint: point.fingerprint,
            unix_milli: point.unix_milli,
            value: point.value,
            ..Self::of_kind(received_ms, Self::KIND_FLOAT)
        }
    }

    /// A kind-1 row: the columns `metric_hist_samples_mv` reads, and no
    /// others. As in [`MetricHistSampleRow`]'s conversion, the histogram was
    /// validated at the ingest seam, so `to_columns` cannot fail here.
    pub fn hist_sample(received_ms: i64, point: &HistogramPoint) -> Self {
        let cols = point
            .histogram
            .to_columns()
            .expect("histogram validated at the ingest seam: to_columns cannot fail");
        MetricLandingRow {
            metric_name: point.metric_name.to_string(),
            fingerprint: point.fingerprint,
            unix_milli: point.unix_milli,
            hist_schema: cols.schema,
            hist_zero_threshold: cols.zero_threshold,
            hist_zero_count: cols.zero_count,
            hist_count: cols.count,
            hist_sum: cols.sum,
            hist_pos_span_offsets: cols.pos_span_offsets,
            hist_pos_span_lengths: cols.pos_span_lengths,
            hist_pos_bucket_deltas: cols.pos_bucket_deltas,
            hist_neg_span_offsets: cols.neg_span_offsets,
            hist_neg_span_lengths: cols.neg_span_lengths,
            hist_neg_bucket_deltas: cols.neg_bucket_deltas,
            hist_custom_values: cols.custom_values,
            hist_counter_reset_hint: cols.counter_reset_hint,
            ..Self::of_kind(received_ms, Self::KIND_HIST)
        }
    }

    /// A kind-2 row: the columns `metric_series_mv` reads, and no others.
    /// `unix_milli` is the activity-bucket floor the caller already
    /// computed, never a sample time — the same contract
    /// [`MetricSeriesRow::from_series_at_bucket`] carries.
    pub fn series(
        received_ms: i64,
        series: &SeriesRef,
        bucket_unix_milli: i64,
        value_type: u8,
    ) -> Self {
        MetricLandingRow {
            metric_name: series.metric_name.to_string(),
            fingerprint: series.fingerprint,
            unix_milli: bucket_unix_milli,
            labels: series.labels.to_canonical_json(),
            value_type,
            ..Self::of_kind(received_ms, Self::KIND_SERIES)
        }
    }

    /// A kind-3 row: the columns `metric_metadata_mv` reads, and no others.
    pub fn metadata(received_ms: i64, meta: &MetricMetadata) -> Self {
        MetricLandingRow {
            metric_name: meta.metric_name.to_string(),
            metric_type: meta.metric_type.clone(),
            help: meta.help.clone(),
            unit: meta.unit.clone(),
            updated_ns: meta.updated_ns,
            ..Self::of_kind(received_ms, Self::KIND_METADATA)
        }
    }
}

impl SpoolEncode for MetricLandingRow {
    /// `kind` and `received_ms`, then exactly the keys that kind's target
    /// row already emits — the two shipped float-bit encodings included, so
    /// a non-finite value keeps its exact bit pattern
    /// ([`MetricSampleRow`]'s impl carries the reason). One encoder
    /// emitting every column of the union would put another kind's fields
    /// in a row that does not carry them.
    fn to_spool_value(&self) -> serde_json::Value {
        match self.kind {
            Self::KIND_FLOAT => serde_json::json!({
                "kind": self.kind,
                "received_ms": self.received_ms,
                "metric_name": self.metric_name,
                "fingerprint": self.fingerprint,
                "unix_milli": self.unix_milli,
                "value": finite_or_null(self.value),
                "value_bits": self.value.to_bits().to_string(),
            }),
            Self::KIND_HIST => serde_json::json!({
                "kind": self.kind,
                "received_ms": self.received_ms,
                "metric_name": self.metric_name,
                "fingerprint": self.fingerprint,
                "unix_milli": self.unix_milli,
                "schema": self.hist_schema,
                "zero_threshold": finite_or_null(self.hist_zero_threshold),
                "zero_threshold_bits": self.hist_zero_threshold.to_bits().to_string(),
                "zero_count": self.hist_zero_count,
                "count": self.hist_count,
                "sum": finite_or_null(self.hist_sum),
                "sum_bits": self.hist_sum.to_bits().to_string(),
                "pos_span_offsets": self.hist_pos_span_offsets,
                "pos_span_lengths": self.hist_pos_span_lengths,
                "pos_bucket_deltas": self.hist_pos_bucket_deltas,
                "neg_span_offsets": self.hist_neg_span_offsets,
                "neg_span_lengths": self.hist_neg_span_lengths,
                "neg_bucket_deltas": self.hist_neg_bucket_deltas,
                "custom_values": self.hist_custom_values.iter().copied().map(finite_or_null)
                    .collect::<Vec<_>>(),
                "custom_values_bits": self.hist_custom_values.iter()
                    .map(|v| v.to_bits().to_string()).collect::<Vec<_>>(),
                "counter_reset_hint": self.hist_counter_reset_hint,
            }),
            Self::KIND_SERIES => serde_json::json!({
                "kind": self.kind,
                "received_ms": self.received_ms,
                "metric_name": self.metric_name,
                "fingerprint": self.fingerprint,
                "unix_milli": self.unix_milli,
                "labels": self.labels,
                "value_type": self.value_type,
            }),
            // Every row is one of the four kinds — nothing else constructs
            // one — so this arm is the descriptor's. Matching it as the
            // fallback rather than panicking keeps the audit record
            // readable on the one path that reaches this encoder at all,
            // which is a failure path.
            _ => serde_json::json!({
                "kind": self.kind,
                "received_ms": self.received_ms,
                "metric_name": self.metric_name,
                "metric_type": self.metric_type,
                "help": self.help,
                "unit": self.unit,
                "updated_ns": self.updated_ns,
            }),
        }
    }
}

/// One span-attribute array element's slot cost in the shape the WRITER
/// QUEUE holds, which is [`TraceSpanRow`]'s: four `String` slots (key,
/// scope, val, and the `val_type` spelling) plus the `Option<f64>` (issue
/// #556). Taken from `size_of` rather than written down, so it cannot
/// drift from the fields it prices.
///
/// The queue reservation must count what we HOLD, not what crossed a
/// wire: an array element is not protobuf bytes.
pub(crate) const ARRAY_ELEMENT_SLOT_BYTES: usize =
    4 * std::mem::size_of::<String>() + std::mem::size_of::<Option<f64>>();
/// The five `Vec` headers one span's attribute arrays carry (issue #556).
pub(crate) const ARRAY_HEADER_BYTES: usize = 5 * std::mem::size_of::<Vec<String>>();
/// The `val_type` spelling charged as a fixed term — the longest of the
/// four is `string`, 6 bytes. The SAME constant [`TraceAttrRow::estimate`]
/// already uses, for the same reason: a fixed term keeps `est_bytes` and
/// `est_source_bytes` equal, which is the reserve-before-materialize
/// invariant.
pub(crate) const VAL_TYPE_SPELLING_BYTES: usize = 6;

/// The byte cost of one span's five attribute arrays: the rendered text of
/// the three string arrays, plus per element the slot cost and the
/// `val_type` spelling, plus the five `Vec` headers once.
fn attr_array_bytes(keys: &[String], scopes: &[String], vals: &[String]) -> usize {
    let text: usize = keys.iter().map(String::len).sum::<usize>()
        + scopes.iter().map(String::len).sum::<usize>()
        + vals.iter().map(String::len).sum::<usize>();
    text + keys.len() * (ARRAY_ELEMENT_SLOT_BYTES + VAL_TYPE_SPELLING_BYTES) + ARRAY_HEADER_BYTES
}

/// One `trace_spans` row (docs/schemas.md §4.1, issue #54). `[u8; N]` ↔
/// `FixedString(N)` (serde arrays serialize as N raw bytes on the RowBinary
/// wire — no length prefix); `payload` is a **binary** protobuf blob stored
/// in a `String` column, routed through `serde_bytes` so it serializes as a
/// length-prefixed byte string (`serialize_bytes`) rather than serde's
/// default `Vec<u8>`-as-sequence encoding, which would target
/// `Array(UInt8)` and fail the insert. Field names/order match the DDL
/// column list.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct TraceSpanRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_id: [u8; 8],
    pub name: String,
    pub service: String,
    pub timestamp_ns: i64,
    pub duration_ns: i64,
    pub status_code: i8,
    /// OTLP `Status.message` (issue #184), `""` when absent. Inserted by
    /// column name (`Row` derive), so the additive migration-35
    /// `status_message String DEFAULT ''` column absorbs it regardless of
    /// physical column order.
    pub status_message: String,
    pub kind: i8,
    /// Always `1` (= OTLP protobuf, docs/schemas.md §4.1's `payload_type`
    /// legend) for rows produced by the OTLP receiver; `2` (Zipkin JSON) is
    /// a compat-receiver value (M6+).
    pub payload_type: i8,
    /// `1` iff this span is a Zipkin shared span (issue #173) — the
    /// `zipkin.shared = "true"` signal promoted from the OTLP attribute
    /// (`SpanRecord::shared`) so the service-graph edge MV can pair a shared
    /// server half correctly. Inserted by column name (`Row` derive), so the
    /// additive migration-31 `shared UInt8 DEFAULT 0` column absorbs it
    /// regardless of physical column order.
    pub shared: u8,
    /// OTLP `InstrumentationScope.name`/`version` (issue #192), `""` when
    /// absent. Inserted by column name (`Row` derive), so the additive
    /// migration-37 `scope_name`/`scope_version LowCardinality(String)
    /// DEFAULT ''` columns absorb them regardless of physical column order.
    pub scope_name: String,
    pub scope_version: String,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    /// The span's own attributes as five ALIGNED arrays (issue #556),
    /// element `i` of each describing one attribute. Appended LAST on
    /// purpose — the `val_type` precedent one type down: the derived `Row`
    /// insert names its columns in FIELD order, and migrations 49-58
    /// append these five at the end of `trace_spans`.
    ///
    /// `attr_type` is the rendered spelling (`AttrValueType::as_str`),
    /// because the column is `Array(LowCardinality(String))`; the record
    /// side keeps the closed enum.
    pub attr_key: Vec<String>,
    pub attr_scope: Vec<String>,
    pub attr_val: Vec<String>,
    pub attr_type: Vec<String>,
    pub attr_num: Vec<Option<f64>>,
}

impl From<&SpanRecord> for TraceSpanRow {
    fn from(record: &SpanRecord) -> Self {
        TraceSpanRow {
            trace_id: record.trace_id,
            span_id: record.span_id,
            parent_id: record.parent_id,
            name: record.name.clone(),
            service: record.service.clone(),
            timestamp_ns: record.timestamp_ns,
            duration_ns: record.duration_ns,
            status_code: record.status_code,
            status_message: record.status_message.clone(),
            kind: record.kind,
            payload_type: 1,
            shared: record.shared,
            scope_name: record.scope_name.clone(),
            scope_version: record.scope_version.clone(),
            payload: record.payload.clone(),
            attr_key: record.attr_key.clone(),
            attr_scope: record.attr_scope.clone(),
            attr_val: record.attr_val.clone(),
            attr_type: record
                .attr_type
                .iter()
                .map(|t| t.as_str().to_string())
                .collect(),
            attr_num: record.attr_num.clone(),
        }
    }
}

impl TraceSpanRow {
    /// See [`LogSampleRow::est_bytes`]'s doc comment for the estimate's
    /// intent and limits.
    pub fn est_bytes(&self) -> u64 {
        Self::estimate(
            &self.name,
            &self.service,
            self.status_message.len(),
            self.scope_name.len() + self.scope_version.len(),
            self.payload.len(),
            attr_array_bytes(&self.attr_key, &self.attr_scope, &self.attr_val),
        )
    }

    /// Estimates a `SpanRecord`'s footprint *before* it is materialized
    /// into a `TraceSpanRow` (reserve-before-materialize, the established
    /// `est_source_bytes` pattern) — identical accounting to
    /// [`Self::est_bytes`], read straight off the source record.
    pub fn est_source_bytes(record: &SpanRecord) -> u64 {
        Self::estimate(
            &record.name,
            &record.service,
            record.status_message.len(),
            record.scope_name.len() + record.scope_version.len(),
            record.payload.len(),
            attr_array_bytes(&record.attr_key, &record.attr_scope, &record.attr_val),
        )
    }

    fn estimate(
        name: &str,
        service: &str,
        status_message_len: usize,
        scope_len: usize,
        payload_len: usize,
        attr_array_len: usize,
    ) -> u64 {
        (name.len() + service.len() + status_message_len + scope_len + payload_len
            + attr_array_len /* the five aligned attribute arrays, issue #556 */
            + 16 /* trace_id */ + 8 /* span_id */ + 8 /* parent_id */
            + 8 /* timestamp_ns */ + 8 /* duration_ns */
            + 1 /* status_code */ + 1 /* kind */ + 1 /* payload_type */
            + 1/* shared */) as u64
    }
}

impl SpoolEncode for TraceSpanRow {
    /// The spool is a human audit artifact, never an insert replay source
    /// (`pulsus-clickhouse`'s "never auto-retried" contract): IDs render as
    /// lowercase hex, and the binary `payload` renders as its byte length
    /// only (`payload_len`) — a multi-KiB protobuf blob as a JSON int array
    /// would bloat the file without helping a human audit it.
    fn to_spool_value(&self) -> serde_json::Value {
        serde_json::json!({
            "trace_id": hex_lower(&self.trace_id),
            "span_id": hex_lower(&self.span_id),
            "parent_id": hex_lower(&self.parent_id),
            "name": self.name,
            "service": self.service,
            "timestamp_ns": self.timestamp_ns,
            "duration_ns": self.duration_ns,
            "status_code": self.status_code,
            "status_message": self.status_message,
            "kind": self.kind,
            "payload_type": self.payload_type,
            "shared": self.shared,
            "scope_name": self.scope_name,
            "scope_version": self.scope_version,
            "payload_len": self.payload.len(),
            "attr_lens": [self.attr_key.len(), self.attr_scope.len(), self.attr_val.len(),
                          self.attr_type.len(), self.attr_num.len()],
        })
    }
}

/// One `trace_attrs_idx` row (docs/schemas.md §4.1, issue #54 as amended:
/// the `scope` discriminator sits between `val` and `val_num`, matching the
/// DDL column order). `date` is the span's UTC **day** since the epoch
/// (`Date::start_of_day_utc` — daily partitions), carried as the bare `u16`
/// ClickHouse `Date` wire value, same convention as `LogStreamRow::month`.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct TraceAttrRow {
    pub date: u16,
    pub key: String,
    pub val: String,
    pub scope: String,
    pub val_num: Option<f64>,
    pub timestamp_ns: i64,
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub duration_ns: i64,
    /// The OTLP kind `val` was rendered from (issue #476), one of
    /// `string`/`int`/`float`/`bool`. LAST field on purpose: the derived
    /// `Row` insert names its columns positionally from this struct, and
    /// migration 39 appends the column at the end of `trace_attrs_idx`.
    pub val_type: String,
}

impl From<&AttrRecord> for TraceAttrRow {
    fn from(record: &AttrRecord) -> Self {
        TraceAttrRow {
            date: record.date,
            key: record.key.clone(),
            val: record.val.clone(),
            scope: record.scope.clone(),
            val_num: record.val_num,
            timestamp_ns: record.timestamp_ns,
            trace_id: record.trace_id,
            span_id: record.span_id,
            duration_ns: record.duration_ns,
            val_type: record.val_type.as_str().to_string(),
        }
    }
}

impl TraceAttrRow {
    /// See [`LogSampleRow::est_bytes`]'s doc comment for the estimate's
    /// intent and limits.
    pub fn est_bytes(&self) -> u64 {
        Self::estimate(&self.key, &self.val, &self.scope)
    }

    /// Estimates an `AttrRecord`'s footprint *before* it is materialized
    /// into a `TraceAttrRow` (reserve-before-materialize).
    pub fn est_source_bytes(record: &AttrRecord) -> u64 {
        Self::estimate(&record.key, &record.val, &record.scope)
    }

    fn estimate(key: &str, val: &str, scope: &str) -> u64 {
        (key.len() + val.len() + scope.len()
            + 2 /* date */ + 9 /* val_num (tag + f64) */ + 8 /* timestamp_ns */
            + 16 /* trace_id */ + 8 /* span_id */ + 8/* duration_ns */
            + 6/* val_type: a constant, the longest of the four spellings
        is `string`; a fixed term keeps `est_bytes` and
        `est_source_bytes` equal, which is the
        reserve-before-materialize invariant */) as u64
    }
}

impl SpoolEncode for TraceAttrRow {
    /// IDs as lowercase hex (same audit rationale as [`TraceSpanRow`]'s
    /// impl); `val_num` is finite-or-`None` by construction
    /// (`otlp_traces::parse` filters non-finite parses), so a plain
    /// `serde_json` number is exact — no `value_bits` hazard.
    fn to_spool_value(&self) -> serde_json::Value {
        serde_json::json!({
            "date": self.date,
            "key": self.key,
            "val": self.val,
            "scope": self.scope,
            "val_num": self.val_num,
            "timestamp_ns": self.timestamp_ns,
            "trace_id": hex_lower(&self.trace_id),
            "span_id": hex_lower(&self.span_id),
            "duration_ns": self.duration_ns,
            "val_type": self.val_type,
        })
    }
}

/// `trace_attrs_idx` backfill identity (issue #139): keyed on the full
/// `ReplacingMergeTree ORDER BY (key, val, scope, timestamp_ns, trace_id,
/// span_id)` tuple. VERSIONLESS (constant `0`): the non-key columns
/// (`date`, `val_num`, `duration_ns`, `val_type`) are deterministic
/// functions of the same attr record/span, so a re-inserted row is the same logical row
/// (`FINAL` collapses to 1) and the equal-version mid-attempt removal is
/// safe — see `MetricSeriesRow`'s impl note. Byte-accounting caveat: the
/// backlog map key clones this row's three strings, so the true footprint
/// is ~2× `est_bytes` for this backlog — the 32 MiB cap still bounds it
/// within a small constant (conservative-estimate ethos; no key-byte
/// accounting by design).
impl BackfillRow for TraceAttrRow {
    type Key = (String, String, String, i64, [u8; 16], [u8; 8]);

    fn backfill_key(&self) -> Self::Key {
        (
            self.key.clone(),
            self.val.clone(),
            self.scope.clone(),
            self.timestamp_ns,
            self.trace_id,
            self.span_id,
        )
    }

    fn backfill_version(&self) -> i64 {
        0
    }

    fn backfill_bytes(&self) -> u64 {
        self.est_bytes()
    }
}

/// Lowercase hex rendering for the trace rows' spool audit encoding.
fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use pulsus_model::{Date, LabelSet, NativeHistogram, STALE_NAN_BITS, Span, UnixNano};

    use super::*;
    use crate::ingest::traces::AttrValueType;

    #[test]
    fn log_sample_row_from_log_row_copies_every_field() {
        let row = LogRow {
            service: "checkout".to_string(),
            fingerprint: Fingerprint::from_raw(42),
            timestamp_ns: UnixNano(1_700_000_000_000_000_000),
            severity: 9,
            body: "hello".to_string(),
            structured_metadata: r#"{"trace_id":"abc"}"#.to_string(),
        };
        let mapped = LogSampleRow::from(&row);
        assert_eq!(mapped.service, "checkout");
        assert_eq!(mapped.fingerprint, Fingerprint::from_raw(42));
        assert_eq!(mapped.timestamp_ns, 1_700_000_000_000_000_000);
        assert_eq!(mapped.severity, 9);
        assert_eq!(mapped.body, "hello");
        assert_eq!(mapped.structured_metadata, r#"{"trace_id":"abc"}"#);
    }

    #[test]
    fn log_sample_row_est_bytes_grows_with_body_length() {
        let short = LogSampleRow {
            service: String::new(),
            fingerprint: Fingerprint::from_raw(0),
            timestamp_ns: 0,
            severity: 0,
            body: "a".to_string(),
            structured_metadata: String::new(),
        };
        let long = LogSampleRow {
            body: "a".repeat(100),
            ..short.clone()
        };
        assert!(long.est_bytes() > short.est_bytes());
        // Structured metadata contributes to the estimate too (issue #97).
        let with_sm = LogSampleRow {
            structured_metadata: r#"{"trace_id":"abc"}"#.to_string(),
            ..short.clone()
        };
        assert!(with_sm.est_bytes() > short.est_bytes());
    }

    #[test]
    fn log_sample_row_est_source_bytes_matches_est_bytes_on_the_materialized_row() {
        let row = LogRow {
            service: "checkout".to_string(),
            fingerprint: Fingerprint::from_raw(42),
            timestamp_ns: UnixNano(1_700_000_000_000_000_000),
            severity: 9,
            body: "hello world".to_string(),
            structured_metadata: r#"{"trace_id":"abc"}"#.to_string(),
        };
        let mapped = LogSampleRow::from(&row);
        assert_eq!(LogSampleRow::est_source_bytes(&row), mapped.est_bytes());
    }

    /// Regression test (issue #26 second review-cycle finding, test gap 2):
    /// pins `LogSampleRow`'s spool audit-file SHAPE (field names/values)
    /// for a known row, proving the `SpoolEncode` generalization did not
    /// change the log writer's existing audit format — `LogSampleRow` has
    /// no non-finite-float hazard, so it must still be a bare
    /// `serde_json::to_value` (no `_bits`-style fields, no restructuring).
    #[test]
    fn log_sample_row_spool_encoding_shape_is_unchanged_plain_json() {
        let row = LogSampleRow {
            service: "checkout".to_string(),
            fingerprint: Fingerprint::from_raw(42),
            timestamp_ns: 1_700_000_000_000_000_000,
            severity: 9,
            body: "hello".to_string(),
            structured_metadata: r#"{"trace_id":"abc"}"#.to_string(),
        };
        let spooled = row.to_spool_value();
        assert_eq!(
            spooled,
            serde_json::json!({
                "service": "checkout",
                "fingerprint": 42,
                "timestamp_ns": 1_700_000_000_000_000_000i64,
                "severity": 9,
                "body": "hello",
                "structured_metadata": r#"{"trace_id":"abc"}"#,
            }),
            "LogSampleRow's spool shape must be exactly its plain field set, unchanged \
             by the SpoolEncode generalization"
        );
    }

    #[test]
    fn log_stream_row_from_stream_row_converts_month_to_days_since_epoch() {
        let (labels, _) =
            LabelSet::from_normalized([("service_name".to_string(), "checkout".to_string())]);
        let row = StreamRow {
            month: Date::start_of_month_utc(1_700_000_000_000_000_000).unwrap(),
            fingerprint: Fingerprint::from_raw(7),
            service: "checkout".to_string(),
            labels,
            updated_ns: 123,
        };
        let mapped = LogStreamRow::from(&row);
        assert_eq!(mapped.month, row.month.days_since_epoch());
        assert_eq!(mapped.fingerprint, Fingerprint::from_raw(7));
        assert_eq!(mapped.service, "checkout");
        assert_eq!(mapped.labels, r#"{"service_name":"checkout"}"#);
        assert_eq!(mapped.updated_ns, 123);
    }

    #[test]
    fn log_stream_row_est_source_bytes_matches_est_bytes_on_the_materialized_row() {
        let (labels, _) = LabelSet::from_normalized([
            ("service_name".to_string(), "checkout".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]);
        let row = StreamRow {
            month: Date::start_of_month_utc(1_700_000_000_000_000_000).unwrap(),
            fingerprint: Fingerprint::from_raw(7),
            service: "checkout".to_string(),
            labels,
            updated_ns: 123,
        };
        let mapped = LogStreamRow::from(&row);
        assert_eq!(LogStreamRow::est_source_bytes(&row), mapped.est_bytes());
    }

    /// Regression test (issue #26 second review-cycle finding, test gap 2):
    /// pins `LogStreamRow`'s spool audit-file SHAPE (field names/values)
    /// for a known row — same rationale as
    /// `log_sample_row_spool_encoding_shape_is_unchanged_plain_json`.
    #[test]
    fn log_stream_row_spool_encoding_shape_is_unchanged_plain_json() {
        let row = LogStreamRow {
            month: 19_800,
            fingerprint: Fingerprint::from_raw(7),
            service: "checkout".to_string(),
            labels: r#"{"service_name":"checkout"}"#.to_string(),
            updated_ns: 123,
        };
        let spooled = row.to_spool_value();
        assert_eq!(
            spooled,
            serde_json::json!({
                "month": 19_800,
                "fingerprint": 7,
                "service": "checkout",
                "labels": "{\"service_name\":\"checkout\"}",
                "updated_ns": 123,
            }),
            "LogStreamRow's spool shape must be exactly its plain field set, unchanged \
             by the SpoolEncode generalization"
        );
    }

    #[test]
    fn metric_sample_row_from_metric_point_copies_every_field() {
        let point = MetricPoint {
            metric_name: Arc::from("http_requests_total"),
            fingerprint: Fingerprint::from_raw(42),
            unix_milli: 1_700_000_000_000,
            value: 1.5,
        };
        let mapped = MetricSampleRow::from(&point);
        assert_eq!(mapped.metric_name, "http_requests_total");
        assert_eq!(mapped.fingerprint, Fingerprint::from_raw(42));
        assert_eq!(mapped.unix_milli, 1_700_000_000_000);
        assert_eq!(mapped.value, 1.5);
    }

    /// Load-bearing (architect plan, edge case 2): a stale-NaN payload's
    /// exact bit pattern must survive `MetricPoint -> MetricSampleRow`
    /// untouched — asserted via `.to_bits()`, never `PartialEq`/`is_nan()`
    /// (NaN != NaN, and a generic "is it NaN" check would not catch a bit
    /// pattern silently corrupted to a *different* NaN payload).
    #[test]
    fn metric_sample_row_preserves_the_stale_nan_bit_pattern_exactly() {
        let point = MetricPoint {
            metric_name: Arc::from("up"),
            fingerprint: Fingerprint::from_raw(1),
            unix_milli: 0,
            value: f64::from_bits(STALE_NAN_BITS),
        };
        let mapped = MetricSampleRow::from(&point);
        assert_eq!(mapped.value.to_bits(), STALE_NAN_BITS);
    }

    /// Issue #26 code-review fix: `SpoolEncode::to_spool_value` must carry
    /// a non-finite `value`'s exact bit pattern via `value_bits`, not
    /// silently collapse it to JSON `null` the way a bare
    /// `serde_json::to_value(&row)` would. `value_bits` must be a JSON
    /// STRING (second review-cycle fix): `0x7FF0000000000002` exceeds
    /// `2^53`, so a bare JSON number would silently round under any
    /// double-based JSON parser (`.as_u64()` on `serde_json::Value` decodes
    /// through `u64`, not `f64`, so this test asserts the *string* shape
    /// explicitly rather than relying on `serde_json`'s own lossless-`u64`
    /// convenience masking the hazard).
    #[test]
    fn metric_sample_row_spool_encoding_preserves_a_stale_nan_via_value_bits_as_a_string() {
        let row = MetricSampleRow {
            metric_name: "up".to_string(),
            fingerprint: Fingerprint::from_raw(1),
            unix_milli: 0,
            value: f64::from_bits(STALE_NAN_BITS),
        };
        let spooled = row.to_spool_value();
        assert_eq!(
            spooled["value_bits"],
            serde_json::Value::String(STALE_NAN_BITS.to_string())
        );
        assert!(
            spooled["value"].is_null(),
            "a non-finite float is not JSON-representable; 'value' is null by design, \
             value_bits is the source of truth"
        );
    }

    /// The finite-value happy path: `value` stays a plain, human-readable
    /// JSON number (not routed through `value_bits`-only encoding), and
    /// `value_bits` — still a string — round-trips exactly.
    #[test]
    fn metric_sample_row_spool_encoding_keeps_a_finite_value_human_readable() {
        let row = MetricSampleRow {
            metric_name: "up".to_string(),
            fingerprint: Fingerprint::from_raw(1),
            unix_milli: 0,
            value: 1.5,
        };
        let spooled = row.to_spool_value();
        assert_eq!(spooled["value"].as_f64(), Some(1.5));
        assert_eq!(
            spooled["value_bits"],
            serde_json::Value::String(1.5f64.to_bits().to_string())
        );
    }

    #[test]
    fn metric_sample_row_est_source_bytes_matches_est_bytes_on_the_materialized_row() {
        let point = MetricPoint {
            metric_name: Arc::from("http_requests_total"),
            fingerprint: Fingerprint::from_raw(42),
            unix_milli: 1_700_000_000_000,
            value: 1.5,
        };
        let mapped = MetricSampleRow::from(&point);
        assert_eq!(
            MetricSampleRow::est_source_bytes(&point),
            mapped.est_bytes()
        );
    }

    #[test]
    fn metric_series_row_from_series_at_bucket_floors_unix_milli_to_the_supplied_bucket() {
        let (labels, _) = LabelSet::from_normalized([("job".to_string(), "checkout".to_string())]);
        let series = SeriesRef {
            metric_name: Arc::from("http_requests_total"),
            fingerprint: Fingerprint::from_raw(7),
            labels,
        };
        let mapped = MetricSeriesRow::from_series_at_bucket(&series, 3_600_000, 0);
        assert_eq!(mapped.metric_name, "http_requests_total");
        assert_eq!(mapped.fingerprint, Fingerprint::from_raw(7));
        assert_eq!(mapped.unix_milli, 3_600_000);
        assert_eq!(mapped.labels, r#"{"job":"checkout"}"#);
        assert_eq!(mapped.value_type, 0);
        // Issue #120: a histogram series stamps value_type = 1.
        let hist = MetricSeriesRow::from_series_at_bucket(&series, 3_600_000, 1);
        assert_eq!(hist.value_type, 1);
    }

    #[test]
    fn metric_series_row_est_source_bytes_matches_est_bytes_on_the_materialized_row() {
        let (labels, _) = LabelSet::from_normalized([
            ("job".to_string(), "checkout".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]);
        let series = SeriesRef {
            metric_name: Arc::from("http_requests_total"),
            fingerprint: Fingerprint::from_raw(7),
            labels,
        };
        let mapped = MetricSeriesRow::from_series_at_bucket(&series, 3_600_000, 1);
        assert_eq!(
            MetricSeriesRow::est_source_bytes(&series),
            mapped.est_bytes()
        );
    }

    #[test]
    fn metric_metadata_row_from_metric_metadata_copies_every_field_including_updated_ns() {
        let meta = MetricMetadata {
            metric_name: Arc::from("http_requests_total"),
            metric_type: "counter".to_string(),
            help: "total requests".to_string(),
            unit: "".to_string(),
            updated_ns: 123,
        };
        let mapped = MetricMetadataRow::from(&meta);
        assert_eq!(mapped.metric_name, "http_requests_total");
        assert_eq!(mapped.metric_type, "counter");
        assert_eq!(mapped.help, "total requests");
        assert_eq!(mapped.unit, "");
        assert_eq!(mapped.updated_ns, 123);
    }

    #[test]
    fn metric_metadata_row_est_source_bytes_matches_est_bytes_on_the_materialized_row() {
        let meta = MetricMetadata {
            metric_name: Arc::from("http_requests_total"),
            metric_type: "counter".to_string(),
            help: "total requests".to_string(),
            unit: "".to_string(),
            updated_ns: 123,
        };
        let mapped = MetricMetadataRow::from(&meta);
        assert_eq!(
            MetricMetadataRow::est_source_bytes(&meta),
            mapped.est_bytes()
        );
    }

    // -- MetricHistSampleRow (issue #120) --

    /// A single-histogram fixture with an internal zero bucket: absolute
    /// counts [5, 0, 3] delta-encode to [5, -5, 3].
    fn hist_point_with_internal_zero(sum: f64) -> HistogramPoint {
        HistogramPoint {
            metric_name: Arc::from("http_request_duration_seconds"),
            fingerprint: Fingerprint::from_raw(99),
            unix_milli: 1_700_000_000_000,
            histogram: NativeHistogram {
                counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
                schema: 2,
                zero_threshold: 1e-9,
                zero_count: 0,
                count: 8,
                sum,
                positive_spans: vec![Span {
                    offset: 1,
                    length: 3,
                }],
                negative_spans: vec![],
                positive_buckets: vec![5, -5, 3],
                negative_buckets: vec![],
                custom_values: vec![],
            },
        }
    }

    #[test]
    fn metric_hist_sample_row_from_point_copies_every_column_in_ddl_order() {
        let point = hist_point_with_internal_zero(4.5);
        let row = MetricHistSampleRow::from(&point);
        assert_eq!(row.metric_name, "http_request_duration_seconds");
        assert_eq!(row.fingerprint, Fingerprint::from_raw(99));
        assert_eq!(row.unix_milli, 1_700_000_000_000);
        assert_eq!(row.schema, 2);
        assert_eq!(row.zero_threshold.to_bits(), 1e-9f64.to_bits());
        assert_eq!(row.zero_count, 0);
        assert_eq!(row.count, 8);
        assert_eq!(row.sum.to_bits(), 4.5f64.to_bits());
        assert_eq!(row.pos_span_offsets, vec![1]);
        assert_eq!(row.pos_span_lengths, vec![3]);
        assert_eq!(row.pos_bucket_deltas, vec![5, -5, 3]);
        assert!(row.neg_span_offsets.is_empty());
        assert!(row.neg_bucket_deltas.is_empty());
        assert!(row.custom_values.is_empty());
    }

    /// AC1 (issue #120): an internal-zero bucket round-trips OTLP →
    /// NativeHistogram → to_columns/MetricHistSampleRow → decode, reproducing
    /// the original absolute counts (including the zero).
    #[test]
    fn metric_hist_sample_row_internal_zero_bucket_reconstructs_absolute_counts() {
        let row = MetricHistSampleRow::from(&hist_point_with_internal_zero(4.5));
        // Delta-decode the running absolute counts.
        let mut running = 0i64;
        let abs: Vec<i64> = row
            .pos_bucket_deltas
            .iter()
            .map(|&d| {
                running += d;
                running
            })
            .collect();
        assert_eq!(abs, vec![5, 0, 3], "the internal zero bucket is preserved");
    }

    #[test]
    fn metric_hist_sample_row_est_source_bytes_matches_est_bytes_on_the_materialized_row() {
        let point = hist_point_with_internal_zero(4.5);
        let row = MetricHistSampleRow::from(&point);
        assert_eq!(
            MetricHistSampleRow::est_source_bytes(&point),
            row.est_bytes()
        );
    }

    /// The stale-NaN and absent-NaN `sum` bit patterns survive the
    /// `HistogramPoint → MetricHistSampleRow` conversion exactly and remain
    /// distinct (asserted via `.to_bits()`, never `PartialEq`/`is_nan()`).
    #[test]
    fn metric_hist_sample_row_preserves_stale_and_absent_nan_sum_bits_distinctly() {
        let stale = MetricHistSampleRow::from(&hist_point_with_internal_zero(f64::from_bits(
            STALE_NAN_BITS,
        )));
        assert_eq!(stale.sum.to_bits(), STALE_NAN_BITS);
        let absent = MetricHistSampleRow::from(&hist_point_with_internal_zero(f64::NAN));
        assert_eq!(absent.sum.to_bits(), f64::NAN.to_bits());
        assert_ne!(stale.sum.to_bits(), absent.sum.to_bits());
    }

    #[test]
    fn metric_hist_sample_row_spool_encoding_preserves_stale_nan_sum_via_bits_string() {
        let row = MetricHistSampleRow::from(&hist_point_with_internal_zero(f64::from_bits(
            STALE_NAN_BITS,
        )));
        let spooled = row.to_spool_value();
        assert_eq!(
            spooled["sum_bits"],
            serde_json::Value::String(STALE_NAN_BITS.to_string())
        );
        assert!(
            spooled["sum"].is_null(),
            "a non-finite sum is not JSON-representable; sum_bits is the source of truth"
        );
        assert_eq!(spooled["pos_bucket_deltas"], serde_json::json!([5, -5, 3]));
    }

    fn span_record() -> SpanRecord {
        SpanRecord {
            trace_id: [0xAB; 16],
            span_id: [0x01; 8],
            parent_id: [0; 8],
            name: "op-a".to_string(),
            service: "checkout".to_string(),
            timestamp_ns: 1_700_000_000_000_000_000,
            duration_ns: 1_000_000_000,
            status_code: 2,
            status_message: String::new(),
            kind: 3,
            shared: 1,
            scope_name: "io.otel".to_string(),
            scope_version: "2.1".to_string(),
            payload: vec![0xDE, 0xAD, 0xBE, 0xEF],
            // Hand-built: no attribute arrays (issue #556). Five EMPTY arrays
            // satisfy the `attr_arrays_aligned` CHECK — 0 = 0 = 0 = 0 = 0.
            attr_key: Vec::new(),
            attr_scope: Vec::new(),
            attr_val: Vec::new(),
            attr_type: Vec::new(),
            attr_num: Vec::new(),
        }
    }

    fn attr_record() -> AttrRecord {
        AttrRecord {
            date: 19_675,
            key: "http.status_code".to_string(),
            scope: "span".to_string(),
            val: "500".to_string(),
            val_type: AttrValueType::Int,
            val_num: Some(500.0),
            timestamp_ns: 1_700_000_000_000_000_000,
            trace_id: [0xAB; 16],
            span_id: [0x01; 8],
            duration_ns: 1_000_000_000,
        }
    }

    #[test]
    fn trace_span_row_from_span_record_copies_every_field_and_pins_payload_type() {
        let mapped = TraceSpanRow::from(&span_record());
        assert_eq!(mapped.trace_id, [0xAB; 16]);
        assert_eq!(mapped.span_id, [0x01; 8]);
        assert_eq!(mapped.parent_id, [0; 8]);
        assert_eq!(mapped.name, "op-a");
        assert_eq!(mapped.service, "checkout");
        assert_eq!(mapped.timestamp_ns, 1_700_000_000_000_000_000);
        assert_eq!(mapped.duration_ns, 1_000_000_000);
        assert_eq!(mapped.status_code, 2);
        assert_eq!(mapped.status_message, "", "empty when the record has none");
        assert_eq!(mapped.kind, 3);
        assert_eq!(mapped.payload_type, 1, "OTLP protobuf payload type");
        assert_eq!(mapped.shared, 1, "the Zipkin shared flag is copied through");
        assert_eq!(mapped.scope_name, "io.otel");
        assert_eq!(mapped.scope_version, "2.1");
        assert_eq!(mapped.payload, vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(mapped.attr_key, Vec::<String>::new());
        assert_eq!(mapped.attr_scope, Vec::<String>::new());
        assert_eq!(mapped.attr_val, Vec::<String>::new());
        assert_eq!(mapped.attr_type, Vec::<String>::new());
        assert_eq!(mapped.attr_num, Vec::<Option<f64>>::new());

        // The arrays copy through element by element, and `attr_type`
        // renders the closed enum to its stored spelling.
        let mut record = span_record();
        record.attr_key = vec!["http.status_code".to_string(), "spanID".to_string()];
        record.attr_scope = vec!["span".to_string(), "link:intrinsic".to_string()];
        record.attr_val = vec!["500".to_string(), "0000000000000001".to_string()];
        record.attr_type = vec![AttrValueType::Int, AttrValueType::String];
        record.attr_num = vec![Some(500.0), None];
        let mapped = TraceSpanRow::from(&record);
        assert_eq!(mapped.attr_key, vec!["http.status_code", "spanID"]);
        assert_eq!(mapped.attr_scope, vec!["span", "link:intrinsic"]);
        assert_eq!(mapped.attr_val, vec!["500", "0000000000000001"]);
        assert_eq!(mapped.attr_type, vec!["int", "string"]);
        assert_eq!(mapped.attr_num, vec![Some(500.0), None]);
    }

    /// The three array constants are computed from `size_of`, so nothing
    /// otherwise binds them to the fields they claim to price. This does:
    /// `ARRAY_ELEMENT_SLOT_BYTES` is measured against one element of each
    /// of the ROW's five arrays, and `ARRAY_HEADER_BYTES` against its five
    /// vector headers.
    #[test]
    fn the_array_slot_constant_prices_the_rows_own_element_types() {
        let mut record = span_record();
        record.attr_key = vec!["k".to_string()];
        record.attr_scope = vec!["span".to_string()];
        record.attr_val = vec!["v".to_string()];
        record.attr_type = vec![AttrValueType::Int];
        record.attr_num = vec![Some(1.0)];
        let row = TraceSpanRow::from(&record);
        let element = std::mem::size_of_val(&row.attr_key[0])
            + std::mem::size_of_val(&row.attr_scope[0])
            + std::mem::size_of_val(&row.attr_val[0])
            + std::mem::size_of_val(&row.attr_type[0])
            + std::mem::size_of_val(&row.attr_num[0]);
        assert_eq!(
            ARRAY_ELEMENT_SLOT_BYTES, element,
            "the constant must price this row's element types"
        );
        let headers = std::mem::size_of_val(&row.attr_key)
            + std::mem::size_of_val(&row.attr_scope)
            + std::mem::size_of_val(&row.attr_val)
            + std::mem::size_of_val(&row.attr_type)
            + std::mem::size_of_val(&row.attr_num);
        assert_eq!(
            ARRAY_HEADER_BYTES, headers,
            "the constant must price this row's five vector headers"
        );
    }

    /// One more array element reserves at least its slots, on top of any
    /// text. Asserted as a DIFFERENCE between two spans whose only
    /// difference is one empty-string element, so the text contributes
    /// nothing and only the slot accounting can satisfy it.
    #[test]
    fn one_more_array_element_reserves_at_least_its_slots() {
        let none = span_record();
        let mut one = span_record();
        one.attr_key = vec![String::new()];
        one.attr_scope = vec![String::new()];
        one.attr_val = vec![String::new()];
        one.attr_type = vec![AttrValueType::String];
        one.attr_num = vec![None];
        let delta = TraceSpanRow::est_source_bytes(&one) - TraceSpanRow::est_source_bytes(&none);
        let floor = (ARRAY_ELEMENT_SLOT_BYTES + VAL_TYPE_SPELLING_BYTES) as u64;
        assert!(
            delta >= floor,
            "one array element must reserve at least its slots ({floor}); it reserved {delta}"
        );
    }

    #[test]
    fn trace_span_row_est_source_bytes_matches_est_bytes_on_the_materialized_row() {
        let record = span_record();
        let mapped = TraceSpanRow::from(&record);
        assert_eq!(TraceSpanRow::est_source_bytes(&record), mapped.est_bytes());
    }

    /// Pins `TraceSpanRow`'s spool audit-file SHAPE: IDs as lowercase hex,
    /// the binary payload as its byte length only (`payload_len`) — the
    /// spool is a human audit artifact, never an insert replay source.
    #[test]
    fn trace_span_row_spool_encoding_renders_hex_ids_and_payload_len_only() {
        // Two array elements, so `attr_lens` pins the five LENGTHS rather
        // than five zeros. The arrays render as their sizes, not their
        // content — the `payload_len` spirit: the alignment is what an
        // auditor of a refused insert needs.
        let mut record = span_record();
        record.attr_key = vec!["k".to_string(), "spanID".to_string()];
        record.attr_scope = vec!["span".to_string(), "link:intrinsic".to_string()];
        record.attr_val = vec!["500".to_string(), "0000000000000001".to_string()];
        record.attr_type = vec![AttrValueType::Int, AttrValueType::String];
        record.attr_num = vec![Some(500.0), None];
        let spooled = TraceSpanRow::from(&record).to_spool_value();
        assert_eq!(
            spooled,
            serde_json::json!({
                "trace_id": "abababababababababababababababab",
                "span_id": "0101010101010101",
                "parent_id": "0000000000000000",
                "name": "op-a",
                "service": "checkout",
                "timestamp_ns": 1_700_000_000_000_000_000i64,
                "duration_ns": 1_000_000_000,
                "status_code": 2,
                "status_message": "",
                "kind": 3,
                "payload_type": 1,
                "shared": 1,
                "scope_name": "io.otel",
                "scope_version": "2.1",
                "payload_len": 4,
                "attr_lens": [2, 2, 2, 2, 2],
            })
        );
    }

    #[test]
    fn trace_attr_row_from_attr_record_copies_every_field() {
        let mapped = TraceAttrRow::from(&attr_record());
        assert_eq!(mapped.date, 19_675);
        assert_eq!(mapped.key, "http.status_code");
        assert_eq!(mapped.val, "500");
        assert_eq!(mapped.scope, "span");
        assert_eq!(mapped.val_num, Some(500.0));
        assert_eq!(mapped.timestamp_ns, 1_700_000_000_000_000_000);
        assert_eq!(mapped.trace_id, [0xAB; 16]);
        assert_eq!(mapped.span_id, [0x01; 8]);
        assert_eq!(mapped.duration_ns, 1_000_000_000);
    }

    #[test]
    fn trace_attr_row_est_source_bytes_matches_est_bytes_on_the_materialized_row() {
        let record = attr_record();
        let mapped = TraceAttrRow::from(&record);
        assert_eq!(TraceAttrRow::est_source_bytes(&record), mapped.est_bytes());
    }

    #[test]
    fn trace_attr_row_spool_encoding_renders_hex_ids_and_plain_val_num() {
        let spooled = TraceAttrRow::from(&attr_record()).to_spool_value();
        assert_eq!(
            spooled,
            serde_json::json!({
                "date": 19_675,
                "key": "http.status_code",
                "val": "500",
                "scope": "span",
                "val_num": 500.0,
                "timestamp_ns": 1_700_000_000_000_000_000i64,
                "trace_id": "abababababababababababababababab",
                "span_id": "0101010101010101",
                "duration_ns": 1_000_000_000,
                "val_type": "int",
            })
        );
    }

    /// Issue #476: the stored type reaches `TraceAttrRow` from the
    /// `AttrRecord`'s enum, spelled by `AttrValueType::as_str`. Every
    /// spelling is asserted with its own arm, so swapping two arms of
    /// `as_str` fails here rather than passing a check that only looks
    /// for four known strings somewhere.
    #[test]
    fn trace_attr_row_carries_the_records_stored_type_spelling() {
        for (kind, spelling) in [
            (AttrValueType::String, "string"),
            (AttrValueType::Int, "int"),
            (AttrValueType::Float, "float"),
            (AttrValueType::Bool, "bool"),
        ] {
            let mut record = attr_record();
            record.val_type = kind;
            assert_eq!(
                TraceAttrRow::from(&record).val_type,
                spelling,
                "{kind:?} must store {spelling:?}"
            );
        }
    }

    /// The estimate stays equal on both sides of materialization — the
    /// reserve-before-materialize invariant the writer's queue depends on.
    #[test]
    fn trace_attr_row_estimates_match_across_materialization() {
        let record = attr_record();
        assert_eq!(
            TraceAttrRow::est_source_bytes(&record),
            TraceAttrRow::from(&record).est_bytes()
        );
    }

    // -- the landing row (issue #603) ---------------------------------

    /// The 26 columns `metric_landing` declares, in its own declaration
    /// order. Written out here so the row type cannot drift from the schema
    /// silently — including the one column the writer must NOT send.
    const LANDING_COLUMNS: [&str; 26] = [
        "event_id",
        "received_ms",
        "kind",
        "metric_name",
        "fingerprint",
        "unix_milli",
        "value",
        "labels",
        "value_type",
        "metric_type",
        "help",
        "unit",
        "updated_ns",
        "hist_schema",
        "hist_zero_threshold",
        "hist_zero_count",
        "hist_count",
        "hist_sum",
        "hist_pos_span_offsets",
        "hist_pos_span_lengths",
        "hist_pos_bucket_deltas",
        "hist_neg_span_offsets",
        "hist_neg_span_lengths",
        "hist_neg_bucket_deltas",
        "hist_custom_values",
        "hist_counter_reset_hint",
    ];

    /// The insert's column list is exactly the row type's `COLUMN_NAMES`, so
    /// omitting `event_id` from the type is what makes the server fill the
    /// column from its own `DEFAULT generateUUIDv7()`. A row type carrying
    /// the column would store whatever the writer put there — the nil UUID on
    /// every row, for an explicit zero — while passing every other case here.
    #[test]
    fn the_insert_omits_event_id_so_the_server_fills_it() {
        let names = <MetricLandingRow as pulsus_clickhouse::Row>::COLUMN_NAMES;
        assert_eq!(names.len(), 25, "the 26 columns less event_id");
        let expected: Vec<&str> = LANDING_COLUMNS
            .iter()
            .copied()
            .filter(|c| *c != "event_id")
            .collect();
        assert_eq!(names, expected.as_slice(), "in the table's own order");
        assert!(
            !names.contains(&"event_id"),
            "the writer never sets the landed event's identity"
        );
    }

    fn landing_float(value: f64) -> MetricLandingRow {
        MetricLandingRow::float_sample(
            7,
            &MetricPoint {
                metric_name: Arc::from("m"),
                fingerprint: Fingerprint::from_raw(1),
                unix_milli: 1_000,
                value,
            },
        )
    }

    fn landing_hist(sum: f64, zero_threshold: f64, custom_values: Vec<f64>) -> MetricLandingRow {
        MetricLandingRow::hist_sample(
            7,
            &HistogramPoint {
                metric_name: Arc::from("m"),
                fingerprint: Fingerprint::from_raw(1),
                unix_milli: 1_000,
                histogram: NativeHistogram {
                    counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
                    schema: 0,
                    zero_threshold,
                    zero_count: 0,
                    count: 1,
                    sum,
                    positive_spans: vec![Span {
                        offset: 1,
                        length: 1,
                    }],
                    negative_spans: vec![],
                    positive_buckets: vec![1],
                    negative_buckets: vec![],
                    custom_values,
                },
            },
        )
    }

    /// A landing row serialized by plain JSON would turn every non-finite
    /// float into `null`. Each `f64` column keeps its exact bit pattern in a
    /// decimal-string `*_bits` field beside a readable value that is `null`
    /// when the original was not finite — the shipped encoding, on the shipped
    /// keys.
    #[test]
    fn the_landing_spool_keeps_every_float_bit() {
        let float = landing_float(f64::from_bits(STALE_NAN_BITS)).to_spool_value();
        assert_eq!(
            float["value_bits"],
            serde_json::Value::String(STALE_NAN_BITS.to_string())
        );
        assert!(float["value"].is_null());

        let hist = landing_hist(
            f64::from_bits(STALE_NAN_BITS),
            f64::INFINITY,
            vec![f64::NAN, 1.5],
        )
        .to_spool_value();
        assert_eq!(
            hist["sum_bits"],
            serde_json::Value::String(STALE_NAN_BITS.to_string())
        );
        assert!(hist["sum"].is_null());
        assert_eq!(
            hist["zero_threshold_bits"],
            serde_json::Value::String(f64::INFINITY.to_bits().to_string())
        );
        assert!(hist["zero_threshold"].is_null());
        assert_eq!(
            hist["custom_values_bits"],
            serde_json::json!([f64::NAN.to_bits().to_string(), 1.5f64.to_bits().to_string()])
        );
        assert_eq!(hist["custom_values"], serde_json::json!([null, 1.5]));
    }

    /// One encoder emitting every column of the union, or omitting `kind`,
    /// would put another kind's fields in a row that does not carry them.
    /// Each kind's key set is exactly `{kind, received_ms}` plus the keys that
    /// kind's shipped target encoder emits.
    #[test]
    fn the_landing_spool_carries_one_kind_and_its_own_fields() {
        let (labels, _) = LabelSet::from_normalized([("a".to_string(), "b".to_string())]);
        let series = SeriesRef {
            metric_name: Arc::from("m"),
            fingerprint: Fingerprint::from_raw(1),
            labels,
        };
        let meta = MetricMetadata {
            metric_name: Arc::from("m"),
            metric_type: "counter".to_string(),
            help: "h".to_string(),
            unit: "s".to_string(),
            updated_ns: 9,
        };

        let cases: [(MetricLandingRow, Vec<&str>); 4] = [
            (
                landing_float(1.5),
                vec![
                    "metric_name",
                    "fingerprint",
                    "unix_milli",
                    "value",
                    "value_bits",
                ],
            ),
            (
                landing_hist(1.0, 0.0, vec![]),
                vec![
                    "metric_name",
                    "fingerprint",
                    "unix_milli",
                    "schema",
                    "zero_threshold",
                    "zero_threshold_bits",
                    "zero_count",
                    "count",
                    "sum",
                    "sum_bits",
                    "pos_span_offsets",
                    "pos_span_lengths",
                    "pos_bucket_deltas",
                    "neg_span_offsets",
                    "neg_span_lengths",
                    "neg_bucket_deltas",
                    "custom_values",
                    "custom_values_bits",
                    "counter_reset_hint",
                ],
            ),
            (
                MetricLandingRow::series(7, &series, 3_600_000, 1),
                vec![
                    "metric_name",
                    "fingerprint",
                    "unix_milli",
                    "labels",
                    "value_type",
                ],
            ),
            (
                MetricLandingRow::metadata(7, &meta),
                vec!["metric_name", "metric_type", "help", "unit", "updated_ns"],
            ),
        ];

        for (row, own_keys) in cases {
            let kind = row.kind;
            let value = row.to_spool_value();
            let got: BTreeSet<String> = value
                .as_object()
                .expect("a spool row is an object")
                .keys()
                .cloned()
                .collect();
            let mut want: BTreeSet<String> = own_keys.into_iter().map(str::to_string).collect();
            want.insert("kind".to_string());
            want.insert("received_ms".to_string());
            assert_eq!(got, want, "kind {kind}'s key set");
            assert_eq!(value["kind"].as_u64(), Some(u64::from(kind)));
            assert_eq!(value["received_ms"].as_i64(), Some(7));
        }
    }

    /// The four builders set their own kind's columns and leave every other
    /// column at the type's default — so nothing of another kind's shape
    /// travels in a row.
    #[test]
    fn each_landing_builder_sets_only_its_own_kind_columns() {
        let float = landing_float(1.5);
        assert_eq!(float.kind, MetricLandingRow::KIND_FLOAT);
        assert_eq!(float.labels, "");
        assert_eq!(float.metric_type, "");
        assert_eq!(float.updated_ns, 0);
        assert_eq!(float.hist_count, 0);
        assert!(float.hist_pos_span_offsets.is_empty());

        let hist = landing_hist(1.0, 0.25, vec![]);
        assert_eq!(hist.kind, MetricLandingRow::KIND_HIST);
        assert_eq!(hist.value, 0.0);
        assert_eq!(hist.labels, "");
        assert_eq!(hist.value_type, 0);

        let (labels, _) = LabelSet::from_normalized([("a".to_string(), "b".to_string())]);
        let series = MetricLandingRow::series(
            7,
            &SeriesRef {
                metric_name: Arc::from("m"),
                fingerprint: Fingerprint::from_raw(1),
                labels,
            },
            3_600_000,
            1,
        );
        assert_eq!(series.kind, MetricLandingRow::KIND_SERIES);
        assert_eq!(series.unix_milli, 3_600_000, "the bucket floor");
        assert_eq!(series.labels, r#"{"a":"b"}"#);
        assert_eq!(series.value_type, 1);
        assert_eq!(series.value, 0.0);
        assert_eq!(series.help, "");

        let meta = MetricLandingRow::metadata(
            7,
            &MetricMetadata {
                metric_name: Arc::from("m"),
                metric_type: "counter".to_string(),
                help: "h".to_string(),
                unit: "s".to_string(),
                updated_ns: 9,
            },
        );
        assert_eq!(meta.kind, MetricLandingRow::KIND_METADATA);
        assert_eq!(meta.help, "h");
        assert_eq!(meta.unit, "s");
        assert_eq!(meta.fingerprint, Fingerprint::from_raw(0));
        assert_eq!(meta.unix_milli, 0);
    }

    /// Issue #603 code review, finding 3: the ingest queue holds a landing
    /// row, not the target row it becomes, so charging it the target row's
    /// size leaves `PULSUS_INGEST_QUEUE_BYTES` bounding something it does not
    /// hold. A kind-0 landing row's target estimate is 33 bytes for a
    /// one-character metric name, while the row itself occupies the union of
    /// all four kinds' columns — so the shortfall is per row and grows with
    /// the row count.
    ///
    /// The floor is derived by hand from the declaration, so the `size_of`
    /// equality below is not the only thing establishing the figure: three
    /// `i64` (24) + three `f64` (24) + two `u64` (16) + one 16-byte
    /// fingerprint + four `String` headers (4 × 24 = 96) + seven `Vec`
    /// headers (7 × 24 = 168) + four one-byte integers = 348, plus alignment
    /// padding for the 16-byte-aligned fingerprint.
    #[test]
    fn a_landing_row_is_charged_the_row_the_queue_holds() {
        const HAND_DERIVED_FLOOR: u64 = 348;
        let slots = LANDING_ROW_SLOT_BYTES;
        assert_eq!(
            slots,
            std::mem::size_of::<MetricLandingRow>() as u64,
            "the constant prices the declaration it names"
        );
        assert!(
            slots >= HAND_DERIVED_FLOOR,
            "a landing row's slots come to at least {HAND_DERIVED_FLOOR}, got {slots}"
        );

        // Every kind's charge is its target estimate plus the row it is held
        // in, so no kind escapes the slot cost — a kind whose columns are
        // mostly empty still occupies every slot.
        for target in [0u64, 14, 28, 33, 75, 1_000_000] {
            assert_eq!(
                MetricLandingRow::est_landing_bytes(target),
                target + LANDING_ROW_SLOT_BYTES,
                "the charge for a row whose buffers cost {target}"
            );
        }
        assert!(
            MetricLandingRow::est_landing_bytes(33)
                >= std::mem::size_of::<MetricLandingRow>() as u64,
            "a kind-0 row's charge must cover the row it is held in"
        );
    }
}
