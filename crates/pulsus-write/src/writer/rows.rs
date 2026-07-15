//! ClickHouse row shapes for `log_samples`/`log_streams` (docs/schemas.md
//! §3.1), their conversions from the parser's `LogRow`/`StreamRow` (issue
//! #8), and a byte-size estimate used for both the `PULSUS_BATCH_BYTES`
//! flush threshold and the `PULSUS_INGEST_QUEUE_BYTES` admission
//! reservation (architect plan).

use pulsus_clickhouse::Row;
use pulsus_model::LabelSet;
use serde::{Deserialize, Serialize};

use crate::ingest::metrics::{MetricMetadata, MetricPoint, SeriesRef};
use crate::ingest::traces::{AttrRecord, SpanRecord};
use crate::protocols::otlp_logs::{LogRow, StreamRow};
use crate::writer::spool::SpoolEncode;

/// One `log_samples` row (docs/schemas.md §3.1).
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct LogSampleRow {
    pub service: String,
    pub fingerprint: u64,
    pub timestamp_ns: i64,
    pub severity: i8,
    pub body: String,
}

impl From<&LogRow> for LogSampleRow {
    fn from(row: &LogRow) -> Self {
        LogSampleRow {
            service: row.service.clone(),
            fingerprint: row.fingerprint,
            timestamp_ns: row.timestamp_ns.0,
            severity: row.severity,
            body: row.body.clone(),
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
        Self::estimate(&self.service, &self.body)
    }

    /// Estimates a `LogRow`'s footprint *before* it is materialized into a
    /// `LogSampleRow` — the reserve-before-materialize hardening
    /// (architect plan amendment 3, finding 2): identical accounting to
    /// [`Self::est_bytes`], read straight off the source row so
    /// `writer::LogWriter::admit_batch`'s `fetch_add` reservation can
    /// happen before the clone that builds the target row.
    pub fn est_source_bytes(row: &LogRow) -> u64 {
        Self::estimate(&row.service, &row.body)
    }

    fn estimate(service: &str, body: &str) -> u64 {
        (service.len() + body.len() + 8 /* fingerprint */ + 8 /* timestamp_ns */ + 1/* severity */)
            as u64
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
    pub fingerprint: u64,
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
        (service.len() + labels_len + 2 /* month */ + 8 /* fingerprint */ + 8/* updated_ns */)
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
    pub fingerprint: u64,
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
        (metric_name.len() + 8 /* fingerprint */ + 8 /* unix_milli */ + 8/* value */) as u64
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
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MetricSeriesRow {
    pub metric_name: String,
    pub fingerprint: u64,
    pub unix_milli: i64,
    pub labels: String,
}

impl MetricSeriesRow {
    /// Builds a row for `series`, bucket-floored to `bucket_unix_milli`
    /// (already computed by the caller via
    /// `pulsus_model::floor_to_activity_bucket`).
    pub fn from_series_at_bucket(series: &SeriesRef, bucket_unix_milli: i64) -> Self {
        MetricSeriesRow {
            metric_name: series.metric_name.to_string(),
            fingerprint: series.fingerprint,
            unix_milli: bucket_unix_milli,
            labels: series.labels.to_canonical_json(),
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
        (metric_name.len() + labels_len + 8 /* fingerprint */ + 8/* unix_milli */) as u64
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
    pub kind: i8,
    /// Always `1` (= OTLP protobuf, docs/schemas.md §4.1's `payload_type`
    /// legend) for rows produced by the OTLP receiver; `2` (Zipkin JSON) is
    /// a compat-receiver value (M6+).
    pub payload_type: i8,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
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
            kind: record.kind,
            payload_type: 1,
            payload: record.payload.clone(),
        }
    }
}

impl TraceSpanRow {
    /// See [`LogSampleRow::est_bytes`]'s doc comment for the estimate's
    /// intent and limits.
    pub fn est_bytes(&self) -> u64 {
        Self::estimate(&self.name, &self.service, self.payload.len())
    }

    /// Estimates a `SpanRecord`'s footprint *before* it is materialized
    /// into a `TraceSpanRow` (reserve-before-materialize, the established
    /// `est_source_bytes` pattern) — identical accounting to
    /// [`Self::est_bytes`], read straight off the source record.
    pub fn est_source_bytes(record: &SpanRecord) -> u64 {
        Self::estimate(&record.name, &record.service, record.payload.len())
    }

    fn estimate(name: &str, service: &str, payload_len: usize) -> u64 {
        (name.len() + service.len() + payload_len
            + 16 /* trace_id */ + 8 /* span_id */ + 8 /* parent_id */
            + 8 /* timestamp_ns */ + 8 /* duration_ns */
            + 1 /* status_code */ + 1 /* kind */ + 1/* payload_type */) as u64
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
            "kind": self.kind,
            "payload_type": self.payload_type,
            "payload_len": self.payload.len(),
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
            + 16 /* trace_id */ + 8 /* span_id */ + 8/* duration_ns */) as u64
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
        })
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
    use std::sync::Arc;

    use pulsus_model::{Date, LabelSet, STALE_NAN_BITS, UnixNano};

    use super::*;

    #[test]
    fn log_sample_row_from_log_row_copies_every_field() {
        let row = LogRow {
            service: "checkout".to_string(),
            fingerprint: 42,
            timestamp_ns: UnixNano(1_700_000_000_000_000_000),
            severity: 9,
            body: "hello".to_string(),
        };
        let mapped = LogSampleRow::from(&row);
        assert_eq!(mapped.service, "checkout");
        assert_eq!(mapped.fingerprint, 42);
        assert_eq!(mapped.timestamp_ns, 1_700_000_000_000_000_000);
        assert_eq!(mapped.severity, 9);
        assert_eq!(mapped.body, "hello");
    }

    #[test]
    fn log_sample_row_est_bytes_grows_with_body_length() {
        let short = LogSampleRow {
            service: String::new(),
            fingerprint: 0,
            timestamp_ns: 0,
            severity: 0,
            body: "a".to_string(),
        };
        let long = LogSampleRow {
            body: "a".repeat(100),
            ..short.clone()
        };
        assert!(long.est_bytes() > short.est_bytes());
    }

    #[test]
    fn log_sample_row_est_source_bytes_matches_est_bytes_on_the_materialized_row() {
        let row = LogRow {
            service: "checkout".to_string(),
            fingerprint: 42,
            timestamp_ns: UnixNano(1_700_000_000_000_000_000),
            severity: 9,
            body: "hello world".to_string(),
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
            fingerprint: 42,
            timestamp_ns: 1_700_000_000_000_000_000,
            severity: 9,
            body: "hello".to_string(),
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
            month: Date::start_of_month_utc(1_700_000_000_000_000_000),
            fingerprint: 7,
            service: "checkout".to_string(),
            labels,
            updated_ns: 123,
        };
        let mapped = LogStreamRow::from(&row);
        assert_eq!(mapped.month, row.month.days_since_epoch());
        assert_eq!(mapped.fingerprint, 7);
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
            month: Date::start_of_month_utc(1_700_000_000_000_000_000),
            fingerprint: 7,
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
            fingerprint: 7,
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
            fingerprint: 42,
            unix_milli: 1_700_000_000_000,
            value: 1.5,
        };
        let mapped = MetricSampleRow::from(&point);
        assert_eq!(mapped.metric_name, "http_requests_total");
        assert_eq!(mapped.fingerprint, 42);
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
            fingerprint: 1,
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
            fingerprint: 1,
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
            fingerprint: 1,
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
            fingerprint: 42,
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
            fingerprint: 7,
            labels,
        };
        let mapped = MetricSeriesRow::from_series_at_bucket(&series, 3_600_000);
        assert_eq!(mapped.metric_name, "http_requests_total");
        assert_eq!(mapped.fingerprint, 7);
        assert_eq!(mapped.unix_milli, 3_600_000);
        assert_eq!(mapped.labels, r#"{"job":"checkout"}"#);
    }

    #[test]
    fn metric_series_row_est_source_bytes_matches_est_bytes_on_the_materialized_row() {
        let (labels, _) = LabelSet::from_normalized([
            ("job".to_string(), "checkout".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]);
        let series = SeriesRef {
            metric_name: Arc::from("http_requests_total"),
            fingerprint: 7,
            labels,
        };
        let mapped = MetricSeriesRow::from_series_at_bucket(&series, 3_600_000);
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
            kind: 3,
            payload: vec![0xDE, 0xAD, 0xBE, 0xEF],
        }
    }

    fn attr_record() -> AttrRecord {
        AttrRecord {
            date: 19_675,
            key: "http.status_code".to_string(),
            scope: "span".to_string(),
            val: "500".to_string(),
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
        assert_eq!(mapped.kind, 3);
        assert_eq!(mapped.payload_type, 1, "OTLP protobuf payload type");
        assert_eq!(mapped.payload, vec![0xDE, 0xAD, 0xBE, 0xEF]);
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
        let spooled = TraceSpanRow::from(&span_record()).to_spool_value();
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
                "kind": 3,
                "payload_type": 1,
                "payload_len": 4,
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
            })
        );
    }
}
