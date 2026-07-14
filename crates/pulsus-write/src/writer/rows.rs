//! ClickHouse row shapes for `log_samples`/`log_streams` (docs/schemas.md
//! §3.1), their conversions from the parser's `LogRow`/`StreamRow` (issue
//! #8), and a byte-size estimate used for both the `PULSUS_BATCH_BYTES`
//! flush threshold and the `PULSUS_INGEST_QUEUE_BYTES` admission
//! reservation (architect plan).

use pulsus_clickhouse::Row;
use pulsus_model::LabelSet;
use serde::{Deserialize, Serialize};

use crate::protocols::otlp_logs::{LogRow, StreamRow};

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

#[cfg(test)]
mod tests {
    use pulsus_model::{Date, LabelSet, UnixNano};

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
}
