//! Result-row shapes for each stage/query kind, deserialized straight off
//! `ChClient::query_stream` (`pulsus_clickhouse::Row` derive, matching the
//! crate's RowBinary convention).

use pulsus_clickhouse::Row;
use serde::{Deserialize, Serialize};

/// Stage 1 — stream resolution (`log_streams_idx`): one fingerprint per
/// matching stream.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct StreamRow {
    pub fingerprint: u64,
}

/// Stage 2 — hydration (`log_streams`): response labels plus the `service`
/// set stage 3 needs. Reads without `FINAL` may return pre-merge duplicate
/// rows per fingerprint (`ReplacingMergeTree`); the engine dedups by
/// `fingerprint` (labels/service are identical per fingerprint, so keeping
/// any one row is safe — docs/schemas.md §3.2 edge cases).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct StreamMetaRow {
    pub fingerprint: u64,
    pub service: String,
    /// Canonical JSON, sorted keys (docs/schemas.md §3.1).
    pub labels: String,
}

/// Stage 3 — samples (`log_samples`): one matching log line.
/// `structured_metadata` is the per-entry canonical JSON String (issue #97),
/// the LAST projected column (append-only, aligning with the additive
/// `ADD COLUMN` migration). Empty string = no structured metadata (also what
/// pre-#97 rows read back via the column's `DEFAULT ''`).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct SampleRow {
    pub fingerprint: u64,
    pub timestamp_ns: i64,
    pub body: String,
    pub structured_metadata: String,
}

/// A live-tail keyset page row (issue #74): stage 3's sample columns plus
/// the ClickHouse-computed `cityHash64(body)` the composite cursor is
/// keyed on (projected server-side so the cursor can never diverge from
/// the SQL predicate's own hash). `structured_metadata` (issue #97) is the
/// per-entry JSON String; the cursor keys on `(timestamp_ns, fingerprint,
/// body_hash)` only — structured metadata never enters it.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct TailSampleRow {
    pub fingerprint: u64,
    pub timestamp_ns: i64,
    pub body: String,
    pub body_hash: u64,
    pub structured_metadata: String,
}

/// The client-aggregated LogQL metric raw scan (`metric_raw_samples`): the
/// same three columns stage 3 projected before issue #97, WITHOUT
/// `structured_metadata`. A metric aggregation never reads structured
/// metadata (it is surfaced only in the streams/tail label set, not in metric
/// grouping — issue #97 is scoped to those paths), so this lean row keeps the
/// unbounded metric scan from reading a column it never uses (the query-
/// performance mandate) and leaves `metric_raw_samples`'s SQL byte-identical.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricScanRow {
    pub fingerprint: u64,
    pub timestamp_ns: i64,
    pub body: String,
}

/// The single `/api/logs/v1/stats` aggregation row (issue #74): both the
/// rollup-served and the raw-fallback shapes project exactly these four
/// counters, in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct LogStatsRow {
    pub streams: u64,
    pub chunks: u64,
    pub entries: u64,
    pub bytes: u64,
}

/// One `/api/logs/v1/volume` aggregation row (issue #169): a fingerprint's
/// summed byte volume over the query window, off `log_metrics_<res>`
/// (rollup-only — the volume endpoint has no raw fallback).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct VolumeRow {
    pub fingerprint: u64,
    pub bytes: u64,
}

/// One `/api/logs/v1/detected_labels` aggregation row (issue #170): a
/// distinct `log_streams_idx` key with its exact value cardinality and the
/// count of values that are neither float nor UUID (`non_id_values` — the
/// server-side half of the reference's `containsAllIDTypes` filter; the
/// engine keeps a key iff it is a static label or `non_id_values > 0`).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct DetectedLabelRow {
    pub key: String,
    pub cardinality: u64,
    pub non_id_values: u64,
}

/// One `/api/logs/v1/patterns` aggregation row (M7-C3, issue #171): a
/// distinct template, its total count across the window, and the ascending
/// `(ts_ns, count)` per-step samples the server-side `groupArray` assembled.
/// `samples` maps to `Array(Tuple(Int64, UInt64))` on the RowBinary wire.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct PatternFetchRow {
    pub pattern: String,
    pub total: u64,
    pub samples: Vec<(i64, u64)>,
}

/// Labels discovery (`log_streams_idx`): one distinct label key.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct LabelNameRow {
    pub name: String,
}

/// Label-values discovery (`log_streams_idx`): one distinct value of the
/// requested key.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct LabelValueRow {
    pub value: String,
}

/// A selectivity probe result (`count()` over one matcher's index prefix).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct ProbeRow {
    pub n: u64,
}

/// A range-query metric bucket: one `(fingerprint, step, n)` point, from
/// either the rollup table (`sum(count)`/`sum(bytes)`) or the raw fallback
/// (`count()`/`sum(length(body))`) — same shape either way
/// (docs/schemas.md §3.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricBucketRow {
    pub fingerprint: u64,
    pub step: i64,
    pub n: u64,
}

/// An instant-query metric point: one `(fingerprint, n)` aggregate over the
/// single evaluation window — structurally no `step` column, matching
/// [`crate::logql::params::QuerySpec::Instant`]'s "no bucketing" contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricInstantRow {
    pub fingerprint: u64,
    pub n: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_row_derives_are_usable() {
        let a = StreamRow { fingerprint: 1 };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn label_name_row_derives_are_usable() {
        let a = LabelNameRow {
            name: "env".to_string(),
        };
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn label_value_row_derives_are_usable() {
        let a = LabelValueRow {
            value: "prod".to_string(),
        };
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn metric_bucket_row_derives_are_usable() {
        let a = MetricBucketRow {
            fingerprint: 1,
            step: 0,
            n: 5,
        };
        assert_eq!(a, a);
    }
}
