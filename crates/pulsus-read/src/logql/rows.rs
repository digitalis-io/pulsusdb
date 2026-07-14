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
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct SampleRow {
    pub fingerprint: u64,
    pub timestamp_ns: i64,
    pub body: String,
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
    fn metric_bucket_row_derives_are_usable() {
        let a = MetricBucketRow {
            fingerprint: 1,
            step: 0,
            n: 5,
        };
        assert_eq!(a, a);
    }
}
