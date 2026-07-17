//! Result-row shape for the issue #31 sample fetch, mirroring
//! `metrics/rows.rs`'s `#[derive(Row)]` convention: deserialized straight
//! off `ChClient::query_stream`.

use pulsus_clickhouse::Row;
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
