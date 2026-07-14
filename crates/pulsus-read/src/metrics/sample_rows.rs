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
