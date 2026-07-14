//! Result-row shape for the label-cache refresh sweep, mirroring
//! `logql/rows.rs`'s `#[derive(Row)]` convention: deserialized straight off
//! `ChClient::query_stream`.

use pulsus_clickhouse::Row;
use serde::{Deserialize, Serialize};

/// One `metric_series` row from the §5.2 sweep (`SELECT fingerprint,
/// metric_name, labels FROM metric_series WHERE ... ORDER BY unix_milli
/// DESC LIMIT 1 BY metric_name, fingerprint`, docs/architecture.md §5.2).
/// `labels` is the canonical JSON string the writer produced
/// (`LabelSet::to_canonical_json`) — parsed into a `LabelSet` by
/// [`super::refresh`], not here (this module only owns the wire shape).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct SeriesRow {
    pub fingerprint: u64,
    pub metric_name: String,
    pub labels: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn series_row_derives_are_usable() {
        let a = SeriesRow {
            fingerprint: 1,
            metric_name: "up".to_string(),
            labels: "{}".to_string(),
        };
        let b = a.clone();
        assert_eq!(a, b);
    }
}
