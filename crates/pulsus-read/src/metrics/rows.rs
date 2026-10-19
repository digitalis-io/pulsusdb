//! Result-row shape for the label-cache refresh sweep, mirroring
//! `logql/rows.rs`'s `#[derive(Row)]` convention: deserialized straight off
//! `ChClient::query_stream`.

use pulsus_clickhouse::Row;
use pulsus_model::Fingerprint;
use serde::{Deserialize, Serialize};

/// One series from statement 2 or 4 (issue #623): its ID, its name and its
/// own lookup row's labels — the sweep ([`super::sql::sweep_query`],
/// docs/architecture.md §5.2), the discovery reads and the fallback's label
/// hydration.
/// `labels` is the canonical JSON string the writer produced
/// (`LabelSet::to_canonical_json`) — parsed into a `LabelSet` by
/// [`super::refresh`], not here (this module only owns the wire shape).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct SeriesRow {
    pub fingerprint: Fingerprint,
    pub metric_name: String,
    pub labels: String,
}

/// Issue #96's degraded-cache discovery probe result row: the single
/// `metric_name` column of the bounded `SELECT DISTINCT metric_name`
/// probe ([`super::sql::distinct_metric_names_probe`]). Deliberately not
/// [`SeriesRow`] (which also carries `fingerprint`/`labels`, columns the
/// probe never selects — reusing the 3-field row here would be a
/// column-count mismatch against the 1-column result set), mirroring the
/// `FingerprintOnlyRow` precedent in [`super::exec`].
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct MetricNameRow {
    pub metric_name: String,
}

/// Issue #635: one row of the label index's `SELECT DISTINCT key`.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct LabelKeyRow {
    pub key: String,
}

/// Issue #635: one row of the label index's `SELECT DISTINCT value`.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct LabelValueRow {
    pub value: String,
}

/// Issue #501 part 2, statement C: the stored exemplars of the selected
/// series in the request window, and the earliest and latest of them.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct ExemplarCountRow {
    pub n: u64,
    pub lo: i64,
    pub hi: i64,
}

/// Issue #501 part 2, statements A and T: one exemplar as stored —
/// `trace_id` and `span_id` all zero when the exemplar carried none, and
/// `labels` the canonical JSON of its filtered attributes.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct ExemplarRow {
    pub fingerprint: Fingerprint,
    pub unix_milli: i64,
    pub value: f64,
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub labels: String,
}

/// Issue #501 part 2, statement H: one series' name and canonical label
/// JSON. The columns are not named `metric_name` and `labels`, so the
/// statement's `WHERE` reads the table's own columns, not its aggregates.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct ExemplarSeriesRow {
    pub fingerprint: Fingerprint,
    pub series_name: String,
    pub series_labels: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn series_row_derives_are_usable() {
        let a = SeriesRow {
            fingerprint: Fingerprint::from_raw(1),
            metric_name: "up".to_string(),
            labels: "{}".to_string(),
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn metric_name_row_derives_are_usable() {
        let a = MetricNameRow {
            metric_name: "up".to_string(),
        };
        let b = a.clone();
        assert_eq!(a, b);
    }
}
