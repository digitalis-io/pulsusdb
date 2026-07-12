//! Sample and series types shared across ingestion and query engines
//! (docs/architecture.md §2). No writer/insert logic lives here — these are
//! plain data carriers between the model, `pulsus-write`, and
//! `pulsus-read`.

use crate::labels::LabelSet;
use crate::time::{Fingerprint, UnixMilli, UnixNano};

/// One metric data point: `(fingerprint, timestamp_ms, value)`. No string
/// data on this hot path — a fingerprint's labels are resolved separately
/// via [`Series`] (docs/architecture.md §2).
#[derive(Debug, Clone, PartialEq)]
pub struct MetricSample {
    pub fingerprint: Fingerprint,
    pub ts: UnixMilli,
    pub value: f64,
}

/// One log line: `(fingerprint, timestamp_ns, severity, body)`.
#[derive(Debug, Clone, PartialEq)]
pub struct LogSample {
    pub fingerprint: Fingerprint,
    pub ts: UnixNano,
    pub severity: i8,
    pub body: String,
}

/// A fingerprint's resolved label set: a metric series or a
/// log/trace/profile stream identity.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub fingerprint: Fingerprint,
    pub labels: LabelSet,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_sample_stores_fields_verbatim() {
        let s = MetricSample {
            fingerprint: 42,
            ts: UnixMilli(1_700_000_000_000),
            value: 1.5,
        };
        assert_eq!(s.fingerprint, 42);
        assert_eq!(s.ts.0, 1_700_000_000_000);
        assert_eq!(s.value, 1.5);
    }

    #[test]
    fn log_sample_stores_fields_verbatim() {
        let s = LogSample {
            fingerprint: 7,
            ts: UnixNano(1_700_000_000_123_456_789),
            severity: 3,
            body: "boot complete".to_string(),
        };
        assert_eq!(s.severity, 3);
        assert_eq!(s.body, "boot complete");
    }

    #[test]
    fn series_pairs_fingerprint_with_labels() {
        let labels = LabelSet::from_verbatim(vec![("service".to_string(), "checkout".to_string())]);
        let series = Series {
            fingerprint: 1,
            labels: labels.clone(),
        };
        assert_eq!(series.labels, labels);
    }
}
