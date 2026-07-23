//! The engine-produced TraceQL metrics result (issue #182): a set of
//! labelled series, each with time-stamped samples and optional exemplars.
//!
//! This is the read-path twin of the Tempo-native metrics response body
//! the server encodes (`pulsus-server/src/traces_api/metrics_response.rs`).
//! It deliberately replaces the Prometheus matrix/vector `QueryResult` on
//! the two traces metrics endpoints (a documented breaking change — those
//! endpoints are Tempo-datasource-only): labels are typed (OTLP-AnyValue
//! `Str`/`Double`) so the encoder can emit `stringValue`/`doubleValue`
//! byte-for-byte, and exemplars ride inline (the Prometheus envelope has
//! no exemplar slot).

/// A typed metrics label value — mirrors the OTLP protojson `AnyValue`
/// subset Tempo emits for metric-series labels (issue #182). By-keys and
/// `__name__` are `Str`; `p` (quantile) and `__bucket` (histogram le) are
/// `Double`.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricLabelValue {
    Str(String),
    Double(f64),
}

/// One `(key, value)` label on a metrics series or exemplar.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricLabel {
    pub key: String,
    pub value: MetricLabelValue,
}

impl MetricLabel {
    pub fn str(key: impl Into<String>, value: impl Into<String>) -> Self {
        MetricLabel {
            key: key.into(),
            value: MetricLabelValue::Str(value.into()),
        }
    }

    pub fn double(key: impl Into<String>, value: f64) -> Self {
        MetricLabel {
            key: key.into(),
            value: MetricLabelValue::Double(value),
        }
    }
}

/// One representative exemplar for a series/bucket (issue #182): the
/// sampled value, its timestamp, and the `trace:id` reference Tempo emits
/// as a label. The `span_id` is retained only for internal dedup, never a
/// wire field (Tempo emits only the trace reference).
#[derive(Debug, Clone, PartialEq)]
pub struct MetricExemplar {
    pub labels: Vec<MetricLabel>,
    pub value: f64,
    pub timestamp_ms: i64,
}

/// One labelled metrics series. `samples` is `(timestamp_ms, value)`
/// pairs — many for a range query, exactly one for an instant query.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceMetricSeries {
    pub labels: Vec<MetricLabel>,
    pub samples: Vec<(i64, f64)>,
    pub exemplars: Vec<MetricExemplar>,
}

/// The complete engine result for one metrics request — the series the
/// server frames into the Tempo-native `{series, metrics}` envelope.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceMetricsResult {
    pub series: Vec<TraceMetricSeries>,
}
