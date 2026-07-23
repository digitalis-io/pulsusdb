//! The Tempo-native TraceQL metrics response body (issue #182) — the wire
//! shape the Tempo datasource expects from
//! `/api/traces/v1/metrics/{query_range,query}` and the two datasource
//! aliases. This **replaces** the Prometheus matrix/vector envelope on
//! those endpoints (a documented breaking change; they are
//! Tempo-datasource-only and never spoke PromQL).
//!
//! Clean-room: the shape below was authored from the published
//! grafana.com/docs/tempo docs plus a black-box capture of the pinned
//! `grafana/tempo:3.0.2` container (Plan v3 Fix 1) — no Tempo/`tempopb`
//! source, `.proto`, or generated code was read or vendored. The captured
//! invariants, all pinned by the byte-for-byte encoder golden below:
//!   * top level `{"series":[…],"metrics":{"completedJobs":…,"totalJobs":…}}`
//!   * labels are OTLP protojson `AnyValue` (camelCase `stringValue`/
//!     `doubleValue`)
//!   * `timestampMs` is a JSON **string** int64
//!   * a sample `value` is **omitted when zero** (protojson default omission)
//!   * exemplars carry the trace reference as a `trace:id` label, not a
//!     top-level `traceId`/`spanId`

use axum::response::{IntoResponse, Response};
use serde::{Serialize, Serializer};

use pulsus_read::{MetricExemplar, MetricLabel, MetricLabelValue, TraceMetricsResult};

/// Serializes an `i64` as a JSON string (protojson int64 convention).
fn i64_str<S: Serializer>(v: &i64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&v.to_string())
}

/// Matches Tempo's protojson default-omission of a zero `value`.
fn f64_is_zero(v: &f64) -> bool {
    *v == 0.0
}

#[derive(Serialize)]
struct MetricsResponse {
    series: Vec<TsSeries>,
    metrics: RangeMetrics,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RangeMetrics {
    completed_jobs: u32,
    total_jobs: u32,
}

#[derive(Serialize)]
struct TsSeries {
    labels: Vec<Label>,
    samples: Vec<Sample>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    exemplars: Vec<Exemplar>,
}

#[derive(Serialize)]
struct Label {
    key: String,
    value: AnyValue,
}

/// The OTLP protojson `AnyValue` subset Tempo emits for metric labels.
/// Externally tagged with camelCase field names → `{"stringValue":…}` /
/// `{"doubleValue":…}`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
enum AnyValue {
    StringValue(String),
    DoubleValue(f64),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Sample {
    #[serde(serialize_with = "i64_str")]
    timestamp_ms: i64,
    #[serde(skip_serializing_if = "f64_is_zero")]
    value: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Exemplar {
    labels: Vec<Label>,
    value: f64,
    #[serde(serialize_with = "i64_str")]
    timestamp_ms: i64,
}

fn label(l: &MetricLabel) -> Label {
    let value = match &l.value {
        MetricLabelValue::Str(s) => AnyValue::StringValue(s.clone()),
        MetricLabelValue::Double(d) => AnyValue::DoubleValue(*d),
    };
    Label {
        key: l.key.clone(),
        value,
    }
}

fn exemplar(e: &MetricExemplar) -> Exemplar {
    Exemplar {
        labels: e.labels.iter().map(label).collect(),
        value: e.value,
        timestamp_ms: e.timestamp_ms,
    }
}

/// Frames the engine result into the Tempo-native response body. `metrics`
/// reports a single synchronous job (`completedJobs == totalJobs == 1`) —
/// the reader executes one pushed-down query, so progress is always 100%.
fn build(result: &TraceMetricsResult) -> MetricsResponse {
    let series = result
        .series
        .iter()
        .map(|s| TsSeries {
            labels: s.labels.iter().map(label).collect(),
            samples: s
                .samples
                .iter()
                .map(|&(timestamp_ms, value)| Sample {
                    timestamp_ms,
                    value,
                })
                .collect(),
            exemplars: s.exemplars.iter().map(exemplar).collect(),
        })
        .collect();
    MetricsResponse {
        series,
        metrics: RangeMetrics {
            completed_jobs: 1,
            total_jobs: 1,
        },
    }
}

/// Serializes the engine result to the compact Tempo-native JSON string
/// (also the byte-for-byte golden surface).
pub(crate) fn encode_json(result: &TraceMetricsResult) -> String {
    serde_json::to_string(&build(result)).expect("metrics response serializes")
}

/// The `application/json` HTTP response the metrics endpoints return.
pub(crate) fn encode_metrics(result: &TraceMetricsResult) -> Response {
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        encode_json(result),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_read::{MetricLabel, MetricLabelValue, TraceMetricSeries};

    #[test]
    fn encoder_pins_the_captured_tempo_wire_shape_byte_for_byte() {
        // Two series: an ungrouped `rate` with a zero sample (value
        // omitted) and a positive one, plus a quantile series carrying a
        // `p` double label and a `trace:id` exemplar. Every captured
        // invariant (camelCase, timestampMs-as-string, value-omitted-at-
        // zero, AnyValue labels, trace:id exemplar) is exercised.
        let result = TraceMetricsResult {
            series: vec![
                TraceMetricSeries {
                    labels: vec![MetricLabel::str("__name__", "rate")],
                    samples: vec![
                        (1_784_796_060_000, 0.0),
                        (1_784_796_120_000, 0.8833333333333333),
                    ],
                    exemplars: vec![],
                },
                TraceMetricSeries {
                    labels: vec![
                        MetricLabel::str("resource.service.name", "checkout"),
                        MetricLabel {
                            key: "p".to_string(),
                            value: MetricLabelValue::Double(0.9),
                        },
                    ],
                    samples: vec![(1_784_796_120_000, 1.5)],
                    exemplars: vec![MetricExemplar {
                        labels: vec![MetricLabel::str("trace:id", "ceae79f2")],
                        value: 1.383,
                        timestamp_ms: 1_784_796_062_834,
                    }],
                },
            ],
        };
        let json = encode_json(&result);
        let expected = concat!(
            "{\"series\":[",
            "{\"labels\":[{\"key\":\"__name__\",\"value\":{\"stringValue\":\"rate\"}}],",
            "\"samples\":[{\"timestampMs\":\"1784796060000\"},",
            "{\"timestampMs\":\"1784796120000\",\"value\":0.8833333333333333}]},",
            "{\"labels\":[{\"key\":\"resource.service.name\",\"value\":{\"stringValue\":\"checkout\"}},",
            "{\"key\":\"p\",\"value\":{\"doubleValue\":0.9}}],",
            "\"samples\":[{\"timestampMs\":\"1784796120000\",\"value\":1.5}],",
            "\"exemplars\":[{\"labels\":[{\"key\":\"trace:id\",\"value\":{\"stringValue\":\"ceae79f2\"}}],",
            "\"value\":1.383,\"timestampMs\":\"1784796062834\"}]}",
            "],\"metrics\":{\"completedJobs\":1,\"totalJobs\":1}}",
        );
        assert_eq!(json, expected);
    }

    #[test]
    fn an_empty_result_still_carries_the_metrics_object() {
        let json = encode_json(&TraceMetricsResult { series: vec![] });
        assert_eq!(
            json,
            "{\"series\":[],\"metrics\":{\"completedJobs\":1,\"totalJobs\":1}}"
        );
    }
}
