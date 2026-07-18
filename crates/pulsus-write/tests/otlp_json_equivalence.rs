//! Exhaustive hermetic protobuf-vs-OTLP/JSON differential (issue #76).
//!
//! For each signal (logs, metrics, traces) this proves that the SAME logical
//! payload, decoded from protobuf and from OTLP/JSON, feeds the SAME `parse`
//! and yields byte-identical normalized rows — the property that lets OTLP/JSON
//! ride the existing routes as a pure second encoding with no new storage path
//! and no live-ClickHouse leg (the row identity is settled here at the `parse`
//! boundary; `trace_ingest_roundtrip` backstops `parse`->ClickHouse).
//!
//! Coverage spans the tricky field types the adjudication (comment 5004660170)
//! named exhaustive:
//! - hex `trace_id`/`span_id`/`parent_span_id` (traces + log correlation),
//! - u64 timestamps as JSON strings,
//! - the full `AnyValue` oneof: string/bool/int(as-string)/double/bytes(base64)
//!   and nested `kvlistValue`/`arrayValue` (logs),
//! - non-finite doubles `"NaN"`/`"Infinity"`/`"-Infinity"` on gauge samples,
//!   histogram `sum`, and an exemplar `asDouble` (metrics) — which decode (not
//!   reject) only because of the vendored+patched `opentelemetry-proto`
//!   (docs/decisions/0004).
//!
//! Two independent checks per signal, each genuinely cross-encoding: the
//! protobuf side is `parse(decode(encode_pb(req)))` — the SAME logical payload
//! encoded to real protobuf wire bytes (prost) and fed through the real
//! `decode` path — never `parse(&req)` on the in-memory builder. So the check
//! binds protobuf-vs-JSON *row identity* (v1 AC2), not merely JSON
//! self-consistency:
//!   AC3 — self round-trip: `parse(decode_json(to_vec(req)))`
//!         == `parse(decode(encode_pb(req)))`.
//!   AC2 — committed golden: `parse(decode_json(golden.json))`
//!         == `parse(decode(encode_pb(req)))`, where the golden is
//!         human-reviewable, spec-correct protojson frozen on disk (regenerate
//!         with the `#[ignore]` test below, then eyeball the diff: hex IDs,
//!         camelCase keys, string timestamps, `"NaN"` strings).

use std::path::PathBuf;

use prost::Message;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, KeyValueList};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics,
    ScopeMetrics, Sum, exemplar, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use opentelemetry_proto::tonic::trace::v1::status::StatusCode;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

use pulsus_config::ExpHistogramMode;
use pulsus_write::protocols::{otlp_logs, otlp_metrics, otlp_traces};
use pulsus_write::{MetricPoint, ParsedMetrics};

const NOW_NS: i64 = 1_700_000_000_000_000_000;

const TRACE_ID: [u8; 16] = [
    0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36,
];
const SPAN_A_ID: [u8; 8] = [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7];
const SPAN_B_ID: [u8; 8] = [0x0c, 0x1d, 0x2e, 0x3f, 0x40, 0x51, 0x62, 0x73];

fn goldens_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/otlp-json")
}

fn read_golden(name: &str) -> Vec<u8> {
    let path = goldens_dir().join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("read golden {}: {e}", path.display()))
}

fn kv(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        key_strindex: 0,
    }
}

// -------------------------------------------------------------------------
// Protobuf leg of the differential. The builder struct is encoded to real
// protobuf wire bytes (prost) and decoded back through the production
// `otlp_*::decode` path — this is what makes each check genuinely
// protobuf-vs-JSON, rather than comparing `decode_json` against a bare
// in-memory struct (which would only prove JSON self-consistency).
// -------------------------------------------------------------------------

fn logs_via_protobuf(req: &ExportLogsServiceRequest) -> ExportLogsServiceRequest {
    otlp_logs::decode(&req.encode_to_vec()).expect("decode logs protobuf")
}

fn metrics_via_protobuf(req: &ExportMetricsServiceRequest) -> ExportMetricsServiceRequest {
    otlp_metrics::decode(&req.encode_to_vec()).expect("decode metrics protobuf")
}

fn traces_via_protobuf(req: &ExportTraceServiceRequest) -> ExportTraceServiceRequest {
    otlp_traces::decode(&req.encode_to_vec()).expect("decode traces protobuf")
}

// -------------------------------------------------------------------------
// Builders — one logical payload per signal, exercising the tricky types.
// -------------------------------------------------------------------------

fn logs_request() -> ExportLogsServiceRequest {
    // The full AnyValue oneof lives here: attributes carry string/bool/int/
    // double/bytes plus a nested kvlist holding an array of mixed scalars.
    let nested = Value::KvlistValue(KeyValueList {
        values: vec![
            kv("inner.str", Value::StringValue("nested".to_string())),
            kv(
                "inner.arr",
                Value::ArrayValue(ArrayValue {
                    values: vec![
                        AnyValue {
                            value: Some(Value::IntValue(7)),
                        },
                        AnyValue {
                            value: Some(Value::DoubleValue(2.5)),
                        },
                        AnyValue {
                            value: Some(Value::BoolValue(false)),
                        },
                    ],
                }),
            ),
        ],
    });

    let record = LogRecord {
        time_unix_nano: 1_700_000_000_000_000_123,
        observed_time_unix_nano: 1_700_000_000_000_000_456,
        severity_number: 9, // SEVERITY_NUMBER_INFO — integer enum form
        severity_text: "INFO".to_string(),
        body: Some(AnyValue {
            value: Some(Value::StringValue("hello world".to_string())),
        }),
        attributes: vec![
            kv("str.attr", Value::StringValue("s".to_string())),
            kv("bool.attr", Value::BoolValue(true)),
            kv("int.attr", Value::IntValue(42)),
            kv("double.attr", Value::DoubleValue(3.5)),
            // bytesValue is base64 in protojson (not hex — that exception is
            // reserved for trace/span IDs).
            kv(
                "bytes.attr",
                Value::BytesValue(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ),
            kv("nested.attr", nested),
        ],
        trace_id: TRACE_ID.to_vec(),
        span_id: SPAN_A_ID.to_vec(),
        ..Default::default()
    };

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv(
                    "service.name",
                    Value::StringValue("checkout".to_string()),
                )],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![record],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn metrics_request() -> ExportMetricsServiceRequest {
    fn point(value: f64, ts: u64) -> NumberDataPoint {
        NumberDataPoint {
            attributes: vec![kv("host", Value::StringValue("h1".to_string()))],
            time_unix_nano: ts,
            value: Some(number_data_point::Value::AsDouble(value)),
            ..Default::default()
        }
    }

    // Gauge with finite + all three non-finite samples: these reach
    // MetricPoint.value and drive the NaN-aware differential comparator.
    let gauge = Metric {
        name: "temperature".to_string(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![
                point(21.5, 1_700_000_000_000_000_001),
                point(f64::NAN, 1_700_000_000_000_000_002),
                point(f64::INFINITY, 1_700_000_000_000_000_003),
                point(f64::NEG_INFINITY, 1_700_000_000_000_000_004),
            ],
        })),
        ..Default::default()
    };

    // A monotonic Sum (counter) — ordinary finite path.
    let counter = Metric {
        name: "requests".to_string(),
        data: Some(metric::Data::Sum(Sum {
            data_points: vec![point(1234.0, 1_700_000_000_000_000_005)],
            aggregation_temporality: 2, // CUMULATIVE
            is_monotonic: true,
        })),
        ..Default::default()
    };

    // Histogram carrying a non-finite `sum` and a non-finite `+Inf` explicit
    // bound plus an exemplar `asDouble = +Inf` — these must DECODE (not 400);
    // the patch is what makes that hold.
    let histogram = Metric {
        name: "latency".to_string(),
        data: Some(metric::Data::Histogram(Histogram {
            data_points: vec![HistogramDataPoint {
                attributes: vec![kv("host", Value::StringValue("h1".to_string()))],
                time_unix_nano: 1_700_000_000_000_000_006,
                count: 3,
                sum: Some(f64::NAN),
                bucket_counts: vec![1, 2],
                explicit_bounds: vec![f64::INFINITY],
                exemplars: vec![Exemplar {
                    time_unix_nano: 1_700_000_000_000_000_006,
                    value: Some(exemplar::Value::AsDouble(f64::INFINITY)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            aggregation_temporality: 2,
        })),
        ..Default::default()
    };

    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", Value::StringValue("api".to_string()))],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![gauge, counter, histogram],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn traces_request() -> ExportTraceServiceRequest {
    let span_a = Span {
        trace_id: TRACE_ID.to_vec(),
        span_id: SPAN_A_ID.to_vec(),
        parent_span_id: vec![], // empty parent -> zero sentinel
        name: "GET /checkout".to_string(),
        kind: SpanKind::Server as i32, // integer enum form
        start_time_unix_nano: NOW_NS as u64,
        end_time_unix_nano: NOW_NS as u64 + 1_000_000_000,
        attributes: vec![
            kv("http.status_code", Value::IntValue(500)),
            kv("http.method", Value::StringValue("GET".to_string())),
        ],
        ..Default::default()
    };
    let span_b = Span {
        trace_id: TRACE_ID.to_vec(),
        span_id: SPAN_B_ID.to_vec(),
        parent_span_id: SPAN_A_ID.to_vec(),
        name: "charge-card".to_string(),
        kind: SpanKind::Client as i32,
        start_time_unix_nano: NOW_NS as u64 + 2_000_000,
        end_time_unix_nano: NOW_NS as u64 + 5_000_000,
        status: Some(Status {
            message: "card declined".to_string(),
            code: StatusCode::Error as i32,
        }),
        ..Default::default()
    };

    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv(
                    "service.name",
                    Value::StringValue("checkout".to_string()),
                )],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![span_a, span_b],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

// -------------------------------------------------------------------------
// NaN-aware metrics comparator (#33/#65 precedent) — derived PartialEq on
// ParsedMetrics compares f64 with `==`, so identical NaN samples would fail.
// Scoped to this test only; production keeps its derives.
// -------------------------------------------------------------------------

fn f64_bit_eq_nan(a: f64, b: f64) -> bool {
    (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits()
}

fn sample_eq(a: &MetricPoint, b: &MetricPoint) -> bool {
    a.metric_name == b.metric_name
        && a.fingerprint == b.fingerprint
        && a.unix_milli == b.unix_milli
        && f64_bit_eq_nan(a.value, b.value)
}

fn assert_parsed_metrics_eq(json: &ParsedMetrics, pb: &ParsedMetrics) {
    assert_eq!(
        json.samples.len(),
        pb.samples.len(),
        "sample count differs (json vs protobuf)"
    );
    for (i, (j, p)) in json.samples.iter().zip(pb.samples.iter()).enumerate() {
        assert!(
            sample_eq(j, p),
            "sample {i} differs: json={j:?} protobuf={p:?}"
        );
    }
    // Everything else compares with derived equality (no bare f64).
    assert_eq!(json.series, pb.series, "series differ");
    assert_eq!(json.metadata, pb.metadata, "metadata differ");
    assert_eq!(json.collisions, pb.collisions, "collisions differ");
    assert_eq!(json.rejected, pb.rejected, "rejected differ");
    assert_eq!(
        json.rejected_message, pb.rejected_message,
        "rejected_message differ"
    );
    // Sanity: the non-finite samples actually landed (guards against a future
    // parse change silently dropping them, which would make the test vacuous).
    let has_nan = pb.samples.iter().any(|s| s.value.is_nan());
    let has_inf = pb.samples.iter().any(|s| s.value.is_infinite());
    assert!(
        has_nan && has_inf,
        "expected non-finite samples in the golden"
    );
}

// -------------------------------------------------------------------------
// AC3 — self round-trip (serialize then decode_json), all three signals.
// -------------------------------------------------------------------------

#[test]
fn logs_self_round_trip_is_row_identical() {
    let req = logs_request();
    let json = serde_json::to_vec(&req).expect("serialize logs");
    let via_json = otlp_logs::decode_json(&json).expect("decode_json logs");
    let via_pb = logs_via_protobuf(&req);
    assert_eq!(
        otlp_logs::parse(&via_json, NOW_NS),
        otlp_logs::parse(&via_pb, NOW_NS)
    );
}

#[test]
fn metrics_self_round_trip_is_row_identical() {
    let req = metrics_request();
    let json = serde_json::to_vec(&req).expect("serialize metrics");
    let via_json = otlp_metrics::decode_json(&json).expect("decode_json metrics");
    let via_pb = metrics_via_protobuf(&req);
    assert_parsed_metrics_eq(
        &otlp_metrics::parse(&via_json, NOW_NS, ExpHistogramMode::Classic)
            .expect("within the expansion budget"),
        &otlp_metrics::parse(&via_pb, NOW_NS, ExpHistogramMode::Classic)
            .expect("within the expansion budget"),
    );
}

#[test]
fn traces_self_round_trip_is_row_identical() {
    let req = traces_request();
    let json = serde_json::to_vec(&req).expect("serialize traces");
    let via_json = otlp_traces::decode_json(&json).expect("decode_json traces");
    let via_pb = traces_via_protobuf(&req);
    assert_eq!(
        otlp_traces::parse(&via_json, NOW_NS).expect("json within budget"),
        otlp_traces::parse(&via_pb, NOW_NS).expect("pb within budget"),
    );
}

// -------------------------------------------------------------------------
// AC2 — committed golden (spec-correct protojson frozen on disk).
// -------------------------------------------------------------------------

#[test]
fn logs_golden_json_is_row_identical_to_protobuf() {
    let via_json = otlp_logs::decode_json(&read_golden("logs.json")).expect("decode logs golden");
    let via_pb = logs_via_protobuf(&logs_request());
    assert_eq!(
        otlp_logs::parse(&via_json, NOW_NS),
        otlp_logs::parse(&via_pb, NOW_NS),
    );
}

#[test]
fn metrics_golden_json_is_row_identical_to_protobuf() {
    let via_json =
        otlp_metrics::decode_json(&read_golden("metrics.json")).expect("decode metrics golden");
    let via_pb = metrics_via_protobuf(&metrics_request());
    assert_parsed_metrics_eq(
        &otlp_metrics::parse(&via_json, NOW_NS, ExpHistogramMode::Classic)
            .expect("within the expansion budget"),
        &otlp_metrics::parse(&via_pb, NOW_NS, ExpHistogramMode::Classic)
            .expect("within the expansion budget"),
    );
}

#[test]
fn traces_golden_json_is_row_identical_to_protobuf() {
    let via_json =
        otlp_traces::decode_json(&read_golden("traces.json")).expect("decode traces golden");
    let via_pb = traces_via_protobuf(&traces_request());
    assert_eq!(
        otlp_traces::parse(&via_json, NOW_NS).expect("json within budget"),
        otlp_traces::parse(&via_pb, NOW_NS).expect("pb within budget"),
    );
}

// -------------------------------------------------------------------------
// AC6 — non-finite metric JSON decodes to 200 (not 400) and yields the exact
// non-finite sample values. This is the property the vendored patch exists for.
// -------------------------------------------------------------------------

#[test]
fn non_finite_metric_json_decodes_and_preserves_values() {
    let via_json = otlp_metrics::decode_json(&read_golden("metrics.json"))
        .expect("non-finite JSON must decode");
    let out = otlp_metrics::parse(&via_json, NOW_NS, ExpHistogramMode::Classic)
        .expect("within the expansion budget");
    let values: Vec<f64> = out.samples.iter().map(|s| s.value).collect();
    assert!(values.iter().any(|v| v.is_nan()), "expected a NaN sample");
    assert!(values.contains(&f64::INFINITY), "expected a +Inf sample");
    assert!(
        values.contains(&f64::NEG_INFINITY),
        "expected a -Inf sample"
    );
}

// -------------------------------------------------------------------------
// #98 — proto3-JSON enum string-NAME acceptance. proto3-JSON permits enum
// fields as EITHER the integer value OR the string name. #76 accepted only the
// integer form (what real emitters send); the vendored patch item P5
// (docs/decisions/0004) now also accepts the string name, deserialize-only, so
// both forms decode identically while serialize/the wire codec stay integer.
// This differential asserts the string-name form decodes to the SAME request
// as the integer form on every signal — traces (`kind` + `status.code`), logs
// (`severityNumber`), metrics (all three `aggregationTemporality` sites: Sum,
// Histogram, ExponentialHistogram) — and that the decoded enum is the expected
// NON-ZERO value (guards against both forms silently decoding to 0). Payloads
// carry no non-finite doubles, so the derived `PartialEq` on the request is a
// sound comparator here. Replaces the #76 rejection test (now lifted).
// -------------------------------------------------------------------------

#[test]
fn string_enum_name_decodes_identically_to_integer_form() {
    // Traces: `kind` (SpanKind) + `status.code` (StatusCode).
    let traces = |kind: &str, code: &str| {
        format!(
            r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":[{{
                "traceId":"4bf92f3577b34da6a3ce929d0e0e4736",
                "spanId":"00f067aa0ba902b7","name":"s","kind":{kind},
                "startTimeUnixNano":"1700000000000000000",
                "endTimeUnixNano":"1700000001000000000",
                "status":{{"code":{code}}}
            }}]}}]}}]}}"#
        )
    };
    let by_name =
        otlp_traces::decode_json(traces("\"SPAN_KIND_SERVER\"", "\"STATUS_CODE_OK\"").as_bytes())
            .expect("traces name form decodes");
    let by_int =
        otlp_traces::decode_json(traces("2", "1").as_bytes()).expect("traces integer form decodes");
    assert_eq!(
        by_name, by_int,
        "trace enum name form must decode like the integer form"
    );
    let span = &by_name.resource_spans[0].scope_spans[0].spans[0];
    assert_eq!(span.kind, 2, "SPAN_KIND_SERVER");
    assert_eq!(
        span.status.as_ref().expect("status present").code,
        1,
        "STATUS_CODE_OK"
    );

    // Logs: `severityNumber` (SeverityNumber).
    let logs = |sev: &str| {
        format!(
            r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[{{
                "timeUnixNano":"1700000000000000000","severityNumber":{sev},
                "body":{{"stringValue":"hi"}}
            }}]}}]}}]}}"#
        )
    };
    let by_name = otlp_logs::decode_json(logs("\"SEVERITY_NUMBER_INFO\"").as_bytes())
        .expect("logs name form");
    let by_int = otlp_logs::decode_json(logs("9").as_bytes()).expect("logs integer form");
    assert_eq!(
        by_name, by_int,
        "log severity name form must decode like the integer form"
    );
    assert_eq!(
        by_name.resource_logs[0].scope_logs[0].log_records[0].severity_number, 9,
        "SEVERITY_NUMBER_INFO"
    );

    // Metrics: all three `aggregationTemporality` sites (Sum, Histogram,
    // ExponentialHistogram) — each a distinct `#[serde(with=...)]` wiring.
    // `aggregationTemporality` lives on the Sum/Histogram/ExponentialHistogram
    // struct itself (not the data points), so no data points are needed to
    // exercise the three deserializers.
    let metrics = |t: &str| {
        format!(
            r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":[
                {{"name":"s","sum":{{"aggregationTemporality":{t}}}}},
                {{"name":"h","histogram":{{"aggregationTemporality":{t}}}}},
                {{"name":"e","exponentialHistogram":{{"aggregationTemporality":{t}}}}}
            ]}}]}}]}}"#
        )
    };
    let by_name =
        otlp_metrics::decode_json(metrics("\"AGGREGATION_TEMPORALITY_CUMULATIVE\"").as_bytes())
            .expect("metrics name form");
    let by_int = otlp_metrics::decode_json(metrics("2").as_bytes()).expect("metrics integer form");
    assert_eq!(
        by_name, by_int,
        "metric aggregationTemporality name form must decode like the integer form"
    );
    let metrics_out = &by_name.resource_metrics[0].scope_metrics[0].metrics;
    for m in metrics_out {
        let temporality = match m.data.as_ref().expect("metric data") {
            metric::Data::Sum(s) => s.aggregation_temporality,
            metric::Data::Histogram(h) => h.aggregation_temporality,
            metric::Data::ExponentialHistogram(e) => e.aggregation_temporality,
            other => panic!("unexpected metric data: {other:?}"),
        };
        assert_eq!(
            temporality, 2,
            "AGGREGATION_TEMPORALITY_CUMULATIVE on {}",
            m.name
        );
    }
}

// -------------------------------------------------------------------------
// Regenerate the committed goldens. `#[ignore]`-gated: run explicitly after
// editing a builder, then review the JSON diff and commit (mirrors
// trace_ingest_fidelity::regenerate_fixtures). Pretty-printed so the goldens
// stay human-reviewable.
// -------------------------------------------------------------------------

#[test]
#[ignore]
fn regenerate_goldens() {
    let dir = goldens_dir();
    std::fs::create_dir_all(&dir).expect("create goldens dir");
    for (name, bytes) in [
        (
            "logs.json",
            serde_json::to_vec_pretty(&logs_request()).unwrap(),
        ),
        (
            "metrics.json",
            serde_json::to_vec_pretty(&metrics_request()).unwrap(),
        ),
        (
            "traces.json",
            serde_json::to_vec_pretty(&traces_request()).unwrap(),
        ),
    ] {
        let mut out = bytes;
        out.push(b'\n');
        std::fs::write(dir.join(name), out).expect("write golden");
    }
}
