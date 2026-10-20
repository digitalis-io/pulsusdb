//! Behavior gate for the vendored+patched `opentelemetry-proto` (issue #76,
//! docs/decisions/0004). The crate is consumed via `[patch.crates-io]`; the
//! patch is a set of additive `#[serde(...)]` annotations that (a) wire the
//! crate's own `serialize_f64_special`/`deserialize_f64_special` (protojson
//! `"NaN"`/`"Infinity"`/`"-Infinity"`) to every double-bearing OTLP field and
//! (b) add the `#[serde(flatten)]` upstream forgot on `Exemplar.value`.
//!
//! These tests assert each patched *behavior* directly against the vendored
//! serde impls, so a future re-vendor that drops the patch (or an upstream bump
//! that reshapes these fields) fails loudly here — this file is the sole guard
//! that the patch survives a re-vendor (there is no source-hash gate; see
//! PATCHES.md's re-vendor rule). Everything here is hermetic.

use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, NumberDataPoint, Sum, SummaryDataPoint, exemplar,
    exponential_histogram_data_point::Buckets, metric, number_data_point,
};
use opentelemetry_proto::tonic::trace::v1::{Span, Status};
use prost::Message as _;

/// Bit-exact equality that treats any two NaNs as equal (the #33/#65
/// precedent): `NaN != NaN` under `==`, so a raw comparison would spuriously
/// fail a non-finite round-trip. Every other value (incl. `+0.0`/`-0.0`,
/// `+Inf`/`-Inf`) compares by exact bit pattern.
fn f64_bit_eq_nan(a: f64, b: f64) -> bool {
    (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits()
}

/// Round-trip a value through the vendored serde impls: serialize to protojson
/// bytes, then deserialize back. A dropped patch makes emit produce `null` or a
/// nested shape, so deserialize either fails or loses the value — caught below.
fn round_trip<T>(value: &T) -> T
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let json = serde_json::to_vec(value).expect("serialize");
    serde_json::from_slice(&json).expect("deserialize")
}

fn to_json_string<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("serialize")
}

// ---- Test 1: Exemplar.value gained `#[serde(flatten)]` -----------------------

#[test]
fn exemplar_as_double_serializes_flat_not_nested() {
    let exemplar = Exemplar {
        value: Some(exemplar::Value::AsDouble(1.5)),
        ..Default::default()
    };
    let json = serde_json::to_value(&exemplar).expect("serialize");
    let object = json.as_object().expect("exemplar serializes to an object");

    // The oneof must be flattened onto the exemplar object, exactly like
    // NumberDataPoint: `"asDouble"` at top level, and NO wrapping `"value"` key.
    assert!(
        object.contains_key("asDouble"),
        "expected flat `asDouble` key, got: {json}"
    );
    assert!(
        !object.contains_key("value"),
        "expected no nested `value` wrapper (flatten missing?), got: {json}"
    );
    assert_eq!(object.get("asDouble").and_then(|v| v.as_f64()), Some(1.5));
}

// ---- Test 2: exact protojson special strings on emit ------------------------

#[test]
fn non_finite_doubles_emit_exact_protojson_strings() {
    // NumberDataPoint.asDouble covers the oneof-variant patch; the raw JSON
    // must carry the exact spec strings, never `null`, never a bare number.
    let nan = NumberDataPoint {
        value: Some(number_data_point::Value::AsDouble(f64::NAN)),
        ..Default::default()
    };
    assert!(
        to_json_string(&nan).contains(r#""asDouble":"NaN""#),
        "NaN must emit the string \"NaN\": {}",
        to_json_string(&nan)
    );

    let pos_inf = NumberDataPoint {
        value: Some(number_data_point::Value::AsDouble(f64::INFINITY)),
        ..Default::default()
    };
    assert!(
        to_json_string(&pos_inf).contains(r#""asDouble":"Infinity""#),
        "+Inf must emit \"Infinity\": {}",
        to_json_string(&pos_inf)
    );

    let neg_inf = NumberDataPoint {
        value: Some(number_data_point::Value::AsDouble(f64::NEG_INFINITY)),
        ..Default::default()
    };
    assert!(
        to_json_string(&neg_inf).contains(r#""asDouble":"-Infinity""#),
        "-Inf must emit \"-Infinity\": {}",
        to_json_string(&neg_inf)
    );

    // And it must never regress to serde_json's lossy `null`.
    assert!(
        !to_json_string(&nan).contains("null"),
        "NaN must not serialize to null: {}",
        to_json_string(&nan)
    );

    // The AnyValue visitor path (shared by logs/traces attributes) too.
    let any = AnyValue {
        value: Some(any_value::Value::DoubleValue(f64::NEG_INFINITY)),
    };
    assert!(
        to_json_string(&any).contains(r#""doubleValue":"-Infinity""#),
        "AnyValue.doubleValue -Inf must emit \"-Infinity\": {}",
        to_json_string(&any)
    );
}

// ---- Test 3: per-field non-finite round-trip, every patched double path ------

#[test]
fn number_data_point_as_double_round_trips_non_finite() {
    let x = NumberDataPoint {
        value: Some(number_data_point::Value::AsDouble(f64::NAN)),
        ..Default::default()
    };
    let back = round_trip(&x);
    match back.value {
        Some(number_data_point::Value::AsDouble(v)) => assert!(v.is_nan()),
        other => panic!("expected AsDouble(NaN), got {other:?}"),
    }
}

#[test]
fn histogram_data_point_double_fields_round_trip_non_finite() {
    let x = HistogramDataPoint {
        sum: Some(f64::INFINITY),
        min: Some(f64::NEG_INFINITY),
        max: Some(f64::NAN),
        explicit_bounds: vec![1.0, f64::INFINITY],
        ..Default::default()
    };
    let back = round_trip(&x);
    assert!(f64_bit_eq_nan(back.sum.unwrap(), f64::INFINITY));
    assert!(f64_bit_eq_nan(back.min.unwrap(), f64::NEG_INFINITY));
    assert!(f64_bit_eq_nan(back.max.unwrap(), f64::NAN));
    assert_eq!(back.explicit_bounds.len(), 2);
    assert!(f64_bit_eq_nan(back.explicit_bounds[0], 1.0));
    assert!(f64_bit_eq_nan(back.explicit_bounds[1], f64::INFINITY));
}

#[test]
fn exponential_histogram_data_point_double_fields_round_trip_non_finite() {
    let x = ExponentialHistogramDataPoint {
        sum: Some(f64::NAN),
        min: Some(f64::NEG_INFINITY),
        max: Some(f64::INFINITY),
        zero_threshold: f64::INFINITY,
        ..Default::default()
    };
    let back = round_trip(&x);
    assert!(f64_bit_eq_nan(back.sum.unwrap(), f64::NAN));
    assert!(f64_bit_eq_nan(back.min.unwrap(), f64::NEG_INFINITY));
    assert!(f64_bit_eq_nan(back.max.unwrap(), f64::INFINITY));
    assert!(f64_bit_eq_nan(back.zero_threshold, f64::INFINITY));
}

#[test]
fn summary_data_point_sum_round_trips_non_finite() {
    let x = SummaryDataPoint {
        sum: f64::NAN,
        ..Default::default()
    };
    let back = round_trip(&x);
    assert!(f64_bit_eq_nan(back.sum, f64::NAN));
}

#[test]
fn exemplar_as_double_round_trips_non_finite() {
    let x = Exemplar {
        value: Some(exemplar::Value::AsDouble(f64::INFINITY)),
        ..Default::default()
    };
    let back = round_trip(&x);
    match back.value {
        Some(exemplar::Value::AsDouble(v)) => assert!(f64_bit_eq_nan(v, f64::INFINITY)),
        other => panic!("expected AsDouble(+Inf), got {other:?}"),
    }
}

#[test]
fn any_value_double_round_trips_non_finite() {
    let x = AnyValue {
        value: Some(any_value::Value::DoubleValue(f64::NAN)),
    };
    let back = round_trip(&x);
    match back.value {
        Some(any_value::Value::DoubleValue(v)) => assert!(v.is_nan()),
        other => panic!("expected DoubleValue(NaN), got {other:?}"),
    }
}

// ---- P5: proto3-JSON enum string-NAME acceptance on OTLP/JSON decode ---------
//
// The vendored types model every `#[prost(enumeration = ...)]` field as a bare
// `i32`, so upstream's derived deserialize accepts only the JSON-number form.
// Patch item P5 adds a `deserialize_with` that accepts BOTH the integer form
// (unchanged) AND the proto NAME string, mapped via prost's `from_str_name`.
// Deserialize-only: serialize stays derived (integer emit), so the wire codec
// and the protojson RESPONSE emit are byte-identical. These tests assert each
// of the 6 enum FIELDS (across 4 enums; all three aggregation_temporality sites
// verified separately since each is a distinct `#[serde(with=...)]` wiring)
// decodes its NAME form and its integer form to the same non-zero value, plus
// the open-enum error posture (unknown name → hard error; unknown int → kept).

#[test]
fn span_kind_decodes_name_and_integer_to_same_value() {
    let by_name: Span = serde_json::from_str(r#"{"kind":"SPAN_KIND_SERVER"}"#).expect("name form");
    let by_int: Span = serde_json::from_str(r#"{"kind":2}"#).expect("integer form");
    assert_eq!(by_name.kind, 2);
    assert_eq!(by_name.kind, by_int.kind);
}

#[test]
fn status_code_decodes_name_and_integer_to_same_value() {
    let by_name: Status = serde_json::from_str(r#"{"code":"STATUS_CODE_OK"}"#).expect("name form");
    let by_int: Status = serde_json::from_str(r#"{"code":1}"#).expect("integer form");
    assert_eq!(by_name.code, 1);
    assert_eq!(by_name.code, by_int.code);
}

#[test]
fn severity_number_decodes_name_and_integer_to_same_value() {
    let by_name: LogRecord =
        serde_json::from_str(r#"{"severityNumber":"SEVERITY_NUMBER_INFO"}"#).expect("name form");
    let by_int: LogRecord = serde_json::from_str(r#"{"severityNumber":9}"#).expect("integer form");
    assert_eq!(by_name.severity_number, 9);
    assert_eq!(by_name.severity_number, by_int.severity_number);
}

#[test]
fn sum_aggregation_temporality_decodes_name_and_integer_to_same_value() {
    let by_name: Sum =
        serde_json::from_str(r#"{"aggregationTemporality":"AGGREGATION_TEMPORALITY_CUMULATIVE"}"#)
            .expect("name form");
    let by_int: Sum =
        serde_json::from_str(r#"{"aggregationTemporality":2}"#).expect("integer form");
    assert_eq!(by_name.aggregation_temporality, 2);
    assert_eq!(
        by_name.aggregation_temporality,
        by_int.aggregation_temporality
    );
}

#[test]
fn histogram_aggregation_temporality_decodes_name_and_integer_to_same_value() {
    let by_name: Histogram =
        serde_json::from_str(r#"{"aggregationTemporality":"AGGREGATION_TEMPORALITY_CUMULATIVE"}"#)
            .expect("name form");
    let by_int: Histogram =
        serde_json::from_str(r#"{"aggregationTemporality":2}"#).expect("integer form");
    assert_eq!(by_name.aggregation_temporality, 2);
    assert_eq!(
        by_name.aggregation_temporality,
        by_int.aggregation_temporality
    );
}

#[test]
fn exponential_histogram_aggregation_temporality_decodes_name_and_integer_to_same_value() {
    let by_name: ExponentialHistogram =
        serde_json::from_str(r#"{"aggregationTemporality":"AGGREGATION_TEMPORALITY_DELTA"}"#)
            .expect("name form");
    let by_int: ExponentialHistogram =
        serde_json::from_str(r#"{"aggregationTemporality":1}"#).expect("integer form");
    assert_eq!(by_name.aggregation_temporality, 1);
    assert_eq!(
        by_name.aggregation_temporality,
        by_int.aggregation_temporality
    );
}

#[test]
fn unknown_enum_name_is_a_loud_error_not_a_silent_zero() {
    let err = serde_json::from_str::<Span>(r#"{"kind":"SPAN_KIND_BOGUS"}"#);
    assert!(err.is_err(), "unknown enum name must be a decode error");
}

#[test]
fn unknown_enum_integer_is_preserved_open_enum() {
    // Proto open-enum semantics: an unknown integer has no name but is a valid
    // value and must survive decode (matching the pre-patch bare-i32 path).
    let span: Span = serde_json::from_str(r#"{"kind":99}"#).expect("unknown int decodes");
    assert_eq!(span.kind, 99);
}

// ---- P6: non-swallowing oneof deserialize + serde(default) parity (#103) ------
//
// Upstream models each `metrics.v1` data oneof as `#[serde(flatten)]
// Option<Enum>`. serde's flatten+Option combo SWALLOWS a present-but-malformed
// inner payload to `None` instead of erroring, so an OTLP/JSON metric with a bad
// field decoded to a silently-DROPPED metric (202, data loss) rather than a 400.
// P6 replaces the swallow with a `deserialize_with` per flatten oneof (Metric.data,
// NumberDataPoint.value, Exemplar.value) that (a) propagates a present-but-malformed
// value as a decode error, (b) rejects >1 oneof member set, and (c) accepts the
// canonical string-encoded `asInt`. Companion: `serde(default)` on the 4 messages
// upstream left without it, so spec-valid SPARSE points still decode. These gates
// fail loudly if a re-vendor drops the patch (swallow returns, or asInt-as-string
// reverts to reject).

/// A fully-populated `ExponentialHistogram` exercising every P6-touched message
/// (`ExponentialHistogramDataPoint`, `Buckets`, `Exemplar`) with finite values,
/// so equality after a round-trip is exact (no NaN handling needed here).
fn full_exponential_histogram() -> ExponentialHistogram {
    ExponentialHistogram {
        data_points: vec![ExponentialHistogramDataPoint {
            attributes: vec![],
            start_time_unix_nano: 1,
            time_unix_nano: 2,
            count: 6,
            sum: Some(3.5),
            scale: 2,
            zero_count: 1,
            positive: Some(Buckets {
                offset: 1,
                bucket_counts: vec![1, 2],
            }),
            negative: Some(Buckets {
                offset: -1,
                bucket_counts: vec![3],
            }),
            flags: 0,
            exemplars: vec![Exemplar {
                filtered_attributes: vec![],
                time_unix_nano: 5,
                span_id: vec![],
                trace_id: vec![],
                value: Some(exemplar::Value::AsInt(7)),
            }],
            min: Some(0.5),
            max: Some(9.0),
            zero_threshold: 0.0,
        }],
        aggregation_temporality: 2,
    }
}

#[test]
fn malformed_metric_data_is_a_decode_error_not_a_swallow() {
    // A bad `count` deep inside the data subtree used to collapse the whole
    // `Metric.data` to `None` (Ok), silently dropping the metric. It must now be
    // a decode error.
    let err = serde_json::from_str::<Metric>(
        r#"{"name":"m","exponentialHistogram":{"dataPoints":[{"count":"nope"}]}}"#,
    );
    assert!(
        err.is_err(),
        "malformed inner field must surface as a decode error, got: {err:?}"
    );
}

#[test]
fn malformed_number_data_point_value_is_a_decode_error() {
    // A malformed scalar `asDouble` used to decode the data point to
    // `value: None` (Ok). It must now error at each of NumberDataPoint and the
    // enclosing Metric.
    let err = serde_json::from_str::<NumberDataPoint>(r#"{"asDouble":{"bad":1}}"#);
    assert!(err.is_err(), "malformed asDouble must error, got: {err:?}");

    let via_metric = serde_json::from_str::<Metric>(
        r#"{"name":"m","gauge":{"dataPoints":[{"asDouble":{"bad":1}}]}}"#,
    );
    assert!(via_metric.is_err(), "must propagate through Metric.data");
}

#[test]
fn malformed_exemplar_value_is_a_decode_error() {
    let err = serde_json::from_str::<Exemplar>(r#"{"asDouble":{"bad":1}}"#);
    assert!(
        err.is_err(),
        "malformed exemplar asDouble must error, got: {err:?}"
    );
}

#[test]
fn malformed_any_value_is_a_decode_error() {
    // `AnyValue.value` is the fourth flatten site (P2's hand-written visitor,
    // not P6's `deserialize_with`). Its `map.next_value()?` propagates a
    // malformed inner scalar rather than swallowing it to `None` — this was
    // previously asserted only in the ADR/PATCHES prose, not tested.
    assert!(serde_json::from_str::<AnyValue>(r#"{"doubleValue":{"bad":1}}"#).is_err());
    assert!(serde_json::from_str::<AnyValue>(r#"{"intValue":{}}"#).is_err());
}

#[test]
fn multiple_metric_data_oneof_members_is_a_decode_error() {
    // Canonical proto3/protojson rejects more than one oneof member set; this
    // used to silently decode to `None` (last-wins/first-wins ambiguity).
    let err = serde_json::from_str::<Metric>(
        r#"{"name":"m","gauge":{"dataPoints":[]},"sum":{"dataPoints":[]}}"#,
    );
    assert!(
        err.is_err(),
        "two data oneof members must be a decode error, got: {err:?}"
    );
}

#[test]
fn multiple_value_oneof_members_is_a_decode_error() {
    let ndp = serde_json::from_str::<NumberDataPoint>(r#"{"asDouble":1.0,"asInt":"2"}"#);
    assert!(ndp.is_err(), "two value members must error, got: {ndp:?}");

    let ex = serde_json::from_str::<Exemplar>(r#"{"asDouble":1.0,"asInt":"2"}"#);
    assert!(ex.is_err(), "two exemplar members must error, got: {ex:?}");
}

#[test]
fn empty_metric_data_oneof_decodes_to_none() {
    // No data key => absent oneof => `data: None`, Ok (unchanged from upstream).
    let m: Metric = serde_json::from_str(r#"{"name":"m"}"#).expect("no data key decodes");
    assert!(m.data.is_none());
}

#[test]
fn number_data_point_accepts_asint_string_and_number() {
    // Canonical proto3/OTLP-JSON string-encodes 64-bit ints incl. asInt; both
    // the string and the number form must decode identically (adjudication #103
    // OVERRIDE — in scope).
    let by_string: NumberDataPoint =
        serde_json::from_str(r#"{"asInt":"42"}"#).expect("string form");
    let by_number: NumberDataPoint = serde_json::from_str(r#"{"asInt":42}"#).expect("number form");
    assert_eq!(by_string.value, Some(number_data_point::Value::AsInt(42)));
    assert_eq!(by_string.value, by_number.value);
}

#[test]
fn exemplar_accepts_asint_string_and_number() {
    let by_string: Exemplar = serde_json::from_str(r#"{"asInt":"42"}"#).expect("string form");
    let by_number: Exemplar = serde_json::from_str(r#"{"asInt":42}"#).expect("number form");
    assert_eq!(by_string.value, Some(exemplar::Value::AsInt(42)));
    assert_eq!(by_string.value, by_number.value);
}

#[test]
fn malformed_asint_is_a_decode_error() {
    // A non-numeric string or a non-scalar must 400, not silently swallow.
    assert!(serde_json::from_str::<NumberDataPoint>(r#"{"asInt":{}}"#).is_err());
    assert!(serde_json::from_str::<NumberDataPoint>(r#"{"asInt":"abc"}"#).is_err());
    assert!(serde_json::from_str::<Exemplar>(r#"{"asInt":{}}"#).is_err());
}

#[test]
fn sparse_exponential_histogram_point_decodes_via_serde_default() {
    // proto3-JSON omits default-valued fields; without P6's `serde(default)` a
    // sparse point 400s on a missing required field. Both an empty object and a
    // single-field object must decode to defaulted structs.
    let empty: ExponentialHistogramDataPoint =
        serde_json::from_str(r#"{}"#).expect("empty sparse point decodes");
    assert_eq!(empty, ExponentialHistogramDataPoint::default());

    let sparse: ExponentialHistogramDataPoint =
        serde_json::from_str(r#"{"count":"3"}"#).expect("sparse point decodes");
    assert_eq!(sparse.count, 3);

    // The three sibling messages that also gained `serde(default)`.
    let _: Buckets = serde_json::from_str(r#"{}"#).expect("sparse Buckets decodes");
    let _: Exemplar = serde_json::from_str(r#"{}"#).expect("sparse Exemplar decodes");
    let _: opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile =
        serde_json::from_str(r#"{}"#).expect("sparse ValueAtQuantile decodes");
}

#[test]
fn full_exponential_histogram_round_trips_byte_identically_through_json() {
    let metric = Metric {
        name: "eh".to_string(),
        data: Some(metric::Data::ExponentialHistogram(
            full_exponential_histogram(),
        )),
        ..Default::default()
    };
    let back = round_trip(&metric);
    assert_eq!(
        metric, back,
        "full exphist metric must survive JSON round-trip"
    );
}

#[test]
fn protobuf_wire_round_trip_is_unaffected() {
    // The P6 patch is deserialize-only + cfg_attr(with-serde)-gated; the prost
    // wire codec must be untouched. This backstops the byte-neutrality argument.
    let metric = Metric {
        name: "eh".to_string(),
        data: Some(metric::Data::ExponentialHistogram(
            full_exponential_histogram(),
        )),
        ..Default::default()
    };
    let bytes = metric.encode_to_vec();
    let decoded = Metric::decode(bytes.as_slice()).expect("wire decode");
    assert_eq!(metric, decoded, "prost wire round-trip must be exact");
}

#[test]
fn valid_number_and_metric_data_still_decode() {
    // No-regression: a valid double gauge point still decodes to Some(variant).
    let ndp: NumberDataPoint = serde_json::from_str(r#"{"asDouble":1.5}"#).expect("valid asDouble");
    assert_eq!(ndp.value, Some(number_data_point::Value::AsDouble(1.5)));

    let m: Metric =
        serde_json::from_str(r#"{"name":"g","gauge":{"dataPoints":[{"asDouble":1.5}]}}"#)
            .expect("valid gauge metric");
    match m.data {
        Some(metric::Data::Gauge(Gauge { data_points })) => {
            assert_eq!(data_points.len(), 1);
            assert_eq!(
                data_points[0].value,
                Some(number_data_point::Value::AsDouble(1.5))
            );
        }
        other => panic!("expected Gauge, got {other:?}"),
    }
}

// ---- Issue #601 part 1: a field omitted at its proto3 default ---------------

use opentelemetry_proto::tonic::common::v1::{ArrayValue, EntityRef, KeyValue, KeyValueList};
use opentelemetry_proto::tonic::resource::v1::Resource;

/// One body per signal carrying `attr` as the one attribute of its one
/// record and `resource` as its one resource, and the decoded record's
/// attributes and resource.
type Decoded = (Vec<KeyValue>, Option<Resource>);

fn decode_on(signal: &str, attr: &str, resource: &str) -> Result<Decoded, String> {
    let attrs = if attr.is_empty() {
        String::new()
    } else {
        format!(r#""attributes":[{attr}]"#)
    };
    match signal {
        "traces" => {
            let body = format!(
                r#"{{"resourceSpans":[{{"resource":{resource},"scopeSpans":[{{"spans":[{{{attrs}}}]}}]}}]}}"#
            );
            pulsus_write::protocols::otlp_traces::decode_json(body.as_bytes())
                .map(|r| {
                    let rs = &r.resource_spans[0];
                    (
                        rs.scope_spans[0].spans[0].attributes.clone(),
                        rs.resource.clone(),
                    )
                })
                .map_err(|e| format!("{e:?}"))
        }
        "logs" => {
            let body = format!(
                r#"{{"resourceLogs":[{{"resource":{resource},"scopeLogs":[{{"logRecords":[{{{attrs}}}]}}]}}]}}"#
            );
            pulsus_write::protocols::otlp_logs::decode_json(body.as_bytes())
                .map(|r| {
                    let rl = &r.resource_logs[0];
                    (
                        rl.scope_logs[0].log_records[0].attributes.clone(),
                        rl.resource.clone(),
                    )
                })
                .map_err(|e| format!("{e:?}"))
        }
        "metrics" => {
            let sep = if attrs.is_empty() { "" } else { "," };
            let body = format!(
                r#"{{"resourceMetrics":[{{"resource":{resource},"scopeMetrics":[{{"metrics":[{{"name":"g","gauge":{{"dataPoints":[{{"asDouble":1{sep}{attrs}}}]}}}}]}}]}}]}}"#
            );
            pulsus_write::protocols::otlp_metrics::decode_json(body.as_bytes())
                .map(|r| {
                    let rm = &r.resource_metrics[0];
                    let attrs = match &rm.scope_metrics[0].metrics[0].data {
                        Some(metric::Data::Gauge(g)) => g.data_points[0].attributes.clone(),
                        other => panic!("a gauge: {other:?}"),
                    };
                    (attrs, rm.resource.clone())
                })
                .map_err(|e| format!("{e:?}"))
        }
        other => unreachable!("{other}"),
    }
}

fn any(value: Option<any_value::Value>) -> Option<AnyValue> {
    Some(AnyValue { value })
}

/// T8: the seven forms proto3-JSON writes for a field at its default each
/// decode, on traces, logs and metrics, to the default; `{}` is a request
/// with no resources; a field given twice is still refused, naming it.
#[test]
fn an_omitted_default_field_decodes_on_every_signal() {
    let kv = |key: &str, value: Option<AnyValue>| KeyValue {
        key: key.to_string(),
        value,
        key_strindex: 0,
    };
    let attr_forms: [(&str, KeyValue); 5] = [
        (
            r#"{"key":"k","value":{"arrayValue":{}}}"#,
            kv(
                "k",
                any(Some(any_value::Value::ArrayValue(ArrayValue {
                    values: vec![],
                }))),
            ),
        ),
        (
            r#"{"key":"k","value":{"kvlistValue":{}}}"#,
            kv(
                "k",
                any(Some(any_value::Value::KvlistValue(KeyValueList {
                    values: vec![],
                }))),
            ),
        ),
        (r#"{"key":"k","value":{}}"#, kv("k", any(None))),
        (r#"{"key":"k","value":{"fooValue":1}}"#, kv("k", any(None))),
        (
            r#"{"value":{"stringValue":"v"}}"#,
            kv(
                "",
                any(Some(any_value::Value::StringValue("v".to_string()))),
            ),
        ),
    ];
    let entity = |schema_url: &str, ty: &str, id: &str| EntityRef {
        schema_url: schema_url.to_string(),
        r#type: ty.to_string(),
        id_keys: vec![id.to_string()],
        description_keys: vec![],
    };
    let ref_forms: [(&str, EntityRef); 2] = [
        (
            r#"{"entityRefs":[{"type":"service","idKeys":["service.name"]}]}"#,
            entity("", "service", "service.name"),
        ),
        (
            r#"{"entityRefs":[{"schemaUrl":"u","idKeys":["a"]}]}"#,
            entity("u", "", "a"),
        ),
    ];
    let mut wrong: Vec<String> = Vec::new();
    for signal in ["traces", "logs", "metrics"] {
        for (attr, want) in &attr_forms {
            match decode_on(signal, attr, "{}") {
                Ok((attrs, _)) if attrs == vec![want.clone()] => {}
                other => wrong.push(format!("{signal} {attr}: {other:?}")),
            }
        }
        for (resource, want) in &ref_forms {
            match decode_on(signal, "", resource) {
                Ok((_, Some(r))) if r.entity_refs == vec![want.clone()] => {}
                other => wrong.push(format!("{signal} {resource}: {other:?}")),
            }
        }
    }
    let empty = [
        pulsus_write::protocols::otlp_traces::decode_json(b"{}")
            .map(|r| r.resource_spans.len())
            .map_err(|e| format!("{e:?}")),
        pulsus_write::protocols::otlp_logs::decode_json(b"{}")
            .map(|r| r.resource_logs.len())
            .map_err(|e| format!("{e:?}")),
        pulsus_write::protocols::otlp_metrics::decode_json(b"{}")
            .map(|r| r.resource_metrics.len())
            .map_err(|e| format!("{e:?}")),
    ];
    for (signal, got) in ["traces", "logs", "metrics"].iter().zip(empty) {
        if got != Ok(0) {
            wrong.push(format!("{signal} {{}}: {got:?}"));
        }
    }
    for signal in ["traces", "logs", "metrics"] {
        for (attr, field) in [
            (
                r#"{"key":"k","value":{"arrayValue":{"values":[],"values":[]}}}"#,
                "`values`",
            ),
            (r#"{"key":"a","key":"b"}"#, "`key`"),
            (
                r#"{"key":"k","value":{"stringValue":"a"},"value":{"stringValue":"b"}}"#,
                "`value`",
            ),
        ] {
            match decode_on(signal, attr, "{}") {
                Err(e) if e.contains(field) => {}
                other => wrong.push(format!(
                    "{signal} {attr}: {other:?}, want an error naming {field}"
                )),
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// T8c: the generated decoder itself, not the bounded one, reads each
/// omitted field as its default — the derives directly.
#[test]
fn the_generated_decoder_reads_an_omitted_field_as_its_default() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::logs::v1::LogsData;
    use opentelemetry_proto::tonic::metrics::v1::MetricsData;
    use opentelemetry_proto::tonic::trace::v1::TracesData;
    fn check<T: serde::de::DeserializeOwned + std::fmt::Debug>(
        wrong: &mut Vec<String>,
        json: &str,
        ok: impl Fn(&T) -> bool,
    ) {
        match serde_json::from_str::<T>(json) {
            Ok(v) if ok(&v) => {}
            other => wrong.push(format!("{} {json}: {other:?}", std::any::type_name::<T>())),
        }
    }
    let mut w: Vec<String> = Vec::new();
    check::<ArrayValue>(&mut w, "{}", |v| v.values.is_empty());
    check::<KeyValueList>(&mut w, "{}", |v| v.values.is_empty());
    check::<AnyValue>(&mut w, "{}", |v| v.value.is_none());
    check::<AnyValue>(&mut w, r#"{"fooValue":1}"#, |v| v.value.is_none());
    check::<AnyValue>(&mut w, r#"{"fooValue":1,"stringValue":"s"}"#, |v| {
        v.value == Some(any_value::Value::StringValue("s".to_string()))
    });
    check::<KeyValue>(&mut w, r#"{"value":{"stringValue":"v"}}"#, |v| {
        v.key.is_empty()
    });
    check::<EntityRef>(&mut w, r#"{"idKeys":["a"]}"#, |v| {
        v.schema_url.is_empty() && v.r#type.is_empty()
    });
    check::<ExportTraceServiceRequest>(&mut w, "{}", |v| v.resource_spans.is_empty());
    check::<ExportLogsServiceRequest>(&mut w, "{}", |v| v.resource_logs.is_empty());
    check::<ExportMetricsServiceRequest>(&mut w, "{}", |v| v.resource_metrics.is_empty());
    check::<TracesData>(&mut w, "{}", |v| v.resource_spans.is_empty());
    check::<LogsData>(&mut w, "{}", |v| v.resource_logs.is_empty());
    check::<MetricsData>(&mut w, "{}", |v| v.resource_metrics.is_empty());
    assert!(w.is_empty(), "{} of 13 refused:\n{}", w.len(), w.join("\n"));
}
