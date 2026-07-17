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
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, ExponentialHistogramDataPoint, HistogramDataPoint, NumberDataPoint, SummaryDataPoint,
    exemplar, number_data_point,
};

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
