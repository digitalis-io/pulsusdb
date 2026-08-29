//! OTLP/JSON float decode must be correctly rounded (issue #270).
//!
//! A decimal literal on the JSON wire has exactly ONE correct `f64`
//! interpretation — the nearest representable value, which is what
//! `str::parse::<f64>()` returns. `serde_json`'s DEFAULT number parser is not
//! correctly rounded: without the `float_roundtrip` feature it can return a
//! neighbouring float — one ULP away on every literal measured for this issue,
//! which is a property of those vectors rather than a proven bound on the
//! parser. All three OTLP/JSON routes decode their
//! bare JSON numbers through that parser, so what we go on to store is derived
//! from a different number than the client serialised — permanently, and
//! without any error being raised.
//!
//! The fix is the workspace-level `serde_json` feature `float_roundtrip`
//! (root `Cargo.toml`); this suite is its gate.
//!
//! # Why the vectors look the way they do
//!
//! Every vector below is a literal where the two parsers **genuinely
//! disagree**. That is the whole point: a fixture built from round numbers
//! (`21.5`, `1234.0`) decodes identically under both parsers and proves
//! nothing. Measured, not assumed: with the feature removed,
//! `otlp_json_equivalence` (the full protobuf-vs-JSON differential),
//! `otlp_json_vendor_patch` and `otlp_metrics_fixtures` all still pass — 8,
//! 30 and 15 tests respectively, 53 that touch this decode and cannot see
//! the defect. Each count is the suite's passing tests; `otlp_json_equivalence`
//! and `otlp_metrics_fixtures` carry one `#[ignore]`d regeneration helper
//! each, which runs in neither configuration.
//!
//! Each vector carries two frozen constants:
//!
//! * `correct` — `lex.parse::<f64>().to_bits()`, the nearest-representable
//!   value. Re-derived at run time by [`assert_vectors_are_self_consistent`],
//!   so a typo cannot silently weaken the assertion.
//! * `naive` — the bits `serde_json` 1.0.150 returns for `lex` WITHOUT
//!   `float_roundtrip`, measured on this repo before the fix. It is never
//!   what a decode may produce; it is recorded so that "these vectors
//!   discriminate" is a checked property (`correct != naive`) rather than a
//!   claim, and so a future reader can reproduce the defect by dropping the
//!   feature.
//!
//! Comparisons are on `to_bits()`, never on rendered text: one float's
//! shortest rendering is frequently a prefix of another's.
//!
//! # Coverage
//!
//! The float leaves reachable from OTLP/JSON were enumerated from the vendored
//! message types, not from memory:
//!
//! ```text
//! grep -nE 'f64' vendor/opentelemetry-proto/src/proto/tonic/opentelemetry.proto.metrics.v1.rs
//! grep -nE 'f64' vendor/opentelemetry-proto/src/proto/tonic/opentelemetry.proto.common.v1.rs
//! ```
//!
//! That is 13 `f64` sites in `metrics.v1` (`NumberDataPoint.asDouble`;
//! `HistogramDataPoint` `sum`/`min`/`max`/`explicitBounds`;
//! `ExponentialHistogramDataPoint` `sum`/`min`/`max`/`zeroThreshold`;
//! `SummaryDataPoint.sum`; `ValueAtQuantile` `quantile`/`value`;
//! `Exemplar.asDouble`) and one in `common.v1`
//! (`AnyValue.doubleValue`). Each gets a discriminating literal below.
//! `logs.v1` and `trace.v1` contain no `f64`/`f32` field at all, so the logs
//! and traces routes reach a double only through `AnyValue`.

use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueEnum;
use opentelemetry_proto::tonic::metrics::v1::{metric, number_data_point};
use pulsus_write::protocols::otlp_metrics::MetricIngestSettings;
use pulsus_write::protocols::{otlp_logs, otlp_metrics, otlp_traces};

/// A decimal literal on which the correctly-rounded parser and `serde_json`'s
/// default (non-`float_roundtrip`) parser return different `f64`s.
#[derive(Debug, Clone, Copy)]
struct Vector {
    /// The literal exactly as it appears in the JSON body.
    lex: &'static str,
    /// Bits of the nearest representable `f64` (`str::parse::<f64>()`).
    correct: u64,
    /// Bits `serde_json` 1.0.150 returns without `float_roundtrip`.
    naive: u64,
}

/// Two literals per decade across the range ordinary observability values
/// live in, one erring high and one erring low, plus one negative decade and
/// the classic normal/subnormal boundary literal (where the default parser
/// crosses from the largest subnormal to the smallest normal — a 1-ULP error
/// that also changes the exponent field).
///
/// Provenance: generated in a scratch crate by rendering `f64`s with Rust's
/// shortest round-trip `{:?}` and keeping those where `str::parse` and
/// `serde_json::from_str` disagree, against `serde_json` 1.0.150 with
/// `default-features = false, features = ["std", "raw_value"]`.
///
/// That is not the feature set this workspace resolved before the fix.
/// Measured on this commit with the manifest line put back to
/// `serde_json = "1"`, `cargo tree -e features -i serde_json --workspace`
/// reports `default`, `raw_value` and `std`. The two differ only by the name
/// `default`, which 1.0.150 declares as `default = ["std"]`
/// (`serde_json-1.0.150/Cargo.toml:68`) and never reads as a `cfg` in its
/// own source, so both builds compile the same `de.rs`: `float_roundtrip`
/// off, and the other number-affecting feature `arbitrary_precision` off.
const VECTORS: &[Vector] = &[
    Vector {
        lex: "0.0018322491389592419",
        correct: 0x3f5e_0502_8851_2b04,
        naive: 0x3f5e_0502_8851_2b05,
    },
    Vector {
        lex: "0.0011928087610940433",
        correct: 0x3f53_8b00_a7a2_4d96,
        naive: 0x3f53_8b00_a7a2_4d95,
    },
    Vector {
        lex: "1.2120550590194719",
        correct: 0x3ff3_6493_d877_0a2a,
        naive: 0x3ff3_6493_d877_0a2b,
    },
    Vector {
        lex: "1.9816883557688978",
        correct: 0x3fff_b4fe_d96e_434b,
        naive: 0x3fff_b4fe_d96e_434a,
    },
    Vector {
        lex: "-1774.1730603736187",
        correct: 0xc09b_b8b1_36bd_13b4,
        naive: 0xc09b_b8b1_36bd_13b5,
    },
    Vector {
        lex: "-1359.8582046894405",
        correct: 0xc095_3f6e_cd35_c9af,
        naive: 0xc095_3f6e_cd35_c9ae,
    },
    Vector {
        lex: "1040930.8800823967",
        correct: 0x412f_c445_c29a_28ef,
        naive: 0x412f_c445_c29a_28f0,
    },
    Vector {
        lex: "1798120.4400873021",
        correct: 0x413b_6fe8_70a9_8fba,
        naive: 0x413b_6fe8_70a9_8fb9,
    },
    Vector {
        lex: "1066074736.6241531",
        correct: 0x41cf_c581_384f_e440,
        naive: 0x41cf_c581_384f_e441,
    },
    Vector {
        lex: "1883265621.1407897",
        correct: 0x41dc_1016_9549_02b3,
        naive: 0x41dc_1016_9549_02b2,
    },
    // Largest subnormal vs smallest normal: the default parser returns
    // 0x0010_0000_0000_0000, one ULP up and across the boundary.
    Vector {
        lex: "2.2250738585072011e-308",
        correct: 0x000f_ffff_ffff_ffff,
        naive: 0x0010_0000_0000_0000,
    },
];

/// Indices into [`VECTORS`] whose value lies in `[0, 1]`, so they can stand in
/// for a `ValueAtQuantile.quantile` without the fixture becoming nonsense.
const IN_UNIT_RANGE: &[usize] = &[0, 1];

/// The table is only worth anything if each row's `correct` really is the
/// nearest-representable value and really differs from `naive`. Both are
/// checked here rather than trusted, so a mistyped constant fails loudly
/// instead of turning a discriminating vector into a vacuous one.
#[test]
fn assert_vectors_are_self_consistent() {
    assert!(!VECTORS.is_empty());
    for v in VECTORS {
        let parsed: f64 = v.lex.parse().expect("vector literal parses as f64");
        assert_eq!(
            parsed.to_bits(),
            v.correct,
            "vector {}: `correct` is not str::parse's result",
            v.lex
        );
        assert_ne!(
            v.correct, v.naive,
            "vector {} does not discriminate the two parsers, so it gates nothing",
            v.lex
        );
        assert_eq!(
            v.correct.abs_diff(v.naive),
            1,
            "vector {}: the recorded default-parser result should be exactly 1 ULP away",
            v.lex
        );
    }
}

// ---------------------------------------------------------------------------
// Bodies
// ---------------------------------------------------------------------------

/// One `/v1/metrics` gauge data point per vector, each `asDouble` written as
/// the bare JSON number the client would have serialised.
fn gauge_body() -> Vec<u8> {
    let points: Vec<String> = VECTORS
        .iter()
        .enumerate()
        .map(|(i, v)| {
            format!(
                r#"{{"timeUnixNano":"{}","asDouble":{}}}"#,
                1_700_000_000_000_000_001u64 + i as u64,
                v.lex
            )
        })
        .collect();
    format!(
        r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":[
             {{"name":"g","gauge":{{"dataPoints":[{}]}}}}
           ]}}]}}]}}"#,
        points.join(",")
    )
    .into_bytes()
}

/// A histogram data point exercising the `Option<f64>` (`sum`/`min`/`max`),
/// `Vec<f64>` (`explicitBounds`) and exemplar `asDouble` decode adapters —
/// each of which goes through the vendored ADR-0004 `f64_special*` wrappers
/// rather than the plain derive, and so needs its own vector.
fn histogram_body() -> Vec<u8> {
    let bounds: Vec<&str> = VECTORS.iter().map(|v| v.lex).collect();
    let counts: Vec<&str> = std::iter::repeat_n("1", VECTORS.len() + 1).collect();
    format!(
        r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":[
             {{"name":"h","histogram":{{"aggregationTemporality":2,"dataPoints":[
               {{"timeUnixNano":"1700000000000000001","count":"{count}",
                 "sum":{sum},"min":{min},"max":{max},
                 "bucketCounts":[{counts}],"explicitBounds":[{bounds}],
                 "exemplars":[{{"timeUnixNano":"1700000000000000001","asDouble":{ex}}}]}}
             ]}}}}
           ]}}]}}]}}"#,
        count = VECTORS.len() + 1,
        sum = VECTORS[0].lex,
        min = VECTORS[1].lex,
        max = VECTORS[2].lex,
        ex = VECTORS[3].lex,
        counts = counts.join(","),
        bounds = bounds.join(","),
    )
    .into_bytes()
}

/// A summary data point: `sum` plus each quantile's `quantile`/`value` pair.
/// `quantile` gets a discriminating literal too — [`IN_UNIT_RANGE`], the two
/// vectors that are plausible quantiles — rather than a round `0.5`, which
/// would leave that leaf ungated.
fn summary_body() -> Vec<u8> {
    let quantiles: Vec<String> = VECTORS
        .iter()
        .enumerate()
        .map(|(i, v)| {
            format!(
                r#"{{"quantile":{},"value":{}}}"#,
                VECTORS[IN_UNIT_RANGE[i % IN_UNIT_RANGE.len()]].lex,
                v.lex
            )
        })
        .collect();
    format!(
        r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":[
             {{"name":"s","summary":{{"dataPoints":[
               {{"timeUnixNano":"1700000000000000001","count":"1","sum":{sum},
                 "quantileValues":[{qs}]}}
             ]}}}}
           ]}}]}}]}}"#,
        sum = VECTORS[0].lex,
        qs = quantiles.join(","),
    )
    .into_bytes()
}

/// An exponential histogram: `zeroThreshold` is a bare `f64` leaf alongside
/// the `Option<f64>` `sum`/`min`/`max`.
fn exp_histogram_body() -> Vec<u8> {
    format!(
        r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":[
             {{"name":"e","exponentialHistogram":{{"aggregationTemporality":2,"dataPoints":[
               {{"timeUnixNano":"1700000000000000001","count":"2","scale":0,
                 "zeroCount":"0","zeroThreshold":{zt},"sum":{sum},"min":{min},"max":{max},
                 "positive":{{"offset":0,"bucketCounts":["1","1"]}}}}
             ]}}}}
           ]}}]}}]}}"#,
        zt = VECTORS[0].lex,
        sum = VECTORS[1].lex,
        min = VECTORS[2].lex,
        max = VECTORS[3].lex,
    )
    .into_bytes()
}

/// `AnyValue.doubleValue` on a log record attribute — reached through the
/// vendored P2 oneof visitor rather than a field-level adapter. It is the
/// logs and traces routes' only float-bearing leaf: the vendored
/// `opentelemetry.proto.logs.v1.rs` and `opentelemetry.proto.trace.v1.rs`
/// contain no `f64`/`f32` field at all, so every double on those two routes
/// arrives inside an `AnyValue` from `common.v1`.
fn logs_body() -> Vec<u8> {
    let attrs: Vec<String> = VECTORS
        .iter()
        .enumerate()
        .map(|(i, v)| format!(r#"{{"key":"a{}","value":{{"doubleValue":{}}}}}"#, i, v.lex))
        .collect();
    format!(
        r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[
             {{"timeUnixNano":"1700000000000000001","body":{{"stringValue":"x"}},
               "attributes":[{}]}}
           ]}}]}}]}}"#,
        attrs.join(",")
    )
    .into_bytes()
}

/// `AnyValue.doubleValue` on a span attribute — the traces route's analog.
fn traces_body() -> Vec<u8> {
    let attrs: Vec<String> = VECTORS
        .iter()
        .enumerate()
        .map(|(i, v)| format!(r#"{{"key":"a{}","value":{{"doubleValue":{}}}}}"#, i, v.lex))
        .collect();
    format!(
        r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":[
             {{"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0102030405060708",
               "name":"sp","kind":1,
               "startTimeUnixNano":"1700000000000000001","endTimeUnixNano":"1700000000000000002",
               "attributes":[{}]}}
           ]}}]}}]}}"#,
        attrs.join(",")
    )
    .into_bytes()
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

#[track_caller]
fn assert_bits(what: &str, lex: &str, got: f64, want: u64) {
    assert_eq!(
        got.to_bits(),
        want,
        "{what}: literal {lex} decoded to 0x{:016x}, want the nearest representable 0x{want:016x}",
        got.to_bits()
    );
}

fn any_double(v: &opentelemetry_proto::tonic::common::v1::AnyValue) -> f64 {
    match v.value.as_ref().expect("attribute carries a value") {
        AnyValueEnum::DoubleValue(d) => *d,
        other => panic!("expected doubleValue, got {other:?}"),
    }
}

#[test]
fn gauge_as_double_decodes_to_the_nearest_representable_f64() {
    let req = otlp_metrics::decode_json(&gauge_body()).expect("gauge body decodes");
    let points = match req.resource_metrics[0].scope_metrics[0].metrics[0]
        .data
        .as_ref()
        .expect("metric carries data")
    {
        metric::Data::Gauge(g) => &g.data_points,
        other => panic!("expected a gauge, got {other:?}"),
    };
    assert_eq!(points.len(), VECTORS.len());
    for (v, p) in VECTORS.iter().zip(points) {
        let got = match p.value.as_ref().expect("data point carries a value") {
            number_data_point::Value::AsDouble(d) => *d,
            other => panic!("expected asDouble, got {other:?}"),
        };
        assert_bits("NumberDataPoint.asDouble", v.lex, got, v.correct);
    }
}

/// The decoded value must survive `otlp_metrics::parse` unchanged — the step
/// between the wire and the `metric_samples` row the writer inserts. Bits
/// again: `MetricPoint::value` is the `f64` that reaches ClickHouse.
#[test]
fn gauge_as_double_reaches_metric_point_value_unchanged() {
    let req = otlp_metrics::decode_json(&gauge_body()).expect("gauge body decodes");
    let parsed = otlp_metrics::parse(
        &req,
        1_700_000_000_000_000_001,
        MetricIngestSettings::default(),
    )
    .expect("gauge body parses");
    assert_eq!(parsed.rejected, 0, "no data point may be dropped");
    assert_eq!(parsed.samples.len(), VECTORS.len());
    for (v, s) in VECTORS.iter().zip(&parsed.samples) {
        assert_bits("MetricPoint::value", v.lex, s.value, v.correct);
    }
}

#[test]
fn histogram_float_leaves_decode_to_the_nearest_representable_f64() {
    let req = otlp_metrics::decode_json(&histogram_body()).expect("histogram body decodes");
    let dp = match req.resource_metrics[0].scope_metrics[0].metrics[0]
        .data
        .as_ref()
        .expect("metric carries data")
    {
        metric::Data::Histogram(h) => &h.data_points[0],
        other => panic!("expected a histogram, got {other:?}"),
    };

    assert_bits(
        "HistogramDataPoint.sum",
        VECTORS[0].lex,
        dp.sum.expect("sum present"),
        VECTORS[0].correct,
    );
    assert_bits(
        "HistogramDataPoint.min",
        VECTORS[1].lex,
        dp.min.expect("min present"),
        VECTORS[1].correct,
    );
    assert_bits(
        "HistogramDataPoint.max",
        VECTORS[2].lex,
        dp.max.expect("max present"),
        VECTORS[2].correct,
    );

    let ex = match dp.exemplars[0].value.as_ref().expect("exemplar value") {
        opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsDouble(d) => *d,
        other => panic!("expected exemplar asDouble, got {other:?}"),
    };
    assert_bits("Exemplar.asDouble", VECTORS[3].lex, ex, VECTORS[3].correct);

    assert_eq!(dp.explicit_bounds.len(), VECTORS.len());
    for (v, b) in VECTORS.iter().zip(&dp.explicit_bounds) {
        assert_bits("HistogramDataPoint.explicitBounds", v.lex, *b, v.correct);
    }
}

#[test]
fn summary_float_leaves_decode_to_the_nearest_representable_f64() {
    let req = otlp_metrics::decode_json(&summary_body()).expect("summary body decodes");
    let dp = match req.resource_metrics[0].scope_metrics[0].metrics[0]
        .data
        .as_ref()
        .expect("metric carries data")
    {
        metric::Data::Summary(s) => &s.data_points[0],
        other => panic!("expected a summary, got {other:?}"),
    };
    assert_bits(
        "SummaryDataPoint.sum",
        VECTORS[0].lex,
        dp.sum,
        VECTORS[0].correct,
    );
    assert_eq!(dp.quantile_values.len(), VECTORS.len());
    for (i, (v, q)) in VECTORS.iter().zip(&dp.quantile_values).enumerate() {
        assert_bits("ValueAtQuantile.value", v.lex, q.value, v.correct);
        let qv = &VECTORS[IN_UNIT_RANGE[i % IN_UNIT_RANGE.len()]];
        assert_bits("ValueAtQuantile.quantile", qv.lex, q.quantile, qv.correct);
    }
}

#[test]
fn exponential_histogram_float_leaves_decode_to_the_nearest_representable_f64() {
    let req = otlp_metrics::decode_json(&exp_histogram_body()).expect("exp histogram decodes");
    let dp = match req.resource_metrics[0].scope_metrics[0].metrics[0]
        .data
        .as_ref()
        .expect("metric carries data")
    {
        metric::Data::ExponentialHistogram(h) => &h.data_points[0],
        other => panic!("expected an exponential histogram, got {other:?}"),
    };
    assert_bits(
        "ExponentialHistogramDataPoint.zeroThreshold",
        VECTORS[0].lex,
        dp.zero_threshold,
        VECTORS[0].correct,
    );
    assert_bits(
        "ExponentialHistogramDataPoint.sum",
        VECTORS[1].lex,
        dp.sum.expect("sum present"),
        VECTORS[1].correct,
    );
    assert_bits(
        "ExponentialHistogramDataPoint.min",
        VECTORS[2].lex,
        dp.min.expect("min present"),
        VECTORS[2].correct,
    );
    assert_bits(
        "ExponentialHistogramDataPoint.max",
        VECTORS[3].lex,
        dp.max.expect("max present"),
        VECTORS[3].correct,
    );
}

#[test]
fn log_attribute_double_value_decodes_to_the_nearest_representable_f64() {
    let req = otlp_logs::decode_json(&logs_body()).expect("logs body decodes");
    let attrs = &req.resource_logs[0].scope_logs[0].log_records[0].attributes;
    assert_eq!(attrs.len(), VECTORS.len());
    for (v, kv) in VECTORS.iter().zip(attrs) {
        let got = any_double(kv.value.as_ref().expect("attribute has a value"));
        assert_bits("LogRecord attribute doubleValue", v.lex, got, v.correct);
    }
}

#[test]
fn span_attribute_double_value_decodes_to_the_nearest_representable_f64() {
    let req = otlp_traces::decode_json(&traces_body()).expect("traces body decodes");
    let attrs = &req.resource_spans[0].scope_spans[0].spans[0].attributes;
    assert_eq!(attrs.len(), VECTORS.len());
    for (v, kv) in VECTORS.iter().zip(attrs) {
        let got = any_double(kv.value.as_ref().expect("attribute has a value"));
        assert_bits("Span attribute doubleValue", v.lex, got, v.correct);
    }
}

/// A quoted numeric string reaches `str::parse::<f64>()` inside the vendored
/// ADR-0004 special-double visitor (`proto.rs`'s `deserialize_f64_special`
/// `visit_str`) rather than `serde_json`'s number parser, so it was already
/// correctly rounded — this test and
/// [`assert_vectors_are_self_consistent`] are the only two here that passed
/// BEFORE the fix, which is what distinguishes the quoted path from the bare
/// one. Pinned so the two spellings of the same value stay indistinguishable.
#[test]
fn quoted_and_bare_numeric_spellings_decode_identically() {
    for v in VECTORS {
        let body = format!(
            r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":[
                 {{"name":"h","histogram":{{"aggregationTemporality":2,"dataPoints":[
                   {{"timeUnixNano":"1700000000000000001","count":"1","sum":"{}",
                     "bucketCounts":["1"],"explicitBounds":[]}}
                 ]}}}}
               ]}}]}}]}}"#,
            v.lex
        );
        let req = otlp_metrics::decode_json(body.as_bytes()).expect("quoted sum decodes");
        let dp = match req.resource_metrics[0].scope_metrics[0].metrics[0]
            .data
            .as_ref()
            .expect("metric carries data")
        {
            metric::Data::Histogram(h) => &h.data_points[0],
            other => panic!("expected a histogram, got {other:?}"),
        };
        assert_bits(
            "HistogramDataPoint.sum (quoted)",
            v.lex,
            dp.sum.expect("sum present"),
            v.correct,
        );
    }
}
