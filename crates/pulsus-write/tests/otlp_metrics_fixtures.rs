//! Fixture-driven tests for the OTLP metrics receiver (issue #27
//! acceptance criteria): `tests/fixtures/otlp-metrics/*.bin` are
//! `ExportMetricsServiceRequest` protobuf payloads (provenance:
//! `tests/fixtures/otlp-metrics/README.md`, same construction method as
//! `tests/fixtures/README.md`'s OTLP logs fixtures — built programmatically
//! against this crate's own `opentelemetry-proto` dependency, not
//! hand-assembled bytes). Each test here only decodes/parses a fixture and
//! asserts on `pulsus_write::protocols::otlp_metrics::parse`'s output —
//! the fixture stands alone; nothing here re-derives the wire bytes it
//! reads (except `regenerate_fixtures`, gated `#[ignore]`).
//!
//! A separate subdir (`fixtures/otlp-metrics/`, not `fixtures/otlp/`):
//! task-manager resolution (issue #27, open question #4) — avoids clashing
//! with the concurrent logs-fidelity work in `fixtures/otlp/` /
//! `tests/ingest_fidelity.rs`.

use std::path::{Path, PathBuf};

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets;
use opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, DataPointFlags, ExponentialHistogram, ExponentialHistogramDataPoint,
    Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    Sum, Summary, SummaryDataPoint, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;

use pulsus_config::ExpHistogramMode;
use pulsus_model::STALE_NAN_BITS;
use pulsus_write::protocols::otlp_metrics::{decode, parse as parse_with_mode};
use pulsus_write::{LogsIngestError, ParsedMetrics};

/// Test shim (issue #120): these fixtures assert the default `Classic`
/// exp-histogram mode, so a thin `parse` wrapper keeps every call site
/// unchanged after the `parse` signature gained a `mode` argument.
fn parse(req: &ExportMetricsServiceRequest, now_ns: i64) -> Result<ParsedMetrics, LogsIngestError> {
    parse_with_mode(req, now_ns, ExpHistogramMode::Classic)
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/otlp-metrics")
}

fn read_fixture(name: &str) -> Vec<u8> {
    std::fs::read(fixtures_dir().join(name))
        .unwrap_or_else(|e| panic!("reading fixture {name}: {e}"))
}

fn kv(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        key_strindex: 0,
    }
}

fn resource(attrs: Vec<KeyValue>) -> Resource {
    Resource {
        attributes: attrs,
        dropped_attributes_count: 0,
        entity_refs: vec![],
    }
}

fn one_metric(resource: Option<Resource>, metric: Metric) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![metric],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn number_dp(time_unix_nano: u64, value: f64, flags: u32) -> NumberDataPoint {
    NumberDataPoint {
        attributes: vec![],
        start_time_unix_nano: 0,
        time_unix_nano,
        exemplars: vec![],
        flags,
        value: Some(number_data_point::Value::AsDouble(value)),
    }
}

// ---------------------------------------------------------------------
// Fixture builders. Shared between `regenerate_fixtures` (which writes
// the committed `.bin` files) and nothing else — every other test in this
// file reads the committed bytes from disk.
// ---------------------------------------------------------------------

fn build_gauge_fixture() -> ExportMetricsServiceRequest {
    let dp = number_dp(1_700_000_000_000_000_000, 0.75, 0);
    let metric = Metric {
        name: "cpu_usage_ratio".to_string(),
        description: "fraction of CPU in use".to_string(),
        unit: "1".to_string(),
        metadata: vec![],
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![dp],
        })),
    };
    one_metric(
        Some(resource(vec![kv(
            "service.name",
            Value::StringValue("checkout".to_string()),
        )])),
        metric,
    )
}

fn build_sum_counter_fixture() -> ExportMetricsServiceRequest {
    let dp = number_dp(1_700_000_000_000_000_000, 42.0, 0);
    let metric = Metric {
        name: "http_requests_total".to_string(),
        description: "total HTTP requests".to_string(),
        unit: "1".to_string(),
        metadata: vec![],
        data: Some(metric::Data::Sum(Sum {
            data_points: vec![dp],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            is_monotonic: true,
        })),
    };
    one_metric(None, metric)
}

fn build_histogram_fixture() -> ExportMetricsServiceRequest {
    // bounds [0.1, 0.5, 1.0], bucket_counts [3, 4, 2, 1] -> cumulative
    // [3, 7, 9, 10]; count = 10 matches the bucket total exactly.
    let dp = HistogramDataPoint {
        attributes: vec![],
        start_time_unix_nano: 0,
        time_unix_nano: 1_700_000_000_000_000_000,
        count: 10,
        sum: Some(6.5),
        bucket_counts: vec![3, 4, 2, 1],
        explicit_bounds: vec![0.1, 0.5, 1.0],
        exemplars: vec![],
        flags: 0,
        min: None,
        max: None,
    };
    let metric = Metric {
        name: "request_duration_seconds".to_string(),
        description: "request latency".to_string(),
        unit: "s".to_string(),
        metadata: vec![],
        data: Some(metric::Data::Histogram(Histogram {
            data_points: vec![dp],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        })),
    };
    one_metric(None, metric)
}

fn build_histogram_count_mismatch_fixture() -> ExportMetricsServiceRequest {
    // bucket_counts sum to 10, but the reported count lies (99) —
    // AC: "reject the whole data point" (architect plan amendment).
    let dp = HistogramDataPoint {
        attributes: vec![],
        start_time_unix_nano: 0,
        time_unix_nano: 1_700_000_000_000_000_000,
        count: 99,
        sum: Some(6.5),
        bucket_counts: vec![3, 4, 2, 1],
        explicit_bounds: vec![0.1, 0.5, 1.0],
        exemplars: vec![],
        flags: 0,
        min: None,
        max: None,
    };
    let metric = Metric {
        name: "request_duration_seconds".to_string(),
        description: String::new(),
        unit: "s".to_string(),
        metadata: vec![],
        data: Some(metric::Data::Histogram(Histogram {
            data_points: vec![dp],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        })),
    };
    one_metric(None, metric)
}

/// Code-review finding 1: bucket counts whose sum overflows `u64` — a
/// payload no legitimate collector would ever produce, but an
/// adversarial/corrupted one could. Rejected the same way as a
/// count-mismatch, never panics or silently under-counts.
fn build_histogram_bucket_overflow_fixture() -> ExportMetricsServiceRequest {
    let dp = HistogramDataPoint {
        attributes: vec![],
        start_time_unix_nano: 0,
        time_unix_nano: 1_700_000_000_000_000_000,
        count: 5,
        sum: None,
        bucket_counts: vec![u64::MAX, 1],
        explicit_bounds: vec![1.0],
        exemplars: vec![],
        flags: 0,
        min: None,
        max: None,
    };
    let metric = Metric {
        name: "latency".to_string(),
        description: String::new(),
        unit: String::new(),
        metadata: vec![],
        data: Some(metric::Data::Histogram(Histogram {
            data_points: vec![dp],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        })),
    };
    one_metric(None, metric)
}

/// Code-review finding 2: a bucketless histogram (legal OTLP shape —
/// `bucket_counts`/`explicit_bounds` both empty, "count and sum are known"
/// only) must still emit `_bucket{le="+Inf"} == _count` unconditionally.
fn build_histogram_bucketless_fixture() -> ExportMetricsServiceRequest {
    let dp = HistogramDataPoint {
        attributes: vec![],
        start_time_unix_nano: 0,
        time_unix_nano: 1_700_000_000_000_000_000,
        count: 5,
        sum: Some(12.5),
        bucket_counts: vec![],
        explicit_bounds: vec![],
        exemplars: vec![],
        flags: 0,
        min: None,
        max: None,
    };
    let metric = Metric {
        name: "latency".to_string(),
        description: String::new(),
        unit: String::new(),
        metadata: vec![],
        data: Some(metric::Data::Histogram(Histogram {
            data_points: vec![dp],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        })),
    };
    one_metric(None, metric)
}

/// Code-review finding 3: an exponential-histogram bucket at an extreme
/// `offset`/index combination — the bound computation must fold to a
/// non-finite (`+Inf`-rendering) value rather than panicking on integer
/// overflow.
fn build_exponential_histogram_extreme_offset_fixture() -> ExportMetricsServiceRequest {
    let dp = ExponentialHistogramDataPoint {
        attributes: vec![],
        start_time_unix_nano: 0,
        time_unix_nano: 1_700_000_000_000_000_000,
        count: 2,
        sum: None,
        scale: -10,
        zero_count: 0,
        positive: Some(Buckets {
            offset: i32::MAX,
            bucket_counts: vec![2],
        }),
        negative: None,
        flags: 0,
        exemplars: vec![],
        min: None,
        max: None,
        zero_threshold: 0.0,
    };
    let metric = Metric {
        name: "payload_size_bytes".to_string(),
        description: String::new(),
        unit: "By".to_string(),
        metadata: vec![],
        data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
            data_points: vec![dp],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        })),
    };
    one_metric(None, metric)
}

fn build_exponential_histogram_fixture() -> ExportMetricsServiceRequest {
    // scale 0 (base = 2): positive offset 0, counts [2, 1] (buckets
    // (1,2]->2 obs, (2,4]->1 obs); negative offset 0, counts [1, 3]
    // (mirrored: <=-1 but >-2 -> 1 obs, <=-2 but >-4 -> 3 obs); zero
    // bucket count 4. Total = 2+1+1+3+4 = 11, matching `count`.
    let dp = ExponentialHistogramDataPoint {
        attributes: vec![],
        start_time_unix_nano: 0,
        time_unix_nano: 1_700_000_000_000_000_000,
        count: 11,
        sum: Some(-2.25),
        scale: 0,
        zero_count: 4,
        positive: Some(Buckets {
            offset: 0,
            bucket_counts: vec![2, 1],
        }),
        negative: Some(Buckets {
            offset: 0,
            bucket_counts: vec![1, 3],
        }),
        flags: 0,
        exemplars: vec![],
        min: None,
        max: None,
        zero_threshold: 0.0,
    };
    let metric = Metric {
        name: "payload_size_bytes".to_string(),
        description: "payload size distribution".to_string(),
        unit: "By".to_string(),
        metadata: vec![],
        data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
            data_points: vec![dp],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        })),
    };
    one_metric(None, metric)
}

fn build_summary_fixture() -> ExportMetricsServiceRequest {
    let dp = SummaryDataPoint {
        attributes: vec![],
        start_time_unix_nano: 0,
        time_unix_nano: 1_700_000_000_000_000_000,
        count: 5,
        sum: 12.5,
        quantile_values: vec![
            ValueAtQuantile {
                quantile: 0.5,
                value: 2.0,
            },
            ValueAtQuantile {
                quantile: 0.99,
                value: 4.5,
            },
        ],
        flags: 0,
    };
    let metric = Metric {
        name: "request_duration_seconds".to_string(),
        description: "request latency (legacy summary export)".to_string(),
        unit: "s".to_string(),
        metadata: vec![],
        data: Some(metric::Data::Summary(Summary {
            data_points: vec![dp],
        })),
    };
    one_metric(None, metric)
}

fn build_delta_temporality_fixture() -> ExportMetricsServiceRequest {
    let dp = number_dp(1_700_000_000_000_000_000, 1.0, 0);
    let metric = Metric {
        name: "requests_total".to_string(),
        description: String::new(),
        unit: String::new(),
        metadata: vec![],
        data: Some(metric::Data::Sum(Sum {
            data_points: vec![dp],
            aggregation_temporality: AggregationTemporality::Delta as i32,
            is_monotonic: true,
        })),
    };
    one_metric(None, metric)
}

fn build_zero_timestamp_fixture() -> ExportMetricsServiceRequest {
    let bad = number_dp(0, 1.0, 0);
    let good = number_dp(1_700_000_000_000_000_000, 2.0, 0);
    let metric = Metric {
        name: "up".to_string(),
        description: String::new(),
        unit: String::new(),
        metadata: vec![],
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![bad, good],
        })),
    };
    one_metric(None, metric)
}

fn build_stale_nan_fixture() -> ExportMetricsServiceRequest {
    let dp = number_dp(
        1_700_000_000_000_000_000,
        1.0,
        DataPointFlags::NoRecordedValueMask as u32,
    );
    let metric = Metric {
        name: "up".to_string(),
        description: String::new(),
        unit: String::new(),
        metadata: vec![],
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![dp],
        })),
    };
    one_metric(None, metric)
}

/// AC: "attribute keys normalized before fingerprinting" — a series has
/// one identity regardless of `service.name` (dotted, OTel transport form)
/// vs `service_name` (already-underscored) wire form. Two fixtures, same
/// logical resource attribute, different key spelling.
fn build_fingerprint_dot_fixture() -> ExportMetricsServiceRequest {
    let dp = number_dp(1_700_000_000_000_000_000, 1.0, 0);
    one_metric(
        Some(resource(vec![kv(
            "service.name",
            Value::StringValue("checkout".to_string()),
        )])),
        Metric {
            name: "up".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![dp],
            })),
        },
    )
}

fn build_fingerprint_underscore_fixture() -> ExportMetricsServiceRequest {
    let dp = number_dp(1_700_000_000_000_000_000, 1.0, 0);
    one_metric(
        Some(resource(vec![kv(
            "service_name",
            Value::StringValue("checkout".to_string()),
        )])),
        Metric {
            name: "up".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![dp],
            })),
        },
    )
}

/// Regenerates every `tests/fixtures/otlp-metrics/*.bin` used by the tests
/// below, plus `malformed.bin` (a deliberately truncated protobuf message).
/// Gated `#[ignore]` — see `tests/fixtures/otlp-metrics/README.md`. Run with
/// `cargo test -p pulsus-write --test otlp_metrics_fixtures -- --ignored
/// regenerate_fixtures` after changing a builder above, then commit the
/// resulting `.bin` diffs.
#[test]
#[ignore = "regenerates the committed fixtures; run explicitly, see doc comment"]
fn regenerate_fixtures() {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).unwrap();

    let write = |name: &str, req: &ExportMetricsServiceRequest| {
        std::fs::write(dir.join(name), req.encode_to_vec()).unwrap();
    };
    write("gauge.bin", &build_gauge_fixture());
    write("sum_counter.bin", &build_sum_counter_fixture());
    write("histogram.bin", &build_histogram_fixture());
    write(
        "histogram_count_mismatch.bin",
        &build_histogram_count_mismatch_fixture(),
    );
    write(
        "histogram_bucket_overflow.bin",
        &build_histogram_bucket_overflow_fixture(),
    );
    write(
        "histogram_bucketless.bin",
        &build_histogram_bucketless_fixture(),
    );
    write(
        "exponential_histogram.bin",
        &build_exponential_histogram_fixture(),
    );
    write(
        "exponential_histogram_extreme_offset.bin",
        &build_exponential_histogram_extreme_offset_fixture(),
    );
    write("summary.bin", &build_summary_fixture());
    write("delta_temporality.bin", &build_delta_temporality_fixture());
    write("zero_timestamp.bin", &build_zero_timestamp_fixture());
    write("stale_nan.bin", &build_stale_nan_fixture());
    write("fingerprint_dot.bin", &build_fingerprint_dot_fixture());
    write(
        "fingerprint_underscore.bin",
        &build_fingerprint_underscore_fixture(),
    );

    // A well-formed field tag/wire-type prefix, cut off mid-value: prost
    // sees a length-delimited field announcing more bytes than are
    // actually present and fails with an unexpected-EOF `DecodeError`.
    let mut truncated = build_gauge_fixture().encode_to_vec();
    truncated.truncate(truncated.len() / 2);
    std::fs::write(dir.join("malformed.bin"), truncated).unwrap();
}

// ---------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------

#[test]
fn gauge_flattens_to_one_sample_named_verbatim() {
    let bytes = read_fixture("gauge.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportMetricsServiceRequest");
    let out = parse(&req, 0).expect("within the expansion budget");

    assert_eq!(out.samples.len(), 1);
    assert_eq!(&*out.samples[0].metric_name, "cpu_usage_ratio");
    assert_eq!(out.samples[0].value, 0.75);
    assert_eq!(out.samples[0].unix_milli, 1_700_000_000_000);
    assert_eq!(out.series[0].labels.get("service_name"), Some("checkout"));
    assert_eq!(out.metadata.len(), 1);
    assert_eq!(out.metadata[0].metric_type, "gauge");
    assert_eq!(out.metadata[0].help, "fraction of CPU in use");
    assert_eq!(out.metadata[0].unit, "1");
}

#[test]
fn sum_monotonic_flattens_to_one_sample_typed_counter() {
    let bytes = read_fixture("sum_counter.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportMetricsServiceRequest");
    let out = parse(&req, 0).expect("within the expansion budget");

    assert_eq!(out.samples.len(), 1);
    assert_eq!(&*out.samples[0].metric_name, "http_requests_total");
    assert_eq!(out.samples[0].value, 42.0);
    assert_eq!(out.metadata[0].metric_type, "counter");
}

#[test]
fn histogram_flattens_to_documented_series_with_exact_le_labels_and_monotonic_buckets() {
    let bytes = read_fixture("histogram.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportMetricsServiceRequest");
    let out = parse(&req, 0).expect("within the expansion budget");

    assert_eq!(out.rejected, 0);
    let mut buckets: Vec<(String, f64)> = out
        .samples
        .iter()
        .filter(|s| &*s.metric_name == "request_duration_seconds_bucket")
        .map(|s| {
            let le = out
                .series
                .iter()
                .find(|r| r.fingerprint == s.fingerprint)
                .unwrap()
                .labels
                .get("le")
                .unwrap()
                .to_string();
            (le, s.value)
        })
        .collect();
    buckets.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    assert_eq!(
        buckets,
        vec![
            ("0.1".to_string(), 3.0),
            ("0.5".to_string(), 7.0),
            ("1".to_string(), 9.0),
            ("+Inf".to_string(), 10.0),
        ]
    );
    // Monotonic cumulative: strictly non-decreasing.
    for pair in buckets.windows(2) {
        assert!(pair[0].1 <= pair[1].1);
    }

    let count = out
        .samples
        .iter()
        .find(|s| &*s.metric_name == "request_duration_seconds_count")
        .unwrap();
    assert_eq!(count.value, 10.0);
    // AC: "+Inf bucket present and equals count".
    assert_eq!(buckets.last().unwrap().1, count.value);

    let sum = out
        .samples
        .iter()
        .find(|s| &*s.metric_name == "request_duration_seconds_sum")
        .unwrap();
    assert_eq!(sum.value, 6.5);

    assert_eq!(out.metadata[0].metric_type, "histogram");
}

#[test]
fn histogram_count_mismatch_rejects_the_data_point_with_no_samples() {
    let bytes = read_fixture("histogram_count_mismatch.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportMetricsServiceRequest");
    let out = parse(&req, 0).expect("within the expansion budget");

    assert_eq!(out.rejected, 1);
    assert!(
        out.rejected_message
            .as_ref()
            .unwrap()
            .contains("request_duration_seconds")
    );
    assert!(out.samples.is_empty());
    // Metadata is still registered (the type is independent of this
    // data point's internal consistency).
    assert_eq!(out.metadata.len(), 1);
}

/// Code-review finding 1: bucket counts summing past `u64::MAX` reject the
/// data point (same family as a reported-count mismatch) rather than
/// panicking or silently wrapping to an under-count.
#[test]
fn histogram_bucket_count_overflow_rejects_without_panicking() {
    let bytes = read_fixture("histogram_bucket_overflow.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportMetricsServiceRequest");
    let out = parse(&req, 0).expect("within the expansion budget");

    assert_eq!(out.rejected, 1);
    assert!(out.samples.is_empty());
    assert!(out.rejected_message.as_ref().unwrap().contains("overflow"));
}

/// Code-review finding 2: a bucketless histogram (legal shape: only
/// `count`/`sum` known) still emits `_bucket{le="+Inf"} == _count`.
#[test]
fn histogram_bucketless_still_emits_inf_bucket_equal_to_count() {
    let bytes = read_fixture("histogram_bucketless.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportMetricsServiceRequest");
    let out = parse(&req, 0).expect("within the expansion budget");

    assert_eq!(out.rejected, 0);
    let bucket = out
        .samples
        .iter()
        .find(|s| &*s.metric_name == "latency_bucket")
        .expect("the +Inf bucket is always emitted, even with no explicit distribution");
    let bucket_series = out
        .series
        .iter()
        .find(|r| r.fingerprint == bucket.fingerprint)
        .unwrap();
    assert_eq!(bucket_series.labels.get("le"), Some("+Inf"));
    let count = out
        .samples
        .iter()
        .find(|s| &*s.metric_name == "latency_count")
        .unwrap();
    assert_eq!(bucket.value, count.value);
    assert_eq!(bucket.value, 5.0);
}

/// Code-review finding 3: an extreme exponential-histogram bucket
/// (`offset = i32::MAX`, coarse negative `scale`) folds its bound to
/// `+Inf` rather than panicking on integer overflow.
#[test]
fn exponential_histogram_extreme_offset_folds_to_inf_without_panicking() {
    let bytes = read_fixture("exponential_histogram_extreme_offset.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportMetricsServiceRequest");
    let out = parse(&req, 0).expect("within the expansion budget");

    assert_eq!(out.rejected, 0);
    let bucket_series_labels: Vec<Option<&str>> = out
        .samples
        .iter()
        .filter(|s| &*s.metric_name == "payload_size_bytes_bucket")
        .map(|s| {
            out.series
                .iter()
                .find(|r| r.fingerprint == s.fingerprint)
                .and_then(|r| r.labels.get("le"))
        })
        .collect();
    // The extreme bucket's bound folded to `+Inf`; since the zero bucket
    // (`le = 0.0`) also renders below it, both collapse into the single
    // final `+Inf` bucket the parser always emits — asserting its
    // presence is the point, not the exact bucket count.
    assert!(bucket_series_labels.contains(&Some("+Inf")));
    let count = out
        .samples
        .iter()
        .find(|s| &*s.metric_name == "payload_size_bytes_count")
        .unwrap();
    assert_eq!(count.value, 2.0);
}

#[test]
fn exponential_histogram_flattens_with_negative_zero_and_positive_buckets_and_inf_equals_count() {
    let bytes = read_fixture("exponential_histogram.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportMetricsServiceRequest");
    let out = parse(&req, 0).expect("within the expansion budget");

    assert_eq!(out.rejected, 0);
    let count = out
        .samples
        .iter()
        .find(|s| &*s.metric_name == "payload_size_bytes_count")
        .unwrap();
    assert_eq!(count.value, 11.0);

    let bucket_samples: Vec<&pulsus_write::MetricPoint> = out
        .samples
        .iter()
        .filter(|s| &*s.metric_name == "payload_size_bytes_bucket")
        .collect();
    assert!(!bucket_samples.is_empty());

    // The `+Inf` bucket is present and equals `_count` (AC).
    let inf_sample = bucket_samples
        .iter()
        .find(|s| {
            out.series
                .iter()
                .any(|r| r.fingerprint == s.fingerprint && r.labels.get("le") == Some("+Inf"))
        })
        .expect("a +Inf bucket is always present");
    assert_eq!(inf_sample.value, count.value);

    // Cumulative buckets are non-decreasing across ascending `le`.
    let mut rendered: Vec<(f64, f64)> = bucket_samples
        .iter()
        .filter_map(|s| {
            let le = out
                .series
                .iter()
                .find(|r| r.fingerprint == s.fingerprint)?
                .labels
                .get("le")?
                .to_string();
            let le_value: f64 = if le == "+Inf" {
                f64::INFINITY
            } else {
                le.parse().ok()?
            };
            Some((le_value, s.value))
        })
        .collect();
    rendered.sort_by(|a, b| a.0.total_cmp(&b.0));
    for pair in rendered.windows(2) {
        assert!(pair[0].1 <= pair[1].1);
    }

    assert_eq!(out.metadata[0].metric_type, "histogram");
}

#[test]
fn summary_flattens_to_quantile_sum_and_count_series() {
    let bytes = read_fixture("summary.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportMetricsServiceRequest");
    let out = parse(&req, 0).expect("within the expansion budget");

    assert_eq!(out.rejected, 0);
    let q = |quantile: &str| {
        out.samples
            .iter()
            .find(|s| {
                &*s.metric_name == "request_duration_seconds"
                    && out.series.iter().any(|r| {
                        r.fingerprint == s.fingerprint && r.labels.get("quantile") == Some(quantile)
                    })
            })
            .unwrap()
    };
    assert_eq!(q("0.5").value, 2.0);
    assert_eq!(q("0.99").value, 4.5);

    let sum = out
        .samples
        .iter()
        .find(|s| &*s.metric_name == "request_duration_seconds_sum")
        .unwrap();
    assert_eq!(sum.value, 12.5);
    let count = out
        .samples
        .iter()
        .find(|s| &*s.metric_name == "request_duration_seconds_count")
        .unwrap();
    assert_eq!(count.value, 5.0);
    assert_eq!(out.metadata[0].metric_type, "summary");
}

#[test]
fn delta_temporality_rejects_the_whole_metric_naming_it() {
    let bytes = read_fixture("delta_temporality.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportMetricsServiceRequest");
    let out = parse(&req, 0).expect("within the expansion budget");

    assert_eq!(out.rejected, 1);
    assert!(
        out.rejected_message
            .as_ref()
            .unwrap()
            .contains("requests_total")
    );
    assert!(out.samples.is_empty());
}

#[test]
fn zero_timestamp_data_point_is_rejected_as_partial_success_the_other_point_still_parses() {
    let bytes = read_fixture("zero_timestamp.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportMetricsServiceRequest");
    let out = parse(&req, 0).expect("within the expansion budget");

    assert_eq!(out.rejected, 1);
    assert_eq!(out.samples.len(), 1);
    assert_eq!(out.samples[0].value, 2.0);
}

#[test]
fn no_recorded_value_flag_emits_the_canonical_stale_nan_bit_pattern() {
    let bytes = read_fixture("stale_nan.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportMetricsServiceRequest");
    let out = parse(&req, 0).expect("within the expansion budget");

    assert_eq!(out.samples.len(), 1);
    // Load-bearing: exact bit pattern via `.to_bits()`, never `is_nan()`
    // (NaN != NaN, and a bare NaN check would not catch a corrupted bit
    // pattern that is still, coincidentally, some NaN).
    assert_eq!(out.samples[0].value.to_bits(), STALE_NAN_BITS);
}

#[test]
fn dotted_and_underscored_service_name_fingerprint_identically() {
    let dot_bytes = read_fixture("fingerprint_dot.bin");
    let underscore_bytes = read_fixture("fingerprint_underscore.bin");
    let dot_req = decode(&dot_bytes).expect("valid request");
    let underscore_req = decode(&underscore_bytes).expect("valid request");

    let dot_out = parse(&dot_req, 0).expect("within the expansion budget");
    let underscore_out = parse(&underscore_req, 0).expect("within the expansion budget");

    assert_eq!(
        dot_out.samples[0].fingerprint, underscore_out.samples[0].fingerprint,
        "a series has one identity regardless of service.name vs service_name transport form"
    );
}

#[test]
fn malformed_protobuf_is_a_whole_request_decode_error() {
    let bytes = read_fixture("malformed.bin");
    let err = decode(&bytes).expect_err("truncated protobuf must not decode");
    assert!(matches!(err, pulsus_write::LogsIngestError::Decode(_)));
}

#[test]
fn parse_is_pure_repeated_calls_on_the_same_fixture_are_identical() {
    let bytes = read_fixture("exponential_histogram.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportMetricsServiceRequest");
    let a = parse(&req, 123).expect("within the expansion budget");
    let b = parse(&req, 123).expect("within the expansion budget");
    assert_eq!(a, b);
}
