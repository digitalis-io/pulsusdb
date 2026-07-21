//! Cross-transport parity between the OTLP metrics receiver (#27) and the
//! Prometheus remote-write receiver (#28) — issue #28 code review hardening
//! findings (test gaps 2 and 3): the same logical series must fingerprint
//! identically regardless of which transport it arrives over
//! (docs/architecture.md §2.3's "one identity per series"), and
//! `metric_metadata.metric_type` strings must be byte-identical across
//! both parsers (the planner keys counter-function legality off them,
//! docs/schemas.md §2.1). Both assertions call the *actual* `parse`
//! functions from both protocol modules — not a self-referential table
//! check against either module's own internal mapping.

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
    ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint, metric, number_data_point,
    summary_data_point::ValueAtQuantile,
};
use opentelemetry_proto::tonic::resource::v1::Resource;

use pulsus_config::ExpHistogramMode;
use pulsus_write::protocols::otlp_metrics;
use pulsus_write::protocols::remote_write::{
    Label, MetricMetadataProto, Sample, TimeSeries, WriteRequest, parse as rw_parse,
};

fn kv(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        key_strindex: 0,
    }
}

fn otlp_gauge_request(metric_name: &str, host: &str, service: &str) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", Value::StringValue(service.to_string()))],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![kv("host", Value::StringValue(host.to_string()))],
                            start_time_unix_nano: 0,
                            time_unix_nano: 1_700_000_000_000_000_000,
                            exemplars: vec![],
                            flags: 0,
                            value: Some(number_data_point::Value::AsDouble(1.0)),
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn rw_label(name: &str, value: &str) -> Label {
    Label {
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn rw_request(metric_name: &str, host: &str, service: &str) -> WriteRequest {
    WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![
                rw_label("__name__", metric_name),
                rw_label("host", host),
                rw_label("service_name", service),
            ],
            samples: vec![Sample {
                value: 1.0,
                // Deliberately a *different* timestamp than the OTLP
                // fixture (fingerprints must not depend on sample data,
                // only on the series' label identity).
                timestamp: 1_800_000_000_000,
            }],
        }],
        metadata: vec![],
    }
}

/// Test gap 2 (code review): the same logical series (`up{host="node-a",
/// service_name="checkout"}`) pushed via OTLP (resource `service.name`
/// attribute + data point `host` attribute) and via remote-write (`host`/
/// `service_name` labels directly) must resolve to the identical
/// `(metric_name, fingerprint)` — proving both receivers' label
/// normalization + fingerprinting paths converge on one series identity
/// regardless of transport, not just self-consistently within each
/// transport's own test suite.
#[test]
fn same_logical_series_fingerprints_identically_across_otlp_and_remote_write() {
    let otlp_req = otlp_gauge_request("up", "node-a", "checkout");
    let otlp_out = otlp_metrics::parse(&otlp_req, 0, ExpHistogramMode::Classic)
        .expect("within the expansion budget");

    let rw_req = rw_request("up", "node-a", "checkout");
    let rw_out = rw_parse(&rw_req, 0).expect("within the expansion budget");

    assert_eq!(otlp_out.samples.len(), 1);
    assert_eq!(rw_out.samples.len(), 1);
    assert_eq!(&*otlp_out.samples[0].metric_name, "up");
    assert_eq!(&*rw_out.samples[0].metric_name, "up");
    assert_eq!(
        otlp_out.samples[0].fingerprint, rw_out.samples[0].fingerprint,
        "the same logical series must fingerprint identically regardless of transport \
         (docs/architecture.md §2.3)"
    );

    // Also holds at the `SeriesRef` label-set level, not just the derived
    // fingerprint scalar.
    assert_eq!(
        otlp_out.series[0].labels.get("host"),
        rw_out.series[0].labels.get("host")
    );
    assert_eq!(
        otlp_out.series[0].labels.get("service_name"),
        rw_out.series[0].labels.get("service_name")
    );
}

/// The dot-vs-underscore normalization form of the same cross-transport
/// identity: OTLP's `service.name` resource attribute (normalized to
/// `service_name`) must fingerprint identically to remote-write's
/// already-underscored `service_name` label.
#[test]
fn dotted_otlp_attribute_and_underscored_remote_write_label_fingerprint_identically() {
    let otlp_req = otlp_gauge_request("cpu_usage_ratio", "node-b", "billing");
    let otlp_out = otlp_metrics::parse(&otlp_req, 0, ExpHistogramMode::Classic)
        .expect("within the expansion budget");

    let rw_req = rw_request("cpu_usage_ratio", "node-b", "billing");
    let rw_out = rw_parse(&rw_req, 0).expect("within the expansion budget");

    assert_eq!(
        otlp_out.samples[0].fingerprint,
        rw_out.samples[0].fingerprint
    );
}

// -- test gap 3: metric_type string parity ------------------------------

/// A helper to build a single-datapoint OTLP request of a given `Metric`
/// data shape, sharing every fixture builder's plumbing except the `data`
/// oneof.
fn otlp_request_with(data: metric::Data, name: &str) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(data),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn rw_metadata_type_string(prompb_type: i32, name: &str) -> String {
    let req = WriteRequest {
        timeseries: vec![],
        metadata: vec![MetricMetadataProto {
            r#type: prompb_type,
            metric_family_name: name.to_string(),
            help: String::new(),
            unit: String::new(),
        }],
    };
    let out = rw_parse(&req, 0).expect("within the expansion budget");
    out.metadata[0].metric_type.clone()
}

/// Test gap 3 (code review): cross-checks remote-write's `prompb.MetricType`
/// -> string table against the OTLP parser's *actual* emitted
/// `metric_type` strings, for every Prometheus type both transports can
/// produce (gauge/counter/histogram/summary — `gaugehistogram`/`info`/
/// `stateset`/`unknown` have no OTLP data-kind equivalent, so only these
/// four are cross-checkable against a real OTLP-parser output).
#[test]
fn metric_type_strings_match_the_otlp_parsers_actual_output_for_every_shared_type() {
    // gauge
    let gauge_req = otlp_request_with(
        metric::Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                start_time_unix_nano: 0,
                time_unix_nano: 1,
                exemplars: vec![],
                flags: 0,
                value: Some(number_data_point::Value::AsDouble(1.0)),
            }],
        }),
        "a_gauge",
    );
    let otlp_gauge_type = otlp_metrics::parse(&gauge_req, 0, ExpHistogramMode::Classic)
        .expect("within the expansion budget")
        .metadata[0]
        .metric_type
        .clone();
    assert_eq!(otlp_gauge_type, rw_metadata_type_string(2, "a_gauge"));

    // counter (monotonic Sum)
    let counter_req = otlp_request_with(
        metric::Data::Sum(Sum {
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                start_time_unix_nano: 0,
                time_unix_nano: 1,
                exemplars: vec![],
                flags: 0,
                value: Some(number_data_point::Value::AsDouble(1.0)),
            }],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            is_monotonic: true,
        }),
        "a_counter",
    );
    let otlp_counter_type = otlp_metrics::parse(&counter_req, 0, ExpHistogramMode::Classic)
        .expect("within the expansion budget")
        .metadata[0]
        .metric_type
        .clone();
    assert_eq!(otlp_counter_type, rw_metadata_type_string(1, "a_counter"));

    // histogram
    let histogram_req = otlp_request_with(
        metric::Data::Histogram(Histogram {
            data_points: vec![HistogramDataPoint {
                attributes: vec![],
                start_time_unix_nano: 0,
                time_unix_nano: 1,
                count: 1,
                sum: Some(1.0),
                bucket_counts: vec![1],
                explicit_bounds: vec![],
                exemplars: vec![],
                flags: 0,
                min: None,
                max: None,
            }],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        }),
        "a_histogram",
    );
    let otlp_histogram_type = otlp_metrics::parse(&histogram_req, 0, ExpHistogramMode::Classic)
        .expect("within the expansion budget")
        .metadata[0]
        .metric_type
        .clone();
    assert_eq!(
        otlp_histogram_type,
        rw_metadata_type_string(3, "a_histogram")
    );

    // summary
    let summary_req = otlp_request_with(
        metric::Data::Summary(Summary {
            data_points: vec![SummaryDataPoint {
                attributes: vec![],
                start_time_unix_nano: 0,
                time_unix_nano: 1,
                count: 1,
                sum: 1.0,
                quantile_values: vec![ValueAtQuantile {
                    quantile: 0.5,
                    value: 1.0,
                }],
                flags: 0,
            }],
        }),
        "a_summary",
    );
    let otlp_summary_type = otlp_metrics::parse(&summary_req, 0, ExpHistogramMode::Classic)
        .expect("within the expansion budget")
        .metadata[0]
        .metric_type
        .clone();
    assert_eq!(otlp_summary_type, rw_metadata_type_string(5, "a_summary"));
}
