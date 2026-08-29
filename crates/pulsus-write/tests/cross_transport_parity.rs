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

use pulsus_write::protocols::otlp_metrics;
use pulsus_write::protocols::otlp_metrics::MetricIngestSettings;
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

/// The remote-write half carries `job`, not `service_name` (issue #461):
/// an OpenTelemetry collector's `prometheusremotewrite` exporter derives
/// `job` from `service.namespace`/`service.name` before it writes, so this
/// is the label an OTLP-sourced series actually arrives with on that
/// transport. The pre-#461 fixture used `service_name`, the logs
/// convention, which our OTLP metrics receiver no longer emits.
fn rw_request(metric_name: &str, host: &str, service: &str) -> WriteRequest {
    WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![
                rw_label("__name__", metric_name),
                rw_label("host", host),
                rw_label("job", service),
            ],
            samples: vec![Sample {
                value: 1.0,
                // Deliberately a *different* timestamp than the OTLP
                // fixture (fingerprints must not depend on sample data,
                // only on the series' label identity).
                timestamp: 1_800_000_000_000,
            }],
            histograms: vec![],
        }],
        metadata: vec![],
    }
}

/// Test gap 2 (code review): the same logical series (`up{host="node-a",
/// job="checkout"}`) pushed via OTLP (resource `service.name`
/// attribute + data point `host` attribute) and via remote-write (`host`/
/// `job` labels directly) must resolve to the identical
/// `(metric_name, fingerprint)` — proving both receivers' label
/// normalization + fingerprinting paths converge on one series identity
/// regardless of transport, not just self-consistently within each
/// transport's own test suite.
#[test]
fn same_logical_series_fingerprints_identically_across_otlp_and_remote_write() {
    let otlp_req = otlp_gauge_request("up", "node-a", "checkout");
    let otlp_out = otlp_metrics::parse(&otlp_req, 0, MetricIngestSettings::default())
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
        otlp_out.series[0].labels.get("job"),
        rw_out.series[0].labels.get("job")
    );
    assert_eq!(otlp_out.series[0].labels.get("job"), Some("checkout"));
}

/// The derivation form of the same cross-transport identity: OTLP's
/// `service.name` resource attribute becomes `job`
/// (`metrics_to_prw.go:420-426 @ v3.13.0`), which must fingerprint
/// identically to a remote-write `job` label carrying the same value.
#[test]
fn otlp_derived_job_and_remote_write_job_label_fingerprint_identically() {
    let otlp_req = otlp_gauge_request("cpu_usage_ratio", "node-b", "billing");
    let otlp_out = otlp_metrics::parse(&otlp_req, 0, MetricIngestSettings::default())
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
    let otlp_gauge_type = otlp_metrics::parse(&gauge_req, 0, MetricIngestSettings::default())
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
    let otlp_counter_type = otlp_metrics::parse(&counter_req, 0, MetricIngestSettings::default())
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
    let otlp_histogram_type =
        otlp_metrics::parse(&histogram_req, 0, MetricIngestSettings::default())
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
    let otlp_summary_type = otlp_metrics::parse(&summary_req, 0, MetricIngestSettings::default())
        .expect("within the expansion budget")
        .metadata[0]
        .metric_type
        .clone();
    assert_eq!(otlp_summary_type, rw_metadata_type_string(5, "a_summary"));
}

// ---------------------------------------------------------------------
// Logs: the structured-metadata seam (issue #381).
// ---------------------------------------------------------------------

/// The same collision rows the two receivers' own suites use, in wire order.
#[rustfmt::skip]
const SM_COLLISION_ROWS: &[&[(&str, &str)]] = &[
    &[("a.b", "x"), ("a_b", "keep")],
    &[("a_b", "keep"), ("a.b", "x")],
    &[("a.b", "1"), ("a-b", "2")],
    &[("a-b", "2"), ("a.b", "1")],
    &[("a_b", "1"), ("a_b", "2")],
    &[("a_b", "2"), ("a_b", "1")],
    &[("a_b", "1"), ("a_b", "2"), ("a.b", "9")],
    &[("a.b", "1"), ("a_b", "2"), ("a.b", "3")],
    &[("a.b", "1"), ("a_b", "2"), ("a-b", "3")],
    &[("a-b", "3"), ("a_b", "2"), ("a.b", "1")],
    &[("a.b", "1"), ("a.b", "2")],
    &[("a_b", ""), ("a.b", "x"), ("a-b", "y")],
    &[("a.b", "x"), ("a_b", "keep"), ("z", "1")],
    &[("a.b", "9"), ("a_b", "1"), ("a_b", "2")],
    &[("a.b", "x"), ("a_b", "p\u{FFFD}")],
    &[("a_b", "p\u{FFFD}"), ("a.b", "x")],
    &[("a_b", "p\u{FFFD}q")],
    &[("a_b", "1"), ("a_b", "p\u{FFFD}")],
];

/// A Loki-push JSON body carrying exactly `sm` as one entry's structured
/// metadata. Assembled as TEXT: a repeated name is half of what is under
/// test and a `serde_json::Map` would collapse it.
fn loki_push_stored(sm: &[(&str, &str)]) -> String {
    let object: Vec<String> = sm
        .iter()
        .map(|(k, v)| {
            format!(
                "{}:{}",
                serde_json::to_string(k).expect("string"),
                serde_json::to_string(v).expect("string")
            )
        })
        .collect();
    let body = format!(
        r#"{{"streams":[{{"stream":{{"service_name":"checkout"}},"values":[["1700000000000000000","hello",{{{}}}]]}}]}}"#,
        object.join(",")
    );
    let out = pulsus_write::parse_loki_json(body.as_bytes(), 0).expect("admissible push body");
    out.rows[0].structured_metadata.clone()
}

/// An OTLP body carrying exactly `sm` as one scope's attributes — scope name
/// and version left empty so nothing else is appended.
fn otlp_scope_stored(sm: &[(&str, &str)]) -> String {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv(
                    "service.name",
                    Value::StringValue("checkout".to_string()),
                )],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: String::new(),
                    version: String::new(),
                    attributes: sm
                        .iter()
                        .map(|(k, v)| kv(k, Value::StringValue(v.to_string())))
                        .collect(),
                    dropped_attributes_count: 0,
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("hello".to_string())),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let out = pulsus_write::parse(&req, 0).expect("admissible OTLP body");
    out.rows[0].structured_metadata.clone()
}

/// The two log receivers run the SAME structured-metadata rule, stated as an
/// equation rather than an exception (issue #381):
///
/// > `OTLP(raw keys) == Loki-push(canonicalized keys)`
///
/// Both call `pulsus_model::resolve_structured_metadata` through the one
/// shared seam. What differs is only what each hands it: the push transport
/// hands it RAW names, the OTLP one hands it names its translation has
/// already renamed. That is the reference's own asymmetry, at the same place
/// — its OTLP translation runs `LabelNamer.Build` over every attribute key
/// before the distributor's builder sees it
/// (`pkg/loghttp/push/otlp.go:602-614 @ v3.7.4`, reached for scope attributes
/// from `:300-317`), whereas the push path hands the builder the wire names.
///
/// So no row is exempted: every row is checked, and the rows whose names are
/// already canonicalize fixed points — where the renaming is the identity and
/// the equation collapses to plain equality — are checked a second time and
/// counted, because those are the rows that were CROSS-TRANSPORT DIVERGENT
/// before this fix. Measured at `b872855`: `[a_b="2", a_b="1"]` stored
/// `a_b="1"` through the OTLP scope path and `a_b="2"` through the push one.
#[test]
fn both_log_receivers_resolve_structured_metadata_with_one_rule() {
    let mut identical = 0usize;
    for row in SM_COLLISION_ROWS {
        let canonicalized: Vec<(String, String)> = row
            .iter()
            .map(|(k, v)| (pulsus_model::canonicalize_label_key(k), (*v).to_string()))
            .collect();
        let as_refs: Vec<(&str, &str)> = canonicalized
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(
            otlp_scope_stored(row),
            loki_push_stored(&as_refs),
            "the OTLP scope path is not the push path over renamed keys: {row:?}"
        );
        if as_refs == *row {
            identical += 1;
            assert_eq!(
                otlp_scope_stored(row),
                loki_push_stored(row),
                "already-canonical keys must resolve identically on both transports: {row:?}"
            );
        }
    }
    // Non-vacuity, and exact: the rows whose every name is already a fixed
    // point are `[a_b,a_b]` twice, `[a_b]` once and `[a_b,a_b]` with a
    // U+FFFD — four of the eighteen.
    assert_eq!(
        identical, 4,
        "the set of already-canonical rows has moved; the equation is being checked over a \
         different subset than the comment claims"
    );
}
