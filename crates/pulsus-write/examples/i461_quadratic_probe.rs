//! Scratch probe (issue #461 code review, finding 1): times the label
//! builder under N promoted empty-label collisions. Deleted after use.
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use pulsus_write::protocols::otlp_metrics::{MetricIngestSettings, parse};

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

/// `n` scope attributes that add a promoted label, each followed by one
/// whose sanitized key collides with it and whose value is empty — so each
/// pair is one `Set` then one `Del` of an entry already in `add`.
fn request(n: usize) -> ExportMetricsServiceRequest {
    // All `n` promoted labels are added first, then all `n` collisions
    // delete them — so every delete removes an entry from a full vector
    // and every stored index behind it is rewritten.
    let mut attributes = Vec::with_capacity(n * 2);
    for i in 0..n {
        attributes.push(kv(&format!("a.{i:06}"), "v"));
    }
    for i in 0..n {
        attributes.push(kv(&format!("a_{i:06}"), ""));
    }
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "quad")],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "s".to_string(),
                    version: "1".to_string(),
                    attributes,
                    dropped_attributes_count: 0,
                }),
                metrics: vec![Metric {
                    name: "q".to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
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

fn main() {
    let settings = MetricIngestSettings {
        promote_scope_metadata: true,
        ..MetricIngestSettings::default()
    };
    for n in [1000usize, 2000, 4000, 8000] {
        let req = request(n);
        let start = std::time::Instant::now();
        let out = parse(&req, 0, settings).expect("within budget");
        let elapsed = start.elapsed();
        println!(
            "n={n:>5}  {:>9.3} ms  labels={}",
            elapsed.as_secs_f64() * 1000.0,
            out.series[0].labels.iter().count()
        );
    }
}
