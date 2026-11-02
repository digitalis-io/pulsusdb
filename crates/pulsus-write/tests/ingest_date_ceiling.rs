//! Issue #494 part 2: a log or metrics push whose rows fall on more than 100
//! UTC dates is refused `413` at admission on every log and metrics route,
//! sync and async, and nothing is inserted; and an asynchronous log push is
//! held for its batch's window before its insert.
//!
//! Every route is mounted on a writer over a recording inserter, so no case
//! here needs a server.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::routing::post;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message as _;
use pulsus_clickhouse::{ChError, ChRow};
use pulsus_config::WriterConfig;
use pulsus_model::ACTIVITY_BUCKET_MS;
use pulsus_write::protocols::remote_write::{Label, Sample, TimeSeries, WriteRequest};
use pulsus_write::writer::{
    BlockInserter, LogLandingRow, LogWriter, MetricLandingRow, MetricWriter, MetricWriterTables,
    WriterRuntime, WriterTables,
};
use pulsus_write::{LogIngestSettings, MetricIngestSettings, push_too_many_days_message};
use tower::ServiceExt as _;

/// One UTC day in nanoseconds.
const NS_PER_DAY: i64 = 86_400_000_000_000;

/// An hour into UTC day `day` after a fixed one, in nanoseconds.
fn ns_on_day(day: i64) -> i64 {
    (1_700_000_000_000_000_000 / NS_PER_DAY + day) * NS_PER_DAY + 3_600_000_000_000
}

/// Counts every insert call and answers each at once.
#[derive(Default)]
struct CountingInserter {
    calls: AtomicUsize,
}

impl CountingInserter {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl<R: ChRow> BlockInserter<R> for CountingInserter {
    fn insert<'a>(
        &'a self,
        _table: &'a str,
        _rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

fn spool_root(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pulsus-date-ceiling-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create the spool root");
    dir
}

fn runtime_at(cfg: &WriterConfig, spool: &std::path::Path) -> WriterRuntime {
    let mut runtime = WriterRuntime::from_config(cfg);
    runtime.spool_dir = spool.to_path_buf();
    runtime
}

fn log_writer(
    cfg: &WriterConfig,
    spool: &std::path::Path,
) -> (Arc<LogWriter>, Arc<CountingInserter>) {
    let inserter = Arc::new(CountingInserter::default());
    let landing: Arc<dyn BlockInserter<LogLandingRow>> = inserter.clone();
    let writer = LogWriter::with_landing_inserter_and_runtime(
        landing,
        runtime_at(cfg, spool),
        WriterTables::logs_default(),
    );
    (Arc::new(writer), inserter)
}

fn metric_writer(
    cfg: &WriterConfig,
    spool: &std::path::Path,
) -> (Arc<MetricWriter>, Arc<CountingInserter>) {
    let inserter = Arc::new(CountingInserter::default());
    let landing: Arc<dyn BlockInserter<MetricLandingRow>> = inserter.clone();
    let writer = MetricWriter::with_landing_inserter_and_runtime(
        landing,
        runtime_at(cfg, spool),
        ACTIVITY_BUCKET_MS,
        MetricWriterTables::metrics_default(),
    );
    (Arc::new(writer), inserter)
}

/// `POST /v1/logs` and the log push of `docs/api.md` §8.2 on `writer`.
fn log_routes(writer: Arc<LogWriter>) -> Router {
    let otlp = writer.clone();
    let loki = writer;
    Router::new()
        .route(
            "/v1/logs",
            post(move |headers: axum::http::HeaderMap, body: Body| {
                let sink = otlp.clone();
                async move {
                    pulsus_write::ingest(
                        sink.as_ref(),
                        headers,
                        body,
                        LogIngestSettings {
                            discover_log_levels: false,
                        },
                    )
                    .await
                }
            }),
        )
        .route(
            "/loki/api/v1/push",
            post(move |headers: axum::http::HeaderMap, body: Body| {
                let sink = loki.clone();
                async move {
                    pulsus_write::ingest_loki_push(
                        sink.as_ref(),
                        headers,
                        body,
                        LogIngestSettings {
                            discover_log_levels: false,
                        },
                    )
                    .await
                }
            }),
        )
}

/// `POST /v1/metrics` and `POST /api/v1/write` on `writer`.
fn metric_routes(writer: Arc<MetricWriter>) -> Router {
    let otlp = writer.clone();
    let rw = writer;
    Router::new()
        .route(
            "/v1/metrics",
            post(move |headers: axum::http::HeaderMap, body: Body| {
                let sink = otlp.clone();
                async move {
                    pulsus_write::ingest_metrics(
                        sink.as_ref(),
                        headers,
                        body,
                        MetricIngestSettings::default(),
                    )
                    .await
                }
            }),
        )
        .route(
            "/api/v1/write",
            post(move |headers: axum::http::HeaderMap, body: Body| {
                let sink = rw.clone();
                async move { pulsus_write::ingest_remote_write(sink.as_ref(), headers, body).await }
            }),
        )
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

/// An OTLP logs push: one resource per `pods` entry, each with one record on
/// each of `days` UTC dates. A pod name over the label value limit fails its
/// stream's label check.
fn otlp_logs(pods: &[&str], days: i64) -> Vec<u8> {
    ExportLogsServiceRequest {
        resource_logs: pods
            .iter()
            .map(|pod| ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "checkout"), kv("k8s.pod.name", pod)],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: (0..days)
                        .map(|d| LogRecord {
                            time_unix_nano: ns_on_day(d) as u64,
                            body: Some(AnyValue {
                                value: Some(Value::StringValue(format!("line on day {d}"))),
                            }),
                            ..Default::default()
                        })
                        .collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect(),
    }
    .encode_to_vec()
}

/// A JSON log push of §8.2: one stream per `pods` entry, each with one entry
/// on each of `days` UTC dates.
fn loki_logs(pods: &[&str], days: i64) -> Vec<u8> {
    let streams: Vec<serde_json::Value> = pods
        .iter()
        .map(|pod| {
            serde_json::json!({
                "stream": { "service_name": "checkout", "pod": pod },
                "values": (0..days)
                    .map(|d| serde_json::json!([ns_on_day(d).to_string(), format!("line on day {d}")]))
                    .collect::<Vec<_>>(),
            })
        })
        .collect();
    serde_json::to_vec(&serde_json::json!({ "streams": streams })).expect("JSON")
}

/// An OTLP metrics push: one gauge point on each of `days` UTC dates.
fn otlp_metrics(days: i64) -> Vec<u8> {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "checkout")],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "queue_depth".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: (0..days)
                            .map(|d| NumberDataPoint {
                                time_unix_nano: ns_on_day(d) as u64,
                                value: Some(number_data_point::Value::AsDouble(1.0)),
                                ..Default::default()
                            })
                            .collect(),
                    })),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec()
}

/// A remote-write push, snappy-compressed: one sample on each of `days` UTC
/// dates.
fn remote_write(days: i64) -> Vec<u8> {
    let label = |name: &str, value: &str| Label {
        name: name.to_string(),
        value: value.to_string(),
    };
    let req = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![label("__name__", "queue_depth"), label("job", "checkout")],
            samples: (0..days)
                .map(|d| Sample {
                    value: 1.0,
                    timestamp: ns_on_day(d) / 1_000_000,
                })
                .collect(),
            histograms: vec![],
            exemplars: vec![],
        }],
        metadata: vec![],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy")
}

/// The `google.rpc.Status` an OTLP route answers an error with.
#[derive(Clone, PartialEq, prost::Message)]
struct Status {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

/// One route's push, and how its envelope carries an error.
struct Route {
    path: &'static str,
    content_type: &'static str,
    otlp: bool,
}

const OTLP_LOGS: Route = Route {
    path: "/v1/logs",
    content_type: "application/x-protobuf",
    otlp: true,
};
const LOKI: Route = Route {
    path: "/loki/api/v1/push",
    content_type: "application/json",
    otlp: false,
};
const OTLP_METRICS: Route = Route {
    path: "/v1/metrics",
    content_type: "application/x-protobuf",
    otlp: true,
};
const REMOTE_WRITE: Route = Route {
    path: "/api/v1/write",
    content_type: "application/x-protobuf",
    otlp: false,
};

/// Sends `body` to `route`, sync or async, and returns the status and the
/// error message the route's envelope carries.
async fn send(
    router: &Router,
    route: &Route,
    body: Vec<u8>,
    asynchronous: bool,
) -> (StatusCode, String) {
    let mut request = Request::builder()
        .method("POST")
        .uri(route.path)
        .header(header::CONTENT_TYPE, route.content_type);
    if route.path == REMOTE_WRITE.path {
        request = request.header(header::CONTENT_ENCODING, "snappy");
    }
    if asynchronous {
        request = request.header("X-Pulsus-Async", "1");
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body)).expect("a request"))
        .await
        .expect("the route answers");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body");
    let message = if route.otlp && !status.is_success() {
        Status::decode(bytes.as_ref())
            .map(|s| s.message)
            .unwrap_or_else(|e| format!("not a google.rpc.Status: {e}"))
    } else {
        String::from_utf8_lossy(&bytes).into_owned()
    };
    (status, message)
}

/// Asserts the refusal: `413`, the message in the route's envelope (an OTLP
/// route's `google.rpc.Status`, the text routes' LF-terminated body).
fn assert_refused(what: &str, route: &Route, (status, message): (StatusCode, String)) {
    let want = push_too_many_days_message(101, 100);
    assert_eq!(
        want,
        "push covers too many UTC dates for one block: 101 dates (limit 100)"
    );
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{what}: {message}");
    if route.otlp {
        assert_eq!(message, want, "{what}");
    } else {
        assert_eq!(message, format!("{want}\n"), "{what}");
    }
}

/// **2-U16.** Every log and metrics route refuses a push on 101 UTC dates
/// `413`, sync and async, with the route's envelope; so does each log route
/// for a partly valid push whose passing streams cover 101 dates. Nothing is
/// inserted.
#[tokio::test]
async fn a_push_over_one_hundred_dates_is_refused_on_every_log_and_metrics_route() {
    let root = spool_root("routes");
    let cfg = WriterConfig::default();
    let (logs, log_inserts) = log_writer(&cfg, &root);
    let (metrics, metric_inserts) = metric_writer(&cfg, &root);
    let log_router = log_routes(logs.clone());
    let metric_router = metric_routes(metrics.clone());
    let too_long = "p".repeat(2_049);

    for asynchronous in [false, true] {
        let mode = if asynchronous { "async" } else { "sync" };
        assert_refused(
            &format!("{mode} /v1/logs"),
            &OTLP_LOGS,
            send(
                &log_router,
                &OTLP_LOGS,
                otlp_logs(&["pod-a"], 101),
                asynchronous,
            )
            .await,
        );
        assert_refused(
            &format!("{mode} §8.2 push"),
            &LOKI,
            send(&log_router, &LOKI, loki_logs(&["pod-a"], 101), asynchronous).await,
        );
        assert_refused(
            &format!("{mode} partly valid /v1/logs"),
            &OTLP_LOGS,
            send(
                &log_router,
                &OTLP_LOGS,
                otlp_logs(&["pod-a", &too_long], 101),
                asynchronous,
            )
            .await,
        );
        assert_refused(
            &format!("{mode} partly valid §8.2 push"),
            &LOKI,
            send(
                &log_router,
                &LOKI,
                loki_logs(&["pod-a", &too_long], 101),
                asynchronous,
            )
            .await,
        );
        assert_refused(
            &format!("{mode} /v1/metrics"),
            &OTLP_METRICS,
            send(
                &metric_router,
                &OTLP_METRICS,
                otlp_metrics(101),
                asynchronous,
            )
            .await,
        );
        assert_refused(
            &format!("{mode} /api/v1/write"),
            &REMOTE_WRITE,
            send(
                &metric_router,
                &REMOTE_WRITE,
                remote_write(101),
                asynchronous,
            )
            .await,
        );
    }
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(log_inserts.calls(), 0, "no log push is inserted");
    assert_eq!(metric_inserts.calls(), 0, "no metrics push is inserted");

    // The same routes store a push on 100 dates, so the refusal above is the
    // date count's and not the fixture's.
    for (router, route, body) in [
        (&log_router, &OTLP_LOGS, otlp_logs(&["pod-b"], 100)),
        (&log_router, &LOKI, loki_logs(&["pod-c"], 100)),
        (&metric_router, &OTLP_METRICS, otlp_metrics(100)),
        (&metric_router, &REMOTE_WRITE, remote_write(100)),
    ] {
        let (status, message) = send(router, route, body, false).await;
        assert!(
            status.is_success(),
            "{} on 100 dates: {status} {message}",
            route.path
        );
    }
    logs.shutdown(Duration::from_secs(2)).await;
    metrics.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **2-A1.** An asynchronous log push is answered `202` at once and inserted
/// when its batch's window closes.
#[tokio::test(start_paused = true)]
async fn an_async_log_push_is_inserted_when_its_window_closes() {
    let root = spool_root("async-window");
    let cfg = WriterConfig {
        landing_batch_ms: 1_000,
        ..WriterConfig::default()
    };
    let (logs, inserts) = log_writer(&cfg, &root);
    let router = log_routes(logs.clone());
    let t0 = tokio::time::Instant::now();
    let (status, message) = send(&router, &OTLP_LOGS, otlp_logs(&["pod-a"], 1), true).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{message}");
    assert_eq!(t0.elapsed(), Duration::ZERO, "answered at 0 ms");
    tokio::time::sleep_until(t0 + Duration::from_millis(999)).await;
    assert_eq!(inserts.calls(), 0, "no insert before the window closes");
    tokio::time::sleep_until(t0 + Duration::from_millis(1_000)).await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(inserts.calls(), 1, "the insert at 1000 ms");
    assert_eq!(t0.elapsed(), Duration::from_millis(1_000));
    logs.shutdown(Duration::from_secs(2)).await;
    std::fs::remove_dir_all(&root).ok();
}

/// **2-A2.** An asynchronous log push answered `202` is held in memory until
/// its batch's window closes: a process that ends 300 ms later, without its
/// drain, has made no insert and filed no spool copy.
#[test]
fn an_async_log_push_is_held_until_its_window_closes() {
    let root = spool_root("async-death");
    let cfg = WriterConfig {
        landing_batch_ms: 1_000,
        ..WriterConfig::default()
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime");
    let (logs, inserts) = runtime.block_on(async { log_writer(&cfg, &root) });
    let router = log_routes(logs.clone());
    let (status, message) =
        runtime.block_on(async { send(&router, &OTLP_LOGS, otlp_logs(&["pod-a"], 1), true).await });
    assert_eq!(status, StatusCode::ACCEPTED, "{message}");
    std::thread::sleep(Duration::from_millis(300));
    runtime.shutdown_background();
    assert_eq!(inserts.calls(), 0, "no insert before the window closes");
    let files = walk(&root);
    assert!(files.is_empty(), "no spool file: {files:?}");
    // The process ended without its drain: the writer is never torn down.
    std::mem::forget(router);
    std::mem::forget(logs);
    std::fs::remove_dir_all(&root).ok();
}

/// Every file under `dir`.
fn walk(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk(&path));
            } else {
                out.push(path);
            }
        }
    }
    out
}
