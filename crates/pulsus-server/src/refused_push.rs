//! A push with refused items is logged (issue #901, docs/api.md §1.4).
//!
//! A receiver that admits a push but refuses some of its items attaches a
//! [`RefusedPush`] to its answer. Each ingest route is mounted with [`post`],
//! which takes it off the answer and writes one `WARN` line naming the
//! signal, the route, the count, the first reason and the sender.
//! [`RefusalLog`] bounds how often: one line a minute for each route and
//! reason.
//!
//! A push with nothing refused does no work here beyond one check of its
//! answer's empty extensions: the request is taken apart by move, and its
//! path and connection address are read only once a refusal is found. The
//! sender's address comes from the connection ([`PeerAddr`], set by
//! [`with_peer`] on both listeners); its user agent and `X-Forwarded-For`
//! come from the request's headers, read by the receiver only when it
//! refused something.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::connect_info::{ConnectInfo, Connected, IntoMakeServiceWithConnectInfo};
use axum::extract::{Request, State};
use axum::http::HeaderMap;
use axum::response::Response;
use axum::routing::MethodRouter;
use axum::serve::IncomingStream;
use pulsus_write::RefusedPush;
use tokio::net::TcpListener;

use crate::app::AppState;
use crate::tls::TlsListener;

/// How long a route and reason stay quiet after their line.
pub(crate) const WINDOW: Duration = Duration::from_secs(60);

/// How many route and reason pairs one [`RefusalLog`] tracks. Past it, a new
/// reason shares its route's one line a minute, so a reason that embeds a
/// value cannot grow the table or the log without bound.
pub(crate) const MAX_REASONS: usize = 64;

/// The address of the connection a request came in on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerAddr(pub(crate) SocketAddr);

impl Connected<IncomingStream<'_, TcpListener>> for PeerAddr {
    fn connect_info(stream: IncomingStream<'_, TcpListener>) -> Self {
        PeerAddr(*stream.remote_addr())
    }
}

impl Connected<IncomingStream<'_, TlsListener>> for PeerAddr {
    fn connect_info(stream: IncomingStream<'_, TlsListener>) -> Self {
        PeerAddr(*stream.remote_addr())
    }
}

/// The service both listeners serve: `router`, with each connection's
/// [`PeerAddr`] in every request's extensions.
pub(crate) fn with_peer(router: Router) -> IntoMakeServiceWithConnectInfo<Router, PeerAddr> {
    router.into_make_service_with_connect_info::<PeerAddr>()
}

/// A [`RefusalLog`] for one group of routes, kept for the life of the
/// process: a router is built once per process, and every request of its
/// routes reads the log without a reference count.
pub(crate) fn leaked() -> &'static RefusalLog {
    Box::leak(Box::new(RefusalLog::new()))
}

/// `POST` to `handler`, with a refused push in its answer logged through
/// `log`. `handler` is an ingest route's own handler, unchanged.
pub(crate) fn post<H, Fut>(log: &'static RefusalLog, handler: H) -> MethodRouter<AppState>
where
    H: FnOnce(State<AppState>, HeaderMap, Body) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response> + Send + 'static,
{
    axum::routing::post(move |state: State<AppState>, request: Request| {
        let handler = handler.clone();
        logged(log, request, move |headers, body| {
            handler(state, headers, body)
        })
    })
}

/// Runs `handler` on the request's headers and body, moved out of it; when
/// the answer carries a [`RefusedPush`], takes it off and logs it, as `log`
/// allows. Only then are the request's path and connection address read.
async fn logged<F, Fut>(log: &'static RefusalLog, request: Request, handler: F) -> Response
where
    F: FnOnce(HeaderMap, Body) -> Fut,
    Fut: Future<Output = Response>,
{
    let (mut parts, body) = request.into_parts();
    let headers = std::mem::take(&mut parts.headers);
    let mut response = handler(headers, body).await;
    if let Some(refused) = response.extensions_mut().remove::<RefusedPush>() {
        let route = parts.uri.path();
        let peer = parts
            .extensions
            .get::<ConnectInfo<PeerAddr>>()
            .map(|info| info.0.0);
        if let Some(held) = log.admit(route, &refused.reason, Instant::now()) {
            tracing::warn!("{}", line(&refused, route, peer, held));
        }
    }
    response
}

/// A route and reason, or a route alone for the reasons past
/// [`MAX_REASONS`].
#[derive(Debug, PartialEq, Eq, Hash)]
struct Key {
    route: String,
    reason: Option<String>,
}

/// When a key was last logged, and how many pushes it has held back since.
struct Entry {
    logged_at: Instant,
    held: u64,
}

/// The rate limit: one line a minute for each route and reason.
pub(crate) struct RefusalLog {
    entries: Mutex<HashMap<Key, Entry>>,
}

impl RefusalLog {
    pub(crate) fn new() -> Self {
        RefusalLog {
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// `Some(held)` when a push refused on `route` for `reason` at `now` is
    /// to be logged, `held` being the pushes like it that were not; `None`
    /// when it is not.
    pub(crate) fn admit(&self, route: &str, reason: &str, now: Instant) -> Option<u64> {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let mut key = Key {
            route: route.to_string(),
            reason: Some(reason.to_string()),
        };
        if !entries.contains_key(&key) && entries.len() >= MAX_REASONS {
            // An entry still holding pushes back stays, so the next line for
            // it reports them all.
            entries
                .retain(|_, entry| entry.held > 0 || now.duration_since(entry.logged_at) < WINDOW);
            if entries.len() >= MAX_REASONS {
                key.reason = None;
            }
        }
        match entries.get_mut(&key) {
            Some(entry) if now.duration_since(entry.logged_at) < WINDOW => {
                entry.held += 1;
                None
            }
            Some(entry) => {
                let held = entry.held;
                entry.logged_at = now;
                entry.held = 0;
                Some(held)
            }
            None => {
                entries.insert(
                    key,
                    Entry {
                        logged_at: now,
                        held: 0,
                    },
                );
                Some(0)
            }
        }
    }
}

/// The log line for `refused`, which came in on `route` from `peer`, with
/// `held` pushes like it not logged since the last line.
pub(crate) fn line(
    refused: &RefusedPush,
    route: &str,
    peer: Option<SocketAddr>,
    held: u64,
) -> String {
    let mut out = format!(
        "{} push on {route}: {} of {} {} rejected ({}) from ",
        refused.signal, refused.refused, refused.total, refused.unit, refused.reason
    );
    // `write!` to a `String` cannot fail.
    match peer {
        Some(addr) => {
            let _ = write!(out, "{addr}");
        }
        None => out.push_str("an unknown address"),
    }
    if let Some(forwarded) = &refused.forwarded_for {
        let _ = write!(out, " (X-Forwarded-For {forwarded:?})");
    }
    if let Some(agent) = &refused.user_agent {
        let _ = write!(out, ", user agent {agent:?}");
    }
    if held > 0 {
        let _ = write!(out, "; {held} more pushes like it were not logged");
    }
    out
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex, OnceLock};

    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use axum::http::{Request, StatusCode};
    use axum::response::Response;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::metrics::v1::{
        AggregationTemporality, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
        metric, number_data_point,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use prost::Message;
    use pulsus_clickhouse::{ChError, ChRow};
    use pulsus_config::Config;
    use pulsus_write::writer::BlockInserter;
    use pulsus_write::{LogWriter, MetricWriter, TraceWriter, TraceWriterTables};
    use tokio::sync::RwLock;
    use tower::ServiceExt;
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;
    use crate::app::{self, BuildInfo};
    use crate::ingest::{MetricWriterSink, TraceWriterSink, WriterSink};

    const PEER: &str = "192.0.2.7:4711";
    const AGENT: &str = "test-agent/1.0";
    const DELTA_REASON: &str =
        "metric http_requests: delta temporality is not ingested; send cumulative";

    /// An insert that always succeeds, so a push is admitted and stored.
    struct OkInserter;

    impl<R: ChRow> BlockInserter<R> for OkInserter {
        fn insert<'a>(
            &'a self,
            _table: &'a str,
            _rows: &'a [R],
        ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    /// The production router over writers whose inserts succeed, with the
    /// compatibility routes on, so the Loki push route is mounted.
    fn router() -> Router {
        let config = Config {
            compat_endpoints: true,
            ..Config::default()
        };
        let (_tail_tx, tail_rx) = tokio::sync::watch::channel(false);
        let log = Arc::new(LogWriter::with_landing_inserter(
            Arc::new(OkInserter),
            &config.writer,
        ));
        let metric = Arc::new(MetricWriter::with_landing_inserter(
            Arc::new(OkInserter),
            &config.writer,
            pulsus_model::ACTIVITY_BUCKET_MS,
        ));
        let trace = Arc::new(TraceWriter::with_inserters_with_tables(
            Arc::new(OkInserter),
            &config.writer,
            TraceWriterTables::traces_default(),
        ));
        let state = AppState {
            pool: Arc::new(RwLock::new(None)),
            retention_days: Arc::new(OnceLock::from(7)),
            config: Arc::new(config.clone()),
            metrics: PrometheusBuilder::new().build_recorder().handle(),
            build: BuildInfo::from_build_env(),
            writer: Arc::new(WriterSink::new(Arc::new(OnceLock::from(log)))),
            metric_writer: Arc::new(MetricWriterSink::new(Arc::new(OnceLock::from(metric)))),
            profile_writer: Arc::new(crate::ingest::ProfileWriterSink::new(Arc::new(
                OnceLock::new(),
            ))),
            trace_writer: Arc::new(TraceWriterSink::new(Arc::new(OnceLock::from(trace)))),
            label_cache: Arc::new(OnceLock::new()),
            rollup_levels: Arc::new(pulsus_read::metrics::rollup::LevelSlot::new()),
            graph_target: pulsus_read::GraphTargetCache::default(),
            read_cache: Default::default(),
            eval_gate: Arc::new(pulsus_read::EvalGate::new(
                config.reader.query_eval_concurrency,
            )),
            started_at: std::time::SystemTime::now(),
            tail: Arc::new(crate::app::TailRuntime::new(
                tail_rx,
                config.reader.tail_max_connections,
            )),
            request_metrics: crate::request_metrics::RequestMetrics::leak_new(),
        };
        app::build_router(state, &config).expect("the router builds")
    }

    /// Every `WARN` line this module writes while the guard is held, on this
    /// thread.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Captured {
        type Writer = Captured;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl Captured {
        fn lines(&self) -> Vec<String> {
            String::from_utf8(self.0.lock().unwrap().clone())
                .expect("the log is text")
                .lines()
                .map(|line| line.trim_start().to_string())
                .collect()
        }
    }

    fn capture() -> (Captured, tracing::subscriber::DefaultGuard) {
        let captured = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(format!(
                "{}=warn",
                module_path!().trim_end_matches("::tests")
            )))
            .with_writer(captured.clone())
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        (captured, guard)
    }

    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch")
            .as_nanos() as u64
    }

    fn string(value: &str) -> Option<AnyValue> {
        Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        })
    }

    fn point(time: u64) -> NumberDataPoint {
        NumberDataPoint {
            time_unix_nano: time,
            value: Some(number_data_point::Value::AsDouble(1.0)),
            ..Default::default()
        }
    }

    /// One gauge point, and with `delta` a delta sum of two points more.
    fn metrics_request(delta: bool) -> Vec<u8> {
        let now = now_ns();
        let mut metrics = vec![Metric {
            name: "up".to_string(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![point(now)],
            })),
            ..Default::default()
        }];
        if delta {
            metrics.push(Metric {
                name: "http_requests".to_string(),
                data: Some(metric::Data::Sum(Sum {
                    data_points: vec![point(now), point(now + 1)],
                    aggregation_temporality: AggregationTemporality::Delta as i32,
                    is_monotonic: true,
                })),
                ..Default::default()
            });
        }
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    fn record(time: u64) -> LogRecord {
        LogRecord {
            time_unix_nano: time,
            body: string("hello"),
            ..Default::default()
        }
    }

    fn resource_logs(service: &str, records: Vec<LogRecord>) -> ResourceLogs {
        ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: string(service),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: records,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// One record, and with `bad_record` one more whose timestamp does not
    /// fit.
    fn logs_request(bad_record: bool) -> Vec<u8> {
        let mut records = vec![record(now_ns())];
        if bad_record {
            records.push(record(u64::MAX));
        }
        ExportLogsServiceRequest {
            resource_logs: vec![resource_logs("checkout", records)],
        }
        .encode_to_vec()
    }

    /// A value one byte over the stream label value limit.
    fn long_value() -> String {
        "b".repeat(2049)
    }

    /// Two resources: one fine, one whose service name is too long.
    fn logs_stream_request() -> Vec<u8> {
        ExportLogsServiceRequest {
            resource_logs: vec![
                resource_logs("checkout", vec![record(now_ns())]),
                resource_logs(&long_value(), vec![record(now_ns())]),
            ],
        }
        .encode_to_vec()
    }

    fn span(name: &str, trace_id: Vec<u8>) -> Span {
        let now = now_ns();
        Span {
            trace_id,
            span_id: vec![2; 8],
            name: name.to_string(),
            start_time_unix_nano: now,
            end_time_unix_nano: now + 1_000,
            ..Default::default()
        }
    }

    /// One span, and with `bad_span` one more whose trace id is 3 bytes.
    fn traces_request(bad_span: bool) -> Vec<u8> {
        let mut spans = vec![span("good", vec![1; 16])];
        if bad_span {
            spans.push(span("bad", vec![1; 3]));
        }
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    /// Two samples of `up`, and with `unnamed` a series of one sample with no
    /// name.
    fn remote_write_request(unnamed: bool) -> Vec<u8> {
        use pulsus_write::protocols::remote_write::{Label, Sample, TimeSeries};
        let ms = (now_ns() / 1_000_000) as i64;
        let series = |labels: Vec<Label>, samples: Vec<Sample>| TimeSeries {
            labels,
            samples,
            histograms: vec![],
            exemplars: vec![],
        };
        let mut timeseries = vec![series(
            vec![Label {
                name: "__name__".to_string(),
                value: "up".to_string(),
            }],
            vec![
                Sample {
                    value: 1.0,
                    timestamp: ms,
                },
                Sample {
                    value: 1.0,
                    timestamp: ms + 1,
                },
            ],
        )];
        if unnamed {
            timeseries.push(series(
                vec![Label {
                    name: "job".to_string(),
                    value: "api".to_string(),
                }],
                vec![Sample {
                    value: 1.0,
                    timestamp: ms,
                }],
            ));
        }
        let body = pulsus_write::WriteRequest {
            timeseries,
            metadata: vec![],
        }
        .encode_to_vec();
        snap::raw::Encoder::new()
            .compress_vec(&body)
            .expect("snappy encodes")
    }

    /// One stream, and with `bad_stream` one more whose label value is too
    /// long.
    fn loki_request(bad_stream: bool) -> Vec<u8> {
        let ts = now_ns();
        let mut streams = vec![format!(
            r#"{{"stream":{{"app":"good"}},"values":[["{ts}","a"]]}}"#
        )];
        if bad_stream {
            streams.push(format!(
                r#"{{"stream":{{"app":"{}"}},"values":[["{ts}","b"]]}}"#,
                long_value()
            ));
        }
        format!(r#"{{"streams":[{}]}}"#, streams.join(",")).into_bytes()
    }

    /// The 5-byte gRPC prefix and `message`.
    fn grpc_frame(message: Vec<u8>) -> Vec<u8> {
        let mut framed = vec![0];
        framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
        framed.extend(message);
        framed
    }

    /// The paths the pushes below go to, and how each is sent.
    #[derive(Clone, Copy)]
    enum Route {
        OtlpLogs,
        OtlpMetrics,
        OtlpTraces,
        GrpcLogs,
        GrpcMetrics,
        GrpcTraces,
        RemoteWrite,
        Loki,
    }

    impl Route {
        fn path(self) -> &'static str {
            match self {
                Route::OtlpLogs => "/v1/logs",
                Route::OtlpMetrics => "/v1/metrics",
                Route::OtlpTraces => "/v1/traces",
                Route::GrpcLogs => "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
                Route::GrpcMetrics => {
                    "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export"
                }
                Route::GrpcTraces => "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
                Route::RemoteWrite => "/api/v1/write",
                Route::Loki => "/loki/api/v1/push",
            }
        }

        fn content_type(self) -> &'static str {
            match self {
                Route::GrpcLogs | Route::GrpcMetrics | Route::GrpcTraces => "application/grpc",
                Route::Loki => "application/json",
                _ => "application/x-protobuf",
            }
        }

        fn body(self, message: Vec<u8>) -> Vec<u8> {
            match self {
                Route::GrpcLogs | Route::GrpcMetrics | Route::GrpcTraces => grpc_frame(message),
                _ => message,
            }
        }
    }

    /// `body` to `route`, from [`PEER`] with [`AGENT`] and `extra` headers.
    async fn push(
        router: &Router,
        route: Route,
        body: Vec<u8>,
        extra: &[(&str, &str)],
    ) -> Response {
        let mut request = Request::post(route.path())
            .header("content-type", route.content_type())
            .header("user-agent", AGENT);
        for (name, value) in extra {
            request = request.header(*name, *value);
        }
        let mut request = request
            .body(Body::from(route.body(body)))
            .expect("a request");
        request
            .extensions_mut()
            .insert(ConnectInfo(PeerAddr(PEER.parse().expect("an address"))));
        router.clone().oneshot(request).await.expect("an answer")
    }

    fn expected(route: Route, counts: &str, reason: &str) -> String {
        format!(
            "WARN {} push on {}: {counts} rejected ({reason}) from {PEER}, user agent \"{AGENT}\"",
            match route {
                Route::OtlpLogs | Route::GrpcLogs | Route::Loki => "logs",
                Route::OtlpMetrics | Route::GrpcMetrics | Route::RemoteWrite => "metrics",
                Route::OtlpTraces | Route::GrpcTraces => "traces",
            },
            route.path()
        )
    }

    /// Each route that can store part of a push logs the line, and answers as
    /// it did, whether the push waits for its flush or not.
    #[tokio::test]
    async fn a_partial_refusal_on_each_route_logs_its_line() {
        let long = long_value();
        let log_record_reason = "log record timestamp 18446744073709551615 exceeds the representable i64 \
             nanosecond range";
        let otlp_stream_reason =
            format!("stream '{{service_name=\"{long}\"}}' has label value too long: '{long}'");
        let loki_stream_reason = format!(
            "stream '{{app=\"{long}\", service_name=\"{long}\"}}' has label value too long: \
             '{long}'"
        );
        let span_reason = "span \"bad\": trace_id must be exactly 16 bytes, got 3";
        let rw_reason = "time series has no __name__ label (or it is empty): series dropped";
        // The route, the push, what the line counts and why, and the answer
        // when the push waits for its flush and when it does not.
        let cases = [
            (
                Route::OtlpMetrics,
                metrics_request(true),
                "2 of 3 data points",
                DELTA_REASON.to_string(),
                [StatusCode::OK, StatusCode::ACCEPTED],
            ),
            (
                Route::GrpcMetrics,
                metrics_request(true),
                "2 of 3 data points",
                DELTA_REASON.to_string(),
                [StatusCode::OK, StatusCode::OK],
            ),
            (
                Route::RemoteWrite,
                remote_write_request(true),
                "1 of 3 samples",
                rw_reason.to_string(),
                [StatusCode::NO_CONTENT, StatusCode::ACCEPTED],
            ),
            (
                Route::OtlpLogs,
                logs_request(true),
                "1 of 2 log records",
                log_record_reason.to_string(),
                [StatusCode::OK, StatusCode::ACCEPTED],
            ),
            (
                Route::GrpcLogs,
                logs_request(true),
                "1 of 2 log records",
                log_record_reason.to_string(),
                [StatusCode::OK, StatusCode::OK],
            ),
            (
                Route::OtlpLogs,
                logs_stream_request(),
                "1 of 2 streams",
                otlp_stream_reason.clone(),
                [StatusCode::BAD_REQUEST, StatusCode::BAD_REQUEST],
            ),
            (
                Route::Loki,
                loki_request(true),
                "1 of 2 streams",
                loki_stream_reason,
                [StatusCode::BAD_REQUEST, StatusCode::BAD_REQUEST],
            ),
            (
                Route::OtlpTraces,
                traces_request(true),
                "1 of 2 spans",
                span_reason.to_string(),
                [StatusCode::OK, StatusCode::ACCEPTED],
            ),
            (
                Route::GrpcTraces,
                traces_request(true),
                "1 of 2 spans",
                span_reason.to_string(),
                [StatusCode::OK, StatusCode::OK],
            ),
        ];
        for (route, body, counts, reason, statuses) in cases {
            for (mode, status) in [
                (None, statuses[0]),
                (Some(("x-pulsus-async", "1")), statuses[1]),
            ] {
                let router = router();
                let (captured, _guard) = capture();
                let answer = push(&router, route, body.clone(), mode.as_slice()).await;
                assert_eq!(answer.status(), status, "{} {mode:?}", route.path());
                assert_eq!(
                    captured.lines(),
                    vec![expected(route, counts, &reason)],
                    "{} {mode:?}",
                    route.path()
                );
            }
        }
    }

    /// A push with nothing refused writes nothing, on every route.
    #[tokio::test]
    async fn a_push_with_nothing_refused_logs_nothing() {
        let router = router();
        let (captured, _guard) = capture();
        for (route, body) in [
            (Route::OtlpMetrics, metrics_request(false)),
            (Route::GrpcMetrics, metrics_request(false)),
            (Route::RemoteWrite, remote_write_request(false)),
            (Route::OtlpLogs, logs_request(false)),
            (Route::GrpcLogs, logs_request(false)),
            (Route::Loki, loki_request(false)),
            (Route::OtlpTraces, traces_request(false)),
            (Route::GrpcTraces, traces_request(false)),
        ] {
            let answer = push(&router, route, body, &[]).await;
            assert!(answer.status().is_success(), "{}", route.path());
        }
        assert_eq!(captured.lines(), Vec::<String>::new());
    }

    /// A sender stuck on one bad metric gets one line, not one a push; and
    /// the forwarded-for header is named beside the connection's address.
    #[tokio::test]
    async fn the_same_refusal_again_within_a_minute_is_not_logged() {
        let router = router();
        let (captured, _guard) = capture();
        for _ in 0..3 {
            let answer = push(
                &router,
                Route::OtlpMetrics,
                metrics_request(true),
                &[("x-forwarded-for", "198.51.100.3")],
            )
            .await;
            assert_eq!(answer.status(), StatusCode::OK);
        }
        assert_eq!(
            captured.lines(),
            vec![format!(
                "WARN metrics push on /v1/metrics: 2 of 3 data points rejected ({DELTA_REASON}) \
                 from {PEER} (X-Forwarded-For \"198.51.100.3\"), user agent \"{AGENT}\""
            )]
        );
    }

    #[test]
    fn a_route_and_reason_log_once_a_minute_and_count_what_they_held() {
        let log = RefusalLog::new();
        let start = Instant::now();
        let at = |secs| start + Duration::from_secs(secs);
        assert_eq!(log.admit("/v1/metrics", "a", at(0)), Some(0));
        assert_eq!(log.admit("/v1/metrics", "a", at(1)), None);
        assert_eq!(log.admit("/v1/metrics", "a", at(59)), None);
        // Another reason, and the same reason on another route, have their
        // own minute.
        assert_eq!(log.admit("/v1/metrics", "b", at(1)), Some(0));
        assert_eq!(log.admit("/api/v1/write", "a", at(1)), Some(0));
        assert_eq!(log.admit("/v1/metrics", "a", at(60)), Some(2));
        assert_eq!(log.admit("/v1/metrics", "a", at(61)), None);
        assert_eq!(log.admit("/v1/metrics", "a", at(120)), Some(1));
    }

    /// Reasons that embed a value: past [`MAX_REASONS`], new ones share one
    /// line a minute for their route.
    #[test]
    fn reasons_past_the_limit_share_one_line_a_minute() {
        let log = RefusalLog::new();
        let now = Instant::now();
        for i in 0..MAX_REASONS {
            assert_eq!(log.admit("/v1/logs", &format!("t={i}"), now), Some(0));
        }
        assert_eq!(log.admit("/v1/logs", "t=first-past", now), Some(0));
        assert_eq!(log.admit("/v1/logs", "t=second-past", now), None);
        // A minute on, the old entries make room again.
        let later = now + WINDOW;
        assert_eq!(log.admit("/v1/logs", "t=new", later), Some(0));
    }

    /// Making room in a full table never drops a count a later line has
    /// still to report.
    #[test]
    fn making_room_keeps_the_pushes_a_reason_held_back() {
        let log = RefusalLog::new();
        let now = Instant::now();
        for i in 0..MAX_REASONS {
            assert_eq!(log.admit("/v1/logs", &format!("t={i}"), now), Some(0));
        }
        let soon = now + Duration::from_secs(1);
        assert_eq!(log.admit("/v1/logs", "t=0", soon), None);
        assert_eq!(log.admit("/v1/logs", "t=0", soon), None);
        // A minute on, a new reason makes room; `t=0` still holds two.
        let later = now + WINDOW;
        assert_eq!(log.admit("/v1/logs", "t=new", later), Some(0));
        assert_eq!(log.admit("/v1/logs", "t=0", later), Some(2));
    }

    #[test]
    fn the_line_says_how_many_pushes_it_stands_for() {
        let refused = RefusedPush {
            signal: "traces",
            unit: "spans",
            refused: 4,
            total: 10,
            reason: "bad span".to_string(),
            user_agent: None,
            forwarded_for: None,
        };
        assert_eq!(
            line(&refused, "/v1/traces", None, 6),
            "traces push on /v1/traces: 4 of 10 spans rejected (bad span) from an unknown \
             address; 6 more pushes like it were not logged"
        );
    }

    /// Over a TLS listener, as `run` serves it, the line names the
    /// connection's address and the gRPC client's user agent.
    #[tokio::test]
    async fn the_line_names_the_address_of_a_tls_connection() {
        use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
        use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

        let (tls_config, pem) = crate::tls::test_server_config();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("an address");
        let service = with_peer(router());
        let server = tokio::spawn(async move {
            let _ = axum::serve(TlsListener::new(listener, tls_config), service).await;
        });
        let (captured, _guard) = capture();
        let channel = Endpoint::from_shared(format!("https://127.0.0.1:{}", addr.port()))
            .expect("an endpoint")
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(pem))
                    .domain_name("localhost"),
            )
            .expect("a TLS config")
            .connect_lazy();
        let request = tonic::Request::new(
            ExportMetricsServiceRequest::decode(metrics_request(true).as_slice()).expect("decodes"),
        );
        MetricsServiceClient::new(channel)
            .export(request)
            .await
            .expect("the export succeeds");
        server.abort();

        let lines = captured.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        let prefix = format!(
            "WARN metrics push on /opentelemetry.proto.collector.metrics.v1.MetricsService/Export: \
             2 of 3 data points rejected ({DELTA_REASON}) from 127.0.0.1:"
        );
        assert!(lines[0].starts_with(&prefix), "{}", lines[0]);
        assert!(lines[0].contains(", user agent \"tonic/"), "{}", lines[0]);
    }

    /// Over a plain listener, as `run` serves it, the line names the
    /// connection's address.
    #[tokio::test]
    async fn the_line_names_the_address_of_a_plain_connection() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("an address");
        let service = with_peer(router());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, service).await;
        });
        let (captured, _guard) = capture();
        let body = traces_request(true);
        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        let local = stream.local_addr().expect("the client's address");
        let head = format!(
            "POST /v1/traces HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-protobuf\r\n\
             X-Pulsus-Async: 1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.expect("write");
        stream.write_all(&body).await.expect("write");
        let mut answer = Vec::new();
        stream.read_to_end(&mut answer).await.expect("read");
        server.abort();

        assert!(
            answer.starts_with(b"HTTP/1.1 202"),
            "{}",
            String::from_utf8_lossy(&answer)
        );
        assert_eq!(
            captured.lines(),
            vec![format!(
                "WARN traces push on /v1/traces: 1 of 2 spans rejected (span \"bad\": trace_id \
                 must be exactly 16 bytes, got 3) from {local}"
            )]
        );
    }
}
