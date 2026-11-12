//! Request counts and request durations per route (issue #838):
//! `pulsus_http_requests_total{route, status}` and
//! `pulsus_http_request_duration_seconds{route}`, rendered onto `/metrics`
//! by [`RequestMetrics::render_into`].

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicU16, AtomicU64};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::BoxError;
use axum::body::{Body, Bytes, HttpBody};
use axum::extract::MatchedPath;
use axum::http::{Request, Response};
use tower::Service;

pub(crate) const UNMATCHED: &str = "unmatched";
const STATUS_CELLS: usize = 24;

/// Every mounted route template, in byte order (binary-searched).
/// Equal to the manifest's `Mounted` set: tests/route_inventory.rs check 4.
pub(crate) const ROUTE_TEMPLATES: &[&str] = &[];

struct StatusCell {
    code: AtomicU16,
    count: AtomicU64,
}

struct RouteCells {
    statuses: [StatusCell; STATUS_CELLS],
    other: AtomicU64,
    buckets: [AtomicU64; 14],
    sum_us: AtomicU64,
}

const FREE_CELL: StatusCell = StatusCell {
    code: AtomicU16::new(0),
    count: AtomicU64::new(0),
};
const ZERO: AtomicU64 = AtomicU64::new(0);
const EMPTY_ROUTE: RouteCells = RouteCells {
    statuses: [FREE_CELL; STATUS_CELLS],
    other: AtomicU64::new(0),
    buckets: [ZERO; 14],
    sum_us: AtomicU64::new(0),
};

pub(crate) struct RequestMetrics {
    routes: [RouteCells; ROUTE_TEMPLATES.len() + 1],
}

impl RequestMetrics {
    /// One table per AppState, leaked so the per-request handle is a plain `&'static`.
    pub(crate) fn leak_new() -> &'static Self {
        Box::leak(Box::new(RequestMetrics {
            routes: [EMPTY_ROUTE; ROUTE_TEMPLATES.len() + 1],
        }))
    }

    fn route_cells(&'static self, _matched: Option<&MatchedPath>) -> &'static RouteCells {
        &self.routes[ROUTE_TEMPLATES.len()]
    }

    /// Appends both families; writes nothing for a family with no request yet.
    pub(crate) fn render_into(&self, _out: &mut String) {}
}

impl RouteCells {
    fn record(&self, _status: u16, _elapsed: Duration) {}

    #[cfg(test)]
    fn set_sum_us(&self, _value: u64) {}
}

#[derive(Clone, Copy)]
pub(crate) struct RequestMetricsLayer {
    table: &'static RequestMetrics,
}

impl RequestMetricsLayer {
    pub(crate) fn new(table: &'static RequestMetrics) -> Self {
        Self { table }
    }
}

impl<S> tower::Layer<S> for RequestMetricsLayer {
    type Service = RequestMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestMetricsService {
            inner,
            table: self.table,
        }
    }
}

#[derive(Clone)]
pub(crate) struct RequestMetricsService<S> {
    inner: S,
    table: &'static RequestMetrics,
}

impl<S, B> Service<Request<Body>> for RequestMetricsService<S>
where
    S: Service<Request<Body>, Response = Response<B>, Error = Infallible>,
    B: HttpBody<Data = Bytes>,
    B::Error: Into<BoxError>,
{
    type Response = Response<TimedBody<B>>;
    type Error = Infallible;
    type Future = Timed<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let start = Instant::now();
        let route = self
            .table
            .route_cells(req.extensions().get::<MatchedPath>());
        let inner = self.inner.call(req);
        Timed {
            inner,
            pending: Some((route, start)),
        }
    }
}

pin_project_lite::pin_project! {
    pub(crate) struct Timed<F> {
        #[pin]
        inner: F,
        pending: Option<(&'static RouteCells, Instant)>,
    }
}

impl<F, B> Future for Timed<F>
where
    F: Future<Output = Result<Response<B>, Infallible>>,
{
    type Output = Result<Response<TimedBody<B>>, Infallible>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match std::task::ready!(this.inner.poll(cx)) {
            Ok(res) => {
                let (route, start) = this.pending.take().expect("Timed polled after completion");
                let status = res.status().as_u16();
                Poll::Ready(Ok(res.map(|inner| TimedBody {
                    inner,
                    route,
                    start,
                    status,
                })))
            }
            Err(never) => match never {},
        }
    }
}

pin_project_lite::pin_project! {
    pub(crate) struct TimedBody<B> {
        #[pin]
        inner: B,
        route: &'static RouteCells,
        start: Instant,
        status: u16,
    }
}

impl<B: HttpBody> HttpBody for TimedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.project().inner.poll_frame(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::body::to_bytes;
    use axum::extract::Query;
    use axum::extract::ws::WebSocketUpgrade;
    use axum::http::StatusCode;
    use axum::routing::get;
    use futures::StreamExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tower::ServiceExt;

    use super::*;

    fn render(t: &RequestMetrics) -> String {
        let mut s = String::new();
        t.render_into(&mut s);
        s
    }

    fn has_line(s: &str, line: &str) -> bool {
        s.lines().any(|l| l == line)
    }

    /// The number after `<series> ` on the line that starts with it.
    fn value_of(s: &str, series: &str) -> Option<f64> {
        s.lines()
            .find_map(|l| l.strip_prefix(series)?.strip_prefix(' '))
            .map(|v| v.parse().expect("a number"))
    }

    /// Sends one request and reads its body to the end.
    async fn send(router: &Router, method: &str, uri: &str) -> StatusCode {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap();
        let res = router.clone().oneshot(req).await.unwrap();
        let status = res.status();
        to_bytes(res.into_body(), usize::MAX).await.unwrap();
        status
    }

    fn unmatched_route(t: &'static RequestMetrics) -> &'static RouteCells {
        &t.routes[ROUTE_TEMPLATES.len()]
    }

    #[tokio::test]
    async fn counts_each_request_by_route_template_and_status() {
        let t = RequestMetrics::leak_new();
        let app = Router::new()
            .route("/api/v1/query", get(|| async { "ok" }))
            .route(
                "/api/v1/label/{name}/values",
                get(|| async { StatusCode::BAD_REQUEST }),
            )
            .layer(RequestMetricsLayer::new(t));
        for _ in 0..3 {
            send(&app, "GET", "/api/v1/query").await;
        }
        for _ in 0..2 {
            send(&app, "GET", "/api/v1/label/job/values").await;
        }
        send(&app, "GET", "/api/v1/label/instance/values").await;
        let s = render(t);
        assert!(
            has_line(
                &s,
                r#"pulsus_http_requests_total{route="/api/v1/query",status="200"} 3"#
            ),
            "{s}"
        );
        assert!(
            has_line(
                &s,
                r#"pulsus_http_requests_total{route="/api/v1/label/{name}/values",status="400"} 3"#
            ),
            "{s}"
        );
        assert!(
            has_line(
                &s,
                r#"pulsus_http_request_duration_seconds_count{route="/api/v1/query"} 3"#
            ),
            "{s}"
        );
        assert!(
            !s.contains("label/job") && !s.contains("label/instance"),
            "{s}"
        );
    }

    #[tokio::test]
    async fn unmatched_paths_count_under_one_value_whatever_answers_them() {
        let t = RequestMetrics::leak_new();
        let app = Router::new()
            .route("/api/v1/query", get(|| async { "ok" }))
            .layer(crate::middleware::cors_layer(&pulsus_config::Config::default()).unwrap())
            .layer(RequestMetricsLayer::new(t));
        for path in ["/nope/1", "/nope/2", "/api/v1/querry"] {
            assert_eq!(send(&app, "GET", path).await, StatusCode::NOT_FOUND);
        }
        let preflight = Request::builder()
            .method("OPTIONS")
            .uri("/nope/3")
            .header("Origin", "http://a.test")
            .header("Access-Control-Request-Method", "GET")
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(preflight).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let s = render(t);
        assert!(
            has_line(
                &s,
                r#"pulsus_http_requests_total{route="unmatched",status="404"} 3"#
            ),
            "{s}"
        );
        assert!(
            has_line(
                &s,
                r#"pulsus_http_requests_total{route="unmatched",status="200"} 1"#
            ),
            "{s}"
        );
        assert!(
            !s.contains(r#"route="/nope"#) && !s.contains("querry"),
            "{s}"
        );
    }

    #[tokio::test]
    async fn a_method_the_route_lacks_counts_under_its_template() {
        let t = RequestMetrics::leak_new();
        let app = Router::new()
            .route("/api/v1/query", get(|| async { "ok" }))
            .layer(RequestMetricsLayer::new(t));
        assert_eq!(
            send(&app, "POST", "/api/v1/query").await,
            StatusCode::METHOD_NOT_ALLOWED
        );
        let s = render(t);
        assert!(
            has_line(
                &s,
                r#"pulsus_http_requests_total{route="/api/v1/query",status="405"} 1"#
            ),
            "{s}"
        );
        assert!(!s.contains(UNMATCHED), "{s}");
    }

    #[test]
    fn templates_are_byte_ordered_and_need_no_escaping() {
        assert!(
            ROUTE_TEMPLATES.windows(2).all(|w| w[0] < w[1]),
            "{ROUTE_TEMPLATES:?}"
        );
        assert_eq!(ROUTE_TEMPLATES.len(), 80);
        assert!(
            ROUTE_TEMPLATES
                .iter()
                .all(|t| !t.contains('"') && !t.contains('\\'))
        );
    }

    #[tokio::test]
    async fn a_streamed_body_is_timed_until_it_is_produced() {
        let t = RequestMetrics::leak_new();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let rx = Arc::new(Mutex::new(Some(rx)));
        let app = Router::new()
            .route(
                "/api/v1/query_range",
                get(move || {
                    let rx = rx.lock().unwrap().take().expect("one request");
                    async move {
                        let first = futures::stream::once(async {
                            Ok::<_, Infallible>(Bytes::from_static(b"a"))
                        });
                        let rest = futures::stream::once(async move {
                            let _ = rx.await;
                            Ok::<_, Infallible>(Bytes::from_static(b"b"))
                        });
                        Body::from_stream(first.chain(rest))
                    }
                }),
            )
            .layer(RequestMetricsLayer::new(t));
        let req = Request::builder()
            .uri("/api/v1/query_range")
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        let s = render(t);
        assert!(!s.contains("pulsus_http_"), "recorded at the head:\n{s}");
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send(()).unwrap();
        to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let s = render(t);
        assert!(
            has_line(
                &s,
                r#"pulsus_http_requests_total{route="/api/v1/query_range",status="200"} 1"#
            ),
            "{s}"
        );
        assert!(
            has_line(
                &s,
                r#"pulsus_http_request_duration_seconds_bucket{route="/api/v1/query_range",le="0.1"} 0"#
            ),
            "{s}"
        );
        assert!(
            has_line(
                &s,
                r#"pulsus_http_request_duration_seconds_count{route="/api/v1/query_range"} 1"#
            ),
            "{s}"
        );
        let sum = value_of(
            &s,
            r#"pulsus_http_request_duration_seconds_sum{route="/api/v1/query_range"}"#,
        );
        assert!(sum.is_some_and(|v| v >= 0.2), "{s}");
    }

    #[tokio::test]
    async fn a_request_cancelled_before_its_response_counts_as_499() {
        let t = RequestMetrics::leak_new();
        let app = Router::new()
            .route("/api/v1/query", get(std::future::pending::<&'static str>))
            .layer(RequestMetricsLayer::new(t));
        let req = Request::builder()
            .uri("/api/v1/query")
            .body(Body::empty())
            .unwrap();
        let cancelled = tokio::time::timeout(Duration::from_millis(50), app.oneshot(req)).await;
        assert!(cancelled.is_err(), "the handler never answers");
        let s = render(t);
        assert!(
            has_line(
                &s,
                r#"pulsus_http_requests_total{route="/api/v1/query",status="499"} 1"#
            ),
            "{s}"
        );
        assert!(
            has_line(
                &s,
                r#"pulsus_http_request_duration_seconds_count{route="/api/v1/query"} 1"#
            ),
            "{s}"
        );
    }

    #[tokio::test]
    async fn a_websocket_upgrade_is_timed_to_the_101_not_the_session() {
        let t = RequestMetrics::leak_new();
        let app = Router::new()
            .route(
                "/api/logs/v1/tail",
                get(|ws: WebSocketUpgrade| async move {
                    ws.on_upgrade(|_s| std::future::pending::<()>())
                }),
            )
            .layer(RequestMetricsLayer::new(t));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        sock.write_all(
            b"GET /api/logs/v1/tail HTTP/1.1\r\nHost: test\r\nConnection: Upgrade\r\n\
              Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n\
              Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
        )
        .await
        .unwrap();
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            assert_eq!(sock.read(&mut byte).await.unwrap(), 1, "connection closed");
            head.push(byte[0]);
        }
        assert!(
            head.starts_with(b"HTTP/1.1 101"),
            "{}",
            String::from_utf8_lossy(&head)
        );
        let line = r#"pulsus_http_requests_total{route="/api/logs/v1/tail",status="101"} 1"#;
        let mut s = render(t);
        for _ in 0..200 {
            if has_line(&s, line) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            s = render(t);
        }
        assert!(has_line(&s, line), "{s}");
        let sum = value_of(
            &s,
            r#"pulsus_http_request_duration_seconds_sum{route="/api/logs/v1/tail"}"#,
        );
        assert!(sum.is_some_and(|v| v < 1.0), "{s}");
        // The socket stays open until here: the session is still running.
        drop(sock);
        server.abort();
    }

    #[test]
    fn render_writes_cumulative_buckets_and_skips_empty_families() {
        let t = RequestMetrics::leak_new();
        assert_eq!(render(t), "");
        let route = unmatched_route(t);
        route.record(200, Duration::from_millis(30));
        route.record(200, Duration::from_secs(3));
        route.record(500, Duration::from_secs(200));
        let s = render(t);
        let bucket = |le: &str| {
            format!(r#"pulsus_http_request_duration_seconds_bucket{{route="unmatched",le="{le}"}}"#)
        };
        for le in ["0.005", "0.01", "0.025"] {
            assert!(has_line(&s, &format!("{} 0", bucket(le))), "{le}\n{s}");
        }
        for le in ["0.05", "0.1", "0.25", "0.5", "1", "2.5"] {
            assert!(has_line(&s, &format!("{} 1", bucket(le))), "{le}\n{s}");
        }
        for le in ["5", "10", "30", "120"] {
            assert!(has_line(&s, &format!("{} 2", bucket(le))), "{le}\n{s}");
        }
        assert!(has_line(&s, &format!("{} 3", bucket("+Inf"))), "{s}");
        assert!(
            has_line(
                &s,
                r#"pulsus_http_request_duration_seconds_count{route="unmatched"} 3"#
            ),
            "{s}"
        );
        assert!(
            has_line(
                &s,
                r#"pulsus_http_request_duration_seconds_sum{route="unmatched"} 203.030000"#
            ),
            "{s}"
        );
        assert!(
            has_line(
                &s,
                r#"pulsus_http_requests_total{route="unmatched",status="200"} 2"#
            ),
            "{s}"
        );
        assert!(
            has_line(
                &s,
                r#"pulsus_http_requests_total{route="unmatched",status="500"} 1"#
            ),
            "{s}"
        );
        let types: Vec<&str> = s.lines().filter(|l| l.starts_with("# TYPE")).collect();
        assert_eq!(
            types,
            [
                "# TYPE pulsus_http_requests_total counter",
                "# TYPE pulsus_http_request_duration_seconds histogram",
            ],
            "{s}"
        );
    }

    #[tokio::test]
    async fn statuses_past_the_24th_count_as_other() {
        let t = RequestMetrics::leak_new();
        let app = Router::new()
            .route(
                "/api/v1/query",
                get(|Query(q): Query<HashMap<String, String>>| async move {
                    StatusCode::from_u16(q["s"].parse().unwrap()).unwrap()
                }),
            )
            .layer(RequestMetricsLayer::new(t));
        for code in 200..=224 {
            send(&app, "GET", &format!("/api/v1/query?s={code}")).await;
        }
        let s = render(t);
        let counters: Vec<&str> = s
            .lines()
            .filter(|l| l.starts_with("pulsus_http_requests_total{"))
            .collect();
        let mut expected: Vec<String> = (200..224)
            .map(|c| {
                format!(r#"pulsus_http_requests_total{{route="/api/v1/query",status="{c}"}} 1"#)
            })
            .collect();
        expected
            .push(r#"pulsus_http_requests_total{route="/api/v1/query",status="other"} 1"#.into());
        assert_eq!(counters, expected, "{s}");
    }

    #[test]
    fn concurrent_first_use_of_one_status_takes_one_cell() {
        let t = RequestMetrics::leak_new();
        let route = unmatched_route(t);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..1_000 {
                        route.record(200, Duration::from_millis(1));
                        route.record(404, Duration::from_millis(1));
                    }
                });
            }
        });
        let s = render(t);
        let mut counters: Vec<&str> = s
            .lines()
            .filter(|l| l.starts_with("pulsus_http_requests_total{"))
            .collect();
        counters.sort_unstable();
        assert_eq!(
            counters,
            [
                r#"pulsus_http_requests_total{route="unmatched",status="200"} 8000"#,
                r#"pulsus_http_requests_total{route="unmatched",status="404"} 8000"#,
            ],
            "{s}"
        );
        assert!(
            has_line(
                &s,
                r#"pulsus_http_request_duration_seconds_count{route="unmatched"} 16000"#
            ),
            "{s}"
        );
    }

    #[test]
    fn the_sum_is_whole_microseconds_rendered_exactly_up_to_its_bound() {
        let t = RequestMetrics::leak_new();
        let first = unmatched_route(t);
        first.record(200, Duration::from_nanos(999));
        first.record(200, Duration::from_nanos(1_000));
        first.record(200, Duration::from_nanos(1_999));
        first.record(200, Duration::from_secs(120));
        let second = &t.routes[0];
        second.record(200, Duration::from_millis(1));
        second.set_sum_us(u64::MAX);
        let second_label = ROUTE_TEMPLATES.first().copied().unwrap_or(UNMATCHED);
        let s = render(t);
        assert!(
            has_line(
                &s,
                r#"pulsus_http_request_duration_seconds_sum{route="unmatched"} 120.000002"#
            ),
            "{s}"
        );
        assert!(
            has_line(
                &s,
                r#"pulsus_http_request_duration_seconds_bucket{route="unmatched",le="120"} 4"#
            ),
            "{s}"
        );
        assert!(
            has_line(
                &s,
                r#"pulsus_http_request_duration_seconds_bucket{route="unmatched",le="30"} 3"#
            ),
            "{s}"
        );
        assert!(
            has_line(
                &s,
                &format!(
                    r#"pulsus_http_request_duration_seconds_sum{{route="{second_label}"}} 18446744073709.551615"#
                )
            ),
            "{s}"
        );
    }

    #[tokio::test]
    async fn the_clock_starts_before_the_inner_call() {
        let t = RequestMetrics::leak_new();
        let svc = tower::service_fn(|_req: Request<Body>| {
            std::thread::sleep(Duration::from_millis(100));
            std::future::ready(Ok::<_, Infallible>(Response::new(Body::empty())))
        });
        let svc = tower::Layer::layer(&RequestMetricsLayer::new(t), svc);
        let req = Request::builder().uri("/x").body(Body::empty()).unwrap();
        let res = svc.oneshot(req).await.unwrap();
        to_bytes(Body::new(res.into_body()), usize::MAX)
            .await
            .unwrap();
        let s = render(t);
        let sum = value_of(
            &s,
            r#"pulsus_http_request_duration_seconds_sum{route="unmatched"}"#,
        );
        assert!(sum.is_some_and(|v| v >= 0.1), "{s}");
        assert!(
            has_line(
                &s,
                r#"pulsus_http_request_duration_seconds_bucket{route="unmatched",le="0.05"} 0"#
            ),
            "{s}"
        );
    }

    #[tokio::test]
    async fn the_wrapper_keeps_the_body_size() {
        let t = RequestMetrics::leak_new();
        let app = Router::new()
            .route("/x", get(|| async { "ok" }))
            .layer(RequestMetricsLayer::new(t));
        let req = Request::builder().uri("/x").body(Body::empty()).unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.body().size_hint().exact(), Some(2));
    }
}
