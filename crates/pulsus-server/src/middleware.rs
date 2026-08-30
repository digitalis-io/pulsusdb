//! Per-layer middleware builders (issue #6 architect plan + amendment).
//! Each builder is small and independently testable; `app::build_router`
//! decides *where* every layer is applied (the amendment's F1/F2 split —
//! public ops sit outside both auth and the generic timeout).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderValue, Request, Response, StatusCode, header};
use tower_http::classify::{ServerErrorsAsFailures, SharedClassifier};
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tower_http::validate_request::{ValidateRequest, ValidateRequestHeaderLayer};

use pulsus_config::Config;

use crate::serve::ServeError;

/// Request/response tracing span for every route it wraps (applied
/// globally in `app::build_router`, including 404s).
pub(crate) fn trace_layer() -> TraceLayer<SharedClassifier<ServerErrorsAsFailures>> {
    TraceLayer::new_for_http()
}

/// Gzip response compression (applied globally).
pub(crate) fn compression_layer() -> CompressionLayer {
    CompressionLayer::new()
}

/// `Access-Control-Allow-Origin` per `PULSUS_CORS_ORIGIN` (applied
/// globally). `*` maps to [`Any`]; a concrete origin is validated as an
/// HTTP header value here — an invalid `PULSUS_CORS_ORIGIN` must fail
/// startup with a clear error, never panic mid-request (architect plan
/// edge case).
pub(crate) fn cors_layer(config: &Config) -> Result<CorsLayer, ServeError> {
    let layer = CorsLayer::new().allow_methods(Any).allow_headers(Any);
    let layer = if config.cors_origin == "*" {
        layer.allow_origin(Any)
    } else {
        let origin = HeaderValue::from_str(&config.cors_origin)
            .map_err(|_| ServeError::InvalidCorsOrigin(config.cors_origin.clone()))?;
        layer.allow_origin(origin)
    };
    Ok(layer)
}

/// Paths under `/api/v1/` that KEEP the bare `408` (issue #471 M2). Read
/// by [`deadline_class`] and by `tests/deadline_partition.rs`, which
/// source-scans this constant and cross-checks it against the route
/// manifest in both directions.
///
/// Exactly one entry, and it is the reason the rule cannot be a bare
/// prefix test: `/api/v1/write` is remote-write **ingest**, not a query
/// surface, and a query-shaped JSON error envelope would be wrong on it.
pub(crate) const DEADLINE_BARE_PATHS: &[&str] = &["/api/v1/write"];

/// How a request-deadline breach is answered on a given path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeadlineClass {
    /// `503` + the three-field JSON error envelope (`prom_api`'s).
    PromApiEnvelope,
    /// The bare `408`: status only, no headers, empty body — byte-identical
    /// to what `tower_http`'s `Timeout` built before issue #471 M2
    /// (`tower-http-0.6.11/src/timeout/service.rs:144-146`:
    /// `Response::new(B::default())` plus a status, nothing else).
    Bare,
}

/// Total over the `/api/v1/` prefix by construction (issue #471 M2): a
/// query route added later cannot land on the wrong side, and the only way
/// to get the bare answer under that prefix is to be named in
/// [`DEADLINE_BARE_PATHS`]. Everything outside `/api/v1/` — the LogQL and
/// TraceQL surfaces and their compat aliases, the OTLP receivers, the ops
/// routes — is [`DeadlineClass::Bare`] by construction; those surfaces
/// write bare `text/plain` error bodies by decision (issue #264) and must
/// stay byte-identical.
///
/// Cheap: one `starts_with` plus at most `DEADLINE_BARE_PATHS.len()`
/// string compares, no allocation.
pub(crate) fn deadline_class(path: &str) -> DeadlineClass {
    if path.starts_with("/api/v1/") && !DEADLINE_BARE_PATHS.contains(&path) {
        DeadlineClass::PromApiEnvelope
    } else {
        DeadlineClass::Bare
    }
}

/// The hard per-request deadline for data-plane routes plus `/config` and
/// `/buildinfo` — never `/ready`/`/metrics` (amendment F2, applied by
/// `app::build_router`'s composition, not by this function). The same
/// `PULSUS_QUERY_TIMEOUT` also drives ClickHouse's `max_execution_time`, so
/// client and server never split-brain on which side gives up first.
///
/// Issue #471 M2 replaced the `tower_http::timeout` layer this server
/// used before with
/// [`RequestDeadlineLayer`]. The third-party layer answered every path a
/// bare `408` with an empty body and no `Content-Type`, which falls outside
/// the status set a client parses a body for, so a slow query rendered as
/// an error box with nothing in it. The replacement answers the PromQL
/// query surface `503` + the JSON envelope and keeps the byte-identical
/// bare `408` everywhere else — a **path test**, not a router-structural
/// change: applying a layer to `prom_api::router()` alone would leave this
/// one still wrapping those routes at the same duration, so two deadlines
/// would race and the observed status would be nondeterministic.
///
/// The call expression in `app::build_router` is unchanged; only this
/// function's return type moves.
pub(crate) fn timeout_layer(config: &Config) -> RequestDeadlineLayer {
    RequestDeadlineLayer {
        timeout: config.query_timeout.0,
    }
}

/// [`tower::Layer`] for [`RequestDeadline`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestDeadlineLayer {
    timeout: Duration,
}

impl<S> tower::Layer<S> for RequestDeadlineLayer {
    type Service = RequestDeadline<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestDeadline {
            inner,
            timeout: self.timeout,
        }
    }
}

/// The per-request deadline service (issue #471 M2).
#[derive(Debug, Clone)]
pub(crate) struct RequestDeadline<S> {
    inner: S,
    timeout: Duration,
}

impl<S> tower::Service<Request<Body>> for RequestDeadline<S>
where
    S: tower::Service<Request<Body>, Response = Response<Body>, Error = std::convert::Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let class = deadline_class(req.uri().path());
        let timeout = self.timeout;
        // The standard tower ready-service swap: `poll_ready` was called
        // on `self.inner`, so the readiness belongs to *that* value and the
        // clone must be the one left behind, never the one driven.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let call = inner.call(req);
        Box::pin(async move {
            let mut call = Box::pin(call);
            let mut sleep = Box::pin(tokio::time::sleep(timeout));
            std::future::poll_fn(move |cx| {
                // **The deadline is polled BEFORE the inner service, and
                // the order is load-bearing.** All four
                // `PULSUS_QUERY_TIMEOUT`-fed clocks carry the same
                // duration — this layer, the ClickHouse stream deadline,
                // the pool-permit wait and ClickHouse's own
                // `max_execution_time` — so at the deadline the inner
                // future is often ready with its OWN timeout error in the
                // same timer tick. Whichever is polled first wins.
                //
                // `tower_http`'s `Timeout` polled its sleep first
                // (`tower-http-0.6.11/src/timeout/service.rs:143-147`),
                // so this layer's answer won those ties before issue
                // #471. Using `tokio::time::timeout` here instead — which
                // polls the value first — flipped that, and it was
                // measured doing so: a stalled `/api/logs/v1/labels`
                // answered `504` + `clickhouse: timeout: query_stream
                // exceeded 3s` where it had answered the bare `408`. The
                // LogQL and TraceQL surfaces must stay byte-identical
                // (issue #264), so the ordering is reproduced exactly.
                if sleep.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Ok(match class {
                        DeadlineClass::PromApiEnvelope => {
                            crate::prom_api::deadline_response(timeout)
                        }
                        DeadlineClass::Bare => {
                            let mut res = Response::new(Body::default());
                            *res.status_mut() = StatusCode::REQUEST_TIMEOUT;
                            res
                        }
                    }));
                }
                call.as_mut().poll(cx)
            })
            .await
        })
    }
}

/// HTTP Basic auth wrapping the data-plane + `/config`/`/buildinfo` group
/// only (amendment F1, applied by `app::build_router`) — `None` unless both
/// `PULSUS_AUTH_USER` and `PULSUS_AUTH_PASSWORD` are set. Built on
/// `ValidateRequestHeaderLayer::custom` rather than tower-http's own
/// `::basic` constructor, which has been deprecated since tower-http 0.6.7.
pub(crate) fn auth_layer(config: &Config) -> Option<ValidateRequestHeaderLayer<BasicAuth>> {
    let user = config.auth_user.as_deref()?;
    let password = config.auth_password.as_ref()?;
    let credentials = format!("{user}:{}", password.expose());
    let expected = format!("Basic {}", base64_encode(credentials.as_bytes()));
    Some(ValidateRequestHeaderLayer::custom(BasicAuth { expected }))
}

/// Validates the `Authorization` header against a precomputed `Basic
/// <base64>` value computed once at layer-build time. A plain string
/// comparison (not constant-time) is judged acceptable for this
/// operator-facing M0 auth gate; hardening against timing side-channels is
/// out of scope here.
#[derive(Clone)]
pub(crate) struct BasicAuth {
    expected: String,
}

impl<B> ValidateRequest<B> for BasicAuth {
    type ResponseBody = Body;

    fn validate(&mut self, request: &mut Request<B>) -> Result<(), Response<Self::ResponseBody>> {
        let matches = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == self.expected);
        if matches {
            return Ok(());
        }
        let mut res = Response::new(Body::from("unauthorized"));
        *res.status_mut() = StatusCode::UNAUTHORIZED;
        res.headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Basic"));
        Err(res)
    }
}

/// Minimal RFC 4648 standard base64 encoder (with padding), used only to
/// build the expected `Authorization: Basic <...>` value at startup.
/// Hand-rolled to avoid a new dependency: tower-http's own basic-auth
/// helper pulls in the `base64` crate transitively but does not re-export
/// it. `pub(crate)` so `app`'s full-router auth-matrix test can compute the
/// same expected header value without duplicating this logic.
pub(crate) fn base64_encode(input: &[u8]) -> String {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied();
        let b2 = chunk.get(2).copied();
        let n =
            (u32::from(b0) << 16) | (u32::from(b1.unwrap_or(0)) << 8) | u32::from(b2.unwrap_or(0));
        out.push(CHARS[((n >> 18) & 0x3F) as usize] as char);
        out.push(CHARS[((n >> 12) & 0x3F) as usize] as char);
        out.push(if b1.is_some() {
            CHARS[((n >> 6) & 0x3F) as usize] as char
        } else {
            '='
        });
        out.push(if b2.is_some() {
            CHARS[(n & 0x3F) as usize] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_config::Secret;

    #[test]
    fn base64_encode_matches_the_rfc_7617_worked_example() {
        assert_eq!(
            base64_encode(b"Aladdin:open sesame"),
            "QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
        );
    }

    #[test]
    fn base64_encode_pads_single_and_double_byte_remainders() {
        assert_eq!(base64_encode(b"a"), "YQ==");
        assert_eq!(base64_encode(b"ab"), "YWI=");
        assert_eq!(base64_encode(b"abc"), "YWJj");
    }

    #[test]
    fn auth_layer_is_none_when_neither_credential_is_set() {
        assert!(auth_layer(&Config::default()).is_none());
    }

    #[test]
    fn auth_layer_is_none_when_only_the_user_is_set() {
        let cfg = Config {
            auth_user: Some("alice".to_string()),
            ..Config::default()
        };
        assert!(auth_layer(&cfg).is_none());
    }

    #[test]
    fn auth_layer_is_some_when_both_credentials_are_set() {
        let cfg = Config {
            auth_user: Some("alice".to_string()),
            auth_password: Some(Secret::new("hunter2")),
            ..Config::default()
        };
        assert!(auth_layer(&cfg).is_some());
    }

    #[test]
    fn cors_layer_accepts_the_wildcard_default() {
        assert!(cors_layer(&Config::default()).is_ok());
    }

    #[test]
    fn cors_layer_accepts_a_concrete_origin() {
        let cfg = Config {
            cors_origin: "https://example.com".to_string(),
            ..Config::default()
        };
        assert!(cors_layer(&cfg).is_ok());
    }

    #[test]
    fn cors_layer_rejects_an_invalid_header_value() {
        let cfg = Config {
            cors_origin: "not\na valid header value".to_string(),
            ..Config::default()
        };
        let err = cors_layer(&cfg).expect_err("newline is not a valid HeaderValue byte");
        assert!(matches!(err, ServeError::InvalidCorsOrigin(_)));
    }

    /// Issue #471 M2: `RequestDeadlineLayer` is ours, so the configured
    /// duration is directly assertable. Before M2 this test could only
    /// prove the builder constructed, because `tower_http`'s layer did not
    /// expose its duration.
    #[test]
    fn timeout_layer_uses_the_configured_query_timeout() {
        let cfg = Config {
            query_timeout: pulsus_config::HumanDuration(std::time::Duration::from_millis(1)),
            ..Config::default()
        };
        // `RequestDeadlineLayer`'s field is private to this module and
        // `mod tests` is its child, so the wiring is read directly rather
        // than through an accessor that production would never call.
        assert_eq!(
            timeout_layer(&cfg).timeout,
            std::time::Duration::from_millis(1)
        );
    }

    /// Issue #471 M2, criterion 4. The partition over concrete paths — the
    /// twelve mounted PromQL query routes get the envelope, and everything
    /// else, inside the prefix and out, keeps the bare `408`.
    #[test]
    fn deadline_class_partitions_every_mounted_api_v1_path() {
        for path in [
            "/api/v1/query",
            "/api/v1/query_range",
            "/api/v1/labels",
            "/api/v1/label/job/values",
            "/api/v1/series",
            "/api/v1/metadata",
            "/api/v1/query_exemplars",
            "/api/v1/status/buildinfo",
            "/api/v1/status/config",
            "/api/v1/status/flags",
            "/api/v1/status/runtimeinfo",
            "/api/v1/status/tsdb",
        ] {
            assert_eq!(
                deadline_class(path),
                DeadlineClass::PromApiEnvelope,
                "{path} must get the PromQL error envelope"
            );
        }
        for path in [
            // Inside the prefix, excluded by name — the case a bare prefix
            // test would get wrong.
            "/api/v1/write",
            "/api/logs/v1/labels",
            "/loki/api/v1/labels",
            "/api/traces/v1/traces/x",
            "/v1/logs",
            "/v1/metrics",
            "/v1/traces",
            "/config",
            "/buildinfo",
            "/ready",
            "/metrics",
        ] {
            assert_eq!(
                deadline_class(path),
                DeadlineClass::Bare,
                "{path} must keep the bare 408"
            );
        }
    }

    #[tokio::test]
    async fn cors_layer_echoes_the_configured_origin_header() {
        use axum::body::Body;
        use axum::routing::get;
        use tower::ServiceExt;

        let cfg = Config {
            cors_origin: "https://example.com".to_string(),
            ..Config::default()
        };
        let router: axum::Router = axum::Router::new()
            .route("/x", get(|| async { "ok" }))
            .layer(cors_layer(&cfg).unwrap());
        let request = Request::builder()
            .uri("/x")
            .header(header::ORIGIN, "https://example.com")
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(request).await.unwrap();
        assert_eq!(
            res.headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .unwrap(),
            "https://example.com"
        );
    }

    /// Issue #471 M2 renamed this from
    /// `timeout_layer_returns_408_for_a_handler_slower_than_the_deadline`.
    /// The old name asserted a contract that is no longer universal: the
    /// route here is `/slow`, which is not under `/api/v1/`, so it keeps
    /// the bare `408` — and the test stayed green through a change that
    /// made its name false. The name now says which class it holds for,
    /// and its sibling below covers the other class.
    #[tokio::test]
    async fn the_deadline_returns_the_bare_408_on_a_path_outside_the_prom_surface() {
        use axum::body::Body;
        use axum::routing::get;
        use std::time::Duration;
        use tower::ServiceExt;

        let cfg = Config {
            query_timeout: pulsus_config::HumanDuration(Duration::from_millis(20)),
            ..Config::default()
        };
        let router: axum::Router = axum::Router::new()
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    "too slow"
                }),
            )
            .layer(timeout_layer(&cfg));
        let request = Request::builder().uri("/slow").body(Body::empty()).unwrap();
        let res = router.oneshot(request).await.unwrap();
        assert_eq!(res.status(), StatusCode::REQUEST_TIMEOUT);
    }

    /// Issue #471 M2, criterion 5. The two classes, driven through a real
    /// router of never-completing handlers.
    ///
    /// At 50 ms the two duration renderings coincide, so this test alone
    /// would not catch `HumanDuration`'s `Display` being used — that is
    /// what `error::tests::deadline_producer_messages_are_the_two_pinned_literals`
    /// carries, with its discriminating `120s` case.
    #[tokio::test]
    async fn the_deadline_answers_the_prom_surface_with_the_error_envelope() {
        use axum::body::Body;
        use axum::routing::{get, post};
        use std::time::Duration;
        use tower::ServiceExt;

        let cfg = Config {
            query_timeout: pulsus_config::HumanDuration(Duration::from_millis(50)),
            ..Config::default()
        };
        fn router(cfg: &Config) -> axum::Router {
            axum::Router::new()
                .route("/api/v1/query", get(std::future::pending::<&'static str>))
                .route(
                    "/api/v1/status/tsdb",
                    get(std::future::pending::<&'static str>),
                )
                .route("/api/v1/write", post(std::future::pending::<&'static str>))
                .route(
                    "/api/logs/v1/labels",
                    get(std::future::pending::<&'static str>),
                )
                .layer(timeout_layer(cfg))
        }

        async fn drive(cfg: &Config, method: &str, uri: &str) -> Response<Body> {
            let request = Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .unwrap();
            router(cfg).oneshot(request).await.unwrap()
        }

        let envelope = concat!(
            r#"{"status":"error","errorType":"timeout","#,
            r#""error":"request exceeded the server deadline of 50ms (PULSUS_QUERY_TIMEOUT)"}"#
        );

        for path in ["/api/v1/query", "/api/v1/status/tsdb"] {
            let res = drive(&cfg, "GET", path).await;
            assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
            assert_eq!(
                res.headers().get(header::CONTENT_TYPE).unwrap(),
                "application/json",
                "{path}"
            );
            let body = axum::body::to_bytes(res.into_body(), usize::MAX)
                .await
                .unwrap();
            // `/api/v1/status/tsdb` gets the IDENTICAL body — this is what
            // pins the message saying *request*, not *query*.
            assert_eq!(String::from_utf8_lossy(&body), envelope, "{path}");
        }

        for (method, path) in [("POST", "/api/v1/write"), ("GET", "/api/logs/v1/labels")] {
            let res = drive(&cfg, method, path).await;
            assert_eq!(res.status(), StatusCode::REQUEST_TIMEOUT, "{path}");
            // Byte-identical to what the pre-#471 `tower_http` layer
            // produced through the same router: the layer itself sets no
            // headers (`Response::new(B::default())` plus a status), and
            // axum's routing adds `content-length: 0` on top — measured
            // identical for both layers. The load-bearing half is the
            // ABSENCE of `Content-Type`, which is what kept this response
            // out of the set a client parses a body for.
            assert_eq!(
                res.headers()
                    .iter()
                    .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap().to_string()))
                    .collect::<Vec<_>>(),
                vec![("content-length".to_string(), "0".to_string())],
                "{path}: the bare 408's header set"
            );
            let body = axum::body::to_bytes(res.into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(body.is_empty(), "{path}: the bare 408 has an empty body");
        }
    }
}
