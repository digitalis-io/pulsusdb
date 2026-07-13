//! Per-layer middleware builders (issue #6 architect plan + amendment).
//! Each builder is small and independently testable; `app::build_router`
//! decides *where* every layer is applied (the amendment's F1/F2 split —
//! public ops sit outside both auth and the generic timeout).

use axum::body::Body;
use axum::http::{HeaderValue, Request, Response, StatusCode, header};
use tower_http::classify::{ServerErrorsAsFailures, SharedClassifier};
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::timeout::TimeoutLayer;
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

/// The hard per-request deadline for data-plane routes plus `/config` and
/// `/buildinfo` — never `/ready`/`/metrics` (amendment F2, applied by
/// `app::build_router`'s composition, not by this function). The same
/// `PULSUS_QUERY_TIMEOUT` also drives ClickHouse's `max_execution_time`, so
/// client and server never split-brain on which side gives up first.
///
/// `TimeoutLayer::with_status_code` (not the deprecated `::new`) is used
/// deliberately: it keeps the wrapped service's error type `Infallible` by
/// returning a `408 Request Timeout` response directly, so no
/// `HandleErrorLayer` conversion is needed and this layer composes directly
/// with `Router::layer`.
pub(crate) fn timeout_layer(config: &Config) -> TimeoutLayer {
    TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, config.query_timeout.0)
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

    #[test]
    fn timeout_layer_uses_the_configured_query_timeout() {
        // `TimeoutLayer` does not expose its duration for inspection; the
        // behavioral 408 contract is covered by
        // `timeout_layer_returns_408_for_a_handler_slower_than_the_deadline`
        // below. This just proves the builder constructs successfully.
        let cfg = Config {
            query_timeout: pulsus_config::HumanDuration(std::time::Duration::from_millis(1)),
            ..Config::default()
        };
        let _ = timeout_layer(&cfg);
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

    #[tokio::test]
    async fn timeout_layer_returns_408_for_a_handler_slower_than_the_deadline() {
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
}
