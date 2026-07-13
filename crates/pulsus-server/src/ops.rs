//! `/ready`, `/metrics`, `/config`, `/buildinfo` (docs/api.md §7). Split
//! into a public sub-router (`/ready`, `/metrics`) and an authed sub-router
//! (`/config`, `/buildinfo`) per the architect plan amendment — see
//! `app::build_router` for how the two are composed with the rest of the
//! middleware stack.

use std::future::Future;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use pulsus_clickhouse::ChError;

use crate::app::AppState;

/// `/ready` and `/metrics` — always unauthenticated (probes/scrapers must
/// work credential-free) and never subject to the generic query timeout
/// (amendment F1/F2 — enforced by composition in `app::build_router`, not
/// here).
pub(crate) fn ops_public_router() -> Router<AppState> {
    Router::new()
        .route("/ready", get(ready))
        .route("/metrics", get(metrics_handler))
}

/// `/config` and `/buildinfo` — inside auth (when configured) and the
/// generic timeout.
pub(crate) fn ops_authed_router() -> Router<AppState> {
    Router::new()
        .route("/config", get(config_handler))
        .route("/buildinfo", get(buildinfo_handler))
}

/// The hard deadline `/ready` gives ClickHouse's `ping` before mapping the
/// attempt to 503 — independent of `PULSUS_QUERY_TIMEOUT` (amendment F2): a
/// slow or hanging ClickHouse must still produce the documented 503, never
/// the generic timeout's 408/504.
const READY_PING_TIMEOUT: Duration = Duration::from_secs(2);

/// `GET /ready` (docs/api.md §7): 200 only after a live, successful
/// ClickHouse ping; 503 (with a short reason body) for "pool not yet
/// established", "ping failed", and "ping exceeded 2s" alike. The pool
/// `Option` is cloned out from behind the lock with the guard dropped
/// before the `.await` on the ping itself, so the lock is never held across
/// an await point.
async fn ready(State(state): State<AppState>) -> Response {
    let pool = {
        let guard = state.pool.read().await;
        guard.clone()
    };
    let Some(pool) = pool else {
        return unavailable("clickhouse pool not yet established");
    };
    ready_from_ping(async move { pool.ping().await }).await
}

/// The 503-mapping core of [`ready`], decoupled from `ChPool` so the
/// "ping exceeds the deadline" branch is unit-testable without a live (or
/// even fake-hanging) ClickHouse connection.
async fn ready_from_ping<F>(ping: F) -> Response
where
    F: Future<Output = Result<(), ChError>>,
{
    match tokio::time::timeout(READY_PING_TIMEOUT, ping).await {
        Ok(Ok(())) => StatusCode::OK.into_response(),
        Ok(Err(err)) => unavailable(&format!("clickhouse ping failed: {err}")),
        Err(_elapsed) => unavailable("clickhouse ping exceeded 2s"),
    }
}

fn unavailable(reason: &str) -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, reason.to_string()).into_response()
}

/// `GET /metrics`: Prometheus exposition of PulsusDB internals.
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
}

/// `GET /config`: effective configuration, secrets redacted
/// (`Config::to_redacted_yaml`).
async fn config_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.config.to_redacted_yaml() {
        Ok(yaml) => (StatusCode::OK, yaml).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "failed to render redacted config");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to render configuration",
            )
                .into_response()
        }
    }
}

/// `GET /buildinfo`: `{"version","revision","builtAt","rustc"}`.
async fn buildinfo_handler(State(state): State<AppState>) -> impl IntoResponse {
    axum::Json(state.build.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use crate::app::BuildInfo;
    use pulsus_config::Config;

    fn test_state() -> AppState {
        AppState {
            pool: Arc::new(RwLock::new(None)),
            config: Arc::new(Config::default()),
            metrics: metrics_exporter_prometheus::PrometheusBuilder::new()
                .build_recorder()
                .handle(),
            build: BuildInfo::from_build_env(),
        }
    }

    #[tokio::test]
    async fn ready_is_503_when_the_pool_is_not_yet_established() {
        let res = ready(State(test_state())).await;
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn ready_from_ping_is_200_on_a_successful_ping() {
        let res = ready_from_ping(async { Ok(()) }).await;
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn ready_from_ping_is_503_on_a_failed_ping() {
        let res = ready_from_ping(async { Err(ChError::Connect("refused".to_string())) }).await;
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn ready_from_ping_is_503_not_408_or_504_when_the_ping_hangs() {
        // A ping future that never resolves stands in for a hung ClickHouse
        // connection (amendment F2's load-bearing case): the 2s internal
        // deadline must still map to 503, never a generic-timeout-style
        // 408/504.
        let hang = std::future::pending::<Result<(), ChError>>();
        let res = ready_from_ping(hang).await;
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_ne!(res.status(), StatusCode::REQUEST_TIMEOUT);
        assert_ne!(res.status(), StatusCode::GATEWAY_TIMEOUT);
    }

    #[tokio::test]
    async fn metrics_handler_renders_the_prometheus_handle() {
        let res = metrics_handler(State(test_state())).await.into_response();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn config_handler_redacts_the_password() {
        let mut cfg = Config::default();
        cfg.clickhouse.auth.password = pulsus_config::Secret::new("s3cret");
        let state = AppState {
            pool: Arc::new(RwLock::new(None)),
            config: Arc::new(cfg),
            metrics: metrics_exporter_prometheus::PrometheusBuilder::new()
                .build_recorder()
                .handle(),
            build: BuildInfo::from_build_env(),
        };
        let res = config_handler(State(state)).await.into_response();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(!text.contains("s3cret"));
    }

    #[tokio::test]
    async fn buildinfo_handler_has_four_non_empty_fields() {
        let res = buildinfo_handler(State(test_state())).await.into_response();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        for field in ["version", "revision", "builtAt", "rustc"] {
            assert!(
                json.get(field)
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| !s.is_empty()),
                "missing or empty field {field:?} in {json}"
            );
        }
    }
}
