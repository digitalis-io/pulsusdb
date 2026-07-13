//! Shared application state and router assembly (issue #6 architect plan +
//! the F1/F2 amendment). [`build_router`] is the single place the amended
//! composition order is encoded — see its doc comment for the exact layer
//! ordering.

use std::sync::Arc;

use axum::Router;
use metrics_exporter_prometheus::PrometheusHandle;
use pulsus_clickhouse::ChPool;
use pulsus_config::Config;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::middleware;
use crate::ops;
use crate::serve::ServeError;
use crate::{compat, modes};

/// Shared, cheaply-`Clone`able application state handed to every handler via
/// `axum::extract::State`. `pool` starts `None` and is filled in exactly
/// once by the background reconnect loop (`serve::spawn_reconnect_loop`) —
/// `tokio::sync::RwLock` because `/ready` reads it on every probe while it
/// is written at most once per process lifetime (reads vastly dominate
/// writes).
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) pool: Arc<RwLock<Option<Arc<ChPool>>>>,
    pub(crate) config: Arc<Config>,
    pub(crate) metrics: PrometheusHandle,
    pub(crate) build: BuildInfo,
}

/// `/buildinfo` payload (docs/api.md §7): `{"version","revision","builtAt","rustc"}`.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BuildInfo {
    pub(crate) version: String,
    pub(crate) revision: String,
    #[serde(rename = "builtAt")]
    pub(crate) built_at: String,
    pub(crate) rustc: String,
}

impl BuildInfo {
    /// Reads the four build-time constants embedded by `build.rs`
    /// (`CARGO_PKG_VERSION` plus the three `PULSUS_*` build-script env vars).
    pub(crate) fn from_build_env() -> Self {
        BuildInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            revision: env!("PULSUS_GIT_SHA").to_string(),
            built_at: env!("PULSUS_BUILT_AT").to_string(),
            rustc: env!("PULSUS_RUSTC").to_string(),
        }
    }
}

/// Assembles the full router for one process: ops (public + authed) +
/// mounted subsystems + compat aliases, then every middleware layer in the
/// amended composition order (architect plan amendment, F1/F2):
///
/// 1. `authed` = authed ops (`/config`, `/buildinfo`) + mounted subsystems + compat aliases.
/// 2. `authed.layer(timeout).layer(auth)` — auth wraps (is outside) the
///    timeout, so an unauthenticated request never even starts the clock.
/// 3. `public ops (/ready, /metrics).merge(authed)` — public ops sit
///    outside both auth and the generic timeout entirely.
/// 4. Global layers on the merged whole, CORS outermost: trace →
///    compression → CORS (auth/timeout are never global).
pub(crate) fn build_router(state: AppState, config: &Config) -> Result<Router, ServeError> {
    let mut authed = ops::ops_authed_router().merge(modes::mount_subsystems(Router::new(), config));
    authed = compat::apply_aliases(authed, config);

    // The generic per-request deadline. `/ready`/`/metrics` never pass
    // through this stack at all (amendment F2), so its 408 response can
    // never race the readiness 503 contract.
    authed = authed.layer(middleware::timeout_layer(config));

    if let Some(auth) = middleware::auth_layer(config) {
        authed = authed.layer(auth);
    }

    let router = ops::ops_public_router().merge(authed);

    let router = router
        .layer(middleware::trace_layer())
        .layer(middleware::compression_layer())
        .layer(middleware::cors_layer(config)?);

    Ok(router.with_state(state))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

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

    #[test]
    fn build_info_from_build_env_has_four_non_empty_fields() {
        let build = BuildInfo::from_build_env();
        assert!(!build.version.is_empty());
        assert!(!build.revision.is_empty());
        assert!(!build.built_at.is_empty());
        assert!(!build.rustc.is_empty());
    }

    #[test]
    fn build_router_succeeds_for_the_default_config() {
        assert!(build_router(test_state(), &Config::default()).is_ok());
    }

    #[test]
    fn build_router_rejects_an_invalid_cors_origin() {
        let cfg = Config {
            cors_origin: "not\na header value".to_string(),
            ..Config::default()
        };
        assert!(build_router(test_state(), &cfg).is_err());
    }

    /// The amendment's load-bearing ops auth matrix: `/ready` and
    /// `/metrics` must stay reachable with no credentials at all (probes
    /// and scrapers), while `/config` and `/buildinfo` require the
    /// configured Basic credentials — wrong or missing credentials on the
    /// authed pair are rejected, right credentials pass.
    #[tokio::test]
    async fn ops_auth_matrix_exempts_ready_and_metrics_but_gates_config_and_buildinfo() {
        use axum::body::Body;
        use axum::http::Request;
        use pulsus_config::Secret;
        use tower::ServiceExt;

        let cfg = Config {
            auth_user: Some("alice".to_string()),
            auth_password: Some(Secret::new("hunter2")),
            ..Config::default()
        };
        let router = build_router(test_state(), &cfg).expect("router builds");
        let valid = format!("Basic {}", middleware::base64_encode(b"alice:hunter2"));
        let invalid = format!("Basic {}", middleware::base64_encode(b"alice:wrong"));

        async fn status(router: &Router, path: &str, auth: Option<&str>) -> StatusCode {
            let mut builder = Request::builder().uri(path).method("GET");
            if let Some(value) = auth {
                builder = builder.header(axum::http::header::AUTHORIZATION, value);
            }
            let request = builder.body(Body::empty()).unwrap();
            router.clone().oneshot(request).await.unwrap().status()
        }

        // Auth-exempt: reachable with zero credentials (pool is `None` in
        // `test_state`, so `/ready` is 503, not 401 — the point is that it
        // is never gated by auth).
        assert_eq!(
            status(&router, "/ready", None).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(status(&router, "/metrics", None).await, StatusCode::OK);

        // Gated: missing/wrong credentials are 401, right credentials 200.
        assert_eq!(
            status(&router, "/config", None).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(&router, "/buildinfo", None).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(&router, "/config", Some(&invalid)).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(&router, "/config", Some(&valid)).await,
            StatusCode::OK
        );
        assert_eq!(
            status(&router, "/buildinfo", Some(&valid)).await,
            StatusCode::OK
        );
    }

    /// With no `PULSUS_AUTH_USER`/`PULSUS_AUTH_PASSWORD` configured, every
    /// ops endpoint is reachable without credentials.
    #[tokio::test]
    async fn ops_endpoints_are_all_open_when_auth_is_unset() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let router = build_router(test_state(), &Config::default()).expect("router builds");
        for path in ["/metrics", "/config", "/buildinfo"] {
            let request = Request::builder().uri(path).body(Body::empty()).unwrap();
            let res = router.clone().oneshot(request).await.unwrap();
            assert_ne!(
                res.status(),
                StatusCode::UNAUTHORIZED,
                "{path} must not require auth when unset"
            );
        }
    }
}
