//! `PULSUS_COMPAT_ENDPOINTS` mounting contract (docs/api.md §8).
//! [`apply_aliases`] is the extension point later issues push more
//! `(alias, native)` surfaces into, gated on `cfg.compat_endpoints`. The
//! M1 log-query aliases (`/loki/api/v1/*`, docs/api.md §8.1) ship here —
//! see `logs_api/mod.rs`'s module doc for why the route list itself lives
//! there, not in this file.

use axum::Router;
use pulsus_config::Config;

use crate::app::AppState;
use crate::modes::{self, Subsystem};

/// Mounts every enabled compatibility alias onto `router`.
///
/// The M1 log-query aliases mount iff `cfg.compat_endpoints` **and** the
/// Reader subsystem is mounted (`modes::mounted`) — mirroring native
/// exactly, so `/loki/api/v1/*` 404s wherever `/api/logs/v1/*` does (e.g.
/// writer-only mode). Gating is router-build-time only: no per-request flag
/// check.
pub(crate) fn apply_aliases(router: Router<AppState>, cfg: &Config) -> Router<AppState> {
    if !cfg.compat_endpoints {
        return router;
    }
    let mut router = router;
    if modes::mounted(cfg).contains(&Subsystem::Reader) {
        router = router.merge(crate::logs_api::compat_router());
    }
    // Future issues add further compat surfaces here, one per docs/api.md
    // §8 row, once the native handler exists.
    router
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use pulsus_config::Mode;
    use tokio::sync::RwLock;
    use tower::ServiceExt;

    /// Minimal `AppState` for router-shape assertions (mirrors `app.rs`'s
    /// own `test_state`): no live pool, so a mounted-but-unauthenticated
    /// handler reaches "no pool" (503), never 404 — exactly what
    /// distinguishes "route absent" from "route present but unready" here.
    fn test_state() -> AppState {
        AppState {
            pool: Arc::new(RwLock::new(None)),
            config: Arc::new(Config::default()),
            metrics: metrics_exporter_prometheus::PrometheusBuilder::new()
                .build_recorder()
                .handle(),
            build: crate::app::BuildInfo::from_build_env(),
        }
    }

    async fn status(router: Router<AppState>, path: &str) -> StatusCode {
        let request = Request::builder()
            .uri(path)
            .body(Body::empty())
            .expect("build request");
        router
            .with_state(test_state())
            .oneshot(request)
            .await
            .expect("router does not fail the request")
            .status()
    }

    #[tokio::test]
    async fn flag_off_leaves_the_loki_surface_unmounted() {
        let cfg = Config {
            compat_endpoints: false,
            ..Config::default()
        };
        let router = apply_aliases(Router::new(), &cfg);
        assert_eq!(
            status(router, "/loki/api/v1/labels").await,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn flag_on_with_reader_mounted_mounts_the_loki_surface() {
        let cfg = Config {
            compat_endpoints: true,
            mode: Mode::All,
            ..Config::default()
        };
        let router = apply_aliases(Router::new(), &cfg);
        assert_ne!(
            status(router, "/loki/api/v1/labels").await,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn flag_on_but_writer_only_mode_leaves_the_loki_surface_unmounted() {
        // Mode invariant: the alias must 404 exactly where native does —
        // writer-only mode never mounts the Reader subsystem, so the
        // compat surface must not appear either, even with the flag on.
        let cfg = Config {
            compat_endpoints: true,
            mode: Mode::Writer,
            ..Config::default()
        };
        let router = apply_aliases(Router::new(), &cfg);
        assert_eq!(
            status(router, "/loki/api/v1/labels").await,
            StatusCode::NOT_FOUND
        );
    }
}
