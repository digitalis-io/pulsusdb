//! `PULSUS_COMPAT_ENDPOINTS` mounting contract (docs/api.md §8).
//! [`apply_aliases`] is the extension point later issues push more
//! `(alias, native)` surfaces into, gated on `cfg.compat_endpoints`. The
//! M1 log-query aliases (`/loki/api/v1/*`) and the M4 Tempo trace-query
//! aliases (issue #61, docs/api.md §8.1) ship here — see
//! `logs_api/mod.rs`'s / `traces_api/mod.rs`'s module docs for why each
//! route list itself lives there, not in this file.

use axum::Router;
use pulsus_config::Config;

use crate::app::AppState;
use crate::modes::{self, Subsystem};

/// Mounts every enabled compatibility alias onto `router`.
///
/// The M1 log-query aliases and the M4 Tempo trace-query aliases (issue
/// #61) mount iff `cfg.compat_endpoints` **and** the Reader subsystem is
/// mounted (`modes::mounted`) — mirroring native exactly, so
/// `/loki/api/v1/*` and the Tempo alias paths 404 wherever their native
/// twins do (e.g. writer-only mode). Gating is router-build-time only: no
/// per-request flag check.
pub(crate) fn apply_aliases(router: Router<AppState>, cfg: &Config) -> Router<AppState> {
    if !cfg.compat_endpoints {
        return router;
    }
    let mut router = router;
    if modes::mounted(cfg).contains(&Subsystem::Reader) {
        router = router.merge(crate::logs_api::compat_router());
        router = router.merge(crate::traces_api::compat_router());
    }
    // The §8.2 WRITER-side compat receivers — the Loki push receiver
    // (issue #77) and the Zipkin v2 JSON trace receiver (issue #75) — mount
    // iff the flag is on (guarded by the early return above) AND the Writer
    // subsystem is mounted — never on the Reader compat flag alone
    // (`Gate::CompatAndWriter`). Kept a separate block from the Reader one so
    // each surface 404s exactly where its native twin does.
    if modes::mounted(cfg).contains(&Subsystem::Writer) {
        router = router.merge(crate::ingest::loki_push_compat_router());
        router = router.merge(crate::ingest::zipkin_compat_router());
    }
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
            writer: Arc::new(crate::ingest::WriterSink::new(Arc::new(
                std::sync::OnceLock::new(),
            ))),
            metric_writer: Arc::new(crate::ingest::MetricWriterSink::new(Arc::new(
                std::sync::OnceLock::new(),
            ))),
            trace_writer: Arc::new(crate::ingest::TraceWriterSink::new(Arc::new(
                std::sync::OnceLock::new(),
            ))),
            label_cache: Arc::new(std::sync::OnceLock::new()),
            started_at: std::time::SystemTime::now(),
            tail: std::sync::Arc::new(crate::app::TailRuntime::for_tests()),
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
    async fn flag_off_leaves_the_compat_surfaces_unmounted() {
        let cfg = Config {
            compat_endpoints: false,
            ..Config::default()
        };
        let router = apply_aliases(Router::new(), &cfg);
        assert_eq!(
            status(router.clone(), "/loki/api/v1/labels").await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(status(router, "/api/search").await, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn flag_on_with_reader_mounted_mounts_the_compat_surfaces() {
        let cfg = Config {
            compat_endpoints: true,
            mode: Mode::All,
            ..Config::default()
        };
        let router = apply_aliases(Router::new(), &cfg);
        assert_ne!(
            status(router.clone(), "/loki/api/v1/labels").await,
            StatusCode::NOT_FOUND
        );
        assert_ne!(status(router, "/api/search").await, StatusCode::NOT_FOUND);
    }

    /// Loki push (issue #77) `Gate::CompatAndWriter` isolation at the
    /// router-build level: a POST probe distinguishes mounted (405 —
    /// method-mismatch on a real route; the route is POST-only and this
    /// helper's default matters little, but a mounted POST route reaches its
    /// handler) from unmounted (404). Three cases prove mount iff (Writer
    /// role AND compat flag): flag-on + writer mounts; flag-on + reader-only
    /// 404s (flag alone never mounts writer-side); flag-off + all-mode 404s.
    async fn push_status(cfg: &Config) -> StatusCode {
        let router = apply_aliases(Router::new(), cfg);
        let request = Request::builder()
            .method("POST")
            .uri("/loki/api/v1/push")
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
    async fn loki_push_mounts_with_flag_on_and_writer_subsystem() {
        let cfg = Config {
            compat_endpoints: true,
            mode: Mode::Writer,
            ..Config::default()
        };
        assert_ne!(push_status(&cfg).await, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn loki_push_404s_with_flag_on_but_reader_only_mode() {
        // The flag alone never mounts the writer-side push route: reader-only
        // mode has no Writer subsystem, so the route stays absent even with
        // compat on — proving `CompatAndWriter`, never on the flag alone.
        let cfg = Config {
            compat_endpoints: true,
            mode: Mode::Reader,
            ..Config::default()
        };
        assert_eq!(push_status(&cfg).await, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn loki_push_404s_with_flag_off_even_in_all_mode() {
        let cfg = Config {
            compat_endpoints: false,
            mode: Mode::All,
            ..Config::default()
        };
        assert_eq!(push_status(&cfg).await, StatusCode::NOT_FOUND);
    }

    /// The Zipkin v2 JSON receiver (issue #75) shares the Loki push
    /// receiver's `Gate::CompatAndWriter` isolation — a POST probe on one of
    /// its two paths distinguishes mounted (not 404) from unmounted (404).
    async fn zipkin_status(cfg: &Config) -> StatusCode {
        let router = apply_aliases(Router::new(), cfg);
        let request = Request::builder()
            .method("POST")
            .uri("/api/v2/spans")
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
    async fn zipkin_mounts_with_flag_on_and_writer_subsystem() {
        let cfg = Config {
            compat_endpoints: true,
            mode: Mode::Writer,
            ..Config::default()
        };
        assert_ne!(zipkin_status(&cfg).await, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn zipkin_404s_with_flag_on_but_reader_only_mode() {
        let cfg = Config {
            compat_endpoints: true,
            mode: Mode::Reader,
            ..Config::default()
        };
        assert_eq!(zipkin_status(&cfg).await, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn zipkin_404s_with_flag_off_even_in_all_mode() {
        let cfg = Config {
            compat_endpoints: false,
            mode: Mode::All,
            ..Config::default()
        };
        assert_eq!(zipkin_status(&cfg).await, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn flag_on_but_writer_only_mode_leaves_the_compat_surfaces_unmounted() {
        // Mode invariant: the alias must 404 exactly where native does —
        // writer-only mode never mounts the Reader subsystem, so the
        // compat surfaces must not appear either, even with the flag on.
        let cfg = Config {
            compat_endpoints: true,
            mode: Mode::Writer,
            ..Config::default()
        };
        let router = apply_aliases(Router::new(), &cfg);
        assert_eq!(
            status(router.clone(), "/loki/api/v1/labels").await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(status(router, "/api/search").await, StatusCode::NOT_FOUND);
    }
}
