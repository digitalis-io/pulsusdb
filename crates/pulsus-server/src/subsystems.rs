//! Stub subsystem routers (issue #6 M0 scaffold). Contract for later
//! issues: relocate each function's body into `pulsus-write`, `pulsus-read`,
//! and `pulsus-ruler` respectively, keeping the `fn() -> Router<AppState>`
//! signature so `modes.rs` never has to change when the real handlers land.

use axum::Router;
use axum::routing::post;

use crate::app::AppState;

/// Ingestion APIs (OTLP, Prometheus remote write, native profile ingest).
/// `POST /v1/logs` (issue #15), `POST /v1/metrics` (issue #27),
/// `POST /v1/traces` (issue #54), and `POST /api/v1/write` (issue #28,
/// docs/api.md §1.1-1.2) are wired; the remaining signals' ingest routes
/// are still empty until their own issues land. `/api/v1/write` coexists
/// with the PromQL query surface's `/api/v1/query*` routes (mounted
/// separately by `reader_router`) because `mount_subsystems` merges
/// distinct full paths — no nest/overlap.
pub(crate) fn writer_router() -> Router<AppState> {
    Router::new()
        .route("/v1/logs", post(crate::ingest::ingest_logs))
        .route("/v1/metrics", post(crate::ingest::ingest_metrics))
        .route("/v1/traces", post(crate::ingest::ingest_traces))
        .route("/api/v1/write", post(crate::ingest::ingest_remote_write))
}

/// Query APIs (`/api/logs/v1`, `/api/v1`, `/api/traces/v1`, `/api/profiles/v1`).
/// `/api/logs/v1` is wired (issue #13); `/api/v1` (the standard Prometheus
/// HTTP API — PulsusDB's *native* metrics surface, issue #32) is wired
/// too; the remaining product surfaces (`/api/traces/v1`,
/// `/api/profiles/v1`) are still empty until their own issues land.
pub(crate) fn reader_router() -> Router<AppState> {
    crate::logs_api::router().merge(crate::prom_api::router())
}

/// Rules API (`/api/rules/v1`). Empty until pulsus-ruler lands its handlers.
pub(crate) fn ruler_router() -> Router<AppState> {
    Router::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_routers_construct_without_panicking() {
        let _ = writer_router();
        let _ = reader_router();
        let _ = ruler_router();
    }

    /// Issue #28 edge case 6: `/api/v1/write` (writer, POST) must coexist
    /// with the reader subsystem's future `/api/v1/query*` (PromQL, GET)
    /// mount point — `Router::merge` panics at router-build time on an
    /// actual path collision, so `writer_router().merge(reader_router())`
    /// succeeding (as it does today, and as `app::build_router`'s own
    /// tests exercise for the full `Mode::All` composition) is itself the
    /// guard; this test additionally pins that the route is present and
    /// reachable via POST, not merely that construction doesn't panic.
    #[tokio::test]
    async fn writer_router_exposes_post_api_v1_write() {
        use std::sync::{Arc, OnceLock};

        use axum::body::Body;
        use axum::http::{Method, Request, StatusCode};
        use pulsus_config::Config;
        use tokio::sync::RwLock;
        use tower::ServiceExt;

        use crate::app::{AppState, BuildInfo};
        use crate::ingest::{MetricWriterSink, TraceWriterSink, WriterSink};

        let state = AppState {
            pool: Arc::new(RwLock::new(None)),
            config: Arc::new(Config::default()),
            metrics: metrics_exporter_prometheus::PrometheusBuilder::new()
                .build_recorder()
                .handle(),
            build: BuildInfo::from_build_env(),
            writer: Arc::new(WriterSink::new(Arc::new(OnceLock::new()))),
            metric_writer: Arc::new(MetricWriterSink::new(Arc::new(OnceLock::new()))),
            trace_writer: Arc::new(TraceWriterSink::new(Arc::new(OnceLock::new()))),
            label_cache: Arc::new(OnceLock::new()),
            started_at: std::time::SystemTime::now(),
        };
        let router = writer_router().with_state(state);
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/write")
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(request).await.unwrap();
        // An empty body is not valid snappy, so this actually 400s inside
        // `ingest_remote_write` before ever reaching the (empty) metric
        // writer slot — the point of this test is only that the route is
        // mounted at all (never `404`), not the exact error status.
        assert_ne!(res.status(), StatusCode::NOT_FOUND);
    }
}
