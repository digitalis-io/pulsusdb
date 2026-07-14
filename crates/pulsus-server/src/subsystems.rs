//! Stub subsystem routers (issue #6 M0 scaffold). Contract for later
//! issues: relocate each function's body into `pulsus-write`, `pulsus-read`,
//! and `pulsus-ruler` respectively, keeping the `fn() -> Router<AppState>`
//! signature so `modes.rs` never has to change when the real handlers land.

use axum::Router;
use axum::routing::post;

use crate::app::AppState;

/// Ingestion APIs (OTLP, Prometheus remote write, native profile ingest).
/// `POST /v1/logs` (issue #15) and `POST /v1/metrics` (issue #27,
/// docs/api.md §1.1) are wired; the remaining signals' ingest routes are
/// still empty until their own issues land.
pub(crate) fn writer_router() -> Router<AppState> {
    Router::new()
        .route("/v1/logs", post(crate::ingest::ingest_logs))
        .route("/v1/metrics", post(crate::ingest::ingest_metrics))
}

/// Query APIs (`/api/logs/v1`, `/api/v1`, `/api/traces/v1`, `/api/profiles/v1`).
/// `/api/logs/v1` is wired (issue #13); the remaining product surfaces
/// (`/api/v1` PromQL, `/api/traces/v1`, `/api/profiles/v1`) are still empty
/// until their own issues land.
pub(crate) fn reader_router() -> Router<AppState> {
    crate::logs_api::router()
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
}
