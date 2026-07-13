//! Stub subsystem routers (issue #6 M0 scaffold). Contract for later
//! issues: relocate each function's body into `pulsus-write`, `pulsus-read`,
//! and `pulsus-ruler` respectively, keeping the `fn() -> Router<AppState>`
//! signature so `modes.rs` never has to change when the real handlers land.

use axum::Router;

use crate::app::AppState;

/// Ingestion APIs (OTLP, Prometheus remote write, native profile ingest).
/// Empty until pulsus-write lands its handlers.
pub(crate) fn writer_router() -> Router<AppState> {
    Router::new()
}

/// Query APIs (`/api/logs/v1`, `/api/v1`, `/api/traces/v1`, `/api/profiles/v1`).
/// Empty until pulsus-read lands its handlers.
pub(crate) fn reader_router() -> Router<AppState> {
    Router::new()
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
