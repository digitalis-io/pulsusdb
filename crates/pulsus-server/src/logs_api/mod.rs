//! `/api/logs/v1` — the five M1 core LogQL query endpoints (issue #13,
//! docs/api.md §2). Thin handlers (`handlers.rs`): parse params
//! (`params.rs`) → parse LogQL (`pulsus-logql`) → dispatch to `LogQlEngine`
//! (`pulsus-read`) → encode the envelope (`encode.rs`, `error.rs`). All
//! planning/SQL/execution stays in `pulsus-read` — this module only ever
//! talks to it through `LogQlEngine`'s public methods.
//!
//! Out of scope here (see the architect plan): `/tail` (WebSocket, §2.4),
//! `/stats` (§2.5), the drilldown endpoints (§2.6, M7), and every
//! `/loki/...` compat alias (owned by `compat.rs`, M6+).

mod encode;
mod error;
mod handlers;
mod params;

use axum::Router;
use axum::routing::get;

use crate::app::AppState;

/// Mounts the five `/api/logs/v1` routes (docs/api.md §2.1-2.3), full
/// method matrix: `GET|POST` on `/query_range` and `/query` (issue #13
/// architect plan amendment 3 §2, ratified by task-manager, reversing
/// amendment 1's M1 GET-only deferral for those two) and `GET|POST` on
/// `/labels` and `/series` (pinned `GET|POST` from amendment 1 onward, per
/// api.md §2.3); `label/{name}/values` is `GET`-only throughout.
pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/logs/v1/query_range",
            get(handlers::query_range).post(handlers::query_range_post),
        )
        .route(
            "/api/logs/v1/query",
            get(handlers::query).post(handlers::query_post),
        )
        .route(
            "/api/logs/v1/labels",
            get(handlers::labels_get).post(handlers::labels_post),
        )
        .route(
            "/api/logs/v1/label/{name}/values",
            get(handlers::label_values),
        )
        .route(
            "/api/logs/v1/series",
            get(handlers::series_get).post(handlers::series_post),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_constructs_without_panicking() {
        let _ = router();
    }
}
