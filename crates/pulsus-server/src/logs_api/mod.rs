//! `/api/logs/v1` — the five M1 core LogQL query endpoints (issue #13,
//! docs/api.md §2). Thin handlers (`handlers.rs`): parse params
//! (`params.rs`) → parse LogQL (`pulsus-logql`) → dispatch to `LogQlEngine`
//! (`pulsus-read`) → encode the envelope (`encode.rs`, `error.rs`). All
//! planning/SQL/execution stays in `pulsus-read` — this module only ever
//! talks to it through `LogQlEngine`'s public methods.
//!
//! The `/loki/api/v1/*` M1 query aliases (docs/api.md §8.1) ship **here**:
//! [`mount_log_query_routes`] is the single source of truth for the five
//! routes' method matrix, shared by [`router`] (native) and
//! [`compat_router`] (alias) so the two surfaces cannot drift apart.
//! `compat.rs` only decides *whether* [`compat_router`] gets merged in
//! (flag + mode gating) — it never duplicates the route list itself.
//!
//! Out of scope here (see the architect plan): `/tail` (WebSocket, §2.4),
//! `/stats` (§2.5), the drilldown endpoints (§2.6, M7), and every other
//! `/loki/...` compat alias (M6+, per docs/api.md §8.1's table).

mod encode;
mod error;
mod handlers;
mod params;

use axum::Router;
use axum::routing::get;

use crate::app::AppState;

/// Mounts the five log-query routes under `prefix` (no trailing slash),
/// e.g. `/api/logs/v1` (native) or `/loki/api/v1` (compat alias, issue #14).
/// Full method matrix, pinned identically for both surfaces: `GET|POST` on
/// `/query_range` and `/query` (issue #13 architect plan amendment 3 §2,
/// ratified by task-manager, reversing amendment 1's M1 GET-only deferral
/// for those two) and `GET|POST` on `/labels` and `/series` (pinned
/// `GET|POST` from amendment 1 onward, per api.md §2.3); `label/{name}/values`
/// is `GET`-only throughout. Any other method on a mounted path is a 405;
/// any method on an unmounted path (alias off, or writer-only mode) is a 404.
fn mount_log_query_routes(router: Router<AppState>, prefix: &str) -> Router<AppState> {
    router
        .route(
            &format!("{prefix}/query_range"),
            get(handlers::query_range).post(handlers::query_range_post),
        )
        .route(
            &format!("{prefix}/query"),
            get(handlers::query).post(handlers::query_post),
        )
        .route(
            &format!("{prefix}/labels"),
            get(handlers::labels_get).post(handlers::labels_post),
        )
        .route(
            &format!("{prefix}/label/{{name}}/values"),
            get(handlers::label_values),
        )
        .route(
            &format!("{prefix}/series"),
            get(handlers::series_get).post(handlers::series_post),
        )
}

/// The native `/api/logs/v1` surface (docs/api.md §2.1-2.3). Unchanged
/// behaviour — now delegates to [`mount_log_query_routes`].
pub(crate) fn router() -> Router<AppState> {
    mount_log_query_routes(Router::new(), "/api/logs/v1")
}

/// The `/loki/api/v1/*` compat alias surface (docs/api.md §8.1, issue #14).
/// Same handler fns, same method matrix as [`router`] — responses are
/// byte-identical to native, including `X-Pulsus-Explain` passthrough,
/// because the two surfaces are pure route bindings onto the same handlers.
/// Mounting this router at all is `compat.rs`'s job (flag + Reader-mode
/// gated); this fn just builds the route set.
pub(crate) fn compat_router() -> Router<AppState> {
    mount_log_query_routes(Router::new(), "/loki/api/v1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_constructs_without_panicking() {
        let _ = router();
    }

    #[test]
    fn compat_router_constructs_without_panicking() {
        let _ = compat_router();
    }
}
