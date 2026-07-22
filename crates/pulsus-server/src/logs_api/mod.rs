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
//! Issue #74 (M6-11) adds `/tail` (WebSocket, §2.4) and `/stats` (§2.5)
//! plus their `/loki/api/v1/{tail,index/stats}` aliases. Issue #169 (M7)
//! adds the first drilldown endpoint, `/volume` (§2.6), with its
//! `/loki/api/v1/index/volume` alias. Neither alias suffix is a prefix
//! swap of its native path (`/index/stats` vs `/stats`, `/index/volume`
//! vs `/volume`), so those routes mount explicitly below rather than
//! through [`mount_log_query_routes`]. Issue #170 (M7) adds
//! `/detected_labels` + `/detected_fields` (§2.6) — both aliases ARE pure
//! prefix swaps, mounted via [`mount_detected_routes`] on both surfaces.
//! Still out of scope: `/patterns` and its alias.

mod detected;
mod encode;
mod error;
mod handlers;
mod params;
mod stats;
mod tail;
mod volume;

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

/// Mounts the two detected-labels/fields drilldown routes under `prefix`
/// (issue #170, docs/api.md §2.6): `GET|POST` form-encoded on both (the
/// `/labels`/`/series` precedent). Unlike `/index/stats`/`/index/volume`,
/// both `/loki/api/v1` aliases ARE pure prefix swaps, so one helper
/// serves both surfaces — the same cannot-drift-apart rationale as
/// [`mount_log_query_routes`].
fn mount_detected_routes(router: Router<AppState>, prefix: &str) -> Router<AppState> {
    router
        .route(
            &format!("{prefix}/detected_labels"),
            get(detected::detected_labels).post(detected::detected_labels_post),
        )
        .route(
            &format!("{prefix}/detected_fields"),
            get(detected::detected_fields).post(detected::detected_fields_post),
        )
}

/// The native `/api/logs/v1` surface (docs/api.md §2.1-2.6): the five
/// query routes via [`mount_log_query_routes`], the two detected
/// drilldown routes via [`mount_detected_routes`] (issue #170), plus
/// `/tail` (WebSocket, issue #74), `/stats`, and `/volume` (issue #169)
/// mounted explicitly (all `GET`-only).
pub(crate) fn router() -> Router<AppState> {
    let router = mount_log_query_routes(Router::new(), "/api/logs/v1")
        .route("/api/logs/v1/tail", get(tail::tail))
        .route("/api/logs/v1/stats", get(stats::stats))
        .route("/api/logs/v1/volume", get(volume::volume));
    mount_detected_routes(router, "/api/logs/v1")
}

/// The `/loki/api/v1/*` compat alias surface (docs/api.md §8.1, issue #14).
/// Same handler fns, same method matrix as [`router`] — responses are
/// byte-identical to native, including `X-Pulsus-Explain` passthrough,
/// because the two surfaces are pure route bindings onto the same handlers.
/// Mounting this router at all is `compat.rs`'s job (flag + Reader-mode
/// gated); this fn just builds the route set.
pub(crate) fn compat_router() -> Router<AppState> {
    let router = mount_log_query_routes(Router::new(), "/loki/api/v1")
        // Issue #74: the M6 aliases. `/index/stats` is deliberately NOT
        // derived from the native `/stats` path — the alias suffix is not
        // a prefix swap (docs/api.md §8.1's M6 row). Issue #169: the M7
        // `/index/volume` alias follows the same irregular-suffix rule.
        .route("/loki/api/v1/tail", get(tail::tail))
        .route("/loki/api/v1/index/stats", get(stats::stats))
        .route("/loki/api/v1/index/volume", get(volume::volume));
    mount_detected_routes(router, "/loki/api/v1")
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
