//! `/api/v1/*` — the standard Prometheus HTTP API (issue #32, docs/api.md
//! §3). This **is** PulsusDB's native metrics API — there are no compat
//! aliases here (M2 decomposition: `PULSUS_COMPAT_ENDPOINTS` never touches
//! this module). Thin handlers (`handlers.rs`): parse params (`params.rs`)
//! → parse PromQL (`pulsus-promql`) → dispatch to `MetricsEngine`
//! (`pulsus-read`) → encode the envelope (`encode.rs`, `error.rs`). All
//! planning/SQL/execution stays in `pulsus-read`/`pulsus-promql` — this
//! module only ever talks to them through `MetricsEngine`'s public
//! methods.
//!
//! Structurally mirrors `logs_api/` (`params.rs`/`error.rs`/`encode.rs`/
//! `handlers.rs` split; one shared GET+POST param core over
//! `Vec<(String,String)>`; a streaming `unfold`-based encoder), but is
//! **fully self-contained** — no shared helpers with `logs_api`, even
//! where the shapes are near-identical (architect plan: coders may be
//! editing `logs_api/` concurrently; a dedupe follow-up is tracked
//! separately, out of scope for this issue). Two things are deliberately
//! *not* copied from `logs_api` because Prometheus's wire contract
//! differs: the error envelope has no `position` field (`error.rs`), and
//! float/timestamp formatting reproduces Go `strconv`/`jsonutil` exactly,
//! not Rust `Display` (`encode.rs`).
//!
//! Out of scope (architect plan): tier/rollup routing and any
//! `exactness != "raw-exact"` value (M3); PromQL parsing/planning/
//! evaluation itself, the #30 cache internals (consumed, not built here);
//! native histograms, real exemplar data, `/api/v1/write` (#28),
//! `/api/v1/rules` (M7).

// `pub(crate)`: the TraceQL metrics endpoints (`traces_api::metrics`,
// issue #59) reuse `encode::query_response` — their responses are the
// same Prometheus matrix/vector envelope byte-for-byte.
pub(crate) mod encode;
mod error;
mod handlers;
mod params;

use axum::Router;
use axum::routing::get;

use crate::app::AppState;

/// Mounts the full `/api/v1` route/method matrix (docs/api.md §3): `GET|POST`
/// on `query`/`query_range`/`labels`/`series`/`query_exemplars`,
/// `GET`-only on `label/{name}/values`/`metadata`/`status/*`. Any other
/// method on a mounted path is a 405; any method on an unmounted path
/// (writer-only mode) is a 404 — reader-router-only mount, exactly like
/// `logs_api::router`.
pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/query",
            get(handlers::query).post(handlers::query_post),
        )
        .route(
            "/api/v1/query_range",
            get(handlers::query_range).post(handlers::query_range_post),
        )
        .route(
            "/api/v1/labels",
            get(handlers::labels).post(handlers::labels_post),
        )
        .route("/api/v1/label/{name}/values", get(handlers::label_values))
        .route(
            "/api/v1/series",
            get(handlers::series).post(handlers::series_post),
        )
        .route("/api/v1/metadata", get(handlers::metadata))
        .route(
            "/api/v1/query_exemplars",
            get(handlers::query_exemplars).post(handlers::query_exemplars_post),
        )
        .route("/api/v1/status/buildinfo", get(handlers::status_buildinfo))
        .route("/api/v1/status/config", get(handlers::status_config))
        .route("/api/v1/status/flags", get(handlers::status_flags))
        .route(
            "/api/v1/status/runtimeinfo",
            get(handlers::status_runtimeinfo),
        )
        .route("/api/v1/status/tsdb", get(handlers::status_tsdb))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_constructs_without_panicking() {
        let _ = router();
    }
}
