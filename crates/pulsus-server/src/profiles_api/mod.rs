//! The profiles query API (issue #821, docs/api.md §5): profile types,
//! label names, label values and series, each one handler bound at three
//! paths — the compat Connect path `/querier.v1.QuerierService/<M>`, the
//! native Connect path `/api/profiles/v1/querier.v1.QuerierService/<M>`,
//! and the native `GET` route. Every request sends one statement over the
//! series table, or none when its window ends before the lookback floor.

pub(crate) mod connect;
mod engine;
mod merge;
pub(crate) mod routes;

use axum::Router;
use axum::routing::{get, post};

use crate::app::AppState;

/// The native surface: the four Connect paths under `/api/profiles/v1`
/// and the four `GET` routes. `.route`-only with literal paths.
pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/profiles/v1/querier.v1.QuerierService/ProfileTypes",
            post(routes::profile_types),
        )
        .route(
            "/api/profiles/v1/querier.v1.QuerierService/LabelNames",
            post(routes::label_names),
        )
        .route(
            "/api/profiles/v1/querier.v1.QuerierService/LabelValues",
            post(routes::label_values),
        )
        .route(
            "/api/profiles/v1/querier.v1.QuerierService/Series",
            post(routes::series),
        )
        .route("/api/profiles/v1/types", get(routes::types_get))
        .route(
            "/api/profiles/v1/labels",
            get(routes::labels_get).post(routes::labels_get),
        )
        .route(
            "/api/profiles/v1/label/{name}/values",
            get(routes::label_values_get),
        )
        .route(
            "/api/profiles/v1/series",
            get(routes::series_get).post(routes::series_get),
        )
}

/// The compat surface: the four Connect paths at the root. Mounting it is
/// `crate::compat::apply_aliases`'s job.
pub(crate) fn compat_router() -> Router<AppState> {
    Router::new()
        .route(
            "/querier.v1.QuerierService/ProfileTypes",
            post(routes::profile_types),
        )
        .route(
            "/querier.v1.QuerierService/LabelNames",
            post(routes::label_names),
        )
        .route(
            "/querier.v1.QuerierService/LabelValues",
            post(routes::label_values),
        )
        .route("/querier.v1.QuerierService/Series", post(routes::series))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_and_compat_routers_merge_without_conflicts() {
        let _ = router().merge(compat_router());
    }
}
