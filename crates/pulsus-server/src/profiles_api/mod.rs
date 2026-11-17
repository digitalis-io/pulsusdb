//! The profiles query API (issue #821, docs/api.md §5): profile types,
//! label names, label values and series, each one handler bound at three
//! paths — the compat Connect path `/querier.v1.QuerierService/<M>`, the
//! native Connect path `/api/profiles/v1/querier.v1.QuerierService/<M>`,
//! and the native `GET` route. Every request sends one statement over the
//! series table, or none when its window ends before the lookback floor.
//! Issue #823: `SelectMergeStacktraces`, the merged flame graph, at the two
//! Connect paths and `GET /api/profiles/v1/merge`, in one statement.
//! Issue #824: `SelectSeries`, the time series of profile totals, at the two
//! Connect paths and `GET /api/profiles/v1/select_series`, in one statement.
//! Issue #825: `SelectHeatmap`, the heatmap of profile and span totals, at
//! the two Connect paths, in one statement.

pub(crate) mod connect;
mod engine;
mod heatmap;
mod merge;
pub(crate) mod routes;
mod series;

use axum::Router;
use axum::routing::{get, post};

use crate::app::AppState;

/// The native surface: the Connect paths under `/api/profiles/v1` and the
/// `GET` routes. `.route`-only with literal paths.
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
        .route(
            "/api/profiles/v1/querier.v1.QuerierService/SelectMergeStacktraces",
            post(merge::select_merge_stacktraces),
        )
        .route(
            "/api/profiles/v1/querier.v1.QuerierService/SelectSeries",
            post(series::select_series),
        )
        .route(
            "/api/profiles/v1/querier.v1.QuerierService/SelectHeatmap",
            post(heatmap::select_heatmap),
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
        .route("/api/profiles/v1/merge", get(merge::merge_get))
        .route(
            "/api/profiles/v1/select_series",
            get(series::select_series_get),
        )
}

/// The compat surface: the Connect paths at the root. Mounting it is
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
        .route(
            "/querier.v1.QuerierService/SelectMergeStacktraces",
            post(merge::select_merge_stacktraces),
        )
        .route(
            "/querier.v1.QuerierService/SelectSeries",
            post(series::select_series),
        )
        .route(
            "/querier.v1.QuerierService/SelectHeatmap",
            post(heatmap::select_heatmap),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_and_compat_routers_merge_without_conflicts() {
        let _ = router().merge(compat_router());
    }
}
