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
//! Issue #827: `Diff`, the diff of two selections, at the two Connect paths
//! only, in one statement.
//! Issue #825: `SelectHeatmap`, the heatmap of profile and span totals, at
//! the two Connect paths, in one statement.
//! Issue #828: `SelectMergeProfile`, the merged profile as pprof, at the two
//! Connect paths and `GET /api/profiles/v1/export`, in one statement.
//! Issue #829: `GET /api/profiles/v1/render`, `/render-diff` and
//! `/label-values`, and their compat paths `/pyroscope/render`,
//! `/pyroscope/render-diff` and `/pyroscope/label-values`, each in one
//! statement; `SelectMergeStacktraces` with `PROFILE_FORMAT_TREE`.
//! Issue #830: the exploration app's `settings.v1.SettingsService/{Get,Set,
//! Delete}`, `GetProfileStats` (and `GET /api/profiles/v1/stats`) and
//! `AnalyzeQuery` at the two Connect surfaces, one statement each and none
//! for `AnalyzeQuery`; `settings.v1.RecordingRulesService/*` and
//! `vcs.v1.VCSService/*` answer `unimplemented`.

mod app;
pub(crate) mod connect;
mod diff;
mod engine;
mod heatmap;
mod merge;
mod pprof;
mod render;
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
            "/api/profiles/v1/querier.v1.QuerierService/Diff",
            post(diff::diff),
        )
        .route(
            "/api/profiles/v1/querier.v1.QuerierService/SelectHeatmap",
            post(heatmap::select_heatmap),
        )
        .route(
            "/api/profiles/v1/querier.v1.QuerierService/SelectMergeProfile",
            post(pprof::select_merge_profile),
        )
        .route(
            "/api/profiles/v1/settings.v1.SettingsService/Get",
            post(app::settings_get),
        )
        .route(
            "/api/profiles/v1/settings.v1.SettingsService/Set",
            post(app::settings_set),
        )
        .route(
            "/api/profiles/v1/settings.v1.SettingsService/Delete",
            post(app::settings_delete),
        )
        .route(
            "/api/profiles/v1/querier.v1.QuerierService/GetProfileStats",
            post(app::profile_stats),
        )
        .route(
            "/api/profiles/v1/querier.v1.QuerierService/AnalyzeQuery",
            post(app::analyze_query),
        )
        .route(
            "/api/profiles/v1/settings.v1.RecordingRulesService/{name}",
            post(app::recording_rules),
        )
        .route("/api/profiles/v1/vcs.v1.VCSService/{name}", post(app::vcs))
        .route("/api/profiles/v1/stats", get(app::stats_get))
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
        .route("/api/profiles/v1/export", get(pprof::export_get))
        .route("/api/profiles/v1/render", get(render::render_get))
        .route("/api/profiles/v1/render-diff", get(render::render_diff_get))
        .route(
            "/api/profiles/v1/label-values",
            get(render::label_values_get),
        )
}

/// The compat surface: the Connect paths at the root, and the render
/// routes under `/pyroscope`. Mounting it is `crate::compat::apply_aliases`'s
/// job.
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
        .route("/querier.v1.QuerierService/Diff", post(diff::diff))
        .route(
            "/querier.v1.QuerierService/SelectHeatmap",
            post(heatmap::select_heatmap),
        )
        .route(
            "/querier.v1.QuerierService/SelectMergeProfile",
            post(pprof::select_merge_profile),
        )
        .route("/settings.v1.SettingsService/Get", post(app::settings_get))
        .route("/settings.v1.SettingsService/Set", post(app::settings_set))
        .route(
            "/settings.v1.SettingsService/Delete",
            post(app::settings_delete),
        )
        .route(
            "/querier.v1.QuerierService/GetProfileStats",
            post(app::profile_stats),
        )
        .route(
            "/querier.v1.QuerierService/AnalyzeQuery",
            post(app::analyze_query),
        )
        .route(
            "/settings.v1.RecordingRulesService/{name}",
            post(app::recording_rules),
        )
        .route("/vcs.v1.VCSService/{name}", post(app::vcs))
        .route("/pyroscope/render", get(render::render_get))
        .route("/pyroscope/render-diff", get(render::render_diff_get))
        .route("/pyroscope/label-values", get(render::label_values_get))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_and_compat_routers_merge_without_conflicts() {
        let _ = router().merge(compat_router());
    }
}
