//! The admin JSON API, `/admin/api/v1/…`: the same view values the pages
//! render.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use super::views::{self, DatabaseView, StatusView};
use crate::app::AppState;

pub(crate) async fn status(State(state): State<AppState>) -> Json<StatusView> {
    Json(StatusView::build(&state).await)
}

pub(crate) async fn database(State(state): State<AppState>) -> Json<DatabaseView> {
    Json(DatabaseView::build(&state).await)
}

pub(crate) async fn config(State(state): State<AppState>) -> Response {
    match views::config_json(&state.config) {
        Ok(value) => Json(value).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "failed to render configuration");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "errors": [{"field": null, "message": "failed to render configuration"}]
                })),
            )
                .into_response()
        }
    }
}
