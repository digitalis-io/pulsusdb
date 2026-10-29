//! The admin JSON API, `/admin/api/v1/…`: the same view values the pages
//! render.

use std::collections::BTreeMap;

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use pulsus_setup::{Inputs, Validated};
use serde_json::json;

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

/// The setup API's body: a JSON object of form fields to strings, read by
/// the same `Inputs::from_pairs` and `validate` as the form. A body that is
/// not such an object is `400`; refused inputs are `422`. Accepted inputs
/// answer with `warnings`, `[]` when there are none.
fn setup_validated(body: &[u8]) -> Result<Validated, Box<Response>> {
    let pairs: BTreeMap<String, String> = serde_json::from_slice(body).map_err(|e| {
        Box::new(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"errors": [{"field": null, "message": e.to_string()}]})),
            )
                .into_response(),
        )
    })?;
    pulsus_setup::validate(&Inputs::from_pairs(pairs)).map_err(|errors| {
        Box::new(
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({ "errors": errors })),
            )
                .into_response(),
        )
    })
}

pub(crate) async fn setup_validate(body: Bytes) -> Response {
    match setup_validated(&body) {
        Ok(v) => Json(json!({ "totals": v.totals(), "warnings": v.warnings })).into_response(),
        Err(res) => *res,
    }
}

pub(crate) async fn setup_bundle(body: Bytes) -> Response {
    match setup_validated(&body) {
        Ok(v) => {
            let files = pulsus_setup::bundle(&v);
            Json(json!({ "totals": v.totals(), "warnings": v.warnings, "files": files }))
                .into_response()
        }
        Err(res) => *res,
    }
}
