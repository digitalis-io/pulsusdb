//! The admin HTML pages: each renders its view value as one `<dl>` row per
//! field, every value through `html_text`.

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use serde::Serialize;
use serde_json::Value;

use super::html::{html_text, layout};
use super::views::{self, DatabaseView, StatusView};
use crate::app::AppState;

/// A JSON value as page text: the string itself, `—` for `null`, the JSON
/// text otherwise.
fn value_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => "—".to_string(),
        other => other.to_string(),
    }
}

/// One `<dt>`/`<dd>` row per `fields` entry, from `view`'s JSON.
fn rows(fields: &[(&str, &str)], view: &impl Serialize) -> Result<String, String> {
    let json = serde_json::to_value(view).map_err(|e| e.to_string())?;
    let mut out = String::from("<dl>");
    for (key, label) in fields {
        out.push_str(&format!(
            "<dt>{label}</dt><dd>{}</dd>",
            html_text(&value_text(&json[*key]))
        ));
    }
    out.push_str("</dl>");
    Ok(out)
}

fn render(title: &str, body: Result<String, String>) -> Response {
    match body {
        Ok(body) => Html(layout(title, &body)).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "failed to render admin page");
            (StatusCode::INTERNAL_SERVER_ERROR, "failed to render page").into_response()
        }
    }
}

pub(crate) async fn status(State(state): State<AppState>) -> Response {
    let view = StatusView::build(&state).await;
    render("Status", rows(StatusView::FIELDS, &view))
}

pub(crate) async fn database(State(state): State<AppState>) -> Response {
    let view = DatabaseView::build(&state).await;
    let body = rows(DatabaseView::FIELDS, &view).map(|mut body| {
        if let Some(levels) = &view.levels {
            body.push_str(
                "<table><thead><tr><th>Table</th><th>Width (ms)</th><th>From (ms)</th></tr>\
                 </thead><tbody>",
            );
            for level in levels {
                body.push_str(&format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
                    html_text(&level.table),
                    html_text(&level.res_ms.to_string()),
                    html_text(&level.from_ms.to_string()),
                ));
            }
            body.push_str("</tbody></table>");
        }
        body
    });
    render("Database", body)
}

pub(crate) async fn config(State(state): State<AppState>) -> Response {
    let yaml = views::config_json(&state.config)
        .and_then(|value| serde_norway::to_string(&value).map_err(|e| e.to_string()));
    match yaml {
        Ok(yaml) => Html(layout(
            "Configuration",
            &format!("<pre>{}</pre>", html_text(&yaml)),
        ))
        .into_response(),
        Err(err) => {
            tracing::error!(error = %err, "failed to render configuration");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to render configuration",
            )
                .into_response()
        }
    }
}

pub(crate) async fn css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css")],
        include_str!("admin.css"),
    )
}
