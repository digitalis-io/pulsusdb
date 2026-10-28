//! The view values each admin page and its JSON endpoint are built from.

use pulsus_config::Config;
use serde::Serialize;

use crate::app::AppState;

/// Step-1 stub.
pub(crate) fn config_json(_cfg: &Config) -> Result<serde_json::Value, String> {
    Err("not implemented".to_string())
}

/// Step-1 stub.
#[derive(Serialize)]
pub(crate) struct StatusView {}

impl StatusView {
    pub(crate) const FIELDS: &[(&str, &str)] = &[];

    pub(crate) async fn build(_state: &AppState) -> Self {
        StatusView {}
    }
}

/// Step-1 stub.
#[derive(Serialize)]
pub(crate) struct DatabaseView {}

impl DatabaseView {
    pub(crate) const FIELDS: &[(&str, &str)] = &[];

    pub(crate) async fn build(_state: &AppState) -> Self {
        DatabaseView {}
    }
}
