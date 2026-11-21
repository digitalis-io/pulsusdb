//! Issue #830: the exploration app's settings, profile stats and query
//! analysis, and the two services it calls that are not served.
//!
//! `settings.v1.SettingsService/{Get,Set,Delete}` read and write
//! `profile_settings`, one statement each (`pulsus_read::profiles::app`);
//! `GetProfileStats` and `GET /api/profiles/v1/stats` read the series
//! table, one statement; `AnalyzeQuery` answers the empty message with no
//! statement; `settings.v1.RecordingRulesService/*` and
//! `vcs.v1.VCSService/*` answer `unimplemented` without reading the request.
//! In the order: the tenant, the codec and the body, then each method's own
//! checks.

use axum::body::Body;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use pulsus_model::Tenant;
use pulsus_profiles_wire::querier_v1::{AnalyzeQueryRequest, AnalyzeQueryResponse};
use pulsus_profiles_wire::settings_v1::{
    DeleteSettingsRequest, DeleteSettingsResponse, GetSettingsRequest, GetSettingsResponse,
    SetSettingsRequest, SetSettingsResponse, Setting,
};
use pulsus_profiles_wire::types_v1::{GetProfileStatsRequest, GetProfileStatsResponse};
use pulsus_read::profiles::ProfilesReadConfig;
use pulsus_read::profiles::app::{self, NEWER_WRITTEN, Written};

use crate::app::AppState;

use super::connect::{self, Code, Codec, ConnectError};
use super::engine::with_engine;
use super::routes::{finish, now_ms, prelude, tenant_of};

/// The recording rules service's answer (D8).
const RULES_NOT_SERVED: &str = "recording rules are not served";

/// The VCS service's answer, the reference's text for `GithubApp` (D8).
const VCS_NOT_CONFIGURED: &str = "GitHub integration is not configured";

/// Sends one settings write; `Refused` when a newer write is stored.
async fn write(state: &AppState, tenant: &Tenant, sql: &str) -> Result<Written, ConnectError> {
    with_engine(state, tenant, |engine| async move {
        Ok(engine.write_setting(sql).await?)
    })
    .await
}

/// `POST …/settings.v1.SettingsService/Get`.
pub(crate) async fn settings_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, _) =
        match prelude::<GetSettingsRequest>(&headers, body, "settings.v1.GetSettingsRequest").await
        {
            Ok(p) => p,
            Err(refused) => return refused.into_response(),
        };
    let sql = app::settings_get_sql(&tenant);
    let sql = sql.as_str();
    let answer = with_engine(&state, &tenant, |engine| async move {
        Ok(engine.read_settings_rows(sql).await?)
    })
    .await
    .map(|rows| GetSettingsResponse {
        settings: rows
            .into_iter()
            .map(|r| Setting {
                name: r.name,
                value: r.value,
                modified_at: r.modified_at,
            })
            .collect(),
    });
    finish(codec, answer)
}

/// `POST …/settings.v1.SettingsService/Set`: no `modifiedAt` is now, and
/// a write older than the stored one is `already_exists`.
pub(crate) async fn settings_set(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, msg) =
        match prelude::<SetSettingsRequest>(&headers, body, "settings.v1.SetSettingsRequest").await
        {
            Ok(p) => p,
            Err(refused) => return refused.into_response(),
        };
    let Some(mut setting) = msg.setting else {
        return ConnectError::new(Code::InvalidArgument, "no setting values provided")
            .into_response();
    };
    if setting.modified_at <= 0 {
        setting.modified_at = now_ms();
    }
    let sql = app::settings_write_sql(
        &tenant,
        &setting.name,
        &setting.value,
        setting.modified_at,
        false,
    );
    let answer = match write(&state, &tenant, &sql).await {
        Ok(Written::Stored) => Ok(SetSettingsResponse {
            setting: Some(setting),
        }),
        Ok(Written::Refused) => Err(ConnectError::new(
            Code::AlreadyExists,
            format!("failed to update {}: {NEWER_WRITTEN}", setting.name),
        )),
        Err(e) => Err(e),
    };
    finish(codec, answer)
}

/// `POST …/settings.v1.SettingsService/Delete`: a tombstone at now, which
/// keeps its time (D5).
pub(crate) async fn settings_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, msg) =
        match prelude::<DeleteSettingsRequest>(&headers, body, "settings.v1.DeleteSettingsRequest")
            .await
        {
            Ok(p) => p,
            Err(refused) => return refused.into_response(),
        };
    if msg.name.is_empty() {
        return ConnectError::new(Code::InvalidArgument, "no setting name provided")
            .into_response();
    }
    let sql = app::settings_write_sql(&tenant, &msg.name, "", now_ms(), true);
    let answer = match write(&state, &tenant, &sql).await {
        Ok(Written::Stored) => Ok(DeleteSettingsResponse {}),
        Ok(Written::Refused) => Err(ConnectError::new(
            Code::AlreadyExists,
            format!("failed to delete {}: {NEWER_WRITTEN}", msg.name),
        )),
        Err(e) => Err(e),
    };
    finish(codec, answer)
}

/// The tenant's stats, in one statement over the series table (D6).
async fn stats(state: &AppState, tenant: &Tenant) -> Result<GetProfileStatsResponse, ConnectError> {
    let config = ProfilesReadConfig::from_config(&state.config);
    let sql = app::stats_sql(tenant, &config.series_table);
    let sql = sql.as_str();
    let row = with_engine(state, tenant, |engine| async move {
        Ok(engine.read_stats(sql).await?)
    })
    .await?;
    Ok(GetProfileStatsResponse {
        data_ingested: row.ingested,
        oldest_profile_time: row.oldest_ms,
        newest_profile_time: row.newest_ms,
    })
}

/// `POST …/querier.v1.QuerierService/GetProfileStats`.
pub(crate) async fn profile_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, _) =
        match prelude::<GetProfileStatsRequest>(&headers, body, "types.v1.GetProfileStatsRequest")
            .await
        {
            Ok(p) => p,
            Err(refused) => return refused.into_response(),
        };
    finish(codec, stats(&state, &tenant).await)
}

/// `GET /api/profiles/v1/stats`: `GetProfileStats`' answer in JSON.
pub(crate) async fn stats_get(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let tenant = match tenant_of(&headers) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    finish(Codec::Json, stats(&state, &tenant).await)
}

/// `POST …/querier.v1.QuerierService/AnalyzeQuery`: the request is read,
/// and the answer is the empty message, with no statement (D7).
pub(crate) async fn analyze_query(headers: HeaderMap, body: Body) -> Response {
    match prelude::<AnalyzeQueryRequest>(&headers, body, "querier.v1.AnalyzeQueryRequest").await {
        Ok((_, codec, _)) => connect::respond(codec, &AnalyzeQueryResponse::default()),
        Err(refused) => refused.into_response(),
    }
}

/// `POST …/settings.v1.RecordingRulesService/{name}`: not served.
pub(crate) async fn recording_rules() -> Response {
    ConnectError::new(Code::Unimplemented, RULES_NOT_SERVED).into_response()
}

/// `POST …/vcs.v1.VCSService/{name}`: not served.
pub(crate) async fn vcs() -> Response {
    ConnectError::new(Code::Unimplemented, VCS_NOT_CONFIGURED).into_response()
}
