//! Issue #831: ad hoc profile uploads —
//! `adhocprofiles.v1.AdHocProfileService/{Upload,Get,List,Diff}`, at the
//! compat Connect path and under `/api/profiles/v1/`.
//!
//! `Upload` converts the file here and sends one statement, the insert of
//! its rows (`pulsus_read::profiles::adhoc`), and answers the flame graph
//! drawn here from the converted stacks; `Get` and `Diff` send S14 once,
//! `List` S15 once. In the order: the tenant, the codec and the body, then
//! each method's own checks, as the reference makes them
//! (`pkg/adhocprofiles/adhocprofiles.go`), with its error texts.

use axum::body::Body;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use pulsus_model::Tenant;
use pulsus_profiles_wire::profileql_adhoc_v1::{
    AdHocProfilesDiffRequest, AdHocProfilesDiffResponse, AdHocProfilesGetRequest,
    AdHocProfilesGetResponse, AdHocProfilesListRequest, AdHocProfilesListResponse,
    AdHocProfilesProfileMetadata, AdHocProfilesUploadRequest,
};
use pulsus_read::profiles::ProfilesReadConfig;
use pulsus_read::profiles::adhoc::convert::{self, ConvertError};
use pulsus_read::profiles::adhoc::{self, AdhocRow, Selected};

use crate::app::AppState;

use super::connect::{Code, ConnectError};
use super::engine::with_engine;
use super::routes::{finish, now_ms, prelude};

/// The reference's text for a profile over its size limit, 4 MiB written
/// as `humanize.Bytes` writes it.
const TOO_LARGE: &str = "profile payload size exceeds limit of 4.2 MB";

/// The same for a pprof over the limit once decompressed.
const UNCOMPRESSED_TOO_LARGE: &str = "uncompressed profile payload size exceeds limit of 4.2 MB";

fn unknown(message: String) -> ConnectError {
    ConnectError::new(Code::Unknown, message)
}

fn invalid_id(id: &str) -> ConnectError {
    ConnectError::new(
        Code::InvalidArgument,
        format!("id '{id}' is invalid: can only contain [a-zA-Z0-9_-.]"),
    )
}

/// An id's canonical text, the form S14 tags its rows with; `None` when
/// it is not an upload's id.
fn canonical(id: &str) -> Option<String> {
    adhoc::is_upload_id(id).then(|| id.to_ascii_lowercase())
}

/// The flame graph answer of one selected type.
fn drawn(
    sel: &Selected<'_>,
    selected: &AdhocRow,
    m: i64,
) -> Result<pulsus_profiles_wire::flamebearer::Flamebearer, ConnectError> {
    Ok(adhoc::render(selected, &sel.tree, m)?)
}

fn type_names(sel: &Selected<'_>) -> Vec<String> {
    sel.types.iter().map(|t| t.display_name.clone()).collect()
}

/// `POST …/adhocprofiles.v1.AdHocProfileService/Upload`.
pub(crate) async fn upload(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, msg) = match prelude::<AdHocProfilesUploadRequest>(
        &headers,
        body,
        "adhocprofiles.v1.AdHocProfilesUploadRequest",
    )
    .await
    {
        Ok(p) => p,
        Err(refused) => return refused.into_response(),
    };
    finish(codec, upload_answer(&state, &tenant, msg).await)
}

async fn upload_answer(
    state: &AppState,
    tenant: &Tenant,
    msg: AdHocProfilesUploadRequest,
) -> Result<AdHocProfilesGetResponse, ConnectError> {
    let config = ProfilesReadConfig::from_config(&state.config);
    let name = adhoc::sanitize_name(&msg.name);
    let m = adhoc::max_nodes(msg.max_nodes, &config)
        .map_err(|e| unknown(format!("could not determine max nodes: {e}")))?;
    if msg.profile.len() > adhoc::MAX_PROFILE_BYTES {
        return Err(ConnectError::new(Code::InvalidArgument, TOO_LARGE));
    }
    let bytes = adhoc::decode_base64(&msg.profile).map_err(|e| {
        unknown(format!(
            "failed to parse profile: failed to upload profile: {e}"
        ))
    })?;
    let format = convert::detect(&name, &bytes)
        .map_err(|e| unknown(format!("failed to parse profile: {e}")))?;
    let converted = convert::convert(format, &name, &bytes).map_err(|e| match e {
        ConvertError::TooLarge => {
            ConnectError::new(Code::InvalidArgument, UNCOMPRESSED_TOO_LARGE)
        }
        ConvertError::Invalid(e) => unknown(format!(
            "failed to parse profile: unable to process the profile. The profile was detected as \"{}\": {e}",
            format.as_str()
        )),
    })?;
    let uploaded_at = now_ms();
    // Minted once, so a rerun on another server inserts under the same
    // deduplication token (D8).
    let id = adhoc::mint_id(uploaded_at);
    let (id_ref, name_ref, converted_ref) = (id.as_str(), name.as_str(), &converted);
    with_engine(state, tenant, |engine| async move {
        Ok(engine
            .store_upload(id_ref, name_ref, uploaded_at, converted_ref)
            .await?)
    })
    .await?;
    let first = &converted.types[0];
    let type_row = AdhocRow {
        part: 1,
        selected: Some(1),
        format: converted.format.as_str().to_string(),
        display_name: first.display_name.clone(),
        spy_name: first.spy_name.clone(),
        sample_rate: first.sample_rate,
        units: first.units.clone(),
        ..AdhocRow::default()
    };
    let fb = adhoc::render(&type_row, &adhoc::tree_rows(&first.stacks), m)?;
    Ok(AdHocProfilesGetResponse {
        id,
        name,
        uploaded_at,
        profile_type: first.display_name.clone(),
        profile_types: converted
            .types
            .iter()
            .map(|t| t.display_name.clone())
            .collect(),
        flamebearer_profile: adhoc::answer_text(&fb),
    })
}

/// `POST …/adhocprofiles.v1.AdHocProfileService/Get`.
pub(crate) async fn get(State(state): State<AppState>, headers: HeaderMap, body: Body) -> Response {
    let (tenant, codec, msg) = match prelude::<AdHocProfilesGetRequest>(
        &headers,
        body,
        "adhocprofiles.v1.AdHocProfilesGetRequest",
    )
    .await
    {
        Ok(p) => p,
        Err(refused) => return refused.into_response(),
    };
    finish(codec, get_answer(&state, &tenant, msg).await)
}

async fn get_answer(
    state: &AppState,
    tenant: &Tenant,
    msg: AdHocProfilesGetRequest,
) -> Result<AdHocProfilesGetResponse, ConnectError> {
    let config = ProfilesReadConfig::from_config(&state.config);
    let id = msg.id;
    if !adhoc::valid_id(&id) {
        return Err(invalid_id(&id));
    }
    let not_found = || ConnectError::new(Code::NotFound, format!("profile {id} not found"));
    let uid = canonical(&id).ok_or_else(not_found)?;
    let sql = adhoc::trees_sql(tenant, &config, &[&uid], msg.profile_type.as_deref());
    let sql = sql.as_str();
    let rows = with_engine(state, tenant, |engine| async move {
        Ok(engine.read_adhoc(sql).await?)
    })
    .await?;
    let sel = adhoc::select(&rows, &uid);
    let selected = sel.selected.ok_or_else(not_found)?;
    let m = adhoc::max_nodes(msg.max_nodes, &config)
        .map_err(|e| unknown(format!("could not determine max nodes: {e}")))?;
    let fb = drawn(&sel, selected, m)?;
    Ok(AdHocProfilesGetResponse {
        id: id.clone(),
        name: selected.upload_name.clone(),
        uploaded_at: selected.uploaded_at,
        profile_type: selected.display_name.clone(),
        profile_types: type_names(&sel),
        flamebearer_profile: adhoc::answer_text(&fb),
    })
}

/// `POST …/adhocprofiles.v1.AdHocProfileService/List`.
pub(crate) async fn list(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, _) = match prelude::<AdHocProfilesListRequest>(
        &headers,
        body,
        "adhocprofiles.v1.AdHocProfilesListRequest",
    )
    .await
    {
        Ok(p) => p,
        Err(refused) => return refused.into_response(),
    };
    let config = ProfilesReadConfig::from_config(&state.config);
    let sql = adhoc::list_sql(&tenant, &config);
    let sql = sql.as_str();
    let answer = with_engine(&state, &tenant, |engine| async move {
        Ok(engine.read_uploads(sql).await?)
    })
    .await
    .map(|rows| AdHocProfilesListResponse {
        profiles: rows
            .into_iter()
            .map(|r| AdHocProfilesProfileMetadata {
                id: r.id,
                name: r.name,
                uploaded_at: r.uploaded_at,
            })
            .collect(),
    });
    finish(codec, answer)
}

/// `POST …/adhocprofiles.v1.AdHocProfileService/Diff`.
pub(crate) async fn diff(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, msg) = match prelude::<AdHocProfilesDiffRequest>(
        &headers,
        body,
        "adhocprofiles.v1.AdHocProfilesDiffRequest",
    )
    .await
    {
        Ok(p) => p,
        Err(refused) => return refused.into_response(),
    };
    finish(codec, diff_answer(&state, &tenant, msg).await)
}

async fn diff_answer(
    state: &AppState,
    tenant: &Tenant,
    msg: AdHocProfilesDiffRequest,
) -> Result<AdHocProfilesDiffResponse, ConnectError> {
    let config = ProfilesReadConfig::from_config(&state.config);
    let (left, right) = (msg.left_id, msg.right_id);
    for id in [&left, &right] {
        if !adhoc::valid_id(id) {
            return Err(invalid_id(id));
        }
    }
    let m = adhoc::max_nodes(msg.max_nodes, &config).map_err(|e| {
        ConnectError::new(
            Code::InvalidArgument,
            format!("could not determine max nodes: {e}"),
        )
    })?;
    let missing = |side: &str, id: &str| {
        ConnectError::new(Code::NotFound, format!("{side} profile {id} not found"))
    };
    let lid = canonical(&left).ok_or_else(|| missing("left", &left))?;
    let rid = canonical(&right).ok_or_else(|| missing("right", &right))?;
    let sql = adhoc::trees_sql(tenant, &config, &[&lid, &rid], msg.profile_type.as_deref());
    let sql = sql.as_str();
    let rows = with_engine(state, tenant, |engine| async move {
        Ok(engine.read_adhoc(sql).await?)
    })
    .await?;
    let ls = adhoc::select(&rows, &lid);
    let lsel = ls.selected.ok_or_else(|| missing("left", &left))?;
    let rs = adhoc::select(&rows, &rid);
    let rsel = rs.selected.ok_or_else(|| missing("right", &right))?;
    let (left_types, right_types) = (type_names(&ls), type_names(&rs));
    let common: Vec<String> = left_types
        .iter()
        .filter(|t| right_types.contains(t))
        .cloned()
        .collect();
    if common.is_empty() {
        return Err(ConnectError::new(
            Code::InvalidArgument,
            format!(
                "no common profile types between left ([{}]) and right ([{}])",
                left_types.join(" "),
                right_types.join(" ")
            ),
        ));
    }
    if lsel.display_name != rsel.display_name {
        return Err(ConnectError::new(
            Code::InvalidArgument,
            format!(
                "profile type mismatch: left profile uses '{}', right profile uses '{}'; please specify a profile type from the common types: [{}]",
                lsel.display_name,
                rsel.display_name,
                common.join(" ")
            ),
        ));
    }
    let fb = adhoc::diff(&drawn(&ls, lsel, m)?, &drawn(&rs, rsel, m)?, m)?;
    Ok(AdHocProfilesDiffResponse {
        profile_types: common,
        flamebearer_profile: adhoc::answer_text(&fb),
    })
}
