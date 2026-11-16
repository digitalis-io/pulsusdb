//! Issue #821: the four catalogue methods' handlers — the Connect calls
//! and the native `GET` routes — over one plan-and-read per request
//! (`pulsus_read::profiles::catalogue`).
//!
//! Every handler reads the tenant first, before any other header or the
//! body. A `GET` route's `from` and `until` are read by the reference's
//! time parser ([`attime`]); an absent one leaves the field 0, so the route
//! behaves as its method with the field unset.

use axum::body::Body;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};
use pulsus_model::{Tenant, tenant_from_headers};
use pulsus_profiles_wire::querier_v1::{
    ProfileTypesRequest, ProfileTypesResponse, SeriesRequest, SeriesResponse,
};
use pulsus_profiles_wire::types_v1::{
    LabelNamesRequest, LabelNamesResponse, LabelPair, LabelValuesRequest, LabelValuesResponse,
    Labels, ProfileType,
};
use pulsus_read::profiles::catalogue::{self, CatalogueRequest, Method};
use pulsus_read::profiles::{Answer, ProfilesReadConfig};

use crate::app::AppState;
use crate::logs_api::params::parse_pairs;

use super::connect::{self, Code, Codec, ConnectError};
use super::engine::with_engine;

/// The capability parameter (`pkg/featureflags/client_capability.go:86`).
const UTF8_LABEL_NAMES: &str = "allow-utf8-labelnames";

pub(super) fn tenant_of(headers: &HeaderMap) -> Result<Tenant, ConnectError> {
    tenant_from_headers(headers)
        .map_err(|_| ConnectError::new(Code::InvalidArgument, "invalid X-Scope-OrgID"))
}

pub(super) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Whether the client may be sent label names outside the legacy rule: any
/// `Accept` value, split on `,`, with the parameter
/// `allow-utf8-labelnames=true` (name case-insensitive).
pub(crate) fn allows_utf8_label_names(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .flat_map(|media| media.split(';').skip(1))
        .filter_map(|param| param.split_once('='))
        .any(|(k, v)| {
            k.trim().eq_ignore_ascii_case(UTF8_LABEL_NAMES) && v.trim().trim_matches('"') == "true"
        })
}

/// The reference's time parser (`pkg/og/util/attime/attime.go`), in
/// milliseconds. On the value with `_`, `,` and spaces removed: all digits
/// is a date when it is eight digits forming a valid `YYYYMMDD` (UTC
/// midnight), else milliseconds at 13 digits, microseconds at 16,
/// nanoseconds at 19 and seconds at any other length. Otherwise `now` plus
/// the offset after the first `+` (or, with none, the first `-`), every
/// number–unit pair of it added ([`offset_seconds`]).
pub(crate) fn attime(s: &str, now_ms: i64) -> i64 {
    let s: String = s
        .trim()
        .chars()
        .filter(|c| !matches!(c, '_' | ',' | ' '))
        .collect();
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
        if s.len() == 8
            && let Ok(date) = chrono::NaiveDate::parse_from_str(&s, "%Y%m%d")
            && chrono::Datelike::year(&date) > 1900
        {
            return date
                .and_hms_opt(0, 0, 0)
                .map(|t| t.and_utc().timestamp_millis())
                .unwrap_or(0);
        }
        // Go's `strconv.Atoi` answers the largest value on overflow.
        let v: i64 = s.parse().unwrap_or(i64::MAX);
        return match s.len() {
            13 => v,
            16 => v / 1_000,
            19 => v / 1_000_000,
            _ => v.saturating_mul(1_000),
        };
    }
    let offset = match s.find('+').or_else(|| s.find('-')) {
        Some(i) => &s[i..],
        None => "",
    };
    now_ms.saturating_add(offset_seconds(offset).saturating_mul(1_000))
}

/// A relative offset: an optional sign, then number–unit pairs, each unit
/// chosen by prefix as the reference chooses it, every pair added — so
/// `now-1h30m` is 90 minutes ago, as a Go duration or PromQL reads it.
/// Deliberate difference (owner, 2026-11-16, "Parse correctly"): the
/// reference's unit loop (`pkg/og/util/attime/attime.go:79-85`) takes every
/// byte after the first number as its unit, so it reads `now-1h30m` as one
/// hour.
fn offset_seconds(offset: &str) -> i64 {
    let (sign, mut rest) = if let Some(r) = offset.strip_prefix('-') {
        (-1, r)
    } else {
        (1, offset.strip_prefix('+').unwrap_or(offset))
    };
    let mut total: i64 = 0;
    while !rest.is_empty() {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        let num: i64 = if digits == 0 {
            0
        } else {
            rest[..digits].parse().unwrap_or(i64::MAX)
        };
        rest = &rest[digits..];
        let unit_len = rest.bytes().take_while(|b| !b.is_ascii_digit()).count();
        let mult = unit_seconds(&rest[..unit_len]);
        rest = &rest[unit_len..];
        total = total.saturating_add(num.saturating_mul(mult));
    }
    total.saturating_mul(sign)
}

/// A unit's seconds by prefix (`attime.go:88-111`); anything else is 0.
fn unit_seconds(unit: &str) -> i64 {
    if unit.starts_with('s') {
        1
    } else if unit.starts_with("mon") || unit.starts_with('M') {
        30 * 86_400
    } else if unit.starts_with("min") || unit.starts_with('m') {
        60
    } else if unit.starts_with('h') {
        3_600
    } else if unit.starts_with('d') {
        86_400
    } else if unit.starts_with('w') {
        7 * 86_400
    } else if unit.starts_with('y') {
        365 * 86_400
    } else {
        0
    }
}

// ---------------------------------------------------------------------
// The four methods
// ---------------------------------------------------------------------

/// Plans `req` and, unless its window is empty, reads its one statement.
async fn run(
    state: &AppState,
    tenant: &Tenant,
    req: CatalogueRequest<'_>,
) -> Result<Option<Answer>, ConnectError> {
    let config = ProfilesReadConfig::from_config(&state.config);
    let Some(read) = catalogue::plan(tenant, &config.series_table, config.limits, &req, now_ms())?
    else {
        return Ok(None);
    };
    let read = &read;
    let answer = with_engine(state, tenant, |engine| async move {
        Ok(engine.read(read).await?)
    })
    .await?;
    Ok(Some(answer))
}

fn strings(answer: Option<Answer>) -> Vec<String> {
    match answer {
        Some(Answer::Strings(v)) => v,
        _ => Vec::new(),
    }
}

fn refs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

/// Each id split on `:`; five or six parts make a type from the first five,
/// anything else is skipped (`query_profile_types.go:58-67`).
fn profile_type(id: String) -> Option<ProfileType> {
    let parts: Vec<&str> = id.split(':').collect();
    if parts.len() != 5 && parts.len() != 6 {
        return None;
    }
    Some(ProfileType {
        name: parts[0].to_string(),
        sample_type: parts[1].to_string(),
        sample_unit: parts[2].to_string(),
        period_type: parts[3].to_string(),
        period_unit: parts[4].to_string(),
        id,
    })
}

async fn profile_types_answer(
    state: &AppState,
    tenant: &Tenant,
    msg: ProfileTypesRequest,
) -> Result<ProfileTypesResponse, ConnectError> {
    let answer = run(
        state,
        tenant,
        CatalogueRequest {
            method: Method::ProfileTypes,
            matchers: &[],
            start_ms: msg.start,
            end_ms: msg.end,
        },
    )
    .await?;
    Ok(ProfileTypesResponse {
        profile_types: strings(answer)
            .into_iter()
            .filter_map(profile_type)
            .collect(),
    })
}

async fn label_names_answer(
    state: &AppState,
    tenant: &Tenant,
    msg: LabelNamesRequest,
    utf8: bool,
) -> Result<LabelNamesResponse, ConnectError> {
    let matchers = refs(&msg.matchers);
    let answer = run(
        state,
        tenant,
        CatalogueRequest {
            method: Method::LabelNames { utf8 },
            matchers: &matchers,
            start_ms: msg.start,
            end_ms: msg.end,
        },
    )
    .await?;
    Ok(LabelNamesResponse {
        names: strings(answer),
    })
}

async fn label_values_answer(
    state: &AppState,
    tenant: &Tenant,
    msg: LabelValuesRequest,
) -> Result<LabelValuesResponse, ConnectError> {
    let matchers = refs(&msg.matchers);
    let answer = run(
        state,
        tenant,
        CatalogueRequest {
            method: Method::LabelValues { name: &msg.name },
            matchers: &matchers,
            start_ms: msg.start,
            end_ms: msg.end,
        },
    )
    .await?;
    Ok(LabelValuesResponse {
        names: strings(answer),
    })
}

async fn series_answer(
    state: &AppState,
    tenant: &Tenant,
    msg: SeriesRequest,
    utf8: bool,
) -> Result<SeriesResponse, ConnectError> {
    let matchers = refs(&msg.matchers);
    let answer = run(
        state,
        tenant,
        CatalogueRequest {
            method: Method::Series {
                label_names: &msg.label_names,
                utf8,
            },
            matchers: &matchers,
            start_ms: msg.start,
            end_ms: msg.end,
        },
    )
    .await?;
    let sets = match answer {
        Some(Answer::Sets(sets)) => sets,
        _ => Vec::new(),
    };
    Ok(SeriesResponse {
        labels_set: sets
            .into_iter()
            .map(|set| Labels {
                labels: set
                    .into_iter()
                    .map(|(name, value)| LabelPair { name, value })
                    .collect(),
            })
            .collect(),
    })
}

// ---------------------------------------------------------------------
// The Connect calls
// ---------------------------------------------------------------------

/// Why a Connect call is answered before its method runs.
pub(super) enum Refused {
    Connect(ConnectError),
    Media(connect::UnsupportedMedia),
}

impl IntoResponse for Refused {
    fn into_response(self) -> Response {
        match self {
            Refused::Connect(e) => e.into_response(),
            Refused::Media(m) => m.into_response(),
        }
    }
}

/// The tenant, then the codec, then the body, then the message.
pub(super) async fn prelude<M>(
    headers: &HeaderMap,
    body: Body,
    full_name: &str,
) -> Result<(Tenant, Codec, M), Refused>
where
    M: prost::Message + Default + serde::de::DeserializeOwned,
{
    let tenant = tenant_of(headers).map_err(Refused::Connect)?;
    let codec = connect::request_codec(headers).map_err(Refused::Media)?;
    let bytes = connect::read_body(headers, body)
        .await
        .map_err(Refused::Connect)?;
    let msg = connect::decode(codec, &bytes, full_name).map_err(Refused::Connect)?;
    Ok((tenant, codec, msg))
}

pub(super) fn finish<M>(codec: Codec, answer: Result<M, ConnectError>) -> Response
where
    M: prost::Message + serde::Serialize,
{
    match answer {
        Ok(msg) => connect::respond(codec, &msg),
        Err(e) => e.into_response(),
    }
}

/// `POST …/querier.v1.QuerierService/ProfileTypes`.
pub(crate) async fn profile_types(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, msg) = match prelude::<ProfileTypesRequest>(
        &headers,
        body,
        "querier.v1.ProfileTypesRequest",
    )
    .await
    {
        Ok(p) => p,
        Err(refused) => return refused.into_response(),
    };
    finish(codec, profile_types_answer(&state, &tenant, msg).await)
}

/// `POST …/querier.v1.QuerierService/LabelNames`.
pub(crate) async fn label_names(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, msg) =
        match prelude::<LabelNamesRequest>(&headers, body, "types.v1.LabelNamesRequest").await {
            Ok(p) => p,
            Err(refused) => return refused.into_response(),
        };
    let utf8 = allows_utf8_label_names(&headers);
    finish(codec, label_names_answer(&state, &tenant, msg, utf8).await)
}

/// `POST …/querier.v1.QuerierService/LabelValues`.
pub(crate) async fn label_values(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, msg) =
        match prelude::<LabelValuesRequest>(&headers, body, "types.v1.LabelValuesRequest").await {
            Ok(p) => p,
            Err(refused) => return refused.into_response(),
        };
    finish(codec, label_values_answer(&state, &tenant, msg).await)
}

/// `POST …/querier.v1.QuerierService/Series`.
pub(crate) async fn series(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, msg) =
        match prelude::<SeriesRequest>(&headers, body, "querier.v1.SeriesRequest").await {
            Ok(p) => p,
            Err(refused) => return refused.into_response(),
        };
    let utf8 = allows_utf8_label_names(&headers);
    finish(codec, series_answer(&state, &tenant, msg, utf8).await)
}

// ---------------------------------------------------------------------
// The `GET` routes
// ---------------------------------------------------------------------

/// The query string's pairs, then a form body's.
pub(super) async fn form_pairs(
    headers: &HeaderMap,
    raw: Option<&str>,
    body: Body,
) -> Result<Vec<(String, String)>, ConnectError> {
    let mut pairs = parse_pairs(raw.unwrap_or(""));
    let is_form = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';').next().is_some_and(|m| {
                m.trim()
                    .eq_ignore_ascii_case("application/x-www-form-urlencoded")
            })
        });
    if is_form {
        let bytes = connect::read_body(headers, body).await?;
        pairs.extend(parse_pairs(&String::from_utf8_lossy(&bytes)));
    }
    Ok(pairs)
}

/// A time parameter's first non-empty value through [`attime`], or 0.
pub(super) fn time_param(pairs: &[(String, String)], key: &str, now: i64) -> i64 {
    pairs
        .iter()
        .find(|(k, v)| k == key && !v.is_empty())
        .map(|(_, v)| attime(v, now))
        .unwrap_or(0)
}

/// A `GET` route's `query`: the type up to the first `{`, and the
/// selector from it; `{}` when there is no `{`.
pub(super) fn split_query(query: &str) -> (&str, &str) {
    match query.find('{') {
        Some(i) => (&query[..i], &query[i..]),
        None => (query, "{}"),
    }
}

/// Every non-empty value of `key`.
pub(super) fn all(pairs: &[(String, String)], key: &str) -> Vec<String> {
    pairs
        .iter()
        .filter(|(k, v)| k == key && !v.is_empty())
        .map(|(_, v)| v.clone())
        .collect()
}

/// `GET /api/profiles/v1/types?from=&until=`.
pub(crate) async fn types_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let tenant = match tenant_of(&headers) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let pairs = parse_pairs(raw.as_deref().unwrap_or(""));
    let now = now_ms();
    let msg = ProfileTypesRequest {
        start: time_param(&pairs, "from", now),
        end: time_param(&pairs, "until", now),
    };
    finish(
        Codec::Json,
        profile_types_answer(&state, &tenant, msg).await,
    )
}

/// `GET|POST /api/profiles/v1/labels?query=&from=&until=`.
pub(crate) async fn labels_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
    body: Body,
) -> Response {
    let tenant = match tenant_of(&headers) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let pairs = match form_pairs(&headers, raw.as_deref(), body).await {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let now = now_ms();
    let msg = LabelNamesRequest {
        matchers: all(&pairs, "query"),
        start: time_param(&pairs, "from", now),
        end: time_param(&pairs, "until", now),
    };
    let utf8 = allows_utf8_label_names(&headers);
    finish(
        Codec::Json,
        label_names_answer(&state, &tenant, msg, utf8).await,
    )
}

/// `GET /api/profiles/v1/label/{name}/values?query=&from=&until=`.
pub(crate) async fn label_values_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw): RawQuery,
) -> Response {
    let tenant = match tenant_of(&headers) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let pairs = parse_pairs(raw.as_deref().unwrap_or(""));
    let now = now_ms();
    let msg = LabelValuesRequest {
        name,
        matchers: all(&pairs, "query"),
        start: time_param(&pairs, "from", now),
        end: time_param(&pairs, "until", now),
    };
    finish(Codec::Json, label_values_answer(&state, &tenant, msg).await)
}

/// `GET|POST /api/profiles/v1/series?match[]=&label_names[]=&from=&until=`.
pub(crate) async fn series_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
    body: Body,
) -> Response {
    let tenant = match tenant_of(&headers) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let pairs = match form_pairs(&headers, raw.as_deref(), body).await {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let now = now_ms();
    let msg = SeriesRequest {
        matchers: all(&pairs, "match[]"),
        label_names: all(&pairs, "label_names[]"),
        start: time_param(&pairs, "from", now),
        end: time_param(&pairs, "until", now),
    };
    let utf8 = allows_utf8_label_names(&headers);
    finish(Codec::Json, series_answer(&state, &tenant, msg, utf8).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn accept(values: &[&str]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for v in values {
            h.append("accept", HeaderValue::from_str(v).expect("a header value"));
        }
        h
    }

    /// C4: the capability is an `Accept` parameter equal to `true`, on any
    /// of the `Accept` headers.
    #[test]
    fn c4_the_capability_is_an_accept_parameter() {
        assert!(allows_utf8_label_names(&accept(&[
            "application/json; allow-utf8-labelnames=true"
        ])));
        assert!(allows_utf8_label_names(&accept(&[
            "application/json",
            "*/*; allow-utf8-labelnames=true"
        ])));
        for h in [
            accept(&["application/json"]),
            accept(&["application/json; allow-utf8-labelnames=false"]),
            HeaderMap::new(),
        ] {
            assert!(!allows_utf8_label_names(&h), "{h:?}");
        }
    }

    /// C5: the reference's time parser, except that a compound offset adds
    /// every number–unit pair (`now-1h30m` is 90 minutes; the reference
    /// reads one hour — see `offset_seconds`).
    #[test]
    fn c5_attime_reads_the_reference_forms() {
        const N: i64 = 1_794_750_000_000;
        for (input, want) in [
            ("1794744600", 1_794_744_600_000),
            ("1794744600000", 1_794_744_600_000),
            ("1794744600000000000", 1_794_744_600_000),
            ("20261115", 1_794_700_800_000),
            ("now-1h", N - 3_600_000),
            ("now-1h30m", N - 5_400_000),
            ("now-30m", N - 1_800_000),
            ("now", N),
        ] {
            assert_eq!(attime(input, N), want, "{input}");
        }
    }
}
