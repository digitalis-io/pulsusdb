//! Issue #828: the merged profile as pprof — `SelectMergeProfile` over the
//! Connect protocol, and `GET /api/profiles/v1/export`, the gzipped pprof
//! file. `SelectMergeStacktraces` with `PROFILE_FORMAT_PPROF` answers the
//! same profile (`merge.rs`).
//!
//! One statement per request (`pulsus_read::profiles::pprof`), every merge
//! done in ClickHouse; the cut to `max_nodes` and the encoding run here. In
//! the order: the tenant, the codec and the body, a span selector with a
//! trace selector, then the planner's own checks.

use std::io::Write;

use axum::body::Body;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use prost::Message;
use pulsus_model::Tenant;
use pulsus_profiles_wire::google_v1::Profile;
use pulsus_profiles_wire::querier_v1::SelectMergeProfileRequest;
use pulsus_profiles_wire::types_v1::StackTraceSelector;
use pulsus_read::profiles::ProfilesReadConfig;
use pulsus_read::profiles::pprof::{self, PprofRequest};

use crate::app::AppState;
use crate::logs_api::params::parse_pairs;

use super::connect::{Code, ConnectError};
use super::engine::with_engine;
use super::routes::{all, finish, now_ms, prelude, split_query, tenant_of, time_param};

/// A selector's call site, root first, and its `go_pgo` as
/// `(keep_locations, aggregate_callees)`.
pub(super) fn stack_trace_selector(
    selector: Option<StackTraceSelector>,
) -> (Vec<String>, Option<(u32, bool)>) {
    selector.map_or((Vec::new(), None), |s| {
        (
            s.call_site.into_iter().map(|l| l.name).collect(),
            s.go_pgo.map(|g| (g.keep_locations, g.aggregate_callees)),
        )
    })
}

/// The merged profile `req` selects, in one statement, or the empty
/// message when its window ends before the lookback floor (D9).
pub(super) async fn merged_profile(
    state: &AppState,
    tenant: &Tenant,
    req: PprofRequest<'_>,
) -> Result<Profile, ConnectError> {
    let config = ProfilesReadConfig::from_config(&state.config);
    let Some(planned) = pprof::plan(tenant, &config, &req, now_ms())? else {
        return Ok(Profile::default());
    };
    let sql = planned.sql.as_str();
    let rows = with_engine(state, tenant, |engine| async move {
        Ok(engine.read_pprof(sql).await?)
    })
    .await?;
    Ok(pprof::build(&planned, rows))
}

/// `msg` as the planner's request, and its merged profile.
async fn answer(
    state: &AppState,
    tenant: &Tenant,
    msg: SelectMergeProfileRequest,
) -> Result<Profile, ConnectError> {
    let (call_site, go_pgo) = stack_trace_selector(msg.stack_trace_selector);
    let req = PprofRequest {
        profile_type: &msg.profile_type_id,
        label_selector: &msg.label_selector,
        start_ms: msg.start,
        end_ms: msg.end,
        max_nodes: msg.max_nodes,
        call_site: &call_site,
        span_selector: &[],
        trace_id_selector: &msg.trace_id_selector,
        profile_id_selector: &msg.profile_id_selector,
        go_pgo,
    };
    merged_profile(state, tenant, req).await
}

/// `POST …/querier.v1.QuerierService/SelectMergeProfile`.
pub(crate) async fn select_merge_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, msg) = match prelude::<SelectMergeProfileRequest>(
        &headers,
        body,
        "querier.v1.SelectMergeProfileRequest",
    )
    .await
    {
        Ok(p) => p,
        Err(refused) => return refused.into_response(),
    };
    finish(codec, answer(&state, &tenant, msg).await)
}

/// `GET /api/profiles/v1/export?query=&from=&until=&traceIdSelector[]=&maxNodes=`:
/// the gzip of the merged profile's bytes, the file pprof readers take as
/// it is. `query` is split as `GET /merge` splits it.
pub(crate) async fn export_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let tenant = match tenant_of(&headers) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let pairs = parse_pairs(raw.as_deref().unwrap_or(""));
    let msg = match export_request(&pairs, now_ms()) {
        Ok(m) => m,
        Err(e) => return e.into_response(),
    };
    match answer(&state, &tenant, msg).await {
        Ok(profile) => match gzip(&profile.encode_to_vec()) {
            Ok(body) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/octet-stream")],
                body,
            )
                .into_response(),
            Err(e) => ConnectError::new(Code::Internal, e.to_string()).into_response(),
        },
        Err(e) => e.into_response(),
    }
}

/// The `GET` route's parameters as the Connect request.
fn export_request(
    pairs: &[(String, String)],
    now: i64,
) -> Result<SelectMergeProfileRequest, ConnectError> {
    let first = |key: &str| {
        pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    let (profile_type_id, label_selector) = split_query(first("query").unwrap_or(""));
    let max_nodes = match first("maxNodes") {
        None => None,
        Some(v) => Some(v.parse::<i64>().map_err(|_| {
            ConnectError::new(Code::InvalidArgument, format!("invalid maxNodes {v:?}"))
        })?),
    };
    Ok(SelectMergeProfileRequest {
        profile_type_id: profile_type_id.to_string(),
        label_selector: label_selector.to_string(),
        start: time_param(pairs, "from", now),
        end: time_param(pairs, "until", now),
        max_nodes,
        trace_id_selector: all(pairs, "traceIdSelector[]"),
        ..SelectMergeProfileRequest::default()
    })
}

fn gzip(bytes: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(bytes)?;
    gz.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(k, val)| (k.to_string(), val.to_string()))
            .collect()
    }

    /// `GET /export` splits `query` at its first `{`, reads `maxNodes` as
    /// an integer and the trace selectors, and refuses anything else.
    #[test]
    fn the_export_route_reads_its_parameters() {
        let m = export_request(
            &pairs(&[
                ("query", r#"a:b:c:d:e{service_name="x"}"#),
                ("from", "1000"),
                ("until", "2000"),
                ("maxNodes", "16384"),
                ("traceIdSelector[]", "0102030405060708090a0b0c0d0e0f10"),
            ]),
            0,
        )
        .expect("a request");
        assert_eq!(m.profile_type_id, "a:b:c:d:e");
        assert_eq!(m.label_selector, r#"{service_name="x"}"#);
        assert_eq!((m.start, m.end), (1_000_000, 2_000_000));
        assert_eq!(m.max_nodes, Some(16_384));
        assert_eq!(m.trace_id_selector, ["0102030405060708090a0b0c0d0e0f10"]);
        let e = export_request(&pairs(&[("maxNodes", "x")]), 0).expect_err("refused");
        assert_eq!(
            (e.code, e.message.as_str()),
            (Code::InvalidArgument, r#"invalid maxNodes "x""#)
        );
    }
}
