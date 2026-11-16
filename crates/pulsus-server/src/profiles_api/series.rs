//! Issue #824: `SelectSeries` — the per-step time series of profile totals —
//! over the Connect protocol, and `GET /api/profiles/v1/select_series`.
//!
//! One statement per request (`pulsus_read::profiles::series`), bucketed
//! and aggregated in ClickHouse; the series are ordered and cut to the
//! limit here. In the order: the tenant, the codec and the body, the
//! planner's checks (the window, the type, the selector, then the step),
//! then the exemplar type.

use axum::body::Body;
use axum::extract::{RawQuery, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use pulsus_model::Tenant;
use pulsus_profiles_wire::querier_v1::{SelectSeriesRequest, SelectSeriesResponse};
use pulsus_profiles_wire::types_v1::{
    ExemplarType, LabelPair, Point, Series, TimeSeriesAggregationType,
};
use pulsus_read::profiles::ProfilesReadConfig;
use pulsus_read::profiles::series::{self, SeriesRequest};

use crate::app::AppState;
use crate::logs_api::params::parse_pairs;

use super::connect::{Code, Codec, ConnectError};
use super::engine::with_engine;
use super::routes::{all, finish, now_ms, prelude, split_query, tenant_of, time_param};

/// The exemplar types served: none yet. Individual exemplars are #826's.
fn check_exemplar_type(v: i32) -> Result<(), ConnectError> {
    match ExemplarType::try_from(v) {
        Ok(ExemplarType::Unspecified | ExemplarType::None) => Ok(()),
        Ok(ExemplarType::Individual) => Err(ConnectError::new(
            Code::Unimplemented,
            "exemplars are not served yet",
        )),
        Ok(ExemplarType::Span) => Err(ConnectError::new(
            Code::Unimplemented,
            "exemplar type span is not implemented",
        )),
        Err(_) => Err(ConnectError::new(
            Code::InvalidArgument,
            format!("unknown exemplar type: {v}"),
        )),
    }
}

/// The time series `msg` asks for, in one statement, or none when its
/// window ends before the lookback floor.
async fn time_series(
    state: &AppState,
    tenant: &Tenant,
    msg: SelectSeriesRequest,
) -> Result<SelectSeriesResponse, ConnectError> {
    let config = ProfilesReadConfig::from_config(&state.config);
    let call_site: Vec<String> = msg
        .stack_trace_selector
        .map(|s| s.call_site.into_iter().map(|l| l.name).collect())
        .unwrap_or_default();
    let req = SeriesRequest {
        profile_type: &msg.profile_type_id,
        label_selector: &msg.label_selector,
        start_ms: msg.start,
        end_ms: msg.end,
        step: msg.step,
        group_by: &msg.group_by,
        average: msg.aggregation == Some(TimeSeriesAggregationType::Average as i32),
        call_site: &call_site,
        limit: msg.limit,
    };
    let Some(sql) = series::plan(tenant, &config, &req, now_ms())? else {
        return Ok(SelectSeriesResponse::default());
    };
    check_exemplar_type(msg.exemplar_type)?;
    let sql = sql.as_str();
    let rows = with_engine(state, tenant, |engine| async move {
        Ok(engine.read_series(sql).await?)
    })
    .await?;
    Ok(wire(series::series(rows, &msg.group_by, msg.limit)))
}

fn wire(series: Vec<series::Series>) -> SelectSeriesResponse {
    SelectSeriesResponse {
        series: series
            .into_iter()
            .map(|s| Series {
                labels: s
                    .labels
                    .into_iter()
                    .map(|(name, value)| LabelPair { name, value })
                    .collect(),
                points: s
                    .points
                    .into_iter()
                    .map(|p| Point {
                        value: p.value,
                        timestamp: p.timestamp,
                        annotations: Vec::new(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// `POST …/querier.v1.QuerierService/SelectSeries`.
pub(crate) async fn select_series(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, msg) = match prelude::<SelectSeriesRequest>(
        &headers,
        body,
        "querier.v1.SelectSeriesRequest",
    )
    .await
    {
        Ok(p) => p,
        Err(refused) => return refused.into_response(),
    };
    finish(codec, time_series(&state, &tenant, msg).await)
}

/// `GET /api/profiles/v1/select_series?query=&from=&until=&step=&groupBy[]=&aggregation=&limit=`:
/// the `SelectSeriesResponse` message's JSON.
pub(crate) async fn select_series_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let tenant = match tenant_of(&headers) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let pairs = parse_pairs(raw.as_deref().unwrap_or(""));
    let msg = match series_request(&pairs, now_ms()) {
        Ok(m) => m,
        Err(e) => return e.into_response(),
    };
    finish(Codec::Json, time_series(&state, &tenant, msg).await)
}

/// The `GET` route's parameters as the Connect request. `step` is seconds,
/// 0 when absent; `aggregation` `avg` is `AVERAGE` and anything else
/// `SUM`.
fn series_request(
    pairs: &[(String, String)],
    now: i64,
) -> Result<SelectSeriesRequest, ConnectError> {
    let first = |key: &str| {
        pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    let (profile_type_id, label_selector) = split_query(first("query").unwrap_or(""));
    let step = match first("step") {
        None => 0.0,
        Some(v) => v
            .parse::<f64>()
            .map_err(|_| ConnectError::new(Code::InvalidArgument, format!("invalid step {v:?}")))?,
    };
    let limit = match first("limit") {
        None => None,
        Some(v) => Some(v.parse::<i64>().map_err(|_| {
            ConnectError::new(Code::InvalidArgument, format!("invalid limit {v:?}"))
        })?),
    };
    let aggregation = if first("aggregation") == Some("avg") {
        TimeSeriesAggregationType::Average
    } else {
        TimeSeriesAggregationType::Sum
    };
    Ok(SelectSeriesRequest {
        profile_type_id: profile_type_id.to_string(),
        label_selector: label_selector.to_string(),
        start: time_param(pairs, "from", now),
        end: time_param(pairs, "until", now),
        group_by: all(pairs, "groupBy[]"),
        step,
        aggregation: Some(aggregation as i32),
        limit,
        ..SelectSeriesRequest::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(k, val)| (k.to_string(), val.to_string()))
            .collect()
    }

    /// The `GET` route splits `query` at its first `{`, reads `step` as
    /// seconds, `groupBy[]` in order, `aggregation` `avg` and `limit`, and
    /// refuses a `step` or a `limit` that is not a number.
    #[test]
    fn the_get_route_reads_its_parameters() {
        let m = series_request(
            &pairs(&[
                ("query", r#"a:b:c:d:e{service_name="x"}"#),
                ("from", "1000"),
                ("until", "2000"),
                ("step", "15"),
                ("groupBy[]", "pod"),
                ("groupBy[]", "service_name"),
                ("aggregation", "avg"),
                ("limit", "1"),
            ]),
            0,
        )
        .expect("a request");
        assert_eq!(m.profile_type_id, "a:b:c:d:e");
        assert_eq!(m.label_selector, r#"{service_name="x"}"#);
        assert_eq!((m.start, m.end), (1_000_000, 2_000_000));
        assert_eq!(m.step, 15.0);
        assert_eq!(m.group_by, ["pod", "service_name"]);
        assert_eq!(
            m.aggregation,
            Some(TimeSeriesAggregationType::Average as i32)
        );
        assert_eq!(m.limit, Some(1));
        let m = series_request(&pairs(&[("query", "a:b:c:d:e"), ("aggregation", "max")]), 0)
            .expect("a request");
        assert_eq!(m.label_selector, "{}");
        assert_eq!(m.aggregation, Some(TimeSeriesAggregationType::Sum as i32));
        assert_eq!((m.step, m.limit), (0.0, None));
        for (key, text) in [
            ("step", r#"invalid step "x""#),
            ("limit", r#"invalid limit "x""#),
        ] {
            let e = series_request(&pairs(&[(key, "x")]), 0).expect_err("refused");
            assert_eq!((e.code, e.message.as_str()), (Code::InvalidArgument, text));
        }
    }

    /// D9: unspecified and `NONE` are served; `INDIVIDUAL` and `SPAN` are
    /// not; another number is refused.
    #[test]
    fn the_exemplar_type_is_checked() {
        for v in [0, 1] {
            assert!(check_exemplar_type(v).is_ok(), "{v}");
        }
        for (v, code, text) in [
            (2, Code::Unimplemented, "exemplars are not served yet"),
            (
                3,
                Code::Unimplemented,
                "exemplar type span is not implemented",
            ),
            (7, Code::InvalidArgument, "unknown exemplar type: 7"),
        ] {
            let e = check_exemplar_type(v).expect_err("refused");
            assert_eq!((e.code, e.message.as_str()), (code, text), "{v}");
        }
    }
}
