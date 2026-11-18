//! Issue #825: `SelectHeatmap` — the heatmap of profile and span totals —
//! over the Connect protocol.
//!
//! One statement per request (`pulsus_read::profiles::heatmap`), the
//! buckets counted in ClickHouse; the series are ordered and cut to the
//! limit here. In the order: the tenant, the codec and the body, the
//! planner's checks (the step, the limit, the window's order, the window,
//! the type and the selector, then the query type), then the exemplar type,
//! on every window (issue #826).
//!
//! Issue #826: an exemplar type matching the query type names each cell's
//! largest value, in the same one statement.

use axum::body::Body;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use pulsus_model::Tenant;
use pulsus_profiles_wire::querier_v1::{SelectHeatmapRequest, SelectHeatmapResponse};
use pulsus_profiles_wire::types_v1::{ExemplarType, HeatmapSeries, HeatmapSlot, LabelPair};
use pulsus_read::profiles::ProfilesReadConfig;
use pulsus_read::profiles::heatmap::{self, HeatmapRequest};

use crate::app::AppState;

use super::connect::{Code, ConnectError};
use super::engine::with_engine;
use super::routes::{finish, now_ms, prelude};
use super::series::exemplar;

/// The exemplar types served: unspecified and `NONE` with either query
/// type, `INDIVIDUAL` and `SPAN` with their own; the rest are
/// `invalid_argument`.
fn check_exemplar_type(v: i32, query_type: i32) -> Result<(), ConnectError> {
    let refuse = |m: String| Err(ConnectError::new(Code::InvalidArgument, m));
    match ExemplarType::try_from(v) {
        Ok(ExemplarType::Unspecified | ExemplarType::None) => Ok(()),
        Ok(ExemplarType::Individual) if query_type != heatmap::QUERY_TYPE_INDIVIDUAL => {
            refuse("individual exemplars only available for individual query type".to_string())
        }
        Ok(ExemplarType::Span) if query_type != heatmap::QUERY_TYPE_SPAN => {
            refuse("span exemplars only available for span query type".to_string())
        }
        Ok(ExemplarType::Individual | ExemplarType::Span) => Ok(()),
        Err(_) => refuse(format!("unknown exemplar type: {v}")),
    }
}

/// The heatmap `msg` asks for, in one statement, or none when its window
/// ends before the lookback floor.
async fn select(
    state: &AppState,
    tenant: &Tenant,
    msg: SelectHeatmapRequest,
) -> Result<SelectHeatmapResponse, ConnectError> {
    let config = ProfilesReadConfig::from_config(&state.config);
    let req = HeatmapRequest {
        profile_type: &msg.profile_type_id,
        label_selector: &msg.label_selector,
        start_ms: msg.start,
        end_ms: msg.end,
        step: msg.step,
        group_by: &msg.group_by,
        query_type: msg.query_type,
        limit: msg.limit,
        exemplars: msg.exemplar_type == ExemplarType::Individual as i32
            || msg.exemplar_type == ExemplarType::Span as i32,
    };
    let planned = heatmap::plan(tenant, &config, &req, now_ms())?;
    check_exemplar_type(msg.exemplar_type, msg.query_type)?;
    let Some(sql) = planned else {
        return Ok(SelectHeatmapResponse::default());
    };
    let sql = sql.as_str();
    if req.exemplars {
        let rows = with_engine(state, tenant, |engine| async move {
            Ok(engine.read_heatmap_exemplars(sql).await?)
        })
        .await?;
        return Ok(wire(heatmap::heatmap_exemplars(
            rows,
            &msg.group_by,
            msg.limit,
        )));
    }
    let rows = with_engine(state, tenant, |engine| async move {
        Ok(engine.read_heatmap(sql).await?)
    })
    .await?;
    Ok(wire(heatmap::heatmap(rows, &msg.group_by, msg.limit)))
}

fn wire(series: Vec<heatmap::Series>) -> SelectHeatmapResponse {
    SelectHeatmapResponse {
        series: series
            .into_iter()
            .map(|s| HeatmapSeries {
                labels: s
                    .labels
                    .into_iter()
                    .map(|(name, value)| LabelPair { name, value })
                    .collect(),
                slots: s
                    .slots
                    .into_iter()
                    .map(|slot| HeatmapSlot {
                        timestamp: slot.timestamp,
                        y_min: slot.y_min,
                        counts: slot.counts,
                        exemplars: slot.exemplars.into_iter().map(exemplar).collect(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// `POST …/querier.v1.QuerierService/SelectHeatmap`.
pub(crate) async fn select_heatmap(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant, codec, msg) =
        match prelude::<SelectHeatmapRequest>(&headers, body, "querier.v1.SelectHeatmapRequest")
            .await
        {
            Ok(p) => p,
            Err(refused) => return refused.into_response(),
        };
    finish(codec, select(&state, &tenant, msg).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_read::profiles::heatmap::{QUERY_TYPE_INDIVIDUAL, QUERY_TYPE_SPAN};

    /// E8 (issue #826): an exemplar type is served with its own query
    /// type, unspecified and `NONE` with either; a mismatch and an unknown
    /// number are `invalid_argument`.
    #[test]
    fn the_exemplar_type_matches_the_query_type() {
        let both = [QUERY_TYPE_INDIVIDUAL, QUERY_TYPE_SPAN];
        for (v, q) in [(2, QUERY_TYPE_INDIVIDUAL), (3, QUERY_TYPE_SPAN)]
            .into_iter()
            .chain(both.into_iter().flat_map(|q| [(0, q), (1, q)]))
        {
            assert!(check_exemplar_type(v, q).is_ok(), "({v}, {q})");
        }
        let mut refused = vec![
            (
                2,
                QUERY_TYPE_SPAN,
                "individual exemplars only available for individual query type",
            ),
            (
                3,
                QUERY_TYPE_INDIVIDUAL,
                "span exemplars only available for span query type",
            ),
        ];
        for q in both {
            refused.push((9, q, "unknown exemplar type: 9"));
        }
        for (v, q, text) in refused {
            let e = check_exemplar_type(v, q).expect_err("refused");
            assert_eq!(
                (e.code, e.message.as_str()),
                (Code::InvalidArgument, text),
                "({v}, {q})"
            );
        }
    }
}
