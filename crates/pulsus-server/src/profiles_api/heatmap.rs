//! Issue #825: `SelectHeatmap` — the heatmap of profile and span totals —
//! over the Connect protocol.
//!
//! One statement per request (`pulsus_read::profiles::heatmap`), the
//! buckets counted in ClickHouse; the series are ordered and cut to the
//! limit here. In the order: the tenant, the codec and the body, the
//! planner's checks (the step, the limit, the window's order, the window,
//! the type and the selector, then the query type), then the exemplar type.

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

/// The exemplar types served: none yet. Exemplars are #826's.
fn check_exemplar_type(v: i32) -> Result<(), ConnectError> {
    match ExemplarType::try_from(v) {
        Ok(ExemplarType::Unspecified | ExemplarType::None) => Ok(()),
        Ok(ExemplarType::Individual | ExemplarType::Span) => Err(ConnectError::new(
            Code::Unimplemented,
            "exemplars are not served yet",
        )),
        Err(_) => Err(ConnectError::new(
            Code::InvalidArgument,
            format!("unknown exemplar type: {v}"),
        )),
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
    };
    let Some(sql) = heatmap::plan(tenant, &config, &req, now_ms())? else {
        return Ok(SelectHeatmapResponse::default());
    };
    check_exemplar_type(msg.exemplar_type)?;
    let sql = sql.as_str();
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
