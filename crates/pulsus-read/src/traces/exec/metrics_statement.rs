//! The metrics statement's engine side (issue #596 part 1): issue the one
//! statement a [`RoutedMetricsPlan`] carries and frame its rows, or hand a
//! plan without one to today's engine.

use std::collections::BTreeMap;

use futures::StreamExt;
use pulsus_clickhouse::ChError;

use super::{TraceEngine, hex16, map_trace_metrics_error, metrics_settings, with_final};
use crate::logql::error::{ReadError, TooBroadReason};
use crate::traces::log2_histogram;
use crate::traces::metrics_plan::{PlanKind, RangeAxis, TraceMetricsPlan};
use crate::traces::metrics_result::{
    MetricExemplar, MetricLabel, TraceMetricSeries, TraceMetricsResult,
};
use crate::traces::spans::metrics::{MetricStatementRow, RoutedMetricsPlan, SERIES_CAP_MESSAGE};

impl TraceEngine {
    /// The range answer to `routed`: its statement's rows, each series'
    /// empty buckets filled with `0` where its flag says so; today's
    /// [`TraceEngine::metrics_range`] when it carries no statement.
    pub async fn metrics_range_routed(
        &self,
        routed: &RoutedMetricsPlan,
    ) -> Result<TraceMetricsResult, ReadError> {
        let Some(statement) = routed.statement() else {
            return self.metrics_range(routed.plan()).await;
        };
        let rows = self.metric_statement_rows(statement.range_sql()).await?;
        frame(routed.plan(), rows, Some(routed.plan().range_axis()))
    }

    /// The instant answer to `routed`: its statement's rows as returned;
    /// today's [`TraceEngine::metrics_instant`] when it carries no
    /// statement.
    pub async fn metrics_instant_routed(
        &self,
        routed: &RoutedMetricsPlan,
    ) -> Result<TraceMetricsResult, ReadError> {
        let Some(statement) = routed.statement() else {
            return self.metrics_instant(routed.plan()).await;
        };
        let rows = self.metric_statement_rows(statement.instant_sql()).await?;
        frame(routed.plan(), rows, None)
    }

    /// Issues `sql` under the metrics settings with `final = 1`.
    async fn metric_statement_rows(&self, sql: &str) -> Result<Vec<MetricStatementRow>, ReadError> {
        let settings = with_final(metrics_settings(&self.config));
        let mut stream = self
            .dispatch
            .query_stream::<MetricStatementRow, _>(sql, &settings, |e| self.map_statement_error(e))
            .await?;
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await {
            rows.push(row.map_err(|e| self.map_statement_error(e))?);
        }
        Ok(rows)
    }

    /// The series cap's `throwIf` is the series cap's `422`, its count
    /// the cap plus one as today's probe reported it; any other error is
    /// the metrics path's.
    fn map_statement_error(&self, e: ChError) -> ReadError {
        match e {
            ChError::Server {
                code: 395,
                ref message,
            } if message.contains(SERIES_CAP_MESSAGE) => {
                let cap = self.config.max_series;
                ReadError::QueryTooBroad(TooBroadReason::TraceMetricsSeriesCap {
                    count: cap.saturating_add(1),
                    cap,
                })
            }
            other => map_trace_metrics_error(other, &self.config),
        }
    }
}

/// Each row one series: its label from the plan, its samples — on the
/// range route with `f = 1`, a `0` at every axis label the row does not
/// carry and a `nan` dropped — and its exemplars. A series left with no
/// sample is dropped.
fn frame(
    plan: &TraceMetricsPlan,
    rows: Vec<MetricStatementRow>,
    axis: Option<RangeAxis>,
) -> Result<TraceMetricsResult, ReadError> {
    let mut series = Vec::with_capacity(rows.len());
    for row in rows {
        let label = label_of(plan, &row.g)?;
        let samples = match axis {
            Some(axis) if row.f == 1 => filled(&row.points, axis),
            _ => row.points,
        };
        if samples.is_empty() {
            continue;
        }
        let exemplars = row
            .exemplars
            .into_iter()
            .map(|(trace_id, start_ns, value)| MetricExemplar {
                labels: vec![MetricLabel::str("trace:id", hex16(&trace_id))],
                value,
                timestamp_ms: start_ns / 1_000_000,
            })
            .collect();
        series.push(TraceMetricSeries {
            labels: vec![label],
            samples,
            exemplars,
        });
    }
    Ok(TraceMetricsResult { series })
}

/// One sample per axis label: the row's value where it carries one and it
/// is not `nan`, `0` where it carries none.
fn filled(points: &[(i64, f64)], axis: RangeAxis) -> Vec<(i64, f64)> {
    let carried: BTreeMap<i64, f64> = points.iter().copied().collect();
    (0..axis.points)
        .map(|i| axis.label_ms(i))
        .filter_map(|t| match carried.get(&t) {
            Some(v) if v.is_nan() => None,
            Some(v) => Some((t, *v)),
            None => Some((t, 0.0)),
        })
        .collect()
}

/// A row's series label: `__name__` for an ungrouped count or aggregate,
/// the group key's value, the quantile `p` the row's index names, or the
/// histogram bucket in seconds.
fn label_of(plan: &TraceMetricsPlan, g: &str) -> Result<MetricLabel, ReadError> {
    let decode = |m: String| ReadError::Clickhouse(ChError::Decode(m));
    Ok(match plan.kind() {
        PlanKind::Quantile => {
            let q = g
                .parse::<usize>()
                .ok()
                .and_then(|i| i.checked_sub(1))
                .and_then(|i| plan.quantiles().get(i))
                .ok_or_else(|| decode(format!("a quantile index {g:?}")))?;
            MetricLabel::double("p", *q)
        }
        PlanKind::Histogram => {
            let bucket = g
                .parse::<u64>()
                .map_err(|e| decode(format!("a histogram bucket {g:?}: {e}")))?;
            MetricLabel::double("__bucket", log2_histogram::bucket_seconds(bucket))
        }
        PlanKind::Count { .. } | PlanKind::Agg(_) => match plan.group_label() {
            Some(key) => MetricLabel::str(key, g),
            None => MetricLabel::str("__name__", plan.metric_name()),
        },
        PlanKind::Compare => return Err(decode("compare() has no statement".to_string())),
    })
}
