//! The metrics statement's engine side (issue #596 part 1): issue the one
//! statement a [`RoutedMetricsPlan`] carries and frame its rows, or hand a
//! plan without one to today's engine.

use std::collections::BTreeMap;

use futures::StreamExt;
use pulsus_clickhouse::{ChError, ChRow};

use super::{TraceEngine, frame_compare_rows, hex16, map_trace_metrics_error, statement_settings};
use crate::logql::error::{ReadError, TooBroadReason};
use crate::traces::log2_histogram;
use crate::traces::metrics_plan::{PlanKind, RangeAxis, TraceMetricsPlan};
use crate::traces::metrics_result::{
    MetricExemplar, MetricLabel, MetricLabelValue, TraceMetricSeries, TraceMetricsResult,
};
use crate::traces::spans::metrics::{
    CompareStatementRow, MetricStatementRow, MetricsStatement, RoutedMetricsPlan,
    SERIES_CAP_MESSAGE, TargetUnit,
};
use crate::traces::values::go_duration_string;

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
        // Issue #597: a comparison's rows are its own.
        if statement.is_compare() {
            let rows = self
                .metric_statement_rows::<CompareStatementRow>(
                    statement.range_sql(),
                    statement.demands(),
                )
                .await?;
            return frame_compare_rows(routed.plan(), rows, Some(routed.plan().range_axis()));
        }
        let rows = self
            .metric_statement_rows::<MetricStatementRow>(statement.range_sql(), statement.demands())
            .await?;
        frame(
            routed.plan(),
            statement,
            rows,
            Some(routed.plan().range_axis()),
        )
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
        if statement.is_compare() {
            let rows = self
                .metric_statement_rows::<CompareStatementRow>(
                    statement.instant_sql(),
                    statement.demands(),
                )
                .await?;
            return frame_compare_rows(routed.plan(), rows, None);
        }
        let rows = self
            .metric_statement_rows::<MetricStatementRow>(
                statement.instant_sql(),
                statement.demands(),
            )
            .await?;
        frame(routed.plan(), statement, rows, None)
    }

    /// Issues `sql` under [`statement_settings`]: a metrics statement's
    /// rows, or a comparison's (issue #597), under the same error mapping.
    async fn metric_statement_rows<R: ChRow>(
        &self,
        sql: &str,
        demands: &[String],
    ) -> Result<Vec<R>, ReadError> {
        let settings = statement_settings(&self.config);
        let mut stream = self
            .dispatch
            .query_stream::<R, _>(sql, &settings, |e| self.map_statement_error(e, demands))
            .await?;
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await {
            rows.push(row.map_err(|e| self.map_statement_error(e, demands))?);
        }
        Ok(rows)
    }

    /// The series cap's `throwIf` is the series cap's `422`, its count
    /// the cap plus one as today's probe reported it; any other error is
    /// the metrics path's.
    fn map_statement_error(&self, e: ChError, demands: &[String]) -> ReadError {
        // Issue #602 part 2a: a `!` demand the filter raised is the
        // request's `400`, carrying the demand's text.
        if let ChError::Server { code: 395, message } = &e
            && let Some(d) = demands.iter().find(|d| message.contains(d.as_str()))
        {
            return ReadError::PipelineInvalid { reason: d.clone() };
        }
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

/// Each row one series: its labels from the row and the statement, its
/// samples — on the range route with `f = 1`, a `0` at every axis label
/// the row does not carry and a `nan` dropped — and its exemplars. A
/// series left with no sample is dropped.
fn frame(
    plan: &TraceMetricsPlan,
    statement: &MetricsStatement,
    rows: Vec<MetricStatementRow>,
    axis: Option<RangeAxis>,
) -> Result<TraceMetricsResult, ReadError> {
    let mut series = Vec::with_capacity(rows.len());
    for row in rows {
        let labels = label_of(plan, statement, &row)?;
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
            labels,
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

/// A row's labels (part 2's D3): one per key value, typed by its stored
/// type — a missing value of an ordinary key dropped, of a well-known key
/// or an array `"nil"` — and the first key `"nil"` when keys were asked and
/// no label is left; then the quantile `p` the row's `o` names, or the
/// histogram bucket `o` in the target's unit; `__name__` for an ungrouped
/// count or aggregate.
fn label_of(
    plan: &TraceMetricsPlan,
    statement: &MetricsStatement,
    row: &MetricStatementRow,
) -> Result<Vec<MetricLabel>, ReadError> {
    let decode = |m: String| ReadError::Clickhouse(ChError::Decode(m));
    let names = statement.labels();
    if row.g.len() != names.len() {
        return Err(decode(format!(
            "a series of {} key values for {} keys",
            row.g.len(),
            names.len()
        )));
    }
    let mut labels = Vec::with_capacity(names.len() + 1);
    for ((text, ty), name) in row.g.iter().zip(names) {
        let value = match ty.as_str() {
            "s" | "i" | "d" | "b" => typed_value(text, ty)?,
            "u" => MetricLabelValue::Str(go_duration_string(
                text.parse()
                    .map_err(|e| decode(format!("a duration key value {text:?}: {e}")))?,
            )),
            "N" | "a" => MetricLabelValue::Str("nil".to_string()),
            "n" => continue,
            other => return Err(decode(format!("a key value of type {other:?}"))),
        };
        labels.push(MetricLabel {
            key: name.clone(),
            value,
        });
    }
    let own = match plan.kind() {
        PlanKind::Quantile => {
            let q = usize::try_from(row.o)
                .ok()
                .and_then(|i| i.checked_sub(1))
                .and_then(|i| plan.quantiles().get(i))
                .ok_or_else(|| decode(format!("a quantile index {}", row.o)))?;
            Some(MetricLabel::double("p", *q))
        }
        PlanKind::Histogram => Some(MetricLabel::double(
            "__bucket",
            match statement.unit() {
                TargetUnit::Seconds => log2_histogram::bucket_seconds(row.o),
                TargetUnit::Raw => row.o as f64,
            },
        )),
        PlanKind::Count { .. } | PlanKind::Agg(_) => None,
        PlanKind::Compare => return Err(decode("compare() has no statement".to_string())),
    };
    if labels.is_empty() && own.is_none() {
        labels.push(match names.first() {
            Some(first) => MetricLabel::str(first.clone(), "nil"),
            None => MetricLabel::str("__name__", plan.metric_name()),
        });
    }
    labels.extend(own);
    Ok(labels)
}

/// A key value `(text, type)` as its label: `s` a string, `i` an integer,
/// `d` a double, `b` a boolean, the conversion a `by()` key and a
/// `compare()` value share (issue #597's D3); any other type is an error.
pub(super) fn typed_value(text: &str, ty: &str) -> Result<MetricLabelValue, ReadError> {
    let decode = |m: String| ReadError::Clickhouse(ChError::Decode(m));
    Ok(match ty {
        "s" => MetricLabelValue::Str(text.to_string()),
        "i" => MetricLabelValue::Int(
            text.parse()
                .map_err(|e| decode(format!("an integer key value {text:?}: {e}")))?,
        ),
        "d" => MetricLabelValue::Double(
            text.parse()
                .map_err(|e| decode(format!("a double key value {text:?}: {e}")))?,
        ),
        "b" => MetricLabelValue::Bool(text == "true"),
        other => return Err(decode(format!("a key value of type {other:?}"))),
    })
}

#[cfg(test)]
mod tests {
    use pulsus_traceql::parse;

    use super::*;
    use crate::traces::filter::SpanFilterCtx;
    use crate::traces::metrics_plan::{MetricsCtx, MetricsParams};
    use crate::traces::metrics_result::MetricLabelValue;
    use crate::traces::spans::metrics::{StatementTables, plan_trace_metrics_routed};

    /// D13: a histogram's `__bucket` takes its unit from the target the
    /// query asked, not from the shell's `duration`.
    #[test]
    fn a_bucket_label_takes_its_targets_unit() {
        let ctx = MetricsCtx {
            filter: SpanFilterCtx {
                spans_table: "trace_spans",
                attrs_table: "trace_attrs_idx",
            },
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            distributed: false,
            skip_unavailable_shards: false,
        };
        let params = MetricsParams {
            start_ns: 1_700_000_000_000_000_000,
            end_ns: 1_700_000_060_000_000_000,
            step_ms: 60_000,
            exemplars: None,
        };
        let tables = StatementTables {
            spans: "spans",
            resources: "resources",
            traces: "traces",
            indexed: &[],
        };
        let bucket = |q: &str, o: u64| {
            let routed =
                plan_trace_metrics_routed(&parse(q).expect("parse"), &params, &ctx, &tables);
            assert!(routed.is_ok(), "{q}: routed refused: {routed:?}");
            let routed = routed.expect("checked");
            let Some(statement) = routed.statement() else {
                panic!("{q}: no statement");
            };
            let row = MetricStatementRow {
                g: Vec::new(),
                o,
                points: vec![(1_700_000_060_000, 1.0)],
                exemplars: Vec::new(),
                f: 0,
            };
            let out = frame(routed.plan(), statement, vec![row], None).expect("framed");
            match out.series[0].labels[..] {
                [
                    MetricLabel {
                        ref key,
                        value: MetricLabelValue::Double(v),
                    },
                ] if key == "__bucket" => v,
                ref other => panic!("{q}: labels {other:?}"),
            }
        };
        assert_eq!(bucket("{} | histogram_over_time(span.n)", 8), 8.0);
        assert_eq!(bucket("{} | histogram_over_time(duration)", 8), 8e-9);
        assert_eq!(
            bucket("{} | histogram_over_time(duration)", 1_073_741_824),
            1.073741824
        );
    }

    /// U5 (issue #597): a compare value is labelled in the arm of its
    /// stored type; an unknown type is an error.
    #[test]
    fn a_compare_value_is_labelled_by_its_stored_type() {
        assert_eq!(typed_value("1", "i").ok(), Some(MetricLabelValue::Int(1)));
        assert_eq!(
            typed_value("1", "d").ok(),
            Some(MetricLabelValue::Double(1.0))
        );
        assert_eq!(
            typed_value("true", "b").ok(),
            Some(MetricLabelValue::Bool(true))
        );
        assert_eq!(
            typed_value("x", "s").ok(),
            Some(MetricLabelValue::Str("x".to_string()))
        );
        assert!(typed_value("1", "q").is_err(), "an unknown type");
    }
}
