//! `evaluate(plan, data) -> Result<QueryValue, PromqlError>` — the pure,
//! series-first evaluator (architect plan: "series-first evaluation" — all
//! steps for one series' selection are naturally colocated since each
//! step re-reads the same pre-fetched, pre-sorted `Vec<Sample>`; group
//! keys are computed once per step in [`aggregation`]/[`binop`], not
//! re-derived per series). Instant query = a single-step range (`params
//! .step_ms == 0`) yielding [`QueryValue::Vector`]/[`QueryValue::Scalar`];
//! a range query yields [`QueryValue::Matrix`].
//!
//! Owns the window-boundary math every downstream module (staleness,
//! range functions) is handed already-filtered data for: **left-open
//! right-closed** `(t − width, t]` at every step, for both the 5-minute
//! staleness lookback ([`staleness`]) and range-vector selection
//! ([`windowed_non_stale`], which additionally strips any stale-NaN-marked
//! sample before it ever reaches [`functions::eval_range_fn`]/
//! [`functions::eval_over_time`] — those functions never have to know
//! about staleness themselves).

pub mod aggregation;
pub mod binop;
pub mod functions;
pub mod staleness;

use std::collections::HashMap;

use pulsus_model::STALE_NAN_BITS;

use crate::error::PromqlError;
use crate::plan::{PlanExpr, QueryPlan, SelectorSpec};
use crate::value::{InstantSample, Labels, QueryValue, RangeSeries, Sample, SeriesData};

/// One step's evaluated value — collapsed into [`QueryValue`] once the
/// whole query (instant, or every range-query step) has been evaluated.
#[derive(Debug, Clone)]
enum StepValue {
    Vector(Vec<InstantSample>),
    Scalar(f64),
}

/// Evaluates `plan` against `data` — pure, no I/O.
pub fn evaluate(plan: &QueryPlan, data: &SeriesData) -> Result<QueryValue, PromqlError> {
    let p = &plan.params;
    if p.step_ms == 0 {
        return Ok(
            match eval_step(&plan.root, &plan.selectors, data, p.start_ms, p.lookback_ms)? {
                StepValue::Vector(v) => QueryValue::Vector(v),
                StepValue::Scalar(s) => QueryValue::Scalar(s),
            },
        );
    }

    let mut vector_points: HashMap<Labels, Vec<(i64, f64)>> = HashMap::new();
    let mut scalar_points: Vec<(i64, f64)> = Vec::new();
    let mut saw_vector = false;
    let mut saw_scalar = false;

    let mut t = p.start_ms;
    while t <= p.end_ms {
        match eval_step(&plan.root, &plan.selectors, data, t, p.lookback_ms)? {
            StepValue::Vector(v) => {
                saw_vector = true;
                for s in v {
                    vector_points.entry(s.labels).or_default().push((t, s.v));
                }
            }
            StepValue::Scalar(v) => {
                saw_scalar = true;
                scalar_points.push((t, v));
            }
        }
        t += p.step_ms;
    }

    if saw_scalar && !saw_vector {
        return Ok(QueryValue::Matrix(vec![RangeSeries {
            labels: Labels::default(),
            points: scalar_points,
        }]));
    }

    let mut out: Vec<RangeSeries> = vector_points
        .into_iter()
        .map(|(labels, points)| RangeSeries { labels, points })
        .collect();
    out.sort_by(|a, b| a.labels.cmp(&b.labels));
    Ok(QueryValue::Matrix(out))
}

/// Slices `samples` (sorted ascending) to the left-open right-closed
/// window `(lower_excl, upper_incl]` and drops any stale-NaN-marked
/// sample — the shared windowing step for both range functions and
/// `*_over_time`.
fn windowed_non_stale(samples: &[Sample], lower_excl: i64, upper_incl: i64) -> Vec<Sample> {
    let start = samples.partition_point(|s| s.t_ms <= lower_excl);
    let end = samples.partition_point(|s| s.t_ms <= upper_incl);
    samples[start..end]
        .iter()
        .copied()
        .filter(|s| s.v.to_bits() != STALE_NAN_BITS)
        .collect()
}

fn eval_step(
    expr: &PlanExpr,
    selectors: &[SelectorSpec],
    data: &SeriesData,
    t_ms: i64,
    lookback_ms: i64,
) -> Result<StepValue, PromqlError> {
    match expr {
        PlanExpr::Scalar(v) => Ok(StepValue::Scalar(*v)),

        PlanExpr::Selector(id) => {
            let sel = &selectors[*id];
            let eff_t = t_ms - sel.offset_ms;
            let mut out = Vec::new();
            for series in data.get(*id) {
                if let Some(sample) = staleness::instant_value(&series.samples, eff_t, lookback_ms)
                {
                    out.push(InstantSample {
                        labels: series.labels.clone(),
                        t_ms,
                        v: sample.v,
                    });
                }
            }
            Ok(StepValue::Vector(out))
        }

        PlanExpr::RangeFn { func, selector } => {
            let sel = &selectors[*selector];
            let eff_t = t_ms - sel.offset_ms;
            let range_ms = sel
                .range_ms
                .expect("plan() only ever builds RangeFn over a matrix selector");
            let lower_excl = eff_t - range_ms;
            let mut out = Vec::new();
            for series in data.get(*selector) {
                let windowed = windowed_non_stale(&series.samples, lower_excl, eff_t);
                if let Some(v) =
                    functions::eval_range_fn(*func, &windowed, range_ms, lower_excl, eff_t)
                {
                    out.push(InstantSample {
                        labels: series.labels.clone(),
                        t_ms,
                        v,
                    });
                }
            }
            Ok(StepValue::Vector(out))
        }

        PlanExpr::OverTime { func, selector } => {
            let sel = &selectors[*selector];
            let eff_t = t_ms - sel.offset_ms;
            let range_ms = sel
                .range_ms
                .expect("plan() only ever builds OverTime over a matrix selector");
            let lower_excl = eff_t - range_ms;
            let mut out = Vec::new();
            for series in data.get(*selector) {
                let windowed = windowed_non_stale(&series.samples, lower_excl, eff_t);
                if let Some(v) = functions::eval_over_time(*func, &windowed) {
                    out.push(InstantSample {
                        labels: series.labels.clone(),
                        t_ms,
                        v,
                    });
                }
            }
            Ok(StepValue::Vector(out))
        }

        PlanExpr::HistogramQuantile { quantile, expr } => {
            let StepValue::Scalar(q) = eval_step(quantile, selectors, data, t_ms, lookback_ms)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "histogram_quantile's first argument must evaluate to a scalar"
                        .to_string(),
                });
            };
            let StepValue::Vector(v) = eval_step(expr, selectors, data, t_ms, lookback_ms)? else {
                return Err(PromqlError::Unsupported {
                    construct: "histogram_quantile's second argument must evaluate to a vector"
                        .to_string(),
                });
            };

            let le_key = "le".to_string();
            let mut groups: HashMap<Labels, Vec<functions::Bucket>> = HashMap::new();
            for s in v {
                let le_str = s
                    .labels
                    .get("le")
                    .ok_or_else(|| PromqlError::HistogramBucket {
                        detail: "bucket series is missing an 'le' label".to_string(),
                    })?;
                let le: f64 = le_str.parse().map_err(|_| PromqlError::HistogramBucket {
                    detail: format!("invalid 'le' label value: {le_str:?}"),
                })?;
                let key = s.labels.without(std::slice::from_ref(&le_key));
                groups
                    .entry(key)
                    .or_default()
                    .push(functions::Bucket { le, count: s.v });
            }

            let mut keys: Vec<Labels> = groups.keys().cloned().collect();
            keys.sort();
            let mut out = Vec::with_capacity(keys.len());
            for key in keys {
                let buckets = groups.remove(&key).expect("key came from groups.keys()");
                let v = functions::histogram_quantile(q, buckets)?;
                out.push(InstantSample {
                    labels: key,
                    t_ms,
                    v,
                });
            }
            Ok(StepValue::Vector(out))
        }

        PlanExpr::Aggregate {
            op,
            expr,
            param,
            grouping,
        } => {
            let StepValue::Vector(v) = eval_step(expr, selectors, data, t_ms, lookback_ms)? else {
                return Err(PromqlError::Unsupported {
                    construct: "aggregation over a scalar expression".to_string(),
                });
            };
            let param_v = match param {
                Some(p) => {
                    let StepValue::Scalar(k) = eval_step(p, selectors, data, t_ms, lookback_ms)?
                    else {
                        return Err(PromqlError::Unsupported {
                            construct: "topk/bottomk's k parameter must evaluate to a scalar"
                                .to_string(),
                        });
                    };
                    Some(k)
                }
                None => None,
            };
            let out = aggregation::aggregate(*op, &v, grouping.as_ref(), param_v)?;
            Ok(StepValue::Vector(out))
        }

        PlanExpr::Binary {
            op,
            lhs,
            rhs,
            bool_modifier,
            matching,
        } => {
            let l = eval_step(lhs, selectors, data, t_ms, lookback_ms)?;
            let r = eval_step(rhs, selectors, data, t_ms, lookback_ms)?;
            match (l, r) {
                (StepValue::Scalar(l), StepValue::Scalar(r)) => {
                    Ok(StepValue::Scalar(binop::scalar_scalar(*op, l, r)))
                }
                (StepValue::Vector(v), StepValue::Scalar(s)) => Ok(StepValue::Vector(
                    binop::vector_scalar(*op, *bool_modifier, &v, s, false),
                )),
                (StepValue::Scalar(s), StepValue::Vector(v)) => Ok(StepValue::Vector(
                    binop::vector_scalar(*op, *bool_modifier, &v, s, true),
                )),
                (StepValue::Vector(l), StepValue::Vector(r)) => Ok(StepValue::Vector(
                    binop::vector_vector(*op, *bool_modifier, matching, &l, &r)?,
                )),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{PlanParams, plan};
    use crate::value::FetchedSeries;

    fn params(start_ms: i64, end_ms: i64, step_ms: i64) -> PlanParams {
        PlanParams {
            start_ms,
            end_ms,
            step_ms,
            lookback_ms: crate::plan::DEFAULT_LOOKBACK_MS,
        }
    }

    fn series(fp: u64, labels: &[(&str, &str)], samples: Vec<Sample>) -> FetchedSeries {
        FetchedSeries {
            fingerprint: fp,
            labels: Labels::new(labels.iter().map(|(k, v)| (k.to_string(), v.to_string()))),
            samples,
        }
    }

    fn s(t_ms: i64, v: f64) -> Sample {
        Sample { t_ms, v }
    }

    // --- windowed_non_stale: left-open right-closed (AC) ---

    #[test]
    fn windowed_non_stale_excludes_the_lower_bound_and_includes_the_upper_bound() {
        let samples = vec![s(0, 1.0), s(100, 2.0), s(200, 3.0)];
        let w = windowed_non_stale(&samples, 0, 200);
        // t=0 excluded (left-open), t=100 and t=200 included (right-closed
        // at the upper edge too).
        assert_eq!(w, vec![s(100, 2.0), s(200, 3.0)]);
    }

    #[test]
    fn windowed_non_stale_drops_stale_marked_samples() {
        let stale = f64::from_bits(STALE_NAN_BITS);
        let samples = vec![s(0, 1.0), s(50, stale), s(100, 2.0)];
        let w = windowed_non_stale(&samples, -1, 100);
        assert_eq!(w, vec![s(0, 1.0), s(100, 2.0)]);
    }

    // --- end-to-end evaluate() ---

    #[test]
    fn evaluates_a_bare_selector_instant_query() {
        let expr = crate::parser::parse("up").unwrap();
        let p = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("job", "a")], vec![s(900, 5.0)]),
                series(2, &[("job", "b")], vec![s(900, 7.0)]),
            ],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 2);
                assert_eq!(v[0].v, 5.0);
                assert_eq!(v[0].t_ms, 1_000);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn a_selector_with_no_sample_in_the_lookback_window_is_absent() {
        let expr = crate::parser::parse("up").unwrap();
        let p = plan(&expr, params(1_000_000, 1_000_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, 5.0)])]);
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => assert!(v.is_empty()),
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn evaluates_rate_over_a_matrix_selector() {
        let expr = crate::parser::parse("rate(http_requests_total[1m])").unwrap();
        let p = plan(&expr, params(60_000, 60_000, 0)).unwrap();
        let mut data = SeriesData::new();
        // t=1, not t=0: the range window at eval time 60_000 is left-open
        // `(0, 60_000]`, so a sample exactly at t=0 would be excluded.
        data.insert(0, vec![series(1, &[], vec![s(1, 0.0), s(60_000, 60.0)])]);
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 1);
                assert!((v[0].v - 1.0).abs() < 1e-9);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn evaluates_sum_by_aggregation() {
        let expr = crate::parser::parse("sum by (job) (up)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("job", "a")], vec![s(0, 1.0)]),
                series(2, &[("job", "a")], vec![s(0, 2.0)]),
                series(3, &[("job", "b")], vec![s(0, 5.0)]),
            ],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 2);
                assert_eq!(v[0].v, 3.0);
                assert_eq!(v[1].v, 5.0);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn evaluates_vector_scalar_arithmetic() {
        let expr = crate::parser::parse("up * 2").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[], vec![s(0, 3.0)])]);
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => assert_eq!(v[0].v, 6.0),
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn evaluates_vector_vector_arithmetic_with_on_matching() {
        let expr = crate::parser::parse("foo + on(job) bar").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, 2.0)])]);
        data.insert(1, vec![series(2, &[("job", "a")], vec![s(0, 3.0)])]);
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => assert_eq!(v[0].v, 5.0),
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn evaluates_a_scalar_literal_expression() {
        let expr = crate::parser::parse("1 + 1").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let data = SeriesData::new();
        match evaluate(&p, &data).unwrap() {
            QueryValue::Scalar(v) => assert_eq!(v, 2.0),
            other => panic!("expected Scalar, got {other:?}"),
        }
    }

    #[test]
    fn evaluates_histogram_quantile() {
        let expr = crate::parser::parse(r#"histogram_quantile(0.5, x_bucket)"#).unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("le", "0.1")], vec![s(0, 1.0)]),
                series(2, &[("le", "0.2")], vec![s(0, 2.0)]),
                series(3, &[("le", "0.5")], vec![s(0, 5.0)]),
                series(4, &[("le", "1")], vec![s(0, 10.0)]),
                series(5, &[("le", "+Inf")], vec![s(0, 10.0)]),
            ],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 1);
                assert!((v[0].v - 0.5).abs() < 1e-9, "got {}", v[0].v);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn evaluates_a_range_query_into_a_matrix() {
        let expr = crate::parser::parse("up").unwrap();
        let p = plan(&expr, params(0, 120_000, 60_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(
                1,
                &[("job", "a")],
                vec![s(0, 1.0), s(60_000, 2.0), s(120_000, 3.0)],
            )],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Matrix(m) => {
                assert_eq!(m.len(), 1);
                assert_eq!(m[0].points, vec![(0, 1.0), (60_000, 2.0), (120_000, 3.0)]);
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    #[test]
    fn offset_shifts_the_effective_lookup_time() {
        let expr = crate::parser::parse("up offset 1m").unwrap();
        let p = plan(&expr, params(120_000, 120_000, 0)).unwrap();
        let mut data = SeriesData::new();
        // Sample lives at t=60_000; querying "up offset 1m" at t=120_000
        // should look up t=60_000 effectively.
        data.insert(0, vec![series(1, &[], vec![s(60_000, 9.0)])]);
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].v, 9.0);
                // The reported timestamp is the query's own step time, not
                // the sample's own timestamp.
                assert_eq!(v[0].t_ms, 120_000);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }
}
