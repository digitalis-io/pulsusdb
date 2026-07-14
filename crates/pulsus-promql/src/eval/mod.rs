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

/// A range query's per-`Labels`-group accumulator: the group's
/// `metric_name` (issue #37 — constant across every step of one series,
/// see [`evaluate`]'s own comment) alongside its accumulated `(t_ms, v)`
/// points.
type RangeGroupAcc = (Option<String>, Vec<(i64, f64)>);

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

    // Issue #37 fix: `metric_name` is accumulated alongside `points`, keyed
    // by the same `Labels` group — it is constant across every step of one
    // series (the evaluated `PlanExpr` shape, and therefore its keep/drop
    // verdict, never changes mid-query), so capturing it once per group
    // (from whichever step first populates the group) is correct. See
    // `InstantSample::metric_name`'s doc for the keep/drop contract itself.
    let mut vector_points: HashMap<Labels, RangeGroupAcc> = HashMap::new();
    let mut scalar_points: Vec<(i64, f64)> = Vec::new();
    let mut saw_vector = false;
    let mut saw_scalar = false;

    let mut t = p.start_ms;
    while t <= p.end_ms {
        match eval_step(&plan.root, &plan.selectors, data, t, p.lookback_ms)? {
            StepValue::Vector(v) => {
                saw_vector = true;
                for s in v {
                    let InstantSample {
                        labels,
                        metric_name,
                        t_ms: _,
                        v: value,
                    } = s;
                    // Defensive invariant (architect adjudication on issue
                    // #37 code review, finding 2 — REJECT, guarded here):
                    // every member of one `Labels` group must agree on
                    // `metric_name` across every step. Two differently-
                    // named series could only collapse into the same
                    // `Labels` group via a set operation (`or`/`and`/
                    // `unless`) merging distinct metrics' outputs, and
                    // `plan::bin_op` never maps `T_LAND`/`T_LOR`/
                    // `T_LUNLESS` — they are `PromqlError::Unsupported`
                    // (`plan::tests::and_is_unsupported` et al.) — so no
                    // reachable M2 plan can produce such a pair. Same-
                    // `Labels` members are the same fingerprint anyway
                    // (fingerprint hashes the full label set,
                    // `Labels` = all labels minus `__name__`), so
                    // collapsing them is correct by construction, not
                    // merely assumed.
                    match vector_points.entry(labels) {
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            debug_assert_eq!(
                                e.get().0,
                                metric_name,
                                "issue #37 invariant: every step of one output series (same \
                                 non-name Labels) must agree on metric_name"
                            );
                            e.get_mut().1.push((t, value));
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert((metric_name, Vec::new())).1.push((t, value));
                        }
                    }
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
            metric_name: None,
            points: scalar_points,
        }]));
    }

    let mut out: Vec<RangeSeries> = vector_points
        .into_iter()
        .map(|(labels, (metric_name, points))| RangeSeries {
            labels,
            metric_name,
            points,
        })
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

        // Issue #37: a bare selector returns the **verbatim value of an
        // existing series** — Prometheus keeps `__name__` here (captured:
        // `query.name_selector_keeps_get.json`; PROVENANCE.md's
        // "`__name__` keep/drop rule" table).
        //
        // **Invariant this synthesizes `__name__` from `sel.metric_name`
        // rather than a per-fetched-row metric name (architect
        // adjudication on issue #37 code review, finding 1 — REJECT,
        // guarded here):** every reachable `SelectorSpec` carries exactly
        // one concrete metric name — `plan.rs::extract_name_and_matchers`
        // rejects both a name-less selector (`{job="x"}`) and a
        // `__name__` `Re`/`NotRe`/`NotEqual` matcher (`{__name__=~"a|b"}`)
        // as `PromqlError::Unsupported` *before* any `QueryPlan`/
        // `SelectorSpec` exists — so every series `data.get(*id)` returns
        // was fetched under `PREWHERE metric_name = {sel.metric_name}`
        // (the #30 resolver is metric-scoped the same way) and is
        // provably that one metric. See
        // `plan::tests::{a_selector_without_a_concrete_metric_name_is_unsupported,
        // a_regex_name_matcher_is_unsupported,
        // a_name_alternation_regex_matcher_is_unsupported}`.
        // M6's multi-metric selectors (`{__name__=~"a|b"}`) will need a
        // real per-row `metric_name` carried on `FetchedSeries` from the
        // fetch layer — this `debug_assert!` exists so that work can't
        // silently reuse this single-name synthesis by accident.
        PlanExpr::Selector(id) => {
            let sel = &selectors[*id];
            debug_assert!(
                !sel.metric_name.is_empty(),
                "every SelectorSpec carries exactly one concrete, non-empty metric name — \
                 plan.rs rejects nameless/regex-__name__ selectors before a QueryPlan exists"
            );
            let eff_t = t_ms - sel.offset_ms;
            let mut out = Vec::new();
            for series in data.get(*id) {
                if let Some(sample) = staleness::instant_value(&series.samples, eff_t, lookback_ms)
                {
                    out.push(InstantSample {
                        labels: series.labels.clone(),
                        metric_name: Some(sel.metric_name.clone()),
                        t_ms,
                        v: sample.v,
                    });
                }
            }
            Ok(StepValue::Vector(out))
        }

        // Issue #37: `rate`/`irate`/`increase`/`delta` **compute** a new
        // value from the windowed samples — Prometheus drops `__name__`
        // here (captured: `query.name_rate_drops_get.json`).
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
                        metric_name: None,
                        t_ms,
                        v,
                    });
                }
            }
            Ok(StepValue::Vector(out))
        }

        // Issue #37: `*_over_time` also **computes** a new value —
        // Prometheus drops `__name__` (interactively verified:
        // `avg_over_time(up[1m])`, PROVENANCE.md's table).
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
                        metric_name: None,
                        t_ms,
                        v,
                    });
                }
            }
            Ok(StepValue::Vector(out))
        }

        // Issue #37: `histogram_quantile` **computes** a new value from
        // the bucket series — Prometheus drops `__name__` (interactively
        // verified: `histogram_quantile(0.5, x_bucket_histogram_bucket)`
        // -> `"metric":{}`, PROVENANCE.md's table).
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
                    metric_name: None,
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

    /// Issue #40 (architect adjudication's semantics guard): a range
    /// `count(...)` must evaluate **per step** with the real 5-minute
    /// staleness lookback — a series that starts mid-window changes the
    /// count from that step onward, never a constant repeated across every
    /// step. This is exactly the case the label-cache-only fast path
    /// cannot see (the cache only knows "active somewhere in an hour-long
    /// bucket", not "had a sample within 5 minutes of *this* step"), which
    /// is why #40 gates that fast path to instant queries only
    /// (`pulsus-read`'s `MetricsEngine::query_inner`) and routes every
    /// range `count`/`group` through this ordinary per-step evaluate path
    /// instead.
    #[test]
    fn a_range_count_with_a_mid_window_series_start_has_non_constant_per_step_counts() {
        let expr = crate::parser::parse("count(up)").unwrap();
        // 3 steps, spaced exactly one lookback (5m) apart: t=0, t=300_000,
        // t=600_000.
        let p = plan(&expr, params(0, 600_000, 300_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                // Series A: present at every step.
                series(
                    1,
                    &[("job", "a")],
                    vec![s(0, 1.0), s(300_000, 1.0), s(600_000, 1.0)],
                ),
                // Series B: first sample lands mid-window (t=300_000) — at
                // t=0 its lookback window `(-300_000, 0]` has no sample of
                // B in it, so it is correctly absent from that step alone.
                series(2, &[("job", "b")], vec![s(300_000, 1.0), s(600_000, 1.0)]),
            ],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Matrix(m) => {
                assert_eq!(m.len(), 1, "count(...) with no grouping is one series");
                assert_eq!(
                    m[0].points,
                    vec![(0, 1.0), (300_000, 2.0), (600_000, 2.0)],
                    "count must be 1 before series B starts and 2 from its first sample onward \
                     — never a constant 2 repeated across every step"
                );
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

    // --- issue #37: `__name__` keep/drop rule per construct class ---
    //
    // Verified against real captured `prom/prometheus:v3.13.0` responses
    // (`crates/pulsus-server/tests/fixtures/prom_api/PROVENANCE.md`'s
    // "`__name__` keep/drop rule per construct class" table) — every case
    // below cites the construct class that table pins.

    fn instant_vector(v: QueryValue) -> Vec<InstantSample> {
        match v {
            QueryValue::Vector(v) => v,
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    /// Bare selector: KEEP (`query.name_selector_keeps_get.json`).
    #[test]
    fn bare_selector_keeps_metric_name() {
        let expr = crate::parser::parse("up").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, 1.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v[0].metric_name.as_deref(), Some("up"));
    }

    /// A selector's `metric_name` propagates into a range query's
    /// `RangeSeries` too (constant across every step of the same series).
    #[test]
    fn bare_selector_range_query_keeps_metric_name() {
        let expr = crate::parser::parse("up").unwrap();
        let p = plan(&expr, params(0, 60_000, 60_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(1, &[("job", "a")], vec![s(0, 1.0), s(60_000, 2.0)])],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Matrix(m) => {
                assert_eq!(m.len(), 1);
                assert_eq!(m[0].metric_name.as_deref(), Some("up"));
                assert_eq!(m[0].points.len(), 2);
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    /// Aggregation: DROP (`query.name_aggregation_drops_get.json`).
    #[test]
    fn aggregation_drops_metric_name() {
        let expr = crate::parser::parse("sum(up)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("job", "a")], vec![s(0, 1.0)]),
                series(2, &[("job", "b")], vec![s(0, 2.0)]),
            ],
        );
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].metric_name, None);
    }

    /// `rate`/`increase`/`delta`/`irate`: DROP
    /// (`query.name_rate_drops_get.json`).
    #[test]
    fn rate_drops_metric_name() {
        let expr = crate::parser::parse("rate(http_requests_total[1m])").unwrap();
        let p = plan(&expr, params(60_000, 60_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[], vec![s(1, 0.0), s(60_000, 60.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].metric_name, None);
    }

    /// `*_over_time`: DROP (interactively verified — see PROVENANCE.md).
    #[test]
    fn over_time_drops_metric_name() {
        let expr = crate::parser::parse("avg_over_time(up[1m])").unwrap();
        let p = plan(&expr, params(60_000, 60_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[], vec![s(1, 1.0), s(60_000, 3.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].metric_name, None);
    }

    /// `histogram_quantile`: DROP (interactively verified — see
    /// PROVENANCE.md).
    #[test]
    fn histogram_quantile_drops_metric_name() {
        let expr = crate::parser::parse(r#"histogram_quantile(0.5, x_bucket)"#).unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("le", "0.5")], vec![s(0, 5.0)]),
                series(2, &[("le", "+Inf")], vec![s(0, 5.0)]),
            ],
        );
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].metric_name, None);
    }

    /// Vector-scalar arithmetic: DROP (interactively verified: `up * 2`).
    #[test]
    fn vector_scalar_arithmetic_drops_metric_name() {
        let expr = crate::parser::parse("up * 2").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[], vec![s(0, 3.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v[0].metric_name, None);
    }

    /// Vector-scalar comparison, filter mode (no `bool`): KEEP
    /// (interactively verified: `up > 0`).
    #[test]
    fn vector_scalar_filter_comparison_keeps_metric_name() {
        let expr = crate::parser::parse("up > 0").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[], vec![s(0, 3.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].metric_name.as_deref(), Some("up"));
    }

    /// Vector-scalar comparison, `bool` mode: DROP (interactively
    /// verified: `up > bool 0`).
    #[test]
    fn vector_scalar_bool_comparison_drops_metric_name() {
        let expr = crate::parser::parse("up > bool 0").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[], vec![s(0, 3.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v[0].metric_name, None);
    }

    /// Vector-vector arithmetic: DROP (interactively verified:
    /// `up + on(job) up`, i.e. `foo + on(job) bar` here).
    #[test]
    fn vector_vector_arithmetic_drops_metric_name() {
        let expr = crate::parser::parse("foo + on(job) bar").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, 2.0)])]);
        data.insert(1, vec![series(2, &[("job", "a")], vec![s(0, 3.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v[0].metric_name, None);
    }

    /// Vector-vector comparison, filter mode (no `bool`): KEEP the LHS's
    /// `metric_name` (interactively verified: `up > (up - 1)`).
    #[test]
    fn vector_vector_filter_comparison_keeps_the_lhs_metric_name() {
        let expr = crate::parser::parse("foo > bar").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, 5.0)])]);
        data.insert(1, vec![series(2, &[("job", "a")], vec![s(0, 3.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].metric_name.as_deref(), Some("foo"));
    }

    /// Vector-vector comparison, `bool` mode: DROP (interactively
    /// verified: `up > bool up`).
    #[test]
    fn vector_vector_bool_comparison_drops_metric_name() {
        let expr = crate::parser::parse("foo > bool bar").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, 5.0)])]);
        data.insert(1, vec![series(2, &[("job", "a")], vec![s(0, 3.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v[0].metric_name, None);
    }

    /// `topk`/`bottomk`: KEEP — they select existing series verbatim,
    /// never compute a new value (interactively verified: `topk(1, up)`).
    #[test]
    fn topk_keeps_metric_name() {
        let expr = crate::parser::parse("topk(1, up)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("job", "a")], vec![s(0, 5.0)]),
                series(2, &[("job", "b")], vec![s(0, 1.0)]),
            ],
        );
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].metric_name.as_deref(), Some("up"));
    }

    /// Issue #37 code-review finding 3: `on(...)` DROPS `__name__` (captured:
    /// `query.name_comparison_on_drops_get.json`) — `Keep(job)` retains
    /// only the `on`-listed labels, `__name__` not among them.
    #[test]
    fn vector_vector_filter_comparison_with_on_drops_metric_name() {
        let expr = crate::parser::parse("foo == on(job) bar").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(
                1,
                &[("job", "a"), ("instance", "1")],
                vec![s(0, 5.0)],
            )],
        );
        data.insert(
            1,
            vec![series(
                2,
                &[("job", "a"), ("instance", "2")],
                vec![s(0, 5.0)],
            )],
        );
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].metric_name, None);
    }

    /// Issue #37 code-review finding 3: `ignoring(...)` KEEPS `__name__`
    /// (captured: `query.name_comparison_plain_keeps_get.json` covers the
    /// no-modifier case; `Del(instance)` drops only the ignored label,
    /// `__name__` survives).
    #[test]
    fn vector_vector_filter_comparison_with_ignoring_keeps_metric_name() {
        let expr = crate::parser::parse("foo == ignoring(instance) bar").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(
                1,
                &[("job", "a"), ("instance", "1")],
                vec![s(0, 5.0)],
            )],
        );
        data.insert(
            1,
            vec![series(
                2,
                &[("job", "a"), ("instance", "2")],
                vec![s(0, 5.0)],
            )],
        );
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].metric_name.as_deref(), Some("foo"));
    }
}
