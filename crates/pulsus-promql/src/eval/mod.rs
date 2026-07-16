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
pub mod datetime;
pub mod elementwise;
pub mod functions;
pub mod labels;
pub mod staleness;

use std::collections::HashMap;

use pulsus_model::STALE_NAN_BITS;

use crate::error::PromqlError;
use crate::plan::{MathFn, OverTimeFn, PlanExpr, QueryPlan, ScalarFn, SelectorSpec};
use crate::value::{InstantSample, Labels, QueryValue, RangeSeries, Sample, SeriesData};

/// One step's evaluated value — collapsed into [`QueryValue`] once the
/// whole query (instant, or every range-query step) has been evaluated.
#[derive(Debug, Clone)]
enum StepValue {
    Vector(Vec<InstantSample>),
    Scalar(f64),
}

/// The FULL upstream series identity (plan v3 Δ5): the kept metric name
/// (`None` for name-dropping constructs) alongside the non-name label
/// set — upstream hashes the complete label set, `__name__` included.
type SeriesIdentity = (Option<String>, Labels);

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

    // Issue #68 (plan v3 Δ5, superseding the #37 `Labels`-only key): the
    // range accumulator keys on the FULL upstream series identity
    // `(metric_name, Labels)` — upstream hashes the complete label set,
    // `__name__` included. The old key relied on "every member of one
    // `Labels` group agrees on `metric_name`", which M6-05's
    // `label_replace`/`label_join` `__name__` rewrites break (a nested
    // `label_replace` can produce two metric names sharing one non-name
    // label set — the `Labels`-only key silently merged them; see
    // `a_labels_only_range_key_would_collapse_distinct_metric_names`).
    // Cross-name merges cannot happen now because the key separates them;
    // every step of one `(metric_name, Labels)` group agrees on the name
    // by construction, so no per-entry assertion is needed. Existing
    // query classes are outcome-unchanged: name-dropping classes map to
    // `(None, Labels)` (a bijection with the old key) and name-keeping
    // classes carry one concrete name per selector (the plan invariant),
    // so the pair partitions identically. Rewritten series with the same
    // full identity at disjoint step times still merge into one output
    // series (they never coexist at a step; the per-step duplicate check
    // in `eval::labels` errors when they do overlap).
    let mut vector_points: HashMap<SeriesIdentity, Vec<(i64, f64)>> = HashMap::new();
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
                    vector_points
                        .entry((metric_name, labels))
                        .or_default()
                        .push((t, value));
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
        .map(|((metric_name, labels), points)| RangeSeries {
            labels,
            metric_name,
            points,
        })
        .collect();
    // `(labels, metric_name)` tie-break (plan v3 Δ5): the wire order is
    // the encoder's own matrix label-sort either way — this only pins
    // internal determinism when two full identities share their non-name
    // labels.
    out.sort_by(|a, b| (&a.labels, &a.metric_name).cmp(&(&b.labels, &b.metric_name)));
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

        // Issue #37: `*_over_time` **computes** a new value — Prometheus
        // drops `__name__` (interactively verified:
        // `avg_over_time(up[1m])`, PROVENANCE.md's table) — with exactly
        // two exceptions (issue #67): `last_over_time`/`first_over_time`
        // act like a time-shifted selector and KEEP the metric name
        // (upstream engine.go: "The last_over_time function acts like
        // offset; thus, it should keep the metric name"; pinned by the
        // vendored `name_label_dropping.test:42-48` — "Does not drop
        // __name__ for last_over_time/first_over_time function" — and
        // arbitrated live by the #67 `last_over_time` differential row).
        // The synthesis from `sel.metric_name` is sound for the same
        // reason as the `Selector` arm's (one concrete metric name per
        // reachable SelectorSpec).
        PlanExpr::OverTime { func, selector } => {
            let sel = &selectors[*selector];
            let eff_t = t_ms - sel.offset_ms;
            let range_ms = sel
                .range_ms
                .expect("plan() only ever builds OverTime over a matrix selector");
            let lower_excl = eff_t - range_ms;
            let keeps_name = matches!(func, OverTimeFn::Last | OverTimeFn::First);
            let mut out = Vec::new();
            for series in data.get(*selector) {
                let windowed = windowed_non_stale(&series.samples, lower_excl, eff_t);
                if let Some(v) = functions::eval_over_time(*func, &windowed) {
                    out.push(InstantSample {
                        labels: series.labels.clone(),
                        metric_name: keeps_name.then(|| sel.metric_name.clone()),
                        t_ms,
                        v,
                    });
                }
            }
            Ok(StepValue::Vector(out))
        }

        // Issue #67 (M6-04): parameterized range-window functions — the
        // same windowing as `OverTime`, plus scalar parameter(s) evaluated
        // per step. `__name__` DROPS (computed values). `predict_linear`'s
        // regression intercept is the evaluation **step time** `t_ms`
        // (upstream `enh.Ts` — the #67 adjudication), passed through as
        // `eval_t_ms`.
        PlanExpr::OverTimeParam {
            func,
            selector,
            args,
        } => {
            let sel = &selectors[*selector];
            let eff_t = t_ms - sel.offset_ms;
            let range_ms = sel
                .range_ms
                .expect("plan() only ever builds OverTimeParam over a matrix selector");
            let lower_excl = eff_t - range_ms;
            let mut scalars = Vec::with_capacity(args.len());
            for a in args {
                let StepValue::Scalar(s) = eval_step(a, selectors, data, t_ms, lookback_ms)? else {
                    return Err(PromqlError::Unsupported {
                        construct: format!("{func:?} with a non-scalar parameter argument"),
                    });
                };
                scalars.push(s);
            }
            let mut out = Vec::new();
            for series in data.get(*selector) {
                let windowed = windowed_non_stale(&series.samples, lower_excl, eff_t);
                // Upstream engine.go only invokes a matrix-argument
                // function for a series with at least one point in the
                // step's window (`if len(ss.Floats)+len(ss.Histograms) >
                // 0`), so per-invocation side effects — specifically
                // `double_exponential_smoothing`'s factor-validation panic
                // — never fire for an empty selection (#67 code review
                // finding 2, resolved empirically against
                // prom/prometheus:v3.13.0: invalid sf/tf over a
                // no-match/no-metric/empty-window selector is HTTP 200
                // with an empty result; the same factors over a selection
                // with data are a 422 execution error). This skip is that
                // engine-level guard; the validation itself stays inside
                // `eval_over_time_param`, mirroring upstream's layering.
                if windowed.is_empty() {
                    continue;
                }
                if let Some(v) = functions::eval_over_time_param(*func, &windowed, &scalars, t_ms)?
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

        // Issue #67 (M6-04): `absent_over_time(m[r])` — one synthetic
        // series (value `1`) iff **no** matched series has a sample in the
        // step's window (a selector matching zero series included);
        // otherwise an empty vector. The synthetic labels port upstream
        // `createLabelsForAbsentFunction` (v3.13.0) exactly — see
        // [`labels::labels_for_absent`] (issue #68 factored the shared
        // matcher-walk out; the full upstream `labels.Builder` provenance
        // trace and the live-verified order-sensitivity live on that
        // helper's doc). `__name__` is never emitted (upstream skips
        // MetricName matchers; `sel.matchers` already excludes it by the
        // planner's metric-scoping rule).
        PlanExpr::AbsentOverTime { selector } => {
            let sel = &selectors[*selector];
            let eff_t = t_ms - sel.offset_ms;
            let range_ms = sel
                .range_ms
                .expect("plan() only ever builds AbsentOverTime over a matrix selector");
            let lower_excl = eff_t - range_ms;
            let present = data
                .get(*selector)
                .iter()
                .any(|series| !windowed_non_stale(&series.samples, lower_excl, eff_t).is_empty());
            if present {
                return Ok(StepValue::Vector(Vec::new()));
            }
            Ok(StepValue::Vector(vec![InstantSample {
                labels: labels::labels_for_absent(&sel.matchers),
                metric_name: None,
                t_ms,
                v: 1.0,
            }]))
        }

        // Issue #68 (M6-05): `absent(v)` — one synthetic series (value
        // `1`) iff the evaluated instant vector is empty at the step;
        // otherwise an empty vector. A bare (paren-stripped) vector-
        // selector argument synthesizes labels from its matchers via the
        // exact `absent_over_time` walk ([`labels::labels_for_absent`] —
        // vendored functions.test:1700-1712); any computed argument
        // (`sum(...)`, `a+b`, `rate(...)`, a filter comparison) yields
        // the empty label set (:1735-1750). `__name__` is never emitted.
        PlanExpr::Absent { arg, selector } => {
            let StepValue::Vector(v) = eval_step(arg, selectors, data, t_ms, lookback_ms)? else {
                return Err(PromqlError::Unsupported {
                    construct: "absent() over a scalar expression".to_string(),
                });
            };
            if !v.is_empty() {
                return Ok(StepValue::Vector(Vec::new()));
            }
            let labels = match selector {
                Some(id) => labels::labels_for_absent(&selectors[*id].matchers),
                None => Labels::default(),
            };
            Ok(StepValue::Vector(vec![InstantSample {
                labels,
                metric_name: None,
                t_ms,
                v: 1.0,
            }]))
        }

        // Issue #68 (M6-05): `sort(v)`/`sort_desc(v)` — pass-through of
        // existing series (KEEP `__name__`), reordered by value with NaN
        // last in BOTH directions (functions.test:703,715). Ordering is
        // observable only for an instant query (the range accumulator
        // above collapses per-step order by construction — upstream's
        // own "sort is ineffective for range queries"); the server
        // encoder preserves this order on the wire for sort-rooted
        // instant queries (`expr_is_sort_root`).
        PlanExpr::Sort { descending, arg } => {
            let StepValue::Vector(mut v) = eval_step(arg, selectors, data, t_ms, lookback_ms)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "sort()/sort_desc() over a scalar expression".to_string(),
                });
            };
            labels::sort_vector(&mut v, *descending);
            Ok(StepValue::Vector(v))
        }

        // Issue #68 (M6-05, experimental): `sort_by_label(_desc)(v, …)` —
        // pass-through reordered by natural (numeric-aware) label
        // collation in argument order, full-virtual-labelset tie-break
        // (functions.test:755-871; plan v2 Δ2).
        PlanExpr::SortByLabel {
            descending,
            labels: names,
            arg,
        } => {
            let StepValue::Vector(mut v) = eval_step(arg, selectors, data, t_ms, lookback_ms)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "sort_by_label()/sort_by_label_desc() over a scalar expression"
                        .to_string(),
                });
            };
            labels::sort_by_label_vector(&mut v, names, *descending);
            Ok(StepValue::Vector(v))
        }

        // Issue #68 (M6-05): `label_replace`/`label_join` — joint
        // `(metric_name, Labels)` rewrites (KEEP `__name__` unless the
        // rewrite itself targets it), with per-step duplicate-identity
        // detection (functions.test:477-527/:562-591).
        PlanExpr::LabelReplace {
            arg,
            dst,
            replacement,
            src,
            regex,
        } => {
            let StepValue::Vector(v) = eval_step(arg, selectors, data, t_ms, lookback_ms)? else {
                return Err(PromqlError::Unsupported {
                    construct: "label_replace() over a scalar expression".to_string(),
                });
            };
            Ok(StepValue::Vector(labels::label_replace_vector(
                v,
                dst,
                replacement,
                src,
                regex,
            )?))
        }
        PlanExpr::LabelJoin {
            arg,
            dst,
            separator,
            src_labels,
        } => {
            let StepValue::Vector(v) = eval_step(arg, selectors, data, t_ms, lookback_ms)? else {
                return Err(PromqlError::Unsupported {
                    construct: "label_join() over a scalar expression".to_string(),
                });
            };
            Ok(StepValue::Vector(labels::label_join_vector(
                v, dst, separator, src_labels,
            )?))
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

        // Issue #65 (M6-02): elementwise math/trig **computes** a new
        // value per sample — `__name__` DROPS (the same class as
        // `rate`/`*_over_time` per the #37 keep/drop table).
        PlanExpr::MathFn {
            func,
            arg,
            scalar_args,
        } => {
            let StepValue::Vector(v) = eval_step(arg, selectors, data, t_ms, lookback_ms)? else {
                return Err(PromqlError::Unsupported {
                    construct: format!("{func:?} over a scalar expression"),
                });
            };
            let mut scalars = Vec::with_capacity(scalar_args.len());
            for sa in scalar_args {
                let StepValue::Scalar(s) = eval_step(sa, selectors, data, t_ms, lookback_ms)?
                else {
                    return Err(PromqlError::Unsupported {
                        construct: format!("{func:?} with a non-scalar bound argument"),
                    });
                };
                scalars.push(s);
            }
            // The planner guarantees `scalar_args`' count per MathFn
            // discriminant; this match re-checks it structurally
            // (defense-in-depth — a descriptive error, never a panic,
            // plan v2 Δ3). Resolved to a tiny Copy op enum once per step —
            // no per-sample or per-step allocation in this hot path.
            #[derive(Clone, Copy)]
            enum Op {
                Clamp(f64, f64),
                ClampMin(f64),
                ClampMax(f64),
                Round(f64),
                Unary(MathFn),
            }
            let op = match (func, scalars.as_slice()) {
                (MathFn::Clamp, &[min, max]) => {
                    // Upstream funcClamp: `max < min` empties the whole
                    // step's vector. Ordinary `<` is NaN-safe here — a
                    // NaN bound never triggers this branch and flows to
                    // a NaN per-sample result instead (plan v2 Δ2).
                    if max < min {
                        return Ok(StepValue::Vector(Vec::new()));
                    }
                    Op::Clamp(min, max)
                }
                (MathFn::ClampMin, &[min]) => Op::ClampMin(min),
                (MathFn::ClampMax, &[max]) => Op::ClampMax(max),
                (MathFn::Round, &[to_nearest]) => Op::Round(to_nearest),
                (
                    func @ (MathFn::Clamp | MathFn::ClampMin | MathFn::ClampMax | MathFn::Round),
                    _,
                ) => {
                    return Err(PromqlError::Unsupported {
                        construct: format!(
                            "{func:?} with {} scalar argument(s) — plan() guarantees the \
                             per-function count; this plan was not built by plan()",
                            scalars.len()
                        ),
                    });
                }
                (func, []) => Op::Unary(*func),
                (func, _) => {
                    return Err(PromqlError::Unsupported {
                        construct: format!(
                            "unary {func:?} with {} scalar argument(s) — plan() guarantees \
                             none; this plan was not built by plan()",
                            scalars.len()
                        ),
                    });
                }
            };
            let out = v
                .into_iter()
                .map(|s| InstantSample {
                    labels: s.labels,
                    metric_name: None,
                    t_ms,
                    v: match op {
                        Op::Clamp(min, max) => elementwise::clamp(min, max, s.v),
                        Op::ClampMin(min) => elementwise::clamp_min(min, s.v),
                        Op::ClampMax(max) => elementwise::clamp_max(max, s.v),
                        Op::Round(to_nearest) => elementwise::round(to_nearest, s.v),
                        Op::Unary(func) => elementwise::unary(func, s.v),
                    },
                })
                .collect();
            Ok(StepValue::Vector(out))
        }

        // Issue #65 (M6-02): scalar→scalar functions.
        PlanExpr::ScalarFn { func, args } => {
            let mut scalars = Vec::with_capacity(args.len());
            for a in args {
                let StepValue::Scalar(s) = eval_step(a, selectors, data, t_ms, lookback_ms)? else {
                    return Err(PromqlError::Unsupported {
                        construct: format!("{func:?} with a non-scalar argument"),
                    });
                };
                scalars.push(s);
            }
            let v = match (func, scalars.as_slice()) {
                (ScalarFn::Pi, []) => elementwise::pi(),
                (ScalarFn::MaxOf, &[a, b]) => elementwise::max_of(a, b),
                (ScalarFn::MinOf, &[a, b]) => elementwise::min_of(a, b),
                (func, _) => {
                    return Err(PromqlError::Unsupported {
                        construct: format!(
                            "{func:?} with {} argument(s) — plan() guarantees the \
                             per-function count; this plan was not built by plan()",
                            scalars.len()
                        ),
                    });
                }
            };
            Ok(StepValue::Scalar(v))
        }

        // Issue #66 (M6-03): `time()` — the evaluation step time in
        // seconds, a scalar (per-step in a range query via `evaluate`'s
        // ordinary scalar-step accumulation).
        PlanExpr::Time => Ok(StepValue::Scalar(t_ms as f64 / 1000.0)),

        // Issue #66 (M6-03): the date/time-field family **computes** a
        // new value — `__name__` DROPS (the same class as `rate` per the
        // #37 keep/drop table).
        PlanExpr::DateFn { func, arg } => match arg {
            // No argument: the field of the evaluation step time, one
            // empty-labelset element (upstream's `vector(time())`
            // default). Go computes `time.Unix(enh.Ts/1000, 0)` — integer
            // division truncating toward zero, exactly Rust `i64 /`.
            None => Ok(StepValue::Vector(vec![InstantSample {
                labels: Labels::default(),
                metric_name: None,
                t_ms,
                v: datetime::field(*func, t_ms / 1000),
            }])),
            // Vector argument: each element's VALUE is the unix-seconds
            // instant. `to_unix_secs` is the total conversion (plan v2
            // Δ1): NaN/±Inf/|v| >= 2^63 yield a NaN result element (kept,
            // labels minus `__name__`), never a platform-defined cast.
            Some(arg) => {
                let StepValue::Vector(v) = eval_step(arg, selectors, data, t_ms, lookback_ms)?
                else {
                    return Err(PromqlError::Unsupported {
                        construct: format!("{func:?} over a scalar expression"),
                    });
                };
                let out = v
                    .into_iter()
                    .map(|s| InstantSample {
                        labels: s.labels,
                        metric_name: None,
                        t_ms,
                        v: datetime::to_unix_secs(s.v)
                            .map(|secs| datetime::field(*func, secs))
                            .unwrap_or(f64::NAN),
                    })
                    .collect();
                Ok(StepValue::Vector(out))
            }
        },

        // Issue #66 (M6-03): `timestamp(v)` — `__name__` DROPS (computed
        // value). The bare-(paren-stripped)-selector case returns each
        // series' REAL sample timestamp (upstream's special case: the
        // sample resolved by the ordinary staleness lookup at the
        // offset-shifted step time, its own `t_ms` in seconds — the raw
        // stored time, with no offset added back; the differential row
        // `timestamp(... offset 1m)` arbitrates that choice per the #66
        // adjudication). Every computed argument instead stamps the
        // evaluation step time per element.
        PlanExpr::Timestamp { arg, bare_selector } => match bare_selector {
            Some(id) => {
                let sel = &selectors[*id];
                let eff_t = t_ms - sel.offset_ms;
                let mut out = Vec::new();
                for series in data.get(*id) {
                    if let Some(sample) =
                        staleness::instant_value(&series.samples, eff_t, lookback_ms)
                    {
                        out.push(InstantSample {
                            labels: series.labels.clone(),
                            metric_name: None,
                            t_ms,
                            v: sample.t_ms as f64 / 1000.0,
                        });
                    }
                }
                Ok(StepValue::Vector(out))
            }
            None => {
                let StepValue::Vector(v) = eval_step(arg, selectors, data, t_ms, lookback_ms)?
                else {
                    return Err(PromqlError::Unsupported {
                        construct: "timestamp() over a scalar expression".to_string(),
                    });
                };
                let out = v
                    .into_iter()
                    .map(|s| InstantSample {
                        labels: s.labels,
                        metric_name: None,
                        t_ms,
                        v: t_ms as f64 / 1000.0,
                    })
                    .collect();
                Ok(StepValue::Vector(out))
            }
        },

        // Issue #66 (M6-03): `scalar(v)` — the singleton element's value,
        // NaN for zero or multiple elements (upstream funcScalar).
        PlanExpr::ScalarOf { arg } => {
            let StepValue::Vector(v) = eval_step(arg, selectors, data, t_ms, lookback_ms)? else {
                return Err(PromqlError::Unsupported {
                    construct: "scalar() over a scalar expression".to_string(),
                });
            };
            Ok(StepValue::Scalar(match v.as_slice() {
                [only] => only.v,
                _ => f64::NAN,
            }))
        }

        // Issue #66 (M6-03): `vector(s)` — one element with the EMPTY
        // label set (and no `__name__`, upstream funcVector).
        PlanExpr::VectorOf { arg } => {
            let StepValue::Scalar(s) = eval_step(arg, selectors, data, t_ms, lookback_ms)? else {
                return Err(PromqlError::Unsupported {
                    construct: "vector() over a non-scalar expression".to_string(),
                });
            };
            Ok(StepValue::Vector(vec![InstantSample {
                labels: Labels::default(),
                metric_name: None,
                t_ms,
                v: s,
            }]))
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
            experimental_functions: false,
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

    // --- issue #66 (M6-03): time/date functions + scalar/vector ---

    #[test]
    fn time_evaluates_to_the_eval_time_in_seconds() {
        let expr = crate::parser::parse("time()").unwrap();
        let p = plan(&expr, params(55_000, 55_000, 0)).unwrap();
        match evaluate(&p, &SeriesData::new()).unwrap() {
            QueryValue::Scalar(v) => assert_eq!(v, 55.0),
            other => panic!("expected Scalar, got {other:?}"),
        }
    }

    /// AC6: `time()` varies per step in a range query — the existing
    /// `t_ms` threading, no new machinery.
    #[test]
    fn time_range_query_varies_per_step() {
        let expr = crate::parser::parse("time()").unwrap();
        let p = plan(&expr, params(0, 120_000, 60_000)).unwrap();
        match evaluate(&p, &SeriesData::new()).unwrap() {
            QueryValue::Matrix(m) => {
                assert_eq!(m.len(), 1);
                assert!(m[0].labels.is_empty());
                assert_eq!(
                    m[0].points,
                    vec![(0, 0.0), (60_000, 60.0), (120_000, 120.0)]
                );
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    #[test]
    fn scalar_of_a_singleton_vector_is_its_value() {
        let expr = crate::parser::parse("scalar(up)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, 42.0)])]);
        match evaluate(&p, &data).unwrap() {
            QueryValue::Scalar(v) => assert_eq!(v, 42.0),
            other => panic!("expected Scalar, got {other:?}"),
        }
    }

    #[test]
    fn scalar_of_a_non_singleton_or_empty_vector_is_nan() {
        let expr = crate::parser::parse("scalar(up)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        // Two elements -> NaN.
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("job", "a")], vec![s(0, 1.0)]),
                series(2, &[("job", "b")], vec![s(0, 2.0)]),
            ],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Scalar(v) => assert!(v.is_nan(), "two elements must be NaN, got {v}"),
            other => panic!("expected Scalar, got {other:?}"),
        }
        // Zero elements -> NaN.
        match evaluate(&p, &SeriesData::new()).unwrap() {
            QueryValue::Scalar(v) => assert!(v.is_nan(), "empty must be NaN, got {v}"),
            other => panic!("expected Scalar, got {other:?}"),
        }
    }

    #[test]
    fn vector_of_a_scalar_is_a_single_empty_labelset_element() {
        let expr = crate::parser::parse("vector(3)").unwrap();
        let p = plan(&expr, params(7_000, 7_000, 0)).unwrap();
        let v = instant_vector(evaluate(&p, &SeriesData::new()).unwrap());
        assert_eq!(v.len(), 1);
        assert!(v[0].labels.is_empty(), "vector() labels must be empty");
        assert_eq!(v[0].metric_name, None);
        assert_eq!(v[0].v, 3.0);
        assert_eq!(v[0].t_ms, 7_000);
    }

    /// AC6: `timestamp(m)` over a bare selector returns the REAL sample
    /// timestamp — instant at 90s over 1-minute samples resolves the 60s
    /// sample, so the value is 60, not 90.
    #[test]
    fn timestamp_of_a_bare_selector_returns_the_sample_time_not_the_step_time() {
        let expr = crate::parser::parse("timestamp(up)").unwrap();
        let p = plan(&expr, params(90_000, 90_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(1, &[("job", "a")], vec![s(0, 5.0), s(60_000, 7.0)])],
        );
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].v, 60.0, "must be the sample time (60s), not 90s");
        assert_eq!(v[0].metric_name, None, "timestamp() drops __name__");
        assert_eq!(
            v[0].t_ms, 90_000,
            "the reported step time stays the query's own"
        );
    }

    /// The eval-time branch: a computed argument stamps the step time per
    /// element (contrast with the bare-selector case above).
    #[test]
    fn timestamp_of_a_computed_argument_returns_the_step_time() {
        for query in [
            "timestamp(abs(up))",
            "timestamp(up + 0)",
            "timestamp(timestamp(up))",
        ] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, params(90_000, 90_000, 0)).unwrap();
            let mut data = SeriesData::new();
            data.insert(
                0,
                vec![series(1, &[("job", "a")], vec![s(0, 5.0), s(60_000, 7.0)])],
            );
            let v = instant_vector(evaluate(&p, &data).unwrap());
            assert_eq!(v.len(), 1, "{query}");
            assert_eq!(
                v[0].v, 90.0,
                "{query}: computed args carry the eval step time"
            );
        }
    }

    /// `timestamp(m offset 1m)`: the base implementation returns the raw
    /// stored sample time (no offset added back) — the differential row
    /// arbitrates this against real Prometheus (#66 adjudication).
    #[test]
    fn timestamp_with_offset_returns_the_raw_stored_sample_time() {
        let expr = crate::parser::parse("timestamp(up offset 1m)").unwrap();
        let p = plan(&expr, params(120_000, 120_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[], vec![s(45_000, 9.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].v, 45.0);
    }

    #[test]
    fn date_fn_reads_element_values_as_unix_seconds_and_drops_metric_name() {
        let expr = crate::parser::parse("month(ts)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("t", "epoch")], vec![s(0, 0.0)]),
                series(2, &[("t", "nov85")], vec![s(0, 500_000_000.0)]),
            ],
        );
        let mut v = instant_vector(evaluate(&p, &data).unwrap());
        v.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].v, 1.0); // epoch -> January
        assert_eq!(v[1].v, 11.0); // 500000000 -> November 1985
        assert!(v.iter().all(|s| s.metric_name.is_none()));
    }

    /// Plan v2 Δ1: NaN/±Inf/out-of-range date inputs yield NaN result
    /// elements (kept, labels minus `__name__`) — our documented, total
    /// behavior — while finite siblings compute normally.
    #[test]
    fn date_fn_maps_nan_and_inf_inputs_to_nan_elements() {
        let expr = crate::parser::parse("year(ts)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("t", "nan")], vec![s(0, f64::NAN)]),
                series(2, &[("t", "inf")], vec![s(0, f64::INFINITY)]),
                series(3, &[("t", "ninf")], vec![s(0, f64::NEG_INFINITY)]),
                series(4, &[("t", "big")], vec![s(0, 1e19)]),
                series(5, &[("t", "fin")], vec![s(0, 500_000_000.0)]),
            ],
        );
        let mut v = instant_vector(evaluate(&p, &data).unwrap());
        v.sort_by(|a, b| a.labels.cmp(&b.labels));
        let by_t: Vec<(&str, f64)> = v
            .iter()
            .map(|s| (s.labels.get("t").unwrap(), s.v))
            .collect();
        assert_eq!(v.len(), 5, "every element is kept");
        for (t, val) in &by_t {
            match *t {
                "fin" => assert_eq!(*val, 1985.0),
                _ => assert!(val.is_nan(), "{t} must yield NaN, got {val}"),
            }
        }
    }

    #[test]
    fn no_argument_date_fn_uses_the_eval_time_with_an_empty_labelset() {
        // 4000s = 1970-01-01 01:06:40 UTC.
        let expr = crate::parser::parse("hour()").unwrap();
        let p = plan(&expr, params(4_000_000, 4_000_000, 0)).unwrap();
        let v = instant_vector(evaluate(&p, &SeriesData::new()).unwrap());
        assert_eq!(v.len(), 1);
        assert!(v[0].labels.is_empty());
        assert_eq!(v[0].metric_name, None);
        assert_eq!(v[0].v, 1.0);
    }

    // --- issue #67 (M6-04): range-vector function completion ---

    fn experimental(p: PlanParams) -> PlanParams {
        PlanParams {
            experimental_functions: true,
            ..p
        }
    }

    /// The new `*_over_time`/counter/regression fns **compute** — DROP
    /// (#37 rule), with the two upstream-pinned exceptions asserted in
    /// the next test.
    #[test]
    fn m6_04_computed_range_fns_drop_metric_name() {
        for query in [
            "stddev_over_time(up[1m])",
            "changes(up[1m])",
            "deriv(up[1m])",
            "quantile_over_time(0.5, up[1m])",
        ] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, params(60_000, 60_000, 0)).unwrap();
            let mut data = SeriesData::new();
            data.insert(0, vec![series(1, &[], vec![s(1, 1.0), s(60_000, 3.0)])]);
            let v = instant_vector(evaluate(&p, &data).unwrap());
            assert_eq!(v.len(), 1, "{query}");
            assert_eq!(v[0].metric_name, None, "{query}");
        }
    }

    /// Issue #67: `last_over_time`/`first_over_time` KEEP `__name__` —
    /// upstream engine.go treats them like a time-shifted selector
    /// ("acts like offset; thus, it should keep the metric name"), pinned
    /// by the vendored `name_label_dropping.test:42-48`. This deviates
    /// from the plan's blanket drop-all-18 note deliberately: the #67
    /// `last_over_time` differential row compares full label sets against
    /// real Prometheus and would fail otherwise.
    #[test]
    fn last_and_first_over_time_keep_metric_name() {
        for (query, want) in [
            ("last_over_time(up[1m])", 3.0),
            ("first_over_time(up[1m])", 1.0),
        ] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, experimental(params(60_000, 60_000, 0))).unwrap();
            let mut data = SeriesData::new();
            data.insert(0, vec![series(1, &[], vec![s(1, 1.0), s(60_000, 3.0)])]);
            let v = instant_vector(evaluate(&p, &data).unwrap());
            assert_eq!(v.len(), 1, "{query}");
            assert_eq!(v[0].metric_name.as_deref(), Some("up"), "{query}");
            assert_eq!(v[0].v, want, "{query}");
        }
    }

    /// `predict_linear` end-to-end: the intercept anchors at the step
    /// time (samples at 1s/2s, step at 2s -> 23; see the functions.rs
    /// golden for the convention contrast).
    #[test]
    fn evaluates_predict_linear_at_the_step_time() {
        let expr = crate::parser::parse("predict_linear(up[1m], 10)").unwrap();
        let p = plan(&expr, params(2_000, 2_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[], vec![s(1_000, 1.0), s(2_000, 3.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].v, 23.0);
        assert_eq!(v[0].metric_name, None);
    }

    /// An out-of-range smoothing factor surfaces as
    /// `PromqlError::InvalidParameter` through `evaluate` (never a wrong
    /// value, never a panic).
    #[test]
    fn double_exponential_smoothing_invalid_factor_errors_through_evaluate() {
        let expr = crate::parser::parse("double_exponential_smoothing(up[1m], 2, 0.5)").unwrap();
        let p = plan(&expr, experimental(params(60_000, 60_000, 0))).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[], vec![s(1, 1.0), s(60_000, 3.0)])]);
        let err = evaluate(&p, &data).unwrap_err();
        assert!(
            matches!(err, PromqlError::InvalidParameter { .. }),
            "got {err:?}"
        );
    }

    /// #67 code review finding 2, pinned to real prom/prometheus:v3.13.0
    /// behavior (empirical: HTTP 200 + empty result in both shapes): an
    /// invalid smoothing/trend factor over a selection with **no windowed
    /// samples** — zero matched series, or matched series whose windows
    /// are empty at the step — succeeds with an empty vector, because the
    /// engine never invokes the function (and therefore never validates)
    /// for an empty selection. Only a selection with data errors (the
    /// test above).
    #[test]
    fn double_exponential_smoothing_invalid_factor_over_an_empty_selection_is_empty_not_error() {
        for (query, bad) in [
            ("double_exponential_smoothing(up[1m], 2, 0.5)", "sf"),
            ("double_exponential_smoothing(up[1m], 0.5, 2)", "tf"),
        ] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, experimental(params(60_000, 60_000, 0))).unwrap();
            // Zero matched series.
            let v = instant_vector(evaluate(&p, &SeriesData::new()).unwrap());
            assert!(v.is_empty(), "{query} ({bad}) zero-series: got {v:?}");
            // A matched series whose window is empty at the step (its
            // only sample lies far outside the 1m range window).
            let mut data = SeriesData::new();
            data.insert(0, vec![series(1, &[], vec![s(10_000_000, 1.0)])]);
            let v = instant_vector(evaluate(&p, &data).unwrap());
            assert!(v.is_empty(), "{query} ({bad}) empty-window: got {v:?}");
        }
    }

    /// `absent_over_time`: present window -> empty vector; absent window
    /// (or zero matched series) -> one synthetic series, value 1, labels
    /// from the equality matchers.
    #[test]
    fn absent_over_time_emits_one_series_only_when_every_window_is_empty() {
        let expr = crate::parser::parse(r#"absent_over_time(up{job="api"}[1m])"#).unwrap();
        // Present at 60s.
        let p = plan(&expr, params(60_000, 60_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "api")], vec![s(30_000, 1.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert!(v.is_empty(), "present window must be empty, got {v:?}");
        // Absent at 10 minutes (sample far outside the window).
        let p = plan(&expr, params(600_000, 600_000, 0)).unwrap();
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].v, 1.0);
        assert_eq!(v[0].metric_name, None, "absent labels never carry __name__");
        assert_eq!(
            v[0].labels,
            Labels::new(vec![("job".to_string(), "api".to_string())])
        );
        // Zero matched series behaves identically to empty windows.
        let v = instant_vector(evaluate(&p, &SeriesData::new()).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].v, 1.0);
    }

    /// The `createLabelsForAbsentFunction` has/delete rule (#67 Δ3):
    /// a label targeted by a duplicate equality matcher, or by an
    /// equality-then-regex pair, is dropped entirely; a regex-then-
    /// equality pair keeps the (first-seen-equality) value — order
    /// matters exactly as upstream; negations/regexes alone contribute
    /// nothing.
    #[test]
    fn absent_over_time_label_synthesis_ports_the_has_delete_rule() {
        for (query, want) in [
            // Duplicate equality -> dropped.
            (
                r#"absent_over_time(up{a="1",a="2"}[1m])"#,
                Labels::default(),
            ),
            // Equality then regex on the same name -> dropped.
            (
                r#"absent_over_time(up{a="1",a=~"x"}[1m])"#,
                Labels::default(),
            ),
            // Regex then equality -> the later first-seen equality wins
            // (upstream's delete lands before the set).
            (
                r#"absent_over_time(up{a=~"x",a="1"}[1m])"#,
                Labels::new(vec![("a".to_string(), "1".to_string())]),
            ),
            // Mixed with an untouched second label (the upstream
            // functions.test shape).
            (
                r#"absent_over_time(up{a="1",a="2",instance="127.0.0.1"}[1m])"#,
                Labels::new(vec![("instance".to_string(), "127.0.0.1".to_string())]),
            ),
            // Pure regex/negation matchers contribute no labels.
            (
                r#"absent_over_time(up{a=~"x",b!="y"}[1m])"#,
                Labels::default(),
            ),
        ] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, params(600_000, 600_000, 0)).unwrap();
            let v = instant_vector(evaluate(&p, &SeriesData::new()).unwrap());
            assert_eq!(v.len(), 1, "{query}");
            assert_eq!(v[0].labels, want, "{query}");
            assert_eq!(v[0].v, 1.0, "{query}");
        }
    }

    /// A range query evaluates absence per step: present steps contribute
    /// nothing, absent steps contribute 1 — the synthetic series appears
    /// only from the step where the window empties.
    #[test]
    fn absent_over_time_range_query_is_per_step() {
        let expr = crate::parser::parse("absent_over_time(up[1m])").unwrap();
        // Steps at 60s, 120s, 180s; the only sample is at 50s, so the
        // 1m window is non-empty at 60s only.
        let p = plan(&expr, params(60_000, 180_000, 60_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[], vec![s(50_000, 1.0)])]);
        match evaluate(&p, &data).unwrap() {
            QueryValue::Matrix(m) => {
                assert_eq!(m.len(), 1);
                assert!(m[0].labels.is_empty());
                assert_eq!(m[0].points, vec![(120_000, 1.0), (180_000, 1.0)]);
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
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

    // --- issue #65 (M6-02): elementwise math/trig + scalar functions ---

    /// Elementwise math **computes** a new value — DROP, the same class
    /// as `rate` (#37 keep/drop table). Mirrors `rate_drops_metric_name`.
    #[test]
    fn math_fn_drops_metric_name() {
        let expr = crate::parser::parse("abs(up)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, -3.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].metric_name, None);
        assert_eq!(v[0].v, 3.0);
    }

    #[test]
    fn clamp_applies_both_bounds_per_sample() {
        let expr = crate::parser::parse("clamp(up, -25, 75)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("l", "a")], vec![s(0, -50.0)]),
                series(2, &[("l", "b")], vec![s(0, 100.0)]),
            ],
        );
        let mut v = instant_vector(evaluate(&p, &data).unwrap());
        v.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert_eq!(v[0].v, -25.0);
        assert_eq!(v[1].v, 75.0);
    }

    /// Upstream funcClamp: `max < min` -> an **empty vector** for the
    /// step, not per-sample NaNs.
    #[test]
    fn clamp_with_max_below_min_returns_an_empty_vector() {
        let expr = crate::parser::parse("clamp(up, 5, -5)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("l", "a")], vec![s(0, 1.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert!(v.is_empty(), "clamp(x, 5, -5) must be empty, got {v:?}");
    }

    /// A NaN bound never trips the `max < min` empty branch (`NaN < x` is
    /// false) — it flows through go_min/go_max to a NaN result for every
    /// sample (plan v2 Δ2).
    #[test]
    fn clamp_with_a_nan_bound_yields_nan_samples_not_an_empty_vector() {
        for query in ["clamp(up, 0, NaN)", "clamp(up, NaN, 0)"] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, params(0, 0, 0)).unwrap();
            let mut data = SeriesData::new();
            data.insert(0, vec![series(1, &[("l", "a")], vec![s(0, 1.0)])]);
            let v = instant_vector(evaluate(&p, &data).unwrap());
            assert_eq!(v.len(), 1, "{query}: NaN bound must keep the sample");
            assert!(v[0].v.is_nan(), "{query}: expected NaN, got {}", v[0].v);
        }
    }

    #[test]
    fn round_uses_the_planned_default_to_nearest() {
        let expr = crate::parser::parse("round(up)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("l", "a")], vec![s(0, 2.5)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v[0].v, 3.0);
    }

    #[test]
    fn pi_evaluates_to_a_scalar() {
        let expr = crate::parser::parse("pi()").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let data = SeriesData::new();
        match evaluate(&p, &data).unwrap() {
            QueryValue::Scalar(v) => assert_eq!(v, std::f64::consts::PI),
            other => panic!("expected Scalar, got {other:?}"),
        }
    }

    #[test]
    fn max_of_and_min_of_evaluate_with_go_semantics() {
        let experimental = PlanParams {
            experimental_functions: true,
            ..params(0, 0, 0)
        };
        for (query, want) in [("max_of(1, 2)", 2.0), ("min_of(1, 2)", 1.0)] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, experimental).unwrap();
            match evaluate(&p, &SeriesData::new()).unwrap() {
                QueryValue::Scalar(v) => assert_eq!(v, want, "{query}"),
                other => panic!("{query}: expected Scalar, got {other:?}"),
            }
        }
        // Go's Inf-before-NaN precedence survives end-to-end.
        let expr = crate::parser::parse("max_of(Inf, NaN)").unwrap();
        let p = plan(&expr, experimental).unwrap();
        match evaluate(&p, &SeriesData::new()).unwrap() {
            QueryValue::Scalar(v) => assert_eq!(v, f64::INFINITY),
            other => panic!("expected Scalar, got {other:?}"),
        }
    }

    /// A math fn over a range query produces per-step points like every
    /// other vector expression.
    #[test]
    fn math_fn_range_query_maps_every_step() {
        let expr = crate::parser::parse("abs(up)").unwrap();
        let p = plan(&expr, params(0, 60_000, 60_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(1, &[("l", "a")], vec![s(0, -1.0), s(60_000, -2.0)])],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Matrix(m) => {
                assert_eq!(m.len(), 1);
                assert_eq!(m[0].metric_name, None);
                assert_eq!(m[0].points, vec![(0, 1.0), (60_000, 2.0)]);
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
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

    // --- issue #68 (M6-05): label, sort & absence functions ---

    /// `sort`/`sort_desc` are pass-throughs of existing series — KEEP
    /// `__name__`, reorder by value with NaN last in BOTH directions
    /// (functions.test:703,715), end-to-end through `evaluate`.
    #[test]
    fn sort_and_sort_desc_keep_metric_name_and_put_nan_last_both_directions() {
        for (query, want) in [
            ("sort(up)", vec!["c", "b", "a"]),
            ("sort_desc(up)", vec!["b", "c", "a"]),
        ] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, params(0, 0, 0)).unwrap();
            let mut data = SeriesData::new();
            data.insert(
                0,
                vec![
                    series(1, &[("i", "a")], vec![s(0, f64::NAN)]),
                    series(2, &[("i", "b")], vec![s(0, 3.0)]),
                    series(3, &[("i", "c")], vec![s(0, 1.0)]),
                ],
            );
            let v = instant_vector(evaluate(&p, &data).unwrap());
            let order: Vec<&str> = v.iter().map(|s| s.labels.get("i").unwrap()).collect();
            assert_eq!(order, want, "{query}");
            assert!(
                v.iter().all(|s| s.metric_name.as_deref() == Some("up")),
                "{query}: sort passes existing series through — __name__ kept"
            );
        }
    }

    /// `label_replace`/`label_join` keep the (possibly rewritten) joint
    /// `(metric_name, Labels)` identity end-to-end.
    #[test]
    fn label_replace_rewrites_labels_and_keeps_metric_name_end_to_end() {
        let expr =
            crate::parser::parse(r#"label_replace(up, "dst", "value-$1", "src", "source-(.*)")"#)
                .unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("src", "source-10")], vec![s(0, 1.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].labels.get("dst"), Some("value-10"));
        assert_eq!(v[0].metric_name.as_deref(), Some("up"));
    }

    /// AC5: `absent()` and `absent_over_time()` share the exact
    /// `labels_for_absent` synthesis (`eval::labels` — both arms call the
    /// one helper): for every matcher shape the two functions synthesize
    /// identical labels over an absent selection.
    #[test]
    fn absent_and_absent_over_time_share_the_label_synthesis() {
        for matchers in [
            r#"{job="testjob", instance="testinstance", method=~".x"}"#,
            r#"{a="1",a="2",instance="127.0.0.1"}"#,
            r#"{a=~"x",a="1"}"#,
            "",
        ] {
            let instant = crate::parser::parse(&format!("absent(nonexistent{matchers})")).unwrap();
            let over_time =
                crate::parser::parse(&format!("absent_over_time(nonexistent{matchers}[1m])"))
                    .unwrap();
            let p_i = plan(&instant, params(0, 0, 0)).unwrap();
            let p_o = plan(&over_time, params(0, 0, 0)).unwrap();
            let v_i = instant_vector(evaluate(&p_i, &SeriesData::new()).unwrap());
            let v_o = instant_vector(evaluate(&p_o, &SeriesData::new()).unwrap());
            assert_eq!(v_i.len(), 1, "{matchers}");
            assert_eq!(v_o.len(), 1, "{matchers}");
            assert_eq!(v_i[0].labels, v_o[0].labels, "{matchers}");
            assert_eq!(v_i[0].metric_name, None, "{matchers}");
            assert_eq!(v_i[0].v, 1.0, "{matchers}");
        }
    }

    /// `absent()` over a present selection (or a non-empty computed
    /// vector) is empty; a computed argument synthesizes the EMPTY label
    /// set when absent (functions.test:1725-1750).
    #[test]
    fn absent_is_empty_when_present_and_empty_labeled_for_computed_arguments() {
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, 1.0)])]);
        for query in ["absent(up)", "absent(sum(up))"] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, params(0, 0, 0)).unwrap();
            let v = instant_vector(evaluate(&p, &data).unwrap());
            assert!(v.is_empty(), "{query}: present -> empty, got {v:?}");
        }
        for query in [
            r#"absent(sum(nonexistent{job="testjob"}))"#,
            "absent(nonexistent > 1)",
            "absent(a + b)",
            "absent(rate(nonexistent[5m]))",
        ] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, params(0, 0, 0)).unwrap();
            let v = instant_vector(evaluate(&p, &SeriesData::new()).unwrap());
            assert_eq!(v.len(), 1, "{query}");
            assert!(v[0].labels.is_empty(), "{query}: computed arg -> {{}}");
            assert_eq!(v[0].v, 1.0, "{query}");
        }
    }

    /// The Δ5(c) contamination regression golden, reachable today: a
    /// nested `label_replace` first writes `__name__` from a
    /// distinguishing label, then drops that label — two metric names
    /// sharing one non-name label set. The full-identity range
    /// accumulator must keep them as TWO separate output series with
    /// their own point sequences, at overlapping and at disjoint step
    /// times (AC12(iii)).
    #[test]
    fn range_query_keeps_distinct_metric_names_with_equal_non_name_labels_separate() {
        let query = r#"label_replace(label_replace(m, "__name__", "$1", "kind", "(.*)"), "kind", "", "kind", ".*")"#;
        let expr = crate::parser::parse(query).unwrap();
        let p = plan(&expr, params(0, 600_000, 300_000)).unwrap();
        // Overlapping: both series present at every step.
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(
                    1,
                    &[("kind", "foo"), ("shared", "x")],
                    vec![s(0, 1.0), s(300_000, 2.0), s(600_000, 3.0)],
                ),
                series(
                    2,
                    &[("kind", "bar"), ("shared", "x")],
                    vec![s(0, 4.0), s(300_000, 5.0), s(600_000, 6.0)],
                ),
            ],
        );
        let QueryValue::Matrix(m) = evaluate(&p, &data).unwrap() else {
            panic!("expected Matrix");
        };
        assert_eq!(m.len(), 2, "two full identities, never merged: {m:?}");
        let by_name = |name: &str| {
            m.iter()
                .find(|s| s.metric_name.as_deref() == Some(name))
                .unwrap_or_else(|| panic!("missing series {name}: {m:?}"))
        };
        assert_eq!(
            by_name("foo").points,
            vec![(0, 1.0), (300_000, 2.0), (600_000, 3.0)]
        );
        assert_eq!(
            by_name("bar").points,
            vec![(0, 4.0), (300_000, 5.0), (600_000, 6.0)]
        );
        assert!(m.iter().all(|s| s.labels.get("shared") == Some("x")));
        // Disjoint: the two series never coexist at a step — still two
        // separate series (same full-identity key, different names).
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("kind", "foo"), ("shared", "x")], vec![s(0, 1.0)]),
                series(
                    2,
                    &[("kind", "bar"), ("shared", "x")],
                    vec![s(600_000, 6.0)],
                ),
            ],
        );
        let QueryValue::Matrix(m) = evaluate(&p, &data).unwrap() else {
            panic!("expected Matrix");
        };
        assert_eq!(
            m.len(),
            2,
            "disjoint steps must not merge across names: {m:?}"
        );
    }

    /// Trip-proof for the Δ5 rekey (plan v3: "locking in that the fix is
    /// load-bearing, not decorative"): the SAME per-step vectors run
    /// through a replica of the pre-fix `Labels`-only accumulator (its
    /// `debug_assert_eq!` included) panic in debug builds — and silently
    /// merge two metric names into one group in release builds — exactly
    /// the contamination the `(Option<metric_name>, Labels)` key removes.
    #[test]
    fn a_labels_only_range_key_would_collapse_distinct_metric_names() {
        let query = r#"label_replace(label_replace(m, "__name__", "$1", "kind", "(.*)"), "kind", "", "kind", ".*")"#;
        let expr = crate::parser::parse(query).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("kind", "foo"), ("shared", "x")], vec![s(0, 1.0)]),
                series(2, &[("kind", "bar"), ("shared", "x")], vec![s(0, 4.0)]),
            ],
        );
        // The pre-fix accumulator, verbatim in shape: keyed by `Labels`
        // alone, `metric_name` captured from whichever step first
        // populates the group, guarded by the old debug_assert.
        type PreFixGroupAcc = (Option<String>, Vec<(i64, f64)>);
        let replica = || {
            let mut acc: HashMap<Labels, PreFixGroupAcc> = HashMap::new();
            let p = plan(&expr, params(0, 0, 0)).unwrap();
            let v = instant_vector(evaluate(&p, &data).unwrap());
            for sample in v {
                match acc.entry(sample.labels) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        debug_assert_eq!(
                            e.get().0,
                            sample.metric_name,
                            "issue #37 invariant: every step of one output series (same \
                             non-name Labels) must agree on metric_name"
                        );
                        e.get_mut().1.push((sample.t_ms, sample.v));
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert((sample.metric_name, Vec::new()))
                            .1
                            .push((sample.t_ms, sample.v));
                    }
                }
            }
            acc
        };
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(replica));
        if cfg!(debug_assertions) {
            assert!(
                outcome.is_err(),
                "the pre-fix Labels-only key must trip its debug_assert on this input"
            );
        } else {
            let acc = outcome.expect("no debug_assert in release");
            assert_eq!(
                acc.len(),
                1,
                "release-mode pre-fix key silently merges two metric names into one group"
            );
        }
    }

    /// AC12(i)/(ii) over the full identity: same `(metric_name, Labels)`
    /// at disjoint timestamps merges into ONE output series; the same
    /// identity coexisting at a shared step errors with the upstream
    /// duplicate-labelset message.
    #[test]
    fn range_rewrites_merge_disjoint_identities_and_error_on_overlap() {
        let query = r#"label_replace(m, "kind", "", "kind", ".*")"#;
        let expr = crate::parser::parse(query).unwrap();
        let p = plan(&expr, params(0, 600_000, 300_000)).unwrap();
        // Disjoint: series one only at t=0, series two only at t=600s.
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("kind", "one"), ("shared", "x")], vec![s(0, 7.0)]),
                series(
                    2,
                    &[("kind", "two"), ("shared", "x")],
                    vec![s(600_000, 9.0)],
                ),
            ],
        );
        let QueryValue::Matrix(m) = evaluate(&p, &data).unwrap() else {
            panic!("expected Matrix");
        };
        assert_eq!(m.len(), 1, "disjoint same-identity rewrites merge: {m:?}");
        assert_eq!(m[0].points, vec![(0, 7.0), (600_000, 9.0)]);
        assert_eq!(m[0].metric_name.as_deref(), Some("m"));
        // Overlap: both present at t=0 — the per-step duplicate check
        // fails the whole query.
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("kind", "one"), ("shared", "x")], vec![s(0, 1.0)]),
                series(2, &[("kind", "two"), ("shared", "x")], vec![s(0, 2.0)]),
            ],
        );
        let err = evaluate(&p, &data).unwrap_err();
        assert!(matches!(err, PromqlError::LabelSet { .. }), "{err:?}");
        assert_eq!(
            err.to_string(),
            "vector cannot contain metrics with the same labelset"
        );
    }
}
