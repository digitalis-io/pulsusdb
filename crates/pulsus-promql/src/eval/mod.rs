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
pub mod extended;
pub mod functions;
pub mod hist_range_fns;
pub mod histogram_fns;
pub(crate) mod info;
pub mod labels;
pub mod quote;
pub mod staleness;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::annotations::Annotations;
use crate::error::PromqlError;
use crate::plan::{
    AggOp, HistogramAccessorFn, MathFn, OverTimeFn, OverTimeParamFn, PlanExpr, QueryPlan,
    RangeSource, ScalarFn, SelectorId, SelectorSpec, SubqueryPlan,
};
use crate::value::{
    FetchedSeries, InstantSample, Labels, Point, QueryValue, RangeSeries, Sample, SeriesData,
};

/// The evaluator's view over the fetched data (issue #125): a selector
/// flagged [`SelectorSpec::histogram_stats`] reads its STATS-REDUCED copy
/// (buckets dropped, counter-reset hints synthesized —
/// [`reduce_histogram_stats_samples`]); every other selector — and every
/// query with no flagged selector at all — borrows the caller's
/// [`SeriesData`] untouched (zero cost for ordinary queries: `stats` is an
/// empty map and `get` falls straight through).
struct EvalData<'a> {
    base: &'a SeriesData,
    stats: HashMap<SelectorId, Vec<FetchedSeries>>,
}

impl<'a> EvalData<'a> {
    fn new(selectors: &[SelectorSpec], data: &'a SeriesData) -> Self {
        let mut stats = HashMap::new();
        for spec in selectors {
            if spec.histogram_stats {
                stats.insert(
                    spec.id,
                    data.get(spec.id)
                        .iter()
                        .map(|fs| FetchedSeries {
                            fingerprint: fs.fingerprint,
                            metric_name: fs.metric_name.clone(),
                            labels: fs.labels.clone(),
                            samples: reduce_histogram_stats_samples(&fs.samples),
                            // Issue #155: the stats reduction is 1:1 per
                            // sample, so the aligned ST channel (if any)
                            // carries over unchanged.
                            start_ts: fs.start_ts.clone(),
                        })
                        .collect(),
                );
            }
        }
        EvalData { base: data, stats }
    }

    fn get(&self, id: SelectorId) -> &[FetchedSeries] {
        match self.stats.get(&id) {
            Some(v) => v.as_slice(),
            None => self.base.get(id),
        }
    }
}

/// The stats-decoding synthesis pass (issue #125) — one ts-order walk per
/// series, porting the pin's `HistogramStatsIterator.AtFloatHistogram` +
/// `getResetHint` (`promql/histogram_stats_iterator.go:91-168`):
///
/// - a REAL histogram sample emits the reduced `{schema, count, sum,
///   counter_reset_hint}` shape (buckets/zero-bucket/`custom_values`
///   dropped — the pin's `populateFH` literal keeps exactly those four
///   fields), with the hint resolved as: the STORED hint when ≠ Unknown;
///   else Unknown when no previous full histogram is retained; else
///   `detect_reset(curr_full, prev_full)` ⇒ CounterReset/NotCounterReset —
///   detection runs on the FULL histograms (`hsi.current.DetectReset(hsi.
///   last)`), never the reduced ones. The retained `prev` then becomes
///   this sample's full histogram (`setLastFromCurrent`).
/// - a STALE histogram sample is emitted unchanged and `prev` is NOT
///   mutated — neither updated nor cleared (the pin's `IsStaleNaN(sum)`
///   early return skips `setLastFromCurrent`, PRESERVING `hsi.last`
///   across the gap, so the next real sample's Unknown still resolves
///   against the pre-stale histogram — plan v3 Δ1, AC9).
/// - a float sample (stale markers included) passes through unchanged and
///   likewise never touches `prev` (floats never route through
///   `AtFloatHistogram`).
fn reduce_histogram_stats_samples(samples: &[Sample]) -> Vec<Sample> {
    use pulsus_model::{CounterResetHint, FloatHistogram};
    let mut prev: Option<&FloatHistogram> = None;
    let mut out = Vec::with_capacity(samples.len());
    for s in samples {
        match &s.h {
            Some(h) if !s.is_stale() => {
                let hint = if h.counter_reset_hint != CounterResetHint::Unknown {
                    h.counter_reset_hint
                } else {
                    match prev {
                        None => CounterResetHint::Unknown,
                        Some(p) => {
                            if h.detect_reset(p) {
                                CounterResetHint::CounterReset
                            } else {
                                CounterResetHint::NotCounterReset
                            }
                        }
                    }
                };
                out.push(Sample::hist(
                    s.t_ms,
                    FloatHistogram {
                        counter_reset_hint: hint,
                        schema: h.schema,
                        zero_threshold: 0.0,
                        zero_count: 0.0,
                        count: h.count,
                        sum: h.sum,
                        positive_spans: Vec::new(),
                        negative_spans: Vec::new(),
                        positive_buckets: Vec::new(),
                        negative_buckets: Vec::new(),
                        custom_values: Vec::new(),
                    },
                ));
                prev = Some(h);
            }
            // Stale (histogram OR float) and plain-float samples: emitted
            // unchanged, `prev` untouched.
            _ => out.push(s.clone()),
        }
    }
    out
}

/// One step's evaluated value — collapsed into [`QueryValue`] once the
/// whole query (instant, or every range-query step) has been evaluated.
/// `String` (issue #86, M6-08d) only ever appears at the plan ROOT
/// (`plan` lifts top-level string literals and rejects string-typed range
/// queries), so no nested `eval_step` arm ever sees it.
#[derive(Debug, Clone)]
enum StepValue {
    Vector(Vec<InstantSample>),
    Scalar(f64),
    String(String),
}

/// The FULL upstream series identity (plan v3 Δ5): the kept metric name
/// (`None` for name-dropping constructs) alongside the non-name label
/// set — upstream hashes the complete label set, `__name__` included.
type SeriesIdentity = (Option<String>, Labels);

/// Cooperative cancellation, checked at eval-loop checkpoints (issue #93):
/// once per range step, once per subquery inner-grid point, and before the
/// single instant `eval_step`. `None` (via [`CancelToken::never`]) means
/// "never cancelled" — the shape [`evaluate`] uses, so every existing
/// caller (corpus, benches, count-gates) observes byte-identical behavior.
/// Carried by value on [`EvalCaches`], not a borrow: the flag is a fresh
/// per-call `Arc` owned by the read path, cheap to clone.
#[derive(Clone, Debug, Default)]
pub struct CancelToken(Option<Arc<AtomicBool>>);

impl CancelToken {
    /// Wraps a caller-owned flag: the read path sets it when the awaiting
    /// request future is dropped (client disconnect / timeout).
    pub fn new(flag: Arc<AtomicBool>) -> Self {
        Self(Some(flag))
    }

    /// A token that never fires — [`evaluate`]'s wrapper shape.
    pub const fn never() -> Self {
        Self(None)
    }

    /// `Relaxed` is sufficient: per-location atomic coherence guarantees
    /// the blocking thread eventually observes the reactor thread's store;
    /// no cross-location ordering is needed (matches [`crate`]'s sibling
    /// `EvalGate` gauges). `None` (never-cancelled) short-circuits to
    /// `false` without touching an atomic.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.0.as_deref().is_some_and(|f| f.load(Ordering::Relaxed))
    }
}

/// Evaluates `plan` against `data` — pure, no I/O. Returns the value
/// alongside the accumulated [`Annotations`] (M7-A5b-i): a generic sink —
/// empty for every float-only query (byte-identical to the pre-A5b-i
/// behavior) — that native-histogram arms populate (`histogram_quantile`'s
/// out-of-range φ, NaN-observation info, …).
pub fn evaluate(
    plan: &QueryPlan,
    data: &SeriesData,
) -> Result<(QueryValue, Annotations), PromqlError> {
    evaluate_cancellable(plan, data, CancelToken::never())
}

/// [`evaluate`] with a live cancellation token (issue #93): the offloaded
/// read path passes a token that fires when the awaiting request future is
/// dropped (client disconnect / `TimeoutLayer` 408), so a still-running
/// `spawn_blocking` eval bails at its next checkpoint instead of burning a
/// full evaluation for a caller that is already gone.
pub fn evaluate_cancellable(
    plan: &QueryPlan,
    data: &SeriesData,
    cancel: CancelToken,
) -> Result<(QueryValue, Annotations), PromqlError> {
    evaluate_counted_with(plan, data, cancel)
        .map(|(value, _counts, annotations)| (value, annotations))
}

/// The evaluation-count observables [`evaluate_counted`] returns
/// alongside the value — each one a Tier-1 (scale-invariant) perf gate's
/// hook, never a wall-time assert.
#[derive(Debug, Default, Clone, Copy)]
struct EvalCounts {
    /// Subquery inner-grid evaluations (issue #83 round-2 gate): a
    /// per-outer-step re-evaluation implementation counts a multiple of
    /// the union-grid size and fails
    /// `tests::subqueries_materialize_once_over_the_union_grid`.
    inner_evals: u64,
    /// Genuine evaluations of MARKED step-invariant roots (issue #88):
    /// [`eval_step`] increments whenever a marked node is actually
    /// evaluated rather than served from cache — which happens exactly
    /// once, during [`prepare_step_invariant`] (it marks before
    /// evaluating, so its own single evaluation is counted through the
    /// same instrument). Per-step cache HITS return early and never
    /// count, so the `== 1` Tier-1 gate
    /// (`tests::step_invariant_roots_evaluate_once_across_a_range`)
    /// catches BOTH regression shapes (code review round 1, finding 1):
    /// a prep pass that recomputes per step, and a stepping phase that
    /// bypasses the cache and re-evaluates the marked subtree (`1 +
    /// steps` either way).
    step_invariant_evals: u64,
    /// Times [`finalize_metadata_labels`]'s `Matrix` arm ran its full
    /// clone + `HashMap` merge machinery (issue #93, finding 3 — the
    /// M6-08 reclaim gate). When NO element of the assembled matrix is
    /// `drop_name`, that pass is provably a no-op (the range accumulator
    /// already deduped on the identical `(metric_name, Labels)` identity
    /// and pre-sorted, and both metadata strips are guarded on
    /// `drop_name`), so it short-circuits WITHOUT incrementing this — a
    /// `drop_name`-free range MUST count 0
    /// (`tests::finalize_skips_the_matrix_merge_pass_when_no_series_is_drop_marked`),
    /// a `drop_name` case MUST count > 0.
    finalize_matrix_merge_passes: u64,
}

/// [`evaluate`] plus [`EvalCounts`] and the drained [`Annotations`] sink —
/// the never-cancelled shape used by every existing caller (corpus, count-
/// gate tests, benches).
#[cfg(test)]
fn evaluate_counted(
    plan: &QueryPlan,
    data: &SeriesData,
) -> Result<(QueryValue, EvalCounts, Annotations), PromqlError> {
    evaluate_counted_with(plan, data, CancelToken::never())
}

/// [`evaluate_counted`] with an explicit [`CancelToken`] (issue #93).
fn evaluate_counted_with(
    plan: &QueryPlan,
    data: &SeriesData,
    cancel: CancelToken,
) -> Result<(QueryValue, EvalCounts, Annotations), PromqlError> {
    let p = &plan.params;

    // Issue #125: build the per-flagged-selector stats-reduced view ONCE,
    // before any stepping. Unflagged selectors (and every query with no
    // `histogram_stats` flag at all) borrow `data` untouched.
    let eval_data = EvalData::new(&plan.selectors, data);
    let data = &eval_data;

    // Issue #83 (round-2 amendment): materialize every subquery ONCE over
    // its epoch-anchored union grid — inside-out for nested subqueries —
    // before any stepping; each step below only slices `(mint, maxt]`
    // windows from the shared results. Issue #82 rides the same pass:
    // every `info()` node's arg0 horizon is walked once here to build
    // its horizon-wide identifying-label narrowing (`prepare_info`).
    let mut caches = EvalCaches {
        cancel,
        ..EvalCaches::default()
    };
    let mut counts = EvalCounts::default();
    // The classifier instance is per-call, like the caches (Δ4). It is
    // threaded through `prepare_subqueries` too (issue #95): each
    // subquery's invariant inner subtrees are frozen once at that
    // subquery's own grid start, from inside `prepare_subquery`.
    let mut classifier = crate::plan::StepInvariance::new(&plan.selectors);
    prepare_subqueries(
        &plan.root,
        &plan.selectors,
        data,
        StepGrid::Dense(Horizon {
            start_ms: p.start_ms,
            end_ms: p.end_ms,
            step_ms: p.step_ms,
        }),
        p.lookback_ms,
        &mut caches,
        &mut counts.inner_evals,
        &mut classifier,
    )?;

    // Issue #88: freeze every highest step-invariant subtree — evaluated
    // ONCE at the range start, the per-step loop below returns the cached
    // clone (upstream `StepInvariantExpr`'s once-and-copy). AFTER
    // `prepare_subqueries`, necessarily: a root over an `@`-anchored
    // subquery evaluates against the already-materialized union grid.
    prepare_step_invariant(
        &plan.root,
        &plan.selectors,
        data,
        p.start_ms,
        p.lookback_ms,
        &mut caches,
        &mut classifier,
    )?;

    if p.step_ms == 0 {
        // Issue #93: a single instant eval has no per-step loop to
        // checkpoint inside, so the cancellation check sits just before it.
        if caches.cancel.is_cancelled() {
            return Err(PromqlError::Cancelled);
        }
        let value = match eval_step(
            &plan.root,
            &plan.selectors,
            data,
            p.start_ms,
            p.lookback_ms,
            &caches,
        )? {
            StepValue::Vector(v) => QueryValue::Vector(v),
            StepValue::Scalar(s) => QueryValue::Scalar(s),
            StepValue::String(s) => QueryValue::String(s),
        };
        counts.step_invariant_evals = caches.step_invariant_evals.get();
        // Issue #130 Δ2: emit the horizon-wide limit_ratio cap warnings
        // BEFORE the annotations drain below.
        flush_ratio_warnings(&caches);
        let value = finalize_metadata_labels(value, &mut counts.finalize_matrix_merge_passes)?;
        return Ok((value, counts, caches.annotations.take()));
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
    //
    // Issue #86 (plan v2 Δ1): `drop_name` is deliberately NOT part of the
    // identity key — it is LATCHED at the identity's first evaluated step
    // and never updated (upstream `rangeEval`, engine.go ~:1556-1565: the
    // `seriess[h]` else-branch sets `DropName` at series creation; the
    // `ok` append-branch never touches it). `(m > 0) or (m + 1)`
    // legitimately alternates the per-step verdict for ONE identity (the
    // filter comparison keeps, the arithmetic drops), and upstream's
    // answer is whichever branch produced the first step. Folding
    // `drop_name` into the key instead would split that series into a
    // kept half and a dropped half with DIFFERENT post-strip labelsets —
    // two output series where upstream has one.
    let mut vector_points: HashMap<SeriesIdentity, (bool, Vec<Point>)> = HashMap::new();
    let mut scalar_points: Vec<Point> = Vec::new();
    let mut saw_vector = false;
    let mut saw_scalar = false;

    let mut t = p.start_ms;
    while t <= p.end_ms {
        // Issue #93: one `Relaxed` load per step — O(1), no allocation, no
        // per-sample work inside `eval_step`.
        if caches.cancel.is_cancelled() {
            return Err(PromqlError::Cancelled);
        }
        match eval_step(&plan.root, &plan.selectors, data, t, p.lookback_ms, &caches)? {
            StepValue::Vector(v) => {
                saw_vector = true;
                for s in v {
                    let InstantSample {
                        labels,
                        metric_name,
                        drop_name,
                        t_ms: _,
                        v: value,
                        h,
                    } = s;
                    vector_points
                        .entry((metric_name, labels))
                        // First step wins (the latch): later inserts only
                        // append points.
                        .or_insert_with(|| (drop_name, Vec::new()))
                        .1
                        // M7-A5a: the histogram channel rides the range
                        // materialization step point (the second selection
                        // site) verbatim; float steps carry `h: None`.
                        .push(Point {
                            t_ms: t,
                            v: value,
                            h,
                        });
                }
            }
            StepValue::Scalar(v) => {
                saw_scalar = true;
                scalar_points.push(Point::float(t, v));
            }
            // Unreachable through `plan()`: string-typed range queries
            // are rejected at plan time (defense in depth, the scalar-
            // subquery precedent).
            StepValue::String(_) => {
                return Err(PromqlError::Unsupported {
                    construct: "string literal in a range query".to_string(),
                });
            }
        }
        t += p.step_ms;
    }

    // Issue #130 Δ2: after the step loop, before EITHER range return's
    // annotations drain (scalar-only below, vector at the tail).
    flush_ratio_warnings(&caches);

    counts.step_invariant_evals = caches.step_invariant_evals.get();
    if saw_scalar && !saw_vector {
        return Ok((
            QueryValue::Matrix(vec![RangeSeries {
                labels: Labels::default(),
                metric_name: None,
                drop_name: false,
                points: scalar_points,
            }]),
            counts,
            caches.annotations.take(),
        ));
    }

    let mut out: Vec<RangeSeries> = vector_points
        .into_iter()
        .map(|((metric_name, labels), (drop_name, points))| RangeSeries {
            labels,
            metric_name,
            drop_name,
            points,
        })
        .collect();
    // `(labels, metric_name)` tie-break (plan v3 Δ5): the wire order is
    // the encoder's own matrix label-sort either way — this only pins
    // internal determinism when two full identities share their non-name
    // labels.
    out.sort_by(|a, b| (&a.labels, &a.metric_name).cmp(&(&b.labels, &b.metric_name)));
    let value = finalize_metadata_labels(
        QueryValue::Matrix(out),
        &mut counts.finalize_matrix_merge_passes,
    )?;
    Ok((value, counts, caches.annotations.take()))
}

/// Issue #130 Δ2: the once-per-query `limit_ratio` cap-warning emission —
/// the port of upstream's pre-step-loop extrema checks
/// (`engine.go:1655-1660` at the pin: `params.Max() > 1.0` ⇒ ONE
/// `NewInvalidRatioWarning(Max(), 1.0)`, `params.Min() < -1.0` ⇒ ONE
/// `…(Min(), -1.0)`; the per-step loop never warns). Called at exactly
/// two sites in [`evaluate_counted_with`] — after the instant eval and
/// after the range step loop — both before their annotations drain.
/// Error paths never reach a call site, matching upstream (its NaN error
/// precedes warn emission, and an `Err` return discards annotations).
/// Two nodes with identical extrema collapse to one message via
/// [`Annotations`]' exact-text dedup — same as upstream's message-keyed
/// map. Deliberately NOT ported (per-step outcome-identical for
/// everything the corpus pins): the all-zero `LIMIT_RATIO` early return
/// (`r = 0` selects nothing per step anyway) and `LIMITK`'s horizon-wide
/// `Max() < 1` / int64-overflow checks.
fn flush_ratio_warnings(caches: &EvalCaches) {
    let extrema = caches.ratio_extrema.borrow();
    let mut annos = caches.annotations.borrow_mut();
    for (_, e) in extrema.iter() {
        if e.max > 1.0 {
            annos.warning(crate::annotations::messages::invalid_ratio_warning(
                e.max, 1.0,
            ));
        }
        if e.min < -1.0 {
            annos.warning(crate::annotations::messages::invalid_ratio_warning(
                e.min, -1.0,
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Terminal metadata-label cleanup (issue #85, M6-08c — PROM-39)
// ---------------------------------------------------------------------------

/// The reserved metadata label names upstream `schema.IsMetadataLabel`
/// covers alongside `__name__` (`schema/labels.go` at the pinned v3.13.0
/// SHA). `__name__` itself is carried outside [`Labels`] by construction
/// (`InstantSample::metric_name`), so only these two ever appear in a
/// label vector. `pub(crate)` since issue #86: [`binop::emit_pair`] ports
/// upstream `resultMetric`'s IMMEDIATE metadata deletion for
/// vector-vector arithmetic (the one metadata drop that is unconditional
/// even under delayed removal — `schema.Metadata{}.SetToLabels` runs
/// whenever `changesMetricSchema(op)`, engine.go:3349).
pub(crate) const METADATA_LABEL_KEYS: [&str; 2] = ["__type__", "__unit__"];

/// Removes `__type__`/`__unit__` from `labels` iff the series' name is
/// dropped (`drop_name == true` — issue #86, M6-08d: the explicit delayed
/// verdict replacing 08c's `metric_name == None` proxy) — the PROM-39
/// metadata drop, unified with name removal exactly like upstream's
/// terminal `cleanupMetricLabels` (`engine.go:4224`):
/// `DropReserved(schema.IsMetadataLabel)` for every series with
/// `DropName` set. Keying on `drop_name` fixes the 08c residual: a
/// `label_replace` that DELETES `__name__` (an explicit write, not a
/// drop) now leaves `__type__`/`__unit__` intact, matching upstream.
///
/// **Timing is root-only, deliberately (plan v3 Δ1, adjudicated; #86
/// makes the delayed model the engine's sole one):** the corpus oracle
/// runs upstream with `EnableDelayedNameRemoval: true`
/// (`promql/promqltest/test.go:111`), under which every per-node
/// `DropReserved` is guarded OFF and the only metadata drop is this
/// terminal one (the single upstream exception — vector-vector
/// arithmetic's `resultMetric` deletion — is ported at its own site,
/// `binop::emit_pair`). Mid-tree, `__type__`/`__unit__` stay in `Labels`
/// so vector matching/grouping see the full signature — per-node
/// stripping would provably break `type_and_unit.test:77/92/222/237`
/// (the set-op signature excludes only `__name__`, `engine.go:1471`).
/// The e2e differential oracle runs with
/// `--enable-feature=promql-delayed-name-removal` so live parity is
/// validated under matching flag state (#86 adjudication).
fn strip_metadata_labels(drop_name: bool, labels: &mut Labels) {
    if drop_name {
        labels
            .0
            .retain(|(k, _)| !METADATA_LABEL_KEYS.contains(&k.as_str()));
    }
}

/// The terminal `cleanupMetricLabels`-equivalent (issue #85 plan v4 Δ1;
/// re-keyed on `drop_name` by issue #86), applied once to the final
/// assembled [`QueryValue`]: for every `drop_name == true` element, strip
/// metadata labels per [`strip_metadata_labels`] AND null the retained
/// `metric_name` (the load-bearing named contract — `evaluate()`'s final
/// output carries `None` for dropped series, so the wire consumer
/// `with_metric_name` and the corpus judge never see a retained
/// to-be-dropped name), then resolve post-drop identity collisions with
/// pinned upstream semantics —
///
/// - **instant vector:** a duplicate `(metric_name, Labels)` identity is a
///   hard error, `vector cannot contain metrics with the same labelset`
///   (upstream `vec.ContainsSameLabelset()` → `errorf`, engine.go:4238);
/// - **range matrix:** series sharing a post-drop identity **merge** when
///   their timestamps are disjoint, and error with the same message when
///   any timestamp overlaps (upstream `mergeSeriesWithSameLabelset`,
///   engine.go:4252);
/// - **string/scalar:** passthrough.
fn finalize_metadata_labels(
    value: QueryValue,
    merge_passes: &mut u64,
) -> Result<QueryValue, PromqlError> {
    fn same_labelset_error() -> PromqlError {
        PromqlError::LabelSet {
            detail: "vector cannot contain metrics with the same labelset".to_string(),
        }
    }

    match value {
        QueryValue::Scalar(_) | QueryValue::String(_) => Ok(value),
        QueryValue::Vector(mut v) => {
            for s in &mut v {
                strip_metadata_labels(s.drop_name, &mut s.labels);
                if s.drop_name {
                    s.metric_name = None;
                }
            }
            let mut seen: std::collections::HashSet<(Option<&str>, &Labels)> =
                std::collections::HashSet::with_capacity(v.len());
            for s in &v {
                if !seen.insert((s.metric_name.as_deref(), &s.labels)) {
                    return Err(same_labelset_error());
                }
            }
            Ok(QueryValue::Vector(v))
        }
        QueryValue::Matrix(m) => {
            // Issue #93 (finding 3 — M6-08 reclaim): short-circuit when
            // NO element is drop-marked. The range accumulator
            // (`evaluate_counted`, above) already deduped the matrix on
            // the EXACT `(metric_name, Labels)` identity this arm merges
            // on, and pre-sorted it. With no `drop_name`,
            // `strip_metadata_labels` and the `metric_name = None` write
            // are BOTH no-ops (each is guarded on `drop_name`), so every
            // post-strip identity equals the accumulator's already-unique,
            // already-sorted key: no merge is reachable, no same-labelset
            // collision is reachable, and the re-sort would re-establish
            // an order that already holds. Returning `m` verbatim is
            // therefore provably outcome-identical to running the full
            // pass — and it skips a per-series clone + `HashMap` build.
            // The `finalize_matrix_merge_passes` counter stays 0 (the
            // Tier-1 reclaim gate); a `drop_name` matrix falls through and
            // increments it. Corpus `expect fail` same-labelset cases
            // exercise the `drop_name` branch, so they never short-circuit.
            if m.iter().all(|s| !s.drop_name) {
                return Ok(QueryValue::Matrix(m));
            }
            *merge_passes += 1;
            let mut merged_at: Vec<usize> = Vec::new();
            let mut by_identity: HashMap<SeriesIdentity, usize> = HashMap::with_capacity(m.len());
            let mut out: Vec<RangeSeries> = Vec::with_capacity(m.len());
            for mut s in m {
                strip_metadata_labels(s.drop_name, &mut s.labels);
                if s.drop_name {
                    s.metric_name = None;
                }
                let key = (s.metric_name.clone(), s.labels.clone());
                match by_identity.get(&key) {
                    Some(&i) => {
                        out[i].points.extend(s.points);
                        merged_at.push(i);
                    }
                    None => {
                        by_identity.insert(key, out.len());
                        out.push(s);
                    }
                }
            }
            for i in merged_at {
                let points = &mut out[i].points;
                points.sort_by_key(|p| p.t_ms);
                if points.windows(2).any(|w| w[0].t_ms == w[1].t_ms) {
                    return Err(same_labelset_error());
                }
            }
            // Re-establish the assembly sort — a merge (or the strip
            // itself) can perturb the pre-strip `(labels, metric_name)`
            // order.
            out.sort_by(|a, b| (&a.labels, &a.metric_name).cmp(&(&b.labels, &b.metric_name)));
            Ok(QueryValue::Matrix(out))
        }
    }
}

// ---------------------------------------------------------------------------
// Subquery materialization (issue #83)
// ---------------------------------------------------------------------------

/// One materialized subquery: its inner expression evaluated exactly once
/// per query over the epoch-anchored union grid, grouped per series.
/// Samples are ascending by construction (the grid is walked ascending).
#[derive(Debug, Clone)]
struct MaterializedSubquery {
    series: Vec<MaterializedSeries>,
}

#[derive(Debug, Clone)]
struct MaterializedSeries {
    labels: Labels,
    metric_name: Option<String>,
    /// The inner expression's delayed name-removal verdict, latched at
    /// the identity's first inner-grid step exactly like the outer range
    /// accumulator (issue #86 plan v2 Δ1) — upstream materializes a
    /// subquery through the same `rangeEval` seriess accumulation, and
    /// the consuming range function ORs it in
    /// (`seriesDropName = dropName || inputDropName`, engine.go:2281):
    /// `last_over_time(abs(m)[10m:])` must drop the name the inner `abs`
    /// marked.
    drop_name: bool,
    samples: Vec<Sample>,
}

/// Materialized subqueries keyed by the [`SubqueryPlan`] node's address —
/// the plan tree is borrowed immutably for the whole evaluation, so node
/// identity is stable; each node appears in the tree exactly once.
type SubqueryCache = HashMap<*const SubqueryPlan, MaterializedSubquery>;

/// One `info()` node's prepared horizon (issue #82): the arg0 vector
/// materialized per evaluation step (upstream materializes the same
/// matrix via `ev.eval(args[0])`), plus the HORIZON-WIDE
/// identifying-label allowed-value map — built once from the full
/// non-ignored base matrix before any per-step combining, exactly as the
/// pin does (`fetchInfoSeries`'s `idLblValues`; the #82 round-2
/// adjudication rejected a per-step reconstruction, which would break
/// churn and duplicate-error semantics).
#[derive(Debug, Clone)]
struct PreparedInfo {
    /// Step time → the evaluated arg0 vector at that step. Keys are
    /// exactly the evaluation times of the node's enclosing horizon (the
    /// query's own steps at the root, a subquery's inner grid inside
    /// one) — `prepare_info` and the stepping phase walk the same grid.
    base_steps: HashMap<i64, Vec<InstantSample>>,
    /// Identifying label → its present, non-empty values across the
    /// non-ignored base matrix. Empty ⇒ the `info.go:183` short-circuit
    /// (zero info participation for the whole horizon).
    id_lbl_values: BTreeMap<String, BTreeSet<String>>,
    /// Issue #82 (retroactive re-review, Option B): the info-family
    /// selector's OWN fetched series (`SeriesData::get`), narrowed ONCE
    /// per horizon down to those passing
    /// `info::is_eligible_info_candidate` — the label-only half of
    /// `combine`'s steps 5+6 (name matchers, data matchers, and the
    /// horizon-wide `id_lbl_values` membership test), which is step-
    /// invariant (it never depends on a sample value or timestamp).
    /// Every series here carries a resolved `metric_name` (`Some`) — a
    /// genuinely nameless fetched series can never be an info source and
    /// is dropped during this narrowing, not carried forward as `None`.
    /// `eval_step`'s `PlanExpr::Info` arm resolves per-step staleness
    /// over ONLY this set, so per-step work is `O(eligible)`, not
    /// `O(fetched)` — fixing the retroactive re-review's [high] finding
    /// (unbounded `O(fetched × steps)` re-scan of the whole info family
    /// at every step).
    eligible_info: Vec<FetchedSeries>,
}

/// Prepared `info()` nodes keyed by the [`PlanExpr::Info`] node's address
/// (the [`SubqueryCache`] node-identity precedent).
type InfoCache = HashMap<*const PlanExpr, PreparedInfo>;

/// The running evaluation-wide `limit_ratio` parameter extrema for one
/// [`PlanExpr::Aggregate`] node (issue #130 Δ2): upstream materializes
/// the param expr over the WHOLE horizon before its step loop and warns
/// from `params.Max()`/`params.Min()` — at most one
/// `NewInvalidRatioWarning` per side per query (`engine.go:1649-1660` at
/// the pin), never per step. Our param is evaluated per step inside the
/// `Aggregate` arm, so the arm folds each step's raw (uncapped) value in
/// here and [`flush_ratio_warnings`] emits once after stepping. Both
/// engines evaluate the param at exactly the node's evaluation grid
/// points (subqueries materialize once over the union grid per #83; a
/// step-invariant root records once — extrema of a constant ≡ per-step),
/// so the accumulated pair equals upstream's `params.Max()/Min()`.
#[derive(Debug, Clone, Copy)]
struct RatioExtrema {
    max: f64,
    min: f64,
}

/// Step-invariant cache roots keyed by node address (issue #88, the
/// [`SubqueryCache`]/[`InfoCache`] node-identity precedent): the highest
/// wrappable-invariant subtrees ([`crate::plan::step_invariance`]),
/// each evaluated exactly once at the range start by
/// [`prepare_step_invariant`]; [`eval_step`] short-circuits to the cached
/// clone at every step.
type StepInvariantCache = HashMap<*const PlanExpr, StepValue>;

/// Everything the prepare pass materializes before stepping begins —
/// subquery grids (issue #83), `info()` horizons (issue #82), and
/// step-invariant roots (issue #88), bundled so [`eval_step`]'s signature
/// carries one shared read-only handle.
///
/// **Lifetime (issue #88, plan v2 Δ4):** constructed fresh inside every
/// [`evaluate_counted`] call and dropped with it — the address keys carry
/// neither start time nor data identity, so any reuse across evaluations
/// would return stale values
/// (`tests::the_step_invariant_cache_is_fresh_per_evaluate_call`).
#[derive(Debug, Default)]
struct EvalCaches {
    subqueries: SubqueryCache,
    infos: InfoCache,
    step_invariant: StepInvariantCache,
    /// The marked-root address set, inserted BEFORE the root's single
    /// prep-pass evaluation (so that evaluation is itself observed by
    /// [`eval_step`]'s instrument). Identical keys to `step_invariant`
    /// once preparation finishes; kept separate so the count instrument
    /// works during the window where a root is being evaluated but not
    /// yet cached.
    step_invariant_marked: HashSet<*const PlanExpr>,
    /// Interior-mutable eval counter (a [`Cell`], because [`eval_step`]
    /// holds `&EvalCaches`); copied into
    /// [`EvalCounts::step_invariant_evals`] after stepping completes.
    /// Single-threaded by construction — the evaluator never shares
    /// `EvalCaches` across threads.
    step_invariant_evals: Cell<u64>,
    /// M7-A5b-i: the annotations sink, threaded via interior mutability
    /// (a [`RefCell`], because [`eval_step`] holds `&EvalCaches`) so
    /// native-histogram arms (and future non-histogram annotation sources)
    /// can push a warning/info without widening every frame's return type
    /// (plan v2 OQ1(a): "thread a collector, do not widen every `Result`").
    /// Drained by [`evaluate_counted`] at both return points.
    annotations: RefCell<Annotations>,
    /// Cooperative cancellation (issue #93): owned, not borrowed, so no
    /// lifetime is added to `EvalCaches` or any `prepare_*` signature.
    /// Defaults to [`CancelToken::never`] — the shape [`evaluate`] uses.
    cancel: CancelToken,
    /// Issue #130 Δ2: per-`limit_ratio`-node parameter extrema, keyed by
    /// node address (the [`InfoCache`] identity precedent — caches are
    /// fresh per [`evaluate_counted_with`] call, so addresses cannot
    /// collide or go stale). A `Vec` + linear scan keeps deterministic
    /// insertion-order emission in [`flush_ratio_warnings`]; N = the
    /// query's `limit_ratio` node count. Interior-mutable (`RefCell`)
    /// because the `Aggregate` arm holds `&EvalCaches`.
    ratio_extrema: RefCell<Vec<(*const PlanExpr, RatioExtrema)>>,
}

/// The evaluation grid a node will be stepped over: the closed
/// `[start_ms, end_ms]` span plus its step (`0` ⇒ a single step at
/// `start_ms` — the instant-query shape). The query's own grid at the
/// root; a subquery's inner grid for its inner expression.
#[derive(Debug, Clone, Copy)]
struct Horizon {
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
}

impl Horizon {
    fn span(self) -> (i64, i64) {
        (self.start_ms, self.end_ms)
    }
}

/// The exact evaluation timestamps a subtree will be stepped at (issue
/// #83 plan v2, sparse subquery envelope pruning): threaded through the
/// subquery prepare pass (`prepare_subqueries`/`prepare_source`/
/// `prepare_subquery`/`prepare_info`) so `prepare_subquery` can prune its
/// own inner materialization to the consumer windows' union instead of
/// the full envelope.
#[derive(Debug, Clone, Copy)]
enum StepGrid<'a> {
    /// A regular grid, `start..=end` by `step` (`step <= 0` ⇒ a single
    /// point at `start` — the instant-query shape). The query's own grid
    /// at the root; a dense (unpruned) subquery's own materialization
    /// grid.
    Dense(Horizon),
    /// A pruned subquery grid: `live` is the ascending, deduped subset of
    /// `envelope`'s points that lies inside at least one consumer window
    /// (see [`live_grid_points`]). `envelope` is kept ONLY for anchor
    /// derivation (`mint_min`/`maxt_max`/`grid_start` in
    /// [`prepare_subquery`]) — upstream's child-evaluator bounds are
    /// envelope bounds regardless of any client-side pruning
    /// (`engine.go:1952-1954` at the pin), so an anchor must never be
    /// derived from `live`.
    Sparse { envelope: Horizon, live: &'a [i64] },
}

impl<'a> StepGrid<'a> {
    /// The full grid extent — ANCHOR DERIVATION ONLY (see the `Sparse`
    /// variant's doc); never the set of points actually visited.
    fn envelope(self) -> Horizon {
        match self {
            StepGrid::Dense(h) => h,
            StepGrid::Sparse { envelope, .. } => envelope,
        }
    }

    /// The ascending evaluation points this grid actually visits.
    fn points(self) -> GridPoints<'a> {
        match self {
            StepGrid::Dense(h) => GridPoints::Dense {
                next: Some(h.start_ms),
                end_ms: h.end_ms,
                step_ms: h.step_ms,
            },
            StepGrid::Sparse { live, .. } => GridPoints::Sparse(live.iter()),
        }
    }
}

/// [`StepGrid::points`]'s iterator — a plain `while` loop for `Dense`
/// (zero allocation, byte-identical to the pre-#83-round-2 materialization
/// loop) or a slice walk for `Sparse`.
enum GridPoints<'a> {
    Dense {
        next: Option<i64>,
        end_ms: i64,
        step_ms: i64,
    },
    Sparse(std::slice::Iter<'a, i64>),
}

impl Iterator for GridPoints<'_> {
    type Item = i64;

    fn next(&mut self) -> Option<i64> {
        match self {
            GridPoints::Dense {
                next,
                end_ms,
                step_ms,
            } => {
                let current = (*next)?;
                *next = if *step_ms <= 0 {
                    None
                } else {
                    let candidate = current + *step_ms;
                    (candidate <= *end_ms).then_some(candidate)
                };
                Some(current)
            }
            GridPoints::Sparse(it) => it.next().copied(),
        }
    }
}

/// The first inner-grid timestamp for a subquery window `(mint, maxt]`:
/// the smallest multiple of `step` STRICTLY GREATER than `mint` — the
/// epoch-anchored ascending grid (upstream `runSubquery`: `subqStart :=
/// step * floor(mint/step)`, corrected up one step on the boundary).
///
/// NOTE (issue #83 plan v2 Δ1): the vendored `at_modifier.test:159`
/// inline comment ("inner subquery: at 905=…, at 915=…") mis-states this
/// grid — the compiled engine at the pinned SHA emits `{900s, 910s}` for
/// that case (epoch-anchored), and the asserted aggregate (360)
/// coincidentally matches both. Do not re-derive an end-anchored grid
/// from that comment; `proof/m6_08a_at_subquery.test`'s
/// `sum_over_time(vector(time())[10s:3s] @ 25) = 63` golden fails any
/// end-anchored port (which would yield 66).
fn subquery_grid_start(mint: i64, step: i64) -> i64 {
    debug_assert!(step > 0, "plan_subquery rejects non-positive steps");
    let s = (mint / step) * step;
    if s <= mint { s + step } else { s }
}

/// Walks `expr` and materializes every [`RangeSource::Subquery`] node —
/// children (nested subqueries) first, so each materialization's own
/// inner evaluations find their nested results already cached. `grid` is
/// the exact set of evaluation times this node will be stepped at (the
/// query's own grid at the root; a subquery's own materialization grid —
/// dense or pruned, issue #83 plan v2 — for its inner expression).
// Issue #95 threads the step-invariance classifier through the subquery
// prep recursion (plan-mandated signature), pushing this to 8 params.
#[allow(clippy::too_many_arguments)]
fn prepare_subqueries(
    expr: &PlanExpr,
    selectors: &[SelectorSpec],
    data: &EvalData<'_>,
    grid: StepGrid<'_>,
    lookback_ms: i64,
    caches: &mut EvalCaches,
    inner_evals: &mut u64,
    classifier: &mut crate::plan::StepInvariance<'_>,
) -> Result<(), PromqlError> {
    match expr {
        PlanExpr::Selector(_)
        | PlanExpr::Scalar(_)
        | PlanExpr::StringLiteral(_)
        | PlanExpr::Time => Ok(()),
        PlanExpr::RangeFn { source, .. }
        | PlanExpr::OverTime { source, .. }
        | PlanExpr::AbsentOverTime { source } => prepare_source(
            source,
            selectors,
            data,
            grid,
            lookback_ms,
            caches,
            inner_evals,
            classifier,
        ),
        PlanExpr::OverTimeParam { source, args, .. } => {
            for a in args {
                prepare_subqueries(
                    a,
                    selectors,
                    data,
                    grid,
                    lookback_ms,
                    caches,
                    inner_evals,
                    classifier,
                )?;
            }
            prepare_source(
                source,
                selectors,
                data,
                grid,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )
        }
        PlanExpr::Absent { arg, .. }
        | PlanExpr::Sort { arg, .. }
        | PlanExpr::SortByLabel { arg, .. }
        | PlanExpr::LabelReplace { arg, .. }
        | PlanExpr::LabelJoin { arg, .. }
        | PlanExpr::Timestamp { arg, .. }
        | PlanExpr::ScalarOf { arg }
        | PlanExpr::VectorOf { arg } => prepare_subqueries(
            arg,
            selectors,
            data,
            grid,
            lookback_ms,
            caches,
            inner_evals,
            classifier,
        ),
        PlanExpr::DateFn { arg, .. } => match arg {
            Some(arg) => prepare_subqueries(
                arg,
                selectors,
                data,
                grid,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            ),
            None => Ok(()),
        },
        PlanExpr::HistogramQuantile { quantile, expr } => {
            prepare_subqueries(
                quantile,
                selectors,
                data,
                grid,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )?;
            prepare_subqueries(
                expr,
                selectors,
                data,
                grid,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )
        }
        PlanExpr::HistogramQuantiles {
            expr, quantiles, ..
        } => {
            prepare_subqueries(
                expr,
                selectors,
                data,
                grid,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )?;
            for q in quantiles {
                prepare_subqueries(
                    q,
                    selectors,
                    data,
                    grid,
                    lookback_ms,
                    caches,
                    inner_evals,
                    classifier,
                )?;
            }
            Ok(())
        }
        PlanExpr::HistogramAccessor { arg, .. } => prepare_subqueries(
            arg,
            selectors,
            data,
            grid,
            lookback_ms,
            caches,
            inner_evals,
            classifier,
        ),
        PlanExpr::HistogramFraction { lower, upper, expr } => {
            prepare_subqueries(
                lower,
                selectors,
                data,
                grid,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )?;
            prepare_subqueries(
                upper,
                selectors,
                data,
                grid,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )?;
            prepare_subqueries(
                expr,
                selectors,
                data,
                grid,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )
        }
        PlanExpr::Aggregate { expr, param, .. } => {
            if let Some(p) = param {
                prepare_subqueries(
                    p,
                    selectors,
                    data,
                    grid,
                    lookback_ms,
                    caches,
                    inner_evals,
                    classifier,
                )?;
            }
            prepare_subqueries(
                expr,
                selectors,
                data,
                grid,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )
        }
        PlanExpr::CountValues { expr, .. } => prepare_subqueries(
            expr,
            selectors,
            data,
            grid,
            lookback_ms,
            caches,
            inner_evals,
            classifier,
        ),
        PlanExpr::Binary { lhs, rhs, .. } | PlanExpr::SetOp { lhs, rhs, .. } => {
            prepare_subqueries(
                lhs,
                selectors,
                data,
                grid,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )?;
            prepare_subqueries(
                rhs,
                selectors,
                data,
                grid,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )
        }
        PlanExpr::MathFn {
            arg, scalar_args, ..
        } => {
            prepare_subqueries(
                arg,
                selectors,
                data,
                grid,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )?;
            for a in scalar_args {
                prepare_subqueries(
                    a,
                    selectors,
                    data,
                    grid,
                    lookback_ms,
                    caches,
                    inner_evals,
                    classifier,
                )?;
            }
            Ok(())
        }
        PlanExpr::ScalarFn { args, .. } => {
            for a in args {
                prepare_subqueries(
                    a,
                    selectors,
                    data,
                    grid,
                    lookback_ms,
                    caches,
                    inner_evals,
                    classifier,
                )?;
            }
            Ok(())
        }
        // Issue #82 (M6-05b): recurse into the base FIRST (nested
        // subqueries and nested info() nodes materialize inside-out,
        // like `prepare_subquery`), then walk the base over this node's
        // full horizon once to build the horizon-wide identifying-label
        // narrowing. The info-family selector itself is a plain instant
        // selector — never a subquery — so only `base` recurses.
        PlanExpr::Info { base, .. } => {
            prepare_subqueries(
                base,
                selectors,
                data,
                grid,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )?;
            prepare_info(expr, selectors, data, grid, lookback_ms, caches)
        }
    }
}

/// Walks one `info()` node's arg0 over its enclosing horizon exactly
/// once (issue #82) — the evaluation-time counterpart of upstream
/// `evalInfo`'s `ev.eval(args[0])` matrix + `fetchInfoSeries`'s
/// `idLblValues` construction (`info.go:150-170`), which are both
/// horizon-wide, never per-step (the #82 round-2 adjudication). Caches
/// each step's base vector (reused verbatim by the stepping phase — the
/// base is never evaluated twice), the allowed identifying-label values
/// of every NON-ignored base series (ignored ⟺ the retained name matches
/// all effective name matchers; empty values never register), and
/// (issue #82 retroactive re-review, Option B) the info-family
/// selector's fetched series narrowed to the eligible subset ONCE — see
/// [`PreparedInfo::eligible_info`]'s doc for why this is sound (the
/// eligibility predicate is purely label-based, never sample-dependent).
///
/// Issue #83 plan v2 (edge case 5): `grid` may be a pruned [`StepGrid`]
/// when this `info()` node sits inside an annotation-free subquery inner
/// — `base_steps`/`id_lbl_values` are then built from only the LIVE
/// points, not the full envelope. That is value-exact: eligibility is
/// purely label-based (never sample-dependent), and the stepping phase
/// only ever looks up a live point's own `base_steps` entry (never a
/// pruned gap point's), so narrowing over a subset of steps that
/// (super)sets every step actually consulted is sound.
fn prepare_info(
    node: &PlanExpr,
    selectors: &[SelectorSpec],
    data: &EvalData<'_>,
    grid: StepGrid<'_>,
    lookback_ms: i64,
    caches: &mut EvalCaches,
) -> Result<(), PromqlError> {
    let PlanExpr::Info {
        base,
        info_selector,
        name_matchers,
        data_matchers,
    } = node
    else {
        // Only ever called from the `PlanExpr::Info` arm above.
        return Err(PromqlError::Unsupported {
            construct: "prepare_info over a non-info node".to_string(),
        });
    };
    let mut matcher_cache = info::MatcherCache::new();
    let mut base_steps: HashMap<i64, Vec<InstantSample>> = HashMap::new();
    let mut id_lbl_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for t in grid.points() {
        let StepValue::Vector(v) = eval_step(base, selectors, data, t, lookback_ms, caches)? else {
            // The vendored parser type-checks info()'s first argument as
            // an instant vector; kept total (defense-in-depth).
            return Err(PromqlError::Unsupported {
                construct: "info() over a non-vector expression".to_string(),
            });
        };
        for s in &v {
            let retained = s.metric_name.as_deref().unwrap_or("");
            if info::matches_all(&mut matcher_cache, name_matchers, retained)? {
                // Ignored (an info series itself) — contributes no
                // identifying-label values (info.go:152-154).
                continue;
            }
            for label in info::identifying_labels() {
                if let Some(value) = s.labels.get(label)
                    && !value.is_empty()
                {
                    id_lbl_values
                        .entry(label.to_string())
                        .or_default()
                        .insert(value.to_string());
                }
            }
        }
        base_steps.insert(t, v);
    }

    // Issue #82 (retroactive re-review, Option B): narrow the
    // info-family selector's OWN fetched series to the eligible subset
    // exactly ONCE, over the whole horizon — the `:183` short-circuit
    // (`id_lbl_values` empty ⇒ zero participation, `combine`'s own
    // guard) applies here too, so a short-circuited horizon never even
    // walks the fetched set. Every kept series' `metric_name` is
    // resolved (concrete-name fallback, the `Selector` arm's own
    // contract) — a genuinely nameless series can never be an info
    // source and is dropped here rather than carried forward as `None`.
    let sel = &selectors[*info_selector];
    let mut eligible_info: Vec<FetchedSeries> = Vec::new();
    if !id_lbl_values.is_empty() {
        for series in data.get(*info_selector) {
            let Some(metric_name) = series
                .metric_name
                .clone()
                .or_else(|| sel.metric_name.clone())
            else {
                continue;
            };
            if info::is_eligible_info_candidate(
                &mut matcher_cache,
                &metric_name,
                &series.labels,
                &id_lbl_values,
                name_matchers,
                data_matchers,
            )? {
                eligible_info.push(FetchedSeries {
                    metric_name: Some(metric_name),
                    ..series.clone()
                });
            }
        }
    }

    caches.infos.insert(
        node as *const _,
        PreparedInfo {
            base_steps,
            id_lbl_values,
            eligible_info,
        },
    );
    Ok(())
}

// Issue #95: threads the classifier to `prepare_subquery` (8 params).
#[allow(clippy::too_many_arguments)]
fn prepare_source(
    source: &RangeSource,
    selectors: &[SelectorSpec],
    data: &EvalData<'_>,
    grid: StepGrid<'_>,
    lookback_ms: i64,
    caches: &mut EvalCaches,
    inner_evals: &mut u64,
    classifier: &mut crate::plan::StepInvariance<'_>,
) -> Result<(), PromqlError> {
    match source {
        RangeSource::Selector(_) => Ok(()),
        RangeSource::Subquery(sq) => prepare_subquery(
            sq,
            selectors,
            data,
            grid,
            lookback_ms,
            caches,
            inner_evals,
            classifier,
        ),
    }
}

/// Materializes one subquery over its own grid: the DENSE epoch-anchored
/// ASCENDING envelope `{ k·step : k·step > mint_min, k·step ≤ maxt_max }`
/// (a single window when the subquery carries its own `@` — the anchor is
/// fixed), or — issue #83 plan v2 — the SPARSE subset of that envelope
/// actually consumed by `grid` (the consumer's own evaluation-time set),
/// whenever pruning to it cannot drop a Prometheus-visible annotation
/// (see [`expr_may_annotate`]). `mint_min`/`maxt_max`/`grid_start` are
/// always derived from `grid.envelope()`, NEVER from `grid`'s live
/// points — upstream's child-evaluator bounds are envelope bounds
/// regardless of any client-side pruning (`engine.go:1952-1954,1981-1986`
/// at the pin), and the aggregate-param quirk (issue #88) makes the
/// freeze anchor value-bearing. Recursion depth is bounded by the
/// planner's `MAX_SUBQUERY_DEPTH` guard.
///
/// Issue #95: after the inside-out `prepare_subqueries` recursion and
/// before the grid loop, [`prepare_step_invariant`] freezes the highest
/// step-invariant subtrees of `sq.inner` at `grid_start`, so the grid
/// loop's per-point `eval_step(&sq.inner, …, it)` returns the frozen
/// clone for those marked nodes — upstream's nested `StepInvariantExpr`
/// copy at `subqStart == grid_start`.
// Issue #95 adds the classifier param (plan-mandated), pushing to 8.
#[allow(clippy::too_many_arguments)]
fn prepare_subquery(
    sq: &SubqueryPlan,
    selectors: &[SelectorSpec],
    data: &EvalData<'_>,
    grid: StepGrid<'_>,
    lookback_ms: i64,
    caches: &mut EvalCaches,
    inner_evals: &mut u64,
    classifier: &mut crate::plan::StepInvariance<'_>,
) -> Result<(), PromqlError> {
    let envelope = grid.envelope();
    let (anchor_min, anchor_max) = match sq.at_ms {
        Some(at) => (at, at),
        None => envelope.span(),
    };
    let maxt_max = anchor_max - sq.offset_ms;
    let mint_min = anchor_min - sq.offset_ms - sq.range_ms;
    let grid_start = subquery_grid_start(mint_min, sq.step_ms);

    // Series identity → (first-step-latched drop_name, grid samples),
    // BTreeMap for deterministic order. The latch mirrors the outer range
    // accumulator's (issue #86 plan v2 Δ1 — see `MaterializedSeries`).
    let mut acc: BTreeMap<SeriesIdentity, (bool, Vec<Sample>)> = BTreeMap::new();
    if grid_start <= maxt_max {
        // Issue #83 plan v2 (codex Q1 ruling): prune to the consumer
        // windows' union ONLY when `sq.inner` is provably annotation-free
        // — a capable inner keeps the FULL envelope exactly as before
        // #83, because a gap point it would otherwise skip can carry a
        // Prometheus-visible warning/info this materialization is the
        // only place that ever observes.
        let live: Option<Vec<i64>> = if expr_may_annotate(&sq.inner) {
            None
        } else {
            live_grid_points(sq, grid, mint_min, maxt_max)
        };

        // Children first (inside-out): the inner expression's own nested
        // subqueries must be materialized before it can be evaluated. Its
        // own grid is this one (dense envelope, or the pruned live set —
        // nested subqueries AND any nested info() nodes are evaluated on
        // it, so pruning compounds inside-out).
        let grid_last = maxt_max - (maxt_max - grid_start).rem_euclid(sq.step_ms);
        let inner_envelope = Horizon {
            start_ms: grid_start,
            end_ms: grid_last,
            step_ms: sq.step_ms,
        };
        let inner_grid = match live.as_deref() {
            Some(pts) => StepGrid::Sparse {
                envelope: inner_envelope,
                live: pts,
            },
            None => StepGrid::Dense(inner_envelope),
        };
        prepare_subqueries(
            &sq.inner,
            selectors,
            data,
            inner_grid,
            lookback_ms,
            caches,
            inner_evals,
            classifier,
        )?;

        // Issue #95: freeze the highest step-invariant subtrees INSIDE
        // this subquery's inner, anchored at `grid_start` — upstream's
        // `preprocessExprHelper` `SubqueryExpr` arm wraps the invariant
        // inner in a nested `StepInvariantExpr` evaluated once at
        // `subqStart` (engine.go:4577-4585 at the pin), which equals this
        // `grid_start` at every nesting level. The per-grid-point
        // `eval_step(&sq.inner, …, it)` below then returns the frozen
        // clone for those marked nodes instead of recomputing. Anchored
        // at `grid_start`, NOT the outer query start: the outer walk in
        // `prepare_step_invariant` stops at range sources and never
        // descends here, so the two anchors never collide.
        prepare_step_invariant(
            &sq.inner,
            selectors,
            data,
            grid_start,
            lookback_ms,
            caches,
            classifier,
        )?;

        // Issue #83 plan v2: `inner_grid.points()` visits the DENSE
        // envelope byte-identically to the pre-#83-round-2 `while it <=
        // maxt_max { …; it += sq.step_ms }` loop when `live` is `None`
        // (zero allocation — see `GridPoints::Dense`), or only the pruned
        // live points when `Some`.
        for it in inner_grid.points() {
            // Issue #93: one `Relaxed` load per grid point, mirroring the
            // outer range-step checkpoint above.
            if caches.cancel.is_cancelled() {
                return Err(PromqlError::Cancelled);
            }
            *inner_evals += 1;
            match eval_step(&sq.inner, selectors, data, it, lookback_ms, caches)? {
                StepValue::Vector(v) => {
                    for s in v {
                        let mut h = s.h;
                        // Issue #125 — the pin's `evalSubquery` hint rule
                        // (`engine.go:2036-2043`): NotCounterReset AND
                        // CounterReset hints on subquery output samples
                        // are reset to Unknown, "because we might
                        // otherwise miss a counter reset happening in
                        // samples not returned by the subquery, or we
                        // might over-detect counter resets if the sample
                        // with a counter reset is returned multiple
                        // times". Gauge and Unknown pass through.
                        if let Some(hist) = h.as_mut()
                            && matches!(
                                hist.counter_reset_hint,
                                pulsus_model::CounterResetHint::NotCounterReset
                                    | pulsus_model::CounterResetHint::CounterReset
                            )
                        {
                            hist.counter_reset_hint = pulsus_model::CounterResetHint::Unknown;
                        }
                        acc.entry((s.metric_name, s.labels))
                            .or_insert_with(|| (s.drop_name, Vec::new()))
                            .1
                            .push(Sample {
                                t_ms: it,
                                v: s.v,
                                h,
                            });
                    }
                }
                // The vendored parser type-checks subqueries as
                // instant-vector-only; kept total (defense-in-depth).
                StepValue::Scalar(_) | StepValue::String(_) => {
                    return Err(PromqlError::Unsupported {
                        construct: "subquery over a non-vector expression".to_string(),
                    });
                }
            }
        }
    }

    let series = acc
        .into_iter()
        .map(
            |((metric_name, labels), (drop_name, samples))| MaterializedSeries {
                labels,
                metric_name,
                drop_name,
                samples,
            },
        )
        .collect();
    // Always insert — an empty grid/result must still satisfy the
    // stepping phase's cache lookup (the at_modifier.test:227-255
    // empty-@ non-panic cases).
    caches
        .subqueries
        .insert(sq as *const _, MaterializedSubquery { series });
    Ok(())
}

/// Union of consumer windows on the subquery's own epoch grid (issue #83
/// plan v2). Per consumer evaluation time `t` (ascending, from `consumer
/// .points()`): `eff_t = sq.at_ms.unwrap_or(t) − sq.offset_ms`, window
/// `(eff_t − sq.range_ms, eff_t]` — the EXACT arithmetic
/// `windowed_range_source` slices with. Emits every step-multiple inside
/// at least one such window, sorted ascending and unique BY CONSTRUCTION
/// (code review [medium]): the consumer points are ascending and the
/// offset is constant, so the windows are pre-sorted — a single
/// monotonic grid cursor resumes each overlapping window just past the
/// last emitted point, pushing every union point exactly once. No
/// duplicate ever enters the buffer and there is no sort/dedup pass, so
/// the work is `O(|consumer points| + |union|)`, never
/// `O(|consumer points| × points-per-window)`
/// (`tests::live_grid_points_with_heavily_overlapping_windows_emits_each_point_once`).
///
/// Returns `None` when pruning cannot help (the caller then iterates the
/// full envelope, unpruned): `sq.at_ms.is_some()` (a single fixed window
/// already equals the envelope), or a [`StepGrid::Dense`] consumer whose
/// step is `<= sq.range_ms` (its windows overlap or touch end-to-end, so
/// their union IS the envelope — the common non-sparse case, kept on
/// today's zero-allocation loop).
fn live_grid_points(
    sq: &SubqueryPlan,
    consumer: StepGrid<'_>,
    mint_min: i64,
    maxt_max: i64,
) -> Option<Vec<i64>> {
    if sq.at_ms.is_some() {
        return None;
    }
    if let StepGrid::Dense(h) = consumer
        && h.step_ms <= sq.range_ms
    {
        return None;
    }
    let mut live: Vec<i64> = Vec::new();
    let mut prev_eff_t: Option<i64> = None;
    for t in consumer.points() {
        // `sq.at_ms` is `None` on this path (checked above).
        let eff_t = t - sq.offset_ms;
        let lower_excl = eff_t - sq.range_ms;
        debug_assert!(
            lower_excl >= mint_min && eff_t <= maxt_max,
            "every consumer window must lie within the caller's envelope-derived bounds \
             (`mint_min`/`maxt_max`) — a violation means `grid`/`consumer` diverged from the \
             envelope this call's `mint_min`/`maxt_max` were computed against"
        );
        // The cursor's soundness contract: ascending consumer points
        // (both `GridPoints` arms are ascending by construction).
        debug_assert!(
            prev_eff_t.is_none_or(|prev| prev < eff_t),
            "consumer points must be strictly ascending for the monotonic cursor to be a union"
        );
        prev_eff_t = Some(eff_t);
        // First candidate in THIS window, clamped past the last emitted
        // point when the window overlaps its predecessor. Both operands
        // are multiples of `sq.step_ms` on the same epoch grid, so the
        // max stays grid-aligned.
        let mut p = subquery_grid_start(lower_excl, sq.step_ms);
        if let Some(&last) = live.last() {
            p = p.max(last + sq.step_ms);
        }
        while p <= eff_t {
            live.push(p);
            p += sq.step_ms;
        }
    }
    Some(live)
}

// ---------------------------------------------------------------------------
// Annotation-capability analysis (issue #83 plan v2, codex Q1 ruling)
// ---------------------------------------------------------------------------

/// CONSERVATIVE static annotation-capability analysis: `true` iff `expr`
/// MAY emit a warning/info [`Annotations`] entry on SOME data. Purely
/// structural — never inspects a sample. [`prepare_subquery`] retains the
/// full envelope (never prunes to the consumer windows' union) whenever
/// this returns `true` for a subquery's inner, because a gap point the
/// pruned grid would skip can be the ONLY place an annotation-capable
/// arm ever sees the data that triggers it.
///
/// Exhaustive match, no wildcard arm: a new [`PlanExpr`] variant is a
/// compile error here, forcing a conscious classification. Any future
/// change that adds an annotation emission site (a `caches.annotations`/
/// `&mut Annotations` access) to an evaluator arm currently classified
/// FREE below MUST move that shape to the CAPABLE side — this is a
/// standing obligation of that change, not merely a suggestion.
///
/// Emission-site inventory this whitelist is verified against (plan
/// review round 2, comment 5024575855): `eval/mod.rs:1659-1685,1803-
/// 1813,1853-1861,1935-1957,2167-2220,2364-2366,2718-2754`,
/// `aggregation.rs:238-349,407-454,571-576,624-626`,
/// `hist_range_fns.rs:64-65,143-177,227-311,492-520,625-629,705-706,780-
/// 782`, `histogram_fns.rs:110-115,128-136,262-270`, `binop.rs:111-254`.
/// No other evaluator module (`elementwise.rs`/`datetime.rs`/`labels.rs`/
/// `info.rs`/`staleness.rs`/`quote.rs`/`functions.rs`) touches the sink.
fn expr_may_annotate(expr: &PlanExpr) -> bool {
    match expr {
        // FREE leaves — no evaluator arm here ever touches the sink.
        PlanExpr::Selector(_)
        | PlanExpr::Scalar(_)
        | PlanExpr::StringLiteral(_)
        | PlanExpr::Time => false,

        // CAPABLE unconditionally: `rate`/`irate`/`increase`/`delta` all
        // carry mixed-floats/not-gauge/mixed-schema warnings and NHCB
        // reconcile infos (`hist_range_fns.rs:65-311`).
        PlanExpr::RangeFn { .. } => true,

        PlanExpr::OverTime { func, source } => {
            over_time_fn_may_annotate(*func) || range_source_may_annotate(source)
        }
        // CAPABLE unconditionally: quantile/predict_linear/
        // double_exponential_smoothing all carry
        // `HistogramIgnoredInMixedRangeInfo` (`hist_range_fns.rs:706-781`).
        PlanExpr::OverTimeParam { .. } => true,
        PlanExpr::AbsentOverTime { source } => range_source_may_annotate(source),
        PlanExpr::Absent { arg, .. } => expr_may_annotate(arg),
        PlanExpr::Sort { arg, .. } => expr_may_annotate(arg),
        PlanExpr::SortByLabel { arg, .. } => expr_may_annotate(arg),
        PlanExpr::LabelReplace { arg, .. } => expr_may_annotate(arg),
        PlanExpr::LabelJoin { arg, .. } => expr_may_annotate(arg),
        // CAPABLE unconditionally: partition warnings, invalid-φ and
        // NaN-observation infos, forced monotonicity (`histogram_fns.rs`).
        PlanExpr::HistogramQuantile { .. } => true,
        // Issue #153: same emission surface as the singular form (per-
        // quantile invalid-φ warnings on top).
        PlanExpr::HistogramQuantiles { .. } => true,
        // FREE own shape (pure accessors, no sink access — `mod.rs:2240-
        // 2266`), recurse into `arg`.
        PlanExpr::HistogramAccessor { arg, .. } => expr_may_annotate(arg),
        PlanExpr::HistogramFraction { .. } => true,
        PlanExpr::Aggregate {
            op, expr, param, ..
        } => {
            agg_op_may_annotate(*op)
                || expr_may_annotate(expr)
                || param.as_deref().is_some_and(expr_may_annotate)
        }
        // FREE own shape (`aggregation.rs:741-810`, no `annos` param),
        // recurse into `expr`.
        PlanExpr::CountValues { expr, .. } => expr_may_annotate(expr),
        // CAPABLE unconditionally: the incompatible-types info fires for
        // ANY operator the moment one operand sample is a histogram
        // (`binop.rs:112-230`).
        PlanExpr::Binary { .. } => true,
        // FREE own shape (verbatim passthrough, `binop.rs:403-430`, no
        // `annos` param), recurse into both operands.
        PlanExpr::SetOp { lhs, rhs, .. } => expr_may_annotate(lhs) || expr_may_annotate(rhs),
        PlanExpr::MathFn {
            arg, scalar_args, ..
        } => expr_may_annotate(arg) || scalar_args.iter().any(|a| expr_may_annotate(a)),
        PlanExpr::ScalarFn { args, .. } => args.iter().any(|a| expr_may_annotate(a)),
        PlanExpr::DateFn { arg, .. } => arg.as_deref().is_some_and(expr_may_annotate),
        PlanExpr::Timestamp { arg, .. } => expr_may_annotate(arg),
        PlanExpr::ScalarOf { arg } => expr_may_annotate(arg),
        PlanExpr::VectorOf { arg } => expr_may_annotate(arg),
        // FREE own shape (`info.rs` has no emission site), recurse into
        // `base`.
        PlanExpr::Info { base, .. } => expr_may_annotate(base),
    }
}

/// [`expr_may_annotate`]'s [`RangeSource`] half: a bare selector is
/// always FREE; a subquery source recurses into its own inner — the
/// nested-subquery-poisons-ancestors case (a capable node anywhere
/// beneath a subquery inner makes every enclosing subquery capable too).
fn range_source_may_annotate(source: &RangeSource) -> bool {
    match source {
        RangeSource::Selector(_) => false,
        RangeSource::Subquery(sq) => expr_may_annotate(&sq.inner),
    }
}

/// [`expr_may_annotate`]'s [`OverTimeFn`] half (`hist_range_fns.rs:417-
/// 455` dispositions): CAPABLE = {`Sum`, `Avg`, `Min`, `Max`, `Stddev`,
/// `Stdvar`, `Mad`, `Deriv`, `TsOfMin`, `TsOfMax`, `Idelta`} (drop-set
/// info / mixed-agg warn / `instant_value_hist`); FREE = {`Last`,
/// `First`, `Count`, `Present`, `Resets`, `Changes`, `TsOfFirst`,
/// `TsOfLast`} (pure, no `annos` access).
fn over_time_fn_may_annotate(f: OverTimeFn) -> bool {
    match f {
        OverTimeFn::Sum
        | OverTimeFn::Avg
        | OverTimeFn::Min
        | OverTimeFn::Max
        | OverTimeFn::Stddev
        | OverTimeFn::Stdvar
        | OverTimeFn::Mad
        | OverTimeFn::Deriv
        | OverTimeFn::TsOfMin
        | OverTimeFn::TsOfMax
        | OverTimeFn::Idelta => true,
        OverTimeFn::Last
        | OverTimeFn::First
        | OverTimeFn::Count
        | OverTimeFn::Present
        | OverTimeFn::Resets
        | OverTimeFn::Changes
        | OverTimeFn::TsOfFirst
        | OverTimeFn::TsOfLast => false,
    }
}

/// [`expr_may_annotate`]'s [`AggOp`] half (`aggregation.rs:250-454,572,
/// 625`): CAPABLE = {`Sum`, `Avg`, `Min`, `Max`, `Stddev`, `Stdvar`,
/// `Quantile`, `Topk`, `Bottomk`} plus `LimitRatio` (issue #130: its
/// ratio-cap warning surface is now ported, emitted from
/// [`flush_ratio_warnings`]'s horizon-wide extrema — CAPABLE is
/// load-bearing precisely because [`expr_may_annotate`] then disables
/// subquery live-grid pruning, keeping every grid point's param in the
/// extrema, upstream's full `params` buffer); FREE = {`Count`, `Group`,
/// `LimitK`} (no `annos` access at all — `aggregation.rs:438-442,
/// 681-726`).
fn agg_op_may_annotate(op: AggOp) -> bool {
    match op {
        AggOp::Sum
        | AggOp::Avg
        | AggOp::Min
        | AggOp::Max
        | AggOp::Stddev
        | AggOp::Stdvar
        | AggOp::Quantile
        | AggOp::Topk
        | AggOp::Bottomk
        | AggOp::LimitRatio => true,
        AggOp::Count | AggOp::Group | AggOp::LimitK => false,
    }
}

// ---------------------------------------------------------------------------
// Step-invariant once-and-copy preparation (issue #88)
// ---------------------------------------------------------------------------

/// Registers the HIGHEST wrappable step-invariant subtrees as cache
/// roots, evaluating each exactly once at `start_ms` — the evaluation
/// model of upstream's `StepInvariantExpr` arm (engine.go:2563-2600 at
/// the pin: the wrapped expr is evaluated with `endTimestamp =
/// startTimestamp` and the single result duplicated across steps; our
/// range accumulator re-stamps step timestamps itself, so returning the
/// cached clone per step is the exact copy).
///
/// The walk mirrors where upstream `preprocessExprHelper` places
/// `StepInvariantExpr` wrappers: a wrappable-invariant node is a root
/// (descent stops — every evaluated descendant of an invariant node is
/// itself invariant, so roots are pairwise disjoint and never nested);
/// otherwise its children are walked, EXCEPT
///
/// - **aggregate params** — upstream's `AggregateExpr` arm never
///   descends into `n.Param`, so no wrapper ever lands inside one (the
///   param-ignoring quirk's flip side);
/// - **subquery inners** (issue #95) — roots ARE placed inside a
///   [`SubqueryPlan::inner`], but exclusively by [`prepare_subquery`]'s
///   own call anchored at that subquery's `grid_start` (its `subqStart`),
///   NEVER by this walk: this walk still stops at range sources (below),
///   so it can never leak the outer `start_ms` anchor into a subquery
///   inner. Upstream's `preprocessExprHelper` wraps the invariant inner
///   in a nested `StepInvariantExpr` frozen once at `subqStart`
///   (engine.go:4577-4585 at the pin), which #95 reproduces by the
///   `grid_start`-anchored freeze there;
/// - **range sources** as such — a matrix selector is not a [`PlanExpr`]
///   node (upstream: `MatrixSelector` returns `shouldWrap = false`; the
///   enclosing call is the wrap candidate), and an `@`-fixed subquery
///   node needs no root (its materialized grid and per-step slice are
///   already step-invariant by construction).
///
/// Must run AFTER [`prepare_subqueries`]: a root's single evaluation may
/// slice from materialized subquery grids.
///
/// A root is MARKED (`step_invariant_marked`) before its single
/// evaluation, so that evaluation flows through [`eval_step`]'s
/// marked-node count instrument — the one genuine eval the Tier-1
/// `== 1` gate expects; every later count is a cache-bypass regression
/// (review round 1, finding 1). `classifier` memoizes by node address,
/// so the walk classifies each node exactly once overall
/// (`tests::the_prepare_walk_classifies_each_node_once` — finding 3).
fn prepare_step_invariant(
    expr: &PlanExpr,
    selectors: &[SelectorSpec],
    data: &EvalData<'_>,
    start_ms: i64,
    lookback_ms: i64,
    caches: &mut EvalCaches,
    classifier: &mut crate::plan::StepInvariance<'_>,
) -> Result<(), PromqlError> {
    let (invariant, wrappable) = classifier.classify(expr);
    if invariant && wrappable {
        let addr = expr as *const PlanExpr;
        caches.step_invariant_marked.insert(addr);
        let value = eval_step(expr, selectors, data, start_ms, lookback_ms, caches)?;
        caches.step_invariant.insert(addr, value);
        return Ok(());
    }
    match expr {
        // No children (or none reachable): leaves, and the range-source
        // arms per the doc comment above.
        PlanExpr::Selector(_)
        | PlanExpr::Scalar(_)
        | PlanExpr::StringLiteral(_)
        | PlanExpr::Time
        | PlanExpr::RangeFn { .. }
        | PlanExpr::OverTime { .. }
        | PlanExpr::AbsentOverTime { .. } => Ok(()),
        // Scalar parameters of a variant call are wrap candidates of
        // their own (upstream wraps invariant args of an unsafe call).
        PlanExpr::OverTimeParam { args, .. } | PlanExpr::ScalarFn { args, .. } => {
            for a in args {
                prepare_step_invariant(
                    a,
                    selectors,
                    data,
                    start_ms,
                    lookback_ms,
                    caches,
                    classifier,
                )?;
            }
            Ok(())
        }
        PlanExpr::Absent { arg, .. }
        | PlanExpr::Sort { arg, .. }
        | PlanExpr::SortByLabel { arg, .. }
        | PlanExpr::LabelReplace { arg, .. }
        | PlanExpr::LabelJoin { arg, .. }
        | PlanExpr::Timestamp { arg, .. }
        | PlanExpr::ScalarOf { arg }
        | PlanExpr::VectorOf { arg }
        | PlanExpr::HistogramAccessor { arg, .. } => prepare_step_invariant(
            arg,
            selectors,
            data,
            start_ms,
            lookback_ms,
            caches,
            classifier,
        ),
        PlanExpr::DateFn { arg, .. } => match arg {
            Some(arg) => prepare_step_invariant(
                arg,
                selectors,
                data,
                start_ms,
                lookback_ms,
                caches,
                classifier,
            ),
            None => Ok(()),
        },
        // Issue #153: variable child count (1 vector + 1..=10 quantiles),
        // so it cannot join the two-child binding below.
        PlanExpr::HistogramQuantiles {
            expr, quantiles, ..
        } => {
            prepare_step_invariant(
                expr,
                selectors,
                data,
                start_ms,
                lookback_ms,
                caches,
                classifier,
            )?;
            for q in quantiles {
                prepare_step_invariant(
                    q,
                    selectors,
                    data,
                    start_ms,
                    lookback_ms,
                    caches,
                    classifier,
                )?;
            }
            Ok(())
        }
        PlanExpr::HistogramQuantile {
            quantile: a,
            expr: b,
        }
        | PlanExpr::Binary { lhs: a, rhs: b, .. }
        | PlanExpr::SetOp { lhs: a, rhs: b, .. } => {
            prepare_step_invariant(
                a,
                selectors,
                data,
                start_ms,
                lookback_ms,
                caches,
                classifier,
            )?;
            prepare_step_invariant(
                b,
                selectors,
                data,
                start_ms,
                lookback_ms,
                caches,
                classifier,
            )
        }
        PlanExpr::HistogramFraction { lower, upper, expr } => {
            prepare_step_invariant(
                lower,
                selectors,
                data,
                start_ms,
                lookback_ms,
                caches,
                classifier,
            )?;
            prepare_step_invariant(
                upper,
                selectors,
                data,
                start_ms,
                lookback_ms,
                caches,
                classifier,
            )?;
            prepare_step_invariant(
                expr,
                selectors,
                data,
                start_ms,
                lookback_ms,
                caches,
                classifier,
            )
        }
        // The param (when present) is deliberately NOT walked — see the
        // doc comment.
        PlanExpr::Aggregate { expr, .. } | PlanExpr::CountValues { expr, .. } => {
            prepare_step_invariant(
                expr,
                selectors,
                data,
                start_ms,
                lookback_ms,
                caches,
                classifier,
            )
        }
        PlanExpr::MathFn {
            arg, scalar_args, ..
        } => {
            prepare_step_invariant(
                arg,
                selectors,
                data,
                start_ms,
                lookback_ms,
                caches,
                classifier,
            )?;
            for a in scalar_args {
                prepare_step_invariant(
                    a,
                    selectors,
                    data,
                    start_ms,
                    lookback_ms,
                    caches,
                    classifier,
                )?;
            }
            Ok(())
        }
        // `Info` is never a root (classifier: `(false, false)`); its
        // base may hold lower roots. The registered root's prepared value
        // is only consulted if the base is re-evaluated (the stepping
        // phase reads `PreparedInfo::base_steps` instead) — harmless
        // either way, and faithful to the upstream wrapper placement.
        PlanExpr::Info { base, .. } => prepare_step_invariant(
            base,
            selectors,
            data,
            start_ms,
            lookback_ms,
            caches,
            classifier,
        ),
    }
}

/// One range-vector function input series at one step, already windowed.
struct WindowedSeries {
    labels: Labels,
    /// The source's RETAINED metric name — the fetched series' own
    /// per-row name (issue #85), or the materialized inner series' name
    /// for a subquery source. Under the delayed model (issue #86) every
    /// range-function output retains it; the verdict is `drop_name`.
    metric_name: Option<String>,
    /// The input's own delayed verdict (`false` for a selector source —
    /// a fetched series always keeps its name; the materialized inner
    /// series' latched verdict for a subquery source). The consuming arm
    /// ORs the function's verdict in (upstream
    /// `seriesDropName = dropName || inputDropName`, engine.go:2281).
    drop_name: bool,
    samples: Vec<Sample>,
    /// Issue #155: the optional start-timestamp channel, windowed in
    /// lockstep with `samples` (aligned 1:1, `0` = unset). `Some` only
    /// for a Selector source whose fetched series carried
    /// [`FetchedSeries::start_ts`]; a Subquery source always cuts it.
    /// Consumed only by the `RangeFn` (rate/irate/increase) and
    /// `OverTime::Resets` arms — the `engine.go:2243-2246` gate.
    st: Option<Vec<i64>>,
}

/// The extended range-selector mode (issue #150) of a [`WindowedSource`],
/// derived from the selector's flags. `Plain` for every subquery source
/// (the parser rejects a modifier on a subquery) and every unmodified
/// selector. `Anchored`/`Smoothed` widen only the sample slice — the
/// `(lower_excl, upper_incl]` bounds stay ORIGINAL so the extended ports
/// have the correct boundary math and `isRate` divisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeMode {
    Plain,
    Anchored,
    Smoothed,
}

/// A range source resolved at one evaluation step: the `(lower_excl,
/// upper_incl]` window (`upper_incl` = the effective evaluation time) and
/// every series' windowed, non-stale samples — the shared input for all
/// four range-function arms (issue #83's `eval_range_source` helper).
struct WindowedSource {
    range_ms: i64,
    lower_excl: i64,
    upper_incl: i64,
    /// Issue #150: the extended range-selector mode. `Plain` unless the
    /// underlying selector carried `anchored`/`smoothed`.
    mode: RangeMode,
    series: Vec<WindowedSeries>,
}

fn windowed_range_source(
    source: &RangeSource,
    selectors: &[SelectorSpec],
    data: &EvalData<'_>,
    subqueries: &SubqueryCache,
    t_ms: i64,
    lookback_ms: i64,
) -> Result<WindowedSource, PromqlError> {
    let source_view = match source {
        RangeSource::Selector(id) => {
            let sel = &selectors[*id];
            let eff_t = sel.at_ms.unwrap_or(t_ms) - sel.offset_ms;
            let range_ms = sel
                .range_ms
                .expect("plan() only ever builds a range source over a matrix selector");
            let lower_excl = eff_t - range_ms;
            // Issue #150: the ORIGINAL `(lower_excl, eff_t]` bounds stay
            // fixed; the extended modes only widen the fetched sample slice
            // (anchored: `(lower−lb, upper]`; smoothed: `(lower−lb,
            // upper+lb]`) — engine.go:1007-1021's per-step window extension.
            let mode = if sel.anchored {
                RangeMode::Anchored
            } else if sel.smoothed {
                RangeMode::Smoothed
            } else {
                RangeMode::Plain
            };
            let (slice_lower, slice_upper) = match mode {
                RangeMode::Plain => (lower_excl, eff_t),
                RangeMode::Anchored => (lower_excl - lookback_ms, eff_t),
                RangeMode::Smoothed => (lower_excl - lookback_ms, eff_t + lookback_ms),
            };
            let series = data
                .get(*id)
                .iter()
                .map(|s| {
                    // Issue #155: a series carrying the ST channel windows
                    // sample/ST PAIRS (the stale filter drops the paired
                    // ST too); the overwhelmingly common `None` case takes
                    // the untouched pre-change path.
                    let (samples, st) = match &s.start_ts {
                        None => (
                            windowed_non_stale(&s.samples, slice_lower, slice_upper),
                            None,
                        ),
                        Some(st) => {
                            let (samples, st) = windowed_non_stale_with_st(
                                &s.samples,
                                st,
                                slice_lower,
                                slice_upper,
                            );
                            (samples, Some(st))
                        }
                    };
                    WindowedSeries {
                        labels: s.labels.clone(),
                        // Per-series name (issue #85), with the same
                        // concrete-name-only fallback the `Selector` arm
                        // documents.
                        metric_name: s.metric_name.clone().or_else(|| sel.metric_name.clone()),
                        drop_name: false,
                        samples,
                        st,
                    }
                })
                .collect();
            WindowedSource {
                range_ms,
                lower_excl,
                upper_incl: eff_t,
                mode,
                series,
            }
        }
        RangeSource::Subquery(sq) => {
            let materialized = subqueries
                .get(&(sq.as_ref() as *const _))
                // Documented invariant: `prepare_subqueries` walks the
                // exact same tree `eval_step` walks and always inserts an
                // entry (empty grids included) before stepping begins.
                .expect("prepare_subqueries materializes every subquery before stepping");
            let eff_t = sq.at_ms.unwrap_or(t_ms) - sq.offset_ms;
            let lower_excl = eff_t - sq.range_ms;
            // Slice this step's (mint, maxt] window from the shared
            // materialized grid — never re-evaluate the inner expression.
            // Materialized values are computed, never stale-marked, so a
            // plain slice (no stale filter) is exact.
            let series = materialized
                .series
                .iter()
                .map(|s| {
                    let start = s.samples.partition_point(|p| p.t_ms <= lower_excl);
                    let end = s.samples.partition_point(|p| p.t_ms <= eff_t);
                    WindowedSeries {
                        labels: s.labels.clone(),
                        metric_name: s.metric_name.clone(),
                        drop_name: s.drop_name,
                        samples: s.samples[start..end].to_vec(),
                        // Issue #155: subqueries CUT start-timestamp
                        // propagation — materialized inner series never
                        // carry the channel (upstream: a materialized
                        // `storageSeriesIterator.AtST` returns 0,
                        // `promql/value.go:528-530`; witnessed by
                        // `start_timestamps.test:35-40,:90-96`).
                        st: None,
                    }
                })
                .collect();
            WindowedSource {
                range_ms: sq.range_ms,
                lower_excl,
                upper_incl: eff_t,
                // A subquery source can never carry a modifier (the parser
                // rejects `foo[5m:1m] anchored`).
                mode: RangeMode::Plain,
                series,
            }
        }
    };
    // M7-A5b-ii/iii: the blanket "reject any histogram in the window"
    // guard that used to live here (M7-A5a) is gone — every `RangeFn`/
    // `OverTimeFn`/`OverTimeParamFn` now has its real pinned histogram
    // disposition (`hist_range_fns`); no 422 histogram guard remains
    // anywhere in the evaluator.
    Ok(source_view)
}

/// Slices `samples` (sorted ascending) to the left-open right-closed
/// window `(lower_excl, upper_incl]` and drops any stale-NaN-marked
/// sample — the shared windowing step for both range functions and
/// `*_over_time`.
fn windowed_non_stale(samples: &[Sample], lower_excl: i64, upper_incl: i64) -> Vec<Sample> {
    let start = samples.partition_point(|s| s.t_ms <= lower_excl);
    let end = samples.partition_point(|s| s.t_ms <= upper_incl);
    // M7-A5a: drop BOTH float and histogram stale markers (`Sample::is_stale`
    // folds the `sum`-bit case). Float-only samples take the identical
    // `v`-bit path as before, so float range output stays byte-identical.
    samples[start..end]
        .iter()
        .filter(|s| !s.is_stale())
        .cloned()
        .collect()
}

/// [`windowed_non_stale`]'s paired variant for a series carrying the
/// start-timestamp channel (issue #155): identical `(lower_excl,
/// upper_incl]` slicing and stale filtering, moving `(sample, st)` pairs
/// TOGETHER so the aligned-channel invariant survives windowing (a
/// desync would shift every reset placement by one).
fn windowed_non_stale_with_st(
    samples: &[Sample],
    st: &[i64],
    lower_excl: i64,
    upper_incl: i64,
) -> (Vec<Sample>, Vec<i64>) {
    debug_assert_eq!(
        samples.len(),
        st.len(),
        "start_ts must be aligned 1:1 with samples"
    );
    let start = samples.partition_point(|s| s.t_ms <= lower_excl);
    let end = samples.partition_point(|s| s.t_ms <= upper_incl);
    let mut out_samples = Vec::with_capacity(end - start);
    let mut out_st = Vec::with_capacity(end - start);
    for (s, &s_st) in samples[start..end].iter().zip(&st[start..end]) {
        if !s.is_stale() {
            out_samples.push(s.clone());
            out_st.push(s_st);
        }
    }
    (out_samples, out_st)
}

/// Issue #150: `extrapolatedRate`'s anchored/smoothed branch
/// (functions.go:459-470) — the mixed / all-histogram / all-float dispatch
/// over the already extended-windowed `samples`. Only Rate/Increase/Delta
/// reach here (the plan-time allow-list rejects every other function on an
/// anchored/smoothed selector); the `(is_counter, is_rate)` pair follows
/// `extrapolatedRate`'s own callers.
#[allow(clippy::too_many_arguments)]
fn eval_extended_range_fn(
    func: crate::plan::RangeFn,
    samples: &[Sample],
    range_ms: i64,
    range_start_ms: i64,
    range_end_ms: i64,
    smoothed: bool,
    metric_name: &str,
    annos: &mut Annotations,
) -> Option<hist_range_fns::RangeValue> {
    use crate::plan::RangeFn;
    let (is_counter, is_rate) = match func {
        RangeFn::Rate => (true, true),
        RangeFn::Increase => (true, false),
        RangeFn::Delta => (false, false),
        RangeFn::Irate => unreachable!("irate is not in the anchored/smoothed allow-list"),
    };
    let hist_count = samples.iter().filter(|s| s.h.is_some()).count();
    if hist_count > 0 && hist_count < samples.len() {
        annos.warning(crate::annotations::messages::mixed_floats_histograms_warning(metric_name));
        return None;
    }
    if hist_count > 0 {
        extended::extended_histogram_rate(
            samples,
            range_ms,
            range_start_ms,
            range_end_ms,
            smoothed,
            is_counter,
            is_rate,
            metric_name,
            annos,
        )
    } else if !samples.is_empty() {
        extended::extended_rate(
            samples,
            range_ms,
            range_start_ms,
            range_end_ms,
            smoothed,
            is_counter,
            is_rate,
        )
        .map(hist_range_fns::RangeValue::Float)
    } else {
        None
    }
}

/// Converts a [`hist_range_fns::RangeValue`] into the `(v, h)` pair
/// [`InstantSample`] carries — `v: 0.0` for a histogram result (matching
/// [`crate::value::Sample::hist`]'s convention: `h` disambiguates).
fn range_value_to_sample_fields(
    value: hist_range_fns::RangeValue,
) -> (f64, Option<Box<pulsus_model::FloatHistogram>>) {
    match value {
        hist_range_fns::RangeValue::Float(v) => (v, None),
        hist_range_fns::RangeValue::Histogram(h) => (0.0, Some(Box::new(h))),
    }
}

/// Converts a [`hist_range_fns::OverTimeValue`] into the `(v, h)` pair
/// [`InstantSample`] carries — see [`range_value_to_sample_fields`].
fn over_time_value_to_sample_fields(
    value: hist_range_fns::OverTimeValue,
) -> (f64, Option<Box<pulsus_model::FloatHistogram>>) {
    match value {
        hist_range_fns::OverTimeValue::Float(v) => (v, None),
        hist_range_fns::OverTimeValue::Histogram(h) => (0.0, Some(Box::new(h))),
    }
}

// M7-A5b-iii: the A5a `histogram_unsupported`/`reject_histogram_vector`/
// `reject_histogram_samples` guard trio is fully retired — every derived
// operation now carries its pinned native-histogram disposition
// (compute / drop+info / passthrough), `predict_linear`/
// `double_exponential_smoothing` included
// (`hist_range_fns::eval_over_time_param_hist`, the last holdout).

/// M7-A5b-i port of upstream `EvalNodeHelper.resetHistograms`
/// (`engine.go:1311-1373`, pinned `40af9c2`): partitions
/// `histogram_quantile`/`histogram_fraction`'s input vector into
/// native-histogram samples and classic `le`-bucket groups, then applies
/// the native/classic same-timestamp conflict filter — an identity
/// carrying BOTH classic buckets and a native histogram evaluates
/// NEITHER, and `NewMixedClassicNativeHistogramsWarning` fires once per
/// conflicting native sample (`engine.go:1358-1369`: the classic group is
/// deleted and the native sample's `H` nil'd, so both are dropped).
/// Upstream keys the native side on the FULL label set (`Metric.Bytes`)
/// and the classic side on labels-without-`le`
/// (`BytesWithoutLabels(le)`); mirrored as `(metric_name, labels)` vs
/// `(metric_name, labels.without("le"))` — same values under the
/// name-outside-`Labels` split.
///
/// A classic (float) sample whose `le` label is missing or unparsable is
/// SKIPPED with a [`crate::annotations::messages::bad_bucket_label_warning`]
/// (`#124` review finding 4) — matching upstream's `resetHistograms`
/// exactly (`engine.go:1331-1341`): the bucket is dropped from its group,
/// not the whole query rejected. This corrects a pre-A5b (M2-era)
/// divergence that used to hard-error here.
fn partition_histogram_inputs(
    v: Vec<InstantSample>,
    caches: &EvalCaches,
) -> (
    Vec<InstantSample>,
    HashMap<SeriesIdentity, Vec<functions::Bucket>>,
) {
    let le_key = "le".to_string();
    let mut native: Vec<InstantSample> = Vec::new();
    let mut groups: HashMap<SeriesIdentity, Vec<functions::Bucket>> = HashMap::new();
    for s in v {
        if s.h.is_some() {
            native.push(s);
            continue;
        }
        // Group key = the FULL retained identity minus `le` (issue #86):
        // upstream's bucket signature is `BytesWithoutLabels(le)` over the
        // whole metric — under the delayed model that still includes the
        // retained `__name__` (`excludedLabels` at the pin is `le` alone,
        // quantile.go:51), so two bucket families sharing non-name labels
        // never merge; the output retains the group's name with
        // `drop_name: true`.
        let le_str = s.labels.get("le").unwrap_or("");
        let le: Result<f64, _> = le_str.parse();
        let Ok(le) = le else {
            caches.annotations.borrow_mut().warning(
                crate::annotations::messages::bad_bucket_label_warning(
                    s.metric_name.as_deref().unwrap_or(""),
                    le_str,
                ),
            );
            continue;
        };
        let key = (
            s.metric_name.clone(),
            s.labels.without(std::slice::from_ref(&le_key)),
        );
        groups
            .entry(key)
            .or_default()
            .push(functions::Bucket { le, count: s.v });
    }
    // The conflict filter (`engine.go:1354-1371`): drop BOTH sides + warn.
    native.retain(|s| {
        let key = (s.metric_name.clone(), s.labels.clone());
        if groups.get(&key).is_some_and(|b| !b.is_empty()) {
            caches.annotations.borrow_mut().warning(
                crate::annotations::messages::mixed_classic_native_histograms_warning(
                    s.metric_name.as_deref().unwrap_or(""),
                ),
            );
            groups.remove(&key);
            false
        } else {
            true
        }
    });
    (native, groups)
}

/// Issue #153: port of `labels.FormatOpenMetricsFloat`
/// (`model/labels/float.go:35-60`, pinned `40af9c2`) — Go `%g` shortest
/// formatting with `".0"` appended when the result carries neither `.`
/// nor `e`. The hardcoded cases run FIRST, exactly as the pin's switch:
/// `f == 0` catches `-0.0` too (Go `-0.0 == 0` is true), which is
/// load-bearing — falling through would render `-0.0` as `"-0.0"` via
/// [`crate::annotations::go_float::format_g`]'s `"-0"`.
fn format_open_metrics_float(f: f64) -> String {
    if f == 1.0 {
        return "1.0".to_string();
    }
    if f == 0.0 {
        return "0.0".to_string();
    }
    if f == -1.0 {
        return "-1.0".to_string();
    }
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    let s = crate::annotations::go_float::format_g(f);
    if s.contains('.') || s.contains('e') {
        s
    } else {
        format!("{s}.0")
    }
}

/// Issue #153: stamps the quantile label onto one output sample's
/// identity — `Labels::set` overwrites an existing entry (upstream
/// `getOrCreateLblsWithQuantile`'s `labels.Builder.Set`); `"__name__"`
/// routes to the metric-name channel instead (`Labels` carries the name
/// outside the map by construction — the `count_values` precedent). The
/// caller keeps `drop_name: true`, so the `__name__` route's net effect
/// equals the pin's Set-then-DropName.
fn set_quantile_label(
    mut labels: Labels,
    metric_name: Option<String>,
    label: &str,
    q_str: &str,
) -> (Labels, Option<String>) {
    if label == "__name__" {
        (labels, Some(q_str.to_string()))
    } else {
        labels.set(label.to_string(), q_str.to_string());
        (labels, metric_name)
    }
}

fn eval_step(
    expr: &PlanExpr,
    selectors: &[SelectorSpec],
    data: &EvalData<'_>,
    t_ms: i64,
    lookback_ms: i64,
    caches: &EvalCaches,
) -> Result<StepValue, PromqlError> {
    // Issue #88: a step-invariant cache root short-circuits to its
    // once-evaluated value — the copy half of once-and-copy (the range
    // accumulator/consuming arm stamps the step time, exactly like
    // upstream's duplicate-with-changed-timestamps). The `is_empty()`
    // guard keeps the common no-`@` query path at one bool check per
    // node, never a hash probe.
    if !caches.step_invariant_marked.is_empty() {
        let addr = expr as *const PlanExpr;
        if let Some(cached) = caches.step_invariant.get(&addr) {
            return Ok(cached.clone());
        }
        // A MARKED root reaching past the cache probe is being genuinely
        // evaluated. That happens exactly once — the prep-pass eval in
        // `prepare_step_invariant`, which marks before evaluating so
        // this instrument observes it. Any additional increment is a
        // per-step cache-bypass regression (review round 1, finding 1),
        // tripping the Tier-1 `== 1` gate.
        if caches.step_invariant_marked.contains(&addr) {
            caches
                .step_invariant_evals
                .set(caches.step_invariant_evals.get() + 1);
        }
    }
    match expr {
        PlanExpr::Scalar(v) => Ok(StepValue::Scalar(*v)),

        // Issue #86 (M6-08d): a top-level string-literal query — the
        // literal's value verbatim; the wire timestamp is stamped by the
        // response encoder (the `Scalar`/`at_ms` precedent).
        PlanExpr::StringLiteral(s) => Ok(StepValue::String(s.clone())),

        // Issue #37: a bare selector returns the **verbatim value of an
        // existing series** — Prometheus keeps `__name__` here (captured:
        // `query.name_selector_keeps_get.json`; PROVENANCE.md's
        // "`__name__` keep/drop rule" table).
        //
        // Issue #85 (M6-08c): the name comes from the fetched series' own
        // `FetchedSeries::metric_name` channel — a matcher-only/
        // regex-`__name__` selector (`sel.metric_name: None`) matches
        // series across metrics, and each output element must carry its
        // own series' real name. The fetch layer (live:
        // `pulsus-read::metrics::exec`'s per-metric cache resolution;
        // hermetic: the corpus test store) owns both the
        // `sel.name_matchers` filter and the per-series name; the
        // evaluator never re-derives either. The `or_else` fallback to
        // `sel.metric_name` fires only for a **concrete-name** selector
        // whose fetch left the per-series channel empty — sound by the
        // fetch contract (every series of a `Some(name)` selector was
        // fetched under `PREWHERE metric_name = name` and is provably
        // that metric); it can never resurrect the pre-#85 single-name
        // synthesis for a multi-metric selector, whose spec name is
        // `None` by construction.
        PlanExpr::Selector(id) => {
            let sel = &selectors[*id];
            // Issue #83: an own `@` fixes the evaluation time (offset
            // applies relative to it) — step-invariant by construction.
            let eff_t = sel.at_ms.unwrap_or(t_ms) - sel.offset_ms;
            let mut out = Vec::new();
            // Issue #150: a `smoothed` instant selector interpolates over the
            // `(eff_t−lb, eff_t+lb]` window per step (`smoothSeries`); the
            // metric name is KEPT (it is still a selection). An `anchored`
            // instant selector is a no-op upstream (engine.go only widens for
            // `Smoothed` in the instant arm), so it falls through unchanged.
            if sel.smoothed {
                for series in data.get(*id) {
                    let metric_name = series
                        .metric_name
                        .clone()
                        .or_else(|| sel.metric_name.clone());
                    let smoothed = {
                        let mut annos = caches.annotations.borrow_mut();
                        extended::smoothed_instant(
                            &series.samples,
                            eff_t,
                            lookback_ms,
                            metric_name.as_deref().unwrap_or(""),
                            &mut annos,
                        )
                    };
                    if let Some((v, h)) = smoothed {
                        out.push(InstantSample {
                            labels: series.labels.clone(),
                            metric_name,
                            drop_name: false,
                            t_ms,
                            v,
                            h,
                        });
                    }
                }
                return Ok(StepValue::Vector(out));
            }
            for series in data.get(*id) {
                if let Some(sample) = staleness::instant_value(&series.samples, eff_t, lookback_ms)
                {
                    // M7-A5a: a bare selector is the one selection site that
                    // carries the native-histogram channel through to the
                    // vector; every derived construct below produces floats
                    // (`h: None`) until the A5b function set lands.
                    out.push(InstantSample {
                        labels: series.labels.clone(),
                        metric_name: series
                            .metric_name
                            .clone()
                            .or_else(|| sel.metric_name.clone()),
                        drop_name: false,
                        t_ms,
                        v: sample.v,
                        h: sample.h,
                    });
                }
            }
            Ok(StepValue::Vector(out))
        }

        // Issue #37: `rate`/`irate`/`increase`/`delta` **compute** a new
        // value from the windowed samples — a name-DROPPING class. Under
        // the delayed model (issue #86) the input's name is RETAINED with
        // `drop_name: true` (upstream funcCall wrapper: `seriesDropName =
        // dropName || inputDropName`, engine.go:2281 — `dropName` is true
        // for every range function but last/first_over_time), so a
        // downstream `label_replace(rate(…), …, "__name__", …)` still
        // reads it; the terminal cleanup nulls it. Issue #83: the source
        // may be a subquery — `windowed_range_source` slices its step
        // window from the once-materialized union grid.
        // M7-A5b-ii: histogram-aware — `hist_range_fns::eval_range_fn_hist`
        // dispatches float-only windows to the byte-unchanged
        // `functions::eval_range_fn` internally and adds the native-
        // histogram `rate`/`increase`/`delta`/`irate` semantics.
        PlanExpr::RangeFn { func, source } => {
            let src = windowed_range_source(
                source,
                selectors,
                data,
                &caches.subqueries,
                t_ms,
                lookback_ms,
            )?;
            let mut out = Vec::new();
            for series in src.series {
                let metric_name = series.metric_name.as_deref().unwrap_or("");
                // Issue #155: the four-function gate (engine.go:2243-2246)
                // — only `rate`/`irate`/`increase` consume the ST channel;
                // `delta` is outside the name list and gets `None`.
                let st = {
                    use crate::plan::RangeFn;
                    matches!(func, RangeFn::Rate | RangeFn::Irate | RangeFn::Increase)
                        .then(|| series.st.as_deref())
                        .flatten()
                };
                let result = {
                    let mut annos = caches.annotations.borrow_mut();
                    if src.mode == RangeMode::Plain {
                        hist_range_fns::eval_range_fn_hist(
                            *func,
                            &series.samples,
                            hist_range_fns::RangeWindow {
                                range_ms: src.range_ms,
                                start_ms: src.lower_excl,
                                end_ms: src.upper_incl,
                            },
                            st,
                            metric_name,
                            &mut annos,
                        )
                    } else {
                        // Issue #150: `extrapolatedRate`'s extended branch
                        // (functions.go:459-470). Plan gating guarantees only
                        // Rate/Increase/Delta reach an anchored/smoothed
                        // selector.
                        eval_extended_range_fn(
                            *func,
                            &series.samples,
                            src.range_ms,
                            src.lower_excl,
                            src.upper_incl,
                            src.mode == RangeMode::Smoothed,
                            metric_name,
                            &mut annos,
                        )
                    }
                };
                if let Some(value) = result {
                    let (v, h) = range_value_to_sample_fields(value);
                    out.push(InstantSample {
                        labels: series.labels,
                        metric_name: series.metric_name,
                        drop_name: true,
                        t_ms,
                        v,
                        h,
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
        // The kept name is the fetched series' own per-row
        // `FetchedSeries::metric_name` (issue #85 — see the `Selector`
        // arm), threaded through `WindowedSeries::metric_name`.
        PlanExpr::OverTime { func, source } => {
            let src = windowed_range_source(
                source,
                selectors,
                data,
                &caches.subqueries,
                t_ms,
                lookback_ms,
            )?;
            let keeps_name = matches!(func, OverTimeFn::Last | OverTimeFn::First);
            let mut out = Vec::new();
            for series in src.series {
                // M7-A5b-iii: `sum_over_time`/`avg_over_time`'s histogram
                // (KahanAdd) path lands in `hist_range_fns::eval_over_time_hist`
                // alongside every other `OverTimeFn` — no more A5a/A5b-ii
                // early guard.
                let metric_name = series.metric_name.as_deref().unwrap_or("");
                // Issue #150: an anchored selector (only `resets`/`changes`
                // reach here — the allow-list) prepends the anchor sample and
                // drops everything before it (`pickFirstSampleIndices`); no
                // anchor after the range start ⇒ no output for the series.
                let anchored_samples = if src.mode == RangeMode::Anchored {
                    match extended::anchor_trim(&series.samples, src.lower_excl) {
                        Some(s) => Some(s),
                        None => continue,
                    }
                } else {
                    None
                };
                let windowed = anchored_samples.unwrap_or(&series.samples);
                // Issue #155: only `resets` is in the four-function gate
                // (engine.go:2243-2246) — every other `OverTimeFn` ignores
                // the ST channel. `anchor_trim` keeps a SUFFIX of the
                // windowed samples, so the paired channel is sliced by the
                // same offset to preserve the 1:1 alignment.
                let st = matches!(func, OverTimeFn::Resets)
                    .then(|| series.st.as_deref())
                    .flatten()
                    .map(|st| &st[series.samples.len() - windowed.len()..]);
                let result = {
                    let mut annos = caches.annotations.borrow_mut();
                    hist_range_fns::eval_over_time_hist(
                        *func,
                        windowed,
                        st,
                        metric_name,
                        &mut annos,
                    )
                };
                if let Some(value) = result {
                    let (v, h) = over_time_value_to_sample_fields(value);
                    out.push(InstantSample {
                        labels: series.labels,
                        // Retained name always (issue #86: selector
                        // source — the fetched series' own name, issue
                        // #85; subquery source — the materialized inner
                        // series' name); the verdict is the upstream OR:
                        // the function's own drop (everything but
                        // last/first_over_time) OR the input's (a
                        // subquery whose inner expression dropped —
                        // `name_label_dropping.test:50`,
                        // `last_over_time(abs(m)[10m:])`).
                        metric_name: series.metric_name,
                        drop_name: !keeps_name || series.drop_name,
                        t_ms,
                        v,
                        h,
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
        PlanExpr::OverTimeParam { func, source, args } => {
            let mut scalars = Vec::with_capacity(args.len());
            for a in args {
                let StepValue::Scalar(s) =
                    eval_step(a, selectors, data, t_ms, lookback_ms, caches)?
                else {
                    return Err(PromqlError::Unsupported {
                        construct: format!("{func:?} with a non-scalar parameter argument"),
                    });
                };
                scalars.push(s);
            }
            let src = windowed_range_source(
                source,
                selectors,
                data,
                &caches.subqueries,
                t_ms,
                lookback_ms,
            )?;
            let mut out = Vec::new();
            for series in src.series {
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
                // It also keeps the at_modifier.test:227-255 empty-@
                // subquery cases empty-not-panicking (issue #83).
                if series.samples.is_empty() {
                    continue;
                }
                // M7-A5b-ii: `quantile_over_time` gets the DROP-set
                // histogram disposition (silent on a histogram-only
                // window, `HistogramIgnoredInMixedRangeInfo` on a mixed
                // one). M7-A5b-iii: `predict_linear`/
                // `double_exponential_smoothing` get their pinned
                // float-subset disposition too (`funcPredictLinear`/
                // `funcDoubleExponentialSmoothing` — compute on the float
                // subset, info on a mixed window, empty below 2 floats;
                // the LAST A5a 422 histogram guard is gone).
                let metric_name = series.metric_name.as_deref().unwrap_or("");
                let v = if *func == OverTimeParamFn::Quantile {
                    let mut annos = caches.annotations.borrow_mut();
                    hist_range_fns::eval_quantile_over_time_hist(
                        scalars[0],
                        &series.samples,
                        metric_name,
                        &mut annos,
                    )
                } else {
                    // `predict_linear`'s regression intercept stays the
                    // outer evaluation STEP time `t_ms` — never the
                    // `@`/offset-shifted window edge (the #67
                    // adjudication; the offset golden lives in
                    // proof/m6_08a_at_subquery.test).
                    let mut annos = caches.annotations.borrow_mut();
                    hist_range_fns::eval_over_time_param_hist(
                        *func,
                        &series.samples,
                        &scalars,
                        t_ms,
                        metric_name,
                        &mut annos,
                    )?
                };
                if let Some(v) = v {
                    out.push(InstantSample {
                        labels: series.labels,
                        // Name-dropping class: retained name + verdict
                        // (issue #86, delayed model).
                        metric_name: series.metric_name,
                        drop_name: true,
                        t_ms,
                        v,
                        h: None,
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
        PlanExpr::AbsentOverTime { source } => {
            let src = windowed_range_source(
                source,
                selectors,
                data,
                &caches.subqueries,
                t_ms,
                lookback_ms,
            )?;
            let present = src.series.iter().any(|s| !s.samples.is_empty());
            if present {
                return Ok(StepValue::Vector(Vec::new()));
            }
            // Selector source: labels synthesized from the matchers;
            // subquery source (issue #83): the empty label set (upstream's
            // createLabelsForAbsentFunction walk only applies to a
            // vector-selector argument).
            let labels = match source {
                RangeSource::Selector(id) => labels::labels_for_absent(&selectors[*id].matchers),
                RangeSource::Subquery(_) => Labels::default(),
            };
            Ok(StepValue::Vector(vec![InstantSample {
                labels,
                metric_name: None,
                drop_name: false,
                t_ms,
                v: 1.0,
                h: None,
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
            let StepValue::Vector(v) = eval_step(arg, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "absent() over a scalar expression".to_string(),
                });
            };
            // M7-A5b-iii: `funcAbsent` is type-agnostic (`len(vec) > 0`,
            // `functions.go:1643-1651`) — a histogram-valued vector still
            // suppresses `absent()`'s synthetic output.
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
                drop_name: false,
                t_ms,
                v: 1.0,
                h: None,
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
            let StepValue::Vector(mut v) =
                eval_step(arg, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "sort()/sort_desc() over a scalar expression".to_string(),
                });
            };
            // M7-A5b-iii: `funcSort`/`funcSortDesc` DROP histogram samples
            // (`filterFloats`, `functions.go:964-971`) — never an error.
            v.retain(|s| s.h.is_none());
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
            let StepValue::Vector(mut v) =
                eval_step(arg, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "sort_by_label()/sort_by_label_desc() over a scalar expression"
                        .to_string(),
                });
            };
            // M7-A5b-iii: sorts by LABEL only — value-agnostic, so a
            // histogram sample passes through unchanged (no guard).
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
            let StepValue::Vector(v) = eval_step(arg, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "label_replace() over a scalar expression".to_string(),
                });
            };
            // M7-A5b-iii: label rewrites are value-agnostic — a histogram
            // sample's `h` passes through `label_replace_vector` untouched
            // (no guard).
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
            let StepValue::Vector(v) = eval_step(arg, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "label_join() over a scalar expression".to_string(),
                });
            };
            // M7-A5b-iii: value-agnostic — no guard (see `LabelReplace`).
            Ok(StepValue::Vector(labels::label_join_vector(
                v, dst, separator, src_labels,
            )?))
        }

        // Issue #37 (M7-A5b-i extends to the native form): `histogram_quantile`
        // **computes** a new value from the bucket series — Prometheus drops
        // `__name__` (interactively verified: `histogram_quantile(0.5,
        // x_bucket_histogram_bucket)` -> `"metric":{}`, PROVENANCE.md's
        // table). Dispatches per sample via [`partition_histogram_inputs`]
        // (the `resetHistograms` port, incl. the native/classic conflict
        // filter): a histogram-valued sample (`h.is_some()`) takes the
        // native `quantile.go` `HistogramQuantile` path (no grouping — one
        // histogram per sample, unlike classic buckets); a float sample
        // carrying an `le` label takes the existing classic
        // `bucketQuantile` path (grouped by identity minus `le`,
        // `funcHistogramQuantile`, `functions.go`), which since M7-A5b-i
        // also reports upstream's forced-monotonicity info.
        PlanExpr::HistogramQuantile { quantile, expr } => {
            let StepValue::Scalar(q) =
                eval_step(quantile, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "histogram_quantile's first argument must evaluate to a scalar"
                        .to_string(),
                });
            };
            let StepValue::Vector(v) = eval_step(expr, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "histogram_quantile's second argument must evaluate to a vector"
                        .to_string(),
                });
            };

            if q.is_nan() || !(0.0..=1.0).contains(&q) {
                caches
                    .annotations
                    .borrow_mut()
                    .warning(crate::annotations::messages::invalid_quantile_warning(q));
            }

            let (native, mut groups) = partition_histogram_inputs(v, caches);
            let mut out = Vec::new();
            for s in native {
                let Some(h) = &s.h else { continue };
                let metric_name = s.metric_name.as_deref().unwrap_or("");
                let qv = {
                    let mut annos = caches.annotations.borrow_mut();
                    histogram_fns::histogram_quantile(q, h, metric_name, &mut annos)
                };
                out.push(InstantSample {
                    labels: s.labels,
                    metric_name: s.metric_name,
                    drop_name: true,
                    t_ms,
                    v: qv,
                    h: None,
                });
            }

            let mut keys: Vec<SeriesIdentity> = groups.keys().cloned().collect();
            keys.sort();
            for key in keys {
                let buckets = groups.remove(&key).expect("key came from groups.keys()");
                let (v, report) =
                    functions::histogram_quantile_with_monotonicity_report(q, buckets)?;
                let (metric_name, labels) = key;
                if report.forced {
                    // `funcHistogramQuantile` (`functions.go:2111-2117`):
                    // BucketQuantile forced monotonicity — info, with the
                    // group's retained metric name (the pin passes it
                    // under `enableDelayedNameRemoval`, pulsus's model).
                    // Repeat firings for the same metric name — other
                    // groups this step, other steps of a range query —
                    // MERGE into one widened info (the pin's per-step
                    // `warnings.Merge(ws)` in `rangeEval` runs
                    // `annoError.Merge` on the key collision).
                    caches.annotations.borrow_mut().forced_monotonicity_info(
                        crate::annotations::messages::histogram_quantile_forced_monotonicity_info(
                            metric_name.as_deref().unwrap_or(""),
                        ),
                        crate::annotations::ForcedMonotonicityDetail::single(
                            t_ms,
                            report.min_bucket,
                            report.max_bucket,
                            report.max_diff,
                        ),
                    );
                }
                out.push(InstantSample {
                    labels,
                    metric_name,
                    drop_name: true,
                    t_ms,
                    v,
                    h: None,
                });
            }
            Ok(StepValue::Vector(out))
        }

        // Issue #153: `histogram_quantiles(v, label, q0, qs…)` — the pin's
        // `funcHistogramQuantiles` (`functions.go:2141-2214`). Order
        // matters, mirroring the pin: every quantile is validated (per-
        // value invalid-quantile warn) BEFORE the input is partitioned;
        // the partition runs ONCE (the single `resetHistograms` at
        // `functions.go:2168` — mixed-histograms / bad-`le` warns fire
        // once, not per quantile); then quantiles loop OUTER over the
        // natives-then-classics inner walk, reusing the singular arm's
        // primitives verbatim. Each output sample sets `label` to the
        // OpenMetrics-formatted quantile — overwriting an existing value
        // (upstream `labels.Builder.Set`); `label == "__name__"` routes to
        // the metric-name channel instead (`Labels` never carries the
        // name), with the net effect equal to the pin's Set-then-DropName
        // since `drop_name` stays true. Duplicate quantile values re-emit
        // (the pin does not de-duplicate).
        PlanExpr::HistogramQuantiles {
            expr,
            label,
            quantiles,
        } => {
            let StepValue::Vector(v) = eval_step(expr, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "histogram_quantiles' first argument must evaluate to a vector"
                        .to_string(),
                });
            };
            let mut qs: Vec<f64> = Vec::with_capacity(quantiles.len());
            for q_expr in quantiles {
                let StepValue::Scalar(q) =
                    eval_step(q_expr, selectors, data, t_ms, lookback_ms, caches)?
                else {
                    return Err(PromqlError::Unsupported {
                        construct:
                            "histogram_quantiles' quantile arguments must evaluate to scalars"
                                .to_string(),
                    });
                };
                // `validateQuantile` per argument (`functions.go:2158`).
                if q.is_nan() || !(0.0..=1.0).contains(&q) {
                    caches
                        .annotations
                        .borrow_mut()
                        .warning(crate::annotations::messages::invalid_quantile_warning(q));
                }
                qs.push(q);
            }

            let (native, groups) = partition_histogram_inputs(v, caches);
            // Sort the classic group keys once (the singular arm's
            // determinism rule — upstream iterates a Go map).
            let mut keys: Vec<SeriesIdentity> = groups.keys().cloned().collect();
            keys.sort();

            let mut out = Vec::new();
            for &q in &qs {
                let q_str = format_open_metrics_float(q);
                for s in &native {
                    let Some(h) = &s.h else { continue };
                    let metric_name = s.metric_name.as_deref().unwrap_or("");
                    let qv = {
                        let mut annos = caches.annotations.borrow_mut();
                        histogram_fns::histogram_quantile(q, h, metric_name, &mut annos)
                    };
                    let (labels, metric_name) =
                        set_quantile_label(s.labels.clone(), s.metric_name.clone(), label, &q_str);
                    out.push(InstantSample {
                        labels,
                        metric_name,
                        drop_name: true,
                        t_ms,
                        v: qv,
                        h: None,
                    });
                }
                for key in &keys {
                    let buckets = groups.get(key).expect("key came from groups.keys()");
                    let (qv, report) =
                        functions::histogram_quantile_with_monotonicity_report(q, buckets.clone())?;
                    let (metric_name, labels) = key.clone();
                    if report.forced {
                        // Merged exactly as the singular arm merges (see
                        // its comment on the pin's `Merge` collision).
                        caches.annotations.borrow_mut().forced_monotonicity_info(
                            crate::annotations::messages::histogram_quantile_forced_monotonicity_info(
                                metric_name.as_deref().unwrap_or(""),
                            ),
                            crate::annotations::ForcedMonotonicityDetail::single(
                                t_ms,
                                report.min_bucket,
                                report.max_bucket,
                                report.max_diff,
                            ),
                        );
                    }
                    let (labels, metric_name) =
                        set_quantile_label(labels, metric_name, label, &q_str);
                    out.push(InstantSample {
                        labels,
                        metric_name,
                        drop_name: true,
                        t_ms,
                        v: qv,
                        h: None,
                    });
                }
            }
            Ok(StepValue::Vector(out))
        }

        // M7-A5b-i: the five single-vector-argument native-histogram
        // accessors (`histogram_count/_sum/_avg/_stddev/_stdvar`) —
        // `simpleHistogramFunc`/`histogramVariance`, `functions.go`.
        // Float-valued input samples are silently dropped (upstream:
        // "process only histogram samples"); the output drops the metric
        // name.
        PlanExpr::HistogramAccessor { func, arg } => {
            let StepValue::Vector(v) = eval_step(arg, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: format!("{func:?} over a scalar expression"),
                });
            };
            let mut out = Vec::new();
            for s in v {
                let Some(h) = &s.h else { continue };
                let value = match func {
                    HistogramAccessorFn::Count => histogram_fns::histogram_count(h),
                    HistogramAccessorFn::Sum => histogram_fns::histogram_sum(h),
                    HistogramAccessorFn::Avg => histogram_fns::histogram_avg(h),
                    HistogramAccessorFn::StdDev => histogram_fns::histogram_stddev(h),
                    HistogramAccessorFn::StdVar => histogram_fns::histogram_stdvar(h),
                };
                out.push(InstantSample {
                    labels: s.labels,
                    metric_name: s.metric_name,
                    drop_name: true,
                    t_ms,
                    v: value,
                    h: None,
                });
            }
            Ok(StepValue::Vector(out))
        }

        // M7-A5b-i: `histogram_fraction(lower, upper, v)` — dispatches per
        // sample like `HistogramQuantile` (native vs classic `le`-labelled
        // float), `funcHistogramFraction`, `functions.go`.
        PlanExpr::HistogramFraction { lower, upper, expr } => {
            let StepValue::Scalar(lower_v) =
                eval_step(lower, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "histogram_fraction's first argument must evaluate to a scalar"
                        .to_string(),
                });
            };
            let StepValue::Scalar(upper_v) =
                eval_step(upper, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "histogram_fraction's second argument must evaluate to a scalar"
                        .to_string(),
                });
            };
            let StepValue::Vector(v) = eval_step(expr, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "histogram_fraction's third argument must evaluate to a vector"
                        .to_string(),
                });
            };

            let (native, mut groups) = partition_histogram_inputs(v, caches);
            let mut out = Vec::new();
            for s in native {
                let Some(h) = &s.h else { continue };
                let metric_name = s.metric_name.as_deref().unwrap_or("");
                let fv = {
                    let mut annos = caches.annotations.borrow_mut();
                    histogram_fns::histogram_fraction(lower_v, upper_v, h, metric_name, &mut annos)
                };
                out.push(InstantSample {
                    labels: s.labels,
                    metric_name: s.metric_name,
                    drop_name: true,
                    t_ms,
                    v: fv,
                    h: None,
                });
            }

            let mut keys: Vec<SeriesIdentity> = groups.keys().cloned().collect();
            keys.sort();
            for key in keys {
                let buckets = groups.remove(&key).expect("key came from groups.keys()");
                let v = functions::bucket_fraction(lower_v, upper_v, buckets);
                let (metric_name, labels) = key;
                out.push(InstantSample {
                    labels,
                    metric_name,
                    drop_name: true,
                    t_ms,
                    v,
                    h: None,
                });
            }
            Ok(StepValue::Vector(out))
        }

        PlanExpr::Aggregate {
            op,
            // `input`, not `expr`: the arm needs the AGGREGATE node's own
            // address (the outer `expr`) as the `ratio_extrema` key
            // (issue #130 Δ2), which the field binding would shadow.
            expr: input,
            param,
            grouping,
        } => {
            let StepValue::Vector(v) =
                eval_step(input, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "aggregation over a scalar expression".to_string(),
                });
            };
            let param_v = match param {
                Some(p) => {
                    let StepValue::Scalar(k) =
                        eval_step(p, selectors, data, t_ms, lookback_ms, caches)?
                    else {
                        return Err(PromqlError::Unsupported {
                            construct: "aggregation parameter must evaluate to a scalar"
                                .to_string(),
                        });
                    };
                    Some(k)
                }
                None => None,
            };
            // M7-A5b-iii: every `AggOp` now has a defined native-histogram
            // disposition (compute for sum/avg, skip+info for min/max/
            // stddev/stdvar/quantile/topk/bottomk, type-agnostic for
            // count/group) — the A5a blanket reject is gone.
            let out = {
                let mut annos = caches.annotations.borrow_mut();
                aggregation::aggregate(*op, &v, grouping.as_ref(), param_v, &mut annos)?
            };
            // Issue #130 Δ2: fold the RAW (uncapped) ratio into this
            // node's evaluation-wide extrema — AFTER `aggregate` returned
            // `Ok` (a NaN param already errored inside `aggregate_limit`,
            // aborting the query with annotations discarded, the same
            // observable as upstream's up-front `HasAnyNaN` error).
            // Emission happens once per query in `flush_ratio_warnings`,
            // never here (upstream warns from the whole-horizon
            // `params.Max()/Min()`, engine.go:1655-1660 at the pin).
            if *op == AggOp::LimitRatio
                && let Some(r) = param_v
            {
                let key = expr as *const PlanExpr;
                let mut extrema = caches.ratio_extrema.borrow_mut();
                match extrema.iter_mut().find(|(k, _)| *k == key) {
                    Some((_, e)) => {
                        e.max = e.max.max(r);
                        e.min = e.min.min(r);
                    }
                    None => extrema.push((key, RatioExtrema { max: r, min: r })),
                }
            }
            Ok(StepValue::Vector(out))
        }

        // Issue #69 (M6-06): `count_values(label, v)` — the two-channel
        // value-label injection (`__name__` → the metric-name channel)
        // lives entirely in `aggregation::count_values`; the label name
        // was validated at plan time.
        PlanExpr::CountValues {
            label,
            expr,
            grouping,
        } => {
            let StepValue::Vector(v) = eval_step(expr, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "aggregation over a scalar expression".to_string(),
                });
            };
            // M7-A5b-iii: a histogram sample's value-label text is its
            // `String()` rendering (`aggregation::histogram_display_string`).
            Ok(StepValue::Vector(aggregation::count_values(
                &v,
                label,
                grouping.as_ref(),
            )))
        }

        // Issue #65 (M6-02): elementwise math/trig **computes** a new
        // value per sample — `__name__` DROPS (the same class as
        // `rate`/`*_over_time` per the #37 keep/drop table).
        PlanExpr::MathFn {
            func,
            arg,
            scalar_args,
        } => {
            let StepValue::Vector(mut v) =
                eval_step(arg, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: format!("{func:?} over a scalar expression"),
                });
            };
            // M7-A5b-iii: `simpleFloatFunc`'s shared `filterFloats` drops
            // histogram samples (`functions.go:964`) — never an error.
            v.retain(|s| s.h.is_none());
            let mut scalars = Vec::with_capacity(scalar_args.len());
            for sa in scalar_args {
                let StepValue::Scalar(s) =
                    eval_step(sa, selectors, data, t_ms, lookback_ms, caches)?
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
                    // Name-dropping class (issue #86): retained + marked.
                    metric_name: s.metric_name,
                    drop_name: true,
                    t_ms,
                    v: match op {
                        Op::Clamp(min, max) => elementwise::clamp(min, max, s.v),
                        Op::ClampMin(min) => elementwise::clamp_min(min, s.v),
                        Op::ClampMax(max) => elementwise::clamp_max(max, s.v),
                        Op::Round(to_nearest) => elementwise::round(to_nearest, s.v),
                        Op::Unary(func) => elementwise::unary(func, s.v),
                    },
                    h: None,
                })
                .collect();
            Ok(StepValue::Vector(out))
        }

        // Issue #65 (M6-02): scalar→scalar functions.
        PlanExpr::ScalarFn { func, args } => {
            let mut scalars = Vec::with_capacity(args.len());
            for a in args {
                let StepValue::Scalar(s) =
                    eval_step(a, selectors, data, t_ms, lookback_ms, caches)?
                else {
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
                // Scalar-derived (the implicit `vector(time())`): a
                // genuinely nameless element, never a drop verdict.
                metric_name: None,
                drop_name: false,
                t_ms,
                v: datetime::field(*func, t_ms / 1000),
                h: None,
            }])),
            // Vector argument: each element's VALUE is the unix-seconds
            // instant. `to_unix_secs` is the total conversion (plan v2
            // Δ1): NaN/±Inf/|v| >= 2^63 yield a NaN result element (kept,
            // labels minus `__name__`), never a platform-defined cast.
            Some(arg) => {
                let StepValue::Vector(v) =
                    eval_step(arg, selectors, data, t_ms, lookback_ms, caches)?
                else {
                    return Err(PromqlError::Unsupported {
                        construct: format!("{func:?} over a scalar expression"),
                    });
                };
                // M7-A5b-iii: `dateWrapper` ignores a histogram sample
                // silently (`continue`, `functions.go:2482-2484`) — never
                // an error.
                let out = v
                    .into_iter()
                    .filter(|s| s.h.is_none())
                    .map(|s| InstantSample {
                        labels: s.labels,
                        // Name-dropping class (issue #86): retained +
                        // marked.
                        metric_name: s.metric_name,
                        drop_name: true,
                        t_ms,
                        v: datetime::to_unix_secs(s.v)
                            .map(|secs| datetime::field(*func, secs))
                            .unwrap_or(f64::NAN),
                        h: None,
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
                // Issue #83: an own `@` fixes the lookup time, so the
                // returned sample timestamp is constant across steps
                // (at_modifier.test:168/:207/:279 — `timestamp(m @ T)`).
                let eff_t = sel.at_ms.unwrap_or(t_ms) - sel.offset_ms;
                let mut out = Vec::new();
                for series in data.get(*id) {
                    if let Some(sample) =
                        staleness::instant_value(&series.samples, eff_t, lookback_ms)
                    {
                        // M7-A5b-iii: `funcTimestamp` is value-agnostic
                        // (`float64(el.T)/1000`, `functions.go:1838`) — a
                        // histogram sample's timestamp is emitted exactly
                        // like a float sample's.
                        out.push(InstantSample {
                            labels: series.labels.clone(),
                            // Name-dropping class (issue #86): retained
                            // (the fetched series' own per-row name, with
                            // the `Selector` arm's concrete-name
                            // fallback) + marked.
                            metric_name: series
                                .metric_name
                                .clone()
                                .or_else(|| sel.metric_name.clone()),
                            drop_name: true,
                            t_ms,
                            v: sample.t_ms as f64 / 1000.0,
                            h: None,
                        });
                    }
                }
                Ok(StepValue::Vector(out))
            }
            None => {
                let StepValue::Vector(v) =
                    eval_step(arg, selectors, data, t_ms, lookback_ms, caches)?
                else {
                    return Err(PromqlError::Unsupported {
                        construct: "timestamp() over a scalar expression".to_string(),
                    });
                };
                // M7-A5b-iii: value-agnostic (see the bare-selector arm
                // above) — no guard.
                let out = v
                    .into_iter()
                    .map(|s| InstantSample {
                        labels: s.labels,
                        // Name-dropping class (issue #86): retained +
                        // marked.
                        metric_name: s.metric_name,
                        drop_name: true,
                        t_ms,
                        v: t_ms as f64 / 1000.0,
                        h: None,
                    })
                    .collect();
                Ok(StepValue::Vector(out))
            }
        },

        // Issue #66 (M6-03): `scalar(v)` — the singleton element's value,
        // NaN for zero or multiple elements (upstream funcScalar).
        PlanExpr::ScalarOf { arg } => {
            let StepValue::Vector(v) = eval_step(arg, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "scalar() over a scalar expression".to_string(),
                });
            };
            // M7-A5b-iii: `funcScalar` ignores histogram samples and
            // returns the single remaining FLOAT (`>1` float found ⇒ NaN;
            // `functions.go:1104-1126`) — never an error.
            let floats: Vec<f64> = v.iter().filter(|s| s.h.is_none()).map(|s| s.v).collect();
            Ok(StepValue::Scalar(match floats.as_slice() {
                [only] => *only,
                _ => f64::NAN,
            }))
        }

        // Issue #66 (M6-03): `vector(s)` — one element with the EMPTY
        // label set (and no `__name__`, upstream funcVector).
        PlanExpr::VectorOf { arg } => {
            let StepValue::Scalar(s) = eval_step(arg, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "vector() over a non-scalar expression".to_string(),
                });
            };
            Ok(StepValue::Vector(vec![InstantSample {
                labels: Labels::default(),
                metric_name: None,
                drop_name: false,
                t_ms,
                v: s,
                h: None,
            }]))
        }

        PlanExpr::Binary {
            op,
            lhs,
            rhs,
            bool_modifier,
            matching,
            group,
            fill,
        } => {
            let l = eval_step(lhs, selectors, data, t_ms, lookback_ms, caches)?;
            let r = eval_step(rhs, selectors, data, t_ms, lookback_ms, caches)?;
            // The planner's typed scalar-operand guard (issue #70,
            // parse.go:807-814) discards `group`/`fill` whenever either
            // operand is scalar-typed, so the scalar arms below can never
            // see them — asserted, not silently ignored.
            match (l, r) {
                (StepValue::Scalar(l), StepValue::Scalar(r)) => {
                    debug_assert!(
                        *group == crate::plan::Group::OneToOne && *fill == Default::default(),
                        "plan_binary discards group/fill for scalar operands"
                    );
                    // Issue #129: upstream `scalarBinop` panics for TRIM
                    // (`engine.go:3434`, surfaced as a query error via
                    // `ev.recover`) — `binop::scalar_scalar` has no
                    // fallible signature, so trim is intercepted here,
                    // before it is ever called.
                    if op.is_trim() {
                        return Err(PromqlError::ScalarOp {
                            op: op.item_type_str(),
                        });
                    }
                    Ok(StepValue::Scalar(binop::scalar_scalar(*op, l, r)))
                }
                (StepValue::Vector(v), StepValue::Scalar(s)) => {
                    debug_assert!(
                        *group == crate::plan::Group::OneToOne && *fill == Default::default(),
                        "plan_binary discards group/fill for scalar operands"
                    );
                    let mut annos = caches.annotations.borrow_mut();
                    Ok(StepValue::Vector(binop::vector_scalar(
                        *op,
                        *bool_modifier,
                        &v,
                        s,
                        false,
                        &mut annos,
                    )))
                }
                (StepValue::Scalar(s), StepValue::Vector(v)) => {
                    debug_assert!(
                        *group == crate::plan::Group::OneToOne && *fill == Default::default(),
                        "plan_binary discards group/fill for scalar operands"
                    );
                    let mut annos = caches.annotations.borrow_mut();
                    Ok(StepValue::Vector(binop::vector_scalar(
                        *op,
                        *bool_modifier,
                        &v,
                        s,
                        true,
                        &mut annos,
                    )))
                }
                (StepValue::Vector(l), StepValue::Vector(r)) => {
                    let mut annos = caches.annotations.borrow_mut();
                    Ok(StepValue::Vector(binop::vector_vector(
                        *op,
                        *bool_modifier,
                        matching,
                        group,
                        fill,
                        &l,
                        &r,
                        &mut annos,
                    )?))
                }
                // Unreachable through `plan()`: a string literal only ever
                // plans as the ROOT (defense in depth, issue #86).
                _ => Err(PromqlError::Unsupported {
                    construct: "binary operator over a string operand".to_string(),
                }),
            }
        }

        // Issue #70 (M6-07): `and`/`or`/`unless` — both operands are
        // vector-typed by the vendored parser's own check ("set operator
        // ... not allowed in binary scalar expression"), so the non-vector
        // arm is defense-in-depth, never reachable through `parse()`.
        PlanExpr::SetOp {
            op,
            lhs,
            rhs,
            matching,
        } => {
            let l = eval_step(lhs, selectors, data, t_ms, lookback_ms, caches)?;
            let r = eval_step(rhs, selectors, data, t_ms, lookback_ms, caches)?;
            match (l, r) {
                // M7-A5b-iii: `and`/`or`/`unless` are value-agnostic
                // (`VectorAnd`/`VectorOr`/`VectorUnless` copy the surviving
                // element unchanged, `h` included) — no guard needed.
                (StepValue::Vector(l), StepValue::Vector(r)) => {
                    Ok(StepValue::Vector(binop::set_op(*op, matching, &l, &r)))
                }
                _ => Err(PromqlError::Unsupported {
                    construct: "set operator over a scalar operand".to_string(),
                }),
            }
        }

        // Issue #82 (M6-05b): `info()` — the metadata-join. The base
        // vector comes from this node's prepared horizon (`prepare_info`
        // evaluated arg0 exactly once per step); the info vector resolves
        // from the ALREADY-ELIGIBILITY-NARROWED `eligible_info` set
        // (retroactive re-review, Option B — `prepare_info` narrowed the
        // info-family selector's fetch to this set once per horizon, so
        // this loop is `O(eligible)`, never `O(fetched)`) at the
        // selector's own effective time (offset/@ copied from arg0's
        // first selector at plan time), carrying each series' real
        // metric name and the resolved sample's ORIGINAL timestamp (the
        // newest-wins dedup key). All narrowing/dedup/join semantics live
        // in `info::combine` — name-keeping with the delayed verdict
        // CLEARED: the pin builds fresh DropName-less output samples, so
        // a name-dropping arg0 re-emerges with its retained name kept
        // (see `combine`'s doc).
        PlanExpr::Info {
            base: _,
            info_selector,
            name_matchers,
            data_matchers,
        } => {
            let prepared = caches
                .infos
                .get(&(expr as *const _))
                // Documented invariant: `prepare_subqueries` walks the
                // exact same tree `eval_step` walks and prepares every
                // info node before stepping begins (the SubqueryCache
                // precedent).
                .expect("prepare_subqueries prepares every info node before stepping");
            let base_v = prepared
                .base_steps
                .get(&t_ms)
                // Documented invariant: `prepare_info` walked exactly the
                // enclosing horizon's evaluation grid (the query's own
                // steps, or the enclosing subquery's inner grid).
                .expect("prepare_info covers every evaluation step of its horizon")
                .clone();
            // Issue #130 Δ1: the info.go:371-373 empty-base short-circuit
            // — `combineWithInfoVector`'s FIRST statement returns an
            // empty result, no error, before any info-side sample is
            // inspected. Per step, exactly like upstream's per-step call.
            // `combine`'s own :268 guard remains the correctness anchor;
            // this one is pure work-skipping (the resolution walk below
            // cannot error since Δ3 moved the type check into `combine`).
            if base_v.is_empty() {
                return Ok(StepValue::Vector(Vec::new()));
            }
            // M7-A5b-iii, amended by issue #130 Δ3: `info()` passes the
            // BASE sample's `v`/`h` straight through (value-agnostic on
            // that side), but an info-side sample resolving to a native
            // histogram is upstream's `info sample should be float` error
            // (info.go:383-385) — carried as the `is_histogram` marker
            // below and validated inside `combine`'s dedup loop, so the
            // per-sample check-vs-duplicate error precedence matches
            // upstream's interleaved loop.

            let sel = &selectors[*info_selector];
            let eff_t = sel.at_ms.unwrap_or(t_ms) - sel.offset_ms;
            let mut info_v = Vec::new();
            for series in &prepared.eligible_info {
                if let Some(sample) = staleness::instant_value(&series.samples, eff_t, lookback_ms)
                {
                    info_v.push(info::InfoSeriesAtStep {
                        // `prepare_info` only ever keeps series whose
                        // name it already resolved (concrete-name
                        // fallback applied there) — documented invariant
                        // of `eligible_info`.
                        metric_name: series
                            .metric_name
                            .clone()
                            .expect("eligible_info series carry a resolved metric_name"),
                        labels: series.labels.clone(),
                        orig_t_ms: sample.t_ms,
                        is_histogram: sample.h.is_some(),
                    });
                }
            }

            let out = info::combine(
                base_v,
                info_v,
                &prepared.id_lbl_values,
                name_matchers,
                data_matchers,
                t_ms,
            )?;
            Ok(StepValue::Vector(out))
        }
    }
}

#[cfg(test)]
mod tests {
    use pulsus_model::{NativeHistogram, STALE_NAN_BITS, Span};

    use super::*;
    use crate::plan::{PlanParams, plan};

    /// M7-A5b-i shim: the crate's public `evaluate` now returns
    /// `(QueryValue, Annotations)` (the annotations channel, plan v2
    /// OQ1(a)) — this local shadow (Rust resolves an inner-scope `fn` over
    /// the `use super::*;` glob import) keeps every pre-existing
    /// float-test call site (`evaluate(&p, &data)`) compiling with **zero
    /// assertion edits** (the A5a `Sample` Copy->Clone migration's own
    /// precedent). Tests that need the annotations directly call
    /// `super::evaluate` instead.
    fn evaluate(plan: &QueryPlan, data: &SeriesData) -> Result<QueryValue, PromqlError> {
        super::evaluate(plan, data).map(|(v, _annotations)| v)
    }

    /// `single_histogram` (`native_histograms.test:34`, A3 corpus fixture).
    fn single_histogram() -> NativeHistogram {
        NativeHistogram {
            counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 4,
            sum: 5.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 3,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1, 1, -1],
            negative_buckets: vec![],
            custom_values: vec![],
        }
    }

    fn params(start_ms: i64, end_ms: i64, step_ms: i64) -> PlanParams {
        PlanParams {
            start_ms,
            end_ms,
            step_ms,
            lookback_ms: crate::plan::DEFAULT_LOOKBACK_MS,
            experimental_functions: false,
        }
    }

    // -- issue #125 (AC9 + plan edge cases): the stats-decoding synthesis
    //    pass (`reduce_histogram_stats_samples`), pinned against
    //    `promql/histogram_stats_iterator.go` @ 40af9c2 --

    /// A schema-0 histogram sample with `count` observations in one
    /// bucket (absolute `[count]`).
    fn count_hist(t_ms: i64, count: u64) -> Sample {
        Sample::hist(
            t_ms,
            NativeHistogram {
                counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
                schema: 0,
                zero_threshold: 0.0,
                zero_count: 0,
                count,
                sum: count as f64,
                positive_spans: vec![Span {
                    offset: 0,
                    length: 1,
                }],
                negative_spans: vec![],
                positive_buckets: vec![count as i64],
                negative_buckets: vec![],
                custom_values: vec![],
            }
            .to_float(),
        )
    }

    fn synth_hint(s: &Sample) -> pulsus_model::CounterResetHint {
        s.h.as_ref().expect("histogram sample").counter_reset_hint
    }

    /// AC9, both directions: the stale sample takes the pin's early-return
    /// arm WITHOUT touching the retained `prev` (`AtFloatHistogram`'s
    /// `IsStaleNaN(sum)` arm skips `setLastFromCurrent` — `hsi.last` is
    /// PRESERVED, not cleared), so the post-stale sample's Unknown stored
    /// hint resolves against the PRE-STALE full histogram: `count 5 →
    /// stale → count 3` ⇒ CounterReset; `count 5 → stale → count 7` ⇒
    /// NotCounterReset. Drill (plan v3 Δ2): an implementation that CLEARS
    /// `prev` on the stale sample yields Unknown for the post-stale sample
    /// in BOTH cases — each assertion fails under that mutation.
    #[test]
    fn stats_synthesis_detects_reset_across_a_stale_gap() {
        use pulsus_model::CounterResetHint::{CounterReset, NotCounterReset};
        // The float stale-marker form (what the test grammar's `stale`
        // loads) and the histogram stale form (an empty histogram with a
        // stale-NaN sum, the A4 encoding the pin's `AtFloatHistogram` arm
        // handles) must BOTH preserve `prev`.
        let stale_forms: [Sample; 2] = [Sample::float(60_000, f64::from_bits(STALE_NAN_BITS)), {
            let mut h = single_histogram().to_float();
            h.sum = f64::from_bits(STALE_NAN_BITS);
            Sample::hist(60_000, h)
        }];
        for stale in stale_forms {
            let reset = reduce_histogram_stats_samples(&[
                count_hist(0, 5),
                stale.clone(),
                count_hist(120_000, 3),
            ]);
            assert_eq!(
                synth_hint(&reset[2]),
                CounterReset,
                "count 5 → stale → count 3 must detect against the PRE-STALE histogram"
            );
            // The stale sample itself is emitted with its content (and
            // hint channel) untouched. (`Sample`'s PartialEq is NaN-exact
            // on the float channel — `NaN != NaN` — so the stale FLOAT
            // form is compared field-by-bits instead.)
            assert_eq!(reset[1].t_ms, stale.t_ms);
            assert!(reset[1].is_stale());
            assert_eq!(reset[1].v.to_bits(), stale.v.to_bits());
            match (&reset[1].h, &stale.h) {
                (None, None) => {}
                (Some(a), Some(b)) => {
                    assert!(a.bits_eq(b), "stale histogram content untouched");
                    assert_eq!(
                        a.counter_reset_hint, b.counter_reset_hint,
                        "stale histogram hint untouched"
                    );
                }
                other => panic!("stale sample channel changed: {other:?}"),
            }

            let no_reset = reduce_histogram_stats_samples(&[
                count_hist(0, 5),
                stale.clone(),
                count_hist(120_000, 7),
            ]);
            assert_eq!(
                synth_hint(&no_reset[2]),
                NotCounterReset,
                "count 5 → stale → count 7 must resolve NOT-reset, never Unknown"
            );
        }
    }

    /// The plain chain rule (`getResetHint`): first sample with no prev ⇒
    /// Unknown; a stored (≠ Unknown) hint is KEPT verbatim; a stored hint
    /// still updates `prev` for the NEXT sample's detection.
    #[test]
    fn stats_synthesis_resolves_unknowns_and_keeps_stored_hints() {
        use pulsus_model::CounterResetHint::{CounterReset, Gauge, NotCounterReset, Unknown};
        let out = reduce_histogram_stats_samples(&[count_hist(0, 5), count_hist(60_000, 7)]);
        assert_eq!(synth_hint(&out[0]), Unknown, "no prev ⇒ Unknown");
        assert_eq!(synth_hint(&out[1]), NotCounterReset);

        // Stored hints are kept (the pin returns them without detection)…
        let mut gauge = count_hist(0, 5);
        gauge.h.as_mut().unwrap().counter_reset_hint = Gauge;
        let mut ncr = count_hist(60_000, 6);
        ncr.h.as_mut().unwrap().counter_reset_hint = NotCounterReset;
        // …and STILL update `prev` (setLastFromCurrent runs for every
        // non-stale sample): the third sample's Unknown resolves against
        // the second's FULL histogram.
        let third = count_hist(120_000, 3);
        let out = reduce_histogram_stats_samples(&[gauge, ncr, third]);
        assert_eq!(synth_hint(&out[0]), Gauge);
        assert_eq!(synth_hint(&out[1]), NotCounterReset);
        assert_eq!(synth_hint(&out[2]), CounterReset, "6 → 3 is a reset");
    }

    /// Plan edge case 2: the emitted sample keeps ONLY `{schema, count,
    /// sum, hint}` (the pin's `populateFH` literal), while detection runs
    /// on the FULL histograms — a bucket-only reset (equal counts,
    /// dropped bucket) is still detected even though the EMITTED shapes
    /// carry no buckets.
    #[test]
    fn stats_synthesis_reduces_shape_but_detects_on_full_histograms() {
        use pulsus_model::CounterResetHint::CounterReset;
        // Same count (4), but bucket 1 drops 2 → 1: a bucket-only reset.
        let a = Sample::hist(0, single_histogram().to_float()); // [1,2,1]
        let b = Sample::hist(
            60_000,
            NativeHistogram {
                counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
                schema: 0,
                zero_threshold: 0.0,
                zero_count: 0,
                count: 4,
                sum: 6.0,
                positive_spans: vec![Span {
                    offset: 0,
                    length: 3,
                }],
                negative_spans: vec![],
                positive_buckets: vec![1, 0, 1], // absolute [1,1,2]
                negative_buckets: vec![],
                custom_values: vec![],
            }
            .to_float(),
        );
        let out = reduce_histogram_stats_samples(&[a, b]);
        let reduced = out[1].h.as_ref().unwrap();
        assert_eq!(
            reduced.counter_reset_hint, CounterReset,
            "bucket-only reset must be detected on the FULL previous histogram"
        );
        assert_eq!(reduced.schema, 0, "schema preserved");
        assert_eq!(reduced.count, 4.0);
        assert!(reduced.positive_buckets.is_empty(), "buckets dropped");
        assert!(reduced.positive_spans.is_empty());
        assert_eq!(reduced.zero_count, 0.0);
        assert!(reduced.custom_values.is_empty());
    }

    /// A fetched series with an EMPTY per-series name channel — the
    /// concrete-name selector arms fall back to `sel.metric_name` for
    /// these (see the `Selector` arm's fallback doc), so every
    /// single-metric test keeps working without stamping a name; tests
    /// exercising the #85 per-series channel use [`named_series`].
    fn series(fp: u64, labels: &[(&str, &str)], samples: Vec<Sample>) -> FetchedSeries {
        FetchedSeries {
            fingerprint: fp,
            metric_name: None,
            labels: Labels::new(labels.iter().map(|(k, v)| (k.to_string(), v.to_string()))),
            samples,
            start_ts: None,
        }
    }

    /// A fetched series carrying its own per-series metric name (issue
    /// #85) — what the fetch layer hands the evaluator for matcher-only /
    /// regex-`__name__` selectors.
    fn named_series(
        name: &str,
        fp: u64,
        labels: &[(&str, &str)],
        samples: Vec<Sample>,
    ) -> FetchedSeries {
        FetchedSeries {
            metric_name: Some(name.to_string()),
            ..series(fp, labels, samples)
        }
    }

    fn s(t_ms: i64, v: f64) -> Sample {
        Sample::float(t_ms, v)
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

    /// Issue #155: the paired variant slices and stale-filters `(sample,
    /// st)` pairs TOGETHER — the stale marker's ST is dropped with it and
    /// the bounds match [`windowed_non_stale`] exactly.
    #[test]
    fn windowed_non_stale_with_st_moves_pairs_together_through_slicing_and_stale_filtering() {
        let stale = f64::from_bits(STALE_NAN_BITS);
        let samples = vec![s(0, 1.0), s(50, stale), s(100, 2.0), s(200, 3.0)];
        let st = vec![10, 20, 30, 40];
        let (w, w_st) = windowed_non_stale_with_st(&samples, &st, -1, 100);
        assert_eq!(w, vec![s(0, 1.0), s(100, 2.0)]);
        assert_eq!(w_st, vec![10, 30]);
        // Left-open lower bound drops the paired ST too.
        let (w, w_st) = windowed_non_stale_with_st(&samples, &st, 0, 200);
        assert_eq!(w, vec![s(100, 2.0), s(200, 3.0)]);
        assert_eq!(w_st, vec![30, 40]);
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

    // --- M7-A5a AC4: selection eval surface carries the histogram channel ---

    #[test]
    fn bare_instant_selector_over_a_histogram_series_yields_h_some() {
        let expr = crate::parser::parse("up").unwrap();
        let p = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(
                1,
                &[("job", "a")],
                vec![Sample::hist(900, single_histogram().to_float())],
            )],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 1);
                assert!(
                    v[0].h
                        .as_deref()
                        .unwrap()
                        .bits_eq(&single_histogram().to_float()),
                    "the decoded histogram is carried through selection"
                );
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn bare_range_selector_over_a_histogram_series_carries_h_in_points() {
        let expr = crate::parser::parse("up").unwrap();
        let p = plan(&expr, params(1_000, 2_000, 1_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(
                1,
                &[("job", "a")],
                vec![
                    Sample::hist(1_000, single_histogram().to_float()),
                    Sample::hist(2_000, single_histogram().to_float()),
                ],
            )],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Matrix(m) => {
                assert_eq!(m.len(), 1);
                assert!(
                    m[0].points.iter().all(|pt| pt
                        .h
                        .as_deref()
                        .is_some_and(|h| h.bits_eq(&single_histogram().to_float()))),
                    "range materialization threads h into every step point"
                );
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    #[test]
    fn a_stale_histogram_sample_makes_the_series_absent_at_that_step() {
        // A4 encodes histogram staleness as sum = STALE_NAN_BITS; the eval
        // layer drops it exactly as it drops a float stale marker.
        let expr = crate::parser::parse("up").unwrap();
        let p = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        let mut stale = single_histogram().to_float();
        stale.sum = f64::from_bits(STALE_NAN_BITS);
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(1, &[("job", "a")], vec![Sample::hist(900, stale)])],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                assert!(v.is_empty(), "a stale histogram makes the series absent")
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    // --- M7-A5a AC8: a DERIVED operation (anything beyond a bare selector)
    //     that consumes a histogram sample must ERROR (histogram-unsupported
    //     → 422 execution), NEVER fold `v`/emit `h: None` (a fabricated 0.0
    //     float). A5a is selection-only; the histogram function set is A5b. ---

    /// One histogram series at selector 0 (the sole selector of a
    /// single-metric query), with two in-window samples.
    fn histogram_selector_data() -> SeriesData {
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(
                1,
                &[("job", "a")],
                vec![
                    Sample::hist(900, single_histogram().to_float()),
                    Sample::hist(1_000, single_histogram().to_float()),
                ],
            )],
        );
        data
    }

    /// M7-A5b-iii superseded the A5a blanket reject for aggregation: a
    /// single-member `sum` group over a histogram series computes the
    /// (trivial, one-addend) Kahan sum — the input histogram, unchanged.
    #[test]
    fn sum_aggregation_over_a_single_histogram_series_computes_the_sum() {
        let expr = crate::parser::parse("sum(up)").unwrap();
        let plan = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        match evaluate(&plan, &histogram_selector_data()) {
            Ok(QueryValue::Vector(v)) => {
                assert_eq!(v.len(), 1);
                let h = v[0]
                    .h
                    .as_ref()
                    .expect("sum() over a histogram computes a histogram");
                assert!(h.bits_eq(&single_histogram().to_float()));
            }
            other => panic!("expected an Ok histogram-valued vector, got {other:?}"),
        }
    }

    /// `histogram + scalar` (`+` unsupported for float/histogram, only
    /// `*` is — `vectorElemBinop`'s `hlhs!=nil,hrhs==nil` arm): the
    /// element is dropped and an `IncompatibleTypesInBinOpInfo` fires,
    /// never an error and never a fabricated float.
    #[test]
    fn binary_op_add_over_a_histogram_and_a_scalar_drops_with_info() {
        let expr = crate::parser::parse("up + 1").unwrap();
        let p = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        let (value, annos) = super::evaluate(&p, &histogram_selector_data()).unwrap();
        assert_eq!(value, QueryValue::Vector(Vec::new()));
        let (_, infos) = annos.as_strings(0, 0);
        assert_eq!(
            infos,
            vec![
                crate::annotations::messages::incompatible_types_in_binop_info(
                    "histogram",
                    "+",
                    "float"
                )
            ]
        );
    }

    /// M7-A5b-iii (codex round-1 [medium]): a histogram-ONLY window is a
    /// SILENT empty result for `predict_linear` — `funcPredictLinear`
    /// returns before any annotation when `len(Floats) == 0`
    /// (`functions.go:1928-1934`), never a 422.
    #[test]
    fn predict_linear_over_a_histogram_only_window_is_silently_empty() {
        let expr = crate::parser::parse("predict_linear(up[5m], 10)").unwrap();
        let p = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        let (value, annos) = super::evaluate(&p, &histogram_selector_data()).unwrap();
        assert_eq!(value, QueryValue::Vector(Vec::new()));
        assert!(
            annos.is_empty(),
            "a histogram-only window is silent: {:?}",
            annos.as_strings(0, 0)
        );
    }

    /// A MIXED window (≥2 floats + histograms) computes the regression on
    /// the FLOAT SUBSET and emits `HistogramIgnoredInMixedRangeInfo` once
    /// (`functions.go:1936-1939`). Floats at t=0/1000ms with values 0/1
    /// regress to slope 1/s, intercept 1 at eval time 1s → `predict_linear
    /// (…, 10) = 11`; the interleaved histogram must not perturb it.
    #[test]
    fn predict_linear_over_a_mixed_window_computes_on_floats_and_annotates() {
        let expr = crate::parser::parse("predict_linear(up[5m], 10)").unwrap();
        let p = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(
                1,
                &[("job", "a")],
                vec![
                    Sample::float(0, 0.0),
                    Sample::hist(500, single_histogram().to_float()),
                    Sample::float(1_000, 1.0),
                ],
            )],
        );
        let (value, annos) = super::evaluate(&p, &data).unwrap();
        match value {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].v, 11.0);
                assert!(v[0].h.is_none());
            }
            other => panic!("expected a float vector, got {other:?}"),
        }
        let (warnings, infos) = annos.as_strings(0, 0);
        assert!(warnings.is_empty());
        assert_eq!(
            infos,
            vec![crate::annotations::messages::histogram_ignored_in_mixed_range_info("up")]
        );
    }

    /// ONE float + histograms → too few float points: EMPTY result but
    /// the mixed-window info still fires (`functions.go:1928-1932`'s
    /// `len(Floats) == 1 && len(Histograms) > 0` arm).
    #[test]
    fn predict_linear_with_one_float_in_a_mixed_window_is_empty_with_info() {
        let expr = crate::parser::parse("predict_linear(up[5m], 10)").unwrap();
        let p = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(
                1,
                &[("job", "a")],
                vec![
                    Sample::hist(500, single_histogram().to_float()),
                    Sample::float(1_000, 1.0),
                ],
            )],
        );
        let (value, annos) = super::evaluate(&p, &data).unwrap();
        assert_eq!(value, QueryValue::Vector(Vec::new()));
        let (_, infos) = annos.as_strings(0, 0);
        assert_eq!(
            infos,
            vec![crate::annotations::messages::histogram_ignored_in_mixed_range_info("up")]
        );
    }

    /// `double_exponential_smoothing` shares the disposition
    /// (`funcDoubleExponentialSmoothing`, `functions.go:930-963`) — mixed
    /// window computes on the float subset + info — AND its sf/tf
    /// validation still precedes the float-count check (`:923-928`): a
    /// histogram-only window with an invalid factor errors, with a valid
    /// factor it is silently empty.
    #[test]
    fn double_exponential_smoothing_histogram_disposition_and_validation_order() {
        let params_exp = PlanParams {
            experimental_functions: true,
            ..params(1_000, 1_000, 0)
        };
        // Histogram-only + valid factors → silent empty.
        let expr = crate::parser::parse("double_exponential_smoothing(up[5m], 0.5, 0.5)").unwrap();
        let p = plan(&expr, params_exp).unwrap();
        let (value, annos) = super::evaluate(&p, &histogram_selector_data()).unwrap();
        assert_eq!(value, QueryValue::Vector(Vec::new()));
        assert!(annos.is_empty());
        // Histogram-only + INVALID factor → the validation error fires
        // before the float-count check (the pin's panic ordering).
        let expr_bad =
            crate::parser::parse("double_exponential_smoothing(up[5m], 2.0, 0.5)").unwrap();
        let p_bad = plan(&expr_bad, params_exp).unwrap();
        assert!(matches!(
            super::evaluate(&p_bad, &histogram_selector_data()),
            Err(PromqlError::InvalidParameter { .. })
        ));
        // Mixed window → float-subset result + the mixed info.
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(
                1,
                &[("job", "a")],
                vec![
                    Sample::float(0, 1.0),
                    Sample::hist(500, single_histogram().to_float()),
                    Sample::float(1_000, 3.0),
                ],
            )],
        );
        let p2 = plan(
            &crate::parser::parse("double_exponential_smoothing(up[5m], 0.5, 0.5)").unwrap(),
            params_exp,
        )
        .unwrap();
        let (value2, annos2) = super::evaluate(&p2, &data).unwrap();
        match value2 {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 1);
                // Two floats [1, 3], sf=tf=0.5: s1 = 0.5*3 + 0.5*(1+2) = 3.
                assert_eq!(v[0].v, 3.0);
            }
            other => panic!("expected a float vector, got {other:?}"),
        }
        let (_, infos2) = annos2.as_strings(0, 0);
        assert_eq!(
            infos2,
            vec![crate::annotations::messages::histogram_ignored_in_mixed_range_info("up")]
        );
    }

    /// M7-A5b-ii: a native-histogram range function computes a real
    /// result — never an error, never a fabricated `0.0` float.
    #[test]
    fn range_function_over_a_histogram_series_now_computes_a_histogram() {
        let expr = crate::parser::parse("rate(up[5m])").unwrap();
        let plan = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        match evaluate(&plan, &histogram_selector_data()) {
            Ok(QueryValue::Vector(v)) => {
                assert_eq!(v.len(), 1);
                assert!(
                    v[0].h.is_some(),
                    "rate() over identical histogram samples yields a histogram result, not a fabricated float"
                );
            }
            other => panic!("expected an Ok histogram-valued vector, got {other:?}"),
        }
    }

    /// M7-A5b-iii: `avg_over_time`'s histogram (KahanAdd direct-mean) path
    /// — two identical fixture samples average to the same histogram.
    #[test]
    fn avg_over_time_over_a_histogram_series_computes_the_mean() {
        let expr = crate::parser::parse("avg_over_time(up[5m])").unwrap();
        let plan = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        match evaluate(&plan, &histogram_selector_data()) {
            Ok(QueryValue::Vector(v)) => {
                assert_eq!(v.len(), 1);
                let h = v[0]
                    .h
                    .as_ref()
                    .expect("avg_over_time() over histograms computes a histogram");
                assert!(h.bits_eq(&single_histogram().to_float()));
            }
            other => panic!("expected an Ok histogram-valued vector, got {other:?}"),
        }
    }

    /// M7-A5b-iii: `simpleFloatFunc`'s `filterFloats` drops histogram
    /// samples silently (empty result, never an error).
    #[test]
    fn elementwise_math_drops_histogram_samples_silently() {
        let expr = crate::parser::parse("abs(up)").unwrap();
        let plan = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        assert_eq!(
            evaluate(&plan, &histogram_selector_data()).unwrap(),
            QueryValue::Vector(Vec::new())
        );
    }

    /// M7-A5b-iii: `label_replace`/`label_join` are value-agnostic — a
    /// histogram sample's `h` passes through the label rewrite unchanged.
    #[test]
    fn label_replace_over_a_histogram_series_preserves_the_histogram() {
        let expr =
            crate::parser::parse("label_replace(up, \"x\", \"y\", \"job\", \".*\")").unwrap();
        let plan = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        match evaluate(&plan, &histogram_selector_data()) {
            Ok(QueryValue::Vector(v)) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].labels.get("x"), Some("y"));
                let h = v[0]
                    .h
                    .as_ref()
                    .expect("label_replace() preserves the histogram sample");
                assert!(h.bits_eq(&single_histogram().to_float()));
            }
            other => panic!("expected an Ok histogram-valued vector, got {other:?}"),
        }
    }

    #[test]
    fn label_join_over_a_histogram_series_preserves_the_histogram() {
        let expr = crate::parser::parse("label_join(up, \"x\", \"-\", \"job\")").unwrap();
        let plan = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        match evaluate(&plan, &histogram_selector_data()) {
            Ok(QueryValue::Vector(v)) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].labels.get("x"), Some("a"));
                assert!(
                    v[0].h.is_some(),
                    "label_join() preserves the histogram sample"
                );
            }
            other => panic!("expected an Ok histogram-valued vector, got {other:?}"),
        }
    }

    /// M7-A5b-iii: `funcTimestamp` is value-agnostic — a histogram
    /// sample's real stored timestamp is emitted exactly like a float
    /// sample's (bare-selector special case, per the #66 adjudication).
    #[test]
    fn timestamp_of_a_bare_histogram_selector_returns_the_sample_time() {
        let expr = crate::parser::parse("timestamp(up)").unwrap();
        let plan = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        match evaluate(&plan, &histogram_selector_data()) {
            Ok(QueryValue::Vector(v)) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].v, 1.0, "1000ms -> 1s");
                assert!(v[0].h.is_none(), "timestamp() always emits a float");
            }
            other => panic!("expected an Ok float-valued vector, got {other:?}"),
        }
    }

    /// The per-step matrix accumulation path (step_ms > 0) computes too,
    /// rather than rejecting or folding a fabricated `0.0` into the
    /// matrix.
    #[test]
    fn a_range_query_sum_aggregation_over_a_histogram_series_computes_every_step() {
        let expr = crate::parser::parse("sum(up)").unwrap();
        let plan = plan(&expr, params(1_000, 2_000, 1_000)).unwrap();
        match evaluate(&plan, &histogram_selector_data()) {
            Ok(QueryValue::Matrix(m)) => {
                assert_eq!(m.len(), 1);
                assert_eq!(m[0].points.len(), 2);
                for p in &m[0].points {
                    let h =
                        p.h.as_ref()
                            .expect("sum() computes a histogram at every step");
                    assert!(h.bits_eq(&single_histogram().to_float()));
                }
            }
            other => panic!("expected an Ok histogram-valued matrix, got {other:?}"),
        }
    }

    /// M7-A5b-iii: `and`/`or`/`unless` are value-agnostic passthroughs —
    /// a histogram operand's `h` survives the set-op membership filter.
    #[test]
    fn set_op_and_over_a_histogram_series_preserves_the_histogram() {
        let expr = crate::parser::parse("up and up").unwrap();
        let p = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        let mut data = SeriesData::new();
        // Cover either planner outcome (one shared or two distinct
        // selector ids) by populating both.
        for id in 0..2 {
            data.insert(
                id,
                vec![series(
                    1,
                    &[("job", "a")],
                    vec![Sample::hist(900, single_histogram().to_float())],
                )],
            );
        }
        match evaluate(&p, &data) {
            Ok(QueryValue::Vector(v)) => {
                assert_eq!(v.len(), 1);
                assert!(v[0].h.is_some(), "and preserves the histogram operand");
            }
            other => panic!("expected an Ok histogram-valued vector, got {other:?}"),
        }
    }

    /// The scalar-op-vector arm (`1 + up`) drops with the same
    /// `IncompatibleTypesInBinOpInfo` as the vector-op-scalar arm
    /// (`up + 1`, covered above) — `float`/`histogram` operand order
    /// swapped in the message.
    #[test]
    fn scalar_vector_binop_add_over_a_histogram_series_drops_with_info() {
        let expr = crate::parser::parse("1 + up").unwrap();
        let p = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        let (value, annos) = super::evaluate(&p, &histogram_selector_data()).unwrap();
        assert_eq!(value, QueryValue::Vector(Vec::new()));
        let (_, infos) = annos.as_strings(0, 0);
        assert_eq!(
            infos,
            vec![
                crate::annotations::messages::incompatible_types_in_binop_info(
                    "float",
                    "+",
                    "histogram"
                )
            ]
        );
    }

    /// Both operands of `up + up` are histogram-valued vectors; `Add` is
    /// a SUPPORTED histogram/histogram op (`hlhs.Add(hrhs)`) — the
    /// vector-vector arm now computes the real sum, never an error.
    #[test]
    fn vector_vector_binop_add_over_histogram_operands_computes_the_sum() {
        let expr = crate::parser::parse("up + up").unwrap();
        let p = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        let mut data = SeriesData::new();
        // `up + up` plans two distinct selector ids (no dedup); populate
        // both with the same histogram series so each operand is a
        // histogram vector.
        for id in 0..2 {
            data.insert(
                id,
                vec![series(
                    1,
                    &[("job", "a")],
                    vec![Sample::hist(1_000, single_histogram().to_float())],
                )],
            );
        }
        match evaluate(&p, &data) {
            Ok(QueryValue::Vector(v)) => {
                assert_eq!(v.len(), 1);
                let h = v[0].h.as_ref().expect("up + up computes a histogram");
                let mut want = single_histogram().to_float();
                want.count = 8.0;
                want.sum = 10.0;
                want.positive_buckets = vec![2.0, 4.0, 2.0];
                assert!(h.bits_eq(&want));
            }
            other => panic!("expected an Ok histogram-valued vector, got {other:?}"),
        }
    }

    /// M7-A5b-iii: `sum_over_time`'s histogram (KahanAdd) path now
    /// computes through a subquery-materialized grid too, not just a
    /// plain range selector. The `:1s` inner step lands the fixture's
    /// t=1000 (=1s) histogram sample on a grid point.
    #[test]
    fn sum_over_time_subquery_over_a_histogram_series_computes_the_sum() {
        let expr = crate::parser::parse("sum_over_time(up[5m:1s])").unwrap();
        let plan = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        match evaluate(&plan, &histogram_selector_data()) {
            Ok(QueryValue::Vector(v)) => {
                assert_eq!(v.len(), 1);
                let h = v[0]
                    .h
                    .as_ref()
                    .expect("sum_over_time() over a subquery grid computes a histogram");
                assert!(h.bits_eq(&single_histogram().to_float()));
            }
            other => panic!("expected an Ok histogram-valued vector, got {other:?}"),
        }
    }

    /// `@ 1` pins the selector at 1 s = 1000 ms — the fixture's second
    /// histogram sample — so the step-invariant `@`-pinned subtree still
    /// feeds a histogram vector into `sum`, which now computes.
    #[test]
    fn at_pinned_sum_aggregation_over_a_histogram_series_computes_the_sum() {
        let expr = crate::parser::parse("sum(up @ 1)").unwrap();
        let plan = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        match evaluate(&plan, &histogram_selector_data()) {
            Ok(QueryValue::Vector(v)) => {
                assert_eq!(v.len(), 1);
                let h = v[0]
                    .h
                    .as_ref()
                    .expect("sum() over an @-pinned histogram computes");
                assert!(h.bits_eq(&single_histogram().to_float()));
            }
            other => panic!("expected an Ok histogram-valued vector, got {other:?}"),
        }
    }

    #[test]
    fn float_aggregation_is_unaffected_by_the_histogram_guard() {
        // No regression: the guard is a no-op for a float-only vector.
        let expr = crate::parser::parse("sum(up)").unwrap();
        let p = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("job", "a")], vec![s(900, 3.0)]),
                series(2, &[("job", "b")], vec![s(900, 4.0)]),
            ],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].v, 7.0);
                assert!(v[0].h.is_none(), "a float aggregation result stays float");
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

    /// Issue #69 (M6-06, AC3): the lifted `group` restriction end-to-end
    /// — `group` over computed bodies evaluates to constant 1 per group.
    #[test]
    fn evaluates_group_over_computed_expressions_to_one_per_group() {
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("job", "a")], vec![s(0, 3.0)]),
                series(2, &[("job", "b")], vec![s(0, 7.0)]),
            ],
        );
        for query in ["group(m + m)", "group by (job) (m * 2)"] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, params(0, 0, 0)).unwrap();
            // Both selectors of `m + m` get the same series.
            let mut d = data.clone();
            for spec in &p.selectors {
                d.insert(spec.id, data.get(0).to_vec());
            }
            match evaluate(&p, &d).unwrap() {
                QueryValue::Vector(v) => {
                    assert!(!v.is_empty(), "{query}");
                    assert!(v.iter().all(|s| s.v == 1.0), "{query}: {v:?}");
                    assert!(v.iter().all(|s| s.metric_name.is_none()), "{query}");
                }
                other => panic!("{query}: expected Vector, got {other:?}"),
            }
        }
    }

    /// Issue #69 (M6-06, plan v2 Δ4): a non-scalar aggregation parameter
    /// (the parser does not type-check limitk/limit_ratio params) is a
    /// descriptive `Unsupported`, never a wrong answer.
    #[test]
    fn a_non_scalar_limitk_parameter_is_unsupported() {
        let expr = crate::parser::parse("limitk(m, m)").unwrap();
        let p = plan(
            &expr,
            PlanParams {
                experimental_functions: true,
                ..params(0, 0, 0)
            },
        )
        .unwrap();
        let mut data = SeriesData::new();
        for spec in &p.selectors {
            data.insert(spec.id, vec![series(1, &[("job", "a")], vec![s(0, 1.0)])]);
        }
        let err = evaluate(&p, &data).unwrap_err();
        match err {
            PromqlError::Unsupported { construct } => {
                assert!(construct.contains("scalar"), "{construct:?}")
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    // --- issue #130 Δ2: limit_ratio cap warning from horizon-wide extrema ---

    /// The limit.test:118/:123 shape: a RANGE query with a CONSTANT
    /// out-of-range ratio emits exactly ONE warning with the pinned
    /// text — not one per step (`Annotations`' exact-text dedup would
    /// mask per-step emission here; the varying-param tests below
    /// discriminate the scheme).
    #[test]
    fn limit_ratio_constant_out_of_range_ratio_warns_exactly_once_over_a_range() {
        for (query, want) in [
            (
                "limit_ratio(1.1, m)",
                "PromQL warning: ratio value should be between -1 and 1, got 1.1, capping to 1",
            ),
            (
                "limit_ratio(-1.1, m)",
                "PromQL warning: ratio value should be between -1 and 1, got -1.1, capping to -1",
            ),
        ] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, experimental(params(0, 120_000, 60_000))).unwrap();
            let mut data = SeriesData::new();
            for spec in &p.selectors {
                data.insert(
                    spec.id,
                    vec![series(
                        1,
                        &[("job", "a")],
                        vec![s(0, 1.0), s(60_000, 1.0), s(120_000, 1.0)],
                    )],
                );
            }
            let (_, annotations) = super::evaluate(&p, &data).unwrap();
            let (warnings, infos) = annotations.as_strings(0, 0);
            assert_eq!(warnings, vec![want.to_string()], "{query}");
            assert!(infos.is_empty(), "{query}: {infos:?}");
        }
    }

    /// Boundary ratios (±1.0) are in range — silent, matching
    /// limit.test:110-115.
    #[test]
    fn limit_ratio_boundary_ratios_are_silent() {
        for query in ["limit_ratio(1.0, m)", "limit_ratio(-1.0, m)"] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, experimental(params(0, 120_000, 60_000))).unwrap();
            let mut data = SeriesData::new();
            for spec in &p.selectors {
                data.insert(
                    spec.id,
                    vec![series(
                        1,
                        &[("job", "a")],
                        vec![s(0, 1.0), s(60_000, 1.0), s(120_000, 1.0)],
                    )],
                );
            }
            let (_, annotations) = super::evaluate(&p, &data).unwrap();
            assert!(
                annotations.is_empty(),
                "{query}: in-range ratios must not warn"
            );
        }
    }

    /// The scheme discriminator (plan v2 Δ2): a VARYING out-of-range
    /// param across range steps yields exactly ONE warning citing the
    /// horizon max — upstream warns from `params.Max()` once, never per
    /// distinct step value (engine.go:1655-1657 at the pin). Per-step
    /// emission would produce TWO distinct messages here (1.1 and 1.3).
    #[test]
    fn limit_ratio_varying_param_warns_once_from_the_horizon_max() {
        let expr = crate::parser::parse("limit_ratio(scalar(r), m)").unwrap();
        let p = plan(&expr, experimental(params(0, 120_000, 60_000))).unwrap();
        let mut data = SeriesData::new();
        for spec in &p.selectors {
            match spec.metric_name.as_deref() {
                Some("r") => data.insert(
                    spec.id,
                    vec![series(
                        1,
                        &[],
                        vec![s(0, 0.5), s(60_000, 1.1), s(120_000, 1.3)],
                    )],
                ),
                Some("m") => data.insert(
                    spec.id,
                    vec![series(
                        2,
                        &[("job", "a")],
                        vec![s(0, 1.0), s(60_000, 1.0), s(120_000, 1.0)],
                    )],
                ),
                other => panic!("unexpected selector {other:?}"),
            }
        }
        let (_, annotations) = super::evaluate(&p, &data).unwrap();
        let (warnings, infos) = annotations.as_strings(0, 0);
        assert_eq!(
            warnings,
            vec![
                "PromQL warning: ratio value should be between -1 and 1, got 1.3, capping to 1"
                    .to_string()
            ],
            "exactly one warning, citing the horizon max"
        );
        assert!(infos.is_empty(), "{infos:?}");
    }

    /// Two-sided variant: steps spanning 1.2 and −1.3 yield exactly the
    /// two extrema messages (`Max() > 1.0` and `Min() < -1.0` each fire
    /// once — engine.go:1655-1660 at the pin).
    #[test]
    fn limit_ratio_two_sided_varying_param_warns_once_per_extrema_side() {
        let expr = crate::parser::parse("limit_ratio(scalar(r), m)").unwrap();
        let p = plan(&expr, experimental(params(0, 120_000, 60_000))).unwrap();
        let mut data = SeriesData::new();
        for spec in &p.selectors {
            match spec.metric_name.as_deref() {
                Some("r") => data.insert(
                    spec.id,
                    vec![series(
                        1,
                        &[],
                        vec![s(0, 1.2), s(60_000, -1.3), s(120_000, 0.5)],
                    )],
                ),
                Some("m") => data.insert(
                    spec.id,
                    vec![series(
                        2,
                        &[("job", "a")],
                        vec![s(0, 1.0), s(60_000, 1.0), s(120_000, 1.0)],
                    )],
                ),
                other => panic!("unexpected selector {other:?}"),
            }
        }
        let (_, annotations) = super::evaluate(&p, &data).unwrap();
        let (warnings, infos) = annotations.as_strings(0, 0);
        assert_eq!(
            warnings,
            vec![
                "PromQL warning: ratio value should be between -1 and 1, got 1.2, capping to 1"
                    .to_string(),
                "PromQL warning: ratio value should be between -1 and 1, got -1.3, capping to -1"
                    .to_string(),
            ],
            "one warning per extrema side"
        );
        assert!(infos.is_empty(), "{infos:?}");
    }

    // --- issue #130 Δ1+Δ3: info() histogram info-sample rejection ---

    /// 4b′(ii): a non-empty base joined against an info-side series whose
    /// resolved sample is a native histogram is upstream's pinned error
    /// (info.go:383-385; info.test:191's `expect fail`).
    #[test]
    fn info_with_a_histogram_info_sample_over_a_non_empty_base_errors() {
        let expr = crate::parser::parse("info(m)").unwrap();
        let p = plan(&expr, experimental(params(0, 0, 0))).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            p.selectors[0].id,
            vec![series(
                1,
                &[("instance", "a"), ("job", "1")],
                vec![s(0, 0.0)],
            )],
        );
        data.insert(
            p.selectors[1].id,
            vec![series(
                2,
                &[("instance", "a"), ("job", "1"), ("data", "x")],
                vec![Sample::hist(0, single_histogram().to_float())],
            )],
        );
        let err = evaluate(&p, &data).unwrap_err();
        assert_eq!(err.to_string(), "info sample should be float");
    }

    /// 4b′(i): an EMPTY base step short-circuits BEFORE any info-side
    /// sample is inspected (info.go:371-373 — `combineWithInfoVector`'s
    /// first statement), so a histogram info sample resolving only at
    /// the empty step never errors. Step 0: base resolves, the info
    /// histogram does not (its sample is at 400s). Step 400s: base is
    /// past its 5m lookback (empty), the histogram resolves — and must
    /// be short-circuited past, not type-checked.
    #[test]
    fn info_with_an_empty_base_step_ignores_a_histogram_info_sample_at_that_step() {
        let expr = crate::parser::parse("info(m)").unwrap();
        let p = plan(&expr, experimental(params(0, 400_000, 400_000))).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            p.selectors[0].id,
            vec![series(
                1,
                &[("instance", "a"), ("job", "1")],
                vec![s(0, 7.0)],
            )],
        );
        data.insert(
            p.selectors[1].id,
            vec![series(
                2,
                &[("instance", "a"), ("job", "1"), ("data", "x")],
                vec![Sample::hist(400_000, single_histogram().to_float())],
            )],
        );
        let (value, annotations) = super::evaluate(&p, &data).unwrap();
        match value {
            QueryValue::Matrix(m) => {
                assert_eq!(m.len(), 1, "only the step-0 base point survives");
                assert_eq!(m[0].points.len(), 1);
                assert_eq!(m[0].points[0].t_ms, 0);
                assert_eq!(m[0].points[0].v, 7.0);
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
        assert!(annotations.is_empty());
    }

    /// 4b′(iv): the float info path is unaffected — an ordinary
    /// `target_info` float sample still enriches, no error, no
    /// annotations.
    #[test]
    fn info_float_path_still_enriches_after_the_histogram_guard() {
        let expr = crate::parser::parse("info(m)").unwrap();
        let p = plan(&expr, experimental(params(0, 0, 0))).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            p.selectors[0].id,
            vec![series(
                1,
                &[("instance", "a"), ("job", "1")],
                vec![s(0, 3.0)],
            )],
        );
        data.insert(
            p.selectors[1].id,
            vec![series(
                2,
                &[("instance", "a"), ("job", "1"), ("data", "x")],
                vec![s(0, 1.0)],
            )],
        );
        let (value, annotations) = super::evaluate(&p, &data).unwrap();
        match value {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].v, 3.0);
                assert_eq!(v[0].labels.get("data"), Some("x"), "enriched");
            }
            other => panic!("expected Vector, got {other:?}"),
        }
        assert!(annotations.is_empty());
    }

    /// Issue #69 (M6-06): `count_values` end-to-end through its dedicated
    /// plan variant, incl. the `__name__` destination writing the
    /// metric-name channel.
    #[test]
    fn evaluates_count_values_end_to_end() {
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("i", "0")], vec![s(0, 6.0)]),
                series(2, &[("i", "1")], vec![s(0, 6.0)]),
                series(3, &[("i", "2")], vec![s(0, 7.0)]),
            ],
        );
        let expr = crate::parser::parse(r#"count_values("version", m)"#).unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 2);
                assert_eq!(v[0].labels.get("version"), Some("6"));
                assert_eq!(v[0].v, 2.0);
                assert_eq!(v[1].labels.get("version"), Some("7"));
                assert_eq!(v[1].v, 1.0);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
        let expr = crate::parser::parse(r#"count_values("__name__", m)"#).unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 2);
                assert!(v.iter().all(|s| s.labels.is_empty()));
                let names: Vec<Option<&str>> = v.iter().map(|s| s.metric_name.as_deref()).collect();
                assert!(
                    names.contains(&Some("6")) && names.contains(&Some("7")),
                    "{v:?}"
                );
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

    /// Issue #129: a trim operator (`</`/`>/`) between two scalars —
    /// upstream `scalarBinop` panics `operator %q not allowed for Scalar
    /// operations` (`engine.go:3434`), surfaced as a query error;
    /// `binop::scalar_scalar` has no fallible signature, so this must be
    /// intercepted before it is ever called.
    #[test]
    fn trim_operator_between_two_scalars_is_a_scalar_op_error() {
        for (query, want_op) in [("1 </ 2", "</"), ("1 >/ 2", ">/")] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, params(0, 0, 0)).unwrap();
            let data = SeriesData::new();
            let err = evaluate(&p, &data).unwrap_err();
            assert_eq!(err, PromqlError::ScalarOp { op: want_op }, "{query}");
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

    /// M7-A5b-i: the classic-`le` path reports upstream's
    /// forced-monotonicity info (`funcHistogramQuantile`,
    /// `functions.go:2111-2117`) when a genuine cumulative-count decrease
    /// was clamped — and stays silent for a monotone input.
    #[test]
    fn classic_histogram_quantile_forced_monotonicity_fires_the_info_once() {
        let expr = crate::parser::parse(r#"histogram_quantile(0.5, x_bucket)"#).unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        // Cumulative counts 5 -> 3 (a real decrease) -> 10: forced.
        data.insert(
            0,
            vec![
                series(1, &[("le", "0.1")], vec![s(0, 5.0)]),
                series(2, &[("le", "0.5")], vec![s(0, 3.0)]),
                series(3, &[("le", "+Inf")], vec![s(0, 10.0)]),
            ],
        );
        let (value, annotations) = super::evaluate(&p, &data).unwrap();
        assert!(matches!(value, QueryValue::Vector(v) if v.len() == 1));
        let (warnings, infos) = annotations.as_strings(0, 0);
        assert!(warnings.is_empty(), "no warning expected: {warnings:?}");
        assert_eq!(infos.len(), 1, "exactly one forced-monotonicity info");
        assert!(
            infos[0].contains("needed to be fixed for monotonicity")
                && infos[0].contains("for metric name \"x_bucket\""),
            "got {infos:?}"
        );

        // Monotone counterpart: NO annotation (float behavior unchanged).
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("le", "0.1")], vec![s(0, 3.0)]),
                series(2, &[("le", "0.5")], vec![s(0, 5.0)]),
                series(3, &[("le", "+Inf")], vec![s(0, 10.0)]),
            ],
        );
        let (_, annotations) = super::evaluate(&p, &data).unwrap();
        assert!(
            annotations.is_empty(),
            "monotone classic input adds no annotation"
        );
    }

    /// `#124` round-2 blocker B: a RANGE query that forces monotonicity at
    /// every step emits ONE merged info — not one per step — with the
    /// pin's widened detail (`over N samples from <minTs> to <maxTs>`),
    /// matching upstream's per-step `warnings.Merge(ws)` in `rangeEval`
    /// (`engine.go:1523-1525`) running `histogramQuantileForcedMonotonicityErr
    /// .Merge` on the base-message key collision.
    #[test]
    fn range_query_merges_forced_monotonicity_infos_across_steps_into_one_widened_info() {
        let expr = crate::parser::parse(r#"histogram_quantile(0.5, x_bucket)"#).unwrap();
        // 3 steps: t = 0, 60s, 120s; the same forced decrease (5 -> 3,
        // clamped at le=0.5, diff 2) is visible at every step via the
        // staleness lookback.
        let p = plan(&expr, params(0, 120_000, 60_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("le", "0.1")], vec![s(0, 5.0)]),
                series(2, &[("le", "0.5")], vec![s(0, 3.0)]),
                series(3, &[("le", "+Inf")], vec![s(0, 10.0)]),
            ],
        );
        let (value, annotations) = super::evaluate(&p, &data).unwrap();
        assert!(matches!(value, QueryValue::Matrix(_)));
        let (warnings, infos) = annotations.as_strings(0, 0);
        assert!(warnings.is_empty(), "no warning expected: {warnings:?}");
        assert_eq!(
            infos,
            vec![
                "PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) for metric name \"x_bucket\", from buckets 0.5 to 0.5, with a max diff of 2, over 3 samples from 1970-01-01T00:00:00Z to 1970-01-01T00:02:00Z".to_string()
            ],
            "exactly one info, merged across the 3 steps"
        );
    }

    /// M7-A5b-i: the `resetHistograms` conflict filter — one identity
    /// carrying BOTH classic `le` buckets and a native histogram at the
    /// same timestamp evaluates NEITHER and warns
    /// (`MixedClassicNativeHistogramsWarning`, `engine.go:1354-1371`);
    /// an unconflicted identity in the same vector still evaluates.
    #[test]
    fn mixed_classic_native_conflict_drops_both_sides_and_warns() {
        let expr = crate::parser::parse(r#"histogram_quantile(0.5, m)"#).unwrap();
        let p = plan(&expr, params(1_000, 1_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                // job=a: classic buckets AND a native histogram — conflict.
                series(1, &[("job", "a"), ("le", "1")], vec![s(900, 5.0)]),
                series(2, &[("job", "a"), ("le", "+Inf")], vec![s(900, 10.0)]),
                series(
                    3,
                    &[("job", "a")],
                    vec![Sample::hist(900, single_histogram().to_float())],
                ),
                // job=b: clean classic identity — evaluates normally.
                series(4, &[("job", "b"), ("le", "1")], vec![s(900, 5.0)]),
                series(5, &[("job", "b"), ("le", "+Inf")], vec![s(900, 10.0)]),
            ],
        );
        let (value, annotations) = super::evaluate(&p, &data).unwrap();
        match value {
            QueryValue::Vector(v) => {
                assert_eq!(
                    v.len(),
                    1,
                    "only the unconflicted identity evaluates: {v:?}"
                );
                assert_eq!(v[0].labels.get("job"), Some("b"));
            }
            other => panic!("expected Vector, got {other:?}"),
        }
        let (warnings, infos) = annotations.as_strings(0, 0);
        assert!(infos.is_empty(), "no info expected: {infos:?}");
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].contains("mix of classic and native histograms"),
            "got {warnings:?}"
        );
    }

    /// `#124` review finding 4: a classic bucket with a malformed
    /// (unparsable) `le` label is SKIPPED — not a 422 — and warns
    /// `bad_bucket_label_warning`; the rest of the group still evaluates.
    /// Matches pinned `resetHistograms` (`engine.go:1331-1341`).
    #[test]
    fn histogram_quantile_skips_a_malformed_le_bucket_and_warns_instead_of_erroring() {
        let expr = crate::parser::parse(r#"histogram_quantile(0.5, x_bucket)"#).unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("le", "0.1")], vec![s(0, 1.0)]),
                // Malformed `le` — must be skipped, not a hard error.
                series(2, &[("le", "notanumber")], vec![s(0, 2.0)]),
                series(3, &[("le", "1")], vec![s(0, 10.0)]),
                series(4, &[("le", "+Inf")], vec![s(0, 10.0)]),
            ],
        );
        let (value, annotations) = super::evaluate(&p, &data).unwrap();
        assert!(
            matches!(value, QueryValue::Vector(v) if v.len() == 1),
            "the query succeeds despite the malformed bucket"
        );
        let (warnings, infos) = annotations.as_strings(0, 0);
        assert!(infos.is_empty(), "no info expected: {infos:?}");
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].contains("bucket label \"le\" is missing or has a malformed value")
                && warnings[0].contains("\"notanumber\""),
            "got {warnings:?}"
        );
    }

    /// A MISSING `le` label (empty raw value, matching upstream's
    /// `labels.Get` not-found convention) is likewise skipped + warned.
    #[test]
    fn histogram_quantile_skips_a_bucket_missing_the_le_label_and_warns() {
        let expr = crate::parser::parse(r#"histogram_quantile(0.5, x_bucket)"#).unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(1, &[("le", "0.1")], vec![s(0, 1.0)]),
                // No `le` label at all.
                series(2, &[("job", "a")], vec![s(0, 2.0)]),
                series(3, &[("le", "+Inf")], vec![s(0, 10.0)]),
            ],
        );
        let (value, annotations) = super::evaluate(&p, &data).unwrap();
        assert!(matches!(value, QueryValue::Vector(v) if v.len() == 1));
        let (warnings, _) = annotations.as_strings(0, 0);
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].contains("is missing or has a malformed value of \"\""),
            "got {warnings:?}"
        );
    }

    // --- issue #153: histogram_quantiles (experimental multi-quantile) ---

    /// AC6: the `FormatOpenMetricsFloat` port (`labels/float.go:35-60`) —
    /// exactly the pinned vector. `-0.0 → "0.0"` is the load-bearing
    /// hardcode-first case (Go `-0.0 == 0` is true; the `%g` fallback
    /// would render `"-0.0"`).
    #[test]
    fn format_open_metrics_float_matches_the_pinned_vector() {
        for (input, want) in [
            (1.0, "1.0"),
            (0.0, "0.0"),
            (-0.0, "0.0"),
            (-1.0, "-1.0"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "+Inf"),
            (f64::NEG_INFINITY, "-Inf"),
            (0.5, "0.5"),
            (0.81, "0.81"),
            (100.0, "100.0"),
            (1e21, "1e+21"),
            (1e-7, "1e-07"),
        ] {
            assert_eq!(format_open_metrics_float(input), want, "input {input:?}");
        }
    }

    /// AC7 (emission half): a two-quantile call over a native histogram
    /// emits one sample per input × quantile — quantile-outer order, the
    /// label set to the formatted value, the metric name dropped, and the
    /// per-quantile value equal to the singular primitive's (row 65's
    /// median witness).
    #[test]
    fn histogram_quantiles_emits_one_sample_per_input_and_quantile() {
        let params_exp = PlanParams {
            experimental_functions: true,
            ..params(60_000, 60_000, 0)
        };
        let expr = crate::parser::parse(r#"histogram_quantiles(m, "q", 0.5, 0.9)"#).unwrap();
        let p = plan(&expr, params_exp).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![named_series(
                "m",
                1,
                &[("job", "a")],
                vec![Sample::hist(60_000, single_histogram().to_float())],
            )],
        );
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 2, "one sample per input × quantile: {v:?}");
        assert_eq!(v[0].labels.get("q"), Some("0.5"));
        assert_eq!(v[1].labels.get("q"), Some("0.9"));
        for s in &v {
            assert_eq!(s.labels.get("job"), Some("a"));
            assert_eq!(s.metric_name, None, "name dropped");
        }
        // The shared native primitive: `single_histogram`'s median is
        // √2 — exponential interpolation's midpoint of 1 < x <= 2
        // (`native_histograms.test:65`).
        assert!(
            (v[0].v - std::f64::consts::SQRT_2).abs() < 1e-12,
            "got {}",
            v[0].v
        );
    }

    /// AC7 (overwrite half): an EXISTING label under the quantile name is
    /// overwritten, never duplicated (upstream `labels.Builder.Set`).
    #[test]
    fn histogram_quantiles_overwrites_an_existing_quantile_label() {
        let params_exp = PlanParams {
            experimental_functions: true,
            ..params(60_000, 60_000, 0)
        };
        let expr = crate::parser::parse(r#"histogram_quantiles(m, "q", 0.5)"#).unwrap();
        let p = plan(&expr, params_exp).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![named_series(
                "m",
                1,
                &[("q", "stale-value")],
                vec![Sample::hist(60_000, single_histogram().to_float())],
            )],
        );
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].labels.get("q"), Some("0.5"), "overwritten, {v:?}");
        assert_eq!(
            v[0].labels.0.iter().filter(|(k, _)| k == "q").count(),
            1,
            "no duplicate entry"
        );
    }

    /// `label == "__name__"` routes to the metric-name channel (the
    /// `count_values` precedent — `Labels` never carries the name), and
    /// `drop_name: true` still removes it from the output, matching the
    /// pin's Set-then-DropName net effect.
    #[test]
    fn histogram_quantiles_routes_a_dunder_name_label_to_the_name_channel() {
        let params_exp = PlanParams {
            experimental_functions: true,
            ..params(60_000, 60_000, 0)
        };
        let expr = crate::parser::parse(r#"histogram_quantiles(m, "__name__", 0.5)"#).unwrap();
        let p = plan(&expr, params_exp).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![named_series(
                "m",
                1,
                &[("job", "a")],
                vec![Sample::hist(60_000, single_histogram().to_float())],
            )],
        );
        // Must not trip `Labels::set`'s `__name__` debug_assert; the
        // written name channel is then dropped (`drop_name: true`).
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].labels.get("__name__"), None);
        assert_eq!(v[0].metric_name, None);
        assert_eq!(v[0].labels.get("job"), Some("a"));
    }

    /// AC7 (validation half): each out-of-`[0,1]`/NaN quantile fires its
    /// own `invalid_quantile_warning` (`validateQuantile` per argument,
    /// `functions.go:2158`) — and every quantile still emits its samples
    /// (the pin computes regardless).
    #[test]
    fn histogram_quantiles_warns_per_invalid_quantile_value() {
        let params_exp = PlanParams {
            experimental_functions: true,
            ..params(60_000, 60_000, 0)
        };
        let expr = crate::parser::parse(r#"histogram_quantiles(m, "q", 1.5, -0.5, 0.5)"#).unwrap();
        let p = plan(&expr, params_exp).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![named_series(
                "m",
                1,
                &[],
                vec![Sample::hist(60_000, single_histogram().to_float())],
            )],
        );
        let (value, annotations) = super::evaluate(&p, &data).unwrap();
        let QueryValue::Vector(v) = value else {
            panic!("expected Vector");
        };
        assert_eq!(v.len(), 3, "invalid quantiles still emit: {v:?}");
        // Quantile-outer emission order (argument source order).
        let qs: Vec<_> = v.iter().map(|s| s.labels.get("q").unwrap()).collect();
        assert_eq!(qs, vec!["1.5", "-0.5", "0.5"]);
        let (warnings, _) = annotations.as_strings(0, 0);
        assert_eq!(
            warnings.len(),
            2,
            "one warn per offending value: {warnings:?}"
        );
        assert!(
            warnings.iter().any(|w| w.contains("1.5"))
                && warnings.iter().any(|w| w.contains("-0.5")),
            "got {warnings:?}"
        );
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

    // --- issue #83 (M6-08a): @ modifier + subqueries ---

    /// The epoch-anchored grid start: the smallest multiple of `step`
    /// STRICTLY GREATER than `mint`, for positive, boundary-aligned, and
    /// negative `mint` (Rust integer division truncates toward zero — the
    /// negative cases pin that the correction still lands on the right
    /// multiple, at_modifier.test:140's `(-50s, 50s]` window).
    #[test]
    fn subquery_grid_start_is_the_smallest_step_multiple_strictly_above_mint() {
        assert_eq!(subquery_grid_start(15_000, 3_000), 18_000);
        assert_eq!(subquery_grid_start(897, 10), 900);
        assert_eq!(subquery_grid_start(900, 10), 910, "boundary is exclusive");
        assert_eq!(subquery_grid_start(0, 1_000), 1_000);
        assert_eq!(subquery_grid_start(-50_000, 25_000), -25_000);
        assert_eq!(subquery_grid_start(-60_000, 25_000), -50_000);
        assert_eq!(subquery_grid_start(-1, 25), 0);
    }

    #[test]
    fn an_at_fixed_selector_is_step_invariant_across_a_range_query() {
        let expr = crate::parser::parse("up @ 100").unwrap();
        let p = plan(&expr, params(0, 120_000, 60_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(
                1,
                &[("job", "a")],
                vec![s(60_000, 5.0), s(100_000, 7.0), s(120_000, 9.0)],
            )],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Matrix(m) => {
                assert_eq!(m.len(), 1);
                // Every step reads the fixed time 100s → 7.0.
                assert_eq!(m[0].points, vec![(0, 7.0), (60_000, 7.0), (120_000, 7.0)]);
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    #[test]
    fn offset_applies_relative_to_at() {
        let expr = crate::parser::parse("up @ 100 offset 50s").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![series(1, &[], vec![s(50_000, 3.0), s(100_000, 9.0)])],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].v, 3.0, "lookup time is @100s − 50s = 50s");
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    /// The issue #83 round-2 evaluation-count gate: over a multi-step
    /// outer range query, a subquery's inner expression is evaluated
    /// EXACTLY once per union-grid point — a per-outer-step reevaluation
    /// implementation counts a multiple of the grid size and fails here.
    #[test]
    fn subqueries_materialize_once_over_the_union_grid() {
        let expr = crate::parser::parse("sum_over_time(up[60s:10s])").unwrap();
        // 7 outer steps (0..=60s step 10s); overlapping windows share
        // their inner-grid points.
        let p = plan(&expr, params(0, 60_000, 10_000)).unwrap();
        let mut data = SeriesData::new();
        let samples: Vec<Sample> = (0..=12).map(|k| s(k * 10_000, 1.0)).collect();
        data.insert(0, vec![series(1, &[("job", "a")], samples)]);
        let (value, counts, _annotations) = evaluate_counted(&p, &data).unwrap();
        // Union grid: multiples of 10s in (0−60s, 60s] = (−60s, 60s] →
        // {−50s, …, 60s} = 12 points. A per-outer-step reevaluation
        // implementation would count one eval per (window, point) pair —
        // 7 overlapping windows of 6–7 points each, i.e. ≫ 12.
        assert_eq!(
            counts.inner_evals, 12,
            "the inner expression must evaluate once per distinct union-grid point"
        );
        // Issue #95: the `up` inner is non-`@` ⇒ not step-invariant ⇒
        // unmarked; the subquery-inner freeze registers no root here.
        assert_eq!(counts.step_invariant_evals, 0);
        // And the sliced windows are still per-step correct: `up` has
        // samples from 0s on, so the sum of 1.0-valued grid points in
        // (t−60s, t] grows to 6 and saturates.
        let QueryValue::Matrix(m) = value else {
            panic!("expected Matrix");
        };
        assert_eq!(
            m[0].points,
            vec![
                (0, 1.0),
                (10_000, 2.0),
                (20_000, 3.0),
                (30_000, 4.0),
                (40_000, 5.0),
                (50_000, 6.0),
                (60_000, 6.0),
            ]
        );
    }

    /// Nested subqueries materialize inside-out under the same rule: the
    /// inner subquery evaluates once over ITS union grid (driven by the
    /// outer's grid extent), never per outer-grid point.
    #[test]
    fn nested_subqueries_materialize_inside_out() {
        let expr = crate::parser::parse("sum_over_time(sum_over_time(up[10s:10s])[30s:10s] @ 100)")
            .unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        let samples: Vec<Sample> = (0..=10).map(|k| s(k * 10_000, 1.0)).collect();
        data.insert(0, vec![series(1, &[], samples)]);
        let (value, counts, _annotations) = evaluate_counted(&p, &data).unwrap();
        // Outer grid: (70s, 100s] step 10s = {80, 90, 100}s → 3 evals of
        // the outer's inner expression. Inner union grid (materialized
        // FIRST, over the outer grid's extent): windows (70,80] ∪ (80,90]
        // ∪ (90,100] step 10s = {80, 90, 100}s → 3 evals of `up`. Exact
        // total: 3 + 3 = 6.
        assert_eq!(counts.inner_evals, 6);
        // Issue #95: the two `up` inners are non-`@` ⇒ not step-invariant ⇒
        // the #95 subquery-inner freeze marks nothing here. But the outer
        // subquery's `@ 100` fixes its whole result, so `sum_over_time(… @
        // 100)` is a step-invariant ROOT — the pre-existing #88 outer-root
        // freeze (unchanged by #95) marks it once. Verified pre-existing:
        // the count is 1 with the #95 sq.inner freeze disabled too.
        assert_eq!(counts.step_invariant_evals, 1);
        let QueryValue::Vector(v) = value else {
            panic!("expected Vector");
        };
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].v, 3.0, "three inner windows of one 1.0 sample each");
    }

    // -----------------------------------------------------------------
    // Issue #83 plan v2: annotation-capability-aware sparse envelope
    // pruning.
    // -----------------------------------------------------------------

    /// AC2: a sparse outer range whose step (600s) far exceeds the
    /// subquery's own range (30s) prunes the inner materialization to
    /// the DISCRETE UNION of the seven disjoint `(t−30s, t]` windows (21
    /// points) — never the 363-point envelope this same query would
    /// materialize on a capable inner (see the gap-annotation regression
    /// below). `up` is FREE (a bare selector), so pruning fires.
    #[test]
    fn sparse_subqueries_materialize_only_the_window_union() {
        let expr = crate::parser::parse("sum_over_time(up[30s:10s])").unwrap();
        let p = plan(&expr, params(0, 3_600_000, 600_000)).unwrap();
        let mut data = SeriesData::new();
        let samples: Vec<Sample> = (0..=360).map(|k| s(k * 10_000, 1.0)).collect();
        data.insert(0, vec![series(1, &[("job", "a")], samples)]);
        let (value, counts, annotations) = evaluate_counted(&p, &data).unwrap();
        assert_eq!(
            counts.inner_evals, 21,
            "seven disjoint 3-point windows, deduped and unioned — never the 363-point envelope"
        );
        assert!(
            annotations.is_empty(),
            "a free (bare-selector) inner emits nothing regardless of pruning"
        );
        let QueryValue::Matrix(m) = value else {
            panic!("expected Matrix");
        };
        assert_eq!(m.len(), 1);
        assert_eq!(
            m[0].points,
            vec![
                (0, 1.0),
                (600_000, 3.0),
                (1_200_000, 3.0),
                (1_800_000, 3.0),
                (2_400_000, 3.0),
                (3_000_000, 3.0),
                (3_600_000, 3.0),
            ]
        );
    }

    /// AC2 (materialized-length half): the per-series materialized
    /// sample count is exactly the live points that resolve to a real
    /// sample — 19, not 21. Two of the union's 21 grid points (the first
    /// window's `−20s`/`−10s`, both before `up`'s earliest real sample at
    /// `0s`) evaluate to an empty vector and contribute nothing to the
    /// accumulator, even though they still count as one `eval_step` each
    /// (`inner_evals` stays 21 — proven by the sibling test above).
    #[test]
    fn sparse_subquery_materialized_length_matches_live_points_with_data() {
        let expr = crate::parser::parse("sum_over_time(up[30s:10s])").unwrap();
        let p = plan(&expr, params(0, 3_600_000, 600_000)).unwrap();
        let mut data = SeriesData::new();
        let samples: Vec<Sample> = (0..=360).map(|k| s(k * 10_000, 1.0)).collect();
        data.insert(0, vec![series(1, &[("job", "a")], samples)]);

        let PlanExpr::OverTime {
            source: RangeSource::Subquery(sq),
            ..
        } = &p.root
        else {
            panic!("expected an OverTime node over a subquery source");
        };
        let mut caches = EvalCaches::default();
        let mut inner_evals = 0u64;
        let mut classifier = crate::plan::StepInvariance::new(&p.selectors);
        prepare_subqueries(
            &p.root,
            &p.selectors,
            &EvalData::new(&p.selectors, &data),
            StepGrid::Dense(Horizon {
                start_ms: p.params.start_ms,
                end_ms: p.params.end_ms,
                step_ms: p.params.step_ms,
            }),
            p.params.lookback_ms,
            &mut caches,
            &mut inner_evals,
            &mut classifier,
        )
        .unwrap();
        let materialized = caches.subqueries.get(&(sq.as_ref() as *const _)).unwrap();
        assert_eq!(materialized.series.len(), 1);
        assert_eq!(
            materialized.series[0].samples.len(),
            19,
            "21 live points minus the 2 pre-data gap points (−20s, −10s) in the first window"
        );
    }

    /// AC3: nested pruning compounds inside-out. The outer subquery
    /// prunes to its own window union (driven by the query's 3 steps),
    /// and that pruned live set becomes the INNER subquery's consumer
    /// grid, so the inner prunes too instead of falling back to its own
    /// envelope (a `Sparse` consumer never gets the dense short-circuit —
    /// only a `Dense` one with `step ≤ range` does).
    #[test]
    fn nested_sparse_subqueries_compound_pruning() {
        let expr =
            crate::parser::parse("count_over_time(count_over_time(up[10s:10s])[20s:10s])").unwrap();
        let p = plan(&expr, params(0, 1_200_000, 600_000)).unwrap();
        let mut data = SeriesData::new();
        // Dense 10s-spaced coverage from −10s through 1210s — every live
        // point either subquery ever visits falls inside this range.
        let samples: Vec<Sample> = (-1..=121).map(|k| s(k * 10_000, 1.0)).collect();
        data.insert(0, vec![series(1, &[("job", "a")], samples)]);
        let (value, counts, _annotations) = evaluate_counted(&p, &data).unwrap();
        // Outer subquery (range=20s, step=10s): consumer = the query's 3
        // steps {0, 600, 1200}s; each window `(t−20s, t]` holds 2 grid
        // points, all six distinct ⇒ 6 outer live points. Those 6 points
        // become the inner subquery's (range=10s, step=10s) consumer
        // grid; each window degenerates to exactly the consumer point
        // itself ⇒ 6 inner live points too. Total: 6 + 6 = 12 — the full
        // envelope is ≈244 points.
        assert_eq!(counts.inner_evals, 12);
        let QueryValue::Matrix(m) = value else {
            panic!("expected Matrix");
        };
        assert_eq!(m.len(), 1);
        assert_eq!(
            m[0].points,
            vec![(0, 2.0), (600_000, 2.0), (1_200_000, 2.0)]
        );
    }

    /// AC4: an `@`-fixed subquery is a single window — its envelope IS
    /// the union, so [`live_grid_points`] returns `None` (`sq.at_ms
    /// .is_some()`) and the full `[100s:1s] @ 100` grid (100 points)
    /// materializes exactly as before #83's round-2 amendment.
    #[test]
    fn at_fixed_subquery_grid_is_never_pruned() {
        let expr = crate::parser::parse("sum_over_time(metric[100s:1s] @ 100)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        let samples: Vec<Sample> = (0..=100).map(|k| s(k * 1_000, 1.0)).collect();
        data.insert(0, vec![series(1, &[], samples)]);
        let (_value, counts, _annotations) = evaluate_counted(&p, &data).unwrap();
        assert_eq!(
            counts.inner_evals, 100,
            "a single @-fixed window is its own full envelope — never pruned"
        );
    }

    /// AC6 (codex Q1 ruling; task-manager precision note, comment
    /// 5024580850): `(up + 1)`'s `Binary` root makes the inner
    /// unconditionally CAPABLE, so `prepare_subquery` retains the FULL
    /// envelope (363 points — the same envelope
    /// `sparse_subqueries_materialize_only_the_window_union`'s FREE `up`
    /// inner prunes to 21) even though every actual OUTPUT window is
    /// disjoint from the one histogram sample at `t=100s`. That gap
    /// point is evaluated anyway, and its
    /// `incompatible_types_in_binop_info` annotation surfaces in the
    /// response — proof the capability gate, not merely a comment, is
    /// what still forces the full-envelope evaluation. Values are
    /// asserted against an EXPLICIT full-envelope reference (hand-derived
    /// from the window arithmetic, not "identical to the free-inner
    /// `up`-only control" — `(up+1) != up`).
    #[test]
    fn capable_subquery_inner_keeps_the_envelope_and_the_gap_annotation_surfaces() {
        let expr = crate::parser::parse("sum_over_time((up + 1)[30s:10s])").unwrap();
        let p = plan(&expr, params(0, 3_600_000, 600_000)).unwrap();
        let mut data = SeriesData::new();
        let mut samples: Vec<Sample> = (0..=360).map(|k| s(k * 10_000, 1.0)).collect();
        // A lone native-histogram sample at t=100s — a GAP point: 100s
        // lies outside every consumer window `(t−30s, t]` for t ∈ {0,
        // 600, …, 3600}s (the nearest window edges are 0s and 570s).
        let gap_idx = samples
            .iter()
            .position(|s| s.t_ms == 100_000)
            .expect("100s is one of the 10s-spaced grid points");
        samples[gap_idx] = Sample::hist(100_000, single_histogram().to_float());
        data.insert(0, vec![series(1, &[("job", "a")], samples)]);

        let (value, counts, annotations) = evaluate_counted(&p, &data).unwrap();
        // (a) Full envelope retained.
        assert_eq!(counts.inner_evals, 363);
        // (b) The gap-point annotation surfaces exactly as the capable
        // `Binary` arm emits it.
        let (_warnings, infos) = annotations.base_messages();
        assert!(
            infos.iter().any(|m| m
                == &crate::annotations::messages::incompatible_types_in_binop_info(
                    "histogram",
                    "+",
                    "float"
                )),
            "the gap-point histogram+scalar info must survive pruning: {infos:?}"
        );
        // (c) Values match an EXPLICIT full-envelope reference: `up`'s
        // all-float shape sums to `2 * (element count)` per window under
        // `+1` — the gap point at 100s never falls in ANY window, so its
        // type has zero effect on any value.
        let QueryValue::Matrix(m) = value else {
            panic!("expected Matrix");
        };
        assert_eq!(m.len(), 1);
        assert_eq!(
            m[0].points,
            vec![
                (0, 2.0),
                (600_000, 6.0),
                (1_200_000, 6.0),
                (1_800_000, 6.0),
                (2_400_000, 6.0),
                (3_000_000, 6.0),
                (3_600_000, 6.0),
            ]
        );
    }

    /// AC7 (predicate unit tests): `expr_may_annotate` false for the
    /// plan's own worked FREE examples.
    #[test]
    fn expr_may_annotate_is_false_for_the_plans_free_worked_examples() {
        for q in [
            "up",
            "count_over_time(up[10s:10s])",
            "label_replace(up, \"x\", \"$1\", \"job\", \"(.*)\")",
            "abs(up)",
        ] {
            let expr = crate::parser::parse(q).unwrap();
            let p = plan(&expr, params(0, 0, 0)).unwrap();
            assert!(!expr_may_annotate(&p.root), "{q:?} must classify FREE");
        }
    }

    /// AC7: `expr_may_annotate` true for the plan's own worked CAPABLE
    /// examples, including the nested-subquery-poisons-ancestors case
    /// (`count_over_time`'s own func is FREE, but its subquery inner
    /// wraps a capable `rate`).
    #[test]
    fn expr_may_annotate_is_true_for_the_plans_capable_worked_examples() {
        for q in [
            "rate(up[1m])",
            "up + 1",
            "up > 0",
            "sum(up)",
            "histogram_quantile(0.9, up)",
            "count_over_time(rate(up[1m])[10s:10s])",
        ] {
            let expr = crate::parser::parse(q).unwrap();
            let p = plan(&expr, params(0, 0, 0)).unwrap();
            assert!(expr_may_annotate(&p.root), "{q:?} must classify CAPABLE");
        }
    }

    /// AC7: a capable grandchild under a FREE parent poisons the whole
    /// tree — `sort()` (FREE) wrapping `rate()` (CAPABLE) must classify
    /// CAPABLE, and the reverse (`rate()` wrapping nothing capable stays
    /// CAPABLE regardless) is covered above.
    #[test]
    fn a_capable_grandchild_poisons_a_free_ancestor() {
        let expr = crate::parser::parse("sort(rate(up[1m]))").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        assert!(expr_may_annotate(&p.root));
    }

    /// AC8 (adversarial whitelist tripwire — the empirical backstop for
    /// the static predicate): every FREE shape, evaluated over data
    /// mixing floats, an exponential-schema histogram, a custom-buckets
    /// (NHCB) histogram, a NaN-sum histogram, and a "malformed" classic-
    /// bucket-shaped series (`le` label with a non-numeric value), yields
    /// EMPTY [`Annotations`]. A future change that adds data-dependent
    /// emission to one of these paths without updating the predicate
    /// would still need to move the shape here to CAPABLE — this is the
    /// runtime half of that obligation.
    #[test]
    fn every_free_shape_over_adversarial_data_emits_no_annotations() {
        let nhcb = pulsus_model::FloatHistogram {
            counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
            schema: pulsus_model::CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 3.0,
            sum: 6.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![],
            positive_buckets: vec![3.0],
            negative_buckets: vec![],
            custom_values: vec![1.0, 5.0, 10.0],
        };
        let nan_sum = pulsus_model::FloatHistogram {
            counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: f64::NAN,
            sum: f64::NAN,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1.0],
            negative_buckets: vec![],
            custom_values: vec![],
        };

        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(
                    1,
                    &[("job", "a")],
                    vec![
                        s(1_000, 5.0),
                        Sample::hist(2_000, single_histogram().to_float()),
                        Sample::hist(3_000, nhcb),
                        Sample::hist(4_000, nan_sum),
                        s(5_000, 7.0),
                    ],
                ),
                // A "malformed" classic-bucket-shaped series: an `le`
                // label whose value never parses as a bucket boundary.
                // None of the FREE shapes below ever interpret `le`
                // specially (only `histogram_quantile`'s classic-bucket
                // reconstruction does, and that shape is CAPABLE).
                series(
                    2,
                    &[("job", "a"), ("le", "not-a-number")],
                    vec![s(1_000, 1.0), s(3_000, 2.0), s(5_000, 3.0)],
                ),
            ],
        );

        let queries = [
            "m{job=\"a\"}",
            "5",
            "time()",
            "vector(time())",
            "last_over_time(m{job=\"a\"}[10s])",
            "count_over_time(m{job=\"a\"}[10s])",
            "present_over_time(m{job=\"a\"}[10s])",
            "resets(m{job=\"a\"}[10s])",
            "changes(m{job=\"a\"}[10s])",
            "absent_over_time(nonexistent{job=\"a\"}[10s])",
            "absent(nonexistent{job=\"a\"})",
            "count(m{job=\"a\"})",
            "group(m{job=\"a\"})",
            "count_values(\"v\", m{job=\"a\"})",
            "m{job=\"a\"} and m{job=\"a\"}",
            "abs(m{job=\"a\"})",
            "pi()",
            "year(m{job=\"a\"})",
            "timestamp(m{job=\"a\"})",
            "scalar(m{job=\"a\"})",
            "vector(5)",
            "sort(m{job=\"a\"})",
            "label_replace(m{job=\"a\"}, \"x\", \"$1\", \"job\", \"(.*)\")",
            "label_join(m{job=\"a\"}, \"x\", \"-\", \"job\")",
            "histogram_count(m{job=\"a\"})",
            "histogram_sum(m{job=\"a\"})",
            "histogram_avg(m{job=\"a\"})",
        ];
        for q in queries {
            let expr = crate::parser::parse(q).unwrap();
            let p = plan(&expr, params(0, 5_000, 0)).unwrap();
            assert!(!expr_may_annotate(&p.root), "{q:?} must classify FREE");
            let (_value, annotations) = super::evaluate(&p, &data).unwrap();
            assert!(
                annotations.is_empty(),
                "{q:?} emitted annotations over adversarial data: {annotations:?}"
            );
        }
    }

    /// AC9 / edge case 4: a `Sparse` consumer with an EMPTY live set
    /// (reachable when an enclosing subquery's own pruning yields zero
    /// points) still inserts a materialized cache entry — the
    /// `windowed_range_source` invariant ("`prepare_subqueries`
    /// materializes every subquery before stepping") tolerates no
    /// exceptions, even for a subquery whose own grid never gets a
    /// single live point.
    #[test]
    fn a_sparse_consumer_with_no_live_points_still_inserts_an_empty_cache_entry() {
        let expr = crate::parser::parse("sum_over_time(up[10s:10s])").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let PlanExpr::OverTime {
            source: RangeSource::Subquery(sq),
            ..
        } = &p.root
        else {
            panic!("expected an OverTime node over a subquery source");
        };
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, 1.0)])]);
        let mut caches = EvalCaches::default();
        let mut inner_evals = 0u64;
        let mut classifier = crate::plan::StepInvariance::new(&p.selectors);
        prepare_subquery(
            sq,
            &p.selectors,
            &EvalData::new(&p.selectors, &data),
            StepGrid::Sparse {
                envelope: Horizon {
                    start_ms: 0,
                    end_ms: 100_000,
                    step_ms: 10_000,
                },
                live: &[],
            },
            p.params.lookback_ms,
            &mut caches,
            &mut inner_evals,
            &mut classifier,
        )
        .unwrap();
        assert_eq!(
            inner_evals, 0,
            "no consumer points ⇒ no grid points to evaluate"
        );
        let materialized = caches
            .subqueries
            .get(&(sq.as_ref() as *const _))
            .expect("the cache entry invariant holds even for an empty live grid");
        assert!(materialized.series.is_empty());
    }

    /// Edge case 2: `mint_min`/`maxt_max`/`grid_start` must derive from
    /// the CONSUMER's envelope, never from its (possibly pruned) live
    /// set — upstream's child-evaluator bounds are envelope bounds
    /// regardless of any client-side pruning. `(m + 0)` is `Binary` ⇒
    /// CAPABLE ⇒ never pruned, so the materialization loop always walks
    /// the DENSE, envelope-derived grid and its first point directly
    /// reveals `grid_start`. A `live[0]`-derived anchor bug would instead
    /// materialize starting at `5000`, not `4000`.
    #[test]
    fn subquery_anchors_derive_from_the_envelope_not_the_live_set() {
        let expr = crate::parser::parse("sum_over_time((m{job=\"a\"} + 0)[1s:1s])").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let PlanExpr::OverTime {
            source: RangeSource::Subquery(sq),
            ..
        } = &p.root
        else {
            panic!("expected an OverTime node over a subquery source");
        };
        let mut data = SeriesData::new();
        let samples: Vec<Sample> = (3..=9).map(|k| s(k * 1_000, 1.0)).collect();
        data.insert(0, vec![series(1, &[("job", "a")], samples)]);

        let envelope = Horizon {
            start_ms: 4_000,
            end_ms: 9_000,
            step_ms: 1_000,
        };
        let live = [5_000i64];
        assert!(
            live[0] > envelope.start_ms,
            "the live set's first point must NOT be the envelope start"
        );

        let mut caches = EvalCaches::default();
        let mut inner_evals = 0u64;
        let mut classifier = crate::plan::StepInvariance::new(&p.selectors);
        prepare_subquery(
            sq,
            &p.selectors,
            &EvalData::new(&p.selectors, &data),
            StepGrid::Sparse {
                envelope,
                live: &live,
            },
            p.params.lookback_ms,
            &mut caches,
            &mut inner_evals,
            &mut classifier,
        )
        .unwrap();
        let materialized = caches
            .subqueries
            .get(&(sq.as_ref() as *const _))
            .expect("cache entry present");
        assert_eq!(
            materialized.series[0].samples[0].t_ms, 4_000,
            "grid_start must be envelope-derived (4000), not live[0]-derived (5000)"
        );
        assert_eq!(
            inner_evals, 6,
            "grid_start=4000..=maxt_max=9000 step 1000 => 6 points"
        );
    }

    /// Code review [medium] (comment 5025425065): `live_grid_points`
    /// must compute the union with a monotonic cursor over the
    /// pre-sorted windows, never by pushing every candidate per window
    /// and globally sort/dedup-ing. Forty sparse consumer points 5s
    /// apart, each carrying a 100s window at a 1s subquery step —
    /// consecutive windows overlap 95%, so a per-window-push
    /// implementation materializes 40 × 100 = 4000 temporary entries
    /// for a 295-point union. Asserts (a) the emitted points equal a
    /// naive per-window-union reference, and (b) the buffer NEVER held a
    /// duplicate: the cursor path has no dedup pass, so any duplicate
    /// push would survive into the returned Vec and fail both the
    /// strictly-ascending walk and the exact 295 length (the
    /// work-proportionality seam — the output IS the raw buffer).
    #[test]
    fn live_grid_points_with_heavily_overlapping_windows_emits_each_point_once() {
        let expr = crate::parser::parse("sum_over_time(up[100s:1s])").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let PlanExpr::OverTime {
            source: RangeSource::Subquery(sq),
            ..
        } = &p.root
        else {
            panic!("expected an OverTime node over a subquery source");
        };

        // 40 consumer points: 100s, 105s, …, 295s (windows (t−100s, t]).
        let consumer_points: Vec<i64> = (0..40).map(|k| 100_000 + k * 5_000).collect();
        let consumer = StepGrid::Sparse {
            envelope: Horizon {
                start_ms: 100_000,
                end_ms: 295_000,
                step_ms: 5_000,
            },
            live: &consumer_points,
        };
        // Envelope-derived bounds, exactly as `prepare_subquery` computes
        // them from `consumer.envelope().span()`.
        let mint_min = 100_000 - sq.range_ms;
        let maxt_max = 295_000;

        let live = live_grid_points(sq, consumer, mint_min, maxt_max)
            .expect("a sparse consumer without @ must take the pruning path");

        // (a) Value: identical to the naive per-window union.
        let mut reference = std::collections::BTreeSet::new();
        for &t in &consumer_points {
            let eff_t = t - sq.offset_ms;
            let mut p = subquery_grid_start(eff_t - sq.range_ms, sq.step_ms);
            while p <= eff_t {
                reference.insert(p);
                p += sq.step_ms;
            }
        }
        let reference: Vec<i64> = reference.into_iter().collect();
        assert_eq!(live, reference);

        // (b) Work: the union is 295 points (100 from the first window +
        // 5 new per subsequent window) and the returned buffer holds
        // exactly them, strictly ascending — a per-window-push
        // implementation would have returned 4000 entries (or needed the
        // removed dedup pass to hide them).
        assert_eq!(live.len(), 295);
        assert!(
            live.windows(2).all(|w| w[0] < w[1]),
            "no duplicate may ever enter the output buffer"
        );
    }

    /// Issue #88 (Δ2, the Tier-1 once-and-copy gate at the eval site): a
    /// wrappable step-invariant root is genuinely evaluated EXACTLY once
    /// across a multi-step range — a per-step recompute counts once per
    /// step (4 here) and, this being the aggregate-param quirk query,
    /// also flips the values (frozen `k = time()%2 = 1` vs oscillating
    /// `1 _ 1 _`) — double-caught.
    #[test]
    fn step_invariant_roots_evaluate_once_across_a_range() {
        let expr = crate::parser::parse("topk(time() % 2, m{job=\"a\"} @ 10)").unwrap();
        // Range 1s..4s step 1s — 4 steps, odd start ⇒ frozen k = 1.
        let p = plan(&expr, params(1_000, 4_000, 1_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(10_000, 1.0)])]);
        let (value, counts, _annotations) = evaluate_counted(&p, &data).unwrap();
        assert_eq!(
            counts.step_invariant_evals, 1,
            "the marked aggregate root must evaluate once, not per step"
        );
        let QueryValue::Matrix(m) = value else {
            panic!("expected Matrix");
        };
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].metric_name.as_deref(), Some("m"), "topk keeps names");
        assert_eq!(
            m[0].points,
            vec![(1_000, 1.0), (2_000, 1.0), (3_000, 1.0), (4_000, 1.0)],
            "the frozen start-step value is copied to every step"
        );
    }

    /// Issue #88 (v1 AC2): the plain (non-quirk) once-and-copy shape —
    /// `abs(m @ 30)` over 7 steps evaluates the marked root once. Value
    /// parity for this class already held pre-#88 (the `@` pushdown), so
    /// the counter is the load-bearing assert.
    #[test]
    fn an_at_fixed_function_root_evaluates_once_over_seven_steps() {
        let expr = crate::parser::parse("abs(m{job=\"a\"} @ 30)").unwrap();
        let p = plan(&expr, params(0, 60_000, 10_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(30_000, -3.0)])]);
        let (value, counts, _annotations) = evaluate_counted(&p, &data).unwrap();
        assert_eq!(counts.step_invariant_evals, 1);
        let QueryValue::Matrix(m) = value else {
            panic!("expected Matrix");
        };
        assert_eq!(m.len(), 1);
        assert!(m[0].points.iter().all(|p| p.v == 3.0), "{:?}", m[0]);
        assert_eq!(m[0].points.len(), 7);
    }

    /// The flagship #95 sample set: `m{job="a"} = t/10` on a 10s grid, so
    /// `m @ 30 = 3` (frozen at the pin) and the plain `m{job="a"}` reads
    /// 0 at 7–9s and 1 at 10–13s (last-sample lookback).
    fn m_ten_second_grid() -> Vec<Sample> {
        vec![s(0, 0.0), s(10_000, 1.0), s(20_000, 2.0), s(30_000, 3.0)]
    }

    /// Issue #95 (Tier-1 gate — the subquery-inner freeze count): the
    /// step-invariant `topk(time()%2, m @ 30)` nested INSIDE a non-`@`-fixed
    /// subquery is frozen ONCE at the subquery grid start (`subqStart = 3`
    /// ⇒ `time()%2 = 1` ⇒ `k = 1`) and copied to every grid point, so
    /// `step_invariant_evals == 1`. A freeze-removal regresses to 0 (nothing
    /// marked), a per-grid-point recompute to `#grid` — both trip this gate
    /// AND the value goldens (the frozen `k = 1` vs oscillating parity).
    #[test]
    fn a_step_invariant_subquery_inner_freezes_once_at_the_grid_start() {
        let expr =
            crate::parser::parse("sum_over_time((topk(time() % 2, m{job=\"a\"} @ 30))[4s:1s])")
                .unwrap();
        // Range 6s..9s step 1s: subqStart = 3 ⇒ time()%2 = 1 ⇒ k = 1.
        let p = plan(&expr, params(6_000, 9_000, 1_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], m_ten_second_grid())]);
        let (value, counts, _annotations) = evaluate_counted(&p, &data).unwrap();
        assert_eq!(
            counts.step_invariant_evals, 1,
            "the invariant subquery inner freezes exactly once at the subquery grid start"
        );
        let QueryValue::Matrix(m) = value else {
            panic!("expected Matrix");
        };
        assert_eq!(m.len(), 1);
        // Each grid point copies the frozen m@30 = 3; each outer window
        // holds 4 grid points ⇒ 12 at every step.
        assert_eq!(
            m[0].points,
            vec![(6_000, 12.0), (7_000, 12.0), (8_000, 12.0), (9_000, 12.0)]
        );
    }

    /// Issue #95 (the discriminating anchor gate): the freeze anchor is the
    /// SUBQUERY grid start — derived from the OUTER query start — not the
    /// eval timestamp. The identical query as an instant at 7s yields empty
    /// (`subqStart = 4` ⇒ `time()%2 = 0` ⇒ `topk(0)`), but the 7s STEP of the
    /// range 6s..9s yields 12 (`subqStart = 3` ⇒ `k = 1`): same eval
    /// timestamp, different value. No per-step or per-grid-point model can
    /// produce this pair — it is the load-bearing outer-start-anchor proof.
    #[test]
    fn the_subquery_inner_freeze_anchors_at_the_outer_start_not_the_eval_step() {
        let expr =
            crate::parser::parse("sum_over_time((topk(time() % 2, m{job=\"a\"} @ 30))[4s:1s])")
                .unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], m_ten_second_grid())]);

        // Instant at 7s: subqStart = 4 ⇒ k = 0 ⇒ topk(0) ⇒ empty.
        let p = plan(&expr, params(7_000, 7_000, 0)).unwrap();
        let (value, _, _annotations) = evaluate_counted(&p, &data).unwrap();
        let QueryValue::Vector(v) = value else {
            panic!("expected Vector");
        };
        assert!(
            v.is_empty(),
            "instant@7s freezes topk(0) ⇒ empty, got {v:?}"
        );

        // Range 6s..9s: the 7s step reads 12, frozen at subqStart = 3 (k = 1).
        let p = plan(&expr, params(6_000, 9_000, 1_000)).unwrap();
        let (value, _, _annotations) = evaluate_counted(&p, &data).unwrap();
        let QueryValue::Matrix(m) = value else {
            panic!("expected Matrix");
        };
        assert_eq!(m.len(), 1);
        let seven = m[0].points.iter().find(|p| p.t_ms == 7_000).map(|p| p.v);
        assert_eq!(
            seven,
            Some(12.0),
            "the 7s step of the range is frozen at the outer start (12), not empty like instant@7"
        );
    }

    /// Issue #95 (partial-invariant nesting): only the step-invariant `topk`
    /// subtree is frozen (`subqStart = 7` ⇒ `k = 1` ⇒ `m@30 = 3`); the
    /// sibling `m{job="a"}` VARIES per grid point (0 at 7–9s, 1 at 10–13s by
    /// last-sample lookback) and is windowed per outer step. Exactly one
    /// freeze; the `13 14 15 16` ramp proves the frozen and per-point halves
    /// coexist on disjoint node addresses.
    #[test]
    fn a_partially_invariant_subquery_inner_freezes_only_the_invariant_subtree() {
        let expr = crate::parser::parse(
            "sum_over_time((topk(time() % 2, m{job=\"a\"} @ 30) + m{job=\"a\"})[4s:1s])",
        )
        .unwrap();
        // Range 10s..13s step 1s: subqStart = 7 ⇒ time()%2 = 1 ⇒ k = 1.
        let p = plan(&expr, params(10_000, 13_000, 1_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], m_ten_second_grid())]);
        data.insert(1, vec![series(1, &[("job", "a")], m_ten_second_grid())]);
        let (value, counts, _annotations) = evaluate_counted(&p, &data).unwrap();
        assert_eq!(
            counts.step_invariant_evals, 1,
            "only the topk subtree is frozen; the sibling m{{job=a}} varies per grid point"
        );
        let QueryValue::Matrix(m) = value else {
            panic!("expected Matrix");
        };
        assert_eq!(m.len(), 1);
        assert_eq!(
            m[0].points,
            vec![
                (10_000, 13.0),
                (11_000, 14.0),
                (12_000, 15.0),
                (13_000, 16.0)
            ]
        );
    }

    /// Issue #93 (finding 3 — the M6-08 reclaim Tier-1 gate): a
    /// `drop_name`-FREE range matrix short-circuits
    /// [`finalize_metadata_labels`]'s `Matrix` arm, so
    /// `finalize_matrix_merge_passes` stays 0; a `drop_name` range falls
    /// through the full clone + merge pass and counts > 0.
    ///
    /// The gate proves the skip is OUTCOME-neutral, not merely
    /// counter-neutral, on BOTH branches (review round 1, finding 2):
    ///
    /// - **short-circuit branch** — the real `count_values` pipeline output
    ///   (drop_name-free, metadata-free, `metric_name: None`) is fed BACK
    ///   through `finalize_metadata_labels` with `drop_name` forced true,
    ///   which forces the full pass over the SAME series content (strip and
    ///   name-null are then content no-ops). The two results are asserted
    ///   identical on labels / name / points / order — so the taken
    ///   short-circuit provably yields exactly what the full pass yields on
    ///   the same input.
    /// - **full-pass branch** — a hand-built `drop_name` matrix whose two
    ///   series share a POST-strip identity with disjoint timestamps is
    ///   asserted to merge into one series with the metadata label stripped,
    ///   the name nulled, and the combined, time-sorted points — the exact
    ///   expected output, so the gate detects any full-pass output change.
    #[test]
    fn finalize_skips_the_matrix_merge_pass_when_no_series_is_drop_marked() {
        // --- short-circuit branch: `count_values` retains no name and marks
        // --- no series (aggregation stamps `drop_name: false`), the exact
        // --- M6-08 count_values-range shape the reclaim targets.
        let expr = crate::parser::parse(r#"count_values("v", m{job="a"})"#).unwrap();
        let p = plan(&expr, params(0, 20_000, 10_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                series(
                    1,
                    &[("job", "a")],
                    vec![s(0, 1.0), s(10_000, 1.0), s(20_000, 2.0)],
                ),
                series(
                    2,
                    &[("job", "a")],
                    vec![s(0, 1.0), s(10_000, 3.0), s(20_000, 2.0)],
                ),
            ],
        );
        let (value, counts, _annotations) = evaluate_counted(&p, &data).unwrap();
        assert_eq!(
            counts.finalize_matrix_merge_passes, 0,
            "a drop_name-free range must short-circuit the finalize Matrix merge pass"
        );
        let QueryValue::Matrix(sc) = value else {
            panic!("expected Matrix");
        };
        assert!(!sc.is_empty());
        assert!(
            sc.iter().all(|sers| sers.metric_name.is_none()),
            "count_values output carries no metric name"
        );

        // Outcome-neutrality of the SKIP: force the full pass over the same
        // series content (metadata-free, `metric_name: None`, so
        // `drop_name = true` leaves strip + name-null content-neutral) and
        // prove it reproduces the short-circuit result on the
        // outcome-relevant fields — labels, name, points, and order.
        let forced = QueryValue::Matrix(
            sc.iter()
                .cloned()
                .map(|mut r| {
                    r.drop_name = true;
                    r
                })
                .collect(),
        );
        let mut forced_passes = 0u64;
        let QueryValue::Matrix(fp) = finalize_metadata_labels(forced, &mut forced_passes).unwrap()
        else {
            panic!("expected Matrix");
        };
        assert!(
            forced_passes > 0,
            "forcing drop_name must exercise the full merge pass, not the skip"
        );
        assert_eq!(
            sc.len(),
            fp.len(),
            "short-circuit and full pass disagree on series count"
        );
        for (a, b) in sc.iter().zip(&fp) {
            assert_eq!(
                a.labels, b.labels,
                "short-circuit vs full-pass labels diverge"
            );
            assert_eq!(
                a.metric_name, b.metric_name,
                "short-circuit vs full-pass metric_name diverge"
            );
            assert_eq!(
                a.points, b.points,
                "short-circuit vs full-pass points diverge"
            );
        }

        // --- full-pass branch: two `drop_name` series that share a POST-strip
        // --- identity with disjoint timestamps MUST merge into one series
        // --- with `__unit__` stripped, the name nulled, and the combined,
        // --- time-sorted points. Asserts the exact expected output so the
        // --- gate catches any change in what the full pass produces.
        let lbls = |unit: bool| {
            let mut v = vec![("job".to_string(), "a".to_string())];
            if unit {
                v.push(("__unit__".to_string(), "seconds".to_string()));
            }
            Labels::new(v)
        };
        let matrix = QueryValue::Matrix(vec![
            RangeSeries {
                labels: lbls(true),
                metric_name: Some("http_seconds".to_string()),
                drop_name: true,
                points: vec![Point::float(0, 1.0), Point::float(10_000, 2.0)],
            },
            RangeSeries {
                labels: lbls(true),
                metric_name: Some("http_seconds".to_string()),
                drop_name: true,
                points: vec![Point::float(20_000, 3.0)],
            },
        ]);
        let mut passes = 0u64;
        let QueryValue::Matrix(merged) = finalize_metadata_labels(matrix, &mut passes).unwrap()
        else {
            panic!("expected Matrix");
        };
        assert_eq!(
            passes, 1,
            "a drop_name matrix must run the full finalize Matrix merge pass"
        );
        assert_eq!(merged.len(), 1, "post-strip identical series must merge");
        assert_eq!(
            merged[0].labels,
            lbls(false),
            "__unit__ must be stripped from a dropped-name series"
        );
        assert_eq!(
            merged[0].metric_name, None,
            "a dropped name must be nulled by the full pass"
        );
        assert_eq!(
            merged[0].points,
            vec![(0, 1.0), (10_000, 2.0), (20_000, 3.0)],
            "merged points must be the combined, time-sorted union"
        );
    }

    /// Issue #88 (Δ4): the step-invariant cache is fresh per `evaluate`
    /// call — the same plan evaluated against different data reflects
    /// each call's own inputs (a leaked address-keyed cache would return
    /// the first call's frozen value for the second).
    #[test]
    fn the_step_invariant_cache_is_fresh_per_evaluate_call() {
        let expr = crate::parser::parse("abs(m{job=\"a\"} @ 30)").unwrap();
        let p = plan(&expr, params(0, 20_000, 10_000)).unwrap();
        for want in [3.0, 7.0] {
            let mut data = SeriesData::new();
            data.insert(0, vec![series(1, &[("job", "a")], vec![s(30_000, -want)])]);
            let QueryValue::Matrix(m) = evaluate(&p, &data).unwrap() else {
                panic!("expected Matrix");
            };
            assert_eq!(m.len(), 1);
            assert!(
                m[0].points.iter().all(|p| p.v == want),
                "want {want}: {:?}",
                m[0]
            );
        }
    }

    /// Issue #88 (review round 1, finding 3): the prepare walk is
    /// single-pass — over a left-deep chain of `n` additions (2n+1 plan
    /// nodes, none invariant, so the walk descends the whole chain) the
    /// classifier COMPUTES each node's verdict exactly once (memo hits
    /// excluded); the pre-review shape re-walked every suffix per level
    /// (≈ n²/2 ≫ 2n+1 computes).
    #[test]
    fn the_prepare_walk_classifies_each_node_once() {
        let n = 60;
        let query = format!("m{}", " + 1".repeat(n));
        let expr = crate::parser::parse(&query).unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut classifier = crate::plan::StepInvariance::new(&p.selectors);
        let mut caches = EvalCaches::default();
        let data = SeriesData::new();
        prepare_step_invariant(
            &p.root,
            &p.selectors,
            &EvalData::new(&p.selectors, &data),
            0,
            300_000,
            &mut caches,
            &mut classifier,
        )
        .unwrap();
        assert_eq!(
            classifier.computed,
            2 * n as u64 + 1,
            "each of the 2n+1 nodes must be classified exactly once"
        );
    }

    /// Issue #88 (Δ1 negative control): the freeze needs an `@`-fixed
    /// expression — without one the aggregate is unmarked and the
    /// eval-time-dependent `k` still oscillates per step (`5 _ 5 _`,
    /// upstream-identical), and no genuine step-invariant eval happens.
    #[test]
    fn an_aggregate_without_at_is_not_marked_and_still_oscillates() {
        let expr = crate::parser::parse("topk(time() % 2, m{job=\"a\"})").unwrap();
        let p = plan(&expr, params(1_000, 4_000, 1_000)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, 5.0)])]);
        let (value, counts, _annotations) = evaluate_counted(&p, &data).unwrap();
        assert_eq!(counts.step_invariant_evals, 0);
        let QueryValue::Matrix(m) = value else {
            panic!("expected Matrix");
        };
        assert_eq!(m.len(), 1);
        assert_eq!(
            m[0].points,
            vec![(1_000, 5.0), (3_000, 5.0)],
            "k oscillates 1,0,1,0 — only the odd-second steps emit"
        );
    }

    /// at_modifier.test:227-255's shape: a subquery `@` pointed at a
    /// data-free time returns empty — never panics (upstream note: "these
    /// were panicking before the fix").
    #[test]
    fn a_subquery_at_a_data_free_time_is_empty_not_a_panic() {
        for query in [
            "max_over_time(up[1h:1m] @ 1111111000)",
            "predict_linear(up[1h:1m] @ 1111111000, 0.1)",
            "rate(up[1h:1m] @ 1111111000)",
        ] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, params(1_111_111_000_000, 1_111_111_000_000, 0)).unwrap();
            let mut data = SeriesData::new();
            data.insert(0, vec![series(1, &[], vec![s(0, 1.0), s(10_000, 2.0)])]);
            match evaluate(&p, &data).unwrap() {
                QueryValue::Vector(v) => assert!(v.is_empty(), "{query}: {v:?}"),
                other => panic!("{query}: expected Vector, got {other:?}"),
            }
        }
    }

    /// `last_over_time` over a subquery keeps the materialized series'
    /// own metric name (the selector-source path keeps the selector's) —
    /// the keeps-name rule survives the source generalization.
    #[test]
    fn last_over_time_over_a_subquery_keeps_the_inner_series_name() {
        let expr = crate::parser::parse("last_over_time(up[60s:10s])").unwrap();
        let p = plan(&expr, params(60_000, 60_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, 4.0)])]);
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].metric_name.as_deref(), Some("up"));
                assert_eq!(v[0].v, 4.0);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    /// Unary minus end-to-end (the adjudicated #83 fold): arithmetic-
    /// class — negated values, `__name__` dropped, labels kept; stacked
    /// unaries nest.
    #[test]
    fn unary_minus_negates_a_vector_and_drops_the_metric_name() {
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, 10.0)])]);
        for (query, want) in [("-up", -10.0), ("---up", -10.0), ("--up", 10.0)] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, params(0, 0, 0)).unwrap();
            match evaluate(&p, &data).unwrap() {
                QueryValue::Vector(v) => {
                    assert_eq!(v.len(), 1, "{query}");
                    assert_eq!(v[0].v, want, "{query}");
                    assert_eq!(v[0].metric_name, None, "{query}: arithmetic drops __name__");
                    assert_eq!(v[0].labels.get("job"), Some("a"), "{query}");
                }
                other => panic!("{query}: expected Vector, got {other:?}"),
            }
        }
    }

    // --- issue #85 (M6-08c): per-series names + the PROM-39 terminal
    // metadata strip ---

    /// AC6: a regex-`__name__` selector over multi-metric fetched data
    /// emits **per-series** metric names — the pre-#85 single-name
    /// synthesis from `sel.metric_name` is gone (the spec's name is
    /// `None` here, so the fallback is structurally inert).
    #[test]
    fn regex_name_selector_emits_per_series_metric_names() {
        let expr = crate::parser::parse(r#"{__name__=~"foo|bar"}"#).unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        assert_eq!(p.selectors[0].metric_name, None);
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                named_series("foo", 1, &[("job", "a")], vec![s(0, 1.0)]),
                named_series("bar", 2, &[("job", "a")], vec![s(0, 2.0)]),
            ],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                let mut names: Vec<Option<&str>> =
                    v.iter().map(|smp| smp.metric_name.as_deref()).collect();
                names.sort();
                assert_eq!(names, vec![Some("bar"), Some("foo")]);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    /// `strip_metadata_labels` drops `__type__`/`__unit__` iff the name
    /// was dropped — the unified name+metadata rule of upstream's
    /// terminal `cleanupMetricLabels`.
    #[test]
    fn strip_metadata_labels_drops_type_and_unit_iff_name_dropped() {
        let build = || {
            Labels::new(vec![
                ("__type__".to_string(), "counter".to_string()),
                ("__unit__".to_string(), "request".to_string()),
                ("job".to_string(), "api".to_string()),
            ])
        };
        let mut kept = build();
        strip_metadata_labels(false, &mut kept);
        assert_eq!(kept, build(), "kept name ⇒ metadata labels stay");
        let mut dropped = build();
        strip_metadata_labels(true, &mut dropped);
        assert_eq!(
            dropped,
            Labels::new(vec![("job".to_string(), "api".to_string())]),
            "dropped name ⇒ __type__/__unit__ stripped"
        );
    }

    /// The strip is terminal (root-only, the delayed-oracle timing —
    /// plan v3 Δ1): mid-tree the metadata labels stay in `Labels` so set-
    /// op matching sees the full signature (`type_and_unit.test:77`'s
    /// shape), and only the final output loses them.
    #[test]
    fn metadata_labels_match_mid_tree_and_strip_only_at_the_root() {
        let expr = crate::parser::parse("(m + 1) and m").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let sv = || {
            vec![named_series(
                "m",
                1,
                &[("__type__", "counter"), ("__unit__", "request"), ("l", "x")],
                vec![s(0, 5.0)],
            )]
        };
        let mut data = SeriesData::new();
        data.insert(0, sv());
        data.insert(1, sv());
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                // Non-empty ⇒ the LHS (metadata kept mid-tree, name
                // dropped) matched the RHS on the full signature; a
                // per-node strip would have emptied this.
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].v, 6.0);
                assert_eq!(v[0].metric_name, None);
                assert_eq!(
                    v[0].labels,
                    Labels::new(vec![("l".to_string(), "x".to_string())]),
                    "root strip removes __type__/__unit__ from the name-dropped output"
                );
            }
            other => panic!("expected Vector, got {other:?}"),
        }
        // Control: the bare selector keeps its name, so the metadata
        // labels survive the root cleanup untouched.
        let expr = crate::parser::parse("m").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, sv());
        match evaluate(&p, &data).unwrap() {
            QueryValue::Vector(v) => {
                assert_eq!(v[0].metric_name.as_deref(), Some("m"));
                assert_eq!(v[0].labels.get("__type__"), Some("counter"));
                assert_eq!(v[0].labels.get("__unit__"), Some("request"));
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    /// Plan v4 Δ1 (instant): two output samples whose identities collapse
    /// to one labelset after the terminal strip are a **hard error** with
    /// the pinned upstream message (`vec.ContainsSameLabelset()` →
    /// `errorf`, engine.go:4238).
    #[test]
    fn instant_metadata_collapse_is_the_upstream_duplicate_labelset_error() {
        let expr = crate::parser::parse("m + 0").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                named_series(
                    "m",
                    1,
                    &[("__type__", "counter"), ("l", "x")],
                    vec![s(0, 1.0)],
                ),
                named_series(
                    "m",
                    2,
                    &[("__type__", "gauge"), ("l", "x")],
                    vec![s(0, 2.0)],
                ),
            ],
        );
        let err = evaluate(&p, &data).unwrap_err();
        assert_eq!(
            err.to_string(),
            "vector cannot contain metrics with the same labelset"
        );
    }

    /// Located regression pin for #69: `rate` retains each input series'
    /// name with `drop_name: true` under the delayed-name-removal model
    /// (#85/#86), so `sum by (__name__)` over two distinct-named `rate`
    /// outputs partitions into two name-dropped groups that BOTH strip to
    /// `{}` at finalization — the upstream duplicate-labelset error. This
    /// drives the full `rate → aggregate by(__name__) → finalize` chain
    /// through `evaluate()`: a bare `aggregate()`-only test would not be
    /// discriminating, since `group_key` already read `metric_name`
    /// correctly — the landed-#69 divergence lived in what `rate` used to
    /// emit (`None`), not in the grouping itself.
    #[test]
    fn sum_by_dunder_name_over_rate_of_distinct_names_is_the_duplicate_labelset_error() {
        let expr = crate::parser::parse("sum by (__name__) (rate({env=\"1\"}[10m]))").unwrap();
        let p = plan(&expr, params(300_000, 300_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                named_series(
                    "metric_total",
                    1,
                    &[("env", "1")],
                    vec![s(1, 0.0), s(300_000, 30.0)],
                ),
                named_series(
                    "another_metric_total",
                    2,
                    &[("env", "1")],
                    vec![s(1, 0.0), s(300_000, 60.0)],
                ),
            ],
        );
        let err = evaluate(&p, &data).unwrap_err();
        assert_eq!(
            err.to_string(),
            "vector cannot contain metrics with the same labelset"
        );
    }

    /// Plan v4 Δ1 (range): post-strip same-identity series **merge** when
    /// their timestamps are disjoint (upstream
    /// `mergeSeriesWithSameLabelset`) and **error** with the same message
    /// when any timestamp overlaps.
    #[test]
    fn range_metadata_collapse_merges_disjoint_timestamps_and_errors_on_overlap() {
        let expr = crate::parser::parse("m + 0").unwrap();
        let p = plan(&expr, params(0, 300_000, 300_000)).unwrap();
        // Disjoint: counter-typed series only at step 0, gauge-typed only
        // at step 300s — one merged output series.
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                named_series(
                    "m",
                    1,
                    &[("__type__", "counter"), ("l", "x")],
                    vec![s(0, 1.0)],
                ),
                named_series(
                    "m",
                    2,
                    &[("__type__", "gauge"), ("l", "x")],
                    vec![s(300_000, 2.0)],
                ),
            ],
        );
        match evaluate(&p, &data).unwrap() {
            QueryValue::Matrix(m) => {
                assert_eq!(m.len(), 1, "disjoint post-strip identities merge: {m:?}");
                assert_eq!(m[0].points, vec![(0, 1.0), (300_000, 2.0)]);
                assert_eq!(
                    m[0].labels,
                    Labels::new(vec![("l".to_string(), "x".to_string())])
                );
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
        // Overlap: both series present at step 0 (the 5m lookback keeps
        // the t=0 samples visible at step 300s too) — the merge finds a
        // duplicate timestamp and fails with the pinned message.
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![
                named_series(
                    "m",
                    1,
                    &[("__type__", "counter"), ("l", "x")],
                    vec![s(0, 1.0)],
                ),
                named_series(
                    "m",
                    2,
                    &[("__type__", "gauge"), ("l", "x")],
                    vec![s(0, 2.0)],
                ),
            ],
        );
        let err = evaluate(&p, &data).unwrap_err();
        assert_eq!(
            err.to_string(),
            "vector cannot contain metrics with the same labelset"
        );
    }

    // --- issue #86 (M6-08d): delayed name removal + string results ---

    /// AC5 / plan v2 Δ2 Class B contract: the IMMEDIATE output of a
    /// name-dropping function retains the input name with
    /// `drop_name: true` (so downstream name readers still see it), and
    /// `evaluate()`'s terminal cleanup nulls it — the load-bearing named
    /// contract every `metric_name`-reading consumer relies on.
    #[test]
    fn a_name_dropping_function_retains_the_name_mid_tree_and_the_final_output_nulls_it() {
        let expr = crate::parser::parse("abs(up)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[("job", "a")], vec![s(0, -3.0)])]);

        // Mid-tree (one raw step, before finalize): retained + marked.
        let step = eval_step(
            &p.root,
            &p.selectors,
            &EvalData::new(&p.selectors, &data),
            0,
            crate::plan::DEFAULT_LOOKBACK_MS,
            &EvalCaches::default(),
        )
        .unwrap();
        let StepValue::Vector(v) = step else {
            panic!("expected a vector step");
        };
        assert_eq!(v[0].metric_name.as_deref(), Some("up"));
        assert!(v[0].drop_name);

        // Final output: the terminal cleanup nulls the retained name.
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v[0].metric_name, None);
        assert_eq!(v[0].v, 3.0);
    }

    /// `label_replace` reads the RETAINED (to-be-dropped) name — the
    /// vendored `name_label_dropping.test:56-65` shapes: rewriting it
    /// into an ordinary label (name still dropped terminally) and
    /// re-writing `__name__` itself (drop verdict cleared).
    #[test]
    fn label_replace_reads_a_to_be_dropped_name_and_can_preserve_it() {
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[], vec![s(1, 0.0), s(60_000, 60.0)])]);

        let expr = crate::parser::parse(
            r#"label_replace(rate(up[1m]), "my_name", "rate_$1", "__name__", "(.+)")"#,
        )
        .unwrap();
        let p = plan(&expr, params(60_000, 60_000, 0)).unwrap();
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].labels.get("my_name"), Some("rate_up"));
        assert_eq!(v[0].metric_name, None, "rate's drop still lands");

        let expr = crate::parser::parse(
            r#"label_replace(rate(up[1m]), "__name__", "rate_$1", "__name__", "(.+)")"#,
        )
        .unwrap();
        let p = plan(&expr, params(60_000, 60_000, 0)).unwrap();
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(
            v[0].metric_name.as_deref(),
            Some("rate_up"),
            "an explicit __name__ write clears the drop verdict"
        );
    }

    /// The 08c residual, fixed by keying the terminal cleanup on
    /// `drop_name`: `label_replace` DELETING `__name__` is an explicit
    /// write (not a drop), so `__type__`/`__unit__` must be RETAINED —
    /// while a genuine drop (`abs`) strips them.
    #[test]
    fn deleting_the_name_via_label_replace_retains_metadata_labels() {
        let mut data = SeriesData::new();
        data.insert(
            0,
            vec![named_series(
                "m",
                1,
                &[("__type__", "counter"), ("job", "a")],
                vec![s(0, 1.0)],
            )],
        );

        let expr = crate::parser::parse(r#"label_replace(m, "__name__", "", "", "")"#).unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v[0].metric_name, None, "the name was explicitly deleted");
        assert_eq!(
            v[0].labels.get("__type__"),
            Some("counter"),
            "an explicit delete is not a drop — metadata stays"
        );

        let expr = crate::parser::parse("abs(m)").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v[0].metric_name, None);
        assert_eq!(
            v[0].labels.get("__type__"),
            None,
            "a genuine drop strips metadata"
        );
    }

    /// Plan v2 Δ1: the range accumulator LATCHES `drop_name` at an
    /// identity's first step (upstream `rangeEval`'s seriess else-branch)
    /// — `(m > 0) or (m + 1)` alternates the per-step verdict for ONE
    /// identity, and the first step decides the output's name.
    #[test]
    fn range_drop_name_latches_at_the_identitys_first_step() {
        let expr = crate::parser::parse("(m > 0) or (m + 1)").unwrap();
        let p = plan(&expr, params(0, 300_000, 300_000)).unwrap();
        // The same series feeds both operand selectors.
        let make_data = |samples: Vec<Sample>| {
            let mut data = SeriesData::new();
            for spec in &p.selectors {
                data.insert(spec.id, vec![series(1, &[], samples.clone())]);
            }
            data
        };

        // First step passes the filter (drop_name=false latched) ⇒ the
        // name survives even though the second step came from the
        // name-dropping arithmetic arm.
        let data = make_data(vec![s(0, 1.0), s(300_000, -1.0)]);
        match evaluate(&p, &data).unwrap() {
            QueryValue::Matrix(m) => {
                assert_eq!(m.len(), 1, "{m:?}");
                assert_eq!(m[0].metric_name.as_deref(), Some("m"));
                assert_eq!(m[0].points, vec![(0, 1.0), (300_000, 0.0)]);
            }
            other => panic!("expected Matrix, got {other:?}"),
        }

        // First step comes from the arithmetic arm (drop_name=true
        // latched) ⇒ the name is nulled for the whole series.
        let data = make_data(vec![s(0, -1.0), s(300_000, 1.0)]);
        match evaluate(&p, &data).unwrap() {
            QueryValue::Matrix(m) => {
                assert_eq!(m.len(), 1, "{m:?}");
                assert_eq!(m[0].metric_name, None);
                assert_eq!(m[0].points, vec![(0, 0.0), (300_000, 1.0)]);
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    /// Review round 1 gap (a): the SUBQUERY materialization latches
    /// `drop_name` at an identity's first inner-grid step, exactly like
    /// the outer range accumulator — observed through `last_over_time`
    /// (name-keeping, ORs the input verdict): the inner
    /// `(m > 0) or (m + 1)` alternates its per-step verdict across the
    /// grid, and the FIRST grid step decides whether the output keeps
    /// the name. A per-step fold (or last-step-wins) implementation
    /// flips one of the two cases.
    #[test]
    fn subquery_materialization_latches_drop_name_at_the_first_inner_grid_step() {
        let expr = crate::parser::parse("last_over_time(((m > 0) or (m + 1))[4m:2m])").unwrap();
        let p = plan(&expr, params(240_000, 240_000, 0)).unwrap();
        let make_data = |samples: Vec<Sample>| {
            let mut data = SeriesData::new();
            for spec in &p.selectors {
                data.insert(spec.id, vec![series(1, &[], samples.clone())]);
            }
            data
        };

        // Inner grid = {120s, 240s}. First grid step takes the filter
        // branch (drop_name=false latched) ⇒ last_over_time's input
        // verdict is false ⇒ the name survives, even though the LAST
        // grid step came from the name-dropping arithmetic branch.
        let data = make_data(vec![s(120_000, 1.0), s(240_000, -1.0)]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].metric_name.as_deref(), Some("m"));
        assert_eq!(
            v[0].v, 0.0,
            "last grid point is the arithmetic branch's m+1"
        );

        // First grid step takes the arithmetic branch (drop_name=true
        // latched) ⇒ the name drops, even though the LAST grid step was
        // the name-keeping filter branch.
        let data = make_data(vec![s(120_000, -1.0), s(240_000, 1.0)]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].metric_name, None);
        assert_eq!(
            v[0].v, 1.0,
            "last grid point is the filter branch's pass-through"
        );
    }

    /// Review round 1 gap (b), end-to-end companion of the binop
    /// direct-call test: a vector-scalar FILTER comparison over an
    /// already-drop-marked input (`rate`) propagates the verdict — the
    /// final output must NOT resurrect the retained name.
    #[test]
    fn filter_comparison_over_a_drop_marked_input_still_drops_the_name_terminally() {
        let expr = crate::parser::parse("rate(up[1m]) > 0").unwrap();
        let p = plan(&expr, params(60_000, 60_000, 0)).unwrap();
        let mut data = SeriesData::new();
        data.insert(0, vec![series(1, &[], vec![s(1, 0.0), s(60_000, 60.0)])]);
        let v = instant_vector(evaluate(&p, &data).unwrap());
        assert_eq!(v.len(), 1, "the comparison passes (rate > 0)");
        assert_eq!(
            v[0].metric_name, None,
            "the filter pass-through carries rate's drop verdict — a forced \
             drop_name=false would leak the retained name here"
        );
    }

    /// AC4: a top-level string literal evaluates to
    /// [`QueryValue::String`] (parens transparent); nested strings stay
    /// rejected; string-typed range queries are rejected at plan time.
    #[test]
    fn a_top_level_string_literal_evaluates_to_a_string_value() {
        for query in ["\"Foo\"", "(\"Foo\")"] {
            let expr = crate::parser::parse(query).unwrap();
            let p = plan(&expr, params(0, 0, 0)).unwrap();
            match evaluate(&p, &SeriesData::new()).unwrap() {
                QueryValue::String(got) => assert_eq!(got, "Foo", "{query}"),
                other => panic!("{query}: expected String, got {other:?}"),
            }
        }
        let expr = crate::parser::parse("\"\"").unwrap();
        let p = plan(&expr, params(0, 0, 0)).unwrap();
        assert_eq!(
            evaluate(&p, &SeriesData::new()).unwrap(),
            QueryValue::String(String::new())
        );

        let expr = crate::parser::parse("\"Foo\"").unwrap();
        let err = plan(&expr, params(0, 60_000, 60_000)).unwrap_err();
        assert!(
            err.to_string().contains("range query"),
            "string range queries are a plan-time rejection: {err}"
        );
    }

    /// Issue #93 acceptance criteria 1-3: a pre-armed [`CancelToken`]
    /// short-circuits evaluation with `Err(PromqlError::Cancelled)`, never
    /// a partial/complete `Ok`. Non-vacuous: removing the range-step
    /// checkpoint turns the first case's assertion into a failing `Ok(..)`
    /// match. The `never()` token (what [`evaluate`] uses) is unaffected —
    /// the same plans/data still complete normally, proving the default
    /// path is unchanged.
    #[test]
    fn a_pre_armed_cancel_token_short_circuits_the_range_step_checkpoint() {
        let armed = CancelToken::new(Arc::new(AtomicBool::new(true)));

        let expr = crate::parser::parse("up").unwrap();
        let range_plan = plan(&expr, params(0, 60_000, 10_000)).unwrap();
        let mut range_data = SeriesData::new();
        range_data.insert(
            0,
            vec![series(1, &[("job", "a")], vec![s(0, 1.0), s(60_000, 2.0)])],
        );
        assert!(
            matches!(
                evaluate_cancellable(&range_plan, &range_data, armed),
                Err(PromqlError::Cancelled)
            ),
            "a pre-armed token must short-circuit the range-step loop"
        );
        assert!(
            matches!(
                evaluate(&range_plan, &range_data),
                Ok(QueryValue::Matrix(_))
            ),
            "the never() token (evaluate's shape) must still complete the same plan"
        );
    }

    /// Issue #93 acceptance criterion 2 — isolates the subquery
    /// inner-grid checkpoint (`:1103`, inside `prepare_subquery`) from the
    /// stepping-phase checkpoints (`:159-160`/`:214`): calling
    /// `prepare_subqueries` directly (rather than through
    /// `evaluate_cancellable`) means no downstream checkpoint can mask an
    /// absent grid-loop check — the stepping phase this plan would
    /// otherwise reach is never invoked here. `inner_evals == 0` on the
    /// `Err` additionally proves the loop bailed on its FIRST grid point,
    /// not after materializing some/all of the grid. Non-vacuous: with the
    /// checkpoint removed, `prepare_subqueries` returns `Ok(())` and
    /// `inner_evals == 12` (the full union grid), failing both assertions.
    #[test]
    fn a_pre_armed_cancel_token_short_circuits_the_subquery_grid_checkpoint() {
        let armed = CancelToken::new(Arc::new(AtomicBool::new(true)));

        let expr = crate::parser::parse("sum_over_time(up[60s:10s])").unwrap();
        let sq_plan = plan(&expr, params(0, 0, 0)).unwrap();
        let mut sq_data = SeriesData::new();
        let samples: Vec<Sample> = (0..=12).map(|k| s(k * 10_000, 1.0)).collect();
        sq_data.insert(0, vec![series(1, &[("job", "a")], samples)]);

        let mut caches = EvalCaches {
            cancel: armed,
            ..EvalCaches::default()
        };
        let mut inner_evals = 0u64;
        let mut classifier = crate::plan::StepInvariance::new(&sq_plan.selectors);
        let err = prepare_subqueries(
            &sq_plan.root,
            &sq_plan.selectors,
            &EvalData::new(&sq_plan.selectors, &sq_data),
            StepGrid::Dense(Horizon {
                start_ms: sq_plan.params.start_ms,
                end_ms: sq_plan.params.end_ms,
                step_ms: sq_plan.params.step_ms,
            }),
            sq_plan.params.lookback_ms,
            &mut caches,
            &mut inner_evals,
            &mut classifier,
        )
        .expect_err("a pre-armed token must short-circuit the subquery inner-grid loop");
        assert!(matches!(err, PromqlError::Cancelled));
        assert_eq!(
            inner_evals, 0,
            "the grid loop must bail on its first point, not after materializing the grid"
        );

        // The never() token (evaluate's shape) still completes the same
        // plan end-to-end.
        assert!(
            evaluate(&sq_plan, &sq_data).is_ok(),
            "the never() token must still complete the subquery plan"
        );
    }
}
