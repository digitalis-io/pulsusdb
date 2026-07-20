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
pub mod hist_range_fns;
pub mod histogram_fns;
pub(crate) mod info;
pub mod labels;
pub mod quote;
pub mod staleness;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::annotations::Annotations;
use crate::error::PromqlError;
use crate::plan::{
    HistogramAccessorFn, MathFn, OverTimeFn, OverTimeParamFn, PlanExpr, QueryPlan, RangeSource,
    ScalarFn, SelectorSpec, SubqueryPlan,
};
use crate::value::{InstantSample, Labels, Point, QueryValue, RangeSeries, Sample, SeriesData};

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

/// Evaluates `plan` against `data` — pure, no I/O. Returns the value
/// alongside the accumulated [`Annotations`] (M7-A5b-i): a generic sink —
/// empty for every float-only query (byte-identical to the pre-A5b-i
/// behavior) — that native-histogram arms populate (`histogram_quantile`'s
/// out-of-range φ, NaN-observation info, …).
pub fn evaluate(
    plan: &QueryPlan,
    data: &SeriesData,
) -> Result<(QueryValue, Annotations), PromqlError> {
    evaluate_counted(plan, data).map(|(value, _counts, annotations)| (value, annotations))
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

/// [`evaluate`] plus [`EvalCounts`] and the drained [`Annotations`] sink.
fn evaluate_counted(
    plan: &QueryPlan,
    data: &SeriesData,
) -> Result<(QueryValue, EvalCounts, Annotations), PromqlError> {
    let p = &plan.params;

    // Issue #83 (round-2 amendment): materialize every subquery ONCE over
    // its epoch-anchored union grid — inside-out for nested subqueries —
    // before any stepping; each step below only slices `(mint, maxt]`
    // windows from the shared results. Issue #82 rides the same pass:
    // every `info()` node's arg0 horizon is walked once here to build
    // its horizon-wide identifying-label narrowing (`prepare_info`).
    let mut caches = EvalCaches::default();
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
        Horizon {
            start_ms: p.start_ms,
            end_ms: p.end_ms,
            step_ms: p.step_ms,
        },
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
}

/// Prepared `info()` nodes keyed by the [`PlanExpr::Info`] node's address
/// (the [`SubqueryCache`] node-identity precedent).
type InfoCache = HashMap<*const PlanExpr, PreparedInfo>;

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
/// inner evaluations find their nested results already cached. `span` is
/// the closed `[start, end]` range of evaluation times this node will be
/// evaluated at (the query's own span at the root; a subquery's grid
/// extent for its inner expression).
// Issue #95 threads the step-invariance classifier through the subquery
// prep recursion (plan-mandated signature), pushing this to 8 params.
#[allow(clippy::too_many_arguments)]
fn prepare_subqueries(
    expr: &PlanExpr,
    selectors: &[SelectorSpec],
    data: &SeriesData,
    horizon: Horizon,
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
            horizon.span(),
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
                    horizon,
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
                horizon.span(),
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
            horizon,
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
                horizon,
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
                horizon,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )?;
            prepare_subqueries(
                expr,
                selectors,
                data,
                horizon,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )
        }
        PlanExpr::HistogramAccessor { arg, .. } => prepare_subqueries(
            arg,
            selectors,
            data,
            horizon,
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
                horizon,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )?;
            prepare_subqueries(
                upper,
                selectors,
                data,
                horizon,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )?;
            prepare_subqueries(
                expr,
                selectors,
                data,
                horizon,
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
                    horizon,
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
                horizon,
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
            horizon,
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
                horizon,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )?;
            prepare_subqueries(
                rhs,
                selectors,
                data,
                horizon,
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
                horizon,
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
                    horizon,
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
                    horizon,
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
                horizon,
                lookback_ms,
                caches,
                inner_evals,
                classifier,
            )?;
            prepare_info(expr, selectors, data, horizon, lookback_ms, caches)
        }
    }
}

/// Walks one `info()` node's arg0 over its enclosing horizon exactly
/// once (issue #82) — the evaluation-time counterpart of upstream
/// `evalInfo`'s `ev.eval(args[0])` matrix + `fetchInfoSeries`'s
/// `idLblValues` construction (`info.go:150-170`), which are both
/// horizon-wide, never per-step (the #82 round-2 adjudication). Caches
/// each step's base vector (reused verbatim by the stepping phase — the
/// base is never evaluated twice) and the allowed identifying-label
/// values of every NON-ignored base series (ignored ⟺ the retained name
/// matches all effective name matchers; empty values never register).
fn prepare_info(
    node: &PlanExpr,
    selectors: &[SelectorSpec],
    data: &SeriesData,
    horizon: Horizon,
    lookback_ms: i64,
    caches: &mut EvalCaches,
) -> Result<(), PromqlError> {
    let PlanExpr::Info {
        base,
        name_matchers,
        ..
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

    let mut t = horizon.start_ms;
    loop {
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
        // A single step for an instant query (step 0, span.0 == span.1);
        // the enclosing horizon's own grid otherwise — exactly the steps
        // the stepping phase will evaluate this node at.
        if horizon.step_ms <= 0 || t >= horizon.end_ms {
            break;
        }
        t += horizon.step_ms;
    }

    caches.infos.insert(
        node as *const _,
        PreparedInfo {
            base_steps,
            id_lbl_values,
        },
    );
    Ok(())
}

// Issue #95: threads the classifier to `prepare_subquery` (8 params).
#[allow(clippy::too_many_arguments)]
fn prepare_source(
    source: &RangeSource,
    selectors: &[SelectorSpec],
    data: &SeriesData,
    span: (i64, i64),
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
            span,
            lookback_ms,
            caches,
            inner_evals,
            classifier,
        ),
    }
}

/// Materializes one subquery over the epoch-anchored ASCENDING union
/// grid: the union of every outer step's `(mint, maxt]` window over
/// `span` (a single window when the subquery carries its own `@` — the
/// anchor is fixed), realized as `{ k·step : k·step > mint_min, k·step ≤
/// maxt_max }`, each point evaluated exactly once. Recursion depth is
/// bounded by the planner's `MAX_SUBQUERY_DEPTH` guard.
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
    data: &SeriesData,
    span: (i64, i64),
    lookback_ms: i64,
    caches: &mut EvalCaches,
    inner_evals: &mut u64,
    classifier: &mut crate::plan::StepInvariance<'_>,
) -> Result<(), PromqlError> {
    let (anchor_min, anchor_max) = match sq.at_ms {
        Some(at) => (at, at),
        None => span,
    };
    let maxt_max = anchor_max - sq.offset_ms;
    let mint_min = anchor_min - sq.offset_ms - sq.range_ms;
    let grid_start = subquery_grid_start(mint_min, sq.step_ms);

    // Series identity → (first-step-latched drop_name, grid samples),
    // BTreeMap for deterministic order. The latch mirrors the outer range
    // accumulator's (issue #86 plan v2 Δ1 — see `MaterializedSeries`).
    let mut acc: BTreeMap<SeriesIdentity, (bool, Vec<Sample>)> = BTreeMap::new();
    if grid_start <= maxt_max {
        // Children first (inside-out): the inner expression's own nested
        // subqueries must be materialized before it can be evaluated. Its
        // evaluation span is exactly this grid's extent.
        let grid_last = maxt_max - (maxt_max - grid_start).rem_euclid(sq.step_ms);
        prepare_subqueries(
            &sq.inner,
            selectors,
            data,
            // The inner horizon's own grid: nested subqueries AND any
            // nested info() nodes are evaluated on it.
            Horizon {
                start_ms: grid_start,
                end_ms: grid_last,
                step_ms: sq.step_ms,
            },
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

        let mut it = grid_start;
        while it <= maxt_max {
            *inner_evals += 1;
            match eval_step(&sq.inner, selectors, data, it, lookback_ms, caches)? {
                StepValue::Vector(v) => {
                    for s in v {
                        let h = s.h;
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
            it += sq.step_ms;
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
    data: &SeriesData,
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
}

/// A range source resolved at one evaluation step: the `(lower_excl,
/// upper_incl]` window (`upper_incl` = the effective evaluation time) and
/// every series' windowed, non-stale samples — the shared input for all
/// four range-function arms (issue #83's `eval_range_source` helper).
struct WindowedSource {
    range_ms: i64,
    lower_excl: i64,
    upper_incl: i64,
    series: Vec<WindowedSeries>,
}

fn windowed_range_source(
    source: &RangeSource,
    selectors: &[SelectorSpec],
    data: &SeriesData,
    subqueries: &SubqueryCache,
    t_ms: i64,
) -> Result<WindowedSource, PromqlError> {
    let source_view = match source {
        RangeSource::Selector(id) => {
            let sel = &selectors[*id];
            let eff_t = sel.at_ms.unwrap_or(t_ms) - sel.offset_ms;
            let range_ms = sel
                .range_ms
                .expect("plan() only ever builds a range source over a matrix selector");
            let lower_excl = eff_t - range_ms;
            let series = data
                .get(*id)
                .iter()
                .map(|s| WindowedSeries {
                    labels: s.labels.clone(),
                    // Per-series name (issue #85), with the same
                    // concrete-name-only fallback the `Selector` arm
                    // documents.
                    metric_name: s.metric_name.clone().or_else(|| sel.metric_name.clone()),
                    drop_name: false,
                    samples: windowed_non_stale(&s.samples, lower_excl, eff_t),
                })
                .collect();
            WindowedSource {
                range_ms,
                lower_excl,
                upper_incl: eff_t,
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
                    }
                })
                .collect();
            WindowedSource {
                range_ms: sq.range_ms,
                lower_excl,
                upper_incl: eff_t,
                series,
            }
        }
    };
    // M7-A5b-ii: the blanket "reject any histogram in the window" guard
    // that used to live here (M7-A5a) is gone — `rate`/`increase`/`delta`/
    // `irate`/`idelta`/`resets`/`changes`/the `OverTimeFn` disposition map
    // now have real histogram semantics (`hist_range_fns`). What remains
    // out of A5b-ii scope (`sum_over_time`/`avg_over_time`'s KahanAdd
    // path, `predict_linear`, `double_exponential_smoothing`) keeps
    // guarding itself explicitly at its own call site below, via
    // [`reject_histogram_samples`] — this function no longer is a single
    // shared chokepoint for that.
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

/// M7-A5a native-histogram guard error (surfaces as 422 `execution`).
///
/// A bare selector is the ONLY evaluation site that carries the
/// native-histogram channel (`Sample::h`) through to output in A5a; every
/// *derived* operation is float-only until the A5b function set lands.
/// Without this guard such an operation would fold the input's `v` (which
/// is `0.0` for a histogram sample by construction — `Sample::hist`) and
/// emit `h: None`, fabricating a `0.0` float from a histogram input — the
/// AC8 "no 0.0-for-histogram" defect. Instead we reject the histogram as
/// an unsupported construct (naming A5b), which the read/API layers map to
/// 422 `execution`, exactly like `pulsus-read`'s output-level
/// `HistogramResultUnsupported` rejection of a bare histogram selector.
fn histogram_unsupported(op: &str) -> PromqlError {
    PromqlError::Unsupported {
        construct: format!("native histogram input to {op} — supported in M7-A5b"),
    }
}

/// Rejects any histogram-valued [`InstantSample`] reaching a derived
/// operation that folds `v` (see [`histogram_unsupported`]). A no-op for a
/// float-only vector (`h` is `None` everywhere), so float evaluation stays
/// byte-identical.
fn reject_histogram_vector(v: &[InstantSample], op: &str) -> Result<(), PromqlError> {
    if v.iter().any(|s| s.h.is_some()) {
        return Err(histogram_unsupported(op));
    }
    Ok(())
}

/// Rejects any histogram-valued [`Sample`] reaching a derived operation
/// that reads raw fetched samples — range functions (via
/// [`windowed_range_source`]) and the `timestamp()`/`info()` selector
/// reads. A no-op for a float-only window.
fn reject_histogram_samples(samples: &[Sample], op: &str) -> Result<(), PromqlError> {
    if samples.iter().any(|s| s.h.is_some()) {
        return Err(histogram_unsupported(op));
    }
    Ok(())
}

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

fn eval_step(
    expr: &PlanExpr,
    selectors: &[SelectorSpec],
    data: &SeriesData,
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
            let src = windowed_range_source(source, selectors, data, &caches.subqueries, t_ms)?;
            let mut out = Vec::new();
            for series in src.series {
                let metric_name = series.metric_name.as_deref().unwrap_or("");
                let result = {
                    let mut annos = caches.annotations.borrow_mut();
                    hist_range_fns::eval_range_fn_hist(
                        *func,
                        &series.samples,
                        src.range_ms,
                        src.lower_excl,
                        src.upper_incl,
                        metric_name,
                        &mut annos,
                    )
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
            let src = windowed_range_source(source, selectors, data, &caches.subqueries, t_ms)?;
            let keeps_name = matches!(func, OverTimeFn::Last | OverTimeFn::First);
            let mut out = Vec::new();
            for series in src.series {
                // M7-A5b-ii: `sum_over_time`/`avg_over_time` histogram
                // (KahanAdd) accumulation is deferred out of this item's
                // scope — kept explicitly 422-guarded, exactly the A5a
                // behavior, rather than silently dispatching through the
                // hist-aware map (which asserts unreachable for these two).
                if matches!(func, OverTimeFn::Sum | OverTimeFn::Avg) {
                    reject_histogram_samples(&series.samples, "sum_over_time/avg_over_time")?;
                    if let Some(v) = functions::eval_over_time(*func, &series.samples) {
                        out.push(InstantSample {
                            labels: series.labels,
                            metric_name: series.metric_name,
                            drop_name: !keeps_name || series.drop_name,
                            t_ms,
                            v,
                            h: None,
                        });
                    }
                    continue;
                }
                let metric_name = series.metric_name.as_deref().unwrap_or("");
                let result = {
                    let mut annos = caches.annotations.borrow_mut();
                    hist_range_fns::eval_over_time_hist(
                        *func,
                        &series.samples,
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
            let src = windowed_range_source(source, selectors, data, &caches.subqueries, t_ms)?;
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
                // one); `predict_linear`/`double_exponential_smoothing`
                // stay out of scope, guarded exactly as A5a left them.
                let v = if *func == OverTimeParamFn::Quantile {
                    let metric_name = series.metric_name.as_deref().unwrap_or("");
                    let mut annos = caches.annotations.borrow_mut();
                    hist_range_fns::eval_quantile_over_time_hist(
                        scalars[0],
                        &series.samples,
                        metric_name,
                        &mut annos,
                    )
                } else {
                    reject_histogram_samples(
                        &series.samples,
                        "predict_linear/double_exponential_smoothing",
                    )?;
                    // `predict_linear`'s regression intercept stays the
                    // outer evaluation STEP time `t_ms` — never the
                    // `@`/offset-shifted window edge (the #67
                    // adjudication; the offset golden lives in
                    // proof/m6_08a_at_subquery.test).
                    functions::eval_over_time_param(*func, &series.samples, &scalars, t_ms)?
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
            let src = windowed_range_source(source, selectors, data, &caches.subqueries, t_ms)?;
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
            reject_histogram_vector(&v, "absent()")?;
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
            reject_histogram_vector(&v, "sort()/sort_desc()")?;
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
            reject_histogram_vector(&v, "sort_by_label()/sort_by_label_desc()")?;
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
            reject_histogram_vector(&v, "label_replace()")?;
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
            reject_histogram_vector(&v, "label_join()")?;
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
            expr,
            param,
            grouping,
        } => {
            let StepValue::Vector(v) = eval_step(expr, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: "aggregation over a scalar expression".to_string(),
                });
            };
            reject_histogram_vector(&v, "an aggregation")?;
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
            let out = aggregation::aggregate(*op, &v, grouping.as_ref(), param_v)?;
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
            reject_histogram_vector(&v, "count_values()")?;
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
            let StepValue::Vector(v) = eval_step(arg, selectors, data, t_ms, lookback_ms, caches)?
            else {
                return Err(PromqlError::Unsupported {
                    construct: format!("{func:?} over a scalar expression"),
                });
            };
            reject_histogram_vector(&v, "an elementwise math function")?;
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
                reject_histogram_vector(&v, "a date/time function")?;
                let out = v
                    .into_iter()
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
                        if sample.h.is_some() {
                            return Err(histogram_unsupported("timestamp()"));
                        }
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
                reject_histogram_vector(&v, "timestamp()")?;
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
            reject_histogram_vector(&v, "scalar()")?;
            Ok(StepValue::Scalar(match v.as_slice() {
                [only] => only.v,
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
                    Ok(StepValue::Scalar(binop::scalar_scalar(*op, l, r)))
                }
                (StepValue::Vector(v), StepValue::Scalar(s)) => {
                    debug_assert!(
                        *group == crate::plan::Group::OneToOne && *fill == Default::default(),
                        "plan_binary discards group/fill for scalar operands"
                    );
                    reject_histogram_vector(&v, "a binary operator")?;
                    Ok(StepValue::Vector(binop::vector_scalar(
                        *op,
                        *bool_modifier,
                        &v,
                        s,
                        false,
                    )))
                }
                (StepValue::Scalar(s), StepValue::Vector(v)) => {
                    debug_assert!(
                        *group == crate::plan::Group::OneToOne && *fill == Default::default(),
                        "plan_binary discards group/fill for scalar operands"
                    );
                    reject_histogram_vector(&v, "a binary operator")?;
                    Ok(StepValue::Vector(binop::vector_scalar(
                        *op,
                        *bool_modifier,
                        &v,
                        s,
                        true,
                    )))
                }
                (StepValue::Vector(l), StepValue::Vector(r)) => {
                    reject_histogram_vector(&l, "a binary operator")?;
                    reject_histogram_vector(&r, "a binary operator")?;
                    Ok(StepValue::Vector(binop::vector_vector(
                        *op,
                        *bool_modifier,
                        matching,
                        group,
                        fill,
                        &l,
                        &r,
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
                (StepValue::Vector(l), StepValue::Vector(r)) => {
                    reject_histogram_vector(&l, "a set operator")?;
                    reject_histogram_vector(&r, "a set operator")?;
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
        // from the synthetic selector's fetch at ITS own effective time
        // (offset/@ copied from arg0's first selector at plan time),
        // carrying each series' real metric name and the resolved
        // sample's ORIGINAL timestamp (the newest-wins dedup key). All
        // narrowing/dedup/join semantics live in `info::combine` —
        // name-keeping with the delayed verdict CLEARED: the pin builds
        // fresh DropName-less output samples, so a name-dropping arg0
        // re-emerges with its retained name kept (see `combine`'s doc).
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
            reject_histogram_vector(&base_v, "info()")?;

            let sel = &selectors[*info_selector];
            let eff_t = sel.at_ms.unwrap_or(t_ms) - sel.offset_ms;
            let mut info_v = Vec::new();
            for series in data.get(*info_selector) {
                if let Some(sample) = staleness::instant_value(&series.samples, eff_t, lookback_ms)
                {
                    if sample.h.is_some() {
                        return Err(histogram_unsupported("info()"));
                    }
                    // Per-series name with the concrete-name fallback
                    // (the `Selector` arm's contract); a genuinely
                    // nameless series can never be an info source.
                    let Some(metric_name) = series
                        .metric_name
                        .clone()
                        .or_else(|| sel.metric_name.clone())
                    else {
                        continue;
                    };
                    info_v.push(info::InfoSeriesAtStep {
                        metric_name,
                        labels: series.labels.clone(),
                        orig_t_ms: sample.t_ms,
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
    use crate::value::FetchedSeries;

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

    /// Plans `query`, evaluates it over [`histogram_selector_data`], and
    /// asserts a native-histogram-unsupported execution error naming A5b —
    /// never an `Ok` result (which would carry a fabricated `0.0`).
    fn assert_histogram_unsupported(query: &str, p: PlanParams) {
        let expr = crate::parser::parse(query).unwrap();
        let plan = plan(&expr, p).unwrap();
        match evaluate(&plan, &histogram_selector_data()) {
            Err(PromqlError::Unsupported { construct }) => assert!(
                construct.contains("native histogram") && construct.contains("A5b"),
                "the error names the native-histogram/A5b cause: {construct:?}"
            ),
            other => panic!(
                "expected a histogram-unsupported error for {query:?}, got {other:?} \
                 — an Ok result would be the AC8 fabricated-0.0 defect"
            ),
        }
    }

    #[test]
    fn aggregation_over_a_histogram_series_errors_never_fabricates_a_float() {
        assert_histogram_unsupported("sum(up)", params(1_000, 1_000, 0));
    }

    #[test]
    fn binary_op_over_a_histogram_series_errors() {
        assert_histogram_unsupported("up + 1", params(1_000, 1_000, 0));
    }

    /// M7-A5b-ii superseded the A5a blanket reject for `rate`/`increase`/
    /// `delta`/`irate`: a native-histogram range function now computes a
    /// real result — never an error, never a fabricated `0.0` float.
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

    /// `sum_over_time`/`avg_over_time`'s histogram (KahanAdd) path is
    /// deferred out of A5b-ii scope — still 422-guarded exactly like A5a.
    #[test]
    fn over_time_function_over_a_histogram_series_errors() {
        assert_histogram_unsupported("avg_over_time(up[5m])", params(1_000, 1_000, 0));
    }

    #[test]
    fn elementwise_math_over_a_histogram_series_errors() {
        assert_histogram_unsupported("abs(up)", params(1_000, 1_000, 0));
    }

    #[test]
    fn label_replace_over_a_histogram_series_errors() {
        assert_histogram_unsupported(
            "label_replace(up, \"x\", \"y\", \"job\", \".*\")",
            params(1_000, 1_000, 0),
        );
    }

    #[test]
    fn timestamp_over_a_histogram_series_errors() {
        assert_histogram_unsupported("timestamp(up)", params(1_000, 1_000, 0));
    }

    #[test]
    fn a_range_query_aggregation_over_a_histogram_series_errors_at_every_step() {
        // The per-step matrix accumulation path (step_ms > 0) rejects too,
        // rather than folding a 0.0 histogram value into the matrix.
        assert_histogram_unsupported("sum(up)", params(1_000, 2_000, 1_000));
    }

    #[test]
    fn set_op_over_a_histogram_series_errors() {
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
        assert!(
            matches!(evaluate(&p, &data), Err(PromqlError::Unsupported { .. })),
            "a set operator over a histogram operand errors, never fabricates a float"
        );
    }

    #[test]
    fn scalar_vector_binop_over_a_histogram_series_errors() {
        // The scalar-op-vector arm (`1 + up`) rejects the histogram vector
        // just like the vector-op-scalar arm (`up + 1`) already covered.
        assert_histogram_unsupported("1 + up", params(1_000, 1_000, 0));
    }

    #[test]
    fn vector_vector_binop_over_histogram_operands_errors() {
        // Both operands of `up + up` are histogram-valued vectors; the
        // vector-vector arm must reject, never fold either operand's `v`.
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
            Err(PromqlError::Unsupported { construct }) => assert!(
                construct.contains("native histogram") && construct.contains("A5b"),
                "the error names the native-histogram/A5b cause: {construct:?}"
            ),
            other => panic!(
                "a vector-vector binary operator over histogram operands must error, \
                 never fabricate a float — got {other:?}"
            ),
        }
    }

    #[test]
    fn label_join_over_a_histogram_series_errors() {
        assert_histogram_unsupported(
            "label_join(up, \"x\", \"-\", \"job\")",
            params(1_000, 1_000, 0),
        );
    }

    #[test]
    fn sum_over_time_subquery_over_a_histogram_series_errors() {
        // The subquery materializes an instant grid that carries the
        // histogram channel (`h`) into its matrix; `sum_over_time` then
        // rejects that matrix rather than folding a 0.0. The `:1s` inner
        // step lands the fixture's t=1000 (=1s) histogram sample on a grid
        // point, so the guarded path is genuinely exercised.
        assert_histogram_unsupported("sum_over_time(up[5m:1s])", params(1_000, 1_000, 0));
    }

    #[test]
    fn at_pinned_aggregation_over_a_histogram_series_errors() {
        // `@ 1` pins the selector at 1 s = 1000 ms — the fixture's second
        // histogram sample — so the step-invariant `@`-pinned subtree still
        // feeds a histogram vector into `sum`, which must reject.
        assert_histogram_unsupported("sum(up @ 1)", params(1_000, 1_000, 0));
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
            &data,
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
            &data,
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
}
