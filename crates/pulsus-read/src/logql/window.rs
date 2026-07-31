//! Window and grid arithmetic for the client-side aggregation path — the
//! query's time domain, with no state and no ClickHouse contact.
//!
//! [`ClientWindow`] is the validated instant/range window a metric query
//! evaluates over; [`GridWindow`] is its start-anchored grid. The
//! `instant_window` submodule holds the unforgeable [`InstantWindow`]
//! witness that narrows a window to its instant case, and the free
//! functions here are the bucket/grid arithmetic every aggregation path
//! shares ([`bucket_grid`], [`clamp_bucket`], [`ensure_grid_resolution`],
//! [`fence_intervals`], [`grid_point_count`]).

use super::error::{ReadError, TooBroadReason};
use super::params::ValidatedDuration;

use super::exec::{MatrixSeries, QueryResult, VectorSample};

/// The evaluation window for one client-aggregated metric query.
/// `step_ns: None` = instant (one window `(at-range, at]` reduced to a
/// single sample). `Some(step)` = a range query evaluated with Loki's
/// **sliding** windows (issue #227): the `[range]` window `(t-range, t]` is
/// re-evaluated at every start-anchored grid point `t ∈ {start+k·step ≤
/// end}`. `range_ns` is that `[range]` selector width — decoupled from
/// `step_ns` so `rate({}[1m])` and `rate({}[10m])` differ (the divisor and
/// the window both track `range`, never `step`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ClientWindow {
    /// A single window, already materialised into `[start_ns, end_ns]` by the
    /// planner (`start = at - range`). An instant query has **no step grid
    /// and no residual `[range]`** — both are structurally absent here, so
    /// neither can be misread downstream.
    Instant { start_ns: i64, end_ns: i64 },
    /// Sliding range evaluation. BOTH durations are
    /// [`ValidatedDuration`]s — boundary-validated and therefore non-zero,
    /// **by type** (issue #227 review round 4, finding 2). There is no
    /// "absent"/zero representation for a range window, so
    /// [`RangeSlideState`] can only ever receive a real, validated range:
    /// the previous `ValidatedDuration::NONE` sentinel (a public unvalidated
    /// mint that could be placed in a range slot) is gone entirely.
    Range {
        /// The start-anchored emit grid's first point.
        grid_start_ns: i64,
        end_ns: i64,
        step_ns: ValidatedDuration,
        /// The `[range]` selector width — the sliding span `(t-range, t]`.
        range_ns: ValidatedDuration,
    },
}

impl ClientWindow {
    /// The evaluation window's lower bound (the emit grid's first point for
    /// a range query).
    pub fn start_ns(&self) -> i64 {
        match *self {
            ClientWindow::Instant { start_ns, .. } => start_ns,
            ClientWindow::Range { grid_start_ns, .. } => grid_start_ns,
        }
    }

    pub fn end_ns(&self) -> i64 {
        match *self {
            ClientWindow::Instant { end_ns, .. } | ClientWindow::Range { end_ns, .. } => end_ns,
        }
    }

    /// `Some` only for a range query — an instant window has no step grid.
    pub fn step_ns(&self) -> Option<ValidatedDuration> {
        match *self {
            ClientWindow::Instant { .. } => None,
            ClientWindow::Range { step_ns, .. } => Some(step_ns),
        }
    }
}

/// The evaluation grid for a leafless `vector(<scalar>)` node (issue #221):
/// a constant series materialised at every step point. Deliberately a
/// SEPARATE type from [`ClientWindow`] (issue #227 review round 4): a vector
/// literal has no `[range]` selector at all, so it must not be able to
/// occupy — or be built from — a range window's range slot.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GridWindow {
    pub start_ns: i64,
    pub end_ns: i64,
    /// `Some` = the range grid (validated step); `None` = a single instant
    /// sample.
    pub step_ns: Option<ValidatedDuration>,
}

/// Converts an i128 bucket start back to the i64 point-timestamp domain.
/// Only the sliver within one step below `i64::MIN` (or, symmetrically,
/// above `i64::MAX`) can fall outside — centuries beyond any real
/// nanosecond timestamp — and it clamps deterministically; both
/// [`bucket_of`] and [`bucket_grid`] clamp IDENTICALLY, so data-driven
/// buckets and the `absent_over_time` grid stay membership-consistent.
pub(in crate::logql) fn clamp_bucket(bucket: i128) -> i64 {
    i64::try_from(bucket).unwrap_or(if bucket < 0 { i64::MIN } else { i64::MAX })
}

/// The start-anchored step grid `{grid_start + k·step : k≥0, ≤ end}` (issue
/// #227: Loki seeds `current` at `start-step`, first point `start`, iterates
/// `≤ end`). Used by `vector(<scalar>)`'s constant matrix so it aligns with
/// the sliding data leaves' start-anchored grid. i128 intermediates keep
/// `grid_start + k·step` overflow-free; every point lies in
/// `[grid_start, end]` (no clamp needed — `k ≥ 0`). The grid passed
/// [`ensure_grid_resolution`] before this runs (≤ 22_002 points; 11_001
/// when the span fits an int64 duration — round 8's saturating fence).
fn bucket_grid(grid_start: i64, end_ns: i64, step_ns: u64) -> Vec<i64> {
    if step_ns == 0 || step_ns > i64::MAX as u64 || end_ns < grid_start {
        return Vec::new();
    }
    let step = step_ns as i128;
    let gs = grid_start as i128;
    let kmax = (end_ns as i128 - gs) / step;
    // Saturating narrowing, like the sliding evaluator's `grid_point` (issue
    // #227 arithmetic sweep): every point is in `[grid_start, end]` by
    // construction, so the clamp is defense-in-depth, never a wrap.
    (0..=kmax).map(|k| clamp_bucket(gs + k * step)).collect()
}

/// The evaluation-resolution cap for client-aggregated range queries, in
/// step INTERVALS — Loki-exact (issue #227 review round 7, finding 1):
/// the reference rejects iff `(end - start) / step > 11000` (truncating
/// integer division, `loghttp/query.go` `errStepTooSmall`), so a request
/// with exactly 11 000 intervals is SERVED, and its inclusive
/// start-anchored grid holds `intervals + 1 = 11_001` points. Every grid
/// guard funnels through [`ensure_grid_resolution`], which encodes that
/// rule — rejected as `QueryTooBroad(MetricBuckets)` BEFORE any grid or
/// accumulator materialization (review round 1, finding 2 — an
/// `absent_over_time` over a huge range with a tiny step must never
/// allocate an attacker-sized grid). A documented constant, not a config
/// field (the `DEFAULT_MAX_STREAMS` precedent).
pub const MAX_CLIENT_AGG_BUCKETS: u64 = 11_000;

/// **The one grid-resolution guard** (issue #227 review round 7,
/// finding 1): rejects iff [`fence_intervals`] — the reference's own
/// SATURATING `floor((end - grid_start) / step)` (round 8) — exceeds
/// [`MAX_CLIENT_AGG_BUCKETS`], and returns the EXACT grid POINT count
/// ([`grid_point_count`]) on success. The HTTP boundary
/// (`ensure_range_resolution` in `pulsus-server`) applies the identical
/// saturating formula over the identical `(start, end, step)` (the emit
/// grid is start-anchored, so `grid_start == start`), so the engine can
/// never 422 a request the request guard admitted — across the WHOLE i64
/// timestamp domain, not just spans that fit an int64 duration. The
/// admitted point count is still hard-bounded: an unsaturated span holds
/// ≤ 11_001 points, and a saturated one forces `step > i64::MAX / 11_001`,
/// so the true `u64` span (< 2^64) holds ≤ 22_002 points — O(fence),
/// never attacker-sized. A degenerate step (zero, or wider than i64 —
/// neither passes `validate_duration_ns`) saturates [`fence_intervals`]
/// to `u64::MAX` and still rejects.
pub(in crate::logql) fn ensure_grid_resolution(
    grid_start_ns: i64,
    end_ns: i64,
    step_ns: u64,
) -> Result<u64, ReadError> {
    let intervals = fence_intervals(grid_start_ns, end_ns, step_ns);
    if intervals > MAX_CLIENT_AGG_BUCKETS {
        return Err(ReadError::QueryTooBroad(TooBroadReason::MetricBuckets {
            buckets: intervals,
            cap: MAX_CLIENT_AGG_BUCKETS,
        }));
    }
    Ok(grid_point_count(grid_start_ns, end_ns, step_ns))
}

/// The fence's interval count, in the reference's EXACT arithmetic (issue
/// #227 review round 8): `End.Sub(Start)` is Go's `time.Time.Sub`, which
/// SATURATES an out-of-range difference at the int64-nanosecond `Duration`
/// bound (`maxDuration = 1<<63-1`) rather than widening, and the division
/// is truncating integer `Duration / Duration`. So a full-domain span at a
/// huge step counts `i64::MAX / step` intervals — under the fence — where
/// the exact i128 span would wrongly reject a request the reference
/// serves. `end < grid_start` is zero intervals (never trips, matching
/// the guard's admit-empty behaviour); a degenerate step saturates to
/// `u64::MAX` so the guard rejects by name instead of dividing by zero.
fn fence_intervals(grid_start_ns: i64, end_ns: i64, step_ns: u64) -> u64 {
    if end_ns < grid_start_ns {
        return 0;
    }
    if step_ns == 0 || step_ns > i64::MAX as u64 {
        return u64::MAX;
    }
    // Loki-exact: the span clamped to i64 (`time.Time.Sub` saturation),
    // non-negative here (`end ≥ grid_start` above).
    end_ns.saturating_sub(grid_start_ns) as u64 / step_ns
}

/// The instant-window witness, in a module of its own so that its field
/// is private to it (issue #236 Part D).
///
/// The nesting is the whole point and is not decoration. A bare
/// `struct InstantWindow;` is a UNIT struct: any line in `exec.rs` can
/// write `InstantWindow` and mint one — and every `ClientAggState::new`
/// call site is in `exec.rs`, so such a guard would be merely
/// *unreachable today*, not *unrepresentable*. With the field private to
/// this module, [`InstantWindow::mint`] is the only way to obtain one
/// anywhere in the crate, INCLUDING this file's own `mod tests`.
mod instant_window {
    use super::ClientWindow;

    /// A WITNESS that a [`ClientWindow`] was narrowed to its instant
    /// case.
    ///
    /// `ClientAggState`'s per-group accumulator is a SINGLE `BucketAcc`,
    /// which is sound only because an instant window has exactly one
    /// bucket. Requiring this witness at the constructor keeps that true
    /// without a runtime check nobody re-reads — and it was already true
    /// at every call site, so nothing is newly forbidden.
    ///
    /// It carries NO bounds, a deliberate difference from plan v14's
    /// "explicit instant bounds": nothing in an instant state reads
    /// them. The two former readers — the stepped-grid guard in the
    /// constructor and the `bucket_grid` walk in `finish` — are both
    /// deleted by Part D, and a field that exists only to look used is
    /// worse than none. The witness is also the stronger of the two
    /// shapes: a pair of `i64` bounds can be hand-derived from a stepped
    /// window, an unforgeable witness cannot.
    #[derive(Debug, Clone, Copy)]
    pub(in crate::logql) struct InstantWindow(());

    impl InstantWindow {
        /// The ONE mint. Refuses a stepped window, so a caller has to say
        /// what it does with one instead of assuming it cannot get one.
        pub(in crate::logql) fn mint(window: ClientWindow) -> Option<Self> {
            match window {
                ClientWindow::Instant { .. } => Some(InstantWindow(())),
                ClientWindow::Range { .. } => None,
            }
        }
    }
}

pub(in crate::logql) use instant_window::InstantWindow;

impl ClientWindow {
    /// Narrows to the INSTANT case, or `None` for a stepped window.
    pub(in crate::logql) fn as_instant(self) -> Option<InstantWindow> {
        InstantWindow::mint(self)
    }
}

/// Materializes `vector(<scalar>)` (issue #221): promotes a constant to a
/// vector/matrix result with an empty label set. Instant queries yield a
/// single `{} => value` sample; range queries yield one constant `{}`
/// series populated at every evaluation-grid step.
///
/// A leafless `vector(n)` bypasses [`RangeSlideState::new`]'s resolution
/// guard (no leaf ever runs), so this checks the same Loki-exact rule via
/// [`ensure_grid_resolution`] BEFORE materializing any grid — an over-cap
/// `vector(n)` returns the identical `QueryTooBroad(MetricBuckets)`
/// 422 as every other over-cap LogQL range query, with zero allocation and
/// no DB round-trip. The grid itself REUSES [`bucket_grid`] (the same
/// i128-safe, `clamp_bucket`-narrowed, start-anchored grid a leaf
/// range query and `absent_over_time` use), so `points.len()` equals the
/// guard's admitted point count (≤ 11 001)
/// and a `data + vector(n)` binop aligns on the data's populated steps.
pub fn materialize_vector_lit(value: f64, window: &GridWindow) -> Result<QueryResult, ReadError> {
    match window.step_ns.map(|d| d.as_u64()) {
        None => Ok(QueryResult::Vector(vec![VectorSample {
            labels: vec![],
            value,
        }])),
        Some(step) => {
            ensure_grid_resolution(window.start_ns, window.end_ns, step)?;
            let points = bucket_grid(window.start_ns, window.end_ns, step)
                .into_iter()
                .map(|ts| (ts, value))
                .collect();
            Ok(QueryResult::Matrix(vec![MatrixSeries {
                labels: vec![],
                points,
            }]))
        }
    }
}

// =====================================================================
// Issue #227: Loki sliding-window range evaluation.
//
// A range query re-evaluates the `[range]` window `(t-range, t]` at every
// start-anchored grid point `t ∈ {grid_start + k·step ≤ end}` (Loki's
// `batchRangeVectorIterator`). Rows arrive in physical-key order
// `(service, fingerprint, timestamp_ns)`, so a fingerprint's samples are
// contiguous and ascending, and same-`(fingerprint, timestamp_ns)`
// collision groups arrive consecutively.
//
// Two grouping shapes:
//  - **non-mutating** (output series == fingerprint): a true streaming
//    slide per fingerprint — interleave grid emission with the ascending
//    stream, evicting `T ≤ t-range` and loading `T ≤ t`, so peak memory is
//    one window's contents. The concurrent-retention cap trips here
//    (charge-on-load / discharge-on-evict).
//  - **mutating/regrouping** (a `label_format`/parser merges fingerprints
//    into one output set): fan each sample into every covering grid cell,
//    retaining fixed-width `(ts, stream_hash, tie_rank, value)` points, then
//    reduce each cell — class-C folds in the canonical `(ts, stream_hash,
//    tie_rank)` order, class-B order-independently.
//
// Reducer classes (finding-4 table): A invert-integer, B re-reduce
// order-independent, C re-reduce canonical-fold. See [`reducer_class`].
// =====================================================================

/// The exact upper bound [`ensure_grid_resolution`] can ADMIT, in grid
/// POINTS. The fence saturates ([`fence_intervals`]) while
/// [`grid_point_count`] is exact over i128, so a full-domain span at a
/// step just wide enough to pass the fence admits
/// `2 * (MAX_CLIENT_AGG_BUCKETS + 1)` points — `ensure_grid_resolution`'s
/// own doc says so. DERIVED from the fence, never chosen, and it enforces
/// nothing on its own: the gate is [`MAX_CLIENT_AGG_BUCKETS`] intervals,
/// which is the reference's rule.
pub const MAX_ADMITTED_GRID_POINTS: u64 = 2 * (MAX_CLIENT_AGG_BUCKETS + 1); // 22_002

/// Start-anchored grid-point count `|{grid_start + k·step ≤ end}|` (Loki:
/// `current` seeded at `start-step`, first point `start`, iterate `≤ end`)
/// — i.e. `floor(span/step) + 1`, BOTH endpoints included, EXACT in i128
/// (the emit grid must cover the true span; only the admission fence
/// saturates — [`fence_intervals`], review round 8). Every caller runs
/// AFTER [`ensure_grid_resolution`] admitted the window, which bounds this
/// count at 22_002 (11_001 when the span fits an int64 duration). Still
/// saturates to `u64::MAX` for a degenerate step, defense-in-depth.
pub(in crate::logql) fn grid_point_count(grid_start: i64, end_ns: i64, step_ns: u64) -> u64 {
    if end_ns < grid_start {
        return 0;
    }
    if step_ns == 0 || step_ns > i64::MAX as u64 {
        return u64::MAX;
    }
    let step = step_ns as i128;
    let span = end_ns as i128 - grid_start as i128; // ≥ 0
    u64::try_from(span / step + 1).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::testkit::*;

    // -----------------------------------------------------------------
    // Issue #236 Part D — `ClientAggState` is instant-only.
    // -----------------------------------------------------------------

    /// AC 16 — a stepped window cannot reach [`ClientAggState`].
    ///
    /// The domain is the `ClientWindow` enum, which has exactly two
    /// variants, so it is enumerated rather than sampled. The guard is
    /// UNREPRESENTABILITY, not unreachability: `InstantWindow`'s single
    /// field is private to `mod instant_window`, so no line in the crate
    /// — including this one — can write the witness directly, and
    /// `mint` is the only source. A bare unit struct would have been
    /// merely unreachable, since every `ClientAggState::new` call site
    /// lives in this file.
    #[test]
    fn a_stepped_window_cannot_mint_the_instant_witness() {
        let instant = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };
        assert!(
            instant.as_instant().is_some(),
            "an instant window must narrow"
        );
        // Every stepped shape the constructor could be handed: the
        // narrowest legal grid and a wide one.
        for (start, end, step, range) in [(0i64, 0i64, 1u64, 1u64), (0, 1_000, 10, 45)] {
            let stepped = slide_window(start, end, step, range);
            assert!(
                stepped.as_instant().is_none(),
                "a stepped window must NOT narrow: {stepped:?}"
            );
        }
    }

    /// Review round 2 finding 2: a HOSTILE client-controlled duration is
    /// rejected cleanly at the planner boundary — never narrowed/wrapped.
    #[test]
    fn hostile_durations_are_rejected_at_the_planner_boundary() {
        use super::super::params::MAX_DURATION_NS;
        // In-domain values pass.
        assert_eq!(
            super::super::params::validate_duration_ns(60_000_000_000, "step")
                .unwrap()
                .get(),
            60_000_000_000
        );
        assert_eq!(
            super::super::params::validate_duration_ns(MAX_DURATION_NS as u64, "step")
                .unwrap()
                .get(),
            MAX_DURATION_NS
        );
        // Zero and everything above the domain are named 400s, NOT wraps.
        // Round 10: the domain is the reference's full positive int64, so
        // `MAX_DURATION_NS + 1` IS `i64::MAX as u64 + 1` — the first value
        // that would narrow to a NEGATIVE i64.
        for hostile in [0u64, MAX_DURATION_NS as u64 + 1, u64::MAX] {
            match super::super::params::validate_duration_ns(hostile, "range selector") {
                Err(ReadError::DurationOutOfRange { value, max, .. }) => {
                    assert_eq!(value, hostile);
                    assert_eq!(max, MAX_DURATION_NS);
                }
                other => panic!("hostile duration {hostile} must be rejected, got {other:?}"),
            }
        }
    }

    /// Review round 4, finding 2: the instant/range modelling. A `Range`
    /// window structurally REQUIRES two `ValidatedDuration`s (no zero/absent
    /// representation exists), and an `Instant` window structurally has no
    /// step and no range — so `RangeSlideState` can only receive a real,
    /// validated, non-zero range. `ValidatedDuration::NONE` is gone.
    #[test]
    fn a_range_window_cannot_carry_an_absent_or_zero_range() {
        // The only way to obtain the durations a `Range` window needs.
        let step = super::super::params::validate_duration_ns(10, "step").unwrap();
        let range = super::super::params::validate_duration_ns(30, "range selector").unwrap();
        let w = ClientWindow::Range {
            grid_start_ns: 0,
            end_ns: 100,
            step_ns: step,
            range_ns: range,
        };
        assert_eq!(w.step_ns(), Some(step));
        assert!(range.get() > 0, "a validated range is never zero");
        // And zero can never be minted: the validator rejects it, so no
        // `ValidatedDuration` carrying 0 exists to place in the range slot.
        assert!(super::super::params::validate_duration_ns(0, "range selector").is_err());
        // An instant window has neither a step nor a range slot at all.
        let i = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };
        assert_eq!(i.step_ns(), None);
    }

    /// Review round 2, finding 1 (carried to the round-7 shared guard):
    /// the grid count uses i128/saturating arithmetic — the
    /// extreme/degenerate shapes below must saturate (and REJECT through
    /// [`ensure_grid_resolution`]) or zero out, never panic or wrap past
    /// the cap.
    #[test]
    fn grid_resolution_guard_is_overflow_safe_at_extreme_bounds() {
        // Full i64 range at step 1: ~2^64 points, saturates cleanly — and
        // the guard rejects, reporting the reference's own saturated
        // interval count (`maxDuration / 1ns`, round 8).
        assert_eq!(grid_point_count(i64::MIN, i64::MAX, 1), u64::MAX);
        assert!(matches!(
            ensure_grid_resolution(i64::MIN, i64::MAX, 1),
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricBuckets {
                // The Sub-saturated span (i64::MAX ns) at a 1ns step.
                buckets: 9_223_372_036_854_775_807, // i64::MAX as u64
                cap: MAX_CLIENT_AGG_BUCKETS,
            }))
        ));
        // Inverted/empty windows are zero points and zero fence intervals,
        // never an underflow — 0 points admits.
        assert_eq!(grid_point_count(i64::MAX, i64::MIN, 1), 0);
        assert_eq!(ensure_grid_resolution(i64::MAX, i64::MIN, 1).unwrap(), 0);
        // A single-point window (start == end) is one grid point.
        assert_eq!(grid_point_count(0, 0, 1), 1);
        // A zero step (structurally `InvalidStep` upstream) and a step
        // wider than i64 (never produced by `parse_step`) both saturate
        // so the guard rejects them by name instead of the evaluator
        // ever dividing by a degenerate step.
        assert_eq!(grid_point_count(0, 1_000, 0), u64::MAX);
        assert_eq!(grid_point_count(0, 1_000, u64::MAX), u64::MAX);
        assert!(ensure_grid_resolution(0, 1_000, 0).is_err());
        // Ordinary shapes stay exact (the 11k fence has its own golden):
        // [0, 120] at step 60 holds grid points 0, 60, 120.
        assert_eq!(grid_point_count(0, 120, 60), 3);
    }

    /// Issue #227 review round 7, finding 1: the resolution fence is
    /// Loki's `(end - start) / step > 11000` (TRUNCATING interval count,
    /// `loghttp/query.go` `errStepTooSmall`) — exactly-at-the-limit is
    /// SERVED with its full 11_001-point inclusive grid; one interval
    /// over is rejected. Both engine grid guards (`RangeSlideState::new`
    /// and `materialize_vector_lit`) funnel through
    /// [`ensure_grid_resolution`], so this pins the fence for the whole
    /// engine against the HTTP guard's identical formula.
    #[test]
    fn grid_resolution_fence_serves_11000_intervals_and_rejects_11001() {
        const S: u64 = 1_000_000_000; // 1s step
        let step = S as i64;

        // Exactly at the limit, step-aligned: 11_000 intervals → admitted,
        // 11_001 inclusive grid points.
        assert_eq!(ensure_grid_resolution(0, 11_000 * step, S).unwrap(), 11_001);
        // One under: admitted, 11_000 points.
        assert_eq!(ensure_grid_resolution(0, 10_999 * step, S).unwrap(), 11_000);
        // One interval over: rejected, reporting the INTERVAL count.
        assert!(matches!(
            ensure_grid_resolution(0, 11_001 * step, S),
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricBuckets {
                buckets: 11_001,
                cap: MAX_CLIENT_AGG_BUCKETS,
            }))
        ));
        // Step not dividing the span (truncating division, like Loki's
        // integer `Duration / Duration`): 11_000 intervals + half a step
        // still truncates to 11_000 → admitted, 11_001 points.
        assert_eq!(
            ensure_grid_resolution(0, 11_000 * step + step / 2, S).unwrap(),
            11_001
        );
        // ... and one nanosecond past the 11_001st interval boundary is
        // 11_001 truncated intervals → rejected.
        assert!(ensure_grid_resolution(0, 11_001 * step + 1, S).is_err());
        // Unaligned start (same span, shifted): the fence depends only on
        // `end - start`, exactly like the reference.
        let off = 123_456_789;
        assert_eq!(
            ensure_grid_resolution(off, off + 11_000 * step, S).unwrap(),
            11_001
        );
        assert!(ensure_grid_resolution(off, off + 11_001 * step, S).is_err());
    }

    /// Issue #227 review round 8: the fence's span subtraction SATURATES
    /// exactly like the reference's — Go's `time.Time.Sub` clamps an
    /// out-of-range difference to the int64-ns `Duration` bound
    /// (`maxDuration = 1<<63-1`) instead of widening, and the division
    /// then truncates. A full-domain span at a huge step is therefore
    /// SERVED (`i64::MAX / step ≤ 11_000`) where the previous exact-i128
    /// fence wrongly rejected it.
    #[test]
    fn grid_resolution_fence_saturates_the_span_like_the_reference() {
        // 1_000_000s step over the whole i64 domain: the saturated fence
        // counts i64::MAX/step = 9_223 intervals (admit); the TRUE span
        // (2^64-1 ns) holds 18_446 — an exact fence would reject a request
        // the reference serves.
        const STEP: u64 = 1_000_000_000_000_000; // 1_000_000s in ns
        assert_eq!(fence_intervals(i64::MIN, i64::MAX, STEP), 9_223);
        assert_eq!(
            ensure_grid_resolution(i64::MIN, i64::MAX, STEP).unwrap(),
            // The emitted grid still covers the TRUE span: the exact
            // inclusive point count, not the saturated one.
            18_447
        );

        // The saturated-fence boundary: floor(i64::MAX / 11_001) is the
        // largest step counting 11_001 saturated intervals (reject); one
        // nanosecond of step more counts 11_000 (admit), whose true grid —
        // 22_002 points — is the guard's hard ceiling.
        let reject_step = (i64::MAX / 11_001) as u64;
        assert!(matches!(
            ensure_grid_resolution(i64::MIN, i64::MAX, reject_step),
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricBuckets {
                buckets: 11_001,
                cap: MAX_CLIENT_AGG_BUCKETS,
            }))
        ));
        assert_eq!(
            ensure_grid_resolution(i64::MIN, i64::MAX, reject_step + 1).unwrap(),
            22_002
        );

        // An UNSATURATED span stays exact — a span of exactly i64::MAX ns
        // is the saturation onset, byte-identical either way, and the
        // ordinary domain is untouched.
        assert_eq!(fence_intervals(-1, i64::MAX - 1, STEP), 9_223);
        assert_eq!(
            fence_intervals(0, 11_000 * 1_000_000_000, 1_000_000_000),
            11_000
        );
    }
}
