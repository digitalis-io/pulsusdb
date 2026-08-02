//! The byte-charge model: how many bytes an aggregation is allowed to
//! retain, and how each retained structure is priced.
//!
//! Pure arithmetic over sizes — the `MAX_*` admission caps, [`AggCaps`],
//! the allocator size-class model ([`alloc_block_bytes`],
//! [`grown_alloc_bytes`]), the per-structure pricing helpers
//! ([`group_entry_bytes`], [`map_entry_bytes`], [`label_set_bytes`],
//! [`rendered_labels_json_len`]) and the charge/discharge pairs the
//! stateful aggregators call before they allocate.

use super::error::{ReadError, TooBroadReason};
use pulsus_logql::RangeAggOp;
use std::borrow::Cow;

use super::agg::{LabelSet, VectorAccum};
use super::client_agg::{BucketAcc, MutGroup, WinSample};
use super::exec::{MatrixSeries, QueryResult};
use super::fold::KSel;

/// The exact-quantile retention cap: `quantile_over_time` is the one
/// reducer whose state grows with surviving rows (every value is kept
/// for the interpolation sort) rather than with `buckets x series`.
/// Past this many retained values (~32 MB of f64) the query aborts as
/// `QueryTooBroad(QuantileValues)` — complete-or-error, never OOM
/// (review round 1, finding 1's quantile bound).
pub const MAX_QUANTILE_VALUES: u64 = 4_000_000;

/// The `rate_counter` retention cap (M8-LQ3, code review rounds 2/3):
/// like `quantile_over_time`, the reset walk is order-dependent, so every
/// unwrapped `(ts, value)` sample is retained per bucket until `finish`.
/// A dense range query retains one point per scanned sample, growing the
/// combined per-bucket vectors without bound. Past this many retained
/// points the query aborts as `QueryTooBroad(CounterValues)` —
/// complete-or-error, never OOM and never a silently truncated increase.
/// Bounds only the retained points; the reset-aware rate value is
/// unchanged below the cap. Same ceiling as [`MAX_QUANTILE_VALUES`] (the
/// shared "reducer state grows with surviving rows" class).
///
/// The `push_rows` charge (`counter_values += 1`) is a TRUE bound on the
/// combined retention: [`bucket_of`] maps each scanned row to exactly one
/// step bucket and `push_rows` performs exactly one `Counter` push per
/// charged row — overlapping range windows (`step < range`) do NOT
/// re-retain a sample, since the raw scan delivers each stored sample
/// once and the buckets partition it. So `Σ retained points ==
/// counter_values` (pinned by
/// `rate_counter_cap_bounds_total_retained_points_across_overlapping_buckets`).
pub const MAX_COUNTER_VALUES: u64 = 4_000_000;

/// The reference's `querier.max-query-series` (grafana/loki v3.7.4,
/// `pkg/validation/limits.go:373`, default 500): the number of distinct
/// series a metric query may **RETURN**.
///
/// Enforced on the **FINAL result of the whole expression**
/// (`pkg/logql/engine.go:538` instant / first step, `:588` distinct
/// series accumulated across steps; frontend duplicate at
/// `pkg/querier/queryrange/limits.go:518`) — **never** on scanned groups,
/// inner-aggregation groups or binary operands — **that is the LIMIT's
/// documented meaning, and it is what PulsusDB implements.**
///
/// **What the reference actually does, captured at 501 groups** (the
/// `b15_wide_aggregation.test` capture, boundary confirmed at 499 / 500 /
/// 501): it serves `sum`, `count`, `min`, `max`, `avg`,
/// `sum by (<low-cardinality>)` and `sum(sum by (id) (...))`, and it
/// REJECTS `topk(k)`, `bottomk(k)`, `stddev`, `stdvar`, `sort`,
/// `sum by (id)`, the bare leaf and `sum(topk(600, ...))` — the last of
/// these even though its result is one series. The split is
/// SHARDABILITY, not result size: the reference's frontend rewrites the
/// associative aggregations into per-shard sub-queries so the wide inner
/// vector never materialises, while the others materialise it and trip
/// the cap on that intermediate. PulsusDB applies the limit to the final
/// result only and therefore SERVES the non-shardable ones — an
/// over-acceptance registered as ledger entry `#236 (f)`, in the
/// direction that matters: PulsusDB rejects nothing the reference
/// serves. Exactly 500 served, 501 rejected, on both sides.
///
/// **Read by [`ensure_result_series`] and by nothing else** (issue #236).
/// Applying it to an intermediate would reject on a *proxy* rather than
/// on the resource consumed: an outer `sum` over an inner `sum by (id)`
/// collapses 501+ inner groups to ONE final series, which the reference
/// serves. Intermediate aggregation state is bounded by BYTES
/// ([`MAX_CLIENT_AGG_GROUP_BYTES`]) and POINTS
/// ([`MAX_METRIC_RESULT_POINTS`]) — and by nothing else.
pub const MAX_QUERY_SERIES: u64 = 500;

/// Enforces [`MAX_QUERY_SERIES`] on a metric query's **final** result.
///
/// Counts TOP-LEVEL series: one per `Vector`/`VectorHist` sample and one
/// per `Matrix`/`MatrixHist` series (the reference counts distinct series,
/// not points — `engine.go:588` accumulates a set across steps, and a
/// PulsusDB matrix already holds one entry per distinct series). `Streams`
/// is a log query (bounded by `max_entries_limit_per_query` instead),
/// and `Scalar`/`String` carry no series, so all three pass.
///
/// `> cap` is the reference's own test (`engine.go:538`), so exactly 500
/// is served.
///
/// `pub` so the conformance runner (`tests/logqltest/runner.rs`) applies
/// the identical gate on its own `Expr::Metric` arm and corpus
/// `eval_fail` cases can pin the reference's body. Exporting the FUNCTION
/// keeps [`MAX_QUERY_SERIES`] read in exactly one place.
pub fn ensure_result_series(result: &QueryResult) -> Result<(), ReadError> {
    let n = match result {
        QueryResult::Vector(v) => v.len() as u64,
        QueryResult::Matrix(m) => m.len() as u64,
        QueryResult::VectorHist(v) => v.len() as u64,
        QueryResult::MatrixHist(m) => m.len() as u64,
        QueryResult::Streams { .. } | QueryResult::Scalar(_) | QueryResult::String(_) => {
            return Ok(());
        }
    };
    if n > MAX_QUERY_SERIES {
        return Err(ReadError::QueryTooBroad(TooBroadReason::MetricSeries {
            cap: MAX_QUERY_SERIES,
        }));
    }
    Ok(())
}

/// Every per-query retention cap in ONE place (issue #221), so the
/// `variants(...)` fan-out path can divide them all at a single auditable
/// point. [`AggCaps::DEFAULT`] is today's six constants verbatim;
/// [`AggCaps::divided`] is what a variants query hands each of its `n`
/// sub-states, so the SUM over sub-states is exactly `DEFAULT` for every
/// field and the query's total retention bound is INDEPENDENT of the
/// variant count. Both aggregation states carry one `caps: AggCaps` field
/// in place of the former `group_bytes_cap`/`retention_cap` fields and
/// inline constant reads — the single-extractor path constructs with
/// `DEFAULT`, so its behaviour and every error value are byte-identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AggCaps {
    pub group_bytes: u64,
    pub retention_points: u64,
    pub quantile_values: u64,
    pub counter_values: u64,
    pub collision_members: u64,
    pub collision_bytes: u64,
    /// Issue #236: fixed-width RESULT point-slots — emitted grid points
    /// and the fold's dense per-group slots — an ADMISSION counter, see
    /// [`charge_result_points`].
    pub result_points: u64,
}

impl AggCaps {
    pub(crate) const DEFAULT: AggCaps = AggCaps {
        group_bytes: MAX_CLIENT_AGG_GROUP_BYTES,
        retention_points: MAX_RETAINED_WINDOW_POINTS,
        quantile_values: MAX_QUANTILE_VALUES,
        counter_values: MAX_COUNTER_VALUES,
        collision_members: MAX_TS_COLLISION_GROUP,
        collision_bytes: MAX_TS_COLLISION_GROUP_BYTES,
        result_points: MAX_METRIC_RESULT_POINTS,
    };

    /// The per-sub-state caps for a `variants(...)` query with `n`
    /// sub-states. PLAIN integer division — a `max(1)` floor would break
    /// the sum property and let the emitted-points term scale with N; the
    /// derived backstop [`super::plan::MAX_VARIANT_SUB_STATES`] (`==
    /// min_field()`) is what keeps every divided field ≥ 1. `n` is in
    /// `1..=MAX_VARIANT_SUB_STATES` at every call site.
    pub(crate) fn divided(self, n: u64) -> AggCaps {
        AggCaps {
            group_bytes: self.group_bytes / n,
            retention_points: self.retention_points / n,
            quantile_values: self.quantile_values / n,
            counter_values: self.counter_values / n,
            collision_members: self.collision_members / n,
            collision_bytes: self.collision_bytes / n,
            result_points: self.result_points / n,
        }
    }

    /// The smallest field — the point past which `divided(n)` would floor
    /// a cap to 0. [`super::plan::MAX_VARIANT_SUB_STATES`] is DERIVED from
    /// this (not chosen), so it moves with the caps.
    pub(crate) const fn min_field(self) -> u64 {
        let mut min = self.group_bytes;
        if self.retention_points < min {
            min = self.retention_points;
        }
        if self.quantile_values < min {
            min = self.quantile_values;
        }
        if self.counter_values < min {
            min = self.counter_values;
        }
        if self.collision_members < min {
            min = self.collision_members;
        }
        if self.collision_bytes < min {
            min = self.collision_bytes;
        }
        if self.result_points < min {
            min = self.result_points;
        }
        min
    }
}

/// Cap on the transient full-body buffer that ranks one consecutive
/// same-`(fingerprint, timestamp_ns)` collision group (issue #227). A
/// same-nanosecond, same-stream run larger than this is
/// pathological/adversarial, never real data; exceeding it is a clean
/// `QueryTooBroad(TsCollisionGroup)`, never an OOM. A documented constant
/// (the `DEFAULT_MAX_STREAMS` precedent).
pub const MAX_TS_COLLISION_GROUP: u64 = 10_000;

/// Byte ceiling on the staged same-`(fingerprint, timestamp_ns)` collision
/// group (issue #227 review rounds 3 + 4, finding 1).
///
/// The member-COUNT cap alone is not a memory bound: 10 000 multi-megabyte
/// members are terabytes of RAM while staying inside the byte-scan budget.
///
/// > **Bound proof (re-derived for EVERY reducer class, round 4).**
/// > 1. [`RangeSlideState::stage_member`] is the ONLY place a per-member
/// >    allocation is pushed into `coll` — no reducer class has another
/// >    path, and it is called exactly once per surviving row.
/// > 2. It charges [`RangeSlideState::member_stage_bytes`] BEFORE allocating.
/// >    That figure is an UPPER BOUND covering **every** per-member
/// >    allocation: the body clone (class C + first/last), the rendered
/// >    label JSON — sized EXACTLY by [`rendered_labels_json_len`], so the
/// >    `\u00xx` six-for-one escape expansion is charged — and the cloned
/// >    `LabelSet` with each of its owned strings (any label-mutating
/// >    pipeline; charged for ALL classes, including the integer class A that
/// >    stages no body), each through [`alloc_block_bytes`] /
/// >    [`grown_alloc_bytes`] so container growth and the allocator's
/// >    size-class rounding (round 7) are covered. Classes that allocate
/// >    nothing extra charge only the `CollMember` slot.
/// > 3. The staging is refused unless
/// >    `coll_bytes + charge <= MAX_TS_COLLISION_GROUP_BYTES`, so the
/// >    breaching allocation is never performed; on success `coll_bytes`
/// >    grows by exactly the charge.
/// > 4. `flush_collision` empties `coll` and resets `coll_bytes` together at
/// >    every `(fingerprint, timestamp_ns)` boundary, so exactly one group is
/// >    staged at a time and the counter can neither leak nor double-count
/// >    across groups (including when a group straddles a fetch chunk: the
/// >    straddling group is simply never flushed between chunks, so its
/// >    charge accumulates continuously in the same counter).
/// >
/// > Therefore `actual staged heap bytes <= coll_bytes <=
/// > MAX_TS_COLLISION_GROUP_BYTES` **at all times, for every reducer class,
/// > any body/label size and any density**. A breach is a clean
/// > `TsCollisionGroup` error — never a truncated group, so the `tie_rank`
/// > order is never silently partial.
/// >
/// > **What survives a flush.** `Vec::clear` DROPS every element (bodies,
/// > rendered JSON and cloned label sets are freed) but keeps `coll`'s own
/// > element buffer. That spare capacity is bounded by the largest group
/// > ever staged, whose slot charge (`VEC_GROWTH_FACTOR ×
/// > size_of::<CollMember>()` per member) was itself inside the cap — so
/// > the residual buffer is `<= MAX_TS_COLLISION_GROUP_BYTES` too, and no
/// > content bytes survive.
///
/// 8 MiB staged for one nanosecond in one stream is far beyond real data
/// (real groups are size 1), so nothing servable is rejected.
pub const MAX_TS_COLLISION_GROUP_BYTES: u64 = 8 * 1024 * 1024;

/// The floor one owned heap allocation costs however short its payload is:
/// `RawVec`'s `min_non_zero_cap` is 8 for byte-sized elements, every
/// mainstream allocator has a smallest size class / minimum chunk of ≤ 32
/// bytes, and glibc's minimum chunk is exactly 32 (issue #227 review
/// round 5, finding 1). The floor keeps [`alloc_block_bytes`] a bound
/// independent of whichever `ToString`/`clone` capacity specialization the
/// standard library happens to use.
pub(in crate::logql) const MIN_ALLOC_BYTES: u64 = 32;

/// A conservative, provable UPPER BOUND on the heap block a real allocator
/// RETAINS for one **exactly reserved** allocation of `content` payload
/// bytes (`String::clone`, `str::to_owned`, `collect` from a `TrustedLen`
/// iterator — all reserve exactly the final length in ONE allocation).
///
/// Issue #227 review round 7, finding 2: charging the request size itself
/// (`content.max(32)`) was NOT an upper bound — allocators round a request
/// up to a size class, so a 33-byte string can retain a 48- or 64-byte
/// block and adversarial label strings undercounted staging and
/// query-lifetime group memory by a constant factor. The model here is a
/// documented over-approximation, `2·content` floored at
/// [`MIN_ALLOC_BYTES`], NOT any specific allocator's class table. Why
/// `2·content` dominates every mainstream allocator's retained block
/// (header/metadata included) for a `content`-byte request:
///
/// - **Size-class allocators with out-of-band metadata** (jemalloc,
///   mimalloc, tcmalloc, snmalloc): no mainstream class grid is coarser
///   than powers of two, so `class(n) ≤ next_pow2(n) ≤ 2n` for every
///   `n ≥ 1`, and the smallest classes sit under the 32-byte floor.
/// - **Inline-header, 16-byte-binned allocators** (glibc malloc): chunk =
///   `align16(n + 8)` with a 32-byte minimum — `≤ n + 23 ≤ 2n` for
///   `n ≥ 23`, and `≤ 32` (the floor) for `n ≤ 23`.
/// - **Page-rounded huge allocations** (mmap past the ~128 KiB
///   threshold): `n + header + 4 KiB page slack ≤ 2n` at those sizes.
///
/// Over-charging is the safe direction (a breach is a clean 422, never an
/// OOM); the factor halves no real workload's headroom — the caps'
/// margins are orders of magnitude above real label/body sizes.
pub(crate) const fn alloc_block_bytes(content: u64) -> u64 {
    // `const`-compatible spelling of `content.saturating_mul(2)
    // .max(MIN_ALLOC_BYTES)` (`Ord::max` is not const): needed so
    // [`MAX_FEEDER_SCRATCH_BYTES`] derives from the SAME rounding model at
    // compile time (issue #244) — one scheme, no const twin to drift.
    let doubled = content.saturating_mul(2);
    if doubled > MIN_ALLOC_BYTES {
        doubled
    } else {
        MIN_ALLOC_BYTES
    }
}

/// An upper bound on the heap bytes a **geometrically grown** buffer of
/// `content` final payload bytes occupies at its peak, allocator rounding
/// included. `RawVec::grow_amortized` sets `cap = max(2·cap_old,
/// required)`, and `cap_old < required <= content` at the last growth, so
/// at the realloc peak two blocks are live: the new buffer (request
/// `≤ 2·content`, retained `≤ alloc_block_bytes(2·content) ≤
/// 2·alloc_block_bytes(content)`) and the old buffer (request
/// `< content`, retained `≤ alloc_block_bytes(content)`) — `3·
/// alloc_block_bytes(content)` dominates the sum for every input (review
/// round 7, finding 2: the previous `3·content` covered the request
/// bytes but not the allocator's per-block rounding).
pub(crate) fn grown_alloc_bytes(content: u64) -> u64 {
    alloc_block_bytes(content).saturating_mul(3)
}

/// The exact number of bytes [`push_json_string`] appends for `s` — the two
/// quotes plus the escaped body — computed WITHOUT allocating so the
/// collision-group charge can size the rendered JSON *before* it is built
/// (issue #227 review round 5, finding 1: charging the content once
/// under-counted the `\u00xx` six-for-one expansion of a C0 control byte).
/// [`rendered_labels_json_len_matches_the_renderer_byte_for_byte`] pins this
/// against the renderer itself, so the two cannot drift.
fn json_string_len(s: &str) -> u64 {
    let mut n: u64 = 2; // the enclosing quotes
    for c in s.chars() {
        n = n.saturating_add(match c {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{08}' | '\u{0C}' => 2,
            c if (c as u32) < 0x20 => 6, // `\u00xx`
            c => c.len_utf8() as u64,
        });
    }
    n
}

/// The exact byte length [`render_labels_json_sorted`] will produce for
/// `sorted_labels` (`{"k":"v",...}`), computed without allocating.
pub(in crate::logql) fn rendered_labels_json_len(
    sorted_labels: &[(Cow<'_, str>, Cow<'_, str>)],
) -> u64 {
    let mut n: u64 = 2; // `{` and `}`
    for (i, (k, v)) in sorted_labels.iter().enumerate() {
        if i > 0 {
            n = n.saturating_add(1); // `,`
        }
        n = n
            .saturating_add(json_string_len(k))
            .saturating_add(1) // `:`
            .saturating_add(json_string_len(v));
    }
    n
}

/// The sliding-window **concurrent** retained-point cap (issue #227):
/// charge-on-load / discharge-on-evict across every retained window entry
/// (non-mutating deque + mutating cells). The invariant `retained ≤ cap`
/// holds at all times, so process memory is bounded to ≈ `cap × entry
/// width` regardless of `[range]`/step/density — the charge is per-load, so
/// it trips as the FIRST oversized window fills, DURING the scan, before
/// RSS grows. Generalizes the instant path's per-reducer
/// [`MAX_QUANTILE_VALUES`]/[`MAX_COUNTER_VALUES`] total-retention proofs
/// into one concurrent invariant. Same 4M magnitude, set generously so
/// nothing Loki reliably serves is rejected (see the cap-generosity corpus
/// case).
///
/// **Unit and byte relationship.** One point is one [`WinSample`]. Actual
/// process bytes are `retained × size_of::<WinSample>()` scaled by the
/// container's allocator growth slack (a `VecDeque`/`Vec` holds up to ~2×
/// its length, plus the old buffer during a realloc) — i.e. `O(cap)`, with
/// no axis that scales with `[range]`, step, cardinality or density. Any
/// per-emit scratch a reducer needs on top of the window is charged
/// per-sample up front by [`retention_points_per_sample`], so it is inside
/// the same bound.
pub const MAX_RETAINED_WINDOW_POINTS: u64 = 4_000_000;

/// **The one retention gate.** Every sliding-path allocation that scales
/// with scanned data is sized in retention POINTS, checked against the cap,
/// and accounted HERE — *before* the allocation is performed (issue #227
/// review round 5, finding 2: the charge sites used to be scattered, and
/// three of them charged AFTER inserting, so the breaching allocation was
/// made before the cap could refuse it). Every caller on the sliding path
/// funnels through this function and [`discharge_retention`], so the
/// "size → check the cap → allocate" rule has exactly one implementation.
///
/// `saturating_add` cannot mask a breach: saturation only ever makes the
/// sum LARGER, so the comparison still rejects.
pub(in crate::logql) fn charge_retention(
    retained: &mut u64,
    points: u64,
    cap: u64,
) -> Result<(), ReadError> {
    let next = retained.saturating_add(points);
    if next > cap {
        return Err(ReadError::QueryTooBroad(TooBroadReason::MetricRetention {
            count: next,
            cap,
        }));
    }
    *retained = next;
    Ok(())
}

/// Releases a charge made by [`charge_retention`], called as the charged
/// memory is freed (eviction, series close, cell consumption). Saturating
/// for panic-proofing only — the charge/discharge pairing is exact, which
/// `finish`'s `retained == 0` post-condition asserts.
pub(in crate::logql) fn discharge_retention(retained: &mut u64, points: u64) {
    *retained = retained.saturating_sub(points);
}

/// Retention points charged per retained sample, in [`WinSample`] units.
///
/// Most reducers re-reduce over a BORROW of the live window and allocate
/// nothing per emit, so one retained sample costs exactly one point.
/// `quantile_over_time` is the sole exception: its reduction must SORT the
/// values, and it cannot sort the window itself (the deque's ascending-`ts`
/// order is what eviction depends on), so it materializes a `Vec<f64>` copy
/// of the LIVE window at every grid point.
///
/// That copy is charged up front, at load, rather than at the emit that
/// makes it — which is what keeps the rule "the cap approves an allocation
/// before it happens" true for a copy taken deep inside a non-fallible
/// reduce. **Bound:** the copy is `8 × W` bytes for a live window of `W`
/// samples (`collect` from a `TrustedLen` iterator reserves exactly `W`),
/// while the extra point charges `size_of::<WinSample>()` = 32 bytes per
/// sample — 4× headroom, so the charge dominates the copy for every window
/// size and every allocator slack.
pub(in crate::logql) fn retention_points_per_sample(op: RangeAggOp) -> u64 {
    match op {
        RangeAggOp::QuantileOverTime => 2,
        _ => 1,
    }
}

/// The **query-lifetime** byte cap on the label-mutating client-aggregation
/// path's distinct output-group state (issue #227 review round 6): each
/// first-seen group MOVES its rendered JSON key and cloned final `LabelSet`
/// into the group map, where they live until finish — bytes charged against
/// NEITHER the collision-group cap (whose counter resets when the group
/// flushes) nor the retention cap (denominated in fixed-width points). The
/// group COUNT used to be bounded too, but multi-MiB extracted label sets
/// were a real memory hazard at any count; this cap bounds their BYTES,
/// charged through [`charge_group_bytes`] BEFORE the insertion that
/// retains each entry and released as finish consumes it.
///
/// **Since issue #236 this is the ONLY bound on the group axis.** The
/// mid-scan group-count cap is deleted (it rejected queries the reference
/// serves — see [`MAX_QUERY_SERIES`]), so every row of the table below
/// that used to lean on a count now leans on bytes or on the grid.
/// Raised 64 MiB → 256 MiB with that deletion (owner ruling O1): the
/// count cap used to keep the group axis small, and the byte cap now
/// carries the whole load.
///
/// **The query-lifetime container audit** (round 6 — earlier sweeps covered
/// the per-sample/per-window dimension; this is the per-QUERY one). Every
/// container that lives for the whole evaluation, and what bounds its bytes:
///
/// | container | bytes bounded by |
/// |---|---|
/// | `RangeSlideState::groups` keys + `MutGroup::labels` | **charged here** (before insertion; released as `finish_in_place` consumes each entry) |
/// | `ClientAggState::label_groups` keys + `LabelSet`s | **charged here** (same helpers, same discipline) |
/// | `MutGroup::int_cells`/`pt_cells`, `FpSlide::win`, `BucketAcc` retention | [`MAX_RETAINED_WINDOW_POINTS`] / [`MAX_QUANTILE_VALUES`] / [`MAX_COUNTER_VALUES`], charge-before-insert |
/// | `base_labels` / `hashes` (both states) | built once from the stage-2 hydration read, which is scan-budget-capped (`max_bytes_to_read`) and stream-capped (`max_streams`); never grown per row |
/// | `ClientAggState::fp_groups` (non-mutating instant) | **charged here** since issue #236 P1 ([`FP_GROUP_SLOT`]), inside the existing `contains_key` probe — before #236 this arm was count-gated only and its bytes were never charged |
/// | non-mutating output labels (`FpSlide::labels`, `series_out`) | **charged here** since issue #236 P2 ([`SERIES_OUT_SLOT`]), before the `labels` clone; discharged at the slider's no-points drop or as `finish_in_place` releases the vector |
/// | emitted points (`FpSlide::points`, `series_out`, fan-out `points`) | fixed-width `(i64, f64)`, charged against [`MAX_METRIC_RESULT_POINTS`] — the former `series × grid` product leaned on the deleted count cap |
/// | `present_cover` / `present` | grid-sized (≤ [`MAX_CLIENT_AGG_BUCKETS`] + 2 entries) |
/// | `absent_labels` | cloned from the parsed query text — bounded by request size |
/// | `coll` staging | NOT query-lifetime: one group at a time, ≤ [`MAX_TS_COLLISION_GROUP_BYTES`] |
/// | `approx_topk` sketch + retention (`cms::CountMinSketch`, `cms::Retention`) | compile-time constants — the 13-row table on `approx_topk_instant`, peak ≤ 7 360 882 B, no input-scaled term (issue #221) |
/// | variants: `VariantArena::{pipelines,slot}` + `VariantsAggState::{subs,sub_charged}` buffers | **charged** as ONE [`variant_driver_buffer_bytes`] term before the first is allocated (N ≥ 2) |
/// | variants: each arena entry (a distinct non-empty unwrap tail) | **charged** ([`variant_pipeline_entry_bytes`]) before `extended_with`; the regex PROGRAMS are `Arc`-shared, only cache pools are new |
/// | variants: each extra sub-state's boxed slot / `base_labels`(+`hashes`) snapshot (C table share + H payload) / range `absent_labels` clone / absent `present_cover` | **charged** ([`variant_state_bytes`]) before construction; sub-state 0 charges 0 (a 1-variant query is admitted exactly when the plain query is) |
/// | variants: `MetricNode::Variants::variants` vec + each `VariantSpec`'s tail/absent/grouping clones (incl. the CREATED `by (__variant__)` grouping and the pushed-into `Grouping::labels` realloc) | **charged at PLAN time** ([`variant_spec_bytes`] + the spec-vector buffer) into the SAME counter the arena continues (`spec_bytes`) — one budget, never two |
/// | variants: per-sub-state growing state (`groups`/retention/collision/quantile/counter) | [`AggCaps::divided`]`(n)` — the per-field SUM over sub-states is exactly the single-query bound above |
/// | post-aggregation selection/grouping keys (`select_k_*`/`group_*`'s `HashMap<LabelSet, _>` owned `group_key` copies) | **flagged, not yet charged** (issue #241): bounded only indirectly by the upstream hydration/series caps; `approx_topk` itself is exempt structurally (`grouping == None` ⇒ a single empty key) |
///
/// **Value: 256 MiB, raised from 64 MiB by issue #236 (owner ruling O1).**
/// With the group-COUNT cap deleted this became the only bound on the
/// group axis, so it had to admit the high-cardinality shapes #236 exists
/// to serve. At the stated 6-pair label model that is ≈ 80 321 admitted
/// groups — 3.4× the pre-#236 figure and comfortably above the issue's
/// reported 20 505-group probe shape. The residual, stated rather than
/// hidden: above ≈ 80 321 groups PulsusDB still refuses where the
/// reference serves. That is a bounded divergence, not a fix — the real
/// fix is a step-ordered evaluator (#250). O2 (1 GiB) was rejected
/// because per-query ceilings do not compose across concurrent queries;
/// O4 (operator-configurable) routes to #25.
pub const MAX_CLIENT_AGG_GROUP_BYTES: u64 = 256 * 1024 * 1024;

/// The cap on emitted points, fold cells and fold slots — one counter
/// each, charged before allocation (issue #236).
///
/// DERIVED, not chosen: a result the reference will serve carries at most
/// [`MAX_QUERY_SERIES`] series, and each holds at most
/// [`MAX_ADMITTED_GRID_POINTS`] points, so every counter's provable
/// maximum for a servable result is `500 × 22 002 = 11 001 000`. The
/// shipped value is the next round figure above it, so no servable result
/// is ever refused by this cap.
///
/// It replaces the structural `series × grid` product that the deleted
/// group-count cap used to supply. Unlike that product it does not reject
/// on a group COUNT, so a wide scan collapsing to a narrow result passes
/// it untouched.
pub const MAX_METRIC_RESULT_POINTS: u64 = 12_000_000;

/// **The one group-byte gate FUNCTION — N counters** (round 6; the
/// counter count made explicit by issue #260): every query-lifetime
/// group-map insertion is sized by [`group_entry_bytes`], checked against
/// the cap, and accounted HERE — *before* the insertion that retains the
/// entry, so the map never holds a refused group. `saturating_add` cannot
/// mask a breach (saturation only grows the sum).
///
/// "One gate" is a statement about the FUNCTION, never about the counter:
/// a leaf can hold [`LEAF_COUNTERS`]`.group_bytes` independent counters
/// against this cap at once, so the bytes the cap proves are the SUM over
/// them — see the [`CounterPlurality`] table and
/// [`MAX_LEAF_RETAINED_BYTES`].
pub(in crate::logql) fn charge_group_bytes(
    charged: &mut u64,
    bytes: u64,
    cap: u64,
) -> Result<(), ReadError> {
    let next = charged.saturating_add(bytes);
    if next > cap {
        return Err(ReadError::QueryTooBroad(
            TooBroadReason::MetricGroupLabelBytes { bytes: next, cap },
        ));
    }
    *charged = next;
    Ok(())
}

/// **The one result-point gate FUNCTION — N counters** (issue #236; the
/// counter count made explicit by issue #260): every fixed-width point
/// slot a metric evaluation will RETAIN is reserved here, against
/// [`MAX_METRIC_RESULT_POINTS`], *before* the allocation that holds it.
/// A leaf holds [`LEAF_COUNTERS`]`.result_points` of them at once, at
/// DIFFERENT slot widths ([`RESULT_POINT_SLOT_BYTES`]) — see
/// [`MAX_LEAF_RETAINED_BYTES`].
///
/// An ADMISSION counter, not a concurrent-retention one — there is no
/// discharge, because the slots it reserves hold the RESULT and live
/// until the result is returned. `charge_retention`'s counters return to
/// zero at finish; this one asserts a charge IDENTITY instead (the tests
/// pin `charged == series x (kmax + 1)`).
///
/// **Charged `O(1)` per output series and per fold group, never per
/// point.** Each reservation is the grid's full width (`kmax + 1`), which
/// is both an exact upper bound on what one series can emit and the
/// model [`MAX_METRIC_RESULT_POINTS`] is derived from
/// (`MAX_QUERY_SERIES x MAX_ADMITTED_GRID_POINTS`) — so the gate and the
/// constant speak the same units, and the read path gains no per-point
/// work.
///
/// `saturating_add` cannot mask a breach (saturation only grows the sum)
/// but keeps a pathological reservation from wrapping the comparison.
pub(in crate::logql) fn charge_result_points(
    charged: &mut u64,
    points: u64,
    cap: u64,
) -> Result<(), ReadError> {
    let next = charged.saturating_add(points);
    if next > cap {
        return Err(ReadError::QueryTooBroad(
            TooBroadReason::MetricResultPoints { count: next, cap },
        ));
    }
    *charged = next;
    Ok(())
}

/// Releases a [`charge_group_bytes`] charge as finish consumes the entry it
/// paid for. Saturating for panic-proofing only — the pairing is exact
/// (charge and discharge run [`group_entry_bytes`] over the SAME unmodified
/// key/labels), which the finish `group_bytes == 0` post-conditions assert.
pub(in crate::logql) fn discharge_group_bytes(charged: &mut u64, bytes: u64) {
    *charged = charged.saturating_sub(bytes);
}

/// The map-entry slot the sliding path's group charge sizes (`groups`).
pub(in crate::logql) const MUT_GROUP_SLOT: usize = size_of::<(String, MutGroup)>();

/// The map-entry slot the instant path's group charge sizes
/// (`label_groups`).
pub(in crate::logql) const INSTANT_GROUP_SLOT: usize = size_of::<(String, (LabelSet, BucketAcc))>();

/// The map-entry slot the non-mutating instant path's group charge sizes
/// (`fp_groups`) — issue #236 P1. The key is a `u64` fingerprint, not a
/// rendered string, so the charge passes an empty key to
/// [`group_entry_bytes`] and the `LabelSet` term prices the hydrated
/// `base_labels` value the group stands for.
pub(in crate::logql) const FP_GROUP_SLOT: usize = size_of::<(u64, BucketAcc)>();

/// The map-entry slot a FOLD group occupies. Both fold maps are keyed by
/// the `LabelSet` the grouping projects; the value differs per fold, so
/// the larger of the two is charged (over-charging is the safe
/// direction, and one constant keeps the two sites speaking one
/// vocabulary).
pub(in crate::logql) const FOLD_GROUP_SLOT: usize = {
    let reduce = size_of::<(LabelSet, Vec<VectorAccum>)>();
    let select = size_of::<(LabelSet, Vec<KSel>)>();
    if reduce > select { reduce } else { select }
};

/// A `Vec` element sized through the map-entry helper — issue #236 P2.
/// `series_out` is a `Vec`, not a map, so this is a deliberate
/// OVER-charge: both leaf paths then speak ONE vocabulary
/// ([`group_entry_bytes`]) and the derivation has a single term to reason
/// about instead of two.
pub(in crate::logql) const SERIES_OUT_SLOT: usize = size_of::<MatrixSeries>();

/// A provable UPPER BOUND on the query-lifetime heap bytes ONE distinct
/// output group's map entry retains: the rendered-JSON key, the cloned
/// `LabelSet` (each owned string plus the element buffer), and the entry's
/// share of the map table itself. Reuses the round-5
/// [`RangeSlideState::member_stage_bytes`] sizing vocabulary
/// ([`alloc_block_bytes`] / [`grown_alloc_bytes`] / [`MIN_ALLOC_BYTES`]) —
/// one scheme, not two. Over-charging is safe; under-charging is the bug.
///
/// **Per allocation** (each retained block charged through the round-7
/// allocator-rounding model, [`alloc_block_bytes`]):
/// - **rendered key** — produced by [`render_labels_json_sorted`], whose
///   pre-size (`2 + Σ(k+v+6)`) is ≤ `len + 1` (each pair renders at least
///   its raw bytes plus the `"":"",` scaffolding, minus the final comma),
///   and whose geometric growth ends below `2·len`; by charge time
///   rendering has returned, so the LIVE request is ≤ `2·len`, retained
///   ≤ `alloc_block_bytes(2·len)` — dominated by
///   [`grown_alloc_bytes`]`(len) = 3·alloc_block_bytes(len)` for every
///   input.
/// - **each owned label `String`** — `Cow::to_string` reserves exactly the
///   length (or the generic path's `min_non_zero_cap = 8`); both requests
///   are inside [`alloc_block_bytes`]'s size-class-rounded bound.
/// - **`LabelSet` element buffer** — `collect` from a `TrustedLen` iterator
///   reserves exactly `pairs`; charged via [`alloc_block_bytes`] (an empty
///   set allocates nothing — the 32-byte floor is pure margin).
/// - **the map entry's table share** — hashbrown keeps at most ~3.43
///   bucket-slots live per entry at peak (7/8 load factor, power-of-two
///   doubling with the old table still mapped during a resize), each
///   `slot_bytes + 1` control byte wide; with each table block retained at
///   ≤ 2× its request (the [`alloc_block_bytes`] model) that peak is
///   ≤ ~6.86 slots per entry — `8×` dominates it for every entry count,
///   and the flat pad covers the table's fixed control-group padding
///   (also ≤ 2×-rounded) many times over. (`MAP_GROWTH_FACTOR = 8` is a
///   LOAD-FACTOR argument and holds for any entry count — issue #236
///   deleted the group-count cap that used to be quoted here as an
///   additional structural bound, and the bound is unaffected.)
///
/// Saturating arithmetic throughout — a hostile label set can only make
/// the charge LARGER, the safe direction.
pub(in crate::logql) fn group_entry_bytes(key: &str, labels: &LabelSet, slot_bytes: usize) -> u64 {
    map_entry_bytes(slot_bytes)
        .saturating_add(grown_alloc_bytes(key.len() as u64))
        .saturating_add(label_set_bytes(labels))
}

/// One map entry's TABLE share: the slot plus its control byte at the
/// growth factor, plus the flat pad. Extracted VERBATIM from
/// [`group_entry_bytes`]'s map-table arithmetic (issue #221) so the
/// variants `meta`-snapshot charge and the group charge share ONE sizing
/// scheme — `group_entry_bytes`'s output is byte-identical to the
/// pre-extraction form (pinned by its existing unit tests).
pub(crate) fn map_entry_bytes(slot_bytes: usize) -> u64 {
    /// See the map-table bullet above: 8 slots per entry dominates the
    /// ~3.43-slot live peak of a 7/8-load, doubling hash table with every
    /// block retained at ≤ 2× its request (review round 7, finding 2).
    const MAP_GROWTH_FACTOR: u64 = 8;
    /// Flat per-entry margin dominating the table's fixed control-group
    /// padding/alignment (≤ 2×-rounded) on every table the series cap
    /// permits.
    const MAP_FIXED_PAD: u64 = 128;

    (slot_bytes as u64)
        .saturating_add(1)
        .saturating_mul(MAP_GROWTH_FACTOR)
        .saturating_add(MAP_FIXED_PAD)
}

/// A cloned [`LabelSet`]'s retained heap: one owned `String` per key and
/// per value (each ≤ [`alloc_block_bytes`]'s size-class-rounded bound)
/// plus the exactly-reserved element buffer. Extracted from
/// [`group_entry_bytes`] (issue #221) so the variants charges reuse the
/// existing vocabulary — one scheme, not two.
pub(crate) fn label_set_bytes(labels: &LabelSet) -> u64 {
    let mut bytes: u64 = 0;
    for (k, v) in labels {
        bytes = bytes
            .saturating_add(alloc_block_bytes(k.len() as u64))
            .saturating_add(alloc_block_bytes(v.len() as u64));
    }
    let elems = (labels.len() as u64).saturating_mul(size_of::<(String, String)>() as u64);
    bytes.saturating_add(alloc_block_bytes(elems))
}

// =====================================================================
// Issue #260 — the COMPOSED bound.
//
// Each gate above is "the one gate" in the sense that a whole class of
// allocation passes through a single FUNCTION. It is not one COUNTER:
// one metric leaf can hold several independent counters against the SAME
// cap at the same time, so the bytes a cap actually proves are the SUM
// over its live counters, not the cap. The pluralities are small and
// fixed, and the owner ruled 2026-08-01 that the counters stay separate
// — sharing one pot would save ~0.8 GB of ~1.8 GB, the same operational
// band, at the price of rejecting the `sum by (many-names)`
// high-cardinality shape #236 deliberately enabled. So the deliverable
// is the honest total, DERIVED from the caps and the slot widths rather
// than written down.
//
// **What holds this together, split by WHO holds it** — stated this way
// because two review rounds faulted a claim that ran ahead of its
// mechanism.
//
// **The compiler holds** that every [`AggCaps`] axis has a multiplicity
// AND a term in the sum. `AggCaps` is, by its #221 contract, every
// per-query retention cap in one place; [`CounterPlurality`] mirrors its
// fields, and BOTH are destructured with explicit field lists in
// [`leaf_retained_bytes`] itself and in
// `the_plurality_table_mirrors_every_agg_caps_axis`. Adding an axis is a
// build failure in the derivation, not a silently missing term (review
// round 2, finding 2: the previous version enumerated the axes but
// field-ACCESSED them in the arithmetic, so a new one could pass every
// table and still be absent from the total).
//
// **The censuses are TRIPWIRES, not proofs.** They are lexical, so a
// destructured or aliased cap read, or a qualified call, evades them:
//   * `every_cap_read_is_enumerated` — every production `caps.<field>`
//     read, with the counter it enforces. This is what catches a counter
//     charged INLINE rather than through a gate function — the shape
//     that hid `quantile_values`/`counter_values` in round 1.
//   * `every_charge_counter_is_enumerated` — the four gate functions'
//     first arguments, naming the counter EXPRESSION behind each.
//
// **Nothing here holds** that a declared multiplicity is the true one,
// or that a retained structure governed by NO cap is accounted for. The
// residual list on [`MAX_LEAF_RETAINED_BYTES`] names the known ones.
// =====================================================================

/// How many LIVE counters enforce each cap inside ONE metric leaf
/// (issue #260). One field per [`AggCaps`] axis, in the same order.
///
/// | cap | live counters | max live at once |
/// |---|---|---|
/// | [`MAX_CLIENT_AGG_GROUP_BYTES`] | `ClientAggState::group_bytes` (instant) **XOR** `RangeSlideState::group_bytes` (range) — the two arms of `MetricAggState`; plus `ReduceFold::group_bytes` **XOR** `SelectFold::group_bytes` (the two arms of `VectorAggFold`), whose cap is fed from the slider's own `caps.group_bytes` at `attach_fold`; plus `VariantsAggState::charged` against [`super::variants::MAX_VARIANT_FANOUT_STATE_BYTES`] (`== AggCaps::DEFAULT.group_bytes`), whose sub-states are `AggCaps::divided(n)` and never take a fold | **2** — `{slider, fold}` on the folded range path, `{fan-out, Σ sub-states}` on the variants path |
/// | [`MAX_RETAINED_WINDOW_POINTS`] | `RangeSlideState::retained` | **1** |
/// | [`MAX_QUANTILE_VALUES`] | `ClientAggState::quantile_values` — the instant path's `BucketAcc::Values(Vec<f64>)` retention. (The RANGE path's `quantile_over_time` retention is charged into `RangeSlideState::retained` instead, at [`retention_points_per_sample`] = 2, so it is inside the retention term, not this one.) | **1** |
/// | [`MAX_COUNTER_VALUES`] | `ClientAggState::counter_values` — the instant path's `BucketAcc::Counter(Vec<(i64, f64)>)` retention; the range path's is likewise inside the retention term | **1** |
/// | [`MAX_TS_COLLISION_GROUP`] | `RangeSlideState`'s staged member COUNT — a count cap whose members' BYTES are charged into `coll_bytes` in the same guard, so it contributes no bytes of its own (see [`leaf_retained_bytes`]) | **1** |
/// | [`MAX_TS_COLLISION_GROUP_BYTES`] | `RangeSlideState::coll_bytes` | **1** |
/// | [`MAX_METRIC_RESULT_POINTS`] | `RangeSlideState::result_points`; `ReduceFold::slots` **XOR** `SelectFold::slots` | **2**, at the two DIFFERENT widths in [`RESULT_POINT_SLOT_BYTES`] |
///
/// The instant and range arms of `MetricAggState` are mutually
/// exclusive, so several of these rows can never be live together. The
/// bound SUMS them anyway: a max-over-arms figure would be smaller but
/// would depend on an arm analysis a future change could silently
/// invalidate, and over-stating a memory ceiling is the safe direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::logql) struct CounterPlurality {
    pub group_bytes: u64,
    pub retention_points: u64,
    pub quantile_values: u64,
    pub counter_values: u64,
    pub collision_members: u64,
    pub collision_bytes: u64,
    pub result_points: u64,
}

/// The multiplicity table above, as the value [`MAX_LEAF_RETAINED_BYTES`]
/// consumes.
pub(in crate::logql) const LEAF_COUNTERS: CounterPlurality = CounterPlurality {
    group_bytes: 2,
    retention_points: 1,
    quantile_values: 1,
    counter_values: 1,
    collision_members: 1,
    collision_bytes: 1,
    result_points: 2,
};

/// The fixed slot width each [`charge_result_points`] counter reserves,
/// one entry per counter in [`LEAF_COUNTERS`]`.result_points`. The two
/// counters price DIFFERENT things — the slider reserves emitted
/// `(timestamp, value)` grid points, the fold reserves dense accumulator
/// slots — so the composed bound sums the terms rather than multiplying
/// one of them by the count.
pub(in crate::logql) const RESULT_POINT_SLOT_BYTES: [u64; 2] = [
    // `RangeSlideState::result_points` / `FpSlide::points` / `series_out`.
    size_of::<(i64, f64)>() as u64,
    // `ReduceFold::slots` (a `VectorAccum`) XOR `SelectFold::slots` (a
    // `KSel`) — the wider of the two, since only one can be live.
    fold_slot_bytes(),
];

/// One [`MAX_RETAINED_WINDOW_POINTS`] point is one [`WinSample`].
pub(in crate::logql) const RETENTION_POINT_BYTES: u64 = size_of::<WinSample>() as u64;

/// One [`MAX_QUANTILE_VALUES`] value is one `f64` in the
/// `BucketAcc::Values` vector.
pub(in crate::logql) const QUANTILE_VALUE_BYTES: u64 = size_of::<f64>() as u64;

/// One [`MAX_COUNTER_VALUES`] value is one `(timestamp, value)` pair in
/// the `BucketAcc::Counter` vector.
pub(in crate::logql) const COUNTER_VALUE_BYTES: u64 = size_of::<(i64, f64)>() as u64;

/// A staged collision MEMBER's own contribution to the bound: NONE.
/// Every member is sized by `RangeSlideState::member_stage_bytes` and
/// refused unless it fits `coll_bytes` in the SAME guard, so the
/// [`MAX_TS_COLLISION_GROUP`] count cap admits no byte the
/// [`MAX_TS_COLLISION_GROUP_BYTES`] term has not already priced.
///
/// Named and multiplied through rather than omitted: the axis stays
/// visibly accounted for in [`leaf_retained_bytes`], and a future change
/// that gives members memory of their own has one place to land.
pub(in crate::logql) const COLLISION_MEMBER_BYTES: u64 = 0;

/// The wider of the two [`VectorAggFold`] arms' slot types
/// ([`super::agg::VectorAccum`] / [`super::fold::KSel`]). `Ord::max` is
/// not `const`, so the comparison is spelled out — the same shape
/// [`FOLD_GROUP_SLOT`] uses.
///
/// [`VectorAggFold`]: super::fold::VectorAggFold
const fn fold_slot_bytes() -> u64 {
    let reduce = size_of::<VectorAccum>() as u64;
    let select = size_of::<KSel>() as u64;
    if reduce > select { reduce } else { select }
}

/// The array and the declared multiplicity are the SAME number, checked
/// at compile time: adding a result-point counter without pricing its
/// slot (or the reverse) fails the build rather than silently changing
/// the bound by one term.
const _: () = assert!(
    RESULT_POINT_SLOT_BYTES.len() as u64 == LEAF_COUNTERS.result_points,
    "every result-point counter needs exactly one slot width"
);

/// The variants fan-out counter shares the group-byte cap's value, which
/// is what makes it a second counter against ONE ceiling rather than a
/// ceiling of its own — the premise the `group_bytes: 2` row rests on.
const _: () = assert!(
    super::variants::MAX_VARIANT_FANOUT_STATE_BYTES <= MAX_CLIENT_AGG_GROUP_BYTES,
    "the variants fan-out counter must not exceed the group-byte ceiling it composes with"
);

/// **The widest query-lifetime retained bytes ONE metric leaf's shipped
/// ceilings prove** (issue #260) — the honest total, not any single cap.
///
/// Derived, never written down: one term per [`AggCaps`] axis, each
/// `multiplicity × cap` with the fixed-width slot terms priced through
/// this module's own [`alloc_block_bytes`] 2× allocator-rounding model,
/// exactly as the charge sites price the allocations they gate.
///
/// ```text
/// group bytes      2 × 268,435,456                        =   536,870,912
/// window retention alloc(4e6 × WinSample = 32)            =   256,000,000
/// quantile values  alloc(4e6 × f64 = 8)                   =    64,000,000
/// counter values   alloc(4e6 × (i64, f64) = 16)           =   128,000,000
/// collision members                    (bytes: see below) =             0
/// collision stage  1 × 8,388,608                          =     8,388,608
/// result slots     alloc(12e6×16) + alloc(12e6×24)        =   960,000,000
///                                                           -------------
///                                                           1,953,259,520  (1.819 GiB)
/// ```
///
/// **Scope of the claim.** This is a sum over the ENUMERATED counters —
/// the [`CounterPlurality`] table, which mirrors every [`AggCaps`] axis
/// (compiler-pinned) and whose per-axis multiplicity is census-pinned
/// against production source. A retained structure governed by NO cap is
/// outside it by construction.
///
/// **Known residuals, stated rather than hidden:** accumulated leaf
/// RESULTS across a binary chain are charged by nothing (#257 on the SQL
/// path, #285 for the chain); post-aggregation selection/grouping keys
/// are flagged-not-charged (#241); a streams response accumulates one
/// row's template output per entry across up to the entries limit
/// (#312); and process RSS is still `N ×` this under concurrency, which
/// #245's closure ruled an operational concern rather than a per-query
/// bound.
pub const MAX_LEAF_RETAINED_BYTES: u64 = leaf_retained_bytes();

/// [`MAX_LEAF_RETAINED_BYTES`] plus ONE row's per-row output budgets —
/// the whole per-query retained-byte figure (issue #260).
///
/// Those budgets are per-ROW since #260 moved the template budget's
/// lifetime off the individual render (whose output the caller RETAINS,
/// so a per-render budget bounded one buffer while the number of live
/// buffers was bounded only by the query-text cap). One row is live at a
/// time on the metric path; the per-row OUTPUTS still accumulate into a
/// streams result across up to the entries limit (#312), which this
/// figure does not cover and #260 deliberately did not close.
///
/// **The row term is a TABLE, and the enumeration behind it is a
/// convention** (issue #287 review rounds 1 and 2, finding 2). #260
/// wrote this as `+ MAX_TEMPLATE_RENDER_BYTES` — a free-standing addend
/// with nothing enumerating it — so #287's second per-row ledger was
/// written, reviewed and committed without appearing here. That was a
/// weakness in #260's mechanism and not merely an omission in #287:
/// #260's derivation was complete over the `AggCaps` axes and over
/// NOTHING ELSE, while its published claim covered the whole per-query
/// figure.
///
/// [`RowBudgets`] replaces the addend with a destructured table, which
/// is a real improvement and is NOT a proof. Round 2 was explicit that
/// a third chain of exhaustive matches would not become one, and the
/// honest sentence is preferred to a fourth: see [`RowBudgets`] for the
/// three routes around it that still compile, and for what closing them
/// would actually take.
pub const MAX_QUERY_RETAINED_BYTES: u64 = MAX_LEAF_RETAINED_BYTES + row_retained_bytes();

/// Every PER-ROW output budget, one field each, in
/// [`super::pipeline::RowBudget`] order.
///
/// A struct rather than a sum expression so the total can DESTRUCTURE
/// it: adding a FIELD here without adding its term to
/// [`row_retained_bytes`] is a build failure.
///
/// **What the compiler holds — exactly this and no more:** every field
/// of this struct is a term of [`row_retained_bytes`], and every
/// [`super::pipeline::RowBudget`] variant resolves to one of these
/// fields ([`row_budget_ceiling`] is an exhaustive `match`, so a new
/// variant will not compile until it is given an answer).
///
/// **What it does NOT hold.** Three routes reach a shipped per-row
/// ledger that is absent from the total, and all three compile:
///
/// 1. a new `RowBudget` variant answered with `0` here;
/// 2. a new `RowBudget` variant answered with an EXISTING field;
/// 3. a new ledger that reuses an existing variant, or that reports its
///    breach through some type other than
///    [`super::pipeline::RowBudgetExceeded`] altogether.
///
/// Route 2 is the one a careless author actually takes — reaching for
/// the nearest plausible ceiling rather than inventing a field — and it
/// COMPILES, verified by mutant rather than assumed. So the enumeration
/// of per-row ledgers is a CONVENTION with a compiler-checked back half,
/// exactly as `AggCaps` is a convention ("every per-query retention cap
/// in one place") with a compiler-checked back half. The published
/// figure is sound for the ledgers listed and says nothing about a
/// ledger nobody listed.
///
/// Routes 1 and 2 do leave a LEXICAL trace — a variant with no field of
/// its own — and `tests/logql_row_budget_enumeration.rs` trips on it by
/// counting variants against fields and requiring the `match` arms to
/// answer with distinct fields. That is a tripwire of exactly the kind
/// #260 already uses beside the `AggCaps` censuses, not a proof: it
/// reads source text, and route 3 leaves no text to read.
///
/// **What would make it complete**, recorded so the next attempt does
/// not start from another chain: the enumeration has to come from where
/// a ledger is CONSTRUCTED, not from where its error is named. A
/// per-row ledger would have to be unconstructable except as one
/// generic type whose sole constructor reads its ceiling out of this
/// table — the sealed-leaf shape `template::retained` already uses for
/// render output, applied to the ledgers themselves. That closes routes
/// 1 and 3 by making the alternatives unrepresentable; route 2 is a
/// semantic choice no mechanism can see. It is a cross-cutting refactor
/// of both existing ledgers, including the sealed template module, and
/// is deliberately not attempted inside this issue.
#[derive(Debug)]
pub struct RowBudgets {
    /// `line_format`/`label_format` render output (issues #230/#260).
    pub template_render: u64,
    /// `| json` full-flatten label keys (issue #287).
    pub json_flatten_keys: u64,
}

/// The shipped ceilings, read from the constants the ledgers are
/// actually constructed with.
pub const ROW_BUDGETS: RowBudgets = RowBudgets {
    template_render: super::template::MAX_TEMPLATE_RENDER_BYTES,
    json_flatten_keys: super::pipeline::MAX_JSON_FLATTEN_KEY_BYTES,
};

/// The ceiling a [`super::pipeline::RowBudget`] variant reports, read
/// out of a DESTRUCTURED [`RowBudgets`] through an exhaustive `match`.
///
/// This forces a new variant to be given an ANSWER. It does not force
/// that answer to be a new field — see [`RowBudgets`] for the routes
/// that leaves open.
pub(in crate::logql) const fn row_budget_ceiling(budget: super::pipeline::RowBudget) -> u64 {
    let RowBudgets {
        template_render,
        json_flatten_keys,
    } = ROW_BUDGETS;
    match budget {
        super::pipeline::RowBudget::TemplateRender => template_render,
        super::pipeline::RowBudget::JsonFlattenKeys => json_flatten_keys,
    }
}

/// ONE row's per-row output budgets, summed over a DESTRUCTURED
/// [`RowBudgets`] so a listed field cannot be left out of the total.
///
/// The terms ADD rather than share: a single row can hold a full
/// template-render output and a full `| json` key expansion at the same
/// time (`{…} | json | line_format …` does exactly that), and the two
/// ledgers refuse independently.
///
/// ```text
/// template render   64 MiB =  67,108,864
/// json flatten keys 64 MiB =  67,108,864
///                            -----------
///                             134,217,728
/// ```
const fn row_retained_bytes() -> u64 {
    let RowBudgets {
        template_render,
        json_flatten_keys,
    } = ROW_BUDGETS;
    template_render + json_flatten_keys
}

/// Each listed [`RowBudgets`] field is the ceiling its
/// [`super::pipeline::RowBudget`] variant reports, so the summed table
/// and the enum cannot drift apart for the variants that exist.
const _: () = assert!(
    row_budget_ceiling(super::pipeline::RowBudget::TemplateRender)
        + row_budget_ceiling(super::pipeline::RowBudget::JsonFlattenKeys)
        == row_retained_bytes(),
    "every RowBudget variant's ceiling must be a term of the per-row total"
);

/// [`MAX_LEAF_RETAINED_BYTES`]'s terms — one per [`AggCaps`] axis, in
/// `AggCaps` order. A `const fn` so the total moves with any cap, any
/// multiplicity and any slot width; there is no second place holding the
/// number.
///
/// **Both operand sets are DESTRUCTURED** (review round 2, finding 2),
/// not field-accessed: adding an axis to [`AggCaps`] fails to compile
/// HERE, in the derivation, as well as in the plurality table. Before
/// this, a new axis could pass both tables and both censuses and still
/// be absent from the sum — the enumeration was complete, and the
/// arithmetic over it was not. Reading the caps out of
/// [`AggCaps::DEFAULT`] rather than the `MAX_*` constants also ties the
/// bound to the values the states are actually handed.
const fn leaf_retained_bytes() -> u64 {
    let AggCaps {
        group_bytes: group_bytes_cap,
        retention_points: retention_points_cap,
        quantile_values: quantile_values_cap,
        counter_values: counter_values_cap,
        collision_members: collision_members_cap,
        collision_bytes: collision_bytes_cap,
        result_points: result_points_cap,
    } = AggCaps::DEFAULT;
    let CounterPlurality {
        group_bytes: group_bytes_n,
        retention_points: retention_points_n,
        quantile_values: quantile_values_n,
        counter_values: counter_values_n,
        collision_members: collision_members_n,
        collision_bytes: collision_bytes_n,
        result_points: result_points_n,
    } = LEAF_COUNTERS;

    // Group bytes are already a BYTE cap, so the multiplicity multiplies
    // it directly; no slot model applies.
    let group_bytes = group_bytes_n * group_bytes_cap;

    // Retention is a POINT cap in `WinSample` units.
    let retention =
        retention_points_n * alloc_block_bytes(retention_points_cap * RETENTION_POINT_BYTES);

    // The two instant-path reducers whose state grows with surviving
    // rows, charged INLINE rather than through a gate function — the
    // pair review round 1 found missing from this sum.
    let quantile =
        quantile_values_n * alloc_block_bytes(quantile_values_cap * QUANTILE_VALUE_BYTES);
    let counter = counter_values_n * alloc_block_bytes(counter_values_cap * COUNTER_VALUE_BYTES);

    // The collision member COUNT contributes no bytes of its own — see
    // [`COLLISION_MEMBER_BYTES`]. `saturating_mul` (not `*`) because a
    // literal-zero product is a lint, and because it is this module's
    // idiom everywhere else. The cap is READ so that the axis cannot be
    // silently dropped from the destructure.
    let collision_members = collision_members_n
        .saturating_mul(COLLISION_MEMBER_BYTES)
        .saturating_mul(if collision_members_cap > 0 { 1 } else { 0 });

    // The collision stage is a byte cap on ONE staged group at a time.
    let collision_bytes = collision_bytes_n * collision_bytes_cap;

    // Result points are a SLOT cap, and the two counters reserve slots of
    // different widths — one `alloc_block_bytes` term each. The
    // multiplicity is the array's length (a compile-time assertion above
    // pins the two equal), so a third width is a third term.
    let mut result_points = 0u64;
    let mut i = 0;
    while i < RESULT_POINT_SLOT_BYTES.len() {
        result_points += alloc_block_bytes(result_points_cap * RESULT_POINT_SLOT_BYTES[i]);
        i += 1;
    }
    let _ = result_points_n;

    group_bytes
        + retention
        + quantile
        + counter
        + collision_members
        + collision_bytes
        + result_points
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::cms;
    use crate::logql::labels::render_labels_json_sorted;
    use crate::logql::plan;

    use super::super::agg::InstantSeries;
    use super::super::exec::VectorSample;

    /// Issue #227 review round 7, finding 2: the per-allocation charge is
    /// a PROVABLE upper bound on the block a real allocator retains, not
    /// just on the request size — a 33-byte request can retain a 48/64-
    /// byte block under size-class rounding. Pins [`alloc_block_bytes`]
    /// against an independent worst-of-mainstream retained-block model at
    /// sizes straddling every class boundary up to 1 MiB:
    /// `max(next_pow2(n), align16(n + 8), 32)` — powers of two are the
    /// coarsest mainstream class grid (jemalloc/mimalloc/tcmalloc are all
    /// finer), and `align16(n + 8)` is glibc's inline-header chunk. The
    /// boundary cases (33, 65, 129, ...) FAIL if the 2× rounding is
    /// removed (a request-size charge of `n.max(32)` sits strictly below
    /// the model there) — asserted explicitly so the test is provably
    /// non-vacuous.
    /// Issue #221 AC 17: the `approx_topk` 13-row accounting is a
    /// compile-time constant — the layout sizes, the capacity constants
    /// and the row arithmetic are all pinned, and the total recomputed
    /// through the codebase's own allocator model
    /// ([`alloc_block_bytes`]/[`grown_alloc_bytes`]) must stay within the
    /// documented 7_360_882-byte ceiling. Asserts the COMPUTED ceiling,
    /// never a measured allocation count (the alloc-bound-test lesson: a
    /// process-global counter flakes on stray allocations).
    #[test]
    fn approx_topk_accounting_total_is_a_compile_time_constant() {
        // Layout pins — a struct-layout change invalidates rows 5/7/12.
        assert_eq!(std::mem::size_of::<cms::SeriesKey>(), 16);
        assert_eq!(std::mem::size_of::<(u32, cms::SeriesKey)>(), 24);
        assert_eq!(std::mem::size_of::<InstantSeries>(), 32);
        // Capacity constants (the `with_capacity(R)` reservations).
        assert_eq!(cms::CMS_WIDTH, 27_183);
        assert_eq!(cms::CMS_DEPTH, 7);
        assert_eq!(cms::CMS_MAX_LABELS, 10_000);
        let r = cms::CMS_MAX_LABELS as u64 + 1; // transient post-push size

        // Row 4: the exact `vec![0.0; W*D]` counter grid.
        let grid = u64::from(cms::CMS_DEPTH) * u64::from(cms::CMS_WIDTH) * 8;
        assert_eq!(grid, 1_522_248);
        // Row 6: hashbrown's `with_capacity(R)` table — `R*8/7` rounded
        // to the next power of two buckets, 8 B per u64 slot + 1 ctrl
        // byte per bucket + 16 trailing ctrl bytes.
        let buckets = (r * 8).div_ceil(7).next_power_of_two();
        let observed = buckets * 8 + buckets + 16;
        assert_eq!(observed, 147_472);

        // The 13 rows (1-3 are the zero-allocation in-place/streaming
        // rows), each pinned to the doc-table figure on
        // `approx_topk_instant`.
        let rows: [(u64, u64); 10] = [
            (alloc_block_bytes(grid), 3_044_496),             // 4: CMS grid
            (alloc_block_bytes(24 * r), 480_048),             // 5: retention heap
            (alloc_block_bytes(observed), 294_944),           // 6: observed set
            (alloc_block_bytes(32 * r), 640_064),             // 7: retained output
            (1_024, 1_024),                                   // 8a: groups table
            (grown_alloc_bytes(8 * r), 480_048),              // 8b: member indices
            (alloc_block_bytes(r), 20_002),                   // 9: keep
            (alloc_block_bytes(16 * r), 320_032),             // 10: candidates
            (alloc_block_bytes(16 * r.div_ceil(2)), 160_032), // 11: sort scratch
            (grown_alloc_bytes(32 * r), 1_920_192),           // 12: selection output
        ];
        let mut total = 0u64;
        for (i, (computed, pinned)) in rows.iter().enumerate() {
            assert_eq!(computed, pinned, "accounting row {i} drifted");
            total += computed;
        }
        assert!(
            total <= 7_360_882,
            "approx_topk peak accounting total {total} exceeds the documented ceiling"
        );
    }

    #[test]
    fn alloc_block_bytes_covers_allocator_size_class_rounding_at_class_boundaries() {
        /// The worst retained block any mainstream allocator keeps for an
        /// `n`-byte request (see the test doc).
        fn worst_mainstream_retained(n: u64) -> u64 {
            let pow2_class = n.max(1).next_power_of_two();
            let glibc_chunk = (n + 8).div_ceil(16) * 16;
            pow2_class.max(glibc_chunk).max(32)
        }

        let sizes: &[u64] = &[
            1,
            8,
            16,
            17,
            24,
            31,
            32,
            33,
            47,
            48,
            63,
            64,
            65,
            96,
            127,
            128,
            129,
            192,
            255,
            256,
            257,
            511,
            512,
            513,
            1023,
            1024,
            1025,
            4095,
            4096,
            4097,
            65_535,
            65_536,
            65_537,
            1 << 20,
            (1 << 20) + 1,
        ];
        for &n in sizes {
            let charge = alloc_block_bytes(n);
            let worst = worst_mainstream_retained(n);
            assert!(
                charge >= worst,
                "alloc_block_bytes({n}) = {charge} under-counts a retained \
                 block of {worst}"
            );
            // The realloc peak: the grown buffer's request is ≤ 2n and the
            // old ≤ n-byte buffer is still mapped — BOTH size-class-rounded.
            let grown = grown_alloc_bytes(n);
            let peak = worst_mainstream_retained(2 * n) + worst;
            assert!(
                grown >= peak,
                "grown_alloc_bytes({n}) = {grown} under-counts the realloc \
                 peak of {peak}"
            );
        }
        // Non-vacuous: at the class-boundary sizes the pre-round-7
        // request-size charge (`n.max(32)`) sits BELOW the retained block,
        // so removing the rounding turns the asserts above red.
        for &n in &[33u64, 65, 129, 257, 1025, 4097] {
            assert!(
                worst_mainstream_retained(n) > n.max(MIN_ALLOC_BYTES),
                "size {n} must straddle a class boundary for this test to \
                 catch a removed rounding"
            );
        }
    }

    /// Review round 5, finding 1: the charge sizes the rendered JSON by
    /// walking the SAME escape arms the renderer uses, so the two cannot
    /// drift. Covers every arm: the two-byte escapes, the `\u00xx` six-byte
    /// C0 expansion (the one the round-4 charge counted as ONE byte),
    /// verbatim ASCII, multi-byte UTF-8, empty strings and the empty set.
    #[test]
    fn rendered_labels_json_len_matches_the_renderer_byte_for_byte() {
        let cases: Vec<Vec<(Cow<'_, str>, Cow<'_, str>)>> = vec![
            vec![],
            vec![(Cow::Borrowed(""), Cow::Borrowed(""))],
            vec![(Cow::Borrowed("app"), Cow::Borrowed("checkout"))],
            // Every short escape.
            vec![(Cow::Borrowed("q"), Cow::Borrowed("\"\\\n\r\t\u{08}\u{0C}"))],
            // Every C0 byte that has no short escape ⇒ six bytes each.
            vec![(
                Cow::Borrowed("c0"),
                Cow::Owned((0u8..0x20).map(|b| b as char).collect::<String>()),
            )],
            // Multi-byte UTF-8 is copied verbatim at its own width.
            vec![
                (Cow::Borrowed("é"), Cow::Borrowed("日本語")),
                (Cow::Borrowed("emoji"), Cow::Borrowed("🚀🚀")),
            ],
            // A dense mixture across several pairs (comma punctuation).
            vec![
                (Cow::Borrowed("a"), Cow::Borrowed("1")),
                (Cow::Borrowed("b"), Cow::Owned("\u{1}x\"y".repeat(7))),
                (Cow::Borrowed("c"), Cow::Borrowed("plain")),
            ],
        ];
        for pairs in &cases {
            assert_eq!(
                rendered_labels_json_len(pairs),
                render_labels_json_sorted(pairs).len() as u64,
                "sized length must equal the rendered length for {pairs:?}"
            );
        }
    }

    /// AC10: the retention cap's over-cap error is `MetricRetention` and the
    /// collision-group cap's is `TsCollisionGroup` — two DISTINCT named 422
    /// reasons, never conflated.
    #[test]
    fn sliding_over_cap_errors_are_distinct_named_reasons() {
        let retention = ReadError::QueryTooBroad(TooBroadReason::MetricRetention {
            count: MAX_RETAINED_WINDOW_POINTS + 1,
            cap: MAX_RETAINED_WINDOW_POINTS,
        })
        .to_string();
        let collision = ReadError::QueryTooBroad(TooBroadReason::TsCollisionGroup {
            count: MAX_TS_COLLISION_GROUP + 1,
            cap: MAX_TS_COLLISION_GROUP,
            bytes: 1,
            bytes_cap: MAX_TS_COLLISION_GROUP_BYTES,
        })
        .to_string();
        assert!(retention.contains("concurrent sample"), "{retention}");
        assert!(collision.contains("same-nanosecond"), "{collision}");
        assert_ne!(retention, collision);
    }

    // -----------------------------------------------------------------
    // Issue #236 — the result-series cap.
    // -----------------------------------------------------------------

    fn n_vector(n: usize) -> QueryResult {
        QueryResult::Vector(
            (0..n)
                .map(|i| VectorSample {
                    labels: vec![("id".to_string(), i.to_string())],
                    value: i as f64,
                })
                .collect(),
        )
    }

    fn n_matrix(n: usize) -> QueryResult {
        QueryResult::Matrix(
            (0..n)
                .map(|i| MatrixSeries {
                    labels: vec![("id".to_string(), i.to_string())],
                    points: vec![(0, i as f64), (1, i as f64)],
                })
                .collect(),
        )
    }

    /// AC 2 — `ensure_result_series` counts TOP-LEVEL SERIES and uses the
    /// reference's own `> cap` test, so exactly `MAX_QUERY_SERIES` is
    /// served and `cap + 1` is refused. Both vector and matrix shapes,
    /// plus the histogram twins and the three pass-through variants.
    #[test]
    fn ensure_result_series_admits_exactly_the_cap_and_refuses_one_more() {
        let cap = MAX_QUERY_SERIES as usize;
        assert_eq!(cap, 500);

        for at in [n_vector(cap), n_matrix(cap)] {
            ensure_result_series(&at).expect("exactly the cap must be served");
        }
        for over in [n_vector(cap + 1), n_matrix(cap + 1)] {
            match ensure_result_series(&over) {
                Err(ReadError::QueryTooBroad(TooBroadReason::MetricSeries { cap: got })) => {
                    assert_eq!(got, MAX_QUERY_SERIES);
                }
                other => panic!("expected MetricSeries, got {other:?}"),
            }
        }

        // A matrix with FEW series but MANY points is not a breach — the
        // reference counts distinct series, never points.
        let deep = QueryResult::Matrix(vec![MatrixSeries {
            labels: vec![],
            points: (0..100_000).map(|k| (k, k as f64)).collect(),
        }]);
        ensure_result_series(&deep).expect("point count is not the series axis");

        // Non-metric shapes pass: log streams are bounded by the entries
        // limit instead, and scalar/string carry no series at all.
        for other in [
            QueryResult::Streams {
                items: Vec::new(),
                partial: false,
            },
            QueryResult::Scalar(1.0),
            QueryResult::String("x".to_string()),
        ] {
            ensure_result_series(&other).expect("non-metric shapes carry no series axis");
        }
    }

    /// AC 11's static companion, as an executable check rather than a
    /// review-time grep: `MAX_QUERY_SERIES` is read by
    /// `ensure_result_series` and by NOTHING else in production source.
    ///
    /// This is the property the plan calls normative — a constant that is
    /// *currently* read once and one that *can only* be read once are
    /// different guarantees, and the second is what stops the next person
    /// reintroducing a mid-scan group cap.
    ///
    /// **Scope, stated because an unscoped conclusion from a scoped
    /// census is worthless:** every `.rs` file **directly in**
    /// `crates/pulsus-read/src/logql/` — the flat level only. The walk is
    /// NON-RECURSIVE, so subdirectory modules such as `template/` are
    /// **not** scanned; the flat-file and subdirectory sets are both
    /// pinned exactly by
    /// `the_region_scan_covers_every_production_file_and_hides_no_subtree`,
    /// and making the walks recursive is #302. Truncated at each file's
    /// column-0 `#[cfg(test)]` marker — i.e. flat-level PRODUCTION
    /// source. Test code is deliberately
    /// out of scope: this very test reads the constant, and so does
    /// `ensure_result_series_admits_exactly_the_cap_and_refuses_one_more`.
    /// The counted unit is SOURCE LINES mentioning the identifier outside
    /// a comment or its own definition.
    #[test]
    fn max_query_series_is_read_in_exactly_one_place() {
        let sources = census_sources();

        let mut reads: Vec<String> = Vec::new();
        for (name, src) in sources {
            for (i, line) in production(src).lines().enumerate() {
                if !line.contains("MAX_QUERY_SERIES") {
                    continue;
                }
                let t = line.trim();
                // The definition and doc/line comments are not reads.
                if t.starts_with("///") || t.starts_with("//") || t.starts_with("pub const") {
                    continue;
                }
                reads.push(format!("{name}:{}: {t}", i + 1));
            }
        }
        // Exactly two production lines mention it, both inside
        // `ensure_result_series`: the `> cap` test and the error payload.
        assert_eq!(
            reads.len(),
            2,
            "MAX_QUERY_SERIES must be read only by ensure_result_series; found {reads:#?}"
        );
        for r in &reads {
            assert!(r.starts_with("charge.rs:"), "unexpected reader: {r}");
        }
        // ...and the deleted mid-scan group cap must not return, under
        // its own name, anywhere in the tree (tests included). The
        // needles are assembled at runtime so this test's OWN source does
        // not contain them — a literal here would match itself and the
        // assertion would fail for the wrong reason.
        // R5: the corrected scope sentence is ASSERTED, not merely
        // written — the old claim covered a tree the non-recursive walk
        // never reads. Assembled at run time so this file does not match
        // itself, the pattern already used for the deleted-cap needles.
        let stale_scope = format!("the whole tree in which the symbol {} nameable", "is");
        assert!(
            !include_str!("charge.rs").contains(&stale_scope),
            "the census still claims a scope its non-recursive walk does not have"
        );
        let deleted_cap = format!("MAX_CLIENT_AGG{}SERIES", '_');
        let deleted_field = format!("caps{}series", '.');
        for (name, src) in sources {
            assert!(
                !src.contains(&deleted_cap),
                "{name} still references the deleted mid-scan group cap"
            );
            assert!(
                !src.contains(&deleted_field),
                "{name} still reads a per-state series cap"
            );
        }
    }

    /// AC 30 — the query-text premise #236's derivation rests on is
    /// ENFORCED by this change, not assumed by it. #279 shipped the cap as
    /// an EXCLUSIVE maximum (`limits.rs:40` rejects `len >= cap`), so the
    /// boundary is pinned from BOTH sides: `cap - 1` accepted, `cap` and
    /// `cap + 1` rejected. Plan v14 phrases it as `cap + 1` only; asserting
    /// the accepted side too is what makes an off-by-one visible.
    #[test]
    fn the_query_text_cap_exists_is_finite_and_rejects_at_the_boundary() {
        let cap = pulsus_logql::MAX_QUERY_BYTES;
        assert!(cap > 0 && cap < usize::MAX, "the cap must be finite");
        assert_eq!(cap, 131_072);

        // A selector whose label VALUE is padded to hit an exact byte
        // length — valid LogQL at every length, so the only thing under
        // test is the admission check.
        let pad = |total: usize| {
            let (head, tail) = (r#"{a=""#, r#""}"#);
            format!(
                "{head}{}{tail}",
                "b".repeat(total - head.len() - tail.len())
            )
        };

        let ok = pad(cap - 1);
        assert_eq!(ok.len(), cap - 1);
        pulsus_logql::parse(&ok).expect("cap - 1 bytes must be accepted");

        for len in [cap, cap + 1] {
            let too_long = pad(len);
            assert_eq!(too_long.len(), len);
            match pulsus_logql::parse(&too_long) {
                Err(pulsus_logql::LogQlError::QueryTooLong {
                    len: got, cap: c, ..
                }) => {
                    assert_eq!((got, c), (len, cap));
                }
                other => panic!("{len} bytes must be rejected, got {other:?}"),
            }
        }
    }

    /// AC 30's second half — the feasible region's `aggs.len()` operand is
    /// `min(MAX_DEPTH, Q/4)`, read from BOTH constants rather than a
    /// literal, so neither can drift without this failing.
    ///
    /// `pulsus_logql::MAX_DEPTH` is `pub(crate)`, so the depth is read
    /// back from the typed error the parser actually raises rather than
    /// by widening another crate's API — which also pins the value the
    /// guard ENFORCES, not merely the one it declares.
    #[test]
    fn the_aggregation_depth_operand_reads_both_constants() {
        let q = pulsus_logql::MAX_QUERY_BYTES as u64;

        let too_deep = format!(
            "{}{}{}",
            "sum(".repeat(200),
            r#"count_over_time({a="b"}[1m])"#,
            ")".repeat(200)
        );
        assert!(too_deep.len() < pulsus_logql::MAX_QUERY_BYTES);
        let depth = match pulsus_logql::parse(&too_deep) {
            Err(pulsus_logql::LogQlError::RecursionLimitExceeded { limit, .. }) => limit as u64,
            other => panic!("expected RecursionLimitExceeded, got {other:?}"),
        };
        assert_eq!(depth, 64);

        // Every nesting level costs at least `sum(` — four bytes of text.
        let operand = depth.min(q / 4);
        assert_eq!(operand, depth, "at Q = {q} the parser depth is binding");
    }

    /// B5 / AC 9, re-derived by issue #236 (AC 35) — `AggCaps::DEFAULT` is
    /// the SIX remaining constants verbatim (`series` is deleted: it was
    /// the mid-scan group cap, and #236 moved the 500 to the final result
    /// as `MAX_QUERY_SERIES`); `divided` keeps the per-field sum ≤ the
    /// single-query bound with every field ≥ 1 for all admissible `n`; and
    /// the backstop is still DERIVED, now landing on
    /// `MAX_TS_COLLISION_GROUP == 10_000` instead of 500 — a strictly
    /// PERMISSIVE re-derivation (the reference is unbounded there).
    #[test]
    fn agg_caps_default_is_the_constants_and_divides_soundly() {
        let d = AggCaps::DEFAULT;
        assert_eq!(d.group_bytes, MAX_CLIENT_AGG_GROUP_BYTES);
        assert_eq!(d.retention_points, MAX_RETAINED_WINDOW_POINTS);
        assert_eq!(d.quantile_values, MAX_QUANTILE_VALUES);
        assert_eq!(d.counter_values, MAX_COUNTER_VALUES);
        assert_eq!(d.collision_members, MAX_TS_COLLISION_GROUP);
        assert_eq!(d.collision_bytes, MAX_TS_COLLISION_GROUP_BYTES);
        assert_eq!(d.result_points, MAX_METRIC_RESULT_POINTS);
        assert_eq!(d.divided(1), d, "divided(1) must be byte-identical");
        // The re-derivation, pinned to the SYMBOL and to the VALUE so a
        // future cap change cannot silently move the backstop.
        assert_eq!(d.min_field(), MAX_TS_COLLISION_GROUP);
        assert_eq!(d.min_field(), 10_000);
        assert_eq!(plan::MAX_VARIANT_SUB_STATES, d.min_field());
        for n in 1..=plan::MAX_VARIANT_SUB_STATES {
            let v = d.divided(n);
            for (field, whole) in [
                (v.group_bytes, d.group_bytes),
                (v.retention_points, d.retention_points),
                (v.quantile_values, d.quantile_values),
                (v.counter_values, d.counter_values),
                (v.collision_members, d.collision_members),
                (v.collision_bytes, d.collision_bytes),
                (v.result_points, d.result_points),
            ] {
                assert!(field >= 1, "divided({n}) floored a cap to 0");
                assert!(field * n <= whole, "divided({n}) sum exceeds the whole");
            }
        }
        // B12 — the collision staging caps are divided like every other
        // field (the fourth N-multiplied allocation, #221 Δ3.3).
        assert_eq!(
            d.divided(2).collision_bytes,
            MAX_TS_COLLISION_GROUP_BYTES / 2
        );
        assert_eq!(d.divided(2).collision_members, MAX_TS_COLLISION_GROUP / 2);
        // Issue #236 §4: the result-point cap divides like every other
        // field, so a `variants(...)` query's sub-states SUM to the
        // single-query bound rather than each getting the whole of it.
        assert_eq!(d.divided(2).result_points, MAX_METRIC_RESULT_POINTS / 2);

        // The hand-written field lists above are the census's weak point:
        // a NEW cap added to `AggCaps` would be divided by `divided` and
        // ignored here. Destructuring is what makes that a build failure
        // — add a field and this stops compiling until it is listed.
        let AggCaps {
            group_bytes: _,
            retention_points: _,
            quantile_values: _,
            counter_values: _,
            collision_members: _,
            collision_bytes: _,
            result_points: _,
        } = d;
    }

    // -----------------------------------------------------------------
    // Issue #260 — the composed bound.
    // -----------------------------------------------------------------

    /// AC 2 — the total is its TERMS, not a written-down number. Both
    /// constants are pinned to their decimal values (so a change is
    /// visible in the diff and in the docs that quote them) AND
    /// recomputed from the caps, the multiplicities and the slot widths
    /// (so raising any cap, widening any slot or changing a listed
    /// multiplicity MOVES the number rather than leaving the two out of
    /// step).
    ///
    /// What it does NOT catch, said plainly: a counter that was never
    /// listed. `the_plurality_table_mirrors_every_agg_caps_axis` and
    /// `every_cap_read_is_enumerated` are what close that.
    #[test]
    fn the_composed_bound_equals_its_terms() {
        // The slot widths the derivation prices, pinned explicitly: a
        // struct that widens moves the bound, and a bound that did not
        // move would be wrong.
        assert_eq!(RETENTION_POINT_BYTES, 32);
        assert_eq!(QUANTILE_VALUE_BYTES, 8);
        assert_eq!(COUNTER_VALUE_BYTES, 16);
        assert_eq!(size_of::<VectorAccum>(), 24);
        assert_eq!(size_of::<KSel>(), 24);
        assert_eq!(RESULT_POINT_SLOT_BYTES, [16, 24]);

        // Every term, recomputed here from the CAPS and the declared
        // multiplicity — independently of `leaf_retained_bytes`, and one
        // per `AggCaps` axis so a missing term is a missing line here.
        let group_bytes = LEAF_COUNTERS.group_bytes * MAX_CLIENT_AGG_GROUP_BYTES;
        let retention = LEAF_COUNTERS.retention_points
            * alloc_block_bytes(MAX_RETAINED_WINDOW_POINTS * RETENTION_POINT_BYTES);
        let quantile = LEAF_COUNTERS.quantile_values
            * alloc_block_bytes(MAX_QUANTILE_VALUES * QUANTILE_VALUE_BYTES);
        let counter = LEAF_COUNTERS.counter_values
            * alloc_block_bytes(MAX_COUNTER_VALUES * COUNTER_VALUE_BYTES);
        // The count cap's members are priced by `collision_bytes` in the
        // same guard, so its own term is zero BY DERIVATION, not by
        // omission.
        let collision_members = LEAF_COUNTERS
            .collision_members
            .saturating_mul(COLLISION_MEMBER_BYTES);
        let collision_bytes = LEAF_COUNTERS.collision_bytes * MAX_TS_COLLISION_GROUP_BYTES;
        let result_points: u64 = RESULT_POINT_SLOT_BYTES
            .iter()
            .map(|w| alloc_block_bytes(MAX_METRIC_RESULT_POINTS * w))
            .sum();
        assert_eq!(group_bytes, 536_870_912);
        assert_eq!(retention, 256_000_000);
        assert_eq!(quantile, 64_000_000);
        assert_eq!(counter, 128_000_000);
        assert_eq!(collision_members, 0);
        assert_eq!(collision_bytes, 8_388_608);
        assert_eq!(result_points, 960_000_000);
        assert_eq!(
            MAX_LEAF_RETAINED_BYTES,
            group_bytes
                + retention
                + quantile
                + counter
                + collision_members
                + collision_bytes
                + result_points,
            "the leaf bound must equal the sum of its terms"
        );
        assert_eq!(MAX_LEAF_RETAINED_BYTES, 1_953_259_520);

        // The query bound adds exactly ONE row's per-row output budgets
        // — BOTH of them (issue #287 review round 1, finding 2: #260's
        // free-standing `+ template` addend lost the json key ledger).
        assert_eq!(
            MAX_QUERY_RETAINED_BYTES,
            MAX_LEAF_RETAINED_BYTES
                + crate::logql::template::MAX_TEMPLATE_RENDER_BYTES
                + crate::logql::pipeline::MAX_JSON_FLATTEN_KEY_BYTES
        );
        assert_eq!(MAX_QUERY_RETAINED_BYTES, 2_087_477_248);

        // The variants leaf is strictly smaller, so the sum above really
        // is an upper bound for it too: its fan-out counter shares the
        // group-byte ceiling, its sub-states divide every cap, and it
        // never attaches a fold.
        let variants_leaf = crate::logql::variants::MAX_VARIANT_FANOUT_STATE_BYTES
            + MAX_CLIENT_AGG_GROUP_BYTES
            + alloc_block_bytes(MAX_METRIC_RESULT_POINTS * RESULT_POINT_SLOT_BYTES[0])
            + alloc_block_bytes(MAX_RETAINED_WINDOW_POINTS * RETENTION_POINT_BYTES)
            + alloc_block_bytes(MAX_QUANTILE_VALUES * QUANTILE_VALUE_BYTES)
            + alloc_block_bytes(MAX_COUNTER_VALUES * COUNTER_VALUE_BYTES)
            + MAX_TS_COLLISION_GROUP_BYTES;
        assert_eq!(variants_leaf, 1_377_259_520);
        assert!(
            variants_leaf < MAX_LEAF_RETAINED_BYTES,
            "the variants leaf must be inside the stated bound"
        );
    }

    /// Review round 1, finding 2 — the structural half of the
    /// completeness claim, and the one the first version lacked.
    ///
    /// `AggCaps` is, by its own contract (issue #221), EVERY per-query
    /// retention cap in one place. Destructuring both it and
    /// [`CounterPlurality`] with explicit field lists means a cap added
    /// to `AggCaps` stops this file compiling until it is given a
    /// multiplicity and a term — the compiler, not a reviewer, is what
    /// keeps the enumeration complete over the cap axes.
    #[test]
    fn the_plurality_table_mirrors_every_agg_caps_axis() {
        let AggCaps {
            group_bytes: cap_group_bytes,
            retention_points: cap_retention_points,
            quantile_values: cap_quantile_values,
            counter_values: cap_counter_values,
            collision_members: cap_collision_members,
            collision_bytes: cap_collision_bytes,
            result_points: cap_result_points,
        } = AggCaps::DEFAULT;
        let CounterPlurality {
            group_bytes,
            retention_points,
            quantile_values,
            counter_values,
            collision_members,
            collision_bytes,
            result_points,
        } = LEAF_COUNTERS;

        // Every axis carries at least one live counter — a zero would
        // mean an unpriced cap, which is the omission this test exists
        // to make impossible.
        for (axis, cap, plurality) in [
            ("group_bytes", cap_group_bytes, group_bytes),
            ("retention_points", cap_retention_points, retention_points),
            ("quantile_values", cap_quantile_values, quantile_values),
            ("counter_values", cap_counter_values, counter_values),
            (
                "collision_members",
                cap_collision_members,
                collision_members,
            ),
            ("collision_bytes", cap_collision_bytes, collision_bytes),
            ("result_points", cap_result_points, result_points),
        ] {
            assert!(cap > 0, "{axis}: a zero cap is not a cap");
            assert!(
                plurality >= 1,
                "{axis}: every AggCaps axis needs at least one enumerated counter"
            );
        }
    }

    /// Issue #287 review round 1, finding 2 — the same structural claim
    /// one layer up, on the axis #260 left as a free-standing addend.
    ///
    /// [`RowBudgets`] is destructured here, in [`row_budget_ceiling`]
    /// (whose `match` over [`super::super::pipeline::RowBudget`] is
    /// exhaustive) and in [`row_retained_bytes`], so a per-row ledger
    /// cannot reach the 422 surface without also reaching the total.
    /// This test adds what the compiler cannot say: that every field is
    /// a REAL ceiling and that the total is their sum, so a field
    /// silently zeroed — the arithmetic way to drop a term while keeping
    /// the destructure — still fails.
    #[test]
    fn every_per_row_budget_is_a_term_of_the_query_bound() {
        let RowBudgets {
            template_render,
            json_flatten_keys,
        } = ROW_BUDGETS;
        for (name, ceiling) in [
            ("template_render", template_render),
            ("json_flatten_keys", json_flatten_keys),
        ] {
            assert!(ceiling > 0, "{name}: a zero ceiling is not a ceiling");
        }
        assert_eq!(
            template_render,
            crate::logql::template::MAX_TEMPLATE_RENDER_BYTES
        );
        assert_eq!(
            json_flatten_keys,
            crate::logql::pipeline::MAX_JSON_FLATTEN_KEY_BYTES
        );
        assert_eq!(
            MAX_QUERY_RETAINED_BYTES - MAX_LEAF_RETAINED_BYTES,
            template_render + json_flatten_keys,
            "the query bound's row term must be the sum of every per-row budget"
        );
        assert_eq!(template_render + json_flatten_keys, 134_217_728);
    }

    /// AC 8 — the published bound is the SHIPPED one. `docs/features.md`
    /// quotes both constants, and a number a doc quotes is a number that
    /// drifts, so it is read back from the committed file and compared
    /// against the derivation.
    ///
    /// Also pins the two documentation corrections issue #260 made in
    /// passing: the variants fan-out budget is 256 MiB (it was documented
    /// as 64 MiB, which source never said), and the ledger's
    /// `template-output-budget` entry describes a per-ROW lifetime.
    #[test]
    fn the_docs_quote_the_shipped_bound() {
        /// `1828368384` → `"1,828,368,384"`, the spelling the docs use.
        fn grouped(mut n: u64) -> String {
            let mut parts = Vec::new();
            while n >= 1_000 {
                parts.push(format!("{:03}", n % 1_000));
                n /= 1_000;
            }
            parts.push(n.to_string());
            parts.reverse();
            parts.join(",")
        }

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let features =
            std::fs::read_to_string(root.join("docs/features.md")).expect("features.md readable");
        for c in [MAX_LEAF_RETAINED_BYTES, MAX_QUERY_RETAINED_BYTES] {
            let spelled = grouped(c);
            assert!(
                features.contains(&spelled),
                "docs/features.md does not quote {spelled} — the published bound has drifted \
                 from the derived one"
            );
        }
        // The stale variants-fan-out figure, corrected here.
        assert!(
            !features.contains("one 64 MiB budget before each allocation"),
            "docs/features.md still calls the variants fan-out budget 64 MiB; source says \
             {}",
            super::super::variants::MAX_VARIANT_FANOUT_STATE_BYTES
        );

        let ledger =
            std::fs::read_to_string(root.join("docs/benchmarks/logs-differential-ledger.md"))
                .expect("ledger readable");
        assert!(ledger.contains("cumulative per-ROW budget"));
        assert!(
            !ledger.contains("cumulative per-render budget"),
            "the ledger still describes the pre-#260 per-render lifetime"
        );
    }

    /// AC 1 — the multiplicity table is not a claim, it is a CENSUS: the
    /// first argument of every production call to the four charge gates
    /// is extracted from source and matched against a pinned list, so a
    /// counter added anywhere in the flat `src/logql/` region either
    /// appears here or fails the test by name.
    ///
    /// **Scope**, stated because an unscoped conclusion from a scoped
    /// census is worthless: the same flat, non-recursive `src/logql/*.rs`
    /// PRODUCTION region `max_query_series_is_read_in_exactly_one_place`
    /// walks — reusing its `SOURCES` list, its directory guard and its
    /// `#[cfg(test)]` truncation. Subdirectories (`template/`, `testkit/`)
    /// hold no charge site; the directory guard is what stops the region
    /// shrinking silently.
    ///
    /// Fails CLOSED: an occurrence whose first argument is not in the
    /// pinned list is a failure naming the file and the token, not a
    /// silent omission.
    #[test]
    fn every_charge_counter_is_enumerated() {
        /// `(file, gate, first argument, occurrences)` — one row per
        /// distinct counter expression. The counters this sums to are
        /// [`LEAF_COUNTERS`]; see its table for which are simultaneously
        /// live.
        const EXPECTED: &[(&str, &str, &str, usize)] = &[
            // `ClientAggState::group_bytes` (instant, ×2: the
            // label-group and fingerprint-group arms) and
            // `RangeSlideState::group_bytes` (range, ×2: the
            // non-mutating `series_out` and mutating `groups` arms).
            (
                "client_agg.rs",
                "charge_group_bytes",
                "&mut self.group_bytes",
                4,
            ),
            // `ReduceFold::group_bytes` XOR `SelectFold::group_bytes`,
            // reached through `charged_group_key`'s `&mut u64` parameter.
            ("fold.rs", "charge_group_bytes", "charged", 1),
            // `RangeSlideState::result_points` (×3: the two slider
            // creations and the absent-series emit).
            (
                "client_agg.rs",
                "charge_result_points",
                "&mut self.result_points",
                3,
            ),
            // `ReduceFold::slots` XOR `SelectFold::slots`, through the
            // `let charged = &mut self.slots` local (×3: the two dense
            // vectors and `SelectFold`'s per-candidate slot).
            ("fold.rs", "charge_result_points", "charged", 3),
            // `RangeSlideState::retained`, through `&mut u64` parameters
            // and the `let retained = &mut self.retained` local.
            ("client_agg.rs", "charge_retention", "retained", 4),
            // `VariantsAggState::charged` / `VariantArena::charged`.
            ("variants.rs", "charge_fanout_bytes", "&mut charged", 3),
            // The plan-time continuation of the SAME fan-out counter.
            ("plan.rs", "charge_fanout_bytes", "charged", 1),
            ("plan.rs", "charge_fanout_bytes", "&mut charged", 1),
        ];
        const GATES: &[&str] = &[
            "charge_group_bytes",
            "charge_result_points",
            "charge_retention",
            "charge_fanout_bytes",
        ];

        let sources = census_sources();
        let mut found: Vec<(&str, &str, String)> = Vec::new();
        for (name, src) in sources {
            // Whitespace-normalised so a multi-line call's first argument
            // is on the same "line" as its gate — the call sites are
            // rustfmt-wrapped, so a line-oriented scan would see the gate
            // and the argument separately.
            let compact = compact_ws(&code_only(production(src)));
            for gate in GATES {
                let needle = format!("{gate}(");
                let mut at = 0usize;
                while let Some(i) = compact[at..].find(&needle) {
                    let start = at + i;
                    at = start + needle.len();
                    // `discharge_<gate>` ends in an identifier character,
                    // so the preceding byte separates a call from a
                    // longer name that merely contains one.
                    let prev = compact[..start].chars().next_back().unwrap_or(' ');
                    if prev.is_alphanumeric() || prev == '_' {
                        continue;
                    }
                    // The definition is not a call.
                    if compact[..start].ends_with("fn ") {
                        continue;
                    }
                    found.push((name, gate, first_argument(&compact[at..])));
                }
            }
        }

        // Every occurrence must be a KNOWN counter — an unknown token is
        // a new counter, which changes the bound.
        for (file, gate, arg) in &found {
            assert!(
                EXPECTED
                    .iter()
                    .any(|(f, g, a, _)| f == file && g == gate && a == arg),
                "unenumerated {gate} counter in {file}: first argument {arg:?} — a new \
                 counter against a shipped cap changes MAX_LEAF_RETAINED_BYTES; add it to \
                 `LEAF_COUNTERS` and to this census"
            );
        }
        // ...and every pinned counter must still exist, at its count.
        for (file, gate, arg, count) in EXPECTED {
            let seen = found
                .iter()
                .filter(|(f, g, a)| f == file && g == gate && a == arg)
                .count();
            assert_eq!(
                seen, *count,
                "{file}: {gate}({arg}) occurs {seen}×, expected {count}×"
            );
        }
        assert_eq!(
            found.len(),
            EXPECTED.iter().map(|(_, _, _, n)| n).sum::<usize>(),
            "the census and the pinned table disagree on the total: {found:#?}"
        );

        // The INDIRECTIONS the census cannot see through (the fold passes
        // a parameter and a local, not a field), pinned by count so a
        // third fold counter cannot hide behind the same token.
        let fold = compact_ws(&code_only(production(include_str!("fold.rs"))));
        assert_eq!(
            fold.matches("&mut self.group_bytes").count(),
            LEAF_COUNTERS.group_bytes as usize,
            "fold.rs must route exactly the enumerated group-byte counters into \
             `charged_group_key`"
        );
        assert_eq!(
            fold.matches("&mut self.slots").count(),
            LEAF_COUNTERS.result_points as usize,
            "fold.rs must route exactly the enumerated slot counters into `charge_result_points`"
        );
    }

    /// Review round 1, finding 2 — a source-derived TRIPWIRE over cap
    /// enforcement. Not a proof: it matches literal `caps.<field>`
    /// tokens, so a destructured or aliased read evades it (review round
    /// 2). What is proved by the compiler is that every axis has a
    /// multiplicity and a term — see
    /// `the_plurality_table_mirrors_every_agg_caps_axis` and the
    /// destructure inside [`leaf_retained_bytes`].
    ///
    /// The gate census below can only see counters that pass through one
    /// of the four charge FUNCTIONS. `ClientAggState::quantile_values`
    /// and `counter_values` do not: they compare inline
    /// (`self.x > self.caps.x`), which is exactly how they escaped the
    /// first version of [`LEAF_COUNTERS`].
    ///
    /// This census is derived from the one thing every enforcement has
    /// in common, gated or not: **enforcing a cap means READING one.**
    /// Every production read of an [`AggCaps`] field in the flat
    /// `src/logql/` region is enumerated here, with the counter it
    /// enforces named. A new counter — inline, gated, in a new struct or
    /// a new file — must read a cap, so it lands in this table or fails
    /// the test by name.
    ///
    /// (`caps.name(...)` in `pipeline.rs` is a regex captures binding,
    /// not an `AggCaps` field; the scan matches only the seven field
    /// names, so it is not a special case.)
    #[test]
    fn every_cap_read_is_enumerated() {
        /// `(file, AggCaps field, reads, the counter each read enforces)`.
        const EXPECTED: &[(&str, &str, usize, &str)] = &[
            // 4 reads feed `charge_group_bytes` (2 in `ClientAggState`,
            // 2 in `RangeSlideState`); the 5th hands the cap to the fold
            // at `attach_fold`, which is the SECOND counter.
            (
                "client_agg.rs",
                "group_bytes",
                5,
                "ClientAggState::group_bytes | RangeSlideState::group_bytes | the fold's cap",
            ),
            (
                "client_agg.rs",
                "retention_points",
                2,
                "RangeSlideState::retained",
            ),
            (
                "client_agg.rs",
                "quantile_values",
                2,
                "ClientAggState::quantile_values (INLINE — no gate function)",
            ),
            (
                "client_agg.rs",
                "counter_values",
                2,
                "ClientAggState::counter_values (INLINE — no gate function)",
            ),
            (
                "client_agg.rs",
                "collision_members",
                2,
                "RangeSlideState::coll member count",
            ),
            (
                "client_agg.rs",
                "collision_bytes",
                2,
                "RangeSlideState::coll_bytes",
            ),
            (
                "client_agg.rs",
                "result_points",
                4,
                "RangeSlideState::result_points | the fold's cap",
            ),
        ];

        let fields = [
            "group_bytes",
            "retention_points",
            "quantile_values",
            "counter_values",
            "collision_members",
            "collision_bytes",
            "result_points",
        ];
        // The scan's field list must BE `AggCaps`'s. Destructuring is
        // what makes a new cap a compile error here rather than a field
        // this census silently never looks for.
        let AggCaps {
            group_bytes: _,
            retention_points: _,
            quantile_values: _,
            counter_values: _,
            collision_members: _,
            collision_bytes: _,
            result_points: _,
        } = AggCaps::DEFAULT;
        assert_eq!(fields.len(), 7, "one scanned name per AggCaps field");

        let mut found: Vec<(&str, &str)> = Vec::new();
        for (name, src) in census_sources() {
            let compact = compact_ws(&code_only(production(src)));
            for field in fields {
                let needle = format!("caps.{field}");
                let mut at = 0usize;
                while let Some(i) = compact[at..].find(&needle) {
                    let start = at + i;
                    at = start + needle.len();
                    // `caps.result_points` must not also count as a read
                    // of a longer field name that ends the same way.
                    let next = compact[at..].chars().next().unwrap_or(' ');
                    if next.is_alphanumeric() || next == '_' {
                        continue;
                    }
                    found.push((name, field));
                }
            }
        }

        for (file, field) in &found {
            assert!(
                EXPECTED.iter().any(|(f, x, _, _)| f == file && x == field),
                "unenumerated `caps.{field}` read in {file} — a cap read is a counter, and a \
                 counter changes MAX_LEAF_RETAINED_BYTES; add it to `LEAF_COUNTERS` and to \
                 this census"
            );
        }
        for (file, field, count, enforces) in EXPECTED {
            let seen = found
                .iter()
                .filter(|(f, x)| f == file && x == field)
                .count();
            assert_eq!(
                seen, *count,
                "{file}: `caps.{field}` is read {seen}×, expected {count}× ({enforces})"
            );
        }
        assert_eq!(
            found.len(),
            EXPECTED.iter().map(|(_, _, n, _)| n).sum::<usize>(),
            "the cap-read census and the pinned table disagree: {found:#?}"
        );

        // Every enumerated axis is actually enforced somewhere — an axis
        // with no reads is a cap nothing checks, which the bound would
        // be pricing for no reason.
        for field in fields {
            assert!(
                found.iter().any(|(_, x)| *x == field),
                "`caps.{field}` is never read: the bound prices an axis nothing enforces"
            );
        }
    }

    /// AC 4, lexical half — a DRIFT TRIPWIRE over `attach_fold`'s call
    /// sites, and deliberately not more than that.
    ///
    /// **What it proves:** no new `.attach_fold(` call has appeared in
    /// the flat production region. **What it does not:** a
    /// fully-qualified call, a method alias, a macro expansion or a
    /// future dispatch arrangement is invisible to it (review round 1,
    /// C6). The claim that the variants path takes no fold — the premise
    /// [`LEAF_COUNTERS`]`.group_bytes == 2` rests on — is carried by
    /// `super::super::variants::tests::the_variants_sub_states_take_no_fold`,
    /// which builds a real `VariantsAggState` through the production
    /// constructor and asserts `sub_folded_aggs() == 0`, shows the
    /// outer-aggregation composition that would supply a fold is a 400,
    /// and shows the fold mechanism itself is live on a plain leaf.
    #[test]
    fn the_variants_path_attaches_no_fold() {
        let callers: Vec<(&str, usize)> = census_sources()
            .iter()
            .map(|(name, src)| {
                (
                    *name,
                    compact_ws(&code_only(production(src)))
                        .matches(".attach_fold(")
                        .count(),
                )
            })
            .filter(|(_, n)| *n > 0)
            .collect();
        assert_eq!(
            callers,
            vec![("exec.rs", 1), ("client_agg.rs", 1)],
            "a new `attach_fold` caller may put a fold on the variants path, which would \
             make the group-byte multiplicity 3 — re-run the runtime leg named above before \
             re-pinning this list"
        );
        // Neither caller is in `variants.rs`, and the fan-out driver
        // constructs its sub-states through `MetricAggState` alone.
        let variants = compact_ws(&code_only(production(include_str!("variants.rs"))));
        assert!(!variants.contains("attach_fold"));
    }

    /// The `SOURCES` list plus its directory guard, shared by the censuses
    /// scoped to the flat `src/logql/` production region (issue #260 adds
    /// the second and third readers of it).
    fn census_sources() -> &'static [(&'static str, &'static str)] {
        const SOURCES: &[(&str, &str)] = &[
            ("exec.rs", include_str!("exec.rs")),
            ("error.rs", include_str!("error.rs")),
            ("plan.rs", include_str!("plan.rs")),
            // Issue #272: `MetricNode`'s drop oracle. A `plan_`-prefixed
            // sibling rather than `plan/drop_order.rs`, because a `plan/`
            // directory is swallowed by a common global gitignore rule
            // and the source would never be committed.
            ("plan_drop_order.rs", include_str!("plan_drop_order.rs")),
            // Issue #293: the plan walk's paired stack gate, holding the
            // BODY of the recursive `build_metric_node` this issue deleted,
            // with two substituted child accessors, as its control.
            // Entirely `#[cfg(test)]`, so its production region
            // is empty by construction.
            (
                "plan_recursive_control.rs",
                include_str!("plan_recursive_control.rs"),
            ),
            ("mod.rs", include_str!("mod.rs")),
            ("pipeline.rs", include_str!("pipeline.rs")),
            ("sql.rs", include_str!("sql.rs")),
            ("detected.rs", include_str!("detected.rs")),
            ("cms.rs", include_str!("cms.rs")),
            ("rows.rs", include_str!("rows.rs")),
            ("params.rs", include_str!("params.rs")),
            ("explain.rs", include_str!("explain.rs")),
            ("escape.rs", include_str!("escape.rs")),
            ("ip.rs", include_str!("ip.rs")),
            ("agg.rs", include_str!("agg.rs")),
            ("charge.rs", include_str!("charge.rs")),
            ("client_agg.rs", include_str!("client_agg.rs")),
            ("detected_probe.rs", include_str!("detected_probe.rs")),
            ("fold.rs", include_str!("fold.rs")),
            ("labels.rs", include_str!("labels.rs")),
            ("post_agg.rs", include_str!("post_agg.rs")),
            ("variants.rs", include_str!("variants.rs")),
            ("walkbound.rs", include_str!("walkbound.rs")),
            ("window.rs", include_str!("window.rs")),
        ];
        assert_source_set_matches_the_directory(SOURCES);
        SOURCES
    }

    /// **The file set must not be able to shrink silently.** `include_str!`
    /// needs compile-time literals, so the list is written out — which
    /// means a source file added to `src/logql` would simply not be
    /// searched, and a census would keep passing while covering less. The
    /// list is therefore compared against the DIRECTORY at run time and a
    /// file it does not name is a loud failure naming the file, not a
    /// quiet reduction in scope.
    fn assert_source_set_matches_the_directory(sources: &[(&str, &str)]) {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/logql");
        let mut on_disk: Vec<String> = std::fs::read_dir(&dir)
            .expect("the logql source directory")
            .map(|e| e.expect("dir entry").path())
            .filter(|p| p.extension().is_some_and(|x| x == "rs"))
            .map(|p| {
                p.file_name()
                    .expect("file name")
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        on_disk.sort();
        let mut named: Vec<String> = sources.iter().map(|(n, _)| (*n).to_string()).collect();
        named.sort();
        assert_eq!(
            named, on_disk,
            "the census file set and `src/logql` have diverged — add the new file to \
             `SOURCES` (and to any other census scoped to this region) rather than \
             letting the search quietly cover less than the module"
        );
    }

    /// Everything above the file's `#[cfg(test)]` marker.
    fn production(src: &str) -> &str {
        match src.find("\n#[cfg(test)]") {
            Some(i) => &src[..i],
            None => src,
        }
    }

    /// Production source with every FULL-LINE comment dropped, so a
    /// census cannot match its own prose. (`charge.rs`'s doc comments
    /// name `caps.group_bytes` and the charge gates by hand; without
    /// this the censuses would count their own documentation.) Dropping
    /// whole comment lines also improves the compaction below: a comment
    /// between a call and its first argument no longer separates them.
    fn code_only(src: &str) -> String {
        src.lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Every run of whitespace collapsed to one space, so a
    /// rustfmt-wrapped call and its arguments read as one string.
    fn compact_ws(src: &str) -> String {
        let mut out = String::with_capacity(src.len());
        let mut space = false;
        for c in src.chars() {
            if c.is_whitespace() {
                space = true;
                continue;
            }
            if space && !out.is_empty() {
                out.push(' ');
            }
            space = false;
            out.push(c);
        }
        out
    }

    /// The first argument of a call whose opening `(` has just been
    /// consumed: everything up to the first depth-0 `,` (or `)`).
    fn first_argument(rest: &str) -> String {
        let mut depth = 0i32;
        let mut end = rest.len();
        for (i, c) in rest.char_indices() {
            match c {
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => {
                    if depth == 0 {
                        end = i;
                        break;
                    }
                    depth -= 1;
                }
                ',' if depth == 0 => {
                    end = i;
                    break;
                }
                _ => {}
            }
        }
        rest[..end].trim().to_string()
    }
}
