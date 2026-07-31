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
use super::client_agg::{BucketAcc, MutGroup};
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

/// **The one group-byte gate** (round 6): every query-lifetime group-map
/// insertion is sized by [`group_entry_bytes`], checked against the cap, and
/// accounted HERE — *before* the insertion that retains the entry, so the
/// map never holds a refused group. `saturating_add` cannot mask a breach
/// (saturation only grows the sum).
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

/// **The one result-point gate** (issue #236): every fixed-width point
/// slot a metric evaluation will RETAIN is reserved here, against
/// [`MAX_METRIC_RESULT_POINTS`], *before* the allocation that holds it.
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
        const SOURCES: &[(&str, &str)] = &[
            ("exec.rs", include_str!("exec.rs")),
            ("error.rs", include_str!("error.rs")),
            ("plan.rs", include_str!("plan.rs")),
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
            ("window.rs", include_str!("window.rs")),
        ];

        // **The file set must not be able to shrink silently.**
        // `include_str!` needs compile-time literals, so `SOURCES` is
        // written out — which means a source file added to `src/logql`
        // (the post-aggregation region is scheduled to move into one of
        // its own after #236 merges) would simply not be searched, and
        // this census would keep passing while covering less. That is the
        // gate-weakening shape this issue has hit repeatedly, so it is
        // closed here rather than noted: the list is compared against the
        // DIRECTORY at run time and a file it does not name is a loud
        // failure naming the file, not a quiet reduction in scope.
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
        let mut named: Vec<String> = SOURCES.iter().map(|(n, _)| (*n).to_string()).collect();
        named.sort();
        assert_eq!(
            named, on_disk,
            "the census file set and `src/logql` have diverged — add the new file to \
             `SOURCES` (and to any other census scoped to this region) rather than \
             letting the search quietly cover less than the module"
        );

        /// Everything above the file's `#[cfg(test)]` marker.
        fn production(src: &str) -> &str {
            match src.find("\n#[cfg(test)]") {
                Some(i) => &src[..i],
                None => src,
            }
        }

        let mut reads: Vec<String> = Vec::new();
        for (name, src) in SOURCES {
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
        for (name, src) in SOURCES {
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
}
