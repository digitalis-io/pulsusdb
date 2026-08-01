//! The grid fold — bounded, per-grid-point vector aggregation.
//!
//! [`VectorAggFold`] folds a metric scan into `groups × steps` retained
//! cells whatever the scan's width, splitting into [`ReduceFold`] for the
//! reducing ops and [`SelectFold`] for the selecting ones. The group-key
//! funnel ([`group_key_bytes`], [`GroupKeyCharge`], [`charged_group_key`],
//! [`group_key`]) exists so the fold CHARGES a group key before it
//! allocates one — `charged_group_key` is textually before `group_key`
//! because that ordering is pinned by a test that reads this file.

use super::error::ReadError;
use super::plan::{self};
use pulsus_logql::{Grouping, GroupingKind, VectorAggOp};
use std::collections::HashMap;

use super::agg::{EMPTY_LABEL_SET, LabelSet, VectorAccum, k_of};
use super::charge::{
    FOLD_GROUP_SLOT, alloc_block_bytes, charge_group_bytes, charge_result_points,
    discharge_group_bytes, grown_alloc_bytes, map_entry_bytes,
};
use super::client_agg::ceil_div_i128;
use super::exec::MatrixSeries;
use super::window::clamp_bucket;

/// The bytes [`group_key`] is ABOUT TO allocate for `labels`/`grouping`,
/// computed without allocating them — the fold's charge-before-allocate
/// needs the size before the key exists.
///
/// It mirrors `group_key`'s projection arm for arm and prices the result
/// through the leaf's own vocabulary ([`alloc_block_bytes`] per owned
/// `String`, one exactly-reserved element buffer), which is
/// [`label_set_bytes`] evaluated on a `LabelSet` that has not been built.
/// `group_key_bytes_matches_group_key` pins the two together, so the
/// sizing cannot drift from the thing it sizes.
fn group_key_bytes(labels: &[(String, String)], grouping: Option<&Grouping>) -> u64 {
    let Some(g) = grouping else {
        // `group_key` returns `Vec::new()`, which allocates nothing.
        return 0;
    };
    let mut bytes: u64 = 0;
    let mut pairs: u64 = 0;
    match g.kind {
        GroupingKind::By => {
            for name in &g.labels {
                let value_len = labels
                    .iter()
                    .find(|(k, _)| k == name)
                    .map_or(0, |(_, v)| v.len());
                bytes = bytes
                    .saturating_add(alloc_block_bytes(name.len() as u64))
                    .saturating_add(alloc_block_bytes(value_len as u64));
                pairs += 1;
            }
        }
        GroupingKind::Without => {
            for (k, v) in labels {
                if g.labels.contains(k) {
                    continue;
                }
                bytes = bytes
                    .saturating_add(alloc_block_bytes(k.len() as u64))
                    .saturating_add(alloc_block_bytes(v.len() as u64));
                pairs += 1;
            }
        }
    }
    // `group_key` collects into an exactly-reserved buffer and then
    // `sort()`s it in place; the sort allocates a scratch of at most the
    // same size, which the `grown_alloc_bytes` growth model covers.
    let elems = pairs.saturating_mul(size_of::<(String, String)>() as u64);
    bytes.saturating_add(grown_alloc_bytes(elems))
}

/// **The refund, by construction** — the RAII half of
/// [`charged_group_key`]. The key's bytes are charged BEFORE the key
/// exists, so the charge is only correct once the key is RETAINED; until
/// then it is transient and owed back. Pairing a `discharge_group_bytes`
/// call with each non-retaining exit is what let the points-charge error
/// path leak (issue #236 whole-branch re-review `[low]`), so the refund
/// is `Drop`'s job instead: the charge sticks ONLY through
/// [`GroupKeyCharge::commit`], called immediately after the insertion
/// that retains the key. Every other exit between the charge and that
/// insertion — including ones added later — refunds by construction.
///
/// The guarantee is this `Drop` impl plus the absence of any other route
/// from the fold to `group_key`
/// (`the_fold_charges_before_it_builds_a_group_key`), NOT a behavioural
/// test: on the leaking path `push_series` returns `Err`, the query
/// aborts and no finish post-condition runs, so nothing observable
/// distinguishes a refunded charge from a leaked one and a test asserting
/// the refund could not fail.
#[derive(Debug)]
#[must_use = "the guard refunds the key charge when it drops; hold it until the key is retained"]
struct GroupKeyCharge<'a> {
    charged: &'a mut u64,
    /// The bytes owed back. [`GroupKeyCharge::commit`] zeroes it, which
    /// is how a committed charge becomes a no-op refund.
    bytes: u64,
}

impl GroupKeyCharge<'_> {
    /// The key is RETAINED — the charge belongs to the counter now.
    fn commit(mut self) {
        self.bytes = 0;
    }
}

impl Drop for GroupKeyCharge<'_> {
    fn drop(&mut self) {
        discharge_group_bytes(self.charged, self.bytes);
    }
}

/// **The fold's only route to [`group_key`]** — charge, THEN allocate,
/// in ONE place so the ordering is read once rather than repeated at
/// every call site (issue #236 review round 1 `[high]`).
///
/// Returns the key and a [`GroupKeyCharge`] guard for the bytes charged
/// for it. The counter tracks what is RETAINED, so the charge survives
/// only if the caller RETAINS the key ([`GroupKeyCharge::commit`]); every
/// other exit — the group was already present, or an intervening charge
/// refused — refunds when the guard drops. The bound therefore covers
/// `retained + one transient`, which is what charging before allocating
/// necessarily costs.
///
/// The guard is built IMMEDIATELY after the charge lands and BEFORE
/// `group_key` runs, because a guard cannot refund a charge it does not
/// yet own: `group_key` allocates, so an unwinding panic inside it would
/// otherwise unwind past a charge with nothing holding the refund. Three
/// steps, in this order — charge, guard, build — and no fallible or
/// allocating step between the first two.
///
/// `the_fold_charges_before_it_builds_a_group_key` pins that statement
/// ORDER inside this function: a behavioural test cannot see the
/// inversion (the transient key is built either way), so the ordering is
/// checked where it is written.
fn charged_group_key<'a>(
    labels: &[(String, String)],
    grouping: Option<&Grouping>,
    charged: &'a mut u64,
    cap: u64,
) -> Result<(LabelSet, GroupKeyCharge<'a>), ReadError> {
    let bytes = group_key_bytes(labels, grouping).saturating_add(map_entry_bytes(FOLD_GROUP_SLOT));
    charge_group_bytes(charged, bytes, cap)?;
    let charge = GroupKeyCharge { charged, bytes };
    let key = group_key(labels, grouping);
    Ok((key, charge))
}

pub(in crate::logql) fn group_key(
    labels: &[(String, String)],
    grouping: Option<&Grouping>,
) -> LabelSet {
    let Some(g) = grouping else {
        return Vec::new();
    };
    let mut kv: Vec<(String, String)> = match g.kind {
        GroupingKind::By => {
            let map: HashMap<&str, &str> = labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            g.labels
                .iter()
                .map(|name| {
                    (
                        name.clone(),
                        map.get(name.as_str())
                            .map(|v| v.to_string())
                            .unwrap_or_default(),
                    )
                })
                .collect()
        }
        GroupingKind::Without => labels
            .iter()
            .filter(|(k, _)| !g.labels.contains(k))
            .cloned()
            .collect(),
    };
    kv.sort();
    kv
}

/// The output grid a [`VectorAggFold`] indexes its dense slots by: the
/// same `(grid_start, step, kmax)` triple `RangeSlideState` emits its
/// points from, so a slot index and a grid point are two views of one
/// value.
#[derive(Clone, Copy, Debug)]
pub(in crate::logql) struct FoldGrid {
    pub(in crate::logql) start: i64,
    pub(in crate::logql) step: u64,
    pub(in crate::logql) kmax: i64,
}

impl FoldGrid {
    /// Slots in one group's dense vector: `kmax + 1`, and 0 for the empty
    /// grid (`kmax == -1`, which `grid_point_count == 0` produces).
    fn slots(&self) -> usize {
        usize::try_from(grid_slot_count(self.kmax)).unwrap_or(0)
    }

    /// `grid_start + k*step`, narrowed exactly as `FpSlide::grid_point`
    /// does — one arithmetic, so a folded point and a materialised point
    /// carry the same timestamp bits.
    fn point(&self, k: usize) -> i64 {
        clamp_bucket(self.start as i128 + k as i128 * self.step as i128)
    }

    /// The inverse of [`Self::point`]: `None` when `t` is not one of this
    /// grid's points. Every producer that feeds the fold emits at
    /// `grid_point(k)` for `k in 0..=kmax` (`FpSlide::emit_at`,
    /// `finish_in_place`'s fan-out arm, `finish_absent`), so `None` is an
    /// internal-invariant breach, not a user-reachable input — it is
    /// reported as an error rather than dropped, because a dropped point
    /// is a silently wrong result.
    fn index_of(&self, t: i64) -> Option<usize> {
        let step = i128::from(self.step);
        if step <= 0 {
            return None;
        }
        let delta = i128::from(t) - i128::from(self.start);
        if delta < 0 || delta % step != 0 {
            return None;
        }
        let k = delta / step;
        if k > i128::from(self.kmax) {
            return None;
        }
        usize::try_from(k).ok()
    }
}

/// [`RangeSlideState::covering_k`]'s body as a free function over the
/// grid scalars, so `finish_in_place`'s C2 sweep can compute the same
/// intervals without holding a `&self` borrow across its `&mut self`
/// discharges. ONE implementation, called from both.
pub(in crate::logql) fn covering_k_of(
    ts: i64,
    grid_start: i64,
    step: u64,
    range: i64,
    kmax: i64,
) -> (i64, i64) {
    let step = step as i128;
    let gs = grid_start as i128;
    let ts = ts as i128;
    let range = range as i128;
    // ts ≤ grid_start + k·step  ⇒  k ≥ ceil((ts-gs)/step)
    let k_lo = ceil_div_i128(ts - gs, step).max(0);
    // grid_start + k·step < ts+range ⇒ k·step ≤ ts+range-gs-1 ⇒
    // k ≤ floor((ts+range-gs-1)/step)
    let k_hi = (ts + range - gs - 1).div_euclid(step).min(kmax as i128);
    (
        i64::try_from(k_lo).unwrap_or(i64::MAX),
        i64::try_from(k_hi).unwrap_or(i64::MIN),
    )
}

/// Grid points on a `kmax`-indexed emit grid: `kmax + 1`, and 0 for the
/// empty grid. The unit every [`charge_result_points`] reservation is
/// made in — one series can emit at most this many points, and
/// [`MAX_METRIC_RESULT_POINTS`] is derived as
/// `MAX_QUERY_SERIES * MAX_ADMITTED_GRID_POINTS` in exactly these units.
pub(in crate::logql) fn grid_slot_count(kmax: i64) -> u64 {
    u64::try_from(kmax.saturating_add(1)).unwrap_or(0)
}

/// The internal-invariant breach [`FoldGrid::index_of`] reports.
fn fold_off_grid(t: i64) -> ReadError {
    ReadError::PipelineInvalid {
        reason: format!(
            "internal: vector-aggregation fold received a point at {t} off the query grid"
        ),
    }
}

/// One `topk`/`bottomk` candidate holding a grid slot: the sample value
/// and the id of the series it came from. Ids are assigned in PUSH order,
/// which is what makes [`SelectFold`]'s emission order `select_k_range`'s
/// (whose survivors come out in the input vector's order).
#[derive(Clone, Copy, Debug)]
pub(in crate::logql) struct Cand {
    value: f64,
    series: u32,
}

/// One grid slot's surviving candidates, best first.
///
/// `Empty`/`One` keep the common cases allocation-free — a slot no series
/// reached, and `topk(1, …)` or a slot only one series reached. `Many` is
/// the only arm that allocates and holds at most `k` elements, so a
/// group's whole selection state is `O(grid x k)` and never `O(scanned
/// series x grid)`.
#[derive(Clone, Debug)]
pub(in crate::logql) enum KSel {
    Empty,
    One(Cand),
    Many(Vec<Cand>),
}

impl KSel {
    fn as_slice(&self) -> &[Cand] {
        match self {
            KSel::Empty => &[],
            KSel::One(c) => std::slice::from_ref(c),
            KSel::Many(v) => v,
        }
    }

    /// Inserts `cand`, keeping the slot ordered best-first under `order`
    /// and at most `k` long. Returns the candidate that lost its place —
    /// which is `cand` itself when the slot is full and `cand` is worse
    /// than every survivor.
    ///
    /// Equivalent to `sort_candidates(all).take(k)` because `order` is a
    /// TOTAL order (see [`cand_order`]): the k best elements of a set are
    /// the same set whatever sequence they arrive in.
    fn insert<F>(&mut self, cand: Cand, k: usize, order: &F) -> Option<Cand>
    where
        F: Fn(&Cand, &Cand) -> std::cmp::Ordering,
    {
        match self {
            KSel::Empty => {
                *self = KSel::One(cand);
                None
            }
            KSel::One(cur) => {
                if k == 1 {
                    if order(&cand, cur) == std::cmp::Ordering::Less {
                        let evicted = *cur;
                        *self = KSel::One(cand);
                        Some(evicted)
                    } else {
                        Some(cand)
                    }
                } else {
                    let pair = if order(&cand, cur) == std::cmp::Ordering::Less {
                        vec![cand, *cur]
                    } else {
                        vec![*cur, cand]
                    };
                    *self = KSel::Many(pair);
                    None
                }
            }
            KSel::Many(v) => {
                let pos = v.partition_point(|c| order(c, &cand) == std::cmp::Ordering::Less);
                if pos >= k {
                    // The slot is full (`pos <= v.len() <= k`) and `cand`
                    // sorts after every survivor.
                    return Some(cand);
                }
                v.insert(pos, cand);
                if v.len() > k { v.pop() } else { None }
            }
        }
    }
}

/// A series that currently holds at least one selection slot. Dropped the
/// moment its refcount reaches 0, so `live` is bounded by `output groups
/// x k` rather than by the number of series pushed.
#[derive(Debug)]
struct LiveSeries {
    labels: LabelSet,
    slots: u64,
}

/// [`sort_candidates`]' order, as a comparator over [`Cand`] and extended
/// with the series id ascending as a final tiebreak.
///
/// The tiebreak is not cosmetic and it is not a divergence: `sort_by` is
/// STABLE and `select_k_range` collects its candidates in ascending input
/// index, so two candidates that tie on `(is_nan, value, labels)` already
/// come out in ascending input order there. Naming it makes the fold's
/// order TOTAL, which is what lets an incremental top-k equal a full sort
/// plus `take(k)`. It is reachable: a fingerprint with no hydrated meta
/// gets an EMPTY label set, so two such series tie on labels.
fn cand_order<'a, F>(a: &Cand, b: &Cand, largest: bool, labels_of: &F) -> std::cmp::Ordering
where
    F: Fn(u32) -> &'a LabelSet,
{
    a.value
        .is_nan()
        .cmp(&b.value.is_nan())
        .then_with(|| {
            if a.value.is_nan() {
                std::cmp::Ordering::Equal
            } else if largest {
                b.value.total_cmp(&a.value)
            } else {
                a.value.total_cmp(&b.value)
            }
        })
        .then_with(|| labels_of(a.series).cmp(labels_of(b.series)))
        .then_with(|| a.series.cmp(&b.series))
}

/// The reducing fold (`sum`/`avg`/`min`/`max`/`count`/`stddev`/`stdvar`):
/// one dense `Vec<VectorAccum>` of `kmax + 1` slots per OUTPUT group,
/// each slot the same [`VectorAccum`] `reduce` uses — so a folded value
/// and a materialised one are the same bits by construction, not by
/// coincidence.
#[derive(Debug)]
pub(in crate::logql) struct ReduceFold {
    op: VectorAggOp,
    grouping: Option<Grouping>,
    grid: FoldGrid,
    groups: HashMap<LabelSet, Vec<VectorAccum>>,
    /// Reserved point-slots, charged through [`charge_result_points`]
    /// BEFORE the dense vector below is allocated (issue #236).
    slots: u64,
    slot_cap: u64,
    /// Retained group-key bytes, charged through [`charge_group_bytes`]
    /// BEFORE [`group_key`] builds the key (issue #236 review round 1
    /// `[high]`). A `by(...)` clause is read off the QUERY TEXT, so
    /// without this the fold retained query-text-derived label bytes with
    /// nothing bounding them.
    group_bytes: u64,
    group_cap: u64,
}

impl ReduceFold {
    fn push_series(&mut self, labels: &LabelSet, points: &[(i64, f64)]) -> Result<(), ReadError> {
        // A group materialises on the first accumulated VALUE, never on
        // first sight of a member (plan v14 §3 Part B): a series with no
        // points must not create — or charge for — a group.
        if points.is_empty() {
            return Ok(());
        }
        let (op, grid) = (self.op, self.grid);
        // CHARGE, THEN ALLOCATE. `or_insert_with` would allocate inside
        // its closure, which is why the entry is matched explicitly: the
        // dense `kmax + 1` vector is reserved against the cap before it
        // exists, so a breach refuses rather than being observed after
        // the fact.
        let charged = &mut self.slots;
        let cap = self.slot_cap;
        // The KEY's bytes are charged before `group_key` builds it —
        // `group_key` allocates one owned `String` per `by` NAME, read
        // off the query text, and the entry expression would otherwise
        // evaluate it before any charge (review round 1 `[high]`). The
        // counter tracks what is RETAINED, so `key_charge` only sticks
        // where the key does: an Occupied group lets the guard drop and
        // refunds (the key is transient there), and the Vacant arm's
        // point reservation can still refuse AFTER the key charge — the
        // guard refunds that exit too.
        let (key, key_charge) = charged_group_key(
            labels,
            self.grouping.as_ref(),
            &mut self.group_bytes,
            self.group_cap,
        )?;
        let slots = match self.groups.entry(key) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                charge_result_points(charged, grid.slots() as u64, cap)?;
                let slots = e.insert(vec![VectorAccum::EMPTY; grid.slots()]);
                key_charge.commit();
                slots
            }
        };
        for &(t, v) in points {
            let k = grid.index_of(t).ok_or_else(|| fold_off_grid(t))?;
            let Some(slot) = slots.get_mut(k) else {
                return Err(fold_off_grid(t));
            };
            if slot.is_empty() {
                *slot = VectorAccum::seed(op, v);
            } else {
                slot.update(op, v);
            }
        }
        Ok(())
    }

    fn finish(self) -> Vec<MatrixSeries> {
        let (op, grid) = (self.op, self.grid);
        self.groups
            .into_iter()
            .filter_map(|(labels, slots)| {
                let points: Vec<(i64, f64)> = slots
                    .into_iter()
                    .enumerate()
                    .filter(|(_, acc)| !acc.is_empty())
                    .map(|(k, acc)| (grid.point(k), acc.finish(op)))
                    .collect();
                (!points.is_empty()).then_some(MatrixSeries { labels, points })
            })
            .collect()
    }
}

/// The selecting fold (`topk`/`bottomk`): one dense `Vec<KSel>` of
/// `kmax + 1` slots per output group, each holding at most `k`
/// candidates, plus the label sets of the series currently holding a
/// slot. `select_k_range` materialises `scanned series x steps` before
/// applying `k`; this never holds more than `output groups x grid x k`.
#[derive(Debug)]
pub(in crate::logql) struct SelectFold {
    /// `topk` keeps the largest, `bottomk` the smallest.
    largest: bool,
    k: usize,
    grouping: Option<Grouping>,
    grid: FoldGrid,
    groups: HashMap<LabelSet, Vec<KSel>>,
    /// Refcounted by held slots — see [`LiveSeries`].
    live: HashMap<u32, LiveSeries>,
    /// The next push-order id; `select_k_range`'s input index.
    next_series: u32,
    /// Reserved point-slots (issue #236): the dense per-group vector, and
    /// one more for each candidate a slot retains beyond its first — the
    /// `KSel::Many` heap, which the dense reservation does not cover.
    slots: u64,
    slot_cap: u64,
    /// Retained group-key bytes — see [`ReduceFold::group_bytes`].
    group_bytes: u64,
    group_cap: u64,
}

impl SelectFold {
    fn push_series(&mut self, labels: &LabelSet, points: &[(i64, f64)]) -> Result<(), ReadError> {
        let id = self.next_series;
        self.next_series = self.next_series.saturating_add(1);
        if points.is_empty() {
            return Ok(());
        }
        let (grid, k, largest) = (self.grid, self.k, self.largest);
        // A group's slot vector is created on the first push that can
        // fill a slot: `k >= 1` here (`k == 0` is `VectorAggFold::Empty`,
        // which never constructs a `SelectFold`), so the first series to
        // reach a fresh group always wins its slots.
        // CHARGE, THEN ALLOCATE — as `ReduceFold::push_series`.
        let charged = &mut self.slots;
        let cap = self.slot_cap;
        // CHARGE, THEN ALLOCATE — as `ReduceFold::push_series`, and for
        // the same reason.
        let (key, key_charge) = charged_group_key(
            labels,
            self.grouping.as_ref(),
            &mut self.group_bytes,
            self.group_cap,
        )?;
        let slots = match self.groups.entry(key) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                charge_result_points(charged, grid.slots() as u64, cap)?;
                let slots = e.insert(vec![KSel::Empty; grid.slots()]);
                key_charge.commit();
                slots
            }
        };
        let live = &mut self.live;
        for &(t, v) in points {
            let idx = grid.index_of(t).ok_or_else(|| fold_off_grid(t))?;
            let Some(slot) = slots.get_mut(idx) else {
                return Err(fold_off_grid(t));
            };
            // CHARGE, THEN ALLOCATE — the reservation must sit AHEAD of
            // the insertion, not observe it afterwards. The slot's
            // occupancy grows by exactly one iff it is not already full,
            // which is known before `insert` runs: below `k` the new
            // candidate is always accepted, at `k` the insertion is
            // occupancy-neutral (it either evicts one or is rejected).
            if slot.as_slice().len() < k {
                charge_result_points(charged, 1, cap)?;
            }
            let evicted = {
                let seen: &HashMap<u32, LiveSeries> = live;
                // Every candidate already in the slot HOLDS it, so its
                // series is live; the pushing series may not be yet.
                // `EMPTY_LABEL_SET` is the defensive arm, never a
                // reachable one.
                let labels_of = |sid: u32| {
                    if sid == id {
                        labels
                    } else {
                        seen.get(&sid).map_or(&EMPTY_LABEL_SET, |s| &s.labels)
                    }
                };
                let order = |a: &Cand, b: &Cand| cand_order(a, b, largest, &labels_of);
                slot.insert(
                    Cand {
                        value: v,
                        series: id,
                    },
                    k,
                    &order,
                )
            };
            // A series pushes at most one candidate per slot (its points
            // carry distinct timestamps), so an evicted candidate from
            // THIS series can only be the one just offered and rejected.
            if let Some(ev) = evicted
                && ev.series == id
            {
                continue;
            }
            if let Some(ev) = evicted {
                let dropped = match live.get_mut(&ev.series) {
                    Some(held) => {
                        held.slots = held.slots.saturating_sub(1);
                        held.slots == 0
                    }
                    None => false,
                };
                if dropped {
                    live.remove(&ev.series);
                }
            }
            // The label set is cloned on the first slot this series
            // actually wins, never on first sight of it.
            live.entry(id)
                .or_insert_with(|| LiveSeries {
                    labels: labels.clone(),
                    slots: 0,
                })
                .slots += 1;
        }
        Ok(())
    }

    fn finish(self) -> Vec<MatrixSeries> {
        let grid = self.grid;
        // A series belongs to exactly one group (its `group_key` is a
        // function of its labels), and each group's slots are walked in
        // ascending grid index, so a survivor's points come out
        // TIMESTAMP-ASCENDING — the order `select_k_range`'s per-survivor
        // `BTreeMap` yields.
        let mut by_series: HashMap<u32, Vec<(i64, f64)>> = HashMap::new();
        for slots in self.groups.into_values() {
            for (k, sel) in slots.iter().enumerate() {
                let t = grid.point(k);
                for cand in sel.as_slice() {
                    by_series
                        .entry(cand.series)
                        .or_default()
                        .push((t, cand.value));
                }
            }
        }
        // Survivors in ORIGINAL PUSH ORDER — `select_k_range` emits
        // `series.into_iter().zip(keep).filter_map(..)`, i.e. the input
        // vector's order, and ids are the input index.
        let mut survivors: Vec<(u32, LiveSeries)> = self.live.into_iter().collect();
        survivors.sort_by_key(|(id, _)| *id);
        survivors
            .into_iter()
            .filter_map(|(id, held)| {
                by_series
                    .remove(&id)
                    .filter(|points| !points.is_empty())
                    .map(|points| MatrixSeries {
                        labels: held.labels,
                        points,
                    })
            })
            .collect()
    }
}

/// The innermost vector aggregation, applied AS the range leaf emits
/// rather than over its materialised output. See the module-section
/// comment above for the bound this replaces.
#[derive(Debug)]
pub(in crate::logql) enum VectorAggFold {
    Reduce(ReduceFold),
    Select(SelectFold),
    /// `topk(0, …)`/`bottomk(0, …)`: the result is empty whatever the
    /// input, so no group is ever constructed and no point is ever
    /// retained. Its reason is purely charge discipline — do not charge
    /// for a group that emits nothing; the group-count rejection whose
    /// premise it used to protect is gone (plan v14 §3 Part B).
    Empty,
}

impl VectorAggFold {
    /// The fold's OWN group-byte counter and the cap it checks against
    /// — a test seam (issue #260) so the "two live counters against
    /// [`super::charge::MAX_CLIENT_AGG_GROUP_BYTES`]" claim can be
    /// OBSERVED at the moment of breach rather than asserted. `Empty`
    /// holds no groups and therefore no counter.
    #[cfg(test)]
    pub(in crate::logql) fn group_byte_counter(&self) -> Option<(u64, u64)> {
        match self {
            VectorAggFold::Reduce(f) => Some((f.group_bytes, f.group_cap)),
            VectorAggFold::Select(f) => Some((f.group_bytes, f.group_cap)),
            VectorAggFold::Empty => None,
        }
    }

    /// `None` when the leaf cannot own the aggregation:
    ///
    /// * `sort`/`sort_desc` — a range matrix is a PASSTHROUGH at
    ///   `group_range` (there is no single sortable value per series), so
    ///   there is nothing to fold;
    /// * `approx_topk` — instant-only, rejected for a range query at
    ///   plan time (`plan.rs`'s `approx_topk` range check).
    ///
    /// The match is exhaustive with no `_` arm, and
    /// `vector_agg_fold_partitions_every_op_like_is_reduction` pins the
    /// reducing arm against [`VectorAccum::is_reduction`], so a new
    /// operator is a build failure here rather than a silent
    /// misclassification.
    pub(in crate::logql) fn new(
        spec: &plan::VectorAggSpec,
        grid: FoldGrid,
        slot_cap: u64,
        group_cap: u64,
    ) -> Option<Self> {
        let (op, grouping, param) = spec;
        match *op {
            VectorAggOp::Sort | VectorAggOp::SortDesc | VectorAggOp::ApproxTopk => None,
            VectorAggOp::Topk | VectorAggOp::Bottomk => {
                let k = k_of(*param);
                if k == 0 {
                    return Some(VectorAggFold::Empty);
                }
                Some(VectorAggFold::Select(SelectFold {
                    largest: matches!(*op, VectorAggOp::Topk),
                    k,
                    grouping: grouping.clone(),
                    grid,
                    groups: HashMap::new(),
                    live: HashMap::new(),
                    next_series: 0,
                    slots: 0,
                    slot_cap,
                    group_bytes: 0,
                    group_cap,
                }))
            }
            VectorAggOp::Sum
            | VectorAggOp::Avg
            | VectorAggOp::Min
            | VectorAggOp::Max
            | VectorAggOp::Count
            | VectorAggOp::Stddev
            | VectorAggOp::Stdvar => Some(VectorAggFold::Reduce(ReduceFold {
                op: *op,
                grouping: grouping.clone(),
                grid,
                groups: HashMap::new(),
                slots: 0,
                slot_cap,
                group_bytes: 0,
                group_cap,
            })),
        }
    }

    /// Folds one COMPLETE leaf series. `points` must be this grid's
    /// points, timestamp-ascending — which is what every emit site
    /// produces.
    ///
    /// Fallible today only through [`FoldGrid::index_of`]; the signature
    /// is the seam plan v14 §4's `charge_result_points` charges through,
    /// so it is `Result` from the start rather than widened later across
    /// every call site.
    pub(in crate::logql) fn push_series(
        &mut self,
        labels: &LabelSet,
        points: &[(i64, f64)],
    ) -> Result<(), ReadError> {
        match self {
            VectorAggFold::Reduce(f) => f.push_series(labels, points),
            VectorAggFold::Select(f) => f.push_series(labels, points),
            VectorAggFold::Empty => Ok(()),
        }
    }

    pub(in crate::logql) fn finish(self) -> Vec<MatrixSeries> {
        match self {
            VectorAggFold::Reduce(f) => f.finish(),
            VectorAggFold::Select(f) => f.finish(),
            VectorAggFold::Empty => Vec::new(),
        }
    }

    /// Retained cells: the quantity plan v14 §4's `charge_result_points`
    /// will charge, and the one AC 8 pins as `output groups x steps`.
    /// Test-only until that counter exists — nothing in the engine reads
    /// it, and exposing it now would suggest it were enforced.
    #[cfg(test)]
    fn cells(&self) -> usize {
        match self {
            VectorAggFold::Reduce(f) => f.groups.values().map(Vec::len).sum(),
            VectorAggFold::Select(f) => f.groups.values().map(Vec::len).sum(),
            VectorAggFold::Empty => 0,
        }
    }

    /// Point-slots reserved so far. Test-only, as [`Self::cells`].
    #[cfg(test)]
    fn reserved_slots(&self) -> u64 {
        match self {
            VectorAggFold::Reduce(f) => f.slots,
            VectorAggFold::Select(f) => f.slots,
            VectorAggFold::Empty => 0,
        }
    }

    /// Output groups currently materialised. Test-only, as [`Self::cells`].
    #[cfg(test)]
    fn groups(&self) -> usize {
        match self {
            VectorAggFold::Reduce(f) => f.groups.len(),
            VectorAggFold::Select(f) => f.groups.len(),
            VectorAggFold::Empty => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::error::TooBroadReason;
    use super::*;
    use crate::logql::MAX_METRIC_RESULT_POINTS;
    use crate::logql::charge::label_set_bytes;
    use crate::logql::testkit::*;

    /// **The ordering, checked where it is written.** A behavioural test
    /// cannot distinguish charge-then-build from build-then-charge — the
    /// transient key is allocated either way and the refusal lands at the
    /// same point — so the three-step order inside `charged_group_key`
    /// (charge, guard, build) is asserted directly, and the fold is
    /// asserted to reach `group_key` through NOTHING ELSE and to commit
    /// its [`GroupKeyCharge`] only after the insertion that retains the
    /// key.
    #[test]
    fn the_fold_charges_before_it_builds_a_group_key() {
        let src = include_str!("fold.rs");
        let start = src
            .find("fn charged_group_key<'a>(")
            .expect("the fold's charged route to group_key");
        let end = src[start..]
            .find("fn group_key(")
            .expect("the end of charged_group_key")
            + start;
        let body = &src[start..end];
        let charge = body
            .find("charge_group_bytes(")
            .expect("charged_group_key must charge");
        let guard = body
            .find("GroupKeyCharge { charged, bytes }")
            .expect("charged_group_key must build the refund guard");
        let build = body
            .find("group_key(labels, grouping)")
            .expect("charged_group_key must build the key");
        assert!(
            charge < build,
            "charged_group_key builds the key BEFORE charging for it — the charge must precede \
             the allocation, not observe it"
        );
        // And the guard sits BETWEEN them. `group_key` allocates, so a
        // guard constructed after it leaves a window in which an
        // unwinding panic passes a charge that nothing owns: the guard
        // cannot refund a charge it does not yet have. Compiles either
        // way — `group_key` does not touch `charged` — so the position
        // is asserted where it is written.
        assert!(
            charge < guard && guard < build,
            "charged_group_key must construct its GroupKeyCharge between the charge and the key \
             — a guard built after group_key cannot refund a charge it does not yet own"
        );

        // And the two fold bodies reach `group_key` only through it: a
        // direct call there would reintroduce the reviewed defect at a
        // site this census does not read.
        for anchor in ["impl ReduceFold {", "impl SelectFold {"] {
            let s = src.find(anchor).unwrap_or_else(|| panic!("{anchor} moved"));
            let e = src[s + anchor.len()..]
                .find("\nimpl ")
                .map_or(src.len(), |o| s + anchor.len() + o);
            let region = &src[s..e];
            assert!(
                region.contains("charged_group_key("),
                "{anchor} must reach group_key through charged_group_key"
            );
            assert!(
                !region.contains("group_key(labels, self.grouping"),
                "{anchor} calls group_key DIRECTLY — the fold has no funnel token and must \
                 charge in its own regime first"
            );
            // The same class, one statement later: `GroupKeyCharge`
            // refunds unless `commit` runs, so a `commit` hoisted above
            // the insertion it pays for would put the fallible point
            // reservation back INSIDE the committed window and leak the
            // key charge on refusal. Compiles either way, and — as
            // above — nothing observable would show it, so the order is
            // asserted where it is written.
            let insert = region
                .find(".insert(vec![")
                .unwrap_or_else(|| panic!("{anchor} must insert its dense slot vector"));
            let commit = region
                .find("key_charge.commit();")
                .unwrap_or_else(|| panic!("{anchor} must commit the key charge it retains"));
            assert!(
                insert < commit,
                "{anchor} commits the key charge BEFORE the insertion that retains the key — \
                 every fallible step ahead of the insertion must still be able to refund"
            );
            assert_eq!(
                region.matches("key_charge.commit();").count(),
                1,
                "{anchor} must commit the key charge exactly once — on the retaining path only"
            );
        }
    }

    /// `group_key_bytes` sizes exactly what `group_key` allocates. If the
    /// two drift the charge stops meaning anything, so they are pinned
    /// against each other over both grouping kinds, absent names, empty
    /// values and the no-grouping case.
    #[test]
    fn group_key_bytes_matches_group_key() {
        let labels = fold_labels(&[("id", "abc"), ("host", ""), ("zone", "eu-west-1")]);
        let cases: Vec<Option<Grouping>> = vec![
            None,
            Some(Grouping {
                kind: GroupingKind::By,
                labels: vec!["id".to_string()],
            }),
            Some(Grouping {
                kind: GroupingKind::By,
                labels: vec!["id".to_string(), "absent".to_string(), "host".to_string()],
            }),
            Some(Grouping {
                kind: GroupingKind::By,
                labels: (0..64).map(|i| format!("absent{i:03}")).collect(),
            }),
            Some(Grouping {
                kind: GroupingKind::Without,
                labels: vec!["id".to_string()],
            }),
            Some(Grouping {
                kind: GroupingKind::Without,
                labels: vec!["nothing".to_string()],
            }),
        ];
        for g in &cases {
            let built = group_key(&labels, g.as_ref());
            let sized = group_key_bytes(&labels, g.as_ref());
            let actual = if built.is_empty() && g.is_none() {
                0
            } else {
                label_set_bytes(&built)
            };
            assert!(
                sized >= actual,
                "{g:?}: sized {sized} under-charges the {actual} bytes group_key allocates"
            );
            // And it must not be a blanket over-charge either: the only
            // slack is `grown_alloc_bytes` vs `alloc_block_bytes` on the
            // element buffer, a factor of 3 on one term.
            assert!(
                sized <= actual.saturating_mul(3).saturating_add(96),
                "{g:?}: sized {sized} is not a tight bound on {actual}"
            );
        }
    }

    /// **Review round 1's `[high]`, as a gate.** The fold charges a
    /// group's KEY bytes before `group_key` builds it: a refused key is
    /// never retained, the map never grows, and the counter does not
    /// stick. Mutant shape: moving the charge after the `entry`
    /// expression, or deleting it, reddens here.
    #[test]
    fn the_fold_charges_a_group_key_before_group_key_allocates_it() {
        let grid = FoldGrid {
            start: 0,
            step: 10,
            kmax: 4,
        };
        // A `by(...)` clause whose names come from the QUERY TEXT — the
        // bytes that were unbounded before this charge existed.
        // One PRESENT distinguishing name, so distinct series land in
        // distinct groups; the other 64 are absent from the data and
        // exist only in the query text — which is the mass this charge
        // exists to bound.
        let wide = Grouping {
            kind: GroupingKind::By,
            labels: std::iter::once("id".to_string())
                .chain((0..64).map(|i| format!("qtext{i:04}")))
                .collect(),
        };
        let labels = fold_labels(&[("id", "a")]);
        let key_bytes =
            group_key_bytes(&labels, Some(&wide)).saturating_add(map_entry_bytes(FOLD_GROUP_SLOT));
        assert!(key_bytes > 0, "a 64-name by-clause must cost something");

        for op in [VectorAggOp::Sum, VectorAggOp::Topk] {
            let param = matches!(op, VectorAggOp::Topk).then_some(2.0);
            // One byte short of the first key.
            let mut fold = VectorAggFold::new(
                &(op, Some(wide.clone()), param),
                grid,
                u64::MAX,
                key_bytes - 1,
            )
            .expect("folds");
            match fold.push_series(&labels, &[(0, 1.0)]) {
                Err(ReadError::QueryTooBroad(TooBroadReason::MetricGroupLabelBytes {
                    bytes,
                    cap,
                })) => {
                    assert_eq!(cap, key_bytes - 1);
                    assert_eq!(bytes, key_bytes);
                }
                other => panic!("{op:?}: expected MetricGroupLabelBytes, got {other:?}"),
            }
            assert_eq!(
                fold.groups(),
                0,
                "{op:?}: the refused group's key must never be retained"
            );
            assert_eq!(
                fold.cells(),
                0,
                "{op:?}: nothing may be allocated behind a refused key"
            );

            // Room for ONE retained key plus the one TRANSIENT key a push
            // must be able to build before it can be compared: the charge
            // precedes the allocation, so the bound necessarily covers
            // `retained + 1`. A second DISTINCT group needs a third and
            // breaches.
            let mut fold = VectorAggFold::new(
                &(op, Some(wide.clone()), param),
                grid,
                u64::MAX,
                2 * key_bytes,
            )
            .expect("folds");
            fold.push_series(&labels, &[(0, 1.0)])
                .expect("the first group fits");
            assert_eq!(fold.groups(), 1);
            // Re-pushing the SAME group REFUNDS its transient key: without
            // the refund the counter would climb on every row of an
            // existing group and the fold would refuse its own data.
            fold.push_series(&labels, &[(10, 2.0)])
                .expect("an existing group refunds its transient key");
            assert_eq!(fold.groups(), 1);
            let second = fold_labels(&[("id", "b")]);
            fold.push_series(&second, &[(0, 1.0)])
                .expect("a second retained group fits the two-key budget");
            assert_eq!(fold.groups(), 2);
            let third = fold_labels(&[("id", "c")]);
            match fold.push_series(&third, &[(0, 1.0)]) {
                Err(ReadError::QueryTooBroad(TooBroadReason::MetricGroupLabelBytes { .. })) => {}
                other => panic!("{op:?}: a third group must breach, got {other:?}"),
            }
            assert_eq!(fold.groups(), 2, "{op:?}: the refusal must not evict");
        }
    }

    /// The FOLD reserves its dense slots before the vector exists, and
    /// the reservation is the vector's own width.
    #[test]
    fn the_fold_reserves_its_dense_slots_before_allocating_them() {
        let grid = FoldGrid {
            start: 0,
            step: 10,
            kmax: 4,
        };
        let slots = 5u64;
        let by_id = Grouping {
            kind: GroupingKind::By,
            labels: vec!["id".to_string()],
        };
        // Two output groups' worth of room, three groups offered.
        let mut fold = VectorAggFold::new(
            &(VectorAggOp::Sum, Some(by_id), None),
            grid,
            2 * slots,
            u64::MAX,
        )
        .expect("sum folds");
        for g in 0..2u32 {
            let labels = fold_labels(&[("id", &g.to_string())]);
            fold.push_series(&labels, &[(0, 1.0)]).expect("admitted");
        }
        assert_eq!(fold.groups(), 2);
        assert_eq!(
            fold.cells(),
            2 * slots as usize,
            "dense, kmax + 1 per group"
        );
        let labels = fold_labels(&[("id", "2")]);
        match fold.push_series(&labels, &[(0, 1.0)]) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricResultPoints { count, cap })) => {
                assert_eq!(cap, 2 * slots);
                assert_eq!(count, 3 * slots);
            }
            other => panic!("expected MetricResultPoints, got {other:?}"),
        }
        assert_eq!(
            fold.groups(),
            2,
            "the refused group's dense vector must never be allocated"
        );

        // The selecting fold charges the same dense reservation, plus one
        // for each candidate a slot retains beyond its first.
        let mut fold =
            VectorAggFold::new(&(VectorAggOp::Topk, None, Some(2.0)), grid, 1_000, u64::MAX)
                .expect("topk folds");
        fold.push_series(&fold_labels(&[("h", "a")]), &[(0, 1.0)])
            .expect("admitted");
        // One group's dense vector (5) + one candidate.
        assert_eq!(fold_slots(&fold), slots + 1);
        fold.push_series(&fold_labels(&[("h", "b")]), &[(0, 2.0)])
            .expect("admitted");
        assert_eq!(fold_slots(&fold), slots + 2, "the slot grew to k = 2");
        // The slot is now FULL: a third candidate evicts rather than
        // growing, so it reserves nothing.
        fold.push_series(&fold_labels(&[("h", "c")]), &[(0, 3.0)])
            .expect("admitted");
        assert_eq!(
            fold_slots(&fold),
            slots + 2,
            "an eviction is occupancy-neutral and must charge nothing"
        );
    }

    const SELECTING_OPS: [VectorAggOp; 5] = [
        VectorAggOp::Topk,
        VectorAggOp::Bottomk,
        VectorAggOp::ApproxTopk,
        VectorAggOp::Sort,
        VectorAggOp::SortDesc,
    ];

    /// The fold's op partition is the SAME partition
    /// [`VectorAccum::is_reduction`] states, plus the two the leaf cannot
    /// own. Both matches are exhaustive with no `_` arm, so a new
    /// operator is a build failure; this pins that they agree with each
    /// other rather than each being separately exhaustive and wrong.
    #[test]
    fn vector_agg_fold_partitions_every_op_like_is_reduction() {
        let grid = FoldGrid {
            start: 0,
            step: 1,
            kmax: 3,
        };
        for op in REDUCING_OPS {
            let fold =
                VectorAggFold::new(&(op, None, None), grid, MAX_METRIC_RESULT_POINTS, u64::MAX)
                    .unwrap_or_else(|| panic!("{op:?} is reducing and must fold"));
            assert!(
                matches!(fold, VectorAggFold::Reduce(_)),
                "{op:?} must be a ReduceFold"
            );
            assert!(VectorAccum::is_reduction(op));
        }
        for op in SELECTING_OPS {
            assert!(!VectorAccum::is_reduction(op));
        }
        // `sort`/`sort_desc` are a matrix PASSTHROUGH at `group_range`,
        // and `approx_topk` is rejected for a range query at plan time —
        // the leaf declines all three and the caller materialises.
        for op in [
            VectorAggOp::Sort,
            VectorAggOp::SortDesc,
            VectorAggOp::ApproxTopk,
        ] {
            assert!(
                VectorAggFold::new(
                    &(op, None, Some(3.0)),
                    grid,
                    MAX_METRIC_RESULT_POINTS,
                    u64::MAX
                )
                .is_none(),
                "{op:?} must be declined by the leaf"
            );
        }
        for op in [VectorAggOp::Topk, VectorAggOp::Bottomk] {
            assert!(matches!(
                VectorAggFold::new(
                    &(op, None, Some(2.0)),
                    grid,
                    MAX_METRIC_RESULT_POINTS,
                    u64::MAX
                ),
                Some(VectorAggFold::Select(_))
            ));
            assert!(matches!(
                VectorAggFold::new(
                    &(op, None, Some(0.0)),
                    grid,
                    MAX_METRIC_RESULT_POINTS,
                    u64::MAX
                ),
                Some(VectorAggFold::Empty)
            ));
        }
    }

    fn fold_slots(fold: &VectorAggFold) -> u64 {
        fold.reserved_slots()
    }

    /// AC 8 — the fold's state is bounded by the OUTPUT, not by the scan.
    ///
    /// A range query over `N` leaf groups collapsing to `G` output groups
    /// over `S` steps retains exactly `G x S` cells, and running at `N`
    /// and `10N` retains the IDENTICAL number. Under the materialising
    /// path the same input holds `N x S` points before the aggregation
    /// runs, which is the quantity this replaces.
    #[test]
    fn the_fold_retains_output_groups_times_steps_whatever_the_scan_width() {
        const STEPS: usize = 7;
        let grid = FoldGrid {
            start: 0,
            step: 10,
            kmax: STEPS as i64 - 1,
        };
        // Two output groups (`by (tier)`), `n` leaf series feeding them.
        let grouping = Grouping {
            kind: GroupingKind::By,
            labels: vec!["tier".to_string()],
        };
        let leaf = |n: usize| -> Vec<FoldInput> {
            (0..n)
                .map(|i| {
                    (
                        fold_labels(&[
                            ("tier", if i % 2 == 0 { "hot" } else { "cold" }),
                            ("id", &i.to_string()),
                        ]),
                        (0..STEPS)
                            .map(|k| ((k as i64) * 10, (i + k) as f64))
                            .collect(),
                    )
                })
                .collect()
        };
        let cells_at = |n: usize| -> (usize, usize, usize) {
            let mut fold = VectorAggFold::new(
                &(VectorAggOp::Sum, Some(grouping.clone()), None),
                grid,
                MAX_METRIC_RESULT_POINTS,
                u64::MAX,
            )
            .expect("sum folds");
            for (labels, points) in leaf(n) {
                fold.push_series(&labels, &points).expect("on-grid");
            }
            let cells = fold.cells();
            let groups = fold.groups();
            let out = fold.finish().len();
            (cells, groups, out)
        };
        let small = cells_at(20);
        let wide = cells_at(200);
        assert_eq!(small, (2 * STEPS, 2, 2), "G x S cells, G output series");
        assert_eq!(
            small, wide,
            "10x the leaf groups must retain the IDENTICAL cell count — the \
             fold is bounded by the OUTPUT, not by the scan"
        );
    }

    /// AC 10 — `topk(0, …)`/`bottomk(0, …)`: no group is ever
    /// constructed and no cell is ever retained, however wide the scan.
    /// A fold that counted groups before consulting `k` would build 501
    /// of them here.
    #[test]
    fn zero_k_fold_constructs_no_group_and_retains_no_cell() {
        let grid = FoldGrid {
            start: 0,
            step: 1,
            kmax: 100,
        };
        // Bare AND `by (id)`: the grouped shape is the one that would
        // build 501 groups if `k` were consulted after the group.
        let by_id = Grouping {
            kind: GroupingKind::By,
            labels: vec!["id".to_string()],
        };
        let shapes = [None, Some(by_id)];
        for op in [VectorAggOp::Topk, VectorAggOp::Bottomk] {
            for grouping in &shapes {
                let mut fold = VectorAggFold::new(
                    &(op, grouping.clone(), Some(0.0)),
                    grid,
                    MAX_METRIC_RESULT_POINTS,
                    u64::MAX,
                )
                .expect("k == 0 still folds");
                for i in 0..501u32 {
                    let labels = fold_labels(&[("id", &i.to_string())]);
                    let points: Vec<(i64, f64)> =
                        (0..101).map(|k| (k, (i + k as u32) as f64)).collect();
                    fold.push_series(&labels, &points).expect("no-op push");
                }
                // The RESOURCE claim first, so a mutant that keeps a real
                // selection state at `k == 0` fails on what actually matters
                // rather than on the enum's shape.
                assert_eq!(fold.groups(), 0, "{op:?}: no group may be constructed");
                assert_eq!(fold.cells(), 0, "{op:?}: no cell may be retained");
                assert!(
                    matches!(fold, VectorAggFold::Empty),
                    "{op:?}: k == 0 is the structurally-empty fold"
                );
                assert!(fold.finish().is_empty(), "{op:?}: the result is empty");
            }
        }
    }

    /// A point that is not on the query grid is an internal-invariant
    /// breach and is REPORTED, never silently dropped — a dropped point
    /// is a silently wrong result.
    #[test]
    fn a_point_off_the_query_grid_is_an_error_not_a_dropped_point() {
        let grid = FoldGrid {
            start: 100,
            step: 10,
            kmax: 4,
        };
        assert_eq!(grid.index_of(100), Some(0));
        assert_eq!(grid.index_of(140), Some(4));
        assert_eq!(grid.index_of(150), None, "past kmax");
        assert_eq!(grid.index_of(105), None, "between grid points");
        assert_eq!(grid.index_of(90), None, "before the grid");
        for k in 0..=4usize {
            assert_eq!(grid.index_of(grid.point(k)), Some(k), "point/index inverse");
        }
        let mut fold = VectorAggFold::new(
            &(VectorAggOp::Sum, None, None),
            grid,
            MAX_METRIC_RESULT_POINTS,
            u64::MAX,
        )
        .expect("sum folds");
        match fold.push_series(&Vec::new(), &[(105, 1.0)]) {
            Err(ReadError::PipelineInvalid { reason }) => {
                assert!(reason.contains("off the query grid"), "{reason}");
            }
            other => panic!("expected an off-grid error, got {other:?}"),
        }
    }
}
