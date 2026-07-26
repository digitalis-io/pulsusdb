//! Byte-exact count-min-sketch port for `approx_topk` (issue #221).
//!
//! In the reference, `approx_topk(k, inner)` is rewritten to
//! `topk(k, CountMinSketchEval(__count_min_sketch__(inner)))`
//! (pkg/logql/optimize.go): every inner series' value is replaced by a
//! count-min-sketch **estimate** keyed by that series' label bytes, and
//! ordinary `topk` selects over the estimates. This module holds the pure
//! sketch/retention port; `exec::approx_topk_instant` drives it.
//!
//! Every constant here is a **reference constant**, none is a PulsusDB
//! tuning knob, and every algorithm is a line-for-line port:
//!
//! - geometry: `NewCountMinSketchFromErrorAndProbability(0.0001, 0.01)`
//!   (pkg/logql/count_min_sketch.go epsilon/delta through
//!   pkg/logql/sketch/cms.go) — verified by executing the reference
//!   package: `Width=27183, Depth=7`;
//! - hashing: `hashn` (pkg/logql/sketch/hash.go) — FNV-1a 32 and the
//!   inlined Jenkins one-at-a-time hash, both WRAPPING `u32`;
//! - cells: `getPos` (sketch/cms.go) — `(h1 + i*h2) % width` with the
//!   `h1 + i*h2` computed in wrapping `u32` BEFORE the modulo (the wrap
//!   is load-bearing: a wide-integer version picks different cells);
//! - key bytes: `stableBytes` (pkg/logql/count_min_sketch.go) — `0xFE`,
//!   then each label as `name 0xFF value`, `0xFF`-joined, labels sorted
//!   by name, NO trailing separator. Deliberately the *slicelabels*
//!   `labels.Bytes` form so it is build-tag independent in the reference
//!   (the checkout's default `labels.Bytes` is the different,
//!   length-prefixed *stringlabels* form — NOT this encoding);
//! - estimator: `Count` (sketch/cms.go) — the MIN over the rows, with
//!   the reference's exact `float64(math.MaxUint64)` sentinel;
//! - retention: `HeapCountMinSketchVector.Add`
//!   (pkg/logql/count_min_sketch.go) — sketch-add FIRST, then the
//!   `xxhash.Sum64(stableBytes)` dedup-on-retention set, the
//!   `heap.Fix(v, 0)` root-refresh branch, and the `> maxLabels`
//!   pop-min + observed-id delete. The heap is Go `container/heap`'s
//!   sift algorithm, ordered by the CURRENT sketch estimate (the
//!   reference's `Less` recomputes `Count` per comparison).
//!
//! MEMORY (the #227 discipline, by construction): nothing here allocates
//! per input series. Keys are streamed into all three hashes in one pass
//! ([`KeyHasher`] — the `stableBytes` sequence is never materialized), a
//! [`SeriesKey`] is 16 `Copy` bytes, and the two retention containers are
//! `with_capacity`-reserved at the compile-time `CMS_MAX_LABELS + 1` and
//! never grow past it. See the accounting table on
//! `exec::approx_topk_instant`.

use std::collections::HashSet;

use xxhash_rust::xxh64::Xxh64;

/// `ceil(E / 0.0001)` — reference `epsilon = 0.0001`
/// (pkg/logql/count_min_sketch.go) through
/// `NewCountMinSketchFromErrorAndProbability` (pkg/logql/sketch/cms.go).
/// Pinned as a literal, not recomputed: the reference computes it in f64
/// and the value is fixed for the pinned v3.7.4 reference (verified by
/// executing that function). The derivation is unit-pinned below.
pub(super) const CMS_WIDTH: u32 = 27_183;

/// `ceil(ln(0.01) / ln(0.5))` — reference `delta = 0.01`.
pub(super) const CMS_DEPTH: u32 = 7;

/// Reference `max_count_min_sketch_heap_size` default
/// (pkg/logql/engine.go): the retention cap on label sets. Below it the
/// heap is inert (no eviction) and the retained set equals the input set.
pub(super) const CMS_MAX_LABELS: usize = 10_000;

/// The three per-series hash digests, all computed over the reference's
/// `stableBytes` sequence in ONE streaming pass: the two sketch cell
/// hashes (`hashn`) and the retention-dedup id (`xxhash.Sum64`). 16
/// bytes, `Copy` — cached only for RETAINED series (inside the heap
/// entries), never per input series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SeriesKey {
    /// FNV-1a 32 (reference `hashn` h1).
    pub(super) h1: u32,
    /// Jenkins one-at-a-time (reference `hashn` h2).
    pub(super) h2: u32,
    /// `xxhash64` seed 0 (reference `xxhash.Sum64`) — the
    /// dedup-on-retention id.
    pub(super) id: u64,
}

/// Streams the `stableBytes` byte sequence into FNV-1a32, Jenkins-OAAT
/// and `Xxh64` simultaneously, allocating NOTHING (the reference itself
/// reuses one buffer for the same reason). `Xxh64::new(0)` /
/// `update` / `digest` is the streaming form of the one-shot
/// `xxh64(bytes, 0)` — byte-identical to `cespare/xxhash/v2.Sum64`.
struct KeyHasher {
    fnv: u32,
    jen: u32,
    xx: Xxh64,
}

impl KeyHasher {
    fn new() -> Self {
        KeyHasher {
            // FNV-1a 32-bit offset basis.
            fnv: 0x811c_9dc5,
            jen: 0,
            xx: Xxh64::new(0),
        }
    }

    /// Feeds `bytes` into all three hashes — the per-byte FNV/Jenkins
    /// steps are the reference's `hashn` loop body, in wrapping `u32`
    /// (the `<<` shifts drop high bits exactly as Go's `uint32` does).
    fn feed(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.fnv = (self.fnv ^ u32::from(b)).wrapping_mul(0x0100_0193);
            self.jen = self.jen.wrapping_add(u32::from(b));
            self.jen = self.jen.wrapping_add(self.jen << 10);
            self.jen ^= self.jen >> 6;
        }
        self.xx.update(bytes);
    }

    /// The Jenkins final avalanche (reference `hashn` tail), then the
    /// three digests.
    fn finish(mut self) -> SeriesKey {
        self.jen = self.jen.wrapping_add(self.jen << 3);
        self.jen ^= self.jen >> 11;
        self.jen = self.jen.wrapping_add(self.jen << 15);
        SeriesKey {
            h1: self.fnv,
            h2: self.jen,
            id: self.xx.digest(),
        }
    }
}

/// Hashes a label set's `stableBytes` sequence — `0xFE`, then each
/// `name 0xFF value` joined by `0xFF`, NO trailing separator — without
/// materializing it. `labels` MUST be name-sorted (the reference's
/// `labels.Labels` always is; `approx_topk_instant` normalizes in
/// place before calling).
pub(super) fn series_key(labels: &[(String, String)]) -> SeriesKey {
    let mut h = KeyHasher::new();
    h.feed(&[0xFE]);
    for (i, (name, value)) in labels.iter().enumerate() {
        if i > 0 {
            h.feed(&[0xFF]);
        }
        h.feed(name.as_bytes());
        h.feed(&[0xFF]);
        h.feed(value.as_bytes());
    }
    h.finish()
}

/// The fixed `CMS_DEPTH x CMS_WIDTH` f64 counter grid, row-major. One
/// fixed allocation of `7 * 27183 * 8` = 1_522_248 bytes per query
/// (calloc-backed zero pages), independent of input.
pub(super) struct CountMinSketch {
    counters: Vec<f64>,
}

impl CountMinSketch {
    pub(super) fn new() -> Self {
        CountMinSketch {
            counters: vec![0.0; (CMS_DEPTH * CMS_WIDTH) as usize],
        }
    }

    /// `getPos` (pkg/logql/sketch/cms.go): `pos_i = (h1 +w i*h2) %
    /// CMS_WIDTH` with WRAPPING u32 arithmetic before the modulo — the
    /// wrap is load-bearing (pinned by the reference-verified collision
    /// fixtures below: a wide-integer version picks different cells).
    fn positions(key: SeriesKey) -> [u32; CMS_DEPTH as usize] {
        std::array::from_fn(|i| key.h1.wrapping_add((i as u32).wrapping_mul(key.h2)) % CMS_WIDTH)
    }

    /// Non-conservative `Add` (sketch/cms.go): `+= count` in EVERY row
    /// (`ConservativeAdd` is a different, unwired reference path).
    pub(super) fn add(&mut self, key: SeriesKey, count: f64) {
        for (row, pos) in Self::positions(key).into_iter().enumerate() {
            self.counters[row * CMS_WIDTH as usize + pos as usize] += count;
        }
    }

    /// `Count` (sketch/cms.go): the MIN over the rows, including the
    /// reference's exact `float64(math.MaxUint64)` starting sentinel and
    /// strict `<` comparison (a NaN cell is never selected, exactly as
    /// in Go).
    pub(super) fn count(&self, key: SeriesKey) -> f64 {
        let mut min_val = u64::MAX as f64;
        for (row, pos) in Self::positions(key).into_iter().enumerate() {
            let cell = self.counters[row * CMS_WIDTH as usize + pos as usize];
            if cell < min_val {
                min_val = cell;
            }
        }
        min_val
    }

    /// Grid size in bytes — pinned by the geometry unit test and charged
    /// in `approx_topk_instant`'s accounting table.
    #[cfg(test)]
    fn grid_bytes(&self) -> usize {
        self.counters.len() * std::mem::size_of::<f64>()
    }
}

/// The retained-label-set cap structure — the port of
/// `HeapCountMinSketchVector`'s heap + `observed` set
/// (pkg/logql/count_min_sketch.go). Entries are `(input index,
/// SeriesKey)` — 24 `Copy` bytes; label sets are NEVER cloned into the
/// heap. Both containers are reserved once at `CMS_MAX_LABELS + 1` (the
/// transient size after a push, before the evict) and never grow past
/// it.
pub(super) struct Retention {
    /// Go `container/heap` min-heap over the CURRENT sketch estimate.
    heap: Vec<(u32, SeriesKey)>,
    /// `observed`: `xxhash.Sum64(stableBytes)` ids of the RETAINED label
    /// sets — the dedup-on-retention set. A label set whose id matches
    /// an already-retained one is added to the sketch (by the caller,
    /// before [`Retention::observe`]) but never retained; an evicted
    /// set's id is deleted, so it can be re-retained if seen again.
    observed: HashSet<u64>,
}

impl Retention {
    pub(super) fn new() -> Self {
        Retention {
            heap: Vec::with_capacity(CMS_MAX_LABELS + 1),
            observed: HashSet::with_capacity(CMS_MAX_LABELS + 1),
        }
    }

    /// Steps 2-4 of the reference `HeapCountMinSketchVector.Add` — the
    /// caller performs step 1 (`sketch.add`) FIRST; the sketch addition
    /// happens unconditionally, whatever the retention decision (pinned
    /// by `retention_dedups_on_the_reference_hash_and_still_adds_to_the_sketch`).
    ///
    /// `same_labels_as(root_idx)` is the reference's
    /// `labels.Equal(v.Metrics[0], metric)` — true iff the incoming
    /// label set equals the current heap root's.
    pub(super) fn observe(
        &mut self,
        idx: u32,
        key: SeriesKey,
        sketch: &CountMinSketch,
        mut same_labels_as: impl FnMut(u32) -> bool,
    ) {
        if !self.observed.contains(&key.id) {
            // heap.Push + observed insert.
            self.heap.push((idx, key));
            self.sift_up(self.heap.len() - 1, sketch);
            self.observed.insert(key.id);
        } else if let Some(&(root, _)) = self.heap.first()
            && same_labels_as(root)
        {
            // heap.Fix(v, 0): the root's estimate may have grown with
            // the duplicate's sketch addition. Go's Fix is
            // `if !down(i) { up(i) }`; at the root, `up` is a no-op.
            self.sift_down(0, sketch);
        }
        if self.heap.len() > CMS_MAX_LABELS {
            // heap.Pop the minimum and delete its observed id.
            let evicted = self.pop_min(sketch);
            self.observed.remove(&evicted.1.id);
        }
    }

    /// The retained entries, heap-order (the caller re-orders by input
    /// index; the reference's own emission order is its heap array).
    pub(super) fn into_entries(self) -> Vec<(u32, SeriesKey)> {
        self.heap
    }

    /// Reference `Less`: compare CURRENT sketch estimates (`Count` is
    /// recomputed per comparison against the evolving sketch).
    fn less(&self, a: usize, b: usize, sketch: &CountMinSketch) -> bool {
        sketch.count(self.heap[a].1) < sketch.count(self.heap[b].1)
    }

    /// Go `container/heap` `up`.
    fn sift_up(&mut self, mut j: usize, sketch: &CountMinSketch) {
        while j > 0 {
            let i = (j - 1) / 2;
            if !self.less(j, i, sketch) {
                break;
            }
            self.heap.swap(i, j);
            j = i;
        }
    }

    /// Go `container/heap` `down` over the first `n` entries.
    fn sift_down(&mut self, i0: usize, sketch: &CountMinSketch) {
        let n = self.heap.len();
        self.sift_down_bounded(i0, n, sketch);
    }

    fn sift_down_bounded(&mut self, i0: usize, n: usize, sketch: &CountMinSketch) {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            let mut j = j1;
            let j2 = j1 + 1;
            if j2 < n && self.less(j2, j1, sketch) {
                j = j2;
            }
            if !self.less(j, i, sketch) {
                break;
            }
            self.heap.swap(i, j);
            i = j;
        }
    }

    /// Go `heap.Pop`: swap root/last, sift the new root down over
    /// `len-1`, remove and return the old root.
    fn pop_min(&mut self, sketch: &CountMinSketch) -> (u32, SeriesKey) {
        let n = self.heap.len() - 1;
        self.heap.swap(0, n);
        self.sift_down_bounded(0, n, sketch);
        self.heap.pop().expect("pop_min on a non-empty heap")
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.heap.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test-only materialization of the reference `stableBytes`
    /// (pkg/logql/count_min_sketch.go) — the production path streams the
    /// SAME sequence through [`KeyHasher`] without allocating;
    /// `series_key_hashes_the_materialized_stable_bytes` pins that the
    /// two cannot drift.
    fn stable_label_bytes(labels: &[(&str, &str)]) -> Vec<u8> {
        let mut out = vec![0xFEu8];
        for (i, (name, value)) in labels.iter().enumerate() {
            if i > 0 {
                out.push(0xFF);
            }
            out.extend_from_slice(name.as_bytes());
            out.push(0xFF);
            out.extend_from_slice(value.as_bytes());
        }
        out
    }

    fn owned(labels: &[(&str, &str)]) -> Vec<(String, String)> {
        labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn lvl_key(token: &str) -> SeriesKey {
        series_key(&owned(&[("lvl", token)]))
    }

    /// Reference `hashn` (pkg/logql/sketch/hash.go) over a materialized
    /// key — the one-shot form the streaming [`KeyHasher`] must equal.
    fn hashn(key: &[u8]) -> (u32, u32) {
        let mut h1: u32 = 0x811c_9dc5;
        for &b in key {
            h1 = (h1 ^ u32::from(b)).wrapping_mul(0x0100_0193);
        }
        let mut h2: u32 = 0;
        for &b in key {
            h2 = h2.wrapping_add(u32::from(b));
            h2 = h2.wrapping_add(h2 << 10);
            h2 ^= h2 >> 6;
        }
        h2 = h2.wrapping_add(h2 << 3);
        h2 ^= h2 >> 11;
        h2 = h2.wrapping_add(h2 << 15);
        (h1, h2)
    }

    #[test]
    fn sketch_geometry_is_the_reference_constants() {
        assert_eq!(CMS_WIDTH, 27_183);
        assert_eq!(CMS_DEPTH, 7);
        assert_eq!(CMS_MAX_LABELS, 10_000);
        // The reference derivations reproduce the pinned literals:
        // width = ceil(E/epsilon), depth = ceil(ln(delta)/ln(0.5))
        // (pkg/logql/sketch/cms.go, epsilon/delta from
        // pkg/logql/count_min_sketch.go).
        assert_eq!((std::f64::consts::E / 0.0001).ceil() as u32, CMS_WIDTH);
        assert_eq!((0.01f64.ln() / 0.5f64.ln()).ceil() as u32, CMS_DEPTH);
        assert_eq!(CountMinSketch::new().grid_bytes(), 1_522_248);
    }

    /// The byte vector was captured by executing the reference
    /// `stableBytes` (`git describe` = v3.7.4): `0xFE`, name, `0xFF`,
    /// value — and for two labels a single `0xFF` join with NO trailing
    /// separator.
    #[test]
    fn stable_label_bytes_matches_the_reference_encoding() {
        assert_eq!(
            stable_label_bytes(&[("lvl", "ctrl")]),
            vec![0xFE, b'l', b'v', b'l', 0xFF, b'c', b't', b'r', b'l'],
        );
        assert_eq!(
            stable_label_bytes(&[("a", "1"), ("b", "2")]),
            vec![0xFE, b'a', 0xFF, b'1', 0xFF, b'b', 0xFF, b'2'],
        );
    }

    /// The streaming hasher equals the one-shot hashes of the
    /// materialized key — so the zero-allocation path cannot drift from
    /// the reference encoding.
    #[test]
    fn series_key_hashes_the_materialized_stable_bytes() {
        for labels in [
            vec![("lvl", "ctrl")],
            vec![("a", "1"), ("b", "2")],
            vec![("app", "x"), ("lvl", "err"), ("region", "eu")],
        ] {
            let bytes = stable_label_bytes(&labels);
            let key = series_key(&owned(&labels));
            assert_eq!((key.h1, key.h2), hashn(&bytes));
            assert_eq!(key.id, xxhash_rust::xxh64::xxh64(&bytes, 0));
        }
    }

    /// Reference-verified full-7-row collision (executed against the
    /// v3.7.4 `pkg/logql` package): `{lvl="v0095169"}` (true 3) and
    /// `{lvl="v0125949"}` (true 4) share ALL SEVEN cells, so both report
    /// 7; `{lvl="ctrl"}` (true 5) is clean and reports 5. This single
    /// test fails for any wrong width, depth, hash function, wrapping
    /// rule or key encoding — values `topk` can never produce.
    #[test]
    fn full_row_collision_reproduces_the_reference_over_estimate() {
        let mut s = CountMinSketch::new();
        s.add(lvl_key("v0095169"), 3.0);
        s.add(lvl_key("v0125949"), 4.0);
        s.add(lvl_key("ctrl"), 5.0);
        assert_eq!(s.count(lvl_key("v0095169")), 7.0);
        assert_eq!(s.count(lvl_key("v0125949")), 7.0);
        assert_eq!(s.count(lvl_key("ctrl")), 5.0);
    }

    /// Second reference-verified full-collision pair: `{lvl="v0054587"}`
    /// (true 2) and `{lvl="v0156565"}` (true 6) both report 8.
    #[test]
    fn second_collision_pair_reproduces_the_reference_over_estimate() {
        let mut s = CountMinSketch::new();
        s.add(lvl_key("v0054587"), 2.0);
        s.add(lvl_key("v0156565"), 6.0);
        assert_eq!(s.count(lvl_key("v0054587")), 8.0);
        assert_eq!(s.count(lvl_key("v0156565")), 8.0);
    }

    /// Dataset P — the MIN-estimator pin (a full-row collision cannot
    /// distinguish the estimator: every row is equally inflated).
    /// Anchor `{lvl="pmin"}` (true 10) plus six single-row colliders
    /// (weight i+1 on row i, rows 0..5; row 6 clean), so its row cells
    /// ladder `11,12,13,14,15,16,10` — reference-verified by reading
    /// `CountMinSketch.Counters` at the predicted positions (which also
    /// independently re-validates the wrapping-u32 `getPos` port).
    ///
    /// What each WRONG implementation returns for `pmin` (the
    /// discriminating table): `max` -> 16, `sum` -> 91, row 0 -> 11,
    /// any fixed row 1..5 -> 12..16, row 6 -> 10 (caught by dataset Q
    /// below), plain `topk` (no sketch) -> 10 (caught by the
    /// full-collision fixtures above). Only `min` returns 10 here AND 8
    /// on Q.
    #[test]
    fn partial_collision_ladder_pins_the_min_estimator() {
        let mut s = CountMinSketch::new();
        let colliders: [(&str, f64); 6] = [
            ("c016000", 1.0), // shares pmin's cell in row 0 only
            ("c007375", 2.0), // row 1
            ("c071035", 3.0), // row 2
            ("c045067", 4.0), // row 3
            ("c007616", 5.0), // row 4
            ("c001130", 6.0), // row 5
        ];
        s.add(lvl_key("pmin"), 10.0);
        for (tok, w) in colliders {
            s.add(lvl_key(tok), w);
        }
        assert_eq!(
            s.count(lvl_key("pmin")),
            10.0,
            "min over the reference-verified ladder 11,12,13,14,15,16,10 \
             — a `max` port returns 16, a `sum` port 91, a fixed row 0..5 \
             port 11..16"
        );
        // No collider-collider cell is shared (search-verified), so each
        // collider's own estimate is exact — reference-verified 1..6.
        for (tok, w) in colliders {
            assert_eq!(s.count(lvl_key(tok)), w);
        }
    }

    /// Dataset Q — the complementary ladder: anchor `{lvl="qmin"}`
    /// (true 8) with ONE collider on row 6 only (weight 3), so its cells
    /// are `8,8,8,8,8,8,11` — a last-row port returns 11 (dataset P
    /// cannot catch that one), `max` -> 11, `sum` -> 59; `min` -> 8
    /// (reference-verified).
    #[test]
    fn partial_collision_on_the_last_row_pins_min_over_a_row6_port() {
        let mut s = CountMinSketch::new();
        s.add(lvl_key("qmin"), 8.0);
        s.add(lvl_key("c003087"), 3.0);
        assert_eq!(
            s.count(lvl_key("qmin")),
            8.0,
            "min over the reference-verified cells 8x6 + 11 — a row-6 \
             (last-row) port returns 11, `max` 11, `sum` 59"
        );
        assert_eq!(s.count(lvl_key("c003087")), 3.0);
    }

    /// The reference dedups retention on `xxhash.Sum64(stableBytes)` but
    /// adds to the sketch FIRST, unconditionally: a repeated label set
    /// is added to the sketch twice (the estimate is the sum) yet
    /// retained once — the operation order a naive port gets wrong.
    #[test]
    fn retention_dedups_on_the_reference_hash_and_still_adds_to_the_sketch() {
        let mut sketch = CountMinSketch::new();
        let mut retention = Retention::new();
        let key = lvl_key("dup");
        // First sighting: add, then retain.
        sketch.add(key, 3.0);
        retention.observe(0, key, &sketch, |_| true);
        // Second sighting of the SAME label set: the sketch add still
        // happens (before the retention decision) …
        sketch.add(key, 4.0);
        retention.observe(1, key, &sketch, |_| true);
        // … so the estimate is the sum, but the set is retained once.
        assert_eq!(sketch.count(key), 7.0);
        assert_eq!(retention.len(), 1);
        let entries = retention.into_entries();
        assert_eq!(entries, vec![(0, key)], "the FIRST sighting is retained");
    }

    /// Above `CMS_MAX_LABELS` the heap evicts the minimum-estimate entry
    /// and deletes its observed id (the reference's pop + delete);
    /// exactly `CMS_MAX_LABELS` entries survive.
    #[test]
    fn retention_cap_is_the_reference_heap_size_and_drops_the_minimum() {
        let mut sketch = CountMinSketch::new();
        let mut retention = Retention::new();
        // CMS_MAX_LABELS + 1 distinct sets with distinct weights 1..=N+1:
        // the weight-1 entry is the unique minimum estimate (collisions
        // only inflate other entries, never deflate the minimum below 1).
        let n = CMS_MAX_LABELS + 1;
        for i in 0..n {
            let key = lvl_key(&format!("s{i:05}"));
            // The production interleaving: sketch add FIRST, then the
            // retention decision, per series.
            sketch.add(key, (i + 1) as f64);
            retention.observe(i as u32, key, &sketch, |_| false);
        }
        assert_eq!(retention.len(), CMS_MAX_LABELS);
        let retained: HashSet<u32> = retention.into_entries().iter().map(|e| e.0).collect();
        assert!(
            !retained.contains(&0),
            "the minimum-estimate entry (weight 1) is the one dropped"
        );
        assert_eq!(retained.len(), CMS_MAX_LABELS);
    }

    /// An evicted id is deleted from `observed`, so the same label set
    /// can be re-retained later (the reference deletes on pop).
    #[test]
    fn eviction_deletes_the_observed_id_allowing_re_retention() {
        let mut sketch = CountMinSketch::new();
        let mut retention = Retention::new();
        let small = lvl_key("tiny");
        sketch.add(small, 1.0);
        retention.observe(0, small, &sketch, |_| false);
        for i in 0..CMS_MAX_LABELS {
            let key = lvl_key(&format!("b{i:05}"));
            sketch.add(key, 100.0);
            retention.observe((i + 1) as u32, key, &sketch, |_| false);
        }
        // `tiny` (estimate 1, the minimum) was evicted when the cap was
        // crossed; its id is gone, so a re-sighting is re-retainABLE. A
        // re-sight at the old minimum estimate would be pushed and then
        // immediately re-evicted (the heap is at cap — reference
        // behaviour); a re-sight whose estimate has grown past the
        // current minimum survives, which is only possible because the
        // eviction deleted the observed id (a stale id would dedup-drop
        // it before the heap ever saw it).
        sketch.add(small, 1_000.0);
        retention.observe(9_999_999, small, &sketch, |_| false);
        let ids: Vec<u32> = retention.into_entries().iter().map(|e| e.0).collect();
        assert!(
            ids.contains(&9_999_999),
            "a re-sighted evicted set must be re-retainable"
        );
        assert!(
            !ids.contains(&0),
            "the original sighting stays evicted — the re-sight is a fresh entry"
        );
    }

    /// The `heap.Fix(v, 0)` branch: a duplicate of the heap ROOT
    /// re-sifts it down after its estimate grew, so a later eviction
    /// drops the TRUE minimum rather than the stale root.
    #[test]
    fn duplicate_of_the_root_refreshes_the_heap_order() {
        let mut sketch = CountMinSketch::new();
        let mut retention = Retention::new();
        let a = lvl_key("root-a");
        let b = lvl_key("other-b");
        sketch.add(a, 1.0);
        retention.observe(0, a, &sketch, |_| false);
        sketch.add(b, 2.0);
        retention.observe(1, b, &sketch, |_| false);
        // Root is `a` (estimate 1). A duplicate sighting of `a` raises
        // its estimate to 4 — the Fix branch must re-sift so `b`
        // becomes the root/minimum.
        sketch.add(a, 3.0);
        retention.observe(2, a, &sketch, |root| root == 0);
        let entries = retention.into_entries();
        assert_eq!(entries.len(), 2, "the duplicate is not re-retained");
        assert_eq!(
            entries[0],
            (1, b),
            "after heap.Fix the root is the true minimum (b, estimate 2)"
        );
    }
}
