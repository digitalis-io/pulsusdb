//! `LruSet<K>`: a hand-rolled, bounded LRU cache of keys populated ONLY on a
//! successful flush (architect plan amendment 1) — no `lru` crate
//! dependency (lean-deps ethos, matching the workspace's other hand-rolled
//! small-data-structure precedents, e.g. `pulsus-model`'s civil-calendar
//! conversion). Generalized from the original `StreamLru` (issue #26
//! architect plan: "the key type is the only difference" between the log
//! and metric registration gates) — `K: Clone + Eq + Hash` rather than
//! `Copy`, since the metric-series key carries an `Arc<str>` metric name.
//!
//! O(1) `contains`/`insert`/eviction via an intrusive doubly-linked list
//! over a slab (`Vec<Slot>`), indexed by a `HashMap<K, usize>`. A false miss
//! is always harmless here (architect plan amendment 1): it just re-emits a
//! registration row that either `ReplacingMergeTree` (`log_streams`) or a
//! read-time `LIMIT 1 BY` (`metric_series`, docs/schemas.md §2.1) collapses,
//! so this cache trades a little redundant writing for a simple,
//! well-understood eviction policy — never correctness. Optimistic
//! promotion is deliberately NOT implemented: `insert` is called only after
//! a row's flush is confirmed `Ok` (see `crate::writer::table`'s
//! `on_flush_success` hook), never at admission time.
//!
//! This module also defines [`MetadataCache`] — not an `LruSet`, but a
//! bounded last-*value* cache (`metric_name -> (metric_type, help, unit)`)
//! built on top of one, for `metric_metadata`'s "emit iff the value changed"
//! semantics (architect plan amendment 1, finding 2: a plain
//! `(metric_name, metric_type)` set would permanently suppress a type
//! reverting A→B→A after the first A). See its own doc comment.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

/// `(fingerprint, month-as-days-since-epoch)` — the same key
/// `docs/schemas.md §3.1`'s monthly `log_streams` partitions dedup on.
pub type StreamKey = (u64, u16);
/// `LruSet<StreamKey>` — unchanged behavior/name from before issue #26's
/// generalization; every existing `log_streams` callsite keeps compiling
/// against this alias.
pub type StreamLru = LruSet<StreamKey>;

/// `(metric_name, fingerprint, bucket_floor_ms)` — the `metric_series`
/// registration-skip key (docs/schemas.md §2.1, issue #26 open question #1
/// resolution: metric-name-scoped, NOT `(fingerprint, bucket)` alone).
/// `metric_fingerprint` excludes `__name__` (`pulsus_model::fingerprint`),
/// so two differently-named metrics sharing a label set share a
/// fingerprint; a name-less key would let one metric's registration
/// false-hit-suppress the other's `metric_series` row. `Arc<str>` (not
/// `String`): the admission hot path already holds an `Arc<str>` metric
/// name (from `MetricPoint`/`SeriesRef`), so building this key per sample
/// is a cheap `Arc` clone, not a fresh heap allocation.
///
/// The trailing `u8` is `value_type` (M7-A4, issue #120: `0` = float, `1` =
/// histogram). It is part of the key so a series that carries BOTH a float
/// and a histogram sample in the same activity bucket registers **both**
/// `metric_series` rows (the per-series float/histogram discriminator) —
/// they are distinct keys, not a false LRU hit that would suppress one.
pub type SeriesKey = (Arc<str>, u64, i64, u8);
pub type SeriesLru = LruSet<SeriesKey>;

struct Slot<K> {
    key: K,
    prev: Option<usize>,
    next: Option<usize>,
}

pub struct LruSet<K> {
    capacity: usize,
    index: HashMap<K, usize>,
    slots: Vec<Slot<K>>,
    free: Vec<usize>,
    /// Most-recently-used slot index.
    head: Option<usize>,
    /// Least-recently-used slot index — the next eviction victim.
    tail: Option<usize>,
}

impl<K: Clone + Eq + Hash> LruSet<K> {
    /// `capacity` is floored at 1 — a zero-capacity cache would make every
    /// `insert` immediately evict what it just inserted, an edge case not
    /// worth threading through the eviction logic.
    pub fn new(capacity: usize) -> Self {
        LruSet {
            capacity: capacity.max(1),
            index: HashMap::new(),
            slots: Vec::new(),
            free: Vec::new(),
            head: None,
            tail: None,
        }
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// `true` if `key` is cached, promoting it to most-recently-used.
    pub fn contains(&mut self, key: &K) -> bool {
        match self.index.get(key).copied() {
            Some(idx) => {
                self.move_to_front(idx);
                true
            }
            None => false,
        }
    }

    /// Records `key` as known (architect plan amendment 1: called only
    /// after a successful confirming flush — see this module's doc
    /// comment). A key already present is just promoted; a new key at
    /// capacity evicts the least-recently-used entry first. Equivalent to
    /// [`Self::insert_evicting`], discarding the evicted key.
    pub fn insert(&mut self, key: K) {
        let _ = self.insert_evicting(key);
    }

    /// As [`Self::insert`], but returns the key evicted to make room, if
    /// any — used by [`MetadataCache`] to keep its side-table of values in
    /// sync with this set's eviction decisions.
    pub(crate) fn insert_evicting(&mut self, key: K) -> Option<K> {
        if let Some(&idx) = self.index.get(&key) {
            self.move_to_front(idx);
            return None;
        }
        let evicted = if self.index.len() >= self.capacity {
            self.evict_lru()
        } else {
            None
        };
        let idx = self.alloc_slot(key.clone());
        self.index.insert(key, idx);
        self.push_front(idx);
        evicted
    }

    fn alloc_slot(&mut self, key: K) -> usize {
        let slot = Slot {
            key,
            prev: None,
            next: None,
        };
        if let Some(idx) = self.free.pop() {
            self.slots[idx] = slot;
            idx
        } else {
            self.slots.push(slot);
            self.slots.len() - 1
        }
    }

    fn evict_lru(&mut self) -> Option<K> {
        let tail = self.tail?;
        self.unlink(tail);
        let key = self.slots[tail].key.clone();
        self.index.remove(&key);
        self.free.push(tail);
        Some(key)
    }

    fn unlink(&mut self, idx: usize) {
        let (prev, next) = (self.slots[idx].prev, self.slots[idx].next);
        match prev {
            Some(p) => self.slots[p].next = next,
            None => self.head = next,
        }
        match next {
            Some(n) => self.slots[n].prev = prev,
            None => self.tail = prev,
        }
        self.slots[idx].prev = None;
        self.slots[idx].next = None;
    }

    fn push_front(&mut self, idx: usize) {
        self.slots[idx].prev = None;
        self.slots[idx].next = self.head;
        if let Some(h) = self.head {
            self.slots[h].prev = Some(idx);
        }
        self.head = Some(idx);
        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }

    fn move_to_front(&mut self, idx: usize) {
        if self.head == Some(idx) {
            return;
        }
        self.unlink(idx);
        self.push_front(idx);
    }
}

/// A `metric_name`'s last durably-emitted `(metric_type, help, unit)`
/// descriptor tuple.
pub type MetadataValue = (String, String, String);

/// A bounded last-*value* cache: `metric_name -> (metric_type, help,
/// unit)`, the descriptor `metric_metadata` last durably wrote for that
/// name (architect plan amendment 1, finding 2). Unlike [`LruSet`] (a pure
/// membership set), this must answer "does the *value* differ from what we
/// last emitted", so a type reverting A→B→A re-emits on the second A rather
/// than being permanently suppressed by a once-only `(metric_name,
/// metric_type)` membership key. Bounded by `METADATA_LRU_CAPACITY`
/// (`writer::config`); eviction just re-emits next time (harmless, collapsed
/// on read by `ReplacingMergeTree(updated_ns)`, docs/schemas.md §2.1).
///
/// Built on an [`LruSet<Arc<str>>`] purely for its eviction *policy*
/// (recency order + capacity), paired with a side `HashMap` holding the
/// actual last-emitted values — [`LruSet::insert_evicting`] reports which
/// key (if any) it evicted so this cache's value map never drifts out of
/// sync with the set's membership.
pub struct MetadataCache {
    order: LruSet<Arc<str>>,
    values: HashMap<Arc<str>, MetadataValue>,
}

impl MetadataCache {
    pub fn new(capacity: usize) -> Self {
        MetadataCache {
            order: LruSet::new(capacity),
            values: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// The last durably-emitted descriptor for `metric_name`, if any. A
    /// pure read — does NOT promote recency (only [`Self::upsert`], called
    /// exclusively on a confirmed `metric_metadata` flush, changes eviction
    /// order): an admission-time peek must never advance eviction state on
    /// behalf of a batch that has not yet been durably flushed.
    pub fn get(&self, metric_name: &Arc<str>) -> Option<&MetadataValue> {
        self.values.get(metric_name)
    }

    /// Records `value` as `metric_name`'s last durably-emitted descriptor
    /// (architect plan amendment 1: called only after a confirmed
    /// `metric_metadata` flush). Overwrites any previous value for the same
    /// name (A→B→A's second A must overwrite B, not merge with it) and
    /// evicts the least-recently-inserted name first once at capacity.
    pub fn upsert(&mut self, metric_name: Arc<str>, value: MetadataValue) {
        if let Some(evicted) = self.order.insert_evicting(metric_name.clone()) {
            self.values.remove(&evicted);
        }
        self.values.insert(metric_name, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn miss_on_an_empty_cache() {
        let mut lru: LruSet<StreamKey> = LruSet::new(10);
        assert!(!lru.contains(&(1, 1)));
    }

    #[test]
    fn hit_after_insert() {
        let mut lru: LruSet<StreamKey> = LruSet::new(10);
        lru.insert((1, 1));
        assert!(lru.contains(&(1, 1)));
        assert_eq!(lru.len(), 1);
    }

    #[test]
    fn distinct_months_for_the_same_fingerprint_are_distinct_keys() {
        let mut lru: LruSet<StreamKey> = LruSet::new(10);
        lru.insert((1, 1));
        assert!(lru.contains(&(1, 1)));
        assert!(!lru.contains(&(1, 2)));
    }

    #[test]
    fn re_inserting_a_known_key_does_not_grow_the_cache() {
        let mut lru: LruSet<StreamKey> = LruSet::new(10);
        lru.insert((1, 1));
        lru.insert((1, 1));
        assert_eq!(lru.len(), 1);
    }

    #[test]
    fn evicts_the_least_recently_used_entry_at_capacity() {
        let mut lru: LruSet<StreamKey> = LruSet::new(2);
        lru.insert((1, 1));
        lru.insert((2, 1));
        // Touch (1,1) so (2,1) becomes the LRU victim.
        assert!(lru.contains(&(1, 1)));
        lru.insert((3, 1));
        assert!(lru.contains(&(1, 1)));
        assert!(lru.contains(&(3, 1)));
        assert!(!lru.contains(&(2, 1)));
        assert_eq!(lru.len(), 2);
    }

    #[test]
    fn capacity_never_exceeded_across_many_inserts() {
        let mut lru: LruSet<StreamKey> = LruSet::new(100);
        for i in 0..1_000u64 {
            lru.insert((i, 0));
        }
        assert_eq!(lru.len(), 100);
    }

    #[test]
    fn zero_capacity_is_floored_to_one() {
        let mut lru: LruSet<StreamKey> = LruSet::new(0);
        lru.insert((1, 1));
        assert!(lru.contains(&(1, 1)));
        assert_eq!(lru.len(), 1);
    }

    #[test]
    fn series_key_is_scoped_by_metric_name_not_just_fingerprint() {
        // Regression for issue #26 open question #1: two different metric
        // names sharing a fingerprint (possible since `metric_fingerprint`
        // excludes `__name__`) must be distinct LRU keys.
        let mut lru: SeriesLru = LruSet::new(10);
        let a: Arc<str> = Arc::from("http_requests_total");
        let b: Arc<str> = Arc::from("http_errors_total");
        lru.insert((a.clone(), 42, 0, 0));
        assert!(lru.contains(&(a, 42, 0, 0)));
        assert!(!lru.contains(&(b, 42, 0, 0)));
    }

    #[test]
    fn series_key_value_type_distinguishes_float_from_histogram() {
        // Issue #120: the same (name, fp, bucket) with value_type 0 (float)
        // and 1 (histogram) are distinct keys — both metric_series rows must
        // register, never one suppressing the other.
        let mut lru: SeriesLru = LruSet::new(10);
        let name: Arc<str> = Arc::from("http_request_duration");
        lru.insert((name.clone(), 7, 0, 0));
        assert!(lru.contains(&(name.clone(), 7, 0, 0)));
        assert!(!lru.contains(&(name, 7, 0, 1)));
    }

    #[test]
    fn insert_evicting_reports_no_eviction_under_capacity() {
        let mut lru: LruSet<StreamKey> = LruSet::new(10);
        assert_eq!(lru.insert_evicting((1, 1)), None);
    }

    #[test]
    fn insert_evicting_reports_the_evicted_key_at_capacity() {
        let mut lru: LruSet<StreamKey> = LruSet::new(1);
        assert_eq!(lru.insert_evicting((1, 1)), None);
        assert_eq!(lru.insert_evicting((2, 1)), Some((1, 1)));
    }

    #[test]
    fn metadata_cache_miss_on_an_empty_cache() {
        let cache = MetadataCache::new(10);
        let name: Arc<str> = Arc::from("http_requests_total");
        assert_eq!(cache.get(&name), None);
    }

    #[test]
    fn metadata_cache_get_returns_the_last_upserted_value() {
        let mut cache = MetadataCache::new(10);
        let name: Arc<str> = Arc::from("http_requests_total");
        cache.upsert(
            name.clone(),
            ("counter".to_string(), "help".to_string(), "".to_string()),
        );
        assert_eq!(
            cache.get(&name),
            Some(&("counter".to_string(), "help".to_string(), "".to_string()))
        );
        assert_eq!(cache.len(), 1);
    }

    /// A→B→A: closes the review-cycle finding 2 gap — a type reverting to
    /// its original value must overwrite, not be swallowed by, an
    /// once-only membership key.
    #[test]
    fn metadata_cache_upsert_overwrites_a_changed_value() {
        let mut cache = MetadataCache::new(10);
        let name: Arc<str> = Arc::from("http_requests_total");
        cache.upsert(
            name.clone(),
            ("counter".to_string(), "".to_string(), "".to_string()),
        );
        cache.upsert(
            name.clone(),
            ("gauge".to_string(), "".to_string(), "".to_string()),
        );
        assert_eq!(
            cache.get(&name),
            Some(&("gauge".to_string(), "".to_string(), "".to_string()))
        );
        cache.upsert(
            name.clone(),
            ("counter".to_string(), "".to_string(), "".to_string()),
        );
        assert_eq!(
            cache.get(&name),
            Some(&("counter".to_string(), "".to_string(), "".to_string()))
        );
        assert_eq!(cache.len(), 1, "one name, overwritten in place");
    }

    #[test]
    fn metadata_cache_evicts_the_least_recently_upserted_name_at_capacity() {
        let mut cache = MetadataCache::new(1);
        let a: Arc<str> = Arc::from("metric_a");
        let b: Arc<str> = Arc::from("metric_b");
        cache.upsert(
            a.clone(),
            ("counter".to_string(), "".to_string(), "".to_string()),
        );
        cache.upsert(
            b.clone(),
            ("gauge".to_string(), "".to_string(), "".to_string()),
        );
        assert_eq!(cache.get(&a), None, "evicted to make room for b");
        assert!(cache.get(&b).is_some());
        assert_eq!(cache.len(), 1);
    }
}
