//! `StreamLru`: a hand-rolled, bounded LRU cache of `(fingerprint, month)`
//! keys populated ONLY on a successful `log_streams` flush (architect plan
//! amendment 1) — no `lru` crate dependency (lean-deps ethos, matching the
//! workspace's other hand-rolled small-data-structure precedents, e.g.
//! `pulsus-model`'s civil-calendar conversion).
//!
//! O(1) `contains`/`insert`/eviction via an intrusive doubly-linked list
//! over a slab (`Vec<Slot>`), indexed by a `HashMap<Key, usize>`. A false
//! miss is always harmless here (architect plan amendment 1): it just
//! re-emits a `log_streams` row that `ReplacingMergeTree` collapses, so
//! this cache trades a little redundant writing for a simple,
//! well-understood eviction policy — never correctness. Optimistic
//! promotion is deliberately NOT implemented: `insert` is called only
//! after a stream's flush is confirmed `Ok` (see `crate::writer::table`'s
//! `on_flush_success` hook), never at admission time.

use std::collections::HashMap;

/// `(fingerprint, month-as-days-since-epoch)` — the same key
/// `docs/schemas.md §3.1`'s monthly `log_streams` partitions dedup on.
pub type StreamKey = (u64, u16);

struct Slot {
    key: StreamKey,
    prev: Option<usize>,
    next: Option<usize>,
}

pub struct StreamLru {
    capacity: usize,
    index: HashMap<StreamKey, usize>,
    slots: Vec<Slot>,
    free: Vec<usize>,
    /// Most-recently-used slot index.
    head: Option<usize>,
    /// Least-recently-used slot index — the next eviction victim.
    tail: Option<usize>,
}

impl StreamLru {
    /// `capacity` is floored at 1 — a zero-capacity cache would make
    /// every `insert` immediately evict what it just inserted, an
    /// edge case not worth threading through the eviction logic.
    pub fn new(capacity: usize) -> Self {
        StreamLru {
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
    pub fn contains(&mut self, key: StreamKey) -> bool {
        match self.index.get(&key).copied() {
            Some(idx) => {
                self.move_to_front(idx);
                true
            }
            None => false,
        }
    }

    /// Records `key` as known (architect plan amendment 1: called only
    /// after a successful `log_streams` flush — see this module's doc
    /// comment). A key already present is just promoted; a new key at
    /// capacity evicts the least-recently-used entry first.
    pub fn insert(&mut self, key: StreamKey) {
        if let Some(&idx) = self.index.get(&key) {
            self.move_to_front(idx);
            return;
        }
        if self.index.len() >= self.capacity {
            self.evict_lru();
        }
        let idx = self.alloc_slot(key);
        self.index.insert(key, idx);
        self.push_front(idx);
    }

    fn alloc_slot(&mut self, key: StreamKey) -> usize {
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

    fn evict_lru(&mut self) {
        let Some(tail) = self.tail else {
            return;
        };
        self.unlink(tail);
        let key = self.slots[tail].key;
        self.index.remove(&key);
        self.free.push(tail);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn miss_on_an_empty_cache() {
        let mut lru = StreamLru::new(10);
        assert!(!lru.contains((1, 1)));
    }

    #[test]
    fn hit_after_insert() {
        let mut lru = StreamLru::new(10);
        lru.insert((1, 1));
        assert!(lru.contains((1, 1)));
        assert_eq!(lru.len(), 1);
    }

    #[test]
    fn distinct_months_for_the_same_fingerprint_are_distinct_keys() {
        let mut lru = StreamLru::new(10);
        lru.insert((1, 1));
        assert!(lru.contains((1, 1)));
        assert!(!lru.contains((1, 2)));
    }

    #[test]
    fn re_inserting_a_known_key_does_not_grow_the_cache() {
        let mut lru = StreamLru::new(10);
        lru.insert((1, 1));
        lru.insert((1, 1));
        assert_eq!(lru.len(), 1);
    }

    #[test]
    fn evicts_the_least_recently_used_entry_at_capacity() {
        let mut lru = StreamLru::new(2);
        lru.insert((1, 1));
        lru.insert((2, 1));
        // Touch (1,1) so (2,1) becomes the LRU victim.
        assert!(lru.contains((1, 1)));
        lru.insert((3, 1));
        assert!(lru.contains((1, 1)));
        assert!(lru.contains((3, 1)));
        assert!(!lru.contains((2, 1)));
        assert_eq!(lru.len(), 2);
    }

    #[test]
    fn capacity_never_exceeded_across_many_inserts() {
        let mut lru = StreamLru::new(100);
        for i in 0..1_000u64 {
            lru.insert((i, 0));
        }
        assert_eq!(lru.len(), 100);
    }

    #[test]
    fn zero_capacity_is_floored_to_one() {
        let mut lru = StreamLru::new(0);
        lru.insert((1, 1));
        assert!(lru.contains((1, 1)));
        assert_eq!(lru.len(), 1);
    }
}
