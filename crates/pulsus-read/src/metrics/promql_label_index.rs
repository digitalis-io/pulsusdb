//! The PromQL label cache's per-tenant index (issue #880): each distinct
//! label string once, in a symbol table; each series' labels packed as
//! symbol references; a sorted series-ID list per `name=value` pair and per
//! metric name. A selector is answered by intersecting those lists, then
//! testing the matchers the lists cannot answer on each candidate.
//!
//! The index is updated in place: a pass inserts the series it has not seen
//! and raises the newest hour of the ones it has ([`LabelIndex::apply`]),
//! and once an hour [`LabelIndex::age_out`] removes the series whose newest
//! registration fell behind the window's hour edge, renumbering the rest
//! densely so every list stays sorted. Nothing stored is copied to grow:
//! columns and packed labels live in fixed segments, and the fingerprint
//! table is 64 shards that rehash alone.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;

use pulsus_model::{ACTIVITY_BUCKET_MS, Fingerprint, LabelSet};
use regex::Regex;

use super::labels::{FallbackReason, RegexCache};
use super::matcher::{LabelMatcher, MatchOp};

/// A series' place in the index: dense, ascending in insertion order, and
/// renumbered (keeping that order) at each age-out.
pub(crate) type SeriesId = u32;

/// Entries per column segment.
const SEG_BITS: u32 = 16;
const SEG_LEN: usize = 1 << SEG_BITS;
/// Bytes per packed-label segment; a series' bytes never straddle two.
const ARENA_SEG_BYTES: usize = 1 << 20;
/// The offset bits of a packed-label address; the rest name the segment.
const ARENA_OFFSET_BITS: u32 = 20;
/// Fingerprint table shards: the top six bits of the hash pick one.
const FP_SHARDS: usize = 64;

/// One column: an entry per series, in fixed segments of [`SEG_LEN`].
#[derive(Debug)]
struct SegVec<T> {
    segs: Vec<Box<[T]>>,
    len: usize,
}

impl<T> Default for SegVec<T> {
    fn default() -> Self {
        SegVec {
            segs: Vec::new(),
            len: 0,
        }
    }
}

impl<T: Copy> SegVec<T> {
    fn push(&mut self, v: T) {
        if self.len == self.segs.len() * SEG_LEN {
            // A new segment, filled with the value being pushed: every
            // entry past `len` is unread.
            self.segs.push(vec![v; SEG_LEN].into_boxed_slice());
        }
        let i = self.len;
        self.segs[i >> SEG_BITS][i & (SEG_LEN - 1)] = v;
        self.len += 1;
    }

    #[inline]
    fn get(&self, i: usize) -> T {
        self.segs[i >> SEG_BITS][i & (SEG_LEN - 1)]
    }

    #[inline]
    fn set(&mut self, i: usize, v: T) {
        self.segs[i >> SEG_BITS][i & (SEG_LEN - 1)] = v;
    }

    fn truncate(&mut self, n: usize) {
        self.len = n;
        self.segs.truncate(n.div_ceil(SEG_LEN));
        self.segs.shrink_to_fit();
    }
}

/// The packed labels: 1 MiB segments, and a segment of its own for a
/// series whose bytes exceed that.
#[derive(Debug, Default)]
struct Arena {
    segs: Vec<Vec<u8>>,
}

impl Arena {
    fn alloc(&mut self, bytes: &[u8]) -> u32 {
        let fits = self
            .segs
            .last()
            .is_some_and(|s| s.len() + bytes.len() <= ARENA_SEG_BYTES);
        if !fits {
            self.segs
                .push(Vec::with_capacity(bytes.len().max(ARENA_SEG_BYTES)));
        }
        let seg = self.segs.len() - 1;
        let s = &mut self.segs[seg];
        let offset = s.len();
        s.extend_from_slice(bytes);
        ((seg as u32) << ARENA_OFFSET_BITS) | offset as u32
    }

    #[inline]
    fn get(&self, at: u32, len: u32) -> &[u8] {
        let s = &self.segs[(at >> ARENA_OFFSET_BITS) as usize];
        let o = (at & ((1 << ARENA_OFFSET_BITS) - 1)) as usize;
        &s[o..o + len as usize]
    }
}

/// Each distinct string once per tenant, with the number of references
/// series hold on it; a string at none leaves and its slot is reused.
#[derive(Debug, Default)]
struct Symbols {
    strs: Vec<Option<Box<str>>>,
    refs: Vec<u32>,
    free: Vec<u32>,
    table: hashbrown::HashTable<u32>,
    hasher: RandomState,
}

impl Symbols {
    fn lookup(&self, s: &str) -> Option<u32> {
        let h = self.hasher.hash_one(s);
        self.table
            .find(h, |&id| self.strs[id as usize].as_deref() == Some(s))
            .copied()
    }

    /// `s`'s symbol, holding one more reference on it.
    fn intern(&mut self, s: &str) -> u32 {
        let h = self.hasher.hash_one(s);
        if let Some(&id) = self
            .table
            .find(h, |&id| self.strs[id as usize].as_deref() == Some(s))
        {
            self.refs[id as usize] += 1;
            return id;
        }
        let id = match self.free.pop() {
            Some(id) => {
                self.strs[id as usize] = Some(s.into());
                self.refs[id as usize] = 1;
                id
            }
            None => {
                self.strs.push(Some(s.into()));
                self.refs.push(1);
                (self.strs.len() - 1) as u32
            }
        };
        let (strs, hasher) = (&self.strs, &self.hasher);
        self.table.insert_unique(h, id, |&i| {
            hasher.hash_one(strs[i as usize].as_deref().unwrap_or_default())
        });
        id
    }

    /// Drops one reference on `id`; at none the string leaves the table.
    fn release(&mut self, id: u32) {
        let refs = &mut self.refs[id as usize];
        *refs -= 1;
        if *refs > 0 {
            return;
        }
        if let Some(s) = self.strs[id as usize].take() {
            let h = self.hasher.hash_one(&*s);
            if let Ok(entry) = self.table.find_entry(h, |&i| i == id) {
                entry.remove();
            }
            self.free.push(id);
        }
    }

    #[inline]
    fn get(&self, id: u32) -> &str {
        self.strs[id as usize].as_deref().unwrap_or_default()
    }
}

fn put_varint(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[inline]
fn get_varint(b: &[u8], i: &mut usize) -> u32 {
    let mut v = 0u32;
    let mut shift = 0;
    loop {
        let x = b[*i];
        *i += 1;
        v |= u32::from(x & 0x7f) << shift;
        if x < 0x80 {
            return v;
        }
        shift += 7;
    }
}

/// The posting key of a `name=value` pair.
#[inline]
fn pair_key(name: u32, value: u32) -> u64 {
    (u64::from(name) << 32) | u64::from(value)
}

/// Appends `id` to a sorted list, growing it by an eighth when full.
fn push_id(list: &mut Vec<SeriesId>, id: SeriesId) {
    if list.len() == list.capacity() {
        list.reserve_exact((list.len() / 8).max(4));
    }
    list.push(id);
}

/// The sorted intersection of two sorted lists: each element of the shorter
/// is found in the longer by a galloping search from where the last ended.
pub(crate) fn intersect(a: &[SeriesId], b: &[SeriesId]) -> Vec<SeriesId> {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    let mut out = Vec::with_capacity(short.len());
    let mut lo = 0usize;
    for &x in short {
        if lo >= long.len() {
            break;
        }
        // Every element before `lo` is below `x`. Gallop until `long[hi]`
        // is at least `x` or `hi` passes the end; the search range then
        // includes `hi` itself.
        let mut hi = lo;
        let mut step = 1usize;
        while hi < long.len() && long[hi] < x {
            lo = hi + 1;
            hi += step;
            step *= 2;
        }
        let end = (hi + 1).min(long.len());
        match long[lo..end].binary_search(&x) {
            Ok(p) => {
                out.push(x);
                lo += p + 1;
            }
            Err(p) => lo += p,
        }
    }
    out
}

/// One tenant's label cache (issue #880). The header is what every gate of
/// the resolvers reads; the rest is the index.
#[derive(Debug)]
pub struct LabelIndex {
    /// `0` = cold (no committed pass yet).
    pub(crate) generation: u64,
    /// The start of the last committed pass: the recency anchor.
    pub(crate) sweep_time_ms: i64,
    /// `floor_to_activity_bucket(now - window)` of the last committed pass
    /// that moved it: the covered-from edge.
    pub(crate) covered_from_ms: i64,
    /// The start of the last committed pass; the next feed reads the
    /// registrations received since it, less the overlap.
    pub(crate) watermark_ms: i64,
    symbols: Symbols,
    fps: SegVec<Fingerprint>,
    /// The symbol of the series' metric name.
    metric: SegVec<u32>,
    /// The newest activity bucket, `bucket_ms / ACTIVITY_BUCKET_MS`.
    last_hour: SegVec<u32>,
    /// `(segment << 20) | offset` into `lab`.
    lab_at: SegVec<u32>,
    lab_len: SegVec<u32>,
    /// Pair count, saturating at 255: a capacity hint only.
    lab_n: SegVec<u8>,
    lab: Arena,
    by_fp: [hashbrown::HashTable<SeriesId>; FP_SHARDS],
    fp_hasher: RandomState,
    /// Metric symbol -> sorted IDs.
    by_metric: HashMap<u32, Vec<SeriesId>>,
    /// `(name symbol << 32) | value symbol` -> sorted IDs.
    postings: HashMap<u64, Vec<SeriesId>>,
}

impl Default for LabelIndex {
    fn default() -> Self {
        LabelIndex {
            generation: 0,
            sweep_time_ms: 0,
            covered_from_ms: 0,
            watermark_ms: 0,
            symbols: Symbols::default(),
            fps: SegVec::default(),
            metric: SegVec::default(),
            last_hour: SegVec::default(),
            lab_at: SegVec::default(),
            lab_len: SegVec::default(),
            lab_n: SegVec::default(),
            lab: Arena::default(),
            by_fp: std::array::from_fn(|_| hashbrown::HashTable::new()),
            fp_hasher: RandomState::new(),
            by_metric: HashMap::new(),
            postings: HashMap::new(),
        }
    }
}

/// What [`LabelIndex::select`] could not answer from the lists alone: one
/// matcher, with its symbols, and for a regex its pattern and verdicts.
struct Residual<'m> {
    matcher: &'m LabelMatcher,
    key: Option<u32>,
    value: Option<u32>,
    compiled: Option<Arc<Regex>>,
    verdicts: HashMap<u32, bool>,
}

/// The verdict-memo key of an absent label (no symbol has this value).
const ABSENT: u32 = u32::MAX;

impl LabelIndex {
    /// The resident series count.
    pub fn len(&self) -> usize {
        self.fps.len
    }

    /// `true` when no series is resident.
    pub fn is_empty(&self) -> bool {
        self.fps.len == 0
    }

    #[inline]
    fn fp_hash(&self, fp: Fingerprint) -> u64 {
        self.fp_hasher.hash_one(fp)
    }

    /// The series ID of `fp`, if resident.
    pub(crate) fn find(&self, fp: Fingerprint) -> Option<SeriesId> {
        let h = self.fp_hash(fp);
        self.by_fp[(h >> 58) as usize]
            .find(h, |&id| self.fps.get(id as usize) == fp)
            .copied()
    }

    /// One registration of the build or the feed. A row whose bucket is
    /// behind `lower_bound_ms` changes nothing; a resident series takes the
    /// newer of its hour and the row's; any other is inserted with the
    /// labels `labels` gives, called only then.
    pub(crate) fn apply<F: FnOnce() -> LabelSet>(
        &mut self,
        fp: Fingerprint,
        metric_name: &str,
        bucket_ms: i64,
        lower_bound_ms: i64,
        labels: F,
    ) {
        if bucket_ms < lower_bound_ms {
            return;
        }
        let hour =
            u32::try_from(bucket_ms.div_euclid(ACTIVITY_BUCKET_MS).max(0)).unwrap_or(u32::MAX);
        if let Some(id) = self.find(fp) {
            if hour > self.last_hour.get(id as usize) {
                self.last_hour.set(id as usize, hour);
            }
            return;
        }
        self.insert(fp, metric_name, &labels(), hour);
    }

    fn insert(&mut self, fp: Fingerprint, metric_name: &str, labels: &LabelSet, hour: u32) {
        let id = self.fps.len as SeriesId;
        let metric = self.symbols.intern(metric_name);
        let mut packed = Vec::with_capacity(labels.len() * 6);
        for (k, v) in labels.iter() {
            let k = self.symbols.intern(k);
            let v = self.symbols.intern(v);
            put_varint(&mut packed, k);
            put_varint(&mut packed, v);
            push_id(self.postings.entry(pair_key(k, v)).or_default(), id);
        }
        let at = self.lab.alloc(&packed);
        self.lab_at.push(at);
        self.lab_len.push(packed.len() as u32);
        self.lab_n.push(labels.len().min(255) as u8);
        self.fps.push(fp);
        self.metric.push(metric);
        self.last_hour.push(hour);
        push_id(self.by_metric.entry(metric).or_default(), id);
        let h = self.fp_hash(fp);
        let (fps, hasher) = (&self.fps, &self.fp_hasher);
        self.by_fp[(h >> 58) as usize]
            .insert_unique(h, id, |&i| hasher.hash_one(fps.get(i as usize)));
    }

    /// The packed bytes of `id`'s labels.
    #[inline]
    fn packed(&self, id: SeriesId) -> &[u8] {
        self.lab
            .get(self.lab_at.get(id as usize), self.lab_len.get(id as usize))
    }

    /// `id`'s `(name, value)` symbol pairs, in stored order.
    fn pairs(&self, id: SeriesId) -> impl Iterator<Item = (u32, u32)> + '_ {
        let b = self.packed(id);
        let mut i = 0;
        std::iter::from_fn(move || {
            if i >= b.len() {
                return None;
            }
            let k = get_varint(b, &mut i);
            let v = get_varint(b, &mut i);
            Some((k, v))
        })
    }

    /// The value symbol `id` holds for the name symbol `key`.
    #[inline]
    fn value_of(&self, id: SeriesId, key: Option<u32>) -> Option<u32> {
        let key = key?;
        self.pairs(id).find(|&(k, _)| k == key).map(|(_, v)| v)
    }

    pub(crate) fn fingerprint(&self, id: SeriesId) -> Fingerprint {
        self.fps.get(id as usize)
    }

    /// `id`'s labels, as the [`LabelSet`] its registration carried.
    pub(crate) fn label_set(&self, id: SeriesId) -> LabelSet {
        let mut pairs = Vec::with_capacity(usize::from(self.lab_n.get(id as usize)));
        for (k, v) in self.pairs(id) {
            pairs.push((
                self.symbols.get(k).to_string(),
                self.symbols.get(v).to_string(),
            ));
        }
        LabelSet::from_verbatim(pairs)
    }

    /// Every metric name and its sorted series IDs, in map order.
    pub(crate) fn metrics(&self) -> impl Iterator<Item = (&str, &[SeriesId])> + '_ {
        self.by_metric
            .iter()
            .map(|(&sym, ids)| (self.symbols.get(sym), ids.as_slice()))
    }

    /// The series of `metric_name` that every matcher selects, in ID order.
    /// `Err` names the first regex that cannot be evaluated in process,
    /// met at the first candidate that needs it.
    pub(crate) fn select(
        &self,
        metric_name: &str,
        matchers: &[LabelMatcher],
        regex_cache: &RegexCache,
    ) -> Result<Vec<SeriesId>, FallbackReason> {
        let Some(metric) = self.symbols.lookup(metric_name) else {
            return Ok(Vec::new());
        };
        let Some(base) = self.by_metric.get(&metric) else {
            return Ok(Vec::new());
        };
        self.select_from(base, matchers, regex_cache)
    }

    /// [`Self::select`] over one metric's sorted IDs.
    pub(crate) fn select_from(
        &self,
        base: &[SeriesId],
        matchers: &[LabelMatcher],
        regex_cache: &RegexCache,
    ) -> Result<Vec<SeriesId>, FallbackReason> {
        let mut lists: Vec<&[SeriesId]> = vec![base];
        let mut residual: Vec<Residual<'_>> = Vec::new();
        for m in matchers {
            let key = self.symbols.lookup(&m.key);
            if m.op == MatchOp::Eq && !m.value.is_empty() {
                let (Some(k), Some(v)) = (key, self.symbols.lookup(&m.value)) else {
                    return Ok(Vec::new());
                };
                let Some(list) = self.postings.get(&pair_key(k, v)) else {
                    return Ok(Vec::new());
                };
                lists.push(list);
            } else {
                residual.push(Residual {
                    matcher: m,
                    key,
                    value: self.symbols.lookup(&m.value),
                    compiled: None,
                    verdicts: HashMap::new(),
                });
            }
        }
        lists.sort_by_key(|l| l.len());
        let mut candidates = lists[0].to_vec();
        for list in &lists[1..] {
            if candidates.is_empty() {
                break;
            }
            candidates = intersect(&candidates, list);
        }
        if residual.is_empty() || candidates.is_empty() {
            return Ok(candidates);
        }
        let mut failed: Option<FallbackReason> = None;
        candidates.retain(|&id| {
            if failed.is_some() {
                return false;
            }
            for r in residual.iter_mut() {
                let stored = self.value_of(id, r.key);
                let ok = match r.matcher.op {
                    MatchOp::Eq | MatchOp::Neq => {
                        let equal = match stored {
                            Some(s) => Some(s) == r.value,
                            None => r.matcher.value.is_empty(),
                        };
                        equal == (r.matcher.op == MatchOp::Eq)
                    }
                    MatchOp::Re | MatchOp::Nre => {
                        let memo = stored.unwrap_or(ABSENT);
                        let verdict = match r.verdicts.get(&memo) {
                            Some(&v) => v,
                            None => {
                                if r.compiled.is_none() {
                                    match regex_cache.compiled(&r.matcher.value) {
                                        Some(re) => r.compiled = Some(re),
                                        None => {
                                            failed = Some(FallbackReason::RegexUnsupported {
                                                key: r.matcher.key.clone(),
                                            });
                                            return false;
                                        }
                                    }
                                }
                                let text = stored.map_or("", |s| self.symbols.get(s));
                                let v = r.compiled.as_ref().is_some_and(|re| re.is_match(text));
                                r.verdicts.insert(memo, v);
                                v
                            }
                        };
                        verdict == (r.matcher.op == MatchOp::Re)
                    }
                };
                if !ok {
                    return false;
                }
            }
            true
        });
        match failed {
            Some(reason) => Err(reason),
            None => Ok(candidates),
        }
    }

    /// Removes every series whose newest hour starts before
    /// `lower_bound_ms`, compacting in place and renumbering the rest in
    /// order. Returns how many were removed.
    pub(crate) fn age_out(&mut self, lower_bound_ms: i64) -> usize {
        let n = self.len();
        let mut remap: Vec<u32> = vec![u32::MAX; n];
        let mut w = 0usize;
        // The arena's write position: segment and offset.
        let (mut wseg, mut woff) = (0usize, 0usize);
        for (i, slot) in remap.iter_mut().enumerate() {
            let at = self.lab_at.get(i);
            let len = self.lab_len.get(i) as usize;
            if i64::from(self.last_hour.get(i)) * ACTIVITY_BUCKET_MS < lower_bound_ms {
                let dead: Vec<(u32, u32)> = self.pairs(i as SeriesId).collect();
                for (k, v) in dead {
                    self.symbols.release(k);
                    self.symbols.release(v);
                }
                self.symbols.release(self.metric.get(i));
                continue;
            }
            *slot = w as u32;
            if w != i {
                self.fps.set(w, self.fps.get(i));
                self.metric.set(w, self.metric.get(i));
                self.last_hour.set(w, self.last_hour.get(i));
                self.lab_n.set(w, self.lab_n.get(i));
            }
            let rseg = (at >> ARENA_OFFSET_BITS) as usize;
            let roff = (at & ((1 << ARENA_OFFSET_BITS) - 1)) as usize;
            let start = if len > ARENA_SEG_BYTES {
                // A segment of its own. It moves whole into the next write
                // segment, every byte of which has already been read.
                if woff > 0 {
                    wseg += 1;
                }
                if wseg != rseg {
                    self.lab.segs.swap(wseg, rseg);
                }
                woff = len;
                0
            } else {
                if woff + len > ARENA_SEG_BYTES {
                    wseg += 1;
                    woff = 0;
                }
                if rseg == wseg {
                    self.lab.segs[wseg].copy_within(roff..roff + len, woff);
                } else {
                    let (head, tail) = self.lab.segs.split_at_mut(rseg);
                    let src = &tail[0][roff..roff + len];
                    let dst = &mut head[wseg];
                    if dst.len() < woff + len {
                        dst.resize(woff + len, 0);
                    }
                    dst[woff..woff + len].copy_from_slice(src);
                }
                woff += len;
                woff - len
            };
            self.lab_at
                .set(w, ((wseg as u32) << ARENA_OFFSET_BITS) | start as u32);
            self.lab_len.set(w, len as u32);
            w += 1;
        }
        let removed = n - w;
        if removed == 0 {
            return 0;
        }
        self.fps.truncate(w);
        self.metric.truncate(w);
        self.last_hour.truncate(w);
        self.lab_at.truncate(w);
        self.lab_len.truncate(w);
        self.lab_n.truncate(w);
        if w == 0 {
            self.lab.segs.clear();
        } else {
            self.lab.segs.truncate(wseg + 1);
            self.lab.segs[wseg].truncate(woff);
        }
        self.lab.segs.shrink_to_fit();
        let rewrite = |list: &mut Vec<SeriesId>| {
            list.retain_mut(|id| {
                let r = remap[*id as usize];
                *id = r;
                r != u32::MAX
            });
            list.shrink_to_fit();
            !list.is_empty()
        };
        self.postings.retain(|_, list| rewrite(list));
        self.by_metric.retain(|_, list| rewrite(list));
        for shard in &mut self.by_fp {
            shard.retain(|id| {
                let r = remap[*id as usize];
                *id = r;
                r != u32::MAX
            });
        }
        removed
    }

    /// Commits a pass that ended without error (step 6 of the pass): moves
    /// the covered-from edge and ages out once it passes an hour, then
    /// stamps the pass and counts it.
    pub(crate) fn commit(&mut self, now_ms: i64, lower_bound_ms: i64) {
        if lower_bound_ms > self.covered_from_ms {
            self.covered_from_ms = lower_bound_ms;
            self.age_out(lower_bound_ms);
        }
        self.sweep_time_ms = now_ms;
        self.watermark_ms = now_ms;
        self.generation = self.generation.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: i64 = ACTIVITY_BUCKET_MS;

    fn labels(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::from_verbatim(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<_>>(),
        )
    }

    fn put(ix: &mut LabelIndex, fp: u128, metric: &str, bucket_ms: i64, ls: &LabelSet) {
        ix.apply(Fingerprint::from_raw(fp), metric, bucket_ms, 0, || {
            ls.clone()
        });
    }

    fn materialise(ix: &LabelIndex, ids: &[SeriesId]) -> Vec<(Fingerprint, LabelSet)> {
        let mut out: Vec<(Fingerprint, LabelSet)> = ids
            .iter()
            .map(|&id| (ix.fingerprint(id), ix.label_set(id)))
            .collect();
        out.sort_by_key(|(fp, _)| *fp);
        out
    }

    /// A small xorshift generator: the fixture and the selectors are the
    /// same on every run.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn matcher(key: &str, op: MatchOp, value: &str) -> LabelMatcher {
        LabelMatcher {
            key: key.to_string(),
            op,
            value: value.to_string(),
        }
    }

    /// The brute-force verdict: every matcher on `get(key).unwrap_or("")`,
    /// a regex anchored as the resolver anchors it (`compiled[i]` is
    /// matcher `i`'s).
    fn brute_matches(ls: &LabelSet, matchers: &[LabelMatcher], compiled: &[Option<Regex>]) -> bool {
        matchers.iter().zip(compiled).all(|(m, re)| {
            let v = ls.get(&m.key).unwrap_or("");
            match (m.op, re) {
                (MatchOp::Eq, _) => v == m.value,
                (MatchOp::Neq, _) => v != m.value,
                (op, Some(re)) => re.is_match(v) == (op == MatchOp::Re),
                (_, None) => unreachable!("every regex matcher is compiled"),
            }
        })
    }

    /// **T1.** 20,000 seeded series and 400 seeded selectors: the index's
    /// answer equals a scan of the generator's list.
    #[test]
    fn index_answers_equal_a_brute_force_scan() {
        let mut rng = Rng(0x880);
        let names: Vec<String> = (0..300).map(|i| format!("m_{i:03}")).collect();
        let keys = [
            "container",
            "endpoint",
            "instance",
            "job",
            "namespace",
            "node",
            "pod",
            "service",
            "image",
            "id",
            "cpu",
            "device",
            "le",
            "scope",
            "zone",
            "opt",
        ];
        let mut series: Vec<(String, Fingerprint, LabelSet)> = Vec::with_capacity(20_000);
        for i in 0..20_000u64 {
            let metric = names[rng.below(300) as usize].clone();
            let n = 12 + rng.below(5) as usize;
            let mut pairs: Vec<(String, String)> = Vec::new();
            for k in keys.iter().take(n) {
                if *k == "opt" && i % 3 == 0 {
                    // Absent on a third of the series.
                    continue;
                }
                let v = match *k {
                    // A stored empty value.
                    "zone" if i % 7 == 0 => String::new(),
                    // A value equal to another series' key, and one equal
                    // to a metric name.
                    "zone" if i % 7 == 1 => "pod".to_string(),
                    "zone" if i % 7 == 2 => "m_007".to_string(),
                    _ => format!("{k}-{}", rng.below(12)),
                };
                pairs.push((k.to_string(), v));
            }
            if i % 5 == 0 {
                pairs.push(("opt".to_string(), String::new()));
            }
            let fp = Fingerprint::from_raw((u128::from(rng.next()) << 64) | u128::from(i));
            series.push((metric, fp, LabelSet::from_verbatim(pairs)));
        }
        // One label set under two metric names.
        let twin = series[0].2.clone();
        series.push(("m_001".to_string(), Fingerprint::from_raw(7), twin.clone()));
        series.push(("m_002".to_string(), Fingerprint::from_raw(8), twin));

        let mut ix = LabelIndex::default();
        // 2 % of the series first arrive an hour earlier, age out, and
        // arrive again.
        let h = 500_000 * H;
        for (i, (m, fp, ls)) in series.iter().enumerate() {
            let bucket = if i % 50 == 0 { h - H } else { h };
            ix.apply(*fp, m, bucket, 0, || ls.clone());
        }
        let before = ix.len();
        assert_eq!(ix.age_out(h), series.len().div_ceil(50));
        assert_eq!(ix.len() + series.len().div_ceil(50), before);
        for (i, (m, fp, ls)) in series.iter().enumerate() {
            if i % 50 == 0 {
                ix.apply(*fp, m, h, h, || ls.clone());
            }
        }
        assert_eq!(ix.len(), series.len());

        let ops = [MatchOp::Eq, MatchOp::Neq, MatchOp::Re, MatchOp::Nre];
        let regexes = ["", ".*", "pod-1.*", "(pod|m_007)", "x?", "job-[0-3]", "a|"];
        let regex_cache = RegexCache::new(4_096);
        let mut non_empty = 0;
        for _ in 0..400 {
            let (metric, _, sample) = &series[rng.below(series.len() as u64) as usize];
            let mut matchers = Vec::new();
            for _ in 0..rng.below(4) {
                let key = keys[rng.below(keys.len() as u64) as usize];
                let op = ops[rng.below(4) as usize];
                let value = match (op, rng.below(4)) {
                    (MatchOp::Eq | MatchOp::Neq, 0) => String::new(),
                    (MatchOp::Eq | MatchOp::Neq, _) => {
                        sample.get(key).unwrap_or("absent-value").to_string()
                    }
                    (_, _) => regexes[rng.below(regexes.len() as u64) as usize].to_string(),
                };
                matchers.push(matcher(key, op, &value));
            }
            let got = materialise(
                &ix,
                &ix.select(metric, &matchers, &regex_cache)
                    .expect("every test pattern compiles"),
            );
            let compiled: Vec<Option<Regex>> = matchers
                .iter()
                .map(|m| {
                    matches!(m.op, MatchOp::Re | MatchOp::Nre)
                        .then(|| Regex::new(&format!("^(?:{})$", m.value)).expect("a test pattern"))
                })
                .collect();
            let mut want: Vec<(Fingerprint, LabelSet)> = series
                .iter()
                .filter(|(m, _, ls)| m == metric && brute_matches(ls, &matchers, &compiled))
                .map(|(_, fp, ls)| (*fp, ls.clone()))
                .collect();
            want.sort_by_key(|(fp, _)| *fp);
            if !want.is_empty() {
                non_empty += 1;
            }
            assert_eq!(got, want, "{metric} {matchers:?}");
        }
        assert!(
            non_empty > 200,
            "the selectors must mostly match: {non_empty} of 400"
        );
    }

    /// **T2.**
    #[test]
    fn age_out_removes_exactly_the_series_behind_the_edge() {
        let h = 400_000 * H;
        let mut ix = LabelIndex::default();
        let old = labels(&[("job", "api"), ("only", "the-oldest")]);
        let mid = labels(&[("job", "api"), ("pod", "p1")]);
        let new = labels(&[("job", "web"), ("pod", "p2")]);
        put(&mut ix, 1, "up", h - 2 * H, &old);
        put(&mut ix, 2, "up", h - H, &mid);
        put(&mut ix, 3, "up", h, &new);
        assert_eq!(ix.len(), 3);

        assert_eq!(ix.age_out(h - H), 1);
        assert_eq!(ix.len(), 2);
        for list in ix.postings.values().chain(ix.by_metric.values()) {
            assert!(list.iter().all(|&id| (id as usize) < ix.len()), "{list:?}");
            assert!(list.windows(2).all(|w| w[0] < w[1]), "{list:?}");
        }
        assert_eq!(ix.symbols.lookup("the-oldest"), None);
        assert_eq!(ix.symbols.lookup("only"), None);
        assert_eq!(ix.find(Fingerprint::from_raw(1)), None);
        let survivors: Vec<(Fingerprint, LabelSet)> = [2u128, 3]
            .iter()
            .map(|&fp| {
                let id = ix.find(Fingerprint::from_raw(fp)).expect("a survivor");
                (ix.fingerprint(id), ix.label_set(id))
            })
            .collect();
        assert_eq!(
            survivors,
            vec![
                (Fingerprint::from_raw(2), mid),
                (Fingerprint::from_raw(3), new)
            ]
        );
        let regex_cache = RegexCache::new(16);
        assert_eq!(
            ix.select("up", &[matcher("job", MatchOp::Eq, "api")], &regex_cache)
                .map(|ids| ids.len()),
            Ok(1)
        );
    }

    /// **T3.**
    #[test]
    fn a_touch_never_lowers_the_hour_and_a_row_behind_the_edge_is_ignored() {
        let h = 400_000 * H;
        let mut ix = LabelIndex::default();
        let ls = labels(&[("job", "api")]);
        ix.apply(Fingerprint::from_raw(1), "up", h, h - H, || ls.clone());
        ix.apply(Fingerprint::from_raw(1), "up", h - H, h - H, || ls.clone());
        assert_eq!(ix.len(), 1, "one series, touched once");
        let id = ix.find(Fingerprint::from_raw(1)).expect("resident");
        assert_eq!(i64::from(ix.last_hour.get(id as usize)) * H, h);
        ix.apply(Fingerprint::from_raw(2), "up", h - 2 * H, h - H, || {
            ls.clone()
        });
        assert_eq!(ix.len(), 1);
        assert_eq!(ix.find(Fingerprint::from_raw(2)), None);
    }

    /// **T10.**
    #[test]
    fn intersect_finds_an_element_at_the_galloping_probe() {
        assert_eq!(intersect(&[1, 2, 5], &[1, 2, 3, 4, 5, 6]), vec![1, 2, 5]);
        assert_eq!(intersect(&[1, 2, 3, 4, 5, 6], &[1, 2, 5]), vec![1, 2, 5]);
    }
}
