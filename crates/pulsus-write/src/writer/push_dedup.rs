//! Issue #494: the per-signal push-suppression index.
//!
//! A client that retries a push after a network timeout used to store its
//! entries or samples twice, so `count_over_time`/`sum_over_time` returned
//! doubled values while `rate` looked right. This module suppresses a
//! content-identical push that reaches **the same writer process** inside
//! `PULSUS_INGEST_DEDUP_WINDOW`, before the insert.
//!
//! # What "the same push" means
//!
//! Every push forms exactly one identity, a single `u128`: the digest of
//! the `Idempotency-Key` bytes when the client sent one, else the digest of
//! the request's own content. The entry separately records the content
//! digest, so the same key carrying different content is refused rather
//! than silently suppressed. `Retry-Attempt` forms no identity and matches
//! nothing — it only labels the duplicate counter.
//!
//! # Why the memory bound is a construction check rather than arithmetic
//!
//! Three review rounds computed a population from a per-entry constant and
//! the next round's allocator disagreed — the last of them by 29,568 bytes
//! against a 16,777,216-byte bound. So the running code charges what it
//! allocates: [`plan_capacities`] reserves the collections once, from their
//! own layout, and steps the reserved sizes **down** until the modelled
//! charge is at or below the knob. A formula that is a step out can
//! therefore only under-use the knob; it cannot exceed it.
//!
//! `crates/pulsus-write/tests/a494_dedup_charge.rs` is the oracle: it
//! builds the index to the capacities the index itself reports, with a
//! counting allocator, and fails if the model and the allocator disagree.
//!
//! # The shape of the bound
//!
//! ```text
//!                 PULSUS_INGEST_DEDUP_MAX_BYTES, per signal
//!   |<----------------------- the knob ------------------------->|
//!   |<--------- claim table, reserved once --------->|<- waiters ->|
//!   |   largest doubling step at or below the 80%    |  remainder, |
//!   |   CEILING; never rehashes, never steps         |  reduced    |
//!   |                                                |  until it   |
//!   |                                                |  fits       |
//! ```
//!
//! 80% is a **ceiling, not a split**: immediately below a doubling step the
//! claim table takes as little as 40% of the knob and the waiters take the
//! rest. Only the ceiling is a rule.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use pulsus_model::{Fingerprint, LabelSet};
use tokio::sync::oneshot;
// `tokio::time::Instant`, not `std::time::Instant`: the window and the
// claim deadline are both minutes long, and a test that has to wait them
// out in wall-clock time is a test nobody runs. Outside a runtime this is
// the standard-library clock; under `#[tokio::test(start_paused = true)]`
// it is the runtime's, so ageing and expiry are exercised in milliseconds.
use tokio::time::Instant;
use xxhash_rust::xxh3::Xxh3;

use crate::ingest::PushHeaders;
use crate::ingest::metrics::ParsedMetrics;
use crate::protocols::otlp_logs::ParsedLogs;

// ---------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------

/// One push's index key: 128 bits, never a compound. It is the digest of
/// the `Idempotency-Key` bytes when the client sent one, else the content
/// digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PushDigest(u128);

impl PushDigest {
    /// Wraps a raw 128-bit value. The digest functions above are the only
    /// production mint; this exists so a caller driving the index directly
    /// — the charge and ownership suites — can name a key.
    pub const fn from_raw(value: u128) -> Self {
        PushDigest(value)
    }
}

/// A push's resolved identity: the index key, the content digest the entry
/// stores for key-reuse detection, and whether the client declared a retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushIdentity {
    pub key: PushDigest,
    pub content: PushDigest,
    pub declared_retry: bool,
}

/// A streaming 128-bit digest over a push's request-controlled content.
///
/// Receiver-generated fields are excluded because they change on a retry:
/// `StreamRow::updated_ns` (`crates/pulsus-write/src/protocols/otlp_logs.rs:111-113`)
/// and `MetricMetadata::updated_ns`
/// (`crates/pulsus-write/src/ingest/metrics.rs:94-97`) are the receive time.
/// `StreamRow::month` stays in: it is `toStartOfMonth` of the **record**
/// timestamp, not of `now_ns` (`otlp_logs.rs:103-106`).
///
/// Every `f64` enters as `to_bits()`, never as a comparison. The two
/// readings first disagree at `0.0` against `-0.0` — equal under `==`, and
/// different patterns — so an `==`-keyed digest would drop a sample the
/// client sent; in the other direction `NaN != NaN`, so an `==`-keyed
/// digest would never match a retried push carrying a stale marker.
struct DigestBuilder {
    hasher: Xxh3,
}

impl DigestBuilder {
    fn new() -> Self {
        DigestBuilder {
            hasher: Xxh3::new(),
        }
    }

    /// Length-prefixed, so `("ab", "c")` and `("a", "bc")` differ.
    fn str(&mut self, s: &str) -> &mut Self {
        self.hasher.update(&(s.len() as u64).to_le_bytes());
        self.hasher.update(s.as_bytes());
        self
    }

    fn u64(&mut self, v: u64) -> &mut Self {
        self.hasher.update(&v.to_le_bytes());
        self
    }

    fn i64(&mut self, v: i64) -> &mut Self {
        self.u64(v as u64)
    }

    fn u8(&mut self, v: u8) -> &mut Self {
        self.hasher.update(&[v]);
        self
    }

    fn f64(&mut self, v: f64) -> &mut Self {
        self.u64(v.to_bits())
    }

    fn fingerprint(&mut self, fp: Fingerprint) -> &mut Self {
        // `Fingerprint` has no accessor for its inner value by design (the
        // rendering seal, `crates/pulsus-model/src/time.rs`), and `Hash` is
        // one of the three routes its own doc comment names as required.
        fp.hash(&mut self.hasher);
        self
    }

    /// Label sets iterate in sorted key order
    /// (`crates/pulsus-model/src/labels.rs:485`), so this needs no sort and
    /// allocates nothing.
    fn labels(&mut self, ls: &LabelSet) -> &mut Self {
        self.u64(ls.len() as u64);
        for (k, v) in ls.iter() {
            self.str(k).str(v);
        }
        self
    }

    fn opt_str(&mut self, s: Option<&str>) -> &mut Self {
        match s {
            Some(s) => self.u8(1).str(s),
            None => self.u8(0),
        }
    }

    fn finish(&self) -> PushDigest {
        PushDigest(self.hasher.digest128())
    }
}

/// The content digest of a parsed log push.
fn log_content_digest(batch: &ParsedLogs) -> PushDigest {
    let mut d = DigestBuilder::new();
    d.str("logs");
    d.u64(batch.rows.len() as u64);
    for row in &batch.rows {
        d.str(&row.service)
            .fingerprint(row.fingerprint)
            .i64(row.timestamp_ns.0)
            .u8(row.severity as u8)
            .str(&row.body)
            .str(&row.structured_metadata);
    }
    d.u64(batch.streams.len() as u64);
    for stream in &batch.streams {
        d.u64(u64::from(stream.month.days_since_epoch()))
            .fingerprint(stream.fingerprint)
            .str(&stream.service)
            .labels(&stream.labels);
    }
    d.u64(batch.rejected);
    d.opt_str(batch.rejected_message.as_deref());
    d.finish()
}

/// The content digest of a parsed metric push.
fn metric_content_digest(batch: &ParsedMetrics) -> PushDigest {
    let mut d = DigestBuilder::new();
    d.str("metrics");
    d.u64(batch.samples.len() as u64);
    for point in &batch.samples {
        d.str(&point.metric_name)
            .fingerprint(point.fingerprint)
            .i64(point.unix_milli)
            .f64(point.value);
    }
    d.u64(batch.hist_samples.len() as u64);
    for point in &batch.hist_samples {
        let h = &point.histogram;
        d.str(&point.metric_name)
            .fingerprint(point.fingerprint)
            .i64(point.unix_milli)
            .i64(i64::from(h.schema))
            .f64(h.zero_threshold)
            .u64(h.zero_count)
            .u64(h.count)
            .f64(h.sum);
        d.u64(h.positive_spans.len() as u64);
        for span in &h.positive_spans {
            d.i64(i64::from(span.offset)).u64(u64::from(span.length));
        }
        d.u64(h.negative_spans.len() as u64);
        for span in &h.negative_spans {
            d.i64(i64::from(span.offset)).u64(u64::from(span.length));
        }
        d.u64(h.positive_buckets.len() as u64);
        for delta in &h.positive_buckets {
            d.i64(*delta);
        }
        d.u64(h.negative_buckets.len() as u64);
        for delta in &h.negative_buckets {
            d.i64(*delta);
        }
        d.u64(h.custom_values.len() as u64);
        for value in &h.custom_values {
            d.f64(*value);
        }
        d.u8(h.counter_reset_hint.as_u8());
    }
    d.u64(batch.series.len() as u64);
    for series in &batch.series {
        d.str(&series.metric_name)
            .fingerprint(series.fingerprint)
            .labels(&series.labels);
    }
    d.u64(batch.metadata.len() as u64);
    for meta in &batch.metadata {
        d.str(&meta.metric_name)
            .str(&meta.metric_type)
            .str(&meta.help)
            .str(&meta.unit);
    }
    d.u64(batch.rejected);
    d.opt_str(batch.rejected_message.as_deref());
    d.finish()
}

/// The digest of an `Idempotency-Key`'s bytes, namespaced away from the
/// content digests above so a key can never collide with a content digest
/// by construction of the same byte stream.
fn key_digest(key: &str) -> PushDigest {
    let mut d = DigestBuilder::new();
    d.str("idempotency-key");
    d.str(key);
    d.finish()
}

/// Resolves a log push's identity — the digest of the `Idempotency-Key`
/// when the client sent one, else of the request's own content.
pub fn log_identity(batch: &ParsedLogs, headers: &PushHeaders) -> PushIdentity {
    identity_from(log_content_digest(batch), headers)
}

/// Resolves a metric push's identity — see [`log_identity`].
pub fn metric_identity(batch: &ParsedMetrics, headers: &PushHeaders) -> PushIdentity {
    identity_from(metric_content_digest(batch), headers)
}

fn identity_from(content: PushDigest, headers: &PushHeaders) -> PushIdentity {
    let key = match &headers.idempotency_key {
        Some(k) => key_digest(k),
        None => content,
    };
    PushIdentity {
        key,
        content,
        declared_retry: headers.declared_retry,
    }
}

// ---------------------------------------------------------------------
// The byte model — ONE place, checked by a counting allocator
// ---------------------------------------------------------------------

/// hashbrown's control-group width, the trailing bytes every table
/// allocates past its buckets. 16 on every target this project builds for
/// (SSE2 on `x86_64`, NEON on `aarch64`); the generic fallback is 8, which
/// would make this model over-count by 8 bytes per map — the safe
/// direction. `a494_dedup_charge.rs` asserts the model against the
/// allocator, so either way a disagreement is a red test rather than a
/// breach.
const HASH_GROUP_WIDTH: usize = 16;

/// The bytes one `tokio::sync::oneshot` channel allocates for the state its
/// sender and receiver share. An **upper bound**, not an equality: the
/// charge must never be less than the allocation, and
/// `a494_dedup_charge.rs::oneshot_state_fits_its_charge` fails if tokio's
/// state grows past it.
const ONESHOT_STATE_BYTES: u64 = 128;

/// hashbrown's `capacity_to_buckets`: the slot count a table allocates to
/// hold `capacity` entries. A table keeps one eighth of its slots free, so
/// above eight entries the bucket count is the next power of two at or
/// above `capacity * 8 / 7`.
const fn capacity_to_buckets(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    if capacity < 8 {
        return if capacity < 4 { 4 } else { 8 };
    }
    (capacity * 8 / 7).next_power_of_two()
}

/// The inverse: how many entries a table of `buckets` slots holds.
const fn buckets_to_capacity(buckets: usize) -> usize {
    if buckets == 0 {
        return 0;
    }
    if buckets < 8 {
        return buckets - 1;
    }
    buckets - buckets / 8
}

const fn round_up(value: usize, align: usize) -> usize {
    value.div_ceil(align) * align
}

/// The heap bytes a `std::collections::HashMap<K, V>` built with
/// `with_capacity(capacity)` allocates.
///
/// hashbrown lays one allocation out as the `(K, V)` slot array, padded up
/// to the table's alignment, followed by one control byte per slot and one
/// trailing control group. The two figures the plan measured are this
/// formula: a table of 229,376 `(u128, 32-byte entry)` pairs allocates
/// 12,845,072 bytes and one more entry doubles it to 25,690,128.
const fn hash_map_bytes<K, V>(capacity: usize) -> u64 {
    let buckets = capacity_to_buckets(capacity);
    if buckets == 0 {
        return 0;
    }
    let slot = size_of::<(K, V)>();
    let align = if align_of::<(K, V)>() > HASH_GROUP_WIDTH {
        align_of::<(K, V)>()
    } else {
        HASH_GROUP_WIDTH
    };
    (round_up(slot * buckets, align) + buckets + HASH_GROUP_WIDTH) as u64
}

/// The heap bytes a `Vec`/`VecDeque` built with `with_capacity(capacity)`
/// allocates: `RawVec` reserves exactly that many elements.
const fn vec_bytes<T>(capacity: usize) -> u64 {
    (capacity * size_of::<T>()) as u64
}

/// The heap bytes `Arc::new` allocates for `T`: two reference counts
/// followed by the value, laid out as one struct.
const fn arc_bytes<T>() -> u64 {
    let header = 2 * size_of::<usize>();
    let align = align_of::<T>();
    round_up(round_up(header, align) + size_of::<T>(), align) as u64
}

/// One blocked sync caller's element in its key's waiter vector.
type WaiterSlot = (RegistrationId, oneshot::Sender<ClaimOutcome>);
type WaiterVec = Vec<WaiterSlot>;

/// The bytes one live waiter registration is charged.
///
/// A `Vec` that has had `k` elements pushed holds `max(4, k.next_power_of_two())`
/// slots for an element this small, so four slots per registration upper-bounds
/// every `k >= 1`: at `k = 1` it is exact, and at every larger `k` the vector's
/// own capacity is below `4k`. The oneshot's shared state is the other half.
/// The waiter map itself is reserved at construction and is **not** charged
/// here.
const WAITER_CHARGE_BYTES: u64 = 4 * size_of::<WaiterSlot>() as u64 + ONESHOT_STATE_BYTES;

/// The claim table's reserved footprint for `claims` entries: the map, plus
/// the two creation-ordered key rings that make window expiry and open-claim
/// aging amortized-constant instead of a scan.
const fn claim_table_bytes(claims: usize) -> u64 {
    hash_map_bytes::<PushDigest, ClaimEntry>(claims)
        + vec_bytes::<PushDigest>(claims)
        + vec_bytes::<PushDigest>(claims)
}

/// The waiter side's footprint at its bound: the map reserved at
/// construction, plus every registration it can hold, charged.
const fn waiter_side_bytes(waiters: usize) -> u64 {
    hash_map_bytes::<PushDigest, WaiterVec>(waiters) + waiters as u64 * WAITER_CHARGE_BYTES
}

/// The whole index's footprint at its bound — the figure the knob bounds.
///
/// Three fixed allocations sit beside the two collections and are charged
/// here because the knob bounds the whole index: the `Arc<Mutex<Core>>`
/// the lock lives in, the `Arc<DedupMetrics>` the counters live in, and
/// the `Arc<PushDedup>` the handle itself lives in. Together they are a
/// few hundred bytes — small, and a model that leaves them out is a model
/// that disagrees with the allocator, which is the failure mode §0 exists
/// to stop.
pub const fn index_bytes(claims: usize, waiters: usize) -> u64 {
    arc_bytes::<Mutex<Core>>()
        + arc_bytes::<DedupMetrics>()
        + arc_bytes::<PushDedup>()
        + claim_table_bytes(claims)
        + waiter_side_bytes(waiters)
}

/// The reserved sizes an index built against `max_bytes` lands on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capacities {
    /// Claims the table holds. Reserved once; the table never rehashes.
    pub claims: usize,
    /// Blocked sync callers the registry holds. Past it a suppressed sync
    /// caller is refused `429`.
    pub waiters: usize,
}

/// The claim table's share of the knob is capped at this fraction —
/// `numerator/denominator`. A **ceiling, not a split**: immediately below a
/// doubling step the table takes as little as 40% and the waiters take the
/// rest.
const CLAIM_CEILING_NUM: u64 = 4;
const CLAIM_CEILING_DEN: u64 = 5;

/// Chooses the reserved sizes for `max_bytes`, from the collections' own
/// layout rather than from a written-down population.
///
/// The claim table takes the largest doubling step whose charge is at or
/// below the 80% **ceiling**; the remainder is the waiter budget, and the
/// waiter step is then reduced until the modelled whole-index charge fits.
/// If no waiter step fits at all, the claim step is reduced and the search
/// repeats — so the returned pair always satisfies
/// `index_bytes(claims, waiters) <= max_bytes`, and a model that is a step
/// out can only under-use the knob.
pub fn plan_capacities(max_bytes: u64) -> Capacities {
    // Bucket counts are powers of two; four is hashbrown's smallest table.
    let mut claim_buckets = 4usize;
    let mut best = Capacities {
        claims: 0,
        waiters: 0,
    };
    loop {
        let claims = buckets_to_capacity(claim_buckets);
        let claim_side = claim_table_bytes(claims);
        if claim_side * CLAIM_CEILING_DEN > max_bytes * CLAIM_CEILING_NUM {
            break;
        }
        let Some(waiters) = largest_waiter_step(claims, max_bytes) else {
            break;
        };
        best = Capacities { claims, waiters };
        let Some(next) = claim_buckets.checked_mul(2) else {
            break;
        };
        claim_buckets = next;
    }
    best
}

/// The largest waiter step that fits alongside `claims` inside `max_bytes`.
fn largest_waiter_step(claims: usize, max_bytes: u64) -> Option<usize> {
    let mut buckets = 4usize;
    let mut best = None;
    loop {
        let waiters = buckets_to_capacity(buckets);
        if index_bytes(claims, waiters) > max_bytes {
            return best;
        }
        best = Some(waiters);
        buckets = buckets.checked_mul(2)?;
    }
}

// ---------------------------------------------------------------------
// Claim state
// ---------------------------------------------------------------------

/// What a claim knows about the rows it stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimState {
    /// Rows are buffered or in flight; their fate is not yet known.
    Open,
    /// At least one target committed, or at least one target's fate is
    /// unknown. A later identical push is suppressed.
    Confirmed,
    /// Every recorded target settled provably not-committed. A later
    /// identical push is admitted.
    Released,
    /// The claim deadline passed before the targets settled. A **tombstone**:
    /// retained for the remainder of the window and never cap-evicted, because
    /// evicting it would re-admit a push that may have committed.
    Unknown,
}

impl ClaimState {
    const fn is_terminal(self) -> bool {
        !matches!(self, ClaimState::Open)
    }

    /// Only a claim that settled with a known fate may be evicted under cap
    /// pressure. `Unknown` is the tombstone and `Open` is live.
    const fn is_evictable(self) -> bool {
        matches!(self, ClaimState::Confirmed | ClaimState::Released)
    }
}

/// What the **original sync caller** was told: the join over the targets
/// that join the durability acknowledgement, which excludes log patterns
/// (`crates/pulsus-write/src/writer/mod.rs:566-568`). A suppressed caller
/// is told this, so a pattern-only failure reproduces the original's
/// success exactly as the original saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOutcome {
    Ok,
    Failed,
}

/// How one target settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetOutcome {
    /// The insert committed.
    Committed,
    /// Provably not committed — a non-retryable failure, or a pre-send
    /// retryable exhausted before the block was sent.
    NotCommitted,
    /// The fate is unknown: a post-send failure, or a forced shutdown.
    Uncertain,
}

/// Issue #494: a monotonic identifier for one blocked caller's
/// registration. A guard removes **only its own pair**, so cancelling one
/// caller never strands the others waiting on the same key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RegistrationId(u64);

/// One claim. Times are milliseconds since the index's own epoch, not
/// `Instant`s: an `Instant` is sixteen bytes on Linux, and this struct is
/// multiplied by the claim table's reserved capacity.
#[derive(Debug)]
pub(crate) struct ClaimEntry {
    /// The content digest, kept separately from the key so the same
    /// `Idempotency-Key` carrying different content is refused.
    content: PushDigest,
    created_ms: u64,
    state: ClaimState,
    outcome: Option<ClaimOutcome>,
    /// `None` until `seal`; `seal` fixes it after the last append.
    targets_total: Option<u32>,
    targets_settled: u32,
    /// The subset of targets that join the durability acknowledgement.
    ack_total: Option<u32>,
    ack_settled: u32,
    ack_failed: bool,
    any_committed: bool,
    any_not_committed: bool,
}

impl ClaimEntry {
    const fn complete(&self) -> bool {
        match self.targets_total {
            Some(total) => self.targets_settled >= total,
            None => false,
        }
    }
}

// ---------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------

/// The index's own counters. One set per signal; the `signal` label is
/// applied where they are exposed (`crates/pulsus-server/src/ops.rs`).
#[derive(Debug, Default)]
pub struct DedupMetrics {
    /// A suppressed push whose client did **not** declare a retry.
    pub duplicate_pushes_total: AtomicU64,
    /// A suppressed push carrying `Retry-Attempt: n`, `n >= 1`.
    pub duplicate_pushes_declared_total: AtomicU64,
    /// Rows the suppressed pushes would have stored.
    pub duplicate_rows_suppressed_total: AtomicU64,
    /// The same `Idempotency-Key` carrying different content: a `400`.
    pub key_reused_total: AtomicU64,
    /// A claim whose targets did not all settle the same way.
    pub mixed_outcome_total: AtomicU64,
    /// A claim that never settled and aged into a tombstone.
    pub unknown_total: AtomicU64,
    /// The `429` at the claim table's bound.
    pub shed_total: AtomicU64,
    /// The `429` at the waiter registry's bound.
    pub wait_shed_total: AtomicU64,
    /// Guards dropped un-sealed — a claim rolled back before any append.
    pub rollbacks_total: AtomicU64,
}

/// A point-in-time reading of [`DedupMetrics`] plus the two gauges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DedupMetricsSnapshot {
    pub duplicate_pushes_total: u64,
    pub duplicate_pushes_declared_total: u64,
    pub duplicate_rows_suppressed_total: u64,
    pub key_reused_total: u64,
    pub mixed_outcome_total: u64,
    pub unknown_total: u64,
    pub shed_total: u64,
    pub wait_shed_total: u64,
    pub rollbacks_total: u64,
    /// Bytes charged to live waiter registrations.
    pub wait_bytes: u64,
    /// The whole index's charge, at its bound.
    pub bytes: u64,
}

// ---------------------------------------------------------------------
// The index
// ---------------------------------------------------------------------

/// Everything the index mutates, behind one lock.
///
/// `std::sync::Mutex`, never an async one: [`WaitGuard`]'s `Drop` is
/// synchronous and must take this lock, and blocking an async mutex from a
/// destructor panics. It is also the shipped convention in this module —
/// the stream cache (`writer/mod.rs:92,201`) and every table buffer
/// (`writer/buffer.rs:10`) use `std::sync::Mutex` already.
#[derive(Debug)]
pub(crate) struct Core {
    /// Reserved once at construction; never grows, never rehashes.
    claims: HashMap<PushDigest, ClaimEntry>,
    /// Every live claim's key in creation order. Window expiry pops the
    /// front, so it is amortized constant rather than a scan.
    order: VecDeque<PushDigest>,
    /// Keys that were open, in creation order. Since the claim deadline is
    /// a constant offset, creation order **is** deadline order, so aging
    /// pops the front too; a key whose claim already settled is skipped.
    open_order: VecDeque<PushDigest>,
    /// Blocked sync callers, keyed. Reserved once at construction. Each
    /// registration carries its own [`RegistrationId`] so a guard removes
    /// only its own pair.
    waiters: HashMap<PushDigest, WaiterVec>,
    next_registration: u64,
    /// Bytes charged to live registrations. Owned by [`WaitGuard`]:
    /// registration charges, `Drop` releases, settlement never does.
    waiter_charge: u64,
    /// How many resident claims are in an evictable state — read before a
    /// scan, so a table with nothing to evict sheds without walking.
    evictable: usize,
    limits: Limits,
}

/// The fixed sizes and deadlines an index is built with.
#[derive(Debug, Clone, Copy)]
struct Limits {
    capacities: Capacities,
    /// Bytes the waiter registry may charge.
    waiter_budget: u64,
    /// How long a writer remembers an accepted push.
    window_ms: u64,
    /// How long a claim may stay open before it becomes a tombstone.
    claim_deadline_ms: u64,
}

/// The per-signal push-suppression index.
#[derive(Debug)]
pub struct PushDedup {
    core: Arc<Mutex<Core>>,
    epoch: Instant,
    metrics: Arc<DedupMetrics>,
    capacities: Capacities,
    bound_bytes: u64,
}

/// What [`PushDedup::admit`] decided. The suppression decision and the
/// waiter registration are taken in **one** acquisition of the lock
/// `settle` also takes, so a settle cannot slip between them and a claim
/// cannot vanish between them either.
#[derive(Debug)]
pub enum Admission {
    /// Admit. `seal` must be called after the last append.
    Admit(ClaimGuard),
    /// Suppressed, and the original push's outcome is already known.
    /// Nothing is stored.
    SuppressedSettled(ClaimOutcome),
    /// Suppressed while the original is still in flight. Await `rx`,
    /// holding `guard` for exactly as long. Nothing is stored.
    SuppressedPending {
        guard: WaitGuard,
        rx: oneshot::Receiver<ClaimOutcome>,
    },
    /// The same `Idempotency-Key` carrying different content — a client
    /// error, never a silent suppression.
    KeyReused,
    /// The claim table is at its bound with nothing evictable: `429`.
    Shed,
    /// A suppressed sync caller could not be registered because the waiter
    /// registry is at its bound: `429`, and nothing is stored.
    WaitShed,
}

/// Whether a suppressed caller needs to be told the original's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitMode {
    /// Sync mode: register, and wait for the original's answer.
    Register,
    /// Async mode: the handler answers `202` without waiting, so nothing is
    /// registered and no bytes are charged.
    None,
}

/// The locked half of the keyed wait.
#[derive(Debug)]
enum WaitStart {
    /// The claim already settled.
    Settled(ClaimOutcome),
    /// The waiter budget is exhausted.
    WaitShed,
    /// Registered. Await `rx`; hold `guard` for exactly as long.
    Registered {
        guard: WaitGuard,
        rx: oneshot::Receiver<ClaimOutcome>,
    },
}

impl PushDedup {
    /// Builds an index bounded by `max_bytes`, reserving its collections
    /// from their own layout (see [`plan_capacities`]).
    pub fn new(max_bytes: u64, window: Duration, claim_deadline: Duration) -> Arc<Self> {
        let capacities = plan_capacities(max_bytes);
        let bound_bytes = index_bytes(capacities.claims, capacities.waiters);
        let waiter_budget = capacities.waiters as u64 * WAITER_CHARGE_BYTES;
        let core = Core {
            claims: HashMap::with_capacity(capacities.claims),
            order: VecDeque::with_capacity(capacities.claims),
            open_order: VecDeque::with_capacity(capacities.claims),
            waiters: HashMap::with_capacity(capacities.waiters),
            next_registration: 0,
            waiter_charge: 0,
            evictable: 0,
            limits: Limits {
                capacities,
                waiter_budget,
                window_ms: window.as_millis().try_into().unwrap_or(u64::MAX),
                claim_deadline_ms: claim_deadline.as_millis().try_into().unwrap_or(u64::MAX),
            },
        };
        Arc::new(PushDedup {
            core: Arc::new(Mutex::new(core)),
            epoch: Instant::now(),
            metrics: Arc::new(DedupMetrics::default()),
            capacities,
            bound_bytes,
        })
    }

    /// The reserved sizes this index landed on — reported by the index, never
    /// taken from a document.
    pub fn capacities(&self) -> Capacities {
        self.capacities
    }

    /// The modelled charge at this index's bound.
    pub fn bound_bytes(&self) -> u64 {
        self.bound_bytes
    }

    pub fn snapshot(&self) -> DedupMetricsSnapshot {
        let m = &self.metrics;
        let wait_bytes = self.lock().waiter_charge;
        DedupMetricsSnapshot {
            duplicate_pushes_total: m.duplicate_pushes_total.load(Ordering::Relaxed),
            duplicate_pushes_declared_total: m
                .duplicate_pushes_declared_total
                .load(Ordering::Relaxed),
            duplicate_rows_suppressed_total: m
                .duplicate_rows_suppressed_total
                .load(Ordering::Relaxed),
            key_reused_total: m.key_reused_total.load(Ordering::Relaxed),
            mixed_outcome_total: m.mixed_outcome_total.load(Ordering::Relaxed),
            unknown_total: m.unknown_total.load(Ordering::Relaxed),
            shed_total: m.shed_total.load(Ordering::Relaxed),
            wait_shed_total: m.wait_shed_total.load(Ordering::Relaxed),
            rollbacks_total: m.rollbacks_total.load(Ordering::Relaxed),
            wait_bytes,
            bytes: self.bound_bytes,
        }
    }

    fn lock(&self) -> MutexGuard<'_, Core> {
        self.core.lock().expect("push dedup mutex poisoned")
    }

    fn now_ms(&self) -> u64 {
        self.epoch
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    /// Looks up, and claims if absent — **one critical section**, so two
    /// concurrent identical pushes cannot both admit, and a suppressed
    /// sync caller registers for the original's outcome before the lock is
    /// released.
    pub fn admit(self: &Arc<Self>, id: PushIdentity, wait: WaitMode) -> Admission {
        let now_ms = self.now_ms();
        let mut core = self.lock();
        core.expire(now_ms);

        if let Some(entry) = core.claims.get(&id.key) {
            if entry.content != id.content {
                self.metrics
                    .key_reused_total
                    .fetch_add(1, Ordering::Relaxed);
                return Admission::KeyReused;
            }
            if entry.state == ClaimState::Released {
                // Every recorded target settled provably not-committed, so
                // the rows are not there: re-admit, replacing the entry.
                core.remove(&id.key);
            } else {
                if wait == WaitMode::None {
                    // Async suppressed callers register nothing: the
                    // handler answers `202` without waiting for anything.
                    return Admission::SuppressedSettled(ClaimOutcome::Ok);
                }
                return match core.start_wait(self, id.key) {
                    WaitStart::Settled(outcome) => Admission::SuppressedSettled(outcome),
                    WaitStart::Registered { guard, rx } => {
                        Admission::SuppressedPending { guard, rx }
                    }
                    WaitStart::WaitShed => {
                        self.metrics.wait_shed_total.fetch_add(1, Ordering::Relaxed);
                        Admission::WaitShed
                    }
                };
            }
        }

        if core.claims.len() >= core.limits.capacities.claims && !core.evict_one() {
            self.metrics.shed_total.fetch_add(1, Ordering::Relaxed);
            return Admission::Shed;
        }

        core.insert(
            id.key,
            ClaimEntry {
                content: id.content,
                created_ms: now_ms,
                state: ClaimState::Open,
                outcome: None,
                targets_total: None,
                targets_settled: 0,
                ack_total: None,
                ack_settled: 0,
                ack_failed: false,
                any_committed: false,
                any_not_committed: false,
            },
        );
        Admission::Admit(ClaimGuard {
            dedup: Arc::clone(self),
            key: id.key,
            targets: 0,
            ack_targets: 0,
            sealed: false,
        })
    }

    /// Counts a suppressed push.
    pub(crate) fn count_suppressed(&self, declared_retry: bool, rows: u64) {
        if declared_retry {
            self.metrics
                .duplicate_pushes_declared_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics
                .duplicate_pushes_total
                .fetch_add(1, Ordering::Relaxed);
        }
        self.metrics
            .duplicate_rows_suppressed_total
            .fetch_add(rows, Ordering::Relaxed);
    }

    /// The locked half of the keyed wait, reached only through [`Self::admit`]
    /// in production. Exposed here so the ownership tests can drive it
    /// directly.
    #[cfg(test)]
    fn begin_wait(self: &Arc<Self>, key: PushDigest) -> Option<WaitStart> {
        let mut core = self.lock();
        if !core.claims.contains_key(&key) {
            return None;
        }
        Some(core.start_wait(self, key))
    }

    /// Reports one target's fate for one claim, and completes the claim
    /// when every recorded target has reported.
    pub fn settle_target(&self, key: PushDigest, outcome: TargetOutcome, counts_for_ack: bool) {
        let mut core = self.lock();
        let Some(entry) = core.claims.get_mut(&key) else {
            return;
        };
        if entry.state == ClaimState::Unknown {
            // The tombstone stands: a late settle does not resurrect a
            // claim whose deadline already passed.
            return;
        }
        entry.targets_settled += 1;
        match outcome {
            TargetOutcome::Committed => entry.any_committed = true,
            TargetOutcome::NotCommitted => entry.any_not_committed = true,
            TargetOutcome::Uncertain => entry.any_committed = true,
        }
        if counts_for_ack {
            entry.ack_settled += 1;
            if outcome != TargetOutcome::Committed {
                entry.ack_failed = true;
            }
        }
        Self::complete_if_ready(&mut core, key, &self.metrics);
    }

    /// Evaluates completion on **both** events — at `seal` and at each
    /// settle — so a generation that settles before `seal` is handled at
    /// `seal`, and one that settles after is handled at settle. There is no
    /// ordering assumption.
    fn complete_if_ready(core: &mut Core, key: PushDigest, metrics: &DedupMetrics) {
        let Some(entry) = core.claims.get_mut(&key) else {
            return;
        };
        if entry.state.is_terminal() || !entry.complete() {
            return;
        }
        let state = if entry.any_committed {
            ClaimState::Confirmed
        } else {
            ClaimState::Released
        };
        let outcome = if entry.ack_failed {
            ClaimOutcome::Failed
        } else {
            ClaimOutcome::Ok
        };
        if entry.any_committed && entry.any_not_committed {
            metrics.mixed_outcome_total.fetch_add(1, Ordering::Relaxed);
        }
        entry.state = state;
        entry.outcome = Some(outcome);
        core.evictable += 1;
        let taken = core.waiters.remove(&key);
        // The senders leave the map here; each guard still holds its own
        // charge and releases it when its caller's future completes.
        if let Some(waiters) = taken {
            for (_, tx) in waiters {
                let _ = tx.send(outcome);
            }
        }
    }

    /// Driven once per flush cycle by the per-table flush task: expires
    /// claims whose window elapsed, and ages an open claim past its
    /// deadline into a tombstone.
    pub fn tick(&self) {
        let now_ms = self.now_ms();
        let mut core = self.lock();
        core.expire(now_ms);
        let aged = core.age_open_claims(now_ms);
        if aged > 0 {
            self.metrics
                .unknown_total
                .fetch_add(aged, Ordering::Relaxed);
        }
    }
}

impl Core {
    /// **The terminal check and the waiter registration happen inside one
    /// acquisition of the lock `settle` also takes**, so there is no window
    /// for a settle to slip between them. Releasing the lock between the
    /// two is exactly the defect `dedup_keyed_wait.rs` breaks to prove this.
    fn start_wait(&mut self, dedup: &Arc<PushDedup>, key: PushDigest) -> WaitStart {
        let Some(entry) = self.claims.get(&key) else {
            // Unreachable from `admit`, which has just found the entry under
            // this same lock; a caller that has not is told the claim
            // settled with no answer of its own.
            return WaitStart::Settled(ClaimOutcome::Failed);
        };
        if entry.state.is_terminal() {
            return WaitStart::Settled(entry.outcome.unwrap_or(ClaimOutcome::Failed));
        }
        if self.waiter_charge + WAITER_CHARGE_BYTES > self.limits.waiter_budget {
            return WaitStart::WaitShed;
        }
        let id = RegistrationId(self.next_registration);
        self.next_registration += 1;
        self.waiter_charge += WAITER_CHARGE_BYTES;
        let (tx, rx) = oneshot::channel();
        self.waiters.entry(key).or_default().push((id, tx));
        WaitStart::Registered {
            guard: WaitGuard {
                dedup: Arc::clone(dedup),
                key,
                id,
            },
            rx,
        }
    }

    fn insert(&mut self, key: PushDigest, entry: ClaimEntry) {
        self.claims.insert(key, entry);
        self.order.push_back(key);
        self.open_order.push_back(key);
    }

    fn remove(&mut self, key: &PushDigest) {
        if let Some(entry) = self.claims.remove(key)
            && entry.state.is_evictable()
        {
            self.evictable -= 1;
        }
        // The two rings carry stale keys until they reach the front, where
        // a key with no claim is skipped. Removing from the middle would be
        // linear; skipping at the front is constant.
    }

    /// Drops every claim whose suppression window has elapsed. The ring is
    /// in creation order, so this stops at the first live claim.
    fn expire(&mut self, now_ms: u64) {
        let window = self.limits.window_ms;
        while let Some(key) = self.order.front().copied() {
            match self.claims.get(&key) {
                Some(entry) if now_ms.saturating_sub(entry.created_ms) < window => break,
                Some(_) => {
                    self.order.pop_front();
                    self.remove(&key);
                    self.wake_absent(&key);
                }
                None => {
                    self.order.pop_front();
                }
            }
        }
    }

    /// Ages open claims past the claim deadline into tombstones. Creation
    /// order is deadline order, so this stops at the first claim still
    /// inside its deadline.
    fn age_open_claims(&mut self, now_ms: u64) -> u64 {
        let deadline = self.limits.claim_deadline_ms;
        let mut aged = 0u64;
        while let Some(key) = self.open_order.front().copied() {
            let Some(entry) = self.claims.get_mut(&key) else {
                self.open_order.pop_front();
                continue;
            };
            if entry.state.is_terminal() {
                self.open_order.pop_front();
                continue;
            }
            if now_ms.saturating_sub(entry.created_ms) < deadline {
                break;
            }
            self.open_order.pop_front();
            // A tombstone is deliberately NOT counted as evictable: the
            // counter and `ClaimState::is_evictable` must agree, or the
            // cap-pressure walk short-circuits on a table it could in fact
            // evict from.
            entry.state = ClaimState::Unknown;
            entry.outcome = Some(ClaimOutcome::Failed);
            aged += 1;
            if let Some(waiters) = self.waiters.remove(&key) {
                for (_, tx) in waiters {
                    let _ = tx.send(ClaimOutcome::Failed);
                }
            }
        }
        aged
    }

    /// Wakes anyone still waiting on a claim that has just left the index.
    fn wake_absent(&mut self, key: &PushDigest) {
        if let Some(waiters) = self.waiters.remove(key) {
            for (_, tx) in waiters {
                // The receiver observing a dropped sender is how an absent
                // claim reaches the caller, which then admits.
                drop(tx);
            }
        }
    }

    /// Evicts the oldest claim in an evictable state. `Open` and `Unknown`
    /// are never evicted: dropping a tombstone would re-admit a push that
    /// may have committed, which is the rule the tombstone exists to keep.
    ///
    /// Cost: the walk skips leading claims that are open or tombstoned, and
    /// returns immediately when there are none to find. Both are the
    /// symptom of a writer whose flushes are not settling, which
    /// `pulsus_ingest_dedup_unknown_total` shows before the shed counter
    /// does.
    fn evict_one(&mut self) -> bool {
        if self.evictable == 0 {
            return false;
        }
        for i in 0..self.order.len() {
            let key = self.order[i];
            let evictable = self
                .claims
                .get(&key)
                .is_some_and(|e| e.state.is_evictable());
            if evictable {
                self.order.remove(i);
                self.remove(&key);
                self.wake_absent(&key);
                return true;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------
// The three RAII owners
// ---------------------------------------------------------------------

/// Held by an admitted push for the length of its admission. Un-sealed on
/// drop means the push never reached its buffers — every early return
/// between `admit` and the last append — so the claim is removed and the
/// same body sent again is stored.
#[derive(Debug)]
pub struct ClaimGuard {
    dedup: Arc<PushDedup>,
    key: PushDigest,
    targets: u32,
    ack_targets: u32,
    sealed: bool,
}

impl ClaimGuard {
    pub fn key(&self) -> PushDigest {
        self.key
    }

    /// Records that this push appended to one more target.
    /// `counts_for_ack` is false for log patterns, which never join the
    /// durability acknowledgement (`writer/mod.rs:566-568`).
    pub fn note_target(&mut self, counts_for_ack: bool) {
        self.targets += 1;
        if counts_for_ack {
            self.ack_targets += 1;
        }
    }

    /// Fixes the target count after the last append. The guard is armed
    /// from here on: dropping it no longer removes the claim.
    pub fn seal(mut self) {
        self.sealed = true;
        let dedup = Arc::clone(&self.dedup);
        let key = self.key;
        let mut core = dedup.lock();
        if let Some(entry) = core.claims.get_mut(&key) {
            entry.targets_total = Some(self.targets);
            entry.ack_total = Some(self.ack_targets);
        }
        PushDedup::complete_if_ready(&mut core, key, &dedup.metrics);
    }
}

impl Drop for ClaimGuard {
    fn drop(&mut self) {
        if self.sealed {
            return;
        }
        let mut core = self.dedup.lock();
        core.remove(&self.key);
        core.wake_absent(&self.key);
        drop(core);
        self.dedup
            .metrics
            .rollbacks_total
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Obtained with a flush generation's rows and nothing else. Settling it
/// reports every claim in the generation; dropping it unsettled reports an
/// unknown fate for each. That is what makes forced-shutdown reporting
/// structural rather than a rule someone has to remember: a disposal path
/// cannot take a generation's rows without also taking the obligation.
///
/// What `Drop` does and does not guarantee: a normal drop reports, and a
/// panic unwind also reports. An explicit leak reports nothing, and a
/// process abort runs no destructors at all — the second costs nothing,
/// because the index dies with the process and a writer restart stores the
/// retry by design.
#[derive(Debug)]
pub(crate) struct ClaimTicket {
    dedup: Option<Arc<PushDedup>>,
    counts_for_ack: bool,
    claims: Vec<PushDigest>,
}

impl ClaimTicket {
    pub(crate) fn new(dedup: Option<Arc<PushDedup>>, counts_for_ack: bool) -> Self {
        ClaimTicket {
            dedup,
            counts_for_ack,
            claims: Vec::new(),
        }
    }

    /// An inert ticket, for a buffer with no suppression index behind it.
    pub(crate) fn inert() -> Self {
        ClaimTicket::new(None, true)
    }

    pub(crate) fn push(&mut self, key: PushDigest) {
        if self.dedup.is_some() {
            self.claims.push(key);
        }
    }

    /// Reports `outcome` for every claim this generation carried.
    pub(crate) fn settle(mut self, outcome: TargetOutcome) {
        self.report(outcome);
    }

    fn report(&mut self, outcome: TargetOutcome) {
        let Some(dedup) = self.dedup.as_ref() else {
            self.claims.clear();
            return;
        };
        for key in std::mem::take(&mut self.claims) {
            dedup.settle_target(key, outcome, self.counts_for_ack);
        }
    }
}

impl Drop for ClaimTicket {
    fn drop(&mut self) {
        if self.claims.is_empty() {
            return;
        }
        // A ticket dropped without an explicit settle: the rows' fate is
        // unknown, which is the answer that never re-admits possibly
        // committed rows.
        self.report(TargetOutcome::Uncertain);
    }
}

/// Held by a blocked sync caller for exactly as long as it waits. Dropping
/// it — on completion, on cancellation, on a client disconnect — removes
/// **this caller's** sender and returns its bytes to the waiter budget.
///
/// The guard owns the charge, and it is the only thing that releases it:
/// settlement takes the senders out of the map but never releases, so
/// neither ordering double-releases and neither leaves a charge behind.
#[derive(Debug)]
pub struct WaitGuard {
    dedup: Arc<PushDedup>,
    key: PushDigest,
    id: RegistrationId,
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        let mut core = self.dedup.lock();
        if let Some(waiters) = core.waiters.get_mut(&self.key) {
            waiters.retain(|(id, _)| *id != self.id);
            if waiters.is_empty() {
                core.waiters.remove(&self.key);
            }
        }
        core.waiter_charge = core.waiter_charge.saturating_sub(WAITER_CHARGE_BYTES);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index() -> Arc<PushDedup> {
        PushDedup::new(
            16 * 1024 * 1024,
            Duration::from_secs(300),
            Duration::from_secs(120),
        )
    }

    /// Every suppression, whichever side of the claim's settlement it
    /// landed on.
    fn is_suppressed(a: &Admission) -> bool {
        matches!(
            a,
            Admission::SuppressedSettled(_) | Admission::SuppressedPending { .. }
        )
    }

    fn admit(d: &Arc<PushDedup>, id: PushIdentity) -> Admission {
        d.admit(id, WaitMode::Register)
    }

    fn identity(content: u128) -> PushIdentity {
        PushIdentity {
            key: PushDigest(content),
            content: PushDigest(content),
            declared_retry: false,
        }
    }

    #[test]
    fn hash_map_bytes_reproduces_the_measured_doubling_step() {
        // The two figures the plan measured with a counting allocator, on a
        // `HashMap` whose pair is 48 bytes: 229,376 entries allocate
        // 12,845,072 bytes and one more entry doubles it.
        struct Entry32(#[allow(dead_code)] [u8; 32]);
        assert_eq!(size_of::<(u128, Entry32)>(), 48);
        assert_eq!(hash_map_bytes::<u128, Entry32>(229_376), 12_845_072);
        assert_eq!(hash_map_bytes::<u128, Entry32>(229_377), 25_690_128);
    }

    #[test]
    fn capacity_and_buckets_are_inverses_at_every_step() {
        for exp in 2..40u32 {
            let buckets = 1usize << exp;
            let capacity = buckets_to_capacity(buckets);
            assert_eq!(
                capacity_to_buckets(capacity),
                buckets,
                "buckets {buckets} -> capacity {capacity} must come back"
            );
            assert!(
                capacity_to_buckets(capacity + 1) > buckets,
                "one entry past capacity {capacity} must step"
            );
        }
    }

    #[test]
    fn planned_capacities_fit_the_knob_and_are_never_empty() {
        for knob in [
            pulsus_config::INGEST_DEDUP_MAX_BYTES_FLOOR,
            2 * 1024 * 1024,
            16 * 1024 * 1024,
            20 * 1024 * 1024,
            pulsus_config::INGEST_DEDUP_MAX_BYTES_CEILING,
        ] {
            let c = plan_capacities(knob);
            assert!(c.claims > 0, "{knob}: the claim table must hold a push");
            assert!(c.waiters > 0, "{knob}: the registry must hold a waiter");
            assert!(
                index_bytes(c.claims, c.waiters) <= knob,
                "{knob}: {c:?} charges {} bytes",
                index_bytes(c.claims, c.waiters)
            );
        }
    }

    #[test]
    fn a_second_identical_push_is_suppressed() {
        let d = index();
        let id = identity(7);
        let Admission::Admit(mut guard) = admit(&d, id) else {
            panic!("the first push must admit");
        };
        guard.note_target(true);
        guard.seal();
        assert!(is_suppressed(&admit(&d, id)));
    }

    /// A push that reached no buffer at all stored nothing, so there is
    /// nothing for a later identical push to duplicate: it is admitted.
    #[test]
    fn a_push_that_touched_no_target_does_not_suppress_the_next_one() {
        let d = index();
        let id = identity(8);
        let Admission::Admit(guard) = admit(&d, id) else {
            panic!("the first push must admit");
        };
        guard.seal();
        assert!(matches!(admit(&d, id), Admission::Admit(_)));
    }

    #[test]
    fn a_guard_dropped_before_seal_removes_the_claim() {
        let d = index();
        let id = identity(9);
        match admit(&d, id) {
            Admission::Admit(guard) => drop(guard),
            other => panic!("expected an admission, got {other:?}"),
        }
        assert_eq!(
            d.snapshot().rollbacks_total,
            1,
            "the rolled-back claim is counted once"
        );
        assert!(
            matches!(admit(&d, id), Admission::Admit(_)),
            "a rolled-back claim must not suppress the next push"
        );
    }

    #[test]
    fn the_same_key_with_different_content_is_a_client_error() {
        let d = index();
        let first = PushIdentity {
            key: PushDigest(1),
            content: PushDigest(100),
            declared_retry: false,
        };
        let Admission::Admit(guard) = admit(&d, first) else {
            panic!("the first push must admit");
        };
        guard.seal();
        let second = PushIdentity {
            content: PushDigest(200),
            ..first
        };
        assert!(matches!(admit(&d, second), Admission::KeyReused));
        assert_eq!(d.snapshot().key_reused_total, 1);
    }

    #[test]
    fn a_claim_whose_targets_all_failed_is_re_admitted() {
        let d = index();
        let id = identity(11);
        let Admission::Admit(mut guard) = admit(&d, id) else {
            panic!("the first push must admit");
        };
        guard.note_target(true);
        guard.seal();
        d.settle_target(id.key, TargetOutcome::NotCommitted, true);
        assert!(
            matches!(admit(&d, id), Admission::Admit(_)),
            "provably-not-committed rows must be re-admitted"
        );
    }

    #[test]
    fn a_claim_with_one_committed_target_stays_suppressed() {
        let d = index();
        let id = identity(12);
        let Admission::Admit(mut guard) = admit(&d, id) else {
            panic!("the first push must admit");
        };
        guard.note_target(true);
        guard.note_target(false);
        guard.seal();
        d.settle_target(id.key, TargetOutcome::Committed, true);
        d.settle_target(id.key, TargetOutcome::NotCommitted, false);
        assert!(is_suppressed(&admit(&d, id)));
        assert_eq!(
            d.snapshot().mixed_outcome_total,
            1,
            "a claim whose targets disagreed is counted"
        );
    }

    #[test]
    fn the_suppressed_caller_is_told_the_acknowledged_targets_outcome() {
        let d = index();
        let id = identity(13);
        let Admission::Admit(mut guard) = admit(&d, id) else {
            panic!("the first push must admit");
        };
        guard.note_target(true);
        guard.note_target(false);
        guard.seal();
        d.settle_target(id.key, TargetOutcome::Committed, true);
        d.settle_target(id.key, TargetOutcome::NotCommitted, false);
        assert!(
            matches!(
                d.begin_wait(id.key),
                Some(WaitStart::Settled(ClaimOutcome::Ok))
            ),
            "a failure on a target outside the acknowledgement must not \
             become the suppressed caller's answer"
        );
    }

    #[test]
    fn an_absent_claim_tells_the_caller_to_admit() {
        let d = index();
        assert!(
            d.begin_wait(PushDigest(404)).is_none(),
            "a claim that is not there cannot be waited on"
        );
        assert!(
            matches!(admit(&d, identity(404)), Admission::Admit(_)),
            "and the push that finds nothing is stored"
        );
    }

    #[tokio::test]
    async fn a_settle_wakes_every_waiter_on_that_key_and_no_other() {
        let d = index();
        let a = identity(21);
        let b = identity(22);
        for id in [a, b] {
            let Admission::Admit(mut guard) = admit(&d, id) else {
                panic!("must admit");
            };
            guard.note_target(true);
            guard.seal();
        }
        let Some(WaitStart::Registered { guard: g1, rx: rx1 }) = d.begin_wait(a.key) else {
            panic!("must register");
        };
        let Some(WaitStart::Registered { guard: g2, rx: rx2 }) = d.begin_wait(a.key) else {
            panic!("must register");
        };
        let Some(WaitStart::Registered { guard: g3, rx: rx3 }) = d.begin_wait(b.key) else {
            panic!("must register");
        };

        d.settle_target(a.key, TargetOutcome::Committed, true);
        assert_eq!(rx1.await.unwrap(), ClaimOutcome::Ok);
        assert_eq!(rx2.await.unwrap(), ClaimOutcome::Ok);
        assert_eq!(
            d.lock().waiters.get(&b.key).map(Vec::len),
            Some(1),
            "a settle of one key must not take another key's waiters"
        );
        drop((g1, g2, g3));
        drop(rx3);
        assert_eq!(
            d.lock().waiter_charge,
            0,
            "every guard releases its own charge exactly once"
        );
    }

    #[tokio::test]
    async fn cancelling_one_waiter_leaves_its_co_waiter_registered() {
        let d = index();
        let id = identity(31);
        let Admission::Admit(mut guard) = admit(&d, id) else {
            panic!("must admit");
        };
        guard.note_target(true);
        guard.seal();
        let Some(WaitStart::Registered { guard: g1, rx: rx1 }) = d.begin_wait(id.key) else {
            panic!("must register");
        };
        let Some(WaitStart::Registered { guard: g2, rx: rx2 }) = d.begin_wait(id.key) else {
            panic!("must register");
        };
        let charge_two = d.lock().waiter_charge;
        drop(g1);
        drop(rx1);
        assert_eq!(
            d.lock().waiter_charge,
            charge_two - WAITER_CHARGE_BYTES,
            "a cancelled caller returns its own bytes and no more"
        );
        assert_eq!(
            d.lock().waiters.get(&id.key).map(Vec::len),
            Some(1),
            "the co-waiter must still be registered"
        );
        d.settle_target(id.key, TargetOutcome::Committed, true);
        assert_eq!(rx2.await.unwrap(), ClaimOutcome::Ok);
        drop(g2);
        assert_eq!(d.lock().waiter_charge, 0);
    }

    #[test]
    fn settling_then_dropping_releases_the_charge_exactly_once() {
        let d = index();
        let id = identity(41);
        let Admission::Admit(mut guard) = admit(&d, id) else {
            panic!("must admit");
        };
        guard.note_target(true);
        guard.seal();
        let Some(WaitStart::Registered { guard: g, rx }) = d.begin_wait(id.key) else {
            panic!("must register");
        };
        d.settle_target(id.key, TargetOutcome::Committed, true);
        assert_eq!(
            d.lock().waiter_charge,
            WAITER_CHARGE_BYTES,
            "settlement takes the sender but never the charge"
        );
        drop(g);
        drop(rx);
        assert_eq!(d.lock().waiter_charge, 0);
    }

    #[test]
    fn the_waiter_registry_sheds_at_its_bound_and_refills_after_cancellation() {
        let d = PushDedup::new(
            pulsus_config::INGEST_DEDUP_MAX_BYTES_FLOOR,
            Duration::from_secs(300),
            Duration::from_secs(120),
        );
        let id = identity(51);
        let Admission::Admit(mut guard) = admit(&d, id) else {
            panic!("must admit");
        };
        guard.note_target(true);
        guard.seal();
        let mut held = Vec::new();
        loop {
            match admit(&d, id) {
                Admission::SuppressedPending { guard, rx } => held.push((guard, rx)),
                Admission::WaitShed => break,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(
            held.len(),
            d.capacities().waiters,
            "the registry holds exactly the capacity it reports"
        );
        assert_eq!(d.snapshot().wait_shed_total, 1);
        held.truncate(held.len() / 2);
        let free = d.capacities().waiters - held.len();
        for _ in 0..free {
            assert!(
                matches!(admit(&d, id), Admission::SuppressedPending { .. }),
                "cancelled registrations must be reusable"
            );
        }
    }

    #[test]
    fn a_tombstone_is_not_evicted_and_a_terminal_claim_is() {
        let d = PushDedup::new(
            pulsus_config::INGEST_DEDUP_MAX_BYTES_FLOOR,
            Duration::from_secs(300),
            Duration::from_millis(0),
        );
        let capacity = d.capacities().claims;
        // Fill with claims that never settle, then age them into tombstones.
        for i in 0..capacity as u128 {
            let Admission::Admit(mut guard) = admit(&d, identity(i)) else {
                panic!("claim {i} must admit");
            };
            guard.note_target(true);
            guard.seal();
        }
        d.tick();
        assert_eq!(d.snapshot().unknown_total, capacity as u64);
        assert!(
            matches!(admit(&d, identity(u128::MAX)), Admission::Shed),
            "a table of tombstones must shed rather than re-admit one"
        );
        assert_eq!(d.snapshot().shed_total, 1);
        assert!(
            is_suppressed(&admit(&d, identity(0))),
            "the tombstone is still resident, so its retry is still suppressed"
        );

        // The complementary leg: a table of settled claims evicts instead.
        let d = PushDedup::new(
            pulsus_config::INGEST_DEDUP_MAX_BYTES_FLOOR,
            Duration::from_secs(300),
            Duration::from_secs(120),
        );
        let capacity = d.capacities().claims;
        for i in 0..capacity as u128 {
            let Admission::Admit(mut guard) = admit(&d, identity(i)) else {
                panic!("claim {i} must admit");
            };
            guard.note_target(true);
            guard.seal();
            d.settle_target(PushDigest(i), TargetOutcome::Committed, true);
        }
        assert!(
            matches!(admit(&d, identity(u128::MAX)), Admission::Admit(_)),
            "a table of settled claims must evict the oldest, not shed"
        );
        assert_eq!(d.snapshot().shed_total, 0);
    }

    #[test]
    fn a_claim_is_forgotten_once_its_window_elapses() {
        let d = PushDedup::new(
            16 * 1024 * 1024,
            Duration::from_millis(0),
            Duration::from_secs(120),
        );
        let id = identity(61);
        let Admission::Admit(mut guard) = admit(&d, id) else {
            panic!("must admit");
        };
        guard.note_target(true);
        guard.seal();
        d.settle_target(id.key, TargetOutcome::Committed, true);
        d.tick();
        assert!(
            matches!(admit(&d, id), Admission::Admit(_)),
            "a byte-identical push after the window is a new push"
        );
    }

    #[test]
    fn an_unsettled_ticket_reports_an_unknown_fate_on_drop() {
        let d = index();
        let id = identity(71);
        let Admission::Admit(mut guard) = admit(&d, id) else {
            panic!("must admit");
        };
        guard.note_target(true);
        guard.seal();
        let mut ticket = ClaimTicket::new(Some(Arc::clone(&d)), true);
        ticket.push(id.key);
        drop(ticket);
        assert!(
            is_suppressed(&admit(&d, id)),
            "rows of unknown fate are never re-admitted"
        );
    }

    #[test]
    fn a_ticket_dropped_during_a_panic_unwind_still_reports() {
        let d = index();
        let id = identity(72);
        let Admission::Admit(mut guard) = admit(&d, id) else {
            panic!("must admit");
        };
        guard.note_target(true);
        guard.seal();
        let for_panic = Arc::clone(&d);
        let result = std::panic::catch_unwind(move || {
            let mut ticket = ClaimTicket::new(Some(for_panic), true);
            ticket.push(PushDigest(72));
            panic!("the flush task died holding a generation");
        });
        assert!(result.is_err());
        assert!(is_suppressed(&admit(&d, id)));
    }

    #[test]
    fn the_digest_distinguishes_zero_from_negative_zero() {
        use crate::ingest::metrics::MetricPoint;
        let point = |value: f64| ParsedMetrics {
            samples: vec![MetricPoint {
                metric_name: "m".into(),
                fingerprint: Fingerprint::from_raw(1),
                unix_milli: 1,
                value,
            }],
            ..Default::default()
        };
        assert_ne!(
            metric_content_digest(&point(0.0)),
            metric_content_digest(&point(-0.0)),
            "0.0 and -0.0 are equal under `==` and are different samples"
        );
    }

    #[test]
    fn the_digest_matches_a_retried_nan() {
        use crate::ingest::metrics::MetricPoint;
        let point = || ParsedMetrics {
            samples: vec![MetricPoint {
                metric_name: "m".into(),
                fingerprint: Fingerprint::from_raw(1),
                unix_milli: 1,
                value: f64::NAN,
            }],
            ..Default::default()
        };
        assert_eq!(
            metric_content_digest(&point()),
            metric_content_digest(&point()),
            "`NaN != NaN`, so an `==`-keyed digest would never match a retry"
        );
    }

    #[test]
    fn a_different_key_is_a_different_push_and_the_same_key_is_not() {
        let batch = ParsedMetrics::default();
        let none = log_identity(&ParsedLogs::default(), &PushHeaders::default());
        let keyed = |k: &str| {
            metric_identity(
                &batch,
                &PushHeaders {
                    idempotency_key: Some(k.to_string()),
                    declared_retry: false,
                },
            )
        };
        assert_ne!(keyed("a").key, keyed("b").key);
        assert_eq!(keyed("a").key, keyed("a").key);
        assert_eq!(
            keyed("a").content,
            keyed("b").content,
            "the key namespaces the identity; it does not change the content"
        );
        assert_ne!(none.key, keyed("a").key);
    }

    #[test]
    fn retry_attempt_forms_no_identity() {
        let batch = ParsedLogs::default();
        let plain = log_identity(&batch, &PushHeaders::default());
        let declared = log_identity(
            &batch,
            &PushHeaders {
                idempotency_key: None,
                declared_retry: true,
            },
        );
        assert_eq!(
            plain.key, declared.key,
            "`Retry-Attempt` must not change what a push matches"
        );
        assert!(declared.declared_retry);
    }

    #[test]
    fn retry_attempt_is_read_only_as_an_attempt_number() {
        assert!(PushHeaders::from_values(None, Some("1")).declared_retry);
        assert!(PushHeaders::from_values(None, Some(" 2 ")).declared_retry);
        assert!(!PushHeaders::from_values(None, Some("0")).declared_retry);
        assert!(!PushHeaders::from_values(None, Some("yes")).declared_retry);
        assert!(!PushHeaders::from_values(None, None).declared_retry);
        assert_eq!(
            PushHeaders::from_values(Some("  "), None).idempotency_key,
            None,
            "a blank key is no key"
        );
    }
}
