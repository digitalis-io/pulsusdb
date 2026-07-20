//! `ChPool`: a fixed-size pool of `pool_size` concurrent leases, spread over
//! one or more ClickHouse (HTTP) endpoints (issue #43).
//!
//! The `clickhouse` crate's `Client` already shares one keep-alive HTTP
//! transport across clones (its own internal `hyper` connection pool), and
//! carries no server-side session state between requests (every setting is
//! sent per-request, never `SET` on a persistent session) — see
//! docs/decisions/0001-clickhouse-client.md. `ChPool` therefore adapts the
//! architect's fixed-size, health-checked, RAII-lease pool contract onto
//! that transport: its primary job is bounding **concurrent request count**
//! to `pool_size` (a single semaphore, unchanged by endpoint count), plus a
//! uniform lease API so callers do not need to know which crate ships
//! underneath.
//!
//! **Connection spreading (issue #43).** The pool holds one
//! `clickhouse::Client` per distinct endpoint and picks one endpoint per
//! `get()` via the pure, lock-free [`candidate_order`] policy: prefer
//! healthy endpoints in this node's own availability zone (round-robin
//! within them), then healthy endpoints in other zones (round-robin), then
//! demoted endpoints whose cooldown has expired, then the rest. There is
//! **no ping on the healthy hot path** — an endpoint is probed only when it
//! is unhealthy or has gone stale. A transport failure on a real query
//! demotes its endpoint (via [`PooledConn::report_transport_failure`]) so
//! the *next* `get()` fails over. Health is published as lock-free mirror
//! atomics for the hot path, backed by a tiny non-async transition lock
//! (never held across an `.await`) that makes every state change — a
//! demotion, a promotion, or a probe outcome — one atomic critical section;
//! demotion never runs in `Drop`.
//!
//! **Background re-probe (issue #43).** Without active recovery, a demoted
//! endpoint stays demoted forever while any other endpoint keeps serving
//! (its own [`candidate_order`] tier ranks below every healthy endpoint, so
//! `get()` never reaches it). [`spawn_reprobe_loop`] runs a pass every
//! `REPROBE_INTERVAL`, re-probing only demoted endpoints whose cooldown has
//! expired and promoting after `REPROBE_PROMOTE_AFTER` consecutive
//! successes (hysteresis, so a flapping endpoint cannot thrash). Each probe
//! borrows one permit from the pool's single semaphore via a **non-queuing**
//! `try_acquire` — it never enters the semaphore's FIFO wait queue, so it
//! can only ever borrow genuinely idle capacity; on any contention the rest
//! of the pass is deferred to the next tick rather than waiting behind real
//! callers.
//!
//! The single-semaphore, one-permit-per-[`PooledConn`] lease discipline is
//! unchanged from the single-endpoint pool: a `ChRowStream` still owns
//! exactly one permit for its whole lifetime, released on drop. Endpoint
//! selection happens *before* hand-out and never extends the lease.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{Semaphore, SemaphorePermit};

use crate::config::{ChConnConfig, ChProto};
use crate::error::ChError;
use crate::tls;

/// An endpoint is pinged before being handed out if it has been idle
/// (unselected/unvalidated) longer than this.
const STALE_AFTER: Duration = Duration::from_secs(30);
const STALE_AFTER_MS: u64 = 30_000;

/// After a transport failure demotes an endpoint, it is skipped entirely
/// (while any other endpoint is healthy) until this cooldown elapses, after
/// which it re-enters as a probe candidate. Bounds the "one failed request
/// per newly-dead endpoint" cost to at most one per cooldown window.
const UNHEALTHY_COOLDOWN_MS: u64 = 5_000;

/// How often the background re-probe pass ([`spawn_reprobe_loop`]) scans for
/// demoted, cooldown-expired endpoints.
const REPROBE_INTERVAL: Duration = Duration::from_secs(5);
/// Per-probe cap on the background re-probe's `SELECT 1` (a genuinely dead
/// endpoint must not stall a pass indefinitely).
const REPROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// Consecutive probe successes the background pass requires before
/// promoting a demoted endpoint (hysteresis: a single flaky success must not
/// resurrect a flapping endpoint). `get()`'s own emergency/staleness probe
/// is unaffected — it keeps promoting on a single success.
const REPROBE_PROMOTE_AFTER: u64 = 2;

/// The authoritative, lock-guarded half of [`EndpointHealth`]'s state.
/// Every transition (`mark_healthy`, `mark_unhealthy`, and both probe
/// appliers) mutates this struct and publishes the lock-free mirror atomics
/// inside the SAME critical section, so a generation snapshot taken under
/// the lock can never be interleaved with the transition it is meant to
/// observe atomically (issue #43 re-probe, Fix 2).
struct HealthState {
    healthy: bool,
    unhealthy_since_ms: u64,
    /// Consecutive probe successes since the last failure/demotion; reset by
    /// any demotion.
    probe_streak: u64,
    /// Bumped by every transition; a probe snapshots this immediately
    /// before its ping and applies its outcome only if unchanged.
    generation: u64,
}

/// The outcome of a generation-gated probe application
/// ([`EndpointHealth::apply_probe_success`]/[`EndpointHealth::apply_probe_failure`]).
#[derive(Debug, PartialEq, Eq)]
enum ProbeApply {
    /// This probe owned the transition (its snapshotted generation still
    /// matched); `promoted` is `true` only for a success that just crossed
    /// the promotion threshold.
    Applied { promoted: bool },
    /// Another transition (a real-traffic demotion, `mark_healthy`, or a
    /// concurrently-applied probe) landed first: this outcome is discarded
    /// entirely — no health write, no cooldown restart, no streak change.
    /// `healthy_now` is the health the WINNING transition left behind, read
    /// under the same lock acquisition (closes the stale-success re-read
    /// visibility gap a separate `is_healthy()` re-read would have).
    Stale { healthy_now: bool },
}

/// Health + telemetry for one endpoint. The hot path (`is_healthy`,
/// `cooldown_expired`, `is_stale`, `record_selection`) reads/writes only
/// lock-free published atomics — **zero locking** on the healthy+fresh
/// `get()` path (query-perf mandate). Every state *transition* instead goes
/// through the tiny `state` mutex, whose critical sections are field
/// assignments only (never held across an `.await`, so a panic under the
/// lock is already a bug — poisoning is unreachable in practice and handled
/// by `expect`). Times are milliseconds on the pool's monotonic `base`
/// clock.
struct EndpointHealth {
    /// Published lock-free mirror, written ONLY inside `state`'s critical
    /// section, ALWAYS as the last store (Release) so a reader observing
    /// `healthy == false` also observes the matching `unhealthy_since_ms`.
    healthy: AtomicBool,
    /// Published lock-free mirror of `state.unhealthy_since_ms`, written
    /// (Relaxed) BEFORE `healthy` inside the same critical section; the
    /// cooldown gate (`cooldown_expired`) reads only this atomic.
    unhealthy_since_ms: AtomicU64,
    /// `base`-relative ms of the last successful probe/hand-out; drives the
    /// staleness gate. Lock-free telemetry, not transition state: never
    /// gated by `state.generation` (a hand-out is not a health transition).
    last_checked_ms: AtomicU64,
    /// Cumulative hand-out count (telemetry / test observability). Same
    /// no-generation-bump treatment as `last_checked_ms`.
    selections: AtomicU64,
    /// Authoritative transition state (see [`HealthState`]).
    state: Mutex<HealthState>,
}

impl EndpointHealth {
    fn new() -> Self {
        Self {
            healthy: AtomicBool::new(true),
            unhealthy_since_ms: AtomicU64::new(0),
            // 0 = "never checked": stale until the first successful probe,
            // so a fresh endpoint is validated before its first hand-out
            // (mirrors the single-endpoint pool's initial-ping behavior).
            last_checked_ms: AtomicU64::new(0),
            selections: AtomicU64::new(0),
            state: Mutex::new(HealthState {
                healthy: true,
                unhealthy_since_ms: 0,
                probe_streak: 0,
                generation: 0,
            }),
        }
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HealthState> {
        // No `.await` ever occurs while this guard is held (probes run
        // between `probe_snapshot` and the `apply_*` call, never inside
        // either), so a panic under the lock is already a bug — `expect`
        // matches this crate's other non-async-mutex precedent
        // (`pulsus_write::writer::buffer::TableBuffer::lock`).
        self.state.lock().expect("endpoint health mutex poisoned")
    }

    /// Demotes the endpoint and (re)starts its cooldown from `now_ms`. One
    /// critical section: bumps the generation, mutates the authoritative
    /// fields, then publishes the mirrors (`unhealthy_since_ms` first,
    /// `healthy` last) before unlocking.
    fn mark_unhealthy(&self, now_ms: u64) {
        let mut state = self.lock();
        state.healthy = false;
        state.unhealthy_since_ms = now_ms;
        state.probe_streak = 0;
        state.generation += 1;
        self.unhealthy_since_ms.store(now_ms, Ordering::Relaxed);
        self.healthy.store(false, Ordering::Release);
    }

    /// Promotes the endpoint and marks it freshly validated at `now_ms`.
    /// Same one-critical-section protocol as [`Self::mark_unhealthy`].
    fn mark_healthy(&self, now_ms: u64) {
        let mut state = self.lock();
        state.healthy = true;
        state.probe_streak = 0;
        state.generation += 1;
        self.last_checked_ms.store(now_ms, Ordering::Relaxed);
        self.healthy.store(true, Ordering::Release);
    }

    /// True once an endpoint handed out at/after `last_checked_ms` has been
    /// idle past [`STALE_AFTER`] and must be re-probed before reuse.
    fn is_stale(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.last_checked_ms.load(Ordering::Relaxed)) >= STALE_AFTER_MS
    }

    /// True when a demoted endpoint's cooldown has expired, so it may be
    /// re-probed. Meaningful only while `!is_healthy()`.
    fn cooldown_expired(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.unhealthy_since_ms.load(Ordering::Relaxed))
            >= UNHEALTHY_COOLDOWN_MS
    }

    /// Records a successful hand-out: bumps the telemetry counter and, since a
    /// hand-out is itself a validation (a failing hand-out demotes via
    /// [`PooledConn::report_transport_failure`]), advances `last_checked_ms`.
    /// This keeps a continuously-selected HEALTHY endpoint fresh, so only
    /// genuinely idle (unselected) endpoints ever trip the staleness probe —
    /// matching [`STALE_AFTER`]'s "idle (unselected/unvalidated)" intent.
    /// Not a health transition (does not touch `state`/the generation): a
    /// hand-out changes no health/cooldown/streak field, so a concurrent
    /// probe outcome remains governed purely by actual transitions.
    fn record_selection(&self, now_ms: u64) {
        self.selections.fetch_add(1, Ordering::Relaxed);
        self.last_checked_ms.store(now_ms, Ordering::Relaxed);
    }

    fn selection_count(&self) -> u64 {
        self.selections.load(Ordering::Relaxed)
    }

    /// Snapshot of the transition generation, taken under the lock
    /// immediately BEFORE a probe's ping — the seam both probe paths
    /// (`get()`'s emergency/staleness probe and the background re-probe
    /// pass) use to detect a concurrently-applied transition.
    fn probe_snapshot(&self) -> u64 {
        self.lock().generation
    }

    /// Applies a probe success gated on `snapshot_gen` still matching the current
    /// generation. On success: bumps the streak; at `promote_after`
    /// consecutive successes, promotes (same field set as `mark_healthy`)
    /// and resets the streak. `get()`'s own probe calls with
    /// `promote_after == 1` (the existing single-success emergency/
    /// staleness promotion, unchanged); the background pass calls with
    /// [`REPROBE_PROMOTE_AFTER`].
    fn apply_probe_success(
        &self,
        snapshot_gen: u64,
        now_ms: u64,
        promote_after: u64,
    ) -> ProbeApply {
        let mut state = self.lock();
        if state.generation != snapshot_gen {
            return ProbeApply::Stale {
                healthy_now: state.healthy,
            };
        }
        state.probe_streak += 1;
        state.generation += 1;
        let promoted = state.probe_streak >= promote_after;
        if promoted {
            state.healthy = true;
            state.probe_streak = 0;
            self.last_checked_ms.store(now_ms, Ordering::Relaxed);
            self.healthy.store(true, Ordering::Release);
        }
        ProbeApply::Applied { promoted }
    }

    /// Applies a probe failure gated on `snapshot_gen` still matching the current
    /// generation. On success (the CAS, not the probe): streak := 0,
    /// demotes, and restarts the cooldown from `now_ms` — same field set as
    /// [`Self::mark_unhealthy`].
    fn apply_probe_failure(&self, snapshot_gen: u64, now_ms: u64) -> ProbeApply {
        let mut state = self.lock();
        if state.generation != snapshot_gen {
            return ProbeApply::Stale {
                healthy_now: state.healthy,
            };
        }
        state.healthy = false;
        state.unhealthy_since_ms = now_ms;
        state.probe_streak = 0;
        state.generation += 1;
        self.unhealthy_since_ms.store(now_ms, Ordering::Relaxed);
        self.healthy.store(false, Ordering::Release);
        ProbeApply::Applied { promoted: false }
    }
}

/// One dial target the pool spreads across: its `clickhouse::Client`, the
/// endpoint's availability `zone`, a stable `label` for telemetry, and its
/// lock-free health.
struct Endpoint {
    client: clickhouse::Client,
    zone: Option<String>,
    label: String,
    health: EndpointHealth,
}

/// The per-endpoint inputs [`candidate_order`] needs, borrowed so the hot
/// path allocates nothing beyond the returned order vector.
pub(crate) struct EndpointState<'a> {
    pub zone: Option<&'a str>,
    pub healthy: bool,
    /// For a demoted endpoint: whether its cooldown has expired (eligible to
    /// re-probe). Ignored when `healthy`.
    pub probe_ok: bool,
}

/// Pure, deterministic endpoint-selection policy (issue #43, the Tier-1
/// gate). Returns endpoint indices in preference order:
///
/// 1. healthy endpoints in `local_zone` (round-robin rotated by `rr`),
/// 2. healthy endpoints in other zones (round-robin rotated by `rr`),
/// 3. demoted endpoints whose cooldown has expired (`probe_ok`),
/// 4. the remaining (still-cooling) demoted endpoints.
///
/// With no `local_zone` (or when no endpoint matches it), tiers 1 collapses
/// into tier 2, degrading to even round-robin across all healthy endpoints.
/// The rotation makes successive `rr` values visit every member of a tier in
/// turn, which is what spreads load. No I/O, no allocation beyond the result.
pub(crate) fn candidate_order(
    states: &[EndpointState<'_>],
    local_zone: Option<&str>,
    rr: u64,
) -> Vec<usize> {
    let mut local_healthy = Vec::new();
    let mut other_healthy = Vec::new();
    let mut probe = Vec::new();
    let mut rest = Vec::new();

    for (i, s) in states.iter().enumerate() {
        if s.healthy {
            let is_local = match (local_zone, s.zone) {
                (Some(lz), Some(z)) => lz == z,
                _ => false,
            };
            if is_local {
                local_healthy.push(i);
            } else {
                other_healthy.push(i);
            }
        } else if s.probe_ok {
            probe.push(i);
        } else {
            rest.push(i);
        }
    }

    let mut order = Vec::with_capacity(states.len());
    rotate_into(&mut order, &local_healthy, rr);
    rotate_into(&mut order, &other_healthy, rr);
    rotate_into(&mut order, &probe, rr);
    rotate_into(&mut order, &rest, rr);
    order
}

/// Appends `tier`'s indices to `out`, rotated so element `rr % len` leads —
/// the round-robin step that spreads successive selections across the tier.
fn rotate_into(out: &mut Vec<usize>, tier: &[usize], rr: u64) {
    let len = tier.len();
    if len == 0 {
        return;
    }
    let start = (rr % len as u64) as usize;
    for k in 0..len {
        out.push(tier[(start + k) % len]);
    }
}

pub struct ChPool {
    endpoints: Vec<Endpoint>,
    semaphore: Arc<Semaphore>,
    /// Free-running round-robin counter; one `fetch_add` per `get()`.
    next_rr: AtomicU64,
    local_zone: Option<String>,
    /// Monotonic base for all endpoint timestamps. Offset into the past by
    /// [`STALE_AFTER`] so freshly-connected endpoints read as stale until
    /// their first successful probe.
    base: Instant,
    query_timeout: Duration,
}

impl ChPool {
    /// Connects the pool: builds one `clickhouse::Client` per resolved
    /// endpoint (single endpoint from `server`/`http_port` when no
    /// multi-endpoint list is configured) and validates them "ping-any" —
    /// the pool starts if **at least one** endpoint answers, so a partial
    /// cluster is still serviceable. Endpoints that fail their startup ping
    /// begin demoted and are skipped until they recover.
    pub async fn connect(cfg: ChConnConfig) -> Result<Self, ChError> {
        cfg.validate()?;
        let resolved = cfg.resolved_endpoints();
        let mut endpoints = Vec::with_capacity(resolved.len());
        for r in &resolved {
            endpoints.push(Endpoint {
                client: build_client(&cfg, &r.url)?,
                zone: r.zone.clone(),
                label: r.label.clone(),
                health: EndpointHealth::new(),
            });
        }

        let pool = Self {
            semaphore: Arc::new(Semaphore::new(cfg.pool_size)),
            endpoints,
            next_rr: AtomicU64::new(0),
            local_zone: cfg.local_zone.clone(),
            base: Instant::now()
                .checked_sub(STALE_AFTER)
                .unwrap_or_else(Instant::now),
            query_timeout: cfg.query_timeout,
        };

        // Fail fast only if EVERY endpoint is unreachable at startup: a
        // misconfigured server/credentials surfaces here rather than on the
        // first caller request, but a single down node in a multi-endpoint
        // deployment must not block startup.
        let now = pool.now_ms();
        let mut any_ok = false;
        let mut last_err = None;
        for ep in &pool.endpoints {
            match ping_client(&ep.client).await {
                Ok(()) => {
                    ep.health.mark_healthy(now);
                    any_ok = true;
                }
                Err(e) => {
                    ep.health.mark_unhealthy(now);
                    last_err = Some(e);
                }
            }
        }
        if any_ok {
            Ok(pool)
        } else {
            Err(last_err.unwrap_or_else(|| {
                ChError::Connect("no ClickHouse endpoints configured".to_string())
            }))
        }
    }

    fn now_ms(&self) -> u64 {
        self.base.elapsed().as_millis() as u64
    }

    /// Checks out a connection, blocking (bounded by the pool's own
    /// `query_timeout`) if all `pool_size` leases are already held. Selects
    /// an endpoint via [`candidate_order`] and probes it only when it is
    /// unhealthy or stale — the healthy hot path issues no ping. If the
    /// chosen candidate fails its probe it is demoted and the next candidate
    /// is tried; the last transport error is returned only if every
    /// candidate is unreachable.
    pub async fn get(&self) -> Result<PooledConn<'_>, ChError> {
        let permit = tokio::time::timeout(self.query_timeout, self.semaphore.acquire())
            .await
            .map_err(|_| ChError::Timeout("pool exhausted: no connection available".to_string()))?
            .expect("semaphore is never closed for the pool's lifetime");

        let rr = self.next_rr.fetch_add(1, Ordering::Relaxed);
        let now = self.now_ms();
        let states: Vec<EndpointState<'_>> = self
            .endpoints
            .iter()
            .map(|ep| EndpointState {
                zone: ep.zone.as_deref(),
                healthy: ep.health.is_healthy(),
                probe_ok: ep.health.cooldown_expired(now),
            })
            .collect();
        let order = candidate_order(&states, self.local_zone.as_deref(), rr);

        let mut last_err = None;
        for idx in order {
            let ep = &self.endpoints[idx];
            if !ep.health.is_healthy() || ep.health.is_stale(now) {
                let snapshot_gen = ep.health.probe_snapshot();
                match ping_client(&ep.client).await {
                    Ok(()) => match ep
                        .health
                        .apply_probe_success(snapshot_gen, self.now_ms(), 1)
                    {
                        ProbeApply::Applied { .. } | ProbeApply::Stale { healthy_now: true } => {}
                        ProbeApply::Stale { healthy_now: false } => {
                            // A fresher demotion (e.g. a concurrent transport
                            // failure) won the race after this ping was
                            // issued: this success is discarded, not handed
                            // out as if it were current.
                            continue;
                        }
                    },
                    Err(e) => {
                        // Applied or Stale, the effect on this candidate is
                        // the same: it is not usable this round. A stale
                        // failure is discarded entirely (no cooldown
                        // restart) rather than clobbering whatever fresher
                        // transition already landed.
                        ep.health.apply_probe_failure(snapshot_gen, self.now_ms());
                        last_err = Some(e);
                        continue;
                    }
                }
            }
            ep.health.record_selection(self.now_ms());
            return Ok(PooledConn {
                client: ep.client.clone(),
                endpoint_idx: idx,
                pool: self,
                _permit: permit,
            });
        }

        Err(last_err
            .unwrap_or_else(|| ChError::Connect("no ClickHouse endpoint available".to_string())))
    }

    /// Health probe (`SELECT 1`) against one selected endpoint. A transport
    /// failure here demotes that endpoint so the next `get()` fails over.
    pub async fn ping(&self) -> Result<(), ChError> {
        let conn = self.get().await?;
        let result = ping_client(conn.client()).await;
        if let Err(ref e) = result {
            conn.report_transport_failure(e);
        }
        result
    }

    /// Per-endpoint cumulative selection counts `(label, selections)`, in
    /// endpoint order. For ops/tests: proves spreading actually reached each
    /// endpoint (the metric-recorder wiring is a follow-up, plan §out-of-scope).
    pub fn endpoint_selection_counts(&self) -> Vec<(String, u64)> {
        self.endpoints
            .iter()
            .map(|ep| (ep.label.clone(), ep.health.selection_count()))
            .collect()
    }

    /// Per-endpoint health flags `(label, healthy)`, in endpoint order —
    /// lets ops/tests observe demotion (a failed-over endpoint reads
    /// `false`) without exposing the atomics.
    pub fn endpoint_health(&self) -> Vec<(String, bool)> {
        self.endpoints
            .iter()
            .map(|ep| (ep.label.clone(), ep.health.is_healthy()))
            .collect()
    }

    /// Per-endpoint demotion timestamps `(label, unhealthy_since_ms)`
    /// (`base`-relative ms; meaningful only while the endpoint is
    /// demoted), in endpoint order. Test/ops observability alongside
    /// [`Self::endpoint_health`]: an *applied* probe failure restarts the
    /// cooldown (advances this timestamp), so a test can distinguish "the
    /// probe ran and failed" from "the probe was skipped by the cooldown
    /// gate" — the latter leaves it unchanged (issue #43 code-review fix:
    /// the live re-probe test must prove the probe actually executed).
    pub fn endpoint_unhealthy_since_ms(&self) -> Vec<(String, u64)> {
        self.endpoints
            .iter()
            .map(|ep| {
                (
                    ep.label.clone(),
                    ep.health.unhealthy_since_ms.load(Ordering::Relaxed),
                )
            })
            .collect()
    }

    /// One real background re-probe pass (issue #43) at the pool's current
    /// clock: probes every demoted, cooldown-expired endpoint with a
    /// `SELECT 1` on its own client, capped at [`REPROBE_TIMEOUT`] (elapsed
    /// => [`ChError::Timeout`]). Healthy and still-cooling endpoints are
    /// never touched. Returns how many endpoints this pass promoted. See
    /// [`Self::reprobe_pass`] for the concurrency contract (non-queuing
    /// permit, generation-gated apply).
    pub async fn reprobe_demoted(&self) -> usize {
        self.reprobe_demoted_at(self.now_ms()).await
    }

    /// [`Self::reprobe_demoted`] with an explicit `base`-relative clock —
    /// the live-test counterpart of the hermetic `reprobe_pass` seam
    /// (issue #43 code-review fix). A freshly-demoted endpoint sits inside
    /// [`UNHEALTHY_COOLDOWN_MS`], where a wall-clock pass would skip it; a
    /// live test drives `now_ms` past the cooldown deterministically (no
    /// sleeps) so the REAL probe executes. `now_ms` only feeds the
    /// cooldown-expiry comparison and the timestamps recorded by applied
    /// outcomes — the probe I/O itself is identical to
    /// [`Self::reprobe_demoted`].
    pub async fn reprobe_demoted_at(&self, now_ms: u64) -> usize {
        self.reprobe_pass(now_ms, |idx| async move {
            let client = &self.endpoints[idx].client;
            match tokio::time::timeout(REPROBE_TIMEOUT, ping_client(client)).await {
                Ok(result) => result,
                Err(_) => Err(ChError::Timeout(
                    "background re-probe: SELECT 1 exceeded REPROBE_TIMEOUT".to_string(),
                )),
            }
        })
        .await
    }

    /// The deterministic seam behind [`Self::reprobe_demoted`]: an explicit
    /// clock and an injectable probe fn, so tests drive it with zero sleeps
    /// and a fake probe. Sequentially, per demoted+cooldown-expired
    /// endpoint (healthy and still-cooling endpoints are skipped with zero
    /// I/O and zero locking beyond the lock-free atomics already read by
    /// [`EndpointHealth::is_healthy`]/[`EndpointHealth::cooldown_expired`]):
    ///
    /// 1. try (never queue) to borrow one permit from the pool's single
    ///    semaphore via `try_acquire` — on contention this `break`s
    ///    immediately, deferring the REST of the pass to the next tick (the
    ///    pool is saturated; a probe must never wait behind real callers,
    ///    and tokio's semaphore is FIFO-fair so a freed permit goes to a
    ///    queued `get()` waiter, never to `try_acquire`);
    /// 2. snapshot the endpoint's transition generation, run `probe(idx)`,
    ///    drop the permit;
    /// 3. apply the outcome via the generation-gated appliers — a
    ///    concurrent transition (a real query's demotion, `get()`'s own
    ///    probe, or another pass) invalidates a stale snapshot, discarding
    ///    this outcome entirely rather than overwriting a fresher one.
    ///
    /// Promotion requires [`REPROBE_PROMOTE_AFTER`] consecutive successes.
    /// Returns how many endpoints this pass promoted.
    pub(crate) async fn reprobe_pass<F, Fut>(&self, now_ms: u64, probe: F) -> usize
    where
        F: Fn(usize) -> Fut,
        Fut: Future<Output = Result<(), ChError>>,
    {
        let mut promoted = 0;
        for (idx, ep) in self.endpoints.iter().enumerate() {
            if ep.health.is_healthy() || !ep.health.cooldown_expired(now_ms) {
                continue;
            }
            let permit = match self.semaphore.try_acquire() {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let snapshot_gen = ep.health.probe_snapshot();
            let outcome = probe(idx).await;
            drop(permit);
            match outcome {
                Ok(()) => {
                    if let ProbeApply::Applied { promoted: true } =
                        ep.health
                            .apply_probe_success(snapshot_gen, now_ms, REPROBE_PROMOTE_AFTER)
                    {
                        promoted += 1;
                        tracing::info!(
                            endpoint = %ep.label,
                            "background re-probe promoted a demoted endpoint"
                        );
                    }
                }
                Err(err) => {
                    ep.health.apply_probe_failure(snapshot_gen, now_ms);
                    tracing::debug!(
                        endpoint = %ep.label,
                        error = %err,
                        "background re-probe failed"
                    );
                }
            }
        }
        promoted
    }
}

/// Spawns the recurring background re-probe loop (issue #43): every
/// [`REPROBE_INTERVAL`] runs one [`ChPool::reprobe_demoted`] pass. Never
/// exits; abort+join it at shutdown, exactly like the rotation and label-
/// cache-refresh tasks (`pulsus_server::serve`'s
/// `shutdown_background_tasks`).
pub fn spawn_reprobe_loop(pool: Arc<ChPool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(REPROBE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            pool.reprobe_demoted().await;
        }
    })
}

/// Builds one `clickhouse::Client` for `url`. Routes through the skip-verify
/// TLS builder ([`tls::skip_verify_ch_client`]) only for `https` +
/// `tls_skip_verify` (docs/configuration.md §2); every other combination
/// (plain `http`, or verified `https` against public CAs) uses the crate's
/// own default `rustls-tls-webpki-roots` connector.
fn build_client(cfg: &ChConnConfig, url: &str) -> Result<clickhouse::Client, ChError> {
    let mut client = if cfg.proto == ChProto::Https && cfg.tls_skip_verify {
        tls::skip_verify_ch_client()?
    } else {
        clickhouse::Client::default()
    };
    client = client
        .with_url(url)
        .with_database(&cfg.database)
        .with_user(&cfg.user);
    if !cfg.password.is_empty() {
        client = client.with_password(&cfg.password);
    }
    Ok(client)
}

async fn ping_client(client: &clickhouse::Client) -> Result<(), ChError> {
    client
        .query("SELECT 1")
        .execute()
        .await
        .map_err(ChError::from)
}

/// An RAII lease on one of the pool's `pool_size` permits, pinned to the
/// endpoint chosen at hand-out. The permit is released back to the pool when
/// this value is dropped (including on early return / error / cancellation),
/// so a caller can never leak a lease by forgetting to call a `release()`
/// method.
pub struct PooledConn<'a> {
    client: clickhouse::Client,
    endpoint_idx: usize,
    pool: &'a ChPool,
    _permit: SemaphorePermit<'a>,
}

impl PooledConn<'_> {
    pub(crate) fn client(&self) -> &clickhouse::Client {
        &self.client
    }

    /// Demotes this lease's endpoint **iff** `err` is a transport-class
    /// (retryable) failure — a `Connect`/`Timeout`/`Io` or a retryable
    /// server code (see [`ChError::is_retryable`]). Bad-SQL / decode / config
    /// errors never demote (retrying them elsewhere would just reproduce the
    /// fault). Centralizing the `is_retryable` guard here keeps every call
    /// site (`insert_block`, `execute`, `ping`, `ChRowStream::poll_next`) a
    /// single, uniform line that cannot forget the guard.
    pub(crate) fn report_transport_failure(&self, err: &ChError) {
        if err.is_retryable() {
            self.pool.endpoints[self.endpoint_idx]
                .health
                .mark_unhealthy(self.pool.now_ms());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    #[test]
    fn stale_after_is_positive() {
        assert!(STALE_AFTER > Duration::ZERO);
    }

    fn state(zone: Option<&'static str>, healthy: bool, probe_ok: bool) -> EndpointState<'static> {
        EndpointState {
            zone,
            healthy,
            probe_ok,
        }
    }

    #[test]
    fn candidate_order_single_endpoint_is_always_index_zero() {
        for rr in 0..5 {
            let states = [state(None, true, false)];
            assert_eq!(candidate_order(&states, None, rr), vec![0]);
        }
    }

    #[test]
    fn candidate_order_unzoned_healthy_spreads_across_every_index() {
        // AC3(b): with no local zone, successive rr values lead with every
        // index in turn (even round-robin).
        let states = [
            state(None, true, false),
            state(None, true, false),
            state(None, true, false),
        ];
        let leads: Vec<usize> = (0..3)
            .map(|rr| candidate_order(&states, None, rr)[0])
            .collect();
        assert_eq!(leads, vec![0, 1, 2]);
        // And every order is a full permutation (all endpoints reachable).
        for rr in 0..3 {
            let mut order = candidate_order(&states, None, rr);
            order.sort_unstable();
            assert_eq!(order, vec![0, 1, 2]);
        }
    }

    #[test]
    fn candidate_order_prefers_local_zone_while_any_local_is_healthy() {
        // AC3(c): endpoints 0,1 in az-a (local); 2,3 in az-b. Every rr must
        // lead with a local-zone index while any local endpoint is healthy.
        let states = [
            state(Some("az-a"), true, false),
            state(Some("az-a"), true, false),
            state(Some("az-b"), true, false),
            state(Some("az-b"), true, false),
        ];
        for rr in 0..4 {
            let order = candidate_order(&states, Some("az-a"), rr);
            assert!(
                order[0] == 0 || order[0] == 1,
                "rr={rr}: expected a local (az-a) lead, got {order:?}"
            );
            // Both local indices precede both remote indices.
            let pos = |i: usize| order.iter().position(|&x| x == i).unwrap();
            assert!(pos(0).max(pos(1)) < pos(2).min(pos(3)));
        }
    }

    #[test]
    fn candidate_order_fails_over_to_other_zone_when_local_all_unhealthy() {
        // AC3(d): both local (az-a) endpoints demoted, cooldown NOT expired;
        // the healthy remote (az-b) endpoints must lead.
        let states = [
            state(Some("az-a"), false, false),
            state(Some("az-a"), false, false),
            state(Some("az-b"), true, false),
            state(Some("az-b"), true, false),
        ];
        for rr in 0..4 {
            let order = candidate_order(&states, Some("az-a"), rr);
            assert!(
                order[0] == 2 || order[0] == 3,
                "rr={rr}: expected an az-b failover lead, got {order:?}"
            );
        }
    }

    #[test]
    fn candidate_order_local_leads_again_once_recovered() {
        // AC3(e): a recovered local endpoint leads again.
        let states = [
            state(Some("az-a"), true, false),
            state(Some("az-b"), true, false),
        ];
        assert_eq!(candidate_order(&states, Some("az-a"), 0)[0], 0);
    }

    #[test]
    fn candidate_order_cooldown_expired_demoted_beats_still_cooling() {
        // A demoted-but-cooldown-expired endpoint (probe candidate) ranks
        // ahead of a demoted-still-cooling one, and both rank behind any
        // healthy endpoint.
        let states = [
            state(None, false, false), // still cooling
            state(None, false, true),  // cooldown expired -> probe candidate
            state(None, true, false),  // healthy
        ];
        let order = candidate_order(&states, None, 0);
        assert_eq!(order, vec![2, 1, 0]);
    }

    #[test]
    fn endpoint_health_demotes_then_recovers_after_cooldown() {
        // AC4: drive the clock via injected now_ms (no wall-time). A demoted
        // endpoint is excluded (unhealthy) and only becomes a probe
        // candidate once its cooldown elapses; a probe success re-promotes.
        let h = EndpointHealth::new();
        assert!(h.is_healthy());

        h.mark_unhealthy(1_000);
        assert!(!h.is_healthy());
        // Within the cooldown window: not yet a probe candidate.
        assert!(!h.cooldown_expired(1_000));
        assert!(!h.cooldown_expired(1_000 + UNHEALTHY_COOLDOWN_MS - 1));
        // Cooldown elapsed: eligible to re-probe.
        assert!(h.cooldown_expired(1_000 + UNHEALTHY_COOLDOWN_MS));

        // A successful probe re-promotes and clears staleness.
        h.mark_healthy(1_000 + UNHEALTHY_COOLDOWN_MS);
        assert!(h.is_healthy());
        assert!(!h.is_stale(1_000 + UNHEALTHY_COOLDOWN_MS));
    }

    #[test]
    fn endpoint_health_staleness_gate_trips_after_stale_after() {
        let h = EndpointHealth::new();
        h.mark_healthy(1_000);
        assert!(!h.is_stale(1_000 + STALE_AFTER_MS - 1));
        assert!(h.is_stale(1_000 + STALE_AFTER_MS));
    }

    #[test]
    fn selection_keeps_active_endpoint_fresh_while_idle_one_goes_stale() {
        // Finding 1: a hand-out is a validation, so a continuously-selected
        // HEALTHY endpoint keeps advancing `last_checked_ms` and NEVER trips
        // the staleness probe, while a validated-once-then-idle endpoint DOES.
        // Deterministic (injected clock), no wall-time.
        let active = EndpointHealth::new();
        active.mark_healthy(0);
        let mut t = 0;
        for _ in 0..4 {
            // Selected just before it would go stale; each hand-out revalidates.
            t += STALE_AFTER_MS - 1;
            assert!(!active.is_stale(t), "active endpoint read stale at t={t}");
            active.record_selection(t);
        }
        // Still fresh right up to STALE_AFTER past the LAST selection.
        assert!(!active.is_stale(t + STALE_AFTER_MS - 1));

        // An endpoint handed out once and never selected again goes stale and
        // must be re-probed.
        let idle = EndpointHealth::new();
        idle.mark_healthy(0);
        idle.record_selection(0);
        assert!(
            idle.is_stale(STALE_AFTER_MS),
            "idle (unselected) endpoint must trip the staleness probe"
        );
    }

    /// Builds a pool over endpoints with lazy (never-dialed) clients, all
    /// starting healthy + freshly checked, so `get()` on the hot path
    /// performs no I/O. For the selection/demotion unit tests only.
    fn test_pool(
        endpoints: Vec<(Option<&str>, &str)>,
        local_zone: Option<&str>,
        permits: usize,
    ) -> ChPool {
        let now_base = Instant::now();
        let eps: Vec<Endpoint> = endpoints
            .into_iter()
            .map(|(zone, label)| {
                let health = EndpointHealth::new();
                // Mark freshly checked so the hot path skips the probe.
                health.mark_healthy(0);
                Endpoint {
                    client: clickhouse::Client::default(),
                    zone: zone.map(str::to_string),
                    label: label.to_string(),
                    health,
                }
            })
            .collect();
        ChPool {
            semaphore: Arc::new(Semaphore::new(permits)),
            endpoints: eps,
            next_rr: AtomicU64::new(0),
            local_zone: local_zone.map(str::to_string),
            base: now_base,
            query_timeout: Duration::from_secs(5),
        }
    }

    #[tokio::test]
    async fn get_spreads_across_endpoints_without_io() {
        // Healthy + fresh endpoints -> get() returns immediately (no ping),
        // round-robining across both. Deterministic, hermetic proof of the
        // spreading policy on the real get() path.
        let pool = test_pool(
            vec![(None, "http://a:8123"), (None, "http://b:8123")],
            None,
            8,
        );
        for _ in 0..10 {
            let conn = pool.get().await.expect("hot-path get needs no server");
            drop(conn);
        }
        let counts = pool.endpoint_selection_counts();
        assert_eq!(counts.len(), 2);
        assert!(
            counts.iter().all(|(_, n)| *n > 0),
            "both endpoints used: {counts:?}"
        );
        assert_eq!(counts.iter().map(|(_, n)| *n).sum::<u64>(), 10);
    }

    #[tokio::test]
    async fn transport_failure_demotes_but_logic_error_does_not() {
        // Trip the demotion deterministically: a retryable (transport-class)
        // error demotes the leased endpoint; a non-retryable (logic) error
        // leaves it healthy.
        let pool = test_pool(vec![(None, "http://a:8123")], None, 8);

        let conn = pool.get().await.expect("get");
        conn.report_transport_failure(&ChError::Io("connection reset".to_string()));
        assert!(
            !pool.endpoint_health()[0].1,
            "a transport error must demote the endpoint"
        );

        // Recover, then a logic error must NOT demote.
        pool.endpoints[0].health.mark_healthy(pool.now_ms());
        let conn = pool.get().await.expect("get");
        conn.report_transport_failure(&ChError::Decode("bad row".to_string()));
        assert!(
            pool.endpoint_health()[0].1,
            "a logic error must never demote the endpoint"
        );
    }

    // ---- issue #43 re-probe: promotion, hysteresis, zero steady-state cost ----

    #[tokio::test]
    async fn reprobe_promotes_demoted_local_and_get_returns_to_it() {
        // AC1 (closes the retroactive re-review TEST GAP): a demoted LOCAL
        // endpoint is probed and, after REPROBE_PROMOTE_AFTER consecutive
        // successes, promoted — and real get() calls afterwards return to it,
        // even though a remote endpoint stayed healthy the whole time.
        let pool = test_pool(
            vec![
                (Some("az-a"), "http://local:8123"),
                (Some("az-b"), "http://remote:8123"),
            ],
            Some("az-a"),
            8,
        );
        pool.endpoints[0].health.mark_unhealthy(0);
        let now = UNHEALTHY_COOLDOWN_MS;

        let invocations = Arc::new(AtomicUsize::new(0));
        let make_ok_probe = |invocations: &Arc<AtomicUsize>| {
            let invocations = Arc::clone(invocations);
            move |idx: usize| {
                let invocations = Arc::clone(&invocations);
                async move {
                    invocations.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(idx, 0, "only the demoted local endpoint is ever probed");
                    Ok(())
                }
            }
        };

        let promoted = pool.reprobe_pass(now, make_ok_probe(&invocations)).await;
        assert_eq!(
            promoted, 0,
            "one success is not enough (REPROBE_PROMOTE_AFTER=2)"
        );
        assert_eq!(invocations.load(Ordering::Relaxed), 1);
        assert!(
            !pool.endpoint_health()[0].1,
            "still demoted after 1 success"
        );

        let promoted = pool.reprobe_pass(now, make_ok_probe(&invocations)).await;
        assert_eq!(promoted, 1, "2 consecutive successes promote");
        assert_eq!(invocations.load(Ordering::Relaxed), 2);
        assert!(
            pool.endpoint_health()[0].1,
            "local endpoint is healthy again"
        );
        assert!(
            pool.endpoint_health()[1].1,
            "remote endpoint was never touched"
        );

        for _ in 0..10 {
            let conn = pool.get().await.expect("get");
            drop(conn);
        }
        let counts = pool.endpoint_selection_counts();
        assert_eq!(counts[0].1, 10, "local leads again: {counts:?}");
        assert_eq!(counts[1].1, 0, "remote stayed idle: {counts:?}");
    }

    #[tokio::test]
    async fn reprobe_requires_consecutive_successes_and_failure_resets_streak() {
        // AC2: success, failure, success, success -> unhealthy until the 4th
        // pass; the failure restarts the cooldown, so a pass run inside the
        // restarted cooldown makes zero probe calls.
        let pool = test_pool(vec![(None, "http://a:8123")], None, 8);
        pool.endpoints[0].health.mark_unhealthy(0);
        let invocations = Arc::new(AtomicUsize::new(0));

        async fn record(invocations: &Arc<AtomicUsize>, ok: bool) -> Result<(), ChError> {
            invocations.fetch_add(1, Ordering::Relaxed);
            if ok {
                Ok(())
            } else {
                Err(ChError::Io("connection reset".to_string()))
            }
        }

        // Pass 1 @ t=5000 (cooldown just expired): success, streak=1.
        let inv = Arc::clone(&invocations);
        let promoted = pool
            .reprobe_pass(UNHEALTHY_COOLDOWN_MS, move |_idx| {
                let inv = Arc::clone(&inv);
                async move { record(&inv, true).await }
            })
            .await;
        assert_eq!(promoted, 0);
        assert_eq!(invocations.load(Ordering::Relaxed), 1);
        assert!(!pool.endpoint_health()[0].1);

        // Pass 2 @ t=5001: failure, streak resets, cooldown restarts from 5001.
        let inv = Arc::clone(&invocations);
        let promoted = pool
            .reprobe_pass(UNHEALTHY_COOLDOWN_MS + 1, move |_idx| {
                let inv = Arc::clone(&inv);
                async move { record(&inv, false).await }
            })
            .await;
        assert_eq!(promoted, 0);
        assert_eq!(invocations.load(Ordering::Relaxed), 2);
        assert!(!pool.endpoint_health()[0].1, "failure keeps it demoted");

        // Pass 3 @ t=5002: still inside the restarted cooldown -> 0 calls.
        let inv = Arc::clone(&invocations);
        let promoted = pool
            .reprobe_pass(UNHEALTHY_COOLDOWN_MS + 2, move |_idx| {
                let inv = Arc::clone(&inv);
                async move { record(&inv, true).await }
            })
            .await;
        assert_eq!(promoted, 0);
        assert_eq!(
            invocations.load(Ordering::Relaxed),
            2,
            "still cooling down after the failure: zero probe calls"
        );

        // Pass 4 @ t=10001 (restarted cooldown elapsed): success, streak=1.
        let inv = Arc::clone(&invocations);
        let promoted = pool
            .reprobe_pass(2 * UNHEALTHY_COOLDOWN_MS + 1, move |_idx| {
                let inv = Arc::clone(&inv);
                async move { record(&inv, true).await }
            })
            .await;
        assert_eq!(promoted, 0);
        assert_eq!(invocations.load(Ordering::Relaxed), 3);
        assert!(!pool.endpoint_health()[0].1, "unhealthy until the 4th pass");

        // Pass 5 @ t=10002: streak=2 -> promoted.
        let inv = Arc::clone(&invocations);
        let promoted = pool
            .reprobe_pass(2 * UNHEALTHY_COOLDOWN_MS + 2, move |_idx| {
                let inv = Arc::clone(&inv);
                async move { record(&inv, true).await }
            })
            .await;
        assert_eq!(promoted, 1);
        assert_eq!(invocations.load(Ordering::Relaxed), 4);
        assert!(
            pool.endpoint_health()[0].1,
            "2 consecutive successes promote"
        );
    }

    #[tokio::test]
    async fn reprobe_never_probes_healthy_or_cooling_endpoints() {
        // AC3: zero-cost-when-healthy — an all-healthy pool never invokes the
        // probe closure, and a demoted-but-still-cooling endpoint is skipped
        // too.
        let pool = test_pool(
            vec![(None, "http://a:8123"), (None, "http://b:8123")],
            None,
            8,
        );
        let invocations = Arc::new(AtomicUsize::new(0));

        let inv = Arc::clone(&invocations);
        let promoted = pool
            .reprobe_pass(0, move |_idx| {
                let inv = Arc::clone(&inv);
                async move {
                    inv.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            })
            .await;
        assert_eq!(promoted, 0);
        assert_eq!(invocations.load(Ordering::Relaxed), 0);

        pool.endpoints[0].health.mark_unhealthy(0);
        let inv = Arc::clone(&invocations);
        let promoted = pool
            .reprobe_pass(UNHEALTHY_COOLDOWN_MS - 1, move |_idx| {
                let inv = Arc::clone(&inv);
                async move {
                    inv.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            })
            .await;
        assert_eq!(promoted, 0);
        assert_eq!(
            invocations.load(Ordering::Relaxed),
            0,
            "a still-cooling demoted endpoint must not be probed"
        );
    }

    // ---- issue #43 re-probe v2/v3: bound preservation + race safety ----

    #[tokio::test(start_paused = true)]
    async fn reprobe_never_queues_and_borrows_only_idle_capacity() {
        // AC9 (v3): the background probe never queues behind a real caller
        // (phase 1), never steals a permit a queued real caller is owed
        // (phase 2), and only borrows a permit while the pool is genuinely
        // idle (phase 3) — proving it draws from the pool's single global
        // budget rather than a second, hidden one.
        let pool = Arc::new(test_pool(
            vec![
                (Some("az-a"), "http://a:8123"),
                (Some("az-a"), "http://b:8123"),
            ],
            Some("az-a"),
            1,
        ));
        pool.endpoints[1].health.mark_unhealthy(0);
        let now = UNHEALTHY_COOLDOWN_MS;

        // Phase 1: contention. `conn0` holds the sole permit; a real caller
        // is parked behind it in the semaphore's FIFO wait queue. The waiter
        // holds its own `PooledConn` for its whole task lifetime (a
        // `PooledConn<'_>` can't cross the spawn boundary as a return value —
        // the lease's lifetime is tied to the pool it borrows) and signals
        // acquisition/release over oneshot channels instead.
        let conn0 = pool.get().await.expect("conn0 takes the sole permit");
        let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let waiter_pool = Arc::clone(&pool);
        let waiter = tokio::spawn(async move {
            let conn = waiter_pool.get().await.expect("queued get() resolves");
            let _ = acquired_tx.send(());
            let _ = release_rx.await;
            drop(conn);
        });
        // Single-threaded (current-thread) test runtime: this deterministically
        // runs the freshly-spawned waiter up to its first pending await (the
        // semaphore acquire, which registers it in the FIFO queue) before
        // control returns here.
        tokio::task::yield_now().await;

        let invocations = Arc::new(AtomicUsize::new(0));
        let inv = Arc::clone(&invocations);
        let counting_probe = move |_idx: usize| {
            let inv = Arc::clone(&inv);
            async move {
                inv.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        };
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            pool.reprobe_pass(now, counting_probe.clone()),
        )
        .await;
        assert!(
            matches!(result, Ok(0)),
            "a saturated pool must return immediately with zero probes: {result:?}"
        );
        assert_eq!(invocations.load(Ordering::Relaxed), 0);
        assert!(!pool.endpoint_health()[1].1, "b untouched");

        // Phase 2: handoff. Freeing the permit must go to the QUEUED real
        // caller, never to a probe.
        drop(conn0);
        acquired_rx
            .await
            .expect("the queued get() resolves once the permit frees");
        assert_eq!(
            invocations.load(Ordering::Relaxed),
            0,
            "the freed permit must go to the queued caller, not a probe"
        );

        // Phase 3: idle. Releasing the waiter's connection (and joining its
        // task, so the permit is provably back in the semaphore before the
        // next pass runs) leaves nothing else holding or waiting on the
        // semaphore, so the probe borrows the one free permit.
        let _ = release_tx.send(());
        waiter.await.expect("waiter task did not panic");
        let observed_available = Arc::new(AtomicUsize::new(usize::MAX));
        let observed = Arc::clone(&observed_available);
        let pool_for_probe = Arc::clone(&pool);
        let promoted = pool
            .reprobe_pass(now, move |_idx| {
                let observed = Arc::clone(&observed);
                let pool_for_probe = Arc::clone(&pool_for_probe);
                async move {
                    observed.store(
                        pool_for_probe.semaphore.available_permits(),
                        Ordering::Relaxed,
                    );
                    Ok(())
                }
            })
            .await;
        assert_eq!(promoted, 0, "one success is not enough to promote yet");
        assert_eq!(
            observed_available.load(Ordering::Relaxed),
            0,
            "the probe must hold the pool's only permit while it runs"
        );
    }

    #[tokio::test]
    async fn stale_background_probe_failure_discarded_after_intervening_promotion() {
        // AC10 (v3), race forward: a background pass's failure outcome must
        // be discarded if a concurrent transition (e.g. get()'s own
        // emergency probe) already promoted the endpoint after the pass
        // snapshotted its generation.
        let pool = test_pool(vec![(None, "http://a:8123")], None, 8);
        pool.endpoints[0].health.mark_unhealthy(1_000);
        let now = 1_000 + UNHEALTHY_COOLDOWN_MS;

        let promoted = pool
            .reprobe_pass(now, |idx| {
                let ep = &pool.endpoints[idx];
                async move {
                    // The concurrent get() emergency probe lands first,
                    // using the same generation the background pass just
                    // snapshotted.
                    let snapshot_gen = ep.health.probe_snapshot();
                    let apply = ep.health.apply_probe_success(snapshot_gen, now, 1);
                    assert_eq!(apply, ProbeApply::Applied { promoted: true });
                    Err(ChError::Io("connection reset".to_string()))
                }
            })
            .await;

        assert_eq!(
            promoted, 0,
            "the stale failure must not count as a promotion"
        );
        assert!(
            pool.endpoint_health()[0].1,
            "the intervening promotion must survive the stale background failure"
        );
        assert_eq!(
            pool.endpoints[0]
                .health
                .unhealthy_since_ms
                .load(Ordering::Relaxed),
            1_000,
            "a discarded stale outcome must not restart the cooldown"
        );
    }

    #[tokio::test]
    async fn stale_get_probe_success_discarded_after_intervening_demotion() {
        // AC10 (v3), race reverse: a late get()-probe success applying
        // against a stale (pre-demotion) generation snapshot must be
        // discarded, not resurrect the endpoint.
        let pool = test_pool(vec![(None, "http://a:8123")], None, 8);
        pool.endpoints[0].health.mark_unhealthy(1_000);
        let now = 1_000 + UNHEALTHY_COOLDOWN_MS;

        // Snapshot BEFORE the background pass runs, as get()'s own (slower)
        // probe would have.
        let g0 = pool.endpoints[0].health.probe_snapshot();

        let promoted = pool
            .reprobe_pass(now, |_idx| async move {
                Err(ChError::Io("connection reset".to_string()))
            })
            .await;
        assert_eq!(promoted, 0);
        assert!(
            !pool.endpoint_health()[0].1,
            "the background failure demotes"
        );

        // The "late" get()-probe success now applies against the stale
        // snapshot.
        let apply = pool.endpoints[0].health.apply_probe_success(g0, now, 1);
        assert_eq!(apply, ProbeApply::Stale { healthy_now: false });
        assert!(
            !pool.endpoint_health()[0].1,
            "a stale success must not resurrect a fresher demotion"
        );
    }

    #[test]
    fn mark_transitions_participate_in_generation_protocol() {
        // AC10 (v3): mark_healthy/mark_unhealthy must also invalidate an
        // in-flight probe snapshot taken before them, not just the
        // generation-gated appliers invalidating each other.
        let h = EndpointHealth::new();
        h.mark_unhealthy(1_000);
        let gen0 = h.probe_snapshot();

        h.mark_healthy(2_000);
        let apply = h.apply_probe_failure(gen0, 3_000);
        assert_eq!(apply, ProbeApply::Stale { healthy_now: true });
        assert!(h.is_healthy(), "the mark_healthy transition must survive");
        assert_eq!(
            h.unhealthy_since_ms.load(Ordering::Relaxed),
            1_000,
            "a discarded stale probe failure must not touch the cooldown mirror"
        );
    }
}
