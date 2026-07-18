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
//! the *next* `get()` fails over. Health is tracked entirely with lock-free
//! atomics, so demotion never needs an async mutex (and never runs in
//! `Drop`).
//!
//! The single-semaphore, one-permit-per-[`PooledConn`] lease discipline is
//! unchanged from the single-endpoint pool: a `ChRowStream` still owns
//! exactly one permit for its whole lifetime, released on drop. Endpoint
//! selection happens *before* hand-out and never extends the lease.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

/// Lock-free health + telemetry for one endpoint. All state is atomic so
/// demotion can fire from any context (including a stream's error arm)
/// without an async mutex. Times are milliseconds on the pool's monotonic
/// `base` clock.
struct EndpointHealth {
    healthy: AtomicBool,
    /// `base`-relative ms of the most recent demotion; drives the cooldown.
    unhealthy_since_ms: AtomicU64,
    /// `base`-relative ms of the last successful probe/hand-out; drives the
    /// staleness gate.
    last_checked_ms: AtomicU64,
    /// Cumulative hand-out count (telemetry / test observability).
    selections: AtomicU64,
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
        }
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    /// Demotes the endpoint and (re)starts its cooldown from `now_ms`.
    fn mark_unhealthy(&self, now_ms: u64) {
        self.healthy.store(false, Ordering::Release);
        self.unhealthy_since_ms.store(now_ms, Ordering::Relaxed);
    }

    /// Promotes the endpoint and marks it freshly validated at `now_ms`.
    fn mark_healthy(&self, now_ms: u64) {
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
    fn record_selection(&self, now_ms: u64) {
        self.selections.fetch_add(1, Ordering::Relaxed);
        self.last_checked_ms.store(now_ms, Ordering::Relaxed);
    }

    fn selection_count(&self) -> u64 {
        self.selections.load(Ordering::Relaxed)
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
                match ping_client(&ep.client).await {
                    Ok(()) => ep.health.mark_healthy(self.now_ms()),
                    Err(e) => {
                        ep.health.mark_unhealthy(self.now_ms());
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
    fn test_pool(endpoints: Vec<(Option<&str>, &str)>, local_zone: Option<&str>) -> ChPool {
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
            semaphore: Arc::new(Semaphore::new(8)),
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
        let pool = test_pool(vec![(None, "http://a:8123"), (None, "http://b:8123")], None);
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
        let pool = test_pool(vec![(None, "http://a:8123")], None);

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
}
