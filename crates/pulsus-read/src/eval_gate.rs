//! `EvalGate` — a process-wide bounded permit gating the one place the
//! read path offloads CPU-bound work onto tokio's blocking pool:
//! `MetricsEngine::query_inner` → `evaluate_offloaded` →
//! `spawn_blocking(pulsus_promql::evaluate)` (issue #101, hardening
//! follow-up to #93). The permit is acquired **after** the ClickHouse
//! fetch has fully drained into owned `SeriesData` and the owned permit is
//! moved **into** the `spawn_blocking` closure, so it is released only when
//! the blocking eval truly finishes — bounding in-flight evals *including*
//! evals for already-disconnected clients (tokio does not cancel a running
//! `spawn_blocking`; that is precisely why the bound matters).
//!
//! **Scope:** this gate covers only `pulsus_promql::evaluate`, the sole
//! production `spawn_blocking` eval. LogQL / TraceQL / trace-search
//! evaluation run inline on the reactor today (no `spawn_blocking`), so
//! there is nothing to bound there; gating them would first require
//! offloading them — a separate issue.
//!
//! **Exhaustion is a bounded wait**, not a fail-fast rejection: a caller
//! past the limit `acquire().await`s and queues, bounded by the existing
//! per-request `TimeoutLayer` (408, `query_timeout`). This deliberately
//! differs from the tail slot's fail-fast `try_acquire_owned`/`429` (a tail
//! holds its slot for the connection's whole lifetime; an eval permit for
//! ~hundreds of ms), so there is no new 429/503 status and no new timeout
//! knob.
//!
//! **Query-perf mandate:** the uncontended path is a single lock-free
//! `try_acquire_owned` — no clock, no atomic, no waker — so a query that
//! runs concurrently today under the (generous) limit is never serialized
//! or slowed. The wait instrumentation lives strictly on the contended
//! slow path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Default eval-concurrency limit (`reader.query_eval_concurrency`). A
/// fixed, generous constant: comfortably below tokio's 512 blocking-pool
/// ceiling (so offloaded evals can never monopolize the pool) yet far above
/// realistic simultaneous heavy-query fan-in (so the fast path is the norm
/// and nothing that runs concurrently today gets throttled).
pub const DEFAULT_EVAL_CONCURRENCY: usize = 256;

/// A bounded permit gating CPU-bound PromQL eval offloads. Cheap to clone
/// the inner `Arc<Semaphore>`; hold one per process (in `AppState`) so the
/// bound survives the per-request rebuild of `MetricsEngine`.
#[derive(Debug)]
pub struct EvalGate {
    sem: Arc<Semaphore>,
    limit: usize,
    /// Number of callers currently blocked in the contended slow path.
    /// A gauge, kept accurate across cancellation by `WaitGuard`.
    waiting: AtomicU64,
    /// Monotonic count of acquisitions that had to wait (took the slow
    /// path). Never touched on the fast path.
    contended_total: AtomicU64,
    /// Cumulative nanoseconds spent in *completed* contended waits. A wait
    /// abandoned by cancellation contributes nothing (intended). Two
    /// independent saturation mechanisms protect the exported value: each
    /// individual wait is clamped to `u64::MAX` at the cast in `acquire()`
    /// before accumulation, and the accumulation itself (`add_wait_nanos`)
    /// saturates at `u64::MAX` rather than wrapping, so the exported
    /// monotonic counter can never regress.
    wait_nanos_total: AtomicU64,
}

/// Point-in-time view of an [`EvalGate`], pulled on `/metrics` scrape
/// (mirrors the `LabelCache` snapshot→`ops.rs` model — the read path never
/// touches the `metrics` facade in its hot loop).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvalGateSnapshot {
    pub limit: usize,
    pub available: usize,
    /// `limit - available`, saturating (a permit reserved by a queued
    /// waiter never pushes this negative).
    pub in_flight: usize,
    pub waiting: u64,
    pub contended_total: u64,
    pub wait_nanos_total: u64,
}

/// Decrements `waiting` on drop — including the drop that happens when the
/// awaiting request future is cancelled mid-wait (408 timeout / client
/// disconnect), so the `waiting` gauge can never leak.
struct WaitGuard<'a>(&'a AtomicU64);

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl EvalGate {
    /// Builds a gate admitting `limit` concurrent evals.
    pub fn new(limit: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(limit)),
            limit,
            waiting: AtomicU64::new(0),
            contended_total: AtomicU64::new(0),
            wait_nanos_total: AtomicU64::new(0),
        }
    }

    /// Acquires an owned permit. **Fast path:** a single lock-free
    /// `try_acquire_owned` when a permit is free — no clock, no atomic, no
    /// waker touched, so it cannot serialize or slow a query that fits
    /// under the limit. **Slow path (contention only):** records the
    /// contention, arms a cancel-safe `WaitGuard` on `waiting`, times the
    /// wait, and `acquire_owned().await`s.
    ///
    /// The `.expect` is unreachable: the semaphore is never closed (it
    /// lives as long as the process's `AppState`).
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        if let Ok(permit) = Arc::clone(&self.sem).try_acquire_owned() {
            return permit;
        }
        self.contended_total.fetch_add(1, Ordering::Relaxed);
        self.waiting.fetch_add(1, Ordering::Relaxed);
        let guard = WaitGuard(&self.waiting);
        let started = std::time::Instant::now();
        let permit = Arc::clone(&self.sem)
            .acquire_owned()
            .await
            .expect("eval-gate semaphore is never closed");
        self.add_wait_nanos(started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
        drop(guard);
        permit
    }

    /// Accumulates a completed contended wait into `wait_nanos_total`,
    /// saturating at `u64::MAX` (never wraps — the exported counter stays
    /// monotonic). Single call site: `acquire()`'s contended slow path.
    #[inline]
    fn add_wait_nanos(&self, nanos: u64) {
        let _ = self
            .wait_nanos_total
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_add(nanos))
            }); // closure always returns Some => Err is unreachable
    }

    /// The single read-path choke point: acquire a permit, then run `f` on
    /// the blocking pool holding the owned permit for the closure's entire
    /// life. The permit is released on the blocking thread when `f`
    /// returns — NOT when the awaiter drops — so a disconnected-client eval
    /// still counts against the bound until it actually finishes. Returns
    /// the raw [`tokio::task::JoinError`] so callers keep their own panic
    /// policy.
    pub async fn run_blocking<F, T>(&self, f: F) -> Result<T, tokio::task::JoinError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let permit = self.acquire().await;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f()
        })
        .await
    }

    /// Snapshots the gate for `/metrics` (see [`EvalGateSnapshot`]).
    pub fn snapshot(&self) -> EvalGateSnapshot {
        let available = self.sem.available_permits();
        EvalGateSnapshot {
            limit: self.limit,
            available,
            in_flight: self.limit.saturating_sub(available),
            waiting: self.waiting.load(Ordering::Relaxed),
            contended_total: self.contended_total.load(Ordering::Relaxed),
            wait_nanos_total: self.wait_nanos_total.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::time::Duration;
    use tokio::sync::Semaphore as TokioSemaphore;

    /// Blocks the calling (blocking-pool) thread until `flag` is set,
    /// yielding the CPU between polls so the test's own runtime is never
    /// starved. Deterministic (no wall-time assert): it returns exactly
    /// when the driver flips the flag.
    fn spin_until(flag: &AtomicBool) {
        while !flag.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// AC2 — the bound actually caps concurrency. `EvalGate::new(N)` with
    /// `N + k` `run_blocking` closures: the first `N` enter and park on a
    /// shared release flag; the `k` extra provably queue at the gate. The
    /// max observed in-flight is read via `fetch_max` (no race) and, once
    /// all closures have run, equals `N` exactly — the bound is tight, not
    /// accidentally unreached. No wall-time assert.
    ///
    /// Issue #101 (re-review hardening): `entered.acquire_many(N)` alone
    /// only proves the first `N` closures entered — nothing forces the `k`
    /// excess closures to have been *polled* yet, so under the exact
    /// regression this test exists to catch (permit dropped before the
    /// closure runs), the over-admission assert below could pass vacuously
    /// if the scheduler simply hadn't run the excess closures yet. A second
    /// rendezvous closes that window: loop until `gate.snapshot().waiting
    /// == K` (every excess acquirer has provably reached the contended slow
    /// path and registered), asserting `entered.available_permits() == 0`
    /// on every iteration (an over-admission tripwire that fires the moment
    /// a broken gate lets an excess closure in). Termination is
    /// deterministic under any scheduler: a correct gate makes every excess
    /// task eventually reach `acquire()`'s slow path; a broken gate makes it
    /// eventually enter and trip the in-loop assert instead. No sleeps, no
    /// wall-time bound. The `contended_total == K` identity is then
    /// provable because no permit is released (via `release`/`admitted`)
    /// before this rendezvous completes — every earlier acquirer holder is
    /// still parked, so exactly `N` fast-path successes and `K` slow-path
    /// acquisitions have occurred.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bound_caps_concurrency_at_the_configured_limit() {
        const N: usize = 2;
        const K: usize = 3;

        let gate = Arc::new(EvalGate::new(N));
        let release = Arc::new(AtomicBool::new(false));
        // Counts closures that have started running (i.e. hold a permit).
        let entered = Arc::new(TokioSemaphore::new(0));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..(N + K) {
            let gate = Arc::clone(&gate);
            let release = Arc::clone(&release);
            let entered = Arc::clone(&entered);
            let in_flight = Arc::clone(&in_flight);
            let max_seen = Arc::clone(&max_seen);
            handles.push(tokio::spawn(async move {
                gate.run_blocking(move || {
                    let cur = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(cur, Ordering::SeqCst);
                    entered.add_permits(1);
                    spin_until(&release);
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                })
                .await
                .unwrap();
            }));
        }

        // Wait until exactly N closures are running (they parked on the
        // release flag). `acquire_many(N)` completes only once N permits
        // have been added — i.e. N closures entered.
        let admitted = entered.acquire_many(N as u32).await.unwrap();
        // The k extra are queued at the gate, not running: no further
        // entry permit exists, and no more than N are in flight.
        assert_eq!(
            entered.available_permits(),
            0,
            "only N closures may be running while the gate is full"
        );
        assert!(
            max_seen.load(Ordering::SeqCst) <= N,
            "in-flight evals must never exceed the configured limit"
        );
        assert_eq!(gate.snapshot().available, 0, "the gate is fully occupied");

        // Second rendezvous: force every excess acquirer to be provably
        // queued at the gate before trusting the over-admission tripwire.
        loop {
            assert_eq!(
                entered.available_permits(),
                0,
                "over-admission: more than N closures entered while the gate is full"
            );
            if gate.snapshot().waiting == K as u64 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(gate.snapshot().available, 0, "the gate is fully occupied");
        assert_eq!(
            gate.snapshot().contended_total,
            K as u64,
            "exactly the K excess acquisitions take the contended slow path"
        );

        // Release everyone and let all N+k run to completion.
        drop(admitted);
        release.store(true, Ordering::SeqCst);
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            N,
            "the bound is tight: exactly N ran concurrently, never fewer or more"
        );
        assert_eq!(
            gate.snapshot().available,
            N,
            "every permit is returned after the evals finish"
        );
    }

    /// AC3 — the uncontended fast path (query-perf gate). With a free
    /// permit, `acquire()` is `Poll::Ready` on its first poll and leaves
    /// every wait counter at zero, and `limit` concurrent `run_blocking`
    /// closures all enter with `contended_total == 0` (no false
    /// serialization of queries that fit under the limit).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uncontended_fast_path_is_ready_immediately_and_uninstrumented() {
        let gate = EvalGate::new(4);

        // First poll is Ready — no waker registered, no slow path taken.
        let permit = {
            let fut = gate.acquire();
            futures::pin_mut!(fut);
            match futures::poll!(fut.as_mut()) {
                std::task::Poll::Ready(p) => p,
                std::task::Poll::Pending => {
                    panic!("uncontended acquire must be Ready on first poll")
                }
            }
        };
        let snap = gate.snapshot();
        assert_eq!(snap.contended_total, 0);
        assert_eq!(snap.waiting, 0);
        assert_eq!(snap.wait_nanos_total, 0);
        drop(permit);

        // `limit` concurrent evals all enter with zero contention.
        const LIMIT: usize = 4;
        let gate = Arc::new(EvalGate::new(LIMIT));
        let release = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(TokioSemaphore::new(0));
        let mut handles = Vec::new();
        for _ in 0..LIMIT {
            let gate = Arc::clone(&gate);
            let release = Arc::clone(&release);
            let entered = Arc::clone(&entered);
            handles.push(tokio::spawn(async move {
                gate.run_blocking(move || {
                    entered.add_permits(1);
                    spin_until(&release);
                })
                .await
                .unwrap();
            }));
        }
        let _admitted = entered.acquire_many(LIMIT as u32).await.unwrap();
        assert_eq!(
            gate.snapshot().contended_total,
            0,
            "limit concurrent evals must all enter without any contention"
        );
        release.store(true, Ordering::SeqCst);
        for h in handles {
            h.await.unwrap();
        }
    }

    /// AC4 — cancel safety. Holding the sole permit, a queued waiter shows
    /// `waiting == 1`; aborting it returns `waiting` to 0 and leaks no
    /// permit (after the held permit drops, `available == limit` and a
    /// fresh `acquire()` succeeds).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborted_waiter_leaks_neither_the_gauge_nor_the_permit() {
        let gate = Arc::new(EvalGate::new(1));
        let held = gate.acquire().await;

        let g = Arc::clone(&gate);
        let waiter = tokio::spawn(async move {
            let _p = g.acquire().await;
        });

        // Deterministically wait for the waiter to reach the slow path.
        loop {
            if gate.snapshot().waiting == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }

        waiter.abort();
        let _ = waiter.await;

        // The gauge returns to 0 even though the wait never completed.
        loop {
            if gate.snapshot().waiting == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        // A cancelled wait records no wait time.
        assert_eq!(gate.snapshot().wait_nanos_total, 0);

        drop(held);
        assert_eq!(
            gate.snapshot().available,
            1,
            "aborting a queued waiter must not leak the reserved permit"
        );
        let _fresh = gate.acquire().await;
    }

    /// AC5 — the permit spans the whole blocking closure (the in-flight /
    /// disconnected-client bound). While a `run_blocking` closure is parked
    /// inside the blocking pool, `available == 0` and a second `acquire()`
    /// queues (`waiting == 1`); only when the closure returns does the
    /// second acquisition proceed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn permit_is_held_through_the_entire_blocking_closure() {
        let gate = Arc::new(EvalGate::new(1));
        let release = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(TokioSemaphore::new(0));

        let g = Arc::clone(&gate);
        let r = Arc::clone(&release);
        let e = Arc::clone(&entered);
        let worker = tokio::spawn(async move {
            g.run_blocking(move || {
                e.add_permits(1);
                spin_until(&r);
            })
            .await
            .unwrap();
        });

        // Once the closure is running, the sole permit is taken.
        let _ = entered.acquire().await.unwrap();
        assert_eq!(gate.snapshot().available, 0);

        // A second acquisition must queue while the closure runs.
        let g2 = Arc::clone(&gate);
        let second = tokio::spawn(async move {
            let _p = g2.acquire().await;
        });
        loop {
            if gate.snapshot().waiting == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }

        // Releasing the closure frees the permit for the second acquirer.
        release.store(true, Ordering::SeqCst);
        worker.await.unwrap();
        second.await.unwrap();
        assert_eq!(gate.snapshot().available, 1);
        assert_eq!(gate.snapshot().contended_total, 1);
    }

    /// Issue #133: the eval-concurrency bound still fires at the maximum
    /// config-accepted `reader.query_eval_concurrency`. Construction at
    /// the ceiling must not panic (tokio `Semaphore::MAX_PERMITS`),
    /// exactly `ceiling` permits are admitted, and the next acquisition
    /// is denied — the bound is not disable-able by any value config
    /// load accepts. Synchronous permit probes, no runtime needed.
    #[test]
    fn gate_at_the_max_accepted_concurrency_admits_exactly_the_ceiling() {
        let ceiling = pulsus_config::QUERY_EVAL_CONCURRENCY_CEILING;
        let gate = EvalGate::new(ceiling);
        let held = gate
            .sem
            .try_acquire_many(u32::try_from(ceiling).expect("ceiling fits u32"))
            .expect("the ceiling-permit gate must admit exactly ceiling permits");
        assert!(
            gate.sem.try_acquire().is_err(),
            "the ceiling+1-th eval must be denied at the accepted max"
        );
        drop(held);
        assert_eq!(gate.snapshot().available, ceiling);
    }

    /// Issue #101 (plan v2 — saturating wait accumulator). Two legs, both
    /// clock-free / deterministic:
    ///
    /// 1. **Unit leg:** seed `wait_nanos_total` directly at `u64::MAX - 1`
    ///    (private-field access, no clock), then `add_wait_nanos(2)` must
    ///    saturate at `u64::MAX` rather than wrap; a further
    ///    `add_wait_nanos(5)` must leave it pinned at `u64::MAX`. The old
    ///    `fetch_add` code wraps the first call to `0` — deterministic
    ///    discrimination, no scheduler/clock dependence.
    /// 2. **Integration leg:** same seed, but the accumulation is driven by
    ///    one real contended wait through `acquire()` (hold the sole permit
    ///    of `EvalGate::new(1)`, spawn a waiter, rendezvous on
    ///    `waiting == 1`, release, join). The exported counter must never
    ///    regress below the seed — a wrap would land it near 0.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_accumulator_saturates_at_u64_max_instead_of_wrapping() {
        // --- Unit leg: direct, clock-free accumulator exercise.
        let gate = EvalGate::new(1);
        gate.wait_nanos_total.store(u64::MAX - 1, Ordering::Relaxed);
        gate.add_wait_nanos(2);
        assert_eq!(
            gate.snapshot().wait_nanos_total,
            u64::MAX,
            "cumulative accumulation must saturate at u64::MAX, not wrap"
        );
        gate.add_wait_nanos(5);
        assert_eq!(
            gate.snapshot().wait_nanos_total,
            u64::MAX,
            "the saturated value must stay pinned at u64::MAX, never move"
        );

        // --- Integration leg: one real contended wait through acquire().
        let gate = Arc::new(EvalGate::new(1));
        gate.wait_nanos_total.store(u64::MAX - 1, Ordering::Relaxed);
        let held = gate.acquire().await;

        let g = Arc::clone(&gate);
        let waiter = tokio::spawn(async move {
            let _p = g.acquire().await;
        });

        loop {
            if gate.snapshot().waiting == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }

        drop(held);
        waiter.await.unwrap();

        assert!(
            gate.snapshot().wait_nanos_total >= u64::MAX - 1,
            "the counter must never regress below the seed through the real accumulation path"
        );
    }
}
