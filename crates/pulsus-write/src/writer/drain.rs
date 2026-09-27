//! The metrics landing path's shutdown boundary (issue #603).
//!
//! **One rule lives here: no attempt starts after the announced shutdown
//! deadline, and nothing the writer spawned is abandoned at it.** Three review
//! rounds found three different ways past that rule while each half of it was
//! written where it was needed — a retry sleep the deadline did not bound, a
//! check the poll could slip past, a settlement task nobody joined. Every part
//! of it is this type's now, and the landing path states none of it again.
//!
//! **What the deadline does not bound is settling a block.** The spool copy is
//! the push's only one, so the drain finishes writing it rather than dropping
//! it at the deadline, and the drain overruns by what that write costs. That is
//! the choice the landing path makes everywhere: a block is never lost to save
//! time.
//!
//! What makes a fourth way past the rule structural rather than lucky:
//!
//! - **An attempt is created here and nowhere else.**
//!   [`DrainWatch::attempt`] takes the constructor, not the future, and calls
//!   it from inside the selection whose deadline arm is biased to win. A
//!   deadline already passed is a zero-length sleep, ready at its first poll,
//!   so the constructor is never called: there is no check to skip and no gap
//!   between a check and a poll for the deadline to expire in.
//! - **Every wait between attempts is [`DrainWatch::sleep`]**, which ends at
//!   the deadline whatever its own length.
//! - **Admission runs inside an [`AdmissionPass`]**, and
//!   [`DrainBoundary::shutdown`] waits for every outstanding pass *before* it
//!   announces the deadline. So no block reaches a closed queue, and a
//!   settlement task — which only a pass can spawn — cannot appear after the
//!   set of them has been joined.
//!
//! **The residual, stated rather than claimed away**: an attempt polled before
//! the deadline and still in flight at it is abandoned, which is the uncertain
//! ending. That is not work starting after the deadline, and it is the one
//! thing no boundary can remove — a send already on the wire cannot be
//! recalled.

use std::future::Future;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::warn;

/// The one owner of the shutdown boundary: the announced deadline, the
/// admission gate and every task the landing path spawns. See the module
/// docs.
pub(crate) struct DrainBoundary {
    /// The announced deadline, `None` until the drain begins. A `watch`
    /// channel so a task blocked inside an insert observes the announcement
    /// the next time it reaches a selection, with no lost-wakeup window.
    signal: watch::Sender<Option<Instant>>,
    /// Admissions inside a pass right now.
    passes: AtomicU64,
    /// Set before the first wait for `passes` to reach zero, so no admission
    /// that reads it can still be holding a pass afterwards.
    closed: AtomicBool,
    /// The insert workers and the suppression ticker.
    background: Mutex<Vec<JoinHandle<()>>>,
    /// Settlements spawned from inside a pass.
    settlements: Mutex<Vec<JoinHandle<()>>>,
}

/// Proof that an admission is inside the boundary: while one of these exists,
/// [`DrainBoundary::shutdown`] has not announced the deadline and has not
/// joined the settlement set.
pub(crate) struct AdmissionPass<'a> {
    boundary: &'a DrainBoundary,
}

impl Drop for AdmissionPass<'_> {
    fn drop(&mut self) {
        self.boundary.passes.fetch_sub(1, Ordering::SeqCst);
    }
}

impl DrainBoundary {
    pub(crate) fn new() -> Self {
        let (signal, _rx) = watch::channel(None);
        DrainBoundary {
            signal,
            passes: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            background: Mutex::new(Vec::new()),
            settlements: Mutex::new(Vec::new()),
        }
    }

    /// One task's view of the deadline. Taken once per task: a
    /// `watch::Receiver`'s "have I seen this version" state must not be shared
    /// across concurrent awaiters.
    pub(crate) fn watch(&self) -> DrainWatch {
        DrainWatch {
            rx: self.signal.subscribe(),
        }
    }

    /// The pass an admission runs inside, or `None` once the drain has begun —
    /// which is the `Backpressure` the caller is refused with.
    ///
    /// The increment and the flag are both `SeqCst`: an admission that reads
    /// the flag as clear must be visible to [`Self::shutdown`]'s count, and
    /// that pair of stores and pair of loads only rule each other out under a
    /// single total order.
    pub(crate) fn enter(&self) -> Option<AdmissionPass<'_>> {
        // Refused without taking a pass, so a caller still pushing after the
        // drain began leaves nothing for [`Self::close_admission`] to wait on.
        // This read is a fast path and not the decision: the pair below is.
        if self.closed.load(Ordering::SeqCst) {
            return None;
        }
        self.passes.fetch_add(1, Ordering::SeqCst);
        let pass = AdmissionPass { boundary: self };
        if self.closed.load(Ordering::SeqCst) {
            return None;
        }
        Some(pass)
    }

    /// Spawns a task that settles a block admission could not hand to the
    /// queue, tracked so [`Self::shutdown`] awaits it.
    ///
    /// It takes the pass rather than looking one up, because holding a pass is
    /// what proves the settlement set has not been joined yet: `shutdown`
    /// joins it only once no pass is outstanding and no further one can be
    /// taken.
    pub(crate) fn spawn_settlement(
        &self,
        _pass: &AdmissionPass<'_>,
        task: impl Future<Output = ()> + Send + 'static,
    ) {
        let handle = tokio::spawn(task);
        self.settlements
            .lock()
            .expect("task handle mutex poisoned")
            .push(handle);
    }

    /// Registers the insert workers and the suppression ticker, which run for
    /// the writer's whole life rather than inside one admission.
    pub(crate) fn track_background(&self, handles: impl IntoIterator<Item = JoinHandle<()>>) {
        self.background
            .lock()
            .expect("task handle mutex poisoned")
            .extend(handles);
    }

    /// Stops admitting, announces `deadline`, and returns once every task the
    /// writer spawned has finished. Idempotent.
    ///
    /// The order is the boundary's whole argument. Admission closes **first**,
    /// so by the time the deadline is announced every block that will ever be
    /// queued is queued, and no settlement can be spawned after the set of
    /// them is joined last.
    pub(crate) async fn shutdown(&self, deadline: Duration) {
        self.close_admission().await;
        // `send_replace`, not `send`: `send` reports every receiver being gone
        // by leaving the value alone, and the value is what a task taking its
        // view later reads.
        self.signal.send_replace(Some(Instant::now() + deadline));
        let background = self.take_background();
        for task in background {
            if let Err(e) = task.await {
                warn!(error = %e, "a metrics landing task panicked during shutdown");
            }
        }
        // Taken until empty rather than once: no pass is outstanding and none
        // can be taken, so one turn is enough, and a later turn costs nothing
        // and holds whatever order a caller gives these steps.
        loop {
            let settlements = self.take_settlements();
            if settlements.is_empty() {
                return;
            }
            for task in settlements {
                if let Err(e) = task.await {
                    warn!(error = %e, "a metrics landing settlement panicked during shutdown");
                }
            }
        }
    }

    /// Refuses every later admission, then waits for the ones already inside.
    ///
    /// The wait is a yield loop rather than a notification: a pass is held
    /// across admission's own body, which never awaits, so what this waits for
    /// is bounded by one admission's synchronous work.
    async fn close_admission(&self) {
        self.closed.store(true, Ordering::SeqCst);
        while self.passes.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    }

    /// The background handles, taken out so no lock is held across an await.
    fn take_background(&self) -> Vec<JoinHandle<()>> {
        std::mem::take(&mut *self.background.lock().expect("task handle mutex poisoned"))
    }

    /// The settlement handles registered so far, taken out so no lock is held
    /// across an await.
    fn take_settlements(&self) -> Vec<JoinHandle<()>> {
        std::mem::take(&mut *self.settlements.lock().expect("task handle mutex poisoned"))
    }

    /// Clears the closed flag after [`Self::shutdown`] has returned, so the
    /// next admission passes the gate and meets the closed queue — the race
    /// the gate makes unreachable from outside.
    #[doc(hidden)]
    pub(crate) fn reopen_for_test(&self) {
        self.closed.store(false, Ordering::SeqCst);
    }
}

/// How one attempt ended. The three endings short of an outcome are the
/// boundary's, not the caller's: **the caller cannot reach the two shutdown
/// endings by its own reasoning about the clock**, and cannot start work the
/// boundary would have refused.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AttemptEnd<T> {
    /// The deadline had passed. The work was never created and never polled,
    /// so nothing was sent.
    NotStarted,
    /// In flight when the deadline passed. Abandoned, and it may have
    /// committed.
    Abandoned,
    /// The budget elapsed during the attempt.
    BudgetElapsed,
    /// It finished inside both bounds.
    Done(T),
}

/// One task's view of the deadline: the only way to run an attempt or to wait
/// between two.
pub(crate) struct DrainWatch {
    rx: watch::Receiver<Option<Instant>>,
}

impl DrainWatch {
    /// The announced deadline, if the drain has begun.
    pub(crate) fn announced(&mut self) -> Option<Instant> {
        *self.rx.borrow_and_update()
    }

    /// Whether the drain has begun.
    pub(crate) fn is_announced(&mut self) -> bool {
        self.announced().is_some()
    }

    /// Returns once the drain has begun — or once the signal is gone, which is
    /// the writer dropped without a graceful shutdown and ends a task the same
    /// way.
    pub(crate) async fn until_announced(&mut self) {
        while !self.is_announced() {
            if self.rx.changed().await.is_err() {
                return;
            }
        }
    }

    /// Runs one attempt, bounded by `budget` and by the announced deadline.
    ///
    /// `make` builds the work. It is called from inside the selection, after
    /// the deadline arm has been polled and found pending — which is what
    /// proves the deadline had not passed — so an attempt cannot be created
    /// once it has. See the module docs.
    pub(crate) async fn attempt<F, T>(
        &mut self,
        budget: Duration,
        make: impl FnOnce() -> F,
    ) -> AttemptEnd<T>
    where
        F: Future<Output = T>,
    {
        // `make` is called by the first poll of this block and not before, so
        // "never polled" and "never created" are one thing here.
        let started = AtomicBool::new(false);
        let work = async {
            started.store(true, Ordering::Relaxed);
            make().await
        };
        let bounded = tokio::time::timeout(budget, work);
        tokio::pin!(bounded);
        loop {
            match self.announced() {
                Some(deadline) => {
                    // `std::time::Instant`: the deadline is the shutdown
                    // caller's own clock reading, and a deadline already passed
                    // leaves a zero-length sleep — ready at its first poll.
                    let left = deadline.saturating_duration_since(Instant::now());
                    let expire = tokio::time::sleep(left);
                    tokio::pin!(expire);
                    return tokio::select! {
                        biased;
                        // The deadline arm first, so it wins rather than
                        // relying on poll order: with the deadline passed it is
                        // ready before the attempt is polled at all, and
                        // nothing is created after the deadline.
                        () = &mut expire => if started.load(Ordering::Relaxed) {
                            AttemptEnd::Abandoned
                        } else {
                            AttemptEnd::NotStarted
                        },
                        outcome = &mut bounded => match outcome {
                            Ok(value) => AttemptEnd::Done(value),
                            Err(_elapsed) => AttemptEnd::BudgetElapsed,
                        },
                    };
                }
                None => {
                    // No deadline is announced, so nothing here needs one to
                    // win. The attempt is polled first, which leaves a closed
                    // signal — the writer dropped without a graceful
                    // shutdown — abandoning an attempt in flight rather than
                    // refusing to start one.
                    tokio::select! {
                        biased;
                        outcome = &mut bounded => return match outcome {
                            Ok(value) => AttemptEnd::Done(value),
                            Err(_elapsed) => AttemptEnd::BudgetElapsed,
                        },
                        changed = self.rx.changed() => {
                            if changed.is_err() {
                                return if started.load(Ordering::Relaxed) {
                                    AttemptEnd::Abandoned
                                } else {
                                    AttemptEnd::NotStarted
                                };
                            }
                        }
                    }
                }
            }
        }
    }

    /// Waits `delay`, returning as soon as an announced deadline has passed —
    /// the caller's own loop then settles, so this reports nothing back.
    ///
    /// A wait that ignored the deadline would end after it, and the next turn
    /// of that loop would ask for an attempt the drain is no longer waiting
    /// for.
    pub(crate) async fn sleep(&mut self, delay: Duration) {
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            match self.announced() {
                // Announced: whichever of the two comes first ends the wait.
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    let _ = tokio::time::timeout(left, sleep.as_mut()).await;
                    return;
                }
                // Not announced yet: wait, and come back to bound the
                // remainder by the deadline if one is announced meanwhile. A
                // closed signal ends the wait, as it ends an attempt.
                None => {
                    tokio::select! {
                        () = &mut sleep => return,
                        changed = self.rx.changed() => {
                            if changed.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// A boundary with nothing registered announces `deadline` and returns, so
    /// this is how a case says "the drain has begun" without a writer.
    async fn announce(boundary: &DrainBoundary, deadline: Duration) {
        boundary.shutdown(deadline).await;
    }

    /// The deadline has passed, so no attempt is created: not created is
    /// stronger than created-and-not-polled, and it is what the landing path
    /// needs — a created insert future is a connection checkout waiting to
    /// happen.
    #[tokio::test(start_paused = true)]
    async fn an_attempt_is_never_created_once_the_deadline_has_passed() {
        let boundary = DrainBoundary::new();
        announce(&boundary, Duration::ZERO).await;
        let mut watch = boundary.watch();

        let created = AtomicBool::new(false);
        let end: AttemptEnd<u8> = watch
            .attempt(Duration::from_secs(600), || {
                created.store(true, Ordering::SeqCst);
                async { 7u8 }
            })
            .await;

        assert_eq!(
            end,
            AttemptEnd::NotStarted,
            "an expired deadline ends the attempt before it exists"
        );
        assert!(
            !created.load(Ordering::SeqCst),
            "the constructor is never called once the deadline has passed"
        );
    }

    /// The same, one layer out: a deadline that passes while the attempt runs
    /// abandons it, and that is a different ending — the insert may have
    /// committed.
    #[tokio::test(start_paused = true)]
    async fn an_attempt_in_flight_when_the_deadline_passes_is_abandoned() {
        let boundary = Arc::new(DrainBoundary::new());
        let mut watch = boundary.watch();
        let announcer = {
            let boundary = boundary.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                announce(&boundary, Duration::ZERO).await;
            })
        };

        let end: AttemptEnd<u8> = watch
            .attempt(Duration::from_secs(600), || async {
                std::future::pending::<u8>().await
            })
            .await;
        announcer.await.expect("the announcer completes");

        assert_eq!(
            end,
            AttemptEnd::Abandoned,
            "an attempt that was polled and then met the deadline may have committed"
        );
    }

    /// With no deadline announced, the budget is the only bound and it reports
    /// its own ending.
    #[tokio::test(start_paused = true)]
    async fn the_budget_ends_an_attempt_with_no_deadline_announced() {
        let boundary = DrainBoundary::new();
        let mut watch = boundary.watch();

        let end: AttemptEnd<u8> = watch
            .attempt(Duration::from_millis(50), || async {
                std::future::pending::<u8>().await
            })
            .await;

        assert_eq!(end, AttemptEnd::BudgetElapsed);
    }

    /// An attempt inside both bounds returns its own outcome, untouched.
    #[tokio::test(start_paused = true)]
    async fn an_attempt_inside_both_bounds_returns_its_outcome() {
        let boundary = DrainBoundary::new();
        let mut watch = boundary.watch();

        let end = watch
            .attempt(Duration::from_secs(600), || async { 42u8 })
            .await;

        assert_eq!(end, AttemptEnd::Done(42u8));
    }

    /// The sleep between attempts ends at the deadline, not at its own draw.
    /// A sleep the deadline does not bound is how the drain overran in round
    /// four: it ends after the deadline and the next turn of the loop builds
    /// an attempt nobody is waiting for.
    #[tokio::test(start_paused = true)]
    async fn a_sleep_ends_at_the_announced_deadline_not_its_own_length() {
        let boundary = DrainBoundary::new();
        announce(&boundary, Duration::from_millis(10)).await;
        let mut watch = boundary.watch();

        let started = tokio::time::Instant::now();
        watch.sleep(Duration::from_secs(3600)).await;

        assert!(
            started.elapsed() <= Duration::from_millis(10),
            "the sleep ran to {:?}, past the 10ms deadline",
            started.elapsed()
        );
    }

    /// And a sleep already running when the deadline is announced ends at it.
    #[tokio::test(start_paused = true)]
    async fn a_sleep_ends_at_a_deadline_announced_while_it_runs() {
        let boundary = Arc::new(DrainBoundary::new());
        let mut watch = boundary.watch();
        let announcer = {
            let boundary = boundary.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                announce(&boundary, Duration::ZERO).await;
            })
        };

        let started = tokio::time::Instant::now();
        watch.sleep(Duration::from_secs(3600)).await;
        announcer.await.expect("the announcer completes");

        assert!(
            started.elapsed() <= Duration::from_millis(10),
            "the sleep ran to {:?} rather than ending at the announcement",
            started.elapsed()
        );
    }

    /// The admission gate: the drain does not return while an admission is
    /// inside it, and every later admission is refused. That is what keeps a
    /// block from reaching a closed queue, and a settlement from being spawned
    /// after the set of them was joined.
    #[tokio::test]
    async fn the_drain_waits_for_an_admission_already_inside_it() {
        let boundary = Arc::new(DrainBoundary::new());
        let pass = boundary.enter().expect("the gate is open");
        let returned = Arc::new(AtomicBool::new(false));
        let drain = {
            let boundary = boundary.clone();
            let returned = returned.clone();
            tokio::spawn(async move {
                boundary.shutdown(Duration::ZERO).await;
                returned.store(true, Ordering::SeqCst);
            })
        };

        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(
            !returned.load(Ordering::SeqCst),
            "the drain returned while an admission was still inside it"
        );
        assert!(
            boundary.enter().is_none(),
            "an admission arriving after the drain began is refused"
        );

        drop(pass);
        drain.await.expect("the drain completes");
        assert!(returned.load(Ordering::SeqCst));
    }

    /// A settlement spawned inside a pass is awaited by the drain. Untracked,
    /// runtime teardown can cancel it — and it holds the push's only copy.
    #[tokio::test]
    async fn the_drain_awaits_a_settlement_a_pass_spawned() {
        let boundary = DrainBoundary::new();
        let settled = Arc::new(AtomicBool::new(false));
        {
            let pass = boundary.enter().expect("the gate is open");
            let settled = settled.clone();
            boundary.spawn_settlement(&pass, async move {
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
                settled.store(true, Ordering::SeqCst);
            });
        }

        boundary.shutdown(Duration::ZERO).await;

        assert!(
            settled.load(Ordering::SeqCst),
            "the drain returned before the settlement it spawned had finished"
        );
    }
}
