//! The metrics landing path's shutdown boundary (issue #603).
//!
//! **One rule lives here: no attempt is authorized after the announced
//! shutdown deadline, and nothing the writer spawned is abandoned at it.**
//! Every part of it is this type's, and the landing path states none of it
//! again.
//!
//! Four review rounds found four ways past that rule: a retry sleep the
//! deadline did not bound, a check the poll could slip past, a settlement task
//! nobody joined, and — with each of those fixed where it was found — a read
//! that found nothing announced, then the publication, then the poll that
//! starts the attempt. The second and the fourth are one shape: **a deadline
//! read, and the work started by a later poll**, with the announcement free to
//! land in between.
//!
//! So publication and authorization are ordered against each other rather
//! than checked:
//!
//! - **[`Deadline`] is the one point that decides.** Publication takes its
//!   lock to write the deadline, and [`Deadline::authorize`] takes the same
//!   lock to read it **and calls the constructor inside that same critical
//!   section**. Every execution therefore puts one strictly before the other:
//!   either the publication went first and authorization reads the deadline it
//!   published, or authorization went first and the publication waits for the
//!   constructor to return. Nothing acts on a stale read, because no read is
//!   separated from the act it decides.
//! - **An attempt is only ever created by this module calling the caller's
//!   constructor**, so authorization is the only way work begins.
//!   [`DrainWatch::attempt`] takes that constructor rather than a future and
//!   hands it to [`Deadline::authorize`]. The landing path issues one insert
//!   and it is inside that constructor, at
//!   `crates/pulsus-write/src/writer/metric.rs:1112`: `git grep -nE
//!   'inserter$' -- crates/pulsus-write/src` returns that call's first line
//!   and nothing else. The pattern is anchored so this comment is not one of
//!   its own results — an unanchored one is, which is how the count read as
//!   two.
//! - **Every wait between attempts is [`DrainWatch::sleep`]**, which ends at
//!   the deadline whatever its own length. A wait that ended late would still
//!   start nothing: the next attempt is authorized afresh.
//! - **Admission runs inside an [`AdmissionPass`]**, and
//!   [`DrainBoundary::shutdown`] waits for every outstanding pass *before* it
//!   announces the deadline. So no block reaches a closed queue, and a
//!   settlement task — which only a pass can spawn — cannot appear after the
//!   set of them has been joined.
//!
//! **What the deadline does not bound is settling a block.** The spool copy is
//! the push's only one, so the drain finishes writing it rather than dropping
//! it at the deadline, and the drain overruns by what that write costs. That is
//! the choice the landing path makes everywhere: a block is never lost to save
//! time.
//!
//! **Two residuals, stated rather than claimed away.** Neither is a stale
//! read; both are the cost of not holding a lock across a send.
//!
//! 1. An attempt polled before the deadline and still in flight at it is
//!    abandoned, which is the uncertain ending. A send already on the wire
//!    cannot be recalled.
//! 2. The constructor returns a future, and the request goes out on that
//!    future's first poll — one store and one call after the lock is released,
//!    with no await in between. A publication on another thread can land in
//!    that gap, and then a request is issued a few instructions after the
//!    deadline. That takes a deadline already expired when it was published,
//!    which takes a grace of zero: with any positive grace the deadline is in
//!    the future at publication and the request is well inside it, and the
//!    shipped grace is ten seconds
//!    (`crates/pulsus-server/src/serve.rs:61`). The ending is then the one
//!    above, the uncertain one, since the deadline arm is ready at its first
//!    poll. Closing it would mean holding [`Deadline`]'s lock across the
//!    insert, where a shutdown would block on the network.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::warn;

/// The announced deadline, and the one point that orders its publication
/// against an attempt's authorization. `None` until the drain begins.
///
/// **One lock, two operations, and the decision is inside the same critical
/// section as the act it decides** — see [`Self::authorize`] and the module
/// docs. Nothing else may read this to decide whether work can start: a read
/// that returns a value has already stopped being the truth.
struct Deadline(Mutex<Option<Instant>>);

impl Deadline {
    fn new() -> Self {
        Deadline(Mutex::new(None))
    }

    /// The deadline as it stands, for the waits that bound themselves by it.
    /// **Not** an authorization: by the time this returns, the value may have
    /// been replaced.
    fn read(&self) -> Option<Instant> {
        *self.0.lock().expect("deadline mutex poisoned")
    }

    /// Announces `at`.
    fn publish(&self, at: Instant) {
        *self.0.lock().expect("deadline mutex poisoned") = Some(at);
    }

    /// Whether this lock is held right now, by any thread including the
    /// caller's. A case asks it from inside an attempt's constructor, where
    /// the answer says whether a publication could have landed between the
    /// read that allowed the attempt and the call that built it.
    #[cfg(test)]
    fn is_locked(&self) -> bool {
        self.0.try_lock().is_err()
    }

    /// Calls `make` if and only if no announced deadline has passed, and
    /// returns what it built; `None` is the refusal, and then `make` was never
    /// called.
    ///
    /// **The read and the call are one critical section.** A publication
    /// either precedes this call — and is what the read returns — or waits for
    /// it, in which case the work was built before the deadline existed. That
    /// ordering is what the whole boundary rests on, so `make` must build the
    /// work and do nothing else: it runs under this lock, it must not await,
    /// and it must not reach back into the boundary.
    fn authorize<F>(&self, make: impl FnOnce() -> F) -> Option<F> {
        let deadline = self.0.lock().expect("deadline mutex poisoned");
        if let Some(at) = *deadline
            && Instant::now() >= at
        {
            return None;
        }
        Some(make())
    }
}

/// The one owner of the shutdown boundary: the announced deadline, the
/// admission gate and every task the landing path spawns. See the module
/// docs.
pub(crate) struct DrainBoundary {
    /// Wakes every task that is waiting on the announcement, and carries no
    /// value — [`Self::deadline`] holds it. A `watch` channel, so a task
    /// blocked inside an insert observes the announcement the next time it
    /// reaches a selection with no lost-wakeup window, and so a task learns
    /// the writer went away without a graceful shutdown when this sender is
    /// dropped with the boundary.
    signal: watch::Sender<()>,
    /// The announced deadline, shared with every [`DrainWatch`].
    deadline: Arc<Deadline>,
    /// Admissions inside a pass right now.
    passes: AtomicU64,
    /// Set before the wait for `passes` to reach zero, so an admission
    /// arriving after that wait began is refused rather than waited for.
    closed: AtomicBool,
    /// The insert workers and the suppression ticker.
    background: Mutex<Vec<JoinHandle<()>>>,
    /// Settlements spawned from inside a pass.
    settlements: Mutex<Vec<JoinHandle<()>>>,
}

/// Proof that an admission is inside the boundary: while one of these exists,
/// [`DrainBoundary::shutdown`] has not announced the deadline and has not
/// joined the settlement set — it waits for every pass before either. The one
/// way to hold a pass outside that guarantee is
/// `MetricWriter::reopen_admission_for_test`, which re-opens the gate after a
/// drain has finished on purpose.
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
        let (signal, _rx) = watch::channel(());
        DrainBoundary {
            signal,
            deadline: Arc::new(Deadline::new()),
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
            deadline: self.deadline.clone(),
            #[cfg(test)]
            barrier: None,
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
        self.publish(Instant::now() + deadline);
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

    /// Announces the deadline `at` and nothing else. Its own step because a
    /// case has to publish inside the window an attempt's authorization
    /// straddles, where the drain's other steps have no part.
    ///
    /// The deadline first and the wake second: a task that has already read
    /// the deadline and parked is woken by the second, and a task that has not
    /// yet read it sees the first.
    fn publish(&self, at: Instant) {
        self.deadline.publish(at);
        // `send_replace`, not `send`: `send` reports every receiver being gone
        // by leaving the version alone, and nothing here is conditional on a
        // task currently watching.
        self.signal.send_replace(());
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
    /// Wakes this view when the deadline is published. It carries no value:
    /// `deadline` is read for that.
    rx: watch::Receiver<()>,
    /// The boundary's own [`Deadline`], which is where an attempt is
    /// authorized.
    deadline: Arc<Deadline>,
    /// Run once inside [`Self::attempt`], after the read of the deadline that
    /// found none announced and before the poll that would start the attempt.
    /// A case publishes a deadline in exactly that window through this, rather
    /// than racing a spawned task for a handful of instructions.
    ///
    /// Test-only: the field does not exist in a build of this crate that is
    /// not its own test binary.
    #[cfg(test)]
    barrier: Option<Box<dyn FnOnce() + Send>>,
}

impl DrainWatch {
    /// This view with `barrier` attached. See the field.
    #[cfg(test)]
    fn with_barrier(mut self, barrier: impl FnOnce() + Send + 'static) -> Self {
        self.barrier = Some(Box::new(barrier));
        self
    }

    /// The announced deadline, if the drain has begun — for bounding a wait,
    /// never for deciding that work may start (see [`Deadline::authorize`]).
    ///
    /// The wake is marked seen **before** the deadline is read, so a
    /// publication that lands between the two is either what this read returns
    /// or what the next `changed()` reports. Marking it after could mark a
    /// version this never observed, and park on a `None` that had already been
    /// replaced.
    fn announced(&mut self) -> Option<Instant> {
        self.rx.mark_unchanged();
        self.deadline.read()
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
    /// `make` is not called here and not called by this function's own reading
    /// of the deadline. It goes to [`Deadline::authorize`], which is the one
    /// place an attempt can begin and which reads the deadline in the same
    /// critical section as the call — so the selection below decides how an
    /// attempt *ends*, and never whether it may start. See the module docs.
    pub(crate) async fn attempt<F, T>(
        &mut self,
        budget: Duration,
        make: impl FnOnce() -> F,
    ) -> AttemptEnd<T>
    where
        F: Future<Output = T>,
    {
        // `make` reaches `authorize` on the first poll of this block and never
        // otherwise, so "never authorized", "never created" and "never polled"
        // are one thing here. `None` out is the refusal.
        let started = AtomicBool::new(false);
        let deadline = self.deadline.clone();
        let work = async {
            let work = deadline.authorize(make)?;
            started.store(true, Ordering::Relaxed);
            Some(work.await)
        };
        let bounded = tokio::time::timeout(budget, work);
        tokio::pin!(bounded);
        loop {
            match self.announced() {
                // `at`, not `deadline`: the shared `Deadline` is what
                // authorizes an attempt, and this is one reading of the
                // instant it holds.
                Some(at) => {
                    // `std::time::Instant`: the deadline is the shutdown
                    // caller's own clock reading, and a deadline already passed
                    // leaves a zero-length sleep — ready at its first poll.
                    let left = at.saturating_duration_since(Instant::now());
                    let expire = tokio::time::sleep(left);
                    tokio::pin!(expire);
                    return tokio::select! {
                        biased;
                        // The deadline arm first, so a passed deadline ends
                        // the attempt at once rather than after however long
                        // one poll of the work takes. It is not what keeps the
                        // work from being created — `authorize` is.
                        () = &mut expire => if started.load(Ordering::Relaxed) {
                            AttemptEnd::Abandoned
                        } else {
                            AttemptEnd::NotStarted
                        },
                        outcome = &mut bounded => match outcome {
                            Ok(Some(value)) => AttemptEnd::Done(value),
                            // Refused: the deadline had passed when the work
                            // would have been built.
                            Ok(None) => AttemptEnd::NotStarted,
                            Err(_elapsed) => AttemptEnd::BudgetElapsed,
                        },
                    };
                }
                None => {
                    #[cfg(test)]
                    if let Some(barrier) = self.barrier.take() {
                        barrier();
                    }
                    // Nothing read here decides anything, so the attempt is
                    // polled first: that way a closed signal — the writer
                    // dropped without a graceful shutdown — abandons an
                    // attempt in flight rather than refusing to start one. A
                    // deadline published between this read and that poll is
                    // what `authorize` sees.
                    tokio::select! {
                        biased;
                        outcome = &mut bounded => return match outcome {
                            Ok(Some(value)) => AttemptEnd::Done(value),
                            Ok(None) => AttemptEnd::NotStarted,
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
                Some(at) => {
                    let left = at.saturating_duration_since(Instant::now());
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

    /// **The window between reading the deadline and starting the attempt.**
    /// A view that read `None` is about to poll the attempt first, by the bias
    /// the arm below it is given on purpose; the deadline is published in
    /// between. The constructor is still never called, because the read that
    /// decides is the one inside the authorization and not this one (issue
    /// #603 code review round 6, finding 1).
    ///
    /// The publication is the barrier itself rather than a spawned task
    /// racing for a handful of instructions: `DrainWatch::barrier` runs after
    /// that `None` and before the poll, so the interleaving is the same every
    /// run.
    #[tokio::test(start_paused = true)]
    async fn an_attempt_is_never_created_by_a_read_the_deadline_overtook() {
        let boundary = Arc::new(DrainBoundary::new());
        let published = {
            let boundary = boundary.clone();
            move || boundary.publish(Instant::now())
        };
        let mut watch = boundary.watch().with_barrier(published);

        let created = AtomicBool::new(false);
        let end: AttemptEnd<u8> = watch
            .attempt(Duration::from_secs(600), || {
                created.store(true, Ordering::SeqCst);
                async { 7u8 }
            })
            .await;

        assert!(
            !created.load(Ordering::SeqCst),
            "the constructor was called after a deadline published while the \
             attempt was between its read and its first poll"
        );
        assert_eq!(
            end,
            AttemptEnd::NotStarted,
            "a deadline that landed in that window ends the attempt before it \
             exists, as an expired one read up front does"
        );
    }

    /// **The read that allows an attempt and the call that builds it are one
    /// critical section.** The case above pins where the deciding read happens;
    /// this one pins that publication cannot get between that read and the
    /// constructor, which is the whole of the ordering and is not something a
    /// cross-thread race could show deterministically — the gap is a couple of
    /// instructions wide.
    ///
    /// It is asked from inside the constructor, of the lock publication takes:
    /// held there means a publication is either already done, and was what the
    /// read returned, or is waiting for this call to come back.
    #[tokio::test(start_paused = true)]
    async fn the_constructor_runs_inside_the_lock_a_publication_takes() {
        let boundary = DrainBoundary::new();
        let mut watch = boundary.watch();

        let locked = AtomicBool::new(false);
        let end = watch
            .attempt(Duration::from_secs(600), || {
                locked.store(boundary.deadline.is_locked(), Ordering::SeqCst);
                async { 7u8 }
            })
            .await;

        assert!(
            locked.load(Ordering::SeqCst),
            "the deadline's lock was free while the attempt was being built, so \
             a publication could have landed between the read that allowed it \
             and this call"
        );
        assert_eq!(end, AttemptEnd::Done(7u8));
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
