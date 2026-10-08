//! The metrics landing path's shutdown boundary (issue #603).
//!
//! **One rule lives here: no attempt is authorized after the announced
//! shutdown deadline, and nothing the writer spawned is abandoned at it.**
//! Every part of it is this type's, and the landing path states none of it
//! again.
//!
//! Five review rounds found five ways past that rule: a retry sleep the
//! deadline did not bound, a check the poll could slip past, a settlement task
//! nobody joined, a read that found nothing announced followed by the
//! publication and then the poll that starts the attempt, and — with each of
//! those fixed where it was found — the attempt **already started**, polled
//! once more after the deadline and sending then. All but the third are one
//! shape: **a deadline read, and the work carried by a later poll**, with the
//! announcement free to land in between.
//!
//! So publication and authorization are ordered against each other rather
//! than checked, and the deadline bounds the polls of an attempt as well as
//! its creation — short of the two residuals at the end of this comment:
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
//!   `crates/pulsus-write/src/writer/metric.rs:1207`: `git grep -nE
//!   'inserter$' -- crates/pulsus-write/src` returns that call's first line
//!   and nothing else. The pattern is anchored so this comment is not one of
//!   its own results — an unanchored one is, which is how the count read as
//!   two.
//! - **Creating an attempt and carrying it on are two different acts**, and
//!   the deadline bounds both. The request leaves on whichever poll of the
//!   attempt gets past the connection checkout, which is rarely its first, so
//!   [`DrainWatch::attempt`] polls it only while no deadline is announced, and
//!   prefers the announcement to it in every selection that can park. The
//!   deadline arm compares the clock against the announced instant on each
//!   poll rather than trusting a timer to have fired — a `sleep` of a duration
//!   already spent is not ready at its first poll.
//! - **Every wait between attempts is [`DrainWatch::sleep`]**, which ends at
//!   the deadline whatever its own length. A wait that ended late would still
//!   start nothing: the next attempt is authorized afresh.
//! - **Admission runs inside an [`AdmissionPass`]**, and
//!   [`DrainBoundary::shutdown`] waits for every outstanding pass *before* it
//!   announces the deadline. So no block reaches a closed queue, and a
//!   settlement task — which only a pass can spawn — cannot appear after the
//!   set of them has been joined.
//! - **However many callers ask for it, the drain is one operation with one
//!   deadline**, and each of them returns only once it has finished. See
//!   [`DrainBoundary::shutdown`].
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
//!    poll whenever the instant has passed. Closing it would mean holding
//!    [`Deadline`]'s lock across the insert's own polls, where a shutdown
//!    would wait on the network and the insert workers would stop
//!    overlapping.

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
    /// Held for the whole of [`Self::shutdown`], so concurrent callers are
    /// one drain rather than several. Without it each caller empties the
    /// handle vectors the others were about to take, and whichever loses the
    /// race returns having awaited nothing — with the writer's tasks still
    /// running and a settlement half written.
    drain: tokio::sync::Mutex<()>,
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
            drain: tokio::sync::Mutex::new(()),
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
    ///
    /// **Concurrent callers are one drain, and every one of them waits for
    /// it.** The body runs under `drain`, so a second caller joins nothing
    /// until the first has finished joining everything; and the deadline is
    /// **the first one announced**, because a later publication would move an
    /// instant workers have already read and bounded their waits by. A caller
    /// that asked for a different grace still returns only once the writer is
    /// drained, which is what `shutdown` means.
    pub(crate) async fn shutdown(&self, deadline: Duration) {
        let _drain = self.drain.lock().await;
        self.close_admission().await;
        if self.deadline.read().is_none() {
            self.publish(Instant::now() + deadline);
        }
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

/// How an attempt the budget bounded ended, from what the bounded future
/// answered. One place, because all three of [`DrainWatch::attempt`]'s
/// selections have this arm and a second reading of `Ok(None)` would be
/// another chance to call a refusal something else.
fn end_of<T>(outcome: Result<Option<T>, tokio::time::error::Elapsed>) -> AttemptEnd<T> {
    match outcome {
        Ok(Some(value)) => AttemptEnd::Done(value),
        // Refused: the deadline had passed when the work would have been
        // built.
        Ok(None) => AttemptEnd::NotStarted,
        Err(_elapsed) => AttemptEnd::BudgetElapsed,
    }
}

/// How an attempt stopped short of its own outcome — at the deadline, or with
/// the signal gone. Whether anything may have been sent is the whole of the
/// difference, and `started` is what records it.
fn stopped_at<T>(started: &AtomicBool) -> AttemptEnd<T> {
    if started.load(Ordering::Relaxed) {
        AttemptEnd::Abandoned
    } else {
        AttemptEnd::NotStarted
    }
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
                    // **The deadline arm reads the clock; it does not trust a
                    // timer to have fired.** `tokio::time::sleep` for a
                    // duration already spent is *not* ready at its first poll —
                    // it yields to the scheduler once — and a timer whose
                    // instant passed while this task was not scheduled has not
                    // necessarily been marked elapsed either, because the
                    // driver runs on the same thread the task does. Either way
                    // a selection that trusted the sleep alone would fall
                    // through to the arm below and poll the attempt, and that
                    // is the poll that issues the request (issue #603 code
                    // review round 7, finding 2). So this arm compares `at`
                    // against the clock on every poll and keeps the sleep only
                    // for the wake.
                    //
                    // `std::time::Instant`: the deadline is the shutdown
                    // caller's own clock reading.
                    let mut timer = Box::pin(tokio::time::sleep(
                        at.saturating_duration_since(Instant::now()),
                    ));
                    let expire = std::future::poll_fn(move |cx| {
                        if Instant::now() >= at {
                            return std::task::Poll::Ready(());
                        }
                        timer.as_mut().poll(cx)
                    });
                    tokio::pin!(expire);
                    return tokio::select! {
                        biased;
                        // The deadline arm first, so a passed deadline ends
                        // the attempt without one further poll of the work. It
                        // is not what keeps the work from being created —
                        // `authorize` is.
                        //
                        // What that costs: an attempt that had finished but
                        // was not polled before the deadline is reported
                        // abandoned rather than by its own outcome. That is an
                        // over-report of uncertainty, which is the direction
                        // this path errs in everywhere, and it needs the same
                        // precondition as the send it prevents — a task left
                        // unscheduled across the whole grace.
                        () = &mut expire => stopped_at(&started),
                        outcome = &mut bounded => end_of(outcome),
                    };
                }
                None => {
                    #[cfg(test)]
                    if let Some(barrier) = self.barrier.take() {
                        barrier();
                    }
                    // **The attempt is polled, and then the signal is
                    // preferred while nothing else can happen. This is the
                    // second half of the boundary's rule**: `authorize` keeps
                    // an attempt from being *created* after the deadline, and
                    // this keeps one already created from *sending* after it —
                    // a different act, one or more polls further on, because
                    // the request goes out of whichever poll of the attempt
                    // gets past the connection checkout
                    // (`crates/pulsus-clickhouse/src/client.rs:249`).
                    //
                    // One poll of the attempt, never waited on: that is what
                    // creates it, and it is what makes a closed signal — the
                    // writer dropped without a graceful shutdown — abandon an
                    // attempt in flight rather than refuse to start one. The
                    // `ready` arm cannot be reached while the first is, so
                    // this parks on nothing.
                    tokio::select! {
                        biased;
                        outcome = &mut bounded => return end_of(outcome),
                        () = std::future::ready(()) => {}
                    }
                    // Then the wait, with the signal first. A publication that
                    // lands while this is parked is what the next poll of it
                    // takes, whenever that comes, and the top of this loop
                    // reads the deadline and bounds everything after it by
                    // that. Were the attempt polled first here, a task not
                    // scheduled for the whole grace would wake with both arms
                    // ready and issue its request past the deadline (issue
                    // #603 code review round 7, finding 2).
                    tokio::select! {
                        biased;
                        changed = self.rx.changed() => {
                            if changed.is_err() {
                                return stopped_at(&started);
                            }
                        }
                        outcome = &mut bounded => return end_of(outcome),
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

    /// A stand-in for the production insert, whose first poll is the
    /// connection checkout and whose request goes out on a **later** one
    /// (`crates/pulsus-clickhouse/src/client.rs:249`). It parks on its first
    /// poll without arranging any wake of its own, so the only thing that can
    /// poll it again is the selection holding it — which is the interleaving
    /// the case below is about.
    struct SendsOnceCheckedOut {
        /// Set by the case: the checkout has come back, so the next poll of
        /// this is the one that issues the request.
        checked_out: Arc<AtomicBool>,
        /// How many times this has been polled, so the case can tell it has
        /// been created and parked.
        polls: Arc<AtomicU64>,
        /// Set by the poll that issues the request.
        sent: Arc<AtomicBool>,
    }

    impl Future for SendsOnceCheckedOut {
        type Output = u8;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<u8> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            if !self.checked_out.load(Ordering::SeqCst) {
                return std::task::Poll::Pending;
            }
            self.sent.store(true, Ordering::SeqCst);
            std::task::Poll::Ready(7)
        }
    }

    /// **A request the attempt produces on a later poll must not leave after
    /// the deadline** (issue #603 code review round 7, finding 2).
    /// Authorization orders publication against the attempt's *construction*,
    /// and the request follows on a poll that can come much later; while the
    /// attempt is parked with no deadline read, the arm holding it is polled
    /// first, so a task that is not scheduled for the whole grace can be
    /// woken past the deadline and send then.
    ///
    /// Deterministic, and it needs no starved scheduler to arrange: the case
    /// runs on a current-thread runtime and blocks that one thread for longer
    /// than the grace, which is what "not scheduled for the whole grace"
    /// means here. The publication has already woken the attempt's task by
    /// then, so both of the selection's arms are ready when it finally runs.
    #[test]
    fn a_request_produced_by_a_later_poll_never_leaves_after_the_deadline() {
        /// Long enough that the blocking sleep below cannot fall short of it,
        /// short enough that the case is not a wait.
        const GRACE: Duration = Duration::from_millis(20);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("build a current-thread runtime");
        runtime.block_on(async {
            let boundary = Arc::new(DrainBoundary::new());
            let checked_out = Arc::new(AtomicBool::new(false));
            let polls = Arc::new(AtomicU64::new(0));
            let sent = Arc::new(AtomicBool::new(false));
            let attempt = {
                let mut watch = boundary.watch();
                let checked_out = checked_out.clone();
                let polls = polls.clone();
                let sent = sent.clone();
                tokio::spawn(async move {
                    watch
                        .attempt(Duration::from_secs(600), || SendsOnceCheckedOut {
                            checked_out,
                            polls,
                            sent,
                        })
                        .await
                })
            };

            // The attempt is authorized, built and parked on its checkout,
            // with no deadline announced.
            for _ in 0..1024 {
                if polls.load(Ordering::SeqCst) > 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(
                polls.load(Ordering::SeqCst) > 0,
                "the attempt must have been created and polled before the drain \
                 begins, or the case proves nothing about a later poll"
            );
            assert!(!sent.load(Ordering::SeqCst), "nothing has been sent yet");

            // The drain begins with a positive grace, the checkout comes back
            // while the deadline is still ahead, and the thread the attempt's
            // task runs on is held past that deadline. No await between the
            // three, so that task cannot run until the hold ends — which is
            // what "not scheduled for the whole grace" is. Its wake is already
            // queued: the publication sent it.
            boundary.publish(Instant::now() + GRACE);
            checked_out.store(true, Ordering::SeqCst);
            std::thread::sleep(GRACE + Duration::from_millis(30));

            let end: AttemptEnd<u8> = attempt.await.expect("the attempt's task completes");

            assert!(
                !sent.load(Ordering::SeqCst),
                "the attempt was polled again after the deadline had passed and \
                 issued its request then (polls {}, end {:?})",
                polls.load(Ordering::SeqCst),
                end
            );
            assert_eq!(
                end,
                AttemptEnd::Abandoned,
                "an attempt already started and not finished by the deadline ends \
                 abandoned, with nothing sent after it"
            );
        });
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

    /// **Two callers are one drain.** `shutdown` is documented idempotent, and
    /// a second caller that empties the handle vectors the first is about to
    /// take returns while the writer's tasks are still running — the process
    /// then exits with a settlement half written (issue #603 code review round
    /// 7, finding 4).
    ///
    /// The interleaving is deterministic rather than raced: `join!` polls the
    /// first caller until it parks on the task it is awaiting, and only then
    /// polls the second.
    #[tokio::test]
    async fn two_concurrent_drains_both_return_only_once_every_task_has_finished() {
        let boundary = Arc::new(DrainBoundary::new());
        let finished = Arc::new(AtomicBool::new(false));
        boundary.track_background([{
            let finished = finished.clone();
            tokio::spawn(async move {
                for _ in 0..16 {
                    tokio::task::yield_now().await;
                }
                finished.store(true, Ordering::SeqCst);
            })
        }]);

        let first = async {
            boundary.shutdown(Duration::from_secs(60)).await;
            assert!(
                finished.load(Ordering::SeqCst),
                "the first caller returned before the task it took had finished"
            );
        };
        let second = async {
            boundary.shutdown(Duration::ZERO).await;
            assert!(
                finished.load(Ordering::SeqCst),
                "the second caller returned while a task the first one took was \
                 still running: one of the two drains awaited nothing"
            );
        };
        tokio::join!(first, second);
    }

    /// And the deadline the first caller announced is the one that stands. A
    /// second publication moves an instant workers have already read and
    /// bounded their waits by, so a worker that read a generous deadline can
    /// find itself past an expired one it never saw announced.
    #[tokio::test]
    async fn a_second_drain_never_moves_the_deadline_the_first_announced() {
        let boundary = Arc::new(DrainBoundary::new());
        boundary.track_background([tokio::spawn(async {
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
        })]);
        let before = Instant::now();

        tokio::join!(
            boundary.shutdown(Duration::from_secs(60)),
            boundary.shutdown(Duration::ZERO),
        );

        let at = boundary.deadline.read().expect("the drain announced one");
        assert!(
            at > before + Duration::from_secs(30),
            "the deadline in force is {:?} after the first caller's reading, so \
             the second caller's replaced it",
            at.saturating_duration_since(before)
        );
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
