//! The landing path both signals share (issue #603): the sealed block, the
//! insert loop and its two fates, the audit copy, and the one place a block's
//! queue reservation is released.
//!
//! **Generic over the row type, not copied per signal.** A copy gives two
//! `release`s and two censuses, each lexical over its own file — a claim about
//! a set checked against a subset. One `release` and one census is the
//! guarantee, and a third signal inherits it.
//!
//! What stays per signal, in `writer::metric` and `writer::log`: the writer
//! itself, admission, the registration gate, and a commit hook
//! ([`LandingContext::on_commit`]).
//!
//! The pattern, the fates, the budget and the shutdown boundary are
//! `docs/ingest-one-source-table.md` §2 to §6 and are not restated here.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pulsus_clickhouse::{ChError, ChRow, QuerySettings};
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::writer::config::WriterRuntime;
use crate::writer::drain::{AttemptEnd, DrainWatch};
use crate::writer::error::WriteError;
use crate::writer::metrics::TableMetrics;
use crate::writer::push_dedup::{self, ClaimTicket, TargetOutcome};
use crate::writer::spool::{SpoolEncode, SpoolKind, SpoolWriter};
use crate::writer::table::{BlockInserter, XorShift64};

/// The message a block still queued when the budget ran out settles with.
const MSG_BUDGET_QUEUED: &str = "the landing budget was spent before the block was sent";
/// The message a block whose budget ran out between attempts settles with.
const MSG_BUDGET_BETWEEN: &str = "the landing budget was spent between attempts";
/// The message an attempt interrupted by the budget settles with.
const MSG_BUDGET_IN_ATTEMPT: &str = "the landing budget elapsed during an attempt";
/// The message a block abandoned mid-attempt at the shutdown deadline
/// settles with.
const MSG_SHUTDOWN_INFLIGHT: &str = "the writer shut down with an attempt in flight";
/// The message a block still queued at shutdown settles with.
pub(crate) const MSG_SHUTDOWN_QUEUED: &str = "the writer shut down before the block was sent";

/// What the queue holds for one block **besides its rows**, charged once per
/// block so that `PULSUS_INGEST_QUEUE_BYTES` bounds everything a queued push
/// retains rather than its rows alone.
///
/// Everything a [`LandingBlock`] holds, field by field, at the most any one of
/// them holds. `rows` is priced per row by the signal's own
/// `est_landing_bytes`; `bytes` and `admitted_at` are inline in the struct;
/// every other field is a term below.
///
/// | what it holds | bytes |
/// |---|---|
/// | the block itself, and the queue slot it is moved into | `2 * size_of::<LandingBlock<R>>()` |
/// | `settings`: ten owned key/value pairs | their vector, sixteen slots — its first allocation is four, the fifth pair doubles it and the ninth doubles it again — plus 408 for their text **by capacity**: 18 + 6, 50 + 10, 26 + 36, 21 + 8, 27 + 10, 33 + 10, 26 + 8, 27 + 10, 32 + 10, 30 + 10, at a 36-byte token and the largest accepted row ceiling. A value rendered from an integer literal is allocated ten bytes whatever its digits; one rendered from the `u64` ceiling is allocated its digits, which is why the largest ceiling is the term (406 at the default, 400 at the floor) |
/// | `claim`: the `Vec<PushDigest>` its one key allocates, four slots at its first push | `4 * size_of::<PushDigest>()` |
/// | `waiter`: one `oneshot` channel in sync mode — a state word, two waker slots and one `Result<(), WriteError>` | 256. Those four come to 8 + 2 × 16 + 32 = 72; the rest is allowance, because the channel's own bookkeeping is private to it |
///
/// **So a block may hold nothing whose size grows with the push except its
/// rows.** A field that did would need its own per-push term and this one
/// would stop bounding it: that is why the keys a commit promotes are derived
/// from the committed rows rather than carried beside them, and why
/// [`LandingContext::on_commit`] is a fixed-size field on the context rather
/// than on the block.
///
/// **The figure does not depend on `R`.** `LandingBlock<R>`'s only
/// `R`-dependent field is a `Vec<R>`, whose header is three words whatever `R`
/// is, and the settings are the same ten names at the same token width and the
/// same largest accepted row ceiling for both signals — the two ceilings'
/// accepted ranges are identical, which is what lets one derivation serve
/// both.
pub const fn landing_block_overhead_bytes<R>() -> u64 {
    2 * std::mem::size_of::<LandingBlock<R>>() as u64
        + 16 * std::mem::size_of::<(String, String)>() as u64
        + 408
        + 4 * std::mem::size_of::<push_dedup::PushDigest>() as u64
        + 256
}

/// What the queue is charged for a push whose landing rows price `row_bytes`.
/// **One figure prices a push**: the per-push byte ceiling refuses against it,
/// `reserve_queued_bytes` takes it, whichever ending settles releases it and
/// `record_flush` reports it, so no two halves of the accounting can drift
/// apart.
pub const fn landing_charge<R>(row_bytes: u64) -> u64 {
    row_bytes + landing_block_overhead_bytes::<R>()
}

/// One admitted push, sealed and queued. A worker runs it to exactly one
/// ending and is the only place it settles.
///
/// **What one of these costs the queue, and what that requires of the fields
/// below, is [`landing_block_overhead_bytes`].**
pub(crate) struct LandingBlock<R> {
    /// The push's landing rows, every kind in one vector. Nothing depends on
    /// their order inside the block: the table's sorting key orders what is
    /// stored, and every consumer matches rows by `kind`.
    pub(crate) rows: Vec<R>,
    /// Exactly what `reserve_queued_bytes` took for this block
    /// ([`landing_charge`]). Released once, and in one place —
    /// [`LandingBlock::release`], which owns when.
    pub(crate) bytes: u64,
    /// [`QuerySettings::landing_insert`], built once here and sent
    /// byte-identical on every resend.
    pub(crate) settings: QuerySettings,
    /// The push's issue-#494 obligation, or an inert ticket whenever
    /// `PULSUS_INGEST_DEDUP` is off.
    claim: ClaimTicket,
    /// The sync caller's waiter; `None` in async mode.
    waiter: Option<oneshot::Sender<Result<(), WriteError>>>,
    /// Taken with the claim. The landing budget runs from here, so a block's
    /// queue wait is spent out of its own budget.
    ///
    /// `tokio::time::Instant`, not `std::time::Instant`: the budget is
    /// compared against the same clock the attempt's own timeout and the
    /// retry sleeps use, so a test that drives the loop on a paused clock
    /// measures one clock rather than two.
    admitted_at: tokio::time::Instant,
}

impl<R> LandingBlock<R> {
    /// The one place a `LandingBlock` is built, so the case that prices what
    /// one holds prices the same value the queue does.
    pub(crate) fn seal(
        rows: Vec<R>,
        bytes: u64,
        settings: QuerySettings,
        claim: ClaimTicket,
        waiter: Option<oneshot::Sender<Result<(), WriteError>>>,
        admitted_at: tokio::time::Instant,
    ) -> Self {
        LandingBlock {
            rows,
            bytes,
            settings,
            claim,
            waiter,
            admitted_at,
        }
    }

    /// **The one place a landing block's reservation is released, and the order
    /// it is released in.**
    ///
    /// The charge is the queue's allowance for what this block holds, so it is
    /// given back only once the block is gone: this consumes it, drops the rows
    /// and the settings — everything the charge prices that grows with the push
    /// — reports the claim, which is where the ticket's own charged vector goes,
    /// and subtracts last. An ending that subtracted for itself would hand the
    /// allowance to a new admission with its own rows still in memory, and with
    /// the rows of every worker waiting on the registration mutex behind it, so
    /// `PULSUS_INGEST_QUEUE_BYTES` would permit more than it names.
    ///
    /// The waiter is returned rather than answered here: the caller resolves it
    /// last, so a sync caller reading its answer has by then seen the bytes
    /// released and the claim reported. What crosses the release is that one
    /// `oneshot` channel — a fixed term of [`landing_block_overhead_bytes`],
    /// jointly owned with the caller, and therefore outside any ordering this
    /// module could choose.
    fn release(
        self,
        ctx: &LandingContext<R>,
        outcome: TargetOutcome,
    ) -> Option<oneshot::Sender<Result<(), WriteError>>> {
        let LandingBlock {
            rows,
            bytes,
            settings,
            claim,
            waiter,
            admitted_at: _,
        } = self;
        drop(rows);
        drop(settings);
        claim.settle(outcome);
        ctx.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
        waiter
    }
}

/// A commit-time callback over the committed block's own rows: the
/// registration cache promotion, which is the one thing a signal does at its
/// commit that this module cannot.
///
/// A named alias rather than an inline `Option<Arc<dyn Fn(..)>>`, for Clippy's
/// `type_complexity`.
pub(crate) type CommitHook<R> = Arc<dyn Fn(&[R]) + Send + Sync>;

/// What a landing worker needs to run a block to an ending.
pub(crate) struct LandingContext<R> {
    pub(crate) table: Arc<str>,
    pub(crate) inserter: Arc<dyn BlockInserter<R>>,
    pub(crate) runtime: Arc<WriterRuntime>,
    /// The landing table's own per-table counters. One insert per push, so
    /// `flushes_total` counts pushes stored and `rows_total` counts landed
    /// events of every kind.
    pub(crate) table_metrics: Arc<TableMetrics>,
    pub(crate) queued_bytes: Arc<AtomicU64>,
    pub(crate) spool: Arc<SpoolWriter>,
    /// Called by [`commit_block`] with the committed block's rows, **while the
    /// block is still charged**. A fixed-size field on the context rather than
    /// on the block, so what a queued block holds does not grow with the push.
    pub(crate) on_commit: Option<CommitHook<R>>,
    /// `PULSUS_<SIGNAL>_LANDING_RETRIES`, resolved by the signal's writer so
    /// this loop names no signal's config key.
    pub(crate) retries: u32,
    /// `PULSUS_<SIGNAL>_LANDING_MAX_ROWS`, likewise — the value both pinned
    /// row limits carry on every insert sent against this table, read from
    /// here when the block is sealed so no config key is named twice.
    pub(crate) max_rows: u64,
}

/// What the loop knows about a block short of a commit — **a value the loop
/// carries, not a decision each ending makes**. Two methods write it and
/// there is no third, and `Uncertain` is terminal short of a commit: an
/// ending whose own knowledge is "this did not send" can never walk the fate
/// back from an earlier attempt that may have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LandingFate {
    NeverSent(String),
    Uncertain(String),
}

/// Why a block reached [`settle_block`]. It decides the waiter's error and
/// nothing else: the spool directory and the claim's outcome come from the
/// fate. A shutdown is not a fate — a block abandoned at the shutdown
/// deadline may have committed, exactly as one abandoned at the budget may —
/// so it travels beside one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TerminalCause {
    Normal,
    ShuttingDown,
}

impl LandingFate {
    /// What an ending whose own knowledge is "this did not send" calls. It
    /// never reverses an earlier `Uncertain`, and the argument is dropped in
    /// that case.
    pub(crate) fn saw_pre_send(&mut self, msg: String) {
        if let LandingFate::NeverSent(m) = self {
            *m = msg;
        }
    }

    /// What an ending that may have sent calls.
    pub(crate) fn saw_uncertain(&mut self, msg: String) {
        *self = LandingFate::Uncertain(msg);
    }

    /// The spool directory, the claim's outcome and the message. The only
    /// place this path names a `SpoolKind` or a non-`Committed`
    /// `TargetOutcome`, so no ending picks its own.
    pub(crate) fn settle(self) -> (SpoolKind, TargetOutcome, String) {
        match self {
            LandingFate::NeverSent(m) => (SpoolKind::Poison, TargetOutcome::NotCommitted, m),
            LandingFate::Uncertain(m) => (SpoolKind::Uncertain, TargetOutcome::Uncertain, m),
        }
    }
}

/// A fresh time-ordered UUID (version 7) as the canonical hyphenated
/// lowercase string: 48 bits of Unix milliseconds, the version nibble `7`,
/// 12 bits of randomness, the two variant bits `10`, and 62 more bits of
/// randomness.
///
/// Written here rather than taken from a dependency: this crate needs one
/// string per admitted push and nothing else a UUID library offers, and the
/// layout is 128 bits of two fields it already has to hand.
///
/// It is the batch's identity for retry purposes only — it is not a landed
/// event's identity, and it is never derived from the block's content.
pub(crate) fn mint_landing_token(rng: &mut XorShift64) -> String {
    let unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
        & 0xFFFF_FFFF_FFFF;
    let rand_a = (rng.next_u64() & 0x0FFF) as u16;
    let rand_b = rng.next_u64() & 0x3FFF_FFFF_FFFF_FFFF;
    format!(
        "{:08x}-{:04x}-7{:03x}-{:04x}-{:012x}",
        ((unix_ms >> 16) & 0xFFFF_FFFF) as u32,
        (unix_ms & 0xFFFF) as u16,
        rand_a,
        0x8000u16 | ((rand_b >> 48) as u16 & 0x3FFF),
        rand_b & 0xFFFF_FFFF_FFFF,
    )
}

/// Spawns one insert worker on the landing queue. Every worker runs the same
/// loop: take the next block, run it to an ending, repeat; once the shutdown
/// signal fires, close the queue and settle what is left.
pub(crate) fn spawn_landing_worker<R>(
    ctx: Arc<LandingContext<R>>,
    rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<LandingBlock<R>>>>,
    mut watch: DrainWatch,
) -> tokio::task::JoinHandle<()>
where
    R: ChRow + SpoolEncode + Send + Sync + 'static,
{
    tokio::spawn(async move {
        // Seeded from the clock, unless a case fixed the seed so its retry
        // delays are the same every run (`WriterRuntime::retry_jitter_seed`).
        let mut rng = match ctx.runtime.retry_jitter_seed {
            Some(seed) => XorShift64::from_seed(seed),
            None => XorShift64::seeded(),
        };
        loop {
            if watch.is_announced() {
                drain_queue(&ctx, &rx).await;
                return;
            }
            let taken = {
                let mut queue = rx.lock().await;
                tokio::select! {
                    block = queue.recv() => Taken::Block(block),
                    () = watch.until_announced() => Taken::ShuttingDown,
                }
            };
            match taken {
                Taken::Block(Some(block)) => {
                    run_block(&ctx, block, &mut watch, &mut rng).await;
                }
                // Every sender is gone, so nothing more can arrive.
                Taken::Block(None) => return,
                Taken::ShuttingDown => {
                    drain_queue(&ctx, &rx).await;
                    return;
                }
            }
        }
    })
}

enum Taken<R> {
    Block(Option<LandingBlock<R>>),
    ShuttingDown,
}

/// Closes the queue, then settles every block still in it through the
/// queued-shutdown ending — no attempt ever ran for one of them, so each
/// gets a fresh fate and is filed as provably not committed. Closing before
/// draining is what makes the drain terminate.
async fn drain_queue<R>(
    ctx: &Arc<LandingContext<R>>,
    rx: &Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<LandingBlock<R>>>>,
) where
    R: ChRow + SpoolEncode + Send + Sync,
{
    let mut queue = rx.lock().await;
    queue.close();
    while let Some(block) = queue.recv().await {
        let mut fate = LandingFate::NeverSent(String::new());
        fate.saw_pre_send(MSG_SHUTDOWN_QUEUED.to_string());
        settle_block(ctx, block, fate, TerminalCause::ShuttingDown).await;
    }
}

/// Runs one block to an ending. Returns only once that block has settled.
///
/// **Two bounds, and the loop owns one of them.** The budget —
/// `WriterRuntime::landing_budget`, measured from the push's admission —
/// bounds the queue wait, every attempt and every sleep. It is **recomputed
/// after every attempt**, so a sleep can never carry the block past it, and
/// the check at the top of the loop is what ends a block whose sleep spent
/// what was left.
///
/// The announced shutdown deadline is `writer::drain`'s, and this loop neither
/// reads nor reasons about it: it asks [`DrainWatch::attempt`] for an attempt
/// and gets one of two shutdown endings instead when the deadline has passed
/// or passes in flight, and every wait between attempts is
/// [`DrainWatch::sleep`].
async fn run_block<R>(
    ctx: &Arc<LandingContext<R>>,
    block: LandingBlock<R>,
    watch: &mut DrainWatch,
    rng: &mut XorShift64,
) where
    R: ChRow + SpoolEncode + Send + Sync,
{
    let mut fate = LandingFate::NeverSent(String::new());
    let mut resends = 0u32;
    loop {
        let remaining = ctx
            .runtime
            .landing_budget
            .saturating_sub(block.admitted_at.elapsed());
        if remaining.is_zero() {
            let msg = if resends == 0 {
                MSG_BUDGET_QUEUED
            } else {
                MSG_BUDGET_BETWEEN
            };
            fate.saw_pre_send(msg.to_string());
            settle_block(ctx, block, fate, TerminalCause::Normal).await;
            return;
        }

        ctx.table_metrics.inflight.fetch_add(1, Ordering::Relaxed);
        // The budget encloses the two phases the client's own deadline does
        // not: the connection checkout and the health ping it may issue.
        let end = watch
            .attempt(remaining, || {
                ctx.inserter
                    .insert_with(&ctx.table, &block.rows, &block.settings)
            })
            .await;
        ctx.table_metrics.inflight.fetch_sub(1, Ordering::Relaxed);

        let sent = match end {
            // The shutdown deadline had passed, so no attempt was created and
            // nothing was sent. The block keeps whatever fate its earlier
            // attempts left, and the queued-shutdown message is read only when
            // that fate is `NeverSent` — where it is exactly what happened,
            // since every pre-send ending means nothing was sent.
            AttemptEnd::NotStarted => {
                fate.saw_pre_send(MSG_SHUTDOWN_QUEUED.to_string());
                settle_block(ctx, block, fate, TerminalCause::ShuttingDown).await;
                return;
            }
            // The shutdown deadline with an attempt in flight. The attempt
            // is abandoned, and it may have committed.
            AttemptEnd::Abandoned => {
                fate.saw_uncertain(MSG_SHUTDOWN_INFLIGHT.to_string());
                settle_block(ctx, block, fate, TerminalCause::ShuttingDown).await;
                return;
            }
            // The budget elapsing mid-attempt. It encloses the checkout and
            // the health ping as well as the send and cannot tell which of
            // the three it interrupted, so it reports the fate as unknown
            // for a block that may never have left the process. That
            // over-reports and never under-reports, which is the direction
            // a claim has to err in.
            AttemptEnd::BudgetElapsed => {
                fate.saw_uncertain(MSG_BUDGET_IN_ATTEMPT.to_string());
                settle_block(ctx, block, fate, TerminalCause::Normal).await;
                return;
            }
            AttemptEnd::Done(result) => result,
        };

        // Every `ChError` here other than `InsertUncertain` is pre-send:
        // `insert_block_with` downgrades every failure from the send onward to
        // `InsertUncertain` whatever its class, and the only errors before it
        // are the connection checkout's and the target's column-metadata
        // read's.
        let resendable = match sent {
            Ok(()) => {
                let latency = block.admitted_at.elapsed();
                commit_block(ctx, block, latency).await;
                return;
            }
            Err(ChError::InsertUncertain(msg)) => {
                fate.saw_uncertain(msg);
                true
            }
            Err(e) if e.is_retryable() => {
                fate.saw_pre_send(e.to_string());
                true
            }
            Err(e) => {
                fate.saw_pre_send(e.to_string());
                false
            }
        };

        if !resendable || resends >= ctx.retries {
            settle_block(ctx, block, fate, TerminalCause::Normal).await;
            return;
        }

        ctx.table_metrics
            .retries_total
            .fetch_add(1, Ordering::Relaxed);
        // The remainder AFTER the attempt, not the one captured before it: a
        // failure arriving near expiry would otherwise sleep past the
        // budget.
        let left = ctx
            .runtime
            .landing_budget
            .saturating_sub(block.admitted_at.elapsed());
        let delay = crate::writer::table::backoff_delay(
            ctx.runtime.retry_base_delay,
            ctx.runtime.retry_max_delay,
            resends + 1,
            rng,
        )
        .min(left);
        watch.sleep(delay).await;
        resends += 1;
    }
}

/// The commit exit: the landing block holds the push's rows, and the target
/// tables are written by that insert's own processing.
///
/// **The commit hook runs while the block is still charged.** It takes a mutex
/// admission and every other worker's commit take too, so a worker can wait
/// there holding a whole block; [`LandingBlock::release`] owns the rule and
/// says what waiting there would otherwise cost.
async fn commit_block<R>(ctx: &Arc<LandingContext<R>>, block: LandingBlock<R>, latency: Duration)
where
    R: ChRow + SpoolEncode + Send + Sync,
{
    ctx.table_metrics
        .record_flush(block.rows.len() as u64, block.bytes, latency);
    if let Some(hook) = &ctx.on_commit {
        hook(&block.rows);
    }
    if let Some(waiter) = block.release(ctx, TargetOutcome::Committed) {
        let _ = waiter.send(Ok(()));
    }
}

/// Every other exit. The fate decides the spool directory and the claim's
/// outcome; `cause` decides only the waiter's error.
///
/// The block is spooled even at the shutdown deadline, because the block is
/// the push's only copy. A spool write that itself fails is logged and counted
/// and never changes the outcome.
pub(crate) async fn settle_block<R>(
    ctx: &Arc<LandingContext<R>>,
    block: LandingBlock<R>,
    fate: LandingFate,
    cause: TerminalCause,
) where
    R: ChRow + SpoolEncode + Send + Sync,
{
    let (kind, outcome, msg) = fate.settle();
    // The spool write runs while the block is still charged, on its error path
    // too. The block's rows are what the write reads, one at a time, so they
    // are live until it returns. What the write itself holds on top of them is
    // one chunk and one row — `writer::spool`'s `write_record` owns that bound
    // — and the release order that makes this the right place for it is
    // [`LandingBlock::release`]'s.
    if let Err(spool_err) = ctx.spool.write(kind, &ctx.table, &block.rows, &msg).await {
        ctx.table_metrics
            .spool_write_failures_total
            .fetch_add(1, Ordering::Relaxed);
        error!(
            table = %ctx.table,
            error = %spool_err,
            "failed to spool a landing block to disk"
        );
    }
    if let Some(waiter) = block.release(ctx, outcome) {
        let err = match cause {
            TerminalCause::ShuttingDown => WriteError::ShuttingDown,
            TerminalCause::Normal => match kind {
                SpoolKind::Uncertain => WriteError::Uncertain(msg),
                SpoolKind::Poison => WriteError::Poisoned(msg),
            },
        };
        let _ = waiter.send(Err(err));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::rows::MetricLandingRow;

    /// The fate is a value the loop carries: an ending whose own knowledge
    /// is "this did not send" must not reverse an earlier attempt that may
    /// have sent, or a block that might be stored would be reported as
    /// provably not stored.
    #[test]
    fn a_pre_send_ending_never_walks_the_fate_back_from_uncertain() {
        let mut fate = LandingFate::NeverSent(String::new());
        fate.saw_uncertain("in flight".to_string());
        fate.saw_pre_send("never sent".to_string());
        assert_eq!(fate, LandingFate::Uncertain("in flight".to_string()));
        let (kind, outcome, msg) = fate.settle();
        assert_eq!(kind, SpoolKind::Uncertain);
        assert_eq!(outcome, TargetOutcome::Uncertain);
        assert_eq!(msg, "in flight");
    }

    /// Pre-send endings only: the block is provably not stored, so the claim
    /// is released and the client's retry is stored.
    #[test]
    fn pre_send_endings_settle_poison_and_not_committed() {
        let mut fate = LandingFate::NeverSent(String::new());
        fate.saw_pre_send("first".to_string());
        fate.saw_pre_send("second".to_string());
        assert_eq!(
            fate.settle(),
            (
                SpoolKind::Poison,
                TargetOutcome::NotCommitted,
                "second".to_string()
            )
        );
    }

    /// Two mints in one process differ — a token derived from the clock
    /// alone would repeat inside one millisecond, and the second push would
    /// be dropped as a resend of the first — and each is a version-7,
    /// variant-`10` UUID in canonical form.
    #[test]
    fn two_minted_tokens_differ_and_are_version_7_uuids() {
        let mut rng = XorShift64::seeded();
        let a = mint_landing_token(&mut rng);
        let b = mint_landing_token(&mut rng);
        assert_ne!(a, b, "two sealed blocks must not share a token");
        for token in [&a, &b] {
            assert_eq!(token.len(), 36, "canonical hyphenated form: {token}");
            let bytes = token.as_bytes();
            assert_eq!(bytes[8], b'-');
            assert_eq!(bytes[13], b'-');
            assert_eq!(bytes[18], b'-');
            assert_eq!(bytes[23], b'-');
            assert_eq!(bytes[14] as char, '7', "version nibble: {token}");
            assert!(
                matches!(bytes[19] as char, '8' | '9' | 'a' | 'b'),
                "variant bits 10: {token}"
            );
            assert!(
                token
                    .chars()
                    .all(|c| c == '-' || c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
                "lowercase hex only: {token}"
            );
        }
    }

    /// **The block-overhead figure does not depend on the row type.** The only
    /// `R`-dependent field is a `Vec<R>`, whose header is three words whatever
    /// `R` is, so one derivation prices both signals' blocks — which is what
    /// lets `docs/ingest-one-source-table.md` §6's derivation be cited from the
    /// logs path rather than recomputed there.
    #[test]
    fn the_block_overhead_is_the_same_figure_for_either_signal() {
        assert_eq!(
            landing_block_overhead_bytes::<MetricLandingRow>(),
            landing_block_overhead_bytes::<crate::writer::rows::LogLandingRow>(),
        );
    }

    /// **The queue gauge is subtracted in one place, and in one order.**
    ///
    /// Two halves. The first is the census the metrics path already carried,
    /// widened to this module — the one `release` both signals use: an exit
    /// path nobody has written yet either goes through that function or this
    /// case names it. The second is the order *inside* `release`, which no
    /// external observation can see without a race: the ticket's charged vector
    /// goes back inside `claim.settle`, and the subtract that follows it is one
    /// instruction later. Both are lexical; `docs/ingest-one-source-table.md`
    /// §9 D9 carries what a lexical census cannot see.
    ///
    /// **The third assertion is over the two per-signal modules**, which must
    /// name no subtract at all: the guarantee is one release point for both
    /// signals, and a census confined to this file would be a claim about a set
    /// checked against a subset.
    ///
    /// The needles are built at run time so the census cannot match itself.
    #[test]
    fn the_landing_reservation_is_released_in_one_place_and_in_one_order() {
        const SRC: &str = include_str!("landing.rs");
        let gauge = "queued_bytes";
        let subtract = format!("{gauge}.{}", "fetch_sub");
        let add = format!("{gauge}.{}", "fetch_add");

        /// The function a line declares, if it declares one: what precedes
        /// `fn` has to be visibility and qualifiers, so prose naming a
        /// function does not open one.
        fn declared_fn(line: &str) -> Option<&str> {
            let (before, after) = line.trim_start().split_once("fn ")?;
            before
                .split_whitespace()
                .all(|w| {
                    matches!(
                        w,
                        "pub" | "pub(crate)" | "pub(super)" | "async" | "const" | "unsafe"
                    )
                })
                .then(|| after.split(['(', '<', ' ']).next().unwrap_or_default())
        }

        let lines: Vec<&str> = SRC.lines().collect();
        let mut current = "<the module body>";
        let mut subtracting: Vec<&str> = Vec::new();
        let mut adding: Vec<&str> = Vec::new();
        for (i, line) in lines.iter().enumerate() {
            if let Some(name) = declared_fn(line) {
                current = name;
            }
            // A statement `rustfmt` may have split over a receiver and a
            // method call, read as one string with its whitespace removed. It
            // ends at this line, so the function named is the one the call sits
            // in rather than one three lines above it.
            let window: String = lines[i.saturating_sub(2)..=i]
                .join("")
                .split_whitespace()
                .collect();
            if window.contains(&subtract) && !subtracting.contains(&current) {
                subtracting.push(current);
            }
            if window.contains(&add) && !adding.contains(&current) {
                adding.push(current);
            }
        }

        assert_eq!(
            subtracting,
            vec!["release"],
            "the queue gauge is decremented outside LandingBlock::release, \
             where the rows it is charged for are still live"
        );
        assert!(
            adding.is_empty(),
            "the queue gauge is incremented here rather than through \
             reserve_queued_bytes, which is the only gate that refuses over \
             the limit: {adding:?}"
        );

        // The order inside `release`: the subtract comes after the rows, the
        // settings and the claim have gone.
        let body = SRC
            .split_once("    fn release(")
            .expect("release is declared here")
            .1;
        let body = body
            .split_once("\n    }\n")
            .expect("release has a closing brace")
            .0;
        let at = |needle: &str| {
            body.find(needle)
                .unwrap_or_else(|| panic!("release no longer contains `{needle}`"))
        };
        let subtract_at = at(&subtract);
        for earlier in ["drop(rows)", "drop(settings)", "claim.settle("] {
            assert!(
                at(earlier) < subtract_at,
                "`{earlier}` must precede the subtract inside release: the \
                 charge covers what the block holds, so it is given back only \
                 once the block is gone"
            );
        }

        // Neither per-signal module may subtract the gauge: one release point
        // for both signals is the guarantee.
        for (name, src) in [
            ("log.rs", include_str!("log.rs")),
            ("metric.rs", include_str!("metric.rs")),
        ] {
            assert!(
                !src.contains(&subtract),
                "{name} subtracts the queue gauge itself; every logs and \
                 metrics ending must release through LandingBlock::release"
            );
        }
    }
}
