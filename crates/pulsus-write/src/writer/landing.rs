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
/// | `settings`: the signal's owned key/value pairs | [`landing_settings_charge`]: the allocation of the widest set any signal sends — every constructor built at a token as wide as [`mint_landing_token`] writes and at each signal's row floor and ceiling, measured with `QuerySettings::allocated_bytes` (the vector's slots and every string by capacity). Measured rather than counted, so a setting added to any signal's insert raises the charge with it |
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
/// is, and the settings term is the widest set any signal sends, so every
/// signal's block is charged at least what its own settings hold (issue #602
/// part 3b-1: the trace set is the widest).
pub fn landing_block_overhead_bytes<R>() -> u64 {
    2 * std::mem::size_of::<LandingBlock<R>>() as u64
        + landing_settings_charge()
        + 4 * std::mem::size_of::<push_dedup::PushDigest>() as u64
        + 256
}

/// A token as wide as [`mint_landing_token`] writes: 36 characters.
const WIDEST_TOKEN: &str = "00000000-0000-7000-8000-000000000000";

/// The bytes the widest landing settings set allocates, measured on the
/// sets themselves: [`QuerySettings::landing_insert`] (metrics and logs) and
/// [`QuerySettings::trace_landing_insert`], each at [`WIDEST_TOKEN`] and at
/// every signal's row floor and ceiling, the most any of them allocates.
/// Built once and kept.
pub fn landing_settings_charge() -> u64 {
    static CHARGE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *CHARGE.get_or_init(|| {
        let rows = [
            pulsus_config::METRICS_LANDING_MAX_ROWS_FLOOR,
            pulsus_config::METRICS_LANDING_MAX_ROWS_CEILING,
            pulsus_config::LOG_LANDING_MAX_ROWS_FLOOR,
            pulsus_config::LOG_LANDING_MAX_ROWS_CEILING,
            pulsus_config::TRACE_LANDING_MAX_ROWS_FLOOR,
            pulsus_config::TRACE_LANDING_MAX_ROWS_CEILING,
        ];
        rows.iter()
            .flat_map(|&rows| {
                [
                    QuerySettings::landing_insert(WIDEST_TOKEN, rows),
                    QuerySettings::trace_landing_insert(WIDEST_TOKEN, rows),
                ]
            })
            .map(|settings| settings.allocated_bytes())
            .max()
            .unwrap_or(0)
    })
}

/// What the queue is charged for one landing block whose rows price
/// `row_bytes`. **One figure prices a block**: the per-push byte ceiling
/// refuses against the sum over a push's blocks, `reserve_queued_bytes` takes
/// that sum, whichever ending settles a block releases its own, and
/// `record_flush` reports it.
pub fn landing_charge<R>(row_bytes: u64) -> u64 {
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
    /// The first and last UTC day, as days since the epoch, of the block's
    /// rows in day-partitioned targets (issue #494 part 2), or `None` for a
    /// block with none. A batch's span is its members' union, and a batch
    /// closes before it would pass [`BATCH_DAY_LIMIT`] days.
    span: Option<(i64, i64)>,
}

/// The widest inclusive UTC-date span one landing batch may cover: the
/// server's `max_partitions_per_insert_block`, which every landing insert
/// pins, so one insert makes at most this many parts in any day-partitioned
/// target (issue #494 part 2).
pub(crate) const BATCH_DAY_LIMIT: u64 = pulsus_clickhouse::MAX_PARTITIONS_PER_INSERT_BLOCK;

/// How many members of a failed batch write their spool copies at once.
/// Each holds its charge, rows and waiter until its own write is done.
pub(crate) const SPOOL_WRITE_CONCURRENCY: usize = 4;

/// A landing row whose receive stamp a batch can read and set: every row of
/// a batch takes its first member's (issue #494 part 2).
pub(crate) trait LandingRow {
    fn received_ms(&self) -> i64;
    fn set_received_ms(&mut self, received_ms: i64);
}

impl LandingRow for crate::writer::rows::LogLandingRow {
    fn received_ms(&self) -> i64 {
        self.received_ms
    }
    fn set_received_ms(&mut self, received_ms: i64) {
        crate::writer::rows::LogLandingRow::set_received_ms(self, received_ms);
    }
}

impl LandingRow for crate::writer::rows::TraceLandingRow {
    fn received_ms(&self) -> i64 {
        self.received_ms
    }
    fn set_received_ms(&mut self, received_ms: i64) {
        crate::writer::rows::TraceLandingRow::set_received_ms(self, received_ms);
    }
}

impl LandingRow for crate::writer::rows::MetricLandingRow {
    fn received_ms(&self) -> i64 {
        self.received_ms
    }
    fn set_received_ms(&mut self, received_ms: i64) {
        self.received_ms = received_ms;
    }
}

/// The inclusive UTC-day span of `days`, as days since the epoch.
pub(crate) fn day_span(days: impl IntoIterator<Item = i64>) -> Option<(i64, i64)> {
    days.into_iter().fold(None, |acc, d| match acc {
        None => Some((d, d)),
        Some((lo, hi)) => Some((lo.min(d), hi.max(d))),
    })
}

/// The union of two spans, and how many days it covers.
fn union_span(a: Option<(i64, i64)>, b: Option<(i64, i64)>) -> Option<(i64, i64)> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some((a0, a1)), Some((b0, b1))) => Some((a0.min(b0), a1.max(b1))),
    }
}

fn span_days(span: Option<(i64, i64)>) -> u64 {
    span.map_or(0, |(lo, hi)| (hi - lo + 1) as u64)
}

impl<R> LandingBlock<R> {
    /// The one place a `LandingBlock` is built, so the case that prices what
    /// one holds prices the same value the queue does.
    #[allow(clippy::too_many_arguments)] // one admission's rows, charge, claim and waiter
    pub(crate) fn seal(
        rows: Vec<R>,
        bytes: u64,
        settings: QuerySettings,
        claim: ClaimTicket,
        waiter: Option<oneshot::Sender<Result<(), WriteError>>>,
        admitted_at: tokio::time::Instant,
        span: Option<(i64, i64)>,
    ) -> Self {
        LandingBlock {
            rows,
            bytes,
            settings,
            claim,
            waiter,
            admitted_at,
            span,
        }
    }

    /// Reports the claim's outcome now, leaving an inert ticket behind, so a
    /// failed batch's claims are all reported before any member's spool copy
    /// is written (issue #494 part 2).
    fn report_claim(&mut self, outcome: TargetOutcome) {
        std::mem::replace(&mut self.claim, ClaimTicket::inert()).settle(outcome);
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
            span: _,
        } = self;
        drop(rows);
        drop(settings);
        claim.settle(outcome);
        ctx.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
        waiter
    }
}

/// A commit-time callback over the committed batch's rows, one slice per
/// member: the registration cache promotion, which is the one thing a signal
/// does at its commit that this module cannot. Called once per committed
/// insert.
///
/// A named alias rather than an inline `Option<Arc<dyn Fn(..)>>`, for Clippy's
/// `type_complexity`.
pub(crate) type CommitHook<R> = Arc<dyn Fn(&[&[R]]) + Send + Sync>;

/// A callback for every ending but a commit: the signal's registration
/// caches are emptied, so the next push of each stream or series registers
/// it again (issue #786).
pub(crate) type FailureHook = Arc<dyn Fn() + Send + Sync>;

/// What a landing worker needs to run a batch to an ending.
pub(crate) struct LandingContext<R> {
    pub(crate) table: Arc<str>,
    pub(crate) inserter: Arc<dyn BlockInserter<R>>,
    pub(crate) runtime: Arc<WriterRuntime>,
    /// The landing table's own per-table counters. One insert per batch for
    /// logs and traces, per block for metrics, so `flushes_total` counts
    /// inserts stored and `rows_total` counts landed events of every kind.
    pub(crate) table_metrics: Arc<TableMetrics>,
    pub(crate) queued_bytes: Arc<AtomicU64>,
    pub(crate) spool: Arc<SpoolWriter>,
    /// Called by [`commit_batch`] with the committed batch's rows, **while
    /// every member is still charged**. A fixed-size field on the context
    /// rather than on the block, so what a queued block holds does not grow
    /// with the push.
    pub(crate) on_commit: Option<CommitHook<R>>,
    /// Called once per batch by [`settle_batch`], before any member's claim
    /// is reported.
    pub(crate) on_failure: Option<FailureHook>,
    /// `PULSUS_<SIGNAL>_LANDING_RETRIES`, resolved by the signal's writer so
    /// this loop names no signal's config key.
    pub(crate) retries: u32,
    /// `PULSUS_<SIGNAL>_LANDING_MAX_ROWS`, likewise — the value both pinned
    /// row limits carry on every insert sent against this table, and the row
    /// count a batch stays strictly under.
    pub(crate) max_rows: u64,
    /// The settings constructor of a batched signal (logs, traces), called
    /// with a token minted when a batch closes; `None` for metrics, whose
    /// blocks are never batched and carry their own settings (issue #494
    /// part 2).
    pub(crate) batch_settings: Option<fn(&str, u64) -> QuerySettings>,
    /// The batch tokens' generator, shared by this queue's workers so no two
    /// batches share a token.
    pub(crate) token_rng: std::sync::Mutex<XorShift64>,
}

impl<R> LandingContext<R> {
    /// How long a worker forms a batch: `PULSUS_LANDING_BATCH_MS` for a
    /// batched signal, zero for metrics.
    fn window(&self) -> Duration {
        if self.batch_settings.is_some() {
            self.runtime.landing_batch
        } else {
            Duration::ZERO
        }
    }
}

/// A landing queue: its receiver, and the block a former kept back because
/// it would have broken a bound of the batch it was forming (issue #494
/// part 2). The next former takes the carried block first.
pub(crate) struct LandingQueue<R> {
    rx: mpsc::UnboundedReceiver<LandingBlock<R>>,
    carry: Option<LandingBlock<R>>,
}

impl<R> LandingQueue<R> {
    pub(crate) fn new(rx: mpsc::UnboundedReceiver<LandingBlock<R>>) -> Self {
        LandingQueue { rx, carry: None }
    }
}

/// The receiver shared by one signal's workers.
pub(crate) type SharedQueue<R> = Arc<tokio::sync::Mutex<LandingQueue<R>>>;

/// What the loop knows about a batch short of a commit — **a value the loop
/// carries, not a decision each ending makes**. Two methods write it and
/// there is no third, and `Uncertain` is terminal short of a commit: an
/// ending whose own knowledge is "this did not send" can never walk the fate
/// back from an earlier attempt that may have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LandingFate {
    NeverSent(String),
    Uncertain(String),
}

/// Why a batch reached [`settle_batch`]. It decides the waiter's error and
/// nothing else: the spool directory and the claim's outcome come from the
/// fate. A shutdown is not a fate — a batch abandoned at the shutdown
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
/// string per insert and nothing else a UUID library offers, and the layout
/// is 128 bits of two fields it already has to hand.
///
/// It is the insert's identity for retry purposes only — it is not a landed
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

/// What one turn of a former produced: a batch to send, a block whose
/// deadline had passed when it was taken (settled alone), and whether the
/// queue is closed and empty.
struct Formed<R> {
    batch: Vec<LandingBlock<R>>,
    expired: Option<LandingBlock<R>>,
    done: bool,
}

/// A block's deadline: its admission plus the landing budget.
fn deadline_of<R>(ctx: &LandingContext<R>, block: &LandingBlock<R>) -> tokio::time::Instant {
    block.admitted_at + ctx.runtime.landing_budget
}

/// Takes the next block: the carried one, else the queue's next. Once the
/// drain is announced the queue is closed and drained.
async fn take_first<R>(
    queue: &mut LandingQueue<R>,
    watch: &mut DrainWatch,
) -> Option<LandingBlock<R>> {
    if let Some(block) = queue.carry.take() {
        return Some(block);
    }
    if watch.is_announced() {
        queue.rx.close();
        return queue.rx.recv().await;
    }
    tokio::select! {
        block = queue.rx.recv() => block,
        () = watch.until_announced() => {
            queue.rx.close();
            queue.rx.recv().await
        }
    }
}

/// Forms one batch under the queue's mutex, by issue #494 part 2's rules: it
/// opens at the first block taken and closes `window` later; sooner when the
/// next block would bring its rows to `max_rows`, its charge above
/// `PULSUS_BATCH_BYTES` or its day span above [`BATCH_DAY_LIMIT`] (that block
/// is carried to the next batch); at once when its earliest member deadline
/// is at or before its close; and at once when the drain is announced. A
/// block whose deadline has passed when it is taken joins nothing.
async fn form<R>(
    ctx: &LandingContext<R>,
    queue: &mut LandingQueue<R>,
    watch: &mut DrainWatch,
) -> Formed<R> {
    let Some(first) = take_first(queue, watch).await else {
        return Formed {
            batch: Vec::new(),
            expired: None,
            done: true,
        };
    };
    let first_take = tokio::time::Instant::now();
    if deadline_of(ctx, &first) <= first_take {
        return Formed {
            batch: Vec::new(),
            expired: Some(first),
            done: false,
        };
    }
    let window = ctx.window();
    let close_at = first_take + window;
    let mut rows = first.rows.len() as u64;
    let mut bytes = first.bytes;
    let mut span = first.span;
    let mut deadline = deadline_of(ctx, &first);
    let mut batch = vec![first];
    let formed = |batch, expired| Formed {
        batch,
        expired,
        done: false,
    };
    if window.is_zero() || deadline <= close_at {
        return formed(batch, None);
    }
    loop {
        let next = if watch.is_announced() {
            // The drain is announced: what is already queued may join, and
            // nothing is waited for.
            queue.rx.close();
            match queue.rx.try_recv() {
                Ok(block) => block,
                Err(_) => return formed(batch, None),
            }
        } else {
            tokio::select! {
                biased;
                () = tokio::time::sleep_until(close_at) => return formed(batch, None),
                () = watch.until_announced() => return formed(batch, None),
                block = queue.rx.recv() => match block {
                    Some(block) => block,
                    None => return formed(batch, None),
                },
            }
        };
        let take = tokio::time::Instant::now();
        if deadline_of(ctx, &next) <= take {
            return formed(batch, Some(next));
        }
        let joined_span = union_span(span, next.span);
        if rows + next.rows.len() as u64 >= ctx.max_rows
            || bytes + next.bytes > ctx.runtime.batch_bytes
            || span_days(joined_span) > BATCH_DAY_LIMIT
        {
            queue.carry = Some(next);
            return formed(batch, None);
        }
        rows += next.rows.len() as u64;
        bytes += next.bytes;
        span = joined_span;
        deadline = deadline.min(deadline_of(ctx, &next));
        batch.push(next);
        if deadline <= close_at {
            return formed(batch, None);
        }
    }
}

/// Spawns one insert worker on the landing queue. Every worker runs the same
/// loop: form the next batch under the queue's mutex, release it, run the
/// batch to an ending through [`run_batch`], repeat. Once the drain is
/// announced — by `DrainBoundary::shutdown`, or by `DrainBoundary::cancel`
/// when the writer is dropped — the worker closes the queue and keeps taking
/// blocks until it is empty, so a block still queued at the announcement has
/// the endings of any other block: committed; filed when the landing budget
/// is spent, or when an insert error cannot be resent or the resends are
/// used up; or filed at the deadline — not started once it has passed,
/// abandoned if in flight when it passes.
pub(crate) fn spawn_landing_worker<R>(
    ctx: Arc<LandingContext<R>>,
    queue: SharedQueue<R>,
    mut watch: DrainWatch,
) -> tokio::task::JoinHandle<()>
where
    R: ChRow + SpoolEncode + LandingRow + Clone + Send + Sync + 'static,
{
    tokio::spawn(async move {
        // Seeded from the clock, unless a case fixed the seed so its retry
        // delays are the same every run (`WriterRuntime::retry_jitter_seed`).
        let mut rng = match ctx.runtime.retry_jitter_seed {
            Some(seed) => XorShift64::from_seed(seed),
            None => XorShift64::seeded(),
        };
        loop {
            let Formed {
                batch,
                expired,
                done,
            } = {
                let mut queue = queue.lock().await;
                form(&ctx, &mut queue, &mut watch).await
            };
            // A block taken past its deadline settles alone at once, while
            // the batch it closed is sent.
            let expired = async {
                if let Some(block) = expired {
                    let mut fate = LandingFate::NeverSent(String::new());
                    fate.saw_pre_send(MSG_BUDGET_QUEUED.to_string());
                    settle_batch(&ctx, vec![block], fate, TerminalCause::Normal).await;
                }
            };
            let sent = async {
                if !batch.is_empty() {
                    run_batch(&ctx, batch, &mut watch, &mut rng).await;
                }
            };
            tokio::join!(expired, sent);
            if done {
                return;
            }
        }
    })
}

/// Runs one batch to an ending. Returns only once every member has settled.
///
/// **One deadline, the earliest member's.** Every attempt, every lapse
/// before one and every sleep between them runs inside the batch's deadline,
/// the earliest member admission plus `WriterRuntime::landing_budget`, and
/// at it every member settles with the same ending. Every attempt sends the
/// same members' rows, in the same order, under the one token minted for the
/// batch when it closed.
///
/// The announced shutdown deadline is `writer::drain`'s, and this loop neither
/// reads nor reasons about it: it asks [`DrainWatch::attempt`] for an attempt
/// and gets one of two shutdown endings instead when the deadline has passed
/// or passes in flight, and every wait between attempts is
/// [`DrainWatch::sleep`].
async fn run_batch<R>(
    ctx: &Arc<LandingContext<R>>,
    mut members: Vec<LandingBlock<R>>,
    watch: &mut DrainWatch,
    rng: &mut XorShift64,
) where
    R: ChRow + SpoolEncode + LandingRow + Clone + Send + Sync,
{
    let first_admitted = members
        .iter()
        .map(|m| m.admitted_at)
        .min()
        .expect("a batch has a member");
    let deadline = first_admitted + ctx.runtime.landing_budget;
    // A batched signal's insert: every row takes the first member's receive
    // stamp, so the batch is one block on one shard and in one landing
    // partition, and the token is minted for the batch.
    let settings = match ctx.batch_settings {
        Some(build) => {
            if let Some(stamp) = members
                .first()
                .and_then(|m| m.rows.first())
                .map(LandingRow::received_ms)
            {
                for member in &mut members {
                    for row in &mut member.rows {
                        row.set_received_ms(stamp);
                    }
                }
            }
            let token = {
                let mut rng = ctx.token_rng.lock().expect("landing token rng poisoned");
                mint_landing_token(&mut rng)
            };
            build(&token, ctx.max_rows)
        }
        None => std::mem::take(&mut members[0].settings),
    };

    let mut fate = LandingFate::NeverSent(String::new());
    let mut resends = 0u32;
    loop {
        let budget_spent = |fate: &mut LandingFate| {
            let msg = if resends == 0 {
                MSG_BUDGET_QUEUED
            } else {
                MSG_BUDGET_BETWEEN
            };
            fate.saw_pre_send(msg.to_string());
        };
        if tokio::time::Instant::now() >= deadline {
            budget_spent(&mut fate);
            settle_batch(ctx, members, fate, TerminalCause::Normal).await;
            return;
        }
        #[cfg(test)]
        if !ctx.runtime.test_lapse.is_zero() {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            watch.sleep(ctx.runtime.test_lapse.min(left)).await;
            if tokio::time::Instant::now() >= deadline {
                budget_spent(&mut fate);
                settle_batch(ctx, members, fate, TerminalCause::Normal).await;
                return;
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());

        ctx.table_metrics.inflight.fetch_add(1, Ordering::Relaxed);
        // The budget encloses the two phases the client's own deadline does
        // not: the connection checkout and the health ping it may issue.
        let end = {
            let parts: Vec<&[R]> = members.iter().map(|m| m.rows.as_slice()).collect();
            watch
                .attempt(remaining, || {
                    ctx.inserter
                        .insert_parts_with(&ctx.table, &parts, &settings)
                })
                .await
        };
        ctx.table_metrics.inflight.fetch_sub(1, Ordering::Relaxed);

        let sent = match end {
            // The shutdown deadline had passed, so no attempt was created and
            // nothing was sent. The batch keeps whatever fate its earlier
            // attempts left, and the queued-shutdown message is read only when
            // that fate is `NeverSent` — where it is exactly what happened,
            // since every pre-send ending means nothing was sent.
            AttemptEnd::NotStarted => {
                fate.saw_pre_send(MSG_SHUTDOWN_QUEUED.to_string());
                settle_batch(ctx, members, fate, TerminalCause::ShuttingDown).await;
                return;
            }
            // The shutdown deadline with an attempt in flight. The attempt
            // is abandoned, and it may have committed.
            AttemptEnd::Abandoned => {
                fate.saw_uncertain(MSG_SHUTDOWN_INFLIGHT.to_string());
                settle_batch(ctx, members, fate, TerminalCause::ShuttingDown).await;
                return;
            }
            // The budget elapsing mid-attempt. It encloses the checkout and
            // the health ping as well as the send and cannot tell which of
            // the three it interrupted, so it reports the fate as unknown
            // for a batch that may never have left the process. That
            // over-reports and never under-reports, which is the direction
            // a claim has to err in. An answer arriving at the deadline
            // itself is the same ending: the deadline is inclusive.
            AttemptEnd::BudgetElapsed => {
                fate.saw_uncertain(MSG_BUDGET_IN_ATTEMPT.to_string());
                settle_batch(ctx, members, fate, TerminalCause::Normal).await;
                return;
            }
            AttemptEnd::Done(_) if tokio::time::Instant::now() >= deadline => {
                fate.saw_uncertain(MSG_BUDGET_IN_ATTEMPT.to_string());
                settle_batch(ctx, members, fate, TerminalCause::Normal).await;
                return;
            }
            AttemptEnd::Done(result) => result,
        };

        // Every `ChError` here other than `InsertUncertain` is pre-send:
        // `insert_parts_with` downgrades every failure from the send onward
        // to `InsertUncertain` whatever its class, and the only errors before
        // it are the connection checkout's and the target's column-metadata
        // read's.
        let resendable = match sent {
            Ok(()) => {
                commit_batch(ctx, members, first_admitted.elapsed()).await;
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
            settle_batch(ctx, members, fate, TerminalCause::Normal).await;
            return;
        }

        ctx.table_metrics
            .retries_total
            .fetch_add(1, Ordering::Relaxed);
        // The remainder AFTER the attempt, not the one captured before it: a
        // failure arriving near expiry would otherwise sleep past the
        // deadline.
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let jittered = crate::writer::table::backoff_delay(
            ctx.runtime.retry_base_delay,
            ctx.runtime.retry_max_delay,
            resends + 1,
            rng,
        );
        #[cfg(test)]
        let jittered = ctx.runtime.test_backoff.unwrap_or(jittered);
        watch.sleep(jittered.min(left)).await;
        resends += 1;
    }
}

/// The commit exit: the batch's insert holds its members' rows, and the
/// target tables are written by that insert's own processing.
///
/// **The commit hook runs once, while every member is still charged.** It
/// takes a mutex admission and every other worker's commit take too, so a
/// worker can wait there holding a whole batch; [`LandingBlock::release`]
/// owns the rule and says what waiting there would otherwise cost.
async fn commit_batch<R>(
    ctx: &Arc<LandingContext<R>>,
    members: Vec<LandingBlock<R>>,
    latency: Duration,
) where
    R: ChRow + SpoolEncode + Send + Sync,
{
    let rows: u64 = members.iter().map(|m| m.rows.len() as u64).sum();
    let bytes: u64 = members.iter().map(|m| m.bytes).sum();
    ctx.table_metrics.record_flush(rows, bytes, latency);
    if let Some(hook) = &ctx.on_commit {
        let parts: Vec<&[R]> = members.iter().map(|m| m.rows.as_slice()).collect();
        hook(&parts);
    }
    for member in members {
        if let Some(waiter) = member.release(ctx, TargetOutcome::Committed) {
            let _ = waiter.send(Ok(()));
        }
    }
}

/// [`settle_batch`] for one block, as the queue-closed path at admission
/// settles it.
pub(crate) async fn settle_block<R>(
    ctx: &Arc<LandingContext<R>>,
    block: LandingBlock<R>,
    fate: LandingFate,
    cause: TerminalCause,
) where
    R: ChRow + SpoolEncode + Send + Sync,
{
    settle_batch(ctx, vec![block], fate, cause).await;
}

/// Every other exit, for every member of a batch with the batch's one fate.
/// The fate decides the spool directory and the claim's outcome; `cause`
/// decides only the waiter's error.
///
/// **Every member's claim is reported first**, so no claim waits behind a
/// spool write; then the members' spool copies are written at most
/// [`SPOOL_WRITE_CONCURRENCY`] at a time, each member keeping its charge,
/// rows and waiter until its own write is done. A member is spooled even at
/// the shutdown deadline, because its block is the push's only copy. A spool
/// write that itself fails is logged and counted and never changes the
/// outcome.
pub(crate) async fn settle_batch<R>(
    ctx: &Arc<LandingContext<R>>,
    mut members: Vec<LandingBlock<R>>,
    fate: LandingFate,
    cause: TerminalCause,
) where
    R: ChRow + SpoolEncode + Send + Sync,
{
    use futures::StreamExt;

    let (kind, outcome, msg) = fate.settle();
    for member in &mut members {
        member.report_claim(outcome);
    }
    let msg = &msg;
    futures::stream::iter(members)
        .for_each_concurrent(SPOOL_WRITE_CONCURRENCY, |member| async move {
            // The spool write runs while the member is still charged, on its
            // error path too. The member's rows are what the write reads, one
            // at a time, so they are live until it returns. What the write
            // itself holds on top of them is one chunk and one row —
            // `writer::spool`'s `write_record` owns that bound — and the
            // release order that makes this the right place for it is
            // [`LandingBlock::release`]'s.
            if let Err(spool_err) = ctx.spool.write(kind, &ctx.table, &member.rows, msg).await {
                ctx.table_metrics
                    .spool_write_failures_total
                    .fetch_add(1, Ordering::Relaxed);
                error!(
                    table = %ctx.table,
                    error = %spool_err,
                    "failed to spool a landing block to disk"
                );
            }
            if let Some(waiter) = member.release(ctx, outcome) {
                let err = match cause {
                    TerminalCause::ShuttingDown => WriteError::ShuttingDown,
                    TerminalCause::Normal => match kind {
                        SpoolKind::Uncertain => WriteError::Uncertain(msg.clone()),
                        SpoolKind::Poison => WriteError::Poisoned(msg.clone()),
                    },
                };
                let _ = waiter.send(Err(err));
            }
        })
        .await;
}

#[cfg(test)]
#[path = "landing_batch_tests.rs"]
mod batch_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::rows::MetricLandingRow;

    /// One log line with `body`, for the batch cases beside this module.
    pub(super) fn log_row(body: String) -> crate::protocols::otlp_logs::LogRow {
        crate::protocols::otlp_logs::LogRow {
            service: "checkout".to_string(),
            fingerprint: pulsus_model::Fingerprint::from_raw(7),
            timestamp_ns: pulsus_model::UnixNano(1_700_000_000_000_000_000),
            severity: 0,
            body,
            structured_metadata: String::new(),
        }
    }

    /// Counts insert calls and answers each at once.
    struct CountingInserter(std::sync::atomic::AtomicUsize);

    impl BlockInserter<MetricLandingRow> for CountingInserter {
        fn insert<'a>(
            &'a self,
            _table: &'a str,
            _rows: &'a [MetricLandingRow],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ChError>> + Send + 'a>>
        {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }

    /// The boundary is dropped on the worker's own thread, after the worker
    /// has taken a block off the queue and read no deadline, and before its
    /// attempt is authorized — the window a writer's teardown can open for a
    /// worker on another thread. A dropped boundary cancels, so that attempt
    /// is refused: no insert is started and the block is filed never sent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_boundary_dropped_after_a_dequeue_starts_no_insert() {
        let spool_dir = std::env::temp_dir().join(format!(
            "pulsus-landing-dropped-boundary-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let mut runtime = WriterRuntime::from_config(&pulsus_config::WriterConfig::default());
        runtime.spool_dir = spool_dir.clone();
        let metrics = Arc::new(crate::writer::metrics::WriterMetrics::default());
        let inserter = Arc::new(CountingInserter(std::sync::atomic::AtomicUsize::new(0)));
        let ctx = Arc::new(LandingContext {
            table: Arc::from("metric_landing"),
            inserter: inserter.clone(),
            runtime: Arc::new(runtime),
            table_metrics: Arc::new(TableMetrics::default()),
            queued_bytes: Arc::new(AtomicU64::new(0)),
            spool: Arc::new(SpoolWriter::new(spool_dir.clone(), metrics)),
            on_commit: None,
            on_failure: None,
            retries: 0,
            max_rows: 1_000,
            batch_settings: None,
            token_rng: std::sync::Mutex::new(XorShift64::seeded()),
        });
        let (tx, rx) = mpsc::unbounded_channel();
        let boundary = crate::writer::drain::DrainBoundary::new();
        let slot = Arc::new(std::sync::Mutex::new(Some(boundary)));
        let watch = {
            let guard = slot.lock().expect("slot");
            let slot = slot.clone();
            guard
                .as_ref()
                .expect("the boundary")
                .watch()
                .with_barrier(move || drop(slot.lock().expect("slot").take()))
        };
        tx.send(LandingBlock::seal(
            Vec::<MetricLandingRow>::new(),
            0,
            QuerySettings::new(),
            ClaimTicket::inert(),
            None,
            tokio::time::Instant::now(),
            None,
        ))
        .unwrap_or_else(|_| panic!("the queue is open"));
        let worker = spawn_landing_worker(
            ctx,
            Arc::new(tokio::sync::Mutex::new(LandingQueue::new(rx))),
            watch,
        );
        tokio::time::timeout(Duration::from_secs(10), worker)
            .await
            .expect("the worker ends once the cancelled queue is empty")
            .expect("the worker does not panic");

        assert_eq!(
            inserter.0.load(Ordering::SeqCst),
            0,
            "no insert was started"
        );
        let poison = std::fs::read_dir(spool_dir.join("poison").join("metric_landing"))
            .map(|d| d.count())
            .unwrap_or(0);
        assert_eq!(poison, 1, "the block is filed never sent");
        drop(tx);
        std::fs::remove_dir_all(&spool_dir).ok();
    }

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

    /// **The block charge covers every signal's settings as they are built**
    /// (issue #602 part 3b-1). For each signal's own constructor, at a token
    /// `mint_landing_token` writes and at the floor, the default and the
    /// largest accepted row ceiling, the settings' measured allocation plus
    /// the block's fixed terms must fit in the charge. A setting added to any
    /// signal's insert without the charge following fails here.
    #[test]
    fn the_block_charge_covers_every_signals_settings_as_built() {
        use crate::writer::rows::{LogLandingRow, TraceLandingRow};
        use pulsus_config as cfg;
        fn fixed<R>() -> u64 {
            2 * std::mem::size_of::<LandingBlock<R>>() as u64
                + 4 * std::mem::size_of::<push_dedup::PushDigest>() as u64
                + 256
        }
        let token = mint_landing_token(&mut XorShift64::seeded());
        assert_eq!(token.len(), 36);
        let defaults = pulsus_config::WriterConfig::default();
        let mut short = Vec::new();
        let mut check = |signal: &str, settings: QuerySettings, fixed: u64, charged: u64| {
            let needed = settings.allocated_bytes() + fixed;
            if needed > charged {
                short.push(format!(
                    "{signal}: the block holds {needed} bytes and is charged {charged}"
                ));
            }
        };
        for rows in [
            cfg::METRICS_LANDING_MAX_ROWS_FLOOR,
            defaults.metrics_landing_max_rows,
            cfg::METRICS_LANDING_MAX_ROWS_CEILING,
        ] {
            check(
                "metrics",
                QuerySettings::landing_insert(&token, rows),
                fixed::<MetricLandingRow>(),
                landing_block_overhead_bytes::<MetricLandingRow>(),
            );
        }
        for rows in [
            cfg::LOG_LANDING_MAX_ROWS_FLOOR,
            defaults.log_landing_max_rows,
            cfg::LOG_LANDING_MAX_ROWS_CEILING,
        ] {
            check(
                "logs",
                QuerySettings::landing_insert(&token, rows),
                fixed::<LogLandingRow>(),
                landing_block_overhead_bytes::<LogLandingRow>(),
            );
        }
        for rows in [
            cfg::TRACE_LANDING_MAX_ROWS_FLOOR,
            defaults.trace_landing_max_rows,
            cfg::TRACE_LANDING_MAX_ROWS_CEILING,
        ] {
            check(
                "traces",
                QuerySettings::trace_landing_insert(&token, rows),
                fixed::<TraceLandingRow>(),
                landing_block_overhead_bytes::<TraceLandingRow>(),
            );
        }
        assert!(short.is_empty(), "{}", short.join("\n"));
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
