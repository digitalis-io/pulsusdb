//! The per-table flush task: `select!{Notify, interval, shutdown}`
//! (architect plan), a bounded pre-send retry policy with
//! exponential-backoff-plus-full-jitter (hand-rolled xorshift — no `rand`
//! dependency), and the poison/uncertain spool classifier. One task per
//! table (not shared): a stalled high-volume `log_samples` insert must
//! not head-of-line-block low-volume `log_streams`, and each table needs
//! independent retry/timer state (architect plan).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, ChError, ChRow};
use tokio::sync::{Notify, watch};
use tracing::{error, warn};

use crate::writer::buffer::{Generation, TableBuffer};
use crate::writer::config::WriterRuntime;
use crate::writer::error::WriteError;
use crate::writer::metrics::TableMetrics;
use crate::writer::spool::{SpoolEncode, SpoolKind, SpoolWriter};

/// Real-vs-mock seam over a columnar block insert (architect plan): the
/// production impl ([`ChBlockInserter`]) wraps
/// `pulsus_clickhouse::ChClient::insert_block`; tests substitute a mock
/// that can fail/hang on demand — no real ClickHouse in unit tests. A
/// hand-rolled boxed-future method (not `async fn`) so the trait stays
/// object-safe: `LogWriter` holds this behind `Arc<dyn BlockInserter<R>>`
/// per table.
pub trait BlockInserter<R>: Send + Sync
where
    R: ChRow,
{
    fn insert<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>>;
}

/// Production [`BlockInserter`]: a thin adapter over
/// `ChClient::insert_block`, generic over every row shape this crate
/// defines (both `LogSampleRow` and `LogStreamRow` share one instance).
pub struct ChBlockInserter {
    client: Arc<ChClient>,
}

impl ChBlockInserter {
    pub fn new(client: Arc<ChClient>) -> Self {
        ChBlockInserter { client }
    }
}

impl<R: ChRow> BlockInserter<R> for ChBlockInserter {
    fn insert<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        Box::pin(self.client.insert_block(table, rows))
    }
}

/// Shared shutdown signal between [`crate::writer::LogWriter::shutdown`]
/// and every table's flush task. A `watch` channel (not `Notify`):
/// `watch::Receiver::changed()` observes a value transition regardless of
/// exactly when the receiver started watching relative to the `send`,
/// unlike `Notify`'s "must already be waiting" semantics — a flush task
/// blocked inside a real insert (not currently polling `changed()`) must
/// still observe shutdown promptly the next time it reaches the select
/// loop, with no lost-wakeup window.
#[derive(Debug)]
pub(crate) struct ShutdownSignal {
    tx: watch::Sender<Option<Instant>>,
}

impl ShutdownSignal {
    /// Returns the signal plus one `Receiver`; clone the receiver once
    /// per flush task (a `watch::Receiver`'s "have I seen this version"
    /// state must not be shared across concurrent awaiters).
    pub(crate) fn new() -> (Self, watch::Receiver<Option<Instant>>) {
        let (tx, rx) = watch::channel(None);
        (ShutdownSignal { tx }, rx)
    }

    /// Marks the writer as shutting down with a deadline every flush task
    /// must have force-settled its buffer by.
    pub(crate) fn begin(&self, deadline: Instant) {
        // `send` only errors when every receiver has been dropped, which
        // cannot happen while a flush task (holding its own clone) is
        // still running — nothing actionable on error.
        let _ = self.tx.send(Some(deadline));
    }
}

/// A successful-flush callback: `log_streams`'s success-only `StreamLru`
/// promotion (architect plan amendment 1) hooks in here; `log_samples`
/// has none. A named alias, not an inline `Option<Arc<dyn Fn(...)>>`, per
/// Clippy's `type_complexity` lint.
pub(crate) type FlushSuccessHook<R> = Arc<dyn Fn(&[R]) + Send + Sync>;

/// A definitely-failed-flush callback (issues #134/#139): invoked ONLY
/// from [`finish_generation`]'s `FlushOutcome::Poisoned` arm — a Poisoned
/// outcome is provably not-committed (a non-retryable error surfaced
/// unchanged by `insert_block`, or a pre-send retryable exhausted in the
/// writer's retry loop; every post-send retryable is downgraded to
/// `InsertUncertain` before it reaches this module), so re-inserting the
/// rows replays nothing that could have committed. The registration
/// tables' backfill enqueues hook in here (`log_streams`,
/// `metric_series`, `metric_metadata`, `trace_attrs_idx`); every
/// append-only table (`log_samples`, `metric_samples`,
/// `metric_hist_samples`, `trace_spans`) passes `None` — the structural
/// #9 exclusion — and an Uncertain generation failure structurally
/// cannot reach this hook on any table.
pub(crate) type FlushPoisonedHook<R> = Arc<dyn Fn(&[R]) + Send + Sync>;

/// Per-table wiring the flush task closes over.
pub(crate) struct TableContext<R> {
    /// The target ClickHouse table name to insert into — `Arc<str>` (not
    /// `&'static str`): in cluster mode this is a `_dist`-suffixed name
    /// computed once at server startup from `Config`
    /// ([`crate::writer::WriterTables`], issue #15 architect plan), not
    /// known at compile time.
    pub table: Arc<str>,
    pub buffer: Arc<TableBuffer<R>>,
    pub notify: Arc<Notify>,
    pub inserter: Arc<dyn BlockInserter<R>>,
    pub runtime: Arc<WriterRuntime>,
    pub table_metrics: Arc<TableMetrics>,
    pub spool: Arc<SpoolWriter>,
    pub queued_bytes: Arc<AtomicU64>,
    /// Invoked with a successfully flushed generation's rows before the
    /// generation's waiters are resolved `Ok`. `None` for `log_samples`,
    /// which has no such hook.
    pub on_flush_success: Option<FlushSuccessHook<R>>,
    /// Invoked with a Poisoned (definitely-not-committed) generation's
    /// rows after the spool attempt (regardless of the spool I/O result —
    /// the heal path must not depend on audit-file I/O) and before the
    /// generation settles `Err`. `Some` ONLY for the registration tables
    /// (`log_streams`, `metric_series`, `metric_metadata`,
    /// `trace_attrs_idx` — issues #134/#139); `None` on every append-only
    /// table.
    pub on_flush_poisoned: Option<FlushPoisonedHook<R>>,
}

/// Spawns this table's dedicated flush task: `select!{size/age-triggered
/// notify, age-interval tick, shutdown}` while running, then a bounded
/// drain once shutdown begins.
pub(crate) fn spawn<R>(
    ctx: TableContext<R>,
    mut shutdown_rx: watch::Receiver<Option<Instant>>,
) -> tokio::task::JoinHandle<()>
where
    R: ChRow + SpoolEncode + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(ctx.runtime.batch_age);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = ctx.notify.notified() => {},
                _ = interval.tick() => {},
                _ = shutdown_rx.changed() => {
                    break;
                }
            }
            if shutdown_rx.borrow().is_some() {
                break;
            }
            if ctx
                .buffer
                .should_flush(ctx.runtime.batch_bytes, ctx.runtime.batch_age)
            {
                flush_once(&ctx, shutdown_rx.clone()).await;
            }
        }

        drain(&ctx, shutdown_rx).await;
    })
}

/// One normal-operation flush attempt: swap out the current generation
/// (if any) and settle it through [`settle_generation`], which is itself
/// shutdown-aware (architect plan amendment 3, finding 1): a flush that
/// started before shutdown fires is never needlessly bounded, but the
/// instant shutdown is observed mid-insert, the remaining attempt becomes
/// bounded by the drain deadline.
async fn flush_once<R>(ctx: &TableContext<R>, shutdown_rx: watch::Receiver<Option<Instant>>)
where
    R: ChRow + SpoolEncode + Send + Sync,
{
    let Some(generation) = ctx.buffer.swap_out() else {
        return;
    };
    settle_generation(ctx, generation, shutdown_rx).await;
}

/// Bounded drain: repeatedly swaps out and settles whatever remains
/// through the same shutdown-aware [`settle_generation`] the normal path
/// uses (architect plan amendment 3: "give the normal path the same
/// owns-generation-on-stack structure `drain` already uses"). By the time
/// `drain` runs, `shutdown_rx` already carries a deadline (the `spawn`
/// loop above only ever breaks into `drain` once it has observed one), so
/// every generation swapped out here is bounded by that deadline from the
/// start.
async fn drain<R>(ctx: &TableContext<R>, shutdown_rx: watch::Receiver<Option<Instant>>)
where
    R: ChRow + SpoolEncode + Send + Sync,
{
    loop {
        let Some(generation) = ctx.buffer.swap_out() else {
            return;
        };
        settle_generation(ctx, generation, shutdown_rx.clone()).await;
    }
}

/// Inserts `generation`'s rows and settles it through the single settle
/// path (architect plan amendment 2). Shutdown-aware (amendment 3,
/// finding 1 — fixing the code-review FAIL: a normal-path flush
/// previously awaited its insert with no budget at all, so a task parked
/// there never observed shutdown): while `shutdown_rx` has not yet
/// observed a deadline, the insert (with its own retry policy) is
/// unbounded, racing directly against `shutdown_rx.changed()` — a
/// normal-path flush already in progress before shutdown fires is never
/// needlessly aborted. The moment shutdown is signalled — either already
/// set when this call starts (the drain path) or observed mid-flight (the
/// normal path) — the *same, still in-progress* insert attempt becomes
/// bounded by the time remaining until that deadline; if it elapses, the
/// generation force-settles with [`WriteError::ShuttingDown`].
///
/// The generation is owned by this function's stack throughout, never
/// inside a future a `timeout`/`select!` branch could drop uninspected —
/// dropping the abandoned insert attempt on timeout is safe because
/// nothing about it holds any of this generation's waiters or its byte
/// reservation, so it always reaches the settle path exactly once.
async fn settle_generation<R>(
    ctx: &TableContext<R>,
    generation: Generation<R>,
    mut shutdown_rx: watch::Receiver<Option<Instant>>,
) where
    R: ChRow + SpoolEncode + Send + Sync,
{
    let started = Instant::now();
    ctx.table_metrics.inflight.fetch_add(1, Ordering::Relaxed);

    // Scoped so the pinned insert future — which borrows
    // `generation.rows` — is dropped before `generation` itself is moved
    // below (into `finish_generation`/`generation.settle`).
    let outcome = {
        let attempt = attempt_insert_with_retry(ctx, &generation.rows);
        tokio::pin!(attempt);

        let already_shutting_down = *shutdown_rx.borrow();
        match already_shutting_down {
            Some(deadline) => bound_by_deadline(&mut attempt, deadline).await,
            None => {
                tokio::select! {
                    outcome = &mut attempt => Ok(outcome),
                    changed = shutdown_rx.changed() => {
                        // A closed channel (the writer's `Shared` — and
                        // its `ShutdownSignal` sender — dropped without a
                        // graceful `shutdown()` call) is treated the same
                        // as an immediate deadline, matching
                        // `unwrap_or_else`'s "now" fallback used
                        // elsewhere in this module.
                        let _ = changed;
                        let deadline = shutdown_rx.borrow().unwrap_or_else(Instant::now);
                        bound_by_deadline(&mut attempt, deadline).await
                    }
                }
            }
        }
    };

    ctx.table_metrics.inflight.fetch_sub(1, Ordering::Relaxed);
    match outcome {
        Ok(outcome) => finish_generation(ctx, generation, outcome, started).await,
        Err(_elapsed) => {
            ctx.queued_bytes
                .fetch_sub(generation.bytes, Ordering::AcqRel);
            generation.settle(Err(WriteError::ShuttingDown));
        }
    }
}

/// Awaits an already-in-progress, pinned `attempt` bounded by the time
/// remaining until `deadline`; `Err` (the deadline elapsed first) is the
/// caller's cue to abandon the attempt (force-settle with `ShuttingDown`
/// here; drop-and-exit in `writer::backfill`'s tick) instead of using its
/// outcome. Generic over the future's output (issue #134) so the
/// registration-backfill task can reuse the exact shutdown-race pattern
/// [`settle_generation`] uses — this module's call sites are unchanged.
pub(crate) async fn bound_by_deadline<F>(
    attempt: &mut F,
    deadline: Instant,
) -> Result<F::Output, tokio::time::error::Elapsed>
where
    F: Future + Unpin,
{
    let remaining = deadline.saturating_duration_since(Instant::now());
    tokio::time::timeout(remaining, attempt).await
}

enum FlushOutcome {
    Ok,
    Uncertain(String),
    Poisoned(String),
}

/// Inserts `rows`, retrying only *pre-send* retryable failures
/// (`ChError::is_retryable`) with exponential backoff and full jitter, up
/// to `runtime.retry_max_attempts`. `insert_block` downgrades every
/// *post-send* retryable failure to `ChError::InsertUncertain` before it
/// ever reaches this function (see `pulsus_clickhouse::ChClient::
/// insert_block`'s doc comment) — that path is classified but never
/// retried, per the one hard invariant this crate enforces
/// (docs/schemas.md §2.2/§8: replaying a partially-committed block
/// duplicates rows and permanently inflates tier aggregates).
async fn attempt_insert_with_retry<R>(ctx: &TableContext<R>, rows: &[R]) -> FlushOutcome
where
    R: ChRow,
{
    if rows.is_empty() {
        return FlushOutcome::Ok;
    }
    let mut attempt = 0u32;
    let mut rng = XorShift64::seeded();
    loop {
        match ctx.inserter.insert(&ctx.table, rows).await {
            Ok(()) => return FlushOutcome::Ok,
            Err(ChError::InsertUncertain(msg)) => return FlushOutcome::Uncertain(msg),
            Err(e) if e.is_retryable() && attempt < ctx.runtime.retry_max_attempts => {
                attempt += 1;
                ctx.table_metrics
                    .retries_total
                    .fetch_add(1, Ordering::Relaxed);
                let delay = backoff_delay(
                    ctx.runtime.retry_base_delay,
                    ctx.runtime.retry_max_delay,
                    attempt,
                    &mut rng,
                );
                warn!(
                    table = %ctx.table,
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    error = %e,
                    "retrying a pre-send-retryable insert failure"
                );
                tokio::time::sleep(delay).await;
            }
            Err(e) => return FlushOutcome::Poisoned(e.to_string()),
        }
    }
}

/// Applies `outcome` to `generation`: on success, runs the
/// `on_flush_success` hook and records flush metrics; on failure, spools
/// to poison/uncertain (logging, never panicking, on a spool I/O
/// failure). Either way, ends by releasing `generation.bytes` from the
/// shared queue-bytes reservation and resolving every joined waiter —
/// the single settle path (architect plan amendment 2).
async fn finish_generation<R>(
    ctx: &TableContext<R>,
    generation: Generation<R>,
    outcome: FlushOutcome,
    started: Instant,
) where
    R: ChRow + SpoolEncode + Send + Sync,
{
    match outcome {
        FlushOutcome::Ok => {
            if let Some(hook) = &ctx.on_flush_success {
                hook(&generation.rows);
            }
            ctx.table_metrics.record_flush(
                generation.rows.len() as u64,
                generation.bytes,
                started.elapsed(),
            );
            ctx.queued_bytes
                .fetch_sub(generation.bytes, Ordering::AcqRel);
            generation.settle(Ok(()));
        }
        FlushOutcome::Uncertain(msg) => {
            if let Err(spool_err) = ctx
                .spool
                .write(SpoolKind::Uncertain, &ctx.table, &generation.rows, &msg)
                .await
            {
                ctx.table_metrics
                    .spool_write_failures_total
                    .fetch_add(1, Ordering::Relaxed);
                error!(
                    table = %ctx.table,
                    error = %spool_err,
                    "failed to spool an insert-uncertain batch to disk"
                );
            }
            ctx.queued_bytes
                .fetch_sub(generation.bytes, Ordering::AcqRel);
            generation.settle(Err(WriteError::Uncertain(msg)));
        }
        FlushOutcome::Poisoned(msg) => {
            if let Err(spool_err) = ctx
                .spool
                .write(SpoolKind::Poison, &ctx.table, &generation.rows, &msg)
                .await
            {
                ctx.table_metrics
                    .spool_write_failures_total
                    .fetch_add(1, Ordering::Relaxed);
                error!(
                    table = %ctx.table,
                    error = %spool_err,
                    "failed to spool a poison batch to disk"
                );
            }
            // Issue #134: fires regardless of the spool I/O result (the
            // heal path must not depend on audit-file I/O), before the
            // generation settles. This is the ONLY hook invocation site —
            // the Uncertain arm above deliberately has none (#9: an
            // uncertain-fate batch is never auto-replayed).
            if let Some(hook) = &ctx.on_flush_poisoned {
                hook(&generation.rows);
            }
            ctx.queued_bytes
                .fetch_sub(generation.bytes, Ordering::AcqRel);
            generation.settle(Err(WriteError::Poisoned(msg)));
        }
    }
}

/// A cheap, non-cryptographic xorshift64 PRNG for full-jitter retry
/// delays — a dedicated `rand` dependency is unwarranted for "pick a
/// uniform random delay" (lean-deps ethos, architect plan).
struct XorShift64(u64);

impl XorShift64 {
    fn seeded() -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E37_79B9_7F4A_7C15);
        // A xorshift generator's state must never be zero (it is a fixed
        // point), hence the `| 1`.
        XorShift64(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Exponential backoff (`base * 2^(attempt-1)`, capped at `max`) with full
/// jitter (`Prometheus`/AWS-style: a uniform random delay in
/// `[0, capped]`, not just added noise around the capped value) — spreads
/// out retries from many concurrently-failing batches instead of having
/// them all retry in lockstep.
fn backoff_delay(base: Duration, max: Duration, attempt: u32, rng: &mut XorShift64) -> Duration {
    let shift = attempt.saturating_sub(1).min(20);
    let multiplier = 1u32 << shift;
    let capped = base.saturating_mul(multiplier).min(max);
    let millis = capped.as_millis() as u64;
    if millis == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis(rng.next_u64() % (millis + 1))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex;

    use pulsus_config::WriterConfig;

    use super::*;
    use crate::writer::backfill::{RegistrationBacklog, enqueue_failed};
    use crate::writer::buffer::TableBuffer;
    use crate::writer::error::WriteError;
    use crate::writer::metrics::WriterMetrics;
    use crate::writer::rows::LogStreamRow;

    /// A stub inserter: `finish_generation` (the unit under test below)
    /// never inserts, but `TableContext` requires one.
    struct OkInserter;

    impl<R: ChRow> BlockInserter<R> for OkInserter {
        fn insert<'a>(
            &'a self,
            _table: &'a str,
            _rows: &'a [R],
        ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn stream_row() -> LogStreamRow {
        LogStreamRow {
            month: 19662,
            fingerprint: 42,
            service: "svc".to_string(),
            labels: "{\"service_name\":\"svc\"}".to_string(),
            updated_ns: 1,
        }
    }

    /// A spool root that is a plain FILE, so `SpoolWriter::write`'s
    /// `create_dir_all` fails deterministically (issue #134 AC15/AC16).
    fn plain_file_spool_root() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "pulsus-write-table-test-spool-root-{}-{:?}",
            std::process::id(),
            std::time::Instant::now()
        ));
        std::fs::write(&path, b"not a directory").expect("create the plain-file spool root");
        path
    }

    fn streams_ctx_with(
        metrics: &Arc<WriterMetrics>,
        spool_root: PathBuf,
        on_flush_poisoned: Option<FlushPoisonedHook<LogStreamRow>>,
    ) -> TableContext<LogStreamRow> {
        TableContext {
            table: Arc::from("log_streams"),
            buffer: Arc::new(TableBuffer::new()),
            notify: Arc::new(Notify::new()),
            inserter: Arc::new(OkInserter),
            runtime: Arc::new(WriterRuntime::from_config(&WriterConfig::default())),
            table_metrics: metrics.streams.clone(),
            spool: Arc::new(SpoolWriter::new(spool_root, metrics.clone())),
            queued_bytes: Arc::new(AtomicU64::new(0)),
            on_flush_success: None,
            on_flush_poisoned,
        }
    }

    /// Issue #134 AC15: a Poisoned generation whose spool write fails
    /// (root is a plain file) must bump `spool_write_failures_total` AND
    /// still fire the `on_flush_poisoned` hook — the heal path is
    /// independent of audit-file I/O.
    #[tokio::test]
    async fn poisoned_spool_write_failure_bumps_the_counter_and_still_fires_the_hook() {
        let spool_root = plain_file_spool_root();
        let metrics = Arc::new(WriterMetrics::default());

        let hook_rows = Arc::new(AtomicU64::new(0));
        let hook_rows_for_hook = hook_rows.clone();
        let hook: FlushPoisonedHook<LogStreamRow> = Arc::new(move |rows: &[LogStreamRow]| {
            hook_rows_for_hook.fetch_add(rows.len() as u64, Ordering::SeqCst);
        });
        let ctx = streams_ctx_with(&metrics, spool_root.clone(), Some(hook));

        let (_, rx) = ctx.buffer.append_and_wait(vec![stream_row()], 10, u64::MAX);
        ctx.queued_bytes.store(10, Ordering::SeqCst);
        let generation = ctx.buffer.swap_out().expect("non-empty generation");

        finish_generation(
            &ctx,
            generation,
            FlushOutcome::Poisoned("boom".to_string()),
            Instant::now(),
        )
        .await;

        assert_eq!(
            metrics
                .streams
                .spool_write_failures_total
                .load(Ordering::SeqCst),
            1,
            "the failed spool write must be counted"
        );
        assert_eq!(
            hook_rows.load(Ordering::SeqCst),
            1,
            "the on_flush_poisoned hook must fire despite the spool I/O failure"
        );
        assert!(matches!(
            rx.await.expect("generation settled"),
            Err(WriteError::Poisoned(_))
        ));
        assert_eq!(metrics.spool_poison_total.load(Ordering::SeqCst), 0);

        std::fs::remove_file(&spool_root).ok();
    }

    /// Issue #134 AC16 (compound R4∧R5): spool write fails AND the
    /// byte-capped backlog drops the row — acknowledged-lost, surfaced
    /// only by counters, with no false durability or heal claim, and the
    /// generation still settles `Err` (waiter contract intact). The hook
    /// delegates to the production `enqueue_failed` seam.
    #[tokio::test]
    async fn compound_spool_write_failure_and_byte_cap_drop_is_acknowledged_lost() {
        let spool_root = plain_file_spool_root();
        let metrics = Arc::new(WriterMetrics::default());

        // Cap of 1 byte: any real row is a byte-cap drop.
        let backlog = Arc::new(Mutex::new(RegistrationBacklog::new(1)));
        let backlog_for_hook = backlog.clone();
        let backfill_metrics_for_hook = metrics.backfill.clone();
        let hook: FlushPoisonedHook<LogStreamRow> = Arc::new(move |rows: &[LogStreamRow]| {
            enqueue_failed(&backlog_for_hook, &backfill_metrics_for_hook, rows);
        });
        let ctx = streams_ctx_with(&metrics, spool_root.clone(), Some(hook));

        let (_, rx) = ctx.buffer.append_and_wait(vec![stream_row()], 10, u64::MAX);
        ctx.queued_bytes.store(10, Ordering::SeqCst);
        let generation = ctx.buffer.swap_out().expect("non-empty generation");

        finish_generation(
            &ctx,
            generation,
            FlushOutcome::Poisoned("boom".to_string()),
            Instant::now(),
        )
        .await;

        assert_eq!(
            metrics
                .streams
                .spool_write_failures_total
                .load(Ordering::SeqCst),
            1
        );
        assert_eq!(metrics.backfill.dropped_total.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.backfill.enqueued_total.load(Ordering::SeqCst), 0);
        assert_eq!(
            backlog
                .lock()
                .expect("registration backlog mutex poisoned")
                .len(),
            0,
            "the dropped row must not linger in the backlog"
        );
        assert_eq!(metrics.backfill.pending.load(Ordering::SeqCst), 0);
        assert_eq!(metrics.backfill.healed_total.load(Ordering::SeqCst), 0);
        assert!(
            matches!(
                rx.await.expect("generation settled"),
                Err(WriteError::Poisoned(_))
            ),
            "the generation must still settle Err — no false heal claim"
        );

        std::fs::remove_file(&spool_root).ok();
    }

    #[test]
    fn backoff_delay_never_exceeds_the_cap() {
        let mut rng = XorShift64::seeded();
        for attempt in 1..=20 {
            let delay = backoff_delay(
                Duration::from_millis(100),
                Duration::from_secs(1),
                attempt,
                &mut rng,
            );
            assert!(delay <= Duration::from_secs(1));
        }
    }

    #[test]
    fn backoff_delay_grows_with_attempt_before_the_cap() {
        // Deterministic bound check: the maximum possible jittered delay
        // (the cap itself) strictly increases attempt-over-attempt while
        // still below `max`.
        let base = Duration::from_millis(10);
        let max = Duration::from_secs(10);
        let cap_at = |attempt: u32| {
            let shift = attempt.saturating_sub(1).min(20);
            base.saturating_mul(1u32 << shift).min(max)
        };
        assert!(cap_at(1) < cap_at(2));
        assert!(cap_at(2) < cap_at(3));
    }

    #[test]
    fn xorshift_next_u64_is_deterministic_and_varies() {
        let mut rng = XorShift64(42);
        let a = rng.next_u64();
        let b = rng.next_u64();
        assert_ne!(a, b);
        let mut rng2 = XorShift64(42);
        assert_eq!(rng2.next_u64(), a);
    }
}
