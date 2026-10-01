//! The writer cores: one per signal, each implementing that signal's
//! ingest seam. This module root holds what they share.
//!
//! **One push is one `INSERT` of one block into one landing table** for logs
//! (`writer::log`, issue #603) and for metrics (`writer::metric`), with
//! materialized views maintaining the tables queries read. `writer::landing`
//! is the block, the insert loop, the audit copy and the one release point
//! both use. Traces (`writer::trace`) still batch per table through
//! `writer::buffer` and `writer::table`.
//!
//! **Registration backfill** (issue #139): a registration flush that fails
//! *definitely* (`Poisoned` — provably not-committed) enqueues its rows into a
//! bounded in-memory backlog (`writer::backfill`) that a dedicated task
//! re-inserts every `WriterRuntime::backfill_retry_interval` (5s) until
//! confirmed, capped at `backfill_max_bytes` (32 MiB) — so a "spans committed,
//! registration lost" orphan self-heals while the process lives. **One
//! backlog is left**, `trace_attrs_idx`'s. Neither landing path has one
//! (issue #603): a registration row rides the events' own block and the
//! registration cache is promoted only when that block commits, so a push
//! whose insert failed emits its registration rows again next time rather than
//! leaving an orphan to heal. Its residuals, which apply to the one remaining
//! backlog:
//! - R1: an uncertain-fate registration whose spans committed is not healed;
//!   an `uncertain/` audit record exists iff that batch's spool write
//!   succeeded (audit-only per #9, never replayed).
//! - R2: a backfill re-insert returning `InsertUncertain` is terminally
//!   abandoned (counted + warn-logged) — same residual class as R1.
//! - R3: the backlog is memory-only — an orphan persists across a crash only
//!   if the attribute also never arrives again in that window.
//! - R4: byte-cap drops under sustained failure are counted
//!   (`backfill_dropped_total`); a poison-spool record for the dropped batch
//!   exists iff that batch's spool write succeeded.
//! - R5: on spool-write failure there is no durable record — the in-memory
//!   backlog is the only (best-effort) remedy; surfaced via the error log +
//!   `spool_write_failures_total`.
//! - R4∧R5 compound: a byte-cap-dropped entry whose generation's spool write
//!   also failed is lost entirely (counters + logs are the sole evidence) —
//!   acknowledged-lost by design, never claimed healed.
//!
//! **Backpressure**: `queued_bytes` is reserved atomically at admission
//! ([`reserve_queued_bytes`] — `fetch_add` first, roll back on overflow) and
//! released exactly once. On either landing path that release is
//! `writer::landing`'s `LandingBlock::release`, which owns the order; on the
//! traces path it is `writer::table`'s settle path.

mod backfill;
mod buffer;
mod config;
mod drain;
mod error;
mod landing;
mod log;
mod metric;
mod metrics;
mod push_dedup;
mod registration;
pub(crate) mod rows;
mod spool;
mod table;
mod trace;
pub(crate) mod trace_json;

use std::sync::atomic::{AtomicU64, Ordering};

use futures::future::try_join_all;
use tokio::sync::oneshot;

pub use config::WriterRuntime;
pub use error::WriteError;
pub use landing::{landing_block_overhead_bytes, landing_charge};
pub use log::{LogWriter, WriterTables};
pub use metric::{MetricWriter, MetricWriterTables};
pub use metrics::{
    BackfillMetricsSnapshot, MetricWriterMetrics, MetricWriterMetricsSnapshot,
    TableMetricsSnapshot, TraceWriterMetrics, TraceWriterMetricsSnapshot, WriterMetrics,
    WriterMetricsSnapshot,
};
pub use push_dedup::{
    Admission, Capacities, ClaimGuard, ClaimOutcome, DedupMetrics, DedupMetricsSnapshot, PushDedup,
    PushDigest, PushIdentity, TargetOutcome, WaitGuard, WaitMode, index_bytes, log_identity,
    metric_identity, plan_capacities,
};
pub use registration::{SeriesLru, StreamLru};
pub use rows::{
    LANDING_ROW_SLOT_BYTES, LOG_LANDING_ROW_SLOT_BYTES, LogLandingRow, LogPatternRow, LogSampleRow,
    LogStreamRow, MetricHistSampleRow, MetricLandingRow, MetricMetadataRow, MetricSampleRow,
    MetricSeriesRow, TRACE_LANDING_ROW_SLOT_BYTES, TraceAttrRow, TraceEventTuple, TraceLandingRow,
    TraceLinkTuple, TraceSpanRow,
};
pub use spool::SPOOL_CHUNK_BYTES;
pub use table::{BlockInserter, ChBlockInserter};
pub use trace::{TraceWriter, TraceWriterTables};
pub use trace_json::{
    TraceJson, TraceJsonEntry, TraceJsonScalar, TraceJsonValue, escape_json_path,
};

use crate::error::LogsIngestError;
use crate::ingest::Backpressure;

/// The atomic queue-bytes admission reservation shared by the log,
/// metric, and trace writers (issue #133 extraction — behavior-identical
/// to the three inline blocks it replaced): reserve first, roll back and
/// count a backpressure rejection on overflow. The counter may
/// transiently over-reserve under concurrent admits (a race loser
/// subtracts back) but never under-reserves, so the memory bound holds.
/// Extracted so the 503 backpressure guard is provable at the maximum
/// config-accepted `writer.ingest_queue_bytes`
/// (`pulsus_config::INGEST_QUEUE_BYTES_CEILING`) with synthetic
/// counters — no real queue is allocated.
pub(crate) fn reserve_queued_bytes(
    queued_bytes: &AtomicU64,
    backpressure_total: &AtomicU64,
    total_bytes: u64,
    queue_bytes_limit: u64,
) -> Result<(), Backpressure> {
    let previous = queued_bytes.fetch_add(total_bytes, Ordering::AcqRel);
    if previous + total_bytes > queue_bytes_limit {
        queued_bytes.fetch_sub(total_bytes, Ordering::AcqRel);
        backpressure_total.fetch_add(1, Ordering::Relaxed);
        return Err(Backpressure);
    }
    Ok(())
}

/// Which admission mode a request asked for (`X-Pulsus-Async`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdmitMode {
    /// Buffer and return; the handler answers `202`.
    Async,
    /// Buffer and wait for the flush; the handler answers `200`/`204`.
    Sync,
}

impl AdmitMode {
    pub(crate) fn wait_mode(self) -> push_dedup::WaitMode {
        match self {
            AdmitMode::Async => push_dedup::WaitMode::None,
            AdmitMode::Sync => push_dedup::WaitMode::Register,
        }
    }
}

/// What an admission did with a request's rows (issue #494).
#[derive(Debug)]
pub(crate) enum Admitted {
    /// Rows were buffered. Empty in async mode, one receiver per touched
    /// generation in sync mode.
    Stored(Vec<oneshot::Receiver<Result<(), WriteError>>>),
    /// This writer already accepted this push and still remembers it, so
    /// nothing was stored and the answer is the original push's.
    Suppressed(Suppressed),
}

/// How a suppressed caller learns the original push's outcome.
#[derive(Debug)]
pub(crate) enum Suppressed {
    /// Already known: the claim had settled, or the caller is async and is
    /// answered without waiting.
    Settled(ClaimOutcome),
    /// The original is still in flight. The guard holds this caller's
    /// registration — and its bytes — for exactly as long as the wait.
    Pending(push_dedup::WaitGuard, oneshot::Receiver<ClaimOutcome>),
}

impl Suppressed {
    /// The answer a suppressed sync caller receives, byte-identical in the
    /// success case to the one this body received when it was fresh.
    pub(crate) async fn into_answer(self) -> Result<(), LogsIngestError> {
        let outcome = match self {
            Suppressed::Settled(outcome) => outcome,
            Suppressed::Pending(guard, rx) => {
                let outcome = rx.await.unwrap_or(ClaimOutcome::Failed);
                // The guard is held across the await deliberately: its drop
                // is what returns this caller's bytes to the waiter budget,
                // and a cancelled future drops it at exactly the right
                // moment.
                drop(guard);
                outcome
            }
        };
        match outcome {
            ClaimOutcome::Ok => Ok(()),
            ClaimOutcome::Failed => Err(LogsIngestError::FlushFailed(
                "the identical push this one repeats did not become durable".to_string(),
            )),
        }
    }
}

/// Records one appended target on a claim, if there is a claim.
pub(crate) fn note_target(guard: &mut Option<ClaimGuard>, counts_for_ack: bool) {
    if let Some(guard) = guard {
        guard.note_target(counts_for_ack);
    }
}

/// Awaits every joined flush-generation receiver, short-circuiting on the
/// first `Err` (architect plan amendment 1: "await all, short-circuiting
/// on first error") via `futures::future::try_join_all`.
pub(crate) async fn join_generations(
    receivers: Vec<oneshot::Receiver<Result<(), WriteError>>>,
) -> Result<(), WriteError> {
    try_join_all(receivers.into_iter().map(|rx| async move {
        match rx.await {
            Ok(result) => result,
            Err(_dropped_sender) => {
                // Unreachable by construction (architect plan amendment
                // 2): every generation resolves all of its waiters
                // (`buffer::Generation::settle`) before it is dropped, on
                // every path including forced shutdown settlement — a
                // sender dropped without a prior `send` would mean some
                // code path forgot to settle a generation.
                debug_assert!(
                    false,
                    "flush generation waiter dropped without settling (violates the \
                     single-settle-path invariant)"
                );
                Err(WriteError::ShuttingDown)
            }
        }
    }))
    .await
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #133 (plan v6 delta 2): the 503 backpressure guard still
    /// fires at the maximum config-accepted `writer.ingest_queue_bytes` —
    /// a reservation landing exactly at the ceiling admits; the next
    /// byte crosses and is rejected LOUDLY (`Err(Backpressure)`, counted
    /// in `backpressure_total`, reservation rolled back). Synthetic
    /// counters only — nothing is allocated; the extracted
    /// [`reserve_queued_bytes`] IS the three writers' admission gate.
    #[test]
    fn queue_reservation_still_fires_at_the_max_accepted_ingest_queue_bytes() {
        let limit = pulsus_config::INGEST_QUEUE_BYTES_CEILING;
        let queued = AtomicU64::new(0);
        let backpressure = AtomicU64::new(0);

        assert!(
            reserve_queued_bytes(&queued, &backpressure, limit, limit).is_ok(),
            "a reservation landing exactly at the ceiling must admit"
        );
        assert_eq!(queued.load(Ordering::Acquire), limit);
        assert_eq!(backpressure.load(Ordering::Relaxed), 0);

        assert!(
            reserve_queued_bytes(&queued, &backpressure, 1, limit).is_err(),
            "the guard must fire at the accepted maximum"
        );
        assert_eq!(
            queued.load(Ordering::Acquire),
            limit,
            "a failed reservation rolls its bytes back"
        );
        assert_eq!(
            backpressure.load(Ordering::Relaxed),
            1,
            "the rejection is counted"
        );
    }
}
