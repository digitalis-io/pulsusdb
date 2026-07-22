//! `TableBuffer<R>`: a generic, mutex-guarded columnar buffer for one
//! destination table. Holds exactly one open "generation" at a time — the
//! rows accumulated since the last flush, plus every sync-mode waiter
//! that joined this generation (architect plan amendment 1: "each buffer
//! generation gets a `Vec<oneshot::Sender>`"). [`TableBuffer::swap_out`]
//! is the only way a generation leaves the buffer; from that point its
//! settlement is the taker's exclusive responsibility (architect plan
//! amendment 2's "single settle path").

use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::writer::error::WriteError;

/// One flush cycle's accumulated rows plus every waiter awaiting this
/// generation's outcome.
pub(crate) struct Generation<R> {
    pub rows: Vec<R>,
    pub bytes: u64,
    waiters: Vec<oneshot::Sender<Result<(), WriteError>>>,
}

impl<R> Generation<R> {
    fn new() -> Self {
        Generation {
            rows: Vec::new(),
            bytes: 0,
            waiters: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Resolves every joined waiter with `result`, consuming the
    /// generation. This is the sole place a generation's waiters are ever
    /// resolved (architect plan amendment 2's "single settle path") —
    /// every `Sender` is consumed via `send` here, on every path
    /// (success, poison/uncertain spool, and forced shutdown
    /// settlement), so a receiver ever observing a dropped sender without
    /// a prior send is unreachable by construction (see
    /// `crate::writer::join_generations`'s `debug_assert!`).
    pub(crate) fn settle(self, result: Result<(), WriteError>) {
        for waiter in self.waiters {
            // The receiver may already be gone (e.g. the sync caller's
            // `FlushWait` future was dropped/cancelled) — `send` returning
            // `Err` just means nobody is listening anymore, not a bug.
            let _ = waiter.send(result.clone());
        }
    }
}

struct Inner<R> {
    current: Generation<R>,
    /// When the current generation's first row was appended — `None`
    /// while the generation is empty. Reset on every `swap_out`.
    oldest: Option<Instant>,
}

pub(crate) struct TableBuffer<R> {
    inner: Mutex<Inner<R>>,
}

impl<R> TableBuffer<R> {
    pub(crate) fn new() -> Self {
        TableBuffer {
            inner: Mutex::new(Inner {
                current: Generation::new(),
                oldest: None,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner<R>> {
        self.inner.lock().expect("table buffer mutex poisoned")
    }

    /// Appends `rows` (contributing `bytes` to this generation's byte
    /// reservation) without registering a waiter — async-mode admission,
    /// which does not need to observe this generation's outcome. Returns
    /// `true` if this append just reached or crossed `max_bytes`; the
    /// caller should `Notify` the flush task on `true`.
    pub(crate) fn append(&self, rows: Vec<R>, bytes: u64, max_bytes: u64) -> bool {
        let mut inner = self.lock();
        Self::append_locked(&mut inner, rows, bytes);
        inner.current.bytes >= max_bytes
    }

    /// As [`Self::append`], but also registers a waiter for this
    /// generation's eventual settlement (sync-mode admission) and returns
    /// its `Receiver` alongside the same size-threshold signal.
    pub(crate) fn append_and_wait(
        &self,
        rows: Vec<R>,
        bytes: u64,
        max_bytes: u64,
    ) -> (bool, oneshot::Receiver<Result<(), WriteError>>) {
        let mut inner = self.lock();
        Self::append_locked(&mut inner, rows, bytes);
        let (tx, rx) = oneshot::channel();
        inner.current.waiters.push(tx);
        (inner.current.bytes >= max_bytes, rx)
    }

    fn append_locked(inner: &mut Inner<R>, rows: Vec<R>, bytes: u64) {
        if inner.current.is_empty() && inner.oldest.is_none() {
            inner.oldest = Some(Instant::now());
        }
        inner.current.rows.extend(rows);
        inner.current.bytes += bytes;
    }

    /// `true` when the current generation should flush: at/over
    /// `max_bytes`, or non-empty and older than `max_age`.
    pub(crate) fn should_flush(&self, max_bytes: u64, max_age: Duration) -> bool {
        let inner = self.lock();
        if inner.current.is_empty() {
            return false;
        }
        inner.current.bytes >= max_bytes || inner.oldest.is_some_and(|t| t.elapsed() >= max_age)
    }

    /// Takes the current generation (if non-empty), leaving a fresh empty
    /// one in its place.
    pub(crate) fn swap_out(&self) -> Option<Generation<R>> {
        let mut inner = self.lock();
        if inner.current.is_empty() {
            return None;
        }
        inner.oldest = None;
        Some(std::mem::replace(&mut inner.current, Generation::new()))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn append_accumulates_rows_and_bytes() {
        let buf: TableBuffer<u32> = TableBuffer::new();
        buf.append(vec![1, 2], 10, 1_000);
        buf.append(vec![3], 5, 1_000);
        let generation = buf.swap_out().expect("non-empty generation");
        assert_eq!(generation.rows, vec![1, 2, 3]);
        assert_eq!(generation.bytes, 15);
    }

    #[test]
    fn append_reports_crossing_the_size_threshold() {
        let buf: TableBuffer<u32> = TableBuffer::new();
        assert!(!buf.append(vec![1], 5, 10));
        assert!(buf.append(vec![2], 5, 10));
    }

    #[test]
    fn should_flush_is_false_for_an_empty_buffer() {
        let buf: TableBuffer<u32> = TableBuffer::new();
        assert!(!buf.should_flush(1, Duration::from_secs(3600)));
    }

    #[test]
    fn should_flush_is_true_once_bytes_reach_the_threshold() {
        let buf: TableBuffer<u32> = TableBuffer::new();
        buf.append(vec![1], 100, 100);
        assert!(buf.should_flush(100, Duration::from_secs(3600)));
    }

    #[test]
    fn should_flush_is_true_once_the_generation_is_older_than_max_age() {
        let buf: TableBuffer<u32> = TableBuffer::new();
        buf.append(vec![1], 1, u64::MAX);
        assert!(buf.should_flush(u64::MAX, Duration::from_millis(0)));
    }

    /// Issue #133 (plan v6 delta 2): the byte-flush trigger still fires
    /// at the maximum config-accepted `writer.batch_bytes` — synthetic
    /// buffered-byte COUNTERS only (the `bytes` argument is bookkeeping;
    /// no 16 GiB is allocated), age held below its own trigger. Holds
    /// one byte under the ceiling, fires at it.
    #[test]
    fn byte_flush_still_fires_at_the_max_accepted_batch_bytes() {
        let cap = pulsus_config::BATCH_BYTES_CEILING;
        let no_age = Duration::from_secs(3600);
        let buf: TableBuffer<u32> = TableBuffer::new();
        buf.append(vec![1], cap - 1, u64::MAX);
        assert!(
            !buf.should_flush(cap, no_age),
            "one byte under the ceiling must hold"
        );
        buf.append(vec![2], 1, u64::MAX);
        assert!(
            buf.should_flush(cap, no_age),
            "the flush decision must fire at the accepted maximum"
        );
    }

    /// Issue #133 (plan v6 delta 2): the age-flush trigger still fires at
    /// the maximum config-accepted `writer.batch_ms` (200_000 ms) — a
    /// SYNTHETIC generation birth instant (no real waiting), bytes held
    /// below their own trigger. The hold leg sits half the ceiling under
    /// the trigger so a scheduler stall can never flake it; the fire leg
    /// is deterministic (elapsed time only grows past the threshold).
    #[test]
    fn age_flush_still_fires_at_the_max_accepted_batch_ms() {
        let max_age = Duration::from_millis(pulsus_config::BATCH_MS_CEILING);
        let buf: TableBuffer<u32> = TableBuffer::new();
        buf.append(vec![1], 1, u64::MAX);

        // Monotonic history is measured from boot on Linux; the build
        // preceding this test run is already far longer than 200 s.
        let synthetic = |age: Duration| {
            Instant::now()
                .checked_sub(age)
                .expect("monotonic clock history exceeds the synthetic age")
        };
        buf.lock().oldest = Some(synthetic(max_age / 2));
        assert!(
            !buf.should_flush(u64::MAX, max_age),
            "a generation younger than the ceiling must hold"
        );
        buf.lock().oldest = Some(synthetic(max_age));
        assert!(
            buf.should_flush(u64::MAX, max_age),
            "the age flush must fire at the accepted maximum"
        );
    }

    #[test]
    fn swap_out_of_an_empty_buffer_returns_none() {
        let buf: TableBuffer<u32> = TableBuffer::new();
        assert!(buf.swap_out().is_none());
    }

    #[test]
    fn swap_out_leaves_a_fresh_empty_generation_behind() {
        let buf: TableBuffer<u32> = TableBuffer::new();
        buf.append(vec![1], 1, 1_000);
        assert!(buf.swap_out().is_some());
        assert!(buf.swap_out().is_none());
    }

    #[tokio::test]
    async fn settle_resolves_every_joined_waiter_with_the_same_result() {
        let buf: TableBuffer<u32> = TableBuffer::new();
        let (_, rx1) = buf.append_and_wait(vec![1], 1, 1_000);
        let (_, rx2) = buf.append_and_wait(vec![2], 1, 1_000);
        let generation = buf.swap_out().expect("non-empty generation");
        generation.settle(Ok(()));
        assert_eq!(rx1.await.unwrap(), Ok(()));
        assert_eq!(rx2.await.unwrap(), Ok(()));
    }

    #[tokio::test]
    async fn settle_with_an_error_resolves_every_waiter_to_that_error() {
        let buf: TableBuffer<u32> = TableBuffer::new();
        let (_, rx) = buf.append_and_wait(vec![1], 1, 1_000);
        let generation = buf.swap_out().expect("non-empty generation");
        generation.settle(Err(WriteError::ShuttingDown));
        assert_eq!(rx.await.unwrap(), Err(WriteError::ShuttingDown));
    }

    #[test]
    fn dropping_a_waiters_receiver_does_not_panic_on_settle() {
        let buf: TableBuffer<u32> = TableBuffer::new();
        let (_, rx) = buf.append_and_wait(vec![1], 1, 1_000);
        drop(rx);
        let generation = buf.swap_out().expect("non-empty generation");
        generation.settle(Ok(()));
    }
}
