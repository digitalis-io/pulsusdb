//! The seam between the OTLP logs parser (`crate::protocols::otlp_logs`)
//! and the writer core (issue #9, not built here): [`LogSink`] plus the
//! types an admitted batch's caller needs. `pulsus-write` stops at
//! admission — no batching, flush scheduling, or ClickHouse writes live on
//! this side (architect plan, "out of scope").

pub mod decompress;
pub mod http;
pub mod metrics;
pub mod traces;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::LogsIngestError;
use crate::protocols::otlp_logs::ParsedLogs;

/// Returned by [`LogSink::admit`]/[`LogSink::admit_flush`] when the sink's
/// buffers are full: the writer core is applying backpressure rather than
/// growing an unbounded queue (docs/architecture.md §4). Maps to HTTP 429
/// / `google.rpc.Status.code = 8` (`RESOURCE_EXHAUSTED`) at the handler
/// (architect plan amendment 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backpressure;

/// The two request headers a client may set, read by the four log/metric
/// handlers and handed to the sink verbatim. Traces read neither.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PushHeaders {
    /// `Idempotency-Key`: when present, its bytes **namespace** the
    /// identity — they do not override the content. Two requests with
    /// different keys are two pushes whatever they carry; the same key with
    /// different content is a client error.
    pub idempotency_key: Option<String>,
    /// `Retry-Attempt: n` with `n >= 1`. Never a suppression key — there is
    /// nothing stored to match a bare marker against, and suppressing on one
    /// would drop a push we have no record of accepting. It sets the
    /// `declared` label on the duplicate counter and nothing else.
    pub declared_retry: bool,
}

impl PushHeaders {
    /// `true` when `Retry-Attempt` named an attempt number of 1 or more.
    fn parse_retry_attempt(value: &str) -> bool {
        value.trim().parse::<u64>().is_ok_and(|n| n >= 1)
    }

    /// Builds the pair from raw header values, accepting only the forms a
    /// client actually sends. Used by the handlers; kept here so the two
    /// header names have one definition.
    pub fn from_values(idempotency_key: Option<&str>, retry_attempt: Option<&str>) -> Self {
        PushHeaders {
            idempotency_key: idempotency_key
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_owned),
            declared_retry: retry_attempt.is_some_and(Self::parse_retry_attempt),
        }
    }
}

/// Issue #494: why an admission stored nothing. Widens the log and metric
/// sinks' `Err` from the bare [`Backpressure`] they used to return, because
/// suppression adds two refusals a handler must answer differently: the
/// index's own byte bound (a `429` with its own counter, not the queue's)
/// and a reused `Idempotency-Key` (a client error, `400`). The trace sink
/// is untouched and still returns [`Backpressure`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitRefusal {
    /// The sink's buffers are full, or it is shutting down. `429`.
    Backpressure,
    /// The suppression index's claim table is at its byte bound with
    /// nothing evictable. `429`, nothing stored, and the client's retry is
    /// answered normally once the table drains.
    DedupShed,
    /// The suppression index's waiter registry is at its byte bound, so a
    /// suppressed sync caller cannot be registered. `429`, nothing stored;
    /// the retry finds the claim terminal and gets the original's outcome.
    DedupWaitShed,
    /// The same `Idempotency-Key` arrived carrying different content. A
    /// client error (`400`), never a silent suppression.
    KeyReused,
}

impl From<Backpressure> for AdmitRefusal {
    fn from(_: Backpressure) -> Self {
        AdmitRefusal::Backpressure
    }
}

/// The message a reused `Idempotency-Key` is answered with, on every
/// log/metric transport.
pub const KEY_REUSED_MESSAGE: &str =
    "Idempotency-Key was already used by this writer for different request content";

/// A handle a sync-mode request (`X-Pulsus-Async` absent or `0`,
/// docs/api.md "Request headers") `.await`s until its admitted batch has
/// been durably flushed by the writer core. A boxed, type-erased future —
/// not a concrete channel type — because *how* the (issue #9) writer core
/// signals completion (channel, polling a queue position, ...) is not this
/// issue's design surface; this crate defines only the seam the handler
/// awaits.
pub struct FlushWait(Pin<Box<dyn Future<Output = Result<(), LogsIngestError>> + Send>>);

impl FlushWait {
    /// Wraps any `Send` future that resolves once the admitted batch is
    /// confirmed durable (`Ok`) or has failed (`Err`) as a `FlushWait`.
    pub fn new(fut: impl Future<Output = Result<(), LogsIngestError>> + Send + 'static) -> Self {
        FlushWait(Box::pin(fut))
    }
}

// `Future` trait objects cannot derive `Debug`; hand-implemented per the
// project's "derive Debug on all types, implement manually when it can't
// be derived" convention.
impl fmt::Debug for FlushWait {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlushWait").finish_non_exhaustive()
    }
}

impl Future for FlushWait {
    type Output = Result<(), LogsIngestError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

/// The boundary the OTLP logs handler (`crate::ingest::http::logs`) hands
/// parsed batches across: admission only, no batching/flush/ClickHouse-
/// write logic lives on this side (issue #9's domain). `Send + Sync`
/// because the axum handler holds an implementor behind
/// `axum::extract::State`, shared across concurrently-handled requests.
pub trait LogSink: Send + Sync {
    /// Admits `batch` for async-mode requests (`X-Pulsus-Async: 1`,
    /// docs/api.md): the handler responds `202` as soon as this returns
    /// `Ok`, without waiting for the batch to be flushed.
    ///
    /// `push` carries the request's `Idempotency-Key`/`Retry-Attempt`
    /// headers (issue #494). `Ok(())` does not promise rows were buffered:
    /// a content-identical push inside the suppression window stores
    /// nothing and is answered exactly as the original was.
    fn admit(&self, batch: ParsedLogs, push: PushHeaders) -> Result<(), AdmitRefusal>;

    /// Admits `batch` for sync-mode requests (`X-Pulsus-Async` absent or
    /// `0`, the default): the handler `.await`s the returned
    /// [`FlushWait`] and only then responds `200`. For a suppressed push
    /// the returned wait resolves to **the original push's** outcome.
    fn admit_flush(
        &self,
        batch: ParsedLogs,
        push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn flush_wait_resolves_to_the_wrapped_futures_output() {
        let wait = FlushWait::new(async { Ok(()) });
        assert!(wait.await.is_ok());

        let wait = FlushWait::new(async { Err(LogsIngestError::FlushFailed("boom".to_string())) });
        assert!(matches!(wait.await, Err(LogsIngestError::FlushFailed(_))));
    }

    #[test]
    fn flush_wait_debug_does_not_panic() {
        let wait = FlushWait::new(async { Ok(()) });
        assert!(format!("{wait:?}").contains("FlushWait"));
    }
}
