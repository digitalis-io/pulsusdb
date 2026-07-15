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
    fn admit(&self, batch: ParsedLogs) -> Result<(), Backpressure>;

    /// Admits `batch` for sync-mode requests (`X-Pulsus-Async` absent or
    /// `0`, the default): the handler `.await`s the returned
    /// [`FlushWait`] and only then responds `200`.
    fn admit_flush(&self, batch: ParsedLogs) -> Result<FlushWait, Backpressure>;
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
