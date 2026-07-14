//! `POST /v1/logs` server wiring (issue #15 architect plan): [`WriterSink`]
//! adapts the async-filled writer slot (`serve::spawn_reconnect_loop`
//! constructs the real [`LogWriter`] once the ClickHouse pool is ready,
//! *before* `pool_slot` — see that module's doc comment) to `pulsus-write`'s
//! [`LogSink`] seam, and [`ingest_logs`] is the thin `State<AppState>`
//! handler that calls straight into `pulsus_write::ingest`'s state-agnostic
//! core (see that fn's doc comment for why the server cannot reuse
//! `pulsus_write::ingest::http::logs::<S>`'s generic-`State` mount point).

use std::sync::{Arc, OnceLock};

use axum::body::Body;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;

use pulsus_write::{Backpressure, FlushWait, LogSink, LogWriter, ParsedLogs};

use crate::app::AppState;

/// Adapts the writer slot (`Arc<OnceLock<Arc<LogWriter>>>`, filled at most
/// once by the reconnect loop) to [`LogSink`]: delegates to the live
/// writer once it exists, and returns [`Backpressure`] (→ 429; the OTLP
/// collector retries) while the slot is still empty — the same "not ready
/// yet" contract `/ready` gives every other consumer of the pool before
/// the reconnect loop's first successful pass.
pub(crate) struct WriterSink {
    slot: Arc<OnceLock<Arc<LogWriter>>>,
}

impl WriterSink {
    pub(crate) fn new(slot: Arc<OnceLock<Arc<LogWriter>>>) -> Self {
        WriterSink { slot }
    }
}

impl LogSink for WriterSink {
    fn admit(&self, batch: ParsedLogs) -> Result<(), Backpressure> {
        match self.slot.get() {
            Some(writer) => writer.admit(batch),
            None => Err(Backpressure),
        }
    }

    fn admit_flush(&self, batch: ParsedLogs) -> Result<FlushWait, Backpressure> {
        match self.slot.get() {
            Some(writer) => writer.admit_flush(batch),
            None => Err(Backpressure),
        }
    }
}

/// `POST /v1/logs` (docs/api.md §1.1): pulls `AppState`'s `WriterSink` and
/// hands straight into `pulsus_write::ingest`'s reused #8 core — no logic
/// of its own beyond that seam.
pub(crate) async fn ingest_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    pulsus_write::ingest(state.writer.as_ref(), headers, body).await
}

#[cfg(test)]
mod tests {
    use super::*;

    use pulsus_model::UnixNano;
    use pulsus_write::LogRow;

    fn batch() -> ParsedLogs {
        ParsedLogs {
            rows: vec![LogRow {
                service: "svc".to_string(),
                fingerprint: 1,
                timestamp_ns: UnixNano(1),
                severity: 0,
                body: "hello".to_string(),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn admit_is_backpressure_while_the_slot_is_empty() {
        let sink = WriterSink::new(Arc::new(OnceLock::new()));
        assert_eq!(sink.admit(batch()), Err(Backpressure));
    }

    #[test]
    fn admit_flush_is_backpressure_while_the_slot_is_empty() {
        let sink = WriterSink::new(Arc::new(OnceLock::new()));
        assert!(sink.admit_flush(batch()).is_err());
    }
}
