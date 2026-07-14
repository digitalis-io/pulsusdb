//! `POST /v1/logs`, `POST /v1/metrics`, and `POST /api/v1/write` server
//! wiring (issue #15/#27/#28 architect plans): [`WriterSink`]/
//! [`MetricWriterSink`] adapt their async-filled writer slots
//! (`serve::spawn_reconnect_loop` constructs the real [`LogWriter`]/
//! [`MetricWriter`] once the ClickHouse pool is ready, *before* `pool_slot`
//! — see that module's doc comment) to `pulsus-write`'s [`LogSink`]/
//! [`MetricSink`] seams, and [`ingest_logs`]/[`ingest_metrics`]/
//! [`ingest_remote_write`] are the thin `State<AppState>` handlers that
//! call straight into `pulsus_write::ingest`/`pulsus_write::ingest_metrics`/
//! `pulsus_write::ingest_remote_write`'s state-agnostic cores (see those
//! fns' doc comments for why the server cannot reuse
//! `pulsus_write::ingest::http::{logs,metrics}::<S>`'s generic-`State`
//! mount points). [`ingest_remote_write`] (issue #28) reuses
//! [`MetricWriterSink`] verbatim — the same `AppState.metric_writer` field
//! #27 introduced — adding only its own route + handler, per the ratified
//! "second issue rebases onto the first" ordering rule.

use std::sync::{Arc, OnceLock};

use axum::body::Body;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;

use pulsus_write::{
    Backpressure, FlushWait, LogSink, LogWriter, MetricSink, MetricWriter, ParsedLogs,
    ParsedMetrics,
};

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

/// [`WriterSink`]'s metrics counterpart (issue #27, deferred from #26):
/// adapts `Arc<OnceLock<Arc<MetricWriter>>>` to [`MetricSink`], same
/// "backpressure while empty" contract.
pub(crate) struct MetricWriterSink {
    slot: Arc<OnceLock<Arc<MetricWriter>>>,
}

impl MetricWriterSink {
    pub(crate) fn new(slot: Arc<OnceLock<Arc<MetricWriter>>>) -> Self {
        MetricWriterSink { slot }
    }
}

impl MetricSink for MetricWriterSink {
    fn admit(&self, batch: ParsedMetrics) -> Result<(), Backpressure> {
        match self.slot.get() {
            Some(writer) => writer.admit(batch),
            None => Err(Backpressure),
        }
    }

    fn admit_flush(&self, batch: ParsedMetrics) -> Result<FlushWait, Backpressure> {
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

/// `POST /v1/metrics` (docs/api.md §1.1, issue #27): pulls `AppState`'s
/// `MetricWriterSink` and hands straight into
/// `pulsus_write::ingest_metrics`'s reused core — no logic of its own
/// beyond that seam.
pub(crate) async fn ingest_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    pulsus_write::ingest_metrics(state.metric_writer.as_ref(), headers, body).await
}

/// `POST /api/v1/write` (docs/api.md §1.2, issue #28): Prometheus remote-
/// write. Pulls `AppState`'s `MetricWriterSink` — the same instance
/// `ingest_metrics` uses — and hands straight into
/// `pulsus_write::ingest_remote_write`'s reused core; no logic of its own
/// beyond that seam.
pub(crate) async fn ingest_remote_write(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    pulsus_write::ingest_remote_write(state.metric_writer.as_ref(), headers, body).await
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

    fn metrics_batch() -> ParsedMetrics {
        ParsedMetrics {
            samples: vec![pulsus_write::MetricPoint {
                metric_name: Arc::from("up"),
                fingerprint: 1,
                unix_milli: 1,
                value: 1.0,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn metric_admit_is_backpressure_while_the_slot_is_empty() {
        let sink = MetricWriterSink::new(Arc::new(OnceLock::new()));
        assert_eq!(sink.admit(metrics_batch()), Err(Backpressure));
    }

    #[test]
    fn metric_admit_flush_is_backpressure_while_the_slot_is_empty() {
        let sink = MetricWriterSink::new(Arc::new(OnceLock::new()));
        assert!(sink.admit_flush(metrics_batch()).is_err());
    }
}
