//! `TraceEngine` — executes the §4.2 trace-by-ID point read against
//! ClickHouse via `ChClient`, streaming the stored per-span rows back to
//! the caller. Deliberately OTLP-agnostic (see [`super`]'s module doc):
//! payload decoding/dedup/assembly is `pulsus-server`'s job.

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, QuerySettings};

use super::rows::{StoredSpan, StoredSpanRow};
use crate::logql::error::ReadError;

/// Owned table configuration a [`TraceEngine`] reads against — mirrors
/// [`crate::logql::EngineConfig`]'s "owned `String`, no borrowed lifetime
/// on the engine itself" shape, at point-read scale (one table, no
/// budgets: a `trace_id` point read is a primary-index read by
/// construction, gated live by `tests/traces_point_read.rs`).
#[derive(Debug, Clone)]
pub struct TraceReadConfig {
    /// `trace_spans` (or `trace_spans_dist` when clustered — the caller
    /// applies the same `_dist` rule as every other read engine's config).
    pub spans_table: String,
}

pub struct TraceEngine {
    client: ChClient,
    config: TraceReadConfig,
}

impl TraceEngine {
    pub fn new(client: ChClient, config: TraceReadConfig) -> Self {
        Self { client, config }
    }

    /// Streams the §4.2 point read for one trace. `hex32` must already be
    /// validated as exactly 32 lowercase hex chars (the server's
    /// `parse_trace_id` is the one validation point) — injection-safe
    /// because only `[0-9a-f]` can then reach the `unhex('...')` literal.
    /// An empty `Vec` means the trace is absent (the handler maps that to
    /// `404`); duplicate `span_id`s from at-least-once ingest are returned
    /// as stored — dedup is the assembler's read-time concern.
    pub async fn fetch_by_id(&self, hex32: &str) -> Result<Vec<StoredSpan>, ReadError> {
        let sql = super::sql::point_read_sql(&self.config.spans_table, hex32);
        let mut spans = Vec::new();
        // Scoped stream: the pooled-connection lease is dropped when this
        // binding leaves scope at the end of the function, after full
        // consumption.
        let mut stream = self
            .client
            .query_stream::<StoredSpanRow>(&sql, &QuerySettings::new())
            .await
            .map_err(ReadError::Clickhouse)?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(ReadError::Clickhouse)?;
            spans.push(StoredSpan::from(row));
        }
        Ok(spans)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_read_config_is_cloneable_and_debuggable() {
        let config = TraceReadConfig {
            spans_table: "trace_spans".to_string(),
        };
        let clone = config.clone();
        assert_eq!(clone.spans_table, "trace_spans");
        assert!(format!("{config:?}").contains("trace_spans"));
    }
}
