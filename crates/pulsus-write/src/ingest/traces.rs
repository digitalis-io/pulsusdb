//! The seam between the OTLP traces receiver (`POST /v1/traces`, issue
//! #54) and the writer core: [`TraceSink`] plus the types an admitted
//! batch carries. Pure data + trait, no I/O — mirrors `ingest/metrics.rs`'s
//! `ParsedMetrics`/`MetricSink` split (issue #26/#27's precedent), minus
//! everything traces do not have: no fingerprints, no label sets (so no
//! `collisions`), no registration/metadata carriers (`trace_tag_catalog` is
//! MV-populated, never writer-written — issue #53).

use crate::ingest::{Backpressure, FlushWait};

/// One `trace_spans` row's source data (docs/schemas.md §4.1), produced by
/// the OTLP traces parser. IDs are raw wire bytes (`FixedString(16)`/
/// `FixedString(8)` columns); `payload` is a self-contained
/// single-`ResourceSpans` `TracesData` protobuf (this span + its resource +
/// its scope), the pinned T2/T3 contract — T3 decodes each span's payload
/// independently and concatenates into a valid `TracesData`.
#[derive(Debug, Clone, PartialEq)]
pub struct SpanRecord {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    /// `[0u8; 8]` for a root span (empty `parent_span_id` on the wire).
    pub parent_id: [u8; 8],
    pub name: String,
    /// String-rendering of resource attr `service.name`, verbatim (not
    /// normalized — docs/architecture.md §2.3), `""` when absent.
    pub service: String,
    pub timestamp_ns: i64,
    pub duration_ns: i64,
    pub status_code: i8,
    pub kind: i8,
    /// Encoded single-`ResourceSpans` `TracesData` (see above).
    pub payload: Vec<u8>,
}

/// One `trace_attrs_idx` row's source data (docs/schemas.md §4.1): one
/// resource or span attribute of one span, key verbatim (never normalized —
/// docs/architecture.md §2.3), discriminated by `scope`.
#[derive(Debug, Clone, PartialEq)]
pub struct AttrRecord {
    /// The span's UTC **day** since the Unix epoch
    /// (`pulsus_model::Date::start_of_day_utc` — `trace_attrs_idx` is
    /// `PARTITION BY date`, daily, unlike `log_streams.month`).
    pub date: u16,
    pub key: String,
    /// `'resource'` or `'span'` — the index's scope discriminator, so
    /// `resource.foo` and `span.foo` never collide (issue #54 plan v2
    /// delta 1). Scope (`InstrumentationScope`) attributes are not indexed
    /// at all; they stay in the span payload.
    pub scope: String,
    pub val: String,
    /// `val.parse::<f64>()` when finite, else `None` (`Nullable(Float64)`).
    pub val_num: Option<f64>,
    pub timestamp_ns: i64,
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub duration_ns: i64,
}

/// The normalized output the OTLP traces parser hands a [`TraceSink`]:
/// rows destined for `trace_spans` and `trace_attrs_idx`, plus the
/// per-request partial-success accounting (`rejected`, `rejected_message`).
/// No `collisions` counter — traces have no `LabelSet` (verbatim keys, no
/// normalization, nothing to collide).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ParsedTraces {
    /// One per accepted span.
    pub spans: Vec<SpanRecord>,
    /// One per indexed (resource ⊕ span) attribute of every accepted span.
    pub attrs: Vec<AttrRecord>,
    /// Count of individual spans dropped during parsing (not requests — a
    /// malformed/truncated payload is a whole-request error, never a
    /// `rejected` count).
    pub rejected: u64,
    /// The first rejection's error message, surfaced verbatim as the OTLP
    /// `partial_success.error_message`.
    pub rejected_message: Option<String>,
}

/// The boundary the OTLP traces handler hands parsed batches across:
/// admission only, no batching/flush/ClickHouse-write logic lives on this
/// side. `Send + Sync` because a server holds an implementor behind
/// `axum::extract::State`, shared across concurrently-handled requests —
/// mirrors [`crate::ingest::LogSink`]/[`crate::ingest::metrics::MetricSink`]
/// exactly, including the reuse of [`FlushWait`] (whose `Output` is
/// `Result<(), LogsIngestError>` — see `MetricSink`'s doc comment for the
/// task-manager resolution deferring a neutral `IngestError` rename to M6).
pub trait TraceSink: Send + Sync {
    /// Admits `batch` for async-mode requests: the caller responds
    /// immediately once this returns `Ok`, without waiting for the batch
    /// to be flushed.
    fn admit(&self, batch: ParsedTraces) -> Result<(), Backpressure>;

    /// Admits `batch` for sync-mode requests: the caller `.await`s the
    /// returned [`FlushWait`] before responding.
    fn admit_flush(&self, batch: ParsedTraces) -> Result<FlushWait, Backpressure>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_record_stores_fields_verbatim() {
        let span = SpanRecord {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_id: [0; 8],
            name: "op-a".to_string(),
            service: "checkout".to_string(),
            timestamp_ns: 1_700_000_000_000_000_000,
            duration_ns: 42,
            status_code: 2,
            kind: 3,
            payload: vec![0xDE, 0xAD],
        };
        assert_eq!(span.trace_id, [1; 16]);
        assert_eq!(span.span_id, [2; 8]);
        assert_eq!(span.parent_id, [0; 8]);
        assert_eq!(span.name, "op-a");
        assert_eq!(span.service, "checkout");
        assert_eq!(span.timestamp_ns, 1_700_000_000_000_000_000);
        assert_eq!(span.duration_ns, 42);
        assert_eq!(span.status_code, 2);
        assert_eq!(span.kind, 3);
        assert_eq!(span.payload, vec![0xDE, 0xAD]);
    }

    #[test]
    fn parsed_traces_default_is_empty() {
        let parsed = ParsedTraces::default();
        assert!(parsed.spans.is_empty());
        assert!(parsed.attrs.is_empty());
        assert_eq!(parsed.rejected, 0);
        assert_eq!(parsed.rejected_message, None);
    }
}
