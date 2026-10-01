//! Ingestion protocol parsers and insert services. See
//! docs/architecture.md §4.

pub mod error;
pub mod ingest;
pub mod patterns;
pub mod protocols;
pub mod writer;

pub use error::LogsIngestError;
pub use ingest::http::{
    ingest, ingest_loki_push, ingest_metrics, ingest_remote_write, ingest_traces, ingest_zipkin,
};
pub use ingest::metrics::{
    HistogramPoint, MetricMetadata, MetricPoint, MetricSink, ParsedMetrics, SeriesRef,
};
pub use ingest::traces::{
    AttrRecord, AttrValueType, LandingEvent, LandingLink, LandingResource, LandingSpan,
    LandingTagName, LandingTagValue, ParsedTraceLanding, ParsedTraces, SpanRecord, TagScope,
    TraceSink,
};
pub use ingest::{
    AdmitRefusal, Backpressure, FlushWait, KEY_REUSED_MESSAGE, LogSink, PushHeaders,
    push_spans_too_many_days_message, push_too_large_message,
};
pub use protocols::log_level::LevelDiscovery;
pub use protocols::loki_push::{
    decode_protobuf as decode_loki_protobuf, parse_json as parse_loki_json,
    parse_protobuf as parse_loki_protobuf, render_structured_metadata,
};
pub use protocols::otlp_logs::{LogIngestSettings, LogRow, ParsedLogs, StreamRow, decode, parse};
pub use protocols::otlp_metrics::{
    MetricIngestSettings, decode as decode_metrics, parse as parse_metrics,
};
pub use protocols::otlp_traces::{
    decode as decode_traces, parse as parse_traces, parse_landing as parse_trace_landing,
    resource_identity,
};
pub use protocols::remote_write::{
    WriteRequest, decode as decode_remote_write, parse as parse_remote_write,
};
pub use protocols::zipkin::{
    Annotation as ZipkinAnnotation, Endpoint as ZipkinEndpoint, ZipkinSpan,
    decode as decode_zipkin, to_otlp as zipkin_to_otlp,
};
pub use writer::{
    Admission, Capacities, ClaimGuard, ClaimOutcome, DedupMetricsSnapshot, LANDING_ROW_SLOT_BYTES,
    LogPatternRow, LogWriter, MetricHistSampleRow, MetricLandingRow, MetricMetadataRow,
    MetricSampleRow, MetricSeriesRow, MetricWriter, MetricWriterTables, PushDedup, PushDigest,
    PushIdentity, TRACE_LANDING_ROW_SLOT_BYTES, TargetOutcome, TraceAttrRow, TraceEventTuple,
    TraceJson, TraceJsonEntry, TraceJsonScalar, TraceJsonValue, TraceLandingRow, TraceLinkTuple,
    TraceSpanRow, TraceWriter, TraceWriterTables, WaitGuard, WaitMode, WriteError, WriterTables,
    escape_json_path, index_bytes, log_identity, metric_identity, plan_capacities, trace_identity,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
