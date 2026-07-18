//! Ingestion protocol parsers and insert services. See
//! docs/architecture.md §4.

pub mod error;
pub mod ingest;
pub mod protocols;
pub mod writer;

pub use error::LogsIngestError;
pub use ingest::http::{
    ingest, ingest_loki_push, ingest_metrics, ingest_remote_write, ingest_traces, ingest_zipkin,
};
pub use ingest::metrics::{MetricMetadata, MetricPoint, MetricSink, ParsedMetrics, SeriesRef};
pub use ingest::traces::{AttrRecord, ParsedTraces, SpanRecord, TraceSink};
pub use ingest::{Backpressure, FlushWait, LogSink};
pub use protocols::loki_push::{
    decode_protobuf as decode_loki_protobuf, parse_json as parse_loki_json,
    parse_protobuf as parse_loki_protobuf,
};
pub use protocols::otlp_logs::{LogRow, ParsedLogs, StreamRow, decode, parse};
pub use protocols::otlp_metrics::{decode as decode_metrics, parse as parse_metrics};
pub use protocols::otlp_traces::{decode as decode_traces, parse as parse_traces};
pub use protocols::remote_write::{
    WriteRequest, decode as decode_remote_write, parse as parse_remote_write,
};
pub use protocols::zipkin::{
    Annotation as ZipkinAnnotation, Endpoint as ZipkinEndpoint, ZipkinSpan,
    decode as decode_zipkin, to_otlp as zipkin_to_otlp,
};
pub use writer::{
    LogWriter, MetricMetadataRow, MetricSampleRow, MetricSeriesRow, MetricWriter,
    MetricWriterTables, TraceAttrRow, TraceSpanRow, TraceWriter, TraceWriterTables, WriteError,
    WriterTables,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
