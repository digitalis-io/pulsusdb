//! Ingestion protocol parsers and insert services. See
//! docs/architecture.md §4.

pub mod error;
pub mod ingest;
pub mod protocols;
pub mod writer;

pub use error::LogsIngestError;
pub use ingest::http::{ingest, ingest_metrics, ingest_remote_write};
pub use ingest::metrics::{MetricMetadata, MetricPoint, MetricSink, ParsedMetrics, SeriesRef};
pub use ingest::{Backpressure, FlushWait, LogSink};
pub use protocols::otlp_logs::{LogRow, ParsedLogs, StreamRow, decode, parse};
pub use protocols::otlp_metrics::{decode as decode_metrics, parse as parse_metrics};
pub use protocols::remote_write::{
    WriteRequest, decode as decode_remote_write, parse as parse_remote_write,
};
pub use writer::{
    LogWriter, MetricMetadataRow, MetricSampleRow, MetricSeriesRow, MetricWriter,
    MetricWriterTables, WriteError, WriterTables,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
