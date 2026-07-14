//! Ingestion protocol parsers and insert services. See
//! docs/architecture.md §4.

pub mod error;
pub mod ingest;
pub mod protocols;
pub mod writer;

pub use error::LogsIngestError;
pub use ingest::{Backpressure, FlushWait, LogSink};
pub use protocols::otlp_logs::{LogRow, ParsedLogs, StreamRow, decode, parse};
pub use writer::{LogWriter, WriteError};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
