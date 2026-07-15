//! Protocol parsers: one module per ingestion protocol
//! (docs/architecture.md §4). Each parser is a pure function from request
//! bytes to normalized rows — no I/O, trivially unit-testable against
//! captured fixtures.

pub mod otlp_logs;
pub mod otlp_metrics;
pub mod otlp_traces;
pub mod remote_write;
