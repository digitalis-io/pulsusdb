//! Protocol parsers: one module per ingestion protocol
//! (docs/architecture.md §4). Each parser is a pure function from request
//! bytes to normalized rows — no I/O, trivially unit-testable against
//! captured fixtures.

pub mod label_name;
pub mod log_label_limits;
pub mod log_level;
pub mod loki_push;
pub mod otlp_depth;
pub mod otlp_exp_histogram;
pub mod otlp_json;
pub mod otlp_logs;
pub mod otlp_metrics;
pub mod otlp_prescan;
pub mod otlp_traces;
pub mod prom_metric_name;
pub mod remote_write;
pub mod service_name;
pub mod zipkin;
