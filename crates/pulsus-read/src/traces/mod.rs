//! Trace read path (issue #55): the docs/schemas.md §4.2 trace-by-ID
//! point read. Deliberately **OTLP-agnostic** (task-manager adjudication
//! on issue #55, open question 1): this module speaks SQL and streamed
//! rows only — no `prost`/`opentelemetry-proto` dependency enters this
//! crate. Decoding the stored per-span payloads, de-duplicating
//! at-least-once replays, and assembling the OTLP `TracesData` response
//! all live server-side (`pulsus-server/src/traces_api/assemble.rs`),
//! mirroring the logs layering (`pulsus-read` returns rows,
//! `logs_api/encode.rs` shapes them).
//!
//! **Module layout** mirrors [`crate::logql`]'s plan/execute split at
//! point-read scale: [`sql`] (the pure, snapshot-tested SQL builder),
//! [`rows`] (`ChClient` result-row shapes), and [`exec`] (`TraceEngine`,
//! the only module here that talks to ClickHouse).

pub mod exec;
pub mod rows;
pub mod sql;

pub use exec::{TraceEngine, TraceReadConfig};
pub use rows::{StoredSpan, StoredSpanRow};
