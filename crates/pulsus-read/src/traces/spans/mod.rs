//! The trace fetch on the span, per-trace and resource tables (issue #587).
//!
//! `pulsus-read` stays OTLP-agnostic, the invariant
//! [`crate::traces`] states: this module speaks SQL and streamed rows
//! only, and the mapping from a decoded `JSON` column to a `KeyValue`
//! lives server-side.
//!
//! * [`fetch`] — the three pure SQL builders: the indexed read, the
//!   complete-predicate read for a bucket set that may have truncated,
//!   and the window fallback;
//! * [`rows`] — the three result-row shapes, the four nested element
//!   tuples, the route enum and the request window;
//! * [`predicate`] — the span-scope predicate compiler (issue #588): one
//!   TraceQL leaf to one ClickHouse boolean over a `spans` row, and the
//!   one place a window and a predicate compose;
//! * [`search`] — the search statement (issue #590): the newest traces
//!   with a matching span, their capped spansets and their roots, in one
//!   statement.

pub mod fetch;
pub mod predicate;
pub mod projection;
pub mod rows;
pub mod search;
pub mod structural;
