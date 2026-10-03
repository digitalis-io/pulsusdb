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
//!   tuples, the route enum and the request window.

pub mod fetch;
pub mod rows;
