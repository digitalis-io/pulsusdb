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
//!   statement;
//! * [`tracelevel`] — the search statement's trace-level intrinsics and
//!   `span:childCount` (issue #594 part 1);
//! * [`numbering`] — the search statement's nested-set intrinsics (issue
//!   #594 part 2);
//! * [`metrics`] — the metrics statement (issue #596 part 1): every
//!   metrics shape but `compare()`, in one statement per request;
//! * [`error_spans`] — which queries the error-span table answers
//!   (issue #601 part 4b);
//! * [`graph`] — the service-graph statement (issue #599): per-minute
//!   edge totals plus the calls they do not cover, paired from spans;
//! * [`tags`] — the two catalog reads of tag discovery (issue #598 part
//!   1): tag names from `tag_names`, unnarrowed values from `tag_values`.

pub mod attr_index;
pub mod by_key;
pub mod error_spans;
pub mod fetch;
pub mod fold;
pub mod graph;
pub mod metrics;
pub mod numbering;
pub mod predicate;
pub mod projection;
pub mod rows;
pub mod search;
pub mod structural;
pub mod tags;
pub mod tracelevel;

/// `(SELECT *{cols} FROM {from_where}\nLIMIT 1 BY trace_id, start_ns, span_id, kind)`:
/// `from_where` is `<table>[ joins]\nWHERE …`, the read's own text unchanged.
///
/// A span read without `final` (issue #601 part 4a): a push stored twice is
/// two rows with one span identity, `(trace_id, start_ns, span_id, kind)` —
/// the `spans` sorting key less its bucket, a function of `start_ns` — and
/// this keeps one of them. `cols` aliases each joined column the outer
/// read names, such as `tv.td`, to that name, so it resolves through the
/// wrap.
pub(crate) fn distinct_spans(from_where: &str, cols: &str) -> String {
    format!("(SELECT *{cols} FROM {from_where}\nLIMIT 1 BY trace_id, start_ns, span_id, kind)")
}

#[cfg(test)]
mod tests {
    use super::distinct_spans;

    /// T15 (issue #601 part 4a): the span read is wrapped whole, its own
    /// text unchanged, and keeps one row per span identity.
    #[test]
    fn distinct_spans_wraps_the_read_whole() {
        assert_eq!(
            distinct_spans("spans\nWHERE a", ""),
            "(SELECT * FROM spans\nWHERE a\nLIMIT 1 BY trace_id, start_ns, span_id, kind)"
        );
        assert_eq!(
            distinct_spans("spans\nWHERE a", ", tv.td AS `tv.td`"),
            "(SELECT *, tv.td AS `tv.td` FROM spans\nWHERE a\nLIMIT 1 BY trace_id, start_ns, span_id, kind)"
        );
    }
}
