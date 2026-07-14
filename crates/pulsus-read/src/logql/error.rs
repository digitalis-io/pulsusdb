//! `ReadError` — the LogQL read-path error taxonomy. Follows
//! `pulsus-schema::SchemaError`/`pulsus-write::LogsIngestError`'s style:
//! `thiserror`, one variant per distinguishable failure, each carrying
//! enough context to be actionable in the `X-Pulsus-Explain`/query-error
//! envelope #13 builds.
//!
//! **Budget vs stream-cap are structurally distinct** (architect plan
//! amendment §4, review finding 4): a ClickHouse row/stream limit must
//! never masquerade as the byte scan budget, and neither is ever surfaced
//! as a generic `ChError::Timeout` — a caller inspecting `ReadError` can
//! always tell "this query was too broad" from "this query's execution
//! genuinely stalled".

use std::fmt;

use pulsus_clickhouse::ChError;
use pulsus_logql::LogQlError;
use thiserror::Error;

/// Why a query was rejected as too broad. Two structurally separate
/// reasons, never conflated (architect plan amendment §4):
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TooBroadReason {
    /// The ClickHouse `max_bytes_to_read` setting (set from
    /// `reader.logql_scan_budget_bytes`) was exceeded — server code 307
    /// (`TOO_MANY_BYTES`). `estimate` is the pre-flight selectivity-probe
    /// estimate when one was computed (best-effort early-abort only; the
    /// ClickHouse setting is the authoritative backstop).
    ScanBudgetBytes {
        budget_bytes: u64,
        estimate: Option<u64>,
    },
    /// Stage 1 resolved more fingerprints than [`crate::logql::params::DEFAULT_MAX_STREAMS`]
    /// (or a caller-supplied cap) — a Rust-side structural limit, never a
    /// ClickHouse row cap (`max_rows_to_read` is deliberately never set,
    /// so ClickHouse code 158 `TOO_MANY_ROWS` can never masquerade as this).
    StreamCap { count: usize, cap: usize },
}

impl fmt::Display for TooBroadReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TooBroadReason::ScanBudgetBytes {
                budget_bytes,
                estimate,
            } => match estimate {
                Some(est) => write!(
                    f,
                    "scan budget of {budget_bytes} bytes exceeded (estimate: {est} bytes)"
                ),
                None => write!(f, "scan budget of {budget_bytes} bytes exceeded"),
            },
            TooBroadReason::StreamCap { count, cap } => {
                write!(
                    f,
                    "resolved {count} streams, exceeding the {cap}-stream cap"
                )
            }
        }
    }
}

/// Errors from planning or executing a LogQL **or PromQL** query — despite
/// living in the `logql` module (this crate's original, pre-#31 error
/// type), `ReadError` is shared crate-wide; issue #31's
/// `metrics::exec::MetricsEngine` reuses it rather than minting a second
/// top-level error type, via the `Promql` variant below.
#[derive(Debug, Error)]
pub enum ReadError {
    /// Surfaced by callers (e.g. #13) that parse a raw query string before
    /// handing the AST to this crate; `plan`/`LogQlEngine` themselves never
    /// parse, so this variant is never constructed inside `pulsus-read`.
    #[error("logql parse error: {0}")]
    Parse(#[from] LogQlError),

    /// Issue #31: any `pulsus_promql::plan`/`evaluate` failure — parse,
    /// unsupported-construct, bad vector matching, or a
    /// `histogram_quantile` bucket error. The inner `PromqlError`'s own
    /// `Display` (in particular `PromqlError::Parse`, which carries the
    /// vendored parser's upstream error text verbatim) is preserved
    /// unmodified inside this variant's message — only this outer "promql:
    /// " prefix is added.
    #[error("promql: {0}")]
    Promql(#[from] pulsus_promql::PromqlError),

    /// The selector has no positive (`=`/`=~`) matcher — Loki's
    /// "match-everything" rejection (task-manager resolution #2: the coarse
    /// "≥1 positive matcher" rule; precise regex-matches-empty detection is
    /// deferred to M6).
    #[error("selector matches everything: at least one equality or regexp matcher is required")]
    EmptyMatcherSet,

    /// Two `Eq` matchers on the same label key with different literal
    /// values — provably empty (a stream's `(key, val)` pair cannot equal
    /// two different values at once).
    #[error("matchers are contradictory: the selector can never match a stream")]
    ContradictoryMatchers,

    /// The query was rejected as too broad. See [`TooBroadReason`].
    #[error("query too broad: {0}")]
    QueryTooBroad(TooBroadReason),

    /// A `Range` metric query's `step_ns` was zero. `0.is_multiple_of(_)`
    /// is trivially `true`, which would otherwise let the routing decision
    /// pick rollup and render `intDiv(bucket_ns, 0)` — undefined in
    /// ClickHouse; the raw fallback's own `intDiv(timestamp_ns, 0)`
    /// bucketing is equally invalid. Rejected in [`super::plan::plan`]
    /// before either SQL builder is reached — defense in depth ahead of
    /// whatever request-level `step > 0` validation #13 adds (task-manager
    /// resolution #4 on issue #12).
    #[error("range query step_ns must be greater than zero")]
    InvalidStep,

    /// An unclassified/passthrough ClickHouse error (network, decode,
    /// server exception not mapped to [`ReadError::QueryTooBroad`]).
    #[error("clickhouse: {0}")]
    Clickhouse(#[from] ChError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_budget_bytes_display_names_the_budget() {
        let reason = TooBroadReason::ScanBudgetBytes {
            budget_bytes: 1024,
            estimate: None,
        };
        assert!(reason.to_string().contains("1024"));
    }

    #[test]
    fn scan_budget_bytes_display_names_the_estimate_when_present() {
        let reason = TooBroadReason::ScanBudgetBytes {
            budget_bytes: 1024,
            estimate: Some(4096),
        };
        let msg = reason.to_string();
        assert!(msg.contains("1024"));
        assert!(msg.contains("4096"));
    }

    #[test]
    fn stream_cap_display_names_count_and_cap() {
        let reason = TooBroadReason::StreamCap {
            count: 150_000,
            cap: 100_000,
        };
        let msg = reason.to_string();
        assert!(msg.contains("150000"));
        assert!(msg.contains("100000"));
    }

    #[test]
    fn query_too_broad_display_wraps_the_reason() {
        let err = ReadError::QueryTooBroad(TooBroadReason::StreamCap { count: 1, cap: 1 });
        assert!(err.to_string().contains("query too broad"));
    }

    #[test]
    fn empty_matcher_set_message_explains_the_rule() {
        assert!(
            ReadError::EmptyMatcherSet
                .to_string()
                .contains("at least one equality or regexp matcher")
        );
    }

    #[test]
    fn contradictory_matchers_message_is_descriptive() {
        assert!(
            ReadError::ContradictoryMatchers
                .to_string()
                .contains("contradictory")
        );
    }

    #[test]
    fn invalid_step_message_names_the_zero_step_rule() {
        assert!(ReadError::InvalidStep.to_string().contains("step_ns"));
    }

    #[test]
    fn promql_display_wraps_the_inner_error_verbatim() {
        let inner = pulsus_promql::PromqlError::Unsupported {
            construct: "the @ modifier".to_string(),
        };
        let err = ReadError::Promql(inner);
        assert!(err.to_string().starts_with("promql: "));
        assert!(err.to_string().contains("the @ modifier"));
    }

    #[test]
    fn promql_parse_error_text_survives_unmodified_inside_read_error() {
        let inner = pulsus_promql::PromqlError::Parse("unexpected character: 'x'".to_string());
        let err = ReadError::from(inner);
        assert_eq!(err.to_string(), "promql: unexpected character: 'x'");
    }
}
