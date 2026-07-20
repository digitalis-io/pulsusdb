//! `PromqlError` — this crate's whole error taxonomy. Mirrors
//! `pulsus-read::ReadError`'s style: `thiserror`, one variant per
//! distinguishable failure, each carrying enough context to be actionable
//! in the `X-Pulsus-Explain`/query-error envelope #13 builds.
//!
//! **`Parse` is a pinned contract (issue #32):** its `Display` carries the
//! vendored parser's upstream error text verbatim (including whatever
//! position text the parser itself produces) — never re-wrapped, never
//! given an added prefix, so a caller surfacing this to an API response
//! shows the parser's own message unmodified.

use thiserror::Error;

/// Errors from parsing, planning, or evaluating a PromQL query. Pure — no
/// I/O variant lives here (this crate never touches ClickHouse); the fetch
/// layer's own I/O errors live in `pulsus-read::ReadError`, which wraps
/// this type via `#[from]`.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum PromqlError {
    /// The vendored parser's own error text, carried verbatim (see the
    /// module doc's pinned contract). Issue #84 rides the same
    /// verbatim-text contract for plan-time duration-expression
    /// *resolution* errors (`plan.rs::resolve_duration_expr`), which
    /// mirror upstream `promql/durations.go` messages ("division by
    /// zero", "duration must be greater than 0", "duration is out of
    /// range", ...) with no added prefix.
    #[error("{0}")]
    Parse(String),

    /// An out-of-subset function, operator, or modifier — named exactly so
    /// the caller never has to guess what silently failed (architect plan:
    /// "no silent wrong answer"). Covers everything outside the
    /// implemented subset: native-histogram arithmetic, gated
    /// experimental constructs with the flag off (including issue #84's
    /// duration expressions, whose gate-off `construct` carries upstream's
    /// "experimental duration expression is not enabled" verbatim), and
    /// every unimplemented function.
    #[error("not yet supported: {construct}")]
    Unsupported { construct: String },

    /// A binary expression's vector matching is invalid — the upstream
    /// duplicate-match errors (a many-to-one match without
    /// `group_left`/`group_right`, a duplicate "one"-side signature, a
    /// non-unique many-to-one output identity) and the modifier-misuse
    /// rejections ported from upstream parse.go (fill with a scalar
    /// operand or a set operator). `detail` carries the exact upstream
    /// v3.13 message verbatim (issue #70) — `Display` is the raw detail
    /// with no added prefix, mirroring `LabelSet` below.
    #[error("{detail}")]
    BadMatching { detail: String },

    /// `histogram_quantile`/`histogram_fraction` could not compute a
    /// result — an empty bucket group or a bucket series missing the
    /// required `+Inf` bucket. Never a silently wrong quantile. (A
    /// malformed/missing `le` label is NOT this variant as of `#124`:
    /// that bucket is skipped with a `bad_bucket_label_warning`,
    /// matching pinned `resetHistograms` — `eval::mod::
    /// partition_histogram_inputs`'s doc.)
    #[error("histogram_quantile error: {detail}")]
    HistogramBucket { detail: String },

    /// A label-rewrite/sort function's label-set contract is violated —
    /// issue #68 (M6-05): `label_replace`'s invalid regex or destination
    /// label name, `label_join`'s invalid destination/source label name,
    /// or a rewrite producing duplicate `(metric_name, labels)` output
    /// identities. `detail` carries the exact upstream v3.13 message
    /// (`promql/functions.go`: `invalid regular expression in
    /// label_replace(): …`, `invalid destination label name in …(): …`,
    /// `invalid source label name in label_join(): …`, `vector cannot
    /// contain metrics with the same labelset`) verbatim — the vendored
    /// `functions.test` asserts these as message substrings, so `Display`
    /// is the raw detail with no added prefix.
    #[error("{detail}")]
    LabelSet { detail: String },

    /// A function parameter is outside its valid domain — issue #67
    /// (M6-04, on the `HistogramBucket` precedent per the task-manager
    /// adjudication): `double_exponential_smoothing`'s smoothing/trend
    /// factors must satisfy `0 < f < 1` (upstream v3.13.0 panics there;
    /// this engine returns a query error instead). The detail names the
    /// parameter and its bounds. NOT used for `quantile_over_time`'s
    /// out-of-range φ, which upstream answers with `±Inf`/`NaN`, never an
    /// error.
    #[error("invalid function parameter: {detail}")]
    InvalidParameter { detail: String },

    /// The evaluation was cancelled by a live [`crate::eval::CancelToken`]
    /// (issue #93) — observed at a per-step/per-grid-point checkpoint after
    /// the awaiting request future was dropped (client disconnect, or the
    /// server's `TimeoutLayer` firing first). `evaluate` (the `never()`
    /// token) can never produce this variant.
    #[error("query evaluation cancelled")]
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_display_carries_the_upstream_message_verbatim() {
        let err = PromqlError::Parse("unexpected character: 'x'".to_string());
        assert_eq!(err.to_string(), "unexpected character: 'x'");
    }

    #[test]
    fn unsupported_display_names_the_construct() {
        let err = PromqlError::Unsupported {
            construct: "the @ modifier".to_string(),
        };
        assert!(err.to_string().contains("the @ modifier"));
    }

    /// Issue #70: the vendored corpus asserts these messages as substrings
    /// (and, after this fix, an anchored regex) of the query error —
    /// `Display` must be the raw upstream text with no added prefix.
    #[test]
    fn bad_matching_display_is_the_raw_detail_with_no_prefix() {
        let err = PromqlError::BadMatching {
            detail: "many-to-one match without group_left/group_right".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "many-to-one match without group_left/group_right"
        );
    }

    #[test]
    fn histogram_bucket_display_names_the_detail() {
        let err = PromqlError::HistogramBucket {
            detail: "no +Inf bucket found".to_string(),
        };
        assert!(err.to_string().contains("+Inf"));
    }

    /// Issue #68 (M6-05): the vendored `functions.test` asserts these
    /// messages as substrings of the query error — `Display` must be the
    /// raw upstream text with no added prefix.
    #[test]
    fn label_set_display_is_the_raw_detail_with_no_prefix() {
        let err = PromqlError::LabelSet {
            detail: "vector cannot contain metrics with the same labelset".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "vector cannot contain metrics with the same labelset"
        );
    }

    #[test]
    fn invalid_parameter_display_names_the_detail() {
        let err = PromqlError::InvalidParameter {
            detail: "invalid smoothing factor: expected 0 < sf < 1, got 2".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "invalid function parameter: invalid smoothing factor: expected 0 < sf < 1, got 2"
        );
    }
}
