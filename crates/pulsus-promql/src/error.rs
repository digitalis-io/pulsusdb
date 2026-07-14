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
    /// module doc's pinned contract).
    #[error("{0}")]
    Parse(String),

    /// An out-of-subset function, operator, or modifier — named exactly so
    /// the caller never has to guess what silently failed (architect plan:
    /// "no silent wrong answer"). Covers everything outside the M2 proof
    /// subset: the `@` modifier, subqueries, `group_left`/`group_right`,
    /// duration-expression arithmetic, native-histogram arithmetic, and
    /// every function outside the M2 list.
    #[error("not yet supported: {construct}")]
    Unsupported { construct: String },

    /// A binary expression's vector matching is invalid or unsupported —
    /// e.g. a many-to-one match without `group_left`/`group_right` (which
    /// are themselves out of the M2 subset, so any many-to-one match is
    /// rejected here, never silently mismatched).
    #[error("binary operator matching error: {detail}")]
    BadMatching { detail: String },

    /// `histogram_quantile` could not compute a quantile — a malformed
    /// `le` label (parse failure) or a bucket series missing the required
    /// `+Inf` bucket. Never a silently wrong quantile.
    #[error("histogram_quantile error: {detail}")]
    HistogramBucket { detail: String },
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

    #[test]
    fn bad_matching_display_names_the_detail() {
        let err = PromqlError::BadMatching {
            detail: "many-to-one match without group_left/group_right".to_string(),
        };
        assert!(err.to_string().contains("many-to-one"));
    }

    #[test]
    fn histogram_bucket_display_names_the_detail() {
        let err = PromqlError::HistogramBucket {
            detail: "no +Inf bucket found".to_string(),
        };
        assert!(err.to_string().contains("+Inf"));
    }
}
