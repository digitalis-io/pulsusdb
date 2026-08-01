//! The TraceQL query-text admission cap (issue #284).
//!
//! # What the reference does
//!
//! grafana/tempo v3.0.2 caps TraceQL expression text in the query-frontend
//! pipeline, not in the parser:
//! `modules/frontend/pipeline/async_query_validator_middleware.go:45`
//! rejects when `len(traceQLQuery) > c.maxQuerySizeBytes`, where the bound
//! is `frontend.Config.MaxQueryExpressionSizeBytes`
//! (`modules/frontend/config.go:45`), defaulted at `:141` to `128 * 1024`.
//! The middleware reads the raw `q` parameter, then the `query` parameter
//! (`query` wins when both are present), and skips the check entirely when
//! the resulting string is empty.
//!
//! Two consequences worth stating, because both are easy to get wrong:
//!
//! 1. **The comparison is `>`, not `>=`** — so [`MAX_QUERY_EXPRESSION_BYTES`]
//!    is an INCLUSIVE maximum: exactly 131,072 bytes is ACCEPTED and
//!    131,073 is the shortest rejected query. This is the opposite
//!    boundary to LogQL's (`pulsus_logql::MAX_QUERY_BYTES`, whose
//!    reference compares `>=`); the two caps share a number and disagree
//!    on the edge.
//! 2. **It is scoped to three pipelines** — `searchPipeline`,
//!    `queryRangePipeline` and `queryInstantPipeline`
//!    (`modules/frontend/frontend.go:159,215,229`). The tag / tag-values
//!    pipelines and trace-by-ID do NOT carry the middleware, and neither
//!    does anything below the frontend: `pkg/traceql`'s `Parse` /
//!    `parseInternal` have no length guard at all.
//! 3. **It reads BOTH parameter names on all three**, `q` then `query`,
//!    including on search — where the request parser itself reads only
//!    `q` (`pkg/api/http.go:180`, `urlParamQuery = "q"`) and a lone
//!    `query` falls into the legacy tag map at `:222`. So on search,
//!    `query` carries the cap without ever being the searched
//!    expression, and PulsusDB reproduces exactly that split
//!    (`params::parse_search_params`). The metrics parsers DO alias the
//!    two (`pkg/api/http.go:295-303`, `:319-327`), and so do ours.
//!
//! One deliberate divergence, pinned by
//! `params::tests::both_search_query_parameters_are_capped_where_the_reference_caps_only_the_selected_one`:
//! the reference's selection is last-write-wins, so an over-cap `q`
//! accompanied by an under-cap `query` is never measured — and the
//! handler then executes that unbounded `q`. That is the validator and
//! the handler disagreeing about which parameter matters. PulsusDB caps
//! both, which is strictly safer and keeps [`TraceQlText`]'s invariant
//! meaningful: the value the search will actually run has been checked.
//!
//! # Why the cap is here and not in `pulsus-traceql`
//!
//! Because (2) is observable. Putting the check at the parser would also
//! cap the string `legacy::compile_legacy` synthesises from the legacy
//! `tags`/`minDuration`/`maxDuration` params — text the client never
//! wrote and the reference never measures (its middleware only ever reads
//! `q`/`query`). That is not theoretical: legacy compilation expands its
//! input by up to ~2.5x (`a=1 ` → `.a="1" && `), and the request target is
//! itself bounded at 65,534 bytes by `http::Uri`, so a legal legacy
//! request can compile to well over 131,072 bytes. Capping it would
//! manufacture a rejection the reference does not have.
//!
//! So the cap is applied where the reference applies it: to the
//! user-supplied `q`/`query` parameter, on the search and metrics
//! surfaces only.
//!
//! # Transport bound (the cap is not reachable over the wire today)
//!
//! Every TraceQL route PulsusDB serves is `GET`-only, and `http::Uri`
//! rejects a request target longer than **65,534 bytes** — measured
//! locally: a `/api/traces/v1/search?q=…` target of 65,024 bytes parses,
//! 65,544 does not. So a query anywhere near this cap is answered by the
//! HTTP stack before routing, where the reference (Go's `net/http`, 1 MiB
//! default `MaxHeaderBytes`) serves it. Same root cause as the LogQL
//! sub-cap band already filed as **#296**; recording it here so the cap's
//! reachability is not overstated. The cap is still the right value at
//! the right boundary — it just becomes observable when #296 does.
//!
//! # The seam
//!
//! [`TraceQlText`] lives in a private LEAF module whose entire contents
//! are the field, its checking constructor and its one accessor. Nothing
//! outside `leaf` — including the rest of this file — can name the field
//! or build the value any other way, so "the cap ran" is a property of
//! the type rather than a convention someone must remember. The
//! request-parameter structs hold `TraceQlText`, so a new parameter
//! parser that reads `q` and forgets the check does not compile.

use thiserror::Error;

pub(crate) use leaf::TraceQlText;

/// The maximum TraceQL **expression** length, in bytes.
///
/// Reference: grafana/tempo v3.0.2, `modules/frontend/config.go:141`
/// (`cfg.MaxQueryExpressionSizeBytes = 128 * 1024`), enforced at
/// `modules/frontend/pipeline/async_query_validator_middleware.go:45` as
/// `if len(traceQLQuery) > c.maxQuerySizeBytes`. An **inclusive maximum**:
/// a query of exactly this many bytes is ACCEPTED.
///
/// The reference exposes this as a frontend config key
/// (`max_query_expression_size_bytes`); PulsusDB pins the reference's
/// default as a constant, the same treatment `pulsus_logql::MAX_QUERY_BYTES`
/// gets. Making it tunable is a config-surface question, not a parity one.
///
/// NOT to be confused with `pulsus_read::MAX_QUERY_TEXT_BYTES` (8 MiB),
/// which bounds the **rendered ClickHouse SQL** — a different quantity at
/// a different layer.
pub(crate) const MAX_QUERY_EXPRESSION_BYTES: usize = 128 * 1024;

/// The one rejection this module can produce. `400 bad_data` via the
/// search / metrics parameter errors that carry it.
#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum QueryTextError {
    /// Wording follows the reference's own
    /// (`async_query_validator_middleware.go:46`), with the cap
    /// substituted.
    #[error(
        "TraceQL expression exceeds the configured maximum size of {cap} bytes, reduce the \
         query expression size or contact your system administrator"
    )]
    TooLong { len: usize, cap: usize },
}

/// **Leaf module — its entire contents are the checked query text, its
/// constructor and its one accessor. Nothing else may be added here.**
/// Every item inside can name `0` and could therefore fabricate an
/// unchecked [`TraceQlText`]; every item outside provably cannot.
/// Measured in this file: a `TraceQlText(raw.to_string())` written in the
/// test module below fails with `E0423` — "cannot initialize a tuple
/// struct which contains private fields" — because the tuple field is
/// private to `leaf`.
mod leaf {
    use super::{MAX_QUERY_EXPRESSION_BYTES, QueryTextError};

    /// A TraceQL expression that has passed the
    /// [`MAX_QUERY_EXPRESSION_BYTES`] admission check.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct TraceQlText(String);

    impl TraceQlText {
        /// The ONLY constructor. `raw` is the client's `q`/`query`
        /// parameter, already percent-decoded — the reference measures
        /// the decoded value too, since Go's `url.Values.Get` decodes
        /// before the length comparison.
        pub(crate) fn new(raw: &str) -> Result<Self, QueryTextError> {
            // `>`, not `>=` — see the module doc: the reference's bound is
            // an inclusive maximum.
            if raw.len() > MAX_QUERY_EXPRESSION_BYTES {
                return Err(QueryTextError::TooLong {
                    len: raw.len(),
                    cap: MAX_QUERY_EXPRESSION_BYTES,
                });
            }
            Ok(TraceQlText(raw.to_string()))
        }

        pub(crate) fn as_str(&self) -> &str {
            &self.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_of(len: usize) -> String {
        "x".repeat(len)
    }

    /// The boundary, both sides, read from the reference's `>` comparison.
    /// Asserted on the exact byte counts so an off-by-one in either
    /// direction fails: `>=` would reject 131,072 and `> cap+1` would
    /// accept 131,073.
    #[test]
    fn the_cap_is_an_inclusive_maximum_of_131072_bytes() {
        assert_eq!(MAX_QUERY_EXPRESSION_BYTES, 131_072);
        assert!(TraceQlText::new(&text_of(131_071)).is_ok());
        assert!(TraceQlText::new(&text_of(131_072)).is_ok());
        assert_eq!(
            TraceQlText::new(&text_of(131_073)),
            Err(QueryTextError::TooLong {
                len: 131_073,
                cap: 131_072,
            })
        );
    }

    #[test]
    fn an_accepted_expression_round_trips_unmodified() {
        let q = r#"{ resource.service.name = "checkout" }"#;
        assert_eq!(TraceQlText::new(q).expect("under cap").as_str(), q);
    }

    /// The length is measured in BYTES, not chars — a multi-byte query
    /// under the cap in characters but over it in bytes is rejected, as
    /// the reference's `len()` (a byte length in Go too) does.
    #[test]
    fn the_cap_counts_bytes_not_characters() {
        // 65_537 two-byte chars = 131,074 bytes, but only 65,537 chars.
        let q = "é".repeat(65_537);
        assert_eq!(q.chars().count(), 65_537);
        assert_eq!(q.len(), 131_074);
        assert!(matches!(
            TraceQlText::new(&q),
            Err(QueryTextError::TooLong { len: 131_074, .. })
        ));
    }

    #[test]
    fn the_rejection_message_names_the_cap_and_nothing_about_the_input() {
        let err = TraceQlText::new(&text_of(200_000)).expect_err("over cap");
        assert_eq!(
            err.to_string(),
            "TraceQL expression exceeds the configured maximum size of 131072 bytes, reduce \
             the query expression size or contact your system administrator"
        );
    }
}
