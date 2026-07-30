//! The LogQL query-text admission cap (issue #279).
//!
//! The reference's cap is not an HTTP-layer rule — it is the first
//! statement of the parser, so no entry point can skip it. PulsusDB
//! mirrors that topology and makes it compile-enforced: the lexer's
//! `tokenize` takes [`CheckedQuery`] instead of `&str`, and the only way
//! to construct one is the length check below.

use crate::error::LogQlError;
use crate::token::Span;

/// The maximum LogQL **query source** length, in bytes.
///
/// Reference: grafana/loki v3.7.4, `pkg/logql/syntax/parser.go:42`
/// (`const maxInputSize = 131072`), enforced at `:86` as
/// `if len(input) >= maxInputSize`. The bound is therefore an **exclusive
/// maximum**: a query of exactly `MAX_QUERY_BYTES` bytes is REJECTED and
/// the longest accepted query is `MAX_QUERY_BYTES - 1` = 131,071 bytes.
/// The reference's constant is compile-time, not tenant-configurable
/// (`pkg/validation/limits.go` carries no query-text limit), so this is a
/// constant here too.
///
/// NOT to be confused with `pulsus_read::MAX_QUERY_TEXT_BYTES` (8 MiB),
/// which bounds the **rendered ClickHouse SQL**, a different quantity at a
/// different layer.
pub const MAX_QUERY_BYTES: usize = 131_072;

/// A query string that has passed the [`MAX_QUERY_BYTES`] admission check.
///
/// The field is private to this module and [`CheckedQuery::new`] is the
/// only constructor, so a token stream cannot be produced from unchecked
/// input: `lexer::tokenize` takes this type, and every public parse entry
/// point must therefore run the check to call it. This is a
/// compile-enforced invariant, not a convention — a new entry point that
/// forgets the cap does not compile.
pub(crate) struct CheckedQuery<'a>(&'a str);

impl<'a> CheckedQuery<'a> {
    pub(crate) fn new(input: &'a str) -> Result<Self, LogQlError> {
        if input.len() >= MAX_QUERY_BYTES {
            return Err(LogQlError::QueryTooLong {
                len: input.len(),
                cap: MAX_QUERY_BYTES,
                span: Span { start: 0, end: 0 },
            });
        }
        Ok(CheckedQuery(input))
    }

    pub(crate) fn as_str(&self) -> &'a str {
        self.0
    }
}
