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

/// **NOTHING IN A LogQL QUERY MAY SPAN MORE THAN 5 YEARS** (43,800 h =
/// 5 × 365 d), in nanoseconds. ONE rule, three places:
///
/// 1. `offset <duration>` — magnitude, in EITHER direction
///    ([`crate::parse`]).
/// 2. The `[range]` selector ([`crate::parse`]).
/// 3. The query's own `start`-to-`end` span
///    (`pulsus_read::logql::plan`, which reads this constant rather than
///    restating it).
///
/// One nanosecond more is a `400 bad_data` echoing the value the user
/// sent, never a clamped value: someone asking for a stupid number gets
/// told plainly rather than silently handed a different answer.
///
/// **A DELIBERATE DIVERGENCE — the only limit here that is ours rather
/// than the reference's.** grafana/loki v3.7.4 accepts an offset and a
/// range anywhere in the `i64` nanosecond domain, and bounds no query
/// span at all — measured on the digest-pinned oracle:
/// `offset 2562047h47m16s854ms775us807ns` (`i64::MAX`) is a 200 there, as
/// is `offset -9223372036854775808ns` (`i64::MIN`), and `[2562047h]` (a
/// 292-year window) is a 200. Retention is days to months and nobody
/// queries five years of logs, so this refuses nothing a real deployment
/// does while removing the whole class of absurd-input arithmetic that
/// issue #343 chased down four successive layers. Ledgered as
/// `five-year-span-cap` in docs/benchmarks/logs-differential-ledger.md.
///
/// Same status as [`MAX_QUERY_BYTES`] — `400 bad_data` — and the two
/// literal forms are enforced at the same layer it is, the parser, so no
/// entry point can skip them.
pub const MAX_QUERY_SPAN_NS: i64 = 157_680_000_000_000_000;

/// [`MAX_QUERY_SPAN_NS`] in whole hours (43,800) — the figure the
/// rejection messages quote. Derived, so the constant and every message
/// that names it cannot drift apart.
pub const MAX_QUERY_SPAN_HOURS: i64 = MAX_QUERY_SPAN_NS / 3_600_000_000_000;

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
