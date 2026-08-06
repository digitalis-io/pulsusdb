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
/// than the reference's.** Retention is days to months and nobody
/// queries five years of logs, so this refuses nothing a real deployment
/// does while removing the whole class of absurd-input arithmetic that
/// issue #343 chased down four successive layers. Ledgered as
/// `five-year-span-cap` in docs/benchmarks/logs-differential-ledger.md.
///
/// **How much of it is a divergence, re-measured** (issue #248 round 5,
/// digest-pinned v3.7.4 oracle). This comment used to say the reference
/// "bounds no query span at all"; it does bound one.
/// `max_query_length` defaults to `721h`
/// (`pkg/validation/limits.go:371` @ v3.7.4) over the window
/// `[start - ([range] + offset), end - offset]`
/// (`pkg/querier/queryrange/shard_resolver.go:94-104` @ v3.7.4), so on a
/// range query BOTH the request span and the `[range]` selector are
/// bounded there far tighter than here — `[720h]` over a `1h` request
/// span is already a `400 the query time range exceeds the limit`. The
/// offset cancels in that subtraction and stays unbounded: `offset
/// 2562047h47m16s854ms775us807ns` (`i64::MAX`) is a 200 there, and so is
/// every other value in the domain SAVE ONE. The exception is
/// `i64::MIN`, and its `200` is conditional in a way this comment used
/// to leave out. A frontend that has not already answered the
/// neighbouring value REFUSES it — `400 this data is no longer
/// available`, Go's negation overflowing inside the shard resolver and
/// inverting the window — and with `cache_index_stats_results: false`
/// that `400` is the verdict in every probe order. At the shipped
/// default (true) it is what a cold frontend answers; only a warm
/// index-stats entry written by `i64::MIN + 1` — one nanosecond away,
/// and indistinguishable in the millisecond-resolution request the shard
/// resolver issues — turns a later `i64::MIN` into a 200. So it is an
/// overflow artefact at a single point rather than a magnitude bound
/// (`i64::MIN + 1` is a 200 in any order, in both configurations), but
/// it is a REFUSAL wherever no neighbouring probe has cached it away.
/// Rounds 6 and 7 of issue #248 settled that; the ledger row carries the
/// order-dependent probe table. So this cap diverges at every offset
/// magnitude past 43,800 h — bar the negative endpoint just described,
/// where a cold reference refuses too — and on an INSTANT query's
/// `[range]` (which
/// the reference admits and then splits into per-hour subqueries that do
/// not answer in practice); on a range query it fires only where the
/// reference, at its shipped default, refuses first.
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
