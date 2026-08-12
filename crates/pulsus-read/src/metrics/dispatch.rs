//! The metrics read path's **only** ClickHouse dispatch (issue #280).
//!
//! # Why the handle lives in a leaf module
//!
//! A PromQL label-matcher regex reaches ClickHouse uncompiled *on
//! purpose*: `super::series_where` renders `match(<target>,
//! '(?-s)^(?:pat)$')` with the
//! user's pattern verbatim, because RE2 — ClickHouse's engine, and Go's,
//! and therefore upstream Prometheus's — is the authority for what a
//! matcher may contain, not the Rust `regex` crate this process links
//! (`super::labels`' `FallbackReason::RegexUnsupported`, and the issue
//! #240 `PromqlRe2Fallback` capability token that seals it). The
//! consequence is that a pattern RE2 rejects is *only* diagnosable here,
//! from ClickHouse's reply — and Prometheus answers that input with **400
//! `bad_data`**, so a raw [`ChError::Server`] passthrough (500 `internal`)
//! is a status divergence on a surface clients branch on.
//!
//! The fix has to be an invariant, not a patched site: **every** metrics
//! ClickHouse reply must pass through [`map_metrics_read_error`].
//!
//! **Private is a scope, not a restriction** (issue #280 review, finding
//! 1). A private field on a struct declared *here* would still be
//! reachable from every other function, method and `impl` written
//! anywhere in this file — so a second raw dispatch added below would
//! compile and skip the mapper. The handle therefore lives in the [`leaf`]
//! submodule, whose ENTIRE contents are the field, its constructor and
//! its one legitimate accessor. Everything else in this file — the
//! mapper, the detail extractor, the `Debug` impl, the tests — is outside
//! `leaf` and cannot name `client` at all. Measured, in this file: a
//! second `fn` doing `d.client.query_stream(..)` fails with `E0616`
//! (`ChRowStream` likewise never escapes `leaf`, so the row-drain seam
//! cannot be reached unmapped either); moving that same `fn` inside
//! `leaf` compiles it. Keeping the accessor's return type `Vec<R>` rather
//! than a stream is what makes the second half hold.
//!
//! The label-cache sweep (`super::refresh`) holds its own client and is
//! deliberately outside this seal: it issues one fixed, matcher-free
//! `metric_series` scan, never user text, and its errors are swallowed
//! into `MetricsCacheStats::sweep_failures` rather than surfaced to a
//! client. Issue #398 gives that sweep the same `max_memory_usage` ceiling
//! this path carries (`super::refresh::sweep_settings`) — being outside
//! the ERROR-MAPPING seal is not a reason to be outside the BUDGET, and
//! an unbounded sweep would be the one metrics read competing for server
//! memory with no ceiling at all. Its failure behaviour is unchanged.

use pulsus_clickhouse::ChError;
use pulsus_promql::PromqlError;

use crate::logql::error::{ReadError, TooBroadReason};

pub(super) use leaf::MetricsDispatch;

/// ClickHouse's `CANNOT_COMPILE_REGEXP`. Raised on the metrics read path
/// only by a `match()` predicate, and every `match()` this path renders
/// takes its pattern from a user label matcher (`super::sql`'s
/// `matcher_predicate`/`metric_name_predicate` — the module's four
/// `match(` sites, none of them a server-authored constant), so the code
/// is an unambiguous "the client's regex is invalid".
const CODE_CANNOT_COMPILE_REGEXP: i32 = 427;

/// ClickHouse's `MEMORY_LIMIT_EXCEEDED` (issue #398). Raised on the
/// metrics read path only by the `max_memory_usage` ceiling
/// `super::exec::metrics_read_settings` sets from
/// `reader.promql_read_max_memory_bytes` — before #398 no metrics read set
/// a memory limit at all, so this code fell through to
/// [`ReadError::Clickhouse`] and a client saw `500 internal` carrying the
/// raw server exception.
const CODE_MEMORY_LIMIT_EXCEEDED: i32 = 241;

/// ClickHouse frames the RE2 rejection as
/// `Code: 427. DB::Exception: OptimizedRegularExpression: cannot compile
/// re2: <anchored pattern>, error: <re2's reason>. Look at <url> ...
/// while executing 'FUNCTION match(JSONExtractString(__table1.labels,
/// ...)' ... (CANNOT_COMPILE_REGEXP) (version 24.8...)`.
const RE2_REJECT_PREFIX: &str = "cannot compile re2: ";
/// The start of ClickHouse's own trailing advice — the cut point for
/// [`re2_reject_detail`], which keeps only the pattern and RE2's reason.
const RE2_REJECT_SUFFIX: &str = ". Look at ";

/// What a `CANNOT_COMPILE_REGEXP` body renders as when it does not carry
/// the shape [`re2_reject_detail`] recognises (issue #280 review, finding
/// 2). Deliberately says nothing about the input: the tail of a real 427
/// body carries the rendered SQL fragment and ClickHouse's internal table
/// aliases (`while executing 'FUNCTION match(JSONExtractString(
/// __table1.labels, 'job'_String) ...`), so echoing an *unparsed* body to
/// a 400 would disclose query internals. The status is unaffected — the
/// classification is by numeric code alone.
const RE2_REJECT_OPAQUE_DETAIL: &str =
    "the storage engine could not compile a label-matcher regex (RE2 syntax)";

/// Issue #324: `super::sql` prefixes every pattern it renders with RE2's
/// `(?-s)` flag group, because ClickHouse's `match()` otherwise lets `.`
/// match a newline. ClickHouse echoes the pattern it was handed, so the
/// flag would reach the client body — and the in-process route
/// (`super::re2_authority::first_invalid_regex_detail`) quotes the user's
/// pattern without it. Stripping it here keeps the two routes rendering
/// ONE `invalid regexp: ^(?:…)$, error: …` body, and keeps the quoted
/// pattern the one the client actually sent.
const RE2_SQL_FLAG_PREFIX: &str = "(?-s)";

/// Extracts the client-facing core — the pattern RE2 was handed plus
/// RE2's own reason — from a `CANNOT_COMPILE_REGEXP` body, dropping
/// ClickHouse's `Code:`/`DB::Exception:` framing, its documentation
/// pointer, the executed-SQL tail and its version banner.
///
/// **Fail-closed on shape.** BOTH delimiters must be present, in order,
/// with a non-empty core between them; anything else — a future
/// ClickHouse rewording, a truncated or proxy-mangled body — renders
/// [`RE2_REJECT_OPAQUE_DETAIL`] and discloses nothing. Requiring the
/// SUFFIX is the load-bearing half: without it a truncated body would
/// hand back everything after `cannot compile re2: `, which is exactly
/// the executed-SQL tail.
fn re2_reject_detail(message: &str) -> String {
    let Some(start) = message.find(RE2_REJECT_PREFIX) else {
        return RE2_REJECT_OPAQUE_DETAIL.to_string();
    };
    let rest = &message[start + RE2_REJECT_PREFIX.len()..];
    let Some(end) = rest.find(RE2_REJECT_SUFFIX) else {
        return RE2_REJECT_OPAQUE_DETAIL.to_string();
    };
    let core = rest[..end].trim();
    if core.is_empty() {
        return RE2_REJECT_OPAQUE_DETAIL.to_string();
    }
    // Only the flag WE render is stripped, and only where we render it —
    // at the very start. A `(?-s)` the user wrote inside their own pattern
    // sits after the `^(?:` anchor and is untouched.
    core.strip_prefix(RE2_SQL_FLAG_PREFIX)
        .unwrap_or(core)
        .to_string()
}

/// The metrics path's `ChError` mapper, mirroring
/// `logql::exec::map_read_error` and `traces::exec::map_trace_read_error`:
/// two server codes are translated to structured, client-facing
/// [`ReadError`]s, and **every** other error passes through as
/// [`ReadError::Clickhouse`] unmapped — never reinterpreted as a timeout
/// or vice versa.
///
/// Issue #398 adds code 241 `MEMORY_LIMIT_EXCEEDED`, raised by the
/// `max_memory_usage` ceiling `super::exec::metrics_read_settings` now
/// carries, mapped to
/// [`crate::logql::error::TooBroadReason::PromqlReadMemory`] → `422
/// execution`. Checked FIRST because 241 is a budget refusal and 427 is a
/// client-regex rejection; the two codes are disjoint, so the order is
/// documentation rather than precedence.
///
/// **The #412 rule** (see `logql::exec::map_read_error`'s doc for the full
/// argument): `ChError::Server.code` used to be parsed out of the exception
/// text on the streaming path and was therefore spoofable by tenant bytes.
/// Issue #412 closed that on any server declaring
/// `X-ClickHouse-Exception-Tag` — 26.3, our floor — leaving the search only
/// for an untagged, out-of-support server. The reasoning that made this
/// mapper safe meanwhile is unchanged and still correct: the BOUND is
/// enforced by ClickHouse regardless of the parse; a missed 241 falls open to
/// the pre-#398 `500`; a false 241 only relabels an already-failing query;
/// and this mapper is a pure function of the already-parsed `code` for the
/// memory arm, so it inherited #412's fix with no edit here.
fn map_metrics_read_error(e: ChError, read_max_memory_bytes: u64) -> ReadError {
    if let ChError::Server {
        code: CODE_MEMORY_LIMIT_EXCEEDED,
        ..
    } = &e
    {
        return ReadError::QueryTooBroad(TooBroadReason::PromqlReadMemory {
            budget_bytes: read_max_memory_bytes,
        });
    }
    if let ChError::Server {
        code: CODE_CANNOT_COMPILE_REGEXP,
        message,
    } = &e
    {
        return ReadError::Promql(PromqlError::InvalidRegexMatcher {
            detail: re2_reject_detail(message),
        });
    }
    ReadError::Clickhouse(e)
}

/// `ChClient` is not `Debug` (it owns a connection pool), so the seal is
/// given a hand-written one rather than dropping the trait. Written
/// OUTSIDE [`leaf`] on purpose: it therefore cannot name `client`, which
/// is also the only rendering that cannot leak connection credentials
/// into a log line.
impl std::fmt::Debug for MetricsDispatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsDispatch").finish_non_exhaustive()
    }
}

/// **Leaf module — its entire contents are the ClickHouse handle, its
/// constructor and its one legitimate accessor. Nothing else may be added
/// here** (issue #280 review, finding 1): every item inside this module
/// can reach `client` and could dispatch around
/// [`map_metrics_read_error`], and every item outside it provably cannot.
/// New helpers belong in the parent module.
mod leaf {
    use futures::StreamExt;
    use pulsus_clickhouse::{ChClient, ChRow, ChRowStream, QuerySettings};

    use crate::logql::error::ReadError;
    use crate::logql::exec::escape_query_placeholders;
    use crate::metrics::exec::SampleBudget;

    use super::map_metrics_read_error;

    /// The sealed owner of the metrics engine's ClickHouse handle.
    pub(crate) struct MetricsDispatch {
        client: ChClient,
        /// Issue #398: `reader.promql_read_max_memory_bytes`, carried here
        /// only so [`super::map_metrics_read_error`] can name the budget a
        /// code-241 breach broke. A constructor-set `u64` cannot dispatch,
        /// so the leaf module's "nothing else may be added here" rule
        /// (issue #280 review, finding 1) still holds: the field, its
        /// constructor argument and its single read inside
        /// `fetch_rows_with`'s error mapping are all it can ever do.
        read_max_memory_bytes: u64,
    }

    impl MetricsDispatch {
        pub(crate) fn new(client: ChClient, read_max_memory_bytes: u64) -> Self {
            MetricsDispatch {
                client,
                read_max_memory_bytes,
            }
        }

        /// Wraps [`ChClient::query_stream`] with the placeholder-escaping
        /// fix [`escape_query_placeholders`] applies — the `SqlFallback`
        /// sub-query's `^(?:...)$` regex predicates always carry a literal
        /// `?`, and the `clickhouse` crate's `SqlBuilder` treats a bare
        /// `?` as an unbound bind placeholder unless doubled. Still no
        /// scan-budget concept in M2's metrics scope (unlike
        /// `logql::exec`'s own `query_stream` wrapper) — that stays a
        /// standing out-of-scope decision. Issue #35 closes a live gap:
        /// this path previously sent NO settings at all, so a broad
        /// selector's rendered `IN` lists could trip ClickHouse's
        /// 262,144-byte `max_query_size` default with an opaque parse
        /// error — now every dispatch carries a settings object AND is
        /// guarded pre-dispatch by
        /// [`crate::querytext::ensure_query_text_fits`] (checked against
        /// the FINAL escaped text, same ordering `logql::exec` uses).
        /// Issue #136 threads the settings in explicitly (rather than
        /// always computing `metrics_read_settings` internally) so the
        /// `SqlFallback` fetches can carry the extra
        /// `distributed_product_mode` setting without a second,
        /// near-duplicate dispatch method.
        ///
        /// Issue #138: `budget` is `Some` on the six sample dispatches
        /// only (the same `Option` seam precedent as `FetchProbe`) and is
        /// charged per row INSIDE the drain loop, before the push — the
        /// guard bounds actual materialization, aborting (and dropping the
        /// `ChRowStream`, releasing its pooled-connection lease) on the
        /// first over-cap row, never a post-hoc total. Cost when charged:
        /// one relaxed `fetch_add` and compare per row, dwarfed by the
        /// poll, RowBinary decode, and `Vec` push already on this loop;
        /// when `None`, one predictable branch.
        ///
        /// Issue #280: BOTH `ChError` seams — the dispatch and each
        /// drained row — go through [`map_metrics_read_error`]. A
        /// ClickHouse HTTP exception can surface at either (the response
        /// head, or mid-body once rows have begun streaming), so mapping
        /// only the first would leave the 500 reachable. Returning `Vec<R>`
        /// rather than the stream keeps the second seam inside the seal:
        /// no caller can obtain a `ChRowStream` to drain unmapped.
        pub(crate) async fn fetch_rows_with<R: ChRow>(
            &self,
            sql: String,
            settings: &QuerySettings,
            budget: Option<&SampleBudget>,
        ) -> Result<Vec<R>, ReadError> {
            let sql = escape_query_placeholders(&sql);
            if let Err(reason) = crate::querytext::ensure_query_text_fits(&sql) {
                return Err(ReadError::QueryTooBroad(reason));
            }
            let mut stream: ChRowStream<'_, R> = self
                .client
                .query_stream::<R>(&sql, settings)
                .await
                .map_err(|e| map_metrics_read_error(e, self.read_max_memory_bytes))?;
            let mut out = Vec::new();
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| map_metrics_read_error(e, self.read_max_memory_bytes))?;
                if let Some(b) = budget {
                    b.charge_one().map_err(ReadError::QueryTooBroad)?;
                }
                out.push(row);
            }
            Ok(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #398: a distinctive, non-default
    /// `reader.promql_read_max_memory_bytes` for these tests.
    const TEST_READ_MEM: u64 = 7_654_321;

    /// The body ClickHouse 24.8.14.39 actually returned for the metrics
    /// discovery query in
    /// `live_metrics_engine::an_re2_rejected_matcher_regex_is_a_client_
    /// rejection_not_a_server_error`, captured verbatim — including the
    /// `while executing 'FUNCTION match(...)'` tail, which is what makes
    /// the "keep only the pattern and RE2's reason" cut load-bearing
    /// rather than cosmetic (the tail names internal table aliases).
    ///
    /// **Archived capture (issue #376).** 24.8.14.39 is no longer a version
    /// we run. These bytes stay as captured — rewriting them to say `26.3`
    /// would falsify a measurement — and the version-leak assertion below
    /// derives its forbidden string FROM this fixture, so it follows
    /// whatever server a future re-capture came from.
    const LIVE_427_BODY: &str = "Code: 427. DB::Exception: OptimizedRegularExpression: \
        cannot compile re2: ^(?:\\p{Alphabetic})$, error: invalid character class range: \
        \\p{Alphabetic}. Look at https://github.com/google/re2/wiki/Syntax for reference. \
        Please note that if you specify regex as an SQL string literal, the slashes have to \
        be additionally escaped. For example, to match an opening brace, write '\\(' -- the \
        first slash is for SQL and the second one is for regex: while executing 'FUNCTION \
        match(JSONExtractString(__table1.labels, 'job'_String) :: 5, \
        '^(?:\\\\p{Alphabetic})$'_String :: 2) -> \
        match(JSONExtractString(__table1.labels, 'job'_String), \
        '^(?:\\\\p{Alphabetic})$'_String) UInt8 : 1': While executing \
        MergeTreeSelect(pool: ReadPoolInOrder, algorithm: InOrder). \
        (CANNOT_COMPILE_REGEXP) (version 24.8.14.39 (official build))";

    /// Fragments of a real 427 body that must never reach a client.
    ///
    /// **No version spelling appears here (issue #376).** It used to carry
    /// `"version 24.8"`, which made the claim "no server version string
    /// leaks" true only of one server's spelling: on 26.3 that substring
    /// never appears in any body, so the entry would have passed while
    /// testing nothing. The version half of the claim is checked by
    /// [`version_banner_of`] instead, which derives the forbidden string
    /// from the fixture the test is actually rendering.
    const MUST_NOT_LEAK: &[&str] = &[
        "DB::Exception",
        "Code: 427",
        "wiki/Syntax",
        "OptimizedRegularExpression",
        "__table1",
        "MergeTreeSelect",
        "JSONExtractString",
        "while executing",
    ];

    /// The `(version <x.y.z.w> (official build))` banner a server body
    /// carries, extracted FROM that body rather than spelled out — so the
    /// leak assertion's forbidden string follows whatever server produced
    /// the fixture and can never go vacuous on a version bump.
    ///
    /// Returns both the whole banner and the bare version number, because
    /// a partial leak (the number without its parentheses) is still a
    /// leak.
    fn version_banner_of(body: &str) -> (String, String) {
        let start = body
            .rfind("(version ")
            .unwrap_or_else(|| panic!("fixture carries no `(version ...)` banner: {body:?}"));
        let banner = body[start..].to_string();
        let number = banner
            .trim_start_matches("(version ")
            .split_whitespace()
            .next()
            .expect("a version number after `(version `")
            .to_string();
        assert!(
            number.chars().next().is_some_and(|c| c.is_ascii_digit()),
            "extracted {number:?} from {banner:?}, which is not a version number"
        );
        (banner, number)
    }

    fn rejection_detail(message: &str) -> String {
        let mapped = map_metrics_read_error(
            ChError::Server {
                code: 427,
                message: message.to_string(),
            },
            TEST_READ_MEM,
        );
        match mapped {
            ReadError::Promql(PromqlError::InvalidRegexMatcher { detail }) => detail,
            other => panic!("expected InvalidRegexMatcher, got {other:?}"),
        }
    }

    #[test]
    fn cannot_compile_regexp_maps_to_the_promql_invalid_regex_rejection() {
        assert_eq!(
            rejection_detail(LIVE_427_BODY),
            "^(?:\\p{Alphabetic})$, error: invalid character class range: \\p{Alphabetic}"
        );
    }

    /// The rendered client message: no `Code:`/`DB::Exception:` framing,
    /// no RE2 wiki pointer, no ClickHouse version banner, no internal
    /// table aliases, no executed-SQL fragment. Rendered from the INNER
    /// `PromqlError` — that is what reaches the wire, since
    /// `prom_api::error::read_error_parts` delegates
    /// `ReadError::Promql(inner)` to `promql_error_parts(inner)` and never
    /// renders the `ReadError` wrapper's own `promql: ` prefix.
    #[test]
    fn rendered_rejection_drops_clickhouse_framing_pointer_and_version() {
        let rendered = PromqlError::InvalidRegexMatcher {
            detail: rejection_detail(LIVE_427_BODY),
        }
        .to_string();
        // The leak assertions run FIRST, so a leak is reported as a leak
        // rather than as a diff against the expected rendering. The
        // equality below is the stronger statement and closes the test.
        for leak in MUST_NOT_LEAK {
            assert!(
                !rendered.contains(leak),
                "{leak:?} leaked into {rendered:?}"
            );
        }

        // The version half of the claim, derived from the fixture rather
        // than spelled (issue #376): whatever server produced
        // `LIVE_427_BODY`, neither its banner nor its bare version number
        // may survive the cut. A version bump moves the fixture and this
        // assertion follows it; it cannot go vacuous.
        let (banner, number) = version_banner_of(LIVE_427_BODY);
        assert!(
            !rendered.contains(&banner),
            "the server version banner {banner:?} leaked into {rendered:?}"
        );
        assert!(
            !rendered.contains(&number),
            "the server version {number:?} leaked into {rendered:?}"
        );

        assert_eq!(
            rendered,
            "invalid regexp: ^(?:\\p{Alphabetic})$, error: invalid character class range: \
             \\p{Alphabetic}"
        );
    }

    /// Issue #324: the SQL renderer's `(?-s)` prefix is ClickHouse-facing
    /// only — the client sees the pattern it sent, in the same shape the
    /// in-process route (`re2_authority::first_invalid_regex_detail`)
    /// produces, so one input cannot render two different bodies depending
    /// on which path answered it.
    #[test]
    fn the_sql_paths_dot_semantics_flag_never_reaches_the_client() {
        let body = LIVE_427_BODY.replace("cannot compile re2: ^", "cannot compile re2: (?-s)^");
        assert!(body.contains("(?-s)"), "premise: the flag is in the body");
        assert_eq!(
            rejection_detail(&body),
            "^(?:\\p{Alphabetic})$, error: invalid character class range: \\p{Alphabetic}"
        );

        // A `(?-s)` the USER wrote is inside their own pattern, after the
        // anchor, and must survive verbatim.
        let user_flag = LIVE_427_BODY.replace(
            "cannot compile re2: ^(?:\\p{Alphabetic})$",
            "cannot compile re2: (?-s)^(?:(?-s)\\p{Alphabetic})$",
        );
        assert_eq!(
            rejection_detail(&user_flag),
            "^(?:(?-s)\\p{Alphabetic})$, error: invalid character class range: \\p{Alphabetic}"
        );
    }

    /// **Issue #412 AC6** (the folded #410): a tenant who plants the
    /// delimiters *inside their own pattern* cannot move the extracted core.
    ///
    /// [`re2_reject_detail`] `find`s the FIRST `cannot compile re2: `, and
    /// ClickHouse echoes the rejected pattern **after** that marker, so a
    /// planted copy is always a LATER occurrence than the real one and
    /// first-occurrence still wins. The exposure #410 recorded was the other
    /// direction — result bytes preceding the exception, which put the real
    /// occurrence after tenant text — and that shape is what #412 removes from
    /// the streaming path: on a tagged response the exception arrives in its
    /// own frame, sliced by declared length, with no result bytes in it.
    ///
    /// This pins the property that makes the remaining search safe. It is
    /// cheap and it fails loudly if the rule is ever changed to last-occurrence
    /// or to a search over a wider region.
    #[test]
    fn a_planted_re2_prefix_in_the_pattern_does_not_move_the_extracted_core() {
        // The tenant's pattern carries a whole second prefix+suffix pair.
        let planted = LIVE_427_BODY.replace(
            "cannot compile re2: ^(?:\\p{Alphabetic})$",
            "cannot compile re2: ^(?:cannot compile re2: EVIL. Look at )$",
        );
        assert_eq!(
            planted.matches(RE2_REJECT_PREFIX).count(),
            2,
            "premise: the body now contains the marker twice"
        );

        // Both delimiters are first-occurrence, so a planted pair can only
        // truncate the tenant's OWN echoed pattern. It cannot move where the
        // core starts, and it cannot make the planted core the answer.
        let detail = rejection_detail(&planted);
        assert_eq!(
            detail, "^(?:cannot compile re2: EVIL",
            "the core still starts at the REAL marker; the planted suffix only \
             cut the tenant's own pattern short"
        );
        assert_ne!(detail, "EVIL");
        assert!(
            !detail.starts_with("EVIL"),
            "a last-occurrence rule would return the planted core: {detail:?}"
        );
        assert!(
            detail.starts_with("^(?:"),
            "the extraction is anchored at the first marker, which is the \
             server's own: {detail:?}"
        );
    }

    /// Issue #280 review, finding 2: an unrecognised body is NOT echoed.
    /// Classification is still by code alone — it rejects as a client
    /// error — but the detail discloses nothing about the input.
    #[test]
    fn an_unrecognised_427_body_is_never_echoed_to_the_client() {
        for message in [
            // A future rewording with no recognised delimiters at all.
            "Code: 427. DB::Exception: regex rejected while executing 'FUNCTION \
             match(JSONExtractString(__table1.labels, 'job'_String))'",
            // The dangerous shape: prefix present, advice suffix absent
            // (truncated/proxy-mangled body). Echoing "everything after
            // the prefix" here would hand back the executed-SQL tail.
            "Code: 427. cannot compile re2: ^(?:x)$, error: bad: while executing 'FUNCTION \
             match(JSONExtractString(__table1.labels, 'job'_String))'",
            // Recognised delimiters, empty core.
            "Code: 427. cannot compile re2: . Look at https://example/ for reference.",
        ] {
            let detail = rejection_detail(message);
            assert_eq!(detail, RE2_REJECT_OPAQUE_DETAIL);
            for leak in MUST_NOT_LEAK {
                assert!(!detail.contains(leak), "{leak:?} leaked into {detail:?}");
            }
            assert!(
                !message.contains(&detail),
                "the opaque detail must not be a slice of the server body"
            );
        }
    }

    /// Every other server code passes through unmapped — including the
    /// neighbouring codes, so the classification cannot widen by accident.
    ///
    /// Issue #398 removed 241 from this list: it is now the PromQL read
    /// memory ceiling's breach code and is asserted by
    /// `map_metrics_read_error_maps_241_to_the_promql_read_memory_reason`.
    /// 240 and 242 stay here, so the new arm cannot widen either.
    #[test]
    fn other_server_codes_pass_through_as_clickhouse_errors() {
        for code in [0, 62, 159, 240, 242, 307, 426, 428] {
            let mapped = map_metrics_read_error(
                ChError::Server {
                    code,
                    message: "cannot compile re2: x, error: y. Look at z".to_string(),
                },
                TEST_READ_MEM,
            );
            match mapped {
                ReadError::Clickhouse(ChError::Server { code: got, .. }) => assert_eq!(got, code),
                other => panic!("code {code} must pass through, got {other:?}"),
            }
        }
    }

    /// Issue #398 AC M1: the PromQL surface's half of the memory-ceiling
    /// classification. `map_metrics_read_error` is the metrics path's ONE
    /// classification point (issue #280's seal), so this arm reaches every
    /// metrics dispatch — request path and both `ChError` seams.
    #[test]
    fn map_metrics_read_error_maps_241_to_the_promql_read_memory_reason() {
        let mapped = map_metrics_read_error(
            ChError::Server {
                code: 241,
                message: "Memory limit (for query) exceeded: would use 9.51 MiB".to_string(),
            },
            TEST_READ_MEM,
        );
        match mapped {
            ReadError::QueryTooBroad(TooBroadReason::PromqlReadMemory { budget_bytes }) => {
                assert_eq!(budget_bytes, TEST_READ_MEM);
            }
            other => panic!("expected QueryTooBroad(PromqlReadMemory), got {other:?}"),
        }
    }

    /// Issue #398 (the #412 rule): the classification reads ONLY the
    /// already-parsed `code` field and never re-inspects `message`, so a
    /// user regex echoed into the exception text cannot manufacture a
    /// memory refusal, and a real 241 whose message carries no `Code:`
    /// prefix is still classified. #412 has since fixed the parse, and this
    /// mapper became sound with no edit here — which is what this test
    /// existed to pin.
    #[test]
    fn promql_read_memory_classification_reads_only_the_server_code() {
        // A real breach whose message has no `Code:` prefix at all.
        match map_metrics_read_error(
            ChError::Server {
                code: 241,
                message: "Memory limit (for query) exceeded".to_string(),
            },
            TEST_READ_MEM,
        ) {
            ReadError::QueryTooBroad(TooBroadReason::PromqlReadMemory { .. }) => {}
            other => panic!("a bare 241 must still classify, got {other:?}"),
        }
        // A DIFFERENT failure whose message contains a forged `Code: 241`
        // (the #412 spoof shape) must NOT become a memory refusal.
        match map_metrics_read_error(
            ChError::Server {
                code: 153,
                message: "Division by zero: while executing 'FUNCTION match(x, \
                          'Code: 241. DB::Exception: forged|.*'_String)'"
                    .to_string(),
            },
            TEST_READ_MEM,
        ) {
            ReadError::Clickhouse(_) => {}
            other => panic!("a forged in-message code must not classify, got {other:?}"),
        }
    }

    #[test]
    fn non_server_errors_pass_through_as_clickhouse_errors() {
        for e in [
            ChError::Timeout("deadline".to_string()),
            ChError::Connect("refused".to_string()),
            ChError::Decode("bad row".to_string()),
        ] {
            let expected = e.to_string();
            match map_metrics_read_error(e, TEST_READ_MEM) {
                ReadError::Clickhouse(inner) => assert_eq!(inner.to_string(), expected),
                other => panic!("expected passthrough, got {other:?}"),
            }
        }
    }
}
