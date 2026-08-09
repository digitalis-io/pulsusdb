//! `ChError` taxonomy: retryable-vs-poison is explicit and classifier-driven
//! (not left to caller inspection of error text), and retry eligibility for
//! maintenance statements is encoded in the [`Idempotency`] type rather than
//! left to prose (issue #3 amendment, Codex finding 3).

use thiserror::Error;

/// Errors from `pulsus-clickhouse`. Every variant carries enough context
/// that a caller can decide whether to retry, alert, or surface to the
/// user without re-parsing the message.
#[derive(Debug, Error)]
pub enum ChError {
    /// Failed to establish or re-establish a connection. Retryable.
    #[error("connect: {0}")]
    Connect(String),
    /// A client-side or server-side deadline was hit. Retryable.
    #[error("timeout: {0}")]
    Timeout(String),
    /// A transport-level I/O failure (reset connection, broken pipe, ...). Retryable.
    #[error("io: {0}")]
    Io(String),
    /// A ClickHouse server exception with an explicit numeric code
    /// (`DB::Exception` `Code: N`). Retryability is classified by `code`,
    /// see [`ChError::is_retryable`].
    #[error("server [{code}]: {message}")]
    Server { code: i32, message: String },
    /// A row failed to (de)serialize, or a query result did not match the
    /// expected shape. Poison: retrying an identical request reproduces it.
    #[error("decode: {0}")]
    Decode(String),
    /// An invalid [`crate::ChConnConfig`] or invariant violation. Poison.
    #[error("config: {0}")]
    Config(String),
    /// A block insert whose commit fate is UNKNOWN because it was aborted by
    /// a timeout or transient transport fault mid-flight. NEVER retryable:
    /// the server may have (partially) committed the block, so a retry
    /// duplicates rows and permanently inflates tier aggregates
    /// (docs/schemas.md §2.2/§8).
    #[error("insert uncertain (may have partially committed): {0}")]
    InsertUncertain(String),
}

/// Explicit retryable ClickHouse server error codes (poison otherwise).
/// Transient/availability faults where retrying the *same* request may
/// succeed once the server recovers.
const RETRYABLE_SERVER_CODES: &[i32] = &[
    209, // SOCKET_TIMEOUT
    210, // NETWORK_ERROR
    279, // ALL_CONNECTION_TRIES_FAILED
    202, // TOO_MANY_SIMULTANEOUS_QUERIES
    159, // TIMEOUT_EXCEEDED
    425, // SYSTEM_ERROR (transient subset)
];

impl ChError {
    /// True only for transient faults where retrying the *same idempotent*
    /// operation may succeed. Poison errors (bad SQL, schema mismatch,
    /// resource-limit-without-relief) are never retryable — retrying them
    /// reproduces the same failure and wastes the retry budget.
    pub fn is_retryable(&self) -> bool {
        match self {
            ChError::Connect(_) | ChError::Timeout(_) | ChError::Io(_) => true,
            ChError::Server { code, .. } => RETRYABLE_SERVER_CODES.contains(code),
            ChError::Decode(_) | ChError::Config(_) | ChError::InsertUncertain(_) => false,
        }
    }

    /// Parses a ClickHouse HTTP error body for the numeric exception code
    /// (the `clickhouse` crate does not expose it as a typed field, only
    /// embedded in `Error::BadResponse`'s message text). Falls back to `0`
    /// — never in [`RETRYABLE_SERVER_CODES`], and in no caller's code
    /// table — when no code can be believed.
    ///
    /// **The code is not always at byte 0** (issue #382). When a query has
    /// already written output, ClickHouse emits the exception into the same
    /// response stream, and the crate's buffered-error path hands the whole
    /// trimmed body back as one `BadResponse` message (`collect_bad_response`,
    /// `response.rs:127-163` @ clickhouse 0.15.1), so the message is
    /// `<already-written result bytes>Code: N. DB::Exception: …`. A
    /// `strip_prefix("Code: ")` read fell back to `0` there, and every
    /// consumer of the code — `traces::exec::map_trace_read_error`'s
    /// 396/307 arms, `logql::exec::map_read_error`'s 307 arm,
    /// `bookkeeping::is_missing_table`, [`ChError::is_retryable`] — silently
    /// lost its classification. The user-visible symptom was a 500
    /// `internal` where the query deserved a 422 `query_too_broad`.
    ///
    /// Every shape below was measured through this client (default LZ4
    /// compression) against the pinned images; see
    /// `parse_exception_code`'s tests for the verbatim bodies.
    ///
    /// | shape | where the code sits | measured on |
    /// |---|---|---|
    /// | `Code: N. DB::Exception: …` | byte 0 | 24.8.14.39 and 26.3.17.110 |
    /// | `Code: N` (no framing at all — the crate's header-derived `reason()`, `response.rs:179-187`) | byte 0 | 26.3.17.110 |
    /// | `<result bytes>Code: N. DB::Exception: …` | byte 10 … 1_262_489 | 26.3.17.110 |
    ///
    /// The third shape is the defect. It is a property of the *server*, not
    /// of any one code: 24.8 discards its not-yet-flushed output buffer when
    /// it turns the response into an error (the compressed body carries a
    /// single block, the exception), while 26.3 keeps it (two blocks: the
    /// written result, then the exception). No 24.8 shape that produced it
    /// was found, so on 24.8 this parse is a pin and on 26.3 it is the fix.
    ///
    /// See [`parse_exception_code`] for the search rule.
    pub(crate) fn server_from_bad_response(message: String) -> ChError {
        let code = parse_exception_code(&message).unwrap_or(0);
        ChError::Server { code, message }
    }
}

/// The marker ClickHouse writes ahead of an exception's numeric code, in
/// both a standalone error body and the copy it appends after output that
/// has already been written.
const EXCEPTION_CODE_MARKER: &str = "Code: ";

/// The framing that follows the digits in a ClickHouse-authored exception.
/// Measured through this client on both 24.8.14.39 and 26.3.17.110, over
/// thirteen error classes, in exactly two renderings:
/// `Code: N. DB::Exception:` (43, 47, 60, 62, 81, 158, 160, 191, 241, 307,
/// 395, 396, 427) and `Code: N. DB::NetException:` (the 519/279/210 of a
/// read through a dead `remote()` shard). `. DB::` is the common prefix,
/// and it is the same evidence the crate's own streaming extractor demands
/// before it will believe a chunk carries an exception at all
/// (`extract_exception_old`, `response.rs:368-377` @ clickhouse 0.15.1:
/// the tail must contain both `DB::` and `Exception:`).
const EXCEPTION_CODE_FRAME: &str = ". DB::";

/// Finds ClickHouse's exception code in an HTTP error body.
///
/// Scans left to right and returns the FIRST `Code: <ascii digits>` that is
/// believable, where believable means either
///
/// 1. it sits at byte 0 — the shape of a body carrying nothing but the
///    exception, and also of the crate's header-derived fallback
///    (`reason()`, `response.rs:179-187` @ clickhouse 0.15.1), which is the
///    bare `Code: 396` with no framing at all; or
/// 2. the digits are followed by [`EXCEPTION_CODE_FRAME`] — the framing
///    only a real ClickHouse exception carries.
///
/// Arm 1 is what keeps this compatible with the `strip_prefix("Code: ")`
/// read it replaces: **for every body whose byte 0 begins `Code: ` followed
/// by a run of ASCII digits that fits `i32` — the shape of every body
/// measured across thirteen error classes on both pinned versions — this
/// returns exactly what `strip_prefix` returned**, so no classification
/// that is correct today moves. The two bodies where
/// the two reads differ are `Code: -5.` and `Code: +5.`: the old read took
/// them as -5 and 5, this one as `None` (0). ClickHouse emits neither, no
/// code table in this workspace holds a negative code, and both are pinned
/// by a test so the divergence is recorded rather than assumed away.
/// Arm 2 is what makes issue #382's post-flush body classify.
///
/// What the other cases do, deliberately:
///
/// - **Nested exceptions.** A failed distributed read renders the outermost
///   code first and its causes after (`Code: 519. DB::NetException: …
///   Code: 279 … Code: 210 …`). First-match returns 519, what actually
///   failed, and the same code `strip_prefix` returned — the outermost one
///   is the prefix.
/// - **Result bytes that contain the framing.** The already-written result
///   precedes the appended exception, so a stored log line or span name
///   holding literal `Code: 42. DB::Exception: …` text wins over the real
///   exception. Accepted, and pinned by a test. The alternative — taking
///   the LAST match, as the crate's own streaming extractor does
///   (`response.rs:369` @ 0.15.1, `rfind`) — would return the innermost 210
///   of the nested case above, moving a classification that is correct
///   today, and 210 is in [`RETRYABLE_SERVER_CODES`] while 519 is not.
/// - **`Code: N` with no framing, past byte 0.** Rejected. Result bytes are
///   far likelier to contain the two words than the whole `Code: N. DB::`
///   shape.
/// - **A truncated tail.** `…Code: 39` at the end is rejected (no framing);
///   `…Code: 396. DB::` truncated right there is accepted — the framing is
///   what carries the meaning, not the description.
/// - **Non-ClickHouse framings.** Poco's `Poco::Exception. Code: 1000,
///   e.code() = 111, …` has no `. DB::` and is not at byte 0, so it reads
///   as `0` — exactly as before this change.
///
/// Cost: one left-to-right pass (`str::find` is `memchr`-backed; `from`
/// only ever advances, so each byte is scanned once) over a body that, on
/// the buffered path, is as large as the written result — 1.26 MB in the
/// largest measured case. This runs once per failed query, never per row.
fn parse_exception_code(message: &str) -> Option<i32> {
    let mut from = 0usize;
    while let Some(offset) = message[from..].find(EXCEPTION_CODE_MARKER) {
        let at = from + offset;
        let after_marker = at + EXCEPTION_CODE_MARKER.len();
        // `EXCEPTION_CODE_MARKER` and ASCII digits are single-byte, so
        // every index derived here is a char boundary.
        let rest = &message[after_marker..];
        let digits = &rest[..rest.bytes().take_while(u8::is_ascii_digit).count()];
        if !digits.is_empty()
            && (at == 0 || rest[digits.len()..].starts_with(EXCEPTION_CODE_FRAME))
            && let Ok(code) = digits.parse::<i32>()
        {
            return Some(code);
        }
        from = after_marker;
    }
    None
}

impl From<clickhouse::error::Error> for ChError {
    fn from(e: clickhouse::error::Error) -> Self {
        use clickhouse::error::Error as E;
        match e {
            E::Network(inner) => ChError::Io(inner.to_string()),
            E::TimedOut => ChError::Timeout(e.to_string()),
            E::BadResponse(msg) => ChError::server_from_bad_response(msg),
            E::InvalidParams(_)
            | E::Compression(_)
            | E::Decompression(_)
            | E::RowNotFound
            | E::SequenceMustHaveLength
            | E::DeserializeAnyNotSupported
            | E::NotEnoughData
            | E::InvalidUtf8Encoding(_)
            | E::InvalidTagEncoding(_)
            | E::VariantDiscriminatorIsOutOfBound(_)
            | E::Custom(_)
            | E::InvalidColumnsHeader(_)
            | E::SchemaMismatch(_) => ChError::Decode(e.to_string()),
            E::Unsupported(_) | E::Other(_) => ChError::Config(e.to_string()),
            other => ChError::Decode(other.to_string()),
        }
    }
}

/// Retry eligibility for [`crate::ChClient::execute`], declared by the
/// caller rather than inferred — the wrapper cannot know whether a given
/// SQL statement's re-execution is safe (edge case #1: a retried
/// `INSERT ... SELECT` backfill duplicates rows and permanently inflates
/// tier `val_sum`/`val_count`, docs/schemas.md §2.2/§8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Idempotency {
    /// Safe to auto-retry on a retryable [`ChError`] (e.g. `CREATE ... IF
    /// NOT EXISTS`, or any statement the caller guarantees cannot duplicate
    /// effects on re-execution).
    Idempotent,
    /// Never auto-retried by the wrapper (e.g. an `INSERT ... SELECT`
    /// backfill). The classified error is surfaced to the caller instead.
    NonIdempotent,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_timeout_and_io_are_retryable() {
        assert!(ChError::Connect("refused".to_string()).is_retryable());
        assert!(ChError::Timeout("deadline".to_string()).is_retryable());
        assert!(ChError::Io("reset".to_string()).is_retryable());
    }

    #[test]
    fn decode_and_config_are_never_retryable() {
        assert!(!ChError::Decode("bad row".to_string()).is_retryable());
        assert!(!ChError::Config("bad pool_size".to_string()).is_retryable());
    }

    #[test]
    fn insert_uncertain_is_never_retryable() {
        // Load-bearing invariant: a caller that retries on `is_retryable()`
        // must never replay an insert with unknown commit fate (issue #3
        // fix plan, finding 2) — retrying would duplicate rows and
        // permanently inflate tier aggregates (docs/schemas.md §2.2/§8).
        assert!(!ChError::InsertUncertain("timed out mid-write".to_string()).is_retryable());
    }

    #[test]
    fn server_error_retryability_is_classified_by_code() {
        let socket_timeout = ChError::Server {
            code: 209,
            message: "SOCKET_TIMEOUT".to_string(),
        };
        assert!(socket_timeout.is_retryable());

        let syntax_error = ChError::Server {
            code: 62,
            message: "SYNTAX_ERROR".to_string(),
        };
        assert!(!syntax_error.is_retryable());

        let memory_limit = ChError::Server {
            code: 241,
            message: "MEMORY_LIMIT_EXCEEDED".to_string(),
        };
        assert!(!memory_limit.is_retryable());
    }

    #[test]
    fn server_from_bad_response_parses_leading_code() {
        let err = ChError::server_from_bad_response(
            "Code: 60. DB::Exception: Table default.x doesn't exist".to_string(),
        );
        match err {
            ChError::Server { code, message } => {
                assert_eq!(code, 60);
                assert!(message.contains("doesn't exist"));
            }
            other => panic!("expected Server, got {other:?}"),
        }
    }

    #[test]
    fn server_from_bad_response_defaults_to_code_zero_when_unparseable() {
        let err = ChError::server_from_bad_response("connection refused".to_string());
        match err {
            ChError::Server { code, .. } => assert_eq!(code, 0),
            other => panic!("expected Server, got {other:?}"),
        }
        // code 0 is not in the retryable allow-list, so an unparseable
        // server error is treated as poison rather than silently retried.
        assert!(!err.is_retryable());
    }

    /// The bare `Code: N` the `clickhouse` crate synthesises from the
    /// `X-ClickHouse-Exception-Code` header when the collected body is
    /// empty or undecodable (`reason()`, `response.rs:179-187` @ clickhouse
    /// 0.15.1). No `. DB::` framing at all, so only the byte-0 arm can
    /// accept it. Measured through this client against 26.3.17.110:
    /// `SELECT randomPrintableASCII(200) AS v FROM numbers(100000000)` with
    /// `max_result_bytes=500000, result_overflow_mode=throw, max_block_size=1000`
    /// returned exactly this 9-byte message.
    #[test]
    fn the_bare_header_derived_code_parses() {
        let body = "Code: 396";
        assert_eq!(body.len(), 9);
        assert_eq!(parse_exception_code(body), Some(396));
    }

    /// Issue #382, the defect this parser exists for, and the discriminating
    /// case: `strip_prefix("Code: ")` reads code `0` from both of these
    /// bodies, so `traces::exec::map_trace_read_error`'s `396 ->
    /// ScanBudgetBytes` arm never fires and the client is told 500
    /// `internal` instead of 422 `query_too_broad`.
    ///
    /// Both bodies are verbatim `ChError::Server.message` values captured
    /// through this client (default LZ4 compression) against ClickHouse
    /// 26.3.17.110, from
    /// `SELECT toString(number) AS v FROM numbers(100000000)` run with
    /// `max_result_bytes=1000000, result_overflow_mode=throw` (396) and with
    /// `max_bytes_to_read=1100000` (307). The leading
    /// `\u{1}\u{1}v\u{6}String` is the `RowBinaryWithNamesAndTypes` column
    /// header the server had already written; the exception follows it.
    ///
    /// 307 is here because 396 is **not** the only affected mapping: 307 is
    /// mapped by `traces::exec::map_trace_read_error` *and* by
    /// `logql::exec::map_read_error`, the LogQL scan-budget 422.
    #[test]
    fn a_code_that_arrives_after_output_has_been_written_parses() {
        const BODY_396: &str = "\u{1}\u{1}v\u{6}StringCode: 396. DB::Exception: Limit for result \
                                exceeded, max bytes: 976.56 KiB, current bytes: 1.64 MiB. \
                                (TOO_MANY_ROWS_OR_BYTES) (version 26.3.17.110 (official build))";
        const BODY_307: &str = "\u{1}\u{1}v\u{6}StringCode: 307. DB::Exception: Limit for rows or \
                                bytes to read exceeded, max bytes: 1.05 MiB, current bytes: 1.50 \
                                MiB: While executing NumbersRange. (TOO_MANY_BYTES) (version \
                                26.3.17.110 (official build))";
        // The fixtures are verbatim captures; these pin them against an
        // accidental edit that would quietly weaken the case.
        assert_eq!(BODY_396.len(), 174);
        assert_eq!(BODY_307.len(), 209);
        assert_eq!(BODY_396.find("Code: "), Some(10));
        assert_eq!(BODY_307.find("Code: "), Some(10));

        for (body, want) in [(BODY_396, 396), (BODY_307, 307)] {
            // The pre-#382 read, restated so the test states what it defeats.
            assert_eq!(body.strip_prefix("Code: "), None);
            match ChError::server_from_bad_response(body.to_string()) {
                ChError::Server { code, message } => {
                    assert_eq!(code, want);
                    // The message is handed on whole, prefix included: it is
                    // the crate's `BadResponse` text and callers such as
                    // `metrics::dispatch::re2_reject_detail` search it.
                    assert_eq!(message, body);
                }
                other => panic!("expected Server, got {other:?}"),
            }
        }
    }

    /// The written result can be arbitrarily long before the exception. The
    /// deepest offset measured on 26.3.17.110 was 1_262_489 bytes — a 396
    /// from `SELECT toString(number) AS v FROM numbers(100000000)` with
    /// `max_result_bytes=8000000, result_overflow_mode=throw`, whose whole
    /// `ChError::Server.message` was 1_262_651 bytes. Only the offset is
    /// measured here; the prefix is synthetic, because a 1.2 MB literal has
    /// no place in a source file.
    #[test]
    fn a_code_deep_inside_a_written_result_still_parses() {
        const DEEPEST_MEASURED_OFFSET: usize = 1_262_489;
        let mut body = "x".repeat(DEEPEST_MEASURED_OFFSET);
        body.push_str("Code: 396. DB::Exception: Limit for result exceeded");
        assert_eq!(body.find("Code: "), Some(DEEPEST_MEASURED_OFFSET));
        assert_eq!(parse_exception_code(&body), Some(396));
    }

    /// A nested exception renders the outermost code first and its causes
    /// after. Verbatim `ChError::Server.message` from `SELECT
    /// toString(dummy) AS v FROM remote('127.0.0.1:9999', system.one)` on
    /// 24.8.14.39 (26.3.17.110 renders the same shape, only the version
    /// strings differ). First-match returns 519, what actually failed;
    /// taking the LAST match instead would return the innermost 210 —
    /// which is in `RETRYABLE_SERVER_CODES` while 519 is not, so the choice
    /// is not cosmetic.
    #[test]
    fn nested_exception_yields_the_outermost_code() {
        let refused = "Code: 210. DB::NetException: Connection refused (127.0.0.1:9999). \
                       (NETWORK_ERROR) (version 24.8.14.39 (official build))";
        let body = format!(
            "Code: 519. DB::NetException: All attempts to get table structure failed. Log: \n\n\
             Code: 279. DB::NetException: All connection tries failed. Log: \n\n\
             {refused}\n{refused}\n{refused}\n\n. \
             (ALL_CONNECTION_TRIES_FAILED) (version 24.8.14.39 (official build))\n\n. \
             (NO_REMOTE_SHARD_AVAILABLE) (version 24.8.14.39 (official build))"
        );
        assert_eq!(parse_exception_code(&body), Some(519));
        // Not the innermost, and not the pre-#382 reading either: both reads
        // agree here, which is the point — the change moves nothing.
        assert_eq!(
            body.strip_prefix("Code: ")
                .and_then(|r| r.split(['.', ' ']).next())
                .and_then(|d| d.parse::<i32>().ok()),
            Some(519)
        );
    }

    /// `Code: N` past byte 0 without the `. DB::` framing is result data,
    /// not an exception — rejected, so a log line that merely mentions a
    /// code cannot re-classify a failure.
    #[test]
    fn an_unframed_code_past_byte_zero_is_not_believed() {
        assert_eq!(
            parse_exception_code("\u{4}line Code: 158 seen in the log"),
            None
        );
        assert_eq!(
            parse_exception_code("\u{4}line Code: 158, then Code: 396. DB::Exception: real"),
            Some(396)
        );
    }

    /// The whole point of the byte-0 arm: for a body that begins with the
    /// code, this parse returns exactly what `strip_prefix("Code: ")`
    /// returned, so the change moves nothing that is right today. Replayed
    /// over messages measured through this client across nine error classes
    /// on 24.8.14.39 and 26.3.17.110, plus the header-derived fallback. The
    /// `Code: N. DB::…` head of each is verbatim; the descriptions after it
    /// are abridged, since only the head is load-bearing here (the verbatim
    /// bodies that ARE load-bearing are the post-flush fixtures above).
    #[test]
    fn every_body_the_old_prefix_read_parsed_yields_the_same_code() {
        let old = |m: &str| {
            m.strip_prefix("Code: ")
                .and_then(|r| r.split(['.', ' ']).next())
                .and_then(|d| d.parse::<i32>().ok())
        };
        for body in [
            "Code: 43. DB::Exception: Illegal type UInt8 of argument of function lower: In scope \
             SELECT toString(number) AS v FROM numbers(1) WHERE lower(1). \
             (ILLEGAL_TYPE_OF_ARGUMENT) (version 26.3.17.110 (official build))",
            "Code: 60. DB::Exception: Unknown table expression identifier 'nosuchtable_382' in \
             scope SELECT v FROM nosuchtable_382. (UNKNOWN_TABLE) (version 24.8.14.39 (official \
             build))",
            "Code: 81. DB::Exception: Database nosuchdb_382 does not exist. (UNKNOWN_DATABASE) \
             (version 26.3.17.110 (official build))",
            "Code: 158. DB::Exception: Limit for rows (controlled by 'max_rows_to_read' setting) \
             exceeded, max rows: 1.00 million, current rows: 100.00 million. (TOO_MANY_ROWS) \
             (version 26.3.17.110 (official build))",
            "Code: 191. DB::Exception: Limit for IN-set exceeded, max rows: 100.00, current rows: \
             65.54 thousand. (SET_SIZE_LIMIT_EXCEEDED) (version 24.8.14.39 (official build))",
            "Code: 241. DB::Exception: Query memory limit exceeded: would use 194.36 MiB (attempt \
             to allocate chunk of 4.49 MiB), maximum: 190.73 MiB. (MEMORY_LIMIT_EXCEEDED) \
             (version 26.3.17.110 (official build))",
            "Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero. \
             (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 24.8.14.39 (official build))",
            "Code: 396. DB::Exception: Limit for result exceeded, max bytes: 19.53 KiB, current \
             bytes: 1.64 MiB. (TOO_MANY_ROWS_OR_BYTES) (version 24.8.14.39 (official build))",
            "Code: 427. DB::Exception: OptimizedRegularExpression: cannot compile re2: [a-, \
             error: missing ]: [a-. Look at https://github.com/google/re2/wiki/Syntax for \
             reference. (CANNOT_COMPILE_REGEXP) (version 26.3.17.110 (official build))",
            // The header-derived fallback, which has no framing at all.
            "Code: 396",
        ] {
            let was = old(body);
            assert!(
                was.is_some(),
                "fixture must parse under the old read: {body}"
            );
            assert_eq!(parse_exception_code(body), was, "moved: {body}");
        }
    }

    /// The two bodies where the new read and the `strip_prefix` read
    /// disagree. ClickHouse emits neither a signed nor a negative code, and
    /// no code table in this workspace holds one — recorded so the
    /// divergence is a checked fact rather than an assumption.
    #[test]
    fn a_signed_code_is_the_one_shape_the_two_reads_disagree_on() {
        let old = |m: &str| {
            m.strip_prefix("Code: ")
                .and_then(|r| r.split(['.', ' ']).next())
                .and_then(|d| d.parse::<i32>().ok())
        };
        assert_eq!(old("Code: -5. DB::Exception: fabricated"), Some(-5));
        assert_eq!(
            parse_exception_code("Code: -5. DB::Exception: fabricated"),
            None
        );
        assert_eq!(old("Code: +5. DB::Exception: fabricated"), Some(5));
        assert_eq!(
            parse_exception_code("Code: +5. DB::Exception: fabricated"),
            None
        );
        // Both fall back to 0, which no caller's table contains.
        assert!(!RETRYABLE_SERVER_CODES.contains(&0));
    }

    /// A digit run too wide for `i32` is not a code: the parse fails, and
    /// the scan must move on rather than stop at the failed candidate.
    #[test]
    fn an_overlong_digit_run_is_skipped_not_returned() {
        assert_eq!(
            parse_exception_code("Code: 99999999999999. DB::Exception: x"),
            None
        );
        assert_eq!(
            parse_exception_code("Code: 99999999999999. DB::Exception: x Code: 396. DB::E"),
            Some(396)
        );
    }

    /// The accepted residual: retained result bytes carrying the FULL
    /// framing win over the exception ClickHouse appended after them.
    /// Pinned rather than fixed — see [`parse_exception_code`] for why
    /// last-match is worse.
    #[test]
    fn framed_text_inside_result_bytes_wins_over_the_appended_exception() {
        let body = "\u{2}a stored log line: Code: 42. DB::Exception: something a tenant logged\
                    Code: 396. DB::Exception: Limit for result exceeded. (TOO_MANY_ROWS_OR_BYTES)";
        assert_eq!(parse_exception_code(body), Some(42));
    }

    /// A body cut short: digits with nothing after them are rejected past
    /// byte 0, but a body truncated immediately after the framing still
    /// classifies — the framing carries the meaning, not the description.
    #[test]
    fn a_truncated_tail_is_rejected_only_when_the_framing_is_missing() {
        assert_eq!(parse_exception_code("\u{3}rows Code: 39"), None);
        assert_eq!(parse_exception_code("\u{3}rows Code: 396. DB::"), Some(396));
        assert_eq!(parse_exception_code("\u{3}rows Code: 396. DB:"), None);
    }

    /// Non-ClickHouse framings read as 0, exactly as before #382:
    /// Poco's own rendering uses `Code: N,` and carries no `. DB::`.
    #[test]
    fn poco_framing_and_a_missing_code_both_read_as_zero() {
        assert_eq!(
            parse_exception_code(
                "Poco::Exception. Code: 1000, e.code() = 111, Connection refused (version 24.8)"
            ),
            None
        );
        assert_eq!(parse_exception_code("Code: not-a-number"), None);
        assert_eq!(parse_exception_code(""), None);
    }

    /// The marker may appear on a multi-byte-character boundary; the scan
    /// must index safely and still find the framed occurrence.
    #[test]
    fn a_multi_byte_prefix_does_not_break_the_scan() {
        let body = "𠜎é span name Code: 307. DB::Exception: Limit for bytes to read exceeded";
        assert_eq!(parse_exception_code(body), Some(307));
    }
}
