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

    /// Reads the numeric exception code from the front of a ClickHouse HTTP
    /// error body. Falls back to `0` — never in [`RETRYABLE_SERVER_CODES`],
    /// and in no caller's code table — when the message does not begin with
    /// one.
    ///
    /// # Issue #382: why this reads only byte 0, and why that is now enough
    ///
    /// When a query has already written output, ClickHouse emits the
    /// exception into the same response stream, so the buffered-error body is
    /// `<already-written result bytes><exception>` and the code is not at byte
    /// 0. Reading it out of that text is **not possible soundly**: the result
    /// bytes are tenant data, and the exception's own description echoes the
    /// failing SQL — including tenant regexes that `pulsus-read` renders into
    /// `match()` (`metrics::series_where`, `logql::plan`). Both halves can
    /// therefore contain a forged `Code: N. DB::Exception:`, so a
    /// first-occurrence rule loses to the result bytes and a last-occurrence
    /// rule loses to the description. Measured, both ways, on 26.3.17.110.
    ///
    /// The fix is not a cleverer parse. ClickHouse sends
    /// `X-ClickHouse-Exception-Code: N` on that response — nothing in the body
    /// can influence it — and the vendored `clickhouse` patch (ADR 0007,
    /// `vendor/clickhouse/PATCHES.md`) puts it at byte 0 ahead of the body
    /// that upstream returned. So this function reads byte 0 and nothing else,
    /// and what it reads came from the header.
    ///
    /// # What is still not sound, and whose it is
    ///
    /// **ClickHouse 24.8 remains forgeable and the patch does not help it.**
    /// On its streaming path the response is HTTP 200 with no exception-code
    /// header, and the crate anchors the message with `rfind(b"Code:")`
    /// (`extract_exception_old`, `response.rs:368-377` @ clickhouse 0.15.1),
    /// so a tenant literal echoed in the description becomes byte 0 and this
    /// function reads it. That is issue #412 — it is live on `main`, predates
    /// #382, and cannot be fixed by any patch here or upstream, because at
    /// HTTP 200 the exception boundary is genuinely unrecoverable from the
    /// text. It closes with the move to ClickHouse 26.3 (#376), whose
    /// streaming path frames the exception with a length the client trusts
    /// (`extract_exception_new`). **Do not read this parse as sound on both
    /// pinned versions.**
    ///
    /// # Shapes this receives, all measured through this client
    ///
    /// | shape | source |
    /// |---|---|
    /// | `Code: N. DB::Exception: …` | an exception with no output before it, both versions |
    /// | `Code: N` alone | the crate's header-derived `reason()`, `response.rs:179-187` |
    /// | `Code: N\n<result bytes>Code: N. DB::Exception: …` | the ADR 0007 patch, 26.3 buffered path |
    pub(crate) fn server_from_bad_response(message: String) -> ChError {
        let code = parse_exception_code(&message).unwrap_or(0);
        ChError::Server { code, message }
    }
}

/// The marker ClickHouse writes ahead of an exception's numeric code, and
/// that the ADR 0007 patch writes ahead of the header-derived code.
const EXCEPTION_CODE_MARKER: &str = "Code: ";

/// Reads `Code: <ascii digits>` at byte 0. Deliberately positional: see
/// [`ChError::server_from_bad_response`] for why nothing past byte 0 can be
/// believed, and what remains unsound on ClickHouse 24.8 (#412).
///
/// Compatible with the `strip_prefix("Code: ").split(['.', ' '])` read this
/// replaces for every body that begins with a run of ASCII digits that fits
/// `i32` — the shape of every body measured across thirteen error classes on
/// both pinned versions. It differs on `Code: -5.` and `Code: +5.` (the old
/// read took them as -5 and 5, this one as `None`), which ClickHouse emits
/// neither of, and it is *required* by the ADR 0007 patch: the patched
/// message continues `\n<body>` after the digits, and the old read's
/// `split(['.', ' '])` would swallow that into the token and fail to parse.
///
/// Cost: one `strip_prefix` and a digit run. Once per failed query.
fn parse_exception_code(message: &str) -> Option<i32> {
    let rest = message.strip_prefix(EXCEPTION_CODE_MARKER)?;
    let digit_len = rest.bytes().take_while(u8::is_ascii_digit).count();
    // The marker and ASCII digits are single-byte, so this is a char boundary.
    rest[..digit_len].parse::<i32>().ok()
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

    /// The bare `Code: N` the crate synthesises from the
    /// `X-ClickHouse-Exception-Code` header when the collected body is empty
    /// or undecodable (`reason()`, `response.rs:179-187` @ clickhouse 0.15.1).
    /// Measured through this client against 26.3.17.110: `SELECT
    /// randomPrintableASCII(200) AS v FROM numbers(100000000)` with
    /// `max_result_bytes=500000, result_overflow_mode=throw, max_block_size=1000`
    /// returned exactly this 9-byte message.
    #[test]
    fn the_bare_header_derived_code_parses() {
        let body = "Code: 396";
        assert_eq!(body.len(), 9);
        assert_eq!(parse_exception_code(body), Some(396));
    }

    /// Issue #382 end to end, in the shape the ADR 0007 vendored patch
    /// delivers. `BODY` is the verbatim `ChError::Server.message` measured
    /// through this client against ClickHouse 26.3.17.110 BEFORE the patch
    /// (`SELECT toString(number) AS v FROM numbers(100000000)` with
    /// `max_result_bytes=1000000, result_overflow_mode=throw`); the leading
    /// `\u{1}\u{1}v\u{6}String` is the `RowBinaryWithNamesAndTypes` column
    /// header the server had already written. `PATCHED` is that same body with
    /// the header-derived code the patch puts in front of it.
    ///
    /// Both halves are load-bearing, and the second is the honest one:
    ///
    /// - patched, the code parses, so `traces::exec::map_trace_read_error`
    ///   raises 422 `query_too_broad` instead of 500 `internal`;
    /// - unpatched, it reads `0`, and that is **correct behaviour** for this
    ///   parse, not a gap. Nothing in that body is trustworthy — see
    ///   `a_forged_code_in_the_result_bytes_is_never_read`.
    #[test]
    fn the_code_the_patch_puts_at_byte_zero_is_what_gets_read() {
        const BODY: &str = "\u{1}\u{1}v\u{6}StringCode: 396. DB::Exception: Limit for result \
                            exceeded, max bytes: 976.56 KiB, current bytes: 1.64 MiB. \
                            (TOO_MANY_ROWS_OR_BYTES) (version 26.3.17.110 (official build))";
        assert_eq!(BODY.len(), 174, "verbatim capture, pinned against an edit");

        let patched = format!("Code: 396\n{BODY}");
        match ChError::server_from_bad_response(patched.clone()) {
            ChError::Server { code, message } => {
                assert_eq!(code, 396);
                // The description survives whole: `metrics::dispatch`'s 427
                // handling searches it.
                assert!(message.contains("TOO_MANY_ROWS_OR_BYTES"));
                assert_eq!(message, patched);
            }
            other => panic!("expected Server, got {other:?}"),
        }

        // Unpatched, the code is not recoverable and must not be guessed.
        assert_eq!(parse_exception_code(BODY), None);
    }

    /// The `[high]` this design exists for, stated as the property that
    /// holds: **result bytes are never read**, because nothing past byte 0
    /// is. A tenant's stored log line carrying a full
    /// `Code: 210. DB::Exception: …` cannot become the classification, and
    /// 210 is in [`RETRYABLE_SERVER_CODES`] while the real 396 is not, so a
    /// forgery would have taken the retry decision as well as the status.
    ///
    /// The patched prefix is what makes the real code readable at all here;
    /// without it the answer is `0`, never the forgery.
    #[test]
    fn a_forged_code_in_the_result_bytes_is_never_read() {
        let forged = "Code: 210. DB::Exception: a line a tenant stored";
        let real = "Code: 396. DB::Exception: Limit for result exceeded. \
                    (TOO_MANY_ROWS_OR_BYTES) (version 26.3.17.110 (official build))";
        for result_bytes in [
            format!("\u{1}\u{1}v\u{6}String{forged} then more rows"),
            format!("\u{1}\u{1}v\u{6}String{} {forged}", "row ".repeat(500)),
            format!("\u{1}\u{1}v\u{6}String{forged}{forged}{forged}"),
        ] {
            let body = format!("{result_bytes}{real}");
            assert_eq!(parse_exception_code(&body), None, "unpatched: no guess");
            assert_eq!(
                parse_exception_code(&format!("Code: 396\n{body}")),
                Some(396),
                "patched: the header code, never the forgery"
            );
        }
        assert!(RETRYABLE_SERVER_CODES.contains(&210));
        assert!(!RETRYABLE_SERVER_CODES.contains(&396));
    }

    /// The other forgery channel, and the one that killed every text rule:
    /// ClickHouse echoes the failing SQL into the description, and
    /// `pulsus-read` renders tenant regexes into `match()`
    /// (`metrics::series_where`, `logql::plan`), so the echo lands AFTER the
    /// server's own code. Measured on 26.3.17.110 with a tenant literal in
    /// `match()` and a late `intDiv` failure: real code 153, forged 210.
    /// Reading byte 0 ignores it.
    #[test]
    fn a_forged_code_echoed_in_the_description_is_never_read() {
        let echoed = "Code: 153. DB::Exception: Division by zero: while executing 'FUNCTION \
                      and(match(toString(number), 'Code: 210. DB::Exception: forged|.*'_String), \
                      notEquals(intDiv(...)))'. (ILLEGAL_DIVISION) (version 26.3.17.110)";
        assert_eq!(parse_exception_code(echoed), Some(153));
        assert_eq!(
            parse_exception_code(&format!("Code: 153\n{echoed}")),
            Some(153),
            "the patched prefix agrees with byte 0 here"
        );
    }

    /// ClickHouse 24.8's streaming path, which this parse CANNOT make sound
    /// and which the ADR 0007 patch does not reach (issue #412, closing with
    /// #376). Verbatim `ChError::Server.message` measured through this client
    /// against 24.8.14.39: the crate's `rfind`-anchored extractor
    /// (`extract_exception_old`, `response.rs:368-377` @ 0.15.1) truncated the
    /// message to start at the tenant's echoed literal, so byte 0 IS the
    /// forgery. The real failure was 153.
    ///
    /// Pinned so the limitation is a checked fact: if a future crate or server
    /// version stops delivering this shape, this test fails and #412 can be
    /// re-assessed rather than forgotten.
    #[test]
    fn on_24_8_a_streamed_forgery_reaches_byte_zero_and_is_read_issue_412() {
        let body = "Code: 210. DB::Exception: forged|.*'_String), notEquals(intDiv(1_UInt8, \
                    minus(toInt64(__table1.number), 400000_UInt32)), 0_UInt8)) UInt8 : 4'. \
                    (ILLEGAL_DIVISION) (version 24.8.14.39 (official build))";
        assert_eq!(body.len(), 199, "verbatim capture, pinned against an edit");
        assert_eq!(
            parse_exception_code(body),
            Some(210),
            "#412: on 24.8 the crate hands us the forgery AT byte 0, and no \
             parse can recover the real 153 — the pre-#382 read returns 210 here too"
        );
    }

    /// A nested exception renders the outermost code first and its causes
    /// after. Verbatim from `SELECT toString(dummy) AS v FROM
    /// remote('127.0.0.1:9999', system.one)` on 24.8.14.39 (26.3.17.110
    /// renders the same shape, only the version strings differ). Byte 0 is the
    /// outermost code — what actually failed, and what the pre-#382 read
    /// returned, so nothing correct today moves.
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
        // 210 is retryable and 519 is not, so a last-occurrence rule would
        // have made a poison distributed failure look transient.
        assert!(body.rfind("Code: 210").is_some());
        assert!(RETRYABLE_SERVER_CODES.contains(&210));
        assert!(!RETRYABLE_SERVER_CODES.contains(&519));
    }

    /// For every body that begins with the code, this returns exactly what
    /// `strip_prefix("Code: ").split(['.', ' '])` returned, so the change
    /// moves nothing that is right today. Replayed over messages measured
    /// through this client across nine error classes on 24.8.14.39 and
    /// 26.3.17.110, plus the header-derived fallback. The `Code: N. DB::…`
    /// head of each is verbatim; the descriptions after it are abridged, since
    /// only the head is load-bearing here.
    #[test]
    fn every_body_the_old_prefix_read_parsed_yields_the_same_code() {
        let old = |m: &str| {
            m.strip_prefix("Code: ")
                .and_then(|r| r.split(['.', ' ']).next())
                .and_then(|d| d.parse::<i32>().ok())
        };
        for body in [
            "Code: 43. DB::Exception: Illegal type UInt8 of argument of function lower. \
             (ILLEGAL_TYPE_OF_ARGUMENT) (version 26.3.17.110 (official build))",
            "Code: 60. DB::Exception: Unknown table expression identifier 'nosuchtable_382'. \
             (UNKNOWN_TABLE) (version 24.8.14.39 (official build))",
            "Code: 81. DB::Exception: Database nosuchdb_382 does not exist. (UNKNOWN_DATABASE) \
             (version 26.3.17.110 (official build))",
            "Code: 158. DB::Exception: Limit for rows (controlled by 'max_rows_to_read' setting) \
             exceeded. (TOO_MANY_ROWS) (version 26.3.17.110 (official build))",
            "Code: 191. DB::Exception: Limit for IN-set exceeded, max rows: 100.00. \
             (SET_SIZE_LIMIT_EXCEEDED) (version 24.8.14.39 (official build))",
            "Code: 241. DB::Exception: Query memory limit exceeded: would use 194.36 MiB. \
             (MEMORY_LIMIT_EXCEEDED) (version 26.3.17.110 (official build))",
            "Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero. \
             (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 24.8.14.39 (official build))",
            "Code: 396. DB::Exception: Limit for result exceeded, max bytes: 19.53 KiB. \
             (TOO_MANY_ROWS_OR_BYTES) (version 24.8.14.39 (official build))",
            "Code: 427. DB::Exception: OptimizedRegularExpression: cannot compile re2: [a-. \
             (CANNOT_COMPILE_REGEXP) (version 26.3.17.110 (official build))",
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

    /// The ADR 0007 patch's message shape is exactly where the two reads
    /// diverge, and this read is the one that works: the patched message
    /// continues `\n<body>` after the digits, which the old read's
    /// `split(['.', ' '])` swallows into the token so it fails to parse.
    #[test]
    fn the_old_prefix_read_could_not_have_parsed_the_patched_shape() {
        let patched = "Code: 396\n\u{1}\u{1}v\u{6}StringCode: 396. DB::Exception: Limit for \
                       result exceeded. (TOO_MANY_ROWS_OR_BYTES)";
        let old = patched
            .strip_prefix("Code: ")
            .and_then(|r| r.split(['.', ' ']).next())
            .and_then(|d| d.parse::<i32>().ok());
        assert_eq!(old, None, "the old read fails on the patched shape");
        assert_eq!(parse_exception_code(patched), Some(396));
    }

    /// The two bodies where this read and the `strip_prefix` read disagree.
    /// ClickHouse emits neither a signed nor a negative code, and no code
    /// table in this workspace holds one — recorded so the divergence is a
    /// checked fact rather than an assumption.
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

    /// Everything that is not `Code: <digits>` at byte 0 reads as `0`: a digit
    /// run too wide for `i32`, Poco's own framing (`Code: N,` with no `DB::`),
    /// a non-numeric code, and an empty body.
    #[test]
    fn anything_that_is_not_a_leading_code_reads_as_zero() {
        assert_eq!(
            parse_exception_code("Code: 99999999999999. DB::Exception: x"),
            None
        );
        assert_eq!(
            parse_exception_code(
                "Poco::Exception. Code: 1000, e.code() = 111, Connection refused (version 24.8)"
            ),
            None
        );
        assert_eq!(parse_exception_code("Code: not-a-number"), None);
        assert_eq!(parse_exception_code("Code: "), None);
        assert_eq!(parse_exception_code(""), None);
        assert_eq!(
            parse_exception_code("\u{4}line Code: 158. DB::Exception: past byte 0"),
            None
        );
    }
}
