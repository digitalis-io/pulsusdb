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
    /// | `<result bytes>Code: N. DB::Exception: …` | past byte 0, unbounded | 26.3.17.110 |
    ///
    /// The third shape is the defect. It is a property of the *server*, not
    /// of any one code: 24.8 discards its not-yet-flushed output buffer when
    /// it turns the response into an error (the compressed body carries a
    /// single block, the exception), while 26.3 keeps it (two blocks: the
    /// written result, then the exception). No 24.8 shape that produced it
    /// was found, so on 24.8 this parse is a pin and on 26.3 it is the fix.
    /// The offset itself is not a stable function of any one setting — the
    /// same `max_result_bytes` yields different offsets run to run — so
    /// nothing here or in the tests depends on a particular offset value.
    ///
    /// **Why the text is parsed at all, when the code is also on the wire.**
    /// ClickHouse sends `X-ClickHouse-Exception-Code: N` on this response —
    /// measured present on every post-flush 500 on both pinned versions —
    /// and that header is a source no query result can forge. The
    /// `clickhouse` crate reads it (`response.rs:85` @ 0.15.1) but does not
    /// surface it: it is passed to `collect_bad_response` only as the
    /// `reason()` fallback used when the body cannot be collected, is empty,
    /// or is not UTF-8 (`response.rs:142`, `:145`, `:161`); when the body
    /// *does* decode, `response.rs:158-163` returns the body and the header
    /// code is dropped. `clickhouse::error::Error` carries no numeric code
    /// in any variant — `BadResponse(String)` is the only channel. So the
    /// typed source exists on the wire and is unreachable from here without
    /// changing the dependency; until then [`parse_exception_code`] must be
    /// sound against a body whose leading bytes are tenant-controlled.
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
/// The body is one of two things, and the rule has one arm for each:
///
/// 1. **The body IS the exception** — nothing was written before it. Then
///    the code is at byte 0, and that is the code, exactly as the
///    `strip_prefix("Code: ")` read this replaces took it. This arm also
///    covers the crate's header-derived fallback (`reason()`,
///    `response.rs:179-187` @ clickhouse 0.15.1), the bare `Code: 396` with
///    no framing at all.
/// 2. **Output was written first**, so the body is
///    `<result bytes><exception>`. Then the code is the **last**
///    `Code: <digits>` in the body, and it must carry the
///    [`EXCEPTION_CODE_FRAME`] framing; anything else reads as `0`.
///
/// # Why the last occurrence, and what makes it sound
///
/// Result bytes are **tenant-controlled**: a stored log line or span name
/// can contain any text at all, including a whole forged
/// `Code: 210. DB::Exception: …`. Taking the *first* believable occurrence
/// would let that forgery decide the classification — a 500 where
/// `docs/api.md`'s contract requires a 422, or a retry decision made by the
/// data. The soundness property that rules this out is positional, not
/// syntactic:
///
/// > ClickHouse appends its exception **after every byte the query
/// > produced**, so every tenant-controlled byte in the message sits at a
/// > strictly smaller index than the server's exception. The last
/// > `Code: ` occurrence therefore always lies inside server-authored text,
/// > whatever the result bytes contain and wherever in them it sits.
///
/// The rule is deliberately anchored on the last occurrence of the *marker*
/// rather than the last *framed* occurrence, and that distinction is the
/// whole guarantee. Anchoring on the last framed occurrence would step over
/// a server exception rendered without `DB::` — `getCurrentExceptionMessage`
/// renders a non-`DB::` failure as `Poco::Exception. Code: 1000, e.code() =
/// …` — and hand the decision back to the tenant's framed text. Anchoring
/// on the last marker cannot: if the server's own trailing `Code: ` is not
/// framed, the parse yields `None` and the caller sees `0`, which is what it
/// saw before this change. **Every way this rule can fail, it fails to `0`
/// and never to a tenant-chosen code.** Both cases are pinned by tests.
///
/// Arm 1 is not forgeable either, for the same positional reason: byte 0 of
/// a written result is the `RowBinaryWithNamesAndTypes` header — a column
/// count varint, then column names and type names, all fixed by the SQL we
/// render, never by stored data (measured: `\u{1}\u{1}v\u{6}String…`).
///
/// # What this costs, deliberately
///
/// - **Nested exceptions.** A failed distributed read renders the outermost
///   code first and its causes after (`Code: 519. DB::NetException: …
///   Code: 279 … Code: 210 …`). Arriving *without* prior output it takes
///   arm 1 and reads 519, what actually failed — the same code
///   `strip_prefix` returned, so nothing correct today moves. Arriving
///   *after* output it takes arm 2 and reads the innermost cause, 210.
///   Accepted and pinned: both codes are server-authored, the tenant cannot
///   steer the choice, and that body reads as `0` today — so this residual
///   only ever touches a case that is already broken.
/// - **A `Code: ` inside the exception's own description.** ClickHouse
///   echoes query text into some messages (a 427 carries the user's regex),
///   so a pattern containing `Code: 42` can become the last marker in the
///   body. Unframed, so the parse yields `None` and the caller sees `0` —
///   again the pre-change reading, never a forged code.
/// - **`Code: N` with no framing, past byte 0.** Rejected.
/// - **A truncated tail.** `…Code: 39` at the end is rejected (no framing);
///   `…Code: 396. DB::` truncated right there is accepted — the framing is
///   what carries the meaning, not the description.
///
/// # Compatibility with the read it replaces
///
/// For every body that begins `Code: ` + a run of ASCII digits that fits
/// `i32` — the shape of every body measured across thirteen error classes
/// on both pinned versions — this returns exactly what `strip_prefix`
/// returned. The two bodies where the two reads differ are `Code: -5.` and
/// `Code: +5.`: the old read took them as -5 and 5, this one as `None` (0).
/// ClickHouse emits neither, no code table in this workspace holds a
/// negative code, and both are pinned by a test so the divergence is
/// recorded rather than assumed away.
///
/// # Cost
///
/// One `str::strip_prefix` at byte 0 plus one `str::rfind` (`memchr`-backed)
/// over a body that, on the buffered path, is as large as the written
/// result. Runs once per failed query, never per row.
///
/// # Re-validating this
///
/// Most of the tests below call this function directly, so **reverting the
/// rule inside this function is the only revert that exercises them all**.
/// Reverting the call site in [`ChError::server_from_bad_response`] instead
/// reaches only the ones that go through `ChError` — one here, plus the two
/// mapper tests in `pulsus-read` — and leaves the rest green, which reads
/// exactly like a passing suite.
///
/// Three reverts are worth keeping distinct, because each is caught by a
/// different set of tests and none subsumes another: the pre-#382
/// `strip_prefix` read; **first**-match over believable occurrences; and
/// **last-framed**-match. The tests name which of the three they defeat.
fn parse_exception_code(message: &str) -> Option<i32> {
    // Arm 1: the body is the exception itself.
    if let Some(code) = code_at(message, 0, Framing::NotRequired) {
        return Some(code);
    }
    // Arm 2: output was written first. The last marker is server-authored
    // no matter what the result bytes hold; the framing is what says it is
    // an exception rather than an echo inside one.
    let at = message.rfind(EXCEPTION_CODE_MARKER)?;
    code_at(message, at, Framing::Required)
}

/// Whether [`code_at`] insists on [`EXCEPTION_CODE_FRAME`] after the digits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Framing {
    Required,
    NotRequired,
}

/// Reads `Code: <ascii digits>` at exactly `at`, or `None`. `at` is always a
/// char boundary: it is either 0 or a `rfind` hit on an all-ASCII needle,
/// and the marker and the digits that follow it are single-byte, so every
/// index derived here is a boundary too.
fn code_at(message: &str, at: usize, framing: Framing) -> Option<i32> {
    let rest = message[at..].strip_prefix(EXCEPTION_CODE_MARKER)?;
    let digit_len = rest.bytes().take_while(u8::is_ascii_digit).count();
    let (digits, tail) = rest.split_at(digit_len);
    if digits.is_empty()
        || (framing == Framing::Required && !tail.starts_with(EXCEPTION_CODE_FRAME))
    {
        return None;
    }
    digits.parse::<i32>().ok()
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
    /// prefix length here is synthetic and carries no claim: the offset is
    /// not a stable function of any setting — repeats of the same
    /// `max_result_bytes` put the code at different offsets — so the test
    /// asserts only that depth does not defeat the scan.
    #[test]
    fn a_code_deep_inside_a_written_result_still_parses() {
        const DEEP: usize = 1_262_489;
        let mut body = "x".repeat(DEEP);
        body.push_str("Code: 396. DB::Exception: Limit for result exceeded");
        assert_eq!(body.find("Code: "), Some(DEEP));
        assert_eq!(parse_exception_code(&body), Some(396));
    }

    /// A nested exception renders the outermost code first and its causes
    /// after. Verbatim `ChError::Server.message` from `SELECT
    /// toString(dummy) AS v FROM remote('127.0.0.1:9999', system.one)` on
    /// 24.8.14.39 (26.3.17.110 renders the same shape, only the version
    /// strings differ). Arriving with nothing written before it, this takes
    /// arm 1 and reads 519 — what actually failed, and what the pre-#382
    /// read returned, so nothing correct today moves.
    fn nested_body() -> String {
        let refused = "Code: 210. DB::NetException: Connection refused (127.0.0.1:9999). \
                       (NETWORK_ERROR) (version 24.8.14.39 (official build))";
        format!(
            "Code: 519. DB::NetException: All attempts to get table structure failed. Log: \n\n\
             Code: 279. DB::NetException: All connection tries failed. Log: \n\n\
             {refused}\n{refused}\n{refused}\n\n. \
             (ALL_CONNECTION_TRIES_FAILED) (version 24.8.14.39 (official build))\n\n. \
             (NO_REMOTE_SHARD_AVAILABLE) (version 24.8.14.39 (official build))"
        )
    }

    #[test]
    fn nested_exception_yields_the_outermost_code() {
        let body = nested_body();
        assert_eq!(parse_exception_code(&body), Some(519));
        // Both reads agree here, which is the point — the change moves nothing.
        assert_eq!(
            body.strip_prefix("Code: ")
                .and_then(|r| r.split(['.', ' ']).next())
                .and_then(|d| d.parse::<i32>().ok()),
            Some(519)
        );
    }

    /// The accepted residual of anchoring arm 2 on the LAST marker: the same
    /// nested exception, arriving after output was written, reports the
    /// innermost cause instead of the outermost. Both codes are
    /// server-authored — the tenant cannot steer which — and this body reads
    /// as `0` under the pre-#382 parse, so the residual only ever touches a
    /// case that is already broken. Recorded rather than claimed away.
    ///
    /// Defeats: `strip_prefix` (0), and first-match (519).
    #[test]
    fn a_nested_exception_after_written_output_reports_the_innermost_cause() {
        let body = format!("\u{1}\u{1}v\u{6}String{}", nested_body());
        assert_eq!(parse_exception_code(&body), Some(210));
        assert_eq!(body.strip_prefix("Code: "), None, "0 before this change");
    }

    /// **The finding this rule exists for.** Result bytes are
    /// tenant-controlled: a stored log line can carry a whole forged
    /// `Code: N. DB::Exception: …`. It must never decide the
    /// classification, wherever in the result it sits — near the front, near
    /// the very end, or repeatedly.
    ///
    /// Defeats: `strip_prefix` (0), and first-match (the forged 210).
    #[test]
    fn result_bytes_cannot_forge_the_code_wherever_they_sit() {
        let forged = "Code: 210. DB::Exception: a line a tenant stored";
        let real = "Code: 396. DB::Exception: Limit for result exceeded. \
                    (TOO_MANY_ROWS_OR_BYTES) (version 26.3.17.110 (official build))";
        for result_bytes in [
            format!("\u{1}\u{1}v\u{6}String{forged} then 800 KiB of other rows"),
            format!("\u{1}\u{1}v\u{6}String{} {forged}", "row ".repeat(2000)),
            format!("\u{1}\u{1}v\u{6}String{forged}{forged}{forged}"),
        ] {
            let body = format!("{result_bytes}{real}");
            assert_eq!(
                parse_exception_code(&body),
                Some(396),
                "the appended exception must win over tenant bytes"
            );
        }
        // 210 is retryable and 396 is not, so picking the forgery would also
        // have handed a tenant the retry decision.
        assert!(RETRYABLE_SERVER_CODES.contains(&210));
        assert!(!RETRYABLE_SERVER_CODES.contains(&396));
    }

    /// The reason arm 2 anchors on the last **marker** and not the last
    /// **framed** marker. `getCurrentExceptionMessage` renders a non-`DB::`
    /// failure as `Poco::Exception. Code: N, e.code() = …`. Had the rule
    /// scanned back for the last *framed* occurrence, it would have stepped
    /// over that trailing exception and handed the decision to the tenant's
    /// forged text. Anchored on the last marker it cannot: the parse fails
    /// to `0`, the pre-change reading, never to the tenant's code.
    ///
    /// Defeats: first-match and last-framed-match (both the forged 210).
    /// `strip_prefix` also reads 0 here, so this one does not discriminate
    /// against the pre-change tree — it discriminates against the two rules
    /// that were considered and rejected.
    #[test]
    fn an_unframed_server_exception_does_not_let_result_bytes_decide() {
        let body = "\u{1}\u{1}v\u{6}StringCode: 210. DB::Exception: a line a tenant stored\
                    Poco::Exception. Code: 1000, e.code() = 111, Connection refused";
        assert_eq!(parse_exception_code(body), None);
        // What a last-FRAMED-occurrence rule would have returned instead.
        assert_eq!(body.rfind("Code: 210. DB::").map(|_| 210), Some(210));
    }

    /// The other way the rule can fail, also to `0`: ClickHouse echoes query
    /// text into some messages (a 427 carries the user's regex), so a
    /// pattern containing `Code: 42` can become the last marker in the body.
    /// Unframed, so the parse yields `None` — the pre-change reading, and
    /// still not a code the pattern chose.
    ///
    /// This is the price of anchoring on the last marker, stated rather than
    /// hidden: first-match and last-framed-match both read 427 here and this
    /// rule reads 0. It is the fail-safe direction, and it is the same
    /// direction the pre-change tree already failed in. Post-flush 427 was
    /// not produced by any measured shape (RE2 compiles before output), so
    /// no measured case pays it — but that is an observation, not a proof of
    /// unreachability, which is why the cost is pinned here.
    ///
    /// Defeats: first-match and last-framed-match (both 427).
    #[test]
    fn a_code_echoed_inside_the_exception_text_fails_to_zero() {
        let body = "\u{1}\u{1}v\u{6}StringCode: 427. DB::Exception: cannot compile re2: \
                    ^(?:Code: 42), error: missing ): while executing 'FUNCTION match'";
        assert_eq!(parse_exception_code(body), None);
    }

    /// `Code: N` past byte 0 without the `. DB::` framing is result data,
    /// not an exception — rejected, so a log line that merely mentions a
    /// code cannot re-classify a failure, and cannot mask one either.
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

    /// A digit run too wide for `i32` is not a code: arm 1 declines it and
    /// arm 2 still reaches the real trailing exception.
    #[test]
    fn an_overlong_digit_run_is_not_returned() {
        assert_eq!(
            parse_exception_code("Code: 99999999999999. DB::Exception: x"),
            None
        );
        assert_eq!(
            parse_exception_code("Code: 99999999999999. DB::Exception: x Code: 396. DB::E"),
            Some(396)
        );
    }

    /// The finding's own case, stated the way it now behaves: retained
    /// result bytes carrying the FULL framing lose to the exception
    /// ClickHouse appended after them. This is the assertion that was
    /// inverted — it read `Some(42)` while the rule took the first match.
    #[test]
    fn the_appended_exception_wins_over_framed_text_inside_result_bytes() {
        let body = "\u{2}a stored log line: Code: 42. DB::Exception: something a tenant logged\
                    Code: 396. DB::Exception: Limit for result exceeded. (TOO_MANY_ROWS_OR_BYTES)";
        assert_eq!(parse_exception_code(body), Some(396));
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
