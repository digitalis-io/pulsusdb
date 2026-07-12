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

    /// Parses a ClickHouse HTTP error body's leading `Code: N` prefix (the
    /// `clickhouse` crate does not expose the exception code as a typed
    /// field, only embedded in `Error::BadResponse`'s message text).
    pub(crate) fn server_from_bad_response(message: String) -> ChError {
        let code = message
            .strip_prefix("Code: ")
            .and_then(|rest| rest.split(['.', ' ']).next())
            .and_then(|digits| digits.parse::<i32>().ok())
            .unwrap_or(0);
        ChError::Server { code, message }
    }
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
}
