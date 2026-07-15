//! `/api/traces/v1/trace/{traceId}` path-parameter parsing: the hex trace
//! id (docs/api.md §4.1: "16 or 32 chars, left-padded"). This is the one
//! validation point on the trace-fetch path — the injection boundary for
//! `pulsus_read::traces::sql::point_read_sql`'s `unhex('...')` literal:
//! only `[0-9a-f]{32}` output ever leaves this module.

use thiserror::Error;

/// Errors from parsing the `{traceId}` path parameter — mapped to `400
/// bad_data` by `error::ApiError`.
#[derive(Debug, Error)]
pub(crate) enum TraceIdError {
    #[error("invalid trace id {0:?}: expected 16 or 32 hex characters")]
    InvalidLength(String),
    #[error("invalid trace id {0:?}: expected hex characters only")]
    NotHex(String),
}

/// Parses `raw` into the canonical 32-char lowercase hex trace id:
/// 32 hex chars pass through (lowercased); 16 hex chars are left-padded
/// with 16 `'0'`s (a 64-bit id in a 128-bit field, docs/api.md §4.1);
/// anything else — empty, odd, wrong length, non-hex — is rejected.
pub(crate) fn parse_trace_id(raw: &str) -> Result<String, TraceIdError> {
    if raw.len() != 16 && raw.len() != 32 {
        return Err(TraceIdError::InvalidLength(raw.to_string()));
    }
    if !raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(TraceIdError::NotHex(raw.to_string()));
    }
    let lowered = raw.to_ascii_lowercase();
    if lowered.len() == 16 {
        Ok(format!("0000000000000000{lowered}"))
    } else {
        Ok(lowered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_32_char_lowercase_hex_id_passes_through_unchanged() {
        assert_eq!(
            parse_trace_id("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
    }

    #[test]
    fn a_16_char_hex_id_is_left_padded_with_16_zeros() {
        assert_eq!(
            parse_trace_id("a3ce929d0e0e4736").unwrap(),
            "0000000000000000a3ce929d0e0e4736"
        );
    }

    #[test]
    fn mixed_case_hex_is_lowercased() {
        assert_eq!(
            parse_trace_id("4BF92F3577B34DA6A3CE929D0E0E4736").unwrap(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
    }

    #[test]
    fn an_odd_length_id_is_rejected() {
        assert!(matches!(
            parse_trace_id("abc"),
            Err(TraceIdError::InvalidLength(_))
        ));
    }

    #[test]
    fn a_non_hex_id_of_valid_length_is_rejected() {
        assert!(matches!(
            parse_trace_id("zzzzzzzzzzzzzzzz"),
            Err(TraceIdError::NotHex(_))
        ));
    }

    #[test]
    fn an_empty_id_is_rejected() {
        assert!(matches!(
            parse_trace_id(""),
            Err(TraceIdError::InvalidLength(_))
        ));
    }

    #[test]
    fn a_too_long_id_is_rejected() {
        let raw = "4bf92f3577b34da6a3ce929d0e0e47360";
        assert!(matches!(
            parse_trace_id(raw),
            Err(TraceIdError::InvalidLength(_))
        ));
    }

    #[test]
    fn a_between_lengths_id_is_rejected() {
        assert!(matches!(
            parse_trace_id("a3ce929d0e0e47361"), // 17 chars
            Err(TraceIdError::InvalidLength(_))
        ));
    }
}
