//! `/api/traces/v1` parameter parsing: the trace-fetch hex id
//! (docs/api.md §4.1: "16 or 32 chars, left-padded" — the injection
//! boundary for `point_read_sql`'s `unhex('...')` literal: only
//! `[0-9a-f]{32}` output ever leaves [`parse_trace_id`]) and the issue
//! #57 search params (docs/api.md §4.2: `q`/legacy params, `start`/`end`
//! unix seconds, `limit`, `spss`). The `(key, value)` pair core mirrors
//! `logs_api`/`prom_api`'s per-surface copies (each module owns its
//! params, established convention).

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

/// Default `limit` when the param is absent (docs/api.md §4.2).
pub(crate) const DEFAULT_LIMIT: u32 = 20;
/// Default `spss` (spans-per-spanset cap) when the param is absent.
pub(crate) const DEFAULT_SPSS: u32 = 3;

/// Errors from parsing `/api/traces/v1/search` request parameters —
/// mapped to `400 bad_data` by `error::ApiError`.
#[derive(Debug, Error)]
pub(crate) enum SearchParamError {
    #[error("missing required parameter {0:?}: start and end (unix seconds) are required")]
    MissingRange(&'static str),
    #[error("invalid timestamp {0:?}: expected unix seconds")]
    InvalidTimestamp(String),
    #[error("invalid range: end ({end}) must be greater than start ({start})")]
    InvalidRange { start: i64, end: i64 },
    #[error("invalid {name:?} {raw:?}: expected a positive integer")]
    InvalidCount { name: &'static str, raw: String },
    #[error(
        "'q' and the legacy search params (tags/minDuration/maxDuration) are mutually \
         exclusive: supply one or the other, never both"
    )]
    ConflictingQuery,
}

/// The raw, percent-decoded search request — `q` XOR the legacy params
/// (validated here; compilation of either into a TraceQL AST is
/// `search.rs`/`legacy.rs`'s job).
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RawSearchParams {
    pub q: Option<String>,
    pub tags: Option<String>,
    pub min_duration: Option<String>,
    pub max_duration: Option<String>,
    pub start_ns: i64,
    pub end_ns: i64,
    pub limit: u32,
    pub spss: u32,
}

/// Parses the search query string (docs/api.md §4.2). `q` present with
/// any legacy param present is an explicit `400` — never silent
/// precedence (task-manager ratification on plan v2).
pub(crate) fn parse_search_params(raw: &str) -> Result<RawSearchParams, SearchParamError> {
    let pairs = parse_pairs(raw);
    let q = get(&pairs, "q")
        .map(str::to_string)
        .filter(|s| !s.is_empty());
    let tags = get(&pairs, "tags")
        .map(str::to_string)
        .filter(|s| !s.is_empty());
    let min_duration = get(&pairs, "minDuration")
        .map(str::to_string)
        .filter(|s| !s.is_empty());
    let max_duration = get(&pairs, "maxDuration")
        .map(str::to_string)
        .filter(|s| !s.is_empty());
    if q.is_some() && (tags.is_some() || min_duration.is_some() || max_duration.is_some()) {
        return Err(SearchParamError::ConflictingQuery);
    }
    let start_ns = parse_unix_seconds_ns(&pairs, "start")?;
    let end_ns = parse_unix_seconds_ns(&pairs, "end")?;
    if end_ns <= start_ns {
        return Err(SearchParamError::InvalidRange {
            start: start_ns / 1_000_000_000,
            end: end_ns / 1_000_000_000,
        });
    }
    let limit = parse_count(&pairs, "limit", DEFAULT_LIMIT)?;
    let spss = parse_count(&pairs, "spss", DEFAULT_SPSS)?;
    Ok(RawSearchParams {
        q,
        tags,
        min_duration,
        max_duration,
        start_ns,
        end_ns,
        limit,
        spss,
    })
}

fn parse_unix_seconds_ns(
    pairs: &[(String, String)],
    name: &'static str,
) -> Result<i64, SearchParamError> {
    let raw = get(pairs, name).ok_or(SearchParamError::MissingRange(name))?;
    let secs: i64 = raw
        .parse()
        .map_err(|_| SearchParamError::InvalidTimestamp(raw.to_string()))?;
    secs.checked_mul(1_000_000_000)
        .ok_or_else(|| SearchParamError::InvalidTimestamp(raw.to_string()))
}

fn parse_count(
    pairs: &[(String, String)],
    name: &'static str,
    default: u32,
) -> Result<u32, SearchParamError> {
    let Some(raw) = get(pairs, name) else {
        return Ok(default);
    };
    raw.parse::<u32>()
        .ok()
        .filter(|n| *n >= 1)
        .ok_or_else(|| SearchParamError::InvalidCount {
            name,
            raw: raw.to_string(),
        })
}

/// Errors from parsing the `/api/traces/v1/tags` query parameters —
/// mapped to `400 bad_data` by `error::ApiError` (issue #58).
#[derive(Debug, Error)]
pub(crate) enum TagsParamError {
    #[error(
        "unsupported scope {0:?}: expected \"resource\" or \"span\" (or omit the parameter \
         for both scopes)"
    )]
    UnsupportedScope(String),
}

/// Errors from parsing the `{tag}` path parameter of
/// `/api/traces/v1/tag/{tag}/values` — mapped to `400 bad_data`.
#[derive(Debug, Error)]
pub(crate) enum TagPathError {
    #[error("invalid tag: the attribute key must be non-empty")]
    EmptyKey,
}

/// The parsed `/api/traces/v1/tags` request: only `scope` filters.
/// `start`/`end` are accepted for client compatibility and IGNORED —
/// `trace_tag_catalog` has no timestamp column, so tag discovery is
/// time-less by contract (docs/api.md §4.3, issue #58 frozen-schema
/// resolution); any other parameter is likewise ignored.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TagsParams {
    pub scope: Option<String>,
}

/// Parses the `/api/traces/v1/tags` query string (docs/api.md §4.3):
/// `scope` ∈ {`resource`, `span`, absent} — anything else (including
/// `intrinsic`/`none` and the empty string) is an explicit `400`, never
/// silently widened to "all scopes" (task-manager adjudication 4 on
/// issue #58).
pub(crate) fn parse_tags_params(raw: &str) -> Result<TagsParams, TagsParamError> {
    let pairs = parse_pairs(raw);
    let scope = match get(&pairs, "scope") {
        None => None,
        Some(s @ ("resource" | "span")) => Some(s.to_string()),
        Some(other) => return Err(TagsParamError::UnsupportedScope(other.to_string())),
    };
    Ok(TagsParams { scope })
}

/// Splits the `{tag}` path parameter into `(scope, key)` (docs/api.md
/// §4.3): a `resource.`/`span.` prefix scopes the lookup; a leading `.`
/// or a bare key is unscoped (both scopes). The remainder after the
/// prefix is the verbatim attribute key (`resource.service.name` →
/// scope `resource`, key `service.name`); an empty remainder is a `400`.
pub(crate) fn parse_tag_path(raw_tag: &str) -> Result<(Option<String>, String), TagPathError> {
    let (scope, key) = if let Some(key) = raw_tag.strip_prefix("resource.") {
        (Some("resource".to_string()), key)
    } else if let Some(key) = raw_tag.strip_prefix("span.") {
        (Some("span".to_string()), key)
    } else if let Some(key) = raw_tag.strip_prefix('.') {
        (None, key)
    } else {
        (None, raw_tag)
    };
    if key.is_empty() {
        return Err(TagPathError::EmptyKey);
    }
    Ok((scope, key.to_string()))
}

/// Splits a query string into ordered, percent-decoded `(key, value)`
/// pairs — the same per-surface pair core as `logs_api`/`prom_api`.
pub(crate) fn parse_pairs(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut it = pair.splitn(2, '=');
            let k = it.next().unwrap_or("");
            let v = it.next().unwrap_or("");
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

/// The first value for `key`, if present.
pub(crate) fn get<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// `application/x-www-form-urlencoded` percent-decoding: `+` → space,
/// `%XX` → the raw byte; malformed escapes pass through best-effort
/// (same behavior as the logs/prom copies).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 3 <= bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    None => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
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

    // -- search params ---------------------------------------------------

    #[test]
    fn a_minimal_q_request_parses_with_defaults() {
        let p = parse_search_params("q=%7B%7D&start=100&end=200").unwrap();
        assert_eq!(p.q.as_deref(), Some("{}"));
        assert_eq!(p.start_ns, 100_000_000_000);
        assert_eq!(p.end_ns, 200_000_000_000);
        assert_eq!(p.limit, DEFAULT_LIMIT);
        assert_eq!(p.spss, DEFAULT_SPSS);
    }

    #[test]
    fn missing_start_or_end_is_rejected() {
        assert!(matches!(
            parse_search_params("q=%7B%7D&end=200"),
            Err(SearchParamError::MissingRange("start"))
        ));
        assert!(matches!(
            parse_search_params("q=%7B%7D&start=100"),
            Err(SearchParamError::MissingRange("end"))
        ));
    }

    #[test]
    fn unparseable_start_is_rejected() {
        assert!(matches!(
            parse_search_params("start=abc&end=200"),
            Err(SearchParamError::InvalidTimestamp(_))
        ));
    }

    #[test]
    fn an_inverted_range_is_rejected() {
        assert!(matches!(
            parse_search_params("start=200&end=100"),
            Err(SearchParamError::InvalidRange { .. })
        ));
    }

    #[test]
    fn non_numeric_or_zero_limit_and_spss_are_rejected() {
        for query in [
            "start=1&end=2&limit=abc",
            "start=1&end=2&limit=0",
            "start=1&end=2&spss=abc",
            "start=1&end=2&spss=0",
        ] {
            assert!(
                matches!(
                    parse_search_params(query),
                    Err(SearchParamError::InvalidCount { .. })
                ),
                "{query} must be rejected"
            );
        }
    }

    #[test]
    fn q_together_with_any_legacy_param_is_a_conflict_never_precedence() {
        for query in [
            "q=%7B%7D&tags=a%3Db&start=1&end=2",
            "q=%7B%7D&minDuration=100ms&start=1&end=2",
            "q=%7B%7D&maxDuration=1s&start=1&end=2",
        ] {
            assert!(
                matches!(
                    parse_search_params(query),
                    Err(SearchParamError::ConflictingQuery)
                ),
                "{query} must conflict"
            );
        }
    }

    #[test]
    fn a_pure_legacy_request_parses() {
        let p =
            parse_search_params("tags=http.method%3DGET&minDuration=100ms&start=1&end=2").unwrap();
        assert_eq!(p.q, None);
        assert_eq!(p.tags.as_deref(), Some("http.method=GET"));
        assert_eq!(p.min_duration.as_deref(), Some("100ms"));
    }

    #[test]
    fn an_empty_request_is_a_time_only_search_not_an_error() {
        let p = parse_search_params("start=1&end=2").unwrap();
        assert_eq!(p.q, None);
        assert_eq!(p.tags, None);
    }

    // -- tags params (issue #58) -------------------------------------------

    #[test]
    fn tags_scope_resource_and_span_parse() {
        assert_eq!(
            parse_tags_params("scope=resource")
                .unwrap()
                .scope
                .as_deref(),
            Some("resource")
        );
        assert_eq!(
            parse_tags_params("scope=span").unwrap().scope.as_deref(),
            Some("span")
        );
    }

    #[test]
    fn tags_absent_scope_means_both_scopes() {
        assert_eq!(parse_tags_params("").unwrap().scope, None);
    }

    #[test]
    fn tags_start_end_are_accepted_and_ignored_not_errors() {
        // The catalog is time-less: the bounds parse away without effect
        // (docs/api.md §4.3).
        let p = parse_tags_params("scope=span&start=100&end=200").unwrap();
        assert_eq!(p.scope.as_deref(), Some("span"));
    }

    #[test]
    fn tags_unknown_scope_is_rejected_never_widened() {
        for raw in ["scope=bogus", "scope=intrinsic", "scope=none", "scope="] {
            assert!(
                matches!(
                    parse_tags_params(raw),
                    Err(TagsParamError::UnsupportedScope(_))
                ),
                "{raw} must be rejected"
            );
        }
    }

    #[test]
    fn tag_path_prefixes_resolve_to_scopes() {
        assert_eq!(
            parse_tag_path("resource.service.name").unwrap(),
            (Some("resource".to_string()), "service.name".to_string())
        );
        assert_eq!(
            parse_tag_path("span.x").unwrap(),
            (Some("span".to_string()), "x".to_string())
        );
    }

    #[test]
    fn tag_path_leading_dot_and_bare_keys_are_unscoped() {
        assert_eq!(parse_tag_path(".x").unwrap(), (None, "x".to_string()));
        assert_eq!(parse_tag_path("x").unwrap(), (None, "x".to_string()));
    }

    #[test]
    fn tag_path_empty_keys_are_rejected() {
        for raw in ["", ".", "resource.", "span."] {
            assert!(
                matches!(parse_tag_path(raw), Err(TagPathError::EmptyKey)),
                "{raw:?} must be rejected"
            );
        }
    }
}
