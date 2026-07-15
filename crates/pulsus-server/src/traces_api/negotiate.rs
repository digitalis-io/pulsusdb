//! RFC 9110 §12.5.1 `Accept` negotiation for the trace-fetch response
//! (issue #55 plan v2 §4 / v3 §3), hand-rolled — no new dependency. Two
//! served representations: `application/json` (the default) and
//! `application/protobuf`; `application/x-protobuf` is accepted as a
//! request-side alias for the latter (the response `Content-Type` is
//! always `application/protobuf`, never `x-protobuf` — see docs/api.md
//! §4.1's documented ingest/response asymmetry).
//!
//! Semantics (plan v3, as amended by the round-3 adjudication):
//! - No `Accept` header ⇒ JSON (Tempo's default) — the default path, not
//!   the 406 path.
//! - A served type's *effective quality* is the `q` of its most specific
//!   matching range (exact `type/subtype` > `type/*` > `*/*`); a served
//!   type with **no** matching range has no effective quality and is
//!   excluded, exactly like a matching range with `q=0`.
//! - Highest effective quality wins; a tie ⇒ JSON.
//! - Both served types excluded ⇒ `406 not_acceptable` — this includes
//!   `Accept: application/protobuf;q=0` (protobuf excluded by q=0, JSON
//!   has no matching range), per the task-manager's round-3 amendment.

use axum::http::{HeaderMap, header};

use super::error::ApiError;

/// Which representation the client gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Wants {
    Json,
    Protobuf,
}

/// Match specificity, ordered: higher wins when picking the range that
/// determines a served type's effective quality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Specificity {
    Wildcard,     // */*
    TypeWildcard, // application/*
    Exact,        // application/json, application/protobuf, application/x-protobuf
}

/// One parsed media range: the lowercased `type/subtype` and its quality
/// value (default 1.0; a malformed `q` drops the whole range, per plan).
#[derive(Debug)]
struct MediaRange {
    media: String,
    q: f32,
}

fn parse_ranges(accept: &str) -> Vec<MediaRange> {
    let mut ranges = Vec::new();
    for part in accept.split(',') {
        let mut segments = part.split(';');
        let Some(media) = segments.next() else {
            continue;
        };
        let media = media.trim().to_ascii_lowercase();
        if media.is_empty() {
            continue;
        }
        let mut q = 1.0f32;
        let mut malformed = false;
        for param in segments {
            let Some((name, value)) = param.split_once('=') else {
                continue;
            };
            if name.trim().eq_ignore_ascii_case("q") {
                match value.trim().parse::<f32>() {
                    Ok(v) if (0.0..=1.0).contains(&v) => q = v,
                    _ => malformed = true,
                }
            }
        }
        if !malformed {
            ranges.push(MediaRange { media, q });
        }
    }
    ranges
}

/// The effective quality of a served type whose exact spellings are
/// `exact_names`, under `ranges`: the `q` of the most specific matching
/// range (ties at the same specificity resolve to the highest `q` —
/// deterministic regardless of header order). `None` when no range
/// matches at all (⇒ excluded, plan v3 §3).
fn effective_q(ranges: &[MediaRange], exact_names: &[&str], type_wildcard: &str) -> Option<f32> {
    let mut best: Option<(Specificity, f32)> = None;
    for range in ranges {
        let spec = if exact_names.contains(&range.media.as_str()) {
            Specificity::Exact
        } else if range.media == type_wildcard {
            Specificity::TypeWildcard
        } else if range.media == "*/*" {
            Specificity::Wildcard
        } else {
            continue;
        };
        let better = match best {
            None => true,
            Some((s, q)) => spec > s || (spec == s && range.q > q),
        };
        if better {
            best = Some((spec, range.q));
        }
    }
    best.map(|(_, q)| q)
}

/// Negotiates from the request's headers, combining **every** `Accept`
/// field line first (issue #55 code review [medium]: RFC 9110 §5.3 — a
/// field repeated across lines is semantically the comma-joined
/// combination; reading only the first line would drop later lines'
/// ranges). Lines that are not valid UTF-8 are skipped, matching the
/// previous single-line behaviour (`to_str().ok()`); no readable line at
/// all ⇒ the absent-header JSON default.
pub(crate) fn negotiate_from_headers(headers: &HeaderMap) -> Result<Wants, ApiError> {
    let lines: Vec<&str> = headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .collect();
    if lines.is_empty() {
        return negotiate(None);
    }
    negotiate(Some(&lines.join(", ")))
}

/// Negotiates the trace-fetch response representation from the request's
/// (already line-combined) `Accept` header value. `None`/blank ⇒ JSON
/// default; an unmatchable or fully-excluded header ⇒
/// `Err(ApiError::NotAcceptable)` (406).
fn negotiate(accept: Option<&str>) -> Result<Wants, ApiError> {
    let Some(accept) = accept else {
        return Ok(Wants::Json);
    };
    if accept.trim().is_empty() {
        return Ok(Wants::Json);
    }
    let ranges = parse_ranges(accept);

    // `q=0` excludes, exactly like "no matching range" (RFC 9110 §12.4.2:
    // "not acceptable").
    let available = |q: Option<f32>| q.filter(|&q| q > 0.0);
    let json_q = available(effective_q(&ranges, &["application/json"], "application/*"));
    let proto_q = available(effective_q(
        &ranges,
        &["application/protobuf", "application/x-protobuf"],
        "application/*",
    ));

    match (json_q, proto_q) {
        (None, None) => Err(ApiError::NotAcceptable),
        (Some(_), None) => Ok(Wants::Json),
        (None, Some(_)) => Ok(Wants::Protobuf),
        // Tie ⇒ JSON (the default representation).
        (Some(j), Some(p)) => Ok(if p > j { Wants::Protobuf } else { Wants::Json }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_accept_header_defaults_to_json() {
        assert_eq!(negotiate(None).unwrap(), Wants::Json);
    }

    #[test]
    fn a_blank_accept_header_defaults_to_json() {
        assert_eq!(negotiate(Some("   ")).unwrap(), Wants::Json);
    }

    #[test]
    fn a_bare_wildcard_defaults_to_json() {
        assert_eq!(negotiate(Some("*/*")).unwrap(), Wants::Json);
    }

    #[test]
    fn application_protobuf_selects_protobuf() {
        assert_eq!(
            negotiate(Some("application/protobuf")).unwrap(),
            Wants::Protobuf
        );
    }

    #[test]
    fn application_x_protobuf_is_a_request_side_alias_for_protobuf() {
        assert_eq!(
            negotiate(Some("application/x-protobuf")).unwrap(),
            Wants::Protobuf
        );
    }

    /// Task-manager round-3 amendment: with protobuf excluded by `q=0` and
    /// no range matching JSON, NO served type is acceptable — 406, not a
    /// JSON fallback (superseding the v2 test expectation).
    #[test]
    fn protobuf_q_zero_alone_leaves_nothing_acceptable_and_is_406() {
        assert!(matches!(
            negotiate(Some("application/protobuf;q=0")),
            Err(ApiError::NotAcceptable)
        ));
    }

    /// The round-3 companion case: an explicit acceptable JSON range makes
    /// the `q=0` protobuf exclusion fall back to JSON.
    #[test]
    fn protobuf_q_zero_with_an_explicit_json_range_falls_back_to_json() {
        assert_eq!(
            negotiate(Some("application/protobuf;q=0, application/json")).unwrap(),
            Wants::Json
        );
    }

    #[test]
    fn quality_precedence_picks_the_higher_q() {
        assert_eq!(
            negotiate(Some("application/protobuf;q=0.9, application/json;q=0.8")).unwrap(),
            Wants::Protobuf
        );
    }

    #[test]
    fn both_served_types_at_q_zero_is_406() {
        assert!(matches!(
            negotiate(Some("application/json;q=0, application/protobuf;q=0")),
            Err(ApiError::NotAcceptable)
        ));
    }

    #[test]
    fn media_types_match_case_insensitively() {
        assert_eq!(
            negotiate(Some("Application/Protobuf")).unwrap(),
            Wants::Protobuf
        );
    }

    /// Plan v3 §3: a present-but-unmatched header (no served type matches
    /// any range) is 406, identical to the all-`q=0` case.
    #[test]
    fn a_single_unmatched_range_is_406() {
        assert!(matches!(
            negotiate(Some("text/plain")),
            Err(ApiError::NotAcceptable)
        ));
    }

    #[test]
    fn an_unmatched_range_with_an_excluded_wildcard_is_406() {
        assert!(matches!(
            negotiate(Some("text/plain, */*;q=0")),
            Err(ApiError::NotAcceptable)
        ));
    }

    #[test]
    fn an_equal_quality_tie_resolves_to_json() {
        assert_eq!(
            negotiate(Some("application/protobuf, application/json")).unwrap(),
            Wants::Json
        );
    }

    #[test]
    fn a_type_wildcard_matches_both_and_ties_to_json() {
        assert_eq!(negotiate(Some("application/*")).unwrap(), Wants::Json);
    }

    /// "malformed q ⇒ ignore that range" (plan v2 §4): the protobuf range
    /// is dropped wholesale, leaving only the JSON range.
    #[test]
    fn a_range_with_a_malformed_q_is_ignored() {
        assert_eq!(
            negotiate(Some("application/protobuf;q=abc, application/json")).unwrap(),
            Wants::Json
        );
    }

    fn header_map(lines: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for line in lines {
            headers.append(header::ACCEPT, line.parse().expect("header value"));
        }
        headers
    }

    /// RFC 9110 §5.3: repeated `Accept` field lines combine into one
    /// comma-joined field before negotiation — the later line's range MUST
    /// participate. With both an exact `q=0` and an exact `q=1` range for
    /// JSON in the combined set, this parser's documented precedence
    /// (same-specificity ties resolve to the **highest** q — see
    /// `effective_q`) makes JSON acceptable: the later non-zero range
    /// governs, the earlier `q=0` does not veto it.
    #[test]
    fn repeated_accept_lines_combine_and_the_highest_q_exact_range_governs() {
        let headers = header_map(&["application/json;q=0", "application/json"]);
        assert_eq!(negotiate_from_headers(&headers).unwrap(), Wants::Json);
    }

    /// Repeated lines where only the *second* names an acceptable type:
    /// reading only the first line would 406; the combined set selects
    /// protobuf.
    #[test]
    fn a_second_accept_line_with_the_only_acceptable_type_participates() {
        let headers = header_map(&["text/plain", "application/protobuf"]);
        assert_eq!(negotiate_from_headers(&headers).unwrap(), Wants::Protobuf);
    }

    #[test]
    fn negotiate_from_headers_without_an_accept_header_defaults_to_json() {
        assert_eq!(
            negotiate_from_headers(&HeaderMap::new()).unwrap(),
            Wants::Json
        );
    }

    #[test]
    fn a_single_accept_line_behaves_exactly_like_the_string_form() {
        let headers = header_map(&["application/x-protobuf"]);
        assert_eq!(negotiate_from_headers(&headers).unwrap(), Wants::Protobuf);
    }

    #[test]
    fn repeated_all_unacceptable_lines_are_406() {
        let headers = header_map(&["text/plain", "text/html;q=0.9"]);
        assert!(matches!(
            negotiate_from_headers(&headers),
            Err(ApiError::NotAcceptable)
        ));
    }

    #[test]
    fn an_exact_match_beats_a_wildcard_for_the_same_type() {
        // Exact protobuf q=0 excludes it even though */* would grant q=1;
        // JSON still matches the wildcard.
        assert_eq!(
            negotiate(Some("application/protobuf;q=0, */*")).unwrap(),
            Wants::Json
        );
    }
}
