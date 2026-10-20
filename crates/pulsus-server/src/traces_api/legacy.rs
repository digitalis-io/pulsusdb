//! Legacy search-param compilation (issue #57; docs/api.md §4.2):
//! `tags` (logfmt `key=value` pairs) + `minDuration`/`maxDuration`
//! compile into a canonical TraceQL string which is then handed to
//! `pulsus_traceql::parse` — a **single validation path** (the parser is
//! the one grammar authority; this module never builds an AST by hand).
//!
//! Grammar (task-manager adjudication 4, pinned in plan v2 — enforced
//! **strictly**, code review round 1: no lenient acceptance of
//! out-of-grammar forms):
//! - `tags` is logfmt: space-separated `key=value` pairs; a value may be
//!   double-quoted to contain spaces/`=`; inside quotes `\"` and `\\`
//!   are the **only** escapes (anything else is a `400`). An unquoted
//!   value ends at whitespace and may contain neither `=` nor `"`; a
//!   quoted value must be followed by whitespace or end-of-input. A bare
//!   key with no `=`, an empty key, or an unterminated quote is a `400`
//!   — every grammar error names a byte offset into the decoded `tags`
//!   value, inside its message.
//! - a `tags` key given twice is a `400`, as the reference refuses it.
//! - `minDuration`/`maxDuration` are Go durations (the reference's
//!   `time.ParseDuration`) and compile to `duration >= <n>ns` /
//!   `duration <= <n>ns` conjuncts, exact to the nanosecond where the
//!   reference cuts to whole milliseconds; a malformed or negative one, or
//!   a `minDuration` above the `maxDuration`, is a `400`.
//! - All conjuncts join with `&&` inside one `{ … }`; empty legacy input
//!   compiles to `{}` (match-all, time-bounded).

use thiserror::Error;

/// Errors from the strict logfmt `tags` grammar — mapped to `400`, each
/// message naming the byte offset into the decoded `tags` value (issue
/// #384: the offset travels in the message, there is no `position` field).
#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum LegacyError {
    #[error("invalid 'tags' logfmt at byte {pos}: bare key {key:?} has no '=' value")]
    BareKey { key: String, pos: usize },
    #[error("invalid 'tags' logfmt at byte {pos}: unterminated quoted value for key {key:?}")]
    UnterminatedQuote { key: String, pos: usize },
    #[error("invalid 'tags' logfmt at byte {pos}: empty key")]
    EmptyKey { pos: usize },
    #[error(
        "invalid 'tags' logfmt at byte {pos}: unquoted value for key {key:?} contains '=' — \
         quote the value to include '='"
    )]
    UnquotedEquals { key: String, pos: usize },
    #[error(
        "invalid 'tags' logfmt at byte {pos}: unquoted value for key {key:?} contains '\"' — \
         a quote may only open the whole value"
    )]
    UnquotedQuote { key: String, pos: usize },
    #[error(
        "invalid 'tags' logfmt at byte {pos}: a quoted value must be followed by whitespace \
         or end of input"
    )]
    MissingSeparator { pos: usize },
    #[error(
        "invalid 'tags' logfmt at byte {pos}: unsupported escape '\\{escape}' — only \\\" and \
         \\\\ are recognized inside quotes"
    )]
    InvalidEscape { escape: char, pos: usize },
    /// A `tags` key given twice — the reference refuses it.
    #[error("invalid 'tags': key {key:?} is given more than once")]
    DuplicateKey { key: String },
    /// A `minDuration`/`maxDuration` that is not a Go duration.
    #[error("invalid {name}: {raw:?} is not a duration (e.g. 300ms, 1.5s, 1m30s)")]
    InvalidDuration { name: &'static str, raw: String },
    /// A negative `minDuration`/`maxDuration`.
    #[error("invalid {name}: {raw:?} is negative")]
    NegativeDuration { name: &'static str, raw: String },
    /// `minDuration` above `maxDuration`.
    #[error("invalid maxDuration: {max:?} is below minDuration {min:?}")]
    MinAboveMax { min: String, max: String },
}

/// One parsed logfmt pair.
#[derive(Debug, PartialEq, Eq)]
struct TagPair {
    key: String,
    value: String,
}

/// Parses the strict logfmt `tags` grammar (module doc).
fn parse_logfmt(tags: &str) -> Result<Vec<TagPair>, LegacyError> {
    let mut pairs = Vec::new();
    let mut it = tags.char_indices().peekable();
    loop {
        while it.peek().is_some_and(|(_, c)| c.is_whitespace()) {
            it.next();
        }
        let Some(&(key_start, _)) = it.peek() else {
            break;
        };
        let mut key = String::new();
        while let Some(&(_, c)) = it.peek() {
            if c == '=' || c.is_whitespace() {
                break;
            }
            key.push(c);
            it.next();
        }
        if key.is_empty() {
            return Err(LegacyError::EmptyKey { pos: key_start });
        }
        match it.peek() {
            Some(&(_, '=')) => {
                it.next();
            }
            _ => {
                return Err(LegacyError::BareKey {
                    key,
                    pos: key_start,
                });
            }
        }
        let mut value = String::new();
        if let Some(&(quote_pos, '"')) = it.peek() {
            it.next();
            let mut terminated = false;
            while let Some((_, c)) = it.next() {
                match c {
                    '"' => {
                        terminated = true;
                        break;
                    }
                    '\\' => match it.next() {
                        Some((_, '"')) => value.push('"'),
                        Some((_, '\\')) => value.push('\\'),
                        Some((escape_pos, other)) => {
                            return Err(LegacyError::InvalidEscape {
                                escape: other,
                                pos: escape_pos,
                            });
                        }
                        None => {
                            return Err(LegacyError::UnterminatedQuote {
                                key,
                                pos: quote_pos,
                            });
                        }
                    },
                    other => value.push(other),
                }
            }
            if !terminated {
                return Err(LegacyError::UnterminatedQuote {
                    key,
                    pos: quote_pos,
                });
            }
            // Strict: a quoted value ends the pair — the next character
            // must be whitespace or end-of-input (`a="x"b=y` is
            // out-of-grammar, never two pairs).
            if let Some(&(sep_pos, c)) = it.peek()
                && !c.is_whitespace()
            {
                return Err(LegacyError::MissingSeparator { pos: sep_pos });
            }
        } else {
            while let Some(&(char_pos, c)) = it.peek() {
                if c.is_whitespace() {
                    break;
                }
                if c == '=' {
                    return Err(LegacyError::UnquotedEquals { key, pos: char_pos });
                }
                if c == '"' {
                    return Err(LegacyError::UnquotedQuote { key, pos: char_pos });
                }
                value.push(c);
                it.next();
            }
        }
        pairs.push(TagPair { key, value });
    }
    Ok(pairs)
}

/// Escapes a raw value into a double-quoted TraceQL string literal
/// (local escaper — `pulsus_traceql`'s own `quote` is `pub(crate)`).
fn quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// Compiles the legacy params into the canonical TraceQL string handed
/// to `pulsus_traceql::parse` (exposed for the AC golden — the unit
/// tests round-trip it through the parser).
pub(crate) fn compile_legacy(
    tags: Option<&str>,
    min_duration: Option<&str>,
    max_duration: Option<&str>,
) -> Result<String, LegacyError> {
    let mut conjuncts: Vec<String> = Vec::new();
    if let Some(tags) = tags {
        let mut seen: Vec<String> = Vec::new();
        for pair in parse_logfmt(tags)? {
            if seen.contains(&pair.key) {
                return Err(LegacyError::DuplicateKey { key: pair.key });
            }
            conjuncts.push(format!(".{}={}", pair.key, quote(&pair.value)));
            seen.push(pair.key);
        }
    }
    let min_ns = min_duration
        .map(|raw| legacy_duration_ns("minDuration", raw))
        .transpose()?;
    let max_ns = max_duration
        .map(|raw| legacy_duration_ns("maxDuration", raw))
        .transpose()?;
    if let (Some(min), Some(max), Some(min_raw), Some(max_raw)) =
        (min_ns, max_ns, min_duration, max_duration)
        && min > 0
        && min > max
    {
        return Err(LegacyError::MinAboveMax {
            min: min_raw.to_string(),
            max: max_raw.to_string(),
        });
    }
    if let Some(min) = min_ns {
        conjuncts.push(format!("duration >= {min}ns"));
    }
    if let Some(max) = max_ns {
        conjuncts.push(format!("duration <= {max}ns"));
    }
    if conjuncts.is_empty() {
        Ok("{}".to_string())
    } else {
        Ok(format!("{{ {} }}", conjuncts.join(" && ")))
    }
}

/// One legacy duration parameter in nanoseconds: refused when it is not
/// a Go duration or is negative.
fn legacy_duration_ns(name: &'static str, raw: &str) -> Result<i64, LegacyError> {
    let ns = go_duration_ns(raw).ok_or_else(|| LegacyError::InvalidDuration {
        name,
        raw: raw.to_string(),
    })?;
    if ns < 0 {
        return Err(LegacyError::NegativeDuration {
            name,
            raw: raw.to_string(),
        });
    }
    Ok(ns)
}

/// A Go duration (`time.ParseDuration`) in nanoseconds: an optional sign,
/// then `0` alone or one or more `<number><unit>`, `<number>` digits with
/// an optional fraction and `<unit>` one of `ns us µs μs ms s m h`. A
/// fraction past the nanosecond is dropped; `None` on anything else or a
/// value past `i64`.
fn go_duration_ns(raw: &str) -> Option<i64> {
    const UNITS: [(&str, i128); 8] = [
        ("ns", 1),
        ("us", 1_000),
        ("µs", 1_000),
        ("μs", 1_000),
        ("ms", 1_000_000),
        ("s", 1_000_000_000),
        ("m", 60_000_000_000),
        ("h", 3_600_000_000_000),
    ];
    let (negative, body) = match raw.as_bytes().first() {
        Some(b'-') => (true, &raw[1..]),
        Some(b'+') => (false, &raw[1..]),
        _ => (false, raw),
    };
    if body == "0" {
        return Some(0);
    }
    if body.is_empty() {
        return None;
    }
    let mut rest = body;
    let mut total: i128 = 0;
    while !rest.is_empty() {
        let int_len = rest.bytes().take_while(u8::is_ascii_digit).count();
        let (int_part, after_int) = rest.split_at(int_len);
        let (frac_part, after_number) = match after_int.strip_prefix('.') {
            Some(f) => {
                let frac_len = f.bytes().take_while(u8::is_ascii_digit).count();
                f.split_at(frac_len)
            }
            None => ("", after_int),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return None;
        }
        // The longest unit, so `ms` is never `m` then `s`.
        let (unit, per_unit) = UNITS
            .iter()
            .filter(|(u, _)| after_number.starts_with(u))
            .max_by_key(|(u, _)| u.len())?;
        // Exact in `i128`: `digits · per_unit / 10^fraction_digits`,
        // truncated, so what falls below 1 ns is dropped. Fraction digits
        // past the eighteenth are below 1 ns for every unit.
        let frac_kept = &frac_part[..frac_part.len().min(18)];
        let digits: i128 = format!("0{int_part}{frac_kept}").parse().ok()?;
        let denominator = 10_i128.pow(frac_kept.len() as u32);
        let value = digits.checked_mul(*per_unit)? / denominator;
        total = total.checked_add(value)?;
        rest = &after_number[unit.len()..];
    }
    i64::try_from(if negative { -total } else { total }).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AC golden: the pinned logfmt → AST compilation (plan v2 legacy
    /// section) — equality is on the parsed AST, the single validation
    /// path.
    #[test]
    fn tags_and_min_duration_compile_to_the_pinned_equivalent_traceql() {
        // `100ms` compiles to its nanoseconds, the same value.
        let compiled =
            compile_legacy(Some("http.method=GET status=error"), Some("100ms"), None).unwrap();
        let compiled_ast = pulsus_traceql::parse(&compiled).expect("compiled TraceQL parses");
        let handwritten = pulsus_traceql::parse(
            r#"{ .http.method="GET" && .status="error" && duration >= 100ms }"#,
        )
        .expect("handwritten parses");
        assert_eq!(compiled_ast, handwritten);
    }

    #[test]
    fn a_quoted_value_keeps_spaces_and_the_two_documented_escapes() {
        let compiled =
            compile_legacy(Some(r#"msg="hello = \"world\" \\ end""#), None, None).unwrap();
        let ast = pulsus_traceql::parse(&compiled).expect("parses");
        let expected =
            pulsus_traceql::parse(r#"{ .msg="hello = \"world\" \\ end" }"#).expect("parses");
        assert_eq!(ast, expected);
    }

    #[test]
    fn a_bare_key_without_equals_is_rejected_with_its_offset() {
        assert_eq!(
            compile_legacy(Some("ok=1 http.method"), None, None),
            Err(LegacyError::BareKey {
                key: "http.method".to_string(),
                pos: 5,
            })
        );
    }

    #[test]
    fn an_unterminated_quote_is_rejected_with_the_quote_offset() {
        assert_eq!(
            compile_legacy(Some(r#"msg="oops"#), None, None),
            Err(LegacyError::UnterminatedQuote {
                key: "msg".to_string(),
                pos: 4,
            })
        );
    }

    #[test]
    fn an_unquoted_value_containing_equals_is_rejected_not_leniently_split() {
        // docs/api.md §4.2: a value may contain '=' only when quoted.
        assert_eq!(
            compile_legacy(Some("a=b=c"), None, None),
            Err(LegacyError::UnquotedEquals {
                key: "a".to_string(),
                pos: 3,
            })
        );
    }

    #[test]
    fn adjacent_pairs_without_a_separator_are_rejected() {
        // `a="x"b=y` — pairs are space-separated; a quoted value must be
        // followed by whitespace or end-of-input.
        assert_eq!(
            compile_legacy(Some(r#"a="x"b=y"#), None, None),
            Err(LegacyError::MissingSeparator { pos: 5 })
        );
    }

    #[test]
    fn an_unsupported_escape_inside_quotes_is_rejected() {
        // Only \" and \\ are documented escapes.
        assert_eq!(
            compile_legacy(Some(r#"a="x\ny""#), None, None),
            Err(LegacyError::InvalidEscape {
                escape: 'n',
                pos: 5,
            })
        );
    }

    #[test]
    fn a_quote_inside_an_unquoted_value_is_rejected() {
        assert_eq!(
            compile_legacy(Some(r#"a=b"c""#), None, None),
            Err(LegacyError::UnquotedQuote {
                key: "a".to_string(),
                pos: 3,
            })
        );
    }

    #[test]
    fn an_empty_key_is_rejected_with_its_offset() {
        assert_eq!(
            compile_legacy(Some("ok=1 =v"), None, None),
            Err(LegacyError::EmptyKey { pos: 5 })
        );
    }

    /// Issue #384: every variant names its byte offset in its own
    /// MESSAGE, which since the JSON envelope went away is the only place
    /// the offset reaches a client
    /// (`traces_api::error::plain_text_error`).
    #[test]
    fn every_error_variant_reports_its_position() {
        let cases: Vec<(LegacyError, usize)> = vec![
            (
                LegacyError::BareKey {
                    key: "k".into(),
                    pos: 7,
                },
                7,
            ),
            (
                LegacyError::UnterminatedQuote {
                    key: "k".into(),
                    pos: 3,
                },
                3,
            ),
            (LegacyError::EmptyKey { pos: 0 }, 0),
            (
                LegacyError::UnquotedEquals {
                    key: "k".into(),
                    pos: 9,
                },
                9,
            ),
            (
                LegacyError::UnquotedQuote {
                    key: "k".into(),
                    pos: 2,
                },
                2,
            ),
            (LegacyError::MissingSeparator { pos: 5 }, 5),
            (
                LegacyError::InvalidEscape {
                    escape: 'n',
                    pos: 4,
                },
                4,
            ),
        ];
        for (err, want) in cases {
            assert!(
                err.to_string().contains(&format!("byte {want}")),
                "{err}: must name byte {want}"
            );
        }
    }

    /// A key given twice is refused, as the reference refuses it
    /// (`http.go:197-199`), rather than compiled to an always-false
    /// conjunction.
    #[test]
    fn a_duplicate_key_is_refused() {
        assert_eq!(
            compile_legacy(Some("k=a k=b"), None, None),
            Err(LegacyError::DuplicateKey {
                key: "k".to_string()
            })
        );
    }

    #[test]
    fn empty_legacy_input_compiles_to_match_all() {
        assert_eq!(compile_legacy(None, None, None).unwrap(), "{}");
    }

    #[test]
    fn max_duration_lowers_to_a_lte_conjunct() {
        let compiled = compile_legacy(None, None, Some("2s")).unwrap();
        let ast = pulsus_traceql::parse(&compiled).expect("parses");
        let expected = pulsus_traceql::parse("{ duration <= 2s }").expect("parses");
        assert_eq!(ast, expected);
    }

    /// A duration that is not a Go duration is refused here, with its
    /// own variant, before any TraceQL is built.
    #[test]
    fn a_malformed_duration_is_refused_before_compilation() {
        for raw in ["100xs", "1.5", "s", "1m30", "--1s", ""] {
            assert_eq!(
                compile_legacy(None, Some(raw), None),
                Err(LegacyError::InvalidDuration {
                    name: "minDuration",
                    raw: raw.to_string()
                }),
                "{raw:?}"
            );
        }
        assert_eq!(
            compile_legacy(None, None, Some("2 s")),
            Err(LegacyError::InvalidDuration {
                name: "maxDuration",
                raw: "2 s".to_string()
            })
        );
    }

    /// P3: `minDuration`/`maxDuration` are Go durations, compiled exact to
    /// the nanosecond; a negative one, and a minimum above the maximum,
    /// are refused; and so is a `tags` key given twice.
    #[test]
    fn legacy_durations_are_go_durations() {
        let min = |raw: &str| compile_legacy(None, Some(raw), None);
        assert_eq!(min("1m30s").unwrap(), "{ duration >= 90000000000ns }");
        assert_eq!(min("0").unwrap(), "{ duration >= 0ns }");
        assert_eq!(min("1.5ms").unwrap(), "{ duration >= 1500000ns }");
        assert_eq!(min("1h2m3.5s").unwrap(), "{ duration >= 3723500000000ns }");
        assert_eq!(min("+2us").unwrap(), "{ duration >= 2000ns }");
        assert_eq!(min("3µs").unwrap(), "{ duration >= 3000ns }");
        for compiled in [min("1m30s").unwrap(), min("0").unwrap()] {
            assert!(pulsus_traceql::parse(&compiled).is_ok(), "{compiled}");
        }
        assert_eq!(
            min("-1s"),
            Err(LegacyError::NegativeDuration {
                name: "minDuration",
                raw: "-1s".to_string()
            })
        );
        assert_eq!(
            compile_legacy(None, Some("2s"), Some("1s")),
            Err(LegacyError::MinAboveMax {
                min: "2s".to_string(),
                max: "1s".to_string()
            })
        );
        assert_eq!(
            compile_legacy(None, Some("1s"), Some("2s")).unwrap(),
            "{ duration >= 1000000000ns && duration <= 2000000000ns }"
        );
        assert_eq!(
            compile_legacy(Some("a=1 a=2"), None, None),
            Err(LegacyError::DuplicateKey {
                key: "a".to_string()
            })
        );
    }

    #[test]
    fn an_empty_value_after_equals_is_an_empty_string_match() {
        let compiled = compile_legacy(Some("k="), None, None).unwrap();
        let ast = pulsus_traceql::parse(&compiled).expect("parses");
        let expected = pulsus_traceql::parse(r#"{ .k="" }"#).expect("parses");
        assert_eq!(ast, expected);
    }
}
