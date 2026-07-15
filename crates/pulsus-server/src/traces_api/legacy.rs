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
//!   key with no `=`, an empty key, or an unterminated quote is a `400
//!   bad_data` — every grammar error carries a `position` (byte offset
//!   into the decoded `tags` value).
//! - `minDuration`/`maxDuration` compile to `duration >= <lit>` /
//!   `duration <= <lit>` conjuncts; a malformed duration surfaces as the
//!   parser's positioned error → `400`.
//! - All conjuncts join with `&&` inside one `{ … }`; empty legacy input
//!   compiles to `{}` (match-all, time-bounded).

use thiserror::Error;

/// Errors from the strict logfmt `tags` grammar — mapped to `400
/// bad_data` with `position` = the byte offset into the decoded `tags`
/// value ([`LegacyError::pos`]).
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
}

impl LegacyError {
    /// The byte offset into the decoded `tags` value — surfaced as the
    /// error envelope's `position` field.
    pub(crate) fn pos(&self) -> usize {
        match self {
            LegacyError::BareKey { pos, .. }
            | LegacyError::UnterminatedQuote { pos, .. }
            | LegacyError::EmptyKey { pos }
            | LegacyError::UnquotedEquals { pos, .. }
            | LegacyError::UnquotedQuote { pos, .. }
            | LegacyError::MissingSeparator { pos }
            | LegacyError::InvalidEscape { pos, .. } => *pos,
        }
    }
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
        for pair in parse_logfmt(tags)? {
            conjuncts.push(format!(".{}={}", pair.key, quote(&pair.value)));
        }
    }
    if let Some(min) = min_duration {
        conjuncts.push(format!("duration >= {min}"));
    }
    if let Some(max) = max_duration {
        conjuncts.push(format!("duration <= {max}"));
    }
    if conjuncts.is_empty() {
        Ok("{}".to_string())
    } else {
        Ok(format!("{{ {} }}", conjuncts.join(" && ")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AC golden: the pinned logfmt → AST compilation (plan v2 legacy
    /// section) — equality is on the parsed AST, the single validation
    /// path.
    #[test]
    fn tags_and_min_duration_compile_to_the_pinned_equivalent_traceql() {
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
            assert_eq!(err.pos(), want, "{err}");
        }
    }

    #[test]
    fn duplicate_keys_become_repeated_conjuncts() {
        let compiled = compile_legacy(Some("k=a k=b"), None, None).unwrap();
        let ast = pulsus_traceql::parse(&compiled).expect("parses");
        let expected = pulsus_traceql::parse(r#"{ .k="a" && .k="b" }"#).expect("parses");
        assert_eq!(ast, expected);
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

    #[test]
    fn a_malformed_duration_surfaces_as_a_parser_error_single_validation_path() {
        let compiled = compile_legacy(None, Some("100xs"), None).unwrap();
        assert!(pulsus_traceql::parse(&compiled).is_err());
    }

    #[test]
    fn an_empty_value_after_equals_is_an_empty_string_match() {
        let compiled = compile_legacy(Some("k="), None, None).unwrap();
        let ast = pulsus_traceql::parse(&compiled).expect("parses");
        let expected = pulsus_traceql::parse(r#"{ .k="" }"#).expect("parses");
        assert_eq!(ast, expected);
    }
}
