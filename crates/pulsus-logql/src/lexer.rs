//! `&str -> Vec<Token>`. Handles double-quoted Go-escaped strings,
//! backtick raw strings (regex bodies), maximal-munch multi-char
//! operators, compound duration/number literals, and identifiers. Every
//! token carries a byte-offset [`Span`]; malformed input always yields a
//! [`LogQlError`], never a panic — this is the crate's primary fuzz
//! surface (architect plan: "String forms").

use crate::error::LogQlError;
use crate::token::{Span, Token, TokenKind};

/// Walks a `&str` by `char`, tracking byte offsets — indexing by `char`
/// position (not raw byte index) keeps every slice operation on a valid
/// UTF-8 boundary without manual boundary arithmetic, so multi-byte units
/// like `µs` and arbitrary fuzzed UTF-8 can never panic on a bad slice.
struct Scanner<'a> {
    input: &'a str,
    chars: Vec<(usize, char)>,
    pos: usize,
}

impl<'a> Scanner<'a> {
    fn new(input: &'a str) -> Self {
        Scanner {
            input,
            chars: input.char_indices().collect(),
            pos: 0,
        }
    }

    fn len(&self) -> usize {
        self.input.len()
    }

    fn byte_offset(&self, idx: usize) -> usize {
        self.chars.get(idx).map_or(self.len(), |(b, _)| *b)
    }

    fn current_byte(&self) -> usize {
        self.byte_offset(self.pos)
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).map(|(_, c)| *c)
    }

    fn peek_at(&self, ahead: usize) -> Option<char> {
        self.chars.get(self.pos + ahead).map(|(_, c)| *c)
    }

    fn advance(&mut self) -> Option<char> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }
}

fn push(tokens: &mut Vec<Token>, kind: TokenKind, start: usize, end: usize) {
    tokens.push(Token {
        kind,
        span: Span { start, end },
    });
}

/// Tokenizes a full LogQL query. Never panics on any input, including
/// arbitrary bytes/UTF-8 that do not form a valid query — malformed input
/// always resolves to a `LogQlError`.
pub(crate) fn tokenize(input: &str) -> Result<Vec<Token>, LogQlError> {
    let mut sc = Scanner::new(input);
    let mut tokens = Vec::new();

    while let Some(c) = sc.peek() {
        let start = sc.current_byte();
        match c {
            ' ' | '\t' | '\r' | '\n' => {
                sc.advance();
            }
            '{' => {
                sc.advance();
                push(&mut tokens, TokenKind::LBrace, start, sc.current_byte());
            }
            '}' => {
                sc.advance();
                push(&mut tokens, TokenKind::RBrace, start, sc.current_byte());
            }
            '(' => {
                sc.advance();
                push(&mut tokens, TokenKind::LParen, start, sc.current_byte());
            }
            ')' => {
                sc.advance();
                push(&mut tokens, TokenKind::RParen, start, sc.current_byte());
            }
            '[' => {
                sc.advance();
                push(&mut tokens, TokenKind::LBracket, start, sc.current_byte());
            }
            ']' => {
                sc.advance();
                push(&mut tokens, TokenKind::RBracket, start, sc.current_byte());
            }
            ',' => {
                sc.advance();
                push(&mut tokens, TokenKind::Comma, start, sc.current_byte());
            }
            '+' => {
                sc.advance();
                push(&mut tokens, TokenKind::Plus, start, sc.current_byte());
            }
            '-' => {
                sc.advance();
                push(&mut tokens, TokenKind::Minus, start, sc.current_byte());
            }
            '*' => {
                sc.advance();
                push(&mut tokens, TokenKind::Star, start, sc.current_byte());
            }
            '/' => {
                sc.advance();
                push(&mut tokens, TokenKind::Slash, start, sc.current_byte());
            }
            '%' => {
                sc.advance();
                push(&mut tokens, TokenKind::Percent, start, sc.current_byte());
            }
            '^' => {
                sc.advance();
                push(&mut tokens, TokenKind::Caret, start, sc.current_byte());
            }
            '=' => {
                sc.advance();
                match sc.peek() {
                    Some('=') => {
                        sc.advance();
                        push(&mut tokens, TokenKind::EqEq, start, sc.current_byte());
                    }
                    Some('~') => {
                        sc.advance();
                        push(&mut tokens, TokenKind::Re, start, sc.current_byte());
                    }
                    _ => push(&mut tokens, TokenKind::Eq, start, sc.current_byte()),
                }
            }
            '!' => {
                sc.advance();
                match sc.peek() {
                    Some('=') => {
                        sc.advance();
                        push(&mut tokens, TokenKind::Neq, start, sc.current_byte());
                    }
                    Some('~') => {
                        sc.advance();
                        push(&mut tokens, TokenKind::Nre, start, sc.current_byte());
                    }
                    _ => {
                        return Err(LogQlError::UnexpectedToken {
                            found: "'!'".to_string(),
                            expected: "'!=' or '!~'".to_string(),
                            span: Span {
                                start,
                                end: sc.current_byte(),
                            },
                        });
                    }
                }
            }
            '>' => {
                sc.advance();
                match sc.peek() {
                    Some('=') => {
                        sc.advance();
                        push(&mut tokens, TokenKind::Gte, start, sc.current_byte());
                    }
                    _ => push(&mut tokens, TokenKind::Gt, start, sc.current_byte()),
                }
            }
            '<' => {
                sc.advance();
                match sc.peek() {
                    Some('=') => {
                        sc.advance();
                        push(&mut tokens, TokenKind::Lte, start, sc.current_byte());
                    }
                    _ => push(&mut tokens, TokenKind::Lt, start, sc.current_byte()),
                }
            }
            '|' => {
                sc.advance();
                match sc.peek() {
                    Some('=') => {
                        sc.advance();
                        push(&mut tokens, TokenKind::PipeExact, start, sc.current_byte());
                    }
                    Some('~') => {
                        sc.advance();
                        push(&mut tokens, TokenKind::PipeMatch, start, sc.current_byte());
                    }
                    _ => push(&mut tokens, TokenKind::Pipe, start, sc.current_byte()),
                }
            }
            '"' => {
                let value = scan_double_quoted(&mut sc, start)?;
                push(
                    &mut tokens,
                    TokenKind::String(value),
                    start,
                    sc.current_byte(),
                );
            }
            '`' => {
                let value = scan_backtick(&mut sc, start)?;
                push(
                    &mut tokens,
                    TokenKind::String(value),
                    start,
                    sc.current_byte(),
                );
            }
            c if c.is_ascii_digit()
                || (c == '.' && matches!(sc.peek_at(1), Some(d) if d.is_ascii_digit())) =>
            {
                // A leading `.` followed by a digit begins a
                // fractional literal (`.5`, `.5s`) — Loki accepts a
                // leading-dot mantissa in label-filter/unwrap position.
                let kind = scan_number_or_duration(&mut sc, start);
                push(&mut tokens, kind, start, sc.current_byte());
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                while matches!(sc.peek(), Some(c) if c.is_ascii_alphanumeric() || c == '_') {
                    sc.advance();
                }
                let end = sc.current_byte();
                push(
                    &mut tokens,
                    TokenKind::Ident(input[start..end].to_string()),
                    start,
                    end,
                );
            }
            other => {
                sc.advance();
                return Err(LogQlError::UnexpectedToken {
                    found: format!("{other:?}"),
                    expected: "a valid LogQL token".to_string(),
                    span: Span {
                        start,
                        end: sc.current_byte(),
                    },
                });
            }
        }
    }

    let end = sc.len();
    tokens.push(Token {
        kind: TokenKind::Eof,
        span: Span { start: end, end },
    });
    Ok(tokens)
}

/// Scans a double-quoted, Go-escaped string. `start` is the byte offset
/// of the opening quote (already peeked, not yet consumed).
fn scan_double_quoted(sc: &mut Scanner<'_>, start: usize) -> Result<String, LogQlError> {
    sc.advance(); // opening quote
    let mut value = String::new();
    loop {
        match sc.peek() {
            None => {
                return Err(LogQlError::UnterminatedString {
                    span: Span {
                        start,
                        end: sc.len(),
                    },
                });
            }
            Some('"') => {
                sc.advance();
                return Ok(value);
            }
            Some('\\') => {
                sc.advance();
                match sc.advance() {
                    None => {
                        return Err(LogQlError::UnterminatedString {
                            span: Span {
                                start,
                                end: sc.len(),
                            },
                        });
                    }
                    Some('n') => value.push('\n'),
                    Some('t') => value.push('\t'),
                    Some('r') => value.push('\r'),
                    // Anything else (`\\`, `\"`, `` \` ``, or an unknown
                    // escape) passes through as the literal character —
                    // lenient by design, this is a proof-subset lexer,
                    // not a full Go-string validator.
                    Some(other) => value.push(other),
                }
            }
            Some(c) => {
                value.push(c);
                sc.advance();
            }
        }
    }
}

/// Scans a backtick raw string (commonly used for regex bodies): no
/// escape processing, everything up to the next backtick is literal.
fn scan_backtick(sc: &mut Scanner<'_>, start: usize) -> Result<String, LogQlError> {
    sc.advance(); // opening backtick
    let mut value = String::new();
    loop {
        match sc.peek() {
            None => {
                return Err(LogQlError::UnterminatedString {
                    span: Span {
                        start,
                        end: sc.len(),
                    },
                });
            }
            Some('`') => {
                sc.advance();
                return Ok(value);
            }
            Some(c) => {
                value.push(c);
                sc.advance();
            }
        }
    }
}

/// Scans a run starting at a digit (or a leading `.` before a digit):
/// either a plain/decimal number, or a compound duration literal (one or
/// more `<mantissa><unit>` groups, e.g. `1h30m`, `1.5s`, `.5s`,
/// `1h1.5m`). Each mantissa may carry a fractional `.<digits>` part.
/// Loki accepts fractional durations in label-filter/unwrap position
/// (`time.ParseDuration` semantics); a fractional mantissa followed by a
/// unit lexes here as a single `Duration` token so the fractional value
/// reaches `parse_duration_seconds` intact. A fractional mantissa with no
/// unit stays a plain `Number` (`2.5`, `.5`). The lexer only decides
/// *which kind* of token this is — unit-to-nanoseconds work lives in
/// `duration::parse_duration` / `parse_duration_seconds`.
fn scan_number_or_duration(sc: &mut Scanner<'_>, start: usize) -> TokenKind {
    // A mantissa is an optional integer digit run plus an optional
    // `.<digits>` fraction (a leading `.5` has no integer part); at least
    // one digit must appear. Returns whether any digit was consumed.
    fn scan_mantissa(sc: &mut Scanner<'_>) -> bool {
        let mut consumed = false;
        while matches!(sc.peek(), Some(c) if c.is_ascii_digit()) {
            sc.advance();
            consumed = true;
        }
        if sc.peek() == Some('.') && matches!(sc.peek_at(1), Some(c) if c.is_ascii_digit()) {
            sc.advance(); // '.'
            while matches!(sc.peek(), Some(c) if c.is_ascii_digit()) {
                sc.advance();
            }
            consumed = true;
        }
        consumed
    }

    let mut is_duration = false;
    loop {
        if !scan_mantissa(sc) {
            break;
        }

        if matches!(sc.peek(), Some(c) if c.is_alphabetic()) {
            // A maximal run of letters right after a mantissa is always
            // *shaped* like a duration unit, valid or not — the lexer
            // only decides the token kind; `duration::parse_duration`
            // owns unit-table validation, so an unknown unit (`5x`) or a
            // corrupted one (`5se`) surfaces as a named `InvalidDuration`
            // parse error instead of silently splitting into a `Number`
            // plus a stray `Ident`. A fractional mantissa + unit (`1.5s`)
            // stays one `Duration` token; the range parser still rejects
            // it downstream (`parse_duration` is integer-only), matching
            // Loki, while `parse_duration_seconds` accepts it for label
            // filters.
            while matches!(sc.peek(), Some(c) if c.is_alphabetic()) {
                sc.advance();
            }
            is_duration = true;
            // A compound duration continues if another mantissa follows
            // (the "30m" in "1h30m", the "1.5m" in "1h1.5m").
            if matches!(sc.peek(), Some(c) if c.is_ascii_digit())
                || (sc.peek() == Some('.')
                    && matches!(sc.peek_at(1), Some(c) if c.is_ascii_digit()))
            {
                continue;
            }
            break;
        }
        break;
    }

    let end = sc.current_byte();
    let raw = sc.input[start..end].to_string();
    if is_duration {
        TokenKind::Duration(raw)
    } else {
        TokenKind::Number(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(input: &str) -> Vec<TokenKind> {
        tokenize(input)
            .unwrap()
            .into_iter()
            .map(|t| t.kind)
            .collect()
    }

    #[test]
    fn tokenizes_a_simple_selector() {
        assert_eq!(
            kinds(r#"{app="x"}"#),
            vec![
                TokenKind::LBrace,
                TokenKind::Ident("app".to_string()),
                TokenKind::Eq,
                TokenKind::String("x".to_string()),
                TokenKind::RBrace,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn maximal_munch_prefers_two_char_operators() {
        assert_eq!(
            kinds("!= !~ =~ |= |~ == >= <="),
            vec![
                TokenKind::Neq,
                TokenKind::Nre,
                TokenKind::Re,
                TokenKind::PipeExact,
                TokenKind::PipeMatch,
                TokenKind::EqEq,
                TokenKind::Gte,
                TokenKind::Lte,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn tokenizes_a_compound_duration_as_one_token() {
        assert_eq!(
            kinds("[1h30m]"),
            vec![
                TokenKind::LBracket,
                TokenKind::Duration("1h30m".to_string()),
                TokenKind::RBracket,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn milliseconds_do_not_lex_as_meters_then_seconds() {
        assert_eq!(
            kinds("500ms"),
            vec![TokenKind::Duration("500ms".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn a_bare_number_has_no_unit() {
        assert_eq!(
            kinds("42"),
            vec![TokenKind::Number("42".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn a_decimal_number_is_not_mistaken_for_a_duration() {
        assert_eq!(
            kinds("0.95"),
            vec![TokenKind::Number("0.95".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn a_fractional_mantissa_with_a_unit_lexes_as_one_duration_token() {
        // Loki accepts fractional durations in label-filter/unwrap RHS
        // position; the whole mantissa+unit must be one Duration token so
        // the fraction survives to parse_duration_seconds.
        assert_eq!(
            kinds("1.5s"),
            vec![TokenKind::Duration("1.5s".to_string()), TokenKind::Eof]
        );
        assert_eq!(
            kinds("250.5ms"),
            vec![TokenKind::Duration("250.5ms".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn a_leading_dot_fractional_duration_lexes_as_one_duration_token() {
        assert_eq!(
            kinds(".5s"),
            vec![TokenKind::Duration(".5s".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn a_compound_duration_with_a_fractional_component() {
        assert_eq!(
            kinds("1h1.5m"),
            vec![TokenKind::Duration("1h1.5m".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn a_fractional_mantissa_without_a_unit_stays_a_number() {
        assert_eq!(
            kinds("2.5"),
            vec![TokenKind::Number("2.5".to_string()), TokenKind::Eof]
        );
        assert_eq!(
            kinds(".5"),
            vec![TokenKind::Number(".5".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn a_digit_run_followed_by_letters_always_lexes_as_one_duration_shaped_token() {
        // The lexer only decides the token *kind*; it does not validate
        // the unit — "5se" (not a real unit) still lexes as a single
        // Duration token so `duration::parse_duration` can reject it with
        // a named `InvalidDuration`, rather than silently splitting into
        // `Number("5")` + `Ident("se")`.
        assert_eq!(
            kinds("5se"),
            vec![TokenKind::Duration("5se".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn decodes_go_escapes_in_double_quoted_strings() {
        assert_eq!(
            kinds(r#""a\nb\t\"c\"""#),
            vec![TokenKind::String("a\nb\t\"c\"".to_string()), TokenKind::Eof,]
        );
    }

    #[test]
    fn backtick_strings_are_raw_no_escape_processing() {
        assert_eq!(
            kinds(r#"`\d+\.\d+`"#),
            vec![TokenKind::String(r"\d+\.\d+".to_string()), TokenKind::Eof,]
        );
    }

    #[test]
    fn unterminated_double_quoted_string_is_an_error_not_a_panic() {
        let err = tokenize(r#""abc"#).unwrap_err();
        assert!(matches!(err, LogQlError::UnterminatedString { .. }));
    }

    #[test]
    fn unterminated_backtick_string_is_an_error_not_a_panic() {
        let err = tokenize("`abc").unwrap_err();
        assert!(matches!(err, LogQlError::UnterminatedString { .. }));
    }

    #[test]
    fn a_lone_bang_is_a_lexer_error_not_a_panic() {
        let err = tokenize("{a!b}").unwrap_err();
        assert!(matches!(err, LogQlError::UnexpectedToken { .. }));
    }

    #[test]
    fn an_unsupported_byte_is_a_lexer_error_not_a_panic() {
        let err = tokenize("{a=\"b\"} #").unwrap_err();
        assert!(matches!(err, LogQlError::UnexpectedToken { .. }));
    }

    #[test]
    fn multi_byte_utf8_never_panics_the_scanner() {
        // Arbitrary non-ASCII input, including inside and outside
        // strings — must error cleanly, never panic on a slice boundary.
        assert!(tokenize("日本語").is_err());
        assert!(tokenize(r#"{app="日本語"}"#).is_ok());
    }

    #[test]
    fn spans_are_byte_offsets_not_char_offsets() {
        let tokens = tokenize(r#"{app="日本語"}"#).unwrap();
        // '{' at byte 0, ident at 1..4, '=' at 4, the string token spans
        // the multi-byte value plus both quote bytes.
        assert_eq!(tokens[0].span, Span { start: 0, end: 1 });
        assert_eq!(tokens[1].span, Span { start: 1, end: 4 });
    }
}
