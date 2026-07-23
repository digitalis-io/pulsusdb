//! `&str -> Vec<Token>`. Handles double-quoted Go-escaped strings,
//! backtick raw strings (regex bodies), maximal-munch multi-char
//! operators (`>=` before `>>` before `>`, `&&`, `||`, `!=`, `!~`, `=~`),
//! single-group number/duration literals, and identifiers. Every token
//! carries a byte-offset [`Span`]; malformed input always yields a
//! [`TraceQlError`], never a panic — this is the crate's primary fuzz
//! surface.

use crate::error::TraceQlError;
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

/// Tokenizes a full TraceQL query. Never panics on any input, including
/// arbitrary bytes/UTF-8 that do not form a valid query — malformed input
/// always resolves to a `TraceQlError`.
///
/// Exposed (`#[doc(hidden)]` via `lib.rs`) solely so the golden-corpus
/// gate can prove every grammar-reachable [`TokenKind`] appears in at
/// least one accept case; it is not a supported API surface.
pub(crate) fn tokenize(input: &str) -> Result<Vec<Token>, TraceQlError> {
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
            ':' => {
                sc.advance();
                push(&mut tokens, TokenKind::Colon, start, sc.current_byte());
            }
            '~' => {
                sc.advance();
                push(&mut tokens, TokenKind::Tilde, start, sc.current_byte());
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
            '.' => {
                // `.5s` / `.5` — a leading-dot fraction is a literal, not
                // the unscoped-attribute `.attr` form.
                if matches!(sc.peek_at(1), Some(c) if c.is_ascii_digit()) {
                    let kind = scan_number_or_duration(&mut sc, start);
                    push(&mut tokens, kind, start, sc.current_byte());
                } else {
                    sc.advance();
                    push(&mut tokens, TokenKind::Dot, start, sc.current_byte());
                }
            }
            '=' => {
                sc.advance();
                match sc.peek() {
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
                    _ => push(&mut tokens, TokenKind::Bang, start, sc.current_byte()),
                }
            }
            '>' => {
                sc.advance();
                match sc.peek() {
                    Some('=') => {
                        sc.advance();
                        push(&mut tokens, TokenKind::Gte, start, sc.current_byte());
                    }
                    Some('>') => {
                        sc.advance();
                        push(&mut tokens, TokenKind::Shr, start, sc.current_byte());
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
                    Some('<') => {
                        sc.advance();
                        push(&mut tokens, TokenKind::Shl, start, sc.current_byte());
                    }
                    _ => push(&mut tokens, TokenKind::Lt, start, sc.current_byte()),
                }
            }
            '&' => {
                sc.advance();
                match sc.peek() {
                    Some('&') => {
                        sc.advance();
                        push(&mut tokens, TokenKind::AndAnd, start, sc.current_byte());
                    }
                    _ => push(&mut tokens, TokenKind::Amp, start, sc.current_byte()),
                }
            }
            '|' => {
                sc.advance();
                match sc.peek() {
                    Some('|') => {
                        sc.advance();
                        push(&mut tokens, TokenKind::OrOr, start, sc.current_byte());
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
            c if c.is_ascii_digit() => {
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
                return Err(TraceQlError::UnexpectedToken {
                    found: format!("{other:?}"),
                    expected: "a valid TraceQL token".to_string(),
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

/// The escape forms named in every malformed-escape error message.
const ESCAPE_EXPECTED: &str = "a valid escape (\\a, \\b, \\f, \\n, \\r, \\t, \\v, \\\\, \\\", \
                               \\xHH, \\NNN octal, \\uXXXX, or \\UXXXXXXXX)";

/// Scans a double-quoted string with the full Go escape grammar:
/// the short escapes `\a \b \f \n \r \t \v \\ \"`, `\xHH` (2 hex),
/// `\NNN` (exactly 3 octal digits), `\uXXXX` (4 hex), and `\UXXXXXXXX`
/// (8 hex). Unknown or malformed escapes are positioned errors, never
/// pass-through, and (as in Go) a raw newline inside the literal is an
/// error. One deliberate, loud divergence from Go's byte semantics
/// (task-manager ruling on #56, round-2 review): `\xHH`/`\NNN` values
/// above `0x7F` denote raw *bytes* in Go, where consecutive escapes can
/// compose into valid UTF-8 — canonically `"\xc3\xa9"`, which Go reads
/// as `"é"`. A Rust `String` cannot hold the intermediate lone bytes,
/// and composing them would add a byte-buffer decode path for marginal
/// value, so every byte escape above `0x7F` is rejected with a
/// positioned error pointing at `\uXXXX` instead — never silently
/// reinterpreted as a Latin-1 code point. If T8's differential gate
/// against real Tempo surfaces genuine `\xc3\xa9`-style usage, this
/// ruling gets revisited. `\u`/`\U` must be Unicode scalar values
/// (surrogates and > `0x10FFFF` rejected), as in Go.
///
/// `start` is the byte offset of the opening quote (already peeked, not
/// yet consumed).
fn scan_double_quoted(sc: &mut Scanner<'_>, start: usize) -> Result<String, TraceQlError> {
    sc.advance(); // opening quote
    let mut value = String::new();
    loop {
        match sc.peek() {
            None => {
                return Err(TraceQlError::UnterminatedString {
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
                let esc_start = sc.current_byte();
                sc.advance(); // backslash
                match sc.advance() {
                    None => {
                        return Err(TraceQlError::UnterminatedString {
                            span: Span {
                                start,
                                end: sc.len(),
                            },
                        });
                    }
                    Some('a') => value.push('\u{07}'),
                    Some('b') => value.push('\u{08}'),
                    Some('f') => value.push('\u{0C}'),
                    Some('n') => value.push('\n'),
                    Some('r') => value.push('\r'),
                    Some('t') => value.push('\t'),
                    Some('v') => value.push('\u{0B}'),
                    Some('\\') => value.push('\\'),
                    Some('"') => value.push('"'),
                    Some('x') => {
                        let code = scan_hex_escape(sc, start, esc_start, 2)?;
                        value.push(byte_escape_to_char(sc, esc_start, code)?);
                    }
                    Some(c @ '0'..='7') => {
                        let code = scan_octal_escape(sc, start, esc_start, c)?;
                        value.push(byte_escape_to_char(sc, esc_start, code)?);
                    }
                    Some('u') => {
                        let code = scan_hex_escape(sc, start, esc_start, 4)?;
                        value.push(unicode_escape_to_char(sc, esc_start, code)?);
                    }
                    Some('U') => {
                        let code = scan_hex_escape(sc, start, esc_start, 8)?;
                        value.push(unicode_escape_to_char(sc, esc_start, code)?);
                    }
                    Some(other) => {
                        return Err(TraceQlError::UnexpectedToken {
                            found: format!("escape sequence '\\{other}'"),
                            expected: ESCAPE_EXPECTED.to_string(),
                            span: Span {
                                start: esc_start,
                                end: sc.current_byte(),
                            },
                        });
                    }
                }
            }
            // Go disallows a raw newline inside an interpreted string
            // literal; it almost always means the closing quote was
            // forgotten, so the unterminated-string diagnostic (pointing
            // at the opening quote) beats silently going multiline.
            // Backtick raw strings still allow newlines, as in Go.
            Some('\n') => {
                return Err(TraceQlError::UnterminatedString {
                    span: Span {
                        start,
                        end: sc.current_byte(),
                    },
                });
            }
            Some(c) => {
                value.push(c);
                sc.advance();
            }
        }
    }
}

/// Consumes exactly `digits` hex digits after `\x`/`\u`/`\U` and returns
/// their value. EOF mid-escape is the string's unterminated error; a
/// non-hex character is a positioned malformed-escape error.
fn scan_hex_escape(
    sc: &mut Scanner<'_>,
    string_start: usize,
    esc_start: usize,
    digits: u32,
) -> Result<u32, TraceQlError> {
    let mut code: u32 = 0;
    for _ in 0..digits {
        match sc.peek() {
            None => {
                return Err(TraceQlError::UnterminatedString {
                    span: Span {
                        start: string_start,
                        end: sc.len(),
                    },
                });
            }
            Some(c) => match c.to_digit(16) {
                Some(d) => {
                    sc.advance();
                    code = code * 16 + d;
                }
                None => {
                    return Err(TraceQlError::UnexpectedToken {
                        found: format!("{c:?} in a hex escape (expected {digits} hex digits)"),
                        expected: ESCAPE_EXPECTED.to_string(),
                        span: Span {
                            start: esc_start,
                            end: sc.current_byte(),
                        },
                    });
                }
            },
        }
    }
    Ok(code)
}

/// Consumes the remaining two digits of a `\NNN` octal escape (`first`
/// is already consumed) and returns the value.
fn scan_octal_escape(
    sc: &mut Scanner<'_>,
    string_start: usize,
    esc_start: usize,
    first: char,
) -> Result<u32, TraceQlError> {
    // `first` is '0'..='7', so to_digit(8) always succeeds.
    let mut code: u32 = first.to_digit(8).unwrap_or(0);
    for _ in 0..2 {
        match sc.peek() {
            None => {
                return Err(TraceQlError::UnterminatedString {
                    span: Span {
                        start: string_start,
                        end: sc.len(),
                    },
                });
            }
            Some(c) => match c.to_digit(8) {
                Some(d) => {
                    sc.advance();
                    code = code * 8 + d;
                }
                None => {
                    return Err(TraceQlError::UnexpectedToken {
                        found: format!("{c:?} in an octal escape (expected 3 octal digits)"),
                        expected: ESCAPE_EXPECTED.to_string(),
                        span: Span {
                            start: esc_start,
                            end: sc.current_byte(),
                        },
                    });
                }
            },
        }
    }
    Ok(code)
}

/// Converts a `\xHH`/`\NNN` byte-escape value to a `char`, rejecting
/// values above `0x7F` (a raw non-ASCII byte is not representable as
/// UTF-8 text — see `scan_double_quoted`).
fn byte_escape_to_char(
    sc: &Scanner<'_>,
    esc_start: usize,
    code: u32,
) -> Result<char, TraceQlError> {
    if code > 0x7F {
        return Err(TraceQlError::UnexpectedToken {
            found: format!("byte escape value 0x{code:02X}"),
            expected: "a byte escape at or below 0x7F (non-ASCII bytes are not representable \
                       as UTF-8 text; use \\uXXXX)"
                .to_string(),
            span: Span {
                start: esc_start,
                end: sc.current_byte(),
            },
        });
    }
    char::from_u32(code).ok_or_else(|| unreachable_escape_error(sc, esc_start, code))
}

/// Converts a `\uXXXX`/`\UXXXXXXXX` value to a `char`, rejecting
/// surrogates and values above `U+10FFFF` (Go's "invalid Unicode code
/// point" rule).
fn unicode_escape_to_char(
    sc: &Scanner<'_>,
    esc_start: usize,
    code: u32,
) -> Result<char, TraceQlError> {
    char::from_u32(code).ok_or_else(|| TraceQlError::UnexpectedToken {
        found: format!("Unicode escape value 0x{code:X}"),
        expected: "a valid Unicode scalar value (not a surrogate, at most 0x10FFFF)".to_string(),
        span: Span {
            start: esc_start,
            end: sc.current_byte(),
        },
    })
}

/// Defensive-only: `byte_escape_to_char` has already bounded `code` to
/// `<= 0x7F`, for which `char::from_u32` always succeeds.
fn unreachable_escape_error(sc: &Scanner<'_>, esc_start: usize, code: u32) -> TraceQlError {
    TraceQlError::UnexpectedToken {
        found: format!("escape value 0x{code:X}"),
        expected: ESCAPE_EXPECTED.to_string(),
        span: Span {
            start: esc_start,
            end: sc.current_byte(),
        },
    }
}

/// Scans a backtick raw string (commonly used for regex bodies): no
/// escape processing, everything up to the next backtick is literal.
fn scan_backtick(sc: &mut Scanner<'_>, start: usize) -> Result<String, TraceQlError> {
    sc.advance(); // opening backtick
    let mut value = String::new();
    loop {
        match sc.peek() {
            None => {
                return Err(TraceQlError::UnterminatedString {
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

/// Scans a run starting at a digit or a `.`-then-digit: an unsigned
/// decimal number (`500`, `1.5`, `.5`), optionally followed by a maximal
/// run of letters. With letters it is one *single-group* `Duration`
/// token, valid unit or not — the lexer only decides the token kind;
/// `duration::parse_duration` owns unit-table validation, so an unknown
/// unit (`5x`) surfaces as a named `InvalidDuration` instead of silently
/// splitting into a `Number` plus a stray `Ident`. A digit after the unit
/// ends the token: compound literals (`1h30m`) lex as two `Duration`
/// tokens and the leftover produces a positioned parse error (docs/api.md
/// §4.2 — no compound literals).
fn scan_number_or_duration(sc: &mut Scanner<'_>, start: usize) -> TokenKind {
    if sc.peek() == Some('.') {
        sc.advance(); // leading-dot fraction (`.5s`); caller saw a digit next
    } else {
        while matches!(sc.peek(), Some(c) if c.is_ascii_digit()) {
            sc.advance();
        }
        if sc.peek() == Some('.') && matches!(sc.peek_at(1), Some(c) if c.is_ascii_digit()) {
            sc.advance();
        }
    }
    while matches!(sc.peek(), Some(c) if c.is_ascii_digit()) {
        sc.advance();
    }

    let mut is_duration = false;
    if matches!(sc.peek(), Some(c) if c.is_alphabetic()) {
        while matches!(sc.peek(), Some(c) if c.is_alphabetic()) {
            sc.advance();
        }
        is_duration = true;
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
    fn tokenizes_a_simple_spanset_filter() {
        assert_eq!(
            kinds(r#"{ name = "GET" }"#),
            vec![
                TokenKind::LBrace,
                TokenKind::Ident("name".to_string()),
                TokenKind::Eq,
                TokenKind::String("GET".to_string()),
                TokenKind::RBrace,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn maximal_munch_prefers_the_longest_operator() {
        assert_eq!(
            kinds(">= >> > <= << < && & || | != !~ =~ = ~ !"),
            vec![
                TokenKind::Gte,
                TokenKind::Shr,
                TokenKind::Gt,
                TokenKind::Lte,
                TokenKind::Shl,
                TokenKind::Lt,
                TokenKind::AndAnd,
                TokenKind::Amp,
                TokenKind::OrOr,
                TokenKind::Pipe,
                TokenKind::Neq,
                TokenKind::Nre,
                TokenKind::Re,
                TokenKind::Eq,
                TokenKind::Tilde,
                TokenKind::Bang,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn a_scoped_attribute_lexes_as_ident_dot_ident_chain() {
        assert_eq!(
            kinds("span.http.status_code"),
            vec![
                TokenKind::Ident("span".to_string()),
                TokenKind::Dot,
                TokenKind::Ident("http".to_string()),
                TokenKind::Dot,
                TokenKind::Ident("status_code".to_string()),
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn a_colon_scoped_intrinsic_lexes_as_ident_colon_ident() {
        assert_eq!(
            kinds("span:childCount"),
            vec![
                TokenKind::Ident("span".to_string()),
                TokenKind::Colon,
                TokenKind::Ident("childCount".to_string()),
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn a_leading_dot_before_a_letter_is_a_dot_token() {
        assert_eq!(
            kinds(".foo"),
            vec![
                TokenKind::Dot,
                TokenKind::Ident("foo".to_string()),
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn a_leading_dot_before_a_digit_is_a_fractional_literal() {
        assert_eq!(
            kinds(".5s"),
            vec![TokenKind::Duration(".5s".to_string()), TokenKind::Eof]
        );
        assert_eq!(
            kinds(".95"),
            vec![TokenKind::Number(".95".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn fractional_durations_lex_as_one_token() {
        assert_eq!(
            kinds("1.5s"),
            vec![TokenKind::Duration("1.5s".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn compound_durations_split_into_two_single_group_tokens() {
        assert_eq!(
            kinds("1h30m"),
            vec![
                TokenKind::Duration("1h".to_string()),
                TokenKind::Duration("30m".to_string()),
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
            kinds("500"),
            vec![TokenKind::Number("500".to_string()), TokenKind::Eof]
        );
        assert_eq!(
            kinds("1.5"),
            vec![TokenKind::Number("1.5".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn a_digit_run_followed_by_letters_always_lexes_as_one_duration_shaped_token() {
        assert_eq!(
            kinds("5x"),
            vec![TokenKind::Duration("5x".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn a_micro_sign_unit_lexes_inside_the_duration_token() {
        assert_eq!(
            kinds("500µs"),
            vec![TokenKind::Duration("500µs".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn a_minus_sign_is_never_folded_into_a_literal() {
        assert_eq!(
            kinds("-2s"),
            vec![
                TokenKind::Minus,
                TokenKind::Duration("2s".to_string()),
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn decodes_go_escapes_in_double_quoted_strings() {
        assert_eq!(
            kinds(r#""a\nb\t\"c\"""#),
            vec![TokenKind::String("a\nb\t\"c\"".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn decodes_every_short_escape() {
        assert_eq!(
            kinds(r#""\a\b\f\n\r\t\v\\\"""#),
            vec![
                TokenKind::String("\u{07}\u{08}\u{0C}\n\r\t\u{0B}\\\"".to_string()),
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn decodes_hex_escapes() {
        assert_eq!(
            kinds(r#""\x41\x7a\x00""#),
            vec![TokenKind::String("Az\u{0}".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn decodes_octal_escapes() {
        assert_eq!(
            kinds(r#""\101\012\177""#),
            vec![TokenKind::String("A\n\u{7F}".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn decodes_four_and_eight_digit_unicode_escapes() {
        assert_eq!(
            kinds(r#""\u00e9\u65e5\U0001F600""#),
            vec![TokenKind::String("é日😀".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn rejects_an_unknown_escape_with_a_position() {
        // `\z` is not pass-through: positioned error at the backslash.
        let err = tokenize(r#"{ name = "a\zb" }"#).unwrap_err();
        match err {
            TraceQlError::UnexpectedToken { found, span, .. } => {
                assert!(found.contains("\\z"), "found: {found}");
                assert_eq!(span.start, 11);
            }
            other => panic!("unexpected {other}"),
        }
    }

    #[test]
    fn rejects_a_single_quote_escape_like_go_string_literals_do() {
        let err = tokenize(r#""\'""#).unwrap_err();
        assert!(matches!(err, TraceQlError::UnexpectedToken { .. }));
    }

    #[test]
    fn rejects_a_malformed_hex_escape() {
        let err = tokenize(r#""\xZ9""#).unwrap_err();
        assert!(matches!(err, TraceQlError::UnexpectedToken { .. }));
    }

    #[test]
    fn rejects_a_malformed_octal_escape() {
        let err = tokenize(r#""\09""#).unwrap_err();
        assert!(matches!(err, TraceQlError::UnexpectedToken { .. }));
    }

    #[test]
    fn rejects_non_ascii_byte_escapes_instead_of_reinterpreting_them() {
        // Go's \xFF / \377 denote raw bytes, which UTF-8 text cannot
        // hold — loud rejection, not silent Latin-1 reinterpretation.
        for input in [r#""\xff""#, r#""\377""#, r#""\200""#] {
            let err = tokenize(input).unwrap_err();
            match err {
                TraceQlError::UnexpectedToken { expected, .. } => {
                    assert!(expected.contains("0x7F"), "input {input:?}: {expected}");
                }
                other => panic!("{input:?} -> unexpected {other}"),
            }
        }
    }

    #[test]
    fn rejects_surrogate_and_out_of_range_unicode_escapes() {
        for input in [r#""\uD800""#, r#""\UFFFFFFFF""#, r#""\U00110000""#] {
            let err = tokenize(input).unwrap_err();
            match err {
                TraceQlError::UnexpectedToken { expected, .. } => {
                    assert!(
                        expected.contains("Unicode scalar"),
                        "input {input:?}: {expected}"
                    );
                }
                other => panic!("{input:?} -> unexpected {other}"),
            }
        }
    }

    #[test]
    fn a_raw_newline_in_a_double_quoted_string_is_an_error_like_go() {
        let err = tokenize("{ name = \"line1\nline2\" }").unwrap_err();
        match err {
            TraceQlError::UnterminatedString { span } => {
                // Points at the opening quote, ends at the newline.
                assert_eq!(span.start, 9);
                assert_eq!(span.end, 15);
            }
            other => panic!("unexpected {other}"),
        }
    }

    #[test]
    fn a_raw_newline_in_a_backtick_string_is_still_allowed_like_go() {
        assert_eq!(
            kinds("`line1\nline2`"),
            vec![
                TokenKind::String("line1\nline2".to_string()),
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn an_escape_truncated_by_end_of_input_is_unterminated() {
        for input in [r#""\"#, r#""\x4"#, r#""\u00e"#, r#""\10"#] {
            let err = tokenize(input).unwrap_err();
            assert!(
                matches!(err, TraceQlError::UnterminatedString { .. }),
                "input {input:?}"
            );
        }
    }

    #[test]
    fn backtick_strings_are_raw_no_escape_processing() {
        assert_eq!(
            kinds(r#"`\d+\.\d+`"#),
            vec![TokenKind::String(r"\d+\.\d+".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn unterminated_double_quoted_string_is_an_error_not_a_panic() {
        let err = tokenize(r#"{ name = "abc"#).unwrap_err();
        assert!(matches!(err, TraceQlError::UnterminatedString { .. }));
    }

    #[test]
    fn unterminated_backtick_string_is_an_error_not_a_panic() {
        let err = tokenize("`abc").unwrap_err();
        assert!(matches!(err, TraceQlError::UnterminatedString { .. }));
    }

    #[test]
    fn an_unsupported_byte_is_a_lexer_error_not_a_panic() {
        let err = tokenize("{ .a = 1 } #").unwrap_err();
        assert!(matches!(err, TraceQlError::UnexpectedToken { .. }));
    }

    #[test]
    fn multi_byte_utf8_never_panics_the_scanner() {
        assert!(tokenize("日本語").is_err());
        assert!(tokenize(r#"{ name = "日本語" }"#).is_ok());
    }

    #[test]
    fn spans_are_byte_offsets_not_char_offsets() {
        let tokens = tokenize(r#"{name="日本語"}"#).unwrap();
        // '{' at byte 0, ident at 1..5, '=' at 5, the string token spans
        // the multi-byte value plus both quote bytes.
        assert_eq!(tokens[0].span, Span { start: 0, end: 1 });
        assert_eq!(tokens[1].span, Span { start: 1, end: 5 });
        assert_eq!(tokens[2].span, Span { start: 5, end: 6 });
        assert_eq!(tokens[3].span, Span { start: 6, end: 17 });
    }
}
