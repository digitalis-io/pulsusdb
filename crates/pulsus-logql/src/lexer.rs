//! `&str -> Vec<Token>`. Handles double-quoted Go-escaped strings,
//! backtick raw strings (regex bodies), maximal-munch multi-char
//! operators, compound duration/number literals, and identifiers. Every
//! token carries a byte-offset [`Span`]; malformed input always yields a
//! [`LogQlError`], never a panic — this is the crate's primary fuzz
//! surface (architect plan: "String forms").
//!
//! **Identifiers are Unicode, and the rule is the reference's** (issue
//! #392). An identifier is `_` or a general-category-`L` rune, then any
//! number of `_`, `L` or `Nd` runes — grafana/loki v3.7.4 builds its
//! scanner on Go `text/scanner` and never assigns `IsIdentRune`, so the
//! default predicate at `pkg/logql/syntax/query_scanner.go:338-343`
//! applies verbatim (`ch == '_' || unicode.IsLetter(ch) ||
//! unicode.IsDigit(ch) && i > 0`), with the leading rune taken through
//! it at `i == 0` (`:675`). That is NARROWER than "non-ASCII is
//! allowed": combining marks, `Nl` and `No` are refused, and a decimal
//! digit may not lead. [`crate::unicode_ident`] holds the predicate and
//! the measurements behind it.

use crate::error::LogQlError;
use crate::limits::CheckedQuery;
use crate::token::{Span, Token, TokenKind};
use crate::unicode_ident;

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
///
/// Takes [`CheckedQuery`], not `&str` (issue #279): the only constructor
/// is the `MAX_QUERY_BYTES` admission check, so a token stream cannot be
/// produced from unchecked input — a new parse entry point that forgets
/// the cap does not compile.
pub(crate) fn tokenize(input: CheckedQuery<'_>) -> Result<Vec<Token>, LogQlError> {
    let input = input.as_str();
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
                // `--<letter>...` is a stage flag (`--strict`, `--keep-empty`);
                // a lone `-`, or `--` not followed by a letter, stays `Minus`
                // (issue #200). Space-separated `x - -1` / `5 - -y` are
                // unaffected — they are not adjacent `--`.
                if sc.peek_at(1) == Some('-')
                    && matches!(sc.peek_at(2), Some(c) if c.is_ascii_alphabetic())
                {
                    sc.advance(); // first '-'
                    sc.advance(); // second '-'
                    let body_start = sc.current_byte();
                    while matches!(sc.peek(), Some(c) if c.is_ascii_alphanumeric() || c == '_' || c == '-')
                    {
                        sc.advance();
                    }
                    let end = sc.current_byte();
                    push(
                        &mut tokens,
                        TokenKind::Flag(input[body_start..end].to_string()),
                        start,
                        end,
                    );
                } else {
                    sc.advance();
                    push(&mut tokens, TokenKind::Minus, start, sc.current_byte());
                }
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
            // Identifier — the reference's rune set, not ASCII (issue
            // #392; see the module header for the citation). This arm
            // must stay BELOW the numeric arm above: a decimal digit may
            // not lead an identifier at the reference either
            // (`unicode.IsDigit(ch) && i > 0`), and `query_scanner.go`
            // orders `case s.isIdentRune(ch, 0)` before
            // `case isDecimal(ch)` for exactly that reason.
            c if unicode_ident::is_ident_start(c) => {
                while matches!(sc.peek(), Some(c) if unicode_ident::is_ident_continue(c)) {
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

/// Scans a double-quoted string with the reference's full escape grammar
/// (issue #400 Stage 1).
///
/// The rule is `prometheus/util/strutil.Unquote`
/// (`vendor/github.com/prometheus/prometheus/util/strutil/quote.go:66-231
/// @ v3.7.4`), which Loki's lexer calls on every `scanner.String` token
/// (`pkg/logql/syntax/lex.go:190-201 @ v3.7.4`) — so this is a LEXER
/// rule and reaches every LogQL construct that carries a string, not
/// particular regex positions. Accepted: the short escapes
/// `\a \b \f \n \r \t \v \\ \"`, `\xHH` (2 hex, a raw BYTE), `\NNN`
/// (exactly 3 octal digits, > 255 is an error), `\uXXXX` (4 hex) and
/// `\UXXXXXXXX` (8 hex, > `0x10FFFF` is an error). **Everything else is
/// an error**, including `\d`, `\w`, `\q`, `\0`, `\'` (`quote.go:220-224`
/// permits the escaped quote only when it IS the delimiter) and a raw
/// newline (`quote.go:85-87`).
///
/// This function used to end in `Some(other) => value.push(other)` —
/// drop the backslash, keep the character. That single arm produced both
/// halves of #400's escape divergence: `{app=~"\101"}` selected the
/// stream `101` where the reference selects `A` (**different rows, both
/// `200`, no error either side**), and `{app=~"\d+"}` was served here as
/// the pattern `d+` where the reference answers
/// `400 … invalid char escape`.
///
/// **Accumulation is into a `Vec<u8>`, not a `String`, and that is the
/// whole point of the byte buffer:** `\xHH`/`\NNN` denote raw bytes in
/// Go, so consecutive escapes compose — `"\xc3\xa9"` is the single
/// character `é`, measured on the pinned container. Decoding each escape
/// to `char::from_u32(HH)` instead would read them as Latin-1 and give
/// four bytes for that literal. `\uXXXX`/`\UXXXXXXXX` are Unicode code
/// points and are encoded as UTF-8, with a **surrogate decoding to
/// U+FFFD** rather than being refused — Go's `utf8.EncodeRune` maps an
/// invalid rune to `RuneError` (`quote.go:172-179` sets `multibyte` and
/// leaves the bounds check at `utf8.MaxRune`), which is why this differs
/// from `pulsus-traceql`'s otherwise-identical scanner.
///
/// **One deliberate narrowing, ledgered as `logql-string-escape-non-utf8`:**
/// a literal whose decoded bytes are not valid UTF-8 (`"\xff"`) is a
/// positioned error here, where the reference serves it at its five
/// `NewFastRegexMatcher` positions. No mounted ingest route can store
/// invalid UTF-8 (`LogRow.body: String`,
/// `pulsus-write/src/protocols/otlp_logs.rs:37-55`), so no line or label
/// value in this store could match such a pattern; `pulsus-traceql`
/// carries the identical ruling (#56).
///
/// `start` is the byte offset of the opening quote (already peeked, not
/// yet consumed).
fn scan_double_quoted(sc: &mut Scanner<'_>, start: usize) -> Result<String, LogQlError> {
    sc.advance(); // opening quote
    let mut bytes: Vec<u8> = Vec::new();
    let mut buf = [0u8; 4];
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
                let end = sc.current_byte();
                return String::from_utf8(bytes).map_err(|_| LogQlError::NonUtf8StringLiteral {
                    span: Span { start, end },
                });
            }
            Some('\\') => {
                let esc_start = sc.current_byte();
                sc.advance(); // backslash
                match sc.advance() {
                    None => {
                        return Err(LogQlError::UnterminatedString {
                            span: Span {
                                start,
                                end: sc.len(),
                            },
                        });
                    }
                    Some('a') => bytes.push(0x07),
                    Some('b') => bytes.push(0x08),
                    Some('f') => bytes.push(0x0C),
                    Some('n') => bytes.push(b'\n'),
                    Some('r') => bytes.push(b'\r'),
                    Some('t') => bytes.push(b'\t'),
                    Some('v') => bytes.push(0x0B),
                    Some('\\') => bytes.push(b'\\'),
                    Some('"') => bytes.push(b'"'),
                    Some('x') => {
                        let code = scan_hex_escape(sc, start, esc_start, 2)?;
                        // `\xHH` is one BYTE, whatever its value — the
                        // composition rule above.
                        bytes.push(code as u8);
                    }
                    Some(c @ '0'..='7') => {
                        let code = scan_octal_escape(sc, start, esc_start, c)?;
                        bytes.push(code as u8);
                    }
                    Some('u') => {
                        let code = scan_hex_escape(sc, start, esc_start, 4)?;
                        push_code_point(sc, &mut bytes, &mut buf, esc_start, code)?;
                    }
                    Some('U') => {
                        let code = scan_hex_escape(sc, start, esc_start, 8)?;
                        push_code_point(sc, &mut bytes, &mut buf, esc_start, code)?;
                    }
                    Some(_) => {
                        return Err(invalid_char_escape(sc, esc_start));
                    }
                }
            }
            // `quote.go:85-87` refuses a raw newline in an interpreted
            // literal outright; Go's own scanner stops the token there
            // too. It nearly always means the closing quote was
            // forgotten, so the unterminated-string diagnostic (pointing
            // at the opening quote) beats silently going multiline.
            // Backtick raw strings still allow newlines, as in Go.
            Some('\n') => {
                return Err(LogQlError::UnterminatedString {
                    span: Span {
                        start,
                        end: sc.current_byte(),
                    },
                });
            }
            Some(c) => {
                bytes.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                sc.advance();
            }
        }
    }
}

/// The offending escape's own source text, from the backslash to
/// wherever the scan stopped — `\d`, `\'`, `\x8"`. Never the whole
/// literal: the message names what the user has to change.
fn invalid_char_escape(sc: &Scanner<'_>, esc_start: usize) -> LogQlError {
    let end = sc.current_byte();
    LogQlError::InvalidCharEscape {
        escape: sc.input[esc_start..end].to_string(),
        span: Span {
            start: esc_start,
            end,
        },
    }
}

/// Consumes exactly `digits` hex digits after `\x`/`\u`/`\U` and returns
/// their value. EOF mid-escape is the string's unterminated error (there
/// is no closing quote to be found); anything else that is not a hex
/// digit — including the closing quote itself, which is Go's
/// `len(s) < n` case — is a positioned invalid-escape error.
fn scan_hex_escape(
    sc: &mut Scanner<'_>,
    string_start: usize,
    esc_start: usize,
    digits: u32,
) -> Result<u32, LogQlError> {
    let mut code: u32 = 0;
    for _ in 0..digits {
        match sc.peek() {
            None => {
                return Err(LogQlError::UnterminatedString {
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
                    sc.advance();
                    return Err(invalid_char_escape(sc, esc_start));
                }
            },
        }
    }
    Ok(code)
}

/// Consumes the remaining two digits of a `\NNN` octal escape (`first`
/// is already consumed) and returns the value, which Go bounds at 255
/// (`quote.go:202-205`) because the escape denotes a byte.
fn scan_octal_escape(
    sc: &mut Scanner<'_>,
    string_start: usize,
    esc_start: usize,
    first: char,
) -> Result<u32, LogQlError> {
    // `first` is '0'..='7', so `to_digit(8)` always succeeds.
    let mut code: u32 = first.to_digit(8).unwrap_or(0);
    for _ in 0..2 {
        match sc.peek() {
            None => {
                return Err(LogQlError::UnterminatedString {
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
                    sc.advance();
                    return Err(invalid_char_escape(sc, esc_start));
                }
            },
        }
    }
    if code > 255 {
        return Err(invalid_char_escape(sc, esc_start));
    }
    Ok(code)
}

/// Appends a `\uXXXX`/`\UXXXXXXXX` code point as UTF-8. Above
/// `utf8.MaxRune` is Go's error (`quote.go:180-183`); a **surrogate is
/// not** — `utf8.EncodeRune` writes U+FFFD for it, which is what the
/// container returns (`|~ "\ud800"` matches a U+FFFD line, measured).
fn push_code_point(
    sc: &Scanner<'_>,
    bytes: &mut Vec<u8>,
    buf: &mut [u8; 4],
    esc_start: usize,
    code: u32,
) -> Result<(), LogQlError> {
    if code > 0x10_FFFF {
        return Err(invalid_char_escape(sc, esc_start));
    }
    let c = char::from_u32(code).unwrap_or(char::REPLACEMENT_CHARACTER);
    bytes.extend_from_slice(c.encode_utf8(buf).as_bytes());
    Ok(())
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

    /// Test shim over the #279 `CheckedQuery` seam — every lexer-test
    /// input is far below `MAX_QUERY_BYTES`.
    fn tok(input: &str) -> Result<Vec<Token>, LogQlError> {
        tokenize(CheckedQuery::new(input)?)
    }

    fn kinds(input: &str) -> Vec<TokenKind> {
        tok(input).unwrap().into_iter().map(|t| t.kind).collect()
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
    fn a_double_dash_flag_lexes_as_a_flag_token() {
        assert_eq!(
            kinds("--strict"),
            vec![TokenKind::Flag("strict".to_string()), TokenKind::Eof]
        );
        assert_eq!(
            kinds("--keep-empty"),
            vec![TokenKind::Flag("keep-empty".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn space_separated_minus_signs_do_not_lex_as_a_flag() {
        // `a - -2` / `5 - -1`: the `-`s are not adjacent `--`, so each stays
        // a `Minus` (issue #200 — the flag token must not steal binary/unary
        // minus).
        assert_eq!(
            kinds("a - -2"),
            vec![
                TokenKind::Ident("a".to_string()),
                TokenKind::Minus,
                TokenKind::Minus,
                TokenKind::Number("2".to_string()),
                TokenKind::Eof,
            ]
        );
        assert_eq!(
            kinds("5 - -1"),
            vec![
                TokenKind::Number("5".to_string()),
                TokenKind::Minus,
                TokenKind::Minus,
                TokenKind::Number("1".to_string()),
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
        let err = tok(r#""abc"#).unwrap_err();
        assert!(matches!(err, LogQlError::UnterminatedString { .. }));
    }

    #[test]
    fn unterminated_backtick_string_is_an_error_not_a_panic() {
        let err = tok("`abc").unwrap_err();
        assert!(matches!(err, LogQlError::UnterminatedString { .. }));
    }

    #[test]
    fn a_lone_bang_is_a_lexer_error_not_a_panic() {
        let err = tok("{a!b}").unwrap_err();
        assert!(matches!(err, LogQlError::UnexpectedToken { .. }));
    }

    #[test]
    fn an_unsupported_byte_is_a_lexer_error_not_a_panic() {
        let err = tok("{a=\"b\"} #").unwrap_err();
        assert!(matches!(err, LogQlError::UnexpectedToken { .. }));
    }

    #[test]
    fn multi_byte_utf8_never_panics_the_scanner() {
        // Issue #392. This used to assert `tok("日本語").is_err()` — it
        // was the one place in the workspace pinning the behaviour that
        // issue calls a defect, so it is rewritten rather than deleted.
        // `日本語` is three general-category-`L` runes, so it is ONE
        // identifier spanning bytes 0..9 (three bytes each), exactly as
        // the reference's scanner tokenises it.
        let tokens = tok("日本語").expect("a non-ASCII identifier lexes");
        assert_eq!(tokens.len(), 2, "one Ident plus Eof: {tokens:?}");
        assert_eq!(tokens[0].kind, TokenKind::Ident("日本語".to_string()));
        assert_eq!(tokens[0].span, Span { start: 0, end: 9 });
        assert!(matches!(tokens[1].kind, TokenKind::Eof));

        // The panic-freedom half, which is what this test is named for:
        // non-ASCII inside a string, and a non-ASCII rune that is NOT an
        // identifier rune (U+00BD, general category No) still resolves to
        // a clean error rather than a bad slice boundary.
        assert!(tok(r#"{app="日本語"}"#).is_ok());
        assert!(tok("½").is_err());
        assert!(tok("é½").is_err());
    }

    #[test]
    fn spans_are_byte_offsets_not_char_offsets() {
        let tokens = tok(r#"{app="日本語"}"#).unwrap();
        // '{' at byte 0, ident at 1..4, '=' at 4, the string token spans
        // the multi-byte value plus both quote bytes.
        assert_eq!(tokens[0].span, Span { start: 0, end: 1 });
        assert_eq!(tokens[1].span, Span { start: 1, end: 4 });
    }
}
