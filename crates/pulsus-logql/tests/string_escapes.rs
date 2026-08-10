//! **The LogQL string-literal escape grammar** — issue #400 Stage 1.
//!
//! A LogQL double-quoted string is decoded by
//! `prometheus/util/strutil.Unquote`
//! (`vendor/github.com/prometheus/prometheus/util/strutil/quote.go:66-231
//! @ v3.7.4`), which Loki's lexer calls on every string token
//! (`pkg/logql/syntax/lex.go:190-201 @ v3.7.4`). Because it is a LEXER
//! rule it applies to every construct that carries a string — the
//! selector, a line filter, `| regexp`, a `line_format` template — and
//! the only thing that moves between them is the byte offset.
//!
//! `scan_double_quoted` used to know `\n`, `\t` and `\r` and end in
//! `Some(other) => value.push(other)`: drop the backslash, keep the
//! character. **That one arm produced wrong ROWS, not merely a lenient
//! accept.** `{app=~"\101"}` is the matcher `A` at the reference (Go
//! octal) and was the matcher `101` here — a disjoint stream, both sides
//! `200`, no error anywhere. The escapes Go does not define (`\d`, `\q`)
//! were the other half: a `400` there, the silently different pattern
//! `d+` here.
//!
//! Every expected value below is read from `quote.go`'s own `unquoteChar`
//! rather than inferred from a probe, and the three that a plausible-but-
//! wrong implementation would get away with are called out at their
//! tests:
//!
//! * an `Err` for `\d` — rules out the ADDITIVE fix (add the missing
//!   escapes, keep the pass-through fallback), which decodes every row of
//!   [`every_go_defined_escape_decodes_to_the_reference_bytes`] correctly
//!   and leaves `{app=~"\d+"}` silently meaning `d+`;
//! * `"\xc3\xa9"` decoding to the two bytes `C3 A9` — rules out reading
//!   each `\xHH` as `char::from_u32(HH)`, which is right on `\x41` and
//!   wrong on every byte above 0x7F;
//! * `"\ud800"` decoding to U+FFFD — rules out lifting
//!   `pulsus-traceql`'s `unicode_escape_to_char` unchanged, which rejects
//!   surrogates. That is Go's SOURCE literal grammar; the `strutil` copy
//!   Loki actually calls sets `multibyte` and hands the value to
//!   `utf8.EncodeRune`, which writes `RuneError` for an invalid rune.
//!
//! The returned-LINES consequence — which stream `{app=~"\101"}` selects
//! — is pinned by `pulsus-read`'s corpus file
//! `tests/logqltest/corpus/b24_string_escapes.test`, captured from the
//! pinned container. This file pins the decode.

use pulsus_logql::{Expr, LogQlError, Stage, parse};

/// The decoded matcher value of `{app=~"<source>"}`, as BYTES. Bytes and
/// not `&str`, because the composition rule this file exists to pin is a
/// byte rule: `"\xc3\xa9"` is two bytes that happen to form one `char`,
/// and a `&str` comparison would pass just as happily against a
/// four-byte Latin-1 reading of the same literal rendered back out.
fn selector_value(source: &str) -> Result<Vec<u8>, LogQlError> {
    let query = format!(r#"{{app=~"{source}"}}"#);
    match parse(&query) {
        Ok(Expr::Log(log)) => Ok(log.selector.matchers[0].value.clone().into_bytes()),
        Ok(other) => panic!("{query}: expected a log query, got {other:?}"),
        Err(e) => Err(e),
    }
}

/// The same source in a `|~` line filter — the same lexer rule at a
/// different construct, which is the claim the reference's own error
/// makes (only the column moves).
fn line_filter_value(source: &str) -> Result<Vec<u8>, LogQlError> {
    let query = format!(r#"{{app="x"}} |~ "{source}""#);
    match parse(&query) {
        Ok(Expr::Log(log)) => match &log.pipeline[0] {
            Stage::LineFilter(lf) => Ok(lf.value.clone().into_bytes()),
            other => panic!("{query}: expected a line filter, got {other:?}"),
        },
        Ok(other) => panic!("{query}: expected a log query, got {other:?}"),
        Err(e) => Err(e),
    }
}

/// The same source inside `| regexp "(?P<c>…)"`, the third construct the
/// reference's measured column numbers cover.
fn regexp_stage_parses(source: &str) -> Result<(), LogQlError> {
    let query = format!(r#"{{app="x"}} | regexp "(?P<c>{source})""#);
    parse(&query).map(|_| ())
}

/// `(source, the bytes `strutil.Unquote` produces)`, one row per arm of
/// `quote.go`'s `unquoteChar` switch.
const DECODES: &[(&str, &[u8])] = &[
    // The short escapes, `quote.go:150-164`.
    (r"\a", &[0x07]),
    (r"\b", &[0x08]),
    (r"\f", &[0x0C]),
    (r"\n", &[0x0A]),
    (r"\r", &[0x0D]),
    (r"\t", &[0x09]),
    (r"\v", &[0x0B]),
    (r"\\", &[0x5C]),
    (r#"\""#, &[0x22]),
    // `\xHH` — two hex digits, ONE byte (`quote.go:186-190`).
    (r"\x41", &[0x41]),
    // `\NNN` — exactly three octal digits (`quote.go:192-206`). This is
    // the row #400 was filed on: the reference selects the stream `A`
    // and this tree used to select `101`.
    (r"\101", &[0x41]),
    (r"\000", &[0x00]),
    // Two octal escapes compose exactly as two hex ones do: 0303 0277
    // is U+00FF. (A LONE `\377` is 0xFF and is the narrowing this file
    // records at `a_lone_high_byte_escape_is_refused`.)
    (r"\303\277", &[0xC3, 0xBF]),
    // `\uXXXX` / `\UXXXXXXXX` — code points, UTF-8 encoded
    // (`quote.go:166-184` + `Unquote`'s `utf8.EncodeRune` at `:99-104`).
    (r"\u0041", &[0x41]),
    (r"\U00000041", &[0x41]),
    (r"\u00e9", &[0xC3, 0xA9]),
    (r"\U0001F600", &[0xF0, 0x9F, 0x98, 0x80]),
    // Consecutive byte escapes COMPOSE, and a code-point escape does
    // not need to.
    (r"\xc3\xa9", &[0xC3, 0xA9]),
    // Mixed with literal text, so the buffer is exercised as a buffer.
    (r"a\x41b", &[0x61, 0x41, 0x62]),
];

#[test]
fn every_go_defined_escape_decodes_to_the_reference_bytes() {
    for (source, expected) in DECODES {
        let got = selector_value(source)
            .unwrap_or_else(|e| panic!("{{app=~\"{source}\"}} should parse: {e}"));
        assert_eq!(
            got, *expected,
            "`{source}`: the selector matcher decodes to {got:02X?}, not the reference's \
             {expected:02X?} (`strutil.Unquote`, quote.go:66-231 @ v3.7.4)"
        );
        // The same rule at a second construct: this is a lexer rule, so
        // the line filter must agree byte for byte.
        let filter = line_filter_value(source)
            .unwrap_or_else(|e| panic!("{{app=\"x\"}} |~ \"{source}\" should parse: {e}"));
        assert_eq!(
            filter, *expected,
            "`{source}`: the line filter decodes differently from the selector — the escape \
             grammar is a LEXER rule and cannot depend on the construct"
        );
    }
}

/// `\101` is the row this issue was filed on, so it gets its own name:
/// the matcher the planner receives is `A`, which selects a stream
/// DISJOINT from the one `101` selects.
#[test]
fn the_octal_escape_selects_the_streams_the_reference_selects() {
    assert_eq!(selector_value(r"\101").expect("parses"), b"A");
    assert_eq!(selector_value(r"\x41").expect("parses"), b"A");
    assert_eq!(selector_value(r"\u0041").expect("parses"), b"A");
    assert_eq!(selector_value(r"\U00000041").expect("parses"), b"A");
    // And the literal spelling still means itself, so the two selections
    // are genuinely different rather than collapsed together.
    assert_eq!(selector_value("101").expect("parses"), b"101");
}

/// **The discriminator against the additive fix.** Adding `\a \b \f \v
/// \x \u \U` and octal to the match arm while keeping
/// `Some(other) => value.push(other)` passes every other test in this
/// file. It fails here, and it has to: an escape the reference's grammar
/// does not define is a `400` there and was a silently different pattern
/// here.
#[test]
fn an_escape_go_does_not_define_is_refused() {
    // `\d \w \s \q` are regex escapes a user reaches for; `\0`/`\1` are
    // octal STARTS with too few digits (`quote.go:196-199`, `len(s) < 2`);
    // `\8` is not an octal digit at all; `\'` is permitted only when the
    // single quote IS the delimiter (`quote.go:220-224`).
    for source in [r"\d", r"\w", r"\s", r"\q", r"\0", r"\1", r"\8", r"\'"] {
        assert!(
            matches!(
                selector_value(source),
                Err(LogQlError::InvalidCharEscape { .. })
            ),
            "`{source}` in a selector must be an InvalidCharEscape, got {:?}",
            selector_value(source)
        );
        assert!(
            matches!(
                line_filter_value(source),
                Err(LogQlError::InvalidCharEscape { .. })
            ),
            "`{source}` in a line filter must be an InvalidCharEscape",
        );
        assert!(
            matches!(
                regexp_stage_parses(source),
                Err(LogQlError::InvalidCharEscape { .. })
            ),
            "`{source}` in a `| regexp` stage must be an InvalidCharEscape",
        );
    }
}

/// The error names the escape the user wrote, not the whole literal —
/// the message is the only actionable thing about a lexer refusal, and
/// no parity is claimed for its TEXT (owner ruling on #246).
#[test]
fn the_refusal_names_the_offending_escape_and_its_offset() {
    let Err(LogQlError::InvalidCharEscape { escape, span }) = selector_value(r"a\db") else {
        panic!("expected an InvalidCharEscape");
    };
    assert_eq!(escape, r"\d");
    // `{app=~"a\db"}` — the backslash is the ninth byte.
    assert_eq!(span.start, 8, "span must point at the backslash");
    assert!(
        LogQlError::InvalidCharEscape { escape, span }
            .to_string()
            .contains(r"\d"),
        "the message must quote the escape"
    );
}

/// A malformed form of an escape the grammar DOES define is the same
/// refusal: too few hex digits, an octal value above the byte the escape
/// denotes (`quote.go:202-205`), a code point above `utf8.MaxRune`
/// (`quote.go:180-183`), and `\u{…}` — the Rust/PCRE brace form, which
/// Go's grammar has no arm for at all.
#[test]
fn a_malformed_form_of_a_defined_escape_is_refused() {
    for source in [r"\x4", r"\xzz", r"\400", r"\777", r"\U00110000", r"\u{263A}"] {
        assert!(
            matches!(
                selector_value(source),
                Err(LogQlError::InvalidCharEscape { .. })
            ),
            "`{source}` must be an InvalidCharEscape, got {:?}",
            selector_value(source)
        );
    }
}

/// **The discriminator against the Latin-1 reading.** `\xHH` denotes a
/// BYTE, so consecutive escapes compose into one UTF-8 character;
/// decoding each to `char::from_u32(HH)` gives four bytes for this
/// literal and passes every `\x41`-shaped test.
#[test]
fn a_byte_escape_composes_with_its_neighbour() {
    let got = selector_value(r"\xc3\xa9").expect("`\\xc3\\xa9` is `é`");
    assert_eq!(got, vec![0xC3, 0xA9]);
    assert_eq!(String::from_utf8(got).expect("valid UTF-8"), "é");
    // The Latin-1 reading would produce these four bytes.
    assert_ne!(selector_value(r"\xc3\xa9").expect("parses"), b"\xc3\x83\xc2\xa9");
}

/// The deliberate narrowing, ledgered as `logql-string-escape-non-utf8`:
/// a decoded byte string that is not valid UTF-8 is a `400` here, where
/// the reference serves it at its five `NewFastRegexMatcher` positions.
/// Nothing in this store could match such a pattern — every mounted
/// ingest route materialises a body and a label value into a Rust
/// `String` (`pulsus-write/src/protocols/otlp_logs.rs:37-55`) — and
/// `pulsus-traceql` carries the identical ruling.
#[test]
fn a_lone_high_byte_escape_is_refused() {
    for source in [r"\xff", r"\x41\xff", r"\xff\x41", r"\377"] {
        assert!(
            matches!(
                selector_value(source),
                Err(LogQlError::NonUtf8StringLiteral { .. })
            ),
            "`{source}` must be a NonUtf8StringLiteral, got {:?}",
            selector_value(source)
        );
    }
}

/// **The discriminator against lifting `pulsus-traceql`'s scanner.** Its
/// `unicode_escape_to_char` refuses a surrogate "as in Go" — true of
/// Go's SOURCE literal grammar, false of the `strutil.Unquote` copy Loki
/// calls, whose `utf8.EncodeRune` writes `RuneError` for one.
#[test]
fn a_surrogate_escape_decodes_to_the_replacement_character() {
    for source in [r"\ud800", r"\udfff", r"\U0000D800"] {
        assert_eq!(
            selector_value(source).unwrap_or_else(|e| panic!("`{source}` should parse: {e}")),
            "\u{FFFD}".as_bytes(),
            "`{source}` must decode to U+FFFD, not be refused"
        );
    }
    // Above `utf8.MaxRune` IS refused, which is the other half of the
    // same `quote.go` branch and stops this test from passing under a
    // "never reject a \\u escape" reading.
    assert!(matches!(
        selector_value(r"\U00110000"),
        Err(LogQlError::InvalidCharEscape { .. })
    ));
}

/// EOF inside an escape, and a trailing backslash, stay the existing
/// unterminated-string diagnostic: there is no closing quote to be
/// found, so pointing at the opening one is the useful message.
#[test]
fn an_escape_cut_off_by_end_of_input_is_an_unterminated_string() {
    for query in [
        r#"{app=~"\"#,
        r#"{app=~"\x"#,
        r#"{app=~"\x4"#,
        r#"{app=~"\u00"#,
        r#"{app=~"\10"#,
    ] {
        assert!(
            matches!(parse(query), Err(LogQlError::UnterminatedString { .. })),
            "{query:?} must be an UnterminatedString, got {:?}",
            parse(query)
        );
    }
}

/// `quote.go:85-87` refuses a raw newline in an interpreted literal
/// before it decodes anything.
#[test]
fn a_raw_newline_inside_a_double_quoted_literal_is_refused() {
    assert!(matches!(
        parse("{app=~\"a\nb\"}"),
        Err(LogQlError::UnterminatedString { .. })
    ));
}

/// **The backtick branch does no escape processing at all**
/// (`quote.go:76-81`), so it is the portable spelling for a regex and
/// must not have moved. Every row of [`DECODES`] plus every refusal
/// above is literal text here.
#[test]
fn a_backtick_raw_string_takes_no_escapes() {
    for source in [r"\d+", r"\101", r"\xff", r"\u{263A}", r"\a", r"\'"] {
        let query = format!("{{app=~`{source}`}}");
        let Ok(Expr::Log(log)) = parse(&query) else {
            panic!("{query} should parse: {:?}", parse(&query));
        };
        assert_eq!(
            log.selector.matchers[0].value.as_bytes(),
            source.as_bytes(),
            "a backtick string must hand the regex its own bytes"
        );
    }
    // A raw newline IS allowed in a backtick string, as in Go.
    assert!(parse("{app=~`a\nb`}").is_ok());
}

/// The escape grammar reaches constructs that are not regexes at all —
/// it is the string LEXER. A `line_format` template and a `label_format`
/// value take the same decode.
#[test]
fn the_grammar_applies_to_every_construct_that_carries_a_string() {
    let ok = r#"{app="x"} | line_format "{{.a}}\x41""#;
    parse(ok).unwrap_or_else(|e| panic!("{ok} should parse: {e}"));
    for query in [
        r#"{app="x"} | line_format "{{.a}}\d""#,
        r#"{app="x"} | label_format a="\d""#,
        r#"{app="x"} | json a="\d""#,
        r#"{app="x"} | a=~"\d""#,
        r#"{app="x"} |= "\d""#,
        r#"count_over_time({app="x"} |~ "\d" [5m])"#,
    ] {
        assert!(
            matches!(parse(query), Err(LogQlError::InvalidCharEscape { .. })),
            "{query:?} must be an InvalidCharEscape, got {:?}",
            parse(query)
        );
    }
}
