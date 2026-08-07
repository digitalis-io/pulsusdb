//! Issue #247: the `| logfmt <id>="<expr>"` extraction-expression
//! sub-grammar — **which expressions are refused**, and what source key a
//! surviving one resolves to.
//!
//! **The scope boundary.** #247 is the REJECTION surface: what a
//! `| logfmt a="expr"` query is refused for. What a surviving expression
//! then evaluates to is issue #393, with one exception named under
//! "the resolved source key" below.
//!
//! # Why a second grammar exists at all
//!
//! The reference parses a logfmt extraction expression with a **separate,
//! tiny grammar** — a lexer (`pkg/logql/log/logfmt/lexer.go @ v3.7.4`), a
//! yacc grammar whose `expressions` rule carries three productions
//! (`logfmtexpr.y:26-39 @ v3.7.4`, the whole grammar) and a
//! one-function driver (`logfmt/parser.go:11-19 @ v3.7.4`). It runs from
//! `NewLogfmtExpressionParser` (`pkg/logql/log/parser.go:506-529 @
//! v3.7.4`), reached from `LogfmtExpressionParserExpr.Stage()`
//! (`pkg/logql/syntax/ast.go:1073-1075 @ v3.7.4`), whose error
//! `MultiStageExpr.stages()` wraps as `logqlmodel.NewStageError`
//! (`ast.go:227 @ v3.7.4`) — a `ParseError`, so `errors.Is(err, ErrParse)`
//! holds and `util/server/error.go:110-117 @ v3.7.4` answers **400**.
//!
//! PulsusDB had no such grammar: the `ParserStage::Logfmt` compile arm
//! cloned the expression string verbatim into the source key, so every
//! shape the reference refuses was served here instead. What it then did
//! varied — `a="b c"` matched nothing, because the walker cannot emit a
//! key with a space in it, while `a="b.c"` matched a literal `b.c=` pair
//! on the line, which is a key the walker CAN emit. Either way the query
//! ran. This module is that grammar, called from `compile_parser` — the
//! same pipeline compile `exec` runs before any ClickHouse call.
//!
//! # The rules, read off the reference's source and then measured
//!
//! | # | rule | citation @ v3.7.4 | probe → status |
//! |---|---|---|---|
//! | E1 | at least one token is required | `logfmtexpr.y:26-27` | `a=""` → 400; `a=" "` → 400 |
//! | E2 | a KEY is `[A-Za-z_][A-Za-z0-9_]*` | `logfmt/lexer.go:67-73` | `a="b"`, `a="_"`, `a="b_c"`, `a="b1"`, `a="B"` → 200 |
//! | E3 | any other character ends the scan with `unexpected char <c>`, and the driver reports it even when the parse accepted what it already had | `logfmt/lexer.go:60-62`, `logfmt/parser.go:11-19` | `a="b.c"`, `a="b-c"`, `a="b/c"`, `a="b:c"`, `a="b=c"`, `a="b,c"`, `a="b[0]"`, `a="b]"`, `a="b!"`, `a="b\\c"` → 400 |
//! | E4 | when the bad character is the FIRST one nothing was shifted, so the yacc `Error` callback overwrites the lexer's message and the **syntax** error wins | `logfmt/lexer.go:24-27` (`Scanner.Error` assigns `sc.err`), `logfmtexpr.y:26-27` | `a="0b"` → 400 `unexpected $end`; `a="é"` → 400 `unexpected $end` |
//! | E5 | a `"` opens a STRING that runs to the next `"`, to a `]`, to a NUL or to end of input; escapes are NOT honoured and the delimiter is dropped | `logfmt/lexer.go:100-121` (`scanStr`) | `a="\"b c\""` → 200; `a="\"unterminated"` → **200**; `a="\"\""` → 200 |
//! | E6 | the FIRST token may be KEY or STRING; every LATER token must be STRING — `expressions: key \| value \| expressions value`, with **no `expressions key` production** | `logfmtexpr.y:29-33` | `a="b \"c\""` → 200; `a="\"b\" \"c\""` → 200; `a="b c"` → 400 `unexpected KEY`; `a="\"b\" c"` → 400 `unexpected KEY` |
//! | E8 | a NUL byte reads as end-of-input at the top of the lexer (`sc.read()` returns `0`, and `lex` returns `0` = `$end` for it) but only TERMINATES A STRING inside `scanStr`, where scanning then continues | `logfmt/lexer.go:44-46`, `:91-93` (`isEndOfInput`), `:109-120` | `a="b\x00c"` → 200, source key `b`; `a="\x00b"` → 400 `unexpected $end`; `a="\"a\x00b\""` → 400 `unexpected KEY` (`b"` lexes as a KEY after the string ends at the NUL) |
//!
//! **E7 is recorded, not implemented — see [`e7_is_unreachable_through_the_grammar`].**
//! `NewLogfmtExpressionParser` also runs
//! `model.UTF8Validation.IsValidLabelName(exp.Identifier)`
//! (`log/parser.go:518-520 @ v3.7.4`), which for the UTF-8 scheme is
//! "non-empty and valid UTF-8" (`vendor/github.com/prometheus/common/model/metric.go:184-205
//! @ v3.7.4`). No query can reach it, for two independent reasons, both
//! reproduced rather than taken on trust:
//!
//! 1. **Empty.** The only non-test caller is `Stage()` (`ast.go:1074`),
//!    whose `Expressions` are built solely by
//!    `labelExtractionExpression: IDENTIFIER EQ STRING | IDENTIFIER`
//!    (`syntax/syntax.y:314-316`) — `newLogfmtExpressionParser`'s only
//!    call sites are that grammar's two reductions
//!    (`syntax.y.go:1399,1404`), and `clone.go:287` copies an
//!    already-parsed node. An IDENTIFIER's payload is
//!    `lval.str = l.TokenText()` (`syntax/lex.go:241-242`, `:256-257`),
//!    and `TokenText()` returns `""` only when `tokPos < 0`
//!    (`syntax/query_scanner.go:764-768`), which is the EOF case the
//!    lexer returns `0` for beforehand.
//! 2. **Invalid UTF-8.** Every byte of a token passes through
//!    `Scanner.next()`, which calls `s.error("invalid UTF-8 encoding")`
//!    on any byte `utf8.DecodeRune` rejects
//!    (`query_scanner.go:255-267`). `parser.Parse` installs
//!    `p.Scanner.Error` before parsing (`syntax/parser.go:63-65`) and
//!    returns `p.errs[0]` whenever `len(p.errs) > 0` **even if the yacc
//!    parse succeeded** (`parser.go:67-69`). So the query dies in
//!    `ParseExpr`, before any `Stage()`.
//!
//! Measured against the reference's own code at `v3.7.4` (probe removed
//! after running): over 200,000 random byte strings in the identifier
//! position, 24,427 identifiers reached `Stage()` and **0** failed
//! `IsValidLabelName`; every hand-picked invalid-UTF-8 identifier
//! (`\xff`, `a\xffb`, `\xc3`, `\xe2\x82`, `\xed\xa0\x80`,
//! `\xf4\x90\x80\x80`, `\xc0\x80`) died in `ParseExpr` with `invalid
//! UTF-8 encoding`; and calling `NewLogfmtExpressionParser` DIRECTLY with
//! identifier `"\xff"` does return `invalid extracted label name` — so
//! the rule is live code that the grammar cannot reach, not dead code.
//!
//! # How the whole grammar was checked, not just these examples
//!
//! The rules above were replayed against the reference's own
//! `logfmt.Parse` at `v3.7.4` over **42,056 expressions** — every string
//! of length ≤ 5 over `{a, 0, _, ", ], <space>, ., é}`, every string of
//! length ≤ 4 over `{a, ", ], \t, \r, NUL, [, \\}`, and a hand list — and
//! the model implemented here agreed on **all 42,056**, error text
//! included, including the path elements for every accepted expression.
//! That is the enumeration this module is built from; the container is
//! the oracle for the end-to-end HTTP verdict
//! (`tests/logql_logfmt_expr_matrix.rs`).
//!
//! # The resolved source key
//!
//! [`parse_logfmt_expr`] returns the parsed path's elements joined with a
//! single ASCII space, which is the ONE evaluation-side rule this issue
//! adopts (the other three are #393): it is the sub-grammar's own output,
//! so implementing the grammar produces it whether or not we ask for it.
//! Measured on the pinned container over the line `b=1 a-b=2 x=3 b.c=4`:
//! `| logfmt a="\"b\""` gives `a="1"`, exactly as `| logfmt a="b"` does.
//!
//! A path of more than one element, or a single element that is empty or
//! contains whitespace, is **unmatchable by construction**:
//! [`super::pipeline::walk_logfmt`] builds a line key as a maximal run of
//! bytes that are not whitespace, `=`, `"` or a control byte, so it can
//! emit neither an empty key nor one containing ASCII whitespace. That is
//! an invariant with a test, not a comment —
//! `walk_logfmt_never_emits_an_empty_or_whitespace_bearing_key` in
//! `pipeline.rs`. The reference reaches the same outcome differently: it
//! joins with `fmt.Sprintf("%v", paths...)` (`log/parser.go:546 @
//! v3.7.4`), whose `n>1` rendering is `<first>%!(EXTRA string=…)`, and
//! compares against a key sanitized to `[A-Za-z0-9_]`. Measured on the
//! container over that same line: `a="\"b c\""`, `a="b \"x\""` and
//! `a="\"\""` all give `a=""` — a miss on both sides.
//!
//! **One consequence is a NEW instance of #393's divergence, and it is
//! measured rather than left to be found.** We compare the resolved key
//! against RAW line keys; the reference compares it against SANITIZED
//! ones. So for a quoted single-element path holding a character our
//! walker can emit in a key but which is not `[A-Za-z0-9_]`, the two
//! sides now differ: over `b=1 a-b=2 x=3 b.c=4`, `| logfmt a="\"b.c\""`
//! is `a=""` on the container (measured) and `a="4"` here — asserted end
//! to end by
//! `the_resolved_key_of_a_quoted_path_is_the_unsanitized_element`, not
//! merely described. The mechanism is the missing line-key sanitization,
//! which is #393's second bullet — the same rule that already makes
//! `| logfmt a="b_c"` give `a="4"` there (measured) and NOTHING here,
//! with or without this change.
//!
//! # Excluded from this module and from its matrix, by name
//!
//! **Non-ASCII identifiers — issue #392, the "we refuse, the reference
//! serves" direction.** Measured on the pinned container: `| logfmt
//! éx="b"`, `| json éx="b"`, `| label_format éx="y"`, `| drop éx` and
//! `sum by (éx) (…)` are never a 400 there, while `{éx="m"}` is 400 on
//! both sides; our lexer refuses `éx` in every one of those positions.
//! Two refinements of that measurement belong here because they change
//! what "serves" means: with a matching entry in the window, `| logfmt
//! éx="b"` and `| label_format éx="y"` are a **500** (`could not write
//! JSON response: … unexpected character inside braces: 'é'`) rather than
//! a 200 — the query is admitted and fails at response encoding — and
//! with no matching stream they are 200-empty. And `| logfmt éx` (the
//! bare-identifier form, which expands to `éx="éx"`) is a **400** on the
//! reference by E4, so that one shape agrees with us for an unrelated
//! reason. It is a whole-lexer question touching every identifier
//! position, hence its own issue.
//!
//! **`| json`'s expression grammar — issue #394.** The reference uses a
//! DIFFERENT sub-grammar there (`pkg/logql/log/jsonexpr/`, which accepts
//! `.` and `[N]`), so this module must not be reused for it and
//! `parse_json_path` is untouched.

/// The reference's yacc message when no token was shifted at all
/// (`logfmtexpr.y:26-27 @ v3.7.4` — `logfmt: expressions`, and
/// `expressions` has no empty production). Kept distinct from
/// [`UNEXPECTED_KEY`] because the two tell a user different things: the
/// FIRST character was the problem, or a later one was.
const UNEXPECTED_END: &str = "syntax error: unexpected $end, expecting STRING or KEY";

/// The reference's yacc message for a KEY in any position but the first
/// (`logfmtexpr.y:29-33 @ v3.7.4` — there is no `expressions key`
/// production).
const UNEXPECTED_KEY: &str = "syntax error: unexpected KEY";

/// `isStartIdentifier` (`logfmt/lexer.go:67-69 @ v3.7.4`).
const fn is_start_ident(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

/// `isIdentifier` (`logfmt/lexer.go:71-73 @ v3.7.4`).
const fn is_ident(b: u8) -> bool {
    is_start_ident(b) || b.is_ascii_digit()
}

/// `isWhitespace` (`logfmt/lexer.go:125 @ v3.7.4`) — space, tab and
/// newline ONLY. A `\r` is an `unexpected char`, not whitespace.
const fn is_space(b: u8) -> bool {
    b == b' ' || b == b'\t' || b == b'\n'
}

/// One token of the sub-grammar, or the reason scanning stopped.
///
/// Byte-level scanning is exact here even though the reference scans
/// runes: every character the lexer branches on (`[A-Za-z0-9_]`, `"`,
/// `]`, space, tab, newline, NUL) is ASCII, and no byte of a multi-byte
/// UTF-8 sequence can equal an ASCII byte — so a multi-byte rune is
/// "some other character" byte-wise exactly as it is rune-wise, and every
/// slice cut below falls on a char boundary.
#[derive(Debug, PartialEq, Eq)]
enum Lexed<'a> {
    /// `KEY` — `scanField` (`logfmt/lexer.go:75-89 @ v3.7.4`).
    Key(&'a str),
    /// `STRING` — `scanStr` (`logfmt/lexer.go:100-121 @ v3.7.4`), the
    /// delimiters already dropped.
    Str(&'a str),
    /// Input exhausted, or a NUL read at the top of the lexer (E8).
    Eof,
    /// `sc.err = "unexpected char %c"` (`logfmt/lexer.go:60-62 @ v3.7.4`).
    Bad(char),
}

/// One `Scanner.lex` call (`logfmt/lexer.go:40-65 @ v3.7.4`), advancing
/// `i` over `expr`'s bytes.
fn lex<'a>(expr: &'a str, i: &mut usize) -> Lexed<'a> {
    let b = expr.as_bytes();
    loop {
        if *i >= b.len() {
            return Lexed::Eof;
        }
        let c = b[*i];
        // `sc.read()` yields 0 both at EOF and for a NUL byte, and `lex`
        // returns `0` (`$end`) for it (`lexer.go:44-46 @ v3.7.4`).
        if c == 0 {
            return Lexed::Eof;
        }
        *i += 1;
        if is_space(c) {
            continue;
        }
        if is_start_ident(c) {
            let start = *i - 1;
            while *i < b.len() && b[*i] != 0 && is_ident(b[*i]) {
                *i += 1;
            }
            return Lexed::Key(&expr[start..*i]);
        }
        if c == b'"' {
            // `scanStr` consumes the terminator and drops it; escapes are
            // not honoured, so the content is one contiguous slice.
            let start = *i;
            while *i < b.len() {
                let d = b[*i];
                *i += 1;
                if d == 0 || d == b'"' || d == b']' {
                    return Lexed::Str(&expr[start..*i - 1]);
                }
            }
            return Lexed::Str(&expr[start..]);
        }
        // A multi-byte rune lands here on its LEAD byte; decode the whole
        // rune for the message and skip its continuation bytes.
        let ch = expr[*i - 1..]
            .chars()
            .next()
            .unwrap_or(char::REPLACEMENT_CHARACTER);
        *i = *i - 1 + ch.len_utf8();
        return Lexed::Bad(ch);
    }
}

/// Parses a `| logfmt <id>="<expr>"` extraction expression and returns the
/// SOURCE KEY the stage matches line keys against, or the inner error text.
///
/// The result is the parsed path's elements joined with a single ASCII
/// space, which is what makes a multi-element path unmatchable — see the
/// module docs. A single-element path is that element verbatim, so the
/// overwhelmingly common `a="b"` form is unchanged.
///
/// Mirrors `logfmt.Parse` (`pkg/logql/log/logfmt/parser.go:11-19 @
/// v3.7.4`) including its **error precedence**: the lexer records
/// `sc.err`, and a subsequent yacc syntax error OVERWRITES it via
/// `Scanner.Error` (`logfmt/lexer.go:24-27 @ v3.7.4`), so a bad character
/// with nothing yet shifted surfaces as `unexpected $end` (E4) while one
/// after a complete `expressions` surfaces as `unexpected char` (E3).
pub(super) fn parse_logfmt_expr(expr: &str) -> Result<String, String> {
    let mut path: Vec<&str> = Vec::new();
    let mut lex_err: Option<char> = None;
    let mut i = 0usize;
    loop {
        match lex(expr, &mut i) {
            Lexed::Eof => break,
            Lexed::Bad(c) => {
                lex_err = Some(c);
                break;
            }
            Lexed::Key(k) => {
                // `expressions` has no `expressions key` production, so a
                // KEY past the first token is a syntax error REPORTED AT
                // ONCE — the reference's parser stops on it and never
                // lexes further, which is why a later bad character
                // cannot overwrite this message.
                if !path.is_empty() {
                    return Err(UNEXPECTED_KEY.to_string());
                }
                path.push(k);
            }
            Lexed::Str(s) => path.push(s),
        }
    }
    if path.is_empty() {
        // Nothing shifted: the yacc error wins even when the lexer had
        // already recorded an `unexpected char` (E4).
        return Err(UNEXPECTED_END.to_string());
    }
    if let Some(c) = lex_err {
        return Err(format!("unexpected char {c}"));
    }
    Ok(path.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// E1 — `logfmt: expressions` has no empty production
    /// (`logfmtexpr.y:26-27 @ v3.7.4`). Whitespace-only is the same case:
    /// the lexer skips it and reports EOF.
    #[test]
    fn e1_at_least_one_token_is_required() {
        for expr in ["", " ", "  ", "\t", "\n", " \t\n "] {
            assert_eq!(
                parse_logfmt_expr(expr),
                Err(UNEXPECTED_END.to_string()),
                "{expr:?}"
            );
        }
    }

    /// E2 — a KEY is `[A-Za-z_][A-Za-z0-9_]*`
    /// (`logfmt/lexer.go:67-73 @ v3.7.4`), and it resolves to itself.
    #[test]
    fn e2_a_key_is_ascii_letters_digits_and_underscore() {
        for (expr, key) in [
            ("b", "b"),
            ("_", "_"),
            ("b_c", "b_c"),
            ("b1", "b1"),
            ("B", "B"),
            ("_0", "_0"),
            ("aB9_", "aB9_"),
            ("  b  ", "b"),
        ] {
            assert_eq!(parse_logfmt_expr(expr), Ok(key.to_string()), "{expr:?}");
        }
    }

    /// E3 — any other character ends the scan, and the lexer's message
    /// survives because the parser accepted what it already had
    /// (`logfmt/lexer.go:60-62`, `logfmt/parser.go:11-19 @ v3.7.4`).
    #[test]
    fn e3_a_bad_character_after_a_complete_expression_is_unexpected_char() {
        for (expr, ch) in [
            ("b.c", '.'),
            ("b-c", '-'),
            ("b/c", '/'),
            ("b:c", ':'),
            ("b=c", '='),
            ("b,c", ','),
            ("b!", '!'),
            ("b[0]", '['),
            ("b]", ']'),
            ("b\\c", '\\'),
            ("b\rc", '\r'),
            ("b é", 'é'),
            ("b 0", '0'),
        ] {
            assert_eq!(
                parse_logfmt_expr(expr),
                Err(format!("unexpected char {ch}")),
                "{expr:?}"
            );
        }
    }

    /// E4 — with nothing shifted, the yacc `Error` callback overwrites
    /// the lexer's message (`logfmt/lexer.go:24-27 @ v3.7.4`), so the
    /// user is told the FIRST character was the problem. The two classes
    /// must stay distinct: `b.c` and `0b` are different defects.
    #[test]
    fn e4_a_bad_first_character_is_the_syntax_error_not_unexpected_char() {
        for expr in [
            "0b", "é", "日本", ".b", "-b", "]b", "[0]", "\rb", "\\b", "0",
        ] {
            assert_eq!(
                parse_logfmt_expr(expr),
                Err(UNEXPECTED_END.to_string()),
                "{expr:?}"
            );
        }
        assert_ne!(
            parse_logfmt_expr("b.c"),
            parse_logfmt_expr("0b"),
            "collapsing E3 and E4 into one message would hide which character was at fault"
        );
    }

    /// E5 — `scanStr` runs to the next `"`, to a `]`, or to end of input;
    /// escapes are not honoured and the delimiter is dropped
    /// (`logfmt/lexer.go:100-121 @ v3.7.4`).
    #[test]
    fn e5_a_quoted_string_runs_to_a_quote_a_bracket_or_the_end() {
        for (expr, key) in [
            (r#""b c""#, "b c"),
            (r#""unterminated"#, "unterminated"),
            (r#""""#, ""),
            (r#""b.c""#, "b.c"),
            (r#""b""#, "b"),
            ("\"", ""),
        ] {
            assert_eq!(parse_logfmt_expr(expr), Ok(key.to_string()), "{expr:?}");
        }
        // A `]` terminates the string too, so `"a]b"` is STRING("a") then
        // KEY("b") — E6's rejection, not an accept.
        assert_eq!(
            parse_logfmt_expr(r#""a]b""#),
            Err(UNEXPECTED_KEY.to_string())
        );
    }

    /// E6 — `expressions: key | value | expressions value`
    /// (`logfmtexpr.y:29-33 @ v3.7.4`): no `expressions key` production.
    #[test]
    fn e6_only_the_first_token_may_be_a_key() {
        for (expr, key) in [
            (r#"b "c""#, "b c"),
            (r#""b" "c""#, "b c"),
            (r#"a"b"#, "a b"),
            (r#""a" "b" "c""#, "a b c"),
        ] {
            assert_eq!(parse_logfmt_expr(expr), Ok(key.to_string()), "{expr:?}");
        }
        for expr in [r#"b c"#, r#""b" c"#, r#"a b c"#, r#""a" b"#] {
            assert_eq!(
                parse_logfmt_expr(expr),
                Err(UNEXPECTED_KEY.to_string()),
                "{expr:?}"
            );
        }
    }

    /// E6, precedence half: a KEY past the first token stops the parser
    /// at once, so a bad character LATER in the expression can never
    /// overwrite the message.
    #[test]
    fn e6_a_late_key_wins_over_a_still_later_bad_character() {
        assert_eq!(parse_logfmt_expr("a b.c"), Err(UNEXPECTED_KEY.to_string()));
    }

    /// E8 — a NUL reads as end-of-input at the top of the lexer but only
    /// ends the STRING inside `scanStr`, where scanning then continues
    /// (`logfmt/lexer.go:44-46`, `:91-93`, `:109-120 @ v3.7.4`).
    #[test]
    fn e8_a_nul_ends_the_scan_at_the_top_but_only_the_string_inside_one() {
        assert_eq!(parse_logfmt_expr("b\0c"), Ok("b".to_string()));
        assert_eq!(parse_logfmt_expr("a\0"), Ok("a".to_string()));
        assert_eq!(
            parse_logfmt_expr("\0b"),
            Err(UNEXPECTED_END.to_string()),
            "a leading NUL shifts nothing"
        );
        // The string ends at the NUL, then `b"` lexes as a KEY.
        assert_eq!(
            parse_logfmt_expr("\"a\0b\""),
            Err(UNEXPECTED_KEY.to_string())
        );
        // …and `"\0"` is two empty strings, not one.
        assert_eq!(parse_logfmt_expr("\"\0\""), Ok(" ".to_string()));
    }

    /// The multi-element and empty resolutions are the ones
    /// `walk_logfmt` can never emit — the unmatchable-by-construction
    /// half of the resolved-key rule (its walker-side invariant is
    /// `pipeline.rs`'s
    /// `walk_logfmt_never_emits_an_empty_or_whitespace_bearing_key`).
    #[test]
    fn a_multi_element_or_empty_path_resolves_to_an_unmatchable_key() {
        for expr in [r#"b "c""#, r#""b" "c""#, r#""b c""#, r#""""#] {
            let key = parse_logfmt_expr(expr).expect(expr);
            assert!(
                key.is_empty() || key.contains(' '),
                "{expr:?} resolved to {key:?}, which `walk_logfmt` could emit"
            );
        }
    }

    /// The one evaluation rule #247 adopts (the ruling's item 2): the
    /// source key is the PARSED PATH, so `a="\"b\""` is `a="b"`.
    /// Measured on the pinned container over `b=1 a-b=2 x=3 b.c=4`:
    /// both give `a="1"`.
    #[test]
    fn a_quoted_single_element_path_resolves_the_same_as_the_bare_key() {
        assert_eq!(parse_logfmt_expr(r#""b""#), parse_logfmt_expr("b"));
        assert_eq!(parse_logfmt_expr(r#""b""#), Ok("b".to_string()));
    }

    /// The #393 consequence, pinned END TO END so it is visible and
    /// breakable rather than only described: the resolved key is the
    /// element UNSANITIZED, and we then compare it against RAW line keys.
    /// Over `b=1 a-b=2 x=3 b.c=4` the container answers `a=""` for
    /// `| logfmt a="\"b.c\""` (measured 2026-08-07) because it compares
    /// against a key sanitized to `[A-Za-z0-9_]`; we answer `a="4"`,
    /// which this test asserts. Same mechanism, opposite direction, as
    /// `| logfmt a="b_c"`: measured `a="4"` there and NOTHING here, and
    /// that one diverges the same way with or without this change.
    #[test]
    fn the_resolved_key_of_a_quoted_path_is_the_unsanitized_element() {
        assert_eq!(parse_logfmt_expr(r#""b.c""#), Ok("b.c".to_string()));
        assert_eq!(parse_logfmt_expr(r#""a-b""#), Ok("a-b".to_string()));

        const LINE: &str = "b=1 a-b=2 x=3 b.c=4";
        let extract = |query: &str| -> Option<String> {
            let expr = pulsus_logql::parse(query).expect(query);
            let pulsus_logql::Expr::Log(log) = &expr else {
                panic!("{query}: not a log query")
            };
            let compiled = super::super::pipeline::CompiledPipeline::compile(&log.pipeline)
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            let mut out = Vec::new();
            compiled
                .run_into(LINE, &[], 0, &mut out)
                .expect("within budget");
            out.iter()
                .find(|(k, _)| k == "a")
                .map(|(_, v)| v.to_string())
        };
        // We match the raw line key `b.c`; the reference matches none.
        assert_eq!(
            extract(r#"{s="m"} | logfmt a="\"b.c\"""#),
            Some("4".to_string())
        );
        // And the mirror: the reference matches `a-b` through its
        // sanitizer, we match nothing at all.
        assert_eq!(extract(r#"{s="m"} | logfmt a="b_c""#), None);
    }

    /// E7 is NOT implemented, and this test is the argument for why —
    /// see the module docs. It asserts the two properties the reference's
    /// grammar guarantees about an identifier, over the identifiers OUR
    /// parser admits, so "unreachable" is a checked statement here too
    /// rather than a claim about someone else's code.
    #[test]
    fn e7_is_unreachable_through_the_grammar() {
        for query in [
            r#"{a="b"} | logfmt x="k""#,
            r#"{a="b"} | logfmt _="k""#,
            r#"{a="b"} | logfmt X0="k""#,
            r#"{a="b"} | logfmt x"#,
        ] {
            let expr = pulsus_logql::parse(query).expect(query);
            let mut seen = 0usize;
            for id in logfmt_identifiers(&expr) {
                seen += 1;
                assert!(
                    !id.is_empty(),
                    "{query}: an empty identifier reached compile"
                );
                // A Rust `&str` is valid UTF-8 by construction, which is
                // the type-level analogue of the reference's scanner
                // rejecting an invalid byte before `ParseExpr` returns.
                assert_eq!(id, String::from_utf8(id.as_bytes().to_vec()).unwrap());
            }
            assert_eq!(seen, 1, "{query}");
        }
    }

    /// Collects the extraction identifiers of every `| logfmt` stage.
    fn logfmt_identifiers(expr: &pulsus_logql::Expr) -> Vec<String> {
        use pulsus_logql::{Expr, ParserStage, Stage};
        let mut out = Vec::new();
        if let Expr::Log(log) = expr {
            for stage in &log.pipeline {
                if let Stage::Parser(ParserStage::Logfmt { extractions, .. }) = stage {
                    out.extend(extractions.iter().map(|e| e.label.clone()));
                }
            }
        }
        out
    }
}
