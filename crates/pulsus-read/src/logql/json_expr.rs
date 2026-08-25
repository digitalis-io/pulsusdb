//! Issue #394, folded into #388: the `| json <id>="<expr>"` extraction
//! -expression sub-grammar — which expressions are refused, and what path
//! a surviving one denotes.
//!
//! # The issue's title points one way and this half points the other
//!
//! #388 is filed as "the reference rejects and we accept". For the json
//! sub-grammar **the harmful direction is the opposite**: at `5d91ef1`
//! PulsusDB refused four expressions the reference serves — `arr[ 0 ]`,
//! `arr[0 ]`, `b[ "c" ]` and `[ "b-c" ]`, all 200 there and extracting
//! the right value — and mis-resolved two more (`b . c` and `b.c ` are
//! 200 on both sides, reading `["b","c"]` there and a literal `"b "`/
//! `" c"` pair here, so they answered `""`). A user moving a working
//! query to PulsusDB lost it with no warning, which is the direction
//! #226 calls the real bug. Both directions close together because both
//! come from one cause: a hand-rolled approximation where the reference
//! has a grammar.
//!
//! # Why a second grammar exists at all
//!
//! The reference parses an extraction expression with a **separate,
//! tiny grammar**: a hand-written lexer
//! (`pkg/logql/log/jsonexpr/lexer.go @ v3.7.4`) and a yacc grammar of
//! six productions (`jsonexpr/jsonexpr.y:34-41`, the whole of it):
//!
//! ```text
//! values := FIELD | key_access | index_access
//!         | values key_access | values index_access | values DOT FIELD
//! key_access   := '[' STRING ']'
//! index_access := '[' INDEX  ']'
//! ```
//!
//! Whitespace (` `, `\t`, `\n`) is skipped between every token
//! (`lexer.go:47-49`) — that is the half `parse_json_path` refused. It
//! runs from `NewJSONExpressionParser` (`log/parser.go:634-651 @
//! v3.7.4`), reached from `JSONExpressionParser.Stage()`, so its error is
//! a `Stage()` error: **window-dependent**, 200 over a window whose end
//! is older than `query_ingesters_within`. A `| pattern` rejection is
//! raised in `ParseExpr` instead and is window-INDEPENDENT. That
//! measured difference is why this module and [`super::pattern_expr`]
//! share no code, no error type and no test harness.
//!
//! **A second consumer, so nobody reads `NewJSONExpressionParser` as the
//! only one.** `jsonexpr.Parse` has exactly two non-test callers at
//! `v3.7.4`: `log/parser.go:638` (the query path, above) and
//! `pkg/distributor/field_detection.go:275`, an INGEST-side call on
//! `allowedLabel`. The second adds no rule to the query path — that is a
//! reading (R14 in [`super::pattern_expr`]'s table), not a measurement.
//!
//! # The rules, read off the reference's source and then measured
//!
//! One row per line of the committed
//! `tests/logql_json_expr_reference_error_sites.txt`, **plus one row that
//! file structurally cannot contain**. `err_class` and `probeable` are
//! readings (R1, R2); the probe column is a capture from the pinned
//! digest.
//!
//! | site @ v3.7.4 | err_class | probeable | rule | probe → status |
//! |---|---|---|---|---|
//! | `lexer.go:26` | Producer | yes | the yacc `Error` callback. **LAST WRITER WINS**: a syntax error OVERWRITES a lexer error raised in the same parse | `b c` → 400 `syntax error: unexpected FIELD`; `b[0` → the syntax error, NOT `non-integer value` |
//! | `lexer.go:56` | Relay | — | relays `scanInt`'s error into `sc.err` — `:138`, `:147` **and** `strconv.Atoi`'s | — (its probes belong to the producers) |
//! | `lexer.go:80` | Producer | yes | a byte no token can start with | `b-c` → 400 `unexpected char -` |
//! | `lexer.go:114` | Producer | **no** — `UnreachableByAnyInput` | `scanStr`'s defensive `r != '"'` branch; `scanStr` is entered only after `unread()` of a `"` (`lexer.go:75-76`) | **none — the one row in either of #388's tables with no empirical support of any kind.** It rests on a reading and is listed as such |
//! | `lexer.go:138` | Producer | yes | `cannot use float as array index` | `b 1.5`, `b[0] 1.5`, `b.c 1.5` → 400 with that text |
//! | `lexer.go:147` | Producer | yes | `non-integer value: <c>` | `b 1x` → 400 `non-integer value: x` |
//! | `strconv/atoi.go` `ErrRange` | Producer | yes | **NOT IN THE ENUMERATION AND NO COMMAND HERE COULD PUT IT THERE** — see below | `b 9223372036854775808]` → 400 `strconv.Atoi: parsing "…": value out of range` |
//!
//! **`:138` and `:147` are text-observable, which reasoning got wrong.**
//! It is natural to conclude that `scanInt`'s errors are always
//! overwritten by the yacc callback, because `scanInt` runs inside `[`
//! where `$end` is never acceptable — and six probes (`b[1.5]`, `b[-1]`,
//! `0b`, `b.0`, …) agree. The conclusion is false: a digit can start a
//! token OUTSIDE a bracket, and there the parser accepts the prefix it
//! has already shifted, so the lexer's message survives. `b 1.5` and
//! `b 1x` are the witnesses. `b 0` reaches `:147` too — `read()` returns
//! NUL at end of input and `%c` formats it — and the response body
//! genuinely carries that unprintable byte, so the committed witness is
//! the printable one.
//!
//! **The rule found outside the enumeration, and the divergence it
//! prevented.** `scanInt` ends `return strconv.Atoi(string(number))`
//! (`lexer.go:153`). On an out-of-range integer that returns `strconv`'s
//! `ErrRange`, relayed by `:56`, and it survives whenever the parser can
//! accept the prefix. Measured: `b 9223372036854775808]` is 400 with the
//! `strconv` text, `b[9223372036854775808]` is 400 with the syntax error
//! that overwrites it, and `b[9223372036854775807]` is **200**. So the
//! index bound is Go's `int` — i64 — and a `usize` parse here would have
//! ACCEPTED 2^63 and introduced a divergence this change exists to
//! close. The enumeration's grep could not see it: its scope is these two
//! packages and the error is created in `strconv`. It is recorded as a
//! rule row with `source=FoundByProbe` rather than folded into the count.
//!
//! # The error TEXT: what is matched and what is ours
//!
//! The four lexer messages are matched byte for byte, because each is a
//! fixed string or one `%c` and a user reads the same sentence on both
//! systems. Two are deliberately ours:
//!
//! * **The yacc syntax error.** Reproducing goyacc's verbose message in
//!   general needs its parse tables. What is reproduced is the message
//!   for each of this parser's five error POSITIONS, and every one was
//!   measured — see [`Position`]. That is a per-position claim, not a
//!   claim about goyacc.
//! * **The out-of-range index.** The reference's text names a Go
//!   standard-library function (`strconv.Atoi: parsing "…": value out of
//!   range`), which has no business in a PulsusDB error. The VERDICT
//!   matches; the wording is ours, and the matrix's `reference_text`
//!   column keeps the capture so the divergence is visible.
//!
//! # What a surviving expression denotes
//!
//! [`parse_json_expr`] returns the same `Vec<JsonPathSeg>` the extractor
//! already walks, and its `Index`/`Field` split already matches the
//! reference's `int`/`string` path elements (`JSONPathToStrings`,
//! `log/parser.go:658-668 @ v3.7.4`) — measured identical on both sides
//! for `b[0]`, `b["0"]`, `arr[0]` and `arr["0"]`. The grammar change must
//! not touch it, and does not.
//!
//! The one evaluation change adopted is the one the grammar produces for
//! free: a whitespace-bearing expression denotes the reference's path, so
//! `b . c` reads `["b","c"]` (measured `nested` on both sides after this
//! change, `""` here before it). What a surviving expression then
//! RESOLVES to is otherwise #389's subject, not this one's.
//!
//! # The capability this deliberately withdraws
//!
//! A JSON key containing `]` is reachable today and is not after this
//! change: `| json v="b]"` extracted it here and is 400 at the reference,
//! and the bracket-quoted escape hatch that reaches every other
//! punctuated key does not reach this one either, because `scanStr`
//! terminates on `]` as well as on `"` (`lexer.go:124-125`). Taken
//! deliberately — being able to extract something the reference cannot is
//! a query that works here and does not port. Ledgered as
//! `json-expression-bracket-key-unreachable` with both measurements.
//!
//! # `IsValidLabelName`, again recorded and not implemented
//!
//! `NewJSONExpressionParser` also runs
//! `model.UTF8Validation.IsValidLabelName(exp.Identifier)`
//! (`log/parser.go:643 @ v3.7.4`). #247 established that the identifier
//! can be neither empty nor invalid UTF-8, so the predicate is total over
//! the grammar's output and no test can distinguish implementing it from
//! not. Recorded, not implemented, not gated — the argument and its
//! falsifier are in [`super::pattern_expr`]'s docs (R7), which also
//! carries the fourteen non-derivable readings behind BOTH halves in one
//! table rather than duplicating it here.

use super::pipeline::JsonPathSeg;

/// Every way `jsonexpr.Parse` (`pkg/logql/log/jsonexpr/parser.go:11-18 @
/// v3.7.4`) can fail.
///
/// A TYPE rather than a `String`, for the reason given in
/// [`super::pattern_expr`]: [`JsonExprError::message`] matches every
/// variant with no `_` arm, beside the rule table above, so a rule added
/// here without a row in that table fails to compile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum JsonExprError {
    /// `lexer.go:80` — a byte no token can start with.
    UnexpectedChar { ch: char },
    /// `lexer.go:138` — a `.` inside an integer.
    FloatIndex,
    /// `lexer.go:147` — a non-digit inside an integer. `ch` is NUL when
    /// the integer ran to end of input.
    NonIntegerIndex { ch: char },
    /// `strconv.Atoi`'s `ErrRange`, relayed by `lexer.go:56`. The index
    /// bound is Go's `int`, so this is "does not fit in i64" and not
    /// "does not fit in usize" — the distinction is measured, see the
    /// module docs.
    IndexOutOfRange { digits: String },
    /// `lexer.go:26` via the yacc `Error` callback. The LAST writer to
    /// `sc.err` wins, so this OVERWRITES any lexer error raised in the
    /// same parse.
    Syntax {
        unexpected: Token,
        position: Position,
    },
}

/// The reference's token names, as its yacc messages spell them
/// (`jsonexpr.y:20-23 @ v3.7.4`; `$end` is goyacc's).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Token {
    End,
    Dot,
    Lsb,
    Rsb,
    Str,
    Field,
    Index,
}

impl Token {
    fn name(self) -> &'static str {
        match self {
            Token::End => "$end",
            Token::Dot => "DOT",
            Token::Lsb => "LSB",
            Token::Rsb => "RSB",
            Token::Str => "STRING",
            Token::Field => "FIELD",
            Token::Index => "INDEX",
        }
    }
}

/// Where in the grammar a syntax error was raised — which decides the
/// "expecting …" clause. **Every one of the five was measured against
/// the pinned digest**, and the "no clause" case is not an omission: in
/// a state whose only action is a default reduction goyacc cannot
/// enumerate the shiftable tokens and prints none.
///
/// | position | message | measured with |
/// |---|---|---|
/// | [`Position::Start`] | `unexpected X, expecting LSB or FIELD` | `""`, `" "`, `.b`, `"b"`, `]`, `0 ` |
/// | [`Position::AfterValues`] | `unexpected X` (no clause) | `b]`, `b c`, `b "c"`, `b 9223372036854775807]`, `b[0]]` |
/// | [`Position::AfterLsb`] | `unexpected X, expecting STRING or INDEX` | `b[]`, `b[c]`, `b[[`, `b[.`, `b[0`, `b[-1]` |
/// | [`Position::AfterKey`] | `unexpected X, expecting RSB` | `b["c" `, `b[0 0]`, `["b]"]` |
/// | [`Position::AfterDot`] | `unexpected X, expecting FIELD` | `b.`, `b..c`, `b.[0]`, `b.]`, `b."c"`, `b.0` |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Position {
    Start,
    AfterValues,
    AfterLsb,
    AfterKey,
    AfterDot,
}

impl Position {
    fn expecting(self) -> Option<&'static str> {
        match self {
            Position::Start => Some("LSB or FIELD"),
            Position::AfterValues => None,
            Position::AfterLsb => Some("STRING or INDEX"),
            Position::AfterKey => Some("RSB"),
            Position::AfterDot => Some("FIELD"),
        }
    }
}

impl JsonExprError {
    /// The inner message the call site wraps. Matches the reference's own
    /// text for every rule but [`JsonExprError::IndexOutOfRange`] — see
    /// the module docs for which is which and why.
    pub(super) fn message(&self) -> String {
        match self {
            JsonExprError::UnexpectedChar { ch } => format!("unexpected char {ch}"),
            JsonExprError::FloatIndex => "cannot use float as array index".to_string(),
            JsonExprError::NonIntegerIndex { ch } => format!("non-integer value: {ch}"),
            // OURS, deliberately: the reference names `strconv.Atoi` here.
            JsonExprError::IndexOutOfRange { digits } => {
                format!("array index out of range: {digits}")
            }
            JsonExprError::Syntax {
                unexpected,
                position,
            } => match position.expecting() {
                Some(what) => {
                    format!(
                        "syntax error: unexpected {}, expecting {what}",
                        unexpected.name()
                    )
                }
                None => format!("syntax error: unexpected {}", unexpected.name()),
            },
        }
    }
}

/// One lexed token, or the reason lexing stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Lexed {
    Tok(Token, Payload),
    /// Input exhausted, or a NUL read at the top of the lexer
    /// (`read()` yields 0 for both, `lexer.go:44-46`).
    Eof,
    /// The lexer recorded `sc.err` and returned `0`, so the token stream
    /// ENDS here — which is what lets a later syntax error overwrite the
    /// message.
    Err(JsonExprError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Payload {
    None,
    Text(String),
    Number(usize),
}

/// `isWhitespace` (`lexer.go:168 @ v3.7.4`) — space, tab and newline
/// only.
///
/// The three predicates here, and [`lex_json`], carry a `json_`/`_json`
/// mark because
/// [`super::logfmt_expr`] has same-named ones for ITS lexer and
/// `logql_post_agg_witness.rs`'s call-graph closure resolves free
/// functions by name across `src/logql`. Two bodies under one name would
/// make that census silently follow the wrong one. The modules stay
/// independent: nothing is shared, only the names are distinct.
const fn is_json_space(c: char) -> bool {
    c == ' ' || c == '\t' || c == '\n'
}

/// `isStartIdentifier` (`lexer.go:86-88 @ v3.7.4`).
const fn is_json_start_ident(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

/// `isIdentifier` (`lexer.go:90-92 @ v3.7.4`).
const fn is_json_ident(c: char) -> bool {
    is_json_start_ident(c) || c.is_ascii_digit()
}

/// One `Scanner.lex` call (`lexer.go:41-84 @ v3.7.4`), advancing `i` over
/// `expr`'s bytes.
fn lex_json(expr: &str, i: &mut usize) -> Lexed {
    loop {
        let Some(c) = expr[*i..].chars().next() else {
            return Lexed::Eof;
        };
        // A NUL byte reads as end of input, exactly as EOF does.
        if c == '\0' {
            return Lexed::Eof;
        }
        *i += c.len_utf8();
        if is_json_space(c) {
            continue;
        }
        if c.is_ascii_digit() {
            *i -= c.len_utf8(); // `sc.unread()`
            return match scan_int(expr, i) {
                Ok(v) => Lexed::Tok(Token::Index, Payload::Number(v)),
                Err(e) => Lexed::Err(e),
            };
        }
        return match c {
            '[' => Lexed::Tok(Token::Lsb, Payload::None),
            ']' => Lexed::Tok(Token::Rsb, Payload::None),
            '.' => Lexed::Tok(Token::Dot, Payload::None),
            c if is_json_start_ident(c) => {
                *i -= c.len_utf8();
                Lexed::Tok(Token::Field, Payload::Text(scan_field(expr, i)))
            }
            '"' => {
                *i -= 1;
                Lexed::Tok(Token::Str, Payload::Text(scan_str(expr, i)))
            }
            c => Lexed::Err(JsonExprError::UnexpectedChar { ch: c }),
        };
    }
}

/// `scanField` (`lexer.go:94-107 @ v3.7.4`).
fn scan_field(expr: &str, i: &mut usize) -> String {
    let start = *i;
    while let Some(c) = expr[*i..].chars().next() {
        if !is_json_ident(c) || c == '\0' {
            break;
        }
        *i += c.len_utf8();
    }
    expr[start..*i].to_string()
}

/// `scanStr` (`lexer.go:109-130 @ v3.7.4`). The opening `"` has been
/// unread, so the `r != '"'` branch at `:114` cannot be taken — the one
/// rule in either of #388's tables supported by nothing but this reading.
///
/// The string ends at a `"`, at a **`]`**, at a NUL or at end of input,
/// and the terminator is CONSUMED. The `]` is what makes a key
/// containing one unreachable by any expression.
fn scan_str(expr: &str, i: &mut usize) -> String {
    *i += 1; // the opening `"`
    let start = *i;
    while let Some(c) = expr[*i..].chars().next() {
        if c == '\0' {
            break;
        }
        *i += c.len_utf8();
        if c == '"' || c == ']' {
            return expr[start..*i - c.len_utf8()].to_string();
        }
    }
    expr[start..*i].to_string()
}

/// `scanInt` (`lexer.go:132-154 @ v3.7.4`), including its final
/// `strconv.Atoi` — whose range error is a rule of its own.
fn scan_int(expr: &str, i: &mut usize) -> Result<usize, JsonExprError> {
    let start = *i;
    loop {
        let c = expr[*i..].chars().next().unwrap_or('\0');
        if c == '.' && *i > start {
            return Err(JsonExprError::FloatIndex);
        }
        if is_json_space(c) || c == '.' || c == ']' {
            break; // `sc.unread()`
        }
        if !c.is_ascii_digit() {
            // Includes end of input and an embedded NUL: `read()` yields
            // 0 for both and `%c` formats it, which is why the committed
            // witness for this arm is a PRINTABLE one (`b 1x`).
            return Err(JsonExprError::NonIntegerIndex { ch: c });
        }
        *i += c.len_utf8();
    }
    let digits = &expr[start..*i];
    // Go's `int` is 64-bit on every platform Loki ships for, so the
    // bound is i64 and NOT usize: measured, `b[9223372036854775807]` is
    // 200 and `b[9223372036854775808]` is 400.
    digits
        .parse::<i64>()
        .map(|v| v as usize)
        .map_err(|_| JsonExprError::IndexOutOfRange {
            digits: digits.to_string(),
        })
}

/// Parses a `| json <id>="<expr>"` extraction expression into the path
/// the extractor walks.
///
/// Mirrors `jsonexpr.Parse` (`parser.go:11-18 @ v3.7.4`) including its
/// **error precedence**: the lexer records `sc.err` and returns `0`, and
/// a subsequent yacc syntax error OVERWRITES it (`lexer.go:25-28`). So a
/// lexer error survives only when the parser can accept the prefix it has
/// already shifted — `b-c` reports `unexpected char -`, while `b[0`
/// reports the syntax error and not `non-integer value`.
///
/// The grammar is `values := FIELD | key_access | index_access | values
/// key_access | values index_access | values DOT FIELD`
/// (`jsonexpr.y:34-41`), i.e. one leading element and then any number of
/// `[…]` or `.FIELD` steps — which is exactly the loop below.
pub(super) fn parse_json_expr(expr: &str) -> Result<Vec<JsonPathSeg>, JsonExprError> {
    // The reference's parser pulls tokens lazily and stops at the first
    // syntax error, so a lexer error PAST that point never happens. It
    // cannot change the outcome here either: a parse that fails reports
    // the syntax error whichever way round, and a parse that succeeds
    // consumed every token up to `$end` anyway.
    let mut toks: Vec<(Token, Payload)> = Vec::new();
    let mut lex_err: Option<JsonExprError> = None;
    let mut i = 0usize;
    loop {
        match lex_json(expr, &mut i) {
            Lexed::Eof => break,
            Lexed::Err(e) => {
                lex_err = Some(e);
                break;
            }
            Lexed::Tok(t, p) => toks.push((t, p)),
        }
    }

    let mut segs: Vec<JsonPathSeg> = Vec::new();
    let mut k = 0usize;
    let peek = |k: usize| toks.get(k).map(|(t, _)| *t).unwrap_or(Token::End);
    let syntax = |k: usize, position: Position| JsonExprError::Syntax {
        unexpected: peek(k),
        position,
    };

    // `values`' first element: FIELD, or a bracketed step.
    match peek(0) {
        Token::Field => {
            let Payload::Text(name) = &toks[0].1 else {
                unreachable!("a FIELD carries its text")
            };
            segs.push(JsonPathSeg::Field(name.clone()));
            k = 1;
        }
        Token::Lsb => {}
        _ => return Err(syntax(0, Position::Start)),
    }

    // …then any number of `[…]` or `.FIELD` steps.
    while k < toks.len() {
        match toks[k].0 {
            Token::Lsb => {
                k += 1;
                match toks.get(k) {
                    Some((Token::Str, Payload::Text(key))) => {
                        segs.push(JsonPathSeg::Field(key.clone()))
                    }
                    Some((Token::Index, Payload::Number(n))) => segs.push(JsonPathSeg::Index(*n)),
                    _ => return Err(syntax(k, Position::AfterLsb)),
                }
                k += 1;
                if peek(k) != Token::Rsb {
                    return Err(syntax(k, Position::AfterKey));
                }
                k += 1;
            }
            Token::Dot => {
                k += 1;
                match toks.get(k) {
                    Some((Token::Field, Payload::Text(name))) => {
                        segs.push(JsonPathSeg::Field(name.clone()))
                    }
                    _ => return Err(syntax(k, Position::AfterDot)),
                }
                k += 1;
            }
            _ => return Err(syntax(k, Position::AfterValues)),
        }
    }

    // A complete parse: now, and only now, a lexer error that the parser
    // never overwrote is what the user is told.
    if let Some(e) = lex_err {
        return Err(e);
    }
    Ok(segs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(s: &str) -> JsonPathSeg {
        JsonPathSeg::Field(s.to_string())
    }

    fn parse(expr: &str) -> Result<Vec<JsonPathSeg>, JsonExprError> {
        parse_json_expr(expr)
    }

    /// The four shapes PulsusDB REFUSED at `5d91ef1` and the reference
    /// serves — the harmful direction, and the reason this half exists.
    /// Their values were measured on the pinned container over
    /// `{"b":{"c":"nested","0":"zero"},"b-c":"dash","arr":["a0","a1"]}`.
    #[test]
    fn whitespace_is_skipped_between_every_token() {
        assert_eq!(
            parse("arr[ 0 ]"),
            Ok(vec![field("arr"), JsonPathSeg::Index(0)])
        );
        assert_eq!(
            parse("arr[0 ]"),
            Ok(vec![field("arr"), JsonPathSeg::Index(0)])
        );
        assert_eq!(parse(r#"b[ "c" ]"#), Ok(vec![field("b"), field("c")]));
        assert_eq!(parse(r#"[ "b-c" ]"#), Ok(vec![field("b-c")]));
        // And the two that were served but mis-resolved: the path is the
        // parsed one, so `b . c` is `["b","c"]` and not `["b "," c"]`.
        assert_eq!(parse("b . c"), Ok(vec![field("b"), field("c")]));
        assert_eq!(parse("b.c "), Ok(vec![field("b"), field("c")]));
    }

    /// The six productions, each exercised.
    #[test]
    fn the_six_productions_are_all_reachable() {
        assert_eq!(parse("b"), Ok(vec![field("b")])); // values := FIELD
        assert_eq!(parse(r#"["b"]"#), Ok(vec![field("b")])); // := key_access
        assert_eq!(parse("[0]"), Ok(vec![JsonPathSeg::Index(0)])); // := index_access
        assert_eq!(parse(r#"b["c"]"#), Ok(vec![field("b"), field("c")]));
        assert_eq!(parse("b[0]"), Ok(vec![field("b"), JsonPathSeg::Index(0)]));
        assert_eq!(parse("b.c"), Ok(vec![field("b"), field("c")]));
        // …and they compose.
        assert_eq!(
            parse(r#"b[0].c["d"]"#),
            Ok(vec![
                field("b"),
                JsonPathSeg::Index(0),
                field("c"),
                field("d")
            ])
        );
    }

    /// `lexer.go:80` — the lexer's message survives, because the parser
    /// accepted the prefix it already had.
    #[test]
    fn a_bad_character_after_a_complete_values_is_unexpected_char() {
        for (expr, ch) in [
            ("b-c", '-'),
            ("b/c", '/'),
            ("b:c", ':'),
            ("b=c", '='),
            ("b,c", ','),
            ("b!", '!'),
            ("bé", 'é'),
        ] {
            assert_eq!(
                parse(expr),
                Err(JsonExprError::UnexpectedChar { ch }),
                "{expr:?}"
            );
        }
    }

    /// `lexer.go:26` — the LAST writer wins. `é` and `0b` reach the
    /// lexer's error first and are told the syntax error instead, which
    /// is a DIFFERENT message from the class above and tells the user
    /// which character was at fault.
    #[test]
    fn a_syntax_error_overwrites_a_lexer_error_from_the_same_parse() {
        for expr in ["é", "0b", "b[0", "b[-1]", "b[1.5]", "b['c']", "b.0"] {
            assert!(
                matches!(parse(expr), Err(JsonExprError::Syntax { .. })),
                "{expr:?}: {:?}",
                parse(expr)
            );
        }
        assert_ne!(
            parse("b-c"),
            parse("é"),
            "collapsing the two classes would hide which character was at fault"
        );
    }

    /// `lexer.go:138` and `:147`, which reasoning wrongly called
    /// unobservable. A digit outside a bracket starts an INDEX token, and
    /// the parser can accept what it already has — so the lexer's message
    /// is what the user sees.
    #[test]
    fn scan_int_errors_survive_outside_a_bracket() {
        for expr in ["b 1.5", "b[0] 1.5", "b.c 1.5"] {
            assert_eq!(parse(expr), Err(JsonExprError::FloatIndex), "{expr:?}");
        }
        assert_eq!(
            parse("b 1x"),
            Err(JsonExprError::NonIntegerIndex { ch: 'x' })
        );
        // `b 0` reaches the same arm with the NUL that end-of-input
        // yields. Asserted here, where a NUL costs nothing, and NOT
        // committed as a golden: the reference's response body carries
        // that unprintable byte.
        assert_eq!(
            parse("b 0"),
            Err(JsonExprError::NonIntegerIndex { ch: '\0' })
        );
    }

    /// The index bound is Go's `int`, measured. A `usize` parse would
    /// accept the second of these and introduce a divergence.
    #[test]
    fn an_index_is_bounded_by_go_int_not_by_usize() {
        assert_eq!(
            parse("b[9223372036854775807]"),
            Ok(vec![
                field("b"),
                JsonPathSeg::Index(9_223_372_036_854_775_807)
            ])
        );
        assert!(matches!(
            parse("b[9223372036854775808]"),
            Err(JsonExprError::Syntax { .. })
        ));
        // Outside a bracket the parser accepts the prefix, so the range
        // error itself is what survives.
        assert_eq!(
            parse("b 9223372036854775808]"),
            Err(JsonExprError::IndexOutOfRange {
                digits: "9223372036854775808".to_string()
            })
        );
    }

    /// `scanStr` terminates on `]` as well as on `"` (`lexer.go:124`),
    /// which is what makes a key containing `]` unreachable by ANY
    /// expression — the one capability this change withdraws.
    #[test]
    fn a_bracket_ends_a_quoted_key_so_such_a_key_is_unreachable() {
        // The `]` closes the string, leaving nothing to close the `[`.
        assert!(matches!(
            parse(r#"["b]"]"#),
            Err(JsonExprError::Syntax {
                unexpected: Token::Str,
                position: Position::AfterKey
            })
        ));
        assert!(matches!(
            parse("b]"),
            Err(JsonExprError::Syntax {
                unexpected: Token::Rsb,
                position: Position::AfterValues
            })
        ));
    }

    /// Every syntax-error POSITION, with the message measured against the
    /// pinned container. The "no expecting clause" case is as real as
    /// the others.
    #[test]
    fn every_syntax_error_position_renders_the_measured_message() {
        for (expr, want) in [
            ("", "syntax error: unexpected $end, expecting LSB or FIELD"),
            (" ", "syntax error: unexpected $end, expecting LSB or FIELD"),
            (".b", "syntax error: unexpected DOT, expecting LSB or FIELD"),
            (
                r#""b""#,
                "syntax error: unexpected STRING, expecting LSB or FIELD",
            ),
            ("]", "syntax error: unexpected RSB, expecting LSB or FIELD"),
            (
                "0 ",
                "syntax error: unexpected INDEX, expecting LSB or FIELD",
            ),
            ("b c", "syntax error: unexpected FIELD"),
            ("b]", "syntax error: unexpected RSB"),
            (r#"b "c""#, "syntax error: unexpected STRING"),
            (
                "b[]",
                "syntax error: unexpected RSB, expecting STRING or INDEX",
            ),
            (
                "b[c]",
                "syntax error: unexpected FIELD, expecting STRING or INDEX",
            ),
            (
                "b[[",
                "syntax error: unexpected LSB, expecting STRING or INDEX",
            ),
            (
                "b[.",
                "syntax error: unexpected DOT, expecting STRING or INDEX",
            ),
            (
                "b[0",
                "syntax error: unexpected $end, expecting STRING or INDEX",
            ),
            (r#"b["c" "#, "syntax error: unexpected $end, expecting RSB"),
            ("b[0 0]", "syntax error: unexpected INDEX, expecting RSB"),
            ("b.", "syntax error: unexpected $end, expecting FIELD"),
            ("b..c", "syntax error: unexpected DOT, expecting FIELD"),
            ("b.[0]", "syntax error: unexpected LSB, expecting FIELD"),
            ("b.]", "syntax error: unexpected RSB, expecting FIELD"),
            (
                r#"b."c""#,
                "syntax error: unexpected STRING, expecting FIELD",
            ),
        ] {
            assert_eq!(
                parse(expr).expect_err("refused").message(),
                want,
                "{expr:?}"
            );
        }
    }

    /// A NUL reads as end of input at the top of the lexer, and ends a
    /// string inside `scanStr` — the same split #247 measured for the
    /// logfmt lexer.
    #[test]
    fn a_nul_reads_as_end_of_input() {
        assert_eq!(parse("b\0c"), Ok(vec![field("b")]));
        assert!(matches!(parse("\0b"), Err(JsonExprError::Syntax { .. })));
    }
}
