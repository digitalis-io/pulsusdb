//! Lexer output types: a byte-offset [`Span`], a [`Token`] carrying its
//! span, and the [`TokenKind`] enum. Pure data — no lexing or parsing
//! behavior lives here (that is `lexer.rs`/`parser.rs`).

/// A byte-offset range into the original query string, `[start, end)`.
/// Every token — and every [`crate::LogQlError`] — carries one so error
/// messages and `X-Pulsus-Explain`-style tooling can point at the exact
/// offending text (docs/api.md §"Errors": "400 for malformed queries with
/// parser position where available").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

/// A single lexed token: its kind plus the byte span it came from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

/// The full token alphabet for the M1 LogQL subset, plus the operators
/// needed to *recognize* (not evaluate) M6 binary expressions so the
/// parser can name them in a `NotYetSupported` error instead of failing
/// generically.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TokenKind {
    LBrace,
    RBrace,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,

    /// `=` — selector matcher equality.
    Eq,
    /// `!=` — selector matcher inequality *and* (positionally) a line
    /// filter *and* (positionally) an M6 binary comparison. Disambiguated
    /// entirely by the parser (docs: architect plan amendments 1-3).
    Neq,
    /// `=~` — selector matcher regex.
    Re,
    /// `!~` — selector matcher negative-regex *and* (positionally) a line
    /// filter. Never a binary operator (amendment 3).
    Nre,

    /// `==` — M6 binary comparison (recognition only).
    EqEq,
    /// `>` — M6 binary comparison (recognition only).
    Gt,
    /// `<` — M6 binary comparison (recognition only).
    Lt,
    /// `>=` — M6 binary comparison (recognition only).
    Gte,
    /// `<=` — M6 binary comparison (recognition only).
    Lte,
    /// `+` — M6 binary arithmetic (recognition only).
    Plus,
    /// `-` — M6 binary arithmetic (recognition only).
    Minus,
    /// `*` — M6 binary arithmetic (recognition only).
    Star,
    /// `/` — M6 binary arithmetic (recognition only).
    Slash,
    /// `%` — M6 binary arithmetic (recognition only).
    Percent,
    /// `^` — M6 binary arithmetic (recognition only).
    Caret,

    /// `|=` — line filter, "contains".
    PipeExact,
    /// `|~` — line filter, "matches regex".
    PipeMatch,
    /// `|` — introduces an M6 pipeline stage (parser, logfmt, label
    /// filter, `line_format`, `label_format`, `unwrap`, ...).
    Pipe,

    Ident(String),
    /// An unescaped string value (double-quoted Go-style escapes or a
    /// backtick raw string already decoded by the lexer).
    String(String),
    /// The raw text of a duration literal (e.g. `"5m"`, `"1h30m"`),
    /// unparsed — `duration::parse_duration` turns it into nanoseconds.
    Duration(String),
    /// The raw text of a numeric literal (e.g. `"0.95"`). Unused by the
    /// M1 grammar (reserved for `quantile_over_time`'s M6 parameter).
    Number(String),

    Eof,
}
