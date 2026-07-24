//! Lexer output types: a byte-offset [`Span`], a [`Token`] carrying its
//! span, and the [`TokenKind`] enum. Pure data — no lexing or parsing
//! behavior lives here (that is `lexer.rs`/`parser.rs`).

/// A byte-offset range into the original query string, `[start, end)`.
/// Every token — and every [`crate::TraceQlError`] — carries one so error
/// messages and the `400` query-error envelope can point at the exact
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

/// The full token alphabet for the M4 TraceQL search subset, plus the
/// operators needed to *recognize* (not evaluate) out-of-scope constructs
/// (structural operators, negation, arithmetic, bracketed attributes) so
/// the parser can name them in a `NotYetSupported` error instead of
/// failing generically.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TokenKind {
    LBrace,
    RBrace,
    LParen,
    RParen,
    /// `[` — only reachable as a bracketed-attribute boundary reject
    /// (`span.["…"]` is recognized-but-unsupported in M4).
    LBracket,
    /// `]` — see [`TokenKind::LBracket`].
    RBracket,
    Comma,
    /// `.` — attribute-scope separator (`span.`, `resource.`) and the
    /// leading unscoped-attribute form (`.attr`). A `.` immediately
    /// followed by a digit is instead lexed as part of a fractional
    /// number/duration literal (`.5s`).
    Dot,
    /// `:` — the intrinsic scope separator (`span:`, `trace:` — the
    /// colon-scoped intrinsic namespace, issue #184). Disambiguated from
    /// the dotted attribute scope purely by parser position; an unknown
    /// colon scope (`event:`, `link:`, `instrumentation:`) is a generic
    /// parse error, not a named boundary.
    Colon,

    /// `=` — comparison equality.
    Eq,
    /// `!=` — comparison inequality.
    Neq,
    /// `=~` — regex comparison.
    Re,
    /// `!~` — negated regex comparison.
    Nre,
    /// `>` — a comparison *inside* a field expression, the structural
    /// child operator (issue #172) *between* spansets. Disambiguated
    /// purely by parser position.
    Gt,
    /// `>=` — dual-role like [`TokenKind::Gt`].
    Gte,
    /// `<` — dual-role like [`TokenKind::Gt`].
    Lt,
    /// `<=` — dual-role like [`TokenKind::Gt`].
    Lte,

    /// `&&` — boolean AND, within a spanset and across spansets.
    AndAnd,
    /// `||` — boolean OR, within a spanset and across spansets.
    OrOr,
    /// `|` — introduces a pipeline stage (aggregate filter or `select`).
    Pipe,

    /// `>>` — structural descendant operator (issue #172).
    Shr,
    /// `<<` — structural ancestor operator (M7 boundary, recognition
    /// only).
    Shl,
    /// `~` — structural sibling operator (issue #172).
    Tilde,
    /// `!` — negation (M7 boundary, recognition only). `!=`/`!~` are
    /// their own tokens.
    Bang,
    /// A single `&` — never valid TraceQL (`&&` is the boolean AND);
    /// always a positioned parse error.
    Amp,

    /// `+` — arithmetic (M7 boundary, recognition only).
    Plus,
    /// `-` — arithmetic (M7 boundary, recognition only). Never folded
    /// into a duration/number literal: signed literals are positioned
    /// parse errors (docs/api.md §4.2 duration grammar).
    Minus,
    /// `*` — arithmetic (M7 boundary, recognition only).
    Star,
    /// `/` — arithmetic (M7 boundary, recognition only).
    Slash,
    /// `%` — arithmetic modulo (field-expression arithmetic).
    Percent,
    /// `^` — arithmetic exponentiation (field-expression arithmetic).
    Caret,

    Ident(String),
    /// An unescaped string value (double-quoted Go-style escapes or a
    /// backtick raw string already decoded by the lexer).
    String(String),
    /// The raw text of a single-group duration literal (e.g. `"2s"`,
    /// `"1.5s"`, `".5s"`), unparsed — `duration::parse_duration` turns it
    /// into nanoseconds. Compound literals never lex as one token: `1h30m`
    /// is `Duration("1h")` then `Duration("30m")`, and the leftover
    /// produces a positioned parse error (docs/api.md §4.2).
    Duration(String),
    /// The raw text of a numeric literal (e.g. `"500"`, `"1.5"`), kept
    /// raw in the AST (`Value::Number`) — T5 parses it to `val_num`.
    Number(String),

    Eof,
}
