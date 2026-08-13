//! Go `text/template` lexer port (issue #230) — a faithful state-machine
//! port of `text/template/parse/lex.go` (go1.26.5, the toolchain the
//! reference binary ships): text vs action, `{{- -}}` trim markers,
//! `{{/* */}}` comments, raw/interpreted strings, char constants,
//! numbers (hex/octal/binary/float/imaginary syntax — validity is the
//! parser's job), `$` variables, `.field`s, parens, `|`, `:=`/`=`.
//! Item positions are **byte offsets into the template text** — the
//! execution-error column rule depends on them (plan v1 §2, `parse.go`
//! `ErrorContext`).
//!
//! Deliberately lazy (item-at-a-time), like Go's channel-driven lexer:
//! an early parse error must surface before a later lex error.

/// One lexed token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    pub typ: ItemType,
    /// Byte offset of the item in the template text.
    pub pos: usize,
    pub val: String,
    /// 1-based line the item starts on.
    pub line: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemType {
    Error,
    Bool,
    Char,
    CharConstant,
    Complex,
    Assign,
    Declare,
    Eof,
    Field,
    Identifier,
    LeftDelim,
    LeftParen,
    Number,
    Pipe,
    RawString,
    RightDelim,
    RightParen,
    Space,
    String,
    Text,
    Variable,
    // Keywords (order irrelevant; all compare > "keyword" in Go's enum).
    Block,
    Break,
    Continue,
    Dot,
    Define,
    Else,
    End,
    If,
    Nil,
    Range,
    Template,
    With,
}

impl ItemType {
    pub fn is_keyword(self) -> bool {
        matches!(
            self,
            ItemType::Block
                | ItemType::Break
                | ItemType::Continue
                | ItemType::Dot
                | ItemType::Define
                | ItemType::Else
                | ItemType::End
                | ItemType::If
                | ItemType::Nil
                | ItemType::Range
                | ItemType::Template
                | ItemType::With
        )
    }
}

impl Item {
    /// Go `item.String()` — the exact token rendering parse errors embed
    /// (`unexpected "," in range`, `unexpected {{end}}`).
    pub fn display(&self) -> String {
        match self.typ {
            ItemType::Eof => "EOF".to_string(),
            ItemType::Error => self.val.clone(),
            t if t.is_keyword() => format!("<{}>", self.val),
            _ => {
                if self.val.chars().count() > 10 {
                    // Go `%.10q...`: quote the first 10 runes.
                    let prefix: String = self.val.chars().take(10).collect();
                    format!("{}...", go_quote_str(&prefix))
                } else {
                    go_quote_str(&self.val)
                }
            }
        }
    }
}

/// `strconv.Quote` over a valid-UTF-8 string — token display only (the
/// full byte-exact quoting for `%q` lives in `gofmt`).
fn go_quote_str(s: &str) -> String {
    crate::logql::template::gofmt::quote_bytes(s.as_bytes(), '"')
}

const LEFT_DELIM: &str = "{{";
const RIGHT_DELIM: &str = "}}";
const LEFT_COMMENT: &str = "/*";
const RIGHT_COMMENT: &str = "*/";
const SPACE_CHARS: &[char] = &[' ', '\t', '\r', '\n'];

/// Named apart from `logfmt_expr.rs`'s `is_space` (issue #302 — the
/// census keys free functions by bare name). This one classifies a
/// `char` against the template lexer's space set; that one classifies a
/// logfmt byte.
fn is_template_space(r: char) -> bool {
    SPACE_CHARS.contains(&r)
}

fn is_alpha_numeric(r: char) -> bool {
    r == '_' || r.is_alphanumeric()
}

/// `hasLeftTrimMarker`: `-` followed by a space (after `{{`).
fn has_left_trim_marker(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() >= 2 && b[0] == b'-' && matches!(b[1], b' ' | b'\t' | b'\r' | b'\n')
}

/// `hasRightTrimMarker`: a space followed by `-` (before `}}`).
fn has_right_trim_marker(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() >= 2 && matches!(b[0], b' ' | b'\t' | b'\r' | b'\n') && b[1] == b'-'
}

/// marker + the space before/after it.
const TRIM_MARKER_LEN: usize = 2;

pub struct Lexer<'t> {
    input: &'t str,
    /// Current scan position (byte).
    pos: usize,
    /// Start of the item being scanned.
    start: usize,
    inside_action: bool,
    paren_depth: i32,
    done: bool,
}

impl<'t> Lexer<'t> {
    pub fn new(input: &'t str) -> Self {
        Lexer {
            input,
            pos: 0,
            start: 0,
            inside_action: false,
            paren_depth: 0,
            done: false,
        }
    }

    fn rest(&self) -> &'t str {
        &self.input[self.pos..]
    }

    fn next(&mut self) -> Option<char> {
        let c = self.rest().chars().next()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn backup(&mut self, c: char) {
        self.pos -= c.len_utf8();
    }

    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn accept(&mut self, valid: &str) -> bool {
        if let Some(c) = self.peek()
            && valid.contains(c)
        {
            self.next();
            return true;
        }
        false
    }

    fn accept_run(&mut self, valid: &str) {
        while self.accept(valid) {}
    }

    /// 1-based line of a byte offset. Compile-time only (templates are
    /// query-length-bounded), never on the per-row path.
    fn line_of(&self, pos: usize) -> usize {
        1 + self.input[..pos].matches('\n').count()
    }

    fn make_item(&mut self, typ: ItemType) -> Item {
        let item = Item {
            typ,
            pos: self.start,
            val: self.input[self.start..self.pos].to_string(),
            line: self.line_of(self.start),
        };
        self.start = self.pos;
        item
    }

    fn ignore(&mut self) {
        self.start = self.pos;
    }

    fn errorf(&mut self, msg: String) -> Item {
        self.done = true;
        Item {
            typ: ItemType::Error,
            pos: self.start,
            val: msg,
            line: self.line_of(self.start),
        }
    }

    fn at_right_delim(&self) -> (bool, bool) {
        let rest = self.rest();
        if has_right_trim_marker(rest) && rest[TRIM_MARKER_LEN..].starts_with(RIGHT_DELIM) {
            return (true, true);
        }
        if rest.starts_with(RIGHT_DELIM) {
            return (true, false);
        }
        (false, false)
    }

    fn at_terminator(&self) -> bool {
        match self.peek() {
            None => true,
            Some(r) if is_template_space(r) => true,
            Some('.') | Some(',') | Some('|') | Some(':') | Some(')') | Some('(') => true,
            _ => self.rest().starts_with(RIGHT_DELIM),
        }
    }

    /// The public interface: the next item. After an Error or EOF item,
    /// returns EOF forever.
    pub fn next_item(&mut self) -> Item {
        if self.done {
            return Item {
                typ: ItemType::Eof,
                pos: self.pos,
                val: String::new(),
                line: self.line_of(self.pos),
            };
        }
        if self.inside_action {
            self.lex_inside_action()
        } else {
            self.lex_text()
        }
    }

    fn lex_text(&mut self) -> Item {
        if let Some(x) = self.rest().find(LEFT_DELIM) {
            if x > 0 {
                self.pos += x;
                // Trim trailing space from the text if the delimiter
                // carries a left trim marker.
                let mut trim_len = 0;
                let delim_end = self.pos + LEFT_DELIM.len();
                if has_left_trim_marker(&self.input[delim_end..]) {
                    let text = &self.input[self.start..self.pos];
                    trim_len = text.len() - text.trim_end_matches(SPACE_CHARS).len();
                }
                self.pos -= trim_len;
                let item = self.make_item(ItemType::Text);
                self.pos += trim_len;
                self.ignore();
                if !item.val.is_empty() {
                    return item;
                }
            }
            return self.lex_left_delim();
        }
        self.pos = self.input.len();
        if self.pos > self.start {
            let item = self.make_item(ItemType::Text);
            self.ignore();
            return item;
        }
        self.done = true;
        self.make_item(ItemType::Eof)
    }

    fn lex_left_delim(&mut self) -> Item {
        self.pos += LEFT_DELIM.len();
        let trim = has_left_trim_marker(self.rest());
        let after_marker = if trim { TRIM_MARKER_LEN } else { 0 };
        if self.input[self.pos + after_marker..].starts_with(LEFT_COMMENT) {
            self.pos += after_marker;
            self.ignore();
            return self.lex_comment();
        }
        let item = self.make_item(ItemType::LeftDelim);
        self.inside_action = true;
        self.pos += after_marker;
        self.ignore();
        self.paren_depth = 0;
        item
    }

    fn lex_comment(&mut self) -> Item {
        self.pos += LEFT_COMMENT.len();
        let Some(x) = self.rest().find(RIGHT_COMMENT) else {
            return self.errorf("unclosed comment".to_string());
        };
        self.pos += x + RIGHT_COMMENT.len();
        let (delim, trim) = self.at_right_delim();
        if !delim {
            return self.errorf("comment ends before closing delimiter".to_string());
        }
        if trim {
            self.pos += TRIM_MARKER_LEN;
        }
        self.pos += RIGHT_DELIM.len();
        if trim {
            let rest = self.rest();
            self.pos += rest.len() - rest.trim_start_matches(SPACE_CHARS).len();
        }
        self.ignore();
        // Comments are never emitted; continue with text.
        self.lex_text()
    }

    fn lex_right_delim(&mut self) -> Item {
        let (_, trim) = self.at_right_delim();
        if trim {
            self.pos += TRIM_MARKER_LEN;
            self.ignore();
        }
        self.pos += RIGHT_DELIM.len();
        let item = self.make_item(ItemType::RightDelim);
        if trim {
            let rest = self.rest();
            self.pos += rest.len() - rest.trim_start_matches(SPACE_CHARS).len();
            self.ignore();
        }
        self.inside_action = false;
        item
    }

    fn lex_inside_action(&mut self) -> Item {
        let (delim, _) = self.at_right_delim();
        if delim {
            if self.paren_depth == 0 {
                return self.lex_right_delim();
            }
            return self.errorf("unclosed left paren".to_string());
        }
        let Some(r) = self.next() else {
            return self.errorf("unclosed action".to_string());
        };
        match r {
            r if is_template_space(r) => {
                self.backup(r);
                self.lex_space()
            }
            '=' => self.make_item(ItemType::Assign),
            ':' => {
                if self.next() != Some('=') {
                    return self.errorf("expected :=".to_string());
                }
                self.make_item(ItemType::Declare)
            }
            '|' => self.make_item(ItemType::Pipe),
            '"' => self.lex_quote(),
            '`' => self.lex_raw_quote(),
            '$' => self.lex_variable(),
            '\'' => self.lex_char(),
            '.' => {
                // Look ahead: ".field" vs a number like ".5".
                match self.peek() {
                    Some(c) if c.is_ascii_digit() => {
                        self.backup('.');
                        self.lex_number()
                    }
                    _ => self.lex_field(),
                }
            }
            '+' | '-' | '0'..='9' => {
                self.backup(r);
                self.lex_number()
            }
            r if is_alpha_numeric(r) => {
                self.backup(r);
                self.lex_identifier()
            }
            '(' => {
                self.paren_depth += 1;
                self.make_item(ItemType::LeftParen)
            }
            ')' => {
                self.paren_depth -= 1;
                if self.paren_depth < 0 {
                    return self.errorf("unexpected right paren".to_string());
                }
                self.make_item(ItemType::RightParen)
            }
            r if (r as u32) <= 0x7F && !r.is_control() => self.make_item(ItemType::Char),
            r => self.errorf(format!(
                "unrecognized character in action: {}",
                go_char_u(r)
            )),
        }
    }

    fn lex_space(&mut self) -> Item {
        let mut num_spaces = 0;
        while let Some(r) = self.peek() {
            if !is_template_space(r) {
                break;
            }
            self.next();
            num_spaces += 1;
        }
        // A trim-marked right delimiter starts with a space we just ate.
        let before = &self.input[self.pos - 1..];
        if has_right_trim_marker(before) && before[TRIM_MARKER_LEN..].starts_with(RIGHT_DELIM) {
            self.pos -= 1; // before the space (always ' ' here, 1 byte)
            if num_spaces == 1 {
                self.start = self.pos;
                return self.lex_right_delim();
            }
        }
        self.make_item(ItemType::Space)
    }

    fn lex_identifier(&mut self) -> Item {
        loop {
            match self.next() {
                Some(r) if is_alpha_numeric(r) => {}
                other => {
                    if let Some(r) = other {
                        self.backup(r);
                    }
                    let word = &self.input[self.start..self.pos];
                    if !self.at_terminator() {
                        let bad = self.peek().unwrap_or('\u{FFFD}');
                        return self.errorf(format!("bad character {}", go_char_u(bad)));
                    }
                    let typ = match word {
                        "block" => ItemType::Block,
                        "break" => ItemType::Break,
                        "continue" => ItemType::Continue,
                        "define" => ItemType::Define,
                        "else" => ItemType::Else,
                        "end" => ItemType::End,
                        "if" => ItemType::If,
                        "nil" => ItemType::Nil,
                        "range" => ItemType::Range,
                        "template" => ItemType::Template,
                        "with" => ItemType::With,
                        "true" | "false" => ItemType::Bool,
                        _ => ItemType::Identifier,
                    };
                    return self.make_item(typ);
                }
            }
        }
    }

    fn lex_field(&mut self) -> Item {
        // The '.' has been scanned.
        self.lex_field_or_variable(ItemType::Field)
    }

    fn lex_variable(&mut self) -> Item {
        if self.at_terminator() {
            return self.make_item(ItemType::Variable);
        }
        self.lex_field_or_variable(ItemType::Variable)
    }

    fn lex_field_or_variable(&mut self, typ: ItemType) -> Item {
        if self.at_terminator() {
            return self.make_item(if typ == ItemType::Variable {
                ItemType::Variable
            } else {
                ItemType::Dot
            });
        }
        loop {
            match self.next() {
                Some(r) if is_alpha_numeric(r) => {}
                other => {
                    if let Some(r) = other {
                        self.backup(r);
                    }
                    if !self.at_terminator() {
                        let bad = self.peek().unwrap_or('\u{FFFD}');
                        return self.errorf(format!("bad character {}", go_char_u(bad)));
                    }
                    return self.make_item(typ);
                }
            }
        }
    }

    fn lex_char(&mut self) -> Item {
        loop {
            match self.next() {
                Some('\\') => match self.next() {
                    Some(r) if r != '\n' => {}
                    _ => return self.errorf("unterminated character constant".to_string()),
                },
                None | Some('\n') => {
                    return self.errorf("unterminated character constant".to_string());
                }
                Some('\'') => break,
                Some(_) => {}
            }
        }
        self.make_item(ItemType::CharConstant)
    }

    fn lex_number(&mut self) -> Item {
        if !self.scan_number() {
            let bad = &self.input[self.start..self.pos];
            return self.errorf(format!("bad number syntax: {}", go_quote_str(bad)));
        }
        if matches!(self.peek(), Some('+') | Some('-')) {
            // Complex: 1+2i.
            self.next();
            if !self.scan_number() || !self.input[self.start..self.pos].ends_with('i') {
                let bad = &self.input[self.start..self.pos];
                return self.errorf(format!("bad number syntax: {}", go_quote_str(bad)));
            }
            return self.make_item(ItemType::Complex);
        }
        self.make_item(ItemType::Number)
    }

    fn scan_number(&mut self) -> bool {
        self.accept("+-");
        let mut digits = "0123456789_";
        if self.accept("0") {
            if self.accept("xX") {
                digits = "0123456789abcdefABCDEF_";
            } else if self.accept("oO") {
                digits = "01234567_";
            } else if self.accept("bB") {
                digits = "01_";
            }
        }
        self.accept_run(digits);
        if self.accept(".") {
            self.accept_run(digits);
        }
        if digits.len() == 10 + 1 && self.accept("eE") {
            self.accept("+-");
            self.accept_run("0123456789_");
        }
        if digits.len() == 16 + 6 + 1 && self.accept("pP") {
            self.accept("+-");
            self.accept_run("0123456789_");
        }
        self.accept("i");
        if self.peek().is_some_and(is_alpha_numeric) {
            self.next();
            return false;
        }
        true
    }

    fn lex_quote(&mut self) -> Item {
        loop {
            match self.next() {
                Some('\\') => match self.next() {
                    Some(r) if r != '\n' => {}
                    _ => return self.errorf("unterminated quoted string".to_string()),
                },
                None | Some('\n') => {
                    return self.errorf("unterminated quoted string".to_string());
                }
                Some('"') => break,
                Some(_) => {}
            }
        }
        self.make_item(ItemType::String)
    }

    fn lex_raw_quote(&mut self) -> Item {
        loop {
            match self.next() {
                None => return self.errorf("unterminated raw quoted string".to_string()),
                Some('`') => break,
                Some(_) => {}
            }
        }
        self.make_item(ItemType::RawString)
    }
}

/// Go `%#U` rendering of a rune: `U+0040 '@'` (printables get the quoted
/// form appended).
fn go_char_u(r: char) -> String {
    let code = r as u32;
    if r.is_control() || (0xD800..=0xDFFF).contains(&code) {
        format!("U+{code:04X}")
    } else {
        format!("U+{code:04X} '{r}'")
    }
}
