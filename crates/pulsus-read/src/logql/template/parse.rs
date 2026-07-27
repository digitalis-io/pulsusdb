//! Go `text/template` parser port (issue #230) — mirrors
//! `text/template/parse/parse.go` (go1.26.5): the node tree, the
//! grammar, parse-time variable/function checking, `define`/`block`
//! tree-set handling, and `Node.String()` rendering (reproduced because
//! execution errors embed it verbatim: `executing "line" at <substr 5 2
//! .a>`). Every node carries its byte offset into the template text —
//! the `ErrorContext` column rule (plan v1 §2).
//!
//! Error strings follow Go's `template: <name>:<line>: <msg>` parse
//! form. Under the owner's error-wording ruling these are matched on a
//! best-effort basis (accept/reject parity is the contract); in practice
//! the port reproduces Go's wording.

use super::lex::{Item, ItemType, Lexer};

/// One enforced structural-depth limit across BOTH parser recursion
/// sites: nested controls (`if`/`with`/`range`/`block` bodies recurse
/// through `item_list`) and parenthesized pipelines (`term` recurses
/// through `pipeline`). Go caps only the paren site, at 10000
/// (`parse.go maxStackDepth`, error `max expression depth exceeded`),
/// and leaves control nesting to growable goroutine stacks; a fixed
/// 2 MiB Rust stack cannot honour either (measured on this build:
/// ~170 nested controls ≈ 12 KiB/level or ~400 nested parens overflow
/// a 2 MiB thread — a live SIGABRT, the #272 class). 40 is a 4×
/// margin on the worst (control) unit; the error text matches Go's
/// paren-site wording. Ledgered (`template-parse-depth-cap`).
const MAX_PARSE_DEPTH: usize = 40;

/// A parse failure. `msg` is Go's inner text (`function "x" not
/// defined`); `Display` (via [`super::TemplateCompileError`]) prepends
/// `template: <name>:<line>: `.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub line: usize,
    pub msg: String,
}

/// The parsed template set: the root tree plus any `{{define}}`/
/// `{{block}}` sub-trees, keyed by name.
#[derive(Debug, Clone)]
pub struct TreeSet {
    /// The main tree (named "line" or "label").
    pub root: List,
    /// `define`/`block` trees in definition order.
    pub defines: Vec<(String, List)>,
}

impl TreeSet {
    pub fn find(&self, name: &str) -> Option<&List> {
        self.defines
            .iter()
            .rev()
            .find(|(n, _)| n == name)
            .map(|(_, l)| l)
    }
}

// ---------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct List {
    pub pos: usize,
    pub nodes: Vec<Node>,
}

#[derive(Debug, Clone)]
pub enum Node {
    Text {
        pos: usize,
        text: String,
    },
    Action {
        pos: usize,
        pipe: Pipe,
    },
    If {
        pos: usize,
        pipe: Pipe,
        list: List,
        else_list: Option<List>,
    },
    Range {
        pos: usize,
        pipe: Pipe,
        list: List,
        else_list: Option<List>,
    },
    With {
        pos: usize,
        pipe: Pipe,
        list: List,
        else_list: Option<List>,
    },
    /// `{{template "name" pipe}}` (also what `{{block}}` compiles to).
    Template {
        pos: usize,
        name: String,
        pipe: Option<Pipe>,
    },
    Break {
        pos: usize,
    },
    Continue {
        pos: usize,
    },
}

#[derive(Debug, Clone)]
pub struct Pipe {
    pub pos: usize,
    /// `=` (assignment to an existing variable) vs `:=` (declaration).
    pub is_assign: bool,
    /// Declared/assigned variable names (with `$`).
    pub decl: Vec<Var>,
    pub cmds: Vec<Cmd>,
}

#[derive(Debug, Clone)]
pub struct Var {
    pub pos: usize,
    /// Full text including `$` and any chained fields (`$x.y`).
    pub idents: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Cmd {
    pub pos: usize,
    pub args: Vec<Arg>,
}

#[derive(Debug, Clone)]
pub enum Arg {
    Dot {
        pos: usize,
    },
    Nil {
        pos: usize,
    },
    /// `.a.b` — idents `["a","b"]`. Pos per Go's chain-merge rule: the
    /// offset of the SECOND field item when fields were chained
    /// (`parse.go` `operand`).
    Field {
        pos: usize,
        idents: Vec<String>,
    },
    /// `$x` or `$x.y.z` — idents `["$x","y","z"]`.
    Var(Var),
    /// A chain rooted at a non-field/non-var term: `(pipeline).x.y`.
    Chain {
        pos: usize,
        base: Box<Arg>,
        fields: Vec<String>,
    },
    Bool {
        pos: usize,
        val: bool,
    },
    Number {
        pos: usize,
        /// Original literal text (rendered in errors).
        text: String,
        num: Num,
    },
    /// Quoted string constant; `text` is the ORIGINAL quoted form
    /// (rendered in errors), `val` the unquoted bytes.
    Str {
        pos: usize,
        text: String,
        val: Vec<u8>,
    },
    /// Function name.
    Ident {
        pos: usize,
        name: String,
    },
    /// Parenthesized pipeline.
    Pipe {
        pos: usize,
        pipe: Pipe,
    },
}

/// A number constant, Go-typed per `exec.go idealConstant`: `int` for
/// integer syntax (INCLUDING character constants — `idealConstant`
/// returns plain `int` for them), `uint64` above `i64::MAX`, `float64`
/// for float syntax, `complex128` for the `i`-suffixed forms.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Num {
    Int(i64),
    Uint(u64),
    Float(f64),
    Complex(f64, f64),
}

impl Arg {
    pub fn pos(&self) -> usize {
        match self {
            Arg::Dot { pos }
            | Arg::Nil { pos }
            | Arg::Field { pos, .. }
            | Arg::Chain { pos, .. }
            | Arg::Bool { pos, .. }
            | Arg::Number { pos, .. }
            | Arg::Str { pos, .. }
            | Arg::Ident { pos, .. }
            | Arg::Pipe { pos, .. } => *pos,
            Arg::Var(v) => v.pos,
        }
    }

    /// Go `Node.String()` — embedded in execution errors.
    pub fn render(&self, out: &mut String) {
        match self {
            Arg::Dot { .. } => out.push('.'),
            Arg::Nil { .. } => out.push_str("nil"),
            Arg::Field { idents, .. } => {
                for id in idents {
                    out.push('.');
                    out.push_str(id);
                }
            }
            Arg::Var(v) => {
                for (i, id) in v.idents.iter().enumerate() {
                    if i > 0 {
                        out.push('.');
                    }
                    out.push_str(id);
                }
            }
            Arg::Chain { base, fields, .. } => {
                // A Pipe base renders its own parens via Arg::render.
                base.render(out);
                for f in fields {
                    out.push('.');
                    out.push_str(f);
                }
            }
            Arg::Bool { val, .. } => out.push_str(if *val { "true" } else { "false" }),
            Arg::Number { text, .. } => out.push_str(text),
            Arg::Str { text, .. } => out.push_str(text),
            Arg::Ident { name, .. } => out.push_str(name),
            Arg::Pipe { pipe, .. } => {
                out.push('(');
                pipe.render(out);
                out.push(')');
            }
        }
    }

    pub fn render_string(&self) -> String {
        let mut s = String::new();
        self.render(&mut s);
        s
    }
}

impl Pipe {
    pub fn render(&self, out: &mut String) {
        if !self.decl.is_empty() {
            for (i, v) in self.decl.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                for (j, id) in v.idents.iter().enumerate() {
                    if j > 0 {
                        out.push('.');
                    }
                    out.push_str(id);
                }
            }
            out.push_str(" := ");
        }
        for (i, c) in self.cmds.iter().enumerate() {
            if i > 0 {
                out.push_str(" | ");
            }
            c.render(out);
        }
    }

    pub fn render_string(&self) -> String {
        let mut s = String::new();
        self.render(&mut s);
        s
    }
}

impl Cmd {
    pub fn render(&self, out: &mut String) {
        for (i, arg) in self.args.iter().enumerate() {
            if i > 0 {
                out.push(' ');
            }
            if let Arg::Pipe { pipe, .. } = arg {
                out.push('(');
                pipe.render(out);
                out.push(')');
            } else {
                arg.render(out);
            }
        }
    }

    pub fn render_string(&self) -> String {
        let mut s = String::new();
        self.render(&mut s);
        s
    }
}

// ---------------------------------------------------------------------
// The parser
// ---------------------------------------------------------------------

pub struct Parser<'t> {
    /// Template display name ("line"/"label") for error prefixes; the
    /// current sub-tree name for `define`.
    name: String,
    text: &'t str,
    lexer: Lexer<'t>,
    /// Up to 3 tokens of backup (Go `backup3`).
    peeked: Vec<Item>,
    vars: Vec<String>,
    range_depth: usize,
    /// Known callable names (registry + builtins + per-line functions).
    funcs: &'static [&'static str],
    defines: Vec<(String, List)>,
    /// The `{{block}}` counter is irrelevant; kept for parity docs.
    action_line: usize,
    /// Structural recursion depth (controls + parens) — see
    /// [`MAX_PARSE_DEPTH`].
    depth: usize,
}

/// Result alias.
type PResult<T> = Result<T, ParseError>;

pub fn parse(
    name: &str,
    text: &str,
    funcs: &'static [&'static str],
) -> Result<TreeSet, ParseError> {
    let mut p = Parser {
        name: name.to_string(),
        text,
        lexer: Lexer::new(text),
        peeked: Vec::new(),
        vars: vec!["$".to_string()],
        range_depth: 0,
        funcs,
        defines: Vec::new(),
        action_line: 0,
        depth: 0,
    };
    let root = p.parse_root()?;
    Ok(TreeSet {
        root,
        defines: p.defines,
    })
}

impl<'t> Parser<'t> {
    fn next(&mut self) -> Item {
        self.peeked.pop().unwrap_or_else(|| self.lexer.next_item())
    }

    fn backup(&mut self, item: Item) {
        self.peeked.push(item);
    }

    fn peek(&mut self) -> Item {
        let item = self.next();
        self.backup(item.clone());
        item
    }

    fn next_non_space(&mut self) -> Item {
        loop {
            let item = self.next();
            if item.typ != ItemType::Space {
                return item;
            }
        }
    }

    fn peek_non_space(&mut self) -> Item {
        let item = self.next_non_space();
        self.backup(item.clone());
        item
    }

    fn errorf(&self, line: usize, msg: String) -> ParseError {
        ParseError { line, msg }
    }

    /// Go `unexpected(token, context)`.
    fn unexpected(&self, token: &Item, context: &str) -> ParseError {
        if token.typ == ItemType::Error {
            let extra = if self.action_line != 0 && self.action_line != token.line {
                format!(" in action started at {}:{}", self.name, self.action_line)
            } else {
                String::new()
            };
            return self.errorf(token.line, format!("{}{}", token.val, extra));
        }
        self.errorf(
            token.line,
            format!("unexpected {} in {}", token.display(), context),
        )
    }

    fn expect(&mut self, expected: ItemType, context: &str) -> PResult<Item> {
        let token = self.next_non_space();
        if token.typ != expected {
            return Err(self.unexpected(&token, context));
        }
        Ok(token)
    }

    fn has_function(&self, name: &str) -> bool {
        self.funcs.contains(&name)
    }

    fn pop_vars(&mut self, n: usize) {
        self.vars.truncate(n);
    }

    fn use_var(&self, pos: usize, line: usize, name: &str) -> PResult<Arg> {
        if self.vars.iter().any(|v| v == name) {
            return Ok(Arg::Var(Var {
                pos,
                idents: vec![name.to_string()],
            }));
        }
        Err(self.errorf(line, format!("undefined variable {}", go_q(name))))
    }

    // -- top level ----------------------------------------------------

    fn parse_root(&mut self) -> PResult<List> {
        let mut root = List {
            pos: self.peek().pos,
            nodes: Vec::new(),
        };
        loop {
            let t = self.peek();
            if t.typ == ItemType::Eof {
                break;
            }
            if t.typ == ItemType::LeftDelim {
                let delim = self.next();
                let after = self.next_non_space();
                if after.typ == ItemType::Define {
                    self.parse_definition()?;
                    continue;
                }
                self.backup(after);
                self.backup(delim);
            }
            let n = self.text_or_action()?;
            match n {
                Flow::End(pos) => {
                    return Err(self.errorf(self.line_of(pos), format!("unexpected {}", "{{end}}")));
                }
                Flow::Else(pos) => {
                    return Err(
                        self.errorf(self.line_of(pos), format!("unexpected {}", "{{else}}"))
                    );
                }
                Flow::Node(n) => root.nodes.push(n),
            }
        }
        Ok(root)
    }

    fn line_of(&self, pos: usize) -> usize {
        1 + self.text[..pos.min(self.text.len())].matches('\n').count()
    }

    fn parse_definition(&mut self) -> PResult<()> {
        const CONTEXT: &str = "define clause";
        let token = self.next_non_space();
        let name = self.parse_template_name(&token, CONTEXT)?;
        self.expect(ItemType::RightDelim, CONTEXT)?;
        let (list, end) = self.item_list()?;
        match end {
            Flow::End(_) => {}
            Flow::Else(pos) => {
                return Err(self.errorf(
                    self.line_of(pos),
                    format!("unexpected {} in {}", "{{else}}", CONTEXT),
                ));
            }
            Flow::Node(_) => unreachable!("item_list only yields End/Else terminators"),
        }
        self.add_define(name, list)?;
        Ok(())
    }

    fn add_define(&mut self, name: String, list: List) -> PResult<()> {
        let is_empty = |l: &List| {
            l.nodes.iter().all(|n| match n {
                Node::Text { text, .. } => text.trim_matches([' ', '\t', '\r', '\n']).is_empty(),
                _ => false,
            })
        };
        if let Some((_, existing)) = self.defines.iter_mut().find(|(n, _)| *n == name) {
            if is_empty(existing) {
                *existing = list;
                return Ok(());
            }
            if !is_empty(&list) {
                return Err(self.errorf(
                    self.line_of(list.pos),
                    format!("template: multiple definition of template {}", go_q(&name)),
                ));
            }
            return Ok(());
        }
        self.defines.push((name, list));
        Ok(())
    }

    fn parse_template_name(&mut self, token: &Item, context: &str) -> PResult<String> {
        match token.typ {
            ItemType::String | ItemType::RawString => {
                let s = unquote_string(&token.val).map_err(|e| self.errorf(token.line, e))?;
                String::from_utf8(s).map_err(|_| {
                    self.errorf(token.line, "invalid UTF-8 in template name".to_string())
                })
            }
            _ => Err(self.unexpected(token, context)),
        }
    }

    // -- lists & actions ----------------------------------------------

    fn text_or_action(&mut self) -> PResult<Flow> {
        let token = self.next_non_space();
        match token.typ {
            ItemType::Text => Ok(Flow::Node(Node::Text {
                pos: token.pos,
                text: token.val,
            })),
            ItemType::LeftDelim => {
                self.action_line = token.line;
                let r = self.action();
                self.action_line = 0;
                r
            }
            _ => Err(self.unexpected(&token, "input")),
        }
    }

    /// itemList: gathers nodes until `{{end}}` or `{{else}}`. Every
    /// nested control body passes through here — the structural-depth
    /// guard site for control nesting (see [`MAX_PARSE_DEPTH`]).
    fn item_list(&mut self) -> PResult<(List, Flow)> {
        let first = self.peek_non_space();
        if self.depth >= MAX_PARSE_DEPTH {
            return Err(self.errorf(first.line, "max expression depth exceeded".to_string()));
        }
        self.depth += 1;
        let result = self.item_list_inner(first.pos);
        self.depth -= 1;
        result
    }

    fn item_list_inner(&mut self, pos: usize) -> PResult<(List, Flow)> {
        let mut list = List {
            pos,
            nodes: Vec::new(),
        };
        loop {
            let t = self.peek();
            if t.typ == ItemType::Eof {
                return Err(self.errorf(t.line, "unexpected EOF".to_string()));
            }
            match self.text_or_action()? {
                Flow::Node(n) => list.nodes.push(n),
                flow => return Ok((list, flow)),
            }
        }
    }

    fn action(&mut self) -> PResult<Flow> {
        let token = self.next_non_space();
        match token.typ {
            ItemType::Block => Ok(Flow::Node(self.block_control(&token)?)),
            ItemType::Break => {
                let next = self.next_non_space();
                if next.typ != ItemType::RightDelim {
                    return Err(self.unexpected(&next, "{{break}}"));
                }
                if self.range_depth == 0 {
                    return Err(self.errorf(token.line, "{{break}} outside {{range}}".to_string()));
                }
                Ok(Flow::Node(Node::Break { pos: token.pos }))
            }
            ItemType::Continue => {
                let next = self.next_non_space();
                if next.typ != ItemType::RightDelim {
                    return Err(self.unexpected(&next, "{{continue}}"));
                }
                if self.range_depth == 0 {
                    return Err(
                        self.errorf(token.line, "{{continue}} outside {{range}}".to_string())
                    );
                }
                Ok(Flow::Node(Node::Continue { pos: token.pos }))
            }
            ItemType::Else => self.else_control(&token),
            ItemType::End => {
                let t = self.expect(ItemType::RightDelim, "end")?;
                Ok(Flow::End(t.pos))
            }
            ItemType::If => Ok(Flow::Node(self.if_control(&token)?)),
            ItemType::Range => Ok(Flow::Node(self.range_control(&token)?)),
            ItemType::Template => Ok(Flow::Node(self.template_control(&token)?)),
            ItemType::With => Ok(Flow::Node(self.with_control(&token)?)),
            _ => {
                self.backup(token);
                let peek_pos = self.peek().pos;
                // Do not pop variables; they persist until "end".
                let pipe = self.pipeline("command", ItemType::RightDelim)?;
                Ok(Flow::Node(Node::Action {
                    pos: peek_pos,
                    pipe,
                }))
            }
        }
    }

    fn else_control(&mut self, _token: &Item) -> PResult<Flow> {
        let peek = self.peek_non_space();
        // "{{else if" / "{{else with" leave the if/with token pending.
        if peek.typ == ItemType::If || peek.typ == ItemType::With {
            return Ok(Flow::Else(peek.pos));
        }
        let token = self.expect(ItemType::RightDelim, "else")?;
        Ok(Flow::Else(token.pos))
    }

    fn parse_control(
        &mut self,
        context: &'static str,
    ) -> PResult<(usize, Pipe, List, Option<List>)> {
        let vars_len = self.vars.len();
        let result = self.parse_control_inner(context);
        self.pop_vars(vars_len);
        result
    }

    fn parse_control_inner(
        &mut self,
        context: &'static str,
    ) -> PResult<(usize, Pipe, List, Option<List>)> {
        let pipe = self.pipeline(context, ItemType::RightDelim)?;
        if context == "range" {
            self.range_depth += 1;
        }
        let list_result = self.item_list();
        if context == "range" {
            self.range_depth -= 1;
        }
        let (list, next) = list_result?;
        let mut else_list = None;
        match next {
            Flow::End(_) => {}
            Flow::Else(else_pos) => {
                if context == "if" && self.peek().typ == ItemType::If {
                    let if_token = self.next();
                    let nested = self.if_control(&if_token)?;
                    else_list = Some(List {
                        pos: else_pos,
                        nodes: vec![nested],
                    });
                } else if context == "with" && self.peek().typ == ItemType::With {
                    let with_token = self.next();
                    let nested = self.with_control(&with_token)?;
                    else_list = Some(List {
                        pos: else_pos,
                        nodes: vec![nested],
                    });
                } else {
                    let (elist, next) = self.item_list()?;
                    match next {
                        Flow::End(_) => {}
                        Flow::Else(pos) => {
                            return Err(self.errorf(
                                self.line_of(pos),
                                format!("expected end; found {}", "{{else}}"),
                            ));
                        }
                        Flow::Node(_) => {
                            unreachable!("item_list only yields End/Else terminators")
                        }
                    }
                    else_list = Some(elist);
                }
            }
            Flow::Node(_) => unreachable!("item_list only yields End/Else terminators"),
        }
        Ok((pipe.pos, pipe, list, else_list))
    }

    fn if_control(&mut self, _token: &Item) -> PResult<Node> {
        let (pos, pipe, list, else_list) = self.parse_control("if")?;
        Ok(Node::If {
            pos,
            pipe,
            list,
            else_list,
        })
    }

    fn range_control(&mut self, _token: &Item) -> PResult<Node> {
        let (pos, pipe, list, else_list) = self.parse_control("range")?;
        Ok(Node::Range {
            pos,
            pipe,
            list,
            else_list,
        })
    }

    fn with_control(&mut self, _token: &Item) -> PResult<Node> {
        let (pos, pipe, list, else_list) = self.parse_control("with")?;
        Ok(Node::With {
            pos,
            pipe,
            list,
            else_list,
        })
    }

    fn template_control(&mut self, _token: &Item) -> PResult<Node> {
        const CONTEXT: &str = "template clause";
        let token = self.next_non_space();
        let name = self.parse_template_name(&token, CONTEXT)?;
        let next = self.next_non_space();
        let pipe = if next.typ == ItemType::RightDelim {
            None
        } else {
            self.backup(next);
            Some(self.pipeline(CONTEXT, ItemType::RightDelim)?)
        };
        Ok(Node::Template {
            pos: token.pos,
            name,
            pipe,
        })
    }

    fn block_control(&mut self, _token: &Item) -> PResult<Node> {
        const CONTEXT: &str = "block clause";
        let token = self.next_non_space();
        let name = self.parse_template_name(&token, CONTEXT)?;
        let next = self.next_non_space();
        let pipe = if next.typ == ItemType::RightDelim {
            None
        } else {
            self.backup(next);
            Some(self.pipeline(CONTEXT, ItemType::RightDelim)?)
        };
        // Parse the block body as a define'd sub-tree sharing this lexer.
        let (list, end) = self.item_list()?;
        match end {
            Flow::End(_) => {}
            Flow::Else(pos) => {
                return Err(self.errorf(
                    self.line_of(pos),
                    format!("unexpected {} in {}", "{{else}}", CONTEXT),
                ));
            }
            Flow::Node(_) => unreachable!("item_list only yields End/Else terminators"),
        }
        self.add_define(name.clone(), list)?;
        Ok(Node::Template {
            pos: token.pos,
            name,
            pipe,
        })
    }

    // -- pipelines ----------------------------------------------------

    fn pipeline(&mut self, context: &'static str, end: ItemType) -> PResult<Pipe> {
        let first = self.peek_non_space();
        let mut pipe = Pipe {
            pos: first.pos,
            is_assign: false,
            decl: Vec::new(),
            cmds: Vec::new(),
        };
        // Declarations / assignments.
        loop {
            let v = self.peek_non_space();
            if v.typ != ItemType::Variable {
                break;
            }
            self.next_non_space(); // consume the variable
            let token_after = self.peek();
            let next = self.peek_non_space();
            match next.typ {
                ItemType::Assign | ItemType::Declare => {
                    pipe.is_assign = next.typ == ItemType::Assign;
                    self.next_non_space();
                    pipe.decl.push(Var {
                        pos: v.pos,
                        idents: vec![v.val.clone()],
                    });
                    self.vars.push(v.val);
                    break;
                }
                ItemType::Char if next.val == "," => {
                    self.next_non_space();
                    pipe.decl.push(Var {
                        pos: v.pos,
                        idents: vec![v.val.clone()],
                    });
                    self.vars.push(v.val);
                    if context == "range" && pipe.decl.len() < 2 {
                        match self.peek_non_space().typ {
                            ItemType::Variable | ItemType::RightDelim | ItemType::RightParen => {
                                // Second range variable; loop.
                                continue;
                            }
                            _ => {
                                return Err(self.errorf(
                                    next.line,
                                    "range can only initialize variables".to_string(),
                                ));
                            }
                        }
                    }
                    return Err(
                        self.errorf(next.line, format!("too many declarations in {context}"))
                    );
                }
                _ if token_after.typ == ItemType::Space => {
                    // `$x` used as a value: restore the variable AND the
                    // separating space (Go `backup3`) — the space decides
                    // operand boundaries in `command()`.
                    self.backup(token_after);
                    self.backup(v);
                    break;
                }
                _ => {
                    self.backup(v);
                    break;
                }
            }
        }
        loop {
            let token = self.next_non_space();
            if token.typ == end {
                self.check_pipeline(&pipe, context, token.line)?;
                return Ok(pipe);
            }
            match token.typ {
                ItemType::Bool
                | ItemType::CharConstant
                | ItemType::Complex
                | ItemType::Dot
                | ItemType::Field
                | ItemType::Identifier
                | ItemType::Number
                | ItemType::Nil
                | ItemType::RawString
                | ItemType::String
                | ItemType::Variable
                | ItemType::LeftParen => {
                    self.backup(token);
                    let cmd = self.command()?;
                    pipe.cmds.push(cmd);
                }
                _ => return Err(self.unexpected(&token, context)),
            }
        }
    }

    fn check_pipeline(&self, pipe: &Pipe, context: &str, line: usize) -> PResult<()> {
        if pipe.cmds.is_empty() {
            return Err(self.errorf(line, format!("missing value for {context}")));
        }
        // Only the first command of a pipeline can start with a
        // non-executable operand.
        for (i, c) in pipe.cmds.iter().enumerate().skip(1) {
            if let Some(
                Arg::Bool { .. }
                | Arg::Dot { .. }
                | Arg::Nil { .. }
                | Arg::Number { .. }
                | Arg::Str { .. },
            ) = c.args.first()
            {
                return Err(self.errorf(
                    line,
                    format!("non executable command in pipeline stage {}", i + 1),
                ));
            }
        }
        Ok(())
    }

    fn command(&mut self) -> PResult<Cmd> {
        let mut cmd = Cmd {
            pos: self.peek_non_space().pos,
            args: Vec::new(),
        };
        loop {
            self.peek_non_space();
            if let Some(op) = self.operand()? {
                cmd.args.push(op);
            }
            let token = self.next();
            match token.typ {
                ItemType::Space => continue,
                ItemType::RightDelim | ItemType::RightParen => {
                    self.backup(token);
                }
                ItemType::Pipe => {}
                _ => return Err(self.unexpected(&token, "operand")),
            }
            break;
        }
        if cmd.args.is_empty() {
            let line = self.line_of(cmd.pos);
            return Err(self.errorf(line, "empty command".to_string()));
        }
        Ok(cmd)
    }

    fn operand(&mut self) -> PResult<Option<Arg>> {
        let Some(node) = self.term()? else {
            return Ok(None);
        };
        if self.peek().typ != ItemType::Field {
            return Ok(Some(node));
        }
        let chain_pos = self.peek().pos;
        let mut fields: Vec<String> = Vec::new();
        while self.peek().typ == ItemType::Field {
            let f = self.next();
            // itemField values include the leading '.'.
            fields.push(f.val[1..].to_string());
        }
        Ok(Some(match node {
            Arg::Field { idents, .. } => {
                let mut all = idents;
                all.extend(fields);
                Arg::Field {
                    pos: chain_pos,
                    idents: all,
                }
            }
            Arg::Var(v) => {
                let mut all = v.idents;
                all.extend(fields);
                Arg::Var(Var {
                    pos: chain_pos,
                    idents: all,
                })
            }
            Arg::Bool { .. }
            | Arg::Str { .. }
            | Arg::Number { .. }
            | Arg::Nil { .. }
            | Arg::Dot { .. } => {
                let line = self.line_of(chain_pos);
                return Err(self.errorf(
                    line,
                    format!("unexpected . after term {}", go_q(&node.render_string())),
                ));
            }
            base => Arg::Chain {
                pos: chain_pos,
                base: Box::new(base),
                fields,
            },
        }))
    }

    fn term(&mut self) -> PResult<Option<Arg>> {
        let token = self.next_non_space();
        Ok(Some(match token.typ {
            ItemType::Identifier => {
                if !self.has_function(&token.val) {
                    return Err(self.errorf(
                        token.line,
                        format!("function {} not defined", go_q(&token.val)),
                    ));
                }
                Arg::Ident {
                    pos: token.pos,
                    name: token.val,
                }
            }
            ItemType::Dot => Arg::Dot { pos: token.pos },
            ItemType::Nil => Arg::Nil { pos: token.pos },
            ItemType::Variable => self.use_var(token.pos, token.line, &token.val)?,
            ItemType::Field => Arg::Field {
                pos: token.pos,
                idents: vec![token.val[1..].to_string()],
            },
            ItemType::Bool => Arg::Bool {
                pos: token.pos,
                val: token.val == "true",
            },
            ItemType::CharConstant | ItemType::Complex | ItemType::Number => {
                let num = parse_number(&token.val, token.typ)
                    .map_err(|msg| self.errorf(token.line, msg))?;
                Arg::Number {
                    pos: token.pos,
                    text: token.val,
                    num,
                }
            }
            ItemType::LeftParen => {
                // Go's own guard site (`parse.go` itemLeftParen,
                // maxStackDepth) — same error text, our measured cap.
                if self.depth >= MAX_PARSE_DEPTH {
                    return Err(
                        self.errorf(token.line, "max expression depth exceeded".to_string())
                    );
                }
                self.depth += 1;
                let result = self.pipeline("parenthesized pipeline", ItemType::RightParen);
                self.depth -= 1;
                Arg::Pipe {
                    pos: token.pos,
                    pipe: result?,
                }
            }
            ItemType::String | ItemType::RawString => {
                let val = unquote_string(&token.val).map_err(|e| self.errorf(token.line, e))?;
                Arg::Str {
                    pos: token.pos,
                    text: token.val,
                    val,
                }
            }
            ItemType::Error => return Err(self.unexpected(&token, "command")),
            _ => {
                self.backup(token);
                return Ok(None);
            }
        }))
    }
}

/// The flow of `text_or_action`: a real node, or an `{{end}}`/`{{else}}`
/// terminator (Go models these as node types; a dedicated enum is
/// clearer in Rust).
enum Flow {
    Node(Node),
    End(usize),
    Else(usize),
}

fn go_q(s: &str) -> String {
    super::gofmt::quote_bytes(s.as_bytes(), '"')
}

// ---------------------------------------------------------------------
// Number & string constants
// ---------------------------------------------------------------------

/// Go `parse.NumberNode` + `exec.go idealConstant`: classify and value
/// a number literal exactly as the reference does — try `ParseUint`/
/// `ParseInt` (base 0), then `ParseFloat`, with `idealConstant`'s "looks
/// like a float" (`.eEpP`, non-hex, non-rune) gate deciding int vs
/// float, and the huge-integer `integer overflow: %q` rejection.
fn parse_number(text: &str, typ: ItemType) -> Result<Num, String> {
    if typ == ItemType::CharConstant {
        // idealConstant returns plain `int` for rune constants.
        let rune =
            unquote_char(text).ok_or_else(|| format!("malformed character constant: {text}"))?;
        return Ok(Num::Int(rune as i64));
    }
    if typ == ItemType::Complex || text.ends_with('i') {
        let (re, im) = parse_go_complex(text)
            .ok_or_else(|| format!("illegal number syntax: {}", go_q(text)))?;
        return Ok(Num::Complex(re, im));
    }
    let cleaned = text.replace('_', "");
    let (neg, body) = match cleaned.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => match cleaned.strip_prefix('+') {
            Some(rest) => (false, rest),
            None => (false, cleaned.as_str()),
        },
    };
    let is_floatish =
        text.contains(['.', 'e', 'E']) || (is_hex_text(text) && text.contains(['p', 'P']));
    if let Some(mag) = parse_go_int(body)
        && (!is_floatish || is_hex_text(text))
    {
        {
            if neg {
                let v = -(mag as i128);
                if let Ok(i) = i64::try_from(v) {
                    return Ok(Num::Int(i));
                }
            } else {
                if let Ok(i) = i64::try_from(mag) {
                    return Ok(Num::Int(i));
                }
                return Ok(Num::Uint(mag));
            }
        }
    }
    // Float syntax (including hex floats, which Rust's parser rejects).
    let parsed = if is_hex_text(body) {
        parse_hex_float(body)
    } else {
        body.parse::<f64>().ok().filter(|f| f.is_finite())
    };
    match parsed {
        Some(f) => {
            if !text.contains(['.', 'e', 'E', 'p', 'P']) {
                // An integer too large for uint64: Go rejects it.
                return Err(format!("integer overflow: {}", go_q(text)));
            }
            Ok(Num::Float(if neg { -f } else { f }))
        }
        None => Err(format!("illegal number syntax: {}", go_q(text))),
    }
}

fn is_hex_text(s: &str) -> bool {
    let s = s.trim_start_matches(['+', '-']);
    s.starts_with("0x") || s.starts_with("0X")
}

/// `strconv.ParseUint(s, 0, 64)`-style magnitude parse (base prefixes,
/// underscore-free input, no sign). `None` = not a valid integer form.
fn parse_go_int(body: &str) -> Option<u64> {
    if body.is_empty() {
        return None;
    }
    let (base, digits) =
        if let Some(rest) = body.strip_prefix("0x").or_else(|| body.strip_prefix("0X")) {
            (16, rest)
        } else if let Some(rest) = body.strip_prefix("0o").or_else(|| body.strip_prefix("0O")) {
            (8, rest)
        } else if let Some(rest) = body.strip_prefix("0b").or_else(|| body.strip_prefix("0B")) {
            (2, rest)
        } else if body.len() > 1 && body.starts_with('0') {
            (8, &body[1..])
        } else {
            (10, body)
        };
    if digits.is_empty() {
        return None;
    }
    u64::from_str_radix(digits, base).ok()
}

/// Go hex-float syntax (`0x1.8p-2`): mantissa hex digits with optional
/// point, mandatory `p` exponent.
fn parse_hex_float(body: &str) -> Option<f64> {
    let rest = body
        .strip_prefix("0x")
        .or_else(|| body.strip_prefix("0X"))?;
    let (mant, exp) = rest
        .split_once(['p', 'P'])
        .map(|(m, e)| (m, Some(e)))
        .unwrap_or((rest, None));
    let exp: i32 = exp?.parse().ok()?;
    let (int_part, frac_part) = mant.split_once('.').unwrap_or((mant, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    let mut value: f64 = 0.0;
    for c in int_part.chars() {
        value = value * 16.0 + c.to_digit(16)? as f64;
    }
    let mut scale = 1.0 / 16.0;
    for c in frac_part.chars() {
        value += c.to_digit(16)? as f64 * scale;
        scale /= 16.0;
    }
    Some(value * (exp as f64).exp2())
}

/// Parses Go complex-literal text (`2i`, `1+2i`, `1e2-3.5i`).
fn parse_go_complex(text: &str) -> Option<(f64, f64)> {
    let body = text.strip_suffix('i')?;
    // Find the split point: a top-level +/- that is not an exponent sign
    // and not the leading sign.
    let bytes = body.as_bytes();
    let mut split = None;
    for i in (1..bytes.len()).rev() {
        let c = bytes[i];
        if (c == b'+' || c == b'-') && !matches!(bytes[i - 1], b'e' | b'E' | b'p' | b'P') {
            split = Some(i);
            break;
        }
    }
    match split {
        Some(i) => {
            let re: f64 = body[..i].parse().ok()?;
            let im_text = &body[i..];
            let im: f64 = if im_text == "+" {
                1.0
            } else if im_text == "-" {
                -1.0
            } else {
                im_text.parse().ok()?
            };
            Some((re, im))
        }
        None => Some((0.0, body.parse().ok()?)),
    }
}

/// Go `strconv.Unquote` for template string literals (interpreted `"…"`
/// and raw `` `…` `` forms). Returns raw bytes (escapes like `\x80` can
/// produce non-UTF-8).
pub fn unquote_string(quoted: &str) -> Result<Vec<u8>, String> {
    let err = || "unterminated quoted string".to_string();
    if quoted.len() >= 2 && quoted.starts_with('`') && quoted.ends_with('`') {
        let inner = &quoted[1..quoted.len() - 1];
        if inner.contains('`') {
            return Err(err());
        }
        // Raw strings drop carriage returns.
        return Ok(inner.bytes().filter(|&b| b != b'\r').collect());
    }
    if quoted.len() < 2 || !quoted.starts_with('"') || !quoted.ends_with('"') {
        return Err(err());
    }
    let mut out = Vec::with_capacity(quoted.len());
    let mut chars = quoted[1..quoted.len() - 1].chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            if c == '"' {
                return Err(err());
            }
            let mut buf = [0u8; 4];
            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            continue;
        }
        let esc = chars.next().ok_or_else(err)?;
        match esc {
            'a' => out.push(0x07),
            'b' => out.push(0x08),
            'f' => out.push(0x0C),
            'n' => out.push(b'\n'),
            'r' => out.push(b'\r'),
            't' => out.push(b'\t'),
            'v' => out.push(0x0B),
            '\\' => out.push(b'\\'),
            '"' => out.push(b'"'),
            '\'' => return Err("invalid syntax".to_string()),
            'x' => {
                let hi = chars.next().and_then(|c| c.to_digit(16)).ok_or_else(err)?;
                let lo = chars.next().and_then(|c| c.to_digit(16)).ok_or_else(err)?;
                out.push((hi * 16 + lo) as u8);
            }
            'u' | 'U' => {
                let n = if esc == 'u' { 4 } else { 8 };
                let mut v: u32 = 0;
                for _ in 0..n {
                    let d = chars.next().and_then(|c| c.to_digit(16)).ok_or_else(err)?;
                    v = v * 16 + d;
                }
                let ch = char::from_u32(v).ok_or_else(err)?;
                let mut buf = [0u8; 4];
                out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
            }
            '0'..='7' => {
                let mut v = esc.to_digit(8).ok_or_else(err)?;
                for _ in 0..2 {
                    let d = chars.next().and_then(|c| c.to_digit(8)).ok_or_else(err)?;
                    v = v * 8 + d;
                }
                if v > 255 {
                    return Err(err());
                }
                out.push(v as u8);
            }
            _ => return Err(err()),
        }
    }
    Ok(out)
}

/// Go `strconv.UnquoteChar` for `'x'` constants.
fn unquote_char(quoted: &str) -> Option<char> {
    if quoted.len() < 2 || !quoted.starts_with('\'') || !quoted.ends_with('\'') {
        return None;
    }
    let inner = &quoted[1..quoted.len() - 1];
    let mut chars = inner.chars();
    let c = chars.next()?;
    let result = if c == '\\' {
        let esc = chars.next()?;
        match esc {
            'a' => '\u{7}',
            'b' => '\u{8}',
            'f' => '\u{C}',
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            'v' => '\u{B}',
            '\\' => '\\',
            '\'' => '\'',
            'x' | 'u' | 'U' => {
                let n = match esc {
                    'x' => 2,
                    'u' => 4,
                    _ => 8,
                };
                let mut v: u32 = 0;
                for _ in 0..n {
                    v = v * 16 + chars.next()?.to_digit(16)?;
                }
                char::from_u32(v)?
            }
            '0'..='7' => {
                let mut v = esc.to_digit(8)?;
                for _ in 0..2 {
                    v = v * 8 + chars.next()?.to_digit(8)?;
                }
                char::from_u32(v)?
            }
            _ => return None,
        }
    } else {
        c
    };
    if chars.next().is_some() {
        return None;
    }
    Some(result)
}
