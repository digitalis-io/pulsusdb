//! The template execution walk (issue #230): a faithful port of
//! `text/template/exec.go` — action/if/with/range/template walking,
//! variable scopes, `missingkey=zero`, pipeline `final`-argument
//! threading, argument coercion (`evalArg`/`validateType`), method
//! dispatch (method → map key → error, in Go's order — plan v2 Δ1a),
//! the `and`/`or` short-circuit, and the builtin function set.
//!
//! Execution errors reproduce Go's
//! `template: <name>:<line>:<col>: executing "<tree>" at <node>: <msg>`
//! shape with byte-offset columns (`parse.Tree.ErrorContext`). Under
//! the owner's error-wording ruling the *message* text is best-effort
//! (in practice ported verbatim); the accept/reject and error-class
//! surfaces are the contract.

use std::borrow::Cow;
use std::collections::HashMap;
use std::rc::Rc;

use super::funcs::{self, FuncCtx, ParamTy};
use super::gofmt::{self, PrintEnv};
use super::methods;
use super::parse::{Arg, Cmd, List, Node, Num, Pipe, Var};
use super::timefns::TemplateEnv;
use super::value::{IntKind, UintKind, Value};

/// Go caps template-invocation depth at 100000 relying on growable
/// goroutine stacks; a fixed Rust stack cannot honour that safely, so
/// PulsusDB pins the wasm-tier cap (Go uses 1000 there for the same
/// reason). Only runaway recursive `define`s can reach it; ledgered.
const MAX_EXEC_DEPTH: usize = 1000;

/// Everything one render needs (plan v1 §5's `render` contract).
pub struct EvalInput<'c, 'a> {
    pub text: &'c str,
    pub parse_name: &'c str,
    pub root: &'c List,
    pub defines: &'c [(String, List)],
    pub labels: &'c [(Cow<'a, str>, Cow<'a, str>)],
    /// The #238 out-of-band pair, ALREADY gate-resolved by the caller
    /// (None = slot empty or gate closed).
    pub err: Option<&'c str>,
    pub err_details: Option<&'c str>,
    pub line: &'c str,
    pub ts_ns: i64,
    pub env: &'c TemplateEnv,
    pub regex_cache: &'c HashMap<String, regex::Regex>,
    /// The per-render output-byte ledger (issue #230 follow-up) —
    /// charged before every caller-multiplied allocation.
    pub budget: &'c super::RenderBudget,
}

/// The `Display` string is what becomes `__error_details__` — except
/// when `budget_breach` is set: a render-budget breach aborts the whole
/// QUERY (the bounded 422), never a per-line tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecError {
    pub msg: String,
    pub budget_breach: bool,
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

pub fn render(input: &EvalInput<'_, '_>, out: &mut Vec<u8>) -> Result<(), ExecError> {
    let mut st = State {
        input,
        out,
        vars: vec![("$", Value::LabelMap, false)],
        tmpl_name: input.parse_name,
        at_pos: 0,
        at_ref: AtRef::None,
        depth: 0,
    };
    let dot = Dot {
        v: Value::LabelMap,
        iface: false,
    };
    st.walk_list(&dot, input.root)?;
    if input.budget.breached() {
        return Err(ExecError {
            msg: format!(
                "template output exceeded the {}-byte render budget",
                super::MAX_TEMPLATE_RENDER_BYTES
            ),
            budget_breach: true,
        });
    }
    Ok(())
}

/// The dot plus Go's interface-wrapping provenance: an element pulled
/// out of a `[]any`/`map[string]any` container stays interface-KINDED
/// until a pipeline unwraps it (`evalPipeline`'s `value.Elem()`), which
/// changes two observable things — a nil receiver in a field chain is
/// `nil pointer evaluating interface {}.x` (wrapped) vs a silent zero
/// (unwrapped), and the `can't evaluate field` type name reads
/// `interface {}`.
#[derive(Clone)]
struct Dot<'a> {
    v: Value<'a>,
    iface: bool,
}

struct State<'s, 'c, 'a> {
    input: &'s EvalInput<'c, 'a>,
    out: &'s mut Vec<u8>,
    /// (name, value, interface-wrapped) — see [`Dot`].
    vars: Vec<(&'c str, Value<'c>, bool)>,
    /// The executing tree's name (`executing %q`).
    tmpl_name: &'c str,
    at_pos: usize,
    at_ref: AtRef<'c>,
    depth: usize,
}

/// The node behind `s.at(...)` — rendered lazily, only when an error is
/// actually constructed, so the happy path never allocates for error
/// context (the per-row allocation gates depend on this).
#[derive(Clone, Copy)]
enum AtRef<'c> {
    None,
    Arg(&'c Arg),
    Pipe(&'c Pipe),
    Cmd(&'c Cmd),
    Template {
        name: &'c str,
        pipe: Option<&'c Pipe>,
    },
}

impl AtRef<'_> {
    fn render(&self) -> String {
        match self {
            AtRef::None => String::new(),
            AtRef::Arg(a) => a.render_string(),
            AtRef::Pipe(p) => p.render_string(),
            AtRef::Cmd(c) => c.render_string(),
            AtRef::Template { name, pipe } => template_node_string(name, *pipe),
        }
    }
}

/// Control flow out of a walk step.
enum Walk {
    Ok,
    Break,
    Continue,
}

type WResult = Result<Walk, ExecError>;
type VResult<'c> = Result<Value<'c>, ExecError>;

impl<'s, 'c, 'a> State<'s, 'c, 'a> {
    // -- error machinery ------------------------------------------------

    fn at(&mut self, pos: usize, node: AtRef<'c>) {
        self.at_pos = pos;
        self.at_ref = node;
    }

    fn at_arg(&mut self, arg: &'c Arg) {
        self.at(arg.pos(), AtRef::Arg(arg));
    }

    /// Go `state.errorf` + `Tree.ErrorContext`.
    fn errorf(&self, msg: String) -> ExecError {
        let pos = self.at_pos.min(self.input.text.len());
        let before = &self.input.text[..pos];
        let line = 1 + before.matches('\n').count();
        let col = match before.rfind('\n') {
            Some(nl) => pos - nl - 1,
            None => pos,
        };
        ExecError {
            msg: format!(
                "template: {}:{}:{}: executing {} at <{}>: {}",
                self.input.parse_name,
                line,
                col,
                gofmt::quote_bytes(self.tmpl_name.as_bytes(), '"'),
                self.at_ref.render(),
                msg
            ),
            budget_breach: false,
        }
    }

    /// The query-aborting budget-breach error (never surfaces as a
    /// per-line `__error_details__`).
    fn budget_error(&self) -> ExecError {
        ExecError {
            msg: format!(
                "template output exceeded the {}-byte render budget",
                super::MAX_TEMPLATE_RENDER_BYTES
            ),
            budget_breach: true,
        }
    }

    // -- variables -------------------------------------------------------

    fn push_var(&mut self, name: &'c str, value: Value<'c>, iface: bool) {
        self.vars.push((name, value, iface));
    }

    fn mark(&self) -> usize {
        self.vars.len()
    }

    fn pop_vars(&mut self, mark: usize) {
        self.vars.truncate(mark);
    }

    fn set_var(&mut self, name: &str, value: Value<'c>, iface: bool) -> Result<(), ExecError> {
        for (n, v, w) in self.vars.iter_mut().rev() {
            if *n == name {
                *v = value;
                *w = iface;
                return Ok(());
            }
        }
        Err(self.errorf(format!(
            "undefined variable: {}",
            gofmt::quote_bytes(name.as_bytes(), '"')
        )))
    }

    fn set_top_var(&mut self, n: usize, value: Value<'c>, iface: bool) {
        let len = self.vars.len();
        self.vars[len - n].1 = value;
        self.vars[len - n].2 = iface;
    }

    fn var_value(&self, name: &str) -> Result<(Value<'c>, bool), ExecError> {
        for (n, v, w) in self.vars.iter().rev() {
            if *n == name {
                return Ok((v.clone(), *w));
            }
        }
        Err(self.errorf(format!(
            "undefined variable: {}",
            gofmt::quote_bytes(name.as_bytes(), '"')
        )))
    }

    // -- walking -----------------------------------------------------------

    fn walk_list(&mut self, dot: &Dot<'c>, list: &'c List) -> Result<(), ExecError> {
        for node in &list.nodes {
            // Break/Continue cannot escape here: the parser rejects
            // them outside {{range}}, and walk_range consumes them.
            let _flow = self.walk(dot, node)?;
        }
        Ok(())
    }

    /// Like `walk_list` but lets break/continue escape to the range.
    fn walk_list_flow(&mut self, dot: &Dot<'c>, list: &'c List) -> WResult {
        for node in &list.nodes {
            match self.walk(dot, node)? {
                Walk::Ok => {}
                flow => return Ok(flow),
            }
        }
        Ok(Walk::Ok)
    }

    fn walk(&mut self, dot: &Dot<'c>, node: &'c Node) -> WResult {
        match node {
            Node::Text { text, .. } => {
                self.out.extend_from_slice(text.as_bytes());
                Ok(Walk::Ok)
            }
            Node::Action { pos, pipe } => {
                self.at(*pos, AtRef::Pipe(pipe));
                let val = self.eval_pipeline(dot, pipe)?;
                if pipe.decl.is_empty() {
                    self.at(*pos, AtRef::Pipe(pipe));
                    self.print_value_go(&val)?;
                }
                Ok(Walk::Ok)
            }
            Node::If {
                pos,
                pipe,
                list,
                else_list,
            } => self.walk_if_or_with(true, *pos, dot, pipe, list, else_list.as_ref()),
            Node::With {
                pos,
                pipe,
                list,
                else_list,
            } => self.walk_if_or_with(false, *pos, dot, pipe, list, else_list.as_ref()),
            Node::Range {
                pos,
                pipe,
                list,
                else_list,
            } => self.walk_range(*pos, dot, pipe, list, else_list.as_ref()),
            Node::Template { pos, name, pipe } => {
                self.walk_template(*pos, dot, name, pipe.as_ref())?;
                Ok(Walk::Ok)
            }
            Node::Break { .. } => Ok(Walk::Break),
            Node::Continue { .. } => Ok(Walk::Continue),
        }
    }

    fn walk_if_or_with(
        &mut self,
        is_if: bool,
        pos: usize,
        dot: &Dot<'c>,
        pipe: &'c Pipe,
        list: &'c List,
        else_list: Option<&'c List>,
    ) -> WResult {
        let mark = self.mark();
        self.at(pos, AtRef::Pipe(pipe));
        let val = self.eval_pipeline(dot, pipe)?;
        let (truth, ok) = self.value_truth(&val);
        if !ok {
            self.at(pos, AtRef::Pipe(pipe));
            let rendered = self.render_value_for_error(&val);
            let e = self.errorf(format!("if/with can't use {rendered}"));
            self.pop_vars(mark);
            return Err(e);
        }
        let result = if truth {
            if is_if {
                self.walk_list_flow(dot, list)
            } else {
                // `with` re-dots to the (pipeline-unwrapped) value.
                let new_dot = Dot {
                    v: val.clone(),
                    iface: false,
                };
                self.walk_list_flow(&new_dot, list)
            }
        } else if let Some(else_list) = else_list {
            self.walk_list_flow(dot, else_list)
        } else {
            Ok(Walk::Ok)
        };
        self.pop_vars(mark);
        result
    }

    fn value_truth(&self, val: &Value<'c>) -> (bool, bool) {
        match val {
            Value::LabelMap => (true, true),
            v => v.is_true(),
        }
    }

    /// `%v` of a value for the `if/with can't use %v` message.
    fn render_value_for_error(&mut self, val: &Value<'c>) -> String {
        // Only the invalid value can fail isTrue; Go prints it as
        // `<invalid reflect.Value>` under %v at depth 0... but through
        // errorf's %v of a reflect.Value the rendering is the same.
        let mut buf = Vec::new();
        let env = InputPrintEnv { input: self.input };
        match val {
            Value::Nil => return "<invalid Value>".to_string(),
            v => gofmt::sprint(&mut buf, std::slice::from_ref(v), &env),
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    fn walk_range(
        &mut self,
        pos: usize,
        dot: &Dot<'c>,
        pipe: &'c Pipe,
        list: &'c List,
        else_list: Option<&'c List>,
    ) -> WResult {
        let outer_mark = self.mark();
        self.at(pos, AtRef::Pipe(pipe));
        let val = self.eval_pipeline(dot, pipe)?;
        let mark = self.mark();

        let mut ran = false;
        let mut result = Walk::Ok;

        macro_rules! one_iteration {
            ($index:expr, $elem:expr, $iface:expr) => {{
                ran = true;
                let index: Option<Value<'c>> = $index;
                let elem: Value<'c> = $elem;
                let iface: bool = $iface;
                if !pipe.decl.is_empty() {
                    if pipe.is_assign {
                        if pipe.decl.len() > 1 {
                            self.set_var(
                                &pipe.decl[0].idents[0],
                                index.clone().unwrap_or(Value::Nil),
                                false,
                            )?;
                        } else {
                            self.set_var(&pipe.decl[0].idents[0], elem.clone(), iface)?;
                        }
                    } else {
                        self.set_top_var(1, elem.clone(), iface);
                    }
                }
                if pipe.decl.len() > 1 {
                    if pipe.is_assign {
                        self.set_var(&pipe.decl[1].idents[0], elem.clone(), iface)?;
                    } else {
                        self.set_top_var(2, index.clone().unwrap_or(Value::Nil), false);
                    }
                }
                let elem_dot = Dot { v: elem, iface };
                let flow = self.walk_list_flow(&elem_dot, list)?;
                self.pop_vars(mark);
                match flow {
                    Walk::Break => {
                        result = Walk::Ok;
                        self.pop_vars(outer_mark);
                        return Ok(result);
                    }
                    _ => {}
                }
            }};
        }

        match &val {
            Value::Int(n, _) | Value::Duration(n) | Value::Month(n) | Value::Weekday(n) => {
                if pipe.decl.len() > 1 {
                    let rendered = self.render_value_for_error(&val);
                    self.pop_vars(outer_mark);
                    return Err(self.errorf(format!(
                        "can't use {rendered} to iterate over more than one variable"
                    )));
                }
                for v in 0..(*n).max(0) {
                    one_iteration!(None, Value::Int(v, IntKind::Int), false);
                }
            }
            Value::Uint(n, _) => {
                if pipe.decl.len() > 1 {
                    let rendered = self.render_value_for_error(&val);
                    self.pop_vars(outer_mark);
                    return Err(self.errorf(format!(
                        "can't use {rendered} to iterate over more than one variable"
                    )));
                }
                for v in 0..*n {
                    one_iteration!(None, Value::Uint(v, UintKind::Uint64), false);
                }
            }
            Value::List(items) => {
                for (i, item) in items.iter().enumerate() {
                    one_iteration!(Some(Value::int(i as i64)), item.clone(), true);
                }
            }
            Value::Map(entries) => {
                for (k, v) in entries.iter() {
                    one_iteration!(Some(Value::Str(Cow::Owned(k.clone()))), v.clone(), true);
                }
            }
            Value::LabelMap => {
                let pairs = self.label_pairs_sorted();
                for (k, v) in pairs {
                    one_iteration!(
                        Some(Value::Str(Cow::Owned(k))),
                        Value::Str(Cow::Owned(v)),
                        false
                    );
                }
            }
            Value::Nil => {
                self.pop_vars(outer_mark);
                return Err(self.errorf("range can't iterate over <invalid Value>".to_string()));
            }
            other => {
                let rendered = self.render_value_for_error(other);
                self.pop_vars(outer_mark);
                return Err(self.errorf(format!("range can't iterate over {rendered}")));
            }
        }

        if !ran && let Some(else_list) = else_list {
            result = self.walk_list_flow(dot, else_list)?;
        }
        self.pop_vars(outer_mark);
        Ok(result)
    }

    fn walk_template(
        &mut self,
        pos: usize,
        dot: &Dot<'c>,
        name: &'c str,
        pipe: Option<&'c Pipe>,
    ) -> Result<(), ExecError> {
        self.at(pos, AtRef::Template { name, pipe });
        // The tree set contains the defines AND the root template under
        // its own name ("line"/"label") — `{{template "line"}}` recurses
        // into the root (bounded by the depth cap, like the reference).
        let tree = match self
            .input
            .defines
            .iter()
            .rev()
            .find(|(n, _)| n == name)
            .map(|(_, l)| l)
        {
            Some(t) => t,
            None if name == self.input.parse_name => self.input.root,
            None => {
                return Err(self.errorf(format!(
                    "template {} not defined",
                    gofmt::quote_bytes(name.as_bytes(), '"')
                )));
            }
        };
        if self.depth == MAX_EXEC_DEPTH {
            return Err(self.errorf(format!(
                "exceeded maximum template depth ({MAX_EXEC_DEPTH})"
            )));
        }
        let new_dot = Dot {
            v: match pipe {
                Some(pipe) => self.eval_pipeline(dot, pipe)?,
                None => Value::Nil,
            },
            iface: false,
        };
        // New scope: no dynamic scoping.
        let saved_vars = std::mem::replace(&mut self.vars, vec![("$", new_dot.v.clone(), false)]);
        let saved_name = std::mem::replace(&mut self.tmpl_name, name);
        self.depth += 1;
        let result = self.walk_list(&new_dot, tree);
        self.depth -= 1;
        self.tmpl_name = saved_name;
        self.vars = saved_vars;
        result
    }

    // -- pipelines ----------------------------------------------------------

    fn eval_pipeline(&mut self, dot: &Dot<'c>, pipe: &'c Pipe) -> VResult<'c> {
        let mut value: Option<Value<'c>> = None;
        for cmd in &pipe.cmds {
            value = Some(self.eval_command(dot, cmd, value)?);
        }
        let value = value.unwrap_or(Value::Nil);
        for var in &pipe.decl {
            if pipe.is_assign {
                self.set_var(&var.idents[0], value.clone(), false)?;
            } else {
                self.push_var(&var.idents[0], value.clone(), false);
            }
        }
        Ok(value)
    }

    fn eval_command(
        &mut self,
        dot: &Dot<'c>,
        cmd: &'c Cmd,
        final_val: Option<Value<'c>>,
    ) -> VResult<'c> {
        let first = &cmd.args[0];
        match first {
            Arg::Field { pos, idents } => {
                self.at(*pos, AtRef::Arg(first));
                self.eval_field_chain(
                    dot,
                    dot.v.clone(),
                    dot.iface,
                    first,
                    idents,
                    &cmd.args,
                    final_val,
                )
            }
            Arg::Chain { pos, base, fields } => {
                self.at(*pos, AtRef::Arg(first));
                if matches!(**base, Arg::Nil { .. }) {
                    return Err(self.errorf(format!(
                        "indirection through explicit nil in {}",
                        first.render_string()
                    )));
                }
                let receiver = self.eval_arg(dot, None, base)?;
                self.eval_field_chain(dot, receiver, false, first, fields, &cmd.args, final_val)
            }
            Arg::Ident { pos, name } => {
                self.at(*pos, AtRef::Arg(first));
                self.eval_function(dot, name, cmd, final_val)
            }
            Arg::Pipe { pipe, .. } => {
                self.not_a_function(&cmd.args, &final_val, first)?;
                self.eval_pipeline(dot, pipe)
            }
            Arg::Var(v) => {
                self.at(v.pos, AtRef::Arg(first));
                self.eval_variable_node(dot, first, v, &cmd.args, final_val)
            }
            word => {
                self.at_arg(word);
                self.not_a_function(&cmd.args, &final_val, word)?;
                match word {
                    Arg::Bool { val, .. } => Ok(Value::Bool(*val)),
                    Arg::Dot { .. } => Ok(dot.v.clone()),
                    Arg::Nil { .. } => Err(self.errorf("nil is not a command".to_string())),
                    Arg::Number { num, text, .. } => self.ideal_constant(num, text),
                    Arg::Str { val, .. } => Ok(Value::Str(Cow::Owned(val.clone()))),
                    _ => Err(self.errorf(format!(
                        "can't evaluate command {}",
                        gofmt::quote_bytes(word.render_string().as_bytes(), '"')
                    ))),
                }
            }
        }
    }

    fn not_a_function(
        &mut self,
        args: &'c [Arg],
        final_val: &Option<Value<'c>>,
        first: &'c Arg,
    ) -> Result<(), ExecError> {
        if args.len() > 1 || final_val.is_some() {
            return Err(self.errorf(format!(
                "can't give argument to non-function {}",
                first.render_string()
            )));
        }
        Ok(())
    }

    fn eval_variable_node(
        &mut self,
        dot: &Dot<'c>,
        node: &'c Arg,
        var: &'c Var,
        args: &'c [Arg],
        final_val: Option<Value<'c>>,
    ) -> VResult<'c> {
        let (value, iface) = self.var_value(&var.idents[0])?;
        if var.idents.len() == 1 {
            self.not_a_function(args, &final_val, node)?;
            return Ok(value);
        }
        self.eval_field_chain_inner(dot, value, iface, node, &var.idents[1..], args, final_val)
    }

    #[allow(clippy::too_many_arguments)]
    fn eval_field_chain(
        &mut self,
        dot: &Dot<'c>,
        receiver: Value<'c>,
        receiver_iface: bool,
        node: &'c Arg,
        idents: &'c [String],
        args: &'c [Arg],
        final_val: Option<Value<'c>>,
    ) -> VResult<'c> {
        self.eval_field_chain_inner(dot, receiver, receiver_iface, node, idents, args, final_val)
    }

    #[allow(clippy::too_many_arguments)]
    fn eval_field_chain_inner(
        &mut self,
        dot: &Dot<'c>,
        mut receiver: Value<'c>,
        mut receiver_iface: bool,
        node: &'c Arg,
        idents: &'c [String],
        args: &'c [Arg],
        final_val: Option<Value<'c>>,
    ) -> VResult<'c> {
        let n = idents.len();
        for ident in &idents[..n - 1] {
            // A value pulled from a `map[string]any` stays interface-
            // kinded for the NEXT step (no pipeline unwrap inside a
            // chain).
            let from_any = matches!(receiver, Value::Map(_));
            receiver = self.eval_field(dot, ident, node, &[], None, receiver, receiver_iface)?;
            receiver_iface = from_any;
        }
        let iface = receiver_iface;
        self.eval_field(dot, &idents[n - 1], node, args, final_val, receiver, iface)
    }

    /// Go `evalField`: method dispatch FIRST, then map key, then error.
    #[allow(clippy::too_many_arguments)]
    fn eval_field(
        &mut self,
        dot: &Dot<'c>,
        field_name: &str,
        node: &'c Arg,
        args: &'c [Arg],
        final_val: Option<Value<'c>>,
        receiver: Value<'c>,
        receiver_iface: bool,
    ) -> VResult<'c> {
        if matches!(receiver, Value::Nil) {
            if receiver_iface {
                // A nil INTERFACE receiver (a json null / missing-key
                // zero inside a chain): `MethodByName would panic`.
                return Err(self.errorf(format!(
                    "nil pointer evaluating interface {{}}.{field_name}"
                )));
            }
            // The INVALID value (post-pipeline-unwrap nil): silent zero.
            return Ok(Value::Nil);
        }
        // Method?
        if let Some(sig) = methods::method(&receiver, field_name) {
            return self.eval_call(
                dot,
                Callee::Method(receiver, sig),
                node,
                None,
                field_name,
                args,
                final_val,
            );
        }
        let has_args = args.len() > 1 || final_val.is_some();
        match &receiver {
            Value::LabelMap => {
                if has_args {
                    return Err(
                        self.errorf(format!("{field_name} is not a method but has arguments"))
                    );
                }
                // map[string]string under missingkey=zero.
                Ok(self
                    .label_get(field_name)
                    .map(|v| Value::Str(Cow::Borrowed(v)))
                    .unwrap_or(Value::Str(Cow::Borrowed(b""))))
            }
            Value::Map(entries) => {
                if has_args {
                    return Err(
                        self.errorf(format!("{field_name} is not a method but has arguments"))
                    );
                }
                // map[string]any under missingkey=zero: zero of any is
                // the nil interface → invalid after unwrap.
                Ok(entries
                    .iter()
                    .find(|(k, _)| k == field_name.as_bytes())
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Value::Nil))
            }
            other => {
                let type_name = if receiver_iface {
                    "interface {}"
                } else {
                    other.type_name()
                };
                Err(self.errorf(format!(
                    "can't evaluate field {field_name} in type {type_name}"
                )))
            }
        }
    }

    // -- function calls -------------------------------------------------------

    fn eval_function(
        &mut self,
        dot: &Dot<'c>,
        name: &str,
        cmd: &'c Cmd,
        final_val: Option<Value<'c>>,
    ) -> VResult<'c> {
        // and/or: short-circuit specials.
        if name == "and" || name == "or" {
            return self.eval_and_or(dot, name, cmd, final_val);
        }
        if name == "call" {
            return self.eval_call_builtin(dot, cmd, final_val);
        }
        if let Some(builtin) = builtin_sig(name) {
            return self.eval_call(
                dot,
                Callee::Builtin(builtin),
                &cmd.args[0],
                Some(cmd),
                name,
                &cmd.args,
                final_val,
            );
        }
        if let Some(def) = funcs::lookup(name) {
            return self.eval_call(
                dot,
                Callee::Registry(def),
                &cmd.args[0],
                Some(cmd),
                name,
                &cmd.args,
                final_val,
            );
        }
        // Parse-time hasFunction makes this unreachable.
        Err(self.errorf(format!(
            "{} is not a defined function",
            gofmt::quote_bytes(name.as_bytes(), '"')
        )))
    }

    /// A bare function name used as an argument (`evalArg` →
    /// `evalFunction` with only the identifier): zero args, no final.
    fn eval_ident_as_arg(&mut self, dot: &Dot<'c>, name: &str, node: &'c Arg) -> VResult<'c> {
        if name == "and" || name == "or" {
            return Err(self.errorf(format!(
                "wrong number of args for {name}: want at least 1 got 0"
            )));
        }
        if name == "call" {
            return Err(
                self.errorf("wrong number of args for call: want at least 1 got 0".to_string())
            );
        }
        if let Some(builtin) = builtin_sig(name) {
            return self.eval_call(dot, Callee::Builtin(builtin), node, None, name, &[], None);
        }
        if let Some(def) = funcs::lookup(name) {
            return self.eval_call(dot, Callee::Registry(def), node, None, name, &[], None);
        }
        Err(self.errorf(format!(
            "{} is not a defined function",
            gofmt::quote_bytes(name.as_bytes(), '"')
        )))
    }

    /// The `call` builtin (`evalCall`'s special case + `funcs.go call`):
    /// no function VALUES exist in this model, so every call errors with
    /// Go's exact text — `call of nil` or `non-function <callee> of type
    /// <T>` where `<callee>` is the first argument NODE's rendering (or
    /// the final pipeline value's `reflect.Value.String()`).
    fn eval_call_builtin(
        &mut self,
        dot: &Dot<'c>,
        cmd: &'c Cmd,
        final_val: Option<Value<'c>>,
    ) -> VResult<'c> {
        let arg_nodes = &cmd.args[1..];
        let num_fixed = arg_nodes.len() + usize::from(final_val.is_some());
        if num_fixed < 1 {
            return Err(
                self.errorf("wrong number of args for call: want at least 1 got 0".to_string())
            );
        }
        let mut argv: Vec<Value<'c>> = Vec::with_capacity(num_fixed);
        for arg in arg_nodes {
            let v = self.eval_arg(dot, Some(ParamTy::Any), arg)?;
            argv.push(v);
        }
        let callee_name: String = if let Some(first) = arg_nodes.first() {
            first.render_string()
        } else {
            // reflect.Value.String() of the final value: the text for a
            // string, `<T Value>` otherwise.
            match final_val.as_ref() {
                Some(Value::Str(s)) => String::from_utf8_lossy(s).into_owned(),
                Some(other) => format!("<{} Value>", other.type_name()),
                None => String::new(),
            }
        };
        if let Some(final_v) = final_val {
            argv.push(final_v);
        }
        let msg = match &argv[0] {
            Value::Nil => "call of nil".to_string(),
            other => format!("non-function {} of type {}", callee_name, other.type_name()),
        };
        self.at_cmd_or_arg(&cmd.args[0], Some(cmd));
        Err(self.errorf(format!("error calling call: {msg}")))
    }

    fn eval_and_or(
        &mut self,
        dot: &Dot<'c>,
        name: &str,
        cmd: &'c Cmd,
        final_val: Option<Value<'c>>,
    ) -> VResult<'c> {
        let is_or = name == "or";
        let args = &cmd.args[1..];
        if args.is_empty() && final_val.is_none() {
            // Go: and/or's first parameter is required.
            return Err(self.errorf(format!(
                "wrong number of args for {name}: want at least 1 got 0"
            )));
        }
        let mut v = Value::Bool(!is_or);
        for arg in args {
            self.at_arg(arg);
            v = self.eval_arg(dot, None, arg)?;
            let (truth, _) = self.value_truth(&v);
            if truth == is_or {
                return Ok(v);
            }
        }
        if let Some(final_v) = final_val {
            v = final_v;
        }
        Ok(v)
    }

    #[allow(clippy::too_many_arguments)]
    fn eval_call(
        &mut self,
        dot: &Dot<'c>,
        callee: Callee<'c>,
        node: &'c Arg,
        cmd: Option<&'c Cmd>,
        name: &str,
        args: &'c [Arg],
        final_val: Option<Value<'c>>,
    ) -> VResult<'c> {
        // args[0] is the function/field node itself.
        let arg_nodes: &'c [Arg] = if args.is_empty() { &[] } else { &args[1..] };
        let (params, variadic): (&[ParamTy], Option<ParamTy>) = match &callee {
            Callee::Registry(def) => (def.params, def.variadic),
            Callee::Builtin(sig) => (sig.params, sig.variadic),
            Callee::Method(_, sig) => (sig.params, sig.variadic),
        };
        let num_in = params.len();
        let num_fixed = arg_nodes.len() + usize::from(final_val.is_some());
        if variadic.is_some() {
            if num_fixed < num_in {
                // Go reports len(args) — the final pipe value is NOT
                // counted in `got` here (exec.go:785).
                let got = arg_nodes.len();
                return Err(self.errorf(format!(
                    "wrong number of args for {name}: want at least {num_in} got {got}",
                )));
            }
        } else if num_fixed != num_in {
            return Err(self.errorf(format!(
                "wrong number of args for {name}: want {num_in} got {num_fixed}",
            )));
        }
        // `goodFunc` runs after the arity check (exec.go:790).
        if let Callee::Method(_, sig) = &callee
            && let methods::MethodImpl::Reject(msg) = sig.call
        {
            return Err(self.errorf(msg.to_string()));
        }
        // Evaluate arguments with the expected types.
        let mut argv: Vec<Value<'c>> = Vec::with_capacity(num_fixed);
        for (i, arg) in arg_nodes.iter().enumerate() {
            let ty = if i < num_in {
                Some(params[i])
            } else {
                variadic
            };
            let v = self.eval_arg(dot, ty, arg)?;
            let v = self.coerce_arg_value(ty, v, arg)?;
            argv.push(v);
        }
        if let Some(final_v) = final_val {
            let idx = argv.len();
            let ty = if idx < num_in {
                Some(params[idx])
            } else {
                variadic
            };
            let v = self.validate_type(final_v, ty)?;
            argv.push(v);
        }
        // Call.
        let result = match callee {
            Callee::Registry(def) => {
                let env = InputPrintEnv { input: self.input };
                let ctx = FuncCtx {
                    print_env: &env,
                    line: self.input.line.as_bytes(),
                    ts_ns: self.input.ts_ns,
                    regex_cache: self.input.regex_cache,
                    budget: self.input.budget,
                    _marker: std::marker::PhantomData,
                };
                (def.call)(&ctx, &argv)
            }
            Callee::Builtin(sig) => {
                let f = sig.call;
                f(self, &argv)
            }
            Callee::Method(receiver, sig) => match sig.call {
                methods::MethodImpl::Reject(msg) => return Err(self.errorf(msg.to_string())),
                methods::MethodImpl::Call(f) => {
                    let env = InputPrintEnv { input: self.input };
                    f(&receiver, &argv, env.env())
                }
            },
        };
        // A budget breach inside the call (a charging function OR the
        // print family's padding) aborts the query — it must never be
        // re-classified as a per-line `error calling …`.
        if self.input.budget.breached() {
            return Err(self.budget_error());
        }
        match result {
            Ok(v) => Ok(v),
            Err(msg) => {
                // Go: s.at(node) where node is the surrounding command
                // for functions; methods keep the field node.
                self.at_cmd_or_arg(node, cmd);
                Err(self.errorf(format!("error calling {name}: {msg}")))
            }
        }
    }

    /// The `error calling` position: the whole command for function
    /// calls (Go passes `cmd` to evalCall), the field node for methods.
    fn at_cmd_or_arg(&mut self, node: &'c Arg, cmd: Option<&'c Cmd>) {
        match (node, cmd) {
            (Arg::Ident { .. }, Some(cmd)) => {
                self.at(cmd.args[0].pos(), AtRef::Cmd(cmd));
            }
            _ => self.at_arg(node),
        }
    }

    /// `evalArg`: literal nodes coerce per the expected Go kind with
    /// Go's `expected <kind>; found <node>` errors; value-producing
    /// nodes route through `validateType`.
    fn eval_arg(&mut self, dot: &Dot<'c>, ty: Option<ParamTy>, arg: &'c Arg) -> VResult<'c> {
        match arg {
            Arg::Dot { .. } => {
                self.at_arg(arg);
                Ok(dot.v.clone())
            }
            Arg::Nil { .. } => {
                self.at_arg(arg);
                match ty {
                    None | Some(ParamTy::Any) | Some(ParamTy::Loc) | Some(ParamTy::BytesTy) => {
                        Ok(Value::Nil)
                    }
                    Some(t) => Err(self.errorf(format!("cannot assign nil to {}", t.go_name()))),
                }
            }
            Arg::Field { pos, idents } => {
                self.at(*pos, AtRef::Arg(arg));
                let v =
                    self.eval_field_chain(dot, dot.v.clone(), dot.iface, arg, idents, &[], None)?;
                Ok(v)
            }
            Arg::Var(var) => {
                self.at(var.pos, AtRef::Arg(arg));
                if var.idents.len() == 1 {
                    self.var_value(&var.idents[0]).map(|(v, _)| v)
                } else {
                    let (value, iface) = self.var_value(&var.idents[0])?;
                    self.eval_field_chain_inner(dot, value, iface, arg, &var.idents[1..], &[], None)
                }
            }
            Arg::Chain { base, fields, .. } => {
                self.at_arg(arg);
                if matches!(**base, Arg::Nil { .. }) {
                    return Err(self.errorf(format!(
                        "indirection through explicit nil in {}",
                        arg.render_string()
                    )));
                }
                let receiver = self.eval_arg(dot, None, base)?;
                self.eval_field_chain_inner(dot, receiver, false, arg, fields, &[], None)
            }
            Arg::Pipe { pipe, .. } => {
                self.at_arg(arg);
                self.eval_pipeline(dot, pipe)
            }
            Arg::Ident { pos, name } => {
                self.at(*pos, AtRef::Arg(arg));
                // A function used as an argument: call it with no args
                // (`evalArg` -> `evalFunction` with just the ident).
                self.eval_ident_as_arg(dot, name, arg)
            }
            Arg::Bool { pos, val } => {
                self.at(*pos, AtRef::Arg(arg));
                match ty {
                    None | Some(ParamTy::Any) | Some(ParamTy::Time) => Ok(Value::Bool(*val)),
                    Some(_) => Ok(Value::Bool(*val)), // coerce_arg_value reports
                }
            }
            Arg::Number { pos, num, text } => {
                self.at(*pos, AtRef::Arg(arg));
                let _ = text;
                match ty {
                    Some(ParamTy::Int) => match num {
                        Num::Int(n) => Ok(Value::Int(*n, IntKind::Int)),
                        Num::Uint(u) => Ok(Value::Int(*u as i64, IntKind::Int)),
                        _ => {
                            Err(self
                                .errorf(format!("expected integer; found {}", arg.render_string())))
                        }
                    },
                    Some(ParamTy::Dur) => match num {
                        Num::Int(n) => Ok(Value::Duration(*n)),
                        Num::Uint(u) => Ok(Value::Duration(*u as i64)),
                        _ => {
                            Err(self
                                .errorf(format!("expected integer; found {}", arg.render_string())))
                        }
                    },
                    Some(ParamTy::Float) => {
                        match num {
                            Num::Int(n) => Ok(Value::Float(*n as f64)),
                            Num::Uint(u) => Ok(Value::Float(*u as f64)),
                            Num::Float(f) => Ok(Value::Float(*f)),
                            Num::Complex(..) => Err(self
                                .errorf(format!("expected float; found {}", arg.render_string()))),
                        }
                    }
                    Some(ParamTy::Str) => {
                        Err(self.errorf(format!("expected string; found {}", arg.render_string())))
                    }
                    _ => self.ideal_constant(num, text),
                }
            }
            Arg::Str { pos, val, .. } => {
                self.at(*pos, AtRef::Arg(arg));
                match ty {
                    Some(ParamTy::Int) | Some(ParamTy::Dur) => {
                        Err(self.errorf(format!("expected integer; found {}", arg.render_string())))
                    }
                    Some(ParamTy::Float) => {
                        Err(self.errorf(format!("expected float; found {}", arg.render_string())))
                    }
                    _ => Ok(Value::Str(Cow::Owned(val.clone()))),
                }
            }
        }
    }

    /// `validateType` for values produced by fields/vars/pipes.
    fn coerce_arg_value(&mut self, ty: Option<ParamTy>, v: Value<'c>, arg: &'c Arg) -> VResult<'c> {
        // Literal nodes already coerced in eval_arg.
        if matches!(
            arg,
            Arg::Number { .. } | Arg::Str { .. } | Arg::Bool { .. } | Arg::Nil { .. }
        ) {
            // Bool literal against non-bool typed param:
            if let (Arg::Bool { .. }, Some(t)) = (arg, ty)
                && !matches!(t, ParamTy::Any)
            {
                self.at_arg(arg);
                return Err(self.errorf(format!("expected bool; found {}", arg.render_string())));
            }
            return Ok(v);
        }
        self.at_arg(arg);
        self.validate_type(v, ty)
    }

    fn validate_type(&mut self, v: Value<'c>, ty: Option<ParamTy>) -> VResult<'c> {
        let Some(ty) = ty else { return Ok(v) };
        match ty {
            ParamTy::Any => Ok(v),
            ParamTy::Str => match v {
                Value::Str(_) => Ok(v),
                other => Err(self.errorf(format!(
                    "wrong type for value; expected string; got {}",
                    other.type_name()
                ))),
            },
            ParamTy::Int => match v {
                Value::Int(n, _) => Ok(Value::Int(n, IntKind::Int)),
                Value::Uint(n, _) => Ok(Value::Int(n as i64, IntKind::Int)),
                Value::Duration(n) | Value::Month(n) | Value::Weekday(n) => {
                    Ok(Value::Int(n, IntKind::Int))
                }
                other => Err(self.errorf(format!(
                    "wrong type for value; expected int; got {}",
                    other.type_name()
                ))),
            },
            ParamTy::Float => match v {
                Value::Float(f) => Ok(Value::Float(f)),
                other => Err(self.errorf(format!(
                    "wrong type for value; expected float64; got {}",
                    other.type_name()
                ))),
            },
            ParamTy::Time => match v {
                Value::Time(_) => Ok(v),
                other => Err(self.errorf(format!(
                    "wrong type for value; expected time.Time; got {}",
                    other.type_name()
                ))),
            },
            ParamTy::Dur => match v {
                Value::Duration(n) => Ok(Value::Duration(n)),
                Value::Int(n, _) => Ok(Value::Duration(n)),
                Value::Uint(n, _) => Ok(Value::Duration(n as i64)),
                Value::Month(n) | Value::Weekday(n) => Ok(Value::Duration(n)),
                other => Err(self.errorf(format!(
                    "wrong type for value; expected time.Duration; got {}",
                    other.type_name()
                ))),
            },
            ParamTy::Loc => match v {
                Value::Location(_) | Value::Nil => Ok(v),
                other => Err(self.errorf(format!(
                    "wrong type for value; expected *time.Location; got {}",
                    other.type_name()
                ))),
            },
            ParamTy::BytesTy => match v {
                Value::Bytes(_) => Ok(v),
                other => Err(self.errorf(format!(
                    "wrong type for value; expected []uint8; got {}",
                    other.type_name()
                ))),
            },
        }
    }

    /// `idealConstant`: an untyped huge-uint literal is an execution
    /// error (`exec.go:614-616`).
    fn ideal_constant(&mut self, num: &Num, text: &str) -> VResult<'c> {
        match num {
            Num::Uint(_) => Err(self.errorf(format!("{text} overflows int"))),
            other => Ok(num_value(other)),
        }
    }

    // -- output -----------------------------------------------------------

    fn print_value_go(&mut self, val: &Value<'c>) -> Result<(), ExecError> {
        // Borrow-split: the print environment reads only `input`, so the
        // renderer can write straight into `out` (no temp buffer — the
        // per-row allocation gates count on it).
        let env = InputPrintEnv { input: self.input };
        gofmt::write_template_value(self.out, val, &env);
        if self.input.budget.breached() {
            return Err(self.budget_error());
        }
        Ok(())
    }

    // -- label map ----------------------------------------------------------

    fn label_get(&self, name: &str) -> Option<&'c [u8]> {
        if let Some(err) = self.input.err
            && name == crate::logql::pipeline::ERROR_LABEL
        {
            return Some(err.as_bytes());
        }
        if let Some(details) = self.input.err_details
            && name == crate::logql::pipeline::ERROR_DETAILS_LABEL
        {
            return Some(details.as_bytes());
        }
        self.input
            .labels
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_bytes())
    }

    fn label_pairs_sorted(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        label_pairs_sorted_of(self.input)
    }
}

/// Sorted data-map pairs (the reference's `lbs.Map()` view, error pair
/// gated by the caller).
fn label_pairs_sorted_of(input: &EvalInput<'_, '_>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = input
        .labels
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect();
    if let Some(err) = input.err {
        upsert(&mut pairs, crate::logql::pipeline::ERROR_LABEL, err);
    }
    if let Some(details) = input.err_details {
        upsert(
            &mut pairs,
            crate::logql::pipeline::ERROR_DETAILS_LABEL,
            details,
        );
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
}

fn upsert(pairs: &mut Vec<(Vec<u8>, Vec<u8>)>, key: &str, value: &str) {
    if let Some(entry) = pairs.iter_mut().find(|(k, _)| k == key.as_bytes()) {
        entry.1 = value.as_bytes().to_vec();
    } else {
        pairs.push((key.as_bytes().to_vec(), value.as_bytes().to_vec()));
    }
}

/// PrintEnv adapter over the render INPUT (not the whole state — the
/// borrow split lets printers write into `state.out` directly).
struct InputPrintEnv<'p, 'c, 'a> {
    input: &'p EvalInput<'c, 'a>,
}

impl PrintEnv for InputPrintEnv<'_, '_, '_> {
    fn env(&self) -> &TemplateEnv {
        self.input.env
    }
    fn label_pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        label_pairs_sorted_of(self.input)
    }
    fn budget(&self) -> &super::RenderBudget {
        self.input.budget
    }
}

fn num_value<'v>(num: &Num) -> Value<'v> {
    match num {
        Num::Int(n) => Value::Int(*n, IntKind::Int),
        Num::Uint(u) => Value::Uint(*u, UintKind::Uint64),
        Num::Float(f) => Value::Float(*f),
        Num::Complex(re, im) => Value::Complex(*re, *im),
    }
}

fn template_node_string(name: &str, pipe: Option<&Pipe>) -> String {
    let quoted = gofmt::quote_bytes(name.as_bytes(), '"');
    match pipe {
        Some(p) => format!("{{{{template {} {}}}}}", quoted, p.render_string()),
        None => format!("{{{{template {}}}}}", quoted),
    }
}

// ---------------------------------------------------------------------
// Builtins (the non-lazy ones)
// ---------------------------------------------------------------------

enum Callee<'c> {
    Registry(&'static funcs::FuncDef),
    Builtin(&'static BuiltinDef),
    Method(Value<'c>, &'static methods::MethodSig),
}

type BuiltinImpl =
    for<'s, 'c, 'a> fn(&mut State<'s, 'c, 'a>, &[Value<'c>]) -> Result<Value<'c>, String>;

struct BuiltinDef {
    params: &'static [ParamTy],
    variadic: Option<ParamTy>,
    call: BuiltinImpl,
}

fn builtin_sig(name: &str) -> Option<&'static BuiltinDef> {
    use ParamTy::{Any, Str};
    macro_rules! def {
        ($params:expr, $variadic:expr, $call:expr) => {{
            static DEF: BuiltinDef = BuiltinDef {
                params: $params,
                variadic: $variadic,
                call: $call,
            };
            Some(&DEF)
        }};
    }
    match name {
        "not" => def!(&[Any], None, |st, a| {
            let (truth, _) = st.value_truth(&a[0]);
            Ok(Value::Bool(!truth))
        }),
        "len" => def!(&[Any], None, |st, a| match &a[0] {
            Value::LabelMap => Ok(Value::int(st.label_pairs_sorted().len() as i64)),
            v => builtin_len(v),
        }),
        "index" => def!(&[Any], Some(Any), |st, a| builtin_index(st, a)),
        "slice" => def!(&[Any], Some(Any), |st, a| builtin_slice(st, a)),
        // `call` is special-cased in eval_function (Go inserts the
        // callee NODE text into the error); this def only carries arity.
        "call" => def!(&[Any], Some(Any), |_st, a| match &a[0] {
            Value::Nil => Err("call of nil".to_string()),
            other => Err(format!("non-function of type {}", other.type_name())),
        }),
        "print" => def!(&[], Some(Any), |st, a| {
            let mut buf = Vec::new();
            let env = InputPrintEnv { input: st.input };
            gofmt::sprint(&mut buf, a, &env);
            Ok(Value::Str(Cow::Owned(buf)))
        }),
        "println" => def!(&[], Some(Any), |st, a| {
            let mut buf = Vec::new();
            let env = InputPrintEnv { input: st.input };
            gofmt::sprintln(&mut buf, a, &env);
            Ok(Value::Str(Cow::Owned(buf)))
        }),
        "printf" => def!(&[Str], Some(Any), |st, a| {
            let mut buf = Vec::new();
            let env = InputPrintEnv { input: st.input };
            let format = match &a[0] {
                Value::Str(s) => s.clone().into_owned(),
                _ => Vec::new(),
            };
            gofmt::sprintf(&mut buf, &format, &a[1..], &env);
            Ok(Value::Str(Cow::Owned(buf)))
        }),
        "html" => def!(&[], Some(Any), |st, a| {
            let text = eval_args_text(st, a);
            Ok(Value::Str(Cow::Owned(html_escape(&text))))
        }),
        "js" => def!(&[], Some(Any), |st, a| {
            let text = eval_args_text(st, a);
            Ok(Value::Str(Cow::Owned(js_escape(&text))))
        }),
        "urlquery" => def!(&[], Some(Any), |st, a| {
            let text = eval_args_text(st, a);
            Ok(Value::Str(Cow::Owned(url_query_escape(&text))))
        }),
        "eq" => def!(&[Any], Some(Any), |st, a| builtin_eq(st, a)),
        "ne" => def!(&[Any, Any], None, |st, a| {
            builtin_eq(st, a).map(|v| match v {
                Value::Bool(b) => Value::Bool(!b),
                other => other,
            })
        }),
        "lt" => def!(&[Any, Any], None, |_st, a| builtin_lt(&a[0], &a[1])
            .map(Value::Bool)),
        "le" => def!(&[Any, Any], None, |st, a| {
            let lt = builtin_lt(&a[0], &a[1])?;
            if lt {
                return Ok(Value::Bool(true));
            }
            builtin_eq(st, a)
        }),
        "gt" => def!(&[Any, Any], None, |st, a| {
            let lt = builtin_lt(&a[0], &a[1])?;
            if lt {
                return Ok(Value::Bool(false));
            }
            let eq = builtin_eq(st, a)?;
            match eq {
                Value::Bool(b) => Ok(Value::Bool(!b)),
                other => Ok(other),
            }
        }),
        "ge" => def!(&[Any, Any], None, |_st, a| {
            let lt = builtin_lt(&a[0], &a[1])?;
            Ok(Value::Bool(!lt))
        }),
        _ => None,
    }
}

/// `evalArgs` (funcs.go): the argument text for html/js/urlquery.
fn eval_args_text<'c>(st: &State<'_, 'c, '_>, args: &[Value<'c>]) -> Vec<u8> {
    if args.len() == 1
        && let Value::Str(s) = &args[0]
    {
        return s.clone().into_owned();
    }
    // printableValue: the invalid value becomes the string "<no value>".
    let mapped: Vec<Value<'c>> = args
        .iter()
        .map(|v| match v {
            Value::Nil => Value::Str(Cow::Borrowed(b"<no value>")),
            other => other.clone(),
        })
        .collect();
    let mut buf = Vec::new();
    let env = InputPrintEnv { input: st.input };
    gofmt::sprint(&mut buf, &mapped, &env);
    buf
}

fn builtin_len<'v>(v: &Value<'v>) -> Result<Value<'v>, String> {
    match v {
        // `length()` reaches `item.Type()` on the zero Value — a panic
        // the reference surfaces verbatim through safeCall (container-
        // captured).
        Value::Nil => Err("reflect: call of reflect.Value.Type on zero Value".to_string()),
        Value::Str(s) => Ok(Value::int(s.len() as i64)),
        Value::Bytes(b) => Ok(Value::int(b.len() as i64)),
        Value::List(l) => Ok(Value::int(l.len() as i64)),
        Value::Map(m) => Ok(Value::int(m.len() as i64)),
        // LabelMap is resolved by the builtin wrapper (needs state).
        Value::LabelMap => Ok(Value::int(0)),
        other => Err(format!("len of type {}", other.type_name())),
    }
}

fn index_arg(index: &Value<'_>, cap: usize) -> Result<usize, String> {
    let x: i64 = match index {
        Value::Int(n, _) => *n,
        Value::Uint(n, _) => *n as i64,
        Value::Duration(n) | Value::Month(n) | Value::Weekday(n) => *n,
        Value::Nil => return Err("cannot index slice/array with nil".to_string()),
        other => {
            return Err(format!(
                "cannot index slice/array with type {}",
                other.type_name()
            ));
        }
    };
    if x < 0 || x as usize > cap {
        return Err(format!("index out of range: {x}"));
    }
    Ok(x as usize)
}

fn builtin_index<'s, 'c, 'a>(
    st: &mut State<'s, 'c, 'a>,
    args: &[Value<'c>],
) -> Result<Value<'c>, String> {
    let mut item = args[0].clone();
    if matches!(item, Value::Nil) {
        return Err("index of untyped nil".to_string());
    }
    for index in &args[1..] {
        // A nil produced by an earlier indexing step is a nil
        // INTERFACE, not the untyped nil (`indirect` reports isNil).
        if matches!(item, Value::Nil) {
            return Err("index of nil pointer".to_string());
        }
        item = match &item {
            Value::List(l) => {
                let x = index_arg(index, l.len())?;
                if x >= l.len() {
                    return Err(format!("index out of range: {x}"));
                }
                l[x].clone()
            }
            Value::Str(s) => {
                let x = index_arg(index, s.len())?;
                if x >= s.len() {
                    return Err(format!("index out of range: {x}"));
                }
                funcs::byte_elem(s[x])
            }
            Value::Bytes(b) => {
                let x = index_arg(index, b.len())?;
                if x >= b.len() {
                    return Err(format!("index out of range: {x}"));
                }
                funcs::byte_elem(b[x])
            }
            Value::Map(entries) => match index {
                Value::Str(key) => entries
                    .iter()
                    .find(|(k, _)| k == key.as_ref())
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Value::Nil),
                Value::Nil => Value::Nil,
                other => {
                    return Err(format!(
                        "value has type {}; should be string",
                        other.type_name()
                    ));
                }
            },
            Value::LabelMap => match index {
                Value::Str(key) => {
                    let name = String::from_utf8_lossy(key).into_owned();
                    st.label_get(&name)
                        .map(|v| Value::Str(Cow::Borrowed(v)))
                        .unwrap_or(Value::Str(Cow::Borrowed(b"")))
                }
                Value::Nil => Value::Str(Cow::Borrowed(b"")),
                other => {
                    return Err(format!(
                        "value has type {}; should be string",
                        other.type_name()
                    ));
                }
            },
            other => {
                return Err(format!("can't index item of type {}", other.type_name()));
            }
        };
    }
    Ok(item)
}

fn builtin_slice<'s, 'c, 'a>(
    _st: &mut State<'s, 'c, 'a>,
    args: &[Value<'c>],
) -> Result<Value<'c>, String> {
    let item = &args[0];
    if matches!(item, Value::Nil) {
        return Err("slice of untyped nil".to_string());
    }
    let indexes = &args[1..];
    if indexes.len() > 3 {
        return Err(format!("too many slice indexes: {}", indexes.len()));
    }
    let cap = match item {
        Value::Str(s) => {
            if indexes.len() == 3 {
                return Err("cannot 3-index slice a string".to_string());
            }
            s.len()
        }
        Value::Bytes(b) => b.len(),
        Value::List(l) => l.len(),
        other => {
            return Err(format!("can't slice item of type {}", other.type_name()));
        }
    };
    let len = match item {
        Value::Str(s) => s.len(),
        Value::Bytes(b) => b.len(),
        Value::List(l) => l.len(),
        _ => 0,
    };
    let mut idx = [0usize, len, 0];
    for (i, index) in indexes.iter().enumerate() {
        idx[i] = index_arg(index, cap)?;
    }
    if idx[0] > idx[1] {
        return Err(format!("invalid slice index: {} > {}", idx[0], idx[1]));
    }
    if indexes.len() == 3 && idx[1] > idx[2] {
        return Err(format!("invalid slice index: {} > {}", idx[1], idx[2]));
    }
    Ok(match item {
        Value::Str(s) => Value::Str(Cow::Owned(s[idx[0]..idx[1]].to_vec())),
        Value::Bytes(b) => Value::Bytes(Cow::Owned(b[idx[0]..idx[1]].to_vec())),
        Value::List(l) => Value::List(Rc::new(l[idx[0]..idx[1]].to_vec())),
        _ => Value::Nil,
    })
}

// -- comparisons (funcs.go eq/lt with the int/uint cross rules) ---------

#[derive(PartialEq, Clone, Copy)]
enum BasicKind {
    Bool,
    Complex,
    Int,
    Float,
    Str,
    Uint,
    Invalid,
}

fn basic_kind(v: &Value<'_>) -> BasicKind {
    match v {
        Value::Bool(_) => BasicKind::Bool,
        Value::Complex(..) => BasicKind::Complex,
        Value::Int(..) | Value::Duration(_) | Value::Month(_) | Value::Weekday(_) => BasicKind::Int,
        Value::Uint(..) => BasicKind::Uint,
        Value::Float(_) => BasicKind::Float,
        Value::Str(_) => BasicKind::Str,
        _ => BasicKind::Invalid,
    }
}

fn int_val(v: &Value<'_>) -> i64 {
    match v {
        Value::Int(n, _) | Value::Duration(n) | Value::Month(n) | Value::Weekday(n) => *n,
        _ => 0,
    }
}

fn builtin_eq<'c>(st: &State<'_, 'c, '_>, args: &[Value<'c>]) -> Result<Value<'c>, String> {
    let arg1 = &args[0];
    if args.len() < 2 {
        return Err("missing argument for comparison".to_string());
    }
    let k1 = basic_kind(arg1);
    for arg in &args[1..] {
        let k2 = basic_kind(arg);
        let truth = if k1 != k2 {
            match (k1, k2) {
                (BasicKind::Int, BasicKind::Uint) => {
                    let i = int_val(arg1);
                    let u = match arg {
                        Value::Uint(u, _) => *u,
                        _ => 0,
                    };
                    i >= 0 && (i as u64) == u
                }
                (BasicKind::Uint, BasicKind::Int) => {
                    let u = match arg1 {
                        Value::Uint(u, _) => *u,
                        _ => 0,
                    };
                    let i = int_val(arg);
                    i >= 0 && u == (i as u64)
                }
                _ => {
                    if !matches!(arg1, Value::Nil) && !matches!(arg, Value::Nil) {
                        return Err(format!(
                            "incompatible types for comparison: {} and {}",
                            arg1.type_name(),
                            arg.type_name()
                        ));
                    }
                    false
                }
            }
        } else {
            match k1 {
                BasicKind::Bool => {
                    matches!((arg1, arg), (Value::Bool(a), Value::Bool(b)) if a == b)
                }
                BasicKind::Complex => matches!(
                    (arg1, arg),
                    (Value::Complex(ar, ai), Value::Complex(br, bi)) if ar == br && ai == bi
                ),
                BasicKind::Float => matches!(
                    (arg1, arg),
                    (Value::Float(a), Value::Float(b)) if a == b
                ),
                BasicKind::Int => int_val(arg1) == int_val(arg),
                BasicKind::Str => {
                    matches!((arg1, arg), (Value::Str(a), Value::Str(b)) if a == b)
                }
                BasicKind::Uint => matches!(
                    (arg1, arg),
                    (Value::Uint(a, _), Value::Uint(b, _)) if a == b
                ),
                BasicKind::Invalid => {
                    // Nil-vs-nil, struct and pointer comparisons; the
                    // non-comparable kinds carry Go's `%s`-rendered
                    // second argument in the error (funcs.go eq).
                    match (arg1, arg) {
                        (Value::Nil, Value::Nil) => true,
                        (Value::Nil, _) | (_, Value::Nil) => false,
                        (Value::Time(a), Value::Time(b)) => a == b,
                        (Value::Location(a), Value::Location(b)) => a == b,
                        (Value::Duration(a), Value::Duration(b)) => a == b,
                        (Value::Month(a), Value::Month(b)) => a == b,
                        (Value::Weekday(a), Value::Weekday(b)) => a == b,
                        (a, b) if a.type_name() != b.type_name() => {
                            let env = InputPrintEnv { input: st.input };
                            let mut ra = Vec::new();
                            let mut p = String::new();
                            gofmt::sprintf(&mut ra, b"%s", std::slice::from_ref(a), &env);
                            p.push_str(&String::from_utf8_lossy(&ra));
                            let mut rb = Vec::new();
                            gofmt::sprintf(&mut rb, b"%v", std::slice::from_ref(b), &env);
                            return Err(format!(
                                "non-comparable types {}: {}, {}: {}",
                                p,
                                a.type_name(),
                                b.type_name(),
                                String::from_utf8_lossy(&rb),
                            ));
                        }
                        (_, b) => {
                            // Same kind, non-comparable type (slice/map):
                            // `non-comparable type %s: %v` over the
                            // SECOND argument.
                            let env = InputPrintEnv { input: st.input };
                            let mut rb = Vec::new();
                            gofmt::sprintf(&mut rb, b"%s", std::slice::from_ref(b), &env);
                            return Err(format!(
                                "non-comparable type {}: {}",
                                String::from_utf8_lossy(&rb),
                                b.type_name(),
                            ));
                        }
                    }
                }
            }
        };
        if truth {
            return Ok(Value::Bool(true));
        }
    }
    Ok(Value::Bool(false))
}

fn builtin_lt(arg1: &Value<'_>, arg2: &Value<'_>) -> Result<bool, String> {
    let k1 = basic_kind(arg1);
    let k2 = basic_kind(arg2);
    if k1 == BasicKind::Invalid || k2 == BasicKind::Invalid {
        return Err("invalid type for comparison".to_string());
    }
    if k1 != k2 {
        return match (k1, k2) {
            (BasicKind::Int, BasicKind::Uint) => {
                let i = int_val(arg1);
                let u = match arg2 {
                    Value::Uint(u, _) => *u,
                    _ => 0,
                };
                Ok(i < 0 || (i as u64) < u)
            }
            (BasicKind::Uint, BasicKind::Int) => {
                let u = match arg1 {
                    Value::Uint(u, _) => *u,
                    _ => 0,
                };
                let i = int_val(arg2);
                Ok(i >= 0 && u < (i as u64))
            }
            _ => Err(format!(
                "incompatible types for comparison: {} and {}",
                arg1.type_name(),
                arg2.type_name()
            )),
        };
    }
    match k1 {
        BasicKind::Bool | BasicKind::Complex => Err("invalid type for comparison".to_string()),
        BasicKind::Float => Ok(matches!((arg1, arg2), (Value::Float(a), Value::Float(b)) if a < b)),
        BasicKind::Int => Ok(int_val(arg1) < int_val(arg2)),
        BasicKind::Str => Ok(matches!((arg1, arg2), (Value::Str(a), Value::Str(b)) if a < b)),
        BasicKind::Uint => Ok(matches!(
            (arg1, arg2),
            (Value::Uint(a, _), Value::Uint(b, _)) if a < b
        )),
        BasicKind::Invalid => Err("invalid type for comparison".to_string()),
    }
}

// -- escapers -----------------------------------------------------------

fn html_escape(b: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(b.len());
    for &c in b {
        match c {
            0 => out.extend_from_slice("\u{FFFD}".as_bytes()),
            b'"' => out.extend_from_slice(b"&#34;"),
            b'\'' => out.extend_from_slice(b"&#39;"),
            b'&' => out.extend_from_slice(b"&amp;"),
            b'<' => out.extend_from_slice(b"&lt;"),
            b'>' => out.extend_from_slice(b"&gt;"),
            c => out.push(c),
        }
    }
    out
}

fn js_escape(b: &[u8]) -> Vec<u8> {
    use std::io::Write as _;
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c < 0x80 {
            match c {
                b'\\' => out.extend_from_slice(b"\\\\"),
                b'\'' => out.extend_from_slice(b"\\'"),
                b'"' => out.extend_from_slice(b"\\\""),
                b'<' => out.extend_from_slice(b"\\u003C"),
                b'>' => out.extend_from_slice(b"\\u003E"),
                b'&' => out.extend_from_slice(b"\\u0026"),
                b'=' => out.extend_from_slice(b"\\u003D"),
                c if c < b' ' => {
                    let _ = write!(out, "\\u00{:X}{:X}", c >> 4, c & 0xF);
                }
                c => out.push(c),
            }
            i += 1;
            continue;
        }
        // Unicode rune.
        let (r, size) = decode_rune_local(b, i);
        if gofmt::is_print(r) {
            out.extend_from_slice(&b[i..i + size]);
        } else {
            let _ = write!(out, "\\u{:04X}", r as u32);
        }
        i += size;
    }
    out
}

fn decode_rune_local(b: &[u8], i: usize) -> (char, usize) {
    let first = b[i];
    let len = match first {
        0x00..=0x7F => 1,
        0xC2..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF4 => 4,
        _ => return ('\u{FFFD}', 1),
    };
    if i + len > b.len() {
        return ('\u{FFFD}', 1);
    }
    match std::str::from_utf8(&b[i..i + len]) {
        Ok(s) => (s.chars().next().unwrap_or('\u{FFFD}'), len),
        Err(_) => ('\u{FFFD}', 1),
    }
}

fn url_query_escape(b: &[u8]) -> Vec<u8> {
    const UPPERHEX: &[u8] = b"0123456789ABCDEF";
    let mut out = Vec::with_capacity(b.len());
    for &c in b {
        if c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.' | b'~') {
            out.push(c);
        } else if c == b' ' {
            out.push(b'+');
        } else {
            out.push(b'%');
            out.push(UPPERHEX[(c >> 4) as usize]);
            out.push(UPPERHEX[(c & 0xF) as usize]);
        }
    }
    out
}
