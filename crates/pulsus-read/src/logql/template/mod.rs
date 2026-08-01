//! Go `text/template` + sprig-subset engine for LogQL
//! `line_format`/`label_format` (issue #230). The reference parses
//! template bodies with `template.New("line"|"label").Option(
//! "missingkey=zero").Funcs(<the 67-name map>)` (`pkg/logql/log/
//! fmt.go:212`, `:379`), so the whole template language is reachable;
//! this module is a purpose-built port (plan v1 §2) — no third-party
//! template crate.
//!
//! **Fast paths** (plan v1 §5/§6): `compile` derives the pre-#230
//! byte-identical shapes from the parsed tree — `Simple` (exactly one
//! `{{.ident}}` action, the reference's own `simpleKey` shortcut,
//! `fmt.go:218-228`) and `Parts` (text + single-ident field actions) —
//! so the `{{.label}}` corpus keeps its existing single-allocation
//! render, and the evaluator only runs for templates that need it.

pub mod decimal;
pub mod eval;
pub mod funcs;
pub mod gofmt;
pub mod golayout;
pub mod lex;
pub mod methods;
pub mod parse;
pub mod retained;
pub mod timefns;
pub mod value;

use std::collections::HashMap;
use std::fmt;

pub use eval::ExecError as TemplateExecError;
pub use retained::{LabelSnapshot, Retained, render_full};
pub use timefns::TemplateEnv;

/// The per-ROW output-byte budget (issue #230 follow-up; lifetime moved
/// from per-render to per-row by issue #260): every allocation whose
/// size a template argument multiplies (`repeat`, `indent`/`nindent`,
/// `alignLeft`/`alignRight`, `printf` padding widths/precisions,
/// `Replace`-with-empty-needle expansion, and the constant-factor string
/// producers) is CHARGED against this budget BEFORE it happens and the
/// whole budget is released when the ROW's pipeline run ends.
///
/// **Why the row and not the render** (issue #260). A render's output is
/// RETAINED by its caller — `line_format` moves it into `line`, and each
/// `label_format` destination `set_label`s it — so a budget whose
/// lifetime ended with the render bounded one live buffer while an
/// unbounded number of them stayed live: a `label_format` stage's
/// destination count is limited only by [`pulsus_logql::MAX_QUERY_BYTES`]
/// (131 072), and a `,x="{{repeat N .a}}"` destination costs ~26 text
/// bytes — so >4 000 simultaneously-live 64 MiB outputs fitted inside the
/// query-text cap. A sum over an unbounded multiplicity is not a bound,
/// so the budget now lives for the whole of
/// `CompiledPipeline::run_mode_into` — the smallest lifetime that
/// contains every render one row performs. Renders of DIFFERENT rows
/// still get their own budget (the per-row outputs' accumulation into the
/// streams result across up to `MAX_LIMIT` entries is a separate, larger
/// hole, deliberately not closed here).
///
/// **Value: 64 MiB.** A breach aborts the QUERY with the bounded 422
/// (`TooBroadReason::TemplateOutputBytes`), never a per-line tag, a
/// truncation, or an OOM. The reference is unbounded here (measured: a
/// 17 GB `repeat` OOM-kills it) — a ledgered bounded divergence
/// (`template-output-budget`).
///
/// **Derivation, and why it is now a standalone constant.** #230 defined
/// this as `= crate::logql::charge::MAX_CLIENT_AGG_GROUP_BYTES`, which was then also
/// 64 MiB — a convenience link on the reasoning that "a single rendered
/// line may not allocate more than a whole query is allowed to retain".
/// Issue #236 raised `MAX_CLIENT_AGG_GROUP_BYTES` to 256 MiB for a reason
/// that is specific to the GROUP axis (deleting the mid-scan group-count
/// cap left that constant carrying the whole load, so it had to admit
/// high-cardinality aggregations the reference serves). Nothing about
/// that argument applies to one line's template output, and following
/// the link would have silently quadrupled an unrelated OOM guard — so
/// the link is severed and #230's shipped 64 MiB behaviour is preserved
/// byte-for-byte. The inequality the original rationale wanted still
/// holds (`64 MiB <= MAX_CLIENT_AGG_GROUP_BYTES`) and is asserted by
/// the const assertion below.
pub const MAX_TEMPLATE_RENDER_BYTES: u64 = 64 * 1024 * 1024;

/// Issue #236: a COMPILE-TIME gate, not a test — one render may never
/// out-allocate a whole query's retained-state budget. #230 expressed
/// this as an equality (`= MAX_CLIENT_AGG_GROUP_BYTES`), which silently
/// followed #236's group-axis raise to 256 MiB; the inequality is what
/// that derivation actually wanted, and stating it here means a future
/// raise of either constant cannot invert it without failing the build.
const _: () = assert!(
    MAX_TEMPLATE_RENDER_BYTES <= crate::logql::charge::MAX_CLIENT_AGG_GROUP_BYTES,
    "one render may not out-allocate a whole query's retention budget"
);

/// The countdown ledger a row's renders charge against (fresh per ROW
/// since issue #260 — the symmetric release point is the end of the
/// row's pipeline run, and every render the row performs shares it).
#[derive(Debug)]
pub struct RenderBudget {
    remaining: std::cell::Cell<u64>,
    breached: std::cell::Cell<bool>,
}

impl Default for RenderBudget {
    fn default() -> Self {
        RenderBudget {
            remaining: std::cell::Cell::new(MAX_TEMPLATE_RENDER_BYTES),
            breached: std::cell::Cell::new(false),
        }
    }
}

/// A refused [`RenderBudget`] charge, carrying no message (issue #260).
///
/// The engine's own breaches surface as an [`TemplateExecError`] whose
/// `msg` the caller may show; the pipeline's `Simple`/`Parts` fast paths
/// abort the query with a fixed-text error instead, so building a message
/// for them would be an allocation on the abort path and a second place
/// holding the wording.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetExhausted;

impl RenderBudget {
    /// Charges `bytes` BEFORE the allocation it pays for. On breach the
    /// budget is poisoned (`breached`) — the evaluator turns that into
    /// the query-aborting error class, never a per-line
    /// `TemplateFormatErr`.
    pub fn charge(&self, bytes: usize) -> Result<(), String> {
        self.charge_retained(bytes).map_err(|BudgetExhausted| {
            format!("template output exceeded the {MAX_TEMPLATE_RENDER_BYTES}-byte render budget")
        })
    }

    /// [`RenderBudget::charge`] without the message — the SAME ledger and
    /// the SAME poison flag, for callers that map a breach onto their own
    /// fixed error (issue #260's `Simple`/`Parts` fast paths, whose
    /// output is retained exactly as the full engine's is and must
    /// therefore be charged the same way).
    ///
    /// One implementation, two surfaces: a second countdown would be a
    /// second ceiling, which is the defect this issue exists to close.
    pub fn charge_retained(&self, bytes: usize) -> Result<(), BudgetExhausted> {
        let need = bytes as u64;
        let left = self.remaining.get();
        if need > left {
            self.breached.set(true);
            return Err(BudgetExhausted);
        }
        self.remaining.set(left - need);
        Ok(())
    }

    pub fn breached(&self) -> bool {
        self.breached.get()
    }

    /// How many bytes this render has charged so far (the runtime
    /// allocation-dominance gate compares allocated bytes against this).
    pub fn charged_bytes(&self) -> u64 {
        MAX_TEMPLATE_RENDER_BYTES - self.remaining.get()
    }
}

/// Which of the reference's two template names this body compiles under
/// — it appears verbatim inside every execution-error string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateKind {
    Line,
    Label,
}

impl TemplateKind {
    fn parse_name(self) -> &'static str {
        match self {
            TemplateKind::Line => "line",
            TemplateKind::Label => "label",
        }
    }
}

/// One fast-path template segment (the pre-#230 `TmplPart` shape).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Part {
    Lit(String),
    Field(String),
}

/// A compiled template body (plan v1 §5).
#[derive(Debug, Clone)]
pub enum Template {
    /// Exactly one single-ident field action (`{{.message}}`).
    Simple(String),
    /// Text + single-ident field actions only.
    Parts(Vec<Part>),
    /// Everything else: the full evaluator.
    Full(Box<Program>),
}

/// A compiled full-evaluator template.
#[derive(Debug, Clone)]
pub struct Program {
    root: parse::List,
    defines: Vec<(String, parse::List)>,
    /// Retained: execution-error columns are byte offsets into it.
    text: String,
    kind: TemplateKind,
    /// `.`/`$` consumed as a value somewhere → the sorted label map may
    /// be materialised at render time.
    pub needs_dot_map: bool,
    /// Calls `__line__`.
    pub needs_line: bool,
    /// Calls `__timestamp__`/`now`/`date` (any wall-clock/ts input).
    pub needs_ts: bool,
    /// Compile-time-compiled literal regex patterns
    /// (`regexReplaceAll`/`regexReplaceAllLiteral`/`count` — plan v1
    /// §6: strictly faster than the reference's per-call compile, with
    /// identical observable behaviour; a literal that FAILS to compile
    /// stays out of the cache so the per-line execution error is
    /// preserved).
    regex_cache: HashMap<String, regex::Regex>,
}

/// Compile-time failure; `Display` is Go's parse-error text
/// (`template: line:1: function "x" not defined`). The pipeline wraps
/// it with the reference's `invalid line template: ` /
/// `invalid template for label '<dst>': ` prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateCompileError(pub String);

impl fmt::Display for TemplateCompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TemplateCompileError {}

/// Parses and compiles a template body.
pub fn compile(text: &str, kind: TemplateKind) -> Result<Template, TemplateCompileError> {
    let name = kind.parse_name();
    let tree = parse::parse(name, text, funcs::all_callable_names())
        .map_err(|e| TemplateCompileError(format!("template: {}:{}: {}", name, e.line, e.msg)))?;

    // Fast-path derivation (the reference keeps the same simpleKey
    // shortcut; Parts generalises it to any text+field mix).
    if tree.defines.is_empty()
        && let Some(parts) = derive_parts(&tree.root)
    {
        if parts.len() == 1
            && tree.root.nodes.len() == 1
            && let Part::Field(name) = &parts[0]
        {
            return Ok(Template::Simple(name.clone()));
        }
        return Ok(Template::Parts(parts));
    }

    let mut flags = Flags::default();
    scan_list(&tree.root, &mut flags);
    for (_, list) in &tree.defines {
        scan_list(list, &mut flags);
    }
    let mut regex_cache = HashMap::new();
    for pattern in flags.literal_regexes {
        if let Ok(re) = regex::Regex::new(&pattern) {
            regex_cache.insert(pattern, re);
        }
    }
    Ok(Template::Full(Box::new(Program {
        root: tree.root,
        defines: tree.defines,
        text: text.to_string(),
        kind,
        needs_dot_map: flags.needs_dot_map,
        needs_line: flags.needs_line,
        needs_ts: flags.needs_ts,
        regex_cache,
    })))
}

// ---------------------------------------------------------------------
// Fast-path derivation + flag scan
// ---------------------------------------------------------------------

/// `Some(parts)` iff the tree is only text and bare single-ident field
/// actions (no pipelines, no functions, no declarations).
fn derive_parts(list: &parse::List) -> Option<Vec<Part>> {
    let mut parts = Vec::with_capacity(list.nodes.len());
    for node in &list.nodes {
        match node {
            parse::Node::Text { text, .. } => {
                if !text.is_empty() {
                    parts.push(Part::Lit(text.clone()));
                }
            }
            parse::Node::Action { pipe, .. } => {
                if !pipe.decl.is_empty() || pipe.cmds.len() != 1 {
                    return None;
                }
                let cmd = &pipe.cmds[0];
                if cmd.args.len() != 1 {
                    return None;
                }
                match &cmd.args[0] {
                    parse::Arg::Field { idents, .. } if idents.len() == 1 => {
                        parts.push(Part::Field(idents[0].clone()));
                    }
                    _ => return None,
                }
            }
            _ => return None,
        }
    }
    Some(parts)
}

#[derive(Default)]
struct Flags {
    needs_dot_map: bool,
    needs_line: bool,
    needs_ts: bool,
    literal_regexes: Vec<String>,
}

fn scan_list(list: &parse::List, flags: &mut Flags) {
    for node in &list.nodes {
        match node {
            parse::Node::Text { .. } => {}
            parse::Node::Action { pipe, .. } => scan_pipe(pipe, flags),
            parse::Node::If {
                pipe,
                list,
                else_list,
                ..
            }
            | parse::Node::Range {
                pipe,
                list,
                else_list,
                ..
            }
            | parse::Node::With {
                pipe,
                list,
                else_list,
                ..
            } => {
                scan_pipe(pipe, flags);
                scan_list(list, flags);
                if let Some(el) = else_list {
                    scan_list(el, flags);
                }
            }
            parse::Node::Template { pipe, .. } => {
                if let Some(p) = pipe {
                    scan_pipe(p, flags);
                }
            }
            parse::Node::Break { .. } | parse::Node::Continue { .. } => {}
        }
    }
}

fn scan_pipe(pipe: &parse::Pipe, flags: &mut Flags) {
    for cmd in &pipe.cmds {
        // Literal-pattern regex precompilation.
        if let Some(parse::Arg::Ident { name, .. }) = cmd.args.first()
            && matches!(
                name.as_str(),
                "regexReplaceAll" | "regexReplaceAllLiteral" | "count"
            )
            && let Some(parse::Arg::Str { val, .. }) = cmd.args.get(1)
            && let Ok(pattern) = String::from_utf8(val.clone())
        {
            flags.literal_regexes.push(pattern);
        }
        for arg in &cmd.args {
            scan_arg(arg, flags);
        }
    }
}

fn scan_arg(arg: &parse::Arg, flags: &mut Flags) {
    match arg {
        parse::Arg::Dot { .. } => flags.needs_dot_map = true,
        parse::Arg::Var(v) => {
            if v.idents[0] == "$" {
                flags.needs_dot_map = true;
            }
        }
        parse::Arg::Ident { name, .. } => match name.as_str() {
            "__line__" => flags.needs_line = true,
            "__timestamp__" => flags.needs_ts = true,
            "now" | "date" => flags.needs_ts = true,
            _ => {}
        },
        parse::Arg::Chain { base, .. } => scan_arg(base, flags),
        parse::Arg::Pipe { pipe, .. } => scan_pipe(pipe, flags),
        _ => {}
    }
}
