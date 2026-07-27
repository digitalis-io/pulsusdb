//! Issue #230 review round 1: the MECHANICAL allocation census over
//! `src/logql/template/` — the boundary is derived from the AST, not
//! from a hand-drawn list (the hand-drawn list missed float
//! precisions, the `align*` rune-vec ordering, the sprig case-mapping
//! charges and `fromJson`; a text grep misses macro bodies, which is
//! exactly where the sprig closures live).
//!
//! **The classifying question** (coordinator ruling): *can a
//! caller-controlled input make this allocation large?* — NOT "is it a
//! multiplier function". Control-flow output accumulation (`range` over
//! an int repeating a text node) is a first-class member of the class.
//!
//! Method: `syn` parses every file in the module; a visitor walks every
//! function INCLUDING the closures inside the `f!`/`def!`/`sig!`
//! macro tables (the registry closures are where round 1's misses
//! hid) and records each function's set of allocation-vocabulary
//! callees. The census asserts the discovered map equals the pinned
//! classification below EXACTLY — a new allocation site, a new
//! function with allocations, or a removed site fails this test until
//! the pin (and its disposition) is updated. Dispositions:
//!
//! - `CHARGED` — a `RenderBudget::charge` DOMINATES the allocation in
//!   the same function (charge-before-allocate);
//! - `INPUT_BOUNDED` — size ≤ (small stated constant) × already-
//!   materialised inputs; safe because every input was itself charged
//!   at production or is store data, and the CUMULATIVE output
//!   accounting (text-node pre-charge + print delta) bounds repeated
//!   emission — so constant-factor chains die at the budget;
//! - `CONST` — fixed small size (≤ a few KiB), independent of inputs;
//! - `COMPILE_TIME` — allocated once per query at template compile
//!   (lexer/parser/fast-path derivation), bounded by the query text the
//!   API already caps — never per row;
//! - `RESIDUAL` — cannot be charged; named with its bound and reason.
//!
//! The zz generator prints the discovered table for re-pinning.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use syn::visit::Visit;

/// The allocation vocabulary. Anything on this list found anywhere in
/// the module must be classified. `push` IS included: round 1's
/// `align*` miss was a per-rune `Vec::push` loop building an uncharged
/// 16×-input intermediate.
const METHOD_VOCAB: &[&str] = &[
    "with_capacity",
    "to_vec",
    "to_owned",
    "into_owned",
    "to_string",
    "to_uppercase",
    "to_lowercase",
    "repeat",
    "collect",
    "clone",
    "extend",
    "extend_from_slice",
    "push",
    "push_str",
    "insert",
    "append",
    "encode",
    // `impl Into<Vec<u8>>` copies borrowed inputs at factor 1.
    "into",
];
const PATH_VOCAB: &[&str] = &["with_capacity", "from_utf8", "from_utf8_lossy", "new"];
const MACRO_VOCAB: &[&str] = &["vec", "format", "write"];

/// Macros whose bodies MUST parse and be visited (the closure tables).
const DESCEND_MACROS: &[&str] = &["f", "def", "sig", "vec", "format", "write", "matches"];

type CensusMap = BTreeMap<(String, String), BTreeSet<String>>;

struct Census {
    file: String,
    scope: Vec<String>,
    out: CensusMap,
}

impl Census {
    fn record(&mut self, callee: String) {
        let scope = self
            .scope
            .last()
            .cloned()
            .unwrap_or_else(|| "<module>".to_string());
        self.out
            .entry((self.file.clone(), scope))
            .or_default()
            .insert(callee);
    }
}

impl<'ast> Visit<'ast> for Census {
    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        self.scope.push(node.sig.ident.to_string());
        syn::visit::visit_item_fn(self, node);
        self.scope.pop();
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        self.scope.push(node.sig.ident.to_string());
        syn::visit::visit_impl_item_fn(self, node);
        self.scope.pop();
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        let name = node.method.to_string();
        if METHOD_VOCAB.contains(&name.as_str()) {
            self.record(format!(".{name}"));
        }
        syn::visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let syn::Expr::Path(p) = &*node.func
            && let Some(seg) = p.path.segments.last()
        {
            let name = seg.ident.to_string();
            // `new` is NOT recorded at all: Vec/String `::new` is
            // zero-alloc until growth (growth is covered by
            // push/extend), and `Rc::new`/`Box::new` are factor-1
            // moves-to-heap of values whose construction is already in
            // vocabulary (collect/to_vec/push at the same site).
            if PATH_VOCAB.contains(&name.as_str()) && name != "new" {
                let qualifier = p
                    .path
                    .segments
                    .iter()
                    .rev()
                    .nth(1)
                    .map(|s| s.ident.to_string())
                    .unwrap_or_default();
                self.record(format!("{qualifier}::{name}"));
            }
        }
        syn::visit::visit_expr_call(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        let Some(seg) = node.path.segments.last() else {
            return;
        };
        let name = seg.ident.to_string();
        if MACRO_VOCAB.contains(&name.as_str()) {
            self.record(format!("{name}!"));
        }
        if !DESCEND_MACROS.contains(&name.as_str()) {
            return;
        }
        // Parse the macro body as a comma-separated expression list and
        // keep walking — the registry/builtin/method tables live INSIDE
        // `f!`/`def!`/`sig!` and are exactly where round 1's misses hid.
        type ExprList = syn::punctuated::Punctuated<syn::Expr, syn::Token![,]>;
        let parsed = syn::parse::Parser::parse2(ExprList::parse_terminated, node.tokens.clone());
        match parsed {
            Ok(exprs) => {
                // `f!`'s first arg names the registry entry: scope the
                // closure under it for per-function pins.
                let mut scoped = false;
                if name == "f"
                    && let Some(syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(s),
                        ..
                    })) = exprs.first()
                {
                    self.scope.push(format!("f!{}", s.value()));
                    scoped = true;
                }
                for e in &exprs {
                    self.visit_expr(e);
                }
                if scoped {
                    self.scope.pop();
                }
            }
            Err(_) => {
                // `matches!` bodies with patterns do not parse as
                // expression lists and cannot allocate; every table
                // macro MUST parse — a silent skip would reopen the
                // round-1 blind spot.
                if name == "f" || name == "def" || name == "sig" {
                    self.record(format!("UNPARSED-{name}!"));
                }
            }
        }
    }
}

fn census() -> CensusMap {
    let dir = format!(
        "{}/src/logql/template",
        env!("CARGO_MANIFEST_DIR").replace('\\', "/")
    );
    let mut files: Vec<String> = std::fs::read_dir(&dir)
        .expect("template module dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(".rs"))
        .collect();
    files.sort();
    assert!(files.len() >= 10, "template module files: {files:?}");
    let mut c = Census {
        file: String::new(),
        scope: Vec::new(),
        out: BTreeMap::new(),
    };
    for f in files {
        let text = std::fs::read_to_string(format!("{dir}/{f}")).expect("read");
        let ast = syn::parse_file(&text).expect("parse");
        c.file = f;
        c.scope.clear();
        c.visit_file(&ast);
    }
    c.out
}

struct Pin {
    file: &'static str,
    func: &'static str,
    callees: &'static [&'static str],
    disposition: &'static str,
    why: &'static str,
}

/// Shorthands: the full reasons live in the module doc's disposition
/// definitions; per-entry `why` states the bound.
const CHARGED: &str = "CHARGED";
const INPUT_BOUNDED: &str = "INPUT_BOUNDED";
const CONST: &str = "CONST";
const COMPILE_TIME: &str = "COMPILE_TIME";

static PINS: &[Pin] = &[
    // --------------------------------------------------------------- decimal.rs
    Pin {
        file: "decimal.rs",
        func: "digits_at",
        callees: &[".clone", ".extend"],
        disposition: CONST,
        why: "digit vectors ≤ 17 + |exp| ≤ ~700 (f64 exponent range)",
    },
    Pin {
        file: "decimal.rs",
        func: "div",
        callees: &[".clone", ".to_string", "vec!"],
        disposition: CONST,
        why: "digit vectors <= ~770 (scale 16 + f64 exponent range); error text fixed",
    },
    Pin {
        file: "decimal.rs",
        func: "from_float",
        callees: &[".collect", ".to_string", "format!"],
        disposition: CONST,
        why: "shortest f64 digits ≤ 17 + exponent",
    },
    Pin {
        file: "decimal.rs",
        func: "mag_add",
        callees: &["vec!"],
        disposition: CONST,
        why: "operand-width + 1 ≤ ~750",
    },
    Pin {
        file: "decimal.rs",
        func: "mag_divmod",
        callees: &[".push", ".to_vec", "Vec::with_capacity"],
        disposition: CONST,
        why: "quotient/remainder ≤ dividend width ≤ ~770 (scale 16 + f64 range)",
    },
    Pin {
        file: "decimal.rs",
        func: "mag_mul",
        callees: &[".collect", "vec!"],
        disposition: CONST,
        why: "≤ 34 digits (17+17 coefficient product); div path ≤ ~750",
    },
    Pin {
        file: "decimal.rs",
        func: "mag_sub",
        callees: &["vec!"],
        disposition: CONST,
        why: "operand width",
    },
    Pin {
        file: "decimal.rs",
        func: "shift_left",
        callees: &[".extend", ".to_vec"],
        disposition: CONST,
        why: "≤ digits + |exp| ≤ ~750",
    },
    Pin {
        file: "decimal.rs",
        func: "to_f64",
        callees: &[".push", ".push_str", ".to_string", "String::with_capacity"],
        disposition: CONST,
        why: "digit-string ≤ ~750 chars",
    },
    // --------------------------------------------------------------- eval.rs
    Pin {
        file: "eval.rs",
        func: "budget_error",
        callees: &["format!"],
        disposition: CONST,
        why: "fixed message",
    },
    Pin {
        file: "eval.rs",
        func: "builtin_eq",
        callees: &[
            ".push_str",
            ".to_string",
            "String::from_utf8_lossy",
            "format!",
        ],
        disposition: INPUT_BOUNDED,
        why: "error paths render one value each",
    },
    Pin {
        file: "eval.rs",
        func: "builtin_index",
        callees: &[
            ".clone",
            ".into_owned",
            ".to_string",
            "String::from_utf8_lossy",
            "format!",
        ],
        disposition: INPUT_BOUNDED,
        why: "element clones are shallow (Rc/Cow); error paths",
    },
    Pin {
        file: "eval.rs",
        func: "builtin_len",
        callees: &[".to_string", "format!"],
        disposition: CONST,
        why: "error message",
    },
    Pin {
        file: "eval.rs",
        func: "builtin_lt",
        callees: &[".to_string", "format!"],
        disposition: INPUT_BOUNDED,
        why: "error paths",
    },
    Pin {
        file: "eval.rs",
        func: "builtin_sig",
        callees: &[".clone", ".into_owned", ".to_string", "format!"],
        disposition: INPUT_BOUNDED,
        why: "print-family buffers: every inner write is charged (padding/precision) or an input-bounded copy; escapers ≤ 6×/rune of args",
    },
    Pin {
        file: "eval.rs",
        func: "builtin_slice",
        callees: &[".to_string", ".to_vec", "format!"],
        disposition: INPUT_BOUNDED,
        why: "subslice ≤ input",
    },
    Pin {
        file: "eval.rs",
        func: "coerce_arg_value",
        callees: &["format!"],
        disposition: INPUT_BOUNDED,
        why: "error path",
    },
    Pin {
        file: "eval.rs",
        func: "decode_rune_local",
        callees: &["str::from_utf8"],
        disposition: CONST,
        why: "str::from_utf8 borrows — no allocation",
    },
    Pin {
        file: "eval.rs",
        func: "errorf",
        callees: &["format!"],
        disposition: INPUT_BOUNDED,
        why: "error path; node text ≤ template text",
    },
    Pin {
        file: "eval.rs",
        func: "eval_and_or",
        callees: &["format!"],
        disposition: INPUT_BOUNDED,
        why: "no allocation",
    },
    Pin {
        file: "eval.rs",
        func: "eval_arg",
        callees: &[".clone", "format!"],
        disposition: INPUT_BOUNDED,
        why: "shallow clones of literals/dot; coercion error paths",
    },
    Pin {
        file: "eval.rs",
        func: "eval_args_text",
        callees: &[".clone", ".collect", ".into_owned"],
        disposition: INPUT_BOUNDED,
        why: "html/js/urlquery arg render ≤ Σ value sizes (each charged at production)",
    },
    Pin {
        file: "eval.rs",
        func: "eval_call",
        callees: &[".push", ".to_string", "Vec::with_capacity", "format!"],
        disposition: INPUT_BOUNDED,
        why: "argv ≤ template arg count; values shallow; error paths",
    },
    Pin {
        file: "eval.rs",
        func: "eval_call_builtin",
        callees: &[
            ".into_owned",
            ".push",
            ".to_string",
            "String::from_utf8_lossy",
            "Vec::with_capacity",
            "format!",
        ],
        disposition: INPUT_BOUNDED,
        why: "argv ≤ template arg count; error path renders ≤ template text + one value",
    },
    Pin {
        file: "eval.rs",
        func: "eval_command",
        callees: &[".clone", ".to_string", "format!"],
        disposition: INPUT_BOUNDED,
        why: "shallow dot/literal clones; error paths",
    },
    Pin {
        file: "eval.rs",
        func: "eval_field",
        callees: &[".clone", "format!"],
        disposition: INPUT_BOUNDED,
        why: "error paths; map lookups return borrowed/shared values",
    },
    Pin {
        file: "eval.rs",
        func: "eval_function",
        callees: &["format!"],
        disposition: INPUT_BOUNDED,
        why: "error path (parse-time unreachable)",
    },
    Pin {
        file: "eval.rs",
        func: "eval_ident_as_arg",
        callees: &[".to_string", "format!"],
        disposition: INPUT_BOUNDED,
        why: "error paths",
    },
    Pin {
        file: "eval.rs",
        func: "eval_pipeline",
        callees: &[".clone"],
        disposition: INPUT_BOUNDED,
        why: "shallow Value clones into decls",
    },
    Pin {
        file: "eval.rs",
        func: "html_escape",
        callees: &[".extend_from_slice", ".push", "Vec::with_capacity"],
        disposition: INPUT_BOUNDED,
        why: "≤ 6× input",
    },
    Pin {
        file: "eval.rs",
        func: "ideal_constant",
        callees: &["format!"],
        disposition: INPUT_BOUNDED,
        why: "error path; literal text ≤ template",
    },
    Pin {
        file: "eval.rs",
        func: "index_arg",
        callees: &[".to_string", "format!"],
        disposition: INPUT_BOUNDED,
        why: "error paths",
    },
    Pin {
        file: "eval.rs",
        func: "js_escape",
        callees: &[
            ".extend_from_slice",
            ".push",
            "Vec::with_capacity",
            "write!",
        ],
        disposition: INPUT_BOUNDED,
        why: "≤ 6× input",
    },
    Pin {
        file: "eval.rs",
        func: "label_pairs_sorted_of",
        callees: &[".collect", ".to_vec"],
        disposition: INPUT_BOUNDED,
        why: "≤ 2× the stored label bytes (store data, line-scoped)",
    },
    Pin {
        file: "eval.rs",
        func: "not_a_function",
        callees: &["format!"],
        disposition: INPUT_BOUNDED,
        why: "error path",
    },
    Pin {
        file: "eval.rs",
        func: "push_var",
        callees: &[".push"],
        disposition: INPUT_BOUNDED,
        why: "≤ one entry per template decl per live scope; values counted at production",
    },
    Pin {
        file: "eval.rs",
        func: "render",
        callees: &["format!", "vec!"],
        disposition: CONST,
        why: "one 1-entry var stack",
    },
    Pin {
        file: "eval.rs",
        func: "render_value_for_error",
        callees: &[".into_owned", ".to_string", "String::from_utf8_lossy"],
        disposition: INPUT_BOUNDED,
        why: "error path; one %v render of one value",
    },
    Pin {
        file: "eval.rs",
        func: "set_var",
        callees: &["format!"],
        disposition: INPUT_BOUNDED,
        why: "error path; name ≤ template text",
    },
    Pin {
        file: "eval.rs",
        func: "template_node_string",
        callees: &["format!"],
        disposition: INPUT_BOUNDED,
        why: "error path; ≤ template text",
    },
    Pin {
        file: "eval.rs",
        func: "upsert",
        callees: &[".push", ".to_vec"],
        disposition: INPUT_BOUNDED,
        why: "one error-pair entry",
    },
    Pin {
        file: "eval.rs",
        func: "url_query_escape",
        callees: &[".push", "Vec::with_capacity"],
        disposition: INPUT_BOUNDED,
        why: "≤ 3× input",
    },
    Pin {
        file: "eval.rs",
        func: "validate_type",
        callees: &["format!"],
        disposition: INPUT_BOUNDED,
        why: "error paths",
    },
    Pin {
        file: "eval.rs",
        func: "var_value",
        callees: &[".clone", "format!"],
        disposition: INPUT_BOUNDED,
        why: "Value clone is Rc/Cow-shallow; error path name ≤ template",
    },
    Pin {
        file: "eval.rs",
        func: "walk",
        callees: &[".extend_from_slice"],
        disposition: CHARGED,
        why: "text-node emission pre-charges text.len() per write — the loop-amplification member",
    },
    Pin {
        file: "eval.rs",
        func: "walk_if_or_with",
        callees: &[".clone", "format!"],
        disposition: INPUT_BOUNDED,
        why: "shallow Value clone for the with-dot",
    },
    Pin {
        file: "eval.rs",
        func: "walk_range",
        callees: &[".to_string", "format!"],
        disposition: INPUT_BOUNDED,
        why: "per-iteration shallow clones; emission inside the body is charged by walk/print",
    },
    Pin {
        file: "eval.rs",
        func: "walk_template",
        callees: &[".clone", "format!", "vec!"],
        disposition: INPUT_BOUNDED,
        why: "fresh 1-entry var stack + shallow dot clone per invocation, depth ≤ 250",
    },
    // --------------------------------------------------------------- funcs.rs
    Pin {
        file: "funcs.rs",
        func: "align",
        callees: &[
            ".extend",
            ".extend_from_slice",
            ".to_vec",
            "Vec::with_capacity",
        ],
        disposition: CHARGED,
        why: "pad path charges first; truncation is a boundary scan + ≤-input copy (round-1 ordering fix: no per-rune Vec)",
    },
    Pin {
        file: "funcs.rs",
        func: "b64_decode_go",
        callees: &[".extend_from_slice", "Vec::with_capacity", "format!"],
        disposition: INPUT_BOUNDED,
        why: "≤ 3/4 input",
    },
    Pin {
        file: "funcs.rs",
        func: "coerce_sprig_date",
        callees: &[".clone"],
        disposition: CONST,
        why: "one GoTime",
    },
    Pin {
        file: "funcs.rs",
        func: "compile_regex",
        callees: &[".clone", ".into_owned", "format!"],
        disposition: INPUT_BOUNDED,
        why: "pattern copy ≤ input; regex programs capped by the crate-wide nfa_size_limit",
    },
    Pin {
        file: "funcs.rs",
        func: "decode_rune",
        callees: &["str::from_utf8"],
        disposition: CONST,
        why: "str::from_utf8 borrows — no allocation",
    },
    Pin {
        file: "funcs.rs",
        func: "f!TrimPrefix",
        callees: &[".to_vec"],
        disposition: INPUT_BOUNDED,
        why: "≤ input",
    },
    Pin {
        file: "funcs.rs",
        func: "f!TrimSuffix",
        callees: &[".to_vec"],
        disposition: INPUT_BOUNDED,
        why: "≤ input",
    },
    Pin {
        file: "funcs.rs",
        func: "f!__line__",
        callees: &[".to_vec"],
        disposition: INPUT_BOUNDED,
        why: "one copy of the stored line",
    },
    Pin {
        file: "funcs.rs",
        func: "f!b64enc",
        callees: &[".encode"],
        disposition: CHARGED,
        why: "4/3×len+8 before encode",
    },
    Pin {
        file: "funcs.rs",
        func: "f!bytes",
        callees: &[".into_owned", ".to_string"],
        disposition: INPUT_BOUNDED,
        why: "one copy + error text",
    },
    Pin {
        file: "funcs.rs",
        func: "f!count",
        callees: &[".into_owned"],
        disposition: INPUT_BOUNDED,
        why: "one owned copy of the haystack for the regex API",
    },
    Pin {
        file: "funcs.rs",
        func: "f!default",
        callees: &[".clone"],
        disposition: INPUT_BOUNDED,
        why: "shallow Value clone",
    },
    Pin {
        file: "funcs.rs",
        func: "f!div",
        callees: &[".to_string"],
        disposition: CONST,
        why: "one integer; error text",
    },
    Pin {
        file: "funcs.rs",
        func: "f!duration",
        callees: &[".into_owned"],
        disposition: INPUT_BOUNDED,
        why: "one copy; ParseDuration errors ≤ ~4× input (time-quote)",
    },
    Pin {
        file: "funcs.rs",
        func: "f!duration_seconds",
        callees: &[".into_owned"],
        disposition: INPUT_BOUNDED,
        why: "as f!duration",
    },
    Pin {
        file: "funcs.rs",
        func: "f!mod",
        callees: &[".to_string"],
        disposition: CONST,
        why: "one integer; error text",
    },
    Pin {
        file: "funcs.rs",
        func: "f!regexReplaceAll",
        callees: &[".into_owned"],
        disposition: CHARGED,
        why: "(len+2)×len(repl)+len upper bound charged before replace_all",
    },
    Pin {
        file: "funcs.rs",
        func: "f!regexReplaceAllLiteral",
        callees: &[".into_owned"],
        disposition: CHARGED,
        why: "same bound as regexReplaceAll",
    },
    Pin {
        file: "funcs.rs",
        func: "f!repeat",
        callees: &[".repeat", ".to_string"],
        disposition: CHARGED,
        why: "count×len charged before repeat (the founding member)",
    },
    Pin {
        file: "funcs.rs",
        func: "f!substr",
        callees: &[".to_vec", "format!"],
        disposition: INPUT_BOUNDED,
        why: "≤ input; panic-text error paths",
    },
    Pin {
        file: "funcs.rs",
        func: "f!toDateInZone",
        callees: &[".into_owned", ".to_vec"],
        disposition: INPUT_BOUNDED,
        why: "layout/value copies ≤ inputs",
    },
    Pin {
        file: "funcs.rs",
        func: "f!trimPrefix",
        callees: &[".to_vec"],
        disposition: INPUT_BOUNDED,
        why: "≤ input",
    },
    Pin {
        file: "funcs.rs",
        func: "f!trimSuffix",
        callees: &[".to_vec"],
        disposition: INPUT_BOUNDED,
        why: "≤ input",
    },
    Pin {
        file: "funcs.rs",
        func: "f!trunc",
        callees: &[".to_vec"],
        disposition: INPUT_BOUNDED,
        why: "≤ input",
    },
    Pin {
        file: "funcs.rs",
        func: "f!unixEpoch",
        callees: &[".to_string"],
        disposition: CONST,
        why: "one integer",
    },
    Pin {
        file: "funcs.rs",
        func: "f!unixEpochMillis",
        callees: &[".to_string"],
        disposition: CONST,
        why: "one integer",
    },
    Pin {
        file: "funcs.rs",
        func: "f!unixEpochNanos",
        callees: &[".to_string"],
        disposition: CONST,
        why: "one integer",
    },
    Pin {
        file: "funcs.rs",
        func: "f!unixToTime",
        callees: &[".into_owned"],
        disposition: INPUT_BOUNDED,
        why: "one copy",
    },
    Pin {
        file: "funcs.rs",
        func: "from_json",
        callees: &["str::from_utf8"],
        disposition: CHARGED,
        why: "caller (f!fromJson) charges the 32× ceiling",
    },
    Pin {
        file: "funcs.rs",
        func: "go_parse_duration",
        callees: &[".into_owned", "String::from_utf8_lossy", "format!"],
        disposition: INPUT_BOUNDED,
        why: "error texts via time-quote ≤ ~4× input",
    },
    Pin {
        file: "funcs.rs",
        func: "go_parse_float",
        callees: &[".collect"],
        disposition: INPUT_BOUNDED,
        why: "≤ input",
    },
    Pin {
        file: "funcs.rs",
        func: "go_parse_int_base0",
        callees: &[".clone", ".collect", ".to_string"],
        disposition: INPUT_BOUNDED,
        why: "≤ input",
    },
    Pin {
        file: "funcs.rs",
        func: "go_replace",
        callees: &[".extend_from_slice", ".to_vec", "Vec::with_capacity"],
        disposition: CHARGED,
        why: "charges len+n×len(new) BEFORE with_capacity",
    },
    Pin {
        file: "funcs.rs",
        func: "go_rune_lower",
        callees: &[".to_lowercase"],
        disposition: CONST,
        why: "one rune",
    },
    Pin {
        file: "funcs.rs",
        func: "go_rune_upper",
        callees: &[".to_uppercase"],
        disposition: CONST,
        why: "one rune",
    },
    Pin {
        file: "funcs.rs",
        func: "go_title",
        callees: &[".extend_from_slice", "Vec::with_capacity"],
        disposition: INPUT_BOUNDED,
        why: "≤ 4× input; caller charges",
    },
    Pin {
        file: "funcs.rs",
        func: "go_trim",
        callees: &[".to_vec"],
        disposition: INPUT_BOUNDED,
        why: "≤ input",
    },
    Pin {
        file: "funcs.rs",
        func: "go_trim_space",
        callees: &[".to_vec"],
        disposition: INPUT_BOUNDED,
        why: "≤ input",
    },
    Pin {
        file: "funcs.rs",
        func: "indent_impl",
        callees: &[
            ".extend_from_slice",
            ".push",
            ".to_string",
            "Vec::with_capacity",
            "vec!",
        ],
        disposition: CHARGED,
        why: "charges pad×lines+len+1 BEFORE the pad vec",
    },
    Pin {
        file: "funcs.rs",
        func: "json_to_value",
        callees: &[".collect"],
        disposition: CHARGED,
        why: "covered by f!fromJson's charge",
    },
    Pin {
        file: "funcs.rs",
        func: "lossy",
        callees: &["String::from_utf8_lossy"],
        disposition: INPUT_BOUNDED,
        why: "borrowed Cow; <= 3x input only for invalid UTF-8 replacement",
    },
    Pin {
        file: "funcs.rs",
        func: "map_runes",
        callees: &[".extend_from_slice", "Vec::with_capacity"],
        disposition: INPUT_BOUNDED,
        why: "≤ 4× input; every caller charges 4×len first",
    },
    Pin {
        file: "funcs.rs",
        func: "query_escape",
        callees: &[".push", "Vec::with_capacity"],
        disposition: INPUT_BOUNDED,
        why: "≤ 3× input; f!urlencode charges",
    },
    Pin {
        file: "funcs.rs",
        func: "query_unescape",
        callees: &[".push", "Vec::with_capacity", "format!"],
        disposition: INPUT_BOUNDED,
        why: "≤ input; error text ≤ 3 bytes quoted",
    },
    Pin {
        file: "funcs.rs",
        func: "registry_names",
        callees: &[".collect"],
        disposition: CONST,
        why: "67 static names (test-facing)",
    },
    Pin {
        file: "funcs.rs",
        func: "trim_decimal",
        callees: &["format!"],
        disposition: INPUT_BOUNDED,
        why: "≤ input + 1",
    },
    Pin {
        file: "funcs.rs",
        func: "unix_to_time",
        callees: &["format!"],
        disposition: INPUT_BOUNDED,
        why: "error texts ≤ ~11× input via strconv quote of a short epoch string",
    },
    // --------------------------------------------------------------- gofmt.rs
    Pin {
        file: "gofmt.rs",
        func: "append_escaped_rune",
        callees: &[".push", ".push_str", "write!"],
        disposition: CONST,
        why: "≤ 10 bytes/rune",
    },
    Pin {
        file: "gofmt.rs",
        func: "bad_arg_num",
        callees: &[".extend_from_slice"],
        disposition: CONST,
        why: "fixed text",
    },
    Pin {
        file: "gofmt.rs",
        func: "bad_verb",
        callees: &[".extend_from_slice", ".push"],
        disposition: INPUT_BOUNDED,
        why: "one %v re-print of one value",
    },
    Pin {
        file: "gofmt.rs",
        func: "can_backquote",
        callees: &["str::from_utf8"],
        disposition: CONST,
        why: "str::from_utf8 borrows — no allocation",
    },
    Pin {
        file: "gofmt.rs",
        func: "decimal_digits",
        callees: &[".collect", "format!"],
        disposition: CONST,
        why: "significant digits capped at 800 (exact-expansion width)",
    },
    Pin {
        file: "gofmt.rs",
        func: "decode_rune",
        callees: &["str::from_utf8"],
        disposition: CONST,
        why: "str::from_utf8 borrows — no allocation",
    },
    Pin {
        file: "gofmt.rs",
        func: "dispatch_bool",
        callees: &[".to_vec"],
        disposition: CONST,
        why: "true/false",
    },
    Pin {
        file: "gofmt.rs",
        func: "dispatch_bytes",
        callees: &[".extend_from_slice", ".push"],
        disposition: INPUT_BOUNDED,
        why: "≤ 4×len decimal walk",
    },
    Pin {
        file: "gofmt.rs",
        func: "dispatch_complex",
        callees: &[".extend_from_slice", ".push"],
        disposition: INPUT_BOUNDED,
        why: "two float renders (each charged via dispatch_float)",
    },
    Pin {
        file: "gofmt.rs",
        func: "dispatch_float",
        callees: &[".extend_from_slice", ".insert", ".push", ".to_vec"],
        disposition: CHARGED,
        why: "charges a PRESENT precision (+32) before any digit rendering — round-1 miss 1",
    },
    Pin {
        file: "gofmt.rs",
        func: "do_printf",
        callees: &[".extend_from_slice", ".push"],
        disposition: INPUT_BOUNDED,
        why: "format-text copies ≤ template text; args via charged dispatchers; EXTRA renders ≤ Σ arg sizes",
    },
    Pin {
        file: "gofmt.rs",
        func: "field_sep",
        callees: &[".extend_from_slice", ".push"],
        disposition: CONST,
        why: "separators",
    },
    Pin {
        file: "gofmt.rs",
        func: "fmt_c",
        callees: &[".to_vec"],
        disposition: CONST,
        why: "one rune",
    },
    Pin {
        file: "gofmt.rs",
        func: "fmt_e",
        callees: &[".extend_from_slice", ".push", ".to_string"],
        disposition: CHARGED,
        why: "fraction padding ≤ prec, under dispatch_float's charge",
    },
    Pin {
        file: "gofmt.rs",
        func: "fmt_f_from_digits",
        callees: &[".extend", ".extend_from_slice", ".push"],
        disposition: CONST,
        why: "shortest digits + |exp| ≤ ~1KB (shortest paths only)",
    },
    Pin {
        file: "gofmt.rs",
        func: "fmt_integer",
        callees: &[".to_vec", "vec!"],
        disposition: CHARGED,
        why: "heap digit buffer (needed > 96) charges BEFORE vec![0; needed]",
    },
    Pin {
        file: "gofmt.rs",
        func: "fmt_pointer",
        callees: &[".extend_from_slice", ".push"],
        disposition: INPUT_BOUNDED,
        why: "pinned-address renders + one deref re-print",
    },
    Pin {
        file: "gofmt.rs",
        func: "fmt_q",
        callees: &[".extend_from_slice", ".push", "Vec::with_capacity"],
        disposition: CHARGED,
        why: "charges 10×len+2 (quote expansion ceiling) before quoting",
    },
    Pin {
        file: "gofmt.rs",
        func: "fmt_sbx",
        callees: &[".push"],
        disposition: INPUT_BOUNDED,
        why: "2×min(len,prec) + separators; width via write_padding's charge",
    },
    Pin {
        file: "gofmt.rs",
        func: "fmt_unicode",
        callees: &[".push", ".push_str", "String::with_capacity", "format!"],
        disposition: CHARGED,
        why: "charges prec+8 BEFORE the manual zero padding (std fmt would panic past 65535 — round-1 miss 1's sibling)",
    },
    Pin {
        file: "gofmt.rs",
        func: "format_float_b",
        callees: &[".extend_from_slice", ".push", ".to_string"],
        disposition: CONST,
        why: "mantissa + exponent",
    },
    Pin {
        file: "gofmt.rs",
        func: "format_float_go",
        callees: &[".extend", ".to_vec", "format!"],
        disposition: CHARGED,
        why: "large precisions render exact-expansion (≤1074/800 digits) + zero padding, both under dispatch_float's precision charge",
    },
    Pin {
        file: "gofmt.rs",
        func: "format_float_hex",
        callees: &[
            ".collect",
            ".extend",
            ".extend_from_slice",
            ".push",
            ".to_string",
        ],
        disposition: CHARGED,
        why: "prec nibbles under dispatch_float's charge; shortest ≤ 13 nibbles",
    },
    Pin {
        file: "gofmt.rs",
        func: "missing_arg",
        callees: &[".extend_from_slice"],
        disposition: CONST,
        why: "fixed text",
    },
    Pin {
        file: "gofmt.rs",
        func: "pad",
        callees: &[".extend_from_slice"],
        disposition: INPUT_BOUNDED,
        why: "content ≤ produced value; padding via write_padding's charge",
    },
    Pin {
        file: "gofmt.rs",
        func: "print_arg",
        callees: &[".to_string"],
        disposition: INPUT_BOUNDED,
        why: "type-name literal",
    },
    Pin {
        file: "gofmt.rs",
        func: "print_byte_slice_walk",
        callees: &[".extend_from_slice", ".push"],
        disposition: INPUT_BOUNDED,
        why: "per-element badVerb ≤ ~24×len",
    },
    Pin {
        file: "gofmt.rs",
        func: "print_field_name",
        callees: &[".extend_from_slice", ".push"],
        disposition: CONST,
        why: "field names",
    },
    Pin {
        file: "gofmt.rs",
        func: "print_location_struct",
        callees: &[".clone", ".extend_from_slice", ".push"],
        disposition: CONST,
        why: "pinned-empty tables (ledgered)",
    },
    Pin {
        file: "gofmt.rs",
        func: "print_map",
        callees: &[".clone", ".extend_from_slice", ".push"],
        disposition: INPUT_BOUNDED,
        why: "≤ 2× map bytes (store/charged data)",
    },
    Pin {
        file: "gofmt.rs",
        func: "print_struct_open",
        callees: &[".extend_from_slice", ".push"],
        disposition: CONST,
        why: "type names",
    },
    Pin {
        file: "gofmt.rs",
        func: "print_value",
        callees: &[".clone", ".collect", ".extend_from_slice", ".push"],
        disposition: INPUT_BOUNDED,
        why: "walks an existing value; map/list entry copies ≤ value size (charged at production / store-bounded)",
    },
    Pin {
        file: "gofmt.rs",
        func: "push_char",
        callees: &[".extend_from_slice"],
        disposition: CONST,
        why: "one rune",
    },
    Pin {
        file: "gofmt.rs",
        func: "quote_rune",
        callees: &[".push", "String::with_capacity"],
        disposition: CONST,
        why: "one rune",
    },
    Pin {
        file: "gofmt.rs",
        func: "quote_with",
        callees: &[".push", "String::with_capacity", "str::from_utf8", "write!"],
        disposition: INPUT_BOUNDED,
        why: "≤ 10× input",
    },
    Pin {
        file: "gofmt.rs",
        func: "rune_len",
        callees: &["str::from_utf8"],
        disposition: CONST,
        why: "str::from_utf8 borrows — no allocation",
    },
    Pin {
        file: "gofmt.rs",
        func: "sprint",
        callees: &[".push"],
        disposition: INPUT_BOUNDED,
        why: "separators; values via print_arg (each charged/bounded)",
    },
    Pin {
        file: "gofmt.rs",
        func: "sprintln",
        callees: &[".push"],
        disposition: INPUT_BOUNDED,
        why: "as sprint",
    },
    Pin {
        file: "gofmt.rs",
        func: "write_padding",
        callees: &[".extend"],
        disposition: CHARGED,
        why: "charges n BEFORE the pad write (widths are caller-controlled)",
    },
    Pin {
        file: "gofmt.rs",
        func: "write_template_value",
        callees: &[".extend_from_slice"],
        disposition: INPUT_BOUNDED,
        why: "<no value> literal; value prints via print_arg",
    },
    // --------------------------------------------------------------- golayout.rs
    Pin {
        file: "golayout.rs",
        func: "append_int",
        callees: &[".extend_from_slice", ".push", ".to_string"],
        disposition: CONST,
        why: "layout widths are 0..4",
    },
    Pin {
        file: "golayout.rs",
        func: "append_nano",
        callees: &[".push"],
        disposition: CONST,
        why: "≤ 9 digits",
    },
    Pin {
        file: "golayout.rs",
        func: "atoi",
        callees: &["str::from_utf8"],
        disposition: CONST,
        why: "no allocation",
    },
    Pin {
        file: "golayout.rs",
        func: "format_layout",
        callees: &[".extend_from_slice", ".push", "Vec::with_capacity"],
        disposition: INPUT_BOUNDED,
        why: "≤ ~10× layout text (longest token expansion is a month name)",
    },
    Pin {
        file: "golayout.rs",
        func: "parse_in_location",
        callees: &[
            ".clone",
            ".into_owned",
            ".to_vec",
            "String::from_utf8_lossy",
        ],
        disposition: INPUT_BOUNDED,
        why: "zone-name copy ≤ input",
    },
    // --------------------------------------------------------------- lex.rs
    Pin {
        file: "lex.rs",
        func: "display",
        callees: &[".clone", ".collect", ".to_string", "format!"],
        disposition: COMPILE_TIME,
        why: "token display in parse errors",
    },
    Pin {
        file: "lex.rs",
        func: "go_char_u",
        callees: &["format!"],
        disposition: COMPILE_TIME,
        why: "one rune",
    },
    Pin {
        file: "lex.rs",
        func: "lex_char",
        callees: &[".to_string"],
        disposition: COMPILE_TIME,
        why: "error texts",
    },
    Pin {
        file: "lex.rs",
        func: "lex_comment",
        callees: &[".to_string"],
        disposition: COMPILE_TIME,
        why: "error texts",
    },
    Pin {
        file: "lex.rs",
        func: "lex_field_or_variable",
        callees: &["format!"],
        disposition: COMPILE_TIME,
        why: "error texts",
    },
    Pin {
        file: "lex.rs",
        func: "lex_identifier",
        callees: &["format!"],
        disposition: COMPILE_TIME,
        why: "error texts",
    },
    Pin {
        file: "lex.rs",
        func: "lex_inside_action",
        callees: &[".to_string", "format!"],
        disposition: COMPILE_TIME,
        why: "error texts",
    },
    Pin {
        file: "lex.rs",
        func: "lex_number",
        callees: &["format!"],
        disposition: COMPILE_TIME,
        why: "error texts",
    },
    Pin {
        file: "lex.rs",
        func: "lex_quote",
        callees: &[".to_string"],
        disposition: COMPILE_TIME,
        why: "error texts",
    },
    Pin {
        file: "lex.rs",
        func: "lex_raw_quote",
        callees: &[".to_string"],
        disposition: COMPILE_TIME,
        why: "error texts",
    },
    Pin {
        file: "lex.rs",
        func: "make_item",
        callees: &[".to_string"],
        disposition: COMPILE_TIME,
        why: "token text ≤ template text",
    },
    // --------------------------------------------------------------- methods.rs
    Pin {
        file: "methods.rs",
        func: "arg_bytes",
        callees: &[".clone", ".into_owned"],
        disposition: INPUT_BOUNDED,
        why: "one copy of a caller-built []byte",
    },
    Pin {
        file: "methods.rs",
        func: "marshal_binary",
        callees: &[".push", ".to_string", "vec!"],
        disposition: CONST,
        why: "15-16 bytes",
    },
    Pin {
        file: "methods.rs",
        func: "marshal_text",
        callees: &["format!"],
        disposition: CONST,
        why: "RFC3339 ≤ 40B",
    },
    Pin {
        file: "methods.rs",
        func: "strict_rfc3339",
        callees: &[".to_string"],
        disposition: CONST,
        why: "error texts",
    },
    Pin {
        file: "methods.rs",
        func: "time_method",
        callees: &[
            ".clone",
            ".extend_from_slice",
            ".into_owned",
            ".push",
            ".to_string",
            "format!",
            "vec!",
        ],
        disposition: CONST,
        why: "method outputs are ≤ ~64B or layout-bounded (Format via format_layout)",
    },
    // --------------------------------------------------------------- mod.rs
    Pin {
        file: "mod.rs",
        func: "charge",
        callees: &["format!"],
        disposition: CHARGED,
        why: "the ledger; breach message is CONST",
    },
    Pin {
        file: "mod.rs",
        func: "compile",
        callees: &[".clone", ".insert", ".to_string", "format!"],
        disposition: COMPILE_TIME,
        why: "tree/fast-path derivation ≤ template text",
    },
    Pin {
        file: "mod.rs",
        func: "derive_parts",
        callees: &[".clone", ".push", "Vec::with_capacity"],
        disposition: COMPILE_TIME,
        why: "≤ template text",
    },
    Pin {
        file: "mod.rs",
        func: "scan_pipe",
        callees: &[".clone", ".push", "String::from_utf8"],
        disposition: COMPILE_TIME,
        why: "literal regex list ≤ template text",
    },
    // --------------------------------------------------------------- parse.rs
    Pin {
        file: "parse.rs",
        func: "action",
        callees: &[".to_string"],
        disposition: COMPILE_TIME,
        why: "no allocation",
    },
    Pin {
        file: "parse.rs",
        func: "add_define",
        callees: &[".push", "format!"],
        disposition: COMPILE_TIME,
        why: "define list ≤ template text",
    },
    Pin {
        file: "parse.rs",
        func: "backup",
        callees: &[".push"],
        disposition: COMPILE_TIME,
        why: "≤ 3 tokens",
    },
    Pin {
        file: "parse.rs",
        func: "block_control",
        callees: &[".clone", "format!"],
        disposition: COMPILE_TIME,
        why: "≤ template text",
    },
    Pin {
        file: "parse.rs",
        func: "check_pipeline",
        callees: &["format!"],
        disposition: COMPILE_TIME,
        why: "error texts",
    },
    Pin {
        file: "parse.rs",
        func: "command",
        callees: &[".push", ".to_string"],
        disposition: COMPILE_TIME,
        why: "arg lists ≤ template text",
    },
    Pin {
        file: "parse.rs",
        func: "item_list",
        callees: &[".push", ".to_string"],
        disposition: COMPILE_TIME,
        why: "via text_or_action",
    },
    Pin {
        file: "parse.rs",
        func: "operand",
        callees: &[".extend", ".push", ".to_string", "format!"],
        disposition: COMPILE_TIME,
        why: "chained idents ≤ template text",
    },
    Pin {
        file: "parse.rs",
        func: "parse",
        callees: &[".to_string", "vec!"],
        disposition: COMPILE_TIME,
        why: "parser state",
    },
    Pin {
        file: "parse.rs",
        func: "parse_control_inner",
        callees: &["format!", "vec!"],
        disposition: COMPILE_TIME,
        why: "node lists ≤ template text",
    },
    Pin {
        file: "parse.rs",
        func: "parse_definition",
        callees: &["format!"],
        disposition: COMPILE_TIME,
        why: "error texts",
    },
    Pin {
        file: "parse.rs",
        func: "parse_number",
        callees: &["format!"],
        disposition: COMPILE_TIME,
        why: "≤ template text",
    },
    Pin {
        file: "parse.rs",
        func: "parse_root",
        callees: &[".push", "format!"],
        disposition: COMPILE_TIME,
        why: "error texts",
    },
    Pin {
        file: "parse.rs",
        func: "parse_template_name",
        callees: &[".to_string", "String::from_utf8"],
        disposition: COMPILE_TIME,
        why: "≤ template text",
    },
    Pin {
        file: "parse.rs",
        func: "peek",
        callees: &[".clone"],
        disposition: COMPILE_TIME,
        why: "one token",
    },
    Pin {
        file: "parse.rs",
        func: "peek_non_space",
        callees: &[".clone"],
        disposition: COMPILE_TIME,
        why: "one token",
    },
    Pin {
        file: "parse.rs",
        func: "pipeline",
        callees: &[".clone", ".push", ".to_string", "format!", "vec!"],
        disposition: COMPILE_TIME,
        why: "decl/cmd lists ≤ template text",
    },
    Pin {
        file: "parse.rs",
        func: "render",
        callees: &[".push", ".push_str"],
        disposition: COMPILE_TIME,
        why: "node text ≤ template text (also used on exec error paths — INPUT_BOUNDED there)",
    },
    Pin {
        file: "parse.rs",
        func: "term",
        callees: &[".to_string", "format!", "vec!"],
        disposition: COMPILE_TIME,
        why: "error texts",
    },
    Pin {
        file: "parse.rs",
        func: "unexpected",
        callees: &["format!"],
        disposition: COMPILE_TIME,
        why: "error texts",
    },
    Pin {
        file: "parse.rs",
        func: "unquote_string",
        callees: &[
            ".collect",
            ".extend_from_slice",
            ".push",
            ".to_string",
            "Vec::with_capacity",
        ],
        disposition: COMPILE_TIME,
        why: "≤ template text",
    },
    Pin {
        file: "parse.rs",
        func: "use_var",
        callees: &[".to_string", "format!", "vec!"],
        disposition: COMPILE_TIME,
        why: "≤ template text",
    },
    // --------------------------------------------------------------- timefns.rs
    Pin {
        file: "timefns.rs",
        func: "add",
        callees: &[".clone"],
        disposition: CONST,
        why: "one GoTime",
    },
    Pin {
        file: "timefns.rs",
        func: "add_date",
        callees: &[".clone"],
        disposition: CONST,
        why: "one GoTime",
    },
    Pin {
        file: "timefns.rs",
        func: "duration_string",
        callees: &[".into_owned", ".to_string", "String::from_utf8_lossy"],
        disposition: CONST,
        why: "≤ 32B",
    },
    Pin {
        file: "timefns.rs",
        func: "go_string",
        callees: &[".to_string", "format!"],
        disposition: CONST,
        why: "≤ ~80B",
    },
    Pin {
        file: "timefns.rs",
        func: "loc_pointer",
        callees: &[".clone"],
        disposition: CONST,
        why: "one GoLoc",
    },
    Pin {
        file: "timefns.rs",
        func: "location_name",
        callees: &[".clone", ".to_string"],
        disposition: CONST,
        why: "zone name",
    },
    Pin {
        file: "timefns.rs",
        func: "month_string",
        callees: &[".to_string", "format!"],
        disposition: CONST,
        why: "name or %!Month(n)",
    },
    Pin {
        file: "timefns.rs",
        func: "process",
        callees: &[".to_string"],
        disposition: CONST,
        why: "zone names, once per compile",
    },
    Pin {
        file: "timefns.rs",
        func: "round",
        callees: &[".clone"],
        disposition: CONST,
        why: "one GoTime",
    },
    Pin {
        file: "timefns.rs",
        func: "string",
        callees: &[".into_owned", "String::from_utf8_lossy"],
        disposition: CONST,
        why: "one layout render (≤ ~40B)",
    },
    Pin {
        file: "timefns.rs",
        func: "truncate",
        callees: &[".clone"],
        disposition: CONST,
        why: "one GoTime",
    },
    Pin {
        file: "timefns.rs",
        func: "tz_zone_at",
        callees: &[".to_string"],
        disposition: CONST,
        why: "abbreviation",
    },
    Pin {
        file: "timefns.rs",
        func: "weekday_string",
        callees: &[".to_string", "format!"],
        disposition: CONST,
        why: "name or %!Weekday(n)",
    },
    Pin {
        file: "timefns.rs",
        func: "zone_abbrev",
        callees: &[".to_string", "format!"],
        disposition: CONST,
        why: "zone abbreviation",
    },
    Pin {
        file: "timefns.rs",
        func: "zone_at",
        callees: &[".clone", ".to_string"],
        disposition: CONST,
        why: "abbreviation",
    },
    // --------------------------------------------------------------- value.rs
    Pin {
        file: "value.rs",
        func: "into_owned",
        callees: &[".clone", ".collect", ".into_owned"],
        disposition: INPUT_BOUNDED,
        why: "one deep copy of an existing (charged/bounded) value; not on the hot path",
    },
    Pin {
        file: "value.rs",
        func: "str_owned",
        callees: &[".into"],
        disposition: INPUT_BOUNDED,
        why: "factor-1 copy of the caller's argument (Into<Vec<u8>>); amplification needs an in-vocabulary loop at the caller",
    },
];

#[test]
fn every_template_allocation_site_is_classified() {
    let found = census();
    let mut pinned: CensusMap = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for p in PINS {
        assert!(
            seen.insert((p.file, p.func)),
            "duplicate pin {}::{}",
            p.file,
            p.func
        );
        // Every pin carries a disposition from the closed set plus a
        // stated bound — a re-pin with a typo'd or empty disposition is
        // itself a census failure.
        assert!(
            [CHARGED, INPUT_BOUNDED, CONST, COMPILE_TIME, "RESIDUAL"].contains(&p.disposition),
            "{}::{}: unknown disposition {:?}",
            p.file,
            p.func,
            p.disposition
        );
        assert!(!p.why.is_empty(), "{}::{}: empty why", p.file, p.func);
        pinned.insert(
            (p.file.to_string(), p.func.to_string()),
            p.callees.iter().map(|s| s.to_string()).collect(),
        );
    }
    // Functions with NO vocabulary hits need no pin; drop empty pins
    // from the comparison the same way (they document zero-alloc fns).
    let found: CensusMap = found.into_iter().filter(|(_, v)| !v.is_empty()).collect();
    let pinned: CensusMap = pinned.into_iter().filter(|(_, v)| !v.is_empty()).collect();
    if found != pinned {
        let mut msg = String::new();
        for (k, v) in &found {
            if pinned.get(k) != Some(v) {
                let _ = writeln!(
                    msg,
                    "NEW/CHANGED {k:?}: {v:?} (pinned: {:?})",
                    pinned.get(k)
                );
            }
        }
        for (k, v) in &pinned {
            if !found.contains_key(k) {
                let _ = writeln!(msg, "STALE PIN {k:?}: {v:?}");
            }
        }
        panic!(
            "template allocation census drifted — classify each new site \
             (CHARGED / INPUT_BOUNDED / CONST / COMPILE_TIME / RESIDUAL) \
             against \"can a caller-controlled input make this allocation \
             large?\" and update the pin in the same change:\n{msg}"
        );
    }
}

/// Maintenance generator: prints the discovered census for re-pinning.
#[test]
#[ignore = "generator: prints the discovered census table"]
fn zz_print_census() {
    for ((file, func), callees) in census() {
        if callees.is_empty() {
            continue;
        }
        println!("{file} :: {func} :: {callees:?}");
    }
}
