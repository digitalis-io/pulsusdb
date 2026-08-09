//! Issue #230 review rounds 1–3: the allocation-site DRIFT TRIPWIRE
//! over `src/logql/template/`.
//!
//! **What this census IS (round-3 adjudication — the same demotion
//! #236 and #272 made): a drift tripwire, not the completeness or
//! dominance argument.** It is a syntactic AST walk over a curated
//! vocabulary: it detects NEW or CHANGED allocation sites (including
//! inside the `f!`/`def!`/`sig!` macro tables, where a text grep is
//! blind) and forces every one to be re-classified with its
//! disposition's checkable side-conditions. It cannot prove that a
//! charge DOMINATES an allocation (dominance is a control-flow
//! property — the round-3 `go_replace` identity branches contained a
//! charge yet copied before it on three paths), and its vocabulary is
//! curated, so an allocator spelled a new way escapes it until pinned.
//! **The dominance and ordering evidence lives in the RUNTIME gate**
//! (`logql_template_alloc_gate.rs`): per-function, per-BRANCH-shape
//! allocated-vs-charged dominance plus a near-exhausted-budget
//! ordering leg that fails any charge moved after its allocation
//! (mutation-verified).
//!
//! **The classifying question** (coordinator ruling): *can a
//! caller-controlled input make this allocation large?* — NOT "is it a
//! multiplier function". Control-flow output accumulation (`range` over
//! an int repeating a text node) is a first-class member of the class.
//!
//! **Round 2 (the fifth amplification class):** a disposition is now a
//! CLAIM WITH EVIDENCE, not a spelling. The round-1 census verified
//! that each label was a legal word and then discarded it — so
//! `f!__line__` sat labelled `INPUT_BOUNDED` with no charge behind it,
//! and per-call copies that a `range`/variable-only body repeats (or a
//! `$a = printf "%s%s" $a $a` chain COMPOUNDS) were invisible. Now:
//!
//! - `CHARGED` — the census asserts the scope itself contains a
//!   `charge(...)` call (charge-before-allocate, in-function);
//! - `CHARGED_VIA` — the charge lives in a caller; the census verifies
//!   the [`VIA`] table: the discovered caller set matches EXACTLY, and
//!   every listed charging caller transitively reaches an in-scope
//!   charge (a NEW caller of an allocating helper fails the census
//!   until classified);
//! - `SINK` — gofmt.rs printer internals that write only into the
//!   printer's `out` buffer, whose cumulative growth is charged at the
//!   emission boundary (`print_value_go`'s value ceiling, the
//!   print-family builtins' pre-charges, `write_padding`'s own charge);
//!   per-call heap temporaries are CONST-bounded. File-checked to
//!   gofmt.rs; runtime-checked by the engine breach gates.
//! - `VALUE_COPY` — a ≤1× copy of an already-charged / store-bounded /
//!   template-text value, freed within the iteration that made it (or
//!   retained at most once per declaration site). It cannot compound:
//!   a copy never exceeds its source, and every size-INCREASING
//!   producer charges at production.
//! - `TRANSIENT` — a ≤ small-constant × input scratch freed by return,
//!   whose RESULT is a scalar (int/float/time/bool): nothing is
//!   retained, so repetition burns CPU exactly like the reference but
//!   cannot grow memory.
//! - `ERROR_PATH` — allocates only while constructing an error that
//!   aborts the render (every `Err` propagates out of the walk), so it
//!   runs at most once per render and is bounded by one value render +
//!   template text.
//! - `CONST` — fixed small size (≤ a few KiB), independent of inputs;
//! - `COMPILE_TIME` — allocated once per query at template compile,
//!   bounded by the query text the API already caps — never per row.
//!   File-checked: only lex.rs / parse.rs / mod.rs host compile paths.
//! - `RESIDUAL` — cannot be charged; named with its bound and reason.
//!   There are zero RESIDUAL entries.
//!
//! Allocating CONSTRUCTORS are IN the vocabulary (round-2 finding):
//! every `X::new(..)` call is recorded (`Regex::new` compiles a
//! caller-sized program; `Rc::new`/`Box::new` are factor-1
//! moves-to-heap) except the zero-capacity constructors
//! (`Vec`/`String`/`HashMap`/`Cell`), which allocate nothing until a
//! growth call that is itself in vocabulary. Round 3 added the two
//! escapees the reviewer named — `serde_json::from_slice` and
//! `RegexBuilder::build` — as tripwire entries; the vocabulary stays
//! curated by nature, which is exactly why the census is not the
//! completeness argument.
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
    // A builder's `.build()` allocates the built artifact
    // (`RegexBuilder::build` compiles a caller-sized program) — round 3.
    "build",
    // `impl Into<Vec<u8>>` copies borrowed inputs at factor 1.
    "into",
];
const PATH_VOCAB: &[&str] = &[
    "with_capacity",
    "from_utf8",
    "from_utf8_lossy",
    "new",
    // `serde_json::from_slice` builds a caller-sized parse tree — round 3.
    "from_slice",
];
const MACRO_VOCAB: &[&str] = &["vec", "format", "write"];

/// `X::new` constructors that provably allocate NOTHING at the call
/// (zero-capacity containers; `Cell` is a plain wrapper). Growth is
/// covered by the push/extend/insert vocabulary. Everything else —
/// `Regex::new`, `Rc::new`, `Box::new`, `Lexer::new`, `P::new`, … — is
/// recorded and must be pinned (round-2 finding: allocating
/// constructors were excluded from the vocabulary outright).
const ZERO_ALLOC_NEW: &[&str] = &["Vec", "String", "HashMap", "Cell"];

/// Macros whose bodies MUST parse and be visited (the closure tables).
const DESCEND_MACROS: &[&str] = &["f", "def", "sig", "vec", "format", "write", "matches"];

/// Calls counted as charge evidence: the two `RenderBudget` PRIMITIVES
/// plus the two State wrappers whose whole body is a charge (the test
/// asserts each wrapper scope really contains a `charge` call).
const CHARGE_FNS: &[&str] = &[
    "charge",
    // Issue #260: `RenderBudget::charge_retained` is the ledger
    // operation itself — `charge` is its message-building twin, so the
    // two share one countdown and either is real charge evidence.
    "charge_retained",
    "charge_print_family",
    "charge_escaper",
];

/// The [`CHARGE_FNS`] that ARE the ledger operation rather than wrappers
/// around another charge. Exempt from the wrapper assertion below, which
/// would otherwise demand that a primitive call itself.
const PRIMITIVE_CHARGE_FNS: &[&str] = &["charge", "charge_retained"];

type CensusMap = BTreeMap<(String, String), BTreeSet<String>>;

struct Census {
    file: String,
    scope: Vec<String>,
    out: CensusMap,
    /// Scopes containing a `charge(...)` call — the EVIDENCE behind a
    /// `CHARGED` pin (round 2: a disposition must be true, not merely a
    /// legal word).
    charged_scopes: BTreeSet<(String, String)>,
    /// Call edges INTO the `VIA` targets: target function name → the
    /// scopes that call it. A new caller of an allocating helper fails
    /// the census until it is classified.
    edges: BTreeMap<String, BTreeSet<(String, String)>>,
}

impl Census {
    fn cur_scope(&self) -> (String, String) {
        (
            self.file.clone(),
            self.scope
                .last()
                .cloned()
                .unwrap_or_else(|| "<module>".to_string()),
        )
    }

    fn record(&mut self, callee: String) {
        let scope = self.cur_scope();
        self.out.entry(scope).or_default().insert(callee);
    }

    fn record_call_name(&mut self, name: &str) {
        // `charge_print_family`/`charge_escaper` are State wrappers
        // whose bodies charge — counted as charge evidence; the test
        // asserts the wrappers themselves contain a real `charge` call.
        if CHARGE_FNS.contains(&name) {
            let scope = self.cur_scope();
            self.charged_scopes.insert(scope);
        }
        if VIA.iter().any(|v| v.func == name) {
            let scope = self.cur_scope();
            self.edges
                .entry(name.to_string())
                .or_default()
                .insert(scope);
        }
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
        self.record_call_name(&name);
        syn::visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let syn::Expr::Path(p) = &*node.func
            && let Some(seg) = p.path.segments.last()
        {
            let name = seg.ident.to_string();
            let qualifier = p
                .path
                .segments
                .iter()
                .rev()
                .nth(1)
                .map(|s| s.ident.to_string())
                .unwrap_or_default();
            if name == "new" {
                // Allocating constructors are IN the vocabulary
                // (round-2 finding); only the zero-capacity
                // constructors are exempt.
                if !ZERO_ALLOC_NEW.contains(&qualifier.as_str()) {
                    self.record(format!("{qualifier}::new"));
                }
            } else if PATH_VOCAB.contains(&name.as_str()) {
                self.record(format!("{qualifier}::{name}"));
            }
            self.record_call_name(&name);
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

fn census() -> Census {
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
        charged_scopes: BTreeSet::new(),
        edges: BTreeMap::new(),
    };
    for f in files {
        let text = std::fs::read_to_string(format!("{dir}/{f}")).expect("read");
        let ast = syn::parse_file(&text).expect("parse");
        c.file = f;
        c.scope.clear();
        c.visit_file(&ast);
    }
    c
}

struct Pin {
    file: &'static str,
    func: &'static str,
    callees: &'static [&'static str],
    disposition: &'static str,
    why: &'static str,
}

/// A `CHARGED_VIA` pin's evidence: the exhaustive caller list. The
/// census asserts the DISCOVERED call-edge set into `func` equals
/// `chargers ∪ other_callers` exactly, and that every charger scope
/// transitively reaches an in-scope `charge(...)` call (a charger may
/// itself be `CHARGED_VIA`; cycles are rejected). `other_callers` name
/// the non-charging callers with the reason each is safe (constant
/// inputs, error path, compile time).
struct Via {
    func: &'static str,
    chargers: &'static [&'static str],
    other_callers: &'static [(&'static str, &'static str)],
}

/// Shorthands: the full reasons live in the module doc's disposition
/// definitions; per-entry `why` states the bound.
const CHARGED: &str = "CHARGED";
const CHARGED_VIA: &str = "CHARGED_VIA";
const SINK: &str = "SINK";
const VALUE_COPY: &str = "VALUE_COPY";
const TRANSIENT: &str = "TRANSIENT";
const ERROR_PATH: &str = "ERROR_PATH";
const CONST: &str = "CONST";
const COMPILE_TIME: &str = "COMPILE_TIME";
// Round-1's evidence-free `INPUT_BOUNDED` no longer exists: a pin that
// claims it fails to compile.
const ALL_DISPOSITIONS: &[&str] = &[
    CHARGED,
    CHARGED_VIA,
    SINK,
    VALUE_COPY,
    TRANSIENT,
    ERROR_PATH,
    CONST,
    COMPILE_TIME,
    "RESIDUAL",
];

/// The caller-charge evidence table (see [`Via`]). `chargers` are the
/// callers whose scopes (directly or via their own chain) contain the
/// dominating `charge(...)`; `other_callers` are the verified
/// non-charging callers with the reason each is safe.
static VIA: &[Via] = &[
    Via {
        func: "eval_args_text",
        chargers: &["builtin_sig"],
        other_callers: &[],
    },
    Via {
        func: "html_escape",
        chargers: &["builtin_sig"],
        other_callers: &[],
    },
    Via {
        func: "js_escape",
        chargers: &["builtin_sig"],
        other_callers: &[],
    },
    Via {
        func: "url_query_escape",
        chargers: &["builtin_sig"],
        other_callers: &[],
    },
    Via {
        func: "b64_decode_go",
        chargers: &["f!b64dec"],
        other_callers: &[],
    },
    Via {
        func: "go_title",
        chargers: &["f!title"],
        other_callers: &[],
    },
    Via {
        func: "map_runes",
        chargers: &["go_to_upper", "go_to_lower"],
        other_callers: &[],
    },
    // Pass-through wrappers on the case-mapping charge chain (no
    // allocations of their own, so no pin).
    Via {
        func: "go_to_upper",
        chargers: &["f!ToUpper", "f!upper"],
        other_callers: &[],
    },
    Via {
        func: "go_to_lower",
        chargers: &["f!ToLower", "f!lower"],
        other_callers: &[],
    },
    Via {
        func: "query_escape",
        chargers: &["f!urlencode"],
        other_callers: &[],
    },
    Via {
        func: "json_to_value",
        chargers: &["from_json"],
        other_callers: &[],
    },
    Via {
        func: "lossy_repaired",
        // Issue #260: `Retained::from_engine` is the third charger — the
        // pipeline boundary where the engine's BYTES become the row's
        // retained `String`, whose repair expansion it charges before
        // allocating (it was free while it lived in a caller's `Vec`).
        chargers: &["lossy_charged", "compile_regex", "from_engine"],
        other_callers: &[],
    },
    Via {
        func: "go_json_sanitize",
        chargers: &["from_json"],
        other_callers: &[],
    },
    Via {
        func: "fmt_e",
        chargers: &["format_float_go"],
        other_callers: &[],
    },
    Via {
        func: "format_float_go",
        chargers: &["dispatch_float"],
        other_callers: &[],
    },
    Via {
        func: "format_float_hex",
        chargers: &["format_float_go"],
        other_callers: &[],
    },
    Via {
        func: "quote_with",
        chargers: &[],
        other_callers: &[
            (
                "quote_bytes",
                "fmt_q charges 10×len+2 before quoting; every other \
                 quote_bytes use renders error/parse texts once",
            ),
            (
                "quote_bytes_ascii",
                "%+q of error/short texts, under fmt_q's charge on the \
                 render path",
            ),
        ],
    },
    Via {
        func: "sprint",
        chargers: &["builtin_sig", "eval_args_text"],
        other_callers: &[(
            "render_value_for_error",
            "error path: one value render, once per render",
        )],
    },
    Via {
        func: "sprintf",
        chargers: &["builtin_sig"],
        other_callers: &[(
            "builtin_eq",
            "error path: non-comparable-type renders, once per render",
        )],
    },
    Via {
        func: "sprintln",
        chargers: &["builtin_sig"],
        other_callers: &[],
    },
    Via {
        func: "write_template_value",
        chargers: &["print_value_go"],
        other_callers: &[],
    },
    Via {
        func: "format_layout",
        chargers: &["f!date", "time_method"],
        other_callers: &[
            ("strict_rfc3339", "fixed RFC3339 layout (36 bytes)"),
            ("string", "fixed Go time.String layout (≤ 40 bytes)"),
        ],
    },
    Via {
        func: "arg_bytes",
        chargers: &["time_method"],
        other_callers: &[],
    },
];

static PINS: &[Pin] = &[
    // -------------------------------------------------------- decimal.rs
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
    // ----------------------------------------------------------- eval.rs
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
        disposition: ERROR_PATH,
        why: "error paths render one value each, once per render",
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
        disposition: VALUE_COPY,
        why: "element clone ≤ its (charged/store-bounded) container; error paths",
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
        disposition: ERROR_PATH,
        why: "error paths",
    },
    Pin {
        file: "eval.rs",
        func: "builtin_sig",
        callees: &[".clone", ".into_owned", ".to_string", "format!"],
        disposition: CHARGED,
        why: "print/println/printf pre-charge 4× value ceilings (charge_print_family), html/js/urlquery 7×/4× (charge_escaper), slice charges in builtin_slice; eq/ne error paths once per render",
    },
    Pin {
        file: "eval.rs",
        func: "builtin_slice",
        callees: &[".to_string", ".to_vec", "Rc::new", "format!"],
        disposition: CHARGED,
        why: "subslice copy cost (byte length / summed element ceilings) charged before the copy",
    },
    Pin {
        file: "eval.rs",
        func: "coerce_arg_value",
        callees: &["format!"],
        disposition: ERROR_PATH,
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
        func: "enter_depth",
        callees: &["format!"],
        disposition: CONST,
        why: "fixed depth-cap message",
    },
    Pin {
        file: "eval.rs",
        func: "errorf",
        callees: &["format!"],
        disposition: ERROR_PATH,
        why: "the error constructor itself; node text ≤ template text",
    },
    Pin {
        file: "eval.rs",
        func: "eval_and_or",
        callees: &["format!"],
        disposition: ERROR_PATH,
        why: "arity error path",
    },
    Pin {
        file: "eval.rs",
        func: "eval_arg",
        callees: &[".clone", "format!"],
        disposition: VALUE_COPY,
        why: "shallow clones of literals/dot; coercion error paths",
    },
    Pin {
        file: "eval.rs",
        func: "eval_args_text",
        callees: &[".clone", ".collect", ".into_owned"],
        disposition: CHARGED_VIA,
        why: "html/js/urlquery arg render ≤ Σ value ceilings, under charge_escaper",
    },
    Pin {
        file: "eval.rs",
        func: "eval_call",
        callees: &[".push", ".to_string", "Vec::with_capacity", "format!"],
        disposition: VALUE_COPY,
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
        disposition: ERROR_PATH,
        why: "call always errors; ≤ template text + one value",
    },
    Pin {
        file: "eval.rs",
        func: "eval_command",
        callees: &[".clone", ".to_string", "format!"],
        disposition: VALUE_COPY,
        why: "shallow dot/literal clones; error paths",
    },
    Pin {
        file: "eval.rs",
        func: "eval_field",
        callees: &[".clone", "format!"],
        disposition: VALUE_COPY,
        why: "map-entry value clone ≤ its container; error paths",
    },
    Pin {
        file: "eval.rs",
        func: "eval_function",
        callees: &["format!"],
        disposition: ERROR_PATH,
        why: "error path (parse-time unreachable)",
    },
    Pin {
        file: "eval.rs",
        func: "eval_ident_as_arg",
        callees: &[".to_string", "format!"],
        disposition: ERROR_PATH,
        why: "error paths",
    },
    Pin {
        file: "eval.rs",
        func: "eval_pipeline_inner",
        callees: &[".clone"],
        disposition: VALUE_COPY,
        why: "shallow Value clones into decls; a copy never grows, every size-increasing producer charges",
    },
    Pin {
        file: "eval.rs",
        func: "html_escape",
        callees: &[".extend_from_slice", ".push", "Vec::with_capacity"],
        disposition: CHARGED_VIA,
        why: "≤ 6× input, under charge_escaper's 7× pre-charge",
    },
    Pin {
        file: "eval.rs",
        func: "ideal_constant",
        callees: &["format!"],
        disposition: ERROR_PATH,
        why: "error path; literal text ≤ template",
    },
    Pin {
        file: "eval.rs",
        func: "index_arg",
        callees: &[".to_string", "format!"],
        disposition: ERROR_PATH,
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
        disposition: CHARGED_VIA,
        why: "≤ 6× input, under charge_escaper's 7× pre-charge",
    },
    Pin {
        file: "eval.rs",
        func: "label_pairs_sorted_of",
        callees: &[".collect", ".to_vec"],
        disposition: VALUE_COPY,
        why: "≤ 2× the stored label bytes (store data, line-scoped)",
    },
    Pin {
        file: "eval.rs",
        func: "not_a_function",
        callees: &["format!"],
        disposition: ERROR_PATH,
        why: "error path",
    },
    Pin {
        file: "eval.rs",
        func: "push_var",
        callees: &[".push"],
        disposition: VALUE_COPY,
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
        disposition: ERROR_PATH,
        why: "one %v render of one value, once per render",
    },
    Pin {
        file: "eval.rs",
        func: "set_var",
        callees: &["format!"],
        disposition: ERROR_PATH,
        why: "error path; name ≤ template text",
    },
    Pin {
        file: "eval.rs",
        func: "template_node_string",
        callees: &["format!"],
        disposition: ERROR_PATH,
        why: "error path; ≤ template text",
    },
    Pin {
        file: "eval.rs",
        func: "upsert",
        callees: &[".push", ".to_vec"],
        disposition: VALUE_COPY,
        why: "one error-pair entry",
    },
    Pin {
        file: "eval.rs",
        func: "url_query_escape",
        callees: &[".push", "Vec::with_capacity"],
        disposition: CHARGED_VIA,
        why: "≤ 3× input, under charge_escaper's 4× pre-charge",
    },
    Pin {
        file: "eval.rs",
        func: "validate_type",
        callees: &["format!"],
        disposition: ERROR_PATH,
        why: "error paths",
    },
    Pin {
        file: "eval.rs",
        func: "var_value",
        callees: &[".clone", "format!"],
        disposition: VALUE_COPY,
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
        disposition: VALUE_COPY,
        why: "shallow Value clone for the with-dot",
    },
    Pin {
        file: "eval.rs",
        func: "walk_range",
        callees: &[".to_string", "format!"],
        disposition: VALUE_COPY,
        why: "per-iteration shallow clones, freed per iteration; error paths",
    },
    Pin {
        file: "eval.rs",
        func: "walk_template",
        callees: &[".clone", "format!", "vec!"],
        disposition: VALUE_COPY,
        why: "fresh 1-entry var stack + shallow dot clone per invocation, unified depth ≤ 250",
    },
    // ---------------------------------------------------------- funcs.rs
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
        why: "pad path charges pad+len first; the truncation copy charges its exact length (round 2)",
    },
    Pin {
        file: "funcs.rs",
        func: "b64_decode_go",
        callees: &[".extend_from_slice", "Vec::with_capacity", "format!"],
        disposition: CHARGED_VIA,
        why: "≤ 3/4 input, under f!b64dec's charge",
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
        callees: &[".clone", ".to_string", "format!", "str::from_utf8"],
        disposition: CHARGED,
        why: "pattern copy (≤3× U+FFFD ceiling when invalid UTF-8, round 4) + what compiling THAT pattern costs, both charged BEFORE converting/building; cache hits share the query-compile program. Issue #291 replaced the `RegexBuilder::new(…).build()` pair with `pulsus_re2::compile_user_regex_with` at the same 1 MiB program ceiling, and the second charge with `regex_compile_transient_bound_with` — the flat ceiling bounded only the NFA phase, so a 32-byte class-heavy pattern allocated 2.67 MB against it",
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
        disposition: CHARGED,
        why: "output length charged before the copy",
    },
    Pin {
        file: "funcs.rs",
        func: "f!TrimSuffix",
        callees: &[".to_vec"],
        disposition: CHARGED,
        why: "output length charged before the copy",
    },
    Pin {
        file: "funcs.rs",
        func: "f!__line__",
        callees: &[".to_vec"],
        disposition: CHARGED,
        why: "one copy of the stored line, charged — repeatable per call (the round-2 fifth-class exemplar)",
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
        disposition: TRANSIENT,
        why: "one lossy copy freed by return; float result; error text bounded",
    },
    Pin {
        file: "funcs.rs",
        func: "f!default",
        callees: &[".clone"],
        disposition: CHARGED,
        why: "the winner's charge ceiling charged before the clone",
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
        disposition: TRANSIENT,
        why: "one copy freed by return; float result; ParseDuration errors ≤ ~4× input",
    },
    Pin {
        file: "funcs.rs",
        func: "f!duration_seconds",
        callees: &[".into_owned"],
        disposition: TRANSIENT,
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
        why: "(len+2)×len(repl)+len upper bound charged before replace_all; program ceiling in compile_regex",
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
        disposition: CHARGED,
        why: "slice length charged before each copy; panic-text error paths",
    },
    Pin {
        file: "funcs.rs",
        func: "f!toDateInZone",
        callees: &[".into_owned", ".to_vec"],
        disposition: TRANSIENT,
        why: "layout/value copies freed by return; Time result",
    },
    Pin {
        file: "funcs.rs",
        func: "f!trimPrefix",
        callees: &[".to_vec"],
        disposition: CHARGED,
        why: "output length charged before the copy",
    },
    Pin {
        file: "funcs.rs",
        func: "f!trimSuffix",
        callees: &[".to_vec"],
        disposition: CHARGED,
        why: "output length charged before the copy",
    },
    Pin {
        file: "funcs.rs",
        func: "f!trunc",
        callees: &[".to_vec"],
        disposition: CHARGED,
        why: "slice length charged before the copy",
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
        disposition: TRANSIENT,
        why: "one copy freed by return; Time result",
    },
    Pin {
        file: "funcs.rs",
        func: "from_json",
        callees: &["serde_json::from_slice"],
        disposition: CHARGED,
        why: "the 35×len+64 tree ceiling is charged before sanitize/parse",
    },
    Pin {
        file: "funcs.rs",
        func: "go_json_sanitize",
        callees: &[".extend_from_slice", "Vec::with_capacity"],
        disposition: CHARGED_VIA,
        why: "≤ 3× input (U+FFFD substitution), under from_json's 35× charge",
    },
    Pin {
        file: "funcs.rs",
        func: "go_parse_duration",
        callees: &[".into_owned", "String::from_utf8_lossy", "format!"],
        disposition: TRANSIENT,
        why: "lossy copy + error texts ≤ ~4× input, freed; scalar result",
    },
    Pin {
        file: "funcs.rs",
        func: "go_parse_float",
        callees: &[".collect"],
        disposition: TRANSIENT,
        why: "≤ input, freed; float result",
    },
    Pin {
        file: "funcs.rs",
        func: "go_parse_int_base0",
        callees: &[".collect"],
        disposition: TRANSIENT,
        why: "underscore-stripped Cow only when '_' present (round-4 sweep), freed; int result",
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
        disposition: CHARGED_VIA,
        why: "≤ 4× input, under f!title's charge",
    },
    Pin {
        file: "funcs.rs",
        func: "go_trim",
        callees: &[".to_vec"],
        disposition: CHARGED,
        why: "output length charged before the copy (round 2)",
    },
    Pin {
        file: "funcs.rs",
        func: "go_trim_space",
        callees: &[".to_vec"],
        disposition: CHARGED,
        why: "output length charged before the copy (round 2)",
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
        callees: &[".collect", "Rc::new"],
        disposition: CHARGED_VIA,
        why: "tree ≤ from_json's 35× ceiling",
    },
    Pin {
        file: "funcs.rs",
        func: "lossy",
        callees: &["String::from_utf8_lossy"],
        disposition: TRANSIENT,
        why: "borrowed Cow; ≤ 3× input only for invalid UTF-8 replacement, freed by the caller",
    },
    Pin {
        file: "funcs.rs",
        func: "lossy_repaired",
        callees: &[
            ".push",
            ".push_str",
            "String::with_capacity",
            "str::from_utf8",
        ],
        disposition: CHARGED_VIA,
        why: "ONE allocation of the precomputed repaired length; its callers charge that exact size first (round 6)",
    },
    Pin {
        file: "funcs.rs",
        func: "lossy_repaired_matches_std_from_utf8_lossy_byte_for_byte",
        callees: &["String::from_utf8_lossy"],
        disposition: CONST,
        why: "test-region equality check over fixed short byte cases (round 6)",
    },
    Pin {
        file: "funcs.rs",
        func: "lossy_repaired_len",
        callees: &["str::from_utf8"],
        disposition: CONST,
        why: "pure length scan, borrows only — no allocation",
    },
    Pin {
        file: "funcs.rs",
        func: "lossy_charged",
        callees: &["str::from_utf8"],
        disposition: CHARGED,
        why: "reserves the ≤3× U+FFFD expansion BEFORE from_utf8_lossy's owned branch (round 4); the validity probe borrows",
    },
    Pin {
        file: "funcs.rs",
        func: "map_runes",
        callees: &[".extend_from_slice", "Vec::with_capacity"],
        disposition: CHARGED_VIA,
        why: "≤ 4× input; every render caller charges 4×len first",
    },
    Pin {
        file: "funcs.rs",
        func: "query_escape",
        callees: &[".push", "Vec::with_capacity"],
        disposition: CHARGED_VIA,
        why: "≤ 3× input, under f!urlencode's charge",
    },
    Pin {
        file: "funcs.rs",
        func: "query_unescape",
        callees: &[".push", "Vec::with_capacity", "format!"],
        disposition: CHARGED,
        why: "input length charged at entry (output ≤ input); error text ≤ 3 bytes quoted",
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
        disposition: TRANSIENT,
        why: "≤ input + 1, freed; feeds an int parse",
    },
    Pin {
        file: "funcs.rs",
        func: "unix_to_time",
        callees: &["format!"],
        disposition: ERROR_PATH,
        why: "error texts ≤ ~11× input via strconv quote of a short epoch string",
    },
    // ---------------------------------------------------------- gofmt.rs
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
        disposition: SINK,
        why: "one %v re-print of one value into out",
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
        disposition: SINK,
        why: "≤ 4×len decimal walk into out (inside the 4× Bytes ceiling)",
    },
    Pin {
        file: "gofmt.rs",
        func: "dispatch_complex",
        callees: &[".extend_from_slice", ".push"],
        disposition: SINK,
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
        disposition: SINK,
        why: "format-text copies ≤ pre-charged format len; args via charged dispatchers; EXTRA renders ≤ Σ arg ceilings",
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
        disposition: CHARGED_VIA,
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
        disposition: SINK,
        why: "pinned-address renders + one deref re-print into out",
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
        disposition: SINK,
        why: "2×min(len,prec) into out (inside the printf 4× ceiling); width via write_padding's charge",
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
        disposition: CHARGED_VIA,
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
        disposition: CHARGED_VIA,
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
        disposition: SINK,
        why: "content ≤ produced value into out; padding via write_padding's charge",
    },
    Pin {
        file: "gofmt.rs",
        func: "print_arg",
        callees: &[".to_string"],
        disposition: CONST,
        why: "type-name literal",
    },
    Pin {
        file: "gofmt.rs",
        func: "print_byte_slice_walk",
        callees: &[".extend_from_slice", ".push"],
        disposition: SINK,
        why: "per-element badVerb ≤ ~24×len into out (inside the 4× Bytes ceiling)",
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
        disposition: SINK,
        why: "entry copies ≤ map bytes (charged/store data), written into out",
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
        disposition: SINK,
        why: "walks an existing value into out; entry copies ≤ value size (charged at production / store-bounded)",
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
        disposition: CHARGED_VIA,
        why: "≤ 10× input, under fmt_q's charge; other callers quote short error/parse texts",
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
        callees: &[".push", "P::new"],
        disposition: CHARGED_VIA,
        why: "print builtin pre-charges 4× value ceilings; error-path callers render one value once",
    },
    Pin {
        file: "gofmt.rs",
        func: "sprintf",
        callees: &["P::new"],
        disposition: CHARGED_VIA,
        why: "printf builtin pre-charges format + 4× value ceilings; error-path callers render one value once",
    },
    Pin {
        file: "gofmt.rs",
        func: "sprintln",
        callees: &[".push", "P::new"],
        disposition: CHARGED_VIA,
        why: "println builtin pre-charges 4× value ceilings",
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
        callees: &[".extend_from_slice", "P::new"],
        disposition: CHARGED_VIA,
        why: "print_value_go charges the value's output ceiling BEFORE this write (round 2, finding 1)",
    },
    // ------------------------------------------------------- golayout.rs
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
        disposition: CHARGED_VIA,
        why: "≤ ~10× layout text (longest token expansion is a month name); render callers charge 10×len first, remaining callers pass fixed layouts",
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
        disposition: TRANSIENT,
        why: "zone-name copies freed by return; Time result",
    },
    // ------------------------------------------------------------ lex.rs
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
    // -------------------------------------------------------- methods.rs
    Pin {
        file: "methods.rs",
        func: "arg_bytes",
        callees: &[".clone", ".into_owned"],
        disposition: CHARGED_VIA,
        why: "one copy of a caller-built []byte, charged by the Append* closures before the copy",
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
        disposition: CHARGED,
        why: "Format/AppendFormat charge the ≤10× layout expansion, Append* charge the argument copy (round 2 — `$b = $t.AppendText $b` grows per call); other outputs ≤ ~64B",
    },
    // ------------------------------------------------------------ mod.rs
    Pin {
        file: "mod.rs",
        func: "<module>",
        callees: &["OnceLock::new"],
        disposition: CONST,
        why: "one empty process-wide timezone slot, statically sized (issue #311)",
    },
    Pin {
        file: "mod.rs",
        func: "charge",
        callees: &["format!"],
        disposition: CONST,
        why: "the ledger itself; the breach message is fixed",
    },
    Pin {
        file: "mod.rs",
        func: "compile",
        callees: &[".clone", ".insert", ".to_string", "Box::new", "format!"],
        disposition: COMPILE_TIME,
        why: "tree/fast-path derivation + literal-regex programs, once per query. This `why` used to end ‘≤ query-capped template text’ and issue #291's review measured that false for the regex half: the prewarm's bare `Regex::new` peaked 298.92 MB on a literal `\\w`x43000 inside the query-text cap. `Regex::new` has left the callee set because the prewarm now goes through `pulsus_re2::compile_user_regex`, which bounds the compile before it runs",
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
    // ------------------------------------------------------- retained.rs
    // Issue #260 review round 2: the charging TYPE. Every constructor
    // charges in its own scope BEFORE it allocates, which is exactly
    // what `CHARGED` asserts — so the census is the evidence for the
    // module's whole claim, not a separate promise about it.
    Pin {
        file: "retained.rs",
        func: "copy",
        callees: &[".to_string"],
        disposition: CHARGED,
        why: "charges src.len() — the exact copy — before making it",
    },
    Pin {
        file: "retained.rs",
        func: "concat",
        callees: &[".clone", ".push_str", "String::with_capacity"],
        disposition: CHARGED,
        why: "sizes the pieces ITSELF (the `.clone` is the sizing walk of the same iterator, \
              which allocates nothing for the `Map`-over-slice the pipeline passes), charges \
              that sum, then allocates exactly it; the written length is reconciled against \
              the charge unconditionally, so an overrun is charged rather than assumed away \
              (round 3)",
    },
    Pin {
        file: "retained.rs",
        func: "render_full",
        callees: &["format!"],
        disposition: ERROR_PATH,
        why: "the fixed breach message, built once as the render aborts (issue #260)",
    },
    Pin {
        file: "retained.rs",
        func: "from_engine",
        callees: &["String::from_utf8"],
        disposition: CHARGED,
        why: "valid UTF-8 MOVES the engine's already-charged buffer (from_utf8 does not \
              allocate); the invalid path charges the repair EXPANSION before \
              lossy_repaired allocates it",
    },
    Pin {
        file: "retained.rs",
        func: "take",
        callees: &[".to_vec"],
        disposition: CHARGED,
        why: "charges the owned halves plus the element buffer before the deep copy",
    },
    Pin {
        file: "retained.rs",
        func: "every_constructor_charges_exactly_what_it_retains",
        callees: &[".to_vec", "vec!"],
        disposition: CONST,
        why: "test region: fixed short byte cases",
    },
    Pin {
        file: "retained.rs",
        func: "a_refused_charge_produces_no_value_and_poisons_the_budget",
        callees: &[".repeat"],
        disposition: CONST,
        why: "test region: one budget-derived string, allocated once and dropped",
    },
    Pin {
        file: "retained.rs",
        func: "concat_refuses_when_the_overrun_crosses_the_budget",
        callees: &[".repeat"],
        disposition: CONST,
        why: "test region: one budget-derived string, allocated once and dropped",
    },
    Pin {
        file: "retained.rs",
        func: "a_snapshot_charges_the_bytes_it_deep_copies",
        callees: &[".repeat", "vec!"],
        disposition: CONST,
        why: "test region: one fixed 1 000-byte label pair",
    },
    // ---------------------------------------------------------- parse.rs
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
        func: "enter_depth",
        callees: &[".to_string"],
        disposition: COMPILE_TIME,
        why: "depth-cap error text (round 3: the guard moved to parse_control/block_control/term so else-if chains count)",
    },
    Pin {
        file: "parse.rs",
        func: "item_list",
        callees: &[".push", ".to_string"],
        disposition: COMPILE_TIME,
        why: "node lists ≤ template text (NOT a guard site — round 3)",
    },
    Pin {
        file: "parse.rs",
        func: "operand",
        callees: &[".extend", ".push", ".to_string", "Box::new", "format!"],
        disposition: COMPILE_TIME,
        why: "chained idents ≤ template text",
    },
    Pin {
        file: "parse.rs",
        func: "parse",
        callees: &[".to_string", "Lexer::new", "vec!"],
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
        why: "node text ≤ template text (also used on exec error paths — once per render there)",
    },
    Pin {
        file: "parse.rs",
        func: "term",
        callees: &[".to_string", "format!", "vec!"],
        disposition: COMPILE_TIME,
        why: "error texts (incl. the paren depth-cap site)",
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
    // -------------------------------------------------------- timefns.rs
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
        func: "for_timezone",
        callees: &[".to_string"],
        disposition: CONST,
        why: "one configured zone name, once per compile (issue #311)",
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
    // ---------------------------------------------------------- value.rs
    Pin {
        file: "value.rs",
        func: "into_owned",
        callees: &[".clone", ".collect", ".into_owned", "Rc::new"],
        disposition: VALUE_COPY,
        why: "one deep copy of an existing (charged/bounded) value; not on the hot path",
    },
    Pin {
        file: "value.rs",
        func: "str_owned",
        callees: &[".into"],
        disposition: VALUE_COPY,
        why: "factor-1 copy of the caller's argument (Into<Vec<u8>>); amplification needs an in-vocabulary loop at the caller",
    },
];

/// Resolves whether `func` (a charger name from the [`VIA`] table)
/// transitively reaches an in-scope `charge(...)` call: either some
/// scope with that name contains one, or the function is itself pinned
/// `CHARGED_VIA` and ALL of its listed chargers resolve (cycles fail).
fn charger_resolves(
    func: &str,
    charged_names: &BTreeSet<&str>,
    via_by_func: &BTreeMap<&str, &Via>,
    visiting: &mut Vec<String>,
) -> bool {
    if charged_names.contains(func) {
        return true;
    }
    if visiting.iter().any(|v| v == func) {
        return false; // cycle: no charge evidence anywhere on the chain
    }
    let Some(via) = via_by_func.get(func) else {
        return false;
    };
    visiting.push(func.to_string());
    let ok = !via.chargers.is_empty()
        && via
            .chargers
            .iter()
            .all(|c| charger_resolves(c, charged_names, via_by_func, visiting));
    visiting.pop();
    ok
}

#[test]
fn every_template_allocation_site_is_classified() {
    let census = census();
    let found = census.out;
    let mut pinned: CensusMap = BTreeMap::new();
    let mut seen = BTreeSet::new();
    let mut errors = String::new();
    for p in PINS {
        assert!(
            seen.insert((p.file, p.func)),
            "duplicate pin {}::{}",
            p.file,
            p.func
        );
        if !ALL_DISPOSITIONS.contains(&p.disposition) {
            let _ = writeln!(
                errors,
                "{}::{}: unknown disposition {:?}",
                p.file, p.func, p.disposition
            );
        }
        assert!(!p.why.is_empty(), "{}::{}: empty why", p.file, p.func);
        pinned.insert(
            (p.file.to_string(), p.func.to_string()),
            p.callees.iter().map(|s| s.to_string()).collect(),
        );
        // --- disposition EVIDENCE (round 2: a claim must be true, not
        // merely a legal word) -----------------------------------------
        match p.disposition {
            // CHARGED: the scope itself must contain a charge call.
            d if d == CHARGED => {
                if !census
                    .charged_scopes
                    .contains(&(p.file.to_string(), p.func.to_string()))
                {
                    let _ = writeln!(
                        errors,
                        "{}::{} is pinned CHARGED but contains no charge(...) call",
                        p.file, p.func
                    );
                }
            }
            // CHARGED_VIA: must have a VIA entry (checked below).
            d if d == CHARGED_VIA => {
                if !VIA.iter().any(|v| v.func == p.func) {
                    let _ = writeln!(
                        errors,
                        "{}::{} is pinned CHARGED_VIA but has no VIA caller-evidence entry",
                        p.file, p.func
                    );
                }
            }
            // SINK is a gofmt-printer-internal claim only.
            d if d == SINK => {
                if p.file != "gofmt.rs" {
                    let _ = writeln!(
                        errors,
                        "{}::{} is pinned SINK outside gofmt.rs (SINK claims the \
                         printer-out charge boundary)",
                        p.file, p.func
                    );
                }
            }
            // COMPILE_TIME allocations live only on the compile surface.
            d if d == COMPILE_TIME => {
                if !matches!(p.file, "lex.rs" | "parse.rs" | "mod.rs") {
                    let _ = writeln!(
                        errors,
                        "{}::{} is pinned COMPILE_TIME outside the compile surface \
                         (lex.rs/parse.rs/mod.rs)",
                        p.file, p.func
                    );
                }
            }
            "RESIDUAL" => {
                let _ = writeln!(
                    errors,
                    "{}::{} is RESIDUAL — an uncharged, unbounded allocation must not ship",
                    p.file, p.func
                );
            }
            _ => {}
        }
    }
    // --- VIA table evidence -------------------------------------------
    let charged_names: BTreeSet<&str> = census
        .charged_scopes
        .iter()
        .map(|(_, f)| f.as_str())
        .collect();
    let via_by_func: BTreeMap<&str, &Via> = VIA.iter().map(|v| (v.func, v)).collect();
    assert_eq!(
        via_by_func.len(),
        VIA.len(),
        "duplicate VIA entries (target names must be unique)"
    );
    // The charge WRAPPERS must themselves contain a real charge call
    // (their callers inherit charge evidence from them).
    for wrapper in CHARGE_FNS
        .iter()
        .filter(|f| !PRIMITIVE_CHARGE_FNS.contains(f))
    {
        if !census.charged_scopes.iter().any(|(_, f)| f == wrapper) {
            let _ = writeln!(
                errors,
                "charge wrapper {wrapper} no longer contains a charge(...) call"
            );
        }
    }
    for via in VIA {
        // Every VIA entry belongs to a CHARGED_VIA pin, or to an
        // allocation-free pass-through wrapper on a charge chain
        // (`go_to_upper`/`go_to_lower`) that therefore has no pin —
        // but never to a pin claiming a DIFFERENT disposition.
        if let Some(p) = PINS.iter().find(|p| p.func == via.func)
            && p.disposition != CHARGED_VIA
        {
            let _ = writeln!(
                errors,
                "VIA entry {} contradicts its pin's disposition {:?}",
                via.func, p.disposition
            );
        }
        // The discovered caller set must equal the pinned one exactly —
        // a NEW caller of an allocating helper fails until classified.
        let expected: BTreeSet<&str> = via
            .chargers
            .iter()
            .copied()
            .chain(via.other_callers.iter().map(|(c, _)| *c))
            .collect();
        let discovered: BTreeSet<&str> = census
            .edges
            .get(via.func)
            .map(|s| {
                s.iter()
                    .map(|(_, f)| f.as_str())
                    .filter(|f| *f != via.func) // ignore self-recursion
                    .collect()
            })
            .unwrap_or_default();
        if discovered != expected {
            let _ = writeln!(
                errors,
                "VIA {}: discovered callers {discovered:?} != pinned {expected:?} — \
                 classify the new/removed caller (charger or other_caller)",
                via.func
            );
        }
        // Every charging caller must transitively reach a real charge.
        for charger in via.chargers {
            let mut visiting = Vec::new();
            if !charger_resolves(charger, &charged_names, &via_by_func, &mut visiting) {
                let _ = writeln!(
                    errors,
                    "VIA {}: charger {charger} has no in-scope charge(...) call \
                     (directly or via its own VIA chain)",
                    via.func
                );
            }
        }
        for (caller, why) in via.other_callers {
            assert!(!why.is_empty(), "VIA {}: empty why for {caller}", via.func);
        }
    }
    // --- exact site-set equality (both directions) ----------------------
    // Functions with NO vocabulary hits need no pin; drop empty pins
    // from the comparison the same way (they document zero-alloc fns).
    let found: CensusMap = found.into_iter().filter(|(_, v)| !v.is_empty()).collect();
    let pinned: CensusMap = pinned.into_iter().filter(|(_, v)| !v.is_empty()).collect();
    if found != pinned {
        for (k, v) in &found {
            if pinned.get(k) != Some(v) {
                let _ = writeln!(
                    errors,
                    "NEW/CHANGED {k:?}: {v:?} (pinned: {:?})",
                    pinned.get(k)
                );
            }
        }
        for (k, v) in &pinned {
            if !found.contains_key(k) {
                let _ = writeln!(errors, "STALE PIN {k:?}: {v:?}");
            }
        }
    }
    if !errors.is_empty() {
        panic!(
            "template allocation census drifted — classify each finding \
             (CHARGED / CHARGED_VIA / SINK / VALUE_COPY / TRANSIENT / \
             ERROR_PATH / CONST / COMPILE_TIME) against \"can a \
             caller-controlled input make this allocation large?\", with \
             the disposition's EVIDENCE, in the same change:\n{errors}"
        );
    }
}

/// Maintenance generator: prints the discovered census for re-pinning.
#[test]
#[ignore = "generator: prints the discovered census table"]
fn zz_print_census() {
    let census = census();
    for ((file, func), callees) in &census.out {
        if callees.is_empty() {
            continue;
        }
        let charged = census
            .charged_scopes
            .contains(&(file.clone(), func.clone()));
        println!(
            "{file} :: {func} :: {callees:?}{}",
            if charged { " [charges]" } else { "" }
        );
    }
    for (target, callers) in &census.edges {
        println!("EDGE {target} <- {callers:?}");
    }
}
