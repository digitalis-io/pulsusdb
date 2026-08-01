//! The TraceQL regex invariant's structural gates (issue #282, review
//! findings 1 and the round-3 fail-open finding).
//!
//! # What is enforced where
//!
//! **rustc** owns the reach half, and it is the real seal: the raw
//! escapers are private to `logql::escape`, so no code in `traces/` can
//! render a regex except through `ch_regex_anchored_checked`; and
//! `search_plan::eval_compile::compile_anchored` is private to its LEAF
//! MODULE, so no code elsewhere in `search_plan.rs` can revive it as a
//! second plan-time validator. Both are compile errors (`E0425`/`E0603`),
//! not conventions.
//!
//! **This file** owns the only hole rustc cannot see: a bypass written
//! INSIDE the leaf, where `compile_anchored` is legitimately in scope. It
//! pins the leaf's contents to a committed table.
//!
//! # Fail-closed by construction (round-3 finding)
//!
//! The first version of this gate classified items with a hand-rolled
//! line scanner and silently IGNORED any spelling it did not recognise —
//! so `pub(in super) fn bypass_compile(…)` sailed through, and a
//! `macro_rules!` inside the leaf could generate a second caller with the
//! item table unchanged. A parser that only knows the shapes someone
//! thought of is the same defect one iteration later.
//!
//! It is now a `syn` parse with an **allowlist of exactly two item
//! forms** — `use` and `fn` — and a catch-all arm that renders anything
//! else as `UNALLOWLISTED-ITEM-FORM`, a string that can never appear in
//! the committed table. Every other Rust item — `macro_rules!`, a macro
//! INVOCATION, `impl`, `trait`, `struct`, `type`, `static`, `const`,
//! `extern`, a nested `mod`, even `Item::Verbatim` (syn's bucket for
//! things it cannot parse) — therefore reddens the gate rather than
//! passing it. Non-`doc` attributes do too, since an attribute macro can
//! rewrite the item it decorates. The default is "reject what I do not
//! understand".
//!
//! `syn` is already a dev-dependency of this crate for precisely this
//! class of gate (see `crates/pulsus-read/Cargo.toml`: "a token/regex
//! rule cannot see generic calls or macro bodies"). No new dependency.

use std::fmt::Write as _;
use std::path::Path;

fn read(rel: &str) -> String {
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {p:?}: {e}"))
}

/// Every item `mod eval_compile` may contain, in source order. Two
/// functions and their imports — nothing else. A third item here would be
/// able to call `compile_anchored`, which is the whole point of the
/// module.
const EVAL_COMPILE_ITEMS: &[&str] = &[
    "use regex::Regex",
    "use pulsus_traceql::ComparisonOp",
    "use super::super::filter::PlanError",
    "use super::StrOp",
    "fn compile_anchored",
    "pub(super) fn planned_str_op",
];

fn path_str(path: &syn::Path) -> String {
    let mut out = if path.leading_colon.is_some() {
        "::".to_string()
    } else {
        String::new()
    };
    let segs: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
    out.push_str(&segs.join("::"));
    out
}

fn vis_str(v: &syn::Visibility) -> String {
    match v {
        syn::Visibility::Inherited => String::new(),
        syn::Visibility::Public(_) => "pub ".to_string(),
        syn::Visibility::Restricted(r) => {
            let inner = path_str(&r.path);
            if r.in_token.is_some() {
                format!("pub(in {inner}) ")
            } else {
                format!("pub({inner}) ")
            }
        }
    }
}

fn use_tree_str(t: &syn::UseTree) -> String {
    match t {
        syn::UseTree::Path(p) => format!("{}::{}", p.ident, use_tree_str(&p.tree)),
        syn::UseTree::Name(n) => n.ident.to_string(),
        syn::UseTree::Rename(r) => format!("{} as {}", r.ident, r.rename),
        // A glob is exactly how extra names would be smuggled in; it
        // renders to something the table does not contain.
        syn::UseTree::Glob(_) => "*".to_string(),
        syn::UseTree::Group(g) => format!(
            "{{{}}}",
            g.items
                .iter()
                .map(use_tree_str)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Attributes other than doc comments. An attribute macro can rewrite or
/// duplicate the item it decorates, so any of these makes the item
/// unrecognisable and it must not match the table.
fn non_doc_attrs(attrs: &[syn::Attribute]) -> Vec<String> {
    attrs
        .iter()
        .filter(|a| !a.path().is_ident("doc"))
        .map(|a| path_str(a.path()))
        .collect()
}

/// Diagnostics only — never a decision. The decision is [`descriptor`]'s
/// catch-all; this just makes the failure message legible.
fn item_kind_hint(item: &syn::Item) -> &'static str {
    match item {
        syn::Item::Macro(_) => "macro_rules! or macro invocation",
        syn::Item::Mod(_) => "nested mod",
        syn::Item::Impl(_) => "impl",
        syn::Item::Trait(_) => "trait",
        syn::Item::Struct(_) => "struct",
        syn::Item::Enum(_) => "enum",
        syn::Item::Const(_) => "const",
        syn::Item::Static(_) => "static",
        syn::Item::Type(_) => "type alias",
        syn::Item::ForeignMod(_) => "extern block",
        syn::Item::Verbatim(_) => "unparseable by syn",
        _ => "other",
    }
}

/// The fail-closed descriptor. Only `use` and `fn` are described; EVERY
/// other item form — and every attributed item — renders to a string the
/// committed table cannot contain.
fn descriptor(item: &syn::Item) -> String {
    let (attrs, core) = match item {
        syn::Item::Use(u) => (
            non_doc_attrs(&u.attrs),
            format!("{}use {}", vis_str(&u.vis), use_tree_str(&u.tree)),
        ),
        syn::Item::Fn(f) => (
            non_doc_attrs(&f.attrs),
            format!("{}fn {}", vis_str(&f.vis), f.sig.ident),
        ),
        // CATCH-ALL — the load-bearing arm. `macro_rules!` and macro
        // invocations are both `Item::Macro`; `Item::Verbatim` is syn's
        // bucket for anything it cannot parse. None of them can be
        // spelled in `EVAL_COMPILE_ITEMS`, so all of them fail.
        other => (
            Vec::new(),
            format!("UNALLOWLISTED-ITEM-FORM({})", item_kind_hint(other)),
        ),
    };
    if attrs.is_empty() {
        core
    } else {
        format!("ATTRIBUTED[{}] {core}", attrs.join(","))
    }
}

fn eval_compile_items() -> Vec<syn::Item> {
    let text = read("src/traces/search_plan.rs");
    let file = syn::parse_file(&text).expect("search_plan.rs must parse");
    let m = file
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Mod(m) if m.ident == "eval_compile" => Some(m),
            _ => None,
        })
        .expect("`mod eval_compile` not found — the seal moved or was renamed");
    assert!(
        matches!(m.vis, syn::Visibility::Inherited),
        "`mod eval_compile` must stay private to `search_plan`; found {:?} visibility",
        vis_str(&m.vis)
    );
    assert!(
        non_doc_attrs(&m.attrs).is_empty(),
        "`mod eval_compile` must carry no attributes but doc comments"
    );
    let (_, items) = m
        .content
        .as_ref()
        .expect("`mod eval_compile` must stay INLINE — a file-backed module moves the contents");
    items.clone()
}

/// The leaf's contents are exactly the committed table — both directions.
/// A new item added inside `eval_compile` (the only place from which
/// `compile_anchored` is reachable) fails here; added anywhere else it
/// fails in rustc.
#[test]
fn the_eval_compile_leaf_holds_only_the_phase2_compile_and_its_one_caller() {
    let found: Vec<String> = eval_compile_items().iter().map(descriptor).collect();
    let mut errors = String::new();
    if found != EVAL_COMPILE_ITEMS {
        let _ = writeln!(
            errors,
            "mod eval_compile contents drifted.\n  found:\n    {}\n  pinned:\n    {}\n\
             Every item in this module can call `compile_anchored` and could revive the \
             second plan-time regex validator issue #282 removed. Any item form other than \
             a plain `use`/`fn` renders as UNALLOWLISTED-ITEM-FORM and fails here by \
             design. If you have a real need for one, this gate is meant to fail and start \
             the conversation.",
            found.join("\n    "),
            EVAL_COMPILE_ITEMS.join("\n    ")
        );
    }
    assert!(errors.is_empty(), "{errors}");
}

/// The last path segment of a function's return type's first generic
/// argument — i.e. the `T` of `Result<T, E>`.
fn result_ok_ident(f: &syn::ItemFn) -> String {
    let syn::ReturnType::Type(_, ty) = &f.sig.output else {
        panic!("{} must return a Result", f.sig.ident);
    };
    let syn::Type::Path(tp) = &**ty else {
        panic!("{} must return a path type", f.sig.ident);
    };
    let last = tp.path.segments.last().expect("non-empty path");
    assert_eq!(last.ident, "Result", "{} must return a Result", f.sig.ident);
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
        panic!("{}'s Result must be generic", f.sig.ident);
    };
    match args.args.first().expect("Result<T, E> has a T") {
        syn::GenericArgument::Type(syn::Type::Path(p)) => {
            p.path.segments.last().expect("non-empty").ident.to_string()
        }
        _ => panic!(
            "the Ok type of {}'s Result must be a plain path type",
            f.sig.ident
        ),
    }
}

/// The compiler itself never escapes the leaf: `planned_str_op` hands
/// back a `StrOp`, so no caller can obtain the `Regex` constructor and
/// re-validate with it. Checked structurally — widening the return type
/// to `Regex` is exactly how the seal would be undone without adding an
/// item to the table above.
#[test]
fn the_leafs_only_export_returns_str_op_not_the_compiler() {
    let mut seen = 0;
    for item in eval_compile_items() {
        let syn::Item::Fn(f) = item else { continue };
        match f.sig.ident.to_string().as_str() {
            "planned_str_op" => {
                assert_eq!(
                    result_ok_ident(&f),
                    "StrOp",
                    "the leaf's one export must keep returning StrOp, never the compiler"
                );
                assert!(
                    matches!(f.vis, syn::Visibility::Restricted(ref r)
                        if r.in_token.is_none() && path_str(&r.path) == "super"),
                    "planned_str_op must stay `pub(super)`"
                );
                seen += 1;
            }
            "compile_anchored" => {
                assert_eq!(result_ok_ident(&f), "Regex");
                assert!(
                    matches!(f.vis, syn::Visibility::Inherited),
                    "compile_anchored must stay private to the leaf"
                );
                seen += 1;
            }
            other => panic!("unexpected fn in the leaf: {other}"),
        }
    }
    assert_eq!(seen, 2, "both leaf functions must be present");
}

/// TraceQL holds no capability token for the raw escapers any more
/// (issue #282's headline). `logql::escape`'s own surface gate (check D
/// in `logqltest_provenance.rs`) pins the escape module; this pins the
/// consumer side, so re-adding a token here is caught even if the
/// escape-module table is edited to match.
#[test]
fn traces_holds_no_capability_token_for_the_raw_escapers() {
    for rel in [
        "src/traces/mod.rs",
        "src/traces/filter.rs",
        "src/traces/search_plan.rs",
        "src/traces/metrics_sql.rs",
    ] {
        let text = read(rel);
        for banned in [
            "TraceqlPrevalidated",
            "ch_regex_anchored_traceql_prevalidated",
        ] {
            // Skip comment lines: the module docs legitimately record
            // that the token was DELETED.
            let hit = text
                .lines()
                .find(|l| !l.trim_start().starts_with("//") && l.contains(banned));
            assert!(
                hit.is_none(),
                "{rel}: {banned:?} is back in code: {:?}",
                hit.unwrap()
            );
        }
    }
}
