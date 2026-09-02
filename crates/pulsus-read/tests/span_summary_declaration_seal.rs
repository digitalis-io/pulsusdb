//! Issue #479 — the span-summary declaration seal.
//!
//! # Two claimed domains, not one
//!
//! **(a) Criteria 1–3** read the PARSED SOURCE TEXT of ONE file in this
//! crate — `src/traces/search_eval.rs` — and say nothing about what that
//! text expands to. They pin the `SpanSummary` declaration: `name` is
//! private and `Option<String>`, the item's complete attribute list is
//! exactly the committed derive, and the file holds exactly one such
//! struct and only the committed item-position macros.
//!
//! **(b) Criterion 4** asks *rustc* one question — does this type
//! implement the serialisation trait — about a set of types DERIVED from
//! the renderer's own production `pulsus_read` names, not hand-listed.
//!
//! # Why the field is private, and what that buys
//!
//! `Option<String>` in a `serde_json::json!` VALUE position compiles and
//! renders `null`. The renderer is the one consumer where the type change
//! alone is not self-enforcing, and `"name":null` is a body the reference
//! never produces. Making the field private turns that silent null into
//! `E0616`. Criterion 1 is what stops a later edit from quietly re-adding
//! `pub` and reopening every access path at once — no type error would
//! follow it.
//!
//! # Fail-closed by construction
//!
//! Every descriptor here follows the rule
//! `tests/traces_regex_seal.rs` already states: *"a parser that only knows
//! the shapes someone thought of is the same defect one iteration
//! later"*. Attributes, types, item forms and `use`-tree shapes that are
//! not explicitly described render to `UNALLOWLISTED-…` strings the
//! committed constants can never contain. The default is "reject what I
//! do not understand".
//!
//! # What defeats all four criteria, stated rather than enumerated
//!
//! Criteria 1–3 quantify over parsed source text and the program that
//! runs is that text's expansion under an unbounded set of macros;
//! criterion 4 quantifies over one trait and one build configuration.
//! There is no finite list of the ways a program can fail that
//! conjunction, and this file does not present one. Four KNOWN members,
//! examples and not a partition: a `#[cfg(feature = …)]`-gated impl
//! (`pulsus-read` declares no `[features]`, so there is no configuration
//! for one to hide in today); an in-module wrapper type reading the
//! private field; a serialisation trait criterion 4 does not name; and
//! `Debug`, which is not a wire format on this route and is deliberately
//! retained.
//!
//! `syn` is already a dev-dependency of this crate for exactly this class
//! of gate; `quote` and syn's `visit-mut` feature are added for the
//! mention identity and the `#[cfg(test)]` strip below.

use std::path::{Path, PathBuf};

use quote::ToTokens as _;
use syn::visit::Visit;
use syn::visit_mut::VisitMut;

/// The crate whose names criterion 4 probes.
const CRATE: &str = "pulsus_read";

/// The `SpanSummary` item's complete non-doc attribute list, in source
/// order. A serialisation derive added in ANY position — stacked above,
/// interleaved with the doc comment, or behind a `cfg_attr` — changes
/// this list, because `syn` attaches every outer attribute of the item to
/// `attrs` regardless of what sits between them.
const PINNED_ATTRS: &[&str] = &["derive(Debug, Clone, PartialEq, Eq)"];

/// The declared fields, in order.
const PINNED_FIELDS: &[&str] = &["span_id", "name", "start_ns", "duration_ns", "attributes"];

/// `name`'s declared type.
const PINNED_NAME_TYPE: &str = "Option<String>";

/// The COMPLETE set of item-position macro invocations in the scanned
/// file, each rendered `<enclosing mod path>::<macro path>!`.
///
/// Any item macro can emit a struct AFTER expansion, which no walk over
/// explicit items can see, so the default is REJECT and the exception is
/// this committed list.
const PINNED_ITEM_MACROS: &[&str] = &["clone_probe::thread_local!"];

/// The `pulsus_read` types criterion 4 probes. Criterion 4 asserts this
/// equals the set DERIVED from the renderer's own production names, so it
/// cannot silently go stale.
const PROBED_SUBJECTS: &[&str] = &[
    "GroupValue",
    "SearchOutput",
    "SpanSetGroup",
    "SpanSummary",
    "TraceSearchResult",
];

fn read(rel: &str) -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {p:?}: {e}"))
}

fn parse(rel: &str) -> syn::File {
    let src = read(rel);
    syn::parse_file(&src).unwrap_or_else(|e| panic!("parse {rel}: {e}"))
}

// ---------------------------------------------------------------------
// identifiers
// ---------------------------------------------------------------------

/// `r#name` -> `name`.
///
/// A raw identifier and its plain spelling are the SAME identifier to the
/// compiler, but `proc_macro2::Ident` keeps the `r#` in BOTH its `Display`
/// and its `PartialEq<str>` — so EVERY comparison against the crate name,
/// in BOTH layers, goes through here: the walk's `use` root in all three
/// of its branches (path, bare name, rename), nested leaves, renames and
/// paths, `extern crate` and its rename, the type alias's two segments,
/// the `mod` name, the item-macro path segments, the `self` test, and the
/// mention comparison.
///
/// The printed diagnostic is NOT normalised: `mention_contexts` shows
/// `r#pulsus_read` exactly as written, so a failure names the spelling the
/// author used.
fn norm(s: &str) -> &str {
    s.strip_prefix("r#").unwrap_or(s)
}

fn ident_text(i: &proc_macro2::Ident) -> String {
    norm(&i.to_string()).to_string()
}

fn is_crate(i: &proc_macro2::Ident) -> bool {
    ident_text(i) == CRATE
}

fn path_str(path: &syn::Path) -> String {
    let mut out = if path.leading_colon.is_some() {
        "::".to_string()
    } else {
        String::new()
    };
    let segs: Vec<String> = path.segments.iter().map(|s| ident_text(&s.ident)).collect();
    out.push_str(&segs.join("::"));
    out
}

// ---------------------------------------------------------------------
// fail-closed descriptors (criteria 1-3)
// ---------------------------------------------------------------------

/// FAIL-CLOSED. Only `Type::Path` (with angle-bracketed arguments),
/// `Type::Array` and `Type::Tuple` are described; every other form renders
/// to a string [`PINNED_NAME_TYPE`] cannot contain.
fn type_str(t: &syn::Type) -> String {
    match t {
        syn::Type::Path(p) if p.qself.is_none() => {
            let segs: Vec<String> = p
                .path
                .segments
                .iter()
                .map(|s| {
                    let ident = ident_text(&s.ident);
                    match &s.arguments {
                        syn::PathArguments::None => ident,
                        syn::PathArguments::AngleBracketed(a) => {
                            let args: Vec<String> = a
                                .args
                                .iter()
                                .map(|arg| match arg {
                                    syn::GenericArgument::Type(t) => type_str(t),
                                    other => format!(
                                        "UNALLOWLISTED-GENERIC-ARG({})",
                                        other.to_token_stream()
                                    ),
                                })
                                .collect();
                            format!("{ident}<{}>", args.join(", "))
                        }
                        syn::PathArguments::Parenthesized(_) => {
                            format!("UNALLOWLISTED-FN-SUGAR({ident})")
                        }
                    }
                })
                .collect();
            segs.join("::")
        }
        syn::Type::Array(a) => format!("[{}; {}]", type_str(&a.elem), a.len.to_token_stream()),
        syn::Type::Tuple(t) => format!(
            "({})",
            t.elems.iter().map(type_str).collect::<Vec<_>>().join(", ")
        ),
        other => format!("UNALLOWLISTED-TYPE-FORM({})", other.to_token_stream()),
    }
}

/// FAIL-CLOSED, over the COMPLETE attribute list `syn` attaches to the
/// item, in source order, whatever sits between the attributes.
/// `#[derive(..)]` renders as `derive(A, B)`; EVERY other attribute path —
/// `cfg_attr`, `serde`, `repr`, an attribute macro — renders as
/// `UNALLOWLISTED-ATTR-FORM(<path>)`.
fn non_doc_attrs(attrs: &[syn::Attribute]) -> Vec<String> {
    attrs
        .iter()
        .filter(|a| !a.path().is_ident("doc"))
        .map(|a| {
            if a.path().is_ident("derive") {
                let mut names: Vec<String> = Vec::new();
                let parsed = a.parse_nested_meta(|meta| {
                    names.push(path_str(&meta.path));
                    Ok(())
                });
                match parsed {
                    Ok(()) => format!("derive({})", names.join(", ")),
                    Err(_) => format!("UNALLOWLISTED-DERIVE-FORM({})", a.to_token_stream()),
                }
            } else {
                format!("UNALLOWLISTED-ATTR-FORM({})", path_str(a.path()))
            }
        })
        .collect()
}

/// Every `syn::Item::Struct` named `SpanSummary` reachable through
/// EXPLICIT `mod` items, PLUS every item-position macro and every
/// `Item::Verbatim`.
///
/// It cannot see through EXPANSION, which is why item macros are PINNED
/// rather than skipped: a function-like item macro can emit the real
/// declaration while an explicit nested decoy carries the pinned shape,
/// and a walk over explicit items alone would read only the decoy.
fn collect(
    items: &[syn::Item],
    scope: &str,
    out: &mut Vec<syn::ItemStruct>,
    macros: &mut Vec<String>,
) {
    for item in items {
        match item {
            syn::Item::Struct(s) if ident_text(&s.ident) == "SpanSummary" => out.push(s.clone()),
            syn::Item::Mod(m) => {
                let inner = if scope.is_empty() {
                    ident_text(&m.ident)
                } else {
                    format!("{scope}::{}", ident_text(&m.ident))
                };
                match &m.content {
                    Some((_, nested)) => collect(nested, &inner, out, macros),
                    // A `mod x;` body lives in another file this parse
                    // never sees.
                    None => macros.push(format!("UNALLOWLISTED-FILE-MOD({inner})")),
                }
            }
            syn::Item::Macro(m) => {
                let path = path_str(&m.mac.path);
                macros.push(if scope.is_empty() {
                    format!("{path}!")
                } else {
                    format!("{scope}::{path}!")
                });
            }
            syn::Item::Verbatim(t) => macros.push(format!("UNALLOWLISTED-ITEM-VERBATIM({t})")),
            _ => {}
        }
    }
}

/// The one scanned declaration, plus the file's complete item-macro list.
fn span_summary_decl() -> (Vec<syn::ItemStruct>, Vec<String>) {
    let file = parse("src/traces/search_eval.rs");
    let mut structs = Vec::new();
    let mut macros = Vec::new();
    collect(&file.items, "", &mut structs, &mut macros);
    (structs, macros)
}

fn the_struct() -> syn::ItemStruct {
    let (structs, _) = span_summary_decl();
    assert_eq!(
        structs.len(),
        1,
        "struct_count={} — criteria 1 and 2 must read the real declaration, not one of several",
        structs.len()
    );
    structs.into_iter().next().expect("checked above")
}

fn named_fields(s: &syn::ItemStruct) -> Vec<&syn::Field> {
    match &s.fields {
        syn::Fields::Named(n) => n.named.iter().collect(),
        _ => panic!("SpanSummary must stay a braced struct with named fields"),
    }
}

// ---------------------------------------------------------------------
// criteria 1-3
// ---------------------------------------------------------------------

/// Criterion 1 — in the parsed source text, `SpanSummary`'s `name` field
/// has INHERITED visibility, a type rendering `Option<String>`, and no
/// non-doc attribute.
///
/// *RED on the tree before this issue landed*: `pub name: String`.
#[test]
fn the_name_field_is_private_and_optional() {
    let s = the_struct();
    let fields = named_fields(&s);
    let name_fields: Vec<String> = fields
        .iter()
        .filter(|f| f.ident.as_ref().is_some_and(|i| ident_text(i) == "name"))
        .map(|f| {
            let vis = match f.vis {
                syn::Visibility::Inherited => String::new(),
                _ => "pub ".to_string(),
            };
            let attrs = non_doc_attrs(&f.attrs);
            let attr_note = if attrs.is_empty() {
                String::new()
            } else {
                format!(" ATTRS{attrs:?}")
            };
            format!("{vis}name: {}{attr_note}", type_str(&f.ty))
        })
        .collect();
    assert_eq!(
        name_fields,
        vec![format!("name: {PINNED_NAME_TYPE}")],
        "name_fields={name_fields:?} — the field must be PRIVATE and Option<String>, with no \
         attribute on it: an attribute macro on the field can rewrite what the declaration says"
    );
}

/// Criterion 2 — the item's COMPLETE non-doc attribute list, in source
/// order, is exactly the pinned derive.
///
/// This is what makes criterion 1's PRE-EXPANSION reading sound: nothing
/// decorates the item that could rewrite it before expansion. It does NOT
/// establish that the type is off the wire — criterion 4 owns that, and a
/// hand-written `impl serde::Serialize` leaves this list untouched.
#[test]
fn the_struct_carries_exactly_the_pinned_attribute_block() {
    let s = the_struct();
    let attrs = non_doc_attrs(&s.attrs);
    assert_eq!(
        attrs,
        PINNED_ATTRS
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>(),
        "attrs={attrs:?}"
    );
}

/// Criterion 3 — the parsed file contains exactly ONE struct named
/// `SpanSummary`, with the pinned field names in the pinned order, public,
/// non-generic, no field attributed; and its complete list of
/// item-position macro invocations is the pinned one.
#[test]
fn exactly_one_span_summary_struct_and_only_the_pinned_item_macros() {
    let (structs, macros) = span_summary_decl();
    assert_eq!(
        macros,
        PINNED_ITEM_MACROS
            .iter()
            .map(|m| m.to_string())
            .collect::<Vec<_>>(),
        "item_macros={macros:?} — an item macro can emit a struct AFTER expansion, which no walk \
         over explicit items can see, so the list is pinned rather than skipped"
    );
    assert_eq!(structs.len(), 1, "struct_count={}", structs.len());
    let s = &structs[0];
    assert!(
        matches!(s.vis, syn::Visibility::Public(_)),
        "vis={:?} — SpanSummary stays a public type; only the one field closes",
        s.vis.to_token_stream().to_string()
    );
    assert!(
        s.generics.params.is_empty(),
        "SpanSummary must stay non-generic"
    );
    let fields = named_fields(s);
    let names: Vec<String> = fields
        .iter()
        .map(|f| ident_text(f.ident.as_ref().expect("named")))
        .collect();
    assert_eq!(
        names,
        PINNED_FIELDS
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>(),
        "fields={names:?}"
    );
    let field_attrs: Vec<String> = fields
        .iter()
        .filter_map(|f| {
            let attrs = non_doc_attrs(&f.attrs);
            (!attrs.is_empty()).then(|| format!("{:?}{attrs:?}", f.ident.as_ref().map(ident_text)))
        })
        .collect();
    assert!(field_attrs.is_empty(), "field_attrs={field_attrs:?}");
}

// ---------------------------------------------------------------------
// criterion 4 — the renderer's own production `pulsus_read` names
// ---------------------------------------------------------------------

/// Removes every `#[cfg(test)]` inline module from the AST, in ALL THREE
/// places a `syn::Item` can sit in syn 2.0.118 — `File::items`,
/// `ItemMod::content`, and `Stmt::Item` inside a `Block`.
///
/// The strip runs BEFORE both layers below so they read the SAME item list
/// and cannot disagree about what "production" means. The match is EXACT
/// on `cfg(test)`, so `#[cfg(all(test, …))]` reddens rather than hides.
struct StripTests;

fn is_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        a.path().is_ident("cfg")
            && a.meta
                .require_list()
                .is_ok_and(|l| l.tokens.to_string() == "test")
    })
}

impl VisitMut for StripTests {
    fn visit_file_mut(&mut self, f: &mut syn::File) {
        f.items
            .retain(|i| !matches!(i, syn::Item::Mod(m) if is_cfg_test(&m.attrs)));
        syn::visit_mut::visit_file_mut(self, f);
    }

    fn visit_item_mod_mut(&mut self, m: &mut syn::ItemMod) {
        if let Some((_, items)) = &mut m.content {
            items.retain(|i| !matches!(i, syn::Item::Mod(m) if is_cfg_test(&m.attrs)));
        }
        syn::visit_mut::visit_item_mod_mut(self, m);
    }

    fn visit_block_mut(&mut self, b: &mut syn::Block) {
        b.stmts
            .retain(|s| !matches!(s, syn::Stmt::Item(syn::Item::Mod(m)) if is_cfg_test(&m.attrs)));
        syn::visit_mut::visit_block_mut(self, b);
    }
}

/// LAYER 1 — the structural walk.
///
/// Recursion into blocks, impl items, trait items and expressions is
/// `syn::visit::Visit`'s own, NOT a hand-written match: a block-local
/// `use pulsus_read::X` inside a fn, an impl method, a const initialiser,
/// a trait default body or an enum discriminant all reach
/// [`Walk::visit_item_use`]. Overridden hooks are exactly `visit_item` (to
/// catch `Item::Verbatim`), `visit_item_use`, `visit_item_extern_crate`,
/// `visit_item_type`, `visit_item_macro` and `visit_item_mod`.
///
/// `explained` counts the `pulsus_read` ident tokens this walk
/// INTERPRETED: one per `use` root naming the crate, one per
/// `extern crate` naming it, one per `type A = pulsus_read::Name;`.
#[derive(Default)]
struct Walk {
    names: Vec<String>,
    rejected: Vec<String>,
    explained: usize,
}

impl Walk {
    /// The ROOT position of a `use` item: "is this our crate?".
    ///
    /// A root `Group` is a group of ROOTS, so it recurses HERE and not
    /// through [`Self::sub_tree`] — writing that recursion the other way
    /// makes `use {pulsus_read::A, pulsus_read::B};` derive the EMPTY set,
    /// which is a green-looking failure.
    fn root_tree(&mut self, t: &syn::UseTree) {
        match t {
            syn::UseTree::Path(p) => {
                if is_crate(&p.ident) {
                    self.explained += 1;
                    self.sub_tree(&p.tree);
                }
            }
            syn::UseTree::Name(n) => {
                if is_crate(&n.ident) {
                    self.explained += 1;
                    self.rejected
                        .push(format!("UNALLOWLISTED-CRATE-IMPORT({CRATE})"));
                }
            }
            syn::UseTree::Rename(r) => {
                if is_crate(&r.ident) {
                    self.explained += 1;
                    self.rejected.push(format!(
                        "UNALLOWLISTED-CRATE-ALIAS({CRATE} as {})",
                        ident_text(&r.rename)
                    ));
                }
            }
            syn::UseTree::Glob(_) => self.rejected.push("UNALLOWLISTED-ROOT-GLOB".to_string()),
            syn::UseTree::Group(g) => {
                for item in &g.items {
                    self.root_tree(item);
                }
            }
        }
    }

    /// The NESTED position, inside `pulsus_read::…`: "which name does it
    /// bring in?".
    fn sub_tree(&mut self, t: &syn::UseTree) {
        match t {
            syn::UseTree::Path(p) => self.rejected.push(format!(
                "UNALLOWLISTED-NESTED-PATH({})",
                ident_text(&p.ident)
            )),
            syn::UseTree::Name(n) => {
                if ident_text(&n.ident) == "self" {
                    self.rejected
                        .push(format!("UNALLOWLISTED-SELF-IMPORT({CRATE})"));
                } else {
                    self.names.push(ident_text(&n.ident));
                }
            }
            syn::UseTree::Rename(r) => self.rejected.push(format!(
                "UNALLOWLISTED-NESTED-RENAME({} as {})",
                ident_text(&r.ident),
                ident_text(&r.rename)
            )),
            syn::UseTree::Glob(_) => self.rejected.push("UNALLOWLISTED-GLOB-IMPORT".to_string()),
            syn::UseTree::Group(g) => {
                for item in &g.items {
                    self.sub_tree(item);
                }
            }
        }
    }
}

impl<'ast> Visit<'ast> for Walk {
    fn visit_item(&mut self, i: &'ast syn::Item) {
        if let syn::Item::Verbatim(t) = i {
            self.rejected
                .push(format!("UNALLOWLISTED-ITEM-VERBATIM({t})"));
        }
        syn::visit::visit_item(self, i);
    }

    fn visit_item_use(&mut self, i: &'ast syn::ItemUse) {
        self.root_tree(&i.tree);
    }

    fn visit_item_extern_crate(&mut self, i: &'ast syn::ItemExternCrate) {
        if is_crate(&i.ident) {
            self.explained += 1;
            let rendered = match &i.rename {
                Some((_, r)) => format!("{CRATE} as {}", ident_text(r)),
                None => CRATE.to_string(),
            };
            self.rejected
                .push(format!("UNALLOWLISTED-EXTERN-CRATE({rendered})"));
        }
    }

    fn visit_item_type(&mut self, i: &'ast syn::ItemType) {
        // `type A = pulsus_read::Name;` brings a read-crate type into
        // production scope under a local name. Exactly that shape is
        // interpreted; any other leaves its mention unexplained, which
        // layer 2 catches.
        if let syn::Type::Path(p) = i.ty.as_ref() {
            let segs: Vec<&syn::PathSegment> = p.path.segments.iter().collect();
            if p.qself.is_none() && segs.len() == 2 && is_crate(&segs[0].ident) {
                self.explained += 1;
                self.names.push(ident_text(&segs[1].ident));
            }
        }
        syn::visit::visit_item_type(self, i);
    }

    fn visit_item_macro(&mut self, i: &'ast syn::ItemMacro) {
        self.rejected.push(format!(
            "UNALLOWLISTED-ITEM-MACRO({}!)",
            path_str(&i.mac.path)
        ));
    }

    fn visit_item_mod(&mut self, i: &'ast syn::ItemMod) {
        match &i.content {
            // A production module's imports are production imports.
            Some(_) => syn::visit::visit_item_mod(self, i),
            None => self
                .rejected
                .push(format!("UNALLOWLISTED-FILE-MOD({})", ident_text(&i.ident))),
        }
    }
}

/// LAYER 2 — the mention identity. Every `pulsus_read` ident token the
/// production items emit, with a short following context so a failure
/// names the construct.
///
/// THIS IS THE PART THAT DOES NOT DEPEND ON THE ITEM ENUMERATION. A
/// variant classified wrongly above still reddens here, because a token is
/// a token whatever item contains it. It is why criterion 4 has no
/// fully-qualified hole: `fn f(_: &pulsus_read::T)` with no import at all
/// is an unexplained mention.
///
/// WHAT IT STILL CANNOT SEE, and this is the whole residual: a macro whose
/// EXPANSION introduces the name while its invocation site carries no
/// `pulsus_read` token, and a re-export reached through another root
/// (`use crate::somewhere::Alias;`).
fn mentions(items: &[syn::Item]) -> Vec<String> {
    fn flatten(stream: proc_macro2::TokenStream, out: &mut Vec<(String, bool)>) {
        for tt in stream {
            match tt {
                proc_macro2::TokenTree::Group(g) => {
                    let (open, close) = match g.delimiter() {
                        proc_macro2::Delimiter::Parenthesis => ("(", ")"),
                        proc_macro2::Delimiter::Brace => ("{", "}"),
                        proc_macro2::Delimiter::Bracket => ("[", "]"),
                        proc_macro2::Delimiter::None => ("", ""),
                    };
                    out.push((open.to_string(), false));
                    flatten(g.stream(), out);
                    out.push((close.to_string(), false));
                }
                proc_macro2::TokenTree::Ident(i) => {
                    let raw = i.to_string();
                    let hit = norm(&raw) == CRATE;
                    out.push((raw, hit));
                }
                other => out.push((other.to_string(), false)),
            }
        }
    }
    let mut stream = proc_macro2::TokenStream::new();
    for item in items {
        item.to_tokens(&mut stream);
    }
    let mut flat = Vec::new();
    flatten(stream, &mut flat);
    let mut out = Vec::new();
    for (idx, (_, hit)) in flat.iter().enumerate() {
        if !*hit {
            continue;
        }
        let end = (idx + 4).min(flat.len());
        out.push(
            flat[idx..end]
                .iter()
                .map(|(t, _)| t.as_str())
                .collect::<Vec<_>>()
                .join(" "),
        );
    }
    out
}

/// The renderer's production `pulsus_read` subject names, everything the
/// walk refuses to interpret, how many mentions it explained, and every
/// mention it saw.
fn renderer_subject_names(file: &syn::File) -> (Vec<String>, Vec<String>, usize, Vec<String>) {
    let mut file = file.clone();
    StripTests.visit_file_mut(&mut file);
    let mut walk = Walk::default();
    walk.visit_file(&file);
    let mut names = walk.names;
    names.sort();
    names.dedup();
    let mut rejected = walk.rejected;
    rejected.sort();
    rejected.dedup();
    (names, rejected, walk.explained, mentions(&file.items))
}

// The compile-time half: placement-independent, because a trait
// implementation is not a position in a file.
struct Probe<T>(std::marker::PhantomData<T>);

trait NotSerialize {
    fn is_serialize(&self) -> bool {
        false
    }
}

impl<T> NotSerialize for Probe<T> {}

impl<T: serde::Serialize> Probe<T> {
    fn is_serialize(&self) -> bool {
        true
    }
}

/// The two-form answer for one subject type: the OWNED form and the
/// SHARED-REFERENCE form.
///
/// The reference form SUBSUMES the owned one — a serialisation bound on
/// `&T` is satisfied by any impl on `T` — and it is the one the renderer
/// actually holds (`fn span_json(span: &SpanSummary, …)`). A legal
/// `impl serde::Serialize for &SpanSummary` leaves the OWNED probe
/// answering `false` while the renderer's own parameter type serialises,
/// which is why probing the owned type alone was never enough. Both are
/// probed: the owned column names which form carries the impl.
///
/// INVOKE WITH A CONCRETE TYPE, which this macro forces. A helper
/// `fn p<T>() -> bool` with an UNBOUNDED parameter resolves
/// `is_serialize` to the TRAIT DEFAULT at definition time and answers
/// `false` for EVERY `T`, including one that derives the trait — a real
/// hazard, met while building this probe. A macro cannot be written that
/// way by accident.
macro_rules! probe {
    ($t:ty) => {
        (
            Probe::<$t>(std::marker::PhantomData).is_serialize(),
            Probe::<&$t>(std::marker::PhantomData).is_serialize(),
        )
    };
}

/// Criterion 4 — the set of names the renderer's own production items
/// bring into scope equals the probed set, and for each, neither `N` nor
/// `&N` implements the serialisation trait.
///
/// Reaching this criterion needs no HTTP request and none is offered: it
/// is a source-AST parse plus a compile-time trait question.
#[test]
fn the_renderer_subject_types_do_not_implement_serde_serialize() {
    let rel = "../pulsus-server/src/traces_api/search_response.rs";
    let src = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(rel))
        .unwrap_or_else(|e| panic!("read {rel}: {e} — the renderer moved; re-point this seal"));
    let file = syn::parse_file(&src).unwrap_or_else(|e| panic!("parse {rel}: {e}"));
    let (derived, rejected, explained, mention_contexts) = renderer_subject_names(&file);

    assert!(
        rejected.is_empty(),
        "derived={derived:?} rejected={rejected:?} — the renderer names a `{CRATE}` type through \
         a shape this seal refuses to interpret"
    );
    assert!(
        !derived.is_empty(),
        "derived is EMPTY — a parse that silently finds nothing must not report green"
    );
    assert_eq!(
        explained,
        mention_contexts.len(),
        "mentions_total={} explained={explained} identity=false mention_contexts={mention_contexts:?} \
         — every `{CRATE}` token a production item emits must be one this walk interpreted",
        mention_contexts.len()
    );
    assert_eq!(
        derived,
        PROBED_SUBJECTS
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        "derived={derived:?} probed={PROBED_SUBJECTS:?} subject_set_matches=false"
    );

    // The probe's own controls, so a derivation that answers `false` for
    // everything — the degenerate shape an unbounded generic parameter
    // produces — cannot report green.
    struct NotSerializable;
    assert_eq!(
        probe!(String),
        (true, true),
        "positive control: a type that DOES implement the trait must answer true in both forms"
    );
    assert_eq!(
        probe!(NotSerializable),
        (false, false),
        "negative control: a type that does not implement it answers false in both forms"
    );

    let answers: Vec<(&str, (bool, bool))> = vec![
        ("GroupValue", probe!(pulsus_read::GroupValue)),
        ("SearchOutput", probe!(pulsus_read::SearchOutput)),
        ("SpanSetGroup", probe!(pulsus_read::SpanSetGroup)),
        ("SpanSummary", probe!(pulsus_read::SpanSummary)),
        ("TraceSearchResult", probe!(pulsus_read::TraceSearchResult)),
    ];
    assert_eq!(
        answers.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
        PROBED_SUBJECTS.to_vec(),
        "the probed list and PROBED_SUBJECTS must be the same set, in the same order"
    );
    let implementing: Vec<String> = answers
        .iter()
        .filter(|(_, (owned, by_ref))| *owned || *by_ref)
        .map(|(n, (owned, by_ref))| format!("{n}: owned={owned} ref={by_ref}"))
        .collect();
    assert!(
        implementing.is_empty(),
        "{implementing:?} — a `{CRATE}` type the renderer names implements the serialisation \
         trait, so the whole struct (including SpanSummary's private name) can reach a wire"
    );
}
