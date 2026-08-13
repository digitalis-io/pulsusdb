//! Issue #312 — an INVENTORY, not a gate, of every site under
//! `src/logql/` that grows a streams-retention container.
//!
//! **What it does.** It parses every `.rs` file directly in
//! `crates/pulsus-read/src/logql/` with `syn` and records the
//! `(file, fn, site)` of every place that grows one of the
//! streams-retention containers, then requires that set to equal a pinned
//! literal list.
//!
//! **What it is for.** It detects that the retention SURFACE CHANGED and
//! forces a human to re-read the charge beside it. It does **not** — and
//! cannot — prove that any listed site charges anything. A site can
//! appear on this list and be completely uncharged; nothing here would
//! notice.
//!
//! **What proves charging is RUNTIME**, in
//! `tests/logql_streams_result_budget.rs`: the conservation identity
//! (`the_ledger_equals_what_came_back`, which asserts EQUALITY between
//! the ledger and the returned footprint, so an under-charge that never
//! trips the cap still reddens) and the staged ceiling
//! (`staged_bytes_are_bounded_by_the_chunk`). Those are path-driven and
//! go red on an uncharged retention the corpus reaches. This file is the
//! tripwire for one the corpus does NOT reach.
//!
//! **What it cannot see**, stated so nobody reads the pinned list as an
//! enumeration of reality:
//!
//! - a container built OUTSIDE this module tree — the walk is
//!   `src/logql/*.rs`, FLAT, so a subdirectory module such as
//!   `template/` is invisible to it, exactly like the other censuses
//!   over this directory;
//! - a retention that moves through a GENERIC, a trait object or a macro
//!   body, where no receiver name appears in the source;
//! - a `Vec` reached through a type ALIAS, or a container whose local
//!   binding is named something outside the receiver set — a `HashMap`
//!   vacant-entry binding (`Vacant(e) => e.insert(..)`) is exactly that
//!   case, and it is inventoried through the `StreamResult { .. }` /
//!   `FanOutGroup { .. }` literal inside it rather than through the
//!   `insert`;
//! - anything in a `#[cfg(test)]` item or module, which is skipped.
//!
//! **How the receiver set is derived, and where it over-includes.** It is
//! not a guessed list of names: pass A collects, per FILE, every `let`
//! binding and every struct field DECLARED with one of the retaining
//! types below, and pass B records container-growing calls whose receiver
//! is one of those names. That is what keeps the metric path's own
//! `groups`/`rows` (`post_agg.rs`, `client_agg.rs`, `fold.rs` — different
//! types entirely) off the list. The set is per FILE, not per scope, so a
//! same-named container of a DIFFERENT type in the same file is
//! over-included; `exec.rs`'s metric-path `chunk`/`by_fp`/`rows` are
//! there for that reason and are marked. Over-inclusion is the safe
//! direction for a tripwire: it can add a finding, never hide one.

use std::collections::BTreeSet;

use syn::visit::Visit;

/// The container-growing operations the visitor matches.
const OPS: &[&str] = &[
    "push",
    "extend",
    "extend_from_slice",
    "append",
    "insert",
    "or_insert_with",
    "or_default",
    "push_back",
    "resize",
    "collect",
];

/// The struct literals that MATERIALISE a retained streams container.
const LITERALS: &[&str] = &["StreamResult", "FanOutGroup"];

/// A `let` binding or a struct FIELD whose type is one of these is a
/// retention container by declaration, whatever is later done to it.
const RETAINING_TYPES: &[&str] = &[
    "Vec<(i64,String)>",
    "Vec<SampleRow>",
    "Vec<TailSampleRow>",
    "Vec<StreamResult>",
    "HashMap<u64,Vec<(i64,String)>>",
    "HashMap<u64,StreamResult>",
    "HashMap<String,FanOutGroup>",
];

// ---------------------------------------------------------------------
// Dependency-free rendering. `quote` is deliberately NOT added to this
// crate's dev-dependencies for one census, so types and receivers are
// reduced from the AST by hand — which also makes the normalisation
// (last path segment only, whitespace-free) explicit rather than a
// property of somebody else's `Display`.
// ---------------------------------------------------------------------

/// `HashMap < u64 , Vec < (i64 , String) > >` -> `HashMap<u64,Vec<(i64,String)>>`,
/// using only each path's LAST segment so `std::collections::HashMap<..>`
/// and `HashMap<..>` read the same.
fn type_text(t: &syn::Type) -> String {
    match t {
        syn::Type::Path(p) => match p.path.segments.last() {
            Some(seg) => {
                let mut out = seg.ident.to_string();
                if let syn::PathArguments::AngleBracketed(a) = &seg.arguments {
                    out.push('<');
                    for (i, arg) in a.args.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        match arg {
                            syn::GenericArgument::Type(t) => out.push_str(&type_text(t)),
                            _ => out.push('?'),
                        }
                    }
                    out.push('>');
                }
                out
            }
            None => "?".to_string(),
        },
        syn::Type::Tuple(t) => {
            let mut out = String::from("(");
            for (i, e) in t.elems.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&type_text(e));
            }
            out.push(')');
            out
        }
        syn::Type::Reference(r) => format!("&{}", type_text(&r.elem)),
        syn::Type::Slice(s) => format!("[{}]", type_text(&s.elem)),
        _ => "?".to_string(),
    }
}

/// The container a method call ultimately grows: the last named binding
/// or field on its receiver chain, so `self.chunk.push(..)`,
/// `fp_groups.entry(k).or_insert_with(..)` and
/// `e.into_mut().entries.push(..)` all reduce to one name.
fn receiver_name(e: &syn::Expr) -> Option<String> {
    match e {
        syn::Expr::Path(p) => p.path.segments.last().map(|s| s.ident.to_string()),
        syn::Expr::Field(f) => match &f.member {
            syn::Member::Named(i) => Some(i.to_string()),
            syn::Member::Unnamed(_) => None,
        },
        syn::Expr::MethodCall(m) => receiver_name(&m.receiver),
        syn::Expr::Index(i) => receiver_name(&i.expr),
        syn::Expr::Paren(p) => receiver_name(&p.expr),
        syn::Expr::Reference(r) => receiver_name(&r.expr),
        syn::Expr::Unary(u) => receiver_name(&u.expr),
        _ => None,
    }
}

fn is_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        a.path().is_ident("cfg")
            && matches!(&a.meta, syn::Meta::List(l) if l.tokens.to_string().replace(' ', "") == "test")
    })
}

/// Pass A: every name in ONE file declared with a retaining type.
#[derive(Default)]
struct Decls {
    names: BTreeSet<String>,
}

impl Visit<'_> for Decls {
    fn visit_item_mod(&mut self, node: &syn::ItemMod) {
        if is_cfg_test(&node.attrs) {
            return;
        }
        syn::visit::visit_item_mod(self, node);
    }
    fn visit_local(&mut self, node: &syn::Local) {
        if let syn::Pat::Type(pt) = &node.pat
            && RETAINING_TYPES.contains(&type_text(&pt.ty).as_str())
            && let syn::Pat::Ident(id) = &*pt.pat
        {
            self.names.insert(id.ident.to_string());
        }
        syn::visit::visit_local(self, node);
    }
    fn visit_field(&mut self, node: &syn::Field) {
        if RETAINING_TYPES.contains(&type_text(&node.ty).as_str())
            && let Some(id) = &node.ident
        {
            self.names.insert(id.to_string());
        }
        syn::visit::visit_field(self, node);
    }
}

/// Pass B.
#[derive(Default)]
struct Inv {
    file: String,
    receivers: BTreeSet<String>,
    fnstack: Vec<String>,
    found: BTreeSet<(String, String, String)>,
}

impl Inv {
    fn record(&mut self, what: String) {
        let f = self
            .fnstack
            .last()
            .cloned()
            .unwrap_or_else(|| "<item>".to_string());
        self.found.insert((self.file.clone(), f, what));
    }
}

impl Visit<'_> for Inv {
    fn visit_item_mod(&mut self, node: &syn::ItemMod) {
        if is_cfg_test(&node.attrs) {
            return;
        }
        syn::visit::visit_item_mod(self, node);
    }

    fn visit_item_fn(&mut self, node: &syn::ItemFn) {
        if is_cfg_test(&node.attrs) {
            return;
        }
        self.fnstack.push(node.sig.ident.to_string());
        syn::visit::visit_item_fn(self, node);
        self.fnstack.pop();
    }

    fn visit_impl_item_fn(&mut self, node: &syn::ImplItemFn) {
        if is_cfg_test(&node.attrs) {
            return;
        }
        self.fnstack.push(node.sig.ident.to_string());
        syn::visit::visit_impl_item_fn(self, node);
        self.fnstack.pop();
    }

    fn visit_expr_method_call(&mut self, node: &syn::ExprMethodCall) {
        let method = node.method.to_string();
        if OPS.contains(&method.as_str())
            && let Some(recv) = receiver_name(&node.receiver)
            && self.receivers.contains(&recv)
        {
            self.record(format!("{recv}.{method}"));
        }
        syn::visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_struct(&mut self, node: &syn::ExprStruct) {
        if let Some(seg) = node.path.segments.last() {
            let name = seg.ident.to_string();
            if LITERALS.contains(&name.as_str()) {
                self.record(format!("{name}{{..}}"));
            }
        }
        syn::visit::visit_expr_struct(self, node);
    }

    fn visit_local(&mut self, node: &syn::Local) {
        if let syn::Pat::Type(pt) = &node.pat {
            let ty = type_text(&pt.ty);
            if RETAINING_TYPES.contains(&ty.as_str()) {
                self.record(format!("let:{ty}"));
            }
        }
        syn::visit::visit_local(self, node);
    }

    fn visit_field(&mut self, node: &syn::Field) {
        let ty = type_text(&node.ty);
        if RETAINING_TYPES.contains(&ty.as_str()) {
            let name = node
                .ident
                .as_ref()
                .map(|i| i.to_string())
                .unwrap_or_else(|| "<tuple>".to_string());
            self.record(format!("field:{name}:{ty}"));
        }
        syn::visit::visit_field(self, node);
    }
}

fn collect() -> BTreeSet<(String, String, String)> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/logql");
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .expect("the logql source directory")
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .collect();
    files.sort();
    assert!(
        files.len() >= 10,
        "only {} source files found in {dir:?} — the inventory is looking in the wrong place",
        files.len()
    );
    let mut inv = Inv::default();
    for path in &files {
        inv.file = path
            .file_name()
            .expect("file name")
            .to_string_lossy()
            .into_owned();
        let text = std::fs::read_to_string(path).expect("read source");
        let parsed = syn::parse_file(&text)
            .unwrap_or_else(|e| panic!("{} does not parse as Rust: {e}", inv.file));
        // Pass A, then pass B over the same file.
        let mut decls = Decls::default();
        decls.visit_file(&parsed);
        inv.receivers = decls.names;
        inv.visit_file(&parsed);
    }
    inv.found
}

/// The pinned surface, `(file, fn, site)`. Adding an uncharged
/// `entries.extend(...)` anywhere under `src/logql/` changes this set and
/// reddens the test — which is ALL it does: it says nothing about
/// whether any of these sites charges.
const PINNED: &[(&str, &str, &str)] = &[
    // --- detected_probe.rs: the fan-out group map and its drain.
    (
        "detected_probe.rs",
        "<item>",
        "field:entries:Vec<(i64,String)>",
    ),
    (
        "detected_probe.rs",
        "<item>",
        "field:groups:HashMap<String,FanOutGroup>",
    ),
    ("detected_probe.rs", "into_streams", "StreamResult{..}"),
    ("detected_probe.rs", "into_streams", "groups.collect"),
    ("detected_probe.rs", "push_fanout_entry", "FanOutGroup{..}"),
    ("detected_probe.rs", "push_fanout_entry", "entries.push"),
    // --- exec.rs: the declared containers.
    ("exec.rs", "<item>", "field:by_fp:HashMap<u64,StreamResult>"),
    ("exec.rs", "<item>", "field:chunk:Vec<SampleRow>"),
    ("exec.rs", "<item>", "field:entries:Vec<(i64,String)>"),
    (
        "exec.rs",
        "<item>",
        "field:fp_groups:HashMap<u64,StreamResult>",
    ),
    ("exec.rs", "<item>", "field:items:Vec<StreamResult>"),
    (
        "exec.rs",
        "<item>",
        "field:label_groups:HashMap<String,FanOutGroup>",
    ),
    ("exec.rs", "<item>", "field:streams:Vec<StreamResult>"),
    // --- exec.rs: the accumulator's two retention branches.
    ("exec.rs", "feed", "StreamResult{..}"),
    ("exec.rs", "feed", "entries.push"),
    // --- exec.rs: the line-filter-only fast path.
    ("exec.rs", "push_row", "StreamResult{..}"),
    ("exec.rs", "push_row", "entries.push"),
    // --- exec.rs: the byte-denominated staging chunk (issue #312).
    ("exec.rs", "push_row", "chunk.push"),
    // --- exec.rs: the drains.
    ("exec.rs", "into_streams", "StreamResult{..}"),
    ("exec.rs", "into_streams", "by_fp.collect"),
    ("exec.rs", "into_streams", "fp_groups.collect"),
    ("exec.rs", "into_streams", "let:Vec<StreamResult>"),
    ("exec.rs", "into_streams", "streams.extend"),
    // --- OVER-INCLUDED: metric-path containers in `exec.rs` that share a
    // --- name with a streams one (see the header's per-FILE note). These
    // --- are NOT streams retention and are charged, where they are
    // --- charged at all, against the metric caps.
    ("exec.rs", "run_metric_client", "chunk.push"),
    // Issue #241 removed `run_metric_inner`'s three `by_fp` rows with the
    // SQL-aggregated RANGE arm that held them: that arm was structurally
    // unreachable (`metric_plan` forces `client = Some(..)` for every
    // `QuerySpec::Range`), so the sites are gone rather than re-homed.
    ("exec.rs", "run_variants", "chunk.push"),
];

#[test]
fn the_streams_retention_surface_is_pinned() {
    let found = collect();
    if std::env::var("PULSUS_INVENTORY_PRINT").is_ok() {
        for (f, n, s) in &found {
            println!("(\"{f}\", \"{n}\", \"{s}\"),");
        }
    }
    let pinned: BTreeSet<(String, String, String)> = PINNED
        .iter()
        .map(|(f, n, s)| (f.to_string(), n.to_string(), s.to_string()))
        .collect();

    let extra: Vec<_> = found.difference(&pinned).collect();
    let missing: Vec<_> = pinned.difference(&found).collect();
    assert!(
        extra.is_empty() && missing.is_empty(),
        "the streams retention surface moved.\n  NEW, unpinned sites (re-read the charge \
         beside each, then pin it): {extra:#?}\n  PINNED sites that no longer exist: \
         {missing:#?}"
    );
}

/// The header's own honesty, asserted rather than trusted: this file says
/// what it is, in these words, so a future reader cannot mistake it for a
/// proof that the listed sites charge.
#[test]
fn the_inventory_says_it_is_not_a_gate() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/logql_streams_retention_inventory.rs"),
    )
    .expect("this file is readable");
    let header: String = src
        .lines()
        .take_while(|l| l.starts_with("//!") || l.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        header.contains("INVENTORY, not a gate"),
        "the header must say, in these words, that it is an INVENTORY, not a gate"
    );
    assert!(
        header.contains("What proves charging is RUNTIME"),
        "the header must name what DOES prove charging"
    );
}
