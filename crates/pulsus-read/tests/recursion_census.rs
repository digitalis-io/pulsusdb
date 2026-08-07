//! Issue #272 — the SCC drift tripwire.
//!
//! **This is a drift tripwire, not a coverage proof, and it is a SOURCE
//! SCAN.** It re-derives the recursive-type inventory by discovering
//! type declarations that hold an SCC member, over every `.rs` file
//! found by reading the two crates' source directories at test time —
//! nothing about the file set or the declaration set is hard-coded — and
//! fails when the shape it pins moves.
//!
//! **What a source scan cannot see, stated plainly rather than claimed
//! away:** a slot introduced by a declarative or procedural macro
//! expansion; a type pulled in by `include!` or generated into
//! `OUT_DIR`; a slot spelled through a `type` alias
//! (`type Kid = Box<MetricExpr>;`); a `cfg_attr`-conditional `derive`;
//! and a new recursive type in a crate outside the two swept here. None
//! of those is what keeps the walks iterative — that is the compiler's
//! job (`Child`/`ChildVec` implement nothing, so per-child dispatch
//! cannot be written; a `#[derive]` beside a manual impl is a
//! conflicting-implementation error) and the paired pinned-stack gates'.
//! This file's job is to notice when something changes. What proves the walks are
//! iterative is the compiler (`Child`/`ChildVec` implement nothing, so
//! per-child dispatch cannot be written), the exhaustive `match`es with
//! no `_` arm, the conflicting-impl error that stops a derive regrowing,
//! and the behavioural gates. A static enumeration cannot validate its
//! own completeness; this file's job is to notice when something changes.
//!
//! Checks:
//!
//! * **(a)** the SCC set equals the pinned set;
//! * **(b)** the child-slot census is exactly the pinned slot/line split,
//!   and every slot is spelled `Child`/`ChildVec`;
//! * **(c)** no `#[derive]` appears on a converted SCC member;
//! * **(d)** the workspace-wide set of `impl Scc for` blocks equals the
//!   pinned inventory;
//! * **(e)** `walk.rs` implements none of the forbidden traits for
//!   `Child`/`ChildVec`/the handles, and declares exactly one `Walk`
//!   constructor, private;
//! * **(f)** `&Walk` appears only in the four handle openers and in the
//!   two token-consuming `Scc` methods — in no other signature, return
//!   type or callback parameter anywhere;
//! * **(g)** every `Scc::child` arm is a literal index ladder or a single
//!   `slice.get(i)`, both prefix-closed by construction;
//! * **(h)** every `walk::dismantle` call site is inside an `impl Drop`
//!   body, one per converted SCC;
//! * **(i)** no `cfg` attribute appears on an item inside the `Scc` trait
//!   block or inside any `impl Scc` block, so the trait surface is
//!   identical in every build mode and in every dependent crate;
//! * **(j)** the single-opened-child escape hatch stays deleted: neither
//!   source tree declares or calls `walk::child_of`, and the module that
//!   existed only to hold its two callers (`plan_legacy_descent.rs`) is
//!   gone. Issue #293 converted `build_metric_node`, which was the sole
//!   consumer; this notices a regrowth.

use std::collections::BTreeSet;

use pulsus_read::logql::pipeline::CompiledPipeline;

/// The two source trees this census sweeps. Read from disk at test time
/// rather than `include_str!`-ed one by one, so a NEW file in either is
/// swept automatically instead of being silently outside the scan.
const ROOTS: &[(&str, &str)] = &[
    ("pulsus-logql/src", "../pulsus-logql/src"),
    ("pulsus-read/src/logql", "src/logql"),
];

/// Every `.rs` file under [`ROOTS`], as `(display path, source)`.
/// Non-recursive, matching the two directory censuses that already exist
/// over `src/logql/` (`charge.rs`).
fn sweep() -> Vec<(String, String)> {
    let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    for (label, rel) in ROOTS {
        let dir = base.join(rel);
        let mut names: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
            .map(|e| e.expect("dir entry").path())
            .filter(|p| p.extension().is_some_and(|x| x == "rs"))
            .collect();
        names.sort();
        for path in names {
            let name = path
                .file_name()
                .expect("file name")
                .to_string_lossy()
                .into_owned();
            let src = std::fs::read_to_string(&path).expect("source");
            out.push((format!("{label}/{name}"), src));
        }
    }
    assert!(
        out.len() >= 20,
        "the sweep collapsed to {} files — a wrong root path silently covers less than the \
         module it claims to",
        out.len()
    );
    out
}

/// Files whose module declaration is `#[cfg(test)]`, DERIVED by reading
/// how each file is declared rather than by naming them. Their contents
/// exist only in a test build; the drop oracles' mirrors live there and
/// are DELIBERATELY recursive — being a per-node recursion is their job
/// as the stack gates' control.
fn test_only_files(files: &[(String, String)]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (_, src) in files {
        let lines: Vec<&str> = src.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            let t = line.trim();
            let stem = if let Some(rest) = t.strip_prefix("mod ") {
                rest.trim_end_matches(';').to_string()
            } else {
                continue;
            };
            let window = lines[i.saturating_sub(3)..i].join("\n");
            if window.contains("#[cfg(test)]") {
                out.insert(format!("{stem}.rs"));
                if let Some(pi) = window.find("#[path = \"") {
                    let rest = &window[pi + 10..];
                    if let Some(end) = rest.find('"') {
                        out.insert(rest[..end].to_string());
                    }
                }
            }
        }
    }
    out
}

fn production(files: &[(String, String)]) -> Vec<(String, String)> {
    let skip = test_only_files(files);
    files
        .iter()
        .filter(|(n, _)| {
            let base = n.rsplit('/').next().unwrap_or(n);
            !skip.contains(base)
        })
        .cloned()
        .collect()
}

fn source_of(files: &[(String, String)], suffix: &str) -> String {
    files
        .iter()
        .find(|(n, _)| n.ends_with(suffix))
        .map(|(_, s)| code_only(s))
        .unwrap_or_else(|| panic!("{suffix} not found in the sweep"))
}

/// Source with `//`-comments removed, so a token inside prose never
/// counts as a use. Deliberately crude: drift detection is the goal.
fn code_only(src: &str) -> String {
    src.lines()
        .map(|l| match l.find("//") {
            Some(i) => &l[..i],
            None => l,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------
// (a)/(b) — the inventory
// ---------------------------------------------------------------------

/// One declared SCC-internal child slot. A *slot* is one field
/// position; a *line* is one source line of a type declaration. One line
/// can declare two slots (`And(Box<T>, Box<T>)`) and one slot can hold N
/// children at run time (`ChildVec`) — that distinction is the whole
/// reason this check exists, so occurrences are counted, never lines.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Slot {
    file: String,
    decl: String,
    line: usize,
    spelling: String,
}

/// The SCC members whose slots this census counts, DISCOVERED rather
/// than listed: every type declared in the swept files whose own
/// declaration body holds a `Box`/`Vec`/`Child`/`ChildVec` of a type
/// declared in those same files, closed under one round of expansion
/// (which is enough for these graphs and is asserted to have converged).
fn discover_members(files: &[(String, String)]) -> Vec<String> {
    let decls = all_declarations(files);
    let names: Vec<&str> = decls.iter().map(|(_, n, _)| n.as_str()).collect();
    let mut members: Vec<String> = Vec::new();
    for round in 0..8 {
        let before = members.len();
        for (_, name, body) in &decls {
            if members.iter().any(|m| m == name) {
                continue;
            }
            let holds_self_or_member = names.iter().any(|other| {
                (*other == name || members.iter().any(|m| m == other))
                    && wrapper_hits(body, other).next().is_some()
            });
            if holds_self_or_member {
                members.push(name.clone());
            }
        }
        if members.len() == before {
            break;
        }
        assert!(round < 7, "the member discovery did not converge");
    }
    members.sort();
    members
}

/// `(file, type name, declaration body)` for every `enum`/`struct` in
/// the sweep.
fn all_declarations(files: &[(String, String)]) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for (name, raw) in &production(files) {
        let code = code_only(raw);
        for (i, line) in code.lines().enumerate() {
            let t = line.trim_start();
            let Some(rest) = t
                .strip_prefix("pub enum ")
                .or_else(|| t.strip_prefix("enum "))
                .or_else(|| t.strip_prefix("pub struct "))
                .or_else(|| t.strip_prefix("struct "))
                .or_else(|| t.strip_prefix("pub(crate) enum "))
                .or_else(|| t.strip_prefix("pub(crate) struct "))
            else {
                continue;
            };
            if !line.ends_with('{') {
                continue;
            }
            let ty = rest.trim_end_matches('{').trim().to_string();
            if ty.contains('<') || ty.is_empty() {
                continue;
            }
            let header = line.to_string();
            let Some((_, body)) = decl_body_at(&code, &header, i) else {
                continue;
            };
            out.push((name.clone(), ty, body));
        }
    }
    out
}

/// Occurrences of `Box<T>` / `Vec<T>` / `Child<T>` / `ChildVec<T>` in
/// `body`, as `(line offset, wrapper)`. The wrapper must start on an
/// identifier boundary, or `ChildVec<MetricExpr>` would also count as a
/// `Vec<MetricExpr>` — one spelling being a prefix of another is exactly
/// how a census silently over-counts.
fn wrapper_hits<'b>(body: &'b str, ty: &str) -> impl Iterator<Item = (usize, &'static str)> + 'b {
    const WRAPPERS: [&str; 4] = ["Box<", "Vec<", "Child<", "ChildVec<"];
    let pats: Vec<(String, &'static str)> = WRAPPERS
        .iter()
        .map(|w| (format!("{w}{ty}>"), w.trim_end_matches('<')))
        .collect();
    body.lines().enumerate().flat_map(move |(i, raw)| {
        let mut hits = Vec::new();
        for (pat, spelling) in &pats {
            for (at, _) in raw.match_indices(pat.as_str()) {
                let boundary = at == 0
                    || (!raw.as_bytes()[at - 1].is_ascii_alphanumeric()
                        && raw.as_bytes()[at - 1] != b'_');
                if boundary {
                    hits.push((i, *spelling));
                }
            }
        }
        hits
    })
}

/// The `{ … }` body of the declaration whose header line is at
/// `line_idx`, with the 1-based source line of that header.
fn decl_body_at(src: &str, header: &str, line_idx: usize) -> Option<(usize, String)> {
    let at = src
        .lines()
        .take(line_idx)
        .map(|l| l.len() + 1)
        .sum::<usize>();
    let rel = src[at..].find(header.trim_start())?;
    let open = at + rel + header.trim_start().len() - 1;
    let bytes = src.as_bytes();
    let mut depth = 0usize;
    for (i, b) in bytes.iter().enumerate().skip(open) {
        match b {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some((line_idx + 1, src[open..=i].to_string()));
                }
            }
            _ => {}
        }
    }
    None
}

fn declared_slots(files: &[(String, String)], members: &[String]) -> Vec<Slot> {
    let mut out = Vec::new();
    for (file, ty, body) in all_declarations(files) {
        if !members.contains(&ty) {
            continue;
        }
        let head_line = {
            let code = source_of(files, file.rsplit('/').next().unwrap_or(&file));
            code.lines()
                .position(|l| l.trim_start().ends_with(&format!("{ty} {{")))
                .unwrap_or(0)
                + 1
        };
        for m in members {
            for (off, spelling) in wrapper_hits(&body, m) {
                out.push(Slot {
                    file: file.clone(),
                    decl: ty.clone(),
                    line: head_line + off,
                    spelling: spelling.to_string(),
                });
            }
        }
    }
    out.sort();
    out
}

/// Recursive types whose depth is bounded by the PARSER that builds them,
/// not by the query text — so the #272 machinery (opaque `Child`/
/// `ChildVec` handles, `impl Scc`, an iterative `Drop`) buys them
/// nothing and check (b) deliberately does not apply.
///
/// `WireJson` (issue #334) is the only one. It is built solely by
/// `serde_json`, whose deserializer refuses a document nested past its
/// own recursion limit, so no log line can make one deeper than that:
/// measured at 127 levels parsing, 128 a `JSONParserErr` and 5 000 the
/// same rather than a stack overflow
/// (`logql_json_flatten_budget.rs::json_nesting_past_the_parser_limit_is_a_parse_error`).
/// The four AST members are a different case entirely — their depth is
/// bounded only by the query-text cap, thousands of levels, which is why
/// #272 exists.
///
/// This is an exemption from (b), NOT from (a): the type still has to
/// appear in the discovered set below, so a sixth recursive type — or
/// this one losing its parser bound — still reddens the file.
///
/// The bound itself is pinned by
/// [`the_depth_bound_the_exemption_rests_on_is_enforced_by_the_parser`],
/// which lives in THIS file rather than beside the parser: an exemption
/// and the precondition that justifies it are worth nothing apart, and
/// deleting one next to the other is a visible act.
const DEPTH_BOUNDED_BY_THEIR_PARSER: &[&str] = &["WireJson"];

/// **The exemption's precondition, as a breakable check rather than a
/// measurement in a comment** (issue #334, adjudicated).
///
/// [`DEPTH_BOUNDED_BY_THEIR_PARSER`] is safe only while the depth of the
/// types it names is capped by the parser that builds them — and that cap
/// **is not ours**. It is `serde_json`'s deserializer recursion limit, a
/// number a `cargo update` can raise and a parser swap can remove
/// outright. Either would make the exemption false while every check
/// above stayed green, so the bound is asserted from BOTH sides here:
///
/// * one level under the limit PARSES, so a bump that LOWERS it fails;
/// * one level over it is refused, so a bump that RAISES it fails;
/// * far over it is refused the same way rather than overflowing the
///   stack, which is the property #272's machinery exists to provide and
///   this type gets from the parser instead.
///
/// Driven through `| json`, because `WireJson` is private to
/// `pipeline.rs`: the observable is the ordinary malformed-line class —
/// line kept, `__error__="JSONParserErr"`, nothing extracted — and a
/// budget breach would be a different return, which the `expect` below
/// separates.
#[test]
fn the_depth_bound_the_exemption_rests_on_is_enforced_by_the_parser() {
    /// One level under `serde_json`'s limit at the pinned version.
    const DEEPEST_ACCEPTED: usize = 127;

    fn nested(depth: usize) -> String {
        let mut s = String::with_capacity(depth * 6 + 8);
        for _ in 0..depth {
            s.push_str("{\"a\":");
        }
        s.push('1');
        for _ in 0..depth {
            s.push('}');
        }
        s
    }
    let expr = pulsus_logql::parse(r#"{app="a"} | json"#).expect("parse");
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("expected a log query")
    };
    let compiled = CompiledPipeline::compile(&log.pipeline).expect("compile");
    let names = |line: &str| -> Vec<String> {
        compiled
            .run(line, &[], 0)
            .expect("a nesting breach is a PARSE error, never a budget breach")
            .expect("the line is kept")
            .labels
            .iter()
            .map(|(k, _)| k.to_string())
            .collect()
    };

    // `DEEPEST_ACCEPTED` nested `{"a": …}` flatten to one leaf `a_a_…_a`.
    let leaf: String = std::iter::repeat_n("a", DEEPEST_ACCEPTED)
        .collect::<Vec<_>>()
        .join("_");
    assert_eq!(
        names(&nested(DEEPEST_ACCEPTED)),
        vec![leaf],
        "the parser stopped accepting {DEEPEST_ACCEPTED} levels — the limit MOVED DOWN, and \
         `DEPTH_BOUNDED_BY_THEIR_PARSER` now names a different bound than it claims"
    );
    let refused = vec!["__error__".to_string(), "__error_details__".to_string()];
    assert_eq!(
        names(&nested(DEEPEST_ACCEPTED + 1)),
        refused,
        "the parser accepted {} levels — the limit MOVED UP, so the bound the exemption \
         rests on is no longer the one measured for it",
        DEEPEST_ACCEPTED + 1
    );
    // Far past any plausible limit: if the cap were removed rather than
    // moved, this recurses instead of returning and takes the stack with
    // it. Reaching the assertion at all is half the result.
    assert_eq!(
        names(&nested(50_000)),
        refused,
        "50 000 levels must be REFUSED by the parser, not recursed through"
    );
}

#[test]
fn check_a_and_b_the_slot_census_matches_the_pinned_inventory() {
    let files = sweep();
    let members = discover_members(&files);
    // (a) the SCC member set, DISCOVERED from declared field types.
    assert_eq!(
        members,
        vec![
            "LabelFilterExpr".to_string(),
            "MetricExpr".to_string(),
            "MetricNode".to_string(),
            "VariantsExpr".to_string(),
            "WireJson".to_string(),
        ],
        "the recursive-type set moved. `CompiledLabelFilter` left it when #272 flattened it \
         to a post-order `Vec<LfOp>`; `WireJson` joined it in #334 and is exempt from (b) by \
         `DEPTH_BOUNDED_BY_THEIR_PARSER`; anything else joining or leaving is a design change."
    );
    // The exemption cannot name a type the scan did not find, so it
    // cannot rot into a list of types that no longer exist.
    for exempt in DEPTH_BOUNDED_BY_THEIR_PARSER {
        assert!(
            members.iter().any(|m| m == exempt),
            "{exempt} is exempted from (b) but is no longer a discovered recursive type — \
             drop it from DEPTH_BOUNDED_BY_THEIR_PARSER"
        );
    }

    let converted: Vec<String> = members
        .iter()
        .filter(|m| !DEPTH_BOUNDED_BY_THEIR_PARSER.contains(&m.as_str()))
        .cloned()
        .collect();
    let slots = declared_slots(&files, &converted);
    // (b) every surviving slot is spelled `Child`/`ChildVec` — no `Box`
    // or `Vec` of an SCC member remains anywhere.
    let unconverted: Vec<&Slot> = slots
        .iter()
        .filter(|s| s.spelling == "Box" || s.spelling == "Vec")
        .collect();
    assert!(
        unconverted.is_empty(),
        "these SCC slots are still raw `Box`/`Vec`, so their type keeps derivable traits and \
         a recursive walk: {unconverted:#?}"
    );

    // 14 SLOTS across 12 declaration LINES — the post-Wave-2 inventory
    // plus issue #276's `label_replace`: SCC-1 `LabelFilterExpr` 4 slots
    // / 2 lines, SCC-2 `MetricExpr` + `VariantsExpr` 6 / 6
    // (`MetricExpr::LabelReplace.inner` joined with #276), SCC-3
    // `MetricNode` 4 / 4 (`MetricNode::LabelReplace.inner` likewise).
    // SCC-4's 4 slots and 2 lines ceased to exist with the flattening.
    let lines: BTreeSet<(&str, usize)> = slots.iter().map(|s| (s.file.as_str(), s.line)).collect();
    assert_eq!(slots.len(), 14, "14 SLOTS (slots, not lines): {slots:#?}");
    assert_eq!(lines.len(), 12, "12 declaration LINES (lines, not slots)");
    let by_decl = |d: &str| slots.iter().filter(|s| s.decl == d).count();
    assert_eq!(by_decl("LabelFilterExpr"), 4);
    assert_eq!(by_decl("MetricExpr"), 5);
    assert_eq!(by_decl("VariantsExpr"), 1);
    assert_eq!(by_decl("MetricNode"), 4);
}

// ---------------------------------------------------------------------
// (c) — no `#[derive]` on a converted member
// ---------------------------------------------------------------------

#[test]
fn check_c_no_derive_survives_on_a_converted_scc_member() {
    let files = sweep();
    for (name, decl) in [
        ("ast.rs", "pub enum MetricExpr {"),
        ("ast.rs", "pub struct VariantsExpr {"),
        ("ast.rs", "pub enum LabelFilterExpr {"),
        ("plan.rs", "pub enum MetricNode {"),
    ] {
        let code = source_of(&files, name);
        let at = code
            .find(decl)
            .unwrap_or_else(|| panic!("{decl} not found"));
        let head = &code[..at];
        let last = head.rfind("\n\n").map_or(0, |i| i + 2);
        assert!(
            !head[last..].contains("#[derive("),
            "a `#[derive]` reappeared on {decl} — it can only compile by re-opening \
             per-child dispatch"
        );
    }
}

// ---------------------------------------------------------------------
// (d) — the `impl Scc` inventory
// ---------------------------------------------------------------------

#[test]
fn check_d_the_impl_scc_set_equals_the_pinned_inventory() {
    let files = sweep();
    let mut found: Vec<String> = Vec::new();
    for (name, src) in &files {
        for line in code_only(src).lines() {
            let t = line.trim();
            if t.starts_with("impl Scc for") || t.starts_with("impl walk::Scc for") {
                found.push(format!("{name}: {t}"));
            }
        }
    }
    found.sort();
    assert_eq!(
        found,
        vec![
            "pulsus-logql/src/ast.rs: impl Scc for LabelFilterScc {".to_string(),
            "pulsus-logql/src/ast.rs: impl Scc for MetricScc {".to_string(),
            "pulsus-read/src/logql/plan.rs: impl walk::Scc for MetricNodeScc {".to_string(),
        ],
        "the `impl Scc` set moved — one per surviving SCC, and no more"
    );
}

// ---------------------------------------------------------------------
// (e) — the handles implement nothing, and `Walk` has one private ctor
// ---------------------------------------------------------------------

#[test]
fn check_e_the_handles_implement_nothing_and_walk_has_one_private_constructor() {
    let files = sweep();
    let code = source_of(&files, "walk.rs");
    for forbidden in [
        "impl<T> fmt::Display for Child",
        "impl<T> PartialEq for Child",
        "impl<T> Hash for Child",
        "impl<T> std::ops::Deref for Child",
        "impl<T> Deref for Child",
        "impl<T> fmt::Debug for Child",
        "impl<T> Clone for Child",
        "impl<T> Default for Child",
        "impl<T> fmt::Debug for ChildVec",
        "impl<'a, T> fmt::Debug for Ref",
        "impl<'a, T> PartialEq for Ref",
    ] {
        assert!(
            !code.contains(forbidden),
            "`walk.rs` grew `{forbidden}` — that re-opens the edge class `Child`/`ChildVec` \
             exist to close"
        );
    }

    let walk_ctor: Vec<&str> = code
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("fn new() -> Self"))
        .collect();
    assert_eq!(
        walk_ctor.len(),
        1,
        "expected exactly one, private, `Walk` constructor; found {walk_ctor:?}"
    );
    assert!(
        !walk_ctor[0].starts_with("pub "),
        "`Walk`'s constructor became public: {walk_ctor:?}"
    );
    assert!(
        code.contains("pub struct Walk(());"),
        "`Walk`'s field stopped being private"
    );
}

// ---------------------------------------------------------------------
// (f) — `&Walk` cannot escape
// ---------------------------------------------------------------------

#[test]
fn check_f_the_capability_appears_only_in_the_openers_and_the_two_token_methods() {
    let files = sweep();
    let mut sites: Vec<String> = Vec::new();
    for (name, src) in &files {
        for (i, line) in code_only(src).lines().enumerate() {
            if line.contains("&Walk") || line.contains("&walk::Walk") {
                sites.push(format!("{name}:{}: {}", i + 1, line.trim()));
            }
        }
    }
    for s in &sites {
        assert!(
            s.starts_with("pulsus-logql/src/walk.rs:")
                || s.starts_with("pulsus-logql/src/ast.rs:")
                || s.starts_with("pulsus-read/src/logql/plan.rs:")
                || s.starts_with("pulsus-read/src/logql/pipeline.rs:"),
            "`&Walk` escaped to a file that must not name it: {s}"
        );
        assert!(
            s.contains("fn open") || s.contains("fn open_slot") || s.contains("w: &Walk"),
            "`&Walk` appears outside the openers and the two token methods: {s}"
        );
    }
    let code = source_of(&files, "walk.rs");
    let openers = code
        .lines()
        .filter(|l| l.trim().starts_with("pub fn open(self, _: &Walk)"))
        .count();
    assert_eq!(openers, 4, "expected exactly four handle openers");
}

// ---------------------------------------------------------------------
// (g) — `child` arms are prefix-closed by construction
// ---------------------------------------------------------------------

#[test]
fn check_g_every_child_arm_is_a_literal_ladder_or_a_single_slice_get() {
    let files = sweep();
    let mut seen = 0usize;
    for (name, src) in &files {
        if name.ends_with("walk.rs") {
            continue; // the trait declaration, not an implementation
        }
        let code = code_only(src);
        let mut from = 0usize;
        while let Some(rel) = code[from..].find("fn child<'a>(") {
            let at = from + rel;
            let body = &code[at..];
            let end = body[1..].find("\n    fn ").map_or(body.len(), |i| i + 1);
            for line in body[..end].lines().map(str::trim) {
                if line.contains("=> Some(") {
                    assert!(
                        line.starts_with("0 =>") || line.starts_with("1 =>"),
                        "{name}: a non-literal `child` arm can break prefix-closure: {line}"
                    );
                }
                if line.contains(".get(i)") {
                    assert!(
                        line.contains("peek().get(i)"),
                        "{name}: the N-ary arm must be a single `slice.get(i)`: {line}"
                    );
                }
            }
            seen += 1;
            from = at + end;
        }
    }
    assert_eq!(
        seen, 3,
        "expected one `child` per surviving SCC, saw {seen}"
    );
}

// ---------------------------------------------------------------------
// (h) — every `dismantle` call site is inside an `impl Drop`
// ---------------------------------------------------------------------

#[test]
fn check_h_every_dismantle_call_site_is_inside_an_impl_drop() {
    let files = sweep();
    let mut sites: Vec<(String, usize)> = Vec::new();
    for (name, src) in &files {
        if name.ends_with("walk.rs") {
            continue; // the definition, not a call site
        }
        for (i, line) in code_only(src).lines().enumerate() {
            if line.contains("dismantle::<") {
                sites.push((name.clone(), i + 1));
            }
        }
    }
    assert_eq!(
        sites.len(),
        3,
        "expected exactly one `dismantle` call per converted SCC, got {sites:?}"
    );
    for (name, line) in &sites {
        let src = source_of(&files, name.rsplit('/').next().unwrap_or(name));
        let lines: Vec<&str> = src.lines().collect();
        let head = lines[..*line]
            .iter()
            .rev()
            .find(|l| l.starts_with("impl ") || l.starts_with("fn ") || l.starts_with("pub fn "))
            .copied()
            .unwrap_or("<none>");
        assert!(
            head.starts_with("impl Drop for"),
            "{name}:{line} calls `dismantle` outside an `impl Drop` body (enclosing item: {head})"
        );
    }
}

// ---------------------------------------------------------------------
// (i) — the `Scc` surface is `cfg`-invariant
// ---------------------------------------------------------------------

#[test]
fn check_i_no_cfg_attribute_appears_inside_the_scc_trait_or_any_impl_scc() {
    let files = sweep();
    let mut regions: Vec<(String, String)> = Vec::new();
    let walk = source_of(&files, "walk.rs");
    let at = walk
        .find("pub trait Scc: Sized + 'static {")
        .expect("the `Scc` trait declaration");
    let end = at + walk[at..].find("\n}\n").expect("the trait's closing brace");
    regions.push(("walk.rs: trait Scc".to_string(), walk[at..end].to_string()));
    for (name, src) in &files {
        let code = code_only(src);
        let mut from = 0usize;
        while let Some(rel) = code[from..].find("Scc for ") {
            let at = from + rel;
            let head = code[..at].rfind("\nimpl ").map(|i| i + 1);
            let Some(h) = head else {
                from = at + 8;
                continue;
            };
            let end = h + code[h..].find("\n}\n").unwrap_or(0);
            regions.push((name.clone(), code[h..end].to_string()));
            from = end.max(at + 8);
        }
    }
    for (name, region) in regions {
        assert!(
            !region.contains("#[cfg("),
            "{name} carries a `cfg` attribute — a `cfg`-conditional trait surface is not a \
             trait surface (E0407 from any dependent crate)"
        );
    }
}

// ---------------------------------------------------------------------
// (j) — the single-opened-child hatch stays deleted
// ---------------------------------------------------------------------

/// `walk::child_of` handed a consumer ONE opened child by index, which is
/// what let `build_metric_node` keep a hand-written per-node recursion
/// after #272 retyped SCC-2's slots. #293 converted that walk to a
/// [`walk::find_preorder`] consumer, so the hatch has no callers and is
/// deleted along with `plan_legacy_descent.rs`, the module that existed
/// only to bound them.
///
/// **What this asserts is absence, and a lexical scan can do that** where
/// it could not establish "exactly one caller": the declaration is gone,
/// so any call site anywhere — nested module, alias, macro — is a
/// COMPILE error, and this test's job is only to notice the declaration
/// regrowing. `walk::slice_of` deliberately survives: its consumer
/// (`build_variants_node`'s per-variant loop) is flat, not a walk.
#[test]
fn check_j_the_single_child_escape_hatch_stays_deleted() {
    let files = sweep();
    let mut sites: Vec<String> = Vec::new();
    for (name, src) in &files {
        for (i, line) in code_only(src).lines().enumerate() {
            if line.contains("child_of") {
                sites.push(format!("{name}:{}: {}", i + 1, line.trim()));
            }
        }
    }
    assert!(
        sites.is_empty(),
        "`walk::child_of` regrew — a single opened child by index is what lets a consumer \
         keep a per-node recursion, and #293 deleted it: {sites:#?}"
    );
    assert!(
        !files
            .iter()
            .any(|(n, _)| n.ends_with("plan_legacy_descent.rs")),
        "`plan_legacy_descent.rs` is back; it existed only to bound the deleted hatch's callers"
    );
    // The surviving N-ary opener has exactly one consuming file.
    let consumers: BTreeSet<&str> = files
        .iter()
        .filter(|(n, _)| !n.ends_with("walk.rs"))
        .filter(|(_, s)| code_only(s).contains("slice_of("))
        .map(|(n, _)| n.as_str())
        .collect();
    assert_eq!(
        consumers,
        BTreeSet::from(["pulsus-read/src/logql/plan.rs"]),
        "`walk::slice_of` gained a consumer; it is kept only for `build_variants_node`'s FLAT \
         per-variant loop"
    );
}
