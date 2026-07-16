//! Issue #36: the hermetic route-inventory drift guard + docs-gap check.
//! No server, no ClickHouse — runs under the plain `ci` job's
//! `cargo test --workspace` (not `PULSUS_TEST_CLICKHOUSE`-gated).
//!
//! Three independent checks:
//!
//! 1. [`every_source_route_matches_the_manifest_exactly`] — source-scans
//!    every `.rs` file under `crates/pulsus-server/src/**` and
//!    `crates/pulsus-write/src/**` (the whole tree, not a hardcoded file
//!    list — plan v2 finding 1) for route-registration tokens, strips
//!    comments and `#[cfg(test)] mod ... { ... }` blocks first, and
//!    compares the extracted `(method, path)` set against
//!    `support::manifest::route_manifest()`'s `Mounted` set, both
//!    directions. A `.route(` construct this scanner cannot classify — a
//!    non-literal path outside the one known `mount_log_query_routes`
//!    prefix-interpolation shape, or a non-literal `mount_log_query_routes(`
//!    prefix argument at any non-definition call site (round-5 finding) —
//!    hard-fails with the offending file:line rather than silently passing.
//! 2. [`every_pinned_function_body_matches_the_snapshot_exactly`] —
//!    task-manager adjudication on issue #36, rounds 4 and 5: after three
//!    review rounds of increasingly elaborate *semantic* provenance-tracing
//!    for `.merge(`/`.nest(`/`.nest_service(` targets (each round's fix
//!    caught by the next round's sharper evasion — a name-only check, then
//!    a qualified-path check, then a wrapper-body check, then a
//!    receiver-blind chain check), round 4 abandoned semantics entirely in
//!    favor of **exact textual pinning** of each individual composition
//!    call site. Round 5 found that grain still too fine: pinning one
//!    call-chain expression at a time let a *second*, textually-identical
//!    call added under a different match arm collapse onto the existing
//!    pinned-set entry (set equality does not see the duplicate), hiding a
//!    real mode-gating change. This check pins the coarser, load-bearing
//!    unit instead — the **whole body** of every function that contains a
//!    composition token or a route-mounting-helper call
//!    (`support::manifest::pinned_function_bodies()`, `(file, fn,
//!    whitespace-normalized full body text)`) — and requires the *live
//!    source* to produce exactly that same set, both directions. Occurrence
//!    count, match-arm placement, receiver swaps, argument swaps: all of it
//!    is now part of one function's pinned text, so any change to any of
//!    it is a detected drift by construction. The guard is not proving
//!    what a function *does*; it is proving that it has not *changed*
//!    since a human last reviewed and re-pinned it — and any change to one
//!    of these few load-bearing functions, however small, is meant to
//!    force that re-pinning.
//! 3. [`every_mounted_route_is_documented_in_docs_api_md`] — every
//!    `Mounted` `RouteSpec`'s `doc_ref` must resolve against
//!    `docs/api.md`'s actual text (docs-first: a mounted-but-undocumented
//!    route fails here, not silently).
//!
//! ## Threat model (task-manager adjudication, issue #36 round 6)
//!
//! This guard targets **accidental** drift — the plan's own acceptance
//! criterion is "adding an unmatrixed route fails CI", not "no attacker
//! with source-tree write access can ever add an unmatrixed route". It
//! enforces a small set of **statically-accountable composition
//! conventions** (route registration is always a literal `.route(` string,
//! or the one sanctioned `format!` shape inside a named, listed helper;
//! router composition is always dot-call syntax, never fully-qualified
//! syntax; a `.merge(`/`.nest(`/`.nest_service(`/helper-call-bearing
//! function's whole body is exactly pinned) by **hard-failing on anything
//! it cannot account for** — a non-literal `.route(` path outside the one
//! sanctioned shape, a computed path outside a listed helper, a UFCS
//! composition call, an unlisted mount helper. It is not, and does not
//! attempt to be, a general dataflow/points-to analysis: a sufficiently
//! determined author *could* still evade it with, say, a
//! `macro_rules!`-generated router-composition call, a `build.rs` codegen
//! step that writes route registrations, or a `#[proc_macro]` that
//! expands to one — none of which a source-text scan can see by
//! construction. That class of deliberate, sophisticated evasion is out of
//! scope for a hermetic source-scan guard; it is covered the same way any
//! other change to these few load-bearing files always has been — human
//! code review. Six review rounds against this guard have all been
//! textual-construction evasions a scan *can* see (and now does); nothing
//! has required inventing a codegen step to demonstrate.

#[path = "support/manifest.rs"]
mod manifest;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use manifest::{DocRef, Gate, Method, RouteStatus, Surface, route_manifest};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root resolves")
}

fn rs_files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk(dir, &mut out);
    out.sort();
    out
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Byte offset -> 1-based line number, against `src`.
fn line_of(src: &str, byte_offset: usize) -> usize {
    src.as_bytes()[..byte_offset.min(src.len())]
        .iter()
        .filter(|&&b| b == b'\n')
        .count()
        + 1
}

/// Advances past a Rust comment or string/char/raw/byte-string literal
/// starting at byte `i`, if `i` is the start of one — returns the index
/// just past it, so a brace/paren *inside* a literal (a `.route(` call's
/// JSON fixture strings are full of unbalanced `{`/`}` split across
/// several `push_str` literals — very real in this codebase's own
/// `#[cfg(test)]` blocks) is never mistaken for a structural token. `None`
/// when `i` starts none of these (including a lifetime like `'a`, which
/// looks like the start of a char literal but never closes).
fn skip_literal_or_comment(bytes: &[u8], i: usize) -> Option<usize> {
    match bytes.get(i) {
        Some(b'/') if bytes.get(i + 1) == Some(&b'/') => {
            let mut j = i + 2;
            while j < bytes.len() && bytes[j] != b'\n' {
                j += 1;
            }
            Some(j)
        }
        Some(b'/') if bytes.get(i + 1) == Some(&b'*') => {
            let mut depth = 1i32;
            let mut j = i + 2;
            while j < bytes.len() && depth > 0 {
                if bytes[j] == b'/' && bytes.get(j + 1) == Some(&b'*') {
                    depth += 1;
                    j += 2;
                } else if bytes[j] == b'*' && bytes.get(j + 1) == Some(&b'/') {
                    depth -= 1;
                    j += 2;
                } else {
                    j += 1;
                }
            }
            Some(j)
        }
        Some(b'"') => Some(skip_quoted(bytes, i + 1)),
        Some(b'\'') => skip_char_literal(bytes, i),
        Some(b'r') => skip_raw_string(bytes, i),
        Some(b'b') => match bytes.get(i + 1) {
            Some(b'"') => Some(skip_quoted(bytes, i + 2)),
            Some(b'\'') => skip_char_literal(bytes, i + 1),
            Some(b'r') => skip_raw_string(bytes, i + 1),
            _ => None,
        },
        _ => None,
    }
}

/// `j` starts just past the opening `"`; returns the index just past the
/// matching (backslash-escape-aware) closing `"`.
fn skip_quoted(bytes: &[u8], mut j: usize) -> usize {
    while j < bytes.len() && bytes[j] != b'"' {
        if bytes[j] == b'\\' {
            j += 1;
        }
        j += 1;
    }
    (j + 1).min(bytes.len())
}

/// `i` points at the opening `'`. Only a genuine char literal (`'c'`,
/// `'\n'`, `'\''`, `'\u{2603}'`, ...) closes within a short lookahead;
/// a lifetime (`'a`) never does, and is left unconsumed (`None`) so the
/// caller advances past just the `'` normally.
fn skip_char_literal(bytes: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    if bytes.get(j) == Some(&b'\\') {
        j += 1;
        if bytes.get(j) == Some(&b'u') && bytes.get(j + 1) == Some(&b'{') {
            j += 2;
            while j < bytes.len() && bytes[j] != b'}' {
                j += 1;
            }
            j += 1;
        } else {
            j += 1;
        }
    } else {
        j += 1;
    }
    if bytes.get(j) == Some(&b'\'') {
        Some(j + 1)
    } else {
        None
    }
}

/// `i` points at `r` (or, via the `b` case above, the `r` of `br"..."`).
/// Handles any hash count (`r"..."`, `r#"..."#`, `r##"..."##`, ...).
fn skip_raw_string(bytes: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    let mut hashes = 0usize;
    while bytes.get(j) == Some(&b'#') {
        hashes += 1;
        j += 1;
    }
    if bytes.get(j) != Some(&b'"') {
        return None;
    }
    j += 1;
    loop {
        if j >= bytes.len() {
            return Some(j);
        }
        if bytes[j] == b'"' {
            let mut k = j + 1;
            let mut matched = 0usize;
            while matched < hashes && bytes.get(k) == Some(&b'#') {
                matched += 1;
                k += 1;
            }
            if matched == hashes {
                return Some(k);
            }
        }
        j += 1;
    }
}

/// One single lexer pass over `src` emitting **both** preprocessed views
/// simultaneously (round-10 adjudication: the previous two-stage pipeline
/// — a hand-rolled comment stripper with its own ordinary-quote loop,
/// followed by a separate lexer-based literal blanker — could
/// desynchronize on a raw string containing `//`, e.g.
/// `r#"{"url":"https://x"}"#`: stage one's quote loop mis-lexed the raw
/// string's delimiters, exposed the interior `//` as a "comment", and
/// blanked the closing delimiter plus real code after it. One pass over
/// the one shared [`skip_literal_or_comment`] lexer makes a stage desync
/// structurally impossible — there is no second stage to disagree with
/// the first):
///
/// - `.0` (`stripped`): comments blanked, string/char literals intact —
///   route-path extraction's view (literals are exactly what it extracts).
///   Doc comments (`///`, `//!`) are ordinary comments to the lexer and
///   are blanked too — load-bearing: this codebase's doc comments
///   illustrate real code with snippets like `.merge(authed)` that a
///   comment-blind scan would mistake for real call sites.
/// - `.1` (`blanked`): comments AND string/char literals blanked — every
///   policy pass's view (a token inside a log message/format string must
///   never trip a policy check).
///
/// Both views are byte-length-identical to `src` (non-newline bytes
/// replaced with spaces), so offsets/line numbers stay valid across all
/// three.
fn preprocess_views(src: &str) -> (String, String) {
    let bytes = src.as_bytes();
    let mut stripped = bytes.to_vec();
    let mut blanked = bytes.to_vec();
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(next) = skip_literal_or_comment(bytes, i) {
            let end = next.min(bytes.len());
            let is_comment = bytes[i] == b'/';
            for j in i..end {
                if bytes[j] != b'\n' {
                    if is_comment {
                        stripped[j] = b' ';
                    }
                    blanked[j] = b' ';
                }
            }
            i = end;
        } else {
            i += 1;
        }
    }
    (
        String::from_utf8(stripped).expect("blanking preserves UTF-8 validity"),
        String::from_utf8(blanked).expect("blanking preserves UTF-8 validity"),
    )
}

/// Every `#[cfg(test)]\nmod <ident> { ... }` block's inclusive byte span
/// in `stripped` (the comment-blanked, literals-intact view — comments
/// are already gone, so a `#[cfg(test)]` inside a doc comment cannot
/// false-match; the brace-depth scan skips literals via
/// [`skip_literal_or_comment`], since this codebase's own test modules
/// build JSON fixture strings with plenty of unbalanced `{`/`}` inside
/// string literals). The caller blanks these spans in *both* views, so
/// test-only code is invisible to route extraction and policy passes
/// alike (plan v2 finding 1: "the only `.route(` calls in
/// `pulsus-write/src/ingest/http.rs` today are test-only routers" — this
/// is what excludes them).
fn cfg_test_mod_spans(stripped: &str) -> Vec<(usize, usize)> {
    let marker = "#[cfg(test)]";
    let bytes = stripped.as_bytes();
    let mut spans = Vec::new();
    let mut search_from = 0usize;
    while let Some(rel) = stripped[search_from..].find(marker) {
        let attr_start = search_from + rel;
        let Some(brace_rel) = stripped[attr_start..].find('{') else {
            break;
        };
        let open = attr_start + brace_rel;
        let between = &stripped[attr_start + marker.len()..open];
        if !between.trim_start().starts_with("mod ") {
            // Not a `#[cfg(test)] mod ... {` shape (e.g. `#[cfg(test)]` on
            // a single item) — this codebase does not use that pattern
            // today; skip past this attribute rather than mis-stripping.
            search_from = attr_start + marker.len();
            continue;
        }
        let mut depth = 0i32;
        let mut close = None;
        let mut i = open;
        while i < bytes.len() {
            if let Some(next) = skip_literal_or_comment(bytes, i) {
                i = next;
                continue;
            }
            match bytes[i] {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        close = Some(i);
                        break;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        let close = close.unwrap_or_else(|| {
            panic!(
                "unterminated #[cfg(test)] mod block starting at line {}",
                line_of(stripped, attr_start)
            )
        });
        spans.push((attr_start, close));
        search_from = close + 1;
    }
    spans
}

/// Blanks each inclusive `(start, end)` span in `text` (non-newline bytes
/// to spaces), preserving byte offsets.
fn blank_spans(text: String, spans: &[(usize, usize)]) -> String {
    let mut bytes = text.into_bytes();
    for &(start, end) in spans {
        for b in &mut bytes[start..=end] {
            if *b != b'\n' {
                *b = b' ';
            }
        }
    }
    String::from_utf8(bytes).expect("blanking preserves UTF-8 validity")
}

/// Every route-mounting method axum 0.7/0.8's `Router` exposes (verified
/// against the axum docs: `route`, `route_service`, `nest`, `nest_service`,
/// `merge`, `fallback`, `fallback_service`, `method_not_allowed_fallback`
/// — everything else on `Router` is layering/state/serving plumbing that
/// mounts no handler). The `MethodRouter` service forms (`get_service`,
/// `on_service`, ...) only ever appear *inside* a `.route(...)` call's
/// method-chain argument, where [`parse_method_chain`] already hard-fails
/// on any verb it does not recognize — so they cannot slip through there
/// either.
const ROUTER_MOUNTING_METHODS: [&str; 8] = [
    "route",
    "route_service",
    "nest",
    "nest_service",
    "merge",
    "fallback",
    "fallback_service",
    "method_not_allowed_fallback",
];

/// Round-7 review finding (high): `.route_service(` and
/// `.fallback_service(` mount an opaque `Service` this guard has no way
/// to account for — a `Service`'s handled methods/paths are invisible to
/// a source scan (unlike `.route(`'s literal path + method chain, or
/// `.fallback(`'s handler, which the whole-body pinning covers). Policy:
/// any occurrence in non-test source hard-fails, pinning the convention;
/// none exist in the codebase today, so this costs nothing. If one is
/// ever genuinely needed, the scanner (and manifest) must be extended
/// deliberately as part of that change.
const FORBIDDEN_SERVICE_MOUNT_TOKENS: [&str; 2] = [".route_service(", ".fallback_service("];

/// `blanked`: the string-literal-blanked view (round-8 finding: an
/// ordinary string constant containing `.route_service(` must never fail
/// CI — every policy pass runs on the shared blanked view, only route-path
/// *extraction* sees literals).
fn assert_no_service_mounts(blanked: &str, file: &Path) {
    for token in FORBIDDEN_SERVICE_MOUNT_TOKENS {
        if let Some(rel) = blanked.find(token) {
            panic!(
                "{}:{}: found `{token}` — mounting an opaque `Service` hides its handled \
                 methods/paths from the drift guard (no literal path + method chain to extract, \
                 no handler body to pin). Use `.route(path, method_router)` (or `.fallback(` for \
                 a fallback handler, which whole-body pinning covers), or extend \
                 `route_inventory.rs`'s scanner and the manifest deliberately, before merging.",
                file.display(),
                line_of(blanked, rel)
            );
        }
    }
}

/// Removes every turbofish (`::<...>`, angle-bracket balanced) so
/// `Router::<AppState>::nest(` normalizes to `Router::nest(` before UFCS
/// token matching (round-7 finding: the generic-argument spelling
/// otherwise evades the substring check).
fn strip_turbofish(src: &str) -> String {
    let bytes = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b':' && bytes.get(i + 1) == Some(&b':') && bytes.get(i + 2) == Some(&b'<') {
            let mut depth = 1i32;
            let mut j = i + 3;
            while j < bytes.len() && depth > 0 {
                match bytes[j] {
                    b'<' => depth += 1,
                    b'>' => depth -= 1,
                    _ => {}
                }
                j += 1;
            }
            // Skip the whole `::<...>`; keep nothing (so `Router::<A>::nest`
            // becomes `Router::nest`).
            i = j;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// Round-8 review finding (high) / structural cut: aliasing axum's
/// `Router` type is **banned at import time** — any `use` declaration in
/// non-test code binding `Router as <anything>` (single-item, brace-
/// grouped, nested groups, `pub use` re-exports alike) hard-fails, on the
/// string-blanked view. This entirely replaces round-7's per-file
/// alias-*call* matching (which was imprecise: it missed brace-grouped
/// imports, and its bare `rsplit("::")` fallback could false-match
/// unrelated names merely *ending* in `Router`): with no alias ever
/// allowed to exist, an aliased UFCS call cannot exist either, and the
/// UFCS check only ever needs the one canonical `Router::` spelling. The
/// identifier-boundary check (the byte before `Router` must not be an
/// identifier byte) keeps `use crate::MyRouter as M;` — a different type
/// that merely ends in "Router" — legal.
fn assert_no_router_alias_imports(blanked: &str, file: &Path) {
    let bytes = blanked.as_bytes();
    let mut from = 0usize;
    while let Some(rel) = blanked[from..].find("use ") {
        let start = from + rel;
        let prev_ok = start == 0 || !is_ident_byte(bytes[start - 1]);
        let Some(semi_rel) = blanked[start..].find(';') else {
            break;
        };
        if prev_ok {
            let stmt = &blanked[start..start + semi_rel];
            let stmt_bytes = stmt.as_bytes();
            let mut inner_from = 0usize;
            while let Some(inner_rel) = stmt[inner_from..].find("Router as ") {
                let pos = inner_from + inner_rel;
                let boundary_ok = pos == 0 || !is_ident_byte(stmt_bytes[pos - 1]);
                if boundary_ok {
                    panic!(
                        "{}:{}: this `use` declaration aliases `Router` — do not alias \
                         axum::Router: the drift guard accounts for router composition by its \
                         canonical name, and an alias would let a fully-qualified-syntax mount \
                         evade the UFCS check. Import it as plain `Router` before merging.",
                        file.display(),
                        line_of(blanked, start + pos)
                    );
                }
                inner_from = pos + "Router as ".len();
            }
        }
        from = start + semi_rel + 1;
    }
}

/// Round-9 review finding (high): a `type` alias (`type AppRouter =
/// Router<AppState>;`) creates another spelling for `Router` the UFCS
/// check would not match (`AppRouter::nest(...)`), the same class the
/// round-8 import-alias ban closed for `use` renames. Same policy family:
/// any `type` (or `pub type`) alias declaration in non-test source whose
/// RHS references `Router` (identifier-boundary-checked on both sides, so
/// `MyRouter`/`RouterExtra` never match) hard-fails at the declaration —
/// do not alias axum::Router; compose via the canonical name. Nothing in
/// either scanned tree aliases `Router` today, so this costs nothing.
fn assert_no_router_type_aliases(blanked: &str, file: &Path) {
    let bytes = blanked.as_bytes();
    let mut from = 0usize;
    while let Some(rel) = blanked[from..].find("type ") {
        let start = from + rel;
        let prev_ok = start == 0 || !is_ident_byte(bytes[start - 1]);
        let Some(semi_rel) = blanked[start..].find(';') else {
            break;
        };
        if prev_ok {
            let stmt = &blanked[start..start + semi_rel];
            if let Some(eq_rel) = stmt.find('=') {
                let rhs = &stmt[eq_rel + 1..];
                let rhs_bytes = rhs.as_bytes();
                let mut inner_from = 0usize;
                while let Some(inner_rel) = rhs[inner_from..].find("Router") {
                    let pos = inner_from + inner_rel;
                    let end = pos + "Router".len();
                    let left_ok = pos == 0 || !is_ident_byte(rhs_bytes[pos - 1]);
                    let right_ok = end >= rhs_bytes.len() || !is_ident_byte(rhs_bytes[end]);
                    if left_ok && right_ok {
                        panic!(
                            "{}:{}: this `type` declaration aliases `Router` — do not alias \
                             axum::Router: compose via the canonical name, so the drift guard's \
                             UFCS check can account for every fully-qualified composition \
                             spelling. Remove the alias before merging.",
                            file.display(),
                            line_of(blanked, start)
                        );
                    }
                    inner_from = end;
                }
            }
        }
        from = start + semi_rel + 1;
    }
}

/// Fully-qualified-syntax router-composition/mount forms
/// (`Router::merge(router, ...)` instead of `router.merge(...)`) hard-fail
/// (round-6 finding, hardened per rounds 7-8): the check runs against the
/// shared string-literal-blanked view (no false positive from a literal
/// containing UFCS-looking text), with turbofish stripped
/// (`Router::<AppState>::nest(` is caught). Only the one canonical
/// `Router::` spelling needs matching — [`assert_no_router_alias_imports`]
/// bans every `Router` alias at import time, so no aliased spelling can
/// exist (round-8 structural cut, replacing per-alias call matching). An
/// `axum::`-style path prefix is covered for free, since
/// `"axum::Router::merge("` contains `"Router::merge("` as a substring.
/// Rather than teach every extraction pass to also recognize UFCS forms,
/// this codebase adopts (and hard-enforces) a convention: router
/// composition must use method-call syntax so the drift guard can account
/// for it. The codebase uses no UFCS forms today, so this check costs
/// nothing and closes the whole class at once.
fn assert_no_ufcs_router_composition(blanked: &str, file: &Path) {
    let normalized = strip_turbofish(blanked);
    for method in ROUTER_MOUNTING_METHODS {
        let token = format!("Router::{method}(");
        if let Some(rel) = normalized.find(&token) {
            panic!(
                "{}:{}: found `{token}` — router composition must use method-call syntax \
                 (e.g. `router.merge(...)`, not fully-qualified-syntax `Router::merge(router, \
                 ...)`) so the drift guard can account for it; rewrite as method-call syntax \
                 before merging.",
                file.display(),
                line_of(&normalized, rel)
            );
        }
    }
}

/// Reads `file`, strips comments and `#[cfg(test)]` blocks, builds the
/// shared string-literal-blanked view, and runs every policy pass against
/// that view (Router-alias import ban, UFCS composition ban, opaque-
/// `Service` mount ban) — the one preprocessing pipeline every extraction
/// pass in this file shares, so no check can be bypassed by a pass that
/// forgot to call it separately. Returns `(stripped, blanked)`: route-path
/// extraction uses `stripped` (it needs the literals); composition-token
/// *selection* uses `blanked` (round-8: a token inside a string constant
/// must not select/trip anything).
fn read_stripped_source(file: &Path) -> (String, String) {
    let original =
        fs::read_to_string(file).unwrap_or_else(|e| panic!("reading {}: {e}", file.display()));
    // One lexer pass produces both views (round-10: no second stage exists
    // to desynchronize from the first); the `#[cfg(test)]` spans are then
    // computed once (on the literals-intact view) and blanked in both, so
    // test-only code stays invisible everywhere.
    let (stripped, blanked) = preprocess_views(&original);
    let test_spans = cfg_test_mod_spans(&stripped);
    let stripped = blank_spans(stripped, &test_spans);
    let blanked = blank_spans(blanked, &test_spans);
    // Round-11: every structural scanner walks `blanked` and slices text
    // from `stripped` at the same offsets — the two views being
    // byte-length-identical (blanking never inserts/removes bytes) is the
    // invariant that makes that sound.
    debug_assert_eq!(
        stripped.len(),
        blanked.len(),
        "{}: preprocessed views must be byte-length-identical",
        file.display()
    );
    assert_no_router_alias_imports(&blanked, file);
    assert_no_router_type_aliases(&blanked, file);
    assert_no_ufcs_router_composition(&blanked, file);
    assert_no_service_mounts(&blanked, file);
    (stripped, blanked)
}

/// Finds the index of the `)` matching the `(` at `open_idx` — operates on
/// the **blanked** view ONLY (round-11 adjudication: all structural
/// scanning runs where string literals and comments no longer exist, so
/// this helper needs — and has — no literal handling of its own; the one
/// place in this file that knows what a Rust string literal is remains
/// [`skip_literal_or_comment`]). Callers slice actual text (paths,
/// arguments) from the offset-identical *stripped* view using the offsets
/// found here.
fn matching_close(blanked: &str, open_idx: usize) -> Option<usize> {
    let bytes = blanked.as_bytes();
    let mut depth = 0i32;
    let mut i = open_idx;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Splits `blanked[start..end)` on top-level occurrences of `sep`
/// (paren/bracket/brace aware) — blanked-view-only, same round-11 policy
/// as [`matching_close`]: literals are already spaces here, so a `,`/`(`
/// inside a path string cannot exist to be mis-split on.
fn split_top_level(blanked: &str, start: usize, end: usize, sep: u8) -> Vec<(usize, usize)> {
    let bytes = blanked.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut seg_start = start;
    let mut i = start;
    while i < end {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b if b == sep && depth == 0 => {
                parts.push((seg_start, i));
                seg_start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push((seg_start, end));
    parts
}

/// `true` when the `mount_log_query_routes(` occurrence starting at
/// `call_start` is the function's own *definition* (`fn
/// mount_log_query_routes(...`), not a call — rustfmt always renders
/// exactly one space between `fn` and the name, so checking the three
/// bytes immediately before `call_start` is precise for this codebase's
/// actual formatting.
fn is_fn_definition_site(src: &str, call_start: usize) -> bool {
    call_start >= 3 && &src[call_start - 3..call_start] == "fn "
}

/// All literal string-argument values passed as the 2nd argument of a
/// `mount_log_query_routes(Router::new(), "<literal>")` call anywhere in
/// `src` — the known-prefix-family call sites (plan v2 interfaces:
/// "expand the `mount_log_query_routes` family for both prefixes"). Round-5
/// review finding (high): every non-definition call site's prefix
/// argument must be a string literal or this hard-fails — a computed
/// prefix would silently mount an entire unmanifested five-route family,
/// invisible to both this extraction and the live matrix, same policy as
/// a non-literal `.route(` path.
/// Round-11 structure: call sites are *located* and *structurally parsed*
/// on the blanked view (a `mount_log_query_routes(` inside a string
/// literal is spaces there and can never false-match; parens/commas
/// inside literals do not exist to desynchronize on), while the prefix
/// argument's literal *text* is sliced from the offset-identical stripped
/// view.
fn mount_log_query_routes_call_prefixes(
    stripped: &str,
    blanked: &str,
    file: &Path,
) -> BTreeSet<String> {
    let mut prefixes = BTreeSet::new();
    let needle = "mount_log_query_routes(";
    let mut from = 0usize;
    while let Some(rel) = blanked[from..].find(needle) {
        let call_start = from + rel;
        if is_fn_definition_site(blanked, call_start) {
            from = call_start + needle.len();
            continue;
        }
        let open = call_start + needle.len() - 1;
        let Some(close) = matching_close(blanked, open) else {
            panic!(
                "{}:{}: unterminated `mount_log_query_routes(` call — the drift guard cannot \
                 parse this route-mounting-helper call site",
                file.display(),
                line_of(blanked, call_start)
            );
        };
        let args = split_top_level(blanked, open + 1, close, b',');
        let Some(&(s, e)) = args.get(1) else {
            panic!(
                "{}:{}: `mount_log_query_routes(...)` call has no second (prefix) argument — the \
                 drift guard cannot classify this route-mounting-helper call site",
                file.display(),
                line_of(blanked, call_start)
            );
        };
        let arg = stripped[s..e].trim();
        if !(arg.starts_with('"') && arg.ends_with('"') && arg.len() >= 2) {
            panic!(
                "{}:{}: `mount_log_query_routes(...)`'s prefix argument {arg:?} is not a string \
                 literal — a computed mount-helper prefix can silently mount an entire \
                 unmanifested route family, invisible to both route-inventory extraction and the \
                 live matrix. Use a literal prefix, or extend `route_inventory.rs`'s scanner, \
                 before merging.",
                file.display(),
                line_of(blanked, call_start)
            );
        }
        prefixes.insert(arg[1..arg.len() - 1].to_string());
        from = close + 1;
    }
    prefixes
}

/// One extracted `.route(...)` registration, before method-chain parsing.
struct RawRoute {
    path_arg: String,
    method_arg_span: (usize, usize),
    line: usize,
}

/// Scans (already `#[cfg(test)]`-stripped) `src` for `.route(` calls,
/// classifying each path argument as either a string literal or the one
/// sanctioned non-literal shape (`&format!("{prefix}<suffix>")`, and only
/// inside a listed route-mounting helper — round-6 finding) — anything
/// else hard-fails naming the file:line (plan v2 finding 1's "non-literal
/// path guard").
/// Round-11 structure: `.route(` call sites are *located* and
/// *structurally parsed* (paren matching, comma splitting) on the blanked
/// view — a `.route(` inside a string literal is spaces there and can
/// never false-match a call site, and parens/commas inside a path literal
/// or an inline handler's raw string do not exist to desynchronize on —
/// while the path argument's actual *text* is sliced from the
/// offset-identical stripped view (literals are exactly what it extracts).
fn extract_route_calls(stripped: &str, blanked: &str, file: &Path) -> Vec<RawRoute> {
    let spans = enumerate_fn_spans(stripped);
    let mut out = Vec::new();
    let needle = ".route(";
    let mut from = 0usize;
    while let Some(rel) = blanked[from..].find(needle) {
        let call_start = from + rel;
        let open = call_start + needle.len() - 1;
        let Some(close) = matching_close(blanked, open) else {
            panic!(
                "{}:{}: unterminated `.route(` call — drift guard cannot parse this route \
                 registration",
                file.display(),
                line_of(blanked, call_start)
            );
        };
        let args = split_top_level(blanked, open + 1, close, b',');
        assert!(
            args.len() >= 2,
            "{}:{}: `.route(...)` must have a path argument and a method-chain argument — found \
             {} top-level argument(s), the drift guard cannot classify this call",
            file.display(),
            line_of(blanked, call_start),
            args.len()
        );
        let (ps, pe) = args[0];
        let path_arg_raw = stripped[ps..pe].trim();
        let enclosing_fn = enclosing_fn_name(&spans, call_start);
        let path_arg = classify_path_arg(
            path_arg_raw,
            file,
            line_of(blanked, call_start),
            enclosing_fn,
        );
        // Everything after the path argument's trailing comma, up to the
        // call's closing paren, is the method-chain argument (there is
        // exactly one top-level comma inside `.route(path, methods)`).
        let method_start = args[0].1 + 1;
        out.push(RawRoute {
            path_arg,
            method_arg_span: (method_start, close),
            line: line_of(blanked, call_start),
        });
        from = close + 1;
    }
    out
}

/// The innermost `fn` name whose body span contains `pos` — `None` when
/// `pos` sits outside every scanned function.
fn enclosing_fn_name(spans: &[FnSpan], pos: usize) -> Option<&str> {
    spans
        .iter()
        .filter(|s| s.start <= pos && pos < s.end)
        .min_by_key(|s| s.end - s.start)
        .map(|s| s.name.as_str())
}

/// Returns the literal path (for a string-literal argument) or, for the
/// one sanctioned `&format!("{prefix}<suffix>")` shape *when the call site
/// sits inside a listed [`manifest::ROUTE_MOUNTING_HELPERS`] function*, a
/// sentinel of the form `"\0FAMILY\0<suffix>"` — any other shape hard-fails,
/// naming the file:line. Round-6 finding (high): a computed/formatted path
/// in a function *not* on the helper list must never be silently expanded
/// (and thereby collide/dedupe against known prefixes) — it is exactly as
/// unaccountable as any other non-literal path, so it gets exactly the
/// same treatment: hard fail.
fn classify_path_arg(raw: &str, file: &Path, line: usize, enclosing_fn: Option<&str>) -> String {
    if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
        return raw[1..raw.len() - 1].to_string();
    }
    let prefix_format = "&format!(\"{prefix}";
    if raw.starts_with(prefix_format) && raw.ends_with("\")") {
        let is_listed_helper =
            enclosing_fn.is_some_and(|f| manifest::ROUTE_MOUNTING_HELPERS.contains(&f));
        if !is_listed_helper {
            panic!(
                "{}:{}: `.route(...)`'s path argument {raw:?} is a computed/formatted path, but \
                 this call site is not inside one of `support::manifest::ROUTE_MOUNTING_HELPERS` \
                 ({:?}) — expansion of a computed route path is permitted only inside a listed \
                 route-mounting helper (whose own whole-body pinning already accounts for it). \
                 Anywhere else, a computed path is never silently expanded (which could collide \
                 with — and so hide behind — an unrelated known prefix); use a literal path, or \
                 add this function to `ROUTE_MOUNTING_HELPERS` and reconcile the manifest, before \
                 merging.",
                file.display(),
                line,
                manifest::ROUTE_MOUNTING_HELPERS
            );
        }
        // `format!`'s own escaping: `{{`/`}}` in the string literal mean a
        // literal `{`/`}` in the formatted output (e.g. the axum path
        // parameter `{{name}}` -> `{name}`) — unescape the same way
        // `format!` itself would before treating this as a route suffix.
        let raw_suffix = &raw[prefix_format.len()..raw.len() - 2];
        let suffix = raw_suffix.replace("{{", "{").replace("}}", "}");
        return format!("\0FAMILY\0{suffix}");
    }
    panic!(
        "{}:{}: `.route(...)`'s path argument {raw:?} is neither a string literal nor the known \
         `&format!(\"{{prefix}}...\")` shape — the drift guard cannot classify this route path. \
         Update `route_inventory.rs`'s scanner before merging a route registered this way.",
        file.display(),
        line
    );
}

/// Parses a `get(...).post(...)`-style method chain (top-level `.`-split,
/// paren/string aware) into the [`Method`]s it names — an unrecognized
/// leading identifier hard-fails.
fn parse_method_chain(
    blanked: &str,
    start: usize,
    end: usize,
    file: &Path,
    line: usize,
) -> Vec<Method> {
    // Blanked-view-only (round-11): a method chain's structural tokens
    // (verbs, dots, parens) are code, identical in both views; any string
    // literal inside an inline handler is spaces here and cannot
    // desynchronize the top-level `.`-split.
    let mut methods = Vec::new();
    for (s, e) in split_top_level(blanked, start, end, b'.') {
        let seg = blanked[s..e].trim();
        if seg.is_empty() {
            continue;
        }
        let Some(paren) = seg.find('(') else {
            panic!(
                "{}:{}: method-chain segment {seg:?} has no `(` — the drift guard cannot parse \
                 this `.route(...)` method chain",
                file.display(),
                line
            );
        };
        let ident = seg[..paren].trim();
        let Some(method) = Method::from_chain_ident(ident) else {
            panic!(
                "{}:{}: unrecognized HTTP method verb {ident:?} in a `.route(...)` method chain \
                 — update `route_inventory.rs`'s scanner before merging a route registered with \
                 this verb.",
                file.display(),
                line
            );
        };
        methods.push(method);
    }
    methods
}

// -- Router-composition exact-pinning snapshot ---------------------------
//
// Task-manager adjudication on issue #36 round 4 (see this module's own
// doc comment for the full history): semantic provenance tracing for
// `.merge(`/`.nest(`/`.nest_service(` targets is abandoned in favor of
// exact textual pinning against `support::manifest::composition_snapshot()`.

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// The router-composition tokens whole-body pinning covers: a function
/// containing any of these joins the pinned set. `.fallback(` and
/// `.method_not_allowed_fallback(` were added per the round-7 review
/// (axum's full mounting surface): neither carries a literal path to
/// extract, but both mount a handler, so — like `.merge(`/`.nest(` — the
/// containing function's whole body gets pinned and any change to it is a
/// detected drift. (`.route_service(`/`.fallback_service(` are hard-failed
/// outright instead — see `FORBIDDEN_SERVICE_MOUNT_TOKENS`.)
const COMPOSITION_TOKENS: [&str; 5] = [
    ".merge(",
    ".nest(",
    ".nest_service(",
    ".fallback(",
    ".method_not_allowed_fallback(",
];

/// Collapses every run of whitespace (space/tab/newline) to a single space
/// and trims — so a pure reformat (line-wrap, reindent) of a composition
/// call site never trips the guard, while any *textual* change (a
/// different receiver, a different argument, a removed/added call) does.
fn normalize_whitespace(s: &str) -> String {
    // First pass: collapse every whitespace run to a single space.
    let mut collapsed = String::with_capacity(s.len());
    let mut last_was_space = false;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !last_was_space {
                collapsed.push(' ');
                last_was_space = true;
            }
        } else {
            collapsed.push(ch);
            last_was_space = false;
        }
    }
    let collapsed: Vec<char> = collapsed.trim().chars().collect();

    // Second pass: rustfmt-specific reformats a *pure* line-wrap can
    // introduce, none changing the expression's meaning, all normalized
    // away so a formatter-only diff never trips the snapshot — a wrapped
    // method chain's continuation `.` starting a new line, and a wrapped
    // call's argument list (open-delimiter-adjacent and close-delimiter-
    // adjacent whitespace, plus the trailing `,` rustfmt adds before a
    // `)`/`]` only when it wraps arguments across lines — always
    // syntactically optional in Rust) turn into literal spaces/commas the
    // single-line canonical form never had once the first pass collapses
    // their surrounding newlines/indentation.
    let mut out = String::with_capacity(collapsed.len());
    let mut i = 0usize;
    while i < collapsed.len() {
        if collapsed[i] == ' ' {
            let next = collapsed.get(i + 1).copied();
            let prev = out.chars().next_back();
            let drop = matches!(next, Some('.') | Some(')') | Some(']') | Some('}'))
                || matches!(prev, Some('(') | Some('[') | Some('{'));
            if drop {
                i += 1;
                continue;
            }
        }
        if collapsed[i] == ',' {
            let mut j = i + 1;
            if collapsed.get(j) == Some(&' ') {
                j += 1;
            }
            if matches!(collapsed.get(j), Some(')') | Some(']')) {
                i += 1;
                continue;
            }
        }
        out.push(collapsed[i]);
        i += 1;
    }
    out
}

/// One `fn <name>(...) { BODY }` span found directly in a file — `start`/
/// `end` bound `BODY` (strictly between its braces), used only to label a
/// composition call site with its enclosing function's name.
struct FnSpan {
    name: String,
    start: usize,
    end: usize,
}

/// Enumerates every named-`fn` body span in `src`, comment/string-literal
/// aware via [`skip_literal_or_comment`] (the same reason
/// [`strip_cfg_test_mods`]'s own brace-depth scan needs it).
fn enumerate_fn_spans(src: &str) -> Vec<FnSpan> {
    let bytes = src.as_bytes();
    let mut spans = Vec::new();
    let mut from = 0usize;
    while let Some(rel) = src[from..].find("fn ") {
        let start = from + rel;
        let before_ok = start == 0 || !is_ident_byte(bytes[start - 1]);
        if !before_ok {
            from = start + 3;
            continue;
        }
        let name_start = start + 3;
        let mut name_end = name_start;
        while name_end < bytes.len() && is_ident_byte(bytes[name_end]) {
            name_end += 1;
        }
        if name_end == name_start {
            from = start + 3;
            continue;
        }
        let name = src[name_start..name_end].to_string();

        let mut i = name_end;
        let mut depth = 0i32;
        let mut open = None;
        while i < bytes.len() {
            if let Some(next) = skip_literal_or_comment(bytes, i) {
                i = next;
                continue;
            }
            match bytes[i] {
                b'(' | b'[' => depth += 1,
                b')' | b']' => depth -= 1,
                b';' if depth == 0 => break, // a signature with no body.
                b'{' if depth == 0 => {
                    open = Some(i);
                    break;
                }
                _ => {}
            }
            i += 1;
        }
        let Some(open) = open else {
            from = name_end;
            continue;
        };
        let mut brace_depth = 0i32;
        let mut j = open;
        let mut close = None;
        while j < bytes.len() {
            if let Some(next) = skip_literal_or_comment(bytes, j) {
                j = next;
                continue;
            }
            match bytes[j] {
                b'{' => brace_depth += 1,
                b'}' => {
                    brace_depth -= 1;
                    if brace_depth == 0 {
                        close = Some(j);
                        break;
                    }
                }
                _ => {}
            }
            j += 1;
        }
        if let Some(close) = close {
            spans.push(FnSpan {
                name,
                start: open + 1,
                end: close,
            });
        }
        from = name_end;
    }
    spans
}

/// `true` when `body` contains a *call* to `name` (an identifier-boundary
/// match immediately followed by `(`) — used to detect a call to one of
/// [`manifest::ROUTE_MOUNTING_HELPERS`] inside a function body.
fn body_contains_call(body: &str, name: &str) -> bool {
    let bytes = body.as_bytes();
    let mut from = 0usize;
    while let Some(rel) = body[from..].find(name) {
        let start = from + rel;
        let end = start + name.len();
        let before_ok = start == 0 || !is_ident_byte(bytes[start - 1]);
        if before_ok && bytes.get(end) == Some(&b'(') {
            return true;
        }
        from = start + name.len();
    }
    false
}

/// Every `(file, fn_name, normalized_whole_body_text)` triple for a
/// function in `src` whose body contains a [`COMPOSITION_TOKENS`] token
/// or a call to one of [`manifest::ROUTE_MOUNTING_HELPERS`] — round-5
/// finding: pinning the *whole function body* (not a per-call-site
/// sub-expression, round 4's grain) makes occurrence count, match-arm
/// placement, and every other control-flow detail part of the pinned
/// text by construction. Token *selection* runs on `blanked` (the
/// string-literal-blanked view, byte-offset-identical to `src` —
/// round-8: a composition token inside a string constant must never
/// spuriously pin the containing function); the pinned *text* itself
/// comes from `src`, literals included, since they are part of the real
/// body (e.g. the mount-helper prefix strings).
fn extract_pinned_function_bodies(
    src: &str,
    blanked: &str,
    file_rel: &str,
) -> BTreeSet<(String, String, String)> {
    let spans = enumerate_fn_spans(src);
    let mut out = BTreeSet::new();
    for span in &spans {
        let body = &src[span.start..span.end];
        let blanked_body = &blanked[span.start..span.end];
        let has_composition = COMPOSITION_TOKENS.iter().any(|t| blanked_body.contains(t));
        let has_helper_call = manifest::ROUTE_MOUNTING_HELPERS
            .iter()
            .any(|h| body_contains_call(blanked_body, h));
        if has_composition || has_helper_call {
            out.insert((
                file_rel.to_string(),
                span.name.clone(),
                normalize_whitespace(body),
            ));
        }
    }
    out
}

/// The full extracted `(method, path)` set for one router tree, expanding
/// the `mount_log_query_routes` family sentinel against every discovered
/// call-site prefix.
fn extract_mounted_routes(files: &[PathBuf]) -> BTreeSet<(Method, String)> {
    // Collect every `.route(`, plus every `mount_log_query_routes(`
    // call-site prefix. `.merge(`/`.nest(`/`.nest_service(`/route-mounting-
    // helper calls are no longer handled here at all — they are the
    // exact-pinning function-body snapshot's job
    // (`every_pinned_function_body_matches_the_snapshot_exactly`, this
    // module's own doc comment explains why).
    let mut prefixes: BTreeSet<String> = BTreeSet::new();
    let mut raw_routes: Vec<(PathBuf, String, RawRoute)> = Vec::new();
    for file in files {
        let (stripped, blanked) = read_stripped_source(file);
        prefixes.extend(mount_log_query_routes_call_prefixes(
            &stripped, &blanked, file,
        ));
        for raw in extract_route_calls(&stripped, &blanked, file) {
            // Method-chain parsing later runs on the blanked view (its
            // structural tokens are identical in both views).
            raw_routes.push((file.clone(), blanked.clone(), raw));
        }
    }

    let mut mounted = BTreeSet::new();
    for (file, blanked, raw) in &raw_routes {
        let methods = parse_method_chain(
            blanked,
            raw.method_arg_span.0,
            raw.method_arg_span.1,
            file,
            raw.line,
        );
        if let Some(suffix) = raw.path_arg.strip_prefix("\0FAMILY\0") {
            assert!(
                !prefixes.is_empty(),
                "{}:{}: `&format!(\"{{prefix}}{suffix}\")` route path found, but no \
                 `mount_log_query_routes(Router::new(), \"<literal>\")` call site was found \
                 anywhere in the scanned tree to supply a concrete `prefix` value.",
                file.display(),
                raw.line
            );
            for prefix in &prefixes {
                for &method in &methods {
                    mounted.insert((method, format!("{prefix}{suffix}")));
                }
            }
        } else {
            for &method in &methods {
                mounted.insert((method, raw.path_arg.clone()));
            }
        }
    }
    mounted
}

fn manifest_mounted_set() -> BTreeSet<(Method, String)> {
    let mut set = BTreeSet::new();
    for spec in route_manifest() {
        if spec.status != RouteStatus::Mounted {
            continue;
        }
        for &method in spec.methods {
            set.insert((method, spec.path.to_string()));
        }
    }
    set
}

/// Plan v2 finding 1 / AC: "adding an unmatrixed route fails CI" — scans
/// **every** `.rs` file under both router trees (no hardcoded file list)
/// and asserts the extracted `(method, path)` set is exactly the
/// manifest's `Mounted` set, both directions.
#[test]
fn every_source_route_matches_the_manifest_exactly() {
    let root = workspace_root();
    let mut files = rs_files_under(&root.join("crates/pulsus-server/src"));
    files.extend(rs_files_under(&root.join("crates/pulsus-write/src")));
    assert!(
        !files.is_empty(),
        "route-inventory scan found zero .rs files — check workspace_root()"
    );

    let source_mounted = extract_mounted_routes(&files);
    let manifest_mounted = manifest_mounted_set();

    let missing_from_manifest: Vec<_> = source_mounted.difference(&manifest_mounted).collect();
    let missing_from_source: Vec<_> = manifest_mounted.difference(&source_mounted).collect();

    assert!(
        missing_from_manifest.is_empty() && missing_from_source.is_empty(),
        "route manifest has drifted from the router source.\n\
         mounted in source but not in the manifest (add a `RouteSpec`): {missing_from_manifest:?}\n\
         listed as `Mounted` in the manifest but not found in source (fix or remove the \
         `RouteSpec`): {missing_from_source:?}"
    );
}

fn manifest_pinned_function_bodies() -> BTreeSet<(String, String, String)> {
    manifest::pinned_function_bodies()
        .iter()
        .map(|f| {
            (
                f.file.to_string(),
                f.function.to_string(),
                f.body.to_string(),
            )
        })
        .collect()
}

/// Task-manager adjudication on issue #36 round 5 (this module's own doc
/// comment has the full rationale): exact textual pinning of every
/// function whose body contains a `.merge(`/`.nest(`/`.nest_service(`
/// token or a call to a route-mounting helper — the *whole function body*,
/// not a per-call-site sub-expression (round 4's finer, and exploitable,
/// grain). Any new, removed, or textually-changed pinned function's body
/// fails this test — including a second, textually-identical composition
/// call added under a different match arm, which changes the *body's*
/// text even though the individual call reads the same as one already
/// pinned; only a whitespace-only reformat is tolerated (both sides are
/// normalized the same way). Any edit to one of these few load-bearing
/// functions forces snapshot re-derivation and manifest reconciliation —
/// that is the guard doing its job, not a false positive.
#[test]
fn every_pinned_function_body_matches_the_snapshot_exactly() {
    let root = workspace_root();
    let mut files = rs_files_under(&root.join("crates/pulsus-server/src"));
    files.extend(rs_files_under(&root.join("crates/pulsus-write/src")));

    let mut source_bodies: BTreeSet<(String, String, String)> = BTreeSet::new();
    for file in &files {
        let (stripped, blanked) = read_stripped_source(file);
        let rel = file
            .strip_prefix(&root)
            .unwrap_or(file)
            .to_string_lossy()
            .replace('\\', "/");
        source_bodies.extend(extract_pinned_function_bodies(&stripped, &blanked, &rel));
    }

    let snapshot = manifest_pinned_function_bodies();
    let missing_from_snapshot: Vec<_> = source_bodies.difference(&snapshot).collect();
    let missing_from_source: Vec<_> = snapshot.difference(&source_bodies).collect();

    assert!(
        missing_from_snapshot.is_empty() && missing_from_source.is_empty(),
        "the pinned-function-body snapshot has drifted from the router-composition-bearing \
         functions actually in source — re-derive the snapshot \
         (`support::manifest::pinned_function_bodies()`) AND reconcile the route manifest (a \
         changed function body can mean a route moved, was added, or was removed — even a \
         textually-identical second composition call under a different match arm changes the \
         *whole body's* pinned text, by design: re-deriving and reconciling is the correct \
         response, not a false positive).\n\
         present in source but not pinned in the snapshot (a new, removed-then-restored, or \
         textually-changed function body — add/update its `PinnedFunctionBody` entry): \
         {missing_from_snapshot:?}\n\
         pinned in the snapshot but not found in source (the function was removed or its body \
         changed — remove or update its `PinnedFunctionBody` entry): {missing_from_source:?}"
    );
}

/// The `Gate`/`Surface` combination for every mounted `RouteSpec` must be
/// internally consistent — a cheap hermetic sanity check alongside the
/// source-scan above (catches a hand-authored manifest typo the source
/// scan alone would not: e.g. an ingest route accidentally tagged
/// `Surface::PromApi`).
#[test]
fn every_mounted_route_spec_has_a_surface_consistent_gate() {
    for spec in route_manifest() {
        if spec.status != RouteStatus::Mounted {
            continue;
        }
        let ok = match spec.surface {
            Surface::OpsPublic | Surface::OpsAuthed => spec.gate == Gate::Always,
            Surface::Ingest => spec.gate == Gate::WriterMode,
            Surface::LogsQuery => matches!(spec.gate, Gate::ReaderMode | Gate::CompatAndReader),
            Surface::PromApi => spec.gate == Gate::ReaderMode,
            // The native traces routes are ReaderMode; their issue #61
            // pure-binding aliases reuse the same surfaces under
            // CompatAndReader (the LogsQuery precedent).
            Surface::TracesFetch | Surface::TracesSearch | Surface::TracesMetrics => {
                matches!(spec.gate, Gate::ReaderMode | Gate::CompatAndReader)
            }
            // Only the native tag-discovery routes use TracesTags — the
            // aliases are reshaping surfaces, compat-gated by definition.
            Surface::TracesTags => spec.gate == Gate::ReaderMode,
            Surface::TracesTagsV1 | Surface::TracesTagsV2 | Surface::Echo => {
                spec.gate == Gate::CompatAndReader
            }
        };
        assert!(
            ok,
            "{:?}: surface {:?} paired with an inconsistent gate {:?}",
            spec.path, spec.surface, spec.gate
        );
    }
}

fn docs_api_md() -> String {
    let root = workspace_root();
    fs::read_to_string(root.join("docs/api.md")).expect("read docs/api.md")
}

/// The single §8.1 table row documenting the M1 `/loki/api/v1` query alias
/// family — located once by anchoring on its first (always fully-prefixed)
/// entry, then reused for every `DocRef::LokiAliasSuffix` check (plan
/// v2/v3 review findings: matching must be scoped to this one row's line,
/// never a whole-document search, or recurring segments like `values`/
/// `series` elsewhere in §8 would false-positive).
fn loki_alias_table_row(docs: &str) -> &str {
    docs.lines()
        .find(|line| line.contains(&format!("`{}/query_range`", manifest::LOKI_V1)))
        .expect(
            "docs/api.md's §8.1 table must have a row starting with the fully-prefixed \
             `/loki/api/v1/query_range` alias entry",
        )
}

/// A path-segment "continuation" byte — matching a path substring that is
/// immediately followed (or preceded) by one of these is a false positive
/// (`/query` "matching" inside `/query_range` via a naive substring
/// search, round-2 review finding), not a genuine token boundary.
fn is_path_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'-'
}

/// `true` when `needle` appears in `haystack` as a whole path token — its
/// match is not immediately preceded/followed by [`is_path_token_byte`].
/// Segment-exact, boundary-aware (round-2 review finding): `/query` no
/// longer "documents itself" via `/query_range`'s literal prefix.
fn contains_path_token(haystack: &str, needle: &str) -> bool {
    let bytes = haystack.as_bytes();
    let mut start = 0usize;
    while let Some(rel) = haystack[start..].find(needle) {
        let pos = start + rel;
        let end = pos + needle.len();
        let before_ok = pos == 0 || !is_path_token_byte(bytes[pos - 1]);
        let after_ok = end >= bytes.len() || !is_path_token_byte(bytes[end]);
        if before_ok && after_ok {
            return true;
        }
        start = pos + 1;
    }
    false
}

/// Plan v3/v4: reconstructs the documented form of a §8.1 alias entry as
/// either the full path (`LOKI_V1` + `suffix`, verbatim — the row's first
/// entry) or a bare backtick-quoted `suffix` (the row's shorthand
/// entries), and checks it against that one row's text only —
/// boundary-aware for the full-path form (round-2 finding: a naive
/// substring search on `/loki/api/v1/query` would false-match inside
/// `/loki/api/v1/query_range`'s own text; the backtick-quoted form is
/// already exact, since the backticks themselves are delimiters).
fn loki_alias_documented(row: &str, suffix: &str) -> bool {
    contains_path_token(row, &format!("{}{suffix}", manifest::LOKI_V1))
        || row.contains(&format!("`{suffix}`"))
}

/// Plan AC: "api.md gaps found by enumeration get fixed docs-first" — every
/// `Mounted` route's `doc_ref` must resolve against `docs/api.md`'s actual
/// text. `Planned` entries are excluded (`DocRef::Skip`, plan v1/v4:
/// "Planned entries don't fail the guard").
#[test]
fn every_mounted_route_is_documented_in_docs_api_md() {
    let docs = docs_api_md();
    let alias_row = loki_alias_table_row(&docs);

    let mut gaps = Vec::new();
    for spec in route_manifest() {
        if spec.status != RouteStatus::Mounted {
            continue;
        }
        let documented = match spec.doc_ref {
            // Round-3 finding: a plain substring search hides a deleted
            // `/api/v1/query`/`/api/logs/v1/query` behind `.../query_range`'s
            // own literal text (same class of bug the §8.1 alias check
            // fixed in round 2) — boundary-aware everywhere, not just §8.1.
            DocRef::Verbatim => contains_path_token(&docs, spec.path),
            DocRef::LokiAliasSuffix { suffix } => loki_alias_documented(alias_row, suffix),
            DocRef::Skip => true,
        };
        if !documented {
            gaps.push(spec.path);
        }
    }
    assert!(
        gaps.is_empty(),
        "mounted route(s) not found in docs/api.md — fix the docs (docs-first) before this \
         guard passes: {gaps:?}"
    );
}

mod loki_alias_boundary_tests {
    use super::*;

    /// The exact round-2 review regression: a naive substring search for
    /// `/query` inside a row containing `/query_range` used to false-match
    /// (`/query_range`'s own text literally contains `/query` as a
    /// prefix). Deleting the `/query` shorthand entry from the row (this
    /// test's whole point) must now be a genuine, detected gap.
    #[test]
    fn query_no_longer_false_matches_inside_query_range() {
        let row_without_query_shorthand =
            "| `/loki/api/v1/query_range`, `/labels`, `/label/{name}/values`, `/series` | ... |";
        assert!(!loki_alias_documented(
            row_without_query_shorthand,
            "/query"
        ));
    }

    #[test]
    fn query_resolves_via_its_own_backtick_quoted_shorthand_entry() {
        let row = "| `/loki/api/v1/query_range`, `/query`, `/labels`, `/label/{name}/values`, \
                    `/series` | ... |";
        assert!(loki_alias_documented(row, "/query"));
        assert!(loki_alias_documented(row, "/query_range"));
    }

    #[test]
    fn contains_path_token_rejects_a_prefix_match_with_a_continuing_identifier_char() {
        assert!(!contains_path_token(
            "/loki/api/v1/query_range",
            "/loki/api/v1/query"
        ));
    }

    #[test]
    fn contains_path_token_accepts_an_exact_boundary_match() {
        assert!(contains_path_token(
            "/loki/api/v1/query, /loki/api/v1/labels",
            "/loki/api/v1/query"
        ));
    }

    /// Round-3 finding (low), `DocRef::Verbatim`'s own instance of the
    /// same bug class: a document that documents `/api/v1/query_range`
    /// but *not* `/api/v1/query` must not let a plain substring search
    /// hide the missing `/api/v1/query` docs-gap.
    #[test]
    fn verbatim_style_full_document_text_does_not_let_query_range_mask_a_missing_query_entry() {
        let docs_without_query =
            "### 3.2 `GET|POST /api/v1/query_range`\n\nsome other unrelated section text\n";
        assert!(!contains_path_token(docs_without_query, "/api/v1/query"));
        let docs_with_both =
            "### 3.1 `GET|POST /api/v1/query`\n\n### 3.2 `GET|POST /api/v1/query_range`\n";
        assert!(contains_path_token(docs_with_both, "/api/v1/query"));
        assert!(contains_path_token(docs_with_both, "/api/v1/query_range"));
    }
}

// -- Pinned-function-body exact pinning: hermetic unit coverage ----------
//
// Task-manager adjudication on issue #36, rounds 4 and 5 (this module's
// own doc comment has the full rationale): "a drift guard needs to detect
// CHANGE, not prove semantics", pinned at the coarser whole-function-body
// grain round 5 required. These synthetic-source tests exercise the
// extraction/normalization machinery directly; a real end-to-end trip was
// also confirmed manually against `crates/pulsus-server/src/subsystems.rs`
// (round 4, receiver swap) and `crates/pulsus-server/src/modes.rs` (round
// 5, a second identical merge added under a new match arm), both
// reverted — not committed, since either would otherwise permanently
// break the drift guard.
mod pinned_function_body_tests {
    use super::*;

    #[test]
    fn normalize_whitespace_collapses_runs_and_trims() {
        assert_eq!(
            normalize_whitespace("  a.merge(\n    b(),\n)  "),
            "a.merge(b())"
        );
    }

    #[test]
    fn normalize_whitespace_is_a_no_op_on_already_single_spaced_text() {
        assert_eq!(
            normalize_whitespace("router.merge(writer_router())"),
            "router.merge(writer_router())"
        );
    }

    #[test]
    fn normalize_whitespace_removes_a_space_before_a_wrapped_chain_dot() {
        assert_eq!(
            normalize_whitespace("router\n    .merge(x())"),
            "router.merge(x())"
        );
    }

    #[test]
    fn normalize_whitespace_removes_a_trailing_comma_before_a_close_paren() {
        assert_eq!(normalize_whitespace("merge(x, y,)"), "merge(x, y)");
        assert_eq!(normalize_whitespace("merge(x, y,\n)"), "merge(x, y)");
    }

    #[test]
    fn body_contains_call_matches_an_identifier_boundary_call() {
        assert!(body_contains_call(
            "mount_log_query_routes(Router::new(), \"/x\")",
            "mount_log_query_routes"
        ));
    }

    #[test]
    fn body_contains_call_does_not_match_a_longer_identifier_prefix() {
        assert!(!body_contains_call(
            "mount_log_query_routes_extra(Router::new())",
            "mount_log_query_routes"
        ));
    }

    fn extract(src: &str) -> BTreeSet<(String, String, String)> {
        let blanked = preprocess_views(src).1;
        extract_pinned_function_bodies(src, &blanked, "synthetic.rs")
    }

    #[test]
    fn a_function_with_a_merge_is_pinned_whole_body() {
        let src = "fn reader_router() -> Router<AppState> {\n    crate::logs_api::router().merge(crate::prom_api::router())\n}\n";
        let calls = extract(src);
        assert_eq!(
            calls,
            BTreeSet::from([(
                "synthetic.rs".to_string(),
                "reader_router".to_string(),
                "crate::logs_api::router().merge(crate::prom_api::router())".to_string(),
            )])
        );
    }

    #[test]
    fn a_function_calling_a_route_mounting_helper_is_pinned_even_with_no_merge() {
        let src = "fn router() -> Router<AppState> {\n    mount_log_query_routes(Router::new(), \"/api/logs/v1\")\n}\n";
        assert_eq!(
            extract(src),
            BTreeSet::from([(
                "synthetic.rs".to_string(),
                "router".to_string(),
                "mount_log_query_routes(Router::new(), \"/api/logs/v1\")".to_string(),
            )])
        );
    }

    #[test]
    fn a_function_with_neither_is_not_pinned() {
        let src = "fn ruler_router() -> Router<AppState> {\n    Router::new()\n}\n";
        assert!(extract(src).is_empty());
    }

    /// Round-4's core regression, still caught at the whole-body grain:
    /// swapping a chain's leading receiver changes the pinned body text.
    #[test]
    fn receiver_swap_changes_the_pinned_body() {
        let original = "fn reader_router() -> Router<AppState> {\n    crate::logs_api::router().merge(crate::prom_api::router())\n}\n";
        let swapped = "fn reader_router() -> Router<AppState> {\n    external_crate::router().merge(crate::prom_api::router())\n}\n";
        assert_ne!(extract(original), extract(swapped));
    }

    #[test]
    fn argument_swap_changes_the_pinned_body() {
        let original = "fn reader_router() -> Router<AppState> {\n    crate::logs_api::router().merge(crate::prom_api::router())\n}\n";
        let swapped = "fn reader_router() -> Router<AppState> {\n    crate::logs_api::router().merge(external_crate::router())\n}\n";
        assert_ne!(extract(original), extract(swapped));
    }

    /// Round-5's core regression: a *second*, textually-identical
    /// `.merge(...)` call added under a different match arm must not
    /// collapse onto the existing pinned entry — round 4's per-call-site
    /// pinning missed exactly this (two identical `(file, fn, text)`
    /// tuples deduplicate in a set); whole-body pinning catches it because
    /// the function's *body* text is now longer/different regardless of
    /// whether the added call reads identically to one already present.
    #[test]
    fn a_second_identical_merge_in_another_match_arm_trips() {
        let before = concat!(
            "fn mount_subsystems(router: Router<AppState>, cfg: &Config) -> Router<AppState> {\n",
            "    let mut router = router;\n",
            "    match subsystem {\n",
            "        Subsystem::Writer => router = router.merge(writer_router()),\n",
            "        Subsystem::Reader => router = router.merge(reader_router()),\n",
            "    }\n",
            "    router\n",
            "}\n",
        );
        // A second, textually-identical `.merge(writer_router())` added
        // under a *new* arm (e.g. a hypothetical `Subsystem::Extra`) —
        // same exact call text as the Writer arm already has, but the
        // function now mounts it under a different condition too.
        let after = concat!(
            "fn mount_subsystems(router: Router<AppState>, cfg: &Config) -> Router<AppState> {\n",
            "    let mut router = router;\n",
            "    match subsystem {\n",
            "        Subsystem::Writer => router = router.merge(writer_router()),\n",
            "        Subsystem::Reader => router = router.merge(reader_router()),\n",
            "        Subsystem::Extra => router = router.merge(writer_router()),\n",
            "    }\n",
            "    router\n",
            "}\n",
        );
        let before_set = extract(before);
        let after_set = extract(after);
        assert_ne!(
            before_set, after_set,
            "a second, textually-identical composition call under a new match arm must change \
             the pinned whole-body text"
        );
        // Precisely one pinned entry either side (whole-body grain, not
        // one entry per call site) — the drift is visible as a changed
        // `mount_subsystems` body, not as a missing/extra `BTreeSet` count.
        assert_eq!(before_set.len(), 1);
        assert_eq!(after_set.len(), 1);
    }

    /// Round-5's other explicit scenario: moving a call site (e.g.
    /// reordering match arms) without changing the actual content still
    /// changes the function's body text, because ordering is part of it.
    #[test]
    fn a_moved_call_site_trips() {
        let original = concat!(
            "fn mount_subsystems(router: Router<AppState>, cfg: &Config) -> Router<AppState> {\n",
            "    match subsystem {\n",
            "        Subsystem::Writer => router.merge(writer_router()),\n",
            "        Subsystem::Reader => router.merge(reader_router()),\n",
            "    }\n",
            "}\n",
        );
        let reordered = concat!(
            "fn mount_subsystems(router: Router<AppState>, cfg: &Config) -> Router<AppState> {\n",
            "    match subsystem {\n",
            "        Subsystem::Reader => router.merge(reader_router()),\n",
            "        Subsystem::Writer => router.merge(writer_router()),\n",
            "    }\n",
            "}\n",
        );
        assert_ne!(extract(original), extract(reordered));
    }

    /// Unchanged source extracts an identical set.
    #[test]
    fn unchanged_source_extracts_an_identical_set() {
        let src = "fn reader_router() -> Router<AppState> {\n    crate::logs_api::router().merge(crate::prom_api::router())\n}\n";
        assert_eq!(extract(src), extract(src));
    }

    /// A pure reformat (re-wrapped, re-indented) of a pinned function must
    /// extract the *same* normalized body text.
    #[test]
    fn formatting_only_change_does_not_trip_the_snapshot() {
        let one_line = "fn f(router: Router<AppState>) -> Router<AppState> {\n    router.merge(writer_router())\n}\n";
        let reformatted = concat!(
            "fn f(router: Router<AppState>) -> Router<AppState> {\n",
            "    router\n",
            "        .merge(\n",
            "            writer_router(),\n",
            "        )\n",
            "}\n",
        );
        assert_eq!(extract(one_line), extract(reformatted));
    }

    /// Round-5 finding 1: a non-literal `mount_log_query_routes(...)`
    /// prefix argument at a real (non-definition) call site must hard-fail
    /// — a computed prefix would silently mount an entire unmanifested
    /// route family.
    #[test]
    #[should_panic(expected = "is not a string literal")]
    fn a_computed_mount_helper_prefix_hard_fails() {
        let src = "fn router(prefix: &str) -> Router<AppState> {\n    mount_log_query_routes(Router::new(), prefix.as_str())\n}\n";
        let _ = mount_log_query_routes_call_prefixes(
            src,
            &preprocess_views(src).1,
            Path::new("synthetic.rs"),
        );
    }

    /// The function's own *definition* line (which also textually contains
    /// `mount_log_query_routes(`, as its parameter list) must never be
    /// mistaken for a call site — its second "argument" (`prefix: &str`)
    /// is a parameter declaration, not a literal, and must not hard-fail.
    #[test]
    fn the_helper_s_own_definition_is_never_treated_as_a_call_site() {
        let src = "pub(crate) fn mount_log_query_routes(router: Router<AppState>, prefix: &str) -> Router<AppState> {\n    router.route(&format!(\"{prefix}/query\"), get(h))\n}\n";
        let prefixes = mount_log_query_routes_call_prefixes(
            src,
            &preprocess_views(src).1,
            Path::new("synthetic.rs"),
        );
        assert!(prefixes.is_empty());
    }

    #[test]
    fn a_literal_mount_helper_prefix_is_extracted() {
        let src = "fn router() -> Router<AppState> {\n    mount_log_query_routes(Router::new(), \"/api/logs/v1\")\n}\n";
        let prefixes = mount_log_query_routes_call_prefixes(
            src,
            &preprocess_views(src).1,
            Path::new("synthetic.rs"),
        );
        assert_eq!(prefixes, BTreeSet::from(["/api/logs/v1".to_string()]));
    }

    /// End to end through the real snapshot: this codebase's actual
    /// `support::manifest::pinned_function_bodies()` must match the
    /// actual live source exactly — the same assertion
    /// `every_pinned_function_body_matches_the_snapshot_exactly` makes,
    /// restated here as a focused unit alongside the synthetic cases
    /// above.
    #[test]
    fn the_real_snapshot_matches_the_real_source_tree() {
        let root = workspace_root();
        let mut files = rs_files_under(&root.join("crates/pulsus-server/src"));
        files.extend(rs_files_under(&root.join("crates/pulsus-write/src")));
        let mut source_bodies: BTreeSet<(String, String, String)> = BTreeSet::new();
        for file in &files {
            let (stripped, blanked) = read_stripped_source(file);
            let rel = file
                .strip_prefix(&root)
                .unwrap_or(file)
                .to_string_lossy()
                .replace('\\', "/");
            source_bodies.extend(extract_pinned_function_bodies(&stripped, &blanked, &rel));
        }
        assert_eq!(source_bodies, manifest_pinned_function_bodies());
    }
}

// -- Rounds 6-8: policy passes (UFCS, service mounts, alias ban,
// helper-scoped format! paths) — hermetic unit coverage -------------------
mod policy_pass_tests {
    use super::*;

    /// Mirrors `read_stripped_source`'s exact policy pipeline on synthetic
    /// source (comments/test-mods assumed already absent from the
    /// fixtures): one shared blanked view, every policy pass run against
    /// it — so these tests exercise the same blanking/ordering the real
    /// scan uses, not each pass in an artificial configuration.
    fn run_policy_checks(src: &str) {
        let blanked = preprocess_views(src).1;
        let file = Path::new("synthetic.rs");
        assert_no_router_alias_imports(&blanked, file);
        assert_no_router_type_aliases(&blanked, file);
        assert_no_ufcs_router_composition(&blanked, file);
        assert_no_service_mounts(&blanked, file);
    }

    // -- UFCS (rounds 6-7) ------------------------------------------------

    #[test]
    #[should_panic(expected = "router composition must use method-call syntax")]
    fn a_ufcs_nest_call_hard_fails() {
        run_policy_checks(
            "fn f() -> Router<AppState> {\n    Router::nest(existing, \"/hidden\", other_router())\n}\n",
        );
    }

    #[test]
    #[should_panic(expected = "router composition must use method-call syntax")]
    fn a_ufcs_merge_call_with_an_axum_path_prefix_hard_fails() {
        // The optional leading path (`axum::`, or any other) is covered
        // for free: `"axum::Router::merge("` contains `"Router::merge("`
        // as a substring.
        run_policy_checks("fn f() -> Router<AppState> {\n    axum::Router::merge(a, b)\n}\n");
    }

    #[test]
    fn ordinary_method_call_syntax_composition_never_trips_the_ufcs_check() {
        run_policy_checks("fn f() -> Router<AppState> {\n    router.merge(other())\n}\n");
    }

    /// Round-6 finding 1's exact regression, end to end: a UFCS `.nest(`
    /// mounting an unmanifested path inside an otherwise-unpinned function
    /// must still be caught — by the UFCS check, not by pinning.
    #[test]
    #[should_panic(expected = "router composition must use method-call syntax")]
    fn a_ufcs_composition_call_hidden_in_an_unpinned_function_is_still_caught() {
        run_policy_checks(concat!(
            "fn writer_router() -> Router<AppState> {\n",
            "    let existing = Router::new().route(\"/v1/logs\", post(h));\n",
            "    Router::nest(existing, \"/hidden\", crate::prom_api::router())\n",
            "}\n",
        ));
    }

    #[test]
    #[should_panic(expected = "router composition must use method-call syntax")]
    fn a_turbofish_ufcs_call_is_caught() {
        run_policy_checks(
            "fn f() -> Router<AppState> {\n    Router::<AppState>::nest(existing, other_router())\n}\n",
        );
    }

    /// UFCS-looking text inside a string literal (a log message, an error
    /// string) must never trip the guard — every policy pass runs on the
    /// shared blanked view (route-path *extraction* keeps literals, since
    /// literals are exactly what it extracts).
    #[test]
    fn ufcs_looking_text_inside_a_string_literal_is_not_flagged() {
        run_policy_checks(
            "fn f() {\n    tracing::warn!(\"never call Router::nest( directly\");\n}\n",
        );
    }

    /// UFCS forms of the round-7 additions are covered by the same
    /// per-method token set.
    #[test]
    #[should_panic(expected = "router composition must use method-call syntax")]
    fn a_ufcs_fallback_call_is_caught() {
        run_policy_checks(
            "fn f() -> Router<AppState> {\n    Router::fallback(existing, handler)\n}\n",
        );
    }

    #[test]
    #[should_panic(expected = "router composition must use method-call syntax")]
    fn a_ufcs_route_service_call_is_caught() {
        run_policy_checks(
            "fn f() -> Router<AppState> {\n    Router::route_service(existing, \"/hidden\", svc)\n}\n",
        );
    }

    // -- Router-alias import ban (round 8, replacing alias-call matching) --

    #[test]
    #[should_panic(expected = "do not alias axum::Router")]
    fn a_single_item_router_alias_import_hard_fails() {
        run_policy_checks("use axum::Router as AxumRouter;\nfn f() {}\n");
    }

    /// Round-8's core regression: a brace-grouped alias with a short name
    /// (`R`) — round-7's alias-*call* matching missed the grouped-import
    /// form entirely, so `R::nest(...)` sailed through. The import-time
    /// ban catches the alias at its declaration, making the call form
    /// unrepresentable.
    #[test]
    #[should_panic(expected = "do not alias axum::Router")]
    fn a_brace_grouped_router_alias_import_hard_fails() {
        run_policy_checks("use axum::{Router as R};\nfn f() {}\n");
    }

    #[test]
    #[should_panic(expected = "do not alias axum::Router")]
    fn a_nested_group_router_alias_import_hard_fails() {
        run_policy_checks("use axum::{routing::get, Router as R};\nfn f() {}\n");
    }

    #[test]
    #[should_panic(expected = "do not alias axum::Router")]
    fn a_pub_use_router_alias_reexport_hard_fails() {
        run_policy_checks("pub use axum::Router as PublicRouter;\nfn f() {}\n");
    }

    /// The identifier-boundary check: a *different* type merely ending in
    /// "Router" (`MyRouter`) may be aliased freely — only axum's `Router`
    /// spelling is banned (round-8: round-7's `rsplit("::")` fallback
    /// false-matched exactly this).
    #[test]
    fn aliasing_an_unrelated_type_ending_in_router_is_legal() {
        run_policy_checks("use crate::routing::MyRouter as M;\nfn f() {}\n");
    }

    #[test]
    fn a_plain_router_import_is_legal() {
        run_policy_checks("use axum::Router;\nuse axum::routing::get;\nfn f() {}\n");
    }

    /// Round-8 false-positive fix: `"Router as "` inside a string literal
    /// must never trip the alias ban.
    #[test]
    fn router_as_text_inside_a_string_literal_is_not_flagged() {
        run_policy_checks(
            "fn f() {\n    tracing::warn!(\"never import Router as an alias; use Router directly\");\n}\n",
        );
    }

    // -- Service-mount ban (round 7, blanked per round 8) -------------------

    #[test]
    #[should_panic(expected = "mounting an opaque `Service`")]
    fn a_route_service_call_hard_fails() {
        run_policy_checks(
            "fn f() -> Router<AppState> {\n    Router::new().route_service(\"/hidden\", svc)\n}\n",
        );
    }

    #[test]
    #[should_panic(expected = "mounting an opaque `Service`")]
    fn a_fallback_service_call_hard_fails() {
        run_policy_checks(
            "fn f() -> Router<AppState> {\n    Router::new().fallback_service(svc)\n}\n",
        );
    }

    /// Round-8 false-positive fix: a service-mount token inside a string
    /// constant must never fail CI.
    #[test]
    fn service_mount_token_inside_a_string_literal_is_not_flagged() {
        run_policy_checks(
            "const HINT: &str = \"do not use .route_service( or .fallback_service( here\";\nfn f() {}\n",
        );
    }

    // -- Pinning additions (round 7) ---------------------------------------

    fn extract(src: &str) -> BTreeSet<(String, String, String)> {
        let blanked = preprocess_views(src).1;
        extract_pinned_function_bodies(src, &blanked, "synthetic.rs")
    }

    /// Plain `.fallback(` (a handler, no path) joins the pinned-body set —
    /// the containing function's whole body becomes part of the snapshot,
    /// so adding one to a previously-unpinned function is a detected
    /// drift, not a silent mount.
    #[test]
    fn a_fallback_handler_pins_the_containing_function_body() {
        let src =
            "fn f() -> Router<AppState> {\n    Router::new().fallback(not_found_handler)\n}\n";
        assert_eq!(
            extract(src),
            BTreeSet::from([(
                "synthetic.rs".to_string(),
                "f".to_string(),
                "Router::new().fallback(not_found_handler)".to_string(),
            )])
        );
    }

    #[test]
    fn a_method_not_allowed_fallback_pins_the_containing_function_body() {
        let src = "fn f() -> Router<AppState> {\n    Router::new().method_not_allowed_fallback(handler)\n}\n";
        assert_eq!(extract(src).len(), 1);
    }

    /// Round-8: a composition token inside a string constant must not
    /// spuriously pin the containing function (selection runs on the
    /// blanked view; the real body text, literals included, is what gets
    /// pinned once a *code* token selects the function).
    #[test]
    fn a_composition_token_inside_a_string_literal_does_not_pin_the_function() {
        let src = "fn f() -> String {\n    format!(\"routers use .merge( to compose\")\n}\n";
        assert!(extract(src).is_empty());
    }

    // -- Helper-scoped format! paths (round 6) ------------------------------

    /// Round-6 finding 2's exact regression: a computed/formatted route
    /// path inside a function that is *not* a listed route-mounting
    /// helper must hard-fail, never silently expand (and thereby collide
    /// with an unrelated known prefix).
    #[test]
    #[should_panic(expected = "is not inside one of")]
    fn a_format_path_in_an_unlisted_function_hard_fails() {
        let src = concat!(
            "fn writer_router() -> Router<AppState> {\n",
            "    let prefix = \"/hidden\";\n",
            "    Router::new().route(&format!(\"{prefix}/query\"), get(h))\n",
            "}\n",
        );
        let _ = extract_route_calls(src, &preprocess_views(src).1, Path::new("synthetic.rs"));
    }

    /// The one sanctioned shape still works: a formatted path inside the
    /// listed helper itself expands to the family sentinel, unchanged.
    #[test]
    fn a_format_path_inside_a_listed_helper_still_expands_correctly() {
        let src = concat!(
            "fn mount_log_query_routes(router: Router<AppState>, prefix: &str) -> Router<AppState> {\n",
            "    router.route(&format!(\"{prefix}/query_range\"), get(h).post(h2))\n",
            "}\n",
        );
        let raws = extract_route_calls(src, &preprocess_views(src).1, Path::new("synthetic.rs"));
        assert_eq!(raws.len(), 1);
        assert_eq!(raws[0].path_arg, "\0FAMILY\0/query_range");
    }

    /// A formatted path inside a function with the *same shape* as the
    /// helper but a different, unlisted name must still hard-fail — the
    /// gate is the enclosing function's identity, not the syntactic shape
    /// of the call.
    #[test]
    #[should_panic(expected = "is not inside one of")]
    fn an_unlisted_helper_with_the_same_format_shape_still_hard_fails() {
        let src = concat!(
            "fn mount_other_routes(router: Router<AppState>, prefix: &str) -> Router<AppState> {\n",
            "    router.route(&format!(\"{prefix}/query\"), get(h).post(h2))\n",
            "}\n",
        );
        let _ = extract_route_calls(src, &preprocess_views(src).1, Path::new("synthetic.rs"));
    }

    // -- Router type-alias ban (round 9) -----------------------------------

    /// Round-9's exact bypass construction: `type AppRouter =
    /// Router<AppState>; AppRouter::nest(...)` matches neither
    /// `Router::nest(` nor `.nest(` — banned at the declaration line,
    /// same policy family as the round-8 `use`-alias ban.
    #[test]
    #[should_panic(expected = "this `type` declaration aliases `Router`")]
    fn a_router_type_alias_ufcs_bypass_hard_fails_at_the_declaration() {
        run_policy_checks(concat!(
            "type AppRouter = Router<AppState>;\n",
            "fn f() -> AppRouter {\n",
            "    AppRouter::nest(existing, \"/hidden\", other_router())\n",
            "}\n",
        ));
    }

    #[test]
    #[should_panic(expected = "this `type` declaration aliases `Router`")]
    fn a_pub_type_router_alias_hard_fails() {
        run_policy_checks("pub type PublicRouter = axum::Router<AppState>;\nfn f() {}\n");
    }

    /// Identifier boundaries both sides: a type alias over a *different*
    /// type merely containing "Router" in its name stays legal.
    #[test]
    fn a_type_alias_over_an_unrelated_router_named_type_is_legal() {
        run_policy_checks("type M = crate::routing::MyRouter;\ntype E = RouterExtra;\nfn f() {}\n");
    }

    /// Ordinary non-Router type aliases (this codebase has several) stay
    /// legal.
    #[test]
    fn ordinary_type_aliases_are_legal() {
        run_policy_checks("pub type StreamKey = (u64, u16);\nfn f() {}\n");
    }

    // -- Raw-string lexing (round 9) ----------------------------------------

    /// Round-9's exact desync regression: a raw string containing an
    /// unescaped quote, followed by a *real* composition call — the old
    /// quote-loop lexer treated the raw string's interior `"` as a
    /// terminator, desynchronizing the blanked view so the real `.nest(`
    /// after it landed inside a phantom "string" and was blanked away
    /// from every policy pass. The lexer-backed blanking must keep the
    /// real call visible (here: to composition-token selection).
    #[test]
    fn a_raw_string_with_an_interior_quote_does_not_hide_a_following_merge() {
        let src = concat!(
            "fn f() -> Router<AppState> {\n",
            "    let msg = r#\"a \" b\"#;\n",
            "    router.nest(\"/x\", other())\n",
            "}\n",
        );
        let blanked = preprocess_views(src).1;
        // The `.nest(` token must survive blanking (it is real code)...
        assert!(
            blanked.contains(".nest("),
            "real .nest( must not be blanked, blanked view: {blanked:?}"
        );
        // ...and the function must therefore be pinned.
        assert_eq!(
            extract_pinned_function_bodies(src, &blanked, "synthetic.rs").len(),
            1
        );
    }

    /// Composition tokens *inside* a raw string are blanked (the inverse
    /// direction: raw-string contents never trip a policy pass).
    #[test]
    fn composition_tokens_inside_a_raw_string_are_blanked() {
        let src = "fn f() -> String {\n    r#\"routers use .merge( and Router::nest( to compose\"#.to_string()\n}\n";
        run_policy_checks(src); // must not panic.
        let blanked = preprocess_views(src).1;
        assert!(!blanked.contains(".merge("));
        assert!(
            extract_pinned_function_bodies(src, &blanked, "synthetic.rs").is_empty(),
            "a raw-string token must not pin the function"
        );
    }

    /// Nested-hash raw strings (`r##"..."##`) lex correctly: the interior
    /// `"#` does not terminate them.
    #[test]
    fn nested_hash_raw_strings_lex_correctly() {
        let src = concat!(
            "fn f() -> Router<AppState> {\n",
            "    let msg = r##\"contains \"# inside\"##;\n",
            "    router.merge(other())\n",
            "}\n",
        );
        let blanked = preprocess_views(src).1;
        assert!(blanked.contains(".merge("));
        assert!(!blanked.contains("inside"));
    }

    /// Byte/raw-byte string variants (`b"..."`, `br#"..."#`) are lexed by
    /// the same helper.
    #[test]
    fn byte_and_raw_byte_strings_are_blanked() {
        let src = "fn f() {\n    let a = b\".merge(\";\n    let c = br#\".nest( \" here\"#;\n    let real = x.merge(y);\n}\n";
        let blanked = preprocess_views(src).1;
        // Exactly the one real `.merge(` survives.
        assert_eq!(blanked.matches(".merge(").count(), 1);
        assert!(!blanked.contains(".nest("));
    }

    /// A char literal containing a quote (`'"'`) must not desynchronize
    /// the scan either.
    #[test]
    fn a_quote_char_literal_does_not_desynchronize_blanking() {
        let src =
            "fn f() {\n    let q = '\"';\n    let real = x.merge(y);\n    let s = \".nest(\";\n}\n";
        let blanked = preprocess_views(src).1;
        assert!(blanked.contains(".merge("));
        assert!(!blanked.contains(".nest("));
    }
}

// -- Round-10: unified single-pass preprocessing --------------------------
mod unified_preprocessing_tests {
    use super::*;

    /// Round-10's exact desync regression: a raw string containing `//`
    /// (URL-like content), followed by real composition code. The old
    /// two-stage pipeline's comment stripper mis-lexed the raw string
    /// with an ordinary-quote loop, saw the interior `//` as a comment
    /// start, and blanked the closing delimiter plus the real `.merge(`
    /// after it. The unified single pass must keep the real call visible
    /// in BOTH views.
    #[test]
    fn a_raw_string_containing_slashes_does_not_hide_following_composition_code() {
        let src = concat!(
            "fn f() -> Router<AppState> {\n",
            "    let payload = r#\"{\"url\":\"https://x\"}\"#;\n",
            "    router.merge(other_router())\n",
            "}\n",
        );
        let (stripped, blanked) = preprocess_views(src);
        assert!(
            stripped.contains(".merge("),
            "stripped view must keep the real .merge(, got {stripped:?}"
        );
        assert!(
            blanked.contains(".merge("),
            "blanked view must keep the real .merge(, got {blanked:?}"
        );
        // The raw string's contents are blanked from the policy view...
        assert!(!blanked.contains("https"));
        // ...but intact in the extraction view.
        assert!(stripped.contains("https"));
        // And the function is pinned (selection runs on the blanked view).
        assert_eq!(
            extract_pinned_function_bodies(&stripped, &blanked, "synthetic.rs").len(),
            1
        );
    }

    /// A comment containing an unbalanced quote must not desynchronize
    /// the lexer for the real code after it, in either view.
    #[test]
    fn a_comment_containing_a_quote_does_not_hide_following_code() {
        let src = concat!(
            "fn f() -> Router<AppState> {\n",
            "    // say \"hi to the reader\n",
            "    router.merge(other_router())\n",
            "}\n",
        );
        let (stripped, blanked) = preprocess_views(src);
        assert!(stripped.contains(".merge("));
        assert!(blanked.contains(".merge("));
        assert!(!stripped.contains("say"), "comments blanked in both views");
        assert!(!blanked.contains("say"));
    }

    /// Both views come out of one pass with identical byte offsets: a
    /// route path literal stays intact in the stripped view at the same
    /// position where the blanked view has spaces.
    #[test]
    fn both_views_are_offset_identical_and_differ_only_in_literals() {
        let src = "fn f() -> Router<AppState> {\n    r.route(\"/x\", get(h)) // c\n}\n";
        let (stripped, blanked) = preprocess_views(src);
        assert_eq!(stripped.len(), src.len());
        assert_eq!(blanked.len(), src.len());
        assert!(stripped.contains("\"/x\""));
        assert!(!blanked.contains("\"/x\""));
        assert!(!stripped.contains("// c"));
        let route_pos_src = src.find(".route(").unwrap();
        assert_eq!(stripped.find(".route("), Some(route_pos_src));
        assert_eq!(blanked.find(".route("), Some(route_pos_src));
    }

    /// A doc comment illustrating composition code must be blanked in
    /// both views (the round-6 false-positive class, restated against the
    /// unified pass).
    #[test]
    fn doc_comment_composition_examples_are_blanked_in_both_views() {
        let src = "/// e.g. `router.merge(authed)` composes.\nfn f() {}\n";
        let (stripped, blanked) = preprocess_views(src);
        assert!(!stripped.contains(".merge("));
        assert!(!blanked.contains(".merge("));
    }
}

// -- Round-11: blanked-view-only structural scanning ----------------------
mod blanked_structural_scanning_tests {
    use super::*;

    fn extract(src: &str) -> Vec<RawRoute> {
        let (stripped, blanked) = preprocess_views(src);
        extract_route_calls(&stripped, &blanked, Path::new("synthetic.rs"))
    }

    /// Round-11's exact desync regression: two chained routes where the
    /// first's inline handler contains a raw string with an interior
    /// quote. The old quote-loop-based `matching_close` mis-lexed the raw
    /// string, swallowed the first call's closing paren, and folded the
    /// second `.route(` into the first — omitting an unmanifested route.
    /// Blanked-view scanning must extract both, with the right paths and
    /// methods.
    #[test]
    fn a_raw_string_with_an_interior_quote_inside_a_handler_does_not_swallow_the_next_route() {
        let src = concat!(
            "fn f() -> Router<AppState> {\n",
            "    Router::new()\n",
            "        .route(\"/a\", get(|| async { r#\"say \" hi\"# }))\n",
            "        .route(\"/b\", post(h))\n",
            "}\n",
        );
        let raws = extract(src);
        assert_eq!(
            raws.len(),
            2,
            "both routes must be extracted, got {:?}",
            raws.iter().map(|r| &r.path_arg).collect::<Vec<_>>()
        );
        assert_eq!(raws[0].path_arg, "/a");
        assert_eq!(raws[1].path_arg, "/b");
        let (_, blanked) = preprocess_views(src);
        let m0 = parse_method_chain(
            &blanked,
            raws[0].method_arg_span.0,
            raws[0].method_arg_span.1,
            Path::new("synthetic.rs"),
            raws[0].line,
        );
        let m1 = parse_method_chain(
            &blanked,
            raws[1].method_arg_span.0,
            raws[1].method_arg_span.1,
            Path::new("synthetic.rs"),
            raws[1].line,
        );
        assert_eq!(m0, vec![Method::Get]);
        assert_eq!(m1, vec![Method::Post]);
    }

    /// A path literal containing `(` and `,` must not break paren
    /// matching or top-level comma splitting — those bytes are spaces in
    /// the blanked view the structural scan walks, while the extracted
    /// path text (from the stripped view) keeps them verbatim.
    #[test]
    fn parens_and_commas_inside_a_path_literal_do_not_break_structural_parsing() {
        let src = "fn f() -> Router<AppState> {\n    Router::new().route(\"/a(b,c\", get(h))\n}\n";
        let raws = extract(src);
        assert_eq!(raws.len(), 1);
        assert_eq!(raws[0].path_arg, "/a(b,c");
    }

    /// A `.route(` inside a string literal is spaces in the blanked view
    /// and can never false-match a call site (a structural consequence of
    /// locating call sites on the blanked view — previously the search ran
    /// on the literals-intact view).
    #[test]
    fn route_token_inside_a_string_literal_is_not_a_call_site() {
        let src = "fn f() -> String {\n    \".route(\\\"/fake\\\", get(h))\".to_string()\n}\n";
        assert!(extract(src).is_empty());
    }

    /// Same principle for the mount helper: a literal containing the
    /// helper's name is never a call site.
    #[test]
    fn mount_helper_token_inside_a_string_literal_is_not_a_call_site() {
        let src = "fn f() -> String {\n    \"mount_log_query_routes(r, prefix)\".to_string()\n}\n";
        let (stripped, blanked) = preprocess_views(src);
        let prefixes =
            mount_log_query_routes_call_prefixes(&stripped, &blanked, Path::new("synthetic.rs"));
        assert!(prefixes.is_empty());
    }
}
