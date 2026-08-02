//! **LEAF MODULE — the only place a `metric_series` window bound or a
//! label-matcher predicate can be rendered from the sanctioned
//! components** (issue #315, review rounds 1–3; the one boundary
//! crossing rustc does NOT police is stated under "What rustc enforces —
//! and what it does not").
//!
//! # Why the renderer lives in a leaf module
//!
//! Issue #315 has to hold an invariant across *every* builder that reaches
//! `metric_series`: a user regex may not be rendered into SQL without the
//! constant compile probe that forces ClickHouse's RE2 to adjudicate it
//! (see [`SeriesWhere`]'s own doc for what the probe is and why it is
//! spliced into the bound). The first cut of that fix satisfied the
//! invariant at all five call sites and left it as a **hand-maintained
//! enumeration**: a sixth builder could call the private predicate
//! renderer, skip the probe, and pass every gate — the coverage test was a
//! list of builders a person had to remember to extend.
//!
//! **Private is a scope, not a restriction, and the scope must be a
//! leaf.** A private item is visible throughout the defining module's
//! subtree, so a private `fn` in `sql.rs` is reachable from every other
//! `fn` in `sql.rs`, and a private constructor in `metrics/mod.rs` is
//! reachable from every descendant — `sql.rs` included, which is how
//! review round 2 found the #240 capability token still constructible
//! from the builders' own file. Everything the hole could be rebuilt from
//! is therefore *written* HERE, in a leaf with no children: `floored_bound`,
//! `anchored_re2_literal`, `matcher_regex_literal`, `re2_compile_probe`
//! and `predicate` are module-private with no visibility modifier at all;
//! [`SeriesWhere`]'s field is private; and [`PromqlRe2Fallback`] — the
//! token the `pub(crate)` escaper demands — has a private field and a
//! private `new`, so no other module can present one.
//!
//! **Written here is not the same as reachable only from here**, and the
//! difference is not academic: review round 9 defined a macro in a sibling
//! module, invoked it at associated-item position inside this file's own
//! `impl SeriesWhere`, and had it expand to a `pub(super)` wrapper around
//! the private `predicate`. From `sql.rs` that rendered
//! `match(JSONExtractString(labels, 'job'), '(?-s)^(?:5..)$')` with **no
//! compile probe** — the #315 hole itself — while every test in this file
//! stayed green. Rust's privacy is per-module, and a macro invoked inside
//! the module expands inside it, so the privates above are reachable from
//! any text a member of this module admits.
//!
//! That is why the rule below is stated as an obligation on authors rather
//! than a guarantee from the compiler, and why the seal is completed by
//! #328's extraction into `pulsus-re2` rather than by anything in here.
//!
//! # Boundary inventory
//!
//! Six declarations carry a visibility modifier, pinned by the in-file
//! census test `the_boundary_inventory_is_pinned` because a
//! hand-maintained inventory here was wrong once (review round 3 found
//! it listing three of the six). That pin reads `pub`-prefixed lines and
//! `impl` headers, not the language's notion of visibility — see the
//! test's own doc for what it cannot see:
//!
//! * [`SeriesWhere`] — the type name (`pub(super)`); its `tail` field
//!   stays private, so the type can be named but not forged, and there is
//!   no direct `w.tail` access. Its derived `Debug` does print `tail`,
//!   which confers nothing beyond what `where_tail` already hands to the
//!   same audience.
//! * [`SeriesWhere::new`] — consumes the matchers and renders bound,
//!   probe and matcher conjuncts together.
//! * [`SeriesWhere::where_tail`] — the only accessor, one string.
//! * [`MatcherTarget`] — `new`'s column selector (`pub(super)`); its
//!   variants inherit that visibility and are constructible wherever the
//!   enum is, which yields nothing renderable: a target names a column.
//! * [`PromqlRe2Fallback`] — the TYPE only (`pub(crate)`, so the
//!   escaper's pinned signature can name it); both ways of *making* one
//!   are private to this file.
//! * [`anchored_re2_literal_for_test`] — the `#[doc(hidden)]` seam, a
//!   plain-`pub` fn returning the anchored pattern literal.
//!
//! # What rustc enforces — and what it does not
//!
//! Measured, by compiling each bypass spelling from `sql.rs` in turn
//! (review round 2; the two token rows were also compiled from `logql`,
//! same diagnostics):
//!
//! | attempted spelling | rustc |
//! |---|---|
//! | `floored_bound(..)`, `anchored_re2_literal(..)`, `matcher_regex_literal(..)`, `re2_compile_probe(..)`, `predicate(..)` — unqualified | `E0425` cannot find function in this scope |
//! | `crate::metrics::series_where::floored_bound(..)` / `…::predicate(..)` — fully qualified | `E0603` private function |
//! | `SeriesWhere { tail: … }` (struct literal) | `E0451` private field |
//! | `w.tail` (field access) | `E0616` private field |
//! | `PromqlRe2Fallback::new()` — including under a `use … as` alias | `E0624` private associated function |
//! | `PromqlRe2Fallback(())` (tuple constructor) | `E0423` cannot initialize a tuple struct which contains private fields |
//!
//! So what rustc enforces is exactly this much: **in safe Rust**, outside
//! this file, no spelling constructs the #240 token, calls the escaper
//! (its signature demands that token), calls the private
//! bound/probe/predicate helpers, forges [`SeriesWhere`] or names its
//! `tail` field — the *sanctioned components* cannot be recombined into
//! "bound without probe".
//!
//! Safe Rust is the boundary that matters here, and it is the only one on
//! offer: privacy is not checked by `unsafe` conversions, so
//! `unsafe { std::mem::zeroed::<PromqlRe2Fallback>() }` builds this
//! unit-field token anywhere in the crate and then satisfies the escaper's
//! signature (review round 4, measured). A workspace-wide
//! `forbid(unsafe_code)` would delete that class outright but is not
//! available — the allocation-ceiling suites install `GlobalAlloc` impls
//! and `pulsus-config`'s test support mutates the environment, both
//! `unsafe` by definition. What remains has the literal seam's standing
//! below: a deliberate, self-announcing act that no ordinary refactor
//! performs and that review reads at a glance, as against the accidental
//! "sixth builder" this seal exists to make impossible.
//!
//! **The literal seam is NOT sealed** (review round 3). Nothing stops
//! production code, at compile time, from calling the plain-`pub`
//! [`anchored_re2_literal_for_test`] and splicing the returned pattern
//! literal into a hand-written `match(...)` conjunct — no token, no
//! probe — which is precisely the #315 hole. The compiler's guarantee
//! ends at the sanctioned components; the seam is covered only by review
//! (a `_for_test` call in a production path is the tell), until issue
//! #328's D1 extracts this screen into the `pulsus-re2` crate, where the
//! differential lives beside the code it tests and the export goes away.
//!
//! **Nothing but the renderer may be added to this file.** Every item
//! declared here can reach those privates and could therefore rebuild the
//! hole — this file is the seal's trust base, and rustc cannot police
//! additions *inside* it. A new builder belongs in `sql.rs`, where the
//! only fragments available to it are a whole `where_tail` and the seam's
//! bare literal.

use pulsus_model::floor_to_activity_bucket;

use crate::logql::escape::{ch_regex_anchored_promql_re2, ch_string};

use super::matcher::{DataWindow, LabelMatcher, MatchOp};

/// Capability token (issue #240). Possession proves the caller is on the
/// PromQL fallback path, where ClickHouse's RE2 — not the Rust `regex`
/// crate — is the regex authority (`labels.rs:496-506`, `:521-526`).
///
/// SEALING FORM IS LOAD-BEARING — do not "tidy" any line:
///  * the tuple field has NO visibility modifier, so the tuple constructor
///    is callable only inside this leaf module;
///  * `new` has NO visibility modifier, for the same reason;
///  * the TYPE is `pub(crate)` so `crate::logql::escape`'s pinned signature
///    can name it (via `super`'s `pub(crate) use` re-export) — naming it
///    confers nothing, every way of *making* one is private to this file.
///
/// The token lived in `metrics/mod.rs` until issue #315's review round 2,
/// with field and `new` private to `metrics` — which reads sealed but is
/// not: a private item is visible to the defining module's DESCENDANTS,
/// and `sql.rs` is one, so a builder there could construct the token and
/// reach the `pub(crate)` escaper under a `use … as` alias that no textual
/// census would catch (measured: it compiled). Defined HERE, in a leaf
/// with no children that is nobody's ancestor, the same mutant is a
/// compile error — `E0624` on `new`, `E0423` on the tuple constructor —
/// measured from both `sql.rs` and `logql`; the module-doc table carries
/// the full battery.
pub(crate) struct PromqlRe2Fallback(());

impl PromqlRe2Fallback {
    fn new() -> Self {
        PromqlRe2Fallback(())
    }
}

/// Which column a matcher set is evaluated against.
///
/// An enum rather than a boolean (workspace rule: no boolean parameters),
/// and matched exhaustively in [`predicate`], so a third target cannot be
/// added without the build failing here — the same "adding a variant
/// breaks the build" property the `MatchOp` arms already carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MatcherTarget {
    /// Ordinary label matchers, read out of the stored JSON blob.
    /// `JSONExtractString` returns `''` for a missing key, which is
    /// Prometheus's absent-label rule and matches `super::labels`'
    /// in-process `""` — load-bearing for the cache-vs-SQL differential.
    Labels,
    /// `__name__` matchers (issue #96's degraded-cache discovery probe),
    /// which address the **`metric_name` column** — the leading
    /// primary-key component of `metric_series`, never a stored label
    /// (docs/schemas.md §2.1). Only ever fed the non-`Eq` name matchers a
    /// regex/negated-`__name__` selector carries; the `Eq` arm is the
    /// concrete-name route and never reaches here, but is kept total
    /// rather than panicking on an unreachable input.
    MetricNameColumn,
}

/// The bucket-floored window bound **and** its matcher conjuncts, rendered
/// together and inseparable.
///
/// # The compile probe (issue #315)
///
/// ClickHouse compiles a `match()` pattern only when it evaluates that
/// `match()` on a row. A selector naming a metric with no stored rows in
/// the window therefore never reaches RE2 at all, so an RE2-rejected
/// pattern came back as an empty `200` where upstream Prometheus (the
/// metrics API's reference of record, issue #283) answers `400` — and
/// issue #309's screen cannot close it, because it *delegates* the verdict
/// to a storage engine that never runs. [`SeriesWhere::new`] therefore
/// renders one extra `match()` per regex matcher over a **constant**
/// subject, which ClickHouse folds during query analysis, before a single
/// part is read: a pattern RE2 refuses raises `Code: 427
/// CANNOT_COMPILE_REGEXP` there, and [`super::dispatch`] classifies it
/// into the same 400 the row predicate would have produced.
///
/// **Why the probe is spliced into the window bound rather than added as
/// its own `AND` conjunct.** Both forms fold, but a standalone constant
/// conjunct stops ClickHouse from *fully* moving the matcher predicate
/// into PREWHERE: the plan keeps a second `Filter` step that re-evaluates
/// `and(metric_name = …, match(JSONExtractString(…), …), 1)` on every row
/// that survived PREWHERE, i.e. it pays the JSON extraction and the regex
/// twice for every matched row (measured on 24.8.14.39 with
/// `EXPLAIN actions=1`; `AND 1` alone is enough to cause it). Folded into
/// the constant lower bound the probe leaves no trace in the plan — the
/// primary key condition still reads `unix_milli in [#, +Inf)` and the
/// whole WHERE still moves to PREWHERE — so it adds no per-row action and
/// no index engagement. That plan-shape claim is the one this repo gates
/// (`tests/explain_indexes.rs`), because it is scale-invariant; a paired
/// wall-clock A/B over 2M-80M rows agreed but could only bound the
/// difference, not resolve it (95% CIs straddling zero; the rejected
/// conjunct shape was the one that measured a detectable cost). Wall-time
/// numbers are recorded on issue #315, never asserted here.
///
/// A matcher set with no regex renders no probe at all, so `up{job="a"}`
/// is byte-identical to what this path emitted before #315. Probes are
/// emitted in matcher order and the analyzer folds the sum left to right,
/// so the FIRST invalid pattern is the one reported — matching upstream's
/// own order and issue #316's in-process `first_invalid_regex_detail`.
#[derive(Debug)]
pub(super) struct SeriesWhere {
    /// `unix_milli >= <floor><probe> AND unix_milli <= <floor>` followed by
    /// one `\n  AND <predicate>` per matcher. Private: the two halves are
    /// never handed out separately, which is what makes "a regex without
    /// its probe" unrepresentable rather than merely untested.
    tail: String,
}

impl SeriesWhere {
    /// Renders the window bound (bucket-floored on both edges, carrying the
    /// compile probe for `matchers`) and the matcher conjuncts, in the one
    /// order every builder uses.
    pub(super) fn new(
        window: DataWindow,
        bucket_ms: i64,
        matchers: &[LabelMatcher],
        target: MatcherTarget,
    ) -> Self {
        let lower = floored_bound(window.start_ms, bucket_ms);
        let upper = floored_bound(window.end_ms, bucket_ms);
        let probe = re2_compile_probe(matchers);
        let mut tail = format!("unix_milli >= {lower}{probe} AND unix_milli <= {upper}");
        for m in matchers {
            tail.push_str("\n  AND ");
            tail.push_str(&predicate(m, target));
        }
        SeriesWhere { tail }
    }

    /// The rendered `WHERE` tail — bound, probe and matchers, as one
    /// string. The only accessor, deliberately: a builder that could ask
    /// for the matchers alone would be able to rebuild the #315 hole.
    pub(super) fn where_tail(&self) -> &str {
        &self.tail
    }
}

/// A narrow, `#[doc(hidden)]` test seam, re-exported by `super` next to
/// `re2_authority`'s seam, which is its shape and precedent. Not a
/// `cfg(test)` or feature gate, for that precedent's reason: the consumer
/// is an external integration binary (`tests/re2_screen_differential.rs`)
/// that links the lib compiled *without* `cfg(test)`, and a cargo feature
/// would either drop that suite's hermetic half from the plain
/// `cargo test --workspace` lane or ship the seam in the CI-tested
/// configuration anyway. The differential must cross ClickHouse's RE2
/// over the **production** rendering, and nothing else can hand it that
/// rendering — the escaper's token is constructible only in this file.
///
/// This is the module's one UNSEALED boundary crossing (module doc,
/// "what rustc does not enforce"): plain `pub`, so production code can
/// call it and splice the returned literal into a hand-written
/// `match(...)` with no token and no probe. The export exists only
/// because the differential is an external binary; issue #328's D1 moves
/// this screen into the `pulsus-re2` crate, where the differential sits
/// beside the code it tests and this export is retired.
#[doc(hidden)]
pub fn anchored_re2_literal_for_test(pattern: &str) -> String {
    anchored_re2_literal(pattern)
}

/// `intDiv({ms}, {bucket_ms}) * {bucket_ms}` — the literal bound
/// docs/schemas.md §2.1 renders, computed via the shared
/// [`floor_to_activity_bucket`] (not re-derived here) so the rendered
/// number is byte-identical to what the writer's own registration gate
/// computes (issue #26 precedent; cross-crate pinned by
/// `tests/metrics_bucket_floor.rs`).
fn floored_bound(ms: i64, bucket_ms: i64) -> i64 {
    floor_to_activity_bucket(ms, bucket_ms)
}

/// The metrics path's **only** regex→SQL rendering (issue #240's PromQL
/// exemption, narrowed to one site by issue #315 so the row predicate and
/// the compile probe can never disagree about the pattern text they name).
/// Issue #324's `(?-s)` prefix lives inside the escaper, not here.
fn anchored_re2_literal(pattern: &str) -> String {
    ch_regex_anchored_promql_re2(PromqlRe2Fallback::new(), pattern)
}

/// `Some(literal)` for the two ops whose value is a regex, `None` for the
/// two whose value is a literal string and is never compiled.
fn matcher_regex_literal(m: &LabelMatcher) -> Option<String> {
    match m.op {
        MatchOp::Re | MatchOp::Nre => Some(anchored_re2_literal(&m.value)),
        MatchOp::Eq | MatchOp::Neq => None,
    }
}

/// The constant `match()` sum spliced into the lower bound, or empty when
/// no matcher carries a regex. See [`SeriesWhere`] for the reasoning.
fn re2_compile_probe(matchers: &[LabelMatcher]) -> String {
    let probes: Vec<String> = matchers
        .iter()
        .filter_map(matcher_regex_literal)
        .map(|literal| format!("match('', {literal})"))
        .collect();
    if probes.is_empty() {
        return String::new();
    }
    format!(" + 0 * ({})", probes.join(" + "))
}

/// One matcher, against whichever column `target` names. Label keys are
/// always string literals (see `super::sql`'s escaping table) — never
/// `ch_ident`, which is reserved for trusted schema identifiers.
fn predicate(m: &LabelMatcher, target: MatcherTarget) -> String {
    let column = match target {
        MatcherTarget::Labels => format!("JSONExtractString(labels, {})", ch_string(&m.key)),
        MatcherTarget::MetricNameColumn => "metric_name".to_string(),
    };
    match m.op {
        MatchOp::Eq => format!("{column} = {}", ch_string(&m.value)),
        MatchOp::Neq => format!("{column} != {}", ch_string(&m.value)),
        MatchOp::Re => format!("match({column}, {})", anchored_re2_literal(&m.value)),
        MatchOp::Nre => format!("NOT match({column}, {})", anchored_re2_literal(&m.value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window() -> DataWindow {
        DataWindow {
            start_ms: 1_000,
            end_ms: 3_600_001,
        }
    }

    fn m(op: MatchOp, key: &str, value: &str) -> LabelMatcher {
        LabelMatcher {
            key: key.to_string(),
            op,
            value: value.to_string(),
        }
    }

    fn tail(matchers: &[LabelMatcher], target: MatcherTarget) -> String {
        SeriesWhere::new(window(), 3_600_000, matchers, target)
            .where_tail()
            .to_string()
    }

    /// The module-doc boundary inventory, kept honest mechanically
    /// (review round 3: the hand-maintained list was wrong once — it
    /// said three items where six are visible). The production half of
    /// this file must contain exactly these visibility-carrying
    /// declarations, in this order; a new `pub` item, a widened
    /// visibility, or a changed signature fails here until both this pin
    /// and the module-doc inventory are updated.
    ///
    /// # What this establishes, and what it provably cannot
    ///
    /// **It establishes exactly one thing: the declarations written in
    /// this file match the module-doc inventory.** That is worth having —
    /// it is the drift this test was added for, and it caught the
    /// inventory listing three items when six were visible.
    ///
    /// **It does NOT establish that nothing else reaches `tail`, and no
    /// version of it can.** Reviews 5–8 each defeated a stricter textual
    /// form, and the last two did so with code whose text is not in this
    /// file at all:
    ///
    ///  * round 5 — a generic `impl<'a> IntoIterator for &'a SeriesWhere`
    ///    slipped an `impl `-with-a-space filter;
    ///  * round 6 — `#[rustfmt::skip]` on a declaration's own line is a
    ///    rustfmt fixpoint, so the declaration never starts a line;
    ///  * round 7 — a `macro_rules!` defined in a sibling and invoked at
    ///    item position here expanded to a leaking `impl`;
    ///  * round 8 — an indented `leak_method!()` inside the existing
    ///    `impl` block, and, in one word on an existing line,
    ///    `#[derive(Debug, serde::Serialize)]`, which was *executed* to
    ///    print `{"tail":"unix_milli >= 0 AND …"}` from a sibling module.
    ///
    /// The pattern is not that each guard was too loose. It is that the
    /// property wanted — *no declaration reaches `tail` except the listed
    /// ones* — is about expansion and reachability, and a check that reads
    /// this file's characters is downstream of neither. Escalating the
    /// textual form buys one spelling per round and keeps meeting the same
    /// wall, so the escalation stops here rather than at the next round.
    ///
    /// **What actually closes it** is making the field unreachable rather
    /// than merely unlisted, which is issue #328's D1 extraction into
    /// `pulsus-re2` — there the differential lives beside the code and the
    /// `_for_test` seam that motivates all of this goes away. Until then
    /// the compile-error table below is the real guarantee (rustc, not
    /// text), and this test is a drift alarm on the written inventory.
    ///
    /// The checks kept below are the cheap ones that demonstrably catch
    /// drift; they are not claimed to be exhaustive over anything.
    #[test]
    fn the_boundary_inventory_is_pinned() {
        let text = include_str!("series_where.rs");
        let production = &text[..text.find("mod tests {").unwrap()];
        // Item-position shape: top-level lines must be plain items or
        // attributes and carry no `!`, which keeps `macro_rules!`, an
        // item-position invocation, `include!` and `mod` out of the file.
        // Kept because it is cheap and does catch those; NOT relied on —
        // an indented invocation inside an existing `impl`, and a derive
        // added to an existing attribute, both pass it (round 8). See the
        // doc above for why no textual form closes the class.
        const ITEM_STARTS: [&str; 5] = ["use ", "pub ", "pub(", "impl ", "fn "];
        for line in production.lines() {
            if line.is_empty() || line.starts_with(' ') || line.starts_with("//") || line == "}" {
                continue;
            }
            let ok = line.starts_with("#[") || ITEM_STARTS.iter().any(|s| line.starts_with(s));
            assert!(
                ok && !line.contains('!'),
                "unexpected item-position line in this leaf: {line:?}\n\
                 Only plain `use`/`pub`/`impl`/`fn` items and `#[...]` \
                 attributes may appear at top level, and none may carry \
                 `!`. Text-importing forms (`macro_rules!`, a macro \
                 invocation, `include!`, `mod`) would put boundary-crossing \
                 code outside this file, where this test cannot see it."
            );
        }
        assert!(
            !production.contains("rustfmt::skip"),
            "`#[rustfmt::skip]` in the production half defeats this pin's \
             line-prefix matching; it is not permitted in this leaf"
        );
        let declared = |prefix: &'static str| -> Vec<&str> {
            production
                .lines()
                .map(str::trim_start)
                .filter(|line| line.starts_with(prefix))
                .collect()
        };
        assert_eq!(
            declared("pub"),
            [
                "pub(crate) struct PromqlRe2Fallback(());",
                "pub(super) enum MatcherTarget {",
                "pub(super) struct SeriesWhere {",
                "pub(super) fn new(",
                "pub(super) fn where_tail(&self) -> &str {",
                "pub fn anchored_re2_literal_for_test(pattern: &str) -> String {",
            ],
            "series_where.rs declarations drifted from this test's list"
        );
        // Round 9: the assertion above compares declarations against a list
        // typed HERE, so editing the module doc alone left it green — the
        // failure message named a comparison it did not make, and the
        // unguarded direction was exactly the round-3 doc drift this test
        // exists for. Read the doc's inventory bullets and compare for real.
        // Both anchors are asserted present. The start anchor already failed
        // closed (a missing section yields an empty list, which cannot equal
        // the array below), but round 10 found the END anchor unverified:
        // renaming it let the scan run to EOF. That could not produce a wrong
        // answer — the names still came back correct and in order — so this
        // is one line closing an asymmetry, not a demonstrated defect.
        const DOC_START: &str = "//! # Boundary inventory";
        const DOC_END: &str = "//! # What rustc enforces";
        // Matched as a LINE PREFIX, not with `contains`: these constants are
        // themselves lines of this file, so `text.contains(DOC_END)` is true
        // of the declaration above and can never fail — a check satisfied by
        // its own definition. The const lines are indented, so a prefix match
        // sees only the real doc headings.
        let anchors = |a: &str| text.lines().filter(|l| l.starts_with(a)).count();
        assert!(
            anchors(DOC_START) == 1 && anchors(DOC_END) == 1,
            "the module doc's inventory section anchors moved; this parse \
             reads between {DOC_START:?} and {DOC_END:?} and cannot bound \
             the section without exactly one of each"
        );
        let doc_names: Vec<&str> = text
            .lines()
            .skip_while(|l| !l.starts_with(DOC_START))
            .take_while(|l| !l.starts_with(DOC_END))
            .filter_map(|l| l.trim_start().strip_prefix("//! * [`"))
            .filter_map(|l| l.split("`]").next())
            .collect();
        assert_eq!(
            doc_names,
            [
                "SeriesWhere",
                "SeriesWhere::new",
                "SeriesWhere::where_tail",
                "MatcherTarget",
                "PromqlRe2Fallback",
                "anchored_re2_literal_for_test",
            ],
            "the module-doc boundary inventory drifted from this test's list"
        );
        let impl_headers: Vec<&str> = production
            .lines()
            .map(str::trim_start)
            .filter(|line| line.starts_with("impl") || line.starts_with("unsafe impl"))
            .collect();
        assert_eq!(
            impl_headers,
            ["impl PromqlRe2Fallback {", "impl SeriesWhere {"],
            "a new impl block can expose private state without declaring `pub`"
        );
    }

    #[test]
    fn floored_bound_matches_the_shared_model_definition() {
        assert_eq!(floored_bound(3_600_001, 3_600_000), 3_600_000);
        assert_eq!(
            floored_bound(3_600_001, 3_600_000),
            floor_to_activity_bucket(3_600_001, 3_600_000)
        );
    }

    /// What this test establishes, no more: for renderings produced by
    /// [`SeriesWhere::new`] — each `MatchOp` × each `MatcherTarget` with
    /// one matcher, at fixed representative values, plus the empty set —
    /// a `match(` predicate appears iff its compile probe does, and a
    /// regex-free set renders the pre-#315 bound byte-for-byte. It does
    /// NOT establish that `new` is the only source of a predicate — and
    /// neither does rustc in full: the module-doc compile-error table
    /// seals the sanctioned components, while the `_for_test` seam's
    /// literal can be hand-spliced into a new `match(...)` (module doc,
    /// "what rustc does not enforce"). Fixed
    /// values rather than a generator, deliberately: the probe/predicate
    /// pairing is decided by `MatchOp` alone — the rendering is a pure
    /// concatenation whose shape is value-independent — so the op × target
    /// arms are the whole input space for the pairing, and the injection
    /// tests in `super::sql` cover hostile values.
    #[test]
    fn a_rendered_regex_and_its_compile_probe_are_inseparable() {
        let regex_ops = [MatchOp::Re, MatchOp::Nre];
        let literal_ops = [MatchOp::Eq, MatchOp::Neq];
        for target in [MatcherTarget::Labels, MatcherTarget::MetricNameColumn] {
            for op in regex_ops {
                let rendered = tail(&[m(op, "job", "5..")], target);
                assert!(rendered.contains("match("), "{op:?}/{target:?}: {rendered}");
                assert!(
                    rendered.contains("+ 0 * (match('', '(?-s)^(?:5..)$'))"),
                    "{op:?}/{target:?} rendered a regex with no probe: {rendered}"
                );
            }
            for op in literal_ops {
                let rendered = tail(&[m(op, "job", "api")], target);
                assert!(
                    !rendered.contains("match("),
                    "{op:?}/{target:?}: {rendered}"
                );
                assert_eq!(
                    rendered.lines().next(),
                    Some("unix_milli >= 0 AND unix_milli <= 3600000"),
                    "{op:?}/{target:?} must render the pre-#315 bound verbatim"
                );
            }
        }
        // The empty set is the `discovery_fetch_multi` shape: bound only.
        assert_eq!(
            tail(&[], MatcherTarget::Labels),
            "unix_milli >= 0 AND unix_milli <= 3600000"
        );
    }

    /// Probes are emitted in matcher order (ClickHouse folds the sum left
    /// to right, so the first invalid pattern is the one reported) and
    /// literal-valued matchers contribute none.
    #[test]
    fn probes_follow_matcher_order_and_skip_literal_matchers() {
        let rendered = tail(
            &[
                m(MatchOp::Re, "status", "5.."),
                m(MatchOp::Eq, "job", "api"),
                m(MatchOp::Nre, "env", "dev"),
            ],
            MatcherTarget::Labels,
        );
        assert!(
            rendered.starts_with(
                "unix_milli >= 0 + 0 * (match('', '(?-s)^(?:5..)$') + \
                 match('', '(?-s)^(?:dev)$')) AND unix_milli <= 3600000"
            ),
            "{rendered}"
        );
    }

    /// The two targets address different columns; `__name__` matchers are
    /// never read out of the JSON blob (`__name__` is not a stored label,
    /// docs/schemas.md §2.1).
    #[test]
    fn the_matcher_target_selects_the_column() {
        let labels = tail(&[m(MatchOp::Re, "job", "api")], MatcherTarget::Labels);
        assert!(labels.contains("match(JSONExtractString(labels, 'job'), '(?-s)^(?:api)$')"));

        let names = tail(
            &[m(MatchOp::Re, "__name__", "up.*")],
            MatcherTarget::MetricNameColumn,
        );
        assert!(names.contains("match(metric_name, '(?-s)^(?:up.*)$')"));
        assert!(!names.contains("JSONExtractString"));
    }
}
