//! Issue #291: the bound on what compiling ONE user pattern may allocate,
//! and the single entry point every user-pattern compile in the workspace
//! goes through.
//!
//! # L1 — what this bounds, and why nothing else did
//!
//! `regex::RegexBuilder::size_limit` is `nfa_size_limit`
//! (`regex-1.13.0/src/builders.rs:184-187`; `dfa_size_limit` is
//! `hybrid_cache_capacity`, `:189-192`). It bounds the **compiled
//! program**, which is produced by the LAST of three phases. Measured on
//! this tree with a counting global allocator, peak live bytes per phase:
//!
//! | pattern | len | AST parse | HIR translate | NFA (10 MiB limit) |
//! |---|---|---|---|---|
//! | `a`×131071 | 131,071 | 9.4 MB | 0.26 MB | 16.4 MB |
//! | `\w`×64 | 128 | 4.8 KB | 425 KB | 4.16 MB |
//! | `\p{L}`×21845 | 109,225 | 3.0 MB | **124.8 MB** | 14.3 MB → Err |
//! | `(?i)\p{L}`×15170 | 91,024 | 2.6 MB | **794.9 MB** | 14.3 MB → Err |
//!
//! The HIR phase is bounded by nothing in the crate: its cost is (number
//! of class atoms) × (ranges the class expands to × 8 B), and no limit
//! touches either factor. Sweeping `size_limit` on a FIXED pattern proves
//! the point directly — `(?i)\p{L}`×170 (854 B) peaks 7.75 MB at
//! `size_limit(4 KiB)` and 28.7 MB at 10 MiB: cutting the limit 2,560×
//! cuts the peak 3.7×, and the pattern is refused the whole way.
//!
//! # L2 — the accounting unit
//!
//! **Bytes requested from the global allocator, peak live**, for one
//! `compile_user_regex` call, refusals included. Peak (not cumulative)
//! because the three phases overlap: the AST is alive while the HIR is
//! built, and the HIR is alive while the NFA is built, so the model sums
//! the phases rather than taking their maximum. Cumulative churn across
//! several compiles in one query is deliberately NOT bounded here — see
//! L5.
//!
//! # L3 — the per-phase bound
//!
//! ```text
//! bound(p) = HIR_BYTES_PER_NODE          * materialised_node_count
//!          + HIR_BYTES_PER_LITERAL_NODE  * collapsed_node_count
//!          + HIR_BYTES_PER_CLASS_RANGE   * total_class_ranges
//!            * (CASE_FOLD_TRANSIENT_FACTOR if the pattern sets `i`)
//!            * (CLASS_NEGATION_TRANSIENT_FACTOR per COMPLEMENTED leaf)
//!          + max(AST_BYTES_PER_PATTERN_BYTE * p.len(),
//!                NFA_PEAK_FACTOR            * size_limit)
//! ```
//!
//! * **AST** — O(pattern length) by construction.
//!   [`AST_BYTES_PER_PATTERN_BYTE`] is the charge.
//! * **HIR** — the one unbounded phase, and the only term that is
//!   *computed* rather than charged: `total_class_ranges` is produced by
//!   [`regex_syntax`] itself, translating each class atom ALONE and
//!   asking the result how many ranges it has. It is not a per-byte
//!   constant fitted to a measurement — that error is what the issue
//!   exists to avoid. The two node charges cover the fixed per-node part
//!   the range count does not see.
//! * **NFA** — already bounded by `size_limit`; [`NFA_PEAK_FACTOR`] is
//!   the observed worst ratio of peak to limit, rounded up.
//!
//! **Why `max` and not `+` for the AST/NFA pair.** The AST and the HIR
//! DO co-exist (the translator reads the AST), so those terms add. The
//! AST and the NFA do not: `regex-automata`'s builder drops the AST when
//! `syntax::parse` returns, before the NFA is compiled. That is
//! measurable rather than asserted — `a`×131071 parses to a 9.44 MB AST
//! and peaks at 29.36 MB with a 0.26 MB HIR live, which is the maximum
//! of the two phases and not their sum (the sum would be 38.5 MB).
//! `the_estimate_upper_bounds_the_measured_peak` is what fails if that
//! stops being true.
//!
//! # L4 — what makes the constants breakable rather than asserted
//!
//! [`regex_compile_transient_bound`] is claimed to be an UPPER bound on
//! the measured peak. That claim is a test, not a comment:
//! `pulsus-re2/tests/regex_compile_budget.rs`'s
//! `the_estimate_upper_bounds_the_measured_peak` measures the real peak
//! of every corpus row and fails if any constant is too small. Shrinking
//! any one of the three reddens it.
//!
//! The same test is the tripwire for **`regex-syntax` version skew**: the
//! estimator must parse with the same version `regex` compiles with (both
//! come from `[workspace.dependencies]`, locked together). A future
//! `regex` bump that moves to a different `regex-syntax` minor silently
//! stops modelling the compiler, and that test is what notices.
//!
//! # L5 — what is deliberately NOT charged
//!
//! * **Cumulative allocation across several compiles in one query.** A
//!   query carrying 1,000 small line filters allocates 5.24 GB over 5.9 s
//!   with a 3.6 MB peak; no per-compile cap can see that. It needs a
//!   query-scoped accumulator threaded through every compile site, and
//!   **#291 stays open to carry it** after this cap lands.
//! * **The `regex` crate's own program memory after `build()` returns.**
//!   That is `size_limit`'s job and it does it.
//! * **Anything RE2 would do differently.** This models the Rust crate,
//!   which is what allocates.
//!
//! # The divergence this creates
//!
//! The cap narrows our accept surface below the reference's, and the
//! residue is small enough to state exactly. Both boundaries bisected —
//! ours over the estimator, the reference's on the pinned v3.7.4
//! container, one atom at a time — for the `\p{L}|…` alternation family:
//! we serve up to **10,013** atoms, the reference up to **12,728**
//! (12,729 is its own `error parsing regexp: expression too large`). The
//! band where it serves and we refuse is therefore **10,014..12,728**,
//! 2,715 atoms out of the 12,728 it accepts.
//!
//! That is deliberate, and the reason is not "we
//! chose 96 MiB": matching the reference's boundary means porting Go's
//! `maxRunes`/`maxSize`/`maxHeight`
//! (`vendor/github.com/grafana/regexp/syntax/parse.go:93,102-103,122-123
//! @ v3.7.4`), and those admit **128 MB parse trees** — the exact
//! unboundedness this issue exists to close. Ledgered as
//! `regex-compile-budget` in `docs/benchmarks/logs-differential-ledger.md`
//! and documented in `docs/api.md` §9.4.

use std::collections::HashMap;

use regex_syntax::ast::{
    Ast, ClassBracketed, ClassSet, ClassSetItem, Flag, Flags, FlagsItemKind, GroupKind, Span,
};
use regex_syntax::hir::translate::TranslatorBuilder;
use regex_syntax::hir::{Class, HirKind};

/// Upper bound on bytes REQUESTED FROM THE GLOBAL ALLOCATOR while
/// compiling one user pattern.
///
/// Standalone by design: it is NOT derived from
/// `MAX_TEMPLATE_RENDER_BYTES` — issue #230 severed exactly that kind of
/// link once, and coupling two unrelated guards through one constant is
/// how a guard changes silently.
///
/// Sizing: the largest ACCEPTED pattern measured through the real chain
/// costs 23.6 MB peak (`{app="x"} |~ \p{L}×200`, 1,015 query bytes,
/// served by the reference in 49 ms), so the cap carries 2.7× headroom
/// over anything a real query does.
pub const MAX_REGEX_COMPILE_TRANSIENT_BYTES: u64 = 96 * 1024 * 1024;

/// The `regex` crate default (10 MiB, `regex-1.13.0/src/builders.rs:53`),
/// made explicit so no site silently inherits a different accept surface
/// than another. Every entry point below that does not take a limit uses
/// this one, which is what `regex::Regex::new` already used at all nine
/// call sites before this issue — the accept surface does not move.
pub const REGEX_PROGRAM_SIZE_LIMIT: usize = 10 * 1024 * 1024;

/// Charge for the AST phase, per byte of pattern. The parser allocates
/// per token and a token is at least one byte, so this phase is
/// O(pattern length) by construction. Measured worst ASYMPTOTIC ratio at
/// 131 KB over 26 shapes: **160 B/byte** (`[abab…]`, one giant bracketed
/// union). Deep nesting reaches 249 B/byte but only on a short pattern
/// (`(`×200 + `a` + `)`×200 = 401 B → 99,640 B), because the parser's own
/// nest limit bounds that shape's ABSOLUTE cost; it is covered by the NFA
/// floor below rather than by this ratio.
const AST_BYTES_PER_PATTERN_BYTE: u64 = 320;

/// Charge for one AST node the translator materialises an `Hir` for.
///
/// Measured worst over 21 node-dominated families: **356.5 B/node**, on
/// `(a)`, `a{2}` and `(a)(b)` — a capture and a counted repetition are
/// the expensive nodes. A bare `.` is 259.5 and `(?:a)` is 179.4.
///
/// **The 432 B/node this comment used to cite was a per-ATOM figure
/// mis-read as per-node** (`.*`×65535 produces 432 B per atom, but each
/// atom is a `Dot` AND a `Repetition` — 216 B/node). The value did not
/// change; its derivation was wrong, and review found the consequence:
/// nothing in the suite reddened when it was halved to 224.
/// `each_hir_charge_dominates_the_phase_cost_it_models` now pins it —
/// red at 331, so the shipped value carries 1.35× over the threshold and
/// 1.26× over the worst measurement. That margin is deliberate: an
/// exact-fit charge against an allocator measurement is width-dependent,
/// which this repo's alloc-bound rule forbids.
const HIR_BYTES_PER_NODE: u64 = 448;

/// Charge for an AST node the translator **collapses** rather than
/// materialising: a literal character, and a literal or range item inside
/// a bracketed class. `a`×131071 is 131,072 AST nodes and a 0.26 MB HIR —
/// **2 B/node** measured — because `regex-syntax` folds a concatenation of
/// literals into one `Hir::literal`. Charging those at
/// [`HIR_BYTES_PER_NODE`] would price a 130 KB literal at 58 MB and refuse
/// a pattern the reference serves in 34 ms; charging the dots at THIS rate
/// would under-bound the HIR by 20×. One constant cannot cover both, which
/// is why there are two.
const HIR_BYTES_PER_LITERAL_NODE: u64 = 24;

/// One `ClassUnicodeRange` is two `char`s = 8 bytes. Charged once per
/// range per class atom, which is the term that grows without bound and
/// the only one COMPUTED (by `regex-syntax` itself) rather than charged.
const HIR_BYTES_PER_CLASS_RANGE: u64 = 8;

/// Multiplier on the range term when the pattern turns `i` on ANYWHERE.
///
/// Case folding does not merely produce a bigger class — it produces a
/// bigger class EXPENSIVELY, and the final range count does not show it.
/// `regex-syntax`'s `SimpleCaseFolder` walks the class range by range and
/// rebuilds the set as it goes, so the transient cost is a multiple of
/// the result. Measured (HIR-phase peak ÷ 8 × final range count):
///
/// | pattern | ratio |
/// |---|---|
/// | `\p{L}`×200 | 1.05 |
/// | `\w`×64 | 1.04 |
/// | `\p{L}`×20000 | 1.06 |
/// | `(?i)\p{L}`×170 | **8.05** |
/// | `(?i)\p{L}`×20000 | **8.05** |
///
/// Rounded up to 10. Found the only way it could be — by checking the
/// estimate at the LogQL template site's 1 MiB program ceiling as well as
/// the 10 MiB default. At 10 MiB the NFA floor is 31.5 MB and hides an
/// eightfold under-charge of the HIR; at 1 MiB the floor is 3.1 MB and
/// `(?i)\p{L}`×170 allocates 9.69 MB, which no other row exposed. A
/// bound that holds only at one caller's limit is not a bound, so
/// `the_estimate_upper_bounds_the_measured_peak` now runs at both.
const CASE_FOLD_TRANSIENT_FACTOR: u64 = 10;

/// Multiplier on the range term for a NEGATED class — `\P{L}`, `\W`,
/// `[^…]`, and any leaf inside a negated bracket.
///
/// Same shape of error as case folding, and found the same way: the
/// COMPLEMENT of `\p{L}` has about as many ranges as `\p{L}` does, so the
/// final count says negation is free. Computing it is not. Measured
/// (HIR-phase peak ÷ 8 × probe range count, 10,000 atoms each):
///
/// | atom | ratio |
/// |---|---|
/// | `\p{L}` | 1.06 |
/// | `\w` | 1.05 |
/// | `[^\p{L}]` | **4.06** |
/// | `\P{L}` | **4.05** |
/// | `[^\w]` | **4.05** |
/// | `\W` | **4.04** |
/// | `[^\p{Nd}]` | **4.56** |
///
/// Rounded up to 5 — red at 3 in
/// `each_hir_charge_dominates_the_phase_cost_it_models`, so 1.67× over
/// the threshold and 1.14× over the worst measurement — and charged PER
/// LEAF rather than per pattern: a
/// whole-pattern rule (the one `casei` uses, where a single `(?i)`
/// anywhere is conservative and cheap) would quintuple the range term for
/// every class in a pattern containing one `[^a]`, which is a large
/// accept-surface cost for no safety.
///
/// **This was found by
/// [`the_bound_holds_on_shapes_the_model_was_not_derived_from`] on its
/// first committed run**, when raising the cap to 96 MiB (owner ruling v2)
/// admitted `[^\p{L}]`×10000 — refused at 64 MiB, so never previously
/// measured against its estimate. It allocated 223.10 MB against an
/// 88.72 MB bound. That is what the cross-check is for.
const CLASS_NEGATION_TRANSIENT_FACTOR: u64 = 5;

/// Ratio of observed NFA-phase peak to the configured `size_limit`.
///
/// Measured worst over the cross-check corpus at a 10 MiB limit, taking
/// the whole-compile peak of shapes whose AST and HIR are small enough
/// not to dominate it: **4.08×** (`[^a-z]`×10000, 42.76 MB) and 4.48×
/// (`[^ab]`×20000, 47.01 MB) — long concatenations of small classes,
/// where the builder's unminimised intermediate is several times the
/// program it compacts to. `a`×131071 is 3.10×.
///
/// Rounded up to 4, and 4 rather than 5 because the node and range terms
/// are ADDED to this floor rather than being alternatives to it: the two
/// shapes above estimate 47.0 MB and 51 MB against measured 42.8 MB and
/// 47.0 MB. `the_bound_holds_on_shapes_the_model_was_not_derived_from`
/// is what fails if that stops being true — it is how 3 was found to be
/// short, on `[^a-z]`×10000 at 41.54 MB against a 36.58 MB bound.
///
/// This term costs the most accept surface of any in the model: at a
/// 10 MiB program limit it is a 41.9 MB floor under every estimate, 42%
/// of the cap.
const NFA_PEAK_FACTOR: u64 = 4;

/// Why a user pattern did not compile.
#[derive(Debug)]
pub enum RegexCompileError {
    /// The `regex` crate's own verdict, unchanged — every existing error
    /// message and accept/reject decision is preserved.
    Engine(regex::Error),
    /// Refused BEFORE the HIR translation that would have cost the
    /// memory. `estimate` and `cap` are bytes.
    TooLarge { estimate: u64, cap: u64 },
}

impl std::fmt::Display for RegexCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegexCompileError::Engine(e) => write!(f, "{e}"),
            // The reference's own wording class for this refusal
            // (`ErrLarge`, `vendor/github.com/grafana/regexp/syntax/
            // parse.go:47 @ v3.7.4`), which is what a Loki user sees at
            // the analogous boundary.
            RegexCompileError::TooLarge { estimate, cap } => write!(
                f,
                "expression too large: compiling it needs up to {estimate} bytes, limit {cap}"
            ),
        }
    }
}

impl std::error::Error for RegexCompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RegexCompileError::Engine(e) => Some(e),
            RegexCompileError::TooLarge { .. } => None,
        }
    }
}

/// The bound, in bytes, for compiling `pattern` EXACTLY as written
/// (callers that anchor must pass the anchored string), at the default
/// [`REGEX_PROGRAM_SIZE_LIMIT`].
///
/// `None` ⇒ the AST did not parse: no estimate is possible and none is
/// needed, the caller falls through to the engine for the canonical
/// error.
///
/// Never returns a value below the true cost of the phases it models;
/// `the_estimate_upper_bounds_the_measured_peak` is what makes that
/// breakable rather than asserted.
pub fn regex_compile_transient_bound(pattern: &str) -> Option<u64> {
    regex_compile_transient_bound_with(pattern, REGEX_PROGRAM_SIZE_LIMIT)
}

/// [`regex_compile_transient_bound`] for a site that compiles with a
/// non-default `size_limit` (the LogQL template's dynamic-pattern
/// ceiling is the only one).
pub fn regex_compile_transient_bound_with(pattern: &str, size_limit: usize) -> Option<u64> {
    let ast = regex_syntax::ast::parse::ParserBuilder::new()
        .build()
        .parse(pattern)
        .ok()?;
    Some(Estimator::new(pattern, &ast, size_limit).run(&ast))
}

/// Compiles a user-supplied pattern at the default program size limit,
/// refusing it first if compiling it could allocate more than
/// [`MAX_REGEX_COMPILE_TRANSIENT_BYTES`].
pub fn compile_user_regex(pattern: &str) -> Result<regex::Regex, RegexCompileError> {
    compile_user_regex_with(pattern, REGEX_PROGRAM_SIZE_LIMIT)
}

/// `^(?:{pattern})$` — estimates the ANCHORED string, which is what gets
/// compiled.
pub fn compile_user_regex_anchored(pattern: &str) -> Result<regex::Regex, RegexCompileError> {
    compile_user_regex(&format!("^(?:{pattern})$"))
}

/// [`compile_user_regex`] for a site that carries its own program size
/// limit. The limit feeds BOTH the compile and the estimate, so a site
/// with a tighter program ceiling is charged the smaller NFA term rather
/// than the default one.
pub fn compile_user_regex_with(
    pattern: &str,
    size_limit: usize,
) -> Result<regex::Regex, RegexCompileError> {
    if let Some(estimate) = regex_compile_transient_bound_with(pattern, size_limit)
        && estimate > MAX_REGEX_COMPILE_TRANSIENT_BYTES
    {
        return Err(RegexCompileError::TooLarge {
            estimate,
            cap: MAX_REGEX_COMPILE_TRANSIENT_BYTES,
        });
    }
    regex::RegexBuilder::new(pattern)
        .size_limit(size_limit)
        .build()
        .map_err(RegexCompileError::Engine)
}

// ---------------------------------------------------------------------
// The estimator
// ---------------------------------------------------------------------

/// Walks the AST iteratively (no recursion — the repo convention since
/// #272/#293, and the reason a 131 KB pattern cannot blow the stack
/// here), summing the per-node charge and the TRUE range count of every
/// class atom, and stops the moment the running total passes the cap.
struct Estimator<'p> {
    pattern: &'p str,
    /// Case folding only ever GROWS a class, so assuming `(?i)`
    /// everywhere once any flag group sets it is the conservative
    /// direction. `(?-u)` only shrinks and is ignored for the same
    /// reason.
    casei: bool,
    total: u64,
    /// Leaf costs memoised on the leaf's source slice — a distinct-name
    /// probe costs 1–30 µs measured (`\p{L}` 20 µs, `(?i)\p{L}` 363 µs,
    /// `\d` 2 µs) and real patterns repeat a handful of names.
    memo: HashMap<&'p str, u64>,
}

impl<'p> Estimator<'p> {
    fn new(pattern: &'p str, ast: &Ast, size_limit: usize) -> Self {
        Estimator {
            pattern,
            casei: ast_sets_case_insensitive(ast),
            // The AST and the NFA never co-exist (module doc L3), so
            // this is the larger of the two, not their sum.
            total: AST_BYTES_PER_PATTERN_BYTE
                .saturating_mul(pattern.len() as u64)
                .max(NFA_PEAK_FACTOR.saturating_mul(size_limit as u64)),
            memo: HashMap::new(),
        }
    }

    /// Bytes charged per class range: the base cost, times the case-fold
    /// factor when the pattern sets `i` anywhere, times the negation
    /// factor when THIS leaf is complemented.
    fn per_range(&self, negated: bool) -> u64 {
        let mut per = HIR_BYTES_PER_CLASS_RANGE;
        if self.casei {
            per *= CASE_FOLD_TRANSIENT_FACTOR;
        }
        if negated {
            per *= CLASS_NEGATION_TRANSIENT_FACTOR;
        }
        per
    }

    fn over_cap(&self) -> bool {
        self.total > MAX_REGEX_COMPILE_TRANSIENT_BYTES
    }

    fn charge(&mut self, bytes: u64) {
        self.total = self.total.saturating_add(bytes);
    }

    fn run(mut self, ast: &Ast) -> u64 {
        let mut stack: Vec<&Ast> = vec![ast];
        while let Some(node) = stack.pop() {
            // A literal character is COLLAPSED into the enclosing
            // `Hir::literal` rather than materialised (module doc L3).
            self.charge(match node {
                Ast::Literal(_) => HIR_BYTES_PER_LITERAL_NODE,
                _ => HIR_BYTES_PER_NODE,
            });
            if self.over_cap() {
                return self.total;
            }
            match node {
                Ast::ClassPerl(_) | Ast::ClassUnicode(_) | Ast::Dot(_) => {
                    let negated = match node {
                        Ast::ClassPerl(p) => p.negated,
                        Ast::ClassUnicode(u) => u.negated,
                        _ => false,
                    };
                    let ranges = self.leaf_ranges(node, node.span());
                    self.charge(self.per_range(negated).saturating_mul(ranges));
                }
                Ast::ClassBracketed(b) => {
                    if self.walk_class_set(&b.kind, b.negated) {
                        return self.total;
                    }
                }
                Ast::Repetition(r) => stack.push(&r.ast),
                Ast::Group(g) => stack.push(&g.ast),
                Ast::Alternation(a) => stack.extend(a.asts.iter()),
                Ast::Concat(c) => stack.extend(c.asts.iter()),
                Ast::Empty(_) | Ast::Flags(_) | Ast::Literal(_) | Ast::Assertion(_) => {}
            }
        }
        self.total
    }

    /// Sums the LEAF items of a bracketed class's set tree, never the
    /// translated result of the whole bracket. Measured trap:
    /// `[\p{L}&&\p{Nd}]` translates to an EMPTY class, so a
    /// whole-bracket rule under-counts by 748 ranges per atom while the
    /// compile still pays for both operands (8,000 such atoms = 120 KB
    /// of pattern, 122 MB cumulative).
    ///
    /// Returns `true` when the cap was passed and the walk must stop.
    fn walk_class_set(&mut self, set: &ClassSet, negated: bool) -> bool {
        // The enclosing bracket's negation reaches every leaf under it:
        // `[^\p{L}\p{Nd}]` complements the UNION, so both operands are
        // built and then complemented.
        let mut stack: Vec<(&ClassSet, bool)> = vec![(set, negated)];
        let mut items: Vec<(&ClassSetItem, bool)> = Vec::new();
        while let Some((node, neg)) = stack.pop() {
            match node {
                ClassSet::BinaryOp(op) => {
                    stack.push((&op.lhs, neg));
                    stack.push((&op.rhs, neg));
                }
                ClassSet::Item(item) => items.push((item, neg)),
            }
            while let Some((item, neg)) = items.pop() {
                // A literal or a range inside a bracket is folded into
                // the class the bracket already pays ranges for; only a
                // named/POSIX/Perl class or a nested set is materialised
                // separately.
                self.charge(match item {
                    ClassSetItem::Literal(_) | ClassSetItem::Range(_) | ClassSetItem::Empty(_) => {
                        HIR_BYTES_PER_LITERAL_NODE
                    }
                    _ => HIR_BYTES_PER_NODE,
                });
                if self.over_cap() {
                    return true;
                }
                match item {
                    ClassSetItem::Union(u) => items.extend(u.items.iter().map(|i| (i, neg))),
                    ClassSetItem::Bracketed(b) => stack.push((&b.kind, neg || b.negated)),
                    ClassSetItem::Empty(_) => {}
                    ClassSetItem::Literal(_)
                    | ClassSetItem::Range(_)
                    | ClassSetItem::Ascii(_)
                    | ClassSetItem::Unicode(_)
                    | ClassSetItem::Perl(_) => {
                        let span = *item.span();
                        let Some(probe) = class_item_probe(item) else {
                            continue;
                        };
                        let leaf_neg = neg
                            || match item {
                                ClassSetItem::Unicode(u) => u.negated,
                                ClassSetItem::Perl(p) => p.negated,
                                ClassSetItem::Ascii(a) => a.negated,
                                _ => false,
                            };
                        let ranges = self.leaf_ranges(&probe, &span);
                        self.charge(self.per_range(leaf_neg).saturating_mul(ranges));
                        if self.over_cap() {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    /// The range count of ONE class atom, produced by the same library
    /// that will do the real work rather than by a per-byte constant.
    ///
    /// A leaf whose standalone translation fails is charged nothing: the
    /// real translation fails at or before that atom too, so it never
    /// pays for what follows either — and charging the rest anyway keeps
    /// the sum an upper bound on the prefix it did pay for.
    fn leaf_ranges(&mut self, probe: &Ast, span: &Span) -> u64 {
        let key = &self.pattern[span.start.offset..span.end.offset];
        if let Some(n) = self.memo.get(key) {
            return *n;
        }
        let n = TranslatorBuilder::new()
            .case_insensitive(self.casei)
            .build()
            .translate(self.pattern, probe)
            .ok()
            .map_or(0, |hir| match hir.kind() {
                HirKind::Class(Class::Unicode(c)) => c.ranges().len() as u64,
                HirKind::Class(Class::Bytes(c)) => c.ranges().len() as u64,
                _ => 0,
            });
        self.memo.insert(key, n);
        n
    }
}

/// A standalone `Ast` that translates to exactly the class one bracketed
/// leaf contributes. POSIX classes, literals and ranges are only legal
/// INSIDE a bracket, so they are re-wrapped in a single-item one; the
/// wrapper's span is the leaf's own, which keeps the memo key and the
/// error spans pointing at the right slice of the pattern.
fn class_item_probe(item: &ClassSetItem) -> Option<Ast> {
    let span = *item.span();
    Some(match item {
        ClassSetItem::Unicode(u) => Ast::ClassUnicode(Box::new(u.clone())),
        ClassSetItem::Perl(p) => Ast::ClassPerl(Box::new(p.clone())),
        ClassSetItem::Literal(_) | ClassSetItem::Range(_) | ClassSetItem::Ascii(_) => {
            Ast::ClassBracketed(Box::new(ClassBracketed {
                span,
                negated: false,
                kind: ClassSet::Item(item.clone()),
            }))
        }
        ClassSetItem::Empty(_) | ClassSetItem::Union(_) | ClassSetItem::Bracketed(_) => {
            return None;
        }
    })
}

/// `true` when ANY flag group in the pattern turns `i` on — a whole-AST
/// question, deliberately not scoped to the group it appears in.
fn ast_sets_case_insensitive(ast: &Ast) -> bool {
    let mut stack: Vec<&Ast> = vec![ast];
    while let Some(node) = stack.pop() {
        match node {
            Ast::Flags(f) => {
                if flags_set_case_insensitive(&f.flags) {
                    return true;
                }
            }
            Ast::Group(g) => {
                if let GroupKind::NonCapturing(flags) = &g.kind
                    && flags_set_case_insensitive(flags)
                {
                    return true;
                }
                stack.push(&g.ast);
            }
            Ast::Repetition(r) => stack.push(&r.ast),
            Ast::Alternation(a) => stack.extend(a.asts.iter()),
            Ast::Concat(c) => stack.extend(c.asts.iter()),
            _ => {}
        }
    }
    false
}

/// `i` MENTIONED, not `i` enabled: `(?-i)` also answers `true`. Case
/// folding only grows a class, so a false positive costs a larger
/// estimate and never a wrong one, while tracking the flag's real scope
/// would mean re-implementing the translator's flag stack.
fn flags_set_case_insensitive(flags: &Flags) -> bool {
    flags
        .items
        .iter()
        .any(|item| matches!(item.kind, FlagsItemKind::Flag(Flag::CaseInsensitive)))
}

// ---------------------------------------------------------------------
// Test seams (issue #291 review finding 2)
// ---------------------------------------------------------------------

/// The range count the estimator would charge for one class atom. Exposed
/// so `regex_compile_budget.rs` can pin each HIR charge against the phase
/// it models using the estimator's OWN probe rather than a replica of it
/// — a replica would drift, and a drifted replica would make the pin
/// agree with itself instead of with the code.
#[doc(hidden)]
pub fn class_ranges_for_test(atom: &str, casei: bool) -> u64 {
    let Ok(ast) = regex_syntax::ast::parse::ParserBuilder::new()
        .build()
        .parse(atom)
    else {
        return 0;
    };
    let mut est = Estimator::new(atom, &ast, REGEX_PROGRAM_SIZE_LIMIT);
    est.casei = casei;
    est.leaf_ranges(&ast, ast.span())
}

/// The model's per-atom HIR charge, built from the same constants and the
/// same `per_range` the walk uses.
#[doc(hidden)]
pub fn per_atom_hir_charge_for_test(
    nodes: u64,
    lits: u64,
    ranges: u64,
    negated: bool,
    casei: bool,
) -> u64 {
    let mut est = Estimator::new(
        "",
        &Ast::empty(regex_syntax::ast::Span::splat(
            regex_syntax::ast::Position {
                offset: 0,
                line: 1,
                column: 1,
            },
        )),
        REGEX_PROGRAM_SIZE_LIMIT,
    );
    est.casei = casei;
    nodes * HIR_BYTES_PER_NODE + lits * HIR_BYTES_PER_LITERAL_NODE + est.per_range(negated) * ranges
}
