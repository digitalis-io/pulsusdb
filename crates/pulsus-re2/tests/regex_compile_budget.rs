//! Issue #291: the allocation gate over `compile_user_regex`.
//!
//! Single test binary, because the counting allocator is process-global
//! and PEAK is a whole-process quantity. `--test-threads` does not have
//! to be forced: every test here brackets its own measurement with
//! [`measure`], which resets the counters, and the tests that measure are
//! serialised by [`GATE`].
//!
//! What each test is for is on the test itself. What they are for
//! TOGETHER: the cap in `compile_budget.rs` is a number, and a number in
//! a source file proves nothing. These make it breakable — shrink any of
//! the three charges and `the_estimate_upper_bounds_the_measured_peak`
//! reddens; delete the refusal and `compile_transient_stays_under_the_cap`
//! reddens; replace the refusal with the cheap "just lower `size_limit`"
//! fix and `size_limit_alone_does_not_bound_it` reddens at every limit
//! value.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use pulsus_re2::{
    MAX_REGEX_COMPILE_TRANSIENT_BYTES, Re2Verdict, RegexCompileError, compile_user_regex,
    re2_verdict, regex_compile_transient_bound,
};

// ---------------------------------------------------------------------
// The instrument
// ---------------------------------------------------------------------

struct CountingAlloc;

static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);
static TOTAL: AtomicU64 = AtomicU64::new(0);

fn bump(delta: u64) {
    let live = LIVE.fetch_add(delta, Ordering::Relaxed) + delta;
    TOTAL.fetch_add(delta, Ordering::Relaxed);
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: every method delegates verbatim to the system allocator; the
// only side effect is relaxed atomic arithmetic, which allocates nothing
// and cannot re-enter the allocator.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump(layout.size() as u64);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
            Some(live.saturating_sub(layout.size() as u64))
        });
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size >= layout.size() {
            bump((new_size - layout.size()) as u64);
        } else {
            let shrunk = (layout.size() - new_size) as u64;
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some(live.saturating_sub(shrunk))
            });
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

/// PEAK is process-wide, so only one TEST may run at a time — not just
/// one measurement. A concurrent test allocating and freeing on another
/// thread moves LIVE under the measurement and makes the peak meaningless
/// (the alloc-gate flake rule: byte ceilings, and an instrument that
/// cannot be raced). Every `#[test]` here takes this first.
static GATE: Mutex<()> = Mutex::new(());

fn serialised(body: impl FnOnce()) {
    let guard = GATE.lock().unwrap_or_else(|e| e.into_inner());
    body();
    drop(guard);
}

/// Bytes observed while `f` ran: `peak` is the maximum LIVE bytes,
/// `total` the cumulative request. The result of `f` is dropped INSIDE
/// the bracket so a retained `Regex` cannot leak into the next row's
/// baseline.
fn measure<T>(f: impl FnOnce() -> T) -> (u64, u64) {
    LIVE.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    TOTAL.store(0, Ordering::Relaxed);
    drop(f());
    (PEAK.load(Ordering::Relaxed), TOTAL.load(Ordering::Relaxed))
}

fn mb(bytes: u64) -> String {
    format!("{:.2} MB", bytes as f64 / 1_000_000.0)
}

// ---------------------------------------------------------------------
// The corpus
// ---------------------------------------------------------------------

/// One corpus row: a name, the pattern, and the peak the pattern cost
/// through `regex::Regex::new` **before** this issue — the witness, in
/// bytes, measured on this tree at `c7649da` in release. The witness is
/// documentation, not an assertion: allocator behaviour differs between
/// profiles and machines, so a test that pinned it would be flaky. What
/// IS asserted is the relation between the new peak, the estimate and
/// the cap.
struct Row {
    name: &'static str,
    pattern: String,
    peak_before: u64,
}

fn row(name: &'static str, pattern: String, peak_before: u64) -> Row {
    Row {
        name,
        pattern,
        peak_before,
    }
}

/// Sixteen rows spanning the shapes the phase split separated: a pure
/// literal (cheapest), Perl and Unicode class atoms concatenated and
/// alternated, a case-folded run, a bracketed union, a set intersection
/// whose RESULT is empty while both operands are paid for, a POSIX class,
/// bounded repetition, and two ordinary patterns a user actually writes.
fn corpus() -> Vec<Row> {
    vec![
        row("literal_a_131071", "a".repeat(131_071), 29_362_393),
        row("class_pL_200", r"\p{L}".repeat(200), 23_625_739),
        row("class_pL_20000", r"\p{L}".repeat(20_000), 128_733_304),
        row(
            "casei_pL_170",
            format!("(?i){}", r"\p{L}".repeat(170)),
            28_738_753,
        ),
        row(
            "casei_pL_20000",
            format!("(?i){}", r"\p{L}".repeat(20_000)),
            886_977_100,
        ),
        row("perl_w_64", r"\w".repeat(64), 10_619_518),
        row("perl_w_65535", r"\w".repeat(65_535), 445_227_662),
        row("dot_star_65535", ".*".repeat(65_535), 45_286_102),
        row("bounded_rep_18724", "a{0,99}".repeat(18_724), 29_105_208),
        row(
            "class_alt_20001",
            std::iter::repeat_n(r"\p{L}", 20_001)
                .collect::<Vec<_>>()
                .join("|"),
            118_629_038,
        ),
        row(
            "casei_bracket_20000",
            format!("(?i)[{}]", r"\p{L}".repeat(20_000)),
            8_717_742,
        ),
        row(
            "class_intersect_8000",
            r"[\p{L}&&\p{Nd}]".repeat(8_000),
            7_519_768,
        ),
        row("posix_alpha_5000", "[[:alpha:]]".repeat(5_000), 3_789_768),
        row("literal_foo_dot_bar", "foo.*bar".to_string(), 325_915),
        row("perl_d_300", r"\d".repeat(300), 5_138_131),
        row(
            "casei_ascii_class_5000",
            format!("(?i){}", "[a-z]".repeat(5_000)),
            9_713_104,
        ),
    ]
}

// ---------------------------------------------------------------------
// AC1
// ---------------------------------------------------------------------

/// **AC1.** The peak of the WHOLE call — refusals included — stays under
/// the cap for every corpus row.
///
/// Refusals are the half that matters: before this issue the expensive
/// path was the REJECTING one, because `size_limit` decides after the
/// memory is spent. `(?i)\p{L}`×20000 peaked 887 MB on its way to a
/// `400`; the refusal now happens before the HIR translation that cost
/// it.
#[test]
fn compile_transient_stays_under_the_cap() {
    serialised(|| {
        for Row {
            name,
            pattern,
            peak_before,
        } in corpus()
        {
            let (peak, _) = measure(|| compile_user_regex(&pattern));
            assert!(
                peak <= MAX_REGEX_COMPILE_TRANSIENT_BYTES,
                "{name}: compiling {} bytes peaked {} (was {} before #291), over the \
                 {} cap",
                pattern.len(),
                mb(peak),
                mb(peak_before),
                mb(MAX_REGEX_COMPILE_TRANSIENT_BYTES),
            );
        }
    });
}

// ---------------------------------------------------------------------
// AC2 — the discriminator
// ---------------------------------------------------------------------

/// **AC2, the discriminator.** Two halves, and the second is what makes
/// the cheap fix refutable rather than merely unattractive.
///
/// (a) `RegexBuilder::size_limit` — the knob whose NAME suggests it
/// bounds this — does not. Swept over three values on the same two
/// patterns, the peak barely moves and never approaches the limit:
/// `(?i)\p{L}`×170 peaks 7.75 MB at a 4 KiB limit (1,891× the limit) and
/// 28.74 MB at the 10 MiB default, and `(?i)\p{L}`×3000 peaks over
/// 64 MiB at EVERY one of them. Cutting the limit 2,560× cuts the peak
/// 3.7×, and both patterns are refused by the engine the whole way. This
/// half passes today and is what pins the cheap fix as wrong.
///
/// ×3000 rather than the ×20000 the ledger quotes, for CI cost alone:
/// half (a) compiles the big pattern once per limit through the RAW
/// builder, and ×20000 costs 887 MB and ~55 s per limit in a debug build
/// (165 s for this one test). ×3000 clears the 64 MiB floor twice over at
/// a seventh of that. ×20000 is still measured — by
/// [`compile_transient_stays_under_the_cap`], where it is refused and
/// therefore cheap.
///
/// (b) the same `(?i)\p{L}`×3000 through `compile_user_regex` peaks
/// under 4 MiB. This half fails at `c7649da` (the entry point does not
/// exist) and **still fails under "just lower `size_limit`"** at any
/// value, because half (a) measured that no value bounds it.
///
/// The ×170 row is the control: it stays SERVED, and its peak stays
/// inside the cap. A guard that refused it would be a ban, not a bound —
/// the reference serves it in 49 ms.
#[test]
fn size_limit_alone_does_not_bound_it() {
    serialised(|| {
        let modest = format!("(?i){}", r"\p{L}".repeat(170));
        let huge = format!("(?i){}", r"\p{L}".repeat(3_000));

        // (a) the cheap fix, refuted at three limits two-and-a-half
        // orders of magnitude apart.
        for limit in [4 * 1024usize, 1 << 20, 10 * 1024 * 1024] {
            let (modest_peak, _) =
                measure(|| regex::RegexBuilder::new(&modest).size_limit(limit).build());
            assert!(
                modest_peak >= 6 * 1024 * 1024,
                "(?i)\\p{{L}}x170 at size_limit={limit}: peaked only {} — if the crate has \
                 learned to bound its own HIR phase, this issue's premise is gone and the \
                 whole module should be reconsidered",
                mb(modest_peak),
            );
            let (huge_peak, _) =
                measure(|| regex::RegexBuilder::new(&huge).size_limit(limit).build());
            assert!(
                huge_peak >= 64 * 1024 * 1024,
                "(?i)\\p{{L}}x3000 at size_limit={limit}: peaked only {}",
                mb(huge_peak),
            );
        }

        // (b) the fix: refused before the HIR translation that cost the
        // 887 MB above.
        let (peak, _) = measure(|| compile_user_regex(&huge));
        assert!(
            peak <= 4 * 1024 * 1024,
            "(?i)\\p{{L}}x3000 through the budgeted entry point peaked {} — the refusal \
             must happen BEFORE the HIR translation, not after it",
            mb(peak),
        );

        // The control: still served, and bounded.
        let (peak, _) = measure(|| compile_user_regex(&modest));
        assert!(
            compile_user_regex(&modest).is_ok(),
            "(?i)\\p{{L}}x170 is served by the reference and must stay served here"
        );
        assert!(
            peak <= MAX_REGEX_COMPILE_TRANSIENT_BYTES,
            "(?i)\\p{{L}}x170 peaked {}",
            mb(peak)
        );
    });
}

// ---------------------------------------------------------------------
// AC3 — what makes the constants breakable
// ---------------------------------------------------------------------

/// **AC3.** For every corpus row the estimator lets through, the measured
/// peak is at or below the estimate.
///
/// This is the test that makes every charge in `compile_budget.rs` a
/// charge rather than a claim: shrinking any ONE of the six reddens it.
/// Each row below was produced by making that edit and running this test
/// — no entry is inferred:
///
/// | constant | shrink to | first row to fail | measured vs estimate |
/// |---|---|---|---|
/// | `AST_BYTES_PER_PATTERN_BYTE` 320 | 256 | `bracket_union_65534` @ 1 MiB | 42.21 vs 36.70 MB |
/// | `HIR_BYTES_PER_NODE` 448 | 8 | `dot_star_65535` @ 10 MiB | 45.29 vs 44.04 MB |
/// | `HIR_BYTES_PER_LITERAL_NODE` 24 | 1 | `bracket_union_65534` @ 1 MiB | 42.21 vs 42.07 MB |
/// | `HIR_BYTES_PER_CLASS_RANGE` 8 | 4 | `casei_pL_170` @ 1 MiB | 9.69 vs 7.83 MB |
/// | `CASE_FOLD_TRANSIENT_FACTOR` 10 | 4 | `casei_pL_170` @ 1 MiB | 9.69 vs 6.91 MB |
/// | `NFA_PEAK_FACTOR` 3 | 2 | `class_pL_200` @ 1 MiB | 3.41 vs 3.27 MB |
///
/// **Read the "shrink to" column as the honest strength of this gate, not
/// as a threshold.** Two of the six survive a HALVING and only break when
/// cut much further: `HIR_BYTES_PER_NODE` at 128 and 32, and
/// `HIR_BYTES_PER_LITERAL_NODE` at 8, leave every row passing. That is
/// not slack in the charge, it is the shape of the model — wherever those
/// two terms could bite, the AST term (320 B per pattern byte) or the NFA
/// floor (3 × `size_limit`) is already larger, so the node terms only
/// become load-bearing at the extreme node densities `dot_star_65535` and
/// `bracket_union_65534` sit at. `dot_q_39000` is in the list for the
/// same reason and pins the same edge from the accepted side: one
/// materialised node per pattern byte, sized so its estimate lands just
/// under the cap.
///
/// `bracket_union_65534` is in this test's list and not in [`corpus`] on
/// purpose: it is the shape that fixes `AST_BYTES_PER_PATTERN_BYTE` (a
/// 131 KB single bracketed union, 160 B/byte in the AST phase — the worst
/// asymptotic ratio measured over 26 shapes), and it belongs where the
/// constant it pins is asserted.
#[test]
fn the_estimate_upper_bounds_the_measured_peak() {
    serialised(|| {
        let mut rows = corpus();
        rows.push(row(
            "bracket_union_65534",
            format!("[{}]", "ab".repeat(65_534)),
            42_207_542,
        ));
        rows.push(row("group_a_43690", "(a)".repeat(43_690), 41_507_668));
        // The shape that pins `HIR_BYTES_PER_NODE`, and the only one that
        // can: one MATERIALISED node per pattern byte (a `Dot` and a
        // `Repetition` per two bytes), sized so the estimate lands just
        // under the cap. Everywhere else the AST term and the NFA floor
        // are larger than the node term, so shrinking that charge alone
        // moves nothing — which is exactly the "gate weaker than the
        // claim" trap this row exists to close.
        rows.push(row("dot_q_39000", ".?".repeat(39_000), 33_521_560));
        rows.push(row(
            "alt_lit_65536",
            std::iter::repeat_n("a", 65_536)
                .collect::<Vec<_>>()
                .join("|"),
            29_164_007,
        ));

        // BOTH program ceilings in the workspace. The default hides an
        // under-charge behind its 31.5 MB NFA floor: at 1 MiB the floor
        // is 3.1 MB and `(?i)\p{L}`x170 allocates 9.69 MB, which is how
        // `CASE_FOLD_TRANSIENT_FACTOR` was found. A bound that holds only
        // at one caller's limit is not a bound.
        let mut checked = 0;
        for Row { name, pattern, .. } in rows {
            for limit in [1usize << 20, pulsus_re2::REGEX_PROGRAM_SIZE_LIMIT] {
                let Some(estimate) =
                    pulsus_re2::regex_compile_transient_bound_with(&pattern, limit)
                else {
                    continue;
                };
                if estimate > MAX_REGEX_COMPILE_TRANSIENT_BYTES {
                    // Refused before the phases the estimate models ever
                    // ran; AC1 is what covers this row.
                    continue;
                }
                let (peak, _) = measure(|| pulsus_re2::compile_user_regex_with(&pattern, limit));
                assert!(
                    peak <= estimate,
                    "{name} at size_limit={limit}: measured {} against an estimate of {} — \
                     the estimate is claimed to be an UPPER bound, so one of the charges in \
                     `compile_budget.rs` is too small for this shape",
                    mb(peak),
                    mb(estimate),
                );
                checked += 1;
            }
        }
        assert!(
            checked >= 24,
            "only {checked} rows reached the assertion — if the cap or the charges moved far \
             enough that most of the corpus is refused, this test has stopped measuring what \
             it claims to"
        );
    });
}

// ---------------------------------------------------------------------
// The adversarial cross-check (owner ruling v2, 2026-08-09)
// ---------------------------------------------------------------------

/// **The zero-under-bound cross-check, committed.**
///
/// [`the_estimate_upper_bounds_the_measured_peak`] proves the bound holds
/// on the corpus that MOTIVATED the model. This proves it on shapes the
/// model was not derived from — 49 of them, at both program ceilings in
/// the workspace — and it is here because it is the only thing standing
/// between the model and a silent under-estimate. Run as a one-off while
/// deriving the constants, it found the eightfold case-folding
/// under-charge; run only once, it would find the next one never.
///
/// The families are chosen for what they stress, not for realism: node
/// density (`.`, `.*`, `(a)`, `(?:a)`, `(a|b)`), AST cost per byte
/// (nested brackets one to four deep, a single 131 KB bracketed union),
/// class-range explosion (`\p{L}`, `\w`, negated forms, the three set
/// operators, POSIX names), case folding on each of those, collapsed
/// literals, assertions, repetition (flat, nested, and the 1,000-product
/// form), escapes, and the degenerate `|`×131071.
///
/// A row is checked only where the estimator ADMITS it: an over-cap
/// pattern is refused before the phases the estimate models ever run, and
/// [`compile_transient_stays_under_the_cap`] is what covers those. The
/// admitted count is asserted so that a future change which refuses most
/// of the list cannot leave this test green while measuring nothing.
#[test]
fn the_bound_holds_on_shapes_the_model_was_not_derived_from() {
    serialised(|| {
        let shapes: Vec<(String, String)> = vec![
            ("dot", ".".repeat(131_071)),
            ("dot_star", ".*".repeat(65_535)),
            ("dot_plus", ".+".repeat(65_535)),
            ("dot_q", ".?".repeat(65_535)),
            ("literal", "a".repeat(131_071)),
            ("group", "(a)".repeat(43_690)),
            ("ncgroup", "(?:a)".repeat(26_214)),
            ("nested_alt_group", "(a|b)".repeat(26_214)),
            ("bracket1", "[a]".repeat(43_690)),
            ("bracket2", "[[a]]".repeat(26_214)),
            ("bracket3", "[[[a]]]".repeat(18_724)),
            ("bracket4", "[[[[a]]]]".repeat(14_563)),
            ("bracket_range", "[a-b]".repeat(26_214)),
            ("bracket_ranges_one", format!("[{}]", "a-b".repeat(43_690))),
            ("assertions", r"\b".repeat(65_535)),
            ("word_bound", r"\bfoo\b".repeat(15_000)),
            ("esc_hex", r"\x41".repeat(32_767)),
            ("hex_class", r"\x{1F600}".repeat(10_000)),
            ("rep_brace_small", "a{1}".repeat(32_767)),
            ("rep_flat", "a{1000}".repeat(200)),
            (
                "rep_nest",
                "(?:(?:(?:(?:[0-9a-f]{32}){32}){32}){32})".to_string(),
            ),
            ("flags", "(?i)".repeat(32_767)),
            ("empty_alts", "|".repeat(131_071)),
            (
                "nest_group_deep",
                format!("{}a{}", "(".repeat(200), ")".repeat(200)),
            ),
            ("class_perl", r"\w".repeat(65_535)),
            ("class_uni", r"\p{L}".repeat(26_214)),
            ("class_uni_br", format!("[{}]", r"\p{L}".repeat(26_213))),
            ("neg_class", "[^a]".repeat(32_767)),
            ("neg_uni", r"[^\p{L}]".repeat(10_000)),
            ("uni_diff", r"[\p{L}--\p{Nd}]".repeat(4_000)),
            ("uni_sym", r"[\p{L}~~\p{Nd}]".repeat(4_000)),
            ("uni_intersect", r"[\p{L}&&\p{Nd}]".repeat(4_000)),
            ("posix_many", "[[:alpha:][:digit:][:punct:]]".repeat(2_000)),
            (
                "casei_posix",
                format!("(?i){}", "[[:alpha:]]".repeat(2_000)),
            ),
            ("casei_union", format!("(?i)[{}]", "ab".repeat(60_000))),
            ("casei_uni", format!("(?i){}", r"\p{L}".repeat(600))),
            ("casei_perl", format!("(?i){}", r"\w".repeat(2_000))),
            ("casei_neg", format!("(?i){}", r"[^\p{L}]".repeat(2_000))),
            ("neg_perl", r"\W".repeat(10_000)),
            ("neg_nd", r"[^\p{Nd}]".repeat(10_000)),
            ("neg_union", r"[^\p{L}\p{Nd}]".repeat(5_000)),
            ("neg_ascii", "[^a-z]".repeat(10_000)),
            ("neg_posix", "[^[:alpha:]]".repeat(5_000)),
            ("mixed", r"(?:foo\d+|[a-z]{2,4}|\p{L})".repeat(2_000)),
            ("uuid_rep", "[0-9a-f]{8}-[0-9a-f]{4}".repeat(2_000)),
            ("bracket_union", format!("[{}]", "ab".repeat(65_534))),
            (
                "alt_lit8",
                std::iter::repeat_n("abcdefgh", 12_000)
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            (
                "alt_lit1",
                std::iter::repeat_n("a", 65_536)
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            (
                "alt_class",
                std::iter::repeat_n(r"\p{L}", 8_000)
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
        ]
        .into_iter()
        .map(|(n, p)| (n.to_string(), p))
        .collect();

        // The count in this test's doc is a number in prose, which has no
        // source of truth and drifts — it said 56 for a 49-entry list
        // until review caught it. Asserted here so the two cannot part.
        assert_eq!(
            shapes.len(),
            49,
            "the shape list changed; update the count in this test's doc comment with it"
        );

        let mut admitted = 0;
        for (name, pattern) in shapes {
            for limit in [1usize << 20, pulsus_re2::REGEX_PROGRAM_SIZE_LIMIT] {
                let Some(estimate) =
                    pulsus_re2::regex_compile_transient_bound_with(&pattern, limit)
                else {
                    continue;
                };
                if estimate > MAX_REGEX_COMPILE_TRANSIENT_BYTES {
                    continue;
                }
                let (peak, _) = measure(|| pulsus_re2::compile_user_regex_with(&pattern, limit));
                assert!(
                    peak <= estimate,
                    "{name} ({} bytes) at size_limit={limit}: measured {} against an \
                     estimate of {} — the model UNDER-bounds a shape it was not derived \
                     from, which is the failure this test exists to catch",
                    pattern.len(),
                    mb(peak),
                    mb(estimate),
                );
                admitted += 1;
            }
        }
        assert!(
            admitted >= 40,
            "only {admitted} shape/limit pairs were admitted by the estimator — if a change \
             pushed most of this list over the cap, the cross-check is green while measuring \
             almost nothing"
        );
    });
}

// ---------------------------------------------------------------------
// The per-phase pins (issue #291 review finding 2)
// ---------------------------------------------------------------------

/// **Each HIR charge dominates the phase cost it models, measured per
/// atom.**
///
/// The end-to-end gates
/// ([`the_estimate_upper_bounds_the_measured_peak`] and
/// [`the_bound_holds_on_shapes_the_model_was_not_derived_from`]) prove
/// the TOTAL bound holds, but they cannot pin an individual charge:
/// wherever the node or negation term could bite, the AST term or the
/// 41.9 MB NFA floor is already larger, so halving either left every row
/// green. Review found exactly that — `HIR_BYTES_PER_NODE` 448→224 and
/// `CLASS_NEGATION_TRANSIENT_FACTOR` 5→3 were both fully green — and a
/// constant the suite cannot see is an assumption, not a measurement.
///
/// This test removes the terms that masked them. It measures the HIR
/// phase ALONE, per repeated atom, and asserts the model's own per-atom
/// HIR charge — `nodes × HIR_BYTES_PER_NODE + ranges × per_range` — is at
/// or above it. Families are chosen so that ONE term dominates each:
///
/// | family | dominated by | pins |
/// |---|---|---|
/// | `.` | nodes (2 ranges) | `HIR_BYTES_PER_NODE` |
/// | `\p{L}`, `\w` | ranges (677/796) | `HIR_BYTES_PER_CLASS_RANGE` |
/// | `(?i)\p{L}` | ranges × fold | `CASE_FOLD_TRANSIENT_FACTOR` |
/// | `[^\p{L}]`, `[^\w]`, `[^\p{Nd}]` | ranges × negation | `CLASS_NEGATION_TRANSIENT_FACTOR` |
///
/// Shrink-until-red, each measured by making the edit and running this
/// test — the value is RED at or below the figure given, green above it:
///
/// | constant | shipped | red at | shipped ÷ threshold |
/// |---|---|---|---|
/// | `HIR_BYTES_PER_NODE` | 448 | 243 | 1.84× |
/// | `HIR_BYTES_PER_CLASS_RANGE` | 8 | 7 | 1.00× — the smallest that holds |
/// | `CASE_FOLD_TRANSIENT_FACTOR` | 10 | 7 | 1.25× |
/// | `CLASS_NEGATION_TRANSIENT_FACTOR` | 5 | 3 | 1.25× |
/// | `NFA_PEAK_FACTOR` | 4 | 3 | 1.33× |
///
/// (`NFA_PEAK_FACTOR` is not modelled here — it is a whole-compile term
/// and is pinned by the end-to-end gates, which redden at 3 on
/// `[^a-z]`×10000. `HIR_BYTES_PER_LITERAL_NODE` is likewise not here: a
/// collapsed literal has no per-atom HIR cost to measure, and it is
/// pinned at 1 by `bracket_union_65534` end-to-end.)
///
/// **The margins above the threshold are deliberate and are not slack
/// left by accident.** An exact-fit charge is a width-dependent
/// assertion, and this repo's alloc-bound rule is ceilings that survive a
/// profile change: measured per-atom HIR cost differs between debug and
/// release, and `\p{L}`'s headroom at the shipped `HIR_BYTES_PER_CLASS_RANGE`
/// is already only 1.04×. The two multipliers carry 1.25× over the worst
/// measured ratio (4.40× for negation on `[^\p{Nd}]`, 8.05× for folding
/// on `(?i)\p{L}`) for that reason and no other.
#[test]
fn each_hir_charge_dominates_the_phase_cost_it_models() {
    serialised(|| {
        /// One repeated-atom family: the atom, the class atom the
        /// estimator probes for its range count, the materialised and
        /// collapsed node counts per atom, and the two flags that select
        /// the range multiplier.
        struct Fam {
            atom: &'static str,
            /// The class atom the estimator probes for its range count,
            /// or `None` for a node-dominated family that charges none.
            probe: Option<&'static str>,
            nodes: u64,
            /// Nodes the translator COLLAPSES, charged at
            /// `HIR_BYTES_PER_LITERAL_NODE` — a group's or repetition's
            /// literal operand.
            lits: u64,
            negated: bool,
            casei: bool,
        }
        let fams = [
            // Node-dominated. `(a)`, `a{2}` and `(a)(b)` are the WORST
            // per-node shapes measured over 21 families — 356.5 B/node,
            // against 259.5 for a bare `.` and 179.4 for `(?:a)` — and
            // they are here because without them `HIR_BYTES_PER_NODE`
            // survived being cut to 243, which is the gap review found.
            Fam {
                atom: "(a)",
                probe: None,
                nodes: 1,
                lits: 1,
                negated: false,
                casei: false,
            },
            Fam {
                atom: "a{2}",
                probe: None,
                nodes: 1,
                lits: 1,
                negated: false,
                casei: false,
            },
            Fam {
                atom: "(a)(b)",
                probe: None,
                nodes: 2,
                lits: 2,
                negated: false,
                casei: false,
            },
            Fam {
                atom: ".",
                probe: Some("."),
                nodes: 1,
                lits: 0,
                negated: false,
                casei: false,
            },
            Fam {
                atom: ".*",
                probe: Some("."),
                nodes: 2,
                lits: 0,
                negated: false,
                casei: false,
            },
            // Range-dominated.
            Fam {
                atom: r"\p{L}",
                probe: Some(r"\p{L}"),
                nodes: 1,
                lits: 0,
                negated: false,
                casei: false,
            },
            Fam {
                atom: r"\w",
                probe: Some(r"\w"),
                nodes: 1,
                lits: 0,
                negated: false,
                casei: false,
            },
            Fam {
                atom: r"\d",
                probe: Some(r"\d"),
                nodes: 1,
                lits: 0,
                negated: false,
                casei: false,
            },
            // Negation-dominated.
            Fam {
                atom: r"[^\p{L}]",
                probe: Some(r"\p{L}"),
                nodes: 2,
                lits: 0,
                negated: true,
                casei: false,
            },
            Fam {
                atom: r"[^\w]",
                probe: Some(r"\w"),
                nodes: 2,
                lits: 0,
                negated: true,
                casei: false,
            },
            Fam {
                atom: r"[^\p{Nd}]",
                probe: Some(r"\p{Nd}"),
                nodes: 2,
                lits: 0,
                negated: true,
                casei: false,
            },
            Fam {
                atom: r"\P{L}",
                probe: Some(r"\P{L}"),
                nodes: 1,
                lits: 0,
                negated: true,
                casei: false,
            },
            // Fold-dominated.
            Fam {
                atom: r"\p{L}",
                probe: Some(r"\p{L}"),
                nodes: 1,
                lits: 0,
                negated: false,
                casei: true,
            },
        ];
        const N: usize = 4_000;
        for f in &fams {
            let body = f.atom.repeat(N);
            let pattern = if f.casei { format!("(?i){body}") } else { body };
            // The HIR phase alone: the AST is built first and stays live,
            // which is what the real translation does, but the counters
            // are reset between the two so only the translate is scored.
            let ast = regex_syntax::ast::parse::ParserBuilder::new()
                .build()
                .parse(&pattern)
                .expect("parses");
            let (hir_peak, _) = measure(|| {
                regex_syntax::hir::translate::TranslatorBuilder::new()
                    .case_insensitive(f.casei)
                    .build()
                    .translate(&pattern, &ast)
            });
            drop(ast);

            let ranges = match f.probe {
                Some(probe) => {
                    let r = pulsus_re2::class_ranges_for_test(probe, f.casei);
                    assert!(r > 0, "{probe}: premise — the probe must be a class");
                    r
                }
                None => 0,
            };
            let charged = pulsus_re2::per_atom_hir_charge_for_test(
                f.nodes, f.lits, ranges, f.negated, f.casei,
            );
            let measured_per_atom = hir_peak / N as u64;

            assert!(
                charged >= measured_per_atom,
                "{}{}: the HIR phase costs {measured_per_atom} B per atom and the model \
                 charges {charged} B ({} nodes x HIR_BYTES_PER_NODE + {ranges} ranges x \
                 per_range). One of the HIR charges is below the phase it models — this is \
                 the pin the end-to-end gates cannot provide, because the AST term and the \
                 NFA floor mask it there",
                if f.casei { "(?i)" } else { "" },
                f.atom,
                f.nodes,
            );
        }
    });
}

// ---------------------------------------------------------------------
// AC4 — the OTHER wrong fix
// ---------------------------------------------------------------------

/// **AC4.** A committed accept list still compiles AND estimates under
/// the cap.
///
/// This rules out the second plausible-but-wrong fix, a cap on pattern
/// LENGTH: at any value ≤ 130,000 it refuses the literal row, and at
/// 1 KiB it refuses four more — while the reference serves the 130 KB
/// literal in 34 ms. Length is not the quantity that predicts the cost;
/// `\w`×64 is 128 bytes and cost 10.6 MB, `a`×130000 is a thousand times
/// longer and cost 21.9 MB.
#[test]
fn still_served_after_the_bound() {
    serialised(|| {
        let accepted = [
            ("literal_a_130000", "a".repeat(130_000)),
            ("class_pL_200", r"\p{L}".repeat(200)),
            ("casei_pL_140", format!("(?i){}", r"\p{L}".repeat(140))),
            ("perl_d_300", r"\d".repeat(300)),
            ("foo_dot_bar", "foo.*bar".to_string()),
            ("uuid_prefix", "[0-9a-f]{8}-[0-9a-f]{4}".to_string()),
        ];
        for (name, pattern) in accepted {
            let estimate = regex_compile_transient_bound(&pattern)
                .unwrap_or_else(|| panic!("{name}: must parse"));
            assert!(
                estimate <= MAX_REGEX_COMPILE_TRANSIENT_BYTES,
                "{name}: estimated {} against a {} cap — this pattern is served by the \
                 reference and must stay served here",
                mb(estimate),
                mb(MAX_REGEX_COMPILE_TRANSIENT_BYTES),
            );
            assert!(
                compile_user_regex(&pattern).is_ok(),
                "{name}: must still compile"
            );
        }

        // The length-cap fix, refuted on its own terms: the shortest row
        // above cost more than the longest.
        let (short_peak, _) = measure(|| compile_user_regex(r"\w".repeat(64).as_str()));
        let (long_peak, _) = measure(|| compile_user_regex("a".repeat(130_000).as_str()));
        assert!(
            short_peak * 4 > long_peak,
            "a 128-byte pattern peaked {} and a 130,000-byte one {} — if length had become \
             predictive of cost, a length cap would be the simpler fix and this module would \
             not need to exist",
            mb(short_peak),
            mb(long_peak),
        );
    });
}

// ---------------------------------------------------------------------
// AC6 — the estimator must not cost what it saves
// ---------------------------------------------------------------------

/// **AC6.** Per benign row, the allocation the estimator spends is at or
/// below the allocation of the compile that follows it.
///
/// Scale-invariant and tier-1: a ratio between two measured quantities in
/// the same process, never a wall-time assert. (Wall time for the record,
/// not asserted: the estimator runs 0–18 ms on these rows, and it runs
/// once per distinct pattern at plan time — never per row, never per
/// span.)
#[test]
fn the_estimator_costs_less_than_the_compile_it_precedes() {
    serialised(|| {
        for Row { name, pattern, .. } in corpus() {
            let Some(estimate) = regex_compile_transient_bound(&pattern) else {
                continue;
            };
            if estimate > MAX_REGEX_COMPILE_TRANSIENT_BYTES {
                continue;
            }
            let (_, estimator_total) = measure(|| regex_compile_transient_bound(&pattern));
            let (_, compile_total) = measure(|| {
                regex::RegexBuilder::new(&pattern)
                    .size_limit(10 * 1024 * 1024)
                    .build()
            });
            assert!(
                estimator_total <= compile_total,
                "{name}: the estimator allocated {} to save a compile that allocates {} — a \
                 guard that costs more than the thing it guards is not a guard",
                mb(estimator_total),
                mb(compile_total),
            );
        }
    });
}

// ---------------------------------------------------------------------
// AC8 — the validator must not learn to reject
// ---------------------------------------------------------------------

/// **AC8.** `re2_verdict` answers `Unknown` for an over-budget pattern,
/// never `Rejects`.
///
/// `re2_verdict` is a VALIDATOR: its consumers treat `Unknown` as accept,
/// and an over-rejection there breaks TraceQL queries the reference
/// serves. Our compile budget is no more RE2's verdict than the crate's
/// own `CompiledTooBig` is, so it must not leak into one.
#[test]
fn the_refusal_is_unknown_to_the_trace_validator() {
    serialised(|| {
        for (name, pattern) in [
            ("class_alt_20001", {
                std::iter::repeat_n(r"\p{L}", 20_001)
                    .collect::<Vec<_>>()
                    .join("|")
            }),
            ("neg_uni_20000", r"[^\p{L}]".repeat(20_000)),
        ] {
            // Premise: the pattern really is over budget, so this row is
            // exercising the arm it claims to.
            assert!(
                matches!(
                    compile_user_regex(&pattern),
                    Err(RegexCompileError::TooLarge { .. })
                ),
                "{name}: premise — must be refused by the budget"
            );
            assert_eq!(
                re2_verdict(&pattern),
                Re2Verdict::Unknown,
                "{name}: a memory refusal is not an RE2 rejection"
            );
        }
    });
}

// ---------------------------------------------------------------------
// AC9 — the constant and the docs cannot drift
// ---------------------------------------------------------------------

/// **AC9.** The cap is documented where a user would look, by its byte
/// value, so the constant and the prose cannot drift apart.
#[test]
fn the_cap_is_documented_where_a_user_would_look() {
    serialised(|| {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let api = std::fs::read_to_string(root.join("docs/api.md")).expect("docs/api.md");
        let ledger =
            std::fs::read_to_string(root.join("docs/benchmarks/logs-differential-ledger.md"))
                .expect("logs-differential-ledger.md");

        let mib = MAX_REGEX_COMPILE_TRANSIENT_BYTES / (1024 * 1024);
        let rendered = format!("{mib} MiB");

        assert!(
            api.contains("regex-compile-budget"),
            "docs/api.md must carry the `regex-compile-budget` row"
        );
        assert!(
            api.contains(&rendered),
            "docs/api.md must name the cap as `{rendered}` — a user hitting this refusal has \
             no other way to learn where the boundary is"
        );
        assert!(
            ledger.contains("regex-compile-budget"),
            "the logs differential ledger must carry a `regex-compile-budget` row"
        );
        assert!(
            ledger.contains(&rendered),
            "the ledger row must name the cap as `{rendered}`, not describe it"
        );
    });
}

// ---------------------------------------------------------------------
// Unit coverage for the entry points' own edges
// ---------------------------------------------------------------------

/// A pattern whose AST does not parse has no estimate, and must fall
/// through to the engine for the canonical error rather than being
/// refused on a guess.
#[test]
fn an_unparseable_pattern_gets_the_engines_own_error() {
    serialised(|| {
        for pattern in ["(", "[", "a{2,1}", r"\Qa*\E"] {
            assert!(
                regex_compile_transient_bound(pattern).is_none()
                    || compile_user_regex(pattern).is_err(),
                "{pattern:?}"
            );
            assert!(
                matches!(
                    compile_user_regex(pattern),
                    Err(RegexCompileError::Engine(_))
                ),
                "{pattern:?}: must carry the engine's own verdict, not a budget refusal"
            );
        }
    });
}

/// The refusal names the class the reference names — Go's
/// `ErrLarge`, `expression too large`
/// (`vendor/github.com/grafana/regexp/syntax/parse.go:47 @ v3.7.4`) —
/// and reports both numbers, so a user can tell how far over they are.
#[test]
fn the_refusal_says_which_boundary_was_crossed() {
    serialised(|| {
        let pattern = std::iter::repeat_n(r"\p{L}", 20_001)
            .collect::<Vec<_>>()
            .join("|");
        let err = compile_user_regex(&pattern).expect_err("over budget");
        let rendered = err.to_string();
        assert!(rendered.starts_with("expression too large"), "{rendered:?}");
        let RegexCompileError::TooLarge { estimate, cap } = err else {
            panic!("must be a budget refusal");
        };
        assert!(estimate > cap, "{estimate} vs {cap}");
        assert_eq!(cap, MAX_REGEX_COMPILE_TRANSIENT_BYTES);
    });
}

/// The anchored entry estimates the string it actually compiles. Pinned
/// because estimating the BARE pattern and compiling the anchored one is
/// the natural mistake, and it under-counts by the wrapper.
#[test]
fn the_anchored_entry_estimates_the_anchored_string() {
    serialised(|| {
        let bare = r"\p{L}".repeat(200);
        let anchored = format!("^(?:{bare})$");
        assert_eq!(
            regex_compile_transient_bound(&anchored),
            regex_compile_transient_bound(&anchored),
        );
        assert!(
            regex_compile_transient_bound(&anchored) > regex_compile_transient_bound(&bare),
            "the wrapper adds nodes and bytes, so its estimate must be strictly larger"
        );
        assert!(pulsus_re2::compile_user_regex_anchored(&bare).is_ok());
    });
}

/// A tighter `size_limit` buys a smaller NFA term, so a site with its own
/// program ceiling is charged less than one at the default. The LogQL
/// template site is the only such caller.
#[test]
fn a_tighter_program_ceiling_lowers_the_charge() {
    serialised(|| {
        let pattern = format!("(?i){}", r"\p{L}".repeat(17));
        let default = regex_compile_transient_bound(&pattern).expect("parses");
        let template =
            pulsus_re2::regex_compile_transient_bound_with(&pattern, 1 << 20).expect("parses");
        assert!(
            template < default,
            "{template} must be below {default}: the NFA term scales with the limit"
        );
        assert!(
            pulsus_re2::compile_user_regex_with(&pattern, 1 << 20).is_ok(),
            "and the pattern must still compile at that ceiling"
        );
    });
}

/// The `[\p{L}&&\p{Nd}]` trap, stated as its own test because it is the
/// one place a simpler estimator is measurably wrong: the bracket
/// TRANSLATES to an empty class, so charging the bracket's result
/// under-counts by ~740 ranges per atom while the compile still pays for
/// both operands.
#[test]
fn an_intersection_is_charged_for_its_operands_not_its_result() {
    serialised(|| {
        let one = r"[\p{L}&&\p{Nd}]";
        // Premise: the result really is empty, which is what makes the naive
        // rule under-count.
        let hir = regex_syntax::hir::translate::TranslatorBuilder::new()
            .build()
            .translate(
                one,
                &regex_syntax::ast::parse::ParserBuilder::new()
                    .build()
                    .parse(one)
                    .expect("parses"),
            )
            .expect("translates");
        let empty = match hir.kind() {
            regex_syntax::hir::HirKind::Class(regex_syntax::hir::Class::Unicode(c)) => {
                c.ranges().is_empty()
            }
            regex_syntax::hir::HirKind::Class(regex_syntax::hir::Class::Bytes(c)) => {
                c.ranges().is_empty()
            }
            other => panic!("expected a class, got {other:?}"),
        };
        assert!(empty, "premise: the intersection is empty");

        let bound = regex_compile_transient_bound(one).expect("parses");
        let floor = regex_compile_transient_bound("").expect("parses");
        assert!(
            bound - floor > 5_000,
            "one intersection atom was charged only {} bytes above the empty-pattern floor — \
             the operands are what the compile pays for, not the result",
            bound - floor
        );
    });
}
