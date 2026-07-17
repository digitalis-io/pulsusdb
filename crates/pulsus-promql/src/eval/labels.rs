//! Issue #68 (M6-05): the label/sort/absence post-fetch surface —
//! `sort`/`sort_desc`, `sort_by_label`/`sort_by_label_desc` (natural,
//! numeric-aware label collation), `label_replace`/`label_join` (joint
//! `(metric_name, Labels)` rewrites with upstream's duplicate-labelset
//! detection), and the shared `absent`/`absent_over_time` label synthesis.
//! Everything here is pure and in-engine: the wrapped expression's
//! selector set (and therefore its fetch SQL) is byte-identical to the
//! bare expression's (pinned by
//! `plan::tests::m6_05_label_sort_absence_fns_keep_the_selector_set_byte_identical`).

use std::cmp::Ordering;
use std::collections::HashSet;

use pulsus_model::{LabelMatcher, MatchOp};

use crate::error::PromqlError;
use crate::value::{InstantSample, Labels};

/// Compiles `label_replace`'s regex with upstream's exact anchoring —
/// `^(?s:regex)$` (`promql/functions.go:2464`, full-string match with the
/// dot-all flag so `.` matches embedded newlines; plan v2 Δ1). Called at
/// plan time (validation — an invalid pattern is a query error before any
/// fetch) and again per evaluation step (the eval arm recompiles; a rare
/// rewrite, not an aggregation hot path — plan edge case 5).
pub fn compile_label_replace_regex(regex: &str) -> Result<regex::Regex, PromqlError> {
    let translated = re2_ascii_perl_classes(regex);
    regex::Regex::new(&format!("^(?s:{translated})$")).map_err(|_| PromqlError::LabelSet {
        detail: format!("invalid regular expression in label_replace(): {regex}"),
    })
}

/// Rewrites the Perl character classes and word boundaries to their
/// **ASCII** definitions before handing the pattern to the `regex` crate
/// (#68 review rounds 1–2): Go RE2's `\d`/`\w`/`\s` (and negations) and
/// `\b`/`\B` are ASCII-only, while Rust `regex` defaults them to Unicode
/// — so e.g. the Arabic-Indic digit `٣` matches Rust's `\d` but not
/// Go's, and Rust's Unicode `\b` sees no boundary in `e\bé` where Go
/// does. Escape-aware single pass:
///
/// - an **unescaped** `\d`/`\D`/`\w`/`\W`/`\s`/`\S` becomes its explicit
///   ASCII class (`[0-9]`, `[0-9A-Za-z_]`, RE2's `\s` = `[\t\n\f\r ]` —
///   note: no `\v`, unlike POSIX `space`); the bracketed replacement is
///   valid both bare and inside a character class, because `regex`
///   supports nested classes (`[a[0-9]]`, `[^[^0-9]]`);
/// - an unescaped `\b`/`\B` **outside a character class** becomes
///   `(?-u:\b)`/`(?-u:\B)` — the crate's ASCII-boundary syntax, exactly
///   RE2's semantics. Class-interior `\b` is NEVER a boundary: RE2 reads
///   `[\b]` as a BACKSPACE literal, which the `regex` crate spells
///   `\x08` (it rejects `[\b]` outright — verified by test — so leaving
///   it verbatim would turn a valid RE2 pattern into a compile error);
///   the pass tracks unescaped `[`…`]` (a boolean suffices — RE2 has no
///   nested classes in its *input* syntax, and `]` directly after
///   `[`/`[^` is an error there, not a literal);
/// - any other `\x` escape pair is copied verbatim, so an escaped
///   backslash (`\\d`, `\\b` = literal `\` then letter) is untouched;
/// - everything else (Unicode literals, `.`, case-folding rules, which
///   already agree between the engines) passes through byte-for-byte.
fn re2_ascii_perl_classes(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 16);
    let mut chars = pattern.chars();
    let mut in_class = false;
    while let Some(c) = chars.next() {
        if c != '\\' {
            match c {
                '[' => in_class = true,
                ']' => in_class = false,
                _ => {}
            }
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('d') => out.push_str("[0-9]"),
            Some('D') => out.push_str("[^0-9]"),
            Some('w') => out.push_str("[0-9A-Za-z_]"),
            Some('W') => out.push_str("[^0-9A-Za-z_]"),
            Some('s') => out.push_str(r"[\t\n\f\r ]"),
            Some('S') => out.push_str(r"[^\t\n\f\r ]"),
            Some('b') => out.push_str(if in_class { r"\x08" } else { r"(?-u:\b)" }),
            Some('B') if !in_class => out.push_str(r"(?-u:\B)"),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            // A trailing lone backslash: copy through; the compiler
            // rejects it, matching RE2 (error parity via LabelSet).
            None => out.push('\\'),
        }
    }
    out
}

/// Upstream `model.LabelName.IsValid()` under v3.13's default UTF-8
/// validation scheme: any non-empty valid-UTF-8 string is a legal label
/// name (`common/model/labels.go` — the legacy `[a-zA-Z_][a-zA-Z0-9_]*`
/// regex only applies under the legacy scheme). A Rust `&str` is valid
/// UTF-8 by construction, so emptiness is the only representable
/// invalidity (the vendored corpus's `"\xff"` invalid-byte case cannot be
/// expressed in a Rust `String` at all).
pub fn is_valid_label_name(name: &str) -> bool {
    !name.is_empty()
}

/// Ports upstream `createLabelsForAbsentFunction` (v3.13.0) exactly,
/// including its `has`/delete rule: walking the matchers in source order,
/// a first-seen **equality** matcher sets its label; *any* other matcher
/// targeting that name (a second equality, or any regex/negation)
/// **deletes** it — so `{a="1",a="2"}` and `{a="1",a=~"x"}` both drop `a`,
/// while `{a=~"x",a="1"}` KEEPS `a="1"` (order matters exactly as
/// upstream — a `labels.Builder`-over-EmptyLabels artifact, traced at the
/// pinned SHA and confirmed live for issue #67; see the
/// `PlanExpr::AbsentOverTime` eval arm's provenance comment). `__name__`
/// is never emitted (`sel.matchers` excludes it by the planner's
/// metric-scoping rule). Shared by `absent()` and `absent_over_time()`
/// (issue #68 factored it out of the #67 arm).
pub fn labels_for_absent(matchers: &[LabelMatcher]) -> Labels {
    let mut synthesized: Vec<(String, String)> = Vec::new();
    let mut has: HashSet<&str> = HashSet::new();
    for m in matchers {
        if m.op == MatchOp::Eq && !has.contains(m.key.as_str()) {
            synthesized.push((m.key.clone(), m.value.clone()));
            has.insert(&m.key);
        } else {
            synthesized.retain(|(k, _)| k != &m.key);
        }
    }
    Labels::new(synthesized)
}

/// Splits `s` into maximal runs of ASCII digits / non-digits — the
/// chunking `natural_cmp` compares over (upstream `sort_by_label` uses
/// `facette/natsort`, whose `(\d+|\D+)` chunk regex is ASCII-digit-based;
/// splitting on `is_ascii_digit` boundaries never lands mid-UTF-8-char
/// because every boundary byte is either an ASCII digit or the start byte
/// of a new character).
fn natural_chunks(s: &str) -> impl Iterator<Item = &str> {
    let mut rest = s;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        let first_is_digit = rest.as_bytes()[0].is_ascii_digit();
        let end = rest
            .bytes()
            .position(|b| b.is_ascii_digit() != first_is_digit)
            .unwrap_or(rest.len());
        let (chunk, tail) = rest.split_at(end);
        rest = tail;
        Some(chunk)
    })
}

/// Compares two all-ASCII-digit runs by numeric value at arbitrary
/// precision — no integer parsing (review adjudication on #68: the
/// earlier `i64`-parse-with-byte-fallback port of Go's `strconv.Atoi`
/// step created ordering **cycles** once a run overflowed, e.g.
/// `"2" < "10" < "199999999999999999999" < "2"`):
///
/// 1. strip leading zeros;
/// 2. more significant digits ⇒ larger;
/// 3. equal length ⇒ lexicographic on the stripped runs (byte order ==
///    numeric order for equal-length digit strings);
/// 4. numerically equal ⇒ the run with MORE leading zeros sorts first,
///    the final deterministic tiebreak (#68 review round 2: this is a
///    pure zero-COUNT ordering, chosen and stated as such — upstream
///    leaves the tie undefined). For runs with a nonzero significant
///    part it coincides with byte order (`"01" < "1"`, since
///    `'0' < '1'..'9'`); for all-zero runs it deliberately does NOT
///    (`"00" < "0"`, where byte order would put the shorter prefix
///    first). Both pinned by goldens below.
///
/// Equal ⇒ byte-identical, so the chunk order is total by construction.
fn digit_run_cmp(x: &str, y: &str) -> Ordering {
    let sx = x.trim_start_matches('0');
    let sy = y.trim_start_matches('0');
    sx.len()
        .cmp(&sy.len())
        .then_with(|| sx.cmp(sy))
        .then_with(|| (y.len() - sy.len()).cmp(&(x.len() - sx.len())))
}

/// Natural (numeric-aware) string ordering — the `facette/natsort`
/// semantics upstream `sort_by_label`/`sort_by_label_desc` use
/// (`promql/functions.go:1064`), made a total order (plan v2 Δ2 + the
/// #68 review adjudication):
///
/// - chunks compare pairwise in order; digit-run pairs compare by
///   [`digit_run_cmp`] (arbitrary-precision numeric — a deliberate,
///   adjudicated deviation from Go's `Atoi`-failure byte fallback, which
///   is cyclic and therefore not a sort order at all for over-`i64`
///   runs), any other pair by bytes;
/// - a string that runs out of chunks first sorts first (so `""` sorts
///   before everything).
///
/// Totality: a digit chunk and a non-digit chunk share no bytes (runs
/// are maximal), so their byte comparison is decided by first-byte
/// *class* — mixed comparisons can never rank two digit chunks
/// inconsistently — and chunk-`Equal` implies byte-identical chunks, so
/// the lexicographic walk is a genuine total order (pinned by the
/// transitivity property test below). Upstream's boolean `Compare` is
/// ill-defined on numerically-tied inputs (`"01"` vs `"1"` returns
/// `true` both ways); `digit_run_cmp`'s zero-count tiebreak resolves it
/// deterministically without disturbing any input upstream defines.
/// Pinned by the vendored cpu (`0,1,2,3,10,11,12,20,21,100`), instance
/// (`4m5,4m600,4m1000`) and release (`1.2.3,1.11.3,1.111.3`) orders plus
/// the leading-zero/mixed/empty edge vectors below.
pub fn natural_cmp(a: &str, b: &str) -> Ordering {
    let mut ca = natural_chunks(a);
    let mut cb = natural_chunks(b);
    loop {
        match (ca.next(), cb.next()) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(x), Some(y)) => {
                let both_digits =
                    x.as_bytes()[0].is_ascii_digit() && y.as_bytes()[0].is_ascii_digit();
                let ord = if both_digits {
                    digit_run_cmp(x, y)
                } else {
                    x.cmp(y)
                };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
        }
    }
}

/// Yields the sample's **virtual full label set** — its `Labels` (sorted,
/// `__name__`-free by construction) with `("__name__", metric_name)`
/// spliced in at its lexically sorted key position (plan v2 Δ2: upstream
/// `labels.Compare` treats `__name__` as an ordinary label at its lexical
/// position — `_` = 0x5f sorts before lowercase, so it is *typically*
/// first, but a `__a…`-named label sorts before it; the cross-metric-tie
/// goldens pin exactly that). `pub(crate)` since issue #69 (M6-06):
/// `eval::aggregation::series_offset` hashes exactly this virtual full
/// label set for `limit_ratio`'s inclusion offset.
pub(crate) fn full_labels(s: &InstantSample) -> impl Iterator<Item = (&str, &str)> {
    let mut name = s.metric_name.as_deref();
    let mut rest = s.labels.0.iter().peekable();
    std::iter::from_fn(move || {
        if let Some(n) = name {
            let name_first = match rest.peek() {
                Some((k, _)) => "__name__" < k.as_str(),
                None => true,
            };
            if name_first {
                name = None;
                return Some(("__name__", n));
            }
        }
        rest.next().map(|(k, v)| (k.as_str(), v.as_str()))
    })
}

/// Ports upstream `labels.Compare` over the virtual full label sets
/// (elementwise `(key, value)` comparison, a strict prefix sorting first
/// — exactly `Iterator::cmp` over `(&str, &str)` pairs).
fn full_labelset_cmp(a: &InstantSample, b: &InstantSample) -> Ordering {
    full_labels(a).cmp(full_labels(b))
}

/// A sample's value for `name` as `sort_by_label`/`label_replace`/
/// `label_join` read it: `__name__` reads `metric_name` (carried outside
/// `Labels` — see `InstantSample::metric_name`), anything else reads the
/// label; a missing label is the empty string (upstream `Metric.Get`).
fn label_value<'a>(s: &'a InstantSample, name: &str) -> &'a str {
    if name == "__name__" {
        s.metric_name.as_deref().unwrap_or("")
    } else {
        s.labels.get(name).unwrap_or("")
    }
}

/// `sort(v)` / `sort_desc(v)`: value order ascending/descending with NaN
/// sorting **last in both directions** (upstream funcSort/funcSortDesc —
/// pinned by the vendored `functions.test:703,715` NaN-final rows). Ties
/// are upstream-unspecified; broken by the virtual full label set
/// ascending for determinism (never observable by a set comparison).
pub fn sort_vector(v: &mut [InstantSample], descending: bool) {
    v.sort_by(|a, b| match (a.v.is_nan(), b.v.is_nan()) {
        (true, true) => full_labelset_cmp(a, b),
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => {
            let ord = if descending {
                b.v.partial_cmp(&a.v)
            } else {
                a.v.partial_cmp(&b.v)
            };
            // Non-NaN floats always compare — `unwrap_or` keeps this
            // total without an unreachable panic path.
            match ord.unwrap_or(Ordering::Equal) {
                Ordering::Equal => full_labelset_cmp(a, b),
                o => o,
            }
        }
    });
}

/// `sort_by_label(v, names…)` / `sort_by_label_desc(v, names…)`
/// (experimental): each named label's value compares by [`natural_cmp`]
/// in argument order (equal values continue to the next name — upstream's
/// `lv1 == lv2 { continue }`); when every named label ties, the final
/// tie-break is the virtual-full-label-set comparison (including
/// `__name__` at lexical position). `desc` reverses the **whole**
/// comparator result (plan v2 Δ2). Values (NaN included) are irrelevant —
/// the sort is purely by labels.
pub fn sort_by_label_vector(v: &mut [InstantSample], names: &[String], descending: bool) {
    v.sort_by(|a, b| {
        let mut ord = Ordering::Equal;
        for name in names {
            let lv1 = label_value(a, name);
            let lv2 = label_value(b, name);
            if lv1 == lv2 {
                continue;
            }
            ord = natural_cmp(lv1, lv2);
            break;
        }
        if ord == Ordering::Equal {
            ord = full_labelset_cmp(a, b);
        }
        if descending { ord.reverse() } else { ord }
    });
}

/// Sets (`value` non-empty) or deletes (`value` empty — Prometheus's
/// empty-label-value-is-absent rule, vendored `functions.test:514,572`)
/// `dst` on the sample's joint `(metric_name, Labels)` identity:
/// `__name__` writes `metric_name` directly (never a `Labels` entry —
/// `Labels` excludes it by construction), everything else edits the
/// sorted label vector in place.
fn set_or_delete(s: &mut InstantSample, dst: &str, value: String) {
    if dst == "__name__" {
        s.metric_name = if value.is_empty() { None } else { Some(value) };
        // An EXPLICIT `__name__` write clears the delayed drop verdict
        // (issue #86; upstream funcLabelReplace/evalLabelJoin:
        // `DropName = false` when `dst == MetricName`, functions.go:2411/
        // :2463) — an empty value is an explicit delete, not a drop, so
        // the terminal cleanup must NOT strip `__type__`/`__unit__` for
        // it (the 08c `metric_name.is_none()`-proxy residual, now fixed).
        s.drop_name = false;
        return;
    }
    let labels = &mut s.labels.0;
    match labels.iter().position(|(k, _)| k == dst) {
        Some(i) => {
            if value.is_empty() {
                labels.remove(i);
            } else {
                labels[i].1 = value;
            }
        }
        None if value.is_empty() => {}
        None => {
            // Keys are unique within one series' labels, so key-sorted
            // insertion preserves the full `(key, value)` sort invariant.
            let pos = labels.partition_point(|(k, _)| k.as_str() < dst);
            labels.insert(pos, (dst.to_string(), value));
        }
    }
}

/// Post-rewrite duplicate detection over the FULL series identity
/// `(metric_name, Labels)` (plan v3 Δ5(b): two series with equal non-name
/// labels but different metric names are distinct — never a false
/// collision), with upstream's exact error text.
fn check_unique_labelsets(v: &[InstantSample]) -> Result<(), PromqlError> {
    let mut seen: HashSet<(Option<&str>, &Labels)> = HashSet::with_capacity(v.len());
    for s in v {
        if !seen.insert((s.metric_name.as_deref(), &s.labels)) {
            return Err(PromqlError::LabelSet {
                detail: "vector cannot contain metrics with the same labelset".to_string(),
            });
        }
    }
    Ok(())
}

/// `label_replace(v, dst, replacement, src, regex)` (upstream
/// funcLabelReplace, vendored `functions.test:477-527`): the anchored
/// dot-all regex fully matches each sample's `src` value (a non-existent
/// `src` reads as `""`; a matched-but-empty `src` still relabels); on
/// match, `replacement` expands `$1`/`${name}` capture references and
/// sets/deletes `dst`; no match leaves the series untouched. `dst`/`src`
/// may be `__name__`. The regex/`dst` were validated at plan time; the
/// duplicate-identity check runs per step over the rewritten vector.
pub fn label_replace_vector(
    v: Vec<InstantSample>,
    dst: &str,
    replacement: &str,
    src: &str,
    regex: &str,
) -> Result<Vec<InstantSample>, PromqlError> {
    let re = compile_label_replace_regex(regex)?;
    let mut out = Vec::with_capacity(v.len());
    for mut s in v {
        // Scoped so the captures' borrow of `s` ends before the mutation.
        let expanded = re.captures(label_value(&s, src)).map(|caps| {
            let mut expanded = String::new();
            caps.expand(replacement, &mut expanded);
            expanded
        });
        if let Some(value) = expanded {
            set_or_delete(&mut s, dst, value);
        }
        out.push(s);
    }
    check_unique_labelsets(&out)?;
    Ok(out)
}

/// `label_join(v, dst, separator, src…)` (upstream funcLabelJoin,
/// vendored `functions.test:562-591`): concatenates the `src` label
/// values in argument order (missing = `""`) with `separator` and
/// sets/deletes `dst` on every sample (zero `src` labels joins to `""`,
/// deleting `dst`). `dst`/`src` names were validated at plan time; the
/// duplicate-identity check runs per step.
pub fn label_join_vector(
    v: Vec<InstantSample>,
    dst: &str,
    separator: &str,
    src_labels: &[String],
) -> Result<Vec<InstantSample>, PromqlError> {
    let mut out = Vec::with_capacity(v.len());
    for mut s in v {
        let mut joined = String::new();
        for (i, src) in src_labels.iter().enumerate() {
            if i > 0 {
                joined.push_str(separator);
            }
            joined.push_str(label_value(&s, src));
        }
        set_or_delete(&mut s, dst, joined);
        out.push(s);
    }
    check_unique_labelsets(&out)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(name: Option<&str>, labels: &[(&str, &str)], v: f64) -> InstantSample {
        InstantSample {
            labels: Labels::new(labels.iter().map(|(k, v)| (k.to_string(), v.to_string()))),
            metric_name: name.map(str::to_string),
            drop_name: false,
            t_ms: 0,
            v,
        }
    }

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    // --- natural_cmp (AC3/AC10): the vendored discriminators + Δ2 edges ---

    #[test]
    fn natural_cmp_reproduces_the_vendored_cpu_instance_and_release_orders() {
        // cpu (functions.test:852): 0,1,2,3,10,11,12,20,21,100.
        let cpu = ["0", "1", "2", "3", "10", "11", "12", "20", "21", "100"];
        // instance (functions.test:865): 4m5, 4m600, 4m1000.
        let instance = ["4m5", "4m600", "4m1000"];
        // release (functions.test:871): 1.2.3, 1.11.3, 1.111.3.
        let release = ["1.2.3", "1.11.3", "1.111.3"];
        for order in [&cpu[..], &instance[..], &release[..]] {
            for w in order.windows(2) {
                assert_eq!(natural_cmp(w[0], w[1]), Ordering::Less, "{w:?}");
                assert_eq!(natural_cmp(w[1], w[0]), Ordering::Greater, "{w:?} reversed");
            }
        }
        // A lexicographic shortcut would order these wrongly:
        assert_eq!(natural_cmp("10", "2"), Ordering::Greater);
        assert_eq!(natural_cmp("1.11.3", "1.2.3"), Ordering::Greater);
    }

    /// Plan v2 Δ2 edge vectors: leading zeros resolve by the byte-order
    /// fallback (deterministically, antisymmetrically — upstream's
    /// boolean comparator is ill-defined exactly here), mixed
    /// alpha-numeric tokens compare chunkwise, the empty string sorts
    /// first, and digits-vs-alpha falls back to byte order.
    #[test]
    fn natural_cmp_edge_vectors_leading_zero_mixed_tokens_and_empty() {
        assert_eq!(natural_cmp("01", "1"), Ordering::Less);
        assert_eq!(natural_cmp("1", "01"), Ordering::Greater);
        assert_eq!(natural_cmp("01", "01"), Ordering::Equal);
        // All-zero runs: the zero-count tiebreak (more zeros first) —
        // deliberately NOT byte order here (#68 review round 2, pinned).
        assert_eq!(natural_cmp("00", "0"), Ordering::Less);
        assert_eq!(natural_cmp("0", "00"), Ordering::Greater);
        assert_eq!(natural_cmp("a01z", "a1z"), Ordering::Less);
        assert_eq!(natural_cmp("a10b2", "a10b10"), Ordering::Less);
        assert_eq!(natural_cmp("a10b10", "a10b2"), Ordering::Greater);
        assert_eq!(natural_cmp("", "0"), Ordering::Less);
        assert_eq!(natural_cmp("0", ""), Ordering::Greater);
        assert_eq!(natural_cmp("", ""), Ordering::Equal);
        assert_eq!(natural_cmp("1", "a"), Ordering::Less);
        assert_eq!(natural_cmp("abc", "abd"), Ordering::Less);
        // A prefix sorts first ("4m5" vs "4m5x").
        assert_eq!(natural_cmp("4m5", "4m5x"), Ordering::Less);
    }

    /// The exact ordering cycle the pre-fix `i64`-parse fallback produced
    /// (`"2" < "10" < "199999999999999999999" < "2"` — #68 review,
    /// finding 1): arbitrary-precision digit-run comparison ranks all
    /// three consistently, over-`i64` runs included.
    #[test]
    fn natural_cmp_ranks_the_former_overflow_cycle_triple_consistently() {
        let big = "199999999999999999999"; // > i64::MAX
        assert_eq!(natural_cmp("2", "10"), Ordering::Less);
        assert_eq!(natural_cmp("10", big), Ordering::Less);
        assert_eq!(
            natural_cmp("2", big),
            Ordering::Less,
            "pre-fix: Greater (the cycle)"
        );
        assert_eq!(natural_cmp(big, "2"), Ordering::Greater);
        // Over-i64 runs against each other: numeric, not byte, order.
        assert_eq!(
            natural_cmp("99999999999999999999", big),
            Ordering::Less,
            "20 digits < 21 digits regardless of leading bytes"
        );
        // Leading zeros on an over-i64 run: value first, zero-count tiebreak.
        assert_eq!(natural_cmp(&format!("0{big}"), big), Ordering::Less);
    }

    /// Property test (#68 review, finding 1): `natural_cmp` is a strict
    /// total order — antisymmetric and transitive over random triples
    /// drawn from a digit-heavy alphabet (runs regularly exceed `i64`).
    /// Deterministic splitmix64 generator, no RNG dependency.
    #[test]
    fn natural_cmp_is_antisymmetric_and_transitive_over_random_triples() {
        fn splitmix64(state: &mut u64) -> u64 {
            *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = *state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }
        // Digit-heavy alphabet: long runs, leading zeros, chunk-boundary
        // characters both below ('.') and above ('a', 'z') the digits.
        const ALPHABET: &[u8] = b"00123999.az";
        let mut state = 0x685f_6d36_0500_u64; // fixed seed
        let gen_string = |state: &mut u64| -> String {
            let len = (splitmix64(state) % 28) as usize;
            (0..len)
                .map(|_| ALPHABET[(splitmix64(state) as usize) % ALPHABET.len()] as char)
                .collect()
        };
        for i in 0..5_000 {
            let a = gen_string(&mut state);
            let b = gen_string(&mut state);
            let c = gen_string(&mut state);
            for (x, y) in [(&a, &b), (&b, &c), (&a, &c)] {
                assert_eq!(
                    natural_cmp(x, y),
                    natural_cmp(y, x).reverse(),
                    "antisymmetry violated at iteration {i}: {x:?} vs {y:?}"
                );
                assert_eq!(
                    natural_cmp(x, y) == Ordering::Equal,
                    x == y,
                    "Equal must mean identical at iteration {i}: {x:?} vs {y:?}"
                );
            }
            // Transitivity: sort the triple with the comparator, then
            // check every adjacent-and-skip pair agrees with it.
            let mut sorted = [a.as_str(), b.as_str(), c.as_str()];
            sorted.sort_by(|x, y| natural_cmp(x, y));
            for w in [(0, 1), (1, 2), (0, 2)] {
                assert_ne!(
                    natural_cmp(sorted[w.0], sorted[w.1]),
                    Ordering::Greater,
                    "transitivity violated at iteration {i}: {sorted:?}"
                );
            }
        }
    }

    // --- sort_vector: NaN last in BOTH directions (AC3) ---

    #[test]
    fn sort_vector_orders_ascending_with_nan_last() {
        let mut v = vec![
            sample(Some("m"), &[("i", "a")], f64::NAN),
            sample(Some("m"), &[("i", "b")], 3.0),
            sample(Some("m"), &[("i", "c")], 1.0),
        ];
        sort_vector(&mut v, false);
        let order: Vec<&str> = v.iter().map(|s| s.labels.get("i").unwrap()).collect();
        assert_eq!(order, vec!["c", "b", "a"], "ascending, NaN last");
    }

    #[test]
    fn sort_vector_orders_descending_with_nan_still_last() {
        let mut v = vec![
            sample(Some("m"), &[("i", "a")], f64::NAN),
            sample(Some("m"), &[("i", "b")], 3.0),
            sample(Some("m"), &[("i", "c")], 1.0),
        ];
        sort_vector(&mut v, true);
        let order: Vec<&str> = v.iter().map(|s| s.labels.get("i").unwrap()).collect();
        assert_eq!(order, vec!["b", "c", "a"], "descending, NaN still last");
    }

    /// Cross-metric value tie: broken by the virtual full label set, so
    /// `__name__` participates at its lexical position (deterministic in
    /// both directions).
    #[test]
    fn sort_vector_breaks_value_ties_by_the_full_label_set_including_name() {
        let mut v = vec![
            sample(Some("zzz"), &[("x", "1")], 5.0),
            sample(Some("aaa"), &[("x", "1")], 5.0),
        ];
        sort_vector(&mut v, false);
        assert_eq!(v[0].metric_name.as_deref(), Some("aaa"));
        sort_vector(&mut v, true);
        assert_eq!(
            v[0].metric_name.as_deref(),
            Some("aaa"),
            "the tie-break stays ascending in sort_desc too (values tie; only the value \
             comparison reverses)"
        );
    }

    // --- sort_by_label_vector: __name__ handling (AC10) ---

    /// Two series identical on the named label, differing only in
    /// `__name__`: the fallback compares the virtual full label set, so
    /// the lexically smaller metric name sorts first — and `desc`
    /// reverses the whole comparator.
    #[test]
    fn sort_by_label_breaks_full_ties_by_name_in_lexical_position_both_directions() {
        let mut v = vec![
            sample(Some("bbb"), &[("x", "1")], 1.0),
            sample(Some("aaa"), &[("x", "1")], 2.0),
        ];
        sort_by_label_vector(&mut v, &strings(&["x"]), false);
        assert_eq!(v[0].metric_name.as_deref(), Some("aaa"));
        sort_by_label_vector(&mut v, &strings(&["x"]), true);
        assert_eq!(v[0].metric_name.as_deref(), Some("bbb"), "desc reverses");
    }

    /// `__name__` lands at its LEXICAL key position, not unconditionally
    /// first: a `__aa`-keyed label (sorting before `__name__`) decides
    /// the fallback even against an opposing metric-name order.
    #[test]
    fn sort_by_label_full_tie_break_puts_name_at_its_lexical_position() {
        let mut v = vec![
            sample(Some("aaa"), &[("__aa", "z"), ("x", "1")], 1.0),
            sample(Some("zzz"), &[("__aa", "a"), ("x", "1")], 2.0),
        ];
        sort_by_label_vector(&mut v, &strings(&["x"]), false);
        assert_eq!(
            v[0].metric_name.as_deref(),
            Some("zzz"),
            "__aa sorts before __name__, so its value ('a' < 'z') decides — not the \
             metric name"
        );
        sort_by_label_vector(&mut v, &strings(&["x"]), true);
        assert_eq!(v[0].metric_name.as_deref(), Some("aaa"), "desc reverses");
    }

    /// A named label of `__name__` reads `metric_name` directly.
    #[test]
    fn sort_by_label_named_dunder_name_reads_metric_name() {
        let mut v = vec![
            sample(Some("m10"), &[("x", "a")], 1.0),
            sample(Some("m2"), &[("x", "b")], 2.0),
        ];
        sort_by_label_vector(&mut v, &strings(&["__name__"]), false);
        assert_eq!(
            v[0].metric_name.as_deref(),
            Some("m2"),
            "natural order: m2 before m10"
        );
    }

    // --- label_replace (AC4/AC9) ---

    #[test]
    fn label_replace_embedded_newline_matches_dot_under_the_dot_all_anchor() {
        // Plan v2 Δ1 golden: src = "a\nb"; regex "(a.b)" must match under
        // `^(?s:…)$` (a `\n`-free build of the regex would leave dst
        // unset). Inexpressible in the `.test` series grammar — pinned
        // here instead.
        let v = vec![sample(Some("m"), &[("src", "a\nb")], 1.0)];
        let out = label_replace_vector(v, "dst", "$1", "src", "(a.b)").unwrap();
        assert_eq!(out[0].labels.get("dst"), Some("a\nb"));
    }

    #[test]
    fn label_replace_expands_named_capture_groups() {
        // Plan v2 Δ4: `${name}` expansion parity with Go's Expand.
        let v = vec![sample(Some("m"), &[("src", "source-value-10")], 1.0)];
        let out = label_replace_vector(v, "dst", "${x}", "src", "(?P<x>.*)").unwrap();
        assert_eq!(out[0].labels.get("dst"), Some("source-value-10"));
    }

    #[test]
    fn label_replace_reads_and_writes_dunder_name_via_metric_name() {
        // src = __name__ reads metric_name; dst = __name__ writes it; an
        // empty expansion deletes it.
        let v = vec![sample(Some("old"), &[("k", "v")], 1.0)];
        let out = label_replace_vector(v, "__name__", "$1-new", "__name__", "(.*)").unwrap();
        assert_eq!(out[0].metric_name.as_deref(), Some("old-new"));
        let out = label_replace_vector(out, "__name__", "", "__name__", ".*").unwrap();
        assert_eq!(out[0].metric_name, None);
    }

    #[test]
    fn label_replace_invalid_regex_is_a_label_set_error_with_the_upstream_message() {
        let err = compile_label_replace_regex("(.*").unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid regular expression in label_replace(): (.*"
        );
    }

    #[test]
    fn label_replace_duplicate_full_identity_errors_with_the_upstream_message() {
        // The vendored functions.test:527 shape: deleting the only
        // distinguishing label collapses both series to one identity.
        let v = vec![
            sample(Some("m"), &[("src", "a"), ("keep", "x")], 1.0),
            sample(Some("m"), &[("src", "b"), ("keep", "x")], 2.0),
        ];
        let err = label_replace_vector(v, "src", "", "src", ".*").unwrap_err();
        assert!(matches!(err, PromqlError::LabelSet { .. }), "{err:?}");
        assert_eq!(
            err.to_string(),
            "vector cannot contain metrics with the same labelset"
        );
    }

    /// Plan v3 Δ5(b): equal non-name labels under DIFFERENT metric names
    /// are distinct full identities — never a false duplicate error.
    #[test]
    fn label_replace_equal_labels_under_different_metric_names_is_not_a_collision() {
        let v = vec![
            sample(Some("foo"), &[("src", "a")], 1.0),
            sample(Some("bar"), &[("src", "a")], 2.0),
        ];
        let out = label_replace_vector(v, "dst", "same", "src", ".*").unwrap();
        assert_eq!(out.len(), 2);
    }

    // --- re2_ascii_perl_classes (#68 review, finding 2): Go RE2's
    // \d/\w/\s are ASCII-only; Rust `regex` defaults them to Unicode ---

    #[test]
    fn perl_classes_are_ascii_only_matching_re2() {
        let re = compile_label_replace_regex(r"\d+").unwrap();
        assert!(re.is_match("123"));
        assert!(
            !re.is_match("\u{0663}"),
            "Arabic-Indic ٣ must NOT match \\d (Go RE2 parity)"
        );
        let re = compile_label_replace_regex(r"\w+").unwrap();
        assert!(re.is_match("a_9"));
        assert!(!re.is_match("é"), "\\w excludes Unicode letters under RE2");
        assert!(!re.is_match("д"));
        let re = compile_label_replace_regex(r"\s").unwrap();
        assert!(re.is_match(" "));
        assert!(re.is_match("\t"));
        assert!(!re.is_match("\u{a0}"), "NBSP is not RE2 \\s");
        assert!(
            !re.is_match("\u{b}"),
            "vertical tab is not RE2 \\s (RE2's \\s is [\\t\\n\\f\\r ])"
        );
    }

    #[test]
    fn negated_perl_classes_complement_the_ascii_sets() {
        let re = compile_label_replace_regex(r"\D").unwrap();
        assert!(
            re.is_match("\u{0663}"),
            "a non-ASCII digit IS \\D under RE2"
        );
        assert!(!re.is_match("7"));
        let re = compile_label_replace_regex(r"\W").unwrap();
        assert!(re.is_match("é"));
        assert!(!re.is_match("_"));
        let re = compile_label_replace_regex(r"\S").unwrap();
        assert!(re.is_match("\u{a0}"), "NBSP is \\S under RE2");
        assert!(!re.is_match(" "));
    }

    #[test]
    fn escaped_backslash_before_d_is_left_untouched() {
        // Pattern `\\d` = escaped backslash, then literal `d` — the
        // translator must not see the `\d` digit class here.
        let re = compile_label_replace_regex(r"\\d").unwrap();
        assert!(re.is_match(r"\d"));
        assert!(!re.is_match("5"), r"`\\d` is not the digit class");
        assert!(!re.is_match("d"));
    }

    #[test]
    fn perl_classes_inside_character_classes_translate_via_nested_classes() {
        let re = compile_label_replace_regex(r"[\d]+").unwrap();
        assert!(re.is_match("42"));
        assert!(!re.is_match("\u{0663}"));
        let re = compile_label_replace_regex(r"[a\d]+").unwrap();
        assert!(re.is_match("a1a"));
        assert!(!re.is_match("b"));
        // A negated class over a negated Perl class: [^\S] == RE2 \s.
        let re = compile_label_replace_regex(r"[^\S]+").unwrap();
        assert!(re.is_match(" \t"));
        assert!(!re.is_match("x"));
        assert!(!re.is_match("\u{a0}"));
        let re = compile_label_replace_regex(r"[a\D]").unwrap();
        assert!(re.is_match("\u{0663}"));
        assert!(re.is_match("a"));
        assert!(!re.is_match("7"));
    }

    #[test]
    fn word_boundaries_are_ascii_matching_re2() {
        // The reviewer's divergence case (#68 review round 2): under
        // Rust's default Unicode \b, 'e' and 'é' are both word chars —
        // no boundary, no match. RE2's ASCII \b sees a boundary ('é' is
        // not an ASCII word char), so Go matches "eé".
        let re = compile_label_replace_regex(r"e\bé").unwrap();
        assert!(re.is_match("eé"), r"ASCII \b must see a boundary before é");
        let re = compile_label_replace_regex(r"e\Bé").unwrap();
        assert!(!re.is_match("eé"), r"…and ASCII \B must not");
        // Plain ASCII behavior is unchanged.
        let re = compile_label_replace_regex(r".*\bword\b.*").unwrap();
        assert!(re.is_match("a word here"));
        assert!(!re.is_match("swordfish"));
    }

    #[test]
    fn class_interior_and_escaped_word_boundaries_are_not_boundaries() {
        // RE2 reads class-interior \b as a BACKSPACE literal — never a
        // boundary. The `regex` crate spells that `\x08` (it REJECTS
        // `[\b]` — pinned below — which is why "leave verbatim" is not
        // an option for a valid-RE2 input).
        // (Built via format! so clippy::invalid_regex doesn't reject the
        // deliberately-invalid literal at lint time.)
        let untranslated = format!("[{}b]", '\\');
        assert!(
            regex::Regex::new(&untranslated).is_err(),
            "premise check: the regex crate rejects class-interior \\b"
        );
        assert_eq!(re2_ascii_perl_classes(r"[\b]"), r"[\x08]");
        let re = compile_label_replace_regex(r"[\b]").unwrap();
        assert!(re.is_match("\u{8}"), "backspace semantics, as in RE2");
        assert!(!re.is_match("b"));
        // …including after other class members, and back OUTSIDE the
        // class the translation resumes.
        assert_eq!(re2_ascii_perl_classes(r"[a\b]x\b"), r"[a\x08]x(?-u:\b)");
        // Escaped backslash before b: literal `\` then `b`, untouched.
        assert_eq!(re2_ascii_perl_classes(r"\\b"), r"\\b");
        let re = compile_label_replace_regex(r"\\b").unwrap();
        assert!(re.is_match(r"\b"));
        assert!(!re.is_match("b"));
    }

    #[test]
    fn translation_passes_everything_else_through_verbatim() {
        assert_eq!(
            re2_ascii_perl_classes(r"a\.b(?i)ünïcode.*\n\x7f$1"),
            r"a\.b(?i)ünïcode.*\n\x7f$1"
        );
        assert_eq!(
            re2_ascii_perl_classes(r"\d[\w]\\S"),
            r"[0-9][[0-9A-Za-z_]]\\S"
        );
        // A trailing lone backslash still fails compilation (error parity).
        assert!(compile_label_replace_regex("\\").is_err());
    }

    // --- label_join (AC4/AC11) ---

    #[test]
    fn label_join_joins_in_order_and_reads_dunder_name() {
        let v = vec![sample(Some("m"), &[("a", "1"), ("b", "2")], 1.0)];
        let out =
            label_join_vector(v, "dst", "-", &strings(&["b", "a", "__name__", "missing"])).unwrap();
        assert_eq!(out[0].labels.get("dst"), Some("2-1-m-"));
    }

    #[test]
    fn label_join_duplicate_identity_errors_with_the_upstream_message() {
        // The vendored functions.test:591 `dup` shape.
        let v = vec![
            sample(Some("dup"), &[("label", "a"), ("this", "a")], 1.0),
            sample(Some("dup"), &[("label", "b"), ("this", "a")], 1.0),
        ];
        let err = label_join_vector(v, "label", "", &strings(&["this"])).unwrap_err();
        assert_eq!(
            err.to_string(),
            "vector cannot contain metrics with the same labelset"
        );
    }

    // --- labels_for_absent (AC5): shared synthesis, has/delete rule ---

    #[test]
    fn labels_for_absent_ports_the_has_delete_rule() {
        let m = |key: &str, op: MatchOp, value: &str| LabelMatcher {
            key: key.to_string(),
            op,
            value: value.to_string(),
        };
        // First-seen equality sets; a duplicate equality deletes.
        assert_eq!(
            labels_for_absent(&[m("a", MatchOp::Eq, "1"), m("a", MatchOp::Eq, "2")]),
            Labels::default()
        );
        // Equality then regex deletes; regex then equality keeps.
        assert_eq!(
            labels_for_absent(&[m("a", MatchOp::Eq, "1"), m("a", MatchOp::Re, "x")]),
            Labels::default()
        );
        assert_eq!(
            labels_for_absent(&[m("a", MatchOp::Re, "x"), m("a", MatchOp::Eq, "1")]),
            Labels::new(vec![("a".to_string(), "1".to_string())])
        );
        // Untouched second label survives; pure regex/negation add nothing.
        assert_eq!(
            labels_for_absent(&[
                m("a", MatchOp::Eq, "1"),
                m("a", MatchOp::Eq, "2"),
                m("instance", MatchOp::Eq, "127.0.0.1"),
                m("b", MatchOp::Nre, "y"),
            ]),
            Labels::new(vec![("instance".to_string(), "127.0.0.1".to_string())])
        );
    }
}
