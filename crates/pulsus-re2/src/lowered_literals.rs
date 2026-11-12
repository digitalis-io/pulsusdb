//! Issue #839: what a pattern says about the strings it matches, read from
//! its parsed form.
//!
//! * [`anchored_matches_every_string`]: whether a stream matcher's anchored
//!   pattern matches every value, so the matcher can remove no stream.
//! * [`lowered_regex_literals`] and [`lowered_needle_literals`]: strings
//!   whose lowering the lower-cased line must contain for a line filter to
//!   keep it. The body's n-gram index is built on `lowerUTF8(body)`, and
//!   these are the strings a check in front of the exact predicate looks up
//!   in it. ASCII is lowered here; any other character is left for the
//!   database to lower (issue #839 addendum, F2).
//!
//! Both read the pattern as the database's `match` and the reference read
//! it: through [`crate::re2_pattern_to_rust`], with `.` matching a newline.

use regex_syntax::hir::{Class, Hir, HirKind};

/// The fewest characters a run must have to become a string: the n-gram
/// length of `idx_body_ngrams`.
const MIN_RUN_CHARS: usize = 4;
/// A result with more OR sets than this is `None` (addendum F3).
pub const LOWERED_MAX_OR_SETS: usize = 64;
/// A result with more strings than this, over all its sets, is `None`.
pub const LOWERED_MAX_STRINGS: usize = 256;
/// A run keeps its first this many bytes, cut at a character boundary.
const MAX_RUN_BYTES: usize = 256;

/// An OR of AND sets of strings.
type Sets = Vec<Vec<String>>;

fn parse(pattern: &str) -> Option<Hir> {
    let translated = crate::re2_pattern_to_rust(pattern);
    regex_syntax::ParserBuilder::new()
        .dot_matches_new_line(true)
        .build()
        .parse(&translated)
        .ok()
}

/// Whether the anchored pattern `^(?:pattern)$` matches every string,
/// newlines included. True iff the parsed pattern, `Capture` wrappers
/// removed, is a `Repetition { min: 0, max: None }` over a class holding
/// every `char`, or an alternation one of whose branches is. A pattern that
/// does not parse answers `false`.
pub fn anchored_matches_every_string(pattern: &str) -> bool {
    fn strip(hir: &Hir) -> &Hir {
        match hir.kind() {
            HirKind::Capture(c) => strip(&c.sub),
            _ => hir,
        }
    }
    fn any_star(hir: &Hir) -> bool {
        match strip(hir).kind() {
            HirKind::Repetition(rep) if rep.min == 0 && rep.max.is_none() => {
                matches!(strip(&rep.sub).kind(), HirKind::Class(Class::Unicode(class))
                    if class.ranges().len() == 1
                        && class.ranges()[0].start() == '\0'
                        && class.ranges()[0].end() == char::MAX)
            }
            _ => false,
        }
    }
    let Some(hir) = parse(pattern) else {
        return false;
    };
    match strip(&hir).kind() {
        HirKind::Alternation(branches) => branches.iter().any(any_star),
        _ => any_star(&hir),
    }
}

/// Strings whose lowering `lowerUTF8(body)` contains on every line the
/// unanchored pattern matches: an OR of AND sets, with this guarantee — for
/// any line the pattern matches, the lower-cased line contains the lowering
/// of every string of at least one set. `None` when no such strings are
/// derived.
pub fn lowered_regex_literals(pattern: &str) -> Option<Vec<Vec<String>>> {
    let hir = parse(pattern)?;
    walk(&hir)
}

/// The needle's runs of four or more run characters, ASCII lower-cased: one
/// AND set, empty when the needle has no such run.
pub fn lowered_needle_literals(needle: &str) -> Vec<String> {
    let mut runs = Runs::default();
    for c in needle.chars() {
        runs.push(run_char(c));
    }
    runs.finish()
}

/// A character the database lowers the same way whatever surrounds it:
/// every character but U+03A3 `Σ`, whose lowering depends on the next one
/// (addendum F2, measured over every code point). ASCII is lowered here; the
/// rest is kept as written and lowered by the database.
fn run_char(c: char) -> Option<char> {
    if c == '\u{3A3}' {
        None
    } else {
        Some(c.to_ascii_lowercase())
    }
}

/// The run character a class stands for: a class equal to `{c, C}` for an
/// ASCII letter is `c`, and `{k, K, U+212A}` is `k`, which the database
/// lowers to `k`. `None` for every other class (`{s, S, U+017F}` among them:
/// `lowerUTF8` leaves U+017F as it is).
fn class_run_char(class: &Class) -> Option<char> {
    let Class::Unicode(class) = class else {
        return None;
    };
    let mut chars = Vec::new();
    for range in class.ranges() {
        if range.start() != range.end() || chars.len() == 3 {
            return None;
        }
        chars.push(range.start());
    }
    match chars.as_slice() {
        [one] => run_char(*one),
        [upper, lower] if upper.is_ascii_uppercase() && upper.to_ascii_lowercase() == *lower => {
            Some(*lower)
        }
        ['K', 'k', '\u{212A}'] => Some('k'),
        _ => None,
    }
}

/// The runs of a concatenation, as its items are read in order.
#[derive(Default)]
struct Runs {
    current: String,
    chars: usize,
    done: Vec<String>,
}

impl Runs {
    fn push(&mut self, c: Option<char>) {
        match c {
            Some(c) => {
                self.current.push(c);
                self.chars += 1;
            }
            None => self.end(),
        }
    }

    fn end(&mut self) {
        let mut run = std::mem::take(&mut self.current);
        if self.chars >= MIN_RUN_CHARS {
            if run.len() > MAX_RUN_BYTES {
                let mut cut = MAX_RUN_BYTES;
                while !run.is_char_boundary(cut) {
                    cut -= 1;
                }
                run.truncate(cut);
            }
            self.done.push(run);
        }
        self.chars = 0;
    }

    fn finish(mut self) -> Vec<String> {
        self.end();
        self.done
    }
}

/// The item's characters into `runs`, or `false` when the item is not a
/// literal or a class.
fn push_chars(hir: &Hir, runs: &mut Runs) -> bool {
    match hir.kind() {
        HirKind::Literal(lit) => {
            for c in String::from_utf8_lossy(&lit.0).chars() {
                runs.push(run_char(c));
            }
            true
        }
        HirKind::Class(class) => {
            runs.push(class_run_char(class));
            true
        }
        _ => false,
    }
}

/// `None` is "no constraint".
fn walk(hir: &Hir) -> Option<Sets> {
    match hir.kind() {
        HirKind::Literal(_) | HirKind::Class(_) => concat(std::slice::from_ref(hir)),
        HirKind::Concat(items) => concat(items),
        HirKind::Alternation(branches) => {
            let mut out: Sets = Vec::new();
            for branch in branches {
                out.extend(walk(branch)?);
                if !within_limits(&out) {
                    return None;
                }
            }
            bounded(out)
        }
        HirKind::Capture(c) => walk(&c.sub),
        HirKind::Repetition(rep) if rep.min >= 1 => walk(&rep.sub),
        HirKind::Repetition(_) | HirKind::Look(_) | HirKind::Empty => None,
    }
}

/// AND of the finished runs and of each other item's result.
fn concat(items: &[Hir]) -> Option<Sets> {
    let mut runs = Runs::default();
    let mut factors: Vec<Sets> = Vec::new();
    for item in items {
        if !push_chars(item, &mut runs) {
            runs.end();
            if let Some(sets) = walk(item) {
                factors.push(sets);
            }
        }
    }
    let mut acc: Sets = vec![runs.finish()];
    for factor in factors {
        let mut next: Sets = Vec::with_capacity(acc.len() * factor.len());
        for a in &acc {
            for b in &factor {
                let mut set = a.clone();
                set.extend(b.iter().cloned());
                next.push(set);
            }
        }
        acc = next;
        if !within_limits(&acc) {
            return None;
        }
    }
    bounded(acc)
}

fn within_limits(sets: &Sets) -> bool {
    sets.len() <= LOWERED_MAX_OR_SETS
        && sets.iter().map(Vec::len).sum::<usize>() <= LOWERED_MAX_STRINGS
}

/// `None` when a set has no string, which makes the OR always true, or the
/// result is past the limits.
fn bounded(sets: Sets) -> Option<Sets> {
    (!sets.is_empty() && sets.iter().all(|s| !s.is_empty()) && within_limits(&sets)).then_some(sets)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sets(v: &[&[&str]]) -> Option<Vec<Vec<String>>> {
        Some(
            v.iter()
                .map(|set| set.iter().map(|s| s.to_string()).collect())
                .collect(),
        )
    }

    /// T1 (issue #839): the strings derived for every pattern and needle of
    /// the plan's prefilter table, and three shapes beside it.
    #[test]
    fn lowered_literals_are_derived_as_the_rules_say() {
        let patterns: Vec<(&str, Option<Vec<Vec<String>>>)> = vec![
            (
                "(?i)(error|fatal|panic|exception|fail)",
                sets(&[
                    &["error"],
                    &["fatal"],
                    &["panic"],
                    &["exception"],
                    &["fail"],
                ]),
            ),
            // `s` breaks the run under `(?i)`: it also matches U+017F, which
            // the database does not lower. So `reset` gives no string.
            ("(?i)connection.*reset", sets(&[&["connection"]])),
            ("^ERROR: ", sets(&[&["error: "]])),
            ("(?i)status", sets(&[&["tatu"]])),
            ("(?i)kubelet", sets(&[&["kubelet"]])),
            ("foo(bar)?bazz", sets(&[&["bazz"]])),
            ("(?:error){2}", sets(&[&["error"]])),
            ("(error|fail)ed", sets(&[&["error"], &["fail"]])),
            ("x+error", sets(&[&["error"]])),
            ("err.*", None),
            ("(?i)(error|ok)", None),
            ("(?i)task", None),
            ("(?i)ΣΦΑΛΜΑ", None),
            ("Größe", sets(&[&["größe"]])),
            ("(?i)größe", None),
            ("ΟΔΟΣ", None),
        ];
        let mut wrong: Vec<String> = Vec::new();
        for (pattern, want) in &patterns {
            let got = lowered_regex_literals(pattern);
            if &got != want {
                wrong.push(format!("regex {pattern:?}: got {got:?}, expected {want:?}"));
            }
        }
        let long = "x".repeat(300);
        let long_cut = "x".repeat(256);
        let cyrillic_cut = "ж".repeat(128);
        let cyrillic = "Ж".repeat(200);
        let needles: Vec<(&str, Vec<String>)> = vec![
            ("Connection refused", vec!["connection refused".into()]),
            // Issue #839 addendum F2: any character but U+03A3 joins a run;
            // ASCII is lowered here, the rest by the database.
            ("Größe des Puffers", vec!["größe des puffers".into()]),
            ("接続エラー", vec!["接続エラー".into()]),
            ("abc", vec![]),
            ("connection", vec!["connection".into()]),
            ("Größe", vec!["größe".into()]),
            ("Ошибка", vec!["Ошибка".into()]),
            ("ΟΔΟΣ οδος", vec![" οδος".into()]),
            // F3: the first 256 bytes of a run, cut at a character boundary.
            (&long, vec![long_cut.clone()]),
            (&cyrillic, vec![cyrillic_cut.to_uppercase()]),
        ];
        for (needle, want) in needles {
            let got = lowered_needle_literals(needle);
            if got != want {
                wrong.push(format!("needle {needle:?}: got {got:?}, expected {want:?}"));
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// T2 (issue #839): the selector matchers whose anchored pattern matches
    /// every string, and the ones that must keep their branch.
    #[test]
    fn anchored_patterns_that_match_every_string_are_recognised() {
        let dropped = [".*", "(?s).*", ".*?", "(.*)", "x|.*"];
        let kept = ["(?-s).*", "", "(?i)", ".+", ".*x"];
        let wrong: Vec<String> = dropped
            .iter()
            .filter(|p| !anchored_matches_every_string(p))
            .map(|p| format!("{p:?} matches every string"))
            .chain(
                kept.iter()
                    .filter(|p| anchored_matches_every_string(p))
                    .map(|p| format!("{p:?} does not")),
            )
            .collect();
        assert!(wrong.is_empty(), "{wrong:#?}");
    }
}
