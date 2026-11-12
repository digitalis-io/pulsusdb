//! Issue #839: what a pattern says about the strings it matches, read from
//! its parsed form.
//!
//! * [`anchored_matches_every_string`]: whether a stream matcher's anchored
//!   pattern matches every value, so the matcher can remove no stream.
//! * [`lowered_regex_literals`] and [`lowered_needle_literals`]: strings that
//!   the lower-cased line must contain for a line filter to keep it. The
//!   body's n-gram index is built on `lowerUTF8(body)`, and these are the
//!   strings a prefilter in front of the exact predicate looks up in it.
//!
//! Both read the pattern as the database's `match` and the reference read
//! it: through [`crate::re2_pattern_to_rust`], with `.` matching a newline.

/// Stub (issue #839, tests first).
pub fn anchored_matches_every_string(_pattern: &str) -> bool {
    false
}

/// Stub (issue #839, tests first).
pub fn lowered_regex_literals(_pattern: &str) -> Option<Vec<Vec<String>>> {
    None
}

/// Stub (issue #839, tests first).
pub fn lowered_needle_literals(_needle: &str) -> Vec<String> {
    Vec::new()
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
        ];
        let mut wrong: Vec<String> = Vec::new();
        for (pattern, want) in &patterns {
            let got = lowered_regex_literals(pattern);
            if &got != want {
                wrong.push(format!("regex {pattern:?}: got {got:?}, expected {want:?}"));
            }
        }
        let needles: [(&str, &[&str]); 6] = [
            ("Connection refused", &["connection refused"]),
            ("Größe des Puffers", &["e des puffers"]),
            ("接続エラー", &["接続エラー"]),
            ("abc", &[]),
            ("connection", &["connection"]),
            ("Größe", &[]),
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
