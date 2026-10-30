//! The word rule of `word(…)` (issue #757): the rule of the database's
//! `splitByNonAlpha` tokenizer over `lowerUTF8(body)`, the expression
//! `idx_body_words` indexes.

/// An ASCII letter or digit, or any byte of a non-ASCII character.
pub fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b >= 0x80
}

/// A `word(…)` argument: one or more word bytes and nothing else.
pub fn is_one_word(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(is_word_byte)
}

/// `lowerUTF8`: each character lowered on its own, with no context.
pub fn lower(s: &str) -> String {
    s.chars().flat_map(char::to_lowercase).collect()
}

/// Stub for plan r4: r1's lowering, until the case key lands.
pub fn case_key(s: &str) -> String {
    lower(s)
}

/// Whether every one of `lowered` (each already [`lower`]ed) is a word of `line`.
pub fn has_all_words(line: &str, lowered: &[String]) -> bool {
    let line = lower(line);
    let words: std::collections::HashSet<&[u8]> = line
        .as_bytes()
        .split(|b| !is_word_byte(*b))
        .filter(|w| !w.is_empty())
        .collect();
    lowered.iter().all(|w| words.contains(w.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #757 H5: every expected answer is written here by hand from
    /// the word rule, not computed.
    #[test]
    fn has_all_words_matches_a_hand_written_table() {
        let table: &[(&str, &[&str], bool)] = &[
            ("Memcached: ok", &["memcached"], true),
            ("xmemcached", &["memcached"], false),
            ("memcached123", &["memcached"], false),
            ("foo_memcached", &["memcached"], true),
            ("memcached.local", &["memcached"], true),
            ("«memcached»", &["memcached"], false),
            ("ÜNÏCODE", &["ünïcode"], true),
            ("ΟΔΟΣ", &["ΟΔΟΣ"], true),
            ("ΟΔΟΣ", &["οδος"], true),
            ("ΟΔΟΣ", &["οδοσ"], true),
            ("ΑΣ.Β", &["ΑΣ"], true),
            ("ΑΣ.Β", &["ας"], true),
            ("ΑΣ.Β", &["ασ"], true),
            ("Α'Σ", &["ς"], true),
            ("Α'Σ", &["Σ"], true),
            ("Straße", &["STRASSE"], true),
            ("STRASSE", &["straße"], true),
            ("ϐ", &["β"], true),
            ("\u{212A}elvin", &["kelvin"], true),
            ("İstanbul", &["istanbul"], false),
            ("İstanbul", &["İSTANBUL"], true),
            ("refused: Connection", &["connection", "refused"], true),
            ("connection pool", &["connection", "refused"], false),
            ("a\u{0}b", &["a"], true),
            ("memcached", &[], false),
        ];
        for (line, ws, want) in table {
            let keyed: Vec<String> = ws.iter().map(|w| case_key(w)).collect();
            assert_eq!(
                has_all_words(line, &keyed),
                *want,
                "line {line:?}, words {ws:?}"
            );
        }
    }
}
