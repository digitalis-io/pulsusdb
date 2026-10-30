//! The word rule of `word(…)` (issue #757): the rule of the database's
//! `splitByNonAlpha` tokenizer over `lowerUTF8(body)`, the expression
//! `idx_body_words` indexes.

/// An ASCII letter or digit, or any byte of a non-ASCII character.
pub fn is_word_byte(_b: u8) -> bool {
    false
}

/// A `word(…)` argument: one or more word bytes and nothing else.
pub fn is_one_word(_s: &str) -> bool {
    false
}

/// `lowerUTF8`: each character lowered on its own, with no context.
pub fn lower(_s: &str) -> String {
    String::new()
}

/// Whether every one of `lowered` (each already [`lower`]ed) is a word of `line`.
pub fn has_all_words(_line: &str, _lowered: &[String]) -> bool {
    false
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
            ("ΟΔΟΣ", &["οδος"], false),
            ("\u{212A}elvin", &["kelvin"], true),
            ("İstanbul", &["istanbul"], false),
            ("İstanbul", &["İSTANBUL"], true),
            ("refused: Connection", &["connection", "refused"], true),
            ("connection pool", &["connection", "refused"], false),
            ("a\u{0}b", &["a"], true),
        ];
        for (line, ws, want) in table {
            let lowered: Vec<String> = ws.iter().map(|w| lower(w)).collect();
            assert_eq!(
                has_all_words(line, &lowered),
                *want,
                "line {line:?}, words {ws:?}"
            );
        }
    }
}
