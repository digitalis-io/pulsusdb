//! Fuzz-smoke test: feeds `parse()` a large number of pseudo-random byte
//! strings plus randomly-mutated valid queries and asserts it never
//! panics (`Ok` or `Err` are both acceptable outcomes — only a panic
//! fails this test). Uses a hand-rolled, deterministically-seeded
//! xorshift64 PRNG — the same pattern as
//! `pulsus-write::writer::table::XorShift64` for its jitter delays — so
//! no `rand` dependency and no `SystemTime` seed (architect plan: "fixed
//! seed... not SystemTime", lean-deps ethos). Plus a fixed set of
//! adversarial cases the review cycles called out by name (deep nesting,
//! unterminated strings, giant durations, lone operators, empty input).

use pulsus_logql::parse;

/// A cheap, non-cryptographic xorshift64 PRNG — deterministic (fixed
/// seed) so this test is reproducible in CI, not `pulsus-write`'s
/// `SystemTime`-seeded variant (that seeding is for spreading real retry
/// jitter; a fuzz smoke test wants a stable, replayable sequence).
struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        // A xorshift generator's state must never be zero (it is a fixed
        // point).
        XorShift64(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn next_range(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

/// A LogQL-biased alphabet: mostly the characters a real query is built
/// from, so random strings occasionally stumble into interesting
/// near-valid shapes, plus a few bytes to exercise the lexer's
/// stray-byte and multi-byte-UTF8 error paths.
const ALPHABET: &[char] = &[
    '{', '}', '(', ')', '[', ']', ',', '=', '!', '~', '|', '"', '`', '\\', '_', 'a', 'b', 'c', 'd',
    'e', 'm', 's', 'h', 'n', 'r', 't', 'u', 'x', 'y', '0', '1', '2', '3', '5', '9', '.', ' ', '+',
    '-', '*', '/', '%', '^', '<', '>', '\n', 'µ',
];

fn random_string(rng: &mut XorShift64, max_len: usize) -> String {
    let len = rng.next_range(max_len + 1);
    (0..len)
        .map(|_| ALPHABET[rng.next_range(ALPHABET.len())])
        .collect()
}

const SEED_QUERIES: &[&str] = &[
    r#"{app="x"}"#,
    r#"{app="x", env!="prod"}"#,
    r#"{app=~"x.*"}"#,
    r#"{app="x"} |= "a" != "b" |~ "c" !~ "d""#,
    r#"rate({app="x"}[5m])"#,
    r#"count_over_time({app="x"}[1h30m])"#,
    r#"bytes_rate({app="x"}[500ms])"#,
    r#"sum by(app)(rate({app="x"}[5m]))"#,
    r#"avg without(app, env)(count_over_time({app="x"}[5m])) "#,
    r#"sum(rate({app="x"}[5m])) by(app)"#,
];

/// Randomly deletes, duplicates, or substitutes a handful of bytes in a
/// valid query — cheap "near-valid" mutation without a full grammar-aware
/// mutator, biased to hit boundary conditions (truncated tokens,
/// duplicated operators, broken UTF-8) that pure-random strings rarely
/// reach.
fn mutate(rng: &mut XorShift64, base: &str) -> String {
    let mut chars: Vec<char> = base.chars().collect();
    if chars.is_empty() {
        return String::new();
    }
    let mutations = 1 + rng.next_range(3);
    for _ in 0..mutations {
        if chars.is_empty() {
            break;
        }
        let idx = rng.next_range(chars.len());
        match rng.next_range(3) {
            0 => {
                chars.remove(idx);
            }
            1 => {
                let c = chars[idx];
                chars.insert(idx, c);
            }
            _ => {
                chars[idx] = ALPHABET[rng.next_range(ALPHABET.len())];
            }
        }
    }
    chars.into_iter().collect()
}

#[test]
fn parse_never_panics_on_pseudo_random_byte_strings() {
    let mut rng = XorShift64::new(0x9E37_79B9_7F4A_7C15);
    for _ in 0..10_000 {
        let input = random_string(&mut rng, 40);
        // Only the absence of a panic is asserted — Ok/Err are both
        // legitimate outcomes for arbitrary input.
        let _ = parse(&input);
    }
}

#[test]
fn parse_never_panics_on_mutated_valid_queries() {
    let mut rng = XorShift64::new(0xC2B2_AE3D_27D4_EB4F);
    for _ in 0..5_000 {
        let base = SEED_QUERIES[rng.next_range(SEED_QUERIES.len())];
        let mutated = mutate(&mut rng, base);
        let _ = parse(&mutated);
    }
}

#[test]
fn parse_never_panics_on_fixed_adversarial_cases() {
    let mut deep = String::new();
    for _ in 0..10_000 {
        deep.push_str("sum(");
    }
    deep.push_str(r#"count_over_time({a="b"}[5m])"#);
    for _ in 0..10_000 {
        deep.push(')');
    }

    let cases = [
        "",
        "{",
        "}",
        "{}",
        "{a",
        r#"{a="b""#,
        r#"{a="unterminated"#,
        "`unterminated",
        r#"rate({a="b"}[99999999999999999999999999y])"#,
        "!",
        "!=",
        "!~",
        "|",
        "|=",
        "|~",
        "=",
        "=~",
        "((((((((((",
        "))))))))))",
        "[[[[[[[[[[",
        "]]]]]]]]]]",
        deep.as_str(),
        "\0\0\0\0",
        "日本語日本語日本語",
        r#"{app="日本語", env=~"テスト.*"}"#,
    ];
    for case in cases {
        let _ = parse(case);
    }
}
