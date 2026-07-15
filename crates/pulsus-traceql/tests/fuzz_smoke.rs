//! Fuzz-smoke test: feeds `parse()` a large number of pseudo-random byte
//! strings plus randomly-mutated valid queries and asserts it never
//! panics (`Ok` or `Err` are both acceptable outcomes — only a panic
//! fails this test). Uses a hand-rolled, deterministically-seeded
//! xorshift64 PRNG — the same pattern as `pulsus-logql`'s fuzz smoke —
//! so no `rand` dependency and no `SystemTime` seed. Plus a fixed set of
//! adversarial cases (deep nesting, unterminated strings, giant and
//! fractional durations, lone operators, empty input).

use pulsus_traceql::parse;

/// A cheap, non-cryptographic xorshift64 PRNG — deterministic (fixed
/// seed) so this test is reproducible in CI.
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

/// A TraceQL-biased alphabet: mostly the characters a real query is
/// built from, so random strings occasionally stumble into interesting
/// near-valid shapes, plus a few bytes to exercise the lexer's
/// stray-byte and multi-byte-UTF8 error paths.
const ALPHABET: &[char] = &[
    '{', '}', '(', ')', '[', ']', ',', '.', '=', '!', '~', '|', '&', '"', '`', '\\', '_', 'a', 'b',
    'c', 'd', 'e', 'k', 'm', 'n', 'o', 'r', 's', 't', 'u', 'v', 'x', 'h', '0', '1', '2', '3', '5',
    '9', ' ', '+', '-', '*', '/', '<', '>', '\n', 'µ',
];

fn random_string(rng: &mut XorShift64, max_len: usize) -> String {
    let len = rng.next_range(max_len + 1);
    (0..len)
        .map(|_| ALPHABET[rng.next_range(ALPHABET.len())])
        .collect()
}

const SEED_QUERIES: &[&str] = &[
    "{}",
    r#"{ name = "GET /api/orders" }"#,
    "{ status = error && kind = server }",
    "{ duration > 1.5s || duration < .5s }",
    r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s }"#,
    r#"{ .env != "prod" && span.retried != false }"#,
    r#"{ span.http.url =~ "/api/.*" }"#,
    "({ .a = 1 } || { .b = 2 }) && { .c = 3 }",
    "{ status = error } | count() > 3",
    "{} | avg(duration) > 100ms | select(name, span.http.status_code)",
];

/// Randomly deletes, duplicates, or substitutes a handful of characters
/// in a valid query — cheap "near-valid" mutation without a full
/// grammar-aware mutator, biased to hit boundary conditions (truncated
/// tokens, duplicated operators, broken literals) that pure-random
/// strings rarely reach.
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
    let mut deep_parens = String::from("{ ");
    for _ in 0..10_000 {
        deep_parens.push('(');
    }
    deep_parens.push_str(".a = 1");
    for _ in 0..10_000 {
        deep_parens.push(')');
    }
    deep_parens.push_str(" }");

    let mut deep_spansets = String::new();
    for _ in 0..10_000 {
        deep_spansets.push('(');
    }
    deep_spansets.push_str("{ .a = 1 }");
    for _ in 0..10_000 {
        deep_spansets.push(')');
    }

    // Paren-free flat boolean chains: the binary-node budget must turn
    // these into clean errors, never a boxed AST that overflows the
    // stack in Display/Drop.
    let mut flat_field_chain = String::from("{ .a = 1");
    for _ in 0..100_000 {
        flat_field_chain.push_str(" && .a = 1");
    }
    flat_field_chain.push_str(" }");

    let mut flat_spanset_chain = String::from("{}");
    for _ in 0..100_000 {
        flat_spanset_chain.push_str(" || {}");
    }

    let cases = [
        "",
        "{",
        "}",
        "{}",
        "{ .a",
        "{ .a = 1",
        r#"{ name = "unterminated"#,
        "`unterminated",
        "{ duration > 99999999999999999999999999h }",
        "{ duration > 0.1ns }",
        "{ duration > 1.999999999999999999999999999999999999999s }",
        "{ duration > .s }",
        "{ duration > 1h30m5s }",
        "!",
        "!=",
        "!~",
        "&",
        "&&",
        "|",
        "||",
        "=",
        "=~",
        "~",
        ">>",
        "<<",
        "((((((((((",
        "))))))))))",
        "[[[[[[[[[[",
        "]]]]]]]]]]",
        "..........",
        deep_parens.as_str(),
        deep_spansets.as_str(),
        flat_field_chain.as_str(),
        flat_spanset_chain.as_str(),
        r#"{ name = "\z" }"#,
        r#"{ name = "\x" }"#,
        r#"{ name = "\xf" }"#,
        r#"{ name = "\xff\377\uD800\UFFFFFFFF" }"#,
        r#"{ name = "\"#,
        r#"{ name = "\u12"#,
        "\0\0\0\0",
        "日本語日本語日本語",
        r#"{ name = "日本語" && .テスト = "µµµ" }"#,
        "{ .a = 1 } | | count() > 1",
    ];
    for case in cases {
        let _ = parse(case);
    }
}
