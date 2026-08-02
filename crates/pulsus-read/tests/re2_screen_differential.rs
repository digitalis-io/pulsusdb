//! Issue #309: the corpus differential behind the RE2-authority screen
//! (`metrics::re2_authority`).
//!
//! The screen decides which matcher patterns the warm label cache may
//! evaluate in-process. It is sound only if it is **conservative in one
//! direction**: every pattern the Rust `regex` crate accepts but RE2
//! rejects must be deferred to storage, while patterns RE2 accepts may be
//! deferred or not (a false positive costs a round-trip, never a
//! rejection). Unit tests in the screen's own module pin the divergence
//! classes we know about; this suite is the evidence that the list is
//! *complete* — it replays a corpus against a real RE2 and asserts zero
//! escapes.
//!
//! Two halves, deliberately split by what they need:
//!
//! * **Hermetic** — the generated half of the corpus is reproduced from its
//!   seed and compared byte-for-byte against the committed fixture (the
//!   whole file, header and terminators included), so the fixture cannot
//!   drift from the generator that made it. Runs in the plain
//!   `cargo test --workspace` lane. The curated half is a hand list, so it
//!   is checked for *content* — that the named divergence classes are still
//!   present — never for byte identity, which would mean nothing there.
//! * **Live** (`PULSUS_TEST_CLICKHOUSE=1`) — three crossings against a real
//!   RE2, none of which can be hermetic because there is no in-process RE2:
//!   * **acceptance** (issue #309) — every corpus pattern the in-process
//!     path can compile is compiled by ClickHouse's RE2 (`SELECT match('x',
//!     …)` over the literal the read path actually renders) and the two
//!     verdicts are crossed with the screen's;
//!   * **meaning** (issue #317) — every corpus pattern *both* engines
//!     accept is evaluated against a fixed subject alphabet by both, and
//!     the two answers must agree bit for bit. This is what proves the
//!     `pulsus_promql::re2_pattern_to_rust` rewrite: the same test with the
//!     rewrite removed reports the divergences it exists to close.
//!   * **SQL meaning** (issue #324) — for every corpus pattern RE2 accepts,
//!     `match()` — the function the read path's predicates use, and which
//!     goes through ClickHouse's `OptimizedRegularExpression` wrapper
//!     before reaching RE2 — must select the same subjects as RE2 itself.
//!     It did not: `match('\n', '^(?:.)$')` is `1` while `replaceRegexpOne`
//!     on the same inputs reports no match, because the wrapper sets RE2's
//!     `dot_nl` option. This crossing is over the **rendered** literal
//!     (`metrics::anchored_re2_literal_for_test`), so it pins the fix
//!     — the `(?-s)` prefix — rather than a hand-copy of it, and it covers
//!     screened patterns too: those are precisely the ones that reach SQL.
//!     Issue #331 rides the same crossing: the flag-group-head defect's
//!     workaround renderings must agree with RE2 corpus-wide, the
//!     underlying SERVER defect is pinned separately by the raw-probe
//!     registry at the bottom of this file (rot-checked in both
//!     directions), and two hermetic tests prove the workaround leaves
//!     unaffected patterns byte-identical and preserves compilability.
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test re2_screen_differential
//! ```
//!
//! Regenerating the fixture after a token/seed change:
//! `PULSUS_REGEN_RE2_CORPUS=1 cargo test -p pulsus-read --test
//! re2_screen_differential generated_corpus` — then review the diff.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChError, ChProto, Idempotency, QuerySettings};
use pulsus_read::metrics::anchored_re2_literal_for_test as rendered_sql_pattern;
use pulsus_read::metrics::pattern_requires_re2_authority_for_test as screened;

/// Fragments the two engines have opinions about — ordinary regex syntax,
/// plus every construct where their accepted grammars are known or
/// suspected to differ. Random concatenations of these reach combinations a
/// hand-written list does not.
const TOKENS: &[&str] = &[
    "a",
    "b",
    "z",
    "0",
    "9",
    ".",
    "-",
    "_",
    "/",
    ":",
    "*",
    "+",
    "?",
    "|",
    "^",
    "$",
    "(",
    ")",
    "(?:",
    "(?i)",
    "(?i:",
    "(?x)",
    "(?u:",
    "(?P<n>",
    "(?<n>",
    "[",
    "]",
    "[^",
    "a-z",
    "0-9",
    "[:alpha:]",
    "&&",
    "--",
    "~~",
    "{2}",
    "{2,}",
    "{2,3}",
    "{1000}",
    "{1001}",
    "{0,2000}",
    "{bbb}",
    "{",
    "}",
    "{,5}",
    "\\d",
    "\\w",
    "\\s",
    "\\D",
    "\\S",
    "\\W",
    "\\b",
    "\\B",
    "\\A",
    "\\z",
    "\\p{L}",
    "\\p{Alphabetic}",
    "\\pN",
    "\\P{Nd}",
    "\\p{Greek}",
    "\\u{41}",
    "\\U00000041",
    "\\x41",
    "\\x{41}",
    "\\<",
    "\\>",
    "\\b{start}",
    "\\.",
    "\\*",
    "\\\\",
    "\\{",
    "\\[",
    "\\Q",
    "\\E",
    "\\1",
    "\\C",
    // Issue #324's subject: `.`, the `s` flag that turns it into
    // "everything", and a literal newline escape. `(?s)` must keep working
    // (it is the user's own opt-in) even though the rendered pattern now
    // carries a leading `(?-s)`.
    "(?s)",
    "(?s:",
    "\\n",
    "[^\\n]",
    // Issue #331's subject: the flag-group heads ClickHouse's
    // `match()` analyzer leaks into its required substring (every
    // no-`i` head), plus the rewrite's own output shape so the corpus
    // also reaches patterns where the user already wrote `-i`.
    "(?m)",
    "(?m:",
    "(?U)",
    "(?U:",
    "(?-s)",
    "(?s-i:",
];

/// Frozen with the fixture: changing either invalidates the committed
/// corpus, and the hermetic test says so.
const SEED: u64 = 0x0000_0000_0000_0309;
const GENERATED_PATTERNS: usize = 4_000;
const MAX_TOKENS_PER_PATTERN: u64 = 6;

/// splitmix64 — the workspace's no-`rand` determinism convention
/// (`xtask/src/bench/dataset.rs`), so a committed corpus stays
/// byte-reproducible across dependency bumps.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The generated half of the corpus: [`GENERATED_PATTERNS`] distinct
/// concatenations of 1..=[`MAX_TOKENS_PER_PATTERN`] tokens, in generation
/// order, deduped. Pure function of [`SEED`] and [`TOKENS`].
fn generated_patterns() -> Vec<String> {
    let mut state = SEED;
    let mut next = || {
        state = state.wrapping_add(1);
        splitmix64(state)
    };
    let mut seen: HashSet<String> = HashSet::new();
    let mut out = Vec::with_capacity(GENERATED_PATTERNS);
    while out.len() < GENERATED_PATTERNS {
        let n = next() % MAX_TOKENS_PER_PATTERN + 1;
        let mut p = String::new();
        for _ in 0..n {
            p.push_str(TOKENS[(next() % TOKENS.len() as u64) as usize]);
        }
        if seen.insert(p.clone()) {
            out.push(p);
        }
    }
    out
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/re2_screen")
}

fn read_file(name: &str) -> String {
    let path = fixture_dir().join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"))
}

/// The corpus records: one pattern per line, with blank lines and `#`
/// comments dropped as prose. Used to *drive* the differential — never to
/// check the fixture against its generator, which is a byte comparison
/// ([`rendered_generated_fixture`]).
fn read_corpus(name: &str) -> Vec<String> {
    read_file(name)
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect()
}

/// The exact bytes `generated.txt` must contain — header included. Single
/// source of truth: the regeneration path writes this, and the drift check
/// compares against this, so the two can never disagree about what the
/// fixture should be.
fn rendered_generated_fixture() -> String {
    let header = format!(
        "# Issue #309 RE2-authority screen differential corpus (generated half).\n\
         # Reproduced byte-for-byte by `generated_patterns()` in\n\
         # tests/re2_screen_differential.rs from seed {SEED:#018x} over its frozen\n\
         # token table. Do not edit by hand; regenerate with\n\
         # PULSUS_REGEN_RE2_CORPUS=1 and review the diff.\n"
    );
    header + &generated_patterns().join("\n") + "\n"
}

/// The last bytes of a file, escaped, for reporting an EOF-only difference.
fn tail(s: &str) -> String {
    let from = s.len().saturating_sub(24);
    format!("{:?}", &s[from..])
}

/// The first way the committed file disagrees with its expected bytes,
/// named precisely.
///
/// Issue #309 review round 3: the comparison is on **bytes** — round 2
/// compared `lines()` with comment/blank filtering, so a header edit, a
/// blank-line edit or a missing trailing newline normalised away and
/// reported "no drift" while the doc claimed byte-for-byte reproduction.
/// The line-oriented *report* is kept (round 2's `[low]`: a 4,000-element
/// `assert_eq!` dumped 145 KB and named nothing), but it now only explains
/// a difference the byte comparison has already found.
fn describe_byte_drift(committed: &str, expected: &str) -> Option<String> {
    if committed == expected {
        return None;
    }
    let have: Vec<&str> = committed.lines().collect();
    let want: Vec<&str> = expected.lines().collect();
    for (i, (h, w)) in have.iter().zip(&want).enumerate() {
        if h != w {
            return Some(format!(
                "generated.txt:{}: committed {h:?}, generator produces {w:?}",
                i + 1
            ));
        }
    }
    if have.len() != want.len() {
        let common = have.len().min(want.len());
        return Some(format!(
            "generated.txt has {} lines, the generator produces {} \
             (the first {common} agree; the difference is at the end of the file)",
            have.len(),
            want.len(),
        ));
    }
    // Every line agrees and the counts agree, yet the bytes do not: the
    // difference is in line terminators or the trailing newline — exactly
    // what a line-oriented comparison used to normalise away.
    Some(format!(
        "generated.txt differs from the generator only in trailing/terminator bytes \
         ({} bytes committed, {} expected; committed ends {}, generator ends {})",
        committed.len(),
        expected.len(),
        tail(committed),
        tail(expected),
    ))
}

/// Curated first (the classes we reason about by name), generated second.
fn full_corpus() -> Vec<String> {
    let mut all = read_corpus("curated.txt");
    all.extend(read_corpus("generated.txt"));
    all
}

/// `generated.txt` is generator output, not a hand-edited list: **every
/// byte of the file**, header included, must be what the seed produces.
/// Regenerate with `PULSUS_REGEN_RE2_CORPUS=1` and review the diff.
#[test]
fn generated_corpus_fixture_is_exactly_what_the_seed_produces() {
    live_gate_or_panic();
    let expected = rendered_generated_fixture();
    if std::env::var("PULSUS_REGEN_RE2_CORPUS").as_deref() == Ok("1") {
        std::fs::write(fixture_dir().join("generated.txt"), &expected).expect("write fixture");
        return;
    }
    if let Some(drift) = describe_byte_drift(&read_file("generated.txt"), &expected) {
        panic!(
            "the committed corpus no longer matches its generator — {drift}. \
             Either the seed/token table moved (regenerate with \
             PULSUS_REGEN_RE2_CORPUS=1 and review the diff) or the fixture was \
             hand-edited (revert it; it is generator output, not a hand list)."
        );
    }
}

/// The curated half must keep naming the classes the live differential
/// discovered (they were not predicted by inspection), so a future edit
/// cannot quietly drop the cases that motivated the screen's shape or
/// issue #324's SQL-side flag.
#[test]
fn curated_corpus_still_carries_the_discovered_divergence_classes() {
    live_gate_or_panic();
    let curated = read_corpus("curated.txt");
    for probe in [
        r"\p{Alphabetic}",
        r"[\p{Alphabetic}]",
        "a**",
        "a{2}{3}",
        "a{1001}",
        r"\u{263A}",
        "(?x)a b",
        "a{bbb}c",
        "a*?",
        "[*+]",
        // Issue #324: the measured case, the `s`-flag opt-in that must keep
        // working over it, and the negated class whose newline behaviour
        // was always correct and must not be "fixed".
        ".",
        "a.b",
        "(?s).",
        "(?s:a.b)",
        r"[^\n]",
        "[^a]",
    ] {
        assert!(
            curated.iter().any(|p| p == probe),
            "curated corpus lost {probe:?}"
        );
    }
    assert!(
        curated.len() > 150,
        "curated corpus shrank: {}",
        curated.len()
    );
}

/// Called first by **every** test in this binary, not just the live one.
///
/// Issue #309 review round 3: round 2 put this check inside the live test
/// alone, so it was per-test, not per-binary — filtering to a hermetic test
/// (or `-- --skip no_re2_rejected_pattern_escapes_the_screen`) still exited
/// `0` with the gate absent. Every test now refuses to run in a live CI job
/// without the gate.
///
/// Issue #320 generalised the mechanism this suite pioneered into
/// [`pulsus_testkit`]; the coverage statement and the one residual it does
/// not close (a filter that selects no test at all) live in that crate's
/// module docs.
fn live_gate_or_panic() {
    pulsus_testkit::require_live_gate(pulsus_testkit::CLICKHOUSE_GATE);
}

/// `true` when the live half should execute.
fn should_run() -> bool {
    if pulsus_testkit::live_clickhouse_enabled() {
        return true;
    }
    eprintln!(
        "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
         (see crates/pulsus-read/tests/re2_screen_differential.rs for setup)"
    );
    false
}

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 8,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

/// A ClickHouse string literal. Backslash and quote are the only escapes
/// the *corpus* needs (it carries no control characters), but the probe
/// subjects deliberately do — tab and newline are in RE2's `\s` and
/// vertical tab is not, which is one of the divergences under test.
fn sql_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    out.push('\'');
    out
}

/// Doubles literal `?` before executing raw SQL text against the real
/// server — the execution-boundary contract `metrics::sql`'s doc comment
/// documents, mirroring `live_metrics_cache.rs`'s own copy. Load-bearing
/// here: the anchored form `^(?:…)$` that the read path renders carries a
/// `?` in every single pattern.
fn double_placeholders(sql: &str) -> String {
    sql.replace('?', "??")
}

/// `true` when ClickHouse's RE2 compiles the anchored pattern. A `427`
/// (`CANNOT_COMPILE_REGEXP`) is the rejection; any other error is a fixture
/// or connectivity fault and panics rather than being scored.
async fn re2_accepts(client: &ChClient, pattern: &str) -> bool {
    let sql = double_placeholders(&format!(
        "SELECT match('x', {})",
        rendered_sql_pattern(pattern)
    ));
    match client
        .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
    {
        Ok(()) => true,
        Err(ChError::Server { code: 427, .. }) => false,
        Err(other) => panic!("{pattern:?}: unexpected ClickHouse failure: {other:?}"),
    }
}

/// The evidence the screen rests on: over the whole corpus, **no pattern
/// the Rust `regex` crate accepts and RE2 rejects escapes the screen**.
/// The reverse (RE2 accepts, screen defers) is allowed and merely counted —
/// it costs a storage round-trip, never a rejection, and the counts are
/// printed so a future change can see whether it widened.
#[tokio::test]
async fn no_re2_rejected_pattern_escapes_the_screen() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/re2_screen_differential.rs for setup)"
        );
        return;
    }

    let client = ChClient::new(test_config()).await.expect("connect");
    let corpus = full_corpus();
    assert!(corpus.len() > 4_000, "corpus shrank: {}", corpus.len());

    let mut rust_accepted = 0usize;
    let mut re2_rejected = 0usize;
    let mut caught = 0usize;
    let mut over_screened = 0usize;
    let mut escapes: Vec<String> = Vec::new();

    for pattern in &corpus {
        // A pattern the in-process path cannot compile never reaches the
        // warm cache — the compiled-regex cache falls back to storage. That
        // path compiles RE2's reading of the pattern (issue #317), not the
        // raw text, so the screen's scope is defined against the SAME
        // rewrite the cache applies.
        if rust_accepts(pattern).is_none() {
            continue;
        }
        rust_accepted += 1;
        let accepted_by_re2 = re2_accepts(&client, pattern).await;
        let deferred = screened(pattern);
        match (accepted_by_re2, deferred) {
            (false, true) => {
                re2_rejected += 1;
                caught += 1;
            }
            (false, false) => {
                re2_rejected += 1;
                escapes.push(pattern.clone());
            }
            (true, true) => over_screened += 1,
            (true, false) => {}
        }
    }

    println!(
        "re2 screen differential: corpus={} rust_accepted={rust_accepted} \
         re2_rejected={re2_rejected} caught={caught} over_screened={over_screened}",
        corpus.len()
    );

    // Sanity: the corpus must actually exercise both engines, or "zero
    // escapes" would be vacuously true.
    assert!(
        rust_accepted > 1_000,
        "corpus no longer exercises the Rust-accepted set: {rust_accepted}"
    );
    assert!(
        re2_rejected > 100,
        "corpus no longer reaches RE2 rejections: {re2_rejected}"
    );
    assert_eq!(
        caught, re2_rejected,
        "patterns the Rust crate accepts and RE2 rejects escaped the screen — \
         the warm cache would answer them in-process: {escapes:?}"
    );
}

/// Whether the vendored PromQL parser would admit a matcher carrying this
/// pattern at all — it compiles every matcher itself
/// (`vendor/promql-parser` `Matcher::try_parse_re`), with a brace-escaping
/// retry for the `a{bbb}c` asymmetry, and rejects the query outright
/// otherwise. A pattern it refuses never reaches the read path, so it
/// cannot be over-rejected there.
fn parser_admits(pattern: &str) -> bool {
    let literal = pattern.replace('\\', "\\\\").replace('"', "\\\"");
    pulsus_promql::parse(&format!("up{{job=~\"{literal}\"}}")).is_ok()
}

/// The probe alphabet for the meaning differential (issue #317). Chosen so
/// every rewritten construct is separable: an ASCII digit against an
/// Arabic-Indic one (`\d`), an ASCII letter against `é` (`\w`), space and
/// tab against NBSP and vertical tab (`\s`), and the punctuation the Rust
/// crate reads as character-class syntax (`&`, `~`, `-`, `[`, `]`).
/// Issue #324 adds the newline-bearing subjects: a bare `\n` alone cannot
/// separate "`.` matched the newline" from "`.` matched nothing and the
/// anchors did the work", and `\r` is the control in the other direction —
/// RE2 excludes ONLY `\n` from `.`, so a fix that excluded `\r` too would
/// be over-correcting and must fail here.
const SUBJECTS: &[&str] = &[
    "", "a", "b", "z", "0", "9", "_", "-", ".", "&", "~", "[", "]", "a]", "ab", "a-b", " ", "\t",
    "\n", "a\nb", "\r", "a\rb", "\u{000b}", "\u{00a0}", "\u{0663}", "é", "a{bbb}c",
];

/// The in-process engine's compiled form of `pattern` — RE2's reading of
/// it, which is exactly what `metrics::labels`' regex cache compiles.
/// `None` when the Rust crate rejects the rewrite, in which case the read
/// path degrades to storage and never answers in-process.
fn rust_accepts(pattern: &str) -> Option<regex::Regex> {
    let rewritten = pulsus_promql::re2_pattern_to_rust(pattern);
    regex::Regex::new(&format!("^(?:{rewritten})$")).ok()
}

/// `is_match` over [`SUBJECTS`], as a bit string.
fn match_bits(re: &regex::Regex) -> String {
    SUBJECTS
        .iter()
        .map(|s| if re.is_match(s) { '1' } else { '0' })
        .collect()
}

/// A replacement string no probe subject contains, so "RE2 found a match"
/// is decidable from `replaceRegexpOne`'s output alone: any match —
/// full-subject, partial or empty-width — changes the haystack, and no
/// match leaves it byte-identical. The comparison is `!= subject`, NOT
/// `= sentinel`: the corpus contains patterns whose own `)` and `|`
/// escape the `^(?:…)$` template (`)|(?i:` renders as `^(?:)|(?i:)$`,
/// an alternation of two half-anchored branches), so a match need not
/// consume the whole subject — measured, `replaceRegexpOne('a',
/// '^(?:)|(?i:)$', 'X')` is `'Xa'`, which a sentinel-equality reference
/// misread as "no match" while `match()` correctly answered 1.
const SENTINEL: &str = "'<matched>'";

/// One RAW `match()` probe: the pattern is sent to the server exactly as
/// written — never through the read path's renderer — so these pin
/// ClickHouse's OWN behaviour independent of issue #331's workaround.
struct RawMatchProbe {
    pattern: &'static str,
    /// Non-empty (the `replaceRegexpOne` reference cannot see the empty
    /// subject), and chosen so RE2 finds a match somewhere inside it —
    /// except for the never-match arm probes, where nothing may match.
    subject: &'static str,
}

const fn raw(pattern: &'static str, subject: &'static str) -> RawMatchProbe {
    RawMatchProbe { pattern, subject }
}

/// Issue #331 (superseding issue #324's single-entry exemption list):
/// ClickHouse's `OptimizedRegularExpression::analyze` leaks a `(?…`
/// flag-group head's own characters into the required substring it
/// extracts, whenever the head's flags carry no `i`. Measured on
/// 24.8.14.39: `match('xaby', '(?s:ab)')` is `0` while `match('xs:aby',
/// '(?s:ab)')` is `1` — the server is requiring the literal `s:ab` —
/// and `replaceRegexpOne`, which reaches RE2 without the wrapper,
/// matches. Every entry here is a form measured broken, across the
/// full enumeration: scoped and flag-only heads, every `{s,m,U}`
/// combination, positive and negated, two-sided, repeated, mid-pattern,
/// grouped, and inside the read path's own anchored template.
///
/// **The rot check:** the registry test asserts every entry is STILL
/// broken raw. The day a ClickHouse upgrade fixes the analyzer, that
/// assertion fails and the issue #331 workaround
/// (`pulsus_re2::clickhouse_match_strategy` and the render shapes in
/// `logql::escape`) is retired rather than carried dead.
const CLICKHOUSE_STILL_BROKEN_RAW: &[RawMatchProbe] = &[
    raw("(?s:a)", "xay"),
    raw("(?s:ab)", "xaby"),
    raw("(?s:a.b)", "xa-by"),
    raw("(?m:a)", "xay"),
    raw("(?m:ab)", "xaby"),
    raw("(?U:ab+)", "xabby"),
    raw("(?sm:ab)", "xaby"),
    raw("(?smU:ab)", "xaby"),
    raw("(?mU:ab)", "xaby"),
    raw("(?sU:ab)", "xaby"),
    raw("(?-s:ab)", "xaby"),
    raw("(?-m:ab)", "xaby"),
    raw("(?-U:ab)", "xaby"),
    raw("(?s-m:ab)", "xaby"),
    raw("(?U-s:ab)", "xaby"),
    raw("(?ss:ab)", "xaby"),
    raw("(?s)ab", "xaby"),
    raw("(?m)ab", "xaby"),
    raw("(?U)ab", "xaby"),
    raw("(?-s)ab", "xaby"),
    raw("(?-m)ab", "xaby"),
    raw("(?-U)ab", "xaby"),
    raw("x(?s:ab)", "zxaby"),
    raw("x(?m:ab)", "zxaby"),
    raw("(?:(?s:ab))", "qaby"),
    // The read path's own template shapes, as they rendered BEFORE the
    // workaround — the defect this issue exists to fix.
    raw("^(?:(?s:ab))$", "ab"),
    raw("(?-s)^(?:(?s:ab))$", "ab"),
    raw("(?-s)^(?:(?s)ab)$", "ab"),
];

/// The workaround's PREMISES, asserted against the same live server: a
/// head carrying `i` anywhere — including every `-i`-appended rewrite
/// output — does not leak, named groups and plain groups do not leak,
/// and the never-match-arm shapes answer exactly as RE2 does. If any of
/// these ever starts failing, the workaround itself is unsound on that
/// server and must be revisited, not patched around.
const CLICKHOUSE_STILL_SOUND_RAW: &[RawMatchProbe] = &[
    // The rewrite outputs for every broken family above.
    raw("(?s-i:ab)", "xaby"),
    raw("(?m-i:ab)", "xaby"),
    raw("(?U-i:ab)", "xaby"),
    raw("(?sm-i:ab)", "xaby"),
    raw("(?smU-i:ab)", "xaby"),
    raw("(?mU-i:ab)", "xaby"),
    raw("(?sU-i:ab)", "xaby"),
    raw("(?-si:ab)", "xaby"),
    raw("(?-mi:ab)", "xaby"),
    raw("(?-Ui:ab)", "xaby"),
    raw("(?s-mi:ab)", "xaby"),
    raw("(?U-si:ab)", "xaby"),
    raw("(?ss-i:ab)", "xaby"),
    raw("(?s-i)ab", "xaby"),
    raw("(?m-i)ab", "xaby"),
    raw("(?U-i)ab", "xaby"),
    raw("(?-si)ab", "xaby"),
    raw("(?-mi)ab", "xaby"),
    raw("(?-Ui)ab", "xaby"),
    // User-written i-heads are untouched by the strategy, so they must
    // be sound on their own.
    raw("(?i:ab)", "xaby"),
    raw("(?i)ab", "xaby"),
    raw("(?im:ab)", "xaby"),
    raw("(?si:ab)", "xaby"),
    raw("(?i-s:ab)", "xaby"),
    raw("(?-is:ab)", "xaby"),
    // Head-shaped constructs the strategy classifies safe.
    raw("(?:ab)", "xaby"),
    raw("(?)ab", "xaby"),
    raw("(?P<n>ab)", "xaby"),
    raw("(?<n>ab)", "xaby"),
    // The never-match-arm render shapes, matching side — including the
    // fix round 3 non-literal-leading routing's outputs.
    raw("^(?:(?s:ab))$|$.", "ab"),
    raw("(?-s)^(?:(?s:ab))$|$.", "ab"),
    raw("(?:(?s:ab))|$.", "xaby"),
    raw("(?:(?s:.*ab.*))|$.", "xaby"),
    raw("^(?:(?s:.*ab.*))$|$.", "xaby"),
    // Issue #324's own preserved semantics under the arm: `.` must not
    // match a newline through the alternation.
];

/// The never-matching arm really never matches — including on subjects
/// with newlines, and with a user `(?m)` contained by the wrap.
const CLICKHOUSE_NEVER_MATCH_RAW: &[RawMatchProbe] = &[
    raw("$.", "a"),
    raw("$.", "a\nb"),
    raw("(?:(?m)x)|$.", "q\n"),
    raw("(?-s)^(?:a.b)$|$.", "a\nb"),
];

/// One `String` column per engine call, so a whole pattern's answer is one
/// round trip. `replaceRegexpOne` runs RE2 directly; `match` is the
/// function the read path's SQL predicates use and goes through
/// ClickHouse's `OptimizedRegularExpression` wrapper first, so both are
/// read and reported separately.
fn bits_sql(pattern: &str) -> String {
    // The reference half is RE2 over the USER's anchored pattern; the
    // `match` half is the literal the read path actually renders, which
    // since issue #324 is not the same text (`(?-s)` prefix). Comparing
    // them is the whole point: the fix has to make ClickHouse's wrapper
    // agree with RE2's own reading of what the user wrote.
    let anchored = sql_literal(&format!("^(?:{pattern})$"));
    let rendered = rendered_sql_pattern(pattern);
    let mut parts: Vec<String> = Vec::with_capacity(SUBJECTS.len() * 2);
    for subject in SUBJECTS {
        // `replaceRegexpOne` returns an empty haystack unchanged without
        // consulting RE2 at all (measured), so the empty subject — which
        // matters, `label=~""` is an ordinary matcher — has to be read
        // through `match`. Every other subject uses the replace form,
        // which reaches RE2 without ClickHouse's
        // `OptimizedRegularExpression` wrapper in front of it.
        if subject.is_empty() {
            parts.push(format!("toString(match('', {anchored}))"));
            continue;
        }
        let subject = sql_literal(subject);
        parts.push(format!(
            "if(replaceRegexpOne({subject}, {anchored}, {SENTINEL}) != {subject}, '1', '0')"
        ));
    }
    for subject in SUBJECTS {
        let subject = sql_literal(subject);
        parts.push(format!("toString(match({subject}, {rendered}))"));
    }
    double_placeholders(&format!("SELECT concat({}) AS bits", parts.join(", ")))
}

/// One row, one column: the concatenated verdict bits.
#[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize)]
struct BitsRow {
    bits: String,
}

/// RE2 refusing the pattern, under either of the two server codes
/// ClickHouse reports it with: `match` raises `427
/// CANNOT_COMPILE_REGEXP` from its `OptimizedRegularExpression` wrapper,
/// while `replaceRegexpOne` raises a plain `36 BAD_ARGUMENTS` naming the
/// pattern. Anything else is a fixture or connectivity fault and must not
/// be scored as a rejection.
fn is_re2_rejection(e: &ChError) -> bool {
    match e {
        ChError::Server { code: 427, .. } => true,
        ChError::Server { code: 36, message } => message.contains("not a valid re2 pattern"),
        _ => false,
    }
}

/// RE2's verdicts for `pattern` over [`SUBJECTS`]: `(true RE2 over the
/// user's anchored pattern, ClickHouse `match` over the RENDERED literal)`.
/// `None` when RE2 refuses to compile the pattern — an acceptance
/// divergence, which the other live test owns.
///
/// The stream is drained inside this function so its pooled-connection
/// lease is released before the caller issues the next query.
async fn re2_bits(client: &ChClient, pattern: &str) -> Option<(String, String)> {
    let sql = bits_sql(pattern);
    let rows: Vec<BitsRow> = {
        let mut stream = match client
            .query_stream::<BitsRow>(&sql, &QuerySettings::new())
            .await
        {
            Ok(stream) => stream,
            Err(e) if is_re2_rejection(&e) => return None,
            Err(other) => panic!("{pattern:?}: unexpected ClickHouse failure: {other:?}"),
        };
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await {
            match row {
                Ok(row) => rows.push(row),
                Err(e) if is_re2_rejection(&e) => return None,
                Err(other) => panic!("{pattern:?}: unexpected ClickHouse failure: {other:?}"),
            }
        }
        rows
    };
    let bits = rows.first().expect("one row").bits.clone();
    let (exact, optimized) = bits.split_at(SUBJECTS.len());
    Some((exact.to_string(), optimized.to_string()))
}

/// The evidence behind the issue #317 rewrite: for **every** corpus pattern
/// both engines accept, the in-process engine and RE2 select the same
/// subjects. Without the rewrite the two disagree on `\d`/`\w`/`\s`,
/// `\b`, and every character class carrying `&&`, `~~`, `--` or a nested
/// `[` — the `fixed_by_the_rewrite` counter is that control, and the test
/// fails if the corpus stops exercising it.
#[tokio::test]
async fn the_rewrite_makes_the_in_process_engine_agree_with_re2() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/re2_screen_differential.rs for setup)"
        );
        return;
    }

    let client = ChClient::new(test_config()).await.expect("connect");
    let corpus = full_corpus();

    let mut compared = 0usize;
    let mut screened_off_the_in_process_path = 0usize;
    let mut re2_rejected = 0usize;
    let mut rust_rejected_after_rewrite = 0usize;
    let mut over_rejected_by_316: Vec<String> = Vec::new();
    let mut fixed_by_the_rewrite = 0usize;
    let mut clickhouse_match_deviations: Vec<String> = Vec::new();
    let mut mismatches: Vec<String> = Vec::new();

    for pattern in &corpus {
        // Issue #324's crossing runs FIRST and is not screened: a screened
        // pattern is precisely one the read path sends to SQL, so it is the
        // most important input to the SQL-vs-RE2 comparison, not an exempt
        // one.
        let Some((re2, optimized)) = re2_bits(&client, pattern).await else {
            re2_rejected += 1;
            continue;
        };
        if optimized != re2 {
            clickhouse_match_deviations.push(format!(
                "{pattern:?}: clickhouse_match={optimized} re2={re2}"
            ));
        }
        // A screened pattern never gets an in-process verdict at all — the
        // read path defers it to storage (issue #309), so RE2 answers it
        // and there is nothing below to agree or disagree with. This is the
        // same order the read path applies: screen first, compile second.
        if screened(pattern) {
            screened_off_the_in_process_path += 1;
            continue;
        }
        let Some(re) = rust_accepts(pattern) else {
            // The rewrite does not compile: on every path with a storage
            // fallback that is a round-trip and a correct answer. On the
            // name-less path (issue #316) it is a 400 instead — an
            // over-rejection whenever RE2 would have answered AND the
            // vendored parser admits the selector at all, which is the
            // combination counted here.
            rust_rejected_after_rewrite += 1;
            if parser_admits(pattern) && over_rejected_by_316.len() < 16 {
                over_rejected_by_316.push(pattern.clone());
            }
            continue;
        };
        compared += 1;
        let rust = match_bits(&re);
        if rust != re2 {
            mismatches.push(format!("{pattern:?}: rust={rust} re2={re2}"));
        }
        // The control: what the same pattern would have done WITHOUT the
        // rewrite. Only counted when the raw form compiles at all.
        if let Ok(raw) = regex::Regex::new(&format!("^(?:{pattern})$"))
            && match_bits(&raw) != re2
        {
            fixed_by_the_rewrite += 1;
        }
    }

    println!(
        "re2 meaning differential: corpus={} compared={compared} \
         screened={screened_off_the_in_process_path} re2_rejected={re2_rejected} \
         rust_rejected_after_rewrite={rust_rejected_after_rewrite} \
         fixed_by_the_rewrite={fixed_by_the_rewrite} \
         clickhouse_match_deviations={} over_rejected_by_316={:?}",
        corpus.len(),
        clickhouse_match_deviations.len(),
        over_rejected_by_316
    );

    // Issues #324/#331: ClickHouse's `match()` must select the same
    // subjects as RE2 itself for the literal the read path renders — with
    // NO exemptions. Before the `(?-s)` prefix this listed every
    // `.`-carrying pattern (the SQL path over-matching any label value
    // containing a line break); before issue #331's flag-head workaround
    // it carried a `(?s:a.b)` exemption (the SQL path silently selecting
    // no rows). The raw-probe registry above is where the underlying
    // server defect stays visible; the RENDERED literal must simply be
    // correct.
    assert!(
        clickhouse_match_deviations.is_empty(),
        "ClickHouse's match() disagrees with RE2 for {} rendered pattern(s) — the SQL \
         path would return wrong rows: {clickhouse_match_deviations:#?}",
        clickhouse_match_deviations.len()
    );

    assert!(
        compared > 500,
        "corpus no longer exercises patterns both engines accept: {compared}"
    );
    // Mutation control: the number of patterns that BOTH engines compile
    // with and without the rewrite, and that answer differently from RE2
    // without it — 80 on this corpus. Without this floor a corpus that
    // stopped reaching `\d`/`\w`/`\s`, `\b` and the class operators would
    // make the zero-mismatch assertion below vacuously true.
    assert!(
        fixed_by_the_rewrite > 40,
        "corpus no longer reaches the constructs the rewrite exists to fix: \
         {fixed_by_the_rewrite}"
    );
    // The invariant that outranks every other one here: a pattern the
    // reference accepts must keep working. A rewrite the Rust crate cannot
    // compile costs a storage round-trip everywhere except the name-less
    // path, where issue #316 turns it into a 400 — so on that path it must
    // never be a pattern RE2 would have answered.
    assert!(
        over_rejected_by_316.is_empty(),
        "the name-less path would reject {} pattern(s) RE2 accepts and the parser admits: \
         {over_rejected_by_316:?}",
        over_rejected_by_316.len()
    );
    assert!(
        mismatches.is_empty(),
        "the in-process engine and RE2 select different subjects for {} pattern(s) — \
         a query would return the wrong rows: {mismatches:#?}",
        mismatches.len()
    );
}

// ---------------------------------------------------------------------
// Issue #331: the flag-group-head defect registry, probed RAW.
// ---------------------------------------------------------------------

/// `(clickhouse_match, re2)` verdicts for one raw probe, via one round
/// trip. The subject is non-empty by [`RawMatchProbe`]'s contract, so
/// `replaceRegexpOne` is a valid RE2 reference.
async fn raw_verdicts(client: &ChClient, p: &RawMatchProbe) -> (char, char) {
    let subject = sql_literal(p.subject);
    let pattern = sql_literal(p.pattern);
    let sql = double_placeholders(&format!(
        "SELECT concat(toString(match({subject}, {pattern})), \
         if(replaceRegexpOne({subject}, {pattern}, '<m>') != {subject}, '1', '0')) AS bits"
    ));
    let mut stream = client
        .query_stream::<BitsRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{:?}: raw probe failed: {e:?}", p.pattern));
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.unwrap_or_else(|e| panic!("{:?}: raw probe failed: {e:?}", p.pattern)));
    }
    let bits = &rows.first().expect("one row").bits;
    let mut chars = bits.chars();
    (
        chars.next().expect("match bit"),
        chars.next().expect("re2 bit"),
    )
}

/// The issue #331 registry, all three faces (see the consts above):
/// every recorded defect is STILL broken raw (the rot check — a
/// ClickHouse fix turns up here, and the workaround is then retired,
/// never carried dead), every premise the workaround rests on still
/// holds, and the never-matching arm still never matches. Probed RAW,
/// never through the renderer, so this test is independent of the fix
/// it justifies.
#[tokio::test]
async fn the_flag_head_defect_registry_holds_raw() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/re2_screen_differential.rs for setup)"
        );
        return;
    }
    let client = ChClient::new(test_config()).await.expect("connect");
    for p in CLICKHOUSE_STILL_BROKEN_RAW {
        let (ch, re2) = raw_verdicts(&client, p).await;
        assert_eq!(
            re2, '1',
            "{:?} vs {:?}: the registry premise broke — RE2 no longer matches this subject",
            p.pattern, p.subject
        );
        assert_eq!(
            ch, '0',
            "{:?} vs {:?}: ClickHouse's match() now AGREES with RE2 — the analyzer defect \
             is fixed on this server. Delete this entry, and once the list is empty retire \
             the issue #331 workaround (pulsus_re2::clickhouse_match_strategy and the \
             render shapes in logql::escape).",
            p.pattern, p.subject
        );
    }
    for p in CLICKHOUSE_STILL_SOUND_RAW {
        let (ch, re2) = raw_verdicts(&client, p).await;
        assert_eq!(
            (ch, re2),
            ('1', '1'),
            "{:?} vs {:?}: a premise of the issue #331 workaround no longer holds on this \
             server (got match={ch} re2={re2}) — revisit the strategy, do not patch around it",
            p.pattern,
            p.subject
        );
    }
    for p in CLICKHOUSE_NEVER_MATCH_RAW {
        let (ch, re2) = raw_verdicts(&client, p).await;
        assert_eq!(
            (ch, re2),
            ('0', '0'),
            "{:?} vs {:?}: the never-matching arm MATCHED (match={ch} re2={re2}) — the \
             defeat rendering would return wrong rows",
            p.pattern,
            p.subject
        );
    }
    println!(
        "flag-head defect registry: {} broken, {} sound, {} never-match probes all hold",
        CLICKHOUSE_STILL_BROKEN_RAW.len(),
        CLICKHOUSE_STILL_SOUND_RAW.len(),
        CLICKHOUSE_NEVER_MATCH_RAW.len()
    );
}

// ---------------------------------------------------------------------
// Issue #331: hermetic proofs over the corpus.
// ---------------------------------------------------------------------

/// Patterns the strategy leaves alone must render **byte-for-byte** what
/// the pre-#331 renderer produced — replicated here from issue #324's
/// shape (`'(?-s)` + the anchored literal) — so the workaround cannot
/// perturb the unaffected majority. Counted on both sides so neither
/// branch can go vacuous. This test covers the PromQL rendering (the
/// one seam an external binary can reach); the SAME corpus-wide
/// crossing for the LogQL anchored and unanchored renderings lives
/// beside the module-private escapers, in `logql::escape`'s tests
/// (fix round 1).
#[test]
fn unaffected_corpus_patterns_render_byte_identically_to_the_pre_331_form() {
    live_gate_or_panic();
    let mut verbatim = 0usize;
    let mut transformed = 0usize;
    for pattern in full_corpus() {
        match pulsus_re2::clickhouse_match_strategy(&pattern) {
            pulsus_re2::ClickhouseMatchStrategy::Verbatim => {
                verbatim += 1;
                let anchored = sql_literal(&format!("^(?:{pattern})$"));
                let pre_331 = format!("'(?-s){}", &anchored[1..]);
                assert_eq!(
                    rendered_sql_pattern(&pattern),
                    pre_331,
                    "{pattern:?}: an unaffected pattern's rendering moved"
                );
            }
            _ => transformed += 1,
        }
    }
    assert!(verbatim > 3_000, "verbatim side went vacuous: {verbatim}");
    assert!(
        transformed > 100,
        "transformed side went vacuous: {transformed}"
    );
}

/// The transform preserves Rust-crate compilability over the whole
/// corpus, per render shape — the evidence behind
/// `escape::ch_regex_anchored_checked`'s doc claim that validating the
/// user's form still covers the emitted form. Each shape is gated on
/// its OWN raw form compiling, exactly as the read path validates
/// before it renders (a raw pattern like `/)(?-s)(?P<n>` is invalid
/// alone yet valid inside the anchoring template, whose group its stray
/// `)` closes — so the two shapes genuinely differ here).
#[test]
fn the_workaround_transform_preserves_compilability_over_the_corpus() {
    live_gate_or_panic();
    let mut checked = 0usize;
    for pattern in full_corpus() {
        let (anchored, unanchored) = match pulsus_re2::clickhouse_match_strategy(&pattern) {
            pulsus_re2::ClickhouseMatchStrategy::Verbatim => continue,
            pulsus_re2::ClickhouseMatchStrategy::RewriteHeads(p) => (format!("^(?:{p})$"), p),
            pulsus_re2::ClickhouseMatchStrategy::NeverMatchArm => {
                (format!("^(?:{pattern})$|$."), format!("(?:{pattern})|$."))
            }
        };
        if regex::Regex::new(&format!("^(?:{pattern})$")).is_ok() {
            assert!(
                regex::Regex::new(&anchored).is_ok(),
                "{pattern:?}: the anchored transform no longer compiles: {anchored:?}"
            );
            checked += 1;
        }
        if regex::Regex::new(&pattern).is_ok() {
            assert!(
                regex::Regex::new(&unanchored).is_ok(),
                "{pattern:?}: the unanchored transform no longer compiles: {unanchored:?}"
            );
            checked += 1;
        }
    }
    assert!(checked > 50, "transform side went vacuous: {checked}");
}
