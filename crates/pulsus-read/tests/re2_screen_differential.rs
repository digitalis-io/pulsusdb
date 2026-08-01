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
//! * **Live** (`PULSUS_TEST_CLICKHOUSE=1`) — every corpus pattern the Rust
//!   crate accepts is compiled by ClickHouse's RE2 (`SELECT match('x',
//!   '^(?:…)$')`, the anchored form the read path renders) and the two
//!   verdicts are crossed with the screen's. There is no in-process RE2, so
//!   this half cannot be hermetic.
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

use pulsus_clickhouse::{ChClient, ChConnConfig, ChError, ChProto, Idempotency, QuerySettings};
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
    "{2}",
    "{2,}",
    "{2,3}",
    "{1000}",
    "{1001}",
    "{0,2000}",
    "{bbb}",
    "{",
    "\\d",
    "\\w",
    "\\s",
    "\\D",
    "\\S",
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

/// The curated half must keep naming the two classes the live differential
/// discovered (they were not predicted by inspection), so a future edit
/// cannot quietly drop the cases that motivated the screen's shape.
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

/// The GitHub Actions job id of the **hermetic** lane — the one that runs
/// `cargo test --workspace` with no ClickHouse and is therefore allowed to
/// skip this suite. Every other job that reaches this binary is a live job.
const HERMETIC_CI_JOB: &str = "ci";

/// Why this run is, or is not, allowed to skip the live half.
#[derive(Debug)]
enum LiveGate {
    /// `PULSUS_TEST_CLICKHOUSE=1` — run against the container.
    Run,
    /// No gate, and nothing claims there should be one: a developer
    /// machine, or the hermetic CI lane. Skip cleanly.
    SkipUngated,
    /// No gate, but this is a CI job that exists to provide the container.
    /// Skipping here is the silent failure this variant exists to prevent.
    MissingInLiveCiJob { job: String },
}

/// Issue #309 review round 2: with the gate unset this suite exited `0`,
/// so a wiring regression in `.github/workflows/ci.yml` would disable it
/// **silently** — the same class as the provenance step that never ran
/// (#272). GitHub Actions sets `GITHUB_JOB` to the job id, which separates
/// the hermetic `cargo test --workspace` lane (allowed to skip) from a job
/// that exists to stand up ClickHouse (must not).
///
/// Deliberately fail-closed on the brittle edge: if `HERMETIC_CI_JOB` is
/// ever renamed, this starts failing the hermetic lane *loudly* rather than
/// quietly excusing a live job — a wrong guess about which job we are in
/// can only ever produce noise, never a silent pass.
///
/// **Scope:** this suite only. The skip-when-ungated idiom is repo-wide
/// (57 gated suites) and generalising it is issue **#320**'s job — see the
/// note there before copying this anywhere else.
fn live_gate() -> LiveGate {
    if std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1") {
        return LiveGate::Run;
    }
    match std::env::var("GITHUB_JOB") {
        Ok(job) if job != HERMETIC_CI_JOB => LiveGate::MissingInLiveCiJob { job },
        _ => LiveGate::SkipUngated,
    }
}

/// Called first by **every** test in this binary, not just the live one.
///
/// Issue #309 review round 3: round 2 put this check inside the live test
/// alone, so it was per-test, not per-binary — filtering to a hermetic test
/// (or `-- --skip no_re2_rejected_pattern_escapes_the_screen`) still exited
/// `0` with the gate absent. Every test now refuses to run in a live CI job
/// without the gate.
///
/// **Exactly what this covers:** in a live CI job with the gate absent,
/// every test in this binary panics, so any invocation that executes **at
/// least one** test exits non-zero. An invocation whose filter selects *no*
/// test executes nothing at all, and a harness binary has no hook that runs
/// in that case — `cargo test` exits `0` there, while nextest (which the
/// hermetic workspace lane uses) exits `4`, `no tests to run`. Closing that
/// last case would need `harness = false`, and nextest cannot list a
/// non-harness binary (measured: `creating test list failed`), so it would
/// break the very lane this guard protects.
fn live_gate_or_panic() -> LiveGate {
    let gate = live_gate();
    if let LiveGate::MissingInLiveCiJob { job } = &gate {
        panic!(
            "PULSUS_TEST_CLICKHOUSE is not set, but this is CI job {job:?}, which exists to \
             provide ClickHouse — the RE2 screen differential would have skipped silently. \
             Restore the gate on the 'RE2 screen differential suite' step in \
             .github/workflows/ci.yml (issue #320 owns generalising this check)."
        );
    }
    gate
}

/// `true` when the live half should execute.
fn should_run() -> bool {
    match live_gate_or_panic() {
        LiveGate::Run => true,
        _ => {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/re2_screen_differential.rs for setup)"
            );
            false
        }
    }
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

/// A ClickHouse string literal: backslash and quote are the only escapes
/// its parser needs here, and the corpus carries no control characters.
fn sql_literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
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
        sql_literal(&format!("^(?:{pattern})$"))
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
        // A pattern the Rust crate cannot compile never reaches the warm
        // cache: the vendored parser rejects it, or the compiled-regex
        // cache falls back. Only the accepted set is in scope here.
        if regex::Regex::new(&format!("^(?:{pattern})$")).is_err() {
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
