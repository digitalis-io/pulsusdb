//! Differential test: the SQL a `|=`/`!=` line filter is MINTED into
//! returns exactly the rows Loki's `bytes.Contains` would (issue #450).
//! Live ClickHouse, gated behind `PULSUS_TEST_CLICKHOUSE=1`, reusing
//! `explain_indexes.rs`'s harness (`should_run`/`test_config`/`test_ctx`/
//! `run_init`) verbatim, as `rollup_differential.rs` does.
//!
//! **Why this suite exists rather than more corpus rows.** The LogQL
//! corpus runner is hermetic: since issue #449 it *executes* its line
//! filters, but through `compile_for_corpus` → `CompiledPipeline::
//! compile_client_side` (`runner.rs:94-95`), i.e. it evaluates them in
//! Rust and never emits the SQL. It therefore cannot observe a
//! SQL-RENDERING defect at all, which is what #450 was: the old rendering
//! ANDed `hasToken(body, <token>)` onto the exact predicate, and
//! `hasToken` is an exact whole-token membership test, so a needle that
//! is a fragment of a longer token answered `0` on a line that plainly
//! contains it. `|=` dropped matching lines, `!=` kept lines it should
//! have excluded, and a needle containing `_` failed the query outright
//! (`Code: 36 … Needle must not contain whitespace or separator
//! characters`).
//!
//! **Why the needles are generated.** The pre-#450 corpus had five
//! distinct `|=` needles, every one a whole word, so both sides agreed on
//! all of them. The point is not to add the one needle that reproduces
//! the bug — it is that the next needle of a shape nobody enumerated is
//! covered too. Every needle here is a substring of [`BODIES`] taken at a
//! real char boundary, and the construction is deterministic: committed
//! bodies, committed lengths, a `BTreeSet`, and a fixed decimation rule.
//! No RNG, no unordered sampling — a committed test that drifts run to
//! run cannot be a gate.
//!
//! The truth side is `str::contains`, which is exactly Loki's
//! `containsFilter` (`pkg/logql/log/filter.go:435-444` @ `v3.7.4`:
//! `bytes.Contains(line, substr)`); it never touches ClickHouse.
//!
//! Run locally:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=18123 \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test logql_line_filter_differential
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_logql::{LineFilter, LineFilterOp, LineMatch};
use pulsus_model::Fingerprint;
use pulsus_read::logql::Direction;
use pulsus_read::logql::pipeline::CompiledPipeline;
use pulsus_read::logql::predicate::{self, literal};
use pulsus_read::logql::rows::SampleRow;
use pulsus_read::logql::sql::{self, TimeWindow};
use pulsus_schema::{RenderCtx, SchemaParams};
use pulsus_schema_testkit::run_init;

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

fn test_ctx(db: &str) -> SchemaParams {
    RenderCtx::for_tests(db, 7)
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/logql_line_filter_differential.rs for setup)"
            );
            return;
        }
    };
}

const FP: u64 = 18_374_000_000_000_000_001;

/// One log body per row, committed verbatim. Between them they carry every
/// shape the deleted `hasToken` prefilter got wrong, and the shapes any
/// replacement can get wrong:
///
/// - a needle that is a FRAGMENT of a longer alphanumeric run (`:1`, the
///   issue's own reproduction line) — `hasToken` answered `0` on it;
/// - `_` inside a key (`:2`) — `hasToken` raised `BAD_ARGUMENTS` and the
///   whole query failed;
/// - `-`, `.`, `/`, `:` punctuation (`:3`, `:4`);
/// - non-ASCII LETTERS (`:5`) and a non-ASCII NON-alphanumeric separator
///   (`:6`, an em dash — ClickHouse 26.3 keeps it inside a token while
///   Rust's `is_alphanumeric()` splits on it, the third wrong-answer
///   axis) and an emoji (`:7`);
/// - literal `%`, `_`, `\` and `'` (`:8`, `:9`) — the characters the new
///   `LIKE` rendering must escape, so a fix cannot swap one `_` bug for
///   another;
/// - plain word-boundary lines (`:0`, `:10`, `:11`) so token-aligned
///   needles stay covered.
///
/// Bodies must be pairwise distinct: the row set a query returns is
/// mapped back to indices by body text.
const BODIES: &[&str] = &[
    "ts=2026-08-13 level=info msg=request handled status=200",
    "id=wxyz06Q924X3qTas1234 end",
    "user_id=42 path=/api/v1",
    "svc=api-gateway-7 host=node-3.dc-1.internal",
    "GET /api/v1/items?q=1 200 12.5ms",
    "msg=café ok naïve résumé",
    "a—b a€b sep",
    "status 🙂 done 🙂🙂",
    "progress=95% rate=100%/s",
    "path=C:\\logs\\app.log quote='q' pct=%_%",
    "connection refused by upstream",
    "level=error err=timeout after 30s",
];

/// Substring lengths, in CHARS, the generator cuts at every char-boundary
/// offset of every body. Committed: 1–3 are the shortest needles, 4 and up
/// walk to needles spanning several fields.
const LENGTHS: &[usize] = &[1, 2, 3, 4, 5, 6, 8, 11, 16, 24];

/// Upper bound on the needle count, so the CI step stays a few thousand
/// small queries rather than tens of thousands. Decimation is by sort
/// order over the deduped set, which is deterministic.
const MAX_NEEDLES: usize = 512;

/// Needles the decimation must never drop: one per wrong-answer axis, each
/// unioned back in AFTER sampling. Every one of these is a substring of
/// some body, like every generated needle — they are pinned, not special.
const SHAPED: &[&str] = &[
    "06Q924X3qTas",       // token-interior fragment: the issue's reproduction
    "user_id",            // `_`: the old rendering failed the query outright
    "a—b",                // em dash: the non-ASCII tokenization axis
    "🙂",                 // emoji
    "café",               // non-ASCII letters
    "95%",                // LIKE wildcard `%` in the needle
    "C:\\logs",           // backslash in the needle
    "e='q'",              // a quote, i.e. the ch_string boundary
    "%_%",                // both LIKE wildcards, adjacent
    "-3.dc-1",            // punctuation run
    "connection refused", // a whole-token phrase, the pre-#450 corpus's shape
];

/// Every needle this suite drives, deterministically: each body cut at
/// every char boundary for each committed length, deduped, decimated by
/// sort order if the set exceeds [`MAX_NEEDLES`], then unioned with
/// [`SHAPED`] so sampling cannot drop a named axis.
///
/// The selection rule was fixed before any result was seen. There is no
/// RNG and no unordered sampling anywhere in it — the needle set is a
/// function of this file's contents alone.
fn generated_needles() -> Vec<String> {
    let mut set: BTreeSet<String> = BTreeSet::new();
    for body in BODIES {
        let chars: Vec<char> = body.chars().collect();
        for start in 0..chars.len() {
            for &len in LENGTHS {
                if start + len <= chars.len() {
                    set.insert(chars[start..start + len].iter().collect());
                }
            }
        }
    }
    let all: Vec<String> = set.into_iter().collect();
    let mut kept: BTreeSet<String> = if all.len() > MAX_NEEDLES {
        let k = all.len().div_ceil(MAX_NEEDLES);
        all.into_iter().step_by(k).collect()
    } else {
        all.into_iter().collect()
    };
    for shaped in SHAPED {
        kept.insert((*shaped).to_string());
    }
    kept.into_iter().collect()
}

/// Loki's `|=`: `bytes.Contains`, which over UTF-8 `str` is
/// `str::contains`. The truth side never touches ClickHouse.
fn expected_matches(needle: &str) -> BTreeSet<usize> {
    BODIES
        .iter()
        .enumerate()
        .filter(|(_, b)| b.contains(needle))
        .map(|(i, _)| i)
        .collect()
}

async fn drop_database(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

/// Seeds one row per entry of `bodies`, one nanosecond apart, and returns a
/// client bound to `db`.
async fn setup(db: &str, ts_ns: i64, bodies: &[&str]) -> ChClient {
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let data_client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");

    let values: Vec<String> = bodies
        .iter()
        .enumerate()
        .map(|(i, body)| {
            let ts = ts_ns - i64::try_from(i).expect("fits i64");
            // `ch_string` is the escaper the production path uses; the
            // fixture is inserted through it so a body carrying `'` or `\`
            // lands byte-for-byte as written above.
            format!(
                "('checkout', {FP}, {ts}, 0, {})",
                pulsus_read::logql::escape::ch_string(body)
            )
        })
        .collect();
    // A literal `?` in a body (and, below, in a needle) is a bound-argument
    // placeholder to the `clickhouse` crate, so it is doubled exactly as
    // `LogQlEngine::query_stream` does internally
    // (`exec.rs::escape_query_placeholders`, `:3310-3316`) — this file calls
    // `ChClient` directly and bypasses that wrapper.
    let insert = format!(
        "INSERT INTO {db}.log_samples \
         (service, fingerprint, timestamp_ns, severity, body) VALUES {}",
        values.join(", ")
    );
    data_client
        .execute(
            &insert.replace('?', "??"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_samples");
    data_client
}

/// Runs one needle through the REAL mint — `predicate::line_filter` into
/// `sql::stage3` — and returns the body indices the server sends back.
///
/// Any ClickHouse error is a test failure, not a skip: a needle containing
/// `_` used to make the query fail outright, so "the query ran" is itself
/// one of the properties under test (AC3).
async fn matched_indices(
    client: &ChClient,
    db: &str,
    ts_ns: i64,
    op: LineFilterOp,
    needle: &str,
    body_index: &BTreeMap<&str, usize>,
) -> BTreeSet<usize> {
    let filter = LineFilter {
        op,
        head: LineMatch::Text(needle.to_string()),
        or_matches: Vec::new(),
    };
    filter_indices(client, db, ts_ns, &filter, body_index).await
}

/// [`matched_indices`] for any line filter: mints it, runs it through
/// `sql::stage3`, and maps the returned bodies back to indices.
async fn filter_indices(
    client: &ChClient,
    db: &str,
    ts_ns: i64,
    filter: &LineFilter,
    body_index: &BTreeMap<&str, usize>,
) -> BTreeSet<usize> {
    let fragment =
        predicate::line_filter(filter).unwrap_or_else(|e| panic!("`{filter}` must mint: {e:?}"));
    let sql = sql::stage3(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        &format!("{db}.log_samples"),
        &[literal("checkout")],
        &[Fingerprint::from_raw(u128::from(FP)).sql_literal()],
        TimeWindow {
            start_ns: ts_ns - 3_600_000_000_000,
            end_ns: ts_ns + 3_600_000_000_000,
        },
        std::slice::from_ref(&fragment),
        Direction::Backward,
        1_000,
    );
    let mut stream = client
        .query_stream::<SampleRow>(&sql.replace('?', "??"), &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("`{filter}` failed: {e}\n{sql}"));
    let mut out = BTreeSet::new();
    while let Some(row) = stream.next().await {
        let row = row.unwrap_or_else(|e| panic!("`{filter}` row decode failed: {e}"));
        let idx = *body_index
            .get(row.body.as_str())
            .unwrap_or_else(|| panic!("unseeded body returned: {:?}", row.body));
        out.insert(idx);
    }
    out
}

/// AC1/AC2/AC3 (issue #450) in one pass over the generated needle set:
/// `|=` returns exactly `str::contains`, `!=` returns exactly its
/// complement, and no needle errors.
///
/// One pass rather than three tests because the fixture and the needle set
/// are the expensive parts and the three properties are assertions over
/// the same round-trip. A failure names the needle, the op and the SQL.
#[tokio::test]
async fn a_line_filter_returns_exactly_the_rows_str_contains_does_for_every_generated_needle() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_line_filter_diff");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns, BODIES).await;

    let needles = generated_needles();
    let all: BTreeSet<usize> = (0..BODIES.len()).collect();
    let body_index: BTreeMap<&str, usize> =
        BODIES.iter().enumerate().map(|(i, b)| (*b, i)).collect();

    for needle in &needles {
        let expected = expected_matches(needle);

        let got = matched_indices(
            &client,
            db,
            ts_ns,
            LineFilterOp::Contains,
            needle,
            &body_index,
        )
        .await;
        assert_eq!(
            got, expected,
            "`|= {needle:?}` must return exactly the bodies `str::contains` matches"
        );

        let got_neg = matched_indices(
            &client,
            db,
            ts_ns,
            LineFilterOp::NotContains,
            needle,
            &body_index,
        )
        .await;
        let complement: BTreeSet<usize> = all.difference(&expected).copied().collect();
        assert_eq!(
            got_neg, complement,
            "`!= {needle:?}` must return exactly the complement of `|= {needle:?}`"
        );
    }

    drop_database(&client, db).await;
}

/// The generator's own coverage, asserted rather than hoped for — the
/// pre-#450 corpus missed this bug because its five needles were all whole
/// words (AC1/AC3).
///
/// This test is hermetic: it reads no server. It fails if a future edit to
/// `BODIES`, `LENGTHS`, `MAX_NEEDLES` or the decimation quietly stops
/// producing a shape, which is how the corpus lost its coverage in the
/// first place.
#[test]
fn the_generated_needle_set_spans_every_shape_the_old_rendering_got_wrong() {
    let needles = generated_needles();
    assert!(
        needles.len() >= 300,
        "the generated needle set must be substantial, got {}",
        needles.len()
    );

    // Every needle is a real substring of a real body, at a char boundary.
    for n in &needles {
        assert!(
            !n.is_empty() && BODIES.iter().any(|b| b.contains(n.as_str())),
            "generated needle {n:?} is not a substring of any body"
        );
    }

    // A needle that is a FRAGMENT of a longer token — the case `hasToken`
    // answered `0` for while the text was plainly present.
    assert!(
        needles.iter().any(|n| n == "06Q924X3qTas"),
        "the token-interior fragment must be in the needle set"
    );
    // `_`, which used to fail the query outright.
    assert!(
        needles.iter().any(|n| n.contains('_')),
        "at least one needle must contain `_`"
    );
    for (label, pred) in [
        ("`-`", '-'),
        ("`.`", '.'),
        ("`/`", '/'),
        ("`:`", ':'),
        ("`%`", '%'),
        ("`\\`", '\\'),
        ("`'`", '\''),
    ] {
        assert!(
            needles.iter().any(|n| n.contains(pred)),
            "at least one needle must contain {label}"
        );
    }
    // Non-ASCII: letters, a non-alphanumeric separator, an emoji.
    assert!(
        needles.iter().any(|n| n.contains('é')),
        "at least one needle must contain a non-ASCII letter"
    );
    assert!(
        needles.iter().any(|n| n.contains('—')),
        "at least one needle must contain an em dash"
    );
    assert!(
        needles.iter().any(|n| n.contains('🙂')),
        "at least one needle must contain an emoji"
    );
    // The sub-4-byte no-pruning residual, and needles wide enough to span
    // several fields.
    assert!(
        needles.iter().any(|n| n.chars().count() == 1),
        "the shortest committed length must survive decimation"
    );
    assert!(
        needles.iter().any(|n| n.chars().count() >= 16),
        "a long needle must survive decimation"
    );

    // Determinism: the set is a function of this file alone.
    assert_eq!(needles, generated_needles());
}

// --- Issue #757: `word(…)` -------------------------------------------------

/// Bodies for the `word(…)` differential, pairwise distinct: markers 0-11
/// of the plan's fixture, then lines for the case and separator edges.
const WORD_BODIES: &[&str] = &[
    "memcached: connection refused",
    "Memcached: timeout after 3s",
    "MEMCACHED down",
    "xmemcached started",
    "memcached123 started",
    "foo_memcached ready",
    "refused: Connection to memcached",
    "ÜNÏCODE word",
    "ünïcode word",
    "mem cached split",
    "memcached.local resolved",
    "naïve «memcached» quote",
    "connection pool ok",
    "login refused",
    "ΟΔΟΣ final sigma",
    "İstanbul dotted",
    "\u{212A}elvin scale",
    "a—b em dash",
    "tab\tseparated\u{1}ctl",
    "ΑΣ.Β dotted",
    "Α'Σ apostrophe",
    "Straße closed",
];

/// The truth side's own tokenizer, written here and not taken from
/// `pulsus_logql::word`: the whole line lowered then upper-cased, split on
/// every byte below `0x80` that is not an ASCII letter or digit.
fn truth_words(line: &str) -> BTreeSet<Vec<u8>> {
    let keyed = line.to_lowercase().to_uppercase();
    let mut out = BTreeSet::new();
    let mut cur: Vec<u8> = Vec::new();
    for &b in keyed.as_bytes() {
        if b.is_ascii_digit() || b.is_ascii_lowercase() || b.is_ascii_uppercase() || b >= 0x80 {
            cur.push(b);
        } else if !cur.is_empty() {
            out.insert(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.insert(cur);
    }
    out
}

/// The tokens of `line` as written, split by the same byte rule but not
/// keyed.
fn tokens_as_written(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for c in line.chars() {
        if c.is_ascii_alphanumeric() || !c.is_ascii() {
            cur.push(c);
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn word_truth(words: &[String]) -> BTreeSet<usize> {
    let needles: Vec<BTreeSet<Vec<u8>>> = words.iter().map(|w| truth_words(w)).collect();
    WORD_BODIES
        .iter()
        .enumerate()
        .filter(|(_, b)| {
            let have = truth_words(b);
            needles.iter().all(|n| n.iter().all(|t| have.contains(t)))
        })
        .map(|(i, _)| i)
        .collect()
}

fn words_filter(op: LineFilterOp, words: &[String]) -> LineFilter {
    LineFilter {
        op,
        head: LineMatch::Words(words.to_vec()),
        or_matches: Vec::new(),
    }
}

fn client_side_indices(filter: &LineFilter) -> BTreeSet<usize> {
    let p =
        CompiledPipeline::compile_client_side(&[pulsus_logql::Stage::LineFilter(filter.clone())])
            .unwrap_or_else(|e| panic!("`{filter}` must compile: {e:?}"));
    WORD_BODIES
        .iter()
        .enumerate()
        .filter(|(_, b)| p.run(b, &[], 0).expect("no budget").is_some())
        .map(|(i, _)| i)
        .collect()
}

/// Issue #757 D1t: `|= word(…)` and `!= word(…)` return exactly the rows
/// the word rule keeps, through the SQL the database runs and through the
/// in-process matcher alike, for every case form of every word.
#[tokio::test]
async fn a_word_filter_returns_exactly_the_rows_the_word_rule_does() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_word_filter_diff");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns, WORD_BODIES).await;
    let all: BTreeSet<usize> = (0..WORD_BODIES.len()).collect();
    let body_index: BTreeMap<&str, usize> = WORD_BODIES
        .iter()
        .enumerate()
        .map(|(i, b)| (*b, i))
        .collect();

    // Hand-written sets, read from the bodies above.
    for (words, want) in [
        (&["memcached"][..], &[0usize, 1, 2, 5, 6, 10][..]),
        (&["connection", "refused"][..], &[0, 6][..]),
        (&["ünïcode"][..], &[7, 8][..]),
        (&["kelvin"][..], &[16][..]),
        (&["istanbul"][..], &[][..]),
        (&["ΟΔΟΣ"][..], &[14][..]),
        (&["οδος"][..], &[14][..]),
        (&["οδοσ"][..], &[14][..]),
        (&["ΑΣ"][..], &[19][..]),
        (&["ας"][..], &[19][..]),
        (&["ασ"][..], &[19][..]),
        (&["Σ"][..], &[20][..]),
        (&["ς"][..], &[20][..]),
        (&["strasse"][..], &[21][..]),
        (&["STRASSE"][..], &[21][..]),
    ] {
        let words: Vec<String> = words.iter().map(|w| w.to_string()).collect();
        let want: BTreeSet<usize> = want.iter().copied().collect();
        let filter = words_filter(LineFilterOp::Contains, &words);
        let got = filter_indices(&client, db, ts_ns, &filter, &body_index).await;
        assert_eq!(got, want, "`{filter}` against the hand-written set");
        assert_eq!(
            client_side_indices(&filter),
            want,
            "`{filter}` in the process against the hand-written set"
        );
    }

    let mut singles: BTreeSet<String> = BTreeSet::new();
    for body in WORD_BODIES {
        for t in tokens_as_written(body) {
            singles.insert(t.to_ascii_uppercase());
            singles.insert(t.to_lowercase());
            singles.insert(t);
        }
    }
    for extra in [
        "absentword",
        "memcache",
        "οδος",
        "οδοσ",
        "ΑΣ",
        "ας",
        "ασ",
        "Σ",
        "ς",
        "strasse",
        "STRASSE",
        "istanbul",
    ] {
        singles.insert(extra.to_string());
    }
    let mut cases: Vec<Vec<String>> = singles.into_iter().map(|w| vec![w]).collect();
    let mut pair_tokens: BTreeSet<String> = BTreeSet::new();
    for body in [WORD_BODIES[0], WORD_BODIES[6]] {
        pair_tokens.extend(tokens_as_written(body));
    }
    for a in &pair_tokens {
        for b in &pair_tokens {
            if a != b {
                cases.push(vec![a.clone(), b.clone()]);
            }
        }
    }
    assert!(
        cases.len() > 100,
        "the case set is substantial: {}",
        cases.len()
    );

    for words in &cases {
        let truth = word_truth(words);
        let pos = words_filter(LineFilterOp::Contains, words);
        assert_eq!(
            filter_indices(&client, db, ts_ns, &pos, &body_index).await,
            truth,
            "`{pos}` must return exactly the bodies the word rule keeps"
        );
        assert_eq!(client_side_indices(&pos), truth, "`{pos}` in the process");
        let neg = words_filter(LineFilterOp::NotContains, words);
        let complement: BTreeSet<usize> = all.difference(&truth).copied().collect();
        assert_eq!(
            filter_indices(&client, db, ts_ns, &neg, &body_index).await,
            complement,
            "`{neg}` must return exactly the complement"
        );
        assert_eq!(
            client_side_indices(&neg),
            complement,
            "`{neg}` in the process"
        );
    }

    drop_database(&client, db).await;
}

/// Every scalar value as a one-character string, `s`, beside its code
/// point, `cp` — the inner select of D2t and D3t.
const SCALAR_VALUES_SQL: &str = "SELECT number AS cp, CAST(multiIf( \
      number < 0x80, char(number), \
      number < 0x800, char(0xC0 + bitShiftRight(number, 6), 0x80 + bitAnd(number, 0x3F)), \
      number < 0x10000, char(0xE0 + bitShiftRight(number, 12), 0x80 + bitAnd(bitShiftRight(number, 6), 0x3F), 0x80 + bitAnd(number, 0x3F)), \
      char(0xF0 + bitShiftRight(number, 18), 0x80 + bitAnd(bitShiftRight(number, 12), 0x3F), 0x80 + bitAnd(bitShiftRight(number, 6), 0x3F), 0x80 + bitAnd(number, 0x3F)) \
    ) AS String) AS s \
    FROM numbers(0x110000) WHERE NOT (number BETWEEN 0xD800 AND 0xDFFF)";

/// Every scalar value, as a Rust string, by code point.
fn scalar_values() -> impl Iterator<Item = (u64, String)> {
    (0..0x11_0000u32).filter_map(|cp| char::from_u32(cp).map(|c| (u64::from(cp), c.to_string())))
}

#[derive(Debug, Row, serde::Serialize, serde::Deserialize)]
struct KeyedRow {
    cp: u64,
    k: String,
}

/// Issue #757 D2t: `pulsus_logql::word::case_key` keys every scalar value
/// as the database's `upperUTF8(lowerUTF8(·))` does. One value at a time:
/// it guards the single-character tables, and D4t owns context.
#[tokio::test]
async fn case_key_equals_the_databases_for_every_scalar_value() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let sql =
        format!("SELECT cp, upperUTF8(lowerUTF8(s)) AS k FROM ({SCALAR_VALUES_SQL}) WHERE k != s");
    let mut stream = client
        .query_stream::<KeyedRow>(&sql, &QuerySettings::new())
        .await
        .expect("the key statement runs");
    let mut server: BTreeMap<u64, String> = BTreeMap::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("row decodes");
        server.insert(row.cp, row.k);
    }
    let ours: BTreeMap<u64, String> = scalar_values()
        .filter_map(|(cp, s)| {
            let k = pulsus_logql::word::case_key(&s);
            (k != s).then_some((cp, k))
        })
        .collect();
    assert!(
        ours.len() > 1_500,
        "the key changes more than 1,500 values: {}",
        ours.len()
    );
    let ours_pairs: BTreeSet<(u64, &str)> = ours.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let server_pairs: BTreeSet<(u64, &str)> =
        server.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let differ: Vec<&(u64, &str)> = ours_pairs.symmetric_difference(&server_pairs).collect();
    assert!(
        differ.is_empty(),
        "`word::case_key` against `upperUTF8(lowerUTF8(·))`: {} entries differ, the first: {:?}",
        differ.len(),
        &differ[..differ.len().min(10)]
    );
}

#[derive(Debug, Row, serde::Serialize, serde::Deserialize)]
struct InvariantRow {
    total: u64,
    changed: u64,
}

/// Issue #757 D3t: no scalar value's key changes when the value is upper-
/// or lower-cased first, on the server and in `word::case_key`.
#[tokio::test]
async fn case_key_ignores_case_for_every_scalar_value() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let sql = format!(
        "SELECT count() AS total, countIf(NOT (k = upperUTF8(lowerUTF8(upperUTF8(s))) \
         AND k = upperUTF8(lowerUTF8(lowerUTF8(s))))) AS changed FROM ( \
         SELECT s, upperUTF8(lowerUTF8(s)) AS k FROM ({SCALAR_VALUES_SQL}))"
    );
    let mut stream = client
        .query_stream::<InvariantRow>(&sql, &QuerySettings::new())
        .await
        .expect("the invariance statement runs");
    let row = stream.next().await.expect("one row").expect("row decodes");
    assert_eq!((row.total, row.changed), (1_112_064, 0), "the server's key");
    let changed: Vec<u64> = scalar_values()
        .filter(|(_, s)| {
            let k = pulsus_logql::word::case_key(s);
            k != pulsus_logql::word::case_key(&s.to_uppercase())
                || k != pulsus_logql::word::case_key(&s.to_lowercase())
        })
        .map(|(cp, _)| cp)
        .collect();
    assert!(
        changed.is_empty(),
        "`word::case_key` changes under case for {} values, the first: {:x?}",
        changed.len(),
        &changed[..changed.len().min(10)]
    );
}

#[derive(Debug, Row, serde::Serialize, serde::Deserialize)]
struct ContextRow {
    s: String,
    key: String,
    invariant: u8,
    commutes: u8,
}

/// Issue #757 D4t: over every string of one to five symbols from an
/// alphabet built around the final sigma, `ß`, `İ` and U+212A, the
/// server's key is case-invariant, the key of a string splits into the key
/// of each of its words, and `word::case_key` equals the server's key. The
/// key's SQL is `predicate::case_key_sql`, the function the predicate uses.
#[tokio::test]
async fn case_key_equals_the_databases_over_every_short_context_string() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let k = predicate::case_key_sql;
    let sql = format!(
        "WITH ['Σ','σ','ς','A','a','Β','.','''',' ','_','1',char(0xCC,0x81),char(0xC2,0xAD),\
         'İ',char(0xE2,0x84,0xAA),'ß','ẞ'] AS al, length(al) AS n \
         SELECT s, {key} AS key, \
           key = {ku} AND key = {kl} AS invariant, \
           tokens(key, 'splitByNonAlpha') = arrayMap(t -> {kt}, tokens(s, 'splitByNonAlpha')) AS commutes \
         FROM ( \
           SELECT arrayStringConcat(arrayMap(i -> al[(intDiv(number, pow(n, i)) % n) + 1], range(len))) AS s \
           FROM numbers(1419857) ARRAY JOIN [1, 2, 3, 4, 5] AS len WHERE number < pow(n, len))",
        key = k("s"),
        ku = k("upperUTF8(s)"),
        kl = k("lowerUTF8(s)"),
        kt = k("t"),
    );
    let mut stream = client
        .query_stream::<ContextRow>(&sql, &QuerySettings::new())
        .await
        .expect("the context statement runs");
    let (mut rows, mut not_invariant, mut not_commuting) = (0u64, 0u64, 0u64);
    let mut differ: Vec<(String, String, String)> = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("row decodes");
        rows += 1;
        not_invariant += u64::from(row.invariant != 1);
        not_commuting += u64::from(row.commutes != 1);
        let ours = pulsus_logql::word::case_key(&row.s);
        if ours != row.key {
            differ.push((row.s, row.key, ours));
        }
    }
    assert_eq!(rows, 1_508_597, "every string of one to five symbols");
    assert_eq!(
        (not_invariant, not_commuting),
        (0, 0),
        "the server's key: rows not case-invariant, rows whose key does not split into its \
         words' keys"
    );
    assert!(
        differ.is_empty(),
        "`word::case_key` against the server: {} strings differ, the first (s, server, ours): {:?}",
        differ.len(),
        &differ[..differ.len().min(10)]
    );
}
