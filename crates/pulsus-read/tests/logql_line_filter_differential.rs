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
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_logql::{LineFilter, LineFilterOp};
use pulsus_read::logql::Direction;
use pulsus_read::logql::predicate::{self, literal};
use pulsus_read::logql::rows::SampleRow;
use pulsus_read::logql::sql::{self, TimeWindow};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

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
    RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    }
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
/// offset of every body. Committed: 1–3 exercise the sub-4-byte no-pruning
/// residual, 4 is exactly the `ngrambf_v1` order, and the rest walk up to
/// needles spanning several fields.
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

/// Seeds one row per [`BODIES`] entry, one nanosecond apart, and returns a
/// client bound to `db`.
async fn setup(db: &str, ts_ns: i64) -> ChClient {
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let data_client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");

    let values: Vec<String> = BODIES
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
    let fragment = predicate::line_filter(&LineFilter {
        op,
        value: needle.to_string(),
        value_is_ip: false,
        or_matches: Vec::new(),
    })
    .unwrap_or_else(|e| panic!("{op:?} {needle:?} must mint: {e:?}"));
    let sql = sql::stage3(
        &format!("{db}.log_samples"),
        &[literal("checkout")],
        &[FP],
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
        .unwrap_or_else(|e| panic!("{op:?} {needle:?} failed: {e}\n{sql}"));
    let mut out = BTreeSet::new();
    while let Some(row) = stream.next().await {
        let row = row.unwrap_or_else(|e| panic!("{op:?} {needle:?} row decode failed: {e}"));
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
    let client = setup(db, ts_ns).await;

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
