//! Issue #839: the lowered-literal prefilter in front of a positive line
//! filter, and the one-statement instant sum's empty answers.
//!
//! * **T9** — for every filter of the plan's prefilter table, the engine's
//!   answer equals the answer of the same statement with the prefilter
//!   removed and `use_skip_indexes = 0`, and where a prefilter is rendered,
//!   `idx_body_ngrams` keeps fewer blocks than it is given.
//! * **G3** — the example over a window with no line answers the empty
//!   vector.
//! * **G4** — `sum(bytes_over_time(…))` over one line with an empty body
//!   answers one sample of value `0`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test line_filter_prefilter
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_logql::parse;
use pulsus_read::logql::{Direction, QueryParams, QuerySpec};
use pulsus_read::{EngineConfig, LogQlEngine, QueryResult};
use pulsus_schema::{RenderCtx, SchemaParams};
use pulsus_schema_testkit::run_init;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/line_filter_prefilter.rs for setup)"
            );
            return;
        }
    };
}

fn test_config(database: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(120),
        ..ChConnConfig::default()
    }
}

fn schema_params(db: &str) -> SchemaParams {
    RenderCtx::for_tests(db, 7)
}

fn engine_config(db: &str) -> EngineConfig {
    EngineConfig {
        skip_unavailable_shards: false,
        read_max_memory_bytes: 4 * 1024 * 1024 * 1024,
        db: db.to_string(),
        streams_idx: "log_streams_idx".to_string(),
        streams: "log_streams".to_string(),
        samples: "log_samples".to_string(),
        rollup_table: "log_metrics_5s".to_string(),
        patterns_table: "log_patterns".to_string(),
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
        distributed: false,
    }
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

fn month_of(ts_ns: i64) -> u16 {
    pulsus_model::Date::start_of_month_utc(ts_ns)
        .expect("a representable month")
        .days_since_epoch()
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct LandingRow {
    org_id: String,
    received_ms: i64,
    kind: u8,
    service: String,
    fingerprint: u128,
    timestamp_ns: i64,
    severity: i8,
    body: String,
    structured_metadata: String,
    month: u16,
    labels: String,
    updated_ns: i64,
    pattern: String,
    pattern_count: u64,
    pattern_level: String,
}

fn stream_row(fingerprint: u128, service: &str, labels: &str, ts: i64) -> LandingRow {
    LandingRow {
        kind: 1,
        service: service.to_string(),
        fingerprint,
        month: month_of(ts),
        labels: labels.to_string(),
        updated_ns: ts,
        ..line_row(fingerprint, service, 0, "")
    }
}

fn line_row(fingerprint: u128, service: &str, timestamp_ns: i64, body: &str) -> LandingRow {
    LandingRow {
        org_id: String::new(),
        received_ms: 0,
        kind: 0,
        service: service.to_string(),
        fingerprint,
        timestamp_ns,
        severity: 0,
        body: body.to_string(),
        structured_metadata: String::new(),
        month: 0,
        labels: String::new(),
        updated_ns: 0,
        pattern: String::new(),
        pattern_count: 0,
        pattern_level: String::new(),
    }
}

/// A fresh database with the schema, and a client on it.
async fn fresh(db: &str) -> ChClient {
    let admin = ChClient::new(test_config("default"))
        .await
        .expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
    run_init(&admin, &schema_params(db))
        .await
        .expect("run_init");
    ChClient::new(test_config(db)).await.expect("connect data")
}

async fn drop_db(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

/// Lines per service: two whole blocks of 8,192 rows.
const LINES_PER_SERVICE: usize = 16_384;

/// Service A's lines; B and C have none of them.
const SPECIAL_LINES: [&str; 8] = [
    "eRrOr x",
    "FAILED y",
    "Exception z",
    "panic: w",
    "TA\u{17F}K v",
    "\u{212A}UBELET u",
    "Größe des Puffers",
    "接続エラー",
];

/// The T9 fixture: services `t9a`, `t9b` and `t9c` under `suite="t9"`,
/// 16,384 lines each, in one insert, so the part holds two whole blocks per
/// service. The filler lines hold digits and the word `line` only.
async fn seed_t9(client: &ChClient) -> i64 {
    let ts = now_ns() - 600_000_000_000;
    let mut rows = Vec::with_capacity(3 * LINES_PER_SERVICE + 3);
    for (s, service) in ["t9a", "t9b", "t9c"].into_iter().enumerate() {
        let fp = 83_920 + s as u128;
        rows.push(stream_row(
            fp,
            service,
            &format!(r#"{{"service_name":"{service}","suite":"t9"}}"#),
            ts,
        ));
        for i in 0..LINES_PER_SERVICE {
            let body = if s == 0 && i % 2_000 == 1_000 && i / 2_000 < SPECIAL_LINES.len() {
                SPECIAL_LINES[i / 2_000].to_string()
            } else {
                format!("line {:05} {:05}", i, (i * 7_919) % 100_000)
            };
            rows.push(line_row(fp, service, ts + i as i64 * 1_000, &body));
        }
    }
    client
        .insert_block("log_landing", &rows)
        .await
        .expect("seed log_landing");
    ts
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct ExplainRow {
    explain: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct LineRow {
    timestamp_ns: i64,
    body: String,
}

/// `(kept, given)` of `idx_body_ngrams`, from `EXPLAIN indexes = 1`.
async fn ngram_blocks(client: &ChClient, sql: &str) -> Option<(u64, u64)> {
    let full = format!("EXPLAIN indexes = 1 {sql}\nSETTINGS explain_query_plan_default = 'legacy'")
        .replace('?', "??");
    let mut stream = client
        .query_stream::<ExplainRow>(&full, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("explain failed: {e}\n{full}"));
    let mut text = String::new();
    while let Some(row) = stream.next().await {
        text.push_str(&row.expect("decode").explain);
        text.push('\n');
    }
    let mut in_block = false;
    for line in text.lines().map(str::trim) {
        if let Some(name) = line.strip_prefix("Name: ") {
            in_block = name == "idx_body_ngrams";
        } else if in_block && let Some(rest) = line.strip_prefix("Granules: ") {
            let (m, n) = rest.split_once('/')?;
            return Some((m.trim().parse().ok()?, n.trim().parse().ok()?));
        }
    }
    None
}

/// The lines a statement answers, through a column list of its own.
async fn lines_of(client: &ChClient, sql: &str) -> Vec<(i64, String)> {
    let sql = sql.replace('?', "??");
    let mut stream = client
        .query_stream::<LineRow>(
            &format!("SELECT timestamp_ns, body FROM ({sql}) SETTINGS use_skip_indexes = 0"),
            &QuerySettings::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("control failed: {e}\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode");
        out.push((row.timestamp_ns, row.body));
    }
    out.sort();
    out
}

/// **T9 (issue #839): the prefilter changes no answer, and the n-gram index
/// skips blocks for every filter that renders one.**
#[tokio::test]
async fn the_prefilter_changes_no_answer_and_the_index_skips_blocks() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_839_t9");
    let client = fresh(&db).await;
    let ts = seed_t9(&client).await;
    let engine = LogQlEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        engine_config(&db),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts - 1,
            end_ns: ts + 60_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 1_000,
        direction: Direction::Backward,
    };
    let ms = |strings: &str| format!("multiSearchAny(lowerUTF8(body), [{strings}])");
    // (filter, the prefilter it renders)
    // (filter, the checks it renders, whether every alternative has one)
    let lc = |s: &str| format!("lowerUTF8('{s}')");
    let rows: Vec<(&str, Vec<String>, bool)> = vec![
        (
            r#"|~ "(?i)(error|fatal|panic|exception|fail)""#,
            vec![ms("'error', 'fatal', 'panic', 'exception', 'fail'")],
            true,
        ),
        (
            r#"|~ "(?i)connection.*reset""#,
            vec![ms("'connection'")],
            true,
        ),
        (r#"|~ "^ERROR: ""#, vec![ms("'error: '")], true),
        (r#"|~ "(?i)status""#, vec![ms("'tatu'")], true),
        (r#"|~ "(?i)kubelet""#, vec![ms("'kubelet'")], true),
        (r#"|~ "foo(bar)?bazz""#, vec![ms("'bazz'")], true),
        (
            r#"|= "Connection refused""#,
            vec![ms("'connection refused'")],
            true,
        ),
        // Issue #839 addendum F2: the database lowers a run that is not
        // ASCII.
        (
            r#"|= "Größe des Puffers""#,
            vec![ms(&lc("größe des puffers"))],
            true,
        ),
        (r#"|= "接続エラー""#, vec![ms(&lc("接続エラー"))], true),
        (r#"|= "Größe""#, vec![ms(&lc("größe"))], true),
        // F1: a check per alternative.
        (
            r#"|= "abc" or "connection""#,
            vec![ms("'connection'")],
            false,
        ),
        (
            r#"|= word("x") or "connection""#,
            vec![ms("'connection'")],
            false,
        ),
        (
            r#"|= word("refused") or "nomatchzzzz""#,
            vec![ms("'nomatchzzzz'")],
            false,
        ),
        (r#"|~ "err.*""#, vec![], false),
        (r#"|~ "(?i)(error|ok)""#, vec![], false),
        (r#"|~ "(?i)task""#, vec![], false),
        (r#"|~ "(?i)ΣΦΑΛΜΑ""#, vec![], false),
        (r#"|= ip("192.0.2.1")"#, vec![], false),
        (r#"!= "error""#, vec![], false),
        (r#"!~ "(?i)error""#, vec![], false),
    ];
    let mut failures: Vec<String> = Vec::new();
    for (filter, checks, prunes) in &rows {
        let query = format!(r#"{{suite="t9"}} {filter}"#);
        let expr = parse(&query).expect("parse");
        let (result, _, explain) = match engine.query_explained(&expr, &params).await {
            Ok(r) => r,
            Err(e) => {
                failures.push(format!("{query}: {e:?}"));
                continue;
            }
        };
        let QueryResult::Streams { items, .. } = &result else {
            panic!("{query}: expected streams");
        };
        let mut answer: Vec<(i64, String)> = items
            .iter()
            .flat_map(|s| s.entries.iter().cloned())
            .collect();
        answer.sort();
        let Some(stage3) = explain
            .stages
            .iter()
            .find(|s| s.name == "stage3_samples")
            .map(|s| s.sql.clone())
        else {
            failures.push(format!("{query}: no sample statement"));
            continue;
        };
        let mut control = stage3.clone();
        for p in checks {
            let conjunct = format!("{p} AND ");
            if !control.contains(&conjunct) {
                failures.push(format!("{query}: the statement lacks `{p}`"));
            }
            control = control.replacen(&conjunct, "", 1);
        }
        if control.contains("multiSearchAny(lowerUTF8(body)") {
            failures.push(format!("{query}: a check beyond the expected:\n{stage3}"));
        }
        // `ip(…)` runs in the engine, after a statement with no body
        // predicate: no line holds an address, so the answer is empty.
        let want = if filter.contains("ip(") {
            Vec::new()
        } else {
            lines_of(&client, &control).await
        };
        if format!("{answer:?}") != format!("{want:?}") {
            failures.push(format!(
                "{query}: the engine answered {answer:?}, the statement without the prefilter \
                 and the skip indexes {want:?}"
            ));
        }
        if *prunes {
            match ngram_blocks(&client, &stage3).await {
                Some((kept, given)) if kept < given => {
                    eprintln!("T9 {query}: idx_body_ngrams kept {kept} of {given} blocks");
                }
                other => failures.push(format!(
                    "{query}: idx_body_ngrams must keep fewer blocks than it is given, \
                     (kept, given) = {other:?}"
                )),
            }
        }
    }
    drop_db(&client, &db).await;
    assert!(
        failures.is_empty(),
        "{} row(s) differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// The G3 and G4 fixture: one stream `{host="h", job="a"}` with one line of
/// an empty body, 60 s before the returned instant.
async fn seed_one_empty_line(client: &ChClient) -> i64 {
    let t = now_ns() - 600_000_000_000;
    let rows = vec![
        stream_row(83_930, "g34", r#"{"host":"h","job":"a"}"#, t),
        line_row(83_930, "g34", t - 60_000_000_000, ""),
    ];
    client
        .insert_block("log_landing", &rows)
        .await
        .expect("seed log_landing");
    t
}

async fn instant(db: &str, query: &str, at_ns: i64) -> Vec<(Vec<(String, String)>, u64)> {
    let engine = LogQlEngine::new(
        ChClient::new(test_config(db)).await.expect("connect"),
        engine_config(db),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let params = QueryParams {
        spec: QuerySpec::Instant { at_ns },
        limit: 100,
        direction: Direction::Backward,
    };
    let (result, _) = engine
        .query(&parse(query).expect("parse"), &params)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
    let QueryResult::Vector(samples) = result else {
        panic!("{query}: expected a vector, got {result:?}");
    };
    samples
        .into_iter()
        .map(|s| (s.labels, s.value.to_bits()))
        .collect()
}

/// **G3 (issue #839): the example over a window with no line answers the
/// empty vector.**
#[tokio::test]
async fn the_example_over_an_empty_window_answers_nothing() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_839_g3");
    let client = fresh(&db).await;
    let t = seed_one_empty_line(&client).await;
    let got = instant(
        &db,
        r#"sum(count_over_time({job=~".+", host=~".+", namespace=~".*", service_name=~".*", pod=~".*", container=~".*", unit=~".*", stream=~".*"} | detected_level=~".*" |~ "(?i)" !~ "(?i)^$" |~ "(?i)(error|fatal|panic|exception|fail)" [1h]))"#,
        t - 7_200_000_000_000,
    )
    .await;
    drop_db(&client, &db).await;
    assert_eq!(got, Vec::new());
}

/// **G4 (issue #839): bytes over one empty line is one sample of `0`.**
#[tokio::test]
async fn bytes_over_an_empty_line_answer_zero() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_839_g4");
    let client = fresh(&db).await;
    let t = seed_one_empty_line(&client).await;
    let got = instant(&db, r#"sum(bytes_over_time({job="a"}[1h]))"#, t).await;
    drop_db(&client, &db).await;
    assert_eq!(got, vec![(Vec::new(), 0f64.to_bits())]);
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct LoweringFailures {
    without_sigma: u64,
    with_sigma: u64,
}

/// **G5 (issue #839 addendum F2): the database lowers a character the same
/// way alone and between neighbours, for every code point but U+03A3.**
/// One read-only statement over `numbers()`: each code point outside the
/// surrogates, lowered alone, must appear in the lowering of itself between
/// any two of ten neighbours. It reads no table, and runs only against the
/// server this suite is given.
#[tokio::test]
async fn the_database_lowers_every_character_but_sigma_without_context() {
    skip_unless_live!();
    let client = ChClient::new(test_config("default"))
        .await
        .expect("connect");
    let sql = "SELECT toUInt64(countIf(fails AND cp != 931)) AS without_sigma, \
                      toUInt64(countIf(fails)) AS with_sigma \
               FROM (SELECT cp, \
                       arrayExists(l -> arrayExists(r -> \
                         position(lowerUTF8(concat(l, x, r)), lowerUTF8(x)) = 0, ns), ns) AS fails \
                     FROM (SELECT toUInt32(number) AS cp, \
                             multiIf(cp < 128, char(cp), \
                                     cp < 2048, char(192 + intDiv(cp, 64), 128 + cp % 64), \
                                     cp < 65536, char(224 + intDiv(cp, 4096), \
                                                      128 + intDiv(cp, 64) % 64, 128 + cp % 64), \
                                     char(240 + intDiv(cp, 262144), 128 + intDiv(cp, 4096) % 64, \
                                          128 + intDiv(cp, 64) % 64, 128 + cp % 64)) AS x, \
                             ['A', ' ', '1', 'İ', 'ς', '\u{307}', 'ß', 'Ω', 'Α', 'Σ'] AS ns \
                           FROM numbers(1114112) \
                           WHERE number < 55296 OR number > 57343)) \
               SETTINGS max_memory_usage = 4294967296";
    let mut stream = client
        .query_stream::<LoweringFailures>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let got = stream.next().await.expect("one row").expect("decode");
    assert_eq!(
        (got.without_sigma, got.with_sigma),
        (0, 1),
        "code points the database lowers differently in context"
    );
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct CountRow {
    n: u64,
}

async fn count_of(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<CountRow>(&sql.replace('?', "??"), &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    stream.next().await.expect("one row").expect("decode").n
}

/// **A5 (issue #839, code review r1): a parsed-label filter's pushed body
/// predicate reaches the lowered index.** `| regexp "(?P<level>.*)" |
/// level="error"` pushes a substring probe of the value; it carries the
/// lowered-literal check, so `idx_body_ngrams` on `lowerUTF8(body)` keeps
/// no more blocks than `main`'s probe kept on an index over `body`, and the
/// answer is the same. `main`'s side is a table with `main`'s index holding
/// the same rows, read by `main`'s probe, `body LIKE '%error%'`.
#[tokio::test]
async fn a_parsed_label_filter_reaches_the_lowered_index() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_839_a5");
    let client = fresh(&db).await;
    let ts = now_ns() - 600_000_000_000;
    let mut rows = vec![stream_row(
        83_940,
        "a5",
        r#"{"service_name":"a5","suite":"a5"}"#,
        ts,
    )];
    for i in 0..200_000usize {
        let body = if i % 40_000 == 7 {
            "error".to_string()
        } else {
            format!("line {i:06} routine request completed")
        };
        rows.push(line_row(83_940, "a5", ts + i as i64 * 1_000, &body));
    }
    client
        .insert_block("log_landing", &rows)
        .await
        .expect("seed log_landing");
    for sql in [
        "CREATE TABLE main_body (body String, INDEX idx_body_ngrams body \
         TYPE ngrambf_v1(4, 32768, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple()"
            .to_string(),
        "INSERT INTO main_body SELECT body FROM log_samples ORDER BY service, fingerprint, \
         timestamp_ns"
            .to_string(),
        "OPTIMIZE TABLE log_samples FINAL".to_string(),
    ] {
        client
            .execute(&sql, &QuerySettings::new(), Idempotency::NonIdempotent)
            .await
            .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    }
    let fragment = pulsus_read::logql::predicate::parsed_string_filter(
        "level",
        pulsus_logql::MatchOp::Eq,
        "error",
        &pulsus_logql::ParserStage::Regexp("(?P<level>.*)".to_string()),
    )
    .expect("route B serves `=`");
    let ours = fragment.as_sql();
    let mains = "body LIKE '%error%'";
    let ours_blocks = ngram_blocks(
        &client,
        &format!("SELECT count() FROM log_samples WHERE {ours}"),
    )
    .await;
    let mains_blocks = ngram_blocks(
        &client,
        &format!("SELECT count() FROM main_body WHERE {mains}"),
    )
    .await;
    let ours_n = count_of(
        &client,
        &format!("SELECT count() AS n FROM log_samples WHERE {ours}"),
    )
    .await;
    let mains_n = count_of(
        &client,
        &format!("SELECT count() AS n FROM main_body WHERE {mains}"),
    )
    .await;
    let unindexed = count_of(
        &client,
        &format!("SELECT count() AS n FROM log_samples WHERE {ours} SETTINGS use_skip_indexes = 0"),
    )
    .await;
    let engine = LogQlEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        engine_config(&db),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts - 1,
            end_ns: ts + 600_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 1_000,
        direction: Direction::Backward,
    };
    let (result, _) = engine
        .query(
            &parse(r#"{suite="a5"} | regexp "(?P<level>.*)" | level="error""#).expect("parse"),
            &params,
        )
        .await
        .expect("the query answers");
    let QueryResult::Streams { items, .. } = result else {
        panic!("expected streams");
    };
    let entries: usize = items.iter().map(|s| s.entries.len()).sum();
    drop_db(&client, &db).await;
    eprintln!("A5: ours {ours_blocks:?}, main's {mains_blocks:?}, {ours}");
    let (Some((kept, given)), Some((main_kept, _))) = (ours_blocks, mains_blocks) else {
        panic!(
            "both statements must list idx_body_ngrams: ours {ours_blocks:?}, main's {mains_blocks:?}"
        );
    };
    assert!(
        kept < given && kept <= main_kept,
        "kept {kept} of {given}; main's probe kept {main_kept}"
    );
    assert_eq!((ours_n, mains_n, unindexed, entries), (5, 5, 5, 5));
}
