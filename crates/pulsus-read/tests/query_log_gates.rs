//! CI regression gate for docs/schemas.md §9's two-tier evidence model,
//! Tier 1 (issue #16): asserts **scale-invariant** `system.query_log`
//! ratios on a deterministic CI-scale corpus. Gated behind
//! `PULSUS_TEST_CLICKHOUSE=1`, reusing `crates/pulsus-read/tests/
//! explain_indexes.rs`'s connection/setup pattern verbatim — the CI
//! `schema-it` job runs this after the EXPLAIN gate, against the same
//! ClickHouse 26.3 container.
//!
//! **Why ratios, not absolute counts (edge case #5 of the #16 architect
//! plan).** `read_rows`/`read_bytes`/`SelectedMarks` all scale with corpus
//! size; an absolute threshold breaks the moment the corpus grows or
//! shrinks. Every assertion here is instead a ratio: `read_rows` relative
//! to `index_granularity` (proving primary-index confinement to a narrow
//! window, not corpus size), and `SelectedMarks` relative to the corpus's
//! own total mark count (proving skip-index pruning, not an absolute
//! granule count).
//!
//! **Corpus sizing (edge case #4).** A too-small corpus can't prove
//! granule skipping — every granule fits in one skip-index check either
//! way. [`CORPUS_ROWS`] (100,000, one stream) yields ~13 marks at the
//! default `index_granularity = 8192`
//! ([`total_marks`], asserted by `corpus_is_large_enough_to_prove_skip_index_pruning`),
//! comfortably `total_marks > selected_marks` while staying a
//! minutes-scale CI load. The needle body is injected at a **known,
//! narrow row range** ([`NEEDLE_START`]/[`NEEDLE_COUNT`]) so body-search
//! selectivity is a controlled constant, not incidental to random data.
//!
//! Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.8.21.10
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test query_log_gates
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_logql::parse;
use pulsus_model::Fingerprint;
use pulsus_read::logql::predicate::{literal, month_literal};
use pulsus_read::logql::sql::{self, TimeWindow};
use pulsus_read::logql::{Direction, Plan, PlanCtx, QueryParams, QuerySpec, plan};
use pulsus_read::{EngineConfig, LogQlEngine, QueryResult, ReadError};
use pulsus_schema::{RenderCtx, SchemaParams};
use pulsus_schema_testkit::run_init;

// Issue #579: the CPU corpus and the two-engine harness, shared with
// `live_metrics_pushed_rate.rs`.
#[path = "pushed_rate_corpus/mod.rs"]
mod pushed_rate_corpus;
// Issue #579 part 2: the multi-name corpus.
#[path = "multi_name_corpus/mod.rs"]
mod multi_name_corpus;

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/query_log_gates.rs for setup)"
            );
            return;
        }
    };
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
        query_timeout: Duration::from_secs(60),
        ..ChConnConfig::default()
    }
}

fn test_ctx(db: &str) -> SchemaParams {
    RenderCtx::for_tests(db)
}

fn plan_ctx(db: &str) -> PlanCtx<'_> {
    PlanCtx {
        tenant: Box::leak(Box::new(
            pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        )),
        db,
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    }
}

/// Nanoseconds since the Unix epoch, right now. See
/// `explain_indexes.rs::now_ns`'s doc comment: fixture timestamps must be
/// wall-clock-recent, never a fixed historical constant, given
/// `log_samples`'s `ttl_only_drop_parts = 1` retention.
fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

const FP_CORPUS: u64 = 18_374_000_000_000_000_002;
const SERVICE: &str = "ci-scale-svc";

/// ClickHouse's default `index_granularity` (docs/schemas.md §8) — every
/// ratio gate below is expressed relative to this, never to
/// [`CORPUS_ROWS`] directly, so the gate stays meaningful if the corpus
/// size ever changes.
const INDEX_GRANULARITY: u64 = 8192;

/// One stream, spanning the last hour, spaced 36ms apart (100,000 rows *
/// 36ms ~= 1h) — large enough to span multiple granules (~13 marks at the
/// default granularity) while completing in well under a minute on a CI
/// runner.
const CORPUS_ROWS: u64 = 100_000;

/// The needle only appears in a narrow, known sub-range near the middle of
/// the corpus — a controlled selectivity constant, not incidental.
const NEEDLE: &str = "zzqneedle9f3ac2";
const NEEDLE_START: u64 = 50_000;
const NEEDLE_COUNT: u64 = 4;

/// A cheap, deterministic 64-bit mix (splitmix64, matching the project's
/// no-`rand`-for-committed-baselines convention —
/// `xtask/src/ch_bench/rows.rs`) used only for realistic byte-length
/// jitter in generated bodies, not for anything load-bearing to the
/// assertions below.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSampleRow {
    /// The tenant (issue #653): the empty tenant's rows.
    org_id: String,
    service: String,
    fingerprint: u128,
    timestamp_ns: i64,
    severity: i8,
    body: String,
}

/// Drops `db` if it exists, then delegates to [`seed_corpus`]. Used by the
/// scale-invariant ratio gates, which reuse a fixed database name across
/// runs and so must clear stale state first. Returns `(client, ts_ns)`.
async fn setup_corpus(db: &str) -> (ChClient, i64) {
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
    seed_corpus(db).await
}

/// Initializes the schema in `db` (`run_init`) and bulk-loads
/// [`CORPUS_ROWS`] rows for one stream via direct RowBinary insert
/// (`ChClient::insert_block`) — the same bulk-load mechanism `xtask bench
/// logs-read`'s dataset generator uses, licensed for fidelity by
/// `crates/pulsus-write/tests/ingest_fidelity.rs`. Does NOT drop `db`, so a
/// caller that created a fresh unique database with a strict `CREATE
/// DATABASE` (the #90 query_log gates) keeps that create as the sole
/// database creation. Returns `(client, ts_ns)`: `client` is bound to `db`,
/// `ts_ns` is the corpus's start timestamp.
async fn seed_corpus(db: &str) -> (ChClient, i64) {
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    run_init(&admin, &test_ctx(db)).await.expect("run_init");

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");

    let ts_ns = now_ns() - 3_600_000_000_000; // corpus start: 1h ago
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) \
                 VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({ts_ns}))), {FP_CORPUS}, \
                 '{SERVICE}', '{{\"service_name\":\"{SERVICE}\"}}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");
    land_seeded_streams(&client, db).await;
    let mut rows = Vec::with_capacity(CORPUS_ROWS as usize);
    for i in 0..CORPUS_ROWS {
        let jitter = (splitmix64(i) % 1000) as i64;
        let timestamp_ns = ts_ns + (i as i64) * 36_000_000 + jitter;
        let body = if (NEEDLE_START..NEEDLE_START + NEEDLE_COUNT).contains(&i) {
            format!("row {i} {NEEDLE} padding_{}", "x".repeat(120))
        } else {
            format!(
                "row {i} routine request completed padding_{}",
                "x".repeat(120)
            )
        };
        rows.push(SeedSampleRow {
            org_id: String::new(),
            service: SERVICE.to_string(),
            fingerprint: u128::from(FP_CORPUS),
            timestamp_ns,
            severity: 0,
            body,
        });
    }
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("bulk insert corpus");
    land_seeded_lines(&client, db).await;

    (client, ts_ns)
}

fn streams_plan(query: &str, params: &QueryParams, db: &str) -> pulsus_read::logql::StreamsPlan {
    let expr = parse(query).expect("parse");
    match plan(&expr, params, &plan_ctx(db)).expect("plan") {
        Plan::Streams(sp) => sp,
        Plan::Metric(_) | Plan::MetricBinary(_) => panic!("expected a Streams plan"),
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct QueryLogRow {
    read_rows: u64,
    read_bytes: u64,
    selected_marks: u64,
    /// `Settings['use_query_condition_cache']`, read back so the pin in
    /// [`run_and_capture`] cannot be deleted silently. `system.query_log`
    /// only records settings a query actually changed, so this is `"0"`
    /// when the pin is in place and `""` when it is not (measured on
    /// 26.3.17.110).
    condition_cache: String,
}

/// Runs `sql` tagged with a unique `query_id`, draining every row of
/// `R`'s shape (the return count itself is not needed by every caller,
/// only that the stream is fully consumed before `SYSTEM FLUSH LOGS` —
/// `system.query_log`'s `QueryFinish` row is only written once the query
/// has fully completed), flushes logs, and reads back the evidence.
///
/// **`use_query_condition_cache = 0` is deliberate — do not delete it.**
/// ClickHouse turned the query-condition cache on by default at 25.4
/// (`system.settings_changes`), so from 26.3 a repeated identical query
/// can be served partly from that cache. The cache can only *reduce*
/// `read_rows`/`read_bytes`/`SelectedMarks`, so it cannot redden a gate
/// wrongly — but it can make one pass by having CACHED rather than by
/// having PRUNED, and proving the index pruned is the entire point of
/// these gates. Coordinator ruling on issue #376, 2026-08-06: pin it here
/// so the gates keep measuring the index. The pin is read back out of
/// `system.query_log` and asserted below, so removing the `.set(...)`
/// line reddens the suite instead of quietly weakening it.
async fn run_and_capture<R: pulsus_clickhouse::ChRow>(
    client: &ChClient,
    admin: &ChClient,
    sql: &str,
    query_id: &str,
) -> (u64, QueryLogRow) {
    let settings = QuerySettings::new()
        .set("query_id", query_id)
        .set("use_query_condition_cache", 0);
    let mut returned = 0u64;
    let mut stream = client
        .query_stream::<R>(sql, &settings)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    while let Some(row) = stream.next().await {
        row.expect("decode row");
        returned += 1;
    }
    drop(stream);

    admin
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");

    let log_sql = format!(
        "SELECT read_rows, read_bytes, ProfileEvents['SelectedMarks'] AS selected_marks, \
         Settings['use_query_condition_cache'] AS condition_cache \
         FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish' \
         ORDER BY event_time_microseconds DESC LIMIT 1"
    );
    let mut log_stream = admin
        .query_stream::<QueryLogRow>(&log_sql, &QuerySettings::new())
        .await
        .expect("query system.query_log");
    let evidence = log_stream
        .next()
        .await
        .unwrap_or_else(|| panic!("no query_log row for query_id {query_id}"))
        .expect("decode query_log row");
    assert_eq!(
        evidence.condition_cache, "0",
        "every gated query must run with use_query_condition_cache = 0 so these gates measure \
         index pruning rather than a cache hit (issue #376); the server reported \
         {:?} for query_id {query_id}",
        evidence.condition_cache
    );
    (returned, evidence)
}

/// The in-run pruning identity that replaced the `SelectedMarks` ratio.
///
/// Runs `gated_sql` and `control_sql` against the **same server in the
/// same test**, both through [`run_and_capture`], and asserts the gated
/// form reads at least `min_byte_ratio`× fewer bytes and `min_row_ratio`×
/// fewer rows than the control. Both numbers come from the server, so a
/// pasted expectation cannot satisfy it — which is what makes
/// "regenerate until green" insufficient rather than merely discouraged.
///
/// `read_bytes` leads because it is what a user pays for, and because it
/// is the quantity that survives lazy materialization (default-on from
/// 25.4, `system.settings_changes`) — a read can touch more rows and
/// still cost less, because the wide `body` column is only materialized
/// for the rows that survive. Rows are asserted too, but second.
///
/// Measured for issue #376 on a faithful SQL reproduction of this
/// suite's corpus shape (100_000 rows, one stream, 4 needle rows,
/// ~150-byte bodies), same query on both servers:
///
/// | | 24.8.14.39 | 26.3.17.110 |
/// |---|---|---|
/// | gated `read_rows` | 8_192 | 8_192 |
/// | gated `read_bytes` | 1_556_440 | 1_382_508 |
/// | gated `SelectedMarks` | 1 / 14 | 13 / 14 |
///
/// So on this shape 26.3 reads the same rows for 11% fewer bytes while
/// `SelectedMarks` moves from 1 to 13 — the marks number is the only one
/// that regressed, and it regressed as a *measure*, not as a cost.
async fn assert_index_pruning_by_bytes(
    client: &ChClient,
    admin: &ChClient,
    gated_sql: &str,
    control_sql: &str,
    min_byte_ratio: f64,
    min_row_ratio: f64,
    what: &str,
) -> QueryLogRow {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let (_, gated) = run_and_capture::<pulsus_read::logql::rows::SampleRow>(
        client,
        admin,
        gated_sql,
        &format!("qlg-prune-gated-{nonce}"),
    )
    .await;
    let (_, control) = run_and_capture::<pulsus_read::logql::rows::SampleRow>(
        client,
        admin,
        control_sql,
        &format!("qlg-prune-control-{nonce}"),
    )
    .await;

    let byte_ratio = control.read_bytes as f64 / gated.read_bytes.max(1) as f64;
    let row_ratio_report = control.read_rows as f64 / gated.read_rows.max(1) as f64;
    // Printed so a CI log records the measurement, not just the verdict —
    // the delta table in docs/benchmarks/clickhouse-26.3-plan-deltas.md is
    // read against these numbers.
    eprintln!(
        "{what}: gated {} rows / {} bytes; control {} rows / {} bytes; \
         ratios {row_ratio_report:.2}x rows, {byte_ratio:.2}x bytes",
        gated.read_rows, gated.read_bytes, control.read_rows, control.read_bytes
    );
    assert!(
        byte_ratio >= min_byte_ratio,
        "{what}: read_bytes ratio {byte_ratio:.2}x (control {} / gated {}) is below the \
         pre-committed {min_byte_ratio}x — the skip index is no longer keeping the read off \
         the corpus",
        control.read_bytes,
        gated.read_bytes
    );
    let row_ratio = control.read_rows as f64 / gated.read_rows.max(1) as f64;
    assert!(
        row_ratio >= min_row_ratio,
        "{what}: read_rows ratio {row_ratio:.2}x (control {} / gated {}) is below the \
         pre-committed {min_row_ratio}x",
        control.read_rows,
        gated.read_rows
    );
    gated
}

/// Total marks the corpus's `log_samples` table holds, read straight off
/// `system.parts` rather than assumed from
/// [`CORPUS_ROWS`]/[`INDEX_GRANULARITY`], so a gate reflects the table's
/// real physical layout.
///
/// **Boundary (issue #376).** This is no longer a denominator for skip-
/// index pruning. From ClickHouse 26.1 `use_skip_indexes_on_data_read` is
/// default-on and moves skip filtering out of mark selection and into the
/// data read, so `SelectedMarks` stops tracking how much a skip index
/// pruned — measured on this corpus's shape, 1/14 on 24.8.14.39 against
/// 13/14 on 26.3.17.110 for the identical query and identical
/// `read_rows` (8_192 on both). It
/// survives as a corpus-size sanity check
/// (`corpus_is_large_enough_to_prove_skip_index_pruning`) and as the
/// denominator of a loose byte ceiling.
async fn total_marks(admin: &ChClient, db: &str) -> u64 {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct MarksRow {
        marks: u64,
    }
    let sql = format!(
        "SELECT sum(marks) AS marks FROM system.parts WHERE database = '{db}' \
         AND table = 'log_samples' AND active"
    );
    let mut stream = admin
        .query_stream::<MarksRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.parts");
    stream
        .next()
        .await
        .expect("one row from system.parts sum()")
        .expect("decode marks row")
        .marks
}

#[tokio::test]
async fn corpus_is_large_enough_to_prove_skip_index_pruning() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_qlg_size");
    let (client, _ts_ns) = setup_corpus(db).await;
    let marks = total_marks(&client, db).await;
    // Edge case #4 of the #16 architect plan: a too-small corpus can't
    // prove granule skipping. Guards the gate itself from silently going
    // meaningless if `CORPUS_ROWS` is ever shrunk.
    assert!(
        marks >= 10,
        "CI corpus must span enough granules to make skip-index pruning \
         observable (got {marks} marks; need >= 10)"
    );
}

#[tokio::test]
async fn stage3_narrow_window_read_rows_are_index_confined_not_a_full_scan() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_qlg_narrow");
    let (client, ts_ns) = setup_corpus(db).await;

    // A window covering ~1,000 of the corpus's 100,000 rows (rows
    // [40_000, 41_000)) — narrow enough that a genuinely index-confined
    // read should touch only a couple of granules, wide enough to be a
    // realistic "last N minutes" shape.
    let window_start = ts_ns + 40_000 * 36_000_000;
    let window_end = ts_ns + 41_000 * 36_000_000;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: window_start,
            end_ns: window_end,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let sp = streams_plan(&format!(r#"{{service_name="{SERVICE}"}}"#), &params, db);
    let sql = sql::stage3(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        &format!("{db}.log_samples"),
        &[literal(SERVICE)],
        &[Fingerprint::from_raw(u128::from(FP_CORPUS)).sql_literal()],
        TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        },
        &sp.line_filters,
        sp.direction,
        sp.scan_limit,
    );

    let (returned, evidence) = run_and_capture::<pulsus_read::logql::rows::SampleRow>(
        &client,
        &client,
        &sql,
        "qlg-narrow-window",
    )
    .await;

    assert!(returned > 0, "the seeded window must return rows");
    // Scale-invariant bound: read_rows relative to index_granularity, not
    // to CORPUS_ROWS. K=4 is generous slack for granule-boundary overlap;
    // the load-bearing fact is that it is nowhere near the corpus total.
    let bound = 4 * INDEX_GRANULARITY;
    assert!(
        evidence.read_rows <= bound,
        "stage-3 read_rows ({}) exceeded {bound} (4 granules) for a window that only needed \
         ~1,000 rows out of a {CORPUS_ROWS}-row corpus — primary-index confinement regressed",
        evidence.read_rows
    );
    assert!(
        evidence.read_rows < CORPUS_ROWS / 2,
        "stage-3 read_rows ({}) was not meaningfully smaller than the corpus \
         ({CORPUS_ROWS}) — looks like a full scan",
        evidence.read_rows
    );
}

#[tokio::test]
async fn body_search_skip_index_prunes_most_granules() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_qlg_bodysearch");
    let (client, ts_ns) = setup_corpus(db).await;

    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts_ns - 3_600_000_000_000,
            end_ns: ts_ns + 3_600_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 1_000,
        direction: Direction::Backward,
    };
    let sp = streams_plan(
        &format!(r#"{{service_name="{SERVICE}"}} |= "{NEEDLE}""#),
        &params,
        db,
    );
    let sql = sql::stage3(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        &format!("{db}.log_samples"),
        &[literal(SERVICE)],
        &[Fingerprint::from_raw(u128::from(FP_CORPUS)).sql_literal()],
        TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        },
        &sp.line_filters,
        sp.direction,
        sp.scan_limit,
    );

    let (returned, _) = run_and_capture::<pulsus_read::logql::rows::SampleRow>(
        &client,
        &client,
        &sql,
        "qlg-body-search",
    )
    .await;
    assert_eq!(
        returned, NEEDLE_COUNT,
        "body search must return exactly the seeded needle rows"
    );

    let total = total_marks(&client, db).await;
    assert!(
        total > 0,
        "corpus must have marks to compute a ratio against"
    );

    // The skip-index pruning gate. It used to be
    // `SelectedMarks/total_marks <= 0.5`. That assertion stopped
    // measuring what it claimed at ClickHouse 26.1, where
    // `use_skip_indexes_on_data_read` became default-on
    // (`system.settings_changes`) and moved skip filtering from mark
    // SELECTION to data READ: the marks are all selected and the
    // filtering happens inside them. Measured for issue #376 on this
    // corpus's shape: 24.8.14.39 reported 1/14 marks, 26.3.17.110
    // reports 13/14 = 0.929 for the identical query — so the old
    // assertion FAILS on 26.3 while `read_rows` is identical (8_192 on
    // both) and `read_bytes` actually FELL 11%. Marks stopped tracking
    // pruning; the read did not get worse.
    //
    // The replacement is a same-server control identity rather than a
    // re-tuned constant: run the same SQL with `use_skip_indexes = 0`
    // and require the gated form to read several times less. Both
    // numbers come from the server in this run, so no pasted number can
    // satisfy it. See [`assert_index_pruning_by_bytes`] for why bytes
    // lead.
    //
    // The ratios are pre-committed floors well under the values this
    // suite measures on 26.3 (12.2x bytes / 6.6x rows on the run that
    // introduced them), so they fire on a genuine loss of pruning, not
    // on corpus drift.
    let control_sql = format!("{sql} SETTINGS use_skip_indexes = 0");
    let evidence = assert_index_pruning_by_bytes(
        &client,
        &client,
        &sql,
        &control_sql,
        4.0,
        3.0,
        "stage-3 body search vs a use_skip_indexes = 0 control",
    )
    .await;

    // read_bytes bounded relative to the corpus's own mark count (a
    // ratio, never an absolute byte count — edge case #5): a generous
    // 4 KiB/row ceiling per granule, comfortably above this corpus's
    // ~170-byte rows, so the bound only fires on a genuine regression
    // (e.g. reading unrelated granules), not on legitimate corpus
    // growth. `total` rather than `SelectedMarks` is the denominator now
    // that marks no longer track pruning (above); it is the weaker of
    // the two bounds and is kept only as a sanity ceiling — the control
    // identity above is the real gate.
    let granule_byte_ceiling = INDEX_GRANULARITY * 4096;
    let byte_bound = total.max(1) * granule_byte_ceiling;
    assert!(
        evidence.read_bytes <= byte_bound,
        "read_bytes ({}) exceeded {byte_bound} (total_marks={total} x {granule_byte_ceiling} \
         byte/granule ceiling)",
        evidence.read_bytes,
    );
}

// ---------------------------------------------------------------------
// Issue #90 AC5 — the fetch-until-limit paging loop's approximate
// best-effort scan guard (NOT a hard byte ceiling). Each keyset page is
// issued with a decrementing `max_bytes_to_read = scan_budget_bytes −
// (bytes already scanned by prior pages)`; the guard never issues a page
// with a zero cap (ClickHouse's *unlimited* sentinel), so every issued
// page carries a positive, strictly-decreasing cap. This gate proves
// those two properties empirically against `system.query_log`
// (`Settings['max_bytes_to_read']` per page): every page has a cap, all
// caps are positive, they strictly decrease, and each cap equals
// `budget − Σ prior read_bytes` — which also detects accidental one-row-
// per-page duplication. The single-shard topology (base `log_samples`,
// no `_dist`) makes each keyset page yield exactly one finalized
// query_log row. Actual bytes can exceed the budget (per-block /
// per-reader / per-shard enforcement); the budget bounds runaway paging,
// not exact bytes. Clustered attribution/behaviour is derived-and-untested,
// routed to #25.
// ---------------------------------------------------------------------

/// Creates a fresh, uniquely-named run database with a **strict**
/// `CREATE DATABASE` (no `IF NOT EXISTS`; asserts success), then seeds the
/// corpus into it. Because the name is unique per invocation, the #90
/// gates below can scope their `system.query_log` reads with a plain
/// `current_database = '{db}'` filter (no time marker) and `seed_corpus`
/// can skip the drop-if-exists. Returns `(admin, run_db, ts_ns)`.
async fn fresh_run_db() -> (ChClient, String, i64) {
    let run_db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_{}",
        uuid::Uuid::new_v4().simple()
    ));
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("CREATE DATABASE {run_db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("strict CREATE DATABASE for unique run db");
    let (_client, ts_ns) = seed_corpus(&run_db).await;
    (admin, run_db, ts_ns)
}

fn engine_config(db: &str, scan_budget_bytes: u64) -> EngineConfig {
    EngineConfig {
        // Issue #398: the per-query ClickHouse memory ceiling; the
        // production default, so this fixture keeps today's behaviour.
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        streams_idx: "log_streams_idx".to_string(),
        streams: "log_streams".to_string(),
        samples: "log_samples".to_string(),
        rollup_table: "log_metrics_5s".to_string(),
        patterns_table: "log_patterns".to_string(),
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
        distributed: false,
    }
}

async fn data_client(db: &str) -> ChClient {
    let mut cfg = test_config();
    cfg.database = db.to_string();
    ChClient::new(cfg).await.expect("connect data client")
}

/// [`data_client`] with a deadline the caller sets, for a test whose own
/// corpus needs longer than this file's 60 s: the deadline has to outlast
/// the scan the query makes, and how long that scan takes is a property of
/// the machine, not of the behaviour under test. See
/// `every_refusal_lands_as_the_table_says`, row R2.
async fn data_client_with_deadline(db: &str, deadline: Duration) -> ChClient {
    let mut cfg = test_config();
    cfg.database = db.to_string();
    cfg.query_timeout = deadline;
    ChClient::new(cfg).await.expect("connect data client")
}

/// One finalized `system.query_log` row per keyset PAGE query for this
/// test's run database, in issue order.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct KeysetPageRow {
    /// The per-page `max_bytes_to_read` cap, from `Settings` — 0 if the
    /// setting was absent (see `has_cap`).
    cap: u64,
    /// The page's FINAL scanned `read_bytes` (accurate under
    /// `wait_end_of_query = 1`).
    read: u64,
    /// Whether the page was issued with a `max_bytes_to_read` cap at all
    /// (1 = present). A page issued without a cap would scan unbounded.
    has_cap: u8,
}

/// Returns every FINALIZED `system.query_log` row for this test's keyset
/// PAGE queries — identified by the `AS body_hash` projection unique to
/// `stage3_keyset` — scoped to the unique run database `db` via
/// `current_database` (the run db is created per invocation, so no time
/// marker is needed) and ordered by issue time. `type != 'QueryStart'`
/// keeps exactly one finalized row per page (single-shard topology),
/// INCLUDING the terminal `ExceptionWhileProcessing` row of a page aborted
/// by its `max_bytes_to_read` cap. The row count doubles as the page
/// count (the zero-budget guard test asserts it is 0).
async fn keyset_page_rows(admin: &ChClient, db: &str) -> Vec<KeysetPageRow> {
    admin
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
    let sql = format!(
        "SELECT toUInt64OrZero(Settings['max_bytes_to_read']) AS cap, \
         read_bytes AS read, \
         toUInt8(mapContains(Settings, 'max_bytes_to_read')) AS has_cap \
         FROM system.query_log \
         WHERE current_database = '{db}' AND type != 'QueryStart' \
         AND query LIKE '%AS body_hash%' \
         ORDER BY query_start_time_microseconds ASC, event_time_microseconds ASC"
    );
    let mut stream = admin
        .query_stream::<KeysetPageRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.query_log");
    let mut pages = Vec::new();
    while let Some(row) = stream.next().await {
        pages.push(row.expect("decode keyset page row"));
    }
    pages
}

fn dropping_query() -> String {
    // A label filter over non-JSON bodies: `json` fails and tags
    // `__error__`, then `status = "500"` drops every line (no `status`
    // label) in-engine — `fetch_until_limit` engages, survivors stay 0, so
    // the loop pages until the byte budget stops it (also proving it
    // advances past entirely-dropped pages instead of stalling).
    format!(r#"{{service_name="{SERVICE}"}} | json | status = "500""#)
}

fn full_window_params(ts_ns: i64, limit: u32) -> QueryParams {
    QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts_ns - 3_600_000_000_000,
            end_ns: ts_ns + 3_600_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit,
        direction: Direction::Backward,
    }
}

#[tokio::test]
async fn fetch_until_limit_pages_issue_strictly_decrementing_positive_scan_caps() {
    skip_unless_live!();
    let (admin, run_db, ts_ns) = fresh_run_db().await;

    // Sized to this ~19 MiB single-stream corpus so the FIRST keyset page
    // (whole-window scan — its lower bound is the full window; the 4-column
    // keyset ORDER BY's body_hash/body tiebreakers, load-bearing for #74's
    // tie-correct OFFSET, defeat `optimize_read_in_order` so the LIMIT does
    // not short-circuit) fits, but the loop must abort on a LATER page.
    let budget: u64 = 24 * 1024 * 1024;
    let engine = LogQlEngine::new(
        data_client(&run_db).await,
        engine_config(&run_db, budget),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );

    // The `read_bytes`-accuracy mechanism the per-page cap accounting below
    // relies on: every keyset PAGE must run with `wait_end_of_query = 1`,
    // which is what makes the CLIENT-side per-page `read_bytes` (used to
    // decrement the remaining cap) the FINAL scanned total rather than the
    // clickhouse crate's understated initial-header value (plan v2,
    // issuecomment-5005919929). This is asserted on the engine's settings
    // object, NOT on `system.query_log`: `wait_end_of_query` is an
    // HTTP-interface-only parameter — it never appears in `system.settings`
    // nor in `query_log.Settings`, and the SERVER-side `read_bytes` is
    // byte-identical with or without it — so the wiring is observable only
    // here. Remove `.set("wait_end_of_query", 1)` from
    // `LogQlEngine::paging_settings` and this assertion trips.
    assert_eq!(
        engine.paging_settings(budget).get("wait_end_of_query"),
        Some("1"),
        "fetch-until-limit paging queries must set wait_end_of_query=1 so per-page \
         read_bytes is the final scanned total, keeping the AC5 cap accounting sound \
         (issue #90)"
    );

    // scan_limit = 5000 × 10 = 50_000: page 1 fetches the newest 50k rows,
    // page 2's cap (budget − page-1 read_bytes) is smaller than page 2's
    // ~11 MiB scan ⇒ page 2 aborts mid-paging.
    let params = full_window_params(ts_ns, 5_000);
    let expr = parse(&dropping_query()).expect("parse");

    let (result, warnings) = engine
        .query(&expr, &params)
        .await
        .unwrap_or_else(|e| panic!("query err: {e:?}"));
    assert!(warnings.is_empty(), "a log-stream query emits no warnings");
    let QueryResult::Streams { items, partial } = result else {
        panic!("a stream selector must return Streams");
    };
    assert!(
        items.iter().all(|s| s.entries.is_empty()),
        "the dropping pipeline must drop every line"
    );
    assert!(
        partial,
        "budget exhaustion mid-paging MUST signal a partial result (stats.pulsus_partial)"
    );

    // Single-shard topology (base `log_samples`, no `_dist`): exactly one
    // finalized query_log row per keyset page, in issue order.
    let pages = keyset_page_rows(&admin, &run_db).await;
    assert!(
        pages.len() > 1,
        "the fetch-until-limit loop must actually PAGE (got {} page(s))",
        pages.len()
    );
    // No page is ever issued with the unlimited (zero) cap: every page
    // carries a `max_bytes_to_read` setting, and every cap is positive.
    // Remove the top-of-loop `spent >= budget` guard and a zero-cap
    // (unlimited) page can be issued — this trips.
    assert!(
        pages.iter().all(|p| p.has_cap == 1),
        "every keyset page must be issued with a max_bytes_to_read cap"
    );
    assert!(
        pages.iter().all(|p| p.cap > 0),
        "no page may be issued with max_bytes_to_read=0 (ClickHouse's unlimited sentinel)"
    );
    // Strictly-decreasing caps: `cap_{i+1} == cap_i − read_i`, and every
    // page that scanned rows has `read_i > 0`, so caps strictly shrink. A
    // duplicated coordinator/remote row for the same page would repeat a cap
    // and break this — so the property also guards one-row-per-page.
    for w in pages.windows(2) {
        assert!(
            w[1].cap < w[0].cap,
            "per-page caps must strictly decrease (got {} then {})",
            w[0].cap,
            w[1].cap
        );
    }
    // Decrementing-cap identity: `cap_i == budget − Σ_{j<i} read_j`. Holds
    // for every page including the terminal aborted one (whose own
    // read_bytes is never folded into a later cap). Also detects accidental
    // page duplication (a repeated cap breaks the running sum).
    let mut running: u64 = 0;
    for (i, p) in pages.iter().enumerate() {
        assert_eq!(
            p.cap,
            budget - running,
            "page {i} cap ({}) must equal budget − Σ prior read_bytes ({})",
            p.cap,
            budget - running
        );
        running += p.read;
    }

    // Review round 5: this test returned without dropping its run
    // database, so a PASSING run left one behind.
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {run_db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

#[tokio::test]
async fn fetch_until_limit_zero_budget_terminates_partial_without_unlimited_page() {
    skip_unless_live!();
    // Direct `EngineConfig` with `scan_budget_bytes = 0` (production config
    // rejects 0 via `positive_bytes`; this drives the loop's top-of-loop
    // `spent >= budget` guard deterministically — a mid-paging exact hit is
    // data-dependent and not reproducible).
    let (admin, run_db, ts_ns) = fresh_run_db().await;
    let engine = LogQlEngine::new(
        data_client(&run_db).await,
        engine_config(&run_db, 0),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let params = full_window_params(ts_ns, 5_000);
    let expr = parse(&dropping_query()).expect("parse");

    let (result, warnings) = engine
        .query(&expr, &params)
        .await
        .unwrap_or_else(|e| panic!("query err: {e:?}"));
    assert!(warnings.is_empty(), "a log-stream query emits no warnings");
    let QueryResult::Streams { items, partial } = result else {
        panic!("a stream selector must return Streams");
    };
    assert!(partial, "a spent budget must terminate with partial");
    assert!(
        items.iter().all(|s| s.entries.is_empty()),
        "no survivors when the guard returns before any page"
    );

    // Prove NO keyset page was issued: the guard must return before issuance
    // (a zero cap = ClickHouse's *unlimited* sentinel must never be issued).
    let pages = keyset_page_rows(&admin, &run_db).await;
    assert_eq!(
        pages.len(),
        0,
        "the zero-budget guard must return before issuing any keyset page (got {} page(s))",
        pages.len()
    );

    // Review round 5: this test returned without dropping its run
    // database, so a PASSING run left one behind.
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {run_db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

// ---------------------------------------------------------------------
// Issue #170 — detected_fields post-pipeline paged sampling (plan v2's
// review fix): a sparse parser/label filter whose ONLY matching rows sit
// after the first `line_limit` raw rows of the Backward walk must still
// have its fields detected (window-exhausted branch), a mid-paging budget
// hit returns the fields so far as `truncated = true`, and a first-page
// budget overflow stays `QueryTooBroad` — the three #90 terminal
// branches, on this endpoint.
// ---------------------------------------------------------------------

/// 30,000 rows, one stream; ONLY the OLDEST [`DETECTED_MATCH_COUNT`] rows
/// are JSON matching `| json | level="rare"` — with `line_limit = 100`
/// and the default scan factor 10 the paged walk (page size 1,000,
/// newest-first) reaches them only on the FINAL page, long after the
/// first `line_limit` raw rows.
const DETECTED_CORPUS_ROWS: u64 = 30_000;
const DETECTED_MATCH_COUNT: u64 = 5;

/// Drops + re-creates `db`, seeds the detected-fields corpus, and returns
/// `(client, ts_ns)` (`ts_ns` = corpus start, 1h ago — the
/// [`seed_corpus`] convention).
async fn setup_detected_corpus(db: &str) -> (ChClient, i64) {
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
    run_init(&admin, &test_ctx(db)).await.expect("run_init");

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");

    let ts_ns = now_ns() - 3_600_000_000_000;
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) \
                 VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({ts_ns}))), {FP_CORPUS}, \
                 '{SERVICE}', '{{\"service_name\":\"{SERVICE}\"}}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");
    land_seeded_streams(&client, db).await;
    let mut rows = Vec::with_capacity(DETECTED_CORPUS_ROWS as usize);
    for i in 0..DETECTED_CORPUS_ROWS {
        let timestamp_ns = ts_ns + (i as i64) * 36_000_000;
        let body = if i < DETECTED_MATCH_COUNT {
            // The oldest rows are the ONLY `| json | level="rare"` matches.
            format!(r#"{{"level":"rare","code":7,"seq":"{i}"}}"#)
        } else {
            // Non-JSON: the json stage tags `__error__` (line kept), then
            // `level="rare"` drops it in-engine — the dropping pipeline.
            format!(
                "row {i} routine request completed padding_{}",
                "x".repeat(120)
            )
        };
        rows.push(SeedSampleRow {
            org_id: String::new(),
            service: SERVICE.to_string(),
            fingerprint: u128::from(FP_CORPUS),
            timestamp_ns,
            severity: 0,
            body,
        });
    }
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("bulk insert detected corpus");
    land_seeded_lines(&client, db).await;
    (client, ts_ns)
}

fn detected_bounds(ts_ns: i64) -> pulsus_read::TimeBounds {
    pulsus_read::TimeBounds {
        start_ns: ts_ns - 3_600_000_000_000,
        end_ns: ts_ns + 3_600_000_000_000,
    }
}

/// Branch 2 (the review-fix branch): matches occurring only AFTER the
/// first `line_limit` raw rows of the walk ARE found — the loop pages to
/// window exhaustion and returns complete (`truncated = false`).
#[tokio::test]
async fn detected_fields_sparse_filter_finds_late_matches_window_exhausted() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_qlg_detected_late");
    let (client, ts_ns) = setup_detected_corpus(db).await;
    drop(client);

    let engine = LogQlEngine::new(
        data_client(db).await,
        engine_config(db, 50 * 1024 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let expr = parse(&format!(
        r#"{{service_name="{SERVICE}"}} | json | level="rare""#
    ))
    .expect("parse");

    let out = engine
        .detected_fields(&expr, detected_bounds(ts_ns), 100, 1000)
        .await
        .unwrap_or_else(|e| panic!("detected_fields err: {e:?}"));
    assert!(
        !out.truncated,
        "window exhaustion is a COMPLETE result, never partial"
    );
    let level = out
        .fields
        .iter()
        .find(|f| f.label == "level")
        .expect("late-occurring `level` field must be detected (the pre-pipeline LIMIT bug)");
    assert_eq!(level.field_type, "string");
    assert_eq!(level.cardinality, 1);
    assert_eq!(level.parsers, vec!["json"]);
    let code = out
        .fields
        .iter()
        .find(|f| f.label == "code")
        .expect("code field");
    assert_eq!(code.field_type, "int");
    let seq = out.fields.iter().find(|f| f.label == "seq").expect("seq");
    assert_eq!(
        seq.cardinality, DETECTED_MATCH_COUNT,
        "every late-occurring match must be sampled, not just the first page"
    );
}

/// Branch 4: a budget spent after >= 1 page returns the fields
/// accumulated so far with `truncated = true` (surfaced as
/// `pulsus_partial`), never an error and never a silently-complete shape.
#[tokio::test]
async fn detected_fields_budget_exhaustion_mid_paging_returns_truncated() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_qlg_detected_budget");
    let (client, ts_ns) = setup_detected_corpus(db).await;
    drop(client);

    // Sized to this ~5 MiB corpus: the FIRST keyset page (whole-window
    // scan — the keyset ORDER BY defeats optimize_read_in_order, so the
    // LIMIT does not short-circuit) fits, but page 2's remaining cap is
    // smaller than its ~whole-remaining-window scan ⇒ mid-paging abort.
    let engine = LogQlEngine::new(
        data_client(db).await,
        engine_config(db, 8 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let expr = parse(&format!(
        r#"{{service_name="{SERVICE}"}} | json | level="rare""#
    ))
    .expect("parse");

    let out = engine
        .detected_fields(&expr, detected_bounds(ts_ns), 100, 1000)
        .await
        .unwrap_or_else(|e| panic!("detected_fields err: {e:?}"));
    assert!(
        out.truncated,
        "budget exhaustion mid-paging MUST signal a truncated result"
    );
    assert!(
        !out.fields.iter().any(|f| f.label == "level"),
        "the matches sit at the window's oldest edge — a budget-truncated walk \
         cannot have reached them (fields so far only)"
    );
}

/// Branch 3: the FIRST page alone overflowing the budget stays a
/// `QueryTooBroad` error — exactly as every other read path.
#[tokio::test]
async fn detected_fields_first_page_over_budget_stays_query_too_broad() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_qlg_detected_tight");
    let (client, ts_ns) = setup_detected_corpus(db).await;
    drop(client);

    let engine = LogQlEngine::new(
        data_client(db).await,
        engine_config(db, 64 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let expr = parse(&format!(
        r#"{{service_name="{SERVICE}"}} | json | level="rare""#
    ))
    .expect("parse");

    let err = engine
        .detected_fields(&expr, detected_bounds(ts_ns), 100, 1000)
        .await
        .expect_err("a first-page-over-budget sample must error, not partial-return");
    assert!(
        matches!(err, ReadError::QueryTooBroad(_)),
        "first-page budget overflow must be QueryTooBroad, got {err:?}"
    );
}

#[tokio::test]
async fn fetch_until_limit_first_page_over_budget_stays_query_too_broad() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_qlg_budget_tight");
    let (_admin, ts_ns) = setup_corpus(db).await;

    // Well below the first page's whole-window scan (~19 MiB): the FIRST
    // page overflows the FULL budget ⇒ a genuinely too-broad query ⇒
    // QueryTooBroad (preserved from the pre-#90 single-scan path), never a
    // silent/partial result.
    let engine = LogQlEngine::new(
        data_client(db).await,
        engine_config(db, 64 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let params = full_window_params(ts_ns, 5_000);
    let expr = parse(&dropping_query()).expect("parse");

    let err = engine
        .query(&expr, &params)
        .await
        .expect_err("a first-page-over-budget query must error, not partial-return");
    assert!(
        matches!(err, ReadError::QueryTooBroad(_)),
        "first-page budget overflow must be QueryTooBroad, got {err:?}"
    );
}

// ---------------------------------------------------------------------
// Issue #261 — the property that makes exactness affordable on
// `/detected_labels`: the aggregation's coordinator fan-in is ONE row per
// distinct KEY and is independent of how many distinct VALUES those keys
// have. It is what separates our aggregate from the only
// reference-faithful alternative (ship every distinct `(key, val)` out of
// ClickHouse to hash coordinator-side), whose fan-in is one row per
// value.
// ---------------------------------------------------------------------

/// Tier-1, scale-invariant (docs/schemas.md §9): runs the PRODUCTION
/// `sql::detected_labels` text over the same three keys at two very
/// different value cardinalities and asserts, from `system.query_log`,
/// that `result_rows` equals the number of distinct KEYS at each — and
/// is therefore the same number at both. Ratios and identities only;
/// never a wall-time or byte threshold, so the gate survives any corpus
/// resize. The absolute duration and memory of this aggregate ARE
/// scale-dependent (measured under #261: ~0.9 GiB at 10 M distinct
/// values on a single node) and belong to #25, not here.
///
/// **Which assertion carries the weight.** The per-case
/// `result_rows == DISTINCT_KEYS` is the one that fails: replacing the
/// aggregate with the reference-faithful `GROUP BY key, val` shape (the
/// only route that could reproduce the reference's own sketch) makes it
/// fire at both cardinalities, and so does an accidental change to the
/// seeded key set. The cross-cardinality equality below is then a
/// restatement rather than an independent check — it is kept because it
/// is the sentence a reader wants ("the fan-in does not grow with value
/// cardinality"), not because it catches a case the per-case identity
/// misses.
#[tokio::test]
async fn detected_labels_fan_in_is_one_row_per_key_at_any_cardinality() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_qlg_detected_labels_fanin");
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
    run_init(&admin, &test_ctx(db)).await.expect("run_init");
    let client = data_client(db).await;

    // The two months are separate partitions, so the same three keys can
    // carry 100 and 7,708 distinct `pod` values without either scan
    // seeing the other's rows. 7,708 is the `pod-{i}` family's measured
    // reference-divergence point (issue #261), i.e. a cardinality the
    // reference could not report exactly.
    const LOW: u64 = 100;
    const HIGH: u64 = 7_708;
    // Issue #399: the aggregation is now bounded by the requested window
    // as well as the month, via a semi-join over the log rollup. This
    // fixture seeds `log_streams_idx` directly (never `log_samples`, so
    // the rollup MV never fires), so it must seed the activity rows too —
    // otherwise every case returns zero rows and the fan-in identity
    // below becomes vacuously true. The window per case is the month
    // itself, keeping the #261 property exactly as it was measured.
    // Issue #286: `sql::detected_labels` takes `MonthLiteral`s, whose only
    // mint takes integers — so the `'YYYY-MM-01'` case labels are split back
    // into `(year, month)` here rather than passed as text.
    let month_year = |month: &str| -> i64 { month.trim_matches('\'')[0..4].parse().expect("year") };
    let month_month =
        |month: &str| -> u32 { month.trim_matches('\'')[5..7].parse().expect("month") };
    let month_window = |month: &str| -> (i64, i64) {
        // `'YYYY-MM-01'` (quoted) → the month's [start, start + 28d] in ns.
        let date = month.trim_matches('\'');
        let y: i64 = date[0..4].parse().expect("year");
        let m: i64 = date[5..7].parse().expect("month");
        // Days from the Unix epoch to `y-m-01`, civil-calendar algorithm
        // (Howard Hinnant's `days_from_civil`) — no chrono dependency in
        // this suite.
        let yy = if m <= 2 { y - 1 } else { y };
        let era = yy.div_euclid(400);
        let yoe = yy - era * 400;
        let mp = (m + 9) % 12;
        let doy = (153 * mp + 2) / 5;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        let days = era * 146_097 + doe - 719_468;
        let start = days * 86_400 * 1_000_000_000;
        (start, start + 28 * 86_400 * 1_000_000_000)
    };
    let cases = [("'2026-06-01'", LOW), ("'2026-07-01'", HIGH)];
    for (month, n) in cases {
        let (start_ns, _) = month_window(month);
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_metrics_5s (fingerprint, bucket_ns, count, bytes) \
                     SELECT number, {start_ns} + 5000000000, 1, 10 FROM numbers({n})"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed rollup activity");
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_streams_idx (month, key, val, fingerprint) \
                     SELECT toDate({month}), 'pod', concat('pod-', toString(number)), number \
                     FROM numbers({n})"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed pod values");
        // Two more keys, at a cardinality that does NOT scale with `n` —
        // the key set is identical in both cases, which is what makes the
        // two `result_rows` comparable.
        for (key, distinct) in [("namespace", 3u64), ("service_name", 5)] {
            client
                .execute(
                    &format!(
                        "INSERT INTO {db}.log_streams_idx (month, key, val, fingerprint) \
                         SELECT toDate({month}), '{key}', \
                         concat('{key}-', toString(number % {distinct})), number \
                         FROM numbers({n})"
                    ),
                    &QuerySettings::new(),
                    Idempotency::Idempotent,
                )
                .await
                .expect("seed low-cardinality keys");
        }
    }
    const DISTINCT_KEYS: u64 = 3;

    /// The active-fingerprint set a discovery read resolves from its own
    /// statement (issue #603) before it renders the index scan.
    async fn active_set(
        client: &ChClient,
        rollup: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Vec<pulsus_model::FpLiteral> {
        #[derive(Debug, pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize)]
        struct Fp {
            fingerprint: pulsus_model::Fingerprint,
        }
        let sql = sql::active_fingerprints(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            rollup,
            None,
            sql::TimeWindow { start_ns, end_ns },
            5_000_000_000,
        );
        let mut stream = client
            .query_stream::<Fp>(&sql, &QuerySettings::new())
            .await
            .expect("the activity statement runs");
        let mut out = Vec::new();
        while let Some(row) = stream.next().await {
            out.push(row.expect("a fingerprint row").fingerprint.sql_literal());
        }
        assert!(!out.is_empty(), "the fixture must have an active stream");
        out
    }

    let mut fan_in = Vec::new();
    for (i, (month, n)) in cases.iter().enumerate() {
        let (start_ns, end_ns) = month_window(month);
        // Issue #603: the activity set is resolved by a statement of its own
        // and rendered into this one as a literal list, which is what
        // production issues.
        let active = active_set(&client, &format!("{db}.log_metrics_5s"), start_ns, end_ns).await;
        let sql = sql::detected_labels(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            &format!("{db}.log_streams_idx"),
            &[month_literal(month_year(month), month_month(month))],
            &active,
        );
        let query_id = format!("qlg-detected-labels-fanin-{i}");
        // The UUID predicate carries literal `?`s, which the clickhouse
        // crate would read as bind placeholders — production doubles them
        // at the execution boundary (`exec::escape_query_placeholders`),
        // and a test issuing the raw text must do the same.
        let (returned, evidence) =
            run_detected_labels(&client, &sql.replace('?', "??"), &query_id).await;
        assert_eq!(
            returned, DISTINCT_KEYS,
            "the aggregate must stream one row per distinct key at n = {n}"
        );
        assert_eq!(
            evidence.result_rows, returned,
            "the server's own view of the result size must match what the client \
             decoded at n = {n}"
        );
        assert_eq!(
            evidence.result_rows, DISTINCT_KEYS,
            "coordinator fan-in at n = {n} must be one row per distinct KEY, not \
             one per distinct value (issue #261)"
        );
        assert!(
            evidence.read_rows >= *n,
            "sanity: the scan at n = {n} must actually have read the seeded rows \
             (read {})",
            evidence.read_rows
        );
        fan_in.push(evidence.result_rows);
    }

    assert_eq!(
        fan_in[0], fan_in[1],
        "the coordinator fan-in must NOT depend on value cardinality: {} rows at \
         {LOW} distinct pod values vs {} rows at {HIGH} — the endpoint's whole \
         design point (docs/api.md §2.6.2) is one row per distinct key, never one \
         per value (issue #261)",
        fan_in[0], fan_in[1]
    );
    assert_eq!(
        fan_in[1], DISTINCT_KEYS,
        "and that constant is the number of distinct KEYS, not a coincidence"
    );
}

/// The `/detected_labels` aggregate's three-column output shape.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct DetectedLabelsRow {
    key: String,
    cardinality: u64,
    non_id_values: u64,
}

/// `result_rows` — the coordinator fan-in — is not a column of the
/// suite-wide [`QueryLogRow`], and this scenario is the only one that
/// needs it; it gets its own row shape rather than widening the shared
/// one, so no other gate's evidence query changes.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct FanInRow {
    result_rows: u64,
    read_rows: u64,
}

/// Runs `sql` under `query_id`, drains every returned row (the
/// `QueryFinish` row only lands once the query has fully completed),
/// flushes logs and reads back `(rows returned, evidence)`. The stream is
/// scoped to this function so its pooled connection lease is released
/// before the `system.query_log` read.
async fn run_detected_labels(client: &ChClient, sql: &str, query_id: &str) -> (u64, FanInRow) {
    let settings = QuerySettings::new().set("query_id", query_id);
    let mut returned = 0u64;
    {
        let mut stream = client
            .query_stream::<DetectedLabelsRow>(sql, &settings)
            .await
            .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
        while let Some(row) = stream.next().await {
            row.expect("decode detected_labels row");
            returned += 1;
        }
    }

    client
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
    let log_sql = format!(
        "SELECT result_rows, read_rows FROM system.query_log \
         WHERE query_id = '{query_id}' AND type = 'QueryFinish' \
         ORDER BY event_time_microseconds DESC LIMIT 1"
    );
    let mut log_stream = client
        .query_stream::<FanInRow>(&log_sql, &QuerySettings::new())
        .await
        .expect("query system.query_log");
    let evidence = log_stream
        .next()
        .await
        .unwrap_or_else(|| panic!("no query_log row for query_id {query_id}"))
        .expect("decode query_log row");
    (returned, evidence)
}

// ---------------------------------------------------------------------
// Issue #398 — the memory-ceiling COMPLETENESS gates, one per engine.
//
// These are `system.query_log` sweeps rather than settings unit tests, and
// the difference is load-bearing. A settings test can only check the
// origins someone remembered to list; a sweep over every finalized `Select`
// this run's database saw catches a dispatch site no enumeration mentions.
//
// The traces sweep exists for a specific wrong fix. TraceQL has THREE
// settings roots, not one: `search_settings` (the obvious one),
// `catalog_settings` (deliberately independent — it omits the clustered-
// reader block on purpose, and it is the root whose unbounded
// `/api/search/tag/{tag}/values` read produced the measured 500 that opened
// #398), and `TraceEngine::fetch_by_id`, the §4.2 point read, which sent a
// bare `QuerySettings::new()` AND mapped both of its `ChError` seams with
// `ReadError::Clickhouse` directly, bypassing `map_trace_read_error`
// entirely. "Add the ceiling to `search_settings`" passes a settings unit
// test on that root while leaving the other two bare. It cannot pass the
// sweep.
// ---------------------------------------------------------------------

/// The per-query memory ceiling both #398 gates configure. A distinctive
/// value, never a default, so `Settings['max_memory_usage']` in
/// `system.query_log` identifies THIS configuration rather than merely
/// being non-empty.
const MEM_CEILING: u64 = 3_221_225_472; // 3 GiB

/// One finalized `system.query_log` row for a #398 sweep.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct MemCeilingRow {
    /// 1 when the query was issued with a `max_memory_usage` setting.
    has_ceiling: u8,
    /// The setting's value, `0` when absent.
    ceiling: u64,
    /// First 120 chars of the SQL — for the failure message only.
    q: String,
}

/// A ClickHouse-side timestamp marker, taken AFTER schema init and corpus
/// seeding and BEFORE the engine is driven. The sweeps scope on it so
/// `run_init`'s own bookkeeping `SELECT`s — which run in the run database
/// and legitimately carry no reader settings — are outside the claim,
/// while every engine dispatch is inside it. Scoping by time rather than by
/// SQL text is what keeps the sweep a COMPLETENESS check: a whitelist of
/// table names could not see a dispatch against a table nobody listed.
async fn query_log_marker(admin: &ChClient) -> String {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct NowRow {
        t: String,
    }
    let mut stream = admin
        .query_stream::<NowRow>("SELECT toString(now64(6)) AS t", &QuerySettings::new())
        .await
        .expect("read server clock");
    let mut out = String::new();
    while let Some(row) = stream.next().await {
        out = row.expect("decode now row").t;
    }
    assert!(!out.is_empty(), "server clock marker must be non-empty");
    out
}

/// Every finalized `Select` issued against `db` at or after `marker`.
/// `type != 'QueryStart'` keeps one row per query INCLUDING the terminal
/// `ExceptionWhileProcessing` row of a query aborted by its own budget.
async fn mem_ceiling_rows(admin: &ChClient, db: &str, marker: &str) -> Vec<MemCeilingRow> {
    admin
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
    let sql = format!(
        "SELECT toUInt8(mapContains(Settings, 'max_memory_usage')) AS has_ceiling, \
         toUInt64OrZero(Settings['max_memory_usage']) AS ceiling, \
         substring(query, 1, 120) AS q \
         FROM system.query_log \
         WHERE current_database = '{db}' AND type != 'QueryStart' \
           AND query_kind = 'Select' \
           AND query_start_time_microseconds >= toDateTime64('{marker}', 6) \
         ORDER BY query_start_time_microseconds ASC"
    );
    let mut stream = admin
        .query_stream::<MemCeilingRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.query_log");
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.expect("decode mem ceiling row"));
    }
    rows
}

/// How long a count of `system.query_log` rows is re-read before the caller
/// asserts on whatever it holds.
const QUERY_LOG_DEADLINE: Duration = Duration::from_secs(10);

/// How long a count must hold still before it is taken as settled.
const QUERY_LOG_SETTLE: Duration = Duration::from_secs(1);

/// [`mem_ceiling_rows`], re-read until it holds at least `expected` rows and
/// has then held still for [`QUERY_LOG_SETTLE`], or until
/// [`QUERY_LOG_DEADLINE`] passes. The caller asserts the exact count.
///
/// `SYSTEM FLUSH LOGS` writes only what is already queued, and the
/// `QueryFinish` row of a statement that has just returned can be queued
/// after it: such a row was seen arriving after the flush that should have
/// held it. One flush and one read therefore miss it now and then. The wait
/// past `expected` is what keeps an exact count able to fail: a statement
/// the engine should not have issued comes after the expected ones, so its
/// row is as late as theirs or later.
async fn mem_ceiling_rows_expecting(
    admin: &ChClient,
    db: &str,
    marker: &str,
    expected: usize,
) -> Vec<MemCeilingRow> {
    let deadline = std::time::Instant::now() + QUERY_LOG_DEADLINE;
    let mut rows = mem_ceiling_rows(admin, db, marker).await;
    let mut held_since = std::time::Instant::now();
    loop {
        let now = std::time::Instant::now();
        if now >= deadline || (rows.len() >= expected && now - held_since >= QUERY_LOG_SETTLE) {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let next = mem_ceiling_rows(admin, db, marker).await;
        if next.len() != rows.len() {
            held_since = std::time::Instant::now();
        }
        rows = next;
    }
}

/// Asserts the sweep is non-empty and that EVERY row carries the ceiling at
/// the configured value.
fn assert_every_row_carries_the_ceiling(rows: &[MemCeilingRow], what: &str) {
    assert!(
        !rows.is_empty(),
        "{what}: the sweep saw no queries at all — it would pass vacuously"
    );
    let bare: Vec<String> = rows
        .iter()
        .filter(|r| r.has_ceiling != 1 || r.ceiling != MEM_CEILING)
        .map(|r| {
            format!(
                "has_ceiling={} ceiling={} :: {}",
                r.has_ceiling, r.ceiling, r.q
            )
        })
        .collect();
    assert!(
        bare.is_empty(),
        "{what}: {} of {} dispatched queries did not carry \
         max_memory_usage = {MEM_CEILING}:\n{}",
        bare.len(),
        rows.len(),
        bare.join("\n")
    );
}

/// Issue #398 AC L8: EVERY query the LogQL engine dispatches carries the
/// per-query memory ceiling. Drives stage-1 resolution, stage-2 hydration,
/// a stage-3 entry read, a client-aggregated metric read, the paged
/// fetch-until-limit loop, and both discovery reads in one run database,
/// then sweeps `system.query_log`.
#[tokio::test]
async fn every_logql_engine_query_carries_the_memory_ceiling() {
    skip_unless_live!();
    let (admin, run_db, ts_ns) = fresh_run_db().await;

    let mut config = engine_config(&run_db, 50 * 1024 * 1024 * 1024);
    config.read_max_memory_bytes = MEM_CEILING;
    // The client is built BEFORE the marker on purpose: `ChClient::new`
    // opens the pool with a `SELECT 1` connectivity probe, which is not an
    // engine dispatch and carries no reader settings. Putting it outside
    // the swept window is a boundary on WHEN, not a whitelist of WHAT — no
    // engine query can hide behind it.
    let client = data_client(&run_db).await;
    let marker = query_log_marker(&admin).await;
    let engine = LogQlEngine::new(
        client,
        config,
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );

    let bounds = pulsus_read::logql::TimeBounds {
        start_ns: ts_ns - 3_600_000_000_000,
        end_ns: ts_ns + 3_600_000_000_000,
    };
    let params = full_window_params(ts_ns, 50);

    // Stage 1 + 2 + 3 (entries).
    let selector = format!(r#"{{service_name="{SERVICE}"}}"#);
    engine
        .query(&parse(&selector).expect("parse"), &params)
        .await
        .expect("entry query");
    // The paged fetch-until-limit loop (a distinct dispatch site).
    engine
        .query(&parse(&dropping_query()).expect("parse"), &params)
        .await
        .expect("paged query");
    // A client-aggregated metric read.
    engine
        .query(
            &parse(&format!("count_over_time({selector}[5m])")).expect("parse"),
            &params,
        )
        .await
        .expect("metric query");
    // Discovery reads — unscoped and, since issue #482, `query=`-scoped
    // (a distinct dispatch site: the scoped form issues stage 1 first).
    engine
        .label_names(None, bounds)
        .await
        .expect("label_names unscoped");
    engine
        .label_names(Some(&parse(&selector).expect("parse")), bounds)
        .await
        .expect("label_names scoped");
    engine
        .label_values("service_name", None, bounds)
        .await
        .expect("label_values unscoped");
    engine
        .label_values(
            "service_name",
            Some(&parse(&selector).expect("parse")),
            bounds,
        )
        .await
        .expect("label_values scoped");
    engine
        .series(&[parse(&selector).expect("parse")], bounds)
        .await
        .expect("series");
    engine
        .stats(&parse(&selector).expect("parse"), bounds)
        .await
        .expect("stats");
    engine
        .detected_labels(Some(&parse(&selector).expect("parse")), bounds)
        .await
        .expect("detected_labels");
    engine
        .detected_fields(&parse(&selector).expect("parse"), bounds, 100, 100)
        .await
        .expect("detected_fields");
    engine
        .detected_field_values(
            "service_name",
            &parse(&selector).expect("parse"),
            bounds,
            100,
            100,
        )
        .await
        .expect("detected_field_values");

    let rows = mem_ceiling_rows(&admin, &run_db, &marker).await;
    assert_every_row_carries_the_ceiling(&rows, "LogQL engine");

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {run_db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop run db");
}

/// **Issue #482 AC 0a — a scoped discovery request that matches no
/// stream issues stage one and NOTHING ELSE.**
///
/// The property is a COUNT of statements the server logged, not a
/// presence test: `mem_ceiling_rows` returns every finalized `Select`
/// against the run database since the marker, with no filter on the
/// statement text (`:1458-1486`), so an extra dispatch pushes the count
/// up and fails.
///
/// Phase 2 is the positive control. Without it, phase 1's `rows.len() ==
/// 1` would be equally consistent with a sweep that cannot see stage-2
/// statements at all.
///
/// Both selectors are pure equality, so no selectivity probe is issued
/// (`sql.rs`: probes are computed only for a selector carrying a regex
/// matcher) and the counts are exactly two statements' worth.
///
/// What this cannot see: a stage-2 statement built and then discarded
/// before dispatch — which is not a user-visible defect — and any
/// statement issued against a database other than the run database.
#[tokio::test]
async fn a_no_match_scoped_discovery_request_issues_stage_one_and_no_stage_two() {
    skip_unless_live!();
    let (admin, run_db, ts_ns) = fresh_run_db().await;
    let config = engine_config(&run_db, 50 * 1024 * 1024 * 1024);
    let client = data_client(&run_db).await;
    let engine = LogQlEngine::new(
        client,
        config,
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let bounds = pulsus_read::logql::TimeBounds {
        start_ns: ts_ns - 3_600_000_000_000,
        end_ns: ts_ns + 3_600_000_000_000,
    };

    // Phase 1 — the no-match request.
    let m1 = query_log_marker(&admin).await;
    let out = engine
        .label_values(
            "service_name",
            Some(&parse(r#"{service_name="no-such-service-482"}"#).expect("parse")),
            bounds,
        )
        .await
        .expect("no-match label_values");
    assert_eq!(out, Vec::<String>::new());
    let rows = mem_ceiling_rows_expecting(&admin, &run_db, &m1, 1).await;
    assert_eq!(
        rows.len(),
        1,
        "exactly one statement: stage one and nothing else\n{}",
        rows.iter()
            .map(|r| r.q.clone())
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        rows[0]
            .q
            .starts_with("SELECT fingerprint\nFROM log_streams_idx"),
        "the one statement must be stage one, got {:?}",
        rows[0].q
    );
    assert!(
        !rows
            .iter()
            .any(|r| r.q.starts_with("SELECT DISTINCT val AS value")),
        "no stage-2 label_values statement may be issued"
    );

    // Phase 2 — the positive control.
    let m2 = query_log_marker(&admin).await;
    let out = engine
        .label_values(
            "service_name",
            Some(&parse(&format!(r#"{{service_name="{SERVICE}"}}"#)).expect("parse")),
            bounds,
        )
        .await
        .expect("matching label_values");
    assert_eq!(out, vec![SERVICE.to_string()]);
    let rows = mem_ceiling_rows_expecting(&admin, &run_db, &m2, 3).await;
    // Issue #603: the activity scan sits between them as a statement of its
    // own. It used to be a subquery nested inside stage two, so this run saw
    // two statements; the predicate is unchanged, but its result now arrives
    // as its own dispatch and is rendered into stage two as a literal list.
    assert_eq!(
        rows.len(),
        3,
        "stage one, the activity scan, then stage two\n{}",
        rows.iter()
            .map(|r| r.q.clone())
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        rows[0]
            .q
            .starts_with("SELECT fingerprint\nFROM log_streams_idx"),
        "rows[0] must be stage one, got {:?}",
        rows[0].q
    );
    assert!(
        rows[1].q.starts_with("SELECT DISTINCT fingerprint"),
        "rows[1] must be the activity scan, got {:?}",
        rows[1].q
    );
    assert!(
        rows[2].q.starts_with("SELECT DISTINCT val AS value"),
        "rows[2] must be stage two, got {:?}",
        rows[2].q
    );

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {run_db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop run db");
}

/// Issue #398 AC T3 — **the traces discriminator / completeness gate.**
/// EVERY query the trace engine dispatches carries the per-query memory
/// ceiling.
///
/// The wrong fix this gate is built to fail is "add the ceiling to
/// `search_settings`, the obvious root". TraceQL has three settings roots
/// (see this section's header): `catalog_settings` is independent of
/// `search_settings` and is the one that produced the measured 500, and
/// `TraceEngine::fetch_by_id` sent a bare `QuerySettings::new()` while
/// mapping its errors around `map_trace_read_error` entirely. Under that
/// wrong fix a settings unit test on `search_settings` passes; this sweep
/// does not, because it sees every dispatch the run actually made rather
/// than the ones an enumeration remembered.
#[tokio::test]
async fn every_trace_engine_query_carries_the_memory_ceiling() {
    skip_unless_live!();
    let run_db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_tr_{}",
        uuid::Uuid::new_v4().simple()
    ));
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("CREATE DATABASE {run_db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("strict CREATE DATABASE for unique run db");
    run_init(&admin, &test_ctx(&run_db))
        .await
        .expect("schema init");

    let seed = data_client(&run_db).await;
    let ts_ns = now_ns();
    let date_days = ts_ns / 86_400_000_000_000;
    let trace_hex = "c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1";
    // A handful of spans, their attribute-index registrations, a tag
    // catalog entry and one service-graph edge pair — enough that every
    // read below returns rows rather than short-circuiting on an empty
    // phase-1 result.
    //
    // **Issue #557: the attribute is written into BOTH stores.** The
    // index row still generates the phase-1 candidate; the span row's
    // arrays are what the phase-2 condition now tests, so a span with
    // empty arrays would be hydrated and then dropped, and the search
    // below would return nothing.
    for sql in [
        format!(
            "INSERT INTO {run_db}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload, \
              attr_key, attr_scope, attr_val, attr_type, attr_num) \
             SELECT unhex('{trace_hex}'), \
                    reinterpretAsFixedString(toUInt64(number + 1)), \
                    reinterpretAsFixedString(toUInt64(0)), \
                    'op', 'checkout', {ts_ns} + number, 1000000, 0, 2, 0, '', \
                    ['http.status_code'], ['span'], ['500'], ['int'], \
                    [500.]::Array(Nullable(Float64)) \
             FROM numbers(64)"
        ),
        format!(
            "INSERT INTO {run_db}.trace_attrs_idx \
             (date, key, val, scope, val_type, val_num, timestamp_ns, trace_id, span_id, \
              duration_ns) \
             SELECT toDate({date_days}), 'http.status_code', '500', 'span', 'int', 500., \
                    {ts_ns} + number, unhex('{trace_hex}'), \
                    reinterpretAsFixedString(toUInt64(number + 1)), 1000000 \
             FROM numbers(64)"
        ),
        format!(
            "INSERT INTO {run_db}.trace_tag_catalog (scope, key, val) \
             SELECT 'span', 'http.status_code', concat('v', toString(number)) FROM numbers(64)"
        ),
        // Issue #587: the fetch moved to the span, per-trace and resource
        // tables, so this fixture has to seed those too — otherwise the
        // fetch below returns nothing and the assertion goes red for the
        // wrong reason. All three of the per-trace fields the statements
        // read are supplied: the bucket set (which is what makes the key
        // read find the spans) and the two extent bounds (which are what
        // makes the resource-day bound include the resource's day).
        format!(
            "INSERT INTO {run_db}.spans \
             (trace_id, span_id, parent_span_id, start_ns, end_ns, service, resource_id, \
              name, kind, status_code) \
             SELECT unhex('{trace_hex}'), \
                    reinterpretAsFixedString(toUInt64(number + 1)), \
                    toFixedString('', 8), {ts_ns} + number, \
                    toUInt64({ts_ns} + number + 1000000), 'checkout', 1, 'op', 2, 0 \
             FROM numbers(64)"
        ),
        format!(
            "INSERT INTO {run_db}.traces \
             (day, trace_id, start_ns, last_start_ns, buckets) VALUES \
             (toDate(fromUnixTimestamp64Nano({ts_ns}), 'UTC'), unhex('{trace_hex}'), \
              {ts_ns}, {ts_ns} + 63, [intDiv({ts_ns}, 300000000000), \
              intDiv({ts_ns} + 63, 300000000000)])"
        ),
        format!(
            "INSERT INTO {run_db}.resources \
             (day, resource_id, service, attrs, schema_url) VALUES \
             (toDate(fromUnixTimestamp64Nano({ts_ns}), 'UTC'), 1, 'checkout', '{{}}', '')"
        ),
    ] {
        seed.execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .unwrap_or_else(|e| panic!("seed failed: {e}\nSQL:\n{sql}"));
    }
    // Issue #558: the two stores must hold the same elements. Phase 1
    // generates candidates from the index and phase 2 reads the value,
    // its number and its stored kind off the span row, so a fixture whose
    // two stores disagree produces a candidate that matches nothing, or a
    // kind the response renders wrong — and either reads as a defect in
    // the code rather than in the seed.
    pulsus_testkit::assert_stores_agree(&run_db);

    let config = pulsus_read::TraceReadConfig {
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
        edges_table: "trace_edges".to_string(),
        recent_table: "trace_recent".to_string(),
        errors_table: "trace_error_spans".to_string(),
        spans_v2_table: "spans".to_string(),
        traces_table: "traces".to_string(),
        resources_table: "resources".to_string(),
        max_candidates: 100_000,
        scan_budget_rows: 50_000_000,
        event_set_max_values: 1_000_000,
        max_depth: 64,
        max_series: 1_000,
        generator_max_memory_bytes: MEM_CEILING,
        // The surface-wide ceiling under test. Set equal to the generator's
        // so the single sweep predicate below covers both — the generator
        // deliberately overrides `max_memory_usage` with its own (tighter
        // in production) value, and this gate is about PRESENCE at the
        // configured ceiling on every dispatch, not about which of the two
        // won on any one query. Their independence is asserted separately
        // by `trace_catalog_settings_carry_the_memory_ceiling`.
        read_max_memory_bytes: MEM_CEILING,
        distributed: false,
        skip_unavailable_shards: false,
    };
    // Built before the marker: `ChClient::new` opens the pool with a
    // `SELECT 1` connectivity probe, which is not an engine dispatch.
    let engine_client = data_client(&run_db).await;
    let marker = query_log_marker(&admin).await;
    let engine = pulsus_read::TraceEngine::new(engine_client, config);

    // 1. Search — the route's statement, with the search settings; the
    //    `spans` rows above hold no attribute, so the query names the span.
    let query = pulsus_traceql::parse(r#"{ name = "op" }"#).expect("parses");
    let request = pulsus_read::plan_search_request(
        &query,
        &pulsus_read::SearchParams {
            start_ns: ts_ns - 3_600_000_000_000,
            end_ns: ts_ns + 3_600_000_000_000,
            limit: 20,
            spss: 10,
        },
        &pulsus_read::StatementTables {
            spans: "spans",
            resources: "resources",
            traces: "traces",
            indexed: &[],
        },
        64,
    )
    .expect("plans");
    let found = engine
        .search_routed(&request)
        .await
        .expect("search executes");
    assert!(
        !found.traces.is_empty(),
        "the search fixture must return rows, or the phase-2 reads never dispatch"
    );

    // 2. Trace-by-id — the fetch's own settings root (issue #587).
    let fetched = engine
        .fetch_by_id(trace_hex, None)
        .await
        .expect("the fetch executes");
    assert!(
        !fetched.spans.is_empty(),
        "the fetch fixture must return rows, or the fetch's reads never dispatch"
    );

    // 3 + 4. Catalog discovery — `catalog_settings`, the root that
    //        produced the measured 500.
    engine.list_tag_names(None).await.expect("tag names");
    engine
        .list_tag_values(
            "http.status_code",
            Some("span"),
            unnarrowed_values_request(),
        )
        .await
        .expect("tag values");

    // 5. A trace-metrics query — `metrics_settings`.
    let metric_query = pulsus_traceql::parse(r#"{ span.http.status_code = "500" } | rate()"#)
        .expect("metric query parses");
    let metric_plan = pulsus_read::traces::metrics_plan::plan_trace_metrics(
        &metric_query,
        &pulsus_read::traces::metrics_plan::MetricsParams {
            start_ns: (ts_ns / 1_000_000_000 - 300) * 1_000_000_000,
            end_ns: (ts_ns / 1_000_000_000 + 60) * 1_000_000_000,
            step_ms: 60_000,
            exemplars: None,
        },
        &engine.metrics_ctx(),
    )
    .expect("metric query plans");
    engine
        .metrics_range(&metric_plan)
        .await
        .expect("metrics range");

    // 6. The service graph — `graph_settings`.
    engine
        .service_graph(pulsus_read::traces::graph_sql::GraphWindow {
            start_ns: ts_ns - 3_600_000_000_000,
            end_ns: ts_ns + 3_600_000_000_000,
        })
        .await
        .expect("service graph");

    let rows = mem_ceiling_rows(&admin, &run_db, &marker).await;
    assert_every_row_carries_the_ceiling(&rows, "trace engine");

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {run_db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop run db");
}

// ---------------------------------------------------------------------
// Issue #472 — the `__name__` discovery projection's Tier-1 ratio gate.
// ---------------------------------------------------------------------

/// 50 metric names x 200 series each. The two numbers are what make the
/// gate's identities readable: the narrow statement returns
/// [`NAMES_472`] rows where the wide one returns [`SERIES_472`].
const NAMES_472: u64 = 50;
const SERIES_472: u64 = 10_000;

/// The pre-committed thresholds. Named constants rather than literals in
/// the assertions so the numbers the plan fixed **before** any measurement
/// are readable in one place, and so a later relaxation is a visible edit.
const B1_MIN_READ_BYTES_RATIO: f64 = 5.0;
const B3_MAX_NARROW_GROWTH: f64 = 1.10;
const B3_MIN_WIDE_GROWTH: f64 = 3.0;

/// `system.query_log` evidence for the #472 gate.
///
/// `result_rows` is the coordinator fan-in and `read_bytes` is the cost —
/// this scenario needs both, and the suite-wide [`QueryLogRow`] carries no
/// `result_rows`. It gets its own row shape rather than widening the shared
/// one (the [`FanInRow`] precedent), so no other gate's evidence query
/// changes.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct NameProjectionRow {
    read_rows: u64,
    read_bytes: u64,
    result_rows: u64,
    result_bytes: u64,
    /// Read back for the same reason [`QueryLogRow::condition_cache`] is:
    /// the pin below must not be deletable silently.
    condition_cache: String,
}

/// Runs `sql` under a unique `query_id`, drains every row of `R`'s shape
/// (the `QueryFinish` row only lands once the query has fully completed),
/// flushes logs and reads the evidence back. The stream is scoped to this
/// function so its pooled connection lease is released before the
/// `system.query_log` read.
///
/// `use_query_condition_cache = 0` is pinned for [`run_and_capture`]'s
/// reason (issue #376): from 26.3 a repeated identical query can be served
/// partly from that cache, which would let a ratio pass by having CACHED
/// rather than by having read less.
async fn run_name_projection<R: pulsus_clickhouse::ChRow>(
    client: &ChClient,
    sql: &str,
    query_id: &str,
) -> NameProjectionRow {
    let settings = QuerySettings::new()
        .set("query_id", query_id)
        .set("use_query_condition_cache", 0);
    {
        let mut stream = client
            .query_stream::<R>(sql, &settings)
            .await
            .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
        while let Some(row) = stream.next().await {
            row.expect("decode row");
        }
    }
    client
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
    let log_sql = format!(
        "SELECT read_rows, read_bytes, result_rows, result_bytes, \
         Settings['use_query_condition_cache'] AS condition_cache \
         FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish' \
         ORDER BY event_time_microseconds DESC LIMIT 1"
    );
    let mut log_stream = client
        .query_stream::<NameProjectionRow>(&log_sql, &QuerySettings::new())
        .await
        .expect("query system.query_log");
    let evidence = log_stream
        .next()
        .await
        .unwrap_or_else(|| panic!("no query_log row for query_id {query_id}"))
        .expect("decode query_log row");
    assert_eq!(
        evidence.condition_cache, "0",
        "the query-condition-cache pin must survive, or this gate measures a cache hit \
         rather than a read (query_id {query_id})"
    );
    evidence
}

/// Creates `{db}.{table}` with `metric_series`' own DDL and `{db}.{labels}`
/// with `metric_labels`' (issue #623), and fills them with [`SERIES_472`]
/// series over [`NAMES_472`] names in ONE activity bucket, server-side
/// (`INSERT … SELECT FROM numbers`), so no row crosses the wire. `pad_bytes`
/// is the only thing that differs between the two corpora, and it is in the
/// label table alone: `(day, fingerprint, metric_name, hours)` is a pure
/// function of `number` and the bucket, so the two series tables are
/// identical in every column — asserted by the identity hash in the test
/// below, which is what makes the blob-invariance comparison a comparison
/// of blob size and nothing else.
async fn seed_metric_series_472(
    client: &ChClient,
    db: &str,
    table: &str,
    labels: &str,
    bucket: i64,
    pad: u64,
) {
    for ddl in [
        format!(
            "CREATE TABLE {db}.{table} (\
               org_id       LowCardinality(String), \
               day          Date, \
               fingerprint  UInt128  CODEC(ZSTD(1)), \
               metric_name  LowCardinality(String), \
               hours        SimpleAggregateFunction(groupBitOr, UInt32)\
             ) ENGINE = AggregatingMergeTree \
             PARTITION BY day \
             ORDER BY (org_id, fingerprint) \
             SETTINGS allow_dimensions_outside_sorting_key = 1"
        ),
        format!(
            "CREATE TABLE {db}.{labels} (\
               org_id       LowCardinality(String), \
               metric_name  LowCardinality(String), \
               fingerprint  UInt128  CODEC(Delta(8), ZSTD(1)), \
               labels       String  CODEC(ZSTD(5)), \
               first_seen   SimpleAggregateFunction(min, Int64) CODEC(ZSTD(1)), \
               last_seen    SimpleAggregateFunction(max, Int64) CODEC(ZSTD(1))\
             ) ENGINE = AggregatingMergeTree \
             ORDER BY (org_id, metric_name, fingerprint) \
             SETTINGS allow_dimensions_outside_sorting_key = 1"
        ),
        format!(
            "INSERT INTO {db}.{table} (day, fingerprint, metric_name, hours) \
             SELECT toDate(fromUnixTimestamp64Milli(toInt64({bucket})), 'UTC'), \
                    number + 1, \
                    concat('metric_', leftPad(toString(number % {NAMES_472}), 2, '0')), \
                    toUInt32(bitShiftLeft(toUInt32(1), \
                      toHour(fromUnixTimestamp64Milli(toInt64({bucket})), 'UTC'))) \
             FROM numbers({SERIES_472})"
        ),
        format!(
            "INSERT INTO {db}.{labels} (metric_name, fingerprint, labels, first_seen, last_seen) \
             SELECT concat('metric_', leftPad(toString(number % {NAMES_472}), 2, '0')), \
                    number + 1, \
                    concat('{{\"job\":\"api\",\"namespace\":\"ns-', toString(number % 13), \
                           '\",\"pod\":\"pod-', toString(number), \
                           '\",\"pad\":\"', repeat('x', {pad}), '\"}}'), \
                    {bucket}, \
                    {bucket} \
             FROM numbers({SERIES_472})"
        ),
    ] {
        client
            .execute(&ddl, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .unwrap_or_else(|e| panic!("seed the #472 corpus: {e}\n{ddl}"));
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CorpusShapeRow {
    rows: u64,
    names: u64,
    mean_label_bytes: f64,
    identity: u64,
}

async fn corpus_shape(client: &ChClient, db: &str, table: &str, labels: &str) -> CorpusShapeRow {
    let sql = format!(
        "SELECT count() AS rows, uniqExact(metric_name) AS names, \
         ifNull((SELECT avg(length(labels)) FROM {db}.{labels}), 0) AS mean_label_bytes, \
         sum(cityHash64(metric_name, fingerprint, day, hours)) AS identity \
         FROM {db}.{table}"
    );
    let mut stream = client
        .query_stream::<CorpusShapeRow>(&sql, &QuerySettings::new())
        .await
        .expect("corpus shape");
    stream
        .next()
        .await
        .expect("one corpus-shape row")
        .expect("decode corpus-shape row")
}

/// Issue #472 — the pre-committed `read_bytes` ratio, the blob-invariance
/// identity, and the fan-in identity for `/api/v1/label/__name__/values`.
///
/// The endpoint used to render `SELECT fingerprint, metric_name, labels …
/// LIMIT 1 BY metric_name, fingerprint` and keep only the distinct
/// `metric_name` set. It now renders `SELECT DISTINCT metric_name` over the
/// **same** `WHERE`. Both statements come from the real builders, both
/// numbers come from `system.query_log` on this server, and the raw pairs
/// are printed so a CI log records the measurement rather than the verdict.
///
/// **Thresholds, fixed before any measurement was taken** (the plan's
/// pre-committed bar, recorded here beside the raw pairs it was checked
/// against):
///
/// - **B1** `read_bytes(wide) / read_bytes(narrow) >= 5.0` on the
///   unfiltered filter. Measured on this corpus at 26.3.17.110:
///   **19.6x** (1 782 055 / 90 858).
/// - **B3** `read_bytes(narrow, inflated) <= 1.10x` its small-blob self and
///   `read_bytes(wide, inflated) >= 3.0x` its own. Measured: **1.000x**
///   (90 858 -> 90 858, not one byte) and **11.05x** (1 782 055 ->
///   19 691 197), for a 12.3x blob inflation (mean `labels` 158 B ->
///   1948 B).
/// - **B2** `result_rows(narrow) == 50` and `result_rows(wide) == 10 000`.
///
/// **The magnitudes are properties of THIS corpus**, not of a deployment:
/// they scale with series-per-name and with blob size, both chosen here.
/// What is corpus-independent — and what B3 and B2 actually gate — is the
/// *shape*: the narrow form's `read_bytes` does not move with blob size and
/// its returned rows equal the name count. Scale and wall time route to
/// issue #25; nothing here asserts a duration.
///
/// **What this gate does NOT claim.** Blob-invariance holds for the
/// **unfiltered** call only. A `match[]` carrying a label matcher reads
/// `metric_labels` to evaluate the filter (issue #623), so the narrow
/// form's bytes grow with the blob too; that case's win is transport and
/// parse count, not bytes read. Since #623 the wide statement reads each
/// series' own label row, and the blob it pays for is the label table's.
#[tokio::test]
async fn name_values_narrow_projection_reads_far_fewer_bytes_and_is_blob_invariant() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_qlg_name_projection");
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
    admin
        .execute(
            &format!("CREATE DATABASE {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create test database");
    let client = data_client(db).await;

    let bucket_ms: i64 = 3_600_000;
    let bucket = (now_ns() / 1_000_000 / bucket_ms) * bucket_ms;
    seed_metric_series_472(&client, db, "series_small", "labels_small", bucket, 100).await;
    seed_metric_series_472(&client, db, "series_big", "labels_big", bucket, 1_890).await;

    let small = corpus_shape(&client, db, "series_small", "labels_small").await;
    let big = corpus_shape(&client, db, "series_big", "labels_big").await;
    eprintln!(
        "#472 corpus: small {} rows / {} names / {:.1} B labels; big {} rows / {} names / {:.1} B \
         labels; blob inflation {:.2}x",
        small.rows,
        small.names,
        small.mean_label_bytes,
        big.rows,
        big.names,
        big.mean_label_bytes,
        big.mean_label_bytes / small.mean_label_bytes
    );
    assert_eq!((small.rows, small.names), (SERIES_472, NAMES_472));
    assert_eq!((big.rows, big.names), (SERIES_472, NAMES_472));
    assert_eq!(
        small.identity, big.identity,
        "the two tables must be identical in (metric_name, fingerprint, day, hours) — otherwise \
         the blob-invariance comparison is comparing two different corpora, not two blob sizes"
    );
    assert!(
        big.mean_label_bytes >= 10.0 * small.mean_label_bytes,
        "the inflated table's labels must be an order of magnitude larger, or B3's wide side \
         cannot move: {:.1} B vs {:.1} B",
        big.mean_label_bytes,
        small.mean_label_bytes
    );

    // The real builders, over the exact filter the discovery client's first
    // call produces: no `match[]` at all.
    let filter = pulsus_read::metrics::DiscoveryFilter::default();
    let window = pulsus_read::metrics::DataWindow {
        start_ms: bucket,
        end_ms: bucket,
    };
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let mut evidence = Vec::new();
    for (tag, table) in [("small", "series_small"), ("big", "series_big")] {
        let qualified = format!("{db}.{table}");
        let labels = format!("{db}.labels_{tag}");
        let wide_sql = pulsus_read::metrics::sql::discovery_query(
            &no_tenant(),
            &qualified,
            &labels,
            &filter,
            window,
        );
        let narrow_sql = pulsus_read::metrics::sql::discovery_distinct_names_query(
            &no_tenant(),
            &qualified,
            &labels,
            &filter,
            window,
        );
        let wide = run_name_projection::<pulsus_read::metrics::rows::SeriesRow>(
            &client,
            &wide_sql,
            &format!("qlg-472-wide-{tag}-{nonce}"),
        )
        .await;
        let narrow = run_name_projection::<pulsus_read::metrics::rows::MetricNameRow>(
            &client,
            &narrow_sql,
            &format!("qlg-472-narrow-{tag}-{nonce}"),
        )
        .await;
        eprintln!(
            "#472 {tag}: wide read_rows {} read_bytes {} result_rows {} result_bytes {}; \
             narrow read_rows {} read_bytes {} result_rows {} result_bytes {}",
            wide.read_rows,
            wide.read_bytes,
            wide.result_rows,
            wide.result_bytes,
            narrow.read_rows,
            narrow.read_bytes,
            narrow.result_rows,
            narrow.result_bytes,
        );
        evidence.push((tag, wide, narrow));
    }
    let (_, wide_small, narrow_small) = &evidence[0];
    let (_, wide_big, narrow_big) = &evidence[1];

    // B1 — the headline ratio, on the small-blob corpus.
    let b1 = wide_small.read_bytes as f64 / narrow_small.read_bytes.max(1) as f64;
    eprintln!("#472 B1 read_bytes ratio: {b1:.2}x");
    assert!(
        b1 >= B1_MIN_READ_BYTES_RATIO,
        "B1: read_bytes ratio {b1:.2}x (wide {} / narrow {}) is below the pre-committed \
         {B1_MIN_READ_BYTES_RATIO}x — the name projection is no longer keeping the read off the \
         labels blob",
        wide_small.read_bytes,
        narrow_small.read_bytes
    );

    // B2 — the fan-in identity. The wide statement returns one row per
    // series; the narrow one returns one per metric name.
    assert_eq!(
        narrow_small.result_rows, NAMES_472,
        "B2: the narrow statement must return one row per metric name"
    );
    assert_eq!(
        wide_small.result_rows, SERIES_472,
        "B2: the wide statement it replaces returned one row per series — if this ever \
         stopped being true the B1 ratio would be measuring something else"
    );

    // B3 — blob invariance, the discriminating result. Inflating `labels`
    // moves the wide read and must not move the narrow one.
    let narrow_growth = narrow_big.read_bytes as f64 / narrow_small.read_bytes.max(1) as f64;
    let wide_growth = wide_big.read_bytes as f64 / wide_small.read_bytes.max(1) as f64;
    eprintln!(
        "#472 B3 growth under blob inflation: narrow {narrow_growth:.3}x, wide {wide_growth:.2}x"
    );
    assert!(
        narrow_growth <= B3_MAX_NARROW_GROWTH,
        "B3: the narrow read grew {narrow_growth:.3}x ({} -> {}) when the labels blob was \
         inflated — it must not read the column at all on an unfiltered call",
        narrow_small.read_bytes,
        narrow_big.read_bytes
    );
    assert!(
        wide_growth >= B3_MIN_WIDE_GROWTH,
        "B3: the wide read grew only {wide_growth:.2}x ({} -> {}) — if the statement being \
         replaced does not pay for the blob, the invariance above proves nothing",
        wide_small.read_bytes,
        wide_big.read_bytes
    );

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

/// Issue #478 added a request argument to `list_tag_values`. With no `q`
/// the read is byte-identical to the pre-#478 catalog read, so the window
/// is inert here; it is still a real one rather than zeros so nothing
/// depends on an unrepresentable value.
fn unnarrowed_values_request() -> pulsus_read::TagValuesRequest<'static> {
    pulsus_read::TagValuesRequest {
        q: None,
        start_ns: 1_700_000_000_000_000_000,
        end_ns: 1_700_003_600_000_000_000,
    }
}

// ---------------------------------------------------------------------
// W2 (issue #507): the anchored bucket expression, executed.
// ---------------------------------------------------------------------

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct BucketRow {
    label: String,
    t: i64,
    got: i64,
    expected: i64,
}

/// **The grid the reference's window implies, computed by the database
/// from the expression this repository renders** (issue #507, W2).
///
/// The window for grid point `g` is `(g - range, g]` on a grid anchored at
/// the query's start, so a sample belongs to the SMALLEST grid point at or
/// above it — a ceiling. The shipped range renderer instead floors onto a
/// grid anchored at the epoch. The two are not spellings of one function:
/// with a grid start that is not a multiple of the step, the floor form's
/// output is not a grid point at all.
///
/// Six inputs, four of which are the four boundary pairs the design states
/// and two of which extend them past the next grid point. The expression
/// under test comes from `predicate::bucket_expr` — the renderer, not a
/// retyped copy — so a change to the renderer moves this test.
///
/// **What makes it discriminating.** The floor form's answer for each input
/// is computed alongside and asserted to DIFFER at every row, so the test
/// cannot pass against the expression it exists to reject. That check
/// matters more than usual here: the only other exercise of the shipped
/// range renderer puts the same expression on both sides of a comparison,
/// and a differential between two paths that share a defect cannot see it.
#[tokio::test]
async fn the_anchored_bucket_expression_is_the_grid_the_window_implies() {
    skip_unless_live!();
    // No corpus and no run database: the expression is evaluated over a
    // literal row set, so there is nothing to seed and nothing to drop.
    let client = ChClient::new(test_config()).await.expect("connect");

    const G: i64 = 1_700_000_000_000_000_000;
    const STEP: i64 = 60_000_000_000;
    let lo = G - STEP;

    // The renderer's own text, minted through the sealed fragment.
    let bucket =
        pulsus_read::logql::predicate::bucket_expr("timestamp_ns", lo, STEP, lo, G + 10 * STEP)
            .expect("a renderable grid");

    let cases: [(&str, i64, i64); 6] = [
        ("G - 59.999999999s", G - 59_999_999_999, G),
        ("G", G, G),
        ("G + 1ns", G + 1, G + STEP),
        ("G + 30s", G + 30_000_000_000, G + STEP),
        ("G + 60s", G + STEP, G + STEP),
        ("G + 60s + 1ns", G + STEP + 1, G + 2 * STEP),
    ];
    let rows_sql = cases
        .iter()
        .map(|(label, t, expected)| {
            format!("SELECT '{label}' AS label, {t}::Int64 AS timestamp_ns, {expected}::Int64 AS expected")
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let sql = format!(
        "SELECT label, timestamp_ns AS t, ({}) AS got, expected FROM ({rows_sql}) ORDER BY t ASC",
        bucket.as_sql()
    );

    let mut stream = client
        .query_stream::<BucketRow>(&sql, &QuerySettings::new())
        .await
        .expect("execute the bucket expression");
    let mut got = Vec::new();
    while let Some(row) = stream.next().await {
        got.push(row.expect("decode a bucket row"));
    }
    assert_eq!(got.len(), cases.len(), "one row per input: {got:?}");

    for row in &got {
        assert_eq!(
            row.got, row.expected,
            "`{}` at {} must bucket to {}, got {}",
            row.label, row.t, row.expected, row.got
        );
        // The grid point is on the query's grid, which the floor form's
        // answer need not be.
        assert_eq!(
            (row.got - G).rem_euclid(STEP),
            0,
            "`{}` must bucket to a point of the query's own grid",
            row.label
        );
        // The expression this replaces gives a different answer here, so a
        // build that kept it fails this test rather than passing it.
        let floored = row.t.div_euclid(STEP) * STEP;
        assert_ne!(
            floored, row.got,
            "`{}`: the epoch-anchored floor must not agree, or this test cannot reject it",
            row.label
        );
    }
}

// ---------------------------------------------------------------------
// W3 (issue #507): the rule every pushdown cell is built to, executed.
// ---------------------------------------------------------------------

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct KeptRow {
    kept: u8,
}

/// **A pushed parsed-name filter never drops a row the evaluator keeps**
/// (issue #507, W3).
///
/// A `Fidelity::Wider` predicate may emit rows the evaluator discards and
/// may never discard one it keeps. The two halves of that claim live in
/// one file, `tests/logql_parsed_filter_witnesses.tsv`: its `keeps` column
/// is the shipped pipeline's own answer, verified by
/// `logql::predicate::tests::every_witness_row_states_the_answer_the_pipeline_gives`,
/// and this test executes the rendered fragment against the database over
/// the same bodies.
///
/// **The assertion is one-directional on purpose.** A row the database
/// keeps and the evaluator drops is the predicate being wider, which is
/// allowed and is counted rather than failed — the count is asserted to be
/// non-zero, because a predicate that admitted exactly the evaluator's set
/// would mean the witnesses cannot tell the two directions apart.
#[tokio::test]
async fn a_pushed_parsed_name_filter_never_drops_a_row_the_evaluator_keeps() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");

    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/logql_parsed_filter_witnesses.tsv"
    );
    let text = std::fs::read_to_string(path).expect("the witness table is readable");
    let mut checked = 0usize;
    let mut wider = 0usize;
    for line in text.lines() {
        if line.starts_with('#') || line.trim().is_empty() || line.starts_with("form\t") {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        assert_eq!(f.len(), 7, "seven columns: {line:?}");
        let (form, parser_name, arg, name, value, body) = (f[0], f[1], f[2], f[3], f[4], f[5]);
        let evaluator_keeps = f[6] == "1";

        let parser = match parser_name {
            "json" => pulsus_logql::ParserStage::Json {
                extractions: Vec::new(),
            },
            "logfmt" => pulsus_logql::ParserStage::Logfmt {
                strict: false,
                keep_empty: false,
                extractions: Vec::new(),
            },
            "regexp" => pulsus_logql::ParserStage::Regexp(arg.to_string()),
            "pattern" => pulsus_logql::ParserStage::Pattern(arg.to_string()),
            other => panic!("unknown parser {other}"),
        };
        let fragment = if form == "neq" {
            pulsus_read::logql::predicate::parsed_string_filter(
                name,
                pulsus_logql::MatchOp::Neq,
                value,
                &parser,
            )
        } else if form == "numeric" {
            pulsus_read::logql::predicate::parsed_numeric_filter(
                name,
                pulsus_logql::CompareOp::Gte,
                value.parse::<f64>().expect("a numeric witness value"),
                &parser,
            )
        } else {
            pulsus_read::logql::predicate::parsed_string_filter(
                name,
                pulsus_logql::MatchOp::Eq,
                value,
                &parser,
            )
        };
        // A refused filter emits nothing, so there is no predicate to be
        // wrong about; the evaluator answers as it does today.
        let Ok(fragment) = fragment else { continue };
        checked += 1;

        let body_literal = pulsus_read::logql::predicate::literal(body);
        let sql = format!(
            "SELECT toUInt8({}) AS kept FROM (SELECT {} AS body, '' AS structured_metadata)",
            fragment.as_sql(),
            body_literal.as_sql()
        );
        let mut stream = client
            .query_stream::<KeptRow>(&sql, &QuerySettings::new())
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        let row = stream
            .next()
            .await
            .expect("one row")
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        let sql_keeps = row.kept == 1;

        if evaluator_keeps {
            assert!(
                sql_keeps,
                "`{form} {parser_name} {name} {value}` over `{body}`: the evaluator keeps this \
                 row and the pushed predicate drops it, which a Wider predicate may never do.\n\
                 {sql}"
            );
        } else if sql_keeps {
            wider += 1;
        }
    }
    assert!(
        checked >= 10,
        "only {checked} witness rows reached a fragment"
    );
    assert!(
        wider > 0,
        "no witness row is kept by the predicate and dropped by the evaluator, so these \
         witnesses cannot tell a wider predicate from an exact one"
    );
}

// ---------------------------------------------------------------------
// W2 (issue #507): the bucketed range read, end to end.
// ---------------------------------------------------------------------

/// One series of a matrix answer: its sorted labels, and its points as
/// `(timestamp, value bits)` so "bit for bit" is literally that.
type AnswerSeries = (Vec<(String, String)>, Vec<(i64, u64)>);

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct BucketedSeedRow {
    /// The tenant (issue #653): the empty tenant's rows.
    org_id: String,
    service: String,
    fingerprint: u128,
    timestamp_ns: i64,
    severity: i8,
    body: String,
    structured_metadata: String,
}

/// Seeds the two-stream fixture the bucketed differential reads, into a
/// fresh database, and returns `(admin, db, T)` where `T` is the emit
/// grid's first point.
///
/// **The two streams fold into ONE output series**, which is the shape
/// that separates folding before emission from folding after it:
///
/// ```text
/// fp 111   {app=a,           service_name=…}   rows carry metadata lvl=info
/// fp 222   {app=a, lvl=info, service_name=…}   rows carry none
/// ```
///
/// ```text
/// grid            T        T+60    T+120   T+180   T+240
/// fp 111 rows     2 (info)   -        -      1       -
/// fp 222 rows     -          -        3      -       -
/// folded series   2          -        3      1       -
/// ```
///
/// Two further rows on fp 111 at the first grid point — one with
/// `lvl=warn`, one with no metadata at all — make three output series, so
/// "everything at this grid point" is not the same answer as "this
/// series at this grid point".
async fn seed_bucketed_corpus() -> (ChClient, String, i64) {
    const STEP: i64 = 60_000_000_000;
    let db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_bucket_{}",
        uuid::Uuid::new_v4().simple()
    ));
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop");
    admin
        .execute(
            &format!("CREATE DATABASE {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create");
    run_init(&admin, &test_ctx(&db)).await.expect("run_init");
    let client = data_client(&db).await;

    // A grid point on a round multiple of the step, an hour back, so the
    // whole fixture sits inside one partition and one retention window.
    let t = ((now_ns() - 3_600_000_000_000) / STEP) * STEP;
    let service = "c507bucket";
    for (fp, labels) in [
        (
            111u64,
            format!(r#"{{"app":"a","service_name":"{service}"}}"#),
        ),
        (
            222u64,
            format!(r#"{{"app":"a","lvl":"info","service_name":"{service}"}}"#),
        ),
    ] {
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, \
                     updated_ns) VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({t}))), \
                     {fp}, '{service}', '{labels}', 0)"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed log_streams");
        land_seeded_streams(&client, &db).await;
    }

    let row = |fp: u64, ts: i64, body: &str, sm: &str| BucketedSeedRow {
        org_id: String::new(),
        service: service.to_string(),
        fingerprint: u128::from(fp),
        timestamp_ns: ts,
        severity: 0,
        body: body.to_string(),
        structured_metadata: sm.to_string(),
    };
    let info = r#"{"lvl":"info"}"#;
    let rows = vec![
        // Grid point T, window (T-60s, T].
        row(111, t - 30_000_000_000, "a", info),
        row(111, t - 20_000_000_000, "b", info),
        row(111, t - 25_000_000_000, "c", r#"{"lvl":"warn"}"#),
        row(111, t - 10_000_000_000, "d", ""),
        // Grid point T+120s: fp 222 only — the point fp 111 has a gap at.
        row(222, t + 90_000_000_000, "e", ""),
        row(222, t + 95_000_000_000, "f", ""),
        row(222, t + 100_000_000_000, "g", ""),
        // Grid point T+180s: fp 111 only.
        row(111, t + 150_000_000_000, "h", info),
    ];
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("insert the bucketed fixture");
    land_seeded_lines(&client, &db).await;
    (admin, db, t)
}

/// **The lowered answer is the client answer, bit for bit — and the two
/// answers come from two different code paths** (issue #507, W2).
///
/// `count_over_time({…}[1m])` at a 1m step is a clean bucketed chain and
/// plans `client: None`, so the database counts. The same selector with
/// `| line_format "{{__line__}}"` — which rewrites every line to itself, so the stage changes
/// no line, no byte and no label set — is a pipeline, so it plans `client: Some(..)` and every
/// line crosses the wire to be counted here. The answers must be
/// identical, which is what `Fidelity::Equivalent` claims for the four
/// counting reducers.
///
/// The plan shapes are asserted first. Without that the test could be
/// comparing one path against itself and would pass for the wrong reason.
///
/// It also pins the GAP: the folded series carries a point wherever
/// either fingerprint had a row and no point at the two grid points where
/// neither did — not a zero, not a NaN, no point.
#[tokio::test]
async fn a_bucketed_range_read_answers_exactly_what_the_client_path_answers() {
    skip_unless_live!();
    const STEP: i64 = 60_000_000_000;
    let (admin, db, t) = seed_bucketed_corpus().await;
    let service = "c507bucket";

    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 4 * STEP,
            step_ns: STEP as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let lowered_query = format!(r#"count_over_time({{service_name="{service}"}}[1m])"#);
    let client_query = format!(
        r#"count_over_time({{service_name="{service}"}} | line_format "{{{{__line__}}}}" [1m])"#
    );

    // The two paths, asserted to BE two paths.
    let shape = |query: &str| match plan(&parse(query).expect("parse"), &params, &plan_ctx(&db))
        .expect("plan")
    {
        Plan::Metric(mp) => mp,
        _ => panic!("expected a metric plan"),
    };
    assert!(
        shape(&lowered_query).client.is_none(),
        "the fixture query must lower its aggregation into the statement"
    );
    assert!(
        shape(&client_query).client.is_some(),
        "the control query must stay on the client path, or this compares one path with itself"
    );

    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let answer = |query: String| {
        let engine = &engine;
        let params = &params;
        async move {
            let (result, _warnings) = engine
                .query(&parse(&query).expect("parse"), params)
                .await
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            let QueryResult::Matrix(series) = result else {
                panic!("{query}: expected a matrix");
            };
            let mut out: Vec<AnswerSeries> = series
                .into_iter()
                .map(|s| {
                    let mut labels = s.labels;
                    labels.sort();
                    (
                        labels,
                        s.points
                            .into_iter()
                            .map(|(ts, v)| (ts, v.to_bits()))
                            .collect(),
                    )
                })
                .collect();
            out.sort();
            out
        }
    };
    let lowered = answer(lowered_query).await;
    let client = answer(client_query).await;

    assert_eq!(
        lowered, client,
        "the lowered answer and the client answer must agree bit for bit"
    );

    // The fixture's own expectation, so a shared defect in both paths
    // cannot pass this test: three series, and the folded one carries
    // points only where a contributing row exists.
    assert_eq!(lowered.len(), 3, "{lowered:?}");
    let folded = lowered
        .iter()
        .find(|(labels, _)| labels.contains(&("lvl".to_string(), "info".to_string())))
        .expect("the folded series");
    assert_eq!(
        folded.1,
        vec![
            (t, 2.0f64.to_bits()),
            (t + 2 * STEP, 3.0f64.to_bits()),
            (t + 3 * STEP, 1.0f64.to_bits()),
        ],
        "a grid point with no contributing row is not emitted at all"
    );

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

/// **The grid-resolution guard holds on the bucketed path too** (issue
/// #507, W2).
///
/// The client path reaches `window::ensure_grid_resolution` by building a
/// `ClientWindow::Range`; the bucketed path builds no window, so the guard
/// is called from the arm itself. Without that call an over-cap grid would
/// be answered instead of refused — a query that 422s today would start
/// returning a matrix — and no other assertion in the tree would notice.
///
/// Both queries below ask for 14 400 intervals against a cap of 11 000:
/// the lowered one and the client-path control, which must refuse the same
/// way. The corpus must resolve at least one stream, because the guard
/// sits after stream resolution on both paths.
#[tokio::test]
async fn an_over_cap_grid_is_refused_on_the_bucketed_path_as_on_the_client_path() {
    skip_unless_live!();
    let (admin, db, t) = seed_bucketed_corpus().await;
    let service = "c507bucket";

    // 4 hours at a 1s step: 14 400 intervals, against MAX_CLIENT_AGG_BUCKETS.
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 14_400_000_000_000,
            step_ns: 1_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    for (query, path) in [
        (
            format!(r#"count_over_time({{service_name="{service}"}}[1s])"#),
            "the bucketed path",
        ),
        (
            format!(
                r#"count_over_time({{service_name="{service}"}} | line_format "{{{{__line__}}}}" [1s])"#
            ),
            "the client path",
        ),
    ] {
        let err = engine
            .query(&parse(&query).expect("parse"), &params)
            .await
            .expect_err("an over-cap grid must be refused");
        assert!(
            matches!(err, ReadError::QueryTooBroad(_)),
            "{path}: expected the named buckets refusal, got {err:?}"
        );
        assert!(
            err.to_string().contains("11000"),
            "{path}: the refusal must name the cap, got {err}"
        );
    }

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

/// **The `/explain` seam reports the statement the engine issues** (issue
/// #507, W2).
///
/// `LogQlEngine::explain` refused a range plan with no client aggregation
/// until W2, arguing — correctly at the time — that the engine would
/// refuse it too. The engine now serves it, so an EXPLAIN that still
/// refused would be a wrong answer on a route a user can reach.
///
/// **This is the only caller of that seam in the repository**: the
/// `X-Pulsus-Explain` header routes through `query_explained`, which
/// collects the payload from the EXECUTION path. So the twin had no
/// coverage at all, and a break placed in it stayed green everywhere —
/// measured, which is why this test exists.
///
/// The assertion is equality with the executing path's own reported
/// statement, not a snapshot: the two come from one function and this is
/// what holds them there.
#[tokio::test]
async fn the_explain_seam_reports_the_bucketed_statement_the_reader_issues() {
    skip_unless_live!();
    const STEP: i64 = 60_000_000_000;
    let (admin, db, t) = seed_bucketed_corpus().await;
    let service = "c507bucket";

    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 4 * STEP,
            step_ns: STEP as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let expr = parse(&format!(
        r#"count_over_time({{service_name="{service}"}}[1m])"#
    ))
    .expect("parse");
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );

    let explained = engine.explain(&expr, &params).await.expect("explain");
    let reported = explained
        .stages
        .iter()
        .find(|s| s.name == "metric_read")
        .map(|s| s.sql.clone())
        .expect("the explain payload names the metric read");
    assert!(
        reported.contains("AS bucket_ns") && reported.contains("GROUP BY fingerprint, bucket_ns"),
        "the explain seam must report the BUCKETED statement, got:\n{reported}"
    );
    assert_eq!(
        explained.routing.as_ref().map(|r| r.reason.as_str()),
        Some("raw: bucketed range aggregation (issue #507)")
    );

    // The executing path's own reported statement, for the same query.
    let (_r, _w, executed) = engine
        .query_explained(&expr, &params)
        .await
        .expect("query with explain");
    let issued = executed
        .stages
        .iter()
        .find(|s| s.name == "metric_read")
        .map(|s| s.sql.clone())
        .expect("the execution payload names the metric read");
    assert_eq!(
        reported, issued,
        "the explain seam and the executing path must report ONE statement"
    );

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

// ---------------------------------------------------------------------
// Issue #624: the sliding statement, executed.
// ---------------------------------------------------------------------

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SlidingRow {
    fingerprint: u128,
    bucket_ns: i64,
    n: u64,
    structured_metadata: String,
}

/// One `(start, end, step, range)` the sliding statement is checked at.
#[derive(Debug, Clone, Copy)]
struct SlidingCase {
    s: i64,
    e: i64,
    step: i64,
    range: i64,
}

/// One cell of an answer: `(fingerprint, metadata, grid point)`.
type SlidingCell = (u128, String, i64);

/// **N3 (issue #624): every row the sliding statement returns is the
/// window definition, evaluated here by brute force.**
///
/// 1,200 lines on three streams and three metadata values, at ±1 ns of
/// 400 points an hour wide, one third with empty bodies. Sixty
/// `(start, end, step, range)` drawn from a fixed seed — steps of 1 s to
/// 300 s with a nanosecond remainder, ranges equal to the step, one
/// nanosecond either side of it, half, twice, twenty times and random,
/// unaligned starts and ends — each rendered for count and for bytes.
///
/// The expectation is independent code: every grid point `g` scans every
/// line and counts it when `g - range < ts <= g`. A cell is expected when
/// its window holds at least one line, so a bytes total of 0 over empty
/// bodies is a point.
#[tokio::test]
async fn the_sliding_statement_is_the_window_definition_at_every_range_and_step() {
    skip_unless_live!();
    const SEC: i64 = 1_000_000_000;
    let db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_sliding_{}",
        uuid::Uuid::new_v4().simple()
    ));
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    for sql in [
        format!("DROP DATABASE IF EXISTS {db}"),
        format!("CREATE DATABASE {db}"),
    ] {
        admin
            .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .expect("create the run database");
    }
    run_init(&admin, &test_ctx(&db)).await.expect("run_init");
    let client = data_client(&db).await;

    // The lines.
    let service = "c624sliding";
    let fps: [u128; 3] = [6_240_001, 6_240_002, 6_240_003];
    let metadata = ["", r#"{"lvl":"info"}"#, r#"{"lvl":"warn"}"#];
    let t0 = ((now_ns() - 2 * 3_600 * SEC) / SEC) * SEC;
    let mut seed = 624u64;
    let mut next = || {
        seed = splitmix64(seed);
        seed
    };
    let mut rows = Vec::new();
    for i in 0..400i64 {
        let point = t0 + i * 9 * SEC + i * 7;
        for d in [-1i64, 0, 1] {
            let n = rows.len();
            let body = if n % 3 == 0 {
                String::new()
            } else {
                "x".repeat(1 + (next() % 40) as usize)
            };
            rows.push(BucketedSeedRow {
                org_id: String::new(),
                service: service.to_string(),
                fingerprint: fps[(next() % 3) as usize],
                timestamp_ns: point + d,
                severity: 0,
                body,
                structured_metadata: metadata[(next() % 3) as usize].to_string(),
            });
        }
    }
    assert_eq!(rows.len(), 1_200);
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("insert the sliding fixture");
    land_seeded_lines(&client, &db).await;

    // The cases.
    let span = 400 * 9 * SEC;
    let mut cases = Vec::new();
    for i in 0..60u64 {
        let step = (1 + (next() % 300) as i64) * SEC + (next() % SEC as u64) as i64;
        let range = match i % 7 {
            0 => step,
            1 => step + 1,
            2 => step - 1,
            3 => step / 2,
            4 => 2 * step,
            5 => 20 * step,
            _ => 1 + (next() % (30 * step as u64)) as i64,
        };
        let s = t0 + (next() % span as u64) as i64 - range / 2;
        let points = 1 + (next() % (span / step + 2).min(150) as u64) as i64;
        let e = s + (points - 1) * step + (next() % step as u64) as i64;
        cases.push(SlidingCase { s, e, step, range });
    }

    let fp_literals: Vec<_> = fps
        .iter()
        .map(|fp| Fingerprint::from_raw(*fp).sql_literal())
        .collect();
    let (mut cells, mut zero_bytes, mut statements) = (0usize, 0usize, 0usize);
    let (mut below, mut equal, mut above) = (0usize, 0usize, 0usize);
    let mut mismatches: Vec<String> = Vec::new();
    for case in &cases {
        match case.range.cmp(&case.step) {
            std::cmp::Ordering::Less => below += 1,
            std::cmp::Ordering::Equal => equal += 1,
            std::cmp::Ordering::Greater => above += 1,
        }
        let kmax = (case.e - case.s).div_euclid(case.step);
        for shape in [sql::MetricShape::RawCount, sql::MetricShape::RawBytes] {
            let is_bytes = shape == sql::MetricShape::RawBytes;
            let statement = sql::metric_range_sliding(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                sql::MetricSource::new("log_samples", shape),
                &[literal(service)],
                &fp_literals,
                sql::SlidingScan {
                    window: TimeWindow {
                        start_ns: case.s - case.range,
                        end_ns: case.e,
                    },
                    lower: sql::ScanLowerBound::Exclusive,
                    grid_start_ns: case.s,
                    step_ns: case.step,
                    range_ns: case.range,
                },
                &[],
            )
            .unwrap_or_else(|r| panic!("{case:?}: the sliding statement refused: {r:?}"));
            statements += 1;

            let mut got: std::collections::BTreeMap<SlidingCell, u64> = Default::default();
            let mut stream = client
                .query_stream::<SlidingRow>(&statement, &QuerySettings::new())
                .await
                .unwrap_or_else(|e| panic!("{case:?}: execute: {e}"));
            while let Some(row) = stream.next().await {
                let row = row.expect("decode a sliding row");
                let prior = got.insert(
                    (row.fingerprint, row.structured_metadata, row.bucket_ns),
                    row.n,
                );
                assert!(prior.is_none(), "{case:?}: a cell returned twice");
            }
            drop(stream);

            let mut want: std::collections::BTreeMap<SlidingCell, u64> = Default::default();
            for k in 0..=kmax {
                let g = case.s + k * case.step;
                cells += fps.len() * metadata.len();
                for line in &rows {
                    if g - case.range < line.timestamp_ns && line.timestamp_ns <= g {
                        let v = if is_bytes { line.body.len() as u64 } else { 1 };
                        *want
                            .entry((line.fingerprint, line.structured_metadata.clone(), g))
                            .or_insert(0) += v;
                    }
                }
            }
            if is_bytes {
                zero_bytes += want.values().filter(|v| **v == 0).count();
            }
            if got != want {
                mismatches.push(format!(
                    "{case:?} bytes={is_bytes}: {} cells returned, {} expected",
                    got.len(),
                    want.len()
                ));
            }
        }
    }

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");

    assert_eq!(statements, 120);
    assert!(
        mismatches.is_empty(),
        "{} of {statements} statements differ from the window definition: {mismatches:#?}",
        mismatches.len()
    );
    assert!(cells >= 10_000, "only {cells} grid cells compared");
    assert!(
        below >= 1 && equal >= 1 && above >= 1,
        "range below / equal / above the step: {below} / {equal} / {above}"
    );
    assert!(zero_bytes >= 1, "no zero-byte point was compared");
}

/// One engine answer, as sorted `(labels, (timestamp, value bits))` series.
async fn sliding_answer(
    engine: &LogQlEngine,
    query: &str,
    params: &QueryParams,
) -> Vec<AnswerSeries> {
    let (result, _warnings) = engine
        .query(&parse(query).expect("parse"), params)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let QueryResult::Matrix(series) = result else {
        panic!("{query}: expected a matrix");
    };
    let mut out: Vec<AnswerSeries> = series
        .into_iter()
        .map(|s| {
            let mut labels = s.labels;
            labels.sort();
            (
                labels,
                s.points
                    .into_iter()
                    .map(|(ts, v)| (ts, v.to_bits()))
                    .collect(),
            )
        })
        .collect();
    out.sort();
    out
}

/// The three series of the bucketed fixture, by their extra label.
fn fixture_series(lvl: Option<&str>) -> Vec<(String, String)> {
    let mut labels = vec![
        ("app".to_string(), "a".to_string()),
        ("service_name".to_string(), "c507bucket".to_string()),
    ];
    if let Some(lvl) = lvl {
        labels.push(("lvl".to_string(), lvl.to_string()));
    }
    labels.sort();
    labels
}

/// A series' expected points from `(seconds after T, count)`.
fn fixture_points(t: i64, points: &[(i64, f64)]) -> Vec<(i64, u64)> {
    points
        .iter()
        .map(|(sec, v)| (t + sec * 1_000_000_000, v.to_bits()))
        .collect()
}

/// The `[150s]` count answer over `T..T+240s` at 60 s, shifted by `shift`
/// seconds.
fn fixture_150s(t: i64, shift: i64) -> Vec<AnswerSeries> {
    let at = |p: &[(i64, f64)]| {
        fixture_points(
            t,
            &p.iter().map(|(s, v)| (s + shift, *v)).collect::<Vec<_>>(),
        )
    };
    let mut out = vec![
        (
            fixture_series(Some("info")),
            at(&[(0, 2.0), (60, 2.0), (120, 4.0), (180, 4.0), (240, 3.0)]),
        ),
        (
            fixture_series(Some("warn")),
            at(&[(0, 1.0), (60, 1.0), (120, 1.0)]),
        ),
        (fixture_series(None), at(&[(0, 1.0), (60, 1.0), (120, 1.0)])),
    ];
    out.sort();
    out
}

/// **N4 (issue #624): a range wider or narrower than the step is counted
/// in the database, and answers what today's route answers, bit for bit.**
///
/// Over the bucketed fixture (`seed_bucketed_corpus`), at a 60 s step:
/// `[150s]` holds a line in two or three windows, `[30s]` leaves lines in
/// none. The control `| line_format "{{__line__}}"` rewrites every line to itself,
/// so it changes no line and no label set and keeps the query on today's route
/// (`| drop zzz`, the control before issue #624 part 2, now lowers too).
///
/// The fixture's own expectation is asserted too, so a defect both routes
/// shared would not pass. A window closed below would give `[150s]`
/// `(T+120, 5)` and `(T+240, 4)`, and `[30s]` `(T, 2)`, `(T+120, 3)` and
/// `(T+180, 1)`.
#[tokio::test]
async fn a_range_unequal_to_the_step_is_counted_in_the_database_and_answers_todays_answer() {
    skip_unless_live!();
    const STEP: i64 = 60_000_000_000;
    let (admin, db, t) = seed_bucketed_corpus().await;
    let selector = r#"{service_name="c507bucket"}"#;
    let params = |from: i64| QueryParams {
        spec: QuerySpec::Range {
            start_ns: from,
            end_ns: from + 4 * STEP,
            step_ns: STEP as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let at_t = params(t);

    // (i) the two routes, asserted to BE two routes.
    let shape = |query: &str| match plan(&parse(query).expect("parse"), &at_t, &plan_ctx(&db))
        .expect("plan")
    {
        Plan::Metric(mp) => mp,
        _ => panic!("expected a metric plan"),
    };
    let lowered = shape(&format!("count_over_time({selector}[150s])"));
    assert!(
        lowered.client.is_none(),
        "a clean counting range read at a range wider than the step is counted in the database"
    );
    assert_eq!(
        lowered.routing.reason,
        "raw: sliding range aggregation in the database (issue #624)"
    );
    assert!(
        shape(&format!(
            "count_over_time({selector} | line_format \"{{{{__line__}}}}\" [150s])"
        ))
        .client
        .is_some(),
        "the control query must stay on today's route"
    );

    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );

    // (ii) every reducer, both ranges: the lowered answer is the control's.
    let mut answers = std::collections::HashMap::new();
    for (reducer, range) in [
        ("count_over_time", "150s"),
        ("bytes_over_time", "150s"),
        ("rate", "150s"),
        ("bytes_rate", "150s"),
        ("count_over_time", "30s"),
    ] {
        let lowered =
            sliding_answer(&engine, &format!("{reducer}({selector}[{range}])"), &at_t).await;
        let control = sliding_answer(
            &engine,
            &format!("{reducer}({selector} | line_format \"{{{{__line__}}}}\" [{range}])"),
            &at_t,
        )
        .await;
        assert_eq!(
            lowered, control,
            "{reducer} [{range}]: the lowered answer must equal today's route's"
        );
        answers.insert((reducer, range), lowered);
    }

    // (iii) the fixture's own expectation.
    assert_eq!(answers[&("count_over_time", "150s")], fixture_150s(t, 0));
    let mut want_30s = vec![
        (
            fixture_series(Some("info")),
            fixture_points(t, &[(0, 1.0), (120, 2.0)]),
        ),
        (fixture_series(Some("warn")), fixture_points(t, &[(0, 1.0)])),
        (fixture_series(None), fixture_points(t, &[(0, 1.0)])),
    ];
    want_30s.sort();
    assert_eq!(answers[&("count_over_time", "30s")], want_30s);

    // (iv) an offset: the same windows, reported 60 s later.
    let later = params(t + STEP);
    let offset = sliding_answer(
        &engine,
        &format!("count_over_time({selector}[150s] offset 60s)"),
        &later,
    )
    .await;
    assert_eq!(offset, fixture_150s(t, 60), "the offset is added back");
    let control = sliding_answer(
        &engine,
        &format!(
            "count_over_time({selector} | line_format \"{{{{__line__}}}}\" [150s] offset 60s)"
        ),
        &later,
    )
    .await;
    assert_eq!(
        offset, control,
        "offset: the lowered answer equals today's route's"
    );

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CommentedStatementRow {
    query: String,
    exception_code: i32,
}

/// **N8 (issue #624): a bucketed statement that fails for memory hands the
/// query to today's route, once, and the answer is today's.**
///
/// The test hook renders `SETTINGS max_memory_usage = 1` into the
/// statement, so the server refuses it with code 241. The query must still
/// answer N4's `[150s]` points, and the query log must hold the sliding
/// statement, failed with 241, under this engine's comment.
#[tokio::test]
async fn a_bucketed_statement_that_fails_for_memory_is_answered_by_todays_route() {
    skip_unless_live!();
    const STEP: i64 = 60_000_000_000;
    let (admin, db, t) = seed_bucketed_corpus().await;
    let comment = format!("c624-memory-{}", uuid::Uuid::new_v4().simple());
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    )
    .with_key_route_test_hooks(pulsus_read::logql::exec::KeyRouteTestHooks {
        bucketed_statement_max_memory_bytes: Some(1),
        ..Default::default()
    })
    .with_query_log_comment(comment.clone());
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 4 * STEP,
            step_ns: STEP as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let got = sliding_answer(
        &engine,
        r#"count_over_time({service_name="c507bucket"}[150s])"#,
        &params,
    )
    .await;
    assert_eq!(got, fixture_150s(t, 0), "today's route answers");

    // The failed statement, from the query log. `SYSTEM FLUSH LOGS` does not
    // guarantee the row is there, so this waits for it.
    let mut logged: Vec<CommentedStatementRow> = Vec::new();
    for _ in 0..30 {
        admin
            .execute(
                "SYSTEM FLUSH LOGS",
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("flush logs");
        let sql = format!(
            "SELECT query, exception_code FROM system.query_log \
             WHERE log_comment = '{comment}' AND type != 'QueryStart' \
             ORDER BY event_time_microseconds ASC"
        );
        let mut stream = admin
            .query_stream::<CommentedStatementRow>(&sql, &QuerySettings::new())
            .await
            .expect("read system.query_log");
        logged.clear();
        while let Some(row) = stream.next().await {
            logged.push(row.expect("decode a query_log row"));
        }
        drop(stream);
        if logged
            .iter()
            .any(|r| r.query.contains("leadInFrame(k0, 1, "))
        {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let sliding: Vec<&CommentedStatementRow> = logged
        .iter()
        .filter(|r| r.query.contains("leadInFrame(k0, 1, "))
        .collect();
    assert_eq!(
        sliding.len(),
        1,
        "one sliding statement under the comment: {logged:#?}"
    );
    assert_eq!(
        sliding[0].exception_code, 241,
        "the sliding statement must fail for memory: {logged:#?}"
    );

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

/// Seeds `streams` streams of service `c624cap`, one line each at `T + 100s`,
/// into a fresh database, and returns `(admin, db, T)`.
async fn seed_cap_corpus(streams: u64) -> (ChClient, String, i64) {
    let db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_cap_{}",
        uuid::Uuid::new_v4().simple()
    ));
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    for sql in [
        format!("DROP DATABASE IF EXISTS {db}"),
        format!("CREATE DATABASE {db}"),
    ] {
        admin
            .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .expect("create the run database");
    }
    run_init(&admin, &test_ctx(&db)).await.expect("run_init");
    let client = data_client(&db).await;
    let t = ((now_ns() - 4 * 3_600_000_000_000) / 1_000_000_000) * 1_000_000_000;
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) \
                 SELECT toStartOfMonth(fromUnixTimestamp64Nano(toInt64({t}))), 6240000 + number, \
                 'c624cap', concat('{{\"pod\":\"p', toString(number), \
                 '\",\"service_name\":\"c624cap\"}}'), 0 FROM numbers({streams})"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");
    land_seeded_streams(&client, &db).await;
    let rows: Vec<BucketedSeedRow> = (0..streams)
        .map(|i| BucketedSeedRow {
            org_id: String::new(),
            service: "c624cap".to_string(),
            fingerprint: u128::from(6_240_000 + i),
            timestamp_ns: t + 100_000_000_000,
            severity: 0,
            body: "x".to_string(),
            structured_metadata: String::new(),
        })
        .collect();
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("insert the cap fixture");
    land_seeded_lines(&client, &db).await;
    (admin, db, t)
}

/// **At a range equal to the step, a reducing aggregation over many sparse
/// series is answered, as it was before** (issue #624, code review round 2).
///
/// 1,091 one-line streams on an 11,001-point grid at a 1 s step, `[1s]`. The
/// bucketed fold keeps each series' points sparsely and charges them one by
/// one, so 1,091 points are charged; reserving one grid per series would be
/// 12,002,091 slots, past `MAX_METRIC_RESULT_POINTS`, and a 422. Each query
/// answers one point at `T + 100s`.
#[tokio::test]
async fn a_reducing_aggregation_over_sparse_series_at_range_equal_to_step_is_answered() {
    skip_unless_live!();
    const GRID: u64 = 11_001;
    let streams = pulsus_read::logql::MAX_METRIC_RESULT_POINTS / GRID + 1;
    let (admin, db, t) = seed_cap_corpus(streams).await;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 11_000_000_000_000,
            step_ns: 1_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let at = t + 100_000_000_000;
    let sel = r#"{service_name=~".+"}"#;
    let by_service = vec![("service_name".to_string(), "c624cap".to_string())];
    for (query, labels, value) in [
        (format!("topk(1, rate({sel}[1s]))"), None, 1.0f64),
        (format!("max(rate({sel}[1s]))"), Some(vec![]), 1.0),
        (
            format!("count by (service_name) (count_over_time({sel}[1s]))"),
            Some(by_service),
            streams as f64,
        ),
    ] {
        let mp = match plan(&parse(&query).expect("parse"), &params, &plan_ctx(&db)).expect("plan")
        {
            Plan::Metric(mp) => mp,
            _ => panic!("expected a metric plan"),
        };
        assert!(
            mp.client.is_none(),
            "{query}: must be counted in the database"
        );
        let got = sliding_answer(&engine, &query, &params).await;
        assert_eq!(got.len(), 1, "{query}: one series, got {got:?}");
        if let Some(labels) = labels {
            assert_eq!(got[0].0, labels, "{query}");
        }
        assert_eq!(got[0].1, vec![(at, value.to_bits())], "{query}");
    }

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

// ---------------------------------------------------------------------
// Issue #624, part 2: counting behind label-only stages, in the database.
// ---------------------------------------------------------------------

/// Which cap a search moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cap624 {
    GroupBytes,
    ResultPoints,
}

/// The hooks one run takes: today's route or the lowered one, an optional
/// cap override, and an optional statement memory ceiling.
fn hooks_624(
    todays: bool,
    cap: Option<(Cap624, u64)>,
    memory: Option<u64>,
) -> pulsus_read::logql::exec::KeyRouteTestHooks {
    let mut caps = pulsus_read::logql::exec::AggCapOverrides::default();
    match cap {
        Some((Cap624::GroupBytes, n)) => caps.group_bytes = Some(n),
        Some((Cap624::ResultPoints, n)) => caps.result_points = Some(n),
        None => {}
    }
    pulsus_read::logql::exec::KeyRouteTestHooks {
        bucketed_statement_max_memory_bytes: memory,
        agg_caps: caps,
        todays_route_only: todays,
    }
}

/// One query through an engine built for this run: its answer, sorted, or
/// its error. `comment` tags every statement the run issues.
async fn run_624(
    db: &str,
    hooks: pulsus_read::logql::exec::KeyRouteTestHooks,
    comment: Option<&str>,
    query: &str,
    params: &QueryParams,
) -> Result<Vec<AnswerSeries>, ReadError> {
    let mut engine = LogQlEngine::new(
        data_client(db).await,
        engine_config(db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    )
    .with_key_route_test_hooks(hooks);
    if let Some(c) = comment {
        engine = engine.with_query_log_comment(c.to_string());
    }
    let (result, _warnings) = engine.query(&parse(query).expect("parse"), params).await?;
    let QueryResult::Matrix(series) = result else {
        panic!("{query}: expected a matrix");
    };
    let mut out: Vec<AnswerSeries> = series
        .into_iter()
        .map(|s| {
            let mut labels = s.labels;
            labels.sort();
            (
                labels,
                s.points
                    .into_iter()
                    .map(|(ts, v)| (ts, v.to_bits()))
                    .collect(),
            )
        })
        .collect();
    out.sort();
    Ok(out)
}

/// The two answers agree: equal matrices, or the same error kind and type.
fn same_outcome_624(
    what: &str,
    lowered: &Result<Vec<AnswerSeries>, ReadError>,
    todays: &Result<Vec<AnswerSeries>, ReadError>,
) {
    match (lowered, todays) {
        (Ok(a), Ok(b)) => assert_eq!(a, b, "{what}: the lowered answer must be today's"),
        (
            Err(ReadError::MetricPipelineError { error_type: a, .. }),
            Err(ReadError::MetricPipelineError { error_type: b, .. }),
        ) => assert_eq!(a, b, "{what}: the same pipeline error"),
        (a, b) => panic!("{what}: lowered {a:?}, today's {b:?}"),
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CommentedRow624 {
    log_comment: String,
    query: String,
    exception_code: i32,
    read_rows: u64,
    result_rows: u64,
}

/// Every finished or failed statement over `log_samples` under each of
/// `comments`. `SYSTEM FLUSH LOGS` does not guarantee the rows are there, so
/// this waits until every comment has one.
async fn logged_624(
    admin: &ChClient,
    comments: &[String],
) -> std::collections::HashMap<String, Vec<CommentedRow624>> {
    let list = comments
        .iter()
        .map(|c| format!("'{c}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut out = std::collections::HashMap::new();
    for _ in 0..30 {
        admin
            .execute(
                "SYSTEM FLUSH LOGS",
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("flush logs");
        let sql = format!(
            "SELECT log_comment, query, exception_code, read_rows, result_rows \
             FROM system.query_log WHERE log_comment IN ({list}) AND type != 'QueryStart' \
             AND query LIKE '%log_samples%' AND query NOT LIKE '%system.query_log%' \
             ORDER BY event_time_microseconds ASC"
        );
        let mut stream = admin
            .query_stream::<CommentedRow624>(&sql, &QuerySettings::new())
            .await
            .expect("read system.query_log");
        out = std::collections::HashMap::new();
        while let Some(row) = stream.next().await {
            let row = row.expect("decode a query_log row");
            out.entry(row.log_comment.clone())
                .or_insert_with(Vec::new)
                .push(row);
        }
        drop(stream);
        if comments.iter().all(|c| out.contains_key(c)) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    out
}

/// A statement of part 1 (either one): it carries the grid column.
fn is_lowered_624(query: &str) -> bool {
    query.contains("AS bucket_ns")
}

/// Today's route's raw read.
fn is_todays_624(query: &str) -> bool {
    query.starts_with("SELECT fingerprint, timestamp_ns, body")
}

/// Runs SQL against the admin client.
async fn exec_624(admin: &ChClient, sql: &str) {
    admin
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

/// Creates `db` fresh, with the schema, and returns it.
async fn fresh_db_624(db: String) -> (ChClient, String) {
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    for sql in [
        format!("DROP DATABASE IF EXISTS {db}"),
        format!("CREATE DATABASE {db}"),
    ] {
        admin
            .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .expect("create the run database");
    }
    run_init(&admin, &test_ctx(&db)).await.expect("run_init");
    (admin, db)
}

async fn drop_db_624(admin: &ChClient, db: &str) {
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

/// Lands one stream through the landing table.
async fn land_stream_624(admin: &ChClient, db: &str, t: i64, fp: u64, service: &str, labels: &str) {
    exec_624(
        admin,
        &format!(
            "INSERT INTO {db}.log_landing (received_ms, kind, service, fingerprint, timestamp_ns, \
             severity, body, structured_metadata, month, labels, updated_ns, pattern, \
             pattern_count) SELECT toUnixTimestamp64Milli(now64(3)), 1, '{service}', \
             toUInt128({fp}), 0, 0, '', '', \
             toStartOfMonth(toDate(fromUnixTimestamp64Nano(toInt64({t})))), '{labels}', 0, '', 0"
        ),
    )
    .await;
}

/// Lands one line through the landing table.
async fn land_line_624(
    admin: &ChClient,
    db: &str,
    fp: u64,
    service: &str,
    ts: i64,
    body: &str,
    sm: &str,
) {
    let body = body.replace('\\', "\\\\").replace('\'', "\\'");
    exec_624(
        admin,
        &format!(
            "INSERT INTO {db}.log_landing (received_ms, kind, service, fingerprint, timestamp_ns, \
             severity, body, structured_metadata, month, labels, updated_ns, pattern, \
             pattern_count) SELECT toUnixTimestamp64Milli(now64(3)), 0, '{service}', \
             toUInt128({fp}), toInt64({ts}), 0, '{body}', '{sm}', toDate(0), '', 0, '', 0"
        ),
    )
    .await;
}

/// The bucketed fixture plus what the label stages read: metadata carrying
/// the names the stages test (`x`, `n`, `d`, `b`, `addr`), a stream whose
/// bodies carry colour codes, and a row whose metadata values do not
/// convert. Every added row sits inside a window at `[30s]`, `[60s]` and
/// `[150s]` on the 60 s grid from `T`.
async fn seed_staged_corpus() -> (ChClient, String, i64) {
    let (admin, db, t) = seed_bucketed_corpus().await;
    let svc = "c507bucket";
    land_stream_624(
        &admin,
        &db,
        t,
        333,
        svc,
        r#"{"app":"c","service_name":"c507bucket"}"#,
    )
    .await;
    let good = r#"{"addr":"10.1.2.3","b":"2KB","d":"2s","n":"7","x":"1"}"#;
    let other = r#"{"addr":"192.168.0.1","b":"1KB","d":"500ms","n":"3","x":"2"}"#;
    let sec = 1_000_000_000i64;
    land_line_624(
        &admin,
        &db,
        333,
        svc,
        t + 50 * sec,
        "\u{1b}[31mred\u{1b}[0m",
        good,
    )
    .await;
    land_line_624(
        &admin,
        &db,
        333,
        svc,
        t + 110 * sec,
        "\u{1b}[32mgreen\u{1b}[0m",
        good,
    )
    .await;
    land_line_624(&admin, &db, 111, svc, t + 55 * sec, "plain", other).await;
    land_line_624(&admin, &db, 111, svc, t + 170 * sec, "plain", good).await;
    land_line_624(
        &admin,
        &db,
        222,
        svc,
        t + 115 * sec,
        "malformed",
        r#"{"b":"bad","d":"bad","n":"bad"}"#,
    )
    .await;
    (admin, db, t)
}

/// The pipelines `label_only_pipeline` admits that this part stages: the
/// planner's list, less the equality that lowers into the statement itself.
const STAGED_PIPELINES_624: &[&str] = &[
    "| drop x",
    r#"| drop x="1""#,
    r#"| drop x=~"1.*""#,
    "| keep a",
    r#"| keep a, x="1""#,
    r#"| x=~"y.+""#,
    r#"| x!~"y.+""#,
    "| n > 5",
    "| n >= 5.5",
    "| d > 1s",
    "| b > 1KB",
    r#"| addr = ip("10.0.0.0/8")"#,
    r#"| x=~"1" and n > 5"#,
    r#"| x=~"1" or n > 5"#,
    "| decolorize",
    "| drop __error__",
    r#"|= "tok" | drop x"#,
];

/// **T3 (issue #624, part 2): every staged query answers what today's route
/// answers, bit for bit, at a range equal to, above and below the step, bare
/// and under `sum by (lvl)` and `sum`.** The query log shows the lowered
/// statement on one run and today's raw read on the other.
#[tokio::test]
async fn label_stages_answer_as_todays_route() {
    skip_unless_live!();
    let (admin, db, t) = seed_staged_corpus().await;
    let sel = r#"{service_name="c507bucket"}"#;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 240_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mut queries: Vec<String> = Vec::new();
    for range in ["60s", "150s", "30s"] {
        let mut inner: Vec<String> = STAGED_PIPELINES_624
            .iter()
            .map(|p| format!("count_over_time({sel} {p} [{range}])"))
            .collect();
        inner.push(format!("rate({sel} | decolorize [{range}])"));
        inner.push(format!("rate({sel} | drop lvl [{range}])"));
        inner.push(format!("bytes_over_time({sel} | drop lvl [{range}])"));
        inner.push(format!("absent_over_time({sel} [{range}])"));
        inner.push(format!(r#"absent_over_time({sel} | x="y" [{range}])"#));
        inner.push(format!(r#"absent_over_time({sel} | x=~"zzz" [{range}])"#));
        for q in inner {
            queries.push(format!("sum by (lvl) ({q})"));
            queries.push(format!("sum({q})"));
            queries.push(q);
        }
    }

    let mut comments: Vec<(String, bool)> = Vec::new();
    for query in &queries {
        let lowered_comment = format!("c624t3-{}", uuid::Uuid::new_v4().simple());
        let todays_comment = format!("c624t3-{}", uuid::Uuid::new_v4().simple());
        let lowered = run_624(
            &db,
            hooks_624(false, None, None),
            Some(&lowered_comment),
            query,
            &params,
        )
        .await;
        let todays = run_624(
            &db,
            hooks_624(true, None, None),
            Some(&todays_comment),
            query,
            &params,
        )
        .await;
        same_outcome_624(query, &lowered, &todays);
        comments.push((lowered_comment, true));
        comments.push((todays_comment, false));
    }
    let all: Vec<String> = comments.iter().map(|(c, _)| c.clone()).collect();
    let logged = logged_624(&admin, &all).await;
    for (comment, lowered) in &comments {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{comment}: no statement logged"));
        if *lowered {
            assert!(
                rows.iter().any(|r| is_lowered_624(&r.query)),
                "{comment}: {rows:#?}"
            );
            assert!(
                !rows.iter().any(|r| is_todays_624(&r.query)),
                "{comment}: {rows:#?}"
            );
        } else {
            assert!(
                rows.iter().any(|r| is_todays_624(&r.query)),
                "{comment}: {rows:#?}"
            );
        }
    }

    // Non-vacuous: a stage drops a group, `drop` merges two fingerprints,
    // and a filter is the only reason an absent point is 1.
    let total = |a: &[AnswerSeries]| -> f64 {
        a.iter()
            .flat_map(|(_, p)| p.iter().map(|(_, v)| f64::from_bits(*v)))
            .sum()
    };
    let plain = run_624(
        &db,
        hooks_624(false, None, None),
        None,
        &format!("count_over_time({sel} [60s])"),
        &params,
    )
    .await
    .expect("answers");
    let filtered = run_624(
        &db,
        hooks_624(false, None, None),
        None,
        &format!(r#"count_over_time({sel} | x=~"1" [60s])"#),
        &params,
    )
    .await
    .expect("answers");
    assert!(total(&filtered) < total(&plain), "a stage drops a group");
    let dropped = run_624(
        &db,
        hooks_624(false, None, None),
        None,
        &format!("count_over_time({sel} | drop lvl [60s])"),
        &params,
    )
    .await
    .expect("answers");
    assert!(
        dropped.len() < plain.len(),
        "drop merges two fingerprints: {dropped:?}"
    );
    let absent_all = run_624(
        &db,
        hooks_624(false, None, None),
        None,
        &format!("absent_over_time({sel} [60s])"),
        &params,
    )
    .await
    .expect("answers");
    let absent_filtered = run_624(
        &db,
        hooks_624(false, None, None),
        None,
        &format!(r#"absent_over_time({sel} | x=~"zzz" [60s])"#),
        &params,
    )
    .await
    .expect("answers");
    let points = |a: &[AnswerSeries]| a.iter().map(|(_, p)| p.len()).sum::<usize>();
    assert!(
        points(&absent_filtered) > points(&absent_all),
        "a filter makes an absent point 1: {absent_all:?} / {absent_filtered:?}"
    );

    drop_db_624(&admin, &db).await;
}

/// **T4 (issue #624, part 2): the logs-volume query is counted in the
/// database** by #507's statement, and answers what today's route answers.
#[tokio::test]
async fn the_logs_volume_query_is_counted_in_the_database() {
    skip_unless_live!();
    let (admin, db, t) = seed_staged_corpus().await;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 240_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let query = r#"sum by (level, detected_level) (count_over_time({service_name="c507bucket"} | drop __error__ [60s]))"#;
    let comment = format!("c624t4-{}", uuid::Uuid::new_v4().simple());
    let lowered = run_624(
        &db,
        hooks_624(false, None, None),
        Some(&comment),
        query,
        &params,
    )
    .await;
    let todays = run_624(&db, hooks_624(true, None, None), None, query, &params).await;
    same_outcome_624(query, &lowered, &todays);
    assert!(
        lowered.as_ref().is_ok_and(|a| !a.is_empty()),
        "an answer: {lowered:?}"
    );
    let logged = logged_624(&admin, std::slice::from_ref(&comment)).await;
    let rows = &logged[&comment];
    let statement = rows
        .iter()
        .find(|r| is_lowered_624(&r.query))
        .unwrap_or_else(|| panic!("the lowered statement: {rows:#?}"));
    assert!(
        !statement.query.contains("leadInFrame"),
        "range equal to step reads the equal-range statement: {}",
        statement.query
    );
    assert!(
        statement.result_rows < statement.read_rows,
        "fewer rows sent than lines scanned: {statement:?}"
    );
    drop_db_624(&admin, &db).await;
}

/// **T6 (issue #624, part 2): a staged statement that fails for memory is
/// answered by today's route**, with the carried aggregation.
#[tokio::test]
async fn a_staged_query_out_of_memory_answers_on_todays_route() {
    skip_unless_live!();
    let (admin, db, t) = seed_staged_corpus().await;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 240_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let query = r#"count_over_time({service_name="c507bucket"} | drop x [150s])"#;
    let comment = format!("c624t6-{}", uuid::Uuid::new_v4().simple());
    let lowered = run_624(
        &db,
        hooks_624(false, None, Some(1)),
        Some(&comment),
        query,
        &params,
    )
    .await;
    let todays = run_624(&db, hooks_624(true, None, None), None, query, &params).await;
    same_outcome_624(query, &lowered, &todays);
    assert!(
        lowered.as_ref().is_ok_and(|a| !a.is_empty()),
        "an answer: {lowered:?}"
    );
    let logged = logged_624(&admin, std::slice::from_ref(&comment)).await;
    let rows = &logged[&comment];
    assert!(
        rows.iter()
            .any(|r| is_lowered_624(&r.query) && r.exception_code == 241),
        "the lowered statement failed for memory: {rows:#?}"
    );
    assert!(
        rows.iter().any(|r| is_todays_624(&r.query)),
        "then today's route: {rows:#?}"
    );
    drop_db_624(&admin, &db).await;
}

/// **`absent_over_time` answers on today's route** over a selector that
/// resolves streams (issue #624, part 2). A line at `T+30s` on one stream and
/// at `T+150s` on a second that carries one more label — so its rows take
/// the label route, not the per-stream slider; the answer is 1 at every grid
/// point whose window holds no line, under the selector's equality labels,
/// and no point where a line falls in the window.
#[tokio::test]
async fn absent_over_time_answers_on_todays_route() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_absent_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 4 * 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    land_stream_624(
        &admin,
        &db,
        t,
        7_400_000,
        "c624abs",
        r#"{"service_name":"c624abs"}"#,
    )
    .await;
    land_line_624(&admin, &db, 7_400_000, "c624abs", t + 30 * sec, "x", "").await;
    land_stream_624(
        &admin,
        &db,
        t,
        7_400_001,
        "c624abs",
        r#"{"pod":"p","service_name":"c624abs"}"#,
    )
    .await;
    land_line_624(&admin, &db, 7_400_001, "c624abs", t + 150 * sec, "y", "").await;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 240 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let labels = vec![("service_name".to_string(), "c624abs".to_string())];
    for (range, empty) in [("1m", vec![0i64, 120, 240]), ("2m", vec![0])] {
        let query = format!(r#"absent_over_time({{service_name="c624abs"}}[{range}])"#);
        let got = run_624(&db, hooks_624(true, None, None), None, &query, &params).await;
        let want: Vec<AnswerSeries> = vec![(
            labels.clone(),
            empty
                .iter()
                .map(|s| (t + s * sec, 1.0f64.to_bits()))
                .collect(),
        )];
        assert_eq!(got.as_ref().ok(), Some(&want), "{query}: {got:?}");
    }
    drop_db_624(&admin, &db).await;
}

/// The cap corpus of T5 and T10: `main` streams of service `c624cap`,
/// labels `{app: a0|a1, pod}`, one line a second from `T+1s` to `T+600s`.
async fn seed_cap_streams_624(admin: &ChClient, db: &str, t: i64, main: u64) {
    exec_624(
        admin,
        &format!(
            "INSERT INTO {db}.log_landing (received_ms, kind, service, fingerprint, timestamp_ns, \
             severity, body, structured_metadata, month, labels, updated_ns, pattern, \
             pattern_count) SELECT toUnixTimestamp64Milli(now64(3)), 1, 'c624cap', \
             toUInt128(7000000 + number), 0, 0, '', '', \
             toStartOfMonth(toDate(fromUnixTimestamp64Nano(toInt64({t})))), \
             concat('{{\"app\":\"a', toString(number % 2), '\",\"pod\":\"p', toString(number), \
             '\",\"service_name\":\"c624cap\"}}'), 0, '', 0 FROM numbers({main})"
        ),
    )
    .await;
    exec_624(
        admin,
        &format!(
            "INSERT INTO {db}.log_landing (received_ms, kind, service, fingerprint, timestamp_ns, \
             severity, body, structured_metadata, month, labels, updated_ns, pattern, \
             pattern_count) SELECT toUnixTimestamp64Milli(now64(3)), 0, 'c624cap', \
             toUInt128(7000000 + number % {main}), \
             toInt64({t}) + toInt64(intDiv(number, {main}) + 1) * 1000000000, 0, 'x', '', \
             toDate(0), '', 0, '', 0 FROM numbers({main} * 600)"
        ),
    )
    .await;
}

/// The smallest cap at which `query` answers, by binary search over
/// `[1, hi]`; `hi` must answer.
async fn min_cap_624(
    db: &str,
    todays: bool,
    cap: Cap624,
    query: &str,
    params: &QueryParams,
    hi: u64,
) -> u64 {
    assert!(
        run_624(
            db,
            hooks_624(todays, Some((cap, hi)), None),
            None,
            query,
            params
        )
        .await
        .is_ok(),
        "{query}: must answer at {hi}"
    );
    let (mut lo, mut hi) = (1u64, hi);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match run_624(
            db,
            hooks_624(todays, Some((cap, mid)), None),
            None,
            query,
            params,
        )
        .await
        {
            Ok(_) => hi = mid,
            Err(ReadError::QueryTooBroad(_)) => lo = mid + 1,
            Err(e) => panic!("{query} at {cap:?} {mid}: {e}"),
        }
    }
    lo
}

fn cap_refusal_624(cap: Cap624, e: &ReadError) -> bool {
    match cap {
        Cap624::GroupBytes => matches!(
            e,
            ReadError::QueryTooBroad(
                pulsus_read::logql::TooBroadReason::MetricGroupLabelBytes { .. }
            )
        ),
        Cap624::ResultPoints => matches!(
            e,
            ReadError::QueryTooBroad(pulsus_read::logql::TooBroadReason::MetricResultPoints { .. })
        ),
    }
}

/// **T5 (issue #624, part 2): a reducing aggregation answers wherever
/// today's route answers.** For a fingerprint-route and a label-route query,
/// at a range equal to and above the step, the smallest cap today's route
/// answers at is the lowered route's: it answers there with today's matrix
/// and refuses one below. And streams whose lines lie only in the gaps
/// between windows reserve no grid on either route (today's route since
/// part 6).
#[tokio::test]
async fn a_reducing_aggregation_answers_wherever_todays_route_answers() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_t5_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 4 * 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    seed_cap_streams_624(&admin, &db, t, 200).await;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 600_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let sel = r#"{service_name="c624cap"}"#;
    for range in ["60s", "150s"] {
        for (query, points) in [
            (
                format!(r#"sum(count_over_time({sel} | pod=~".+" [{range}]))"#),
                2_200u64,
            ),
            (
                format!("sum(count_over_time({sel} | drop pod [{range}]))"),
                22,
            ),
        ] {
            for (cap, hi) in [
                (Cap624::ResultPoints, 100_000u64),
                (Cap624::GroupBytes, 256 * 1024 * 1024),
            ] {
                let today = min_cap_624(&db, true, cap, &query, &params, hi).await;
                if cap == Cap624::ResultPoints {
                    assert_eq!(today, points, "{query}: today's route's smallest point cap");
                }
                let want = run_624(
                    &db,
                    hooks_624(true, Some((cap, today)), None),
                    None,
                    &query,
                    &params,
                )
                .await
                .expect("today's route answers at its smallest cap");
                let got = run_624(
                    &db,
                    hooks_624(false, Some((cap, today)), None),
                    None,
                    &query,
                    &params,
                )
                .await;
                assert_eq!(
                    got.as_ref().ok(),
                    Some(&want),
                    "{query} at {cap:?} {today}: {got:?}"
                );
                let below = run_624(
                    &db,
                    hooks_624(false, Some((cap, today - 1)), None),
                    None,
                    &query,
                    &params,
                )
                .await;
                assert!(
                    below.as_ref().is_err_and(|e| cap_refusal_624(cap, e)),
                    "{query} at {cap:?} {}: the lowered route refuses one below, got {below:?}",
                    today - 1
                );
            }
        }
    }
    drop_db_624(&admin, &db).await;

    // The gap case: 100 more pods whose only lines lie in `(g, g + 30s]`.
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_t5gap_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    seed_cap_streams_624(&admin, &db, t, 200).await;
    exec_624(
        &admin,
        &format!(
            "INSERT INTO {db}.log_landing (received_ms, kind, service, fingerprint, timestamp_ns, \
             severity, body, structured_metadata, month, labels, updated_ns, pattern, \
             pattern_count) SELECT toUnixTimestamp64Milli(now64(3)), 1, 'c624cap', \
             toUInt128(7100000 + number), 0, 0, '', '', \
             toStartOfMonth(toDate(fromUnixTimestamp64Nano(toInt64({t})))), \
             concat('{{\"app\":\"a', toString(number % 2), '\",\"pod\":\"g', toString(number), \
             '\",\"service_name\":\"c624cap\"}}'), 0, '', 0 FROM numbers(100)"
        ),
    )
    .await;
    exec_624(
        &admin,
        &format!(
            "INSERT INTO {db}.log_landing (received_ms, kind, service, fingerprint, timestamp_ns, \
             severity, body, structured_metadata, month, labels, updated_ns, pattern, \
             pattern_count) SELECT toUnixTimestamp64Milli(now64(3)), 0, 'c624cap', \
             toUInt128(7100000 + number % 100), \
             toInt64({t}) + toInt64(intDiv(number, 100)) * 60000000000 + 15000000000, 0, 'x', '', \
             toDate(0), '', 0, '', 0 FROM numbers(100 * 10)"
        ),
    )
    .await;
    let query = format!(r#"sum(count_over_time({sel} | pod=~".+" [30s]))"#);
    let cap = Cap624::ResultPoints;
    let today = min_cap_624(&db, true, cap, &query, &params, 100_000).await;
    assert_eq!(
        today, 2_200,
        "today's route reserves no grid for a fingerprint whose lines are all in gaps (#624, part 6)"
    );
    let want = run_624(
        &db,
        hooks_624(true, Some((cap, today)), None),
        None,
        &query,
        &params,
    )
    .await
    .expect("today's route answers at its smallest cap");
    let got = run_624(
        &db,
        hooks_624(false, Some((cap, today)), None),
        None,
        &query,
        &params,
    )
    .await;
    assert_eq!(got.as_ref().ok(), Some(&want), "{query} at {today}");
    drop_db_624(&admin, &db).await;
}

/// **T10 (issue #624, part 2): a selected fingerprint that returns no group
/// charges nothing.** Stream X is selected and hydrated, and its lines lie
/// (i) only in the gaps between windows, (ii) only in the tail after the last
/// grid point, or (iii) nowhere in the scan. The lowered route's smallest
/// answering cap with X selected equals its smallest without X, and at
/// today's route's smallest cap with X selected the lowered route answers
/// with today's matrix.
#[tokio::test]
async fn fingerprints_that_return_no_group_charge_nothing() {
    skip_unless_live!();
    let t = ((now_ns() - 4 * 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    for placement in ["gaps", "tail", "none"] {
        let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
            "pulsus_read_it_qlg_t10_{}",
            uuid::Uuid::new_v4().simple()
        )))
        .await;
        seed_cap_streams_624(&admin, &db, t, 200).await;
        land_stream_624(
            &admin,
            &db,
            t,
            7_200_000,
            "c624cap",
            r#"{"app":"solo","extra":"1","pod":"px","service_name":"c624cap"}"#,
        )
        .await;
        let (range, end, lines): (&str, i64, Vec<i64>) = match placement {
            "gaps" => (
                "30s",
                t + 600 * sec,
                (0..10).map(|k| t + k * 60 * sec + 15 * sec).collect(),
            ),
            "tail" => (
                "60s",
                t + 630 * sec,
                vec![t + 610 * sec, t + 620 * sec, t + 630 * sec],
            ),
            _ => ("60s", t + 600 * sec, vec![t - 86_400 * sec]),
        };
        for ts in lines {
            land_line_624(&admin, &db, 7_200_000, "c624cap", ts, "x", "").await;
        }
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: t,
                end_ns: end,
                step_ns: 60_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        // (query with X selected, without it, the point minimum without X)
        for (with_x, without_x, points) in [
            (
                format!(
                    r#"sum(count_over_time({{service_name="c624cap"}} | pod=~".+" [{range}]))"#
                ),
                format!(
                    r#"sum(count_over_time({{service_name="c624cap", extra!="1"}} | pod=~".+" [{range}]))"#
                ),
                2_200u64,
            ),
            (
                format!(
                    r#"sum(count_over_time({{service_name="c624cap"}} | drop pod, extra [{range}]))"#
                ),
                format!(
                    r#"sum(count_over_time({{service_name="c624cap", extra!="1"}} | drop pod, extra [{range}]))"#
                ),
                22,
            ),
        ] {
            for (cap, hi) in [
                (Cap624::GroupBytes, 256 * 1024 * 1024u64),
                (Cap624::ResultPoints, 100_000),
            ] {
                let lowered_with = min_cap_624(&db, false, cap, &with_x, &params, hi).await;
                let lowered_without = min_cap_624(&db, false, cap, &without_x, &params, hi).await;
                if cap == Cap624::ResultPoints {
                    assert_eq!(
                        lowered_without, points,
                        "{placement}: {without_x}: one grid per series"
                    );
                }
                assert_eq!(
                    lowered_with, lowered_without,
                    "{placement}: {with_x} at {cap:?}: X charges nothing"
                );
                let today = min_cap_624(&db, true, cap, &with_x, &params, hi).await;
                let want = run_624(
                    &db,
                    hooks_624(true, Some((cap, today)), None),
                    None,
                    &with_x,
                    &params,
                )
                .await
                .expect("today's route answers at its smallest cap");
                let got = run_624(
                    &db,
                    hooks_624(false, Some((cap, today)), None),
                    None,
                    &with_x,
                    &params,
                )
                .await;
                assert_eq!(
                    got.as_ref().ok(),
                    Some(&want),
                    "{placement}: {with_x} at {cap:?} {today}"
                );
            }
        }
        drop_db_624(&admin, &db).await;
    }
}

/// **T9 (issue #624, part 2): a row no window holds neither fails the query
/// nor counts, as in the reference.** Stream A carries `n="7"`, a line every
/// 10 s from `T-60s` to `T+270s`; stream B carries `n="bad"`, one line.
/// Part 6 adds today's route: forced, and reached by a query that does not
/// lower (`| label_format`).
#[tokio::test]
async fn a_row_no_window_holds_neither_fails_nor_counts() {
    skip_unless_live!();
    let t = ((now_ns() - 4 * 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    let query =
        |range: &str| format!(r#"count_over_time({{service_name="c624gap"}} | n > 5 [{range}])"#);
    let a_labels = vec![
        ("n".to_string(), "7".to_string()),
        ("service_name".to_string(), "c624gap".to_string()),
    ];
    // (B's line, range, end, the answer A gives at each grid point, or the
    // error case).
    for (b_at, range, end, per_point) in [
        (t + 75 * sec, "30s", t + 240 * sec, Some(3.0f64)),
        (t + 250 * sec, "60s", t + 270 * sec, Some(6.0)),
        (t + 75 * sec, "60s", t + 240 * sec, None),
    ] {
        let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
            "pulsus_read_it_qlg_t9_{}",
            uuid::Uuid::new_v4().simple()
        )))
        .await;
        land_stream_624(
            &admin,
            &db,
            t,
            7_300_000,
            "c624gap",
            r#"{"n":"7","service_name":"c624gap"}"#,
        )
        .await;
        land_stream_624(
            &admin,
            &db,
            t,
            7_300_001,
            "c624gap",
            r#"{"n":"bad","service_name":"c624gap"}"#,
        )
        .await;
        for k in -6..=27 {
            land_line_624(&admin, &db, 7_300_000, "c624gap", t + k * 10 * sec, "a", "").await;
        }
        land_line_624(&admin, &db, 7_300_001, "c624gap", b_at, "b", "").await;
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: t,
                end_ns: end,
                step_ns: 60_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let comment = format!("c624t9-{}", uuid::Uuid::new_v4().simple());
        let got = run_624(
            &db,
            hooks_624(false, None, None),
            Some(&comment),
            &query(range),
            &params,
        )
        .await;
        match per_point {
            Some(v) => {
                let want: Vec<AnswerSeries> = vec![(
                    a_labels.clone(),
                    (0..5).map(|k| (t + k * 60 * sec, v.to_bits())).collect(),
                )];
                assert_eq!(
                    got.as_ref().ok(),
                    Some(&want),
                    "{range} with B at {b_at}: {got:?}"
                );
                // Issue #624, part 6: today's route answers the same, whether
                // forced or reached by a query that does not lower.
                let todays = run_624(
                    &db,
                    hooks_624(true, None, None),
                    None,
                    &query(range),
                    &params,
                )
                .await;
                assert_eq!(
                    todays.as_ref().ok(),
                    Some(&want),
                    "today's route {range}: {todays:?}"
                );
                let lf = format!(
                    r#"count_over_time({{service_name="c624gap"}} | n > 5 | label_format m="x" [{range}])"#
                );
                let lf_comment = format!("c624p6-{}", uuid::Uuid::new_v4().simple());
                let lf_got = run_624(
                    &db,
                    hooks_624(false, None, None),
                    Some(&lf_comment),
                    &lf,
                    &params,
                )
                .await;
                let mut lf_labels = a_labels.clone();
                lf_labels.push(("m".to_string(), "x".to_string()));
                lf_labels.sort();
                let lf_want: Vec<AnswerSeries> = vec![(
                    lf_labels,
                    (0..5).map(|k| (t + k * 60 * sec, v.to_bits())).collect(),
                )];
                assert_eq!(
                    lf_got.as_ref().ok(),
                    Some(&lf_want),
                    "label_format {range}: {lf_got:?}"
                );
                let lf_logged = logged_624(&admin, std::slice::from_ref(&lf_comment)).await;
                assert!(
                    lf_logged[&lf_comment]
                        .iter()
                        .all(|r| !is_lowered_624(&r.query))
                        && lf_logged[&lf_comment]
                            .iter()
                            .any(|r| is_todays_624(&r.query)),
                    "label_format stays on today's route: {:#?}",
                    lf_logged[&lf_comment]
                );
            }
            None => {
                let todays = run_624(
                    &db,
                    hooks_624(true, None, None),
                    None,
                    &query(range),
                    &params,
                )
                .await;
                assert!(
                    matches!(got, Err(ReadError::MetricPipelineError { .. })),
                    "B inside a window fails the query: {got:?}"
                );
                same_outcome_624("the in-window control", &got, &todays);
            }
        }
        let logged = logged_624(&admin, std::slice::from_ref(&comment)).await;
        assert!(
            logged[&comment].iter().any(|r| is_lowered_624(&r.query)),
            "{range}: the lowered statement ran: {:#?}",
            logged[&comment]
        );
        drop_db_624(&admin, &db).await;
    }
}

// ---------------------------------------------------------------------
// History (issue #507, the group key read retired by issue #624, part 4b):
// does a single-threaded `sum` accumulate in scan order?
// ---------------------------------------------------------------------

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct UnwrapSeedRow {
    /// The tenant (issue #653): the empty tenant's rows.
    org_id: String,
    service: String,
    fingerprint: u128,
    timestamp_ns: i64,
    severity: i8,
    body: String,
    structured_metadata: String,
}

/// One `log_landing` kind-0 row, in the table's own column order.
///
/// **The part-layout case seeds the landing table block by block** (issue
/// #603), rather than through [`land_seeded_lines`]: that helper stages the
/// whole of `log_samples`, empties it and lands it as one block, which is
/// exactly the layout the case is built to control. One landing insert per
/// chunk gives `log_samples` one part per chunk, through the view.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct UnwrapLandingRow {
    /// The tenant (issue #653): the empty tenant's rows.
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
}

fn unwrap_landing_rows(received_ms: i64, rows: &[UnwrapSeedRow]) -> Vec<UnwrapLandingRow> {
    rows.iter()
        .map(|r| UnwrapLandingRow {
            org_id: String::new(),
            received_ms,
            kind: 0,
            service: r.service.clone(),
            fingerprint: r.fingerprint,
            timestamp_ns: r.timestamp_ns,
            severity: r.severity,
            body: r.body.clone(),
            structured_metadata: r.structured_metadata.clone(),
            month: 0,
            labels: String::new(),
            updated_ns: 0,
            pattern: String::new(),
            pattern_count: 0,
        })
        .collect()
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SumRow {
    s: f64,
    /// XOR of every extracted value's bit pattern — order-independent, so
    /// it compares the VALUE SET and cannot be confused with a summation
    /// order difference.
    x: u64,
    n: u64,
}

/// **The claim the group key read's exact comparison rested on, measured
/// — and it is false** (issue #507 §6).
///
/// The claim was: at `max_threads = 1` the database's `sum` accumulates a
/// single fingerprint's rows in `(service, fingerprint, timestamp_ns)`
/// order, which within one fingerprint is timestamp order, so the two
/// summations are the same summation and agree bit for bit.
///
/// **They do not agree, at any of the three sizes**, measured on
/// server 26.3 (26.3.29.7):
///
/// ```text
///  N        ours                 the database         ULPs   |diff|      bound
///  1e3      0xc16bd86f1537bba1   0xc16bd86f1537bba2      1   1.86e-9   3.29e-5
///  1e5      0x41b936b66d98bf1a   0x41b936b66d98bf12      8   4.77e-7   2.90e-1
///  1e6      0x41ac0f9d50e2fd32   0x41ac0f9d50e2fd51     31   9.24e-7   2.86e+1
/// ```
///
/// **The cause is the block, not the thread.** At `max_threads = 1` and
/// `max_block_size = 1` the database reproduces the left-to-right sum
/// exactly; every larger block size differs, and not monotonically —
/// measured over 100 000 of these values against a strictly sequential
/// `arrayFold` in the database itself:
///
/// ```text
///  left to right (arrayFold)  C193FE7B56D2A381
///  max_block_size = 1         C193FE7B56D2A381   equal
///  max_block_size = 64        C193FE7B56D2A394
///  max_block_size = 1024      C193FE7B56D2A3A9
///  max_block_size = 8192      C193FE7B56D2A3AE
///  max_block_size = 65505     C193FE7B56D2A193
/// ```
///
/// A block size of one is not a setting a read path can carry, so pinning
/// the thread count does not recover a bit-exact comparison.
///
/// **What this test therefore asserts** is the property that does hold and
/// that a reader needs: the two answers differ by at most
/// `2(n−1)·u·Σ|vᵢ|`, the bound any two evaluation orders of the same `n`
/// values obey. It also rules out the one confound that would make the
/// sums incomparable — a value set that is not the same on both sides —
/// before reading anything from them.
///
/// The values are generated once, in Rust, and written with `{:?}` —
/// Rust's shortest round-tripping float form — so the bytes the database
/// parses decode back to exactly the `f64` this test summed.
#[tokio::test]
async fn the_database_sum_is_not_the_evaluators_order_but_stays_inside_the_bound() {
    skip_unless_live!();
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    sweep_leftovers(
        &admin,
        &pulsus_testkit::test_db("pulsus_read_it_qlg_w4sum_"),
    )
    .await;
    let db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_w4sum_{}",
        uuid::Uuid::new_v4().simple()
    ));
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop");
    admin
        .execute(
            &format!("CREATE DATABASE {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create");
    run_init(&admin, &test_ctx(&db)).await.expect("run_init");
    let client = data_client(&db).await;

    let mut results: Vec<(u64, f64, f64, f64, f64)> = Vec::new();
    for (n, fp) in [(1_000u64, 901u64), (100_000, 902), (1_000_000, 903)] {
        // Mixed magnitude and sign, deterministic (the splitmix64 pattern).
        let values: Vec<f64> = (0..n)
            .map(|i| {
                let r = splitmix64(i);
                let mag = 10f64.powi(((r >> 40) % 13) as i32 - 6);
                let frac = ((r & 0xFFFF_FFFF) as f64) / (u32::MAX as f64);
                let sign = if r & 1 == 0 { 1.0 } else { -1.0 };
                sign * mag * (1.0 + frac)
            })
            .collect();
        // Recent, not a fixed epoch: `log_samples` carries a TTL on
        // `timestamp_ns` with `ttl_only_drop_parts`, so a part written at a
        // date older than the retention window is dropped on arrival and
        // the query answers `0` with no error. Measured: a 2023 timestamp
        // inserted, reported success, and left zero rows.
        let t0 = now_ns() - 3_600_000_000_000;
        let rows: Vec<UnwrapSeedRow> = values
            .iter()
            .enumerate()
            .map(|(i, v)| UnwrapSeedRow {
                org_id: String::new(),
                service: "w4".to_string(),
                fingerprint: u128::from(fp),
                timestamp_ns: t0 + i as i64,
                severity: 0,
                body: format!(r#"{{"v":{v:?}}}"#),
                structured_metadata: String::new(),
            })
            .collect();
        client
            .insert_block("log_samples", &rows)
            .await
            .expect("insert");
        land_seeded_lines(&client, &db).await;

        // Left to right, in timestamp order — the evaluator's accumulation.
        let mut ours = 0.0f64;
        for v in &values {
            ours += *v;
        }

        let sql = format!(
            "SELECT sum(JSONExtractFloat(body, 'v')) AS s, \
             groupBitXor(reinterpretAsUInt64(JSONExtractFloat(body, 'v'))) AS x, \
             count() AS n \
             FROM {db}.log_samples WHERE service = 'w4' AND fingerprint = {fp}"
        );
        let settings = QuerySettings::new().set("max_threads", 1);
        let mut stream = client
            .query_stream::<SumRow>(&sql, &settings)
            .await
            .expect("execute");
        let row = stream.next().await.expect("one row").expect("decode");
        drop(stream);

        // The confound, ruled out first: if the database decoded even one
        // value differently from the bytes we wrote, the sums would differ
        // for a reason that has nothing to do with order. The XOR of the
        // bit patterns is order-independent, so it compares the value SET.
        let our_xor = values.iter().fold(0u64, |a, v| a ^ v.to_bits());
        assert_eq!(row.n, n, "N={n}: every row must be present");
        assert_eq!(
            row.x, our_xor,
            "N={n}: the database decoded a different value set, so nothing \
             about summation order can be read from the sums"
        );

        let theirs = row.s;
        let sum_abs: f64 = values.iter().map(|v| v.abs()).sum();
        let bound = 2.0 * ((n - 1) as f64) * (f64::EPSILON / 2.0) * sum_abs;
        let ulps = (ours.to_bits() as i64 - theirs.to_bits() as i64).abs();
        eprintln!(
            "N={n}: ours={ours:?} ({:#018x})  theirs={theirs:?} ({:#018x})  \
             ulps={ulps}  diff={:e}  bound={bound:e}  sum|v|={sum_abs:e}",
            ours.to_bits(),
            theirs.to_bits(),
            (ours - theirs).abs()
        );
        results.push((n, ours, theirs, sum_abs, bound));
    }

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop");

    for (n, ours, theirs, sum_abs, bound) in &results {
        eprintln!(
            "N={n}: agree={}  |diff|={:e}  bound={:e}  sum|v|={:e}",
            ours.to_bits() == theirs.to_bits(),
            (ours - theirs).abs(),
            bound,
            sum_abs
        );
    }
    // The property that holds, and the one a reader can rely on.
    for (n, ours, theirs, _sum_abs, bound) in &results {
        let diff = (ours - theirs).abs();
        assert!(
            diff <= *bound,
            "N={n}: two evaluation orders of the same {n} values may differ by \
             at most 2(n-1)*u*sum|v| = {bound:e}, got {diff:e}"
        );
    }
    // And the non-vacuity of the sentence above: they DO differ, so the
    // bound is doing work rather than passing on equality. If this ever
    // stops holding, the database has changed its accumulation and the
    // comparison can be tightened — which is good news and should not be
    // discovered by a silent pass.
    assert!(
        results
            .iter()
            .any(|(_, o, t, _, _)| o.to_bits() != t.to_bits()),
        "every size agreed bit for bit: re-read this test's doc comment, the \
         measurement it records has changed"
    );
}

// ---------------------------------------------------------------------
// History (issue #507, the group key read retired by issue #624, part 4b):
// the spread gate and the reproducibility gate.
// ---------------------------------------------------------------------

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct PartCountRow {
    n: u64,
}

/// One database name, for [`sweep_leftovers`].
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct DbNameRow {
    name: String,
}

/// The name prefix a leftover sweep may match, or `None` when the sweep
/// must not run.
///
/// **Why a suite sweeps at all.** A test that names its database with a
/// UUID and drops it on the last line leaves that database behind whenever
/// it fails — and a break test, which is how this suite's assertions are
/// shown to work, fails on purpose. Each such run would leak one database
/// for good.
///
/// **Why it refuses without a prefix** (issue #507, review round 5).
/// The sweep matches every database whose name starts with `stem`. With
/// `PULSUS_TEST_CH_DATABASE_PREFIX` unset, `pulsus_testkit::test_db` leaves
/// the name as written, so the stem is the same in every checkout, and one
/// checkout's sweep would drop the database another checkout's run of the
/// same test is using at that moment — a live run and a leftover look
/// identical from the server. The prefix is the one thing that makes the
/// stem this checkout's own, so without it nothing is known to be ours,
/// and the leftovers are the lesser harm.
///
/// Blank is read as unset and surrounding whitespace is trimmed, exactly as
/// `test_db` reads the same variable. With a prefix set, `stem` must begin
/// with it — a stem that does not was not composed by `test_db` under this
/// prefix, and sweeping it would reach names this checkout does not own.
///
/// `stem` must end in `_`, so that one test's stem cannot be a prefix of a
/// sibling's (`…_w4spread_` against `…_w4spread2_`).
fn leftover_stem<'a>(prefix: Option<&str>, stem: &'a str) -> Option<&'a str> {
    assert!(
        stem.ends_with('_'),
        "a sweep stem must end in `_`, or it matches a sibling test's names: {stem:?}"
    );
    let prefix = prefix.map(str::trim).filter(|p| !p.is_empty())?;
    assert!(
        stem.starts_with(&format!("{prefix}_")),
        "a sweep stem must be composed by test_db under the prefix {prefix:?}: {stem:?}"
    );
    Some(stem)
}

/// Drops the databases earlier runs of ONE test left behind, and only when
/// this checkout has a database-name prefix — see [`leftover_stem`] for why
/// both halves of that sentence are load-bearing.
///
/// `stem` is `pulsus_testkit::test_db(<the test's name stem>)`, the string
/// its database name is composed from before the UUID.
async fn sweep_leftovers(admin: &ChClient, stem: &str) {
    let prefix = std::env::var(pulsus_testkit::DATABASE_PREFIX_VAR).ok();
    let Some(stem) = leftover_stem(prefix.as_deref(), stem) else {
        eprintln!(
            "not sweeping leftovers of {stem}: {} is unset, so the stem is shared with every \
             other checkout and a sweep could drop a database another run is using",
            pulsus_testkit::DATABASE_PREFIX_VAR
        );
        return;
    };
    let mut names = admin
        .query_stream::<DbNameRow>(
            &format!("SELECT name FROM system.databases WHERE startsWith(name, '{stem}')"),
            &QuerySettings::new(),
        )
        .await
        .expect("list leftover databases");
    let mut found = Vec::new();
    while let Some(row) = names.next().await {
        found.push(row.expect("decode a database name").name);
    }
    drop(names);
    for name in &found {
        admin
            .execute(
                &format!("DROP DATABASE IF EXISTS {name}"),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("drop a leftover database");
    }
    if !found.is_empty() {
        eprintln!(
            "dropped {} database(s) an earlier run of this test left behind",
            found.len()
        );
    }
}

/// [`leftover_stem`]'s refusal, hermetically: every reading of the prefix
/// that `test_db` treats as unset refuses the sweep, and a set prefix
/// admits a stem composed under it.
#[test]
fn the_leftover_sweep_refuses_without_a_prefix() {
    let bare = "sweep_fixture_stem_";
    for unset in [None, Some(""), Some("   "), Some("\t")] {
        assert_eq!(
            leftover_stem(unset, bare),
            None,
            "prefix {unset:?}: the stem would be shared with every checkout"
        );
    }
    let composed = "wt3_sweep_fixture_stem_";
    assert_eq!(leftover_stem(Some("wt3"), composed), Some(composed));
    assert_eq!(
        leftover_stem(Some(" wt3 "), composed),
        Some(composed),
        "trimmed, as test_db trims"
    );
}

/// A stem without its trailing `_` would let `…_w4spread` sweep
/// `…_w4spread2_…`, a different test's databases.
#[test]
#[should_panic(expected = "must end in `_`")]
fn a_sweep_stem_without_its_trailing_underscore_is_refused() {
    let _ = leftover_stem(Some("wt3"), "wt3_sweep_fixture_stem");
}

/// A set prefix and a stem that does not carry it: the stem was not
/// composed under this prefix, so it may name another checkout's databases.
#[test]
#[should_panic(expected = "must be composed by test_db under the prefix")]
fn a_sweep_stem_the_prefix_did_not_compose_is_refused() {
    let _ = leftover_stem(Some("wt3"), "sweep_fixture_stem_");
}

/// The unwrapped-value corpus both gates read: `n` deterministic values
/// of mixed magnitude and sign, one fingerprint, one JSON body each.
///
/// Returns the values in timestamp order. **The caller asserts the row
/// count against `n` before measuring anything** — the standing rule this
/// issue adopted after an insert that reported success and left zero rows
/// (a fixed 2023 timestamp against the table's retention rule, which drops
/// whole parts).
fn unwrap_values(n: u64) -> Vec<f64> {
    (0..n)
        .map(|i| {
            let r = splitmix64(i);
            let mag = 10f64.powi(((r >> 40) % 13) as i32 - 6);
            let frac = ((r & 0xFFFF_FFFF) as f64) / (u32::MAX as f64);
            let sign = if r & 1 == 0 { 1.0 } else { -1.0 };
            sign * mag * (1.0 + frac)
        })
        .collect()
}

fn unwrap_rows(fp: u64, t0: i64, values: &[f64]) -> Vec<UnwrapSeedRow> {
    values
        .iter()
        .enumerate()
        .map(|(i, v)| UnwrapSeedRow {
            org_id: String::new(),
            service: "w4".to_string(),
            fingerprint: u128::from(fp),
            timestamp_ns: t0 + i as i64,
            severity: 0,
            body: format!(r#"{{"v":{v:?}}}"#),
            structured_metadata: String::new(),
        })
        .collect()
}

/// `2(n−1)·u·Σ|vᵢ|` with `u = 2⁻⁵³` — the most two evaluation orders of the
/// same `n` values can differ by (issue #507 §5).
fn summation_spread_bound(values: &[f64]) -> f64 {
    let sum_abs: f64 = values.iter().map(|v| v.abs()).sum();
    2.0 * ((values.len() as f64) - 1.0) * (f64::EPSILON / 2.0) * sum_abs
}

/// **The gate that would tell us the decision was wrong** (issue #507 §6).
///
/// The owner accepted that the database chooses the summation order and may
/// choose differently between two executions. What that ruling assumes is
/// that the resulting answers stay within a rounding difference of each
/// other. This asserts exactly that, and against nothing foreign: it
/// compares our own answers at four thread counts **with each other**, so
/// it needs no tolerance against another implementation, and it is
/// scale-invariant, so it is a Tier-1 gate.
///
/// It fails precisely when the accepted divergence stops being last-bits.
#[tokio::test]
async fn the_thread_count_spread_stays_inside_the_summation_bound() {
    skip_unless_live!();
    const N: u64 = 1_000_000;
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    sweep_leftovers(
        &admin,
        &pulsus_testkit::test_db("pulsus_read_it_qlg_w4spread_"),
    )
    .await;
    let db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_w4spread_{}",
        uuid::Uuid::new_v4().simple()
    ));
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop");
    admin
        .execute(
            &format!("CREATE DATABASE {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create");
    run_init(&admin, &test_ctx(&db)).await.expect("run_init");
    let client = data_client(&db).await;

    let values = unwrap_values(N);
    let rows = unwrap_rows(1, now_ns() - 3_600_000_000_000, &values);
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("insert");
    land_seeded_lines(&client, &db).await;

    let sql = format!(
        "SELECT sum(JSONExtractFloat(body, 'v')) AS s, \
         groupBitXor(reinterpretAsUInt64(JSONExtractFloat(body, 'v'))) AS x, \
         count() AS n FROM {db}.log_samples WHERE service = 'w4' AND fingerprint = 1"
    );
    let our_xor = values.iter().fold(0u64, |a, v| a ^ v.to_bits());

    let mut answers: Vec<(u64, f64)> = Vec::new();
    for threads in [1u64, 2, 4, 8] {
        for _rep in 0..6 {
            let settings = QuerySettings::new().set("max_threads", threads);
            let mut stream = client
                .query_stream::<SumRow>(&sql, &settings)
                .await
                .expect("execute");
            let row = stream.next().await.expect("one row").expect("decode");
            drop(stream);
            // The standing rule: the input is asserted present, and the
            // same values, before the result is read for anything.
            assert_eq!(
                row.n, N,
                "max_threads={threads}: the corpus must be present"
            );
            assert_eq!(row.x, our_xor, "max_threads={threads}: the same value set");
            answers.push((threads, row.s));
        }
    }

    let bound = summation_spread_bound(&values);
    for (ta, a) in &answers {
        for (tb, b) in &answers {
            let diff = (a - b).abs();
            assert!(
                diff <= bound,
                "max_threads={ta} answered {a:?} and max_threads={tb} answered {b:?}: two \
                 evaluation orders of the same {N} values may differ by at most \
                 2(n-1)*u*sum|v| = {bound:e}, got {diff:e} — the accepted divergence has \
                 stopped being a rounding difference"
            );
        }
    }
    // Non-vacuity: the thread count DOES move the answer, so the bound is
    // doing work rather than passing on equality.
    let distinct: std::collections::BTreeSet<u64> =
        answers.iter().map(|(_, v)| v.to_bits()).collect();
    assert!(
        distinct.len() > 1,
        "every thread count answered the same bits: the spread this bounds no longer \
         exists, and the gate is passing on equality rather than on the bound"
    );

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop");
}

/// **Repeated executions of the same query on unchanged data agree bit for
/// bit, at `max_threads = 1` and a fixed part layout** (issue #507 §3).
///
/// Its failure means a source of nondeterminism exists that is neither the
/// thread count nor the part count — which is a thing we would want to know
/// and currently could not learn. It is exact, not tolerance-based: the
/// claim is reproducibility, not closeness.
///
/// **The scope was measured rather than assumed**, because the plan scoped
/// it to one part and named the measurement that would widen it. Six
/// repetitions per cell, over 1 000 000 identical values laid out in 1, 2
/// and 8 parts with merges stopped, **on an otherwise idle server**:
///
/// ```text
///  parts  threads=1           threads=4           threads=8
///  1      C1B845C49C9466C4    C1B845C49C9465EC    C1B845C49C946670
///  2      C1B845C49C94663A    C1B845C49C946623    C1B845C49C946622
///  8      C1B845C49C946663    C1B845C49C946662    C1B845C49C946663
/// ```
///
/// Constant in every cell, and different between cells. So the part count
/// is a **third source of divergence**, alongside the thread count and the
/// block accumulation that separates us from the database in the first
/// place.
///
/// **`max_threads = 1` is not a detail of this test, it is its subject.**
/// The first version pinned four threads and passed alone and failed
/// inside the suite, fourteen ULPs apart. The cause is not scheduling
/// jitter: `max_threads` is an UPPER BOUND, and the server reduces the
/// EFFECTIVE degree of parallelism when it is busy, so the answer moves
/// with the load rather than with the setting. Measured directly, six
/// repetitions each, with six concurrent heavy queries as the load:
///
/// ```text
///  quiet,     threads=4   C1B845C49C9465EC   constant
///  under load, threads=4  C1B845C49C9465E6   constant, and DIFFERENT
///  under load, threads=1  C1B845C49C9466C4   constant, and equal to quiet
/// ```
///
/// At one thread the effective degree is one whatever the server is doing,
/// which is what makes the claim below reproducible rather than merely
/// usually true. **The user-visible consequence is worth stating: two
/// refreshes of an unchanged dashboard can differ because the server was
/// busier, not because anything was configured differently.**
///
/// `SYSTEM STOP MERGES` is what makes the part count an input rather than a
/// race: without it a background merge rewrites eight parts into three
/// while the test runs, which was measured before this test was written.
#[tokio::test]
async fn repeated_executions_agree_bit_for_bit_at_a_fixed_layout() {
    skip_unless_live!();
    const N: u64 = 1_000_000;
    const THREADS: u64 = 1;
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    sweep_leftovers(
        &admin,
        &pulsus_testkit::test_db("pulsus_read_it_qlg_w4parts_"),
    )
    .await;
    let db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_w4parts_{}",
        uuid::Uuid::new_v4().simple()
    ));
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop");
    admin
        .execute(
            &format!("CREATE DATABASE {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create");
    run_init(&admin, &test_ctx(&db)).await.expect("run_init");
    let client = data_client(&db).await;
    client
        .execute(
            &format!("SYSTEM STOP MERGES {db}.log_samples"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("stop merges");

    let values = unwrap_values(N);
    let t0 = now_ns() - 3_600_000_000_000;
    let mut per_layout: Vec<(usize, u64)> = Vec::new();
    for (parts, fp) in [(1usize, 11u64), (2, 12), (8, 13)] {
        let chunk = values.len() / parts;
        for c in 0..parts {
            let lo = c * chunk;
            let hi = if c + 1 == parts {
                values.len()
            } else {
                lo + chunk
            };
            let rows = unwrap_rows(fp, t0 + lo as i64, &values[lo..hi]);
            let landing = unwrap_landing_rows(now_ns() / 1_000_000, &rows);
            client
                .insert_block("log_landing", &landing)
                .await
                .expect("insert");
        }
        let sql = format!(
            "SELECT sum(JSONExtractFloat(body, 'v')) AS s, \
             groupBitXor(reinterpretAsUInt64(JSONExtractFloat(body, 'v'))) AS x, \
             count() AS n FROM {db}.log_samples WHERE service = 'w4' AND fingerprint = {fp}"
        );
        let our_xor = values.iter().fold(0u64, |a, v| a ^ v.to_bits());
        let mut seen: Vec<u64> = Vec::new();
        for rep in 0..6 {
            let settings = QuerySettings::new().set("max_threads", THREADS);
            let mut stream = client
                .query_stream::<SumRow>(&sql, &settings)
                .await
                .expect("execute");
            let row = stream.next().await.expect("one row").expect("decode");
            drop(stream);
            assert_eq!(row.n, N, "parts={parts}: the corpus must be present");
            assert_eq!(row.x, our_xor, "parts={parts}: the same value set");
            seen.push(row.s.to_bits());
            assert_eq!(
                seen[0], seen[rep],
                "parts={parts}, max_threads={THREADS}: repetition {rep} answered different \
                 bits from repetition 0, so a source of nondeterminism exists that is \
                 neither the thread count nor the part count"
            );
        }
        // The layout is the one asked for, not the one a merge left behind.
        let layout_sql = format!(
            "SELECT count() AS n FROM system.parts WHERE database = '{db}' \
             AND table = 'log_samples' AND active AND rows > 0"
        );
        let mut stream = admin
            .query_stream::<PartCountRow>(&layout_sql, &QuerySettings::new())
            .await
            .expect("read the part layout");
        let observed = stream.next().await.expect("one row").expect("decode").n;
        drop(stream);
        per_layout.push((parts, seen[0]));
        assert!(
            observed >= parts as u64,
            "parts={parts}: the table holds {observed} active parts, so the layout this cell \
             names was merged away before it was measured"
        );
    }
    // The measured finding this gate's scope rests on: the part layout
    // moves the answer. If it stopped doing so, the scope could be widened
    // and the user-facing sentence loses a clause — which should be a
    // decision, not a silent pass.
    let distinct: std::collections::BTreeSet<u64> = per_layout.iter().map(|(_, b)| *b).collect();
    assert!(
        distinct.len() > 1,
        "every part layout answered the same bits: {per_layout:?} — the part count is no \
         longer a source of divergence and the documentation says it is"
    );

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop");
}

// ---------------------------------------------------------------------
// Issue #507 and #624, part 4b: the spread reducers after a parser.
// ---------------------------------------------------------------------

/// **The spread reducers are lowered after a parser, and answer the
/// evaluator's value on the high-offset corpus** (issue #507, review
/// rounds 1 and 3; issue #624, parts 4 and 4b).
///
/// Over `{1e16, 1e16+2, +4, +8, +16}` the plain `stddevPop`/`varPop` answer
/// `0` and `0` against the evaluator's `31.2` and `5.585696017507576`. The
/// reduced statement reduces each window in timestamp order (part 4), and
/// since part 4b reads the value through the `| json` stage: the plan must
/// be the reduced one, the last reported statement must be the reduced
/// statement (no fallback ran after it), and the answer must be the
/// evaluator's, bit for bit.
#[tokio::test]
async fn the_spread_reducers_are_lowered_and_answer_the_evaluators_value() {
    skip_unless_live!();
    const STEP: i64 = 60_000_000_000;
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    sweep_leftovers(
        &admin,
        &pulsus_testkit::test_db("pulsus_read_it_qlg_w4spread2_"),
    )
    .await;
    let db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_w4spread2_{}",
        uuid::Uuid::new_v4().simple()
    ));
    for stmt in [
        format!("DROP DATABASE IF EXISTS {db}"),
        format!("CREATE DATABASE {db}"),
    ] {
        admin
            .execute(&stmt, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .expect("set up");
    }
    run_init(&admin, &test_ctx(&db)).await.expect("run_init");
    let client = data_client(&db).await;

    let t = ((now_ns() - 3_600_000_000_000) / STEP) * STEP;
    let service = "c507spread";
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) \
                 VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({t}))), 301, \
                 '{service}', '{{\"service_name\":\"{service}\"}}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");
    land_seeded_streams(&client, &db).await;
    let offsets = [0.0f64, 2.0, 4.0, 8.0, 16.0];
    let rows: Vec<BucketedSeedRow> = offsets
        .iter()
        .enumerate()
        .map(|(i, d)| BucketedSeedRow {
            org_id: String::new(),
            service: service.to_string(),
            fingerprint: 301,
            timestamp_ns: t - 30_000_000_000 + i as i64,
            severity: 0,
            body: format!(r#"{{"latency":{:?}}}"#, 1e16f64 + d),
            structured_metadata: String::new(),
        })
        .collect();
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("insert the high-offset fixture");
    land_seeded_lines(&client, &db).await;
    let mut seeded = admin
        .query_stream::<PartCountRow>(
            &format!("SELECT count() AS n FROM {db}.log_samples"),
            &QuerySettings::new(),
        )
        .await
        .expect("count the corpus");
    let n = seeded.next().await.expect("one row").expect("decode").n;
    drop(seeded);
    assert_eq!(n, offsets.len() as u64, "the corpus must be present");

    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t,
            step_ns: STEP as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );

    // The evaluator's own answers on this corpus, which are also the
    // reference's: the variance of five values 0, 2, 4, 8, 16 above a
    // shared offset is 31.2, and its square root 5.585696017507576.
    for (op, want) in [
        ("stdvar_over_time", 31.2f64),
        ("stddev_over_time", 5.585696017507576f64),
    ] {
        let q = format!(
            r#"{op}({{service_name="{service}"}} | json latency="latency" | unwrap latency [1m])"#
        );
        match plan(&parse(&q).expect("parse"), &params, &plan_ctx(&db)) {
            Ok(Plan::Metric(mp)) => assert!(
                mp.client.is_none() && matches!(mp.value, sql::MetricValue::Reduced(_)),
                "{op} lowers to the reduced statement: {:?}",
                mp.value
            ),
            other => panic!("{q}: {other:?}"),
        }
        let (result, _w, explain) = engine
            .query_explained(&parse(&q).expect("parse"), &params)
            .await
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        let QueryResult::Matrix(series) = result else {
            panic!("{q}: expected a matrix");
        };
        let read = explain
            .stages
            .iter()
            .rfind(|s| s.name == "metric_read")
            .map(|s| s.sql.clone())
            .unwrap_or_default();
        assert!(
            is_reduced_624p4(&read),
            "{op}: the last statement must be the reduced statement, got:\n{read}"
        );
        let points: Vec<(i64, u64)> = series
            .into_iter()
            .flat_map(|s| s.points)
            .map(|(ts, v)| (ts, v.to_bits()))
            .collect();
        assert_eq!(
            points,
            vec![(t, want.to_bits())],
            "{op}: the evaluator's value, bit for bit"
        );
    }

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

// ---------------------------------------------------------------------
// Issue #507's group key cases, answered since issue #624, part 4b by the
// reduced statement.
// ---------------------------------------------------------------------

/// One case of `tests/fixtures/group_key/cases.tsv`: one stream, its rows,
/// one query and the answer.
#[derive(Debug, Clone)]
struct GroupKeyCase {
    id: String,
    stream: std::collections::BTreeMap<String, String>,
    /// `(body, structured metadata)` per row.
    entries: Vec<(String, String)>,
    query: String,
    expected: String,
}

fn unhex_utf8(h: &str) -> String {
    let bytes: Vec<u8> = (0..h.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&h[i..i + 2], 16).expect("hex"))
        .collect();
    String::from_utf8(bytes).expect("utf-8 fixture")
}

fn load_group_key_cases(path: &str) -> Vec<GroupKeyCase> {
    let text = std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
        .expect("read the fixture");
    text.lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .map(|l| {
            let f: Vec<&str> = l.split('\t').collect();
            assert_eq!(f.len(), 5, "a fixture row has 5 fields: {l}");
            let stream = if f[1].is_empty() {
                std::collections::BTreeMap::new()
            } else {
                serde_json::from_str(f[1]).expect("stream labels")
            };
            let entries = f[2]
                .split(',')
                .map(|e| {
                    let (b, sm) = e.split_once(':').expect("body:metadata");
                    (unhex_utf8(b), unhex_utf8(sm))
                })
                .collect();
            GroupKeyCase {
                id: f[0].to_string(),
                stream,
                entries,
                query: f[3].to_string(),
                expected: f[4].to_string(),
            }
        })
        .collect()
}

/// Seeds each case as its own stream `{service_name="<id>", …}` at
/// fingerprint `fp_base + index`, its rows 50 s before `t`.
async fn seed_group_key_cases(
    admin: &ChClient,
    client: &ChClient,
    db: &str,
    t: i64,
    fp_base: u64,
    cases: &[GroupKeyCase],
) {
    let month = format!("toStartOfMonth(fromUnixTimestamp64Nano(toInt64({t})))");
    let mut rows = Vec::new();
    let mut streams = Vec::new();
    let mut idx = Vec::new();
    for (i, case) in cases.iter().enumerate() {
        let fp = fp_base + i as u64;
        let mut labels = case.stream.clone();
        labels.insert("service_name".to_string(), case.id.clone());
        let labels_json = serde_json::to_string(&labels).expect("labels json");
        streams.push(format!(
            "({month}, {fp}, {}, {}, 0)",
            literal(&case.id).as_sql(),
            literal(&labels_json).as_sql()
        ));
        for (k, v) in &labels {
            idx.push(format!(
                "({month}, {}, {}, {fp})",
                literal(k).as_sql(),
                literal(v).as_sql()
            ));
        }
        for (j, (body, sm)) in case.entries.iter().enumerate() {
            rows.push(BucketedSeedRow {
                org_id: String::new(),
                service: case.id.clone(),
                fingerprint: u128::from(fp),
                timestamp_ns: t - 50_000_000_000 + j as i64,
                severity: 0,
                body: body.clone(),
                structured_metadata: sm.clone(),
            });
        }
    }
    for (table, cols, values) in [
        (
            "log_streams",
            "(month, fingerprint, service, labels, updated_ns)",
            &streams,
        ),
        ("log_streams_idx", "(month, key, val, fingerprint)", &idx),
    ] {
        for chunk in values.chunks(500) {
            admin
                .execute(
                    &format!(
                        "INSERT INTO {db}.{table} {cols} VALUES {}",
                        chunk.join(", ")
                    ),
                    &QuerySettings::new(),
                    Idempotency::Idempotent,
                )
                .await
                .unwrap_or_else(|e| panic!("seed {table}: {e}"));
        }
    }
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("insert the group key cases");
    land_seeded_lines(client, db).await;
}

/// A case's query with its selector, and the request it runs as: a range
/// query at one grid point `t` whose step is the query's range; `[2m]`
/// keeps a one-minute step, which is the point of that case. A log query
/// reads the five minutes before `t`.
fn group_key_request(case: &GroupKeyCase, t: i64) -> (String, QueryParams) {
    let query = case
        .query
        .replace("SEL", &format!("{{service_name={:?}}}", case.id));
    let is_metric = query.contains("_over_time(") || query.contains("rate(");
    let params = if is_metric {
        let step: u64 = if query.contains("[5m]") {
            300_000_000_000
        } else {
            60_000_000_000
        };
        QueryParams {
            spec: QuerySpec::Range {
                start_ns: t,
                end_ns: t,
                step_ns: step,
            },
            limit: 100,
            direction: Direction::Backward,
        }
    } else {
        QueryParams {
            spec: QuerySpec::Range {
                start_ns: t - 300_000_000_000,
                end_ns: t,
                step_ns: 60_000_000_000,
            },
            limit: 10,
            direction: Direction::Backward,
        }
    };
    (query, params)
}

fn group_key_labels(labels: &[(String, String)]) -> String {
    let mut l: Vec<&(String, String)> =
        labels.iter().filter(|(k, _)| k != "service_name").collect();
    l.sort();
    format!(
        "{{{}}}",
        l.iter()
            .map(|(k, v)| format!("{k}={v:?}"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// The canonical text of an answer, as the fixture writes it: series sorted
/// by label set, `service_name` left out, each value as Rust's `{:?}` of its
/// `f64`.
fn group_key_answer(res: Result<(QueryResult, pulsus_read::Warnings), ReadError>) -> String {
    match res {
        Ok((QueryResult::Vector(v), _)) if v.is_empty() => "no series".to_string(),
        Ok((QueryResult::Matrix(m), _)) if m.is_empty() => "no series".to_string(),
        Ok((QueryResult::Vector(v), _)) => {
            let mut s: Vec<String> = v
                .iter()
                .map(|x| format!("{} {:?}", group_key_labels(&x.labels), x.value))
                .collect();
            s.sort();
            s.join("; ")
        }
        Ok((QueryResult::Matrix(m), _)) => {
            let mut s: Vec<String> = m
                .iter()
                .map(|x| {
                    let pts: Vec<String> = x.points.iter().map(|(_, v)| format!("{v:?}")).collect();
                    format!("{} {}", group_key_labels(&x.labels), pts.join(","))
                })
                .collect();
            s.sort();
            s.join("; ")
        }
        Ok((QueryResult::Streams { items, .. }, _)) if items.is_empty() => "no series".to_string(),
        Ok((QueryResult::Streams { items, .. }, _)) => {
            let mut s: Vec<String> = items
                .iter()
                .map(|x| {
                    let m: std::collections::BTreeMap<String, String> =
                        serde_json::from_str(&x.labels_json).expect("stream labels json");
                    group_key_labels(&m.into_iter().collect::<Vec<_>>())
                })
                .collect();
            s.sort();
            format!("streams {}", s.join("; "))
        }
        Ok((other, _)) => format!("unexpected {other:?}"),
        Err(ReadError::MetricPipelineError { error_type, .. }) => {
            format!("400 pipeline error: '{error_type}'")
        }
        Err(ReadError::QueryTooBroad(reason)) => {
            let name = format!("{reason:?}");
            let name = name.split([' ', '{', '(']).next().unwrap_or("").to_string();
            format!("422 {name}")
        }
        Err(ReadError::Parse(_) | ReadError::PipelineInvalid { .. }) => "400 parse".to_string(),
        Err(other) => format!("error {other}"),
    }
}

/// Which statement a logged query is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupKeyStatement {
    /// The reduced statement (issue #624, part 4).
    Reduced,
    /// Today's raw scan.
    Raw,
    /// A lowered `| json` count (issue #624, part 3b).
    Count,
}

/// Seeds `cases` into a fresh database, runs each through the engine, and
/// returns every way a case differs from its row; a statement in
/// `system.query_log` that reads `uw_r` is a difference too.
async fn check_group_key_cases(stem: &str, fp_base: u64, cases: &[GroupKeyCase]) -> Vec<String> {
    let (admin, client, db) = group_key_db(stem).await;
    let t = ((now_ns() - 3_600_000_000_000) / 300_000_000_000) * 300_000_000_000;
    seed_group_key_cases(&admin, &client, &db, t, fp_base, cases).await;
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 50 * 1024 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let mut wrong = Vec::new();
    let mut planned = 0usize;
    for case in cases {
        let (query, params) = group_key_request(case, t);
        let answer = match parse(&query) {
            Ok(expr) => {
                if plan(&expr, &params, &plan_ctx(&db)).is_ok() {
                    planned += 1;
                }
                group_key_answer(engine.query(&expr, &params).await)
            }
            Err(_) => "400 parse".to_string(),
        };
        if answer != case.expected {
            wrong.push(format!(
                "{}: {query}\n    got  {answer}\n    want {}",
                case.id, case.expected
            ));
        }
    }
    // Every case that plans runs at least one statement over the samples.
    let logged = statements_over_samples(&admin, &db, planned).await;
    assert!(
        logged.len() >= planned,
        "{} statements logged for {planned} planned cases",
        logged.len()
    );
    for query in logged.iter().filter(|q| q.contains("uw_r")) {
        wrong.push(format!("a statement reads uw_r:\n{query}"));
    }
    drop_group_key_db(&admin, &db).await;
    wrong
}

/// Every finished or failed statement over `db`'s samples, in the order they
/// ran. `SYSTEM FLUSH LOGS` does not guarantee the rows are there, so this
/// waits until at least `want` are logged.
async fn statements_over_samples(admin: &ChClient, db: &str, want: usize) -> Vec<String> {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct Q {
        query: String,
    }
    let mut out = Vec::new();
    for _ in 0..30 {
        admin
            .execute(
                "SYSTEM FLUSH LOGS",
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("flush logs");
        let sql = format!(
            "SELECT query FROM system.query_log \
             WHERE has(databases, '{db}') AND type != 'QueryStart' \
             AND query LIKE '%log_samples%' AND query NOT LIKE '%system.query_log%' \
             ORDER BY event_time_microseconds ASC"
        );
        let mut stream = admin
            .query_stream::<Q>(&sql, &QuerySettings::new())
            .await
            .expect("read system.query_log");
        out.clear();
        while let Some(row) = stream.next().await {
            out.push(row.expect("decode a query_log row").query);
        }
        drop(stream);
        if out.len() >= want {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    out
}

/// **T5 (issue #624, part 4b): every case of the group key fixture answers
/// its expected value**, and no statement reads the retired group key
/// read's `uw_r`.
///
/// `tests/fixtures/group_key/cases.tsv` holds each case of issue #507's
/// tables — the JSON text rules, nesting, text after a value, empty and
/// absent values, lines that are not JSON, the q, g, e and k rows, the
/// collision rows, the refused chains, the reserved-name rows at the
/// reference's answers, the three rows of the unconvertible value under
/// `__preserve_error__`, and a filter naming the unwrapped label — with its
/// answer, value bits included. Each query runs as a range query at one
/// grid point whose step is its range.
#[tokio::test]
async fn the_group_key_cases_answer_their_expected_values() {
    skip_unless_live!();
    let cases = load_group_key_cases("tests/fixtures/group_key/cases.tsv");
    assert_eq!(cases.len(), 286, "the fixture's cases");
    let wrong = check_group_key_cases("gk", 507_000, &cases).await;
    assert!(
        wrong.is_empty(),
        "{} of {} cases differ:\n{}",
        wrong.len(),
        cases.len(),
        wrong.join("\n")
    );
}

/// **Criteria 29 and 42: reserved label names answer as the pinned reference
/// build answers, on every metric route** (issue #507).
///
/// One stream per source of a reserved name, one row each, an hour back:
///
/// ```text
/// fp 901  {__error__="s", service_name="rn1"}   {"latency":5}                     stream label
/// fp 902  {service_name="rn2"}                   {"latency":5,"__error__":"boom"}  a field in the line
/// fp 903  {service_name="rn3"}                   {garbage}  metadata __preserve_error__=true
/// fp 904  {service_name="rn4"}                   {garbage}
/// fp 905  {service_name="k3"}                    {"latency":"abc"}  metadata __preserve_error__=true
/// fp 906  {service_name="k4"}                    {"latency":"abc","__preserve_error__":"true"}
/// ```
///
/// Each query's route is asserted from its plan before its answer, so a
/// query that stopped reaching its fold would fail here rather than pass on
/// another path. Every expected answer was captured from the pinned
/// reference build (issue #507, the reserved-name tables, and revision 10's
/// k rows).
#[tokio::test]
async fn reserved_names_answer_as_the_reference_on_every_metric_route() {
    skip_unless_live!();
    const STEP: i64 = 60_000_000_000;
    let db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_rn_{}",
        uuid::Uuid::new_v4().simple()
    ));
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    for stmt in [
        format!("DROP DATABASE IF EXISTS {db}"),
        format!("CREATE DATABASE {db}"),
    ] {
        admin
            .execute(&stmt, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .expect("db");
    }
    run_init(&admin, &test_ctx(&db)).await.expect("run_init");
    let client = data_client(&db).await;
    let t = ((now_ns() - 3_600_000_000_000) / STEP) * STEP;
    let streams = [
        (
            901u64,
            "rn1",
            r#"{"__error__":"s","service_name":"rn1"}"#,
            r#"{"latency":5}"#,
            "",
        ),
        (
            902u64,
            "rn2",
            r#"{"service_name":"rn2"}"#,
            r#"{"latency":5,"__error__":"boom"}"#,
            "",
        ),
        (
            903u64,
            "rn3",
            r#"{"service_name":"rn3"}"#,
            "{garbage",
            r#"{"__preserve_error__":"true"}"#,
        ),
        (904u64, "rn4", r#"{"service_name":"rn4"}"#, "{garbage", ""),
        (
            905u64,
            "k3",
            r#"{"service_name":"k3"}"#,
            r#"{"latency":"abc"}"#,
            r#"{"__preserve_error__":"true"}"#,
        ),
        (
            906u64,
            "k4",
            r#"{"service_name":"k4"}"#,
            r#"{"latency":"abc","__preserve_error__":"true"}"#,
            "",
        ),
    ];
    let mut rows = Vec::new();
    for (fp, service, labels, body, sm) in streams {
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) \
                     VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({t}))), {fp}, '{service}', '{labels}', 0)"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed log_streams");
        land_seeded_streams(&client, &db).await;
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_streams_idx (month, key, val, fingerprint) VALUES \
                     (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({t}))), 'service_name', '{service}', {fp})"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed log_streams_idx");
        land_seeded_streams(&client, &db).await;
        rows.push(BucketedSeedRow {
            org_id: String::new(),
            service: service.to_string(),
            fingerprint: u128::from(fp),
            timestamp_ns: t - 10_000_000_000,
            severity: 0,
            body: body.to_string(),
            structured_metadata: sm.to_string(),
        });
    }
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("insert");
    land_seeded_lines(&client, &db).await;

    let instant = QueryParams {
        spec: QuerySpec::Instant { at_ns: t },
        limit: 100,
        direction: Direction::Backward,
    };
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t,
            step_ns: STEP as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    #[derive(Debug, PartialEq)]
    enum Route {
        Client,
        Lowered,
    }
    let route = |query: &str, params: &QueryParams| match plan(
        &parse(query).expect("parse"),
        params,
        &plan_ctx(&db),
    )
    .expect("plan")
    {
        Plan::Metric(mp) if mp.client.is_some() => Route::Client,
        Plan::Metric(_) => Route::Lowered,
        _ => panic!("{query}: a metric plan"),
    };
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let render = |labels: &[(String, String)]| {
        let mut l = labels.to_vec();
        l.sort();
        format!(
            "{{{}}}",
            l.iter()
                .map(|(k, v)| format!("{k}={v:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    // (query, params, route, the reference's answer)
    let cases: [(&str, &QueryParams, Route, &str); 12] = [
        (
            r#"sum by (latency) (sum_over_time({service_name="rn1"} | json latency="latency" | unwrap latency [1m]))"#,
            &range,
            Route::Client,
            r#"{latency="5"} 5"#,
        ),
        (
            r#"sum by (service_name) (count_over_time({service_name="rn1"} [1m]))"#,
            &instant,
            Route::Lowered,
            r#"{service_name="rn1"} 1"#,
        ),
        (
            r#"sum by (service_name) (count_over_time({service_name="rn1"} [1m]))"#,
            &range,
            Route::Lowered,
            r#"{service_name="rn1"} 1"#,
        ),
        (
            r#"sum by (service_name) (sum_over_time({service_name="rn1"} | json latency="latency" | unwrap latency [1m]))"#,
            &range,
            Route::Lowered,
            r#"{service_name="rn1"} 5"#,
        ),
        (
            r#"sum by (service_name) (sum_over_time({service_name="rn2"} | json | unwrap latency [5m]))"#,
            &instant,
            Route::Lowered,
            r#"{service_name="rn2"} 5"#,
        ),
        (
            r#"sum by (service_name) (sum_over_time({service_name="rn2"} | json | unwrap latency [5m]))"#,
            &range,
            Route::Lowered,
            r#"{service_name="rn2"} 5"#,
        ),
        (
            r#"count_over_time({service_name="rn3"} | json [5m])"#,
            &instant,
            Route::Client,
            r#"{__error__="JSONParserErr", __error_details__="Value looks like object, but can't find closing '}' symbol", __preserve_error__="true", service_name="rn3"} 1"#,
        ),
        (
            r#"sum by (__error__) (count_over_time({service_name="rn4"} | json [5m]))"#,
            &instant,
            Route::Client,
            r#"{__error__="JSONParserErr"} 1"#,
        ),
        (
            r#"sum by (service_name) (count_over_time({service_name="rn4"} | json | __error__!="" [5m]))"#,
            &range,
            Route::Client,
            r#"{service_name="rn4"} 1"#,
        ),
        (
            r#"count_over_time({service_name="rn1"} [1m])"#,
            &instant,
            Route::Lowered,
            "400 pipeline error: 's'",
        ),
        // Revision 10's k3.00 and k4.00, lowered since issue #624, part 4b:
        // a preserved failed conversion counts zero (R2); a parsed
        // `__preserve_error__` not required by the hints is skipped (R4), so
        // k4's error fails the query.
        (
            r#"sum by (service_name) (sum_over_time({service_name="k3"} | json | unwrap latency [5m]))"#,
            &instant,
            Route::Lowered,
            r#"{service_name="k3"} 0"#,
        ),
        (
            r#"sum by (service_name) (sum_over_time({service_name="k4"} | json | unwrap latency [5m]))"#,
            &instant,
            Route::Lowered,
            "400 pipeline error: 'SampleExtractionErr'",
        ),
    ];
    for (query, params, want_route, want) in cases {
        assert_eq!(route(query, params), want_route, "{query}: the route");
        let got = match engine.query(&parse(query).expect("parse"), params).await {
            Ok((QueryResult::Vector(v), _)) => v
                .iter()
                .map(|s| format!("{} {}", render(&s.labels), s.value))
                .collect::<Vec<_>>()
                .join("; "),
            Ok((QueryResult::Matrix(m), _)) => m
                .iter()
                .map(|s| {
                    format!(
                        "{} {}",
                        render(&s.labels),
                        s.points
                            .iter()
                            .map(|(_, v)| v.to_string())
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                })
                .collect::<Vec<_>>()
                .join("; "),
            Ok((other, _)) => panic!("{query}: {other:?}"),
            Err(ReadError::MetricPipelineError { error_type, series }) => {
                assert!(
                    !series.contains("__preserve_error__"),
                    "{query}: a parsed __preserve_error__ the hints do not require is skipped: {series}"
                );
                format!("400 pipeline error: '{error_type}'")
            }
            Err(e) => panic!("{query}: {e}"),
        };
        assert_eq!(got, want, "{query}");
    }
    // A `variants(...)` sub-state takes its variant's parent `sum` (the
    // reference, `variants(sum by (service_name) (count_over_time({…} [5m])))
    // of ({…} | json [5m])` over a line with a `__error__` field:
    // `{__variant__="0", service_name=…} 1`).
    let variants = r#"variants(sum by (service_name) (count_over_time({service_name="rn2"} [5m]))) of ({service_name="rn2"} | json [5m])"#;
    let got = match engine
        .query(&parse(variants).expect("parse"), &instant)
        .await
    {
        Ok((QueryResult::Vector(v), _)) => v
            .iter()
            .map(|s| format!("{} {}", render(&s.labels), s.value))
            .collect::<Vec<_>>()
            .join("; "),
        Ok((other, _)) => panic!("{variants}: {other:?}"),
        Err(e) => panic!("{variants}: {e}"),
    };
    assert_eq!(
        got, r#"{__variant__="0", service_name="rn2"} 1"#,
        "{variants}"
    );
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

/// A fresh run database with the schema, for one group key test.
async fn group_key_db(stem: &str) -> (ChClient, ChClient, String) {
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    let db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_{stem}_{}",
        uuid::Uuid::new_v4().simple()
    ));
    for stmt in [
        format!("DROP DATABASE IF EXISTS {db}"),
        format!("CREATE DATABASE {db}"),
    ] {
        admin
            .execute(&stmt, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .expect("database");
    }
    run_init(&admin, &test_ctx(&db)).await.expect("run_init");
    let client = data_client(&db).await;
    (admin, client, db)
}

async fn drop_group_key_db(admin: &ChClient, db: &str) {
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

/// The server's clock, in microseconds: a marker between two queries'
/// `system.query_log` rows.
async fn server_micros(admin: &ChClient) -> u64 {
    let mut s = admin
        .query_stream::<PartCountRow>(
            "SELECT toUInt64(toUnixTimestamp64Micro(now64(6))) AS n",
            &QuerySettings::new(),
        )
        .await
        .expect("now64");
    let n = s.next().await.expect("one row").expect("decode").n;
    drop(s);
    n
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TimedStatementRow {
    query: String,
    exception_code: i32,
}

/// The statements over `db` that finished or failed in
/// `[from, to)` server microseconds, in the order they ran. Waits until at
/// least `want` of them are logged.
async fn group_key_statements_between(
    admin: &ChClient,
    db: &str,
    from: u64,
    to: u64,
    want: usize,
) -> Vec<(GroupKeyStatement, i32)> {
    let mut out = Vec::new();
    for _ in 0..30 {
        admin
            .execute(
                "SYSTEM FLUSH LOGS",
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("flush logs");
        let sql = format!(
            "SELECT query, exception_code FROM system.query_log \
             WHERE has(databases, '{db}') AND type != 'QueryStart' \
             AND query NOT LIKE '%system.query_log%' \
             AND query_start_time_microseconds >= fromUnixTimestamp64Micro(toInt64({from})) \
             AND query_start_time_microseconds < fromUnixTimestamp64Micro(toInt64({to})) \
             ORDER BY query_start_time_microseconds ASC"
        );
        let mut stream = admin
            .query_stream::<TimedStatementRow>(&sql, &QuerySettings::new())
            .await
            .expect("read system.query_log");
        out.clear();
        while let Some(row) = stream.next().await {
            let row = row.expect("decode");
            let kind = if is_reduced_624p4(&row.query) {
                GroupKeyStatement::Reduced
            } else if row
                .query
                .starts_with("SELECT fingerprint, timestamp_ns, body")
            {
                GroupKeyStatement::Raw
            } else if row.query.contains("throwIf(decided = 0) = 0") {
                GroupKeyStatement::Count
            } else {
                continue;
            };
            out.push((kind, row.exception_code));
        }
        drop(stream);
        if out.len() >= want {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    out
}

/// Seeds `rows` rows of the realistic corpus into `db`: 20 streams
/// `{service_name="checkout", pod="checkout-<i>"}`, one row every 1.8 ms from
/// `start`, 90% request bodies and 10% cache messages, each with its own
/// `request_id` and `ts`. `undecided` writes every request body's `status`
/// as a float (`200` becomes `0.200`), which the key statement cannot
/// decide.
async fn seed_realistic_corpus(admin: &ChClient, db: &str, start: i64, rows: u64, undecided: bool) {
    seed_realistic_corpus_into(admin, db, "", start, rows, undecided).await;
}

/// [`seed_realistic_corpus`] into tables named with `suffix` (`_dist` on a
/// cluster, where each insert waits for every shard).
async fn seed_realistic_corpus_into(
    admin: &ChClient,
    db: &str,
    suffix: &str,
    start: i64,
    rows: u64,
    undecided: bool,
) {
    let month = format!("toStartOfMonth(fromUnixTimestamp64Nano(toInt64({start})))");
    let status = if undecided { "0." } else { "" };
    for sql in [
        format!(
            "INSERT INTO {db}.log_streams{suffix} (month, fingerprint, service, labels, updated_ns) \
             SELECT {month}, 1000 + number, 'checkout', \
             concat('{{\"pod\":\"checkout-', toString(number), '\",\"service_name\":\"checkout\"}}'), 0 \
             FROM numbers(20)"
        ),
        format!(
            "INSERT INTO {db}.log_streams_idx{suffix} (month, key, val, fingerprint) \
             SELECT {month}, k, v, 1000 + number FROM numbers(20) \
             ARRAY JOIN ['service_name', 'pod'] AS k, ['checkout', concat('checkout-', toString(number))] AS v"
        ),
        format!(
            "INSERT INTO {db}.log_samples{suffix} (service, fingerprint, timestamp_ns, severity, body, structured_metadata) \
             SELECT 'checkout', 1000 + (number % 20), {start} + 1 + number * 1800000 AS ts, 0, \
             if(number % 10 = 9, \
               concat('{{\"ts\":\"', formatDateTime(fromUnixTimestamp64Nano(ts), '%Y-%m-%dT%H:%i:%S.%fZ'), \
                 '\",\"request_id\":\"', lower(hex(cityHash64(number))), lower(hex(cityHash64(number + 1))), \
                 '\",\"service\":\"checkout\",\"level\":\"info\",\"msg\":\"cache refreshed\"}}'), \
               concat('{{\"ts\":\"', formatDateTime(fromUnixTimestamp64Nano(ts), '%Y-%m-%dT%H:%i:%S.%fZ'), \
                 '\",\"request_id\":\"', lower(hex(cityHash64(number))), lower(hex(cityHash64(number + 1))), \
                 '\",\"service\":\"checkout\",\"method\":\"', ['GET','POST','PUT','DELETE'][number % 4 + 1], \
                 '\",\"status\":{status}', toString([200,201,204,400,404,500][number % 6 + 1]), \
                 ',\"path\":\"/api/v1/items/', toString(number % 1000), \
                 '\",\"latency\":', toString(round(((number * 7919) % 100000) / 100, 2)), '}}')), '' \
             FROM numbers({rows})"
        ),
    ] {
        admin
            .execute(
                &sql,
                &QuerySettings::new().set("distributed_foreground_insert", 1),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed the realistic corpus");
    }
}

/// Inserts `sql`'s rows as one stream `{service_name="<service>"}` at `fp`,
/// with `pod` as a second stream label when it is not empty.
async fn seed_one_stream(
    admin: &ChClient,
    db: &str,
    t: i64,
    fp: u64,
    service: &str,
    pod: &str,
    rows_sql: &str,
) {
    let month = format!("toStartOfMonth(fromUnixTimestamp64Nano(toInt64({t})))");
    let (labels, idx) = if pod.is_empty() {
        (
            format!("{{\"service_name\":\"{service}\"}}"),
            format!("({month}, 'service_name', '{service}', {fp})"),
        )
    } else {
        (
            format!("{{\"pod\":\"{pod}\",\"service_name\":\"{service}\"}}"),
            format!(
                "({month}, 'service_name', '{service}', {fp}), ({month}, 'pod', '{pod}', {fp})"
            ),
        )
    };
    for sql in [
        format!(
            "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) VALUES \
             ({month}, {fp}, '{service}', '{labels}', 0)"
        ),
        format!("INSERT INTO {db}.log_streams_idx (month, key, val, fingerprint) VALUES {idx}"),
        format!(
            "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) \
             SELECT '{service}', {fp}, ts, 0, body FROM ({rows_sql})"
        ),
    ] {
        admin
            .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .unwrap_or_else(|e| panic!("seed {service}: {e}"));
    }
}

/// **Criterion 23: every refusal lands as the refusal table says** (issue
/// #507, §5.5).
///
/// The reduced route (issue #624, parts 4 and 4b) reproduces every rule
/// that decides one line's outcome and every ceiling on the answer, the
/// scan or the statement; it does not reproduce the ceilings on buffers only
/// today's route allocates. Each row runs the query on the reduced route
/// and on today's route (the same query with `| zzz=""` after the unwrap,
/// which keeps every line and takes today's route):
///
/// ```text
/// row  data                                                 today's route               reduced route
/// R1   6,213 {"latency":1} at one nanosecond                 {} 6213                     {} 6213
/// R1   6,214 at one nanosecond                              {} 6214 (see below)         {} 6214
/// R2   4,100,000 {"latency":1} within 50 s                  422 retained window points  {} 4100000
/// R3   3,400 lines, each its own request_id, one per second one series (see below)     one series, 3,400 points of 1
///      over an hour, step 1 s
/// R4   the realistic corpus, 3 minutes                       three points (see below)    three points (the plan's figures)
/// R5   501 values of r under by (r)                         422 series cap              422 series cap
/// R6   the realistic corpus under a scan budget below it    422 scan budget             422 scan budget, no raw scan
/// ```
///
/// R1's second row, R3 and R4 answered `422` on today's route (same-nanosecond
/// staging, result point-slots and retained label bytes) until issue #624,
/// part 4 (F3): under a parent `sum`, `sum_over_time` now takes the sum's
/// grouping as its own, as the reference injects it, so today's route keeps
/// one series per sum group, each line under the sum's labels alone, and
/// answers all three as the reduced route does.
///
/// R9 (the statement text cap) and R10 (the memory ceiling) send the
/// reduced route to today's route once; R10 is
/// `a_reduced_query_out_of_memory_answers_on_todays_route`.
#[tokio::test]
async fn every_refusal_lands_as_the_table_says() {
    skip_unless_live!();
    const MIN: i64 = 60_000_000_000;
    let (admin, _client, db) = group_key_db("gk_refusals").await;
    let base = ((now_ns() - 7_200_000_000_000) / MIN) * MIN;
    // R1's two streams carry the labels the plan measured them under
    // (`{pod="checkout-1000", service_name="checkout"}`), at the same
    // lengths: the staging charge counts each line's rendered labels.
    seed_one_stream(
        &admin,
        &db,
        base,
        2301,
        "checkou1",
        "checkout-1000",
        &format!(
            "SELECT {} AS ts, '{{\"latency\":1}}' AS body FROM numbers(6213)",
            base + 90_000_000_000
        ),
    )
    .await;
    seed_one_stream(
        &admin,
        &db,
        base,
        2302,
        "checkou2",
        "checkout-1000",
        &format!(
            "SELECT {} AS ts, '{{\"latency\":1}}' AS body FROM numbers(6214)",
            base + 90_000_000_000
        ),
    )
    .await;
    seed_one_stream(&admin, &db, base, 2303, "r2", "",
        &format!("SELECT {} + intDiv(number * 50000000000, 4100000) AS ts, '{{\"latency\":1}}' AS body FROM numbers(4100000)", base + 1_000_000_000)).await;
    seed_one_stream(&admin, &db, base, 2304, "r3", "",
        &format!("SELECT {base} + (number + 1) * 1000000000 AS ts, concat('{{\"request_id\":\"', toString(number), '\",\"latency\":1}}') AS body FROM numbers(3400)")).await;
    seed_one_stream(&admin, &db, base, 2305, "r5", "",
        &format!("SELECT {} + number AS ts, concat('{{\"latency\":1,\"r\":\"', toString(number), '\"}}') AS body FROM numbers(501)", base + 30_000_000_000)).await;
    // The realistic corpus's first 3 minutes and a little more.
    seed_realistic_corpus(&admin, &db, base, 120_000, false).await;

    // R2's control is the slowest query in this file by a wide margin: the
    // retention cap is 4,000,000 points (`charge::MAX_RETAINED_WINDOW_POINTS`)
    // and the row's stream holds 4,100,000 samples, so today's route reads
    // 4,000,001 of them before it can refuse. The deadline therefore has to
    // outlast that scan wherever the suite runs. At this file's 60 s it was a
    // race the machine decided: it passed here and CI answered
    // `timeout: query_stream exceeded 60s` for that one row instead of
    // `422 MetricRetention`. Measured here with a deliberately short deadline,
    // the failure reproduces exactly, and at 300 s the row answers on both.
    const REFUSAL_TABLE_DEADLINE: Duration = Duration::from_secs(300);
    let engine = |budget: u64| {
        let db = db.clone();
        async move {
            LogQlEngine::new(
                data_client_with_deadline(&db, REFUSAL_TABLE_DEADLINE).await,
                engine_config(&db, budget),
                pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            )
        }
    };
    let wide = engine(50 * 1024 * 1024 * 1024).await;
    let range = |start_s: i64, end_s: i64, step: i64| QueryParams {
        spec: QuerySpec::Range {
            start_ns: base + start_s * 1_000_000_000,
            end_ns: base + end_s * 1_000_000_000,
            step_ns: (step * 1_000_000_000) as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let is_reduced = |q: &str, p: &QueryParams| {
        matches!(
            plan(&parse(q).expect("parse"), p, &plan_ctx(&db)).expect("plan"),
            Plan::Metric(mp) if matches!(mp.value, sql::MetricValue::Reduced(_))
        )
    };
    let sum_by = |svc: &str, extra: &str, r: &str| {
        format!(
            r#"sum by (service_name) (sum_over_time({{service_name="{svc}"}} | json | unwrap latency{extra} [{r}]))"#
        )
    };
    let short = |res: Result<(QueryResult, pulsus_read::Warnings), ReadError>| -> String {
        match res {
            Err(ReadError::QueryTooBroad(reason)) => {
                let name = format!("{reason:?}");
                format!("422 {}", name.split([' ', '{', '(']).next().unwrap_or(""))
            }
            Err(e) => format!("error {e}"),
            Ok((QueryResult::Matrix(m), _)) => m
                .iter()
                .map(|s| {
                    let vals: Vec<String> =
                        s.points.iter().map(|(_, v)| format!("{v:?}")).collect();
                    format!("{} {}", group_key_labels(&s.labels), vals.join(","))
                })
                .collect::<Vec<_>>()
                .join("; "),
            Ok((other, _)) => format!("unexpected {other:?}"),
        }
    };

    // R1 and R2: (reduced-route query, today's control, params, today's answer, reduced answer)
    for (svc, params, today_want, key_want) in [
        ("checkou1", range(60, 120, 60), "{} 6213.0", "{} 6213.0"),
        ("checkou2", range(60, 120, 60), "{} 6214.0", "{} 6214.0"),
        (
            "r2",
            range(60, 120, 60),
            "422 MetricRetention",
            "{} 4100000.0",
        ),
    ] {
        let key_q = sum_by(svc, "", "1m");
        let today_q = sum_by(svc, r#" | zzz="""#, "1m");
        assert!(is_reduced(&key_q, &params), "{key_q}: the reduced route");
        assert!(!is_reduced(&today_q, &params), "{today_q}: today's route");
        assert_eq!(
            short(wide.query(&parse(&today_q).expect("parse"), &params).await),
            today_want,
            "{svc}: today's route"
        );
        assert_eq!(
            short(wide.query(&parse(&key_q).expect("parse"), &params).await),
            key_want,
            "{svc}: the reduced route"
        );
    }

    // R3: one series of 3,400 points, each 1, on both routes.
    {
        let params = range(1, 3600, 1);
        let key_q = sum_by("r3", "", "1s");
        let today_q = sum_by("r3", r#" | zzz="""#, "1s");
        assert!(is_reduced(&key_q, &params), "R3: the reduced route");
        for (route, q) in [("today's route", &today_q), ("the reduced route", &key_q)] {
            match wide.query(&parse(q).expect("parse"), &params).await {
                Ok((QueryResult::Matrix(m), _)) => {
                    assert_eq!(m.len(), 1, "R3, {route}: one series");
                    assert_eq!(m[0].points.len(), 3400, "R3, {route}: 3,400 points");
                    assert!(
                        m[0].points.iter().all(|(_, v)| *v == 1.0),
                        "R3, {route}: each 1"
                    );
                }
                other => panic!("R3: {route} answers: {other:?}"),
            }
        }
    }

    // R4: the realistic corpus over 3 minutes.
    {
        let params = range(60, 180, 60);
        let key_q = sum_by("checkout", "", "1m");
        let today_q = sum_by("checkout", r#" | zzz="""#, "1m");
        assert!(is_reduced(&key_q, &params), "R4: the reduced route");
        for (route, q) in [("today's route", &today_q), ("the reduced route", &key_q)] {
            match wide.query(&parse(q).expect("parse"), &params).await {
                Ok((QueryResult::Matrix(m), _)) => {
                    assert_eq!(m.len(), 1, "R4, {route}: one series");
                    let got: Vec<f64> = m[0].points.iter().map(|(_, v)| *v).collect();
                    let want = [14_999_019.46, 14_998_940.27, 15_001_940.27];
                    assert_eq!(got.len(), 3, "R4, {route}: three points: {got:?}");
                    for (g, w) in got.iter().zip(want) {
                        assert!(
                            (g - w).abs() < 0.005,
                            "R4, {route}: {got:?} against the plan's {want:?}"
                        );
                    }
                }
                other => panic!("R4: {route} answers: {other:?}"),
            }
        }
    }

    // R5: the series cap on both routes.
    {
        let params = range(60, 60, 60);
        let key_q = r#"avg_over_time({service_name="r5"} | json | unwrap latency [1m]) by (r)"#;
        let today_q =
            r#"avg_over_time({service_name="r5"} | json | unwrap latency | zzz="" [1m]) by (r)"#;
        assert!(is_reduced(key_q, &params), "R5: the reduced route");
        assert_eq!(
            short(wide.query(&parse(today_q).expect("parse"), &params).await),
            "422 MetricSeries",
            "R5: today's route"
        );
        assert_eq!(
            short(wide.query(&parse(key_q).expect("parse"), &params).await),
            "422 MetricSeries",
            "R5: the reduced route"
        );
    }

    // R6: the scan budget on both routes, and no raw scan after the reduced
    // statement's 307.
    {
        let params = range(60, 180, 60);
        let tight = engine(1_000_000).await;
        let key_q =
            r#"avg_over_time({service_name="checkout"} | json | unwrap latency [1m]) by (service)"#;
        let today_q = r#"avg_over_time({service_name="checkout"} | json | unwrap latency | zzz="" [1m]) by (service)"#;
        assert!(is_reduced(key_q, &params), "R6: the reduced route");
        assert_eq!(
            short(tight.query(&parse(today_q).expect("parse"), &params).await),
            "422 ScanBudgetBytes",
            "R6: today's route"
        );
        let from = server_micros(&admin).await;
        assert_eq!(
            short(tight.query(&parse(key_q).expect("parse"), &params).await),
            "422 ScanBudgetBytes",
            "R6: the reduced route"
        );
        let to = server_micros(&admin).await + 1;
        assert_eq!(
            group_key_statements_between(&admin, &db, from, to, 1).await,
            vec![(GroupKeyStatement::Reduced, 307)],
            "R6: the reduced statement meets the budget and no second read runs"
        );
    }
    drop_group_key_db(&admin, &db).await;
}

// ---------------------------------------------------------------------
// Issue #545 — `docs/query-to-sql.md` §1.3: the selection operators
// contribute no SQL on any route
//
// §1.3's row says `topk`, `bottomk`, `approx_topk`, `sort` and
// `sort_desc` become no SQL and are evaluated after the read. Before this
// issue nothing checked that sentence: the row was rewritten to claim a
// `LIMIT 3 BY bucket_ns` and the three suites that read the document —
// 34 tests — stayed green.
//
// **Five assertions, and no assertion takes its expected value from the
// engine under test.**
//
//     A   §1.3's row, "SQL emitted today" column, is the literal `none`
//         <- docs/query-to-sql.md, read from disk at run time
//     B1  the committed statements carry no selection clause
//         <- a closed vocabulary written as literals below
//     B2  the committed statements hash to PINNED_SELECTION_STATEMENTS
//         <- that constant, in this source file
//     B3  every query — the base and every wrapping — issues exactly
//         its route's committed statements, byte for byte
//         <- crates/pulsus-read/tests/golden/logql_selection_statements.txt
//     C   the answers differ as the corpus requires
//         <- the seeded corpus
//
// A, B1 and B2 are hermetic and run before the live gate, so a developer
// with no container still cannot regenerate the committed statements past
// their digest. B3 and C need the container.
//
// **Two of the five call sites §1.3's row names are read, not run**: the
// binary/`variants` tree (`exec.rs:2455`) and the extracted-field
// group-key read (`exec.rs:5534`). Neither is reachable from the three
// routes below, so the row carries them on the source alone and this
// test does not close them.
//
// **What no test can close**, stated rather than left to be found: a
// drift outside B1's vocabulary, regenerated with the digest deliberately
// updated in the same change. The digest is this test's expected value,
// so a change to both is indistinguishable, to any test, from a change
// the author meant. It is caught by a person reading a diff that must
// contain the changed SQL and the changed constant together.
// ---------------------------------------------------------------------

/// The committed statements, relative to the repository root.
const SELECTION_GOLDEN: &str = "crates/pulsus-read/tests/golden/logql_selection_statements.txt";

/// `sha256sum crates/pulsus-read/tests/golden/logql_selection_statements.txt`
///
/// Pinned HERE, in the test source, and deliberately not beside the data
/// — the posture `crates/pulsus-read/tests/golden_sql_freeze.rs` takes
/// for its own corpus. Running `regenerate_the_logql_selection_statements`
/// alone leaves this constant standing and B2 fails, so the author has to
/// move a source line in the same change and the diff carries both.
const PINNED_SELECTION_STATEMENTS: &str =
    "6b4ec3bca0f3784f83aebc61448f0f394e3d2568fbdafc94e9371d5fd4380715";

/// One grid point, on a step boundary: 2030-01-01T00:00:00Z.
///
/// **Fixed, not `now_ns()`**, because a committed statement carries its
/// window bounds and has to be reproducible. It is in the FUTURE, which
/// `now_ns()`'s doc comment does not cover and the retention TTL forces:
/// `log_samples` carries `TTL toDateTime(fromUnixTimestamp64Nano(
/// timestamp_ns)) + INTERVAL 7 DAY DELETE` (`catalog.rs:256`), and a
/// fixed constant in the past is already expired. Measured on 26.3.29.7:
/// 22 rows inserted at a timestamp 14 days old left
/// `SELECT count() FROM log_samples` answering 0.
const SEL_AT_NS: i64 = 1_893_456_000_000_000_000;
const SEL_STEP_NS: i64 = 60_000_000_000;

/// The six streams, their second grouping label, and how many samples
/// each has in the one bucket.
///
/// `pod` exists so that a wrapping can group by something other than
/// `service_name` between two aggregations — the middle-selection shape
/// below. It changes no statement: the selector reads `env`, and
/// `PREWHERE`/`fingerprint IN` are built from the resolved streams.
///
/// Six rather than three, with a deliberate tie at 3, so that `topk(2, …)`
/// drops four series and `bottomk(1, …)` drops five: a row-limiting drift
/// with a small literal changes an answer as well as a statement. It does
/// not close that class — an attacker picks a larger literal, which is
/// what red run 4's `<= 10` does — and B3 against the committed
/// statements is what closes it.
const SEL_STREAMS: [(u64, &str, &str, u32); 6] = [
    (545_001, "c545a", "p1", 6),
    (545_002, "c545b", "p2", 5),
    (545_003, "c545c", "p3", 4),
    (545_004, "c545d", "p4", 3),
    (545_005, "c545e", "p5", 3),
    (545_006, "c545f", "p6", 1),
];

/// One route's committed block.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SelectionBlock {
    route: String,
    query: String,
    request: String,
    /// `(explain stage name, statement text)`, in issue order.
    statements: Vec<(String, String)>,
}

/// What each route drives: its base query, its request, and the seven
/// wrappings of that base.
///
/// The two range routes differ in the ONE stage that decides the route —
/// `| json | __error__=""` goes beyond a line filter, so the first
/// aggregation level cannot lower — and the test asserts their routing
/// reasons differ, so a change that quietly sent both down one path fails
/// rather than passing twice over the same statements.
///
/// **Four wrappings put the selection OUTERMOST and three put it inside.**
/// The chain is stored outer-first and walked with `aggs.iter().rev()`
/// (`post_agg.rs:3018`), so an outermost-only matrix leaves
/// `vector_aggs.last()` — the innermost spec — never a selection, and a
/// change that lowered only an innermost selection would pass. Both
/// nested shapes are ordinary queries that answer `200`:
///
///     sum by (service_name) (topk(2, count_over_time(SEL[1m])))
///     topk(2, sum by (service_name) (bottomk(5, count_over_time(SEL[1m]))))
///     sum by (service_name) (topk(2, sum by (pod) (count_over_time(SEL[1m]))))
///
/// The third has a selection that is neither the first spec nor the last,
/// so a condition indexed at EITHER end misses it. What the check rests on
/// is therefore membership of the chain, not a position in it.
fn selection_routes() -> Vec<(&'static str, String, QueryParams, [String; 7])> {
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: SEL_AT_NS,
            end_ns: SEL_AT_NS,
            step_ns: SEL_STEP_NS as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let instant = QueryParams {
        spec: QuerySpec::Instant { at_ns: SEL_AT_NS },
        limit: 100,
        direction: Direction::Backward,
    };
    // The selector each route reads, and the base query over it. The
    // nested wrappings are built from the SELECTOR rather than by
    // wrapping the base, because their selection sits inside the `sum by`.
    let plain = r#"{env="prod"}"#;
    // The trailing space is deliberate: `__error__=""[1m]` does not parse
    // as a range selector.
    let parsed = r#"{env="prod"} | json | __error__=""#.to_string() + "\" ";
    let base_of = |sel: &str| format!("sum by (service_name) (count_over_time({sel}[1m]))");
    let bucketed = base_of(plain);
    let client = base_of(&parsed);
    // `second` is the operator and its parameter, e.g. `bottomk(1`.
    let six = |sel: &str, second: &str| {
        let base = base_of(sel);
        [
            format!("topk(2, {base})"),
            format!("{second}, {base})"),
            format!("sort({base})"),
            format!("sort_desc({base})"),
            // The selection is the INNERMOST spec: `vector_aggs.last()`.
            format!("sum by (service_name) (topk(2, count_over_time({sel}[1m])))"),
            // A selection at both ends of one chain.
            format!("topk(2, sum by (service_name) (bottomk(5, count_over_time({sel}[1m]))))"),
            // The selection is NEITHER the first spec NOR the last, so a
            // condition indexed at either end misses it in both
            // directions. `sum by (pod)` groups on the corpus's second
            // label, so the middle stage is a real grouping rather than a
            // repeat of the outer one.
            format!("sum by (service_name) (topk(2, sum by (pod) (count_over_time({sel}[1m]))))"),
        ]
    };
    vec![
        (
            "range, bucketed read",
            bucketed.clone(),
            range,
            six(plain, "bottomk(1"),
        ),
        (
            "range, client-aggregated read",
            client,
            range,
            six(&parsed, "bottomk(1"),
        ),
        // `approx_topk` replaces `bottomk` here: it is refused on a range
        // query and this is the only route that can carry it.
        (
            "instant read",
            bucketed,
            instant,
            six(plain, "approx_topk(2"),
        ),
    ]
}

/// The five operators section 1.3's row names.
fn is_selection_op(op: &pulsus_logql::VectorAggOp) -> bool {
    matches!(
        op,
        pulsus_logql::VectorAggOp::Topk
            | pulsus_logql::VectorAggOp::Bottomk
            | pulsus_logql::VectorAggOp::ApproxTopk
            | pulsus_logql::VectorAggOp::Sort
            | pulsus_logql::VectorAggOp::SortDesc
    )
}

/// **What red run 2's rationale rests on, asserted rather than assumed.**
///
/// That run conditions a mutation on the plan carrying a selection, and
/// its evidence is that the route's BASE statement does not move. That
/// holds only while no route's base carries a selection ANYWHERE in its
/// chain — a property of these query strings, not of the code. A base
/// changed to `sum by (pod) (topk(2, count_over_time(…)))` is a valid
/// query on the same route and would silently make that run prove
/// nothing, so it fails here instead, naming the operator it found.
///
/// **Membership, not position.** The seven wrappings put a selection
/// first, last and in the middle, so nothing here may be indexed at an
/// end: this walks the whole chain.
fn every_base_query_carries_no_selection(db: &str) {
    for (route, base, params, _) in selection_routes() {
        let expr = parse(&base).unwrap_or_else(|e| panic!("parse {base}: {e:?}"));
        let Plan::Metric(mp) = plan(&expr, &params, &plan_ctx(db)).expect("plan") else {
            panic!("{route}: the base query must be a metric plan");
        };
        let found: Vec<String> = mp
            .vector_aggs
            .iter()
            .filter(|(op, _, _)| is_selection_op(op))
            .map(|(op, _, _)| format!("{op:?}"))
            .collect();
        assert!(
            found.is_empty(),
            "{route}: the base query {base} carries {found:?} in its chain. Red run 2's \
             evidence is that the base statement does not move under a selection-conditioned \
             mutation, and a base that carries a selection makes that run prove nothing"
        );
    }
}

fn selection_request_line(params: &QueryParams) -> String {
    match params.spec {
        QuerySpec::Range {
            start_ns,
            end_ns,
            step_ns,
        } => format!("range start={start_ns} end={end_ns} step={step_ns}"),
        QuerySpec::Instant { at_ns } => format!("instant time={at_ns}"),
    }
}

/// The one normalisation applied to a live statement before it is
/// compared with the committed one.
///
/// The integers inside `fingerprint IN (...)` arrive in the order the
/// database's `GROUP BY fingerprint` returned them —
/// `resolve_fingerprints` (`exec.rs:969`) pushes rows as they stream and
/// does not sort, where `all_active_fingerprints` sorts for exactly this
/// reason. That order is an execution artefact, not a property of the
/// corpus: measured on 26.3.29.7, one selector over four streams answers
/// `99120 18374 30001 40001` at `max_block_size=1` and
/// `30001 40001 99120 18374` at the default. Sorting pins the SET, which
/// is what the statement means, and leaves nowhere for a selection clause
/// to hide — a clause is not an integer.
fn sort_fingerprint_lists(sql: &str) -> String {
    let needle = "fingerprint IN (";
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(at) = rest.find(needle) {
        let (head, tail) = rest.split_at(at + needle.len());
        out.push_str(head);
        // Each element is `toUInt128('<decimal>')` since issue #498, so the
        // list's own closing parenthesis is the LAST one before the next
        // clause, not the first — the first now closes the first element's
        // call. Scanning to a balanced close is what keeps this a list
        // sorter rather than a text cutter.
        let mut depth = 1usize;
        let mut close = None;
        for (i, c) in tail.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        close = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }
        match close {
            None => {
                rest = tail;
                break;
            }
            Some(close) => {
                let mut ids: Vec<u128> = tail[..close]
                    .split(',')
                    .filter_map(|t| {
                        let t = t.trim();
                        // `toUInt128('123')` or, on a sub-query form, text
                        // that is not an element at all.
                        let inner = t
                            .strip_prefix("toUInt128('")
                            .and_then(|r| r.strip_suffix("')"))
                            .unwrap_or(t);
                        inner.parse().ok()
                    })
                    .collect();
                ids.sort_unstable();
                out.push_str(
                    &ids.iter()
                        .map(|v| format!("toUInt128('{v}')"))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                rest = &tail[close..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// B1's vocabulary: the two clause shapes §2.3 and §2.7.2 name as the
/// designed lowering of a selection, written here as literals.
///
/// - a `LIMIT` with a ` BY ` after it — ClickHouse's per-group row limit;
/// - an `ORDER BY` whose first token in any item is the aggregate alias
///   `n` or the group alias `g0` — the ordering that limit needs in front
///   of it.
///
/// The `ORDER BY` test compares whole tokens, never substrings: every
/// statement here orders by `fingerprint`, and `fingerprint` contains an
/// `n`. The clause is read to the end of the statement or to a following
/// `LIMIT`/`SETTINGS`, whichever comes first.
fn selection_clause(sql: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    if let Some(at) = upper.find("LIMIT ")
        && upper[at..].contains(" BY ")
    {
        return Some(format!(
            "a per-group limit: {:?}",
            sql[at..].lines().next().unwrap_or("")
        ));
    }
    let mut from = 0usize;
    while let Some(rel) = upper[from..].find("ORDER BY ") {
        let at = from + rel + "ORDER BY ".len();
        let tail = &sql[at..];
        let end = ["\nLIMIT", "\nSETTINGS"]
            .iter()
            .filter_map(|k| tail.find(k))
            .min()
            .unwrap_or(tail.len());
        for item in tail[..end].split(',') {
            let token = item.split_whitespace().next().unwrap_or("");
            if token == "n" || token == "g0" {
                return Some(format!(
                    "an ordering on the selection's alias {token:?}: {:?}",
                    tail[..end].lines().next().unwrap_or("")
                ));
            }
        }
        from = at;
    }
    None
}

fn render_selection_golden(blocks: &[SelectionBlock]) -> String {
    let mut out = String::new();
    out.push_str(SELECTION_GOLDEN_HEADER);
    for b in blocks {
        out.push_str(&format!("\n=== route: {}\n", b.route));
        out.push_str(&format!("--- query\n{}\n", b.query));
        out.push_str(&format!("--- request\n{}\n", b.request));
        for (i, (name, sql)) in b.statements.iter().enumerate() {
            out.push_str(&format!("--- statement {} {name}\n{sql}\n", i + 1));
        }
    }
    out
}

const SELECTION_GOLDEN_HEADER: &str = "\
# The statements the three LogQL routes issue for `docs/query-to-sql.md` §1.3's
# selection row, committed so that the check compares live SQL with bytes this
# engine did not produce at test time.
#
# GENERATED by the ignored `regenerate_the_logql_selection_statements` in
# crates/pulsus-read/tests/query_log_gates.rs. Not hand-written: the digest
# pinned there as PINNED_SELECTION_STATEMENTS has to move in the same change,
# and that pairing in one diff is the review.
#
# ONE normalisation is applied to a live statement before it is compared: the
# integers inside `fingerprint IN (...)` are sorted ascending. Their order is
# the database's grouping order and moves with settings; the SET is what the
# statement means and it is still pinned. `sort_fingerprint_lists` carries the
# measurement.
";

fn parse_selection_golden(text: &str) -> Vec<SelectionBlock> {
    let mut blocks: Vec<SelectionBlock> = Vec::new();
    let mut section: Option<(String, String)> = None;
    let mut body: Vec<&str> = Vec::new();
    let flush = |blocks: &mut Vec<SelectionBlock>,
                 section: &Option<(String, String)>,
                 body: &mut Vec<&str>| {
        // The blank line the renderer puts between two blocks belongs to
        // neither of them.
        while body.last() == Some(&"") {
            body.pop();
        }
        let text = body.join("\n");
        body.clear();
        let Some((kind, arg)) = section else { return };
        let b = blocks.last_mut().expect("a section before any route");
        match kind.as_str() {
            "query" => b.query = text,
            "request" => b.request = text,
            "statement" => b.statements.push((arg.clone(), text)),
            other => panic!("{SELECTION_GOLDEN}: unknown section {other:?}"),
        }
    };
    for line in text.lines() {
        if let Some(route) = line.strip_prefix("=== route: ") {
            flush(&mut blocks, &section, &mut body);
            section = None;
            blocks.push(SelectionBlock {
                route: route.to_string(),
                query: String::new(),
                request: String::new(),
                statements: Vec::new(),
            });
            continue;
        }
        if let Some(marker) = line.strip_prefix("--- ")
            && (marker == "query" || marker == "request" || marker.starts_with("statement "))
        {
            flush(&mut blocks, &section, &mut body);
            section = Some(if let Some(rest) = marker.strip_prefix("statement ") {
                let name = rest
                    .split_once(' ')
                    .map(|(_, n)| n.to_string())
                    .unwrap_or_else(|| panic!("{SELECTION_GOLDEN}: statement marker {marker:?}"));
                ("statement".to_string(), name)
            } else {
                (marker.to_string(), String::new())
            });
            continue;
        }
        if section.is_none() {
            // The header, before the first route.
            continue;
        }
        body.push(line);
    }
    flush(&mut blocks, &section, &mut body);
    blocks
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn repo_file(rel: &str) -> String {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    std::fs::read_to_string(root.join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
}

/// The cells of one markdown table row, respecting `\|` inside a cell.
fn table_cells(line: &str) -> Vec<String> {
    let mut cells = vec![String::new()];
    let mut escaped = false;
    for c in line.chars() {
        match c {
            '\\' if !escaped => {
                escaped = true;
                cells.last_mut().expect("a cell").push(c);
            }
            '|' if !escaped => cells.push(String::new()),
            _ => {
                escaped = false;
                cells.last_mut().expect("a cell").push(c);
            }
        }
    }
    cells
}

/// §1.3's selection row, located rather than restated.
///
/// Finding it is part of the contract: a moved section, a reworded
/// heading or a second matching row each yield a row count that is not
/// one, and that is a failure with its own message rather than a check
/// that quietly stops checking anything.
fn selection_row_cells(doc: &str) -> Vec<Vec<String>> {
    const HEADING: &str = "### 1.3 LogQL — the parts that are not stages";
    let mut inside = false;
    let mut rows = Vec::new();
    for line in doc.lines() {
        if line.starts_with("### ") {
            inside = line == HEADING;
            continue;
        }
        if inside && line.starts_with('|') {
            let cells = table_cells(line);
            if cells.len() >= 4 && cells[1].contains("`topk(k, …)`") {
                rows.push(cells);
            }
        }
    }
    rows
}

async fn seed_selection_corpus() -> (ChClient, String) {
    let db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_sel_{}",
        uuid::Uuid::new_v4().simple()
    ));
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop");
    admin
        .execute(
            &format!("CREATE DATABASE {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create");
    run_init(&admin, &test_ctx(&db)).await.expect("run_init");
    // **The retention TTL comes off this table before anything is
    // seeded.** `test_ctx` renders `TTL toDateTime(fromUnixTimestamp64Nano(
    // timestamp_ns)) + INTERVAL 7 DAY DELETE` (`catalog.rs:256`), and this
    // fixture's timestamps are a FIXED constant, so the fixture would
    // start expiring once the wall clock passed it — a check that goes red
    // on a date rather than on a change. Measured on 26.3.29.7 over two
    // tables with this DDL and one row 14 days old: 0 rows with the TTL,
    // 1 row with it removed. `log_streams` (`catalog.rs:209`) and
    // `log_streams_idx` (`catalog.rs:227`) carry no TTL, so this one
    // statement covers the fixture.
    admin
        .execute(
            &format!("ALTER TABLE {db}.log_samples REMOVE TTL"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("remove the retention TTL from the fixture table");
    // **Asserted, not assumed.** The statement above is the only thing
    // standing between this fixture and a check that reddens on a date;
    // taking it away reddens here instead, at once and on any machine.
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct DdlRow {
        ddl: String,
    }
    let mut ddl = String::new();
    {
        let mut stream = admin
            .query_stream::<DdlRow>(
                &format!(
                    "SELECT create_table_query AS ddl FROM system.tables WHERE database = \
                     '{db}' AND name = 'log_samples'"
                ),
                &QuerySettings::new(),
            )
            .await
            .expect("read the fixture table's DDL");
        while let Some(row) = stream.next().await {
            ddl = row.expect("decode the DDL row").ddl;
        }
    }
    assert!(
        !ddl.is_empty(),
        "the fixture's log_samples has no DDL to read"
    );
    // Upper case, so the `ttl_only_drop_parts` setting in the same text is
    // not mistaken for a TTL clause.
    assert!(
        !ddl.contains("TTL "),
        "the fixture's log_samples still carries a TTL clause: {ddl}. Its timestamps are a \
         fixed constant, so the fixture would expire once the wall clock passed it"
    );
    let client = data_client(&db).await;

    let mut rows = Vec::new();
    for (fp, service, pod, count) in SEL_STREAMS {
        let labels = format!(r#"{{"env":"prod","pod":"{pod}","service_name":"{service}"}}"#);
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, \
                     updated_ns) VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({\
                     SEL_AT_NS}))), {fp}, '{service}', '{labels}', 0)"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed log_streams");
        land_seeded_streams(&client, &db).await;
        for i in 0..count {
            rows.push(BucketedSeedRow {
                org_id: String::new(),
                service: service.to_string(),
                fingerprint: u128::from(fp),
                // Inside the one window `(T - 60s, T]`, and every body is
                // JSON so `| json | __error__=""` keeps every line and the
                // two range routes count the same rows.
                timestamp_ns: SEL_AT_NS - 30_000_000_000 + i64::from(i) * 1_000_000_000,
                severity: 0,
                // **Every body is the same JSON**, so `| json` adds the
                // same label to every line and the client-aggregated
                // route's series are one per stream, as the bucketed
                // route's are. A body that varied would split each
                // stream's lines into one series per value, and the two
                // routes would answer differently for a selection applied
                // BEFORE the `sum by` — which is what the nested
                // wrappings drive.
                body: r#"{"ok":1}"#.to_string(),
                structured_metadata: String::new(),
            });
        }
    }
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("insert the selection fixture");
    land_seeded_lines(&client, &db).await;
    (admin, db)
}

/// What one query issued and answered.
struct SelectionRun {
    query: String,
    statements: Vec<(String, String)>,
    result: QueryResult,
}

/// Every query's statements and answer, per route, from a live engine.
///
/// **The base query gets no special treatment here.** Its statements are
/// what the regenerator commits, and the test compares every query —
/// base and wrappings alike — against the COMMITTED file, never against
/// the base's live statements. Comparing a wrapping with the base would
/// take its expected value from the engine under test, and a drift
/// applied to both moves them together.
async fn observed_selection_blocks(db: &str) -> Vec<(SelectionBlock, Vec<SelectionRun>)> {
    let engine = LogQlEngine::new(
        data_client(db).await,
        engine_config(db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let mut out = Vec::new();
    for (route, base, params, wrappings) in selection_routes() {
        let mut runs: Vec<SelectionRun> = Vec::new();
        let mut block = SelectionBlock {
            route: route.to_string(),
            query: base.clone(),
            request: selection_request_line(&params),
            statements: Vec::new(),
        };
        for (n, query) in std::iter::once(base.clone())
            .chain(wrappings.iter().cloned())
            .enumerate()
        {
            let expr = parse(&query).unwrap_or_else(|e| panic!("parse {query}: {e:?}"));
            let (result, _warnings, explain) = engine
                .query_explained(&expr, &params)
                .await
                .unwrap_or_else(|e| panic!("{route}: {query}: {e:?}"));
            let statements: Vec<(String, String)> = explain
                .stages
                .iter()
                .map(|s| (s.name.to_string(), sort_fingerprint_lists(&s.sql)))
                .collect();
            if n == 0 {
                block.statements = statements.clone();
                block.request = format!(
                    "{} routing={}",
                    block.request,
                    explain
                        .routing
                        .as_ref()
                        .map(|r| r.reason.clone())
                        .unwrap_or_else(|| "none".to_string())
                );
            }
            runs.push(SelectionRun {
                query,
                statements,
                result,
            });
        }
        out.push((block, runs));
    }
    out
}

/// The labels of a matrix or vector answer, in the order the engine
/// returned them.
fn answer_labels(result: &QueryResult) -> Vec<String> {
    // A series named by its `service_name`, or — when a wrapping grouped
    // on something else and dropped it — by its whole label set, so that
    // `{}` is a readable name for the one unlabelled series the
    // middle-selection wrapping answers.
    let name = |labels: &[(String, String)]| {
        labels
            .iter()
            .find(|(k, _)| k == "service_name")
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| {
                let mut kv: Vec<String> =
                    labels.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
                kv.sort();
                format!("{{{}}}", kv.join(", "))
            })
    };
    match result {
        QueryResult::Matrix(series) => series.iter().map(|s| name(&s.labels)).collect(),
        QueryResult::Vector(samples) => samples.iter().map(|s| name(&s.labels)).collect(),
        other => panic!("a selection answer is a matrix or a vector, not {other:?}"),
    }
}

/// Every value in an answer, in the order the engine returned them.
fn answer_values(result: &QueryResult) -> Vec<f64> {
    match result {
        QueryResult::Matrix(series) => series
            .iter()
            .flat_map(|s| s.points.iter().map(|(_, v)| *v))
            .collect(),
        QueryResult::Vector(samples) => samples.iter().map(|s| s.value).collect(),
        other => panic!("a selection answer is a matrix or a vector, not {other:?}"),
    }
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// **`docs/query-to-sql.md` §1.3's selection row, checked against the
/// code** (issue #545). See this section's comment for the five
/// assertions and where each one's expected value comes from.
#[tokio::test]
async fn the_selection_operators_add_no_clause_to_any_statement() {
    // ---- A: the document's own cell -------------------------------
    let doc = repo_file("docs/query-to-sql.md");
    let rows = selection_row_cells(&doc);
    assert_eq!(
        rows.len(),
        1,
        "docs/query-to-sql.md §1.3 must carry exactly ONE row whose first cell names \
         `topk(k, …)`; found {}. A moved row, a reworded heading or a second such row all \
         land here rather than silently checking nothing",
        rows.len()
    );
    assert_eq!(
        rows[0][2].trim(),
        "none",
        "docs/query-to-sql.md §1.3's selection row says the SQL emitted today is {:?}. \
         This test asserts the statements carry no selection clause, so the row and the \
         code disagree: one of them is wrong",
        rows[0][2].trim()
    );

    // ---- B1: the committed statements, against the vocabulary -----
    let golden_text = repo_file(SELECTION_GOLDEN);
    let golden = parse_selection_golden(&golden_text);
    assert_eq!(
        golden.len(),
        3,
        "{SELECTION_GOLDEN} holds one block per route"
    );
    for block in &golden {
        for (name, sql) in &block.statements {
            assert!(
                selection_clause(sql).is_none(),
                "{SELECTION_GOLDEN}: {}'s {name} statement carries {}. §1.3's row says the \
                 selection becomes no SQL, so a regeneration that absorbed one fails here",
                block.route,
                selection_clause(sql).expect("just checked"),
            );
        }
    }

    // ---- B2: the committed bytes, against the pinned digest --------
    assert_eq!(
        sha256_hex(golden_text.as_bytes()),
        PINNED_SELECTION_STATEMENTS,
        "{SELECTION_GOLDEN} is not the file PINNED_SELECTION_STATEMENTS names. Regenerating \
         it is not a way to make this green: move the constant in the same change and the \
         diff carries both the statements and the pin"
    );

    skip_unless_live!();

    // ---- B3 and C: the live engine ---------------------------------
    let (admin, db) = seed_selection_corpus().await;
    every_base_query_carries_no_selection(&db);
    let observed = observed_selection_blocks(&db).await;

    for (block, runs) in &observed {
        let want = golden
            .iter()
            .find(|g| g.route == block.route)
            .unwrap_or_else(|| panic!("{SELECTION_GOLDEN} has no block for {}", block.route));
        assert_eq!(
            block.query, want.query,
            "{}: the committed base query is not the one this test drives",
            block.route
        );
        assert_eq!(
            block.request, want.request,
            "{}: the committed request is not the one this test makes",
            block.route
        );
        for run in runs {
            assert_eq!(
                run.statements.len(),
                want.statements.len(),
                "{}: {} issued {} statements, {SELECTION_GOLDEN} holds {}",
                block.route,
                run.query,
                run.statements.len(),
                want.statements.len()
            );
            for ((got_name, got_sql), (want_name, want_sql)) in
                run.statements.iter().zip(want.statements.iter())
            {
                assert_eq!(
                    got_name, want_name,
                    "{}: {} moved a stage name",
                    block.route, run.query
                );
                assert_eq!(
                    got_sql, want_sql,
                    "{}: {}'s {got_name} statement is not the committed one. §1.3's row says \
                     the selection becomes no SQL; every query on this route — the base and \
                     every wrapping of it, with the selection outermost, innermost and in the \
                     middle — must issue these bytes",
                    block.route, run.query
                );
            }
        }
    }

    // The two range routes must be two routes.
    let reason = |route: &str| {
        observed
            .iter()
            .find(|(b, _)| b.route == route)
            .map(|(b, _)| b.request.clone())
            .unwrap_or_else(|| panic!("no observed block for {route}"))
    };
    assert_ne!(
        reason("range, bucketed read"),
        reason("range, client-aggregated read"),
        "both range routes reported the same routing reason, so this compares one path with \
         itself"
    );

    // ---- C: the answers ---------------------------------------------
    let all: Vec<String> = SEL_STREAMS
        .iter()
        .map(|(_, s, _, _)| s.to_string())
        .collect();
    for (block, runs) in &observed {
        for run in runs {
            let (query, result) = (&run.query, &run.result);
            let labels = answer_labels(result);
            // `topk(2, …)` and `approx_topk(2, …)` must answer the same
            // two series on this corpus: the sketch is exact well under
            // its own error bound at six series.
            //
            // The two nested forms, over counts 6, 5, 4, 3, 3 and 1: an
            // inner `topk(2, …)` keeps 6 and 5 before the `sum by` groups
            // them; an inner `bottomk(5, …)` drops only the 6, and the
            // outer `topk(2, …)` then takes 5 and 4. No tie decides
            // either answer.
            // The middle-selection wrapping groups by `pod` INSIDE, so
            // its series carry no `service_name`, and the outer
            // `sum by (service_name)` folds all of them into one
            // unlabelled series. Its value is the only thing that says
            // which two pods the selection kept, so it is asserted: 6 + 5.
            let want_values: Option<Vec<f64>> =
                if query.starts_with("sum by (service_name) (topk(2, sum by (pod)") {
                    Some(vec![11.0])
                } else {
                    None
                };
            if let Some(want_values) = want_values {
                assert_eq!(
                    answer_values(result),
                    want_values,
                    "{}: {query} answered the wrong value",
                    block.route
                );
            }
            let want: Vec<String> =
                if query.starts_with("sum by (service_name) (topk(2, sum by (pod)") {
                    vec!["{}".to_string()]
                } else if query.starts_with("sum by (service_name) (topk(2,") {
                    vec!["c545a".to_string(), "c545b".to_string()]
                } else if query.starts_with("topk(2, sum by (service_name) (bottomk(5,") {
                    vec!["c545b".to_string(), "c545c".to_string()]
                } else if query.starts_with("topk(2,") || query.starts_with("approx_topk(2,") {
                    vec!["c545a".to_string(), "c545b".to_string()]
                } else if query.starts_with("bottomk(1,") {
                    vec!["c545f".to_string()]
                } else {
                    all.clone()
                };
            assert_eq!(
                sorted(labels.clone()),
                sorted(want.clone()),
                "{}: {query} answered {labels:?}; the corpus requires {want:?}",
                block.route
            );
        }
    }

    // The one refusal in the family: `approx_topk` on a RANGE query,
    // before any statement is issued.
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: SEL_AT_NS,
            end_ns: SEL_AT_NS,
            step_ns: SEL_STEP_NS as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let refused = engine
        .query_explained(
            &parse(r#"approx_topk(2, sum by (service_name) (count_over_time({env="prod"}[1m])))"#)
                .expect("parse"),
            &range,
        )
        .await;
    match refused {
        Err(ReadError::PipelineInvalid { reason }) => assert_eq!(
            reason, "count min sketches are only supported on instant queries",
            "the range `approx_topk` refusal §1.3's row names"
        ),
        other => panic!("`approx_topk` on a range query must be refused, not {other:?}"),
    }

    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

/// Rewrites `crates/pulsus-read/tests/golden/logql_selection_statements.txt`
/// from a live engine. Ignored, so it never runs in CI.
///
/// **Running it is not a way to make a red check green.** The committed
/// statements are this test's oracle: rewriting them to match whatever
/// the engine has started emitting is the defect the file exists to
/// catch. `PINNED_SELECTION_STATEMENTS` stays where it is until a person
/// has read the diff.
///
/// ```text
/// PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test query_log_gates \
///   -- --ignored regenerate_the_logql_selection_statements
/// ```
#[tokio::test]
#[ignore = "writes crates/pulsus-read/tests/golden/logql_selection_statements.txt"]
async fn regenerate_the_logql_selection_statements() {
    assert!(
        should_run(),
        "the regenerator needs a live ClickHouse: set PULSUS_TEST_CLICKHOUSE=1"
    );
    let (admin, db) = seed_selection_corpus().await;
    let blocks: Vec<SelectionBlock> = observed_selection_blocks(&db)
        .await
        .into_iter()
        .map(|(b, _)| b)
        .collect();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let text = render_selection_golden(&blocks);
    std::fs::write(root.join(SELECTION_GOLDEN), &text).expect("write the committed statements");
    eprintln!(
        "wrote {SELECTION_GOLDEN} ({} bytes), sha256 {}",
        text.len(),
        sha256_hex(text.as_bytes())
    );
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the run database");
}

/// Re-lands the rows a fixture seeded into `log_streams` through
/// `log_landing`, so `log_streams` and `log_streams_idx` are both written by
/// the views that maintain them (issue #603).
///
/// **Why the rows are moved rather than copied.** `log_streams_mv` and
/// `log_streams_idx_mv` both read `log_landing` now, so a fixture that
/// inserted into `log_streams` directly leaves the index a stage-1 or
/// discovery read looks at empty. Landing a copy of those rows would leave
/// two `log_streams` rows per stream, so the seeded rows are staged, the
/// table is emptied, and the stage is landed as kind-1 rows: what the reads
/// then see is exactly what the views produce.
///
/// **Idempotent**: a second call stages whatever is in `log_streams` at that
/// point and lands it again, which is the same set.
async fn land_seeded_streams(client: &ChClient, db: &str) {
    for sql in [
        format!(
            "CREATE TABLE {db}.seed_stage_streams ENGINE = MergeTree ORDER BY tuple() \
             AS SELECT * FROM {db}.log_streams"
        ),
        format!("TRUNCATE TABLE {db}.log_streams"),
        format!("TRUNCATE TABLE {db}.log_streams_idx"),
        format!(
            "INSERT INTO {db}.log_landing \
             (received_ms, kind, service, fingerprint, timestamp_ns, severity, body, \
              structured_metadata, month, labels, updated_ns, pattern, pattern_count) \
             SELECT toUnixTimestamp64Milli(now64(3)), 1, service, fingerprint, 0, 0, '', '', \
                    month, labels, updated_ns, '', 0 \
             FROM {db}.seed_stage_streams"
        ),
        format!("DROP TABLE {db}.seed_stage_streams"),
    ] {
        client
            .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "land seeded streams failed: {e}
SQL:
{sql}"
                )
            });
    }
}

/// Re-lands the rows a fixture seeded into `log_samples` through
/// `log_landing`, so `log_samples` and `log_metrics_<res>` are both written
/// by the views that maintain them (issue #603) — the same move
/// [`land_seeded_streams`] makes, for the line kind.
///
/// The rollup is truncated with the samples because its `count`/`bytes` are
/// `SimpleAggregateFunction(sum, UInt64)`: landing the same lines twice
/// without emptying it would double every count.
#[allow(dead_code)]
async fn land_seeded_lines(client: &ChClient, db: &str) {
    for sql in [
        format!(
            "CREATE TABLE {db}.seed_stage_lines ENGINE = MergeTree ORDER BY tuple() \
             AS SELECT * FROM {db}.log_samples"
        ),
        format!("TRUNCATE TABLE {db}.log_samples"),
        format!("TRUNCATE TABLE {db}.log_metrics_5s"),
        format!(
            "INSERT INTO {db}.log_landing \
             (received_ms, kind, service, fingerprint, timestamp_ns, severity, body, \
              structured_metadata, month, labels, updated_ns, pattern, pattern_count) \
             SELECT toUnixTimestamp64Milli(now64(3)), 0, service, fingerprint, timestamp_ns, \
                    severity, body, structured_metadata, toDate(0), '', 0, '', 0 \
             FROM {db}.seed_stage_lines"
        ),
        format!("DROP TABLE {db}.seed_stage_lines"),
    ] {
        client
            .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "land seeded lines failed: {e}
SQL:
{sql}"
                )
            });
    }
}

// ---------------------------------------------------------------------
// Issue #623 — a named read reads its metric; a metric's samples are one
// key range.
// ---------------------------------------------------------------------

async fn exec_623(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

/// A fresh database `db` (composed by the caller through
/// `pulsus_testkit::test_db`), schema applied.
async fn fresh_db_623(db: String) -> (ChClient, String, ChClient) {
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
    run_init(&admin, &test_ctx(&db)).await.expect("run_init");
    let client = data_client_with_deadline(&db, Duration::from_secs(600)).await;
    (admin, db, client)
}

async fn drop_db_623(admin: &ChClient, db: &str) {
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

/// A builder's text as the client sends it: the driver reads `?` as a bind
/// placeholder, and the regex probe carries `(?-s)`.
fn unbound(sql: &str) -> String {
    sql.replace('?', "??")
}

fn eq_matcher(key: &str, value: &str) -> pulsus_read::metrics::LabelMatcher {
    pulsus_read::metrics::LabelMatcher {
        key: key.to_string(),
        op: pulsus_read::metrics::MatchOp::Eq,
        value: value.to_string(),
    }
}

/// The series ID of series `s` of `name`, through the writers' own
/// function.
fn series_id_623(name: &str, s: u32) -> Fingerprint {
    let (labels, _) = pulsus_model::LabelSet::from_normalized([
        ("instance".to_string(), format!("i-{s}")),
        ("job".to_string(), "api".to_string()),
    ]);
    pulsus_model::series_fingerprint(name, &labels)
}

/// Lands one kind-2 row per series in `ids` (a table of `s`, `fingerprint`,
/// `name`) at `hour`, so the views write the lookup and activity rows a push
/// writes.
async fn land_series_623(client: &ChClient, ids: &str, hour: i64, now_ms: i64) {
    exec_623(
        client,
        &format!(
            "INSERT INTO metric_landing \
             (received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
             SELECT {now_ms}, 2, name, fingerprint, {hour}, \
                    concat('{{\"instance\":\"i-', toString(s), '\",\"job\":\"api\"}}'), 0 \
             FROM {ids}"
        ),
    )
    .await;
}

/// Writes the IDs of `count` series under each of `names` into a new table
/// `table` (`s`, `fingerprint`, `name`), computed by
/// [`series_id_623`].
async fn id_table_623(client: &ChClient, table: &str, names: &[String], count: u32) {
    exec_623(
        client,
        &format!(
            "CREATE TABLE {table} (s UInt32, fingerprint UInt128, name String) \
             ENGINE = MergeTree ORDER BY s"
        ),
    )
    .await;
    for name in names {
        let values: Vec<String> = (0..count)
            .map(|s| format!("({s}, {}, '{name}')", series_id_623(name, s).sql_literal()))
            .collect();
        for chunk in values.chunks(10_000) {
            exec_623(
                client,
                &format!(
                    "INSERT INTO {table} (s, fingerprint, name) VALUES {}",
                    chunk.join(", ")
                ),
            )
            .await;
        }
    }
}

/// **Q1 (issue #623): a named read reads its metric.** 400,000 `bg_`
/// series under 1,000 names, all `job="api"`, and `m_q` with 100 series in
/// the same hour. Statements 1, 2 and 3 for `m_q{job="api"}` each read
/// fewer than 100,000 rows: the lookup by the name's key range and the
/// activity by the IDs it yields, which sort together under the name's
/// prefix. A read that scanned either table reads at least 400,000.
#[tokio::test]
async fn a_named_read_reads_its_metric() {
    skip_unless_live!();
    let (admin, db, client) =
        fresh_db_623(pulsus_testkit::test_db("pulsus_read_it_qlg_623_named")).await;
    let now_ms = now_ns() / 1_000_000;
    let hour = (now_ms / 3_600_000) * 3_600_000;
    let bg: Vec<String> = (0..1_000).map(|n| format!("bg_{n}")).collect();
    id_table_623(&client, "ids_bg", &bg, 400).await;
    id_table_623(&client, "ids_q", &["m_q".to_string()], 100).await;
    land_series_623(&client, "ids_bg", hour, now_ms).await;
    land_series_623(&client, "ids_q", hour, now_ms).await;

    let window = pulsus_read::metrics::DataWindow {
        start_ms: hour,
        end_ms: hour,
    };
    let matchers = [eq_matcher("job", "api")];
    let filter = pulsus_read::metrics::DiscoveryFilter {
        metric_name: Some("m_q".to_string()),
        name_matchers: Vec::new(),
        matchers: matchers.to_vec(),
    };
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    for (what, sql, want) in [
        (
            "statement 1",
            pulsus_read::metrics::sql::historical_series_subquery(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                "m_q",
                window,
                &matchers,
            ),
            100,
        ),
        (
            "statement 2",
            pulsus_read::metrics::sql::discovery_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &filter,
                window,
            ),
            100,
        ),
        (
            "statement 3",
            pulsus_read::metrics::sql::discovery_distinct_names_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &filter,
                window,
            ),
            1,
        ),
    ] {
        let wrapped = unbound(&format!("SELECT count() AS n FROM (\n{sql}\n)"));
        let (_, evidence) = run_and_capture::<CountRow623>(
            &client,
            &admin,
            &wrapped,
            &format!("q623-named-{}-{nonce}", what.replace(' ', "-")),
        )
        .await;
        let returned = count_623(&client, &wrapped).await;
        eprintln!(
            "Q1 {what}: returned {returned}, read_rows {}",
            evidence.read_rows
        );
        assert_eq!(returned, want, "{what}: m_q's own answer\n{sql}");
        assert!(
            evidence.read_rows < 100_000,
            "{what} read {} rows: it must read m_q's rows, not every series'\n{sql}",
            evidence.read_rows
        );
    }

    drop_db_623(&admin, &db).await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow623 {
    n: u64,
}

async fn count_623(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<CountRow623>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    stream
        .next()
        .await
        .expect("one row")
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
        .n
}

/// **F6 (issue #623): a metric's samples are one key range.** 200,000
/// series under 200 names, 72 samples each over one UTC day, IDs from the
/// writers' own [`pulsus_model::series_fingerprint`], merged to one part.
/// The cache path's fetch of one name's 1,000 IDs returns its 72,000 samples
/// and reads fewer than 200,000 rows: the name's IDs share a prefix, so its
/// samples sit together under `(fingerprint, unix_milli)`. IDs spread by a
/// uniform hash put the 1,000 series in about 1,000 different granules,
/// at least 6 million rows.
#[tokio::test]
async fn a_metrics_samples_are_one_key_range() {
    skip_unless_live!();
    let (admin, db, client) =
        fresh_db_623(pulsus_testkit::test_db("pulsus_read_it_qlg_623_one_range")).await;
    let now_ms = now_ns() / 1_000_000;
    let day_ms: i64 = 86_400_000;
    let day = (now_ms / day_ms - 1) * day_ms;
    let names: Vec<String> = (0..200).map(|n| format!("m_{n}")).collect();
    id_table_623(&client, "ids", &names, 1_000).await;
    exec_623(
        &client,
        &format!(
            "INSERT INTO metric_samples (fingerprint, unix_milli, value) \
             SELECT fingerprint, {day} + k * 1200000, toFloat64(k) \
             FROM ids ARRAY JOIN range(72) AS k"
        ),
    )
    .await;
    exec_623(&client, "OPTIMIZE TABLE metric_samples FINAL").await;

    let fps: Vec<pulsus_model::FpLiteral> = (0..1_000)
        .map(|s| series_id_623("m_7", s).sql_literal())
        .collect();
    let sql = pulsus_read::metrics::sample_sql::sample_fetch(
        &no_tenant(),
        "metric_samples",
        &fps,
        day - 1,
        day + day_ms,
    );
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let (returned, evidence) = run_and_capture::<pulsus_read::metrics::sample_rows::SampleRow>(
        &client,
        &admin,
        &unbound(&sql),
        &format!("f6-623-{nonce}"),
    )
    .await;
    eprintln!(
        "F6: returned {returned}, read_rows {}, marks {}",
        evidence.read_rows, evidence.selected_marks
    );
    assert_eq!(returned, 72_000, "m_7's 1,000 series, 72 samples each");
    assert!(
        evidence.read_rows < 200_000,
        "one name's samples read {} rows: they are not one key range",
        evidence.read_rows
    );

    drop_db_623(&admin, &db).await;
}

/// Issue #579, T2 (item 2): the issue's query over 24 h at 60 s sends two
/// statements — the shape-A statement, at most one row per group per step
/// plus the sentinel row, and the run template for the inner
/// `count by (cpu)` — and no statement selects the raw samples.
///
/// Without the push the same request sends four statements, a float and a
/// histogram fetch per selector, and 1,478,966 rows (the design's
/// measurement).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_cpu_query_sends_groups_not_samples() {
    skip_unless_live!();
    use pushed_rate_corpus::{ISSUE_QUERY, anchor, cpu_corpus, harness, statements_since};
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_qlog_cpu_query");
    let h = harness(&db, t, &cpu_corpus(t), None).await;
    let p = h.day(60_000);
    let marker = pushed_rate_corpus::server_marker(&h.admin).await;
    let r = pushed_rate_corpus::Harness::run(&h.pushed, ISSUE_QUERY, &p).await;
    assert!(!r.answer.is_empty());
    let sent = statements_since(&h.admin, &h.db, &marker).await;
    let summary: Vec<(u64, String)> = sent
        .iter()
        .map(|s| (s.result_rows, s.query.chars().take(160).collect()))
        .collect();
    assert_eq!(sent.len(), 2, "exactly two statements: {summary:?}");
    let shape_a: Vec<_> = sent
        .iter()
        .filter(|s| s.query.contains("AS range_ms") && s.query.contains("UNION ALL"))
        .collect();
    let runs: Vec<_> = sent
        .iter()
        .filter(|s| s.query.contains("AS lookback") && s.query.contains("gi_start"))
        .collect();
    assert_eq!((shape_a.len(), runs.len()), (1, 1), "{summary:?}");
    assert!(
        shape_a[0].result_rows <= 8 * 1_441 + 1,
        "the shape-A statement returned {} rows, over 8 groups x 1,441 steps + the sentinel",
        shape_a[0].result_rows
    );
    assert!(
        runs[0].result_rows <= 16 * 1_441,
        "the run template returned {} rows, over 16 groups x 1,441 steps",
        runs[0].result_rows
    );
    assert!(
        sent.iter()
            .all(|s| !s.query.contains("SELECT fingerprint, unix_milli, value")),
        "a statement selects the raw samples: {summary:?}"
    );
    h.finish().await;
}

/// Issue #579 part 2, T2: the Series panel over the last 6 h at 15 s sends
/// only run statements, and fewer than 1,000 rows in all. Without the push
/// it is the two-statement raw fetch of every sample.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_series_panel_sends_runs_not_samples() {
    skip_unless_live!();
    use pushed_rate_corpus::{anchor, harness, statements_since};
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_qlog_series_panel");
    let h = harness(&db, t, &multi_name_corpus::multi_name_corpus(t), None).await;
    let p = pulsus_read::MetricQueryParams {
        start_ms: t - 6 * multi_name_corpus::HOUR_MS,
        end_ms: t,
        step_ms: 15_000,
    };
    let marker = pushed_rate_corpus::server_marker(&h.admin).await;
    let r = pushed_rate_corpus::Harness::run(&h.pushed, multi_name_corpus::SERIES_PANEL, &p).await;
    assert!(!r.answer.is_empty());
    let sent = statements_since(&h.admin, &h.db, &marker).await;
    let summary: Vec<(u64, String)> = sent
        .iter()
        .map(|s| (s.result_rows, s.query.chars().take(160).collect()))
        .collect();
    assert!(!sent.is_empty(), "the request sent no statement");
    assert!(
        sent.iter()
            .all(|s| !s.query.contains("SELECT fingerprint, unix_milli, value")),
        "a statement selects the raw samples: {summary:?}"
    );
    assert!(
        sent.iter()
            .all(|s| s.query.contains("AS lookback") && s.query.contains("gi_start")),
        "every statement is the run template: {summary:?}"
    );
    let rows: u64 = sent.iter().map(|s| s.result_rows).sum();
    assert!(rows < 1_000, "{rows} rows sent: {summary:?}");
    h.finish().await;
}

/// Issue #579 part 2, T3: `sum(rate({__name__=~"node_.+"}[5m]))` sends one
/// shape-A statement per time chunk and no raw fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_name_rate_is_one_statement_per_chunk() {
    skip_unless_live!();
    use pulsus_read::metrics::grouped::{Grid, PUSHED_SERIES_STEPS_PER_STATEMENT, time_chunks};
    use pushed_rate_corpus::{anchor, harness, statements_since};
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_qlog_multi_rate");
    let fx = multi_name_corpus::multi_name_corpus(t);
    let series = fx.iter().filter(|s| s.metric.starts_with("node_")).count();
    let h = harness(&db, t, &fx, None).await;
    let p = pulsus_read::MetricQueryParams {
        start_ms: t - 6 * multi_name_corpus::HOUR_MS,
        end_ms: t,
        step_ms: 15_000,
    };
    let points = u32::try_from((p.end_ms - p.start_ms) / p.step_ms + 1).expect("points");
    let chunks = time_chunks(
        Grid {
            start_ms: p.start_ms,
            step_ms: p.step_ms,
            points,
            lookback_ms: pulsus_promql::DEFAULT_LOOKBACK_MS,
        },
        series,
        PUSHED_SERIES_STEPS_PER_STATEMENT,
    )
    .len();
    let marker = pushed_rate_corpus::server_marker(&h.admin).await;
    let q = "sum(rate({__name__=~\"node_.+\"}[5m]))";
    let r = pushed_rate_corpus::Harness::run(&h.pushed, q, &p).await;
    assert!(!r.answer.is_empty());
    let sent = statements_since(&h.admin, &h.db, &marker).await;
    let summary: Vec<(u64, String)> = sent
        .iter()
        .map(|s| (s.result_rows, s.query.chars().take(160).collect()))
        .collect();
    assert_eq!(
        sent.len(),
        chunks,
        "{series} series x {points} steps is {chunks} statements: {summary:?}"
    );
    assert!(
        sent.iter().all(|s| s.query.contains("AS range_ms")),
        "every statement is shape A: {summary:?}"
    );
    h.finish().await;
}

/// Issue #579 part 3, T2: at 100,000 series, above the label cache's cap,
/// a shape-A node sends its statements one after another, each reading
/// its series through the selector's ID statement: no ID literal, and
/// every statement under 64 KiB.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shape_a_sends_no_id_list() {
    skip_unless_live!();
    use pushed_rate_corpus::{anchor, harness_sql, scale_corpus_sql, statements_since};
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_qlog_p3_no_id_list");
    let h = harness_sql(&db, t, &scale_corpus_sql(t, 100_000, false), None).await;
    let p = h.ten_minutes(60_000);
    for q in [
        "sum(rate(cpu_seconds[5m]))",
        "sum by (mode) (rate(cpu_seconds[5m]))",
    ] {
        let marker = pushed_rate_corpus::server_marker(&h.admin).await;
        let r = pushed_rate_corpus::Harness::run(&h.pushed, q, &p).await;
        assert!(!r.answer.is_empty(), "{q}");
        let sent = statements_since(&h.admin, &h.db, &marker).await;
        let summary: Vec<(u64, usize, String)> = sent
            .iter()
            .map(|s| {
                (
                    s.result_rows,
                    s.query.len(),
                    s.query.chars().take(120).collect(),
                )
            })
            .collect();
        let mut shape_a: Vec<_> = sent
            .iter()
            .filter(|s| s.query.contains("AS range_ms"))
            .collect();
        assert_eq!(
            shape_a.len(),
            2,
            "{q}: 100,000 series x 11 steps is two statements: {summary:?}"
        );
        for s in &shape_a {
            assert!(
                !s.query.contains("toUInt128("),
                "{q}: an ID literal in a shape-A statement: {summary:?}"
            );
            assert!(
                s.query.len() < 64 * 1024,
                "{q}: a shape-A statement of {} bytes",
                s.query.len()
            );
        }
        shape_a.sort_by_key(|s| s.start_us);
        for pair in shape_a.windows(2) {
            assert!(
                pair[1].start_us >= pair[0].end_us,
                "{q}: two statements of one node ran at once: {} started before {} ended",
                pair[1].start_us,
                pair[0].end_us
            );
        }
    }
    h.finish().await;
}

/// The single-tenant deployment's tenant: no `X-Scope-OrgID`.
#[allow(dead_code)]
fn no_tenant() -> pulsus_model::Tenant {
    pulsus_model::Tenant::from_header(None, false).expect("no header is the empty tenant")
}

// ---------------------------------------------------------------------
// Issue #624, part 3a: counting after a `regexp` parser, in the database;
// every LogQL regex reads as the database reads it.
// ---------------------------------------------------------------------

/// The regex text the database receives for `pattern`, read back out of the
/// production renderer's `rx` column.
fn rendered_pattern_624p3(pattern: &str) -> String {
    let column = pulsus_read::logql::predicate::regexp_captures_column(&[
        pulsus_read::logql::pipeline::RxSource::Regexp(pattern.to_string()),
    ])
    .unwrap_or_else(|e| panic!("{pattern}: {e}"));
    column
        .as_sql()
        .strip_prefix("[extractGroups(body, ")
        .and_then(|s| s.strip_suffix(")]"))
        .unwrap_or_else(|| panic!("{pattern}: {}", column.as_sql()))
        .to_string()
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct RegexProbeRow624p3 {
    i: u64,
    m: u8,
    g: Vec<String>,
}

/// **T4 (issue #624, part 3a, D7): a regex that runs in PulsusDB reads as
/// the database reads it.** For every pattern and body of §2.2's corpus —
/// Perl classes, boundaries, `.`, case folding with `K`, `ſ`, `σ`, `straße`;
/// bodies with `é`, `٣`, U+00A0, U+2028, U+000B, U+0085, `\n`, `\r\n` —
/// `compile_regex_as_database(p)` agrees with ClickHouse's `match()` on
/// whether it matches and with `extractGroups` on what it captures, over the
/// literal the read path renders.
#[tokio::test]
async fn in_process_regexes_read_as_the_database() {
    skip_unless_live!();
    let patterns: [&str; 34] = [
        r"\w",
        r"\W",
        r"\d",
        r"\D",
        r"\s",
        r"\S",
        r"\b\w+\b",
        r"\B",
        r"^\w+$",
        r".",
        r"^.$",
        r"a.b",
        r"^.*$",
        r"(?i)k",
        r"(?i)K",
        r"(?i)s",
        r"(?i)ſ",
        r"(?i)σ",
        r"(?i)Σ",
        r"(?i)straße",
        r"(?i)STRASSE",
        r"[\w]",
        r"[^\w]",
        r"[\d.]",
        r"[\s]",
        r"\pL",
        r"[[:alpha:]]",
        r"(?P<w>\w+)",
        r"(?P<d>\d+)",
        r"(?P<x>.+)",
        r"(?P<s>\S+)\s(?P<t>\S+)",
        r"x\b",
        r"(?m)^b",
        r"(?s).",
    ];
    let bodies: [&str; 32] = [
        "",
        "a",
        "é",
        "café",
        "٣",
        "12",
        "x y",
        "a\u{00A0}b",
        "a\u{2028}b",
        "a\u{000B}b",
        "a\u{0085}b",
        "a\nb",
        "a\r\nb",
        "\n",
        "K",
        "k",
        "\u{212A}",
        "ſ",
        "s",
        "S",
        "σ",
        "Σ",
        "ς",
        "straße",
        "STRASSE",
        "Straße",
        "\u{1E9E}",
        "x",
        "xé",
        "éx",
        "日本",
        "a_b",
    ];
    // Three cells are out of the corpus: `\B` against a body whose only
    // non-boundary is between two bytes of one multi-byte character. RE2
    // matches a zero-width `\B` there (`match('a\xC2\xA0b', '\B')` is 1),
    // and an in-process regex over text cannot match inside a character.
    // The database's answer is the answer; docs/api.md §9.2 records the
    // difference as a documented limit (issue #624, part 3a).
    let outside: [(&str, &str); 3] = [
        (r"\B", "a\u{00A0}b"),
        (r"\B", "a\u{2028}b"),
        (r"\B", "a\u{0085}b"),
    ];
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    let list = bodies
        .iter()
        .map(|b| literal(b).as_sql().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let (mut match_diffs, mut capture_diffs, mut capture_cells) = (0usize, 0usize, 0usize);
    let mut shown: Vec<String> = Vec::new();
    for pattern in patterns {
        let re = pulsus_read::logql::pipeline::compile_regex_as_database(pattern)
            .unwrap_or_else(|e| panic!("{pattern}: {e}"));
        let groups = re.captures_len() > 1;
        let lit = rendered_pattern_624p3(pattern);
        let g = if groups {
            format!("extractGroups(bs[i], {lit})")
        } else {
            "CAST([], 'Array(String)')".to_string()
        };
        let sql = format!(
            "SELECT toUInt64(i) AS i, toUInt8(match(bs[i], {lit})) AS m, {g} AS g \
             FROM (SELECT [{list}] AS bs) ARRAY JOIN arrayEnumerate(bs) AS i ORDER BY i"
        );
        let mut stream = admin
            .query_stream::<RegexProbeRow624p3>(&sql.replace('?', "??"), &QuerySettings::new())
            .await
            .unwrap_or_else(|e| panic!("{pattern}: {e}\n{sql}"));
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await {
            rows.push(row.unwrap_or_else(|e| panic!("{pattern}: {e}")));
        }
        drop(stream);
        assert_eq!(rows.len(), bodies.len(), "{pattern}: one row per body");
        for (row, body) in rows.iter().zip(bodies) {
            if outside.contains(&(pattern, body)) {
                continue;
            }
            let ours = re.is_match(body);
            if ours != (row.m == 1) {
                match_diffs += 1;
                if shown.len() < 40 {
                    shown.push(format!(
                        "match {pattern:?} {body:?}: ours {ours}, database {}",
                        row.m
                    ));
                }
            }
            if groups {
                capture_cells += 1;
                let caps: Vec<String> = match re.captures(body) {
                    Some(c) => (1..re.captures_len())
                        .map(|k| c.get(k).map_or(String::new(), |m| m.as_str().to_string()))
                        .collect(),
                    None => Vec::new(),
                };
                if caps != row.g {
                    capture_diffs += 1;
                    if shown.len() < 40 {
                        shown.push(format!(
                            "captures {pattern:?} {body:?}: ours {caps:?}, database {:?}",
                            row.g
                        ));
                    }
                }
            }
        }
    }
    println!(
        "{} match cells, {match_diffs} differ; {capture_cells} capture cells, {capture_diffs} differ",
        patterns.len() * bodies.len() - outside.len()
    );
    assert_eq!(
        (match_diffs, capture_diffs),
        (0, 0),
        "the in-process compile reads as the database:\n{}",
        shown.join("\n")
    );
}

/// The two-stream bucketed fixture, a third stream labelled `proc`, and
/// `syslog`-shaped lines: `name[pid]` processes, a kernel line with no
/// match, bodies with `é`, Greek and ANSI colour codes, different `proc`
/// captures on each stream. Every line sits inside a window at `[30s]`,
/// `[60s]` and `[150s]` on the 60 s grid from `T`.
async fn seed_regexp_corpus_624p3() -> (ChClient, String, i64) {
    let (admin, db, t) = seed_bucketed_corpus().await;
    let svc = "c507bucket";
    land_stream_624(
        &admin,
        &db,
        t,
        444,
        svc,
        r#"{"app":"d","proc":"p0","service_name":"c507bucket"}"#,
    )
    .await;
    let sec = 1_000_000_000i64;
    for (fp, at, body) in [
        (111u64, 40i64, "sshd[42]: session opened café"),
        (111, 100, "cron[7]: run σ"),
        (111, 170, "kernel: [12.5] audit"),
        (222, 45, "sshd[43]: closed"),
        (222, 110, "systemd[1]: Started Ελληνικά"),
        (222, 230, "\u{1b}[31mERROR\u{1b}[0m dockerd[9]: x"),
        (444, 50, "kubelet[5]: ok"),
        (444, 115, "\u{1b}[32mcron[8]\u{1b}[0m: done"),
        (444, 175, "plain é line"),
        (444, 235, "polkitd[3]: auth"),
    ] {
        land_line_624(&admin, &db, fp, svc, t + at * sec, body, "").await;
    }
    (admin, db, t)
}

/// T1's lowered queries over `sel` at `range`.
fn regexp_lowered_queries_624p3(sel: &str, range: &str) -> Vec<String> {
    let parser = r#"| regexp "(?P<proc>[a-z]+)\\[[0-9]+\\]""#;
    let mut inner: Vec<String> = Vec::new();
    for op in [
        "count_over_time",
        "rate",
        "bytes_over_time",
        "bytes_rate",
        "absent_over_time",
    ] {
        for after in [r#"| proc != """#, r#"| proc=~"s.*""#, "| drop proc"] {
            inner.push(format!("{op}({sel} {parser} {after} [{range}])"));
        }
    }
    for op in ["count_over_time", "rate"] {
        inner.push(format!("{op}({sel} {parser} | decolorize [{range}])"));
    }
    for pattern in [r#"(?P<a>x)?y"#, r#"(?P<a>x)|(?P<b>y)"#, r#"(?P<a>\\w+)"#] {
        inner.push(format!(
            r#"count_over_time({sel} | regexp "{pattern}" [{range}])"#
        ));
    }
    let mut out: Vec<String> = Vec::new();
    for q in &inner {
        out.push(format!("sum by (proc) ({q})"));
        out.push(format!("topk(10, sum by (proc) ({q}))"));
        out.push(format!("count by (proc) ({q})"));
    }
    out.push(format!(
        r#"topk(10, sum by (proc) (count_over_time({sel} | regexp `(?P<proc>[a-zA-Z0-9_.-]+)\[[0-9]+\]` | proc != "" [{range}])))"#
    ));
    out
}

/// **T5 (issue #624, part 3a): a `regexp` count answers what today's route
/// answers, bit for bit**, for every query T1 lowers, at a range equal to,
/// above and below the step; the query log shows the lowered statement.
/// `bytes_over_time` and `bytes_rate` with `| decolorize` after the parser
/// stay on today's route and answer the same.
#[tokio::test]
async fn a_regexp_count_answers_as_todays_route() {
    skip_unless_live!();
    let (admin, db, t) = seed_regexp_corpus_624p3().await;
    let sel = r#"{service_name="c507bucket"}"#;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 240_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mut comments: Vec<(String, bool)> = Vec::new();
    let mut nonempty = 0usize;
    let parser = r#"| regexp "(?P<proc>[a-z]+)\\[[0-9]+\\]""#;
    for range in ["60s", "150s", "30s"] {
        let mut queries: Vec<(String, bool)> = regexp_lowered_queries_624p3(sel, range)
            .into_iter()
            .map(|q| (q, true))
            .collect();
        for op in ["bytes_over_time", "bytes_rate"] {
            queries.push((
                format!("sum by (proc) ({op}({sel} {parser} | decolorize [{range}]))"),
                false,
            ));
        }
        for (query, lowers) in &queries {
            let comment = format!("c624p3t5-{}", uuid::Uuid::new_v4().simple());
            let lowered = run_624(
                &db,
                hooks_624(false, None, None),
                Some(&comment),
                query,
                &params,
            )
            .await;
            let todays = run_624(&db, hooks_624(true, None, None), None, query, &params).await;
            same_outcome_624(query, &lowered, &todays);
            if lowered.as_ref().is_ok_and(|a| !a.is_empty()) {
                nonempty += 1;
            }
            comments.push((comment, *lowers));
        }
    }
    assert!(nonempty > 150, "the corpus answers: {nonempty}");
    let all: Vec<String> = comments.iter().map(|(c, _)| c.clone()).collect();
    let logged = logged_624(&admin, &all).await;
    for (comment, lowers) in &comments {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{comment}: no statement logged"));
        let lowered = rows
            .iter()
            .any(|r| is_lowered_624(&r.query) && r.query.contains("extractGroups"));
        assert_eq!(lowered, *lowers, "{comment}: {rows:#?}");
    }
    drop_db_624(&admin, &db).await;
}

/// **T6 (issue #624, part 3a, D4): a parent that is not a `sum` keeps every
/// capture.** `count by (a)` counts the series below it, which differ in `b`
/// alone, so `b` must reach the group key: two, as today's route answers.
#[tokio::test]
async fn a_parent_count_keeps_every_capture() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3t6_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 4 * 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    land_stream_624(
        &admin,
        &db,
        t,
        7_600_000,
        "c624t6",
        r#"{"service_name":"c624t6"}"#,
    )
    .await;
    land_line_624(&admin, &db, 7_600_000, "c624t6", t + 10 * sec, "x1", "").await;
    land_line_624(&admin, &db, 7_600_000, "c624t6", t + 20 * sec, "x2", "").await;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t + 60 * sec,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let query = r#"count by (a) (count_over_time({service_name="c624t6"} | regexp "(?P<a>[a-z])(?P<b>[0-9])" [60s]))"#;
    let comment = format!("c624p3t6-{}", uuid::Uuid::new_v4().simple());
    let lowered = run_624(
        &db,
        hooks_624(false, None, None),
        Some(&comment),
        query,
        &params,
    )
    .await;
    let todays = run_624(&db, hooks_624(true, None, None), None, query, &params).await;
    same_outcome_624(query, &lowered, &todays);
    let want: Vec<AnswerSeries> = vec![(
        vec![("a".to_string(), "x".to_string())],
        vec![(t + 60 * sec, 2.0f64.to_bits())],
    )];
    assert_eq!(lowered.as_ref().ok(), Some(&want), "{lowered:?}");
    let logged = logged_624(&admin, std::slice::from_ref(&comment)).await;
    assert!(
        logged[&comment]
            .iter()
            .any(|r| is_lowered_624(&r.query) && r.query.contains("extractGroups")),
        "the lowered statement ran: {:#?}",
        logged[&comment]
    );
    drop_db_624(&admin, &db).await;
}

/// **T7 (issue #624, part 3a, D5): a `regexp` count answers wherever
/// today's route answers.** Part 2's cap search over a `regexp` chain, at a
/// range equal to and above the step, with one more stream whose lines lie
/// only in the tail after the last grid point: at today's smallest answering
/// cap the lowered route answers with today's matrix, and the request ran
/// the lowered statement.
#[tokio::test]
async fn a_regexp_count_answers_wherever_todays_route_answers() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3t7_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 4 * 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    seed_cap_streams_624(&admin, &db, t, 200).await;
    land_stream_624(
        &admin,
        &db,
        t,
        7_200_000,
        "c624cap",
        r#"{"app":"solo","extra":"1","pod":"px","service_name":"c624cap"}"#,
    )
    .await;
    for at in [610i64, 620, 630] {
        land_line_624(&admin, &db, 7_200_000, "c624cap", t + at * sec, "x", "").await;
    }
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 630 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mut comments: Vec<String> = Vec::new();
    for range in ["60s", "150s"] {
        let query = format!(
            r#"sum(count_over_time({{service_name="c624cap"}} | regexp "(?P<pod>[a-z0-9]+)" [{range}]))"#
        );
        for (cap, hi) in [
            (Cap624::ResultPoints, 100_000u64),
            (Cap624::GroupBytes, 256 * 1024 * 1024),
        ] {
            let today = min_cap_624(&db, true, cap, &query, &params, hi).await;
            let want = run_624(
                &db,
                hooks_624(true, Some((cap, today)), None),
                None,
                &query,
                &params,
            )
            .await
            .expect("today's route answers at its smallest cap");
            let comment = format!("c624p3t7-{}", uuid::Uuid::new_v4().simple());
            let got = run_624(
                &db,
                hooks_624(false, Some((cap, today)), None),
                Some(&comment),
                &query,
                &params,
            )
            .await;
            assert_eq!(
                got.as_ref().ok(),
                Some(&want),
                "{query} at {cap:?} {today}: {got:?}"
            );
            comments.push(comment);
        }
    }
    let logged = logged_624(&admin, &comments).await;
    for comment in &comments {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{comment}: no statement logged"));
        assert!(
            rows.iter()
                .any(|r| is_lowered_624(&r.query) && r.query.contains("extractGroups")),
            "{comment}: the lowered statement answered: {rows:#?}"
        );
    }
    drop_db_624(&admin, &db).await;
}

/// **T8 (issue #624, part 3a, D5): a `regexp` count that fails for memory
/// is answered by today's route**, which reads the captures from the
/// database too: the lowered statement fails with code 241, then
/// `metric_raw_samples_sliding` runs with `extractGroups`.
#[tokio::test]
async fn a_regexp_count_out_of_memory_answers_on_todays_route() {
    skip_unless_live!();
    let (admin, db, t) = seed_regexp_corpus_624p3().await;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 240_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let query = r#"topk(10, sum by (proc) (count_over_time({service_name="c507bucket"} | regexp `(?P<proc>[a-zA-Z0-9_.-]+)\[[0-9]+\]` | proc != "" [150s])))"#;
    let comment = format!("c624p3t8-{}", uuid::Uuid::new_v4().simple());
    let lowered = run_624(
        &db,
        hooks_624(false, None, Some(1)),
        Some(&comment),
        query,
        &params,
    )
    .await;
    let todays = run_624(&db, hooks_624(true, None, None), None, query, &params).await;
    same_outcome_624(query, &lowered, &todays);
    assert!(
        lowered.as_ref().is_ok_and(|a| !a.is_empty()),
        "an answer: {lowered:?}"
    );
    let logged = logged_624(&admin, std::slice::from_ref(&comment)).await;
    let rows = &logged[&comment];
    let failed = rows
        .iter()
        .position(|r| is_lowered_624(&r.query) && r.exception_code == 241)
        .unwrap_or_else(|| panic!("the lowered statement failed for memory: {rows:#?}"));
    assert!(
        rows[failed + 1..].iter().any(|r| is_todays_624(&r.query)
            && r.query.contains("extractGroups")
            && r.query
                .contains("ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC")),
        "then today's sliding read, with the captures column: {rows:#?}"
    );
    drop_db_624(&admin, &db).await;
}

/// One stream `{s="u"}` with the lines `café 12` and `x 1`, for T11, T15
/// and T16. Returns `(admin, db, T)`; both lines lie in `(T, T + 60s]`.
async fn seed_u_624p3() -> (ChClient, String, i64) {
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3u_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 4 * 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    land_stream_624(
        &admin,
        &db,
        t,
        7_700_000,
        "c624u",
        r#"{"s":"u","service_name":"c624u"}"#,
    )
    .await;
    land_line_624(&admin, &db, 7_700_000, "c624u", t + 10 * sec, "café 12", "").await;
    land_line_624(&admin, &db, 7_700_000, "c624u", t + 20 * sec, "x 1", "").await;
    (admin, db, t)
}

/// The values of label `name` across a log query's streams, sorted.
fn label_values_624p3(streams: &[pulsus_read::logql::StreamResult], name: &str) -> Vec<String> {
    let mut out: Vec<String> = streams
        .iter()
        .filter(|s| !s.entries.is_empty())
        .filter_map(|s| {
            let labels: std::collections::BTreeMap<String, String> =
                serde_json::from_str(&s.labels_json).expect("labels json");
            labels.get(name).cloned()
        })
        .collect();
    out.sort();
    out
}

/// **T11 (issue #624, part 3a, D6 and D7): a LogQL regex answers the same
/// on every route.** `\w` is ASCII in the database, so `café 12` is not
/// `^\w+ \d+$` and `\w+` captures `caf` from it — in a pushed line filter, a
/// line filter after a rewrite, a log query, a tail page, a `regexp` after a
/// rewrite, and a `regexp` count on both of its routes.
#[tokio::test]
async fn a_logql_regex_answers_the_same_on_every_route() {
    skip_unless_live!();
    let (admin, db, t) = seed_u_624p3().await;
    let sec = 1_000_000_000i64;
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t + 60 * sec,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    for query in [
        r#"count_over_time({s="u"} |~ "^\\w+ \\d+$" [1m])"#,
        r#"count_over_time({s="u"} | line_format "{{__line__}}" |~ "^\\w+ \\d+$" [1m])"#,
    ] {
        let got = run_624(&db, hooks_624(false, None, None), None, query, &range)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let total: f64 = got
            .iter()
            .flat_map(|(_, p)| p.iter().map(|(_, v)| f64::from_bits(*v)))
            .sum();
        assert_eq!(total, 1.0, "{query}: only `x 1` matches: {got:?}");
    }

    let logs = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let want = vec!["caf".to_string(), "x".to_string()];
    let engine_with = |comment: &str| {
        let comment = comment.to_string();
        let db = db.clone();
        async move {
            LogQlEngine::new(
                data_client(&db).await,
                engine_config(&db, 64 * 1024 * 1024),
                pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            )
            .with_query_log_comment(comment)
        }
    };
    let mut database_routes: Vec<(String, &str)> = Vec::new();
    for (query, database) in [
        (r#"{s="u"} | regexp "(?P<w>\\w+)""#, true),
        (
            r#"{s="u"} | line_format "{{__line__}}" | regexp "(?P<w>\\w+)""#,
            false,
        ),
    ] {
        let comment = format!("c624p3t11-{}", uuid::Uuid::new_v4().simple());
        let engine = engine_with(&comment).await;
        let (result, _) = engine
            .query(&parse(query).expect("parse"), &logs)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let QueryResult::Streams { items, .. } = result else {
            panic!("{query}: streams");
        };
        assert_eq!(label_values_624p3(&items, "w"), want, "{query}");
        if database {
            database_routes.push((comment, "the log query"));
        }
    }
    {
        let comment = format!("c624p3t11-{}", uuid::Uuid::new_v4().simple());
        let engine = engine_with(&comment).await;
        let mut setup = engine
            .tail_setup(
                &parse(r#"{s="u"} | regexp "(?P<w>\\w+)""#).expect("parse"),
                &logs,
            )
            .expect("a tail setup");
        let page = engine
            .tail_poll(
                &mut setup,
                pulsus_read::logql::exec::TailLower::Start { start_ns: t },
                t + 60 * sec,
                100,
            )
            .await
            .expect("a tail page");
        assert_eq!(
            label_values_624p3(&page.streams, "w"),
            want,
            "the tail page"
        );
        database_routes.push((comment, "the tail page"));
    }
    let count = r#"sum by (w) (count_over_time({s="u"} | regexp "(?P<w>\\w+)" [1m]))"#;
    for todays in [false, true] {
        let comment = format!("c624p3t11-{}", uuid::Uuid::new_v4().simple());
        let got = run_624(
            &db,
            hooks_624(todays, None, None),
            Some(&comment),
            count,
            &range,
        )
        .await
        .unwrap_or_else(|e| panic!("{count}: {e}"));
        let mut ws: Vec<String> = got
            .iter()
            .filter_map(|(l, _)| l.iter().find(|(k, _)| k == "w").map(|(_, v)| v.clone()))
            .collect();
        ws.sort();
        assert_eq!(ws, want, "{count}, today's route {todays}");
        database_routes.push((
            comment,
            if todays {
                "today's route"
            } else {
                "the lowered count"
            },
        ));
    }
    let comments: Vec<String> = database_routes.iter().map(|(c, _)| c.clone()).collect();
    let logged = logged_624(&admin, &comments).await;
    for (comment, route) in &database_routes {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{route}: no statement logged"));
        assert!(
            rows.iter().any(|r| r.query.contains("extractGroups")),
            "{route}: the database ran the regexp: {rows:#?}"
        );
    }
    drop_db_624(&admin, &db).await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow624p3 {
    n: u64,
}

/// One `count()` over `db.table` under `predicate`, with the skip indexes on
/// or off.
async fn count_where_624p3(admin: &ChClient, table: &str, predicate: &str, indexes: bool) -> u64 {
    let settings = if indexes {
        String::new()
    } else {
        " SETTINGS use_skip_indexes = 0".to_string()
    };
    let sql = format!("SELECT count() AS n FROM {table} WHERE {predicate}{settings}");
    let mut stream = admin
        .query_stream::<CountRow624p3>(&sql.replace('?', "??"), &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let row = stream
        .next()
        .await
        .expect("one row")
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    row.n
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainLine624p3 {
    explain: String,
}

/// **T12 (issue #624, part 3a, F2): a pushed line filter returns the rows
/// it returns without the skip indexes.** (a) The n-gram index drops a
/// granule `match()` accepts for `(?i)(denied|refused)`; (b) over §2.3's
/// sweep — 16 shapes × 11 flag placements, 26 bodies, one granule per row —
/// every pattern's pushed predicate counts what it counts unindexed; (c) the
/// fix is per predicate, so a second filter in the same statement keeps its
/// n-gram pruning.
#[tokio::test]
async fn pushed_line_filters_equal_the_unindexed_answer() {
    skip_unless_live!();
    // (a)
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3t12_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 4 * 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    land_stream_624(
        &admin,
        &db,
        t,
        7_800_000,
        "c624f2",
        r#"{"service_name":"c624f2"}"#,
    )
    .await;
    land_line_624(
        &admin,
        &db,
        7_800_000,
        "c624f2",
        t + 10 * sec,
        "audit DENIED open",
        "",
    )
    .await;
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t + 60 * sec,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let query = r#"count_over_time({service_name="c624f2"} |~ "(?i)(denied|refused)" [1m])"#;
    let got = run_624(&db, hooks_624(false, None, None), None, query, &range)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let total: f64 = got
        .iter()
        .flat_map(|(_, p)| p.iter().map(|(_, v)| f64::from_bits(*v)))
        .sum();
    assert_eq!(total, 1.0, "(a) {query}: {got:?}");

    // (b)
    let table = format!("{db}.f2_sweep");
    exec_624(
        &admin,
        &format!(
            "CREATE TABLE {table} (body String, \
             INDEX idx_body_ngrams body TYPE text(tokenizer = ngrams(4))) \
             ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1"
        ),
    )
    .await;
    let bodies: [&str; 26] = [
        "audit DENIED open",
        "audit denied open",
        "Denied",
        "DENIED",
        "access denied",
        "refused",
        "REFUSED",
        "Refused x",
        "connection refused",
        "ERROR",
        "error",
        "Error!",
        "an error here",
        "fail",
        "FAIL",
        "Fail",
        "failed hard",
        "denied refused",
        "auditDENIEDopen",
        "xdeniedy",
        "xREFUSEDy",
        "x refused y",
        "ok",
        "nothing here",
        "panic",
        "success",
    ];
    let values = bodies
        .iter()
        .map(|b| format!("({})", literal(b).as_sql()))
        .collect::<Vec<_>>()
        .join(", ");
    exec_624(
        &admin,
        &format!("INSERT INTO {table} (body) VALUES {values}"),
    )
    .await;
    // `{H}` a flag head at the start, `{M}` after a leading literal, `{A}`
    // inside the first alternation arm.
    let shapes: [&str; 16] = [
        "{H}{M}denied{A}",
        "{H}{M}{A}denied|refused",
        "{H}{M}({A}denied|refused)",
        "{H}{M}(?:{A}denied|refused)",
        "{H}audit {M}({A}denied|refused)",
        "{H}{M}({A}denied|refused) open",
        "{H}{M}((?:{A}denied|refused)|(?:error|fail))",
        "{H}audit {M}({A}denied|refused)?",
        "{H}{M}({A}denied|refused)+",
        "{H}{M}({A}den[i]ed|ref[u]sed)",
        "{H}{M}({A}den.ed|ref.sed)",
        "{H}{M}({A}error|fail)",
        "{H}{M}({A}error|fail|denied|refused)",
        "{H}{M}({A}DENIED|REFUSED)",
        "{H}x{M}({A}denied|refused)y",
        "{H}{M}({A}denied)|(refused)",
    ];
    let fill = |shape: &str, h: &str, m: &str, a: &str| {
        shape.replace("{H}", h).replace("{M}", m).replace("{A}", a)
    };
    let mut patterns: Vec<String> = Vec::new();
    for shape in shapes {
        for placement in 0..11 {
            patterns.push(match placement {
                0 => fill(shape, "", "", ""),
                1 => fill(shape, "(?i)", "", ""),
                2 => format!("(?i:{})", fill(shape, "", "", "")),
                3 => fill(shape, "", "(?i)", ""),
                4 => fill(shape, "", "", "(?i)"),
                5 => fill(shape, "(?i)(?-i)", "", ""),
                6 => fill(shape, "(?s)", "", ""),
                7 => fill(shape, "(?m)", "", ""),
                8 => fill(shape, "(?U)", "", ""),
                9 => fill(shape, "(?-s)", "", ""),
                _ => fill(shape, "(?-s)(?i)", "", ""),
            });
        }
    }
    assert_eq!(patterns.len(), 176);
    let mut differing: Vec<String> = Vec::new();
    for pattern in &patterns {
        let fragment = pulsus_read::logql::predicate::line_filter(&pulsus_logql::LineFilter {
            op: pulsus_logql::LineFilterOp::Regex,
            value: pattern.clone(),
            value_is_ip: false,
            or_matches: Vec::new(),
        })
        .unwrap_or_else(|e| panic!("{pattern}: {e}"));
        let indexed = count_where_624p3(&admin, &table, fragment.as_sql(), true).await;
        let unindexed = count_where_624p3(&admin, &table, fragment.as_sql(), false).await;
        if indexed != unindexed {
            differing.push(format!(
                "{pattern}: {indexed} indexed, {unindexed} unindexed"
            ));
        }
    }
    println!(
        "{} of 176 patterns differ from the unindexed answer",
        differing.len()
    );
    assert!(
        differing.is_empty(),
        "(b) {} patterns lose rows:\n{}",
        differing.len(),
        differing.join("\n")
    );

    // (c)
    let table = format!("{db}.f2_prune");
    exec_624(
        &admin,
        &format!(
            "CREATE TABLE {table} (body String, \
             INDEX idx_body_ngrams body TYPE text(tokenizer = ngrams(4))) \
             ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1"
        ),
    )
    .await;
    exec_624(
        &admin,
        &format!(
            "INSERT INTO {table} (body) SELECT if(number % 50 = 7, \
             concat('app[1]: panic: rare failure ', toString(number)), \
             concat('session opened for user ', toString(number))) FROM numbers(500)"
        ),
    )
    .await;
    let filter = |op, value: &str| {
        pulsus_read::logql::predicate::line_filter(&pulsus_logql::LineFilter {
            op,
            value: value.to_string(),
            value_is_ip: false,
            or_matches: Vec::new(),
        })
        .expect("renders")
    };
    let contains = filter(pulsus_logql::LineFilterOp::Contains, "rare failure");
    let regex = filter(pulsus_logql::LineFilterOp::Regex, "(?i)(panic|refused)");
    let predicate = format!("{} AND {}", contains.as_sql(), regex.as_sql());
    assert_eq!(
        count_where_624p3(&admin, &table, &predicate, true).await,
        10,
        "(c) the ten rare lines"
    );
    let sql = format!("EXPLAIN indexes = 1 SELECT count() FROM {table} WHERE {predicate}");
    let mut stream = admin
        .query_stream::<ExplainLine624p3>(&sql.replace('?', "??"), &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let mut lines = Vec::new();
    while let Some(row) = stream.next().await {
        lines.push(row.expect("an explain line").explain);
    }
    drop(stream);
    let at = lines
        .iter()
        .position(|l| l.contains("Name: idx_body_ngrams"))
        .unwrap_or_else(|| panic!("(c) the n-gram index is used: {lines:#?}"));
    let granules = lines[at..]
        .iter()
        .find_map(|l| l.trim().strip_prefix("Granules: "))
        .unwrap_or_else(|| panic!("(c) its granules: {lines:#?}"));
    let (kept, total) = granules.split_once('/').expect("kept/total");
    let (kept, total): (u64, u64) = (
        kept.trim().parse().expect("kept"),
        total.trim().parse().expect("total"),
    );
    assert!(kept < total, "(c) idx_body_ngrams prunes: {granules}");
    drop_db_624(&admin, &db).await;
}

/// **T14 (issue #624, part 3a, D6): two `regexp` stages read two columns**
/// of one `rx` — on a log query and on today's route for a count with two
/// parsers.
#[tokio::test]
async fn two_regexp_stages_read_two_columns() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3t14_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 4 * 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    land_stream_624(
        &admin,
        &db,
        t,
        7_900_000,
        "c624v",
        r#"{"s":"v","service_name":"c624v"}"#,
    )
    .await;
    land_line_624(&admin, &db, 7_900_000, "c624v", t + 10 * sec, "ab 12", "").await;
    land_line_624(&admin, &db, 7_900_000, "c624v", t + 20 * sec, "cd 34", "").await;
    let column =
        r"[extractGroups(body, '(?P<a>[a-z]+)'), extractGroups(body, '(?P<b>[0-9]+)')] AS rx";
    let stages = r#"| regexp "(?P<a>[a-z]+)" | regexp "(?P<b>[0-9]+)""#;

    let log_comment = format!("c624p3t14-{}", uuid::Uuid::new_v4().simple());
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    )
    .with_query_log_comment(log_comment.clone());
    let logs = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let query = format!(r#"{{s="v"}} {stages}"#);
    let (result, _) = engine
        .query(&parse(&query).expect("parse"), &logs)
        .await
        .expect("answers");
    let QueryResult::Streams { items, .. } = result else {
        panic!("streams");
    };
    let mut pairs: Vec<(String, String)> = items
        .iter()
        .map(|s| {
            let labels: std::collections::BTreeMap<String, String> =
                serde_json::from_str(&s.labels_json).expect("labels json");
            (labels["a"].clone(), labels["b"].clone())
        })
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("ab".to_string(), "12".to_string()),
            ("cd".to_string(), "34".to_string())
        ]
    );

    let count_comment = format!("c624p3t14-{}", uuid::Uuid::new_v4().simple());
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t + 60 * sec,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let count = format!(r#"sum by (a, b) (count_over_time({{s="v"}} {stages} [1m]))"#);
    let got = run_624(
        &db,
        hooks_624(false, None, None),
        Some(&count_comment),
        &count,
        &range,
    )
    .await
    .expect("answers");
    let one = 1.0f64.to_bits();
    let pair = |k: &str, v: &str| (k.to_string(), v.to_string());
    assert_eq!(
        got,
        vec![
            (
                vec![pair("a", "ab"), pair("b", "12")],
                vec![(t + 60 * sec, one)]
            ),
            (
                vec![pair("a", "cd"), pair("b", "34")],
                vec![(t + 60 * sec, one)]
            ),
        ]
    );
    let logged = logged_624(&admin, &[log_comment.clone(), count_comment.clone()]).await;
    for comment in [&log_comment, &count_comment] {
        assert!(
            logged[comment].iter().any(|r| r.query.contains(column)),
            "{comment}: both captures from one column: {:#?}",
            logged[comment]
        );
    }
    drop_db_624(&admin, &db).await;
}

/// **T15 (issue #624, part 3a, D6): an instant `regexp` count reads its
/// captures from the database**, through `metric_raw_samples` on today's
/// route; since part 5 the query itself is counted at one grid point, and
/// that statement runs the captures too.
#[tokio::test]
async fn an_instant_regexp_count_reads_captures_from_the_database() {
    skip_unless_live!();
    let (admin, db, t) = seed_u_624p3().await;
    let comment = format!("c624p3t15-{}", uuid::Uuid::new_v4().simple());
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    )
    .with_query_log_comment(comment.clone());
    let params = QueryParams {
        spec: QuerySpec::Instant {
            at_ns: t + 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let query = r#"sum by (w) (count_over_time({s="u"} | regexp "(?P<w>\\w+)" [1m]))"#;
    let (result, _) = engine
        .query(&parse(query).expect("parse"), &params)
        .await
        .expect("answers");
    let QueryResult::Vector(samples) = result else {
        panic!("a vector: {result:?}");
    };
    let mut got: Vec<(Vec<(String, String)>, f64)> =
        samples.into_iter().map(|s| (s.labels, s.value)).collect();
    got.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        got,
        vec![
            (vec![("w".to_string(), "caf".to_string())], 1.0),
            (vec![("w".to_string(), "x".to_string())], 1.0),
        ]
    );
    // Today's route for the same instant: the instant read with the
    // captures column.
    let todays_comment = format!("c624p3t15-{}", uuid::Uuid::new_v4().simple());
    let todays = run_instant_624p4(
        &db,
        hooks_624(true, None, None),
        Some(&todays_comment),
        query,
        t + 60_000_000_000,
    )
    .await
    .expect("today's route answers");
    let lowered: InstantAnswer624p4 = got
        .iter()
        .map(|(labels, v)| (labels.clone(), v.to_bits()))
        .collect();
    assert_eq!(todays, lowered, "today's route answers the same");
    let logged = logged_624(&admin, &[comment.clone(), todays_comment.clone()]).await;
    assert!(
        logged[&comment]
            .iter()
            .any(|r| is_lowered_624(&r.query) && r.query.contains("extractGroups")),
        "the one-point count with the captures: {:#?}",
        logged[&comment]
    );
    assert!(
        logged[&todays_comment].iter().any(|r| r
            .query
            .contains("ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC")
            && r.query.contains("extractGroups")),
        "metric_raw_samples with the captures column: {:#?}",
        logged[&todays_comment]
    );
    drop_db_624(&admin, &db).await;
}

/// **T16 (issue #624, part 3a, D6): a `variants(...)` tail's `regexp` reads
/// its captures from the database**, and a tail that rewrites the line first
/// runs its own, reading as the database does (D7). One statement, one
/// `extractGroups`.
#[tokio::test]
async fn a_variant_tail_regexp_reads_captures_from_the_database() {
    skip_unless_live!();
    let (admin, db, t) = seed_u_624p3().await;
    let sec = 1_000_000_000i64;
    let comment = format!("c624p3t16-{}", uuid::Uuid::new_v4().simple());
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t + 60 * sec,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let query = r#"variants(sum by (w) (count_over_time({s="u"} | regexp "(?P<w>\\w+)" [1m])), sum by (v) (count_over_time({s="u"} | line_format "z{{__line__}}" | regexp "(?P<v>\\w+)" [1m]))) of ({s="u"} [1m])"#;
    let got = run_624(
        &db,
        hooks_624(false, None, None),
        Some(&comment),
        query,
        &range,
    )
    .await
    .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut labels: Vec<Vec<(String, String)>> = got.into_iter().map(|(l, _)| l).collect();
    labels.sort();
    let pair = |k: &str, v: &str| (k.to_string(), v.to_string());
    assert_eq!(
        labels,
        vec![
            vec![pair("__variant__", "0"), pair("w", "caf")],
            vec![pair("__variant__", "0"), pair("w", "x")],
            vec![pair("__variant__", "1"), pair("v", "zcaf")],
            vec![pair("__variant__", "1"), pair("v", "zx")],
        ]
    );
    let logged = logged_624(&admin, std::slice::from_ref(&comment)).await;
    let reads: Vec<&CommentedRow624> = logged[&comment]
        .iter()
        .filter(|r| is_todays_624(&r.query))
        .collect();
    assert_eq!(reads.len(), 1, "one statement: {reads:#?}");
    assert_eq!(
        reads[0].query.matches("extractGroups").count(),
        1,
        "one pattern: {}",
        reads[0].query
    );
    drop_db_624(&admin, &db).await;
}

/// **T18 (issue #624, part 3a, code review round 1): a `regexp` whose
/// braces RE2 reads as literals answers on every route that takes its
/// captures from the database**, as it does where the stage runs in process.
/// For `a{bbb}c`, `a{,5}` and `a{}` the stream's matching line is captured
/// whole — by a log query, a lowered count and today's route — and each
/// database route ran `extractGroups` with the pattern as written.
#[tokio::test]
async fn a_brace_pattern_answers_on_every_database_route() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3t18_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 4 * 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    land_stream_624(
        &admin,
        &db,
        t,
        7_950_000,
        "c624br",
        r#"{"s":"br","service_name":"c624br"}"#,
    )
    .await;
    for (at, body) in [(10i64, "a{bbb}c"), (20, "a{,5}"), (30, "a{}"), (40, "abc")] {
        land_line_624(&admin, &db, 7_950_000, "c624br", t + at * sec, body, "").await;
    }
    let logs = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t + 60 * sec,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    // (comment, query, the lowered statement's `extractGroups` when the
    // request must have run it)
    let mut comments: Vec<(String, String, Option<String>)> = Vec::new();
    for literal_text in ["a{bbb}c", "a{,5}", "a{}"] {
        let stage = format!(r#"| regexp "(?P<x>{literal_text})""#);
        let want = vec![literal_text.to_string()];

        let comment = format!("c624p3t18-{}", uuid::Uuid::new_v4().simple());
        let engine = LogQlEngine::new(
            data_client(&db).await,
            engine_config(&db, 64 * 1024 * 1024),
            pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        )
        .with_query_log_comment(comment.clone());
        let query = format!(r#"{{s="br"}} {stage} | x != """#);
        let (result, _) = engine
            .query(&parse(&query).expect("parse"), &logs)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let QueryResult::Streams { items, .. } = result else {
            panic!("{query}: streams");
        };
        assert_eq!(label_values_624p3(&items, "x"), want, "{query}");
        comments.push((comment, query, None));

        let count = format!(r#"sum by (x) (count_over_time({{s="br"}} {stage} | x != "" [1m]))"#);
        for todays in [false, true] {
            let comment = format!("c624p3t18-{}", uuid::Uuid::new_v4().simple());
            let got = run_624(
                &db,
                hooks_624(todays, None, None),
                Some(&comment),
                &count,
                &range,
            )
            .await
            .unwrap_or_else(|e| panic!("{count}, today's route {todays}: {e}"));
            let pair = ("x".to_string(), literal_text.to_string());
            assert_eq!(
                got,
                vec![(vec![pair], vec![(t + 60 * sec, 1.0f64.to_bits())])],
                "{count}, today's route {todays}"
            );
            let lowered =
                (!todays).then(|| format!("extractGroups(body, '(?P<x>{literal_text})') AS g"));
            comments.push((comment, count.clone(), lowered));
        }
    }
    let all: Vec<String> = comments.iter().map(|(c, _, _)| c.clone()).collect();
    let logged = logged_624(&admin, &all).await;
    for (comment, query, lowered) in &comments {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{query}: no statement logged"));
        assert!(
            rows.iter().any(|r| r.query.contains("extractGroups")),
            "{query}: the database ran the regexp: {rows:#?}"
        );
        // The count not forced onto today's route ran the lowered range
        // statement, the brace pattern in its group key, and nothing after
        // it: a decline to today's route would answer the same.
        if let Some(extract) = lowered {
            assert!(
                rows.iter().any(|r| is_lowered_624(&r.query)
                    && r.query.contains(extract.as_str())
                    && r.query.contains("matched, caps")
                    && r.exception_code == 0),
                "{query}: the lowered statement with `{extract}` in its group key: {rows:#?}"
            );
            assert!(
                !rows.iter().any(|r| is_todays_624(&r.query)),
                "{query}: no fallback to today's route: {rows:#?}"
            );
        }
    }
    drop_db_624(&admin, &db).await;
}

/// Issue #624, part 3b (§7): one stream of 300,000 lines, 100 of them in a run
/// holding `panic: rare`, written straight to the tables.
async fn seed_condition_cache_corpus_624p3b() -> (ChClient, String, i64) {
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3bs_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let month = format!("toStartOfMonth(fromUnixTimestamp64Nano(toInt64({t})))");
    for sql in [
        format!(
            "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) \
             SELECT {month}, 931, 'c3s', '{{\"service_name\":\"c3s\"}}', 0"
        ),
        format!(
            "INSERT INTO {db}.log_streams_idx (month, key, val, fingerprint) \
             SELECT {month}, 'service_name', 'c3s', 931"
        ),
        format!(
            "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body, \
             structured_metadata) SELECT 'c3s', 931, {t} + 1 + number * 1000000, 0, \
             if(number BETWEEN 150000 AND 150099, 'x panic: rare y', 'hay'), '' \
             FROM numbers(300000)"
        ),
    ] {
        exec_624(&admin, &sql).await;
    }
    (admin, db, t)
}

/// **S-T3 (issue #624, part 3b): a line filter leaves the next query's count
/// intact.** On 26.3.29.7 a `PREWHERE service = …` statement whose `WHERE`
/// holds `body LIKE` over a `text` index records, in the query condition
/// cache, granules of the `service` condition as matching nothing; a later
/// statement with the same `PREWHERE` and no body predicate then counts
/// too few lines. The log query runs first, then the count: 300,000.
#[tokio::test]
async fn a_line_filter_leaves_the_next_count_intact() {
    skip_unless_live!();
    let (admin, db, t) = seed_condition_cache_corpus_624p3b().await;
    exec_624(&admin, "SYSTEM DROP QUERY CONDITION CACHE").await;
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let logs = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 600_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 1000,
        direction: Direction::Backward,
    };
    let (found, _) = engine
        .query(
            &parse(r#"{service_name="c3s"} |= "panic: rare""#).expect("parse"),
            &logs,
        )
        .await
        .expect("the log query answers");
    let QueryResult::Streams { items: streams, .. } = found else {
        panic!("a log query answers streams");
    };
    let lines: usize = streams.iter().map(|s| s.entries.len()).sum();
    assert_eq!(lines, 100, "the needle's lines");
    let at = QueryParams {
        spec: QuerySpec::Instant {
            at_ns: t + 600_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let (count, _) = engine
        .query(
            &parse(r#"sum(count_over_time({service_name="c3s"} [10m]))"#).expect("parse"),
            &at,
        )
        .await
        .expect("the count answers");
    let QueryResult::Vector(samples) = count else {
        panic!("an instant query answers a vector");
    };
    let total: f64 = samples.iter().map(|s| s.value).sum();
    assert_eq!(total, 300_000.0, "every line, after the line filter ran");
    exec_624(&admin, "SYSTEM DROP QUERY CONDITION CACHE").await;
    drop_db_624(&admin, &db).await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct DirectReadRow624p3b {
    query: String,
    direct_read: String,
}

/// **S-T4 (issue #624, part 3b): every statement over `log_samples` that
/// carries a body predicate is sent with
/// `query_plan_direct_read_from_text_index = 0`** — a log query, a page
/// loop, a lowered count, today's route for a staged count, an instant
/// query — and at least one statement of each shape is logged.
#[tokio::test]
async fn every_body_predicate_statement_disables_the_text_index_direct_read() {
    skip_unless_live!();
    let (admin, db, t) = seed_condition_cache_corpus_624p3b().await;
    let comment = format!("c624p3bs4-{}", uuid::Uuid::new_v4().simple());
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    )
    .with_query_log_comment(comment.clone());
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 600_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 50,
        direction: Direction::Backward,
    };
    let instant = QueryParams {
        spec: QuerySpec::Instant {
            at_ns: t + 600_000_000_000,
        },
        limit: 50,
        direction: Direction::Backward,
    };
    for (query, params) in [
        (r#"{service_name="c3s"} |= "panic: rare""#, &range),
        (
            r#"{service_name="c3s"} |~ "panic: r.re" | json | x="1""#,
            &range,
        ),
        (r#"{service_name="c3s"} != "hay""#, &range),
        (
            r#"sum(count_over_time({service_name="c3s"} |= "panic" [1m]))"#,
            &range,
        ),
        (
            r#"sum(count_over_time({service_name="c3s"} |= "panic" | drop x [2m]))"#,
            &range,
        ),
        (
            r#"sum(count_over_time({service_name="c3s"} |~ "(?i)(panic|refused)" [10m]))"#,
            &instant,
        ),
    ] {
        engine
            .query(&parse(query).expect("parse"), params)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
    }
    let sql = format!(
        "SELECT query, Settings['query_plan_direct_read_from_text_index'] AS direct_read \
         FROM system.query_log WHERE log_comment = '{comment}' AND type = 'QueryFinish' \
         AND query LIKE '%log_samples%' AND query LIKE '%body%' \
         AND (query LIKE '%body LIKE%' OR query LIKE '%match(body%' OR query LIKE '%match(identity(body)%')"
    );
    let mut rows: Vec<DirectReadRow624p3b> = Vec::new();
    for _ in 0..30 {
        exec_624(&admin, "SYSTEM FLUSH LOGS").await;
        let mut stream = admin
            .query_stream::<DirectReadRow624p3b>(&sql, &QuerySettings::new())
            .await
            .expect("read system.query_log");
        rows.clear();
        while let Some(row) = stream.next().await {
            rows.push(row.expect("decode"));
        }
        drop(stream);
        if rows.len() >= 6 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(
        rows.len() >= 6,
        "every shape logged a body-predicate statement: {rows:#?}"
    );
    for r in &rows {
        assert_eq!(r.direct_read, "0", "{}", r.query);
    }
    drop_db_624(&admin, &db).await;
}

/// Issue #624, part 3b: four streams of JSON lines under `p3bj`. Stream 602
/// carries a stream label `level`, which the body's `level` collides with;
/// stream 603's lines carry metadata `level`. Every value is one the
/// database decides. Lines sit in windows at `[30s]`, `[60s]` and `[150s]`
/// on the 60 s grid from `T`, two groups of one stream in one window.
async fn seed_json_corpus_624p3b() -> (ChClient, String, i64) {
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3b_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let svc = "p3bj";
    for (fp, labels) in [
        (601u64, r#"{"app":"j","service_name":"p3bj"}"#),
        (602, r#"{"app":"j","level":"stream","service_name":"p3bj"}"#),
        (603, r#"{"app":"k","service_name":"p3bj"}"#),
    ] {
        land_stream_624(&admin, &db, t, fp, svc, labels).await;
    }
    let sec = 1_000_000_000i64;
    for (fp, at, body, sm) in [
        (
            601u64,
            10i64,
            r#"{"level":"info","status":200,"method":"GET","msg":"ok"}"#,
            "",
        ),
        (
            601,
            20,
            r#"{"level":"error","status":500,"method":"POST","msg":"café"}"#,
            "",
        ),
        (
            601,
            70,
            r#"{"level":"info","status":"404","method":"GET"}"#,
            "",
        ),
        (601, 80, r#"{"level":"warn","method":"PUT"}"#, ""),
        (
            601,
            130,
            r#"{"level":"info","status":200,"method":"GET","msg":"ok"}"#,
            "",
        ),
        (
            601,
            200,
            r#"{"level":"error","status":502,"method":"GET","msg":"Ελληνικά"}"#,
            "",
        ),
        (
            602,
            15,
            r#"{"level":"debug","status":201,"method":"GET"}"#,
            "",
        ),
        (602, 75, r#"{"level":"error","status":503}"#, ""),
        (602, 140, r#"{"method":"DELETE","status":204}"#, ""),
        (
            603,
            25,
            r#"{"level":"info","status":200}"#,
            r#"{"level":"meta"}"#,
        ),
        (603, 85, r#"{"status":400,"method":"GET"}"#, ""),
        (603, 210, r#"{"level":"info","status":200}"#, ""),
        (
            601,
            45,
            r#"{"level":"info","status":301,"method":"GET"}"#,
            "",
        ),
        (
            601,
            50,
            r#"{"level":"error","status":500,"method":"GET"}"#,
            "",
        ),
        (
            602,
            105,
            r#"{"level":"warn","status":429,"method":"POST"}"#,
            "",
        ),
        (
            603,
            170,
            r#"{"level":"info","status":200,"method":"GET"}"#,
            r#"{"level":"meta"}"#,
        ),
        (
            601,
            235,
            r#"{"level":"debug","method":"GET","status":200}"#,
            "",
        ),
    ] {
        land_line_624(&admin, &db, fp, svc, t + at * sec, body, sm).await;
    }
    (admin, db, t)
}

/// T3's lowered queries over `sel` at `range`.
fn json_lowered_queries_624p3b(sel: &str, range: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for op in ["count_over_time", "rate", "bytes_over_time", "bytes_rate"] {
        out.push(format!("sum by (level) ({op}({sel} | json [{range}]))"));
        out.push(format!("sum({op}({sel} | json [{range}]))"));
        out.push(format!(
            r#"topk(3, sum by (level) ({op}({sel} | json | status != "" [{range}])))"#
        ));
        out.push(format!(
            r#"sum by (level, method) ({op}({sel} | json | status >= 400 [{range}]))"#
        ));
        out.push(format!(r#"{op}({sel} | json lvl="level" [{range}])"#));
        out.push(format!(r#"{op}({sel} | json level="level" [{range}])"#));
        out.push(format!(
            r#"count by (st) ({op}({sel} | json st="status", m="method" | m="GET" [{range}]))"#
        ));
    }
    out.push(format!(
        r#"absent_over_time({sel} | json lvl="level" [{range}])"#
    ));
    out
}

/// **T3 (issue #624, part 3b): a `| json` count answers what today's route
/// answers, bit for bit**, for every query T1 lowers, at a range equal to,
/// above and below the step; the query log shows the lowered statement, and
/// for the queries that stay, only today's read.
#[tokio::test]
async fn a_json_count_answers_as_todays_route() {
    skip_unless_live!();
    let (admin, db, t) = seed_json_corpus_624p3b().await;
    let sel = r#"{service_name="p3bj"}"#;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 240_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mut comments: Vec<(String, bool, bool)> = Vec::new();
    let mut nonempty = 0usize;
    for range in ["60s", "150s", "30s"] {
        let mut queries: Vec<(String, bool)> = json_lowered_queries_624p3b(sel, range)
            .into_iter()
            .map(|q| (q, true))
            .collect();
        for q in [
            format!("count_over_time({sel} | json [{range}])"),
            format!("count by (level) (count_over_time({sel} | json [{range}]))"),
            format!(r#"count_over_time({sel} | json a="level.x" [{range}])"#),
        ] {
            queries.push((q, false));
        }
        for (query, lowers) in &queries {
            let comment = format!("c624p3bt3-{}", uuid::Uuid::new_v4().simple());
            let lowered = run_624(
                &db,
                hooks_624(false, None, None),
                Some(&comment),
                query,
                &params,
            )
            .await;
            let todays = run_624(&db, hooks_624(true, None, None), None, query, &params).await;
            same_outcome_624(query, &lowered, &todays);
            if lowered.as_ref().is_ok_and(|a| !a.is_empty()) {
                nonempty += 1;
            }
            // Issue #624, part 3c (D7): a bare `sum` or an
            // `absent_over_time` with no stage reading a label skips the
            // `| json`; part 2's statement counts it, with no readers.
            let no_label = *lowers
                && (query.starts_with("sum(") || query.starts_with("absent_over_time("))
                && !query.contains("| json |");
            comments.push((comment, *lowers, no_label));
        }
    }
    assert!(nonempty >= 90, "the corpus answers: {nonempty}");
    let all: Vec<String> = comments.iter().map(|(c, _, _)| c.clone()).collect();
    let logged = logged_624(&admin, &all).await;
    for (comment, lowers, no_label) in &comments {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{comment}: no statement logged"));
        let lowered = rows.iter().any(|r| {
            is_lowered_624(&r.query)
                && r.query.contains("throwIf(decided = 0)") != *no_label
                && r.exception_code == 0
        });
        assert_eq!(lowered, *lowers, "{comment}: {rows:#?}");
    }
    drop_db_624(&admin, &db).await;
}

/// **T4 (issue #624, part 3b): a row the database does not decide sends the
/// query to today's route.** One service per case, each with one decided
/// line and one undecided line: the lowered statement fails with code 395,
/// today's read runs under the same comment, and the answer — a matrix or
/// the pipeline error — is today's.
#[tokio::test]
async fn an_undecided_json_row_answers_on_todays_route() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3bu_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    let cases: [(&str, &str, &str); 11] = [
        ("u10", r#"{"level":18446744073709551616}"#, "bare"),
        ("u11", r#"{"level":-9223372036854775809}"#, "targeted"),
        ("u1", "not json", "bare"),
        ("u2", "[1]", "bare"),
        ("u3", r#"{"level":1.5}"#, "bare"),
        ("u4", r#"{"level":0}"#, "bare"),
        ("u5", r#"{"level":true}"#, "bare"),
        ("u6", r#"{"level":"a\"b"}"#, "bare"),
        ("u7", "5", "bare"),
        ("u8", r#"{"level":"x"} trailing"#, "targeted"),
        ("u9", r#"{"level":1.5}"#, "targeted"),
    ];
    for (i, (case, body, _)) in cases.iter().enumerate() {
        let svc = format!("p3b{case}");
        let fp = 700 + i as u64;
        land_stream_624(
            &admin,
            &db,
            t,
            fp,
            &svc,
            &format!(r#"{{"app":"u","service_name":"{svc}"}}"#),
        )
        .await;
        land_line_624(
            &admin,
            &db,
            fp,
            &svc,
            t + 10 * sec,
            r#"{"level":"info"}"#,
            "",
        )
        .await;
        land_line_624(&admin, &db, fp, &svc, t + 20 * sec, body, "").await;
    }
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t + 60 * sec,
            end_ns: t + 120 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mut comments: Vec<String> = Vec::new();
    for (case, _, form) in &cases {
        let sel = format!(r#"{{service_name="p3b{case}"}}"#);
        for range in ["60s", "150s"] {
            let query = if *form == "bare" {
                format!("sum by (level) (count_over_time({sel} | json [{range}]))")
            } else {
                format!(r#"count_over_time({sel} | json lvl="level" [{range}])"#)
            };
            let comment = format!("c624p3bt4-{}", uuid::Uuid::new_v4().simple());
            let lowered = run_624(
                &db,
                hooks_624(false, None, None),
                Some(&comment),
                &query,
                &params,
            )
            .await;
            let todays = run_624(&db, hooks_624(true, None, None), None, &query, &params).await;
            same_outcome_624(&query, &lowered, &todays);
            comments.push(comment);
        }
    }
    let logged = logged_624(&admin, &comments).await;
    for comment in &comments {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{comment}: no statement logged"));
        let thrown = rows
            .iter()
            .position(|r| is_lowered_624(&r.query) && r.exception_code == 395);
        let todays = rows.iter().position(|r| is_todays_624(&r.query));
        assert!(
            matches!((thrown, todays), (Some(a), Some(b)) if a < b),
            "{comment}: the lowered statement threw, then today's read ran: {rows:#?}"
        );
    }
    drop_db_624(&admin, &db).await;
}

/// **T5 (issue #624, part 3b): a `| json` count out of memory answers on
/// today's route.** The lowered statement under a one-byte ceiling fails
/// with code 241; today's read follows under the same comment.
#[tokio::test]
async fn a_json_count_out_of_memory_answers_on_todays_route() {
    skip_unless_live!();
    let (admin, db, t) = seed_json_corpus_624p3b().await;
    let query = r#"sum by (level) (count_over_time({service_name="p3bj"} | json [150s]))"#;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 240_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let comment = format!("c624p3bt5-{}", uuid::Uuid::new_v4().simple());
    let lowered = run_624(
        &db,
        hooks_624(false, None, Some(1)),
        Some(&comment),
        query,
        &params,
    )
    .await;
    let todays = run_624(&db, hooks_624(true, None, None), None, query, &params).await;
    same_outcome_624(query, &lowered, &todays);
    assert!(todays.as_ref().is_ok_and(|a| !a.is_empty()));
    let logged = logged_624(&admin, std::slice::from_ref(&comment)).await;
    let rows = &logged[&comment];
    let oom = rows.iter().position(|r| {
        is_lowered_624(&r.query)
            && r.query.contains("throwIf(decided = 0)")
            && r.exception_code == 241
    });
    let todays_at = rows.iter().position(|r| is_todays_624(&r.query));
    assert!(
        matches!((oom, todays_at), (Some(a), Some(b)) if a < b),
        "the lowered statement ran out of memory, then today's read ran: {rows:#?}"
    );
    drop_db_624(&admin, &db).await;
}

/// **T7 (issue #624, part 3b): a `rate` or `bytes_rate` directly under a
/// `sum` divides once per sum group, on every route.** Two streams, 7 and 3
/// lines of `{}` (2 bytes each) in one minute: the reference counts each
/// sum group as one series (`canInjectVectorGrouping`), so the answer is the
/// bits of `10 / R` (rate) or `20 / R` (bytes), never of `7 / R + 3 / R`.
///
/// The cells are the product of four tables, so none can be left out:
/// reducer (`rate`, `bytes_rate`, the two F1 changes) × parent (`sum`,
/// `sum by`, `sum without`) × pipeline (none, `| json`, `| drop extra`) ×
/// route (range lowered and range on today's route at `[60s]` and
/// `[120s]`; instant, which is the clean statement with no pipeline and,
/// since issue #624 part 5, the lowered statement at one grid point with a
/// pipeline whose range form lowers). Each cell checks its route in the query log;
/// a bare `| json` under `sum without` does not lower (D1), so its range
/// cells are today's route.
#[tokio::test]
async fn a_rate_under_a_sum_divides_once_per_group() {
    skip_unless_live!();
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Route7 {
        RangeLowered(&'static str, f64),
        RangeTodays(&'static str, f64),
        Instant,
    }
    const OPS: [(&str, f64); 2] = [("rate", 10.0), ("bytes_rate", 20.0)];
    const PARENTS: [&str; 3] = ["sum", "sum by (service_name)", "sum without (app)"];
    // `| drop extra` drops a label no stream has: label-only (part 2), and the
    // two streams stay two series until the sum.
    const PIPELINES: [&str; 3] = ["", " | json", " | drop extra"];
    const ROUTES: [Route7; 5] = [
        Route7::RangeLowered("60s", 60.0),
        Route7::RangeLowered("120s", 120.0),
        Route7::RangeTodays("60s", 60.0),
        Route7::RangeTodays("120s", 120.0),
        Route7::Instant,
    ];
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3brate_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    let svc = "p3brate";
    for (fp, n) in [(811u64, 7i64), (812, 3)] {
        land_stream_624(
            &admin,
            &db,
            t,
            fp,
            svc,
            &format!(r#"{{"app":"r{fp}","service_name":"{svc}"}}"#),
        )
        .await;
        for i in 0..n {
            land_line_624(&admin, &db, fp, svc, t + (10 + i) * sec, "{}", "").await;
        }
    }
    let sel = r#"{service_name="p3brate"}"#;
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 120 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let instant = QueryParams {
        spec: QuerySpec::Instant {
            at_ns: t + 60 * sec,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mut cells: Vec<(String, Route7, &str, bool, String)> = Vec::new();
    // Every problem found, by cell, so the count is of cells.
    let mut divided: Vec<(usize, String)> = Vec::new();
    let mut routed: Vec<(usize, String)> = Vec::new();
    for (op, total) in OPS {
        for parent in PARENTS {
            for pipeline in PIPELINES {
                for route in ROUTES {
                    let (r, secs) = match route {
                        Route7::RangeLowered(r, s) | Route7::RangeTodays(r, s) => (r, s),
                        Route7::Instant => ("60s", 60.0),
                    };
                    let query = format!("{parent} ({op}({sel}{pipeline} [{r}]))");
                    let want = (total / secs).to_bits();
                    let comment = format!("c624p3bt7-{}", uuid::Uuid::new_v4().simple());
                    let hooks = match route {
                        Route7::RangeTodays(..) => hooks_624(false, None, Some(1)),
                        _ => hooks_624(false, None, None),
                    };
                    let mut engine = LogQlEngine::new(
                        data_client(&db).await,
                        engine_config(&db, 64 * 1024 * 1024),
                        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                    )
                    .with_key_route_test_hooks(hooks);
                    engine = engine.with_query_log_comment(comment.clone());
                    let params = if route == Route7::Instant {
                        &instant
                    } else {
                        &range
                    };
                    let (result, _) = engine
                        .query(&parse(&query).expect("parse"), params)
                        .await
                        .unwrap_or_else(|e| panic!("{query} ({route:?}): {e}"));
                    let points: Vec<u64> = match result {
                        QueryResult::Matrix(series) => series
                            .iter()
                            .flat_map(|s| s.points.iter().map(|(_, v)| v.to_bits()))
                            .collect(),
                        QueryResult::Vector(samples) => {
                            samples.iter().map(|s| s.value.to_bits()).collect()
                        }
                        other => panic!("{query}: {other:?}"),
                    };
                    if points.is_empty() || points.iter().any(|v| *v != want) {
                        divided.push((
                            cells.len(),
                            format!(
                                "{query} ({route:?}): want every point the bits of {total} / {secs} = {want}, got {points:?}"
                            ),
                        ));
                    }
                    // D1: a bare `| json` lowers under `sum` and `sum by` only.
                    let lowers = !(pipeline == " | json" && parent.starts_with("sum without"));
                    cells.push((comment, route, pipeline, lowers, query));
                }
            }
        }
    }
    assert_eq!(
        cells.len(),
        OPS.len() * PARENTS.len() * PIPELINES.len() * ROUTES.len()
    );
    let comments: Vec<String> = cells.iter().map(|c| c.0.clone()).collect();
    let logged = logged_624(&admin, &comments).await;
    for (cell, (comment, route, pipeline, lowers, query)) in cells.iter().enumerate() {
        let Some(rows) = logged.get(comment) else {
            routed.push((cell, format!("{query} ({route:?}): no statement logged")));
            continue;
        };
        let lowered_at = |code: i32| {
            rows.iter()
                .position(|r| is_lowered_624(&r.query) && r.exception_code == code)
        };
        let todays_at = rows.iter().position(|r| is_todays_624(&r.query));
        let ok = match route {
            Route7::RangeLowered(..) | Route7::RangeTodays(..) if !*lowers => {
                todays_at.is_some() && !rows.iter().any(|r| is_lowered_624(&r.query))
            }
            Route7::RangeLowered(..) => lowered_at(0).is_some() && todays_at.is_none(),
            Route7::RangeTodays(..) => {
                matches!((lowered_at(241), todays_at), (Some(a), Some(b)) if a < b)
            }
            Route7::Instant if pipeline.is_empty() => {
                todays_at.is_none() && rows.iter().all(|r| r.exception_code == 0)
            }
            // Issue #624, part 5: an instant whose range form lowers is
            // counted at one grid point.
            Route7::Instant if *lowers => lowered_at(0).is_some() && todays_at.is_none(),
            Route7::Instant => todays_at.is_some(),
        };
        if !ok {
            routed.push((
                cell,
                format!("{query} ({route:?}): the route is not the cell's: {rows:#?}"),
            ));
        }
    }
    let wrong: std::collections::BTreeSet<usize> = divided
        .iter()
        .chain(&routed)
        .map(|(cell, _)| *cell)
        .collect();
    assert!(
        wrong.is_empty(),
        "{} of {} cells are wrong; {} divide before the sum, {} ran on another route:\n{}",
        wrong.len(),
        cells.len(),
        divided.len(),
        routed.len(),
        divided
            .iter()
            .chain(&routed)
            .map(|(_, m)| m.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    );
    drop_db_624(&admin, &db).await;
}

/// **T8 (issue #624, part 3b): a decided row whose labels carry an error
/// fails the lowered `| json` count as it fails today's route.** Every line
/// is one the database decides, so the lowered statement answers (no 395)
/// and the group's pipeline run in PulsusDB meets the error: a `status`
/// that is the string `abc` under `| status >= 400` (`LabelFilterErr`), and
/// a line whose structured metadata holds `__error__`. The query log holds
/// the lowered statement with exception 0 and no read of today's route.
#[tokio::test]
async fn a_decided_row_error_fails_the_lowered_json_count() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3berr_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    for (fp, svc, lines) in [
        (
            821u64,
            "p3berr1",
            [
                (r#"{"level":"info","status":500}"#, ""),
                (r#"{"level":"info","status":"abc"}"#, ""),
            ],
        ),
        (
            822,
            "p3berr2",
            [
                (r#"{"level":"info","status":500}"#, ""),
                (
                    r#"{"level":"warn","status":500}"#,
                    r#"{"__error__":"SampleExtractionErr"}"#,
                ),
            ],
        ),
    ] {
        land_stream_624(
            &admin,
            &db,
            t,
            fp,
            svc,
            &format!(r#"{{"app":"e","service_name":"{svc}"}}"#),
        )
        .await;
        for (i, (body, sm)) in lines.iter().enumerate() {
            land_line_624(&admin, &db, fp, svc, t + (10 + i as i64) * sec, body, sm).await;
        }
    }
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 120 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mut comments: Vec<String> = Vec::new();
    for svc in ["p3berr1", "p3berr2"] {
        for range in ["60s", "150s"] {
            let query = format!(
                r#"sum by (level) (count_over_time({{service_name="{svc}"}} | json | status >= 400 [{range}]))"#
            );
            let comment = format!("c624p3bt8-{}", uuid::Uuid::new_v4().simple());
            let lowered = run_624(
                &db,
                hooks_624(false, None, None),
                Some(&comment),
                &query,
                &params,
            )
            .await;
            let todays = run_624(&db, hooks_624(true, None, None), None, &query, &params).await;
            assert!(
                matches!(lowered, Err(ReadError::MetricPipelineError { .. })),
                "{query}: a pipeline error, got {lowered:?}"
            );
            same_outcome_624(&query, &lowered, &todays);
            comments.push(comment);
        }
    }
    let logged = logged_624(&admin, &comments).await;
    for comment in &comments {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{comment}: no statement logged"));
        assert!(
            rows.iter().any(|r| is_lowered_624(&r.query)
                && r.query.contains("throwIf(decided = 0)")
                && r.exception_code == 0),
            "{comment}: the lowered statement answered: {rows:#?}"
        );
        assert!(
            !rows.iter().any(|r| is_todays_624(&r.query)),
            "{comment}: today's route did not run: {rows:#?}"
        );
    }
    drop_db_624(&admin, &db).await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct PatternCellRow624p3c {
    id: u32,
    pm: Vec<String>,
}

/// The rows of a statement returning `(id, pm)` (issue #624, part 3c).
async fn read_pattern_cells_624p3c(admin: &ChClient, sql: String) -> Vec<PatternCellRow624p3c> {
    let mut stream = admin
        .query_stream::<PatternCellRow624p3c>(&sql.replace('?', "??"), &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let mut out: Vec<PatternCellRow624p3c> = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.unwrap_or_else(|e| panic!("{e}\n{sql}")));
    }
    out
}

/// The patterns and lines of T4's sweep (issue #624, part 3c).
const PATTERNS_624P3C: [&str; 27] = [
    "foo <foo> bar",
    "foo <foo> bar<fuzz>",
    "<foo>foo <bar> bar",
    "<foo> bar<fuzz>",
    "<foo>bar<baz>",
    "<foo> bar<baz>",
    "<foo>bar <baz>",
    "<foo>",
    "<path>?<_>",
    r#"<ip> <userid> <user> [<_>] "<method> <path> <_>" <status> <size>"#,
    "<_> <a>",
    "<a> <_>",
    "<_> <a> <_>",
    "x<a>",
    "<a>x",
    "x<a>x",
    "<a> <b> <c>",
    "<a>,<b>",
    "[<a>]",
    "<a>\n<b>",
    "é<a>ü<b>",
    "<a> <b>",
    "<_>:<a>:<b>",
    "level=<lvl> msg=<msg>",
    "<a>  <b>",
    "aa<a>aa",
    "<a>aa<b>",
];
const LINES_624P3C: [&str; 40] = [
    "foo buzz bar",
    " bar",
    " bar ",
    "foo buzz bar bar",
    "",
    "x",
    "xx",
    "xyx",
    "a b c",
    "a b",
    "a",
    " ",
    "  ",
    "a,b,c",
    ",",
    "[x]",
    "[x",
    "x]",
    "a\nb",
    "a\nb\nc",
    "éxüy",
    "éüü",
    "level=info msg=hi there",
    "level=info",
    "msg=x level=y",
    "/api/plugins/versioncheck?slugIn=a&b=c",
    "/api/plugins/status",
    r#"127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326"#,
    "127.0.0.1 - frank [10/Oct/2000",
    "aaaa",
    "aaa",
    "aa",
    "aaaaa",
    "a  b",
    "a   b",
    ":x:y",
    "x:y",
    "::",
    "foofoo bar",
    "foo  bar",
];

/// **T4 (issue #624, part 3c, D2): the database's pattern captures are the
/// reference's.** (a) The reference's own 23 matcher fixtures, each line as
/// `body`: the statement's expression returns the fixture's captures. (b)
/// 27 patterns × 40 lines in one table: the expression equals the in-process
/// matcher (which T3 holds to the same fixtures) on all 1,080 cells.
#[tokio::test]
async fn the_database_pattern_matches_the_reference_and_the_walk() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3ct4_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    // (a)
    let fixtures: Vec<(String, String, Vec<String>)> =
        include_str!("fixtures/pattern_matcher/reference_fixtures.tsv")
            .lines()
            .filter(|l| !l.starts_with('#') && !l.is_empty())
            .map(|l| {
                let cols: Vec<&str> = l.split('\t').collect();
                (
                    serde_json::from_str(cols[0]).expect("pattern"),
                    serde_json::from_str(cols[1]).expect("line"),
                    serde_json::from_str(cols[2]).expect("captures"),
                )
            })
            .collect();
    assert_eq!(fixtures.len(), 23);
    for (pattern, line, want) in &fixtures {
        let expr = pulsus_read::logql::predicate::pattern_captures(pattern).expect("renders");
        let sql = format!(
            "SELECT toUInt32(0) AS id, {} AS pm FROM (SELECT {} AS body)",
            expr.as_sql(),
            pulsus_read::logql::predicate::literal(line).as_sql()
        );
        let rows = read_pattern_cells_624p3c(&admin, sql).await;
        assert_eq!(rows[0].pm, *want, "{pattern:?} over {line:?}");
    }
    // (b)
    let table = format!("{db}.p3c_sweep");
    exec_624(
        &admin,
        &format!("CREATE TABLE {table} (id UInt32, body String) ENGINE = MergeTree ORDER BY id"),
    )
    .await;
    let values = LINES_624P3C
        .iter()
        .enumerate()
        .map(|(i, b)| {
            format!(
                "({i}, {})",
                pulsus_read::logql::predicate::literal(b).as_sql()
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    exec_624(
        &admin,
        &format!("INSERT INTO {table} VALUES {values}").replace('?', "??"),
    )
    .await;
    let mut cells = 0usize;
    let mut differing: Vec<String> = Vec::new();
    for pattern in PATTERNS_624P3C {
        let expr = pulsus_read::logql::predicate::pattern_captures(pattern).expect("renders");
        let rows = read_pattern_cells_624p3c(
            &admin,
            format!(
                "SELECT id, {} AS pm FROM {table} ORDER BY id",
                expr.as_sql()
            ),
        )
        .await;
        for row in rows {
            let line = LINES_624P3C[row.id as usize];
            let want = pulsus_read::logql::pipeline::pattern_matches_in_process(pattern, line)
                .expect("a valid pattern");
            cells += 1;
            if row.pm != want {
                differing.push(format!(
                    "{pattern:?} over {line:?}: {:?} against {want:?}",
                    row.pm
                ));
            }
        }
    }
    assert_eq!(cells, 1_080);
    assert!(differing.is_empty(), "{differing:#?}");
    drop_db_624(&admin, &db).await;
}

/// Issue #624, part 3c: syslog-shaped lines under `c624p3c` — some fit
/// `<_> <proc>[<pid>]: <msg>`, some only a prefix of it, some not at all.
/// Stream 912 carries the stream label `proc`, which the capture collides
/// with; one line carries metadata `pid`.
async fn seed_pattern_corpus_624p3c() -> (ChClient, String, i64) {
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3c_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let svc = "c624p3c";
    for (fp, labels) in [
        (911u64, r#"{"app":"a","service_name":"c624p3c"}"#),
        (
            912,
            r#"{"app":"a","proc":"stream","service_name":"c624p3c"}"#,
        ),
        (913, r#"{"app":"b","service_name":"c624p3c"}"#),
    ] {
        land_stream_624(&admin, &db, t, fp, svc, labels).await;
    }
    let sec = 1_000_000_000i64;
    for (fp, at, body, sm) in [
        (911u64, 10i64, "host-1 sshd[42]: session opened café", ""),
        (911, 20, "host-1 sshd[43]: closed", ""),
        (911, 70, "host-1 cron[7", ""),
        (911, 80, "host-1 kernel: audit", ""),
        (911, 130, "orphan", ""),
        (911, 200, "host-2 systemd[1]: Started Ελληνικά", ""),
        (912, 15, "host-1 sshd[44]: ok", ""),
        (912, 75, "host-3 dockerd[9]: x", r#"{"pid":"meta"}"#),
        (912, 140, "host-3 dockerd", ""),
        (913, 25, "host-4 \u{1b}[31mcron\u{1b}[0m[8]: done", ""),
        (913, 85, "host-4 polkitd[3]: auth", ""),
        (913, 210, " [5]: odd", ""),
        (911, 45, "host-1 sshd[45]: again", ""),
        (912, 105, "host-1 sshd[46", ""),
        (913, 235, "host-4 cron[9]: ", ""),
    ] {
        land_line_624(&admin, &db, fp, svc, t + at * sec, body, sm).await;
    }
    (admin, db, t)
}

/// T5's lowered queries over `sel` at `range`.
fn pattern_lowered_queries_624p3c(sel: &str, range: &str) -> Vec<String> {
    let parser = r#"| pattern "<_> <proc>[<pid>]: <msg>""#;
    let mut inner: Vec<String> = Vec::new();
    for op in [
        "count_over_time",
        "rate",
        "bytes_over_time",
        "bytes_rate",
        "absent_over_time",
    ] {
        for after in [r#"| proc != """#, r#"| pid=~"4.*""#, "| drop msg", ""] {
            inner.push(format!("{op}({sel} {parser} {after} [{range}])"));
        }
    }
    for op in ["count_over_time", "rate"] {
        inner.push(format!("{op}({sel} {parser} | decolorize [{range}])"));
    }
    for pattern in ["<a> <b>", "<_> <a>[<_>", "host-<n> <_>", "<a>"] {
        inner.push(format!(
            r#"count_over_time({sel} | pattern "{pattern}" [{range}])"#
        ));
    }
    let mut out: Vec<String> = Vec::new();
    for q in &inner {
        out.push(format!("sum by (proc) ({q})"));
        out.push(format!("topk(10, sum by (proc, pid) ({q}))"));
        out.push(format!("count by (proc) ({q})"));
        out.push(q.clone());
    }
    out
}

/// **T5 (issue #624, part 3c): a `pattern` count answers what today's route
/// answers, bit for bit**, for every query T1's shapes lower over a corpus of
/// lines that fit the pattern, a prefix of it, or none of it, at a range
/// equal to, above and below the step; the query log holds the lowered
/// statement with `arrayFold`.
#[tokio::test]
async fn a_pattern_count_answers_as_todays_route() {
    skip_unless_live!();
    let (admin, db, t) = seed_pattern_corpus_624p3c().await;
    let sel = r#"{service_name="c624p3c"}"#;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 240_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mut comments: Vec<(String, bool)> = Vec::new();
    let mut nonempty = 0usize;
    let mut total = 0usize;
    for range in ["60s", "150s", "30s"] {
        for query in pattern_lowered_queries_624p3c(sel, range) {
            let comment = format!("c624p3ct5-{}", uuid::Uuid::new_v4().simple());
            let lowered = run_624(
                &db,
                hooks_624(false, None, None),
                Some(&comment),
                &query,
                &params,
            )
            .await;
            let todays = run_624(&db, hooks_624(true, None, None), None, &query, &params).await;
            same_outcome_624(&query, &lowered, &todays);
            total += 1;
            if lowered.as_ref().is_ok_and(|a| !a.is_empty()) {
                nonempty += 1;
            }
            // D7: `absent_over_time` with no stage reading a capture needs
            // no labels; its statement runs no pattern.
            let no_label = query.contains("absent_over_time")
                && !query.contains("| proc != ")
                && !query.contains("| pid=~");
            comments.push((comment, no_label));
        }
    }
    assert_eq!(total, 3 * 4 * (5 * 4 + 2 + 4));
    assert!(nonempty >= 250, "the corpus answers: {nonempty} of {total}");
    let all: Vec<String> = comments.iter().map(|(c, _)| c.clone()).collect();
    let logged = logged_624(&admin, &all).await;
    for (comment, no_label) in &comments {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{comment}: no statement logged"));
        assert!(
            rows.iter().any(|r| is_lowered_624(&r.query)
                && r.query.contains("arrayFold") != *no_label
                && r.exception_code == 0),
            "{comment}: the lowered statement ran: {rows:#?}"
        );
    }
    drop_db_624(&admin, &db).await;
}

/// **T6 (issue #624, part 3c, F1 and D6): a line that fits part of a
/// pattern sets the captures before the missing literal, on every route.**
/// Stream `{s="pc"}`, lines `p q r` and `x`; `| pattern "<a> <b>"`: the
/// reference sets `a="p", b="q r"` and, from `x`, `a="x"`. Asserted on a log
/// query, a tail page, a log query after `line_format "z{{__line__}}"` (in
/// process, over the rewritten line: `zp`, `zx`), a
/// lowered count and today's route, an instant count and a `variants` tail;
/// the database routes' statements hold `arrayFold`.
#[tokio::test]
async fn a_partial_pattern_line_sets_its_prefix_on_every_route() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3ct6_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    land_stream_624(
        &admin,
        &db,
        t,
        931,
        "c624pc",
        r#"{"s":"pc","service_name":"c624pc"}"#,
    )
    .await;
    land_line_624(&admin, &db, 931, "c624pc", t + 10 * sec, "p q r", "").await;
    land_line_624(&admin, &db, 931, "c624pc", t + 20 * sec, "x", "").await;
    let want = vec!["p".to_string(), "x".to_string()];
    let logs = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let engine_with = |comment: &str| {
        let comment = comment.to_string();
        let db = db.clone();
        async move {
            LogQlEngine::new(
                data_client(&db).await,
                engine_config(&db, 64 * 1024 * 1024),
                pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            )
            .with_query_log_comment(comment)
        }
    };
    let mut database_routes: Vec<(String, &str)> = Vec::new();
    for (query, database, want, want_b) in [
        (r#"{s="pc"} | pattern "<a> <b>""#, true, want.clone(), "q r"),
        (
            r#"{s="pc"} | line_format "z{{__line__}}" | pattern "<a> <b>""#,
            false,
            vec!["zp".to_string(), "zx".to_string()],
            "q r",
        ),
    ] {
        let comment = format!("c624p3ct6-{}", uuid::Uuid::new_v4().simple());
        let (result, _) = engine_with(&comment)
            .await
            .query(&parse(query).expect("parse"), &logs)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let QueryResult::Streams { items, .. } = result else {
            panic!("{query}: streams");
        };
        assert_eq!(label_values_624p3(&items, "a"), want, "{query}");
        assert_eq!(
            label_values_624p3(&items, "b"),
            vec![want_b.to_string()],
            "{query}"
        );
        if database {
            database_routes.push((comment, "the log query"));
        }
    }
    {
        let comment = format!("c624p3ct6-{}", uuid::Uuid::new_v4().simple());
        let engine = engine_with(&comment).await;
        let mut setup = engine
            .tail_setup(
                &parse(r#"{s="pc"} | pattern "<a> <b>""#).expect("parse"),
                &logs,
            )
            .expect("a tail setup");
        let page = engine
            .tail_poll(
                &mut setup,
                pulsus_read::logql::exec::TailLower::Start { start_ns: t },
                t + 60 * sec,
                100,
            )
            .await
            .expect("a tail page");
        assert_eq!(
            label_values_624p3(&page.streams, "a"),
            want,
            "the tail page"
        );
        database_routes.push((comment, "the tail page"));
    }
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t + 60 * sec,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let count = r#"sum by (a) (count_over_time({s="pc"} | pattern "<a> <b>" [1m]))"#;
    for todays in [false, true] {
        let comment = format!("c624p3ct6-{}", uuid::Uuid::new_v4().simple());
        let got = run_624(
            &db,
            hooks_624(todays, None, None),
            Some(&comment),
            count,
            &range,
        )
        .await
        .unwrap_or_else(|e| panic!("{count}: {e}"));
        let mut a: Vec<String> = got
            .iter()
            .filter_map(|(l, _)| l.iter().find(|(k, _)| k == "a").map(|(_, v)| v.clone()))
            .collect();
        a.sort();
        assert_eq!(a, want, "{count}, today's route {todays}");
        database_routes.push((
            comment,
            if todays {
                "today's route"
            } else {
                "the lowered count"
            },
        ));
    }
    for query in [
        count.to_string(),
        r#"variants(sum by (a) (count_over_time({s="pc"} | pattern "<a> <b>" [1m]))) of ({s="pc"} [1m])"#
            .to_string(),
    ] {
        let comment = format!("c624p3ct6-{}", uuid::Uuid::new_v4().simple());
        let instant = QueryParams {
            spec: QuerySpec::Instant { at_ns: t + 60 * sec },
            limit: 100,
            direction: Direction::Backward,
        };
        let (result, _) = engine_with(&comment)
            .await
            .query(&parse(&query).expect("parse"), &instant)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let QueryResult::Vector(samples) = result else {
            panic!("{query}: an instant query answers a vector");
        };
        let mut a: Vec<String> = samples
            .iter()
            .filter_map(|s| s.labels.iter().find(|(k, _)| k == "a").map(|(_, v)| v.clone()))
            .collect();
        a.sort();
        assert_eq!(a, want, "{query}: instant");
        database_routes.push((comment, "an instant query"));
    }
    let comments: Vec<String> = database_routes.iter().map(|(c, _)| c.clone()).collect();
    let logged = logged_624(&admin, &comments).await;
    for (comment, route) in &database_routes {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{route}: no statement logged"));
        assert!(
            rows.iter().any(|r| r.query.contains("arrayFold")),
            "{route}: the database ran the pattern: {rows:#?}"
        );
    }
    drop_db_624(&admin, &db).await;
}

/// **T7 (issue #624, part 3c): a `pattern` count out of memory answers on
/// today's route.** The lowered statement under a one-byte ceiling fails
/// with code 241; today's read follows under the same comment, and the
/// answer is today's.
#[tokio::test]
async fn a_pattern_count_out_of_memory_answers_on_todays_route() {
    skip_unless_live!();
    let (admin, db, t) = seed_pattern_corpus_624p3c().await;
    let query = r#"sum by (proc) (count_over_time({service_name="c624p3c"} | pattern "<_> <proc>[<pid>]: <msg>" [150s]))"#;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 240_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let comment = format!("c624p3ct7-{}", uuid::Uuid::new_v4().simple());
    let lowered = run_624(
        &db,
        hooks_624(false, None, Some(1)),
        Some(&comment),
        query,
        &params,
    )
    .await;
    let todays = run_624(&db, hooks_624(true, None, None), None, query, &params).await;
    same_outcome_624(query, &lowered, &todays);
    let logged = logged_624(&admin, std::slice::from_ref(&comment)).await;
    let rows = &logged[&comment];
    let oom = rows
        .iter()
        .position(|r| is_lowered_624(&r.query) && r.exception_code == 241);
    let todays_at = rows.iter().position(|r| is_todays_624(&r.query));
    assert!(
        matches!((oom, todays_at), (Some(a), Some(b)) if a < b),
        "the lowered statement ran out of memory, then today's read ran: {rows:#?}"
    );
    drop_db_624(&admin, &db).await;
}

/// **T9 (issue #624, part 3c, D4 and the no-label hints): `absent_over_time`
/// extracts no pattern capture unless a later stage reads one.** Stream
/// `{s="x"}`, lines at `t+10s` `boom`, `t+20s` `hit x`, `t+70s` `miss y`,
/// `t+190s` `hit z`; range `t`..`t+240s`, step 60 s. (A) `| pattern
/// "<__error__>"`, read by no stage: the reference's parser extracts
/// nothing, so no line carries `__error__` and the answer is 1 where no line
/// lies, at `t` and `t+180s`; the lowered statement runs no pattern. (B)
/// `| pattern "<a> <_>" | a="hit"`: 1 where no `hit` line lies, at `t`,
/// `t+120s` and `t+180s`. (C) `| line_format "{{__line__}}" | pattern
/// "<__error__>"`: (A) after a line rewrite, so the stage walks the line in
/// process and has no `rx` element; only the stage's own no-label skip keeps
/// `boom` from becoming `__error__`. It stays on today's route, so no
/// lowered statement is asked of it. On the lowered statement, on today's
/// route (`todays_route_only` and a one-byte ceiling), and instant at
/// `t+120s` and `t+180s`.
#[tokio::test]
async fn an_absent_pattern_count_extracts_only_what_a_stage_reads() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3ct9_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    land_stream_624(
        &admin,
        &db,
        t,
        941,
        "c624ab",
        r#"{"s":"x","service_name":"c624ab"}"#,
    )
    .await;
    for (at, body) in [
        (10i64, "boom"),
        (20, "hit x"),
        (70, "miss y"),
        (190, "hit z"),
    ] {
        land_line_624(&admin, &db, 941, "c624ab", t + at * sec, body, "").await;
    }
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 240 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let one = 1.0f64.to_bits();
    let labels = vec![("s".to_string(), "x".to_string())];
    // The third column is what the lowered statement must hold: "" for no
    // pattern at all, `-` for a query that does not lower.
    let cases: [(&str, &[i64], &str); 3] = [
        (
            r#"absent_over_time({s="x"} | pattern "<__error__>" [1m])"#,
            &[0, 180],
            "",
        ),
        (
            r#"absent_over_time({s="x"} | pattern "<a> <_>" | a="hit" [1m])"#,
            &[0, 120, 180],
            "arraySlice([g[1]]",
        ),
        (
            r#"absent_over_time({s="x"} | line_format "{{__line__}}" | pattern "<__error__>" [1m])"#,
            &[0, 180],
            "-",
        ),
    ];
    let mut comments: Vec<(String, &str)> = Vec::new();
    for (query, absent_at, caps) in cases {
        let want: Vec<AnswerSeries> = vec![(
            labels.clone(),
            absent_at.iter().map(|s| (t + s * sec, one)).collect(),
        )];
        for hooks in [
            hooks_624(false, None, None),
            hooks_624(true, None, None),
            hooks_624(false, None, Some(1)),
        ] {
            let comment = format!("c624p3ct9-{}", uuid::Uuid::new_v4().simple());
            let got = run_624(&db, hooks, Some(&comment), query, &range)
                .await
                .unwrap_or_else(|e| panic!("{query} ({hooks:?}): {e}"));
            assert_eq!(got, want, "{query} ({hooks:?})");
            if hooks == hooks_624(false, None, None) && caps != "-" {
                comments.push((comment, caps));
            }
        }
        for at in [120i64, 180] {
            let engine = LogQlEngine::new(
                data_client(&db).await,
                engine_config(&db, 64 * 1024 * 1024),
                pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            );
            let instant = QueryParams {
                spec: QuerySpec::Instant {
                    at_ns: t + at * sec,
                },
                limit: 100,
                direction: Direction::Backward,
            };
            let (result, _) = engine
                .query(&parse(query).expect("parse"), &instant)
                .await
                .unwrap_or_else(|e| panic!("{query} instant at +{at}s: {e}"));
            let QueryResult::Vector(samples) = result else {
                panic!("{query}: a vector");
            };
            let got: Vec<f64> = samples.iter().map(|s| s.value).collect();
            let want: Vec<f64> = if absent_at.contains(&at) {
                vec![1.0]
            } else {
                vec![]
            };
            assert_eq!(got, want, "{query} instant at +{at}s");
        }
    }
    let all: Vec<String> = comments.iter().map(|(c, _)| c.clone()).collect();
    let logged = logged_624(&admin, &all).await;
    for (comment, caps) in &comments {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{comment}: no statement logged"));
        // (A) needs no labels: part 2's statement, no pattern run at all.
        let ok = if caps.is_empty() {
            rows.iter().any(|r| is_lowered_624(&r.query))
                && !rows.iter().any(|r| r.query.contains("arrayFold"))
        } else {
            rows.iter()
                .any(|r| is_lowered_624(&r.query) && r.query.contains(caps))
        };
        assert!(
            ok,
            "{comment}: the lowered statement for `{caps}`: {rows:#?}"
        );
    }
    drop_db_624(&admin, &db).await;
}

/// **T10 (issue #624, part 3c, D7): a query that needs no labels skips
/// `json`, `logfmt`, `unpack` and `pattern`, as the reference does; one that
/// needs a label runs them.** One service per parser form, each with a line
/// the parser reads and one it fails on (for `pattern`, one that fits only
/// part of it). The cells are the product of three constant tables, and the
/// count is asserted: parser form × need (`sum(bytes_over_time(…))` needs
/// no labels: the two stored lines' bytes, so no parse, no rewrite and no
/// extraction in the database; `sum by (a) (bytes_over_time(…))` needs `a`:
/// the parser's error, or for `pattern` one series per captured `a`) ×
/// route (lowered, `todays_route_only`, a one-byte ceiling, instant, a
/// `variants(...)` of it). A lowered no-label cell's statements all end
/// without an exception; a `pattern` cell runs `arrayFold` in the database
/// exactly when it needs a label.
#[tokio::test]
async fn a_query_that_needs_no_labels_skips_the_parsers() {
    skip_unless_live!();
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Route10 {
        Lowered,
        TodaysOnly,
        Memory,
        Instant,
        Variants,
    }
    // (service, stage, line it reads, line it fails on, what `sum by (a)` gives)
    const PARSERS: [(&str, &str, &str, &str, &str); 6] = [
        (
            "t10j",
            "| json",
            r#"{"a":"x","_entry":"hi"}"#,
            "not json",
            "JSONParserErr",
        ),
        (
            "t10e",
            r#"| json a="a""#,
            r#"{"a":"x","_entry":"hi"}"#,
            "not json",
            "JSONParserErr",
        ),
        (
            "t10l",
            "| logfmt --strict",
            "a=x",
            r#"a="open"#,
            "LogfmtParserErr",
        ),
        (
            "t10m",
            r#"| logfmt --strict a="a""#,
            "a=x",
            r#"a="open"#,
            "LogfmtParserErr",
        ),
        (
            "t10u",
            "| unpack",
            r#"{"a":"x","_entry":"hi"}"#,
            "not json",
            "JSONParserErr",
        ),
        (
            "t10p",
            r#"| pattern "<a> <_>""#,
            "x y",
            "z",
            r#"a="x":3,a="z":1"#,
        ),
    ];
    const NEEDS: [bool; 2] = [false, true];
    const ROUTES: [Route10; 5] = [
        Route10::Lowered,
        Route10::TodaysOnly,
        Route10::Memory,
        Route10::Instant,
        Route10::Variants,
    ];
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3ct10_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    for (i, (svc, _, good, bad, _)) in PARSERS.iter().enumerate() {
        let fp = 960 + i as u64;
        land_stream_624(
            &admin,
            &db,
            t,
            fp,
            svc,
            &format!(r#"{{"s":"{svc}","service_name":"{svc}"}}"#),
        )
        .await;
        land_line_624(&admin, &db, fp, svc, t + 10 * sec, good, "").await;
        land_line_624(&admin, &db, fp, svc, t + 20 * sec, bad, "").await;
    }
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t + 60 * sec,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let instant = QueryParams {
        spec: QuerySpec::Instant {
            at_ns: t + 60 * sec,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mut cells = 0usize;
    let mut wrong: Vec<String> = Vec::new();
    let mut lowered_no_label: Vec<String> = Vec::new();
    let mut pattern_cells: Vec<(String, bool, String)> = Vec::new();
    for (svc, parser, good, bad, needed) in PARSERS {
        let bytes = (good.len() + bad.len()) as f64;
        for needs in NEEDS {
            for route in ROUTES {
                cells += 1;
                let inner = format!(r#"bytes_over_time({{s="{svc}"}} {parser} [1m])"#);
                let metric = if needs {
                    format!("sum by (a) ({inner})")
                } else {
                    format!("sum({inner})")
                };
                let query = match route {
                    Route10::Variants => format!(r#"variants({metric}) of ({{s="{svc}"}} [1m])"#),
                    _ => metric,
                };
                let comment = format!("c624p3ct10-{}", uuid::Uuid::new_v4().simple());
                let hooks = match route {
                    Route10::TodaysOnly => hooks_624(true, None, None),
                    Route10::Memory => hooks_624(false, None, Some(1)),
                    _ => hooks_624(false, None, None),
                };
                let mut engine = LogQlEngine::new(
                    data_client(&db).await,
                    engine_config(&db, 64 * 1024 * 1024),
                    pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                )
                .with_key_route_test_hooks(hooks);
                engine = engine.with_query_log_comment(comment.clone());
                let params = if route == Route10::Instant {
                    &instant
                } else {
                    &range
                };
                let got = engine.query(&parse(&query).expect("parse"), params).await;
                // The answer as `a="<value>":total` per series, or
                // `a=<absent>:total` where the series has no `a`, sorted.
                let series_of = |labels: Vec<(String, String)>, value: f64| -> String {
                    match labels.iter().find(|(k, _)| k == "a") {
                        Some((_, v)) => format!("a={v:?}:{value}"),
                        None => format!("a=<absent>:{value}"),
                    }
                };
                let answer: Result<Vec<String>, String> = match &got {
                    Ok((QueryResult::Matrix(series), _)) => Ok(series
                        .iter()
                        .map(|s| series_of(s.labels.clone(), s.points.iter().map(|p| p.1).sum()))
                        .collect()),
                    Ok((QueryResult::Vector(samples), _)) => Ok(samples
                        .iter()
                        .map(|s| series_of(s.labels.clone(), s.value))
                        .collect()),
                    Ok(_) => Err("another result".to_string()),
                    Err(ReadError::MetricPipelineError { error_type, .. }) => {
                        Err(error_type.clone())
                    }
                    Err(e) => Err(e.to_string()),
                };
                let ok = match (answer.clone(), needs) {
                    (Ok(series), false) => series == vec![format!("a=<absent>:{bytes}")],
                    (Ok(mut series), true) => {
                        series.sort();
                        series.join(",") == needed
                    }
                    (Err(e), true) => e == needed,
                    (Err(_), false) => false,
                };
                if !ok {
                    wrong.push(format!("{query} ({route:?}): {answer:?}"));
                }
                if !needs && route == Route10::Lowered {
                    lowered_no_label.push(comment.clone());
                }
                if svc == "t10p" {
                    pattern_cells.push((comment, needs, format!("{query} ({route:?})")));
                }
            }
        }
    }
    assert_eq!(cells, PARSERS.len() * NEEDS.len() * ROUTES.len());
    assert!(
        wrong.is_empty(),
        "{} of {cells} cells:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
    let mut all = lowered_no_label.clone();
    all.extend(pattern_cells.iter().map(|(c, _, _)| c.clone()));
    let logged = logged_624(&admin, &all).await;
    for comment in &lowered_no_label {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{comment}: no statement logged"));
        assert!(
            rows.iter().any(|r| is_lowered_624(&r.query))
                && rows.iter().all(|r| r.exception_code == 0),
            "{comment}: the lowered statement answered, with no fallback: {rows:#?}"
        );
    }
    for (comment, needs, what) in &pattern_cells {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{what}: no statement logged"));
        assert_eq!(
            rows.iter().any(|r| r.query.contains("arrayFold")),
            *needs,
            "{what}: the database runs the pattern exactly when a label is needed: {rows:#?}"
        );
    }

    // (R) A `regexp` reading after a skipped `pattern`: the pattern's empty
    // element keeps the regexp's position, element 2. The query needs no
    // labels, the regexp still extracts (the reference's has no early
    // return), and its `__error__` group fails the query. It does not lower
    // (two parsers), so every route reads the raw rows.
    land_stream_624(
        &admin,
        &db,
        t,
        970,
        "t10r",
        r#"{"s":"t10r","service_name":"t10r"}"#,
    )
    .await;
    land_line_624(&admin, &db, 970, "t10r", t + 10 * sec, "bad x", "").await;
    land_line_624(&admin, &db, 970, "t10r", t + 20 * sec, "ok y", "").await;
    const R_ROUTES: [Route10; 4] = [
        Route10::Lowered,
        Route10::TodaysOnly,
        Route10::Memory,
        Route10::Instant,
    ];
    let r_query =
        r#"absent_over_time({s="t10r"} | pattern "<p> <_>" | regexp "(?P<__error__>bad)" [1m])"#;
    let r_column =
        r#"[CAST([], 'Array(String)'), extractGroups(body, '(?P<__error__>bad)')] AS rx"#;
    let mut r_comments: Vec<(String, Route10)> = Vec::new();
    for route in R_ROUTES {
        let comment = format!("c624p3ct10r-{}", uuid::Uuid::new_v4().simple());
        let hooks = match route {
            Route10::TodaysOnly => hooks_624(true, None, None),
            Route10::Memory => hooks_624(false, None, Some(1)),
            _ => hooks_624(false, None, None),
        };
        let engine = LogQlEngine::new(
            data_client(&db).await,
            engine_config(&db, 64 * 1024 * 1024),
            pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        )
        .with_key_route_test_hooks(hooks)
        .with_query_log_comment(comment.clone());
        let params = if route == Route10::Instant {
            &instant
        } else {
            &range
        };
        let got = engine.query(&parse(r_query).expect("parse"), params).await;
        assert!(
            matches!(&got, Err(ReadError::MetricPipelineError { error_type, .. }) if error_type == "bad"),
            "{r_query} ({route:?}): {:?}",
            got.map(|r| r.0)
        );
        r_comments.push((comment, route));
    }
    let comments: Vec<String> = r_comments.iter().map(|(c, _)| c.clone()).collect();
    let logged = logged_624(&admin, &comments).await;
    for (comment, route) in &r_comments {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{route:?}: no statement logged"));
        assert!(
            rows.iter().any(|r| r.query.contains(r_column)),
            "{route:?}: the raw read's rx column is `{r_column}`: {rows:#?}"
        );
    }

    // (V) A `variants(...)` set sharing one `pattern` between a label-free
    // operand and a `by (a)` operand: the element is needed by one reader,
    // so it is sent ("every variant" that reads it must need no labels).
    land_stream_624(
        &admin,
        &db,
        t,
        971,
        "t10v",
        r#"{"s":"t10v","service_name":"t10v"}"#,
    )
    .await;
    land_line_624(&admin, &db, 971, "t10v", t + 10 * sec, "x y", "").await;
    land_line_624(&admin, &db, 971, "t10v", t + 20 * sec, "z", "").await;
    let v_query = r#"variants(sum(bytes_over_time({s="t10v"} | pattern "<a> <_>" [1m])), sum by (a) (bytes_over_time({s="t10v"} | pattern "<a> <_>" [1m]))) of ({s="t10v"} [1m])"#;
    for params in [&range, &instant] {
        let engine = LogQlEngine::new(
            data_client(&db).await,
            engine_config(&db, 64 * 1024 * 1024),
            pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        );
        let (result, _) = engine
            .query(&parse(v_query).expect("parse"), params)
            .await
            .unwrap_or_else(|e| panic!("{v_query}: {e}"));
        let mut got: Vec<String> = match result {
            QueryResult::Matrix(series) => series
                .iter()
                .map(|s| (s.labels.clone(), s.points.iter().map(|p| p.1).sum::<f64>()))
                .map(|(l, v)| {
                    let get = |k: &str| {
                        l.iter()
                            .find(|(n, _)| n == k)
                            .map(|(_, x)| format!("{x:?}"))
                    };
                    format!(
                        "{}/{}:{v}",
                        get("__variant__").unwrap_or_default(),
                        get("a").unwrap_or_else(|| "<absent>".to_string())
                    )
                })
                .collect(),
            QueryResult::Vector(samples) => samples
                .iter()
                .map(|s| {
                    let get = |k: &str| {
                        s.labels
                            .iter()
                            .find(|(n, _)| n == k)
                            .map(|(_, x)| format!("{x:?}"))
                    };
                    format!(
                        "{}/{}:{}",
                        get("__variant__").unwrap_or_default(),
                        get("a").unwrap_or_else(|| "<absent>".to_string()),
                        s.value
                    )
                })
                .collect(),
            other => panic!("{v_query}: {other:?}"),
        };
        got.sort();
        assert_eq!(
            got,
            [r#""0"/<absent>:4"#, r#""1"/"x":3"#, r#""1"/"z":1"#],
            "{v_query}"
        );
    }
    drop_db_624(&admin, &db).await;
}

/// Issue #624, part 3d: T2's lines, each with whether the reference's
/// decoder reads it cleanly (no error, no field directly followed by a byte
/// above `' '`, every key printable ASCII) — the lines the statement decides.
const LINES_624P3D: [(&str, bool); 40] = [
    ("level=info msg=hello", true),
    ("level=info level=error", true),
    ("level= level=error", true),
    ("level level=warn", true),
    ("level=\"\" level=x", true),
    ("a.b=1 a_b=2", true),
    ("a_b=1 a.b=2", true),
    ("1x=7 _1x=8", true),
    ("_1x=8 1x=7", true),
    ("b.c=4 b_c=5", true),
    ("msg=\"said \\\"hi\\\"\" level=info", true),
    ("level=info msg=\"unterminated", false),
    ("level=info =x", false),
    ("=x level=info", false),
    ("level=\"x\"b=1", false),
    ("level=info a=b=c", false),
    ("level=info a\"b", false),
    ("", true),
    ("   ", true),
    ("level=info\u{1}msg=x", true),
    ("lvl=1 level=2 level=3", true),
    ("level=error", true),
    ("msg=\"a\\x\" level=info", false),
    ("level=info\nmsg=x", true),
    ("é=1 level=info", false),
    ("level=\u{fffd}", true),
    ("level_extracted=z level=y", true),
    ("level=\"\\u0041\" msg=x", true),
    ("a.b=1 a_b=2 level=info", true),
    ("__error__=boom level=x", true),
    ("k=v\u{fffd} level=y", true),
    ("level=\"x y\" msg=\"k=v z=w\"", true),
    ("level_extracted=z level=w", true),
    ("lvl=w level=v lvl=q", true),
    ("level=\"a\\\"b\" lvl=\"\\ud800x\"", true),
    ("b.c=4 level=k _1x=3", true),
    ("msg=a=b level=x", false),
    ("level=\"\\x\"", false),
    ("a-b=1 level=warn a_b=", true),
    ("level=info msg=\"said \\\"hi\\\"\"", true),
];

/// Issue #624, part 3d: T2's queries; `{S}` is one stream's selector.
const FORMS_624P3D: [&str; 15] = [
    r#"sum by (level) (count_over_time({S} | logfmt [1m]))"#,
    r#"sum by (level, msg) (count_over_time({S} | logfmt --strict [1m]))"#,
    r#"sum by (a_b) (count_over_time({S} | logfmt --keep-empty [1m]))"#,
    r#"sum by (_1x, b_c) (count_over_time({S} | logfmt --strict --keep-empty [1m]))"#,
    r#"sum by (level) (count_over_time({S} | logfmt | msg != "" [1m]))"#,
    r#"count_over_time({S} | logfmt lvl="level" [1m])"#,
    r#"sum by (lvl) (count_over_time({S} | logfmt --strict lvl="level", m="msg" [1m]))"#,
    r#"sum by (level) (count_over_time({S} | logfmt level="level" [1m]))"#,
    r#"count_over_time({S} | logfmt --strict a="b_c" [1m])"#,
    r#"count_over_time({S} | logfmt a="a", b="a_b" [1m])"#,
    r#"sum by (level_extracted) (count_over_time({S} | logfmt [1m]))"#,
    r#"sum by (level) (bytes_over_time({S} | logfmt | __error__="" [1m]))"#,
    r#"sum by (level) (count_over_time({S} | logfmt | drop msg [1m]))"#,
    r#"count_over_time({S} | logfmt __error__="level" [1m])"#,
    r#"absent_over_time({S} | logfmt lvl="level" | lvl="info" [1m])"#,
];

/// An engine answer as text: each series' sorted labels and its total, or
/// the error.
fn answer_624p3d<W>(got: &Result<(QueryResult, W), ReadError>) -> String {
    match got {
        Ok((QueryResult::Matrix(series), _)) => {
            let mut s: Vec<String> = series
                .iter()
                .map(|s| {
                    let mut l = s.labels.clone();
                    l.sort();
                    format!("{l:?}={}", s.points.iter().map(|p| p.1).sum::<f64>())
                })
                .collect();
            s.sort();
            s.join(" ")
        }
        Ok(_) => "another result".to_string(),
        Err(ReadError::MetricPipelineError { error_type, .. }) => format!("error {error_type}"),
        Err(e) => format!("failed {e}"),
    }
}

/// **T2 (issue #624, part 3d).** Every line of `LINES_624P3D` alone in a
/// stream without and with a stream label `level`, under every query of
/// `FORMS_624P3D`, and the decided lines together in one stream: the
/// lowered answer equals today's route's, a decided line is answered by the
/// statement alone and an undecided one hands over with code 395; and the
/// decided stream under a one-byte ceiling hands over with code 241.
#[tokio::test]
async fn a_logfmt_count_answers_as_todays_route() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3d_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    let cols = "(received_ms, kind, service, fingerprint, timestamp_ns, severity, body, \
                structured_metadata, month, labels, updated_ns, pattern, pattern_count)";
    let mut streams = Vec::new();
    let mut rows = Vec::new();
    let mut stream = |svc: &str, fp: u64, extra: &str| {
        streams.push(format!(
            "(toUnixTimestamp64Milli(now64(3)), 1, '{svc}', toUInt128({fp}), 0, 0, '', '', \
             toStartOfMonth(toDate(fromUnixTimestamp64Nano(toInt64({t})))), \
             '{{\"s\":\"{svc}\",\"service_name\":\"{svc}\"{extra}}}', 0, '', 0)"
        ));
    };
    let mut line = |svc: &str, fp: u64, at: i64, body: &str| {
        rows.push(format!(
            "(toUnixTimestamp64Milli(now64(3)), 0, '{svc}', toUInt128({fp}), toInt64({at}), 0, \
             {}, '', toDate(0), '', 0, '', 0)",
            literal(body).as_sql()
        ));
    };
    for (i, (body, _)) in LINES_624P3D.iter().enumerate() {
        for (b, extra) in [(0u64, ""), (1, r#","level":"stream""#)] {
            let svc = format!("p3d{b}x{i}");
            stream(&svc, 2000 + 2 * i as u64 + b, extra);
            line(&svc, 2000 + 2 * i as u64 + b, t + 10 * sec, body);
        }
    }
    stream("p3dall", 1999, "");
    for (i, (body, decided)) in LINES_624P3D.iter().enumerate() {
        if *decided {
            line("p3dall", 1999, t + sec + i as i64 * 1_000_000_000, body);
        }
    }
    for chunk in [streams, rows] {
        exec_624(
            &admin,
            &format!(
                "INSERT INTO {db}.log_landing {cols} VALUES {}",
                chunk.join(", ")
            ),
        )
        .await;
    }
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t + 60 * sec,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    // (query, decided, a one-byte ceiling, the lowered request's comment, answers)
    let mut cells: Vec<(String, bool, bool, String, String, String)> = Vec::new();
    let (db_r, range_r) = (&db, &range);
    let ask = move |query: String, decided: bool, memory: bool| async move {
        let mut out = Vec::new();
        for todays in [true, false] {
            let comment = format!("c624p3d-{}", uuid::Uuid::new_v4().simple());
            let hooks = hooks_624(todays, None, if memory && !todays { Some(1) } else { None });
            let engine = LogQlEngine::new(
                data_client(db_r).await,
                engine_config(db_r, 64 * 1024 * 1024),
                pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            )
            .with_key_route_test_hooks(hooks)
            .with_query_log_comment(comment.clone());
            let got = engine.query(&parse(&query).expect("parse"), range_r).await;
            out.push((comment, answer_624p3d(&got)));
        }
        (
            query,
            decided,
            memory,
            out[1].0.clone(),
            out[0].1.clone(),
            out[1].1.clone(),
        )
    };
    for form in FORMS_624P3D {
        for (i, (_, decided)) in LINES_624P3D.iter().enumerate() {
            for b in 0..2 {
                let query = form.replace("{S}", &format!(r#"{{s="p3d{b}x{i}"}}"#));
                cells.push(ask(query, *decided, false).await);
            }
        }
        // A destination named `__error__` fails the query with the value of
        // whichever line the route reads first; one line per stream above.
        if !form.contains(r#"__error__="level""#) {
            cells.push(ask(form.replace("{S}", r#"{s="p3dall"}"#), true, false).await);
        }
    }
    cells.push(
        ask(
            FORMS_624P3D[0].replace("{S}", r#"{s="p3dall"}"#),
            true,
            true,
        )
        .await,
    );
    assert_eq!(
        cells.len(),
        FORMS_624P3D.len() * (2 * LINES_624P3D.len() + 1) - 1 + 1
    );
    let comments: Vec<String> = cells.iter().map(|c| c.3.clone()).collect();
    let logged = logged_624(&admin, &comments).await;
    let mut wrong: Vec<String> = Vec::new();
    let mut non_empty = 0usize;
    for (query, decided, memory, comment, todays, lowered) in &cells {
        if !lowered.is_empty() {
            non_empty += 1;
        }
        if todays != lowered {
            wrong.push(format!("{query}: today's {todays} | lowered {lowered}"));
        }
        let rows = logged.get(comment).cloned().unwrap_or_default();
        let statement = rows.iter().find(|r| r.query.contains(" AS fields"));
        let handed = rows.iter().any(|r| is_todays_624(&r.query));
        let want = match (decided, memory) {
            (_, true) => Some(241),
            (true, false) => Some(0),
            (false, false) => Some(395),
        };
        let route_ok = statement.map(|r| r.exception_code) == want && handed == (want != Some(0));
        if !route_ok {
            wrong.push(format!(
                "{query}: statement {:?}, today's read {handed}, want {want:?}",
                statement.map(|r| r.exception_code)
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} cells:\n{}",
        wrong.len(),
        cells.len(),
        wrong.join("\n")
    );
    assert!(non_empty >= 1000, "{non_empty} non-empty answers");
    drop_db_624(&admin, &db).await;
}

/// **T6 (issue #624, part 3d).** A targeted source `id_extracted` is not a
/// field the parser reads, so 50 lines whose `id` differs form one group:
/// the statement sends one row, with code 0, and answers as today's route.
#[tokio::test]
async fn a_targeted_logfmt_count_groups_only_on_the_fields_it_reads() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3dt6_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    land_stream_624(
        &admin,
        &db,
        t,
        2500,
        "p3did",
        r#"{"s":"p3did","service_name":"p3did"}"#,
    )
    .await;
    let rows: Vec<String> = (0..50)
        .map(|i| {
            format!(
                "(toUnixTimestamp64Milli(now64(3)), 0, 'p3did', toUInt128(2500), toInt64({}), 0, \
                 {}, '', toDate(0), '', 0, '', 0)",
                t + sec + i * 100_000_000,
                literal(&format!("id={i} level=info")).as_sql()
            )
        })
        .collect();
    exec_624(
        &admin,
        &format!(
            "INSERT INTO {db}.log_landing (received_ms, kind, service, fingerprint, timestamp_ns, \
             severity, body, structured_metadata, month, labels, updated_ns, pattern, \
             pattern_count) VALUES {}",
            rows.join(", ")
        ),
    )
    .await;
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t + 60 * sec,
            end_ns: t + 60 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let query = r#"count_over_time({s="p3did"} | logfmt x="id_extracted" [1m])"#;
    let mut answers = Vec::new();
    let mut comment = String::new();
    for todays in [true, false] {
        comment = format!("c624p3dt6-{}", uuid::Uuid::new_v4().simple());
        let engine = LogQlEngine::new(
            data_client(&db).await,
            engine_config(&db, 64 * 1024 * 1024),
            pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        )
        .with_key_route_test_hooks(hooks_624(todays, None, None))
        .with_query_log_comment(comment.clone());
        answers.push(answer_624p3d(
            &engine.query(&parse(query).expect("parse"), &range).await,
        ));
    }
    assert_eq!(answers[0], answers[1], "the lowered answer is today's");
    let logged = logged_624(&admin, std::slice::from_ref(&comment)).await;
    let rows = logged.get(&comment).expect("logged");
    let statement = rows
        .iter()
        .find(|r| r.query.contains(" AS fields"))
        .unwrap_or_else(|| panic!("no lowered statement: {rows:#?}"));
    assert_eq!(
        (statement.exception_code, statement.result_rows),
        (0, 1),
        "one group for 50 ids"
    );
    drop_db_624(&admin, &db).await;
}

/// **T3 (issue #624, part 3d).** The statement's two columns over the 650
/// lines of `fixtures/logfmt_fields/reference_fields.tsv` — every field of
/// a table alone and every ordered pair — for each of its three name sets:
/// `decided` is the fixture's `ok`, and on a decided line `fields` is the
/// reference decoder's.
#[tokio::test]
async fn the_database_logfmt_fields_are_the_reference_decoders() {
    skip_unless_live!();
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct FieldsRow {
        i: u32,
        decided: u8,
        fields: String,
    }
    const SETS: [&[&str]; 3] = [
        &["level"],
        &["_1x", "a_b", "level", "level_extracted"],
        &["__error__", "b_c", "level", "lvl", "msg"],
    ];
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/logfmt_fields/reference_fields.tsv"
    ))
    .expect("the fixture");
    let rows: Vec<(String, bool, [String; 3])> = text
        .lines()
        .filter(|l| !l.starts_with('#') && !l.starts_with("line\t"))
        .map(|l| {
            let c: Vec<&str> = l.split('\t').collect();
            let s = |x: &str| serde_json::from_str::<String>(x).expect("a JSON string");
            (s(c[0]), c[1] == "1", [s(c[2]), s(c[3]), s(c[4])])
        })
        .collect();
    assert_eq!(rows.len(), 650);
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p3dt3_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    exec_624(
        &admin,
        &format!("CREATE TABLE {db}.lf (i UInt32, body String) ENGINE = MergeTree ORDER BY i"),
    )
    .await;
    let values: Vec<String> = rows
        .iter()
        .enumerate()
        .map(|(i, (line, _, _))| format!("({i}, {})", literal(line).as_sql()))
        .collect();
    exec_624(
        &admin,
        &format!("INSERT INTO {db}.lf VALUES {}", values.join(", ")),
    )
    .await;
    let client = data_client(&db).await;
    let mut cells = 0usize;
    let mut wrong: Vec<String> = Vec::new();
    for (n, set) in SETS.iter().enumerate() {
        let names: Vec<String> = set.iter().map(|s| s.to_string()).collect();
        let [decided, fields] = sql::logfmt_count_columns(&names);
        let query = format!("SELECT i, {decided}, {fields} FROM lf ORDER BY i").replace('?', "??");
        let mut stream = client
            .query_stream::<FieldsRow>(&query, &QuerySettings::new())
            .await
            .unwrap_or_else(|e| panic!("{e}\n{query}"));
        while let Some(row) = stream.next().await {
            let row = row.expect("a row");
            let (line, ok, want) = &rows[row.i as usize];
            cells += 1;
            if (row.decided == 1) != *ok || (*ok && row.fields != want[n]) {
                wrong.push(format!(
                    "set {n} {line:?}: decided {} fields {:?}, want {ok} {:?}",
                    row.decided, row.fields, want[n]
                ));
            }
        }
    }
    assert_eq!(cells, SETS.len() * rows.len());
    assert!(
        wrong.is_empty(),
        "{} of {cells} cells:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
    drop_db_624(&admin, &db).await;
}

/// **T1 (issue #624, part 3d-3): a filter after a targeted parser returns
/// what the evaluator returns.** Each line alone in a stream; the log
/// query's lines equal the expected count and those of the same query with
/// its filter not pushed (a regular expression, or the same filter's
/// equivalent). Each pushed request's statements must carry the mapped
/// field, or for a collision case the stream guard, so a filter or guard
/// that is not pushed fails the test.
#[tokio::test]
async fn a_filter_after_a_targeted_parser_returns_what_the_evaluator_returns() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_d33_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    const BASE: &str = r#","status":"base""#;
    const GUARD: &str = "(fingerprint IN (toUInt128('";
    // (line, extra stream labels, pushed pipeline, the not-pushed control,
    //  expected lines, what the pushed statements must carry)
    let cases: &[(&str, &str, &str, &str, usize, &str)] = &[
        (
            "status=500 code=200",
            "",
            r#"| logfmt status="code" | status="200""#,
            r#"| logfmt status="code" | status=~"200""#,
            1,
            "has(['code', 'status'],",
        ),
        (
            "status=200 code=500",
            "",
            r#"| logfmt status="code" | status!="200""#,
            r#"| logfmt status="code" | status!~"200""#,
            1,
            "has(['code', 'status'],",
        ),
        (
            "status=500 code",
            "",
            r#"| logfmt status="code" | status!="500""#,
            r#"| logfmt status="code" | status!~"500""#,
            1,
            "has(['code', 'status'],",
        ),
        (
            r#"{"status":"500","code":"200"}"#,
            "",
            r#"| json status="code" | status="200""#,
            r#"| json status="code" | status=~"200""#,
            1,
            "JSONExtractString(body, 'code')",
        ),
        (
            r#"{"status":"200","code":"500"}"#,
            "",
            r#"| json status="code" | status!="200""#,
            r#"| json status="code" | status!~"200""#,
            1,
            "JSONExtractString(body, 'code')",
        ),
        (
            r#"{"a":{"b":"200"},"status":"500"}"#,
            "",
            r#"| json status="a.b" | status="200""#,
            r#"| json status="a.b" | status=~"200""#,
            1,
            "JSONExtractString(body, 'a', 'b')",
        ),
        (
            r#"{"status":100,"code":300}"#,
            "",
            r#"| json status="code" | status >= 200"#,
            r#"| json status="code" | status=~"300""#,
            1,
            "JSONExtractFloat(body, 'code')",
        ),
        (
            "code=200",
            BASE,
            r#"| logfmt status="code" | status_extracted="200""#,
            r#"| logfmt status="code" | status_extracted=~"200""#,
            1,
            GUARD,
        ),
        (
            r#"{"code":"200"}"#,
            BASE,
            r#"| json status="code" | status_extracted="200""#,
            r#"| json status="code" | status_extracted=~"200""#,
            1,
            GUARD,
        ),
        (
            "code=200 status=500",
            "",
            r#"| logfmt status="code" | status="200""#,
            r#"| logfmt status="code" | status=~"200""#,
            0,
            "has(['code', 'status'],",
        ),
        (
            r#"x=1 code="2 0""#,
            "",
            r#"| logfmt status="code" | status="2 0""#,
            r#"| logfmt status="code" | status=~"2 0""#,
            1,
            "has(['code', 'status'],",
        ),
        (
            r#"{"other":5}"#,
            "",
            r#"| json status="code" | other >= 1"#,
            r#"| json status="code" | other=~"5""#,
            0,
            "(0 OR structured_metadata != '')",
        ),
        (
            "code=200",
            "",
            r#"| logfmt status="\"code\"" | status="200""#,
            r#"| logfmt status="\"code\"" | status=~"200""#,
            1,
            "has(['code', 'status'],",
        ),
        (
            "code=200",
            "",
            r#"| logfmt status="code", status="other" | status!="200""#,
            r#"| logfmt status="code", status="other" | status!~"200""#,
            1,
            "has(['other', 'status'],",
        ),
    ];
    for (i, (line, extra, _, _, _, _)) in cases.iter().enumerate() {
        let fp = 9600 + i as u64;
        land_stream_624(
            &admin,
            &db,
            t,
            fp,
            "d33",
            &format!(r#"{{"s":"d33k{i}","service_name":"d33"{extra}}}"#),
        )
        .await;
        land_line_624(&admin, &db, fp, "d33", t + sec, line, "").await;
    }
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t + 120 * sec,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let lines = |result: Result<(QueryResult, _), ReadError>, q: &str| match result {
        Ok((QueryResult::Streams { items, .. }, _)) => {
            items.iter().map(|s| s.entries.len()).sum::<usize>()
        }
        other => panic!("{q}: {:?}", other.map(|_| ())),
    };
    let mut wrong: Vec<String> = Vec::new();
    let mut comments: Vec<(String, usize)> = Vec::new();
    for (i, (_, _, pushed, control, want, _)) in cases.iter().enumerate() {
        let comment = format!("c624d33-{}", uuid::Uuid::new_v4().simple());
        let engine = LogQlEngine::new(
            data_client(&db).await,
            engine_config(&db, 64 * 1024 * 1024),
            pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        )
        .with_query_log_comment(comment.clone());
        let q = format!(r#"{{s="d33k{i}"}} {pushed}"#);
        let got = lines(engine.query(&parse(&q).expect("parse"), &params).await, &q);
        let cq = format!(r#"{{s="d33k{i}"}} {control}"#);
        let engine = LogQlEngine::new(
            data_client(&db).await,
            engine_config(&db, 64 * 1024 * 1024),
            pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        );
        let not_pushed = lines(
            engine.query(&parse(&cq).expect("parse"), &params).await,
            &cq,
        );
        if got != *want || not_pushed != *want {
            wrong.push(format!(
                "{q}: {got} lines, not pushed {not_pushed}, want {want}"
            ));
        }
        comments.push((comment, i));
    }
    let names: Vec<String> = comments.iter().map(|(c, _)| c.clone()).collect();
    let logged = logged_624(&admin, &names).await;
    for (comment, i) in &comments {
        let (_, _, pushed, _, _, carries) = cases[*i];
        let carries = if carries == GUARD {
            format!("{GUARD}{}'))", 9600 + *i as u64)
        } else {
            carries.to_string()
        };
        let rows = logged.get(comment).cloned().unwrap_or_default();
        if !rows.iter().any(|r| r.query.contains(&carries)) {
            wrong.push(format!(
                "{pushed}: no statement carries {carries:?}: {:#?}",
                rows.iter().map(|r| &r.query).collect::<Vec<_>>()
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} cases:\n{}",
        wrong.len(),
        cases.len(),
        wrong.join("\n")
    );
    drop_db_624(&admin, &db).await;
}

// ---------------------------------------------------------------------
// Issue #624, parts 4 and 5: the `| unwrap` reducers, and instant queries
// with a pipeline, in the database.
// ---------------------------------------------------------------------

/// The reduced statement: its fan-out of each run to its grid points.
fn is_reduced_624p4(query: &str) -> bool {
    query.contains("ARRAY JOIN range(lo, hi + 1) AS k")
}

/// Today's range read, `metric_raw_samples_sliding`.
fn is_todays_sliding_624p4(query: &str) -> bool {
    is_todays_624(query)
        && query.contains("ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC")
}

/// Today's instant read, `metric_raw_samples`.
fn is_todays_instant_624p4(query: &str) -> bool {
    is_todays_624(query) && query.contains("ORDER BY timestamp_ns ASC")
}

/// One instant answer: sorted labels and the value's bits, sorted.
type InstantAnswer624p4 = Vec<(Vec<(String, String)>, u64)>;

/// One instant query at `at_ns` through an engine built for this run.
async fn run_instant_624p4(
    db: &str,
    hooks: pulsus_read::logql::exec::KeyRouteTestHooks,
    comment: Option<&str>,
    query: &str,
    at_ns: i64,
) -> Result<InstantAnswer624p4, ReadError> {
    let mut engine = LogQlEngine::new(
        data_client(db).await,
        engine_config(db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    )
    .with_key_route_test_hooks(hooks);
    if let Some(c) = comment {
        engine = engine.with_query_log_comment(c.to_string());
    }
    let params = QueryParams {
        spec: QuerySpec::Instant { at_ns },
        limit: 100,
        direction: Direction::Backward,
    };
    let (result, _warnings) = engine.query(&parse(query).expect("parse"), &params).await?;
    let QueryResult::Vector(samples) = result else {
        panic!("{query}: expected a vector, got {result:?}");
    };
    let mut out: InstantAnswer624p4 = samples
        .into_iter()
        .map(|s| {
            let mut labels = s.labels;
            labels.sort();
            (labels, s.value.to_bits())
        })
        .collect();
    out.sort();
    Ok(out)
}

/// The two instant answers agree: equal vectors, or the same pipeline error.
fn same_instant_outcome_624p4(
    what: &str,
    lowered: &Result<InstantAnswer624p4, ReadError>,
    todays: &Result<InstantAnswer624p4, ReadError>,
) {
    match (lowered, todays) {
        (Ok(a), Ok(b)) => assert_eq!(a, b, "{what}: the lowered answer must be today's"),
        (
            Err(ReadError::MetricPipelineError { error_type: a, .. }),
            Err(ReadError::MetricPipelineError { error_type: b, .. }),
        ) => assert_eq!(a, b, "{what}: the same pipeline error"),
        (a, b) => panic!("{what}: lowered {a:?}, today's {b:?}"),
    }
}

/// A range query on the 60 s grid over `[start, end]`.
fn grid_624p4(start_ns: i64, end_ns: i64) -> QueryParams {
    QueryParams {
        spec: QuerySpec::Range {
            start_ns,
            end_ns,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    }
}

/// The landing table's columns, in the order every seed below lists them.
const LANDING_624P4: &str = "(received_ms, kind, service, fingerprint, timestamp_ns, severity, \
     body, structured_metadata, month, labels, updated_ns, pattern, pattern_count)";

/// T4's corpus, in a fresh database: `(admin, db, T)`.
///
/// `c624p4`: 20 streams `{inst="<s>", pod="p<s % 5>"}`, one line a second
/// from `T + 1s` to `T + 600s`, every stream at the same seconds, so lines of
/// two streams with the same `route` meet at one nanosecond everywhere. Every
/// 37th second a stream has a second line at the same nanosecond with other
/// values and the same `level` and `route`. Metadata `dur` (numbers), `lat`
/// (durations), `size` (bytes), `level`, `route` (one value holding `"` and
/// `\`), `trace_id`; one line in a hundred fails each conversion (`n/a`,
/// `abc`, `kb`).
///
/// `c624p4lean`: 4 streams `{pod="q<i>"}`, the same seconds, metadata only
/// `dur`, `lat`, `size` and `route`, each from a short cycle.
async fn seed_reduced_corpus_624p4() -> (ChClient, String, i64) {
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let month = format!("toStartOfMonth(toDate(fromUnixTimestamp64Nano(toInt64({t}))))");
    exec_624(
        &admin,
        &format!(
            "INSERT INTO {db}.log_landing {LANDING_624P4} \
             SELECT toUnixTimestamp64Milli(now64(3)), 1, 'c624p4', toUInt128(6240400 + number), \
             0, 0, '', '', {month}, concat('{{\"inst\":\"', toString(number), '\",\"pod\":\"p', \
             toString(number % 5), '\",\"service_name\":\"c624p4\"}}'), 0, '', 0 \
             FROM numbers(20)"
        ),
    )
    .await;
    exec_624(
        &admin,
        &format!(
            "INSERT INTO {db}.log_landing {LANDING_624P4} \
             SELECT toUnixTimestamp64Milli(now64(3)), 1, 'c624p4lean', \
             toUInt128(6240500 + number), 0, 0, '', '', {month}, concat('{{\"pod\":\"q', \
             toString(number), '\",\"service_name\":\"c624p4lean\"}}'), 0, '', 0 \
             FROM numbers(4)"
        ),
    )
    .await;
    // `s` the stream, `i` the second, `k` the value seed; `tie` adds the
    // second line of a same-stream tie.
    let values = |k: &str, fail: &str| {
        format!(
            "if({fail} = 0, 'n/a', concat(toString({k} % 997), '.', toString({k} % 10))) AS dur, \
             if({fail} = 1, 'abc', multiIf({k} % 3 = 0, concat(toString({k} % 900), 'ms'), \
               {k} % 3 = 1, concat(toString({k} % 50), '.', toString({k} % 7), 's'), \
               concat(toString({k} % 5), 'm', toString({k} % 60), 's'))) AS lat, \
             if({fail} = 2, 'kb', multiIf({k} % 3 = 0, concat(toString({k} % 900), 'KB'), \
               {k} % 3 = 1, concat(toString({k} % 50), ' MiB'), \
               concat(toString({k} % 10000), 'b'))) AS size"
        )
    };
    for tie in [false, true] {
        let (k, filter) = if tie {
            ("(i * 7 + s * 13 + 5)", "WHERE i % 37 = 0")
        } else {
            ("(i * 7 + s * 13)", "")
        };
        exec_624(
            &admin,
            &format!(
                "INSERT INTO {db}.log_landing {LANDING_624P4} \
                 SELECT toUnixTimestamp64Milli(now64(3)), 0, 'c624p4', toUInt128(6240400 + s), \
                 toInt64({t}) + toInt64(i) * 1000000000, 0, concat('x', toString({k})), \
                 concat('{{\"dur\":', toJSONString(dur), ',\"lat\":', toJSONString(lat), \
                   ',\"level\":', toJSONString(level), ',\"route\":', toJSONString(route), \
                   ',\"size\":', toJSONString(size), ',\"trace_id\":', toJSONString(trace), '}}'), \
                 toDate(0), '', 0, '', 0 \
                 FROM (SELECT intDiv(number, 600) AS s, number % 600 + 1 AS i, \
                       {}, ['info', 'warn', 'error'][i % 3 + 1] AS level, \
                       ['ra', 'rb', 'r\"q\\\\z', 'rd'][(i + s) % 4 + 1] AS route, \
                       hex({k} * 1000 + s) AS trace \
                       FROM numbers(20 * 600) {filter})",
                values(k, "((i + s) % 100)")
            ),
        )
        .await;
    }
    exec_624(
        &admin,
        &format!(
            "INSERT INTO {db}.log_landing {LANDING_624P4} \
             SELECT toUnixTimestamp64Milli(now64(3)), 0, 'c624p4lean', toUInt128(6240500 + s), \
             toInt64({t}) + toInt64(i) * 1000000000, 0, 'y', \
             concat('{{\"dur\":', toJSONString(dur), ',\"lat\":', toJSONString(lat), \
               ',\"route\":', toJSONString(route), ',\"size\":', toJSONString(size), '}}'), \
             toDate(0), '', 0, '', 0 \
             FROM (SELECT intDiv(number, 600) AS s, number % 600 + 1 AS i, {}, \
                   ['ra', 'rb'][(i + s) % 2 + 1] AS route FROM numbers(4 * 600))",
            values("(i % 5 + s)", "((i + s) % 100)")
        ),
    )
    .await;
    (admin, db, t)
}

/// One count off `db`'s lines.
async fn count_624p4(admin: &ChClient, sql: &str) -> u64 {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct N {
        n: u64,
    }
    let mut stream = admin
        .query_stream::<N>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let n = stream
        .next()
        .await
        .expect("one row")
        .expect("decode the count")
        .n;
    drop(stream);
    n
}

/// T4's queries at `range`: `(query, lowers)`.
fn reduced_queries_624p4(range: &str) -> Vec<String> {
    let main = r#"{service_name="c624p4"}"#;
    let lean = r#"{service_name="c624p4lean"}"#;
    const VALUES: [(&str, &str); 3] = [
        ("dur", "lat, size"),
        ("duration(lat)", "dur, size"),
        ("bytes(size)", "dur, lat"),
    ];
    let mut out = Vec::new();
    for op in [
        "avg_over_time",
        "min_over_time",
        "max_over_time",
        "stddev_over_time",
        "stdvar_over_time",
        "quantile_over_time",
        "first_over_time",
        "last_over_time",
    ] {
        let param = if op == "quantile_over_time" {
            "0.9, "
        } else {
            ""
        };
        for (value, others) in VALUES {
            let inner = format!(r#"{op}({param}{main} | unwrap {value} | __error__="" [{range}])"#);
            out.push(format!("{inner} by (route)"));
            out.push(format!("{inner} without (trace_id, {others})"));
        }
    }
    for op in ["sum_over_time", "rate", "rate_counter"] {
        for (value, _) in VALUES {
            let over =
                |sel: &str| format!(r#"{op}({sel} | unwrap {value} | __error__="" [{range}])"#);
            out.push(over(lean));
            if op != "rate_counter" {
                out.push(format!("sum by (route) ({})", over(main)));
            }
            out.push(format!("max by (route) ({})", over(lean)));
        }
    }
    out
}

/// **T4 (issue #624, part 4): every lowered `| unwrap` reducer answers what
/// today's route answers, bit for bit**, keyed `by` and `without`, under a
/// parent `sum` and a parent `max`, at a range equal to, above and below the
/// step. The query log holds the reduced statement for one run and today's
/// sliding read for the other.
#[tokio::test]
async fn unwrap_reducers_answer_as_todays_route() {
    skip_unless_live!();
    let (admin, db, t) = seed_reduced_corpus_624p4().await;
    // Non-vacuous: both kinds of tie, each between two values.
    let same_stream = count_624p4(
        &admin,
        &format!(
            "SELECT count() AS n FROM (SELECT fingerprint, timestamp_ns FROM {db}.log_samples \
             WHERE service = 'c624p4' GROUP BY fingerprint, timestamp_ns \
             HAVING uniqExact(JSONExtractString(structured_metadata, 'dur')) > 1 \
             AND uniqExact(JSONExtractString(structured_metadata, 'route')) = 1)"
        ),
    )
    .await;
    assert!(same_stream >= 100, "same-stream ties: {same_stream}");
    let cross_stream = count_624p4(
        &admin,
        &format!(
            "SELECT count() AS n FROM (SELECT timestamp_ns FROM {db}.log_samples \
             WHERE service = 'c624p4' \
             GROUP BY JSONExtractString(structured_metadata, 'route'), timestamp_ns \
             HAVING uniqExact(fingerprint) > 1 \
             AND uniqExact(JSONExtractString(structured_metadata, 'dur')) > 1)"
        ),
    )
    .await;
    assert!(cross_stream >= 100, "cross-stream ties: {cross_stream}");

    let params = grid_624p4(t, t + 600_000_000_000);
    let mut comments: Vec<(String, bool)> = Vec::new();
    let mut nonempty = 0usize;
    let mut total = 0usize;
    for range in ["60s", "150s", "30s"] {
        for query in reduced_queries_624p4(range) {
            let lowered_comment = format!("c624p4t4-{}", uuid::Uuid::new_v4().simple());
            let todays_comment = format!("c624p4t4-{}", uuid::Uuid::new_v4().simple());
            let lowered = run_624(
                &db,
                hooks_624(false, None, None),
                Some(&lowered_comment),
                &query,
                &params,
            )
            .await;
            let todays = run_624(
                &db,
                hooks_624(true, None, None),
                Some(&todays_comment),
                &query,
                &params,
            )
            .await;
            same_outcome_624(&query, &lowered, &todays);
            total += 1;
            if lowered.as_ref().is_ok_and(|a| !a.is_empty()) {
                nonempty += 1;
            }
            comments.push((lowered_comment, true));
            comments.push((todays_comment, false));
        }
    }
    assert_eq!(total, 3 * (8 * 3 * 2 + 3 * 3 * 3 - 3));
    assert_eq!(nonempty, total, "every query answers");
    let all: Vec<String> = comments.iter().map(|(c, _)| c.clone()).collect();
    let logged = logged_624(&admin, &all).await;
    for (comment, lowered) in &comments {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{comment}: no statement logged"));
        if *lowered {
            assert!(
                rows.iter()
                    .any(|r| is_reduced_624p4(&r.query) && r.exception_code == 0),
                "{comment}: the reduced statement answers: {rows:#?}"
            );
            assert!(
                !rows.iter().any(|r| is_todays_sliding_624p4(&r.query)),
                "{comment}: no read of today's route: {rows:#?}"
            );
        } else {
            assert!(
                rows.iter().any(|r| is_todays_sliding_624p4(&r.query)),
                "{comment}: {rows:#?}"
            );
            assert!(
                !rows.iter().any(|r| is_reduced_624p4(&r.query)),
                "{comment}: {rows:#?}"
            );
        }
    }
    drop_db_624(&admin, &db).await;
}

/// **T8 (issue #624, part 4): a reduced statement that runs out of memory
/// hands the query to today's route, once**, and the answer is today's.
#[tokio::test]
async fn a_reduced_query_out_of_memory_answers_on_todays_route() {
    skip_unless_live!();
    let (admin, db, t) = seed_reduced_corpus_624p4().await;
    let params = grid_624p4(t, t + 600_000_000_000);
    let query =
        r#"avg_over_time({service_name="c624p4"} | unwrap dur | __error__="" [150s]) by (route)"#;
    let comment = format!("c624p4t8-{}", uuid::Uuid::new_v4().simple());
    let got = run_624(
        &db,
        hooks_624(false, None, Some(1)),
        Some(&comment),
        query,
        &params,
    )
    .await
    .expect("today's route answers");
    let want = run_624(&db, hooks_624(true, None, None), None, query, &params)
        .await
        .expect("today's route answers");
    assert_eq!(got, want);
    assert!(!got.is_empty());
    let logged = logged_624(&admin, std::slice::from_ref(&comment)).await;
    let rows = logged.get(&comment).cloned().unwrap_or_default();
    let reduced: Vec<&CommentedRow624> =
        rows.iter().filter(|r| is_reduced_624p4(&r.query)).collect();
    assert_eq!(reduced.len(), 1, "{rows:#?}");
    assert_eq!(reduced[0].exception_code, 241, "{rows:#?}");
    assert_eq!(
        rows.iter()
            .filter(|r| is_todays_sliding_624p4(&r.query))
            .count(),
        1,
        "today's route runs once: {rows:#?}"
    );
    drop_db_624(&admin, &db).await;
}

/// The reference's Go code's answers for T5: bits per reducer, at window 1,
/// window 2 and the instant.
const T5_EXPECTED_624P4: [(&str, [u64; 3]); 11] = [
    (
        "sum_over_time",
        [0x3ff999999999999a, 0x4020000000000000, 0x4023333333333333],
    ),
    (
        "avg_over_time",
        [0x3fe0666666666667, 0x4000000000000000, 0x3ff0222222222222],
    ),
    (
        "stdvar_over_time",
        [0x4673b8b5b5056e16, 0x4023000000000000, 0x466a4b9cf15c92c8],
    ),
    (
        "stddev_over_time",
        [0x4331c37937e08000, 0x4008a85c24f70659, 0x432d01fe3eaa494c],
    ),
    (
        "min_over_time",
        [0xc341c37937e08000, 0xc008000000000000, 0xc341c37937e08000],
    ),
    (
        "max_over_time",
        [0x4341c37937e08000, 0x4014000000000000, 0x4341c37937e08000],
    ),
    (
        "first_over_time",
        [0x3fc999999999999a, 0x4014000000000000, 0x3fc999999999999a],
    ),
    (
        "last_over_time",
        [0x3feccccccccccccd, 0x4010000000000000, 0x4010000000000000],
    ),
    (
        "quantile_over_time",
        [0x432550f7dca6fffd, 0x4012cccccccccccd, 0x401399999999999a],
    ),
    (
        "rate",
        [0x3f9b4e81b4e81b4f, 0x3fc1111111111111, 0x3fb47ae147ae147b],
    ),
    (
        "rate_counter",
        [0x42e2f2a36ecd5556, 0x3fb6c16c16c16c16, 0x42d3cf1f39a81f0c],
    ),
];

/// **T5 (issue #624, part 4): the eleven reducers answer the reference's
/// values**, from its Go code fed each window in `(timestamp, stream hash,
/// value)` order, on both routes. Each tie's larger value has the body that
/// sorts first, so body order and value order disagree at every tie.
#[tokio::test]
async fn unwrap_reducers_answer_the_references_values() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4t5_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    land_stream_624(
        &admin,
        &db,
        t,
        6_240_600,
        "c624p4ref",
        r#"{"service_name":"c624p4ref"}"#,
    )
    .await;
    for (at, body, v) in [
        (10, "a", "0.7"),
        (10, "b", "0.2"),
        (20, "b", "0.1"),
        (30, "b", "1e16"),
        (40, "b", "-1e16"),
        (50, "b", "0.3"),
        (60, "a", "0.9"),
        (60, "b", "0.4"),
        (70, "b", "5"),
        (80, "a", "2"),
        (80, "b", "-3"),
        (90, "b", "4"),
    ] {
        land_line_624(
            &admin,
            &db,
            6_240_600,
            "c624p4ref",
            t + at * sec,
            body,
            &format!(r#"{{"v":"{v}"}}"#),
        )
        .await;
    }
    let params = grid_624p4(t + 60 * sec, t + 120 * sec);
    let mut wrong: Vec<String> = Vec::new();
    for (op, [w1, w2, instant]) in T5_EXPECTED_624P4 {
        let param = if op == "quantile_over_time" {
            "0.9, "
        } else {
            ""
        };
        for todays in [false, true] {
            let query = format!(r#"{op}({param}{{service_name="c624p4ref"}} | unwrap v [60s])"#);
            let got = run_624(&db, hooks_624(todays, None, None), None, &query, &params)
                .await
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            let points: Vec<(i64, u64)> = got.iter().flat_map(|(_, p)| p.clone()).collect();
            let want = vec![(t + 60 * sec, w1), (t + 120 * sec, w2)];
            if got.len() != 1 || points != want {
                wrong.push(format!(
                    "{query} (today's {todays}): {points:x?}, want {want:x?}"
                ));
            }
            let query = format!(r#"{op}({param}{{service_name="c624p4ref"}} | unwrap v [120s])"#);
            let got = run_instant_624p4(
                &db,
                hooks_624(todays, None, None),
                None,
                &query,
                t + 120 * sec,
            )
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
            let values: Vec<u64> = got.iter().map(|(_, v)| *v).collect();
            if values != vec![instant] {
                wrong.push(format!(
                    "{query} instant (today's {todays}): {values:x?}, want {instant:x}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    drop_db_624(&admin, &db).await;
}

/// **T6 (issue #624, part 4, F3): under a parent `sum`, `sum_over_time` and
/// `rate` reduce the merged series**, as the reference does, and `rate` is
/// divided once.
#[tokio::test]
async fn a_parent_sum_reduces_the_merged_series() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4t6_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    for (fp, pod) in [(6_240_610u64, "a"), (6_240_611, "b")] {
        land_stream_624(
            &admin,
            &db,
            t,
            fp,
            "c624p4inj",
            &format!(r#"{{"pod":"{pod}","service_name":"c624p4inj"}}"#),
        )
        .await;
    }
    for (fp, at, v) in [
        (6_240_610u64, 10, "0.1"),
        (6_240_610, 30, "1.1"),
        (6_240_611, 20, "0.1"),
    ] {
        land_line_624(
            &admin,
            &db,
            fp,
            "c624p4inj",
            t + at * sec,
            "x",
            &format!(r#"{{"route":"r","v":"{v}"}}"#),
        )
        .await;
    }
    let params = grid_624p4(t + 60 * sec, t + 60 * sec);
    for (op, bits) in [
        ("sum_over_time", 0x3ff4cccccccccccdu64),
        ("rate", 0x3f962fc962fc9630),
    ] {
        let query =
            format!(r#"sum by (route) ({op}({{service_name="c624p4inj"}} | unwrap v [60s]))"#);
        for todays in [false, true] {
            let got = run_624(&db, hooks_624(todays, None, None), None, &query, &params)
                .await
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            assert_eq!(
                got,
                vec![(
                    vec![("route".to_string(), "r".to_string())],
                    vec![(t + 60 * sec, bits)]
                )],
                "{query} (today's {todays})"
            );
        }
    }
    drop_db_624(&admin, &db).await;
}

/// **T7 (issue #624, part 4): a value that fails to convert, and one the
/// statement does not decide.**
#[tokio::test]
async fn failed_and_undecided_values() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4t7_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    // (service, fingerprint, the odd line's second and metadata)
    for (svc, fp, at, sm) in [
        (
            "c624p4fa",
            6_240_620u64,
            40,
            r#"{"lat":"abc","size":"1KB"}"#,
        ),
        ("c624p4fc", 6_240_621, 40, r#"{"lat":"5s","size":"1,024"}"#),
        ("c624p4fd", 6_240_622, 15, r#"{"lat":"abc","size":"1KB"}"#),
    ] {
        land_stream_624(
            &admin,
            &db,
            t,
            fp,
            svc,
            &format!(r#"{{"service_name":"{svc}"}}"#),
        )
        .await;
        for s in [50, 110, 170] {
            land_line_624(
                &admin,
                &db,
                fp,
                svc,
                t + s * sec,
                "x",
                r#"{"lat":"2s","size":"3KB"}"#,
            )
            .await;
        }
        land_line_624(&admin, &db, fp, svc, t + at * sec, "y", sm).await;
    }
    let params = grid_624p4(t + 60 * sec, t + 180 * sec);
    // (query, the reduced statement's code, today's route reads, the answer
    // is an error)
    let cases: [(&str, i32, bool, bool); 4] = [
        (
            r#"max_over_time({service_name="c624p4fa"} | unwrap duration(lat) [30s])"#,
            0,
            true,
            true,
        ),
        (
            r#"max_over_time({service_name="c624p4fa"} | unwrap duration(lat) | __error__="" [30s])"#,
            0,
            false,
            false,
        ),
        (
            r#"max_over_time({service_name="c624p4fc"} | unwrap bytes(size) [30s])"#,
            395,
            true,
            false,
        ),
        (
            r#"max_over_time({service_name="c624p4fd"} | unwrap duration(lat) [30s])"#,
            0,
            false,
            false,
        ),
    ];
    let mut comments = Vec::new();
    for (query, _, _, error) in cases {
        let comment = format!("c624p4t7-{}", uuid::Uuid::new_v4().simple());
        let lowered = run_624(
            &db,
            hooks_624(false, None, None),
            Some(&comment),
            query,
            &params,
        )
        .await;
        let todays = run_624(&db, hooks_624(true, None, None), None, query, &params).await;
        same_outcome_624(query, &lowered, &todays);
        assert_eq!(lowered.is_err(), error, "{query}: {lowered:?}");
        comments.push(comment);
    }
    let logged = logged_624(&admin, &comments).await;
    for ((query, code, todays, _), comment) in cases.iter().zip(&comments) {
        let rows = logged.get(comment).cloned().unwrap_or_default();
        let reduced: Vec<&CommentedRow624> =
            rows.iter().filter(|r| is_reduced_624p4(&r.query)).collect();
        assert_eq!(reduced.len(), 1, "{query}: {rows:#?}");
        assert_eq!(reduced[0].exception_code, *code, "{query}: {rows:#?}");
        let reduced_at = rows.iter().position(|r| is_reduced_624p4(&r.query));
        let todays_at = rows.iter().position(|r| is_todays_sliding_624p4(&r.query));
        assert_eq!(
            todays_at.is_some(),
            *todays,
            "{query}: today's read: {rows:#?}"
        );
        assert!(
            todays_at.is_none_or(|b| reduced_at.is_some_and(|a| a < b)),
            "{query}: the reduced statement, then today's read: {rows:#?}"
        );
    }
    drop_db_624(&admin, &db).await;
}

/// **T2's live half (issue #624, part 4, code review round 1): a compound
/// duration whose sum passes the range fails its conversion** on today's
/// route, as any out-of-range duration does: no sample, and the line carries
/// `__error__`, which fails the query unless a filter drops it. The reduced
/// statement leaves the text undecided (each component has 19 digits), so
/// the query reaches today's route either way.
#[tokio::test]
async fn an_overflowing_compound_duration_fails_its_conversion() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4ovf_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    for (fp, svc, lat) in [
        (
            6_240_670u64,
            "c624p4ovf",
            "9223372036854775808ns9223372036854775808ns",
        ),
        (
            6_240_671,
            "c624p4ovfn",
            "-9223372036854775808ns9223372036854775808ns",
        ),
    ] {
        land_stream_624(
            &admin,
            &db,
            t,
            fp,
            svc,
            &format!(r#"{{"service_name":"{svc}"}}"#),
        )
        .await;
        land_line_624(&admin, &db, fp, svc, t + 20 * sec, "x", r#"{"lat":"2s"}"#).await;
        land_line_624(
            &admin,
            &db,
            fp,
            svc,
            t + 30 * sec,
            "y",
            &format!(r#"{{"lat":"{lat}"}}"#),
        )
        .await;
        let params = grid_624p4(t + 60 * sec, t + 60 * sec);
        for todays in [false, true] {
            let query =
                format!(r#"max_over_time({{service_name="{svc}"}} | unwrap duration(lat) [60s])"#);
            match run_624(&db, hooks_624(todays, None, None), None, &query, &params).await {
                Err(ReadError::MetricPipelineError { error_type, .. }) => assert_eq!(
                    error_type,
                    pulsus_read::logql::SAMPLE_EXTRACTION_ERROR,
                    "{query} (today's {todays})"
                ),
                other => panic!("{query} (today's {todays}): a conversion failure, got {other:?}"),
            }
            let query = format!(
                r#"max_over_time({{service_name="{svc}"}} | unwrap duration(lat) | __error__="" [60s])"#
            );
            let got = run_624(&db, hooks_624(todays, None, None), None, &query, &params)
                .await
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            let points: Vec<(i64, u64)> = got.iter().flat_map(|(_, p)| p.clone()).collect();
            assert_eq!(
                points,
                vec![(t + 60 * sec, 2f64.to_bits())],
                "{query} (today's {todays}): the overflowing line is no sample"
            );
        }
    }
    drop_db_624(&admin, &db).await;
}

/// **T14 (issue #624, part 4): `without (zone)` keeps the metadata `zone` a
/// stream label shadows, as `zone_extracted`**, and the reduced statement
/// answers it alone.
#[tokio::test]
async fn without_keeps_the_metadata_name_a_stream_label_shadows() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4t14_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    land_stream_624(
        &admin,
        &db,
        t,
        6_240_630,
        "c624p4zone",
        r#"{"service_name":"c624p4zone","zone":"a"}"#,
    )
    .await;
    land_stream_624(
        &admin,
        &db,
        t,
        6_240_631,
        "c624p4zone",
        r#"{"service_name":"c624p4zone"}"#,
    )
    .await;
    land_line_624(
        &admin,
        &db,
        6_240_630,
        "c624p4zone",
        t + 20 * sec,
        "x",
        r#"{"v":"3","zone":"m"}"#,
    )
    .await;
    land_line_624(
        &admin,
        &db,
        6_240_631,
        "c624p4zone",
        t + 30 * sec,
        "x",
        r#"{"v":"4","zone":"n"}"#,
    )
    .await;
    let params = grid_624p4(t + 60 * sec, t + 60 * sec);
    let query = r#"max_over_time({service_name="c624p4zone"} | unwrap v [60s]) without (zone)"#;
    let comment = format!("c624p4t14-{}", uuid::Uuid::new_v4().simple());
    let got = run_624(
        &db,
        hooks_624(false, None, None),
        Some(&comment),
        query,
        &params,
    )
    .await
    .expect("answers");
    let want = run_624(&db, hooks_624(true, None, None), None, query, &params)
        .await
        .expect("answers");
    assert_eq!(got, want);
    let svc = ("service_name".to_string(), "c624p4zone".to_string());
    assert_eq!(
        got,
        vec![
            (vec![svc.clone()], vec![(t + 60 * sec, 4f64.to_bits())]),
            (
                vec![svc, ("zone_extracted".to_string(), "m".to_string())],
                vec![(t + 60 * sec, 3f64.to_bits())]
            ),
        ]
    );
    let logged = logged_624(&admin, std::slice::from_ref(&comment)).await;
    let rows = logged.get(&comment).cloned().unwrap_or_default();
    assert!(
        rows.iter()
            .any(|r| is_reduced_624p4(&r.query) && r.exception_code == 0),
        "{rows:#?}"
    );
    assert!(
        !rows.iter().any(|r| is_todays_sliding_624p4(&r.query)),
        "the answer is the statement's own: {rows:#?}"
    );
    drop_db_624(&admin, &db).await;
}

/// The eleven reducers, each as `<op>(<param>`.
const REDUCERS_624P4: [&str; 11] = [
    "sum_over_time(",
    "avg_over_time(",
    "min_over_time(",
    "max_over_time(",
    "stddev_over_time(",
    "stdvar_over_time(",
    "quantile_over_time(0.5, ",
    "first_over_time(",
    "last_over_time(",
    "rate(",
    "rate_counter(",
];

/// **T15 (issue #624, part 4): a window with no sample is no point, for
/// every reducer**, range and instant, on both routes.
#[tokio::test]
async fn an_empty_window_emits_no_point_for_any_reducer() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4t15_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    land_stream_624(
        &admin,
        &db,
        t,
        6_240_640,
        "c624p4gap",
        r#"{"service_name":"c624p4gap"}"#,
    )
    .await;
    for at in [10, 130] {
        land_line_624(
            &admin,
            &db,
            6_240_640,
            "c624p4gap",
            t + at * sec,
            "x",
            r#"{"v":"2"}"#,
        )
        .await;
    }
    let params = grid_624p4(t + 60 * sec, t + 180 * sec);
    let mut comments: Vec<String> = Vec::new();
    for op in REDUCERS_624P4 {
        let query = format!(r#"{op}{{service_name="c624p4gap"}} | unwrap v [60s])"#);
        for todays in [false, true] {
            let comment = (!todays).then(|| format!("c624p4t15-{}", uuid::Uuid::new_v4().simple()));
            let got = run_624(
                &db,
                hooks_624(todays, None, None),
                comment.as_deref(),
                &query,
                &params,
            )
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
            comments.extend(comment);
            let at: Vec<i64> = got
                .iter()
                .flat_map(|(_, p)| p.iter().map(|(ts, _)| *ts))
                .collect();
            assert_eq!(
                at,
                vec![t + 60 * sec, t + 180 * sec],
                "{query} (today's {todays})"
            );
            let comment = (!todays).then(|| format!("c624p4t15-{}", uuid::Uuid::new_v4().simple()));
            let got = run_instant_624p4(
                &db,
                hooks_624(todays, None, None),
                comment.as_deref(),
                &query,
                t + 120 * sec,
            )
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
            assert!(
                got.is_empty(),
                "{query} instant (today's {todays}): {got:?}"
            );
            comments.extend(comment);
        }
    }
    let logged = logged_624(&admin, &comments).await;
    for comment in &comments {
        let rows = logged.get(comment).cloned().unwrap_or_default();
        assert!(
            rows.iter()
                .any(|r| is_reduced_624p4(&r.query) && r.exception_code == 0)
                && !rows.iter().any(|r| is_todays_624(&r.query)),
            "{comment}: the reduced statement answers: {rows:#?}"
        );
    }
    drop_db_624(&admin, &db).await;
}

/// **T16 (issue #624, part 4, F4): `rate_counter` over one sample is 0, a
/// point, and over two it is extrapolated in seconds to the window's
/// edges.**
#[tokio::test]
async fn a_single_sample_counter_window_is_zero() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4t16_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    for (fp, svc, lines) in [
        (6_240_650u64, "c624p4one", vec![(10, "42")]),
        (6_240_651, "c624p4two", vec![(25, "200"), (35, "260")]),
    ] {
        land_stream_624(
            &admin,
            &db,
            t,
            fp,
            svc,
            &format!(r#"{{"service_name":"{svc}"}}"#),
        )
        .await;
        for (at, c) in lines {
            land_line_624(
                &admin,
                &db,
                fp,
                svc,
                t + at * sec,
                "x",
                &format!(r#"{{"c":"{c}"}}"#),
            )
            .await;
        }
    }
    let params = grid_624p4(t + 60 * sec, t + 60 * sec);
    for (svc, want) in [("c624p4one", 0.0f64), ("c624p4two", 2.0)] {
        let query = format!(r#"rate_counter({{service_name="{svc}"}} | unwrap c [60s])"#);
        for todays in [false, true] {
            let got = run_624(&db, hooks_624(todays, None, None), None, &query, &params)
                .await
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            let points: Vec<(i64, u64)> = got.iter().flat_map(|(_, p)| p.clone()).collect();
            assert_eq!(
                points,
                vec![(t + 60 * sec, want.to_bits())],
                "{query} (today's {todays})"
            );
        }
    }
    drop_db_624(&admin, &db).await;
}

/// **T13 (issue #624, part 4): a reduced query answers wherever today's
/// route answers.** Part 2's T5 harness with metadata `v` on every line: at
/// today's route's smallest answering cap, the reduced route answers with
/// today's matrix.
#[tokio::test]
async fn a_reduced_query_answers_wherever_todays_route_answers() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4t13_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 4 * 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    seed_cap_streams_624(&admin, &db, t, 200).await;
    exec_624(
        &admin,
        &format!(
            "ALTER TABLE {db}.log_samples UPDATE structured_metadata = \
             concat('{{\"v\":\"', toString(timestamp_ns % 7), '\"}}') WHERE 1 \
             SETTINGS mutations_sync = 2"
        ),
    )
    .await;
    let params = grid_624p4(t, t + 600_000_000_000);
    let sel = r#"{service_name="c624cap"}"#;
    for range in ["60s", "150s"] {
        for query in [
            format!("sum(sum_over_time({sel} | unwrap v [{range}]))"),
            format!("max_over_time({sel} | unwrap v [{range}]) by (pod)"),
        ] {
            for (cap, hi) in [
                (Cap624::ResultPoints, 100_000u64),
                (Cap624::GroupBytes, 256 * 1024 * 1024),
            ] {
                let today = min_cap_624(&db, true, cap, &query, &params, hi).await;
                let want = run_624(
                    &db,
                    hooks_624(true, Some((cap, today)), None),
                    None,
                    &query,
                    &params,
                )
                .await
                .expect("today's route answers at its smallest cap");
                assert!(!want.is_empty(), "{query}");
                let comment = format!("c624p4t13-{}", uuid::Uuid::new_v4().simple());
                let got = run_624(
                    &db,
                    hooks_624(false, Some((cap, today)), None),
                    Some(&comment),
                    &query,
                    &params,
                )
                .await;
                assert_eq!(
                    got.as_ref().ok(),
                    Some(&want),
                    "{query} at {cap:?} {today}: {got:?}"
                );
                let logged = logged_624(&admin, std::slice::from_ref(&comment)).await;
                let rows = logged.get(&comment).cloned().unwrap_or_default();
                assert!(
                    rows.iter().any(|r| is_reduced_624p4(&r.query))
                        && !rows.iter().any(|r| is_todays_sliding_624p4(&r.query)),
                    "{query} at {cap:?} {today}: the reduced route answers: {rows:#?}"
                );
            }
        }
    }
    drop_db_624(&admin, &db).await;
}

/// The instants T12 asks at over a corpus: a line's timestamp, a nanosecond
/// past it, and ten minutes after the last line.
async fn instants_624p4(admin: &ChClient, db: &str, service: &str) -> [i64; 3] {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct Span {
        lo: i64,
        hi: i64,
    }
    let sql = format!(
        "SELECT min(timestamp_ns) AS lo, max(timestamp_ns) AS hi FROM {db}.log_samples \
         WHERE service = '{service}'"
    );
    let mut stream = admin
        .query_stream::<Span>(&sql, &QuerySettings::new())
        .await
        .expect("read the corpus span");
    let span = stream.next().await.expect("one row").expect("decode");
    drop(stream);
    assert!(span.lo > 0, "{service}: the corpus has lines");
    [span.lo, span.lo + 1, span.hi + 600_000_000_000]
}

/// **T12 (issue #624, part 5): an instant query with a pipeline is answered
/// in the database**, for each family that lowers — label stages, `regexp`,
/// `| json`, `| pattern`, `| logfmt`, and the `| unwrap` reducers — at a
/// line's timestamp, between lines and after the data, at `[60s]` and
/// `[5m]`. The vector equals today's route's; the query log holds the
/// lowered statement for one run and today's instant read for the other.
#[tokio::test]
async fn instant_queries_with_a_pipeline_are_answered_in_the_database() {
    skip_unless_live!();
    // (admin, db, service, queries with `<R>` for the range)
    let mut corpora: Vec<(ChClient, String, &str, Vec<String>)> = Vec::new();
    let (admin, db, _) = seed_staged_corpus().await;
    corpora.push((
        admin,
        db,
        "c507bucket",
        vec![
            r#"sum by (lvl) (count_over_time({service_name="c507bucket"} | drop x [<R>]))"#
                .to_string(),
        ],
    ));
    let (admin, db, _) = seed_regexp_corpus_624p3().await;
    corpora.push((
        admin,
        db,
        "c507bucket",
        vec![regexp_lowered_queries_624p3(r#"{service_name="c507bucket"}"#, "<R>")[0].clone()],
    ));
    let (admin, db, _) = seed_json_corpus_624p3b().await;
    corpora.push((
        admin,
        db,
        "p3bj",
        vec![json_lowered_queries_624p3b(r#"{service_name="p3bj"}"#, "<R>")[0].clone()],
    ));
    let (admin, db, _) = seed_pattern_corpus_624p3c().await;
    corpora.push((
        admin,
        db,
        "c624p3c",
        vec![pattern_lowered_queries_624p3c(r#"{service_name="c624p3c"}"#, "<R>")[0].clone()],
    ));
    {
        let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
            "pulsus_read_it_qlg_p5lf_{}",
            uuid::Uuid::new_v4().simple()
        )))
        .await;
        let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
        land_stream_624(
            &admin,
            &db,
            t,
            6_240_660,
            "c624p5lf",
            r#"{"service_name":"c624p5lf"}"#,
        )
        .await;
        for (i, (body, decided)) in LINES_624P3D.iter().enumerate() {
            if *decided {
                land_line_624(
                    &admin,
                    &db,
                    6_240_660,
                    "c624p5lf",
                    t + (i as i64 + 1) * 1_000_000_000,
                    body,
                    "",
                )
                .await;
            }
        }
        corpora.push((
            admin,
            db,
            "c624p5lf",
            vec![
                r#"sum by (level) (count_over_time({service_name="c624p5lf"} | logfmt [<R>]))"#
                    .to_string(),
            ],
        ));
    }
    let (admin, db, _) = seed_reduced_corpus_624p4().await;
    corpora.push((
        admin,
        db,
        "c624p4",
        vec![
            r#"avg_over_time({service_name="c624p4"} | unwrap dur | __error__="" [<R>]) by (route)"#
                .to_string(),
            r#"quantile_over_time(0.9, {service_name="c624p4"} | unwrap duration(lat) | __error__="" [<R>]) by (route)"#
                .to_string(),
            r#"max by (route) (rate_counter({service_name="c624p4lean"} | unwrap dur | __error__="" [<R>]))"#
                .to_string(),
        ],
    ));
    let mut answered = 0usize;
    for (admin, db, service, queries) in &corpora {
        let instants = instants_624p4(admin, db, service).await;
        let mut comments: Vec<(String, bool)> = Vec::new();
        for template in queries {
            for range in ["60s", "5m"] {
                let query = template.replace("<R>", range);
                for at in instants {
                    let lowered_comment = format!("c624p5t12-{}", uuid::Uuid::new_v4().simple());
                    let todays_comment = format!("c624p5t12-{}", uuid::Uuid::new_v4().simple());
                    let lowered = run_instant_624p4(
                        db,
                        hooks_624(false, None, None),
                        Some(&lowered_comment),
                        &query,
                        at,
                    )
                    .await;
                    let todays = run_instant_624p4(
                        db,
                        hooks_624(true, None, None),
                        Some(&todays_comment),
                        &query,
                        at,
                    )
                    .await;
                    same_instant_outcome_624p4(&format!("{query} at {at}"), &lowered, &todays);
                    if lowered.as_ref().is_ok_and(|a| !a.is_empty()) {
                        answered += 1;
                    }
                    comments.push((lowered_comment, true));
                    comments.push((todays_comment, false));
                }
            }
        }
        let all: Vec<String> = comments.iter().map(|(c, _)| c.clone()).collect();
        let logged = logged_624(admin, &all).await;
        for (comment, lowered) in &comments {
            let rows = logged.get(comment).cloned().unwrap_or_default();
            if *lowered {
                assert!(
                    rows.iter()
                        .any(|r| is_lowered_624(&r.query) && r.exception_code == 0),
                    "{comment}: the lowered statement: {rows:#?}"
                );
                assert!(
                    !rows.iter().any(|r| is_todays_624(&r.query)),
                    "{comment}: no read of today's route: {rows:#?}"
                );
            } else {
                assert!(
                    rows.iter().any(|r| is_todays_instant_624p4(&r.query)),
                    "{comment}: today's instant read: {rows:#?}"
                );
                assert!(
                    !rows.iter().any(|r| is_lowered_624(&r.query)),
                    "{comment}: {rows:#?}"
                );
            }
        }
    }
    assert!(answered >= 12, "the instants answer: {answered}");
    for (admin, db, _, _) in &corpora {
        drop_db_624(admin, db).await;
    }
}

/// A fixed sequence of texts for T3: each of the eleven templates, filled
/// from one linear congruential generator.
fn conversion_texts_624p4(count: usize) -> Vec<String> {
    const DURATION_UNITS: [&str; 8] = ["ns", "us", "\u{b5}s", "\u{3bc}s", "ms", "s", "m", "h"];
    const BYTES_UNITS: [&str; 25] = [
        "b", "k", "kb", "ki", "kib", "m", "mb", "mi", "mib", "g", "gb", "gi", "gib", "t", "tb",
        "ti", "tib", "p", "pb", "pi", "pib", "e", "eb", "ei", "eib",
    ];
    let mut state: u64 = 0x0624_0004;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state >> 33
    };
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let i = next() % 100_000;
        let f = next() % 1_000;
        let x = next() % 40;
        let u = DURATION_UNITS[(next() % 8) as usize];
        let mut b = BYTES_UNITS[(next() % 25) as usize].to_string();
        if next() % 2 == 0 {
            b = b.to_uppercase();
        }
        let text = match next() % 11 {
            0 => format!("{i}"),
            1 => format!("{i}.{f}"),
            2 => format!("-{i}.{f}"),
            3 => format!("{i}e{}", x as i64 - 20),
            4 => format!(".{f}"),
            5 => format!("{i}{u}"),
            6 => format!("{i}.{f}{u}"),
            7 => format!("{}h{}m{}.{}s", i % 100, f % 60, x, next() % 1000),
            8 => format!("{i}{b}"),
            9 => format!("{i} {b}"),
            _ => format!("{i}.{f}{b}"),
        };
        out.push(text);
    }
    out
}

/// **T3 (issue #624, part 4): every value and failure the statement's
/// conversion decides is today's route's.** 20,000 generated texts, T2's,
/// and a set of edge cases, through `reduced_conversion_columns` over a
/// `VALUES` table, per conversion.
#[tokio::test]
async fn the_database_conversions_are_todays_routes_where_they_decide() {
    skip_unless_live!();
    use pulsus_read::logql::pipeline::unwrap_conversion_probe;
    use pulsus_read::logql::sql::{UnwrapConversion, reduced_conversion_columns};
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct Decided {
        i: u32,
        ut: String,
        us: u8,
        uv: f64,
    }
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    let generated = conversion_texts_624p4(20_000);
    let mut texts = generated.clone();
    for extra in [
        "9ms",
        "13ms",
        "300ms",
        "1.5s",
        "1.s",
        ".5s",
        "-1.5h",
        "1m.5s",
        "1\u{b5}s",
        "1\u{3bc}s",
        "0",
        "-0",
        "9223372036s",
        "2562047h",
        "1.0000000000000000001s",
        "00",
        "1d",
        "1w",
        "1hm",
        "1msx",
        ".s",
        "abc",
        "9223372037s",
        "2562048h",
        "n/a",
        "-",
        "1.e5",
        "1,024",
        "1_000",
        "0x1p-2",
        "inf",
        "1e400",
    ] {
        texts.push(extra.to_string());
    }
    let settings = QuerySettings::new().set("max_query_size", 4_194_304u64);
    for (conversion, name) in [
        (UnwrapConversion::Number, "number"),
        (UnwrapConversion::Duration, "duration"),
        (UnwrapConversion::Bytes, "bytes"),
    ] {
        let mut wrong: Vec<String> = Vec::new();
        let mut decided_generated = 0usize;
        let columns = reduced_conversion_columns(conversion);
        let numbered: Vec<(usize, &String)> = texts.iter().enumerate().collect();
        for chunk in numbered.chunks(2_000) {
            let list = chunk
                .iter()
                .map(|(i, t)| format!("({i}, {})", literal(t).as_sql()))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT i, ut, us, uv FROM (SELECT i, ut, {columns} \
                 FROM values('i UInt32, ut String', {list})) \
                 SETTINGS precise_float_parsing = 1"
            );
            // The client reads `?` as a bound argument; the engine doubles
            // a literal one (`escape_query_placeholders`), and so does this.
            let sql = sql.replace('?', "??");
            let mut stream = admin
                .query_stream::<Decided>(&sql, &settings)
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            while let Some(row) = stream.next().await {
                let row = row.unwrap_or_else(|e| panic!("{name}: {e}"));
                let ours = unwrap_conversion_probe(conversion, &row.ut);
                match row.us {
                    0 => {
                        if ours.map(f64::to_bits) != Some(row.uv.to_bits()) {
                            wrong.push(format!(
                                "{:?}: statement {:?}, ours {ours:?}",
                                row.ut, row.uv
                            ));
                        }
                    }
                    1 => {
                        if ours.is_some() {
                            wrong.push(format!("{:?}: statement fails, ours {ours:?}", row.ut));
                        }
                    }
                    _ => {}
                }
                if row.us <= 1 && (row.i as usize) < generated.len() {
                    decided_generated += 1;
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "{name}: {} texts differ:\n{}",
            wrong.len(),
            wrong.join("\n")
        );
        eprintln!(
            "{name}: the statement decides {decided_generated} of the 20,000 generated texts"
        );
        assert!(
            decided_generated >= 14_000,
            "{name}: the statement decides {decided_generated} of the 20,000 generated texts"
        );
    }
}

// ---------------------------------------------------------------------
// Issue #624, part 4b: the `| unwrap` reducers after a parser, in the
// database.
// ---------------------------------------------------------------------

/// The four fixture files of part 4b, `tests/fixtures/unwrap_parsers/`.
fn unwrap_parsers_fixture_624p4b(name: &str) -> String {
    std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/unwrap_parsers")
            .join(name),
    )
    .unwrap_or_else(|e| panic!("read {name}: {e}"))
}

/// The rows of a fixture file, split on tabs, comments left out.
fn fixture_rows_624p4b(text: &str) -> Vec<Vec<String>> {
    text.lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .map(|l| l.split('\t').map(str::to_string).collect())
        .collect()
}

/// A `lines.tsv` body as the reference's extractor read it: `\t` is a
/// tab, then `\\` a backslash, every other byte itself.
fn reference_line_624p4b(field: &str) -> String {
    field.replace("\\t", "\t").replace("\\\\", "\\")
}

/// The reduced plan's payload for one of `queries.tsv`'s queries, as §2 of
/// part 4b's plan admits it: the parser and the key each shape takes.
fn fixture_value_624p4b(query: &str) -> pulsus_read::logql::sql::ReducedUnwrap {
    use pulsus_logql::RangeAggOp;
    use pulsus_read::logql::sql::{
        CaptureParser, KeyMode, ReducedParser, ReducedUnwrap, UnwrapConversion, UnwrapForm,
        UnwrapKeyLabel,
    };
    let body_start = query.find("{app=\"a\"}").expect("a selector");
    let body_end = query.find(" [1m]").expect("a range");
    let log = &query[body_start..body_end];
    let pulsus_logql::Expr::Log(le) = parse(log).expect("parse the pipeline") else {
        panic!("{log}: a log expression");
    };
    let pipeline = le.pipeline;
    let label = if log.contains("unwrap y") || log.contains("(y)") {
        "y"
    } else {
        "x"
    };
    let conversion = if log.contains("duration(") {
        UnwrapConversion::Duration
    } else if log.contains("bytes(") {
        UnwrapConversion::Bytes
    } else {
        UnwrapConversion::Number
    };
    let route = || {
        vec![UnwrapKeyLabel {
            label: "route".to_string(),
            source: "route".to_string(),
        }]
    };
    let parser = if log.contains("| json x=\"a.b\"") {
        ReducedParser::Json {
            form: UnwrapForm::Targeted,
            keys: route(),
            x_path: vec!["a".to_string(), "b".to_string()],
        }
    } else if log.contains("| json x=") {
        ReducedParser::Json {
            form: UnwrapForm::Targeted,
            keys: route(),
            x_path: vec!["x".to_string()],
        }
    } else if log.contains("| json") {
        ReducedParser::Json {
            form: UnwrapForm::Bare,
            keys: route(),
            x_path: vec!["x".to_string()],
        }
    } else if log.contains("| logfmt y=") {
        ReducedParser::Logfmt {
            names: vec!["route".to_string()],
            x_source: "x".to_string(),
            x_destination: Some("y".to_string()),
        }
    } else if log.contains("| logfmt") {
        ReducedParser::Logfmt {
            names: vec!["route".to_string()],
            x_source: "x".to_string(),
            x_destination: None,
        }
    } else if log.contains("| regexp") {
        ReducedParser::Capture {
            parser: CaptureParser::Regexp,
            pattern: r"x=(?P<x>\S*) route=(?P<route>\S+)".to_string(),
            groups: vec![("route".to_string(), 2)],
            x_index: 1,
        }
    } else {
        ReducedParser::Capture {
            parser: CaptureParser::Pattern,
            pattern: "<_> x=<x> route=<route>".to_string(),
            groups: vec![("route".to_string(), 2)],
            x_index: 1,
        }
    };
    let keep = query.starts_with("sum by (route)");
    let (reducer, mode, grouping) = if keep {
        (
            RangeAggOp::SumOverTime,
            KeyMode::Keep(vec!["route".to_string()]),
            Some(Box::new(pulsus_read::logql::RangeGrouping::By(vec![
                "route".to_string(),
            ]))),
        )
    } else {
        (
            RangeAggOp::MaxOverTime,
            KeyMode::Drop(vec![label.to_string()]),
            None,
        )
    };
    ReducedUnwrap {
        reducer,
        quantile: None,
        label: label.to_string(),
        conversion,
        mode,
        key_stages: pipeline.clone(),
        todays_route: pulsus_read::logql::ClientAgg {
            pipeline,
            value: pulsus_read::logql::ClientValue::Unwrap,
            range_op: reducer,
            param: None,
            absent_labels: Vec::new(),
            grouping,
        },
        instant: false,
        parser: Some(parser),
    }
}

/// A reference route as `outcomes.tsv` writes it, unescaped; `-` and the
/// empty text are no route.
fn reference_route_624p4b(text: &str) -> Option<String> {
    if text.is_empty() || text == "-" {
        return None;
    }
    let mut out = String::new();
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(n) = chars.next() {
                out.push(n);
            }
        } else {
            out.push(c);
        }
    }
    Some(out)
}

/// **T3 (issue #624, part 4b): the value readers are the reference's
/// extraction.** `lines.tsv` is loaded into a table byte for byte as the
/// reference read it; for each of the 21 queries of `queries.tsv`,
/// `reduced_parser_columns` is rendered over it and `st`, `pu`, `ut`, `v`
/// and the route key are read:
///
/// ```text
/// undecided (st = 2 or pu = 1)   the set of (query, line) pairs equals undecided.tsv
/// every other pair               none ↔ ut = ''; fail ↔ st = 1; err:… never decided;
///                                a value ↔ st = 0, the same bits and the same route
/// ```
///
/// A reference route `-` or empty is no `route` in `pk`, or an empty one;
/// a `| logfmt` route is read from `pk`'s fields by our own parser.
#[tokio::test]
async fn the_value_readers_are_the_references_extraction() {
    skip_unless_live!();
    use pulsus_read::logql::sql::{ReducedKeyColumns, ReducedParser, reduced_parser_columns};
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct Decided {
        id: u32,
        st: u8,
        pu: u8,
        ut: String,
        v: f64,
        pk: Vec<(String, String)>,
    }
    let lines = fixture_rows_624p4b(&unwrap_parsers_fixture_624p4b("lines.tsv"));
    let queries = fixture_rows_624p4b(&unwrap_parsers_fixture_624p4b("queries.tsv"));
    let outcomes = fixture_rows_624p4b(&unwrap_parsers_fixture_624p4b("outcomes.tsv"));
    let undecided: std::collections::BTreeSet<(u32, u32)> =
        fixture_rows_624p4b(&unwrap_parsers_fixture_624p4b("undecided.tsv"))
            .iter()
            .map(|f| {
                (
                    f[0].parse().expect("query id"),
                    f[1].parse().expect("line id"),
                )
            })
            .collect();
    assert_eq!(lines.len(), 2_900, "the fixture's lines");
    assert_eq!(queries.len(), 21, "the fixture's queries");
    assert_eq!(outcomes.len(), 15_600, "the fixture's outcomes");
    assert_eq!(undecided.len(), 4_603, "the fixture's undecided pairs");

    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4b_t3_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    exec_624(
        &admin,
        &format!("CREATE TABLE {db}.lines (id UInt32, body String) ENGINE = MergeTree ORDER BY id"),
    )
    .await;
    // Each body is loaded byte for byte as the reference's extractor read
    // it: `\t` is a tab and `\\` a backslash; any other byte is itself, a
    // bare `\"` included. A tab-separated reader would turn that `\"` into
    // `"` and make 37 nested rows invalid JSON.
    for chunk in lines.chunks(500) {
        let values = chunk
            .iter()
            .map(|f| {
                format!(
                    "({}, {})",
                    f[0],
                    literal(&reference_line_624p4b(&f[2])).as_sql()
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        exec_624(
            &admin,
            &format!("INSERT INTO {db}.lines (id, body) VALUES {values}").replace('?', "??"),
        )
        .await;
    }
    assert_eq!(
        count_624p4(&admin, &format!("SELECT count() AS n FROM {db}.lines")).await,
        2_900,
        "the lines are loaded"
    );

    let logfmt =
        pulsus_read::logql::CompiledPipeline::compile(&match parse(r#"{app="a"} | logfmt"#)
            .expect("parse")
        {
            pulsus_logql::Expr::Log(le) => le.pipeline,
            _ => panic!("a log expression"),
        })
        .expect("compile | logfmt");
    let mut ours: std::collections::HashMap<(u32, u32), Decided> = std::collections::HashMap::new();
    for q in &queries {
        let qid: u32 = q[0].parse().expect("query id");
        let value = fixture_value_624p4b(&q[2]);
        let columns =
            reduced_parser_columns(&value, &ReducedKeyColumns::default(), value.conversion)
                .unwrap_or_else(|e| panic!("{}: {e:?}", q[2]));
        let sql = format!(
            "SELECT id, st, pu, ut, v, pk FROM (WITH {columns} SELECT id, st, pu, ut, v, pk \
             FROM (SELECT id, body, '' AS structured_metadata, toUInt128(0) AS fingerprint \
             FROM {db}.lines)) ORDER BY id \
             SETTINGS precise_float_parsing = 1, max_ast_elements = 1000000, \
             max_expanded_ast_elements = 10000000"
        )
        .replace('?', "??");
        let mut stream = admin
            .query_stream::<Decided>(&sql, &QuerySettings::new())
            .await
            .unwrap_or_else(|e| panic!("{}: {e}\n{sql}", q[2]));
        while let Some(row) = stream.next().await {
            let row = row.unwrap_or_else(|e| panic!("{}: {e}", q[2]));
            ours.insert((qid, row.id), row);
        }
        drop(stream);
    }

    let mut left: std::collections::BTreeSet<(u32, u32)> = std::collections::BTreeSet::new();
    let mut wrong: Vec<String> = Vec::new();
    let mut decided: std::collections::BTreeMap<u32, usize> = std::collections::BTreeMap::new();
    for o in &outcomes {
        let key: (u32, u32) = (
            o[0].parse().expect("query id"),
            o[1].parse().expect("line id"),
        );
        let row = ours
            .get(&key)
            .unwrap_or_else(|| panic!("{key:?}: no statement row"));
        if row.st == 2 || row.pu == 1 {
            left.insert(key);
            continue;
        }
        *decided.entry(key.0).or_default() += 1;
        let query = &queries[key.0 as usize][2];
        let ours_outcome = if row.ut.is_empty() {
            "none".to_string()
        } else if row.st == 1 {
            "fail".to_string()
        } else {
            let route = match fixture_value_624p4b(query).parser {
                Some(ReducedParser::Logfmt { .. }) => {
                    let fields = row.pk.first().map(|(_, f)| f.as_str()).unwrap_or("");
                    let out = logfmt.run(fields, &[], 0).expect("run | logfmt");
                    out.and_then(|o| {
                        o.labels
                            .iter()
                            .find(|(k, _)| k.as_ref() == "route")
                            .map(|(_, v)| v.to_string())
                    })
                }
                _ => row
                    .pk
                    .iter()
                    .find(|(k, _)| k == "route")
                    .map(|(_, v)| v.clone()),
            }
            .filter(|r| !r.is_empty());
            format!("{:016x} {route:?}", row.v.to_bits())
        };
        let reference = match o[2].split_once(' ') {
            Some((bits, route)) => format!("{bits} {:?}", reference_route_624p4b(route)),
            None => o[2].clone(),
        };
        if ours_outcome != reference {
            wrong.push(format!(
                "query {} line {}: ours {ours_outcome}, the reference {reference} ({query})",
                key.0, key.1
            ));
        }
    }
    let extra: Vec<&(u32, u32)> = left.difference(&undecided).collect();
    let missing: Vec<&(u32, u32)> = undecided.difference(&left).collect();
    assert!(
        extra.is_empty() && missing.is_empty(),
        "the undecided pairs differ from undecided.tsv: {} more ({:?}…), {} fewer ({:?}…)",
        extra.len(),
        &extra[..extra.len().min(20)],
        missing.len(),
        &missing[..missing.len().min(20)]
    );
    assert!(
        wrong.is_empty(),
        "{} decided pairs differ from the reference:\n{}",
        wrong.len(),
        wrong[..wrong.len().min(40)].join("\n")
    );
    eprintln!("decided per query: {decided:?}");
    assert_eq!(
        decided.values().sum::<usize>(),
        10_997,
        "the decided pairs: {decided:?}"
    );
    drop_db_624(&admin, &db).await;
}

/// T4's corpus `c624p4b`, in a fresh database: `(admin, db, T)`.
///
/// 20 streams `{inst="<s>", pod="p<s % 5>"}` from `T + 1s` to `T + 600s`,
/// one line a second, every stream at the same seconds: streams 0-9
/// (`service_name="c624p4bj"`) write JSON lines, 10-19 (`c624p4bl`) logfmt
/// lines `request_id=… status=… route=… latency=…`. `latency` is an
/// integer (a JSON number), a decimal, a duration, a byte size or an
/// integer text by turns, each one the three conversions decide (a
/// compound duration such as `1m30s` is undecided under `bytes(x)`, part
/// 4's grammar, and would send every `bytes` query to today's route); `route` is one of four, so two streams meet at
/// one nanosecond with one route (a cross-stream tie). Every 37th second a
/// stream has a second line at the same nanosecond with another `latency`
/// and the same `route` (a same-stream tie). The JSON lines also carry
/// `"a":{"b":<n>}`, an integer 1-999 or a decimal with two places, never 0.
async fn seed_parser_corpus_624p4b() -> (ChClient, String, i64) {
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4b_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let month = format!("toStartOfMonth(toDate(fromUnixTimestamp64Nano(toInt64({t}))))");
    exec_624(
        &admin,
        &format!(
            "INSERT INTO {db}.log_landing {LANDING_624P4} \
             SELECT toUnixTimestamp64Milli(now64(3)), 1, if(number < 10, 'c624p4bj', 'c624p4bl'), \
             toUInt128(6240600 + number), 0, 0, '', '', {month}, concat('{{\"inst\":\"', \
             toString(number), '\",\"pod\":\"p', toString(number % 5), '\",\"service_name\":\"', \
             if(number < 10, 'c624p4bj', 'c624p4bl'), '\"}}'), 0, '', 0 FROM numbers(20)"
        ),
    )
    .await;
    // `s` the stream, `i` the second, `k` the value seed.
    for tie in [false, true] {
        let (k, filter) = if tie {
            ("(i * 7 + s * 13 + 5)", "WHERE i % 37 = 0")
        } else {
            ("(i * 7 + s * 13)", "")
        };
        exec_624(
            &admin,
            &format!(
                "INSERT INTO {db}.log_landing {LANDING_624P4} \
                 SELECT toUnixTimestamp64Milli(now64(3)), 0, if(s < 10, 'c624p4bj', 'c624p4bl'), \
                 toUInt128(6240600 + s), toInt64({t}) + toInt64(i) * 1000000000, 0, \
                 if(s < 10, \
                   concat('{{\"request_id\":\"', rid, '\",\"status\":', toString(status), \
                     ',\"route\":\"', route, '\",\"latency\":', \
                     if(kind = 0, lat, concat('\"', lat, '\"')), ',\"a\":{{\"b\":', ab, '}}}}'), \
                   concat('request_id=', rid, ' status=', toString(status), ' route=', route, \
                     ' latency=', lat)), \
                 '', toDate(0), '', 0, '', 0 \
                 FROM (SELECT intDiv(number, 600) AS s, number % 600 + 1 AS i, \
                       {k} % 5 AS kind, \
                       multiIf(kind = 0, toString({k} % 997 + 1), \
                               kind = 1, concat(toString({k} % 50 + 1), '.', toString({k} % 9 + 1)), \
                               kind = 2, multiIf({k} % 3 = 0, concat(toString({k} % 900 + 1), 'ms'), \
                                                 {k} % 3 = 1, concat(toString({k} % 50 + 1), '.5s'), \
                                                 concat(toString({k} % 5 + 1), 'm')), \
                               kind = 3, multiIf({k} % 3 = 0, concat(toString({k} % 900 + 1), 'KB'), \
                                                 {k} % 3 = 1, concat(toString({k} % 50 + 1), 'MiB'), \
                                                 concat(toString({k} % 10000 + 1), 'b')), \
                               toString({k} % 400 + 1)) AS lat, \
                       if({k} % 2 = 0, toString({k} % 999 + 1), \
                          concat(toString({k} % 99 + 1), '.', leftPad(toString({k} % 100), 2, '0'))) AS ab, \
                       ['ra', 'rb', 'rc', 'rd'][(i + s) % 4 + 1] AS route, \
                       [200, 404, 500][{k} % 3 + 1] AS status, \
                       hex({k} * 1000 + s) AS rid \
                       FROM numbers(20 * 600) {filter})"
            ),
        )
        .await;
    }
    (admin, db, t)
}

/// T1's lowered queries of part 4b over `c624p4b` at `range`, each with
/// `| __error__=""` after the unwrap and one conversion by turns.
fn parser_queries_624p4b(range: &str) -> Vec<String> {
    let json = r#"{service_name="c624p4bj"}"#;
    let lines = r#"{service_name="c624p4bl"}"#;
    // (selector, stage, bare)
    let parsers = [
        (json, "| json", true),
        (json, r#"| json latency="latency", route="route""#, false),
        (lines, "| logfmt", true),
        (lines, r#"| logfmt latency="latency", route="route""#, false),
        (
            lines,
            r#"| regexp "route=(?P<route>\\S+) latency=(?P<latency>\\S+)""#,
            false,
        ),
        (
            lines,
            r#"| pattern "<_> route=<route> latency=<latency>""#,
            false,
        ),
    ];
    let conversions = ["latency", "duration(latency)", "bytes(latency)"];
    let mut out = Vec::new();
    let mut turn = 0usize;
    let mut next = || {
        turn += 1;
        conversions[turn % 3]
    };
    for (sel, stage, bare) in parsers {
        for op in [
            "avg_over_time",
            "min_over_time",
            "max_over_time",
            "stddev_over_time",
            "stdvar_over_time",
            "quantile_over_time",
            "first_over_time",
            "last_over_time",
        ] {
            let param = if op == "quantile_over_time" {
                "0.9, "
            } else {
                ""
            };
            let inner = |conv: &str| {
                format!(r#"{op}({param}{sel} {stage} | unwrap {conv} | __error__="" [{range}])"#)
            };
            out.push(format!("{} by (route)", inner(next())));
            if !bare {
                out.push(inner(next()));
                out.push(format!("{} without (request_id)", inner(next())));
            }
        }
        for (op, injected) in [
            ("sum_over_time", true),
            ("rate", true),
            ("rate_counter", false),
        ] {
            let inner = |conv: &str| {
                format!(r#"{op}({sel} {stage} | unwrap {conv} | __error__="" [{range}])"#)
            };
            if !bare {
                out.push(inner(next()));
                out.push(format!("max by (route) ({})", inner(next())));
            }
            if !bare || injected {
                out.push(format!("sum by (route) ({})", inner(next())));
            }
        }
    }
    out
}

/// **T4 (issue #624, part 4b): an `| unwrap` reducer after a parser answers
/// what today's route answers, bit for bit**, at a range equal to, above
/// and below the step; the query log holds the reduced statement for one run
/// and today's read for the other. (b) A nested path `x="a.b"` finishes in
/// the reduced statement, with no 395 and no raw read.
#[tokio::test]
async fn unwrap_after_a_parser_answers_as_todays_route() {
    skip_unless_live!();
    let (admin, db, t) = seed_parser_corpus_624p4b().await;
    // Non-vacuous: both kinds of tie, each between two values.
    for service in ["c624p4bj", "c624p4bl"] {
        let same_stream = count_624p4(
            &admin,
            &format!(
                "SELECT count() AS n FROM (SELECT fingerprint, timestamp_ns FROM {db}.log_samples \
                 WHERE service = '{service}' GROUP BY fingerprint, timestamp_ns \
                 HAVING uniqExact(body) > 1)"
            ),
        )
        .await;
        assert!(
            same_stream >= 100,
            "{service}: same-stream ties: {same_stream}"
        );
        let cross_stream = count_624p4(
            &admin,
            &format!(
                "SELECT count() AS n FROM (SELECT timestamp_ns FROM {db}.log_samples \
                 WHERE service = '{service}' GROUP BY timestamp_ns HAVING uniqExact(fingerprint) > 1)"
            ),
        )
        .await;
        assert!(
            cross_stream >= 100,
            "{service}: cross-stream ties: {cross_stream}"
        );
    }

    let params = grid_624p4(t, t + 600_000_000_000);
    let mut comments: Vec<(String, bool)> = Vec::new();
    let mut nonempty = 0usize;
    let mut total = 0usize;
    for range in ["60s", "150s", "30s"] {
        for query in parser_queries_624p4b(range) {
            let lowered_comment = format!("c624p4bt4-{}", uuid::Uuid::new_v4().simple());
            let todays_comment = format!("c624p4bt4-{}", uuid::Uuid::new_v4().simple());
            let lowered = run_624(
                &db,
                hooks_624(false, None, None),
                Some(&lowered_comment),
                &query,
                &params,
            )
            .await;
            let todays = run_624(
                &db,
                hooks_624(true, None, None),
                Some(&todays_comment),
                &query,
                &params,
            )
            .await;
            same_outcome_624(&query, &lowered, &todays);
            total += 1;
            if lowered.as_ref().is_ok_and(|a| !a.is_empty()) {
                nonempty += 1;
            }
            comments.push((lowered_comment, true));
            comments.push((todays_comment, false));
        }
    }
    assert_eq!(total, 3 * (4 * 33 + 2 * 10));
    assert_eq!(nonempty, total, "every query answers");

    // (b) the nested path.
    let instant_at = t + 600_000_000_000;
    let nested = [
        r#"max_over_time({service_name="c624p4bj"} | json x="a.b", route="route" | unwrap x [60s])"#,
        r#"sum by (route) (sum_over_time({service_name="c624p4bj"} | json x="a.b", route="route" | unwrap x [60s]))"#,
    ];
    let mut nested_comments: Vec<String> = Vec::new();
    for query in nested {
        let lowered_comment = format!("c624p4bt4b-{}", uuid::Uuid::new_v4().simple());
        let lowered = run_624(
            &db,
            hooks_624(false, None, None),
            Some(&lowered_comment),
            query,
            &params,
        )
        .await;
        let todays = run_624(&db, hooks_624(true, None, None), None, query, &params).await;
        same_outcome_624(query, &lowered, &todays);
        assert!(
            lowered.as_ref().is_ok_and(|a| !a.is_empty()),
            "{query}: answers"
        );
        nested_comments.push(lowered_comment);
        let instant_comment = format!("c624p4bt4b-{}", uuid::Uuid::new_v4().simple());
        let lowered = run_instant_624p4(
            &db,
            hooks_624(false, None, None),
            Some(&instant_comment),
            query,
            instant_at,
        )
        .await;
        let todays =
            run_instant_624p4(&db, hooks_624(true, None, None), None, query, instant_at).await;
        same_instant_outcome_624p4(query, &lowered, &todays);
        assert!(
            lowered.as_ref().is_ok_and(|a| !a.is_empty()),
            "{query}: answers at an instant"
        );
        nested_comments.push(instant_comment);
    }

    let all: Vec<String> = comments
        .iter()
        .map(|(c, _)| c.clone())
        .chain(nested_comments.iter().cloned())
        .collect();
    let logged = logged_624(&admin, &all).await;
    for (comment, lowered) in &comments {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{comment}: no statement logged"));
        if *lowered {
            assert!(
                rows.iter()
                    .any(|r| is_reduced_624p4(&r.query) && r.exception_code == 0),
                "{comment}: the reduced statement answers: {rows:#?}"
            );
            assert!(
                !rows.iter().any(|r| is_todays_624(&r.query)),
                "{comment}: no read of today's route: {rows:#?}"
            );
        } else {
            assert!(
                rows.iter().any(|r| is_todays_624(&r.query)),
                "{comment}: {rows:#?}"
            );
            assert!(
                !rows.iter().any(|r| is_reduced_624p4(&r.query)),
                "{comment}: {rows:#?}"
            );
        }
    }
    for comment in &nested_comments {
        let rows = logged
            .get(comment)
            .unwrap_or_else(|| panic!("{comment}: no statement logged"));
        let reduced: Vec<&CommentedRow624> =
            rows.iter().filter(|r| is_reduced_624p4(&r.query)).collect();
        assert!(
            reduced.len() == 1 && reduced[0].exception_code == 0,
            "{comment}: exactly one reduced statement, finished: {rows:#?}"
        );
        assert!(
            !rows.iter().any(|r| is_todays_624(&r.query)),
            "{comment}: no raw read: {rows:#?}"
        );
    }
    drop_db_624(&admin, &db).await;
}

/// The statements one run logged over the samples, in order, as
/// `(reduced, today's read, exception code)`.
fn statement_kinds_624p4b(rows: &[CommentedRow624]) -> Vec<(&'static str, i32)> {
    rows.iter()
        .filter_map(|r| {
            if is_reduced_624p4(&r.query) {
                Some(("reduced", r.exception_code))
            } else if is_todays_624(&r.query) {
                Some(("today", r.exception_code))
            } else {
                None
            }
        })
        .collect()
}

/// **T6 (issue #624, part 4b): a stream labelled `x` gives `x` from the
/// label**, before the parser and before structured metadata.
///
/// ```text
/// A {app="a", x="7"}  {"x":3,"route":"r"}  metadata x="5"
/// B {app="a"}         {"x":3,"route":"r"}
/// C {app="c"}         {"x":3,"route":"r"}  metadata x="5"
/// ```
///
/// (a) `sum by (route)` over `| json | unwrap x` is 7 + 3 on both routes,
/// from the reduced statement; (b) with no parser, in drop mode, A answers
/// 7 with `x_extracted="5"` from the reduced statement, and with a parser in
/// drop mode the query runs no reduced statement; (c) on C, metadata `x`
/// beside a parser fails the reduced statement (395) and today's route
/// answers 5.
#[tokio::test]
async fn a_stream_labelled_x_reads_the_label() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4b_t6_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    for (fp, service, labels) in [
        (
            6240701u64,
            "c624p4bt6a",
            r#"{"app":"a","service_name":"c624p4bt6a","x":"7"}"#,
        ),
        (
            6240702u64,
            "c624p4bt6a",
            r#"{"app":"a","service_name":"c624p4bt6a"}"#,
        ),
        (
            6240703u64,
            "c624p4bt6c",
            r#"{"app":"c","service_name":"c624p4bt6c"}"#,
        ),
    ] {
        land_stream_624(&admin, &db, t, fp, service, labels).await;
    }
    let line = r#"{"x":3,"route":"r"}"#;
    land_line_624(
        &admin,
        &db,
        6240701,
        "c624p4bt6a",
        t + 30 * sec,
        line,
        r#"{"x":"5"}"#,
    )
    .await;
    land_line_624(&admin, &db, 6240702, "c624p4bt6a", t + 30 * sec, line, "").await;
    land_line_624(
        &admin,
        &db,
        6240703,
        "c624p4bt6c",
        t + 30 * sec,
        line,
        r#"{"x":"5"}"#,
    )
    .await;
    let params = grid_624p4(t + 60 * sec, t + 60 * sec);

    // (a)
    let q = r#"sum by (route) (sum_over_time({app="a"} | json | unwrap x [60s]))"#;
    let lowered_comment = format!("c624p4bt6-{}", uuid::Uuid::new_v4().simple());
    let lowered = run_624(
        &db,
        hooks_624(false, None, None),
        Some(&lowered_comment),
        q,
        &params,
    )
    .await;
    let todays = run_624(&db, hooks_624(true, None, None), None, q, &params).await;
    same_outcome_624(q, &lowered, &todays);
    let want: Vec<AnswerSeries> = vec![(
        vec![("route".to_string(), "r".to_string())],
        vec![(t + 60 * sec, 10f64.to_bits())],
    )];
    assert_eq!(lowered.as_ref().ok(), Some(&want), "{q}: 7 + 3");

    // (b)
    let q_b = r#"max_over_time({app="a"} | unwrap x [60s])"#;
    let b_comment = format!("c624p4bt6-{}", uuid::Uuid::new_v4().simple());
    let lowered_b = run_624(
        &db,
        hooks_624(false, None, None),
        Some(&b_comment),
        q_b,
        &params,
    )
    .await;
    let todays_b = run_624(&db, hooks_624(true, None, None), None, q_b, &params).await;
    same_outcome_624(q_b, &lowered_b, &todays_b);
    let want_b: Vec<AnswerSeries> = vec![(
        vec![
            ("app".to_string(), "a".to_string()),
            ("service_name".to_string(), "c624p4bt6a".to_string()),
            ("x_extracted".to_string(), "5".to_string()),
        ],
        vec![(t + 60 * sec, 7f64.to_bits())],
    )];
    assert_eq!(
        lowered_b.as_ref().ok(),
        Some(&want_b),
        "{q_b}: A's label, its metadata renamed"
    );
    let q_bp = r#"max_over_time({app="a"} | json x="x" | unwrap x [60s])"#;
    let bp_comment = format!("c624p4bt6-{}", uuid::Uuid::new_v4().simple());
    let lowered_bp = run_624(
        &db,
        hooks_624(false, None, None),
        Some(&bp_comment),
        q_bp,
        &params,
    )
    .await;
    let todays_bp = run_624(&db, hooks_624(true, None, None), None, q_bp, &params).await;
    same_outcome_624(q_bp, &lowered_bp, &todays_bp);

    // (c)
    let q_c = r#"sum by (route) (sum_over_time({app="c"} | json | unwrap x [60s]))"#;
    let c_comment = format!("c624p4bt6-{}", uuid::Uuid::new_v4().simple());
    let lowered_c = run_instant_624p4(
        &db,
        hooks_624(false, None, None),
        Some(&c_comment),
        q_c,
        t + 31 * sec,
    )
    .await;
    assert_eq!(
        lowered_c.as_ref().ok(),
        Some(&vec![(
            vec![("route".to_string(), "r".to_string())],
            5f64.to_bits()
        )]),
        "{q_c}: the metadata's 5"
    );

    let logged = logged_624(
        &admin,
        &[
            lowered_comment.clone(),
            b_comment.clone(),
            bp_comment.clone(),
            c_comment.clone(),
        ],
    )
    .await;
    let kinds = |c: &str| statement_kinds_624p4b(logged.get(c).map(Vec::as_slice).unwrap_or(&[]));
    assert_eq!(kinds(&lowered_comment), vec![("reduced", 0)], "{q}");
    assert_eq!(kinds(&b_comment), vec![("reduced", 0)], "{q_b}");
    assert!(
        !kinds(&bp_comment).iter().any(|(k, _)| *k == "reduced"),
        "{q_bp}: no reduced statement: {:?}",
        kinds(&bp_comment)
    );
    let c_kinds = kinds(&c_comment);
    assert_eq!(c_kinds.len(), 2, "{q_c}: {c_kinds:?}");
    assert_eq!(
        c_kinds[0],
        ("reduced", 395),
        "{q_c}: the reduced statement fails"
    );
    assert_eq!(c_kinds[1].0, "today", "{q_c}: then today's read");
    drop_db_624(&admin, &db).await;
}

/// **T7 (issue #624, part 4b): a parser row the readers cannot decide sends
/// the query to today's route.**
///
/// ```text
/// (a) a JSON line {"x":1              395, then today's read
/// (b) a logfmt line x=1 x=2           395, then today's read
/// (c) a regexp line that does not match   no sample, no throw
/// (d) a JSON line {"x":-0} under bytes(x) 395, then today's read
/// (e) `not json at all` beside {"x":2,"route":"r"}, with and without
///     | __error__="": {route="r"} 2, after one 395
/// ```
#[tokio::test]
async fn undecided_parser_rows_answer_on_todays_route() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4b_t7_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    let at = t + 60 * sec;
    // (service, lines)
    let streams: [(&str, &[&str]); 5] = [
        ("c624p4bt7a", &[r#"{"x":1"#, r#"{"x":2,"route":"r"}"#]),
        ("c624p4bt7b", &["route=r x=1 x=2", "route=r x=3"]),
        ("c624p4bt7c", &["nothing here", "x=4 tail"]),
        ("c624p4bt7d", &[r#"{"x":-0}"#, r#"{"x":5}"#]),
        ("c624p4bt7e", &["not json at all", r#"{"x":2,"route":"r"}"#]),
    ];
    for (i, (service, lines)) in streams.iter().enumerate() {
        let fp = 6240710 + i as u64;
        land_stream_624(
            &admin,
            &db,
            t,
            fp,
            service,
            &format!(r#"{{"service_name":"{service}"}}"#),
        )
        .await;
        for (j, line) in lines.iter().enumerate() {
            land_line_624(
                &admin,
                &db,
                fp,
                service,
                t + (10 + j as i64) * sec,
                line,
                "",
            )
            .await;
        }
    }
    // (query, expected statements of the lowered run, the answer when pinned)
    type Case<'a> = (
        &'a str,
        &'a [(&'a str, Option<i32>)],
        Option<InstantAnswer624p4>,
    );
    let cases: [Case<'_>; 6] = [
        (
            r#"sum by (route) (sum_over_time({service_name="c624p4bt7a"} | json | unwrap x [60s]))"#,
            &[("reduced", Some(395)), ("today", None)],
            None,
        ),
        (
            r#"sum by (route) (sum_over_time({service_name="c624p4bt7b"} | logfmt | unwrap x [60s]))"#,
            &[("reduced", Some(395)), ("today", None)],
            None,
        ),
        (
            r#"max_over_time({service_name="c624p4bt7c"} | regexp "x=(?P<x>\\S+)" | unwrap x [60s])"#,
            &[("reduced", Some(0))],
            None,
        ),
        (
            r#"max_over_time({service_name="c624p4bt7d"} | json x="x" | unwrap bytes(x) [60s])"#,
            &[("reduced", Some(395)), ("today", None)],
            None,
        ),
        (
            r#"sum by (route) (sum_over_time({service_name="c624p4bt7e"} | json | unwrap x [60s]))"#,
            &[("reduced", Some(395)), ("today", None)],
            Some(vec![(
                vec![("route".to_string(), "r".to_string())],
                2f64.to_bits(),
            )]),
        ),
        (
            r#"sum by (route) (sum_over_time({service_name="c624p4bt7e"} | json | __error__="" | unwrap x [60s]))"#,
            &[("reduced", Some(395)), ("today", None)],
            Some(vec![(
                vec![("route".to_string(), "r".to_string())],
                2f64.to_bits(),
            )]),
        ),
    ];
    let mut comments = Vec::new();
    for (query, _, want) in &cases {
        let comment = format!("c624p4bt7-{}", uuid::Uuid::new_v4().simple());
        let lowered =
            run_instant_624p4(&db, hooks_624(false, None, None), Some(&comment), query, at).await;
        let todays = run_instant_624p4(&db, hooks_624(true, None, None), None, query, at).await;
        same_instant_outcome_624p4(query, &lowered, &todays);
        if let Some(want) = want {
            assert_eq!(lowered.as_ref().ok(), Some(want), "{query}");
        }
        comments.push(comment);
    }
    let logged = logged_624(&admin, &comments).await;
    for ((query, want, _), comment) in cases.iter().zip(&comments) {
        let got = statement_kinds_624p4b(logged.get(comment).map(Vec::as_slice).unwrap_or(&[]));
        assert_eq!(got.len(), want.len(), "{query}: {got:?}");
        for ((kind, code), (want_kind, want_code)) in got.iter().zip(want.iter()) {
            assert_eq!(kind, want_kind, "{query}: {got:?}");
            if let Some(c) = want_code {
                assert_eq!(code, c, "{query}: {got:?}");
            }
        }
    }
    drop_db_624(&admin, &db).await;
}

/// **T9 (issue #624, part 4b): #507's group key statement is gone.** For each of
/// its five shapes, the statement EXPLAIN reports is the reduced statement
/// or today's read, and no statement reads `uw_r`.
#[tokio::test]
async fn the_w4_statement_is_gone() {
    skip_unless_live!();
    let (admin, db) = fresh_db_624(pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_p4b_t9_{}",
        uuid::Uuid::new_v4().simple()
    )))
    .await;
    let t = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let sec = 1_000_000_000i64;
    land_stream_624(
        &admin,
        &db,
        t,
        6240720,
        "c624p4bt9",
        r#"{"service_name":"c624p4bt9"}"#,
    )
    .await;
    for (j, line) in [r#"{"latency":5,"code":"a"}"#, r#"{"latency":7,"code":"b"}"#]
        .iter()
        .enumerate()
    {
        land_line_624(
            &admin,
            &db,
            6240720,
            "c624p4bt9",
            t + (10 + j as i64) * sec,
            line,
            "",
        )
        .await;
    }
    let s = r#"{service_name="c624p4bt9"}"#;
    let shapes = [
        format!(r#"sum_over_time({s} | json lat="latency", c="code" | unwrap lat [1m])"#),
        format!(r#"avg_over_time({s} | json lat="latency", c="code" | unwrap lat [1m]) by (c)"#),
        format!(
            r#"avg_over_time({s} | json lat="latency", c="code" | unwrap lat [1m]) without (c)"#
        ),
        format!(r#"sum by (code) (sum_over_time({s} | json | unwrap latency [1m]))"#),
        format!(r#"avg_over_time({s} | json | unwrap latency [1m]) by (code)"#),
    ];
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 64 * 1024 * 1024),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let params = grid_624p4(t + 60 * sec, t + 60 * sec);
    for q in &shapes {
        let (_, _, explain) = engine
            .query_explained(&parse(q).expect("parse"), &params)
            .await
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        let reads: Vec<&str> = explain
            .stages
            .iter()
            .filter(|s| s.name == "metric_read")
            .map(|s| s.sql.as_str())
            .collect();
        assert!(!reads.is_empty(), "{q}: a metric read is reported");
        for sql in &reads {
            assert!(
                !sql.contains("uw_r"),
                "{q}: the group key statement is reported:\n{sql}"
            );
        }
        let last = reads[reads.len() - 1];
        assert!(
            is_reduced_624p4(last) || is_todays_624(last),
            "{q}: the reduced statement or today's read:\n{last}"
        );
    }
    drop_db_624(&admin, &db).await;
}
