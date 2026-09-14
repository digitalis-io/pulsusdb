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
//! granule skipping — every granule fits in one bloom filter check either
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
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test query_log_gates
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_logql::parse;
use pulsus_read::logql::predicate::{literal, month_literal};
use pulsus_read::logql::sql::{self, TimeWindow};
use pulsus_read::logql::{Direction, Plan, PlanCtx, QueryParams, QuerySpec, plan};
use pulsus_read::{EngineConfig, LogQlEngine, QueryResult, ReadError};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

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
    RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    }
}

fn plan_ctx(db: &str) -> PlanCtx<'_> {
    PlanCtx {
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
    service: String,
    fingerprint: u64,
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
            service: SERVICE.to_string(),
            fingerprint: FP_CORPUS,
            timestamp_ns,
            severity: 0,
            body,
        });
    }
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("bulk insert corpus");

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
        &format!("{db}.log_samples"),
        &[literal(SERVICE)],
        &[FP_CORPUS],
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
        &format!("{db}.log_samples"),
        &[literal(SERVICE)],
        &[FP_CORPUS],
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
    let engine = LogQlEngine::new(data_client(&run_db).await, engine_config(&run_db, budget));

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
    let engine = LogQlEngine::new(data_client(&run_db).await, engine_config(&run_db, 0));
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
            service: SERVICE.to_string(),
            fingerprint: FP_CORPUS,
            timestamp_ns,
            severity: 0,
            body,
        });
    }
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("bulk insert detected corpus");
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
    let engine = LogQlEngine::new(data_client(db).await, engine_config(db, 8 * 1024 * 1024));
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

    let engine = LogQlEngine::new(data_client(db).await, engine_config(db, 64 * 1024));
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
    let engine = LogQlEngine::new(data_client(db).await, engine_config(db, 64 * 1024));
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

    let mut fan_in = Vec::new();
    for (i, (month, n)) in cases.iter().enumerate() {
        let (start_ns, end_ns) = month_window(month);
        let sql = sql::detected_labels(
            &format!("{db}.log_streams_idx"),
            &[month_literal(month_year(month), month_month(month))],
            None,
            &format!("{db}.log_metrics_5s"),
            sql::TimeWindow { start_ns, end_ns },
            5_000_000_000,
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
    let engine = LogQlEngine::new(client, config);

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
    let engine = LogQlEngine::new(client, config);
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
    let rows = mem_ceiling_rows(&admin, &run_db, &m1).await;
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
    let rows = mem_ceiling_rows(&admin, &run_db, &m2).await;
    assert_eq!(
        rows.len(),
        2,
        "stage one, then stage two\n{}",
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
        rows[1].q.starts_with("SELECT DISTINCT val AS value"),
        "rows[1] must be stage two, got {:?}",
        rows[1].q
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
    for sql in [
        format!(
            "INSERT INTO {run_db}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT unhex('{trace_hex}'), \
                    reinterpretAsFixedString(toUInt64(number + 1)), \
                    reinterpretAsFixedString(toUInt64(0)), \
                    'op', 'checkout', {ts_ns} + number, 1000000, 0, 2, 0, '' \
             FROM numbers(64)"
        ),
        format!(
            "INSERT INTO {run_db}.trace_attrs_idx \
             (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
             SELECT toDate({date_days}), 'http.status_code', '500', 'span', NULL, \
                    {ts_ns} + number, unhex('{trace_hex}'), \
                    reinterpretAsFixedString(toUInt64(number + 1)), 1000000 \
             FROM numbers(64)"
        ),
        format!(
            "INSERT INTO {run_db}.trace_tag_catalog (scope, key, val) \
             SELECT 'span', 'http.status_code', concat('v', toString(number)) FROM numbers(64)"
        ),
    ] {
        seed.execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .unwrap_or_else(|e| panic!("seed failed: {e}\nSQL:\n{sql}"));
    }

    let config = pulsus_read::TraceReadConfig {
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
        edges_table: "trace_edges".to_string(),
        max_candidates: 100_000,
        scan_budget_rows: 50_000_000,
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

    // 1. Search — `search_settings` + `generator_settings` (phase 1) and
    //    the phase-2 hydration/membership reads.
    let query = pulsus_traceql::parse(r#"{ span.http.status_code = "500" }"#).expect("parses");
    let plan = pulsus_read::traces::search_plan::plan_search(
        &query,
        &pulsus_read::traces::search_plan::SearchParams {
            start_ns: ts_ns - 3_600_000_000_000,
            end_ns: ts_ns + 3_600_000_000_000,
            limit: 20,
            spss: 10,
        },
        &engine.search_ctx(),
    )
    .expect("plans");
    let found = engine.search(&plan).await.expect("search executes");
    assert!(
        !found.traces.is_empty(),
        "the search fixture must return rows, or the phase-2 reads never dispatch"
    );

    // 2. Trace-by-id — the §4.2 point read, the third settings root.
    let spans = engine.fetch_by_id(trace_hex).await.expect("point read");
    assert!(!spans.is_empty(), "the point-read fixture must return rows");

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

/// Creates `{db}.{table}` with `metric_series`'s own DDL and fills it with
/// [`SERIES_472`] rows over [`NAMES_472`] names in ONE activity bucket,
/// server-side (`INSERT … SELECT FROM numbers`), so no row crosses the
/// wire. `pad_bytes` is the only thing that differs between the two
/// tables: `(metric_name, fingerprint, unix_milli)` is a pure function of
/// `number` and `bucket_ms`, so the two tables are identical in every
/// column the `WHERE` and the projection touch — asserted by the identity
/// hash in the test below, which is what makes the blob-invariance
/// comparison a comparison of blob size and nothing else.
async fn seed_metric_series_472(client: &ChClient, db: &str, table: &str, bucket: i64, pad: u64) {
    client
        .execute(
            &format!(
                "CREATE TABLE {db}.{table} (\
                   metric_name  LowCardinality(String), \
                   fingerprint  UInt64  CODEC(Delta(8), ZSTD(1)), \
                   unix_milli   Int64   CODEC(Delta(8), ZSTD(1)), \
                   labels       String  CODEC(ZSTD(5))\
                 ) ENGINE = MergeTree \
                 PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(unix_milli)) \
                 ORDER BY (metric_name, fingerprint, unix_milli)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create the #472 corpus table");
    client
        .execute(
            &format!(
                "INSERT INTO {db}.{table} \
                 SELECT concat('metric_', leftPad(toString(number % {NAMES_472}), 2, '0')), \
                        number + 1, \
                        {bucket}, \
                        concat('{{\"job\":\"api\",\"namespace\":\"ns-', toString(number % 13), \
                               '\",\"pod\":\"pod-', toString(number), \
                               '\",\"pad\":\"', repeat('x', {pad}), '\"}}') \
                 FROM numbers({SERIES_472})"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed the #472 corpus");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CorpusShapeRow {
    rows: u64,
    names: u64,
    mean_label_bytes: f64,
    identity: u64,
}

async fn corpus_shape(client: &ChClient, db: &str, table: &str) -> CorpusShapeRow {
    let sql = format!(
        "SELECT count() AS rows, uniqExact(metric_name) AS names, \
         avg(length(labels)) AS mean_label_bytes, \
         sum(cityHash64(metric_name, fingerprint, unix_milli)) AS identity \
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
/// **unfiltered** call only. A `match[]` carrying a label matcher renders
/// `JSONExtractString(labels, …)` into the same `WHERE`, so `labels` is
/// read to evaluate the filter and the narrow form's bytes grow with the
/// blob too; that case's win is transport and parse count, not bytes read.
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
    seed_metric_series_472(&client, db, "series_small", bucket, 100).await;
    seed_metric_series_472(&client, db, "series_big", bucket, 1_890).await;

    let small = corpus_shape(&client, db, "series_small").await;
    let big = corpus_shape(&client, db, "series_big").await;
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
        "the two tables must be identical in (metric_name, fingerprint, unix_milli) — otherwise \
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
        let wide_sql =
            pulsus_read::metrics::sql::discovery_query(&qualified, &filter, window, bucket_ms);
        let narrow_sql = pulsus_read::metrics::sql::discovery_distinct_names_query(
            &qualified, &filter, window, bucket_ms,
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
    service: String,
    fingerprint: u64,
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
    }

    let row = |fp: u64, ts: i64, body: &str, sm: &str| BucketedSeedRow {
        service: service.to_string(),
        fingerprint: fp,
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
    (admin, db, t)
}

/// **The lowered answer is the client answer, bit for bit — and the two
/// answers come from two different code paths** (issue #507, W2).
///
/// `count_over_time({…}[1m])` at a 1m step is a clean bucketed chain and
/// plans `client: None`, so the database counts. The same selector with
/// `| drop zzz` — a label the corpus does not carry, so the stage changes
/// no label set — is a pipeline, so it plans `client: Some(..)` and every
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
    let client_query = format!(r#"count_over_time({{service_name="{service}"}} | drop zzz [1m])"#);

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

    let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db, 64 * 1024 * 1024));
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
    let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db, 64 * 1024 * 1024));
    for (query, path) in [
        (
            format!(r#"count_over_time({{service_name="{service}"}}[1s])"#),
            "the bucketed path",
        ),
        (
            format!(r#"count_over_time({{service_name="{service}"}} | drop zzz [1s])"#),
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
    let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db, 64 * 1024 * 1024));

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
// W4 (issue #507): does a single-threaded `sum` accumulate in scan order?
// ---------------------------------------------------------------------

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct UnwrapSeedRow {
    service: String,
    fingerprint: u64,
    timestamp_ns: i64,
    severity: i8,
    body: String,
    structured_metadata: String,
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

/// **The claim W4's exact comparison rested on, measured — and it is
/// false** (issue #507 W4 §6).
///
/// The claim was: at `max_threads = 1` the database's `sum` accumulates a
/// single fingerprint's rows in `(service, fingerprint, timestamp_ns)`
/// order, which within one fingerprint is timestamp order, so the two
/// summations are the same summation and agree bit for bit.
///
/// **They do not agree, at any of the three sizes**, measured on
/// `clickhouse/clickhouse-server:26.3` (server 26.3.29.7):
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
                service: "w4".to_string(),
                fingerprint: fp,
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
    // stops holding, the database has changed its accumulation and W4's
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
// W4 (issue #507): the spread gate and the reproducibility gate.
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
/// **Why it refuses without a prefix** (issue #507 W4, review round 5).
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

/// The unwrapped-value corpus both W4 gates read: `n` deterministic values
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
            service: "w4".to_string(),
            fingerprint: fp,
            timestamp_ns: t0 + i as i64,
            severity: 0,
            body: format!(r#"{{"v":{v:?}}}"#),
            structured_metadata: String::new(),
        })
        .collect()
}

/// `2(n−1)·u·Σ|vᵢ|` with `u = 2⁻⁵³` — the most two evaluation orders of the
/// same `n` values can differ by (issue #507 W4 §5).
fn summation_spread_bound(values: &[f64]) -> f64 {
    let sum_abs: f64 = values.iter().map(|v| v.abs()).sum();
    2.0 * ((values.len() as f64) - 1.0) * (f64::EPSILON / 2.0) * sum_abs
}

/// **The gate that would tell us the ruling was wrong** (issue #507 W4 §6).
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
/// bit, at `max_threads = 1` and a fixed part layout** (issue #507 W4 §3).
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
            client
                .insert_block("log_samples", &rows)
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
// W4 (issue #507): the unwrapped read, end to end, one case per measured
// class of parser disagreement.
// ---------------------------------------------------------------------

/// **One case per measured class, each with the value that produces it**
/// (issue #507 W4, review round 4).
///
/// ```text
///  case        value                       expected     the two parsers
///  ordinary    45.25, "2.25", 4            LOWERS       agree; the quoted form is the point
///  one_ulp     "9367469347402735e292"      LOWERS       agree ONLY under the parser setting
///  class A     "E12"                       FALLS BACK   NULL here, a parse error in ours
///  class B     "1.7976931348623159e308"    FALLS BACK   NULL here, inf in ours
///  class C     "5e-324"                    LOWERS       bits 0x1 on both sides
///  over        "inf"                       FALLS BACK   both parse it; the prefix test does not
///  nan_exp     "0e999999"                  FALLS BACK   both +0; the denotation clause refuses it
///  under_exp   "9999999999999999e-324"     LOWERS       bits 0x730d67819e8d2 on both sides
///  shadowed    metadata latency=5, bodies 7 and 8       FALLS BACK   TWO series, keyed by the
///                                                       parsed value under latency_extracted
///  fixed_sub   "0.000…0005", 326 chars     LOWERS       bits 0x1 on both sides
///  avg_over    two × 1e308, avg_over_time  FALLS BACK   inf here, 1e308 in ours — a FINITE
///                                                       mean turned into infinity
/// ```
///
/// **Three cases moved from FALLS BACK to LOWERS in review round 4**, and
/// the reason is one statement-level setting rather than a change to the
/// guard. `sql::UNWRAP_PARSER_SETTING` renders `precise_float_parsing = 1`
/// into the statement; under it C, `under_exp` and `fixed_sub` convert to
/// the same bits as `f64::from_str` and qualify. `one_ulp` is the case that
/// makes the setting necessary rather than tidy: with `n = 1` the
/// summation-order bound `2(n−1)·u·Σ|vᵢ|` is ZERO, so the default parser's
/// `0x7fe0acb5cadc2918` against our `0x7fe0acb5cadc2917` is a wrong answer
/// with nothing to absorb it. Delete the setting from `metric_range_unwrapped`
/// and this case is what goes red.
///
/// A and B are still refused, now by `isNotNull`: both texts convert to
/// NULL under the setting. `over` and `nan_exp` are over-rejections — the
/// two parsers agree on `"inf"` and on `"0e999999"` — kept because dropping
/// a conjunct widens the lowered set, which is not a thing to do on one
/// setting at the end of a wave.
///
/// **The control is the same query with `| drop zzz` after the unwrap** —
/// a label filter after the unwrap blocks the lowering, and one naming a label the
/// corpus does not carry keeps every line (an absent label reads as empty). The plan shapes are
/// asserted to differ first, so the test cannot compare one path with
/// itself.
///
/// The fallback is observed rather than inferred: on a fallback the
/// explain payload's LAST `metric_read` is the client sliding scan, whose
/// `ORDER BY service ASC` no lowered statement carries.
#[tokio::test]
async fn the_unwrapped_read_agrees_with_the_client_path_or_falls_back() {
    skip_unless_live!();
    const STEP: i64 = 60_000_000_000;
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    sweep_leftovers(
        &admin,
        &pulsus_testkit::test_db("pulsus_read_it_qlg_w4unwrap_"),
    )
    .await;
    let db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_w4unwrap_{}",
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

    let t = ((now_ns() - 3_600_000_000_000) / STEP) * STEP;
    // Each class gets its own stream, so one class's fallback cannot
    // decide another's.
    // The 326-character fixed-point spelling of `5e-324`: no exponent, so
    // round 1's `[eE]` clause did not see it (review round 2).
    let fixed_subnormal = format!("\"0.{}5\"", "0".repeat(323));
    /// One case of the differential: its name and stream, the values every
    /// row carries, the structured metadata on every row, the reducer, and
    /// whether the lowered query is expected to fall back.
    type UnwrapCase<'a> = (&'a str, u64, &'a [&'a str], &'a str, &'a str, bool);
    let cases: [UnwrapCase<'_>; 11] = [
        (
            "ordinary",
            201,
            &["45.25", "\"2.25\"", "4"],
            "",
            "sum_over_time",
            false,
        ),
        // Review round 4: ONE row, so the summation-order bound is zero
        // and the only slack is the parser's. The default parser converts
        // this text to `0x7fe0acb5cadc2918` and `f64::from_str` gives
        // `0x7fe0acb5cadc2917`; the statement's
        // `SETTINGS precise_float_parsing = 1` is what makes the two sides
        // equal, and this case is what reddens without it.
        (
            "one_ulp",
            211,
            &["\"9367469347402735e292\""],
            "",
            "sum_over_time",
            false,
        ),
        ("class_a", 202, &["1", "\"E12\""], "", "sum_over_time", true),
        (
            "class_b",
            203,
            &["1", "\"1.7976931348623159e308\""],
            "",
            "sum_over_time",
            true,
        ),
        // Review round 4: under the parser setting this converts to bits
        // `0x1`, which is what `f64::from_str` gives, so it QUALIFIES and
        // the two paths must answer the same bits. It fell back before the
        // setting, when the conversion was `0`.
        (
            "class_c",
            204,
            &["1", "\"5e-324\""],
            "",
            "sum_over_time",
            false,
        ),
        (
            "over_reject",
            205,
            &["1", "\"inf\""],
            "",
            "sum_over_time",
            true,
        ),
        // Review round 1: a digit with an exponent that overflows, and one
        // that underflows. Both pass the anchored prefix test, so the hole
        // was never in that test. Review round 4: under the parser setting
        // `"0e999999"` converts to `+0` rather than to a negative NaN, and
        // what refuses it now is the denotation clause — an over-rejection,
        // since our parser also gives `+0`.
        // Issue #507, criterion 11: on the group key read the text is decided
        // (the database's `+0` is our parser's), so it answers there.
        (
            "nan_exp",
            206,
            &["1", "\"0e999999\""],
            "",
            "sum_over_time",
            false,
        ),
        // Review round 4: the same move as `class_c` at a magnitude where
        // the old divergence was obvious — `0` against
        // `0x730d67819e8d2`. Under the setting both sides convert to
        // `0x730d67819e8d2`.
        (
            "under_exp",
            207,
            &["1", "\"9999999999999999e-324\""],
            "",
            "sum_over_time",
            false,
        ),
        // Review round 2: the same underflow written WITHOUT an exponent,
        // which is why the guard asks what the text DENOTES rather than how
        // it is spelled. Review round 4: under the parser setting this
        // converts to bits `0x1` on both sides, so it lowers.
        (
            "fixed_subnormal",
            209,
            &["1", fixed_subnormal.as_str()],
            "",
            "sum_over_time",
            false,
        ),
        // Review round 2: two accepted samples whose SUM overflows where
        // the reference's incremental mean does not — the database
        // answers `inf`, the evaluator `1e308`.
        (
            "avg_overflow",
            210,
            &["1e308", "1e308"],
            "",
            "avg_over_time",
            true,
        ),
        // Review round 1: the metadata carries the unwrapped name, so the
        // evaluator keys its series by the PARSED value under
        // `latency_extracted` — two series here, which no group key the
        // statement has can express.
        (
            "shadowed",
            208,
            &["7", "8"],
            r#"{"latency":"5"}"#,
            "sum_over_time",
            true,
        ),
    ];
    let mut rows: Vec<BucketedSeedRow> = Vec::new();
    for (name, fp, values, sm, _, _) in cases {
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, \
                     updated_ns) VALUES \
                     (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({t}))), {fp}, '{name}', \
                     '{{\"service_name\":\"{name}\"}}', 0)"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed log_streams");
        for (i, v) in values.iter().enumerate() {
            rows.push(BucketedSeedRow {
                service: name.to_string(),
                fingerprint: fp,
                timestamp_ns: t - 30_000_000_000 + i as i64,
                severity: 0,
                body: format!(r#"{{"latency":{v}}}"#),
                structured_metadata: sm.to_string(),
            });
        }
    }
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("insert the unwrapped fixture");
    // The standing rule: the input is asserted present before anything is
    // read from it.
    let seeded = admin
        .query_stream::<PartCountRow>(
            &format!("SELECT count() AS n FROM {db}.log_samples"),
            &QuerySettings::new(),
        )
        .await
        .expect("count the corpus");
    let seeded = {
        let mut s = seeded;
        let n = s.next().await.expect("one row").expect("decode").n;
        drop(s);
        n
    };
    assert_eq!(seeded, rows.len() as u64, "the corpus must be present");

    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t,
            step_ns: STEP as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db, 64 * 1024 * 1024));

    for (name, _fp, _values, _sm, op, expect_fallback) in cases {
        let lowered_q = format!(
            r#"{op}({{service_name="{name}"}} | json latency="latency" | unwrap latency [1m])"#
        );
        let client_q = format!(
            r#"{op}({{service_name="{name}"}} | json latency="latency" | unwrap latency | zzz="" [1m])"#
        );
        // Two paths, asserted to BE two paths.
        let shape = |q: &str| match plan(&parse(q).expect("parse"), &params, &plan_ctx(&db)) {
            Ok(Plan::Metric(mp)) => mp,
            other => panic!("{q}: {other:?}"),
        };
        assert!(
            shape(&lowered_q).client.is_none(),
            "{name}: the fixture query must plan the lowered read"
        );
        assert!(
            shape(&client_q).client.is_some(),
            "{name}: the control must stay on the client path"
        );

        let answer = |q: String| {
            let engine = &engine;
            let params = &params;
            async move {
                match engine
                    .query_explained(&parse(&q).expect("parse"), params)
                    .await
                {
                    Ok((result, _w, explain)) => {
                        let QueryResult::Matrix(series) = result else {
                            panic!("{q}: expected a matrix");
                        };
                        let last_read = explain
                            .stages
                            .iter()
                            .rfind(|s| s.name == "metric_read")
                            .map(|s| s.sql.clone())
                            .unwrap_or_default();
                        let points: Vec<(i64, u64)> = series
                            .into_iter()
                            .flat_map(|s| s.points)
                            .map(|(ts, v)| (ts, v.to_bits()))
                            .collect();
                        Ok((points, last_read))
                    }
                    // A value the EVALUATOR rejects is a 400 for the whole
                    // series, and the lowered statement cannot produce one:
                    // it would have summed a `0` and answered a number. So
                    // an error here is itself the proof that the query fell
                    // back — a stronger observable than the reported SQL.
                    Err(e) => Err(e.to_string()),
                }
            }
        };
        let lowered_res = answer(lowered_q.clone()).await;
        let client_res = answer(client_q).await;

        if let (Err(le), Err(ce)) = (&lowered_res, &client_res) {
            assert_eq!(le, ce, "{name}: both paths must refuse the same way");
            assert!(
                expect_fallback,
                "{name}: a refusal means the lowered path fell back, which this case did not \
                 expect"
            );
            continue;
        }
        let (lowered, last_read) = lowered_res.unwrap_or_else(|e| panic!("{name}: {e}"));
        let (client_ans, _) = client_res.unwrap_or_else(|e| panic!("{name} control: {e}"));

        let fell_back = last_read.contains("ORDER BY service ASC");
        assert_eq!(
            fell_back, expect_fallback,
            "{name}: expected fallback={expect_fallback}; the last reported metric_read was:\n\
             {last_read}"
        );

        assert_eq!(
            lowered, client_ans,
            "{name}: the two paths must answer the same bits"
        );
        if name == "shadowed" {
            // And the shape the collapse would have produced: TWO series,
            // because the parsed value survives as `latency_extracted` and
            // its two values are two series. A recomputed group answers
            // ONE series of 10 here, which is what an earlier round did.
            assert_eq!(
                lowered.len(),
                2,
                "{name}: the parsed collision label splits the series: {lowered:?}"
            );
            for (_, v) in &lowered {
                assert_eq!(
                    *v,
                    5.0f64.to_bits(),
                    "{name}: each series is the metadata's own value"
                );
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
}

/// **The spread reducers are NOT lowered, and the high-offset corpus is
/// why** (issue #507 W4, review rounds 1 and 3).
///
/// They were lowered through `stddevPopStable`/`varPopStable`, which fixed
/// the catastrophic cancellation the plain `stddevPop`/`varPop` have —
/// over `{1e16, 1e16+2, +4, +8, +16}` those answer `0` and `0` against the
/// evaluator's `31.2` and `5.585696017507576`. The stable variants
/// returned the evaluator's bits on that corpus, and **that was not
/// enough**: over 300,000 samples at `max_threads = 8` and
/// `max_block_size = 65536` the database's variance came back FINITE and
/// wrong — bits `9090485321501537692` against the client's
/// `9090485321501537553`, a difference of `1.0334767513920592e286`.
///
/// **A finite wrong answer is what the reader's guard cannot see.** The
/// non-finite fallback rests on overflow being absorbing, which holds for
/// a sum and not for a partial-moment algorithm. So the pair is withdrawn
/// at `sql::UnwrapReducer`, and this test is what fails if it comes back:
/// the reported statement must be the client scan, and the answer must be
/// the evaluator's.
#[tokio::test]
async fn the_spread_reducers_are_not_lowered_and_answer_the_evaluators_value() {
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
    let offsets = [0.0f64, 2.0, 4.0, 8.0, 16.0];
    let rows: Vec<BucketedSeedRow> = offsets
        .iter()
        .enumerate()
        .map(|(i, d)| BucketedSeedRow {
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
    let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db, 64 * 1024 * 1024));

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
                mp.client.is_some(),
                "{op} must stay client-side: the database's partial-moment aggregate can be \
                 finite and wrong"
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
            read.contains("ORDER BY service ASC"),
            "{op}: the statement must be the client sliding scan, got:\n{read}"
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

/// **The three boundary corpora of W4 §8** (issue #507), each chosen for
/// where the two summations can differ rather than for being realistic.
///
/// The condition number `κ = Σ|vᵢ| / |Σvᵢ|` is what decides whether the
/// summation-order bound is tight or vacuous: the absolute bound
/// `2(n−1)·u·Σ|vᵢ|` is the same either way, but as a FRACTION of the answer
/// it grows with `κ` without limit.
///
/// ```text
///  corpus       values                        κ      the two paths
///  exact        1, 2, 4, 8                    1      the same bits; no order can round
///  mild         55 × +1e9+δ, 5 × −1e9+δ       1.2    inside 2(n−1)·u·Σ|vᵢ|
///  cancelling   21 × (1e16, 1, −1e16)         huge   the same wrong answer, where the exact
///                                                    sum of the STORED samples is 21
/// ```
///
/// **The cancelling corpus is shared behaviour, not a divergence**, and it
/// is here to be recorded as such: the reference's own summation is a plain
/// accumulation with no compensation, so it loses the same digits. A
/// compensated sum would answer 21.
///
/// **Every value is exactly representable, and the residual survives the
/// INSERT.** An earlier fixture stored `-1e16 + 1.0`, which is already
/// `-1e16` as an `f64`, so its sum was exactly zero before any
/// accumulation and the agreement it asserted followed from the data. Nothing in this build promises
/// otherwise, and the ledger carries no row for it.
#[tokio::test]
async fn the_three_boundary_corpora_behave_as_their_condition_number_says() {
    skip_unless_live!();
    const STEP: i64 = 60_000_000_000;
    let admin = ChClient::new(test_config()).await.expect("connect admin");
    // Review round 4: this test had no teardown at all, so every run left
    // its database behind. The end of the test drops it; this sweeps what
    // earlier runs left, including the deliberate failures a break test
    // produces, which never reach the end.
    sweep_leftovers(
        &admin,
        &pulsus_testkit::test_db("pulsus_read_it_qlg_w4kappa_"),
    )
    .await;
    let db = pulsus_testkit::test_db(&format!(
        "pulsus_read_it_qlg_w4kappa_{}",
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

    // exactly representable | well conditioned | catastrophic cancellation
    let exact: Vec<f64> = vec![1.0, 2.0, 4.0, 8.0];
    // 55 positive and 5 negative of the same magnitude gives
    // `κ = 60/50 = 1.2`; the fractional parts are what force the rounding
    // the two orders can differ on.
    let mild: Vec<f64> = (0..60)
        .map(|i| {
            let m = 1e9 + (i as f64) / 8.0;
            if i < 55 { m } else { -m }
        })
        .collect();
    // **Every value is exactly representable and the residual is NOT
    // rounded away before insertion** (review round 2): `-1e16 + 1.0` is
    // already `-1e16` as an `f64`, so the earlier fixture stored exactly
    // opposing values and its agreement on zero followed from the data
    // rather than from the accumulation. Here each triple is
    // `1e16, 1, -1e16` — the `1` survives in the stored samples, and it is
    // the ACCUMULATION that loses it, which is the thing being measured.
    let cancelling: Vec<f64> = (0..21).flat_map(|_| [1e16f64, 1.0, -1e16f64]).collect();

    let mut out: Vec<(String, f64, u64, u64)> = Vec::new();
    for (name, fp, values) in [
        ("exact", 401u64, &exact),
        ("mild", 402, &mild),
        ("cancelling", 403, &cancelling),
    ] {
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, \
                     updated_ns) VALUES \
                     (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({t}))), {fp}, '{name}', \
                     '{{\"service_name\":\"{name}\"}}', 0)"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed log_streams");
        let rows: Vec<BucketedSeedRow> = values
            .iter()
            .enumerate()
            .map(|(i, v)| BucketedSeedRow {
                service: name.to_string(),
                fingerprint: fp,
                timestamp_ns: t - 30_000_000_000 + i as i64,
                severity: 0,
                body: format!(r#"{{"latency":{v:?}}}"#),
                structured_metadata: String::new(),
            })
            .collect();
        client
            .insert_block("log_samples", &rows)
            .await
            .expect("insert");
        let mut seeded = admin
            .query_stream::<PartCountRow>(
                &format!("SELECT count() AS n FROM {db}.log_samples WHERE service = '{name}'"),
                &QuerySettings::new(),
            )
            .await
            .expect("count");
        let n = seeded.next().await.expect("one row").expect("decode").n;
        drop(seeded);
        assert_eq!(n, values.len() as u64, "{name}: the corpus must be present");

        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: t,
                end_ns: t,
                step_ns: STEP as u64,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db, 64 * 1024 * 1024));
        let answer = |q: String| {
            let engine = &engine;
            let params = &params;
            async move {
                let (result, _w) = engine
                    .query(&parse(&q).expect("parse"), params)
                    .await
                    .unwrap_or_else(|e| panic!("{q}: {e}"));
                let QueryResult::Matrix(series) = result else {
                    panic!("{q}: expected a matrix");
                };
                series
                    .into_iter()
                    .flat_map(|s| s.points)
                    .map(|(_, v)| v)
                    .next()
                    .expect("one point")
            }
        };
        let lowered = answer(format!(
            r#"sum_over_time({{service_name="{name}"}} | json latency="latency" | unwrap latency [1m])"#
        ))
        .await;
        let client_ans = answer(format!(
            r#"sum_over_time({{service_name="{name}"}} | json latency="latency" | unwrap latency | zzz="" [1m])"#
        ))
        .await;
        let sum_abs: f64 = values.iter().map(|v| v.abs()).sum();
        let mut lr = 0.0f64;
        for v in values.iter() {
            lr += *v;
        }
        let kappa = sum_abs / lr.abs();
        eprintln!(
            "{name}: kappa={kappa:e} lowered={lowered:?} ({:#018x}) client={client_ans:?} \
             ({:#018x}) bound={:e}",
            lowered.to_bits(),
            client_ans.to_bits(),
            2.0 * ((values.len() as f64) - 1.0) * (f64::EPSILON / 2.0) * sum_abs
        );
        out.push((
            name.to_string(),
            kappa,
            lowered.to_bits(),
            client_ans.to_bits(),
        ));

        let bound = 2.0 * ((values.len() as f64) - 1.0) * (f64::EPSILON / 2.0) * sum_abs;
        match name {
            // κ = 1 and every partial sum exactly representable: no order
            // can round, so the two answers are the same bits.
            "exact" => {
                assert_eq!(kappa, 1.0, "the fixture must be perfectly conditioned");
                assert_eq!(
                    lowered.to_bits(),
                    client_ans.to_bits(),
                    "{name}: an exactly representable sum cannot depend on the order"
                );
                assert_eq!(lowered, 15.0);
            }
            // κ ≈ 1.2: the bound is meaningful and both answers are inside
            // it. Measured on this corpus they are also equal; the
            // assertion is the bound, because equality here is the
            // corpus's size and not a property of the two paths.
            "mild" => {
                assert!(
                    (kappa - 1.2).abs() < 0.01,
                    "{name}: the fixture's condition number is {kappa}"
                );
                assert!(
                    (lowered - client_ans).abs() <= bound,
                    "{name}: {lowered:?} vs {client_ans:?} exceeds {bound:e}"
                );
            }
            // The accumulated sum is zero, so the bound is vacuous as a
            // fraction of the answer. **Both sides return the same wrong
            // number** — the exact total of the stored values is 21, and
            // each triple `1e16`, `1`, `−1e16` loses its `1` to rounding
            // in either order. That is shared behaviour, not a divergence:
            // the reference's own summation is a plain accumulation with
            // no compensation. (This comment described the DISCARDED pair
            // fixture and its stated `32` until review round 3.)
            "cancelling" => {
                // **The exact sum is DERIVED FROM THE STORED VALUES, not
                // stated**, because an earlier fixture stated `32` for
                // samples whose exact sum was `0`: `-1e16 + 1.0` is
                // already `-1e16` as an `f64`, so the residual it claimed
                // had been rounded away before the insert. A compensated
                // (Neumaier) sum recovers the exact total that a plain
                // accumulation of the same values loses, and the two
                // together are what say the corpus is what it claims.
                let mut acc = 0.0f64;
                let mut comp = 0.0f64;
                for v in values.iter() {
                    let t = acc + v;
                    comp += if acc.abs() >= v.abs() {
                        (acc - t) + v
                    } else {
                        (v - t) + acc
                    };
                    acc = t;
                }
                let exact_sum = acc + comp;
                assert_eq!(
                    exact_sum, 21.0,
                    "{name}: the stored values must carry a residual a plain accumulation \
                     loses; their exact sum is {exact_sum}"
                );
                assert!(kappa > 1e14, "{name}: κ = {kappa}");
                assert_ne!(
                    lowered, exact_sum,
                    "{name}: the fixture must be one an accumulation loses, or it is not \
                     testing cancellation"
                );
                assert_eq!(
                    lowered.to_bits(),
                    client_ans.to_bits(),
                    "{name}: both paths lose the same digits — the reference's own summation \
                     is a plain accumulation with no compensation, so this is shared \
                     behaviour and not a divergence"
                );
            }
            other => panic!("unnamed corpus {other}"),
        }
    }
    assert_eq!(out.len(), 3, "all three corpora ran");

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
// Issue #507: the extracted-field group key read, end to end.
// ---------------------------------------------------------------------

/// One case of `tests/fixtures/group_key/cases.tsv`: one stream, its rows,
/// one query, the route the planner must choose, what `system.query_log`
/// must show, and the answer.
#[derive(Debug, Clone)]
struct GroupKeyCase {
    id: String,
    stream: std::collections::BTreeMap<String, String>,
    /// `(body, structured metadata)` per row.
    entries: Vec<(String, String)>,
    query: String,
    /// `key`: the plan is the group key read; `today`: it is not; `none`:
    /// the query does not plan.
    planned: String,
    /// `s1`: the key statement answered and nothing followed; `s1+raw`: it
    /// returned rows the fold sent to today's route; `throw`: it threw (395)
    /// and today's raw scan followed; `lane`: it threw, today's route
    /// refused, and the one read ran; `raw`: no key-route statement ran;
    /// `none`: no statement ran.
    observed: String,
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
            assert_eq!(f.len(), 7, "a fixture row has 7 fields: {l}");
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
                planned: f[4].to_string(),
                observed: f[5].to_string(),
                expected: f[6].to_string(),
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
                service: case.id.clone(),
                fingerprint: fp,
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
}

/// A case's query with its selector, and the request it runs as: a range
/// query at one grid point `t` whose step is the query's range, so the
/// planner can choose the group key read; `[2m]` keeps a one-minute step,
/// which is the point of that case. A log query reads the five minutes
/// before `t`.
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

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct GroupKeyStatementRow {
    query: String,
    exception_code: i32,
}

/// Which statement a logged query is, for the group key read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupKeyStatement {
    /// S1, the key statement.
    Key,
    /// L, the one read.
    Lane,
    /// Today's raw scan.
    Raw,
}

/// Every finished or failed statement over `db`'s samples, per fingerprint,
/// in the order they ran. `SYSTEM FLUSH LOGS` does not guarantee the rows
/// are there, so this waits until `want` fingerprints have one.
async fn group_key_statements(
    admin: &ChClient,
    db: &str,
    want: usize,
) -> std::collections::HashMap<u64, Vec<(GroupKeyStatement, i32)>> {
    let fp_re = regex::Regex::new(r"fingerprint IN \((\d+)\)").expect("regex");
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
            "SELECT query, exception_code FROM system.query_log \
             WHERE has(databases, '{db}') AND type != 'QueryStart' \
             AND query NOT LIKE '%system.query_log%' \
             ORDER BY event_time_microseconds ASC"
        );
        let mut stream = admin
            .query_stream::<GroupKeyStatementRow>(&sql, &QuerySettings::new())
            .await
            .expect("read system.query_log");
        out = std::collections::HashMap::new();
        while let Some(row) = stream.next().await {
            let row = row.expect("decode query_log row");
            let kind = if row
                .query
                .contains("throwIf(decided = 0 AND uw_missing = 0)")
            {
                GroupKeyStatement::Key
            } else if row
                .query
                .contains("WHERE NOT (decided = 0 AND uw_missing = 1)")
            {
                GroupKeyStatement::Lane
            } else if row
                .query
                .starts_with("SELECT fingerprint, timestamp_ns, body")
            {
                GroupKeyStatement::Raw
            } else {
                continue;
            };
            let Some(fp) = fp_re
                .captures(&row.query)
                .and_then(|c| c[1].parse::<u64>().ok())
            else {
                continue;
            };
            out.entry(fp)
                .or_insert_with(Vec::new)
                .push((kind, row.exception_code));
        }
        drop(stream);
        if out.len() >= want {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    out
}

/// What `system.query_log` shows for one case, in the fixture's words.
fn group_key_observed(statements: &[(GroupKeyStatement, i32)]) -> String {
    use GroupKeyStatement::{Key, Lane, Raw};
    match statements {
        [] => "none".to_string(),
        [(Key, 0)] => "s1".to_string(),
        [(Key, 0), (Raw, _)] => "s1+raw".to_string(),
        [(Key, 395), (Raw, _)] => "throw".to_string(),
        [(Key, 395), (Raw, _), (Lane, 0)] => "lane".to_string(),
        s if s.iter().all(|(k, _)| *k == Raw) => "raw".to_string(),
        s => format!("{s:?}"),
    }
}

/// Seeds `cases` into a fresh database, runs each through the engine and
/// `system.query_log`, and returns every way a case differs from its row.
async fn check_group_key_cases(stem: &str, fp_base: u64, cases: &[GroupKeyCase]) -> Vec<String> {
    let (admin, client, db) = group_key_db(stem).await;
    let t = ((now_ns() - 3_600_000_000_000) / 300_000_000_000) * 300_000_000_000;
    seed_group_key_cases(&admin, &client, &db, t, fp_base, cases).await;
    let engine = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 50 * 1024 * 1024 * 1024),
    );
    let mut wrong = Vec::new();
    for case in cases {
        let (query, params) = group_key_request(case, t);
        let planned = match parse(&query) {
            Err(_) => "none",
            Ok(expr) => match plan(&expr, &params, &plan_ctx(&db)) {
                Ok(Plan::Metric(mp)) if matches!(mp.value, sql::MetricValue::Unwrapped(_)) => "key",
                Ok(_) => "today",
                Err(_) => "none",
            },
        };
        if planned != case.planned {
            wrong.push(format!(
                "{}: planned {planned}, want {}",
                case.id, case.planned
            ));
        }
        let answer = match parse(&query) {
            Ok(expr) => group_key_answer(engine.query(&expr, &params).await),
            Err(_) => "400 parse".to_string(),
        };
        if answer != case.expected {
            wrong.push(format!(
                "{}: {query}\n    got  {answer}\n    want {}",
                case.id, case.expected
            ));
        }
    }
    let want = cases.iter().filter(|c| c.observed != "none").count();
    let statements = group_key_statements(&admin, &db, want).await;
    for (i, case) in cases.iter().enumerate() {
        let observed = group_key_observed(
            statements
                .get(&(fp_base + i as u64))
                .map(Vec::as_slice)
                .unwrap_or(&[]),
        );
        if observed != case.observed {
            wrong.push(format!(
                "{}: observed {observed}, want {}",
                case.id, case.observed
            ));
        }
    }
    drop_group_key_db(&admin, &db).await;
    wrong
}

/// **Criterion 3: the group key read answers every case of the plan's
/// tables as today's route does** (issue #507).
///
/// `tests/fixtures/group_key/cases.tsv` holds each case of the design's
/// tables — the JSON text rules, nesting, text after a value, empty and
/// absent values, lines that are not JSON, the q, g, e and k rows, the
/// collision rows, the refused chains, the reserved-name rows at the
/// reference's answers, the three rows of the unconvertible value under
/// `__preserve_error__`, and a filter naming the unwrapped label — with its
/// answer. For each, the test asserts:
///
/// ```text
/// planned   the plan is the group key read, or it is not
/// observed  system.query_log: S1 alone; S1 then today's raw scan (a fold
///           fallback); S1 throwing (395) then the raw scan; that and L;
///           or no key-route statement
/// answer    the fixture's answer, value bits included
/// ```
///
/// Each query runs as a range query at one grid point whose step is its
/// range, which is what lets the planner choose the group key read; the
/// design's tables give instant-query answers over the same rows.
#[tokio::test]
async fn the_group_key_read_agrees_with_the_client_path() {
    skip_unless_live!();
    let cases = load_group_key_cases("tests/fixtures/group_key/cases.tsv");
    let wrong = check_group_key_cases("gk", 507_000, &cases).await;
    assert!(
        wrong.is_empty(),
        "{} of {} cases differ:\n{}",
        wrong.len(),
        cases.len(),
        wrong.join("\n")
    );
}

/// **Criterion 22: the group key read refuses where the flattened-key budget
/// refuses** (issue #507, §3.1).
///
/// ```text
/// k01  quadratic_line(32_761, 2_979) with "latency":5, 65,548 B   undecided: S1 throws, today's route: 422 budget
/// k05  p 20, m 1,232, 13,590 B (the bound over, the charge under) undecided: S1 throws, today's route: {} 5
/// k02  p 32,761, m 1,022, 44,021 B (the parser accepts it)        S1 throws, today's route refuses on its
///                                                                  same-nanosecond staging, L: {} 5
/// ```
///
/// The rows are the fixture's k01, k02 and k05, under
/// `sum by (service_name) (sum_over_time({…} | json | unwrap latency [1m]))`.
#[tokio::test]
async fn the_group_key_read_refuses_where_the_key_budget_refuses() {
    skip_unless_live!();
    let cases: Vec<GroupKeyCase> = load_group_key_cases("tests/fixtures/group_key/cases.tsv")
        .into_iter()
        .filter(|c| ["k01", "k02", "k05"].contains(&c.id.as_str()))
        .collect();
    assert_eq!(cases.len(), 3, "the fixture holds k01, k02 and k05");
    let wrong = check_group_key_cases("gk_budget", 522_000, &cases).await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
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
        rows.push(BucketedSeedRow {
            service: service.to_string(),
            fingerprint: fp,
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
    let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db, 64 * 1024 * 1024));
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
            Route::Client,
            r#"{service_name="rn2"} 5"#,
        ),
        (
            r#"sum by (service_name) (sum_over_time({service_name="rn2"} | json | unwrap latency [5m]))"#,
            &range,
            Route::Client,
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
        // Revision 10's k3.00 and k4.00 on the client path: a preserved
        // failed conversion counts zero (R2); a parsed `__preserve_error__`
        // not required by the hints is skipped (R4), so k4's error fails the
        // query.
        (
            r#"sum by (service_name) (sum_over_time({service_name="k3"} | json | unwrap latency [5m]))"#,
            &instant,
            Route::Client,
            r#"{service_name="k3"} 0"#,
        ),
        (
            r#"sum by (service_name) (sum_over_time({service_name="k4"} | json | unwrap latency [5m]))"#,
            &instant,
            Route::Client,
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

/// A one-stream case built in code, for the tests that do not read the
/// fixture.
fn group_key_case(id: &str, stream: &[(&str, &str)], entries: &[(&str, &str)]) -> GroupKeyCase {
    GroupKeyCase {
        id: id.to_string(),
        stream: stream
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        entries: entries
            .iter()
            .map(|(b, sm)| (b.to_string(), sm.to_string()))
            .collect(),
        query: String::new(),
        planned: String::new(),
        observed: String::new(),
        expected: String::new(),
    }
}

/// **Criterion 44: the one read answers reserved-name rows under the query's
/// rules** (issue #507, revision 10).
///
/// k3's and k4's rows and queries, with today's retained-label ceiling
/// lowered so today's route refuses after S1 throws on the unconvertible
/// value:
///
/// ```text
/// S1 throws (395) ── today's route refuses (retained label bytes) ── L
/// k3  {"latency":"abc"}, metadata __preserve_error__="true"   L: preserved, counts 0   {service_name="k3"} 0
/// k4  {"latency":5}, then                                   L: decided, 5
///     {"latency":"abc","__preserve_error__":"true"}           L: the hints skip the parsed name,
///                                                                 so the error fails the query
/// ```
#[tokio::test]
async fn the_lane_answers_reserved_name_rows_under_the_rules() {
    skip_unless_live!();
    const FP_BASE: u64 = 544_000;
    let (admin, client, db) = group_key_db("gk_lane_rn").await;
    let cases = [
        group_key_case(
            "k3",
            &[],
            &[(r#"{"latency":"abc"}"#, r#"{"__preserve_error__":"true"}"#)],
        ),
        // k4's row alone fails today's route with its pipeline error before
        // any label set is retained, so today's route would answer rather than
        // refuse. A decided row one nanosecond earlier is retained first, which
        // trips the lowered ceiling and hands the query to L.
        group_key_case(
            "k4",
            &[],
            &[
                (r#"{"latency":5}"#, ""),
                (r#"{"latency":"abc","__preserve_error__":"true"}"#, ""),
            ],
        ),
    ];
    let t = ((now_ns() - 3_600_000_000_000) / 300_000_000_000) * 300_000_000_000;
    seed_group_key_cases(&admin, &client, &db, t, FP_BASE, &cases).await;
    let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db, 64 * 1024 * 1024))
        .with_key_route_test_hooks(pulsus_read::logql::exec::KeyRouteTestHooks {
            todays_route_group_bytes: Some(1),
            key_statement_test_knobs: None,
        });
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t,
            step_ns: 300_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    for (case, want) in [
        ("k3", r#"{service_name="k3"} 0"#),
        ("k4", "400 pipeline error: 'SampleExtractionErr'"),
    ] {
        let query = format!(
            r#"sum by (service_name) (sum_over_time({{service_name="{case}"}} | json | unwrap latency [5m]))"#
        );
        let expr = parse(&query).expect("parse");
        match plan(&expr, &params, &plan_ctx(&db)).expect("plan") {
            Plan::Metric(mp) => assert!(
                matches!(mp.value, sql::MetricValue::Unwrapped(_)),
                "{query}: the plan is the group key read"
            ),
            _ => panic!("{query}: a metric plan"),
        }
        let got = match engine.query(&expr, &params).await {
            Ok((QueryResult::Matrix(m), _)) => m
                .iter()
                .map(|s| {
                    let mut l = s.labels.clone();
                    l.sort();
                    format!(
                        "{{{}}} {}",
                        l.iter()
                            .map(|(k, v)| format!("{k}={v:?}"))
                            .collect::<Vec<_>>()
                            .join(", "),
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
                    "{query}: the hints skip a parsed __preserve_error__ the query does not require: {series}"
                );
                format!("400 pipeline error: '{error_type}'")
            }
            Err(e) => panic!("{query}: {e}"),
        };
        assert_eq!(got, want, "{query}");
    }
    let statements = group_key_statements(&admin, &db, cases.len()).await;
    for (i, case) in cases.iter().enumerate() {
        let seen = group_key_observed(
            statements
                .get(&(FP_BASE + i as u64))
                .map(Vec::as_slice)
                .unwrap_or(&[]),
        );
        assert_eq!(
            seen, "lane",
            "{}: S1 throws, today's route refuses, and L answers",
            case.id
        );
    }
    drop_group_key_db(&admin, &db).await;
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

/// The group key read's statements over `db` that finished or failed in
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
            let kind = if row
                .query
                .contains("throwIf(decided = 0 AND uw_missing = 0)")
            {
                GroupKeyStatement::Key
            } else if row
                .query
                .contains("WHERE NOT (decided = 0 AND uw_missing = 1)")
            {
                GroupKeyStatement::Lane
            } else if row
                .query
                .starts_with("SELECT fingerprint, timestamp_ns, body")
            {
                GroupKeyStatement::Raw
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

/// **Criterion 8: the key statement does not throw on rows outside its
/// filter** (issue #507, risk 2).
///
/// ```text
/// fp 1009  5,000 rows, one per millisecond from T; rows 1,000..=2,000 {"latency":1,"status":1}  (decided)
///          every other row {"latency":1,"status":1.5}                                        (undecided)
/// fp 1010  5,000 undecided rows from T + 1 s
/// S1 over fp 1009, (T + 999 ms, T + 2,000 ms]  ->  one group: n_value 1,001, no throw
/// ```
///
/// The undecided rows share parts and granules with the decided ones, so a
/// `throwIf` evaluated before the time and stream filters would throw. The
/// same holds through the engine on a one-second grid.
#[tokio::test]
async fn the_key_statement_does_not_throw_on_rows_outside_its_filter() {
    skip_unless_live!();
    let (admin, client, db) = group_key_db("gk_tt").await;
    let base = ((now_ns() - 3_600_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let month = format!("toStartOfMonth(fromUnixTimestamp64Nano(toInt64({base})))");
    for (fp, pod) in [(1009u64, "a"), (1010u64, "b")] {
        admin
            .execute(
                &format!(
                    "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) VALUES \
                     ({month}, {fp}, 'checkout', '{{\"pod\":\"{pod}\",\"service_name\":\"checkout\"}}', 0)"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("streams");
        admin
            .execute(
                &format!(
                    "INSERT INTO {db}.log_streams_idx (month, key, val, fingerprint) VALUES \
                     ({month}, 'service_name', 'checkout', {fp}), ({month}, 'pod', '{pod}', {fp})"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("idx");
    }
    for sql in [
        format!(
            "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) \
             SELECT 'checkout', 1009, {base} + number * 1000000, 0, \
             if(number BETWEEN 1000 AND 2000, '{{\"latency\":1,\"status\":1}}', '{{\"latency\":1,\"status\":1.5}}') \
             FROM numbers(5000)"
        ),
        format!(
            "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) \
             SELECT 'checkout', 1010, {base} + number * 1000000 + 1000000000, 0, \
             '{{\"latency\":1,\"status\":1.5}}' FROM numbers(5000)"
        ),
        format!("OPTIMIZE TABLE {db}.log_samples FINAL"),
    ] {
        admin
            .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .expect("seed the throw layout");
    }

    // The statement itself, rendered by the reader's builder.
    let query = r#"sum by (status) (sum_over_time({service_name="checkout", pod="a"} | json | unwrap latency [1s]))"#;
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: base + 2_000_000_000,
            end_ns: base + 2_000_000_000,
            step_ns: 1_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mp = match plan(&parse(query).expect("parse"), &params, &plan_ctx(&db)).expect("plan") {
        Plan::Metric(mp) => mp,
        _ => panic!("a metric plan"),
    };
    let sql::MetricValue::Unwrapped(u) = &mp.value else {
        panic!("{query}: the group key read");
    };
    let columns = sql::GroupKeyColumns {
        keys: u.keys.iter().map(|k| (k.clone(), Vec::new())).collect(),
        classes: Some(vec![vec![1009]]),
    };
    let s1 = sql::metric_range_unwrapped(
        "log_samples",
        u,
        &columns,
        &[literal("checkout")],
        &[1009],
        sql::BucketedScan {
            window: TimeWindow {
                start_ns: base + 999_000_000,
                end_ns: base + 2_000_000_000,
            },
            lower: sql::ScanLowerBound::Exclusive,
            lo_ns: base,
            step_ns: 60_000_000_000,
        },
        &[],
        sql::UndecidedRows::Throw,
        None,
    )
    .expect("render S1");
    let mut stream = client
        .query_stream::<pulsus_read::logql::rows::MetricRangeUnwrappedRow>(
            &s1.replace('?', "??"),
            &QuerySettings::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("S1 must not throw on rows outside its filter: {e}"));
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(
            row.unwrap_or_else(|e| panic!("S1 must not throw on rows outside its filter: {e}")),
        );
    }
    drop(stream);
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(
        (
            rows[0].n_value,
            rows[0].n_missing,
            rows[0].n_undecided,
            rows[0].v
        ),
        (1001, 0, 0, 1001.0),
        "{rows:?}"
    );

    // And through the engine: the window (T + 1 s, T + 2 s] holds rows
    // 1,001..=2,000.
    let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db, 64 * 1024 * 1024));
    let from = server_micros(&admin).await;
    let answer = group_key_answer(engine.query(&parse(query).expect("parse"), &params).await);
    let to = server_micros(&admin).await + 1;
    assert_eq!(answer, r#"{status="1"} 1000.0"#);
    let statements = group_key_statements_between(&admin, &db, from, to, 1).await;
    assert_eq!(
        statements,
        vec![(GroupKeyStatement::Key, 0)],
        "the key statement answered, and nothing followed it"
    );
    drop_group_key_db(&admin, &db).await;
}

/// **Criterion 9: the key route falls back on its own memory error** (issue
/// #507, code 241 end to end).
///
/// 20 streams `{service_name="checkout", pod="checkout-<fp>"}`, one hour,
/// 360,000 lines `{"latency":1,"k":"k<n % 5>"}` (five per second per
/// stream). `sum by (pod, k) (sum_over_time(… | json | unwrap latency [1s]))`
/// over seconds 1–3,600 groups 20 × 5 × 3,600 keys:
///
/// ```text
/// read ceiling 64 MiB   S1 fails (241) -> today's raw scan answers
/// default ceiling       S1 answers, no raw scan
/// both                  100 series, 359,980 points, each 1, bit-equal to the control
/// ```
///
/// The control `… | unwrap latency | latency >= 0 [1s]` is today's route.
#[tokio::test]
async fn the_key_route_falls_back_on_its_own_memory_error() {
    skip_unless_live!();
    let (admin, _client, db) = group_key_db("gk_mem").await;
    let base = ((now_ns() - 7_200_000_000_000) / 60_000_000_000) * 60_000_000_000;
    let month = format!("toStartOfMonth(fromUnixTimestamp64Nano(toInt64({base})))");
    for sql in [
        format!(
            "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) \
             SELECT {month}, 1000 + number, 'checkout', \
             concat('{{\"pod\":\"checkout-', toString(1000 + number), '\",\"service_name\":\"checkout\"}}'), 0 \
             FROM numbers(20)"
        ),
        format!(
            "INSERT INTO {db}.log_streams_idx (month, key, val, fingerprint) \
             SELECT {month}, k, v, 1000 + number FROM numbers(20) \
             ARRAY JOIN ['service_name', 'pod'] AS k, ['checkout', concat('checkout-', toString(1000 + number))] AS v"
        ),
        format!(
            "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) \
             SELECT 'checkout', 1000 + intDiv(number, 18000) % 20, \
             {base} + intDiv(number % 18000, 5) * 1000000000 + (number % 5) * 1000, 0, \
             concat('{{\"latency\":1,\"k\":\"k', toString(number % 5), '\"}}') FROM numbers(360000)"
        ),
    ] {
        admin
            .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
            .await
            .expect("seed the memory corpus");
    }
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: base + 1_000_000_000,
            end_ns: base + 3_600_000_000_000,
            step_ns: 1_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let query = r#"sum by (pod, k) (sum_over_time({service_name="checkout"} | json | unwrap latency [1s]))"#;
    let control = r#"sum by (pod, k) (sum_over_time({service_name="checkout"} | json | unwrap latency | latency >= 0 [1s]))"#;
    let matrix =
        |res: Result<(QueryResult, pulsus_read::Warnings), ReadError>, what: &str| match res {
            Ok((QueryResult::Matrix(m), _)) => {
                let mut out: MatrixBits = m
                    .into_iter()
                    .map(|s| {
                        let mut l = s.labels;
                        l.sort();
                        (
                            l,
                            s.points
                                .into_iter()
                                .map(|(t, v)| (t, v.to_bits()))
                                .collect(),
                        )
                    })
                    .collect();
                out.sort();
                out
            }
            other => panic!("{what}: {other:?}"),
        };
    let today = LogQlEngine::new(
        data_client(&db).await,
        engine_config(&db, 50 * 1024 * 1024 * 1024),
    );
    let want = matrix(
        today.query(&parse(control).expect("parse"), &params).await,
        "the control",
    );
    assert_eq!(want.len(), 100, "100 series");
    assert_eq!(
        want.iter().map(|(_, p)| p.len()).sum::<usize>(),
        359_980,
        "359,980 points"
    );
    assert!(
        want.iter()
            .all(|(_, p)| p.iter().all(|(_, v)| *v == 1f64.to_bits())),
        "every point is 1"
    );
    for (ceiling, statements_want) in [
        (
            64 * 1024 * 1024u64,
            vec![(GroupKeyStatement::Key, 241), (GroupKeyStatement::Raw, 0)],
        ),
        (8 * 1024 * 1024 * 1024u64, vec![(GroupKeyStatement::Key, 0)]),
    ] {
        let engine = LogQlEngine::new(
            data_client(&db).await,
            EngineConfig {
                read_max_memory_bytes: ceiling,
                ..engine_config(&db, 50 * 1024 * 1024 * 1024)
            },
        );
        let from = server_micros(&admin).await;
        let got = matrix(
            engine.query(&parse(query).expect("parse"), &params).await,
            &format!("the key route at a {ceiling}-byte ceiling"),
        );
        let to = server_micros(&admin).await + 1;
        assert_eq!(
            got, want,
            "ceiling {ceiling}: the answer is the control's, bit for bit"
        );
        let statements =
            group_key_statements_between(&admin, &db, from, to, statements_want.len()).await;
        assert_eq!(
            statements, statements_want,
            "ceiling {ceiling}: S1 failed with 241 and today's raw scan answered, or S1 answered alone"
        );
    }
    drop_group_key_db(&admin, &db).await;
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

/// A matrix answer: each series' sorted labels and its `(grid point, the
/// value's bits)` points.
type MatrixBits = Vec<(Vec<(String, String)>, Vec<(i64, u64)>)>;

/// The same, with the grid points dropped: one series' values alone.
type SeriesBits = Vec<(Vec<(String, String)>, Vec<u64>)>;

/// One agreement group: its name, its corpus, and up to two label pairs to
/// attach to the stream and to the metadata.
type AgreementGroup = (String, &'static str, Option<Pair>, Option<Pair>);

/// A label pair as the agreement groups carry it.
type Pair = (&'static str, &'static str);

/// A matrix answer, sorted, with each value's bits, or the error's text.
fn matrix_bits(
    res: Result<(QueryResult, pulsus_read::Warnings), ReadError>,
) -> Result<MatrixBits, String> {
    match res {
        Ok((QueryResult::Matrix(m), _)) => {
            let mut out: MatrixBits = m
                .into_iter()
                .map(|s| {
                    let mut l = s.labels;
                    l.sort();
                    (
                        l,
                        s.points
                            .into_iter()
                            .map(|(t, v)| (t, v.to_bits()))
                            .collect(),
                    )
                })
                .collect();
            out.sort();
            Ok(out)
        }
        Ok((other, _)) => Err(format!("not a matrix: {other:?}")),
        Err(e) => Err(format!("{e}")),
    }
}

/// **Criterion 19: the undecided rows come from one read** (issue #507, §4.1).
///
/// The realistic corpus with every request body's `status` a float, so the
/// key statement cannot decide one of them, under
/// `sum by (status) (sum_over_time({service_name="checkout"} | json | unwrap latency [1m]))`:
///
/// ```text
/// 3 minutes, today's retained-label ceiling lowered
///   S1 throws (395) -> today's raw scan refuses on the ceiling -> L, exactly one statement
///   the query answers; no GROUP BY statement follows the raw scan
/// 2 minutes
///   without the lowered ceiling: S1 throws, today's route answers
///   with it: L answers, and its answer equals today's, series by series
/// ```
///
/// L folds each row under the grouping the range step uses (a parent `sum`
/// by `status`), so 3 minutes of rows with a unique `request_id` each make
/// six series, not one per row.
///
/// **On CI's two-shard leg** (`PULSUS_TEST_CH_CLUSTER` names the cluster) the
/// same test runs against the `_dist` tables: the statements above are the
/// initiator's, which run its own shard's part, and the other shard's own
/// `system.query_log` holds exactly one part of L after its part of today's
/// raw scan.
#[tokio::test]
async fn the_undecided_rows_come_from_one_read() {
    skip_unless_live!();
    const MIN: i64 = 60_000_000_000;
    let cluster = std::env::var("PULSUS_TEST_CH_CLUSTER").ok();
    let suffix = if cluster.is_some() { "_dist" } else { "" };
    let (admin, db) = match &cluster {
        None => {
            let (admin, _client, db) = group_key_db("gk_oneread").await;
            (admin, db)
        }
        Some(name) => {
            let admin = ChClient::new(test_config()).await.expect("connect admin");
            let db = pulsus_testkit::test_db(&format!(
                "pulsus_read_it_qlg_gk_oneread_{}",
                uuid::Uuid::new_v4().simple()
            ));
            admin
                .execute(
                    &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{name}' SYNC"),
                    &QuerySettings::new(),
                    Idempotency::Idempotent,
                )
                .await
                .expect("drop");
            let ctx = RenderCtx {
                cluster: Some(name.clone()),
                ..test_ctx(&db)
            };
            run_init(&admin, &ctx).await.expect("run_init (clustered)");
            (admin, db)
        }
    };
    let start = ((now_ns() - 3_600_000_000_000) / MIN) * MIN;
    // Twelve minutes at the corpus's density; the queries read minutes 8–11.
    seed_realistic_corpus_into(&admin, &db, suffix, start, 400_000, true).await;
    let config = || {
        let mut c = engine_config(&db, 50 * 1024 * 1024 * 1024);
        if cluster.is_some() {
            c.streams_idx = "log_streams_idx_dist".to_string();
            c.streams = "log_streams_dist".to_string();
            c.samples = "log_samples_dist".to_string();
            c.rollup_table = "log_metrics_5s_dist".to_string();
            c.distributed = true;
        }
        c
    };
    let query = r#"sum by (status) (sum_over_time({service_name="checkout"} | json | unwrap latency [1m]))"#;
    let expr = parse(query).expect("parse");
    // `points` grid points ending at minute 11: 3 points read 3 minutes.
    let range = |points: i64| QueryParams {
        spec: QuerySpec::Range {
            start_ns: start + (12 - points) * MIN,
            end_ns: start + 11 * MIN,
            step_ns: MIN as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let lowered = LogQlEngine::new(data_client(&db).await, config()).with_key_route_test_hooks(
        pulsus_read::logql::exec::KeyRouteTestHooks {
            todays_route_group_bytes: Some(1),
            key_statement_test_knobs: None,
        },
    );
    let plain = LogQlEngine::new(data_client(&db).await, config());

    // 3 minutes: the query answers from L.
    let from = server_micros(&admin).await;
    let three = matrix_bits(lowered.query(&expr, &range(3)).await).expect("L answers at 3 minutes");
    let to = server_micros(&admin).await + 1;
    assert_eq!(
        three.len(),
        6,
        "one series per status: {:?}",
        three.iter().map(|s| &s.0).collect::<Vec<_>>()
    );
    assert!(three.iter().all(|(_, p)| p.len() == 3), "three points each");
    let statements = group_key_statements_between(&admin, &db, from, to, 3).await;
    assert_eq!(
        statements,
        vec![
            (GroupKeyStatement::Key, 395),
            (GroupKeyStatement::Raw, 0),
            (GroupKeyStatement::Lane, 0),
        ],
        "S1 throws, today's raw scan runs, and exactly one key-route statement follows it"
    );
    // Every statement of the query, in order: none after today's raw scan
    // holds a GROUP BY.
    let mut s = admin
        .query_stream::<TimedStatementRow>(
            &format!(
                "SELECT query, exception_code FROM system.query_log WHERE has(databases, '{db}') \
                 AND type != 'QueryStart' AND is_initial_query AND query NOT LIKE '%system.query_log%' \
                 AND query_start_time_microseconds >= fromUnixTimestamp64Micro(toInt64({from})) \
                 AND query_start_time_microseconds < fromUnixTimestamp64Micro(toInt64({to})) \
                 ORDER BY query_start_time_microseconds ASC"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("read the query's statements");
    let mut all = Vec::new();
    while let Some(row) = s.next().await {
        all.push(row.expect("decode").query);
    }
    drop(s);
    let raw_at = all
        .iter()
        .position(|q| q.starts_with("SELECT fingerprint, timestamp_ns, body"))
        .expect("today's raw scan ran");
    let grouped_after: Vec<&String> = all[raw_at + 1..]
        .iter()
        .filter(|q| q.contains("GROUP BY"))
        .collect();
    assert!(
        grouped_after.is_empty(),
        "no GROUP BY statement after today's raw scan: {grouped_after:?}"
    );
    if let Some(name) = &cluster {
        // The initiator runs its own shard's part inside the statements
        // above; the other shard logs its parts as its own queries.
        let per_shard = group_key_shard_statements(&admin, name, &db, from, to).await;
        assert_eq!(
            per_shard.len(),
            1,
            "the other shard ran its parts: {per_shard:?}"
        );
        for (host, kinds) in &per_shard {
            let raw_at = kinds
                .iter()
                .position(|k| *k == GroupKeyStatement::Raw)
                .unwrap_or_else(|| panic!("{host}: its part of today's raw scan: {kinds:?}"));
            assert_eq!(
                kinds[raw_at + 1..].to_vec(),
                vec![GroupKeyStatement::Lane],
                "{host}: exactly one part of L after its part of today's raw scan: {kinds:?}"
            );
        }
    }

    // 2 minutes: L's answer is today's route's, series by series.
    let from = server_micros(&admin).await;
    let today = matrix_bits(plain.query(&expr, &range(2)).await)
        .expect("today's route answers at 2 minutes");
    let mid = server_micros(&admin).await + 1;
    let lane = matrix_bits(lowered.query(&expr, &range(2)).await).expect("L answers at 2 minutes");
    let to = server_micros(&admin).await + 1;
    // Series by series and point by point, within the summation-order bound
    // `2(n−1)·u·Σ|vᵢ|` of each point's rows: L adds its rows in the order
    // they arrive and today's route in its own, and the owner accepted that
    // the order is not fixed (docs/query-to-sql.md §8).
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct PointRow {
        status: f64,
        bucket: i64,
        n: u64,
        sum_abs: f64,
    }
    let mut s = admin
        .query_stream::<PointRow>(
            &format!(
                "SELECT JSONExtractFloat(body, 'status') AS status, \
                 {start} + intDiv(timestamp_ns - {start} + {MIN} - 1, {MIN}) * {MIN} AS bucket, \
                 count() AS n, sum(abs(JSONExtractFloat(body, 'latency'))) AS sum_abs \
                 FROM {db}.log_samples{suffix} WHERE JSONHas(body, 'latency') \
                 AND timestamp_ns > {} AND timestamp_ns <= {} GROUP BY status, bucket",
                start + 8 * MIN,
                start + 11 * MIN
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("each point's rows");
    let mut points = std::collections::HashMap::new();
    while let Some(row) = s.next().await {
        let row = row.expect("decode");
        points.insert((row.status.to_string(), row.bucket), (row.n, row.sum_abs));
    }
    drop(s);
    assert_eq!(
        lane.iter()
            .map(|(l, p)| (l.clone(), p.iter().map(|(t, _)| *t).collect::<Vec<_>>()))
            .collect::<Vec<_>>(),
        today
            .iter()
            .map(|(l, p)| (l.clone(), p.iter().map(|(t, _)| *t).collect::<Vec<_>>()))
            .collect::<Vec<_>>(),
        "at 2 minutes L answers the series and points today's route answers"
    );
    for ((labels, lp), (_, tp)) in lane.iter().zip(&today) {
        let status = &labels
            .iter()
            .find(|(k, _)| k == "status")
            .expect("status")
            .1;
        for ((t, a), (_, b)) in lp.iter().zip(tp) {
            let (n, sum_abs) = points[&(status.clone(), *t)];
            let bound = 2.0 * ((n as f64) - 1.0) * (f64::EPSILON / 2.0) * sum_abs;
            let (a, b) = (f64::from_bits(*a), f64::from_bits(*b));
            assert!(
                (a - b).abs() <= bound,
                "status {status} at {t}: L {a:?}, today's route {b:?}, bound {bound:e} over {n} rows"
            );
        }
    }
    assert_eq!(
        group_key_statements_between(&admin, &db, from, mid, 2).await,
        vec![(GroupKeyStatement::Key, 395), (GroupKeyStatement::Raw, 0)],
        "without the lowered ceiling today's route answers"
    );
    assert_eq!(
        group_key_statements_between(&admin, &db, mid, to, 3).await,
        vec![
            (GroupKeyStatement::Key, 395),
            (GroupKeyStatement::Raw, 0),
            (GroupKeyStatement::Lane, 0),
        ],
        "with it, L answers"
    );
    match &cluster {
        None => drop_group_key_db(&admin, &db).await,
        Some(name) => admin
            .execute(
                &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{name}' SYNC"),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("drop the run database"),
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ShardStatementRow {
    host: String,
    query: String,
}

/// Each shard's own parts of the group key read's statements over `db` in
/// `[from, to)` initiator microseconds, per host, in the order they ran.
async fn group_key_shard_statements(
    admin: &ChClient,
    cluster: &str,
    db: &str,
    from: u64,
    to: u64,
) -> std::collections::BTreeMap<String, Vec<GroupKeyStatement>> {
    let mut out = std::collections::BTreeMap::new();
    for _ in 0..30 {
        admin
            .execute(
                &format!("SYSTEM FLUSH LOGS ON CLUSTER '{cluster}'"),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("flush logs on the cluster");
        let sql = format!(
            "SELECT hostName() AS host, query FROM clusterAllReplicas('{cluster}', system.query_log) \
             WHERE has(databases, '{db}') AND type != 'QueryStart' AND NOT is_initial_query \
             AND query_start_time_microseconds >= fromUnixTimestamp64Micro(toInt64({from})) \
             AND query_start_time_microseconds < fromUnixTimestamp64Micro(toInt64({to})) \
             ORDER BY host, query_start_time_microseconds ASC"
        );
        let mut stream = admin
            .query_stream::<ShardStatementRow>(&sql, &QuerySettings::new())
            .await
            .expect("read each shard's system.query_log");
        out.clear();
        while let Some(row) = stream.next().await {
            let row = row.expect("decode");
            // A shard's part is the initiator's statement rewritten over the
            // local table, so the markers are what survives the rewrite: the
            // key statement's throw, the per-row readers, or neither.
            if !row.query.contains("`log_samples`") {
                continue;
            }
            let kind = if row.query.contains("throwIf") {
                GroupKeyStatement::Key
            } else if row.query.contains("JSONExtractRaw") {
                GroupKeyStatement::Lane
            } else {
                GroupKeyStatement::Raw
            };
            out.entry(row.host).or_insert_with(Vec::new).push(kind);
        }
        drop(stream);
        if !out.is_empty() && out.values().all(|k| k.contains(&GroupKeyStatement::Lane)) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    out
}

/// **Criterion 31: a timeout of the key statement is the timeout response**
/// (issue #507, owner ruling: no race, no fallback).
///
/// The realistic corpus; `avg_over_time({service_name="checkout"} | json |
/// path != "" | unwrap latency [1m]) by (service, method, status, level,
/// msg)` over two minutes, with S1 slowed by a per-row delay past a
/// deadline:
///
/// ```text
/// S1 ── the timeout ── the timeout response (ChError::Timeout, the logs API's 504)
///                      today's route does not run
/// ```
///
/// **Both deadlines are exercised, because either can arrive first and they
/// are two code paths.** A run with the server's own `max_execution_time`
/// below the client's stream deadline ends in the server's code 159, which
/// `key_statement_refusal` maps to the timeout response; a run with the
/// client's deadline below the server's ends in `ChError::Timeout` from the
/// stream itself. With only the second, mapping 159 to today's route stays
/// green — measured: that break left this test passing until the first run
/// was added.
#[tokio::test]
async fn the_key_statement_timeout_is_the_timeout_response() {
    skip_unless_live!();
    const MIN: i64 = 60_000_000_000;
    let (admin, _client, db) = group_key_db("gk_timeout").await;
    let start = ((now_ns() - 3_600_000_000_000) / MIN) * MIN;
    seed_realistic_corpus(&admin, &db, start, 200_000, false).await;
    let query = r#"avg_over_time({service_name="checkout"} | json | path != "" | unwrap latency [1m]) by (service, method, status, level, msg)"#;
    let expr = parse(query).expect("parse");
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: start + 3 * MIN,
            end_ns: start + 4 * MIN,
            step_ns: MIN as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    match plan(&expr, &params, &plan_ctx(&db)).expect("plan") {
        Plan::Metric(mp) => assert!(
            matches!(mp.value, sql::MetricValue::Unwrapped(_)),
            "the plan is the group key read"
        ),
        _ => panic!("a metric plan"),
    }
    // (what, the client's stream deadline, the server's own limit on S1)
    for (what, client_timeout, server_limit_s) in [
        ("the server's own limit", Duration::from_secs(20), Some(0.3)),
        ("the client's stream deadline", Duration::from_secs(1), None),
    ] {
        let mut cfg = test_config();
        cfg.database = db.clone();
        cfg.query_timeout = client_timeout;
        let slow = LogQlEngine::new(
            ChClient::new(cfg).await.expect("connect"),
            engine_config(&db, 50 * 1024 * 1024 * 1024),
        )
        .with_key_route_test_hooks(pulsus_read::logql::exec::KeyRouteTestHooks {
            todays_route_group_bytes: None,
            key_statement_test_knobs: Some(sql::KeyStatementTestKnobs {
                row_delay_micros: 40,
                max_execution_s: server_limit_s,
            }),
        });
        let from = server_micros(&admin).await;
        let res = slow.query(&expr, &params).await;
        // S1 may still be running on the server after the client's deadline;
        // its row is logged when the server stops it.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let to = server_micros(&admin).await + 1;
        match &res {
            Err(ReadError::Clickhouse(pulsus_clickhouse::ChError::Timeout(_))) => {}
            other => panic!(
                "{what}: the key statement's timeout must be the timeout response, got {other:?}"
            ),
        }
        let statements = group_key_statements_between(&admin, &db, from, to, 1).await;
        assert!(
            matches!(statements.as_slice(), [(GroupKeyStatement::Key, code)] if *code != 0),
            "{what}: S1 ended with its timeout and no raw scan followed it: {statements:?}"
        );
        if server_limit_s.is_some() {
            assert!(
                matches!(statements.as_slice(), [(GroupKeyStatement::Key, 159)]),
                "the server's limit ends S1 with code 159, the code the reader maps: \
                 {statements:?}"
            );
        }
    }
    drop_group_key_db(&admin, &db).await;
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
/// The key route reproduces every rule that decides one line's outcome and
/// every ceiling on the answer, the scan or the statement; it does not
/// reproduce the ceilings on buffers only today's route allocates. Each row
/// runs the query on the key route and on today's route (the same query
/// with `| zzz=""` after the unwrap, which keeps every line and takes
/// today's route):
///
/// ```text
/// row  data                                                 today's route               key route
/// R1   6,213 {"latency":1} at one nanosecond                 {} 6213                     {} 6213
/// R1   6,214 at one nanosecond                              422 same-nanosecond staging {} 6214
/// R2   4,100,000 {"latency":1} within 50 s                  422 retained window points  {} 4100000
/// R3   3,400 lines, each its own request_id, one per second 422 result point-slots      one series, 3,400 points of 1
///      over an hour, step 1 s
/// R4   the realistic corpus, 3 minutes                       422 retained label bytes    three points (the plan's figures)
/// R5   501 values of r under by (r)                         422 series cap              422 series cap
/// R6   the realistic corpus under a scan budget below it    422 scan budget             422 scan budget, no raw scan
/// ```
///
/// R9 (the statement text cap) is the hermetic
/// `plan::tests::a_key_statement_over_the_text_cap_takes_todays_route`, and
/// R10 (the memory ceiling) is `the_key_route_falls_back_on_its_own_memory_error`.
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

    let engine = |budget: u64| {
        let db = db.clone();
        async move { LogQlEngine::new(data_client(&db).await, engine_config(&db, budget)) }
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
    let is_key = |q: &str, p: &QueryParams| {
        matches!(
            plan(&parse(q).expect("parse"), p, &plan_ctx(&db)).expect("plan"),
            Plan::Metric(mp) if matches!(mp.value, sql::MetricValue::Unwrapped(_))
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

    // R1 and R2: (key-route query, today's control, params, today's answer, key answer)
    for (svc, params, today_want, key_want) in [
        ("checkou1", range(60, 120, 60), "{} 6213.0", "{} 6213.0"),
        (
            "checkou2",
            range(60, 120, 60),
            "422 TsCollisionGroup",
            "{} 6214.0",
        ),
        (
            "r2",
            range(60, 120, 60),
            "422 MetricRetention",
            "{} 4100000.0",
        ),
    ] {
        let key_q = sum_by(svc, "", "1m");
        let today_q = sum_by(svc, r#" | zzz="""#, "1m");
        assert!(is_key(&key_q, &params), "{key_q}: the key route");
        assert!(!is_key(&today_q, &params), "{today_q}: today's route");
        assert_eq!(
            short(wide.query(&parse(&today_q).expect("parse"), &params).await),
            today_want,
            "{svc}: today's route"
        );
        assert_eq!(
            short(wide.query(&parse(&key_q).expect("parse"), &params).await),
            key_want,
            "{svc}: the key route"
        );
    }

    // R3: one series of 3,400 points, each 1, where today's route runs out of
    // result point-slots.
    {
        let params = range(1, 3600, 1);
        let key_q = sum_by("r3", "", "1s");
        let today_q = sum_by("r3", r#" | zzz="""#, "1s");
        assert!(is_key(&key_q, &params), "R3: the key route");
        assert_eq!(
            short(wide.query(&parse(&today_q).expect("parse"), &params).await),
            "422 MetricResultPoints",
            "R3: today's route"
        );
        match wide.query(&parse(&key_q).expect("parse"), &params).await {
            Ok((QueryResult::Matrix(m), _)) => {
                assert_eq!(m.len(), 1, "R3: one series");
                assert_eq!(m[0].points.len(), 3400, "R3: 3,400 points");
                assert!(m[0].points.iter().all(|(_, v)| *v == 1.0), "R3: each 1");
            }
            other => panic!("R3: the key route answers: {other:?}"),
        }
    }

    // R4: the realistic corpus over 3 minutes.
    {
        let params = range(60, 180, 60);
        let key_q = sum_by("checkout", "", "1m");
        let today_q = sum_by("checkout", r#" | zzz="""#, "1m");
        assert!(is_key(&key_q, &params), "R4: the key route");
        assert_eq!(
            short(wide.query(&parse(&today_q).expect("parse"), &params).await),
            "422 MetricGroupLabelBytes",
            "R4: today's route"
        );
        match wide.query(&parse(&key_q).expect("parse"), &params).await {
            Ok((QueryResult::Matrix(m), _)) => {
                assert_eq!(m.len(), 1, "R4: one series");
                let got: Vec<f64> = m[0].points.iter().map(|(_, v)| *v).collect();
                let want = [14_999_019.46, 14_998_940.27, 15_001_940.27];
                assert_eq!(got.len(), 3, "R4: three points: {got:?}");
                for (g, w) in got.iter().zip(want) {
                    assert!(
                        (g - w).abs() < 0.005,
                        "R4: {got:?} against the plan's {want:?}"
                    );
                }
            }
            other => panic!("R4: the key route answers: {other:?}"),
        }
    }

    // R5: the series cap on both routes.
    {
        let params = range(60, 60, 60);
        let key_q = r#"avg_over_time({service_name="r5"} | json | unwrap latency [1m]) by (r)"#;
        let today_q =
            r#"avg_over_time({service_name="r5"} | json | unwrap latency | zzz="" [1m]) by (r)"#;
        assert!(is_key(key_q, &params), "R5: the key route");
        assert_eq!(
            short(wide.query(&parse(today_q).expect("parse"), &params).await),
            "422 MetricSeries",
            "R5: today's route"
        );
        assert_eq!(
            short(wide.query(&parse(key_q).expect("parse"), &params).await),
            "422 MetricSeries",
            "R5: the key route"
        );
    }

    // R6: the scan budget on both routes, and no raw scan after the key
    // statement's 307.
    {
        let params = range(60, 180, 60);
        let tight = engine(1_000_000).await;
        let key_q =
            r#"avg_over_time({service_name="checkout"} | json | unwrap latency [1m]) by (service)"#;
        let today_q = r#"avg_over_time({service_name="checkout"} | json | unwrap latency | zzz="" [1m]) by (service)"#;
        assert!(is_key(key_q, &params), "R6: the key route");
        assert_eq!(
            short(tight.query(&parse(today_q).expect("parse"), &params).await),
            "422 ScanBudgetBytes",
            "R6: today's route"
        );
        let from = server_micros(&admin).await;
        assert_eq!(
            short(tight.query(&parse(key_q).expect("parse"), &params).await),
            "422 ScanBudgetBytes",
            "R6: the key route"
        );
        let to = server_micros(&admin).await + 1;
        assert_eq!(
            group_key_statements_between(&admin, &db, from, to, 1).await,
            vec![(GroupKeyStatement::Key, 307)],
            "R6: the key statement meets the budget and no second read runs"
        );
    }
    drop_group_key_db(&admin, &db).await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ProjectedMetadataRow {
    pairs: Vec<(String, String)>,
}

/// **Criterion 5: the key route's reduced input answers as the full input**
/// (issue #507, §6.3).
///
/// The fold never sees a row as it was stored. It sees:
///
/// ```text
/// metadata      only the entries the plan names, projected by the key statement's own SQL
/// bare keys     a key label's body value removed where a stream label or metadata entry holds the name
/// stream labels only the class names
/// ```
///
/// Each case is stored twice, as it is and reduced, and today's route (an
/// instant query) answers both; the answers must be equal. 7 queries × 14
/// metadata variants over the same four bodies on a stream
/// `{pod="p1", zone="z1"}`: 98 cases, of which 20 carry a name that sends the
/// key route to today's route (the unwrapped label, `__error__` or
/// `__error_details__`), so they have no reduced input and are not compared.
/// The names come from the plan the key route runs.
#[tokio::test]
async fn the_reduced_input_answers_as_the_full_input() {
    skip_unless_live!();
    let (admin, client, db) = group_key_db("gk_reduced").await;
    let queries = [
        (
            "qa",
            "sum by (code) (sum_over_time(SEL | json | unwrap latency [1m]))",
        ),
        (
            "qb",
            "avg_over_time(SEL | json | unwrap latency [1m]) by (code)",
        ),
        (
            "qc",
            r#"sum by (code) (sum_over_time(SEL | json | a="x" | unwrap latency [1m]))"#,
        ),
        (
            "qd",
            "sum by (pod) (sum_over_time(SEL | json | unwrap latency [1m]))",
        ),
        ("qe", "sum(sum_over_time(SEL | json | unwrap latency [1m]))"),
        (
            "qf",
            r#"avg_over_time(SEL | json c="code", lat="latency" | unwrap lat [1m]) by (c)"#,
        ),
        (
            "qg",
            r#"avg_over_time(SEL | json c="code", lat="latency" | unwrap lat [1m]) without (pod)"#,
        ),
    ];
    let bodies = [
        r#"{"latency":1,"code":"a","a":"x"}"#,
        r#"{"latency":2,"code":"b","a":"x","pod":"body-pod"}"#,
        r#"{"latency":4,"code":"a","a":"y","c":"bc"}"#,
        r#"{"latency":8}"#,
    ];
    let metadata: [&[&str]; 14] = [
        &[""],
        &[r#"{"code":"m"}"#],
        &[r#"{"a":"x"}"#],
        &[r#"{"a":"q"}"#],
        &[r#"{"pod":"sm-pod"}"#],
        &[r#"{"trace_id":"t1"}"#],
        &[r#"{"zone":"sz"}"#],
        &[r#"{"code":"m","trace_id":"t"}"#],
        &[r#"{"latency":"9"}"#],
        &[r#"{"__error__":"boom"}"#],
        &[r#"{"__error_details__":"d"}"#],
        &[r#"{"c":"k"}"#],
        &[r#"{"lat":"7"}"#],
        &[
            r#"{"trace_id":"t1"}"#,
            r#"{"trace_id":"t2"}"#,
            r#"{"code":"m"}"#,
            "",
        ],
    ];
    let stream = [("pod", "p1"), ("zone", "z1")];
    let t = ((now_ns() - 3_600_000_000_000) / 300_000_000_000) * 300_000_000_000;
    let range = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let mut cases: Vec<GroupKeyCase> = Vec::new();
    let mut compared: Vec<(String, String)> = Vec::new();
    let mut to_today = 0;
    for (qn, q) in queries {
        let planned = q.replace("SEL", r#"{service_name="x"}"#);
        let u = match plan(&parse(&planned).expect("parse"), &range, &plan_ctx(&db)).expect("plan")
        {
            Plan::Metric(mp) => match mp.value {
                sql::MetricValue::Unwrapped(u) => *u,
                other => panic!("{q}: the group key read, got {other:?}"),
            },
            _ => panic!("{q}: a metric plan"),
        };
        for (si, sms) in metadata.iter().enumerate() {
            let id = format!("sm_{qn}_{si:02}");
            let keys_of = |sm: &str| -> Vec<String> {
                if sm.is_empty() {
                    return Vec::new();
                }
                let m: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(sm).expect("metadata json");
                m.keys().cloned().collect()
            };
            // A row naming the unwrapped label or a presence name sends the
            // key route to today's route: it has no reduced input.
            let presence: Vec<String> = match &u.metadata {
                sql::MetadataSent::Projected { presence, .. } => presence.clone(),
                sql::MetadataSent::Text => vec![u.label.clone(), "__error__".to_string()],
            };
            if sms.iter().any(|sm| {
                keys_of(sm)
                    .iter()
                    .any(|k| presence.contains(k) || *k == u.label)
            }) {
                to_today += 1;
                continue;
            }
            let mut full = group_key_case(&format!("{id}_f"), &stream, &[]);
            let mut reduced_stream: Vec<(&str, &str)> = match &u.classes {
                sql::ClassNames::Projected(names) => stream
                    .iter()
                    .copied()
                    .filter(|(k, _)| names.iter().any(|n| n == k))
                    .collect(),
                _ => stream.to_vec(),
            };
            reduced_stream.sort();
            let mut reduced = group_key_case(&format!("{id}_r"), &reduced_stream, &[]);
            for (j, body) in bodies.iter().enumerate() {
                let sm = sms[j % sms.len()];
                full.entries.push((body.to_string(), sm.to_string()));
                // The bare form's blank rule.
                let rbody = if u.form == sql::UnwrapForm::Bare {
                    let doc: serde_json::Map<String, serde_json::Value> =
                        serde_json::from_str(body).expect("body json");
                    let sm_keys = keys_of(sm);
                    let kept: serde_json::Map<String, serde_json::Value> = doc
                        .into_iter()
                        .filter(|(k, _)| {
                            let is_key = u.keys.iter().any(|key| key.source == *k);
                            let held = stream.iter().any(|(s, _)| s == k) || sm_keys.contains(k);
                            !(is_key && held)
                        })
                        .collect();
                    serde_json::to_string(&kept).expect("json")
                } else {
                    body.to_string()
                };
                // The metadata the key statement sends, computed by its SQL.
                let rsm = match &u.metadata {
                    sql::MetadataSent::Text => sm.to_string(),
                    sql::MetadataSent::Projected { values, presence } => {
                        let expr = pulsus_read::logql::predicate::metadata_names_projection(
                            values, presence,
                        );
                        let mut s = admin
                            .query_stream::<ProjectedMetadataRow>(
                                &format!(
                                    "SELECT {} AS pairs FROM (SELECT {} AS structured_metadata)",
                                    expr.as_sql(),
                                    literal(sm).as_sql()
                                ),
                                &QuerySettings::new(),
                            )
                            .await
                            .expect("project the metadata");
                        let pairs = s.next().await.expect("one row").expect("decode").pairs;
                        drop(s);
                        if pairs.is_empty() {
                            String::new()
                        } else {
                            let m: serde_json::Map<String, serde_json::Value> = pairs
                                .into_iter()
                                .map(|(k, v)| (k, serde_json::Value::String(v)))
                                .collect();
                            serde_json::to_string(&m).expect("json")
                        }
                    }
                };
                reduced.entries.push((rbody, rsm));
            }
            full.query = q.to_string();
            reduced.query = q.to_string();
            compared.push((full.id.clone(), reduced.id.clone()));
            cases.push(full);
            cases.push(reduced);
        }
    }
    assert_eq!(
        (compared.len(), to_today),
        (78, 20),
        "78 compared, 20 on today's route"
    );
    seed_group_key_cases(&admin, &client, &db, t, 588_000, &cases).await;
    let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db, 64 * 1024 * 1024));
    let instant = QueryParams {
        spec: QuerySpec::Instant { at_ns: t },
        limit: 100,
        direction: Direction::Backward,
    };
    let mut answers = std::collections::HashMap::new();
    for case in &cases {
        let query = case
            .query
            .replace("SEL", &format!("{{service_name={:?}}}", case.id));
        answers.insert(
            case.id.clone(),
            group_key_answer(engine.query(&parse(&query).expect("parse"), &instant).await),
        );
    }
    let differ: Vec<String> = compared
        .iter()
        .filter(|(f, r)| answers[f] != answers[r])
        .map(|(f, r)| format!("{f}: full {}, reduced {}", answers[f], answers[r]))
        .collect();
    assert!(
        differ.is_empty(),
        "{} differ:\n{}",
        differ.len(),
        differ.join("\n")
    );
    drop_group_key_db(&admin, &db).await;
}

/// `quad(p, m)` of the key-budget corpus: `lead`, one key of `p` `a`s, and
/// an object of `m` members under it.
fn quadratic_body(p: usize, m: usize, lead: &str) -> String {
    let members: Vec<String> = (0..m).map(|i| format!("\"k{i:05}\":0")).collect();
    format!("{lead}\"{}\":{{{}}}}}", "a".repeat(p), members.join(","))
}

/// **Criterion 4: the group key read agrees with our parser on every fixed
/// body** (issue #507, §6).
///
/// Each body is its own stream. For every corpus, stream-label or metadata
/// variant, and form, the one read (L, the key statement's per-row columns)
/// returns the database's verdict per body, and the reader's fold answers
/// each returned row. The body's own answer is the fold of the same body as
/// an undecided row: our parser over the line, under the query's rules.
///
/// ```text
/// decided   the fold of L's row answers what the body answers (labels, value bits after + 0.0),
///           or sends the query to today's route
/// missing   no L row; the body contributes nothing
/// undecided our parser reads the body: nothing to compare
/// ```
///
/// Corpora: H1–H5 and G (1,026 bodies), H5B less its two flat bodies (6,
/// generated here), and H6 (26 bodies, each about a reserved name). Variants:
/// none; a stream label or one metadata entry of `__error__="s"`,
/// `__preserve_error__="true"`, `__variant__="v"`, `__error_details__="d"`
/// (H1–H5 + G and H6). Twenty form-rule cells each. The decided / missing /
/// undecided counts of H1–H5 + G and of H5B are the design's.
#[tokio::test]
async fn the_group_key_read_agrees_on_every_fixed_body() {
    skip_unless_live!();
    use pulsus_read::logql::group_key_probe::{GroupKeyProbe, ProbeOutcome};
    use pulsus_read::logql::rows::{StreamMetaRow, UnwrappedLaneRow};
    const MIN: i64 = 60_000_000_000;
    let (admin, client, db) = group_key_db("gk_agree").await;
    let t = ((now_ns() - 3_600_000_000_000) / MIN) * MIN;

    let mut corpora: std::collections::BTreeMap<&str, Vec<(String, String)>> = Default::default();
    for line in std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/group_key/agreement_bodies.tsv"),
    )
    .expect("read the bodies")
    .lines()
    .filter(|l| !l.starts_with('#'))
    {
        let f: Vec<&str> = line.split('\t').collect();
        let corpus = if f[0] == "NAMED6" { "NAMED6" } else { "H6" };
        corpora
            .entry(corpus)
            .or_default()
            .push((f[1].to_string(), unhex_utf8(f[2])));
    }
    corpora.insert(
        "H5B6",
        vec![
            (
                "H5B-budget-quadratic-32761-2979".into(),
                quadratic_body(32_761, 2_979, "{\"latency\":5,"),
            ),
            (
                "H5B-budget-quadratic-no-latency-32761-2979".into(),
                quadratic_body(32_761, 2_979, "{"),
            ),
            (
                "H5B-budget-parser-accepts-32761-1022".into(),
                quadratic_body(32_761, 1_022, "{\"latency\":5,"),
            ),
            (
                "H5B-budget-parser-refuses-32761-1023".into(),
                quadratic_body(32_761, 1_023, "{\"latency\":5,"),
            ),
            (
                "H5B-budget-bound-within-20-1231".into(),
                quadratic_body(20, 1_231, "{\"latency\":5,"),
            ),
            (
                "H5B-budget-bound-over-20-1232".into(),
                quadratic_body(20, 1_232, "{\"latency\":5,"),
            ),
        ],
    );
    assert_eq!(
        corpora
            .iter()
            .map(|(k, v)| (*k, v.len()))
            .collect::<Vec<_>>(),
        vec![("H5B6", 6), ("H6", 26), ("NAMED6", 1026)]
    );

    // (group, corpus, extra stream label, extra metadata)
    let kvs = [
        ("__error__", "s"),
        ("__preserve_error__", "true"),
        ("__variant__", "v"),
        ("__error_details__", "d"),
    ];
    let mut groups: Vec<AgreementGroup> = Vec::new();
    for corpus in ["NAMED6", "H5B6", "H6"] {
        groups.push((format!("ag_{corpus}_none"), corpus, None, None));
        if corpus != "H5B6" {
            for (i, kv) in kvs.iter().enumerate() {
                groups.push((format!("ag_{corpus}_s{i}"), corpus, Some(*kv), None));
                groups.push((format!("ag_{corpus}_m{i}"), corpus, None, Some(*kv)));
            }
        }
    }
    let month = format!("toStartOfMonth(fromUnixTimestamp64Nano(toInt64({t})))");
    let mut fp = 900_000u64;
    let mut group_meta: Vec<std::collections::HashMap<u64, StreamMetaRow>> = Vec::new();
    let mut group_bodies: Vec<std::collections::HashMap<u64, (String, String, String)>> =
        Vec::new();
    for (name, corpus, stream, sm) in &groups {
        let mut labels = std::collections::BTreeMap::new();
        labels.insert("service_name".to_string(), name.clone());
        if let Some((k, v)) = stream {
            labels.insert(k.to_string(), v.to_string());
        }
        let labels_json = serde_json::to_string(&labels).expect("json");
        let sm_text = sm
            .map(|(k, v)| format!("{{\"{k}\":\"{v}\"}}"))
            .unwrap_or_default();
        let mut meta = std::collections::HashMap::new();
        let mut bodies = std::collections::HashMap::new();
        let mut streams = Vec::new();
        let mut idx = Vec::new();
        let mut rows = Vec::new();
        for (case, body) in &corpora[corpus] {
            fp += 1;
            meta.insert(
                fp,
                StreamMetaRow {
                    fingerprint: fp,
                    service: name.clone(),
                    labels: labels_json.clone(),
                },
            );
            bodies.insert(fp, (case.clone(), body.clone(), sm_text.clone()));
            streams.push(format!(
                "({month}, {fp}, '{name}', {}, 0)",
                literal(&labels_json).as_sql()
            ));
            idx.push(format!("({month}, 'service_name', '{name}', {fp})"));
            rows.push(BucketedSeedRow {
                service: name.clone(),
                fingerprint: fp,
                timestamp_ns: t - 30_000_000_000,
                severity: 0,
                body: body.clone(),
                structured_metadata: sm_text.clone(),
            });
        }
        for chunk in streams.chunks(2000) {
            admin
                .execute(
                    &format!(
                        "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) VALUES {}",
                        chunk.join(", ")
                    ),
                    &QuerySettings::new(),
                    Idempotency::Idempotent,
                )
                .await
                .expect("streams");
        }
        for chunk in idx.chunks(2000) {
            admin
                .execute(
                    &format!(
                        "INSERT INTO {db}.log_streams_idx (month, key, val, fingerprint) VALUES {}",
                        chunk.join(", ")
                    ),
                    &QuerySettings::new(),
                    Idempotency::Idempotent,
                )
                .await
                .expect("idx");
        }
        client
            .insert_block("log_samples", &rows)
            .await
            .expect("rows");
        group_meta.push(meta);
        group_bodies.push(bodies);
    }

    // The twenty form-rule cells, as the queries the planner derives their
    // rules from.
    let targeted = r#"json c="code", lat="latency", m="missing""#;
    let cells: [(&str, String); 20] = [
        (
            "B",
            "sum(sum_over_time(SEL | json | unwrap latency [1m]))".into(),
        ),
        (
            "BK",
            "sum by (a, a_b, code) (sum_over_time(SEL | json | unwrap latency [1m]))".into(),
        ),
        (
            "BU",
            "sum by (_) (sum_over_time(SEL | json | unwrap latency [1m]))".into(),
        ),
        (
            "BF",
            r#"sum(sum_over_time(SEL | json | a="x" | unwrap latency [1m]))"#.into(),
        ),
        (
            "BN",
            "sum(sum_over_time(SEL | json | code > 100 | unwrap latency [1m]))".into(),
        ),
        (
            "T plain",
            r#"sum_over_time(SEL | json latency="latency" | unwrap latency [1m])"#.into(),
        ),
        (
            "T parent",
            r#"sum(sum_over_time(SEL | json latency="latency" | unwrap latency [1m]))"#.into(),
        ),
        (
            "R plain",
            r#"sum_over_time(SEL | json lat="latency" | unwrap lat [1m])"#.into(),
        ),
        (
            "R parent",
            r#"sum(sum_over_time(SEL | json lat="latency" | unwrap lat [1m]))"#.into(),
        ),
        (
            "M plain",
            format!("sum_over_time(SEL | {targeted} | unwrap lat [1m])"),
        ),
        (
            "M parent",
            format!("sum(sum_over_time(SEL | {targeted} | unwrap lat [1m]))"),
        ),
        (
            "P plain",
            r#"sum_over_time(SEL | json lat="req.latency" | unwrap lat [1m])"#.into(),
        ),
        (
            "P parent",
            r#"sum(sum_over_time(SEL | json lat="req.latency" | unwrap lat [1m]))"#.into(),
        ),
        (
            "GBY1",
            "avg_over_time(SEL | json | unwrap latency [1m]) by (a)".into(),
        ),
        (
            "GBY2",
            "avg_over_time(SEL | json | unwrap latency [1m]) by (a_b, code)".into(),
        ),
        (
            "GBYS",
            "avg_over_time(SEL | json | unwrap latency [1m]) by (service_name)".into(),
        ),
        (
            "GBYE",
            "avg_over_time(SEL | json | unwrap latency [1m]) by ()".into(),
        ),
        (
            "GBU",
            "avg_over_time(SEL | json | unwrap latency [1m]) by (_)".into(),
        ),
        (
            "GTBY",
            format!("avg_over_time(SEL | {targeted} | unwrap lat [1m]) by (c)"),
        ),
        (
            "GTWO",
            format!("avg_over_time(SEL | {targeted} | unwrap lat [1m]) without (m)"),
        ),
    ];
    // The design's decided / missing / undecided counts (§6.2; H5B from
    // revision 8, less its two flat bodies).
    let design_counts = |corpus: &str, form: &str| -> Option<(u64, u64, u64)> {
        let form = form.split(' ').next().unwrap_or(form);
        let named = [
            ("B", (819, 6, 201)),
            ("BK", (594, 6, 426)),
            ("BU", (0, 6, 1020)),
            ("BF", (738, 6, 282)),
            ("BN", (788, 6, 232)),
            ("T", (909, 12, 105)),
            ("R", (909, 12, 105)),
            ("M", (879, 12, 135)),
            ("P", (9, 943, 74)),
            ("GBY1", (738, 6, 282)),
            ("GBY2", (654, 6, 366)),
            ("GBYS", (819, 6, 201)),
            ("GBYE", (819, 6, 201)),
            ("GBU", (0, 6, 1020)),
            ("GTBY", (879, 12, 135)),
            ("GTWO", (879, 12, 135)),
        ];
        let h5b = [
            ("B", (1, 0, 5)),
            ("BK", (1, 0, 5)),
            ("BU", (0, 0, 6)),
            ("BF", (1, 0, 5)),
            ("BN", (1, 0, 5)),
            ("T", (5, 1, 0)),
            ("R", (5, 1, 0)),
            ("M", (5, 1, 0)),
            ("P", (0, 6, 0)),
            ("GBY1", (1, 0, 5)),
            ("GBY2", (1, 0, 5)),
            ("GBYS", (1, 0, 5)),
            ("GBYE", (1, 0, 5)),
            ("GBU", (0, 0, 6)),
            ("GTBY", (5, 1, 0)),
            ("GTWO", (5, 1, 0)),
        ];
        let table: &[(&str, (u64, u64, u64))] = match corpus {
            "NAMED6" => &named,
            "H5B6" => &h5b,
            _ => return None,
        };
        table.iter().find(|(f, _)| *f == form).map(|(_, c)| *c)
    };

    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: t,
            end_ns: t,
            step_ns: MIN as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let canon = |o: ProbeOutcome| -> Result<SeriesBits, String> {
        match o {
            ProbeOutcome::Answer(series) => Ok(series
                .into_iter()
                .map(|(mut l, p)| {
                    l.sort();
                    (l, p.into_iter().map(|(_, v)| (v + 0.0).to_bits()).collect())
                })
                .collect()),
            ProbeOutcome::TodaysRoute(why) => Err(format!("today's route: {why}")),
            ProbeOutcome::Refusal(e) => Err(format!("refusal: {e}")),
        }
    };
    let mut comparisons = 0u64;
    let mut wrong = Vec::new();
    let mut h6_erroring: std::collections::BTreeMap<String, u64> = Default::default();
    for (gi, (name, corpus, _, _)) in groups.iter().enumerate() {
        for (form, q) in &cells {
            let query = q.replace("SEL", &format!("{{service_name={name:?}}}"));
            let mp = match plan(&parse(&query).expect("parse"), &params, &plan_ctx(&db))
                .expect("plan")
            {
                Plan::Metric(mp) => mp,
                _ => panic!("{query}: a metric plan"),
            };
            let probe = GroupKeyProbe::new(&mp, &group_meta[gi])
                .unwrap_or_else(|| panic!("{query}: the group key read"));
            let sql::MetricValue::Unwrapped(u) = &mp.value else {
                unreachable!("the probe exists")
            };
            let lane = sql::metric_range_unwrapped_rows(
                "log_samples",
                u,
                probe.columns(),
                &[literal(name)],
                probe.fingerprints(),
                sql::BucketedScan {
                    window: TimeWindow {
                        start_ns: mp.start_ns,
                        end_ns: mp.end_ns,
                    },
                    lower: mp.scan_lower,
                    lo_ns: mp.grid_start_ns - MIN,
                    step_ns: MIN,
                },
                &mp.extra_predicates,
            )
            .expect("render L");
            let mut stream = client
                .query_stream::<UnwrappedLaneRow>(&lane.replace('?', "??"), &QuerySettings::new())
                .await
                .unwrap_or_else(|e| panic!("{query}: L: {e}"));
            let mut returned = std::collections::HashMap::new();
            while let Some(row) = stream.next().await {
                let row = row.unwrap_or_else(|e| panic!("{query}: L row: {e}"));
                returned.insert(row.fingerprint, row);
            }
            drop(stream);
            let (mut decided, mut missing, mut undecided) = (0u64, 0u64, 0u64);
            for (fp, (case, body, sm)) in &group_bodies[gi] {
                comparisons += 1;
                let as_body = UnwrappedLaneRow {
                    class: 0,
                    bucket_ns: t,
                    decided: 0,
                    keys: probe
                        .columns()
                        .keys
                        .iter()
                        .map(|_| (0, String::new()))
                        .collect(),
                    v: 0.0,
                    body: body.clone(),
                    fingerprint: *fp,
                    sm_text: sm.clone(),
                    sm_kept: Vec::new(),
                };
                let body_answer = canon(probe.fold_lane_rows(std::slice::from_ref(&as_body)));
                match returned.get(fp) {
                    None => {
                        missing += 1;
                        if *corpus == "H6"
                            && (case == "H6-pe-bad-latency" || case == "H6-pe-invalid-json")
                        {
                            *h6_erroring.entry(format!("{case} missing")).or_default() += 1;
                        }
                        if body_answer != Ok(Vec::new()) {
                            wrong.push(format!("{name} {form} {case}: missing, but the body answers {body_answer:?}"));
                        }
                    }
                    Some(row) if row.decided == 0 => {
                        undecided += 1;
                        if *corpus == "H6"
                            && (case == "H6-pe-bad-latency" || case == "H6-pe-invalid-json")
                        {
                            *h6_erroring.entry(format!("{case} undecided")).or_default() += 1;
                        }
                    }
                    Some(row) => {
                        decided += 1;
                        let key = canon(probe.fold_lane_rows(std::slice::from_ref(row)));
                        match (&key, &body_answer) {
                            (Err(why), _) if why.starts_with("today's route") => {}
                            (k, b) if k == b => {}
                            _ => wrong.push(format!(
                                "{name} {form} {case}: the key route {key:?}, the body {body_answer:?}"
                            )),
                        }
                    }
                }
            }
            if stream_is_plain(name)
                && let Some(want) = design_counts(corpus, form)
            {
                assert_eq!(
                    (decided, missing, undecided),
                    want,
                    "{name} {form}: decided / missing / undecided"
                );
            }
        }
    }
    assert_eq!(
        comparisons,
        1026 * 180 + 26 * 180 + 6 * 20,
        "row–form comparisons"
    );
    assert!(
        wrong.is_empty(),
        "{} wrong:\n{}",
        wrong.len(),
        wrong
            .iter()
            .take(40)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    // Over the 180 H6 cells, the two erroring bodies are never decided: an
    // unconvertible latency is undecided except in the 18 P cells, whose path
    // `req.latency` is absent from it (missing, so it contributes nothing on
    // either route), and invalid JSON is always undecided. (The design's 166
    // and 14 count the four P cells whose metadata carries a presence name as
    // undecided: those rows take today's route whatever the verdict.)
    assert_eq!(
        h6_erroring,
        [
            ("H6-pe-bad-latency missing".to_string(), 18),
            ("H6-pe-bad-latency undecided".to_string(), 162),
            ("H6-pe-invalid-json undecided".to_string(), 180),
        ]
        .into_iter()
        .collect(),
        "the erroring H6 bodies' verdicts"
    );
    drop_group_key_db(&admin, &db).await;
}

/// Whether an agreement group carries no extra stream label or metadata.
fn stream_is_plain(group: &str) -> bool {
    group.ends_with("_none")
}
