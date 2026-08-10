//! CI regression gate for docs/schemas.md §9's two-tier evidence model,
//! Tier 1 (issue #16): asserts **scale-invariant** `system.query_log`
//! ratios on a deterministic CI-scale corpus. Gated behind
//! `PULSUS_TEST_CLICKHOUSE=1`, reusing `crates/pulsus-read/tests/
//! explain_indexes.rs`'s connection/setup pattern verbatim — the CI
//! `schema-it` job runs this after the EXPLAIN gate, against the same
//! ClickHouse 24.8 container.
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
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test query_log_gates
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_logql::parse;
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
}

/// Runs `sql` tagged with a unique `query_id`, draining every row of
/// `R`'s shape (the return count itself is not needed by every caller,
/// only that the stream is fully consumed before `SYSTEM FLUSH LOGS` —
/// `system.query_log`'s `QueryFinish` row is only written once the query
/// has fully completed), flushes logs, and reads back the evidence.
async fn run_and_capture<R: pulsus_clickhouse::ChRow>(
    client: &ChClient,
    admin: &ChClient,
    sql: &str,
    query_id: &str,
) -> (u64, QueryLogRow) {
    let settings = QuerySettings::new().set("query_id", query_id);
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
        "SELECT read_rows, read_bytes, ProfileEvents['SelectedMarks'] AS selected_marks \
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
    (returned, evidence)
}

/// Total marks the corpus's `log_samples` table holds — the denominator
/// for the skip-index pruning ratio, read straight off `system.parts`
/// rather than assumed from [`CORPUS_ROWS`]/[`INDEX_GRANULARITY`], so the
/// gate reflects the table's real physical layout.
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
        &[format!("'{SERVICE}'")],
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
        &[format!("'{SERVICE}'")],
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

    // The skip-index pruning gate: SelectedMarks/total_marks must be well
    // under 1 — proving the token/ngram bloom filter is actually skipping
    // granules that cannot contain the needle, not scanning every granule
    // in the stream (docs/schemas.md §3.2's whole point for finding #3).
    let ratio = evidence.selected_marks as f64 / total as f64;
    assert!(
        ratio <= 0.5,
        "SelectedMarks/total_marks ratio ({ratio:.3} = {}/{total}) did not show skip-index \
         pruning — expected the body skip index to rule out most of the corpus's granules",
        evidence.selected_marks
    );

    // read_bytes bounded relative to selected_marks (a ratio, never an
    // absolute byte count — edge case #5): a generous 4 KiB/row ceiling
    // per granule, comfortably above this corpus's ~170-byte rows, so the
    // bound only fires on a genuine regression (e.g. reading unrelated
    // granules), not on legitimate corpus growth.
    let granule_byte_ceiling = INDEX_GRANULARITY * 4096;
    let byte_bound = evidence.selected_marks.max(1) * granule_byte_ceiling;
    assert!(
        evidence.read_bytes <= byte_bound,
        "read_bytes ({}) exceeded {byte_bound} (selected_marks={} x {granule_byte_ceiling} \
         byte/granule ceiling)",
        evidence.read_bytes,
        evidence.selected_marks
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

    let result = engine
        .query(&expr, &params)
        .await
        .unwrap_or_else(|e| panic!("query err: {e:?}"));
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

    let result = engine
        .query(&expr, &params)
        .await
        .unwrap_or_else(|e| panic!("query err: {e:?}"));
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
            &[month.to_string()],
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
    // Discovery reads.
    engine.label_names(bounds).await.expect("label_names");
    engine
        .label_values("service_name", bounds)
        .await
        .expect("label_values");
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
        catalog_table: "trace_tag_catalog".to_string(),
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
        .list_tag_values("http.status_code", Some("span"))
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
            step_s: 60,
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
