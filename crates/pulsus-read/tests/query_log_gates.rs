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

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
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
    let db = "pulsus_read_it_qlg_size";
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
    let db = "pulsus_read_it_qlg_narrow";
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
    let db = "pulsus_read_it_qlg_bodysearch";
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
    let run_db = format!("pulsus_read_it_qlg_{}", uuid::Uuid::new_v4().simple());
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
        db: db.to_string(),
        streams_idx: "log_streams_idx".to_string(),
        streams: "log_streams".to_string(),
        samples: "log_samples".to_string(),
        rollup_table: "log_metrics_5s".to_string(),
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
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

#[tokio::test]
async fn fetch_until_limit_first_page_over_budget_stays_query_too_broad() {
    skip_unless_live!();
    let db = "pulsus_read_it_qlg_budget_tight";
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
