//! Issue #57 AC2/AC6 plus the re-audit's AC-A2/AC-A3/AC-B1/AC-B2
//! (Tier-1, scale-invariant): live gates for the two-phase TraceQL
//! search against ClickHouse 24.8.
//!
//! - **AC2** — `EXPLAIN indexes = 1` on the **real** per-generator plans
//!   (the exact SQL `plan_search` emits, which since plan v7 is also the
//!   exact execution shape): `service_time` projection selection for the
//!   service-equality generator (corroborated through
//!   `system.query_log.projections` + a `read_rows` ratio), attr-index
//!   granule pruning within the `(key, val)` prefix **isolating time
//!   pruning** per issue #53 AC3b (full-range vs narrow-window over one
//!   dense fixed prefix — strictly fewer granules), the key-only
//!   numeric/regex generator classes, the indexed
//!   `resource.service.name =~` generator, and a top-K `Limit` node.
//! - **AC6 (a)** — the scan budget trips for real (a fallback-generator
//!   search under a tiny `scan_budget_rows` → 158 →
//!   `TooBroadReason::TraceScanBudgetRows`).
//! - **AC-A2** (re-audit, source truncation) — a multi-byte 4 MiB span
//!   name is truncated at the SOURCE to exactly the documented byte
//!   ceiling, proven by successful decode (no oversized block ever
//!   reaches the driver) and no retention-counter trip.
//! - **AC-A3′** (re-audit v7, the Layer-1 per-batch gate) — the sub-A
//!   source-truncation projection makes `max_result_bytes` accounting
//!   effective on hydration reads (a deliberate hardening), so a single
//!   over-sized batch now trips SERVER-side: 32 traces (=
//!   `BATCH_TRACES`, one phase-2 batch) × 1,250 spans × 8,000-byte
//!   (untruncated, ≤ `TRACE_STR_COL_CAP`) ASCII names → code 396 →
//!   `ScanBudgetBytes { TRACE_MAX_RESULT_BYTES }` → 422, before the
//!   driver materializes anything. Carries the full drift guard
//!   (P1/P2/P3′/P4-P7/P9/P10 + the enumerated S1-S8 loud-only
//!   residuals; M1 lives as a hermetic unit test in
//!   `pulsus_read::traces::exec`).
//! - **AC-A4** (re-audit v7/v8, the Layer-2 cross-batch gate) — the
//!   retention counter's distinct job is retained accumulation ACROSS
//!   batches: 256 traces (= 8 × `BATCH_TRACES`) × 160 spans × 8,000-byte
//!   names with `limit`=300/`spss`=200 (no evict, every match retained)
//!   keep every batch's result far under `TRACE_MAX_RESULT_BYTES`, while
//!   the heap-held `SpanSummary` charges (≥ 8,064 B each, surviving the
//!   per-batch release) accumulate to 330,301,440 B > the 256 MiB
//!   budget → `ScanBudgetBytes { HYDRATION_BYTE_BUDGET }` → 422 at
//!   ~batch 7 of 8. Guard Q1-Q9 + the S-carries.
//! - **AC-B1** (re-audit, generator memory) — the phase-1 candidate-
//!   generator's memory ceiling trips for real on the dense common-value
//!   corpus under a tiny `generator_max_memory_bytes` → 241 →
//!   `TooBroadReason::TraceGeneratorMemory`.
//! - **AC-B2** (re-audit, no regression) — AC6 (a) above and gate 8's
//!   transfer/`(Limit)` differential (below) stay green: the generator
//!   SQL shape is unchanged by the re-audit.
//! - **AC6 (c)** — a per-trace span overflow (> `MAX_SPANS_PER_TRACE`
//!   in-window spans) truncates and marks the response partial.
//! - **Issue #172 (structural operators)** — S1: `>`/`>>`/`~` e2e
//!   correctness (direct-child vs descendant discrimination, sibling
//!   pair, RHS-only spanSets, the zero-parent sibling pin); S2: the
//!   structural plan's generators keep the shipped index classes
//!   (`service_time` projection / attr `(key, val)` prefix — SQL is
//!   byte-identical to `&&`, pinned hermetically in `search_plan`); S3:
//!   a structural search under a tiny `scan_budget_rows` trips
//!   `TraceScanBudgetRows` → 422.
//!
//! Corpus: ≥100k time-spread spans, ≤5% low-frequency target service
//! (issue #53's binding fixture requirements for the data-dependent 24.8
//! optimizer). Live-gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_search_explain
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::logql::{ReadError, TooBroadReason};
use pulsus_read::traces::search_plan::{SearchParams, plan_search};
use pulsus_read::traces::search_sql;
use pulsus_read::{SearchPlan, TraceEngine, TraceReadConfig};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
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

const DB: &str = "pulsus_traces_search_it";
/// ≥100k time-spread spans (issue #53 fixture floor).
const CORPUS_SPANS: u64 = 120_000;
/// The default MergeTree index granularity: reads are quantized to
/// whole granules of this many rows, so every `read_rows` bound below
/// must budget in granule multiples, never in exact matched-row counts
/// (issue #60 CI flake: a `CORPUS_SPANS / 5` = 24,000 bound sat below
/// the 3-granule mark of 24,576 — a part/merge layout that selected one
/// granule more than the local layout breached it with the projection
/// working perfectly).
const GRANULE_ROWS: u64 = 8_192;
/// The whole corpus spans 47h ending "now".
const WINDOW_NS: i64 = 47 * 3_600 * 1_000_000_000;
/// `checkout` frequency: 1-in-50 spans (2% ≤ the 5% ceiling).
const CHECKOUT_EVERY: u64 = 50;
/// `http.status_code = 500` frequency: 1%.
const ERROR_EVERY: u64 = 100;

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

/// Seeds the AC2 corpus: `CORPUS_SPANS` single-span traces spread over
/// 47h, plus per-span attr-index rows — the dense `env=prod` prefix
/// (every span — the AC3b fixture), the 1% `http.status_code=500`
/// numeric target, and the `service.name` resource attribute.
async fn seed_corpus(client: &ChClient, db: &str, base_ns: i64) {
    let spread = WINDOW_NS / CORPUS_SPANS as i64;
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT \
               toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               toFixedString(unhex('0000000000000000'), 8), \
               'op', \
               if(number % {CHECKOUT_EVERY} = 0, 'checkout', concat('svc-', toString(number % 8))), \
               {base_ns} + toInt64(number) * {spread}, \
               toInt64(number) * 10000, \
               if(number % {ERROR_EVERY} = 0, 2, 0), 1, 1, 'p' \
             FROM numbers({CORPUS_SPANS})"
        ),
    )
    .await;
    // Dense (key, val) prefix: every span carries env=prod at resource
    // scope — the AC3b time-pruning-isolation fixture.
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_attrs_idx \
             (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
             SELECT \
               toDate(fromUnixTimestamp64Nano({base_ns} + toInt64(number) * {spread})), \
               'env', 'prod', 'resource', NULL, \
               {base_ns} + toInt64(number) * {spread}, \
               toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               1000000 \
             FROM numbers({CORPUS_SPANS})"
        ),
    )
    .await;
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_attrs_idx \
             (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
             SELECT \
               toDate(fromUnixTimestamp64Nano({base_ns} + toInt64(number) * {spread})), \
               'http.status_code', \
               if(number % {ERROR_EVERY} = 0, '500', '200'), 'span', \
               if(number % {ERROR_EVERY} = 0, 500.0, 200.0), \
               {base_ns} + toInt64(number) * {spread}, \
               toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               1000000 \
             FROM numbers({CORPUS_SPANS})"
        ),
    )
    .await;
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_attrs_idx \
             (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
             SELECT \
               toDate(fromUnixTimestamp64Nano({base_ns} + toInt64(number) * {spread})), \
               'service.name', \
               if(number % {CHECKOUT_EVERY} = 0, 'checkout', concat('svc-', toString(number % 8))), \
               'resource', NULL, \
               {base_ns} + toInt64(number) * {spread}, \
               toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               1000000 \
             FROM numbers({CORPUS_SPANS})"
        ),
    )
    .await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainRow {
    #[serde(with = "serde_bytes")]
    explain: Vec<u8>,
}

async fn explain_raw(client: &ChClient, sql: &str) -> String {
    // The engine doubles literal `?` at its own execution boundary
    // (`escape_query_placeholders`); this raw EXPLAIN path must apply the
    // same driver-quirk fix for regex generators (`(?:` patterns).
    let full = format!("EXPLAIN indexes = 1 {}", sql.replace('?', "??"));
    let mut out = String::new();
    let mut stream = client
        .query_stream::<ExplainRow>(&full, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("explain failed: {e}\nSQL:\n{full}"));
    while let Some(row) = stream.next().await {
        out.push_str(&String::from_utf8_lossy(
            &row.expect("decode explain row").explain,
        ));
        out.push('\n');
    }
    out
}

/// Raw `EXPLAIN PIPELINE` text (the plan v7 common-value gate runs it on
/// the real generator shape).
async fn explain_pipeline_raw(client: &ChClient, sql: &str) -> String {
    let full = format!("EXPLAIN PIPELINE {}", sql.replace('?', "??"));
    let mut out = String::new();
    let mut stream = client
        .query_stream::<ExplainRow>(&full, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("explain pipeline failed: {e}\nSQL:\n{full}"));
    while let Some(row) = stream.next().await {
        out.push_str(&String::from_utf8_lossy(
            &row.expect("decode explain row").explain,
        ));
        out.push('\n');
    }
    out
}

/// The `PrimaryKey` block's `Granules: k/N` ratio (panics with the raw
/// text when absent — same idiom as `traces_point_read.rs`).
fn primary_key_granules(raw: &str) -> (u64, u64) {
    const BLOCK_TITLES: &[&str] = &["MinMax", "Partition", "PrimaryKey", "Skip"];
    let mut in_pk = false;
    for line in raw.lines() {
        let trimmed = line.trim();
        if BLOCK_TITLES.contains(&trimmed) {
            in_pk = trimmed == "PrimaryKey";
            continue;
        }
        if in_pk && let Some(ratio) = trimmed.strip_prefix("Granules: ") {
            let (selected, total) = ratio
                .split_once('/')
                .unwrap_or_else(|| panic!("unparseable granules {trimmed:?}\n{raw}"));
            return (
                selected.trim().parse().expect("selected"),
                total.trim().parse().expect("total"),
            );
        }
    }
    panic!("no PrimaryKey Granules line in EXPLAIN output:\n{raw}");
}

/// A named `Skip` index block's `Granules: k/N` ratio — used for the
/// `idx_duration` minmax reduction gate.
fn skip_index_granules(raw: &str, index_name: &str) -> (u64, u64) {
    const BLOCK_TITLES: &[&str] = &["MinMax", "Partition", "PrimaryKey", "Skip"];
    let mut in_skip = false;
    let mut named = false;
    for line in raw.lines() {
        let trimmed = line.trim();
        if BLOCK_TITLES.contains(&trimmed) {
            in_skip = trimmed == "Skip";
            named = false;
            continue;
        }
        if !in_skip {
            continue;
        }
        if let Some(name) = trimmed.strip_prefix("Name: ") {
            named = name.trim() == index_name;
            continue;
        }
        if named && let Some(ratio) = trimmed.strip_prefix("Granules: ") {
            let (selected, total) = ratio
                .split_once('/')
                .unwrap_or_else(|| panic!("unparseable granules {trimmed:?}\n{raw}"));
            return (
                selected.trim().parse().expect("selected"),
                total.trim().parse().expect("total"),
            );
        }
    }
    panic!("no Skip block named {index_name:?} with a Granules line in EXPLAIN output:\n{raw}");
}

/// Drains one tagged query to completion (the gate-8 LIMIT differential
/// runs the generator SQL directly, outside the engine, so each variant
/// carries its own `query_id`).
async fn drain_tagged(client: &ChClient, sql: &str, query_id: &str) {
    use pulsus_read::traces::rows::CandidateRow;
    let sql = sql.replace('?', "??");
    let settings = QuerySettings::new().set("query_id", query_id);
    let mut stream = client
        .query_stream::<CandidateRow>(&sql, &settings)
        .await
        .unwrap_or_else(|e| panic!("tagged query failed: {e}\nSQL:\n{sql}"));
    while let Some(row) = stream.next().await {
        row.expect("decode candidate row");
    }
}

/// The `QueryFinish` row for an exact `query_id`.
async fn query_log_by_id(client: &ChClient, query_id: &str) -> QueryLogRow {
    let sql = format!(
        "SELECT read_rows, result_rows, memory_usage, projections FROM system.query_log \
         WHERE query_id = '{query_id}' AND type = 'QueryFinish' \
         ORDER BY event_time_microseconds DESC LIMIT 1"
    );
    let mut stream = client
        .query_stream::<QueryLogRow>(&sql, &QuerySettings::new())
        .await
        .expect("query_log read");
    let mut row = None;
    while let Some(r) = stream.next().await {
        row = Some(r.expect("decode query_log row"));
    }
    row.unwrap_or_else(|| panic!("no QueryFinish row for query_id {query_id}"))
}

/// The most recent `QueryFinish` row for a generator identified by SQL
/// fragments — read after the engine executed the real search.
async fn generator_query_log(client: &ChClient, like_fragments: &[&str]) -> Option<QueryLogRow> {
    let mut predicate = format!("type = 'QueryFinish' AND current_database = '{DB}'");
    for fragment in like_fragments {
        predicate.push_str(&format!(" AND query LIKE '%{fragment}%'"));
    }
    let sql = format!(
        "SELECT read_rows, result_rows, memory_usage, projections FROM system.query_log \
         WHERE {predicate} ORDER BY event_time_microseconds DESC LIMIT 1"
    );
    let mut stream = client
        .query_stream::<QueryLogRow>(&sql, &QuerySettings::new())
        .await
        .expect("query_log read");
    let mut row = None;
    while let Some(r) = stream.next().await {
        row = Some(r.expect("decode query_log row"));
    }
    row
}

fn engine_config() -> TraceReadConfig {
    TraceReadConfig {
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
        catalog_table: "trace_tag_catalog".to_string(),
        edges_table: "trace_edges".to_string(),
        max_candidates: 100_000,
        scan_budget_rows: 50_000_000,
        max_series: 1_000,
        generator_max_memory_bytes: 536_870_912,
        distributed: false,
        skip_unavailable_shards: false,
    }
}

async fn data_client() -> ChClient {
    let mut cfg = test_config();
    cfg.database = DB.to_string();
    ChClient::new(cfg).await.expect("connect data client")
}

fn plan_for(engine: &TraceEngine, q: &str, start_ns: i64, end_ns: i64) -> SearchPlan {
    plan_for_with(engine, q, start_ns, end_ns, 20, 3)
}

/// `plan_for` with explicit `limit`/`spss` — the AC-A4 retained-
/// accumulation gate needs both caps above its fixture dimensions so the
/// heap never evicts and every matched span is retained.
fn plan_for_with(
    engine: &TraceEngine,
    q: &str,
    start_ns: i64,
    end_ns: i64,
    limit: u32,
    spss: u32,
) -> SearchPlan {
    let query = pulsus_traceql::parse(q).expect("query parses");
    plan_search(
        &query,
        &SearchParams {
            start_ns,
            end_ns,
            limit,
            spss,
        },
        &engine.search_ctx(),
    )
    .expect("query plans")
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct QueryLogRow {
    read_rows: u64,
    result_rows: u64,
    memory_usage: u64,
    projections: Vec<String>,
}

/// One `#[tokio::test]` running every gate in sequence — the corpus is
/// seeded once; ordering between gates never matters but re-seeding per
/// gate would be pure waste.
#[tokio::test]
async fn two_phase_search_explain_and_budget_gates() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/traces_search_explain.rs for setup)"
        );
        return;
    }

    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    run_init(&admin, &test_ctx(DB)).await.expect("run_init");

    let now = now_ns();
    let base = now - WINDOW_NS;
    let client = data_client().await;
    seed_corpus(&client, DB, base).await;

    let engine = TraceEngine::new(data_client().await, engine_config());

    // ---- AC2 gate 1: service-equality generator → service_time --------
    let plan = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s }"#,
        base,
        now,
    );
    assert_eq!(plan.generator_sqls.len(), 1);
    let service_gen = &plan.generator_sqls[0];
    let raw = explain_raw(&client, service_gen).await;
    assert!(
        raw.contains("service_time"),
        "the service-equality generator must select the service_time projection:\n{raw}"
    );
    assert!(
        raw.contains("Limit"),
        "a top-K Limit node must be present:\n{raw}"
    );

    // Execute the REAL search once, then corroborate through
    // system.query_log: the generator's QueryFinish row names the
    // projection and reads a small fraction of the corpus.
    let output = engine.search(&plan).await.expect("search executes");
    assert!(!output.partial, "an in-budget search is not partial");
    exec(&client, "SYSTEM FLUSH LOGS").await;
    let row = generator_query_log(&client, &["PREWHERE service = \\'checkout\\'", "bound_ts"])
        .await
        .expect("the generator's QueryFinish row must exist");
    assert!(
        row.projections.iter().any(|p| p.contains("service_time")),
        "query_log.projections must name service_time, got {:?}",
        row.projections
    );
    // Granule evidence (the honest pruning unit, #53 pattern): the
    // EXPLAIN above must select a small subset of the projection's
    // granules — the layout-robust form of "touches a small fraction".
    let (proj_sel, proj_total) = primary_key_granules(&raw);
    assert!(
        proj_sel > 0 && proj_sel * 2 <= proj_total,
        "the service_time projection read must select at most half the granules \
         ({proj_sel}/{proj_total}):\n{raw}"
    );
    // Row bound with granule headroom — do NOT re-tighten (issue #60 CI
    // flake, run 29469732884): the true match is ~2,400 checkout rows
    // (CORPUS_SPANS / CHECKOUT_EVERY), but reads quantize to 8,192-row
    // granules PER PART, so the measured `read_rows` is a small number
    // of granules whose count varies with the part/merge layout (CI
    // observed 26,233 ≈ 3.2 granules where local layouts read under
    // 24,576). CORPUS_SPANS / 3 = 40,000 ≈ 5 granules: several granules
    // of headroom above the observed quantization boundary, still 3×
    // below the 120k full scan this gate exists to catch.
    assert!(
        row.read_rows < CORPUS_SPANS / 3,
        "the projection read must touch a small fraction of the corpus \
         (read {} of {CORPUS_SPANS}; bound {} ≈ 5 granules of {GRANULE_ROWS} rows)",
        row.read_rows,
        CORPUS_SPANS / 3
    );

    // ---- AC2 gate 2 (issue #53 AC3b): time pruning isolated within one
    // dense (key, val) prefix — full range vs narrow window, strictly
    // fewer granules for the narrow read. -------------------------------
    let full_plan = plan_for(&engine, r#"{ .env = "prod" }"#, base, now);
    let narrow_plan = plan_for(
        &engine,
        r#"{ .env = "prod" }"#,
        now - 30 * 60 * 1_000_000_000,
        now,
    );
    let (full_sel, full_total) =
        primary_key_granules(&explain_raw(&client, &full_plan.generator_sqls[0]).await);
    let (narrow_sel, _) =
        primary_key_granules(&explain_raw(&client, &narrow_plan.generator_sqls[0]).await);
    assert!(
        full_sel <= full_total && full_sel > 0,
        "full-range prefix read must engage the primary key ({full_sel}/{full_total})"
    );
    assert!(
        narrow_sel < full_sel,
        "the narrow window must prune strictly fewer granules within the SAME dense \
         (key, val) prefix — time pruning isolated (narrow {narrow_sel} vs full {full_sel})"
    );

    // ---- AC2 gate 3: key-only numeric generator ------------------------
    let plan = plan_for(&engine, "{ span.http.status_code >= 500 }", base, now);
    let raw = explain_raw(&client, &plan.generator_sqls[0]).await;
    let (sel, total) = primary_key_granules(&raw);
    assert!(
        sel < total,
        "the key-only numeric generator must prune on the (key) prefix ({sel}/{total}):\n{raw}"
    );

    // ---- AC2 gate 4: key-only regex generator --------------------------
    let plan = plan_for(&engine, r#"{ .env =~ "pro.*" }"#, base, now);
    let raw = explain_raw(&client, &plan.generator_sqls[0]).await;
    let (sel, total) = primary_key_granules(&raw);
    assert!(
        sel < total,
        "the key-only regex generator must prune on the (key) prefix ({sel}/{total}):\n{raw}"
    );

    // ---- AC2 gate 4b (issue #185): attribute EXISTENCE is index-served --
    // `{ span.http.status_code != nil }` compiles to a key-only `(key)`
    // prefix scan (the no-op `1` value predicate); it must prune on the
    // key prefix, never a full attr-table scan.
    let plan = plan_for(&engine, r#"{ span.http.status_code != nil }"#, base, now);
    let generator = &plan.generator_sqls[0];
    assert!(
        generator.contains("key = 'http.status_code' AND 1"),
        "existence compiles to the key-only predicate:\n{generator}"
    );
    let raw = explain_raw(&client, generator).await;
    let (sel, total) = primary_key_granules(&raw);
    assert!(
        sel < total,
        "attribute existence must prune on the (key) prefix ({sel}/{total}):\n{raw}"
    );

    // ---- AC2 gate 4c (issue #185): single-attribute ARITHMETIC pushes to
    // a column-side `val_num` predicate that still prunes on the (key)
    // prefix — NOT a post-hydration Rust scan. The query-performance
    // mandate: general arithmetic renders column-side, index-served.
    let plan = plan_for(
        &engine,
        r#"{ span.http.status_code * 1 >= 500 }"#,
        base,
        now,
    );
    let generator = &plan.generator_sqls[0];
    assert!(
        generator.contains("key = 'http.status_code' AND (val_num * 1) >= 500"),
        "single-attr arithmetic renders a column-side val_num predicate:\n{generator}"
    );
    let raw = explain_raw(&client, generator).await;
    let (sel, total) = primary_key_granules(&raw);
    assert!(
        sel < total,
        "arithmetic val_num pushdown must prune on the (key) prefix ({sel}/{total}):\n{raw}"
    );

    // ---- AC2 gate 5: indexed resource.service.name =~ ------------------
    let plan = plan_for(
        &engine,
        r#"{ resource.service.name =~ "check.*" }"#,
        base,
        now,
    );
    let generator = &plan.generator_sqls[0];
    assert!(
        generator.contains("FROM trace_attrs_idx"),
        "positive service regex must use its indexed attr row, not the fallback:\n{generator}"
    );
    let raw = explain_raw(&client, generator).await;
    let (sel, total) = primary_key_granules(&raw);
    assert!(
        sel < total,
        "service.name key prefix must prune ({sel}/{total})"
    );

    // ---- AC2 gate 6: duration generator — idx_duration minmax REDUCES
    // granules (code review round 1: presence alone is not a gate). The
    // corpus's durations grow monotonically with time (`number * 10µs`),
    // so slow spans cluster and the minmax index can prune for a
    // top-decile threshold. -----------------------------------------------
    let plan = plan_for(&engine, "{ duration > 1100ms }", base, now);
    let raw = explain_raw(&client, &plan.generator_sqls[0]).await;
    let (sel, total) = skip_index_granules(&raw, "idx_duration");
    assert!(
        sel < total,
        "idx_duration must prune granules for a clustered duration predicate \
         ({sel}/{total}):\n{raw}"
    );

    // ---- AC2 gate 7: status/kind span-scan class (code review round 1:
    // every real generator class is gated). No selective index exists —
    // the honest contract is a bounded time-window scan: the predicate
    // and top-K Limit are in the plan, and the executed read never
    // touches more than the window's rows. --------------------------------
    let plan = plan_for(&engine, "{ status = error }", base, now);
    let generator = &plan.generator_sqls[0];
    assert!(generator.contains("status_code = 2"));
    let raw = explain_raw(&client, generator).await;
    assert!(
        raw.contains("Limit"),
        "the span-scan generator must carry the top-K Limit node:\n{raw}"
    );
    let output = engine.search(&plan).await.expect("status search executes");
    assert!(output.returned > 0, "1% of the corpus is status=error");
    exec(&client, "SYSTEM FLUSH LOGS").await;
    let row = generator_query_log(&client, &["status_code = 2", "bound_ts"])
        .await
        .expect("the span-scan generator's QueryFinish row must exist");
    assert!(
        row.read_rows <= CORPUS_SPANS,
        "the span-scan generator is window-bounded — it must never read past the \
         corpus window (read {} of {CORPUS_SPANS})",
        row.read_rows
    );

    // ---- AC2 gate 8 (plan v7 delta on the flagship shape): the
    // common-value fixture — a (key, val) prefix shared by EVERY trace
    // (env=prod, 120k rows). The real per-generator execution shape must
    // stay bounded: EXPLAIN PIPELINE shows the top-K Limit, the
    // generator's transfer out of the read is exactly the cap+1 probe
    // rows (bounded output, never the full match set), the read stays
    // within the key prefix, and the engine reports the truncation as
    // partial. -------------------------------------------------------------
    let mut capped = engine_config();
    capped.max_candidates = 5_000;
    let capped_engine = TraceEngine::new(data_client().await, capped);
    let plan = plan_for(&capped_engine, r#"{ .env = "prod" }"#, base, now);
    let pipeline = explain_pipeline_raw(&client, &plan.generator_sqls[0]).await;
    assert!(
        pipeline.contains("Limit"),
        "EXPLAIN PIPELINE must show top-K early termination on the common-value \
         generator:\n{pipeline}"
    );
    let output = capped_engine
        .search(&plan)
        .await
        .expect("common-value search executes");
    assert!(
        output.partial,
        "a generator truncated at gen_cap+1 on a common value must report partial"
    );
    assert_eq!(output.returned, 20, "the full page is still served");

    // The embedded LIMIT differential (code review round 2's trip-proof,
    // made permanent): the SAME generator SQL runs with and without its
    // trailing LIMIT, tagged, and the two query_log rows are compared.
    // Measured physics on 24.8 (reported for adjudication): a
    // `GROUP BY trace_id ORDER BY max(ts) LIMIT k` top-K MUST visit every
    // row of the dense prefix for correctness — any trace in the prefix
    // can hold the max bound — so `read_rows` is the full prefix with or
    // without the LIMIT (asserted equal below, deliberately: reads are
    // NOT what the LIMIT bounds on this pinned shape). What the LIMIT
    // provably bounds, and what a removed LIMIT FAILS here: the transfer
    // out of the generator (result_rows == cap+1, vs the full 120k match
    // set) and the sort-stage memory (strictly smaller with the LIMIT).
    let limited_sql = &plan.generator_sqls[0];
    let unlimited_sql = limited_sql
        .rsplit_once("\nLIMIT ")
        .map(|(head, _)| head.to_string())
        .expect("the generator SQL ends with its LIMIT clause");
    drain_tagged(&client, limited_sql, "gate8-limited").await;
    drain_tagged(&client, &unlimited_sql, "gate8-unlimited").await;
    exec(&client, "SYSTEM FLUSH LOGS").await;
    let limited = query_log_by_id(&client, "gate8-limited").await;
    let unlimited = query_log_by_id(&client, "gate8-unlimited").await;
    assert_eq!(
        limited.result_rows, 5_001,
        "the generator ships exactly the cap+1 probe rows — bounded transfer, \
         never the full 120k-trace match set"
    );
    assert_eq!(
        unlimited.result_rows,
        CORPUS_SPANS - 1,
        "without the LIMIT the full common-value match set ships (every in-window \
         trace; row 0 sits exactly on the half-open start bound) — the bounded-\
         transfer gate above genuinely discriminates"
    );
    // Execution-graph differential (deterministic — a memory_usage
    // comparison proved cold-server-flaky: the 120k-group aggregation
    // state dominates both runs): the LIMIT materializes as the
    // `(Limit)` pipeline step only in the limited variant.
    // (`LimitsCheckingTransform` appears in both, hence the exact
    // parenthesized step marker.) Memory is recorded, never gated.
    let limited_pipeline = explain_pipeline_raw(&client, limited_sql).await;
    let unlimited_pipeline = explain_pipeline_raw(&client, &unlimited_sql).await;
    assert!(
        limited_pipeline.contains("(Limit)"),
        "the limited generator's execution pipeline must carry the Limit step:\n{limited_pipeline}"
    );
    assert!(
        !unlimited_pipeline.contains("(Limit)"),
        "removing the LIMIT must remove the Limit step — the differential \
         genuinely discriminates:\n{unlimited_pipeline}"
    );
    eprintln!(
        "gate8 recorded memory_usage: limited={} unlimited={}",
        limited.memory_usage, unlimited.memory_usage
    );
    assert_eq!(
        limited.read_rows, unlimited.read_rows,
        "documented physics: the GROUP-BY top-K visits the whole prefix either \
         way — a read-bounded common-value generator needs a different (plan-\
         amended) SQL shape, not a test assertion"
    );
    // Granule-aware slop — do NOT re-tighten (same quantization
    // arithmetic as gate 1): the true prefix is exactly CORPUS_SPANS
    // attr rows, but each covering part rounds its contiguous
    // (key, val) range up to whole granules on both ends (≤ ~2 granules
    // per part), and the attr table's three seeded keys across up to
    // three date partitions can leave the prefix spread over several
    // parts depending on merge timing — up to ~6 padding granules
    // (49,152 rows) in the worst layout, which the previous
    // CORPUS_SPANS / 4 (30,000 ≈ 3.6 granules) slop could not absorb.
    // 8 granules (65,536) of headroom still sits far below the 3-key
    // full-table scan (3 × CORPUS_SPANS = 360k) this gate exists to
    // catch.
    assert!(
        limited.read_rows <= CORPUS_SPANS + 8 * GRANULE_ROWS,
        "the read stays confined to the (key, val) prefix (+8-granule slop), never \
         the whole 3-key attr table (read {} of {} attr rows)",
        limited.read_rows,
        3 * CORPUS_SPANS
    );

    // ---- AC-B1 (issue #57 re-audit): the phase-1 generator memory
    // ceiling trips for real ------------------------------------------------
    // Same dense common-value corpus as gate 8 above (env=prod, 120k
    // rows / 120k distinct trace_id groups): under a deliberately tiny
    // `generator_max_memory_bytes`, the `GROUP BY trace_id` aggregation
    // state exceeds the ceiling before the top-K `LIMIT` can trim it
    // (live-verified physics, plan v2: a dense distinct-key prefix's
    // aggregation state scales with the matching prefix, not the LIMIT).
    let mut tiny_mem = engine_config();
    tiny_mem.generator_max_memory_bytes = 1024 * 1024; // 1 MiB
    let tiny_mem_engine = TraceEngine::new(data_client().await, tiny_mem);
    let plan = plan_for(&tiny_mem_engine, r#"{ .env = "prod" }"#, base, now);
    let err = tiny_mem_engine
        .search(&plan)
        .await
        .expect_err("a 120k-distinct-key generator must exceed a 1 MiB memory ceiling");
    match err {
        ReadError::QueryTooBroad(TooBroadReason::TraceGeneratorMemory { budget_bytes }) => {
            assert_eq!(budget_bytes, 1024 * 1024);
        }
        other => panic!("expected TraceGeneratorMemory, got {other:?}"),
    }

    // ---- AC-A2 (issue #57 re-audit): source truncation is a hard BYTE
    // ceiling, proven on multi-byte UTF-8 ------------------------------------
    // One trace, two spans, in a disjoint future window: span 0 (the
    // all-zero-parent root) carries a 4 MiB name of 4-byte code points
    // (`repeat('𠜎', 1_000_000)`) and a 20 KB service of 2-byte code
    // points (`repeat('é', 10_000)`) — both far past `TRACE_STR_COL_CAP`;
    // span 1 carries an exactly-8192-byte ASCII name (at the cap, so
    // untouched passthrough). A successful RowBinary decode into `String`
    // is itself the UTF-8-validity proof (invalid UTF-8 cannot decode).
    let mb_start = now + 15 * 3_600_000_000_000;
    exec(
        &client,
        &format!(
            "INSERT INTO {DB}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT \
               toFixedString(unhex(leftPad(lower(hex(3000000)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               toFixedString(unhex(if(number = 0, '0000000000000000', 'ffffffffffffffff')), 8), \
               if(number = 0, repeat('𠜎', 1000000), repeat('a', 8192)), \
               if(number = 0, repeat('é', 10000), 'svc'), \
               {mb_start} + toInt64(number), 1000, 0, 1, 1, 'p' \
             FROM numbers(2)"
        ),
    )
    .await;
    let plan = plan_for(&engine, "{}", mb_start - 1, mb_start + 1_000_000_000);
    let output = engine
        .search(&plan)
        .await
        .expect("the multi-byte fixture must NOT trip the retention counter");
    assert!(!output.partial, "a small, in-budget search is not partial");
    assert_eq!(output.returned, 1);
    let trace = &output.traces[0];
    assert_eq!(trace.spans.len(), 2, "both spans fit spss=3");
    // Span 0 (the root): the 4 MiB / 4-byte-code-point name truncates to
    // EXACTLY the byte ceiling (2048 code points x 4 bytes = 8192 bytes)
    // — the exact boundary case verified live in the plan's empirics.
    assert_eq!(
        trace.spans[0].name.len(),
        search_sql::TRACE_STR_COL_CAP as usize,
        "the 4-byte-code-point name must truncate to exactly the byte ceiling"
    );
    assert_eq!(
        trace.spans[0].name.chars().count(),
        2048,
        "the fallback cut is exactly 2048 code points"
    );
    // The root's service (2-byte code points, 20 KB) truncates to 2048
    // code points x 2 bytes = 4096 bytes.
    assert_eq!(
        trace.root.service.len(),
        4096,
        "the 2-byte-code-point service must truncate to 2048 code points (4096 bytes)"
    );
    // Span 1: exactly-8192-byte ASCII name passes through byte-identical
    // (the `length(col) <= TRACE_STR_COL_CAP` branch).
    assert_eq!(
        trace.spans[1].name.len(),
        search_sql::TRACE_STR_COL_CAP as usize
    );
    assert_eq!(trace.spans[1].name, "a".repeat(8192));

    // ---- AC6 (a): the scan budget trips for real -----------------------
    let mut tight = engine_config();
    tight.scan_budget_rows = 1_000;
    let tight_engine = TraceEngine::new(data_client().await, tight);
    let plan = plan_for(&tight_engine, r#"{ .env != "prod" }"#, base, now);
    let err = tight_engine
        .search(&plan)
        .await
        .expect_err("a fallback scan over 120k spans must exceed a 1k-row budget");
    match err {
        ReadError::QueryTooBroad(TooBroadReason::TraceScanBudgetRows { budget_rows }) => {
            assert_eq!(budget_rows, 1_000);
        }
        other => panic!("expected TraceScanBudgetRows, got {other:?}"),
    }

    // ---- AC-A3′ (issue #57 re-audit v7): the newly-effective Layer-1
    // per-batch bound trips server-side on one over-sized batch ----------
    // Fixture: N = BATCH_TRACES (32, exactly one phase-2 batch — all
    // 40k rows flow through ONE hydration query) traces x
    // M = 1,250 spans/trace (<= MAX_SPANS_PER_TRACE, so every row
    // hydrates) x NAME_BYTES = 8,000-byte ASCII names (<=
    // TRACE_STR_COL_CAP, untouched passthrough) = 320,000,000 result
    // bytes >= 4x TRACE_MAX_RESULT_BYTES. The sub-A source-truncation
    // projection makes `max_result_bytes` accounting effective on the
    // hydration read (live-verified — unwrapped passthrough columns were
    // never accounted; a deliberate hardening), so ClickHouse throws
    // code 396 BEFORE the driver materializes anything →
    // ScanBudgetBytes { TRACE_MAX_RESULT_BYTES } → 422.
    const AC_A3_N: u64 = 32;
    const AC_A3_M: u64 = 1_250;
    const AC_A3_NAME_BYTES: u64 = 8_000;

    let engine_cfg = engine_config();

    // ---- Guard layer 1: hermetic preconditions (v4/v5/v6 drift guard) --
    // P1-P4 compare only `const` fixture/production values — clippy
    // correctly observes they're compile-time-decidable, so they're
    // written as `const` blocks: a drift fails to COMPILE (louder than a
    // runtime panic, and still exactly the guard the plan specifies).
    const _: () = assert!(
        AC_A3_N == pulsus_read::BATCH_TRACES as u64,
        "P1: the fixture's trace count must equal BATCH_TRACES exactly — otherwise \
         per-batch charge release can split the accumulation under budget"
    );
    const _: () = assert!(
        AC_A3_M <= pulsus_read::MAX_SPANS_PER_TRACE as u64,
        "P2: the fixture's per-trace span count must not exceed MAX_SPANS_PER_TRACE, or \
         the hydration LIMIT truncates it away from the counter"
    );
    const _: () = assert!(
        AC_A3_N * AC_A3_M * AC_A3_NAME_BYTES
            >= 4 * pulsus_read::traces::exec::TRACE_MAX_RESULT_BYTES,
        "P3': the fixture's total result bytes must be at least 4x TRACE_MAX_RESULT_BYTES \
         — the A/B-observed 396 onset is <= ~2x the setting, so 4x pins a deterministic \
         throw with >= 2x headroom"
    );
    const _: () = assert!(
        AC_A3_NAME_BYTES <= search_sql::TRACE_STR_COL_CAP,
        "P4: the fixture's name bytes must not exceed TRACE_STR_COL_CAP, or sub-problem \
         A's truncation shrinks the seeded bytes before they're hydrated"
    );
    assert!(
        engine_cfg.max_candidates >= AC_A3_N,
        "P5: max_candidates must admit every fixture trace through the generator LIMIT \
         and the consumption ceiling"
    );
    assert_eq!(
        engine_cfg.scan_budget_rows, 50_000_000,
        "P6a: scan_budget_rows must equal the production default (pulsus-config \
         model.rs) — a tightened test config would falsely bound reads before P6b"
    );
    assert!(
        engine_cfg.generator_max_memory_bytes >= 64 * 1024 * 1024,
        "P7: generator_max_memory_bytes must stay well above this tiny generator's \
         real memory use, or the new memory ceiling could preempt this gate with a \
         DIFFERENT (loud, distinguishable) error"
    );

    let ac_a3_start = now + 3_600_000_000_000;
    exec(
        &client,
        &format!(
            "INSERT INTO {DB}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT \
               toFixedString(unhex(leftPad(lower(hex(1000000 + number % {AC_A3_N})), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               toFixedString(unhex('0000000000000000'), 8), \
               repeat('x', {AC_A3_NAME_BYTES}), 'bulky', {ac_a3_start} + toInt64(number), \
               1000, 0, 1, 1, 'p' \
             FROM numbers({})",
            AC_A3_N * AC_A3_M
        ),
    )
    .await;

    // ---- Guard layer 2: live fixture-integrity pre-check (v4) ----------
    // P9: the seeded fixture is EXACTLY the shape P1-P3' assume, and no
    // foreign row shares the window — a foreign candidate could displace
    // a bulky trace into a later batch, defeating batch confinement.
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct FixtureIntegrityRow {
        distinct_traces: u64,
        total_rows: u64,
        min_count: u64,
        max_count: u64,
        min_name_len: u64,
        max_name_len: u64,
    }
    /// One window's fixture-integrity aggregate — a helper fn so each
    /// read's `ChRowStream` (and its pooled-connection lease) is scoped
    /// here, not held to the end of the whole test body.
    async fn fixture_integrity(
        client: &ChClient,
        start_ns: i64,
        end_ns: i64,
    ) -> FixtureIntegrityRow {
        let sql = format!(
            "SELECT uniqExact(trace_id) AS distinct_traces, sum(c) AS total_rows, \
                    min(c) AS min_count, max(c) AS max_count, \
                    min(mn) AS min_name_len, max(mx) AS max_name_len \
             FROM (SELECT trace_id, count() AS c, min(length(name)) AS mn, \
                          max(length(name)) AS mx \
                   FROM {DB}.trace_spans \
                   WHERE timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns} \
                   GROUP BY trace_id)"
        );
        let mut stream = client
            .query_stream::<FixtureIntegrityRow>(&sql, &QuerySettings::new())
            .await
            .expect("fixture-integrity read");
        let mut integrity = None;
        while let Some(row) = stream.next().await {
            integrity = Some(row.expect("decode fixture-integrity row"));
        }
        integrity.expect("the fixture-integrity aggregate must return a row")
    }
    let integrity = fixture_integrity(&client, ac_a3_start - 1, ac_a3_start + 1_000_000_000).await;
    assert_eq!(
        integrity.distinct_traces, AC_A3_N,
        "P9: distinct trace count in the retention window"
    );
    assert_eq!(
        integrity.total_rows,
        AC_A3_N * AC_A3_M,
        "P9: total row count in the retention window"
    );
    assert_eq!(
        integrity.min_count, AC_A3_M,
        "P9: per-trace row count must be uniform (min)"
    );
    assert_eq!(
        integrity.max_count, AC_A3_M,
        "P9: per-trace row count must be uniform (max)"
    );
    assert_eq!(
        integrity.min_name_len, AC_A3_NAME_BYTES,
        "P9: name length must be uniform (min)"
    );
    assert_eq!(
        integrity.max_name_len, AC_A3_NAME_BYTES,
        "P9: name length must be uniform (max)"
    );

    // P6b: the whole-DB row count is a sound physical-read upper bound
    // for any single query — a query cannot select more granule rows
    // than the touched tables contain, so this re-derives with the
    // corpus (10x margin absorbs multi-stage PREWHERE/projection
    // accounting; no granule arithmetic needed).
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct RowCountRow {
        n: u64,
    }
    async fn table_row_count(client: &ChClient, table: &str) -> u64 {
        let sql = format!("SELECT count() AS n FROM {table}");
        let mut stream = client
            .query_stream::<RowCountRow>(&sql, &QuerySettings::new())
            .await
            .expect("row count read");
        let mut n = 0u64;
        while let Some(row) = stream.next().await {
            n = row.expect("decode row-count row").n;
        }
        n
    }
    let spans_total = table_row_count(&client, &format!("{DB}.trace_spans")).await;
    let attrs_total = table_row_count(&client, &format!("{DB}.trace_attrs_idx")).await;
    assert!(
        10 * (spans_total + attrs_total) <= engine_cfg.scan_budget_rows,
        "P6b: the whole-DB row count (10x margin) must stay under scan_budget_rows \
         (spans={spans_total}, attrs={attrs_total}, budget={})",
        engine_cfg.scan_budget_rows
    );

    let plan = plan_for(&engine, "{}", ac_a3_start - 1, ac_a3_start + 1_000_000_000);

    // P10 (v6, reclassified belt-and-braces in v7): the pre-hydration
    // Layer-2 charge bound, derived from the PLAN's own candidate cap
    // (the runtime source, exec.rs) — the tripwire pins it equal to the
    // engine config's cap too, so a future plan/config bifurcation trips
    // loudly instead of silently underbounding.
    assert_eq!(
        plan.max_candidates(),
        engine_cfg.max_candidates,
        "P10 tripwire: the plan's candidate cap must match the engine config's"
    );
    let cap = usize::try_from(plan.max_candidates()).expect("max_candidates fits usize");
    let pre_hydration_worst =
        2 * plan.generator_sqls.len() * (cap + 1) * pulsus_read::CANDIDATE_TUPLE_BYTES
            + pulsus_read::BATCH_TRACES * std::mem::size_of::<[u8; 16]>()
            + pulsus_read::RETAINED_ENTRY_OVERHEAD;
    assert!(
        pre_hydration_worst < pulsus_read::HYDRATION_BYTE_BUDGET / 8,
        "P10: the pre-hydration charge bound ({pre_hydration_worst} B) must stay far \
         under the hydration budget — a pre-hydration Layer-2 breach would carry \
         HYDRATION_BYTE_BUDGET, a loudly-different budget_bytes on this gate"
    );

    // Skipped-with-mechanism (v4/v5/v7): every OTHER trip source can only
    // fail this gate LOUDLY — a mismatched error variant or
    // `budget_bytes`, or an outright panic — never silently: S1 the
    // read-side byte preempt (code 307) is pinned to its OWN
    // budget_bytes by exec.rs's `m1_*` unit test; S3 the query-text
    // guard maps to a distinct TooBroadReason; S4 threshold termination
    // requires a populated heap (structurally inert on the first batch);
    // S5 `max_block_size` is framing only; S6 the generator's `+1`
    // truncation probe cannot engage (P5+P9 establish exactly N groups
    // exist); S7 `charge_explain` charges exactly 0 on this
    // `engine.search()` (non-explained) call path; S8 (v7) a Layer-2
    // retention preempt would carry HYDRATION_BYTE_BUDGET != the
    // expected TRACE_MAX_RESULT_BYTES (M1 distinctness → panic), and
    // cannot fire first anyway: client-side charge at 396-arrival is
    // <= ~2x 64 MiB (the observed onset ceiling) + P10's budget/8
    // ≈ 167.7 MB < 268.4 MB.
    let err = engine.search(&plan).await.expect_err(
        "32 x 1,250 x 8,000-byte ASCII names in one batch must trip max_result_bytes \
         server-side (code 396)",
    );
    match err {
        ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { budget_bytes, .. }) => {
            assert_eq!(
                budget_bytes,
                pulsus_read::traces::exec::TRACE_MAX_RESULT_BYTES,
                "AC-A3': the newly-effective Layer-1 per-batch bound (396) must fire, \
                 not the retention counter — the projection made max_result_bytes \
                 accounting effective on hydration reads"
            );
        }
        other => panic!("expected ScanBudgetBytes (TRACE_MAX_RESULT_BYTES), got {other:?}"),
    }

    // ---- AC-A4 (issue #57 re-audit v7/v8): the Layer-2 retention
    // counter's distinct job — CROSS-BATCH retained accumulation --------
    // 256 traces (= 8 x BATCH_TRACES) x 160 spans x 8,000-byte ASCII
    // names, searched with limit=300 (> N: the heap never fills, never
    // evicts, never threshold-terminates) and spss=200 (> M: every
    // matched span's summary is retained). Every batch's RESULT stays
    // far under TRACE_MAX_RESULT_BYTES (Q5), but the heap-held
    // SpanSummary charges (>= RETAINED_ENTRY_OVERHEAD + name.len() =
    // 8,064 B each, charged pre-clone in search_eval and NEVER released
    // — they survive the per-batch `budget.release(batch_charged)`)
    // accumulate to 256 x 160 x 8,064 = 330,301,440 B > 268,435,456 B →
    // the retention counter trips ~batch 7 of 8 →
    // ScanBudgetBytes { HYDRATION_BYTE_BUDGET } → 422. The trip site is
    // any ByteBudget::charge overflow; ATTRIBUTION to cross-batch
    // retention rests on Q7's arithmetic (no single batch plus phase-1
    // can trip alone), not on which site fired. The per-entry charge
    // formula itself (the 64-B overhead term + name bytes, at exact
    // equality) is pinned by the hermetic
    // `span_summary_charge_is_exactly_overhead_plus_name_len` unit in
    // `search_eval.rs` — this aggregate gate's slack over the budget
    // deliberately exceeds the summed overhead term (name bytes alone
    // trip it), so the unit test, not this gate, is what fails if the
    // overhead term is silently dropped.
    const AC_A4_N: u64 = 256;
    const AC_A4_M: u64 = 160;
    const AC_A4_NAME_BYTES: u64 = 8_000;
    const AC_A4_LIMIT: u32 = 300;
    const AC_A4_SPSS: u32 = 200;

    // Q1-Q5: compile-time where both sides are consts (the ratified
    // const-block discipline — drift fails to compile).
    const _: () = assert!(
        AC_A4_N.is_multiple_of(pulsus_read::BATCH_TRACES as u64)
            && AC_A4_N / (pulsus_read::BATCH_TRACES as u64) >= 4,
        "Q1: the fixture must be genuinely multi-batch (a whole multiple of \
         BATCH_TRACES, at least 4 batches)"
    );
    const _: () = assert!(
        AC_A4_M <= pulsus_read::MAX_SPANS_PER_TRACE as u64,
        "Q2: the per-trace span count must not exceed MAX_SPANS_PER_TRACE — the \
         hydration LIMIT BY must retain every seeded row"
    );
    const _: () = assert!(
        AC_A4_N * AC_A4_M * (pulsus_read::RETAINED_ENTRY_OVERHEAD as u64 + AC_A4_NAME_BYTES)
            > pulsus_read::HYDRATION_BYTE_BUDGET as u64,
        "Q3 (v8): the retained floor — every matched span's summary charges at least \
         RETAINED_ENTRY_OVERHEAD + name.len() before its clone and is never released \
         (Q6: no evict) — must exceed HYDRATION_BYTE_BUDGET"
    );
    const _: () = assert!(
        AC_A4_NAME_BYTES <= search_sql::TRACE_STR_COL_CAP,
        "Q4: names must pass the source-truncation cap untouched end-to-end — \
         hydration rows AND retained summaries carry the full seeded bytes"
    );
    const _: () = assert!(
        (pulsus_read::BATCH_TRACES as u64) * AC_A4_M * (AC_A4_NAME_BYTES + 1024)
            <= pulsus_read::traces::exec::TRACE_MAX_RESULT_BYTES * 3 / 4,
        "Q5: no per-batch 396 — one batch's result bytes (1,024 over-bounds the \
         per-row non-name bytes) must stay at or below 3/4 of TRACE_MAX_RESULT_BYTES, \
         under the accounting threshold"
    );

    let ac_a4_start = now + 20 * 3_600_000_000_000;
    exec(
        &client,
        &format!(
            "INSERT INTO {DB}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT \
               toFixedString(unhex(leftPad(lower(hex(4000000 + number % {AC_A4_N})), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               toFixedString(unhex('0000000000000000'), 8), \
               repeat('y', {AC_A4_NAME_BYTES}), 'retained', {ac_a4_start} + toInt64(number), \
               1000, 0, 1, 1, 'p' \
             FROM numbers({})",
            AC_A4_N * AC_A4_M
        ),
    )
    .await;

    // Q9 (live integrity, the P9 aggregate shape over the new window):
    // the seeded fixture is exactly the Q1-Q5 shape and no foreign row
    // shares the window.
    let integrity = fixture_integrity(&client, ac_a4_start - 1, ac_a4_start + 1_000_000_000).await;
    assert_eq!(integrity.distinct_traces, AC_A4_N, "Q9: distinct traces");
    assert_eq!(integrity.total_rows, AC_A4_N * AC_A4_M, "Q9: total rows");
    assert_eq!(
        integrity.min_count, AC_A4_M,
        "Q9: uniform per-trace count (min)"
    );
    assert_eq!(
        integrity.max_count, AC_A4_M,
        "Q9: uniform per-trace count (max)"
    );
    assert_eq!(
        integrity.min_name_len, AC_A4_NAME_BYTES,
        "Q9: uniform name length (min)"
    );
    assert_eq!(
        integrity.max_name_len, AC_A4_NAME_BYTES,
        "Q9: uniform name length (max)"
    );

    // Q8 + P6a/P6b re-run (delivery; the whole-DB row-count bound
    // re-derives itself with the +40,960 fixture rows).
    assert!(
        engine_cfg.max_candidates >= AC_A4_N,
        "Q8: max_candidates must admit every AC-A4 trace"
    );
    assert_eq!(
        engine_cfg.scan_budget_rows, 50_000_000,
        "P6a (re-run): scan_budget_rows must equal the production default"
    );
    let spans_total = table_row_count(&client, &format!("{DB}.trace_spans")).await;
    let attrs_total = table_row_count(&client, &format!("{DB}.trace_attrs_idx")).await;
    assert!(
        10 * (spans_total + attrs_total) <= engine_cfg.scan_budget_rows,
        "P6b (re-run): the whole-DB row count (10x margin) must stay under \
         scan_budget_rows (spans={spans_total}, attrs={attrs_total})"
    );

    let plan = plan_for_with(
        &engine,
        "{}",
        ac_a4_start - 1,
        ac_a4_start + 1_000_000_000,
        AC_A4_LIMIT,
        AC_A4_SPSS,
    );

    // Q6 (runtime, on the BUILT plan — the runtime source): the heap
    // never reaches `limit`, so there is no evict-release and no
    // threshold termination; every matched span is fully retained
    // (spss > M).
    assert!(
        plan.limit() as u64 >= AC_A4_N && plan.spss() as u64 >= AC_A4_M,
        "Q6: limit ({}) must be >= N ({AC_A4_N}) and spss ({}) >= M ({AC_A4_M}) — \
         eviction or spss-capping would release retained charges and defeat the gate",
        plan.limit(),
        plan.spss()
    );

    // Q7 (attribution): phase-1 charges plus TWO batch-transient
    // envelopes stay under the budget — so no single batch plus phase-1
    // can trip alone, and the asserted trip REQUIRES retained carryover
    // from >= 4 completed prior batches (the cross-batch path, by
    // arithmetic). The 2x(NAME+1024) per-row envelope covers one batch's
    // transient hydration charge plus its own retained summaries plus
    // eval sets/transients.
    assert_eq!(
        plan.max_candidates(),
        engine_cfg.max_candidates,
        "Q7 tripwire (P10's): plan cap must match config cap"
    );
    let cap = usize::try_from(plan.max_candidates()).expect("max_candidates fits usize");
    let pre_hydration_worst =
        2 * plan.generator_sqls.len() * (cap + 1) * pulsus_read::CANDIDATE_TUPLE_BYTES
            + pulsus_read::BATCH_TRACES * std::mem::size_of::<[u8; 16]>()
            + pulsus_read::RETAINED_ENTRY_OVERHEAD;
    let batch_ceiling =
        2 * pulsus_read::BATCH_TRACES * (AC_A4_M as usize) * ((AC_A4_NAME_BYTES as usize) + 1024);
    assert!(
        pre_hydration_worst + 2 * batch_ceiling < pulsus_read::HYDRATION_BYTE_BUDGET,
        "Q7: phase-1 worst ({pre_hydration_worst} B) + 2 batch ceilings \
         ({batch_ceiling} B each) must stay under HYDRATION_BYTE_BUDGET — otherwise a \
         single batch could trip without cross-batch accumulation"
    );

    // S-carries (v7): S3 query-text (unchanged 32-id batches), S5 block
    // framing, S6 the +1 probe (Q9: exactly N groups), S7 explain-zero;
    // S1/S2/per-batch-396 loud via M1 value distinctness AND non-firing
    // via Q5.
    let err = engine.search(&plan).await.expect_err(
        "256 x 160 x 8,000-byte retained summaries must trip the retention counter \
         across batches",
    );
    match err {
        ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { budget_bytes, .. }) => {
            assert_eq!(
                budget_bytes,
                pulsus_read::HYDRATION_BYTE_BUDGET as u64,
                "AC-A4: the retention counter (cross-batch retained accumulation) must \
                 be the tripping bound — a 396 here would carry TRACE_MAX_RESULT_BYTES"
            );
        }
        other => panic!("expected ScanBudgetBytes (retention counter), got {other:?}"),
    }

    // ---- AC6 (c): per-trace span overflow → truncate + partial, AND the
    // root still resolves (code review round 1: root hydration is
    // trace-wide with no row cap). One trace with MAX_SPANS_PER_TRACE +
    // 100 spans in another disjoint future window — the ONLY all-zero-
    // parent root is the trace's LAST span (position 10,100, past any
    // 10,001-row cap and past the hydration truncation point).
    let overflow_start = now + 10 * 3_600_000_000_000;
    let overflow_spans = pulsus_read::MAX_SPANS_PER_TRACE as u64 + 100;
    exec(
        &client,
        &format!(
            "INSERT INTO {DB}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT \
               toFixedString(unhex(leftPad(lower(hex(2000000)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               toFixedString(unhex(if(number = {last}, '0000000000000000', \
                                      'ffffffffffffffff')), 8), \
               if(number = {last}, 'wide-root', 'wide'), 'wide-svc', \
               {overflow_start} + toInt64(number), 1000, 0, 1, 1, 'p' \
             FROM numbers({overflow_spans})",
            last = overflow_spans - 1
        ),
    )
    .await;
    let plan = plan_for(
        &engine,
        "{}",
        overflow_start - 1,
        overflow_start + 3_600_000_000_000,
    );
    let output = engine
        .search(&plan)
        .await
        .expect("overflow search executes");
    assert_eq!(
        output.returned, 1,
        "the overflowing trace is still returned"
    );
    assert!(
        output.partial,
        "a truncated trace is never silently reported complete"
    );
    assert_eq!(
        output.traces[0].root.name, "wide-root",
        "the all-zero-parent root arriving as the trace's 10,100th span must still \
         resolve — the root read is trace-wide and uncapped"
    );
    assert_eq!(
        output.traces[0].root.start_ns,
        overflow_start + (overflow_spans as i64 - 1),
        "root metadata comes from the true root span"
    );

    // ---- Issue #172 gate S1: structural correctness e2e ----------------
    // Disjoint future window. Trace T1: root A (checkout) → child B
    // (span.foo=x) → grandchild C (status=error), plus D (child of A,
    // B's sibling) and a second zero-parent root E. Control trace T2:
    // the same B-shape span (foo=x) and an error span exist, but under a
    // NON-checkout root — `>`/`>>` must not match them.
    let st = now + 30 * 3_600_000_000_000;
    const T1: &str = "00000000000000000000000000517201";
    const T2: &str = "00000000000000000000000000517202";
    /// `(span_hex, parent_hex, name, service, ts, status_code)`.
    type StructuralSpanSpec<'a> = (&'a str, &'a str, &'a str, &'a str, i64, i8);
    async fn insert_structural_span(
        client: &ChClient,
        trace_hex: &str,
        spec: StructuralSpanSpec<'_>,
    ) {
        let (span_hex, parent_hex, name, service, ts, status) = spec;
        exec(
            client,
            &format!(
                "INSERT INTO {DB}.trace_spans \
                 (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
                  status_code, kind, payload_type, payload) \
                 SELECT toFixedString(unhex('{trace_hex}'), 16), \
                        toFixedString(unhex('{span_hex}'), 8), \
                        toFixedString(unhex('{parent_hex}'), 8), \
                        '{name}', '{service}', {ts}, 1000, {status}, 1, 1, 'p'"
            ),
        )
        .await;
    }
    async fn insert_structural_attr(client: &ChClient, trace_hex: &str, span_hex: &str, ts: i64) {
        exec(
            client,
            &format!(
                "INSERT INTO {DB}.trace_attrs_idx \
                 (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
                 SELECT toDate(fromUnixTimestamp64Nano({ts})), 'foo', 'x', 'span', NULL, {ts}, \
                        toFixedString(unhex('{trace_hex}'), 16), \
                        toFixedString(unhex('{span_hex}'), 8), 1000"
            ),
        )
        .await;
    }
    const ZERO8: &str = "0000000000000000";
    const A: &str = "00000000000000a1";
    const B: &str = "00000000000000b1";
    const C: &str = "00000000000000c1";
    const D: &str = "00000000000000d1";
    for spec in [
        (A, ZERO8, "root-a", "checkout", st, 0),
        (B, A, "child-b", "websvc", st + 10, 0),
        (C, B, "grand-c", "websvc", st + 20, 2),
        (D, A, "sib-d", "websvc", st + 30, 0),
        ("00000000000000e1", ZERO8, "root-b", "websvc", st + 40, 0),
    ] {
        insert_structural_span(&client, T1, spec).await;
    }
    for spec in [
        ("00000000000000f1", ZERO8, "root-f", "othersvc", st + 50, 0),
        (
            "00000000000000f2",
            "00000000000000f1",
            "child-g",
            "websvc",
            st + 60,
            0,
        ),
        (
            "00000000000000f3",
            "00000000000000f1",
            "err-h",
            "websvc",
            st + 70,
            2,
        ),
    ] {
        insert_structural_span(&client, T2, spec).await;
    }
    insert_structural_attr(&client, T1, B, st + 10).await;
    insert_structural_attr(&client, T2, "00000000000000f2", st + 60).await;

    let (s_start, s_end) = (st - 1, st + 3_600_000_000_000);

    // `>`: the direct child only, RHS spans only.
    let plan = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" } > { span.foo = "x" }"#,
        s_start,
        s_end,
    );
    let output = engine.search(&plan).await.expect("child search executes");
    assert_eq!(
        output.returned, 1,
        "only the trace whose foo=x span sits under a checkout parent"
    );
    assert_eq!(output.traces[0].matched, 1, "RHS result set only");
    assert_eq!(output.traces[0].spans.len(), 1);
    assert_eq!(
        output.traces[0].spans[0].name, "child-b",
        "the spanSet holds the RHS span, never the checkout LHS span"
    );

    // `>` does NOT reach the grandchild…
    let plan = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" } > { status = error }"#,
        s_start,
        s_end,
    );
    let output = engine.search(&plan).await.expect("search executes");
    assert_eq!(
        output.returned, 0,
        "the error span is a grandchild, not a child"
    );

    // …while `>>` does.
    let plan = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" } >> { status = error }"#,
        s_start,
        s_end,
    );
    let output = engine
        .search(&plan)
        .await
        .expect("descendant search executes");
    assert_eq!(
        output.returned, 1,
        "T2's error span has no checkout ancestor"
    );
    assert_eq!(output.traces[0].matched, 1);
    assert_eq!(output.traces[0].spans[0].name, "grand-c");

    // `~`: the sibling pair under the shared parent, RHS side returned.
    let plan = plan_for(
        &engine,
        r#"{ span.foo = "x" } ~ { name = "sib-d" }"#,
        s_start,
        s_end,
    );
    let output = engine.search(&plan).await.expect("sibling search executes");
    assert_eq!(output.returned, 1);
    assert_eq!(output.traces[0].matched, 1);
    assert_eq!(output.traces[0].spans[0].name, "sib-d");

    // Adjudicated pin 2: zero-parent roots never match `~`.
    let plan = plan_for(
        &engine,
        r#"{ name = "root-a" } ~ { name = "root-b" }"#,
        s_start,
        s_end,
    );
    let output = engine
        .search(&plan)
        .await
        .expect("root-sibling search executes");
    assert_eq!(
        output.returned, 0,
        "all-zero parent_id spans share no parent — never siblings"
    );

    // ---- Issue #172 gate S2: structural generators keep the shipped
    // index classes (no new unindexed SQL shape exists) -------------------
    let plan = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" } >> { .env = "prod" }"#,
        base,
        now,
    );
    assert_eq!(
        plan.generator_sqls.len(),
        2,
        "superset union of both operands' generators — the && shape"
    );
    let raw = explain_raw(&client, &plan.generator_sqls[0]).await;
    assert!(
        raw.contains("service_time"),
        "the structural LHS's service-equality generator must still select the \
         service_time projection:\n{raw}"
    );
    let raw = explain_raw(&client, &plan.generator_sqls[1]).await;
    let (sel, total) = primary_key_granules(&raw);
    assert!(
        sel < total,
        "the structural RHS's attr generator must still prune on its (key, val) \
         prefix ({sel}/{total}):\n{raw}"
    );

    // ---- Issue #172 gate S3: the scan budget trips on a structural
    // search exactly like the shipped classes (AC6a shape reused) ---------
    let mut tight = engine_config();
    tight.scan_budget_rows = 1_000;
    let tight_engine = TraceEngine::new(data_client().await, tight);
    let plan = plan_for(
        &tight_engine,
        r#"{ resource.service.name = "checkout" } >> { status = error }"#,
        base,
        now,
    );
    let err = tight_engine
        .search(&plan)
        .await
        .expect_err("the structural RHS's span-scan generator must exceed a 1k-row budget");
    match err {
        ReadError::QueryTooBroad(TooBroadReason::TraceScanBudgetRows { budget_rows }) => {
            assert_eq!(budget_rows, 1_000);
        }
        other => panic!("expected TraceScanBudgetRows, got {other:?}"),
    }

    // ---- Issue #183 AC6: the complete 15-combination structural matrix
    // (all 5 base ops × 3 modifiers + the 2 empty-LHS negation edges) over
    // the byte-frozen fixture T1, with a control trace T2 that matches none
    // of them (per-trace scoping). ---------------------------------------
    // T1: A root (.k=a, .h=hg) → B (.k=b, .g=gg, .h=hg) → C (.k=c, .g=gg),
    // plus B2 (.k=b2, .g=gg, .h=hg) — a second child of A. So .g="gg"
    // selects {B,C,B2} and .h="hg" selects {A,B,B2}. T2: lone root D (.k=d).
    let ac6_st = now + 33 * 3_600_000_000_000;
    const AC6_T1: &str = "00000000000000000000000000ac6001";
    const AC6_T2: &str = "00000000000000000000000000ac6002";
    const AC6_A: &str = "00000000000000a6";
    const AC6_B: &str = "00000000000000b6";
    const AC6_C: &str = "00000000000000c6";
    const AC6_B2: &str = "00000000000000d6";
    const AC6_D: &str = "00000000000000e6";
    // Spans (name carries the label so the assertions read the returned set).
    for spec in [
        (AC6_A, ZERO8, "ac6-a", "svc", ac6_st, 0),
        (AC6_B, AC6_A, "ac6-b", "svc", ac6_st + 10, 0),
        (AC6_C, AC6_B, "ac6-c", "svc", ac6_st + 20, 0),
        (AC6_B2, AC6_A, "ac6-b2", "svc", ac6_st + 30, 0),
    ] {
        insert_structural_span(&client, AC6_T1, spec).await;
    }
    insert_structural_span(
        &client,
        AC6_T2,
        (AC6_D, ZERO8, "ac6-d", "svc", ac6_st + 40, 0),
    )
    .await;
    // Attribute rows: (span, key, val).
    async fn insert_ac6_attr(
        client: &ChClient,
        trace_hex: &str,
        span_hex: &str,
        key: &str,
        val: &str,
        ts: i64,
    ) {
        exec(
            client,
            &format!(
                "INSERT INTO {DB}.trace_attrs_idx \
                 (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
                 SELECT toDate(fromUnixTimestamp64Nano({ts})), '{key}', '{val}', 'span', NULL, {ts}, \
                        toFixedString(unhex('{trace_hex}'), 16), \
                        toFixedString(unhex('{span_hex}'), 8), 1000"
            ),
        )
        .await;
    }
    for (span_hex, rows) in [
        (AC6_A, &[("k", "a"), ("h", "hg")][..]),
        (AC6_B, &[("k", "b"), ("g", "gg"), ("h", "hg")][..]),
        (AC6_C, &[("k", "c"), ("g", "gg")][..]),
        (AC6_B2, &[("k", "b2"), ("g", "gg"), ("h", "hg")][..]),
    ] {
        for (key, val) in rows {
            insert_ac6_attr(&client, AC6_T1, span_hex, key, val, ac6_st).await;
        }
    }
    insert_ac6_attr(&client, AC6_T2, AC6_D, "k", "d", ac6_st + 40).await;

    let (ac6_start, ac6_end) = (ac6_st - 1, ac6_st + 3_600_000_000_000);
    async fn ac6_search_names(
        engine: &TraceEngine,
        q: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Vec<String> {
        let plan = plan_for_with(engine, q, start_ns, end_ns, 20, 16);
        let output = engine
            .search(&plan)
            .await
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        if output.returned == 0 {
            return vec![];
        }
        assert_eq!(output.returned, 1, "{q}: only T1 matches (T2 is a control)");
        let mut names: Vec<String> = output.traces[0]
            .spans
            .iter()
            .map(|s| s.name.clone())
            .collect();
        names.sort();
        names
    }
    // sorted order of the labels: ac6-a < ac6-b < ac6-b2 < ac6-c.
    let matrix: &[(&str, &[&str])] = &[
        // Plain
        (r#"{ .k = "a" } > { .g = "gg" }"#, &["ac6-b", "ac6-b2"]),
        (
            r#"{ .k = "a" } >> { .g = "gg" }"#,
            &["ac6-b", "ac6-b2", "ac6-c"],
        ),
        (r#"{ .k = "b" } < { .h = "hg" }"#, &["ac6-a"]),
        (r#"{ .k = "c" } << { .h = "hg" }"#, &["ac6-a", "ac6-b"]),
        (r#"{ .k = "b" } ~ { .g = "gg" }"#, &["ac6-b2"]),
        // Negated (incl. the empty-LHS edges)
        (r#"{ .k = "a" } !> { .g = "gg" }"#, &["ac6-c"]),
        (
            r#"{ .k = "none" } !> { .g = "gg" }"#,
            &["ac6-b", "ac6-b2", "ac6-c"],
        ),
        (r#"{ .k = "b" } !>> { .g = "gg" }"#, &["ac6-b", "ac6-b2"]),
        (r#"{ .k = "c" } !< { .h = "hg" }"#, &["ac6-a", "ac6-b2"]),
        (r#"{ .k = "c" } !<< { .h = "hg" }"#, &["ac6-b2"]),
        (
            r#"{ .k = "none" } !<< { .h = "hg" }"#,
            &["ac6-a", "ac6-b", "ac6-b2"],
        ),
        (r#"{ .k = "b" } !~ { .g = "gg" }"#, &["ac6-b", "ac6-c"]),
        // Union
        (
            r#"{ .k = "a" } &> { .g = "gg" }"#,
            &["ac6-a", "ac6-b", "ac6-b2"],
        ),
        (
            r#"{ .k = "a" } &>> { .g = "gg" }"#,
            &["ac6-a", "ac6-b", "ac6-b2", "ac6-c"],
        ),
        (r#"{ .k = "b" } &< { .h = "hg" }"#, &["ac6-a", "ac6-b"]),
        (
            r#"{ .k = "c" } &<< { .h = "hg" }"#,
            &["ac6-a", "ac6-b", "ac6-c"],
        ),
        (r#"{ .k = "b" } &~ { .g = "gg" }"#, &["ac6-b", "ac6-b2"]),
        // Self-relating (codex #183 Finding 1): both sides = .g="gg"
        // ({B,C,B2}). C is a proper descendant of a DIFFERENT set member
        // (B), so C is yielded — NOT blanket-excluded for being an LHS
        // match; symmetrically B is a proper ancestor of C.
        (r#"{ .g = "gg" } >> { .g = "gg" }"#, &["ac6-c"]),
        (r#"{ .g = "gg" } << { .g = "gg" }"#, &["ac6-b"]),
        (r#"{ .g = "gg" } !>> { .g = "gg" }"#, &["ac6-b", "ac6-b2"]),
    ];
    for (q, expected) in matrix {
        let names = ac6_search_names(&engine, q, ac6_start, ac6_end).await;
        assert_eq!(&names, expected, "AC6 matrix: {q}");
    }

    // ---- Issue #183 AC9: field-vs-field value correctness --------------
    // The field-compare coercion matrix, VERIFIED against
    // grafana/tempo:3.0.2 for the cross-type case (a string vs a numeric
    // with COINCIDENT text "5" is no match under `=` AND `!=` — Tempo
    // type-gates every operator). Spans: fc-eq (a=b=5 numeric-equal), fc-ne
    // (a=9,b=1 numeric), fc-xt (a="5" STRING vs b=5 numeric — the
    // adversarial coincident-text case ⇒ no match), fc-ab (a=5, b absent ⇒
    // no match), fc-sx (g="apple",h="banana" — both strings, gating LEXICAL
    // string ordering). Broader value-parity vs a data-loaded Tempo remains
    // a #185 close condition (the #180 differential is parse-only), but the
    // cross-type rule is Tempo-verified and correct HERE.
    let fc_st = now + 36 * 3_600_000_000_000;
    const FC_T: &str = "00000000000000000000000000fc0001";
    for spec in [
        ("00000000000000f1", ZERO8, "fc-eq", "svc", fc_st, 0),
        ("00000000000000f2", ZERO8, "fc-ne", "svc", fc_st + 10, 0),
        ("00000000000000f3", ZERO8, "fc-xt", "svc", fc_st + 20, 0),
        ("00000000000000f4", ZERO8, "fc-ab", "svc", fc_st + 30, 0),
        ("00000000000000f5", ZERO8, "fc-sx", "svc", fc_st + 40, 0),
    ] {
        insert_structural_span(&client, FC_T, spec).await;
    }
    // fc-eq: a=5, b=5 (val_num set → numeric-comparable, and equal).
    async fn insert_num_attr(
        client: &ChClient,
        trace_hex: &str,
        span_hex: &str,
        key: &str,
        val: &str,
        num: f64,
        ts: i64,
    ) {
        exec(
            client,
            &format!(
                "INSERT INTO {DB}.trace_attrs_idx \
                 (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
                 SELECT toDate(fromUnixTimestamp64Nano({ts})), '{key}', '{val}', 'span', {num}, {ts}, \
                        toFixedString(unhex('{trace_hex}'), 16), \
                        toFixedString(unhex('{span_hex}'), 8), 1000"
            ),
        )
        .await;
    }
    insert_num_attr(&client, FC_T, "00000000000000f1", "a", "5", 5.0, fc_st).await;
    insert_num_attr(&client, FC_T, "00000000000000f1", "b", "5", 5.0, fc_st).await;
    insert_num_attr(&client, FC_T, "00000000000000f2", "a", "9", 9.0, fc_st + 10).await;
    insert_num_attr(&client, FC_T, "00000000000000f2", "b", "1", 1.0, fc_st + 10).await;
    // fc-xt: a is a STRING "5" (val_num NULL), b is numeric 5 — the
    // adversarial COINCIDENT-text cross-type case; Tempo type-gates ⇒ no
    // match under `=` or `!=`.
    insert_ac6_attr(&client, FC_T, "00000000000000f3", "a", "5", fc_st + 20).await;
    insert_num_attr(&client, FC_T, "00000000000000f3", "b", "5", 5.0, fc_st + 20).await;
    // fc-ab: a present, b absent ⇒ no match (absent key).
    insert_num_attr(&client, FC_T, "00000000000000f4", "a", "5", 5.0, fc_st + 30).await;
    // fc-sx: two string attrs for lexical ordering (apple < banana).
    insert_ac6_attr(&client, FC_T, "00000000000000f5", "g", "apple", fc_st + 40).await;
    insert_ac6_attr(&client, FC_T, "00000000000000f5", "h", "banana", fc_st + 40).await;
    let (fc_start, fc_end) = (fc_st - 1, fc_st + 3_600_000_000_000);
    let plan = plan_for(&engine, r#"{ .a = .b }"#, fc_start, fc_end);
    let output = engine.search(&plan).await.expect("field-compare eq search");
    assert_eq!(
        output.returned, 1,
        "{{ .a = .b }} matches only fc-eq — cross-type coincident text (fc-xt) and absent key \
         (fc-ab) never match"
    );
    assert_eq!(output.traces[0].matched, 1, "only fc-eq, never fc-xt/fc-ab");
    assert_eq!(output.traces[0].spans[0].name, "fc-eq");
    // The demonstrable-bug guard: `!=` must ALSO reject the cross-type
    // coincident-text span (fc-xt), matching only the genuinely-unequal
    // numeric fc-ne (9 != 1).
    let plan = plan_for(&engine, r#"{ .a != .b }"#, fc_start, fc_end);
    let output = engine
        .search(&plan)
        .await
        .expect("field-compare neq search");
    assert_eq!(
        output.returned, 1,
        "{{ .a != .b }} matches only fc-ne — cross-type (fc-xt) never matches != either"
    );
    assert_eq!(output.traces[0].spans[0].name, "fc-ne");
    let plan = plan_for(&engine, r#"{ .a > .b }"#, fc_start, fc_end);
    let output = engine.search(&plan).await.expect("field-compare gt search");
    assert_eq!(output.returned, 1);
    assert_eq!(
        output.traces[0].matched, 1,
        "only fc-ne (9 > 1); fc-xt's cross-type operands block ordering"
    );
    assert_eq!(
        output.traces[0].spans[0].name, "fc-ne",
        "9 > 1 numeric ordering"
    );
    // Lexical string ordering (Tempo-verified): apple < banana matches fc-sx.
    let plan = plan_for(&engine, r#"{ .g < .h }"#, fc_start, fc_end);
    let output = engine.search(&plan).await.expect("string-ordering search");
    assert_eq!(
        output.returned, 1,
        "{{ .g < .h }} matches fc-sx (apple < banana lexically)"
    );
    assert_eq!(output.traces[0].spans[0].name, "fc-sx");

    // ---- Issue #183: the field-vs-field generator granule-prunes on the
    // attribute `(key)` prefix (EXPLAIN indexes=1), never a bare scan.
    let plan = plan_for(&engine, r#"{ .env = .env }"#, base, now);
    let generator = &plan.generator_sqls[0];
    assert!(
        generator.contains("FROM trace_attrs_idx") && generator.contains("key = 'env'"),
        "the field-compare generator must be the key-existence scan:\n{generator}"
    );
    let raw = explain_raw(&client, generator).await;
    let (sel, total) = primary_key_granules(&raw);
    assert!(
        sel < total,
        "the field-compare key-existence generator must prune on the (key) prefix \
         ({sel}/{total}):\n{raw}"
    );

    // ---- Issue #183: a structural search under a tiny scan_budget_rows
    // trips TraceScanBudgetRows for the new forms exactly like #172. ------
    let mut tight183 = engine_config();
    tight183.scan_budget_rows = 1_000;
    let tight183_engine = TraceEngine::new(data_client().await, tight183);
    let plan = plan_for(
        &tight183_engine,
        r#"{ resource.service.name = "checkout" } !> { status = error }"#,
        base,
        now,
    );
    let err = tight183_engine
        .search(&plan)
        .await
        .expect_err("a negated structural span-scan must exceed a 1k-row budget");
    assert!(
        matches!(
            err,
            ReadError::QueryTooBroad(TooBroadReason::TraceScanBudgetRows { budget_rows: 1_000 })
        ),
        "got {err:?}"
    );

    // ---- Issue #184 gate T1: `trace:id =` engages the trace_id PK
    // prefix (`ORDER BY (trace_id, timestamp_ns)`) — Tier-1 EXPLAIN
    // evidence on the 120k corpus: a single trace selects a tiny granule
    // subset. ------------------------------------------------------------
    // Corpus trace ids are leftPad(hex(number), 32, '0'): number 66 = 0x42.
    let corpus_tid = "00000000000000000000000000000042";
    let plan = plan_for(
        &engine,
        &format!(r#"{{ trace:id = "{corpus_tid}" }}"#),
        base,
        now,
    );
    assert_eq!(plan.generator_sqls.len(), 1);
    assert!(
        plan.generator_sqls[0].contains(&format!("trace_id = unhex('{corpus_tid}')")),
        "the PK-prefix predicate: {}",
        plan.generator_sqls[0]
    );
    let raw = explain_raw(&client, &plan.generator_sqls[0]).await;
    let (sel, total) = primary_key_granules(&raw);
    assert!(
        total >= 8 && sel * 4 <= total,
        "trace:id must prune via the trace_id PK prefix to a small granule subset \
         ({sel}/{total}):\n{raw}"
    );
    let output = engine.search(&plan).await.expect("trace:id search");
    assert_eq!(output.returned, 1, "exactly the addressed trace");
    assert_eq!(
        output.traces[0].trace_id[15], 0x42,
        "the addressed trace id round-trips"
    );

    // ---- Issue #184 gate T2: end-to-end trace-level intrinsics on a
    // purpose-built fixture in a disjoint future window — the co-load's
    // window-independence (AC-Δ1a live), the displayed-root agreement
    // (root-less fallback + over-cap byte-capping), statusMessage
    // filtering on the migration-35 column, and the span:id/span:parentID
    // hex comparisons. ----------------------------------------------------
    let g = now + 60 * 3_600_000_000_000;
    const G1: &str = "00000000000000000000000000518401"; // rooted
    const G2: &str = "00000000000000000000000000518402"; // root-less
    const G3: &str = "00000000000000000000000000518403"; // over-cap root
    const R184: &str = "00000000000184a1";
    const C184_1: &str = "00000000000184c1";
    const C184_2: &str = "00000000000184c2";
    /// `(trace, span, parent, name, service, ts, duration, status_message)`.
    async fn insert_184_span(
        client: &ChClient,
        spec: (&str, &str, &str, &str, &str, i64, i64, &str),
    ) {
        let (trace, span, parent, name, service, ts, dur, msg) = spec;
        exec(
            client,
            &format!(
                "INSERT INTO {DB}.trace_spans \
                 (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
                  status_code, status_message, kind, payload_type, payload) \
                 SELECT toFixedString(unhex('{trace}'), 16), \
                        toFixedString(unhex('{span}'), 8), \
                        toFixedString(unhex('{parent}'), 8), \
                        {name}, '{service}', {ts}, {dur}, 0, '{msg}', 1, 1, 'p'"
            ),
        )
        .await;
    }
    let two_hours = 2 * 3_600_000_000_000i64;
    for spec in [
        // G1: root at g, two children at +2h and +2h+60s — the trace's
        // full envelope spans ~2h1m.
        (
            G1,
            R184,
            "0000000000000000",
            "'GET /checkout184'",
            "gw184",
            g,
            5_000_000i64,
            "",
        ),
        (
            G1,
            C184_1,
            R184,
            "'child-op184'",
            "child184",
            g + two_hours,
            1_000,
            "boom-184",
        ),
        (
            G1,
            C184_2,
            R184,
            "'child-op184b'",
            "child184",
            g + two_hours + 60_000_000_000,
            1_000,
            "",
        ),
        // G2: NO zero-parent span (both parents dangle/point inside) —
        // pick_roots and the co-load must agree on the earliest-span
        // fallback.
        (
            G2,
            "00000000000184d1",
            "00000000000000ff",
            "'earliest-184'",
            "svcb184",
            g,
            1_000,
            "",
        ),
        (
            G2,
            "00000000000184d2",
            "00000000000184d1",
            "'later-184'",
            "svcb184",
            g + 10,
            1_000,
            "",
        ),
    ] {
        insert_184_span(&client, spec).await;
    }
    // G3: a root whose name exceeds TRACE_STR_COL_CAP (9000 > 8192 bytes)
    // — both the co-load and the displayed-root path must return the SAME
    // 2048-code-point capped value.
    insert_184_span(
        &client,
        (
            G3,
            "00000000000184e1",
            "0000000000000000",
            "repeat('x', 9000)",
            "over184",
            g,
            1_000,
            "",
        ),
    )
    .await;
    insert_184_span(
        &client,
        (
            G3,
            "00000000000184e2",
            "00000000000184e1",
            "'child-over184'",
            "over184",
            g + 10,
            1_000,
            "",
        ),
    )
    .await;

    let wide = (g - 1_000_000_000, g + 3 * 3_600_000_000_000);
    // A narrow window holding ONLY G1's first child — the root and the
    // trace's max-end span are both OUTSIDE it.
    let narrow = (g + two_hours - 1_000_000_000, g + two_hours + 1_000_000_000);

    // (a) AC-Δ1a live: the sole trace-level predicates resolve the
    // FULL-trace values under the narrow window, and the displayed root
    // is the out-of-window true root (co-load ↔ root_sql agreement).
    for (q, expect_match) in [
        (r#"{ rootServiceName = "gw184" }"#.to_string(), true),
        (r#"{ rootName = "GET /checkout184" }"#.to_string(), true),
        ("{ traceDuration > 1h }".to_string(), true),
        ("{ traceDuration > 3h }".to_string(), false),
        // The windowed view alone could never produce these values.
        (r#"{ rootServiceName = "child184" }"#.to_string(), false),
    ] {
        let plan = plan_for(&engine, &q, narrow.0, narrow.1);
        let output = engine.search(&plan).await.expect("narrow-window search");
        let hit = output.traces.iter().any(|t| t.trace_id[15] == 0x01);
        assert_eq!(hit, expect_match, "{q} (narrow window)");
        if expect_match {
            let t = output
                .traces
                .iter()
                .find(|t| t.trace_id[15] == 0x01)
                .expect("G1");
            assert_eq!(
                t.root.name, "GET /checkout184",
                "{q}: the displayed root is the out-of-window TRUE root"
            );
            assert_eq!(t.root.service, "gw184", "{q}");
        }
    }

    // (b) childCount is full-trace-exact: a window covering the root and
    // ONE child (the second child excluded) must still evaluate the
    // root's FULL child count of 2 — and reject the windowed count of 1.
    let partial_window = (g - 1_000_000_000, g + two_hours + 1_000_000_000);
    let plan = plan_for(
        &engine,
        "{ span:childCount = 2 }",
        partial_window.0,
        partial_window.1,
    );
    let output = engine.search(&plan).await.expect("childCount = 2");
    assert!(
        output.traces.iter().any(|t| t.trace_id[15] == 0x01),
        "the root's FULL-trace child count (2) must match under the partial window"
    );
    let plan = plan_for(
        &engine,
        "{ span:childCount = 1 }",
        partial_window.0,
        partial_window.1,
    );
    let output = engine.search(&plan).await.expect("childCount = 1");
    assert!(
        !output.traces.iter().any(|t| t.trace_id[15] == 0x01),
        "the WINDOWED count (1) must never be the evaluated value"
    );

    // (c) Root-less trace: co-load argMin fallback ↔ pick_roots fallback
    // agreement — the earliest span is the root on BOTH paths.
    let plan = plan_for(&engine, r#"{ rootName = "earliest-184" }"#, wide.0, wide.1);
    let output = engine.search(&plan).await.expect("root-less search");
    let t = output
        .traces
        .iter()
        .find(|t| t.trace_id[15] == 0x02)
        .expect("the root-less trace must match via the earliest-span fallback");
    assert_eq!(t.root.name, "earliest-184", "displayed root agrees");
    let plan = plan_for(&engine, r#"{ rootName = "later-184" }"#, wide.0, wide.1);
    let output = engine.search(&plan).await.expect("root-less negative");
    assert!(
        !output.traces.iter().any(|t| t.trace_id[15] == 0x02),
        "the later span is NOT the fallback root on either path"
    );

    // (d) Over-cap root: the co-load's evaluated value and the displayed
    // root are BOTH the 2048-code-point capped rendering — byte-identical
    // (AC-Δ1a over-cap fixture; the substringUTF8 fallback branch).
    let capped = "x".repeat(2048);
    let plan_q = format!(r#"{{ rootName = "{capped}" }}"#);
    let plan = plan_for(&engine, &plan_q, wide.0, wide.1);
    let output = engine.search(&plan).await.expect("over-cap search");
    let t = output
        .traces
        .iter()
        .find(|t| t.trace_id[15] == 0x03)
        .expect("the capped root name must match the co-load value");
    assert_eq!(
        t.root.name, capped,
        "the displayed root carries the IDENTICAL capped value"
    );
    let raw_q = format!(r#"{{ rootName = "{}" }}"#, "x".repeat(9000));
    let plan = plan_for(&engine, &raw_q, wide.0, wide.1);
    let output = engine.search(&plan).await.expect("raw over-cap search");
    assert!(
        !output.traces.iter().any(|t| t.trace_id[15] == 0x03),
        "the RAW (uncapped) value matches on neither path"
    );

    // (e) statusMessage on the migration-35 column: equality + regex +
    // the scoped spelling, and the matched spanset is exactly the
    // carrying span.
    for q in [
        r#"{ statusMessage = "boom-184" }"#,
        r#"{ span:statusMessage = "boom-184" }"#,
        r#"{ statusMessage =~ "boom.*" }"#,
    ] {
        let plan = plan_for(&engine, q, wide.0, wide.1);
        let output = engine.search(&plan).await.expect("statusMessage search");
        assert_eq!(output.returned, 1, "{q}");
        assert_eq!(output.traces[0].trace_id[15], 0x01, "{q}");
        assert_eq!(output.traces[0].matched, 1, "{q}");
        assert_eq!(output.traces[0].spans[0].name, "child-op184", "{q}");
    }

    // (e2) Over-cap statusMessage (issue #184 code review): Phase-1
    // candidate selection must compare the SAME byte-capped value Phase 2
    // hydrates and evaluates. G4's stored message exceeds
    // TRACE_STR_COL_CAP (9004 > 8192 bytes: 9000 y's + 'TAIL'), so its
    // capped rendering is exactly 2048 y's — a raw Phase-1 comparison
    // never selects the candidate even though Phase 2 would match it.
    const G4: &str = "00000000000000000000000000518404";
    exec(
        &client,
        &format!(
            "INSERT INTO {DB}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, status_message, kind, payload_type, payload) \
             SELECT toFixedString(unhex('{G4}'), 16), \
                    toFixedString(unhex('00000000000184f1'), 8), \
                    toFixedString(unhex('0000000000000000'), 8), \
                    'overmsg-op184', 'overmsg184', {g}, 1000, 2, \
                    concat(repeat('y', 9000), 'TAIL'), 1, 1, 'p'"
        ),
    )
    .await;
    // Equality on the capped literal: Phase 1 selects the candidate AND
    // Phase 2 matches it — the two phases agree on the capped value.
    let capped_msg = "y".repeat(2048);
    let q = format!(r#"{{ statusMessage = "{capped_msg}" }}"#);
    let plan = plan_for(&engine, &q, wide.0, wide.1);
    let output = engine
        .search(&plan)
        .await
        .expect("over-cap statusMessage equality search");
    let t = output
        .traces
        .iter()
        .find(|t| t.trace_id[15] == 0x04)
        .expect("Phase 1 must select the over-cap trace on the CAPPED value");
    assert_eq!(t.matched, 1, "Phase 2 matches the same capped value");
    assert_eq!(t.spans[0].name, "overmsg-op184");
    assert!(
        plan.generator_sqls[0].contains("substringUTF8(status_message, 1, 2048)"),
        "the Phase-1 predicate compares the capped column:\n{}",
        plan.generator_sqls[0]
    );
    // The regex form runs on the capped value in BOTH phases too: the
    // anchored `y+` matches the all-y capped rendering but NOT the raw
    // stored value (which ends in 'TAIL').
    let plan = plan_for(&engine, r#"{ statusMessage =~ "y+" }"#, wide.0, wide.1);
    let output = engine
        .search(&plan)
        .await
        .expect("over-cap statusMessage regex search");
    assert!(
        output.traces.iter().any(|t| t.trace_id[15] == 0x04),
        "the anchored regex matches the capped rendering on both phases"
    );
    // The RAW (uncapped) value matches in neither phase.
    let raw_msg = format!("{}TAIL", "y".repeat(9000));
    let plan = plan_for(
        &engine,
        &format!(r#"{{ statusMessage = "{raw_msg}" }}"#),
        wide.0,
        wide.1,
    );
    let output = engine
        .search(&plan)
        .await
        .expect("raw over-cap statusMessage search");
    assert!(
        !output.traces.iter().any(|t| t.trace_id[15] == 0x04),
        "the RAW (uncapped) value matches in neither phase"
    );

    // (f) span:id / span:parentID hex comparisons (case-insensitive Eq).
    let plan = plan_for(
        &engine,
        r#"{ span:id = "00000000000184C1" }"#, // uppercase query hex
        wide.0,
        wide.1,
    );
    let output = engine.search(&plan).await.expect("span:id search");
    assert_eq!(output.returned, 1);
    assert_eq!(output.traces[0].matched, 1);
    assert_eq!(output.traces[0].spans[0].name, "child-op184");
    let plan = plan_for(
        &engine,
        r#"{ span:parentID = "00000000000184a1" }"#,
        wide.0,
        wide.1,
    );
    let output = engine.search(&plan).await.expect("span:parentID search");
    assert_eq!(output.returned, 1);
    assert_eq!(
        output.traces[0].matched, 2,
        "both direct children of the G1 root match"
    );

    // (g) Issue #192 PR-B: span-event search-value coverage + the AC8 hard
    // namespace partition, proven at the RESULT-SET level across TWO SEPARATE
    // spans (plan v2 Δ1 — a sender attribute keyed `name` can NEVER satisfy
    // the `event:name` intrinsic, and vice-versa):
    //  - Span P: one event whose INTRINSIC `event:name` = "A" (under the
    //    dedicated `scope='event:intrinsic'`), plus `timeSinceStart` = 3 ms
    //    (via `val_num`) and a verbatim `exception.type` = "IOError" attribute
    //    (`scope='event'`). P carries NO user attribute keyed `name`.
    //  - Span Q (distinct trace): one event-scoped user attribute literally
    //    keyed `name` = "B" (`event."name"`, `scope='event'`), and NO
    //    intrinsic name "B".
    const EP: &str = "000000000000000000000000000e1921"; // P: trace_id[15] = 0x21
    const EP_SPAN: &str = "0000000000e19201";
    const EQ: &str = "000000000000000000000000000e1922"; // Q: trace_id[15] = 0x22
    const EQ_SPAN: &str = "0000000000e19202";
    async fn insert_event_span(
        client: &ChClient,
        ts: i64,
        trace_hex: &str,
        span_hex: &str,
        name: &str,
    ) {
        exec(
            client,
            &format!(
                "INSERT INTO {DB}.trace_spans \
                 (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
                  status_code, kind, payload_type, payload) \
                 SELECT toFixedString(unhex('{trace_hex}'), 16), \
                        toFixedString(unhex('{span_hex}'), 8), \
                        toFixedString(unhex('0000000000000000'), 8), \
                        '{name}', 'evt192', {ts}, 1000, 0, 1, 1, 'p'"
            ),
        )
        .await;
    }
    async fn insert_event_attr(
        client: &ChClient,
        ts: i64,
        ids: (&str, &str),
        key: &str,
        val: &str,
        scope: &str,
        val_num: &str,
    ) {
        let (trace_hex, span_hex) = ids;
        exec(
            client,
            &format!(
                "INSERT INTO {DB}.trace_attrs_idx \
                 (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
                 SELECT toDate(fromUnixTimestamp64Nano({ts})), '{key}', '{val}', '{scope}', \
                        {val_num}, {ts}, toFixedString(unhex('{trace_hex}'), 16), \
                        toFixedString(unhex('{span_hex}'), 8), 1000"
            ),
        )
        .await;
    }
    insert_event_span(&client, g, EP, EP_SPAN, "event-span-p").await;
    insert_event_attr(
        &client,
        g,
        (EP, EP_SPAN),
        "name",
        "A",
        "event:intrinsic",
        "NULL",
    )
    .await;
    insert_event_attr(
        &client,
        g,
        (EP, EP_SPAN),
        "timeSinceStart",
        "3000000",
        "event:intrinsic",
        "3000000",
    )
    .await;
    insert_event_attr(
        &client,
        g,
        (EP, EP_SPAN),
        "exception.type",
        "IOError",
        "event",
        "NULL",
    )
    .await;
    insert_event_span(&client, g, EQ, EQ_SPAN, "event-span-q").await;
    insert_event_attr(&client, g, (EQ, EQ_SPAN), "name", "B", "event", "NULL").await;

    // Each construct returns EXACTLY span P (index-served), never span Q.
    for q in [
        r#"{ event:name = "A" }"#,
        r#"{ event:timeSinceStart > 1ms }"#,
        r#"{ event.exception.type = "IOError" }"#,
    ] {
        let plan = plan_for(&engine, q, wide.0, wide.1);
        let output = engine.search(&plan).await.expect("event search");
        assert_eq!(output.returned, 1, "{q}");
        assert_eq!(output.traces[0].trace_id[15], 0x21, "{q}");
        assert_eq!(output.traces[0].matched, 1, "{q}");
        assert_eq!(output.traces[0].spans[0].name, "event-span-p", "{q}");
    }

    // timeSinceStart is a real numeric comparison: 3 ms does NOT satisfy > 5 ms.
    let plan = plan_for(&engine, r#"{ event:timeSinceStart > 5ms }"#, wide.0, wide.1);
    let output = engine.search(&plan).await.expect("event tss negative");
    assert!(
        !output.traces.iter().any(|t| t.trace_id[15] == 0x21),
        "3 ms timeSinceStart must not match > 5 ms"
    );

    // The AC8 hard partition, proven as DISJOINT RESULT SETS: the intrinsic
    // namespace (`event:name`, `scope='event:intrinsic'`) and the user
    // attribute namespace (`event."name"`, `scope='event'`) never cross.
    async fn event_hits(engine: &TraceEngine, q: &str, start: i64, end: i64) -> Vec<u8> {
        let plan = plan_for(engine, q, start, end);
        let output = engine
            .search(&plan)
            .await
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        let mut ids: Vec<u8> = output.traces.iter().map(|t| t.trace_id[15]).collect();
        ids.sort_unstable();
        ids
    }
    // The intrinsic query resolves EXACTLY P (the intrinsic name), never Q's
    // same-keyed user attribute.
    assert_eq!(
        event_hits(&engine, r#"{ event:name = "A" }"#, wide.0, wide.1).await,
        vec![0x21],
        "event:name intrinsic returns ONLY span P"
    );
    // The quoted user attribute resolves EXACTLY Q, never P's intrinsic.
    assert_eq!(
        event_hits(&engine, r#"{ event."name" = "B" }"#, wide.0, wide.1).await,
        vec![0x22],
        r#"event."name" attribute returns ONLY span Q"#
    );
    // Cross-namespace value lookups match NOTHING: "B" is only a user
    // attribute (never an intrinsic), "A" is only an intrinsic (never an
    // attribute).
    assert!(
        event_hits(&engine, r#"{ event:name = "B" }"#, wide.0, wide.1)
            .await
            .is_empty(),
        "the user-attribute value B never satisfies the event:name intrinsic"
    );
    assert!(
        event_hits(&engine, r#"{ event."name" = "A" }"#, wide.0, wide.1)
            .await
            .is_empty(),
        r#"the intrinsic value A never satisfies the event."name" attribute"#
    );

    // (h) Issue #192 PR-C: span-link search-value coverage + the AC8 hard
    // namespace partition, proven as DISJOINT RESULT SETS across TWO SEPARATE
    // spans (plan v2 Δ1 — a sender attribute keyed `spanID` can NEVER satisfy
    // the `link:spanID` intrinsic, and vice-versa).
    //
    // AC8 differential scope (identical bar to PR-A/PR-B, per the accepted
    // events precedent): the search-value parity for the new constructs is
    // proven by THIS hermetic disjoint-partition test plus the parse-level
    // Tempo differential (automatic via the disposition registry —
    // `tempo_differential.rs`). A dedicated `compare()`-value differential
    // exists ONLY where a `compare()` key exists (instrumentation had two;
    // events and links have none — they are not `compare()` targets), so links
    // add no new differential file, exactly as span events (PR-B) did not.
    //
    // The seeded spans:
    //  - Span P: one link whose INTRINSIC `link:spanID` = "0a1b2c3d4e5f6071"
    //    and `link:traceID` = "aabbccddeeff00112233445566778899" (lowercase
    //    hex, under `scope='link:intrinsic'`), plus a verbatim `relation` =
    //    "child_of" attribute (`scope='link'`). P carries NO user attribute
    //    keyed `spanID`.
    //  - Span Q (distinct trace): one link-scoped user attribute literally
    //    keyed `spanID` = "shadowval" (`link."spanID"`, `scope='link'`), and
    //    NO intrinsic.
    const LP: &str = "000000000000000000000000000e1931"; // P: trace_id[15] = 0x31
    const LP_SPAN: &str = "0000000000e19301";
    const LQ: &str = "000000000000000000000000000e1932"; // Q: trace_id[15] = 0x32
    const LQ_SPAN: &str = "0000000000e19302";
    const LINK_SPAN_HEX: &str = "0a1b2c3d4e5f6071";
    const LINK_TRACE_HEX: &str = "aabbccddeeff00112233445566778899";
    // insert_event_span / insert_event_attr are generic trace_spans /
    // trace_attrs_idx inserters (scope is a parameter) — reused verbatim for
    // the link rows.
    insert_event_span(&client, g, LP, LP_SPAN, "link-span-p").await;
    insert_event_attr(
        &client,
        g,
        (LP, LP_SPAN),
        "spanID",
        LINK_SPAN_HEX,
        "link:intrinsic",
        "NULL",
    )
    .await;
    insert_event_attr(
        &client,
        g,
        (LP, LP_SPAN),
        "traceID",
        LINK_TRACE_HEX,
        "link:intrinsic",
        "NULL",
    )
    .await;
    insert_event_attr(
        &client,
        g,
        (LP, LP_SPAN),
        "relation",
        "child_of",
        "link",
        "NULL",
    )
    .await;
    insert_event_span(&client, g, LQ, LQ_SPAN, "link-span-q").await;
    insert_event_attr(
        &client,
        g,
        (LQ, LQ_SPAN),
        "spanID",
        "shadowval",
        "link",
        "NULL",
    )
    .await;

    // Each construct returns EXACTLY span P (index-served), never span Q.
    for q in [
        format!(r#"{{ link:spanID = "{LINK_SPAN_HEX}" }}"#),
        format!(r#"{{ link:traceID = "{LINK_TRACE_HEX}" }}"#),
        r#"{ link.relation = "child_of" }"#.to_string(),
    ] {
        let plan = plan_for(&engine, &q, wide.0, wide.1);
        let output = engine.search(&plan).await.expect("link search");
        assert_eq!(output.returned, 1, "{q}");
        assert_eq!(output.traces[0].trace_id[15], 0x31, "{q}");
        assert_eq!(output.traces[0].matched, 1, "{q}");
        assert_eq!(output.traces[0].spans[0].name, "link-span-p", "{q}");
    }

    // The AC8 hard partition, proven as DISJOINT RESULT SETS: the intrinsic
    // namespace (`link:spanID`, `scope='link:intrinsic'`) and the user
    // attribute namespace (`link."spanID"`, `scope='link'`) never cross.
    // The intrinsic query resolves EXACTLY P (the intrinsic id), never Q's
    // same-keyed user attribute.
    assert_eq!(
        event_hits(
            &engine,
            &format!(r#"{{ link:spanID = "{LINK_SPAN_HEX}" }}"#),
            wide.0,
            wide.1
        )
        .await,
        vec![0x31],
        "link:spanID intrinsic returns ONLY span P"
    );
    // The quoted user attribute resolves EXACTLY Q, never P's intrinsic.
    assert_eq!(
        event_hits(
            &engine,
            r#"{ link."spanID" = "shadowval" }"#,
            wide.0,
            wide.1
        )
        .await,
        vec![0x32],
        r#"link."spanID" attribute returns ONLY span Q"#
    );
    // Cross-namespace value lookups match NOTHING: "shadowval" is only a user
    // attribute (never an intrinsic), the hex id is only an intrinsic (never a
    // user attribute).
    assert!(
        event_hits(&engine, r#"{ link:spanID = "shadowval" }"#, wide.0, wide.1)
            .await
            .is_empty(),
        "the user-attribute value shadowval never satisfies the link:spanID intrinsic"
    );
    assert!(
        event_hits(
            &engine,
            &format!(r#"{{ link."spanID" = "{LINK_SPAN_HEX}" }}"#),
            wide.0,
            wide.1
        )
        .await
        .is_empty(),
        r#"the intrinsic id never satisfies the link."spanID" attribute"#
    );

    // Review finding: `link:spanID`/`link:traceID` matching is CASE-INSENSITIVE,
    // consistent with `span:id`/`trace:id` — an UPPERCASE-hex literal is
    // lowercased at compile time and resolves the seeded (lowercase-hex-stored)
    // span P, rather than silently returning zero rows.
    assert_eq!(
        event_hits(
            &engine,
            &format!(r#"{{ link:spanID = "{}" }}"#, LINK_SPAN_HEX.to_uppercase()),
            wide.0,
            wide.1
        )
        .await,
        vec![0x31],
        "an uppercase-hex link:spanID literal must resolve the lowercase-hex-stored span P"
    );
    assert_eq!(
        event_hits(
            &engine,
            &format!(
                r#"{{ link:traceID = "{}" }}"#,
                LINK_TRACE_HEX.to_uppercase()
            ),
            wide.0,
            wide.1
        )
        .await,
        vec![0x31],
        "an uppercase-hex link:traceID literal must resolve the lowercase-hex-stored span P"
    );
}
