//! Issue #57 AC2/AC6 (Tier-1, scale-invariant): live gates for the
//! two-phase TraceQL search against ClickHouse 24.8.
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
//! - **AC6** — the scan budget trips for real (a fallback-generator
//!   search under a tiny `scan_budget_rows` → 158 →
//!   `TooBroadReason::TraceScanBudgetRows`); the Layer-1 result-byte
//!   ceiling trips for real (oversized string payloads → 396 → 422); a
//!   per-trace span overflow (> `MAX_SPANS_PER_TRACE` in-window spans)
//!   truncates and marks the response partial.
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
        max_candidates: 100_000,
        scan_budget_rows: 50_000_000,
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
    let query = pulsus_traceql::parse(q).expect("query parses");
    plan_search(
        &query,
        &SearchParams {
            start_ns,
            end_ns,
            limit: 20,
            spss: 3,
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
    assert!(
        row.read_rows < CORPUS_SPANS / 5,
        "the projection read must touch a small fraction of the corpus \
         (read {} of {CORPUS_SPANS})",
        row.read_rows
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
    assert!(
        limited.read_rows <= CORPUS_SPANS + CORPUS_SPANS / 4,
        "the read stays confined to the (key, val) prefix (+granule slop), never \
         the whole 3-key attr table (read {} of {} attr rows)",
        limited.read_rows,
        3 * CORPUS_SPANS
    );

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

    // ---- AC6 (b): the Layer-2 retention counter trips for real ---------
    // One trace with 300 spans × 900 KB names in a disjoint future
    // window: a single hydration batch would accumulate ~270 MB of
    // unbounded-String rows. ClickHouse 24.8 does NOT throw
    // `max_result_bytes` on streamed SELECT shapes (verified against a
    // live 24.8 — it throws only on aggregated results), which is exactly
    // why the final plan amendment makes the Rust retention counter the
    // BINDING bound on accumulated state: the engine charges every row as
    // it streams and trips the 256 MiB budget mid-stream → 422.
    let big_start = now + 3_600_000_000_000;
    exec(
        &client,
        &format!(
            "INSERT INTO {DB}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT \
               toFixedString(unhex(leftPad(lower(hex(1000000)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               toFixedString(unhex('0000000000000000'), 8), \
               repeat('x', 900000), 'bulky', {big_start} + toInt64(number), 1000, 0, 1, 1, 'p' \
             FROM numbers(300)"
        ),
    )
    .await;
    let plan = plan_for(&engine, "{}", big_start - 1, big_start + 1_000_000_000);
    let err = engine
        .search(&plan)
        .await
        .expect_err("an oversized-string hydration must trip the retention counter");
    match err {
        ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { budget_bytes, .. }) => {
            assert_eq!(budget_bytes, pulsus_read::HYDRATION_BYTE_BUDGET as u64);
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
}
