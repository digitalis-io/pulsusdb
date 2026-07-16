//! Issue #59 AC3 (Tier-1, scale-invariant): live `EXPLAIN indexes = 1`
//! gates for the TraceQL metrics pushdown against ClickHouse 24.8, on
//! the **real** generated SQL (`plan_trace_metrics` output is the exact
//! execution shape):
//!
//! - a service-equality metric selects the `service_time` projection
//!   (PREWHERE hoist), corroborated through `system.query_log.projections`
//!   after a real execution;
//! - an attribute-filter metric's semi-join subquery is index-served:
//!   granule pruning on the `trace_attrs_idx` `(key, val)` prefix, with
//!   time pruning isolated within one dense fixed prefix (narrow window
//!   → strictly fewer granules, the issue #53 AC3b pattern);
//! - the scan budget trips for real (tiny `scan_budget_rows` → code 158
//!   → `TooBroadReason::TraceScanBudgetRows`);
//! - the semi-join IN-set budget trips for real (> `TRACE_METRICS_MAX_SET_ROWS`
//!   matching attr rows → code 191 → the dedicated
//!   `TooBroadReason::TraceMetricsSetRows`, plan v2 delta 3's "confirm
//!   the exact 24.8 code" mandate).
//!
//! Corpus: the search-explain fixture shape (≥100k time-spread spans,
//! ≤5% target service — issue #53's binding requirements for the
//! data-dependent 24.8 optimizer). Live-gated behind
//! `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_metrics_explain
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::logql::{ReadError, TooBroadReason};
use pulsus_read::traces::metrics_plan::{MetricsParams, plan_trace_metrics};
use pulsus_read::{
    QueryResult, TRACE_METRICS_MAX_SET_ROWS, TraceEngine, TraceMetricsPlan, TraceReadConfig,
};
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

const DB: &str = "pulsus_traces_metrics_expl_it";
/// ≥100k time-spread spans (issue #53 fixture floor).
const CORPUS_SPANS: u64 = 120_000;
/// The default MergeTree index granularity: reads quantize to whole
/// granules of this many rows per part, so `read_rows` bounds must
/// budget in granule multiples (issue #60 CI flake on the sibling
/// `traces_search_explain` suite — see the gate-1 comment below).
const GRANULE_ROWS: u64 = 8_192;
/// The whole corpus spans 47h ending "now".
const WINDOW_NS: i64 = 47 * 3_600 * 1_000_000_000;
/// `checkout` frequency: 1-in-50 spans (2% ≤ the 5% ceiling).
const CHECKOUT_EVERY: u64 = 50;
/// `http.status_code = 500` frequency: 1%.
const ERROR_EVERY: u64 = 100;

const NS_PER_S: i64 = 1_000_000_000;

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

/// Seeds the AC3 corpus (the search-explain fixture, issue #57):
/// `CORPUS_SPANS` single-span traces over 47h, the dense `env=prod`
/// resource prefix on every span (the time-pruning-isolation fixture),
/// and the 1% `http.status_code=500` numeric target.
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
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainRow {
    #[serde(with = "serde_bytes")]
    explain: Vec<u8>,
}

async fn explain_raw(client: &ChClient, sql: &str) -> String {
    // The engine doubles literal `?` at its own execution boundary
    // (`escape_query_placeholders`); this raw EXPLAIN path must apply the
    // same driver-quirk fix (regex fragments carry `(?:`).
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

/// The `PrimaryKey` `Granules: k/N` ratio of the `ReadFromMergeTree`
/// block reading `table` — the metrics EXPLAIN carries two read blocks
/// (the outer `trace_spans` scan and the semi-join's `trace_attrs_idx`
/// subquery), so the parse is scoped to the named table's section.
fn table_primary_key_granules(raw: &str, table: &str) -> (u64, u64) {
    const BLOCK_TITLES: &[&str] = &["MinMax", "Partition", "PrimaryKey", "Skip"];
    let mut in_table = false;
    let mut in_pk = false;
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("ReadFromMergeTree") {
            in_table = trimmed.contains(table);
            in_pk = false;
            continue;
        }
        if !in_table {
            continue;
        }
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
    panic!("no PrimaryKey Granules line for table {table:?} in EXPLAIN output:\n{raw}");
}

/// Extracts the REAL embedded semi-join subquery (`SELECT trace_id,
/// span_id FROM trace_attrs_idx …`) from a generated metrics SQL — byte
/// identical to what ClickHouse executes under `CreatingSets`, whose
/// child plan `EXPLAIN indexes = 1` does not render on 24.8 (verified
/// live: the outer explain shows only "Create sets before main query
/// execution"), so the subquery is explained standalone.
fn extract_semi_join_subquery(sql: &str) -> String {
    let start = sql
        .find("IN (SELECT")
        .unwrap_or_else(|| panic!("no semi-join in SQL:\n{sql}"))
        + "IN (".len();
    let bytes = sql.as_bytes();
    let mut depth = 1usize;
    for (offset, b) in bytes[start..].iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return sql[start..start + offset].to_string();
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced semi-join parens in SQL:\n{sql}");
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

fn plan_for(engine: &TraceEngine, q: &str, start_ns: i64, end_ns: i64) -> TraceMetricsPlan {
    let query = pulsus_traceql::parse(q).expect("query parses");
    plan_trace_metrics(
        &query,
        &MetricsParams {
            start_ns,
            end_ns,
            step_s: 3_600,
        },
        &engine.metrics_ctx(),
    )
    .expect("query plans")
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct QueryLogRow {
    read_rows: u64,
    projections: Vec<String>,
}

/// The most recent `QueryFinish` row matching every SQL fragment.
async fn query_log_like(client: &ChClient, like_fragments: &[&str]) -> Option<QueryLogRow> {
    let mut predicate = format!("type = 'QueryFinish' AND current_database = '{DB}'");
    for fragment in like_fragments {
        predicate.push_str(&format!(" AND query LIKE '%{fragment}%'"));
    }
    let sql = format!(
        "SELECT read_rows, projections FROM system.query_log \
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

/// One `#[tokio::test]` running every gate in sequence — the corpus is
/// seeded once.
#[tokio::test]
async fn metrics_explain_and_budget_gates() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-read/tests/traces_metrics_explain.rs for setup)"
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

    // ---- AC3 gate 1: the service PREWHERE hoist selects service_time ---
    let plan = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 } | rate()"#,
        base,
        now,
    );
    assert!(
        plan.range_sql().contains("PREWHERE service = 'checkout'"),
        "the hoist is in the generated SQL:\n{}",
        plan.range_sql()
    );
    let raw = explain_raw(&client, plan.range_sql()).await;
    assert!(
        raw.contains("service_time"),
        "the service-equality metric must select the service_time projection:\n{raw}"
    );
    // Execute the REAL query, then corroborate via query_log.
    let result = engine.metrics_range(&plan).await.expect("range executes");
    match &result {
        QueryResult::Matrix(series) => assert_eq!(series.len(), 1, "matching spans exist"),
        other => panic!("expected a matrix, got {other:?}"),
    }
    exec(&client, "SYSTEM FLUSH LOGS").await;
    let row = query_log_like(
        &client,
        &[
            "PREWHERE service = \\'checkout\\'",
            "uniqExact(trace_id, span_id)",
        ],
    )
    .await
    .expect("the metrics query's QueryFinish row must exist");
    assert!(
        row.projections.iter().any(|p| p.contains("service_time")),
        "query_log.projections must name service_time, got {:?}",
        row.projections
    );
    // read_rows covers BOTH reads: the semi-join's key prefix is the
    // dense http.status_code key (~CORPUS_SPANS attr rows, the documented
    // key-only-scan honesty note), so the spans side must contribute only
    // the projection's small service prefix on top — without the
    // projection the spans side alone would add another full CORPUS_SPANS.
    //
    // Granule-aware bound — do NOT re-tighten (issue #60 CI flake, run
    // 29469732884, on the sibling search suite's identical projection
    // shape): both reads quantize to 8,192-row granules per part. The
    // attr key prefix is CORPUS_SPANS rows plus up to ~6 padding
    // granules across the layout's parts; the spans-side projection
    // prefix is ~2,400 matched rows that CI layouts have realized as
    // ~26k read rows (3-4 granules/parts' worth). The old
    // CORPUS_SPANS / 4 (30,000) slop left only ~4k of headroom on that
    // observed CI layout. CORPUS_SPANS / 2 (60,000 ≈ 7.3 granules)
    // absorbs both quantization terms while an unprojected spans-side
    // full scan (another whole CORPUS_SPANS → ≥ 240k total) still fails
    // this gate by a wide margin.
    assert!(
        row.read_rows < CORPUS_SPANS + CORPUS_SPANS / 2,
        "the spans side must be served by the service_time projection's prefix, not a \
         full scan (read {} total; attr key prefix alone is ~{CORPUS_SPANS}, bound adds \
         {} ≈ 7 granules of {GRANULE_ROWS} rows of quantization headroom)",
        row.read_rows,
        CORPUS_SPANS / 2
    );

    // ---- AC3 gate 2: the attr semi-join subquery prunes on the
    // (key, val) prefix, with time pruning isolated within the dense
    // env=prod prefix (issue #53 AC3b pattern). --------------------------
    let full_plan = plan_for(&engine, r#"{ .env = "prod" } | rate()"#, base, now);
    let narrow_plan = plan_for(
        &engine,
        r#"{ .env = "prod" } | rate()"#,
        now - 30 * 60 * NS_PER_S,
        now,
    );
    let (full_sel, full_total) = table_primary_key_granules(
        &explain_raw(&client, &extract_semi_join_subquery(full_plan.range_sql())).await,
        "trace_attrs_idx",
    );
    let (narrow_sel, _) = table_primary_key_granules(
        &explain_raw(
            &client,
            &extract_semi_join_subquery(narrow_plan.range_sql()),
        )
        .await,
        "trace_attrs_idx",
    );
    assert!(
        full_sel <= full_total && full_sel > 0,
        "the semi-join's prefix read must engage the attr primary key ({full_sel}/{full_total})"
    );
    assert!(
        narrow_sel < full_sel,
        "the narrow window must prune strictly fewer granules within the SAME dense \
         (key, val) prefix — time pruning isolated (narrow {narrow_sel} vs full {full_sel})"
    );
    // And the key-only numeric class prunes on its (key) prefix too.
    let plan = plan_for(
        &engine,
        "{ span.http.status_code >= 500 } | rate()",
        base,
        now,
    );
    let raw = explain_raw(&client, &extract_semi_join_subquery(plan.range_sql())).await;
    let (sel, total) = table_primary_key_granules(&raw, "trace_attrs_idx");
    assert!(
        sel < total,
        "the key-only numeric semi-join must prune on the (key) prefix ({sel}/{total}):\n{raw}"
    );

    // ---- AC3 gate 3: the scan budget trips for real → 158 → 422 -------
    let mut tight = engine_config();
    tight.scan_budget_rows = 1_000;
    let tight_engine = TraceEngine::new(data_client().await, tight);
    let plan = plan_for(&tight_engine, "{} | rate()", base, now);
    let err = tight_engine
        .metrics_range(&plan)
        .await
        .expect_err("a match-all metric over 120k spans must exceed a 1k-row budget");
    match err {
        ReadError::QueryTooBroad(TooBroadReason::TraceScanBudgetRows { budget_rows }) => {
            assert_eq!(budget_rows, 1_000);
        }
        other => panic!("expected TraceScanBudgetRows, got {other:?}"),
    }

    // ---- AC3 gate 4: the IN-set budget trips for real → code 191 → the
    // dedicated TraceMetricsSetRows (plan v2 delta 3's code-confirmation
    // mandate). Seed TRACE_METRICS_MAX_SET_ROWS + 50k in-window rows of
    // one key: the semi-join's materialized set overflows. ---------------
    let bulk_rows = TRACE_METRICS_MAX_SET_ROWS + 50_000;
    exec(
        &client,
        &format!(
            "INSERT INTO {DB}.trace_attrs_idx \
             (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
             SELECT \
               toDate(fromUnixTimestamp64Nano({base} + toInt64(number))), \
               'bulk', 'x', 'span', NULL, \
               {base} + toInt64(number), \
               toFixedString(unhex(leftPad(lower(hex(number + 5000000)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               1000000 \
             FROM numbers({bulk_rows})"
        ),
    )
    .await;
    let plan = plan_for(&engine, r#"{ span.bulk = "x" } | rate()"#, base, now);
    let err = engine
        .metrics_range(&plan)
        .await
        .expect_err("a semi-join set past max_rows_in_set must throw");
    match err {
        ReadError::QueryTooBroad(TooBroadReason::TraceMetricsSetRows { max_set_rows }) => {
            assert_eq!(max_set_rows, TRACE_METRICS_MAX_SET_ROWS);
        }
        other => panic!("expected TraceMetricsSetRows (code 191), got {other:?}"),
    }
    // The instant form carries the same settings — same rejection.
    let err = engine
        .metrics_instant(&plan)
        .await
        .expect_err("the instant form carries the same set limits");
    assert!(matches!(
        err,
        ReadError::QueryTooBroad(TooBroadReason::TraceMetricsSetRows { .. })
    ));
}
