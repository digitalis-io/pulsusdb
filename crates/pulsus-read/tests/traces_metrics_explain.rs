//! Issue #59 AC3 (Tier-1, scale-invariant): live `EXPLAIN indexes = 1`
//! gates for the TraceQL metrics pushdown against ClickHouse 26.3, on
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
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_metrics_explain
//! podman rm -f pulsus-ch-test
//! ```

use std::collections::BTreeSet;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::logql::{ReadError, TooBroadReason};
use pulsus_read::traces::log2_histogram;
use pulsus_read::traces::metrics_plan::{MetricsParams, plan_trace_metrics};
use pulsus_read::{TRACE_METRICS_MAX_SET_ROWS, TraceEngine, TraceMetricsPlan, TraceReadConfig};
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

static DB: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_traces_metrics_expl_it");
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

/// Isolates the DATE-BOUNDED base `trace_spans` scan (the `raw` inner
/// select) from a compare cross-tab SQL — the first bucketed
/// `SELECT … FROM trace_spans WHERE timestamp_ns …` up to its dedup
/// `GROUP BY t, trace_id, span_id`. Deliberately the base read, NOT the
/// window-free roots `argMin` read (which is intentionally unpruned): the
/// two are distinct trace_spans reads and only the base one is gated for
/// window pruning. The extracted scan explains cleanly standalone (its
/// `is_sel` semi-join is a `CreatingSets` child).
fn extract_compare_base_scan(cross: &str) -> String {
    let start = cross
        .find("SELECT toUnixTimestamp64Milli")
        .unwrap_or_else(|| panic!("no base scan in compare SQL:\n{cross}"));
    let rel_end = cross[start..]
        .find("\n  )\n  GROUP BY t, trace_id, span_id")
        .unwrap_or_else(|| panic!("no base-scan terminator in compare SQL:\n{cross}"));
    cross[start..start + rel_end].to_string()
}

fn engine_config() -> TraceReadConfig {
    TraceReadConfig {
        // Issue #398: the per-query ClickHouse memory ceiling; the
        // production default, so this fixture keeps today's behaviour.
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
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

fn plan_for(engine: &TraceEngine, q: &str, start_ns: i64, end_ns: i64) -> TraceMetricsPlan {
    let query = pulsus_traceql::parse(q).expect("query parses");
    plan_trace_metrics(
        &query,
        &MetricsParams {
            start_ns,
            end_ns,
            step_ms: 3_600_000,
            exemplars: None,
        },
        &engine.metrics_ctx(),
    )
    .expect("query plans")
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct QueryLogRow {
    read_rows: u64,
    /// Issue #252 AC13: the histogram's row count is now data-dependent
    /// (one row per occupied `(t, bucket)`), so the gate asserts it as an
    /// exact identity against the seeded durations.
    result_rows: u64,
    projections: Vec<String>,
}

/// The most recent `QueryFinish` row matching every SQL fragment.
async fn query_log_like(client: &ChClient, like_fragments: &[&str]) -> Option<QueryLogRow> {
    let mut predicate = format!("type = 'QueryFinish' AND current_database = '{DB}'");
    for fragment in like_fragments {
        predicate.push_str(&format!(" AND query LIKE '%{fragment}%'"));
    }
    let sql = format!(
        "SELECT read_rows, result_rows, projections FROM system.query_log \
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

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CostRow {
    read_rows: u64,
    /// `ProfileEvents['RowsReadByPrewhereReaders']` — the one execution
    /// counter that can see whether a `PREWHERE` exists at all. Its
    /// ABSOLUTE value is population- and granule-layout dependent and is
    /// never asserted (issue #458 plan v5 Delta C: the same query read
    /// 49,152 for an 8,000-row service and 73,728 for an 800-row one in a
    /// single corpus, the smaller population reading the larger number).
    /// Only the identity between two plans and the `!= read_rows`
    /// presence check are asserted.
    prewhere_rows: u64,
    projections: Vec<String>,
}

/// The most recent `QueryFinish` row whose `query` is **byte-identical**
/// to `sql`. Issue #458 AC 6 reads execution counters for two plans that
/// share the `PREWHERE service = 'checkout'` fragment, so a `LIKE` match
/// could return either one's row; equality cannot.
async fn query_log_exact(client: &ChClient, sql: &str) -> Option<CostRow> {
    let literal = sql.replace('\\', "\\\\").replace('\'', "\\'");
    let query = format!(
        "SELECT read_rows, ProfileEvents['RowsReadByPrewhereReaders'] AS prewhere_rows, \
         projections FROM system.query_log WHERE type = 'QueryFinish' AND \
         current_database = '{DB}' AND query = '{literal}' \
         ORDER BY event_time_microseconds DESC LIMIT 1"
    );
    let mut stream = client
        .query_stream::<CostRow>(&query, &QuerySettings::new())
        .await
        .expect("query_log read");
    let mut row = None;
    while let Some(r) = stream.next().await {
        row = Some(r.expect("decode query_log cost row"));
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
    run_init(&admin, &test_ctx(&DB)).await.expect("run_init");

    let now = now_ns();
    let base = now - WINDOW_NS;
    let client = data_client().await;
    seed_corpus(&client, &DB, base).await;

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
    assert_eq!(result.series.len(), 1, "matching spans exist");
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

    // ---- Issue #182 gate: by(resource.service.name) grouping pushes the
    // GROUP BY down to ClickHouse (Aggregating step), keeps the service
    // PREWHERE hoist, and does not regress granule pruning. -------------
    let by_plan = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" } | rate() by(resource.service.name)"#,
        base,
        now,
    );
    assert!(
        by_plan.range_sql().contains("service AS g0"),
        "the by-key lowers to the physical service column:\n{}",
        by_plan.range_sql()
    );
    assert!(
        by_plan
            .range_sql()
            .contains("PREWHERE service = 'checkout'"),
        "the service PREWHERE hoist survives grouping:\n{}",
        by_plan.range_sql()
    );
    let by_raw = explain_raw(&client, by_plan.range_sql()).await;
    assert!(
        by_raw.contains("Aggregating"),
        "the GROUP BY must push down as an Aggregating step:\n{by_raw}"
    );
    // The real grouped query executes and returns one series (only
    // `checkout` matches the filter).
    let by_result = engine
        .metrics_range(&by_plan)
        .await
        .expect("grouped range executes");
    assert_eq!(by_result.series.len(), 1, "one matching service");

    // The distinct-by-key probe SQL exists for the grouped plan and
    // carries the LIMIT cap+1 sentinel (bucket-count-independent).
    let probe = by_plan
        .range_probe_sql()
        .expect("grouped plans render a range probe");
    assert!(
        probe.contains("GROUP BY g0") && probe.contains("LIMIT 1001"),
        "the probe counts distinct label-sets under a cap+1 limit:\n{probe}"
    );

    // ---- Issue #458 AC 6: the root filter costs nothing extra --------
    //
    // `{ service && nestedSetParent<0 }` must prune exactly as tightly as
    // `{ service }`: `parent_id` is an unindexed residual `WHERE`
    // conjunct, so it adds no pruning and — this is the part that can
    // break — must remove none either.
    //
    // **Both plans come from `plan_trace_metrics`, are executed through
    // `TraceEngine`, and every figure below is read back from the exact
    // `range_sql()` text those plans emitted.** A test that hand-wrote the
    // SQL could not observe the planner emitting an anti-semi-join, which
    // is the only lowering this gate exists to forbid (`{ } EXCEPT
    // (SELECT … FROM trace_spans …)`-style: measured `read_rows` 8192 →
    // 208 192 and `CreatingSets` 0 → 1 on this fixture's shape).
    //
    // This is a COST oracle, not a truth oracle. This corpus is
    // all-roots, so the root conjunct selects everything; whether the
    // planner lowered the *right* predicate is AC 3b's job
    // (`traces_metrics_nested_set_live.rs`), on a corpus with real
    // non-roots. A planner returning `{ }`'s SQL for `{ nestedSetParent<0 }`
    // would pass this gate with a perfect ratio and fail that one.
    let baseline_plan = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" } | rate()"#,
        base,
        now,
    );
    let rooted_plan = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" && nestedSetParent < 0 } | rate()"#,
        base,
        now,
    );
    assert!(
        rooted_plan
            .range_sql()
            .contains("PREWHERE service = 'checkout'"),
        "the hoist survives the root conjunct:\n{}",
        rooted_plan.range_sql()
    );
    for (label, plan) in [
        ("baseline", &baseline_plan),
        ("root-filtered", &rooted_plan),
    ] {
        let raw = explain_raw(&client, plan.range_sql()).await;
        assert!(
            raw.contains("service_time"),
            "{label}: the service_time projection must still be selected:\n{raw}"
        );
        assert_eq!(
            raw.matches("CreatingSets").count(),
            0,
            "{label}: the lowering must add no set-building step — an anti-semi-join \
             against the span table would add one:\n{raw}"
        );
        engine
            .metrics_range(plan)
            .await
            .unwrap_or_else(|e| panic!("{label} range executes: {e}"));
    }
    exec(&client, "SYSTEM FLUSH LOGS").await;
    let base_cost = query_log_exact(&client, baseline_plan.range_sql())
        .await
        .expect("the baseline metric's QueryFinish row must exist");
    let root_cost = query_log_exact(&client, rooted_plan.range_sql())
        .await
        .expect("the root-filtered metric's QueryFinish row must exist");
    assert_eq!(
        root_cost.read_rows, base_cost.read_rows,
        "the root conjunct must not change how many rows are read \
         (baseline {} vs root-filtered {})",
        base_cost.read_rows, root_cost.read_rows
    );
    assert_eq!(
        root_cost.prewhere_rows, base_cost.prewhere_rows,
        "the PREWHERE hoist must survive the root conjunct \
         (baseline {} vs root-filtered {})",
        base_cost.prewhere_rows, root_cost.prewhere_rows
    );
    for (label, cost) in [("baseline", &base_cost), ("root-filtered", &root_cost)] {
        assert!(
            cost.projections.iter().any(|p| p.contains("service_time")),
            "{label}: query_log.projections must name service_time, got {:?}",
            cost.projections
        );
        // Presence, not magnitude: a plan with no PREWHERE reads its rows
        // through the main reader only, so the two counters COLLAPSE onto
        // each other. The ratio between them is a granule-layout property
        // (measured 1.5 and 1.8 for two populations of one corpus) and is
        // reported, never asserted.
        assert_ne!(
            cost.prewhere_rows, cost.read_rows,
            "{label}: a PREWHERE must be present — RowsReadByPrewhereReaders \
             ({}) collapses onto read_rows ({}) when the hoist is dropped",
            cost.prewhere_rows, cost.read_rows
        );
        eprintln!(
            "issue #458 AC 6 [{label}]: read_rows={} RowsReadByPrewhereReaders={} ratio={:.3} \
             (reported, not asserted — population- and layout-dependent)",
            cost.read_rows,
            cost.prewhere_rows,
            cost.prewhere_rows as f64 / cost.read_rows as f64
        );
    }

    // ---- Issue #182 P6b: compare() cross-tab pushes down (Aggregating +
    // the intrinsic/attr union), executes, and its distinct-(key,value)
    // cap probe trips a static 422 under a tight max_series. -------------
    let cmp_plan = plan_for(
        &engine,
        r#"{} | compare({ span.http.status_code = "500" })"#,
        base,
        now,
    );
    let (cross, _totals) = cmp_plan.compare_range().expect("compare range SQL");
    // Issue #189: the window-free per-trace roots read is LEFT JOINed into
    // the intrinsics branch (trace-wide `argMin`, no time predicate).
    assert!(
        cross.contains("AS root_name") && cross.contains("AS root_service"),
        "the compare cross-tab carries the roots argMin projections:\n{cross}"
    );
    assert!(
        cross.contains("LEFT JOIN"),
        "the roots read is LEFT JOINed on trace_id into the intrinsics branch:\n{cross}"
    );
    let cmp_raw = explain_raw(&client, cross).await;
    assert!(
        cmp_raw.contains("Aggregating"),
        "compare cross-tab GROUP BY must push down:\n{cmp_raw}"
    );
    // The added roots LEFT JOIN must not regress the DATE-BOUNDED base
    // trace_spans scan (distinct from the deliberately window-free roots
    // read — don't gate that one). Isolate the base scan and EXPLAIN it
    // standalone under the whole-corpus window vs a narrow window: a real
    // pruning read selects STRICTLY FEWER granules for the narrow window
    // (the issue #53 AC3b / gate-2 discriminator). A base read degraded to
    // a full scan would select the same granules either way → this fails.
    let narrow_cmp = plan_for(
        &engine,
        r#"{} | compare({ span.http.status_code = "500" })"#,
        now - 30 * 60 * NS_PER_S,
        now,
    );
    let (narrow_cross, _) = narrow_cmp
        .compare_range()
        .expect("narrow compare range SQL");
    let base_full = extract_compare_base_scan(cross);
    let base_narrow = extract_compare_base_scan(narrow_cross);
    let (full_sel, full_total) =
        table_primary_key_granules(&explain_raw(&client, &base_full).await, "trace_spans");
    let (narrow_sel, narrow_total) =
        table_primary_key_granules(&explain_raw(&client, &base_narrow).await, "trace_spans");
    assert!(
        full_sel > 0 && full_sel <= full_total,
        "the compare base trace_spans scan must engage the primary key \
         ({full_sel}/{full_total})"
    );
    assert!(
        narrow_sel < full_sel,
        "the compare base trace_spans scan must prune strictly harder on a narrow window \
         (narrow {narrow_sel} vs full {full_sel}/{full_total}) — the roots LEFT JOIN must not \
         degrade it to a window-independent full scan"
    );
    // ---- Issue #460 AC 7: the four-argument compare()'s selection window
    // is a SELECT-list conjunct, so it cannot touch granule pruning. Over
    // the SAME narrow request window as `narrow_cross` above, the base
    // trace_spans scan must select the SAME granules — and still prune
    // strictly harder than the whole-corpus window.
    //
    // **What each half below catches, established by breaking it rather
    // than by reading.** Two defects are possible and they are not the
    // same defect:
    //
    //   (a) the window MOVED out of `is_sel` into `WHERE` — caught by the
    //       positive `is_sel` assertion immediately below;
    //   (b) the window ADDED to `WHERE` while `is_sel` keeps it — this is
    //       the one that silently turns a repartition into a FILTER, and
    //       it is caught by the negative assertion after it.
    //
    // The granule-equality assertion at the end catches NEITHER on this
    // corpus: measured, defect (b) leaves the selected/total granule
    // counts unchanged (5/5 both ways), because this fixture's layout does
    // not prune further at that predicate. It is kept because it is the
    // scale-invariant statement of the property and would catch a pruning
    // regression from a different cause — but it is not what stands
    // between us and a window in `WHERE`. What stands between us and (b)
    // in terms of the ANSWER is `compare_arity_differential`'s B4 and B5,
    // both of which redden (measured). -----------------------------------
    let windowed = plan_for(
        &engine,
        &format!(
            r#"{{}} | compare({{ span.http.status_code = "500" }}, 3, {}, {})"#,
            now - 20 * 60 * NS_PER_S,
            now - 10 * 60 * NS_PER_S
        ),
        now - 30 * 60 * NS_PER_S,
        now,
    );
    let (windowed_cross, _) = windowed
        .compare_range()
        .expect("windowed compare range SQL");
    assert!(
        windowed_cross.contains(&format!(
            "AND timestamp_ns > {} AND timestamp_ns <= {}) AS is_sel",
            now - 20 * 60 * NS_PER_S,
            now - 10 * 60 * NS_PER_S
        )),
        "the selection window renders as a conjunct on the is_sel SELECT-list expression:\n\
         {windowed_cross}"
    );
    // (b): the window must appear NOWHERE in a filter position. It
    // repartitions the population into baseline/selection; a copy of it in
    // `PREWHERE`/`WHERE` would DROP the spans the reference merely moves
    // to `baseline`, and every total would change.
    let base_windowed = extract_compare_base_scan(windowed_cross);
    for line in base_windowed.lines() {
        let trimmed = line.trim_start();
        if !(trimmed.starts_with("WHERE") || trimmed.starts_with("PREWHERE")) {
            continue;
        }
        assert!(
            !line.contains(&format!("timestamp_ns > {}", now - 20 * 60 * NS_PER_S)),
            "the selection window reached a filter position — it must repartition the \
             population, never filter it:\n{line}"
        );
    }
    let (windowed_sel, windowed_total) =
        table_primary_key_granules(&explain_raw(&client, &base_windowed).await, "trace_spans");
    assert_eq!(
        (windowed_sel, windowed_total),
        (narrow_sel, narrow_total),
        "the selection window must not change granule pruning: the four-argument form selected \
         {windowed_sel}/{windowed_total} granules where the one-argument form over the SAME \
         request window selected {narrow_sel}/{narrow_total}. A window predicate that reached \
         WHERE or PREWHERE would move this — and would also drop the spans the reference merely \
         re-partitions into baseline"
    );
    assert!(
        windowed_sel < full_sel,
        "the windowed compare base scan must still prune strictly harder on a narrow request \
         window ({windowed_sel} vs {full_sel}/{full_total})"
    );

    let cmp_res = engine
        .metrics_range(&cmp_plan)
        .await
        .expect("compare executes");
    assert!(
        cmp_res
            .series
            .iter()
            .any(|s| s.labels.iter().any(|l| l.key == "__meta_type")),
        "compare emits __meta_type meta-series"
    );
    // A tight max_series makes the distinct-(key,value) probe reject.
    let mut capped_cfg = engine_config();
    capped_cfg.max_series = 1;
    let capped = TraceEngine::new(data_client().await, capped_cfg);
    let capped_plan = plan_for(
        &capped,
        r#"{} | compare({ span.http.status_code = "500" })"#,
        base,
        now,
    );
    let err = capped
        .metrics_range(&capped_plan)
        .await
        .expect_err("many distinct (key,value) pairs > cap 1 must reject");
    assert!(
        matches!(
            err,
            ReadError::QueryTooBroad(TooBroadReason::TraceMetricsSeriesCap { .. })
        ),
        "compare cap breach is a 422 query_too_broad, got {err:?}"
    );

    // ---- Issue #252 AC7: the NEW histogram SQL shape carries the same
    // pushdown as the count/agg forms. The whole design rests on the
    // inner replay-dedup subquery being byte-identical to
    // `metrics_agg_range_sql`'s, so both index behaviours are gated on
    // the histogram's own generated SQL, with the same assertions and
    // the same thresholds as gates 1 and 2 above. The quantile form
    // rides along: its SQL is unchanged by #252, and pinning it here is
    // what makes "unchanged" checkable rather than asserted. ----------
    for (label, q) in [
        (
            "histogram",
            r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 } | histogram_over_time(duration)"#,
        ),
        (
            "quantile",
            r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 } | quantile_over_time(duration, 0.5, 0.9)"#,
        ),
    ] {
        let plan = plan_for(&engine, q, base, now);
        assert!(
            plan.range_sql().contains("PREWHERE service = 'checkout'"),
            "{label}: the hoist survives into the generated SQL:\n{}",
            plan.range_sql()
        );
        let raw = explain_raw(&client, plan.range_sql()).await;
        assert!(
            raw.contains("service_time"),
            "{label}: the service-equality form must still select the service_time \
             projection:\n{raw}"
        );
        let result = plan.range_sql().to_string();
        engine
            .metrics_range(&plan)
            .await
            .unwrap_or_else(|e| panic!("{label} range executes: {e}\n{result}"));
        exec(&client, "SYSTEM FLUSH LOGS").await;
        let marker = if label == "histogram" {
            "roundToExp2"
        } else {
            "quantilesTDigest"
        };
        let row = query_log_like(&client, &["PREWHERE service = \\'checkout\\'", marker])
            .await
            .unwrap_or_else(|| panic!("{label}: the query's QueryFinish row must exist"));
        assert!(
            row.projections.iter().any(|p| p.contains("service_time")),
            "{label}: query_log.projections must name service_time, got {:?}",
            row.projections
        );
        // Same granule-aware bound as gate 1 — do NOT re-tighten.
        assert!(
            row.read_rows < CORPUS_SPANS + CORPUS_SPANS / 2,
            "{label}: the spans side must be served by the service_time projection's prefix, \
             not a full scan (read {})",
            row.read_rows
        );

        // …and the attribute form's semi-join still prunes on the
        // (key, val) prefix, with time pruning isolated inside the dense
        // env=prod prefix (the gate-2 discriminator).
        let attr_q = q.replace(
            r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 }"#,
            r#"{ .env = "prod" }"#,
        );
        let full = plan_for(&engine, &attr_q, base, now);
        let narrow = plan_for(&engine, &attr_q, now - 30 * 60 * NS_PER_S, now);
        let (full_sel, full_total) = table_primary_key_granules(
            &explain_raw(&client, &extract_semi_join_subquery(full.range_sql())).await,
            "trace_attrs_idx",
        );
        let (narrow_sel, _) = table_primary_key_granules(
            &explain_raw(&client, &extract_semi_join_subquery(narrow.range_sql())).await,
            "trace_attrs_idx",
        );
        assert!(
            full_sel <= full_total && full_sel > 0,
            "{label}: the semi-join's prefix read must engage the attr primary key \
             ({full_sel}/{full_total})"
        );
        assert!(
            narrow_sel < full_sel,
            "{label}: the narrow window must prune strictly fewer granules within the SAME \
             dense (key, val) prefix (narrow {narrow_sel} vs full {full_sel})"
        );
    }

    // ---- Issue #252 AC13: the log2 histogram's cost, gated ------------
    // What this asserts and NOTHING more: (1) the scan is untouched —
    // `read_rows` equals a count-only query over the byte-identical inner
    // dedup subquery, so the change is post-scan on this corpus; (2)
    // `result_rows` equals `Σ over steps (distinct occupied buckets)`
    // computed independently from the seeded durations — an exact
    // identity that fails on over- AND under-emission; (3) the same
    // identity at MAXIMUM occupancy, where the row count is largest;
    // (4) the static `64 × steps` ceiling on both corpora.
    //
    // Deliberately NOT asserted: `memory_usage`, query time, or any
    // resource budget, at typical or worst-case occupancy — those are
    // scale questions that route to #25 rather than to a CI assertion
    // that would be flaky (docs/schemas.md §9: no wall-time in CI).
    let hist_plan = plan_for(&engine, "{} | histogram_over_time(duration)", base, now);
    let hist_sql = hist_plan.range_sql();
    assert!(
        hist_sql.contains("toUInt64(roundToExp2(val - 1)) * 2 AS bucket"),
        "the log2 bucket expression is in the generated SQL:\n{hist_sql}"
    );
    // The count-only twin over the SAME inner subquery: the outer
    // aggregate is all that differs, so any `read_rows` gap is the
    // aggregation touching rows the scan did not.
    let inner = extract_log2_inner(hist_sql);
    let count_only = format!("SELECT count() AS scan_identity_probe FROM (\n  {inner}\n)");
    exec(&client, &count_only).await;
    engine
        .metrics_range(&hist_plan)
        .await
        .expect("histogram range executes");
    exec(&client, "SYSTEM FLUSH LOGS").await;
    let probe_row = query_log_like(&client, &["scan_identity_probe"])
        .await
        .expect("the count-only probe's QueryFinish row must exist");
    let hist_row = query_log_like(&client, &["roundToExp2"])
        .await
        .expect("the histogram query's QueryFinish row must exist");
    assert_eq!(
        hist_row.read_rows, probe_row.read_rows,
        "the log2 histogram must read exactly the rows its inner dedup subquery reads — the \
         change is post-scan (histogram {} vs count-only {})",
        hist_row.read_rows, probe_row.read_rows
    );

    // The independent expectation, from the seeding rule alone.
    let seeded: Vec<(i64, i64)> = (0..CORPUS_SPANS)
        .map(|n| {
            let spread = WINDOW_NS / CORPUS_SPANS as i64;
            (base + n as i64 * spread, n as i64 * 10_000)
        })
        .collect();
    let (want_rows, want_max_occupancy) = expected_bucket_rows(&seeded);
    assert_eq!(
        hist_row.result_rows, want_rows,
        "one row per OCCUPIED (step, bucket), computed from the seeded durations"
    );
    let steps = distinct_steps(&seeded);
    assert!(
        hist_row.result_rows <= 64 * steps,
        "the static ceiling: {} rows > 64 × {steps} steps",
        hist_row.result_rows
    );
    assert!(
        want_max_occupancy > 1,
        "the seeded corpus must actually occupy several buckets per step, or the identity is \
         vacuous (max per-step occupancy {want_max_occupancy})"
    );

    // Maximum occupancy: one span per reachable power of two, so every
    // one of the 62 storable buckets is occupied at once and the exact
    // identity is exercised where the row count is largest.
    // Anchored to a step START and packed 10 s apart, so all 63 land in
    // ONE step: the ceiling is only exercised where a single step
    // carries every bucket.
    //
    // `1i64 << k` for k in 1..=62 occupies buckets 2^1..2^62, and the
    // extra `2^62 + 1` span occupies 2^63 — the TOP of the reachable
    // range, which is the whole reason `bucket_ns` is `UInt64` and the
    // SQL casts before doubling. Without it this corpus would claim
    // "every reachable bucket" while leaving the one that motivated the
    // type untested.
    let maxocc_base = step_start_ns(base + WINDOW_NS / 2) + 60 * NS_PER_S;
    let maxocc: Vec<(i64, i64)> = (1..=62u32)
        .map(|k| 1i64 << k)
        .chain(std::iter::once((1i64 << 62) + 1))
        .enumerate()
        .map(|(i, dur_ns)| (maxocc_base + i as i64 * 10 * NS_PER_S, dur_ns))
        .collect();
    let values: Vec<String> = maxocc
        .iter()
        .enumerate()
        .map(|(i, (ts_ns, dur_ns))| {
            let id = 8_000_000 + i as u64;
            format!(
                "(toFixedString(unhex('{id:032x}'), 16), toFixedString(unhex('{id:016x}'), 8), \
                 toFixedString(unhex('0000000000000000'), 8), 'op', 'maxocc', {ts_ns}, \
                 {dur_ns}, 0, 1, 1, 'p')"
            )
        })
        .collect();
    exec(
        &client,
        &format!(
            "INSERT INTO {DB}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) VALUES {}",
            values.join(", ")
        ),
    )
    .await;
    let max_plan = plan_for(
        &engine,
        r#"{ resource.service.name = "maxocc" } | histogram_over_time(duration)"#,
        base,
        now,
    );
    let max_result = engine
        .metrics_range(&max_plan)
        .await
        .expect("max-occupancy histogram executes");
    exec(&client, "SYSTEM FLUSH LOGS").await;
    let max_row = query_log_like(&client, &["roundToExp2", "maxocc"])
        .await
        .expect("the max-occupancy query's QueryFinish row must exist");
    let (max_want_rows, max_occupancy) = expected_bucket_rows(&maxocc);
    assert_eq!(
        max_want_rows, 63,
        "every reachable bucket is seeded: 2^1..=2^62 plus 2^63 (from a 2^62 + 1 duration)"
    );
    assert!(
        maxocc.iter().any(|(_, d)| *d == (1i64 << 62) + 1),
        "the 2^63 bucket's seed span must be present"
    );
    assert_eq!(
        max_row.result_rows, max_want_rows,
        "the same exact identity at maximum occupancy"
    );
    assert!(
        max_row.result_rows <= 64 * distinct_steps(&maxocc),
        "the static ceiling holds at maximum occupancy too"
    );
    assert!(
        max_occupancy >= 63,
        "the max-occupancy corpus must pack its buckets into few steps to exercise the ceiling \
         (max per-step occupancy {max_occupancy})"
    );

    // Row COUNT is not the claim. The top of the domain has to survive
    // the whole path — ClickHouse's `toUInt64(...) * 2` producing 2^63,
    // the RowBinary decode into `MetricLog2BucketRow.bucket_ns: u64`, and
    // `bucket_seconds` framing it into a label — so assert the EMITTED
    // `__bucket` values, bit-for-bit, not just how many there are. The
    // `Int64` form this replaced would have decoded 2^63 as a NEGATIVE
    // label, which `result_rows` alone cannot see.
    let emitted: BTreeSet<u64> = max_result
        .series
        .iter()
        .map(|s| {
            assert_eq!(s.labels.len(), 1, "one __bucket label: {s:?}");
            assert_eq!(s.labels[0].key, "__bucket");
            let pulsus_read::MetricLabelValue::Double(seconds) = s.labels[0].value else {
                panic!("__bucket is a double: {s:?}");
            };
            assert!(
                seconds > 0.0,
                "a bucket label is positive — a negative one is the signed-overflow bug this \
                 corpus exists to catch ({seconds})"
            );
            seconds.to_bits()
        })
        .collect();
    let want: BTreeSet<u64> = (1..=63u32)
        .map(|k| log2_histogram::bucket_seconds(1u64 << k).to_bits())
        .collect();
    assert_eq!(
        emitted, want,
        "every reachable bucket label must arrive intact, 2^1 through 2^63"
    );
    let top = log2_histogram::bucket_seconds(1u64 << 63);
    assert!(
        emitted.contains(&top.to_bits()),
        "the 2^63 bucket ({top} s) must be emitted end to end — this is the case the UInt64 \
         cast exists for, and a pure-Rust unit test cannot reach it"
    );
    assert_eq!(top, 9_223_372_036.854_776_f64);

    // ---- Issue #477 AC8: what the two NEW statements per range request
    // cost. Exemplars are on by DEFAULT now, and a grouped range query
    // runs its own probe over the widened window, so this issue adds up to
    // two statements to a panel that previously ran one. Each is gated
    // against the range query it rides beside — a relation, never a
    // literal: granule denominators move between Compact and Wide parts on
    // the same fixture, so an absolute count is a flake, not a gate. -----
    let default_ex = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" } | rate()"#,
        base,
        now,
    );
    assert_eq!(
        default_ex.exemplar_budget(),
        100,
        "no hint and no parameter resolves the default budget"
    );
    let exemplar_sql = default_ex
        .exemplar_sql()
        .expect("the default budget renders exemplar SQL");

    // (i) The exemplar query selects the SAME projection and prunes to the
    // same granule count as the range query it accompanies. It carries the
    // identical PREWHERE/WHERE/window by construction; this is the live
    // confirmation that the access path is identical too.
    let range_raw = explain_raw(&client, default_ex.range_sql()).await;
    let ex_raw = explain_raw(&client, exemplar_sql).await;
    assert!(
        ex_raw.contains("service_time"),
        "the exemplar query must select the same service_time projection:\n{ex_raw}"
    );
    // Keyed on `service_time`, not on `trace_spans`: when the projection
    // is selected the `ReadFromMergeTree` block names the PROJECTION, so
    // parsing it is itself the assertion that both statements took the
    // projection path rather than the base table.
    assert_eq!(
        table_primary_key_granules(&ex_raw, "service_time"),
        table_primary_key_granules(&range_raw, "service_time"),
        "the exemplar query must prune to the same granules as its range query\n         range:\n{range_raw}\nexemplar:\n{ex_raw}"
    );

    // (ii) …and read the same rows. `read_rows`, not `read_bytes`: the two
    // statements project different column sets, so the byte figures differ
    // legitimately while the row figures must not.
    let _ = engine
        .metrics_range(&default_ex)
        .await
        .expect("the default-exemplar range executes");
    exec(&client, "SYSTEM FLUSH LOGS").await;
    let range_cost = query_log_exact(&client, default_ex.range_sql())
        .await
        .expect("the range statement is in query_log");
    let ex_cost = query_log_exact(&client, exemplar_sql)
        .await
        .expect("the exemplar statement is in query_log");
    assert_eq!(
        ex_cost.read_rows, range_cost.read_rows,
        "the exemplar query reads the same rows as its range query          ({} vs {})",
        ex_cost.read_rows, range_cost.read_rows
    );

    // (ii-b, wave-3 ruling) The POOLED quantile placement domain costs no
    // second scan. A `quantile_over_time` exemplar is placed against the
    // quantiles of the WHOLE range window, and the two ways to get them
    // are a second aggregation over the spans or a window function over
    // the per-bucket rows the statement already produces. We took the
    // window function, and this is the measurement that says so rather
    // than a reading of the query text: the exemplar statement reads the
    // SAME rows as its range query (a second pass over the spans would
    // roughly double it), and the whole request still puts exactly TWO
    // statements over `trace_spans` into the query log.
    let quant = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" } | quantile_over_time(duration, 0.5, 0.9)"#,
        base,
        now,
    );
    let quant_ex = quant
        .exemplar_sql()
        .expect("the default budget renders quantile exemplar SQL");
    assert!(
        quant_ex.contains(" OVER () AS Array(Float64)) AS qs"),
        "the pooled array must come from a window function over the range partition:\n{quant_ex}"
    );
    exec(&client, "SYSTEM FLUSH LOGS").await;
    const SPANS: &str = "FROM trace_spans";
    let spans0 = count_statements_like(&client, SPANS).await;
    let framed = engine
        .metrics_range(&quant)
        .await
        .expect("the quantile range executes");
    assert!(
        framed
            .series
            .iter()
            .any(|s| s.samples.iter().any(|(_, v)| *v != 0.0)),
        "the fixture must produce a non-zero quantile, or the exemplar statement is skipped \
         and this gate measures nothing"
    );
    exec(&client, "SYSTEM FLUSH LOGS").await;
    assert_eq!(
        count_statements_like(&client, SPANS).await - spans0,
        2,
        "a quantile range request with exemplars issues the range statement and the exemplar \
         statement and NOTHING else - a third would be the second aggregation this design \
         exists to avoid"
    );
    let q_range_cost = query_log_exact(&client, quant.range_sql())
        .await
        .expect("the quantile range statement is in query_log");
    let q_ex_cost = query_log_exact(&client, quant_ex)
        .await
        .expect("the quantile exemplar statement is in query_log");
    assert_eq!(
        q_ex_cost.read_rows, q_range_cost.read_rows,
        "the pooled quantiles must ride the exemplar statement's own scan          ({} vs {})",
        q_ex_cost.read_rows, q_range_cost.read_rows
    );

    // (iii) A range request whose framed result has no non-zero sample
    // issues EXACTLY ONE statement. This is what bounds turning exemplars
    // on by default: after densification an empty answer is a full grid of
    // zeros, which is the commonest shape a sparse panel produces, and
    // there is no bucket an exemplar could belong to.
    //
    // Counted by STATEMENT SHAPE, not by a service literal: `AGG` is the
    // aggregation every range statement carries and `EX` the exemplar
    // collection's own aggregate, so both candidate statements are inside
    // the count and neither can be classified out of it.
    const AGG: &str = "uniqExact(trace_id, span_id) AS n";
    const EX: &str = "groupArraySample(";
    let absent = plan_for(
        &engine,
        r#"{ resource.service.name = "no-such-service-477" } | rate()"#,
        base,
        now,
    );
    assert!(
        absent.exemplar_sql().is_some(),
        "the plan still RENDERS exemplar SQL - the skip is a runtime decision on the framed \
         result, not a planning one, or this gate would be vacuous"
    );
    exec(&client, "SYSTEM FLUSH LOGS").await;
    let agg0 = count_statements_like(&client, AGG).await;
    let ex0 = count_statements_like(&client, EX).await;
    let framed = engine
        .metrics_range(&absent)
        .await
        .expect("the no-match range executes");
    assert!(
        framed.series[0].samples.iter().all(|(_, v)| *v == 0.0),
        "the fixture must produce an all-zero frame, or the skip is not the thing under test"
    );
    exec(&client, "SYSTEM FLUSH LOGS").await;
    let agg1 = count_statements_like(&client, AGG).await;
    let ex1 = count_statements_like(&client, EX).await;
    assert_eq!(agg1 - agg0, 1, "the range statement itself");
    assert_eq!(
        ex1 - ex0,
        0,
        "an all-zero frame must issue NO exemplar statement"
    );

    // The control that makes the zero above a measurement rather than a
    // blind spot: the SAME counters over a request that DOES produce a
    // non-zero sample see the exemplar statement.
    let matched = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" } | rate()"#,
        base,
        now,
    );
    let framed = engine
        .metrics_range(&matched)
        .await
        .expect("the matching range executes");
    assert!(
        framed.series[0].samples.iter().any(|(_, v)| *v != 0.0),
        "the control must produce a non-zero sample, or it proves nothing"
    );
    exec(&client, "SYSTEM FLUSH LOGS").await;
    let agg2 = count_statements_like(&client, AGG).await;
    let ex2 = count_statements_like(&client, EX).await;
    assert_eq!(agg2 - agg1, 1, "the range statement itself");
    assert_eq!(
        ex2 - ex1,
        1,
        "a frame with a non-zero sample DOES issue the exemplar statement - without this the \
         zero above could mean the counter is blind"
    );

    // (iv) The range probe selects the same projection as the range query
    // and prunes at least as many granules as the instant probe — it
    // covers a strictly wider window, so `>=`, a relation and not a
    // literal.
    let grouped = plan_for(
        &engine,
        r#"{ resource.service.name = "checkout" } | rate() by(resource.service.name)"#,
        base,
        now,
    );
    let range_probe_raw =
        explain_raw(&client, grouped.range_probe_sql().expect("a range probe")).await;
    let instant_probe_raw = explain_raw(
        &client,
        grouped.instant_probe_sql().expect("an instant probe"),
    )
    .await;
    assert!(
        range_probe_raw.contains("service_time"),
        "the range probe must select the same projection as the range query:\n{range_probe_raw}"
    );
    let (range_probe_granules, _) = table_primary_key_granules(&range_probe_raw, "service_time");
    let (instant_probe_granules, _) =
        table_primary_key_granules(&instant_probe_raw, "service_time");
    assert!(
        range_probe_granules >= instant_probe_granules,
        "the range probe covers a strictly wider window, so it can never prune MORE granules          ({range_probe_granules} < {instant_probe_granules})"
    );
}

/// How many `QueryFinish` statements in this database carry `fragment` —
/// the counter AC8(iii) reads.
///
/// `query NOT LIKE '%system.query_log%'` excludes this counter's own
/// statement, which necessarily contains the fragment it searches for and
/// would otherwise be counted on the next call.
async fn count_statements_like(client: &ChClient, fragment: &str) -> u64 {
    #[derive(Row, serde::Serialize, serde::Deserialize)]
    struct CountRow {
        n: u64,
    }
    let sql = format!(
        "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' \
         AND current_database = '{DB}' AND query NOT LIKE '%system.query_log%' \
         AND position(query, '{fragment}') > 0"
    );
    let mut stream = client
        .query_stream::<CountRow>(&sql, &QuerySettings::new())
        .await
        .expect("query_log count");
    let mut n = 0;
    while let Some(row) = stream.next().await {
        n = row.expect("row").n;
    }
    n
}

/// Isolates the inner replay-dedup subquery of a log2 histogram range
/// SQL — the text between `FROM (` and the outer `WHERE val >= 2`. Used
/// to build a count-only query over the byte-identical scan.
fn extract_log2_inner(sql: &str) -> String {
    let start = sql
        .find("FROM (\n  ")
        .unwrap_or_else(|| panic!("no inner subquery in\n{sql}"))
        + "FROM (\n  ".len();
    let rel_end = sql[start..]
        .find("\n)\nWHERE val >= 2")
        .unwrap_or_else(|| panic!("no inner-subquery terminator in\n{sql}"));
    sql[start..start + rel_end].to_string()
}

/// `(Σ over steps of distinct occupied buckets, the largest per-step
/// occupancy)` for a `(timestamp_ns, duration_ns)` schedule, derived the
/// way the SQL derives it: floor the timestamp to the step grid, drop
/// `duration < 2`, round the rest up to the next power of two.
fn expected_bucket_rows(spans: &[(i64, i64)]) -> (u64, usize) {
    let mut per_step: std::collections::BTreeMap<i64, std::collections::BTreeSet<u64>> =
        std::collections::BTreeMap::new();
    for (ts_ns, dur_ns) in spans {
        let Some(bucket) = pulsus_read::traces::log2_histogram::log2_bucketize_ns(*dur_ns) else {
            continue;
        };
        per_step
            .entry(step_start_ns(*ts_ns))
            .or_default()
            .insert(bucket);
    }
    let total: usize = per_step.values().map(std::collections::BTreeSet::len).sum();
    let max = per_step
        .values()
        .map(std::collections::BTreeSet::len)
        .max()
        .unwrap_or(0);
    (total as u64, max)
}

/// The number of distinct steps a schedule touches.
fn distinct_steps(spans: &[(i64, i64)]) -> u64 {
    spans
        .iter()
        .map(|(ts_ns, _)| step_start_ns(*ts_ns))
        .collect::<std::collections::BTreeSet<_>>()
        .len() as u64
}

/// `toStartOfInterval(…, INTERVAL 3600000 MILLISECOND)` in Rust: the
/// step grid is anchored at the epoch, so the bucket start is the
/// timestamp floored to a multiple of the step.
fn step_start_ns(ts_ns: i64) -> i64 {
    const STEP_NS: i64 = 3_600 * NS_PER_S;
    ts_ns.div_euclid(STEP_NS) * STEP_NS
}

/// The per-statement read cost of a range request, printed rather than
/// asserted — the reproducible source of the cost figures in
/// [`metrics_sql::metrics_compare_exemplar_range_sql`]'s and
/// [`metrics_sql::metrics_quantile_exemplar_range_sql`]'s doc comments
/// (issue #477 wave 3).
///
/// `#[ignore]`d and gated on the same `PULSUS_TEST_CLICKHOUSE` as the
/// rest of this file. It is a MEASUREMENT, not a gate: it asserts only
/// that it saw statements at all, because absolute row counts are a
/// property of the corpus and wall times are a property of the machine.
/// Its purpose is that a reader can re-derive the numbers instead of
/// taking them from a comment.
///
/// ```text
/// PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=18123 \
///   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
///   cargo nextest run -p pulsus-read --test traces_metrics_explain \
///   -E 'test(=the_per_statement_read_cost_of_a_range_request)' \
///   --run-ignored all --no-capture
/// ```
#[tokio::test]
#[ignore = "a measurement probe, not a gate; see the doc comment"]
async fn the_per_statement_read_cost_of_a_range_request() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse");
        return;
    }
    #[derive(Row, serde::Serialize, serde::Deserialize)]
    struct StmtRow {
        read_rows: u64,
        read_bytes: u64,
        memory_usage: u64,
        query_duration_ms: u64,
        head: String,
    }
    #[derive(Row, serde::Serialize, serde::Deserialize)]
    struct MarkRow {
        mark: String,
    }

    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    run_init(&admin, &test_ctx(&DB)).await.expect("run_init");
    let now = now_ns();
    let base = now - WINDOW_NS;
    let client = data_client().await;
    seed_corpus(&client, &DB, base).await;
    let engine = TraceEngine::new(data_client().await, engine_config());

    // The high-water mark of this database's query log, so each request's
    // statements are the ones logged after its own marker rather than
    // everything in a trailing time window.
    async fn watermark(client: &ChClient, db: &str) -> String {
        let sql = format!(
            "SELECT toString(max(event_time_microseconds)) AS mark FROM system.query_log \
             WHERE current_database = '{db}'"
        );
        let mut stream = client
            .query_stream::<MarkRow>(&sql, &QuerySettings::new())
            .await
            .expect("watermark");
        let mut mark = String::from("1970-01-01 00:00:00.000000");
        while let Some(r) = stream.next().await {
            let r = r.expect("row");
            if !r.mark.is_empty() {
                mark = r.mark;
            }
        }
        mark
    }

    for q in [
        r#"{ } | rate()"#,
        r#"{ } | quantile_over_time(duration, 0.5, 0.9)"#,
        r#"{ } | compare({ resource.service.name = "checkout" })"#,
    ] {
        let plan = plan_for(&engine, q, base, now);
        exec(&client, "SYSTEM FLUSH LOGS").await;
        let mark = watermark(&client, &DB).await;
        let framed = engine.metrics_range(&plan).await.expect("executes");
        exec(&client, "SYSTEM FLUSH LOGS").await;
        let sql = format!(
            "SELECT read_rows, read_bytes, memory_usage, toUInt64(query_duration_ms) AS \
             query_duration_ms, replaceAll(substring(query, 1, 58), '\\n', ' ') AS head \
             FROM system.query_log WHERE type = 'QueryFinish' AND current_database = '{DB}' \
             AND query NOT LIKE '%system.query_log%' AND query NOT LIKE '%SYSTEM FLUSH%' \
             AND event_time_microseconds > toDateTime64('{mark}', 6) \
             ORDER BY event_time_microseconds ASC"
        );
        let mut stream = client
            .query_stream::<StmtRow>(&sql, &QuerySettings::new())
            .await
            .expect("query_log");
        let mut n = 0usize;
        let mut rows = 0u64;
        println!("\n=== {q}   ({} series)", framed.series.len());
        while let Some(r) = stream.next().await {
            let r = r.expect("row");
            n += 1;
            rows += r.read_rows;
            println!(
                "  stmt {n}: read_rows={:>9} read_bytes={:>11} mem={:>11} dur_ms={:>5}  {}",
                r.read_rows, r.read_bytes, r.memory_usage, r.query_duration_ms, r.head
            );
        }
        println!("  statements: {n}, read_rows total: {rows}");
        assert!(
            n >= 2,
            "every one of these requests issues at least the range statement and its exemplar statement"
        );
    }
    exec(&admin, &format!("DROP DATABASE IF EXISTS {DB}")).await;
}
