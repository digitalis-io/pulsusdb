//! Issue #173 (M7-E1) AC7/AC-new: live Tier-1 gates for the service-graph
//! read against ClickHouse 24.8 (scale-invariant ratios only — no
//! wall-time asserts). Seeds a multi-day `trace_edges` corpus through the
//! ingest MV (spans → `trace_edges_mv` → `trace_edges`), then proves:
//!
//! - **AC7(a) — daily-partition MinMax prune:** a narrow (1-day) window
//!   selects strictly fewer parts than the full-range window on the same
//!   per-side subquery, corroborated by a `system.query_log` `read_rows`
//!   ratio on the full service-graph query.
//! - **AC7(b) — leading-`side` PrimaryKey prune:** a per-side subquery
//!   (`WHERE side = 1`) reads strictly fewer granules than an unfiltered
//!   full-table scan of `trace_edges`.
//! - **AC-new (quantiles type):** the read's `CAST(... AS Array(Float64))`
//!   expression is `Array(Float64)` on the pinned server (loud on drift),
//!   and the whole `GraphEdgeRow` decodes into `Vec<f64>` through the real
//!   `TraceEngine::service_graph`.
//! - **Determinism:** the engine's edges are byte-identical before and
//!   after `OPTIMIZE TABLE trace_edges FINAL` (merge invariance).
//! - **Scan-budget 422:** a tiny `scan_budget_rows` trips code 158 →
//!   `TooBroadReason::TraceScanBudgetRows`.
//!
//! Live-gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_graph_explain
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::logql::{ReadError, TooBroadReason};
use pulsus_read::{GraphWindow, ServiceGraph, TraceEngine, TraceReadConfig, service_graph_sql};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

fn test_config(db: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: db.to_string(),
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

const DB: &str = "pulsus_traces_graph_it";
/// Edge pairs seeded — one client + one server span each, so `trace_edges`
/// holds `2 * PAIRS` rows across `DAYS` daily partitions.
const PAIRS: u64 = 120_000;
const DAYS: i64 = 7;
const DAY_NS: i64 = 86_400 * 1_000_000_000;

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

/// Seeds `PAIRS` client(kind 3)/server(kind 2) span pairs into
/// `trace_spans` — the `trace_edges_mv` populates `trace_edges` with the
/// half-rows. Each pair's timestamp is `(number % DAYS)` whole days before
/// `anchor`, so the ledger spans `DAYS` daily partitions.
async fn seed_pairs(client: &ChClient, anchor_ns: i64) {
    // Client halves (kind 3, service 'client-svc'): pair_id = own span_id.
    // Ids start at `number + 1` so no span_id is the all-zero root sentinel
    // (which would make its server partner look like a root and drop it).
    exec(
        client,
        &format!(
            "INSERT INTO {DB}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT toFixedString(unhex(leftPad(lower(hex(number + 1)), 32, '0')), 16), \
                    toFixedString(unhex(leftPad(lower(hex(number + 1)), 16, '0')), 8), \
                    toFixedString(unhex('0000000000000000'), 8), \
                    'op', 'client-svc', \
                    {anchor_ns} - toInt64(number % {DAYS}) * {DAY_NS}, \
                    toInt64(number % 1000) * 100000, 0, 3, 1, 'p' \
             FROM numbers({PAIRS})"
        ),
    )
    .await;
    // Server halves (kind 2, service 'server-svc'): parent_id = the client's
    // span_id (same `number`), status 500 on 1% (failed edges).
    exec(
        client,
        &format!(
            "INSERT INTO {DB}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT toFixedString(unhex(leftPad(lower(hex(number + 1)), 32, '0')), 16), \
                    toFixedString(unhex(leftPad(lower(hex(number + 1 + {PAIRS})), 16, '0')), 8), \
                    toFixedString(unhex(leftPad(lower(hex(number + 1)), 16, '0')), 8), \
                    'op', 'server-svc', \
                    {anchor_ns} - toInt64(number % {DAYS}) * {DAY_NS}, \
                    toInt64(number % 1000) * 100000, if(number % 100 = 0, 2, 0), 2, 1, 'p' \
             FROM numbers({PAIRS})"
        ),
    )
    .await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainRow {
    explain: String,
}

async fn explain_indexes(client: &ChClient, sql: &str) -> String {
    let full = format!("EXPLAIN indexes = 1 {sql}");
    let mut out = String::new();
    let mut stream = client
        .query_stream::<ExplainRow>(&full, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN failed: {e}\nSQL:\n{full}"));
    while let Some(row) = stream.next().await {
        out.push_str(&row.expect("decode explain row").explain);
        out.push('\n');
    }
    out
}

/// Sum of selected parts across every `Parts: k/N` line in the plan (a
/// JOIN plan has one `ReadFromMergeTree` block per subquery).
fn selected_parts(raw: &str) -> u64 {
    raw.lines()
        .filter_map(|l| l.trim().strip_prefix("Parts: "))
        .filter_map(|r| r.split_once('/'))
        .filter_map(|(k, _)| k.trim().parse::<u64>().ok())
        .sum()
}

/// The single `PrimaryKey` `Granules: k/N` ratio of a single-table plan.
fn primary_key_granules(raw: &str) -> (u64, u64) {
    const TITLES: &[&str] = &["MinMax", "Partition", "PrimaryKey", "Skip"];
    let mut in_pk = false;
    for line in raw.lines() {
        let t = line.trim();
        if TITLES.contains(&t) {
            in_pk = t == "PrimaryKey";
            continue;
        }
        if in_pk && let Some(r) = t.strip_prefix("Granules: ") {
            let (k, n) = r.split_once('/').expect("k/N");
            return (k.trim().parse().expect("k"), n.trim().parse().expect("n"));
        }
    }
    panic!("no PrimaryKey Granules line:\n{raw}");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ReadRowsRow {
    read_rows: u64,
}

async fn read_rows_of(client: &ChClient, sql: &str, query_id: &str) -> u64 {
    let settings = QuerySettings::new().set("query_id", query_id);
    let mut stream = client
        .query_stream::<pulsus_read::GraphEdgeRow>(sql, &settings)
        .await
        .unwrap_or_else(|e| panic!("tagged read failed: {e}\nSQL:\n{sql}"));
    while let Some(row) = stream.next().await {
        row.expect("decode edge row");
    }
    exec(client, "SYSTEM FLUSH LOGS").await;
    let log_sql = format!(
        "SELECT read_rows FROM system.query_log \
         WHERE query_id = '{query_id}' AND type = 'QueryFinish' \
         ORDER BY event_time_microseconds DESC LIMIT 1"
    );
    let mut stream = client
        .query_stream::<ReadRowsRow>(&log_sql, &QuerySettings::new())
        .await
        .expect("query_log read");
    stream
        .next()
        .await
        .unwrap_or_else(|| panic!("no query_log row for {query_id}"))
        .expect("decode")
        .read_rows
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TypeNameRow {
    t: String,
}

fn engine_config(scan_budget_rows: u64) -> TraceReadConfig {
    TraceReadConfig {
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
        catalog_table: "trace_tag_catalog".to_string(),
        edges_table: "trace_edges".to_string(),
        max_candidates: 100_000,
        scan_budget_rows,
        max_series: 1_000,
        generator_max_memory_bytes: 536_870_912,
        distributed: false,
        skip_unavailable_shards: false,
    }
}

/// One `#[tokio::test]` runs every gate in sequence over the shared corpus
/// (the seed dominates runtime).
#[tokio::test]
async fn service_graph_tier1_gates() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect");
    exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    run_init(&bootstrap, &test_ctx(DB)).await.expect("run_init");

    let client = ChClient::new(test_config(DB)).await.expect("data client");
    let anchor = now_ns();
    seed_pairs(&client, anchor).await;

    // Windows: full range covers all DAYS partitions; narrow covers just the
    // most recent day. Both use the read builder's own `[S, E)` bound.
    let full = GraphWindow {
        start_ns: anchor - DAYS * DAY_NS - DAY_NS,
        end_ns: anchor + DAY_NS,
    };
    let narrow = GraphWindow {
        start_ns: anchor - DAY_NS,
        end_ns: anchor + DAY_NS,
    };

    // -- AC7(a): daily-partition MinMax prune on the server subquery --------
    let server_subquery = |w: GraphWindow| {
        format!(
            "SELECT trace_id, span_id, max(failed) AS failed FROM {DB}.trace_edges \
             WHERE side = 1 \
               AND date >= toDate(fromUnixTimestamp64Nano({s})) \
               AND date <= toDate(fromUnixTimestamp64Nano({e})) \
               AND timestamp_ns >= {s} AND timestamp_ns < {e} \
             GROUP BY trace_id, span_id",
            s = w.start_ns,
            e = w.end_ns,
        )
    };
    let full_parts = selected_parts(&explain_indexes(&client, &server_subquery(full)).await);
    let narrow_parts = selected_parts(&explain_indexes(&client, &server_subquery(narrow)).await);
    assert!(
        narrow_parts < full_parts,
        "a narrow window must MinMax-prune to strictly fewer parts than the full range \
         (narrow={narrow_parts}, full={full_parts})"
    );

    // -- AC7(b): leading-`side` PrimaryKey prune ---------------------------
    // The side-filtered subquery reads strictly fewer granules than an
    // unfiltered full-table scan (side leads the ORDER BY).
    let side_scan = format!(
        "SELECT trace_id, span_id FROM {DB}.trace_edges WHERE side = 1 GROUP BY trace_id, span_id"
    );
    let unfiltered =
        format!("SELECT trace_id, span_id FROM {DB}.trace_edges GROUP BY trace_id, span_id");
    let (side_g, _) = primary_key_granules(&explain_indexes(&client, &side_scan).await);
    let (all_g, _) = primary_key_granules(&explain_indexes(&client, &unfiltered).await);
    assert!(
        side_g < all_g,
        "the leading-side PK prune must read strictly fewer granules than an unfiltered scan \
         (side={side_g}, all={all_g})"
    );

    // -- AC7(a) corroboration: read_rows ratio on the full read ------------
    let full_read = read_rows_of(
        &client,
        &service_graph_sql(full, "trace_edges", 1_000),
        "graph_full",
    )
    .await;
    let narrow_read = read_rows_of(
        &client,
        &service_graph_sql(narrow, "trace_edges", 1_000),
        "graph_narrow",
    )
    .await;
    assert!(
        narrow_read < full_read,
        "the narrow window must read strictly fewer rows than the full range \
         (narrow={narrow_read}, full={full_read})"
    );

    // -- AC-new: the quantiles CAST is Array(Float64) on the pinned server --
    let mut stream = client
        .query_stream::<TypeNameRow>(
            "SELECT toTypeName(CAST(quantilesTDigest(0.5, 0.95, 0.99)(toInt64(number)) \
             AS Array(Float64))) AS t FROM numbers(10)",
            &QuerySettings::new(),
        )
        .await
        .expect("toTypeName query");
    let type_name = stream.next().await.expect("row").expect("decode").t;
    assert_eq!(
        type_name, "Array(Float64)",
        "the quantiles CAST must pin Array(Float64) — a server-version drift must fail loudly"
    );

    // -- Determinism + real decode: the engine's edges round-trip Vec<f64>
    //    and are byte-identical before/after OPTIMIZE ... FINAL ------------
    let engine = TraceEngine::new(
        ChClient::new(test_config(DB)).await.expect("engine client"),
        engine_config(50_000_000),
    );
    let before: ServiceGraph = engine.service_graph(full).await.expect("service_graph");
    assert!(
        !before.edges.is_empty(),
        "the seeded corpus must yield an edge"
    );
    let edge = before
        .edges
        .iter()
        .find(|e| e.client == "client-svc" && e.server == "server-svc")
        .expect("the client-svc -> server-svc edge");
    assert_eq!(edge.conn_type, "rpc");
    assert_eq!(edge.calls, PAIRS, "one edge instance per seeded pair");
    assert_eq!(
        edge.failed,
        PAIRS / 100,
        "1% of server halves carry status 500"
    );
    assert_eq!(
        edge.quantiles_ns.len(),
        3,
        "quantiles decode as [p50, p95, p99] f64"
    );
    assert!(edge.quantiles_ns.iter().all(|q| q.is_finite()));

    exec(&client, &format!("OPTIMIZE TABLE {DB}.trace_edges FINAL")).await;
    let after = engine
        .service_graph(full)
        .await
        .expect("service_graph after FINAL");
    // Merge invariance: the DETERMINISTIC identity — the edge set and its
    // replay-deduped `calls`/`failed` — is byte-identical before and after
    // the merge (read-time per-side dedup, not merge state). `quantilesTDigest`
    // is an approximate, merge-order-sensitive estimator (plan risk 5), so the
    // quantiles are tolerance-banded, never asserted byte-equal.
    assert_eq!(
        after.edges.len(),
        before.edges.len(),
        "edge count is merge-invariant"
    );
    for (b, a) in before.edges.iter().zip(after.edges.iter()) {
        assert_eq!(
            (&b.client, &b.server, &b.conn_type, b.calls, b.failed),
            (&a.client, &a.server, &a.conn_type, a.calls, a.failed),
            "the edge identity + deduped counts must be byte-identical across a merge"
        );
        for (bq, aq) in b.quantiles_ns.iter().zip(a.quantiles_ns.iter()) {
            let tol = bq.abs() * 0.05 + 1.0;
            assert!(
                (bq - aq).abs() <= tol,
                "TDigest quantile drift across a merge must stay within tolerance \
                 (before={bq}, after={aq})"
            );
        }
    }

    // -- Scan-budget 422: a tiny row budget trips code 158 -----------------
    let tiny = TraceEngine::new(
        ChClient::new(test_config(DB)).await.expect("tiny client"),
        engine_config(1),
    );
    match tiny.service_graph(full).await {
        Err(ReadError::QueryTooBroad(TooBroadReason::TraceScanBudgetRows { .. })) => {}
        other => panic!("a tiny scan budget must trip TraceScanBudgetRows, got {other:?}"),
    }

    exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {DB}")).await;
}
