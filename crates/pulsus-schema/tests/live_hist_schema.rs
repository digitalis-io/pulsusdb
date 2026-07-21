//! Integration tests for the M7-A2 native-histogram storage schema (issue
//! #113, A1 design #112) against a real ClickHouse server.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, mirroring `tests/live_schema.rs`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19124:8123 -p 19001:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=19124 \
//!     cargo test -p pulsus-schema --test live_hist_schema
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Each test uses its own dedicated database so tests can run concurrently
//! against the same server without racing on shared table names.

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, QuerySettings, Row};
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
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
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

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-schema/tests/live_hist_schema.rs for setup)"
            );
            return;
        }
    };
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct NameRow {
    name: String,
}

async fn table_names(client: &ChClient, db: &str) -> Vec<String> {
    let sql = format!("SELECT name FROM system.tables WHERE database = '{db}' ORDER BY name");
    let mut stream = client
        .query_stream::<NameRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.tables");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode NameRow").name);
    }
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct ColumnRow {
    name: String,
    #[serde(rename = "type")]
    ty: String,
}

/// The `(name, type)` of `column` on `db.table`, or `None` if absent.
async fn column_type(client: &ChClient, db: &str, table: &str, column: &str) -> Option<String> {
    let sql = format!(
        "SELECT name, type FROM system.columns \
         WHERE database = '{db}' AND table = '{table}' AND name = '{column}'"
    );
    let mut stream = client
        .query_stream::<ColumnRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.columns");
    stream.next().await.map(|r| r.expect("decode ColumnRow").ty)
}

async fn drop_database(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            pulsus_clickhouse::Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainRow {
    explain: String,
}

/// A native-histogram sample row, in the Prometheus integer sparse wire form.
/// Field order matches the `SELECT` column order below (RowBinary is
/// positional). `Vec<T>` maps to `Array(T)`.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct HistSampleRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    schema: i8,
    zero_threshold: f64,
    zero_count: u64,
    count: u64,
    sum: f64,
    pos_span_offsets: Vec<i32>,
    pos_span_lengths: Vec<u32>,
    pos_bucket_deltas: Vec<i64>,
    neg_span_offsets: Vec<i32>,
    neg_span_lengths: Vec<u32>,
    neg_bucket_deltas: Vec<i64>,
    custom_values: Vec<f64>,
}

const HIST_SELECT_COLS: &str = "metric_name, fingerprint, unix_milli, schema, zero_threshold, \
     zero_count, count, sum, pos_span_offsets, pos_span_lengths, pos_bucket_deltas, \
     neg_span_offsets, neg_span_lengths, neg_bucket_deltas, custom_values";

/// Issue #113 (AC): `run_init` on a fresh database creates
/// `metric_hist_samples` and adds `metric_series.value_type UInt8`; a second
/// run is a no-op (no `MigrationDrift` on ids 23–26); and the frozen
/// `metric_samples`/`metric_series` base CREATEs are untouched (the float read
/// path stays byte-frozen).
#[tokio::test]
async fn native_histogram_migrations_apply_and_are_idempotent() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_hist_it_apply";
    drop_database(&client, db).await;
    let ctx = test_ctx(db);

    run_init(&client, &ctx).await.expect("run_init (first run)");

    let names = table_names(&client, db).await;
    assert!(
        names.contains(&"metric_hist_samples".to_string()),
        "metric_hist_samples must exist after reconcile: {names:?}"
    );

    // The additive routing column lands on metric_series as UInt8.
    assert_eq!(
        column_type(&client, db, "metric_series", "value_type")
            .await
            .as_deref(),
        Some("UInt8"),
        "value_type must be a UInt8 column on metric_series after reconcile"
    );

    // metric_samples (id 5) gains no histogram column; metric_series (id 4)
    // gains ONLY value_type (via ALTER), nothing else.
    assert_eq!(
        column_type(&client, db, "metric_samples", "schema").await,
        None,
        "metric_samples must NOT gain any histogram column"
    );
    assert_eq!(
        column_type(&client, db, "metric_samples", "value")
            .await
            .as_deref(),
        Some("Float64"),
        "metric_samples value column stays Float64 (float path byte-frozen)"
    );

    // Second run: idempotent, no MigrationDrift on ids 23–26.
    run_init(&client, &ctx)
        .await
        .expect("run_init (second run — ids 23–26 must not drift)");
    let names_after = table_names(&client, db).await;
    assert_eq!(names, names_after, "second run must not add/remove objects");

    drop_database(&client, db).await;
}

/// Issue #113 (AC): `EXPLAIN indexes=1` on a `metric_hist_samples` fetch shows
/// the `metric_name`-led primary key driving the read — matching the
/// `metric_samples` gate precedent in `live_schema.rs`, so the native fetch
/// prunes identically (no full scan).
#[tokio::test]
async fn metric_hist_samples_explain_shows_metric_name_pk_pruning() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_hist_it_explain";
    drop_database(&client, db).await;
    let ctx = test_ctx(db);
    run_init(&client, &ctx).await.expect("run_init");

    // A histogram sample must exist first: EXPLAIN on an empty MergeTree
    // trims to a NullSource read (no primary key in the plan), so the PK-prune
    // gate is only meaningful once a part exists — mirrors the metric_samples
    // precedent (live_schema.rs inserts before its EXPLAIN gate).
    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let data_client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock");
    let unix_milli = i64::try_from(now.as_millis()).expect("fits i64");
    let seed = HistSampleRow {
        metric_name: "http_request_duration_seconds".to_string(),
        fingerprint: 18374588331335825905,
        unix_milli,
        schema: 2,
        zero_threshold: 1e-38,
        zero_count: 0,
        count: 3,
        sum: 1.5,
        pos_span_offsets: vec![0],
        pos_span_lengths: vec![2],
        pos_bucket_deltas: vec![1, 1],
        neg_span_offsets: vec![],
        neg_span_lengths: vec![],
        neg_bucket_deltas: vec![],
        custom_values: vec![],
    };
    data_client
        .insert_block("metric_hist_samples", std::slice::from_ref(&seed))
        .await
        .expect("insert seed histogram sample");

    let mut explain = client
        .query_stream::<ExplainRow>(
            &format!(
                "EXPLAIN indexes = 1 SELECT fingerprint, unix_milli, count, sum \
                 FROM {db}.metric_hist_samples \
                 WHERE metric_name = 'http_request_duration_seconds' \
                 AND fingerprint IN (18374588331335825905)"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("explain metric_hist_samples fetch");
    let mut plan = String::new();
    while let Some(row) = explain.next().await {
        plan.push_str(&row.expect("decode explain row").explain);
        plan.push('\n');
    }
    assert!(
        plan.contains("metric_name"),
        "EXPLAIN output must show metric_name driving the primary key read, got:\n{plan}"
    );
}

/// Collects the raw `EXPLAIN indexes = 1` lines for `sql`.
async fn explain_lines(client: &ChClient, sql: &str) -> Vec<String> {
    let mut stream = client
        .query_stream::<ExplainRow>(&format!("EXPLAIN indexes = 1 {sql}"), &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode explain row").explain);
    }
    out
}

/// The LAST `Granules: k/N` *selected* count in the plan — the PrimaryKey
/// section's post-pruning selection, which follows the MinMax/Partition
/// sections (each of which reports its own `Granules` line). `None` when
/// the query prunes so completely that no `Granules` line survives; both
/// `None` and `Some(0)` mean "zero granules read" (the `last_granules`
/// parser precedent, live_traces.rs).
fn last_selected_granules(plan: &[String]) -> Option<u64> {
    plan.iter()
        .rev()
        .find_map(|l| l.trim().strip_prefix("Granules: "))
        .and_then(|g| g.split_once('/'))
        .map(|(k, _)| k.trim().parse::<u64>().expect("granules selected"))
}

/// M7-A5a AC3 (Tier-1, query-performance mandate): the dual-read's
/// complementary `metric_hist_samples` read touches **zero granules** for a
/// single-type series — proven for BOTH prune components of the compound
/// primary key `(metric_name, fingerprint, unix_milli)`:
///   (i) a **pure-float metric name** — the leading-PK `metric_name`
///       PREWHERE prunes every histogram granule; and
///   (ii) a **float-only fingerprint inside a histogram-bearing metric** —
///        the `fingerprint IN (…)` prunes the histogram granule whose
///        fingerprint range excludes it.
/// The complementary read is dispatched concurrently with the float read
/// (latency-hidden), and this gate proves it is also byte-free on the wire
/// for single-type series (A1 v5), never a full scan.
#[tokio::test]
async fn complementary_hist_read_selects_zero_granules_for_single_type_series() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_hist_it_zero_granules";
    drop_database(&client, db).await;
    let ctx = test_ctx(db);
    run_init(&client, &ctx).await.expect("run_init");

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let data_client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock");
    let unix_milli = i64::try_from(now.as_millis()).expect("fits i64");

    // Seed ONE histogram granule under `mixed_metric` at fingerprint BIG, so
    // the hist table is non-empty (an empty MergeTree trims to a NullSource
    // read and would make the prune trivially/meaninglessly pass).
    const HIST_FP: u64 = 18374588331335825905;
    let seed = HistSampleRow {
        metric_name: "mixed_metric".to_string(),
        fingerprint: HIST_FP,
        unix_milli,
        schema: 0,
        zero_threshold: 0.0,
        zero_count: 0,
        count: 4,
        sum: 5.0,
        pos_span_offsets: vec![0],
        pos_span_lengths: vec![3],
        pos_bucket_deltas: vec![1, 1, -1],
        neg_span_offsets: vec![],
        neg_span_lengths: vec![],
        neg_bucket_deltas: vec![],
        custom_values: vec![],
    };
    data_client
        .insert_block("metric_hist_samples", std::slice::from_ref(&seed))
        .await
        .expect("insert seed histogram sample");

    let lo = unix_milli - 300_000;
    let hi = unix_milli + 1;

    // (i) Pure-float metric name: the complementary hist read for a metric
    // that has NO histogram data — metric_name PREWHERE prunes all granules.
    let float_only = format!(
        "SELECT fingerprint, unix_milli, count, sum FROM {db}.metric_hist_samples \
         PREWHERE metric_name = 'pure_float_metric' \
         WHERE unix_milli > {lo} AND unix_milli <= {hi} AND fingerprint IN ({HIST_FP}) \
         ORDER BY fingerprint, unix_milli"
    );
    let plan_i = explain_lines(&client, &float_only).await;
    assert!(
        matches!(last_selected_granules(&plan_i), None | Some(0)),
        "(i) pure-float metric name must select zero histogram granules, got:\n{}",
        plan_i.join("\n")
    );

    // (ii) Float-only fingerprint inside a histogram-bearing metric: the
    // `fingerprint IN (1)` prunes the only hist granule (fingerprint range
    // [BIG, BIG] excludes 1).
    let float_fp = format!(
        "SELECT fingerprint, unix_milli, count, sum FROM {db}.metric_hist_samples \
         PREWHERE metric_name = 'mixed_metric' \
         WHERE unix_milli > {lo} AND unix_milli <= {hi} AND fingerprint IN (1) \
         ORDER BY fingerprint, unix_milli"
    );
    let plan_ii = explain_lines(&client, &float_fp).await;
    assert!(
        matches!(last_selected_granules(&plan_ii), None | Some(0)),
        "(ii) float-only fingerprint must select zero histogram granules, got:\n{}",
        plan_ii.join("\n")
    );

    // Sanity: the matching read DOES select the granule (the gate is not
    // vacuously passing because the table/part is unreadable).
    let matching = format!(
        "SELECT fingerprint, unix_milli, count, sum FROM {db}.metric_hist_samples \
         PREWHERE metric_name = 'mixed_metric' \
         WHERE unix_milli > {lo} AND unix_milli <= {hi} AND fingerprint IN ({HIST_FP}) \
         ORDER BY fingerprint, unix_milli"
    );
    let plan_hit = explain_lines(&client, &matching).await;
    assert_eq!(
        last_selected_granules(&plan_hit),
        Some(1),
        "the matching histogram read must select its one granule, got:\n{}",
        plan_hit.join("\n")
    );

    drop_database(&client, db).await;
}

/// Issue #113 (AC): a native-histogram row round-trips LOSSLESSLY for BOTH a
/// standard exponential sample AND an NHCB (schema −53) sample — insert →
/// select → field-equal, including the sparse spans, delta buckets, and NHCB
/// `custom_values` arrays.
#[tokio::test]
async fn native_histogram_row_round_trips_losslessly_exponential_and_nhcb() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_hist_it_roundtrip";
    drop_database(&client, db).await;
    let ctx = test_ctx(db);
    run_init(&client, &ctx).await.expect("run_init");

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let data_client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock");
    let base_ms = i64::try_from(now.as_millis()).expect("fits i64");

    // Standard exponential (schema 2): populated zero bucket + positive AND
    // negative spans/deltas; no custom_values. Deltas are the Prometheus
    // wire form (first absolute, then signed deltas) stored verbatim.
    let exponential = HistSampleRow {
        metric_name: "http_request_duration_seconds".to_string(),
        fingerprint: 0xFFFF_FFFF_FFFF_FFF1,
        unix_milli: base_ms,
        schema: 2,
        zero_threshold: 2.938735877055719e-39,
        zero_count: 7,
        count: 42,
        sum: 123.456,
        pos_span_offsets: vec![0, 3],
        pos_span_lengths: vec![2, 1],
        pos_bucket_deltas: vec![4, -2, 1],
        neg_span_offsets: vec![-1],
        neg_span_lengths: vec![2],
        neg_bucket_deltas: vec![3, -1],
        custom_values: vec![],
    };

    // NHCB (schema −53): only schema/count/sum/positive spans+deltas +
    // custom_values (explicit bucket bounds) are used; zero/negative fields
    // empty (matches upstream custom-buckets contract). Lossless too.
    let nhcb = HistSampleRow {
        metric_name: "http_request_duration_seconds".to_string(),
        fingerprint: 0xFFFF_FFFF_FFFF_FFF1,
        unix_milli: base_ms + 1,
        schema: -53,
        zero_threshold: 0.0,
        zero_count: 0,
        count: 15,
        sum: 88.5,
        pos_span_offsets: vec![0],
        pos_span_lengths: vec![3],
        pos_bucket_deltas: vec![5, 3, -2],
        neg_span_offsets: vec![],
        neg_span_lengths: vec![],
        neg_bucket_deltas: vec![],
        custom_values: vec![0.005, 0.01, 0.025, 0.05],
    };

    let inserted = vec![exponential, nhcb];
    data_client
        .insert_block("metric_hist_samples", &inserted)
        .await
        .expect("insert metric_hist_samples");

    let mut stream = client
        .query_stream::<HistSampleRow>(
            &format!(
                "SELECT {HIST_SELECT_COLS} FROM {db}.metric_hist_samples \
                 ORDER BY unix_milli"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("select metric_hist_samples");
    let mut got = Vec::new();
    while let Some(row) = stream.next().await {
        got.push(row.expect("decode HistSampleRow"));
    }

    assert_eq!(got.len(), 2, "both histogram rows present");
    assert_eq!(
        got[0], inserted[0],
        "exponential native-histogram row must round-trip field-for-field"
    );
    assert_eq!(
        got[1], inserted[1],
        "NHCB native-histogram row must round-trip field-for-field (incl. custom_values)"
    );

    drop_database(&client, db).await;
}
