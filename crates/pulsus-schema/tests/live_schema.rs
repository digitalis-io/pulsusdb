//! Integration tests against a real ClickHouse server — the "every DDL
//! block in schemas.md is rendered and executed against a fresh ClickHouse
//! in CI" contract (docs/schemas.md §1, issue #5).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, mirroring
//! `crates/pulsus-clickhouse/tests/live_clickhouse.rs`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-schema --test live_schema
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Each test uses its own dedicated database (`CREATE DATABASE IF NOT
//! EXISTS`, dropped at the start of the test) so tests can run concurrently
//! against the same server without racing on shared table names.

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_schema::{RenderCtx, SchemaParams, check_version};
use pulsus_schema_testkit::run_init;

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
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

fn test_ctx(db: &str) -> SchemaParams {
    RenderCtx::for_tests(db)
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-schema/tests/live_schema.rs for setup)"
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

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CreateQueryRow {
    create_table_query: String,
}

/// The live `CREATE TABLE` statement ClickHouse reports back for `name`
/// (used to assert the TTL a re-init actually applied, not just what we
/// think we rendered).
async fn create_table_query(client: &ChClient, db: &str, name: &str) -> String {
    let sql = format!(
        "SELECT create_table_query FROM system.tables WHERE database = '{db}' AND name = '{name}'"
    );
    let mut stream = client
        .query_stream::<CreateQueryRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.tables create_table_query");
    stream
        .next()
        .await
        .expect("row present")
        .expect("decode")
        .create_table_query
}

async fn drop_database(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct MetricSampleRow {
    metric_name: String,
    /// `UInt128` since issue #498, read as the bare integer: this suite
    /// pins the COLUMN, and `pulsus-model`'s newtype is not a dependency
    /// of this crate.
    fingerprint: u128,
    unix_milli: i64,
    value: f64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct LogSampleRow {
    service: String,
    fingerprint: u128,
    timestamp_ns: i64,
    severity: i8,
    body: String,
    /// Issue #97: the additive per-entry structured-metadata column
    /// (canonical JSON String, `DEFAULT ''`), migration ids 21/22.
    structured_metadata: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct DescribeRow {
    name: String,
    #[serde(rename = "type")]
    ty: String,
}

/// The pre-#97 `log_samples` row shape, WITHOUT `structured_metadata` — used
/// to prove the column is backward-compatible (a row inserted with the old
/// explicit column list reads back the empty-string default).
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct LegacyLogSampleRow {
    service: String,
    fingerprint: u128,
    timestamp_ns: i64,
    severity: i8,
    body: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainRow {
    explain: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

async fn count(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("count query failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").n
}

/// The core M0 acceptance contract (issue #5): `run_init` on a fresh
/// database creates every table/MV, a second run is a no-op, sample data
/// round-trips, and the metrics fetch path (docs/schemas.md §2.3) uses the
/// `metric_name` primary-key prefix.
#[tokio::test]
async fn run_init_creates_every_m0_table_and_mv_and_is_idempotent() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_full");
    drop_database(&client, db).await;
    let ctx = test_ctx(db);

    run_init(&client, &ctx).await.expect("run_init (first run)");

    let expected_base_tables = [
        "metric_metadata",
        "metric_series",
        "metric_samples",
        "log_streams",
        "log_streams_idx",
        "log_samples",
        "log_metrics_5s",
    ];
    let expected_mvs = ["log_streams_idx_mv", "log_metrics_5s_mv"];

    let names = table_names(&client, db).await;
    for t in expected_base_tables.iter().chain(expected_mvs.iter()) {
        assert!(
            names.contains(&t.to_string()),
            "missing {t} in system.tables: {names:?}"
        );
    }

    // Second run: idempotent, no error — every `CREATE TABLE` carries `IF
    // NOT EXISTS` and every view is dropped before it is created.
    run_init(&client, &ctx)
        .await
        .expect("run_init (second run, no-op)");
    let names_after = table_names(&client, db).await;
    assert_eq!(
        names, names_after,
        "second run must not add or remove objects"
    );

    // Smoke insert + round-trip on both raw sample tables. `insert_block`
    // (the `clickhouse` crate's typed insert path) escapes its whole
    // `table` argument as a single identifier, so it cannot take a
    // `{{db}}.table`-qualified name the way `execute`'s raw-SQL path can
    // (see `src/bookkeeping.rs`'s module doc) — a second client bound
    // directly to `db` (which exists now that `run_init` has created it)
    // is the realistic shape of a real writer's connection anyway (issue
    // #6: a writer's `ChConnConfig.database` is the target db directly).
    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let data_client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");

    // Must be within the table's `PULSUS_RETENTION_DAYS` (7) TTL window —
    // `ttl_only_drop_parts = 1` makes a whole already-expired part eligible
    // for background-merge deletion almost immediately after insert, so a
    // fixed historical constant (safe for pulsus-clickhouse's own TTL-less
    // smoke tables) would flake here.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock");
    let unix_milli = i64::try_from(now.as_millis()).expect("fits i64");
    let timestamp_ns = i64::try_from(now.as_nanos()).expect("fits i64");

    let metric_rows = vec![MetricSampleRow {
        metric_name: "http_requests_total".to_string(),
        fingerprint: 0xFFFF_FFFF_FFFF_FFF1,
        unix_milli,
        value: 42.5,
    }];
    data_client
        .insert_block("metric_samples", &metric_rows)
        .await
        .expect("insert metric_samples");

    let log_rows = vec![LogSampleRow {
        service: "checkout".to_string(),
        fingerprint: 12345,
        timestamp_ns,
        severity: 9,
        body: "connection refused".to_string(),
        structured_metadata: r#"{"trace_id":"abc"}"#.to_string(),
    }];
    data_client
        .insert_block("log_samples", &log_rows)
        .await
        .expect("insert log_samples");

    let mut ms = client
        .query_stream::<MetricSampleRow>(
            &format!("SELECT metric_name, fingerprint, unix_milli, value FROM {db}.metric_samples"),
            &QuerySettings::new(),
        )
        .await
        .expect("select metric_samples");
    let got_metric = ms.next().await.expect("one row").expect("decode");
    assert_eq!(got_metric, metric_rows[0]);

    let mut ls = client
        .query_stream::<LogSampleRow>(
            &format!(
                "SELECT service, fingerprint, timestamp_ns, severity, body, structured_metadata \
                 FROM {db}.log_samples"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("select log_samples");
    let got_log = ls.next().await.expect("one row").expect("decode");
    assert_eq!(got_log, log_rows[0]);

    // EXPLAIN indexes=1 sanity check (docs/schemas.md §2.3 fetch shape):
    // the metric_name-led primary key must be in play, not a full scan.
    let mut explain = client
        .query_stream::<ExplainRow>(
            &format!(
                "EXPLAIN indexes = 1 SELECT fingerprint, unix_milli, value FROM {db}.metric_samples \
                 WHERE metric_name = 'http_requests_total' AND fingerprint IN (toUInt128('18374588331335825905'))"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("explain metric_samples fetch");
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

/// Issue #97 (AC-1/AC-2): the additive `structured_metadata` ALTER (migration
/// id 21) lands the canonical JSON String column on `log_samples`, existing
/// rows read back the empty-string default (backward compatible — no data
/// migration), and a second `run_init` no-ops ids 21/22 with no
/// `MigrationDrift`.
#[tokio::test]
async fn structured_metadata_column_is_additive_and_backward_compatible() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_sm");
    drop_database(&client, db).await;
    let ctx = test_ctx(db);

    run_init(&client, &ctx).await.expect("run_init (first run)");

    // AC-1: the catalog shows `structured_metadata String` after reconcile.
    let mut desc = client
        .query_stream::<DescribeRow>(
            &format!(
                "SELECT name, type FROM system.columns \
                 WHERE database = '{db}' AND table = 'log_samples' \
                 AND name = 'structured_metadata'"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("describe log_samples");
    let mut sm_type: Option<String> = None;
    while let Some(row) = desc.next().await {
        let row = row.expect("decode describe row");
        if row.name == "structured_metadata" {
            sm_type = Some(row.ty);
        }
    }
    assert_eq!(
        sm_type.as_deref(),
        Some("String"),
        "structured_metadata must be a String column after reconcile"
    );

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let data_client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock");
    let timestamp_ns = i64::try_from(now.as_nanos()).expect("fits i64");

    // AC-1 backward compat: a row inserted with the PRE-#97 explicit column
    // list (no structured_metadata) reads back the empty-string default —
    // proving existing/old-writer data needs no migration.
    let legacy = vec![LegacyLogSampleRow {
        service: "legacy".to_string(),
        fingerprint: 111,
        timestamp_ns,
        severity: 0,
        body: "no structured metadata".to_string(),
    }];
    data_client
        .insert_block("log_samples", &legacy)
        .await
        .expect("insert legacy log_samples");

    // A row WITH structured metadata (the #97 writer shape) round-trips.
    let with_sm = vec![LogSampleRow {
        service: "modern".to_string(),
        fingerprint: 222,
        timestamp_ns,
        severity: 0,
        body: "has structured metadata".to_string(),
        structured_metadata: r#"{"trace_id":"abc","user_id":"42"}"#.to_string(),
    }];
    data_client
        .insert_block("log_samples", &with_sm)
        .await
        .expect("insert log_samples with structured metadata");

    let mut ls = client
        .query_stream::<LogSampleRow>(
            &format!(
                "SELECT service, fingerprint, timestamp_ns, severity, body, structured_metadata \
                 FROM {db}.log_samples ORDER BY fingerprint"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("select log_samples");
    let mut got = Vec::new();
    while let Some(row) = ls.next().await {
        got.push(row.expect("decode"));
    }
    assert_eq!(got.len(), 2, "both rows present");
    assert_eq!(
        got[0].structured_metadata, "",
        "the legacy row reads back the empty-string default"
    );
    assert_eq!(
        got[1].structured_metadata, r#"{"trace_id":"abc","user_id":"42"}"#,
        "the modern row round-trips its structured metadata verbatim"
    );

    // AC-2: a second run_init is a no-op.
    run_init(&client, &ctx)
        .await
        .expect("run_init (second run, no-op — ids 21/22 must not drift)");

    drop_database(&client, db).await;
}

/// A view dropped out from under an existing schema comes back on the next
/// run. The file carries `DROP VIEW IF EXISTS` before every `CREATE
/// MATERIALIZED VIEW`, so a run always restates every view's definition —
/// which is also why a second run cannot fail on one.
#[tokio::test]
async fn a_second_run_recreates_a_view_that_was_dropped() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_mv_absent");
    drop_database(&client, db).await;
    let ctx = test_ctx(db);

    run_init(&client, &ctx).await.expect("initial run");
    let before = create_table_query(&client, db, "log_streams_idx_mv").await;

    client
        .execute(
            &format!("DROP VIEW IF EXISTS {db}.log_streams_idx_mv"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop the view out from under the schema");
    assert!(
        !table_names(&client, db)
            .await
            .contains(&"log_streams_idx_mv".to_string())
    );

    run_init(&client, &ctx)
        .await
        .expect("the next run heals it");

    assert!(
        table_names(&client, db)
            .await
            .contains(&"log_streams_idx_mv".to_string()),
        "a view missing from system.tables must be recreated"
    );
    assert_eq!(
        before,
        create_table_query(&client, db, "log_streams_idx_mv").await,
        "the recreated view's definition must be byte-identical"
    );
}

/// Pure version-gate refusal, proven against a real server's actual
/// `SELECT version()` string (parsing/comparison logic itself is unit
/// tested without a container in `src/checks.rs`).
#[tokio::test]
async fn check_version_accepts_the_live_test_servers_reported_version() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let mut stream = client
        .query_stream::<VersionRow>("SELECT version() AS v", &QuerySettings::new())
        .await
        .expect("select version()");
    let row = stream.next().await.expect("one row").expect("decode");
    check_version(&row.v).expect("the live test server must be >= 26.3");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct VersionRow {
    v: String,
}

/// `PULSUS_RETENTION_DAYS` reaches every retained table's delete-TTL, in
/// the saturating `least(..., 4294967295)` form (issues #131/#137/#187),
/// and a changed value reaches it after a rebuild.
///
/// **A retention change is a rebuild**, not an `ALTER`: the TTL is declared
/// in the `CREATE`, so `schema/schema.sh` drops the database and builds it
/// again. That is what this does — the old separate `apply_ttl` pass, and
/// the background task that reapplied it, are gone.
#[tokio::test]
async fn the_configured_retention_reaches_every_retained_tables_ttl() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_retention_change");
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    ctx.retention_days = 7;

    run_init(&client, &ctx)
        .await
        .expect("run_init (retention_days=7)");
    // The saturating expression (issues #131/#137) is declared in the
    // `CREATE`; ClickHouse normalizes the rendered `{{retention_days}} *
    // 86400` product by wrapping it in parens (live_traces.rs pins the same
    // normalization for the ns form).
    let before_metric = create_table_query(&client, db, "metric_samples").await;
    assert!(
        before_metric.contains("least(intDiv(unix_milli, 1000) + (7 * 86400), 4294967295)"),
        "metric_samples' initial TTL must reflect retention_days=7: {before_metric}"
    );
    let before_hist = create_table_query(&client, db, "metric_hist_samples").await;
    assert!(
        before_hist.contains("least(intDiv(unix_milli, 1000) + (7 * 86400), 4294967295)"),
        "metric_hist_samples' initial TTL must be the saturating form: {before_hist}"
    );
    let before_log = create_table_query(&client, db, "log_samples").await;
    assert!(
        before_log.contains("least(intDiv(timestamp_ns, 1000000000) + (7 * 86400), 4294967295)"),
        "log_samples' initial TTL must reflect retention_days=7: {before_log}"
    );
    // Issue #187: `log_patterns` (nanosecond `bucket_ns`) carries the
    // saturating form too, not the wrap-prone INTERVAL one.
    let before_patterns = create_table_query(&client, db, "log_patterns").await;
    assert!(
        before_patterns.contains("least(intDiv(bucket_ns, 1000000000) + (7 * 86400), 4294967295)"),
        "log_patterns' initial TTL must be the saturating form: {before_patterns}"
    );

    // A changed retention is a rebuild: drop and build again.
    drop_database(&client, db).await;
    ctx.retention_days = 30;
    run_init(&client, &ctx)
        .await
        .expect("a rebuild at the new PULSUS_RETENTION_DAYS must succeed");

    for table in [
        "metric_samples",
        "log_samples",
        "metric_hist_samples",
        "log_patterns",
        "metric_series",
    ] {
        let after = create_table_query(&client, db, table).await;
        assert!(
            after.contains("(30 * 86400)"),
            "{table}'s TTL must be updated to the new retention_days: {after}"
        );
        assert!(
            !after.contains("(7 * 86400)"),
            "{table}: stale retention_days=7 TTL: {after}"
        );
        assert!(
            !after.contains("toIntervalDay("),
            "{table}: the wrap-prone INTERVAL form must be superseded: {after}"
        );
    }
}

/// `PULSUS_LOG_ROLLUP_RESOLUTION` is config-derived into the rollup
/// table/MV *name* — a re-init after it changes must succeed, create the
/// new-named objects, and leave the old ones (and their data) in place
/// rather than dropping them.
/// This test proves the live functional outcome: the new objects are
/// created and the old ones are left alone.
#[tokio::test]
async fn run_init_after_log_rollup_resolution_change_creates_new_table_and_retains_old() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_rollup_change");
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    ctx.log_rollup = Duration::from_secs(5);

    run_init(&client, &ctx).await.expect("run_init (rollup=5s)");
    let names_before = table_names(&client, db).await;
    assert!(names_before.contains(&"log_metrics_5s".to_string()));
    assert!(names_before.contains(&"log_metrics_5s_mv".to_string()));

    ctx.log_rollup = Duration::from_secs(10);
    run_init(&client, &ctx)
        .await
        .expect("re-init after a PULSUS_LOG_ROLLUP_RESOLUTION change must succeed");

    let names_after = table_names(&client, db).await;
    assert!(
        names_after.contains(&"log_metrics_10s".to_string()),
        "the new-resolution rollup table must be created: {names_after:?}"
    );
    assert!(
        names_after.contains(&"log_metrics_10s_mv".to_string()),
        "the new-resolution rollup MV must be created: {names_after:?}"
    );
    assert!(
        names_after.contains(&"log_metrics_5s".to_string()),
        "the old-resolution rollup table must be retained, not dropped: {names_after:?}"
    );
    assert!(
        names_after.contains(&"log_metrics_5s_mv".to_string()),
        "the old-resolution rollup MV must be retained, not dropped: {names_after:?}"
    );
}

/// The last admitted metric millisecond (issue #137): the final millisecond
/// of day 49_709 (2106-02-06), whose floor-seconds value `4_294_943_999` is
/// the last whole second of the last fully u32-representable UTC day.
const BOUNDARY_TS_MS: i64 = 49_710 * 86_400_000 - 1;

/// A day-50_000 (2106-11-22) instant, inside `(2106-02-07, 2149-06-06]`:
/// partitions correctly (u16 `Date` range) but its seconds value
/// `4_320_000_000` exceeds `u32::MAX`, so the pre-#137 TTL expression wraps
/// for it.
const DAY_50_000_MS: i64 = 50_000 * 86_400_000;

/// The saturating metric TTL expression the file declares (issue #137,
/// the millisecond sibling of #131's trace form), as a SELECT-able snippet
/// over a literal `ts`.
fn new_ms_ttl_expr(ts_ms: i64, retention_days: u32) -> String {
    format!(
        "toDateTime(least(intDiv(toInt64({ts_ms}), 1000) + {retention_days} * 86400, 4294967295))"
    )
}

/// Issue #137 (mirroring #131 AC10a/b/d for the millisecond form): semantics
/// of the saturating metric TTL expression on a live 24.8 server —
/// (a) for a normal-range timestamp it is value-identical to the pre-#137
///     `toDateTime(fromUnixTimestamp64Milli(ts)) + INTERVAL n DAY` form;
/// (b) at the last admitted millisecond it clamps exactly to
///     `toDateTime(4294967295)` (2106-02-07T06:28:15Z);
/// (d) a build at `retention_days = u32::MAX` is accepted by the server,
///     both millisecond tables' DDL carries the extreme retention product,
///     the expression clamps an admitted present-day timestamp exactly to
///     `toDateTime(4294967295)`, and the un-clamped seconds arithmetic
///     stays Int64.
#[tokio::test]
async fn metric_ttl_expression_is_equivalent_in_range_and_saturates_at_the_boundary() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_metric_ttl_expr");
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    run_init(&client, &ctx).await.expect("run_init");

    // (a) Equivalence for a normal-range timestamp (2023-11-14T22:13:20Z).
    let normal_ts_ms: i64 = 1_700_000_000_123;
    let new_expr = new_ms_ttl_expr(normal_ts_ms, 7);
    let equal = count(
        &client,
        &format!(
            "SELECT toUInt64({new_expr} = \
             (toDateTime(fromUnixTimestamp64Milli(toInt64({normal_ts_ms}))) + INTERVAL 7 DAY)) AS n"
        ),
    )
    .await;
    assert_eq!(
        equal, 1,
        "new expression must equal the pre-#137 expiry for a normal-range ts"
    );

    // (b) Saturation at the last admitted millisecond.
    let boundary_expr = new_ms_ttl_expr(BOUNDARY_TS_MS, 7);
    let saturated = count(
        &client,
        &format!("SELECT toUInt64({boundary_expr} = toDateTime(4294967295)) AS n"),
    )
    .await;
    assert_eq!(
        saturated, 1,
        "last-admitted ms + 7d must clamp exactly to toDateTime(4294967295)"
    );

    // (d) Extreme retention: a build at retention_days = u32::MAX is
    // accepted on both millisecond tables, and the expression clamps an
    // admitted present-day ts exactly to the u32::MAX instant. Also pin the
    // arithmetic type: the un-clamped sum stays Int64 on the server.
    drop_database(&client, db).await;
    ctx.retention_days = u32::MAX;
    run_init(&client, &ctx)
        .await
        .expect("a build at retention_days = u32::MAX must be accepted");
    for table in ["metric_samples", "metric_hist_samples"] {
        let ddl = create_table_query(&client, db, table).await;
        assert!(
            ddl.contains("(4294967295 * 86400)"),
            "{table}'s TTL must carry the extreme retention product: {ddl}"
        );
    }
    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("post-epoch clock")
            .as_millis(),
    )
    .expect("present-day ms fits in i64");
    let extreme_expr = new_ms_ttl_expr(now_ms, u32::MAX);
    let clamped = count(
        &client,
        &format!("SELECT toUInt64({extreme_expr} = toDateTime(4294967295)) AS n"),
    )
    .await;
    assert_eq!(
        clamped, 1,
        "an admitted present-day ts must clamp exactly to toDateTime(4294967295) \
         at retention_days = u32::MAX"
    );
    let int64_type = count(
        &client,
        &format!(
            "SELECT toUInt64(toTypeName(intDiv(toInt64({now_ms}), 1000) + \
             4294967295 * 86400) = 'Int64') AS n"
        ),
    )
    .await;
    assert_eq!(
        int64_type, 1,
        "the un-clamped seconds arithmetic must resolve to Int64 on the server"
    );

    drop_database(&client, db).await;
}

/// Issue #137 (survival, non-vacuous — mirroring #131 AC10c): directly
/// inserted day-50_000 rows in `metric_samples`, `log_samples`, and
/// `metric_hist_samples` — inside `(2106-02-07, 2149-06-06]`, deliberately
/// bypassing ingest to model pre-existing/non-ingest rows (post-#137
/// ingest rejects the range) — survive `MATERIALIZE TTL` +
/// `OPTIMIZE ... FINAL` under the saturating expression the file declares
/// (retention 7): their expiry clamps to
/// `toDateTime(4294967295)` = 2106-02-07T06:28:15Z, the horizon, not an
/// already-past instant. The same millisecond-table rows DROP once the
/// pre-#137 wrapping expression is re-installed — the wrapped expiry is
/// ~1970-10, so the part reads as long-expired (`ttl_only_drop_parts = 1`).
/// The second phase pins the pre-fix defect in-test: on the pre-#137
/// expression the first phase fails on all three tables.
#[tokio::test]
async fn day_50_000_rows_survive_saturating_ttl_and_drop_under_the_wrapping_ttl() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_ttl_boundary_2106");
    drop_database(&client, db).await;
    let ctx = test_ctx(db); // retention_days = 7
    run_init(&client, &ctx).await.expect("run_init");

    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_samples (metric_name, fingerprint, unix_milli, value) \
                 VALUES ('m_boundary', 1, {DAY_50_000_MS}, 1.0)"
            ),
            &QuerySettings::new(),
            Idempotency::NonIdempotent,
        )
        .await
        .expect("insert day-50_000 metric sample");
    let day_50_000_ns: i64 = DAY_50_000_MS * 1_000_000;
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) \
                 VALUES ('svc-boundary', 1, {day_50_000_ns}, 0, 'body-boundary')"
            ),
            &QuerySettings::new(),
            Idempotency::NonIdempotent,
        )
        .await
        .expect("insert day-50_000 log sample");
    // Unspecified `metric_hist_samples` columns (spans/deltas/custom_values,
    // zero_*, counter_reset_hint) fill with their type defaults — the TTL
    // only reads `unix_milli`.
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_hist_samples \
                     (metric_name, fingerprint, unix_milli, schema, count, sum) \
                 VALUES ('h_boundary', 1, {DAY_50_000_MS}, 0, 4, 2.0)"
            ),
            &QuerySettings::new(),
            Idempotency::NonIdempotent,
        )
        .await
        .expect("insert day-50_000 hist sample");

    let materialize_and_optimize = |table: &'static str| {
        let client = &client;
        async move {
            client
                .execute(
                    &format!(
                        "ALTER TABLE {db}.{table} MATERIALIZE TTL SETTINGS mutations_sync = 2"
                    ),
                    &QuerySettings::new(),
                    Idempotency::Idempotent,
                )
                .await
                .expect("MATERIALIZE TTL");
            client
                .execute(
                    &format!("OPTIMIZE TABLE {db}.{table} FINAL"),
                    &QuerySettings::new(),
                    Idempotency::Idempotent,
                )
                .await
                .expect("OPTIMIZE FINAL");
        }
    };

    for table in ["metric_samples", "log_samples", "metric_hist_samples"] {
        materialize_and_optimize(table).await;
        let survived = count(&client, &format!("SELECT count() AS n FROM {db}.{table}")).await;
        assert_eq!(
            survived, 1,
            "{table}'s day-50_000 row must survive MATERIALIZE TTL + OPTIMIZE FINAL under \
             the saturating expression (fails on the pre-#137 wrapping expression)"
        );
    }

    // Re-install the pre-#137 wrapping expression verbatim on both
    // millisecond tables: the same rows' expiry wraps past u32::MAX to
    // ~1970-10 and the parts are dropped.
    for table in ["metric_samples", "metric_hist_samples"] {
        client
            .execute(
                &format!(
                    "ALTER TABLE {db}.{table} MODIFY TTL \
                     toDateTime(fromUnixTimestamp64Milli(unix_milli)) + INTERVAL 7 DAY DELETE"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("re-install the pre-#137 wrapping TTL");
        materialize_and_optimize(table).await;
        let dropped = count(&client, &format!("SELECT count() AS n FROM {db}.{table}")).await;
        assert_eq!(
            dropped, 0,
            "{table}'s row must drop under the pre-#137 wrapping expression — this pins \
             the defect the saturating expression closes"
        );
    }

    drop_database(&client, db).await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ColumnTypeRow {
    table: String,
    name: String,
    r#type: String,
}

/// Issue #498 criterion 9: **a fresh database creates every fingerprint
/// column as `UInt128`, and nothing was altered to get there.**
///
/// The `CREATE`s were edited in place rather than followed by `ALTER`s,
/// which is only sound while the condition the amendment policy names
/// holds. The two halves of that are checked together: the shape a fresh
/// `--mode init` produces, and the absence of any mutation — a column
/// widened by `ALTER ... MODIFY COLUMN` would rewrite every part in the
/// background and would show up in `system.mutations`, so an empty
/// mutation list is what says the widening came from the `CREATE`.
///
/// The census is the ten places a fingerprint is stored or projected: the
/// eight base tables, and the two materialized views, whose target columns
/// are the same ones read back here through the tables they write into.
#[tokio::test]
async fn a_fresh_database_creates_every_fingerprint_column_as_uint128() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_fp128");
    drop_database(&client, db).await;
    let ctx = test_ctx(db);
    run_init(&client, &ctx).await.expect("run_init");

    // The eight `CREATE`s that carry the column (issue #498 names them as
    // migration ids 4, 5, 6, 7, 8, 9, 23 and 29), with `log_metrics_5s`
    // standing for the resolution-suffixed rollup this context renders.
    let expected_tables = [
        "log_metrics_5s",
        "log_patterns",
        "log_samples",
        "log_streams",
        "log_streams_idx",
        "metric_hist_samples",
        "metric_labels",
        "metric_landing",
        "metric_samples",
        "metric_series",
    ];

    let sql = format!(
        "SELECT table, name, type FROM system.columns \
         WHERE database = '{db}' AND name = 'fingerprint' ORDER BY table"
    );
    let mut stream = client
        .query_stream::<ColumnTypeRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.columns");
    let mut seen: Vec<(String, String)> = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode ColumnTypeRow");
        seen.push((row.table, row.r#type));
    }
    drop(stream);

    let tables: Vec<&str> = seen.iter().map(|(t, _)| t.as_str()).collect();
    for want in expected_tables {
        assert!(
            tables.contains(&want),
            "{want} has no `fingerprint` column in a freshly initialised database; \
             system.columns returned {tables:?}"
        );
    }
    for (table, ty) in &seen {
        assert_eq!(
            ty, "UInt128",
            "{table}.fingerprint is {ty}, not UInt128 — a bare decimal literal above 2^64 would \
             be read as Float64 against it (issue #498)"
        );
    }

    // The two materialized views project the column into the two tables
    // above, so their SELECT list must carry it and the target column is
    // already asserted. Reading the view's own `fingerprint` column type
    // says the projection did not narrow on the way through.
    for (view, target) in [
        ("log_streams_idx_mv", "log_streams_idx"),
        ("log_metrics_5s_mv", "log_metrics_5s"),
    ] {
        let create = create_table_query(&client, db, view).await;
        assert!(
            create.contains("fingerprint"),
            "{view} no longer projects `fingerprint` into {target}:\n{create}"
        );
    }

    // **No column was altered.** `system.mutations` is the record of every
    // `ALTER` a database has run in the background, and a column widened
    // after the fact — `ALTER TABLE … MODIFY COLUMN fingerprint UInt128` —
    // would appear here and would rewrite every part.
    //
    // Issue #498's plan says the list is empty. It is not: `run_init`
    // issues mutations of its own on a fresh database, measured on
    // ClickHouse 26.3.29.7, none of them touching a column. So the
    // assertion is the **set** the initialisation is allowed to issue,
    // with its cardinality — not merely the absence of one kind. Excluding
    // `MODIFY COLUMN` alone would admit `MATERIALIZE COLUMN fingerprint`,
    // which is exactly the shape the criterion exists to rule out.
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct MutationRow {
        command: String,
    }
    let sql =
        format!("SELECT command FROM system.mutations WHERE database = '{db}' ORDER BY command");
    let mut stream = client
        .query_stream::<MutationRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.mutations");
    let mut commands: Vec<String> = Vec::new();
    while let Some(row) = stream.next().await {
        commands.push(row.expect("decode MutationRow").command);
    }
    drop(stream);

    // **A fresh build issues no mutation at all.** Every TTL and every
    // projection is declared in its table's own `CREATE`, so there is no
    // `MODIFY TTL` to materialise and no `ADD PROJECTION` to fill in. The
    // nineteen this used to assert — fifteen `MATERIALIZE TTL` and four
    // `PROJECTION` commands — were the numbered migrations' own cost: each
    // `ALTER` queued a background mutation on a table that had just been
    // created empty.
    //
    // Pinned at zero rather than dropped, because a mutation appearing here
    // means a statement in `schema/schema.sql` mutates a table instead of
    // declaring it, which is the shape the file exists to avoid.
    assert!(
        commands.is_empty(),
        "a fresh build issued {} mutation(s); every TTL and projection belongs \
         in its table's own CREATE: {commands:?}",
        commands.len()
    );

    drop_database(&client, db).await;
}

// ---------------------------------------------------------------------
// The metrics landing table and its views (issues #603, #623)
// ---------------------------------------------------------------------

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct LandingColumnRow {
    name: String,
    r#type: String,
    compression_codec: String,
    default_expression: String,
}

/// `metric_landing`'s 25 columns, in declaration order, with the type and
/// codec each carries **as the server reports them**. Written out so the
/// shipped table cannot drift from the row type the writer sends.
///
/// The codecs are the server's normalised spellings, not the `CREATE`'s text:
/// `Gorilla` is reported as `Gorilla(8)`, and a column with no codec clause
/// reports the empty string. The same normalisation the four shipped TTL
/// assertions read around a parenthesised product.
const LANDING_COLUMNS: [(&str, &str, &str); 25] = [
    ("received_ms", "Int64", "CODEC(DoubleDelta, ZSTD(1))"),
    ("kind", "UInt8", "CODEC(ZSTD(1))"),
    ("metric_name", "LowCardinality(String)", ""),
    ("fingerprint", "UInt128", "CODEC(Delta(8), ZSTD(1))"),
    ("unix_milli", "Int64", "CODEC(DoubleDelta, ZSTD(1))"),
    ("value", "Float64", "CODEC(Gorilla(8), ZSTD(1))"),
    ("labels", "String", "CODEC(ZSTD(5))"),
    ("value_type", "UInt8", "CODEC(ZSTD(1))"),
    ("metric_type", "LowCardinality(String)", ""),
    ("help", "String", "CODEC(ZSTD(1))"),
    ("unit", "String", "CODEC(ZSTD(1))"),
    ("updated_ns", "Int64", "CODEC(DoubleDelta, ZSTD(1))"),
    ("hist_schema", "Int8", "CODEC(ZSTD(1))"),
    (
        "hist_zero_threshold",
        "Float64",
        "CODEC(Gorilla(8), ZSTD(1))",
    ),
    ("hist_zero_count", "UInt64", "CODEC(T64, ZSTD(1))"),
    ("hist_count", "UInt64", "CODEC(T64, ZSTD(1))"),
    ("hist_sum", "Float64", "CODEC(Gorilla(8), ZSTD(1))"),
    ("hist_pos_span_offsets", "Array(Int32)", "CODEC(ZSTD(1))"),
    ("hist_pos_span_lengths", "Array(UInt32)", "CODEC(ZSTD(1))"),
    ("hist_pos_bucket_deltas", "Array(Int64)", "CODEC(ZSTD(1))"),
    ("hist_neg_span_offsets", "Array(Int32)", "CODEC(ZSTD(1))"),
    ("hist_neg_span_lengths", "Array(UInt32)", "CODEC(ZSTD(1))"),
    ("hist_neg_bucket_deltas", "Array(Int64)", "CODEC(ZSTD(1))"),
    ("hist_custom_values", "Array(Float64)", "CODEC(ZSTD(1))"),
    ("hist_counter_reset_hint", "UInt8", "CODEC(ZSTD(1))"),
];

/// A fresh `run_init` creates `metric_landing` with exactly the declared
/// columns, in order, with the declared types and codecs, and the engine,
/// partition, sorting key and fixed settings the design pins. It carries no
/// `event_id` (issue #623). The five `metric_*_mv` views exist beside it.
#[tokio::test]
async fn metric_landing_and_its_views_exist_after_init() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_metric_landing");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let sql = format!(
        "SELECT name, type, compression_codec, default_expression FROM system.columns \
         WHERE database = '{db}' AND table = 'metric_landing' ORDER BY position"
    );
    let mut stream = client
        .query_stream::<LandingColumnRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.columns");
    let mut seen: Vec<LandingColumnRow> = Vec::new();
    while let Some(row) = stream.next().await {
        seen.push(row.expect("decode LandingColumnRow"));
    }
    drop(stream);

    assert_eq!(seen.len(), LANDING_COLUMNS.len(), "column count");
    for (got, (name, ty, codec)) in seen.iter().zip(LANDING_COLUMNS) {
        assert_eq!(got.name, name, "column order");
        assert_eq!(got.r#type, ty, "{name}'s type");
        assert_eq!(got.compression_codec, codec, "{name}'s codec");
    }
    assert!(
        seen.iter().all(|c| c.default_expression.is_empty()),
        "no landing column is filled by the server"
    );

    let create = create_table_query(&client, db, "metric_landing").await;
    for want in [
        "ENGINE = MergeTree",
        "PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))",
        "ORDER BY (kind, metric_name, fingerprint, unix_milli)",
        "ttl_only_drop_parts = 1",
        "merge_with_ttl_timeout = 3600",
    ] {
        assert!(create.contains(want), "expected {want:?} in {create}");
    }

    let names = table_names(&client, db).await;
    for view in [
        "metric_samples_mv",
        "metric_hist_samples_mv",
        "metric_series_mv",
        "metric_metadata_mv",
        "metric_labels_mv",
    ] {
        assert!(
            names.contains(&view.to_string()),
            "{view} must exist after run_init: {names:?}"
        );
    }

    drop_database(&client, db).await;
}

/// `IF NOT EXISTS` is what makes a re-run safe when a creation committed and
/// its response was lost: an existing landing table is adopted, `run_init`
/// returns `Ok`, and the migration is recorded. Without it the retry fails
/// and the migration is never recorded.
///
/// The `CREATE` is written out here rather than read from the catalogue, so
/// the case is a claim about the shipped statement's text and not a
/// tautology over whatever the catalogue happens to hold.
#[tokio::test]
async fn an_existing_landing_table_is_adopted_by_a_rerun() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_metric_landing_adopt");
    drop_database(&client, db).await;
    client
        .execute(
            &format!("CREATE DATABASE IF NOT EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create the database");

    let create = format!(
        "CREATE TABLE IF NOT EXISTS {db}.metric_landing (
             received_ms              Int64  CODEC(DoubleDelta, ZSTD(1)),
             kind                     UInt8  CODEC(ZSTD(1)),
             metric_name              LowCardinality(String),
             fingerprint              UInt128  CODEC(Delta(8), ZSTD(1)),
             unix_milli               Int64  CODEC(DoubleDelta, ZSTD(1)),
             value                    Float64  CODEC(Gorilla, ZSTD(1)),
             labels                   String  CODEC(ZSTD(5)),
             value_type               UInt8  CODEC(ZSTD(1)),
             metric_type              LowCardinality(String),
             help                     String  CODEC(ZSTD(1)),
             unit                     String  CODEC(ZSTD(1)),
             updated_ns               Int64  CODEC(DoubleDelta, ZSTD(1)),
             hist_schema              Int8  CODEC(ZSTD(1)),
             hist_zero_threshold      Float64  CODEC(Gorilla, ZSTD(1)),
             hist_zero_count          UInt64  CODEC(T64, ZSTD(1)),
             hist_count               UInt64  CODEC(T64, ZSTD(1)),
             hist_sum                 Float64  CODEC(Gorilla, ZSTD(1)),
             hist_pos_span_offsets    Array(Int32)  CODEC(ZSTD(1)),
             hist_pos_span_lengths    Array(UInt32)  CODEC(ZSTD(1)),
             hist_pos_bucket_deltas   Array(Int64)  CODEC(ZSTD(1)),
             hist_neg_span_offsets    Array(Int32)  CODEC(ZSTD(1)),
             hist_neg_span_lengths    Array(UInt32)  CODEC(ZSTD(1)),
             hist_neg_bucket_deltas   Array(Int64)  CODEC(ZSTD(1)),
             hist_custom_values       Array(Float64)  CODEC(ZSTD(1)),
             hist_counter_reset_hint  UInt8  CODEC(ZSTD(1))
         ) ENGINE = MergeTree
         PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))
         ORDER BY (kind, metric_name, fingerprint, unix_milli)
         SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600;"
    );
    client
        .execute(&create, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .expect("create metric_landing directly, as a lost response would have left it");

    let before = create_table_query(&client, db, "metric_landing").await;
    run_init(&client, &test_ctx(db))
        .await
        .expect("a re-run adopts the existing table rather than failing");
    assert_eq!(
        before,
        create_table_query(&client, db, "metric_landing").await,
        "the existing table is adopted, not recreated"
    );

    drop_database(&client, db).await;
}

/// The landing table's delete-TTL is the configured hours. The value is
/// declared in the `CREATE`, so a change to it is a rebuild rather than an
/// `ALTER` — which is what this does.
#[tokio::test]
async fn run_init_installs_the_landing_ttl_at_the_configured_hours() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_metric_landing_ttl");
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    ctx.metrics_landing_retention_hours = 1;
    run_init(&client, &ctx).await.expect("run_init at 1 hour");

    let create = create_table_query(&client, db, "metric_landing").await;
    assert!(
        create.contains("least(intDiv(received_ms, 1000) + (1 * 3600), 4294967295)"),
        "the installed TTL must be the configured 1 hour: {create}"
    );

    drop_database(&client, db).await;
    ctx.metrics_landing_retention_hours = 168;
    run_init(&client, &ctx).await.expect("rebuild at 168 hours");
    let create = create_table_query(&client, db, "metric_landing").await;
    assert!(
        create.contains("least(intDiv(received_ms, 1000) + (168 * 3600), 4294967295)"),
        "the TTL must move with the configuration: {create}"
    );
    assert!(
        !create.contains("(1 * 3600)"),
        "the old value must be gone: {create}"
    );

    drop_database(&client, db).await;
}

/// The landing table and all four tables the views maintain carry the
/// configured deduplication window: a view's insert carries a block id
/// derived from the source block, and only a table with a window recognises
/// the repeat.
#[tokio::test]
async fn dedup_settings_reach_the_landing_table_and_all_four_targets() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_metric_dedup_windows");
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    ctx.metrics_dedup_window = 5_000;
    run_init(&client, &ctx).await.expect("run_init");

    for table in [
        "metric_landing",
        "metric_samples",
        "metric_series",
        "metric_metadata",
        "metric_hist_samples",
        "metric_labels",
    ] {
        let create = create_table_query(&client, db, table).await;
        assert!(
            create.contains("non_replicated_deduplication_window = 5000"),
            "{table} must carry the configured block window: {create}"
        );
    }

    drop_database(&client, db).await;
}

/// One landing row, every column, in the table's order. The client sends
/// a full row; a kind-2 row leaves the other kinds' columns at zero.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
struct LandingSeriesRow {
    received_ms: i64,
    kind: u8,
    metric_name: String,
    fingerprint: u128,
    unix_milli: i64,
    value: f64,
    labels: String,
    value_type: u8,
    metric_type: String,
    help: String,
    unit: String,
    updated_ns: i64,
    hist_schema: i8,
    hist_zero_threshold: f64,
    hist_zero_count: u64,
    hist_count: u64,
    hist_sum: f64,
    hist_pos_span_offsets: Vec<i32>,
    hist_pos_span_lengths: Vec<u32>,
    hist_pos_bucket_deltas: Vec<i64>,
    hist_neg_span_offsets: Vec<i32>,
    hist_neg_span_lengths: Vec<u32>,
    hist_neg_bucket_deltas: Vec<i64>,
    hist_custom_values: Vec<f64>,
    hist_counter_reset_hint: u8,
}

/// **Issue #623: a label set is stored once, activity once per hour.** Three
/// metric names over one label set, registered in two hours, land six kind-2
/// rows. `metric_series` keeps the six activity rows and no label text;
/// `metric_labels` keeps the one label set, once the engine has merged.
#[tokio::test]
async fn kind_2_rows_store_one_label_set_and_one_activity_row_per_hour() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_metric_labels");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");
    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let data_client = ChClient::new(data_cfg).await.expect("connect (data)");

    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("fits i64");
    let hour = (now / 3_600_000) * 3_600_000;
    let labels = r#"{"instance":"a:9100","job":"node"}"#;
    let mut rows = Vec::new();
    for name in ["up", "node_load1", "node_cpu_seconds_total"] {
        for bucket in [hour - 3_600_000, hour] {
            rows.push(LandingSeriesRow {
                received_ms: now,
                kind: 2,
                metric_name: name.to_string(),
                fingerprint: 77,
                unix_milli: bucket,
                labels: labels.to_string(),
                ..Default::default()
            });
        }
    }
    // Two blocks, so the label table holds two parts until it merges.
    data_client
        .insert_block("metric_landing", &rows[..3])
        .await
        .expect("insert the first block");
    data_client
        .insert_block("metric_landing", &rows[3..])
        .await
        .expect("insert the second block");

    assert_eq!(
        count(
            &client,
            &format!("SELECT count() AS n FROM {db}.metric_series")
        )
        .await,
        6,
        "one activity row per name per hour"
    );
    let series_columns = count(
        &client,
        &format!(
            "SELECT count() AS n FROM system.columns \
             WHERE database = '{db}' AND table = 'metric_series' AND name = 'labels'"
        ),
    )
    .await;
    assert_eq!(series_columns, 0, "metric_series stores no label text");

    client
        .execute(
            &format!("OPTIMIZE TABLE {db}.metric_labels FINAL"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("merge metric_labels");
    assert_eq!(
        count(
            &client,
            &format!("SELECT count() AS n FROM {db}.metric_labels")
        )
        .await,
        1,
        "one row per label set"
    );
    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM {db}.metric_labels \
                 WHERE fingerprint = 77 AND labels = '{labels}'"
            ),
        )
        .await,
        1,
        "the label set is the one the kind-2 rows carried"
    );

    drop_database(&client, db).await;
}

/// The `log_landing` columns, in the table's own declaration order, with the
/// type and codec the server reports back for each. The codec spellings are
/// the SERVER's, read back — `Delta(8)` and `Gorilla(8)` carry their byte
/// width where the DDL writes them bare.
const LOG_LANDING_COLUMNS: &[(&str, &str, &str)] = &[
    ("event_id", "UUID", ""),
    ("received_ms", "Int64", "CODEC(DoubleDelta, ZSTD(1))"),
    ("kind", "UInt8", "CODEC(ZSTD(1))"),
    ("service", "LowCardinality(String)", ""),
    ("fingerprint", "UInt128", "CODEC(Delta(8), ZSTD(1))"),
    ("timestamp_ns", "Int64", "CODEC(DoubleDelta, ZSTD(1))"),
    ("severity", "Int8", "CODEC(ZSTD(1))"),
    ("body", "String", "CODEC(ZSTD(1))"),
    ("structured_metadata", "String", "CODEC(ZSTD(1))"),
    ("month", "Date", "CODEC(ZSTD(1))"),
    ("labels", "String", "CODEC(ZSTD(5))"),
    ("updated_ns", "Int64", "CODEC(DoubleDelta, ZSTD(1))"),
    ("pattern", "String", "CODEC(ZSTD(1))"),
    ("pattern_count", "UInt64", "CODEC(T64, ZSTD(1))"),
];

/// **T44.** A fresh `run_init` creates `log_landing` with exactly the declared
/// columns, in order, with the declared types and codecs, `event_id` defaulted
/// by the server's own time-ordered UUID function, and the engine, partition,
/// sorting key and fixed settings the design pins. **All five `log_*_mv`
/// views exist beside it.**
///
/// It fails on an absent statement, and on a view whose `TO` target does not
/// exist yet — which the server refuses at `CREATE`.
#[tokio::test]
async fn log_landing_and_its_views_exist_after_init() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_log_landing");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let sql = format!(
        "SELECT name, type, compression_codec, default_expression FROM system.columns \
         WHERE database = '{db}' AND table = 'log_landing' ORDER BY position"
    );
    let mut stream = client
        .query_stream::<LandingColumnRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.columns");
    let mut seen: Vec<LandingColumnRow> = Vec::new();
    while let Some(row) = stream.next().await {
        seen.push(row.expect("decode LandingColumnRow"));
    }
    drop(stream);

    assert_eq!(seen.len(), LOG_LANDING_COLUMNS.len(), "column count");
    for (got, (name, ty, codec)) in seen.iter().zip(LOG_LANDING_COLUMNS.iter().copied()) {
        assert_eq!(got.name, name, "column order");
        assert_eq!(got.r#type, ty, "{name}'s type");
        assert_eq!(got.compression_codec, codec, "{name}'s codec");
    }
    assert_eq!(
        seen[0].default_expression, "generateUUIDv7()",
        "the landed event's identity is the server's, not the writer's"
    );

    let create = create_table_query(&client, db, "log_landing").await;
    for want in [
        "ENGINE = MergeTree",
        "PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))",
        "ORDER BY (kind, service, fingerprint, timestamp_ns)",
        "ttl_only_drop_parts = 1",
        "merge_with_ttl_timeout = 3600",
    ] {
        assert!(create.contains(want), "expected {want:?} in {create}");
    }

    let names = table_names(&client, db).await;
    const LOG_VIEWS: [&str; 5] = [
        "log_samples_mv",
        "log_streams_mv",
        "log_streams_idx_mv",
        "log_metrics_5s_mv",
        "log_patterns_mv",
    ];
    for view in LOG_VIEWS {
        assert!(
            names.contains(&view.to_string()),
            "{view} must exist after run_init: {names:?}"
        );
    }
    drop_database(&client, db).await;
}

/// **T45.** `IF NOT EXISTS` is what makes a re-run safe when a creation
/// committed and its response was lost: an existing `log_landing` is adopted,
/// `run_init` returns `Ok`, and the migration is recorded.
///
/// **Running `run_init` twice would construct nothing**: the first run
/// creates the table, so the second is a no-op and could not fail on the
/// defect this names. The `CREATE` is issued directly
/// instead — the state a committed creation whose response was lost leaves —
/// and written out here rather than read from the catalogue, so the case is a
/// claim about the shipped statement's text and not a tautology over whatever
/// the catalogue happens to hold.
#[tokio::test]
async fn an_existing_log_landing_table_is_adopted_by_a_rerun() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_log_landing_adopt");
    drop_database(&client, db).await;
    client
        .execute(
            &format!("CREATE DATABASE IF NOT EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("create the database");

    let create = format!(
        "CREATE TABLE IF NOT EXISTS {db}.log_landing (
             event_id             UUID DEFAULT generateUUIDv7(),
             received_ms          Int64  CODEC(DoubleDelta, ZSTD(1)),
             kind                 UInt8  CODEC(ZSTD(1)),
             service              LowCardinality(String),
             fingerprint          UInt128  CODEC(Delta(8), ZSTD(1)),
             timestamp_ns         Int64  CODEC(DoubleDelta, ZSTD(1)),
             severity             Int8  CODEC(ZSTD(1)),
             body                 String  CODEC(ZSTD(1)),
             structured_metadata  String  CODEC(ZSTD(1)),
             month                Date  CODEC(ZSTD(1)),
             labels               String  CODEC(ZSTD(5)),
             updated_ns           Int64  CODEC(DoubleDelta, ZSTD(1)),
             pattern              String  CODEC(ZSTD(1)),
             pattern_count        UInt64  CODEC(T64, ZSTD(1))
         ) ENGINE = MergeTree
         PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))
         ORDER BY (kind, service, fingerprint, timestamp_ns)
         SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600;"
    );
    client
        .execute(&create, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .expect("create log_landing directly, as a lost response would have left it");

    let before = create_table_query(&client, db, "log_landing").await;
    run_init(&client, &test_ctx(db))
        .await
        .expect("a re-run adopts the existing table rather than failing");
    assert_eq!(
        before,
        create_table_query(&client, db, "log_landing").await,
        "the existing table is adopted, not recreated"
    );

    drop_database(&client, db).await;
}

/// **T46.** The logs landing table's delete-TTL is the configured hours.
/// The value is declared in the `CREATE`, so a change to it is a rebuild
/// rather than an `ALTER`.
///
/// **The server parenthesises the multiplication** when it renders the
/// expression back, so the unparenthesised form the statement is written in
/// would reject a correctly installed TTL.
#[tokio::test]
async fn run_init_installs_the_log_landing_ttl_at_the_configured_hours() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_log_landing_ttl");
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    ctx.log_landing_retention_hours = 24;
    run_init(&client, &ctx).await.expect("run_init at 24 hours");

    let create = create_table_query(&client, db, "log_landing").await;
    assert!(
        create.contains("least(intDiv(received_ms, 1000) + (24 * 3600), 4294967295)"),
        "the installed TTL must be the configured 24 hours: {create}"
    );

    drop_database(&client, db).await;
    ctx.log_landing_retention_hours = 168;
    run_init(&client, &ctx).await.expect("rebuild at 168 hours");
    let create = create_table_query(&client, db, "log_landing").await;
    assert!(
        create.contains("least(intDiv(received_ms, 1000) + (168 * 3600), 4294967295)"),
        "the TTL must move with the configuration: {create}"
    );
    assert!(
        !create.contains("(24 * 3600)"),
        "the old value must be gone: {create}"
    );

    drop_database(&client, db).await;
}

/// **T47.** The logs landing table and all five tables the views maintain
/// carry the configured deduplication window: a view's insert carries a block
/// id derived from the source block, and only a table with a window recognises
/// the repeat.
///
/// **Single-node, so the non-replicated name is the one in force** — and no
/// `replicated_` name may reach these tables, which do not carry one.
/// `the_cluster_windows_are_the_replicated_pair_on_every_write_path_table`
/// (`crates/pulsus-schema/tests/live_cluster.rs`) is the clustered half.
#[tokio::test]
async fn the_log_dedup_window_reaches_the_landing_table_and_all_five_targets() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_log_dedup_windows");
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    ctx.log_dedup_window = 5_000;
    run_init(&client, &ctx).await.expect("run_init");

    for table in [
        "log_landing",
        "log_samples",
        "log_streams",
        "log_streams_idx",
        "log_metrics_5s",
        "log_patterns",
    ] {
        let create = create_table_query(&client, db, table).await;
        assert!(
            create.contains("non_replicated_deduplication_window = 5000"),
            "{table} must carry the configured block window: {create}"
        );
        // `non_replicated_deduplication_window` CONTAINS the replicated
        // spelling, so the absence claim is about the replicated setting
        // standing on its own — the name the clustered rendering sends.
        assert!(
            !create.contains(" replicated_deduplication_window")
                && !create.contains(",replicated_deduplication_window"),
            "{table} is a plain MergeTree here and carries no replicated \
             window: {create}"
        );
    }

    drop_database(&client, db).await;
}

/// The startup name check reads the server's own catalogues: every required
/// name is there, and a name that is not is reported by name.
#[tokio::test]
async fn required_names_are_read_from_the_server() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");

    let absent = pulsus_schema::absent_server_names(&client, pulsus_schema::REQUIRED_SERVER_NAMES)
        .await
        .expect("the catalogue query runs");
    assert!(
        absent.is_empty(),
        "every name this build sends must exist on the server: {absent:?}"
    );

    let made_up = pulsus_schema::absent_server_names(
        &client,
        &[(
            "pulsus_not_a_setting",
            pulsus_schema::NameCatalogue::Setting,
        )],
    )
    .await
    .expect("the catalogue query runs");
    assert_eq!(made_up, vec!["pulsus_not_a_setting"]);
}

/// A catalogue the deployment's user cannot read is a catalogue this build
/// cannot check, **not** a refusal: `system.merge_tree_settings` needs a grant
/// a user granted only its own database does not hold, and refusing on the
/// denial would stop every least-privilege deployment from starting.
///
/// The readable catalogues are still checked, which is what separates
/// "unreadable" from "unchecked": a made-up setting name in
/// `system.settings`, which any user may read, is still reported absent for
/// the same user.
#[tokio::test]
async fn a_catalogue_the_user_cannot_read_is_unchecked_not_a_refusal() {
    skip_unless_live!();
    let admin = ChClient::new(test_config()).await.expect("connect");
    let user = pulsus_testkit::test_ident("pulsus_schema_it_least_privilege");
    let db = &pulsus_testkit::test_db("pulsus_schema_it_least_privilege_db");

    let exec = |sql: String| {
        let admin = &admin;
        async move {
            admin
                .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    };
    exec(format!("DROP USER IF EXISTS {user}")).await;
    exec(format!("CREATE DATABASE IF NOT EXISTS {db}")).await;
    exec(format!("CREATE USER {user} IDENTIFIED WITH no_password")).await;
    exec(format!("GRANT ALL ON {db}.* TO {user}")).await;

    let mut as_user = test_config();
    as_user.user = user.clone();
    as_user.database = db.to_string();
    let client = ChClient::new(as_user)
        .await
        .expect("connect as the least-privilege user");

    // The user cannot read `system.merge_tree_settings`, so
    // `merge_with_ttl_timeout` goes unchecked rather than reported absent.
    let absent = pulsus_schema::absent_server_names(&client, pulsus_schema::REQUIRED_SERVER_NAMES)
        .await
        .expect("an unreadable catalogue is not an error");
    assert!(
        absent.is_empty(),
        "a denied catalogue must not refuse startup: {absent:?}"
    );

    // And the readable ones are still checked.
    let made_up = pulsus_schema::absent_server_names(
        &client,
        &[
            (
                "pulsus_not_a_setting",
                pulsus_schema::NameCatalogue::Setting,
            ),
            (
                "pulsus_not_a_merge_tree_setting",
                pulsus_schema::NameCatalogue::MergeTreeSetting,
            ),
        ],
    )
    .await
    .expect("the readable catalogue answers");
    assert_eq!(
        made_up,
        vec!["pulsus_not_a_setting"],
        "the readable catalogue still reports an absent name; the unreadable one reports \
         nothing either way"
    );

    exec(format!("DROP USER IF EXISTS {user}")).await;
    drop_database(&admin, db).await;
}
