//! Integration tests against a real ClickHouse server — the "every DDL
//! block in schemas.md is rendered and executed against a fresh ClickHouse
//! in CI" contract (docs/schemas.md §1, issue #5).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, mirroring
//! `crates/pulsus-clickhouse/tests/live_clickhouse.rs`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
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
use pulsus_schema::{
    Family, RenderCtx, SchemaParams, apply_ttl, check_version, reconcile, run_init,
};

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
struct MvChecksumRow {
    mv_name: String,
    checksum: String,
    updated_at: u32,
}

async fn mv_checksum(client: &ChClient, db: &str, mv_name: &str) -> Option<String> {
    let sql = format!(
        "SELECT mv_name, checksum, updated_at FROM {db}.mv_checksums FINAL WHERE mv_name = '{mv_name}'"
    );
    let mut stream = client
        .query_stream::<MvChecksumRow>(&sql, &QuerySettings::new())
        .await
        .expect("query mv_checksums");
    stream.next().await.map(|r| r.expect("decode row").checksum)
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
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct LogSampleRow {
    service: String,
    fingerprint: u64,
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
    fingerprint: u64,
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
    let db = "pulsus_schema_it_full";
    drop_database(&client, db).await;
    let ctx = test_ctx(db);

    run_init(&client, &ctx).await.expect("run_init (first run)");

    let expected_base_tables = [
        "schema_migrations",
        "mv_checksums",
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

    // Second run: idempotent, no error (in particular, no MigrationDrift —
    // rendering the same params against the same catalog must reproduce
    // identical checksums).
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
                 WHERE metric_name = 'http_requests_total' AND fingerprint IN (18374588331335825905)"
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
    let db = "pulsus_schema_it_sm";
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

    // AC-2: a second run_init no-ops ids 21/22 — no MigrationDrift.
    run_init(&client, &ctx)
        .await
        .expect("run_init (second run, no-op — ids 21/22 must not drift)");

    drop_database(&client, db).await;
}

/// MV crash-safety (issue #5 plan amendment 1): the view being physically
/// absent from `system.tables` — even though `mv_checksums` still records
/// the current-looking checksum from before the simulated crash — must
/// still trigger a recreate. This simulates a crash between the `DROP VIEW`
/// and `CREATE MATERIALIZED VIEW` steps.
#[tokio::test]
async fn reconcile_recreates_a_materialized_view_missing_from_system_tables() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_schema_it_mv_absent";
    drop_database(&client, db).await;
    let ctx = test_ctx(db);

    reconcile(&client, &ctx).await.expect("initial reconcile");
    let checksum_before = mv_checksum(&client, db, "log_streams_idx_mv")
        .await
        .expect("checksum recorded after initial reconcile");

    // Simulate "crashed after DROP VIEW, before CREATE": the checksum row
    // still says current, but the object itself is gone.
    client
        .execute(
            &format!("DROP VIEW IF EXISTS {db}.log_streams_idx_mv"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("simulate crash: drop the view out from under mv_checksums");
    assert!(
        !table_names(&client, db)
            .await
            .contains(&"log_streams_idx_mv".to_string())
    );

    reconcile(&client, &ctx)
        .await
        .expect("reconcile must self-heal");

    assert!(
        table_names(&client, db)
            .await
            .contains(&"log_streams_idx_mv".to_string()),
        "reconcile must recreate a view missing from system.tables even when mv_checksums looked current"
    );
    let checksum_after = mv_checksum(&client, db, "log_streams_idx_mv")
        .await
        .expect("checksum recorded after self-heal");
    assert_eq!(
        checksum_before, checksum_after,
        "the rendered template did not change, so the healed checksum must match"
    );
}

/// MV crash-safety (issue #5 plan amendment 1): a `mv_checksums` row that
/// no longer matches the current rendered checksum — simulating a crash
/// between `CREATE MATERIALIZED VIEW` and the checksum upsert (or an
/// external corruption) — must still trigger a recreate even though the
/// view object itself is present and correct.
#[tokio::test]
async fn reconcile_recreates_a_materialized_view_whose_checksum_row_is_stale() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_schema_it_mv_stale_checksum";
    drop_database(&client, db).await;
    let ctx = test_ctx(db);

    reconcile(&client, &ctx).await.expect("initial reconcile");

    // Simulate "crashed after CREATE, before the checksum upsert" by
    // directly corrupting the recorded checksum — the view itself is left
    // untouched (present and correct), only the bookkeeping is stale. Raw
    // `execute` (not `insert_block`, which cannot take a `db.table`
    // qualified name — see `src/bookkeeping.rs`'s module doc) against the
    // `default`-bound `client`, matching how the controller itself writes
    // bookkeeping rows.
    let now = u32::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs(),
    )
    .expect("timestamp fits u32");
    client
        .execute(
            &format!(
                "INSERT INTO {db}.mv_checksums (mv_name, checksum, updated_at) \
                 VALUES ('log_streams_idx_mv', '0000000000000000', {now})"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("simulate crash: corrupt the recorded checksum");
    assert_eq!(
        mv_checksum(&client, db, "log_streams_idx_mv")
            .await
            .as_deref(),
        Some("0000000000000000")
    );

    reconcile(&client, &ctx)
        .await
        .expect("reconcile must self-heal");

    assert!(
        table_names(&client, db)
            .await
            .contains(&"log_streams_idx_mv".to_string()),
        "the view must still exist after a checksum-mismatch-triggered recreate"
    );
    let healed = mv_checksum(&client, db, "log_streams_idx_mv")
        .await
        .expect("checksum recorded after self-heal");
    assert_ne!(
        healed, "0000000000000000",
        "reconcile must overwrite the stale/bogus checksum with the current rendered one"
    );
}

/// Pure version-gate refusal, proven against a real 24.8 server's actual
/// `SELECT version()` string (parsing/comparison logic itself is unit
/// tested without a container in `src/controller.rs`).
#[tokio::test]
async fn check_version_accepts_the_live_test_servers_reported_version() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let mut stream = client
        .query_stream::<VersionRow>("SELECT version() AS v", &QuerySettings::new())
        .await
        .expect("select version()");
    let row = stream.next().await.expect("one row").expect("decode");
    check_version(&row.v).expect("the live test server must be >= 24.8");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct VersionRow {
    v: String,
}

/// `docs/schemas.md §7` invariant, live: every `_dist` table in a family
/// carries the byte-identical sharding expression. The single-shard live
/// server here still exercises the clustered rendering path end to end
/// (`ON CLUSTER` against a one-node "cluster" is a ClickHouse no-op when no
/// `remote_servers` cluster of that name is configured would fail — so this
/// test only asserts the rendering, not execution; live execution against a
/// real multi-shard cluster is `tests/live_cluster.rs`, CI-side).
#[test]
fn family_sharding_expr_is_the_single_source_of_truth() {
    assert_eq!(
        Family::Metrics.sharding_expr(),
        "cityHash64(metric_name, fingerprint)"
    );
    assert_eq!(Family::Logs.sharding_expr(), "fingerprint");
}

/// Issue #5 fix plan F1: `PULSUS_RETENTION_DAYS` is mutable operational
/// config, excluded from migration identity — a re-init after it changes
/// must succeed (not `MigrationDrift`) and must actually update the TTL
/// (`apply_ttl`'s job, run every `run_init`). Issue #137 re-points the TTL
/// asserts at the saturating expression `apply_ttl` now renders for the
/// metric/log tables, and extends coverage to `metric_hist_samples` — the
/// hist assert fails on pre-#137 main, where the table is absent from
/// `apply_ttl` and its TTL stays CREATE-static (the retention-propagation
/// gap #137 closes).
#[tokio::test]
async fn run_init_after_retention_days_change_succeeds_and_updates_ttl() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_schema_it_retention_change";
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    ctx.retention_days = 7;

    run_init(&client, &ctx)
        .await
        .expect("run_init (retention_days=7)");
    // `apply_ttl` (issues #131/#137) supersedes the CREATE-time TTL with the
    // saturating expression; ClickHouse normalizes the rendered
    // `{{retention_days}} * 86400` product by wrapping it in parens
    // (live_traces.rs pins the same 24.8 normalization for the ns form).
    let before_metric = create_table_query(&client, db, "metric_samples").await;
    assert!(
        before_metric.contains("least(intDiv(unix_milli, 1000) + (7 * 86400), 4294967295)"),
        "metric_samples' initial TTL must reflect retention_days=7: {before_metric}"
    );
    let before_hist = create_table_query(&client, db, "metric_hist_samples").await;
    assert!(
        before_hist.contains("least(intDiv(unix_milli, 1000) + (7 * 86400), 4294967295)"),
        "metric_hist_samples' initial TTL must be the runtime saturating form (fails on \
         pre-#137 main, where the table is absent from apply_ttl): {before_hist}"
    );
    let before_log = create_table_query(&client, db, "log_samples").await;
    assert!(
        before_log.contains("least(intDiv(timestamp_ns, 1000000000) + (7 * 86400), 4294967295)"),
        "log_samples' initial TTL must reflect retention_days=7: {before_log}"
    );

    ctx.retention_days = 30;
    run_init(&client, &ctx)
        .await
        .expect("re-init after a PULSUS_RETENTION_DAYS change must succeed, not MigrationDrift");

    for table in ["metric_samples", "log_samples", "metric_hist_samples"] {
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

/// Issue #5 fix plan F1: `PULSUS_LOG_ROLLUP_RESOLUTION` is config-derived
/// into the rollup table/MV *name* (`MigrationScope::ConfigName`) — a
/// re-init after it changes must succeed, create the new-named objects, and
/// leave the old ones (and their data) in place rather than dropping them.
/// The orphan-warning selection logic itself is unit-tested in
/// `src/controller.rs` (`orphaned_rollup_siblings`); this test proves the
/// live functional outcome (new created, old retained, no drift/error).
#[tokio::test]
async fn run_init_after_log_rollup_resolution_change_creates_new_table_and_retains_old() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_schema_it_rollup_change";
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    ctx.log_rollup = Duration::from_secs(5);

    run_init(&client, &ctx).await.expect("run_init (rollup=5s)");
    let names_before = table_names(&client, db).await;
    assert!(names_before.contains(&"log_metrics_5s".to_string()));
    assert!(names_before.contains(&"log_metrics_5s_mv".to_string()));

    ctx.log_rollup = Duration::from_secs(10);
    run_init(&client, &ctx).await.expect(
        "re-init after a PULSUS_LOG_ROLLUP_RESOLUTION change must succeed, not MigrationDrift",
    );

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

/// The saturating metric TTL expression `apply_ttl` renders (issue #137,
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
/// (d) `apply_ttl` with `retention_days = u32::MAX` is accepted by the
///     server, both millisecond tables' DDL carries the extreme retention
///     product, the expression clamps an admitted present-day timestamp
///     exactly to `toDateTime(4294967295)`, and the un-clamped seconds
///     arithmetic stays Int64.
#[tokio::test]
async fn metric_ttl_expression_is_equivalent_in_range_and_saturates_at_the_boundary() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_schema_it_metric_ttl_expr";
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

    // (d) Extreme retention: the rendered ALTER is accepted at
    // retention_days = u32::MAX on both millisecond tables, and the
    // expression clamps an admitted present-day ts exactly to the u32::MAX
    // instant. Also pin the arithmetic type: the un-clamped sum stays Int64
    // on the server.
    ctx.retention_days = u32::MAX;
    apply_ttl(&client, &ctx)
        .await
        .expect("apply_ttl at retention_days = u32::MAX must be accepted");
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
/// `OPTIMIZE ... FINAL` under the saturating expression `apply_ttl`
/// installed (retention 7): their expiry clamps to
/// `toDateTime(4294967295)` = 2106-02-07T06:28:15Z, the horizon, not an
/// already-past instant. The same millisecond-table rows DROP once the
/// pre-#137 wrapping expression is re-installed — the wrapped expiry is
/// ~1970-10, so the part reads as long-expired (`ttl_only_drop_parts = 1`).
/// The second phase pins the pre-fix defect in-test: on pre-#137
/// `apply_ttl` text the first phase fails on all three tables
/// (`metric_hist_samples` included: pre-#137 it kept its wrap-prone
/// CREATE-time TTL, being absent from the runtime ALTER list).
#[tokio::test]
async fn day_50_000_rows_survive_saturating_ttl_and_drop_under_the_wrapping_ttl() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = "pulsus_schema_it_ttl_boundary_2106";
    drop_database(&client, db).await;
    let ctx = test_ctx(db); // retention_days = 7; run_init applies the new TTL
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
    // millisecond tables (for `metric_hist_samples` it is the CREATE-time
    // TTL the table kept pre-#137, being absent from `apply_ttl`): the same
    // rows' expiry wraps past u32::MAX to ~1970-10 and the parts are
    // dropped.
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
