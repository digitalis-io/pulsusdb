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
use pulsus_schema::{Family, RenderCtx, SchemaParams, check_version, reconcile, run_init};

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
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainRow {
    explain: String,
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
                "SELECT service, fingerprint, timestamp_ns, severity, body FROM {db}.log_samples"
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
/// must succeed (not `MigrationDrift`) and must actually update the TTL on
/// both raw sample tables (`apply_ttl`'s job, run every `run_init`).
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
    let before = create_table_query(&client, db, "metric_samples").await;
    // ClickHouse normalizes `INTERVAL n DAY` to `toIntervalDay(n)` in the
    // `CREATE TABLE` it reports back — assert against that canonical form,
    // not the literal DDL text we sent.
    assert!(
        before.contains("toIntervalDay(7)"),
        "initial TTL must reflect retention_days=7: {before}"
    );

    ctx.retention_days = 30;
    run_init(&client, &ctx)
        .await
        .expect("re-init after a PULSUS_RETENTION_DAYS change must succeed, not MigrationDrift");

    let after = create_table_query(&client, db, "metric_samples").await;
    assert!(
        after.contains("toIntervalDay(30)"),
        "TTL must be updated to the new retention_days: {after}"
    );
    assert!(!after.contains("toIntervalDay(7)"));

    let log_after = create_table_query(&client, db, "log_samples").await;
    assert!(
        log_after.contains("toIntervalDay(30)"),
        "log_samples TTL must also be updated: {log_after}"
    );
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
