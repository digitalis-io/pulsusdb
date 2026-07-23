//! Clustered-mode integration tests: `_dist` write/read-back and the live
//! sharding-identity invariant (docs/schemas.md §7), against the 2-shard +
//! Keeper fixture at `ci/clickhouse-cluster/compose.yaml` (issue #5 plan
//! amendment, "2-shard leg" fold-in).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, matching the crate's other live
//! tests, but requires the **cluster** fixture specifically (not the plain
//! single-node container `live_schema.rs` uses):
//!
//! ```text
//! podman-compose -f ci/clickhouse-cluster/compose.yaml up -d
//! # or: docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-schema --test live_cluster
//! podman-compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```
//!
//! Each shard is queried directly by its fixture-static container IP
//! (compose.yaml's 172.28.0.0/24 `ipv4_address`es, KISS directive on issue
//! #5 — no DNS aliases, no hostname resolution), not through a name or a
//! load balancer. Overridable via `PULSUS_TEST_CH_SHARD1_HOST` /
//! `PULSUS_TEST_CH_SHARD1_HTTP_PORT` and the `_SHARD2_` equivalents, for
//! runtimes where the host cannot route directly to the compose network
//! (falls back to the fixture's published host ports in that case).

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, ChRow, Idempotency, QuerySettings, Row};
use pulsus_schema::{Family, RenderCtx, SchemaParams, run_init};

const CLUSTER_NAME: &str = "pulsus_test_cluster";
// Each test uses its own dedicated database (mirroring live_schema.rs's
// convention) rather than one shared constant: back-to-back `DROP DATABASE
// ON CLUSTER` + immediate re-`CREATE` of the same `Replicated*` zoo paths
// races ClickHouse's asynchronous replica-metadata cleanup in ZooKeeper/
// Keeper (`REPLICA_ALREADY_EXISTS`) when two tests share a name; distinct
// databases (and therefore distinct zoo paths, which are `{{db}}`-qualified)
// sidestep the race entirely rather than papering over it with a retry.
const TEST_DB_DIST: &str = "pulsus_schema_it_cluster_dist";
const TEST_DB_BOOKKEEPING: &str = "pulsus_schema_it_cluster_bookkeeping";

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

/// `host_env`/`default_host` default to the fixture's static container IP
/// (compose.yaml, 172.28.0.0/24) — each shard is dialed directly by IP, not
/// a name (KISS directive on issue #5). `port_env`/`default_port` default
/// to the plain in-cluster HTTP port (8123); overriding both env vars lets
/// this test run against the fixture's published host ports instead, for a
/// runtime where the host cannot route to the compose network directly.
fn shard_config(
    host_env: &str,
    default_host: &str,
    port_env: &str,
    default_port: u16,
) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var(host_env).unwrap_or_else(|_| default_host.to_string()),
        http_port: std::env::var(port_env)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(default_port),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

fn shard1_config() -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD1_HOST",
        "172.28.0.11",
        "PULSUS_TEST_CH_SHARD1_HTTP_PORT",
        8123,
    )
}

fn shard2_config() -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD2_HOST",
        "172.28.0.12",
        "PULSUS_TEST_CH_SHARD2_HTTP_PORT",
        8123,
    )
}

fn cluster_ctx(db: &str) -> SchemaParams {
    RenderCtx {
        db: db.to_string(),
        cluster: Some(CLUSTER_NAME.to_string()),
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
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
                 run this test (see crates/pulsus-schema/tests/live_cluster.rs for setup)"
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

/// The `CREATE TABLE ... ENGINE = Distributed(...)` statement ClickHouse
/// reports back for `name`, read live from `system.tables` (not from our
/// own rendered string) — this is what makes the sharding-identity
/// assertion below a *live-DDL* check, not just a unit test on the
/// renderer.
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

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct LogSampleRow {
    service: String,
    fingerprint: u64,
    timestamp_ns: i64,
    severity: i8,
    body: String,
}

/// The core clustered-mode acceptance contract (issue #5 plan amendment,
/// 2-shard leg fold-in): `run_init` with `PULSUS_CLUSTER` set renders
/// `Replicated*` engines + `_dist` wrappers via `ON CLUSTER`, the DDL lands
/// on *every* shard (not just the one the client is connected to), a write
/// through a `_dist` table is readable back through `_dist`, and every
/// `_dist` table in a family carries the byte-identical
/// `Family::sharding_expr()` string live in `system.tables`.
#[tokio::test]
async fn run_init_clustered_creates_dist_wrappers_on_every_shard_with_identical_sharding() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");

    // `SYNC`: the default Atomic database engine only soft-deletes on a
    // plain `DROP DATABASE` (physical cleanup, including each dropped
    // `Replicated*` table's Keeper replica znode, is deferred up to
    // `database_atomic_delay_before_drop_table_sec`, 480s by default) — the
    // global bookkeeping replica set's zoo path is db-and-table-qualified
    // only (docs/schemas.md §7), so re-running this test against the same
    // live fixture without `SYNC` collides with the still-registered
    // replica from the previous run (`REPLICA_ALREADY_EXISTS`).
    shard1
        .execute(
            &format!("DROP DATABASE IF EXISTS {TEST_DB_DIST} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database on cluster");

    let ctx = cluster_ctx(TEST_DB_DIST);
    run_init(&shard1, &ctx).await.expect("run_init (clustered)");

    // The DDL must be visible on BOTH shards — proof that `ON CLUSTER`
    // actually distributed it, not just created objects locally on shard1.
    let names1 = table_names(&shard1, TEST_DB_DIST).await;
    let names2 = table_names(&shard2, TEST_DB_DIST).await;
    assert_eq!(names1, names2, "DDL must land identically on every shard");

    let dist_tables = [
        ("metric_series_dist", Family::Metrics),
        ("metric_samples_dist", Family::Metrics),
        ("log_streams_dist", Family::Logs),
        ("log_streams_idx_dist", Family::Logs),
        ("log_samples_dist", Family::Logs),
        ("log_metrics_5s_dist", Family::Logs),
        ("trace_spans_dist", Family::Traces),
        ("trace_attrs_idx_dist", Family::Traces),
        // Service-graph edge ledger (M7-E1, issue #173): co-shards with the
        // rest of the Traces family on `cityHash64(trace_id)`.
        ("trace_edges_dist", Family::Traces),
    ];
    for (table, family) in dist_tables {
        assert!(
            names1.contains(&table.to_string()),
            "missing {table} on shard1"
        );
        let ddl1 = create_table_query(&shard1, TEST_DB_DIST, table).await;
        let ddl2 = create_table_query(&shard2, TEST_DB_DIST, table).await;
        assert_eq!(
            ddl1, ddl2,
            "{table}'s CREATE statement must be identical on every shard"
        );
        assert!(
            ddl1.contains(family.sharding_expr()),
            "{table} must carry its family's sharding expression {:?}, got: {ddl1}",
            family.sharding_expr()
        );
    }

    // Every family table's `_dist` wrapper must carry the byte-identical
    // sharding expression (docs/schemas.md §7 invariant), read live from
    // `system.tables` rather than from our own renderer.
    let metrics_dist = ["metric_series_dist", "metric_samples_dist"];
    let metrics_exprs: Vec<String> = {
        let mut v = Vec::new();
        for t in metrics_dist {
            v.push(create_table_query(&shard1, TEST_DB_DIST, t).await);
        }
        v
    };
    for ddl in &metrics_exprs {
        assert!(ddl.contains(Family::Metrics.sharding_expr()));
    }

    let logs_dist = [
        "log_streams_dist",
        "log_streams_idx_dist",
        "log_samples_dist",
        "log_metrics_5s_dist",
    ];
    for t in logs_dist {
        let ddl = create_table_query(&shard1, TEST_DB_DIST, t).await;
        assert!(ddl.contains(Family::Logs.sharding_expr()));
    }

    let traces_dist = [
        "trace_spans_dist",
        "trace_attrs_idx_dist",
        "trace_edges_dist",
    ];
    for t in traces_dist {
        let ddl = create_table_query(&shard1, TEST_DB_DIST, t).await;
        assert!(ddl.contains(Family::Traces.sharding_expr()));
    }

    // trace_tag_catalog is a Global catalog table (issue #53 adjudication):
    // present on every shard, but never wrapped in a `_dist` table — tag
    // reads serve from the local replica without fan-out.
    for names in [&names1, &names2] {
        assert!(
            names.contains(&"trace_tag_catalog".to_string()),
            "trace_tag_catalog must exist on every shard: {names:?}"
        );
        assert!(
            !names.contains(&"trace_tag_catalog_dist".to_string()),
            "trace_tag_catalog must NOT have a _dist wrapper: {names:?}"
        );
    }

    // Write/read-back through `_dist`: insert into `log_samples_dist` via a
    // client bound directly to `TEST_DB_DIST` (see live_schema.rs's module doc:
    // `insert_block` cannot take a qualified name), then read the row back
    // through the same `_dist` table from the OTHER shard's connection —
    // proving the Distributed layer actually fans reads out cluster-wide.
    let mut data_cfg = shard1_config();
    data_cfg.database = TEST_DB_DIST.to_string();
    let data_client = ChClient::new(data_cfg).await.expect("connect data client");

    let now_ns = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64");
    let row = LogSampleRow {
        service: "checkout".to_string(),
        fingerprint: 0xABCD_EF01_2345_6789,
        timestamp_ns: now_ns,
        severity: 9,
        body: "dist write/read-back smoke".to_string(),
    };
    data_client
        .insert_block("log_samples_dist", std::slice::from_ref(&row))
        .await
        .expect("insert via _dist");

    // Read back via shard2's connection: only correct if the Distributed
    // engine on shard2 fans this query out to wherever the row actually
    // landed (docs/schemas.md §7: `fingerprint` sharding for the logs
    // family). A `Distributed` table's cross-shard forwarding is
    // asynchronous by default (`distributed_foreground_insert = 0`), so the
    // remote shard may not have the block yet the instant `insert_block`
    // returns — polled with the same retry/tolerance discipline as the
    // bookkeeping-consistency test above, rather than a single racy read.
    let sql = format!(
        "SELECT service, fingerprint, timestamp_ns, severity, body FROM {TEST_DB_DIST}.log_samples_dist \
         WHERE fingerprint = {}",
        row.fingerprint
    );
    let mut got = None;
    for _ in 0..40 {
        let mut stream = shard2
            .query_stream::<LogSampleRow>(&sql, &QuerySettings::new())
            .await
            .expect("select via _dist from the other shard");
        if let Some(row) = stream.next().await {
            got = Some(row.expect("decode"));
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(
        got,
        Some(row),
        "row inserted via _dist must become visible cluster-wide through _dist"
    );
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct MigrationRow {
    id: u32,
    checksum: String,
    applied_at: u32,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct MvChecksumRow {
    mv_name: String,
    checksum: String,
    updated_at: u32,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct MetricMetadataRow {
    metric_name: String,
    metric_type: String,
    help: String,
    unit: String,
    updated_ns: i64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct TraceTagRow {
    scope: String,
    key: String,
    val: String,
}

/// Reads `table` `FINAL` (bookkeeping/catalog tables are `ReplacingMergeTree`
/// — duplicate rows from a retried idempotent `execute` must be collapsed)
/// with `select_sequential_consistency = 1` (issue #5 fix plan F2 test
/// requirement): forces the read to wait until Keeper confirms this
/// replica has caught up to the table's latest known state, rather than
/// racing ClickHouse's normal asynchronous replication.
async fn bookkeeping_rows<R>(client: &ChClient, db: &str, table: &str, order_by: &str) -> Vec<R>
where
    R: ChRow + std::fmt::Debug + 'static,
{
    let sql = format!("SELECT * FROM {db}.{table} FINAL ORDER BY {order_by}");
    let settings = QuerySettings::new().set("select_sequential_consistency", 1);
    let mut stream = client
        .query_stream::<R>(&sql, &settings)
        .await
        .unwrap_or_else(|e| panic!("query {db}.{table}: {e}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode row"));
    }
    out
}

/// Polls both shards for up to ~10s until a bookkeeping table's rows agree,
/// tolerating the brief asynchronous-replication window right after `DROP
/// DATABASE ... ON CLUSTER` + `run_init` even under
/// `select_sequential_consistency = 1` (issue #5 fix plan test requirement:
/// "tolerate async replication via poll/retry").
async fn poll_until_matching<R, F, Fut>(mut fetch: F) -> (Vec<R>, Vec<R>)
where
    R: Clone + PartialEq + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = (Vec<R>, Vec<R>)>,
{
    let mut last = (Vec::new(), Vec::new());
    for _ in 0..40 {
        let (a, b) = fetch().await;
        if !a.is_empty() && a == b {
            return (a, b);
        }
        last = (a, b);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    last
}

/// Issue #5 fix plan F2: `schema_migrations`, `mv_checksums`, and
/// `metric_metadata` join ONE shard-less, cluster-wide replica set
/// (`/clickhouse/tables/all/<db>.<table>`) rather than each shard's own
/// per-shard replica set — so bookkeeping/catalog rows written while
/// connected to shard1 must read back identically from shard2 directly
/// (never through `_dist`; these tables carry no `_dist` wrapper).
#[tokio::test]
async fn bookkeeping_and_catalog_tables_are_identical_on_every_shard() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");

    // `SYNC`: see the sibling test's comment on the same statement — forces
    // immediate physical cleanup (including Keeper replica znodes) instead
    // of the Atomic database engine's default deferred drop.
    shard1
        .execute(
            &format!(
                "DROP DATABASE IF EXISTS {TEST_DB_BOOKKEEPING} ON CLUSTER '{CLUSTER_NAME}' SYNC"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database on cluster");

    let ctx = cluster_ctx(TEST_DB_BOOKKEEPING);
    run_init(&shard1, &ctx).await.expect("run_init (clustered)");

    let (migrations1, migrations2) = poll_until_matching(|| async {
        (
            bookkeeping_rows::<MigrationRow>(
                &shard1,
                TEST_DB_BOOKKEEPING,
                "schema_migrations",
                "id",
            )
            .await,
            bookkeeping_rows::<MigrationRow>(
                &shard2,
                TEST_DB_BOOKKEEPING,
                "schema_migrations",
                "id",
            )
            .await,
        )
    })
    .await;
    assert!(
        !migrations1.is_empty(),
        "schema_migrations must have rows after run_init"
    );
    assert_eq!(
        migrations1, migrations2,
        "schema_migrations rows must be identical on every shard"
    );

    let (mvs1, mvs2) = poll_until_matching(|| async {
        (
            bookkeeping_rows::<MvChecksumRow>(
                &shard1,
                TEST_DB_BOOKKEEPING,
                "mv_checksums",
                "mv_name",
            )
            .await,
            bookkeeping_rows::<MvChecksumRow>(
                &shard2,
                TEST_DB_BOOKKEEPING,
                "mv_checksums",
                "mv_name",
            )
            .await,
        )
    })
    .await;
    assert!(
        !mvs1.is_empty(),
        "mv_checksums must have rows after run_init"
    );
    assert_eq!(
        mvs1, mvs2,
        "mv_checksums rows must be identical on every shard"
    );

    // metric_metadata has no M0 writer, so it is legitimately empty — the
    // invariant under test is that both shards agree (both empty counts as
    // "identical"), not that it is populated.
    let metadata1 = bookkeeping_rows::<MetricMetadataRow>(
        &shard1,
        TEST_DB_BOOKKEEPING,
        "metric_metadata",
        "metric_name",
    )
    .await;
    let metadata2 = bookkeeping_rows::<MetricMetadataRow>(
        &shard2,
        TEST_DB_BOOKKEEPING,
        "metric_metadata",
        "metric_name",
    )
    .await;
    assert_eq!(
        metadata1, metadata2,
        "metric_metadata rows must be identical on every shard"
    );

    // trace_tag_catalog (issue #53): the traces tag catalog joins the same
    // shard-less Global replica set — a row written while connected to
    // shard1 must read back identically from shard2 directly (no `_dist`
    // wrapper exists for it). Unlike metric_metadata (legitimately empty in
    // this test), a row is written explicitly so the assertion proves live
    // replication, not just two empty tables agreeing.
    shard1
        .execute(
            &format!(
                "INSERT INTO {TEST_DB_BOOKKEEPING}.trace_tag_catalog (scope, key, val) \
                 VALUES ('span', 'http.status_code', '500')"
            ),
            &QuerySettings::new(),
            Idempotency::NonIdempotent,
        )
        .await
        .expect("insert trace_tag_catalog row via shard1");
    let (tags1, tags2) = poll_until_matching(|| async {
        (
            bookkeeping_rows::<TraceTagRow>(
                &shard1,
                TEST_DB_BOOKKEEPING,
                "trace_tag_catalog",
                "scope, key, val",
            )
            .await,
            bookkeeping_rows::<TraceTagRow>(
                &shard2,
                TEST_DB_BOOKKEEPING,
                "trace_tag_catalog",
                "scope, key, val",
            )
            .await,
        )
    })
    .await;
    assert!(
        !tags1.is_empty(),
        "trace_tag_catalog must have the inserted row"
    );
    assert_eq!(
        tags1, tags2,
        "trace_tag_catalog rows must be identical on every shard"
    );
}
