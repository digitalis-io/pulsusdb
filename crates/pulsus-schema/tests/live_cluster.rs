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

mod common;

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, ChRow, Idempotency, QuerySettings, Row};
use pulsus_schema::{RenderCtx, SchemaParams};
use pulsus_schema_testkit::run_init;

const CLUSTER_NAME: &str = "pulsus_test_cluster";
// Each test uses its own dedicated database (mirroring live_schema.rs's
// convention) rather than one shared constant: back-to-back `DROP DATABASE
// ON CLUSTER` + immediate re-`CREATE` of the same `Replicated*` zoo paths
// races ClickHouse's asynchronous replica-metadata cleanup in ZooKeeper/
// Keeper (`REPLICA_ALREADY_EXISTS`) when two tests share a name; distinct
// databases (and therefore distinct zoo paths, which are `{{db}}`-qualified)
// sidestep the race entirely rather than papering over it with a retry.
static TEST_DB_DIST: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_dist");
static TEST_DB_CATALOG: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_catalog");
// Issue #603: the two deduplication windows a clustered write-path table
// carries.
static TEST_DB_DEDUP_WINDOWS: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_dedup_windows");
static TEST_DB_DEDUP_PROFILE: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_dedup_profile");
// Issues #584 to #586: the trace landing table, its five targets and the two
// routing wrappers, clustered.
static TEST_DB_TRACE_LANDING: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_trace_landing");
// Issue #497: the retention every replica states.
static TEST_DB_RETENTION: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_retention");
// Issue #496: the newest instant survives a late row on every replica.
static TEST_DB_LATE_ROW: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_late_row");
// Issue #18 part A: each shard builds its slice of the first level once.
static TEST_DB_LEVELS: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_levels");

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
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

/// Shard 1's second replica.
fn shard1b_config() -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD1B_HOST",
        "172.28.0.13",
        "PULSUS_TEST_CH_SHARD1B_HTTP_PORT",
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
        metrics_landing_retention_hours: 6,
        metrics_dedup_window: 10_000,
        log_landing_retention_hours: 6,
        log_dedup_window: 10_000,
        trace_landing_retention_hours: 6,
        trace_dedup_window: 10_000,
        trace_indexed_attributes: Vec::new(),
        graph_job_max_rows: pulsus_schema::GRAPH_JOB_MAX_ROWS,
        rollup_levels: pulsus_schema::DEFAULT_ROLLUP_LEVELS.to_vec(),
        rollup_ranges: pulsus_schema::DEFAULT_ROLLUP_RANGES.to_vec(),
        rollup_counter_buckets: pulsus_schema::DEFAULT_ROLLUP_COUNTER_BUCKETS,
        cold_volume: None,
        metrics_hot_days: None,
        logs_hot_days: None,
        traces_hot_days: None,
        rollup_retention_days: Vec::new(),
        rollup_hot_days: Vec::new(),
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
    /// The tenant (issue #653): the empty tenant's rows.
    org_id: String,
    service: String,
    fingerprint: u128,
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
/// family sharding expression live in `system.tables`.
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

    let ctx = cluster_ctx(&TEST_DB_DIST);
    run_init(&shard1, &ctx).await.expect("run_init (clustered)");

    // The DDL must be visible on BOTH shards — proof that `ON CLUSTER`
    // actually distributed it, not just created objects locally on shard1.
    let names1 = table_names(&shard1, &TEST_DB_DIST).await;
    let names2 = table_names(&shard2, &TEST_DB_DIST).await;
    assert_eq!(names1, names2, "DDL must land identically on every shard");

    // The sharding expression every wrapper in a family must carry
    // (docs/schemas.md §7): one fingerprint maps to one shard, so a series'
    // rollups land where its samples do. Written out here rather than read
    // from a renderer, so this side of the assertion is independent of
    // `schema/schema.sql`.
    const METRICS_KEY: &str = "cityHash64(fingerprint)";
    const LOGS_KEY: &str = "cityHash64(fingerprint)";
    const TRACES_KEY: &str = "cityHash64(trace_id)";
    let dist_tables = [
        ("metric_series_dist", METRICS_KEY),
        ("metric_samples_dist", METRICS_KEY),
        // Issue #623: one label row per series, keyed like the series;
        // nothing is inserted through this wrapper.
        ("metric_labels_dist", METRICS_KEY),
        // Issue #635: the label index, keyed like the series; nothing is
        // inserted through this wrapper.
        ("metric_label_index_dist", METRICS_KEY),
        // Issue #501 part 1: exemplars, keyed like the samples.
        ("metric_exemplars_dist", METRICS_KEY),
        ("log_streams_dist", LOGS_KEY),
        ("log_streams_idx_dist", LOGS_KEY),
        ("log_samples_dist", LOGS_KEY),
        ("log_metrics_5s_dist", LOGS_KEY),
        // The trace family's three routed targets (issues #586, #602):
        // every span of a trace on one shard, `cityHash64(trace_id)`.
        ("spans_dist", TRACES_KEY),
        ("traces_dist", TRACES_KEY),
        ("error_spans_dist", TRACES_KEY),
        // Issue #18 part A: the default rollup levels, keyed like the
        // samples.
        ("metric_samples_5s_dist", METRICS_KEY),
        ("metric_samples_60s_dist", METRICS_KEY),
        ("metric_samples_720s_dist", METRICS_KEY),
        ("metric_samples_7200s_dist", METRICS_KEY),
        ("metric_samples_86400s_dist", METRICS_KEY),
    ];
    for (table, sharding_expr) in dist_tables {
        assert!(
            names1.contains(&table.to_string()),
            "missing {table} on shard1"
        );
        let ddl1 = create_table_query(&shard1, &TEST_DB_DIST, table).await;
        let ddl2 = create_table_query(&shard2, &TEST_DB_DIST, table).await;
        assert_eq!(
            ddl1, ddl2,
            "{table}'s CREATE statement must be identical on every shard"
        );
        assert!(
            ddl1.contains(sharding_expr),
            "{table} must carry its family's sharding expression {sharding_expr:?}, got: {ddl1}"
        );
    }

    // Every family table's `_dist` wrapper must carry the byte-identical
    // sharding expression (docs/schemas.md §7 invariant), read live from
    // `system.tables` rather than from our own renderer.
    let metrics_dist = ["metric_series_dist", "metric_samples_dist"];
    let metrics_exprs: Vec<String> = {
        let mut v = Vec::new();
        for t in metrics_dist {
            v.push(create_table_query(&shard1, &TEST_DB_DIST, t).await);
        }
        v
    };
    for ddl in &metrics_exprs {
        assert!(ddl.contains(METRICS_KEY));
    }

    let logs_dist = [
        "log_streams_dist",
        "log_streams_idx_dist",
        "log_samples_dist",
        "log_metrics_5s_dist",
    ];
    for t in logs_dist {
        let ddl = create_table_query(&shard1, &TEST_DB_DIST, t).await;
        assert!(ddl.contains(LOGS_KEY));
    }

    let traces_dist = ["spans_dist", "traces_dist", "error_spans_dist"];
    for t in traces_dist {
        let ddl = create_table_query(&shard1, &TEST_DB_DIST, t).await;
        assert!(ddl.contains(TRACES_KEY));
    }

    // The trace tag catalogs are global (issue #586): present on every
    // shard, never wrapped in a `_dist` table — tag reads serve from the local
    // replica without fan-out.
    for names in [&names1, &names2] {
        for catalog in ["tag_names", "tag_values"] {
            assert!(
                names.contains(&catalog.to_string()),
                "{catalog} must exist on every shard: {names:?}"
            );
            assert!(
                !names.contains(&format!("{catalog}_dist")),
                "{catalog} must NOT have a _dist wrapper: {names:?}"
            );
        }
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
        org_id: String::new(),
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
        "SELECT org_id, service, fingerprint, timestamp_ns, severity, body FROM {TEST_DB_DIST}.log_samples_dist \
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
struct MetricMetadataRow {
    org_id: String,
    metric_name: String,
    metric_type: String,
    help: String,
    unit: String,
    updated_ns: i64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct TraceTagRow {
    /// Issue #738: the tenant, the key's first column.
    org_id: String,
    scope: String,
    key: String,
    value: String,
    val_type: String,
    /// Issue #496: the latest start of a span carrying the value.
    last_start_ns: i64,
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

/// The catalog tables join ONE shard-less, cluster-wide replica set
/// (`/clickhouse/tables/all/<db>.<table>`) rather than each shard's own
/// per-shard replica set — so a catalog row written while connected to
/// shard1 must read back identically from shard2 directly (never through
/// `_dist`; these tables carry no `_dist` wrapper).
///
/// **This is also what tells a correct replication path from the silently
/// broken one.** A path whose `{shard}` macro was substituted away reaches
/// the same validation point as a correct one on a single node, and only a
/// keeper-backed cluster shows the difference: here `metric_metadata` and
/// `tag_values` must share a replica set while every per-shard table
/// must not.
#[tokio::test]
async fn the_catalog_tables_are_identical_on_every_shard() {
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
            &format!("DROP DATABASE IF EXISTS {TEST_DB_CATALOG} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database on cluster");

    let ctx = cluster_ctx(&TEST_DB_CATALOG);
    run_init(&shard1, &ctx).await.expect("run_init (clustered)");

    // metric_metadata has no M0 writer, so it is legitimately empty — the
    // invariant under test is that both shards agree (both empty counts as
    // "identical"), not that it is populated.
    let metadata1 = bookkeeping_rows::<MetricMetadataRow>(
        &shard1,
        &TEST_DB_CATALOG,
        "metric_metadata",
        "metric_name",
    )
    .await;
    let metadata2 = bookkeeping_rows::<MetricMetadataRow>(
        &shard2,
        &TEST_DB_CATALOG,
        "metric_metadata",
        "metric_name",
    )
    .await;
    assert_eq!(
        metadata1, metadata2,
        "metric_metadata rows must be identical on every shard"
    );

    // tag_values (issue #586): the trace tag-value catalog joins the same
    // shard-less Global replica set — a row written while connected to
    // shard1 must read back identically from shard2 directly (no `_dist`
    // wrapper exists for it). Unlike metric_metadata (legitimately empty in
    // this test), a row is written explicitly so the assertion proves live
    // replication, not just two empty tables agreeing.
    shard1
        .execute(
            &format!(
                "INSERT INTO {TEST_DB_CATALOG}.tag_values \
                     (scope, key, value, val_type, last_start_ns) \
                 VALUES ('span', 'http.status_code', '500', 'int', \
                         toUnixTimestamp64Nano(now64(9)))"
            ),
            &QuerySettings::new(),
            Idempotency::NonIdempotent,
        )
        .await
        .expect("insert tag_values row via shard1");
    let (tags1, tags2) = poll_until_matching(|| async {
        (
            bookkeeping_rows::<TraceTagRow>(
                &shard1,
                &TEST_DB_CATALOG,
                "tag_values",
                "scope, key, value, val_type",
            )
            .await,
            bookkeeping_rows::<TraceTagRow>(
                &shard2,
                &TEST_DB_CATALOG,
                "tag_values",
                "scope, key, value, val_type",
            )
            .await,
        )
    })
    .await;
    assert!(!tags1.is_empty(), "tag_values must have the inserted row");
    assert_eq!(
        tags1, tags2,
        "tag_values rows must be identical on every shard"
    );
}

// ---------------------------------------------------------------------
// The two-shard topology the clustered cases assume
// ---------------------------------------------------------------------

/// **The topology this suite claims, established rather than assumed.**
///
/// `should_run()` is `pulsus_testkit::live_clickhouse_enabled()`, the same
/// gate the single-node suites use, and `shard1_config`/`shard2_config`
/// default their host and port to the compose fixture's static container IPs.
/// Both variables pointed at ONE server is the unsafe direction, and whether a
/// case notices depends on what it asserts: one that claims *distinct*
/// per-shard content does, one that claims *identical* content cannot. So
/// every case that reads the same thing off both shards opens with this: one
/// catalogue read, no insert, and a false pass becomes a failure.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct ShardCountRow {
    shards: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct HostRow {
    host: String,
}

async fn require_two_shard_topology(shard1: &ChClient, shard2: &ChClient) {
    let mut hosts = Vec::new();
    for (i, client) in [shard1, shard2].into_iter().enumerate() {
        let sql = format!(
            "SELECT toUInt64(count(DISTINCT shard_num)) AS shards FROM system.clusters \
             WHERE cluster = '{CLUSTER_NAME}'"
        );
        let mut stream = client
            .query_stream::<ShardCountRow>(&sql, &QuerySettings::new())
            .await
            .expect("read system.clusters");
        let shards = stream
            .next()
            .await
            .expect("one row")
            .expect("decode ShardCountRow")
            .shards;
        drop(stream);
        assert_eq!(
            shards,
            2,
            "shard{} reports {shards} shards in '{CLUSTER_NAME}': this suite's \
             claims are about two, and one server answering for both would \
             pass every identical-content assertion below",
            i + 1
        );

        let mut stream = client
            .query_stream::<HostRow>("SELECT hostName() AS host", &QuerySettings::new())
            .await
            .expect("read hostName()");
        hosts.push(
            stream
                .next()
                .await
                .expect("one row")
                .expect("decode HostRow")
                .host,
        );
    }
    assert_ne!(
        hosts[0], hosts[1],
        "both shard clients reached the same server ({}): the two shard \
         variables are aliased",
        hosts[0]
    );
}

/// **T51.** After a clustered `run_init`, every write-path table carries the
/// **replicated pair** of deduplication windows on **both** shards: the
/// configured block window under the replicated engine's own setting name, and
/// the pinned seconds window — and no `non_replicated_deduplication_window`
/// anywhere, because these tables do not carry it.
///
/// **This is the case for the whole of the deduplication fix, and a clustered
/// deployment is the production shape.** Before it, `apply_ttl` sent the
/// non-replicated name here, so the deployment's configured window was ignored
/// and the server's own default governed; and nothing at all set the seconds
/// window, which forgets a block's hash on a timer whatever the block window
/// says.
///
/// Reading the value back off both shards is the only thing that confirms it
/// took: the `CREATE` in `schema/schema.sql` declares it, so a table built any
/// other way keeps whatever the server's own default is.
#[tokio::test]
async fn the_cluster_windows_are_the_replicated_pair_on_every_write_path_table() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    require_two_shard_topology(&shard1, &shard2).await;

    // Dropped and built ONCE at the configured window: a second build over
    // an existing table adopts it, so the window would stay at the default
    // and this case would assert the default against itself.
    let db = &TEST_DB_DEDUP_WINDOWS;
    drop_cluster_db(&shard1, db).await;
    let mut ctx = cluster_ctx(db);
    ctx.metrics_dedup_window = 5_000;
    ctx.log_dedup_window = 5_000;
    run_init(&shard1, &ctx).await.expect("run_init (clustered)");

    const WRITE_PATH_TABLES: [&str; 15] = [
        "log_landing",
        "log_samples",
        "log_streams",
        "log_streams_idx",
        "log_metrics_5s",
        "log_patterns",
        "metric_landing",
        "metric_samples",
        "metric_series",
        "metric_metadata",
        "metric_hist_samples",
        "metric_exemplars",
        "metric_labels",
        "metric_label_index",
        "metric_label_values",
    ];

    for (i, shard) in [&shard1, &shard2].into_iter().enumerate() {
        for table in WRITE_PATH_TABLES {
            let create = create_table_query(shard, db, table).await;
            assert!(
                create.contains("replicated_deduplication_window = 5000"),
                "shard{}: {table} must carry the configured BLOCK window under \
                 the replicated engine's own setting name: {create}",
                i + 1
            );
            assert!(
                create.contains(&format!(
                    "replicated_deduplication_window_seconds = {}",
                    pulsus_schema::DEDUP_WINDOW_SECONDS
                )),
                "shard{}: {table} must carry the pinned SECONDS window, or a \
                 block's hash is forgotten on a timer whatever the block \
                 window says: {create}",
                i + 1
            );
            assert!(
                !create.contains("non_replicated_deduplication_window"),
                "shard{}: {table} renders a Replicated* engine and does not \
                 carry the non-replicated setting: {create}",
                i + 1
            );
        }
    }

    exec_on(
        &shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

/// **The clustered shape of the trace write path.** Two of the five targets
/// carry a routing wrapper and three do not, and the landing table carries
/// none at all — a push carries many trace ids, so inserting the push itself
/// through a `Distributed` wrapper would split one push per shard.
///
/// **The `SETTINGS` clause is asserted, not merely the engine**: a setting
/// the server accepts on a `CREATE` and then discards leaves no trace in
/// this column, which is how `distributed_foreground_insert` behaves on a
/// `Distributed` table. `fsync_after_insert` and `fsync_directories` are
/// kept, and this is what shows it.
#[tokio::test]
async fn the_clustered_form() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    require_two_shard_topology(&shard1, &shard2).await;

    let db = &TEST_DB_TRACE_LANDING;
    fresh_cluster_db(&shard1, db).await;

    for (i, shard) in [&shard1, &shard2].into_iter().enumerate() {
        for base in ["spans", "traces"] {
            let got = engine_full_on(shard, db, &format!("{base}_dist")).await;
            let want = format!(
                "Distributed('{CLUSTER_NAME}', '{db}', '{base}', \
                 cityHash64(trace_id)) \
                 SETTINGS fsync_after_insert = 1, fsync_directories = 1"
            );
            assert_eq!(
                got,
                want,
                "shard{}: {base}_dist's stored engine, settings clause included",
                i + 1
            );
        }

        // The three catalog targets and the landing table exist on every
        // shard with no routing twin.
        let present = table_names(shard, db).await;
        for name in ["resources", "tag_names", "tag_values", "trace_landing"] {
            assert!(
                present.iter().any(|n| n == name),
                "shard{}: {name} must exist on every shard: {present:?}",
                i + 1
            );
            let twin = format!("{name}_dist");
            assert!(
                !present.contains(&twin),
                "shard{}: {name} must have no routing twin: {present:?}",
                i + 1
            );
        }
    }

    exec_on(
        &shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

/// `system.tables.engine_full` for one object on one shard's own
/// connection — the stored definition, not our own rendered string.
async fn engine_full_on(client: &ChClient, db: &str, name: &str) -> String {
    let sql = format!(
        "SELECT engine_full AS create_table_query FROM system.tables \
         WHERE database = '{db}' AND name = '{name}'"
    );
    let mut stream = client
        .query_stream::<CreateQueryRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.tables engine_full");
    stream
        .next()
        .await
        .expect("row present")
        .expect("decode")
        .create_table_query
}

async fn exec_on(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

async fn fresh_cluster_db(shard1: &ChClient, db: &str) {
    drop_cluster_db(shard1, db).await;
    run_init(shard1, &cluster_ctx(db))
        .await
        .expect("run_init (clustered)");
}

/// Drops `db` on every shard, without building anything.
///
/// **A case that wants a non-default parameter has to build only once.**
/// Every `CREATE` carries `IF NOT EXISTS` and nothing mutates a table in
/// place any more, so a second build over an existing one adopts it and the
/// parameter never lands. `SYNC` forces the physical cleanup, Keeper replica
/// znodes included, rather than the Atomic engine's deferred drop.
async fn drop_cluster_db(shard1: &ChClient, db: &str) {
    exec_on(
        shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct HexRow {
    id: String,
}

/// The sorted, lower-case hex trace ids `table` holds, read on `client`.
async fn trace_ids(client: &ChClient, db: &str, table: &str) -> Vec<String> {
    let sql = format!("SELECT DISTINCT lower(hex(trace_id)) AS id FROM {db}.{table} ORDER BY id");
    let mut stream = client
        .query_stream::<HexRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("read {db}.{table}: {e}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode HexRow").id);
    }
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

async fn count_on(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("count failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").n
}

fn cluster_now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
        - 3_600_000_000_000
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct MetricSampleRow {
    org_id: String,
    fingerprint: u128,
    unix_milli: i64,
    value: f64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SettingRow {
    v: String,
}

/// Issue #560 criterion 22, clustered: under a user profile that disables
/// `deduplicate_insert`, a repeated log block and a repeated metric block
/// are both stored twice — `insert_block` carries no pin — while the
/// repeated trace landing block, sent with the writer's pins, is stored once
/// in `spans`.
#[tokio::test]
async fn a_disabling_profile_keeps_repeated_log_and_metric_blocks_and_drops_the_repeated_trace_block()
 {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    let db: &str = &TEST_DB_DEDUP_PROFILE;
    let user = format!("{db}_dedup_off");
    fresh_cluster_db(&shard1, db).await;
    // Users are local to each node.
    for shard in [&shard1, &shard2] {
        exec_on(shard, &format!("DROP USER IF EXISTS {user}")).await;
        exec_on(
            shard,
            &format!(
                "CREATE USER {user} IDENTIFIED WITH no_password \
                 SETTINGS deduplicate_insert = 'disable'"
            ),
        )
        .await;
        exec_on(shard, &format!("GRANT ALL ON {db}.* TO {user}")).await;
    }

    let mut cfg = shard1_config();
    cfg.database = db.to_string();
    cfg.user = user.clone();
    let as_user = ChClient::new(cfg)
        .await
        .expect("connect as the profile's user");
    let now = cluster_now_ns();
    let log = LogSampleRow {
        org_id: String::new(),
        service: "checkout".to_string(),
        fingerprint: 0x0560_0560_0560_0560,
        timestamp_ns: now,
        severity: 9,
        body: "issue 560 repeated log block".to_string(),
    };
    let metric = MetricSampleRow {
        org_id: String::new(),
        fingerprint: 0x0560_0560_0560_0561,
        unix_milli: now / 1_000_000,
        value: 1.0,
    };
    for _ in 0..2 {
        as_user
            .insert_block("log_samples_dist", std::slice::from_ref(&log))
            .await
            .expect("insert the log block");
        exec_on(
            &shard1,
            &format!("SYSTEM FLUSH DISTRIBUTED {db}.log_samples_dist"),
        )
        .await;
        as_user
            .insert_block("metric_samples_dist", std::slice::from_ref(&metric))
            .await
            .expect("insert the metric block");
        exec_on(
            &shard1,
            &format!("SYSTEM FLUSH DISTRIBUTED {db}.metric_samples_dist"),
        )
        .await;
        push_landing(
            &as_user,
            db,
            &[("00000000000000000000000000000560", "checkout")],
            1,
            "{}",
            "t32-repeated-trace-block",
        )
        .await
        .expect("push the trace landing block");
        exec_on(
            &shard1,
            &format!("SYSTEM FLUSH DISTRIBUTED {db}.spans_dist"),
        )
        .await;
    }

    let tables = [("log_samples", 2u64), ("metric_samples", 2), ("spans", 1)];
    let mut counts = vec![0u64; tables.len()];
    for _ in 0..40 {
        for (i, (table, _)) in tables.iter().enumerate() {
            counts[i] = count_on(&shard1, &format!("SELECT count() AS n FROM {db}.{table}")).await
                + count_on(&shard2, &format!("SELECT count() AS n FROM {db}.{table}")).await;
        }
        if counts.iter().all(|n| *n > 0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let mut settings = Vec::new();
    for shard_cfg in [shard1_config(), shard2_config()] {
        let mut cfg = shard_cfg;
        cfg.user = user.clone();
        let client = ChClient::new(cfg).await.expect("connect as the user");
        let mut stream = client
            .query_stream::<SettingRow>(
                "SELECT toString(getSetting('deduplicate_insert')) AS v",
                &QuerySettings::new(),
            )
            .await
            .expect("read the user's setting");
        settings.push(stream.next().await.expect("a row").expect("decode").v);
    }
    eprintln!("T32 counts log/metric/spans={counts:?}; settings={settings:?}");

    let mut mismatches: Vec<String> = Vec::new();
    for (i, (table, want)) in tables.iter().enumerate() {
        if counts[i] != *want {
            mismatches.push(format!("{table}: expected {want}, got {}", counts[i]));
        }
    }
    for (i, got) in settings.iter().enumerate() {
        if got != "disable" {
            mismatches.push(format!(
                "shard{}: the user's deduplicate_insert reads {got:?}",
                i + 1
            ));
        }
    }
    for shard in [&shard1, &shard2] {
        exec_on(shard, &format!("DROP USER IF EXISTS {user}")).await;
    }
    exec_on(
        &shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    assert!(mismatches.is_empty(), "T32 mismatches: {mismatches:?}");
}

// ---------------------------------------------------------------------
// Issue #586 — the trace write path, clustered
// ---------------------------------------------------------------------
//
// **None of these can run on a single node**: each needs the two-shard
// compose fixture, and each reads a target straight after a push, so each
// waits on `system.distribution_queue.data_files` reaching 0 for the routing
// table rather than sleeping — except the second, whose whole assertion is
// the value of that column at the moment the insert returns.
//
// The push is issued as a literal `INSERT INTO trace_landing` carrying
// `QuerySettings::trace_landing_insert`, which is what the writer sends: this
// crate cannot reach `pulsus-write`'s row type, because that crate depends on
// this one.

static TEST_DB_TRACE_SHARDS: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_trace_shards");
static TEST_DB_TRACE_FOREGROUND: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_trace_foreground");
static TEST_DB_TRACE_COLUMNS: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_trace_columns");
static TEST_DB_TRACE_REFUSAL: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_trace_refusal");
static TEST_DB_TRACE_PROFILE: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_trace_profile");
static TEST_DB_TRACE_CATALOGS: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_trace_catalogs");

/// The per-push row ceiling both pinned row limits carry, the shipped
/// default.
const TRACE_LANDING_MAX_ROWS: u64 = 1_048_576;

/// An instant inside the span retention, so no TTL merge can drop a part in
/// the middle of a case.
fn trace_t0() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after the epoch")
        .as_nanos() as i64;
    (now / 86_400_000_000_000) * 86_400_000_000_000 + 3_600_000_000_000
}

/// Two trace ids `cityHash64` places on **different** shards, asked of the
/// server rather than assumed: the selection is `slots[hash % total_weight]`
/// over a slot map keyed by shard weight, and the fixture's two shards carry
/// equal weight.
async fn two_ids_on_two_shards(client: &ChClient) -> (String, String) {
    let mut zero = None;
    let mut one = None;
    for n in 1..256u32 {
        let hex = format!("{n:032x}");
        // `toUInt64`, because `%` over a `UInt64` and a literal `2`
        // answers `UInt8` and the row type this reads into takes a `u64`.
        let slot = count_on(
            client,
            &format!("SELECT toUInt64(cityHash64(unhex('{hex}')) % 2) AS n"),
        )
        .await;
        if slot == 0 && zero.is_none() {
            zero = Some(hex);
        } else if slot == 1 && one.is_none() {
            one = Some(hex);
        }
        if zero.is_some() && one.is_some() {
            break;
        }
    }
    (
        zero.expect("a trace id on slot 0"),
        one.expect("a trace id on slot 1"),
    )
}

/// The `INSERT` one push of `spans` per trace id makes: one kind-0 row per
/// span, one kind-1 row per trace's service, one kind-2 and one kind-3 row.
fn trace_landing_insert_sql(
    db: &str,
    traces: &[(&str, &str)],
    spans: usize,
    attrs: &str,
) -> String {
    let t0 = trace_t0();
    let mut rows: Vec<String> = Vec::new();
    for (i, (trace_hex, service)) in traces.iter().enumerate() {
        for s in 0..spans {
            rows.push(format!(
                "(1, 0, unhex('{trace_hex}'), unhex('{:016x}'), unhex('0000000000000000'), \
                 {}, 1000, {}, 'GET /api', 2, '{service}', '{attrs}', toDate(0), '', '', '', '', \
                 'string')",
                0x100 + (i * spans + s),
                t0 + (i * spans + s) as i64,
                i + 1,
            ));
        }
        rows.push(format!(
            "(1, 1, unhex('00000000000000000000000000000000'), unhex('0000000000000000'), \
             unhex('0000000000000000'), 0, 0, {}, '', 0, '{service}', '{attrs}', \
             toDate(fromUnixTimestamp64Nano({t0})), '', '', '', '', '')",
            i + 1,
        ));
    }
    // Issue #496: a catalog row carries the latest span start in its
    // `start_ns`; at `0` the catalog's TTL drops it at once.
    rows.push(format!(
        "(1, 2, unhex('00000000000000000000000000000000'), unhex('0000000000000000'), \
         unhex('0000000000000000'), {t0}, 0, 0, '', 0, '', '{{}}', toDate(0), 'span', \
         'http.route', '', '', '')"
    ));
    // Issue #496: a catalog row carries the latest span start in its
    // `start_ns`; at `0` the catalog's TTL drops it at once.
    rows.push(format!(
        "(1, 3, unhex('00000000000000000000000000000000'), unhex('0000000000000000'), \
         unhex('0000000000000000'), {t0}, 0, 0, '', 0, '', '{{}}', toDate(0), 'span', \
         'http.route', '/api', 'string', '')"
    ));
    format!(
        "INSERT INTO {db}.trace_landing (received_ms, row_kind, trace_id, span_id, \
         parent_span_id, start_ns, duration_ns, resource_id, name, kind, service, attrs, day, \
         tag_scope, tag_key, tag_value, tag_type, service_type) VALUES {}",
        rows.join(", ")
    )
}

/// Issues one landing insert carrying the settings the writer pins.
async fn push_landing(
    client: &ChClient,
    db: &str,
    traces: &[(&str, &str)],
    spans: usize,
    attrs: &str,
    token: &str,
) -> Result<(), pulsus_clickhouse::ChError> {
    client
        .execute(
            &trace_landing_insert_sql(db, traces, spans, attrs),
            &QuerySettings::trace_landing_insert(token, TRACE_LANDING_MAX_ROWS),
            Idempotency::NonIdempotent,
        )
        .await
}

/// Waits until `table`'s send directory is empty on `client`, which is the
/// deterministic settle this fixture offers — never a sleep.
async fn wait_distribution_drained(client: &ChClient, db: &str, table: &str) {
    let sql = format!(
        "SELECT toUInt64(sum(data_files)) AS n FROM system.distribution_queue \
         WHERE database = '{db}' AND table = '{table}'"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if count_on(client, &sql).await == 0 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{db}.{table}'s send directory never drained"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// **A trace lands whole on one shard.** Without the routing both traces'
/// spans sit on whichever node took the push, and one shard holds 20 while
/// the other holds 0.
#[tokio::test]
async fn a_trace_lands_whole_on_one_shard() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    require_two_shard_topology(&shard1, &shard2).await;

    let db = &TEST_DB_TRACE_SHARDS;
    fresh_cluster_db(&shard1, db).await;
    let (a, b) = two_ids_on_two_shards(&shard1).await;
    push_landing(
        &shard1,
        db,
        &[(&a, "checkout"), (&b, "cart")],
        10,
        "{}",
        "cluster-whole-1",
    )
    .await
    .expect("the routed push commits");
    for table in ["spans_dist", "traces_dist"] {
        wait_distribution_drained(&shard1, db, table).await;
    }

    for (i, shard) in [&shard1, &shard2].into_iter().enumerate() {
        assert_eq!(
            count_on(shard, &format!("SELECT count() AS n FROM {db}.spans")).await,
            10,
            "shard{}: one whole trace's spans and no other's",
            i + 1
        );
        assert_eq!(
            count_on(
                shard,
                &format!("SELECT toUInt64(uniqExact(trace_id)) AS n FROM {db}.spans")
            )
            .await,
            1,
            "shard{}: exactly one trace id",
            i + 1
        );
        let span_trace = trace_ids(shard, db, "spans").await;
        let index_trace = trace_ids(shard, db, "traces").await;
        assert_eq!(
            span_trace,
            index_trace,
            "shard{}: the per-trace row is on the SAME shard as that trace's \
             spans",
            i + 1
        );
    }

    exec_on(
        &shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

/// **`distributed_foreground_insert` reaches the view's insert.** With the
/// remote shard's target renamed away — so every routed block for that shard
/// fails and no background sender can ever drain it — the push must **fail**
/// rather than queue.
///
/// A queued success is a failure of this case, not an alternative outcome it
/// records: a success followed by a read that misses the routed rows is the
/// exact behaviour it exists to exclude.
#[tokio::test]
async fn distributed_foreground_insert_reaches_the_views_insert() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    require_two_shard_topology(&shard1, &shard2).await;

    let db = &TEST_DB_TRACE_FOREGROUND;
    fresh_cluster_db(&shard1, db).await;
    let (a, b) = two_ids_on_two_shards(&shard1).await;
    let traces: Vec<(&str, &str)> = vec![(&a, "checkout"), (&b, "cart")];

    exec_on(
        &shard2,
        &format!("RENAME TABLE {db}.spans TO {db}.spans_parked"),
    )
    .await;
    let err = push_landing(&shard1, db, &traces, 10, "{}", "cluster-foreground-1")
        .await
        .expect_err("the push must fail: the pin makes the view's insert wait");
    eprintln!("foreground: the push failed with {err}");

    assert_eq!(
        count_on(
            &shard1,
            &format!(
                "SELECT toUInt64(sum(data_files)) AS n FROM system.distribution_queue \
                 WHERE database = '{db}' AND table = 'spans_dist'"
            )
        )
        .await,
        0,
        "nothing was queued: a foreground insert writes no send directory"
    );
    for (i, shard) in [&shard1, &shard2].into_iter().enumerate() {
        let table = if i == 0 { "spans" } else { "spans_parked" };
        let n = count_on(shard, &format!("SELECT count() AS n FROM {db}.{table}")).await;
        eprintln!(
            "foreground: shard{} {table} held {n} rows (recorded)",
            i + 1
        );
    }

    exec_on(
        &shard2,
        &format!("RENAME TABLE {db}.spans_parked TO {db}.spans"),
    )
    .await;
    push_landing(&shard1, db, &traces, 10, "{}", "cluster-foreground-2")
        .await
        .expect("with the target back, the same push succeeds");
    for table in ["spans_dist", "traces_dist"] {
        wait_distribution_drained(&shard1, db, table).await;
    }
    for (i, shard) in [&shard1, &shard2].into_iter().enumerate() {
        assert_eq!(
            count_on(shard, &format!("SELECT count() AS n FROM {db}.spans FINAL")).await,
            10,
            "shard{}: each shard holds its own trace",
            i + 1
        );
    }

    exec_on(
        &shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

/// **A `JSON` value and an aggregate column cross the routed hop intact.**
/// The column types survive the wrapper's `AS` copy — read back from
/// `system.columns` — and nothing else yet shows the values do.
#[tokio::test]
async fn a_json_and_an_aggregate_column_cross_the_hop() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    require_two_shard_topology(&shard1, &shard2).await;

    let db = &TEST_DB_TRACE_COLUMNS;
    fresh_cluster_db(&shard1, db).await;
    let (_a, b) = two_ids_on_two_shards(&shard1).await;
    let attrs = r#"{\"s\":\"v\",\"i\":1,\"f\":1.5,\"b\":true,\"arr\":[\"x\",\"y\"]}"#;
    push_landing(&shard1, db, &[(&b, "checkout")], 1, attrs, "cluster-json-1")
        .await
        .expect("the routed push commits");
    for table in ["spans_dist", "traces_dist"] {
        wait_distribution_drained(&shard1, db, table).await;
    }

    // Shard 2 is the one slot 1 selects, so the row crossed the hop.
    assert_eq!(
        count_on(&shard2, &format!("SELECT count() AS n FROM {db}.spans")).await,
        1,
        "the span crossed to the shard its trace id selects"
    );
    for (predicate, what) in [
        ("assumeNotNull(attrs.s.:String) = 'v'", "a string"),
        ("assumeNotNull(attrs.i.:Int64) = 1", "an integer"),
        ("assumeNotNull(attrs.f.:Float64) = 1.5", "a double"),
        ("assumeNotNull(attrs.b.:Bool) = true", "a bool"),
        (
            "has(attrs.arr.:`Array(Nullable(String))`, 'y')",
            "an array element",
        ),
    ] {
        assert_eq!(
            count_on(
                &shard2,
                &format!("SELECT count() AS n FROM {db}.spans WHERE {predicate}")
            )
            .await,
            1,
            "{what} must cross the hop intact: {predicate}"
        );
    }
    assert_eq!(
        count_on(
            &shard2,
            &format!(
                "SELECT count() AS n FROM {db}.traces WHERE has(services, 'checkout') \
                 AND start_ns = {}",
                trace_t0()
            )
        )
        .await,
        1,
        "and the per-trace row's aggregate columns carry the push's own values"
    );

    exec_on(
        &shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

/// **A shard that refuses loses whole traces, and a retry restores them.**
/// Without the routing a shard failure takes an arbitrary subset of each
/// trace's spans; a target that summed rather than collapsed would answer 20
/// after the retry.
#[tokio::test]
async fn a_shard_that_refuses_loses_whole_traces_and_a_retry_restores_them() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    require_two_shard_topology(&shard1, &shard2).await;

    let db = &TEST_DB_TRACE_REFUSAL;
    fresh_cluster_db(&shard1, db).await;
    let (a, b) = two_ids_on_two_shards(&shard1).await;
    let traces: Vec<(&str, &str)> = vec![(&a, "checkout"), (&b, "cart")];

    // Shard 2's span table replaced by one that refuses every insert.
    exec_on(
        &shard2,
        &format!("RENAME TABLE {db}.spans TO {db}.spans_held"),
    )
    .await;
    exec_on(
        &shard2,
        &format!(
            "CREATE TABLE {db}.spans AS {db}.spans_held ENGINE = MergeTree \
             ORDER BY (intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind) \
             PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns))"
        ),
    )
    .await;
    exec_on(
        &shard2,
        &format!("ALTER TABLE {db}.spans ADD CONSTRAINT refuse CHECK start_ns < 0"),
    )
    .await;

    let err = push_landing(&shard1, db, &traces, 10, "{}", "cluster-refuse-1")
        .await
        .expect_err("the refusing shard fails the routed insert");
    eprintln!("refusal: the push failed with {err}");
    assert_eq!(
        count_on(&shard1, &format!("SELECT count() AS n FROM {db}.spans")).await,
        10,
        "the surviving shard's own trace is COMPLETE: the routing makes the \
         missing unit a whole trace rather than an arbitrary subset of one"
    );
    assert_eq!(
        count_on(&shard2, &format!("SELECT count() AS n FROM {db}.spans")).await,
        0,
        "and the refused shard holds none of it"
    );

    // Restore the shard and re-send the identical rows under a new token,
    // which is what a client's retry outside the suppression window is.
    exec_on(&shard2, &format!("DROP TABLE {db}.spans")).await;
    exec_on(
        &shard2,
        &format!("RENAME TABLE {db}.spans_held TO {db}.spans"),
    )
    .await;
    push_landing(&shard1, db, &traces, 10, "{}", "cluster-refuse-2")
        .await
        .expect("the retry commits");
    for table in ["spans_dist", "traces_dist"] {
        wait_distribution_drained(&shard1, db, table).await;
    }
    for (i, shard) in [&shard1, &shard2].into_iter().enumerate() {
        // **`FINAL`, because the retry carries its own token**: the shard
        // that committed the first push holds those rows and the retry's
        // copy beside them until a merge, so the unmerged count there is 20.
        // That is the target's key doing its job, not a second copy of the
        // data: a target that summed rather than collapsed would answer 20
        // under `FINAL` too.
        assert_eq!(
            count_on(shard, &format!("SELECT count() AS n FROM {db}.spans FINAL")).await,
            10,
            "shard{}: after the retry each shard holds its own trace, once",
            i + 1
        );
        let unmerged = count_on(shard, &format!("SELECT count() AS n FROM {db}.spans")).await;
        eprintln!(
            "refusal: shard{} held {unmerged} unmerged rows (recorded)",
            i + 1
        );
        let rows = trace_ids(shard, db, "traces").await;
        assert_eq!(rows.len(), 1, "shard{}: one per-trace row", i + 1);
    }

    exec_on(
        &shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

/// **A hostile profile cannot move rows off their shard**, cannot turn a
/// view's exception into a success, and cannot make a dropped target a
/// silent absence. The push's own pinned settings win all three ways.
///
/// `async_insert` is not in this case: a profile cannot beat the client's own
/// pin, and the setting that can — the table's — is not reachable from a
/// profile at all.
#[tokio::test]
async fn a_hostile_profile_cannot_move_rows_off_their_shard() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    require_two_shard_topology(&shard1, &shard2).await;

    let db = &TEST_DB_TRACE_PROFILE;
    fresh_cluster_db(&shard1, db).await;
    let (a, b) = two_ids_on_two_shards(&shard1).await;
    let traces: Vec<(&str, &str)> = vec![(&a, "checkout"), (&b, "cart")];

    let user = format!("{db}_hostile");
    exec_on(
        &shard1,
        &format!("DROP USER IF EXISTS {user} ON CLUSTER '{CLUSTER_NAME}'"),
    )
    .await;
    exec_on(
        &shard1,
        &format!(
            "CREATE USER {user} ON CLUSTER '{CLUSTER_NAME}' IDENTIFIED WITH no_password \
             SETTINGS insert_shard_id = 1, materialized_views_ignore_errors = 1, \
             ignore_materialized_views_with_dropped_target_table = 1"
        ),
    )
    .await;
    // **Scoped to the test database**, which is all this user issues a
    // statement against: `GRANT ALL ON *.*` is refused outright, because the
    // granting user does not itself hold `SHOW NAMED COLLECTIONS SECRETS ON
    // *` with the grant option — measured in CI's two-shard job, `Code: 497
    // ... Not enough privileges`.
    exec_on(
        &shard1,
        &format!("GRANT ALL ON {db}.* TO {user} ON CLUSTER '{CLUSTER_NAME}'"),
    )
    .await;
    let hostile = ChClient::new(ChConnConfig {
        database: db.to_string(),
        user: user.clone(),
        password: String::new(),
        ..shard1_config()
    })
    .await
    .expect("connect as the hostile profile");

    // (a) Placement: each trace's spans are on the shard its id selects.
    push_landing(&hostile, db, &traces, 10, "{}", "cluster-profile-1")
        .await
        .expect("the push commits");
    for table in ["spans_dist", "traces_dist"] {
        wait_distribution_drained(&shard1, db, table).await;
    }
    for (i, shard) in [&shard1, &shard2].into_iter().enumerate() {
        assert_eq!(
            count_on(shard, &format!("SELECT count() AS n FROM {db}.spans")).await,
            10,
            "shard{}: `insert_shard_id` must not put both traces on one shard",
            i + 1
        );
    }

    // (b) A view pointed at an incompatible table still fails the insert.
    exec_on(&shard1, &format!("DROP VIEW {db}.traces_mv")).await;
    exec_on(
        &shard1,
        &format!(
            "CREATE TABLE {db}.traces_broken (day Date, trace_id Int8, start_ns Int64, \
             end_ns Int64, root_service String, root_name String, services Array(String)) \
             ENGINE = MergeTree ORDER BY trace_id"
        ),
    )
    .await;
    exec_on(
        &shard1,
        &format!(
            "CREATE MATERIALIZED VIEW {db}.traces_mv TO {db}.traces_broken AS \
             SELECT toDate(fromUnixTimestamp64Nano(s)) AS day, trace_id, s AS start_ns, \
                    e AS end_ns, rs AS root_service, rn AS root_name, sv AS services \
             FROM (SELECT trace_id, min(start_ns) AS s, max(start_ns + duration_ns) AS e, \
                          maxIf(service, parent_span_id = toFixedString('', 8)) AS rs, \
                          maxIf(name, parent_span_id = toFixedString('', 8)) AS rn, \
                          groupUniqArray(toString(service)) AS sv \
                   FROM {db}.trace_landing WHERE row_kind = 0 GROUP BY trace_id)"
        ),
    )
    .await;
    let err = push_landing(&hostile, db, &traces, 10, "{}", "cluster-profile-2")
        .await
        .expect_err("`materialized_views_ignore_errors = 1` must not win");
    eprintln!("hostile profile: the broken view failed the insert with {err}");

    // (c) A view whose target cannot be locked **fails the insert**, rather
    // than being skipped with that target's rows silently absent — which is
    // what `observePath` returns at
    // `ignore_materialized_views_with_dropped_target_table = 1`. The
    // failure is the assertion: a success here is the silent absence.
    //
    // **Not a claim about the landing table.** A view's exception fails the
    // insert, and the source rows are present in 297 of 300 measured trials
    // anyway: the fan-out is not a transaction, and this design discloses
    // that rather than preventing it. So the landing count is recorded.
    exec_on(
        &shard1,
        &format!("RENAME TABLE {db}.resources TO {db}.resources_parked"),
    )
    .await;
    let err = push_landing(&hostile, db, &traces, 10, "{}", "cluster-profile-3")
        .await
        .expect_err("`ignore_materialized_views_with_dropped_target_table = 1` must not win");
    eprintln!("hostile profile: the missing target failed the insert with {err}");
    let landed = count_on(
        &shard1,
        &format!("SELECT count() AS n FROM {db}.trace_landing"),
    )
    .await;
    eprintln!("hostile profile: trace_landing held {landed} rows (recorded)");
    exec_on(
        &shard1,
        &format!("RENAME TABLE {db}.resources_parked TO {db}.resources"),
    )
    .await;

    exec_on(
        &shard1,
        &format!("DROP USER IF EXISTS {user} ON CLUSTER '{CLUSTER_NAME}'"),
    )
    .await;
    exec_on(
        &shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

/// **The three catalog targets reach every shard**, and none of them has a
/// routing twin.
///
/// All three, not two: a version of this case that checked `resources` and
/// `tag_values` only would pass a per-shard `tag_names` and lose **name
/// discovery** on every other shard — the one catalog read the API
/// leaves bounded by the retention floor alone.
///
/// The `SYSTEM SYNC REPLICA` is the assertion's precondition, not a
/// convenience: the reach is eventual on the shipped consistency defaults,
/// so a case that read straight after the push would be a flake dressed as a
/// guarantee.
#[tokio::test]
async fn the_catalog_targets_reach_every_shard() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    require_two_shard_topology(&shard1, &shard2).await;

    let db = &TEST_DB_TRACE_CATALOGS;
    fresh_cluster_db(&shard1, db).await;
    let (a, b) = two_ids_on_two_shards(&shard1).await;
    push_landing(
        &shard1,
        db,
        &[(&a, "checkout"), (&b, "cart")],
        2,
        "{}",
        "cluster-catalogs-1",
    )
    .await
    .expect("the push commits");

    for table in ["resources", "tag_names", "tag_values"] {
        exec_on(&shard2, &format!("SYSTEM SYNC REPLICA {db}.{table}")).await;
    }
    for table in ["resources", "tag_names", "tag_values"] {
        let on_one = count_on(
            &shard1,
            &format!("SELECT count() AS n FROM {db}.{table} FINAL"),
        )
        .await;
        let on_two = count_on(
            &shard2,
            &format!("SELECT count() AS n FROM {db}.{table} FINAL"),
        )
        .await;
        assert!(on_one > 0, "{table} must hold the push's own rows");
        assert_eq!(
            on_one, on_two,
            "{table} is one cluster-wide replica set, so both connections \
             answer the same"
        );
    }
    for (i, shard) in [&shard1, &shard2].into_iter().enumerate() {
        let present = table_names(shard, db).await;
        for table in ["resources", "tag_names", "tag_values"] {
            let twin = format!("{table}_dist");
            assert!(
                !present.contains(&twin),
                "shard{}: {table} must have no routing twin: {present:?}",
                i + 1
            );
        }
    }

    exec_on(
        &shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

/// Issue #497: the binary takes its retention from the database only when
/// every replica of the cluster holds it with the same valid comment. Built
/// at seven days, then one replica's comment is changed or its database
/// dropped, each by a statement sent to that replica alone.
#[tokio::test]
async fn every_replica_must_state_the_same_retention() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    require_two_shard_topology(&shard1, &shard2).await;
    let db = &TEST_DB_RETENTION;
    fresh_cluster_db(&shard1, db).await;
    let read = || pulsus_schema::schema_retention(&shard1, Some(CLUSTER_NAME), db);
    use pulsus_schema::SchemaRetention::{Agreed, Refused};

    assert_eq!(
        read().await.expect("read"),
        Agreed(7),
        "built at seven days"
    );

    exec_on(
        &shard2,
        &format!("ALTER DATABASE {db} MODIFY COMMENT 'retention_days=8'"),
    )
    .await;
    match read().await.expect("read") {
        Refused { hosts, answered } => {
            assert_eq!(answered.len(), hosts, "every host answers");
            let eights = answered
                .iter()
                .filter(|(_, c)| c == "retention_days=8")
                .count();
            let sevens = answered
                .iter()
                .filter(|(_, c)| c == "retention_days=7")
                .count();
            assert_eq!((eights, sevens), (1, hosts - 1), "{answered:?}");
        }
        other => panic!("one replica at eight days must refuse: {other:?}"),
    }
    assert_eq!(
        pulsus_schema::schema_retention(&shard1, Some("no_such_cluster"), db)
            .await
            .expect("read"),
        Agreed(7),
        "a cluster the server does not define reads the connected host alone"
    );

    exec_on(&shard2, &format!("ALTER DATABASE {db} MODIFY COMMENT ''")).await;
    assert!(
        matches!(read().await.expect("read"), Refused { .. }),
        "one replica stating nothing refuses"
    );

    exec_on(
        &shard2,
        &format!("ALTER DATABASE {db} MODIFY COMMENT 'retention_days=7'"),
    )
    .await;
    assert_eq!(read().await.expect("read"), Agreed(7), "agreement restored");

    exec_on(&shard2, &format!("DROP DATABASE {db} SYNC")).await;
    match read().await.expect("read") {
        Refused { hosts, answered } => {
            assert_eq!(
                answered.len(),
                hosts - 1,
                "the replica without it does not answer"
            )
        }
        other => panic!("one replica without the database must refuse: {other:?}"),
    }

    drop_cluster_db(&shard1, db).await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct IntRow {
    v: i64,
}

async fn ints_on(client: &ChClient, sql: &str) -> Vec<i64> {
    let mut stream = client
        .query_stream::<IntRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("read failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode IntRow").v);
    }
    out
}

/// Issue #496: a late row does not move a catalog's instant back, on any
/// replica. Pushed to shard 1, newest first: a kind-2 `metric_landing` row
/// and kind-2 and kind-3 `trace_landing` rows of now, then in a second
/// insert the same keys an hour ago. After a final merge that every replica
/// waits for, each replica holding the table answers one row per key,
/// carrying now, read without `max()` or `FINAL`: shard 1's two replicas for
/// the per-shard metric tables, and those and shard 2 for the cluster-wide
/// tag tables.
#[tokio::test]
async fn a_late_row_keeps_the_newest_instant_on_every_replica() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard1b = ChClient::new(shard1b_config())
        .await
        .expect("connect shard1b");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    require_two_shard_topology(&shard1, &shard2).await;
    let db = &TEST_DB_LATE_ROW;
    fresh_cluster_db(&shard1, db).await;

    let now_ns = cluster_now_ns();
    let now_ms = now_ns / 1_000_000;
    let (late_ms, late_ns) = (now_ms - 3_600_000, now_ns - 3_600_000_000_000);
    for (ms, ns) in [(now_ms, now_ns), (late_ms, late_ns)] {
        exec_on(
            &shard1,
            &format!(
                "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels) \
                 VALUES ('', {now_ms}, 2, 'v', 9, {ms}, '{{\"job\":\"late\"}}')"
            ),
        )
        .await;
        exec_on(
            &shard1,
            &format!(
                "INSERT INTO {db}.trace_landing (received_ms, row_kind, start_ns, tag_scope, tag_key, tag_value, tag_type) \
                 VALUES ({now_ms}, 2, {ns}, 'span', 'late', '', ''), ({now_ms}, 3, {ns}, 'span', 'late', 'v', 'string')"
            ),
        )
        .await;
    }

    let tables: [(&str, &str, &str, bool, i64); 5] = [
        (
            "metric_labels",
            "last_seen",
            "metric_name = 'v'",
            false,
            now_ms,
        ),
        (
            "metric_label_index",
            "last_seen",
            "key = 'job' AND value = 'late'",
            false,
            now_ms,
        ),
        (
            "metric_label_values",
            "last_seen",
            "key = 'job' AND value = 'late'",
            false,
            now_ms,
        ),
        ("tag_names", "last_start_ns", "key = 'late'", true, now_ns),
        ("tag_values", "last_start_ns", "key = 'late'", true, now_ns),
    ];
    let mut got = Vec::new();
    let mut want = Vec::new();
    for (table, column, filter, everywhere, newest) in tables {
        let replicas: Vec<(&str, &ChClient)> = if everywhere {
            vec![
                ("shard1", &shard1),
                ("shard1b", &shard1b),
                ("shard2", &shard2),
            ]
        } else {
            vec![("shard1", &shard1), ("shard1b", &shard1b)]
        };
        for (_, r) in &replicas {
            exec_on(r, &format!("SYSTEM SYNC REPLICA {db}.{table}")).await;
        }
        exec_on(
            &shard1,
            &format!("OPTIMIZE TABLE {db}.{table} FINAL SETTINGS alter_sync = 2"),
        )
        .await;
        for (name, r) in &replicas {
            exec_on(r, &format!("SYSTEM SYNC REPLICA {db}.{table}")).await;
            let sql = format!("SELECT toInt64({column}) AS v FROM {db}.{table} WHERE {filter}");
            got.push((table, *name, ints_on(r, &sql).await));
            want.push((table, *name, vec![newest]));
        }
    }
    drop_cluster_db(&shard1, db).await;
    assert_eq!(
        got, want,
        "one row per key on every replica, carrying the newest instant"
    );
}

/// The rollup level views of part A's set, 60 s first, finest first
/// (issue #18); the default set begins at 5 s.
const LEVEL_VIEWS: [&str; 4] = [
    "metric_samples_60s_mv",
    "metric_samples_720s_mv",
    "metric_samples_7200s_mv",
    "metric_samples_86400s_mv",
];

/// **A10 (issue #18 part A).** Each shard builds its slice of the first
/// level once, on its first replica, from every replica's `metric_landing`.
/// One series' samples go alternately to shard 1's and shard 2's landing
/// table; shard 1's first replica, its builder, stops fetching landing, and
/// one more sample goes to shard 1's second replica, so only that replica
/// holds it. After one 60 s run on every replica, the level read through its
/// wrapper has one data row per bucket, counting every sample pushed, that
/// one included; and each shard holds one run row from that run, so the
/// second replica wrote none.
#[tokio::test]
async fn each_shard_builds_its_slice_once() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard1b = ChClient::new(shard1b_config())
        .await
        .expect("connect shard1b");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    require_two_shard_topology(&shard1, &shard2).await;
    let db = &TEST_DB_LEVELS;
    drop_cluster_db(&shard1, db).await;
    let ctx = RenderCtx {
        rollup_levels: vec![60, 720, 7200, 86400],
        rollup_ranges: vec![86400, 604800, 7776000],
        ..cluster_ctx(db)
    };
    run_init(&shard1, &ctx).await.expect("run_init (clustered)");
    let replicas = [&shard1, &shard1b, &shard2];
    // Each view ran once at creation; wait for it, stop every view, and
    // empty the levels, so the runs below are the test's own.
    for r in replicas {
        for view in LEVEL_VIEWS {
            exec_on(r, &format!("SYSTEM WAIT VIEW {db}.{view}")).await;
            exec_on(r, &format!("SYSTEM STOP VIEW {db}.{view}")).await;
        }
    }
    for r in [&shard1, &shard2] {
        for view in LEVEL_VIEWS {
            let table = view.trim_end_matches("_mv");
            exec_on(
                r,
                &format!("TRUNCATE TABLE {db}.{table} SETTINGS alter_sync = 2"),
            )
            .await;
        }
    }

    const MINUTE: i64 = 60_000;
    let now = ints_on(
        &shard1,
        "SELECT toInt64(toUnixTimestamp64Milli(now64())) AS v",
    )
    .await[0];
    // Five whole minutes ending at least five minutes ago: inside the
    // run's window whatever minute it starts in.
    let seed = now / MINUTE * MINUTE - 10 * MINUTE;
    let fp: u128 = 1;
    for r in [&shard1, &shard2] {
        exec_on(
            r,
            &format!(
                "INSERT INTO {db}.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, built_at) \
                 VALUES (1, '', 0, {seed}, {}, 0)",
                seed - MINUTE
            ),
        )
        .await;
    }
    let landing = |t: i64, v: f64| {
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             VALUES ('', {}, 0, '', {fp}, {t}, {v:?})",
            t + 5_000
        )
    };
    let mut pushed: std::collections::BTreeMap<i64, i64> = Default::default();
    for k in 1..=20i64 {
        let t = seed + k * 15_000;
        let to = if k % 2 == 0 { &shard1 } else { &shard2 };
        exec_on(to, &landing(t, k as f64)).await;
        *pushed
            .entry((t - 1) / MINUTE * MINUTE + MINUTE)
            .or_default() += 1;
    }
    exec_on(
        &shard1b,
        &format!("SYSTEM SYNC REPLICA {db}.metric_landing"),
    )
    .await;
    exec_on(&shard1, &format!("SYSTEM STOP FETCHES {db}.metric_landing")).await;
    let late = seed + 5 * MINUTE - 7_000;
    exec_on(&shard1b, &landing(late, 99.0)).await;
    *pushed.entry(seed + 5 * MINUTE).or_default() += 1;
    let builder_copy = ints_on(
        &shard1,
        &format!("SELECT toInt64(count()) AS v FROM {db}.metric_landing WHERE unix_milli = {late}"),
    )
    .await[0];
    assert_eq!(
        builder_copy, 0,
        "the builder's own copy lacks the last push"
    );

    let view = LEVEL_VIEWS[0];
    for r in replicas {
        exec_on(r, &format!("SYSTEM START VIEW {db}.{view}")).await;
        exec_on(r, &format!("SYSTEM REFRESH VIEW {db}.{view}")).await;
        exec_on(r, &format!("SYSTEM WAIT VIEW {db}.{view}")).await;
        exec_on(r, &format!("SYSTEM STOP VIEW {db}.{view}")).await;
    }
    exec_on(
        &shard1b,
        &format!("SYSTEM SYNC REPLICA {db}.metric_samples_60s"),
    )
    .await;
    exec_on(
        &shard1,
        &format!("SYSTEM SYNC REPLICA {db}.metric_samples_60s"),
    )
    .await;

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
    struct BucketRow {
        t_end: i64,
        count: u32,
    }
    let mut stream = shard1
        .query_stream::<BucketRow>(
            &format!(
                "SELECT t_end, count FROM {db}.metric_samples_60s_dist \
                 WHERE kind = 0 AND fingerprint = {fp} ORDER BY t_end, count"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("read the level through its wrapper");
    let mut got = Vec::new();
    while let Some(row) = stream.next().await {
        got.push(row.expect("decode BucketRow"));
    }
    drop(stream);
    let want: Vec<BucketRow> = pushed
        .iter()
        .map(|(t_end, n)| BucketRow {
            t_end: *t_end,
            count: u32::try_from(*n).expect("small"),
        })
        .collect();

    let mut runs = Vec::new();
    for (name, r) in [
        ("shard 1", &shard1),
        ("shard 1's second replica", &shard1b),
        ("shard 2", &shard2),
    ] {
        let n = ints_on(
            r,
            &format!(
                "SELECT toInt64(count()) AS v FROM {db}.metric_samples_60s \
                 WHERE kind = 1 AND t_end > {seed}"
            ),
        )
        .await[0];
        runs.push((name, n));
    }
    exec_on(
        &shard1,
        &format!("SYSTEM START FETCHES {db}.metric_landing"),
    )
    .await;
    drop_cluster_db(&shard1, db).await;

    assert_eq!(
        got, want,
        "one data row per bucket, counting every sample pushed once"
    );
    assert_eq!(
        runs,
        vec![
            ("shard 1", 1),
            ("shard 1's second replica", 1),
            ("shard 2", 1)
        ],
        "one run row per shard, from its first replica: a second replica's run would add one"
    );
}

// Issue #743 part A: the pre-check's cluster form, and each replica's own
// copy on the object store.
static TEST_DB_TIER_REFUSE: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_tier_refuse");
static TEST_DB_TIER_MOVE: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_tier_move");

/// One column, every row, as text, on `client`.
async fn strings_on(client: &ChClient, sql: &str) -> Vec<String> {
    client
        .query_strings(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("read failed: {e}\nSQL:\n{sql}"))
}

/// The one value of a one-row, one-column answer on `client`.
async fn one_on(client: &ChClient, sql: &str) -> String {
    let rows = strings_on(client, sql).await;
    assert_eq!(rows.len(), 1, "one row: {sql}");
    rows.into_iter().next().expect("one row")
}

/// **A6b (issue #743 part A): the script refuses a policy one host lacks
/// before it drops anything, and builds on a policy every host carries.**
/// `storage_partial.xml` gives `pulsus_test_partial` to shard 1's two
/// servers only.
#[tokio::test]
async fn the_script_refuses_a_policy_missing_on_one_host_before_it_drops() {
    skip_unless_live!();
    let servers = [
        ("shard 1", shard1_config()),
        ("shard 1b", shard1b_config()),
        ("shard 2", shard2_config()),
    ];
    let mut clients = Vec::new();
    for (name, cfg) in &servers {
        clients.push((*name, ChClient::new(cfg.clone()).await.expect("connect")));
    }
    let shard1 = &clients[0].1;
    let d = TEST_DB_TIER_REFUSE.name();
    drop_cluster_db(shard1, d).await;
    exec_on(
        shard1,
        &format!("CREATE DATABASE {d} ON CLUSTER '{CLUSTER_NAME}'"),
    )
    .await;
    exec_on(
        shard1,
        &format!(
            "CREATE TABLE {d}.keep ON CLUSTER '{CLUSTER_NAME}' (x UInt8) \
             ENGINE = MergeTree ORDER BY x"
        ),
    )
    .await;
    let keep = format!(
        "SELECT toString(count()) AS s FROM system.tables WHERE database = '{d}' AND name = 'keep'"
    );

    let cfg = &servers[0].1;
    let script = |policy: &str| {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut cmd = std::process::Command::new("sh");
        cmd.arg(root.join("schema/schema.sh")).current_dir(&root);
        for (var, _) in std::env::vars() {
            if var.starts_with("PULSUS_") {
                cmd.env_remove(var);
            }
        }
        cmd.env("CLICKHOUSE_SERVER", &cfg.server)
            .env("CLICKHOUSE_HTTP_PORT", cfg.http_port.to_string())
            .env("PULSUS_SCHEMA_WAIT_SECONDS", "30")
            .env("PULSUS_CLUSTER", CLUSTER_NAME)
            .env("CLICKHOUSE_DB", d)
            .env("PULSUS_RETENTION_DAYS", "7")
            .env("PULSUS_STORAGE_POLICY", policy)
            .env("PULSUS_COLD_VOLUME", "cold")
            .env("PULSUS_METRICS_HOT_DAYS", "2")
            .env("PULSUS_LOGS_HOT_DAYS", "2")
            .env("PULSUS_TRACES_HOT_DAYS", "2");
        let out = cmd.output().expect("run schema.sh");
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };

    let (code, err) = script("pulsus_test_partial");
    assert_eq!(code, Some(1), "(1) a policy on 2 of 3 hosts: {err}");
    assert_eq!(
        err.trim_end().lines().last(),
        Some(
            "schema.sh: storage policy 'pulsus_test_partial' with a volume 'cold' after its \
             first and move_factor 0 is defined on 2 of 3 hosts; every server's configuration \
             must carry it (docs/configuration.md §3)"
        ),
        "(1) the message"
    );
    for (name, client) in &clients {
        assert_eq!(
            one_on(client, &keep).await,
            "1",
            "(1) {name}: nothing is dropped"
        );
    }

    let (code, err) = script("pulsus_test_tiered");
    assert_eq!(code, Some(0), "(2) a policy on every host: {err}");
    for (name, client) in &clients {
        assert_eq!(one_on(client, &keep).await, "0", "(2) {name}: rebuilt");
        let q = create_table_query(client, d, "metric_samples").await;
        assert!(q.contains("TO VOLUME 'cold'"), "(2) {name}: {q}");
    }

    drop_cluster_db(shard1, d).await;
}

/// **A7 (issue #743 part A): each replica moves its own copy.** A day past
/// the move age, written through the routing table, is on the object store
/// on every server, and each server wrote objects under its own
/// `{shard}/{replica}` prefix; shard 1's two replicas wrote as many.
#[tokio::test]
async fn each_replica_moves_its_own_copy() {
    skip_unless_live!();
    let servers = [
        ("shard 1", shard1_config()),
        ("shard 1b", shard1b_config()),
        ("shard 2", shard2_config()),
    ];
    let mut clients = Vec::new();
    for (name, cfg) in &servers {
        clients.push((*name, ChClient::new(cfg.clone()).await.expect("connect")));
    }
    let shard1 = &clients[0].1;
    let db = TEST_DB_TIER_MOVE.name();
    drop_cluster_db(shard1, db).await;
    let ctx = RenderCtx {
        storage_policy: Some("pulsus_test_tiered".to_string()),
        cold_volume: Some("cold".to_string()),
        metrics_hot_days: Some(2),
        logs_hot_days: Some(7),
        traces_hot_days: Some(7),
        ..cluster_ctx(db)
    };
    run_init(shard1, &ctx).await.expect("run_init (clustered)");

    // The objects under each server's own prefix.
    let mut prefixes = Vec::new();
    for (_, client) in &clients {
        let shard = one_on(
            client,
            "SELECT substitution AS s FROM system.macros WHERE macro = 'shard'",
        )
        .await;
        let replica = one_on(
            client,
            "SELECT substitution AS s FROM system.macros WHERE macro = 'replica'",
        )
        .await;
        prefixes.push(format!(
            "SELECT toString(count()) AS s FROM s3('http://172.28.0.30:9000/pulsus-ci/ci/{shard}/{replica}/**', \
             'pulsus-ci', 'pulsus-ci-secret', 'One')"
        ));
    }
    let mut before = Vec::new();
    for ((_, client), objects) in clients.iter().zip(&prefixes) {
        let n: u64 = one_on(client, objects).await.parse().expect("a count");
        before.push(n);
    }

    let now_ms: i64 = one_on(
        shard1,
        "SELECT toString(toUnixTimestamp64Milli(now64())) AS s",
    )
    .await
    .parse()
    .expect("milliseconds");
    let ts = now_ms - 302_400_000; // 3.5 days
    exec_on(
        shard1,
        &format!(
            "INSERT INTO {db}.metric_samples_dist (org_id, fingerprint, unix_milli, value) \
             SELECT '', number + 1, toInt64({ts}), 1 FROM numbers(100)"
        ),
    )
    .await;
    exec_on(
        shard1,
        &format!("SYSTEM FLUSH DISTRIBUTED {db}.metric_samples_dist"),
    )
    .await;
    for (_, client) in &clients {
        exec_on(client, &format!("SYSTEM SYNC REPLICA {db}.metric_samples")).await;
    }

    let partition = one_on(
        shard1,
        &format!("SELECT toString(toDate(fromUnixTimestamp64Milli(toInt64({ts})), 'UTC')) AS s"),
    )
    .await;
    let mut counts = Vec::new();
    let mut gained = Vec::new();
    for (((name, client), objects), before) in clients.iter().zip(&prefixes).zip(&before) {
        let disks = strings_on(
            client,
            &format!(
                "SELECT disk_name AS s FROM system.parts WHERE database = '{db}' \
                 AND table = 'metric_samples' AND active AND partition = '{partition}'"
            ),
        )
        .await;
        assert!(!disks.is_empty(), "{name}: the day has a part");
        assert!(
            disks.iter().all(|d| d == "cold_store"),
            "{name}: every part of the day is on the object store: {disks:?}"
        );
        let n: u64 = one_on(
            client,
            &format!("SELECT toString(count()) AS s FROM {db}.metric_samples"),
        )
        .await
        .parse()
        .expect("a count");
        counts.push(n);
        let after: u64 = one_on(client, objects).await.parse().expect("a count");
        assert!(
            after > *before,
            "{name}: objects written under its own prefix: {before} then {after}"
        );
        gained.push(after - before);
    }
    assert_eq!(
        counts[0], counts[1],
        "shard 1's two replicas hold the same rows"
    );
    assert_eq!(
        counts[0] + counts[2],
        100,
        "the two shards hold every row once"
    );
    assert_eq!(
        gained[0], gained[1],
        "shard 1's two replicas each wrote their own copy"
    );

    drop_cluster_db(shard1, db).await;
}
