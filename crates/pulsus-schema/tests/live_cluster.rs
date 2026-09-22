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
static TEST_DB_DIST: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_dist");
static TEST_DB_BOOKKEEPING: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_bookkeeping");
static TEST_DB_SPAN_ARRAYS: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_span_arrays");
// Issue #560: the two derived trace tables, clustered.
static TEST_DB_DERIVED: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_derived");
static TEST_DB_DERIVED_REPLAY: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_derived_replay");
static TEST_DB_DEDUP_PROFILE: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_cluster_dedup_profile");

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

    let ctx = cluster_ctx(&TEST_DB_DIST);
    run_init(&shard1, &ctx).await.expect("run_init (clustered)");

    // The DDL must be visible on BOTH shards — proof that `ON CLUSTER`
    // actually distributed it, not just created objects locally on shard1.
    let names1 = table_names(&shard1, &TEST_DB_DIST).await;
    let names2 = table_names(&shard2, &TEST_DB_DIST).await;
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
        let ddl1 = create_table_query(&shard1, &TEST_DB_DIST, table).await;
        let ddl2 = create_table_query(&shard2, &TEST_DB_DIST, table).await;
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
            v.push(create_table_query(&shard1, &TEST_DB_DIST, t).await);
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
        let ddl = create_table_query(&shard1, &TEST_DB_DIST, t).await;
        assert!(ddl.contains(Family::Logs.sharding_expr()));
    }

    let traces_dist = [
        "trace_spans_dist",
        "trace_attrs_idx_dist",
        "trace_edges_dist",
    ];
    for t in traces_dist {
        let ddl = create_table_query(&shard1, &TEST_DB_DIST, t).await;
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

    // Issue #476, and this is the ONLY place it can be checked. Migration
    // 41's `ADD COLUMN … , MODIFY ORDER BY …` runs `ON CLUSTER` against
    // the `Replicated*` engine, which a single local ClickHouse cannot
    // even create (it refuses without ZooKeeper), so nothing about the
    // clustered form of that ALTER is exercised anywhere else. Asserted on
    // BOTH shards, and as two separate string comparisons per shard: the
    // prune that serves every tag-values read depends on the primary key
    // staying `scope, key, val`, and the two-types-per-value answer
    // depends on `val_type` reaching the sorting key.
    //
    // Migration 40's `_dist` twin of the attribute index is covered by the
    // same reasoning one table over: it is asserted here because the
    // wrapper is created from a `CREATE … AS` that does not inherit the
    // base table's ALTERs.
    for (shard, label) in [(&shard1, "shard1"), (&shard2, "shard2")] {
        let keys = table_keys(shard, &TEST_DB_DIST, "trace_tag_catalog").await;
        assert_eq!(
            keys.primary_key, "scope, key, val",
            "{label}: the clustered ALTER must leave the primary key alone"
        );
        assert_eq!(
            keys.sorting_key, "scope, key, val, val_type",
            "{label}: the clustered ALTER must append val_type to the sorting key"
        );
        for table in ["trace_attrs_idx", "trace_attrs_idx_dist"] {
            let ddl = create_table_query(shard, &TEST_DB_DIST, table).await;
            assert!(
                ddl.contains("val_type"),
                "{label}: {table} must carry val_type after migrations 39/40: {ddl}"
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
    /// Issue #476: the stored OTLP type. Read here as well as written so
    /// the cross-shard identity check covers the new column rather than
    /// agreeing on the three it already had.
    val_type: String,
}

/// `system.tables`'s two key columns for one table on one shard (issue
/// #476).
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TableKeysRow {
    primary_key: String,
    sorting_key: String,
}

async fn table_keys(client: &ChClient, db: &str, table: &str) -> TableKeysRow {
    let mut stream = client
        .query_stream::<TableKeysRow>(
            &format!(
                "SELECT primary_key, sorting_key FROM system.tables \
                 WHERE database = '{db}' AND name = '{table}'"
            ),
            &QuerySettings::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("query system.tables for {db}.{table}: {e}"));
    let row = stream
        .next()
        .await
        .unwrap_or_else(|| panic!("{db}.{table} must exist"))
        .expect("decode system.tables row");
    drop(stream);
    row
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

    let ctx = cluster_ctx(&TEST_DB_BOOKKEEPING);
    run_init(&shard1, &ctx).await.expect("run_init (clustered)");

    let (migrations1, migrations2) = poll_until_matching(|| async {
        (
            bookkeeping_rows::<MigrationRow>(
                &shard1,
                &TEST_DB_BOOKKEEPING,
                "schema_migrations",
                "id",
            )
            .await,
            bookkeeping_rows::<MigrationRow>(
                &shard2,
                &TEST_DB_BOOKKEEPING,
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
                &TEST_DB_BOOKKEEPING,
                "mv_checksums",
                "mv_name",
            )
            .await,
            bookkeeping_rows::<MvChecksumRow>(
                &shard2,
                &TEST_DB_BOOKKEEPING,
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
        &TEST_DB_BOOKKEEPING,
        "metric_metadata",
        "metric_name",
    )
    .await;
    let metadata2 = bookkeeping_rows::<MetricMetadataRow>(
        &shard2,
        &TEST_DB_BOOKKEEPING,
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
                "INSERT INTO {TEST_DB_BOOKKEEPING}.trace_tag_catalog \
                     (scope, key, val, val_type) \
                 VALUES ('span', 'http.status_code', '500', 'int')"
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
                &TEST_DB_BOOKKEEPING,
                "trace_tag_catalog",
                "scope, key, val, val_type",
            )
            .await,
            bookkeeping_rows::<TraceTagRow>(
                &shard2,
                &TEST_DB_BOOKKEEPING,
                "trace_tag_catalog",
                "scope, key, val, val_type",
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

/// The five span-attribute array columns and `attr_num` re-read as BITS.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct SpanArrayRow {
    attr_key: Vec<String>,
    attr_scope: Vec<String>,
    attr_val: Vec<String>,
    attr_type: Vec<String>,
    attr_num_bits: Vec<Option<u64>>,
}

/// Issue #556 criterion 3, and this is the ONLY place it can be checked.
/// Migrations 49-58's `_dist` twins run `ON CLUSTER` against the wrapper,
/// which a single local ClickHouse never creates.
///
/// A `_dist` wrapper is created from a `CREATE ... AS` that does NOT
/// inherit the base table's `ALTER`s, so without the five cluster-only
/// twins the wrapper's attr columns are `[]` and an insert through it is
/// `Code: 16 NO_SUCH_COLUMN_IN_TABLE`.
///
/// The read back is **polled**, 40 times at 250 ms, exactly as the
/// `log_samples_dist` read-back above polls the same asynchronous
/// forwarding. Not a precaution: a `Distributed` insert forwards
/// asynchronously (`distributed_foreground_insert = 0`), so the read that
/// follows it can legitimately see nothing.
#[tokio::test]
async fn span_attribute_arrays_round_trip_through_the_dist_wrapper() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");

    shard1
        .execute(
            &format!(
                "DROP DATABASE IF EXISTS {TEST_DB_SPAN_ARRAYS} ON CLUSTER '{CLUSTER_NAME}' SYNC"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database on cluster");
    run_init(&shard1, &cluster_ctx(&TEST_DB_SPAN_ARRAYS))
        .await
        .expect("run_init (clustered)");

    // The wrapper carries all five columns on BOTH shards.
    let expected = vec![
        "attr_key".to_string(),
        "attr_num".to_string(),
        "attr_scope".to_string(),
        "attr_type".to_string(),
        "attr_val".to_string(),
    ];
    for (shard, label) in [(&shard1, "shard1"), (&shard2, "shard2")] {
        for table in ["trace_spans", "trace_spans_dist"] {
            let sql = format!(
                "SELECT name FROM system.columns WHERE database = '{TEST_DB_SPAN_ARRAYS}' \
                 AND table = '{table}' AND name LIKE 'attr\\\\_%' ORDER BY name"
            );
            let mut stream = shard
                .query_stream::<NameRow>(&sql, &QuerySettings::new())
                .await
                .expect("query system.columns");
            let mut got = Vec::new();
            while let Some(row) = stream.next().await {
                got.push(row.expect("decode NameRow").name);
            }
            assert_eq!(
                got, expected,
                "{label}: {table} must carry the five array columns after migrations 49-58"
            );
        }
    }

    // The constraint lives on the BASE table only: a Distributed table
    // refuses `ADD_CONSTRAINT`, and does not need one.
    let base_ddl = create_table_query(&shard1, &TEST_DB_SPAN_ARRAYS, "trace_spans").await;
    let dist_ddl = create_table_query(&shard1, &TEST_DB_SPAN_ARRAYS, "trace_spans_dist").await;
    assert!(
        base_ddl.contains("attr_arrays_aligned"),
        "the base table must carry the constraint: {base_ddl}"
    );
    assert!(
        !dist_ddl.contains("CONSTRAINT"),
        "the wrapper must carry no constraint: {dist_ddl}"
    );

    let now_ns = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64");
    let trace_hex = "0102030405060708090a0b0c0d0e0f10";
    let insert = |arrays: &str| {
        format!(
            "INSERT INTO {TEST_DB_SPAN_ARRAYS}.trace_spans_dist \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload, \
              attr_key, attr_scope, attr_val, attr_type, attr_num) \
             VALUES (unhex('{trace_hex}'), unhex('0102030405060708'), \
             unhex('0000000000000000'), 'n', 's', {now_ns}, 1, 0, 2, 1, '', {arrays})"
        )
    };

    shard1
        .execute(
            &insert("['a','b'], ['span','link:intrinsic'], ['1','2'], ['int','string'], [1, NULL]"),
            &QuerySettings::new(),
            Idempotency::NonIdempotent,
        )
        .await
        .expect("an aligned row must insert through the wrapper");

    // Read back through the wrapper from the OTHER shard, polled.
    let sql = format!(
        "SELECT attr_key, attr_scope, attr_val, attr_type, \
         arrayMap(x -> if(isNull(x), NULL, reinterpretAsUInt64(assumeNotNull(x))), attr_num) \
         AS attr_num_bits \
         FROM {TEST_DB_SPAN_ARRAYS}.trace_spans_dist WHERE trace_id = unhex('{trace_hex}')"
    );
    let mut got = None;
    for _ in 0..40 {
        let mut stream = shard2
            .query_stream::<SpanArrayRow>(&sql, &QuerySettings::new())
            .await
            .expect("select the arrays via _dist from the other shard");
        if let Some(row) = stream.next().await {
            got = Some(row.expect("decode"));
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(
        got,
        Some(SpanArrayRow {
            attr_key: vec!["a".to_string(), "b".to_string()],
            attr_scope: vec!["span".to_string(), "link:intrinsic".to_string()],
            attr_val: vec!["1".to_string(), "2".to_string()],
            attr_type: vec!["int".to_string(), "string".to_string()],
            // The NULL stays at its OWN position; nothing is dropped.
            attr_num_bits: vec![Some(1.0f64.to_bits()), None],
        }),
        "the arrays inserted via _dist must read back cluster-wide through _dist"
    );

    // A misaligned row through the wrapper is refused by the BASE table's
    // constraint, and the refusal reaches the client. Asserted on the code
    // and the constraint name only — the server appends the table's UUID
    // and, on some insert paths, a wait-phase clause.
    //
    // `distributed_foreground_insert = 1` is load-bearing and is not a
    // convenience. A `Distributed` insert forwards asynchronously by
    // default, so whether the refusal reaches the client depends on WHICH
    // shard the row is destined for, measured on this fixture at
    // ClickHouse 26.3.29.7 with one misaligned row per sharding slot:
    //
    //     cityHash64(trace_id) % 2   destination    result at the client
    //     0                          shard 1, local  HTTP 500, Code: 469
    //     1                          shard 2, remote HTTP 200; the block is
    //                                                queued and shard 2
    //                                                refuses it later
    //
    // This row's `trace_id` is in the second class — deliberately, because
    // the read-back above must cross a shard boundary to be worth making.
    // Forwarding it in the foreground makes the base table's constraint run
    // before the client is answered, so the assertion is about the
    // constraint rather than about where the row happened to land.
    let err = shard1
        .execute(
            &insert("['a','b'], ['span','span'], ['1','2'], ['int','int'], [1]"),
            &QuerySettings::new().set("distributed_foreground_insert", 1),
            Idempotency::NonIdempotent,
        )
        .await
        .expect_err("a misaligned row must be refused through the wrapper");
    match err {
        pulsus_clickhouse::ChError::Server { code, ref message } => {
            assert_eq!(code, 469, "VIOLATED_CONSTRAINT; got: {err}");
            assert!(
                message.contains("attr_arrays_aligned"),
                "the refusal must name the constraint; got: {message}"
            );
        }
        other => panic!("expected a server error, got: {other}"),
    }

    // And nothing of it landed: the aligned row is still the only one.
    let sql = format!(
        "SELECT name FROM {TEST_DB_SPAN_ARRAYS}.trace_spans_dist \
         WHERE trace_id = unhex('{trace_hex}')"
    );
    let mut stream = shard1
        .query_stream::<NameRow>(&sql, &QuerySettings::new())
        .await
        .expect("count the rows under this trace id");
    let mut rows = 0usize;
    while let Some(row) = stream.next().await {
        row.expect("decode");
        rows += 1;
    }
    assert_eq!(rows, 1, "the refused row must not be stored");
}

// ---------------------------------------------------------------------
// Issue #560 — `trace_recent` and `trace_error_spans`, clustered
// ---------------------------------------------------------------------

/// The two trace ids issue #560's cluster tests write: measured,
/// `cityHash64(toFixedString(unhex(id), 16)) % 2` is `0` for the first
/// and `1` for the second, so the family's sharding key puts them on
/// different shards.
const DERIVED_TRACES: [&str; 2] = [
    "00000000000000000000000000000001",
    "00000000000000000000000000000003",
];

async fn exec_on(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

async fn fresh_cluster_db(shard1: &ChClient, db: &str) {
    exec_on(
        shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    run_init(shard1, &cluster_ctx(db))
        .await
        .expect("run_init (clustered)");
}

/// Every one of `tables` exists in `db` on both shards; the missing ones
/// are listed together.
async fn assert_clustered_tables(shard1: &ChClient, shard2: &ChClient, db: &str, tables: &[&str]) {
    let mut missing = Vec::new();
    for (shard, label) in [(shard1, "shard1"), (shard2, "shard2")] {
        let names = table_names(shard, db).await;
        for t in tables {
            if !names.iter().any(|n| n == t) {
                missing.push(format!("{label}:{t}"));
            }
        }
    }
    assert!(missing.is_empty(), "missing clustered tables: {missing:?}");
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

/// The `INSERT` that writes one error span on each of the two traces
/// through the span table's wrapper.
fn derived_insert(db: &str, ts_ns: i64) -> String {
    let values: Vec<String> = DERIVED_TRACES
        .iter()
        .enumerate()
        .map(|(i, id)| {
            format!(
                "(unhex('{id}'), unhex('000000000000000{}'), unhex('0000000000000000'), 'op', \
                 'svc', {}, 1000, 2, 2, 1, '')",
                i + 1,
                ts_ns + i as i64
            )
        })
        .collect();
    format!(
        "INSERT INTO {db}.trace_spans_dist (trace_id, span_id, parent_id, name, service, \
         timestamp_ns, duration_ns, status_code, kind, payload_type, payload) VALUES {}",
        values.join(", ")
    )
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

/// Polls, 40 times at 250 ms, until every shard's local `trace_spans`
/// holds at least one row (a `Distributed` insert forwards
/// asynchronously; the flush before this makes it synchronous, and the
/// poll covers the remainder).
async fn wait_for_spans(shard1: &ChClient, shard2: &ChClient, db: &str) {
    for _ in 0..40 {
        let a = count_on(
            shard1,
            &format!("SELECT count() AS n FROM {db}.trace_spans"),
        )
        .await;
        let b = count_on(
            shard2,
            &format!("SELECT count() AS n FROM {db}.trace_spans"),
        )
        .await;
        if a > 0 && b > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Issue #560 criterion 13: both wrappers exist, a clustered write lands
/// each trace's derived rows on the shard that holds its spans, and each
/// wrapper reads the union.
#[tokio::test]
async fn the_derived_trace_tables_are_written_per_shard_and_read_through_their_wrappers() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    let db: &str = &TEST_DB_DERIVED;
    fresh_cluster_db(&shard1, db).await;
    assert_clustered_tables(
        &shard1,
        &shard2,
        db,
        &[
            "trace_recent",
            "trace_recent_dist",
            "trace_error_spans",
            "trace_error_spans_dist",
        ],
    )
    .await;

    exec_on(&shard1, &derived_insert(db, cluster_now_ns())).await;
    exec_on(
        &shard1,
        &format!("SYSTEM FLUSH DISTRIBUTED {db}.trace_spans_dist"),
    )
    .await;
    wait_for_spans(&shard1, &shard2, db).await;

    let mut failures: Vec<String> = Vec::new();
    let mut shard_sets: Vec<Vec<String>> = Vec::new();
    for (shard, label) in [(&shard1, "shard1"), (&shard2, "shard2")] {
        let spans = trace_ids(shard, db, "trace_spans").await;
        for table in ["trace_recent", "trace_error_spans"] {
            let got = trace_ids(shard, db, table).await;
            if got != spans {
                failures.push(format!(
                    "{label}: local {table} holds {got:?}, local trace_spans {spans:?}"
                ));
            }
        }
        shard_sets.push(spans);
    }
    let mut sorted = shard_sets.clone();
    sorted.sort();
    let want: Vec<Vec<String>> = DERIVED_TRACES
        .iter()
        .map(|id| vec![id.to_string()])
        .collect();
    if sorted != want {
        failures.push(format!(
            "the shards' local span sets are {shard_sets:?}, expected {want:?} in some order"
        ));
    }
    let union: Vec<String> = DERIVED_TRACES.iter().map(|s| s.to_string()).collect();
    for (shard, label) in [(&shard1, "shard1"), (&shard2, "shard2")] {
        for table in ["trace_recent_dist", "trace_error_spans_dist"] {
            let got = trace_ids(shard, db, table).await;
            if got != union {
                failures.push(format!(
                    "{label}: {table} reads {got:?}, expected {union:?}"
                ));
            }
        }
    }
    exec_on(
        &shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Issue #560 criterion 9, clustered: the same block written twice
/// through the span table's wrapper leaves every shard's physical counts
/// unchanged. Merges on both derived tables are stopped on each shard
/// before the first write, so no merge can bring a duplicate back to one
/// before the count is read.
#[tokio::test]
async fn the_same_block_twice_through_the_wrapper_leaves_every_shard_count_unchanged() {
    skip_unless_live!();
    let shard1 = ChClient::new(shard1_config())
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config())
        .await
        .expect("connect shard2");
    let db: &str = &TEST_DB_DERIVED_REPLAY;
    fresh_cluster_db(&shard1, db).await;
    assert_clustered_tables(
        &shard1,
        &shard2,
        db,
        &[
            "trace_recent",
            "trace_recent_dist",
            "trace_error_spans",
            "trace_error_spans_dist",
        ],
    )
    .await;
    for shard in [&shard1, &shard2] {
        for table in ["trace_recent", "trace_error_spans"] {
            exec_on(shard, &format!("SYSTEM STOP MERGES {db}.{table}")).await;
        }
    }

    let insert = derived_insert(db, cluster_now_ns());
    let union: Vec<String> = DERIVED_TRACES.iter().map(|s| s.to_string()).collect();
    let mut failures: Vec<String> = Vec::new();
    for write in 1..=2 {
        exec_on(&shard1, &insert).await;
        exec_on(
            &shard1,
            &format!("SYSTEM FLUSH DISTRIBUTED {db}.trace_spans_dist"),
        )
        .await;
        wait_for_spans(&shard1, &shard2, db).await;
        for (shard, label) in [(&shard1, "shard1"), (&shard2, "shard2")] {
            let mut counts = Vec::new();
            for table in ["trace_spans", "trace_recent", "trace_error_spans"] {
                counts
                    .push(count_on(shard, &format!("SELECT count() AS n FROM {db}.{table}")).await);
            }
            eprintln!("write {write}, {label}: trace_spans/recent/error = {counts:?}");
            if counts != [1, 1, 1] {
                failures.push(format!(
                    "write {write}, {label}: local trace_spans/trace_recent/trace_error_spans \
                     = {counts:?}, expected [1, 1, 1]"
                ));
            }
        }
        for table in ["trace_recent_dist", "trace_error_spans_dist"] {
            let got = trace_ids(&shard1, db, table).await;
            if got != union {
                failures.push(format!(
                    "write {write}: {table} reads {got:?}, expected {union:?}"
                ));
            }
        }
    }
    for shard in [&shard1, &shard2] {
        for table in ["trace_recent", "trace_error_spans"] {
            exec_on(shard, &format!("SYSTEM START MERGES {db}.{table}")).await;
        }
    }
    exec_on(
        &shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct MetricSampleRow {
    metric_name: String,
    fingerprint: u128,
    unix_milli: i64,
    value: f64,
}

/// The span columns this test writes: every column without a default,
/// the five attribute arrays empty. The rest take their defaults.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct SpanInsertRow {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_id: [u8; 8],
    name: String,
    service: String,
    timestamp_ns: i64,
    duration_ns: i64,
    status_code: i8,
    kind: i8,
    payload_type: i8,
    payload: String,
    attr_key: Vec<String>,
    attr_scope: Vec<String>,
    attr_val: Vec<String>,
    attr_type: Vec<String>,
    attr_num: Vec<Option<f64>>,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SettingRow {
    v: String,
}

/// Issue #560 criterion 22, clustered: under a user profile that disables
/// `deduplicate_insert`, a repeated log block and a repeated metric block
/// are both stored twice — `insert_block`, which the log and metric
/// writers reach, carries no pin — while the repeated span block sent
/// with the span inserter's pins is stored once, in the span table and in
/// both derived tables.
#[tokio::test]
async fn a_disabling_profile_keeps_repeated_log_and_metric_blocks_and_drops_the_repeated_span_block()
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
    assert_clustered_tables(
        &shard1,
        &shard2,
        db,
        &[
            "trace_recent",
            "trace_recent_dist",
            "trace_error_spans",
            "trace_error_spans_dist",
        ],
    )
    .await;
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
        for table in ["trace_recent", "trace_error_spans"] {
            exec_on(shard, &format!("SYSTEM STOP MERGES {db}.{table}")).await;
        }
    }

    let mut cfg = shard1_config();
    cfg.database = db.to_string();
    cfg.user = user.clone();
    let as_user = ChClient::new(cfg)
        .await
        .expect("connect as the profile's user");
    let now = cluster_now_ns();
    let log = LogSampleRow {
        service: "checkout".to_string(),
        fingerprint: 0x0560_0560_0560_0560,
        timestamp_ns: now,
        severity: 9,
        body: "issue 560 repeated log block".to_string(),
    };
    let metric = MetricSampleRow {
        metric_name: "issue560_repeat".to_string(),
        fingerprint: 0x0560_0560_0560_0561,
        unix_milli: now / 1_000_000,
        value: 1.0,
    };
    let mut trace_id = [0u8; 16];
    trace_id[15] = 1;
    let span = SpanInsertRow {
        trace_id,
        span_id: [0, 0, 0, 0, 0, 0, 0, 1],
        parent_id: [0; 8],
        name: "op".to_string(),
        service: "svc".to_string(),
        timestamp_ns: now,
        duration_ns: 1_000,
        status_code: 2,
        kind: 2,
        payload_type: 1,
        payload: String::new(),
        attr_key: Vec::new(),
        attr_scope: Vec::new(),
        attr_val: Vec::new(),
        attr_type: Vec::new(),
        attr_num: Vec::new(),
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
        as_user
            .insert_block_with(
                "trace_spans_dist",
                std::slice::from_ref(&span),
                &QuerySettings::deduplicate_through_views(),
            )
            .await
            .expect("insert the span block");
        exec_on(
            &shard1,
            &format!("SYSTEM FLUSH DISTRIBUTED {db}.trace_spans_dist"),
        )
        .await;
    }

    let tables = [
        ("log_samples", 2u64),
        ("metric_samples", 2),
        ("trace_spans", 1),
        ("trace_recent", 1),
        ("trace_error_spans", 1),
    ];
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
    eprintln!("T32 counts log/metric/span/recent/error={counts:?}; settings={settings:?}");

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
        for table in ["trace_recent", "trace_error_spans"] {
            exec_on(shard, &format!("SYSTEM START MERGES {db}.{table}")).await;
        }
        exec_on(shard, &format!("DROP USER IF EXISTS {user}")).await;
    }
    exec_on(
        &shard1,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    assert!(mismatches.is_empty(), "T32 mismatches: {mismatches:?}");
}
