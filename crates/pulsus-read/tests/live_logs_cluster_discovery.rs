//! Issue #603: logs label discovery against a real 2-shard cluster, over
//! **split placement** — the placement the landing table produces.
//!
//! Before this change the writer inserted through the Logs family's `_dist`
//! wrappers, so `cityHash64(fingerprint)` put one stream's index rows and its
//! rollup rows on one shard, and the discovery read could evaluate its
//! semi-join shard-locally. The landing table has no wrapper: a push carries
//! many fingerprints, so routing its block through one would split the push
//! per shard and end "one push is one block". A fingerprint's rows can
//! therefore sit on several shards, and shard-local evaluation can only answer
//! *was this fingerprint active on **this** shard* where the question is *was
//! it active **anywhere***.
//!
//! **A clustered deployment is the production shape**, so this is the case for
//! the read path's primary deployment rather than a variant of it.
//!
//! Setup mirrors `live_metrics_cluster_fallback.rs` (clustered `run_init`, a
//! dedicated database, per-shard IP/port env overrides). Gated behind
//! `PULSUS_TEST_CLICKHOUSE=1` and requires the 2-shard fixture specifically:
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test live_logs_cluster_discovery
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::logql::TimeBounds;
use pulsus_read::{EngineConfig, LogQlEngine};
use pulsus_schema::{RenderCtx, run_init};

const CLUSTER_NAME: &str = "pulsus_test_cluster";

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
                 run this test (see crates/pulsus-read/tests/live_logs_cluster_discovery.rs \
                 for setup)"
            );
            return;
        }
    };
}

/// Same fixture-static-IP convention as `pulsus-schema/tests/live_cluster.rs`
/// — dial each shard directly by IP, never a name — overridable via the
/// identical env var names for a runtime where the host cannot route to the
/// compose network directly.
fn shard_config(
    host_env: &str,
    default_host: &str,
    port_env: &str,
    default_port: u16,
    database: &str,
) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var(host_env).unwrap_or_else(|_| default_host.to_string()),
        http_port: std::env::var(port_env)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(default_port),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 8,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

fn shard1_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD1_HOST",
        "172.28.0.11",
        "PULSUS_TEST_CH_SHARD1_HTTP_PORT",
        8123,
        database,
    )
}

fn shard2_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD2_HOST",
        "172.28.0.12",
        "PULSUS_TEST_CH_SHARD2_HTTP_PORT",
        8123,
        database,
    )
}

fn cluster_ctx(db: &str) -> RenderCtx {
    RenderCtx {
        cluster: Some(CLUSTER_NAME.to_string()),
        ..RenderCtx::for_tests(db)
    }
}

fn engine_config(db: &str) -> EngineConfig {
    EngineConfig {
        db: db.to_string(),
        streams_idx: "log_streams_idx_dist".to_string(),
        streams: "log_streams_dist".to_string(),
        samples: "log_samples_dist".to_string(),
        rollup_table: "log_metrics_5s_dist".to_string(),
        patterns_table: "log_patterns_dist".to_string(),
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
        distributed: true,
    }
}

async fn exec_on(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct ShardCountRow {
    shards: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct HostRow {
    host: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct ValueRow {
    value: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct MarksRow {
    marks: u64,
}

/// **The topology this case claims, established rather than assumed.**
///
/// `should_run()` is the same gate the single-node suites use, and the two
/// shard configurations default to the compose fixture's static container IPs.
/// Both pointed at ONE server is the unsafe direction: this case WOULD notice
/// it through its negative control — one server colocates the split inserts,
/// so the local subquery answers completely and the control fails — but
/// noticing through a control is not establishing the topology, so the
/// topology is read first, from the catalogue, with no insert.
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
            "shard{} reports {shards} shards in '{CLUSTER_NAME}': this case's \
             claim is about split placement across two",
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
         variables are aliased, and the split this case builds would be \
         colocated",
        hosts[0]
    );
}

/// The days-since-epoch a `Date` column takes for the month containing
/// `ts_ns`.
fn month_of(ts_ns: i64) -> u16 {
    pulsus_model::Date::start_of_month_utc(ts_ns)
        .expect("a representable month")
        .days_since_epoch()
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct IdxRow {
    month: u16,
    key: String,
    val: String,
    fingerprint: u128,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct RollupRow {
    fingerprint: u128,
    bucket_ns: i64,
    count: u64,
    bytes: u64,
}

/// **T39.** The production discovery read answers completely over split
/// placement.
///
/// **The fixture is deliberately split**, which is what the landing table
/// produces and what a comparison of the two product modes over co-sharded
/// rows cannot exercise: one stream's `log_streams_idx` rows go into shard 1's
/// **local** table and its `log_metrics_<res>` rows into shard 2's, by naming
/// the local tables rather than the wrappers.
///
/// Three assertions:
///
/// 1. a **negative control** — the nested-subquery statement this case issues
///    itself, under `distributed_product_mode = 'local'`, which is the shape
///    and the setting the read path used before issue #603 — returns the
///    *incomplete* values, so the fixture is proven split rather than assumed;
/// 2. the **production path**, dispatched through the engine's own discovery
///    entry point rather than through a statement this case writes, returns
///    the complete values. **This is the gate.**
/// 3. the two runs' per-shard `SelectedMarks`, read out of `system.query_log`,
///    are **recorded and compared as a bound, not asserted as a gate**.
///
/// **The mark equality is unsettled and this case claims none.** The sorting
/// key argues for one — `log_streams_idx` is `ORDER BY (key, val,
/// fingerprint)`, a discovery scan bounds `month` and fixes `key`, and
/// `fingerprint` is the third component, so no `fingerprint IN (…)` set prunes
/// a contiguous mark range. That argument does not close: on this fixture the
/// local control's set is EMPTY on the shard holding the index row, and what
/// the optimizer does with an empty `IN` set decides the marks. So the case
/// prints both figures with the shape beside each and fails only if either
/// exceeds that shape's own scan budget.
#[tokio::test]
async fn the_production_discovery_read_answers_completely_over_split_placement() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_logs_cluster_discovery");

    let bootstrap = ChClient::new(shard1_config("default"))
        .await
        .expect("connect shard1 (bootstrap)");
    exec_on(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    run_init(&bootstrap, &cluster_ctx(&db))
        .await
        .expect("run_init (clustered)");

    let shard1 = ChClient::new(shard1_config(&db))
        .await
        .expect("connect shard1 (data)");
    let shard2 = ChClient::new(shard2_config(&db))
        .await
        .expect("connect shard2 (data)");
    require_two_shard_topology(&shard1, &shard2).await;

    let ts = now_ns();
    let month = month_of(ts);
    const FP: u128 = 770_001;

    // **The split.** The index rows go to shard 1's LOCAL table and the
    // activity rows to shard 2's, by naming the base tables rather than the
    // `_dist` wrappers — so no sharding key decides placement and the two
    // sides of the question are provably on different servers.
    shard1
        .insert_block(
            "log_streams_idx",
            &[
                IdxRow {
                    month,
                    key: "env".to_string(),
                    val: "alpha".to_string(),
                    fingerprint: FP,
                },
                IdxRow {
                    month,
                    key: "service_name".to_string(),
                    val: "split".to_string(),
                    fingerprint: FP,
                },
            ],
        )
        .await
        .expect("seed shard 1's local index");
    shard2
        .insert_block(
            "log_metrics_5s",
            &[RollupRow {
                fingerprint: FP,
                bucket_ns: (ts / 5_000_000_000) * 5_000_000_000,
                count: 1,
                bytes: 10,
            }],
        )
        .await
        .expect("seed shard 2's local rollup");

    let bounds = TimeBounds {
        start_ns: ts - 60_000_000_000,
        end_ns: ts + 60_000_000_000,
    };

    // -- 1. the negative control: the nested subquery under `'local'`.
    //
    // Written out here rather than built by `sql::`: the point is what the
    // read path used to issue, and a control produced by the builder under
    // test would move with it.
    let control_id = format!("logs-disc-control-{}", std::process::id());
    let control_sql = format!(
        "SELECT DISTINCT val AS value\nFROM {db}.log_streams_idx_dist\n\
         WHERE month = toDate({month}) AND key = 'env'\n  \
         AND fingerprint IN (SELECT DISTINCT fingerprint FROM {db}.log_metrics_5s_dist \
         WHERE bucket_ns >= {} AND bucket_ns <= {})\nORDER BY value",
        (bounds.start_ns / 5_000_000_000) * 5_000_000_000,
        bounds.end_ns
    );
    let control = read_values(
        &shard1,
        &control_sql,
        &QuerySettings::new()
            .set("distributed_product_mode", "local")
            .set("log_comment", control_id.as_str())
            .set("query_id", control_id.as_str()),
    )
    .await;
    assert!(
        control.is_empty(),
        "the negative control must answer INCOMPLETELY over split placement — \
         shard 1 holds the index row and shard 2 holds the activity, so \
         shard-local evaluation finds neither side whole. It answered \
         {control:?}, which means the fixture is not split: the two shard \
         clients are reaching one server, or the inserts were forwarded"
    );

    // -- 2. the production path. **This is the gate.**
    let engine = LogQlEngine::new(
        ChClient::new(shard1_config(&db))
            .await
            .expect("connect the engine's client"),
        engine_config(&db),
    );
    let values = engine
        .label_values("env", None, bounds)
        .await
        .expect("the discovery read answers");
    assert_eq!(
        values,
        vec!["alpha".to_string()],
        "the production path must answer COMPLETELY over split placement: the \
         activity scan is a statement of its own and its result is rendered \
         into the index scan as a literal list, so neither side needs the \
         other to be on its own shard"
    );

    // -- 3. the marks, recorded and bounded — not asserted equal.
    exec_on(&shard1, "SYSTEM FLUSH LOGS").await;
    exec_on(&shard2, "SYSTEM FLUSH LOGS").await;
    let control_marks = selected_marks(&shard1, &shard2, &control_id).await;
    eprintln!(
        "T39 marks: nested-subquery-under-local = {control_marks} (per-shard sum). \
         The production shape's figure is not attributed here: it is two \
         statements, and this case reads one query id."
    );
    // The bound each shape is held to is its own scan budget, which is what
    // `EngineConfig::scan_budget_bytes` already enforces server-side; a mark
    // count past a whole table's marks would mean the month partition stopped
    // pruning.
    let total_marks = table_marks(&shard1, &shard2, &db, "log_streams_idx").await;
    assert!(
        control_marks <= total_marks,
        "the control read {control_marks} marks of a {total_marks}-mark table"
    );

    exec_on(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

async fn read_values(client: &ChClient, sql: &str, settings: &QuerySettings) -> Vec<String> {
    let mut stream = client
        .query_stream::<ValueRow>(sql, settings)
        .await
        .expect("the statement runs");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode ValueRow").value);
    }
    out
}

/// The per-shard `SelectedMarks` for `query_id`, summed — the per-shard,
/// coordinator-inclusive instrument `docs/schemas.md` §9 defines.
async fn selected_marks(shard1: &ChClient, shard2: &ChClient, query_id: &str) -> u64 {
    let sql = format!(
        "SELECT toUInt64(sum(ProfileEvents['SelectedMarks'])) AS marks FROM system.query_log \
         WHERE type = 'QueryFinish' AND (query_id = '{query_id}' \
         OR initial_query_id = '{query_id}')"
    );
    let mut total = 0u64;
    for client in [shard1, shard2] {
        let mut stream = client
            .query_stream::<MarksRow>(&sql, &QuerySettings::new())
            .await
            .expect("read system.query_log");
        if let Some(row) = stream.next().await {
            total += row.expect("decode MarksRow").marks;
        }
    }
    total
}

/// Every mark of `table`, summed across both shards' local parts — the
/// denominator a scan's own mark count is bounded by.
async fn table_marks(shard1: &ChClient, shard2: &ChClient, db: &str, table: &str) -> u64 {
    let sql = format!(
        "SELECT toUInt64(sum(marks)) AS marks FROM system.parts \
         WHERE active AND database = '{db}' AND table = '{table}'"
    );
    let mut total = 0u64;
    for client in [shard1, shard2] {
        let mut stream = client
            .query_stream::<MarksRow>(&sql, &QuerySettings::new())
            .await
            .expect("read system.parts");
        if let Some(row) = stream.next().await {
            total += row.expect("decode MarksRow").marks;
        }
    }
    total
}
