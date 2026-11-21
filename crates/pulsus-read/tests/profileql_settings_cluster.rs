//! Issue #830: the exploration app's settings (S13-R and S13-W) on the
//! cluster-wide `profile_settings`, and the profile stats (S11') over the
//! series wrapper, against the 2-shard cluster.
//!
//! Engine `A` sends to shard 1's server and engine `B` to shard 2's. Each
//! test syncs the replicas before it reads or checks an older write, so it
//! shows that both servers decide the same way once the rows are there; it
//! does not show that a write is visible on another server at once, which
//! the plan leaves to replication.
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test profileql_settings_cluster
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::Tenant;
use pulsus_read::profiles::app::{
    SettingRow, StatsRow, Written, settings_get_sql, settings_write_sql, stats_sql,
};
use pulsus_read::profiles::selector::WindowLimits;
use pulsus_read::profiles::{ProfilesEngine, ProfilesReadConfig};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CLUSTER_NAME: &str = "pulsus_test_cluster";
const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const HOUR_MS: i64 = 3_600_000;
const NO_TRACE: &str = "00000000000000000000000000000000";
const PROFILE: &str = "00000000-0000-0000-0000-0000000000a0";

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn shard_config(host_env: &str, default_host: &str, port_env: &str, db: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var(host_env).unwrap_or_else(|_| default_host.to_string()),
        http_port: std::env::var(port_env)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8123),
        database: db.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(120),
        ..ChConnConfig::default()
    }
}

fn shard1_config(db: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD1_HOST",
        "172.28.0.11",
        "PULSUS_TEST_CH_SHARD1_HTTP_PORT",
        db,
    )
}

fn shard2_config(db: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD2_HOST",
        "172.28.0.12",
        "PULSUS_TEST_CH_SHARD2_HTTP_PORT",
        db,
    )
}

fn shard1b_config(db: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD1B_HOST",
        "172.28.0.13",
        "PULSUS_TEST_CH_SHARD1B_HTTP_PORT",
        db,
    )
}

fn cluster_ctx(db: &str) -> RenderCtx {
    RenderCtx {
        db: db.to_string(),
        cluster: Some(CLUSTER_NAME.to_string()),
        dist_suffix: "_dist".to_string(),
        ..RenderCtx::for_tests(db, 7)
    }
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

fn tenant(name: &str) -> Tenant {
    let header = http::HeaderValue::from_str(name).expect("a header value");
    Tenant::from_header(Some(&header), false).expect("a valid tenant")
}

fn config() -> ProfilesReadConfig {
    ProfilesReadConfig {
        series_table: "profile_series_dist".to_string(),
        samples_table: "profile_samples_dist".to_string(),
        entries_table: "profile_entries_dist".to_string(),
        landing_table: "profile_landing_dist".to_string(),
        adhoc_table: "profile_adhoc_dist".to_string(),
        dist_suffix: "_dist".to_string(),
        limits: WindowLimits::default(),
        scan_budget_rows: 50_000_000,
        max_tree_nodes: 4_194_304,
        max_result_bytes: 268_435_456,
        query_memory_bytes: 4_294_967_296,
        distributed: true,
        skip_unavailable_shards: false,
        max_nodes_default: 8_192,
        max_nodes_max: 1_048_576,
    }
}

/// A series row and an entry of `fp` at `t`, as kind 2 and 3 landing rows.
fn profile_rows(org: &str, fp: u128, t: i64, received: i64) -> String {
    let ns = t * 1_000_000;
    let labels = format!(r#"{{"__profile_type__":"{CPU}","service_name":"svc"}}"#);
    let row = |kind: u8, value: i64, labels: &str| {
        format!(
            "('{org}', {received}, {kind}, '{CPU}', 'svc', {fp}, {ns}, 0, {value}, 0, \
             unhex('{NO_TRACE}'), [], [], '{labels}', '{PROFILE}')"
        )
    };
    format!(
        "INSERT INTO profile_landing \
         (org_id, received_ms, kind, type_id, service, fingerprint, timestamp_ns, stack_id, \
          value, span_id, trace_id, node_ids, names, labels, profile_id) VALUES {}, {}",
        row(2, 0, &labels),
        row(3, 7, "")
    )
}

async fn sync(clients: &[&ChClient], tables: &[&str]) {
    for client in clients {
        for table in tables {
            exec(client, &format!("SYSTEM SYNC REPLICA {table} LIGHTWEIGHT")).await;
        }
    }
}

async fn write(
    engine: &ProfilesEngine,
    org: &str,
    name: &str,
    value: &str,
    m: i64,
    ctx: &str,
) -> Written {
    let sql = settings_write_sql(&tenant(org), name, value, m, false);
    engine
        .write_setting(&sql)
        .await
        .unwrap_or_else(|e| panic!("{ctx}: {e:?}\n{sql}"))
}

async fn listed(engine: &ProfilesEngine, org: &str, ctx: &str) -> Vec<SettingRow> {
    let sql = settings_get_sql(&tenant(org));
    engine
        .read_settings_rows(&sql)
        .await
        .unwrap_or_else(|e| panic!("{ctx}: {e:?}\n{sql}"))
}

fn row(name: &str, value: &str, modified_at: i64) -> SettingRow {
    SettingRow {
        name: name.to_string(),
        value: value.to_string(),
        modified_at,
    }
}

/// K1–K3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_settings_and_the_stats_agree_on_every_server() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
             run this test (see this file's module doc)"
        );
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_read_it_profileql_settings_cluster");
    let bootstrap = ChClient::new(shard1_config("default"))
        .await
        .expect("connect shard1 (bootstrap)");
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    run_init(&bootstrap, &cluster_ctx(&db))
        .await
        .expect("run_init (clustered)");
    let shard1 = ChClient::new(shard1_config(&db)).await.expect("shard1");
    let shard2 = ChClient::new(shard2_config(&db)).await.expect("shard2");
    let replica1b = ChClient::new(shard1b_config(&db))
        .await
        .expect("shard 1's second replica");
    let engine = |client: ChClient, org: &str| ProfilesEngine::new(client, config(), tenant(org));
    let a = engine(ChClient::new(shard1_config(&db)).await.expect("A"), "t1");
    let b = engine(ChClient::new(shard2_config(&db)).await.expect("B"), "t1");
    let all = [&shard1, &replica1b, &shard2];

    // K1: one time written on two servers: both stored, and both servers
    // answer the larger hash.
    assert_eq!(
        write(&a, "t1", "b", "x", 100, "K1 A").await,
        Written::Stored
    );
    assert_eq!(
        write(&b, "t1", "b", "y", 100, "K1 B").await,
        Written::Stored
    );
    sync(&all, &["profile_settings"]).await;
    let larger = shard1
        .query_strings(
            "SELECT if(cityHash64('x', toUInt8(0)) > cityHash64('y', toUInt8(0)), 'x', 'y') AS s",
            &QuerySettings::new(),
        )
        .await
        .expect("the larger hash")
        .remove(0);
    for (e, ctx) in [(&a, "K1 get A"), (&b, "K1 get B")] {
        assert_eq!(
            listed(e, "t1", ctx).await,
            [row("b", &larger, 100)],
            "{ctx}"
        );
    }

    // K2: an older write on another server, once the newer one has
    // replicated, is refused there.
    assert_eq!(write(&a, "t1", "a", "1", 10, "K2 A").await, Written::Stored);
    sync(&all, &["profile_settings"]).await;
    assert_eq!(write(&b, "t1", "a", "2", 5, "K2 B").await, Written::Refused);
    sync(&all, &["profile_settings"]).await;
    for (e, ctx) in [(&a, "K2 get A"), (&b, "K2 get B")] {
        assert_eq!(
            listed(e, "t1", ctx).await,
            [row("a", "1", 10), row("b", &larger, 100)],
            "{ctx}"
        );
    }

    // K3: one profile on each shard, two hours apart: both servers read
    // both shards' hours through the series wrapper.
    let now = chrono::Utc::now().timestamp_millis();
    let h = (now - 4 * HOUR_MS).div_euclid(HOUR_MS) * HOUR_MS;
    exec(&shard1, &profile_rows("t1", 301, h + 600_000, now)).await;
    exec(
        &shard2,
        &profile_rows("t1", 302, h + 2 * HOUR_MS + 600_000, now),
    )
    .await;
    sync(&all, &["profile_series"]).await;
    let want = StatsRow {
        ingested: true,
        oldest_ms: h,
        newest_ms: h + 3 * HOUR_MS - 1,
    };
    let sql = stats_sql(&tenant("t1"), "profile_series_dist");
    for (e, ctx) in [(&a, "K3 A"), (&b, "K3 B")] {
        let got = e
            .read_stats(&sql)
            .await
            .unwrap_or_else(|err| panic!("{ctx}: {err:?}\n{sql}"));
        assert_eq!(got, want, "{ctx}");
    }

    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}
