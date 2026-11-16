//! Issue #824: the time series' statement (S6) over the distributed entry
//! and series tables, against the 2-shard cluster.
//!
//! Plan §6.2's `x` rows for `t1`: fingerprints 1 and 2 with their series
//! rows on shard 1, fingerprint 4 with its series row on shard 2. Each
//! shard joins its own entries to its own series rows and pre-aggregates;
//! the node that receives the statement merges them — one statement.
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test profiles_series_cluster
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```

#[path = "query_log_wait/mod.rs"]
mod query_log_wait;
use query_log_wait::settle_query_log;

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::Tenant;
use pulsus_read::profiles::selector::WindowLimits;
use pulsus_read::profiles::series::{self, SeriesRequest};
use pulsus_read::profiles::{ProfilesEngine, ProfilesReadConfig};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CLUSTER_NAME: &str = "pulsus_test_cluster";
const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const MINUTE_MS: i64 = 60_000;
const STEP_MS: i64 = 15_000;
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

fn insert(rows: &[String]) -> String {
    format!(
        "INSERT INTO profile_landing \
         (org_id, received_ms, kind, type_id, service, fingerprint, timestamp_ns, stack_id, \
          value, span_id, trace_id, node_ids, names, labels, profile_id) VALUES {}",
        rows.join(", ")
    )
}

/// A series row and its entries, `pod` when given.
fn rows(s: i64, fp: u128, pod: Option<&str>, entries: &[(i64, i64)]) -> Vec<String> {
    let pod = pod.map(|p| format!(r#""pod":"{p}","#)).unwrap_or_default();
    let labels = format!(r#"{{"__profile_type__":"{CPU}",{pod}"service_name":"x"}}"#);
    let mut out = vec![format!(
        "('t1', {s}, 2, '{CPU}', 'x', {fp}, {}, 0, 0, 0, unhex('{NO_TRACE}'), [], [], \
         '{labels}', '{PROFILE}')",
        s * 1_000_000
    )];
    for (ts, total) in entries {
        out.push(format!(
            "('t1', {s}, 3, '{CPU}', 'x', {fp}, {}, 0, {total}, 0, unhex('{NO_TRACE}'), [], [], \
             '', '{PROFILE}')",
            ts * 1_000_000
        ));
    }
    out
}

/// A series as `(labels, [(timestamp, value)])`.
type Got = (Vec<(String, String)>, Vec<(i64, f64)>);

fn pod(v: &str) -> Vec<(String, String)> {
    vec![("pod".to_string(), v.to_string())]
}

/// K1 and K2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_time_series_merges_both_shards_in_one_statement() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
             run this test (see this file's module doc)"
        );
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_read_it_profiles_series_cluster");
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

    let now = chrono::Utc::now().timestamp_millis();
    let s = (now - 120 * MINUTE_MS).div_euclid(MINUTE_MS) * MINUTE_MS;
    let e = s + MINUTE_MS;
    let mut on1 = rows(
        s,
        1,
        Some("a"),
        &[(s - STEP_MS, 1), (s + STEP_MS, 2), (e + 1, 4)],
    );
    on1.extend(rows(s, 2, Some("b"), &[(s + 25_000, 2), (s + 30_000, 4)]));
    exec(&shard1, &insert(&on1)).await;
    exec(&shard2, &insert(&rows(s, 4, None, &[(s + 45_000, 5)]))).await;
    for client in [&shard1, &replica1b, &shard2] {
        for table in ["profile_entries", "profile_series"] {
            exec(client, &format!("SYSTEM SYNC REPLICA {table} LIGHTWEIGHT")).await;
        }
    }

    let config = ProfilesReadConfig {
        series_table: "profile_series_dist".to_string(),
        samples_table: "profile_samples_dist".to_string(),
        entries_table: "profile_entries_dist".to_string(),
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
    };
    let by_pod = vec!["pod".to_string()];
    let req = SeriesRequest {
        profile_type: CPU,
        label_selector: r#"{service_name="x"}"#,
        start_ms: s,
        end_ms: e,
        step: 15.0,
        group_by: &by_pod,
        ..SeriesRequest::default()
    };
    let sql = match series::plan(&tenant("t1"), &config, &req, now) {
        Ok(Some(sql)) => sql,
        other => panic!("K1: expected a plan, got {other:?}"),
    };
    let comment = format!("{db}-k1-{now}");
    let rows = ProfilesEngine::new(
        ChClient::new(shard1_config(&db)).await.expect("shard1"),
        config,
        tenant("t1"),
    )
    .with_query_log_comment(comment.clone())
    .read_series(&sql)
    .await
    .unwrap_or_else(|err| panic!("K1: {err:?}\n{sql}"));

    // K1: L4's three series, from both shards.
    let got: Vec<Got> = series::series(rows, &by_pod, None)
        .into_iter()
        .map(|x| {
            (
                x.labels,
                x.points.iter().map(|p| (p.timestamp, p.value)).collect(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (pod("b"), vec![(s + 2 * STEP_MS, 6.0)]),
            (vec![], vec![(s + 3 * STEP_MS, 5.0)]),
            (pod("a"), vec![(s, 1.0), (s + STEP_MS, 2.0)]),
        ],
        "K1"
    );

    // K2: one statement on the node that received it.
    let filter = format!("is_initial_query AND log_comment = '{comment}'");
    settle_query_log(&[&shard1], &filter, 1).await;
    let n = shard1
        .query_strings(
            &format!(
                "SELECT toString(count()) AS s FROM system.query_log \
                 WHERE {filter} AND type = 'QueryFinish'"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("query_log");
    assert_eq!(n, ["1"], "K2");

    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}
