//! Issue #827: the diff's statement (S7) over the distributed sample table,
//! against the 2-shard cluster.
//!
//! The left service's samples are split across the shards, the right
//! service's are on shard 2, each shard with its own series rows; the stacks
//! are landed once, on shard 1, and the cluster copies them to every node.
//! Each side's branch reads the samples through their wrapper and merges the
//! per-shard sums, and the stacks are read on the node that receives the
//! statement — one statement.
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test profileql_diff_cluster
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```

#[path = "query_log_wait/mod.rs"]
mod query_log_wait;
use query_log_wait::settle_query_log;

use std::collections::BTreeMap;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::Tenant;
use pulsus_read::profiles::diff::{self, DiffRow};
use pulsus_read::profiles::selector::WindowLimits;
use pulsus_read::profiles::tree::TreeRequest;
use pulsus_read::profiles::{ProfilesEngine, ProfilesReadConfig};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CLUSTER_NAME: &str = "pulsus_test_cluster";
const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const MINUTE_MS: i64 = 60_000;
const NO_TRACE: &str = "00000000000000000000000000000000";
const PROFILE: &str = "00000000-0000-0000-0000-0000000000a0";

/// The stacks: id, frames, service, value, and the shard its sample lands
/// on.
const STACKS: [(u128, &str, &str, i64, u8); 3] = [
    (1, "main,a", "left", 1, 1),
    (1, "main,a", "left", 2, 2),
    (2, "main,b", "right", 5, 2),
];

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

/// A node id: a hash of the path from the root.
fn node_id(path: &[&str]) -> u128 {
    let mut h: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    for name in path {
        for b in name.bytes().chain([0xff]) {
            h ^= u128::from(b);
            h = h.wrapping_mul(0x0000_0000_0100_0000_0000_0000_0000_013b);
        }
    }
    h
}

fn list<T: std::fmt::Display>(v: impl Iterator<Item = T>) -> String {
    format!(
        "[{}]",
        v.map(|x| x.to_string()).collect::<Vec<_>>().join(", ")
    )
}

fn insert(rows: &[String]) -> String {
    format!(
        "INSERT INTO profile_landing \
         (org_id, received_ms, kind, type_id, service, fingerprint, timestamp_ns, stack_id, \
          value, span_id, trace_id, node_ids, names, labels, profile_id) VALUES {}",
        rows.join(", ")
    )
}

/// `{path → (left total, left self, right total, right self)}` of the rows.
fn paths(rows: &[DiffRow]) -> BTreeMap<String, (i64, i64, i64, i64)> {
    let by_id: BTreeMap<u128, &DiffRow> = rows.iter().map(|r| (r.node, r)).collect();
    rows.iter()
        .map(|r| {
            let mut names = vec![r.name.clone()];
            let mut parent = r.parent;
            while parent != 0 {
                let p = by_id
                    .get(&parent)
                    .unwrap_or_else(|| panic!("no row for parent {parent}"));
                names.push(p.name.clone());
                parent = p.parent;
            }
            names.reverse();
            (
                names.join("/"),
                (r.left_total, r.left_self, r.right_total, r.right_self),
            )
        })
        .collect()
}

/// K1 and K2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_diff_merges_both_shards_in_one_statement() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
             run this test (see this file's module doc)"
        );
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_read_it_profileql_diff_cluster");
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
    let t = now - 30 * MINUTE_MS;
    let at = (t + 1_000) * 1_000_000;

    // The stacks, once, on shard 1.
    let stacks: Vec<String> = [(1u128, "main,a"), (2, "main,b")]
        .iter()
        .map(|(id, frames)| {
            let names: Vec<&str> = frames.split(',').collect();
            format!(
                "('t1', {t}, 1, '', '', 0, 0, {id}, 0, 0, unhex('{NO_TRACE}'), {}, {}, '', \
                 '{PROFILE}')",
                list((1..=names.len()).map(|n| node_id(&names[..n]))),
                list(names.iter().map(|n| format!("'{n}'")))
            )
        })
        .collect();
    exec(&shard1, &insert(&stacks)).await;

    // Each shard's samples, under its own series.
    for (shard, client) in [(1u8, &shard1), (2u8, &shard2)] {
        let mut rows = Vec::new();
        for (service, base) in [("left", 200u128), ("right", 300)] {
            let fp = base + u128::from(shard);
            let labels = format!(r#"{{"__profile_type__":"{CPU}","service_name":"{service}"}}"#);
            rows.push(format!(
                "('t1', {t}, 2, '{CPU}', '{service}', {fp}, {at}, 0, 0, 0, unhex('{NO_TRACE}'), \
                 [], [], '{labels}', '{PROFILE}')"
            ));
        }
        for (id, _, service, value, on) in STACKS {
            if on == shard {
                let fp = if service == "left" { 200 } else { 300 } + u128::from(shard);
                rows.push(format!(
                    "('t1', {t}, 0, '{CPU}', '{service}', {fp}, {at}, {id}, {value}, 0, \
                     unhex('{NO_TRACE}'), [], [], '', '{PROFILE}')"
                ));
            }
        }
        exec(client, &insert(&rows)).await;
    }
    for client in [&shard1, &replica1b, &shard2] {
        for table in ["profile_samples", "profile_series", "profile_stacks"] {
            exec(client, &format!("SYSTEM SYNC REPLICA {table} LIGHTWEIGHT")).await;
        }
    }

    let config = ProfilesReadConfig {
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
    };
    let side = |selector: &'static str| TreeRequest {
        profile_type: CPU,
        label_selector: selector,
        start_ms: t - 30 * MINUTE_MS,
        end_ms: t + 29 * MINUTE_MS,
        ..TreeRequest::default()
    };
    let (left, right) = (
        side(r#"{service_name="left"}"#),
        side(r#"{service_name="right"}"#),
    );
    let planned = match diff::plan(&tenant("t1"), &config, &left, &right, now) {
        Ok(Some(p)) => p,
        other => panic!("K1: expected a plan, got {other:?}"),
    };
    let comment = format!("{db}-k1-{now}");
    let rows = ProfilesEngine::new(
        ChClient::new(shard1_config(&db)).await.expect("shard1"),
        config,
        tenant("t1"),
    )
    .with_query_log_comment(comment.clone())
    .read_diff(&planned.sql)
    .await
    .unwrap_or_else(|e| panic!("K1: {e:?}\n{}", planned.sql));

    // K1: both shards' samples, merged per side.
    let expected: BTreeMap<String, (i64, i64, i64, i64)> = [
        ("main", (3, 0, 5, 0)),
        ("main/a", (3, 3, 0, 0)),
        ("main/b", (0, 0, 5, 5)),
    ]
    .iter()
    .map(|(p, v)| (p.to_string(), *v))
    .collect();
    assert_eq!(paths(&rows), expected, "K1");

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
