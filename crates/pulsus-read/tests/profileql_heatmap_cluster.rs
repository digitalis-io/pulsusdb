//! Issue #825: the heatmap's statement (S8) over the distributed entry and
//! series tables, against the 2-shard cluster.
//!
//! Plan §6.2's `x` rows for `t1`: fingerprints 1 and 2 with their series
//! rows on shard 1, fingerprint 3 with its series row on shard 2. Each
//! shard joins its own entries to its own series rows and sends each
//! cell's values; the node that receives the statement takes the range
//! over all of them and counts the buckets — one statement.
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test profileql_heatmap_cluster
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```

#[path = "query_log_wait/mod.rs"]
mod query_log_wait;
use query_log_wait::settle_query_log;

use std::collections::BTreeMap;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::Tenant;
use pulsus_read::profiles::heatmap::{self, HeatmapRequest, QUERY_TYPE_INDIVIDUAL};
use pulsus_read::profiles::selector::WindowLimits;
use pulsus_read::profiles::series::Exemplar;
use pulsus_read::profiles::{ProfilesEngine, ProfilesReadConfig};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CLUSTER_NAME: &str = "pulsus_test_cluster";
const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const MINUTE_MS: i64 = 60_000;
const SECOND_MS: i64 = 1_000;
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

/// A series row and its entries as `(timestamp ns, total)`, `pod` when
/// given.
fn rows(s: i64, fp: u128, pod: Option<&str>, entries: &[(i64, i64)]) -> Vec<String> {
    let pod = pod.map(|p| format!(r#""pod":"{p}","#)).unwrap_or_default();
    let labels = format!(r#"{{"__profile_type__":"{CPU}",{pod}"service_name":"x"}}"#);
    let mut out = vec![format!(
        "('t1', {s}, 2, '{CPU}', 'x', {fp}, {}, 0, 0, 0, unhex('{NO_TRACE}'), [], [], \
         '{labels}', '{PROFILE}')",
        s * 1_000_000
    )];
    for (ts_ns, total) in entries {
        out.push(format!(
            "('t1', {s}, 3, '{CPU}', 'x', {fp}, {ts_ns}, 0, {total}, 0, unhex('{NO_TRACE}'), [], \
             [], '', '{PROFILE}')"
        ));
    }
    out
}

/// A slot as its timestamp and its non-zero counts by bucket.
type Slot = (i64, BTreeMap<usize, i32>);
/// A series as its labels, its slots and its first slot's bucket starts.
type Got = (Vec<(String, String)>, Vec<Slot>, Vec<f64>);

fn pod(v: &str) -> Vec<(String, String)> {
    vec![("pod".to_string(), v.to_string())]
}

fn counts(v: &[(usize, i32)]) -> BTreeMap<usize, i32> {
    v.iter().copied().collect()
}

/// K1 and K2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_heatmap_merges_both_shards_in_one_statement() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
             run this test (see this file's module doc)"
        );
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_read_it_profileql_heatmap_cluster");
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
    let ns = |ms: i64| ms * 1_000_000;
    let mut on1 = rows(
        s,
        1,
        Some("a"),
        &[
            (ns(s + 5 * SECOND_MS), 0),
            (ns(s + 10 * SECOND_MS), 10),
            (ns(s + STEP_MS) + 500_000, 20),
        ],
    );
    on1.extend(rows(
        s,
        2,
        Some("b"),
        &[
            (ns(s - STEP_MS), 30),
            (ns(s - STEP_MS - 1), 30),
            (ns(e + 5 * SECOND_MS), 40),
        ],
    ));
    exec(&shard1, &insert(&on1)).await;
    exec(
        &shard2,
        &insert(&rows(s, 3, None, &[(ns(s + 50 * SECOND_MS), 10)])),
    )
    .await;
    for client in [&shard1, &replica1b, &shard2] {
        for table in ["profile_entries", "profile_series"] {
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
    let by_pod = vec!["pod".to_string()];
    let req = HeatmapRequest {
        profile_type: CPU,
        label_selector: r#"{service_name="x"}"#,
        start_ms: s,
        end_ms: e + 7 * SECOND_MS,
        step: 15.0,
        group_by: &by_pod,
        query_type: QUERY_TYPE_INDIVIDUAL,
        limit: None,
        exemplars: false,
    };
    let sql = match heatmap::plan(&tenant("t1"), &config, &req, now) {
        Ok(Some(sql)) => sql,
        other => panic!("K1: expected a plan, got {other:?}"),
    };
    let comment = format!("{db}-k1-{now}");
    let rows = ProfilesEngine::new(
        ChClient::new(shard1_config(&db)).await.expect("shard1"),
        config.clone(),
        tenant("t1"),
    )
    .with_query_log_comment(comment.clone())
    .read_heatmap(&sql)
    .await
    .unwrap_or_else(|err| panic!("K1: {err:?}\n{sql}"));

    // K1: L3's three series, from both shards, over one range.
    let got: Vec<Got> = heatmap::heatmap(rows, &by_pod, None)
        .into_iter()
        .map(|x| {
            let y_min = x.slots.first().map(|s| s.y_min.clone()).unwrap_or_default();
            let slots = x
                .slots
                .iter()
                .map(|slot| {
                    let c = slot
                        .counts
                        .iter()
                        .enumerate()
                        .filter(|(_, c)| **c != 0)
                        .map(|(i, c)| (i, *c))
                        .collect();
                    (slot.timestamp, c)
                })
                .collect();
            (x.labels, slots, y_min)
        })
        .collect();
    let y: Vec<f64> = (0..20).map(|i| f64::from(2 * i)).collect();
    assert_eq!(
        got,
        vec![
            (vec![], vec![(e, counts(&[(5, 1)]))], y.clone()),
            (
                pod("a"),
                vec![(s + STEP_MS, counts(&[(0, 1), (5, 1), (10, 1)]))],
                y.clone(),
            ),
            (
                pod("b"),
                vec![(s, counts(&[(15, 1)])), (e + STEP_MS, counts(&[(19, 1)]))],
                y,
            ),
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

    // K3 (issue #826): with `INDIVIDUAL` exemplars, K1's slots, each
    // cell's largest value, labelled from both shards' series rows; one
    // statement, the exemplar statement.
    let with = HeatmapRequest {
        exemplars: true,
        ..req
    };
    let sql = match heatmap::plan(&tenant("t1"), &config, &with, now) {
        Ok(Some(sql)) => sql,
        other => panic!("K3: expected a plan, got {other:?}"),
    };
    let comment = format!("{db}-k3-{now}");
    let rows = ProfilesEngine::new(
        ChClient::new(shard1_config(&db)).await.expect("shard1"),
        config,
        tenant("t1"),
    )
    .with_query_log_comment(comment.clone())
    .read_heatmap_exemplars(&sql)
    .await
    .unwrap_or_else(|err| panic!("K3: {err:?}\n{sql}"));
    let labels = vec![
        ("__profile_type__".to_string(), CPU.to_string()),
        ("service_name".to_string(), "x".to_string()),
    ];
    let exemplar = |timestamp: i64, value: i64| Exemplar {
        timestamp,
        profile_id: PROFILE.to_string(),
        span_id: String::new(),
        trace_id: String::new(),
        value,
        labels: labels.clone(),
    };
    type Slots = Vec<(i64, BTreeMap<usize, i32>, Vec<Exemplar>)>;
    let got: Vec<(Vec<(String, String)>, Slots)> = heatmap::heatmap_exemplars(rows, &by_pod, None)
        .into_iter()
        .map(|x| {
            let slots = x
                .slots
                .into_iter()
                .map(|slot| {
                    let c = slot
                        .counts
                        .iter()
                        .enumerate()
                        .filter(|(_, c)| **c != 0)
                        .map(|(i, c)| (i, *c))
                        .collect();
                    (slot.timestamp, c, slot.exemplars)
                })
                .collect();
            (x.labels, slots)
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (
                vec![],
                vec![(e, counts(&[(5, 1)]), vec![exemplar(s + 50 * SECOND_MS, 10)])]
            ),
            (
                pod("a"),
                vec![(
                    s + STEP_MS,
                    counts(&[(0, 1), (5, 1), (10, 1)]),
                    vec![
                        exemplar(s + 5 * SECOND_MS, 0),
                        exemplar(s + 10 * SECOND_MS, 10),
                        exemplar(s + STEP_MS, 20),
                    ]
                )]
            ),
            (
                pod("b"),
                vec![
                    (s, counts(&[(15, 1)]), vec![exemplar(s - STEP_MS, 30)]),
                    (
                        e + STEP_MS,
                        counts(&[(19, 1)]),
                        vec![exemplar(e + 5 * SECOND_MS, 40)]
                    ),
                ]
            ),
        ],
        "K3"
    );
    let filter = format!("is_initial_query AND log_comment = '{comment}'");
    settle_query_log(&[&shard1], &filter, 1).await;
    let sent = shard1
        .query_strings(
            &format!(
                "SELECT query AS s FROM system.query_log \
                 WHERE {filter} AND type = 'QueryFinish'"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("query_log");
    assert_eq!(sent.len(), 1, "K3 statements: {sent:?}");
    for expected in [
        "groupArray((e.total, e.timestamp_ns, e.profile_id, e.fingerprint))",
        "UNION ALL",
    ] {
        assert!(
            sent[0].contains(expected),
            "K3: {expected:?} missing from {}",
            sent[0]
        );
    }

    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}
