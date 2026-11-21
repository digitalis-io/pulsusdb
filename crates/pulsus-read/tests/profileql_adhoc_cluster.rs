//! Issue #831: ad hoc uploads on the 2-shard cluster (plan §7.3, K1–K5):
//! an upload stored through the landing wrapper on one server and read
//! through the `profile_adhoc` wrapper on another, the list over both
//! shards, and the block pins holding on the shard the upload lands on.
//!
//! Engine `A` sends to shard 1's server and engine `B` to shard 2's.
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test profileql_adhoc_cluster
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```

#[path = "profileql_adhoc_fixtures/mod.rs"]
mod fx;

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::Tenant;
use pulsus_read::profiles::adhoc::convert::{convert, detect};
use pulsus_read::profiles::adhoc::{AdhocRow, UploadRow, list_sql, mint_id, select, trees_sql};
use pulsus_read::profiles::selector::WindowLimits;
use pulsus_read::profiles::{ProfilesEngine, ProfilesReadConfig};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CLUSTER_NAME: &str = "pulsus_test_cluster";

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

async fn store(engine: &ProfilesEngine, name: &str, file: &[u8], at: i64, ctx: &str) -> String {
    let format = detect(name, file).unwrap_or_else(|e| panic!("{ctx}: {e}"));
    let converted = convert(format, name, file).unwrap_or_else(|e| panic!("{ctx}: {e:?}"));
    let id = mint_id(at);
    engine
        .store_upload(&id, name, at, &converted)
        .await
        .unwrap_or_else(|e| panic!("{ctx}: {e:?}"));
    id
}

async fn trees(
    engine: &ProfilesEngine,
    ids: &[&str],
    name: Option<&str>,
    ctx: &str,
) -> Vec<AdhocRow> {
    let sql = trees_sql(&tenant("t1"), &config(), ids, name);
    engine
        .read_adhoc(&sql)
        .await
        .unwrap_or_else(|e| panic!("{ctx}: {e:?}\n{sql}"))
}

async fn listed(engine: &ProfilesEngine, ctx: &str) -> Vec<UploadRow> {
    let sql = list_sql(&tenant("t1"), &config());
    engine
        .read_uploads(&sql)
        .await
        .unwrap_or_else(|e| panic!("{ctx}: {e:?}\n{sql}"))
}

/// The parts and rows of upload `id` on one node's own table.
async fn local(client: &ChClient, id: &str) -> (u64, u64) {
    let s = client
        .query_strings(
            &format!(
                "SELECT concat(toString(uniqExact(_part)), ' ', toString(count())) AS s \
                 FROM profile_adhoc WHERE upload_id = toUUID('{id}')"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("the local rows")
        .remove(0);
    let (p, n) = s.split_once(' ').expect("two counts");
    (p.parse().expect("parts"), n.parse().expect("rows"))
}

async fn sync(clients: &[&ChClient]) {
    for client in clients {
        exec(client, "SYSTEM SYNC REPLICA profile_adhoc").await;
    }
}

/// K1–K5.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_upload_is_read_and_listed_on_every_server() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
             run this test (see this file's module doc)"
        );
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_read_it_profileql_adhoc_cluster");
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
    let engine = |client: ChClient| ProfilesEngine::new(client, config(), tenant("t1"));
    let a = engine(ChClient::new(shard1_config(&db)).await.expect("A"));
    let b = engine(ChClient::new(shard2_config(&db)).await.expect("B"));
    let all = [&shard1, &replica1b, &shard2];
    let now = chrono::Utc::now().timestamp_millis();

    // K1: stored through A, read through B.
    let p = store(&a, "p.pb.gz", &fx::f_p(), now, "K1 store").await;
    sync(&all).await;
    let rows = trees(&b, &[&p], Some("cpu"), "K1 read").await;
    let s = select(&rows, &p);
    assert_eq!(s.types.len(), 2, "K1 types");
    assert_eq!(s.selected.map(|t| t.type_ordinal), Some(0), "K1 selected");
    let mut tree: Vec<(String, i64, i64)> = s
        .tree
        .iter()
        .map(|r| (r.name.clone(), r.total, r.self_value))
        .collect();
    tree.sort();
    assert_eq!(
        tree,
        [
            ("1f2e".to_string(), 2, 2),
            ("a".to_string(), 10, 10),
            ("b".to_string(), 0, 0),
            ("main".to_string(), 12, 0)
        ],
        "K1 tree"
    );

    // K2: one upload stored on each server, listed on both, newest first.
    let c = store(&a, "c.txt", fx::F_C, now + 1, "K2 store c").await;
    let x = store(&b, "x.txt", fx::F_X, now + 2, "K2 store x").await;
    sync(&all).await;
    for (e, ctx) in [(&a, "K2 list A"), (&b, "K2 list B")] {
        let ids: Vec<String> = listed(e, ctx).await.into_iter().map(|u| u.id).collect();
        assert_eq!(ids, [x.clone(), c.clone(), p.clone()], "{ctx}");
    }

    // K3: two uploads in one read, each with one selected type.
    let rows = trees(&a, &[&c, &x], None, "K3 read").await;
    for (id, nodes) in [(&c, 3), (&x, 4)] {
        let s = select(&rows, id);
        assert_eq!(s.types.len(), 1, "K3 {id} types");
        assert_eq!(s.selected.map(|t| t.type_ordinal), Some(0), "K3 {id}");
        assert_eq!(s.tree.len(), nodes, "K3 {id} nodes");
    }

    // K4: F-BIG, above the server's default block, lands whole in one
    // part on each replica of one shard and nowhere else.
    let big = store(&a, "big", &fx::f_big(), now + 3, "K4 store").await;
    sync(&all).await;
    let (s1, s1b, s2) = (
        local(&shard1, &big).await,
        local(&replica1b, &big).await,
        local(&shard2, &big).await,
    );
    eprintln!("K4: shard 1 {s1:?}, shard 1's second replica {s1b:?}, shard 2 {s2:?}");
    let whole = (1, fx::F_BIG_ROWS);
    assert!(
        (s1 == whole && s1b == whole && s2 == (0, 0))
            || (s1 == (0, 0) && s1b == (0, 0) && s2 == whole),
        "K4: parts and rows per node: {s1:?} {s1b:?} {s2:?}"
    );

    // K5: an upload the view refuses through the landing wrapper leaves no
    // row on any node.
    let refused = "01900000-0000-7000-8000-0000000000bb";
    let row = |kind: u8, is_type: u8, at: i64| {
        format!(
            "('t1', {now}, {kind}, toUUID('{refused}'), 'two-days', {at}, 0, {is_type}, 'collapsed', 'two-days', 'unknown', 100, '')"
        )
    };
    let insert = format!(
        "INSERT INTO profile_landing_dist (org_id, received_ms, kind, upload_id, upload_name, uploaded_at, \
         type_ordinal, is_type, upload_format, display_name, spy_name, sample_rate, units) VALUES {}, {}",
        row(5, 1, now),
        row(6, 0, now - 86_400_000)
    );
    let refusal = shard1
        .execute(
            &insert,
            &QuerySettings::new()
                .set("max_partitions_per_insert_block", 1u64)
                .set("distributed_foreground_insert", 1u64)
                .set("insert_deduplicate", 0u64),
            Idempotency::NonIdempotent,
        )
        .await;
    eprintln!("K5: the insert answered {refusal:?}");
    assert!(refusal.is_err(), "K5: the two-day upload was accepted");
    sync(&all).await;
    for node in all {
        assert_eq!(
            local(node, refused).await,
            (0, 0),
            "K5: a row of the refused upload"
        );
    }
    for (e, ctx) in [(&a, "K5 A"), (&b, "K5 B")] {
        assert!(
            !listed(e, ctx).await.iter().any(|u| u.id == refused),
            "{ctx}: listed"
        );
        assert!(
            trees(e, &[refused], None, ctx).await.is_empty(),
            "{ctx}: read"
        );
    }

    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}
