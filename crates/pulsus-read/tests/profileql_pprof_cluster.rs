//! Issue #828: the pprof export's statement (S9) over the distributed
//! sample and series tables, against the 2-shard cluster.
//!
//! Fixture P's samples are split across the shards, each shard with its
//! own series rows; the stack and location rows are landed once, on shard
//! 1, and the cluster copies them to every node. The statement reads the
//! samples through their wrapper and joins the merged stacks to the stacks
//! and locations read on the node that receives it — one statement.
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test profileql_pprof_cluster
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```

#[path = "profileql_pprof_nf/mod.rs"]
mod nf;
#[path = "query_log_wait/mod.rs"]
mod query_log_wait;
use query_log_wait::settle_query_log;

use std::time::Duration;

use nf::{CPU, P1, PERIOD, UNIT};
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::Tenant;
use pulsus_read::profiles::pprof::{self, PprofRequest};
use pulsus_read::profiles::selector::WindowLimits;
use pulsus_read::profiles::{ProfilesEngine, ProfilesReadConfig};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CLUSTER_NAME: &str = "pulsus_test_cluster";
const MINUTE_MS: i64 = 60_000;
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

fn list<T: std::fmt::Display>(v: impl Iterator<Item = T>) -> String {
    format!(
        "[{}]",
        v.map(|x| x.to_string()).collect::<Vec<_>>().join(", ")
    )
}

fn quoted<'a>(v: impl Iterator<Item = &'a str>) -> String {
    list(v.map(|s| format!("'{s}'")))
}

const COLUMNS: &str = "org_id, received_ms, kind, type_id, service, fingerprint, timestamp_ns, \
    stack_id, value, span_id, trace_id, node_ids, names, location_ids, location_id, mapping_file, \
    mapping_build_id, mapping_start, mapping_limit, mapping_offset, mapping_flags, address, \
    line_functions, line_system_names, line_files, line_start_lines, line_numbers, labels, \
    profile_id, period";

fn insert(rows: &[String]) -> String {
    format!(
        "INSERT INTO profile_landing ({COLUMNS}) VALUES {}",
        rows.join(", ")
    )
}

/// A landing row; every column a kind leaves unset is its default.
#[derive(Default)]
struct Row<'a> {
    kind: u8,
    service: &'a str,
    fingerprint: u128,
    ts_ms: i64,
    stack_id: u128,
    value: i64,
    trace: Option<&'a str>,
    names: Vec<&'a str>,
    location_ids: Vec<u128>,
    location_id: u128,
    mapping: (&'a str, &'a str, u64, u64, u8),
    address: u64,
    lines: Vec<(&'a str, i64, i64)>,
    labels: String,
    profile: Option<&'a str>,
    period: i64,
}

fn row(org: &str, received_ms: i64, r: &Row<'_>) -> String {
    let type_id = if r.kind == 0 || r.kind == 2 { CPU } else { "" };
    format!(
        "('{org}', {received_ms}, {}, '{type_id}', '{}', {}, {}, {}, {}, 0, unhex('{}'), {}, {}, \
         {}, {}, '{}', '{}', {}, {}, 0, {}, {}, {}, {}, {}, {}, {}, '{}', '{}', {})",
        r.kind,
        r.service,
        r.fingerprint,
        r.ts_ms * 1_000_000,
        r.stack_id,
        r.value,
        r.trace.unwrap_or(NO_TRACE),
        list((1..=r.names.len()).map(|i| i as u128)),
        quoted(r.names.iter().copied()),
        list(r.location_ids.iter()),
        r.location_id,
        r.mapping.0,
        r.mapping.1,
        r.mapping.2,
        r.mapping.3,
        r.mapping.4,
        r.address,
        quoted(r.lines.iter().map(|l| l.0)),
        quoted(r.lines.iter().map(|l| l.0)),
        quoted(r.lines.iter().map(|_| "main.go")),
        list(r.lines.iter().map(|l| l.1)),
        list(r.lines.iter().map(|l| l.2)),
        r.labels,
        r.profile.unwrap_or(PROFILE),
        r.period,
    )
}

/// P's location rows, `k` = 1–9, as ingest stores them.
fn location_rows() -> Vec<Row<'static>> {
    let mut out: Vec<Row<'static>> = nf::LINES
        .iter()
        .enumerate()
        .map(|(i, (name, line))| Row {
            kind: 4,
            location_id: i as u128 + 1,
            mapping: ("app", "bid", 0, 0, 15),
            lines: vec![(name, nf::function(name).3, *line)],
            ..Row::default()
        })
        .collect();
    out.push(Row {
        kind: 4,
        location_id: 9,
        mapping: ("libx.so", "", nf::LIBX_START, nf::LIBX_LIMIT, 0),
        address: nf::L9_ADDRESS,
        ..Row::default()
    });
    out
}

/// P's stack rows, `stack_id` = 1–7 in `nf::SAMPLES`' order.
fn stack_rows() -> Vec<Row<'static>> {
    nf::SAMPLES
        .iter()
        .enumerate()
        .map(|(i, (locs, _))| {
            let names: Vec<&'static str> = locs
                .iter()
                .rev()
                .filter(|&&k| k <= 8)
                .map(|&k| nf::LINES[k as usize - 1].0)
                .collect();
            Row {
                kind: 1,
                stack_id: i as u128 + 1,
                location_ids: locs.iter().map(|&k| u128::from(k)).collect(),
                names,
                ..Row::default()
            }
        })
        .collect()
}

fn series_row(service: &'static str, fingerprint: u128, ts_ms: i64) -> Row<'static> {
    Row {
        kind: 2,
        service,
        fingerprint,
        ts_ms,
        labels: format!(r#"{{"__profile_type__":"{CPU}","service_name":"{service}"}}"#),
        ..Row::default()
    }
}

/// K1 and K2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pprof_statement_merges_both_shards_in_one_statement() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
             run this test (see this file's module doc)"
        );
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_read_it_profileql_pprof_cluster");
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
    let at = t + 1_000;

    // The stacks and the locations, once, on shard 1.
    let rows: Vec<String> = stack_rows()
        .iter()
        .chain(location_rows().iter())
        .map(|r| row("t1", t, r))
        .collect();
    exec(&shard1, &insert(&rows)).await;

    // `[L2,L1]`, `[L3,L1]` and `[L4,L2,L1]` on shard 1, the rest on shard
    // 2, each shard under its own series.
    for (shard, client) in [(1u8, &shard1), (2u8, &shard2)] {
        let fp = 200 + u128::from(shard);
        let mut rows = vec![row("t1", t, &series_row("svc", fp, at))];
        for (i, (_, count)) in nf::SAMPLES.iter().enumerate() {
            let on = if i < 3 { 1 } else { 2 };
            if on == shard {
                rows.push(row(
                    "t1",
                    t,
                    &Row {
                        kind: 0,
                        service: "svc",
                        fingerprint: fp,
                        ts_ms: at,
                        stack_id: i as u128 + 1,
                        value: count * UNIT,
                        trace: (i == 0).then_some(nf::T1),
                        period: PERIOD,
                        ..Row::default()
                    },
                ));
            }
        }
        exec(client, &insert(&rows)).await;
    }
    for client in [&shard1, &replica1b, &shard2] {
        for table in [
            "profile_samples",
            "profile_series",
            "profile_stacks",
            "profile_locations",
        ] {
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
    let end = t + 29 * MINUTE_MS;
    let req = PprofRequest {
        profile_type: CPU,
        label_selector: r#"{service_name="svc"}"#,
        start_ms: t - 30 * MINUTE_MS,
        end_ms: end,
        ..PprofRequest::default()
    };
    let planned = match pprof::plan(&tenant("t1"), &config, &req, now) {
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
    .read_pprof(&planned.sql)
    .await
    .unwrap_or_else(|e| panic!("K1: {e:?}\n{}", planned.sql));

    // K1: both shards' samples, merged.
    assert_eq!(
        nf::nf(&pprof::build(&planned, rows)),
        nf::expected(&P1, UNIT, PERIOD, end * 1_000_000),
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
