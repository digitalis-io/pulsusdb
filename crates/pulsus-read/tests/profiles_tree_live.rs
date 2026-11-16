//! Issue #823: the merged flame graph's statement (S5) answers its tenant's
//! merged tree, against a live ClickHouse.
//!
//! The rows are kind 0, 1 and 2 landing rows, so the views fill the sample,
//! stack and series tables as a push does. Merges of `profile_stacks` are
//! stopped, so each stack's three stored copies — two on the day before
//! `D0`, one on `D0` — stay unmerged and the statement has to count each
//! stack once. Each case plans with `tree::plan`, reads through
//! `ProfilesEngine::read_tree`, and compares `{path → (total, self)}`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test profiles_tree_live
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::Tenant;
use pulsus_read::profiles::selector::WindowLimits;
use pulsus_read::profiles::tree::{self, TreeRequest, TreeRow};
use pulsus_read::profiles::{ProfilesEngine, ProfilesReadConfig, ProfilesReadError};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const MINUTE_MS: i64 = 60_000;
const DAY_MS: i64 = 86_400_000;
const FP_SVC: u128 = 101;
const FP_SVC2: u128 = 102;
const PROFILE_SVC: &str = "00000000-0000-0000-0000-0000000000a0";
const PROFILE_A: &str = "00000000-0000-0000-0000-0000000000a1";
const PROFILE_B: &str = "00000000-0000-0000-0000-0000000000b1";
const TRACE_1: &str = "11111111111111111111111111111111";
const TRACE_2: &str = "22222222222222222222222222222222";
const NO_TRACE: &str = "00000000000000000000000000000000";

/// `svc`'s stacks (low-level §7.2), each with its id.
const SVC_STACKS: [(u128, &str); 6] = [
    (1, "main,a"),
    (2, "main,a,b"),
    (3, "main,a,c"),
    (4, "main,d"),
    (5, "main,e,f"),
    (6, "main,other,x"),
];
/// `svc2`'s one stack.
const SVC2_STACK: (u128, &str) = (7, "main,g");

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn test_config(database: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(120),
        ..ChConnConfig::default()
    }
}

fn tenant(name: &str) -> Tenant {
    let header = http::HeaderValue::from_str(name).expect("a header value");
    Tenant::from_header(Some(&header), false).expect("a valid tenant")
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(
            sql,
            &QuerySettings::new().set("insert_deduplicate", 0u64),
            Idempotency::NonIdempotent,
        )
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

/// A node id: a hash of the path from the root, so a shared prefix shares
/// its ids, as ingest's ids do.
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

fn sql_array<T: std::fmt::Display>(v: impl Iterator<Item = T>) -> String {
    format!(
        "[{}]",
        v.map(|x| x.to_string()).collect::<Vec<_>>().join(", ")
    )
}

/// A kind-1 row: the stack's node ids and names, root first.
fn stack_row(org: &str, received_ms: i64, (id, stack): (u128, &str)) -> String {
    let names: Vec<&str> = stack.split(',').collect();
    let ids = (1..=names.len()).map(|n| node_id(&names[..n]));
    format!(
        "('{org}', {received_ms}, 1, '', '', 0, 0, {id}, 0, 0, unhex('{NO_TRACE}'), {}, {}, '', \
         '{PROFILE_SVC}')",
        sql_array(ids),
        sql_array(names.iter().map(|n| format!("'{n}'")))
    )
}

/// A kind-0 row.
#[allow(clippy::too_many_arguments)]
fn sample_row(
    org: &str,
    received_ms: i64,
    service: &str,
    fp: u128,
    ts_ms: i64,
    stack: u128,
    value: i64,
    span: u64,
    trace: &str,
    profile: &str,
) -> String {
    format!(
        "('{org}', {received_ms}, 0, '{CPU}', '{service}', {fp}, {}, {stack}, {value}, {span}, \
         unhex('{trace}'), [], [], '', '{profile}')",
        ts_ms * 1_000_000
    )
}

/// A kind-2 row: the series' stored labels.
fn series_row(org: &str, received_ms: i64, service: &str, fp: u128, ts_ms: i64) -> String {
    let labels = format!(r#"{{"__profile_type__":"{CPU}","service_name":"{service}"}}"#);
    format!(
        "('{org}', {received_ms}, 2, '{CPU}', '{service}', {fp}, {}, 0, 0, 0, unhex('{NO_TRACE}'), \
         [], [], '{labels}', '{PROFILE_SVC}')",
        ts_ms * 1_000_000
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

/// Span id text as ingest stores it: 8 bytes, little-endian.
fn span(hex: &str) -> u64 {
    let mut b = [0u8; 8];
    for (i, byte) in b.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).expect("hex");
    }
    u64::from_le_bytes(b)
}

struct Fixture {
    bootstrap: ChClient,
    db: String,
    /// `D0 − 30 min`, `D0` today's UTC midnight.
    t: i64,
}

impl Fixture {
    async fn new() -> Self {
        let db = pulsus_testkit::test_db("pulsus_read_it_profiles_tree");
        let bootstrap = ChClient::new(test_config("default"))
            .await
            .expect("connect (bootstrap)");
        exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
        run_init(&bootstrap, &RenderCtx::for_tests(&db, 7))
            .await
            .expect("run_init");
        let client = ChClient::new(test_config(&db)).await.expect("connect");
        exec(&client, "SYSTEM STOP MERGES profile_stacks").await;
        let d0 = chrono::Utc::now().timestamp_millis().div_euclid(DAY_MS) * DAY_MS;
        let t = d0 - 30 * MINUTE_MS;

        // The stacks: three copies of `svc`'s for each tenant, two received
        // at `T`, one at `D0 + 1 s`; `svc2`'s once, for `t1`.
        for (copy, received) in [(0, t), (1, t), (2, d0 + 1_000)] {
            let mut rows = Vec::new();
            for org in ["t1", "t2"] {
                for s in SVC_STACKS {
                    rows.push(stack_row(org, received, s));
                }
            }
            if copy == 0 {
                rows.push(stack_row("t1", received, SVC2_STACK));
            }
            exec(&client, &insert(&rows)).await;
        }

        // The samples and the series.
        let s1 = span("0000000000000001");
        let s2 = span("0000000000000002");
        let mut rows = Vec::new();
        for (org, scale) in [("t1", 1), ("t2", 100)] {
            let at = t + 1_000;
            for (stack, value, ts, span) in [
                (1, 2, at, 0),
                (2, 10, at, s1),
                (2, 7, t + 2_000, s2),
                (3, 5, at, 0),
                (4, 3, at, 0),
                (5, 1, at, 0),
                (6, 4, at, 0),
            ] {
                rows.push(sample_row(
                    org,
                    t,
                    "svc",
                    FP_SVC,
                    ts,
                    stack,
                    value * scale,
                    span,
                    NO_TRACE,
                    PROFILE_SVC,
                ));
            }
            rows.push(series_row(org, t, "svc", FP_SVC, at));
        }
        let at = t + 1_000;
        rows.push(sample_row(
            "t1", t, "svc2", FP_SVC2, at, 7, 3, 0, TRACE_1, PROFILE_A,
        ));
        rows.push(sample_row(
            "t1", t, "svc2", FP_SVC2, at, 7, 5, 0, TRACE_2, PROFILE_B,
        ));
        rows.push(series_row("t1", t, "svc2", FP_SVC2, at));
        exec(&client, &insert(&rows)).await;
        Fixture { bootstrap, db, t }
    }

    async fn finish(self) {
        exec(
            &self.bootstrap,
            &format!("DROP DATABASE IF EXISTS {}", self.db),
        )
        .await;
    }

    fn config(&self) -> ProfilesReadConfig {
        ProfilesReadConfig {
            series_table: "profile_series".to_string(),
            samples_table: "profile_samples".to_string(),
            dist_suffix: String::new(),
            limits: WindowLimits::default(),
            scan_budget_rows: 50_000_000,
            max_tree_nodes: 4_194_304,
            max_result_bytes: 268_435_456,
            query_memory_bytes: 4_294_967_296,
            distributed: false,
            skip_unavailable_shards: false,
            max_nodes_default: 8_192,
            max_nodes_max: 1_048_576,
        }
    }

    /// `[T − 30 min, T + 29 min]`.
    fn request<'a>(&self, selector: &'a str) -> TreeRequest<'a> {
        TreeRequest {
            profile_type: CPU,
            label_selector: selector,
            start_ms: self.t - 30 * MINUTE_MS,
            end_ms: self.t + 29 * MINUTE_MS,
            ..TreeRequest::default()
        }
    }

    /// Plans `req` for `org` and reads its rows under `config`.
    async fn read(
        &self,
        org: &str,
        config: ProfilesReadConfig,
        req: &TreeRequest<'_>,
    ) -> Result<Vec<TreeRow>, ProfilesReadError> {
        let now = chrono::Utc::now().timestamp_millis();
        let planned = match tree::plan(&tenant(org), &config, req, now) {
            Ok(Some(p)) => p,
            other => panic!("expected a plan for {req:?}, got {other:?}"),
        };
        let client = ChClient::new(test_config(&self.db)).await.expect("connect");
        ProfilesEngine::new(client, config, tenant(org))
            .read_tree(&planned.sql)
            .await
    }

    async fn paths(&self, org: &str, req: &TreeRequest<'_>) -> BTreeMap<String, (i64, i64)> {
        let rows = self
            .read(org, self.config(), req)
            .await
            .unwrap_or_else(|e| panic!("{req:?}: {e:?}"));
        paths(&rows)
    }
}

/// `{path → (total, self)}` of the merged tree's rows.
fn paths(rows: &[TreeRow]) -> BTreeMap<String, (i64, i64)> {
    let by_id: BTreeMap<u128, &TreeRow> = rows.iter().map(|r| (r.node, r)).collect();
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
            (names.join("/"), (r.total, r.self_value))
        })
        .collect()
}

fn expected(rows: &[(&str, i64, i64)]) -> BTreeMap<String, (i64, i64)> {
    rows.iter()
        .map(|(p, total, self_value)| (p.to_string(), (*total, *self_value)))
        .collect()
}

fn l1() -> BTreeMap<String, (i64, i64)> {
    expected(&[
        ("main", 32, 0),
        ("main/a", 24, 2),
        ("main/a/b", 17, 17),
        ("main/a/c", 5, 5),
        ("main/d", 3, 3),
        ("main/e", 1, 0),
        ("main/e/f", 1, 1),
        ("main/other", 4, 0),
        ("main/other/x", 4, 4),
    ])
}

/// L0–L8 against one database.
#[tokio::test]
async fn the_merged_tree_answers_its_tenants_samples() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let fx = Fixture::new().await;
    let svc = r#"{service_name="svc"}"#;
    let svc2 = r#"{service_name="svc2"}"#;

    // L0: the fixture holds its copies.
    let client = ChClient::new(test_config(&fx.db)).await.expect("connect");
    let n = client
        .query_strings(
            "SELECT toString(count()) AS s FROM profile_stacks WHERE org_id = 't1'",
            &QuerySettings::new(),
        )
        .await
        .expect("count");
    assert_eq!(n, ["19"], "L0: 6 stacks × 3 copies, and svc2's once");

    // L1: each stack counted once, across two days and two copies on one.
    assert_eq!(fx.paths("t1", &fx.request(svc)).await, l1(), "L1");

    // L2: the call site keeps the stacks under `main, a`.
    let call_site = vec!["main".to_string(), "a".to_string()];
    assert_eq!(
        fx.paths(
            "t1",
            &TreeRequest {
                call_site: &call_site,
                ..fx.request(svc)
            }
        )
        .await,
        expected(&[
            ("main", 24, 0),
            ("main/a", 24, 2),
            ("main/a/b", 17, 17),
            ("main/a/c", 5, 5),
        ]),
        "L2"
    );

    // L3: the span filter.
    let spans = vec!["0000000000000002".to_string()];
    assert_eq!(
        fx.paths(
            "t1",
            &TreeRequest {
                span_selector: &spans,
                ..fx.request(svc)
            }
        )
        .await,
        expected(&[("main", 7, 0), ("main/a", 7, 0), ("main/a/b", 7, 7)]),
        "L3"
    );

    // L4: each tenant reads its own stacks and samples.
    let t2: BTreeMap<String, (i64, i64)> = l1()
        .into_iter()
        .map(|(p, (total, self_value))| (p, (total * 100, self_value * 100)))
        .collect();
    assert_eq!(fx.paths("t2", &fx.request(svc)).await, t2, "L4 as t2");
    let t1 = fx.paths("t1", &fx.request(svc)).await;
    assert!(
        t1.values().all(|(total, _)| *total < 100),
        "L4: t1 holds a t2 value: {t1:?}"
    );

    // L5: the profile id filter.
    let ids = vec![PROFILE_A.to_string()];
    assert_eq!(
        fx.paths(
            "t1",
            &TreeRequest {
                profile_id_selector: &ids,
                ..fx.request(svc2)
            }
        )
        .await,
        expected(&[("main", 3, 0), ("main/g", 3, 3)]),
        "L5"
    );
    assert_eq!(
        fx.paths("t1", &fx.request(svc2)).await,
        expected(&[("main", 8, 0), ("main/g", 8, 8)]),
        "L5 without the selector"
    );

    // L6: the trace filter.
    let traces = vec![TRACE_2.to_string()];
    assert_eq!(
        fx.paths(
            "t1",
            &TreeRequest {
                trace_id_selector: &traces,
                ..fx.request(svc2)
            }
        )
        .await,
        expected(&[("main", 5, 0), ("main/g", 5, 5)]),
        "L6"
    );

    // L7: the tree's rows are bounded: 9 rows pass a cap of 9, a cap of 8
    // refuses.
    let capped = |n: u64| ProfilesReadConfig {
        max_tree_nodes: n,
        ..fx.config()
    };
    assert_eq!(
        fx.read("t1", capped(8), &fx.request(svc)).await,
        Err(ProfilesReadError::TooBroad(
            "query_too_broad: PULSUS_PROFILES_MAX_TREE_NODES or PULSUS_PROFILES_MAX_RESULT_BYTES"
                .to_string()
        )),
        "L7 at 8"
    );
    let rows = fx
        .read("t1", capped(9), &fx.request(svc))
        .await
        .unwrap_or_else(|e| panic!("L7 at 9: {e:?}"));
    assert_eq!(paths(&rows), l1(), "L7 at 9");

    // L8: the scan budget.
    let budget = ProfilesReadConfig {
        scan_budget_rows: 1,
        ..fx.config()
    };
    assert_eq!(
        fx.read("t1", budget, &fx.request(svc)).await,
        Err(ProfilesReadError::TooBroad(
            "query_too_broad: PULSUS_PROFILES_SCAN_BUDGET_ROWS".to_string()
        )),
        "L8"
    );

    fx.finish().await;
}
