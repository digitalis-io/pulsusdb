//! Issue #827: the diff's statement (S7) answers both selections' merged
//! trees side by side, against a live ClickHouse.
//!
//! The rows are kind 0, 1 and 2 landing rows, so the views fill the sample,
//! stack and series tables as a push does. Merges of `profile_stacks` are
//! stopped, so a stack landed twice stays stored twice and the statement
//! has to count it once. Each case plans with `diff::plan`, reads through
//! `ProfilesEngine::read_diff`, and compares
//! `{path → (left total, left self, right total, right self)}`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test profileql_diff_live
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::Tenant;
use pulsus_read::profiles::diff::{self, DiffRow};
use pulsus_read::profiles::selector::WindowLimits;
use pulsus_read::profiles::tree::TreeRequest;
use pulsus_read::profiles::{ProfilesEngine, ProfilesReadConfig, ProfilesReadError};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;
const FP_LEFT: u128 = 301;
const FP_RIGHT: u128 = 302;
const NO_TRACE: &str = "00000000000000000000000000000000";
const PROFILE: &str = "00000000-0000-0000-0000-0000000000a0";
const LEFT: &str = r#"{service_name="left"}"#;
const RIGHT: &str = r#"{service_name="right"}"#;

/// The stacks: id and frames.
const MAIN_A: (u128, &str) = (1, "main,a");
const MAIN_B: (u128, &str) = (2, "main,b");
const MAIN_C: (u128, &str) = (3, "main,c");

/// `{path → (left total, left self, right total, right self)}`.
type Paths = BTreeMap<String, (i64, i64, i64, i64)>;

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
         '{PROFILE}')",
        sql_array(ids),
        sql_array(names.iter().map(|n| format!("'{n}'")))
    )
}

/// A kind-0 row, received 1 s before its time.
fn sample_row(org: &str, service: &str, fp: u128, ts_ms: i64, stack: u128, value: i64) -> String {
    format!(
        "('{org}', {}, 0, '{CPU}', '{service}', {fp}, {}, {stack}, {value}, 0, \
         unhex('{NO_TRACE}'), [], [], '', '{PROFILE}')",
        ts_ms - 1_000,
        ts_ms * 1_000_000
    )
}

/// A kind-2 row: the series' stored labels.
fn series_row(org: &str, service: &str, fp: u128, ts_ms: i64) -> String {
    let labels = format!(r#"{{"__profile_type__":"{CPU}","service_name":"{service}"}}"#);
    format!(
        "('{org}', {}, 2, '{CPU}', '{service}', {fp}, {}, 0, 0, 0, unhex('{NO_TRACE}'), \
         [], [], '{labels}', '{PROFILE}')",
        ts_ms - 1_000,
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

struct Fixture {
    bootstrap: ChClient,
    db: String,
    /// `D0 − 30 min`, `D0` today's UTC midnight.
    t: i64,
    /// `D0 − 2 days + 1 h`.
    u: i64,
}

impl Fixture {
    async fn new() -> Self {
        let db = pulsus_testkit::test_db("pulsus_read_it_profileql_diff");
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
        let u = d0 - 2 * DAY_MS + HOUR_MS;

        // The stacks, per tenant: `main,a` and `main,b` landed at `T` and
        // again at `D0 + 1 s`, `main,c` once at `U`.
        for received in [t, d0 + 1_000] {
            let mut rows = Vec::new();
            for org in ["t1", "t2"] {
                rows.push(stack_row(org, received, MAIN_A));
                rows.push(stack_row(org, received, MAIN_B));
            }
            exec(&client, &insert(&rows)).await;
        }
        let rows: Vec<String> = ["t1", "t2"]
            .iter()
            .map(|org| stack_row(org, u, MAIN_C))
            .collect();
        exec(&client, &insert(&rows)).await;

        // The samples and the series.
        let mut rows = Vec::new();
        for (org, scale) in [("t1", 1), ("t2", 100)] {
            let at = t + 1_000;
            rows.push(sample_row(org, "left", FP_LEFT, at, MAIN_A.0, 3 * scale));
            rows.push(series_row(org, "left", FP_LEFT, at));
            rows.push(sample_row(org, "right", FP_RIGHT, at, MAIN_B.0, 5 * scale));
            rows.push(series_row(org, "right", FP_RIGHT, at));
            let at = u + 1_000;
            rows.push(sample_row(org, "right", FP_RIGHT, at, MAIN_C.0, 7 * scale));
            rows.push(series_row(org, "right", FP_RIGHT, at));
        }
        exec(&client, &insert(&rows)).await;
        Fixture {
            bootstrap,
            db,
            t,
            u,
        }
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
            entries_table: "profile_entries".to_string(),
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

    /// `W = [T − 30 min, T + 29 min]`.
    fn w<'a>(&self, selector: &'a str) -> TreeRequest<'a> {
        TreeRequest {
            profile_type: CPU,
            label_selector: selector,
            start_ms: self.t - 30 * MINUTE_MS,
            end_ms: self.t + 29 * MINUTE_MS,
            ..TreeRequest::default()
        }
    }

    /// `W_old = [U − 30 min, U + 29 min]`.
    fn w_old<'a>(&self, selector: &'a str) -> TreeRequest<'a> {
        TreeRequest {
            start_ms: self.u - 30 * MINUTE_MS,
            end_ms: self.u + 29 * MINUTE_MS,
            ..self.w(selector)
        }
    }

    /// Plans `left` against `right` for `org` and reads the rows under
    /// `config`: the statement, and its answer.
    async fn read(
        &self,
        org: &str,
        config: ProfilesReadConfig,
        left: &TreeRequest<'_>,
        right: &TreeRequest<'_>,
    ) -> (String, Result<Vec<DiffRow>, ProfilesReadError>) {
        let now = chrono::Utc::now().timestamp_millis();
        let planned = match diff::plan(&tenant(org), &config, left, right, now) {
            Ok(Some(p)) => p,
            other => panic!("expected a plan for {left:?} and {right:?}, got {other:?}"),
        };
        let client = ChClient::new(test_config(&self.db)).await.expect("connect");
        let rows = ProfilesEngine::new(client, config, tenant(org))
            .read_diff(&planned.sql)
            .await;
        (planned.sql, rows)
    }

    async fn paths(&self, org: &str, left: &TreeRequest<'_>, right: &TreeRequest<'_>) -> Paths {
        let (sql, rows) = self.read(org, self.config(), left, right).await;
        paths(&rows.unwrap_or_else(|e| panic!("{e:?}\n{sql}")))
    }
}

/// `{path → (left total, left self, right total, right self)}` of the rows.
fn paths(rows: &[DiffRow]) -> Paths {
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

fn expected(rows: &[(&str, (i64, i64, i64, i64))]) -> Paths {
    rows.iter().map(|(p, v)| (p.to_string(), *v)).collect()
}

fn v1() -> Paths {
    expected(&[
        ("main", (3, 0, 5, 0)),
        ("main/a", (3, 3, 0, 0)),
        ("main/b", (0, 0, 5, 5)),
    ])
}

fn v2() -> Paths {
    expected(&[("main", (3, 0, 0, 0)), ("main/a", (3, 3, 0, 0))])
}

/// V0–V7 against one database.
#[tokio::test]
async fn the_diff_answers_both_sides_in_one_statement() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let fx = Fixture::new().await;

    // V0: the fixture holds two copies of `main,a`'s and `main,b`'s stack
    // rows, and the diff counts each stack once.
    let client = ChClient::new(test_config(&fx.db)).await.expect("connect");
    let n = client
        .query_strings(
            "SELECT toString(count()) AS s FROM profile_stacks WHERE org_id = 't1'",
            &QuerySettings::new(),
        )
        .await
        .expect("count");
    assert_eq!(n, ["5"], "V0: two stacks × 2 copies, and main,c once");
    assert_eq!(
        fx.paths("t1", &fx.w(LEFT), &fx.w(LEFT)).await,
        expected(&[("main", (3, 0, 3, 0)), ("main/a", (3, 3, 3, 3))]),
        "V0: each stack once on each side"
    );

    // V1: each side its own service.
    assert_eq!(fx.paths("t1", &fx.w(LEFT), &fx.w(RIGHT)).await, v1(), "V1");

    // V2: a right window with no data.
    let empty = TreeRequest {
        start_ms: fx.t - 6 * HOUR_MS,
        end_ms: fx.t - 5 * HOUR_MS,
        ..fx.w(RIGHT)
    };
    assert_eq!(fx.paths("t1", &fx.w(LEFT), &empty).await, v2(), "V2");

    // V3: a right window before the lookback floor has no branch.
    let now = chrono::Utc::now().timestamp_millis();
    let old = TreeRequest {
        start_ms: now - 8 * DAY_MS - HOUR_MS,
        end_ms: now - 8 * DAY_MS,
        ..fx.w(RIGHT)
    };
    let (sql, rows) = fx.read("t1", fx.config(), &fx.w(LEFT), &old).await;
    assert!(!sql.contains("SELECT 1 AS side"), "V3: {sql}");
    assert_eq!(
        paths(&rows.unwrap_or_else(|e| panic!("V3: {e:?}"))),
        v2(),
        "V3"
    );

    // V4: each tenant reads its own stacks and samples.
    let t2: Paths = v1()
        .into_iter()
        .map(|(p, (a, b, c, d))| (p, (a * 100, b * 100, c * 100, d * 100)))
        .collect();
    assert_eq!(
        fx.paths("t2", &fx.w(LEFT), &fx.w(RIGHT)).await,
        t2,
        "V4 as t2"
    );
    let t1 = fx.paths("t1", &fx.w(LEFT), &fx.w(RIGHT)).await;
    assert!(
        t1.values().all(|(a, _, c, _)| *a < 100 && *c < 100),
        "V4: t1 holds a t2 value: {t1:?}"
    );

    // V5: a call site on the left keeps only its stacks on that side; the
    // stacks neither side keeps are not answered.
    let call_site = vec!["main".to_string(), "a".to_string()];
    assert_eq!(
        fx.paths(
            "t1",
            &TreeRequest {
                call_site: &call_site,
                ..fx.w(RIGHT)
            },
            &fx.w(LEFT)
        )
        .await,
        expected(&[("main", (0, 0, 3, 0)), ("main/a", (0, 0, 3, 3))]),
        "V5"
    );

    // V6: the right window two days back reads its own days' stack rows.
    assert_eq!(
        fx.paths("t1", &fx.w(LEFT), &fx.w_old(RIGHT)).await,
        expected(&[
            ("main", (3, 0, 7, 0)),
            ("main/a", (3, 3, 0, 0)),
            ("main/c", (0, 0, 7, 7)),
        ]),
        "V6"
    );

    // V7: the scan budget counts both sides, and its refusal is
    // `query_too_broad`.
    let budget = ProfilesReadConfig {
        scan_budget_rows: 1,
        ..fx.config()
    };
    let (sql, rows) = fx.read("t1", budget, &fx.w(LEFT), &fx.w(RIGHT)).await;
    assert_eq!(
        rows,
        Err(ProfilesReadError::TooBroad(
            "query_too_broad: PULSUS_PROFILES_SCAN_BUDGET_ROWS".to_string()
        )),
        "V7: {sql}"
    );

    fx.finish().await;
}
