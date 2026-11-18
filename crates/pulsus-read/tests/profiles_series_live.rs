//! Issue #824: the time series' statement (S6) answers its tenant's
//! per-step totals, against a live ClickHouse.
//!
//! The rows are kind 0, 1, 2 and 3 landing rows, so the views fill the
//! sample, stack, series and entry tables as a push does. Each case plans
//! with `series::plan`, reads through `ProfilesEngine::read_series`, orders
//! the rows with `series::series` and compares every series' labels and
//! `(timestamp, value)` points.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test profiles_series_live
//! ```

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::Tenant;
use pulsus_read::profiles::selector::WindowLimits;
use pulsus_read::profiles::series::{self, SeriesRequest};
use pulsus_read::profiles::{ProfilesEngine, ProfilesReadConfig, ProfilesReadError};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const MINUTE_MS: i64 = 60_000;
const STEP_MS: i64 = 15_000;
const NO_TRACE: &str = "00000000000000000000000000000000";
const PROFILE: &str = "00000000-0000-0000-0000-0000000000a0";
const PROFILE_A: &str = "00000000-0000-0000-0000-0000000000a1";
const PROFILE_B: &str = "00000000-0000-0000-0000-0000000000b1";
const C1: &str = "00000000-0000-0000-0000-0000000000c1";
const C2: &str = "00000000-0000-0000-0000-0000000000c2";
const C3: &str = "00000000-0000-0000-0000-0000000000c3";

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

fn insert(rows: &[String]) -> String {
    format!(
        "INSERT INTO profile_landing \
         (org_id, received_ms, kind, type_id, service, fingerprint, timestamp_ns, stack_id, \
          value, span_id, trace_id, node_ids, names, labels, profile_id) VALUES {}",
        rows.join(", ")
    )
}

/// A kind-2 row: the series' stored labels, `pod` when given.
fn series_row(org: &str, s: i64, service: &str, fp: u128, pod: Option<&str>) -> String {
    let pod = pod.map(|p| format!(r#""pod":"{p}","#)).unwrap_or_default();
    let labels = format!(r#"{{"__profile_type__":"{CPU}",{pod}"service_name":"{service}"}}"#);
    format!(
        "('{org}', {s}, 2, '{CPU}', '{service}', {fp}, {}, 0, 0, 0, unhex('{NO_TRACE}'), [], [], \
         '{labels}', '{PROFILE}')",
        s * 1_000_000
    )
}

/// A kind-3 row: one profile's total.
fn entry_row(org: &str, s: i64, service: &str, fp: u128, ts_ms: i64, total: i64) -> String {
    profile_entry_row(org, s, service, fp, ts_ms, total, PROFILE)
}

/// A kind-3 row: `profile`'s total.
fn profile_entry_row(
    org: &str,
    s: i64,
    service: &str,
    fp: u128,
    ts_ms: i64,
    total: i64,
    profile: &str,
) -> String {
    format!(
        "('{org}', {s}, 3, '{CPU}', '{service}', {fp}, {}, 0, {total}, 0, unhex('{NO_TRACE}'), \
         [], [], '', '{profile}')",
        ts_ms * 1_000_000
    )
}

/// A kind-1 row: a stack's frames, root first.
fn stack_row(org: &str, s: i64, id: u128, frames: &str) -> String {
    let names: Vec<String> = frames.split(',').map(|n| format!("'{n}'")).collect();
    let ids: Vec<String> = (1..=names.len())
        .map(|n| (id * 100 + n as u128).to_string())
        .collect();
    format!(
        "('{org}', {s}, 1, '', '', 0, 0, {id}, 0, 0, unhex('{NO_TRACE}'), [{}], [{}], '', \
         '{PROFILE}')",
        ids.join(", "),
        names.join(", ")
    )
}

/// A kind-0 row: one sample of `profile`.
fn sample_row(
    org: &str,
    s: i64,
    fp: u128,
    ts_ms: i64,
    stack: u128,
    value: i64,
    profile: &str,
) -> String {
    format!(
        "('{org}', {s}, 0, '{CPU}', 'y', {fp}, {}, {stack}, {value}, 0, unhex('{NO_TRACE}'), [], \
         [], '', '{profile}')",
        ts_ms * 1_000_000
    )
}

/// Plan §6.2's rows. `S` is two hours ago, floored to the minute.
async fn land(client: &ChClient, s: i64) {
    let e = s + MINUTE_MS;
    let mut rows = Vec::new();
    for (org, scale) in [("t1", 1), ("t2", 100)] {
        rows.push(series_row(org, s, "x", 1, Some("a")));
        for (ts, total) in [(s - STEP_MS, 1), (s + STEP_MS, 2), (e + 1, 4)] {
            rows.push(entry_row(org, s, "x", 1, ts, total * scale));
        }
    }
    rows.push(series_row("t1", s, "x", 2, Some("b")));
    rows.push(entry_row("t1", s, "x", 2, s + 25_000, 2));
    rows.push(entry_row("t1", s, "x", 2, s + 30_000, 4));
    rows.push(series_row("t1", s, "x", 4, None));
    rows.push(entry_row("t1", s, "x", 4, s + 45_000, 5));
    rows.push(series_row("t1", s, "y", 3, Some("c")));
    for (id, frames) in [(11, "main,f"), (12, "main,f,h"), (13, "main,g")] {
        rows.push(stack_row("t1", s, id, frames));
    }
    let at = s + STEP_MS;
    for (stack, value, profile) in [
        (11, 1, PROFILE_A),
        (12, 1, PROFILE_A),
        (11, 4, PROFILE_B),
        (13, 100, PROFILE_B),
    ] {
        rows.push(sample_row("t1", s, 3, at, stack, value, profile));
    }
    exec(client, &insert(&rows)).await;
}

fn config() -> ProfilesReadConfig {
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

/// A series as `(labels, [(timestamp, value)])`.
type Got = (Vec<(String, String)>, Vec<(i64, f64)>);

struct Fixture {
    bootstrap: ChClient,
    db: String,
    s: i64,
}

impl Fixture {
    async fn new() -> Self {
        Self::in_database("pulsus_read_it_profiles_series").await
    }

    /// The fixture in a database of its own, `name`.
    async fn in_database(name: &str) -> Self {
        let db = pulsus_testkit::test_db(name);
        let bootstrap = ChClient::new(test_config("default"))
            .await
            .expect("connect (bootstrap)");
        exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
        run_init(&bootstrap, &RenderCtx::for_tests(&db, 7))
            .await
            .expect("run_init");
        let client = ChClient::new(test_config(&db)).await.expect("connect");
        let now = chrono::Utc::now().timestamp_millis();
        let s = (now - 120 * MINUTE_MS).div_euclid(MINUTE_MS) * MINUTE_MS;
        land(&client, s).await;
        Fixture { bootstrap, db, s }
    }

    async fn finish(self) {
        exec(
            &self.bootstrap,
            &format!("DROP DATABASE IF EXISTS {}", self.db),
        )
        .await;
    }

    /// `[S, S + 60 s]`, step 15 s.
    fn request<'a>(&self, selector: &'a str) -> SeriesRequest<'a> {
        SeriesRequest {
            profile_type: CPU,
            label_selector: selector,
            start_ms: self.s,
            end_ms: self.s + MINUTE_MS,
            step: 15.0,
            ..SeriesRequest::default()
        }
    }

    /// Plans `req` for `org`, reads its rows under `config` and orders them.
    async fn read(
        &self,
        org: &str,
        config: ProfilesReadConfig,
        req: &SeriesRequest<'_>,
    ) -> Result<Vec<Got>, ProfilesReadError> {
        let now = chrono::Utc::now().timestamp_millis();
        let sql = match series::plan(&tenant(org), &config, req, now) {
            Ok(Some(sql)) => sql,
            other => panic!("expected a plan for {req:?}, got {other:?}"),
        };
        let client = ChClient::new(test_config(&self.db)).await.expect("connect");
        let rows = ProfilesEngine::new(client, config, tenant(org))
            .read_series(&sql)
            .await?;
        Ok(series::series(rows, req.group_by, req.limit)
            .into_iter()
            .map(|s| {
                (
                    s.labels,
                    s.points.iter().map(|p| (p.timestamp, p.value)).collect(),
                )
            })
            .collect())
    }

    /// Issue #826: plans `req` with exemplars for `org`, reads its rows
    /// and orders them.
    async fn exemplars(&self, org: &str, req: &SeriesRequest<'_>) -> Vec<series::Series> {
        let req = SeriesRequest {
            exemplars: true,
            ..*req
        };
        let now = chrono::Utc::now().timestamp_millis();
        let sql = match series::plan(&tenant(org), &config(), &req, now) {
            Ok(Some(sql)) => sql,
            other => panic!("expected a plan for {req:?}, got {other:?}"),
        };
        let client = ChClient::new(test_config(&self.db)).await.expect("connect");
        let rows = ProfilesEngine::new(client, config(), tenant(org))
            .read_series_exemplars(&sql)
            .await
            .unwrap_or_else(|e| panic!("{req:?}: {e:?}\n{sql}"));
        series::series_exemplars(rows, req.group_by, req.limit)
    }

    async fn got(&self, org: &str, req: &SeriesRequest<'_>) -> Vec<Got> {
        self.read(org, config(), req)
            .await
            .unwrap_or_else(|e| panic!("{req:?}: {e:?}"))
    }
}

fn pod(v: &str) -> Vec<(String, String)> {
    vec![("pod".to_string(), v.to_string())]
}

/// L0–L6 against one database.
#[tokio::test]
async fn the_time_series_answers_its_tenants_totals() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let fx = Fixture::new().await;
    let s = fx.s;
    let by_pod = vec!["pod".to_string()];

    // L0: the fixture holds `t1`'s six entries.
    let client = ChClient::new(test_config(&fx.db)).await.expect("connect");
    let n = client
        .query_strings(
            "SELECT toString(count()) AS s FROM profile_entries WHERE org_id = 't1'",
            &QuerySettings::new(),
        )
        .await
        .expect("count");
    assert_eq!(n, ["6"], "L0");

    // L1: a profile a step before the window lands on its first point; one
    // after its end is not read.
    let a = r#"{pod="a"}"#;
    assert_eq!(
        fx.got("t1", &fx.request(a)).await,
        vec![(vec![], vec![(s, 1.0), (s + STEP_MS, 2.0)])],
        "L1"
    );

    // L2: `AVERAGE` is the mean of the step's totals; `SUM` their sum.
    let b = r#"{pod="b"}"#;
    assert_eq!(
        fx.got(
            "t1",
            &SeriesRequest {
                average: true,
                ..fx.request(b)
            }
        )
        .await,
        vec![(vec![], vec![(s + 2 * STEP_MS, 3.0)])],
        "L2 average"
    );
    assert_eq!(
        fx.got("t1", &fx.request(b)).await,
        vec![(vec![], vec![(s + 2 * STEP_MS, 6.0)])],
        "L2 sum"
    );

    // L3: the call site keeps each profile's samples on its stacks; the
    // average is over the profiles.
    let y = r#"{service_name="y"}"#;
    let call_site = vec!["main".to_string(), "f".to_string()];
    let with_call_site = SeriesRequest {
        call_site: &call_site,
        ..fx.request(y)
    };
    assert_eq!(
        fx.got(
            "t1",
            &SeriesRequest {
                average: true,
                ..with_call_site
            }
        )
        .await,
        vec![(vec![], vec![(s + STEP_MS, 3.0)])],
        "L3 average"
    );
    assert_eq!(
        fx.got("t1", &with_call_site).await,
        vec![(vec![], vec![(s + STEP_MS, 6.0)])],
        "L3 sum"
    );

    // L4: grouped by `pod`, ordered by the series' sums; the series without
    // a `pod` has no label.
    let x = r#"{service_name="x"}"#;
    let grouped = SeriesRequest {
        group_by: &by_pod,
        ..fx.request(x)
    };
    let l4 = vec![
        (pod("b"), vec![(s + 2 * STEP_MS, 6.0)]),
        (vec![], vec![(s + 3 * STEP_MS, 5.0)]),
        (pod("a"), vec![(s, 1.0), (s + STEP_MS, 2.0)]),
    ];
    assert_eq!(fx.got("t1", &grouped).await, l4, "L4");
    assert_eq!(
        fx.got(
            "t1",
            &SeriesRequest {
                limit: Some(1),
                ..grouped
            }
        )
        .await,
        vec![(pod("b"), vec![(s + 2 * STEP_MS, 6.0)])],
        "L4 limit 1"
    );

    // L5: each tenant reads its own entries.
    assert_eq!(
        fx.got("t2", &fx.request(a)).await,
        vec![(vec![], vec![(s, 100.0), (s + STEP_MS, 200.0)])],
        "L5 as t2"
    );
    for req in [fx.request(a), fx.request(b), grouped] {
        let got = fx.got("t1", &req).await;
        assert!(
            got.iter()
                .flat_map(|(_, points)| points)
                .all(|(_, v)| *v < 100.0),
            "L5: t1 holds a t2 value: {got:?}"
        );
    }

    // L6: the scan budget.
    let budget = ProfilesReadConfig {
        scan_budget_rows: 1,
        ..config()
    };
    assert_eq!(
        fx.read("t1", budget, &grouped).await,
        Err(ProfilesReadError::TooBroad(
            "query_too_broad: PULSUS_PROFILES_SCAN_BUDGET_ROWS".to_string()
        )),
        "L6"
    );

    fx.finish().await;
}

fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
    v.iter()
        .map(|(n, v)| (n.to_string(), v.to_string()))
        .collect()
}

/// A series of one point carrying one exemplar.
fn one_point(
    labels: Vec<(String, String)>,
    (timestamp, value): (i64, f64),
    (at, profile, total): (i64, &str, i64),
    exemplar_labels: Vec<(String, String)>,
) -> series::Series {
    series::Series {
        labels,
        points: vec![series::Point {
            timestamp,
            value,
            exemplar: Some(series::Exemplar {
                timestamp: at,
                profile_id: profile.to_string(),
                span_id: String::new(),
                trace_id: String::new(),
                value: total,
                labels: exemplar_labels,
            }),
        }],
    }
}

/// Issue #826, E1–E6: each point's exemplar is its largest profile,
/// labelled with its series' stored labels less the `group_by` names.
#[tokio::test]
async fn exemplars_name_each_points_largest_profile() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let fx = Fixture::in_database("pulsus_read_it_profiles_series_exemplars").await;
    let s = fx.s;
    let client = ChClient::new(test_config(&fx.db)).await.expect("connect");
    exec(
        &client,
        &insert(&[
            series_row("t1", s, "v", 9, Some("e")),
            profile_entry_row("t1", s, "v", 9, s + 20_000, 3, C1),
            profile_entry_row("t1", s, "v", 9, s + 25_000, 5, C2),
            series_row("t1", s, "v", 10, Some("f")),
            profile_entry_row("t1", s, "v", 10, s + 28_000, 4, C3),
        ]),
    )
    .await;
    let l = pairs(&[
        ("__profile_type__", CPU),
        ("pod", "e"),
        ("service_name", "v"),
    ]);
    let v = r#"{service_name="v"}"#;

    // E1: the point's largest profile, with all its series' labels.
    let e1 = fx.exemplars("t1", &fx.request(v)).await;
    assert_eq!(
        e1,
        vec![one_point(
            vec![],
            (s + 2 * STEP_MS, 12.0),
            (s + 25_000, C2, 5),
            l.clone()
        )],
        "E1"
    );

    // E2: grouped by `pod`, one pick per series, the labels without `pod`.
    let by_pod = vec!["pod".to_string()];
    let rest = pairs(&[("__profile_type__", CPU), ("service_name", "v")]);
    assert_eq!(
        fx.exemplars(
            "t1",
            &SeriesRequest {
                group_by: &by_pod,
                ..fx.request(v)
            }
        )
        .await,
        vec![
            one_point(
                pod("e"),
                (s + 2 * STEP_MS, 8.0),
                (s + 25_000, C2, 5),
                rest.clone()
            ),
            one_point(pod("f"), (s + 2 * STEP_MS, 4.0), (s + 28_000, C3, 4), rest),
        ],
        "E2"
    );

    // E3: with a call site, the exemplar's value is the profile's
    // call-site total.
    let call_site = vec!["main".to_string(), "f".to_string()];
    assert_eq!(
        fx.exemplars(
            "t1",
            &SeriesRequest {
                call_site: &call_site,
                ..fx.request(r#"{service_name="y"}"#)
            }
        )
        .await,
        vec![one_point(
            vec![],
            (s + STEP_MS, 6.0),
            (s + STEP_MS, PROFILE_B, 4),
            pairs(&[
                ("__profile_type__", CPU),
                ("pod", "c"),
                ("service_name", "y")
            ])
        )],
        "E3"
    );

    // E4: another tenant reads none of it.
    assert_eq!(fx.exemplars("t2", &fx.request(v)).await, vec![], "E4");

    // E5: off, no point carries an exemplar.
    let now = chrono::Utc::now().timestamp_millis();
    let sql = match series::plan(&tenant("t1"), &config(), &fx.request(v), now) {
        Ok(Some(sql)) => sql,
        other => panic!("E5: expected a plan, got {other:?}"),
    };
    let rows = ProfilesEngine::new(client, config(), tenant("t1"))
        .read_series(&sql)
        .await
        .expect("E5");
    let off = series::series(rows, &[], None);
    assert_eq!(off.len(), 1, "E5: {off:?}");
    assert!(
        off.iter()
            .flat_map(|s| &s.points)
            .all(|p| p.exemplar.is_none()),
        "E5: {off:?}"
    );

    // E6: `AVERAGE`: the point is the mean, the exemplar the largest
    // profile.
    assert_eq!(
        fx.exemplars(
            "t1",
            &SeriesRequest {
                average: true,
                ..fx.request(r#"{pod="e"}"#)
            }
        )
        .await,
        vec![one_point(
            vec![],
            (s + 2 * STEP_MS, 4.0),
            (s + 25_000, C2, 5),
            l
        )],
        "E6"
    );

    fx.finish().await;
}
