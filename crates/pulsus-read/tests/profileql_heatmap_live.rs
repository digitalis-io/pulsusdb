//! Issue #825: the heatmap's statement (S8) answers its tenant's bucket
//! counts, against a live ClickHouse.
//!
//! The rows are kind 0, 2 and 3 landing rows, so the views fill the sample,
//! series and entry tables as a push does. Each case plans with
//! `heatmap::plan`, reads through `ProfilesEngine::read_heatmap`, orders
//! the rows with `heatmap::heatmap` and compares every series' labels,
//! slots, counts and bucket starts.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test profileql_heatmap_live
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::Tenant;
use pulsus_read::profiles::heatmap::{
    self, HeatmapRequest, QUERY_TYPE_INDIVIDUAL, QUERY_TYPE_SPAN,
};
use pulsus_read::profiles::selector::WindowLimits;
use pulsus_read::profiles::series::Exemplar;
use pulsus_read::profiles::{ProfilesEngine, ProfilesReadConfig, ProfilesReadError};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const SECOND_MS: i64 = 1_000;
const MINUTE_MS: i64 = 60_000;
const STEP_MS: i64 = 15_000;
const NO_TRACE: &str = "00000000000000000000000000000000";
const T1: &str = "00000000000000000000000000000001";
const T2: &str = "00000000000000000000000000000002";
const PROFILE: &str = "00000000-0000-0000-0000-0000000000a0";
const P1: &str = "00000000-0000-0000-0000-0000000000a1";
const P2: &str = "00000000-0000-0000-0000-0000000000a2";
const D1: &str = "00000000-0000-0000-0000-0000000000d1";
const D2: &str = "00000000-0000-0000-0000-0000000000d2";
const D3: &str = "00000000-0000-0000-0000-0000000000d3";
const SPAN_A: u64 = 10;
const SPAN_B: u64 = 11;

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

/// A kind-3 row: one profile's total at `ts_ns`.
fn entry_row(org: &str, s: i64, service: &str, fp: u128, ts_ns: i64, total: i64) -> String {
    profile_entry_row(org, s, service, fp, ts_ns, total, PROFILE)
}

/// A kind-3 row: `profile`'s total at `ts_ns`.
fn profile_entry_row(
    org: &str,
    s: i64,
    service: &str,
    fp: u128,
    ts_ns: i64,
    total: i64,
    profile: &str,
) -> String {
    format!(
        "('{org}', {s}, 3, '{CPU}', '{service}', {fp}, {ts_ns}, 0, {total}, 0, \
         unhex('{NO_TRACE}'), [], [], '', '{profile}')"
    )
}

/// A kind-0 row: one sample of `profile`, on `stack`, in `span` of `trace`.
#[allow(clippy::too_many_arguments)]
fn sample_row(
    org: &str,
    s: i64,
    fp: u128,
    ts_ms: i64,
    stack: u128,
    value: i64,
    span: u64,
    trace: &str,
    profile: &str,
) -> String {
    format!(
        "('{org}', {s}, 0, '{CPU}', 'z', {fp}, {}, {stack}, {value}, {span}, unhex('{trace}'), \
         [], [], '', '{profile}')",
        ts_ms * 1_000_000
    )
}

/// Plan §6.2's rows. `S` is two hours ago, floored to the minute.
async fn land(client: &ChClient, s: i64) {
    let e = s + MINUTE_MS;
    let ns = |ms: i64| ms * 1_000_000;
    let mut rows = Vec::new();
    for (org, scale) in [("t1", 1), ("t2", 100)] {
        rows.push(series_row(org, s, "x", 1, Some("a")));
        for (ts_ns, total) in [
            (ns(s + 5 * SECOND_MS), 0),
            (ns(s + 10 * SECOND_MS), 10),
            (ns(s + STEP_MS) + 500_000, 20),
        ] {
            rows.push(entry_row(org, s, "x", 1, ts_ns, total * scale));
        }
    }
    rows.push(series_row("t1", s, "x", 2, Some("b")));
    for (ts_ms, total) in [
        (s - STEP_MS, 30),
        (s - STEP_MS - 1, 30),
        (e + 5 * SECOND_MS, 40),
    ] {
        rows.push(entry_row("t1", s, "x", 2, ns(ts_ms), total));
    }
    // Three series of service `w`, one value each: two hold only `b`, one
    // only `a`, so the label order and the raw values' order differ.
    for (fp, labels) in [(6, r#""b":"x","#), (7, r#""b":"y","#), (8, r#""a":"v","#)] {
        let labels = format!(r#"{{"__profile_type__":"{CPU}",{labels}"service_name":"w"}}"#);
        rows.push(format!(
            "('t1', {s}, 2, '{CPU}', 'w', {fp}, {}, 0, 0, 0, unhex('{NO_TRACE}'), [], [], \
             '{labels}', '{PROFILE}')",
            s * 1_000_000
        ));
        rows.push(entry_row("t1", s, "w", fp, ns(s + 5 * SECOND_MS), 5));
    }
    rows.push(series_row("t1", s, "x", 3, None));
    rows.push(entry_row("t1", s, "x", 3, ns(s + 50 * SECOND_MS), 10));
    // Both profiles at one time, so a key without the profile merges
    // their span A of trace T1.
    rows.push(series_row("t1", s, "z", 5, Some("d")));
    let at = s + 5 * SECOND_MS;
    for (stack, value, span, trace, profile) in [
        (11, 3, SPAN_A, T1, P1),
        (12, 4, SPAN_A, T1, P1),
        (11, 5, SPAN_B, T1, P1),
        (13, 100, 0, NO_TRACE, P1),
        (11, 2, SPAN_A, T1, P2),
        (11, 9, SPAN_A, T2, P2),
    ] {
        rows.push(sample_row(
            "t1", s, 5, at, stack, value, span, trace, profile,
        ));
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

/// A slot as its timestamp and its non-zero counts by bucket.
type Slot = (i64, BTreeMap<usize, i32>);
/// A series as its labels, its slots, and the bucket starts every slot
/// carries.
type Got = (Vec<(String, String)>, Vec<Slot>, Vec<f64>);

/// Issue #826: a series as its labels and its slots, each slot its
/// timestamp, its non-zero counts and its exemplars.
type WithExemplars = (
    Vec<(String, String)>,
    Vec<(i64, BTreeMap<usize, i32>, Vec<Exemplar>)>,
);

struct Fixture {
    bootstrap: ChClient,
    db: String,
    s: i64,
}

impl Fixture {
    async fn new() -> Self {
        Self::in_database(pulsus_testkit::test_db("pulsus_read_it_profileql_heatmap")).await
    }

    /// The fixture in the database `db`.
    async fn in_database(db: String) -> Self {
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

    /// `[S, S + 60 s]`, step 15 s, `INDIVIDUAL`.
    fn request<'a>(&self, selector: &'a str) -> HeatmapRequest<'a> {
        HeatmapRequest {
            profile_type: CPU,
            label_selector: selector,
            start_ms: self.s,
            end_ms: self.s + MINUTE_MS,
            step: 15.0,
            query_type: QUERY_TYPE_INDIVIDUAL,
            ..HeatmapRequest::default()
        }
    }

    /// Plans `req` for `org`, reads its rows under `config` and orders them.
    async fn read(
        &self,
        org: &str,
        config: ProfilesReadConfig,
        req: &HeatmapRequest<'_>,
    ) -> Result<Vec<Got>, ProfilesReadError> {
        let now = chrono::Utc::now().timestamp_millis();
        let sql = match heatmap::plan(&tenant(org), &config, req, now) {
            Ok(Some(sql)) => sql,
            other => panic!("expected a plan for {req:?}, got {other:?}"),
        };
        let client = ChClient::new(test_config(&self.db)).await.expect("connect");
        let rows = ProfilesEngine::new(client, config, tenant(org))
            .read_heatmap(&sql)
            .await?;
        Ok(heatmap::heatmap(rows, req.group_by, req.limit)
            .into_iter()
            .map(|series| {
                let y_min = series
                    .slots
                    .first()
                    .map(|slot| slot.y_min.clone())
                    .unwrap_or_default();
                let slots = series
                    .slots
                    .iter()
                    .map(|slot| {
                        assert_eq!(slot.y_min, y_min, "every slot carries the same starts");
                        assert_eq!(slot.counts.len(), 20, "20 counts per slot");
                        let counts = slot
                            .counts
                            .iter()
                            .enumerate()
                            .filter(|(_, c)| **c != 0)
                            .map(|(i, c)| (i, *c))
                            .collect();
                        (slot.timestamp, counts)
                    })
                    .collect();
                (series.labels, slots, y_min)
            })
            .collect())
    }

    /// Issue #826: plans `req` with exemplars for `org`, reads its rows
    /// and orders them, each slot as its timestamp, its non-zero counts
    /// and its exemplars.
    async fn exemplars(&self, org: &str, req: &HeatmapRequest<'_>) -> Vec<WithExemplars> {
        let req = HeatmapRequest {
            exemplars: true,
            ..*req
        };
        let now = chrono::Utc::now().timestamp_millis();
        let sql = match heatmap::plan(&tenant(org), &config(), &req, now) {
            Ok(Some(sql)) => sql,
            other => panic!("expected a plan for {req:?}, got {other:?}"),
        };
        let client = ChClient::new(test_config(&self.db)).await.expect("connect");
        let rows = ProfilesEngine::new(client, config(), tenant(org))
            .read_heatmap_exemplars(&sql)
            .await
            .unwrap_or_else(|e| panic!("{req:?}: {e:?}\n{sql}"));
        heatmap::heatmap_exemplars(rows, req.group_by, req.limit)
            .into_iter()
            .map(|series| {
                let slots = series
                    .slots
                    .into_iter()
                    .map(|slot| {
                        let counts = slot
                            .counts
                            .iter()
                            .enumerate()
                            .filter(|(_, c)| **c != 0)
                            .map(|(i, c)| (i, *c))
                            .collect();
                        (slot.timestamp, counts, slot.exemplars)
                    })
                    .collect();
                (series.labels, slots)
            })
            .collect()
    }

    async fn got(&self, org: &str, req: &HeatmapRequest<'_>) -> Vec<Got> {
        self.read(org, config(), req)
            .await
            .unwrap_or_else(|e| panic!("{req:?}: {e:?}"))
    }
}

fn pod(v: &str) -> Vec<(String, String)> {
    vec![("pod".to_string(), v.to_string())]
}

fn counts(v: &[(usize, i32)]) -> BTreeMap<usize, i32> {
    v.iter().copied().collect()
}

/// The 20 bucket starts, `f(i)` for bucket `i`.
fn starts(f: impl Fn(i32) -> i32) -> Vec<f64> {
    (0..20).map(|i| f64::from(f(i))).collect()
}

/// L0–L6 against one database.
#[tokio::test]
async fn the_heatmap_answers_its_tenants_bucket_counts() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let fx = Fixture::new().await;
    let s = fx.s;
    let e = s + MINUTE_MS;
    let by_pod = vec!["pod".to_string()];

    // L0: the fixture holds `t1`'s ten entries.
    let client = ChClient::new(test_config(&fx.db)).await.expect("connect");
    let n = client
        .query_strings(
            "SELECT toString(count()) AS s FROM profile_entries WHERE org_id = 't1'",
            &QuerySettings::new(),
        )
        .await
        .expect("count");
    assert_eq!(n, ["10"], "L0");

    // L1: a value on a bucket start lands in that bucket; a time 500 µs
    // past a slot's edge is in that slot.
    let a = r#"{pod="a"}"#;
    let l1 = vec![(
        vec![],
        vec![(s + STEP_MS, counts(&[(0, 1), (10, 1), (19, 1)]))],
        starts(|i| i),
    )];
    assert_eq!(fx.got("t1", &fx.request(a)).await, l1, "L1");

    // L2: a profile a step before `start` lands on `start`, one a
    // millisecond earlier is not read; one after the last grid point lands
    // in the slot after it.
    let b = r#"{pod="b"}"#;
    let past_end = HeatmapRequest {
        end_ms: e + 7 * SECOND_MS,
        ..fx.request(b)
    };
    assert_eq!(
        fx.got("t1", &past_end).await,
        vec![(
            vec![],
            vec![(s, counts(&[(1, 1)])), (e + STEP_MS, counts(&[(19, 1)])),],
            starts(|i| 30 + i / 2),
        )],
        "L2"
    );

    // L3: one range over every series; ordered by labels; past a limit,
    // by value count.
    let x = r#"{service_name="x"}"#;
    let grouped = HeatmapRequest {
        end_ms: e + 7 * SECOND_MS,
        group_by: &by_pod,
        ..fx.request(x)
    };
    let y = starts(|i| 2 * i);
    let l3 = vec![
        (vec![], vec![(e, counts(&[(5, 1)]))], y.clone()),
        (
            pod("a"),
            vec![(s + STEP_MS, counts(&[(0, 1), (5, 1), (10, 1)]))],
            y.clone(),
        ),
        (
            pod("b"),
            vec![(s, counts(&[(15, 1)])), (e + STEP_MS, counts(&[(19, 1)]))],
            y.clone(),
        ),
    ];
    assert_eq!(fx.got("t1", &grouped).await, l3, "L3");
    assert_eq!(
        fx.got(
            "t1",
            &HeatmapRequest {
                limit: Some(1),
                ..grouped
            }
        )
        .await,
        vec![l3[1].clone()],
        "L3 limit 1"
    );
    assert_eq!(
        fx.got(
            "t1",
            &HeatmapRequest {
                limit: Some(3),
                ..grouped
            }
        )
        .await,
        l3,
        "L3 limit 3"
    );

    // L4: `SPAN` sums each profile's samples of one span of one trace;
    // samples outside a span are not read.
    let z = r#"{service_name="z"}"#;
    assert_eq!(
        fx.got(
            "t1",
            &HeatmapRequest {
                query_type: QUERY_TYPE_SPAN,
                ..fx.request(z)
            }
        )
        .await,
        vec![(
            vec![],
            vec![(s + STEP_MS, counts(&[(2, 1), (11, 1), (17, 1), (19, 1)]))],
            starts(|i| 2 + i / 3),
        )],
        "L4"
    );

    // L5: each tenant reads its own entries.
    let t2_starts = starts(|i| 100 * i);
    assert_eq!(
        fx.got("t2", &fx.request(a)).await,
        vec![(
            vec![],
            vec![(s + STEP_MS, counts(&[(0, 1), (10, 1), (19, 1)]))],
            t2_starts,
        )],
        "L5 as t2"
    );
    let held: i32 = fx.got("t1", &fx.request(a)).await[0]
        .1
        .iter()
        .flat_map(|(_, c)| c.values())
        .sum();
    assert_eq!(held, 3, "L5: t1 holds its own three values");

    // L7: past a limit, equal counts tie on the labels, so `{a="v"}` is
    // kept ahead of `{b="x"}` and `{b="y"}`.
    let by_ab = vec!["a".to_string(), "b".to_string()];
    let w = r#"{service_name="w"}"#;
    let kept: Vec<Vec<(String, String)>> = fx
        .got(
            "t1",
            &HeatmapRequest {
                group_by: &by_ab,
                limit: Some(1),
                ..fx.request(w)
            },
        )
        .await
        .into_iter()
        .map(|(labels, _, _)| labels)
        .collect();
    assert_eq!(kept, vec![vec![("a".to_string(), "v".to_string())]], "L7");

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

/// An `INDIVIDUAL` exemplar.
fn individual(timestamp: i64, profile: &str, value: i64, labels: &[(String, String)]) -> Exemplar {
    Exemplar {
        timestamp,
        profile_id: profile.to_string(),
        span_id: String::new(),
        trace_id: String::new(),
        value,
        labels: labels.to_vec(),
    }
}

/// Issue #826, H1–H3: one exemplar per non-empty cell, its largest value,
/// in bucket order.
#[tokio::test]
async fn heatmap_exemplars_name_each_cells_largest_value() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let fx = Fixture::in_database(pulsus_testkit::test_db(
        "pulsus_read_it_profileql_heatmap_exemplars",
    ))
    .await;
    let s = fx.s;
    let ns = |ms: i64| ms * 1_000_000;
    let client = ChClient::new(test_config(&fx.db)).await.expect("connect");
    exec(
        &client,
        &insert(&[
            series_row("t1", s, "u", 11, Some("g")),
            profile_entry_row("t1", s, "u", 11, ns(s + 5 * SECOND_MS), 0, D1),
            profile_entry_row("t1", s, "u", 11, ns(s + 6 * SECOND_MS), 100, D2),
            profile_entry_row("t1", s, "u", 11, ns(s + 7 * SECOND_MS), 101, D3),
        ]),
    )
    .await;

    // H1: two cells, two exemplars; `D2` is not a cell's largest.
    let u = r#"{service_name="u"}"#;
    let all = pairs(&[
        ("__profile_type__", CPU),
        ("pod", "g"),
        ("service_name", "u"),
    ]);
    let h1 = |labels: &[(String, String)]| {
        vec![
            individual(s + 5 * SECOND_MS, D1, 0, labels),
            individual(s + 7 * SECOND_MS, D3, 101, labels),
        ]
    };
    assert_eq!(
        fx.exemplars("t1", &fx.request(u)).await,
        vec![(
            vec![],
            vec![(s + STEP_MS, counts(&[(0, 1), (19, 2)]), h1(&all))]
        )],
        "H1"
    );
    let by_pod = vec!["pod".to_string()];
    let rest = pairs(&[("__profile_type__", CPU), ("service_name", "u")]);
    assert_eq!(
        fx.exemplars(
            "t1",
            &HeatmapRequest {
                group_by: &by_pod,
                ..fx.request(u)
            }
        )
        .await,
        vec![(
            pod("g"),
            vec![(s + STEP_MS, counts(&[(0, 1), (19, 2)]), h1(&rest))]
        )],
        "H1 by pod"
    );

    // H2: `SPAN` exemplars carry the span and the trace, and no profile.
    let z_labels = pairs(&[
        ("__profile_type__", CPU),
        ("pod", "d"),
        ("service_name", "z"),
    ]);
    let span = |value: i64, span: &str, trace: &str| Exemplar {
        timestamp: s + 5 * SECOND_MS,
        profile_id: String::new(),
        span_id: span.to_string(),
        trace_id: trace.to_string(),
        value,
        labels: z_labels.clone(),
    };
    assert_eq!(
        fx.exemplars(
            "t1",
            &HeatmapRequest {
                query_type: QUERY_TYPE_SPAN,
                ..fx.request(r#"{service_name="z"}"#)
            }
        )
        .await,
        vec![(
            vec![],
            vec![(
                s + STEP_MS,
                counts(&[(2, 1), (11, 1), (17, 1), (19, 1)]),
                vec![
                    span(2, "0a00000000000000", T1),
                    span(5, "0b00000000000000", T1),
                    span(7, "0a00000000000000", T1),
                    span(9, "0a00000000000000", T2),
                ]
            )]
        )],
        "H2"
    );

    // H3: each tenant's own values.
    let a = r#"{pod="a"}"#;
    let a_labels = pairs(&[
        ("__profile_type__", CPU),
        ("pod", "a"),
        ("service_name", "x"),
    ]);
    for (org, scale) in [("t1", 1), ("t2", 100)] {
        assert_eq!(
            fx.exemplars(org, &fx.request(a)).await,
            vec![(
                vec![],
                vec![(
                    s + STEP_MS,
                    counts(&[(0, 1), (10, 1), (19, 1)]),
                    vec![
                        individual(s + 5 * SECOND_MS, PROFILE, 0, &a_labels),
                        individual(s + 10 * SECOND_MS, PROFILE, 10 * scale, &a_labels),
                        individual(s + STEP_MS, PROFILE, 20 * scale, &a_labels),
                    ]
                )]
            )],
            "H3 as {org}"
        );
    }

    fx.finish().await;
}
