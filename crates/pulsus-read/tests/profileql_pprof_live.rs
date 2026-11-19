//! Issue #828: the pprof export's statement (S9) answers its tenant's merged
//! profile, against a live ClickHouse.
//!
//! Fixture P (plan §8.1) as kind 0, 1, 2 and 4 landing rows, so the views
//! fill the sample, stack, series and location tables as a push does.
//! Merges are stopped, so each stack and location row's three copies — two
//! on the day before `D0`, one on `D0` — stay unmerged and the statement
//! has to read each once. Each case plans with `pprof::plan`, reads through
//! `ProfilesEngine::read_pprof`, builds with `pprof::build` and compares
//! `NF`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test profileql_pprof_live
//! ```

#[path = "profileql_pprof_nf/mod.rs"]
mod nf;

use std::time::Duration;

use nf::{CPU, Nf, P1, P5, P6, P7, PERIOD, UNIT};
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::Tenant;
use pulsus_read::profiles::pprof::{self, PprofRequest, PprofRow};
use pulsus_read::profiles::selector::WindowLimits;
use pulsus_read::profiles::{ProfilesEngine, ProfilesReadConfig, ProfilesReadError};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const MINUTE_MS: i64 = 60_000;
const DAY_MS: i64 = 86_400_000;
const FP: u128 = 101;
const FP_D: u128 = 102;
const NO_TRACE: &str = "00000000000000000000000000000000";
const PROFILE: &str = "00000000-0000-0000-0000-0000000000a0";
const PROFILE_A: &str = "00000000-0000-0000-0000-0000000000a1";
const PROFILE_B: &str = "00000000-0000-0000-0000-0000000000b1";

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

struct Fixture {
    bootstrap: ChClient,
    db: String,
    /// `D0 − 30 min`, `D0` today's UTC midnight.
    t: i64,
}

impl Fixture {
    async fn new() -> Self {
        let db = pulsus_testkit::test_db("pulsus_read_it_profileql_pprof");
        let bootstrap = ChClient::new(test_config("default"))
            .await
            .expect("connect (bootstrap)");
        exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
        run_init(&bootstrap, &RenderCtx::for_tests(&db, 7))
            .await
            .expect("run_init");
        let client = ChClient::new(test_config(&db)).await.expect("connect");
        exec(&client, "SYSTEM STOP MERGES profile_stacks").await;
        exec(&client, "SYSTEM STOP MERGES profile_locations").await;
        let d0 = chrono::Utc::now().timestamp_millis().div_euclid(DAY_MS) * DAY_MS;
        let t = d0 - 30 * MINUTE_MS;

        // Every stack and location row three times per tenant: two received
        // on `T`'s day, one at `D0 + 1 s`; each copy its own block.
        for received in [t, t + 1, d0 + 1_000] {
            let mut rows = Vec::new();
            for org in ["t1", "t2"] {
                for r in stack_rows().iter().chain(location_rows().iter()) {
                    rows.push(row(org, received, r));
                }
            }
            exec(&client, &insert(&rows)).await;
        }

        // The samples and the series: P under `svc`, scaled per tenant.
        let at = t + 1_000;
        let mut rows = Vec::new();
        for (org, scale) in [("t1", 1), ("t2", 100)] {
            for (i, (_, count)) in nf::SAMPLES.iter().enumerate() {
                rows.push(row(
                    org,
                    t,
                    &Row {
                        kind: 0,
                        service: "svc",
                        fingerprint: FP,
                        ts_ms: at,
                        stack_id: i as u128 + 1,
                        value: count * UNIT * scale,
                        trace: (i == 0).then_some(nf::T1),
                        period: PERIOD,
                        ..Row::default()
                    },
                ));
            }
            rows.push(row(org, t, &series_row("svc", FP, at)));
        }
        // LP2d: P under `svc-d` as two profiles: A, period 10 ms, holding
        // the T1 sample; B, period 20 ms, holding the rest.
        for (i, (_, count)) in nf::SAMPLES.iter().enumerate() {
            let a = i == 0;
            rows.push(row(
                "t1",
                t,
                &Row {
                    kind: 0,
                    service: "svc-d",
                    fingerprint: FP_D,
                    ts_ms: if a { at } else { at + 1_000 },
                    stack_id: i as u128 + 1,
                    value: count * UNIT,
                    trace: a.then_some(nf::T1),
                    profile: Some(if a { PROFILE_A } else { PROFILE_B }),
                    period: if a { PERIOD } else { 2 * PERIOD },
                    ..Row::default()
                },
            ));
        }
        rows.push(row("t1", t, &series_row("svc-d", FP_D, at)));
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

    /// `[T − 30 min, T + 29 min]`.
    fn request<'a>(&self, selector: &'a str) -> PprofRequest<'a> {
        PprofRequest {
            profile_type: CPU,
            label_selector: selector,
            start_ms: self.t - 30 * MINUTE_MS,
            end_ms: self.t + 29 * MINUTE_MS,
            ..PprofRequest::default()
        }
    }

    fn end_ns(&self) -> i64 {
        (self.t + 29 * MINUTE_MS) * 1_000_000
    }

    /// Plans `req` for `org` and reads its rows under `config`.
    async fn rows(
        &self,
        org: &str,
        config: ProfilesReadConfig,
        req: &PprofRequest<'_>,
    ) -> Result<(pprof::Planned, Vec<PprofRow>), ProfilesReadError> {
        let now = chrono::Utc::now().timestamp_millis();
        let planned = match pprof::plan(&tenant(org), &config, req, now) {
            Ok(Some(p)) => p,
            other => panic!("expected a plan for {req:?}, got {other:?}"),
        };
        let client = ChClient::new(test_config(&self.db)).await.expect("connect");
        let rows = ProfilesEngine::new(client, config, tenant(org))
            .read_pprof(&planned.sql)
            .await?;
        Ok((planned, rows))
    }

    async fn nf_under(
        &self,
        org: &str,
        config: ProfilesReadConfig,
        req: &PprofRequest<'_>,
    ) -> Result<Nf, ProfilesReadError> {
        let (planned, rows) = self.rows(org, config, req).await?;
        Ok(nf::nf(&pprof::build(&planned, rows)))
    }

    async fn nf(&self, org: &str, req: &PprofRequest<'_>) -> Nf {
        self.nf_under(org, self.config(), req)
            .await
            .unwrap_or_else(|e| panic!("{req:?}: {e:?}"))
    }
}

/// LP0–LP6 against one database.
#[tokio::test]
async fn the_pprof_statement_answers_its_tenants_merged_profile() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let fx = Fixture::new().await;
    let svc = r#"{service_name="svc"}"#;
    let want =
        |samples: &[(&str, i64)], unit: i64| nf::expected(samples, unit, PERIOD, fx.end_ns());

    // LP0: the fixture holds its copies.
    let client = ChClient::new(test_config(&fx.db)).await.expect("connect");
    let n = client
        .query_strings(
            "SELECT toString(count()) AS s FROM profile_locations WHERE org_id = 't1'",
            &QuerySettings::new(),
        )
        .await
        .expect("count");
    assert_eq!(n, ["27"], "LP0: 9 locations × 3 copies");

    // LP1: every stack and location read once, across two days and two
    // copies on one; `a:20 main:10` and `a:21 main:10` stay two samples.
    assert_eq!(fx.nf("t1", &fx.request(svc)).await, want(&P1, UNIT), "LP1");

    // LP2: `go_pgo{keep 1}`.
    let keep1 = PprofRequest {
        go_pgo: Some((1, false)),
        ..fx.request(svc)
    };
    assert_eq!(fx.nf("t1", &keep1).await, want(&P6, UNIT), "LP2");

    // LP2b: `go_pgo{keep 5, aggregate}`: the statement re-merges the
    // callees, so its rows are P5's samples, summed, one location per path
    // position.
    let aggregate = PprofRequest {
        go_pgo: Some((5, true)),
        ..fx.request(svc)
    };
    let (planned, rows) = fx
        .rows("t1", fx.config(), &aggregate)
        .await
        .unwrap_or_else(|e| panic!("LP2b: {e:?}"));
    let mut stacks: Vec<(usize, i64)> = rows
        .iter()
        .filter(|r| r.kind == 0)
        .map(|r| (r.location_ids.len(), r.v))
        .collect();
    stacks.sort();
    let mut want_stacks: Vec<(usize, i64)> = P5
        .iter()
        .map(|(p, count)| (p.split_whitespace().count(), count * UNIT))
        .collect();
    want_stacks.sort();
    assert_eq!(stacks, want_stacks, "LP2b: the statement's stack rows");
    assert_eq!(
        nf::nf(&pprof::build(&planned, rows)),
        want(&P5, UNIT),
        "LP2b"
    );

    // LP2c: the pushed period.
    assert_eq!(fx.nf("t1", &fx.request(svc)).await.period, PERIOD, "LP2c");

    // LP2d: the period of the samples the filters select.
    let svc_d = r#"{service_name="svc-d"}"#;
    let t1 = vec![nf::T1.to_string()];
    let traced = PprofRequest {
        trace_id_selector: &t1,
        ..fx.request(svc_d)
    };
    assert_eq!(fx.nf("t1", &traced).await.period, PERIOD, "LP2d traced");
    assert_eq!(
        fx.nf("t1", &fx.request(svc_d)).await.period,
        2 * PERIOD,
        "LP2d all"
    );

    // LP3: the trace filter.
    let traced = PprofRequest {
        trace_id_selector: &t1,
        ..fx.request(svc)
    };
    assert_eq!(fx.nf("t1", &traced).await, want(&P7, UNIT), "LP3");

    // LP4: each tenant reads its own rows.
    assert_eq!(
        fx.nf("t2", &fx.request(svc)).await,
        want(&P1, 100 * UNIT),
        "LP4 as t2"
    );
    let mine = fx.nf("t1", &fx.request(svc)).await;
    assert!(
        mine.samples
            .keys()
            .all(|(_, values)| values.iter().all(|v| *v < 100 * UNIT)),
        "LP4: t1 holds a t2 value: {mine:?}"
    );

    // LP5: the stack and location rows are bounded.
    let bytes = |n: u64| ProfilesReadConfig {
        max_result_bytes: n,
        ..fx.config()
    };
    assert_eq!(
        fx.nf_under("t1", bytes(100), &fx.request(svc)).await,
        Err(ProfilesReadError::TooBroad(
            "query_too_broad: PULSUS_PROFILES_MAX_TREE_NODES or PULSUS_PROFILES_MAX_RESULT_BYTES"
                .to_string()
        )),
        "LP5 at 100 bytes"
    );
    assert_eq!(
        fx.nf_under("t1", bytes(1_000_000), &fx.request(svc))
            .await
            .unwrap_or_else(|e| panic!("LP5 at 1,000,000 bytes: {e:?}")),
        want(&P1, UNIT),
        "LP5 at 1,000,000 bytes"
    );

    // LP6: the scan budget.
    let budget = ProfilesReadConfig {
        scan_budget_rows: 1,
        ..fx.config()
    };
    assert_eq!(
        fx.nf_under("t1", budget, &fx.request(svc)).await,
        Err(ProfilesReadError::TooBroad(
            "query_too_broad: PULSUS_PROFILES_SCAN_BUDGET_ROWS".to_string()
        )),
        "LP6"
    );

    fx.finish().await;
}
