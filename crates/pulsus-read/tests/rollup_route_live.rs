//! Issue #614, part 2: the log rollup answers the range aggregations it can
//! answer exactly, and every answer equals the line count the reference
//! semantics give, computed here from the fixture's own definition.
//!
//! Live ClickHouse, gated behind `PULSUS_TEST_CLICKHOUSE=1`.

use std::collections::BTreeMap;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_logql::parse;
use pulsus_read::logql::{
    Direction, EngineConfig, LogQlEngine, QueryParams, QueryResult, QuerySpec, RouteChoice,
};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const RES_NS: i64 = 5_000_000_000;

/// The routing reason of each rollup fallback (issue #614, part 2).
const LABEL_REASON: &str =
    "raw: a sum by label is not a stream label of every selected stream (issue #614)";
const REFUSED_REASON: &str = "raw: the rollup statement cannot render this grid (issue #614)";
const MEMORY_REASON: &str = "raw: the rollup statement exceeded its memory (issue #614)";
const SEC: i64 = 1_000_000_000;
/// The fixture's span: twelve minutes of lines.
const SPAN_S: i64 = 720;

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

/// One fixture line: stream, timestamp, body length, structured metadata.
#[derive(Clone)]
struct Line {
    fp: u64,
    ts: i64,
    len: usize,
    sm: &'static str,
}

const FP_A: u64 = 614_001;
const FP_B: u64 = 614_002;
const FP_C: u64 = 614_003;

/// Stream labels as the stream table stores them.
fn stream_labels(fp: u64) -> (&'static str, &'static [(&'static str, &'static str)]) {
    match fp {
        FP_A => (
            r#"{"app":"a","env":"p","service_name":"svc"}"#,
            &[("app", "a"), ("env", "p"), ("service_name", "svc")],
        ),
        FP_B => (
            r#"{"app":"a","env":"q","service_name":"svc"}"#,
            &[("app", "a"), ("env", "q"), ("service_name", "svc")],
        ),
        _ => (
            r#"{"app":"b","service_name":"svc"}"#,
            &[("app", "b"), ("service_name", "svc")],
        ),
    }
}

/// The lines. Every count on a bucket start differs from its neighbours'
/// (`(s / 5) % 4` lines at second `s`), so a window that counted `[t - r, t)`
/// instead of `(t - r, t]` gives a different total; B carries two metadata
/// values, so its lines form three series on the raw route; C has no `env`
/// stream label and carries `env` as metadata.
fn lines(base: i64) -> Vec<Line> {
    let mut out = Vec::new();
    for s in 0..SPAN_S {
        let t = base + s * SEC;
        if s % 5 == 0 {
            for i in 0..((s / 5) % 4) {
                out.push(Line {
                    fp: FP_A,
                    ts: t,
                    len: (i as usize) * 3,
                    sm: "",
                });
            }
        } else {
            out.push(Line {
                fp: FP_A,
                ts: t,
                len: (s % 7) as usize,
                sm: "",
            });
        }
        if s % 3 == 0 {
            out.push(Line {
                fp: FP_A,
                ts: t + 250_000_000,
                len: 11,
                sm: "",
            });
        }
        let sm = match s % 3 {
            0 => "",
            1 => r#"{"level":"x"}"#,
            _ => r#"{"level":"y"}"#,
        };
        out.push(Line {
            fp: FP_B,
            ts: t + 400_000_000,
            len: 5,
            sm,
        });
        if s % 10 == 0 {
            out.push(Line {
                fp: FP_B,
                ts: t,
                len: 2,
                sm,
            });
            out.push(Line {
                fp: FP_B,
                ts: t,
                len: 0,
                sm: "",
            });
        }
        if s % 2 == 0 {
            out.push(Line {
                fp: FP_C,
                ts: t,
                len: 4,
                sm: r#"{"env":"p"}"#,
            });
        }
    }
    out
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

async fn setup(db: &str, base: i64) -> ChClient {
    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {db}")).await;
    run_init(&admin, &RenderCtx::for_tests(db, 7))
        .await
        .expect("run_init");
    let client = ChClient::new(ChConnConfig {
        database: db.to_string(),
        ..test_config()
    })
    .await
    .expect("connect (db)");
    let mut rows = Vec::new();
    for fp in [FP_A, FP_B, FP_C] {
        let (json, _) = stream_labels(fp);
        rows.push(format!(
            "(0, 1, 'svc', {fp}, 0, '', '', toStartOfMonth(fromUnixTimestamp64Nano(toInt64({base}))), '{json}', 0)"
        ));
    }
    for l in lines(base) {
        rows.push(format!(
            "(0, 0, 'svc', {}, {}, '{}', '{}', toDate(0), '', 0)",
            l.fp,
            l.ts,
            "b".repeat(l.len),
            l.sm
        ));
    }
    exec(
        &client,
        &format!(
            "INSERT INTO log_landing (received_ms, kind, service, fingerprint, timestamp_ns, \
             body, structured_metadata, month, labels, updated_ns) VALUES {}",
            rows.join(", ")
        ),
    )
    .await;
    client
}

fn engine(client: ChClient, db: &str, rollup_res_ns: u64) -> LogQlEngine {
    LogQlEngine::new(
        client,
        EngineConfig {
            skip_unavailable_shards: false,
            read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
            db: db.to_string(),
            streams_idx: "log_streams_idx".to_string(),
            streams: "log_streams".to_string(),
            samples: "log_samples".to_string(),
            rollup_table: "log_metrics_5s".to_string(),
            patterns_table: "log_patterns".to_string(),
            rollup_res_ns,
            scan_budget_bytes: 50 * 1024 * 1024 * 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
            distributed: false,
        },
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    )
}

/// What one case asks and how its expectation is computed.
struct Case {
    query: &'static str,
    /// Selected streams.
    fps: &'static [u64],
    /// Range, step and offset in seconds; the grid start as an offset from
    /// the fixture base, in nanoseconds.
    range_s: i64,
    step_s: i64,
    offset_s: i64,
    start_off_ns: i64,
    /// Bytes rather than lines; divided by the range in seconds.
    bytes: bool,
    rate: bool,
    /// The output series a line counts toward.
    group: Group,
    route: RouteChoice,
    /// The routing reason, where the case pins one (issue #614: a fallback
    /// names its own cause).
    reason: Option<&'static str>,
}

/// How a case's lines form its output series.
#[derive(Clone, Copy)]
enum Group {
    /// `sum`: one series with no labels.
    Sum,
    /// `sum by (env)`: the stream's `env`, else the line's metadata `env`.
    ByEnv,
    /// `sum without (app)`: the stream labels and the metadata, less `app`.
    WithoutApp,
    /// No aggregation: the stream labels and the metadata.
    Series,
}

/// A label set rendered as the comparison key: `k=v` pairs, sorted, joined.
fn render(mut labels: Vec<(String, String)>) -> String {
    labels.sort();
    labels
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// A line's stream labels merged with its metadata (no key collides in this
/// fixture, so no `_extracted` rename arises).
fn line_labels(l: &Line) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = stream_labels(l.fp)
        .1
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    for (k, v) in [("level", "x"), ("level", "y"), ("env", "p")] {
        if l.sm.contains(&format!("\"{k}\":\"{v}\"")) {
            out.push((k.to_string(), v.to_string()));
        }
    }
    out
}

/// The answer from the fixture's definition: per grid point `t` and group,
/// the lines (or bytes) in `(t - offset - range, t - offset]`; a point only
/// where the window holds a line.
fn expected(case: &Case, base: i64) -> BTreeMap<String, BTreeMap<i64, f64>> {
    let all = lines(base);
    let start = base + case.start_off_ns;
    let end = base + SPAN_S * SEC;
    let mut out: BTreeMap<String, BTreeMap<i64, f64>> = BTreeMap::new();
    let mut t = start;
    while t <= end {
        let hi = t - case.offset_s * SEC;
        let lo = hi - case.range_s * SEC;
        let mut groups: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        for l in all
            .iter()
            .filter(|l| case.fps.contains(&l.fp) && l.ts > lo && l.ts <= hi)
        {
            let labels = line_labels(l);
            let key = match case.group {
                Group::Sum => String::new(),
                Group::ByEnv => render(labels.into_iter().filter(|(k, _)| k == "env").collect()),
                Group::WithoutApp => {
                    render(labels.into_iter().filter(|(k, _)| k != "app").collect())
                }
                Group::Series => render(labels),
            };
            let g = groups.entry(key).or_default();
            g.0 += 1;
            g.1 += l.len as u64;
        }
        for (key, (n, b)) in groups {
            let v = if case.bytes { b } else { n } as f64;
            let v = if case.rate {
                v / case.range_s as f64
            } else {
                v
            };
            out.entry(key).or_default().insert(t, v);
        }
        t += case.step_s * SEC;
    }
    out
}

fn as_groups(result: QueryResult) -> BTreeMap<String, BTreeMap<i64, f64>> {
    let QueryResult::Matrix(series) = result else {
        panic!("expected a matrix");
    };
    series
        .into_iter()
        .map(|s| (render(s.labels), s.points.into_iter().collect()))
        .collect()
}

fn cases() -> Vec<Case> {
    let base = |query, fps, range_s, step_s, bytes, rate, group, route| Case {
        query,
        fps,
        range_s,
        step_s,
        offset_s: 0,
        start_off_ns: 60 * SEC,
        bytes,
        rate,
        group,
        route,
        reason: None,
    };
    let a: &[u64] = &[FP_A, FP_B];
    let abc: &[u64] = &[FP_A, FP_B, FP_C];
    vec![
        base(
            r#"sum(count_over_time({app="a"}[1m]))"#,
            a,
            60,
            15,
            false,
            false,
            Group::Sum,
            RouteChoice::Rollup,
        ),
        base(
            r#"sum by (env) (rate({app="a"}[5m]))"#,
            a,
            300,
            60,
            false,
            true,
            Group::ByEnv,
            RouteChoice::Rollup,
        ),
        base(
            r#"sum(bytes_over_time({app="a"}[15s]))"#,
            a,
            15,
            15,
            true,
            false,
            Group::Sum,
            RouteChoice::Rollup,
        ),
        Case {
            offset_s: 10,
            ..base(
                r#"sum(bytes_rate({app="a"}[1m] offset 10s))"#,
                a,
                60,
                30,
                true,
                true,
                Group::Sum,
                RouteChoice::Rollup,
            )
        },
        Case {
            reason: Some(LABEL_REASON),
            ..base(
                r#"sum by (env) (count_over_time({app=~"a|b"}[1m]))"#,
                abc,
                60,
                15,
                false,
                false,
                Group::ByEnv,
                RouteChoice::Raw,
            )
        },
        Case {
            start_off_ns: 60 * SEC + 1,
            ..base(
                r#"sum(count_over_time({app="a"}[1m]))"#,
                a,
                60,
                15,
                false,
                false,
                Group::Sum,
                RouteChoice::Raw,
            )
        },
        base(
            r#"count_over_time({app="a"}[1m])"#,
            a,
            60,
            15,
            false,
            false,
            Group::Series,
            RouteChoice::Raw,
        ),
        base(
            r#"sum without (app) (count_over_time({app="a"}[1m]))"#,
            a,
            60,
            15,
            false,
            false,
            Group::WithoutApp,
            RouteChoice::Raw,
        ),
    ]
}

#[tokio::test]
async fn every_rollup_answer_equals_the_line_count_and_the_raw_route() {
    if !pulsus_testkit::live_clickhouse_enabled() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_read_it_rollup_route");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64;
    let base = (now - 3600) / 300 * 300 * SEC;
    let _ = setup(db, base).await;
    let connect = || async {
        ChClient::new(ChConnConfig {
            database: db.to_string(),
            ..test_config()
        })
        .await
        .expect("connect (engine)")
    };
    let rollup = engine(connect().await, db, RES_NS as u64);
    let raw = engine(connect().await, db, 0);
    let mut failures = Vec::new();
    for case in cases() {
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: base + case.start_off_ns,
                end_ns: base + SPAN_S * SEC,
                step_ns: (case.step_s * SEC) as u64,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let expr = parse(case.query).expect("parse");
        let (got, _, explain) = rollup
            .query_explained(&expr, &params)
            .await
            .expect("rollup engine");
        let (raw_got, _) = raw.query(&expr, &params).await.expect("raw engine");
        let want = expected(&case, base);
        let route = explain.routing.as_ref().map(|r| r.chosen);
        let reason = explain.routing.as_ref().map(|r| r.reason.as_str());
        if case.reason.is_some() && reason != case.reason {
            failures.push(format!("{}: reason {reason:?}", case.query));
        }
        let (got, raw_got) = (as_groups(got), as_groups(raw_got));
        if want.is_empty() || want.values().all(BTreeMap::is_empty) {
            failures.push(format!("{}: the expectation is empty", case.query));
        }
        if got != want || raw_got != want || route != Some(case.route) {
            failures.push(format!(
                "{}: route {route:?} (want {:?}); engine == want: {}; raw == want: {}",
                case.query,
                case.route,
                got == want,
                raw_got == want
            ));
        }
    }
    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {db}")).await;
    assert_eq!(failures, Vec::<String>::new());
}

/// Issue #614, part 2: a rollup statement the server refuses for memory is
/// answered from the lines, through the raw plan and the samples table.
#[tokio::test]
async fn a_rollup_statement_out_of_memory_is_answered_from_the_lines() {
    if !pulsus_testkit::live_clickhouse_enabled() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_read_it_rollup_memory");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64;
    let base = (now - 3600) / 300 * 300 * SEC;
    let _ = setup(db, base).await;
    let client = ChClient::new(ChConnConfig {
        database: db.to_string(),
        ..test_config()
    })
    .await
    .expect("connect (engine)");
    let rollup = engine(client, db, RES_NS as u64).with_key_route_test_hooks(
        pulsus_read::logql::exec::KeyRouteTestHooks {
            bucketed_statement_max_memory_bytes: Some(1),
            ..Default::default()
        },
    );
    let case = &cases()[0];
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: base + case.start_off_ns,
            end_ns: base + SPAN_S * SEC,
            step_ns: (case.step_s * SEC) as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let got = rollup
        .query_explained(&parse(case.query).expect("parse"), &params)
        .await
        .map(|(r, _, explain)| {
            let tried = explain
                .stages
                .iter()
                .any(|s| s.name == "metric_read" && s.sql.contains("FROM log_metrics_5s"));
            (
                tried,
                last_read_table(&explain),
                routing_of(&explain),
                as_groups(r),
            )
        });
    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {db}")).await;
    assert_eq!(
        got.map_err(|e| e.to_string()),
        Ok((
            true,
            Some("log_samples"),
            Some((RouteChoice::Raw, MEMORY_REASON.to_string())),
            expected(case, base)
        )),
        "(the rollup statement was issued, the fallback's table, the route reported, the answer)"
    );
}

/// The table the explain's last `metric_read` stage reads: the fallback's,
/// when there is one.
fn last_read_table(explain: &pulsus_read::logql::PlanExplain) -> Option<&'static str> {
    let sql = &explain
        .stages
        .iter()
        .rev()
        .find(|s| s.name == "metric_read")?
        .sql;
    match (
        sql.contains("FROM log_samples"),
        sql.contains("FROM log_metrics_5s"),
    ) {
        (true, false) => Some("log_samples"),
        (false, true) => Some("log_metrics_5s"),
        _ => None,
    }
}

/// The routing an explain reports.
fn routing_of(explain: &pulsus_read::logql::PlanExplain) -> Option<(RouteChoice, String)> {
    explain
        .routing
        .as_ref()
        .map(|r| (r.chosen, r.reason.clone()))
}

/// Issue #614, part 2: a grid the planner routes to the rollup but the
/// statement builder refuses (a 5e18 ns step overflows the cover's widest
/// numerator) is answered from the lines and reported as the raw route with
/// the refusal as its cause.
#[tokio::test]
async fn a_rollup_grid_the_statement_cannot_render_is_answered_from_the_lines() {
    if !pulsus_testkit::live_clickhouse_enabled() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_read_it_rollup_refused");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64;
    let base = (now - 3600) / 300 * 300 * SEC;
    let _ = setup(db, base).await;
    let client = ChClient::new(ChConnConfig {
        database: db.to_string(),
        ..test_config()
    })
    .await
    .expect("connect (engine)");
    let rollup = engine(client, db, RES_NS as u64);
    let case = Case {
        step_s: 5_000_000_000,
        ..cases().swap_remove(0)
    };
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: base + case.start_off_ns,
            end_ns: base + SPAN_S * SEC,
            step_ns: (case.step_s * SEC) as u64,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let expr = parse(case.query).expect("parse");
    let planned = rollup
        .explain(&expr, &params)
        .await
        .map(|e| (last_read_table(&e), routing_of(&e)));
    let got = rollup
        .query_explained(&expr, &params)
        .await
        .map(|(r, _, explain)| {
            (
                last_read_table(&explain),
                routing_of(&explain),
                as_groups(r),
            )
        });
    let admin = ChClient::new(test_config()).await.expect("connect");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {db}")).await;
    let refused = Some((RouteChoice::Raw, REFUSED_REASON.to_string()));
    assert_eq!(
        (
            planned.map_err(|e| e.to_string()),
            got.map_err(|e| e.to_string())
        ),
        (
            Ok((Some("log_samples"), refused.clone())),
            Ok((Some("log_samples"), refused, expected(&case, base)))
        ),
        "(the EXPLAIN twin's fallback table and route, then the executed fallback's table, route \
         and answer)"
    );
}
