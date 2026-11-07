//! Issue #793: a matcher that matches the empty string selects streams
//! without its label, on every read that takes a selector.
//!
//! A stream without label `k` reads as `k=""`. Six streams of one tenant,
//! each with `service_name="s"`; stream `fp` has `n` lines `fp=<fp>`, and the
//! line counts are powers of two, so every sum names exactly one set of
//! streams:
//!
//! ```text
//!   fp  labels besides service_name             n
//!   1   host=h1, unit=a.service                 1
//!   2   host=h1, unit=b.service                 2
//!   3   host=h1, namespace=ns1                  4
//!   4   host=h1, namespace=ns2                  8
//!   5   host=h2, namespace=ns1, unit=c.service  16
//!   6   app=other                               32
//! ```
//!
//! Every expected value follows from that rule applied to this table.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test live_logs_empty_matcher
//! ```

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_logql::{Expr, LogExpr, parse, parse_selector};
use pulsus_model::Tenant;
use pulsus_read::logql::{
    Direction, EngineConfig, LogQlEngine, QueryParams, QueryResult, QuerySpec, ReadError,
    RouteChoice, TailLower, TimeBounds, VolumeAggregateBy, VolumeEntry, VolumeQuery,
};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const SEC_NS: i64 = 1_000_000_000;
const MINUTE_NS: i64 = 60 * SEC_NS;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn conn(database: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

async fn execute(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

fn engine_config(db: &str) -> EngineConfig {
    EngineConfig {
        skip_unavailable_shards: false,
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        streams_idx: "log_streams_idx".to_string(),
        streams: "log_streams".to_string(),
        samples: "log_samples".to_string(),
        rollup_table: "log_metrics_5s".to_string(),
        patterns_table: "log_patterns".to_string(),
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
        distributed: false,
    }
}

fn tenant(name: &str) -> Tenant {
    Tenant::from_header(
        Some(&http::HeaderValue::from_str(name).expect("a value")),
        false,
    )
    .expect("a valid tenant")
}

fn selector(src: &str) -> Expr {
    Expr::Log(LogExpr {
        selector: parse_selector(src).expect("parse selector"),
        pipeline: Vec::new(),
    })
}

/// The label set of stream `fp`, as `labels_json` renders it.
fn labels_of(fp: u32) -> String {
    match fp {
        1 => r#"{"host":"h1","service_name":"s","unit":"a.service"}"#,
        2 => r#"{"host":"h1","service_name":"s","unit":"b.service"}"#,
        3 => r#"{"host":"h1","namespace":"ns1","service_name":"s"}"#,
        4 => r#"{"host":"h1","namespace":"ns2","service_name":"s"}"#,
        5 => r#"{"host":"h2","namespace":"ns1","service_name":"s","unit":"c.service"}"#,
        6 => r#"{"app":"other","service_name":"s"}"#,
        _ => unreachable!("no stream {fp}"),
    }
    .to_string()
}

fn labels_of_all(fps: &[u32]) -> Vec<String> {
    let mut out: Vec<String> = fps.iter().map(|fp| labels_of(*fp)).collect();
    out.sort();
    out
}

/// An instant query's sample values at `at_ns`.
async fn instant(engine: &LogQlEngine, src: &str, at_ns: i64) -> Vec<f64> {
    let params = QueryParams {
        spec: QuerySpec::Instant { at_ns },
        limit: 100,
        direction: Direction::Backward,
    };
    let (result, _) = engine
        .query(&parse(src).expect("parse"), &params)
        .await
        .unwrap_or_else(|e| panic!("{src}: {e}"));
    let QueryResult::Vector(samples) = result else {
        panic!("{src}: a vector result");
    };
    samples.into_iter().map(|s| s.value).collect()
}

/// A range log query over `b`, as the streams' `labels_json`, sorted.
async fn stream_labels(
    engine: &LogQlEngine,
    src: &str,
    b: TimeBounds,
) -> Result<Vec<String>, ReadError> {
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: b.start_ns,
            end_ns: b.end_ns,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let (result, _) = engine.query(&selector(src), &params).await?;
    let QueryResult::Streams { items, .. } = result else {
        panic!("{src}: a streams result");
    };
    let mut out: Vec<String> = items.into_iter().map(|s| s.labels_json).collect();
    out.sort();
    Ok(out)
}

/// **Issue #793: every read that takes a selector reads a missing label as
/// empty.** One insert, one engine, one assertion per row of the plan's
/// table.
#[tokio::test]
async fn a_matcher_matching_empty_selects_streams_without_its_label() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_read_it_log_empty_matcher");
    let bootstrap = ChClient::new(conn("default")).await.expect("connect");
    execute(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
    run_init(&bootstrap, &RenderCtx::for_tests(db, 7))
        .await
        .expect("run_init");
    let client = ChClient::new(conn(db)).await.expect("connect");

    let now_ns = chrono::Utc::now().timestamp_nanos_opt().expect("now");
    let t0 = (now_ns - 60 * MINUTE_NS) / MINUTE_NS * MINUTE_NS;
    let received_ms = now_ns / 1_000_000;
    let month = format!("toStartOfMonth(fromUnixTimestamp64Nano(toInt64({t0})))");
    let org = "t793";
    let mut rows = Vec::new();
    for (fp, n) in [(1u32, 1i64), (2, 2), (3, 4), (4, 8), (5, 16), (6, 32)] {
        rows.push(format!(
            "('{org}', {received_ms}, 1, 's', {fp}, 0, '', {month}, '{}', {t0})",
            labels_of(fp)
        ));
        for i in 1..=n {
            rows.push(format!(
                "('{org}', {received_ms}, 0, 's', {fp}, {}, 'fp={fp}', toDate(0), '', 0)",
                t0 + i * SEC_NS
            ));
        }
    }
    execute(
        &client,
        &format!(
            "INSERT INTO log_landing (org_id, received_ms, kind, service, fingerprint, \
             timestamp_ns, body, month, labels, updated_ns) VALUES {}",
            rows.join(", ")
        ),
    )
    .await;

    let b = TimeBounds {
        start_ns: t0 - 30 * MINUTE_NS,
        end_ns: t0 + 30 * MINUTE_NS,
    };
    let t = t0 + 30 * MINUTE_NS;
    let engine = LogQlEngine::new(
        ChClient::new(conn(db)).await.expect("connect"),
        engine_config(db),
        tenant(org),
    );
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let count = |sel: &str| format!("sum(count_over_time({sel}[1h]))");

    // L1-L6, L18: instant counts.
    assert_eq!(
        instant(&engine, &count(r#"{host=~".+"}"#), t).await,
        vec![31.0],
        "L1"
    );
    assert_eq!(
        instant(&engine, &count(r#"{host=~".+", unit=~".*"}"#), t).await,
        vec![31.0],
        "L2"
    );
    assert_eq!(
        instant(&engine, &count(r#"{host=~".+", unit=""}"#), t).await,
        vec![12.0],
        "L3"
    );
    assert_eq!(
        instant(&engine, &count(r#"{host=~".+", foo=~".*"}"#), t).await,
        vec![31.0],
        "L4"
    );
    assert_eq!(
        instant(&engine, &count(r#"{host=~".+", unit!~".+"}"#), t).await,
        vec![12.0],
        "L5"
    );
    assert_eq!(
        instant(&engine, &count(r#"{host=~".+", unit!=""}"#), t).await,
        vec![19.0],
        "L6"
    );
    assert_eq!(
        instant(&engine, &count(r#"{host=~".+", unit!~".*"}"#), t).await,
        Vec::<f64>::new(),
        "L18"
    );

    // L7: the rollup route reaches the same rule.
    {
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: t - MINUTE_NS,
                end_ns: t,
                step_ns: 60_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let q = count(r#"{host=~".+", unit=""}"#);
        let (result, _, explain) = engine
            .query_explained(&parse(&q).expect("parse"), &params)
            .await
            .unwrap_or_else(|e| panic!("L7: {e}"));
        assert_eq!(
            explain.routing.as_ref().map(|r| r.chosen),
            Some(RouteChoice::Rollup),
            "L7 precondition: the rollup route"
        );
        assert!(
            explain
                .stages
                .iter()
                .any(|s| s.name == "metric_read" && s.sql.contains("FROM log_metrics_5s")),
            "L7 precondition: a metric_read stage reads the rollup: {:?}",
            explain.stages
        );
        let QueryResult::Matrix(series) = result else {
            panic!("L7: a matrix result");
        };
        let points: Vec<Vec<(i64, f64)>> = series.into_iter().map(|s| s.points).collect();
        assert_eq!(points, vec![vec![(t - MINUTE_NS, 12.0), (t, 12.0)]], "L7");
    }

    // L8: a log query.
    assert_eq!(
        stream_labels(&engine, r#"{host=~".+", unit=~".*"}"#, b)
            .await
            .expect("L8"),
        labels_of_all(&[1, 2, 3, 4, 5]),
        "L8"
    );

    // L9: series.
    {
        let mut got = engine
            .series(&[selector(r#"{host=~".+", unit=~".*"}"#)], b)
            .await
            .expect("L9");
        got.sort();
        assert_eq!(got, labels_of_all(&[1, 2, 3, 4, 5]), "L9");
    }

    let no_unit = selector(r#"{host=~".+", unit=""}"#);

    // L10, L11: label names and values.
    assert_eq!(
        engine
            .label_names(Some(&no_unit), b)
            .await
            .expect("L10")
            .items,
        s(&["host", "namespace", "service_name"]),
        "L10"
    );
    assert_eq!(
        engine
            .label_values("namespace", Some(&no_unit), b)
            .await
            .expect("L11")
            .items,
        s(&["ns1", "ns2"]),
        "L11"
    );

    // L12: volume.
    assert_eq!(
        engine
            .volume(
                &no_unit,
                &VolumeQuery {
                    bounds: b,
                    limit: 100,
                    aggregate_by: VolumeAggregateBy::Series,
                    target_labels: Vec::new(),
                },
            )
            .await
            .expect("L12"),
        vec![VolumeEntry {
            labels: vec![("host".to_string(), "h1".to_string())],
            bytes: 48,
        }],
        "L12"
    );

    // L13: stats (`chunks` counts days, so it is not asserted).
    {
        let st = engine.stats(&no_unit, b).await.expect("L13");
        assert_eq!((st.streams, st.entries, st.bytes), (2, 12, 48), "L13");
    }

    // L14: detected field values.
    {
        let mut got = engine
            .detected_field_values("fp", &no_unit, b, 1000, 100)
            .await
            .expect("L14")
            .values;
        got.sort();
        assert_eq!(got, s(&["3", "4"]), "L14");
    }

    // L15: the tail.
    {
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: t0,
                end_ns: t0 + MINUTE_NS,
                step_ns: 60_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let mut setup = engine
            .tail_setup(&selector(r#"{host=~".+", unit=~".*"}"#), &params)
            .expect("L15: a tail setup");
        let page = engine
            .tail_poll(
                &mut setup,
                TailLower::Start { start_ns: t0 },
                t0 + MINUTE_NS,
                100,
            )
            .await
            .expect("L15: a tail page");
        let mut got: Vec<String> = page.streams.into_iter().map(|s| s.labels_json).collect();
        got.sort();
        got.dedup();
        assert_eq!(got, labels_of_all(&[1, 2, 3, 4, 5]), "L15");
    }

    // L16, L17: the refusal rule.
    {
        let got = stream_labels(&engine, r#"{unit=~".*"}"#, b).await;
        assert!(
            matches!(got, Err(ReadError::EmptyMatcherSet)),
            "L16: expected EmptyMatcherSet, got {got:?}"
        );
    }
    assert_eq!(
        stream_labels(&engine, r#"{unit!=""}"#, b)
            .await
            .map_err(|e| e.to_string()),
        Ok(labels_of_all(&[1, 2, 5])),
        "L17"
    );

    execute(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
}
