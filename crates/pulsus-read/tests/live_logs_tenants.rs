//! Issue #653: every log-table read answers its own tenant, through the
//! engine, over rows the writer cannot produce.
//!
//! The writer never gives one fingerprint two label sets; these rows do, so
//! an unscoped stream-index or stream read would hand one tenant the other
//! tenant's labels and lines. `tenant-o` sorts before `tenant-q`, so in
//! `(org_id, fingerprint)` order a leaked `log_streams` row is read first,
//! and hydration keeps the first.
//!
//! ```text
//!   fingerprint  tenant-q                               tenant-o
//!   7            {app="web", service_name="web"}: w1    {app="api", service_name="api", zone="o"}: a1
//!   8            {app="old", service_name="old"}: q-old {app="old", service_name="old"}: o-old
//! ```
//!
//! `q-old` lies two days before the window; every other line inside it.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test live_logs_tenants
//! ```

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_logql::{Expr, LogExpr, parse_selector};
use pulsus_model::Tenant;
use pulsus_read::logql::{
    Direction, EngineConfig, LogQlEngine, QueryParams, QueryResult, QuerySpec, TimeBounds,
};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const MINUTE_NS: i64 = 60_000_000_000;
const DAY_NS: i64 = 1_440 * MINUTE_NS;

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

/// A range query's streams as `(labels, lines)`, sorted.
async fn streams(engine: &LogQlEngine, src: &str, b: TimeBounds) -> Vec<(String, Vec<String>)> {
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: b.start_ns,
            end_ns: b.end_ns,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let (result, _) = engine
        .query(&selector(src), &params)
        .await
        .unwrap_or_else(|e| panic!("{src}: {e}"));
    let QueryResult::Streams { items, .. } = result else {
        panic!("{src}: a streams result");
    };
    let mut out: Vec<(String, Vec<String>)> = items
        .into_iter()
        .map(|s| {
            (
                s.labels_json,
                s.entries.into_iter().map(|(_, line)| line).collect(),
            )
        })
        .collect();
    out.sort();
    out
}

/// **T-B (issue #653): every log-table read answers its own tenant.** §9.5's
/// rows in one insert; one engine per tenant; each case equals that
/// tenant's column.
#[tokio::test]
async fn every_log_table_read_answers_its_own_tenant() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_read_it_log_tenants");
    let bootstrap = ChClient::new(conn("default")).await.expect("connect");
    execute(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
    run_init(&bootstrap, &RenderCtx::for_tests(db))
        .await
        .expect("run_init");
    let client = ChClient::new(conn(db)).await.expect("connect");

    let now_ns = chrono::Utc::now().timestamp_nanos_opt().expect("now");
    let t0 = (now_ns - 60 * MINUTE_NS) / MINUTE_NS * MINUTE_NS;
    let received_ms = now_ns / 1_000_000;
    let month = format!("toStartOfMonth(fromUnixTimestamp64Nano(toInt64({t0})))");
    let stream = |org: &str, fp: u32, service: &str, labels: &str| {
        format!("('{org}', {received_ms}, 1, '{service}', {fp}, 0, '', {month}, '{labels}', {t0})")
    };
    let line = |org: &str, fp: u32, service: &str, at: i64, body: &str| {
        format!("('{org}', {received_ms}, 0, '{service}', {fp}, {at}, '{body}', toDate(0), '', 0)")
    };
    let rows = [
        stream(
            "tenant-q",
            7,
            "web",
            r#"{"app":"web","service_name":"web"}"#,
        ),
        line("tenant-q", 7, "web", t0 + MINUTE_NS, "w1"),
        stream(
            "tenant-o",
            7,
            "api",
            r#"{"app":"api","service_name":"api","zone":"o"}"#,
        ),
        line("tenant-o", 7, "api", t0 + 2 * MINUTE_NS, "a1"),
        stream(
            "tenant-q",
            8,
            "old",
            r#"{"app":"old","service_name":"old"}"#,
        ),
        line("tenant-q", 8, "old", t0 - 2 * DAY_NS, "q-old"),
        stream(
            "tenant-o",
            8,
            "old",
            r#"{"app":"old","service_name":"old"}"#,
        ),
        line("tenant-o", 8, "old", t0 + 3 * MINUTE_NS, "o-old"),
    ];
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
    let q = LogQlEngine::new(
        ChClient::new(conn(db)).await.expect("connect"),
        engine_config(db),
        tenant("tenant-q"),
    );
    let o = LogQlEngine::new(
        ChClient::new(conn(db)).await.expect("connect"),
        engine_config(db),
        tenant("tenant-o"),
    );
    let web = r#"{"app":"web","service_name":"web"}"#.to_string();
    let api = r#"{"app":"api","service_name":"api","zone":"o"}"#.to_string();
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();

    // B1, B2: the stream index, the stream table and the samples.
    assert_eq!(
        streams(&q, r#"{app="api"}"#, b).await,
        vec![],
        "B1 tenant-q"
    );
    assert_eq!(
        streams(&o, r#"{app="api"}"#, b).await,
        vec![(api.clone(), s(&["a1"]))],
        "B1 tenant-o"
    );
    assert_eq!(
        streams(&q, r#"{app="web"}"#, b).await,
        vec![(web.clone(), s(&["w1"]))],
        "B2 tenant-q"
    );
    assert_eq!(
        streams(&o, r#"{app="web"}"#, b).await,
        vec![],
        "B2 tenant-o"
    );

    // B3: label names.
    assert_eq!(
        q.label_names(None, b).await.expect("B3 tenant-q"),
        s(&["app", "service_name"]),
        "B3 tenant-q"
    );
    assert_eq!(
        o.label_names(None, b).await.expect("B3 tenant-o"),
        s(&["app", "service_name", "zone"]),
        "B3 tenant-o"
    );

    // B4: label values of `zone`.
    assert_eq!(
        q.label_values("zone", None, b).await.expect("B4 tenant-q"),
        Vec::<String>::new(),
        "B4 tenant-q"
    );
    assert_eq!(
        o.label_values("zone", None, b).await.expect("B4 tenant-o"),
        s(&["o"]),
        "B4 tenant-o"
    );

    // B5: detected labels.
    let detected = |v: Vec<pulsus_read::logql::DetectedLabelOut>| {
        v.into_iter().map(|d| d.label).collect::<Vec<_>>()
    };
    let q5 = detected(q.detected_labels(None, b).await.expect("B5 tenant-q"));
    assert!(!q5.contains(&"zone".to_string()), "B5 tenant-q: {q5:?}");
    let o5 = detected(o.detected_labels(None, b).await.expect("B5 tenant-o"));
    assert!(o5.contains(&"zone".to_string()), "B5 tenant-o: {o5:?}");

    // B6: series.
    assert_eq!(
        q.series(&[selector(r#"{app="web"}"#)], b)
            .await
            .expect("B6 tenant-q"),
        vec![web.clone()],
        "B6 tenant-q"
    );
    assert_eq!(
        o.series(&[selector(r#"{app="web"}"#)], b)
            .await
            .expect("B6 tenant-o"),
        Vec::<String>::new(),
        "B6 tenant-o"
    );

    // B7: label values of `app`, over each tenant's own activity.
    assert_eq!(
        q.label_values("app", None, b).await.expect("B7 tenant-q"),
        s(&["web"]),
        "B7 tenant-q"
    );
    assert_eq!(
        o.label_values("app", None, b).await.expect("B7 tenant-o"),
        s(&["api", "old"]),
        "B7 tenant-o"
    );

    execute(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
}
