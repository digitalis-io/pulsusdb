//! Issue #635 part 2: the inverted label index, against a live ClickHouse.
//!
//! Each test creates a database with `run_init`, writes kind-2 rows through
//! `metric_landing` so the views fill every table as a push does, and asks
//! [`MetricsEngine`]'s discovery reads. The expected answers are computed
//! here, from the generator's own label columns — never from the lookup's
//! JSON or from the index — and `system.query_log` says which statements
//! read the index.
//!
//! The corpus is the design's (§6) at 2,000 series: `az`, `env` (absent 5%,
//! present with an empty value 5%), `instance`, `job`, `pod` (40 values),
//! `status` on every fourth metric, `zone`. Series with `s % 10 = 3` are
//! active only on the day before the window; seven `m_old` series there
//! carry a key, `old_key`, no other series has.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test live_metrics_label_index
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::{LabelMatcher, MatchOp};
use pulsus_read::metrics::matcher::{DataWindow, DiscoveryFilter};
use pulsus_read::{LabelCache, LabelCacheConfig, MetricsConfig, MetricsEngine};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const DAY_MS: i64 = 86_400_000;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
            return;
        }
    };
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
        pool_size: 8,
        query_timeout: Duration::from_secs(120),
        ..ChConnConfig::default()
    }
}

fn engine_config(db: &str) -> MetricsConfig {
    MetricsConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        samples_table: "metric_samples".to_string(),
        hist_samples_table: "metric_hist_samples".to_string(),
        series_table: "metric_series".to_string(),
        labels_table: "metric_labels".to_string(),
        label_index_table: "metric_label_index".to_string(),
        label_values_table: "metric_label_values".to_string(),
        metadata_table: "metric_metadata".to_string(),
        experimental_functions: false,
        max_metric_fanout: 1_000,
        max_cache_scan: 200_000,
        cache_max_series: 50_000,
        max_info_series: 100_000,
        max_samples: 50_000_000,
        distributed: false,
        grouped_push: true,
    }
}

fn cache_config(db: &str) -> LabelCacheConfig {
    LabelCacheConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        series_table: "metric_series".to_string(),
        labels_table: "metric_labels".to_string(),
        window_ms: 3 * DAY_MS,
        cache_max_series: 50_000,
        ttl: Duration::from_secs(3_600),
        staleness_multiplier: 3,
    }
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

async fn strings(client: &ChClient, sql: &str) -> Vec<String> {
    client
        .query_strings(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
}

fn date_text(day_start_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(day_start_ms)
        .expect("a day")
        .format("%Y-%m-%d")
        .to_string()
}

/// One generated series: its ID, name, the labels it stores (an empty
/// value included), and whether it is active in the window.
#[derive(Debug, Clone)]
struct Generated {
    metric_name: String,
    labels: Vec<(String, String)>,
    in_window: bool,
}

struct Fixture {
    admin: ChClient,
    bootstrap: ChClient,
    db: String,
    engine: MetricsEngine,
    window: DataWindow,
    series: Vec<Generated>,
}

/// The design's §6 corpus statement at 2,000 series, `pod` modulo 40, with
/// the window on yesterday and the out-of-window day the one before.
fn corpus_sql(window_day: &str, old_day: &str) -> String {
    format!(
        "CREATE TABLE gen ENGINE = Memory AS
SELECT s, metric_name, ks, vs,
       bitOr(bitShiftLeft(toUInt128(bitShiftRight(cityHash64(metric_name), 32)), 96),
             bitAnd(bitOr(bitShiftLeft(toUInt128(cityHash64(buf)), 64), toUInt128(xxHash64(buf))),
                    toUInt128('79228162514264337593543950335'))) AS fingerprint,
       toUnixTimestamp64Milli(toDateTime64(if(s % 10 = 3 OR metric_name = 'm_old', '{old_day} 12:00:00', '{window_day} 12:00:00'), 3, 'UTC')) AS unix_milli
FROM (
    SELECT s, metric_name, ks, vs,
           concat(metric_name, unhex('FF'), arrayStringConcat(arrayMap((k, v) -> concat(k, unhex('FF'), v, unhex('FF')), ks, vs), '')) AS buf
    FROM (
        SELECT number AS s,
               concat('m_', toString(intDiv(s, 1000))) AS metric_name,
               cityHash64(s, 'j') % 100 AS rj,
               cityHash64(s, 'st') % 100 AS rs,
               cityHash64(s, 'e') % 10 AS re,
               [['eu-1a', 'eu-1b', 'eu-1c'][s % 3 + 1],
                multiIf(re < 6, 'prod', re < 8, 'staging', re < 9, 'dev', s % 2 = 0, '', '~'),
                concat('host-', toString(cityHash64(s, 'i') % 5000)),
                multiIf(rj < 40, 'api', rj < 80, concat('api-', toString(rj % 20)), rj < 90, 'db', rj < 95, 'cache', 'worker'),
                concat('pod-', toString(cityHash64(s, 'p') % 40)),
                if(intDiv(s, 1000) % 4 = 0,
                   multiIf(rs < 60, '200', rs < 70, '201', rs < 75, '204', rs < 78, '301', rs < 80, '304', rs < 85, '400',
                           rs < 88, '401', rs < 90, '403', rs < 95, '404', rs < 97, '500', rs < 99, '502', '503'), '~'),
                concat('z', leftPad(toString(cityHash64(s, 'z') % 100), 2, '0'))] AS all_vs,
               arrayFilter((k, v) -> v != '~', ['az', 'env', 'instance', 'job', 'pod', 'status', 'zone'], all_vs) AS ks,
               arrayFilter(v -> v != '~', all_vs) AS vs
        FROM numbers(2000)
        UNION ALL
        SELECT 100000000 + number AS s, 'm_old', 0, 0, 0, [], ['job', 'old_key', 'status', 'zone'],
               ['api', concat('x', toString(number)), '503', 'z07']
        FROM numbers(7)
    )
)"
    )
}

const LANDING_FROM_GEN: &str = "INSERT INTO metric_landing (received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type)
SELECT toUnixTimestamp64Milli(now64(3)), 2, metric_name, fingerprint, unix_milli,
       concat('{', arrayStringConcat(arrayMap((k, v) -> concat('\"', k, '\":\"', v, '\"'), ks, vs), ','), '}'),
       0
FROM gen";

/// `db` is composed by `pulsus_testkit::test_db` at the call site, where
/// the naming guards read it.
async fn fixture(db: &str, extra_landing: &[&str]) -> Fixture {
    let db = db.to_string();
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
    run_init(&bootstrap, &RenderCtx::for_tests(&db))
        .await
        .expect("run_init");
    let admin = ChClient::new(test_config(&db)).await.expect("connect");
    let now = chrono::Utc::now().timestamp_millis();
    let window_start = (now.div_euclid(DAY_MS) - 1) * DAY_MS;
    let window_day = date_text(window_start);
    let old_day = date_text(window_start - DAY_MS);
    exec(&admin, &corpus_sql(&window_day, &old_day)).await;
    exec(&admin, LANDING_FROM_GEN).await;
    for stmt in extra_landing {
        exec(
            &admin,
            &stmt
                .replace("{window_ms}", &(window_start + 12 * 3_600_000).to_string())
                .replace("{received}", &now.to_string()),
        )
        .await;
    }
    let series = strings(
        &admin,
        &format!(
            "SELECT concat(metric_name, '\\t', toString(unix_milli >= {window_start}), '\\t', \
             arrayStringConcat(arrayMap((k, v) -> concat(k, '=', v), ks, vs), '\\x01')) AS s FROM gen"
        ),
    )
    .await
    .into_iter()
    .map(|line| {
        let mut parts = line.splitn(3, '\t');
        let metric_name = parts.next().expect("name").to_string();
        let in_window = parts.next().expect("flag") == "1";
        let labels = parts
            .next()
            .unwrap_or("")
            .split('\x01')
            .filter(|p| !p.is_empty())
            .map(|p| {
                let (k, v) = p.split_once('=').expect("k=v");
                (k.to_string(), v.to_string())
            })
            .collect();
        Generated {
            metric_name,
            labels,
            in_window,
        }
    })
    .collect();
    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    let engine = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache,
        engine_config(&db),
    );
    Fixture {
        admin,
        bootstrap,
        db,
        engine,
        window: DataWindow {
            start_ms: window_start,
            end_ms: window_start + DAY_MS - 1,
        },
        series,
    }
}

impl Fixture {
    async fn finish(self) {
        exec(
            &self.bootstrap,
            &format!("DROP DATABASE IF EXISTS {}", self.db),
        )
        .await;
    }

    async fn marker(&self) -> String {
        strings(&self.admin, "SELECT toString(now64(6)) AS s")
            .await
            .remove(0)
    }

    /// The tables every statement since `marker` read, one entry per
    /// statement, waiting for the log to settle.
    async fn tables_since(&self, marker: &str) -> Vec<String> {
        let sql = format!(
            "SELECT arrayStringConcat(tables, ',') AS s FROM system.query_log \
             WHERE current_database = '{}' AND type = 'QueryFinish' AND query_kind = 'Select' \
               AND query_start_time_microseconds >= toDateTime64('{marker}', 6) \
               AND query NOT LIKE '%system.query_log%' AND query NOT LIKE '%now64(6)%' \
             ORDER BY query_start_time_microseconds",
            self.db
        );
        let mut previous: Option<usize> = None;
        for _ in 0..30 {
            exec(&self.admin, "SYSTEM FLUSH LOGS").await;
            let rows = strings(&self.admin, &sql).await;
            if previous == Some(rows.len()) {
                return rows;
            }
            previous = Some(rows.len());
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!("the query log never settled");
    }

    fn reads_index(&self, tables: &[String]) -> bool {
        tables
            .iter()
            .any(|t| t.contains(&format!("{}.metric_label_index", self.db)))
    }
}

fn m(key: &str, op: MatchOp, value: &str) -> LabelMatcher {
    LabelMatcher {
        key: key.to_string(),
        op,
        value: value.to_string(),
    }
}

/// The expectation: whether `labels` satisfy every matcher, reading an
/// absent key and a stored empty value both as `""`.
fn satisfies(labels: &[(String, String)], matchers: &[LabelMatcher]) -> bool {
    matchers.iter().all(|mt| {
        let v = labels
            .iter()
            .find(|(k, _)| *k == mt.key)
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
        let re = || regex::Regex::new(&format!("^(?:{})$", mt.value)).expect("a test regex");
        match mt.op {
            MatchOp::Eq => v == mt.value,
            MatchOp::Neq => v != mt.value,
            MatchOp::Re => re().is_match(v),
            MatchOp::Nre => !re().is_match(v),
        }
    })
}

fn expected_series(fx: &Fixture, matchers: &[LabelMatcher]) -> Vec<Vec<(String, String)>> {
    let mut out: Vec<Vec<(String, String)>> = fx
        .series
        .iter()
        .filter(|g| g.in_window && satisfies(&g.labels, matchers))
        .map(|g| {
            let mut pairs = g.labels.clone();
            pairs.push(("__name__".to_string(), g.metric_name.clone()));
            pairs.sort();
            pairs
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

fn filter(matchers: Vec<LabelMatcher>) -> DiscoveryFilter {
    DiscoveryFilter {
        metric_name: None,
        name_matchers: Vec::new(),
        matchers,
    }
}

/// S1-S8, C1-C4 and E1-E4 of the design's §6.
fn selectors() -> Vec<(&'static str, Vec<LabelMatcher>)> {
    use MatchOp::{Eq, Neq, Nre, Re};
    vec![
        ("S1", vec![m("job", Eq, "api"), m("status", Re, "5..")]),
        (
            "S2",
            vec![
                m("job", Re, "api.*"),
                m("zone", Eq, "z07"),
                m("env", Neq, "dev"),
            ],
        ),
        ("S3", vec![m("zone", Eq, "z07"), m("status", Nre, "2..")]),
        ("S4", vec![m("zone", Eq, "z07"), m("job", Neq, "api")]),
        ("S5", vec![m("job", Eq, "api")]),
        ("S6", vec![m("zone", Eq, "z07")]),
        ("S7", vec![m("status", Re, "(4|5)..")]),
        ("S8", vec![m("pod", Re, "pod-1.*7")]),
        ("C1", vec![m("zone", Eq, "z07"), m("env", Eq, "")]),
        ("C2", vec![m("env", Neq, "")]),
        ("C3", vec![m("zone", Eq, "z07"), m("env", Re, "prod|")]),
        ("C4", vec![m("env", Nre, "prod|")]),
        ("E1", vec![m("job", Eq, "api"), m("env", Eq, "")]),
        ("E2", vec![m("zone", Eq, "z07"), m("env", Neq, "")]),
        ("E3", vec![m("zone", Eq, "z07"), m("env", Nre, ".+")]),
        ("E4", vec![m("zone", Eq, "z07"), m("env", Re, ".+")]),
    ]
}

/// **V1: every selector of the design answers from the index, exactly.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v1_name_less_selectors_answer_from_the_index() {
    skip_unless_live!();
    let fx = fixture(
        &pulsus_testkit::test_db("pulsus_read_it_label_index_v1"),
        &[],
    )
    .await;
    for (id, matchers) in selectors() {
        let want = expected_series(&fx, &matchers);
        assert!(!want.is_empty(), "{id}: the corpus selects something");
        let marker = fx.marker().await;
        let got = fx
            .engine
            .series(&[filter(matchers.clone())], fx.window)
            .await
            .unwrap_or_else(|e| panic!("{id}: {e:?}"));
        assert_eq!(got.len(), want.len(), "{id}: series count");
        assert_eq!(got, want, "{id}: the label sets");
        let tables = fx.tables_since(&marker).await;
        assert!(
            fx.reads_index(&tables),
            "{id}: no statement read metric_label_index: {tables:?}"
        );
    }
    fx.finish().await;
}

/// **V4: a stored empty value reads as absent.** Two series of `job="t"`,
/// one storing `"env":""` and one without the key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v4_an_empty_value_reads_as_absent() {
    skip_unless_live!();
    let rows = [
        "INSERT INTO metric_landing (received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
         VALUES ({received}, 2, 'emp', 9001, {window_ms}, '{\"env\":\"\",\"job\":\"t\",\"x\":\"1\"}', 0), \
                ({received}, 2, 'emp', 9002, {window_ms}, '{\"job\":\"t\",\"x\":\"2\"}', 0)",
    ];
    let fx = fixture(
        &pulsus_testkit::test_db("pulsus_read_it_label_index_v4"),
        &rows,
    )
    .await;
    use MatchOp::{Eq, Neq, Nre, Re};
    for (matchers, want) in [
        (vec![m("job", Eq, "t"), m("env", Eq, "")], 2usize),
        (vec![m("job", Eq, "t"), m("env", Neq, "")], 0),
        (vec![m("job", Eq, "t"), m("env", Re, ".+")], 0),
        (vec![m("job", Eq, "t"), m("env", Nre, ".+")], 2),
    ] {
        let marker = fx.marker().await;
        let got = fx
            .engine
            .series(&[filter(matchers.clone())], fx.window)
            .await
            .unwrap_or_else(|e| panic!("{matchers:?}: {e:?}"));
        assert_eq!(got.len(), want, "{matchers:?}: {got:?}");
        let tables = fx.tables_since(&marker).await;
        assert!(fx.reads_index(&tables), "{matchers:?}: {tables:?}");
    }
    fx.finish().await;
}

/// **V2: the label endpoints answer from the index.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_the_label_endpoints_answer_from_the_index() {
    skip_unless_live!();
    let fx = fixture(
        &pulsus_testkit::test_db("pulsus_read_it_label_index_v2"),
        &[],
    )
    .await;
    let keys = |matchers: &[LabelMatcher]| -> Vec<String> {
        let mut set: BTreeSet<String> = fx
            .series
            .iter()
            .filter(|g| g.in_window && satisfies(&g.labels, matchers))
            .flat_map(|g| g.labels.iter().map(|(k, _)| k.clone()))
            .collect();
        set.insert("__name__".to_string());
        set.into_iter().collect()
    };
    let values = |key: &str, matchers: &[LabelMatcher]| -> Vec<String> {
        let set: BTreeSet<String> = fx
            .series
            .iter()
            .filter(|g| g.in_window && satisfies(&g.labels, matchers))
            .flat_map(|g| {
                g.labels
                    .iter()
                    .filter(|(k, _)| k == key)
                    .map(|(_, v)| v.clone())
            })
            .collect();
        set.into_iter().collect()
    };

    let zone = vec![m("zone", MatchOp::Eq, "z07")];
    for (filters, matchers) in [(Vec::new(), Vec::new()), (vec![filter(zone.clone())], zone)] {
        let marker = fx.marker().await;
        let got = fx
            .engine
            .label_names(&filters, fx.window)
            .await
            .expect("label names");
        assert_eq!(got, keys(&matchers), "{filters:?}");
        assert!(!got.contains(&"old_key".to_string()));
        assert!(
            fx.reads_index(&fx.tables_since(&marker).await),
            "{filters:?}"
        );
    }
    let s1 = vec![
        m("job", MatchOp::Eq, "api"),
        m("status", MatchOp::Re, "5.."),
    ];
    let cases: [(&str, Vec<LabelMatcher>); 4] = [
        ("job", Vec::new()),
        ("pod", Vec::new()),
        ("env", Vec::new()),
        ("instance", s1),
    ];
    for (key, matchers) in cases {
        let filters = if matchers.is_empty() {
            Vec::new()
        } else {
            vec![filter(matchers.clone())]
        };
        let marker = fx.marker().await;
        let got = fx
            .engine
            .label_values(key, &filters, fx.window)
            .await
            .expect("label values");
        assert_eq!(got, values(key, &matchers), "{key}");
        if key == "env" {
            assert!(got.contains(&String::new()), "env lists the empty value");
        }
        assert!(fx.reads_index(&fx.tables_since(&marker).await), "{key}");
    }
    fx.finish().await;
}

/// **V3: the view decodes escaped JSON.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v3_escaped_labels_round_trip() {
    skip_unless_live!();
    let rows = [
        "INSERT INTO metric_landing (received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
         VALUES ({received}, 2, 'esc', 9100, {window_ms}, '{\"k\":\"a\\\\\"b\\\\\\\\c\",\"n\":\"line\\\\nx\",\"u\":\"é\"}', 0)",
    ];
    let fx = fixture(
        &pulsus_testkit::test_db("pulsus_read_it_label_index_v3"),
        &rows,
    )
    .await;
    let marker = fx.marker().await;
    assert_eq!(
        fx.engine
            .label_values("k", &[], fx.window)
            .await
            .expect("k values"),
        vec!["a\"b\\c".to_string()]
    );
    assert_eq!(
        fx.engine
            .label_values("n", &[], fx.window)
            .await
            .expect("n values"),
        vec!["line\nx".to_string()]
    );
    let got = fx
        .engine
        .series(&[filter(vec![m("k", MatchOp::Eq, "a\"b\\c")])], fx.window)
        .await
        .expect("series");
    let names: BTreeMap<String, String> =
        got.first().expect("one series").iter().cloned().collect();
    assert_eq!(names.get("__name__").map(String::as_str), Some("esc"));
    assert_eq!(names.get("u").map(String::as_str), Some("é"));
    assert!(fx.reads_index(&fx.tables_since(&marker).await));
    fx.finish().await;
}
