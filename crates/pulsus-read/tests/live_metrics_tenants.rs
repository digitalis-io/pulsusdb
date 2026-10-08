//! Issue #635 part 4: every metrics read answers its own tenant, against a
//! live ClickHouse.
//!
//! Two tenants, `tenant-q` and `tenant-o`, hold the same series IDs with
//! different names, labels, samples and descriptors (the design's §9 data).
//! The rows are written through `metric_landing` with their `org_id`, so
//! the views fill every table as a push does; the writer cannot make two
//! tenants share an ID, so they are inserted directly. `tenant-o` sorts
//! before `tenant-q`, so a leaked row reaches an `any()` first.
//!
//! Every expected answer is the design's case table, written out here.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test live_metrics_tenants
//! ```

#[path = "pushed_rate_corpus/mod.rs"]
mod pushed_rate_corpus;

use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};
use pulsus_model::{LabelMatcher, MatchOp, Tenant};
use pulsus_promql::parser::parse;
use pulsus_read::metrics::matcher::{DataWindow, DiscoveryFilter};
use pulsus_read::{
    HistOrFloat, LabelCache, LabelCacheConfig, MetricQueryParams, MetricsConfig, MetricsEngine,
    QueryResult,
};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;
use pushed_rate_corpus::{drop_database, server_marker, statements_since, test_config};

const MINUTE_MS: i64 = 60_000;
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

fn tenant(name: &str) -> Tenant {
    let header = http::HeaderValue::from_str(name).expect("a header value");
    Tenant::from_header(Some(&header), false).expect("a valid tenant")
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
        window_ms: DAY_MS,
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

/// The design's §9 rows as landing rows, offsets from `t0`. One kind-2 row
/// per series (tenant-q's ID 45 only two days back), the float samples as
/// kind-0 rows, the histograms as kind-1 rows with one bucket holding the
/// count, and the three descriptors as kind-3 rows.
fn data_sql(t0: i64) -> Vec<String> {
    let m = |minutes: i64| t0 + minutes * MINUTE_MS;
    let back = t0 - 2 * DAY_MS;
    let series = [
        ("tenant-q", "m_x", 42, t0, r#"{"job":"api","status":"200"}"#),
        ("tenant-q", "m_x", 43, t0, r#"{"job":"api","status":"500"}"#),
        (
            "tenant-q",
            "m_x",
            45,
            back,
            r#"{"job":"api","status":"501"}"#,
        ),
        ("tenant-q", "m_x", 46, t0, r#"{"job":"web","status":"500"}"#),
        ("tenant-q", "h_x", 47, t0, r#"{"job":"api","status":"500"}"#),
        ("tenant-q", "m_w", 49, t0, r#"{"job":"api","status":"500"}"#),
        ("tenant-q", "m_x", 50, t0, r#"{"job":"api","status":"404"}"#),
        ("tenant-o", "m_x", 42, t0, r#"{"job":"api","status":"500"}"#),
        (
            "tenant-o",
            "m_x",
            43,
            t0,
            r#"{"job":"api","status":"500","zone":"o"}"#,
        ),
        ("tenant-o", "m_o", 44, t0, r#"{"job":"api","status":"502"}"#),
        ("tenant-o", "m_y", 45, t0, r#"{"job":"db","status":"200"}"#),
        ("tenant-o", "m_z", 46, t0, r#"{"job":"api","status":"200"}"#),
        ("tenant-o", "h_x", 47, t0, r#"{"job":"api","status":"500"}"#),
        ("tenant-o", "m_x", 49, t0, r#"{"job":"api","status":"500"}"#),
        ("tenant-o", "h_y", 50, t0, r#"{"job":"db"}"#),
    ];
    let floats = [
        ("tenant-q", "m_x", 42, m(0), 1.0),
        ("tenant-q", "m_x", 42, m(1), 11.0),
        ("tenant-q", "m_x", 43, m(0), 2.0),
        ("tenant-q", "m_x", 43, m(1), 12.0),
        ("tenant-q", "m_x", 45, back, 15.0),
        ("tenant-q", "m_x", 45, m(0), 5.0),
        ("tenant-q", "m_x", 45, m(1), 65.0),
        ("tenant-q", "m_x", 46, m(0), 6.0),
        ("tenant-q", "m_x", 46, m(1), 16.0),
        ("tenant-q", "m_w", 49, m(0), 9.0),
        ("tenant-q", "m_w", 49, m(1), 39.0),
        ("tenant-q", "m_x", 50, m(0), 10.0),
        ("tenant-q", "m_x", 50, m(1), 20.0),
        ("tenant-o", "m_x", 42, m(0), 3.0),
        ("tenant-o", "m_x", 42, m(1), 13.0),
        ("tenant-o", "m_x", 43, m(5), 4.0),
        ("tenant-o", "m_x", 43, m(6), 14.0),
        ("tenant-o", "m_o", 44, m(0), 5.0),
        ("tenant-o", "m_y", 45, m(0), 7.0),
        ("tenant-o", "m_z", 46, m(0), 8.0),
        ("tenant-o", "m_x", 49, m(0), 99.0),
        ("tenant-o", "m_x", 49, m(1), 109.0),
    ];
    let hists = [
        ("tenant-q", "h_x", 47, m(0), 10, 1.0),
        ("tenant-q", "h_x", 47, m(1), 20, 2.0),
        ("tenant-o", "h_x", 47, m(5), 30, 3.0),
        ("tenant-o", "h_y", 50, m(6), 40, 4.0),
    ];
    let descriptors = [
        ("tenant-q", "m_x", "counter", "q help"),
        ("tenant-o", "m_x", "counter", "o help"),
        ("tenant-o", "m_o", "gauge", "o only"),
    ];
    let mut out = Vec::new();
    let rows: Vec<String> = series
        .iter()
        .map(|(org, name, id, at, labels)| {
            format!("('{org}', 0, 2, '{name}', toUInt128({id}), {at}, 0, '{labels}', 0)")
        })
        .chain(floats.iter().map(|(org, name, id, at, v)| {
            format!("('{org}', 0, 0, '{name}', toUInt128({id}), {at}, {v}, '', 0)")
        }))
        .collect();
    out.push(format!(
        "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, fingerprint, \
         unix_milli, value, labels, value_type) VALUES {}",
        rows.join(", ")
    ));
    let rows: Vec<String> = hists
        .iter()
        .map(|(org, name, id, at, count, sum)| {
            format!(
                "('{org}', 0, 1, '{name}', toUInt128({id}), {at}, 0, {count}, {sum}, [0], [1], [{count}])"
            )
        })
        .collect();
    out.push(format!(
        "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, fingerprint, \
         unix_milli, hist_schema, hist_count, hist_sum, hist_pos_span_offsets, \
         hist_pos_span_lengths, hist_pos_bucket_deltas) VALUES {}",
        rows.join(", ")
    ));
    let rows: Vec<String> = descriptors
        .iter()
        .map(|(org, name, ty, help)| format!("('{org}', 0, 3, '{name}', '{ty}', '{help}', 1)"))
        .collect();
    out.push(format!(
        "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, metric_type, \
         help, updated_ns) VALUES {}",
        rows.join(", ")
    ));
    out
}

struct Fixture {
    admin: ChClient,
    bootstrap: ChClient,
    db: String,
    /// The design's anchor `T0`: thirty minutes ago, minute-aligned.
    t0: i64,
}

/// `db` is composed by `pulsus_testkit::test_db` at the call site, where
/// the naming guards read it.
async fn fixture(db: &str) -> Fixture {
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    drop_database(&bootstrap, db).await;
    run_init(&bootstrap, &RenderCtx::for_tests(db))
        .await
        .expect("run_init");
    let admin = ChClient::new(test_config(db)).await.expect("connect");
    let now = chrono::Utc::now().timestamp_millis();
    let t0 = (now / MINUTE_MS) * MINUTE_MS - 30 * MINUTE_MS;
    for sql in data_sql(t0) {
        exec(&admin, &sql).await;
    }
    Fixture {
        admin,
        bootstrap,
        db: db.to_string(),
        t0,
    }
}

impl Fixture {
    async fn finish(self) {
        drop_database(&self.bootstrap, &self.db).await;
    }

    async fn engine(&self, cache: &Arc<LabelCache>) -> MetricsEngine {
        MetricsEngine::new(
            ChClient::new(test_config(&self.db)).await.expect("connect"),
            Arc::clone(cache),
            engine_config(&self.db),
        )
    }

    async fn cache(&self) -> Arc<LabelCache> {
        Arc::new(LabelCache::new(
            ChClient::new(test_config(&self.db)).await.expect("connect"),
            cache_config(&self.db),
        ))
    }

    fn at(&self, minutes: i64) -> MetricQueryParams {
        let t = self.t0 + minutes * MINUTE_MS;
        MetricQueryParams {
            start_ms: t,
            end_ms: t,
            step_ms: 0,
        }
    }

    fn window(&self) -> DataWindow {
        DataWindow {
            start_ms: self.t0 - 10 * MINUTE_MS,
            end_ms: self.t0 + 10 * MINUTE_MS,
        }
    }
}

/// One answer, written the way the case table writes it: per series its
/// labels, then each point as `minutes:value`, a histogram's value as
/// `count/sum`, a step's time in minutes from `t0`.
fn rendered(r: &QueryResult, t0: i64) -> Vec<String> {
    fn labels(l: &[(String, String)]) -> String {
        let mut l = l.to_vec();
        l.sort();
        let name = l
            .iter()
            .find(|(k, _)| k == "__name__")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        let rest: Vec<String> = l
            .iter()
            .filter(|(k, _)| k != "__name__")
            .map(|(k, v)| format!("{k}=\"{v}\""))
            .collect();
        format!("{name}{{{}}}", rest.join(","))
    }
    let minutes = |t: i64| {
        let d = t - t0;
        if d % MINUTE_MS == 0 {
            format!("{}", d / MINUTE_MS)
        } else {
            format!("{}s", d / 1_000)
        }
    };
    fn value(v: &HistOrFloat) -> String {
        match v {
            HistOrFloat::Float(f) => format!("{f:?}"),
            HistOrFloat::Hist(h) => format!("{}/{}", h.count, h.sum),
        }
    }
    let mut out: Vec<String> = match r {
        QueryResult::Vector(v) => v
            .iter()
            .map(|s| format!("{} {:?}", labels(&s.labels), s.value))
            .collect(),
        QueryResult::VectorHist(v) => v
            .iter()
            .map(|s| format!("{} {}", labels(&s.labels), value(&s.value)))
            .collect(),
        QueryResult::Matrix(m) => m
            .iter()
            .map(|s| {
                let pts: Vec<String> = s
                    .points
                    .iter()
                    .map(|(t, v)| format!("{}:{v:?}", minutes(*t)))
                    .collect();
                format!("{} {}", labels(&s.labels), pts.join(" "))
            })
            .collect(),
        QueryResult::MatrixHist(m) => m
            .iter()
            .map(|s| {
                let pts: Vec<String> = s
                    .points
                    .iter()
                    .map(|(t, v)| format!("{}:{}", minutes(*t), value(v)))
                    .collect();
                format!("{} {}", labels(&s.labels), pts.join(" "))
            })
            .collect(),
        other => panic!("unexpected result shape: {other:?}"),
    };
    out.sort();
    out
}

fn sorted(v: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = v.iter().map(|s| s.to_string()).collect();
    out.sort();
    out
}

async fn query(
    engine: &MetricsEngine,
    tenant: &Tenant,
    q: &str,
    p: &MetricQueryParams,
    t0: i64,
) -> Vec<String> {
    let expr = parse(q).expect("parse");
    let (r, _) = engine
        .query(tenant, &expr, p)
        .await
        .unwrap_or_else(|e| panic!("{q} as {}: {e:?}", tenant.as_str()));
    rendered(&r, t0)
}

fn mt(key: &str, op: MatchOp, value: &str) -> LabelMatcher {
    LabelMatcher {
        key: key.to_string(),
        op,
        value: value.to_string(),
    }
}

/// The design's selectors as discovery filters: A `{job="api",
/// status=~"5.."}`, B `{job="api", zone!="o"}`, C `m_x{job="api",
/// status=~"5.."}`.
fn filter(which: char) -> DiscoveryFilter {
    let (metric_name, matchers) = match which {
        'A' => (
            None,
            vec![
                mt("job", MatchOp::Eq, "api"),
                mt("status", MatchOp::Re, "5.."),
            ],
        ),
        'B' => (
            None,
            vec![mt("job", MatchOp::Eq, "api"), mt("zone", MatchOp::Neq, "o")],
        ),
        _ => (
            Some("m_x".to_string()),
            vec![
                mt("job", MatchOp::Eq, "api"),
                mt("status", MatchOp::Re, "5.."),
            ],
        ),
    };
    DiscoveryFilter {
        metric_name,
        name_matchers: Vec::new(),
        matchers,
    }
}

/// A series' label set as the case table writes it.
fn series_text(s: &[(String, String)]) -> String {
    let mut l = s.to_vec();
    l.sort();
    let name = l
        .iter()
        .find(|(k, _)| k == "__name__")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    let rest: Vec<String> = l
        .iter()
        .filter(|(k, _)| k != "__name__")
        .map(|(k, v)| format!("{k}=\"{v}\""))
        .collect();
    format!("{name}{{{}}}", rest.join(","))
}

const A: &str = r#"{job="api", status=~"5.."}"#;
const C: &str = r#"m_x{job="api", status=~"5.."}"#;

/// **T10: every read answers its own tenant.** Cases C1-C6 of the design,
/// on an engine whose label cache is never refreshed, as each tenant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_read_answers_its_own_tenant() {
    skip_unless_live!();
    let fx = fixture(&pulsus_testkit::test_db("pulsus_read_it_tenants_t10")).await;
    let cache = fx.cache().await;
    let engine = fx.engine(&cache).await;
    let (q, o) = (tenant("tenant-q"), tenant("tenant-o"));
    let t0 = fx.t0;

    // C1: statement R and the two sample reads over statement 1 of A.
    let c1 = format!("{A}[10m]");
    assert_eq!(
        query(&engine, &q, &c1, &fx.at(9), t0).await,
        sorted(&[
            r#"h_x{job="api",status="500"} 0:10/1 1:20/2"#,
            r#"m_w{job="api",status="500"} 0:9.0 1:39.0"#,
            r#"m_x{job="api",status="500"} 0:2.0 1:12.0"#,
        ]),
        "C1 as tenant-q"
    );
    assert_eq!(
        query(&engine, &o, &c1, &fx.at(9), t0).await,
        sorted(&[
            r#"h_x{job="api",status="500"} 5:30/3"#,
            r#"m_o{job="api",status="502"} 0:5.0"#,
            r#"m_x{job="api",status="500"} 0:3.0 1:13.0"#,
            r#"m_x{job="api",status="500",zone="o"} 5:4.0 6:14.0"#,
            r#"m_x{job="api",status="500"} 0:99.0 1:109.0"#,
        ]),
        "C1 as tenant-o"
    );

    // C2: the concrete-name fallback and its label hydration.
    let c2 = format!("{C}[10m]");
    assert_eq!(
        query(&engine, &q, &c2, &fx.at(9), t0).await,
        sorted(&[r#"m_x{job="api",status="500"} 0:2.0 1:12.0"#]),
        "C2 as tenant-q"
    );
    assert_eq!(
        query(&engine, &o, &c2, &fx.at(9), t0).await,
        sorted(&[
            r#"m_x{job="api",status="500"} 0:3.0 1:13.0"#,
            r#"m_x{job="api",status="500",zone="o"} 5:4.0 6:14.0"#,
            r#"m_x{job="api",status="500"} 0:99.0 1:109.0"#,
        ]),
        "C2 as tenant-o"
    );

    // C3: the histogram read.
    assert_eq!(
        query(&engine, &q, "h_x[10m]", &fx.at(9), t0).await,
        sorted(&[r#"h_x{job="api",status="500"} 0:10/1 1:20/2"#]),
        "C3 as tenant-q"
    );
    assert_eq!(
        query(&engine, &o, "h_x[10m]", &fx.at(9), t0).await,
        sorted(&[r#"h_x{job="api",status="500"} 5:30/3"#]),
        "C3 as tenant-o"
    );

    // C4: /series over A, B and C.
    let series = |t: &Tenant, which: char| {
        let engine = &engine;
        let t = t.clone();
        let window = fx.window();
        async move {
            let mut got: Vec<String> = engine
                .series(&t, &[filter(which)], window)
                .await
                .unwrap_or_else(|e| panic!("series {which}: {e:?}"))
                .iter()
                .map(|s| series_text(s))
                .collect();
            got.sort();
            got
        }
    };
    for (which, want_q, want_o) in [
        (
            'A',
            vec![
                r#"h_x{job="api",status="500"}"#,
                r#"m_w{job="api",status="500"}"#,
                r#"m_x{job="api",status="500"}"#,
            ],
            vec![
                r#"h_x{job="api",status="500"}"#,
                r#"m_o{job="api",status="502"}"#,
                r#"m_x{job="api",status="500"}"#,
                r#"m_x{job="api",status="500",zone="o"}"#,
                r#"m_x{job="api",status="500"}"#,
            ],
        ),
        (
            'B',
            vec![
                r#"h_x{job="api",status="500"}"#,
                r#"m_w{job="api",status="500"}"#,
                r#"m_x{job="api",status="200"}"#,
                r#"m_x{job="api",status="500"}"#,
                r#"m_x{job="api",status="404"}"#,
            ],
            vec![
                r#"h_x{job="api",status="500"}"#,
                r#"m_o{job="api",status="502"}"#,
                r#"m_x{job="api",status="500"}"#,
                r#"m_x{job="api",status="500"}"#,
                r#"m_z{job="api",status="200"}"#,
            ],
        ),
        (
            'C',
            vec![r#"m_x{job="api",status="500"}"#],
            vec![
                r#"m_x{job="api",status="500"}"#,
                r#"m_x{job="api",status="500",zone="o"}"#,
                r#"m_x{job="api",status="500"}"#,
            ],
        ),
    ] {
        // `/series` answers label sets: two series of one name and one
        // label set (tenant-o's IDs 42 and 49) are one answer.
        let unique = |v: &[&str]| {
            let mut v = sorted(v);
            v.dedup();
            v
        };
        assert_eq!(
            series(&q, which).await,
            unique(&want_q),
            "C4 {which} as tenant-q"
        );
        assert_eq!(
            series(&o, which).await,
            unique(&want_o),
            "C4 {which} as tenant-o"
        );
    }

    // C5: label names, the values of `zone` and of `__name__`, over A.
    let a = [filter('A')];
    let names = |t: Tenant| {
        let engine = &engine;
        let a = a.clone();
        let window = fx.window();
        async move {
            (
                engine
                    .label_names(&t, &a, window)
                    .await
                    .expect("label names"),
                engine
                    .label_values(&t, "zone", &a, window)
                    .await
                    .expect("zone values"),
                engine
                    .label_values(&t, "__name__", &a, window)
                    .await
                    .expect("name values"),
            )
        }
    };
    assert_eq!(
        names(q.clone()).await,
        (
            sorted(&["__name__", "job", "status"]),
            Vec::<String>::new(),
            sorted(&["h_x", "m_w", "m_x"]),
        ),
        "C5 as tenant-q"
    );
    assert_eq!(
        names(o.clone()).await,
        (
            sorted(&["__name__", "job", "status", "zone"]),
            sorted(&["o"]),
            sorted(&["h_x", "m_o", "m_x"]),
        ),
        "C5 as tenant-o"
    );

    // C6: metadata.
    let meta = |t: Tenant| {
        let engine = &engine;
        async move {
            let mut got: Vec<String> = engine
                .metadata(&t, None, None)
                .await
                .expect("metadata")
                .into_iter()
                .map(|m| format!("{} {} {}", m.name, m.metric_type, m.help))
                .collect();
            got.sort();
            got
        }
    };
    assert_eq!(
        meta(q.clone()).await,
        sorted(&["m_x counter q help"]),
        "C6 as tenant-q"
    );
    assert_eq!(
        meta(o.clone()).await,
        sorted(&["m_o gauge o only", "m_x counter o help"]),
        "C6 as tenant-o"
    );
    fx.finish().await;
}

/// **T4: the fallback hydration reads the requesting tenant's labels.** A
/// cold cache, two tenants holding IDs 42 and 43 with different labels:
/// tenant-q's `m_x{job="api"}` carries tenant-q's labels only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fallback_hydration_reads_its_own_tenants_labels() {
    skip_unless_live!();
    let fx = fixture(&pulsus_testkit::test_db("pulsus_read_it_tenants_t4")).await;
    let cache = fx.cache().await;
    let engine = fx.engine(&cache).await;
    let t0 = fx.t0;
    assert_eq!(
        query(
            &engine,
            &tenant("tenant-q"),
            r#"m_x{job="api"}"#,
            &fx.at(1),
            t0
        )
        .await,
        sorted(&[
            r#"m_x{job="api",status="200"} 11.0"#,
            r#"m_x{job="api",status="404"} 20.0"#,
            r#"m_x{job="api",status="500"} 12.0"#,
        ]),
        "tenant-q's series, with tenant-q's labels"
    );
    fx.finish().await;
}

/// **T12: a warm cache answers each tenant its own members.** The same
/// rows, the label cache refreshed for both tenants, the push on; cases
/// W1-W5 as each tenant. W3 as tenant-q reads the sample tables in exactly
/// one statement, the pushed one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_warm_cache_answers_each_tenant_its_own_members() {
    skip_unless_live!();
    let fx = fixture(&pulsus_testkit::test_db("pulsus_read_it_tenants_t12")).await;
    let cache = fx.cache().await;
    let (q, o) = (tenant("tenant-q"), tenant("tenant-o"));
    cache.refresh_tenant(&q).await.expect("refresh tenant-q");
    cache.refresh_tenant(&o).await.expect("refresh tenant-o");
    let engine = fx.engine(&cache).await;
    let t0 = fx.t0;

    // W1: the cache's IDs, a literal sample fetch.
    let w1_q = [
        r#"m_x{job="api",status="200"} 0:1.0 1:11.0"#,
        r#"m_x{job="api",status="404"} 0:10.0 1:20.0"#,
        r#"m_x{job="api",status="500"} 0:2.0 1:12.0"#,
    ];
    let w1_o = [
        r#"m_x{job="api",status="500"} 0:3.0 1:13.0"#,
        r#"m_x{job="api",status="500",zone="o"} 5:4.0 6:14.0"#,
        r#"m_x{job="api",status="500"} 0:99.0 1:109.0"#,
    ];
    let w1 = r#"m_x{job="api"}[10m]"#;
    assert_eq!(
        query(&engine, &q, w1, &fx.at(9), t0).await,
        sorted(&w1_q),
        "W1 as tenant-q"
    );
    assert_eq!(
        query(&engine, &o, w1, &fx.at(9), t0).await,
        sorted(&w1_o),
        "W1 as tenant-o"
    );

    // W2: a `__name__` matcher, the multi-name fetch and its histogram twin.
    let w2 = r#"{__name__=~"m_x|h_x", job="api"}[10m]"#;
    let mut w2_q = w1_q.to_vec();
    w2_q.push(r#"h_x{job="api",status="500"} 0:10/1 1:20/2"#);
    let mut w2_o = w1_o.to_vec();
    w2_o.push(r#"h_x{job="api",status="500"} 5:30/3"#);
    assert_eq!(
        query(&engine, &q, w2, &fx.at(9), t0).await,
        sorted(&w2_q),
        "W2 as tenant-q"
    );
    assert_eq!(
        query(&engine, &o, w2, &fx.at(9), t0).await,
        sorted(&w2_o),
        "W2 as tenant-o"
    );

    // W3: the pushed rate, one statement over the cache's members.
    let w3 = r#"sum(rate(m_x{job="api"}[5m]))"#;
    let w3_params = MetricQueryParams {
        start_ms: t0 + 2 * MINUTE_MS,
        end_ms: t0 + 6 * MINUTE_MS + 30_000,
        step_ms: 270_000,
    };
    let marker = server_marker(&fx.admin).await;
    assert_eq!(
        query(&engine, &q, w3, &w3_params, t0).await,
        sorted(&["{} 2:0.22666666666666668"]),
        "W3 as tenant-q"
    );
    let sent = statements_since(&fx.admin, &fx.db, &marker).await;
    let sample_reads: Vec<&str> = sent
        .iter()
        .map(|s| s.query.as_str())
        .filter(|s| s.contains("metric_samples") || s.contains("metric_hist_samples"))
        .collect();
    assert_eq!(
        sample_reads.len(),
        1,
        "W3 reads the sample tables in one statement: {sample_reads:#?}"
    );
    assert!(
        sample_reads[0].trim_start().starts_with("WITH "),
        "the pushed statement: {}",
        sample_reads[0]
    );
    assert_eq!(
        query(&engine, &o, w3, &w3_params, t0).await,
        sorted(&["{} 2:0.15999999999999998 390s:0.06333333333333332"]),
        "W3 as tenant-o"
    );

    // W4: the pushed count.
    let w4 = r#"count(m_x{job="api"})"#;
    let w4_params = MetricQueryParams {
        start_ms: t0 + MINUTE_MS,
        end_ms: t0 + 7 * MINUTE_MS,
        step_ms: 6 * MINUTE_MS,
    };
    assert_eq!(
        query(&engine, &q, w4, &w4_params, t0).await,
        sorted(&["{} 1:3.0"]),
        "W4 as tenant-q"
    );
    assert_eq!(
        query(&engine, &o, w4, &w4_params, t0).await,
        sorted(&["{} 1:2.0 7:1.0"]),
        "W4 as tenant-o"
    );

    // W5: the cache's IDs, a histogram fetch.
    assert_eq!(
        query(&engine, &q, "h_x[10m]", &fx.at(9), t0).await,
        sorted(&[r#"h_x{job="api",status="500"} 0:10/1 1:20/2"#]),
        "W5 as tenant-q"
    );
    assert_eq!(
        query(&engine, &o, "h_x[10m]", &fx.at(9), t0).await,
        sorted(&[r#"h_x{job="api",status="500"} 5:30/3"#]),
        "W5 as tenant-o"
    );
    fx.finish().await;
}
