//! Issue #501 part 2: `/api/v1/query_exemplars`' read against a live
//! ClickHouse — which exemplars, a stored repeat once, the window's bounds,
//! the tenant, the 100,000 cap and the thinned answer past it.
//!
//! Series are registered through `metric_landing` (kind 2), so the views
//! fill the activity, lookup and index tables as a push does; exemplars are
//! inserted into `metric_exemplars` directly, at times inside the
//! retention. Every expected answer is written here from the fixture's own
//! definition, not read back.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test live_metrics_exemplars
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_model::{LabelMatcher, MatchOp, Tenant};
use pulsus_promql::PromqlError;
use pulsus_read::{
    DataWindow, DiscoveryFilter, ExemplarSeries, LabelCache, LabelCacheConfig, MetricsConfig,
    MetricsEngine, ReadError, StoredExemplar,
};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 3_600_000;
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
        query_timeout: Duration::from_secs(300),
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
        exemplars_table: "metric_exemplars".to_string(),
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

fn tenant(name: &str) -> Tenant {
    if name.is_empty() {
        return Tenant::from_header(None, false).expect("the empty tenant");
    }
    let header = http::HeaderValue::from_str(name).expect("a header value");
    Tenant::from_header(Some(&header), false).expect("a valid tenant")
}

fn mt(key: &str, op: MatchOp, value: &str) -> LabelMatcher {
    LabelMatcher {
        key: key.to_string(),
        op,
        value: value.to_string(),
    }
}

/// `m{<key>="<value>"}`.
fn m_eq(key: &str, value: &str) -> DiscoveryFilter {
    DiscoveryFilter {
        metric_name: Some("m".to_string()),
        name_matchers: vec![],
        matchers: vec![mt(key, MatchOp::Eq, value)],
    }
}

/// One series registration: tenant, ID, labels JSON, and the hours it is
/// registered in (each a time inside the hour).
struct Series<'a> {
    org: &'a str,
    fp: u128,
    labels: &'a str,
    hours: Vec<i64>,
}

struct Fixture {
    admin: ChClient,
    bootstrap: ChClient,
    db: String,
    /// Thirty minutes ago, minute-aligned.
    t0: i64,
}

impl Fixture {
    /// `db` is composed by `pulsus_testkit::test_db` at the call site,
    /// where the naming guard reads it.
    async fn new(db: String) -> Fixture {
        let bootstrap = ChClient::new(test_config("default"))
            .await
            .expect("connect (bootstrap)");
        drop_database(&bootstrap, &db).await;
        run_init(&bootstrap, &RenderCtx::for_tests(&db))
            .await
            .expect("run_init");
        let admin = ChClient::new(test_config(&db)).await.expect("connect");
        let now = chrono::Utc::now().timestamp_millis();
        let t0 = (now / MINUTE_MS) * MINUTE_MS - 30 * MINUTE_MS;
        Fixture {
            admin,
            bootstrap,
            db,
            t0,
        }
    }

    async fn exec(&self, sql: &str) {
        self.admin
            .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
            .await
            .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    }

    /// Registers `series` through `metric_landing`, kind 2, one row per
    /// hour each is registered in.
    async fn register(&self, series: &[Series<'_>]) {
        let rows: Vec<String> = series
            .iter()
            .flat_map(|s| {
                s.hours.iter().map(move |at| {
                    format!(
                        "('{}', 0, 2, 'm', toUInt128({}), {at}, 0, '{}', 0)",
                        s.org, s.fp, s.labels
                    )
                })
            })
            .collect();
        self.exec(&format!(
            "INSERT INTO metric_landing (org_id, received_ms, kind, metric_name, fingerprint, \
             unix_milli, value, labels, value_type) VALUES {}",
            rows.join(", ")
        ))
        .await;
    }

    /// Inserts exemplar rows `(org, fp, ms, value, trace byte, span byte,
    /// labels)` — a zero byte stores the all-zero id.
    async fn exemplars(&self, rows: &[(&str, u128, i64, f64, u8, u8, &str)]) {
        let values: Vec<String> = rows
            .iter()
            .map(|(org, fp, ms, v, trace, span, labels)| {
                format!(
                    "('{org}', toUInt128({fp}), {ms}, {v}, {}, {}, '{labels}')",
                    id_sql(*trace, 16),
                    id_sql(*span, 8)
                )
            })
            .collect();
        self.exec(&format!(
            "INSERT INTO metric_exemplars (org_id, fingerprint, unix_milli, value, trace_id, \
             span_id, labels) VALUES {}",
            values.join(", ")
        ))
        .await;
    }

    async fn engine(&self) -> MetricsEngine {
        let cache = Arc::new(LabelCache::new(
            ChClient::new(test_config(&self.db)).await.expect("connect"),
            cache_config(&self.db),
        ));
        MetricsEngine::new(
            ChClient::new(test_config(&self.db)).await.expect("connect"),
            cache,
            engine_config(&self.db),
        )
    }

    async fn finish(self) {
        drop_database(&self.bootstrap, &self.db).await;
    }
}

/// The SQL of an id whose bytes are all `b`, or all zero for `0`.
fn id_sql(b: u8, len: usize) -> String {
    if b == 0 {
        format!("toFixedString('', {len})")
    } else {
        format!("unhex('{}')", format!("{b:02x}").repeat(len))
    }
}

/// The hex text of an id whose bytes are all `b`.
fn id_hex(b: u8, len: usize) -> String {
    format!("{b:02x}").repeat(len)
}

async fn drop_database(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

fn pairs(p: &[(&str, &str)]) -> Vec<(String, String)> {
    p.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn ex(labels: Vec<(String, String)>, value: f64, unix_milli: i64) -> StoredExemplar {
    StoredExemplar {
        labels,
        value,
        unix_milli,
    }
}

/// L1, L2, L3, L4, L5 and L7 over one small fixture.
#[tokio::test(flavor = "multi_thread")]
async fn the_answer_is_the_selected_series_stored_exemplars() {
    skip_unless_live!();
    let fx = Fixture::new(pulsus_testkit::test_db("pulsus_exemplars_small")).await;
    let t0 = fx.t0;
    let (start, end) = (t0 - 5 * MINUTE_MS, t0 + 5 * MINUTE_MS);
    fx.register(&[
        Series {
            org: "",
            fp: 1,
            labels: r#"{"job":"api","s":"a"}"#,
            hours: vec![start, end],
        },
        Series {
            org: "",
            fp: 2,
            labels: r#"{"job":"web","s":"b"}"#,
            hours: vec![start, end],
        },
        Series {
            org: "",
            fp: 3,
            labels: r#"{"job":"edge","s":"c"}"#,
            hours: vec![start, end],
        },
        Series {
            org: "",
            fp: 4,
            labels: r#"{"job":"repeat","s":"d"}"#,
            hours: vec![start, end],
        },
        Series {
            org: "t1",
            fp: 7,
            labels: r#"{"job":"tenant"}"#,
            hours: vec![start, end],
        },
        Series {
            org: "t2",
            fp: 7,
            labels: r#"{"job":"tenant"}"#,
            hours: vec![start, end],
        },
    ])
    .await;
    fx.exemplars(&[
        ("", 1, t0, 1.5, 0x11, 0x21, r#"{"k":"a1"}"#),
        ("", 1, t0 + 1_000, 2.5, 0, 0, r#"{"k":"a2"}"#),
        ("", 1, t0 + 2_000, -3.25, 0x13, 0, "{}"),
        ("", 2, t0, 10.0, 0x14, 0x24, r#"{"k":"b1"}"#),
        ("", 2, t0 + 1_000, 20.0, 0, 0, "{}"),
        ("", 2, t0 + 2_000, 30.0, 0, 0, "{}"),
        // L3: either side of each bound.
        ("", 3, start - 1, 1.0, 0, 0, "{}"),
        ("", 3, start, 2.0, 0, 0, "{}"),
        ("", 3, end, 3.0, 0, 0, "{}"),
        ("", 3, end + 1, 4.0, 0, 0, "{}"),
        // L4: one exemplar stored twice.
        ("", 4, t0, 9.0, 0x19, 0, "{}"),
        ("", 4, t0, 9.0, 0x19, 0, "{}"),
        // L5: one series ID, two tenants.
        ("t1", 7, t0, 71.0, 0, 0, "{}"),
        ("t2", 7, t0 + 1, 72.0, 0, 0, "{}"),
    ])
    .await;
    let engine = fx.engine().await;
    let window = DataWindow {
        start_ms: start,
        end_ms: end,
    };
    let none = tenant("");
    let a_series = pairs(&[("__name__", "m"), ("job", "api"), ("s", "a")]);
    let a_exemplars = vec![
        ex(
            pairs(&[
                ("k", "a1"),
                ("span_id", &id_hex(0x21, 8)),
                ("trace_id", &id_hex(0x11, 16)),
            ]),
            1.5,
            t0,
        ),
        ex(pairs(&[("k", "a2")]), 2.5, t0 + 1_000),
        ex(pairs(&[("trace_id", &id_hex(0x13, 16))]), -3.25, t0 + 2_000),
    ];
    let a_answer = vec![ExemplarSeries {
        series_labels: a_series,
        exemplars: a_exemplars,
    }];

    // L1: exactly `a`'s three.
    let got = engine
        .exemplars(&none, &[m_eq("job", "api")], window)
        .await
        .expect("L1");
    assert_eq!(got, a_answer, "L1");

    // L2: two selectors matching `a` give it once.
    let got = engine
        .exemplars(&none, &[m_eq("s", "a"), m_eq("job", "api")], window)
        .await
        .expect("L2");
    assert_eq!(got, a_answer, "L2");

    // L3: the bounds are inclusive.
    let got = engine
        .exemplars(&none, &[m_eq("job", "edge")], window)
        .await
        .expect("L3");
    assert_eq!(
        got,
        vec![ExemplarSeries {
            series_labels: pairs(&[("__name__", "m"), ("job", "edge"), ("s", "c")]),
            exemplars: vec![ex(vec![], 2.0, start), ex(vec![], 3.0, end)],
        }],
        "L3"
    );

    // L4: a stored repeat once.
    let got = engine
        .exemplars(&none, &[m_eq("job", "repeat")], window)
        .await
        .expect("L4");
    assert_eq!(
        got,
        vec![ExemplarSeries {
            series_labels: pairs(&[("__name__", "m"), ("job", "repeat"), ("s", "d")]),
            exemplars: vec![ex(pairs(&[("trace_id", &id_hex(0x19, 16))]), 9.0, t0)],
        }],
        "L4"
    );

    // L5: each tenant its own exemplar.
    for (org, value, at) in [("t1", 71.0, t0), ("t2", 72.0, t0 + 1)] {
        let got = engine
            .exemplars(&tenant(org), &[m_eq("job", "tenant")], window)
            .await
            .expect("L5");
        assert_eq!(
            got,
            vec![ExemplarSeries {
                series_labels: pairs(&[("__name__", "m"), ("job", "tenant")]),
                exemplars: vec![ex(vec![], value, at)],
            }],
            "L5 {org}"
        );
    }

    // No exemplar on a selected series: an empty answer.
    let got = engine
        .exemplars(&none, &[m_eq("job", "nothing")], window)
        .await
        .expect("empty");
    assert_eq!(got, vec![], "no stored exemplar");

    // L7: a regex the database rejects is the client's error.
    let got = engine
        .exemplars(
            &none,
            &[DiscoveryFilter {
                metric_name: Some("m".to_string()),
                name_matchers: vec![],
                matchers: vec![mt("job", MatchOp::Re, "(")],
            }],
            window,
        )
        .await;
    match got {
        Err(ReadError::Promql(PromqlError::InvalidRegexMatcher { .. })) => {}
        other => panic!("L7: expected the regex refusal, got {other:?}"),
    }
    fx.finish().await;
}

/// L4b: 60,000 distinct exemplars, each stored twice — 120,000 stored rows,
/// so a branch on the stored count would thin — are answered whole.
#[tokio::test(flavor = "multi_thread")]
async fn distinct_exemplars_under_the_cap_are_answered_whole_whatever_is_stored() {
    skip_unless_live!();
    let fx = Fixture::new(pulsus_testkit::test_db("pulsus_exemplars_l4b")).await;
    let t0 = fx.t0;
    let start = t0 - 10 * MINUTE_MS;
    let series: Vec<Series<'_>> = (0..3u128)
        .map(|fp| Series {
            org: "",
            fp: 100 + fp,
            labels: ["{\"s\":\"0\"}", "{\"s\":\"1\"}", "{\"s\":\"2\"}"][fp as usize],
            hours: vec![start, start + 10 * MINUTE_MS],
        })
        .collect();
    fx.register(&series).await;
    // Row `n % 60000` twice: series `i % 3`, at `start + i`, value `i`.
    fx.exec(&format!(
        "INSERT INTO metric_exemplars (org_id, fingerprint, unix_milli, value, trace_id, \
         span_id, labels) \
         SELECT '', toUInt128(100 + (number % 60000) % 3), {start} + number % 60000, \
                toFloat64(number % 60000), toFixedString('', 16), toFixedString('', 8), '{{}}' \
         FROM numbers(120000)"
    ))
    .await;
    let engine = fx.engine().await;
    let got = engine
        .exemplars(
            &tenant(""),
            &[DiscoveryFilter {
                metric_name: Some("m".to_string()),
                name_matchers: vec![],
                matchers: vec![],
            }],
            DataWindow {
                start_ms: start,
                end_ms: start + 10 * MINUTE_MS,
            },
        )
        .await
        .expect("L4b");
    let mut want: Vec<ExemplarSeries> = (0..3usize)
        .map(|s| ExemplarSeries {
            series_labels: pairs(&[("__name__", "m"), ("s", &s.to_string())]),
            exemplars: (0..60_000i64)
                .filter(|i| *i as usize % 3 == s)
                .map(|i| ex(vec![], i as f64, start + i))
                .collect(),
        })
        .collect();
    want.sort_by(|a, b| a.series_labels.cmp(&b.series_labels));
    assert_eq!(
        got.iter().map(|s| s.exemplars.len()).sum::<usize>(),
        60_000,
        "L4b: every distinct exemplar"
    );
    assert!(got == want, "L4b: the answer differs from the fixture");
    fx.finish().await;
}

/// The thinned answer's expectation: per `w`-ms epoch-aligned slice, the
/// highest value, the earliest of equal values, then the lowest series —
/// `(series index, time, value)` per kept exemplar.
fn thinned(rows: &[(usize, i64, f64)], w: i64) -> BTreeSet<(usize, i64, u64)> {
    let mut best: BTreeMap<i64, (usize, i64, f64)> = BTreeMap::new();
    for &(s, t, v) in rows {
        let slice = t.div_euclid(w);
        let better = match best.get(&slice) {
            None => true,
            Some(&(bs, bt, bv)) => (v, -t, -(s as i64)) > (bv, -bt, -(bs as i64)),
        };
        if better {
            best.insert(slice, (s, t, v));
        }
    }
    best.values()
        .map(|&(s, t, v)| (s, t, v.to_bits()))
        .collect()
}

/// The answer as `(series index, time, value)`, the series index read from
/// its `s` label.
fn kept(answer: &[ExemplarSeries]) -> BTreeSet<(usize, i64, u64)> {
    answer
        .iter()
        .flat_map(|series| {
            let s: usize = series
                .series_labels
                .iter()
                .find(|(k, _)| k == "s")
                .map(|(_, v)| v.parse().expect("an index"))
                .expect("an s label");
            series
                .exemplars
                .iter()
                .map(move |e| (s, e.unix_milli, e.value.to_bits()))
        })
        .collect()
}

/// L6: 150,000 exemplars over 150 s — past the cap, so each 10 ms slice
/// keeps its highest.
#[tokio::test(flavor = "multi_thread")]
async fn past_the_cap_each_slice_keeps_its_highest_exemplar() {
    skip_unless_live!();
    let fx = Fixture::new(pulsus_testkit::test_db("pulsus_exemplars_l6")).await;
    let t0 = (fx.t0 / 10) * 10;
    let series: Vec<Series<'_>> = (0..3u128)
        .map(|fp| Series {
            org: "",
            fp: 100 + fp,
            labels: ["{\"s\":\"0\"}", "{\"s\":\"1\"}", "{\"s\":\"2\"}"][fp as usize],
            hours: vec![t0, t0 + 150_000],
        })
        .collect();
    fx.register(&series).await;
    fx.exec(&format!(
        "INSERT INTO metric_exemplars (org_id, fingerprint, unix_milli, value, trace_id, \
         span_id, labels) \
         SELECT '', toUInt128(100 + number % 3), {t0} + number, \
                toFloat64((number * 7919) % 1000), toFixedString('', 16), \
                toFixedString('', 8), '{{}}' \
         FROM numbers(150000)"
    ))
    .await;
    let rows: Vec<(usize, i64, f64)> = (0..150_000i64)
        .map(|i| ((i % 3) as usize, t0 + i, ((i * 7919) % 1000) as f64))
        .collect();
    let want = thinned(&rows, 10);
    assert_eq!(want.len(), 15_000);
    let engine = fx.engine().await;
    let got = engine
        .exemplars(
            &tenant(""),
            &[DiscoveryFilter {
                metric_name: Some("m".to_string()),
                name_matchers: vec![],
                matchers: vec![],
            }],
            DataWindow {
                start_ms: t0,
                end_ms: t0 + 149_999,
            },
        )
        .await
        .expect("L6");
    assert_eq!(
        got.iter().map(|s| s.exemplars.len()).sum::<usize>(),
        15_000,
        "L6: one exemplar per slice"
    );
    assert!(kept(&got) == want, "L6: the kept exemplars differ");
    fx.finish().await;
}

/// L6b: 600,000 exemplars with distinct values over two days: every
/// epoch-aligned 15 s bucket's highest is in the thinned answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_fifteen_second_panel_keeps_every_buckets_highest() {
    skip_unless_live!();
    let fx = Fixture::new(pulsus_testkit::test_db("pulsus_exemplars_l6b")).await;
    let end = (fx.t0 / 1_000) * 1_000;
    let start = end - 2 * DAY_MS;
    let hours: Vec<i64> = (0..=48).map(|h| start + h * HOUR_MS).collect();
    let series: Vec<Series<'_>> = (0..3u128)
        .map(|fp| Series {
            org: "",
            fp: 100 + fp,
            labels: ["{\"s\":\"0\"}", "{\"s\":\"1\"}", "{\"s\":\"2\"}"][fp as usize],
            hours: hours.clone(),
        })
        .collect();
    fx.register(&series).await;
    // Row `i`: series `i % 3`, at `start + 288 i`, value `(7919 i) mod
    // 600000` — 7919 is prime to 600,000, so every value is distinct.
    fx.exec(&format!(
        "INSERT INTO metric_exemplars (org_id, fingerprint, unix_milli, value, trace_id, \
         span_id, labels) \
         SELECT '', toUInt128(100 + number % 3), {start} + number * 288, \
                toFloat64((number * 7919) % 600000), toFixedString('', 16), \
                toFixedString('', 8), '{{}}' \
         FROM numbers(600000)"
    ))
    .await;
    let rows: Vec<(usize, i64, f64)> = (0..600_000i64)
        .map(|i| {
            (
                (i % 3) as usize,
                start + i * 288,
                ((i * 7919) % 600_000) as f64,
            )
        })
        .collect();
    let engine = fx.engine().await;
    let got = engine
        .exemplars(
            &tenant(""),
            &[DiscoveryFilter {
                metric_name: Some("m".to_string()),
                name_matchers: vec![],
                matchers: vec![],
            }],
            DataWindow {
                start_ms: start,
                end_ms: end,
            },
        )
        .await
        .expect("L6b");
    let answer = kept(&got);
    assert!(
        !answer.is_empty() && answer.len() < 100_000,
        "L6b: a thinned answer, {} exemplars",
        answer.len()
    );
    let buckets = thinned(&rows, 15_000);
    let missing: Vec<_> = buckets.iter().filter(|b| !answer.contains(b)).collect();
    assert!(
        missing.is_empty(),
        "L6b: {} of {} 15 s buckets lost their highest, first {:?}",
        missing.len(),
        buckets.len(),
        missing.first()
    );
    fx.finish().await;
}
