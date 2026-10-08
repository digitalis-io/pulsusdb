//! Issue #579: `sum`/`avg`/`count`/`min`/`max` over `rate`/`irate`/
//! `increase`, answered in the database, against a live ClickHouse.
//!
//! Every test asks the same query of two engines over one label-cache
//! snapshot — `grouped_push` on and off — and compares the answers as
//! bits, labels and annotations. The route with the push off is the one
//! that evaluates every step in this process; the push must answer what
//! it answers. Each test also checks the push was taken, or declined for
//! the reason it names, so a route that silently stopped pushing cannot
//! pass.
//!
//! The corpora are in `pushed_rate_corpus/mod.rs`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test live_metrics_pushed_rate
//! ```

#[path = "multi_name_corpus/mod.rs"]
mod multi_name_corpus;
#[path = "pushed_rate_corpus/mod.rs"]
mod pushed_rate_corpus;

use std::sync::Arc;

use pulsus_clickhouse::ChClient;
use pulsus_read::{LabelCache, MetricQueryParams, MetricsEngine};
use pushed_rate_corpus::{
    CPU_METRIC, DAY_MS, EDGE_METRIC, EQUAL_TIME_METRIC, HIST_ONLY_METRIC, Harness, ISSUE_QUERY,
    MIXED_METRIC, Routed, SAMPLE_EDGE_METRIC, SCALE_METRIC, SCRAPE_MS, SeedSeries, anchor,
    cache_config, cpu_corpus, edge_corpus, engine_config, equal_time_corpus, fresh_database,
    harness, harness_sql, histogram_corpora, now_ms, pushed_declines, pushed_statements,
    sample_edge_corpus, scale_corpus_sql, seed, seed_activity, seed_samples, server_marker,
    statements_since, test_config,
};

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

/// The pushed route sent a shape-A statement for `query`.
fn assert_pushed(query: &str, step_ms: i64, r: &Routed) {
    let statements = pushed_statements(r);
    assert!(
        !statements.is_empty(),
        "{query} at step {step_ms}: no pushed_aggregate statement in {:?}",
        r.stages
    );
    assert!(
        statements.iter().all(|s| s.contains("AS range_ms")),
        "{query}: {statements:?}"
    );
}

/// T1 (item 1): the issue's query over 24 h at 60 s, at 15 s, and as an
/// instant query: identical with the push on and off, and both of its
/// pushed nodes taken.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_cpu_query_answers_identically() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_pushed_rate_t1");
    let h = harness(&db, t, &cpu_corpus(t), None).await;
    for p in [h.day(60_000), h.day(15_000), h.instant()] {
        let r = h.agree(ISSUE_QUERY, &p).await;
        assert!(!r.answer.is_empty(), "the issue's query answers something");
        assert_pushed(ISSUE_QUERY, p.step_ms, &r);
        assert!(
            r.stages.iter().any(|(name, _)| name == "grouped_fetch"),
            "the inner count by (cpu) is pushed: {:?}",
            r.stages
        );
        assert!(
            !r.stages.iter().any(|(name, _)| name == "sample_fetch"),
            "no selector of the issue's query fetches samples: {:?}",
            r.stages
        );
    }
    h.finish().await;
}

/// T3 (item 3): each covered aggregation over each covered function,
/// under every grouping form, at 60 s, 15 s and instant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_covered_shape_answers_identically() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_pushed_rate_t3");
    let h = harness(&db, t, &cpu_corpus(t), None).await;
    for p in [h.day(60_000), h.day(15_000), h.instant()] {
        for func in ["rate", "irate", "increase"] {
            for agg in ["sum", "avg", "count", "min", "max"] {
                for grouping in ["by (mode) ", "by (cpu) ", "without (cpu) ", ""] {
                    let q = format!("{agg} {grouping}({func}({CPU_METRIC}[5m]))");
                    let r = h.agree(&q, &p).await;
                    assert_pushed(&q, p.step_ms, &r);
                }
            }
        }
    }
    h.finish().await;
}

/// T3b: the float edges — a `sum` that overflows, an `avg` that switches
/// to the incremental mean, and a NaN sample.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_float_edges_answer_identically() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_pushed_rate_t3b");
    let h = harness(&db, t, &edge_corpus(t), None).await;
    let mut saw_infinite_sum = false;
    for p in [h.ten_minutes(15_000), h.instant()] {
        for func in ["increase", "rate"] {
            for agg in ["sum", "avg", "min", "max"] {
                let q = format!("{agg}({func}({EDGE_METRIC}[5m]))");
                let r = h.agree(&q, &p).await;
                assert_pushed(&q, p.step_ms, &r);
                saw_infinite_sum |= agg == "sum"
                    && r.answer
                        .iter()
                        .any(|line| line.contains(&format!("{:016x}", f64::INFINITY.to_bits())));
            }
        }
    }
    assert!(
        saw_infinite_sum,
        "the corpus must make a sum overflow, or the branch is not exercised"
    );
    h.finish().await;
}

/// The stages whose text is a statement the route sends: every stage but
/// the push's own decline entries.
fn statement_stages(r: &Routed) -> Vec<(String, String)> {
    r.stages
        .iter()
        .filter(|(name, _)| name != "pushed_aggregate" && name != "grouped_push")
        .cloned()
        .collect()
}

/// T4 (item 4): one query for each named fallback. Explain's statement
/// texts and the answers are identical with the push on and off, and the
/// pushed engine names why it declined.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uncovered_queries_take_todays_route() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_pushed_rate_t4");
    let mut fx = cpu_corpus(t);
    fx.extend(histogram_corpora(t));
    let h = harness(&db, t, &fx, None).await;
    let day = h.day(60_000);
    let cases: [(&str, &str, MetricQueryParams); 6] = [
        (
            "F1",
            "stddev by (mode) (rate(node_cpu_seconds_total[5m]))",
            day,
        ),
        (
            "F2",
            "sum by (mode) (delta(node_cpu_seconds_total[5m]))",
            day,
        ),
        (
            "F3",
            "sum by (mode) (rate(node_cpu_seconds_total[5m] offset 5m))",
            day,
        ),
        (
            "F4",
            "sum by (__name__) (rate(node_cpu_seconds_total[5m]))",
            day,
        ),
        (
            "F6",
            "sum(rate(mixed_counter_total[5m]))",
            h.ten_minutes(15_000),
        ),
        ("F7", "max by (cpu, mode) (node_cpu_seconds_total)", day),
    ];
    let want = [
        "Aggregation",
        "RangeFunction",
        "Selector",
        "NameGrouping",
        "HistogramSamples",
        "TooFewSeriesPerGroup",
    ];
    for ((f, q, p), reason) in cases.iter().zip(want) {
        let (a, b) = h.both(q, p).await;
        assert_eq!(a.answer, b.answer, "{f} {q}: the answers differ");
        assert_eq!(
            a.annotations, b.annotations,
            "{f} {q}: the annotations differ"
        );
        assert_eq!(
            statement_stages(&a),
            statement_stages(&b),
            "{f} {q}: the statements differ"
        );
        let declines: Vec<String> = a
            .stages
            .iter()
            .filter(|(name, _)| name == "pushed_aggregate" || name == "grouped_push")
            .filter_map(|(_, sql)| sql.strip_prefix("declined: ").map(str::to_string))
            .collect();
        assert!(
            declines.iter().any(|d| d.contains(reason)),
            "{f} {q}: the pushed engine's explain names no {reason} decline: {:?}",
            a.stages
        );
    }

    // F5: a cold cache answers `SqlFallback`, so there is no fingerprint
    // list to assign groups over.
    let cold = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    let cold_pushed = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        Arc::clone(&cold),
        engine_config(&db, true),
    );
    let cold_unpushed = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        Arc::clone(&cold),
        engine_config(&db, false),
    );
    let q = "sum by (mode) (rate(node_cpu_seconds_total[5m]))";
    let p = h.ten_minutes(60_000);
    let a = Harness::run(&cold_pushed, q, &p).await;
    let b = Harness::run(&cold_unpushed, q, &p).await;
    assert_eq!(a.answer, b.answer, "F5 {q}: the answers differ");
    assert!(!a.answer.is_empty(), "F5 {q}: the fallback answers");
    assert_eq!(statement_stages(&a), statement_stages(&b), "F5 {q}");
    assert!(
        pushed_declines(&a)
            .iter()
            .any(|d| d.contains("ResolutionNotFingerprints")),
        "F5 {q}: {:?}",
        a.stages
    );

    // F8: a grid whose arithmetic would wrap in the database: `end +
    // range` passes `i64::MAX` for a 200-year range.
    let end_ms = i64::MAX - 5_000_000_000_000;
    let p = MetricQueryParams {
        start_ms: end_ms - 10_000_000_000_000,
        end_ms,
        step_ms: 10_000_000_000,
    };
    let q = "sum by (mode) (rate(node_cpu_seconds_total[200y]))";
    let (a, b) = h.both(q, &p).await;
    assert_eq!(a.answer, b.answer, "F8 {q}: the answers differ");
    assert_eq!(statement_stages(&a), statement_stages(&b), "F8 {q}");
    assert!(
        pushed_declines(&a)
            .iter()
            .any(|d| d.contains("GridOverflow")),
        "F8 {q}: {:?}",
        a.stages
    );
    h.finish().await;
}

/// T6 (F6): histogram samples in the window. (a) one histogram sample on
/// one series among float series; (b) series holding only histogram
/// samples, 20 to every window. The sentinel row counts them, the answer
/// equals the push-off answer, and explain names the decline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn histogram_samples_fall_back() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_pushed_rate_t6");
    let h = harness(&db, t, &histogram_corpora(t), None).await;
    for metric in [MIXED_METRIC, HIST_ONLY_METRIC] {
        for p in [h.ten_minutes(15_000), h.instant()] {
            for agg in ["sum", "avg", "max"] {
                let q = format!("{agg}(rate({metric}[5m]))");
                let r = h.agree(&q, &p).await;
                assert!(
                    pushed_declines(&r)
                        .iter()
                        .any(|d| d.contains("HistogramSamples")),
                    "{q}: {:?}",
                    r.stages
                );
                if metric == HIST_ONLY_METRIC && agg == "sum" {
                    assert!(
                        !r.answer.is_empty(),
                        "{q}: a histogram rate, not an empty success"
                    );
                }
            }
        }
    }
    h.finish().await;
}

/// T9: with the cap lowered to 262,144, the 15 s `sum` is split by time
/// into three statements and answers identically.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_node_is_split_by_time() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_pushed_rate_t9");
    let h = harness(&db, t, &cpu_corpus(t), Some(262_144)).await;
    let q = format!("sum by (mode) (rate({CPU_METRIC}[5m]))");
    let p = h.day(15_000);
    let marker = server_marker(&h.admin).await;
    let r = Harness::run(&h.pushed, &q, &p).await;
    let sent = statements_since(&h.admin, &h.db, &marker).await;
    let shape_a: Vec<_> = sent
        .iter()
        .filter(|s| s.query.contains("AS range_ms"))
        .collect();
    assert_eq!(
        shape_a.len(),
        3,
        "128 series x 5,761 steps under a cap of 262,144 is three statements: {sent:?}"
    );
    assert_eq!(
        sent.len(),
        3,
        "nothing else is sent for this query: {sent:?}"
    );
    let unpushed = Harness::run(&h.unpushed, &q, &p).await;
    assert_eq!(r.answer, unpushed.answer, "{q}: the split answer differs");
    assert_eq!(p.end_ms - p.start_ms, DAY_MS);
    h.finish().await;
}

// ------------------------------------------------ issue #579 part 2

/// The last 6 h ending at the anchor.
fn six_hours(h: &Harness, step_ms: i64) -> MetricQueryParams {
    MetricQueryParams {
        start_ms: h.t - 6 * multi_name_corpus::HOUR_MS,
        end_ms: h.t,
        step_ms,
    }
}

/// Some statement of `r` is a pushed one: a run statement or a shape-A
/// statement.
fn assert_pushed_any(query: &str, step_ms: i64, r: &Routed) {
    assert!(
        r.stages.iter().any(
            |(name, sql)| (name == "grouped_fetch" || name == "pushed_aggregate")
                && sql.starts_with("WITH ")
        ),
        "{query} at step {step_ms}: nothing was pushed: {:?}",
        r.stages
    );
    assert!(
        !r.stages.iter().any(|(name, _)| name == "sample_fetch"),
        "{query} at step {step_ms}: a sample fetch was sent: {:?}",
        r.stages
    );
}

/// One canonical answer line as a response body carries it: every NaN is
/// the text `NaN`, whatever its payload. The bits of any other value are
/// kept.
fn nan_as_text(line: &str) -> String {
    let chars: Vec<char> = line.chars().collect();
    let mut out = String::with_capacity(line.len());
    let mut i = 0;
    while i < chars.len() {
        let run = chars[i..]
            .iter()
            .take_while(|c| c.is_ascii_hexdigit())
            .count();
        let bounded = (i == 0 || !chars[i - 1].is_alphanumeric())
            && chars.get(i + run).is_none_or(|c| !c.is_alphanumeric());
        if run == 16 && bounded {
            let token: String = chars[i..i + 16].iter().collect();
            match u64::from_str_radix(&token, 16) {
                Ok(bits) if f64::from_bits(bits).is_nan() => out.push_str("NaN"),
                _ => out.push_str(&token),
            }
            i += 16;
        } else if run > 0 {
            out.extend(&chars[i..i + run]);
            i += run;
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

/// T1: the design's queries over multi-name selectors, plus `min`/`max`
/// over the NaN pair, at 60 s, 15 s and instant: the response bodies are
/// identical with the push on and off, and every query is pushed. A body
/// renders every NaN as `NaN`, so a NaN compares as one here; the payload
/// rule is T4's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_name_shapes_answer_identically() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_multi_name_t1");
    let h = harness(&db, t, &multi_name_corpus::multi_name_corpus(t), None).await;
    let mut queries: Vec<&str> = multi_name_corpus::QUERIES.to_vec();
    queries.extend([
        "min({__name__=~\"node_nan_.+\"})",
        "max({__name__=~\"node_nan_.+\"})",
    ]);
    for p in [six_hours(&h, 60_000), six_hours(&h, 15_000), h.instant()] {
        for q in &queries {
            let (a, b) = h.both(q, &p).await;
            assert_eq!(
                a.answer.iter().map(|l| nan_as_text(l)).collect::<Vec<_>>(),
                b.answer.iter().map(|l| nan_as_text(l)).collect::<Vec<_>>(),
                "{q} at step {}: the response bodies differ",
                p.step_ms
            );
            assert_eq!(a.annotations, b.annotations, "{q}: the annotations differ");
            assert_pushed_any(q, p.step_ms, &a);
        }
    }
    h.finish().await;
}

/// T4: members fold in `(metric_name, fingerprint)` order. Over several
/// names that differs from fingerprint order; `avg` of `irate` is the
/// compensated fold and `max` of the all-NaN hour answers the last
/// member's payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn member_order_is_name_then_id() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_multi_name_t4");
    let h = harness(&db, t, &multi_name_corpus::multi_name_corpus(t), None).await;
    for p in [six_hours(&h, 60_000), six_hours(&h, 15_000)] {
        for q in [
            "avg(irate({__name__=~\"node_m27.*\"}[5m]))",
            "max({__name__=~\"node_nan_.+\"})",
        ] {
            let r = h.agree(q, &p).await;
            assert_pushed_any(q, p.step_ms, &r);
        }
    }
    h.finish().await;
}

/// T8: one histogram sample in a multi-name shape-A selector. The answer
/// is identical, explain names the decline, and the fallback is today's
/// multi-name fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_name_histograms_fall_back() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_multi_name_t8");
    let h = harness(&db, t, &multi_name_corpus::multi_name_corpus(t), None).await;
    let q = "sum(rate({__name__=~\"hmix_.+\"}[5m]))";
    for p in [h.ten_minutes(15_000), h.instant()] {
        let (a, b) = h.both(q, &p).await;
        assert_eq!(a.answer, b.answer, "{q}: the answers differ");
        assert_eq!(a.annotations, b.annotations, "{q}: the annotations differ");
        assert!(
            pushed_declines(&a)
                .iter()
                .any(|d| d.contains("HistogramSamples")),
            "{q}: {:?}",
            a.stages
        );
        let fetches = |r: &Routed| -> Vec<(String, String)> {
            r.stages
                .iter()
                .filter(|(name, _)| name == "sample_fetch" || name == "hist_sample_fetch")
                .cloned()
                .collect()
        };
        assert!(!fetches(&b).is_empty());
        assert_eq!(
            fetches(&a),
            fetches(&b),
            "{q}: the fallback is today's fetch"
        );
    }
    h.finish().await;
}

/// T9: a multi-name node the threshold declines takes today's route: one
/// resolution, today's fetch, the same answer; a cold cache answers
/// today's error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_name_declines_keep_todays_route() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_multi_name_t9");
    let h = harness(&db, t, &multi_name_corpus::multi_name_corpus(t), None).await;
    // node_m10 … node_m19 hold one series each: ten groups, ten series.
    let q = "count by (__name__)({__name__=~\"node_m1[0-9]\"})";
    let p = six_hours(&h, 60_000);
    let (a, b) = h.both(q, &p).await;
    assert_eq!(a.answer, b.answer, "{q}: the answers differ");
    assert!(
        a.stages
            .iter()
            .any(|(name, sql)| name == "grouped_push" && sql == "declined: TooFewSeriesPerGroup"),
        "{q}: {:?}",
        a.stages
    );
    assert_eq!(
        a.stages
            .iter()
            .filter(|(name, _)| name == "series_resolution")
            .count(),
        1,
        "{q}: one resolution: {:?}",
        a.stages
    );
    let statements = |r: &Routed| -> Vec<(String, String)> {
        r.stages
            .iter()
            .filter(|(name, _)| name != "grouped_push" && name != "pushed_aggregate")
            .cloned()
            .collect()
    };
    assert_eq!(statements(&a), statements(&b), "{q}: today's statements");

    let cold = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    let expr = pulsus_promql::parser::parse(q).expect("parse");
    let mut errors = Vec::new();
    for push in [true, false] {
        let engine = MetricsEngine::new(
            ChClient::new(test_config(&db)).await.expect("connect"),
            Arc::clone(&cold),
            engine_config(&db, push),
        );
        errors.push(format!("{:?}", engine.query(&expr, &p).await.err()));
    }
    assert!(errors[0] != "None", "a cold cache answers an error");
    assert_eq!(errors[0], errors[1], "{q}: the cold-cache error differs");
    h.finish().await;
}

// ------------------------------------------------ issue #579 part 3

/// T10's first half: every pushed statement is part 3's form — the
/// samples gathered per series into arrays, merged with the step
/// boundaries, summed by running sums — and holds no window function and
/// no per-element Kahan fold.
fn assert_by_id_shape(query: &str, r: &Routed) {
    let statements = pushed_statements(r);
    assert!(
        !statements.is_empty(),
        "{query}: no pushed_aggregate statement in {:?}",
        r.stages
    );
    for s in statements {
        for want in [
            "arraySort(arrayZip(groupArray(unix_milli)",
            "arrayReverseFill(",
            "arrayCumSum(",
        ] {
            assert!(s.contains(want), "{query}: no `{want}` in {s}");
        }
        for unwanted in ["WINDOW w", "arrayFold((acc, x) -> (acc.1 + x"] {
            assert!(!s.contains(unwanted), "{query}: `{unwanted}` in {s}");
        }
    }
}

/// The two queries of part 3's measurements.
const SCALE_QUERIES: [&str; 2] = [
    "sum(rate(cpu_seconds[5m]))",
    "sum by (mode) (rate(cpu_seconds[5m]))",
];

/// T1: 100,000 series, above the label cache's 50,000-series cap, so the
/// selector resolves by SQL: both queries are pushed and answer what the
/// push turned off answers, bit for bit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hundred_thousand_series_push() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_pushed_rate_p3_t1");
    let h = harness_sql(&db, t, &scale_corpus_sql(t, 100_000, false), None).await;
    let p = h.ten_minutes(60_000);
    for (q, groups) in SCALE_QUERIES.iter().zip([1, 8]) {
        let r = h.agree(q, &p).await;
        assert_pushed(q, p.step_ms, &r);
        assert_eq!(r.answer.len(), groups, "{q}: {:?}", r.answer);
    }
    h.finish().await;
}

/// T3: with the push turned off, a selector above the cache's cap takes
/// the SQL route, and its label lookup reads by the ID statement rather
/// than a list of 100,000 IDs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_off_above_the_cache_cap() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_pushed_rate_p3_t3");
    let h = harness_sql(&db, t, &scale_corpus_sql(t, 100_000, false), None).await;
    let p = h.ten_minutes(60_000);
    let q = SCALE_QUERIES[0];
    let r = Harness::run(&h.unpushed, q, &p).await;
    assert_eq!(r.answer.len(), 1, "{q}: one series: {:?}", r.answer);
    assert_eq!(
        r.answer[0].matches(':').count(),
        11,
        "{q}: a point at each of the 11 steps: {:?}",
        r.answer
    );
    h.finish().await;
}

/// T4: 20,000 series, every grouping form and each aggregation and range
/// function the push answers: push on and off identical.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_thousand_series_by_and_without() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_pushed_rate_p3_t4");
    let h = harness_sql(&db, t, &scale_corpus_sql(t, 20_000, false), None).await;
    let p = h.ten_minutes(60_000);
    for q in [
        "sum by (mode) (rate(cpu_seconds[5m]))",
        "sum without (cpu) (rate(cpu_seconds[5m]))",
        "avg by (instance) (rate(cpu_seconds[5m]))",
        "max(irate(cpu_seconds[5m]))",
        "min(increase(cpu_seconds[5m]))",
        "count(rate(cpu_seconds[5m]))",
    ] {
        let r = h.agree(q, &p).await;
        assert_pushed(q, p.step_ms, &r);
        assert!(!r.answer.is_empty(), "{q}");
    }
    h.finish().await;
}

/// T5: 20,000 float series and one histogram series in the selector: the
/// statement counts the histogram samples and the push's fallback answers
/// as the push turned off.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn histogram_fallback_above_the_cap() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_pushed_rate_p3_t5");
    let h = harness_sql(&db, t, &scale_corpus_sql(t, 20_000, true), None).await;
    let p = h.ten_minutes(60_000);
    for q in SCALE_QUERIES {
        let r = h.agree(q, &p).await;
        assert!(
            pushed_declines(&r)
                .iter()
                .any(|d| d.contains("HistogramSamples")),
            "{q}: {:?}",
            r.stages
        );
        assert!(!r.answer.is_empty(), "{q}");
    }
    h.finish().await;
}

/// T10: the sample edges part 3's merge and dedup stages must keep:
/// scrape intervals from 5 to 75 s, a single-sample series, infinities,
/// NaN, stale markers, negative and `-0.` values, and every third series's
/// samples written again in another part. Every op over each function,
/// `sum` and `sum by`: the statement is part 3's, and push on and off
/// answer identically.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sample_edges_answer_identically() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_pushed_rate_p3_t10");
    let (fx, repeats) = sample_edge_corpus(t);
    let h = harness(&db, t, &fx, None).await;
    seed_samples(&h.admin, &repeats).await;
    let half_hour = |step_ms| pulsus_read::MetricQueryParams {
        start_ms: t - 1_800_000,
        end_ms: t,
        step_ms,
    };
    for p in [half_hour(15_000), half_hour(60_000), h.instant()] {
        for func in ["rate", "increase", "irate"] {
            for agg in ["sum", "avg", "count", "min", "max"] {
                for grouping in ["", "by (grp) "] {
                    let q = format!("{agg} {grouping}({func}({SAMPLE_EDGE_METRIC}[5m]))");
                    let (a, b) = h.both(&q, &p).await;
                    assert_by_id_shape(&q, &a);
                    assert_eq!(
                        a.answer, b.answer,
                        "{q} at step {}: the pushed answer differs from the unpushed one",
                        p.step_ms
                    );
                    assert_eq!(a.annotations, b.annotations, "{q}: the annotations differ");
                }
            }
        }
    }
    h.finish().await;
}

/// `rate` over `samples`, ordered by time then value bits, at the step
/// ending `end` with a 5-minute range — the extrapolation every route
/// computes, written out — or `None` with fewer than two samples.
fn expected_rate(samples: &[(i64, f64)], end: i64) -> Option<f64> {
    let range = 300_000i64;
    let w: Vec<(i64, f64)> = samples
        .iter()
        .copied()
        .filter(|(t, _)| *t > end - range && *t <= end)
        .collect();
    if w.len() < 2 {
        return None;
    }
    let (t_f, y_f) = w[0];
    let (t_l, y_l) = w[w.len() - 1];
    let mut result = if y_f < 0.0 {
        (y_l - y_f) + 0.0
    } else {
        y_l - y_f
    };
    for pair in w.windows(2) {
        if pair[1].1 < pair[0].1 {
            result += pair[0].1;
        }
    }
    let n = w.len() as f64;
    let d_start_raw = (t_f - (end - range)) as f64 / 1000.;
    let d_end_raw = (end - t_l) as f64 / 1000.;
    let sampled = (t_l - t_f) as f64 / 1000.;
    let avg_dur = sampled / (n - 1.0);
    let threshold = avg_dur * 1.1;
    let d_start_1 = if d_start_raw >= threshold {
        avg_dur / 2.
    } else {
        d_start_raw
    };
    let d_zero = if result > 0. && y_f >= 0. {
        sampled * (y_f / result)
    } else {
        d_start_1
    };
    let d_start = if d_zero < d_start_1 {
        d_zero
    } else {
        d_start_1
    };
    let d_end = if d_end_raw >= threshold {
        avg_dur / 2.
    } else {
        d_end_raw
    };
    let factor = ((sampled + d_start + d_end) / sampled) / (range as f64 / 1000.);
    Some(result * factor)
}

/// T11: two different values at one millisecond, written in both orders
/// across two parts. The pushed answer is the same in ten runs and is
/// `rate` over the samples ordered by time then value bits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn equal_time_samples_are_ordered_by_value_bits() {
    skip_unless_live!();
    let t = anchor();
    let db = pulsus_testkit::test_db("pulsus_read_it_pushed_rate_p3_t11");
    let (first, second) = equal_time_corpus(t);
    let h = harness(&db, t, &first, None).await;
    seed_samples(&h.admin, &second).await;
    let p = h.ten_minutes(60_000);
    let q = format!("sum by (series) (rate({EQUAL_TIME_METRIC}[5m]))");

    let mut want: Vec<String> = first
        .iter()
        .zip(&second)
        .map(|(a, b)| {
            let mut samples: Vec<(i64, f64)> =
                a.samples.iter().chain(&b.samples).copied().collect();
            samples.sort_by_key(|(t, v)| (*t, v.to_bits()));
            let points: Vec<String> = (0..=(p.end_ms - p.start_ms) / p.step_ms)
                .filter_map(|g| {
                    let end = p.start_ms + g * p.step_ms;
                    expected_rate(&samples, end).map(|v| format!("{end}:{:016x}", v.to_bits()))
                })
                .collect();
            format!("{:?} {}", a.labels, points.join(","))
        })
        .collect();
    want.sort();

    for run in 0..10 {
        let r = Harness::run(&h.pushed, &q, &p).await;
        assert_by_id_shape(&q, &r);
        assert_eq!(r.answer, want, "{q}: run {run}");
    }
    h.finish().await;
}

#[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize, Debug)]
struct CountRow {
    n: u64,
}

/// T12: the time chunks count distinct series, not the ID statement's
/// rows. 1,000 series whose activity is written in separate inserts that
/// are never merged, on both days of a window crossing midnight UTC: four
/// activity rows a series. With the label cache never refreshed, the
/// series count comes from the database; under a cap of 30,500
/// series-steps, 61 steps are 3 statements of 30 steps (4,000 rows would
/// make 9 of 7).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chunk_count_follows_distinct_series() {
    skip_unless_live!();
    use futures::StreamExt;
    use pulsus_clickhouse::{Idempotency, QuerySettings};

    let now = now_ms();
    let mut midnight = now / DAY_MS * DAY_MS;
    if now - midnight < 3_600_000 {
        midnight -= DAY_MS;
    }
    let fx: Vec<SeedSeries> = (0..1_000u128)
        .map(|i| SeedSeries {
            fp: 0x5796_0000_0000_0000_0000_0000_0000_0000 | i,
            metric: SCALE_METRIC.to_string(),
            labels: vec![
                ("cpu".to_string(), (i % 16).to_string()),
                ("series".to_string(), i.to_string()),
            ],
            samples: (0..320i64)
                .map(|k| {
                    (
                        midnight - 2_700_000 + k * SCRAPE_MS + (i as i64 % 900),
                        (k * 3) as f64 + i as f64,
                    )
                })
                .collect(),
            hist: Vec::new(),
        })
        .collect();
    let db = pulsus_testkit::test_db("pulsus_read_it_pushed_rate_p3_t12");
    let (bootstrap, client) = fresh_database(&db).await;
    client
        .execute(
            "SYSTEM STOP MERGES metric_series",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("stop merges");
    let day_before = u16::try_from(midnight / DAY_MS - 1).expect("day");
    let day_after = u16::try_from(midnight / DAY_MS).expect("day");
    seed(&client, &fx, midnight - 600_000).await;
    seed_activity(&client, &fx, &[day_after], (1 << 24) - 1).await;
    seed_activity(&client, &fx, &[day_before, day_after], (1 << 23) | 1).await;
    let mut stream = client
        .query_stream::<CountRow>(
            "SELECT count() AS n FROM metric_series",
            &QuerySettings::new(),
        )
        .await
        .expect("count activity rows");
    let rows = stream.next().await.expect("one row").expect("decode").n;
    drop(stream);
    assert_eq!(rows, 4_000, "four activity rows a series");

    let cold = std::sync::Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    let pushed = MetricsEngine::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cold,
        engine_config(&db, true),
    )
    .with_pushed_series_steps_cap(30_500);
    let p = MetricQueryParams {
        start_ms: midnight - 1_800_000,
        end_ms: midnight + 1_800_000,
        step_ms: 60_000,
    };
    let q = format!("sum(rate({SCALE_METRIC}[5m]))");
    let marker = server_marker(&client).await;
    let r = Harness::run(&pushed, &q, &p).await;
    assert!(!r.answer.is_empty(), "{q}");
    let sent = statements_since(&client, &db, &marker).await;
    let shape_a = sent
        .iter()
        .filter(|s| s.query.contains("AS range_ms"))
        .count();
    assert_eq!(
        shape_a,
        3,
        "1,000 series x 61 steps under a cap of 30,500 is three statements: {:?}",
        sent.iter()
            .map(|s| s.query.chars().take(120).collect::<String>())
            .collect::<Vec<_>>()
    );
    pushed_rate_corpus::drop_database(&bootstrap, &db).await;
}
