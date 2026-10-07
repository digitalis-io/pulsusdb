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

#[path = "pushed_rate_corpus/mod.rs"]
mod pushed_rate_corpus;

use std::sync::Arc;

use pulsus_clickhouse::ChClient;
use pulsus_read::{LabelCache, MetricQueryParams, MetricsEngine};
use pushed_rate_corpus::{
    CPU_METRIC, DAY_MS, EDGE_METRIC, HIST_ONLY_METRIC, Harness, ISSUE_QUERY, MIXED_METRIC, Routed,
    anchor, cache_config, cpu_corpus, edge_corpus, engine_config, harness, histogram_corpora,
    pushed_declines, pushed_statements, server_marker, statements_since, test_config,
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
