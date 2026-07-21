//! CH-free wall-time A/B measurement for the range vector-vector binop
//! reclaim (issue #93, finding 1). `#[ignore]`-by-default — it asserts
//! NOTHING about wall time (the methodology bar: wall-time never enters
//! CI) and is run explicitly:
//!
//! ```text
//! cargo test -p pulsus-promql --test binop_range_bench -- --ignored --nocapture
//! ```
//!
//! Structure follows the alloc-gate pattern (in-memory `SeriesData`, no
//! ClickHouse, no criterion dep — `std::time::Instant` + the test harness
//! suffice) and the #16/#34/#35 methodology: warmup reps discarded, then
//! timed reps with **interleaved / rotated A/B ordering** (A and B
//! alternate per rep so scheduler drift biases neither), and **per-rep
//! distributions** are printed, not scalars. The recorded output is
//! transcribed into `docs/benchmarks/metrics-read-path.md`.
//!
//! Two measurements:
//!   1. `dedup_hotspot_ab` — an in-binary interleaved A/B of the EXACT
//!      operation the fix changed: the many-to-one duplicate-detection set
//!      rebuilt per step. A = pre-#93 (clone the full `(Labels,
//!      Option<String>)` identity into a `HashSet`); B = post-#93 (insert
//!      the identity's hash into a `HashSet<u64>`). This isolates the
//!      reclaimed hotspot the profile pinned.
//!   2. `evaluate_range_shapes` — end-to-end `evaluate()` wall time for the
//!      pinned `group_right` range and `count_values` range shapes,
//!      measuring whatever tree it is built against (the doc records a
//!      pre-fix and a post-fix run).

use std::collections::HashSet;
use std::time::{Duration, Instant};

use pulsus_promql::{
    FetchedSeries, Labels, PlanParams, QueryPlan, Sample, SeriesData, evaluate, parse, plan,
};

const STEPS: i64 = 200;
const STEP_MS: i64 = 15_000;

fn identity_hash<T: std::hash::Hash>(v: &T) -> u64 {
    use std::hash::Hasher;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

/// One rep's timing pair, kept per-rep (never pre-averaged).
struct Rep {
    a: Duration,
    b: Duration,
}

/// Prints a per-rep distribution table + summary (min / median / p90 /
/// max) for the A and B sample sets — no averaging into a single scalar.
fn report(name: &str, reps: &[Rep]) {
    let mut a: Vec<u128> = reps.iter().map(|r| r.a.as_nanos()).collect();
    let mut b: Vec<u128> = reps.iter().map(|r| r.b.as_nanos()).collect();
    a.sort_unstable();
    b.sort_unstable();
    let stat = |v: &[u128]| -> (u128, u128, u128, u128) {
        let n = v.len();
        (v[0], v[n / 2], v[(n * 9) / 10], v[n - 1])
    };
    let (amin, amed, ap90, amax) = stat(&a);
    let (bmin, bmed, bp90, bmax) = stat(&b);
    println!("\n== {name} (n={} reps, ns) ==", reps.len());
    println!("        min      median   p90      max");
    println!("A(base) {amin:<8} {amed:<8} {ap90:<8} {amax}");
    println!("B(opt)  {bmin:<8} {bmed:<8} {bp90:<8} {bmax}");
    println!(
        "median B/A = {:.3} ({:.1}% faster)",
        bmed as f64 / amed as f64,
        (1.0 - bmed as f64 / amed as f64) * 100.0
    );
    println!("per-rep A ns: {a:?}");
    println!("per-rep B ns: {b:?}");
}

/// The output identities of one many-side step vector (bar-like, 3
/// labels), for the dedup microbench.
fn step_identities(n: usize) -> Vec<(Labels, Option<String>)> {
    (0..n)
        .map(|i| {
            (
                Labels::new([
                    ("g".to_string(), format!("g{}", i % 4)),
                    ("inst".to_string(), format!("i{i}")),
                    ("region".to_string(), "us-east-1".to_string()),
                ]),
                Some("bar".to_string()),
            )
        })
        .collect()
}

#[test]
#[ignore = "wall-time measurement; run explicitly with --ignored (never in CI)"]
fn dedup_hotspot_ab() {
    const IDENTITIES: usize = 32; // many-side series per step
    const WARMUP: usize = 30;
    const REPS: usize = 60;
    let ids = step_identities(IDENTITIES);

    // A = clone the full identity into the set every step (pre-#93 shape).
    let run_a = || {
        let mut sink = 0usize;
        for _ in 0..STEPS {
            let mut set: HashSet<(Labels, Option<String>)> = HashSet::with_capacity(IDENTITIES);
            for (labels, name) in &ids {
                set.insert((labels.clone(), name.clone()));
            }
            sink += set.len();
        }
        std::hint::black_box(sink)
    };
    // B = insert the identity hash (post-#93 shape).
    let run_b = || {
        let mut sink = 0usize;
        for _ in 0..STEPS {
            let mut set: HashSet<u64> = HashSet::with_capacity(IDENTITIES);
            for (labels, name) in &ids {
                set.insert(identity_hash(&(labels, name)));
            }
            sink += set.len();
        }
        std::hint::black_box(sink)
    };

    for _ in 0..WARMUP {
        std::hint::black_box(run_a());
        std::hint::black_box(run_b());
    }
    let mut reps = Vec::with_capacity(REPS);
    for i in 0..REPS {
        // Rotate order per rep so neither side is systematically first.
        let (a, b) = if i % 2 == 0 {
            let ta = time(&run_a);
            let tb = time(&run_b);
            (ta, tb)
        } else {
            let tb = time(&run_b);
            let ta = time(&run_a);
            (ta, tb)
        };
        reps.push(Rep { a, b });
    }
    report(
        "dedup_hotspot (per-step dup-detection set, 32 ids × 200 steps)",
        &reps,
    );
}

fn time<F: Fn() -> usize>(f: &F) -> Duration {
    let start = Instant::now();
    std::hint::black_box(f());
    start.elapsed()
}

/// End-to-end `evaluate()` wall time for the two pinned range shapes —
/// measures the tree it is compiled against (the doc records a pre-fix and
/// a post-fix run). Not an A/B within one binary; the per-rep distribution
/// is the recorded evidence.
#[test]
#[ignore = "wall-time measurement; run explicitly with --ignored (never in CI)"]
fn evaluate_range_shapes() {
    const WARMUP: usize = 20;
    const REPS: usize = 40;
    for (name, expr_src, build) in [
        (
            "group_right range",
            "foo / on(g) group_right bar",
            build_group_right as fn() -> (QueryPlan, SeriesData),
        ),
        (
            "count_values range",
            r#"count_values("v", bar)"#,
            build_count_values as fn() -> (QueryPlan, SeriesData),
        ),
    ] {
        let (qp, data) = build();
        // Sanity: prove the shape actually evaluates.
        let _ = parse(expr_src).expect("parse");
        for _ in 0..WARMUP {
            std::hint::black_box(evaluate(&qp, &data).expect("evaluate"));
        }
        let mut ns: Vec<u128> = Vec::with_capacity(REPS);
        for _ in 0..REPS {
            let start = Instant::now();
            let out = evaluate(&qp, &data).expect("evaluate");
            ns.push(start.elapsed().as_nanos());
            std::hint::black_box(out);
        }
        ns.sort_unstable();
        let n = ns.len();
        println!(
            "\n== evaluate {name} (n={n}) == min={} median={} p90={} max={} ns",
            ns[0],
            ns[n / 2],
            ns[(n * 9) / 10],
            ns[n - 1]
        );
        println!("per-rep ns: {ns:?}");
    }
}

fn build_group_right() -> (QueryPlan, SeriesData) {
    const GROUPS: usize = 4;
    const MANY: usize = 8;
    let params = range_params();
    let qp = plan(&parse("foo / on(g) group_right bar").unwrap(), params).unwrap();
    let mut data = SeriesData::new();
    for sel in &qp.selectors {
        let name = sel.metric_name.clone().unwrap();
        let mut series = Vec::new();
        if name == "foo" {
            for g in 0..GROUPS {
                series.push(fs(
                    g as u64,
                    "foo",
                    Labels::new([("g".to_string(), format!("g{g}"))]),
                    1.0,
                ));
            }
        } else {
            let mut fp = 100_000u64;
            for g in 0..GROUPS {
                for m in 0..MANY {
                    series.push(fs(
                        fp,
                        "bar",
                        Labels::new([
                            ("g".to_string(), format!("g{g}")),
                            ("inst".to_string(), format!("i{m}")),
                            ("region".to_string(), "us-east-1".to_string()),
                        ]),
                        2.0,
                    ));
                    fp += 1;
                }
            }
        }
        data.insert(sel.id, series);
    }
    (qp, data)
}

fn build_count_values() -> (QueryPlan, SeriesData) {
    const SERIES: usize = 32;
    let params = range_params();
    let qp = plan(&parse(r#"count_values("v", bar)"#).unwrap(), params).unwrap();
    let mut data = SeriesData::new();
    for sel in &qp.selectors {
        let mut series = Vec::new();
        for i in 0..SERIES {
            series.push(fs(
                i as u64,
                "bar",
                Labels::new([("inst".to_string(), format!("i{i}"))]),
                (i % 5) as f64,
            ));
        }
        data.insert(sel.id, series);
    }
    (qp, data)
}

fn range_params() -> PlanParams {
    PlanParams {
        start_ms: 0,
        end_ms: (STEPS - 1) * STEP_MS,
        step_ms: STEP_MS,
        lookback_ms: pulsus_promql::DEFAULT_LOOKBACK_MS,
        experimental_functions: false,
    }
}

fn fs(fp: u64, name: &str, labels: Labels, base: f64) -> FetchedSeries {
    FetchedSeries {
        fingerprint: fp,
        metric_name: Some(name.to_string()),
        labels,
        samples: (0..STEPS)
            .map(|k| Sample::float(k * STEP_MS, base + k as f64))
            .collect(),
        start_ts: None,
    }
}
