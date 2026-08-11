//! Issue #249 — the one measurement the RULING asked for: what withholding
//! the zero-allocation streaming slider actually costs.
//!
//! `slider_safe_fingerprints` is a SOUND OVER-APPROXIMATION. Its
//! minimum-length rule can withhold the slider from a fingerprint that was
//! in fact safe, whenever a selection mixes stream shapes — and that is a
//! performance cliff a user cannot see. The RULING accepted the
//! over-approximation on the condition that the cost be MEASURED once, on a
//! selection where it actually fires, rather than argued.
//!
//! **Print-only, `#[ignore]`d, never a CI gate.** Wall-time assertions do
//! not belong in CI (docs/schemas.md §9 Tier-1); this is the
//! `zz_print_dedup_grouping_timings` pattern verbatim. Zero CI budget.
//!
//! ```text
//! cargo test -p pulsus-read --release --test logql_slider_withholding_timings \
//!     -- --ignored zz_print_mixed_shape_slider_withholding_timings --nocapture
//! ```
//!
//! Protocol, fixed before any number was read: `--release`, 3 warm-up reps
//! discarded, 11 measured reps, the two arms INTERLEAVED (A,B,A,B,…), and
//! per-rep nanoseconds printed for both arms plus min/median/max and the
//! median ratio B/A.
//!
//! - **Arm A** — the fingerprint KEEPS the slider: a single-stream
//!   selection `{app="a"}` over 20 000 metadata-free rows.
//! - **Arm B** — the SAME rows, under the mixed-shape selection
//!   `{app="a"}` + `{app="a", pod="p"}`, which makes the minimum-length
//!   rule withhold the slider from the LONGER stream although nothing can
//!   reach its base label set. `count_over_time(…[5m])` range, step 10s.

use std::collections::HashMap;
use std::time::Instant;

use pulsus_read::logql::rows::{MetricScanRow, StreamMetaRow};
use pulsus_read::logql::{
    ClientAgg, ClientValue, ClientWindow, CompiledPipeline, Direction, Plan, PlanCtx, QueryParams,
    QuerySpec, plan, run_client_agg_rows,
};

const ROWS: usize = 20_000;
const WARMUP: usize = 3;
const REPS: usize = 11;

fn meta_of(streams: &[(u64, &str)]) -> HashMap<u64, StreamMetaRow> {
    streams
        .iter()
        .map(|(fp, labels)| {
            (
                *fp,
                StreamMetaRow {
                    fingerprint: *fp,
                    service: "svc".to_string(),
                    labels: labels.to_string(),
                },
            )
        })
        .collect()
}

/// 20 000 metadata-free rows on the fingerprint under test.
fn rows_on(fp: u64) -> Vec<MetricScanRow> {
    (0..ROWS)
        .map(|i| MetricScanRow {
            fingerprint: fp,
            timestamp_ns: (i as i64) * 1_000_000,
            body: "line".to_string(),
            structured_metadata: String::new(),
        })
        .collect()
}

fn client_and_window() -> (ClientAgg, ClientWindow, Option<u64>) {
    let ctx = PlanCtx {
        db: "pulsus",
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    };
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: 0,
            end_ns: 60_000_000_000,
            step_ns: 10_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let expr = pulsus_logql::parse(r#"count_over_time({app="a"}[5m])"#).expect("parse");
    let Plan::Metric(mp) = plan(&expr, &params, &ctx).expect("plan") else {
        panic!("expected a Metric plan");
    };
    let client = mp.client.clone().expect("client-aggregated");
    let window = match mp.step_ns {
        Some(step_ns) => ClientWindow::Range {
            grid_start_ns: mp.grid_start_ns,
            end_ns: mp.end_ns,
            step_ns,
            range_ns: mp.range_ns,
            offset_ns: mp.offset_ns,
        },
        None => panic!("the fixture is a range query"),
    };
    assert!(matches!(client.value, ClientValue::Count));
    (client, window, mp.rate_window_ns)
}

/// One timing arm: its printed name, the streams the selection resolves
/// to, and which fingerprint the rows sit on.
type Arm = (&'static str, Vec<(u64, &'static str)>, u64);

/// The INSTANT arm's plan: a label filter forces the client path, so this
/// reaches `ClientAggState` rather than the SQL pushdown.
fn instant_client_and_window() -> (ClientAgg, ClientWindow, Option<u64>) {
    let ctx = PlanCtx {
        db: "pulsus",
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    };
    let params = QueryParams {
        spec: QuerySpec::Instant {
            at_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let expr = pulsus_logql::parse(r#"count_over_time({app="a"} | x="1" [5m])"#).expect("parse");
    let Plan::Metric(mp) = plan(&expr, &params, &ctx).expect("plan") else {
        panic!("expected a Metric plan");
    };
    let client = mp
        .client
        .clone()
        .expect("a label filter forces the client path");
    let window = ClientWindow::Instant {
        start_ns: mp.grid_start_ns,
        end_ns: mp.end_ns,
    };
    (client, window, mp.rate_window_ns)
}

fn median(mut v: Vec<u128>) -> u128 {
    v.sort_unstable();
    v[v.len() / 2]
}

/// The three RANGE selection shapes plus one INSTANT arm.
///
/// - **A** single stream `{app="a", x="1"}` — no mixed shape at all, the
///   slider is kept on both trees. Its residual over the pre-#249 tree is
///   the routing discipline's own per-row cost.
/// - **B** `{app="a"}` + `{app="a", pod="p"}`, rows on the LONGER stream.
///   The shorter's key set IS a proper subset, so merging its metadata can
///   genuinely produce the longer's label set: withholding the slider here
///   is REQUIRED for correctness, and no tightening of the safety test
///   could recover it.
/// - **C** `{app="a", x="1"}` + `{app="a", y="1", z="2"}`, rows on the
///   LONGER stream. The shorter's key set is NOT a subset (`x` is absent
///   from the longer) and a merge only ever adds or overwrites keys, so no
///   merge can reach the longer's set: this withholding is the
///   OVER-APPROXIMATION, and is what an exact subset test would recover.
/// - **I** the instant path, with a client-forcing stage so it reaches
///   `ClientAggState` rather than the SQL pushdown.
///
/// Every arm's rows carry NO structured metadata.
#[test]
#[ignore = "generator: prints release-mode slider-withholding timings for the #249 record"]
fn zz_print_mixed_shape_slider_withholding_timings() {
    let (client, window, rate_window_ns) = client_and_window();
    let compiled = CompiledPipeline::compile(&client.pipeline).expect("the pipeline compiles");

    let arms: [Arm; 3] = [
        (
            "A single-shape     ",
            vec![(1, r#"{"app":"a","x":"1"}"#)],
            1,
        ),
        (
            "B mixed, SUBSET    ",
            vec![(1, r#"{"app":"a"}"#), (2, r#"{"app":"a","pod":"p"}"#)],
            2,
        ),
        (
            "C mixed, NON-subset",
            vec![
                (1, r#"{"app":"a","x":"1"}"#),
                (2, r#"{"app":"a","y":"1","z":"2"}"#),
            ],
            2,
        ),
    ];
    let metas: Vec<_> = arms.iter().map(|(_, st, _)| meta_of(st)).collect();
    let rowsets: Vec<_> = arms.iter().map(|(_, _, fp)| rows_on(*fp)).collect();

    // The INSTANT arm: a label filter forces the client path.
    let (i_client, i_window, i_rate) = instant_client_and_window();
    let i_compiled =
        CompiledPipeline::compile(&i_client.pipeline).expect("the instant pipeline compiles");
    let i_meta = meta_of(&[(1, r#"{"app":"a","x":"1"}"#)]);
    let i_rows = rows_on(1);

    let run = |rows: &[MetricScanRow], meta: &HashMap<u64, StreamMetaRow>| -> u128 {
        let t0 = Instant::now();
        let out = run_client_agg_rows(rows, &compiled, meta, &client, window, rate_window_ns)
            .expect("served");
        let dt = t0.elapsed().as_nanos();
        std::hint::black_box(&out);
        dt
    };
    let run_instant = || -> u128 {
        let t0 = Instant::now();
        let out = run_client_agg_rows(&i_rows, &i_compiled, &i_meta, &i_client, i_window, i_rate)
            .expect("served");
        let dt = t0.elapsed().as_nanos();
        std::hint::black_box(&out);
        dt
    };

    for _ in 0..WARMUP {
        for k in 0..arms.len() {
            run(&rowsets[k], &metas[k]);
        }
        run_instant();
    }

    let mut samples: Vec<Vec<u128>> = vec![Vec::new(); arms.len() + 1];
    println!(
        "rep  {}  I instant",
        arms.iter()
            .map(|(n, _, _)| *n)
            .collect::<Vec<_>>()
            .join("  ")
    );
    for r in 0..REPS {
        // INTERLEAVED, so a drifting machine moves every arm together.
        for k in 0..arms.len() {
            let t = run(&rowsets[k], &metas[k]);
            samples[k].push(t);
        }
        samples[arms.len()].push(run_instant());
        let row: Vec<String> = samples.iter().map(|v| format!("{:>10}", v[r])).collect();
        println!("{:>3}  {}", r + 1, row.join("  "));
    }
    for (k, name) in arms
        .iter()
        .map(|(n, _, _)| *n)
        .chain(std::iter::once("I instant         "))
        .enumerate()
    {
        let v = samples[k].clone();
        println!(
            "{name}  min {:>10}  median {:>10}  max {:>10}",
            v.iter().min().unwrap(),
            median(v.clone()),
            v.iter().max().unwrap()
        );
    }
    println!("rows per arm: {ROWS}; warm-up reps discarded: {WARMUP}; measured reps: {REPS}");
}
