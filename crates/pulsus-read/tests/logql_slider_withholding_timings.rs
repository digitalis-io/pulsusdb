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

fn median(mut v: Vec<u128>) -> u128 {
    v.sort_unstable();
    v[v.len() / 2]
}

#[test]
#[ignore = "generator: prints release-mode slider-withholding timings for the #249 record"]
fn zz_print_mixed_shape_slider_withholding_timings() {
    let (client, window, rate_window_ns) = client_and_window();
    let compiled = CompiledPipeline::compile(&client.pipeline).expect("the pipeline compiles");

    // Arm A: single-stream selection -> fp 1 is the minimum length, keeps
    // the slider.
    let meta_a = meta_of(&[(1, r#"{"app":"a"}"#)]);
    // Arm B: the SAME rows on fp 2, co-selected with a SHORTER stream, so
    // the minimum-length rule withholds the slider from fp 2.
    let meta_b = meta_of(&[(1, r#"{"app":"a"}"#), (2, r#"{"app":"a","pod":"p"}"#)]);
    let rows_a = rows_on(1);
    let rows_b = rows_on(2);

    let run = |rows: &[MetricScanRow], meta: &HashMap<u64, StreamMetaRow>| -> u128 {
        let t0 = Instant::now();
        let out = run_client_agg_rows(rows, &compiled, meta, &client, window, rate_window_ns)
            .expect("served");
        let dt = t0.elapsed().as_nanos();
        std::hint::black_box(&out);
        dt
    };

    for _ in 0..WARMUP {
        run(&rows_a, &meta_a);
        run(&rows_b, &meta_b);
    }

    let mut a: Vec<u128> = Vec::with_capacity(REPS);
    let mut b: Vec<u128> = Vec::with_capacity(REPS);
    println!("rep  A_ns(slider kept)  B_ns(slider withheld)");
    for i in 0..REPS {
        // INTERLEAVED, so a drifting machine moves both arms together.
        let ta = run(&rows_a, &meta_a);
        let tb = run(&rows_b, &meta_b);
        a.push(ta);
        b.push(tb);
        println!("{:>3}  {ta:>17}  {tb:>21}", i + 1);
    }
    let (amin, amax) = (*a.iter().min().unwrap(), *a.iter().max().unwrap());
    let (bmin, bmax) = (*b.iter().min().unwrap(), *b.iter().max().unwrap());
    let (amed, bmed) = (median(a), median(b));
    println!("A  min {amin}  median {amed}  max {amax}");
    println!("B  min {bmin}  median {bmed}  max {bmax}");
    println!("median ratio B/A = {:.3}", bmed as f64 / amed as f64);
    println!("rows per arm: {ROWS}; warm-up reps discarded: {WARMUP}; measured reps: {REPS}");
}
