//! Issue #548, check 1: **every statement this engine renders for the
//! thirty queries below, captured at the merge base `8f3348e8`.**
//!
//! # What this freeze is, and what it is not
//!
//! **It is an external audit anchor, not a self-check.** Its digest was
//! published on the issue before the code existed, and a reader
//! re-derives it by running [`render`] against `8f3348e8` in a worktree
//! of their own. That protection is exercised by a person; it is worth
//! exactly as much as that comparison.
//!
//! What it is NOT: a check that a coordinated edit of the generator, the
//! golden and the digest is impossible. It is not. Measured at the base,
//! three probes on the generator:
//!
//! ```text
//!   generator edit                      digest      lines   bytes    entries
//!   STEP_MS   60,000 -> 30,000          87c1641f     510    26,727     30
//!   START_MS  moved one minute later    c278302d     510    26,727     30
//!   one query swapped for another       2568fffd     523    27,506     30
//!   one query dropped (time())          a1cb6b7e     506    26,636     29
//! ```
//!
//! [`the_freeze_has_the_published_entry_line_and_byte_counts`] catches
//! the last two — an edit that changes the entry count or the rendered
//! size. Nothing mechanical catches the first two, and the three
//! constants it asserts are published on the issue so that silencing it
//! means editing three numbers a reader can check.
//!
//! **And it is a characterization of two functions, not of what the
//! server sends.** [`render`] calls `pulsus_promql::plan` and
//! `metrics::sample_sql` directly. Measured at the base: inserting
//! `let lower_excl = lower_excl + 1;` after `sel.fetch_window(..)` in
//! `crates/pulsus-read/src/metrics/exec.rs` shifts every fetch window the
//! engine sends and leaves this digest at `1ae98d41…` with all of
//! `pulsus-read --lib` green. That hole is what
//! `tests/live_metrics_plan_parts.rs` exists for: it reads the statements
//! back out of `system.query_log`.

use sha2::{Digest, Sha256};

use pulsus_promql::{DEFAULT_LOOKBACK_MS, PlanParams, parse, plan};
use pulsus_read::metrics::sample_sql;

const GOLDEN: &str = include_str!("golden/promql_statements.txt");
const PINNED: &str = include_str!("golden/promql_statements.sha256");

/// The three constants published on issue #548 before the code existed.
const ENTRIES: usize = 30;
const LINES: usize = 510;
const BYTES: usize = 26_727;

const START_MS: i64 = 1_782_907_200_000;
const END_MS: i64 = 1_782_928_800_000;
const STEP_MS: i64 = 60_000;
const FPS: [u64; 3] = [101, 205, 990];
const SAMPLES: &str = "metric_samples";
const HIST: &str = "metric_hist_samples";

fn names() -> Vec<String> {
    vec![
        "http_requests_total".to_string(),
        "http_errors_total".to_string(),
    ]
}

/// (query, instant?)
#[rustfmt::skip]
const QUERIES: &[(&str, bool)] = &[
    ("http_requests_total{status=\"500\"}", false),
    ("abs(http_requests_total{status=\"500\"})", false),
    ("max by (status) (http_requests_total{status=\"500\"})", false),
    ("min without (instance) (http_requests_total{status=\"500\"})", false),
    ("count(http_requests_total{status=\"500\"})", false),
    ("group(http_requests_total{status=\"500\"})", false),
    ("topk(3, http_requests_total{status=\"500\"})", false),
    ("count_values(\"v\", http_requests_total{status=\"500\"})", false),
    ("stddev by (status) (http_requests_total{status=\"500\"})", false),
    ("histogram_quantile(0.5, http_requests_total{status=\"500\"})", false),
    ("label_replace(http_requests_total{status=\"500\"}, \"l\", \"$1\", \"status\", \"(.*)\")", false),
    ("timestamp(http_requests_total{status=\"500\"})", false),
    ("absent(http_requests_total{status=\"500\"})", false),
    ("sort_desc(http_requests_total{status=\"500\"})", true),
    ("rate(http_requests_total{status=\"500\"}[5m])", false),
    ("sum by (status) (rate(http_requests_total{status=\"500\"}[5m]))", false),
    ("max_over_time(http_requests_total{status=\"500\"}[1m])", false),
    ("quantile_over_time(0.5, http_requests_total{status=\"500\"}[10m])", false),
    ("avg_over_time(http_requests_total{status=\"500\"}[5m] offset 1h)", false),
    ("sum_over_time(http_requests_total{status=\"500\"}[5m] @ 1782900000)", false),
    ("rate(http_requests_total{status=\"500\"}[5m:1m])", false),
    ("http_requests_total{status=\"500\"}[5m]", true),
    ("time()", false),
    ("vector(1)", false),
    ("pi()", false),
    ("info(http_requests_total{status=\"500\"})", false),
    ("rate(http_requests_total{status=\"500\"}[5m]) / on (instance) rate(http_errors_total{status=\"500\"}[5m])", false),
    ("http_requests_total{status=\"500\"} and http_requests_total{status=\"200\"}", false),
    ("http_requests_total{status=\"500\"} * 100", false),
    ("{__name__=~\"http_.*\",status=\"500\"}", false),
];

fn render() -> String {
    let mut out = String::new();
    for (q, instant) in QUERIES {
        let params = PlanParams {
            start_ms: START_MS,
            end_ms: if *instant { START_MS } else { END_MS },
            step_ms: if *instant { 0 } else { STEP_MS },
            lookback_ms: DEFAULT_LOOKBACK_MS,
            experimental_functions: true,
        };
        out.push_str(&format!("== {q} ==\n"));
        out.push_str(&format!(
            "params start_ms={} end_ms={} step_ms={}\n",
            params.start_ms, params.end_ms, params.step_ms
        ));
        let p = plan(&parse(q).expect("parse"), params).expect("plan");
        out.push_str(&format!("selectors {}\n", p.selectors.len()));
        for (i, sel) in p.selectors.iter().enumerate() {
            let (lo, hi) = sel.fetch_window(&params);
            out.push_str(&format!(
                "-- selector[{i}] name={} range_ms={} window=({lo}, {hi}]\n",
                sel.metric_name.as_deref().unwrap_or("<none>"),
                sel.range_ms
                    .map_or_else(|| "-".to_string(), |r| r.to_string()),
            ));
            match &sel.metric_name {
                Some(n) => {
                    out.push_str(&sample_sql::sample_fetch(SAMPLES, n, &FPS, lo, hi));
                    out.push('\n');
                    out.push_str(&sample_sql::hist_sample_fetch(HIST, n, &FPS, lo, hi));
                    out.push('\n');
                }
                None => {
                    out.push_str(&sample_sql::sample_fetch_multi(
                        SAMPLES,
                        &names(),
                        &FPS,
                        lo,
                        hi,
                    ));
                    out.push('\n');
                    out.push_str(&sample_sql::hist_sample_fetch_multi(
                        HIST,
                        &names(),
                        &FPS,
                        lo,
                        hi,
                    ));
                    out.push('\n');
                }
            }
        }
        out.push('\n');
    }
    out
}

/// Criterion 1, half one: the committed golden is the file whose digest
/// was published on the issue before any of this code existed.
#[test]
fn the_promql_statement_freeze_matches_its_committed_digest() {
    let digest = Sha256::digest(GOLDEN.as_bytes());
    assert_eq!(
        format!("{digest:x}"),
        PINNED.trim(),
        "crates/pulsus-read/tests/golden/promql_statements.txt was edited without its digest \
         (issue #548 criterion 1). The digest published on the issue is \
         1ae98d4165013f726354bc462da2e8df7c470874b0908e26a26fb56bff725d08, re-derivable by \
         running this file's `render` against 8f3348e8."
    );
}

/// Criterion 1, half two: the planner and the statement builders still
/// render, byte for byte, what they rendered at the merge base.
#[test]
fn every_statement_is_the_one_the_merge_base_rendered() {
    let rendered = render();
    if rendered != GOLDEN {
        let (a, b) = first_difference(&rendered, GOLDEN);
        panic!(
            "a statement moved from the one the merge base rendered (issue #548 criterion 1).\n\
             rendered: {a:?}\n  golden: {b:?}\n\
             This golden is a capture of 8f3348e8's output; regenerating it against the changed \
             tree moves the digest away from a number already published."
        );
    }
}

/// Criterion 1a: the entry, line and byte counts published on the issue.
///
/// A count and two sizes — exact content is criterion 1's business, and
/// criterion 1 is an external audit rather than a self-check.
#[test]
fn the_freeze_has_the_published_entry_line_and_byte_counts() {
    assert_eq!(QUERIES.len(), ENTRIES, "the generator's own query list");
    assert_eq!(
        GOLDEN.matches("== ").count(),
        ENTRIES,
        "the golden's `== ` entry count"
    );
    assert_eq!(GOLDEN.lines().count(), LINES, "the golden's line count");
    assert_eq!(GOLDEN.len(), BYTES, "the golden's byte length");
}

/// The first line the two texts disagree on, so a failure names the byte
/// that moved rather than printing 26,727 of them.
fn first_difference<'a>(a: &'a str, b: &'a str) -> (&'a str, &'a str) {
    for (x, y) in a.lines().zip(b.lines()) {
        if x != y {
            return (x, y);
        }
    }
    ("<one text is a prefix of the other>", "")
}

/// Writes the golden and its digest. `#[ignore]`d: it is the capture
/// step, run once at the merge base, never as part of a suite.
#[test]
#[ignore = "regenerates the golden; run only to capture at a named commit"]
fn zz_regenerate_golden() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let body = render();
    let digest = Sha256::digest(body.as_bytes());
    std::fs::write(dir.join("promql_statements.txt"), &body).expect("write golden");
    std::fs::write(
        dir.join("promql_statements.sha256"),
        format!("{digest:x}\n"),
    )
    .expect("write digest");
}
