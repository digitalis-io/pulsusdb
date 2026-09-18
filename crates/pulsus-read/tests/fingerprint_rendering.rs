//! Issue #498 criterion 4b, the output-checked leg: **every site that puts
//! a fingerprint into SQL renders the exact call form.**
//!
//! The sealed type (`pulsus_model::Fingerprint`) makes the accidental
//! spelling a compile error, and the typed signatures make a raw identity
//! unable to reach a builder. Neither says what a builder actually emits:
//! a site whose signature still reads `FpLiteral` but which stopped using
//! it would pass both. So each of the six sites is driven with the four
//! boundary values and its rendered text is read.
//!
//! **The four values are where the two readings first disagree**, measured
//! on ClickHouse 26.3.29.7:
//!
//! ```text
//!   18446744073709551615   2^64 - 1   the last value a bare literal reads exactly
//!   18446744073709551616   2^64       the first Float64, and what 2^64+1 rounds ONTO
//!   18446744073709551617   2^64 + 1   the value a bare literal loses
//!   18446744073709551618   2^64 + 2   its neighbour
//! ```
//!
//! A test built on the first two alone passes on a build that renders bare
//! decimals, which is why all four are used at every site.
//!
//! **The six sites, by enclosing function** — the closed inventory issue
//! #498 froze, derived by searching for the rendering rather than for
//! callers of a helper:
//!
//! ```text
//!   1  fp_list                        logql/sql.rs        private, reached through stage2
//!   2  stage3_keyset                  logql/sql.rs        the keyset tuple's middle term
//!   3  fingerprint_test               logql/predicate.rs  private, reached through
//!                                                           metadata_string_filter
//!   4  render_fingerprint_list        metrics/sample_sql.rs
//!   5  series_labels_by_fingerprint   metrics/sql.rs
//!   6  discovery_fetch_multi          metrics/sql.rs
//! ```
//!
//! `metrics/sample_sql.rs`'s `fingerprints_predicate` and
//! `metrics/compile.rs` consume site 4's text, and `grouped_sql.rs` embeds
//! it; they are not separate sites and are covered here through it.

use pulsus_model::{Fingerprint, FpLiteral};
use pulsus_read::logql::Direction;
use pulsus_read::logql::predicate::{self, MetadataNameClasses};
use pulsus_read::logql::sql::{self, KeysetLower, TimeWindow};
use pulsus_read::metrics::matcher::{DataWindow, DiscoveryFilter};
use pulsus_read::metrics::{sample_sql, sql as metrics_sql};

/// `2^64-1`, `2^64`, `2^64+1`, `2^64+2`.
const BOUNDARY: [u128; 4] = [
    18_446_744_073_709_551_615,
    18_446_744_073_709_551_616,
    18_446_744_073_709_551_617,
    18_446_744_073_709_551_618,
];

fn literals() -> Vec<FpLiteral> {
    BOUNDARY
        .iter()
        .map(|v| Fingerprint::from_raw(*v).sql_literal())
        .collect()
}

/// No boundary value appears written bare. `toUInt128('<decimal>')` is
/// the only accepted spelling, so an occurrence whose preceding byte is
/// not the opening quote is a bare literal.
fn assert_no_boundary_value_is_written_bare(site: &str, sql: &str) {
    for v in BOUNDARY {
        let decimal = v.to_string();
        let mut from = 0usize;
        while let Some(at) = sql[from..].find(&decimal) {
            let at = from + at;
            let before = sql[..at].chars().next_back();
            assert_eq!(
                before,
                Some('\''),
                "{site}: {decimal} appears at byte {at} outside a toUInt128('…') call\n{sql}"
            );
            from = at + decimal.len();
        }
    }
}

/// Every boundary value, spelled the one exact way, present in `sql` — and
/// none of them written bare anywhere in it.
fn assert_every_boundary_value_is_an_exact_call(site: &str, sql: &str) {
    for v in BOUNDARY {
        let exact = format!("toUInt128('{v}')");
        assert!(
            sql.contains(&exact),
            "{site}: the rendered statement does not carry {exact}\n{sql}"
        );
    }
    assert_no_boundary_value_is_written_bare(site, sql);
}

const WINDOW: TimeWindow = TimeWindow {
    start_ns: 1_700_000_000_000_000_000,
    end_ns: 1_700_003_600_000_000_000,
};

/// Site 1: `fp_list`, reached through the stage-2 hydration builder. It is
/// private, and the sixteen call sites in its own file all render through
/// it, so one of them is enough to read its output.
#[test]
fn site_1_fp_list_renders_the_exact_call_form() {
    let sql = sql::stage2("log_streams", &literals());
    assert_every_boundary_value_is_an_exact_call("fp_list (through stage2)", &sql);
}

/// Site 2: the keyset tuple's middle term. This is the site a bare literal
/// damages differently from the others — a tuple comparison at a rounded
/// boundary re-delivers the boundary row walking forward and skips it
/// walking back, rather than emptying the result.
#[test]
fn site_2_stage3_keyset_renders_the_exact_call_form_in_its_tuple() {
    for value in BOUNDARY {
        let sql = sql::stage3_keyset(
            "log_samples",
            &[predicate::literal("checkout")],
            &literals(),
            WINDOW,
            KeysetLower::After {
                tuple: (
                    1_700_000_000_000_000_001,
                    Fingerprint::from_raw(value).sql_literal(),
                    42,
                ),
                offset: 3,
            },
            Direction::Forward,
            &[],
            100,
        );
        assert!(
            sql.contains(&format!(
                "(timestamp_ns, fingerprint, cityHash64(body)) >= (1700000000000000001, \
                 toUInt128('{value}'), 42)"
            )),
            "stage3_keyset: the tuple does not carry toUInt128('{value}')\n{sql}"
        );
        assert_every_boundary_value_is_an_exact_call("stage3_keyset", &sql);
    }
}

/// Site 3: `fingerprint_test`, reached through the structured-metadata
/// label filter, whose class lists are the fingerprints it renders.
///
/// One value at a time, each in turn the stream-label class: that arm is
/// membership alone, so it lists its class and nothing else. With all four
/// in one class the renderer picks the encoding whose complement arm lists
/// no fingerprint at all, and the filter would carry none of them — which
/// is why the partition is varied rather than the list.
#[test]
fn site_3_fingerprint_test_renders_the_exact_call_form() {
    for (i, value) in BOUNDARY.iter().enumerate() {
        let one = [Fingerprint::from_raw(*value).sql_literal()];
        let rest: Vec<FpLiteral> = BOUNDARY
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, v)| Fingerprint::from_raw(*v).sql_literal())
            .collect();
        let selected: Vec<FpLiteral> = one.iter().chain(rest.iter()).copied().collect();
        let (fragment, _) = predicate::metadata_string_filter(
            "trace_id",
            pulsus_logql::MatchOp::Eq,
            "abc",
            MetadataNameClasses {
                selected: &selected,
                stream_label: &one,
                stream_label_true: &one,
                unsuffixed: &[],
                direct: &rest,
                base_name: None,
            },
            usize::MAX,
        )
        .expect("the two-class shape renders");
        assert!(
            fragment.as_sql().contains(&format!("toUInt128('{value}')")),
            "fingerprint_test: the filter does not carry toUInt128('{value}')\n{}",
            fragment.as_sql()
        );
        assert_no_boundary_value_is_written_bare(
            "fingerprint_test (through metadata_string_filter)",
            fragment.as_sql(),
        );
    }
}

/// Site 4: `render_fingerprint_list`, read directly — it is `pub` because
/// the cost model is bounded against what it renders.
#[test]
fn site_4_render_fingerprint_list_renders_the_exact_call_form() {
    let rendered = sample_sql::render_fingerprint_list(&literals());
    assert_every_boundary_value_is_an_exact_call("render_fingerprint_list", &rendered);
    // And the statement that embeds it, so the list reaches SQL whole.
    let sql = sample_sql::sample_fetch("metric_samples", "up", &literals(), 0, 100);
    assert_every_boundary_value_is_an_exact_call("sample_fetch", &sql);
}

/// Site 5: `series_labels_by_fingerprint`.
#[test]
fn site_5_series_labels_by_fingerprint_renders_the_exact_call_form() {
    let sql = metrics_sql::series_labels_by_fingerprint("metric_series", "up", &literals());
    assert_every_boundary_value_is_an_exact_call("series_labels_by_fingerprint", &sql);
}

/// Site 6: `discovery_fetch_multi`.
#[test]
fn site_6_discovery_fetch_multi_renders_the_exact_call_form() {
    let sql = metrics_sql::discovery_fetch_multi(
        "metric_series",
        &["up".to_string()],
        &literals(),
        DataWindow {
            start_ms: 0,
            end_ms: 100,
        },
        3_600_000,
    );
    assert_every_boundary_value_is_an_exact_call("discovery_fetch_multi", &sql);
    let _ = DiscoveryFilter::default();
}
