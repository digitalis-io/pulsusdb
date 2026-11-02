//! Issue #498 criterion 4b, the output-checked leg: **every site that puts
//! a fingerprint into SQL renders an exact form.**
//!
//! The sealed type (`pulsus_model::Fingerprint`) makes the accidental
//! spelling a compile error, and the typed signatures make a raw identity
//! unable to reach a builder. Neither says what a builder actually emits:
//! a site whose signature still reads `FpLiteral` but which stopped using
//! it would pass both. So each site is driven with the four boundary values
//! and its rendered text is read.
//!
//! **The three exact forms** (issue #399): a set on the right of `IN` is
//! `'<decimal>', …` (`pulsus_model::FpSet`), which the server converts to
//! the `UInt128` column type exactly and folds into one parse-tree node; a
//! typed array is `CAST(['<decimal>', …], 'Array(UInt128)')`
//! (`pulsus_model::FpArray`); a single value is `toUInt128('<decimal>')`.
//! What none of them may be is a bare decimal.
//!
//! **The four values are where a bare decimal and an exact form first
//! disagree**, measured on ClickHouse 26.3.29.7:
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
//! **The sites, by enclosing function** — the closed inventory issue #498
//! froze, as issues #623 and #399 left it, derived by searching for the
//! rendering rather than for callers of a helper:
//!
//! ```text
//!   1  fp_list                        logql/sql.rs        private, reached through stage2
//!   2  stage3_keyset_with_rx          logql/sql.rs        the keyset tuple's middle term;
//!                                                           `stage3_keyset` is it without
//!                                                           the `rx` column (issue #624)
//!   3  fingerprint_test               logql/predicate.rs  private, reached through
//!                                                           metadata_string_filter
//!   4  render_fingerprint_list        metrics/sample_sql.rs
//!   5  ids_scope                      metrics/sql.rs      private, reached through
//!                                                           discovery_fetch_multi
//!   6  render_fingerprint_array       metrics/sample_sql.rs
//!   7  fp_array                       logql/sql.rs        private; its pub builders need
//!                                                           a lowered unwrap, so its case
//!                                                           is a unit test beside it
//! ```
//!
//! `metrics/sample_sql.rs`'s `fingerprints_predicate` and
//! `metrics/compile.rs` consume site 4's text, and `grouped_sql.rs` embeds
//! site 6's; they are not separate sites and are covered here through them.

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

/// No boundary value appears written bare: every occurrence's preceding
/// byte is the opening quote of `'<decimal>'`, alone or inside a call.
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
                "{site}: {decimal} appears at byte {at} outside a quoted literal\n{sql}"
            );
            from = at + decimal.len();
        }
    }
}

/// The four boundary values as the set form renders them, in order.
fn set_form() -> String {
    BOUNDARY
        .iter()
        .map(|v| format!("'{v}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Every boundary value is in `sql` in the set form, none in the call
/// form, and none written bare.
fn assert_every_boundary_value_is_in_the_set_form(site: &str, sql: &str) {
    assert!(
        sql.contains(&set_form()),
        "{site}: the rendered statement does not carry the set {}\n{sql}",
        set_form()
    );
    for v in BOUNDARY {
        let call = format!("toUInt128('{v}')");
        assert!(
            !sql.contains(&call),
            "{site}: a set element is rendered as the call {call}\n{sql}"
        );
    }
    assert_no_boundary_value_is_written_bare(site, sql);
}

/// The four boundary values in the typed array form, and none written bare.
fn assert_every_boundary_value_is_in_the_array_form(site: &str, sql: &str) {
    let array = format!("CAST([{}], 'Array(UInt128)')", set_form());
    assert!(
        sql.contains(&array),
        "{site}: the rendered text does not carry {array}\n{sql}"
    );
    assert_no_boundary_value_is_written_bare(site, sql);
}

const WINDOW: TimeWindow = TimeWindow {
    start_ns: 1_700_000_000_000_000_000,
    end_ns: 1_700_003_600_000_000_000,
};

/// Site 1: `fp_list`, reached through the stage-2 hydration builder. It is
/// private, and the call sites in its own file all render through it, so
/// one of them is enough to read its output.
#[test]
fn site_1_fp_list_renders_the_set_form() {
    let sql = sql::stage2(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        "log_streams",
        &literals(),
    );
    assert_every_boundary_value_is_in_the_set_form("fp_list (through stage2)", &sql);
}

/// Site 2: the keyset tuple's middle term, a single value and so the call
/// form. This is the site a bare literal damages differently from the
/// others — a tuple comparison at a rounded boundary re-delivers the
/// boundary row walking forward and skips it walking back, rather than
/// emptying the result. The statement's own list is the set form.
#[test]
fn site_2_stage3_keyset_renders_the_exact_call_form_in_its_tuple() {
    for value in BOUNDARY {
        let sql = sql::stage3_keyset_with_rx(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
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
            None,
        );
        assert!(
            sql.contains(&format!(
                "(timestamp_ns, fingerprint, cityHash64(body)) >= (1700000000000000001, \
                 toUInt128('{value}'), 42)"
            )),
            "stage3_keyset: the tuple does not carry toUInt128('{value}')\n{sql}"
        );
        assert!(
            sql.contains(&format!("fingerprint IN ({})", set_form())),
            "stage3_keyset: the list is not the set form\n{sql}"
        );
        assert_no_boundary_value_is_written_bare("stage3_keyset", &sql);
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
fn site_3_fingerprint_test_renders_the_set_form() {
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
            fragment
                .as_sql()
                .contains(&format!("fingerprint IN ('{value}')")),
            "fingerprint_test: the filter does not carry the set ('{value}')\n{}",
            fragment.as_sql()
        );
        assert!(
            !fragment.as_sql().contains("toUInt128("),
            "fingerprint_test: a set element is rendered as a call\n{}",
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
fn site_4_render_fingerprint_list_renders_the_set_form() {
    let rendered = sample_sql::render_fingerprint_list(&literals());
    assert_every_boundary_value_is_in_the_set_form("render_fingerprint_list", &rendered);
    // And the statements that embed it, so the list reaches SQL whole.
    let sql = sample_sql::sample_fetch(&no_tenant(), "metric_samples", &literals(), 0, 100);
    assert_every_boundary_value_is_in_the_set_form("sample_fetch", &sql);
    let sql = sample_sql::sample_fetch_multi(&no_tenant(), "metric_samples", &literals(), 0, 100)
        .expect("fits");
    assert_every_boundary_value_is_in_the_set_form("sample_fetch_multi", &sql);
    let sql = sample_sql::hist_sample_fetch_multi(
        &no_tenant(),
        "metric_hist_samples",
        &literals(),
        0,
        100,
    )
    .expect("fits");
    assert_every_boundary_value_is_in_the_set_form("hist_sample_fetch_multi", &sql);
}

/// Site 5: `ids_scope`, reached through `discovery_fetch_multi` (issue
/// #579 part 3: `series_labels_by_fingerprint` reads by an ID statement).
#[test]
fn site_6_discovery_fetch_multi_renders_the_set_form() {
    let sql = metrics_sql::discovery_fetch_multi(
        &no_tenant(),
        "metric_series",
        "metric_labels",
        &["up".to_string()],
        &literals(),
        DataWindow {
            start_ms: 0,
            end_ms: 100,
        },
    )
    .expect("fits");
    assert_every_boundary_value_is_in_the_set_form("discovery_fetch_multi", &sql);
    let _ = DiscoveryFilter::default();
}

/// Site 6: `render_fingerprint_array`, the grouped fetch's `fps` array,
/// read directly.
#[test]
fn site_7_render_fingerprint_array_renders_the_array_form() {
    let rendered = sample_sql::render_fingerprint_array(&literals());
    assert_eq!(
        rendered,
        format!("CAST([{}], 'Array(UInt128)')", set_form()),
        "render_fingerprint_array"
    );
    assert_every_boundary_value_is_in_the_array_form("render_fingerprint_array", &rendered);
    assert_eq!(
        sample_sql::render_fingerprint_array(&[]),
        "CAST([], 'Array(UInt128)')"
    );
}

/// The single-tenant deployment's tenant: no `X-Scope-OrgID`.
#[allow(dead_code)]
fn no_tenant() -> pulsus_model::Tenant {
    pulsus_model::Tenant::from_header(None, false).expect("no header is the empty tenant")
}
