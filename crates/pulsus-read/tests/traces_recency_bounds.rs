//! Issue #560: the recency table's bucket clause at its boundary values.
//!
//! `trace_recent` is ordered by `(bucket, trace_id)`, where the view
//! writes `bucket = toUInt32(intDiv(timestamp_ns, 300000000000))`. The
//! reader renders `bucket >= <lo> AND bucket <= <hi>`, `lo` from the
//! window's start and `hi` from its last included nanosecond, both by
//! floor division, and SIGNED: a pre-epoch window renders negative
//! literals rather than clamping them.
//!
//! ```text
//!   window (-1, 0]                 lo = floor(-1 / B) = -1   hi = 0
//!   window (0, B - 1]              lo = 0                    hi = 0
//!   window (0, B]                  lo = 0                    hi = 1   <- one ns apart
//!   window [0, B)                  last included ns = B - 1  hi = 0
//!   window (0, i64::MAX]           hi = 30744573
//! ```

use pulsus_read::traces::window_sql::WindowSql;

#[test]
fn the_bucket_clause_floors_both_ends_and_renders_signed() {
    let cases: [(&str, WindowSql, &str); 7] = [
        (
            "start_open_end_closed(-1, 0)",
            WindowSql::start_open_end_closed(-1, 0),
            "bucket >= -1 AND bucket <= 0",
        ),
        (
            "start_open_end_closed(0, 1)",
            WindowSql::start_open_end_closed(0, 1),
            "bucket >= 0 AND bucket <= 0",
        ),
        (
            "start_open_end_closed(0, 299_999_999_999)",
            WindowSql::start_open_end_closed(0, 299_999_999_999),
            "bucket >= 0 AND bucket <= 0",
        ),
        (
            "start_open_end_closed(0, 300_000_000_000)",
            WindowSql::start_open_end_closed(0, 300_000_000_000),
            "bucket >= 0 AND bucket <= 1",
        ),
        (
            "start_closed_end_open(0, 300_000_000_000)",
            WindowSql::start_closed_end_open(0, 300_000_000_000),
            "bucket >= 0 AND bucket <= 0",
        ),
        (
            "start_open_end_closed(-2_000_000_000_000, -1_000_000_000_000)",
            WindowSql::start_open_end_closed(-2_000_000_000_000, -1_000_000_000_000),
            "bucket >= -7 AND bucket <= -4",
        ),
        (
            "start_open_end_closed(0, i64::MAX)",
            WindowSql::start_open_end_closed(0, i64::MAX),
            "bucket >= 0 AND bucket <= 30744573",
        ),
    ];
    let mut wrong: Vec<String> = Vec::new();
    for (label, window, expected) in cases {
        let got = window.bucket_clause();
        if got != expected {
            wrong.push(format!("{label}: expected {expected:?}, got {got:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of 7 bucket clauses are wrong:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}
