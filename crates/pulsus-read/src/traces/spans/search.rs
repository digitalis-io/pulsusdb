//! The TraceQL search statement (issue #590). Stub: the tests come first.

use super::predicate::SpanPredicate;
use crate::traces::window_sql::WindowSql;

/// Stub.
pub fn search_sql(
    _spans_table: &str,
    _traces_table: &str,
    _w: WindowSql,
    _p: &SpanPredicate,
    _limit: u32,
    _spss: u32,
) -> String {
    String::new()
}
