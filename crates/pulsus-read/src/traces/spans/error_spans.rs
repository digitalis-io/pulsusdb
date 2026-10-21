//! The error-span table's admission (issue #601 part 4b).

use pulsus_traceql::Query;

/// Whether `query` is answered from the error-span table.
pub fn admits(_query: &Query) -> bool {
    false
}
