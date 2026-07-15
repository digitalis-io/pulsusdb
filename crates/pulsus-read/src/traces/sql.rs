//! Pure SQL builder for the trace-by-ID point read — the snapshot-testing
//! surface (`tests/traces_point_read.rs`), same convention as
//! [`crate::logql::sql`]: `validated inputs → String`, no `ChClient`, no
//! I/O. Callers are responsible for validating `hex32` before it reaches
//! this builder — that is the injection boundary, not this module.

/// The docs/schemas.md §4.2 canonical trace-by-ID point read, byte-for-byte
/// (issue #55 plan v2 §1: the documented query wins — no `ORDER BY`;
/// assembly is order-independent by construction, so row order is never
/// relied on).
///
/// `hex32` must already be validated as exactly 32 lowercase hex chars
/// (`[0-9a-f]{32}`) — the server's `parse_trace_id` is the one validation
/// point — so only hex digits can ever reach the `unhex('...')` literal
/// (injection-safe by construction).
pub fn point_read_sql(spans_table: &str, hex32: &str) -> String {
    debug_assert!(
        hex32.len() == 32 && hex32.bytes().all(|b| b.is_ascii_hexdigit()),
        "hex32 must be caller-validated 32-char hex, got {hex32:?}"
    );
    format!(
        "SELECT trace_id, span_id, parent_id, payload_type, payload\n\
         FROM {spans_table}\n\
         WHERE trace_id = unhex('{hex32}')"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AC1: byte-equal to the docs/schemas.md §4.2 "Trace by ID" block.
    #[test]
    fn point_read_sql_is_byte_exact_to_schemas_md_4_2() {
        assert_eq!(
            point_read_sql("trace_spans", "4bf92f3577b34da6a3ce929d0e0e4736"),
            "SELECT trace_id, span_id, parent_id, payload_type, payload\n\
             FROM trace_spans\n\
             WHERE trace_id = unhex('4bf92f3577b34da6a3ce929d0e0e4736')"
        );
    }

    #[test]
    fn point_read_sql_targets_the_caller_supplied_table() {
        let sql = point_read_sql("trace_spans_dist", "00000000000000000000000000000001");
        assert!(sql.contains("FROM trace_spans_dist\n"));
    }
}
