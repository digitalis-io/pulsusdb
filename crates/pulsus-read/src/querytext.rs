//! The read-path query-text admission guard (issue #35, "full-shape parse
//! bound"). ClickHouse's own `max_query_size` setting (default 262,144
//! bytes) is a SQL-text **parse-buffer** cap, entirely distinct from
//! `max_bytes_to_read`/the scan budget: it bounds how large the literal
//! request text itself may be, not how much data the query scans. Every
//! read-path query in this crate inlines its `IN (...)` sets as literal
//! text (fingerprints, service names, metric names, pre-rendered line-
//! filter predicates) rather than binding them as parameters, so a broad
//! selector can produce SQL text past the ClickHouse default well before it
//! ever reaches the byte scan budget — previously an opaque server parse
//! error ("Max query size exceeded") instead of the engine's own `422
//! query_too_broad` class.
//!
//! **Two-part fix, not one constant.** [`MAX_QUERY_TEXT_BYTES`] is sent as
//! `max_query_size` on every read-path query (raising ClickHouse's own
//! parse buffer), **and** [`ensure_query_text_fits`] rejects the rendered
//! SQL pre-dispatch when it would not fit — because no single constant can
//! honestly bound every accepted request shape. LogQL `stage3`'s services
//! and line-filter text are unbounded in width by the ingest contract (no
//! label-value length cap, and a maximal accepted line-filter POST body
//! renders to roughly 27 MiB after per-token expansion — see
//! `crates/pulsus-read/src/logql/exec.rs`'s full-shape analysis in the #35
//! plan record); metrics selectors can resolve up to the 1,000,000-name
//! fan-out ceiling. The guard is the actual backstop for those shapes; the
//! raised setting only ensures the documented, *bounded* worst case (the
//! stream/fingerprint cap) is never rejected.
//!
//! **Sizing argument for [`MAX_QUERY_TEXT_BYTES`] (8 MiB).** The
//! guaranteed-admitted envelope this cap must cover: `DEFAULT_MAX_STREAMS`
//! (100,000) worst-case `u64::MAX` fingerprint literals (20 digits + `", "`
//! separator = 22 B/entry ⇒ 2,199,998 B), plus 10,000 distinct 64-byte
//! escaped service literals (~660 KB), plus 1 MiB of pre-rendered
//! line-filter predicate text (16 × 64 KiB filters — a generous multiple of
//! realistic pipelines), plus template/window/keyset-tuple overhead
//! (< 400 B) — roughly 3.91 MB ≈ 3.73 MiB, comfortably under 8 MiB (≈2.15×
//! headroom) and well above ClickHouse's 262,144-byte default (which admits
//! only ≈11,915 `u64` literals — below the product's own 100k stream cap).
//! Requests whose rendered text exceeds this envelope (unbounded
//! service/filter width, or a large metrics fan-out) are rejected by
//! [`ensure_query_text_fits`] before dispatch, never sent to ClickHouse to
//! fail with an opaque parse error.

/// The `max_query_size` session setting every read-path query now carries,
/// and the pre-dispatch admission cap [`ensure_query_text_fits`] enforces
/// against the FINAL rendered SQL text (after any placeholder-escaping).
/// See the module doc for the sizing argument.
pub const MAX_QUERY_TEXT_BYTES: u64 = 8 * 1024 * 1024;

/// Rejects `sql` pre-dispatch when its rendered byte length would not fit
/// under [`MAX_QUERY_TEXT_BYTES`] — an O(1) `str::len()` check, zero
/// ClickHouse round-trip. Conservative by one byte (`len >= cap` rejects,
/// never `len > cap`): ClickHouse's own exactly-full-buffer behavior at the
/// setting's boundary is never relied on. Callers convert the returned
/// [`crate::logql::TooBroadReason`] into `ReadError::QueryTooBroad`, which
/// every API mapper already maps to a clean `422` (never an opaque
/// ClickHouse parse error).
pub fn ensure_query_text_fits(sql: &str) -> Result<(), crate::logql::TooBroadReason> {
    let rendered_bytes = sql.len() as u64;
    if rendered_bytes >= MAX_QUERY_TEXT_BYTES {
        return Err(crate::logql::TooBroadReason::QueryTextBytes {
            rendered_bytes,
            cap: MAX_QUERY_TEXT_BYTES,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_query_under_the_cap_fits() {
        assert!(ensure_query_text_fits("SELECT 1").is_ok());
    }

    #[test]
    fn a_query_exactly_at_the_cap_is_rejected_conservatively() {
        let sql = "a".repeat(MAX_QUERY_TEXT_BYTES as usize);
        assert!(ensure_query_text_fits(&sql).is_err());
    }

    #[test]
    fn a_query_one_byte_under_the_cap_fits() {
        let sql = "a".repeat(MAX_QUERY_TEXT_BYTES as usize - 1);
        assert!(ensure_query_text_fits(&sql).is_ok());
    }

    #[test]
    fn a_query_past_the_cap_names_both_numbers() {
        let sql = "a".repeat(MAX_QUERY_TEXT_BYTES as usize + 100);
        match ensure_query_text_fits(&sql) {
            Err(crate::logql::TooBroadReason::QueryTextBytes {
                rendered_bytes,
                cap,
            }) => {
                assert_eq!(rendered_bytes, MAX_QUERY_TEXT_BYTES + 100);
                assert_eq!(cap, MAX_QUERY_TEXT_BYTES);
            }
            other => panic!("expected QueryTextBytes, got {other:?}"),
        }
    }
}
