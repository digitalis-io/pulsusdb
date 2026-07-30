//! Issue #279: the LogQL query-text admission cap.
//!
//! Reference: grafana/loki v3.7.4, `pkg/logql/syntax/parser.go:42`
//! (`const maxInputSize = 131072`) enforced at `:86` as
//! `if len(input) >= maxInputSize` — an **exclusive maximum**: 131,072
//! bytes is rejected, 131,071 is the longest accepted query. Unit is
//! bytes (Go `len()`), not chars.

use pulsus_logql::{LogQlError, MAX_QUERY_BYTES, parse, parse_selector};

/// A valid selector query (`{app="aaa…"}`) padded to exactly `len` bytes
/// by widening the label value — valid for both `parse` and
/// `parse_selector`, so an over-cap rejection can only be the cap.
fn padded_query(len: usize) -> String {
    let fixed = r#"{app=""}"#.len(); // 8 bytes of syntax
    assert!(len > fixed, "padded_query needs len > {fixed}");
    format!(r#"{{app="{}"}}"#, "a".repeat(len - fixed))
}

fn assert_rejected_as_too_long(res: Result<(), LogQlError>, input_len: usize, entry: &str) {
    match res {
        Err(LogQlError::QueryTooLong { len, cap, span }) => {
            assert_eq!(
                len, input_len,
                "{entry}: `len` must be the real input length"
            );
            assert_eq!(
                cap, MAX_QUERY_BYTES,
                "{entry}: `cap` must be MAX_QUERY_BYTES"
            );
            assert_eq!(span.start, 0, "{entry}: zero-width span at 0");
            assert_eq!(span.end, 0, "{entry}: zero-width span at 0");
        }
        other => panic!("{entry}: expected QueryTooLong at {input_len} bytes, got {other:?}"),
    }
}

/// AC1: the boundary is exact and exclusive — table-driven over both
/// public entry points at 0, 1, 131 070, 131 071 (accepted), 131 072
/// (rejected), 131 073.
#[test]
fn the_cap_boundary_is_exact_and_exclusive_for_both_entry_points() {
    // Under-cap valid queries are ACCEPTED (parse succeeds outright).
    for len in [131_070, 131_071] {
        let q = padded_query(len);
        assert_eq!(q.len(), len);
        assert!(
            parse(&q).is_ok(),
            "parse must accept a valid {len}-byte query"
        );
        assert!(
            parse_selector(&q).is_ok(),
            "parse_selector must accept a valid {len}-byte query"
        );
    }
    // Under-cap degenerate inputs fail for OTHER reasons — never the cap.
    for input in ["", "x"] {
        for (entry, err) in [
            ("parse", parse(input).expect_err("degenerate input")),
            (
                "parse_selector",
                parse_selector(input).expect_err("degenerate input"),
            ),
        ] {
            assert!(
                !matches!(err, LogQlError::QueryTooLong { .. }),
                "{entry}: a {}-byte input must never trip the cap: {err:?}",
                input.len()
            );
        }
    }
    // At and past the cap: rejected as QueryTooLong even though the query
    // text itself is valid — the cap fires before tokenization.
    for len in [131_072, 131_073] {
        let q = padded_query(len);
        assert_eq!(q.len(), len);
        assert_rejected_as_too_long(parse(&q).map(|_| ()), len, "parse");
        assert_rejected_as_too_long(parse_selector(&q).map(|_| ()), len, "parse_selector");
    }
}

/// AC2: the unit is bytes, not chars. A multi-byte query far under the
/// cap by `chars().count()` but at the cap by `len()` is rejected; a
/// 131,071-byte query with multi-byte content is accepted.
#[test]
fn the_cap_counts_bytes_not_chars() {
    // 8 syntax bytes + 43,688 three-byte 'あ' = exactly 131,072 bytes,
    // but only 43,696 chars — under the cap by chars, at it by bytes.
    let over = format!(r#"{{app="{}"}}"#, "あ".repeat(43_688));
    assert_eq!(over.len(), 131_072);
    assert!(over.chars().count() < MAX_QUERY_BYTES);
    assert_rejected_as_too_long(parse(&over).map(|_| ()), over.len(), "parse");
    assert_rejected_as_too_long(
        parse_selector(&over).map(|_| ()),
        over.len(),
        "parse_selector",
    );

    // 8 + 2 ASCII + 43,687 three-byte 'あ' = exactly 131,071 bytes:
    // the longest accepted query, with multi-byte content.
    let max_accepted = format!(r#"{{app="aa{}"}}"#, "あ".repeat(43_687));
    assert_eq!(max_accepted.len(), 131_071);
    assert!(parse(&max_accepted).is_ok());
    assert!(parse_selector(&max_accepted).is_ok());
}

/// AC6: the constant is public, equals the reference's `maxInputSize`,
/// and a silent retune reddens here.
#[test]
fn max_query_bytes_is_the_reference_max_input_size() {
    assert_eq!(MAX_QUERY_BYTES, 131_072);
}

/// AC3: the seam drift-guard. The safety property is compile-enforced
/// (`lexer::tokenize` takes `CheckedQuery<'_>`, whose only constructor is
/// the cap check) — this test only keeps the entry-point enumeration
/// honest as the code moves.
#[test]
fn the_checked_query_seam_has_no_bypass_and_no_new_entry_point() {
    let parser_src = include_str!("../src/parser.rs");
    let lib_src = include_str!("../src/lib.rs");

    // Every `lexer::tokenize(` call in parser.rs goes through
    // `CheckedQuery::new(` — a tokenize call on anything else would not
    // compile, but this names the file and the fix if the shape drifts.
    let tokenize_calls = parser_src.matches("lexer::tokenize(").count();
    let seamed_calls = parser_src
        .matches("lexer::tokenize(CheckedQuery::new(")
        .count();
    assert_eq!(
        tokenize_calls, seamed_calls,
        "crates/pulsus-logql/src/parser.rs: every `lexer::tokenize(` call must be \
         `lexer::tokenize(CheckedQuery::new(input)?)` — route new entry points through \
         the #279 CheckedQuery seam and add them to the #279 surface enumeration"
    );
    assert_eq!(
        parser_src.matches("CheckedQuery::new(").count(),
        2,
        "crates/pulsus-logql/src/parser.rs: expected exactly two CheckedQuery::new call \
         sites (`parse`, `parse_selector`); a third means a new LogQL entry point exists \
         — add it to the #279 surface enumeration (issue #279 §2) before shipping"
    );

    // lib.rs still exports exactly {parse, parse_selector} as the parse
    // entry set.
    assert!(
        lib_src.contains("pub use parser::{parse, parse_selector};"),
        "crates/pulsus-logql/src/lib.rs: the exported parse entry set must stay exactly \
         `pub use parser::{{parse, parse_selector}};` — a new exported parse entry needs \
         the #279 CheckedQuery seam and a row in the #279 surface enumeration"
    );
    let parser_exports = lib_src.matches("pub use parser::").count();
    assert_eq!(
        parser_exports, 1,
        "crates/pulsus-logql/src/lib.rs: a second `pub use parser::` export line exists — \
         new parse entry points must run the #279 cap (CheckedQuery seam) and be added to \
         the #279 surface enumeration"
    );
}
