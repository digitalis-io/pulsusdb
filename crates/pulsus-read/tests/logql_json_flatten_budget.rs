//! Issue #287 — `| json`'s full-flatten is quadratic in the line length,
//! and the per-ROW flattened-key byte budget that bounds it.
//!
//! **The reference does the same flatten.** grafana/loki v3.7.4,
//! `pkg/logql/log/parser.go`: `JSONParser.parseLabelValue` names each
//! leaf label with `buildSanitizedPrefixFromBuffer()`'s `_`-joined
//! ancestor path, so the reference's emitted key bytes are ours. The
//! expansion is therefore PARITY and the label names must not change;
//! what diverges is only that the reference has no ceiling (no cap in
//! `JSONParser`, none in `LabelsBuilder.Set`, `labels.go:344`) while
//! PulsusDB refuses with a bounded 422 —
//! `MAX_JSON_FLATTEN_KEY_BYTES`, the same shape as the template output
//! budget.
//!
//! **The construction** (the issue's corrected, closed-form one). For
//!
//! ```text
//! {"<p bytes>":{"k00000":0,"k00001":0,…,"k{m-1:05}":0}}
//! ```
//!
//! the input is `p + 11m + 6` bytes and the emitted keys are `m·(p + 7)`
//! bytes — maximised at `≈ L²/44`. Both closed forms are re-measured
//! here rather than asserted from the issue.
//!
//! **What the budget bounds, restated so it can be read beside the
//! assertions:** the sum of `key.len()` over every key string the row's
//! `| json` full-flattens ALLOCATE — leaf label names AND the
//! intermediate object prefixes — charged before each allocation and
//! shared by every `| json` stage of the row. Not values, not the
//! targeted `| json a="b.c"` form, not `null`/array fields (which build
//! no key at all).

use pulsus_read::logql::pipeline::CompiledPipeline;
use pulsus_read::logql::{
    DetectedFieldsProbe, MAX_JSON_FLATTEN_KEY_BYTES, ReadError, RowBudget, TooBroadReason,
};

fn compiled(query: &str) -> CompiledPipeline {
    let expr = pulsus_logql::parse(query).expect("parse");
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("expected a log query: {query}");
    };
    CompiledPipeline::compile(&log.pipeline).expect("compile")
}

/// `{"<p bytes>":{ m five-digit-named zero leaves }}` — the issue's
/// construction. Input `p + 11m + 6`, emitted keys `m·(p + 7)`.
fn quadratic_line(p: usize, m: usize) -> String {
    assert!(m <= 100_000, "the leaf names are five digits wide");
    let mut s = String::with_capacity(p + 11 * m + 6);
    s.push_str("{\"");
    for _ in 0..p {
        s.push('a');
    }
    s.push_str("\":{");
    for i in 0..m {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("\"k{i:05}\":0"));
    }
    s.push_str("}}");
    s
}

/// The emitted key bytes of one line under a bare `| json`.
fn flatten(line: &str) -> Result<usize, RowBudget> {
    let compiled = compiled(r#"{app="a"} | json"#);
    match compiled.run(line, &[], 0) {
        Ok(out) => Ok(out
            .expect("a bare `| json` never drops a line")
            .labels
            .iter()
            .map(|(k, _)| k.len())
            .sum()),
        Err(e) => {
            assert_eq!(
                e.budget_bytes, MAX_JSON_FLATTEN_KEY_BYTES,
                "a key-budget breach must report the key budget's ceiling"
            );
            Err(e.budget)
        }
    }
}

// ---------------------------------------------------------------------
// The shape: measured, not approximated
// ---------------------------------------------------------------------

/// The two closed forms hold exactly, and the amplification grows with
/// the input — the property that makes a key COUNT cap useless here
/// (the 64 KiB row emits only 2 979 keys).
#[test]
fn flattened_key_bytes_follow_the_measured_quadratic_construction() {
    for (p, m) in [
        (100usize, 10usize),
        (1_000, 100),
        (5_000, 500),
        (20_000, 1_600),
    ] {
        let line = quadratic_line(p, m);
        assert_eq!(
            line.len(),
            p + 11 * m + 6,
            "input closed form (p={p}, m={m})"
        );
        assert_eq!(
            flatten(&line),
            Ok(m * (p + 7)),
            "emitted key closed form (p={p}, m={m})"
        );
    }
}

/// The 64 KiB row of the issue's corrected table: `p = 32 761`,
/// `m = 2 979` is exactly 65 536 input bytes and would emit
/// 97 615 872 key bytes — a 1 489.5x amplification. It is now refused,
/// on the key budget, having allocated at most the budget.
#[test]
fn the_measured_64_kib_worst_shaped_line_is_refused_on_the_key_budget() {
    let line = quadratic_line(32_761, 2_979);
    assert_eq!(line.len(), 65_536);
    assert_eq!(2_979usize * (32_761 + 7), 97_615_872);
    const { assert!(97_615_872u64 > MAX_JSON_FLATTEN_KEY_BYTES) };
    assert_eq!(flatten(&line), Err(RowBudget::JsonFlattenKeys));
}

/// The breach is the bounded 422, carrying its OWN reason — not the
/// template budget's, whose ledger did not refuse.
#[test]
fn the_breach_maps_to_its_own_query_too_broad_reason() {
    let compiled = compiled(r#"{app="a"} | json"#);
    let err = compiled
        .run(&quadratic_line(32_761, 2_979), &[], 0)
        .expect_err("the budget refuses");
    match ReadError::from(err) {
        ReadError::QueryTooBroad(TooBroadReason::JsonFlattenKeyBytes { budget_bytes }) => {
            assert_eq!(budget_bytes, MAX_JSON_FLATTEN_KEY_BYTES);
        }
        other => panic!("expected the json-flatten 422, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// The boundary: exactly at the budget is served, one byte over is not
// ---------------------------------------------------------------------

/// `{"<p a's>":{ m leaves },"<pad z's>":0}` — the nested block builds
/// `p + m·(p + 7)` key bytes and the top-level pad leaf exactly its own
/// name length, so any total is constructible to the byte.
fn line_charging(pad: usize) -> String {
    const P: usize = 100_000;
    const M: usize = 330;
    let nested = quadratic_line(P, M);
    let mut s = nested[..nested.len() - 1].to_string();
    s.push_str(",\"");
    for _ in 0..pad {
        s.push('z');
    }
    s.push_str("\":0}");
    s
}

/// The exact ceiling, in the units the ledger actually spends: the
/// charge is `alloc_block_bytes(key_len)` — `2 x` above its 32-byte
/// floor, and every key here is far above it — so a line whose key
/// CONTENT is exactly `MAX / 2` charges exactly `MAX` and is served in
/// full, while one more byte of key name is refused. Pins the
/// comparison as `charged > remaining`: an off-by-one either way fails
/// one of these two.
#[test]
fn exactly_the_budget_is_served_and_one_more_key_byte_is_refused() {
    let content_ceiling = MAX_JSON_FLATTEN_KEY_BYTES as usize / 2;
    let nested_content = 100_000 + 330 * (100_000 + 7);
    let pad = content_ceiling - nested_content;
    assert_eq!(nested_content + pad, content_ceiling);
    assert!(
        pad > 16,
        "the pad leaf must sit above alloc_block_bytes' floor"
    );

    // At the ceiling the LEAF keys are emitted — everything the flatten
    // built except the intermediate prefix, which is charged but never
    // retained.
    assert_eq!(flatten(&line_charging(pad)), Ok(content_ceiling - 100_000));
    assert_eq!(
        flatten(&line_charging(pad + 1)),
        Err(RowBudget::JsonFlattenKeys)
    );
}

// ---------------------------------------------------------------------
// What is charged, and what is not
// ---------------------------------------------------------------------

/// The INTERMEDIATE object prefixes are charged, not only the leaf keys
/// they end up inside. 119 nested objects of a 12 000-byte key allocate
/// `Σ_d (d·K + d - 2) ≈ 87 MB` of prefix while the single leaf key they
/// build is 1.44 MB — so a ledger that charged only what it EMITS would
/// serve this line, and the shipped one refuses it. That difference is
/// the whole mechanism, stated as an assertion.
#[test]
fn intermediate_object_prefixes_are_charged_not_only_the_emitted_leaf_keys() {
    const K: usize = 12_000;
    const D: usize = 119;
    let key: String = "a".repeat(K);
    let mut line = String::new();
    for _ in 0..D {
        line.push('{');
        line.push('"');
        line.push_str(&key);
        line.push_str("\":");
    }
    line.push('0');
    for _ in 0..D {
        line.push('}');
    }

    // Both sides in CHARGED units — `alloc_block_bytes` is `2 x` here.
    // What a leaf-only ledger would have charged: one key, the full
    // joined path, comfortably inside the budget.
    let only_emitted_key = D * K + (D - 1);
    assert_eq!(only_emitted_key, 1_428_118);
    assert!((2 * only_emitted_key as u64) < MAX_JSON_FLATTEN_KEY_BYTES);

    // What the shipped ledger charges: every prefix on the way down.
    let with_prefixes: usize = (1..=D).map(|d| d * K + d - 1).sum::<usize>() - (D - 1);
    assert!(
        (2 * with_prefixes as u64) > MAX_JSON_FLATTEN_KEY_BYTES,
        "{with_prefixes} key bytes must exceed the budget once charged for this fixture \
         to discriminate"
    );

    assert_eq!(flatten(&line), Err(RowBudget::JsonFlattenKeys));
}

/// `null` and array fields build no key, so they are charged nothing —
/// the budget is on keys ALLOCATED, and this line allocates only the
/// one intermediate prefix.
#[test]
fn null_and_array_fields_build_no_key_and_are_charged_nothing() {
    let mut line = String::from("{\"");
    line.push_str(&"a".repeat(32_761));
    line.push_str("\":{");
    for i in 0..2_979 {
        if i > 0 {
            line.push(',');
        }
        line.push_str(&format!(
            "\"k{i:05}\":{}",
            if i % 2 == 0 { "null" } else { "[1,2]" }
        ));
    }
    line.push_str("}}");
    assert_eq!(
        flatten(&line),
        Ok(0),
        "no leaf key is built, none is charged"
    );
}

/// The targeted form derives no label name from the line — its names
/// come from the compiled stage — so it is charged nothing and serves
/// the very line the full-flatten refuses.
#[test]
fn targeted_extractions_are_not_charged_and_serve_the_refused_line() {
    let key = "a".repeat(32_761);
    let compiled = compiled(&format!(r#"{{app="a"}} | json v="{key}.k00000""#));
    let line = quadratic_line(32_761, 2_979);
    let out = compiled
        .run(&line, &[], 0)
        .expect("a targeted extraction spends no key budget")
        .expect("kept");
    assert_eq!(out.labels, vec![("v".into(), "0".into())]);
}

// ---------------------------------------------------------------------
// The ledger's lifetime: per row, shared across stages
// ---------------------------------------------------------------------

/// One ledger per ROW, shared by every `| json` stage — the issue #260
/// lesson on the parser axis. A line charging 50.2 MB is served by one
/// stage and refused by two, because the second stage re-flattens the
/// same line into a second, simultaneously-live set of keys.
#[test]
fn the_key_budget_is_shared_across_a_rows_json_stages() {
    let line = quadratic_line(100_000, 250);
    // Charged units: `alloc_block_bytes` doubles the key content.
    let charge = 2 * (100_000 + 250 * (100_000 + 7)) as u64;
    assert!(charge < MAX_JSON_FLATTEN_KEY_BYTES);
    assert!(2 * charge > MAX_JSON_FLATTEN_KEY_BYTES);

    assert!(compiled(r#"{app="a"} | json"#).run(&line, &[], 0).is_ok());

    let err = compiled(r#"{app="a"} | json | json"#)
        .run(&line, &[], 0)
        .expect_err("two flattens of this line exceed one row's budget");
    assert_eq!(err.budget, RowBudget::JsonFlattenKeys);
}

/// The ledger's lifetime ENDS with the row: the same near-budget line
/// is served row after row.
#[test]
fn every_row_starts_from_a_full_key_budget() {
    let line = quadratic_line(100_000, 250);
    let compiled = compiled(r#"{app="a"} | json"#);
    for row in 0..3 {
        assert!(
            compiled.run(&line, &[], row).is_ok(),
            "row {row} must start from a full budget"
        );
    }
}

// ---------------------------------------------------------------------
// Every surface that reaches the flatten
// ---------------------------------------------------------------------

/// `/detected_fields` runs the bare `| json` on EVERY sampled line
/// whether or not the query mentions it, so it pays the expansion
/// unconditionally. Its auto-parse pass must surface the 422 rather
/// than treat the breach as "not json" and answer from logfmt's reading
/// of a JSON line.
#[test]
fn the_detected_fields_auto_parse_pass_surfaces_the_breach() {
    let mut probe = DetectedFieldsProbe::new(10, 100);
    probe.add_stream(1, &[("app".to_string(), "a".to_string())]);
    let err = probe
        .feed_row(
            &compiled(r#"{app="a"}"#),
            1,
            0,
            &quadratic_line(32_761, 2_979),
            "",
        )
        .expect_err("the unconditional auto-parse flatten breaches the budget");
    assert!(
        matches!(
            err,
            ReadError::QueryTooBroad(TooBroadReason::JsonFlattenKeyBytes { .. })
        ),
        "expected the json-flatten 422, got {err:?}"
    );
}

/// A line whose ancestor key trims to WHITESPACE: it contributes zero
/// bytes to every label name (`buildSanitizedPrefixFromBuffer` skips an
/// empty-trimming part, `pkg/logql/log/parser.go:213-228 @ v3.7.4`) but
/// its full length to every descendant leaf's captured json path
/// (`buildJSONPathFromPrefixBuffer`, `:234-248`). Under the query path
/// this line is cheap — the key charges see `m·7` bytes; under
/// `/detected_fields`' capture it allocates `m·p` path bytes.
///
/// This is the axis the key charges CANNOT see (review round 1, medium),
/// so it is the discriminating case for pricing the capture on the same
/// ledger: the query path below must still pass, and the capturing path
/// must raise the SAME 422 rather than allocate unbounded.
#[test]
fn a_whitespace_ancestor_is_free_for_keys_and_charged_for_captured_paths() {
    let line = whitespace_ancestor_line(32_761, 2_979);

    // Query path: capture off, so the ancestor costs nothing and the
    // line flattens well inside the ledger.
    let keys = flatten(&line).expect("the query path is far below the budget");
    assert!(
        keys < 40_000,
        "the whitespace ancestor must contribute no key bytes, got {keys}"
    );

    // `/detected_fields`: capture on — the same ledger refuses.
    let mut probe = DetectedFieldsProbe::new(10, 100);
    probe.add_stream(1, &[("app".to_string(), "a".to_string())]);
    let err = probe
        .feed_row(&compiled(r#"{app="a"}"#), 1, 0, &line, "")
        .expect_err("the captured paths must be charged to the row ledger");
    assert!(
        matches!(
            err,
            ReadError::QueryTooBroad(TooBroadReason::JsonFlattenKeyBytes { .. })
        ),
        "expected the json-flatten 422, got {err:?}"
    );
}

/// `{"<p spaces>":{"k00000":0, … ×m}}` — see the test above.
fn whitespace_ancestor_line(p: usize, m: usize) -> String {
    assert!(m <= 100_000, "the leaf names are five digits wide");
    let mut s = String::with_capacity(p + 11 * m + 6);
    s.push_str("{\"");
    for _ in 0..p {
        s.push(' ');
    }
    s.push_str("\":{");
    for i in 0..m {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("\"k{i:05}\":0"));
    }
    s.push_str("}}");
    s
}

/// The capture's DEPTH axis — `JsonPathCapture::stack` grows once per
/// open object (review round 2, high: it was the one owned container the
/// previous revision left unpriced). It is now charged before it grows,
/// and this pins the ceiling that makes its peak SMALL, so the two facts
/// are checkable together.
///
/// **What bounds the stack is serde_json's recursion limit, not our
/// ledger.** Measured here: 126 wrapper objects parse, 127 do not — the
/// crate's default `RECURSION_LIMIT` of 128. A refused line never
/// reaches the flatten at all (it is the malformed-JSON class: line
/// kept, `JSONParserErr` tagged), so the stack can hold at most 127
/// `&str` = 2 032 content bytes, i.e.
/// `grown_alloc_bytes(2032) = 12 192` charged bytes out of 64 MiB.
///
/// **This test therefore cannot demonstrate a stack-only breach, and
/// does not pretend to**: 12 KiB cannot exhaust the ledger, so the
/// charge is defence-in-depth against a future recursion-limit change
/// rather than a reachable 422 today. What it does gate is the premise —
/// if serde_json's limit ever moves, the peak arithmetic above stops
/// holding and this fails. The path-component charges, which CAN
/// breach, are gated by
/// `a_whitespace_ancestor_is_free_for_keys_and_charged_for_captured_paths`.
#[test]
fn nesting_depth_is_capped_upstream_which_bounds_the_capture_stack() {
    // The deepest line that parses, and the first that does not.
    assert_eq!(
        flatten(&nested_line(126)),
        Ok(nested_leaf_key_bytes(126)),
        "126 wrappers must flatten"
    );
    let mut probe = DetectedFieldsProbe::new(10, 1000);
    probe.add_stream(1, &[("app".to_string(), "a".to_string())]);
    probe
        .feed_row(&compiled(r#"{app="a"}"#), 1, 0, &nested_line(126), "")
        .expect("126 wrappers is inside the ledger under capture too");

    // 127 is refused by the JSON parse itself, before the flatten: the
    // line is kept and tagged, never walked, so no stack is built.
    assert!(
        serde_json::from_str::<serde_json::Value>(&nested_line(127)).is_err(),
        "serde_json's RECURSION_LIMIT must still cap the walk at 127 — if this \
         moves, JsonPaths' peak-live-set arithmetic must be recomputed"
    );
    probe
        .feed_row(&compiled(r#"{app="a"}"#), 1, 0, &nested_line(127), "")
        .expect("an over-deep line is the malformed class, never an error");
}

/// The single leaf key `a_a_…_a_leaf` a `nested_line(d)` emits.
fn nested_leaf_key_bytes(depth: usize) -> usize {
    // `d` prefix parts of one byte, joined by `_`, then `_leaf`.
    depth * 2 + 4
}

/// `{"a":{"a":{ … ×d … {"leaf":0} … }}}` — `d` wrapper objects, one leaf.
fn nested_line(depth: usize) -> String {
    let mut s = String::with_capacity(depth * 6 + 16);
    for _ in 0..depth {
        s.push_str("{\"a\":");
    }
    s.push_str("{\"leaf\":0}");
    for _ in 0..depth {
        s.push('}');
    }
    s
}

/// The metric path reaches the same flatten and the same ledger.
#[test]
fn the_metric_path_charges_the_same_key_budget() {
    let compiled = compiled(r#"{app="a"} | json"#);
    let err = compiled
        .run_metric_into(
            &quadratic_line(32_761, 2_979),
            &[],
            0,
            None,
            &mut Vec::new(),
        )
        .expect_err("the metric entrypoint shares the ledger");
    assert_eq!(err.budget, RowBudget::JsonFlattenKeys);
}

// ---------------------------------------------------------------------
// The parser's own nesting bound (issue #334)
// ---------------------------------------------------------------------

/// `WireJson`'s depth is bounded by `serde_json`'s deserializer, not by
/// anything this crate does: a document nested past its recursion limit
/// is refused, so no line can build a tree deep enough for the walks over
/// it — or its `Drop` — to run away. Measured rather than assumed, and it
/// is what lets `| json` keep an ordinary recursive tree instead of the
/// #272 iterative-dismantle machinery the query AST needs.
///
/// A breach is the ordinary malformed-line class: line kept,
/// `__error__="JSONParserErr"`, no extracted label.
#[test]
fn json_nesting_past_the_parser_limit_is_a_parse_error() {
    fn nested(depth: usize) -> String {
        let mut s = String::with_capacity(depth * 6 + 8);
        for _ in 0..depth {
            s.push_str("{\"a\":");
        }
        s.push('1');
        for _ in 0..depth {
            s.push('}');
        }
        s
    }
    let compiled = compiled(r#"{app="a"} | json"#);
    let names = |line: &str| -> Vec<String> {
        compiled
            .run(line, &[], 0)
            .expect("a nesting breach is a parse error, never a budget breach")
            .expect("the line is kept")
            .labels
            .iter()
            .map(|(k, _)| k.to_string())
            .collect()
    };
    // 127 nested `{"a": …}` flatten to one leaf named `a_a_…_a`.
    let deepest_ok: String = std::iter::repeat_n("a", 127).collect::<Vec<_>>().join("_");
    assert_eq!(names(&nested(127)), vec![deepest_ok], "127 levels parse");
    for depth in [128usize, 200, 5_000] {
        assert_eq!(
            names(&nested(depth)),
            vec!["__error__".to_string(), "__error_details__".to_string()],
            "{depth} levels must be refused by the parser, not overflow the stack"
        );
    }
}

// ---------------------------------------------------------------------
// Below the budget, nothing changed
// ---------------------------------------------------------------------

/// The emitted pairs of an ordinary nested line are exactly what they
/// were before the budget existed — same names, same `null`/array
/// skipping.
///
/// The ORDER is the DOCUMENT's, not the sorted one it used to be
/// (issue #334). The flatten walks the object in wire order now, as the
/// reference's `jsonparser.ObjectEach` does, because once the FIRST
/// extraction of a name wins the walk order decides which of two
/// colliding keys survives. The emitted vector is unsorted by contract
/// — renderers and grouping keys sort it — so this is an internal order
/// only; it is still asserted positionally, so a silent re-sort fails
/// here.
#[test]
fn ordinary_lines_flatten_exactly_as_before() {
    let compiled = compiled(r#"{app="a"} | json"#);
    let out = compiled
        .run(
            r#"{"level":"info","http":{"status":200,"path":"/x","tags":[1,2],"ref":null},"ok":true}"#,
            &[],
            0,
        )
        .expect("well inside the budget")
        .expect("kept");
    let pairs: Vec<(String, String)> = out
        .labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("level".to_string(), "info".to_string()),
            ("http_status".to_string(), "200".to_string()),
            ("http_path".to_string(), "/x".to_string()),
            ("ok".to_string(), "true".to_string()),
        ]
    );
}
