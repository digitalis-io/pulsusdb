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
// Below the budget, nothing changed
// ---------------------------------------------------------------------

/// The emitted pairs of an ordinary nested line are exactly what they
/// were before the budget existed — same names, same order, same
/// `null`/array skipping.
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
            ("http_path".to_string(), "/x".to_string()),
            ("http_status".to_string(), "200".to_string()),
            ("level".to_string(), "info".to_string()),
            ("ok".to_string(), "true".to_string()),
        ]
    );
}
