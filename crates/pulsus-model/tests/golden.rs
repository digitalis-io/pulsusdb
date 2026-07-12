//! Hermetic golden tests: replays `tests/fixtures/fingerprints.json`
//! (generated once from a live ClickHouse 24.8 server, issue #4) against
//! this crate's canonicalization and fingerprint functions. No network
//! dependency — `tests/live_cityhash.rs` is what re-verifies the same
//! vectors against a live server.
//!
//! `normalization_chain_pins_otel_and_prenormalized_paths_to_identical_output`
//! additionally freezes the AC#3 identity chain (docs/architecture.md
//! §2.3): a `{service_name="checkout"}` label, an OTel `service.name`
//! attribute, and the physical `service` column value must all resolve
//! identically through the real `LabelSet::from_normalized` path — not
//! `from_verbatim`, which every other case in this file uses for traces.

use pulsus_model::{
    LabelSet, build_metric_buffer, build_stream_buffer, canonicalize_label_key, metric_fingerprint,
    raw_cityhash64, stream_fingerprint,
};
use serde_json::Value;

const FIXTURES: &str = include_str!("fixtures/fingerprints.json");

fn fixtures() -> Value {
    serde_json::from_str(FIXTURES).expect("tests/fixtures/fingerprints.json must be valid JSON")
}

fn decode_hex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd-length hex string: {s}");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex digit"))
        .collect()
}

/// Decodes a fixture `[[key, value], ...]` array into raw pairs, without
/// choosing a `LabelSet` constructor — callers pick `from_verbatim` (traces)
/// or `from_normalized` (logs/metrics, including the normalization chain
/// cases) themselves.
fn pairs_from_json(labels: &Value) -> Vec<(String, String)> {
    labels
        .as_array()
        .expect("labels is an array")
        .iter()
        .map(|pair| {
            let pair = pair.as_array().expect("label pair is a 2-array");
            (
                pair[0].as_str().expect("key is a string").to_string(),
                pair[1].as_str().expect("value is a string").to_string(),
            )
        })
        .collect()
}

fn labels_from_json(labels: &Value) -> LabelSet {
    LabelSet::from_verbatim(pairs_from_json(labels))
}

/// Issue #4 AC#3 / architecture.md §2.3: an OTel-style attribute
/// (`service.name = "checkout"`), an already-normalized label
/// (`service_name = "checkout"`), and the physical `service` column value
/// must all resolve identically through the *real* normalized path
/// ([`LabelSet::from_normalized`], not `from_verbatim`). Each fixture case
/// builds a `LabelSet` from both an OTel-shaped pair list and a
/// pre-normalized pair list, asserts they are byte-identical (canonical
/// JSON, both fingerprints, and the `service()` projection), and pins that
/// output against the committed fixture so the chain cannot silently drift.
#[test]
fn normalization_chain_pins_otel_and_prenormalized_paths_to_identical_output() {
    let fx = fixtures();
    let cases = fx["normalization_chain"].as_array().expect("array");
    assert!(!cases.is_empty());
    for case in cases {
        let name = case["name"].as_str().expect("name");
        let otel_pairs = pairs_from_json(&case["otel_pairs"]);
        let normalized_pairs = pairs_from_json(&case["normalized_pairs"]);

        let (otel_set, otel_collisions) = LabelSet::from_normalized(otel_pairs);
        let (normalized_set, normalized_collisions) = LabelSet::from_normalized(normalized_pairs);
        assert_eq!(otel_collisions, 0, "{name}: unexpected OTel-path collision");
        assert_eq!(
            normalized_collisions, 0,
            "{name}: unexpected pre-normalized-path collision"
        );

        // The whole point of the chain: both inputs must resolve to one
        // identical `LabelSet`, not merely equivalent-looking output.
        assert_eq!(
            otel_set, normalized_set,
            "{name}: OTel path and pre-normalized path diverged"
        );

        let expected_json = case["canonical_json"].as_str().expect("canonical_json");
        let expected_metric_fp: u64 = case["metric_fingerprint"]
            .as_str()
            .expect("metric_fingerprint")
            .parse()
            .expect("metric_fingerprint parses as u64");
        let expected_stream_fp: u64 = case["stream_fingerprint"]
            .as_str()
            .expect("stream_fingerprint")
            .parse()
            .expect("stream_fingerprint parses as u64");
        let expected_service = case["service"].as_str().expect("service");

        for (label, set) in [("otel", &otel_set), ("normalized", &normalized_set)] {
            assert_eq!(
                set.to_canonical_json(),
                expected_json,
                "{name} ({label}): canonical JSON"
            );
            assert_eq!(
                metric_fingerprint(set),
                expected_metric_fp,
                "{name} ({label}): metric fingerprint"
            );
            assert_eq!(
                stream_fingerprint(set),
                expected_stream_fp,
                "{name} ({label}): stream fingerprint"
            );
            assert_eq!(
                set.service(),
                expected_service,
                "{name} ({label}): service() projection"
            );
        }
    }
}

#[test]
fn canonicalization_vectors_match() {
    let fx = fixtures();
    let cases = fx["canonicalization"].as_array().expect("array");
    assert!(!cases.is_empty());
    for case in cases {
        let input = case["input"].as_str().expect("input");
        let expected = case["expected"].as_str().expect("expected");
        assert_eq!(
            canonicalize_label_key(input),
            expected,
            "canonicalize_label_key({input:?})"
        );
    }
}

#[test]
fn metric_fingerprint_vectors_match() {
    let fx = fixtures();
    let cases = fx["metric_fingerprints"].as_array().expect("array");
    assert!(!cases.is_empty());
    for case in cases {
        let name = case["name"].as_str().expect("name");
        let labels = labels_from_json(&case["labels"]);
        let expected_buf = decode_hex(case["buffer_hex"].as_str().expect("buffer_hex"));
        let expected_fp: u64 = case["fingerprint"]
            .as_str()
            .expect("fingerprint")
            .parse()
            .expect("fingerprint parses as u64");

        assert_eq!(build_metric_buffer(&labels), expected_buf, "{name}: buffer");
        assert_eq!(
            metric_fingerprint(&labels),
            expected_fp,
            "{name}: fingerprint"
        );
    }
}

#[test]
fn raw_cityhash64_length_class_vectors_match() {
    let fx = fixtures();
    let cases = fx["raw_cityhash64_vectors"].as_array().expect("array");
    // Every mandated length branch (issue #4 plan amendment) must be
    // present, not a sampling.
    let required_lengths = [
        0usize, 1, 3, 4, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 5000,
    ];
    assert_eq!(cases.len(), required_lengths.len());
    for case in cases {
        let name = case["name"].as_str().expect("name");
        let buf = decode_hex(case["buffer_hex"].as_str().expect("buffer_hex"));
        let expected_fp: u64 = case["fingerprint"]
            .as_str()
            .expect("fingerprint")
            .parse()
            .expect("fingerprint parses as u64");
        assert_eq!(raw_cityhash64(&buf), expected_fp, "{name}");
    }
    for &len in &required_lengths {
        let found = cases
            .iter()
            .any(|c| c["name"] == Value::String(format!("raw_len_{len}")));
        assert!(found, "missing length-class vector for {len} bytes");
    }
}

#[test]
fn stream_fingerprint_vectors_match() {
    let fx = fixtures();
    let cases = fx["stream_fingerprints"].as_array().expect("array");
    assert!(!cases.is_empty());
    for case in cases {
        let name = case["name"].as_str().expect("name");
        let labels = labels_from_json(&case["labels"]);
        let expected_buf = decode_hex(case["buffer_hex"].as_str().expect("buffer_hex"));
        let expected_fp: u64 = case["fingerprint"]
            .as_str()
            .expect("fingerprint")
            .parse()
            .expect("fingerprint parses as u64");

        assert_eq!(build_stream_buffer(&labels), expected_buf, "{name}: buffer");
        assert_eq!(
            stream_fingerprint(&labels),
            expected_fp,
            "{name}: fingerprint"
        );
    }
}

#[test]
fn stream_fingerprint_vectors_cover_every_boundary_straddle_and_nonascii_case() {
    let fx = fixtures();
    let cases = fx["stream_fingerprints"].as_array().expect("array");
    let names: Vec<&str> = cases.iter().map(|c| c["name"].as_str().unwrap()).collect();
    for target in [15, 16, 17, 31, 32, 33, 63, 64, 65] {
        assert!(
            names
                .iter()
                .any(|n| n.starts_with(&format!("multilabel_straddle_{target}_"))),
            "missing multi-label boundary-straddle vector for target {target}"
        );
    }
    // Non-ASCII UTF-8 values must cross at least two of the 32/64
    // boundaries between them (issue #4 plan amendment).
    let nonascii_lens: Vec<usize> = names
        .iter()
        .filter(|n| n.starts_with("nonascii_"))
        .map(|n| {
            let hex = &cases[names.iter().position(|x| x == n).unwrap()]["buffer_hex"];
            decode_hex(hex.as_str().unwrap()).len()
        })
        .collect();
    assert!(!nonascii_lens.is_empty());
    // At least one buffer past the 32-byte boundary, and at least one past
    // the 64-byte boundary too — collectively crossing (at least) two
    // length-class boundaries, per the issue #4 plan amendment.
    assert!(nonascii_lens.iter().any(|&l| (33..=64).contains(&l)));
    assert!(nonascii_lens.iter().any(|&l| l > 64));
}
