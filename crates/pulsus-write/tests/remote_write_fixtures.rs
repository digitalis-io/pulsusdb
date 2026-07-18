//! Fixture-driven tests for the Prometheus remote-write receiver (issue
//! #28 acceptance criteria). `tests/fixtures/remote-write/basic_series.bin`
//! and `.../metadata.bin` are **real captures** — snappy-compressed
//! `prompb.WriteRequest` payloads recorded from a live OpenTelemetry
//! Collector's `prometheusremotewrite` exporter (provenance:
//! `tests/fixtures/remote-write/README.md`) — proving the hand-rolled
//! prompb tags in `protocols/remote_write.rs` decode real wire bytes, not
//! just self-consistent synthetic round-trips through the same structs.
//! The remaining edge cases (missing `__name__`, unsorted labels, malformed
//! snappy/protobuf) have no real-collector equivalent — a standard sender
//! never omits `__name__` or ships malformed wire data — so those cases are
//! built programmatically in-test instead.

use std::path::{Path, PathBuf};

use pulsus_model::STALE_NAN_BITS;
use pulsus_write::ingest::decompress::{Encoding, decompress};
use pulsus_write::protocols::remote_write::{
    Label, Sample, TimeSeries, WriteRequest, decode, parse,
};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/remote-write")
}

fn read_fixture(name: &str) -> Vec<u8> {
    std::fs::read(fixtures_dir().join(name))
        .unwrap_or_else(|e| panic!("reading fixture {name}: {e}"))
}

/// Reads and decodes a captured `.bin` fixture: snappy-decompress (the
/// captured bytes are exactly what a real sender's HTTP body would be,
/// `Content-Encoding: snappy` and all) then prost-decode — mirrors what
/// `ingest_remote_write` does at the HTTP boundary.
fn decode_fixture(name: &str) -> WriteRequest {
    let compressed = read_fixture(name);
    let decompressed =
        decompress(Encoding::Snappy, &compressed).expect("fixture is valid block-snappy");
    decode(&decompressed).expect("fixture is a valid WriteRequest")
}

fn label(name: &str, value: &str) -> Label {
    Label {
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn snappy_compress(data: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new().compress_vec(data).unwrap()
}

// ---------------------------------------------------------------------
// Real-capture fixtures.
// ---------------------------------------------------------------------

#[test]
fn basic_series_fixture_produces_exact_rows_matching_independently_recomputed_fingerprints() {
    let req = decode_fixture("basic_series.bin");
    let out = parse(&req, 0).expect("within the expansion budget");

    assert_eq!(out.samples.len(), 3);
    assert_eq!(out.series.len(), 3);
    assert_eq!(out.rejected, 0);

    let cpu = out
        .samples
        .iter()
        .find(|s| &*s.metric_name == "cpu_usage_ratio")
        .expect("cpu_usage_ratio sample present");
    assert_eq!(cpu.value, 0.42);
    let cpu_series = out
        .series
        .iter()
        .find(|r| r.fingerprint == cpu.fingerprint)
        .unwrap();
    assert_eq!(cpu_series.labels.get("host"), Some("node-a"));
    assert_eq!(cpu_series.labels.get("job"), Some("checkout"));
    assert_eq!(cpu_series.labels.get("service_name"), Some("checkout"));
    // `__name__` never enters the LabelSet (architect plan).
    assert_eq!(cpu_series.labels.get("__name__"), None);
    // Independently recompute the fingerprint via the frozen model
    // function over the same labels, proving the parser's fingerprint is
    // not just internally self-consistent.
    assert_eq!(
        pulsus_model::metric_fingerprint(&cpu_series.labels),
        cpu.fingerprint
    );

    let http = out
        .samples
        .iter()
        .find(|s| &*s.metric_name == "http_requests_total")
        .expect("http_requests_total sample present");
    assert_eq!(http.value, 1234.0);
    let http_series = out
        .series
        .iter()
        .find(|r| r.fingerprint == http.fingerprint)
        .unwrap();
    assert_eq!(http_series.labels.get("method"), Some("GET"));
}

/// AC: "Stale-marker sample round-trips bit-identical." — the collector's
/// own OTLP-to-remote-write translation of a `NoRecordedValueMask`-flagged
/// data point (`up_ratio` in this capture) preserves the exact stale-NaN
/// bit pattern; the parser must carry it through verbatim too.
#[test]
fn stale_marker_sample_in_the_real_capture_survives_bit_exact() {
    let req = decode_fixture("basic_series.bin");
    let out = parse(&req, 0).expect("within the expansion budget");

    let stale = out
        .samples
        .iter()
        .find(|s| &*s.metric_name == "up_ratio")
        .expect("up_ratio (stale-flagged in OTLP) sample present");
    assert_eq!(
        stale.value.to_bits(),
        STALE_NAN_BITS,
        "the collector's own OTLP->remote-write translation of a stale data point must \
         decode to the exact canonical stale-NaN bit pattern"
    );
}

/// AC: metadata records — a real capture of the collector's `send_metadata`
/// remote-write metadata push, proving the hand-rolled `MetricMetadataProto`
/// tags (1/2/4/5, gap at 3) decode a real collector's wire bytes.
#[test]
fn metadata_fixture_decodes_every_record_with_type_string_parity() {
    let req = decode_fixture("metadata.bin");
    let out = parse(&req, 999).expect("within the expansion budget");

    assert_eq!(out.metadata.len(), 3);

    let cpu = out
        .metadata
        .iter()
        .find(|m| &*m.metric_name == "cpu_usage_ratio")
        .unwrap();
    assert_eq!(cpu.metric_type, "gauge");
    assert_eq!(cpu.help, "fraction of CPU in use");
    assert_eq!(cpu.updated_ns, 999);

    let http = out
        .metadata
        .iter()
        .find(|m| &*m.metric_name == "http_requests_total")
        .unwrap();
    assert_eq!(http.metric_type, "counter");
    assert_eq!(http.help, "total HTTP requests");

    let up = out
        .metadata
        .iter()
        .find(|m| &*m.metric_name == "up_ratio")
        .unwrap();
    assert_eq!(up.metric_type, "gauge");
}

/// `assert_eq!` on the whole `ParsedMetrics` would spuriously fail here:
/// `basic_series.bin` carries a stale-NaN sample (see the test above), and
/// `f64`'s `PartialEq` makes `NaN != NaN` even when the bit patterns are
/// identical — `MetricSampleRow`'s own doc comment documents the same
/// hazard for exactly this reason. Compares each field explicitly, values
/// via `.to_bits()`, so purity is still pinned bit-exactly without
/// tripping over `NaN`'s reflexivity.
#[test]
fn parse_of_the_real_capture_is_pure_repeated_calls_are_identical() {
    let req = decode_fixture("basic_series.bin");
    let a = parse(&req, 123).expect("within the expansion budget");
    let b = parse(&req, 123).expect("within the expansion budget");

    assert_eq!(a.samples.len(), b.samples.len());
    for (sa, sb) in a.samples.iter().zip(&b.samples) {
        assert_eq!(sa.metric_name, sb.metric_name);
        assert_eq!(sa.fingerprint, sb.fingerprint);
        assert_eq!(sa.unix_milli, sb.unix_milli);
        assert_eq!(sa.value.to_bits(), sb.value.to_bits());
    }
    assert_eq!(a.series, b.series);
    assert_eq!(a.metadata, b.metadata);
    assert_eq!(a.collisions, b.collisions);
    assert_eq!(a.rejected, b.rejected);
    assert_eq!(a.rejected_message, b.rejected_message);
}

// ---------------------------------------------------------------------
// Synthetic edge cases (no real-collector equivalent — see module doc).
// ---------------------------------------------------------------------

#[test]
fn missing_name_label_drops_the_series_request_still_succeeds_as_partial() {
    let req = WriteRequest {
        timeseries: vec![
            TimeSeries {
                labels: vec![label("job", "checkout")],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1,
                }],
            },
            TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1,
                }],
            },
        ],
        metadata: vec![],
    };
    let out = parse(&req, 0).expect("within the expansion budget");
    assert_eq!(out.rejected, 1);
    assert_eq!(out.samples.len(), 1);
    assert_eq!(&*out.samples[0].metric_name, "up");
}

/// Pins the re-sort-or-reject rule (architect plan): wire label order is
/// never a rejection trigger — `LabelSet::from_normalized` re-sorts
/// deterministically, so out-of-order labels on the wire fingerprint
/// identically to sorted ones.
#[test]
fn out_of_order_wire_labels_are_accepted_and_fingerprint_identically_to_sorted() {
    let sorted = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![
                label("__name__", "up"),
                label("a_label", "1"),
                label("z_label", "2"),
            ],
            samples: vec![Sample {
                value: 1.0,
                timestamp: 1,
            }],
        }],
        metadata: vec![],
    };
    let unsorted = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![
                label("z_label", "2"),
                label("__name__", "up"),
                label("a_label", "1"),
            ],
            samples: vec![Sample {
                value: 1.0,
                timestamp: 1,
            }],
        }],
        metadata: vec![],
    };
    let sorted_out = parse(&sorted, 0).expect("within the expansion budget");
    let unsorted_out = parse(&unsorted, 0).expect("within the expansion budget");
    assert_eq!(
        sorted_out.samples[0].fingerprint,
        unsorted_out.samples[0].fingerprint
    );
    assert_eq!(sorted_out.rejected, 0);
    assert_eq!(unsorted_out.rejected, 0);
}

#[test]
fn malformed_snappy_is_a_whole_request_decompress_error() {
    let err = decompress(Encoding::Snappy, b"\xFF\xFF\xFF\xFF not snappy").unwrap_err();
    assert!(matches!(
        err,
        pulsus_write::LogsIngestError::Decompress {
            encoding: "snappy",
            ..
        }
    ));
}

#[test]
fn malformed_protobuf_after_valid_snappy_is_a_whole_request_decode_error() {
    let compressed = snappy_compress(b"not a valid WriteRequest protobuf \xFF\xFF");
    let decompressed = decompress(Encoding::Snappy, &compressed).expect("valid snappy");
    let err = decode(&decompressed).expect_err("not a valid WriteRequest");
    assert!(matches!(err, pulsus_write::LogsIngestError::Decode(_)));
}
