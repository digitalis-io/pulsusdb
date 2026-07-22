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

use prost::Message;
use pulsus_model::{CounterResetHint, STALE_NAN_BITS};
use pulsus_write::ingest::decompress::{Encoding, decompress};
use pulsus_write::protocols::remote_write::{
    BucketSpan, Histogram, HistogramCount, Label, Sample, TimeSeries, WriteRequest, decode, parse,
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

/// Issue #140 AC 7: `native_histogram.bin` is a **real capture** from
/// Prometheus 3.13.0's own remote-write sender (`send_native_histograms:
/// true`, provenance: `tests/fixtures/remote-write/README.md`) carrying one
/// integer native histogram. It pins the hand-rolled `prompb.Histogram`/
/// `BucketSpan` tag layout — including every zigzag field at a NEGATIVE
/// value (`schema` −2 sint32, positive span `offset` −2 sint32, a −1
/// positive delta sint64) — against genuine wire bytes: a self-consistent
/// wrong tag or a plain-int mis-declaration of a sint field would decode
/// without error but corrupt exactly these values, which a synthetic
/// round-trip through the same structs cannot catch.
#[test]
fn native_histogram_fixture_decodes_the_real_prometheus_wire_layout_exactly() {
    let req = decode_fixture("native_histogram.bin");
    assert_eq!(req.timeseries.len(), 1);
    let ts = &req.timeseries[0];
    assert!(ts.samples.is_empty(), "a histogram-only series on the wire");
    assert_eq!(ts.histograms.len(), 1);
    let h = &ts.histograms[0];

    // Pinned decoded values (recorded at capture time; the OTLP →
    // Prometheus ingest translation shifts each exponential bucket offset
    // by +1 and the negative-side sign flip mirrors it).
    assert_eq!(h.count, Some(HistogramCount::Int(6)));
    assert_eq!(h.sum.to_bits(), 10.5f64.to_bits());
    assert_eq!(
        h.schema, -2,
        "sint32 zigzag schema decodes to its negative value"
    );
    assert_eq!(h.zero_threshold.to_bits(), 1e-128f64.to_bits());
    assert_eq!(h.zero_count, Some(HistogramCount::Int(1)));
    assert_eq!(
        h.positive_spans,
        vec![BucketSpan {
            offset: -2,
            length: 3
        }],
        "sint32 zigzag span offset decodes to its negative value"
    );
    assert_eq!(
        h.positive_deltas,
        vec![1, -1, 2],
        "sint64 zigzag packed deltas decode signed values exactly"
    );
    assert_eq!(
        h.negative_spans,
        vec![BucketSpan {
            offset: 1,
            length: 1
        }]
    );
    assert_eq!(h.negative_deltas, vec![2]);
    assert!(
        h.positive_counts.is_empty(),
        "integer flavor: no float counts"
    );
    assert!(h.negative_counts.is_empty());
    assert!(h.custom_values.is_empty());
    assert_eq!(h.reset_hint, 0);
    assert_eq!(h.timestamp, 1_784_717_351_135);

    // Through `parse`: one HistogramPoint, hint Unknown (wire UNKNOWN), the
    // series registered from its histogram alone.
    let out = parse(&req, 0).expect("within the expansion budget");
    assert_eq!(out.rejected, 0);
    assert_eq!(out.hist_samples.len(), 1);
    let point = &out.hist_samples[0];
    assert_eq!(&*point.metric_name, "rw_capture_latency");
    assert_eq!(point.unix_milli, 1_784_717_351_135);
    assert_eq!(
        point.histogram.counter_reset_hint,
        CounterResetHint::Unknown
    );
    assert_eq!(point.histogram.count, 6);
    assert_eq!(point.histogram.positive_buckets, vec![1, -1, 2]);
    assert_eq!(out.series.len(), 1);
    assert_eq!(out.series[0].labels.get("job"), Some("checkout"));
}

/// Issue #140 AC 7 (gauge variant, OQ1 resolution): common senders do not
/// readily emit gauge-hint native histograms, so the GAUGE variant is built
/// programmatically (this file's established "no real-sender equivalent →
/// built in-test" precedent) and pushed through the same
/// `snappy → decompress → decode → parse` path the real capture takes; the
/// tag layout itself is pinned by the real capture above (only the enum
/// value at tag 14 differs).
#[test]
fn synthetic_gauge_hint_body_lands_counter_reset_hint_gauge_through_the_full_path() {
    let req = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![label("__name__", "queue_depth"), label("job", "checkout")],
            samples: vec![],
            histograms: vec![Histogram {
                count: Some(HistogramCount::Int(4)),
                sum: 5.0,
                schema: 0,
                zero_count: Some(HistogramCount::Int(0)),
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 3,
                }],
                positive_deltas: vec![1, 1, -1],
                reset_hint: 3, // GAUGE
                timestamp: 1_700_000_000_000,
                ..Default::default()
            }],
        }],
        metadata: vec![],
    };
    let compressed = snappy_compress(&req.encode_to_vec());

    let decompressed = decompress(Encoding::Snappy, &compressed).expect("valid snappy");
    let decoded = decode(&decompressed).expect("valid WriteRequest");
    let out = parse(&decoded, 0).expect("within the expansion budget");

    assert_eq!(out.rejected, 0);
    assert_eq!(out.hist_samples.len(), 1);
    assert_eq!(
        out.hist_samples[0].histogram.counter_reset_hint,
        CounterResetHint::Gauge,
        "a wire GAUGE reset hint must land CounterResetHint::Gauge"
    );
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
                histograms: vec![],
            },
            TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1,
                }],
                histograms: vec![],
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
            histograms: vec![],
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
            histograms: vec![],
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

// ---------------------------------------------------------------------
// Decode-time byte budget (issue #127, AC 8).
// ---------------------------------------------------------------------

/// Appends a base-128 varint.
fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

/// Hand-rolls one `WriteRequest.timeseries` (tag 1) wire occurrence carrying
/// `labels` empty labels and `samples` empty samples (2 wire bytes each) —
/// the amplified wire is built WITHOUT materializing the equivalent structs,
/// so the test itself stays cheap while the decoder faces the fan-out.
fn wire_series(labels: usize, samples: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(2 * (labels + samples));
    for _ in 0..labels {
        payload.extend_from_slice(&[0x0A, 0x00]); // TimeSeries.labels (tag 1), len 0
    }
    for _ in 0..samples {
        payload.extend_from_slice(&[0x12, 0x00]); // TimeSeries.samples (tag 2), len 0
    }
    let mut out = Vec::with_capacity(payload.len() + 8);
    out.push(0x0A); // WriteRequest.timeseries (tag 1), length-delimited
    put_uvarint(&mut out, payload.len() as u64);
    out.extend_from_slice(&payload);
    out
}

/// Issue #127 AC 8: a request whose decoded-byte estimate exceeds
/// `MAX_DECODED_BYTES` while EVERY element-count cap is respected (labels and
/// samples both at — never over — their per-series and aggregate caps)
/// rejects whole-request with the byte-budget `OversizeMessage` before
/// `parse` runs. All sizing is derived from the caps and `size_of` (no
/// literals) and the over-budget precondition is self-asserted.
#[test]
fn over_budget_decode_rejects_decoded_bytes_before_parse() {
    use pulsus_write::LogsIngestError;
    use pulsus_write::protocols::otlp_prescan::MAX_DECODED_BYTES;
    use pulsus_write::protocols::remote_write::{
        MAX_LABELS_PER_SERIES, MAX_SAMPLES_PER_SERIES, MAX_TIMESERIES_PER_REQUEST,
        MAX_TOTAL_LABELS_PER_REQUEST, MAX_TOTAL_SAMPLES_PER_REQUEST,
    };

    // As many full-to-the-per-series-cap series as the aggregates admit.
    let label_series = MAX_TOTAL_LABELS_PER_REQUEST / MAX_LABELS_PER_SERIES;
    let sample_series = MAX_TOTAL_SAMPLES_PER_REQUEST / MAX_SAMPLES_PER_SERIES;
    // Self-asserted preconditions: no count cap can fire...
    assert!(label_series * MAX_LABELS_PER_SERIES <= MAX_TOTAL_LABELS_PER_REQUEST);
    assert!(sample_series * MAX_SAMPLES_PER_SERIES <= MAX_TOTAL_SAMPLES_PER_REQUEST);
    assert!(label_series + sample_series <= MAX_TIMESERIES_PER_REQUEST);
    // ...while the decoded-byte estimate is over budget by construction.
    let estimate = label_series
        * (std::mem::size_of::<TimeSeries>()
            + MAX_LABELS_PER_SERIES * std::mem::size_of::<Label>())
        + sample_series
            * (std::mem::size_of::<TimeSeries>()
                + MAX_SAMPLES_PER_SERIES * std::mem::size_of::<Sample>());
    assert!(
        estimate > MAX_DECODED_BYTES,
        "fixture must exceed the byte budget by construction (estimate {estimate})"
    );

    let mut body = Vec::new();
    for _ in 0..label_series {
        body.extend_from_slice(&wire_series(MAX_LABELS_PER_SERIES, 0));
    }
    for _ in 0..sample_series {
        body.extend_from_slice(&wire_series(0, MAX_SAMPLES_PER_SERIES));
    }

    match decode(&body) {
        Err(LogsIngestError::OversizeMessage {
            field,
            limit,
            actual,
        }) => {
            assert_eq!(
                field, "decoded bytes (estimated)",
                "the BYTE budget must fire, not a count cap"
            );
            assert_eq!(limit, MAX_DECODED_BYTES);
            assert!(actual > limit);
        }
        other => panic!("over-budget request must reject whole-request, got {other:?}"),
    }
}
