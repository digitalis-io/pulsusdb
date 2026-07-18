//! Prometheus remote-write parser (issue #28 architect plan, docs/
//! architecture.md §4): a pure `bytes -> WriteRequest -> ParsedMetrics`
//! pipeline with no I/O — structurally identical to `otlp_metrics`'s
//! decode/parse split, but simpler: remote-write arrives **pre-flattened**
//! (a histogram's `_bucket`/`_sum`/`_count` and a summary's quantile series
//! are already distinct `TimeSeries`, each carrying its own `__name__` and
//! `le`/`quantile` labels), so there is no per-type flattening, no
//! temporality, no exponential-bucket math — just `__name__` extraction,
//! label normalization through the frozen `LabelSet::from_normalized`,
//! `metric_fingerprint`, and verbatim `(ms, value)` samples.
//!
//! ## Wire types: hand-rolled prompb structs
//!
//! The prompb message set below is the RW-1.0 stable schema, hand-rolled as
//! `#[derive(::prost::Message)]` structs at their exact field tags —
//! mirroring the hand-rolled `google.rpc.Status` in `ingest/http.rs` — no
//! protoc/build-dep, no new crate dependency (`prost`/`snap` are already
//! `pulsus-write` deps). `exemplars` (`TimeSeries` tag 3) and native/RW-2.0
//! histograms (`TimeSeries` tag 4) are intentionally undeclared: `prost`
//! silently skips unknown fields on decode, and both are out of scope (M7).
//!
//! Tag layout is pinned by the architect plan and cross-checked against a
//! real capture from the OpenTelemetry Collector's `prometheusremotewrite`
//! exporter (`tests/fixtures/remote-write/README.md`) — a self-consistent
//! wrong tag would decode without error but silently corrupt every field
//! after it, which only a real-wire fixture (not a synthetic round-trip
//! through the same structs) can catch.

use std::collections::HashSet;
use std::sync::Arc;

use prost::Message;
use pulsus_model::{Fingerprint, LabelSet, METRIC_NAME_LABEL, metric_fingerprint};

use crate::error::LogsIngestError;
use crate::ingest::metrics::{MetricMetadata, MetricPoint, ParsedMetrics, SeriesRef};

/// `prompb.WriteRequest` (RW-1.0): `timeseries` at tag 1, `metadata` at tag
/// 3 (tag 2 is reserved on the wire for a Cortex-specific source marker,
/// never populated by a standard sender and never read here).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
    #[prost(message, repeated, tag = "3")]
    pub metadata: Vec<MetricMetadataProto>,
}

/// `prompb.TimeSeries`: `labels` at tag 1, `samples` at tag 2.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

/// `prompb.Label`: `name` at tag 1, `value` at tag 2.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// `prompb.Sample`: `value` (a `double`) at tag 1, `timestamp` (milliseconds
/// since the Unix epoch) at tag 2.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

/// `prompb.MetricMetadata`: `type` at tag 1, `metric_family_name` at tag 2,
/// `help` at tag 4, `unit` at tag 5 (tag 3 is a gap in the upstream schema —
/// no field was ever assigned it). Named `MetricMetadataProto` (not
/// `MetricMetadata`) to avoid colliding with `crate::ingest::metrics::
/// MetricMetadata`, the seam type [`parse`] produces from this wire type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetricMetadataProto {
    #[prost(int32, tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub metric_family_name: String,
    #[prost(string, tag = "4")]
    pub help: String,
    #[prost(string, tag = "5")]
    pub unit: String,
}

/// Decode-time structural DoS guards (issue #28 code review hardening
/// finding): generous, documented per-request bounds on repeated-field
/// counts, sized so no legitimate remote-write batch ever approaches them.
/// A raw body is already capped at 64 MiB decompressed
/// (`crate::ingest::decompress::MAX_DECOMPRESSED_BYTES`), but that byte cap
/// alone does not bound the *decoded* structure's size: many minimal-length
/// repeated submessages (e.g. a `TimeSeries` with no labels/samples costs
/// only a couple of wire bytes but ~50+ heap-adjacent bytes once decoded
/// into a `Vec<TimeSeries>` entry) let a 64 MiB body unpack into a far
/// larger in-memory structure. Checked in [`decode`] immediately after
/// `WriteRequest::decode` succeeds — before [`parse`] performs any further
/// per-element allocation (label-set construction, fingerprinting, output
/// row materialization).
pub const MAX_TIMESERIES_PER_REQUEST: usize = 1_000_000;
/// See [`MAX_TIMESERIES_PER_REQUEST`]'s doc comment.
pub const MAX_LABELS_PER_SERIES: usize = 256;
/// See [`MAX_TIMESERIES_PER_REQUEST`]'s doc comment.
pub const MAX_SAMPLES_PER_SERIES: usize = 100_000;
/// See [`MAX_TIMESERIES_PER_REQUEST`]'s doc comment.
pub const MAX_METADATA_PER_REQUEST: usize = 10_000;

/// The per-request cap on [`parse`]'s **estimated expanded output bytes**
/// (issue #62). Own constant, same value and derivation as
/// `otlp_metrics::MAX_EXPANDED_BYTES` / `otlp_traces::MAX_EXPANDED_BYTES`
/// (4× the 64 MiB decompressed body cap = 256 MiB). The
/// [`MAX_TIMESERIES_PER_REQUEST`]-family caps bound each *dimension*
/// (series × labels × samples-per-series) but NOT aggregate output: a
/// minimal wire `Sample` is 2 bytes (empty body — `value`/`timestamp` are
/// proto3 defaults) yet decodes to one ~40-byte `MetricPoint`, so a 64 MiB
/// body of ~33.5M such samples packs into ≈ 336 series (each ≤ 100k) —
/// far under the 1M-timeseries cap — while materializing ≈ 1.25 GiB of
/// output. This byte budget bounds the total: it admits ≤
/// `MAX_EXPANDED_BYTES / SAMPLE_ROW_OVERHEAD` ≈ 4.2M samples (≈ 256 MiB),
/// far above Prometheus's `max_samples_per_send` default of 2,000 — an
/// order-of-magnitude DoS guard, not a tight quota.
pub const MAX_EXPANDED_BYTES: usize = 4 * crate::ingest::decompress::MAX_DECOMPRESSED_BYTES;

/// Estimated fixed heap cost of one emitted [`MetricPoint`]: `metric_name`
/// `Arc<str>` (shared per series, not per sample) + fingerprint +
/// `unix_milli` + `value` ≈ 40 bytes, floored to a round constant. The
/// dominant multiplicative term (one per wire sample).
const SAMPLE_ROW_OVERHEAD: usize = 64;
/// Estimated fixed heap cost of one [`SeriesRef`] beyond its label bytes.
const SERIES_ROW_OVERHEAD: usize = 64;
/// Estimated fixed heap cost of one [`MetricMetadata`] beyond its
/// name/help/unit bytes.
const META_ROW_OVERHEAD: usize = 64;

/// Adds `amount` to the running expansion estimate and fails the whole
/// request the moment it exceeds [`MAX_EXPANDED_BYTES`] (issue #62) — the
/// single charge/check point every materialization site reserves through
/// before allocating. Identical body to `otlp_metrics::charge_budget`
/// (remote-write labels are already `String`s, charged 1× — no `AnyValue`
/// expansion factors).
fn charge_budget(expanded_bytes: &mut usize, amount: usize) -> Result<(), LogsIngestError> {
    *expanded_bytes = expanded_bytes.saturating_add(amount);
    if *expanded_bytes > MAX_EXPANDED_BYTES {
        return Err(LogsIngestError::OversizeMessage {
            field: "expanded metric row bytes (estimated)",
            limit: MAX_EXPANDED_BYTES,
            actual: *expanded_bytes,
        });
    }
    Ok(())
}

/// Decodes a (decompressed) `POST /api/v1/write` request body, then applies
/// the [`MAX_TIMESERIES_PER_REQUEST`]-family structural bounds. The sole
/// decode boundary: a malformed/truncated protobuf, or a message exceeding
/// one of those bounds, is a whole-request, atomic failure (mirrors
/// `otlp_metrics::decode`) — never partially applied.
pub fn decode(body: &[u8]) -> Result<WriteRequest, LogsIngestError> {
    let req = WriteRequest::decode(body)?;
    validate_bounds(&req)?;
    Ok(req)
}

/// Enforces the [`MAX_TIMESERIES_PER_REQUEST`]-family bounds, failing fast
/// on the first field that exceeds its limit (message-level fields before
/// per-series fields, so a request with too many series is rejected before
/// this function ever inspects any individual series' labels/samples).
fn validate_bounds(req: &WriteRequest) -> Result<(), LogsIngestError> {
    if req.timeseries.len() > MAX_TIMESERIES_PER_REQUEST {
        return Err(LogsIngestError::OversizeMessage {
            field: "timeseries",
            limit: MAX_TIMESERIES_PER_REQUEST,
            actual: req.timeseries.len(),
        });
    }
    if req.metadata.len() > MAX_METADATA_PER_REQUEST {
        return Err(LogsIngestError::OversizeMessage {
            field: "metadata",
            limit: MAX_METADATA_PER_REQUEST,
            actual: req.metadata.len(),
        });
    }
    for ts in &req.timeseries {
        if ts.labels.len() > MAX_LABELS_PER_SERIES {
            return Err(LogsIngestError::OversizeMessage {
                field: "labels",
                limit: MAX_LABELS_PER_SERIES,
                actual: ts.labels.len(),
            });
        }
        if ts.samples.len() > MAX_SAMPLES_PER_SERIES {
            return Err(LogsIngestError::OversizeMessage {
                field: "samples",
                limit: MAX_SAMPLES_PER_SERIES,
                actual: ts.samples.len(),
            });
        }
    }
    Ok(())
}

/// Maps a `prompb.MetricMetadata.type` wire value to the same lowercase
/// Prometheus exposition-format type string `otlp_metrics::parse` emits
/// (architect plan's pinned table) — cross-transport `metric_metadata.
/// metric_type` parity is a hard invariant (docs/schemas.md §2.1; the
/// planner keys counter-function legality off these strings). An
/// out-of-range value (outside the eight defined `prompb.MetricType`
/// values) degrades to `"unknown"` rather than a decode error — a forward-
/// compatible unknown type on the wire must not fail the whole request.
fn metric_type_name(t: i32) -> &'static str {
    match t {
        1 => "counter",
        2 => "gauge",
        3 => "histogram",
        4 => "gaugehistogram",
        5 => "summary",
        6 => "info",
        7 => "stateset",
        _ => "unknown",
    }
}

/// Parses a decoded `WriteRequest` into normalized rows. Pure: a function
/// of `req` and `now_ns` only, no I/O, no clock reads — the caller (the
/// ingest handler) is the only clock/IO boundary. `now_ns` becomes every
/// metadata row's `updated_ns` (the `ReplacingMergeTree` version column,
/// issue #26 amendment).
///
/// `Err` iff the request's estimated expanded output exceeds
/// [`MAX_EXPANDED_BYTES`] (issue #62) — a whole-request, atomic structural
/// failure, exactly like a decode/bounds error; everything else (a series
/// missing `__name__`) stays a per-series drop counted in `rejected` inside
/// the `Ok`.
pub fn parse(req: &WriteRequest, now_ns: i64) -> Result<ParsedMetrics, LogsIngestError> {
    let mut out = ParsedMetrics::default();
    let mut expanded_bytes: usize = 0;
    // Dedups `SeriesRef` registration within this request by `(metric_name,
    // fingerprint)` — a labels carrier, not a per-sample registration
    // (mirrors `otlp_metrics::parse`).
    let mut seen_series: HashSet<(Arc<str>, Fingerprint)> = HashSet::new();

    for ts in &req.timeseries {
        parse_time_series(&mut out, &mut expanded_bytes, &mut seen_series, ts)?;
    }

    // Metadata dedup within-request by family name, last-wins (architect
    // plan) — a later entry for the same name overwrites an earlier one
    // rather than both being emitted; `metric_family_name` is used verbatim
    // as `metric_name` (RW carries the base family name explicitly, unlike
    // OTLP where a suffix must never be stripped either — there is simply
    // no suffix to strip here).
    let mut by_name: std::collections::HashMap<Arc<str>, usize> = std::collections::HashMap::new();
    for meta in &req.metadata {
        // Charge the metadata row BEFORE building it (issue #62).
        charge_budget(
            &mut expanded_bytes,
            META_ROW_OVERHEAD + meta.metric_family_name.len() + meta.help.len() + meta.unit.len(),
        )?;
        let name: Arc<str> = Arc::from(meta.metric_family_name.as_str());
        let row = MetricMetadata {
            metric_name: Arc::clone(&name),
            metric_type: metric_type_name(meta.r#type).to_string(),
            help: meta.help.clone(),
            unit: meta.unit.clone(),
            updated_ns: now_ns,
        };
        match by_name.get(&name) {
            Some(&idx) => out.metadata[idx] = row,
            None => {
                by_name.insert(name, out.metadata.len());
                out.metadata.push(row);
            }
        }
    }

    Ok(out)
}

/// Parses one `TimeSeries`: extracts `__name__` (missing/empty -> drop the
/// whole series, `rejected += sample_count` — the only semantic per-series
/// violation remote-write has, architect plan's reject-boundary rule),
/// normalizes the remaining labels, fingerprints them, and emits one
/// [`MetricPoint`] per sample plus (if it has >=1 accepted sample) one
/// [`SeriesRef`] for the series.
fn parse_time_series(
    out: &mut ParsedMetrics,
    expanded_bytes: &mut usize,
    seen_series: &mut HashSet<(Arc<str>, Fingerprint)>,
    ts: &TimeSeries,
) -> Result<(), LogsIngestError> {
    // Charge this series' label/`SeriesRef` materialization BEFORE building
    // `rest`/`from_normalized` (issue #62). Allocation-free: sums wire
    // string lengths only.
    let label_charge = ts.labels.iter().fold(SERIES_ROW_OVERHEAD, |acc, l| {
        acc.saturating_add(l.name.len())
            .saturating_add(l.value.len())
    });
    charge_budget(expanded_bytes, label_charge)?;

    let mut name: Option<&str> = None;
    let mut rest: Vec<(String, String)> = Vec::with_capacity(ts.labels.len());
    for label in &ts.labels {
        if label.name == METRIC_NAME_LABEL {
            name = Some(label.value.as_str());
        } else {
            rest.push((label.name.clone(), label.value.clone()));
        }
    }

    let Some(name) = name.filter(|n| !n.is_empty()) else {
        out.rejected += ts.samples.len() as u64;
        if out.rejected_message.is_none() {
            out.rejected_message = Some(
                "time series has no __name__ label (or it is empty): series dropped".to_string(),
            );
        }
        return Ok(());
    };
    let metric_name: Arc<str> = Arc::from(name);

    let (labels, collisions) = LabelSet::from_normalized(rest);
    out.collisions += collisions as u64;
    let fingerprint = metric_fingerprint(&labels);

    // A sampleless series (legal on the wire, e.g. a metadata-only push)
    // registers no `SeriesRef` — the writer derives `metric_series` rows
    // from `ParsedMetrics::samples`' timestamps, so a series with zero
    // accepted samples would yield no row anyway (architect plan).
    if !ts.samples.is_empty() && seen_series.insert((Arc::clone(&metric_name), fingerprint)) {
        out.series.push(SeriesRef {
            metric_name: Arc::clone(&metric_name),
            fingerprint,
            labels,
        });
    }

    for sample in &ts.samples {
        // Charge each sample BEFORE pushing it (issue #62): the dominant
        // multiplicative term (a 2-byte wire sample → one ~40-byte
        // `MetricPoint`), so a 33.5M-sample fan-out aborts here before mass
        // materialization.
        charge_budget(expanded_bytes, SAMPLE_ROW_OVERHEAD)?;
        out.samples.push(MetricPoint {
            metric_name: Arc::clone(&metric_name),
            fingerprint,
            // Verbatim: remote-write timestamps are already milliseconds,
            // with no `0`-is-unset sentinel (unlike OTLP's nanosecond
            // `time_unix_nano`, architect plan) — `0` is a literal 1970
            // timestamp here, not a rejection trigger.
            unix_milli: sample.timestamp,
            value: sample.value,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_model::STALE_NAN_BITS;

    fn label(name: &str, value: &str) -> Label {
        Label {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn sample(value: f64, timestamp: i64) -> Sample {
        Sample { value, timestamp }
    }

    // -- decode -----------------------------------------------------------

    #[test]
    fn decode_rejects_malformed_bytes() {
        let err = decode(b"\xFF\xFF\xFF not a protobuf message").unwrap_err();
        assert!(matches!(err, LogsIngestError::Decode(_)));
    }

    #[test]
    fn decode_round_trips_an_encoded_request() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(1.0, 1)],
            }],
            metadata: vec![],
        };
        let bytes = req.encode_to_vec();
        let decoded = decode(&bytes).expect("valid protobuf decodes");
        assert_eq!(decoded, req);
    }

    // -- decode-time structural bounds (issue #28 code review hardening) --

    #[test]
    fn validate_bounds_accepts_a_request_within_every_limit() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(1.0, 1)],
            }],
            metadata: vec![],
        };
        assert!(validate_bounds(&req).is_ok());
    }

    #[test]
    fn validate_bounds_rejects_too_many_timeseries() {
        let req = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![],
                    samples: vec![],
                };
                MAX_TIMESERIES_PER_REQUEST + 1
            ],
            metadata: vec![],
        };
        let err = validate_bounds(&req).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "timeseries",
                limit: MAX_TIMESERIES_PER_REQUEST,
                actual,
            } if actual == MAX_TIMESERIES_PER_REQUEST + 1
        ));
    }

    #[test]
    fn validate_bounds_rejects_too_many_labels_in_one_series() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("k", "v"); MAX_LABELS_PER_SERIES + 1],
                samples: vec![],
            }],
            metadata: vec![],
        };
        let err = validate_bounds(&req).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "labels",
                ..
            }
        ));
    }

    #[test]
    fn validate_bounds_rejects_too_many_samples_in_one_series() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![],
                samples: vec![sample(1.0, 1); MAX_SAMPLES_PER_SERIES + 1],
            }],
            metadata: vec![],
        };
        let err = validate_bounds(&req).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "samples",
                ..
            }
        ));
    }

    #[test]
    fn validate_bounds_rejects_too_much_metadata() {
        let entry = MetricMetadataProto {
            r#type: 0,
            metric_family_name: String::new(),
            help: String::new(),
            unit: String::new(),
        };
        let req = WriteRequest {
            timeseries: vec![],
            metadata: vec![entry; MAX_METADATA_PER_REQUEST + 1],
        };
        let err = validate_bounds(&req).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "metadata",
                ..
            }
        ));
    }

    /// Proves the bound is actually wired into the public [`decode`]
    /// boundary (not just callable directly, same guard `LogsIngestError`
    /// classifies as a whole-request `400`), by round-tripping a too-large
    /// request through real protobuf encode/decode.
    #[test]
    fn decode_enforces_the_timeseries_bound_end_to_end() {
        let req = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![],
                    samples: vec![],
                };
                MAX_TIMESERIES_PER_REQUEST + 1
            ],
            metadata: vec![],
        };
        let bytes = req.encode_to_vec();
        let err = decode(&bytes).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "timeseries",
                ..
            }
        ));
    }

    // -- parse: basic series ----------------------------------------------

    #[test]
    fn parse_of_empty_request_returns_empty_output() {
        let out = parse(
            &WriteRequest {
                timeseries: vec![],
                metadata: vec![],
            },
            1_000,
        )
        .expect("within the expansion budget");
        assert_eq!(out, ParsedMetrics::default());
    }

    #[test]
    fn parse_is_a_pure_function_of_its_arguments() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up"), label("job", "checkout")],
                samples: vec![sample(1.0, 1_700_000_000_000)],
            }],
            metadata: vec![],
        };
        let a = parse(&req, 42).expect("within the expansion budget");
        let b = parse(&req, 42).expect("within the expansion budget");
        assert_eq!(a, b);
    }

    #[test]
    fn time_series_extracts_name_and_fingerprints_remaining_labels() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    label("__name__", "http_requests_total"),
                    label("job", "checkout"),
                    label("method", "GET"),
                ],
                samples: vec![sample(42.0, 1_700_000_000_000)],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.samples.len(), 1);
        assert_eq!(&*out.samples[0].metric_name, "http_requests_total");
        assert_eq!(out.samples[0].value, 42.0);
        assert_eq!(out.samples[0].unix_milli, 1_700_000_000_000);
        assert_eq!(out.series.len(), 1);
        assert_eq!(out.series[0].labels.get("job"), Some("checkout"));
        assert_eq!(out.series[0].labels.get("method"), Some("GET"));
        // `__name__` never enters the LabelSet (architect plan).
        assert_eq!(out.series[0].labels.get("__name__"), None);
    }

    #[test]
    fn multiple_samples_on_one_series_share_one_series_ref() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(1.0, 1), sample(2.0, 2), sample(3.0, 3)],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.samples.len(), 3);
        assert_eq!(out.series.len(), 1);
    }

    #[test]
    fn a_sampleless_series_emits_no_series_ref() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert!(out.samples.is_empty());
        assert!(out.series.is_empty());
    }

    // -- reject boundary: missing/empty __name__ ---------------------------

    #[test]
    fn missing_name_label_drops_the_series_and_counts_rejected_samples() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("job", "checkout")],
                samples: vec![sample(1.0, 1), sample(2.0, 2)],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 2);
        assert!(out.samples.is_empty());
        assert!(out.series.is_empty());
        assert!(out.rejected_message.is_some());
    }

    #[test]
    fn empty_name_label_value_drops_the_series() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "")],
                samples: vec![sample(1.0, 1)],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 1);
        assert!(out.samples.is_empty());
    }

    #[test]
    fn one_bad_series_does_not_reject_the_rest_of_the_request() {
        let req = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![label("job", "checkout")],
                    samples: vec![sample(1.0, 1)],
                },
                TimeSeries {
                    labels: vec![label("__name__", "up")],
                    samples: vec![sample(1.0, 1)],
                },
            ],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 1);
        assert_eq!(out.samples.len(), 1);
        assert_eq!(&*out.samples[0].metric_name, "up");
    }

    // -- timestamps verbatim, no sentinel -----------------------------------

    #[test]
    fn zero_timestamp_is_accepted_verbatim_no_sentinel_rule() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(1.0, 0)],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 0);
        assert_eq!(out.samples[0].unix_milli, 0);
    }

    #[test]
    fn negative_timestamp_is_accepted_verbatim() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(1.0, -1_000)],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 0);
        assert_eq!(out.samples[0].unix_milli, -1_000);
    }

    // -- stale marker --------------------------------------------------------

    #[test]
    fn stale_nan_sample_survives_bit_exact() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(f64::from_bits(STALE_NAN_BITS), 1)],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.samples[0].value.to_bits(), STALE_NAN_BITS);
    }

    // -- label normalization / fingerprint identity --------------------------

    #[test]
    fn unsorted_wire_labels_are_accepted_and_resorted_deterministically() {
        let req_a = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    label("__name__", "up"),
                    label("z_label", "1"),
                    label("a_label", "2"),
                ],
                samples: vec![sample(1.0, 1)],
            }],
            metadata: vec![],
        };
        let req_b = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    label("a_label", "2"),
                    label("__name__", "up"),
                    label("z_label", "1"),
                ],
                samples: vec![sample(1.0, 1)],
            }],
            metadata: vec![],
        };
        let out_a = parse(&req_a, 0).expect("within the expansion budget");
        let out_b = parse(&req_b, 0).expect("within the expansion budget");
        assert_eq!(out_a.samples[0].fingerprint, out_b.samples[0].fingerprint);
        assert_eq!(
            out_a.series[0].labels.iter().collect::<Vec<_>>(),
            vec![("a_label", "2"), ("z_label", "1")]
        );
    }

    #[test]
    fn dotted_and_underscored_labels_fingerprint_identically_cross_transport_identity() {
        let req_dot = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up"), label("service.name", "checkout")],
                samples: vec![sample(1.0, 1)],
            }],
            metadata: vec![],
        };
        let req_underscore = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up"), label("service_name", "checkout")],
                samples: vec![sample(1.0, 1)],
            }],
            metadata: vec![],
        };
        let out_dot = parse(&req_dot, 0).expect("within the expansion budget");
        let out_underscore = parse(&req_underscore, 0).expect("within the expansion budget");
        assert_eq!(
            out_dot.samples[0].fingerprint,
            out_underscore.samples[0].fingerprint
        );
    }

    #[test]
    fn le_and_quantile_remain_ordinary_labels_in_the_fingerprint() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "latency_bucket"), label("le", "0.5")],
                samples: vec![sample(3.0, 1)],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.series[0].labels.get("le"), Some("0.5"));
    }

    // -- metadata ------------------------------------------------------------

    #[test]
    fn metadata_maps_every_documented_type_string() {
        let cases: &[(i32, &str)] = &[
            (0, "unknown"),
            (1, "counter"),
            (2, "gauge"),
            (3, "histogram"),
            (4, "gaugehistogram"),
            (5, "summary"),
            (6, "info"),
            (7, "stateset"),
            (99, "unknown"),
        ];
        for &(wire_type, expected) in cases {
            assert_eq!(metric_type_name(wire_type), expected);
        }
    }

    #[test]
    fn metadata_entry_maps_to_the_seam_type_with_injected_updated_ns() {
        let req = WriteRequest {
            timeseries: vec![],
            metadata: vec![MetricMetadataProto {
                r#type: 1,
                metric_family_name: "http_requests_total".to_string(),
                help: "total requests".to_string(),
                unit: "".to_string(),
            }],
        };
        let out = parse(&req, 123).expect("within the expansion budget");
        assert_eq!(out.metadata.len(), 1);
        assert_eq!(&*out.metadata[0].metric_name, "http_requests_total");
        assert_eq!(out.metadata[0].metric_type, "counter");
        assert_eq!(out.metadata[0].help, "total requests");
        assert_eq!(out.metadata[0].updated_ns, 123);
    }

    #[test]
    fn metadata_family_name_is_used_verbatim_no_suffix_stripping() {
        let req = WriteRequest {
            timeseries: vec![],
            metadata: vec![MetricMetadataProto {
                r#type: 3,
                metric_family_name: "latency".to_string(),
                help: String::new(),
                unit: String::new(),
            }],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(&*out.metadata[0].metric_name, "latency");
        assert_eq!(out.metadata[0].metric_type, "histogram");
    }

    #[test]
    fn duplicate_metadata_family_name_dedups_last_wins() {
        let req = WriteRequest {
            timeseries: vec![],
            metadata: vec![
                MetricMetadataProto {
                    r#type: 2,
                    metric_family_name: "up".to_string(),
                    help: "first".to_string(),
                    unit: String::new(),
                },
                MetricMetadataProto {
                    r#type: 2,
                    metric_family_name: "up".to_string(),
                    help: "second".to_string(),
                    unit: String::new(),
                },
            ],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.metadata.len(), 1);
        assert_eq!(out.metadata[0].help, "second");
    }

    // -- expansion budget (issue #62) -------------------------------------

    /// A single named series carrying more than the admissible ~4.2M-sample
    /// ceiling trips [`MAX_EXPANDED_BYTES`] (issue #62 Δ1) — the per-sample
    /// caps (per-series bounds) do not stop it, only this cumulative byte
    /// budget does. The `actual <= limit + SAMPLE_ROW_OVERHEAD` bound proves
    /// charge-before-materialize: each sample is charged (and the abort
    /// fires) BEFORE its `MetricPoint` is pushed, so materialization stops at
    /// the tipping sample rather than after the whole fan-out. Sample count
    /// derives from the constants so a retune cannot silently weaken it.
    #[test]
    fn expansion_budget_rejects_sample_fan_out() {
        let sample_count = MAX_EXPANDED_BYTES / SAMPLE_ROW_OVERHEAD + 2;
        let samples: Vec<Sample> = (0..sample_count as i64).map(|i| sample(0.0, i)).collect();
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples,
            }],
            metadata: vec![],
        };

        let err = parse(&req, 0).expect_err("sample fan-out must trip the expansion budget");
        let LogsIngestError::OversizeMessage { limit, actual, .. } = err else {
            panic!("unexpected error: {err}");
        };
        assert_eq!(limit, MAX_EXPANDED_BYTES);
        assert!(actual > MAX_EXPANDED_BYTES);
        assert!(
            actual <= MAX_EXPANDED_BYTES + SAMPLE_ROW_OVERHEAD,
            "abort must fire at the tipping sample charge (charge-before-materialize): \
             actual={actual}"
        );
    }

    /// The budget is a whole-request bound, not a per-series truncation: an
    /// ordinary request (multiple series, samples, metadata) parses `Ok`.
    #[test]
    fn expansion_budget_admits_ordinary_request() {
        let req = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![label("__name__", "up"), label("job", "checkout")],
                    samples: vec![sample(1.0, 1), sample(2.0, 2)],
                },
                TimeSeries {
                    labels: vec![label("__name__", "latency_bucket"), label("le", "0.5")],
                    samples: vec![sample(3.0, 1)],
                },
            ],
            metadata: vec![MetricMetadataProto {
                r#type: 1,
                metric_family_name: "up".to_string(),
                help: "total".to_string(),
                unit: String::new(),
            }],
        };
        let out = parse(&req, 0).expect("ordinary request is within the budget");
        assert_eq!(out.samples.len(), 3);
        assert_eq!(out.series.len(), 2);
        assert_eq!(out.metadata.len(), 1);
    }
}
