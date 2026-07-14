# OTLP metrics fixtures (issue #27)

`ExportMetricsServiceRequest` protobuf payloads used by
`crates/pulsus-write/tests/otlp_metrics_fixtures.rs`.

A separate subdir from `../otlp/` (task-manager resolution, issue #27 open
question #4): `../otlp/` and `tests/ingest_fidelity.rs` are the logs
receiver's fixture corpus (issue #16), built concurrently — this directory
avoids clashing with that work.

## Provenance

Same construction method as `../README.md`'s OTLP logs fixtures: there is
no live OpenTelemetry Collector reachable from this repository's sandboxed
CI/dev environment, so these are **not** a packet capture from a real
collector. They are constructed programmatically, directly against this
crate's own `opentelemetry-proto` dependency (the exact wire types a
collector's `MetricsServiceClient::export` call sends), then
`prost`-encoded to bytes — wire-format-identical to what a real collector
would send.

`malformed.bin` is the one exception: a real `ExportMetricsServiceRequest`
encoding, truncated to half its length, so it is guaranteed to fail to
decode (an unexpected-EOF `DecodeError`) rather than accidentally still
being valid.

## Regenerating

The builder functions and the fixture list live in
`crates/pulsus-write/tests/otlp_metrics_fixtures.rs`, in a `#[ignore]`-gated
test (`regenerate_fixtures`). After editing a builder function:

```sh
cargo test -p pulsus-write --test otlp_metrics_fixtures -- --ignored regenerate_fixtures
```

then review and commit the resulting `.bin` diffs alongside the builder
change.

## Fixture index

| File | Covers |
|------|--------|
| `gauge.bin` | a `Gauge` data point flattens to one sample named verbatim; resource `service.name` normalizes into the `service_name` label; metadata type `gauge`, `help`/`unit` forwarded from the descriptor. |
| `sum_counter.bin` | a monotonic, cumulative `Sum` data point flattens to one sample; metadata type `counter` (monotonic Sum, not `gauge`). |
| `histogram.bin` | a classic `Histogram` data point flattens to cumulative `_bucket{le}` series (exact `le` labels, monotonic non-decreasing), `_sum`, `_count`; `_bucket{le="+Inf"}` equals `_count` by construction. |
| `histogram_count_mismatch.bin` | a `Histogram` data point whose `bucket_counts` sum (10) disagrees with the reported `count` (99) — rejected wholesale into partial success, zero samples emitted (architect plan amendment invariant). |
| `histogram_bucket_overflow.bin` | a `Histogram` data point whose `bucket_counts` sum past `u64::MAX` — rejected the same way as a count mismatch, never panics or silently wraps (code review finding 1). |
| `histogram_bucketless.bin` | a `Histogram` data point with no bucket distribution (`bucket_counts`/`explicit_bounds` both empty, only `count`/`sum` known) — `_bucket{le="+Inf"}` is still emitted, equal to `_count` (code review finding 2). |
| `exponential_histogram.bin` | an `ExponentialHistogram` data point with positive, negative, *and* zero buckets — flattened bounds, monotonic cumulative `_bucket` series, `_bucket{le="+Inf"}` equals `_count`. |
| `exponential_histogram_extreme_offset.bin` | an `ExponentialHistogram` bucket at `offset = i32::MAX` with a coarse negative `scale` — the bound computation folds to `+Inf` rather than panicking on integer overflow (code review finding 3). |
| `summary.bin` | a `Summary` data point flattens to per-quantile series (`quantile` label) plus `_sum`/`_count`. |
| `delta_temporality.bin` | a `Sum` with `AGGREGATION_TEMPORALITY_DELTA` — the whole metric (all its data points) is rejected into partial success, naming the metric; delta->cumulative conversion is out of scope until M7. |
| `zero_timestamp.bin` | one `Gauge` data point with `time_unix_nano == 0` alongside one valid data point — the malformed point is rejected (partial success), the valid one still parses. |
| `stale_nan.bin` | a `Gauge` data point flagged `NoRecordedValueMask` — the sample's value is the canonical stale-NaN bit pattern (`pulsus_model::STALE_NAN_BITS`), asserted via `.to_bits()`. |
| `fingerprint_dot.bin` / `fingerprint_underscore.bin` | the same logical resource attribute (`service.name` vs `service_name`) — a series has one identity (identical fingerprint) regardless of transport form (AC: "normalized before fingerprinting"). |
| `malformed.bin` | truncated protobuf -> whole-request decode error (maps to HTTP 400 / `google.rpc.Status.code = 3` at the handler). |
