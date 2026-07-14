# Prometheus remote-write fixtures (issue #28)

Snappy-compressed `prompb.WriteRequest` protobuf payloads used by
`crates/pulsus-write/tests/remote_write_fixtures.rs`.

A separate subdir from `../otlp/` and `../otlp-metrics/` (same rationale as
issue #27's own `otlp-metrics/` subdir): avoids clashing with the
concurrently-developed logs (`../otlp/`, `tests/ingest_fidelity.rs`) and
OTLP metrics (`../otlp-metrics/`, `tests/otlp_metrics_fixtures.rs`) fixture
corpora.

## Provenance

`basic_series.bin` and `metadata.bin` are **real packet captures** — unlike
`../otlp/` and `../otlp-metrics/`, which document "no live collector
reachable from this sandboxed environment", this issue's sandbox *did* have
`podman` and a locally cached `otel/opentelemetry-collector-contrib` image
available, so these two fixtures are genuine wire bytes recorded from a
live collector's `prometheusremotewrite` exporter, not synthetic
constructions through this crate's own hand-rolled prompb structs:

1. A collector was started via `podman run --network host
   otel/opentelemetry-collector-contrib`, configured with an `otlp` HTTP
   receiver and a `prometheusremotewrite` exporter (`send_metadata: true`)
   pointed at a small local HTTP server that dumps each POST body verbatim
   to disk.
2. A hand-built `ExportMetricsServiceRequest` (one `Gauge` `cpu_usage_ratio`
   with a `host` attribute, one monotonic `Sum` `http_requests_total`, and
   one `Gauge` `up` flagged `NoRecordedValueMask` — all under a
   `service.name=checkout` resource) was POSTed to the collector's OTLP
   `/v1/metrics` HTTP receiver.
3. The collector's own `prometheusremotewrite` exporter translated and
   forwarded that batch as two separate `WriteRequest`s — one carrying the
   three samples (`basic_series.bin`), one carrying the three metric
   descriptors as remote-write metadata (`metadata.bin`) — captured exactly
   as sent (`Content-Encoding: snappy`, `X-Prometheus-Remote-Write-
   Version: 0.1.0`, confirming the legacy/RW-1.0 `prompb.WriteRequest`
   wire shape this receiver decodes, not RW-2.0).

This proves the hand-rolled `WriteRequest`/`TimeSeries`/`Label`/`Sample`/
`MetricMetadataProto` tags in `protocols/remote_write.rs` decode a real
collector's wire bytes — a self-consistent wrong tag would decode without
error but silently corrupt every field after it, which a synthetic
round-trip through the same structs cannot catch (architect plan edge case
8). Notably, the collector's own OTel-unit-to-Prometheus-suffix convention
renamed `up` to `up_ratio` (OTel unit `"1"` → a `_ratio` suffix) — left
as-is rather than "corrected", since it is exactly what a real sender
produces.

The remaining cases below have no real-collector equivalent — a standard
sender never omits `__name__`, sends labels a spec-conformant client would
already emit in whatever order, or ships malformed wire data — so they are
built programmatically in `remote_write_fixtures.rs` itself rather than
committed as separate `.bin` files:

- missing/empty `__name__` (semantic per-series drop)
- out-of-order wire labels (re-sort, never a rejection)
- malformed snappy / malformed protobuf-after-valid-snappy (structural
  whole-request failure)

If a future issue (#33's corpus work) needs these as committed fixtures
too, or wants to recapture `basic_series.bin`/`metadata.bin` with a richer
metric mix (histograms, summaries — not exercised here since remote-write
receives them pre-flattened into ordinary series, so a plain gauge/counter
capture already exercises every code path `protocols/remote_write.rs` has),
repeat the steps above; there is no `#[ignore]`-gated `regenerate_fixtures`
test for these two (unlike `../otlp-metrics/`'s synthetic corpus) since
they are not derived from a builder function committed to this repository.

## Fixture index

| File | Covers |
|------|--------|
| `basic_series.bin` | three real `TimeSeries` (`cpu_usage_ratio` gauge with a `host` label, `http_requests_total` counter with a `method` label, `up_ratio` gauge) sharing `job`/`service_name` labels — exact sample rows, independently-recomputed fingerprints, and (via `up_ratio`) a real collector's OTLP-stale-flag-to-remote-write translation, asserted bit-exact via `.to_bits()`. |
| `metadata.bin` | three real `MetricMetadata` records (`cpu_usage_ratio` gauge, `http_requests_total` counter, `up_ratio` gauge) with `help` text — proves the hand-rolled tag layout (`type`=1, `metric_family_name`=2, `help`=4, `unit`=5, gap at 3) against real wire bytes, and pins `metric_type` string parity with `otlp_metrics::parse`'s own type table. |
