# Loki push fixtures (issue #77)

A snappy-compressed `logproto.PushRequest` protobuf payload used by
`crates/pulsus-write/tests/loki_push_fixtures.rs` — proving the hand-rolled
`logproto` tags in `protocols/loki_push.rs` decode real wire bytes, not just
self-consistent synthetic round-trips through the same structs (a wrong tag
decodes without error but silently corrupts every following field).

A separate subdir from `../remote-write/`, `../otlp/`, and `../otlp-metrics/`
(same rationale as those): keeps the concurrently-developed fixture corpora
from clashing.

## Provenance

`promtail_push.bin` is a **real packet capture** from a live
`grafana/promtail:3.4.2` agent — the version-matched, Loki-native producer
for the digest-pinned `grafana/loki:3.4.2` oracle
(`docs/benchmarks/logs-differential-ledger.md:7`). It is genuine wire bytes
recorded from a real emitter, not a synthetic construction through this
crate's own hand-rolled `logproto` structs.

Task-manager Q3 adjudication (issue #77) rules that a live grafana/loki
cross-send is the *wrong* oracle (Loki is a receiver, not a producer); the
correct oracle is a real **producer** body. The OpenTelemetry Collector's
`loki` exporter — the plan's first-choice producer — has been **removed
upstream** (`otel/opentelemetry-collector-contrib:latest` no longer lists a
`loki` exporter type), so `promtail` was used instead: a real Loki agent
that emits exactly the push wire format a deployed Loki datasource sends.

Capture method:

1. A one-line HTTP dump server was started on `:9999`, saving any POST body
   verbatim to disk and returning `204`.
2. `promtail:3.4.2` was run via
   `podman run --network host grafana/promtail:3.4.2`, configured with a
   `static_configs` target carrying `service_name=checkout`, `env=prod`
   labels tailing a small log file, and a single `clients` URL pointed at
   the dump server's `/loki/api/v1/push`.
3. Two log lines (`hello from real producer`, `second line from promtail`)
   were written to the tailed file; promtail scraped them and pushed the
   batch. The captured body's headers were `Content-Type:
   application/x-protobuf` with **no** `Content-Encoding` — confirming the
   implicit-snappy protobuf wire shape this receiver decodes (a Loki agent
   snappy-compresses the body without a `Content-Encoding: snappy` header,
   exactly like Prometheus remote-write).

The captured body was additionally cross-sent to the digest-pinned
`grafana/loki:3.4.2` oracle's own `/loki/api/v1/push` and confirmed to
return `204` — proving the bytes are a wire-valid push Loki itself accepts,
not merely something our decoder happens to read.

The agent promotes its `static_config` labels plus a `filename` label, so
the decoded stream is
`{env="prod", filename="/logdir/app.log", service_name="checkout"}`. The
per-entry timestamps are promtail's ingestion time (capture-run-dependent),
so the golden test asserts them positive rather than pinning an exact value;
labels, line bodies, counts, and the independently-recomputed
`stream_fingerprint` are pinned.

The remaining cases (dual-encoding equivalence, cross-transport fingerprint
identity with OTLP logs, malformed label strings / timestamps, structural
DoS bounds) have no real-producer equivalent — a standard agent never ships
malformed wire data — so they are built programmatically in
`loki_push_fixtures.rs` and `protocols/loki_push.rs`'s own test module rather
than committed as separate `.bin` files.

## Oracle-pinned response codes (`grafana/loki:3.4.2`)

Probed directly against the digest-pinned oracle's `/loki/api/v1/push`:

| Request | Loki status | Body |
|---------|-------------|------|
| valid JSON (`application/json`) | **204** | empty |
| valid snappy-protobuf (`application/x-protobuf`, the captured body) | **204** | empty |
| malformed JSON under `application/json` | **400** | `text/plain` (`loghttp.PushRequest...`) |
| JSON body under `application/x-protobuf` | **400** | `text/plain` (`snappy: corrupt input`) |
| unsupported `Content-Type` (`text/plain`, non-protobuf body) | **400** | `text/plain` (`snappy: corrupt input` — defaults to protobuf) |
| absent `Content-Type` (non-snappy body) | **400** | defaults to protobuf |
| oversize (a 6 MiB line, over Loki's `max_line_size`) | **400** | `text/plain` (NOT 413) |
| `GET` (wrong method) | **404** | — (Loki registers push as POST-only) |

Findings pinned into the receiver's contract:

- Success is **204 empty** for both encodings; errors are **400 plain-text**.
  PulsusDB matches both byte-shapes (the shared remote-write empty-`204`/
  plain-text response family).
- Content-Type negotiation: `application/json` → JSON path; anything else or
  absent → the snappy-protobuf path. Identical to PulsusDB's rule.
- Oversize is **400**, not 413 — PulsusDB's 64 MiB decompressed cap also
  maps to 400 (`OversizeBody`); the cap *size* differs from Loki's
  per-line/per-stream limits (a deliberate, documented divergence), but the
  status class matches.
- PulsusDB-contract-only codes with no Loki equivalent: **202** (async,
  `X-Pulsus-Async: 1`) and **429** (sink backpressure).
- Wrong-method: PulsusDB returns **405** with an `Allow` header (axum's
  method router), a documented divergence from Loki's 404.

## Fixture index

| File | Covers |
|------|--------|
| `promtail_push.bin` | one real `StreamAdapter` (`{env="prod", filename="/logdir/app.log", service_name="checkout"}`) with two `EntryAdapter` log lines — pins the hand-rolled tag layout (`PushRequest.streams`=1, `StreamAdapter.labels`=1/`entries`=2, `EntryAdapter.timestamp`=1/`line`=2, `Timestamp.seconds`=1/`nanos`=2) against real promtail wire bytes, and the independently-recomputed `stream_fingerprint`. |
