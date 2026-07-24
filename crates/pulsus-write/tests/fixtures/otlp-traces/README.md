# OTLP traces fixtures (issue #54)

`ExportTraceServiceRequest` protobuf payloads used by
`crates/pulsus-write/tests/trace_ingest_fidelity.rs` (hermetic parser
golden) and `crates/pulsus-write/tests/trace_ingest_roundtrip.rs`
(env-gated ClickHouse round-trip).

A separate subdir from `../otlp/` and `../otlp-metrics/`, following the
per-signal fixture-corpus convention those directories established.

## Provenance

Same construction method as `../README.md`'s OTLP logs fixtures and
`../otlp-metrics/README.md`'s metrics fixtures: there is no live
OpenTelemetry Collector reachable from this repository's sandboxed CI/dev
environment, so these are **not** a packet capture from a real collector.
They are constructed programmatically, directly against this crate's own
`opentelemetry-proto` dependency (the exact wire types a collector's
`TraceServiceClient::export` call sends), then `prost`-encoded to bytes —
wire-format-identical to what a real collector would send.

## Regenerating

The builder functions live in
`crates/pulsus-write/tests/trace_ingest_fidelity.rs`, in an
`#[ignore]`-gated test (`regenerate_fixtures`). After editing a builder:

```sh
cargo test -p pulsus-write --test trace_ingest_fidelity -- --ignored regenerate_fixtures
```

then review and commit the resulting `.bin` diffs alongside the builder
change (`committed_fixture_matches_the_builder` fails until you do).

## Fixture index

| File | Covers |
|------|--------|
| `two_spans_dual_scope.bin` | one resource (`service.name` = `checkout`, `deployment.environment` = `prod`) → one instrumentation scope (`checkout-instrumentation` v1.2.3, with a `scope.only.attr` attribute indexed under `scope='instrumentation'`, issue #192) → two spans. Span A (root, Server, unset status) carries `http.status_code` = Int 500, `http.method` = "GET", and `deployment.environment` = "prod" — the same verbatim key/value as the resource attribute, at span scope (the dual-scope same-key golden) — plus one span **event** (`exception` at +3 ms with an `exception.type` = "IOError" attribute, issue #192 PR-B). Span B (child of A, Client, Error status) carries no span attributes. Exercises: verbatim keys, scope discriminators (incl. `event`/`event:intrinsic`), `val_num`, `timeSinceStart`, day-floored `date`, parent-id sentinel vs real parent, status/kind/duration mapping, and the self-contained single-`ResourceSpans` `TracesData` payload contract. |
