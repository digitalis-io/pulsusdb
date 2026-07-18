# OTLP logs fixtures (issue #8)

`ExportLogsServiceRequest` protobuf payloads used by
`crates/pulsus-write/tests/otlp_logs.rs`.

## Provenance

There is no live OpenTelemetry Collector reachable from this repository's
sandboxed CI/dev environment, so these are **not** a packet capture from a
real collector. They are constructed programmatically, directly against
this crate's own `opentelemetry-proto` dependency (the exact wire types a
collector's `LogsServiceClient::export` call sends), then `prost`-encoded
to bytes — i.e. wire-format-identical to what a real collector would send,
built with the same generated message types this crate decodes, rather than
hand-assembled bytes.

`malformed.bin` is the one exception: a real `ExportLogsServiceRequest`
encoding, truncated to half its length, so it is guaranteed to fail to
decode (an unexpected-EOF `DecodeError`) rather than accidentally still
being valid.

## Regenerating

The builder functions and the fixture list live in
`crates/pulsus-write/tests/otlp_logs.rs`, in a `#[ignore]`-gated test
(`regenerate_fixtures`) — not part of the normal test run, since fixture
content is meant to be stable across runs and only change deliberately.
After editing a builder function:

```sh
cargo test -p pulsus-write --test otlp_logs -- --ignored regenerate_fixtures
```

then review and commit the resulting `.bin` diffs alongside the builder
change.

## Fixture index

| File | Covers |
|------|--------|
| `attributes_labels_body_severity_timestamp.bin` | RESOURCE attribute flattening into normalized stream labels (including `service.name` -> `service` column + `service_name` label); the record's `InstrumentationScope` (name/version + a `team` attribute) routed into per-entry STRUCTURED METADATA — `scope_name`/`scope_version`/`team`, NOT stream labels (issue #109, Loki 3.4.2 parity); string body verbatim, severity preserved, `time_unix_nano` preserved exactly. |
| `malformed.bin` | truncated protobuf -> whole-request decode error (maps to HTTP 400 / `google.rpc.Status.code = 3` at the handler). |
| `partial_success.bin` | one record with an unrepresentable timestamp (`time_unix_nano = u64::MAX`) alongside two valid records -> `rejected = 1`, `rejected_message` set, the two valid rows still parsed (OTLP partial success, HTTP 200). |
| `cross_month.bin` | one stream, two records straddling the 2024-01-31/2024-02-01 UTC boundary -> two `StreamRow`s with distinct months, both records present as samples. |
| `backfilled.bin` | one record timestamped 2020-01-01, parsed with a much later `now_ns` -> the `StreamRow`'s month follows the record's own timestamp, not the receive time. |
