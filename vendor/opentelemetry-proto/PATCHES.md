# Patches applied to `opentelemetry-proto 0.32.0`

This is a patched, vendored copy of [`opentelemetry-proto`
0.32.0](https://github.com/open-telemetry/opentelemetry-rust/tree/main/opentelemetry-proto),
wired into the workspace via `[patch.crates-io]` (root `Cargo.toml`) so every
`opentelemetry_proto::...` import path and the exact `=0.32.0` version pin are
unchanged. See
[`docs/decisions/0004-opentelemetry-proto-vendor-patch.md`](../../docs/decisions/0004-opentelemetry-proto-vendor-patch.md)
for the decision this vendored copy implements.

## Upstream base (pinned)

- **Crate:** `opentelemetry-proto`
- **Version:** `0.32.0`
- **crates.io release SHA-256:** `56d658ba1faf63f7b9c492cfbe6e0ec365440a16132d3270c1065f7b33f1b638`
  (the `opentelemetry-proto-0.32.0.crate` tarball)
- **Upstream VCS commit:** `ec289cb3c6f8260951699c51df968560943c1451`
  (`.cargo_vcs_info.json`)

Only the files needed to build the `gen-tonic-messages` + `logs`/`metrics`/
`trace` + `with-serde` feature set are vendored (`src/lib.rs`, `src/proto.rs`,
`src/proto/tonic/*.rs`, `src/transform/*.rs`, `Cargo.toml`, `LICENSE`,
`README.md`, `CHANGELOG.md`). The upstream `src/proto/opentelemetry-proto/`
`.proto` source tree and `tests/` are omitted: there is no `build.rs`/codegen
step (the `.rs` files are pre-generated and checked in upstream), so nothing
references them at build time. `Cargo.toml` is the upstream *normalized*
manifest with concrete dependency versions, plus an empty `[workspace]` table
and a vendoring note (so Cargo does not treat this nested manifest as an
orphaned member of the root workspace); dependency versions are copied verbatim
from the published release.

## Re-vendor rule

On any `opentelemetry-proto` version bump, re-apply every patch below. If
upstream has independently landed the fix (see "Upstream status"), DROP the
corresponding item instead of re-applying it. There is **no source-hash gate**
(rejected in review — the promql-parser precedent hashes test-corpus fixtures,
not vendored source). The guard that a re-vendor preserves the patch is the
hermetic **behavior gate** `crates/pulsus-write/tests/otlp_json_vendor_patch.rs`
(and the `otlp_json_equivalence.rs` differential): both fail loudly if the patch
is lost. Run `cargo test -p pulsus-write` after any re-vendor.

The patch is **additive and wire-neutral**: it only adds `#[serde(...)]`
annotations and serde adapter functions. The prost protobuf codec (field tags,
wire types, message shapes) is byte-for-byte identical to upstream, so the
existing protobuf ingest path is unaffected.

## Why

`opentelemetry-proto 0.32.0` ships working `serialize_f64_special` /
`deserialize_f64_special` (`src/proto.rs`) that map non-finite `f64` to/from the
proto3-JSON strings `"NaN"` / `"Infinity"` / `"-Infinity"`, but wires them to
ONLY `SummaryDataPoint.ValueAtQuantile.{quantile,value}`. Every other
double-bearing field is a plain `f64`, so a spec-valid OTLP/JSON body carrying a
non-finite double on any other field fails `serde_json::from_slice` (→ 400,
silent metric-data loss). Separately, `Exemplar.value` is missing the
`#[serde(flatten)]` its sibling `NumberDataPoint.value` has, so its `asDouble`
serializes nested instead of flat and its NaN emits `null`.

## Patch items

### P1. `serializers` module additions — `src/proto.rs`

New, additive helpers in the `crate::proto::serializers` module (all delegating
to the crate's existing scalar `serialize_f64_special`/`deserialize_f64_special`):

- `pub struct F64Special(pub f64)` with `Serialize`/`Deserialize` impls — a
  transparent `f64` newtype routing through the special-double logic. Used where
  a field-level `serialize_with`/`deserialize_with` cannot reach (the visitor-
  driven `AnyValue` path, and inside the `Option`/`Vec` adapters).
- `pub mod f64_special_opt` — `Option<f64>` field adapter (`serialize`/
  `deserialize`). `None` ↔ `null`; `Some(v)` routes through the scalar helper.
- `pub mod f64_special_vec` — `Vec<f64>` field adapter, per-element special.

### P2. `AnyValue.doubleValue` special-double — `src/proto.rs`

In the hand-written `serialize_to_value` / `deserialize_from_value` visitors
(the `AnyValue.value` oneof serde path):

- **serialize:** added a `Some(Value::DoubleValue(d))` arm emitting
  `{"doubleValue": F64Special(*d)}` (upstream fell through to the derived
  serialize, which emits `null` for NaN).
- **deserialize:** the `"doubleValue"` arm now reads `F64Special` (upstream read
  a bare `f64`, rejecting `"NaN"`/`"Infinity"`/`"-Infinity"`).

This ONE fix covers **logs and traces**: their only doubles arrive via
`AnyValue` attributes / log body; `logs.v1` and `trace.v1` carry zero bare
double fields (grep-verified).

### P3. `metrics.v1` double fields — `src/proto/tonic/opentelemetry.proto.metrics.v1.rs`

Added `#[cfg_attr(feature = "with-serde", serde(serialize_with=..., deserialize_with=...))]`
(scalar `serialize_f64_special`, or the `f64_special_opt` / `f64_special_vec`
adapters for `Option<f64>` / `Vec<f64>`) to every remaining double-bearing field:

| # | Field | Rust type | Adapter |
|---|-------|-----------|---------|
| 1 | `number_data_point::Value::AsDouble` | `f64` (oneof variant) | scalar |
| 2 | `HistogramDataPoint.sum` | `Option<f64>` | `f64_special_opt` |
| 3 | `HistogramDataPoint.explicit_bounds` | `Vec<f64>` | `f64_special_vec` |
| 4 | `HistogramDataPoint.min` | `Option<f64>` | `f64_special_opt` |
| 5 | `HistogramDataPoint.max` | `Option<f64>` | `f64_special_opt` |
| 6 | `ExponentialHistogramDataPoint.sum` | `Option<f64>` | `f64_special_opt` |
| 7 | `ExponentialHistogramDataPoint.min` | `Option<f64>` | `f64_special_opt` |
| 8 | `ExponentialHistogramDataPoint.max` | `Option<f64>` | `f64_special_opt` |
| 9 | `ExponentialHistogramDataPoint.zero_threshold` | `f64` | scalar |
| 10 | `SummaryDataPoint.sum` | `f64` | scalar |
| 11 | `exemplar::Value::AsDouble` | `f64` (oneof variant) | scalar |

Already wired upstream (unchanged): `SummaryDataPoint.ValueAtQuantile.{quantile,value}`.

### P4. `Exemplar.value` gains `#[serde(flatten)]` — `src/proto/tonic/opentelemetry.proto.metrics.v1.rs`

Added `#[cfg_attr(feature = "with-serde", serde(flatten))]` on the
`Exemplar.value` oneof field, matching its sibling `NumberDataPoint.value`
(`metrics.v1.rs`, ~line 380, which already has it). Without this the exemplar's
`asDouble` serializes nested and the spec-required flat form fails to
round-trip. Flatten audit of every oneof holding a double (complete):
`number_data_point::Value` (has flatten), `exemplar::Value` (this fix),
`metric::Data` (holds messages, no bare double → n/a),
`any_value::Value` (has flatten + the visitor path from P2). No other oneof
holds a double.

### P5. proto3-JSON enum string-NAME acceptance — `src/proto.rs` + the three signal `.rs` files (#98)

proto3-JSON permits an enum field as EITHER its integer value OR its string
name. Upstream models every `#[prost(enumeration = ...)]` field as a bare
`i32`, so the derived `Deserialize` accepts ONLY the integer form; the
string-name form (`"kind":"SPAN_KIND_SERVER"`) is rejected. This item adds a
`deserialize_with` that accepts BOTH forms, mapping the name via prost's
generated `from_str_name`.

- **`src/proto.rs`** (`mod serializers`): `deserialize_enum_int_or_name` (a
  `deserialize_any` engine — `visit_i64`/`visit_u64` for the integer form,
  `visit_str` for the name) plus four thin per-enum wrapper modules
  (`enum_span_kind`, `enum_status_code`, `enum_severity_number`,
  `enum_aggregation_temporality`), each coercing that enum's `from_str_name`.
- Wired (deserialize-only) to the **6** enum-typed fields across **4** enums:

  | Field | Enum | File |
  |-------|------|------|
  | `Span.kind` | `span::SpanKind` | `opentelemetry.proto.trace.v1.rs` |
  | `Status.code` | `status::StatusCode` | `opentelemetry.proto.trace.v1.rs` |
  | `LogRecord.severity_number` | `SeverityNumber` | `opentelemetry.proto.logs.v1.rs` |
  | `Sum.aggregation_temporality` | `AggregationTemporality` | `opentelemetry.proto.metrics.v1.rs` |
  | `Histogram.aggregation_temporality` | `AggregationTemporality` | `opentelemetry.proto.metrics.v1.rs` |
  | `ExponentialHistogram.aggregation_temporality` | `AggregationTemporality` | `opentelemetry.proto.metrics.v1.rs` |

  The `flags` fields (`SpanFlags`/`LogRecordFlags`/`DataPointFlags`) are plain
  `fixed32`/`uint32` scalars, NOT enum-typed — excluded by design.
- **Deserialize-only** (no `serialize_with`): serialization stays derived
  (integer emit), so the prost wire codec and PulsusDB's protojson RESPONSE
  emit are byte-for-byte identical. Open-enum semantics: an unknown INTEGER is
  preserved (matching the pre-patch bare-i32 path); an unknown NAME is a hard
  decode error (a clean, named 400 — never a silent 0). Covered by the
  behavior gate (each of the 6 fields, name-vs-integer) and the
  `otlp_json_equivalence.rs` differential (string-enum JSON per signal).

## Out of scope (pre-existing upstream behavior, NOT introduced here)

- `asInt`/`asDouble` **integer** oneof arms decode only as JSON numbers (not
  int64-as-string). Unrelated to non-finite doubles.

## Upstream status

The non-finite-double wiring and the `Exemplar.value` flatten are candidate
upstream fixes; PR links to be recorded here once filed. Until then, this
vendored patch is the source of truth. Drop the corresponding item on any bump
where upstream has landed the fix (verify via the behavior gate).
