# ADR 0004: vendor-and-patch `opentelemetry-proto` for OTLP/JSON non-finite doubles

Status: **Accepted** (2026-07-17)
Issue: [#76](https://github.com/digitalis-io/pulsusdb/issues/76) (M6-13, OTLP/JSON encoding on all OTLP endpoints)
Owner sign-off: [issue #76, 2026-07-17](https://github.com/digitalis-io/pulsusdb/issues/76#issuecomment-5005648608) (Option 1, vendor+patch)
Related: [ADR 0003](0003-promql-parser-vendor-patch.md) establishes the vendor+patch discipline this ADR reuses.

## Context

Issue #76 adds OTLP/JSON as a second *encoding* on the three already-mounted
OTLP endpoints (`POST /v1/logs`, `/v1/metrics`, `/v1/traces`). It is a
decoder-only seam: a `Content-Type: application/json` body is fed through
`serde_json::from_slice` into the *same* `Export*ServiceRequest` types the
protobuf path uses, then the *same* `otlp_*::parse` → the same rows. The serde
impls that make this work already ship in `opentelemetry-proto 0.32.0`'s
`with-serde` feature (enabled since issue #55 for the trace-fetch JSON encoder):
hex trace/span IDs, camelCase, u64-as-string, base64 `bytesValue`, the
`AnyValue` oneof shape.

One gap makes a spec-valid body **fail to decode**, which is silent
metric-data loss. proto3-JSON encodes the non-finite doubles as the strings
`"NaN"` / `"Infinity"` / `"-Infinity"`. `opentelemetry-proto 0.32.0` ships
working helpers for exactly this — `serialize_f64_special` /
`deserialize_f64_special` (`src/proto.rs`) — but wires them to **only** the
`SummaryDataPoint.ValueAtQuantile.{quantile,value}` pair. Every other
double-bearing field is a plain `f64`, so a NaN gauge sample, a `+Infinity`
histogram bound, an `Infinity` exemplar, etc. sent as OTLP/JSON make
`serde_json::from_slice` reject the whole request (→ 400). A second defect:
`Exemplar.value` (the `asDouble`/`asInt` oneof) is missing the
`#[serde(flatten)]` that its sibling `NumberDataPoint.value` has, so an
exemplar's `asDouble` serializes nested (`{"value":{"asDouble":..}}`) instead
of the spec-required flat `{"asDouble":..}` and its NaN emits `null`.

The task-manager adjudication ([issue #76,
2026-07-17](https://github.com/digitalis-io/pulsusdb/issues/76#issuecomment-5004660170))
ruled non-finite doubles a **MUST** ("dropping/rejecting them is silent metric
data loss"), not a documented limitation.

## Decision

**Vendor `opentelemetry-proto 0.32.0` and apply an additive patch**, wired via
the root `[patch.crates-io]` (alongside the ADR-0003 `promql-parser` entry), so
every `opentelemetry_proto::...` import path and the exact `=0.32.0` version pin
are unchanged. The patch:

1. wires the crate's OWN `serialize_f64_special` / `deserialize_f64_special` (and
   two thin `Option<f64>` / `Vec<f64>` adapters delegating to them) to every
   double-bearing field the audit enumerated; and
2. adds the `#[serde(flatten)]` upstream forgot on `Exemplar.value`.

The prost protobuf wire codec is **byte-for-byte untouched** — the change is
purely additive serde annotations plus one `AnyValue` visitor arm. The
exhaustive change list, verified field-by-field, lives in
[`vendor/opentelemetry-proto/PATCHES.md`](../../vendor/opentelemetry-proto/PATCHES.md).

### Why vendor+patch over a local mirror / shadow structs

Considered and rejected: re-declaring the affected metrics tree (~18
structs/enums) as local "shadow" structs with the correct serde and a
conversion layer. That route (a) still has to re-mirror `AnyValue` — which
carries `doubleValue` and is shared by logs and traces — so it does not even
avoid the hard part, while (b) abandoning `with-serde` for the whole metrics
signal and (c) owning a hand-written conversion layer forever. The patch is
~11 field annotations reusing functions the crate already ships, all
wire-identical, and reuses established machinery (`[patch.crates-io]`,
PATCHES.md, this ADR). Smaller and cleaner, even though it adds a second
vendored crate — the owner weighed exactly this and approved Option 1.

### Standing obligation (re-vendor rule)

Every future bump of `opentelemetry-proto` MUST re-apply this patch. If upstream
independently annotates these fields (track the upstream issue in PATCHES.md),
the corresponding patch item is dropped on the bump. There is **no source-tree
hash gate** — that idea was reviewed and rejected (the promql-parser precedent
hashes test *corpus* fixtures, not vendored source; inventing a source-integrity
gate was unfounded). Because `[patch.crates-io]` pins the crate to one version,
drift can only occur on a deliberate re-vendor. The guard against a re-vendor
silently dropping the patch is a **hermetic behavior gate**,
`crates/pulsus-write/tests/otlp_json_vendor_patch.rs`, which asserts each patched
behavior directly (Exemplar flattens; NaN/±Inf emit the exact protojson strings;
every patched double field round-trips a non-finite value). If the patch is lost,
those tests fail loudly.

### Scope / limitations

- **String enum names** (e.g. `"kind":"SPAN_KIND_SERVER"`) — *superseded by the
  Update below.* As originally shipped, enums decoded only in their integer form
  (what real OTLP/JSON emitters send); the string-name form was rejected with a
  named 400. Now accepted (see Update (#98)).
- `asInt` / `asDouble` **integer** oneof arms decode only as JSON numbers
  (not int64-as-string). Pre-existing upstream behavior, unrelated to non-finite
  doubles; out of scope, noted in PATCHES.md.

### Update (#98, 2026-07-18) — string-enum names now accepted

Patch item **P5** (PATCHES.md) lifts the string-enum limitation above: a
`deserialize_with` on the 6 enum-typed fields (across `SpanKind`, `StatusCode`,
`SeverityNumber`, `AggregationTemporality`) accepts **both** the integer form
and the proto3-JSON string name, mapped via prost's generated `from_str_name`.
The patch is **deserialize-only** — serialization stays derived (integer emit),
so the prost wire codec and the protojson RESPONSE emit are byte-identical (the
#55 trace-fetch response and the e2e `normalize_kind` contract are unaffected).
An unknown enum *name* is still a clean, named 400 (`LogsIngestError::DecodeJson`,
never a silent 0); an unknown *integer* is preserved (proto open-enum). Proven
by the `otlp_json_vendor_patch.rs` behavior gate (each of the 6 fields,
name-vs-integer) and the `otlp_json_equivalence.rs` string-enum differential.

## Consequences

- A second permanently-vendored third-party crate under the ADR-0003 discipline
  (owner-accepted maintenance commitment).
- OTLP/JSON on all three signals decodes non-finite doubles losslessly, proven
  hermetically (`otlp_json_equivalence.rs` differential + `otlp_json_vendor_patch.rs`
  behavior gate) — no live-ClickHouse leg is added; row identity is settled at the
  `parse` boundary and `trace_ingest_roundtrip` backstops `parse`→ClickHouse.
- The `#36` conformance assertions that pinned "`application/json` ignored,
  protobuf-under-json → 200" are flipped in the same change (content negotiation
  is now real).
