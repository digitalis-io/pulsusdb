# ADR 0005: native (sparse) histogram storage — a separate co-sharded samples table

Status: **Accepted** (2026-07-18)
Issue: [#113](https://github.com/digitalis-io/pulsusdb/issues/113) (M7-A2, native-histogram storage)
Related: [#112](https://github.com/digitalis-io/pulsusdb/issues/112) (M7-A1, the native-histogram value model + read-path design this storage serves).

## Context

Until M7, OTLP exponential histograms flatten at ingest to classic
`_bucket`/`_sum`/`_count` float series ([docs/schemas.md §2](../schemas.md)).
That is lossy — the sparse exponential structure (per-bucket resolution, the
zero bucket, negative buckets, NHCB custom bounds) collapses to whatever fixed
`le` boundaries the flattening picks — and it inflates series cardinality (one
descriptor becomes many suffixed series). M7 adds first-class Prometheus native
(sparse) histograms end to end; A2 is the **storage** layer under that work.

Two storage shapes were on the table:

1. **Widen `metric_samples`** — add the histogram columns (sparse spans/deltas,
   schema, zero bucket, sum/count) to the existing float sample table, with a
   discriminator picking float-vs-histogram per row.
2. **A separate `metric_hist_samples` table** carrying the histogram wire form,
   sharing `metric_samples`' identity/ordering/partition/TTL/sharding contract.

`metric_samples` is the metrics read path's hottest table and its float fetch
SQL, `EXPLAIN` pruning gate, and migration checksum are all frozen behavior that
must not regress (the standing query-performance mandate). Widening it would add
a dozen mostly-empty array columns to every float row, change the id-5 migration
checksum, and force the float hot path to carry histogram columns it never reads.

## Decision

### (a) Separate `metric_hist_samples` table, not a widened `metric_samples`

Histogram samples get their **own** Metrics-family table
([docs/schemas.md §2.4](../schemas.md)) storing the Prometheus integer sparse
wire form — `schema`, `zero_threshold`, `zero_count`, `count`, `sum`, and the
positive/negative span-offset/length/bucket-delta arrays plus `custom_values`
for NHCB (schema −53). It reuses `metric_samples`' exact identity and access
shape: PK/ordering key `(metric_name, fingerprint, unix_milli)`, daily
partitions, `ttl_only_drop_parts` retention, matching codecs on the shared
scalar columns; the array columns carry `CODEC(ZSTD(1))` (§8's minimum).

`metric_samples` (migration id 5) is **not altered** — the float read path, its
EXPLAIN gate, and its checksum are untouched. The float hot path never pays for
histogram columns, and each table's rows stay narrow for the queries that read
them.

### (b) Co-sharded with floats via the byte-identical Metrics sharding key

`metric_hist_samples` reuses `Family::Metrics`, so its `_dist` wrapper shards on
the **byte-identical** `cityHash64(metric_name, fingerprint)` expression that
`metric_samples`, its tiers, and `metric_series` use ([docs/schemas.md §7](../schemas.md)).
A series' float samples, histogram samples, and `metric_series` metadata
therefore land on the **same shard** — the read path can co-load both sample
types for a resolved fingerprint set shard-locally, with no cross-shard join.
This co-location is the whole reason to keep the histogram table inside the
Metrics family rather than treating it as an independent signal.

### (c) `value_type` discriminator on `metric_series`

> **Removed by issue #623.** `metric_series` became one row per series per UTC
> day with an hour mask, and `value_type` left it: no reader read it. The kind-2
> landing row still carries it. The text below is the record of what was
> decided then.

`metric_series` gains a `value_type UInt8 DEFAULT 0` column (`0 = float`,
`1 = histogram`), added by additive `ALTER` — the §3.1 `structured_metadata`
precedent, never a mutation of the frozen initial `CREATE`, so fresh and
upgraded deployments converge byte-identically and pre-M7 rows read back `0`
with no data migration. This is the per-series routing signal telling the read
path which sample table(s) to touch for a fingerprint.

> **Superseded by A1's plan v5 (#112), 2026-07-18 — the sentence above and the
> paragraph below.** `value_type` is not a routing signal and the read path does
> not consult it. The type-mask-plus-probe mechanism was A1's plans v2 and v3;
> review failed it twice, and plan v5 deleted it and passed with no findings:
> "A1's read path must not consult `value_type` for routing … The column remains
> inert to the read path in M7." The rule in force, and the reason for it, is
> [docs/schemas.md §2.4](../schemas.md)'s writer-contract paragraph. Both
> passages are kept as the record of what was proposed.

The read path uses `value_type` as a **snapshot type-mask plus a bounded
live-tail probe** to stay correct when a series changes or mixes types near the
query edge — the mechanism is designed in [A1 (#112)](https://github.com/digitalis-io/pulsusdb/issues/112);
this ADR governs storage only.

## Consequences

- Native histograms are stored losslessly for both the standard exponential
  schema (−4..8) and NHCB (−53), replacing the lossy classic-bucket flattening
  for native-histogram sources; the classic flattening path remains for
  non-native inputs.
- The float sample read path is provably unchanged (id-5 checksum and EXPLAIN
  gate intact); histogram reads are a new, separate access path.
- Clustered co-location of float + histogram + series metadata per shard is a
  structural guarantee of the shared sharding expression, not a runtime choice.
- Downstream milestones build on this table: the engine value model, OTLP native
  ingest, and histogram PromQL functions/routing (A3/A4/A5) — A2 delivers the
  tables (`metric_hist_samples`, its `_dist` wrapper, and `metric_series.value_type`)
  and nothing else. The milestone names are the ones A2 used; A5's own
  `value_type` routing is the superseded design marked in §(c) above.
