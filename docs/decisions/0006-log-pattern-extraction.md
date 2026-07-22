# ADR 0006: Log-pattern extraction (deterministic token-class templating)

Status: **Accepted** (2026-07-22)
Issue: [#171](https://github.com/digitalis-io/pulsusdb/issues/171)

## Context

M7-C3 ships log patterns: a "group these lines by shape" drilldown
(`GET /api/logs/v1/patterns`, [api.md §2.6](../api.md)). The design spike had to
settle three questions: how a pattern is extracted, where the extraction runs,
and how the results are stored so the read stays blazingly fast and the
aggregation stays correct across a distributed, retrying ingest path.

## Decision

**D1. Extraction is deterministic, stateless token-class templating — NOT a
drain-style online clusterer.** [`extract_template`](../../crates/pulsus-write/src/patterns.rs)
is a pure function `body -> template`: the same line always yields the same
template, on every shard/replica and across retries. Rules (frozen by golden
unit tests): examine only the first 1 KiB of the body (a longer body ends the
template in `<_>`); tokens are maximal non-whitespace runs joined by single
spaces (runs collapse — normalized, not round-trip matchable); leading/trailing
wrapper punctuation `(){}[]"',;` stays literal; a `key=value`/`key:value` core
keeps `key` + separator literal and classifies only the value; a fragment
becomes `<_>` iff it contains an ASCII digit or exceeds 64 bytes; caps of 64
tokens / 512 template bytes with truncation at a token boundary plus a trailing
`<_>`.

Rejected: a drain prefix-tree clusterer. Its per-stream mutable state makes the
template identity depend on arrival order / node / restart, so two shards emit
*different* templates for identical lines — which breaks mergeable aggregation
and idempotent re-inserts, exactly where the ingest-throughput mandate forbids
per-line surprises. The trade-off accepted is coarser clustering (a digit-free
variable word stays literal); because identity is the stored template string, a
read-time secondary merge can refine it later without touching stored rows.

**D2. Storage is a fourth Logs-family table, batch-pre-aggregated at ingest —
NOT a materialized view and NOT per-line rows.** An MV is impossible
(extraction is Rust, not SQL); per-line rows would double `log_samples` write
volume. `LogWriter::admit_batch` aggregates each request batch into
`(fingerprint, bucket_ns, template) -> count` before append, so row volume is
~ distinct templates per stream per 10s bucket per batch. The
[`log_patterns`](../schemas.md) table is an `AggregatingMergeTree` with `count
SimpleAggregateFunction(sum, UInt64)`, **`ORDER BY (fingerprint, bucket_ns,
pattern)`** (bucket_ns before pattern, so a bounded time range prunes at the PK
level inside each fingerprint's key range — proven by a live `EXPLAIN
indexes=1` gate), daily partitions, and the same delete-TTL as `log_samples`.
The 10s ingest bucket is a code constant (`PATTERN_BUCKET_NS`), so the table is
checksum-gated like every other structural table. Its `_dist` wrapper co-shards
on `fingerprint` with the rest of the logs family.

**D3. Exactly-once framing.** The writer never auto-replays a block that could
have committed: a post-send retryable failure is downgraded to `InsertUncertain`
and spooled audit-only, never re-inserted. So `sum` inflation is impossible
within the writer's own machinery. The only inflation vector is a client-level
re-send after a 5xx/timeout ack — the identical event class that already
inflates the `log_metrics` rollup. Pattern counts are therefore **exact on the
clean path and best-effort-approximate under ingest-failure re-sends**, at
`log_metrics` parity (proven by a live fidelity test that re-admits a batch and
cross-checks both tables inflating by the same factor). Patterns are excluded
from the sync durability ack (a `log_patterns` flush failure never 500s an
ingest whose log lines landed).

**D4. Memory bound — a fixed per-request ceiling.** The per-batch aggregation
map is charged into the reserve-before-materialize gate as
`reserve = Σ template_bound(row) + AGG_BASE_OVERHEAD (1024 B)
+ min(distinct, MAX_DISTINCT_PATTERNS_PER_BATCH) × PATTERN_ROW_OVERHEAD (256 B)`.
A hard cap of `MAX_DISTINCT_PATTERNS_PER_BATCH = 10_000` makes the aggregation
buffer a **fixed ceiling (≈ 2.44 MiB/request)**, independent of any hashbrown
modeling: at the cap, a row whose template is not already present is dropped
from pattern accounting only (the log line is untouched), counted in
`patterns_dropped_total`, in deterministic parse order — an under-count folded
into D3's approximate semantics. A dealloc-aware live-peak allocator gate asserts
the measured aggregation peak never exceeds the charged reservation.

**D5. Read shape.** Stage-1 fingerprint resolution (selector only — line filters
are rejected, the bodies are gone), then ONE pushed-down aggregate over
`log_patterns` with no hydration: `fingerprint IN` engages the PK prefix, daily
partitions prune the window, and the aggregation + top-1000 + `step`-rebucketing
all execute in ClickHouse. `step` is floored to the 10s bucket; the
`(end-start)/step` grid is capped at 11,000. The response is the Loki-interop
envelope, ordered total-count desc then pattern asc (a PulsusDB determinism pin).

**Kill-switch.** `PULSUS_LOG_PATTERNS` (default `true`): disabled ⇒ zero
extraction and zero `log_patterns` appends; the read endpoint stays mounted and
serves empty data.

## Consequences

- Counts are mergeable and idempotent under the writer's own retries;
  client-level re-sends are documented best-effort-approximate at `log_metrics`
  parity, not silent corruption.
- Clustering is coarser than a drain clusterer, deliberately, in exchange for
  distributed-correct identity that a read-time merge can later refine.
- The write path pays a bounded, reservation-gated ≈ 2.44 MiB aggregation
  ceiling per in-flight request; a pathological >10k-distinct-template request
  is a bounded under-count, never an OOM.
