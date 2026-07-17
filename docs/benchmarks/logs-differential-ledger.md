# Logs differential divergence ledger

The M6-09 LogQL-pipeline differential (`e2e/src/logs.rs`,
`logs_pipeline_differential`, nightly/dispatch `e2e-single` tier) gates
every committed pipeline case in `test/fixtures/logs/differential.json`
against both the corpus's by-construction expectation and the pinned
reference log store (`grafana/loki:3.4.2`, digest-pinned in
`deploy/e2e/compose.single.yaml`). **The exclusion list starts empty.**

A case moves from `mode: "gated"` to `mode: "informational"` only via
the established triage discipline (the traces-ledger precedent):

1. an **observed live divergence** (a failed gated run with its dumped
   repro artifact from `target/e2e-artifacts/logs-diff/`),
2. triaged **fix-our-bug vs ratify-documented-oracle-delta** — an entry
   here must state the **exact accepted delta**, never a case-level free
   waiver, and
3. recorded here as an entry whose id the fixture case's `ledger` field
   references (a hermetic unit test in `e2e/src/logs.rs` enforces the
   fixture↔ledger link both ways).

**PulsusDB is always hard-gated against the corpus expectation, even for
informational cases** — only the oracle comparison is ever downgraded.
Entries are append-only; re-gating a case removes its `ledger` reference
but keeps the entry for history.

Out of this ledger's scope by design:

- **The `limit`-oversample under-return boundary** is a PulsusDB-vs-
  requested-limit contract (docs/configuration.md §6,
  `reader.logql_pipeline_scan_factor`), gated hermetically (AC9), not an
  oracle delta — the differential corpus is sized strictly below the
  request limit so it can never trip it.

## Entries

### tumbling-vs-sliding-rate

- **Case:** `metric_rate_tumbling` (issue M6-10 — the range-window
  divergence deliberately left for the metric differential by the M6-09
  ledger).
- **Exact accepted delta:** for RANGE metric queries, PulsusDB evaluates
  fixed, epoch-aligned, non-overlapping tumbling buckets
  (`intDiv(timestamp_ns, step) * step`; `rate` = bucket count / step
  seconds, point stamped at the bucket start), while the oracle
  re-evaluates a sliding `[range]` window at every request-aligned step
  timestamp. Point timestamps therefore differ by alignment (bucket
  start, epoch-aligned vs evaluation instant, request-`start`-aligned)
  and window membership differs at the edges — the two point sets are
  disjoint-by-construction for an unaligned request `start`. This is the
  documented M1 tumbling contract (docs/architecture.md §5.3 /
  `logql::params::QuerySpec::Range`), not a bug; sliding-window parity
  is a scheduled later milestone.
- **Gating:** the oracle comparison is informational for this case ONLY;
  PulsusDB remains hard-gated against the tumbling by-construction
  corpus expectation, and anti-rot applies (if the oracle ever matches
  exactly, the run fails so the case is re-gated). INSTANT metric
  queries have identical window semantics on both stores (`(t - range,
  t]` at one evaluation instant) — every other M6-10 metric case is
  instant-shaped and stays fully gated.
