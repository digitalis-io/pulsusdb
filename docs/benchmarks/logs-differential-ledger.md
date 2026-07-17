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

- **The `limit`-oversample under-return divergence is removed (#90).**
  Filtering pipelines now fetch-until-limit via keyset paging (fill exactly
  to `limit`, docs/configuration.md §6, `reader.logql_pipeline_scan_factor`
  now a first-page-size hint), so there is no under-return boundary to
  ratify. The exactness is gated **hermetically** by the #90 AC1 tests (a
  heavily-dropping pipeline over a corpus sized ABOVE `limit × factor`
  matching lines returns exactly `limit`, asserted by construction — no
  oracle store involved). A nightly-Loki (`grafana/loki:3.4.2`)
  differential case for the same property was found infeasible in the
  current set-equality diff harness and is **deferred to follow-up issue
  #100**; it is NOT part of this ledger's diff today. The
  byte-budget-truncated partial (`data.stats.pulsus_partial`) is a
  PulsusDB-only contract with no Loki equivalent and stays out of oracle
  scope.

## Entries

### tumbling-vs-sliding-rate

- **Case:** `metric_rate_tumbling` (issue M6-10 — the range-window
  divergence deliberately left for the metric differential by the M6-09
  ledger), and the issue #91 RANGE vector-matching cases
  `metric_match_on_range`, `metric_match_ignoring_range`,
  `metric_match_group_left_range`, `metric_match_group_right_range` (the
  per-step instant join over `count_over_time` inherits the SAME
  tumbling-vs-sliding bucket-alignment divergence — the join is applied
  identically per step on both stores, so the only delta is the
  underlying range-window alignment already ratified here).
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

### matching-error-status-divergence (informational note, not a gate downgrade)

- **Cases:** `metric_match_multiple_err`, `metric_match_duplicate_err`
  (issue #91). These queries are runtime vector-matching failures on both
  stores.
- **Probed live against `grafana/loki:3.4.2`:**
  - many-to-one without a grouping modifier → HTTP **500**, body
    `multiple matches for labels: many-to-one matching must be explicit
    (group_left/group_right)` (byte-identical to PulsusDB's message).
  - duplicate one-side signature (many-to-many) → HTTP **500**, body
    `found duplicate series on the right hand-side;many-to-many matching
    not allowed: matching labels must be unique on one side`
    (PulsusDB emits the same string).
- **Exact accepted delta:** Loki returns HTTP **500** for these
  execution-time matching errors; PulsusDB classifies them as a bad
  request (`ReadError::PipelineInvalid` → HTTP **400**), which is the
  semantically correct code for a user-query cardinality error. The two
  stores therefore agree on the error BODY (the gated substring) but not
  the status code. The `metric_match_error` differential cases stay
  **gated on the shared error-body substring** and deliberately do NOT
  gate the HTTP status (per the plan-adjudication probe decision: bodies
  share a substring, so gate on it). This entry records the status-code
  divergence for the record; it is not a `mode: "informational"`
  downgrade (the cases remain gated on their substring).
