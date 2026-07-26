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
  differential case for the same property now exists (issue #100,
  `fetch_until_limit_paged`, `kind: "streams_limited"`): the shared
  set-equality harness could not express "exactly `limit` entries with
  ordered truncation," so #100 added a per-case `limit` + an **ordered
  earliest-`limit` `Vec<(labels, ts, line)>`** comparison (each store's
  per-stream `values` are asserted ascending as received — the forward
  contract — then k-way merged, so a response-order regression fails
  rather than being sorted away) and a heavily-dropping pipeline
  (`| json | status = "503" | took_ms = "500"`) whose earliest-`limit=4`
  survivors span >= 2 keyset pages at the full tier — with j9 & j69 both
  `GET /api/users 503 500` sharing one stream, giving a real intra-stream
  order to verify (`raw == limit` is the page-2 proof). It is **`gated`**
  — parity holds
  against the oracle, so it needs no informational entry and no ledger id
  — and rides the existing nightly `e2e-metrics-full` lane. The
  byte-budget-truncated partial (`data.stats.pulsus_partial`) is a
  PulsusDB-only contract with no Loki equivalent and stays out of oracle
  scope.

- **`__error_details__` off-corpus detail classes (issue #99,
  informational).** The streams-path `__error_details__` companion to
  `__error__` is matched **byte-exact** against `grafana/loki:3.4.2` for
  the differential corpus and the hermetic goldens: the representative
  `JSONParserErr` message (a top-level non-object line), the
  unterminated-quote-at-EOF `LogfmtParserErr` position message, and the
  `LabelFilterErr` number/duration families (Go `strconv.ParseFloat` and
  `time.ParseDuration`'s `invalid duration` / `missing unit` branches).
  The offending value is interpolated through the SAME Go-stdlib quoter
  Loki's error carries — `strconv.Quote` for the number/bytes families,
  Go `time`'s internal `quote` for the duration family — so the rendered
  value is **byte-exact for ALL label values** (embedded quotes, control
  bytes, and multi-byte UTF-8 included), not merely plain ASCII. What
  remains deliberately faithful-format (not byte-exact) is the CLASS
  selection / component extraction for a handful of off-corpus inputs —
  reproducing each Go library's internal state there is disproportionate
  for a diagnostic label clients rarely filter on byte-exact (unlike
  `__error__`, which IS byte-exact). The ledgered off-corpus classes:
  - `JSONParserErr` on a **partial** object/array (`{"a":1`): buger/
    jsonparser emits an internal-scanner-state message and Loki partially
    extracts; our engine reports the one representative message and does
    not partially extract.
  - `LogfmtParserErr` classes **other than** the unterminated quote
    (`unexpected '='`, invalid key): since M8-LQ1 (#200) these ARE error
    sites under `| logfmt --strict` and set the correct `__error__`
    LABEL. Only the `__error_details__` position STRING for these two
    classes stays faithful-format (same structure, ledgered position) —
    the #99 detail-only precedent, LABEL always correct. The default
    (non-strict) `| logfmt` is now reference-lenient (best-effort, never
    sets `__error__`), resolving the former #72 default-trigger delta.
  - `LabelFilterErr` **bytes** family (`humanize.ParseBytes` interpolates
    an internal numeric split) and the duration **`unknown unit`** branch
    (Go consumes valid leading components first for compound values, so
    the identified unit *component* may differ) — the interpolated value
    and unit are nonetheless `time.quote`-rendered byte-exactly.

  These classes are NOT exercised by the differential (the committed
  error cases use the byte-exact corpus); the probe transcript
  (`crates/pulsus-read/tests/golden/logql_error_details/oracle_probe.txt`)
  records the exact Loki strings for each. This is a documented fidelity
  note, not a gated-case downgrade — every committed `__error_details__`
  differential case stays hard-gated.

- **The Loki-push structured-metadata (SM) differential is a separate
  lane (issue #102).** OTLP carries no per-entry structured metadata on the
  collector path, so the SM surfacing/collision behavior #97 shipped is
  proven by a NEW scenario (`logs_structured_metadata_differential`,
  `e2e/src/logs_sm_corpus.rs`) that dual-pushes identical native Loki JSON
  `[ts, line, {sm}]` bodies to both stores' `/loki/api/v1/push` and compares
  the SM-derived response label sets (surfacing, a `| key="value"` filter on a
  non-colliding SM key, and a `| key_extracted="value"` filter on a collided
  key). It has its OWN `run_id`, `sm_differential.json` fixture, and
  completeness gate — the OTLP `differential.json` / `CASE_IDS` id-lock above
  is untouched. Every SM case is **`gated`** (SM behavior is byte-exact vs
  `grafana/loki:3.4.2` under this file's `allow_structured_metadata: true` /
  `discover_log_levels: false` — no informational entry, no ledger id) and
  rides the existing nightly `e2e-metrics-full` lane. **Stream-fingerprint
  invariance stays hermetic-only:** a `query_range` response is label-driven
  and SM fans into response labels, so an SM entry and a non-SM entry on the
  same base labels return under different response label sets on both stores —
  the physical stream identity is not black-box-observable, and Loki exposes
  no comparable fingerprint. #97 pins that storage semantics hermetically
  (`protocols/loki_push.rs`, `writer/rows.rs`); this lane does not fabricate a
  cross-store fingerprint assertion. There is **no SM predicate pushdown** —
  SM label filters are evaluated client-side (the #97 baseline), so this lane
  adds no read-path SQL and cannot regress the Tier-1 SQL/alloc goldens.
  **Cross-store duplicate-on-retry semantics are a permanent carve-out
  (issue #102 un-defer).** `grafana/loki:3.4.2`'s native `/loki/api/v1/push`
  has no idempotency-key or request-dedup contract, so whether a live
  double-push renders as one entry or two on the oracle side is an
  undocumented implementation detail, not a comparable behavior — and
  deliberately double-pushing against the shared nightly stores would poison
  this lane's own `raw == distinct` duplicate-delivery validity gate
  (`wait_for_sm_completeness`/`run_sm_case`, `e2e/src/logs.rs:2051-2070`).
  That gate is the permanent live backstop: any retry-path regression that
  actually replays a body against a real store surfaces there as a loud
  `bail!`, never a silent pass. The producer's own no-replay invariant is
  instead pinned hermetically, per-PR, against a loopback fake store that
  deterministically reproduces "server ingested the body, then the transport
  died before the response" — a fault the live stores cannot be coerced into
  reproducing deterministically — by
  `sm_push_lane_cannot_replay_the_corpus_on_an_ambiguous_post_ingest_failure`
  (`e2e/src/logs.rs`), which drives the real `push_sm_corpus` producer
  through `poll_until` and `push_loki_json` end to end.

- **`__error_details__` on the METRIC pipeline-error message (issue #99
  OQ2 → RESOLVED, issue #104).** The `grafana/loki:3.4.2` probe found
  that Loki DOES include `__error_details__` in its metric `pipeline
  error: '…' for series: '{…}'` message — contradicting the #91 deferral
  premise. #99 stayed streams-only and escalated; issue #104 brought the
  metric path to parity by reusing #99's machinery verbatim (the same
  `label_filter_error_details` / `logfmt_error_details` / `JSON_ERROR_DETAILS`
  formatters, now recorded on both paths). Parity is byte-exact for the
  classes reachable on the metric path: `JSONParserErr` (the pinned
  buger/jsonparser message), the `SampleExtractionErr` unwrap-conversion
  failure (`unwrap duration/number/bytes` share the label-filter
  conversion — Number → Go `strconv.ParseFloat`, Duration → Go
  `time.ParseDuration`'s `invalid duration` / `missing unit` branches,
  Bytes → the empty-prefix `ParseFloat` quirk — oracle-confirmed live
  byte-exact against `grafana/loki:3.4.2`, so NO dedicated
  `sample_extraction_error_details` fallback is needed), and the
  `LabelFilterErr` families already ledgered above. The off-corpus
  faithful-format classes enumerated in the `__error_details__` note
  above (Bytes internal split, duration `unknown unit`, partial-object
  JSON) apply identically on the metric path and are unchanged. The ONLY
  hermetic golden that gates the metric detail BYTES is
  `crates/pulsus-read/tests/logql_metric_agg_golden.rs` — the server
  error-mapping goldens (`logs_api/error.rs`, `prom_api/error.rs`)
  construct SYNTHETIC `MetricPipelineError`s and assert status + message
  PREFIX only, so they do not gate detail bytes and are untouched. The
  live cross-check is the existing nightly `metric_unwrap_error`
  differential (unchanged: it asserts both stores carry the
  `SampleExtractionErr` class, a substring the flip does not remove).

## Entries

### tumbling-vs-sliding-rate — RESOLVED (issue #227)

- **Status:** RESOLVED. The former tumbling divergence is fixed — RANGE
  metric queries now evaluate Loki's **sliding** `[range]` windows
  bit-exactly (issue #227). The window `(t-range, t]` is re-evaluated at
  every start-anchored grid point `{start + k·step ≤ end}`, streamed off
  raw `log_samples` (the 5s rollup fast-path is retired for range reads —
  it cannot reproduce Loki's per-event boundary). `rate({}[1m])` and
  `rate({}[10m])` now differ, and point timestamps + window membership
  match Loki. Reducer classes A (invert-integer) / B (re-reduce
  order-independent) / C (canonical-fold) mirror Loki's
  `batchRangeVectorIterator`.
- **One ratified residual divergence:** same-nanosecond, **same-stream**
  samples are ordered deterministically by full body bytes (a group-local
  `tie_rank`) rather than Loki's unstored chunk-insertion order (PulsusDB
  stores no ingestion ordinal). Cross-stream same-ts ties ARE Loki-exact
  via `StableHash` (`xxhash64`, seed 0, sorted `name·0xFF·value·0xFF`).
  The residual is measure-rare (two samples at the identical nanosecond in
  one stream), byte-stable across runs, and bounded by a
  `MAX_TS_COLLISION_GROUP` clean 422 — it affects only class-C float
  low-bits and `first`/`last` value at an identical-ns same-stream
  boundary.
- **Gating:** the live `schema-it` differential now asserts the raw
  sliding path is bit-exact to `grafana/loki:3.7.4` on an off-5s-boundary
  grid (env-gated), plus the hermetic `logqltest` `eval range` corpus.
  INSTANT metric queries were already identical (`(t - range, t]` at one
  evaluation instant) and are unchanged.
- **e2e cases re-gated (issue #227).** The five cases this entry covered
  are no longer informational — the divergence they recorded is gone, so
  `test/fixtures/logs/differential.json` carries them at `mode: "gated"`
  and the e2e exclusion list (`e2e/src/logs.rs`
  `INFORMATIONAL_CASE_IDS`) is EMPTY again: `metric_rate_tumbling`,
  **renamed `metric_rate_sliding`** (the id named the retired semantics),
  and the four issue-#91 range vector-matching cases
  `metric_match_on_range`, `metric_match_ignoring_range`,
  `metric_match_group_left_range`, `metric_match_group_right_range`.
  The e2e by-construction expectation (`expected_metric_matrix`) now
  computes the sliding contract itself — start-anchored grid, half-open
  `(t - range, t]`, gaps for empty windows, `rate` over the `[range]` —
  and a hermetic test evaluates every committed range case through the
  shipped sliding evaluator and requires the two to agree.

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

### approx-topk-determinism-and-range-status (issue #221)

- **Construct:** `approx_topk(k, ...)` — probabilistic top-k via a
  byte-exact count-min-sketch port (corpus `b10_approx_topk.test`;
  collision fixtures verified by executing the reference
  `pkg/logql`/`pkg/logql/sketch` package at v3.7.4).
- **Reference nondeterminism above the retention cap (exclusion rule,
  not a divergence):** above 10 000 distinct inner series — and at ties
  on the k-th selection boundary — the reference's own result depends on
  a randomized Go map iteration (heap eviction order + topk tie order).
  Probed live against `grafana/loki:3.7.4`: three identical
  `approx_topk(15, sum by (id)(count_over_time(...)))` queries over one
  immutable 20 000-series dataset returned three different tails (the
  true heavy hitters were stable). **No one can match that regime,
  including the reference itself.** PulsusDB pins a deterministic
  canonical insertion order (label-ascending) instead — the same
  treatment as the ratified instant `first_over_time`/`last_over_time`
  tie pin — and the corpus pre-commits to staying below the cap and off
  k-boundary ties (rule recorded in the `.test` header).
- **Range-rejection status delta (the matching-error-status-divergence
  precedent, third instance):** any `approx_topk` in a range query is
  refused with the reference's exact body
  `count min sketches are only supported on instant queries` — Loki
  surfaces it as HTTP **500** (probed live against `grafana/loki:3.7.4`),
  PulsusDB as `ReadError::PipelineInvalid` → HTTP **400**, per the same
  adjudicated rule as the entry above: gate on the shared body substring,
  never the status code.
- **Enablement delta (not gated):** the reference disables `approx_topk`
  by default (`limits_config.shard_aggregations` + protobuf frontend
  encoding — capture procedure in `tests/logqltest/PROVENANCE.md`);
  PulsusDB has no per-tenant limits config, so the construct is
  unconditionally available, matching the enabled configuration.
