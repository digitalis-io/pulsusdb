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

### detected-cardinality-exact-not-estimated (issues #244, #261)

- **Construct:** `/detected_fields`' per-field `cardinality` (and, when
  issue #261 lands its sibling audit, `/detected_labels`' — cross-reference
  #261). Informational note, not a gate downgrade.
- **Direction:** **PulsusDB reports the EXACT distinct-value count** over
  the sampled entries; the reference reports a **p14 HyperLogLog
  estimate** (`grafana/loki` v3.7.4 =
  `b318f2829f0ae2094ab3a1e90780450e9e4b03be`,
  `pkg/querier/queryrange/detected_fields.go` `parsedFields.sketch`,
  vendored `github.com/axiomhq/hyperloglog` `New()` = precision 14,
  sparse). The estimate equals the exact count for every `N <= 5327`;
  the first divergence is `N = 5328` (sparse-key collision
  `"v2888"`/`"v5327"`), captured with the pre-committed larger points in
  `crates/pulsus-read/tests/golden/detected_cardinality/reference_divergence.tsv`
  and pinned by `detected_fields_witness.rs`'s AC 19 gate (our side is
  recomputed through the production accumulator; the reference column is
  the recorded estimate).
- **Reachability — NOT ESTABLISHED.** The divergence is real and is
  registered here at the ESTIMATOR level; the largest per-field
  cardinality reachable through the HTTP endpoint is **not established,
  and no bound is claimed**. An earlier revision of this entry asserted
  that `N >= 5328` was unreachable at default limits, reasoning from
  "one value per key per entry" and `line_limit <= 5000`. **That premise
  is false:** a SINGLE sampled row can contribute distinct values for
  the same key more than once — once from its structured-metadata pairs
  and again from the auto-parse pass over the post-pipeline line (both
  call `observe_pair` for the same row;
  `crates/pulsus-read/src/logql/detected_probe.rs`,
  `observe_detected_row` / `auto_parse_observe`) — so 5 328 distinct
  values fit comfortably inside 5 000 sampled rows. Deriving the true
  maximum needs that per-row multiplicity argued exactly, on both
  stores; a second wrong bound in this entry would be worse than an
  acknowledged gap, so none is given. What IS established: every
  `crates/pulsus-read/tests/logqltest/corpus/b14_detected_fields.test`
  case captures a cardinality `<= 100`, far inside the agreeing range,
  so every corpus case is pure hard-gated parity rather than a
  divergence.
- **Fixture status:** `/detected_fields` has no case in
  `test/fixtures/logs/differential.json`, so this entry is not referenced
  from the fixture (`informational_cases_are_recorded_in_the_committed_ledger`
  guards fixture-referenced entries only); it is registered here so the
  divergence has a ledger identity before any future fixture case lands.

### detected-fields-array-order-pinned (issues #244, #258)

- **Construct:** the ORDER of the `fields` array in a
  `/detected_fields` `200`. Per-field object shape and the zero-field
  body are byte-exact (issues #254/#258); this entry is scoped to the
  array order alone, which is why the endpoint is **not** byte-exact
  end to end for a populated response.
- **Reference behaviour (source, not inferred):** irreproducible **Go
  map iteration order**, at BOTH points that build the slice
  (`grafana/loki` v3.7.4 = `b318f2829f0ae2094ab3a1e90780450e9e4b03be`):
  - the single-response path fills `fields` with `for k, v := range
    detectedFields` over a `map[string]*parsedFields`
    (`pkg/querier/queryrange/detected_fields.go:57-75`, map built at
    `:282-284`);
  - the sharded MERGE path repeats it — `detected.MergeFields`
    accumulates into `map[string]*UnmarshaledDetectedField` and emits
    `for _, field := range mergedFields`
    (`pkg/storage/detected/fields.go:54-101`, called from
    `pkg/querier/queryrange/codec.go:1562-1590`).

  Nothing sorts on the way out: `WriteDetectedFieldsResponseJSON` is a
  straight `jsoniter` `WriteVal` of the slice as it stands
  (`pkg/util/marshal/marshal.go:182-188`). The Go spec leaves map
  iteration order unspecified and explicitly does not guarantee it is
  the same from one iteration to the next, and the gc runtime
  randomises it; so **no reproducible order is guaranteed to exist to
  mirror.** (That is the precise claim: the spec withholds a
  reproducibility guarantee. It is not a guarantee that two runs must
  DIFFER, which nothing states and which this entry does not rely on.)
  Corroborated by our own capture record:
  `crates/pulsus-read/tests/logqltest/PROVENANCE.md` rule A4 already
  transcribes captured fields as a SET for exactly this reason, citing
  case C1 where `uid` precedes `lvl`; the #258 capture's `msg`, `code`,
  `detected_level` is another draw from the same distribution, not a
  rule.
- **PulsusDB behaviour:** **label-ascending**, pinned in
  `FieldAccumulator::finish`. This is the ratified treatment of every
  irreproducible reference tie in this repo — the same call as
  `label-replace-collision-tie-order` and `approx_topk` beyond the
  retention cap. Mirroring is not available at any price: no reproducible
  order is guaranteed to exist. The SET of fields, and the
  `limit`/zero-field bodies, are reference-exact; the array ORDER is
  pinned rather than mirrored. Per-field OBJECT exactness is scoped in
  `detected-fields-jsonpath-survives-merge` below — it holds against the
  reference's single-response path, which is what the pinned single-node
  container serves and what our captures record, and deliberately does
  not hold against its sharded merge path.
- **Consumer impact:** none identified. `fields` is a lookup collection
  keyed by `label`, the reference ships no order guarantee a client
  could have depended on, and a deterministic order is strictly easier
  to consume and to diff.
- **Fixture status:** as the entry above — no
  `test/fixtures/logs/differential.json` case, registered for identity.
  Gated by `logs_detected_live.rs`'s label-ordered `fields_of`
  comparison and `detected.rs::finish_sorts_fields_by_label`.

### detected-fields-jsonpath-survives-merge (issues #254, #258)

- **Status: REGISTERED EXCEPTION — a deliberate divergence we decline to
  mirror, not an observation.** It is the parity mandate's
  "diverge from defects" arm, and it is the reason the per-field-object
  byte-exactness claim in `detected-fields-array-order-pinned` is scoped
  to the reference's single-response path.
- **Construct:** the presence of `jsonPath` on a per-field object in a
  `/detected_fields` `200`.
- **Reference behaviour:** path-dependent, and self-inconsistent. The
  single-response path carries `JsonPath` through
  (`pkg/querier/queryrange/detected_fields.go:71 @ v3.7.4`), but the
  SHARDED merge rebuilds every field as
  `{Label, Type, Cardinality, Parsers, Sketch}` with **no `JsonPath`
  field set** (`pkg/storage/detected/fields.go:92-99`), so the key
  vanishes under `omitempty`. The same query against the same data
  therefore returns paths on a single-shard deployment and silently
  loses them on a sharded one. Nothing in the proto or the handler marks
  this intentional; `MergeFields`'s neighbours (`Parsers`) are carried,
  so it reads as an omission when `jsonPath` was added.
- **PulsusDB behaviour:** `jsonPath` is emitted on **every** response,
  for every json-flattened field, regardless of how the answer was
  assembled.
- **Why we diverge rather than mirror:** mirroring would mean dropping a
  documented, client-consumable field on some responses and not others,
  with no rule a client could predict — the divergence would be *worse*
  for the consumer than the parity break. `jsonPath` is exactly the
  parseable structure the parity bar protects (it is what lets a client
  turn a detected field into a working `| json <expr>` selector for a
  nested key, issue #254), so dropping it conditionally is the outcome
  the bar exists to prevent. This is the standing "match the reference
  except where it is wrong" ruling applied.
- **Consumer impact of the divergence:** a client written against a
  sharded reference deployment sees a key it may not expect. It is
  additive and `omitempty`-shaped on the reference side, so a decoder
  that tolerates the single-shard reference tolerates ours.
- **Fixture status:** no `test/fixtures/logs/differential.json` case.
  Gated by `logs_detected_live.rs`'s per-field `jsonPath` assertions and
  `encode.rs`'s byte-exact goldens.

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
  This issue's own change is to the RANGE path: the sliding window
  replaces the tumbling one there, and the instant path already evaluated
  `(t - range, t]` at one evaluation instant. Issue #344 later corrected
  the instant path's `first_over_time`/`last_over_time` tie order to the
  same `(timestamp, StableHash)` delivery order this entry's sliding path
  uses (`pkg/iter/sample_iterator.go:139-148` @ v3.7.4), so the
  same-nanosecond same-stream residual above applies to both paths alike.
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

### grouping-dedup-avg-sharded-frontend (issue #288, oracle-config note — no case downgraded)

- **What diverges:** the reference's **sharded query-frontend path**,
  not its LogQL engine, and only under our oracle config. With
  `ci/logql/config.yaml`'s `frontend.encoding: protobuf` +
  `limits_config.shard_aggregations`, `avg by (fp, fp) (…)` — an `avg`
  whose `by` clause repeats a label — returns an **EMPTY 200**, while
  every other aggregation dedupes and serves, and `avg by (fp)` itself
  serves. The shard mapper rewrites `avg X` to `sum X / count X`
  carrying the DUPLICATED `Grouping.Groups` verbatim
  (`pkg/logql/shardmapper.go` `OpTypeAvg`, v3.7.4), and that division
  leg drops every match on the sharded path.
- **The behaviour of record:** the DEFAULT-config reference container
  serves `avg by (fp, fp)` identically to `avg by (fp)` (probed on the
  same image, same dataset) — consistent with its own engine
  (`VectorAggEvaluator` builds group labels from the metric's unique
  label set, so duplicates are inert). PulsusDB matches THAT — gated
  by the hermetic equivalence test below, never by a captured corpus
  row (b17 carries none for avg).
- **Why a note, not a divergence:** the engines agree; one frontend
  configuration of the reference disagrees with the reference itself.
- **How avg is gated (fix round 1, U6 — STRUCTURAL, not documentary):**
  `b17_grouping_dedup.test` carries **no** `avg` duplicate-grouping row
  at all, so there is no captured constant a future oracle-config
  recapture could silently rewrite to the artifact's empty 200. `avg`
  dedup is covered by the hermetic equivalence gate
  `logql_metric_agg_golden.rs::avg_by_duplicate_grouping_equals_the_deduped_form`
  (`avg by (fp, fp)` == `avg by (fp)` bit-for-bit through the real
  plan + client-aggregation path, anchored to hand-derived
  integer-exact values so it cannot pass vacuously) — a route no
  container run can touch. The b17 header repeats the do-not-re-add
  instruction with the condition for lifting it.

### frontend-step-alignment (issue #301, oracle-config note — no case downgraded)

- **What diverges:** the reference's **query-frontend**, not its LogQL
  engine. `metricQuerySplitter.split`
  (`pkg/querier/queryrange/splitters.go`, v3.4.2 L242, unchanged at
  v3.7.4 L236) calls `alignStartEnd(step, start, end)` on every RANGE
  metric request before the engine sees it: `start` is floored to a
  multiple of `step` **in absolute epoch time** and `end` is ceilinged.
  It is unconditional — it runs even when the query produces a single
  split, and it is NOT `align_queries_with_step` (that limit is `false`
  here and gates a different middleware). The only switch is
  `split_queries_by_interval`: `split_by_interval.go`'s `Do` returns
  `h.next.Do(ctx, r)` before the splitter when the interval is `0`.
- **Measured** against the pinned `grafana/loki:3.4.2` image
  (digest `sha256:58a6c186…`), one corpus, two containers differing only
  in that limit, `rate({…}[1m])`, `step=60`, request `start` off a
  60s boundary by `…157567938ns`:
  - default `1h`: points at `…760, …820, …` — exact multiples of 60,
    i.e. the requested `start` discarded;
  - `0`: points at `…715.157, …775.157, …` — `start + k·step`, byte-equal
    to PulsusDB and to the corpus expectation, including both partial
    edge windows.
  - With the default, a request for `[t₀, t₁]` returned a point BEFORE
    `t₀` and one AFTER `t₁` (requested `1785483001.158 → 1785483200.158`,
    returned `1785483000` and `1785483240`).
- **Verdict — PulsusDB is correct, the frontend rewrite is the defect.**
  Returning samples outside the requested `[start, end]`, at timestamps
  that are not `start + k·step`, breaks the Prometheus `query_range`
  contract the endpoint mirrors, and contradicts the reference's OWN
  engine (`pkg/logql`, start-anchored — the semantics issue #227 ported
  and the `logqltest` corpus pins). Same binary, two answers, decided by
  a tenant limit. PulsusDB does **not** implement the rewrite and must
  not: it would make our answer depend on config and put points outside
  the window a client asked for.
- **Consequence for the differential:** `deploy/e2e/loki.yaml` sets
  `split_queries_by_interval: 0`, so the oracle answers the range query
  it was asked and the five `metric_range` cases stay **`gated`** with
  their full value comparison (both edge buckets on an off-boundary
  grid — the most sensitive points of a sliding-window implementation).
  No case is downgraded and no ledger id is referenced from the fixture.
  Blast radius measured on the same two containers: only the five
  `metric_range` cases differ between the two configs; the other 32
  shipped cases (streams, `streams_limited`, instant, ordered-instant,
  error) are byte-identical.
- **History:** this is why `e2e-metrics-full (single)` was red from the
  2026-07-27 nightly (first scheduled run after issue #227 landed on
  07-26) — #227 moved the expectation from a tumbling, step-aligned grid
  (which coincided with the frontend's rewrite) to the start-anchored
  one, so the oracle's aligned timestamps stopped matching. Failure mode
  was `oracle diverged from the corpus expectation` on
  `metric_rate_sliding`, the first of the five range cases to run.

### matching-error-status-divergence (informational note, not a gate downgrade)

- **Cases:** `metric_match_multiple_err`, `metric_match_duplicate_err`
  (issue #91). These queries are runtime vector-matching failures on both
  stores.
- **Probed live against `grafana/loki:3.4.2`, re-probed byte-identical
  at `grafana/loki:3.7.4` (issue #240 wave0):**
  - many-to-one without a grouping modifier → HTTP **500**, body
    `multiple matches for labels: many-to-one matching must be explicit
    (group_left/group_right)` — PulsusDB's WHOLE wire body matches it
    byte-for-byte since issue #240 (before it, a fixed PulsusDB-only
    prefix on `ReadError::PipelineInvalid` made this claim false on the
    wire).
  - duplicate one-side signature (many-to-many) → HTTP **500**, body
    `found duplicate series on the right hand-side;many-to-many matching
    not allowed: matching labels must be unique on one side` — the same:
    byte-identical since issue #240, prefix-divergent before it.
- **Exact accepted delta:** Loki returns HTTP **500** for these
  execution-time matching errors; PulsusDB classifies them as a bad
  request (`ReadError::PipelineInvalid` → HTTP **400**), which is the
  semantically correct code for a user-query cardinality error. The two
  stores therefore agree on the error BODY but not the status code.
  Since issue #240 body identity is enforced BYTE-EXACTLY: the corpus
  rows carry `msg_exact:` (whole produced text) and
  `tests/logqltest_provenance.rs` checks A/B tie each pinned body to its
  `pulsus-240-bodies` capture row, both directions. The cases still
  deliberately do NOT gate the HTTP status: they gate the whole
  produced body, byte-exactly, via `msg_exact:` (issue #240 superseded
  the older shared-substring gate). This entry records the status-code
  divergence for the record; it is not a `mode: "informational"`
  downgrade (the cases remain gated on their bodies).

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
  treatment as the ratified same-nanosecond SAME-STREAM `tie_rank` order
  — and the corpus pre-commits to staying below the cap and off
  k-boundary ties (rule recorded in the `.test` header). (This bullet
  used to cite the instant `first_over_time`/`last_over_time` tie pin as
  its precedent. Issue #344 deleted that pin: the reference's instant tie
  order is specified after all, so ours was a wrong value rather than a
  choice among irreproducible orders, and it is no longer an example of
  anything.)
- **Range-rejection status delta (the matching-error-status-divergence
  precedent, third instance):** any `approx_topk` in a range query is
  refused with the reference's body, byte-for-byte:
  `count min sketches are only supported on instant queries` (since
  issue #240 this identity holds on the WHOLE wire body and is enforced
  by `b10_approx_topk.test`'s `msg_exact:` gate; before #240 the fixed
  `PipelineInvalid` prefix made it inner-text-only) — Loki
  surfaces it as HTTP **500** (probed live against `grafana/loki:3.7.4`),
  PulsusDB as `ReadError::PipelineInvalid` → HTTP **400**, per the same
  adjudicated rule as the entry above: gate on the body (byte-exact via
  `msg_exact:` since issue #240), never the status code.
- **Enablement delta (not gated):** the reference disables `approx_topk`
  by default (`limits_config.shard_aggregations` + protobuf frontend
  encoding — capture procedure in `tests/logqltest/PROVENANCE.md`);
  PulsusDB has no per-tenant limits config, so the construct is
  unconditionally available, matching the enabled configuration.

### error-pair-duplicate-slot (informational note, not a gate downgrade — issue #238)

- **Construct:** the out-of-band `__error__`/`__error_details__` pair
  (issue #238). No fixture case references this entry; the PulsusDB side
  is pinned hermetically by two corpus cases in
  `crates/pulsus-read/tests/logqltest/corpus/b12_error_pair_model.test`
  §3 (`b12c`): the streams shape `{service_name="b12c"} | logfmt | json`
  and the `eval_fail` on `count_over_time({service_name="b12c"} | logfmt
  | json [30m])`.
- **Reachable shape:** a parser extracts a label literally named
  `__error__`/`__error_details__` (an ORDINARY parsed label —
  `pkg/logql/log/parser.go` `Set(ParsedLabel, …)`, never the slots) and a
  LATER stage sets the out-of-band slot, e.g. `{…} | logfmt | json` over
  the line `__error__=mine __error_details__=mydet k=v`.
- **Reference behaviour (probed, `grafana/loki:3.7.4`):**
  `UnsortedLabels` appends the parsed entry (`labels.go:517`) and then
  `appendErrors` appends the slot (`labels.go:519-521`), and `labels.New`
  does not deduplicate — so BOTH entries reach the emitted slice. Streams
  JSON and the metric `metric` map dedupe last-wins (the slot value), but
  the metric error message renders both and picks its error type via
  `metric.Get(ErrorLabel)` = the FIRST = the parsed value:
  `pipeline error: 'mine' for series: '{__error__="mine",
  __error__="JSONParserErr", __error_details__="mydet",
  __error_details__="Value looks like object, but can't find closing '}'
  symbol", k="v", service_name="e238c"}'`.
- **PulsusDB behaviour (the pinned deterministic side):** one entry per
  name, slot value wins — the streams JSON and the metric series labels
  are IDENTICAL to the reference's deduped view; the metric error reads
  `pipeline error: 'JSONParserErr' for series: '{__error__="JSONParserErr",
  __error_details__="Value looks like object, but can't find closing '}'
  symbol", k="v", service_name="…"}'`.
- **Exact accepted delta:** only the doubly-populated metric ERROR
  MESSAGE differs — the selected error class (`'mine'` vs
  `'JSONParserErr'`) and the duplicate rendering inside the quoted
  series. Both stores still FAIL the query with a `pipeline error:`
  naming a real error class.
- **Why not matched (task-manager ratified, issue #238):** reproducing
  the duplicate-entry rendering and the first-wins error type would
  require a duplicate-permitting emitted label set, which breaks
  `labels_json` canonicalisation, stream fingerprinting and metric
  grouping — core representation everything else depends on (the
  same-nanosecond same-stream tie-order precedent). Reachable only when
  log data contains a literal `__error__`-family key extracted before an
  erroring parser.

### `variants-nonconforming-shape-status` (issue #221, adjudicated)

Head-of-group rule for the three `variants-*` entries, recorded once
(task-manager adjudication, issue #221): **we do not reproduce the
reference where it is self-inconsistent or crashing.** A nil-pointer
panic is not behaviour; a 500 where the same implementation returns 400
everywhere else is a bug, not a contract; a provably wrong value is not
a semantic. Where both sides reject, PulsusDB matches the REJECTION even
though the status differs — stated in each entry so a future reader does
not "fix" us toward the panic.

- **Reference behaviour (probed, `grafana/loki:3.7.4`,
  `enable_multi_variant_queries: true`):** a non-conforming variant shape
  inside `variants(...) of (...)` returns one of THREE different 500
  bodies — `expected range aggregation expression but got
  *syntax.VectorAggregationExpr` (doubly-nested vector aggregation),
  `expected aggregation operator but got "approx_topk"`, `unexpected
  empty result` (a literal/`vector(n)` variant) — or a nil-pointer PANIC
  (`runtime error: invalid memory address or nil pointer dereference`)
  for a binary variant (`count_over_time(A[5m]) + 1`) and for an
  unwrap-arity mismatch (`variants(sum_over_time({...}[5m])) of (...)`).
- **PulsusDB behaviour:** one plan-time 400 naming the rule (`variant N
  must be a range aggregation, optionally wrapped in one vector
  aggregation (...)`), before any DB read — except the arity mismatch,
  whose wording borrows the reference's non-variants arity phrasing
  (`invalid aggregation sum_over_time without unwrap`). The reference
  nil-panics on the variants form itself, so NO reference body exists
  to pin: the byte-exact provenance row is BLOCKED (issue #240 AC10 —
  a capture of the different, non-variants query is not substituted;
  `logqltest/PROVENANCE.md` §#240) and `b13_variants.test` gates that
  body by substring (`msg:`), claiming no reference identity.
- **Why deliberate:** both sides reject every one of these shapes — we
  match the rejection; the panic text is unmatchable by construction and
  a crash is not a contract. Gated by `b13_variants.test`'s `eval_fail`
  cases (each header-annotated with the observed reference status+body).

### `variants-surviving-error-status` (issue #221, adjudicated)

- **Reference behaviour (probed):** a surviving `__error__` inside
  `variants` returns **500 `unexpected empty result`**, or silently drops
  that variant while another answers — while the SAME implementation
  returns **400 `pipeline error: '<Err>' for series: '{...}'`** for the
  identical input outside `variants`.
- **PulsusDB behaviour:** the existing 400 `pipeline error: ...`,
  byte-identical to the reference's own non-variants surface, raised by
  the lowest-indexed failing variant (chunks fan out in index order —
  deterministic).
- **Why deliberate:** the reference contradicts itself; we match its
  consistent branch. Same class as the ratified
  `matching-error-status-divergence`.

### `variants-label-collision-and-fanout-bounds` (issue #221, adjudicated)

- **(a) `__variant__` collision.** *Reference:* with `| label_format
  __variant__="..."` in the COMMON pipeline, the consolidated extractor
  APPENDS a duplicate `__variant__` (add + sort, no override) and then
  routes samples by re-parsing the two-valued label string — probed: a
  `bytes_over_time` variant reports **2** where the truth is **58**, and
  a non-integer collision value yields an empty 200. Provably wrong
  output, stable within a run. *PulsusDB:* `append_variant_label`
  OVERRIDES (the index wins) — the single-valued outcome, values always
  correct. The corpus PRE-COMMITS (in `b13_variants.test`'s header, not
  post-hoc) to setting no `__variant__` in a common pipeline; the
  override is gated hermetically.
- **(b) Fan-out bounds.** *Reference:* unbounded in variant count.
  *PulsusDB:* a clean 422 `query_too_broad` at two DERIVED thresholds —
  `MAX_VARIANT_SUB_STATES` = `AggCaps::DEFAULT.min_field()` and
  `MAX_VARIANT_FANOUT_STATE_BYTES` = `AggCaps::DEFAULT.group_bytes`
  (256 MiB) of charged fan-out state (plan-time spec clones + arena +
  per-sub-state snapshots, one counter end to end). The worked
  thresholds are emitted by the charge functions' own unit tests
  (`crates/pulsus-read/src/logql/charge.rs`), never hand-computed here.

  **Re-derived by #236.** Deleting `AggCaps::series` (the mid-scan
  500-group cap) moved `min_field()` off that 500 and onto
  `MAX_TS_COLLISION_GROUP`, so `MAX_VARIANT_SUB_STATES` is now
  **10 000** — strictly permissive, in the direction the reference sits.
  It is also now **UNREACHABLE**: at #279's `MAX_QUERY_BYTES` (131 072,
  exclusive) the largest expressible variants query carries **4 368**
  variants, so no legal query can trip this backstop. The divergence is
  therefore registered as *unreachable* rather than live —
  `variants_past_the_derived_backstop_reject_at_plan_time` computes the
  verdict from the two constants and fails if it ever becomes reachable.
- **(c) Per-variant series cap.** *Reference:* applies `maxSeries` PER
  VARIANT and SKIPS the breaching variant with a warning. *PulsusDB:*
  applies the result-series cap per variant too (#236 — matching the
  reference's GRANULARITY, a strict acceptance win: a 3-variant query
  returning 3×400 series is served), but **422s** on breach where the
  reference skips-and-warns. The remaining divergence is the
  skip-and-warn behaviour, which needs a `warnings` response-envelope
  field that exists nowhere in the tree: owned by **#277**, a real
  parity bug deferred for sequencing, not an accepted shape.

### `label-replace-scalar-operand-status` (issue #276)

- **Reference behaviour (probed, `grafana/loki:3.7.4`):**
  `label_replace(2, "d", "r", "s", ".*")` — and any other scalar-typed
  operand, including a folded `1 + 1` — returns **500 `unexpected expr
  type (*syntax.LiteralExpr) for Evaluator type
  (*logql.DefaultEvaluator)`**: the evaluator factory has no arm for a
  literal where a sample expression is required.
- **PulsusDB behaviour:** a plan-time **400** `label_replace requires a
  vector operand, got a scalar expression`, decided in `fold_plan_ops`
  from the operand's series typing — before any DB read. The regex
  compile error keeps priority over it (the reference's own ordering:
  its parse-time regex error surfaces first).
- **Why deliberate:** both sides reject; the reference's body is an
  internal Go type-assertion message, the head-of-group rule
  (`variants-nonconforming-shape-status`) applies — we match the
  REJECTION, never the crash-shaped surface. Gated by
  `b16_label_replace.test`'s `eval_fail` cases and
  `label_replace_over_a_scalar_typed_operand_is_rejected_at_plan_time`.

### `label-replace-collision-tie-order` (issue #276)

- **Reference behaviour (probed):** when `label_replace` maps several
  range-query series onto ONE label set, the engine's per-step
  accumulation merges them into a single series whose points repeat per
  timestamp (`{src="same"} [[t,1],[t,1],[t,1],[t,1]] …` on the wire).
  The SAME-timestamp intra-order is the evaluator's per-step vector
  order — for an aggregated operand, a Go map walk the reference cannot
  reproduce even against itself.
- **PulsusDB behaviour:** the identical merged shape (timestamp-
  ascending, duplicates kept), with the same-timestamp tie pinned to
  the DETERMINISTIC input-series order — the ratified treatment of every
  irreproducible reference tie (`approx_topk` beyond the retention cap,
  same-nanosecond same-stream `tie_rank`; the instant
  `first/last_over_time` pin this used to name was DELETED by issue #344,
  which found the reference's order there specified and ours wrong).
  Values and multiset of
  points are reference-exact; only the intra-timestamp ordering is
  pinned rather than mirrored. Instant queries return the duplicate
  samples unmerged, exactly as the reference does (no divergence
  there). Gated by `b16_label_replace.test` (r2, c14) and
  `label_replace_range_collisions_merge_with_input_order_ties`.

### `label-replace-template-amplification` (issue #276 fix round 2 — the O8 threshold, WIRED)

- **Reference behaviour:** no cap exists on this path. The evaluator
  rewrites and retains every series' label set with no budget, so a
  large replacement template times a large series count materialises
  gigabytes of labels and exhausts process memory.
- **PulsusDB behaviour:** `apply_label_replace` charges
  `2 × W_LABEL_BYTE × L` bytes per series *before* allocating, `L =
  dst.len() + replacement.len() + #'$' × max_value_bytes` (each `$` can
  expand to the input's widest label value), against
  `MAX_POST_AGG_BYTES` (8 GiB); a breach is a clean **422**
  `query_too_broad`, never an OOM. **Where refusal begins, concretely:**
  impossible below `L = 2 413` template bytes at any series count
  (`L_MIN`, at `N = 435 645`); guaranteed from the amplifying term alone
  once `L × series > 1 431 655 765` byte·series
  (`MAX_POST_AGG_BYTES / (2 × W_LABEL_BYTE)`) — e.g. a `$`-free
  replacement of 3 286 bytes at the region's `N_max = 435 771` series;
  between the two, the input's own envelope terms decide and only lower
  the point of refusal.
- **Why deliberate:** folding `replacement.len()` into the ceiling was
  considered and rejected (fix round 2 ruling): the replacement is
  bounded only by the 131 072-byte query-text cap, so an absorbing
  ceiling would sit in the tens of gigabytes and stop protecting
  anything. Refusing where the reference OOMs is PulsusDB being correct
  — the reference being unbounded is not copied (the
  `template-output-budget` precedent). This funnel is wired today, as
  O6's and O7's are — rows (d) and (e) above, driven end to end by
  `both_amplifiers_are_refused_end_to_end_from_query_text`. Gated by
  `o8_the_label_replace_template_threshold_bounds_where_refusal_is_possible`
  (below `L_MIN` admitted over the whole feasible region, refusable at
  `L_MIN`, `$` gearing exact) and
  `label_replace_charges_the_collision_merge_clone_before_it_allocates`
  (charge lands before the allocation it prices); numbers regenerated
  by `zz_witness_report`.

### `byte-literal-render-quantization` (issue #350)

- **Reference behaviour (measured, grafana/loki:3.7.3 AND :3.7.4, both
  the default config and ci/logql/config.yaml):** a QUERY-side byte
  threshold is silently QUANTIZED to display precision. Measured: a
  size=1000 line passes `size >= 1KiB`; `1024B` and `1025B` behave as
  1000; `1536B` behaves as 1500; decimal-round values like `3kB`/`1TB`
  compare exactly. Zero-valued literals are unserveable (`0KB` → 400
  `binary literal has no digits` on the default config, an internal
  retry-storm 500 under the comparison config; the error names `0B`, a
  spelling the query never contained). The CONSISTENT explanation — not
  traced end to end, stated as such — is the threshold's render
  round-trip: `BytesLabelFilter::String` renders the parsed value
  through `humanize.Bytes` with spaces stripped
  (`pkg/logql/log/label_filter.go`), whose 1-decimal SI output ("1.0kB",
  "0B") matches every measured quantization step and every observed
  error spelling exactly. The measurements are the pinned facts; the
  mechanism is the reading they support.
- **PulsusDB behaviour:** the ENGINE-exact threshold — `1KiB` is 1024,
  every accepted literal compares at its parsed value. Zero-valued
  literals are REJECTED (both engines reject; parity, not divergence).
- **Why deliberate:** the reference disagrees with itself — it documents
  and parses `1KiB` as 1024, and the value it then compares against is
  measurably 1000. That is
  display-precision corruption of a comparison value, the
  head-of-group class (`variants-nonconforming-shape-status`): we match
  the reference's consistent branch (its own engine), never the
  corruption. Gated by `b8_byte_parity.test`'s 1KiB boundary row, which
  is header-marked as this PINNED divergence and NOT a container
  capture (issue #350's provenance discipline).

### `five-year-span-cap` (issue #343, owner mandate — a deliberate limit, not a defect)

- **Reference behaviour, measured** on the digest-pinned v3.7.4 oracle
  (`grafana/loki@sha256:87f0a067…`, buildinfo `3.7.4` / `b318f282`): both
  LogQL duration literals are accepted across the whole `i64` nanosecond
  domain — `offset 2562047h47m16s854ms775us807ns` (`i64::MAX`) and
  `offset -9223372036854775808ns` (`i64::MIN`) are 200s, as is
  `[2562047h]`, a 292-year window — and the query's own `start`-to-`end`
  span is not bounded at all.
- **PulsusDB behaviour (the delta): NOTHING IN A LogQL QUERY MAY SPAN MORE
  THAN 5 YEARS** — `MAX_QUERY_SPAN_NS` = 157,680,000,000,000,000 ns =
  43,800 h = 5 × 365 d. One rule, three places, all against that one
  constant:
  1. `offset` magnitude, either direction (parser).
  2. The `[range]` selector (parser).
  3. The query's `start`-to-`end` span (planner, `plan()` — so it covers
     streams and metric queries alike). An instant query has no span; its
     window is bounded by the capped `[range]`.
- **Status and shape:** `400 bad_data`, the same class as the query-text
  cap and `DurationOutOfRange`. The value the user sent is echoed —
  `offset too long (-43801h > 43800h)`, `range too long (43801h > 43800h)`,
  `query time range of N ns is outside the supported range (0 to
  157680000000000000 ns)` — never a clamped value: someone asking for a
  stupid number is told plainly rather than silently handed a different
  answer.
- **Why:** retention is days to months and nobody queries five years of
  logs, so this refuses nothing a real deployment does. What it does
  remove is the whole class of absurd-input arithmetic issue #343 chased
  down four successive layers — including the last hole, a `start` in 1677
  with an ordinary `offset 1h`, which the two literal caps alone would not
  have closed.
- **Pinned by** `crates/pulsus-logql/src/parser.rs`'s
  `both_duration_literals_cap_at_five_years_and_refuse_rather_than_clamp`
  and `the_span_cap_is_exactly_five_365_day_years`, and
  `crates/pulsus-read/src/logql/plan.rs`'s
  `a_query_span_over_five_years_is_refused`.

### `offset-domain-edge-exact-arithmetic` (issue #343, informational note — not a gate downgrade)

- **Reference behaviour, cited:** v3.7.4
  (`b318f2829f0ae2094ab3a1e90780450e9e4b03be`)
  `pkg/logql/range_vector.go:50-52` shifts the evaluation domain once at
  the boundary — `start = start - offset` / `end = end - offset` — in
  plain `int64` nanoseconds, and `:195` / `:589`
  (`ts := r.current/1e+6 + r.offset/1e+6`) invert that shift on emit.
  Plain `int64` subtraction **WRAPS** when the shifted instant leaves the
  domain, relocating the window to an unrelated instant.
- **PulsusDB behaviour (the delta):** the same one-shift-at-the-boundary
  structure, evaluated EXACTLY (`i64::checked_sub`). When the shift leaves
  `[i64::MIN, i64::MAX]` the query answers **empty** — it neither wraps
  (the reference) nor clamps onto the rail (what PulsusDB shipped before
  this fix, which scanned `timestamp_ns > 223372036854775807`, a
  1977-01-08 floor, for a query about 2026). No new rejection surface: a
  large, negative or domain-crossing offset is a 200 answering empty,
  never a 400.
- **The residual.** Notation: request range spec `S <= E`, step `p`,
  offset `d`, range `r`; `A = S - d`, `B = E - d` EXACT;
  `MIN = -2^63`, `MAX = 2^63 - 1`; `min_stored_ts = 0` and
  `max_stored_ts = 4_294_943_999_999_999_999` are the ingest gate's floor
  and ceiling (1970-01-01 .. 2106-02-06,
  `crates/pulsus-write/src/protocols/loki_push.rs`). The shift fails iff
  `A` or `B` leaves `[MIN, MAX]`; `A < MIN AND B > MAX` is impossible (it
  needs `E - S > 2^64 - 1`), so four entry branches partition the failing
  space:

  | branch | entry inequality | what is lost, UNDER THE 5-YEAR CAPS |
  |---|---|---|
  | **R1a** low, total | `B < MIN` ⟺ `d > E + 2^63` (forces `E <= -2`) | **absence-only** — every grid point is `< MIN`, so its window `(g-r, g]` lies wholly below `MIN` and no representable sample can occupy it. **Still reachable**, with an ordinary `offset 1h` and `E` near `MIN` |
  | **R1b** low, partial | `E + 2^63 >= d > S + 2^63` (forces `S <= -2`) | **absence-only now.** Reachable only at the corner where `d` is the full 5-year cap and `S` within it of `MIN` |
  | **R1c** high, total | `-d > MAX - S` (**no sign constraint on `S`**) | **absence-only now** (its value-op clause pointed at R2, which is dead). Reachable with `S` within 5 years of `MAX` |
  | **R1d** high, partial | `MAX - E < -d <= MAX - S` | **absence-only now.** Reachable with `S, E` within 5 years of `MAX` |
  | **R2 / R2-instant** | a beyond-rail grid instant whose window reaches stored data | **UNREACHABLE** — see below |

  **The `five-year-span-cap` row above narrowed this residual to
  `absent_over_time` alone — FOR DATA ADMITTED THROUGH THE ENFORCED
  INGESTION PATHS.** Before the caps, R1b and R1d each carried a
  demonstrated witness that lost a STORED SAMPLE; both are now impossible,
  and R2 — the only route by which a beyond-rail window could reach stored
  data at all — cannot be expressed. **Every "impossible" below is
  conditional on `min_stored_ts = 0` and
  `max_stored_ts = 4_294_943_999_999_999_999`, which are the ingest gate's
  bounds (`crates/pulsus-write/src/protocols/loki_push.rs`), not a
  storage-enforced invariant** — stopping point (ii) below says the same
  thing about the branch conditions, and it applies here verbatim. A row
  written by another door (a direct ClickHouse `INSERT`, a restored
  backup) can hold a timestamp outside those bounds, and then the
  proofs' premises no longer hold and the branch it lands in loses real
  data again. Enforcing the timestamp domain at storage would make the
  unconditional claim true; that is a larger change than this issue and is
  not taken here. The three arguments, each under that condition:

  * **R1b**: `B = E - d <= (S + cap) - d < (d - 2^63 + cap) - d = MIN + cap`,
    far below `min_stored_ts = 0`, so the necessary condition
    `E - d >= min_stored_ts` is unsatisfiable.
  * **R1d**: `B > MAX` forces `E > MAX - cap`, and the span cap then forces
    `S >= E - cap > MAX - 2*cap = 8908012036854775807`; every in-domain
    window's lower bound is `>= A - r >= 8.9e18 - cap`, far above
    `max_stored_ts`, so `S - d - range < max_stored_ts` is unsatisfiable.
  * **R2**: reaching `max_stored_ts` from `MAX + 1` needs
    `range > 4928428036854775809 ns` (~156.28 years). The range cap is
    43,800 h — 31 times too small. `[1369008h]`, the shortest range that
    would have reached, does not parse.

  **So no value-producing operation can lose real data any more, for any
  row the ingest gate admitted.** What remains is `absent_over_time`'s
  synthetic `1`s on branches that need a request sitting within 5 years of
  an `i64` rail. For a row loaded outside those paths the older, wider
  analysis above it still applies unchanged. The rows are kept
  rather than deleted so that raising a cap re-opens them visibly; the
  three witnesses below are recorded as they were MEASURED, and two of
  them are no longer expressible.

  Elsewhere below, the word is **possible**, never *does*.
- **The two corrected necessary conditions**, each certainly true:
  - **R1b**: real-data loss requires `E - d >= min_stored_ts` (a lost
    sample satisfies `s <= g <= B`). `E - S >= 2^63 + 1` follows from it
    and is kept only as a consequence.
  - **R1d**: real-data loss requires `S - d - range < max_stored_ts` —
    every in-domain window's lower bound is `>= A - r`, so if
    `A - r >= max_stored_ts` no stored sample can be inside one.

  **Sufficiency depends on grid phase, step, range and the storage bounds
  together and is deliberately NOT enumerated.** A span-only formulation
  was tried and is **not even necessary**: the R1d witness below has span
  `4_928_428_036_854_775_808`, one nanosecond under that bound, and loses
  real data anyway.
- **The three witnesses** as originally measured — **two of them (R1b, R2-instant) can no longer be expressed under the 5-year caps**, and the R1d one is likewise dead; they are kept as the record of what the branches did before the caps, and as the shape to re-derive if a cap is ever raised:
  - **R1b** — `S = -9223372036854775808`, `E = 1779627963145224192`,
    `step = 1000000000000000`,
    `count_over_time({app="x"}[5m] offset 1h)`, one row at
    `1779624363145224192`. Admitted end to end (the HTTP fence saturates
    the span to `i64::MAX`, `MAX / 1e15 = 9223 <= 11000`);
    `A = -9223375636854775808` underflows, `B = 1779624363145224192` does
    not; the exact grid holds 11 004 points and its last one is exactly
    `B`, whose `(B-5m, B]` window holds that row. Exact answers `1` at
    caller grid point `1779627963145224192`; PulsusDB answers empty.
  - **R1d** — `S = -1105056000000000000`, `E = 3823372036854775808`,
    `step = 500000000000000`,
    `count_over_time({app="x"}[5m] offset -1500000h)`, one row at
    `4294943999999999999` (`max_stored_ts`).
    `A = 4294944000000000000 <= MAX`, `B = 9223372036854775808 = MAX + 1`;
    `A - 5m = 4294943700000000000 < max_stored_ts <= A`, so grid point
    `k = 0` covers the row. Exact answers `1`; PulsusDB answers empty.
  - **R2-instant** — instant `at_ns = 3823372036854775808`,
    `count_over_time({app="x"}[1369008h] offset -1500000h)`, one row at
    `4294943999999999999`. `A = MAX + 1`; the window
    `(4294943236854775808, MAX+1]` holds the row. Exact answers `1`;
    PulsusDB answers empty.
- **R2 and R2-instant — a beyond-rail grid instant whose window still
  reaches stored data.** The first such instant is `MAX + 1`, and
  `(g-r, g]` is strict on the left, so it reaches `max_stored_ts` only
  when

  ```text
  range > (i64::MAX + 1) - max_stored_ts
        = 9_223_372_036_854_775_808 - 4_294_943_999_999_999_999
        = 4_928_428_036_854_775_809 ns   (~1_369_007.79 h ~ 156.28 years)
  ```

  so `[1369008h]` can reach and `[1369007h]` cannot — that pair is the
  test boundary.
- **R1-instant:** the instant spec is the degenerate `A = B = at - d`. Not
  representable ⇒ empty, which is exact for value ops when `at - d < MIN`;
  for `absent_over_time` the exact answer is a single `1`.
- **Where the enumeration stops**, stated rather than implied: (i) it
  assumes `S <= E` — for `S > E` the grid is empty on both sides and every
  branch is vacuous; (ii) `min/max_stored_ts` are the *ingest gate's*
  bounds, so data loaded by another door widens the value-op branches,
  which is why the inequalities are written in terms of those symbols and
  not baked constants; (iii) sufficiency, as above.
- **Why the residual is accepted:** every branch needs an absurd input — a
  292-year request span, a 236-year offset, or a 156-year `[range]` — and
  the sub-grid trim that would close them puts extra arithmetic on the one
  site the whole correctness argument rests on. Adjudicated on issue #343.
- **Pinned by** `crates/pulsus-read/tests/logql_metric_agg_golden.rs`, one
  test per live branch (`r1a_…`, `r1b_…`, `r1c_…`, `r1d_…`) plus
  `r2_is_unreachable_under_the_five_year_range_cap`, which asserts the
  156-year arithmetic and measures the parse refusal rather than
  evaluating a branch that can no longer occur. Each names this entry. If
  a branch is ever made exact, or a cap raised, its test is what has to
  change.

## Issue #236 — high-cardinality aggregations: the result-size cap

- **(a) Result-series cap semantics.** *Reference:* `querier.max-query-series`
  (default 500, `pkg/validation/limits.go:373`) counts the series a metric
  query RETURNS, enforced on the final result (`pkg/logql/engine.go:538`
  instant, `:588` accumulated across steps); nothing anywhere caps scanned
  or inner-aggregation groups. *PulsusDB:* identical semantics, threshold
  and `> cap` test, enforced by `ensure_result_series` on the whole
  expression's output and read in exactly one place (gated by
  `max_query_series_is_read_in_exactly_one_place`). Before #236 PulsusDB
  applied its 500 MID-SCAN, rejecting a broad class of aggregations the
  reference serves (`sum(...)` over 20,505 groups returned `{} 20505`
  there and a 422 here); that divergence is CLOSED. The remaining
  difference is the HTTP status — 422 `query_too_broad` where the
  reference returns 400 — carrying the reference's verbatim body, under
  the established matching-error-status precedent.

- **(b) `avg`/`stddev`/`stdvar` member order.** *Reference:* computes these
  with Welford's online recurrence (`pkg/logql/evaluator.go:547-550`,
  finish `:586-596`), which is ORDER-SENSITIVE, and accumulates in Go map
  order — so the reference is nondeterministic run to run on identical
  data (measured 10/2 over 12 runs). *PulsusDB:* ports the recurrence
  bit-for-bit (so the values are the reference's, not the former
  `sum/len` + two-pass `population_variance`) and then PINS the member
  order ascending by label set, one sort per stage before grouping
  (`pin_reduction_order`), because PulsusDB's own incoming order is a
  randomly-seeded `HashMap` walk. Without the pin the same query returned
  different bits on different runs — 6 failures in 20 runs of
  `logqltest_corpus`; with it, 20/20.

  **Residual, and it must not be over-read.** Deterministic is not the
  same as order-identical to the reference. The committed
  `b4_vector_aggs.test` captures sit in a wide majority basin —
  enumerating all 24 member orders of `{2,4,6,8}`, 20 of them (including
  ascending) yield exactly the captured `stdvar=5.0` /
  `stddev=2.23606797749979` — so the green corpus is genuine evidence
  that the pin agrees with the reference on that data. It is NOT proof
  that the sorted order IS the reference's: on other data a different
  member order could differ in the last bit, and no order can match a
  source that does not reproduce itself. What PulsusDB guarantees is
  reproducibility; agreement with any single reference run is only
  established where captured.

- **(c) The non-mutating range leaf still materialises its series before
  aggregating** — the price of (b), recorded as a residual rather than
  remembered. *Reference:* evaluates a range query step by step, so a
  `sum(...)` over a very wide non-mutating selector holds one step's
  vector at a time and serves it. *PulsusDB:* the streaming aggregation
  fold (issue #236 Part B) collapses the leaf's output to
  `output groups x grid points` on the label-mutating fan-out path — the
  shape this issue exists to serve — but the NON-mutating path folds only
  after its sliders have finished, from `series_out`, so that vector is
  still materialised at `streams x grid points`.

  **Why, and it is a consequence of (b), not an oversight.** Feeding the
  fold at each slider's close would be strictly better on memory, but
  sliders complete in FINGERPRINT order (the scan's physical-key order),
  which is deterministic yet is not the label-set order (b) pins. Folding
  in it would make the folded answer differ from the materialised one in
  the last bit for `sum`/`avg`/`stddev`/`stdvar` — i.e. it would buy
  memory by breaking the equivalence the fold's correctness argument
  rests on. Sliders cannot be reordered without buffering, and the buffer
  IS `series_out`.

  **The price, stated before it bites.** Once emitted points are charged
  (`MAX_METRIC_RESULT_POINTS`, not yet levied), a non-mutating range leaf
  wide enough that `streams x grid points` exceeds the charge will be
  REFUSED where the reference serves it. The bound is the product, so it
  is reached by breadth and grid fineness together, not by either alone.
  Removing it needs a step-ordered evaluator (**#250**) or a fold order
  that a streaming emit can reproduce — not a larger constant.

- **(d) O6 — the `by(...)` grouping-name amplifier.** *Reference:* has no
  post-aggregation byte bound at all: it evaluates step-ordered and never
  materialises the inner matrix, so a wide `by(...)` clause costs it one
  step's worth of keys. *PulsusDB:* materialises the stage, so
  `group_key`'s `By` arm builds one owned pair per `by` NAME per output
  group; the funnel charges that as `W_GROUPNAME x series x
  group_name_bytes` against `MAX_POST_AGG_BYTES` (8 GiB) and refuses
  above it with `MetricPostAggBytes` (HTTP 422). **Threshold, as a
  number a reader can compare their query against: `A_MIN = 597` total
  `by`-clause bytes** — with `A_NAME_MIN = 2` (a one-character name plus
  its separator) that is **at least 299 one-character `by` names** —
  measured at the argmin `N = 435,558` series. Strictly below `A_MIN`,
  refusal is impossible at ANY group count; above it, refusal begins at
  proportionally fewer series. **Reachable**: 597 bytes fits easily
  inside `MAX_QUERY_BYTES = 131,072`, and
  `both_amplifiers_are_refused_end_to_end_from_query_text` drives the
  refusal from real query text. Owner: **#250** (a step-ordered
  evaluator removes the materialisation this bound exists for).

- **(e) O7 — the `group_left/right(include)` amplifier.** *Reference:*
  same reason as (d) — no equivalent bound. *PulsusDB:* `instant_join`
  copies each include label onto every many-side series through
  `set_label_sorted`'s insert chain, so the cost is `B_INCLUDE x
  many.series x include_bytes` where `include_bytes = Σ (name.len() +
  one.max_value_bytes + 1)`. **Threshold: `AMP_MIN = 97,030,221`**, the
  smallest `many.series x include_bytes` PRODUCT at which refusal is
  possible, at the argmin `N_many = 546`. **Reachable** within the query
  text cap — a few thousand include names against a one side carrying a
  kilobyte-scale label value clears it — and the same test drives that
  refusal end to end. Owner: **#250**.

  **What the cap does and does not claim, for both (d) and (e).**
  `MAX_POST_AGG_BYTES` is derived by MEASUREMENT — a cohort-attributed
  allocator witness (`tests/logql_post_agg_witness.rs`), coefficients =
  the observed per-unit rates times a stated 2x margin — not by
  enumerating containers. It guarantees that every client-leaf-sourced
  input carrying NEITHER amplifier is admitted, and nothing broader. It
  is not a worst-case proof: the residual is a distribution adversarial
  in a dimension no ladder varies, and the margin is what covers it.
  Before #236 this stage had no bound at all and could OOM; the
  divergence registered here is a clean bounded refusal replacing an
  unbounded path.

- **(f) The reference's 500 is NOT a pure result-size cap for
  NON-SHARDABLE aggregations — PulsusDB over-accepts there.** *Measured*
  on `grafana/loki:3.7.4` (digest `sha256:87f0a0…56cfcc`, default config)
  at exactly 501 distinct inner groups, with the boundary confirmed by
  capture at 499 / 500 / 501: the reference **serves** `sum`, `count`,
  `min`, `max`, `avg` (bare and grouped), `sum by (<low-cardinality>)`
  and `sum(sum by (id) (…))`, and **rejects** `topk(k)`, `bottomk(k)`,
  `stddev`, `stdvar`, `sort`, `sum by (id)`, the bare leaf,
  `sum(topk(600, …))` and `count(topk(3, …))` — all with the same
  `maximum number of series (500) reached for a single query…` body,
  even where the FINAL result is 1 or 3 series. The split is
  shardability: Loki's frontend rewrites the associative aggregations
  into per-shard sub-queries so the wide inner vector never materialises,
  while the others materialise it and trip `max_query_series` on that
  intermediate. *PulsusDB:* applies the 500 to the final result only, so
  it **serves** `topk(3, …)`/`stddev(…)`/`bottomk(…)` over 501+ inner
  groups where the reference refuses. That is **over-acceptance**, the
  same direction (and the same disposition) as the SQL instant path's
  missing series cap: PulsusDB answers a query the reference declines,
  which no user query breaks. It is registered rather than fixed because
  matching it would mean reintroducing an intermediate group cap — the
  exact rejection surface issue #236 exists to delete — and because the
  reference's own behaviour here is an artefact of its sharding plan, not
  of its documented limit semantics (`pkg/validation/limits.go:373`
  describes series "returned by a metric query").

  **This corrects plan v14 §1's live probe**, which recorded `stddev`,
  `topk(3, …)`, `approx_topk(3, …)` and `sum(topk(600, …))` as served
  over 600 groups. They are not, on the pinned image.

## Issue #230 — `line_format`/`label_format` template engine

The full Go `text/template` + reference function-map surface landed in
issue #230; 688 container-captured corpus directives — 678 `eval`
(60+228+34+29+258+69 across `tests/logqltest/corpus/t1…t6_*.test`) +
10 `eval_fail` reject-parity cases (all in t1) — replay byte-exact
hermetically, including execution-error strings. (The pre-round-2
"676 cases" figure was 666 `eval` + the 10 `eval_fail` counted without
saying so; round 2 added 12 captured `fromJson` invalid-UTF-8 /
surrogate cases to t6.) The following residuals are the
complete divergence set, each pinned deterministic (owner adjudications
on #230: byte-model + lossy boundary; pin-deterministic where the
reference is non-reproducible; error WORDING is not load-bearing where
clients only display it).

### `template-pinned-address-cells` (issue #230, plan v7 §D + capture)

- **Reference behaviour:** 29 cells of the 224-cell printf verb×shape
  domain print a process memory address — either per-process heap
  pointers (23 cells, proven by the committed dual-container diff:
  `cargo run -p xtask -- template-audit`) or package-global addresses
  identical across containers but coupled to the reference binary's
  layout (6 cells, caught by the audit's decimal/octal/binary
  address-token scan). Exact cell list = the audit's flagged union,
  re-derivable on any reference bump.
- **PulsusDB behaviour:** the pinned address constant `0xfa11ed`
  (16388589) in the same structural position — shape-asserted in
  `tests/logql_template_engine.rs`; a grep-gate proves no corpus golden
  ever contains a pinned-address rendering.
- **Why deliberate:** the values are addresses of the reference
  process's own memory; not reproducible even by the reference across
  rebuilds.

### `template-tzdata-table-cells` (issue #230, capture-surfaced plan correction)

- **Reference behaviour:** 11 further cells (`%O %c %U %e %E %f %F %g
  %G %t` + the catch-all rune on a LOADED `*time.Location`) dump the
  zone's ENTIRE IANA transition table (with NUL bytes and invalid
  UTF-8). Deterministic within one binary but coupled to the embedded
  tzdata release. Plan v7 §D classified these as goldens because its
  R1 criterion was address-presence only; the capture surfaced the
  gap (goldens 195 → 184, exclusions 29 → 40).
- **PulsusDB behaviour:** the same struct shape with PINNED-EMPTY
  `zone`/`tx` tables and the pinned `cacheZone` address (`chrono-tz`
  does not expose raw transition tables; OQ-3's ratified tzdata-skew
  class).
- **Why deliberate:** tzdata-release-coupled output; the adjudication
  on #230 OQ-3 pins deterministic substitutes for this class.

### `template-exec-depth-cap` (issue #230)

- **Reference behaviour:** runaway recursive `{{define}}` invocations
  error at depth 100 000 (`exceeded maximum template depth (100000)`),
  a bound that relies on Go's growable goroutine stacks.
- **PulsusDB behaviour:** the same per-line error at depth 250 —
  derived from the stack floor, not chosen: the smallest stack the
  render runs on is the 2 MiB default (tokio workers, test threads),
  one invocation level costs ≈2 KiB of debug-build frames (a 1000-deep
  cap measurably overflowed a 2 MiB thread), so 250 levels ≈ 0.5 MiB
  with a 4× margin. A crash is never acceptable; reachable only by
  deliberately recursive templates.
- **Round 2:** the 250 counter is UNIFIED across every evaluator
  recursion site — `{{template}}` invocation, nested
  `if`/`with`/`range` walks and parenthesized-pipeline evaluation all
  increment the same counter. A cap that counted only the invocation
  hop let a recursive define whose body nests controls multiply the
  two depths past the stack (250 invocations × 30 uncounted if-levels
  overflowed a 2 MiB thread — the #272 class); gated in
  `logql_template_engine.rs`
  (`combined_define_recursion_and_nesting_…`).

### `template-parse-depth-cap` (issue #230, review round 2)

- **Reference behaviour:** the parser caps parenthesized-pipeline
  nesting at 10 000 (`text/template/parse.go maxStackDepth`, error
  `max expression depth exceeded` — captured verbatim on
  grafana/loki:3.7.4: depth 10 000 renders, 10 001 rejects) and does
  NOT cap control nesting at all (2 000 nested `{{if}}` render on the
  container; in practice its 131 072-byte query-length limit bounds
  nesting at ≈12 k).
- **PulsusDB behaviour:** ONE structural-depth counter across every
  parser recursion shape — nested control bodies and `else if`/`else
  with` CHAINS (guarded in `parse_control`, whose frame stays live
  across a chain — round 3 fixed the `item_list` guard placement that
  let a 5000-link else-if chain bypass the cap and SIGABRT a 2 MiB
  thread), nested `block` bodies (`block_control`), and parenthesized
  pipelines (`term`) — capped at 40 with Go's own error
  text (`template: line:N: max expression depth exceeded`, surfaced
  through the same `invalid line template: `/`invalid template for
  label…` compile-error wrapping the reference uses for its paren
  cap). Derived from the measured floor, not chosen: ~170 nested
  controls (≈12 KiB/level of debug parser frames) or ~400 nested
  parens overflow a 2 MiB thread — a live SIGABRT pre-fix — so 40 is
  a 4× margin on the worst unit. Depths 41..10 000 parse in the
  reference and reject here (compile-time 400, same shape as the
  reference's own paren rejection); realistic templates nest < 10.
  Gated on a 2 MiB thread both ways in `logql_template_engine.rs`
  (`structural_nesting_is_capped_at_parse_time_…`,
  `else_if_and_else_with_chains_are_capped_…`). The 40 stays the
  ledgered interim (round-3 adjudication): it is not raised without
  iterative parsing, the #255 shape.

### `template-error-wording-residuals` (issue #230, owner wording ruling)

- **regex compile errors** inside `regexReplaceAll`/
  `regexReplaceAllLiteral`/`count`: the reference embeds Go's
  `error parsing regexp: missing closing ): …` wording in
  `__error_details__`; PulsusDB embeds rust-regex's diagnostic behind
  the same `error parsing regexp: ` prefix. Accept/reject boundaries
  match (both RE2-class engines); the error CLASS
  (`TemplateFormatErr`) and position prefix are byte-exact. Kept out
  of the corpus; hermetically gated instead.
- Everything else — including `divf`'s `decimal division by 0`,
  slice-bounds panics, `unixToTime`'s `%!w(<nil>)` quirk, goodFunc
  signature rejects and coercion errors — proved byte-exact in the
  captured corpus and stays in it.

### `template-timezone-configured` (issue #311, supersedes `template-local-zone-environment` from #230 adjudication 3)

- **Reference behaviour:** the `Local` zone used by the template time
  functions (`__timestamp__`, `now`, `date`, `toDate`) is resolved from
  the PROCESS — `$TZ` names the zone, else `/etc/localtime` is read and
  the result is named "Local", else the degenerate UTC form.
- **PulsusDB behaviour:** the zone is resolved from SERVER
  CONFIGURATION — `reader.template_timezone` / `PULSUS_TEMPLATE_TIMEZONE`
  (docs/configuration.md §6), defaulting to `UTC`. `$TZ` and
  `/etc/localtime` are never read on any path that can reach a query
  result. An unknown zone name fails config load; there is no fallback.
- **Why we diverge:** host-resolved state makes one query return
  different text depending on which server in a cluster answered it —
  a defect in a database, and one that already reddened CI once (a
  fixture generated under `Europe/London` failed under `Etc/UTC`,
  #272). Configuration is not the same thing as ambient inheritance:
  it is declared once and uniform across the fleet, rather than
  discovered per machine.
- **The same behaviour remains available, it merely has to be stated
  rather than inherited.** A deployment that deliberately runs in a
  local zone sets `template_timezone` to it once and gets exactly what
  the reference gave it: a configured zone keeps its own IANA name,
  which is precisely the reference's `$TZ=<name>` branch. Only its
  `/etc/localtime` branch — the one that renames the zone to "Local" —
  has no counterpart here, because nothing reads that file.
- **Invisible in the common deployment:** the shipped default, `UTC`,
  produces Go's degenerate all-nil `Local`, which is also what a stock
  reference container (no `$TZ`, no host zoneinfo) produces. The
  hermetic corpus and its captures pin that form (PROVENANCE
  precondition) and are unchanged.
- Residuals inside this class, unchanged from #230: `chrono-tz`
  0.10.4's IANA tables vs the reference toolchain's (mainstream
  post-1970 zones agree), zone-abbreviation lookups for layout PARSING
  approximate Go's `lookupName` with instant±6-month probes, and
  zone-offset lookups clamp beyond chrono's ±262k-year range.

### `template-output-budget` (issue #230 follow-up, bounded divergence)

- **Reference behaviour:** template output size is UNBOUNDED — `repeat
  1073741824 "x"×17` (17 GB) OOM-kills the reference container
  (measured); `printf` padding widths up to 2^30 allocate eagerly.
- **PulsusDB behaviour:** every RETAINABLE render production — any
  string/bytes/list/map a template value can hold — is CHARGED against
  a cumulative per-ROW budget BEFORE it is built, and the budget is
  released when the row's pipeline run ends. (Per-ROW since issue #260,
  which moved the lifetime off the individual render: a render's output
  is RETAINED by its caller — `line_format` into the line, every
  `label_format` destination via `set_label` — so a per-render budget
  bounded ONE live buffer while the number of simultaneously-live
  buffers was bounded only by the query-text cap. A `label_format`
  destination costs ~26 source bytes, so >4 000 of them, each holding a
  64 MiB output, fitted inside 131 072 bytes. Every render one row
  performs now shares one budget; renders of different rows do not.
  Every retention point on that path is a `template::Retained` or a
  `template::LabelSnapshot` — private-field types in a leaf module that
  holds only those types and their constructors, each of which charges
  before it allocates and none of whose PUBLIC constructors takes a
  length, a writer or a buffer from its caller (a charge the callee
  cannot verify is not a
  charge, and a charge reconciled after the allocation it pays for is
  not charge-before-allocate — the concatenating constructor charges
  each piece BEFORE pushing it, so a source that writes more than it
  sized refuses without the buffer ever growing) — so the set is the
  COMPILER's rather than a swept list: the two compile-time fast paths
  (`line_format "{{.a}}"` derives a single-substitution `Simple`,
  `label_format d="{{.a}}"` a text+field `Parts`; each copies straight
  into the retained destination at an exactly known length), the
  full-engine render, its byte→`String` repair at the pipeline boundary
  (invalid UTF-8 expands, and inside a caller's `Vec<u8>` that expansion
  was nobody's), and the once-per-stage `label_format` data-map
  snapshot, which deep-copies every OWNED value in the label set —
  including output the row was already charged for — while a
  `Cow::Borrowed` clone is a free pointer copy and costs nothing.)
  That covers the multipliers
  (`repeat`'s `count × len`, `indent`/`nindent`, `align*`, `printf`
  padding widths/precisions, `Replace`-with-empty-needle, the
  regex-replace bound, case mapping, `fromJson`'s 35× tree ceiling,
  the ≤10× `date`/`Time.Format` layout expansion), the print-family
  and html/js/urlquery output buffers (pre-charged at 4×/7× value
  ceilings), action output (`print_value_go` charges the value's
  ceiling BEFORE writing), text-node emission per iteration, dynamic
  regex programs (a 1 MiB `RegexBuilder::size_limit` ceiling charged
  per dynamic compile; query-compile-time literal programs are shared,
  not re-charged), AND plain input-bounded copies (`__line__`, the
  trim/trunc/substr/slice family, `default`'s clone, `Append*`'s
  argument copy) — review round 2 closed the fifth amplification
  class, where an uncharged per-call copy repeats inside a
  `range`/variable-only body that emits no text, or COMPOUNDS through
  `{{ $a = printf "%s%s" $a $a }}` (uncharged, that doubling is a
  literal OOM-kill — reproduced), and — round 3 — the IDENTITY /
  no-match / `n == 0` early-return copies (`replace` with `old ==
  new`, `align` at its identity count) that a single-shape check let
  through. Scalar-returning parse scratch (freed by return, nothing
  retained) and once-per-render error paths stay uncharged. The
  evidence split (round 3, the #236/#272 demotion): the AST census
  (`logql_template_alloc_census.rs`) is a DRIFT TRIPWIRE over new or
  changed sites, and the runtime gate
  (`logql_template_alloc_gate.rs`) is the dominance proof — every
  registry function runs through its branch shapes (happy / empty /
  identity / no-match / error) asserting allocated ≤ charged, plus a
  near-exhausted-budget ORDERING leg that fails any charge moved
  after its allocation (mutation-verified). Round 4 added a DERIVED
  invalid-UTF-8 variant of every shape (big string arguments get
  invalid bytes mechanically, not by hand-listing), which surfaced
  and closed the conversion gap on the regex argument/pattern paths.
  Round 5 reserved a ≤3× ceiling before converting; round 6 replaced
  that with the stronger form — the repaired length is PRECOMPUTED,
  charged, and the buffer allocated ONCE at exactly that size, because
  `from_utf8_lossy` may start at `len` and grow-double to 4×
  (cumulatively requesting up to 7×), so no constant is a provable
  bound. Valid-UTF-8 inputs borrow and charge nothing, so every pinned
  boundary is unchanged. The gate additionally asserts, structurally,
  that every registered shape EXECUTES and that every shape committed
  to the ordering leg actually REACHES it — a shape that errors
  upstream is otherwise indistinguishable from a shape that passed. A breach aborts the query with the bounded
  `422 query_too_broad` (`TooBroadReason::TemplateOutputBytes`) — never
  a per-line `TemplateFormatErr`, never a truncation, never an OOM.
- **Threshold:** `MAX_TEMPLATE_RENDER_BYTES` = 64 MiB, a standalone
  constant asserted at compile time to stay
  `<= MAX_CLIENT_AGG_GROUP_BYTES` — one row's template output may not
  out-allocate what a whole query is allowed to retain. (#230 spelled
  this as an equality with that constant; #236 raised the group-byte
  cap to 256 MiB for a reason specific to the GROUP axis, so the link
  was severed and #230's shipped 64 MiB preserved byte-for-byte.) The
  budget is CUMULATIVE over the row and a
  maximal output line is charged twice (once when the value is built,
  once when it is printed), so the single-`repeat` rejection boundary
  sits at budget/2 = 32 MiB of output: `tests/logql_template_engine.rs`
  pins both directions — a `repeat` of exactly budget/2 renders, one
  byte past it is the clean 422 on the streams, metric and
  `label_format` paths alike. `tests/logql_render_budget_composes.rs`
  pins the per-ROW half: a `label_format` stage whose destinations each
  fit comfortably but whose SUM does not is the same clean 422, and the
  identical fixture is shown to be ACCEPTED under a reconstructed
  per-render lifetime, so the gate fails if the lifetime regresses.
- **Why deliberate:** the reference has no bound, so no finite cap can
  match it (the #236 O1 shape); the standing charge-before-allocate
  rule (#227) and the "never copy the reference where it is wrong"
  ruling both require the bound. Consequences inside the same class:
  templates whose CUMULATIVE productions cross 64 MiB reject even when
  each individual value — and, since #260, each individual RENDER — is
  small, and a dynamic (per-line-computed)
  regex pattern whose compiled program exceeds the 1 MiB ceiling gets
  the per-line `error parsing regexp: Compiled regex exceeds size
  limit…` where the unbounded reference would compile it. Overflowing
  `int` still panics with the reference's exact `strings: Repeat
  output length overflow` per line (that surface is bounded and
  correct).

### `json-flatten-key-budget` (issue #287, bounded divergence)

- **Reference behaviour — the FLATTEN itself is parity, only the
  ceiling diverges.** grafana/loki v3.7.4, `pkg/logql/log/parser.go`:
  `JSONParser.parseLabelValue` names every leaf label with
  `buildSanitizedPrefixFromBuffer()`'s `_`-joined ancestor path, so a
  nested object's whole prefix is repeated into each of its leaves.
  PulsusDB emits the identical names — the label semantics must NOT
  change. What the reference does not have is any ceiling on the
  result: no cap in `JSONParser`, none in `LabelsBuilder.Set`
  (`pkg/logql/log/labels.go:344`).
- **Name parity was NOT free, and is now held by capture** (review
  round 2). PulsusDB used to join RAW keys and sanitize the joined
  result, which agrees with `appendSanitized`
  (`pkg/logql/log/util.go:42`) only for keys needing no trimming. The
  reference trims each part, drops a part that trims empty WITHOUT its
  separator, applies the leading-digit `_` only when nothing has been
  emitted yet, maps each rejected RUNE to one `_`, and DROPS a
  top-level field whose key sanitizes to nothing. Container-captured
  divergences, all fixed: `{" a ":1}` gave `_a_` (reference `a`),
  `{"x":{" b ":1}}` gave `x__b_` (`x_b`), `{"  ":{"b":1}}` gave `___b`
  (`b`), `{"x":{"":1}}` gave `x_` (`x`), `{"a":{"b":{" ":{"c":1}}}}`
  gave `a_b___c` (`a_b_c`), and `{"":1}` emitted a label with an EMPTY
  NAME. 51 probes captured from `grafana/loki:3.7.4` — 28 construction
  rows, a 19-cell collision matrix covering the depth × label-category ×
  dropped-or-live product, and 4 recorded divergences — with the RAW
  container responses committed as
  `tests/fixtures/json_key_sanitization/capture.json`;
  `tests/logql_json_key_sanitization.rs` derives every assertion about
  what a probe yields from that artifact (the four recorded divergences
  are asserted as relations between the artifact-derived reference side
  and our computed output, values included; the literals left in the
  suite fall into five machine-inventoried classes — probe inputs, the
  extractor's response-schema names and constants, pins and plumbing
  such as the artifact path and image pin, the `_extracted` rule
  constant, and pre-fix `was` records asserted only as differing —
  plus assertion-message prose). The artifact's PROVENANCE is attested by
  the CI drift leg's live re-capture of all 51 probes against the
  digest-pinned oracle on every run; the extractor's nanosecond
  timestamp and execution-stats checks are local schema-plausibility
  sanity, not a provenance proof. Regeneration is
  gated on a live container reporting exactly v3.7.4. **Still
  divergent, out of scope and recorded there (artifact-evidenced, filed
  on #334) rather than claimed:** parsed-vs-parsed key COLLISIONS —
  `{"a-b":1,"a.b":2}` is `a_b="1"` for the reference
  (`ParserHints.Extracted`, first wins) and `a_b="1"` plus
  `a_b_extracted="2"` for PulsusDB (`add_extracted`'s suffix) — and
  `drop <base-label>` before `| json` on a recolliding key at EITHER
  depth (the reference's `BaseHas` reads the original stream labels and
  still renames; our collision check reads the evolving set and does
  not). Both rules predate this issue and are shared by every parser.
- **Why that matters:** the emitted key bytes are `Θ(L²)` in the line
  length. For `{"<p bytes>":{"k00000":0,… ×m}}` the input is
  `p + 11m + 6` bytes and the keys are `m·(p + 7)` bytes, maximised at
  `≈ L²/44`. Measured end to end: **65 536 input bytes → 97 615 872 key
  bytes (1 489.5×)**; 1 MiB extrapolates to ~23.3 GiB by the same
  closed form. `/query_range` pays this whenever the query carries
  `| json`; `/detected_fields` pays it on every sampled line
  unconditionally, through its auto-parse probe.
- **PulsusDB behaviour:** every key string the full-flatten allocates —
  the emitted leaf label names AND the intermediate object prefixes
  they are built from — is CHARGED before it is allocated against a
  per-ROW ledger shared by all of the row's `| json` stages (the #260
  lifetime lesson: a per-STAGE ledger would bound one stage while the
  number of stages is bounded only by the query-text cap, and each
  extra `| json` re-flattens the line into another simultaneously-live
  `_extracted` label set). A breach aborts the query with the bounded
  `422 query_too_broad` (`TooBroadReason::JsonFlattenKeyBytes`) —
  never a per-line `__error__`, never a truncated label set, never an
  OOM. It bounds key bytes and nothing else: extracted VALUES are
  linear in the line (each scalar's text appears once in the input),
  `null`/array fields build no key and are charged nothing, and the
  targeted form `| json v="a.b"` takes its label names from the
  compiled stage and is charged nothing — it still serves the very
  line the full-flatten refuses.
- **Threshold:** `MAX_JSON_FLATTEN_KEY_BYTES` = 64 MiB, the template
  budget's value for the template budget's reason, asserted at compile
  time to stay `<= MAX_CLIENT_AGG_GROUP_BYTES`. What is spent against
  it is `alloc_block_bytes(key_len)` — the crate's pinned bound on the
  block a real allocator RETAINS for one exactly-reserved allocation,
  not the request size, because `String::with_capacity(n)` guarantees
  only `capacity >= n` — so the ceiling admits 32 MiB of key CONTENT
  per row and the worst-SHAPED line it refuses is ~38 KiB. It is a TERM
  of the published per-query retained-byte figure
  (`MAX_QUERY_RETAINED_BYTES` = 2,087,477,248 B): #260's row term was a
  free-standing addend that lost this ledger entirely, and is now a
  destructured `RowBudgets` table. That table is a convention with a
  compiler-checked back half, NOT a proof — a new variant answered with
  an existing field still compiles (verified by mutant); a lexical
  tripwire (`tests/logql_row_budget_enumeration.rs`) catches that shape
  and cannot catch a ledger that never declares a variant. A key COUNT cap
  would not be a bound at all here — the 64 KiB construction emits only
  2 979 keys — while a normally-shaped line, whose key bytes are a small
  multiple of `L`, passes at tens of MiB against the ~38 KiB
  worst-SHAPED refusal above. `tests/logql_json_flatten_budget.rs` pins the
  closed forms by measurement, the exact ±1 byte boundary, the
  intermediate-prefix charge (with the leaf-only counterfactual
  asserted, so a ledger that charged only what it emits fails it), the
  per-row shared lifetime and its reset, and the streams, metric and
  `/detected_fields` surfaces.
- **Why deliberate:** the reference is unbounded, so no finite cap can
  match it (the #236 O1 shape); "never copy the reference where it is
  wrong" and the standing charge-before-allocate rule both require the
  bound. Consequence inside the same class: a line whose flattened keys
  cross 64 MiB is refused where the reference would serve it (or die
  trying) — reachable in practice only from adversarially-shaped JSON,
  since it needs a multi-KiB parent key over thousands of leaves.

### `logql-error-envelope` (issue #240)

- **What changed:** `ReadError::PipelineInvalid`'s `Display` is now the
  BARE `reason`. The removed prefix bytes — recorded here ONCE,
  deliberately outside `crates/` so AC1's zero-hit grep cannot rot —
  were `invalid pipeline: ` (18 bytes, trailing space included). For
  the bodies with a captured reference counterpart (the two runtime
  vector-matching errors and the range `approx_topk` rejection) the
  wire body is now byte-identical to the reference's and gated so
  (`msg_exact:` + provenance checks A/B, rows B1–B3). The `variants`
  unwrap-arity body has NO reference counterpart — the reference
  nil-panics on that query — so its byte-exact row is BLOCKED per
  issue #240 AC10 (`logqltest/PROVENANCE.md` §#240) and it stays on a
  substring gate; every other `PipelineInvalid` body is PulsusDB-only
  prose.
- **Accepted cosmetic divergence (owner-ruled):** PulsusDB does NOT
  reproduce the reference's `parse error : …` / `stage '…' :` envelope
  wording around parse/pipeline errors. Status must match, the response
  container must match, the accept/reject decision must match; the
  message prose need not. (The JSON-vs-`text/plain` CONTAINER
  divergence is tracked separately in **#264**; the WebSocket close
  frame still truncates reasons at 123 bytes.)
- **Rejection-status fix (probed, `pulsus-240-status`):** an
  uncompilable regex in a pushed-down line filter (`{…} |~ "("`) or a
  stream matcher (`{app=~"("}`) was a ClickHouse-side 500 `internal`;
  the reference answers 400 on the query and index-stats routes
  (probed 2026-07-27 at v3.7.4, both 400). Every path that turns a
  user regex into SQL now validates it first, in exactly the form it
  emits (`escape.rs`'s `_checked` renderers; the raw escapers are
  module-private; PromQL/TraceQL hold sealed capability tokens —
  PromQL's SQL fallback deliberately defers to ClickHouse's RE2 as the
  regex authority (#280), TraceQL Rust-compiles upstream and migrates
  to `_checked` under #282). The generated SQL is byte-identical; the
  cost is one plan-time `Regex::new` per pushed regex, never per row.
- **Residual, recorded not built (filed as #286):** `ch_string` remains
  `pub`, so a future `logql/` file could render an UNANCHORED user
  regex into a `match(body, …)` fragment without touching the regex
  seam. The anchored form of that bypass is closed by the provenance
  check-C guard (`^(?:{` at exactly two committed sites); the
  unanchored form is a dataflow property no lexical gate can see —
  the fix is a validated-literal newtype through `logql::sql`'s
  builders (#286). The corpus runner's pushdown blind spot (**#278**)
  is why AC7's gates are Rust tests rather than corpus rows.

## Issue #279 — LogQL query-text cap

The 131,072-byte `MAX_QUERY_BYTES` cap (docs/api.md §2.3, the reference's
`maxInputSize`) matches the reference exactly at the parse seam. Its one
divergence is a transport-layer bound discovered while shipping it.

### `get-request-target-uri-bound` (issue #279, informational note, not a gate downgrade)

No fixture case references this entry — nothing is downgraded. It records
a divergence at a public surface, found while implementing the cap.

- **What diverges:** an over-cap LogQL query (131,072 bytes or more, the
  400 `bad_data` row in docs/api.md §2.3) cannot reach PulsusDB through
  ANY GET query string. Our HTTP stack bounds the whole request-target
  at **65,534 bytes** — `http::Uri`'s hard maximum — under half the cap,
  so the request is refused by the HTTP layer before routing and before
  any PulsusDB code runs. The bound is on the request-target as a whole
  (path + `?query=` + the percent-encoded value, where `{` `"` `}` `[`
  `]` each cost 3 bytes), so the longest query text a GET can actually
  carry is somewhat under 65,534 and depends on the route and the query's
  own punctuation. The same bound also blocks legitimate **sub-cap**
  queries: everything from roughly 65.5 KB up to the 131,071-byte longest
  accepted query is unreachable by GET too, even though the parse seam
  would accept it.
- **PulsusDB behaviour, measured 2026-07-30, two surfaces of the one
  limit:**
  - *In process* (the `tower::ServiceExt::oneshot` harness the
    `logs_api` tests use): `"…".parse::<http::Uri>()` succeeds at 65,534
    bytes and fails at 65,535 with **`InvalidUri(TooLong)`**; a
    request-target carrying a 131,072-byte query is 131,097 bytes and
    fails the same way. This is why #279's AC5 `/index/stats` GET leg
    pins alias/native identity on a short query and pins the over-cap
    rejection of that same handler directly in `stats.rs`.
  - *Over a real socket* (`axum::serve` + default hyper h1 — the exact
    configuration `crates/pulsus-server/src/serve.rs:225,237` runs in
    production): a 65,534-byte request-target reaches the handler
    (`200`); at 65,535 bytes hyper answers **`HTTP/1.1 414 URI Too
    Long`** with `content-length: 0` and `connection: close`, before
    routing. 100,000- and 131,097-byte targets: the same 414.
- **Reference behaviour (probed 2026-07-30, `grafana/loki:3.7.4`
  @ `sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`,
  fresh container, `GET /loki/api/v1/query?query=…`):** a 100,000-byte
  query (curl `size_request` 100,120 bytes — request line plus headers,
  so a request-target well past 100,000) → **200** `application/json`,
  empty vector; a 131,071-byte query (`size_request` 131,191) → **200**
  likewise; a 131,072-byte query → **400** `text/plain; charset=utf-8`,
  `Content-Length: 51`, body `parse error : input size too long
  (131072 > 131072)`. So the reference serves GET request-targets far
  above our 65,534-byte ceiling, and applies its own cap at the same
  131,072 boundary we do — the downstream half is established, not
  assumed.
- **Exact accepted delta:** for a GET whose request-target exceeds
  65,534 bytes PulsusDB answers **414 with an empty body** where the
  reference answers **200** (sub-cap query) or its **400 `parse error :
  …`** (at or over the cap). Status, response container (no JSON error
  envelope at all) and — for sub-cap queries — the accept/reject
  decision all differ. At or below a 65,534-byte request-target the two
  agree; and the cap boundary itself (131,071 accepted / 131,072
  rejected `400` with the same reason text, `input size too long
  (131072 > 131072)` — the JSON-vs-`text/plain` container divergence is
  the separate #264) agrees whenever the query arrives by POST instead.
- **Why this is a divergence and not a defect we chose:** the limit is
  imposed by our HTTP stack (`http::Uri` stores its length in a `u16`),
  not by any PulsusDB decision, and it is not reachable through
  configuration; it surfaces a different error — a bare 414 from the
  transport — than the reference produces for the same request. Nothing
  in the cap's own design or in the routing layer sets it, and no
  PulsusDB code observes the request.
- **What actually carries an over-cap query in production:** exactly five
  routes — those that both take a query parameter and accept a POST form
  body: `/query`, `/query_range`, `/series` (per `match[]` value),
  `/detected_labels`, `/detected_fields`. Each is mounted on **both** the
  native `/api/logs/v1` and the `/loki/api/v1` alias prefix, so ten paths
  in total. Those are where `MAX_QUERY_BYTES` and its `400 bad_data`
  `input size too long (…)` envelope are genuinely exercised
  (byte-identically on native and alias routes, `logs_api/mod.rs`).
  **The carrier set is not "the `GET|POST` form routes"** — that phrase is
  false in the other direction, and a reader deriving the list from it
  gets six. There are six `GET|POST` form routes per prefix, four in
  `mount_log_query_routes` (`/query_range` `:55`, `/query` `:59`,
  `/labels` `:63`, `/series` `:71`) and two in `mount_detected_routes`
  (`/detected_labels` `:85`, `/detected_fields` `:89`), both helpers
  called for each prefix (`mod.rs:99,104` native; `:114,126` alias).
  **`/labels` is `GET|POST` and is NOT a carrier:** `labels_impl`
  (`handlers.rs:272`) consumes only `parse_bounds` — `start` and `end`.
  It never reads a `query` parameter, never builds a selector and so
  never reaches the cap seam; **among POST-form carriers**
  `pulsus_logql::parse`/`parse_selector` occur at `handlers.rs:122`
  (`/query_range`), `:190` (`/query`), `:365` (`/series`) and
  `detected.rs:88`, `:153` — the five carriers and nothing else. That
  exclusivity is scoped to the carrier set, not repo-wide: the server
  crate has **nine** production parse sites, and the other four —
  `patterns.rs:51`, `stats.rs:50`, `tail.rs:123`, `volume.rs:52` — take a
  `query` parameter on GET-only routes, so they can never receive an
  over-cap one (`git grep -nE 'pulsus_logql::parse(_selector)?\('
  -- crates/pulsus-server/src` = 10 lines, the tenth being the
  `handlers.rs` in-module test). A query of any length cannot arrive at
  `/labels`, so listing it would assert a reachability that does not
  exist.
  **Correction to an earlier reading:
  `/tail` is NOT such a carrier** — `/api/logs/v1/tail` is a GET
  WebSocket upgrade (`logs_api/mod.rs:100,119`), so on the wire it sits
  under the same 65,534-byte ceiling; its cap enforcement is pinned at
  the params seam (`parse_tail_params`, `tail.rs`), not over a socket.
  The other GET-only routes — `/stats`, `/volume`, `/patterns`,
  `/label/{name}/values` and the `/index/*` aliases — likewise cannot
  receive a long query by any transport. On three of those the reference
  does mount POST (probed at v3.7.4: `POST /loki/api/v1/index/stats`,
  `/index/volume`, `/patterns` all `200`), a pre-existing and already
  documented method-matrix deviation (docs/api.md §2.6.1) that this
  entry only notes as the reason those routes have no long-query carrier
  at all.
- **Re-derivation (both halves, no fixture needed):**
  - *Ours:* parse `http::Uri` at 65,534 / 65,535 bytes; and serve any
    `axum::Router` with `axum::serve` on `127.0.0.1:0`, then write a raw
    `GET <target> HTTP/1.1\r\nHost: …\r\nConnection: close\r\n\r\n` with
    an all-unreserved-character target of 65,534 and 65,535 bytes and
    read the response. (Use unreserved characters: an unencoded `{` in
    the target is an invalid request-target and yields hyper's generic
    empty `400` at any length, which is a different rejection.)
  - *Reference:* `podman run --rm -d -p 127.0.0.1:3199:3100
    grafana/loki:3.7.4`, wait for `/ready`, then
    `curl -sS -G --data-urlencode "query@<file>"
    http://127.0.0.1:3199/loki/api/v1/query` with files holding
    `count_over_time({app="a…"}[5m])` at exactly 100,000 / 131,071 /
    131,072 bytes.

### `grouped-avg-over-time-unexplained` (issue #344 review round 1, MEASURED, MECHANISM UNIDENTIFIED)

**This entry is not a deliberate divergence.** Every other entry in this
file records a difference we chose, with a reason. This one records a
difference we **measured and cannot yet explain**, and it is here so that
the measurement is not lost and so that no future reader mistakes the
absence of a corpus row for an oversight.

- **What was measured.** On the pinned `grafana/loki:3.7.4`, default
  single-binary config, the reference answers a GROUPED `avg_over_time`
  differently from an UNGROUPED one over the same samples in the same
  order. One stream, four samples at distinct ascending timestamps
  `{83.2, 42.2, 79.0, 12.6}`:

  | query | answer |
  |---|---|
  | `avg_over_time({…} \| logfmt \| unwrap v [5m])` | `54.24999999999999` |
  | `… by (env)` / `by ()` / `by (service_name)` / `without (v)` | `54.25` |
  | `… without ()` (the no-op grouping) | `54.24999999999999` |

  `54.24999999999999` is the range reducer's own recurrence
  (`mean += F/count - mean/count`, `pkg/logql/range_vector.go:379-401`
  batched / `:716-744` streaming @ v3.7.4) folded in timestamp order.
  `54.25` is `sum/count` — the reference also answers `sum_over_time`
  = `217` over those four samples. Reproduced on a second fixture:
  `{1, 5, 7}` grouped answers `4.333333333333333`, which is `13/3`.

- **Stable, so not a Go-map walk.** 25 consecutive runs of each form
  returned the same value every time.

- **NOT a fold-order effect**, which is the explanation to rule out first
  and the one this issue's other work was about. `stdvar_over_time` over
  the SAME stream answers `832.6475000000002` both grouped and ungrouped,
  and that fixture's 24 permutations span five distinct values — so the
  fold order is unchanged by the grouping and only `avg` moves.

- **Not located in the query mappers.** `pkg/logql/rangemapper.go`'s
  `splittableRangeVectorOp` does not list `OpRangeTypeAvg`, and
  `pkg/logql/shardmapper.go`'s `avg -> sum(x)/count(x)` rewrite
  (`:259-275`) is for the VECTOR aggregation, not the range one. **The
  mechanism is unidentified.** No mechanism is asserted here.

- **What PulsusDB does.** It computes the reducer's recurrence for both
  the grouped and the ungrouped form, so it answers `54.24999999999999`
  either way. **We do not reproduce the grouped value**, because doing so
  would mean making our reducer return a different result depending on
  whether the query carries a `by (…)` clause — copying an inconsistency
  we cannot explain, at a cost of one ULP.

- **What IS proven, and where.** `avg_over_time`'s recurrence is pinned
  by the UNGROUPED row in
  `crates/pulsus-read/tests/logqltest/corpus/b18_range_agg_grouping.test`
  (the `service_name="avgr"` section), captured from the same container:
  `54.24999999999999` over those four values, chosen so the range
  recurrence, the vector aggregation's `(F - mean)/count` recurrence and
  `sum/count` give three different answers. That row is the parity claim;
  there is deliberately no grouped `avg` row beside it, and the file's
  header says why.

- **Re-derivation.** `podman run --rm -d -p 3100:3100 grafana/loki:3.7.4`
  (no config file — the deltas in `ci/logql/config.yaml` are not needed
  and were not used), push the four values one second apart on one
  stream, then query `avg_over_time`, `avg_over_time … by (env)`,
  `avg_over_time … without ()`, `stdvar_over_time` and
  `stdvar_over_time … by (env)` at an instant covering them.
