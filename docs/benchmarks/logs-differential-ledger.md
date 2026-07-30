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
  call `observe_pair` for the same row; `crates/pulsus-read/src/logql/exec.rs`,
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
  treatment as the ratified instant `first_over_time`/`last_over_time`
  tie pin — and the corpus pre-commits to staying below the cap and off
  k-boundary ties (rule recorded in the `.test` header).
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
  (`crates/pulsus-read/src/logql/exec.rs`), never hand-computed here.

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

### `template-local-zone-environment` (issue #230, adjudication 3)

- The `Local` zone (`__timestamp__`, `date`, `toDate`) resolves from
  the process environment exactly like a reference process on the same
  host (`$TZ` name → that zone; no `$TZ` → `/etc/localtime`, named
  "Local"; else the degenerate UTC form). The hermetic corpus and its
  captures pin the degenerate-UTC form (stock container, PROVENANCE
  precondition). Residuals inside this class: `chrono-tz` 0.10.4's
  IANA tables vs the reference toolchain's (mainstream post-1970 zones
  agree), zone-abbreviation lookups for layout PARSING approximate
  Go's `lookupName` with instant±6-month probes, and zone-offset
  lookups clamp beyond chrono's ±262k-year range.

### `template-output-budget` (issue #230 follow-up, bounded divergence)

- **Reference behaviour:** template output size is UNBOUNDED — `repeat
  1073741824 "x"×17` (17 GB) OOM-kills the reference container
  (measured); `printf` padding widths up to 2^30 allocate eagerly.
- **PulsusDB behaviour:** every RETAINABLE render production — any
  string/bytes/list/map a template value can hold — is CHARGED against
  a cumulative per-render budget BEFORE it is built, and the budget is
  released when the render ends. That covers the multipliers
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
- **Threshold (derived, not chosen):**
  `MAX_TEMPLATE_RENDER_BYTES = MAX_CLIENT_AGG_GROUP_BYTES` (64 MiB) —
  the crate's established per-query retained-bytes budget (#104, reused
  by #221's fan-out charge): one render is the line path's peak
  transient retention, so it may not allocate more than a whole query
  is allowed to retain. The budget is CUMULATIVE over the render and a
  maximal output line is charged twice (once when the value is built,
  once when it is printed), so the single-`repeat` rejection boundary
  sits at budget/2 = 32 MiB of output: `tests/logql_template_engine.rs`
  pins both directions — a `repeat` of exactly budget/2 renders, one
  byte past it is the clean 422 on the streams, metric and
  `label_format` paths alike.
- **Why deliberate:** the reference has no bound, so no finite cap can
  match it (the #236 O1 shape); the standing charge-before-allocate
  rule (#227) and the "never copy the reference where it is wrong"
  ruling both require the bound. Consequences inside the same class:
  templates whose CUMULATIVE productions cross 64 MiB reject even when
  each individual value is small, and a dynamic (per-line-computed)
  regex pattern whose compiled program exceeds the 1 MiB ceiling gets
  the per-line `error parsing regexp: Compiled regex exceeds size
  limit…` where the unbounded reference would compile it. Overflowing
  `int` still panics with the reference's exact `strings: Repeat
  output length overflow` per line (that surface is bounded and
  correct).

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
