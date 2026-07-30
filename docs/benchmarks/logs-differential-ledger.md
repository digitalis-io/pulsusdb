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
  `MAX_VARIANT_SUB_STATES` = `AggCaps::DEFAULT.min_field()` (currently
  500, the point past which a divided per-sub-state cap would floor to
  zero; it moves with `MAX_CLIENT_AGG_SERIES`) and
  `MAX_VARIANT_FANOUT_STATE_BYTES` = `AggCaps::DEFAULT.group_bytes`
  (64 MiB) of charged fan-out state (plan-time spec clones + arena +
  per-sub-state snapshots, one counter end to end). The worked
  thresholds are emitted by the charge functions' own unit tests
  (`crates/pulsus-read/src/logql/exec.rs`), never hand-computed here.
- **(c) Per-variant series cap.** *Reference:* applies `maxSeries` PER
  VARIANT and SKIPS the breaching variant with a warning. *PulsusDB:*
  422s on the shared divided cap — the pre-existing #236 class
  (mid-scan group cap vs result-size cap), not re-litigated here.

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
