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

- **Construct:** two endpoints' `cardinality`, one estimator.
  `/detected_fields`' per-field `cardinality`, and — landed by issue #261,
  no longer a forward reference — `/detected_labels`' per-key
  `cardinality`, `uniqExact(val) AS cardinality` in
  `crates/pulsus-read/src/logql/sql.rs:163-178`. On the reference both come
  from the same sketch type: `newParsedFields` and `newParsedLabels` each
  build `hyperloglog.New()` (`pkg/querier/querier.go:934`, `:942` @ `grafana/loki`
  v3.7.4 = `b318f2829f0ae2094ab3a1e90780450e9e4b03be`), and
  `countLabelsAndCardinality` (`querier.go:757`) reports the raw
  `v.Estimate()` (`querier.go:799`). Informational note, not a gate
  downgrade.
- **Direction:** **PulsusDB reports the EXACT distinct-value count**; the
  reference reports a **p14 HyperLogLog estimate** — `New()` = `New14()` =
  `newSketchNoError(14, true)`, precision 14 with the sparse
  representation enabled (`vendor/github.com/axiomhq/hyperloglog/hyperloglog.go:27`,
  `:30`; module `github.com/axiomhq/hyperloglog v0.2.6`), the value taken
  raw from `Estimate()` with no post-processing
  (`hyperloglog.go:161-172`).
- **The agreement threshold is a property of the VALUE STRINGS, not of
  `N`.** An earlier revision of this entry named a single number, 5327,
  as the largest distinct-value count at which the two are guaranteed to
  agree, without saying which values it had been measured on. **That
  claim was false as stated**: it was measured on one value family
  (`v{i}`) and written as if it held for all of them. The family
  `svc-{i}` diverges at **`N = 4533`**, below 5327 — confirmed
  end-to-end against the container.

  **The correction must not repeat the mistake, so state the floor
  exactly.** There IS a universal `N` below which the two always agree:
  `N <= 1`. A single value has nothing to collide with, one insert can
  never trip the sparse-to-dense check, and one is below the linear
  counter's exactness ceiling — so agreement at `N <= 1` holds for every
  value set there is. It is also useless, and it is the ONLY such `N`:
  from `N = 2` upward a collision is possible, so no larger bound holds
  for all value sets and none is claimed here.
  Above that floor the estimate equals the exact count only while **all
  three** of the following hold. The first two depend on the value
  strings; the third does not:
  1. **no sparse-key collision yet.** In sparse mode each value is
     encoded into a 25-bit key derived from `metro.Hash64(v, 1337)`
     (`vendor/.../utils.go:44`, `sparse.go:18-25`); the first pair of
     values sharing a key makes the count low by one. Which pair that
     is, and at what `N` it arrives, is a property of the strings.
  2. **the sketch is still sparse.** `maybeToNormal` flips to the dense
     estimator once the varint sparse list's **byte** length exceeds `m`
     (`hyperloglog.go:76-83`, `compressed.go:108-110`), after which the
     answer is an estimate by construction.
  3. **the sparse-key count is below 8192.** Sparse mode returns
     `uint64(linearCount(2^25, 2^25 - count))` (`hyperloglog.go:161-165`,
     `utils.go:31-34`, `mp = 1<<25` at `hyperloglog.go:11-14`), and that
     value truncates back to `count` for every `count < 8192`, first
     missing at `count = 8192`, where it returns 8193. Re-derived here
     from those lines rather than inferred from the captures. This is the
     one part of the picture that is a property of the ESTIMATOR alone,
     and it is exactly why the instrument hazard recorded in PROVENANCE
     lands on 8192. Note it bounds the sparse-key count, not `N`: above
     8191 an exactly-offsetting number of collisions could still land on
     `N` by coincidence, so it is not a bound above which agreement is
     impossible.
  Measured first divergences, fresh sketch per `N`, one family per row —
  **each is that family's threshold and nothing else's**: `v{i}` **5328**
  (the #244 capture, a sparse-key collision),
  `svc-{i}` **4533** (collision, still sparse), `pod-{i}` **7708**
  (no collision first — this family reaches the dense flip intact),
  `instance-{i}` **7708**, `10.42.0.{i}` **7708**. At that shared
  `N = 7708` the three families answer **7640**, **7720** and **7700**:
  one `N`, three reference answers. `pod-` and `svc-` were read back
  from the container end-to-end; `instance-` and `10.42.0.` are
  library-only, and the artifact's `observed_by` column records which is
  which per point.

  **The mechanism is observed per family, not fitted to the numbers.**
  Re-deriving each family's sparse keys directly —
  `encodeHash(metro.Hash64(v, 1337), 14, 25)` — at its own divergence
  point: `v{i}` at 5328 has exactly ONE collision, `"v2888"`/`"v5327"`
  on key 52686402; `svc-{i}` at 4533 has exactly ONE,
  `"svc-787"`/`"svc-4532"` on key 36184712; and `pod-{i}`,
  `instance-{i}` and `10.42.0.{i}` at 7708 have **zero** collisions —
  7708 distinct keys each — so their divergence is the sparse-to-dense
  flip alone, with no collision involved. Two families diverge by
  mechanism 1, three by mechanism 2, at counts that share no pattern. The `v{i}` points are captured in
  `crates/pulsus-read/tests/golden/detected_cardinality/reference_divergence.tsv`
  (pinned by `detected_fields_witness.rs`'s AC 19 gate); the
  `/detected_labels` points are in
  `crates/pulsus-read/tests/golden/detected_labels_cardinality/reference_divergence.tsv`,
  pinned by `crates/pulsus-read/tests/detected_labels_cardinality.rs`,
  which also asserts that this entry stays family-scoped and that the
  retracted sentence does not return.
- **Reachability — `/detected_fields`: NOT ESTABLISHED.** The divergence is real and is
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
  case captures a cardinality `<= 100`, and each of those is the
  container's own answer replayed against ours, so every corpus case is
  pure hard-gated parity rather than a divergence. (That last clause
  previously appealed to a universal agreeing range; #261 replaced it
  with the reason that does not need one — the values were captured from
  the container. The rest of this bullet is unchanged from #244 and
  still stands.)
- **Reachability — `/detected_labels`: ROUTINE.** Unlike the
  `/detected_fields` bullet above, this one does not need a bound
  argued: on `/detected_labels` the count is not over a sampled window
  at all. `N` is the number of distinct values a stream-label key has
  across the whole month partition(s) the request's window touches,
  narrowed only by the optional `query=`'s `fingerprint IN` filter
  (`sql::detected_labels`, `crates/pulsus-read/src/logql/sql.rs:163-178`);
  **no request parameter bounds it** — `line_limit` and `limit` do not
  exist on this endpoint, and `start`/`end` select partitions rather
  than rows (the within-month granularity gap is issue #399). The
  count therefore accumulates over a whole month rather than over the
  requested window: no measurement here bounds a real deployment, but a
  namespace whose pods churn passes a few thousand distinct `pod` values
  in a month as a matter of course, so the thresholds above sit inside
  ordinary operation rather than at an extreme.
- **Cost — reference-faithfulness is the MOST expensive option,
  measured.**
  `clickhouse/clickhouse-server:24.8`, one node, `system.query_log`,
  3 reps, 2026-08-08. Corpus A: 3,000,000 rows in ONE month partition of
  the `log_streams_idx` shape = 1,000,000 distinct `pod` values + 50
  `namespace` + 500 `service`. The query is the production text of
  `sql::detected_labels`; `uniq` is the same text with the aggregate
  swapped (**not** reference-compatible — a different estimator
  entirely); "ship distinct `(key,val)`" is the only route that could
  reproduce the reference's own sketch, since ClickHouse has no
  `axiomhq/hyperloglog`-compatible estimator and the values would have
  to be hashed coordinator-side.

  | variant | duration ms (3 reps) | `memory_usage` | `result_rows` | `result_bytes` |
  |---|---|---|---|---|
  | `uniqExact` (what ships) | 120 / 99 / 128 | 125.92 / 129.34 / 123.50 MiB | 3 | 8.77 KiB |
  | `uniq` (approximate, not the reference's) | 69 / 55 / 50 | 8.16 / 8.09 / 8.88 MiB | 3 | 2.94 KiB |
  | ship distinct `(key,val)` (the faithful route) | 134 / 112 / 124 | 221.23 / 210.62 / 218.42 MiB | 1,000,550 | 25.28 MiB |

  Corpus B: 10,000,000 distinct `pod` values, one partition, one row per
  fingerprint. `uniqExact` 1714 / 1801 / 1730 ms at 896.17 / 897.93 /
  893.79 MiB; `uniq` 144 / 209 / 181 ms at 12.56 / 12.12 / 12.65 MiB.

  So exactness costs roughly 2× the time and 15× the ClickHouse-side
  memory of a cheap estimator at 1 M distinct values, and about 12× and
  70× at 10 M — while **reference-faithfulness costs more than either**,
  adding five orders of magnitude to the coordinator fan-in on a path
  whose design point (docs/api.md §2.6.2) is "one row per distinct key
  crosses the network, never one per value". Matching the reference here
  would be both less accurate and more expensive, which is why this
  entry records a divergence rather than a TODO. The fan-in property
  itself is gated, scale-invariantly, by
  `detected_labels_fan_in_is_one_row_per_key_at_any_cardinality`
  (`crates/pulsus-read/tests/query_log_gates.rs`).
- **Memory characteristic — a refusal on this path is a 500, not a 422,
  and that is issue #398's to fix, not this entry's.** LogQL reads set
  `max_bytes_to_read` but no `max_memory_usage`
  (`read_query_settings`, `crates/pulsus-read/src/logql/exec.rs:3225`),
  so a
  ClickHouse code 241 falls through `map_read_error` (`exec.rs:3255`) to
  `ReadError::Clickhouse` and surfaces as **500**, where the
  `QueryTooBroad` family answers 422. It is deliberately NOT fixed under
  #261, because it is not this endpoint's exposure: on the identical
  corpus B above, the SHARED stage-1 stream resolution
  (`sql::stage1`, which `/query_range`, `/series`, `/detected_fields`
  and `/detected_labels` all run) used **3.19 / 3.03 / 2.91 GiB**
  against this endpoint's aggregate at 0.87–0.88 GiB — 3.3-3.6× more. A cap
  scoped to `/detected_labels` would sit on the cheaper half and leave
  the expensive half uncapped. One mechanism, one issue: **#398**.
- **Fixture status:** neither `/detected_fields` nor `/detected_labels`
  has a case in `test/fixtures/logs/differential.json`, so this entry is
  not referenced from the fixture
  (`informational_cases_are_recorded_in_the_committed_ledger`
  guards fixture-referenced entries only); it is registered here so the
  divergence has a ledger identity before any future fixture case lands.

### detected-fields-limit-saturates-not-wraps (issue #253)

- **Construct:** `/detected_fields`' `limit` (legacy alias `field_limit`)
  and `line_limit`, for values above `u32::MAX`. Scoped to those two
  parameters on that one endpoint; nothing else in the API converts a
  parsed limit through an unchecked narrowing cast. The entry id names
  the **field** axis's mechanism, which is what #253 changed; `line_limit`
  is recorded here because it is the same reference cast, but our side of
  it rejects rather than saturating — see the next bullet.
- **Direction: the reference wraps; PulsusDB never does — but the two
  parameters avoid it by different mechanisms, and the entry axis does
  NOT saturate.** The reference's limit helpers parse an `int`, check
  `l <= 0`, and then `return uint32(l)` with no range check —
  `detectedFieldsLimit` (`grafana/loki` v3.7.4 =
  `b318f2829f0ae2094ab3a1e90780450e9e4b03be`,
  `pkg/loghttp/params.go:49-64`) and `lineLimit` (`:38-46`) both. So a
  larger limit can return *fewer* entries or fields. On our side
  (`crates/pulsus-server/src/logs_api/params.rs`):
  - **field-name `limit` / `field_limit` — saturates.**
    `parse_field_limit` ends `u32::try_from(n).unwrap_or(u32::MAX)`, so
    anything above `u32::MAX` runs at `u32::MAX`. There is no ceiling on
    this axis, so a value is always produced.
  - **`line_limit` — rejects.** `parse_line_limit` compares the parsed
    `i64` against `MAX_LIMIT` and returns `LimitTooLarge` *before* any
    `u32` conversion; the `as u32` on its success path is reached only on
    `0 < n <= 5000`. Nothing is clamped or saturated at any magnitude —
    the outcomes are a value in `1..=5000` or a 400. It avoids the wrap
    by refusing, not by pinning to a maximum.
- **Measured** (2026-08-07, `grafana/loki:3.7.4` single-binary, default
  `limits_config` plus `allow_structured_metadata: true` /
  `discover_log_levels: false` / `split_queries_by_interval: 0`; one
  stream of 30 JSON entries carrying 41 distinct field names):

  | `limit` | reference status | reference `fields` | reference `"limit"` echo | PulsusDB effective `field_limit` |
  |---|---|---|---|---|
  | `4294967295` | 200 | 41 | `4294967295` | `u32::MAX` |
  | `4294967296` | 200 | **0** | **key absent** | `u32::MAX` |
  | `4294967297` | 200 | **1** | `1` | `u32::MAX` |
  | `9223372036854775807` | 200 | 41 | `4294967295` | `u32::MAX` |
  | `9223372036854775808` | 400 `strconv.Atoi: … value out of range` | — | — | 400 |

  The reference columns are the container's answers on that fixture; the
  PulsusDB column is the parsed limit the request runs with, which is
  also what the response echoes, and is asserted by the gate below rather
  than transcribed from a run.

  On the entry axis the same cast shows up as an accept-surface
  difference rather than a value one: `line_limit=4294967295` is a 400
  there (`max entries limit per query exceeded … (4294967295 > 5000)`),
  `line_limit=4294967296` a 400 (wrapped to 0, so `limit must be a
  positive value`), but `line_limit=4294967396` is a **200** — it wrapped
  back into the legal range. That it is really *served* at the wrapped
  value, rather than merely accepted, was measured discriminatingly on
  the same fixture using a per-entry-varying field's `cardinality`:
  `line_limit=4294967297` reports cardinality 1, exactly like
  `line_limit=1`, and `line_limit=4294967326` reports 30, exactly like
  `line_limit=30`. PulsusDB answers 400 to every one of them, by the
  rejection described above — not by saturating first and then failing a
  ceiling, which is not what that function does.
- **Why we decline to mirror it** (owner ruling on #253, 2026-08-07):
  "a request for more than four billion fields returning *zero* fields is
  not a value worth matching, it is an unchecked cast wrapping … someone
  who asks for more and receives nothing has been given a wrong answer,
  not a different one." This is the parity mandate's "except where they
  are wrong" case. The **accept surface** on the field axis is identical
  over the whole `i64` domain either way — only the field *count*
  differs, and only on `[2^32, i64::MAX]`. No new refusal is introduced
  anywhere by #253.
- **Not a ceiling.** #253 removed PulsusDB's 5000 cap on the field-name
  axis outright and put nothing in its place: the reference imposes no
  maximum there and, per the measurement recorded on issue #253 over a
  50 000-field sample, does not degrade either (`limit` from 50 000 to
  4 294 967 295 returned an identical body in an identical time). On that
  axis, saturation is the last representable value of the `u32` the
  parameter already had, not a policy limit. (`line_limit`'s 5000 IS a
  limit, but a reference-matching one — see the entry-axis bullets above;
  #253 did not touch it.)
- **Fixture status:** as the entries above — no
  `test/fixtures/logs/differential.json` case, so
  `informational_cases_are_recorded_in_the_committed_ledger` does not
  apply; registered here for identity.
- **Gated by:** `crates/pulsus-server/src/logs_api/params.rs`'s
  `parse_field_limit_saturates_where_the_reference_wraps` (the field
  axis, all four rows above) and
  `parse_line_limit_matches_the_reference_atoi_surface` (the entry axis,
  including the wrap-back-into-range spellings).

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

- **Reference behaviour, RE-MEASURED** on the digest-pinned v3.7.4 oracle
  (`grafana/loki@sha256:87f0a067…`, buildinfo `3.7.4` / `b318f282`,
  `ci/logql/config.yaml`, 2026-08-06). **This bullet used to say that the
  reference accepts both duration literals across the whole `i64`
  nanosecond domain and bounds no query span at all. Two thirds of that
  is wrong**, and it was wrong when written: issue #380 re-measured
  `max_query_length` while establishing something else, and the
  contradiction with this row (and with `docs/features.md`) went
  unnoticed until issue #248's last round. The measurements:

  | probe | verdict |
  |---|---|
  | request span `720h`, `721h` | `200` |
  | request span `721h1s` | `400 the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)` |
  | request span `43800h` (our cap) | `400`, same message |
  | `count_over_time({app="x"}[720h])` over a `1h` request span | `400 … (query length: 721h1m0s, limit: 30d1h)` |
  | `count_over_time({app="x"}[2562047h])` over a `1h` request span | `400 … (query length: 2562047h47m16.854775807s, …)` |
  | `count_over_time({app="x"}[43800h])` as an INSTANT query | never answers: the connection is closed with no HTTP status line at all (38 s and 60 s on two runs). `split_instant_metric_queries_by_interval` defaults to `1h` (`pkg/validation/limits.go:434 @ v3.7.4`) and is reduced only for a SHORTER range vector (`pkg/querier/queryrange/splitters.go:206-215`), so this decomposes into 43,800 subqueries |
  | `offset 2562047h47m16s854ms775us807ns` (`i64::MAX`), instant and range | `200` |
  | `offset -9223372036854775807ns` (`i64::MIN + 1`) | `200`, in any order |
  | `offset -9223372036854775808ns` (`i64::MIN`) on a frontend that has not seen the neighbouring value | `400 this data is no longer available, it is past now - max_query_lookback (0s)`, instant and range alike |
  | the same, after `i64::MIN + 1` has been probed on that frontend | `200` |
  | `offset ±43800h` | `200` |

  **The `i64::MIN` row depends on probe ORDER, and that is the whole of
  the disagreement it caused** (issue #248 round 6: one round measured
  only the `400`, review measured only the `200`, and both reproduce on
  demand). `cache_index_stats_results` defaults to `true`
  (`pkg/querier/queryrange/roundtrip.go:66 @ v3.7.4`) and the two offsets
  differ by one nanosecond — indistinguishable in the
  millisecond-resolution index-stats request the shard resolver issues,
  so whichever runs FIRST decides. Reproducible in either direction on a
  freshly booted container:

  | container | probe order | verdicts |
  |---|---|---|
  | `ci/logql/config.yaml` as shipped | `i64::MIN`, `i64::MIN + 1`, `i64::MIN` | `400`, `200`, `200` |
  | the same plus `query_range.cache_index_stats_results: false` | `i64::MIN + 1`, `i64::MIN`, `i64::MIN + 1`, `i64::MIN` | `200`, `400`, `200`, `400` |

  With the stats cache off, the `400` is the reference's verdict at that
  value in every order; with it on, a warm entry hides it.

  **The rules behind them, from the source.** `max_query_length` defaults
  to `721h` (`pkg/validation/limits.go:371 @ v3.7.4`) and is enforced at
  `pkg/querier/queryrange/limits.go:194-201` and
  `pkg/querier/limits/validation.go:88-91` with the message at
  `pkg/util/validation/validate.go:5`; there is no dedicated `[range]`
  cap in the shipped config, `max_query_range` defaulting to `0s`
  (`limits.go:374-375`, consulted only when nonzero,
  `pkg/logql/engine.go:388-395`). The length is measured over the
  range-selector-adjusted window `[start - ([range] + offset),
  end - offset]` (`pkg/querier/queryrange/shard_resolver.go:94-104`), so
  the `[range]` literal COUNTS against `max_query_length` on a range
  query while the offset CANCELS.

  **The `i64::MIN` `400`, traced end to end** (round 6 — round 5 named
  the right overflow but the wrong caller, and left the caching out).
  The query-frontend's AST mapper decides whether the request is
  shardable from `maxRangeVectorAndOffsetDuration`
  (`pkg/querier/queryrange/split_by_interval.go:262-278 @ v3.7.4`),
  which takes `r.Offset` only when `r.Offset > maxOffset` — a NEGATIVE
  offset therefore contributes `0`, the schema lookup at
  `querysharding.go:167` sees the UNSHIFTED window, finds a valid TSDB
  period and proceeds. The shard resolver then computes the real window
  as `diff := Interval + Offset; from = start.Add(-diff);
  through = end.Add(-Offset)`
  (`pkg/querier/queryrange/shard_resolver.go:94-104`). At `Offset =
  i64::MIN` Go's `-Offset` overflows to `i64::MIN` itself, so `through`
  moves 292 years BACK, while `-diff` does not overflow (`Interval`
  pulls the sum off the rail) and `from` moves 292 years FORWARD. That
  inverted window reaches `IndexStats` (`pkg/querier/querier.go:535`),
  whose `ValidateQueryTimeRangeLimits` takes the `through.Before(from)`
  branch (`pkg/querier/limits/validation.go:92-94`, message at
  `pkg/util/validation/validate.go:7`) and returns `400`; the container
  logs it as `middleware=QueryShard.astMapperware msg="failed mapping
  AST"`. At `i64::MIN + 1` both endpoints move forward together and the
  window stays ordered — a one-value artefact, not a bound.
- **What is left of the divergence, stated exactly.** On the request span
  and on a RANGE query's `[range]`, the reference's default config
  already refuses far more than our cap does (`721h` against our
  `43,800h`), so there our cap can only fire where it also rejects. The
  divergence is real for the `offset` magnitude WITHIN THE BAND THE
  REFERENCE'S LEXER ADMITS — `i64` nanoseconds (a `200` there at every
  value but `i64::MIN`, and a `200` even at `i64::MIN` once the stats
  cache is warm; a `400` here past 5 years) — and for an INSTANT
  query's `[range]`, which the reference admits and then decomposes into
  per-hour subqueries that do not answer in practice.
- **Past that band there is nothing to diverge from** (issue #248 round
  8; the qualifier in the bullet above is that round's — it used to
  quantify over magnitude alone, which counted these rows as
  divergences). A magnitude too large for a Go `time.Duration` never
  becomes a DURATION token, so the reference refuses it at the lexer.
  Re-measured on the same oracle in round 8, in
  `count_over_time({app="x"}[5m] offset <lit>)` (which is where the
  column comes from): `9223372036854775808ns` and
  `18446744073709551615ns` are `400 parse error at line 1, col 38:
  syntax error: unexpected NUMBER, expecting DURATION`, and
  `-9223372036854775809ns` is the same with `unexpected -, expecting
  DURATION`. The neighbouring endpoints bracket them —
  `9223372036854775807ns` and `2562047h47m16s854ms775us807ns` (both
  `i64::MAX`) are `200`s, and one nanosecond more,
  `2562047h47m16s854ms775us808ns`, is the NUMBER refusal again.
  PulsusDB refuses those three out-of-band literals too (`offset too
  long`), so they are reject parity — a different message, the same
  `400 bad_data` class.
  WHICH branch refuses each is not observable from the wire (the lexer
  discards `parseDuration`'s error), and it is two branches over the
  three rows — none of them one of `model.ParseDuration`'s, whose unit
  map has no `ns`, so an `ns` literal leaves it at the unit lookup. The
  source-level account, with the two discriminating probes that separate
  Go's `leadingInt` overflow from its trailing positive-only range
  check, is at `crates/pulsus-logql/src/parser.rs`'s
  `both_duration_literals_cap_at_five_years_and_refuse_rather_than_clamp`.
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
  `group_key`'s `By` arm builds one owned pair per `by` name PRESENT on
  the group's series (since issue #241 an ABSENT name contributes
  nothing — the key is selected from the series' own labels,
  reference-exact); the funnel charges that as `W_GROUPNAME x series x
  group_name_bytes` against `MAX_POST_AGG_BYTES` (8 GiB) and refuses
  above it with `MetricPostAggBytes` (HTTP 422). `group_name_bytes` is
  read off the QUERY TEXT and so counts every name, absent ones included
  — which names the data carries is unknowable before the stage runs —
  so since #241 the charge OVER-estimates what is allocated, and the
  thresholds below, being properties of the charge, are unmoved by that
  fix. **Threshold, as a
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
  divergence was tracked separately in **#264** and is now **closed** —
  every LogQL error is the reference's bare `text/plain` body, so the
  container matches too and only the prose still differs; the WebSocket
  close frame still truncates reasons at 123 bytes.)
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
  (131072 > 131072)`, and — since #264 closed — in the same bare
  `text/plain` container) agrees whenever the query arrives by POST
  instead.
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

### inadmissible-label-name-status (issue #259)

- **Status: REGISTERED EXCEPTION — a deliberate divergence we decline to
  mirror.** Adjudicated on issue #259; this row is the ledger half the
  adjudication asked for, alongside the docs/api.md §8.2 note.
- **Construct:** the response STATUS for a structured-metadata name (or an
  OTLP attribute key) the reference refuses outright — an empty name, or one
  that sanitizes to nothing but underscores
  (`otlptranslator.LabelNamer.Build`,
  `vendor/github.com/prometheus/otlptranslator/label_namer.go:66-90 @
  v3.7.4`).
- **Reference behaviour, measured on `grafana/loki:3.7.4`:** it disagrees
  with ITSELF. `POST /loki/api/v1/push` answers **500** (both encodings),
  `POST /otlp/v1/logs` answers **400**, for the identical condition.
- **Why the 500 is accidental rather than designed.** The push error escapes
  the distributor's validation closure as a bare `errors.New` carrying no
  gRPC status (`pkg/distributor/distributor.go:703-706 @ v3.7.4`), so
  `httpgrpc.HTTPResponseFromError` fails and `pushHandler` falls into its
  status-less `else` branch (`pkg/distributor/http.go:170-180`) — while
  every sibling failure in that same loop is client-classified `400` through
  `validationErrors`. Provenance agrees: the `if err != nil { return err }`
  arrived in Loki commit `09d831ea85`, *"chore: upgrade Prometheus to
  208187eaa19b (#18756)"*, a mechanical bump adding the minimum needed to
  compile once `Build` grew an `error` return. Before it, the same input
  produced no error at all.
- **PulsusDB behaviour:** **400 on both transports.** The input is entirely
  client-controlled and no retry can succeed, while `5xx` is precisely the
  class log agents retry — a 500 makes a well-behaved agent retry an
  unfixable body forever and books it against server-error SLOs. It is also
  what the reference itself answers on one of its own two transports.
- **What IS byte-identical.** The push response body, terminator included:
  the reference writes every push error through `push.HTTPError` ->
  `http.Error` -> `fmt.Fprintln` (`pkg/loghttp/push/push.go:606-608 @
  v3.7.4`), so its body is `label name is empty\n` — the same 20 bytes
  PulsusDB writes. On OTLP the reference wraps the same text in
  `symbolizer lookup: ` (`pkg/loghttp/push/otlp.go:613 @ v3.7.4`) and
  PulsusDB reproduces that prefix, so the `google.rpc.Status` MESSAGE is
  byte-identical.
- **Related, pre-existing, not owned here:** the enclosing
  `google.rpc.Status` differs in one field — the reference deliberately
  omits `code` (`grpcstatus.New(0, errorStr)`, `push.go:571-582 @ v3.7.4`,
  "Status 0 because we omit the Status.code field") while every PulsusDB
  OTLP receiver sets `code = 3` for a 400-class failure, a receiver-wide
  contract from issue #8 that predates this issue and applies to all four
  signals.
- **Fixture status:** no `test/fixtures/logs/differential.json` case (the
  differential corpus carries no inadmissible name, and its oracle is still
  `grafana/loki:3.4.2`, which predates `Build` returning an error at all).
  Gated hermetically by `ingest/http.rs`'s
  `loki_inadmissible_structured_metadata_name_is_400_on_both_encodings`,
  `otlp_inadmissible_attribute_key_returns_400_with_status_code_3` and
  `every_loki_push_error_body_is_lf_terminated`, and live by
  `loki_push_live.rs`'s
  `inadmissible_structured_metadata_names_are_refused_and_nothing_is_stored`.

### inadmissible-label-name-echo-escaping (issue #259)

- **Status: REGISTERED EXCEPTION — a USER-VISIBLE runtime-rendering
  difference we do not chase.** Verdict, status and sentence all match; only
  the echoed name's escaping differs. A client CAN reach it: see the
  reachability note below, which corrects an earlier version of this row
  that argued the difference was unreachable.
- **Construct:** the `%q`-quoted name inside
  `normalization for label name %q resulted in invalid name %q`.
- **Reference behaviour:** Go's `fmt` `%q` (`strconv.Quote`), whose
  printability table is Go's own.
- **PulsusDB behaviour:** Rust's `{:?}` (`char::escape_debug`), whose
  printability table is Rust's own.
- **The exact, bounded difference.** Comparing the two renderings over all
  1 112 064 codepoints: they agree on every assigned, printable,
  non-combining character. Every disagreement is a control character
  (`"\x01"` vs `"\u{1}"`, `"\x00"` vs `"\0"`), a format/separator
  character (`"\u00a0"` vs `"\u{a0}"`), an unassigned or private-use
  codepoint, or a `Grapheme_Extend` mark (Go prints U+0301 raw, Rust
  escapes it). The only exceptions carrying a letter/number/punctuation/
  symbol category at all are U+FF9E and U+FF9F, themselves
  `Grapheme_Extend`. On the 80-name matrix the whole rule was replayed
  over, accept/reject agrees **80/80** and the message agrees **71/80**.
- **Reachability — a client CAN hit this, and sees different bytes.** A lone
  combining mark is exactly the sort of name this rule refuses, so it is a
  reachable input rather than a theoretical one. Measured on
  `grafana/loki:3.7.4` (`b318f282`), pushing structured metadata keyed
  `U+0301` on both push encodings and as an OTLP attribute:

  | side | response body |
  |---|---|
  | reference | `normalization for label name "<CC 81>" resulted in invalid name "_"` — the raw two UTF-8 bytes inside the quotes |
  | PulsusDB | `normalization for label name "\u{301}" resulted in invalid name "_"` — eight ASCII bytes |

  What is bounded is the CLASS of characters that differ, never the
  reachability. An earlier version of this row said the difference was one
  "no consumer can act on"; that was wrong, and this row says so instead of
  re-deriving a narrower bound.
- **Why we do not mirror it:** the two tables track different Unicode
  versions and move with each runtime's releases, so byte agreement here is
  not a stable property either side can hold — Go's `%q` printability is
  `strconv.IsPrint`'s generated table (categories L/M/N/P/S plus ASCII
  space), Rust's `{:?}` additionally escapes `Grapheme_Extend`, and neither
  is reachable from the other's standard library. Reproducing Go's would
  mean vendoring a Unicode category table and pinning it to the Go release
  that built the image. The impact is bounded to the echoed name: the name
  is refused either way, with the same condition and the same sentence, and
  no PulsusDB response status changes.
- **Fixture status:** pinned by `label_name.rs`'s
  `the_escape_syntax_for_an_unprintable_name_is_our_runtimes_not_the_references`,
  which carries the reference's measured rendering beside ours for every
  member of the known set.

### `malformed-query-refused-in-every-window` (issue #380, owner ruling 2026-08-06 — deliberate, the reference is wrong here)

- **Reference behaviour, measured** on the digest-pinned v3.7.4 oracle
  (`grafana/loki@sha256:87f0a067…`, buildinfo `3.7.4` / `b318f282`,
  `ci/logql/config.yaml`). The same malformed query,
  `{app="checkout"} | addr = ip("nope")`, over different windows:

  | window | status |
  |---|---|
  | 1 h ending now | `400` `parse error : stage '\| addr=ip("nope")' : ip: invalid pattern: "nope"` |
  | 1 h ending 2 h ago | `400` |
  | 1 h ending 3 h ago | `200`, empty |
  | 1 h ending ten years ago | `200`, empty |
  | `start=0&end=1` | `200`, empty |
  | 1 h starting a day from now | `400` |

- **The boundary is `query_ingesters_within` (3 h by default), NOT
  `max_query_lookback`.** The lookback middleware
  (`pkg/querier/queryrange/limits.go:167-181 @ v3.7.4`) was the first
  explanation offered for this — on issue #380, and in a comment on
  `logql_nested_ip_matrix.rs` — and it is wrong: the pinned container
  reports `max_query_lookback: 0s` on `/config`, which is the shipped
  default (`pkg/validation/limits.go:379 @ v3.7.4`), so that branch never
  runs. Bisecting the window by hour puts the transition exactly at 3 h,
  the default `query_ingesters_within`.

- **The rule, from the source.** When a query's END precedes
  `now - query_ingesters_within`, the ingester is not consulted at all
  and only the store is —
  `BuildQueryIntervalsWithLookback` returns a nil ingester interval
  (`pkg/querier/intervals.go:32-38 @ v3.7.4`, via
  `isWithinIngesterMaxLookbackPeriod`, `pkg/querier/querier.go:277-287`).
  The store's `SelectLogs` then returns `iter.NoopEntryIterator` as soon
  as no chunk matches (`pkg/storage/store.go:491 @ v3.7.4`) — **before**
  `expr.Pipeline()` at `:497`. The same 3 h boundary was measured
  independently on issue #352, from the other side: pushes older than it
  succeed and then answer nothing (`logqltest_replay.rs`'s
  `INGESTION_WINDOW` table). The malformed `ip()` pattern is raised
  when the pipeline is BUILT (`log.NewIPLabelFilter` records the error
  and leaves the matcher nil, `pkg/logql/log/ip.go:94-103`;
  `PatternError()`'s only caller is `LabelFilterExpr.Stage()`,
  `pkg/logql/syntax/ast.go:801-809`), so it is never raised and the query
  answers 200-empty.

- **The class is pipeline-BUILD errors, not parse errors** — measured,
  and it corrects issue #380's body, which said this applies to "every
  parse-time-invalid construct". Each query below over a 1 h window
  ending now vs a 1 h window ending 4 h ago:

  | query | recent | 4 h old |
  |---|---|---|
  | `{app="checkout"} \| addr = ip("nope")` | `400` | `200` |
  | `{app="checkout"} \| line_format "{{"` | `400` | `200` |
  | `{app="checkout"} \| json \| label_format a=b,a=c` | `400` | `200` |
  | `{app="checkout"} \|\|\|` | `400` | `400` |
  | `{app=` | `400` | `400` |
  | `count_over_time({app="x"}[5m]) +` | `400` | `400` |
  | `{app="checkout"} \| unwrap foo \| __error__=` \`x\` | `400` | `400` |
  | `{app="checkout"} \| logfmt a="b.c"` | `400` | `200` |
  | `{app="checkout"} \| logfmt a="b",` | `400` | `400` |

  A syntax error is refused in both windows; an error the parser accepts
  and the pipeline builder rejects is refused only while the ingester is
  in the query's path.

  The last two rows (issue #247, measured 2026-08-07 on the same pinned
  image) are the same construct split across both classes, so they show
  the boundary rather than merely instancing one side of it. A malformed
  extraction EXPRESSION is refused by the logfmt sub-grammar at
  `Stage()`, which is pipeline-build — window-dependent. A dangling comma
  in the extraction LIST is a `syntax.y` production error refused by
  `ParseExpr` — window-independent. Both are `400` in every window here.

- **The window is not the only way the reference hides one of these — a
  `variants(...)` common pipeline hides it in EVERY window** (issue #247,
  measured 2026-08-07 on the same pinned image, window ending now):

  | query | reference | PulsusDB |
  |---|---|---|
  | `variants(count_over_time({app="x"} [5m])) of ({app="x"} \| logfmt a="b.c" [5m])` | `200`, empty | `400` |
  | `variants(count_over_time({app="x"} \| logfmt a="b.c" [5m])) of ({app="x"} [5m])` | `400` | `400` (closed) |

  The first row is **not** the reference serving the query. Read from the
  container's own log during that request, the ingester answers `rpc
  error: code = Code(400) desc = error extracting common pipeline: parse
  error : stage '| logfmt a="b.c"' : cannot parse expression [b.c]:
  unexpected char .` — the build fails exactly as it does everywhere
  else, and the querier swallows it, handing the user an empty `200` with
  nothing to say the query was broken. The control is the same query with
  a well-formed expression, which returns a series. So this is the same
  divergence class as the rest of this entry, in a stronger form: the
  reference hides it regardless of the window, and the owner's ruling
  below applies unchanged.

  The second row is **closed** (issue #247 round 2) and is kept here to
  show the contrast. The reference builds each variant's extractor
  (`evaluator.go:1417 @ v3.7.4`) purely to count it (`for range
  extractors`, `:1422`) while the real extraction passes `nil` variant
  stages (`extractor.go:186`, `:225`) — it validates syntax it then
  ignores. PulsusDB now does the same: `build_variants_node` compiles a
  variant's own pipeline and discards the result, so `| logfmt a="b.c"`,
  `| line_format "{{"`, a malformed `ip()` pattern and an uncompilable
  regex in a variant are all refused here as they are there. Unlike every
  other row in this entry that rejection is **window-independent on both
  sides**, because the reference raises it in the querier's
  `newVariantsEvaluator` rather than behind the store's stale-window
  short circuit — measured: no variant-side point moves when the window
  is aged past `query_ingesters_within`. Both rows are pinned point by
  point in `crates/pulsus-read/tests/logql_logfmt_expr_matrix.rs`.

- **PulsusDB behaviour (the delta): a malformed query is a `400` in every
  window.** Nothing about our rejection depends on the dates asked for:
  `plan()` and `CompiledPipeline::compile` both run before any I/O
  (`logql/exec.rs:612`, `:906`, `:2290`, `:2576`, `logql/variants.rs:509`,
  propagated with `?` and surfaced by `logs_api/error.rs` as a 400), so an
  invalid pipeline cannot reach a "no chunks, return empty" path in the
  first place.

- **Why we do not copy it** (owner ruling, 2026-08-06). Answering `200`
  with no results for a query that has a typo in it is wrong: the user
  reads an empty result as "no logs matched", not "your query is
  broken", and the same query gets both answers depending on which dates
  they picked. `{app="checkout"} | addr = ip("nope")` is malformed
  whatever window you ask for.

- **Reachability — the five-year span cap does not narrow this** (issue
  #380 option 3, measured). The divergence needs a window whose END is
  more than `query_ingesters_within` in the past; it puts no lower bound
  on the SPAN, and the natural cases are tiny (1 h ending 4 h ago, 1 h
  ten years ago, `start=0&end=1` — all far under 5 years), so
  `five-year-span-cap` fires on none of them. In the other direction, a
  span big enough to trip our cap is already refused by the reference on
  its own `max_query_length` (default 721 h = `30d1h`): measured,
  `start=0` to ten-years-ago is a `400 the query time range exceeds the
  limit (query length: 408519h40m31s, limit: 30d1h)` there. So the SPAN
  half of that cap only ever fires where the reference also rejects (its
  `offset` half does not — see `five-year-span-cap`), and the divergence
  surface is exactly what this entry describes.

- Gated by `b20_nested_ip.test`'s
  `# provenance: divergence(malformed-query-refused-in-every-window)`
  row, which pins that the lone malformed `ip()` filter is refused by the
  value evaluator with no window in the picture at all.

- **Re-derivation.** `podman run -d -p 13380:3100 -v
  ci/logql/config.yaml:/etc/loki/local-config.yaml:ro
  grafana/loki@sha256:87f0a067…`, wait for `/ready`, then `curl -sS -G
  --data-urlencode 'query={app="checkout"} | addr = ip("nope")'
  --data-urlencode start=… --data-urlencode end=…
  http://localhost:13380/loki/api/v1/query_range` with the windows in the
  first table. `curl -s http://localhost:13380/config | grep
  max_query_lookback` shows the `0s` that rules out the lookback
  middleware.

## Issue #374 — the per-stream label rules at log ingest

### `ingest-label-bounds` (issue #374 — parity ADOPTED; residuals named below)

PulsusDB previously bounded a log push only by counts (streams per
request, entries per stream, entries per request) and by decode-time
materialization caps. Nothing measured a label name, a label value, the
number of labels in a stream, or a repeated name — so a push the
reference refuses with `400` was accepted and stored. Issue #374 adopts
the reference's rules; this entry records what now matches and, under
**Residual N** headings, every place where the observable still differs.
Neither the title nor the prose restates how many there are: the sentence
that did said "three named residuals" while the list below it named five. Same rule for the case counts — they live on `compare.py`'s own
summary line in the transcript, which is generated, and are not copied
back up here.

- **Reference behaviour, measured** on the digest-pinned v3.7.4 oracle
  (`grafana/loki@sha256:87f0a067…`, buildinfo `3.7.4` / `b318f282`),
  `pkg/distributor/validator.go:157-199 @ v3.7.4` reached from
  `pkg/distributor/distributor.go:1370-1387 @ v3.7.4`. In order: at most
  15 label names (`entry for stream '%s' has %d label names; limit %d`),
  names at most 1024 bytes (`stream '%s' has label name too long: '%s'`),
  values at most 2048 bytes (`stream '%s' has label value too long:
  '%s'`), no repeated name (`stream '%s' has duplicate label name:
  '%s'`). All four are `400`. Limits are the flag defaults at
  `pkg/validation/limits.go:324-326 @ v3.7.4`; messages are
  `pkg/validation/validate.go:58-69 @ v3.7.4`.
- **What PulsusDB now does: the same rules, in the same order, at the
  same limits, on BOTH log receivers** —
  `crates/pulsus-write/src/protocols/log_label_limits.rs`, called from
  `loki_push::parse_protobuf`/`parse_json` and from `otlp_logs::parse`.
  The reference validates its OTLP logs endpoint through the identical
  distributor seam (`pkg/distributor/http.go:28-33 @ v3.7.4`), so both
  paths are in scope, not just the Loki-push one — but not on the same
  data, see "the OTLP subset" below. Four inherited rules come with the
  bounds, each load-bearing:
  - an empty-valued label is **dropped before the stream is hashed**, not
    merely skipped by the validator (`syntax.ParseLabels` ends in
    `ls.WithoutEmpty()`, `pkg/logql/syntax/parser.go:279-296 @ v3.7.4`,
    whose comment gives the reason: empty values "alter the Hash values
    created"). `{a="1", ignored=""}` is therefore one stream with one
    fingerprint on both sides. `StreamLabels` is the single seam that
    applies it, and the value it validates is the value handed to
    `LabelSet::from_normalized` — so the bounds are charged on the
    stripped set, never on the raw pair list. The rule that applies to a
    *stream label* drops the empty **pair**; the rule that applies to
    *structured metadata* deletes **by name** (a `labels.Builder` reset,
    `distributor.go:698-722 @ v3.7.4`, issue #259). The two disagree on
    exactly one input, a repeated name with one empty copy: the pair rule
    keeps the surviving twin, delete-by-name would lose it too. Measured
    on the oracle in both orders — `{d="", d="keep"}` and `{d="keep",
    d=""}` are `204` and store `d="keep"`, while `{d="one", d="two"}` is
    `400`;
  - a stream carrying `__aggregated_metric__` or `__pattern__` is exempt
    from all four bounds (`validator.go:164-167 @ v3.7.4`). PulsusDB
    generates no such stream, but both are ordinary client-settable label
    names on both sides, so the exemption is part of the accept surface;
  - `service_name` does not count toward the 15
    (`validator.go:169-174`), making the effective rule "at most 15
    labels other than `service_name`";
  - a stream with no entries is skipped before validation
    (`distributor.go:639-641`).
- **The OTLP subset, selected on the RAW attribute name.** On its OTLP
  logs endpoint the reference splits a resource's attributes in two before
  the distributor ever sees them
  (`pkg/loghttp/push/otlp.go:180-212 @ v3.7.4`): the 18 names in
  `distributor.otlp.default_resource_attributes_as_index_labels`
  (`pkg/loghttp/push/otlp_config.go:56-73 @ v3.7.4`) become stream labels,
  and every other attribute becomes structured metadata, which
  `ValidateLabels` never sees. PulsusDB indexes them all as stream labels
  (issue #109). Charging the four bounds on our whole set is therefore
  NOT parity — measured on the pinned oracle, a resource carrying `app`
  with a 2049-byte value, a 1025-byte attribute key, or 16 arbitrary
  attributes all answer `204` there (the attribute is stored as
  structured metadata and fans into the query response). The bounds are
  charged on the indexed subset instead, which reproduces the oracle's
  answer in both directions: `k8s.pod.name` with a 2049-byte value is
  `400` on both, 16 indexed attributes is `400` on both, 15 indexed plus
  40 arbitrary ones is accepted by both.

  The subset is selected from the **raw wire key**, not from the
  canonicalized label name, and the difference is observable.
  `otlp.go:193 @ v3.7.4` calls `otlpConfig.ActionForResourceAttribute(k)`
  on `k` and only then calls `attributeToLabels(k, …)`, which
  canonicalizes through `otlptranslator.LabelNamer.Build`
  (`otlp.go:610-614 @ v3.7.4`); the match inside `actionForAttribute` is
  `cfgAttr == attribute`, exact string equality (`otlp_config.go:88-99 @
  v3.7.4`). An attribute whose raw name merely *canonicalizes into* the
  list — `service_name`, `service-name`, `k8s_pod_name`, anything of the
  form `service?name` for a non-alphanumeric `?` — is structured metadata
  upstream and is bounded by nothing. Measured: `{service_name:
  "x"*2049}` is `204` on the oracle where `{service.name: "x"*2049}` is
  `400`. Both directions are now measured for **all 18** names, generated
  from the reference's list rather than sampled.

  The consequence is that a non-indexed OTLP resource attribute is
  bounded by nothing here and by the structured-metadata limits (64 kB
  and 128 entries per line, `pkg/validation/limits.go:60-61 @ v3.7.4`)
  there; mapping those onto data we store as stream labels belongs with
  the #109 attribute-placement decision, not here.

  **This reaches a validated label too, and that half is about storage
  rather than about the accept surface.** `{service.name: "ok",
  service_name: "x"*2049}` is accepted by the oracle (`204`) and by
  PulsusDB (`200`) — the rejection surface agrees, because upstream
  indexes the dotted spelling and routes the underscored one to
  structured metadata. But we index both (#109), both canonicalize onto
  `service_name`, and `from_normalized`'s frozen collision rule (#4)
  keeps the greatest *original* key, where `_` (0x5F) sorts after `.`
  (0x2E). So the unvalidated near-miss wins: 2049 bytes are stored under
  a label the validator passed at two, and the stream's fingerprint
  follows the winner. This is not a divergence introduced here — it is
  what indexing every attribute means once a bound exists on a subset of
  them — but it does mean a reader of the four bounds cannot conclude
  that a *stored* label obeys them. The module doc of
  `crates/pulsus-write/src/protocols/log_label_limits.rs` states that as
  a limit of what the bound covers, and the fix (indexing what the
  reference indexes) is #109's. Pinned on stored state rather than on the
  wire verdict by
  `otlp_logs::tests::an_index_attribute_and_its_near_miss_collide_on_the_unvalidated_value`
  and
  `loki_push_live::an_otlp_near_miss_spelling_stores_an_over_wide_indexed_label`;
  the harness's four `otlp/index-key-vs-near-miss/*` cases measure only
  the wire half, and say so.
- **A breach is stream-local.** The reference validates every stream,
  `continue`s past the ones that fail, writes the ones that pass, and
  only then answers `400` (`distributor.go:645-655, 780-790, 929 @
  v3.7.4`). PulsusDB now does the same: `ParsedLogs::stream_errors`
  carries the failing streams' messages, the rest of the batch is
  admitted, and the response is `400` — plain text on the Loki path, a
  `google.rpc.Status` with `code = 3` on the OTLP path (upstream's
  `push.OTLPError` writer does the same). When no stream survives,
  nothing is written and nothing is admitted, matching
  `distributor.go:786-789`. This is not a cosmetic difference: a `400` is
  not retried by a well-behaved log shipper, so refusing the batch
  atomically would destroy the good streams permanently.
- **A push with no streams is `422`, before any of this runs** (found by
  the round-5 review, and **pre-existing** — measured identically on the
  branch point `5969a94`, so neither the empty-value drop nor the bounds
  made it reachable). `PushWithResolver` returns
  `httpgrpc.Errorf(StatusUnprocessableEntity,
  validation.MissingStreamsErrorMsg)` for `len(req.Streams) == 0`
  (`distributor.go:579-581 @ v3.7.4`; message at
  `pkg/validation/validate.go:15`), which on the OTLP receiver covers any
  payload with no log records at all — its translation short-circuits to an
  empty push request when `ld.LogRecordCount() == 0`
  (`pkg/loghttp/push/otlp.go:144-146 @ v3.7.4`). PulsusDB answered `204` /
  `200` and stored nothing; it now answers `422` with the reference's
  message on both receivers. On the JSON push transport the count is what
  decides, not the shape of the object: `{"streams":[]}`, `{}` and a body
  whose only key is an unrelated one (`{"nope":1}` — the spelling issue
  #259 measured) are one case, `422` on both sides, because both decoders
  ignore an unknown field rather than refusing it. The count is on the streams the request
  *carries*, so the two neighbouring shapes stay accepted: a stream with
  labels and no entries is still a stream (`204` on both), and a stream
  whose labels breach a bound is a `400`, not a `422`. Charged at
  `loki_push::validate_bounds` (the one seam both Loki-push encodings
  reach) and in `otlp_logs::parse` — after decode, before any per-record
  work and before the per-stream bounds, but *after* the `AnyValue` depth
  cap, which is residual 6 below. The `422` body carries no PulsusDB prefix, exactly like the four
  bound messages; on the OTLP path it rides the same `google.rpc.Status`
  with `code = 3` — upstream's `OTLPError` omits the code field on *every*
  error body it writes (`grpcstatus.New(0, errorStr)`,
  `pkg/loghttp/push/push.go:571-581 @ v3.7.4`), a pre-existing difference
  across our whole OTLP error family rather than one of this status.
- **How the JSON envelope's own keys are matched**, which is what
  decides whether the `422` above is reached at all. `loghttp.PushRequest`
  is a one-field struct (`pkg/loghttp/query.go:91-93 @ v3.7.4`) decoded by
  `jsoniter.NewDecoder`, i.e. under `ConfigDefault`, whose `CaseSensitive`
  is false. So the reference matches `streams` with **ASCII case folding**
  — the wire key is folded over `'A'..'Z'` before it is compared, and the
  tag gets a `strings.ToLower` alias
  (`iter_object.go:49-90`, `reflect_struct_decoder.go:36-41 @ jsoniter
  v1.1.12`, vendored in the Loki tree) — and a **repeat of the key is
  last-wins**, because the field decoder re-runs on every match and the
  slice decoder re-grows from index zero
  (`reflect_struct_decoder.go:574-590`, `reflect_slice.go:66-99`); a
  `null` overwrites with nil, so it empties the request exactly as `[]`
  does. Measured on the pinned oracle: `Streams`, `STREAMS`, `StReAmS`,
  `streamS` and an escaped `"\u0053treams"` are all `204` **and the lines
  read back out of `/loki/api/v1/query_range`**; of two spellings only the
  last one's line is stored.

  PulsusDB matched the key byte-for-byte, which was invisible while it
  answered `204` (the case variant was an unknown key, so the push was
  empty and silently dropped) and became a `422` once the stream-less
  check above landed. It now folds the same way and is last-wins, so both
  the divergence and the silent drop that preceded it are closed. Note
  which layer the rule lives on: a stream object's own `stream`/`values`
  keys are decoded by a hand-written `LogProtoStream.UnmarshalJSON` that
  switches on `string(key)` (`query.go:99-121 @ v3.7.4`), so those stay
  case-**sensitive** on both sides (measured: `Stream`/`Values` are `204`
  storing nothing) — while a repeat of them is last-wins there too, except
  that a `null` `values` returns before assigning and leaves the entries
  alone (`query.go:110-112`). The same question over the other ingest
  envelope has no gap: OTLP/JSON is decoded by pdata's generated
  field switch, exact match plus the proto3 snake_case alias, and all
  seven spellings of `resourceLogs` measured identically on both sides.
  Harness: the `json/streams-key-*`, `json/stream-object-keys-*` and
  `json/values-null-*` cases; storage, which a status cannot see, is
  `loki_push_live::a_case_variant_streams_key_is_accepted_and_its_lines_are_stored`.
- **Where last-wins puts our own structural caps.** Because the reference
  never inspects a superseded value, a cap-breaking occurrence followed by
  a valid one is `204` there, with the valid one's line stored. PulsusDB's
  decode-time caps used to be charged where they trip, so those bodies were
  `400` here. They are now charged **after** the envelope resolves: the
  `streams`, `values` and `structured_metadata` visitors stop RETAINING
  one element past their cap and parse the rest without keeping it, and the
  `MAX + 1` sentinel they leave behind is read by `validate_bounds` /
  `canonical_structured_metadata`; the JSON `stream` map carries its raw
  pair count out to `parse_json`; and `MAX_LABELS_PER_STREAM` is counted on
  the labels that survive rather than on raw pairs — 257 repetitions of one
  key are ONE label upstream, because `LabelSet.UnmarshalJSON` assigns into
  a `map[string]string` and has no count bound of its own
  (`pkg/loghttp/labels.go:25-40 @ v3.7.4`). The protobuf transport needs
  none of this: every cap there is on a repeated (merged) field, and its
  two singular fields (`labels`, `line`) carry no decode-time cap.
  Enumerated rather than sampled — three resolution points (`streams`,
  `stream`, `values`) crossed with every per-occurrence cap reachable
  inside each, as the `json/superseded-*` harness cases; storage, which a
  status cannot see, is
  `loki_push_live::a_superseded_over_cap_value_is_accepted_and_the_final_one_is_stored`.
  The two SHARED cross-request counters are the exception, residual 7.
  **What "discarded" does not mean: unread.** The remainder past a cap is
  parsed in full — element types, object structure and nesting depth are
  checked exactly as they are on the retained side of the cap, message text
  included — and only the retention stops. The first cut of this deferral
  drained with `serde::de::IgnoredAny`, which `serde_json` implements as a
  bracket-matching skip (`Deserializer::ignore_value`, `de.rs:1102 @
  1.0.150`) that types nothing, so crossing a cap silently switched the
  checking off: 257 structured-metadata pairs followed by `"bad":[]`, and
  100,001 entries followed by a bare `0`, each superseded by a valid
  occurrence, were `400` upstream and `204` here — while the SAME tails one
  element below the cap were `400` on both sides. Upstream does not skip a
  superseded value either: jsoniter decodes every occurrence of a repeated
  field in full before the last one wins
  (`reflect_struct_decoder.go:574-590 @ jsoniter v1.1.12`). Pinned as
  triples (below the cap / past it / past it and superseded, all three
  producing the same message) by
  `loki_push::tests::parse_json_a_drained_value_is_checked_exactly_like_a_retained_one`,
  and on the wire by the `json/drained-*` harness cases.
  **The same applies to a value under a key neither side reads**, and there
  the checks are the reference's SKIP's rather than a type's: nesting depth
  (residual 8) and out-of-range numbers. The number rule begins at `Skip`'s
  DISPATCH, which picks the reader from the value's FIRST BYTE and nothing
  else: `case '0'` calls `ReadFloat32` and `case '-', '1'..'9'` calls
  `skipNumber` (`iter_skip.go:72-96`, `:83-87 @ jsoniter v1.1.12`). So a
  token starting with `0` is range-checked against **`f32`** — `0.35e39` is
  `400` on both sides, while `-0.35e39` and `3.5e38`, the same magnitude and
  equally out of `f32` range, are `204` on both because their first byte
  routes them elsewhere. On that other route, upstream's `trySkipNumber` walks
  a run of digits with at most one dot and skips it unevaluated; everything
  else — an exponent, in practice — leaves that fast path and is PARSED by
  `ParseFloat`, which fails on OVERFLOW of **`f64`**
  (`iter_skip_strict.go:10-21,24-59`,
  `iter_float.go:299-315`), so an ignored `1e999` is `400`
  there. Round 12 matched that by
  accident, because `serde` evaluated every number it walked; round 14, which
  reads such a value as raw text instead, applies the rule explicitly rather
  than inheriting it — the two are told apart by `json/ignored-number-int`
  against `json/ignored-number-overflow`, and by
  `loki_push::tests::parse_json_a_discarded_number_follows_the_references_overflow_rule`,
  whose rows are measured in each of the three positions. That is the
  EXPONENT axis and it agrees exactly, invariantly under wire framing and
  byte offset; the `f32` route, closed in round 18, agrees the same way and
  for a stronger reason — it never enters the read-buffer-bounded fast path
  at all. The LENGTH of a digits-only run does not agree and is not
  matched: **residual 10**.
- **Multi-failure bodies** are grouped as `util.GroupedErrors.Error()`
  groups them (`pkg/util/errors.go:105-131 @ v3.7.4`): identical messages
  collapsed with an `N errors like: ` prefix, distinct groups joined with
  `"; "`, a lone failure rendered bare. Group ORDER is a Go map walk
  upstream and therefore deliberately randomized; ours is first-seen.
  There is no byte-reproducible upstream body to match for more than one
  distinct failure, only the format: repeated batches of 40 sends of one such
  request return BOTH orderings, one dominating and the other a minority of
  the batch — and a batch showing only one order has been observed too. Only
  "both orders occur" is a measurement; no split is quoted here, because a
  split is a fresh sample every time, and quoting one as though it were a
  figure is what left this row's artifacts disagreeing over two review rounds
  (rounds 15 and 16, each `[low]`). The transcript sorts the groups for exactly that
  reason, in `compare.py`'s `trim`, so that its stated reproduce recipe
  ("diff empty") holds; the sort is applied to both sides and cannot reach a
  verdict, which is a status.
- **The `400` body is LF-terminated on the Loki push path**, and only
  there. The reference binds one error writer per push endpoint —
  `push.HTTPError` for `/loki/api/v1/push`, `push.OTLPError` for the OTLP
  one (`pkg/distributor/http.go:27-33 @ v3.7.4`) — and a label-bound breach
  leaves `PushWithResolver` as an `httpgrpc` error handed to that same
  writer (`http.go:161-171`), i.e. to `http.Error` -> `fmt.Fprintln`
  (`pkg/loghttp/push/push.go:606-608 @ v3.7.4`). Measured on the pinned
  oracle: a 2049-byte label value answers `400` whose last body byte is
  `0x0a`. Merged in from issue #259, which established the same terminator
  for this endpoint's decode-failure bodies; the stream-local `400` this
  row adds is written through the same seam and had to follow. The OTLP
  body is a `google.rpc.Status` protobuf and carries no terminator. In the
  transcript below, every case where BOTH sides answer `400` now agrees on
  it; the six where exactly one side terminates are the six where exactly
  one side answers `400` at all, i.e. the recorded status divergences.
  Gated by `ingest/http.rs`'s `every_loki_push_error_body_is_lf_terminated`,
  which #259 wrote as an enumeration over the endpoint's error classes and
  which this row extends with its three: the stream-less `422` and the
  stream-local `400` in both its forms (nothing written, and
  written-then-refused).
- **Residual 1 — the rendered label set inside the message.** Every one of
  the four messages interpolates the stream's label set, and the
  reference's copy carries a `service_name` label its own push parser
  injected (`discover_service_name`, `pkg/loghttp/push/push.go:441-453 @
  v3.7.4`, on by default — e.g. `service_name="unknown_service"`, or the
  value of an `app` label). PulsusDB does not implement service-name
  discovery, so its rendered set omits that label. This is a pre-existing
  difference in what we store, not something #374 introduced, and it
  never changes a status: the reference decrements its own injected label
  out of the count, so the counts agree exactly (17 user labels reports
  `has 17 label names` on both). The same injection is why the
  reference's `MissingLabelsErrorMsg` is unreachable there and not
  implemented here, and why a stream whose labels are all empty-valued is
  stored as `{service_name="unknown_service"}` upstream and with no
  labels here.
  Loki error bodies additionally carry the trailing newline Go's
  `http.Error` appends, which none of our Loki plain-text error bodies
  have — also pre-existing, and not specific to these four.
- **Residual 2 — non-ASCII escaping in the rendered label set.**
  Prometheus' `Labels.String` (`model/labels/labels_common.go:57-80`,
  vendored at `v3.7.4`) renders every label value through Go's
  `strconv.Quote` and every non-legacy label name likewise. PulsusDB
  reproduces that exactly for every code point below `U+0080` — `\"`,
  `\\`, `\a`, `\b`, `\f`, `\n`, `\r`, `\t`, `\v`, and `\xNN` for the
  remaining C0 controls and `DEL` — and passes code points at or above
  `U+0080` through verbatim, where Go emits `\uXXXX` for those its
  `strconv.IsPrint` rejects (non-ASCII spaces, format and unassigned code
  points such as `U+00A0` or `U+200B`). Matching that last class needs
  Go's ~750-line `isPrint` range tables ported for the sole benefit of an
  error string; the difference is confined to the bytes of a `400` body
  for a stream that has already breached a bound, and never changes what
  is accepted.
- **Residual 3 — two PulsusDB-only structural caps that the reference
  does not have.** The reference bounds a stream's label literal by SIZE
  only (`maxStreamLabelsSize` = 16 MiB, `pkg/logql/syntax/parser.go:22 @
  v3.7.4`), which PulsusDB now adopts verbatim on the protobuf transport.
  Two count caps remain on top of it:
  - `MAX_LABELS_PER_STREAM` = 256 **surviving** labels per stream —
    distinct names whose final value is non-empty, counted after the
    envelope's last-wins resolution rather than on raw pairs. A stream with
    more than 256 such labels is `400` on both sides, but ours says
    `stream labels exceed the 256 per-stream bound` where the reference
    says `has N label names; limit 15`. Wording only, and only for input
    more than 17x over the parity bound. Neither empty-valued labels nor
    superseded ones count on either transport, so the
    16-non-empty-plus-241-empty stream that used to hit this cap gets the
    reference's message and 257 repetitions of one key are accepted as the
    one label the reference stores (`json/duplicate-key-257-collapses`,
    `204` on both). Measured on both transports (`json/257-nonempty`,
    `pb/257-nonempty`): `400` on both sides, ours reading `stream labels
    exceed the 256 per-stream bound` and `labels count 257 exceeds the
    documented limit of 256` respectively.
  - `MAX_RAW_LABEL_PAIRS_PER_STREAM` = 65,536 raw `(key, value)` pairs in
    one JSON `stream` object. The JSON transport must retain
    empty-valued pairs until their names have been grammar-checked and
    the map's last-write-wins collapse has happened, so this bounds that
    intermediate; the protobuf transport needs no equivalent because it
    drops empty values as it reads the literal. A stream carrying more
    than 65,536 raw JSON keys that collapse to 15 or fewer surviving
    labels is accepted upstream (within the 16 MiB rendered-literal bound)
    and refused here. Measured in both spellings —
    `json/65537-raw-empty-keys` (65,537 distinct empty-valued keys) and
    `json/duplicate-key-65537-raw` (65,537 repetitions of one key): `204`
    there, `400` here reading `stream label pairs exceed the 65536
    per-stream bound`. The count travels with the map it describes, so a
    superseded `stream` occurrence takes its breach with it
    (`json/superseded-stream-raw-pairs`, `204` on both).
- **Residual 4 — a repeated OTLP index-attribute key.** A resource that
  carries the same index attribute twice with different values is
  resolved last-write-wins upstream (`streamLabels` is a map,
  `otlp.go:191-193 @ v3.7.4`), so the bound is charged on whichever value
  came last. PulsusDB collapses the repeat through
  `LabelSet::from_normalized`, whose resolution is issue #4's frozen
  greatest-`(key, value)` rule, so the bound is charged on the value that
  would actually be stored. Measured: `[k8s.pod.name="ok",
  k8s.pod.name="b"*2049]` is `400` upstream and `200` here; the reverse
  order agrees. Matching upstream's choice would mean validating a value
  we do not store — the defect this issue's first round was about — so
  the real fix is to change the collision rule for OTLP resource
  attributes, which changes stored stream identities and belongs to
  #4/#109. OTLP's data model requires attribute keys to be unique within
  a map, so this is undefined input.
- **Residual 5 — a map-valued OTLP index attribute.**
  `attributeToLabels` (`otlp.go:602-640 @ v3.7.4`) recurses into a map
  value and emits one label per leaf, named `<parent>_<leaf>`, so a
  1025-byte nested key becomes an over-long label NAME upstream.
  PulsusDB renders a map attribute to a single JSON-valued label (issue
  #109), so the long key ends up inside a value that is under the value
  bound: `400` upstream, `200` here. The same payload with a long nested
  *value* is `400` on both, by different routes. This is the
  attribute-flattening difference, not the bound; it belongs with #109.
- **Residual 6 — the `AnyValue` depth cap outranks the stream-less
  `422`.** PulsusDB bounds OTLP `AnyValue` nesting at
  `MAX_ANYVALUE_DEPTH` = 32 (finding #54, a stack-safety guard); the
  reference has no such bound — a record-*bearing* resource whose
  attribute nests 33 `AnyValue` nodes deep is `204` upstream and `400`
  here, measured on both transports. Because both our transports charge the cap inside
  decode (`otlp_prescan::prescan_logs` for protobuf,
  `otlp_json::AnyValueSeed` for JSON, and `ensure_logs_anyvalue_depth`
  repeats it as `parse`'s first statement), a body that is *both*
  record-less *and* over-deep answers `400` here where the reference
  answers `422` — the depth reject wins because it runs first. Measured
  on both transports (`otlp/record-less-over-deep-attr`,
  `otlppb/record-less-over-deep-attr`, at depth 33; their at-cap
  neighbours, one `AnyValue` node shallower at depth 32 — the deepest tree
  still accepted — are `422` on both sides), and **pre-existing**: the same `400` comes out of the
  branch point `5969a94`, so neither the bounds nor the `422` introduced
  it. Nothing upstream fixes the order between a bound the reference does
  not have and one it does, so the order is stated rather than matched.
  Reordering it is not a local change either: the record count cannot be
  read before the body is decoded, and the depth cap is charged *during*
  decode precisely so the over-deep tree is never materialized or
  recursed into — the JSON seed refuses before descending further, the
  protobuf pre-scan before `prost` allocates. Deferring it would undo
  that.
- **Residual 7 — a superseded occurrence still charges the SHARED decode
  budget.** The per-occurrence caps are charged after last-wins resolves
  (the bullet above), but the two counters that run across the WHOLE
  request are not: `MAX_TOTAL_ENTRIES_PER_REQUEST` = 5,000,000 entries and
  `MAX_DECODED_BYTES` = 256 MiB of `size_of`-estimated materialization stay
  immediately fatal. They measure memory the request has already cost
  across every occurrence, and a supersession does not give it back while
  the superseding value is still decoding — so deferring them would mean
  decoding past the budget to find out whether it mattered, trading a
  rejection divergence for a resource one. The reference has no equivalent
  bound on DECODING: the only limit a push body meets before it is decoded
  is the 100 MiB compressed body (`distributor.max-recv-msg-size` default
  `100<<20`, `pkg/distributor/distributor.go:124 @ v3.7.4`, applied by
  `io.LimitReader` in `parsePushRequestBody`,
  `pkg/loghttp/push/push.go:322-325`), inside which jsoniter materializes a
  superseded value in full and throws it away. That is not the same as "no
  other limit": what SURVIVES decoding is forwarded to the ingesters over
  gRPC and meets a 4 MiB message ceiling there
  (`server.grpc-max-recv-msg-size-bytes` default `4*1024*1024`,
  `vendor/github.com/grafana/dskit/server/server.go:220`, vendored at
  v3.7.4), which answers `500 rpc error: code = ResourceExhausted desc =
  grpc: received message larger than max (4600096 vs. 4194304)` — measured.
  A surviving 100,001-entry stream is that `500` upstream against our `400`,
  which is why those two neighbours are asserted hermetically rather than
  carried as harness rows.
  Reachable, and measured rather than reasoned: 38 superseded streams of
  100,000 minimal entries is a 34 MB body — inside both that 100 MiB and our
  own 64 MiB decompressed cap — and answers `204` upstream with the
  surviving line stored, `400` here (`decoded bytes (estimated) 268435520
  exceed the request decode budget of 268435456`). The same shape at 30
  streams (27 MB, under the budget) agrees `204`/`204` and stores the same
  line. Harness: `json/superseded-shared-budget` and its `-under`
  discriminator.
  **What the deferral itself costs, stated exactly.** A drained run is
  parsed, so it costs parse time over the rest of the body; what it does
  NOT cost is retention. Peak retained is what the caps admit —
  `MAX_STREAMS_PER_REQUEST + 1` streams, `MAX_ENTRIES_PER_STREAM + 1`
  entries per stream, `MAX_STRUCTURED_METADATA_PER_ENTRY + 1` pairs per
  entry — plus ONE in-flight element being read and dropped, and the input
  is bounded before decode by the 64 MiB decompressed body cap. A value of
  arbitrary shape costs even less: it is captured as a BORROWED slice of the
  request body and scanned for nesting with a counter (residual 8), so the
  only memory it costs is the body it is already part of.
  **What RSS can and cannot show about that.** It is too coarse an
  instrument for this claim, and an earlier round of this issue over-read
  it. Measured on a freshly started subject: five 400 kB, 200,000-level
  bodies — all refused — took RSS from 30,824 kB to 36,412 kB, and the next
  five ORDINARY pushes took it back DOWN to 35,720 kB. That is a few MB of
  allocator churn in both directions, neither a retained cost nor a leak;
  the earlier round read a flat pair of numbers off one long-warmed process
  and reported the absolute figure as though it were a property of the
  change. What is bounded is bounded structurally, above, and does not rest
  on an RSS sample.
- **Residual 8 — CLOSED in round 14; kept for the record because the
  numbering is cited elsewhere.** For one round the JSON body's nesting
  ceiling was 128 levels rather than the reference's 10,000, and that was an
  over-rejection of ours: bodies the reference accepts and STORES were
  refused here. Nesting of arbitrary depth reaches only values this decoder
  does not keep — a key it does not read (an unknown envelope key, an
  unknown key inside a stream object, an entry's fourth+ element) or a run
  past one of the caps above. Round 12 deserialized all of them through
  `serde`, which put them under `serde_json`'s `RECURSION_LIMIT` of 128
  (`de.rs:63,1375 @ 1.0.150`); that constant is fixed and cannot be raised,
  so 127..9,999 levels under an ignored key was `400` here and `204` there
  (bisected on both servers: we accepted 126, upstream accepted 9,999).
  Round 14 charges depth against **the reference's own bound** instead. A
  value of arbitrary shape is captured as raw text — `serde_json`'s
  `RawValue`, filled by the same `Deserializer::ignore_value` walk
  `IgnoredAny` used, so syntax is still checked and nothing is retained —
  and its nesting is charged against `maxDepth = 10000` (`iter.go:331-338 @
  jsoniter v1.1.12`, vendored, error `exceeded max depth`), its numbers
  against the reference's own skip-time overflow rule (the bullet above).
  Typed values
  keep serde's 128, which nothing legal reaches: a container in any of those
  positions is a type error before its depth is looked at.
  **What upstream counts is not the brackets to the left of the value**, and
  reading that off its decoders rather than off one measured example is what
  makes this parity rather than a coincidence at one position. The envelope
  object counts (`oneFieldStructDecoder` increments,
  `reflect_struct_decoder.go:574-594`); the `streams` ARRAY does not
  (`sliceDecoder` never increments, `reflect_slice.go:59-99`); the stream
  object and everything under it do, because `LogProtoStream` has an
  `UnmarshalJSON` (`pkg/loghttp/query.go:99 @ v3.7.4`) that jsoniter reaches
  through `unmarshalerDecoder`/`SkipAndReturnBytes`, whose `iter.Skip()`
  walks the object with `ReadObjectCB`/`ReadArrayCB`
  (`iter_skip_strict.go:85-99`). So the deepest ACCEPTED nest is 9,999 under
  an unknown envelope key, 9,998 under an unknown key inside a stream and
  9,996 as an entry's fourth element — all six boundaries bisected against
  the two servers, all six now agreeing. Harness: the `json/ignored-depth-*`
  cases, three positions × (deepest accepted, first refused, 200,000) plus
  126/127; hermetically,
  `loki_push::tests::parse_json_bounds_nesting_depth_at_the_references_ceiling`
  asserts those literals, and
  `loki_push::tests::parse_json_a_discarded_value_is_still_syntax_checked_and_strings_are_not_nesting`
  pins what the byte scan must not confuse for nesting.
- **Residual 9 — a non-string value in a `stream` label map.** Upstream does
  not type these: `LabelSet.UnmarshalJSON` runs
  `jsonparser.ParseString(val)` over the raw bytes of whatever the value is
  and stores the result (`pkg/loghttp/labels.go:29-37 @ v3.7.4`), so
  `{"stream":{"a":123}}` is `204` there and stores the label `a="123"`,
  while `serde` refuses it here with `invalid type: integer 123, expected a
  string`. Pre-existing and not introduced by any of this row's work — it
  is the retained path's rule, and the only thing round 12 changed is that
  the same rule now also applies PAST the raw-pair cap, where an
  `IgnoredAny` drain used to let a non-string value through (measured
  `204`/`204` by accident before, `204`/`400` consistently after). Adjacent
  and the same mechanism: `"values":[null,["ts","x"]]` is `204` upstream,
  which skips null elements explicitly (`if ty == jsonparser.Null { return
  }`, `pkg/loghttp/query.go:131-134 @ v3.7.4`), and `400` here. Both belong
  to a value-typing change, not to this row's cap ordering. Harness:
  `json/drained-label-map-non-string` and its `-under-cap` control, both
  recorded divergences.
- **Residual 10 — the LENGTH of a digits-only number in a value neither side
  reads.** PulsusDB's rule: a run of digits with at most one dot, under a key
  the decoder does not read, is accepted **whatever its length**. The
  reference's rule, stated as its mechanism rather than as the shape that
  mechanism produces: a digits-only run is **skipped unevaluated only while
  it fits inside the decoder's current read buffer**; a run that spans the
  buffer's end leaves that fast path, **is parsed as a float**, and is
  refused **only if the parse overflows `f64`**. The fast skip scans that
  buffer and nothing else — `for i := iter.head; i < iter.tail`
  (`iter_skip_strict.go:26 @ jsoniter v1.1.12`) — over the 512 bytes
  `jsoniter.NewDecoder` allocates (`config.go:366`, reached from
  `unmarshal.DecodePushRequest`, `pkg/util/unmarshal/unmarshal.go:17 @
  v3.7.4`); a token that reaches `tail` returns `false` from `trySkipNumber`
  (`:55,:58`) and falls through to
  `ReadFloat64`/`readFloat64SlowPath`/`strconv.ParseFloat`
  (`iter_skip_strict.go:10-21`, `iter_float.go:299-315`), whose `ErrRange`
  is then surfaced through the `ReadBigFloat` retry over an already-consumed
  buffer as `readNumberAsString: invalid number`. **Crossing the boundary is
  not itself a rejection — it removes the exemption.**

  **Why this is registered and not matched.** Where that buffer boundary
  falls is not a property of the request. It moves with the token's byte
  offset in the body and with how the client chunked its writes, so the same
  bytes get different verdicts from different senders. Measured on
  `grafana/loki@sha256:87f0a067…` and a PulsusDB built from this branch
  (issue #374 round 17), one ignored digit run under an unknown envelope
  key:

  | shape | reference | PulsusDB |
  |---|---|---|
  | 400 digits, body written in one piece | `204` | `204` |
  | 400 digits, **same bytes**, 512-byte chunks | `204` | `204` |
  | 400 digits, **same bytes**, 256-byte chunks | **`400`** | `204` |
  | 400 digits, one write, run at offset 111 | `204` | `204` |
  | 400 digits, one write, run at offset 112 | **`400`** | `204` |
  | 504 digits at offset 7 | `204` | `204` |
  | 505 digits at offset 7 | **`400`** | `204` |
  | 308 nines crossing a boundary | `204` | `204` |
  | 309 nines crossing a boundary | **`400`** | `204` |
  | 309 digits, `1` then 308 zeros, crossing | `204` | `204` |
  | 1,000 digits, any offset, any framing | **`400`** | `204` |

  The last three rows are the mechanism rather than a length threshold:
  crossing only removes the exemption, so the same 309-digit length is
  **`400`** when the value overflows `f64` and **`204`** when it does not
  (measured at three offsets, all crossing). Length alone decides nothing
  either — 400 digits is either answer depending on framing. A run of 512
  digits or more cannot fit in a 512-byte window at any offset, and no run of
  310 digits or more is representable in `f64`, so a 1,000-digit run is the
  one corner of this that is framing-independent in **both** conditions, and
  is what the harness pins. That independence is measured rather than argued:
  `number_route_probe.py`'s `ctl-1000-nines` sends it in all three ignored
  positions at three offsets in four framings, 36 cells, `400` upstream and
  `204` here in every one.

  Matching the reference here would mean reproducing the sender's socket
  behaviour, which is worse for a user than the difference: our acceptance
  surface would stop being a function of the request. So this is the
  standing rule applied — copy the reference except where it is wrong — and
  it is recorded rather than chased.

  **Bounded**: the divergence is exactly the digits-only runs that span the
  end of the reader's current 512-byte window **and** whose value overflows
  `f64` — for an all-nines run that means 309 digits or more, while a
  309-digit run of smaller magnitude is accepted on both sides. The exponent
  rule agrees on both edges (`1e999`, `1e309`, `1.7976931348623159e308`
  refused on both; `1e308`, `1.7976931348623157e308`, `1e-999`, `5e-324`
  accepted on both), and each of the fourteen ignored-number shapes the
  harness and the hermetic test pin between them was re-measured over 12 cells
  in round 17 — 4 wire framings × 3 byte offsets, 168 cells in all — and over
  36 cells each in round 19 — those same 12 in each of the three ignored
  positions, 504 cells — without moving on either server. Those 504 cells are
  the exponent group of
  `crates/pulsus-write/tests/golden/log_label_bounds/number_route_probe.py`,
  a raw-socket matrix that writes each body in the four framings at the three
  offsets and exits non-zero on a cell that moves; the figure is that script's
  output rather than a report of one, and `number_route_probe.txt` beside it
  records a run of it and of its stale-timestamp mutant. Thirteen of the
  fourteen take `skipNumber`, and for the eleven of those that carry an `e`
  that invariance is what `trySkipNumber` leaving the fast path on `e` in every
  window predicts: the exponent axis cannot depend on the buffer. The other
  two, `12345678901234567890` and `-0.0`, are short digits-only runs, `204`
  whether they are skipped or parsed. The fourteenth shape, `0e999`, takes
  `ReadFloat32` on its first byte, and zero is finite in both widths, so its
  cells are `204` on either route and none of them could have shown the
  dispatch in a status.
  Round 18 re-derived the table above on the same two servers after changing
  the `f32` route below, rather than restating it: bisecting the offset of a
  400-nine run in one write gives a last accepted offset of 111 (offset +
  length + 1 = 512 exactly), `400` at 112; the same bytes are `204` in one
  write and in 512-byte chunks and `400` in 256- and 128-byte chunks;
  crossing, 308 nines is `204`, 309 nines is
  `400`, 309 digits of `1` then zeros is `204`; 1,000 nines is `400` at four
  offsets. PulsusDB answered `204` to every one of those, unchanged. The
  111/112 pair is no longer a one-round derivation: it is the pair of controls
  `number_route_probe.py` sends on every run, in all three ignored positions
  and all four framings, and they are there to prove the framing and offset
  knobs reach upstream's decoder at all — without them a run whose chunked
  writes had coalesced would still have reported 720 agreeing cells (measured,
  with the pause between chunks removed).

  **One shape was outside this residual and is now CLOSED (found round 17,
  fixed round 18).** `Skip` never reaches `skipNumber` for a token whose first
  byte is `0`: it calls `ReadFloat32` directly (`iter_skip.go:83-85 @ jsoniter
  v1.1.12`), so upstream range-checks that token against **`f32`**, not
  `f64`. Measured on the two servers over 36 cells per shape (3 ignored
  positions × 4 wire framings × 3 byte offsets, 216 cells — the `f32` group of
  `number_route_probe.py`), each shape single-valued:
  `0.35e39` and `0.34028236e39` are **`400`**, `0.34e39` and `0.34028235e39`
  — the latter still rounding to `f32::MAX` — are `204`, and the same
  magnitude written `3.5e38` or `-0.35e39`, neither of which starts with `0`,
  is `204` because both take the `skipNumber`/`f64` route. Unlike the length
  axis this is deterministic on the request — the `ReadFloat32` route never
  enters `trySkipNumber`, so no read buffer is involved — which is why it was
  matched rather than registered. PulsusDB now dispatches on the same byte
  (`check_ignored_number`), and all six shapes agree on both sides.

  Harness: the `f32` route is pinned by six agreeing rows —
  `json/ignored-number-lead0-over`, `-lead0-under`, `-lead0-at-f32-max`,
  `-lead0-past-f32-max`, and the two controls `-signed-lead0` and `-no-lead0`
  that make them mean the dispatch rather than the value.
  `json/ignored-number-int-1000` is the recorded divergence;
  `json/ignored-number-int` (400 nines) is the control that agrees, and it
  agrees only because that body puts the run at offset 86, so its terminating
  byte lands at index 486 against a last-fitting index of 511 — 25 bytes of
  headroom. A note in `compare.py` says so, because growing that body by 26
  bytes ahead of the run flips it (measured: +25 is `204`, +26 is `400`).
  Hermetically,
  `loki_push::tests::parse_json_a_discarded_number_follows_the_references_overflow_rule`
  asserts our side of it in all three ignored positions.

  **How this was missed for a round.** Round 14 claimed "66 probes over 22
  shapes × 3 positions, 0 disagreements". The probes were real and the count
  was real, but every one of them placed the digit run where it happened to
  fit the window, so the gate could not see the axis it was being quoted to
  cover. A 66-probe slice at the same three positions, placed differently,
  found the disagreements. Same instrument, same count, opposite result:
  the placement was the variable and it was never varied.
- **Measured side by side**: every case in the harness is sent
  byte-identically to `grafana/loki@sha256:87f0a067…` and to a running
  PulsusDB, over both Loki-push encodings and both OTLP encodings. Statuses
  agree everywhere except residual 3's second half (in both its spellings)
  and residuals 4, 5, 6, 7, 9 and 10 — residual 8 was closed in round 14 and
  its rows are agreements now — which the harness carries as expected
  divergences so that they stay the ones that were recorded; the case,
  agreement and divergence counts are on
  `compare.py`'s generated summary line at the end of the transcript's
  table, and are deliberately not copied into this row. The
  transcript and the harness that produced it are checked in at
  `crates/pulsus-write/tests/golden/log_label_bounds/`; the harness exits
  non-zero on any verdict that is not its expected one. A case is one body
  written in one piece, so one thing that directory carries cannot be a case:
  the claim that the ignored-number verdicts do not depend on the wire
  framing or the token's byte offset. `number_route_probe.py` beside it is the
  instrument for that one — a raw-socket writer over 20 shapes × 3 ignored
  positions × 4 framings × 3 offsets, plus three deliberately framing-aware
  controls, exiting non-zero on a cell that moves — and
  `number_route_probe.txt` is its recorded run.
  An earlier 44-case version of that harness scored 44/44 against an
  implementation that selected the OTLP subset on the canonicalized name,
  because every OTLP case it sent used either an exact dotted index name
  or an obviously arbitrary one — never a raw key whose canonical form
  disagrees with it about membership, which is where that defect lived.
  The `otlp/raw/…` and `otlp/canonical/…` pairs are now generated from the
  reference's own list for that reason; re-running the current cases
  against that implementation produces 25 unexpected verdicts. **The list
  itself is read out of the reference too** —
  `git show b318f282:pkg/loghttp/push/otlp_config.go`, parsed at run time,
  with a non-zero exit rather than a fallback if it cannot be read —
  because pairs generated over a hand-copied list are still drawn from our
  own model of the rule. **And these are statuses:** the four
  `otlp/index-key-vs-near-miss/*` cases agree on the wire and diverge in
  what is stored, which is why that half is asserted on stored state
  instead (see the raw-name paragraph above).
- **Pinned by** `log_label_limits`'s own unit tests (the four bounds, both
  edges each, the `service_name` decrement, the empty-value rule and its
  effect on stored identity, the internal-stream exemption, the
  check-order cases including count-vs-duplicate and value-vs-duplicate,
  and the whole ASCII range of the Go quoting transcribed from
  `strconv.Quote`; plus
  `every_index_attribute_is_bounded_in_its_raw_spelling_only`, which walks
  the reference's own 18-name list and asserts both directions rather
  than sampling), `loki_push::tests::parse_json_*`/`parse_protobuf_*`
  and `otlp_logs::tests::*` (both receivers reach the rules, empty labels
  do not consume the decode caps, the OTLP bounds are charged on the
  raw-name-selected indexed subset while the empty-value drop is not —
  `every_raw_index_name_bounds_and_its_canonical_spelling_does_not`,
  `a_raw_attribute_that_only_canonicalizes_into_an_index_name_is_not_bounded`
  — and a bad stream costs only itself),
  `ingest/http.rs`'s
  `loki_over_long_label_value_is_400_with_the_reference_message` /
  `loki_over_wide_stream_is_400_with_the_reference_message` /
  `logs_over_long_resource_attribute_value_returns_400_with_status_code_3`
  / `loki_mixed_batch_admits_the_good_streams_and_still_answers_400` /
  `logs_mixed_batch_admits_the_good_resources_and_still_answers_400` /
  `stream_errors_are_grouped_the_way_the_reference_groups_them` (the wire
  status, body and admission), and the live
  `crates/pulsus-server/tests/loki_push_live.rs` quintet
  `over_wide_label_value_is_rejected_and_stores_nothing`,
  `a_mixed_batch_stores_the_good_streams_and_still_answers_400`,
  `an_empty_valued_label_does_not_split_the_stream` (`400` at the wire
  **and** the matching row counts read back out of ClickHouse, with an
  at-bound control push proving the read-back discriminates),
  `a_stream_less_push_is_422_on_both_receivers_and_stores_nothing` and
  `an_otlp_near_miss_spelling_stores_an_over_wide_indexed_label` — the one
  case the wire cannot show, where both sides answer success and the
  stored label and fingerprint are read back to show the 2049-byte value
  on an indexed label. The stream-less `422` is pinned at every tier: the
  parsers (`parse_json_rejects_a_request_with_no_streams`,
  `decode_protobuf_rejects_a_request_with_no_streams`,
  `parse_rejects_a_request_with_no_log_records`), the wire
  (`loki_push_with_no_streams_is_422_with_the_reference_message`,
  `loki_protobuf_push_with_no_streams_is_422`,
  `logs_request_with_no_log_records_is_422_with_the_reference_message`),
  and — because a status-only rule is easy to over-apply — its accepted
  neighbours beside it at each tier
  (`parse_json_entry_less_stream_alone_is_not_a_stream_less_request`,
  `parse_accepts_a_request_whose_records_are_all_in_one_resource`,
  `loki_push_with_an_entry_less_stream_is_still_accepted`). Its one
  exception — residual 6's ordering against the `AnyValue` depth cap — is
  pinned by `the_depth_cap_outranks_the_stream_less_check`, whose at-cap
  neighbour is inside the same test, and on the wire by the four
  `*-over-deep-attr` harness cases.
