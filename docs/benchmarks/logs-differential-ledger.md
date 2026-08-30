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

## Every entry states the conditions that reproduce it — and nothing enforces that

An entry that records a measurement carries the conditions that produced
it: the **route**, the **window**, the **anchor** (how the instant the
window is relative to is chosen, whenever a recorded byte depends on
where in wall-clock time it falls), the **seed** (what was pushed, with
its sample timestamps), the complete **query** text, and the reference
**digest**. A number without them is a figure nobody can re-measure, and
this ledger has shipped that three times: a missing route cost three
rounds on #294, a missing window cost a round on #455, a missing anchor
cost another.

**This is a discipline, not a check. Nothing verifies it.** Issue #455
built four mechanisms to enforce it and every one was defeated inside a
review round: a flat field list (a neighbouring entry's literal satisfied
it), a section-scoped search (an invented table name matched everything),
a table-scoped span (a literal moved into an HTML comment inside the span
still matched), and a span-scoped exemption reason (the reason was struck
through and contradicted beside itself, and the original bytes remained).
Each asked *does this string appear?* when the claim is *does this table
state this condition?* — and a presence test cannot tell a live claim
from one that has been hidden, withdrawn or contradicted, because those
are properties of meaning. A fifth would find a fifth decoy.

So the check is a reader. When you touch an entry that records a
measurement, re-read its conditions; when you add one, write them.

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

### detected-labels-window-scoped-to-rollup-bucket (issue #399)

- **Construct:** the time bound on the four log **discovery** endpoints —
  `/detected_labels`, `/labels`, `/label/{name}/values`, `/series`. All
  four scan `log_streams_idx`, which carries no time column at all
  (`month Date, key, val, fingerprint` —
  `crates/pulsus-schema/src/catalog.rs:224-238`), so before #399 the only
  bound any of them applied was the calendar `month`. A one-hour request
  was answered from the whole month containing it. Informational note,
  not a gate downgrade.
- **Direction:** **PulsusDB is now strictly TIGHTER than the reference on
  three of the four, and matches its intent on the fourth.** The bound is
  a semi-join against the log rollup — `fingerprint IN (SELECT DISTINCT
  fingerprint FROM log_metrics_<res> WHERE bucket_ns >= <floor(start)> AND
  bucket_ns <= <end>)` — whose lower edge is the rollup bucket
  **containing** `start`, because the MV stores `bucket_ns =
  intDiv(timestamp_ns, res) * res`. So ours over-includes by at most one
  rollup resolution (5s at the default `PULSUS_LOG_ROLLUP_RESOLUTION`) at
  each edge, and can never drop a stream with a line in the window.

- **The reference is not uniform across the four, and that asymmetry is
  the reason all four moved together here rather than one at a time.**
  Read at `grafana/loki` v3.7.4 =
  `b318f2829f0ae2094ab3a1e90780450e9e4b03be`:
  - `TSDBIndex.LabelNames` and `TSDBIndex.LabelValues` **ignore
    `from`/`through` entirely** — the parameters are literally `_, _
    model.Time` in both signatures
    (`pkg/storage/stores/shipper/indexshipper/tsdb/single_file_index.go:304`,
    `:312`). What bounds them is which index FILES overlap the window,
    selected by `forMatchingIndices`
    (`multi_file_index.go:115-132`, `:247-289`), i.e. the index period —
    24h in the probe below.
  - `TSDBIndex.Series` **is** window-bounded, and finely: it passes
    `from`/`through` down to `i.reader.Series(...)` and drops any series
    whose `chks` came back empty (`single_file_index.go:282-302`), i.e.
    chunk-granular.
- **Measured, not inferred** (`grafana/loki:3.7.4`, tsdb/v13, filesystem
  store, index period 24h; `/flush` + restart so the ingester is empty and
  exactly one index file exists; three streams with single lines at T−6h,
  T−1h and T−5m):
  - `/detected_labels` over `[T−2h, T−1h]` returned a label whose only
    line sat ~5h outside that window; over the **previous day** it
    returned `[]` — so the bound is real but index-file-granular.
  - `[T−7h, T−5h]` (store-only, outside `query_ingesters_within`=3h,
    containing only the T−6h stream): `/labels` returned all three
    streams' keys and `/label/job/values` both values, while `/series`
    returned **exactly the one stream**.
- **What PulsusDB answers on the same fixture shape:** all four return
  only what the window contains, bucket-granular. The endpoint where the
  reference is closest to us is `/series`; the three others are where we
  are tighter.

- **Two bounded consequences, both accepted:**
  1. **Bucket quantization at a month boundary.** With `start` within one
     bucket of a month start, the floored lower bound can name a bucket in
     the previous month, whose index rows the outer `month` predicate then
     still excludes. Bounded by one rollup resolution and strictly
     narrower than the whole-month error it replaces. `months_overlapping`
     is deliberately NOT widened.
  2. **Rollup-table rename exposure.** `PULSUS_LOG_ROLLUP_RESOLUTION`
     names the table (`catalog.rs` id 9, `MigrationScope::ConfigName`), so
     after a resolution change the old table is orphaned and pre-change
     activity is invisible to these endpoints. This is the exposure
     `/stats` and `/volume` already carry (`sql::log_stats_rollup`,
     `sql::log_volume_rollup` read only the configured name) —
     pre-existing and consistent, not newly introduced by #399. No
     multi-table fallback is built.

#### Cost

The correctness fix is not free, and the number is recorded here rather
than as a ratio so the deferred activity-index decision (routed to **#25**
by the issue #399 rulings) re-opens on arithmetic instead of projection.

**Architect's fixture, the measurement the ruling deferred against**
(2,000 streams each emitting into every 5s bucket for a full day =
34,560,000 rollup rows/day, 206.58 MiB on disk/day; 6,009
`log_streams_idx` rows across 3 keys; request window 10 minutes;
ClickHouse 24.8.14.39, single node, `system.query_log`, mean of 3):

| query | read rows | read bytes | ms |
|---|---|---|---|
| pre-#399 (wrong) aggregation — index only | 6,009 | 134.29 KiB | 11 |
| fixed aggregation, **unscoped** | 29,086,663 | 287.45 MiB | 70 |
| the activity subquery alone, **unscoped** | 29,080,654 | 287.27 MiB | 50 |
| the activity subquery alone, **scoped to 5 fingerprints** | 73,728 | 1.13 MiB | 2 |
| the activity subquery, unscoped, corpus **doubled** to two days (69,120,006 rows) | 29,080,654 | 287.27 MiB | 59 |

**Independently reproduced during implementation** on the same shape
(2,000 streams × 17,280 5s buckets = 34,560,000 rows/day, 80.88 MiB on
disk/day across 12 parts; 6,000 `log_streams_idx` rows across 3 keys;
10-minute window; ClickHouse 24.8.14.39; three runs each, range shown):

| query | read rows | read bytes | ms |
|---|---|---|---|
| pre-#399 (wrong) aggregation — index only | 6,000 | 134.15 KiB | 14–21 |
| fixed aggregation, **unscoped** | 25,808,136 | 261.96 MiB | 244–477 |
| the activity subquery alone, **unscoped** | 25,802,136 | 261.78 MiB | 231–529 |
| the activity subquery alone, **scoped to 5 fingerprints** | 65,536 | 1.00 MiB | 6–18 |
| the activity subquery, unscoped, corpus **doubled** to two days (69,120,000 rows) | 25,802,136 | 261.78 MiB | 75–90 |

Both runs agree on the four things that decide the deferral:

1. **The unscoped path costs hundreds of MiB per request on a
   2,000-hot-stream day** — a ~4,300× read amplification in rows over the
   pre-#399 wrong answer. Well inside the 50 GiB
   `logql_scan_budget_bytes` default; not free, and not called free.
2. **Cost scales with the window's DAYS, not with retention.** Doubling
   the corpus left the read byte-identical (25,802,136 rows / 261.78 MiB
   in the reproduction), because `bucket_ns` is the partition key's only
   input column: measured `EXPLAIN indexes = 1` shows `MinMax … Parts:
   1/8, Granules: 3258/8440`, then `PrimaryKey … Granules: 3150/3258`.
   The absolute law is `rows read ≈ active_streams × (86400/res) ×
   days_touched`, and it falls proportionally with a coarser
   `PULSUS_LOG_ROLLUP_RESOLUTION`.
3. **Pushing a stage-1 fingerprint list INTO the subquery is worth ~394×**
   (65,536 vs 25,802,136 rows; the architect measured ~395× on the same
   shape). That is what makes `/series` — always scoped by `match[]` — and
   `detected_labels?query=` cheap.
4. **The expensive cases are exactly the unscoped ones.** An UNSCOPED
   `/labels`, `/label/{name}/values` or `detected_labels` pays the whole
   activity scan. Issue #482 gave the first two the same `query=`
   narrowing `detected_labels` already had — the reference passes
   `matchers` to both store calls (`pkg/querier/querier.go:706-737`) —
   so a SCOPED request on either now takes the embedded form measured in
   item 3 instead, at the cost of one stage-1 round trip. What remains
   expensive is a request that supplies no selector, which is the
   request that has no narrowing to apply. A day-granular activity index
   would take that scan from ~26M rows to ~2,000. Deferred per the issue
   #399 rulings; re-open against these tables, not against a projection.

`DISTINCT` on the `(fingerprint, bucket_ns)` PK prefix is a streaming
distinct (`Distinct (Preliminary DISTINCT)` in the measured plan), so the
`IN` set is O(active streams), not O(streams × buckets).

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
  value set there is. It is also useless, and it is the ONLY such `N` —
  a fact with a witness rather than a hope: `{"svc-787", "svc-4532"}` is
  a TWO-value set whose sparse keys collide (both 36184712, the pair
  identified below), and the reference answers **1** for it. So no bound
  at or above `N = 2` holds for every value set, and none is claimed
  here.

  **Above that floor the following are SUFFICIENT conditions, not
  necessary ones.** The estimate equals the exact count **whenever** all
  three hold — the first two depend on the value strings, the third does
  not — and it frequently equals it when they do not. What changes above
  the floor is not that agreement stops; it is that agreement stops
  being guaranteed:
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

  **Agreement outside those conditions is routine, and is coincidence.**
  Driving the vendored library directly: `instance-{i}` at `N` = 7966,
  7989, 8012 and 8015; `10.42.0.{i}` at 7760, 7762, 7767 and 7768; and
  `v{i}` at 7780, 7782, 7794 and 7797 each report exactly `N` with the
  sketch **already dense** — condition 2 broken, answer still right.
  Read nothing above the floor as a rule in either direction: not "they
  agree below X", and not "they disagree above X" either.
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
- **Memory characteristic — CLOSED by issue #398; the measurement that
  motivated it is kept.** Before #398, LogQL reads set `max_bytes_to_read`
  but no `max_memory_usage` (`read_query_settings`,
  `crates/pulsus-read/src/logql/exec.rs`), so a ClickHouse code 241 fell
  through `map_read_error` to `ReadError::Clickhouse` and surfaced as
  **500** — carrying the raw server exception in the body — where the
  `QueryTooBroad` family answers 422. It was deliberately NOT fixed under
  #261, because it is not this endpoint's exposure: on the identical
  corpus B above, the SHARED stage-1 stream resolution (`sql::stage1`,
  which `/query_range`, `/series`, `/detected_fields` and
  `/detected_labels` all run) used **3.19 / 3.03 / 2.91 GiB** against this
  endpoint's aggregate at 0.87–0.88 GiB — 3.3-3.6× more. A cap scoped to
  `/detected_labels` would have sat on the cheaper half and left the
  expensive half uncapped. One mechanism, one issue: **#398**.

  **What #398 shipped.** `reader.logql_read_max_memory_bytes`
  (`PULSUS_LOGQL_READ_MAX_MEMORY_BYTES`, default 8 GiB) applies
  `max_memory_usage` + `max_bytes_before_external_group_by = 0`
  (throw-not-spill) at `read_query_settings` — the single origin every
  LogQL dispatch site's settings object comes from, so the shared stage-1
  read is covered along with everything else — and code 241 maps to
  `TooBroadReason::LogqlReadMemory`, i.e. **422 `query_too_broad`** naming
  the knob. Sibling knobs do the same on the other two read surfaces
  (`promql_read_max_memory_bytes`, `traceql_read_max_memory_bytes`). No
  endpoint-scoped cap exists anywhere, so there is no carve-out to
  justify. Gated by `stage1_memory_breach_is_422_on_every_stage1_endpoint`
  (all five stage-1 endpoints, `crates/pulsus-server/tests/logs_api_live.rs`)
  and `every_logql_engine_query_carries_the_memory_ceiling`
  (`crates/pulsus-read/tests/query_log_gates.rs`).
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
  `/detected_fields` `200`, and (issue #482) the ORDER of the `values`
  array in a `/detected_field/{name}/values` `200`. Per-field object
  shape and the zero-field body are byte-exact (issues #254/#258); this
  entry is scoped to the array order alone, which is why neither
  endpoint is byte-exact end to end for a populated response. The two
  routes are one registered class, not two: the reference builds both
  arrays by ranging the same kind of Go map, and a row that did not name
  its second endpoint would go stale invisibly.
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
  `FieldAccumulator::finish`; `values` is **byte-ascending**, pinned in
  `FieldAccumulator::into_field_values`. This is the ratified treatment of every
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
  comparison and `detected.rs::finish_sorts_fields_by_label`; the
  `values` clause is gated by `logs_detected_live.rs`'s
  `detected_field_values_*` cases, which assert whole response bodies
  including the ascending array.

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
- **Ruling, 2026-08-12 (issue #425 Part A).** The verdict above was
  challenged and **upheld**. The case for reversing it was that the
  reference's default `split_queries_by_interval` is `1h`, so every stock
  deployment shows the aligned grid and a dashboard moved between the two
  stores would look different. Refused: that argument is about migrating
  existing deployments, and the aligned grid remains an artefact of query
  splitting that hands back points **outside** the requested
  `[start, end]`. Part A closed with no code change; the divergence is
  recorded as its own row, `range-step-grid-start-anchored`, below.
  Owner ruling: https://github.com/digitalis-io/pulsusdb/issues/425#issuecomment-5263485356

### range-step-grid-start-anchored (issue #425, owner ruling 2026-08-12 — a deliberate divergence, not a defect)

- **What we do:** a metric range query emits points on the
  **start-anchored** grid `{start + k·step ≤ end}`, so every point lies
  inside the window the caller requested. `step` itself, when omitted, is
  the reference's own whole-second derivation — issue #425 Part B, which
  carries no ledger row of its own because it restores parity rather than
  creating a divergence. The grid's *origin* diverges; its *spacing* does
  not.
- **What the reference does:** rewrites the request before its engine
  sees it. `metricQuerySplitter.split` calls
  `alignStartEnd(step, start, end)` (`pkg/querier/queryrange/
  splitters.go:236 @ grafana/loki v3.7.4 b318f282`, the definition at
  `:308`), flooring `start` and ceiling `end` to absolute multiples of
  `step`. **Measured** on the pinned image
  `grafana/loki@sha256:87f0a067…` 2026-08-12: a 501 s window from a 60 s
  -aligned `T0` with no `step` (both stores derive `2s` after Part B)
  returns **252 points ending `T0 + 502s`** there against our **251
  ending `T0 + 500s`** — two seconds past the `end` that was asked for.
  Controls: at 499 s and 900 s both bounds already sit on the derived
  grid and the two stores agree point for point (500 and 301 points),
  so what the 501 s row measures is the alignment and not the fixture.

  <!-- range-grid:start -->window_ns=501000000000 our_points=251 our_step_ns=2000000000 ref_points=252 our_last_offset_ns=500000000000 ref_last_offset_ns=502000000000<!-- range-grid:end -->

  **The measurement lives here and nowhere else.** `docs/api.md` and
  `logs_api_live.rs`'s doc comment used to restate it; both now cite this
  row. Two copies remain and they are BOUND to each other: the anchor
  block above, and the executable case tuple `(501_000_000_000, 251,
  2_000_000_000)` at `crates/pulsus-server/tests/logs_api_live.rs`, which
  is the only copy that fails when the number is wrong.
  `the_range_grid_anchor_binds_the_executable_case` asserts
  `window_ns`/`our_points`/`our_step_ns` equal that tuple and that
  `ref_last_offset_ns - our_last_offset_ns == our_step_ns`.

  **Not to be confused with the step-derivation record.** `251` also
  appears at `logs_api_live.rs`'s "before this change we derived
  `span_ns / 250` NANOSECONDS" note and at
  `crates/pulsus-server/src/logs_api/params.rs`, which belong to issue
  #425 Part B — a different claim about a different quantity that
  happens to share the digits, and which has no ledger row because it
  restored parity rather than creating a divergence. A census that
  conflates the two reports a phantom duplicate.

  **What still overlaps, stated rather than removed.** The anchor's
  `our_step_ns` and `params.rs`'s "a 501 s window `2s`" record the same
  derived step for the same window, in prose and in an executable tuple
  at `params.rs`, and nothing binds them to this row. Removing the
  overlap would mean deleting a term from the step-derivation record to
  serve this one, which is the wrong direction. Four sites state the
  `501 s → 2 s` fact — this anchor, `logs_api_live.rs`'s case tuple,
  `params.rs`'s prose and `params.rs`'s executable tuple — and the
  binding covers the first two.
- **Why it is accepted.** The alignment is an implementation detail of
  **query splitting** surfacing in the API, not a documented contract:
  the reference splits a range query into hour chunks to run them in
  parallel, the chunks have to line up on a grid, and rather than
  resampling the result back onto the caller's timestamps it returns the
  chunk boundaries. One tenant limit switches it off
  (`split_queries_by_interval: 0` returns `h.next.Do(ctx, r)` before the
  splitter), so the same binary answers the same request two ways —
  which is what a scaling detail looks like, not a semantic. It also
  contradicts the reference's OWN engine (`pkg/logql` is start-anchored;
  that is the semantics issue #227 ported). Handing back data outside
  the window someone asked for is the reference being wrong, and the
  standing mandate is to match it **except where it is wrong**. The
  counter-argument — every stock deployment shows the aligned grid, so a
  migrated dashboard shifts — is about existing deployments; the stated
  goal is new ones, where points from your start, inside your window, at
  your step is what the documentation implies.

  **Measured on the CLIENT, issue #462 (closed on this), and it replaces
  the weaker framing above rather than sitting beside it.** The primary
  client already sends a step-aligned `start`, so under a stock
  deployment there is **no timestamp difference at all** — the whole
  divergence is the reference's one extra trailing point, which a time
  series panel clips off-canvas. Grafana **13.2.0**
  (`sha256:3fd54ae1214669f8355f065ec9f6445d5279a3d77095ab048ca045685272429b`)
  with its bundled datasource plugin **13.1.0**, one timeseries panel
  over a 6 h `now`-relative range, sends `start=1787947980000000000`,
  `end=1787969582798000000`, `step=10000ms`. `start % step == 0` and
  `end % step != 0`, so `floor(start, step) == start`: the two grids
  carry identical timestamps up to our last point at
  `1787969580000000000`. Our grid carries 2161 points; the reference's
  grid carries 2162 points, the extra one at `ceil(end, step)` —
  one further point at `1787969590000000000`, which sits 7.202 s past
  the `end` that was asked for, off the right-hand edge of the panel's
  canvas. So the migrated-dashboard counter-argument does not describe
  what a stock client actually sees: it sees the same points plus one it
  does not draw.

  <!-- grid-capture:start -->start=1787947980000000000 end=1787969582798000000 step_ns=10000000000 our_last=1787969580000000000 our_points=2161 ref_extra=1787969590000000000 ref_points=2162 past_end_ns=7202000000<!-- grid-capture:end -->

  **Limits of that capture, stated so the row is not read wider than it
  was measured.** The datasource's own query splitting never ran (one
  request per panel was observed), the `> 11000`-point resolution clamp
  never fired, and only a `now`-relative range query on a **timeseries**
  panel was captured — no absolute range, no other panel type, no split
  query, and nothing about `/query`. The capture is the coordinator's,
  recorded as such; the arithmetic above is re-derived from its three
  inputs by `the_grid_capture_anchor_recomputes_from_its_own_inputs`,
  which also reads each value out of the role phrase that gives it its
  meaning, so swapping the two timestamps or the two point counts reds.
- **Close condition:** the owner reverses the 2026-08-12 ruling. Nothing
  else reopens this; it is not waiting on a fix.
- **Pin:** `crates/pulsus-read/tests/logqltest/corpus/
  b9_range_sliding.test:48` — a range query from an UNALIGNED `T0`
  (`from 7s to 37s step 10s` ⇒ `17s 1  27s 1`), which fails the moment an
  aligned grid is reintroduced. Live: `logs_api_live.rs`'s
  `query_range_derives_the_reference_whole_second_step` asserts no point
  falls outside `[start, end]`. `deploy/e2e/loki.yaml` keeps
  `split_queries_by_interval: 0` so the differential's oracle answers the
  range query it was asked; the five `metric_range` cases stay `gated`.
- **Not to be confused with** `frontend-step-alignment` above, which
  records the same mechanism as an **oracle-config** note for the e2e
  differential. This row records it as a **product divergence** and
  carries the ruling that keeps it. Both stay: one explains why the
  oracle is configured as it is, the other why our answer differs from a
  stock reference.

### empty-value-oracle-version-skew (issue #259 reopen, oracle-version note — no case downgraded)

- **What diverges:** nothing in PulsusDB. This entry records that the
  logs differential's **oracle container is two minor versions behind
  the reference PulsusDB is built against**, and that the two versions
  disagree about one input class — an attribute or structured-metadata
  pair carrying an **empty value**. The e2e oracle is
  `grafana/loki:3.4.2@sha256:58a6c186…` (`deploy/e2e/compose.single.yaml`,
  buildinfo `3.4.2` / revision `4fa045d3`); the pinned log-ingest
  reference is `grafana/loki:3.7.4@sha256:87f0a067…`
  (`.github/workflows/ci.yml`, buildinfo `3.7.4` / revision `b318f282`).
  **3.4.2 keeps an empty value; 3.7.4 and PulsusDB drop it.**
- **Measured**, three stores, one probe run, both images given this
  repo's own `deploy/e2e/loki.yaml` and PulsusDB built from the branch
  and run against a throwaway ClickHouse database. Read back with
  `/loki/api/v1/query_range` and a `| label_format` stage, so structured
  metadata is flattened into the returned label set on every store:

  | seam | input | Loki 3.4.2 (e2e oracle) | Loki 3.7.4 (pinned reference) | PulsusDB |
  |---|---|---|---|---|
  | OTLP scope attribute | `emptyattr=""` | **kept** | dropped | dropped |
  | OTLP scope attribute (**control**) | `keepattr="kept"` | kept | kept | kept |
  | OTLP scope attribute (**control**) | `dup.key`/`dup_key` collision | `dup_key="v_us"` | same | same |
  | OTLP scope identity (**control**) | `scope.name="LOSE"` vs identity | `scope_name="coll-scope"` | same | same |
  | OTLP resource attribute | `emptyres=""` | **kept** | dropped | dropped |
  | OTLP resource attribute (**control**) | `keepres="kept"` | kept | kept | kept |
  | Loki-push structured metadata | `emptyattr=""` | **kept** | dropped | dropped |
  | Loki-push structured metadata (**control**) | `keepattr="kept"` | kept | kept | kept |
  | Loki-push stream label | `emptylbl=""` | dropped (merges into the label-less stream) | dropped | dropped |

  Every control row is identical on all three stores, so the probe
  harness is sound; the case rows split **3.4.2 versus everything else**
  on both ingest paths and on both OTLP attribute scopes. There is no
  seam, and no ingest path, on which v3.7.4 keeps an empty value.
- **The rule, from the source.** v3.4.2's distributor mutates an entry's
  structured metadata **in place** with no empty-value filter anywhere on
  the path (`pkg/distributor/distributor.go:552 @ v3.4.2` — direct
  assignment to `structuredMetadata[i].Name`). At v3.7.4 the same block
  routes through Prometheus' `labels.Builder`
  (`pkg/distributor/distributor.go:700` and `:722 @ v3.7.4`), whose
  `Reset` records the name of **every empty-valued base label** in `del`
  (`vendor/github.com/prometheus/prometheus/model/labels/labels_stringlabels.go:471-480
  @ v3.7.4`), so `Labels()` never emits it. The stream-label seam strips
  in both versions, which is why that row does not split.
- **Verdict — PulsusDB is correct and no ingest code changed for this
  entry.** v3.7.4 is the pinned log-ingest reference: roughly twenty
  committed tests, `docs/api.md` §8.2 and two ledger entries
  (`inadmissible-label-name-status`,
  `inadmissible-label-name-echo-escaping`) are written against it. A
  container digest in a compose file does not outrank that.
- **Consequence for the differential — the corpus, not the ingest path.**
  A shared corpus scored against BOTH stores cannot contain an input the
  two stores answer differently, so `logs_corpus::SCOPE_WITNESS_ATTRS`
  lost its fourth element `("emptyattr", "")` (it keeps the three
  collision-bearing attributes, which every store agrees on — control
  rows 3 and 4 above). The `scope_structured_metadata` case stays
  **`gated`** with its full set-equality comparison and names no `ledger`
  field; no case is downgraded and no other corpus input changed.
- **History:** `e2e-metrics-full` succeeded on 2026-08-05 and 08-06 and
  failed on every scheduled run from 08-07, the day `b54b542`
  ("ingest: strip empty labels and metadata where the reference strips
  them") merged. Artifacts of runs `31294900892` (08-09) and
  `31356879557` (08-10) both report the oracle at `raw 330 / matched 330
  / missing 0 / extra 0` and PulsusDB at `matched 329`, missing the
  single record whose labels include `emptyattr: ""` and extra the same
  record without it. The **cluster** variant fails identically even
  though it runs oracle-less (issue #204), because
  `wait_for_completeness` requires every present store to equal
  `corpus.expected_all_records()` — so an oracle-side-only fix would have
  left it red, and the corpus-side fix fixes both.
- **Guard.** `logs_corpus::tests::the_shared_corpus_carries_no_empty_valued_attribute`
  walks the corpus's actual OTLP export body at both scales and refuses
  any empty `stringValue`, naming the JSON path and this entry.
  `logs::tests::the_scope_case_construct_does_not_promise_an_empty_attribute`
  does the same for the shipped fixture's prose. **Boundary, stated so it
  is not over-read:** both cover exactly the empty-VALUE class — the one
  divergence measured here. Neither is a general 3.4.2-versus-3.7.4 skew
  detector, and nothing in this repo is.
- **CLOSE CONDITION.** This entry closes when
  `deploy/e2e/compose.single.yaml`'s Loki digest moves to v3.7.4
  (owner-scheduled; re-pinning re-scores all 39 gated cases in
  `test/fixtures/logs/differential.json` plus `sm_differential.json`
  against a new oracle and is only validatable in CI). Whoever does that
  makes exactly these edits **in the same commit**:
  1. restore `("emptyattr", "")` to `logs_corpus::SCOPE_WITNESS_ATTRS`;
  2. restore the expectation in
     `the_scope_witness_record_resolves_its_collisions_and_is_isolated`
     with `emptyattr` **absent** from the resolved SM map — v3.7.4's
     answer, not 3.4.2's, so the corpus's by-construction oracle keeps
     agreeing with both stores;
  3. delete `the_shared_corpus_carries_no_empty_valued_attribute` and
     `the_scope_case_construct_does_not_promise_an_empty_attribute`,
     which would otherwise block step 1, and drop this entry's needles
     from `the_oracle_version_skew_is_recorded_in_the_committed_ledger`.

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
- **Newly reachable through a WRAPPED variant's own pipeline (issue
  #397, measured 2026-08-10).** Until #397 a variant's own pipeline was
  discarded before evaluation, so this row could only be reached through
  the COMMON pipeline. A variant wrapped in a vector aggregation now runs
  its whole pipeline (the reference's own type switch,
  `pkg/logql/syntax/extractor.go:114 @ v3.7.4`), so an error raised
  there reaches the same surface. Measured on the pinned image with a
  seeded logfmt store, `S = {service_name="v397"}`:

  | query | reference | PulsusDB |
  |---|---|---|
  | `variants(sum by (env) (count_over_time(S \| json [5m]))) of (S[5m])` | `500 unexpected empty result` | `400 pipeline error: 'JSONParserErr'` |
  | the same wrapped variant BESIDE a clean one | `200`, silently dropping the erroring variant | `400 pipeline error: 'JSONParserErr'` |
  | `variants(sum by (__error__) (count_over_time(S \| json [5m]))) of (S[5m])` | `200` `{__error__="JSONParserErr", __variant__="0"} 6` | `400`, as above |

  Three behaviours for one condition on the reference side, the middle
  one being the worst: **answering 200 while silently discarding a
  variant is returning wrong data with nothing to indicate it**, the
  same failure class #397 exists to fix. We keep the 400. No new entry —
  this is the decision above reaching further, not a fresh one. Corpus
  rows W27 and W28 in `b13_variants.test` pin the first two.

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
- **(c) Per-variant series cap — CLOSED by #277, no divergence remains
  at instant.** *Reference:* applies `maxSeries` PER VARIANT and SKIPS
  the breaching variant with a warning, at HTTP 200. *PulsusDB:* the
  same. #236 landed the GRANULARITY and #277 landed the skip-and-warn
  plus the `warnings` response-envelope field, so a breaching variant is
  removed entirely, the rest of the query is served, and the response
  carries `"warnings":["maximum of series (500) reached for variant
  (0)"]` as its LAST top-level key.

  **A correction to what this entry used to claim.** It said "a
  3-variant query returning 3×400 series is served". That was **false at
  the API** until #277: a variants query plans to
  `Plan::MetricBinary(MetricNode::Variants{…})` and `exec.rs` then capped
  the CONCATENATION unconditionally, so #236's per-variant gate was
  shadowed and 1 200 result series were a 422. #277's
  `final_series_gate_applies` exempts a root `MetricNode::Variants` — the
  reference's own rule, which dispatches on the ROOT expression
  (`pkg/logql/engine.go:321-322 @ grafana/loki v3.7.4 b318f282`) — and
  `three_variants_of_four_hundred_series_each_are_served`
  (`crates/pulsus-server/tests/logs_variants_warnings_live.rs`) is the
  behavioural proof at the HTTP surface.

  The residual range boundary is entry **(d)**.
- **(d) The range boundary, and the two warnings we deliberately do not
  emit** (issue #277, adjudicated).

  **The divergence.** `multiVariantVectorsToSeries`
  (`pkg/logql/engine.go:473-508 @ grafana/loki v3.7.4
  b318f2829f0ae2094ab3a1e90780450e9e4b03be`) tests
  `len(sm[variantLabel]) >= maxSeries` **before** looking up whether the
  incoming point's series already exists, so a point belonging to an
  ALREADY-COUNTED series deletes the entire variant. Its own sibling
  `vectorsToSeriesWithLimit` (`:440-471`) has exactly the `!ok` guard
  this function is missing. Measured on the pinned container
  (`grafana/loki:3.7.4`, digest `sha256:87f0a067…`, buildinfo
  `3.7.4`/`b318f282`) over 501 `| logfmt` groups:

  | grid | 498 | 499 | 500 | 501 |
  |---|---|---|---|---|
  | instant | served | served | served | skipped + warning |
  | range, `60s→120s step 30s` | served | served | **skipped + warning** | skipped + warning |
  | range, `60s→60s step 30s` (one point) | — | served | served | skipped + warning |

  The single-point row is a **measured refinement** of the wording the
  #277 plan carried ("at a single step as well as many"): the skip needs
  at least TWO grid points, which is exactly what the missing `!ok` guard
  predicts, since the delete can only fire when a later point revisits an
  already-counted series. The reference's instant path serves 500, so its
  two paths disagree with each other.

  **PulsusDB applies `> 500` uniformly**, instant and range alike. The
  divergence is an **over-acceptance in the safe direction**: every query
  the reference serves, PulsusDB serves; PulsusDB additionally serves a
  range variant of exactly 500 series. Same class as `#236 (f)`.
  Corpus case 8 of `b21_variant_series_cap.test` asserts our verdict and
  keeps the reference's captured skip beside it as a `# reference:`
  annotation, so a later "parity fix" toward the off-by-one has to delete
  the evidence rather than overlook it;
  `range_variant_at_the_cap_is_served_where_the_reference_skips_it`
  (`crates/pulsus-read/src/logql/variants.rs`) reddens if our gate moves
  to `>= 500`.

  **The warnings PulsusDB does not emit — measured non-emissions, not
  omissions.** The reference's whole emission inventory is three message
  families (every `AddWarning`/`AddWarnings` call site under `pkg/`,
  tests and generated files excluded, is one of them; the wire array has
  no other source, because `metadata.Context`'s `warnings
  map[string]struct{}` is unexported and its package is a single
  non-test file with exactly two insertion statements, `context.go:84`
  and `:140`):

  1. `maximum of series (%d) reached for variant (%s)` — **implemented**,
     above.
  2. `maximum number of series (%d) reached for a single query;
     returning partial results` (`engine.go:542`, `:582`,
     `limits.go:512`) — **NOT emitted, and not implementable.** It is
     gated on the request header `X-Query-Tags:
     source=grafana-lokiexplore-app` (`pkg/util/httpreq/tags.go:108-128`,
     `constants.LogsDrilldownAppName`). Measured five times with that
     header over the 501-group dataset, it returned a DIFFERENT
     500-series subset each run — the dropped `id` was 184, 396, 370, 42
     and 303 — because `engine.go:541` truncates `vec[:maxSeries]`
     before any sort. (The #277 plan's own five runs, on a separate
     container from the same digest, dropped 461, 265, 387, 450 and 486:
     ten runs, ten different ids.) There is no stable rule there to
     match, only an unstable one to imitate. PulsusDB rejects that case
     with its existing `maximum number of series (500) …` error instead.
  3. `Query was executed using the new experimental query engine[ and
     dataobj storage.]` (`pkg/engine/basic_engine.go:265`,
     `pkg/engine/engine.go:298`, `pkg/engine/handler.go:532`) — **NOT
     emitted.** It is an announcement about the reference's own
     experimental dataobj engine; emitting it from PulsusDB would be a
     false statement about our implementation.

  `the_reference_warning_inventory_is_three_families`
  (`crates/pulsus-read/tests/warning_inventory.rs`) pins this decision
  and parses these two non-emission records out of this entry, so
  deleting them reddens.

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

  **Issue #398 adds a member to this same status family and opens no new
  row.** A read that exhausts ClickHouse's memory is now refused
  `422 query_too_broad` on all three read surfaces
  (`reader.{logql,promql,traceql}_read_max_memory_bytes`, server code 241)
  rather than answering `500` with the raw server exception. That is the
  same "we could not afford this query" class as the result-size cap above
  and inherits its 400-vs-422 delta against Loki. It needs no ledger row of
  its own, and — this is the part worth recording — **the other two
  surfaces need no row at all**, because their references agree with us.
  Measured against containers with data in the store while planning #398:

  | reference | condition | status |
  |---|---|---|
  | Loki v3.7.4 | `max_query_series: 1` on `sum by (…)(count_over_time(…))` | **400** |
  | Loki v3.7.4 | `max_entries_limit_per_query: 2` at `limit=5` | **400** |
  | Loki v3.7.4 | `max_query_bytes_read: 1B` | **400 — SOURCE-DERIVED, NOT MEASURED** (the limiter ran but index stats report `0B` on that corpus, so it could not trip; status read from `pkg/querier/queryrange/limits.go:405 @ v3.7.4`). Do not promote this row to a measurement. |
  | Prometheus v3.13.0 | `--query.max-samples=1`, instant **and** range | **422** `execution` (`web/api/v1/api.go:2236-2237 @ v3.13.0`) |
  | Tempo v3.0.2 | `max_bytes_per_trace: 1000`, `GET /api/traces/{id}` | **422** (`modules/frontend/combiner/trace_by_id.go:99`, `modules/querier/http.go:398`) |

  So PulsusDB's 422 matches Prometheus and Tempo on this condition and
  diverges only from Loki, inside this entry's existing family.

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
issue #230; 699 container-captured corpus directives — 689 `eval`
(60+228+34+29+258+80 across `tests/logqltest/corpus/t1…t6_*.test`) +
10 `eval_fail` reject-parity cases (all in t1) — replay byte-exact
hermetically, including execution-error strings. (Issue #294 added the
last of the t6 rows: the `duration`/`duration_seconds` failures over
invalid-UTF-8 arguments, whose quoted halves were `\xef\xbf\xbd`
escapes before that issue made the parse read raw bytes.) (The pre-round-2
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
  the same `error parsing regexp: ` prefix. The error CLASS
  (`TemplateFormatErr`) and position prefix are byte-exact. Kept out
  of the corpus; hermetically gated instead.

  **Correction (issue #246, measured 2026-08-08).** This bullet used to
  claim the two engines agree about which patterns they accept, on the
  ground that both are RE2-class. They do not agree, and being RE2-class
  is not the same as agreeing. Driven through
  `{{ regexReplaceAll <pattern> .app `z` }}` over a line whose `app` is
  `x` — the pattern passed as a Go template RAW string, because a
  double-quoted template literal makes Go's own scanner refuse `\Q`,
  `\p` and `\u{` before the regex engine sees them — **18 of 20 probes
  disagree** on the pinned `grafana/loki@sha256:87f0a067…` v3.7.4 oracle
  vs this tree. Nine render there and raise `TemplateFormatErr` here
  (`a{bbb}c`, `\Qa*\E`, `\101`, `(?ss:ab)`, `(?)a`, `a{,5}`, `a{}`,
  `a(?i){2}`, `(?P<n>a)(?P<n>b)`); nine raise `TemplateFormatErr` there
  and render here (`\p{Alphabetic}`, `a{1001}`, `a**`, `[[:foo:]]`,
  `\U0001F600`, `\u{263A}`, `(?x)a`, `(?R)a`, `[a--b]`). The two
  agreements are `a.*b` and `(`.

  **Two of those are wrong ANSWERS, not lenient accepts**: `a**` renders
  **`zxz`** from the input `x`, because the Rust crate reads it as
  `(a*)*` and replaces at every position, and `\p{Alphabetic}` renders
  **`z`**, because that property exists in the crate's UCD tables and
  not in RE2's fixed set, so it matches `x`. The reference answers
  neither — it raises `TemplateFormatErr` for both.

  The divergence classes are the same ones the query-side surface has;
  they are enumerated in `logql-regex-accept-surface-divergence` below
  and owned by **#400**. The witness is
  `crates/pulsus-read/tests/logql_regex_accept_matrix.rs`
  (`the_template_regex_boundary_does_not_match_the_reference`, hermetic,
  plus `live_template_axis_against_the_reference`, which re-measures the
  reference half by pushing one line and reading `__error__` back).
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
- **Issue #294 — the error text is charged, and the parse moved to raw
  bytes.** The gate's ordering leg used to admit a shape only when it
  returned a retainable `Ok`, so **every branch that allocated and then
  returned a scalar or an error sat outside it at any size**. Admitting
  on ALLOCATION instead turned 31 shapes across 22 registry functions
  red on `df4bdbd`, all one mechanism: caller bytes converted or copied
  inside a template function with no charge covering the copy. The
  fixes: `duration`/`duration_seconds`/`unixToTime` parse the RAW bytes
  and charge the EXACT rendered length of their failure before building
  it; `cast_to_i64`/`cast_to_f64` borrow instead of repairing (a repair
  inserts U+FFFD, which neither Go parser accepts, so the value cannot
  move); `toDateInZone` borrows all three arguments; `bytes` keeps its
  repair but pays for it through the charged conversion. The 31 are
  sealed by name in `logql_template_alloc_gate.rs`'s
  `WERE_RED_ON_DF4BDBD`, and the six error shapes additionally satisfy
  `charged == allocated == err.len()` — an equality, not a bound.

  **A byte-level parity divergence closed in passing.** Repairing the
  argument before parsing it also repaired what the message QUOTED.
  Measured on `grafana/loki:3.7.4`
  (`sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`,
  2026-08-26) against `df4bdbd`:

  | query | reference | `df4bdbd` |
  |---|---|---|
  | `{{ duration (b64dec "/////////////w==") }}` (10 B) | `time: invalid duration "\xff"`×10 | `"\xef\xbf\xbd"`×10 |
  | `{{ duration (b64dec "YYCA") }}` (3 B) | `time: invalid duration "a\x80\x80"` | `"a\xef\xbf\xbd\xef\xbf\xbd"` |
  | `{{ duration (b64dec "4IA=") }}` (2 B) | `time: invalid duration "\xe0\x80"` | `"\xef\xbf\xbd\xef\xbf\xbd"` |
  | `{{ duration (b64dec "MTL/") }}` (3 B) | `time: unknown unit "\xff" in duration "12\xff"` | both halves U+FFFD |
  | `{{ unixToTime (b64dec "//8=") }}` (2 B) | quoted half `parsing "\xff\xff"` | quoted half U+FFFD |

  All now match. The four `duration` rows are captured into
  `t6_errors_edges.test`; the `unixToTime` row is **not** — its `%v`
  half carries a raw invalid byte the reference serves and a
  `.test` label value cannot, so it is asserted in
  `tests/logql_template_engine.rs`
  (`unix_to_time_quotes_the_raw_argument_and_repairs_only_the_percent_v_half`),
  which pins both halves on one string. A genuine U+FFFD still escapes
  as its three bytes (`Ye+/vWI=`, 5 B) — Go's `time.quote` escapes
  `width` bytes, which is 1 for a decode error and 3 for a real
  replacement character.

  **The accept surface moved, and it moved inside the reference's own
  failure region.** Charging the error text means a big enough argument
  now refuses the query. Measured on the same container, one 80-byte
  line, argument grown in-template by `{{ duration (repeat N __line__) }}`:

  | argument | reference | mechanism |
  |---|---|---|
  | 1,048,560 B | **200**, `__error_details__` = 1,048,685 B | served |
  | 2,097,120 / 4,194,240 / 8,388,560 B | **500** `rpc error: code = ResourceExhausted … max (… vs. 4194304)` | gRPC send cap, **configurable**; its operand is not stable between runs and is deliberately not pinned |
  | 16,777,200 B | **500** `error while processing request: String too long to encode as label.` | recovered panic — `sizeWhenEncoded` refuses `> 1<<24`, `vendor/github.com/prometheus/prometheus/model/labels/labels_stringlabels.go:543-549 @ v3.7.4` |
  | 33,554,400 B | **500** same panic | as above |

  Our first refusal for that template lands at 419,430 repeats
  (33,554,400 B) and 419,429 is still served — pinned both ways by
  `a_thirty_two_mib_duration_argument_is_the_bounded_422_not_a_served_error_detail`.
  Every argument our `422` refuses is one the reference answers with a
  `500`; the `1<<24` label cap is not tunable, so that row alone carries
  the claim.

- **Table `template-output-budget/query_range`. Issue #455 — the
  substitution, and what is left after it.** This
  bullet REPLACES the "two U+FFFD divergences #294 did NOT change"
  account that stood here; that account described a state that no longer
  ships, and leaving it beside this one would leave a reader to pick.

  Two changes, and they compose. The template layer's invalid-UTF-8
  repair now follows **Go's** granularity — one U+FFFD per invalid
  BYTE, `utf8.DecodeRune`'s advance, measured against go1.25.5 as
  `utf8.DecodeRuneInString("\xe0\xa0'")` -> size `1` — instead of
  `String::from_utf8_lossy`'s one-per-maximal-invalid-subsequence, a
  rule with no counterpart anywhere on the reference's path. And the
  streams response marshaller maps **every U+FFFD rune in a stream LABEL
  value to one space** (`0x20`), which is what
  `pkg/util/marshal/query.go:25-32 @ v3.7.4` does with `removeInvalidUtf`
  ("The rune error replacement is rejected by Prometheus hence replacing
  them with space"), applied by `bytes.Map` in `NewStreams` at `:92-93`.
  Log LINE values are not touched, on either side.

  Together they mean **our `__error_details__` label can no longer
  contain a U+FFFD at all**. Where the reference's own slot already held
  U+FFFD — the `bytes` family, via `strings.ToLower` — we are now
  byte-identical to it. Where the reference's slot holds RAW invalid
  bytes — the `%v` family, `unixToTime` — we cannot be, and that is the
  divergence recorded below.

  Measured 2026-08-27 on
  `docker.io/grafana/loki@sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`
  (`/loki/api/v1/status/buildinfo` -> `3.7.4` / `b318f282`), **route
  `/loki/api/v1/query_range`, window `start = NS - 1h` / `end = NS + 1h`,
  `direction=backward`, `limit=100`**. Values are the hex of the
  `__error_details__` span, LABEL surface unless the row says ENTRY:

  | query | reference | PulsusDB |
  |---|---|---|
  | `{{ bytes "12\xe0\xa0" }}` — tail after `unhandled size name: ` | `2020` | `2020` |
  | `{{ bytes (b64dec "MTL/") }}` — same tail | `20` | `20` |
  | `{{ unixToTime "\xe0\xa0" }}` — span between `time '` and `':` | `e0a0` | **`2020`** |
  | `{{ unixToTime (b64dec "//8=") }}` — same span | `ffff` | **`2020`** |
  | `… \| line_format` echoing the pair, ENTRY, `unixToTime` | `5c75666666645c7566666664` | `efbfbdefbfbd` |
  | `… \| line_format` echoing the pair, ENTRY, `bytes` | `efbfbdefbfbd` | `efbfbdefbfbd` |
  | `x{{ "\ufffd" }}y`, ENTRY | `78efbfbd79` | `78efbfbd79` |
  | `label_format foo=` + `{{ "\ufffd" }}` — an ORDINARY label | `20` | `20` |

  **How to re-measure every cell above.** Push ONE line `hello world`
  under `{app="foo"}` at an instant `NS`, wait for it to be readable,
  then `GET` the route above with the window, direction and limit named
  above and one of:

  ```logql
  {app="foo"} | line_format `<template>`
  {app="foo"} | line_format `<template>` | line_format `Error: {{.__error__}} - {{.__error_details__}}`
  {app="foo"} | label_format foo=`{{ "\ufffd" }}`
  ```

  where `<template>` is the cell's first column; the second shape is what
  the two ENTRY rows use, and the third is the ordinary-label row.

  **`NS` itself is not a condition for this table: it is simply `now`, no
  value here depends on where it falls, and no compared span contains
  it.** That is the one anchor exemption in this ledger, and
  `streams-split-merge`'s anchor DOES matter and is stated there.

  The two ENTRY rows differ only in JSON escaping: the reference writes
  `\ufffd` as six ASCII bytes for the `unixToTime` echo and raw UTF-8 for
  the `bytes` one; both decode to two U+FFFD, as ours do.

  **What is left, and it is a type constraint, not a preference.** The
  `%v` half of `unable to parse time '%v'` reaches the client as raw
  `e0 a0` from the reference and as two spaces from us. It cannot be
  matched: the value flows through `ExecError.msg`, a `String`, into
  `ErrorSlots::details`, a `Cow<'a, str>`, and neither can hold a byte
  that is not valid UTF-8. Two consequences worth stating rather than
  burying — our response becomes **valid UTF-8** where the reference's is
  not, and the **character count now matches** (two spaces against two
  raw bytes) where under the previous single-U+FFFD rule it did not.

  The reference's `500 could not write JSON response: 1:51: parse error:
  invalid UTF-8 rune` for `{{ bytes (b64dec "MTL/") }}`, recorded here
  before with no route, comes from its OTHER stream-encode path:
  `encodeStream` re-parses the stream labels with Prometheus' lexer
  (`pkg/util/marshal/query.go:416 @ v3.7.4`) and `marshal.go:60` wraps
  the rejection. On `/loki/api/v1/query_range` it answers `200`. **We
  never reproduce the `500`** — a valid query with a well-formed result
  refused at serialisation by a label lexer that has no business there is
  the "except where they are wrong" case.

  The `bytes` arm's charge is unchanged and still an over-charge of up to
  4x on the two arms that embed the argument verbatim
  (`4*len + 64` before converting; before review round 1 a 1 MiB numeric
  overflow allocated 3,145,761 B and a 1 MiB unknown unit 3,145,807 B
  with nothing charged), and it moves the accept surface the boundary
  table above describes, one quarter as far out.

  **Table `template-output-budget/tail`. The substitution is scoped to
  the QUERY response, and the repair is
  not — measured on `/loki/api/v1/tail`, both.** The two changes have
  different reach and it would be wrong to describe them as one.
  `render_stream_item_into` is shared with the tail frame encoder, so the
  caller now names the rule it wants; `/api/logs/v1/tail` and its
  `/loki/api/v1/tail` alias keep their label bytes verbatim. The repair
  rule cannot be scoped that way and is not: it runs in the read
  pipeline, before any encoder, so every route that renders a template
  carries it.

  **How to re-measure the tail table below.** Route `/loki/api/v1/tail`
  (its `/api/logs/v1/tail` sibling is the same handler), against
  `docker.io/grafana/loki@sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`
  — spelled out here rather than borrowed from the table above, because a
  table that leans on its neighbour's conditions is a table that stops
  being re-measurable the moment the neighbour is edited. **Anchor:** `NS` is the most recent half-past-the-hour at
  least 30 s in the past — `NS = (now / 1h) * 1h + 30m`, minus a further
  `1h` when that lands later than `now - 30s`. **Seed:** ONE line
  `tailprobe` under `{app="tf1"}`, whose sample timestamp is exactly
  `NS`. **Window and parameters:** `start = NS - 60s`, `delay_for=5`,
  `limit=1000`. **Queries**, one per row:

  ```logql
  {app="tf1"} | label_format k=`{{ "\ufffd" }}`
  {app="tf1"} | line_format `{{ unixToTime "\xe0\xa0" }}`
  ```

  Take the FIRST text frame. **The byte counts are the frame prefix up to
  `],"dropped_entries"`** — the stream array and nothing after it —
  because `dropped_total` is a running counter and no claim here is about
  it. A fixed anchor and a fixed line are what make the three columns
  byte-comparable rather than merely similar; without them two captures
  differ in their timestamp and no byte count means anything.

  | tail case | before (`ececfc2`) | this change | reference |
  |---|---|---|---|
  | a U+FFFD in a stream label | `"k":"efbfbd"`, frame served, 114 B | **byte-identical**, 114 B | **no frame**: `101`, then close `1011` `could not write JSON tail response: 1:41: parse error: invalid UTF-8 rune` |
  | `{{ unixToTime "\xe0\xa0" }}` in `__error_details__` | `parse time 'efbfbd'`, 340 B | `parse time 'efbfbdefbfbd'`, 343 B | `parse time 'e0a0'`, raw bytes |

  The second row moves and the reason is the repair rule alone; it moves
  the tail label's rune count from one to two, which is the reference's
  count, exactly as on `query_range`. The residual is the same type
  constraint.

  The first row is why the substitution stops at the query response.
  **The reference does not substitute on tail either** — it refuses the
  frame, with the same Prometheus-lexer rejection its `encodeStream` path
  raises as a `500`, which is the "except where they are wrong" case we
  already decline to reproduce. So matching it there would mean either
  killing a stream or applying a rule whose only evidence comes from a
  route where the reference runs different code (`NewStreams`). Neither
  is decidable from what this issue measured; the bytes stay put until
  something measures them. `dropped_entries` labels in the same frame
  were already spliced verbatim and still are.

  Held still by `logs_api::encode`'s
  `tail_frames_keep_their_stream_label_bytes_verbatim`, which asserts
  both halves on ONE `StreamResult` — verbatim through the tail frame,
  substituted through the query response — so an encoder that stopped
  substituting everywhere cannot pass it.

  Pinned by `tests/logql_template_engine.rs`
  (`the_two_recorded_utf8_divergences_render_as_they_did_before_this_issue`,
  `unix_to_time_quotes_the_raw_argument_and_repairs_only_the_percent_v_half`)
  at the render layer, and by
  `crates/pulsus-server/tests/logs_utf8_substitution_live.rs` at the
  wire, against the digest above.

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

### `streams-split-merge` (issue #455 — the reference is internally inconsistent; we match the branch we structurally resemble)

- **What collides.** Once a U+FFFD in a stream label becomes a space
  (see `template-output-budget`), two label sets that differed ONLY in
  that character become identical. What the reference does with the pair
  depends on how many query splits the request was cut into.
- **Table `streams-split-merge/collision`. Reference behaviour,
  measured** 2026-08-27 on
  `docker.io/grafana/loki@sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`
  (`/loki/api/v1/status/buildinfo` -> `3.7.4` / `b318f282`), **route
  `/loki/api/v1/query_range`**, `direction=backward`, `limit=100`.

  **How to re-measure the table below.** **Anchor:** unlike
  `template-output-budget`'s table, `NS` here IS a condition — the split
  boundary is absolute-time aligned, so a window's placement decides
  which branch of the reference answers. `NS` is the most recent
  half-past-the-hour at least 60 s in the past — `NS = (now / 1h) * 1h +
  30m`, minus a further `1h` when that lands later than `now - 60s` —
  which puts a `±15m` window inside one wall-clock hour. **Seed:** stream
  `{app="c1"}` carries three lines, at `NS`, `NS+10ns` and `NS+11ns`;
  stream `{app="c2"}` carries one, at `NS+1ns`. **Query:**

  ```logql
  {app=~"c1|c2"} | label_format k=`{{ if eq .app "c1" }} {{ else }}{{ "\ufffd" }}{{ end }}` | drop app, service_name, detected_level
  ```

  Swapping which branch emits the U+FFFD is the ordering check described
  further down. The `splits` column is read from the reference's own
  `data.stats.summary.splits`, never assumed.

  | window | reference `splits` | reference | PulsusDB |
  |---|---|---|---|
  | `start = NS - 15m` / `end = NS + 15m`, placed inside one wall-clock hour | `0` | **two objects**, both `{"k":" "}` | **two objects**, both `{"k":" "}` |
  | `start = NS - 1h` / `end = NS + 1h` | `> 0` | **one merged object** `{"k":" "}` carrying all four entries | **two objects**, both `{"k":" "}` |

  **The wide row's count is a `> 0`, not a number, and that is the point
  of the paragraph below.** Two 1 h cases in ONE run reported `2` and `3`
  — same width, different placement. Recording either as *the* value
  would be a figure a reader could not reproduce; what is stable is the
  branch, and the branch is what both the table and the test assert.

- **The reference is internally inconsistent here.** It emits two objects
  below its split boundary and one merged object above it, so no single
  behaviour matches it at every window. **We match its unsplit branch
  because that is our structural configuration**: `git grep -nE
  'split_queries_by_interval|split_interval' crates/pulsus-read/src/logql/`
  returns nothing, our read path has no split step, and our stats object
  carries no `splits` field at all. The reference's `splits=0` row IS our
  structural analogue; matching it is matching the reference in the
  configuration we actually resemble, not choosing a convenient half.
  Merging instead would require inventing an ordering rule over
  information the substitution has already destroyed, which is a second
  implementation rather than parity.
- **Where the boundary comes from, and why no test may encode it.**
  `GET /config` on the running container reports
  `split_queries_by_interval: 1h` — its shipped default, not raised by
  `ci/logql/config.yaml`. The boundary is **absolute-time aligned, not
  width-aligned**: a ten-window sweep gave `splits` of
  `0,0,0,0,0,2,3,5,13,49`, and a later run with a fresh seed gave
  `splits=2` at `h=15m` where an earlier one gave `0` — same width,
  different placement. **A test that encoded an `h -> splits` mapping
  would fail on the clock**, not merely against someone else's config.
  So `logs_utf8_substitution_live.rs` places its 15-minute windows inside
  a single wall-clock hour, asserts the `splits` the reference REPORTS for
  each window it actually sent, and pins no mapping.
- **What this row is, stated so it is not read as more.** It is prose,
  and **nothing checks it** — see this ledger's header for the four
  mechanisms that tried and the reason none can. What DOES measure the
  numbers is the live differential
  (`crates/pulsus-server/tests/logs_utf8_substitution_live.rs`), which
  puts both windows to the reference in one run and compares the answers;
  it asserts the behaviour, not this table's description of it. A checker
  that appeared to validate the table would have been worse than none,
  and for four rounds that is exactly what one was.
- **Two readings that cannot be told apart, so neither is claimed.** When
  two label sets collide, "first in pre-substitution sort order wins" and
  "the substituted value loses" are **indistinguishable by construction**:
  U+FFFD begins `0xEF` and a space is `0x20`, so a substituted value
  always sorts AFTER the literal-space one it collides with. The other
  reading is untested, not wrong, and no code here implements it.
- **Object order, and a claim withdrawn twice.** Below the boundary the
  two objects order by the **pre-substitution** label set, and swapping
  which stream carries the U+FFFD **reverses** them — measured on two
  fixtures in both directions, on both sides.

  Above the boundary, where the reference merges, an earlier revision of
  this account claimed the merged block order follows the **original
  stream label** (`c1` before `c2`, `a2` before `z1`). **That is
  withdrawn: it is falsified by a probe built to discriminate it.** With
  a stream `aaa` holding three OLD lines and a stream `zzz` holding one
  NEWER line, the merged object came back `zzz-new, aaa-3, aaa-2, aaa-1`
  — `zzz` first, though `aaa` sorts first and holds more entries — and it
  did not change when the U+FFFD moved between them. What the measurement
  supports is that the blocks follow **each block's newest entry under
  the query's `direction`**, and the mechanism (a block-level sort versus
  per-split concatenation) was **not** separated. We emit two objects at
  every window, so we reproduce none of this; it is recorded because the
  previous sentence was wrong and a reader deserves the correction rather
  than its absence.

  The cause of the earlier error is worth more than the conclusion: it
  came from **comparing two captures taken against two different
  fixtures** — one before two lines were added to a stream, one after.
  Each capture was individually correct, which is why it looked
  confirmed. **This is the same defect as Q10**, the control query one
  round earlier whose expected answer had quietly stopped matching its
  own seed after those two lines were added — caught there, missed here.
  An expected answer compared against a fixture it no longer matches is
  invisible until someone runs it. The defence is to re-derive both sides
  on the same seed in the same run, which is why every case in
  `logs_utf8_substitution_live.rs` seeds its OWN base, asserts that base
  in its own response, and is listed in a committed inventory the test
  checks by set equality.
- **Why deliberate:** owner ruling, 2026-08-27. Not merging needs no new
  code, keeps the row we already match byte-identical, and avoids
  inventing aggregation semantics the reference does not have.

### `json-nonvalidating-scan-residual` (issue #389, measured residual — the record, not a fix)

- **What issue #389 CLOSED, so this row is not read as covering it.**
  A line with bytes after a COMPLETE JSON value is now parsed as the
  reference parses it, at all of `| json`, `| json <id>="<path>"` and
  `| unpack`; a targeted extraction that lands on an object or array
  now hands back the document's own bytes. Those are parity, gated by
  `b23_json_raw_read.test` and `tests/logql_pipeline_golden.rs`. What
  remains is the class below, which is a different mechanism.
- **Reference behaviour.** grafana/loki v3.7.4 reads a log line with
  `jsonparser`, a scanner that never validates the document. `EachKey`'s
  byte dispatch has no default case
  (`vendor/github.com/grafana/jsonparser/parser.go:568-577`) and
  `ObjectEach` returns the moment it reaches the closing brace
  (`:1108-1112,1155-1160`), so the reference extracts from a line that is
  malformed AFTER — or beside — the part it needed. PulsusDB reads the
  line with `serde_json`, which validates the whole first value, so a
  byte we cannot parse costs us everything downstream of it.
- **Measured, on `grafana/loki:3.7.4` digest
  `sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`
  (buildinfo read from the running process: version 3.7.4, revision
  b318f282), 2026-08-09:**

  | line | query | reference | PulsusDB |
  |---|---|---|---|
  | `{"o":{"z":1}x,"a":2}` | `\| json` | `o_z="1"` **and** the error pair | the error pair alone |
  | `{"o":{"z":1}x,"a":2}` | `\| json v="a"` | `v="2"` | `v=""` |
  | `{"o":{"z":1}x,"a":2}` | `\| json o="o"` | `o="{"z":1}"` | `o=""` |
  | `{"o":["a\qb"],"a":2}` | `\| json` | `a="2"`, no error at all | the error pair, no label |
  | `{"o":["a\qb"],"a":2}` | `\| json v="a"` | `v="2"` | `v=""` |
  | `{"o":["a\qb"],"a":2}` | `\| json o="o"` | `o=""` | `o=""` — the bound: where the malformed part is INSIDE the selected span, both sides answer the empty string |

- **Why deliberate, for now.** Closing it means writing a
  non-validating JSON scanner of our own; nothing short of that reaches
  the class, because every route through `serde_json` validates. That
  same scanner is the only affordable route to the OTHER open residual
  in this area — emitting a number's raw lexeme rather than a
  re-rendering (`1.500` → `1.5`), whose read-side gate
  (`crates/pulsus-read/tests/logql_json_float_roundtrip.rs`) lives on
  issue **#270** and would pass BY CONSTRUCTION once the lexeme is
  emitted, so that gate has to be re-established on a surviving
  observable before anyone changes the emit. **Issue #389 stays open to
  carry both**, with the three cheaper designs that were measured and
  rejected recorded on it; this row is the record of what remains, not
  a plan. Gated by `b23_json_raw_read.test`'s `jr-mid-obj` and
  `jr-mid-esc` rows, each carrying
  `# provenance: divergence(json-nonvalidating-scan-residual)`.
- **A second, opposite cell in the same area, and it turns on the
  reference's own config: `| json o="o"` over `{"o":{"k":"x\u{FFFD}y"}}`
  — a raw U+FFFD inside the extracted object's bytes.** The object arm
  copies the span verbatim (`readValue`'s `case jsonparser.Object`,
  `pkg/logql/log/parser.go:700-706`), so the rune reaches the label
  value; what happens next is the response path's, and the two paths
  disagree. Isolated by A/B on the pinned image, changing one setting at
  a time:

  | reference config | answer |
  |---|---|
  | default (JSON) frontend encoding | `HTTP 500 could not write JSON response: 1:4: parse error: invalid UTF-8 rune` |
  | `frontend.encoding: protobuf` (what `ci/logql/config.yaml` sets) | `200`, `o="{"k":"x y"}"` — U+FFFD mapped to a space |
  | `discover_log_levels` either way | no effect (probed both) |

  So the reference either fails to answer its own query or rewrites the
  bytes it just promised to copy, depending on a transport setting.
  PulsusDB answers `200` with the bytes in both cases. The `200` half is
  a deliberate non-replication of a reference defect under the standing
  "match it except where it is wrong" rule; the space-mapping half is
  the SAME divergence as the scalar one in the last bullet — the
  reference maps U+FFFD out at the response boundary
  (`pkg/util/marshal/query.go:87-100`, `NewStreams`), which is not the
  parser's arm and is not this issue's. The ARRAY arm needs none of
  that: `unescapeJSONString` maps the rune itself (`:44-49,278-281`), so
  `{"o":["x\u{FFFD}y"]}` gives `["x y"]` on both sides under either
  config, and that row IS in the corpus. No corpus row can gate the
  object cell — under one config the reference cannot answer the query
  at all.
- **Not in this row, and not this issue's:** `| unpack` emitting
  `| json`'s error-detail constant (the reference has
  `expecting json object(6), but it is not` for `unpack`'s non-object
  gate, measured on the same container), and U+FFFD in a SCALAR value
  not being mapped to a space (`{"a":"x\u{FFFD}y"}` is `a="x y"` there
  and `a="x\u{FFFD}y"` here). Both were measured in the same run and
  both are owned elsewhere.

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
  close frame still truncates reasons at 123 bytes.) **This covers the
  inner regex-compiler text too** (issue #246, owner ruling 2026-08-08 —
  no translation table): where the reference embeds Go's
  `error parsing regexp: <code>: `<expr>`` PulsusDB embeds the Rust
  crate's diagnostic, and byte parity there is unreachable for two
  STRUCTURAL reasons, not for want of effort. First, Go's `Error.Expr`
  is the offending **sub-token** rather than the pattern
  (`vendor/github.com/grafana/regexp/syntax/parse.go:16-22 @ v3.7.4`
  builds the message from `Code` plus `Expr`) — measured, `{app=~"[z-a]"}`
  answers ``invalid character class range: `z-a` `` and `{app=~"a**"}`
  answers ``invalid nested repetition operator: `**` `` — so reproducing
  it means reproducing that parser's cursor, i.e. the port refused on
  #331. Second, `label_replace` quotes the **anchored** form where every
  other call site quotes the bare one, so even the quoted-expression rule
  is per-site. Nothing branches on the text: Loki v3.7.4's own four
  non-vendor occurrences of `error parsing regexp` are all in its
  `_test.go` files. The STATUS is `400` on both sides at every point of
  the surface, pinned by
  `crates/pulsus-read/tests/logql_regex_accept_matrix.rs`.
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
  builders (#286). AC7's gates are Rust tests rather than corpus rows
  because at the time the corpus runner did not execute a pushed-down
  line filter at all (**#278**, since closed); they were not moved
  afterwards, since a Rust test states that property more directly than
  a corpus row would.

## Issue #291 — the regex compile budget

### `regex-compile-budget` (issue #291, owner ruling 2026-08-09 — a deliberate limit, and a deliberate narrowing)

- **The defect this closes.** `regex::RegexBuilder::size_limit` is
  `nfa_size_limit` (`regex-1.13.0/src/builders.rs:184-187`) and bounds the
  compiled PROGRAM, which is the last of three compile phases. Nothing in
  the crate bounds the phase before it. Measured on this tree with a
  counting global allocator, peak live bytes, `regex::Regex::new`:

  | pattern | pattern bytes | AST parse | HIR translate | whole compile |
  |---|---|---|---|---|
  | `a`×131071 | 131,071 | 9.44 MB | 0.26 MB | 29.36 MB, `Ok` |
  | `\w`×64 | 128 | 4.8 KB | 0.43 MB | 10.62 MB, `Ok` |
  | `\p{L}`×20000 | 100,000 | 2.78 MB | **114.64 MB** | 128.73 MB, `Err` |
  | `\w`×65535 | 131,070 | 4.72 MB | **432.01 MB** | 445.23 MB, `Err` |
  | `(?i)\p{L}`×20000 | 100,004 | 2.78 MB | **872.88 MB** | **886.98 MB, `Err`** |

  The last row is the shape of the problem: a **100 KB** query, inside
  #279's 131,072-byte text cap, allocating **887 MB** on its way to a
  `400`. Sweeping `size_limit` on a fixed pattern proves the knob is the
  wrong one — `(?i)\p{L}`×170 (854 B) peaks 7.75 MB at `size_limit(4 KiB)`
  and 28.74 MB at the 10 MiB default: cutting the limit 2,560× cuts the
  peak 3.7×, and the pattern is refused at every value.

- **The limit.** One entry point, `pulsus_re2::compile_user_regex`, used
  by all nine user-pattern compile sites across LogQL, PromQL and TraceQL.
  Before compiling, it bounds the allocation compiling would take, from
  the pattern's own AST, and refuses at **96 MiB** with `400 bad_data` and
  the message `expression too large`. `400` and not `422` because the
  refusal is decidable from the query text alone, before any data is
  touched — the same class as `five-year-span-cap`; a budget discovered
  while executing against data is the `422` class (`template-output-budget`,
  `variants-label-collision-and-fanout-bounds`).

- **Reference behaviour, MEASURED** on the digest-pinned v3.7.4 oracle
  with a populated store (an empty one answers `200` to almost anything),
  and from the reference's source. Loki, Prometheus and Tempo all parse
  user regexes with **`github.com/grafana/regexp/syntax`** — Loki and
  Prometheus by vendoring it, Tempo through
  `prometheus/model/labels.NewFastRegexMatcher`, which vendors the same
  fork (`vendor/github.com/prometheus/prometheus/model/labels/regexp.go:22,67
  @ tempo v3.0.2`). It carries three limits the Rust crate has no
  counterpart for: `maxHeight = 1000`, `maxSize = 128<<20 / 40` and
  `maxRunes = 128<<20 / 4`, raised as `ErrLarge` / `expression too large`
  (`vendor/github.com/grafana/regexp/syntax/parse.go:47,93,102-103,122-123,161-163,206-207
  @ loki v3.7.4`; Go's own standard library carries the identical
  constants at `src/regexp/syntax/parse.go:94,103,123`). So all three
  references bound this the SAME way, which is why one cap serves all nine
  sites.

  | probe | reference | PulsusDB |
  |---|---|---|
  | `\p{L}`×200 (1,000 B) | `200`, 49 ms | `200` |
  | `(?i)\p{L}`×1000 | `200`, 1.33 s | `400` |
  | `\w`×20000 | `200`, 35 ms | `400` |
  | `\w`×40000 | `200`, 62 ms | `400` |
  | `\p{L}`×20000 | `200`, 2.31 s | `400` |
  | `(?i)[\p{L}×20000]` | `200`, 1.26 s | `400` |
  | `(?i)\p{L}`×20000 | **no answer in 50 s**, not rejected | `400` |
  | `\p{L}\|…`×10,013 atoms | `200` | `200` — the last size WE accept |
  | `\p{L}\|…`×10,014 atoms | `200` | `400` `expression too large` — the band opens |
  | `\p{L}\|…`×12,728 atoms | `200` — the last size IT accepts | `400` — the band closes |
  | `\p{L}\|…`×12,729 atoms | `400 error parsing regexp: expression too large` | `400` |
  | `a`×130000 | `200`, 34 ms | `200` |

- **The divergence at the SHIPPED cap, both boundaries measured.** Neither
  number here is projected from the formula. Ours was bisected over the
  estimator and the reference's against the pinned container, one atom at
  a time: on the `\p{L}|…` alternation family **we serve up to 10,013
  atoms and the reference up to 12,728** (12,729 is its own
  `error parsing regexp: expression too large`). **A band remains, and it
  is 10,014..12,728** — 2,715 atoms out of the 12,728 the reference
  accepts, 21% of its range. Outside that band the two agree on this
  family.

  Four of the rows above were already a `400` here before this issue (the
  crate's own `CompiledTooBig`, a limit that has always been in force);
  the 10,014..12,728 band and one further row — `(?i)[\p{L}×20000]`, which
  moves from `200` to `400` — are what this cap adds.

  **The pair "~11,000 against ~13,000" that the first plan and the first
  ruling both used was never measured.** It came from the range term alone
  while the formula that produced it carries three more terms. Measured at
  the 64 MiB the first ruling approved, our boundary was 6,079 — roughly
  half the reference's range, not the near-parity the pair implied — and
  the cap was raised to 96 MiB on that measurement (owner ruling v2,
  2026-08-09). Do not reuse the old pair; the table above is the only
  boundary claim this row makes, and the note below records how it
  reached its final value.

- **Why the divergence is justified, in the terms the owner ruled.** A
  divergence needs a defect in the reference to justify it, and here there
  is one, measured. **Matching the reference's boundary means porting Go's
  `maxRunes`/`maxSize`/`maxHeight`, and those admit 128 MB parse trees —
  the exact unboundedness this issue exists to close.** They are per-limit
  caps on a parse tree, not a bound on what compiling costs; the reference
  burns over 50 s on `(?i)\p{L}`×20000 without ever refusing it. Adopting
  its boundary would mean adopting its defect. This is not "we chose
  96 MiB": 96 MiB is what a bound that actually bounds costs us, and the
  price is the measured 10,014..12,728 band.

  **The boundary moved twice while this landed, and both moves were the
  cross-check doing its job.** At 64 MiB it was 6,079. Raising the cap to
  96 MiB put it at 11,801 — and admitted `[^\p{L}]`×10000, a shape the
  64 MiB cap had always refused and which therefore had never been
  measured against its estimate. It allocated 223.10 MB against an
  88.72 MB bound. Two charges were short: negation
  (`CLASS_NEGATION_TRANSIENT_FACTOR`, new, measured 4.06–4.56×) and the
  NFA floor (`NFA_PEAK_FACTOR` 3 → 4, measured 4.08× on `[^a-z]`×10000).
  Correcting them moved our boundary to its final 10,013. The number in
  the table is the one the shipped constants produce, not the one the cap
  raise projected.

- **A second, deliberate consequence: the LogQL template site's per-compile
  render charge.** `template/funcs.rs` charged a flat
  `DYNAMIC_REGEX_PROGRAM_CEILING` (**1 MiB**) against
  `MAX_TEMPLATE_RENDER_BYTES` before compiling a template-computed
  pattern, on the belief that `size_limit` bounded what compiling costs.
  It does not. Measured at that same 1 MiB program ceiling, `\w`×16 — a
  **32-byte** pattern — peaks **2.67 MB**, and `[[:alpha:]]`×5000 peaks
  3.79 MB. The charge is now
  `pulsus_re2::regex_compile_transient_bound_with(text, 1 MiB)`, whose
  floor is **4,194,328 B** — `NFA_PEAK_FACTOR` (4) × the 1 MiB program
  ceiling, paid by even a one-character pattern, since that term is a
  floor rather than a function of the pattern.

  **Accept-surface consequence, recorded rather than absorbed:** one
  render fits **15** distinct dynamic regex compiles where it fitted
  **64** before (64 MiB ÷ 1 MiB → 64 MiB ÷ 4,194,329 B including the
  pattern copy). Nothing about the render budget or the program ceiling
  moved; the charge stopped under-reporting. Cached and literal patterns
  are unaffected at RENDER time — a literal pattern is served from the
  prewarmed cache — but the prewarm itself is budgeted too, so a literal
  pattern too large to compile is simply not cached and takes its verdict
  at render time. Only a template that COMPUTES its pattern per line pays
  the per-compile charge, and one computing fifteen distinct patterns is
  already an unusual shape.

  Pinned by three assertions inside
  `pulsus-read/tests/logql_template_alloc_gate.rs`'s
  `every_registry_function_charge_dominates_its_allocations`: the floor a
  one-character dynamic pattern is charged (**4,194,329 B**, asserted
  inside a 4 KiB window), the consequence
  (`MAX_TEMPLATE_RENDER_BYTES / floor == 15`, an `assert_eq!`), and the
  class-heavy dominance row that was red under the flat charge —
  `allocated 7,805,952 B > bound 4,260,040 B (charged 1,048,626 B)`.

- **What is NOT bounded, stated plainly.** Cumulative allocation across
  several compiles in ONE query. A query carrying 1,000 small line filters
  (95,009 bytes, inside the text cap) allocates 5.24 GB over 5.9 s with a
  3.6 MB peak; no per-compile cap can see that. It needs a query-scoped
  accumulator threaded through the nine sites. **#291 stays open to carry
  it** after this cap lands.

- **The one site that was outside this cap, and is not any more.**
  `template/mod.rs`'s literal-regex PREWARM — the query-compile-time fill
  of a template's `regex_cache` — compiled with a bare `Regex::new`.
  Being a cache warm-up bought it nothing: the pattern is the user's and
  the allocation is the same allocation. Measured on a template carrying
  a literal `\w`×43000, **86,033 bytes of template text and 129,033 bytes
  of query text — inside the 131,072-byte cap** — it peaked **298.92 MB
  and returned `Ok`**. It now goes through
  `pulsus_re2::compile_user_regex` at the crate default program limit,
  which is what it always compiled at, so nothing but the over-budget
  refusal moves. A refusal is dropped exactly as a compile failure
  already was: the pattern is not prewarmed and `funcs.rs`'s render-time
  seam takes the verdict, budgeted and charged. Pinned by the prewarm
  block in `logql_template_alloc_gate.rs`, which asserts both text
  lengths so it stays a reachable input.

- **Every charge is bounded below by a measurement, and the margins are
  stated rather than implied.** Each figure below was produced by making
  the edit and running the suite — the constant is RED at or below the
  threshold given:

  | constant | shipped | red at | over threshold | worst measured |
  |---|---|---|---|---|
  | `AST_BYTES_PER_PATTERN_BYTE` | 320 | 256 | 1.25× | 160 B/byte |
  | `HIR_BYTES_PER_NODE` | 448 | 331 | 1.35× | 356.5 B/node |
  | `HIR_BYTES_PER_LITERAL_NODE` | 24 | 1 | 24× | ~2 B/node |
  | `HIR_BYTES_PER_CLASS_RANGE` | 8 | 7 | 1.14× | 8.34 B/range |
  | `CASE_FOLD_TRANSIENT_FACTOR` | 10 | 7 | 1.43× | 8.05× |
  | `CLASS_NEGATION_TRANSIENT_FACTOR` | 5 | 3 | 1.67× | 4.40× |
  | `NFA_PEAK_FACTOR` | 4 | 3 | 1.33× | 4.08× |

  The two node charges are pinned by
  `each_hir_charge_dominates_the_phase_cost_it_models`, which measures
  the HIR phase ALONE per repeated atom; the end-to-end gates cannot pin
  them, because wherever those terms could bite the AST term or the
  41.9 MB NFA floor is already larger. Review found exactly that gap —
  `HIR_BYTES_PER_NODE` 448→224 and `CLASS_NEGATION_TRANSIENT_FACTOR` 5→3
  were both green before that test existed. The residual margins are
  deliberate: an exact-fit charge against an allocator measurement is
  width-dependent, which the alloc-bound rule forbids.

- **Pinned by** `crates/pulsus-re2/tests/regex_compile_budget.rs` — a
  counting global allocator over a 16-row corpus asserting the measured
  peak stays under the cap (refusals included), that the estimate upper
  bounds the measured peak (which is what makes the four charges breakable
  rather than asserted), that `size_limit` alone does not bound it at any
  of three values two-and-a-half orders of magnitude apart, that the
  committed accept list still compiles, and that an over-budget pattern is
  `Unknown` and never `Rejects` to the TraceQL validator. The wire
  verdicts are pinned by `crates/pulsus-read/tests/logql_regex_accept_matrix.rs`'s
  `class_alt_over_budget` row, whose reference column was captured from
  the pinned container at **10,100 atoms** — inside the band, near its
  cheap end.

  **That row costs the reference real time, and the number, the client
  deadline and a readiness gate are all load-bearing.** It first shipped
  at 12,000 and failed CI with `unexpected status 000` — curl never
  obtaining a status.

  **`000` has TWO causes.** (a) The reference's own 30 s HTTP write
  timeout — see (c), which is where it is actually explained; raising our
  client deadline from 30 s to 120 s did NOT fix that, it only stopped our
  own deadline from racing the server's and let the server's behaviour
  surface as what it is. (b) The reference is not READY yet on
  a freshly created container. The evidence is a LOG LINE, not a
  duration: review saw a cold first probe fail on a container that was
  not OOM-killed and had `RestartCount=0`, whose log carried
  `empty ring`. Reproduced here — `ratestore.go:110 err="empty
  ring"`, the HTTP port refusing connections for ~3 s and `/ready`
  answering 503 for ~23 s. No deadline touches (b), and **CI creates a
  fresh container every run, so the first probe is exactly where it
  lives**. It is closed by `wait_for_reference`, which polls until the
  reference answers BOTH `/ready` 200 and a trivial `query_range` 200,
  bounded at 180 s and panicking loudly if readiness never arrives.
  Verified by pointing the suite at a container created seconds earlier
  with no external wait: green twice.

  **(c) There was never a third mode. There was ONE mechanism, and it
  was the reference's own HTTP write timeout.** `server.http-write-timeout`
  defaults to **30 s**
  (`vendor/github.com/grafana/dskit/server/server.go:217 @ v3.7.4`, wired
  into Go's `http.Server.WriteTimeout` at `:544`), and
  `ci/logql/config.yaml` sets no timeout, so the default is what runs.
  When it expires with nothing written, Go closes the connection. The
  client then sees an empty reply (curl **52**), or a reset (**56**) if it
  reads after the RST, or — while our own deadline was also 30 s — its own
  timeout (**28**) winning the race. Three appearances, one cause.

  **This was found by reading the reference's source and confirmed by
  moving its timeout, after four rounds of inferring from the symptom.**
  The same query at the same N answers
  `000 | curl exit 52 | Empty reply from server` after **6.28 s** with
  `http_server_write_timeout: 5s`, and after **30.48 s** with the shipped
  30 s default — which is the CI failure exactly.

  **Two observations are deliberately NOT attributed to this.** The
  `000`s at 47.7 s and 47.8 s recorded on 2026-08-09 predate the
  exit-code capture, so neither has a cause on record, and neither fits:
  the wall tracks its setting closely (5 s → 6.28 s, 30 s → 30.48 s) and
  47.7 matches no setting. Firing a heavy query before the container was
  ready — testing whether ~17 s of startup plus a 30 s deadline lands
  near 47 — did not reproduce it either; the query was served in 13.68 s.
  They stay recorded as dated observations whose cause was not captured,
  rather than assigned to the nearest known mechanism. The symptom tracks the
  SERVER's timeout, not our client deadline, which is why every round
  that raised OUR patience produced what looked like another mode.

  **Consequence for the live leg (owner ruling, 2026-08-10).** The
  reference cannot serve this pattern in more than 30 s, on any hardware,
  by its own configuration. It costs 0.6 s at `line_re`, 6.5-7.1 s at
  `labelfilter_re` unconstrained, 10-27 s on two cores, and past the wall
  on a CI runner. The live probe was narrowed to `line_re` — 0.6 s here,
  ~48x margin — and **failed there too, at 31.45 s**. Three positions,
  four rounds, one wall. **`class_alt_over_budget` is therefore pinned
  from the measurements already taken and re-probed at NO position.**

  Everything else about the row is unchanged: it stays whole in
  `PATTERNS` and in this divergence's enumeration, our verdict is still
  asserted hermetically at all eighteen positions, and the reference
  column stays the recorded 2026-08-09 capture (`200` at all eighteen,
  pinned container, store populated and verified queryable).

  **What that costs, recorded because it is a real cost:** this row is no
  longer re-verified against the reference on any run, so a change in the
  reference's behaviour here would go unnoticed until someone re-measures
  by hand. Its only remaining automatic detector is on OUR side — 10,100
  sits 87 atoms above our boundary of 10,013, so a shift in our estimate
  flips the verdict to `Accept` and fails
  `pulsus_verdicts_match_the_committed_table` hermetically.

  Raising the write timeout in `ci/logql/config.yaml` was refused
  deliberately: it would alter the shared oracle every differential row is
  measured against, to rescue one row. Raising its LOG LEVEL to `info` was
  approved on the opposite reasoning — verbosity changes what the
  reference tells us, not what it answers — and stays.

  **What is still not known, recorded rather than resolved: why `line_re`
  costs 0.6 s here and past 30 s on the runner.** With `log_level: info`
  shipped, a failing run captured **3,024** per-query entries from the
  reference and **none of them was this query**: no `latency=slow`,
  nothing with a query text over 60,000 characters, and a 29-second gap
  between the last entry and the failure. `caller=metrics.go` logs on
  query COMPLETION, so a request the server closes without responding can
  never appear there — the one query needing description is the one the
  reference structurally cannot describe. Refuted along the way, each with
  a measurement: runner speed alone; store contents (empty, one line and
  ~10,000 lines all 0.65 s); CPU starvation (0.62 s at 0.25 CPU with
  data); container death, restart or memory pressure (`OOMKilled=false`,
  `RestartCount=0`, 0.42% CPU, 105 MiB at the moment of failure); our own
  client deadline; and the theory that Go's write deadline starts when the
  request is read (a heavy query fired at a cold container was served in
  13.68 s).

  The deadline half was reproduced by constraining the container
  (`--cpus 2 --memory 4g`): at the four positions that build a
  `NewFastRegexMatcher` — the two label filters, `drop` and `keep` — Go
  pays **10-27 s per query** for a pattern this size, and a cold container
  spikes past 30 s. **Lowering N does not fix it**: 10,014, the very
  bottom of the band, still peaked at 30.08 s cold, and the one other
  shape this cap newly refuses, `(?i)[\p{L}x20000]`, costs the same
  16-27 s. The divergence exists precisely when the pattern is enormous,
  so no cheap member of the class exists. **No client-side number fixes
  this** — the wall is the reference's, described in (c) — and no position
  escapes it either, which is why this row ended up pinned rather than
  probed. 10,100 is kept because it is the cheapest N that carries the
  divergence, which matters to anyone re-measuring it by hand.


## Issue #279 — LogQL query-text cap

The 131,072-byte `MAX_QUERY_BYTES` cap (docs/api.md §2.3, the reference's
`maxInputSize`) matches the reference exactly at the parse seam. Its one
divergence is a transport-layer bound discovered while shipping it.

### `timestamp-tie-order` (issue #406 — both stores deterministic, neither settles into the other's order)

No fixture case references this entry — nothing is downgraded. It records
a divergence found while fixing a defect of our own: `sql::stage3` used to
order by `timestamp_ns` alone, so entries sharing a timestamp came back in
whatever order the parts were read in, and the same query answered
differently run to run (up to fifteen orderings, and — at a `LIMIT`
cutting the tie group — fifteen different result SETS). That is fixed:
`stage3` now renders the total order `sql::stage3_keyset` always did.
**Both stores are now individually deterministic. They do not agree on
WHICH order.**

- **The probe, 2026-08-10.** Ten lines `ord_0 … ord_9` pushed as ten
  separate appends at ONE byte-identical `timestamp_ns` into one stream,
  on `grafana/loki:3.7.4` and on PulsusDB, then one `query_range`
  repeated ten times against each.

  | | `direction=backward` | `direction=forward` |
  |---|---|---|
  | reference (10/10 runs) | `ord_9, ord_8, ord_7, ord_6, ord_5, ord_4, ord_3, ord_2, ord_1, ord_0` | `ord_0, ord_1, … ord_9` |
  | PulsusDB (10/10 runs) | `ord_3, ord_7, ord_5, ord_2, ord_4, ord_1, ord_6, ord_8, ord_0, ord_9` | the exact reverse of ours |

  Ten identical runs gave one sequence on each store, so this is a
  difference of ORDER, not of stability.

- **The consequence, and it is the reason this is a ledger row rather than
  a footnote: at a `LIMIT` that cuts through a tie group, a different
  order means a different SUBSET survives.** Same query, same data, same
  limit — `limit=4` over that fixture returns
  `{ord_9, ord_8, ord_7, ord_6}` at the reference and
  `{ord_3, ord_7, ord_5, ord_2}` here. One row in common out of four.
  These are **different rows, not the same rows rearranged**, and no
  amount of client-side sorting recovers the reference's answer.

- **The two mechanisms.** Ours is
  `ORDER BY timestamp_ns, fingerprint, cityHash64(body), body`, all
  following `direction` — verified against ClickHouse directly: the
  backward sequence above is exactly `cityHash64(body)` descending
  (`ord_3` = 18389771029585043774 … `ord_9` = 555397834495227519). The
  reference's is two different keys depending on the case
  (`pkg/iter/entry_iterator.go:241-275` @ grafana/loki v3.7.4
  `b318f2829f0ae2094ab3a1e90780450e9e4b03be`): **across** streams it
  compares `streamHash`, falling back to `labels` when the hash is
  unavailable; **within** a stream it compares nothing at all — the merge
  iterator's own comment says it "does not merge entries within individual
  iterator", so entries keep the order they were appended in, which is
  what the table above shows.

- **Why we do not match it, stated as a decision.** Two separate gaps, and
  neither is a line of SQL:
  1. *Within a stream* the reference's key is **arrival order**. We do not
     store one. `log_samples` rows carry no ordinal, and a MergeTree part
     exposes no stable append rank a query can read, so matching would
     mean adding a per-entry sequence column to the hottest table on the
     write path and threading it through ingest.
  2. *Across streams* its key is `streamHash`. Our `fingerprint` is a
     different identity function over the same label set, so even ordering
     by it does not reproduce their sequence; matching would mean
     computing and storing Loki's stream hash alongside our own.

  Both are real work with their own storage and ingest cost, weighed
  against a divergence that is only observable when several entries share
  a nanosecond AND a limit cuts through them. We took determinism now and
  left order-equivalence unclaimed rather than half-claimed.

- **What IS gated, so the claim above stays honest:** our own stability
  (`logs_api_live.rs::entries_sharing_a_timestamp_come_back_in_a_stable_order`
  — 40 single-row parts at one timestamp, one query repeated twelve times
  at two limits, one of which cuts the tie group) and the agreement of our
  two stage-3 builders
  (`sql_snapshots.rs::stage3_breaks_timestamp_ties_with_the_same_total_order_as_the_keyset_builder`,
  which reads the key list off `stage3_keyset` rather than restating it).
  Nothing gates equivalence with the reference's order, because we do not
  claim it.

- **The same divergence holds on the TAIL route (issue #469, 2026-08-29).**
  That route now renders one stream object per entry (see
  `tail-stream-object-granularity-unflagged`), so an equal-timestamp
  group's order becomes an order of OBJECTS rather than of values inside
  one. It is the same divergence, not a new one, and the reason is the
  one stated above: within a stream the reference's key is arrival order
  and we store no ordinal.

  Measured by the review of that issue's plan, on two discriminating
  pairs pushed in BOTH directions — pairs chosen so that storage-hash
  order, lexicographic order and push order do not all agree — **ours is
  the storage hash order regardless of push direction, and the reference
  follows push order.** The reference side of the first pair was repeated
  twelve times; no repetition count is recorded for the first pair's
  local side or for either side of the second pair, because the record
  does not carry one.

  **No reference literal is pinned for any equal-timestamp tail fixture,
  and none can be**, since push order is not a function of anything we
  store. What is gated is our own determinism:
  `encode.rs::the_tail_tie_order_is_deterministic_and_input_order_independent`
  (twenty renders, the item vector reversed on odd rounds, byte-identical
  every time) and
  `encode.rs::the_per_entry_key_is_total_over_the_plan`.

- **Sibling: `sort-tie-order`** (below). Same SHAPE — a tie in an ordering
  key that the reference breaks by something we do not reproduce —
  different mechanism and, decisively, different CONSEQUENCE. This entry
  is about log **entries** in a `streams` response and its consequence is
  a different SUBSET surviving a `LIMIT`. `sort-tie-order` is about
  **samples** in an instant `vector`, and for the two committed terminal
  sort cases the whole vector reaches the wire, so only the arrangement
  inside an equal-value run differs. Do not fold the two together: the
  subset consequence is what makes this entry important.

### `sort-tie-order` (issue #406 — the reference specifies no order among equal-valued samples, so we pin our own)

`metric_sort_order` and `metric_sort_desc_order` stay `mode: "gated"` —
nothing is downgraded and neither case carries a `ledger` field. What this
entry records is exactly which part of the store-vs-store comparison was
RELAXED, and what stays asserted.

- **Reference behaviour (from source, not from a behavioural claim).**
  The heap comparators order on the sample value **alone**; there is no
  label or identity tie-break — `vectorByValueHeap.Less`
  (`pkg/logql/vector.go:16-21`) and `vectorByReverseValueHeap.Less`
  (`:45-50`), both @ grafana/loki v3.7.4
  `b318f2829f0ae2094ab3a1e90780450e9e4b03be`. The final ordering is
  `sort.Sort(sort.Reverse(...))` over the heap's backing array —
  `sort.Sort`, not `sort.Stable`, whose documented contract does not
  preserve the order of equal elements
  (`pkg/logql/evaluator.go:598-608` for `OpTypeTopK, OpTypeSortDesc`, the
  sort itself at `:600`, and `:610-620` for `OpTypeBottomK, OpTypeSort`,
  the sort at `:612` — both @ v3.7.4). The e2e oracle tag carries the same
  code: `v3.4.2:pkg/logql/vector.go:16-21, 45-50` and
  `v3.4.2:pkg/logql/evaluator.go:588-598` (sort at `:590`) and `:600-610`
  (sort at `:602`). **So nothing in the reference assigns an order to
  equal-valued samples.**

- **What we have OBSERVED, and nothing beyond it.** Over the counts
  `{a:5, b:1, c:5}` the reference returned `b,a,c` when the case was
  written and `b,c,a` on `grafana/loki:3.4.2` at the nightly full tier on
  2026-08-10 (`e2e-metrics-full (single)` run 31439057683). Both are
  correctly ascending by value. **Whether the arrangement varies run to
  run at a FIXED version has not been measured, and no claim that the
  reference is random, stable or version-dependent is made here or
  anywhere else.**

- **PulsusDB behaviour.** `value` then label set ascending, deterministic:
  `crates/pulsus-read/src/logql/post_agg.rs::sort_instant` (`:738-766`,
  the tie-break at `:764`).
  It stays. This is the ratified treatment of every irreproducible
  reference tie in this repo (see `label-replace-collision-tie-order`).

- **Asserted by the store-vs-store gate** (`value_ordered_sequences_agree`,
  `e2e/src/logs.rs`): both stores return the same multiset of
  `(labels, value)` (the pre-existing set-equal validity gate against the
  by-construction corpus, on both stores); PulsusDB's sequence is monotone
  in the requested direction (exact `>=`/`<=`, no tolerance); the two
  value sequences agree pointwise; and the entries occupying each
  equal-value run are the same multiset on both sides.

- **Not asserted:** the arrangement *within* an equal-value run. The runs
  are anchor-defined over the PulsusDB sequence alone — the comparison
  declines to observe the oracle's own run boundaries and does not
  establish that they coincide.

- **Not covered — a composed (non-terminal) `sort`.** `ledger-marker: sort-tie-order/not-covered` — an inner sort beneath `topk`, `bottomk` or `approx_topk` truncates, so which sample survives can change; that is a subset consequence, not a cosmetic one. Open remainder on issue #406, not closed here.

  *(The line above is one line on purpose: it carries this exclusion's
  marker and every term the AC13 guard
  (`the_sort_tie_order_divergence_is_recorded_in_the_committed_ledger`,
  `e2e/src/logs.rs`) asserts, and that guard requires the marker to occur
  exactly once in this file. Rendered text, not an HTML comment, because
  the point of recording the exclusion is that a person reads it.)*

  The cosmetic
  conclusion is about a TERMINAL `sort`/`sort_desc`, whose entire result
  vector reaches the wire. LogQL's grammar admits a sort as an inner
  operand (`metricExpr: … | vectorAggregationExpr`,
  `pkg/logql/syntax/syntax.y:114-121`; `vectorAggregationExpr: vectorOp
  '(' NUMBER ',' metricExpr ')'`, `:176-184`, both @ v3.7.4 `b318f282`;
  same productions at `v3.4.2:pkg/logql/syntax/expr.y:162-170, 226-234`),
  so `topk(1, sort(rate({app="x"}[5m])))` is a legal query and the outer
  operator **does** truncate. At the reference the survivor of a tie at
  the k boundary depends on arrival order — `group.heap[0].F < s.F` is
  strict, so the first-arrived of two equal samples is kept
  (`pkg/logql/evaluator.go:549`, `bottomk` mirror `:560`; identical at
  `v3.4.2:pkg/logql/evaluator.go:539, 550`) — so an inner sort's intra-run
  arrangement can change WHICH sample survives. That is a subset
  consequence, not a cosmetic one. PulsusDB's selection is arrival-order
  independent (`select_k_instant`,
  `crates/pulsus-read/src/logql/post_agg.rs:858`, → `sort_candidates`,
  `:778`, value then label set ascending). **No committed case exercises
  it** —
  `test/fixtures/logs/differential.json` contains no
  `topk`/`bottomk`/`approx_topk` query, and every committed `sort` is
  terminal (pinned by AC20's `every_committed_sort_case_is_terminal`,
  which PARSES each committed query and inspects its AST root rather than
  its text).

- **PulsusDB's own order is pinned** — by
  `shipped_sort_case_evaluates_in_the_pinned_value_order`, its `sort_desc`
  mirror (both `e2e/src/logs.rs`), and the two `eval_ordered` rows in
  `differential_metric_reducers.test`. **All four observe PulsusDB only
  and are evidence about our determinism, never about the reference.**

- **Relationship to `timestamp-tie-order`** (above, same issue): same
  shape, different surface and different consequence.

  | | `timestamp-tie-order` | `sort-tie-order` |
  |---|---|---|
  | surface | log **entries** in a `streams` response | **samples** in an instant `vector` |
  | our key | `ORDER BY timestamp_ns, fingerprint, cityHash64(body), body` in `sql::stage3` | `value` then label set ascending, `post_agg::sort_instant` |
  | reference's key | arrival order within a stream / `streamHash` across (`pkg/iter/entry_iterator.go:241-275`) | none — value only, unstable sort |
  | consequence | **a different SUBSET survives a `LIMIT`** — one row in four in common | **cosmetic for the two committed TERMINAL sort cases**: the whole result vector reaches the wire, so the multiset on the wire is identical and only the arrangement inside an equal-value run differs |

- Gated by `differential_metric_reducers.test` (the two `eval_ordered`
  rows, each carrying `# provenance: divergence(sort-tie-order)`), by
  `value_ordered_agreement_*` / `tie_groups_*` in `e2e/src/logs.rs`, and
  by `the_sort_tie_order_divergence_is_recorded_in_the_committed_ledger`.

### `nested-sort-order` (issue #406 R2 — where the reference's surviving order is its own map walk, ours stays deterministic)

This entry records a **deliberate** difference in one place only: which
instant `vector` responses keep a `sort`/`sort_desc`'s value order on the
wire. No fixture case is downgraded and no case carries a `ledger` field
for it.

**The whole record, on one line so a machine can check it.** `ledger-marker: nested-sort-order` — reference rule `Sortable` (`pkg/logql/evaluator.go:242-260`), call site `pkg/logql/engine.go:564`; its surviving order is a Go map walk, `evaluator.go:584` over `map[uint64]*groupedAggregation`; ours would be too, `post_agg.rs:1122-1135`; our rule is `sorted_order_reaches_the_wire`. Measured 2026-08-11, 20 repeats per store per query on a 2/1/3 fixture, against `grafana/loki@sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc` (`b318f282`) and `grafana/loki@sha256:58a6c186ce78ba04d58bfe2a927eff296ba733a430df09645d56cdc158f3ba08` (`4fa045d3`). We DIVERGE (our deterministic label order, their map walk) on: `sum by (svc) (sort(X))`; `topk(2, sort(X))`; `Y * sort(X)`; `sort(X) or Y`; a `variants(…) of (…)` root. We AGREE (value order, 20/20 both images) on `label_replace`, a scalar operand, the many side of a vector binop including `group_right`, `and`/`unless`, and `sort(A) or sort(B)`. **Not covered**: whether an inner sort changes which sample a `topk`/`bottomk` keeps — that is R1 on issue #406, a subset consequence rather than an order one, ruled **no work** (comment 5252213426) because nothing gated reaches it and `topk` orders its input anyway.

  *(ONE line on purpose, and the only gated one. The AC 9 guard
  (`the_nested_sort_order_divergence_is_recorded_in_the_committed_ledger`,
  `e2e/src/logs.rs`) asserts that this marker occurs exactly once in this
  file and that every needle above sits on its line — so nothing the
  record claims can drift away from the marker, and no text elsewhere can
  satisfy the check. It was three lines until code review round 4; one
  line means there is no "are these lines in the same entry" question to
  answer, and the four rounds of markdown edge cases that question cost
  are deleted with it. The prose below is the same content for a human
  reader and is not gated.)*

R2's defect itself is FIXED, not ledgered: `label_replace(sort(…), …)`,
`sort(…) * 1` and a vector binary operand now return value order here, in
agreement with the reference. What this entry records is the residue —
the cases where matching the reference would mean reproducing an
arbitrary answer.

- **Reference rule, from source.** The re-sort is suppressed whenever a
  `sort`/`sort_desc` appears **anywhere** in the AST: `Sortable`
  (`pkg/logql/evaluator.go:242-260`) walks the whole tree with
  `expr.Walk` (`:246-255`) and returns true on any
  `VectorAggregationExpr` whose operation is `OpTypeSort`/`OpTypeSortDesc`;
  its call sites are `pkg/logql/engine.go:564` and `:627`, each guarding a
  `sort.Slice` on the label set at `:569`/`:632`. Both @ grafana/loki
  v3.7.4 `b318f2829f0ae2094ab3a1e90780450e9e4b03be`. A variants root
  short-circuits to `false` (`evaluator.go:244-245`).

- **Why that rule cannot be copied: what it leaves on the wire is a Go
  map walk.** Under a vector aggregation the surviving sequence is
  `VectorAggEvaluator.Next`'s emission loop, `for _, aggr := range result`
  over `result := map[uint64]*groupedAggregation{}`
  (`pkg/logql/evaluator.go:584`, the map declared at `:442`, both @
  v3.7.4). Go randomises map iteration order, so the reference's answer to
  `sum by (svc) (sort(…))` is arbitrary. **Ours would be too:**
  `post_agg::group_instant` collects with
  `groups.into_iter()` out of a `HashMap<LabelSet, Vec<f64>>`
  (`crates/pulsus-read/src/logql/post_agg.rs:1122-1135`). So suppressing
  the re-sort there would put OUR hash walk on the wire — a correctness
  defect, not a parity one, and it would hold even if the reference were
  stable.

- **Measured, 2026-08-11.** Corpus: three streams with **2 / 1 / 3** lines
  pushed byte-identically to every store over `POST /loki/api/v1/push`, so
  label order (`a,b,c`), ascending (`b,a,c`) and descending (`c,a,b`) are
  three different arrangements and no two values are equal. 20 instant
  queries per store per query, `time` nudged +1 s per repeat. Stores:
  `grafana/loki@sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`
  (buildinfo `{"version":"3.7.4","revision":"b318f282"}`) and
  `grafana/loki@sha256:58a6c186ce78ba04d58bfe2a927eff296ba733a430df09645d56cdc158f3ba08`
  (`{"version":"3.4.2","revision":"4fa045d3"}`, the e2e oracle,
  `deploy/e2e/compose.single.yaml:172`).

  | query | PulsusDB | loki 3.7.4 | loki 3.4.2 |
  |---|---|---|---|
  | `sum by (svc) (sort(X))` | `a,b,c` **20/20** | `b,a,c` 11, `a,c,b` 6, `c,b,a` 3 | `b,a,c` 17, `a,c,b` 2, `c,b,a` 1 |
  | `X * sort(Y)` (vector LHS) | `a,b,c` **20/20** | `a,b,c` 14, `c,a,b` 6 | `a,b,c` 16, `c,a,b` 2, `b,c,a` 2 |

  Sets equal in every cell; values byte-identical; the difference is order
  only. The controls on the same run discriminate: bare `X` was
  label-ordered 20/20 on all three stores, `sort(X)` was `b,a,c` 20/20 on
  all three, `sort_desc(X)` was `c,a,b` 20/20 on all three. The full
  per-query tables, including the six wrapper queries that now AGREE, are
  posted on issue #406.

- **PulsusDB rule.** An instant vector keeps the engine's order exactly
  when a `sort`/`sort_desc`'s order reaches the root through
  order-preserving wrappers —
  `crates/pulsus-read/src/logql/order.rs::sorted_order_reaches_the_wire`,
  consumed by `logs_api::handlers::preserve_vector_order`. Everywhere
  else the encoder's label sort stands
  (`logs_api::encode::query_response`).

- **Where we AGREE with the reference** (measured above, 20/20 on both
  images, and the reason this is a residue rather than a divergence):
  `label_replace(sort(…), …)`; `sort(…) ⊗ <scalar>` and
  `<scalar> ⊗ sort(…)`; the many side of a vector binary operand — the
  left normally, the right under `group_right`; `sort(…) and Y`;
  `sort(…) unless Y`; and `sort(A) or sort(B)`, which returned
  `c=3|b=1|a=2` (the whole LHS in order, then the RHS-only entries in
  order) 20/20 on both images and 20/20 here.

- **Where we DIVERGE, enumerated.** Each is a shape whose reference answer
  is the map walk above, and in each we return our deterministic label
  order:
  1. under any non-sort vector aggregation — `sum`, `avg`, `min`, `max`,
     `count`, `stddev`, `stdvar` — e.g. `sum by (svc) (sort(X))`;
  2. under a k-selection — `topk`, `bottomk`, `approx_topk` — e.g.
     `topk(2, sort(X))`;
  3. on the NON-many side of a vector binary operand: `Y * sort(X)`, and
     `sort(X) + on(l) group_right Y` where the swap makes `Y` the carrier;
  4. `sort(X) or Y` with an unsorted `Y`, where the appended RHS tail is
     itself a map walk;
  5. a `variants(…) of (…)` root, where the reference does not even
     consult the tree (`evaluator.go:244-245`).

- **Consumer impact.** A client that keys on `(labels, value)` sees no
  difference at all — the multiset is identical in every case above. A
  client that reads position sees a stable answer here and an unstable one
  there, which is the direction the standing mandate asks for: match the
  reference except where it is wrong, and an arbitrary answer to a
  deterministic question is wrong.

- **Not covered** — whether an inner sort changes WHICH sample a
  `topk`/`bottomk` keeps. That is R1 on issue #406, a subset consequence
  rather than an order one, ruled **no work** (comment 5252213426)
  because nothing gated reaches it and `topk` orders its input anyway.
  Row 2 above records only the ORDER half. *(The gated copy of this
  exclusion is on the record line at the top of the entry; this is the
  same statement for a human reader.)*

- Gated by
  `the_nested_sort_order_divergence_is_recorded_in_the_committed_ledger`
  (`e2e/src/logs.rs`), by
  `crates/pulsus-read/src/logql/order.rs`'s classification tests, and by
  `logs_api_live.rs::a_wrapped_sort_keeps_its_value_order_on_the_wire`.

### `logs-timestamp-i64-nanosecond-domain` (issue #406 Part D, rulings v3 — a representation limit with a date on it)

No fixture case references this entry — nothing is downgraded. It records
the one place PulsusDB **refuses** a `start`/`end`/`time` value the
reference serves, and it is a consequence of our representation rather
than a policy choice.

- **The boundary is a date, not an integer.** Every PulsusDB timestamp is
  an `i64` count of nanoseconds since the Unix epoch, so the whole
  representable range is **1677-09-21T00:12:43.145224192Z** to
  **2262-04-11T23:47:16.854775807Z**. `i64::MAX` nanoseconds is ~9.22e18,
  which is 2262 — not an arbitrary cut-off, and not something a larger
  constant could move without changing the type. The reference has no such
  limit on the parse: Go's `time.Time` stores seconds and nanoseconds in
  separate fields, so it represents any second-valued instant a client can
  spell.
- **What that costs, measured 2026-08-10 against `grafana/loki:3.7.4`
  with a corpus loaded.** `?start=9999999999` — ten characters, so unix
  SECONDS under both stores' length rule — is logged by the reference as
  `start=2286-11-20T17:46:39Z` and served. `9999999999 s` is ~1.0e19 ns,
  past `i64::MAX`, so PulsusDB answers `400 invalid timestamp
  "9999999999": expected unix seconds (<= 10 characters), unix
  nanoseconds, a fractional-second value, or RFC3339`. The largest
  ten-character seconds value we accept is `9223372036`
  (2262-04-11T23:47:16Z); `9223372037` is the first refused.
  `crates/pulsus-server/src/logs_api/params.rs`'s
  `parse_ts_refuses_a_seconds_value_outside_the_i64_nanosecond_domain`
  pins both sides of that boundary.
- **Why this is acceptable rather than a hole: the two spellings of one
  unrepresentable instant agree.** `parse_ts`' RFC3339 branch has ALWAYS
  refused out-of-domain values — `chrono`'s `timestamp_nanos_opt` returns
  `None` outside 1677-2262 — so `?start=2286-11-20T17:46:39Z` was already
  a `400` here before issue #406, and Part D did not introduce a new
  asymmetry. What Part D changed is that `9999999999` now means that same
  instant instead of 10 seconds after the epoch, so it now gets the same
  answer. A `checked_mul` decides it; nothing wraps and nothing saturates
  to an arbitrary instant.
- **Consumer impact:** none reachable. The refusal starts in the year 2262
  at one end and 1677 at the other; no query a user writes against stored
  observability data lands outside it, and the retention TTLs make the
  point moot long before the type does.

### `shards-no-pulsus-counterpart` (issue #406, informational note, not a gate downgrade)

No fixture case references this entry — nothing is downgraded. It records
a request parameter the reference reads on five routes we mount and that
PulsusDB deliberately does not implement.

- **What diverges:** `shards` is read through the repeated seam
  (`r.Form["shards"]`, `pkg/loghttp/params.go:79-81` @ grafana/loki
  v3.7.4 `b318f2829f0ae2094ab3a1e90780450e9e4b03be`) on `query_range`,
  `query`, `series`, `index/stats` and `index/volume`. It names TSDB index
  shards for the query frontend to fan a query across. PulsusDB has no
  such object: sharding is ClickHouse's, decided by the cluster's own
  `fingerprint`-keyed distribution (docs/schemas.md §7), and there is no
  client-addressable shard identifier to accept.
- **Reference behaviour, container-measured 2026-08-10 against
  `grafana/loki:3.7.4` on a 160-entry, 3-stream corpus:** `?shards=` and
  `?shards=bogus` are **`500`** on `/query_range` and `/query` — the
  reference renders a client error as a server error here — and inert
  (`200`) on `/series` and `/labels`.
- **PulsusDB behaviour:** the parameter is accepted and ignored (`200`),
  the same answer as omitting it.
- **Why not implement it:** copying the parameter would mean either
  inventing a shard identifier with no meaning in our storage layout, or
  reproducing a `500` on a malformed client value. Neither is a behaviour
  worth having. Recorded rather than silently dropped so the absence is a
  decision with a date on it.

### `storechunks-no-pulsus-counterpart` (issue #406, informational note, not a gate downgrade)

No fixture case references this entry — nothing is downgraded.

- **What diverges:** `storeChunks` is read on `query_range` and `query`
  (`pkg/querier/queryrange/codec.go:2328` @ grafana/loki v3.7.4
  `b318f282`), an undocumented escape hatch carrying a serialized chunk
  reference set so a query can be replayed against specific stored chunks.
  It is a debugging affordance over the reference's own chunk store.
- **Reference behaviour, container-measured 2026-08-10:** a garbage value
  is a `400`.
- **PulsusDB behaviour:** accepted and ignored (`200`).
- **Why not implement it:** PulsusDB stores log lines as ClickHouse rows,
  not as chunk objects; there is no chunk reference for a client to name,
  so there is nothing the parameter could select. It is also undocumented
  upstream, so no client can depend on it by contract.

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

### structured-metadata-collision-resolution (issue #381)

- **Status: PARITY, with three named residuals.** The construct itself is
  fixed, not exempted: PulsusDB now runs the reference's own builder at the
  one shared structured-metadata seam, and reproduces its answer on every
  measured row. What remains are the three residuals below, two of which are
  cases where the reference has no stable answer of its own.
- **Construct:** which of several structured-metadata pairs sharing a stored
  label name is the one stored, and what a value containing `utf8.RuneError`
  (U+FFFD) stores as.
- **The rule, from source.** Loki's distributor runs an entry's structured
  metadata through Prometheus' `labels.Builder`
  (`pkg/distributor/distributor.go:697-722 @ v3.7.4` over
  `vendor/github.com/prometheus/prometheus/model/labels/labels_stringlabels.go:454-521`
  and `labels_common.go:163-200`): `base` is the entry's pairs sorted by name
  with duplicates preserved, `Reset` seeds `del` with the raw name of every
  empty-valued pair, the loop `Del`s + `Set`s a pair whose name renames and
  `Set`s a pair whose value carries U+FFFD (`removeInvalidUtf`, `:75-80`),
  and `Labels()` lets an `add` entry REPLACE the first base entry of the same
  name. So a pair that was `Set` beats a pair that was not, wherever either
  sits in wire order; among `Set` pairs the last wins; among pairs never
  `Set` the reference keeps them all as duplicate labels and a JSON consumer
  observes the last. **It is not last-write-wins**, which cannot explain why
  both wire orders of `{a.b="x", a_b="keep"}` store `a_b="x"`.
- **Measured** on `grafana/loki:3.7.4` (`buildinfo` `3.7.4` / `b318f282`,
  `ci/logql/config.yaml`), pushed as JSON structured metadata with duplicate
  object keys emitted verbatim and read back with
  `X-Loki-Response-Encoding-Flags: categorize-labels`. The raw response
  bodies are committed at
  `crates/pulsus-write/tests/fixtures/structured_metadata_collisions/capture.json`
  and are the source every hermetic expectation is derived from; the drift
  leg re-captures them in CI (`schema-it`, "Structured-metadata collision
  capture drift leg"). Of the 17 rows the reference can serve, **12 differed
  from PulsusDB at `b872855`** and 5 already agreed (`c04`, `c05`, `c07`,
  `c11`, `c18` — the frozen greatest-original-key rule happens to elect the
  same pair there, so they pin the rule without discriminating the fix).
- **Residual A — the reference's own answer is unspecified past 12 pairs.**
  `base` is ordered by Go's `slices.SortFunc` (`ScratchBuilder.Sort`,
  `labels_stringlabels.go:627-629 @ v3.7.4`), insertion sort up to 12
  elements and pdqsort above, so a repeated canonical name PLUS a rename
  landing on it resolves by an unstable permutation. Measured with `k` copies
  of `a_b` followed by `a.b="REN"`: the container returns the last wire copy
  at k=3 (`3`), k=5 (`5`), k=8 (`8`) and k=11 (`11`), and returns `a_b="2"`
  at k=12, 13, 15 and 20 — the boundary is 13 pairs in the entry. With no
  rename present it returns the last wire copy at k=13, 20 and 40. PulsusDB
  sorts stably and returns the last wire copy at every k, so the two agree
  everywhere except "repeated canonical name + a rename onto it + at least 13
  pairs". Not chased: the reference has no answer there to match. Pinned on
  our side by `labels.rs`'s
  `the_stable_sort_returns_the_last_wire_copy_on_both_sides_of_the_boundary`.
- **Residual B — a row the reference accepts and cannot serve.**
  `{a_b="1", a_b="p\ufffd"}` is a **204** push whose read is a **500**:
  `failed to parse series labels to categorize labels: 1:6: parse error:
  invalid UTF-8 rune`, with and without the categorize header. Its `Labels()`
  emits two `a_b` entries — the `add` rewrite `"p "` and the untouched base
  duplicate — and its own read path then refuses the series. There is no
  consumer-observable reference value, so PulsusDB's is a choice: the
  duplicate collapse keeps the last, `a_b="p\ufffd"`, and the entry is
  readable. Pinned by `labels.rs`'s
  `a_duplicate_the_reference_cannot_serve_collapses_to_the_last_pair` and by
  `structured_metadata_collisions.rs`'s
  `the_row_the_reference_cannot_serve_is_stored_by_us_as_the_last_pair`,
  which asserts the captured 500 alongside our stored value.
- **Residual C — inherited, unchanged.** The builder groups by PulsusDB's
  `canonicalize_label_key` rather than the reference's `LabelNamer.Build`, so
  wherever the two renamings differ the collision GROUPS differ (`a..b` and
  `a__b` are `a_b` there and `a__b` here; `9bad` gains a `key_` prefix
  there). That is the renaming divergence already registered under issue #259
  / docs/api.md §8.2, not a second rule, and it disappears when that one
  does.
- **In scope deliberately: the U+FFFD branch is a value change.** It is not
  separable from the collision rule — the branch is a `Set`, and `Set` is
  what decides the tier, so omitting it resolves `{a.b="x", a_b="p\ufffd"}`
  and its reverse to the wrong pair. It comes with a rewrite that is
  user-visible on its own, with no collision needed:
  `{a_b="p\ufffdq"}` now stores `a_b="p q"` where it stored `a_b="p\ufffdq"`
  before. Rule 6 of the docs/api.md §8.2 table.
- **Not in scope, and named rather than omitted:** stream-label collisions of
  any kind. Structured metadata is a per-entry column and never enters
  `stream_fingerprint`, so no stored stream identity moves here;
  `LabelSet::from_normalized`'s frozen greatest-original-key rule (issue #4)
  is untouched and still governs stream labels, including residual 4 of
  `ingest-label-bounds` (a repeated OTLP index attribute) and the
  `{service.name, service_name}` near-miss, which belong to issues #4/#109.
- **Fixture status:** capture-backed parity in
  `crates/pulsus-write/tests/structured_metadata_collisions.rs`
  (`the_stored_string_reproduces_the_reference_capture`,
  `the_table_discriminates_the_pre_fix_resolution`), the rule itself in
  `pulsus-model/src/labels.rs`
  (`structured_metadata_resolution_is_the_references_builder`, and
  `the_fast_path_is_the_builders_identity_case` over an exhaustive 4 368-case
  enumeration against an independent transcription of the builder),
  cross-encoding agreement in `loki_push.rs`
  (`both_push_encodings_resolve_a_metadata_collision_identically`),
  cross-transport agreement in
  `crates/pulsus-write/tests/cross_transport_parity.rs`
  (`both_log_receivers_resolve_structured_metadata_with_one_rule`), and
  storage in `loki_push_live.rs`
  (`colliding_structured_metadata_is_stored_as_the_reference_resolves_it`).

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
  | `{app="x"} \|~ "("` | `400` | `200` |
  | `{app="x"} \| line_format "{{.x}}" \|~ "("` | `400` | `200` |
  | `count_over_time({app="x"} \|~ "(" [5m])` | `400` | `200` |
  | `{app=~"("}` | `400` | `400` |
  | `{app="x"} \| regexp "(?P<c>()"` | `400` | `400` |
  | `{app="x"} \| drop a=~"("` | `400` | `400` |
  | `{app="x"} \| logfmt \| a=~"("` | `400` | `400` |
  | `label_replace(rate({app="x"}[5m]),"d","$1","s","(")` | `400` | `400` |

  A syntax error is refused in both windows; an error the parser accepts
  and the pipeline builder rejects is refused only while the ingester is
  in the query's path.

  The last eight rows (issue #246, measured 2026-08-08 on the same pinned
  image, 30-day-old window for the stale column) are a MALFORMED REGEX
  put to every LogQL construct that carries one, and they split the same
  way. Only the line filter is window-dependent, because it is the one
  regex construct the reference raises at pipeline BUILD
  (`pkg/logql/log/filter.go:646 @ v3.7.4`, `parseRegexpFilter` reached
  from `newRegexpFilter` at `:363`); every other construct compiles its
  pattern at PARSE — the stream selector and the label filter through
  `mustNewMatcher` (`pkg/logql/syntax/ast.go:1102-1108`), `| regexp`
  through `newLabelParserExpr` (`ast.go:729-741`), and `label_replace`
  through `mustNewLabelReplaceExpr` (`ast.go:2225-2233`) — so it is
  `400` in every window. **A live leg that used a stale window would
  therefore score the reference as accepting an entire half of the regex
  accept surface**, which is why
  `crates/pulsus-read/tests/logql_regex_accept_matrix.rs`'s
  `live_matrix_against_the_reference` ends its window at `now` and
  asserts both dispositions of this split rather than describing them.

  The last two rows (issue #247, measured 2026-08-07 on the same pinned
  image) are the same construct split across both classes, so they show
  the boundary rather than merely instancing one side of it. A malformed
  extraction EXPRESSION is refused by the logfmt sub-grammar at
  `Stage()`, which is pipeline-build — window-dependent. A dangling comma
  in the extraction LIST is a `syntax.y` production error refused by
  `ParseExpr` — window-independent. Both are `400` in every window here.

- **Issue #388 adds two more sub-grammars, and they land on OPPOSITE
  sides of this entry's split** (measured 2026-08-13 on the same pinned
  image, as container `pulsus-c388-loki` on port 13488; 1 h window ending
  now vs 1 h window ending 24 h ago):

  | query | recent | 24 h old | PulsusDB |
  |---|---|---|---|
  | `{service_name="m"} \| json v="b-c"` | `400` | `200` | `400` |
  | `{service_name="m"} \| json v="b c"` | `400` | `200` | `400` |
  | `{service_name="m"} \| json v="b 1.5"` | `400` | `200` | `400` |
  | `{service_name="m"} \| pattern "<a> <a>"` | `400` | **`400`** | `400` |
  | `{service_name="m"} \| pattern "<1a> x"` | `400` | **`400`** | `400` |
  | `{service_name="m"} \| pattern ""` | `400` | **`400`** | `400` |

  The `| json` rows are window-dependent for exactly the reason the
  logfmt row above is: `jsonexpr.Parse` runs from
  `NewJSONExpressionParser` (`pkg/logql/log/parser.go:634-651 @ v3.7.4`)
  at `Stage()`, which is pipeline-build.

  **The `| pattern` rows are window-INDEPENDENT on both sides, and that
  is a new fact for this entry.** `NewPatternParser` is called from
  `newLabelParserExpr` (`pkg/logql/syntax/ast.go:730-741 @ v3.7.4`),
  which **panics with a `logqlmodel.ParseError` during `ParseExpr`** —
  one layer earlier than every other build error catalogued here. So a
  malformed pattern is refused before the store's stale-window short
  circuit can hide it, and it is refused at `variants(...)`' common
  pipeline too, where a `Stage()` error is swallowed: measured,
  `variants(count_over_time({service_name="m"}[5m])) of
  ({service_name="m"} | pattern "<a> <a>" [5m])` is `400` while the same
  shape with `| json v="b-c"` is `200`, empty.

  That difference is why the two halves of #388 have two matrices with
  two windows and no shared harness —
  `crates/pulsus-read/tests/logql_pattern_expr_matrix.rs` asserts the
  window-independence and `logql_json_expr_matrix.rs` asserts the
  window-dependence, each with the other stage as its control, so neither
  claim can quietly become a statement about stale windows in general.

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
  regex **in a `| regexp`, `| drop`, `| keep` or label-filter stage** of a
  variant are all refused here as they are there. Unlike every
  other row in this entry that rejection is **window-independent on both
  sides**, because the reference raises it in the querier's
  `newVariantsEvaluator` rather than behind the store's stale-window
  short circuit — measured: no variant-side point moves when the window
  is aged past `query_ingesters_within`. Both rows are pinned point by
  point in `crates/pulsus-read/tests/logql_logfmt_expr_matrix.rs`.

  **Correction (issue #246, measured 2026-08-08).** That sentence said
  "an uncompilable regex in a variant" without qualification, and one
  construct escapes it: a variant's own **PUSHABLE line filter**.
  Measured on the same pinned image and through this tree's
  `parse → plan → compile` chain:

  | query | reference | PulsusDB |
  |---|---|---|
  | `variants(count_over_time({app="x"} \|~ "(" [5m])) of ({app="x"}[5m])` | `400` | **`200`** |
  | `variants(count_over_time({app="x"} \| line_format "{{.x}}" \|~ "(" [5m])) of ({app="x"}[5m])` | `400` | `400` |
  | the same with `\| regexp "("`, `\| drop a=~"("`, `\| logfmt a="b.c"` or `\| line_format "{{"` | `400` | `400` |

  **The escape is the PUSHDOWN, not the construct**, which the first
  version of this correction got wrong by writing "line filter" flat.
  `VariantSpec::try_new` (`plan.rs:2641`) does compile the variant's
  discarded prefix, but `compile_stage` returns `Ok(None)` for a pushable
  line filter (`pipeline.rs:986-996`) before it reaches `compile_regex` at
  `:1013`; a pushable filter's regex is validated on the SQL-rendering
  path instead (`logql/escape.rs`'s `_checked` renderers), and a discarded
  prefix renders no SQL. Put the filter after a `line_format` and
  `seen_line_format` clears the pushdown, so the filter IS compiled and
  the second row agrees. This is the one place in this entry where the
  divergence runs the OTHER way (we serve what the reference refuses), so
  the owner ruling below does not cover it; it is enumerated as
  `variants_variant_side_skips_the_line_filter` in
  `logql-regex-accept-surface-divergence`, bounded there by the
  `variants_variant_after_line_format` position, and owned by **#400**.

  **Narrowing (issue #397, measured 2026-08-10).** The escape needs one
  more word: the variant must be **BARE**. The reference type-switches on
  the variant expression (`pkg/logql/syntax/extractor.go:114 @ v3.7.4`)
  and only a bare `*RangeAggregationExpr` gets the `nil` stages above; a
  variant wrapped in a vector aggregation reaches `variant.Extractors()`
  (`:128`) and the full stage list, so PulsusDB now compiles that whole
  pipeline, its pushable line filter included. Measured on the pinned
  image, same session:

  | query | reference | PulsusDB |
  |---|---|---|
  | `variants(count_over_time({app="x"} \|~ "(" [5m])) of ({app="x"}[5m])` | `400` | **`200`** (BARE — the escape, unchanged) |
  | `variants(sum by (env) (count_over_time({app="x"} \|~ "(" [5m]))) of ({app="x"}[5m])` | `400` | `400` (WRAPPED — parity, newly gained) |

  Corpus row W29 in `b13_variants.test` pins the wrapped half. Adding a
  wrapped POSITION to `logql_regex_accept_matrix.rs` belongs to **#400**
  with the rest of that surface.

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

## Issue #246 — the LogQL regex accept surface

### `logql-regex-accept-surface-divergence` (issue #246, measured; every class OWNED by #400 unless noted)

**Not a wording entry.** Issue #246 was filed to reproduce Go's `regexp`
error prose byte for byte and was scoped out of that by the owner (rulings
2026-07-26 and 2026-08-08): the prose difference is covered by
`logql-error-envelope`, and no translation table exists or is owed. What
this entry records is what the scouting run found while measuring the
prose question — the two engines disagree about the **decision** far more
often than they agree about it.

- **Measurement conditions.** 16 query positions × 48 patterns = 768
  points, plus 2 masked positions × 48 = 96, so 864 probed in all. (These
  read 704/88/792 until #400 Stage 1 and were stale by one pattern: the
  three assertions in `the_divergence_set_is_exactly_the_committed_enumeration`
  that say "the ledger says N" are recomputed from the table, so the
  table had already moved while this sentence had not. #400 Stage 2 adds
  three patterns — `\p{Cs}`, `(?P<1n>a)` and `\p{LC}` — which is where
  45 → 48 comes from.) Reference: the digest-pinned
  `grafana/loki@sha256:87f0a067…` v3.7.4 oracle
  (`/loki/api/v1/status/buildinfo` = `{"version":"3.7.4","revision":"b318f282"}`)
  with `ci/logql/config.yaml`, probed at `/loki/api/v1/query_range` over a
  5-minute window **ending at `now`** — load-bearing, see
  `malformed-query-refused-in-every-window`. PulsusDB: `parse → plan →
  CompiledPipeline::compile`, the compile `exec` runs before any I/O.
  **226 of the 768 unmasked points disagree**, in the classes below —
  down from 321 of 720 before #400 Stage 2, which both REMOVED
  disagreements (nine patterns at fifteen positions each) and ADDED them
  (two new Class E patterns at sixteen), so the figure is taken from the
  test's own printed line and not from arithmetic on the old one. Every
  figure here is recomputed by
  `crates/pulsus-read/tests/logql_regex_accept_matrix.rs` rather than
  restated: the class enumeration is asserted equal to the matrix's own
  derived disagreement set, and no class may be redundant.

- **Direction A — PulsusDB rejects, the reference serves.** A query that
  works there fails here.

  | class | patterns | positions | note |
  |---|---|---|---|
  | `engine_dir_a_perl_and_flag_forms` | `\Qa*\E`, `\101`, `a(?i){2}`, `(?ss:ab)`, `(?)a` | 15 | the `re2_pattern_to_rust` rewrite does not change them, so `label_replace` is affected too |
  | `engine_dir_a_brace_forms` | `a{bbb}c`, `a{,5}`, `a{}` | 14 | `label_replace` excluded — its rewrite escapes the braces. That is #331's deferred partial fix, live at one site out of fourteen |
  | `engine_dir_a_duplicate_capture_name` | `(?P<n>a)(?P<n>b)` | 14 | **not one of the eighteen classes #400 was filed with** — found by this matrix. The reference's vendored parser has no duplicate-name check (`git grep -n duplicate vendor/github.com/grafana/regexp/ @ v3.7.4` finds only an unrelated comment); the Rust crate refuses it. `regexp_named` excluded: the reference refuses it there for its own reason, `duplicate extracted label name 'n'` (`pkg/logql/log/parser.go:309-311 @ v3.7.4`) |
  | `engine_dir_a_nesting_limit` | 999 nested groups | 7 | a LIMIT, not a construct. The reference's `maxHeight` is 1000 (`vendor/github.com/grafana/regexp/syntax/parse.go:93 @ v3.7.4`) and every site that wraps the pattern in `^(?:…)$` spends part of it, so this rejects on both sides at the anchored positions and only diverges at the unanchored ones |
  | `variants_common_side_hides_the_build_error` | the 14 patterns both sides reject | 1 | **owned by #380**, not #400: the reference swallows a `variants(...)` common-pipeline build error in every window. Deliberate, and the direction the owner ruled for |
  | `engine_dir_a_non_utf8_string_literal` | `"\xff"` | 7 | **new in #400 Stage 1, and DELIBERATE** — see `logql-string-escape-non-utf8` below for the reachability argument, which is the part that makes it acceptable. With the reference's string grammar in the lexer, `"\xff"` decodes to the byte 0xFF here as it does there, and a decoded byte string that is not valid UTF-8 cannot be a Rust `String`, so it is a positioned `400` at every position. The nine positions that reach the reference's regex parser therefore now AGREE. What is left is the six `NewFastRegexMatcher` sites, which short-circuit a plain literal before `syntax.Parse` (`vendor/github.com/prometheus/prometheus/model/labels/regexp.go:56-72 @ v3.7.4`) and answer `200` with nothing, plus `variants_common_side`, whose `200` is #380's swallowed build error rather than the short-circuit |

- **Direction B — PulsusDB serves, the reference rejects. THIS DIRECTION
  IS NOW EMPTY, and that is #400 Stage 2's result.** Every class below is
  retired; the table is kept because it is the record of what was
  divergent, why each member was a WRONG ANSWER rather than leniency, and
  what closed it. `the_divergence_set_is_exactly_the_committed_enumeration`
  asserts the emptiness — a new Direction-B row reddens it rather than
  being merely reviewable.

  | class | patterns | positions | fate |
  |---|---|---|---|
  | *(retired by #400 Stage 2)* `engine_dir_b_read_as_a_different_pattern` | `\U0001F600`, `(?R)a`, `(?x)a`, `a**`, `[[:foo:]]`, `\p{Alphabetic}`, `a{1001}` | was 15 | `a**` was read as `(a*)*`, which matches **every** subject tested (`""`, `a`, `b`, `:`, `101`, an emoji), so a line filter carrying it returned the whole stream; `[[:foo:]]` as a nested class matching `:`/`f`/`o`. In a `line_format` template `a**` renders **`zxz`** from the input `x` and `\p{Alphabetic}` renders **`z`** — see `template-error-wording-residuals`. **Correction (issue #400, measured 2026-08-10):** this cell said `(?R)` is read as the crate's line-terminator flag "and matches everything". It is not: `(?R)a` matches `"a"` and does not match `""`, `"b"` or `"x\r\ny"`. The false half was an inference from a category — a flag we do not have — sitting beside two measured facts. **Closed by `pulsus_re2::re2_definitely_rejects` at the LogQL compile seams**; the readings are pinned over eleven subjects by `crates/pulsus-re2/tests/re2_reject_classes.rs`'s `the_rust_crate_reads_these_as_a_different_pattern`, which lives in that file precisely so it outlives this row |
  | *(retired by #400 Stage 2)* `engine_dir_b_class_forms` | `[a--b]`, `\u{263A}` | was 14 | `label_replace` was excluded — its rewrite made the Rust crate agree with the reference, so that one position was the only LogQL site where these were refused. **Owner ruling 2026-08-10: both are #400's to close with a decidable check, NOT the deferred `re2_pattern_to_rust` rewrite**, and that is exactly how they were closed. **The deferred #331/#336 pattern rewrite must not later be credited with either**: `[a--b]` is refused by rule (g), which reads `a--` as the RANGE `a`..`-` (`parse.go:1815 @ v3.7.4`), and `\u{263A}` by rule (d), which refuses every `\u`/`\U` spelling. Stage 1 closed neither — it is the string-literal LEXER, and both are REGEX bodies reaching the compiler with their backslashes doubled; the spelling `{app=~"\u{263A}"}` was Stage 1's and `{app=~"\\u{263A}"}` is Stage 2's, and both are `400` now |
  | *(retired by #400 Stage 1)* `engine_dir_b_invalid_utf8_escape` | `"\xff"` | was 9 | **this row is gone and its successor points the other way** — see `engine_dir_a_non_utf8_string_literal` in Direction A above. It recorded the verdict half of a wrong-answer defect: Go's unquoting gives the reference one 0xFF byte, refused as invalid UTF-8 at the nine positions that reach its parser, while our lexer dropped the backslash and compiled the three ASCII characters `xff`. Stage 1 gave the lexer the reference's own grammar, so those nine positions now AGREE and the six that never parse a plain literal are what is left |
  | *(retired by #400 Stage 2)* `variants_variant_side_skips_the_line_filter` | the patterns both sides reject | was 1 | found by this matrix. The reference refuses a malformed line filter inside a `variants(...)` variant; we served it — but only while the filter was PUSHABLE **and the variant was BARE** (issue #397: a variant wrapped in a vector aggregation runs its whole pipeline, so the filter was compiled and both sides refused). `compile_stage` returns `Ok(None)` for a pushable line filter before reaching `compile_regex`, so the discarded variant prefix `VariantSpec::try_new` compiles never saw the regex, and a discarded prefix renders no SQL either. **Closed by `build_variants_node`'s bare arm validating those regexes directly** (`plan.rs`'s `validate_pushable_line_filter_regexes`) — validation only, no pushdown decision moved — and `variants_variant_side`'s rule moved `AcceptsEverything → PerPattern` with it. The position that BOUNDED this row, `variants_variant_after_line_format`, is still in the matrix and still agrees at every pattern |

- **Class E, added by #400 Stage 2's corpus sweep — PulsusDB rejects,
  the reference serves, and neither member is in the eighteen classes
  this issue was filed with.** Both are Direction A; they are recorded,
  not fixed, because closing either needs pattern transformation at the
  as-written seams, which the 2026-08-05 owner ruling refused. Both
  produce an error, never a wrong row.

  | class | patterns | positions | note |
  |---|---|---|---|
  | `engine_class_e_reference_serves_what_the_rust_crate_refuses` | `\p{Cs}`, `(?P<1n>a)` | 16 | `Cs` IS a `unicode.Categories` key, so `unicodeTable` resolves it and the reference serves `\p{Cs}` (measured `200`); the Rust crate answers `Unicode property value not found`. `(?P<1n>a)` is a valid capture name there — `isValidCaptureName` is `[A-Za-z0-9_]+` and its own comment says "Python rejects names starting with digits. We don't enforce either of those" (`vendor/github.com/grafana/regexp/syntax/parse.go:1261-1272 @ v3.7.4`) — while the Rust crate requires an XID start. **`re2_definitely_rejects` is asserted NOT to claim either**: rule (f)'s committed table CONTAINS `Cs`, and rule (h) fires only on a byte outside `[A-Za-z0-9_]`, so a digit-leading name reaches no rule. Its mirror image, `(?P<n.x>a)`, is a `400` on both sides |

- **The LogQL string ESCAPE was one root cause with TWO divergences, and
  #400 Stage 1 closed both at the one line they shared.**
  `scan_double_quoted` (`crates/pulsus-logql/src/lexer.rs`) knew `\n`,
  `\t` and `\r` and handled everything else with
  `Some(other) => value.push(other)` — dropping the backslash and keeping
  the character. It now carries the grammar the reference actually uses:
  `prometheus/util/strutil.Unquote`
  (`vendor/github.com/prometheus/prometheus/util/strutil/quote.go:66-231 @ v3.7.4`),
  which Loki's lexer calls on every string token
  (`pkg/logql/syntax/lex.go:190-201`). Measured on the pinned oracle and
  through this tree's parser; the "was" column is what shipped until
  2026-08-10:

  | spelling | reference | PulsusDB, was | PulsusDB, now |
  |---|---|---|---|
  | `{app=~"\101"}` | `200`, a matcher for `A` (Go octal) | `200`, a matcher for `101` | `200`, a matcher for `A` |
  | `{app=~"\x41"}` | `200`, a matcher for `A` | `200`, a matcher for `x41` | `200`, a matcher for `A` |
  | `{app=~"\u0041"}`, `{app=~"\U00000041"}` | `200`, a matcher for `A` | `200`, `u0041` / `U00000041` | `200`, a matcher for `A` |
  | `"\xc3\xa9"` | one character, `é` (byte escapes compose) | the six characters `xc3xa9` | one character, `é` |
  | `"\ud800"` | U+FFFD (`utf8.EncodeRune` on a surrogate) | the five characters `ud800` | U+FFFD |
  | `{app=~"\d+"}` | `400 parse error at line 1, col 7: invalid char escape` | `200`, a matcher for `d+` | `400` |
  | `{app=~"\0"}`, `{app=~"\q"}`, `{app=~"\'"}` | `400 … invalid char escape` | `200`, `0` / `q` / `'` | `400` |
  | `{app="x"} \|~ "\d+"` | `400 … col 14` | `200`, filter `d+` | `400` |
  | `{app="x"} \| regexp "(?P<c>\d+)"` | `400 … col 20` | `200` | `400` |
  | `{app=~"\xff"}` | `200` at the selector, `400 … invalid UTF-8` wherever it parses | `200`, a matcher for `xff` | `400` everywhere — see `logql-string-escape-non-utf8` |

  **Why the value half was the severe one, and it is not an accept-surface
  story at all.** Pushed as two streams under one `service_name` and asked
  as one query on the pinned container: `{app=~"\101", service_name="esc2"}`
  returns the `app="A"` stream and its line, and `{app=~"101", …}` returns
  the `app="101"` stream — disjoint answers, both `200`, no error on either
  side. That is a query silently reading a different part of the store, not
  a permissive parse.

  **What pins it now.** The decoded values are asserted BY BYTE in
  `crates/pulsus-logql/tests/string_escapes.rs` (one row per arm of
  `unquoteChar`, at the selector and at a line filter, so the claim that
  this is a LEXER rule is itself checked), and the LINES each escape
  selects are `crates/pulsus-read/tests/logqltest/corpus/b24_string_escapes.test`,
  captured from the pinned container. The three rows that discriminate
  against a plausible-but-wrong implementation are named there: an `Err`
  for `\d` (rules out keeping the pass-through fallback), `"\xc3\xa9"`
  decoding to two bytes (rules out a Latin-1 reading of `\xHH`), and
  `"\ud800"` decoding to U+FFFD (rules out lifting `pulsus-traceql`'s
  scanner, which refuses surrogates — true of Go's SOURCE literal grammar,
  false of the `strutil` copy Loki calls).

  **Neither half was in the matrix's points, and the reason needs its
  qualifier**: every **other** pattern there is a regex body that
  `logql_quote` escapes on the way in, so it cannot carry a string escape
  at all. `Body::LogqlSource` deliberately exempts one body variant, and
  `invalid_utf8` (`\xff`) IS that variant — the exemption is the whole
  point of it. So the coverage argument splits 44 patterns × 18 positions
  = 792 points (quoted, cannot carry a string escape) plus **one pattern
  at 18 points** (`invalid_utf8`, unquoted), and 792 + 18 = 810. Those 18
  carry `\x`, an escape Go DEFINES, so they were the value half's probe
  and never the reject half's. **The matrix was not widened to cover
  either half** — it scores verdicts, and a decode is not a verdict.

  **Why the value half survived a review round is the limit worth keeping**:
  that matrix scores VERDICTS. Both sides accepted at all 16 positions, so
  no cell moved, no test moved, and no amount of adding patterns or
  positions would have caught it. Its module docs say so under "What this
  matrix cannot see". The PromQL side of the same escape family was
  examined once before, on
  `docs/decisions/0002-promql-parser-selection.md:110`.

- **Class R has a SECOND root cause, and it is not the lexer** (issue #400, measured 2026-08-10 on the pinned container against this tree's `regex` crate). The escape fix above closes the lexer half completely. It does not touch the character-class **set algebra** of docs/api.md §9.2: `[a&&b]`, `[a~~b]` and `[a[b]]` are accepted by BOTH engines and read differently by each, so both stores answer `200` and return different lines. Measured at a `| decolorize |~` line filter (pushdown cleared, evaluated in process) over one stream carrying `a`, `b`, `&`, `~`, `-`, `[` and none of them:

  | pattern | reference | PulsusDB, in process |
  |---|---|---|
  | `[a&&b]` | the `a`, `b` and `&` lines | nothing |
  | `[a~~b]` | those plus the `~` line | those minus the `~` line |
  | `[a[b]]` | nothing | five lines |

  `[a--b]` is NOT in this group — the reference answers `400 error parsing regexp: invalid character class range` there, so it is an accept-surface class, not a wrong-rows one.

  **The domain is narrower than the escape half's**: ClickHouse's RE2 evaluates every pushed-down predicate and agrees with the reference, so this is confined to §9.1's **as written** positions plus `label_replace`. **Recorded here because a fix that closes only the lexer looks complete.**

  **Ruled ACCEPTED on 2026-08-12, and the family is eight patterns rather than three.** It now carries its own row — `logql-class-algebra-wrong-rows` below — with the owner's ruling, both sides' selections subject by subject, and the reference parser's own rule. One reading of the table above is worth correcting while the table stays: `[a[b]]` shows `nothing` for the reference because *that* measurement's subjects carried no `]`. Over a subject set that does, the reference selects the `a]` line — the class is `a`, `[`, `b` and the trailing `]` is a literal, so it matches something; just not what the crate's nested-class reading matches.

- **Backtick raw strings agree, and this was checked rather than assumed.** Go's own `strconv.Unquote` STRIPS carriage returns from a raw string literal; the fork Loki calls does not (`quote.go:76-81` has no carriage-return branch), and neither does `scan_backtick`. Discriminated at the wire over a line carrying a raw CR: the filter whose backtick pattern contains a raw CR returns that line on the reference — so the CR survived into the pattern — while the control whose pattern has the CR removed returns nothing. This tree's parser yields the same bytes for the backtick and the double-quoted spelling alike (`63 72 0D 61 66 74 65 72`). A raw CR inside a double-quoted literal is likewise kept by both: `Unquote` refuses a raw newline (`quote.go:85-87`) and says nothing about a carriage return.

- **Status parity, in contrast, holds everywhere.** Every rejection above
  is `400` on both sides — ours through `PipelineError::BadRegex` →
  `ReadError::PipelineInvalid` → `StatusCode::BAD_REQUEST`, and, for the
  string-escape refusals #400 Stage 1 added, through
  `LogQlError::{InvalidCharEscape, NonUtf8StringLiteral}` →
  `ApiError::LogQl` → `StatusCode::BAD_REQUEST`
  (`crates/pulsus-server/src/logs_api/error.rs:160`) — at all 810 probed
  points. That is the half this issue shipped as a pin.

- **The three ways to measure this and learn nothing**, each hit during
  the scouting run and each now built into the fixture: probing `| regexp`
  **without** a named capture (both sides refuse it for that reason alone
  — 0 of 44 disagree); probing the negated selector as `{app!~"P"}` (both
  sides refuse a selector with no positive matcher — 0 of 44 disagree);
  and putting the live leg to a window older than `query_ingesters_within`
  (the reference's line filter is a build error, so it answers `200` there
  — the leg would score the whole of Direction B as agreement).

- **The pattern set is enumerated from the reference's TAXONOMY, and two
  unreachability claims in the first version of it were false in the same
  way.** Of the 16 `ErrorCode` constants the vendored parser declares
  (`vendor/github.com/grafana/regexp/syntax/parse.go:28-48 @ v3.7.4` — 16,
  not the 17 the plan stated), 14 are now raised by a named pattern, each
  tied to the container's own captured error body. Only `ErrInternalError`
  and `ErrInvalidCharClass` are excused, and their argument is a grep over
  the reference's source (declared, never raised anywhere in the package)
  rather than a probe.

  The two that were wrongly excused, recorded because the shape recurs:
  `ErrInvalidUTF8` was excused on a probe that sent a **raw** `%FF` byte in
  the `query=` parameter, which the LogQL scanner refuses first
  (`pkg/logql/syntax/query_scanner.go:264 @ v3.7.4`) — while the string
  **escape** `"\xff"` reaches the parser normally and `{app="x"} |~ "\xff"`
  is a `400 … invalid UTF-8`. `ErrLarge` was excused on a probe of a
  *nested* repeat, which the repeat-product cap pre-empts
  (`repeatIsValid(re, 1000)`, `parse.go:434-437`) — while 4,000 copies of
  `a{999}`, 24,000 characters, reaches `maxSize`, which is
  `128<<20 / instSize` with `instSize = 40` = **3,355,443** instructions
  (33,554,432 is `maxRunes`, the other limit, and the figure the first
  version quoted for this one). **An unreachability claim is a claim about
  every route into a rule, so the probe's domain has to be the claim's
  domain.** The census now fails if a pattern is credited with a code whose
  captured body does not begin with that code's message, and the live leg
  fails if any excused code is ever observed on the wire.

- **What is NOT here.** No fix. Both root fixes — porting Go's
  `regexp/syntax`, and embedding RE2 — were rejected by the owner on #331,
  and the partial fix (applying `re2_pattern_to_rust` at the LogQL
  in-process sites, which would close the brace and class-form classes and
  nothing else) was deferred there. #400 owns the decision.

- **Fixture status:** `crates/pulsus-read/tests/logql_regex_accept_matrix.rs`
  — hermetic halves plus `live_matrix_against_the_reference` and
  `live_template_axis_against_the_reference`, both gated on
  `PULSUSDB_LOGQL_DIFF_URL` and wired into CI's `schema-it` job against
  the already-running `pulsus-logql-diff` container. The per-route surface
  axis is `crates/pulsus-server/tests/logs_api_live.rs`'s
  `a_malformed_selector_regex_is_refused_on_every_mounted_logs_route`.

### `logql-storage-re2-property-table` (issue #400 Stage 2 — measured, not fixed here)

- **What differs.** `\p{LC}` is served by grafana/loki v3.7.4 and refused
  by ClickHouse's RE2. One pattern, three engines, and the third answer
  is not the second's:

  | engine | verdict | how measured |
  |---|---|---|
  | grafana/loki v3.7.4 (`grafana/loki@sha256:87f0a067…`) | **`200`** at all sixteen matrix positions | `logql_regex_accept_matrix.rs`'s `live_matrix_against_the_reference`, `PATTERNS` row `unicode_prop_lc_category` |
  | the Rust `regex` crate 1.13.0 | compiles it | `re2_screen_differential.rs`'s `the_property_table_the_storage_engine_carries_is_not_the_references` |
  | ClickHouse 26.3.17.110's RE2 | **`Code: 427 CANNOT_COMPILE_REGEXP`** — `cannot compile re2: ^(?:\p{LC})$, error: invalid character class range: \p{LC}` | the same test, live |

- **Cause, at its measured strength.** The reference's property table is
  Go's `unicode` package, **read**: `unicodeTable` is `"Any"`, then
  `unicode.Categories[name]`, then `unicode.Scripts[name]`, then `nil`
  (`vendor/github.com/grafana/regexp/syntax/parse.go:1646-1658 @ v3.7.4`),
  and `LC` is a `Categories` key. ClickHouse's is upstream RE2's own
  generated table, **not read**. That the two differ on this name is the
  measurement above; *why* they differ is not claimed here.

- **What PulsusDB does, and why.** `pulsus_re2::re2_definitely_rejects`'s
  rule (f) reads the REFERENCE's table, so it answers `false` for
  `\p{LC}` and the pattern is not refused at plan time. Refusing it would
  be an over-rejection against the accept-surface authority, which is the
  Loki container and not the storage engine.

- **The consequence, probed end to end and NOT fixed here.**
  `{app=~"\\p{LC}"}` through the real `pulsusdb` binary against ClickHouse
  26.3.17.110 answers **`500`** with
  `clickhouse: server [427]: Code: 427. DB::Exception:
  OptimizedRegularExpression: cannot compile re2: ^(?:\p{LC})$` — **but
  only when the query actually scans a row**. Measured 2026-08-12 on the
  same server process: with `log_streams_idx` holding no row for the
  matched key it is a `200` with an empty result, because ClickHouse
  never evaluates the `match()`; after seeding one stream row it is the
  `500` above. `{app=~"\\p{L}"}` is a `200` in both states, which is the
  discriminator that makes this about the NAME rather than about
  `\p{…}` support.

  This path is **pre-existing** — every pattern ClickHouse's RE2 refuses
  and the Rust crate accepts reaches it the same way — and #400 Stage 2
  neither creates nor closes it. It is recorded here so the next reader
  meets the measurement rather than rediscovering it. `\p{Lc}` (lowercase
  `c`) is a `400` at plan time on both sides, because `unicodeTable` does
  no case folding of the NAME; that is the pair that separates "the
  storage engine has a different table" from "the pre-check is wrong".

### `logql-string-escape-non-utf8` (issue #400 Stage 1 — DELIBERATE narrowing, owner ruling 2026-08-10)

- **Reference behaviour.** `"\xff"` in a LogQL string literal is one
  0xFF byte (`strutil.Unquote`'s `\xHH` arm is a byte, `quote.go:186-190`).
  The reference refuses that pattern wherever it reaches its regex parser
  — `400 error parsing regexp: invalid UTF-8` at the line filter in both
  operators, after a `line_format`, at `| regexp`, inside
  `count_over_time` and a binary op, at `label_replace` and at both
  `variants(...)` positions — and SERVES it at the five
  `NewFastRegexMatcher` sites (the selector in both operators, both label
  filters, `drop`, `keep`), because `optimizeAlternatingLiterals` returns
  a string matcher for a plain literal before `syntax.Parse`
  (`vendor/github.com/prometheus/prometheus/model/labels/regexp.go:56-72 @ v3.7.4`).
  Measured on the pinned container: `{app=~"\xff", service_name="esc"}`
  is `200` with an empty result; `{service_name="esc"} | decolorize |~ "\xff"`
  is `400 … invalid UTF-8`.

- **PulsusDB behaviour.** `400` at every position. The lexer decodes the
  same 0xFF byte the reference does, and a decoded byte string that is not
  valid UTF-8 cannot become the `String` the rest of the engine carries,
  so it is a positioned `LogQlError::NonUtf8StringLiteral`. Reached only
  by a `\xHH`/`\NNN` escape above `0x7F` that no neighbouring escape
  completes: `"\xc3\xa9"` composes to `é` and is served.

- **Why this is acceptable — the reachability argument, which is the part
  that makes it a narrowing and not a regression.** **No mounted ingest
  route can store invalid UTF-8.** Every one materialises a line body and
  a label value into a Rust `String` — `LogRow.body: String`,
  `StreamRow.labels: LabelSet`
  (`crates/pulsus-write/src/protocols/otlp_logs.rs:37-55`) — through prost
  or `serde_json`, both of which require valid UTF-8 and fail the push
  otherwise. So no line and no label value in this store could match the
  reference's 0xFF byte, and the five positions where it serves the
  pattern are exactly the five where it answers `200` **and nothing**. A
  user cannot observe a row difference through any door they have; they
  observe an error instead of an empty result.

- **The alternative was refused.** Compiling a never-matching pattern so
  those positions answer `200` and nothing would make the plan-time
  validity check assert something false, and would answer differently from
  the nine positions where the reference itself sends `400`.

- **`pulsus-traceql` carries the identical ruling** (`lexer.rs`, #56), and
  two surfaces disagreeing on the same question would be worse than either
  answer. LogQL's version is NARROWER than TraceQL's: the byte buffer lets
  `"\xc3\xa9"` through, where TraceQL refuses any byte escape above
  `0x7F`.

- **Fixture status:** `crates/pulsus-logql/tests/string_escapes.rs`
  (`a_lone_high_byte_escape_is_refused`, `a_byte_escape_composes_with_its_neighbour`),
  the `engine_dir_a_non_utf8_string_literal` row and
  `the_parsed_pattern_value_is_committed_where_the_escape_changes_it` in
  `crates/pulsus-read/tests/logql_regex_accept_matrix.rs`, whose live leg
  re-measures the reference's `Accept` at the short-circuit positions
  rather than assuming it, and the `\xff` rows of
  `crates/pulsus-read/tests/logqltest/corpus/b24_string_escapes.test`.

### `logql-class-algebra-wrong-rows` (issue #400, owner ruling 2026-08-12 — ACCEPTED, deliberately not fixed)

- **What differs.** Eight patterns are accepted by **both** engines and
  read differently by each, so the same filter selects different lines
  and **neither side reports anything**:

  ```
  [a&&b]   [a~~b]   [a[b]]   [[a][b]]   [\w&&\d]   [!--b]   [+--b]   [ --a]
  ```

  This is the wrong-rows severity, not an accept-surface one. Every one
  of the eight is `200` there and `200` here; no status moves, no error
  is raised, and the answer is simply a different set of lines. It is the
  residue of `logql-regex-accept-surface-divergence` above: Stage 1
  closed the string-escape half of Class R and Stage 2 closed Class S2,
  and this is what neither reached.

- **Measured, both sides, 2026-08-12.** Thirteen lines were pushed as one
  stream, each line being *exactly* one subject and nothing else (extra
  bytes would match instead of the subject): `x`, `a`, `b`, `&`, `~`,
  `-`, `[`, `]`, `a]`, `1`, `!`, `+`, `Z Z`.

  **Reference:** the digest-pinned image from `.github/workflows/ci.yml`,
  `grafana/loki@sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`,
  booted on `ci/logql/config.yaml` with no delta of its own as container
  `pulsus-i400led-loki` on port 13411 — its own
  `/loki/api/v1/status/buildinfo` reports `3.7.4` / `b318f282`. Asked
  over `/loki/api/v1/query_range` with a window ending after the last
  line pushed.

  **PulsusDB:** the same thirteen lines and the same eight queries
  through the hermetic `logqltest` runner
  (`crates/pulsus-read/tests/logqltest/runner.rs`'s `run_file`), which
  executes the real pipeline and no SQL — so what it reports is the
  in-process reading, which is the position this divergence lives at.

  Both sides at `| decolorize |~`, and both in the **backtick** spelling
  (`` |~ `[a&&b]` ``). Both details are load-bearing: `| decolorize`
  clears the pushdown so the filter is evaluated in process rather than
  by ClickHouse (see the domain note below), and a backtick literal takes
  no string escapes, so `[\w&&\d]` reaches the regex parser — the
  double-quoted `"[\w&&\d]"` is a `400` on **both** sides since Stage 1,
  which measures the lexer and not the class. Measured on the same
  container: `{service_name="ca401"} | decolorize |~ "[\w&&\d]"` is
  `400 parse error at line 1, col 40: invalid char escape`, and our own
  side is pinned for `\w` at the selector, the line filter and
  `| regexp` by `crates/pulsus-logql/tests/string_escapes.rs`'s
  `an_escape_go_does_not_define_is_refused`.
  Control first: `` |~ `.` `` returns all thirteen lines on both sides.

  | pattern | the reference selects | PulsusDB in process selects |
  |---|---|---|
  | `[a&&b]` | `&`, `a`, `a]`, `b` | **nothing** |
  | `[a~~b]` | `a`, `a]`, `b`, `~` | `a`, `a]`, `b` |
  | `[a[b]]` | `a]` | `a`, `a]`, `b` |
  | `[[a][b]]` | **nothing** | `a`, `a]`, `b` |
  | `[\w&&\d]` | `&`, `1`, `Z Z`, `a`, `a]`, `b`, `x` | `1` |
  | `[!--b]` | `!`, `&`, `+`, `-`, `b` | `!` |
  | `[+--b]` | `+`, `-`, `b` | `+` |
  | `[ --a]` | `!`, `&`, `+`, `-`, `Z Z`, `a`, `a]` | `Z Z` |

  The `Z Z` line is how the **space** subject is carried: the corpus DSL
  `trim_start`s a body, so a line that is *only* a space cannot be
  written in it, and a space between two word characters can. It is the
  only one of the thirteen carrying a space, which is why it is the only
  line PulsusDB returns for `[ --a]`.

- **The mechanism, read from the reference rather than inferred from the
  table.** `parseClass`
  (`vendor/github.com/grafana/regexp/syntax/parse.go:1736-1825 @
  grafana/loki v3.7.4 b318f2829f0ae2094ab3a1e90780450e9e4b03be`) has
  **no set-operation syntax and no nested-class branch at all**. Inside a
  class it recognises exactly three structured members — `[:name:]`
  (`:1766`, which requires the next byte to be `:`), `\p{…}` (`:1778`)
  and a Perl class escape (`:1788`) — and everything else falls through
  to "single character or simple range" at `:1793`. So `&`, `~` and `[`
  are ordinary members; the loop ends at the first unescaped `]` that is
  not in first position (`:1756`), which is why `[a[b]]` closes after
  `b` and leaves a **literal `]`** behind it; and the one operator a
  class has is `X-Y`, whose `hi < lo` is `ErrInvalidCharRange`
  (`:1806-1809`) — which is why `[!--b]` is the valid range `!`..`-`
  plus `b` and not an error.

  The Rust `regex` crate 1.13.0 reads the same bytes as its documented
  character-class operators: `&&` intersection, `~~` symmetric
  difference, `--` difference, and `[…]` nested inside a class as a
  union. `[a&&b]` is therefore `[a] ∩ [b]` = empty (and a pattern that
  can never match is not an error there), `[a[b]]` and `[[a][b]]` are
  both the union `{a, b}`, `[\w&&\d]` is `\w ∩ \d` = the decimal digits
  (Unicode `Nd`, since the crate's classes are Unicode where RE2's are
  ASCII — §9.2's first row), and `[!--b]` is `[!] - [b]` = `!`. Each of
  those readings is exactly the right-hand column above.

- **The domain, and it is narrower than the table suggests.** At every
  position the planner pushes the predicate into the scan (§9.1 of
  `docs/api.md`), the pattern is evaluated by **ClickHouse's `match()`,
  which is RE2 itself** — and it agrees with the reference subject for
  subject. Measured 2026-08-12 on the shared ClickHouse
  (`SELECT version()` = `26.3.17.110`, HTTP 18123) as
  `SELECT match(<subject>, <pattern>)` over the same thirteen subjects
  and the same eight patterns: the hit set is identical to the
  reference's column above on **all eight**. So the divergence is
  confined to §9.1's **as written** rows — a line filter after a
  `line_format` or with an `ip(…)` alternative, `| regexp`, a
  parsed-label filter, `drop`/`keep` — plus `label_replace`.

- **Why it is accepted — the owner's judgement, recorded as such.**
  Owner ruling 2026-08-12, on all eight: **"rare, so who cares"**.
  Writing `&&`, `~~` or a nested class inside a log-filter regex is not
  something a Grafana user does by accident, and nobody has hit it. The
  one-line fix is cheap; the plan, review, implementation and review
  round around it are not, and the same cycle spent elsewhere buys more.
  This is a judgement about cost and reach, **not** a technical finding
  that the divergence is harmless — the rows above are a query silently
  reading different lines, and that is what makes it worth a ledger row
  even though it is not worth a fix.

  Recorded here because the issue trail does **not** survive the fresh
  repository at release. A decision that lives only in a GitHub comment
  is a decision that will be re-litigated from scratch by whoever meets
  `[a&&b]` next.

- **Close condition:** the owner reverses the 2026-08-12 ruling. Nothing
  else reopens this; it is not waiting on a fix. No route to one is
  sketched here on purpose — the mechanism that would close the
  class-form classes is the `re2_pattern_to_rust` rewrite at the **as
  written** sites, which `logql-regex-accept-surface-divergence`'s "What
  is NOT here" records as deferred on #331/#336 with the decision owned
  by #400. This row is that decision, and it is "keep it".

- **Fixture status, stated at its real strength.** No test pins the eight
  READINGS. What is pinned is that the eight stay **served**:
  `crates/pulsus-re2/tests/re2_reject_classes.rs`'s
  `rule_g_is_the_range_rule_and_not_a_double_dash_rule` asserts
  `!re2_definitely_rejects(p)` for `[!--b]`, `[+--b]`, `[ --a]`,
  `[a&&b]` and `[a~~b]` — five of the eight — so a later change that
  "fixed" this family by over-rejecting would fail there rather than
  silently refusing patterns the reference serves. `[a[b]]`,
  `[[a][b]]` and `[\w&&\d]` have no such assertion, and nothing anywhere
  asserts which lines any of the eight select. The natural home for the
  readings is that file's `the_rust_crate_reads_these_as_a_different_pattern`,
  which already pins Stage 2's classes over eleven subjects; it was
  **not** widened here, because this task was to record the ruling and
  not to fix or to fence the family.

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
- **Residual 1 — CLOSED by issue #379; kept for the record because the
  reasoning it carried was wrong and load-bearing.** It read: every one of
  the four messages interpolates the stream's label set, the reference's
  copy carries a `service_name` its own push parser injected, PulsusDB does
  not implement service-name discovery, so our rendered set omits it. That
  half was true and is now fixed — PulsusDB synthesizes `service_name` on
  both receivers, before validation, so the rendered set is the reference's
  (`crates/pulsus-write/src/protocols/service_name.rs`, issue #379), and the
  bodies agree in the transcript below.

  The half that was false: "the same injection is why the reference's
  `MissingLabelsErrorMsg` is unreachable there and not implemented here."
  It is unreachable from `/loki/api/v1/push`, where synthesis always refills
  the set. It is **reachable from `/otlp/v1/logs`**, whose discovery
  algorithm has no non-empty guard: a resource whose only index attribute is
  `container.name=""` writes `service_name=""`, suppresses the
  `unknown_service` fallback (`pkg/loghttp/push/otlp.go:198-220 @ v3.7.4`),
  strips to nothing, and answers `400 error at least one label pair is
  required per stream` — measured on `grafana/loki@sha256:87f0a067…`, stock
  config. That sentence was used to justify not implementing the check, so
  the check was missing for as long as the sentence stood. It is implemented
  now, as the first check in `StreamLabels::validate`, matching
  `pkg/distributor/validator.go:158-167 @ v3.7.4`.

  Loki error bodies additionally carry the trailing newline Go's
  `http.Error` appends, which none of our Loki plain-text error bodies
  have — pre-existing, not specific to these four, and unchanged.
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

## Issue #379 — `service_name` discovery at log ingest

### `ingest-service-name-discovery` (issue #379 — parity ADOPTED; one residual, owned by #109)

PulsusDB previously stored every stream exactly as pushed, where the
reference synthesizes a `service_name` label for every stream that does not
carry one. The label set differed on **every** push that omitted it, so
stream identities differed and a query filtering or grouping on
`service_name` returned nothing here where it returned data there. Issue
#379 adopts the reference's rules — both of them, because the reference has
two.

- **Reference behaviour, measured** on the digest-pinned v3.7.4 oracle
  (`grafana/loki@sha256:87f0a067…`, buildinfo `3.7.4` / `b318f282`, stock
  config), read back through `/loki/api/v1/series`:

  | pushed / resource attributes | stored stream |
  |---|---|
  | push `{name="nn379", app="aa379"}` | `{app="aa379", name="nn379", service_name="aa379"}` |
  | push `{workload="ww379", job="jj379"}` | `{job=…, service_name="ww379", workload=…}` |
  | push `{component="cc379", container="kk379"}` | `{component=…, container=…, service_name="kk379"}` |
  | push `{service_name="", app="aa2-379"}` | `{app=…, service_name="aa2-379"}` |
  | push `{__pattern__="1", app="pp379"}` | `{__pattern__="1", app="pp379"}` — no synthesis |
  | push `{}`, `{"onlyempty":""}`, `{"z"*2000:""}` | `204`, all one `{service_name="unknown_service"}` stream |
  | OTLP `k8s.job.name=j379` | `{k8s_job_name=…, service_name="j379"}` |
  | OTLP `k8s.pod.name=p-379` | `{k8s_pod_name=…, service_name="unknown_service"}` |
  | OTLP `container.name=c4-379, service.name=""` | `{container_name="c4-379"}` — **no `service_name` at all** |
  | OTLP `service.name="ok379", service_name=<2049 B>` | `{service_name="ok379"}` |
  | OTLP `container.name=""` | `400 error at least one label pair is required per stream` |

  The push rule scans the thirteen `DiscoverServiceName` defaults
  (`pkg/validation/limits.go:329-343 @ v3.7.4`) in LIST order over every
  stream label, skipping empty values, after the strip and before validation
  (`pkg/loghttp/push/push.go:442-456 @ v3.7.4`). The OTLP rule scans the
  resource attributes in WIRE order, only over the eighteen it promotes to
  index labels, with no non-empty guard, and lets a `service.name` attribute
  overwrite the slot wherever it appears (`pkg/loghttp/push/otlp.go:174-220 @
  v3.7.4`). They disagree observably —
  `{k8s.container.name, container.name}` resolves by arrival order on OTLP
  and always to `container_name` on push — so PulsusDB implements two
  functions, not one.

- **What PulsusDB now does: both rules, in the same positions** —
  `crates/pulsus-write/src/protocols/service_name.rs`, called from
  `loki_push::parse_json`, `loki_push::parse_protobuf` and
  `otlp_logs::parse`. The discovered value also populates the physical
  `log_streams.service` / `log_samples.service` column, which
  `log_samples`' `ORDER BY (service, fingerprint, timestamp_ns)` leads on;
  before this change that column was `''` for every stream pushed without an
  explicit `service_name`, so the leading primary-key column was a constant.
  The list is not configurable, exactly as the four label bounds of
  `ingest-label-bounds` are not.

- **Residual — the extra stream label, owned by #109 and NOT new.** On the
  OTLP path PulsusDB stores every resource attribute as a stream label while
  the reference indexes eighteen and routes the rest to structured metadata.
  So a resource carrying `app=x` stores `{app="x", service_name="unknown_service"}`
  here and `{service_name="unknown_service"}` there: the `service_name` now
  agrees and the extra label does not. Two consequences of making the slot
  authoritative, both of the same #109 mechanism: an attribute whose raw name
  merely canonicalizes onto `service_name` (`service_name`, `service-name`,
  `service name`) is now stored **nowhere** here, where the reference keeps it
  as structured metadata; and that attribute no longer wins the
  `from_normalized` collision (issue #4) it used to win, so
  `{service.name: "ok", service_name: <2049 B>}` stores the validated `"ok"`
  on both sides. The seventeen other index names still resolve that collision
  the old way — `{k8s.pod.name: "ok", k8s_pod_name: <2049 B>}` stores the
  unvalidated value here — which is `ingest-label-bounds`' *What these bounds
  do not cover*, unchanged. Issue #109 owns the placement rule and therefore
  owns all of this.

- **Pinned by** `service_name`'s own unit tests (the thirteen defaults and
  their order, list order vs wire order asserted side by side so unifying the
  two resolvers fails a test, the skip cases, the OTLP slot's overwrite order
  including the empty-value cases, and the three-of-eighteen intersection
  enumerated from the reference's own two lists), by
  `the_discovered_label_is_inside_the_bound_message` and
  `parse_protobuf_rejects_a_label_value_over_2048_bytes` in `loki_push.rs`
  (synthesis precedes validation, proved through byte-equal `400` bodies), by
  `an_empty_stream_label_set_is_the_reference_message` and
  `the_empty_check_precedes_the_internal_stream_exemption` in
  `log_label_limits.rs`, by `an_empty_index_attribute_value_empties_the_stream`
  and `a_resource_with_no_index_attributes_still_validates` in `otlp_logs.rs`
  (both directions of the empty-label check), and live by
  `the_discovered_service_name_reaches_the_service_column_on_both_receivers`
  and `an_otlp_near_miss_spelling_stores_an_over_wide_indexed_label` in
  `crates/pulsus-server/tests/loki_push_live.rs`, which read the `service`
  column, the stored labels JSON and the fingerprints back out of ClickHouse.

### `lexer-identifier-charset` (issue #392, owner ruling 2026-08-10 — parity ADOPTED; two residuals, both reference defects)

**What changed.** PulsusDB's LogQL lexer accepted `[A-Za-z_][A-Za-z0-9_]*`
and nothing else, so `| logfmt éx="b"`, `| json éx="b"`,
`| label_format éx="y"`, `| drop éx` and `sum by (éx)` were `400` here
and `200` at the reference — a query that works there was broken here.
The lexer now applies the reference's rule.

**The rule, from the source.** Loki v3.7.4 builds its LogQL scanner on a
vendored Go `text/scanner` and **never assigns `IsIdentRune`**
(`pkg/logql/syntax/query_scanner.go:157` declares the hook, `:339-340` is
its only use; `git grep IsIdentRune pkg/` @ v3.7.4 returns nothing else),
so the default predicate at `:338-343` is the rule verbatim —
`ch == '_' || unicode.IsLetter(ch) || unicode.IsDigit(ch) && i > 0` —
with the leading rune taken through it at `i == 0` (`:675`). Go's
`unicode.IsLetter` is general category `L` and `unicode.IsDigit` is `Nd`,
nothing else.

**It is narrower than "allow non-ASCII", and the boundary is measured.**
29 rune classes were put to the pinned oracle at `| drop X`
(`grafana/loki@sha256:87f0a067…`, buildinfo `3.7.4` / `b318f282`,
`ci/logql/config.yaml`, two lines pushed into the store first). The rule
above reproduces **all 29 verdicts with zero mismatches**:

| class | example | reference |
|---|---|---|
| `L` — Ll, Lo, Lm, Lt | `éx`, `日本語`, `ʰx`, `ǅx` | `200` |
| `Nd`, non-leading | `x٣` (U+0663), `x३` (U+0969) | `200` |
| `Nd`, leading | `٣x`, `3x` | `400` |
| `Mc` / `Mn` combining mark | `का` (U+0915 U+093E), `e` + U+0301 | `400` |
| `Nl` letter-number | `x` + U+2167 | `400` |
| `No` other-number | `x½`, `x³` | `400` |
| `So` / `Cf` / `Co` / `Cn` | `x🙂`, `x` + U+200D + `y`, `x` + U+E000, `x` + U+0378 | `400` |

**No Rust std predicate is this rule.** Measured code-point counts:
`char::is_alphabetic` 147,421 vs Go `L` **136,104**; `char::is_numeric`
1,924 vs Go `Nd` **680**. `is_alphabetic` is true for U+093E (`Mc`) and
U+2167 (`Nl`); `is_numeric` is true for U+00BD (`No`). The tables are
therefore committed general-category `L`/`Nd`, generated from
`regex-syntax` 0.8.11 (`src/unicode_ident_tables.rs`, 677 + 71 ranges)
with an ASCII fast path.

**Keyword folding moved with it.** The reference folds a keyword with Go
`strings.ToLower` at LEX time (`pkg/logql/syntax/lex.go:226`). Enumerated
over the whole code space with Go 1.25.5, exactly **two** non-ASCII
identifier runes lower to an ASCII letter: **U+0130** `İ` → `i` and
**U+212A** (KELVIN SIGN) → `k`. Confirmed at the wire: `| <U+212A>EEP ax`
`200`; `| drop <U+212A>EEP` `400 unexpected keep, expecting IDENTIFIER`;
`sum by (İGNORING)` `400 unexpected ignoring, expecting IDENTIFIER or )`;
`| logfmt | addr = İP("1.2.3.4")` `200`. Rust's `char::to_lowercase` is
the FULL mapping and yields `"i\u{307}"` for U+0130, so it cannot be used
— the fold is a two-entry table with the enumeration as its proof
(`the_two_non_ascii_runes_that_fold_to_ascii_are_exactly_u0130_and_u212a`).
This falsified `parser.rs:90-92`'s standing claim that ASCII folding was
safe "because a non-ASCII identifier cannot lex"; the comment is corrected
in the same change.

#### Residual 1 — `{éx="m"}`: reference `400`, PulsusDB `200`. DELIBERATE.

The reference's **lexer tokenises `éx` in a selector too**. The `400`
comes from a round trip below the parser: the query-frontend
re-serialises the parsed AST and vendored Prometheus
`labels.Matcher.String()` quotes any name outside
`[A-Za-z_][A-Za-z0-9_]*`
(`vendor/github.com/prometheus/prometheus/model/labels/matcher.go:81-104`,
`shouldQuoteName` at `:97-104`), producing `{"éx"="m"}` — which LogQL's
own grammar has no production for.

**The proof it is the round trip and not the lexer:** `{"éx"="m"}`
returns the **byte-identical** error at the **identical** column as
`{éx="m"}` — `parse error at line 1, col 2: syntax error: unexpected
STRING, expecting IDENTIFIER or }`.

Adopting a `shouldQuoteName`-shaped refusal would reproduce a defect
deliberately, so we serve it (owner ruling, 2026-08-10). Censused as an
`over-acceptance` row in `crates/pulsus-logql/tests/case_folding.rs`.

#### Residual 2 — `{app="x"} | éx="v"`: the reference does not answer. DELIBERATE.

The same re-serialisation, but this position's rewritten query reaches
the querier and `500`s, and the frontend retries it. **Measured on the
pinned container, five probes:**

| probe | result | elapsed |
|---|---|---|
| 1 | `500` | 28.1 s |
| 2 | no HTTP status — `curl` exit 52, *Empty reply from server* | 37.5 s |
| 3 | no HTTP status — `curl` exit 52 | 39.0 s |
| 4 | no HTTP status — `curl` exit 52 | 37.4 s |
| 5 | no HTTP status — `curl` exit 52 | 37.4 s |

The container log shows the re-serialised query and the retry storm:
`caller=retry.go:107 … query="{app=\"…\"} | \"éx\"=\"b\"" … code=Code(500)`
on `try=0` through `try=4`, then
`(500) 37.397651986s Response: "failed to enqueue request"`.

We serve it (`200`, empty). A future reader comparing us against the
reference here needs to know **the reference does not answer**, which is
why the measurement is recorded rather than the verdict alone. **This
position is never probed by the live leg** — it burns ~40 s and floods
the shared oracle's scheduler with retries, which perturbs the CI steps
that run after it; the row carries
`Skip("no HTTP status in ~40 s — measured five times")` and the skip
count is pinned.

#### A third reference behaviour, recorded so "serves" does not paper over it

With a **matching** stream, `| logfmt éx="b"` and `| label_format éx="y"`
are a **`500`** at the reference — `could not write JSON response:
1:75: parse error: unexpected character inside braces: 'é'` — because the
response encoder re-parses the rendered label set. The **extraction
destination itself keeps its bytes on both sides**: the reference
validates the identifier rather than sanitizing it
(`model.UTF8Validation.IsValidLabelName(exp.Identifier)`,
`pkg/logql/log/parser.go:518 @ v3.7.4`), measured over the line
`ax=7 bx=8` —
`sum by (éx) (count_over_time({…} | logfmt éx="ax" [1m]))` returns a
series labelled `éx`, while `sum by (_x) (…)` over the same query returns
none. PulsusDB matched the sanitizing behaviour before #392 and now
matches the reference (`KeyOrigin::QueryIdentifier`,
`crates/pulsus-read/src/logql/pipeline.rs`); line keys are still
sanitized, which is also what the reference does.

#### Unicode version skew — 16.0.0 here, 15.0.0 there. DELIBERATE, with a tripwire.

`regex-syntax` 0.8.11 carries Unicode **16.0.0**; Go 1.25.5
(`unicode.Version`) carries **15.0.0**. Measured across the whole code
space, in **both** directions:

| category | in 16.0.0 and not 15.0.0 | in 15.0.0 and not 16.0.0 |
|---|---|---|
| `L` | **4,924** | **0** |
| `Nd` | **80** | **0** |

A strict one-directional superset, so **nothing the reference accepts is
refused here**; the only effect is that PulsusDB accepts code points
Unicode 15.0 leaves unassigned. Pinning a 15.0-era table nothing in the
Rust toolchain can regenerate was refused as a maintenance hazard traded
for a difference that cannot currently reject a valid query (owner
ruling, 2026-08-10).

**The tripwire is what makes that a decision rather than an oversight.**
The Unicode 15.0.0 baseline is committed
(`crates/pulsus-logql/tests/unicode15/go-1.25.5-general-categories.txt`,
659 `L` + 64 `Nd` maximal ranges) and
`the_unicode_version_skew_is_one_directional` re-checks the claim on
every run: it fails the moment a code point that is `L` or `Nd` at the
reference stops being one here. `the_committed_unicode_tables_are_regex_syntax_general_category`
fails first if a dependency bump leaves the committed tables stale, so a
bump cannot widen the accept surface silently. The baseline was produced
by walking `0..=0x10FFFF` with Go 1.25.5's `unicode.IsLetter` /
`unicode.IsDigit` and coalescing maximal runs:

```go
for r := rune(0); r <= 0x10FFFF; r++ { if unicode.IsLetter(r) { /* coalesce */ } }
```

#### What is NOT closed by this entry

Issue #392 stays open for two things the ruling kept on it:

- **Keyword resolution at lex time.** The reference resolves keywords in
  the lexer, so a keyword spelling can never be an identifier payload
  there: `| drop json`, `| drop by`, `| drop ignoring`, `| drop KEEP`,
  `sum by (ignoring)` and `| drop İgnoring` are all `400`; we accept all
  of them. Only `{by="x"}` / `{json="x"}` were censused before #392,
  which added `| drop İgnoring`; the other pipeline and grouping
  positions are still uncensused.
- **The `--flag` scan set.** `crates/pulsus-logql/src/lexer.rs:123-132`
  scans ASCII alphanumerics plus `_`/`-`; the reference's `tryScanFlag`
  scans `unicode.IsLetter(r) || r == '-'`
  (`pkg/logql/syntax/lex.go:273-290 @ v3.7.4`). No verdict differs today
  only because `parserFlags` is a fixed ASCII pair — the sets are not the
  same and nothing checks that the equivalence still holds.

#### Where this is gated

- `crates/pulsus-logql/tests/identifier_charset.rs` — the 29-row rune
  boundary, the 18-position grammar enumeration, the discriminators, the
  committed-table check, the version-skew tripwire, and the live leg
  (`PULSUSDB_LOGQL_DIFF_URL`, its own CI step).
- `crates/pulsus-logql/src/unicode_ident.rs` — the ASCII fast-path
  equivalence and the whole-code-space fold enumeration.
- `crates/pulsus-logql/tests/case_folding.rs` — the two new
  `over-acceptance` census rows and the two Unicode-folded keyword
  spellings in the live `FOLDING_PROBES`.
- `crates/pulsus-read/src/logql/pipeline.rs` — the extraction
  destination keeps its bytes, and the `KeyOrigin` carve-out is proved a
  no-op for every pre-#392 identifier.
- `crates/pulsus-read/tests/sql_snapshots.rs` — a non-ASCII label name
  plans to byte-identical SQL, so no index, projection or pushdown
  changes (Tier 1 identity, no wall-time claim).

### `logfmt-expression-duplicate-source-key-tiebreak` (issue #393 — the reference has no answer here, so there is nothing to match)

- **What issue #393 CLOSED, so this row is not read as covering it.**
  `| logfmt <id>="<expr>"` now evaluates the way
  `LogfmtExpressionParser.Process`
  (`pkg/logql/log/parser.go:531-624 @ v3.7.4`) does: every identifier is
  pre-seeded to `""` before the line is read, one document-order pass
  chooses each pair's destination by SANITIZED line key (source keys
  first, then identifiers), the last write wins, a repeated identifier
  keeps only its last source key, and a value containing `U+FFFD` is
  emptied. Those are parity, gated by
  `crates/pulsus-read/tests/logqltest/corpus/b24_logfmt_expr_eval.test`
  and by the `#393` block in
  `crates/pulsus-read/src/logql/pipeline.rs`'s test module. What remains
  is the single case below, which is not a behaviour of the reference at
  all.
- **The case.** Two DIFFERENT extraction identifiers naming the SAME
  source key — `| logfmt a="x", b="x"` — leaves the reference choosing
  which identifier a matching line key renames to, and it chooses by
  iterating a Go map: `for id, orig := range keys`
  (`parser.go:594-599`), whose order the Go runtime deliberately
  randomises per iteration. There is no rule to port.
- **Measured, on `grafana/loki:3.7.4` digest
  `sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`
  (buildinfo read from the running process: version 3.7.4, revision
  b318f282, branch release-3.7.x), 2026-08-10.** One line `x=3 y=4`
  pushed once, then each query issued 30 times against
  `/loki/api/v1/query_range` in one process, the answers tallied with
  `collections.Counter` after dropping the container's injected
  `detected_level`:

  ```
  for query in ['{service_name="lfe5"} | logfmt a="x", b="x"',
                '{service_name="lfe5"} | logfmt b="x", a="x"',
                '{service_name="lfe5"} | logfmt a="x", b="y", c="x"']:
      c = Counter(q(query) for _ in range(30))
  ```

  | query | outcome A | outcome B | split (A/B) |
  |---|---|---|---|
  | `a="x", b="x"` | `{a="3", b=""}` | `{a="", b="3"}` | 29 / 1 |
  | `b="x", a="x"` | `{a="", b="3"}` | `{a="3", b=""}` | 21 / 9 |
  | `a="x", b="y", c="x"` | `{a="3", b="4", c=""}` | `{a="", b="4", c="3"}` | 23 / 7 |

  An independent earlier run of the first and third shapes, same method,
  split 25/5 and 21/9; a run before that split 23/7 and 16/14. The splits
  do not converge and are not the point — the point is that BOTH outcomes
  occur for every shape.

- **PulsusDB's rule: QUERY ORDER.** The first-declared identifier whose
  source key matches wins;
  `crates/pulsus-read/src/logql/pipeline.rs`'s `logfmt_target_for`
  scans the compiled extraction list in the order the user wrote it.
- **Why this is the "be correct where they are wrong" case rather than a
  divergence we are choosing.** A query cannot be given a stable answer
  by a reference that does not have one. Query order is the only order a
  user can predict from the text they typed, and it makes the answer
  reproducible across repeats, replicas and versions, which the reference's
  is not. **Do not "fix" this determinism back toward the reference's coin
  flip.** Note in particular that the reference does not merely differ from
  query order on average — a single capture of `b="x", a="x"` in the same
  session answered `{a="3", b=""}`, i.e. the SECOND-declared identifier
  won, so any attempt to reproduce it by rule will disagree with the next
  measurement.
- **Blast radius.** Only a query that names one source key from two
  identifiers. A repeated IDENTIFIER (`a="x", a="y"`) is not this case:
  the reference resolves it deterministically at construction
  (`paths[exp.Identifier] = path`, `parser.go:521` — last declaration
  wins) and PulsusDB ports that exactly.
- Gated by `b24_logfmt_expr_eval.test`'s `lfe5` rows, each carrying
  `# provenance: divergence(logfmt-expression-duplicate-source-key-tiebreak)`,
  and by `pipeline.rs`'s
  `two_identifiers_sharing_a_source_key_are_broken_by_query_order`.

### `logfmt-expression-parser-hints-unmodelled` (issue #393 — a residual whose printed value MOVED at this commit, and why that is not a regression)

**Read this row before reading the metric answers below as a regression.**
One of them changed at the commit that closed #393, and it changed
because the extraction became CORRECT while a second, independent
mechanism stayed unmodelled. Two axes, one moved.

- **The mechanism.** The reference gates line-key extraction on
  `ParserHint.ShouldExtract` (`pkg/logql/log/parser.go:580 @ v3.7.4`),
  with `alwaysExtract` (`:554-558`) as the escape hatch for a key that is
  itself an extraction identifier. `Hints.ShouldExtract`
  (`pkg/logql/log/parser_hints.go:73-85 @ v3.7.4`) answers true only for
  a REQUIRED label — one the rest of the query actually needs — or for
  every key when the query requires none. A `sum by (a) (…)` makes `a`
  required and nothing else, so a line key that is not `a` and is not an
  identifier is skipped, and the identifier's PRE-SEEDED empty string
  survives to the grouping. `git grep ShouldExtract -- crates/` returns
  doc comments only: PulsusDB extracts every key, always.
- **This is invisible for the implicit parsers** (`| logfmt`, `| json`,
  `| regexp`, `| pattern`), because grouping discards the extra labels
  anyway. Only the EXPRESSION parsers make it observable, and only
  because of the pre-seed — there has to be an empty value already
  sitting under the identifier for the skipped extraction to leave
  something behind.
- **Measured on `grafana/loki:3.7.4` digest
  `sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`
  (buildinfo read from the running process: version 3.7.4, revision
  b318f282, branch release-3.7.x), 2026-08-10**, one line per stream
  pushed once and each query issued at a single instant through
  `/loki/api/v1/query`:

  | line | query | reference | PulsusDB before #393 | PulsusDB now | moved? |
  |---|---|---|---|---|---|
  | `b=1 a-b=2 x=3` | `sum by (a) (count_over_time(… \| logfmt a="nosuch" [5m]))` | `{a=""}` | `{}` | `{a=""}` | **moved, and now AGREES** |
  | `b=1 a-b=2 x=3 b.c=4` | `sum by (a) (count_over_time(… \| logfmt a="b_c" [5m]))` | `{a=""}` | `{}` | `{a="4"}` | **MOVED, still differs — this row** |
  | `b=1 a-b=2 x=3` | `sum by (a) (count_over_time(… \| logfmt a="x" [5m]))` | `{a=""}` | `{a="3"}` | `{a="3"}` | **did NOT move** |
  | `b=1 a-b=2 x=3` | `sum by (a,x) (count_over_time(… \| logfmt a="x" [5m]))` | `{a="3"}` | `{a="3"}` | `{a="3"}` | agrees — the control |

- **Why the second row moved, stated plainly.** Before #393 the source
  key `b_c` was compared against RAW line keys, matched nothing, and the
  empty-drop rule then removed the label entirely — so the series carried
  no `a` at all and `by (a)` grouped it to `{}`. #393 fixed both halves:
  the line key `b.c` now sanitizes to `b_c` and matches, and a miss now
  leaves the pre-seeded empty label instead of nothing. The extraction is
  therefore CORRECT now — `| logfmt a="b_c"` really does read `4` on the
  reference too, which the streams row in
  `b24_logfmt_expr_eval.test` pins. What still differs is only whether
  the reference KEEPS that value once it knows the query needs only `a`:
  it does not, because `ShouldExtract("b_c")` is false, so its
  pre-seeded `""` survives. The printed value changed and is still not
  the reference's, and both of those facts have separate causes.
- **Why the fourth row is the control.** Add `x` to the `by` clause and
  the source key becomes a required label, `ShouldExtract` returns true,
  and the reference extracts and renames it — answering `{a="3"}`, which
  is what we answer unconditionally. The divergence is exactly the hint,
  not the rename, not the pre-seed and not the sanitizer.
- **Why the third row is the one that did NOT move.** Its source key `x`
  was already matching before #393 (a raw line key equal to the source
  key needs no sanitizer), so #393 changed nothing about it. It is the
  same mechanism as the second row and the same direction, but it was
  divergent at `ec774ee` and is divergent now with the identical value —
  which is what makes the pair distinguishable: one row's value changed
  at this commit and one did not.
- **What closing it would take, and why it is not this issue's.**
  `ParserHint` is a whole-pipeline concern — it needs the set of labels
  the rest of the query requires threaded into every parser stage, and it
  changes the per-row cost model for all of them (it exists in the
  reference as an optimisation, not as a semantic). It touches
  `| json`'s expression parser identically. Not filed as a new issue by
  the #393 plan's ruling; recorded here so the metric rows above are
  checkable rather than remembered.
- Our side of every row above is gated by
  `b24_logfmt_expr_eval.test`'s first metric directive (the row that
  moved INTO agreement) and by its `lfe2` streams row, both of which
  redden when `crates/pulsus-read/src/logql/pipeline.rs` is checked out
  at `ec774ee`. The two rows that still differ carry no corpus
  expectation, deliberately: a corpus row asserting our answer would need
  a `divergence(...)` marker naming this id, and it would then be
  excluded from the live replay leg — which is the one thing that would
  notice if the reference's answer ever changed.

### `streams-result-budget` (issue #312, bounded divergence)

PulsusDB refuses a streams query whose PEAK RETENTION — staged rows plus
the assembled result — would exceed `MAX_STREAMS_RESULT_BYTES` = 1 GiB,
with a named `422 query_too_broad` carrying
`TooBroadReason::StreamsResultBytes`. The reference refuses too, at a far
smaller size and far worse; that difference is why this is a bounded
divergence rather than a gap.

- **Reference behaviour, container-measured against `grafana/loki:3.7.4`
  (`buildinfo.revision = b318f282`)** on a corpus of 5,000 x 1 KiB lines
  under `{job="i312b"}`, `query_range` over an 11 s window. Provenance:
  the capture posted on issue #312 as comment `5265167134`'s round-1
  table; reproduced here rather than re-derived.

  | query | HTTP | body / bytes | latency |
  |---|---|---|---|
  | `{job="i312b"}` limit=3000 | 200 | 3,156,741 B JSON | 0.17 s |
  | `{job="i312b"}` limit=3900 | 200 | **4,102,645 B JSON** | 0.058 s |
  | `{job="i312b"}` limit=3990 / 4000 / 4500 / 5000 | **no response at all** — server log records `(504) 1m0.008s` | 0 B | 60 s |
  | `… \| line_format "{{ repeat 1000 __line__ }}"` limit=1 | 200 | 1,027,734 B | 0.34 s |
  | `… \| line_format "{{ repeat 1000 __line__ }}"` limit=10 | **500** | 104 B `text/plain; charset=utf-8` | 17.1 s (4 internal retries) |
  | `… \| line_format "{{ repeat 100 __line__ }}"` limit=5000 | **500** | 104 B `text/plain` | 16.8 s |

  Verbatim 500 body: `rpc error: code = ResourceExhausted desc = trying to
  send message larger than max (13113586 vs. 4194304)`. Server-log cause
  of the 504 class: `rpc error: code = ResourceExhausted desc = grpc:
  received message larger than max (4733318 vs. 4194304)` /
  `(5258815 vs. 4194304)`. Two `4194304`-byte gRPC ceilings sit on the
  reference read path — ingester→querier (batches of 128 entries;
  `13113586 = 128 x 102400` confirms the batch size) and
  querier→frontend (the whole result). Neither truncates. Neither serves.

- **What we do instead, and why it is the "except where they are wrong"
  clause:** the reference's three experiences at its ceiling are a raw
  gRPC internals string leaked as `text/plain`, a 60-second hang ending
  in a `504`, or a served result. Refusal is therefore parity in kind;
  only the size and the message differ. PulsusDB answers a named `422`
  with a legible body, at a cap **256x** the largest streams response the
  reference could serve here (4,102,645 B), so nothing the reference
  serves is refused — the `#236 (f)` direction.

- **Units.** The cap is denominated in RETAINED bytes; the observable
  guarantee is `stats.bytes <= MAX_STREAMS_RESULT_BYTES` on the wire, and
  the ratio is about two because `alloc_block_bytes` doubles. Wide-label
  results are charged up to 3x their true footprint, so their effective
  ceiling is ~357,913,941 B (~341 MiB) — still ~87x the reference's
  largest served response.

- **Residual (issue #312, not fixed here) — the ENCODED BODY.** The cap
  bounds retained bytes. It does not bound the encoded response, which is
  bounded only derivatively:

  - **Encoded-body factor:** a rendered stream item is at most `3 ×` its
    charged bytes. Derived over everything the item carries — the
    `{"stream":` / `,"values":[` / `]}` wrapper (23 B), `labels_json`,
    per-entry framing plus up to 20 timestamp digits plus the separator
    comma and `json_string`'s two quotes (28 B/entry), and `serde_json`'s
    six-for-one `\u00XX` expansion of a C0 control byte — against the
    same item's charge (`map_entry_bytes(STREAM_GROUP_SLOT)` = 1,032,
    `6·|labels_json|` from `grown_alloc_bytes`, `STREAM_ENTRY_SLOT` = 32
    per entry, and `≥ 2·Σ|line|`). Every term of the output is dominated
    by three times its own contribution to the charge, and `6L ≤ 3·2L`
    is exact, so the factor is tight rather than nominal. Hence at most
    **3,221,225,472 B (~3.0 GiB)** encoded for a query at the cap.
  - **The input that reaches it:** one stream, 5,000 entries, every line
    107,340 B of `0x01`. That is admitted (`5,000 × (alloc_block_bytes
    (107,340) + 32)` = 1,073,560,000 B plus a ~1,448 B group charge,
    inside 1,073,741,824) and renders `5,000 × 107,340 × 6` =
    3,220,200,000 B.
  - **Measured at CI scale:** 200 entries × 512 B of `0x01`,
    `labels_json` 22 B — `rendered = 619,844 B`, `charged = 212,204 B`,
    ratio **2.921**. Pinned by
    `logs_api/encode.rs::the_encoder_body_factor_matches_what_the_renderer_produces`,
    which parses the `3 ×` above out of THIS file at run time, so
    breaking the document alone reddens the test.

  - **Categorised encoded-body factor (#463):** at most `3 ×` its charged bytes.

    A rendered CATEGORISED stream item — the three-element `values` shape
    `X-Loki-Response-Encoding-Flags: categorize-labels` asks for.

    A SECOND anchor, not a re-use of the one above, and the reason is
    arithmetic rather than preference. `alloc_block_bytes(n) = max(2n,
    32)` and `grown_alloc_bytes(n) = 3·alloc_block_bytes(n)`, so every
    shape's rendered/charged tends to 3 from below — a C0 control byte
    renders as six characters and is charged two — while the third
    element adds strictly more FIXED per-entry overhead (two `Vec`
    spines through `grown_alloc_bytes`, plus `size_of::<EntryCategories>()`
    = 48 B) than it adds amplification. So the categorised ratio is
    LOWER than the plain one and a single anchor could never be forced to
    move by it.

    **Derivation, from the binding variant.** 200 entries, each a 512-byte
    all-`\u{0001}` line, carrying structured metadata `{"kk1": <512 B of
    \u{0001}>, "__error__": <the same>}` — the framing that carries BOTH
    category objects. Driven through the shipped fast-path accumulator
    and the shipped item renderer: `rendered = 1,860,644 B`,
    `charged = 759,596 B`, ratio **2.450**. The other three framings
    measure 2.433 (`structuredMetadata` only), 2.431 (`parsed` only) and
    2.383 (`{}`), so the two-category framing is the binding one.

    **The charge's term list**, which `entry_category_bytes` destructures
    so a new field without a term is a build failure:
    `structured_metadata`, `parsed`. Each pair's key and value go through
    `alloc_block_bytes`; each vector's spine goes through
    `grown_alloc_bytes`; plus `size_of::<EntryCategories>()`.

    Pinned by
    `logs_api/encode.rs::the_categorised_body_factor_matches_what_the_renderer_produces`,
    which parses the `3 ×` on this bullet's first line out of THIS file
    at run time, and by
    `the_categorised_charge_terms_match_the_destructured_bindings`, which
    reads the term list above out of this file and compares it against
    the function's own bindings.

- **Peak HEAP while rendering — measured, and reduced by this issue.**
  The encoder renders one item at a time, so the transient is one item,
  never the whole body. Before #312 that item cost **1,612** allocations
  and a peak of **4.28 × R** (`R` = the rendered item's length); the
  single-buffer render (`render_stream_item_into`, writing framing,
  timestamps and `serde_json::to_writer`-escaped lines in place) and the
  zero-copy `Step::Sep` separator chunk bring it to **1** allocation and
  **1.00 × R** for lines needing no escape, **2.09 × R** at the escaped
  extreme. Whole-response figures over the same corpus: **15**
  allocations and **1.01 × R** benign, **18** and **2.22 × R**
  adversarial. Those whole-response numbers are a recorded MEASUREMENT
  (reproduced by issue #312's round-4 reviewer), not a gated property;
  the per-item figures ARE gated, by
  `logs_api/encode.rs::ac19_render_path_peak_and_allocation_profile`,
  which pins peak live BYTES — `(1, 108045, …)` benign and
  `(4, 1296540, …)` adversarial — because an allocation COUNT is blind to
  allocation SIZE.

  When escaping expands past the reservation the buffer grows, and the
  house model prices that peak at `grown_alloc_bytes(R) = 3 ×
  alloc_block_bytes(R) ≤ 6R` (`charge.rs`) — **measured 2.09 R**, so the
  MODEL is conservative by ~2.9x. Composed with the `3 ×` encoded-body
  factor: peak ≤ **18 × charged** modelled, ≈ **6.7 × charged** measured
  at the worst shape. The `6R` figure is a model; the `2.09 R` is a
  measurement, and they are labelled here so nobody quotes one as the
  other.

## Issue #463 — `X-Loki-Response-Encoding-Flags: categorize-labels`

### `encoding-flags-echo-order` (issue #463, owner ruling — ordering only, and ours is the stable one)

- **What diverges: the ORDER of the echoed `encodingFlags` array, and
  nothing else.** The token CONTENT is the reference's own, echoed
  verbatim: unknown flags pass through unchanged, whitespace is not
  trimmed, matching is exact and case-sensitive, empty tokens are kept,
  and duplicates collapse. Fourteen header shapes were measured and all
  fourteen agree on content — see criterion 6's table in
  `crates/pulsus-server/src/logs_api/params.rs`.

- **The reference's own order is NOT STABLE.** `ParseEncodingFlags`
  builds a fresh `map[string]struct{}` per request and the marshaller
  walks it, so a two-token header comes back in either order from the
  same process. Measured: 200 requests with `foo,categorize-labels`
  split **183 / 17** between the two orders. An earlier reading of eight
  agreeing repeats as evidence of per-process stability was wrong and is
  withdrawn — under a 183/17 split, eight agreeing draws have
  probability `0.915^8 ≈ 0.49`, a likelihood ratio near 2 against the
  alternative, which is not evidence of anything.

- **What we do:** first-occurrence request order, deterministically. The
  same tokens, in the order the client sent them.

- **Read this row as ordering only.** It is NOT "we reorder their flags":
  there is no reference order to preserve. A later reader who tries to
  "fix" this back will be matching a map walk.

- **One state the reference cannot enter, and what we do in it.** Our
  decision is `flag present AND every stream can serve a third element`;
  theirs is the flag alone. When a stream cannot serve one, ours turns
  off — and the echo then drops `categorize-labels`, because echoing it
  beside two-element values is exactly the parser desynchronisation the
  whole design exists to prevent. Unknown tokens are unaffected. The
  reference has no answer here because it cannot reach the state; this
  is our rule in it, and it is the safe direction (a feature loss, never
  an unparseable body).

- **Close condition:** the reference emits a stable order. Nothing else.

- **Pinned by** `logs_api/params.rs`'s
  `the_encoding_flags_header_parses_as_the_reference_does` (content and
  decision, all fourteen cases) and `logs_api/encode.rs`'s
  `the_advertisement_and_the_arity_are_one_decision` (the removal
  clause, on both envelopes).

### `categorize-tail-noop-pipeline` (issue #463, owner ruling — the reference is wrong here, and we do not copy it)

`witness: T4` · `contrast: T8` · `control: T2` · `alternative:
rename-colliding-metadata`

- **What T4 shows.** Query `{app="<probe>"}` — no pipeline — tailed LIVE
  with the header. The metadata-bearing stream object has **lost `app`**
  from its `stream` map and carries the raw, **unrenamed** `app` inside
  `structuredMetadata`; its sibling plain-entry object in the same frame
  still retains `app`. A renderer that renamed on collision — which is
  what every other tail path does — would have produced the control's
  frame instead.

- **T8 isolates the cause.** Same delivery path — live — and the same
  fixture; the only change is the query, `` {app="<probe>"} |= `tail` ``.
  The stream object **retains `app`** and the metadata key is renamed to
  **`app_extracted`**. One line filter restores the label, so the
  variable is the PIPELINE, not the path. Mechanism:
  `pkg/ingester/tailer.go:181-184 @ grafana/loki v3.7.4 b318f282` returns
  the raw stream and short-circuits, and only when
  `log.IsNoopPipeline(t.pipeline)` holds.

- **T2 shows the delivery path is not implicated.** The same no-pipeline
  query delivered as CATCH-UP returns `app` in the `stream` map and
  `app_extracted` in the metadata — the correct behaviour. Without it
  this row would read as "the reference deletes stream labels on tail",
  which is false of half its own behaviour. The defect is exactly
  `noop ∧ live`, measured in all eight cells of the
  pipeline × path × header grid.

- **What we do:** categorise uniformly, on every tail query and every row
  age, so PulsusDB answers what the reference's own catch-up path
  answers. A response whose shape changes because of an unrelated
  pipeline detail is not a contract, and the parity mandate's
  "except where they are wrong" clause is what this is for.

- **On the reference's merge deduplication, corrected.** An earlier
  reading of this path said there is no entry-level deduplication.
  There is, but it is WINDOWED: `mergeEntryIterator.fillBuffer`
  (`pkg/iter/entry_iterator.go:116-155`) buffers entries while the
  stream hash AND timestamp match, and drops an equal one inside that
  buffer. Three reachable routes leave both copies alive — a different
  stream hash, different structured metadata, or the copies not being
  co-resident in the buffer. `EntryAdapter.Equal` also compares
  `Parsed` (`pkg/push/push.pb.go:493-500`), but **no reachable input
  producing differing `Parsed` between a catch-up and a live copy was
  found for the two query shapes probed** (`| logfmt`, one entry and
  two) — that is two query shapes, not a domain, and two holes in the
  derivation that once claimed more are named on the issue. None of this
  changes what PulsusDB implements: our tail has one source and no
  historical/live merge, so it has no handoff to race at.

- **Close condition:** the reference stops short-circuiting the noop
  pipeline on the live path.

- **Residual:** nothing mechanically prevents a later edit from
  substituting a non-discriminating witness, contrast or control. The
  reviewer of any change to those three ids, or to the probes they name,
  re-checks the three sentences above against the capture.

### `tail-stream-object-granularity-unflagged` (issue #463 — RECORDED here; CLOSED 2026-08-29 by #469)

`witness: T17` · `alternative: group-by-stream-map`

**CLOSED 2026-08-29 (issue #469). Nothing is downgraded and nothing
diverges here any more; the row stays as the record of why the tail
renders the way it does.** Both tail paths — with and without
`categorize-labels` — now emit one stream object per entry, ordered by
`(timestamp_ns, labels_json, fingerprint, entry_index)`, because the
granularity is taken from the WIRE SURFACE and no longer from the request
flag. `T17` stops being a divergence witness and becomes a replay: our
`streams` array must EQUAL the captured reference's, object for object,
gated by
`categorize_labels_differential.rs::pulsus_replays_the_granularity_witness_frame_object_for_object`
(live) and by
`encode.rs::the_tail_frame_emits_one_object_per_entry_in_timestamp_order`
(hermetic). The alternative below is what those gates reject.

- **What diverged, before 2026-08-29.** On the tail WITHOUT
  `categorize-labels`, the reference emitted **one stream object per
  entry, in strict timestamp order**; PulsusDB grouped a frame's entries
  by their label set. This predated issue #463, which changed only the
  flagged path — and the flag reaches this route only when an operator
  has configured it as a datasource proxy header, so the path every
  default client takes stayed packed until #469.

- **What T17 shows.** Two streams differing in one label, entries
  interleaved in time — prod@t, staging@t+1, prod@t+2 — pushed once and
  tailed with no pipeline and no header. Three stream objects come back
  in timestamp order, and **the identical prod map appears twice, on
  either side of staging**. A renderer that groups values by their
  `stream` map emits **two** objects here and renders the rows
  `A1, A3, B2` — log lines out of order in a tail view.

- **Why THIS frame and not the `| logfmt` one.** With a parser and no
  header the parsed label folds into `stream`, so the three maps are
  already DISTINCT and a grouping renderer emits three objects too. That
  frame cannot tell the two behaviours apart; this one can, because the
  two prod maps are byte-identical. Mechanism:
  `pkg/querier/tail/tail.go:114-125 @ grafana/loki v3.7.4 b318f282`
  appends one `logproto.Stream` per entry as it pops the oldest from the
  merge iterator.

- **Both tail paths now do the same thing.** One object per entry,
  ordered by `(timestamp_ns, labels_json, fingerprint, entry_index)`. The
  timestamp key is the reference's own order; the rest is our
  deterministic tiebreak, because the reference's tie order is its merge
  tree's arrival order and is not reproducible from the data (the
  `timestamp-tie-order` precedent, and see the tail-route paragraph in
  that entry).

- **The query route is deliberately NOT changed, and that is the control
  that makes this safe.** Measured on the pinned reference at
  `discover_log_levels: false`, on ONE container at one moment: the same
  interleaved fixture comes back as three objects on
  `/loki/api/v1/tail` and as two on `/loki/api/v1/query_range`. So
  splitting is a property of the tail wire surface, not a global rule,
  and `encode.rs::the_query_response_still_packs_a_streams_entries_into_one_object`
  is what keeps the query response packed.

- **What it costs.** Splitting repeats the label map once per entry:
  `(entries − streams) × (len(labels_json) + 14)` bytes on the one
  surface that buffers a whole frame. `reader.tail_max_fetch_limit`
  bounds the entries in one frame and is the operator's knob for it. No
  cap was introduced — the reference has the same property, and inventing
  one would be a divergence created by the fix.

- **Close condition (met):** issue #469 decided that the unflagged tail
  adopts per-entry objects.

- **Residual:** nothing mechanically prevents a later edit from
  substituting a non-discriminating witness. Whoever changes T17, the
  named alternative, or the probe it cites re-checks the discrimination
  sentence above. **A tie fixture in particular proves nothing unless its
  values separate storage-hash order from lexicographic order from push
  order** — three candidates, and push order is the one nobody separated
  for eight revisions of the plan behind this row.

### `categorize-instant-log-query` (issue #463 — no reference answer to match, so none is claimed)

`reference: F2-ref` · `pulsus: F2-pulsus`

- **What the pair shows.** `GET /loki/api/v1/query?query={app="…"}` with
  the header: the reference **rejects** an instant log query with `400`
  and a plain-text message (`log queries are not supported as an instant
  query type, please change your query to a range query type`), while
  PulsusDB **plans it as a streams query** and answers `200` with a
  categorised body.

- **The divergence is in WHAT IS SERVED, not in the categorisation.**
  Serving the instant log query predates this issue
  (`crates/pulsus-read/src/logql/plan.rs` routes `Expr::Log` for any
  spec); what issue #463 adds is that our answer follows exactly the
  rules `query_range` follows — the same advertisement placement, the
  same third element, the same all-or-nothing switch — and that
  `/api/logs/v1/query` and its `/loki/api/v1/query` alias are
  byte-identical.

- **So there is no parity claim here**, and the two ids are recorded as a
  one-sided pair rather than as a comparison: `F2-ref` can only be
  captured from the reference, `F2-pulsus` can only be replayed against
  PulsusDB, and neither side has an answer for the other's probe.

- **Close condition:** the reference serves instant log queries, or
  PulsusDB stops.

### `json-expression-bracket-key-unreachable` (issue #388, deliberate WITHDRAWAL, owner ruling 2026-08-13)

- **What is withdrawn.** A JSON key containing `]` was reachable through
  a `| json <id>="<expr>"` extraction at `5d91ef1` and is not after this
  change. It is the one capability #388 removes, and it is removed
  because the reference cannot express it either.

- **Measured** 2026-08-13 against the pinned oracle
  (`grafana/loki@sha256:87f0a067…f756cfcc`, in-process identity
  `3.7.4` / `b318f282` read from `/loki/api/v1/status/buildinfo`), over a
  line carrying the key: `{"b]":"brk", …}`. Both bodies are literal
  captures, not paraphrases:

  | probe | PulsusDB `5d91ef1` | the reference `v3.7.4` | PulsusDB now |
  |---|---|---|---|
  | `\| json v="b]"` | `200`, `v="brk"` | `400` — `parse error : stage '\| json v="b]"' : cannot parse expression [b]]: syntax error: unexpected RSB` | `400` |
  | `\| json v="[\"b]\"]"` | `400` — `index must be a number or a quoted key` | `400` — `parse error : stage '\| json v="[\"b]\"]"' : cannot parse expression [["b]"]]: syntax error: unexpected STRING, expecting RSB` | `400` |

- **The second row is why the loss is TOTAL rather than a change of
  spelling.** The bracket-quoted form is the escape hatch that reaches
  every other punctuated key — `[ "b-c" ]` and `[ "b c" ]` both work on
  both sides — but it does not reach this one, because `scanStr`
  terminates on `]` as well as on `"`
  (`pkg/logql/log/jsonexpr/lexer.go:124-125 @ v3.7.4`). So after this
  change no expression addresses such a key at all.

- **Why it is taken.** Being able to extract something the reference
  cannot is a query that works here and does not port: the user builds on
  it and discovers the gap when the query moves. A capability nobody else
  offers, on a pathological key, is not worth a divergence.

- **Search space for existing use, with its scope stated.**
  `git grep -n -E '\| json [^"[:space:]]+="[^"]*\][^"]*"' -- crates/pulsus-read/tests/logqltest/corpus`
  → eight rows, all array-index paths, no `]` key. **That search covers
  the corpus directory and nothing else** — it says nothing about the
  rest of the tree or about user data, and the withdrawal is taken
  anyway. Recorded as reading R13 in
  `crates/pulsus-read/src/logql/pattern_expr.rs`'s non-derivable table.

- **Pinned by** `b26_json_expr.test`'s two `eval_fail` rows and
  `json_expr.rs`'s
  `a_bracket_ends_a_quoted_key_so_such_a_key_is_unreachable`.
