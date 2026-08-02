# logqltest corpus — provenance & capture procedure

The `logqltest` corpus (issue #220) is a promqltest-style set of LogQL
`.test` files replayed **hermetically** by `tests/logqltest_corpus.rs`
against the pure value evaluator in `pulsus-read` — no container at test
time. Every expected value is **bit-exact** (`f64::to_bits`, no tolerance —
the #218 lesson): a one-ULP perturbation reddens the runner
(`a_perturbed_expected_value_reddens_the_runner`).

## Pinned reference

- **Container (capture only):** `grafana/loki:3.7.4`.
- **Semantics of record:** the Go checkout at `/home/hayato/git/loki @ v3.7.4`
  (`pkg/logql/`).
- Instant metric queries are **semantically identical** to the reference
  (`docs/features.md`), so their goldens are bit-exact against the container.
- **Range queries (issue #227)** now evaluate Loki's **sliding** windows
  bit-exactly, so `eval range from <T0> to <T1> step <S>` is in scope. Two
  capture disciplines:
  - **Integer-exact reducers** (count / bytes / sum-of-small-ints / min /
    max / first / last / clean-division rate / absent): the sliding-window
    result is integer-exact in f64 and **hand-derived** against
    `pkg/logql/range_vector.go`'s `batchRangeVectorIterator` (half-open
    `(t-range, t]`, popBack/load, start-anchored grid, empty-window gap) —
    Loki-exact by construction, no container needed. `b9_range_sliding.test`.
  - **Float-drift-sensitive class-C folds** and **cross-stream StableHash
    same-nanosecond tie** cases MUST be captured from the container (their
    bit pattern depends on Loki's exact fold/heap order) — these live in the
    env-gated live `schema-it` differential, NOT this hermetic file. The one
    ratified divergence (same-ns SAME-stream `tie_rank` order) is documented
    in `docs/features.md` / the differential ledger.

## `.test` DSL

```
# comment
load
  {env="prod", service_name="svc-json"} service=svc-json   # stream: labels [+ service scope]
  0s   {"status":500,"method":"GET"}                        # <offset> <body>
  5s   {"status":200,"method":"POST"}

eval instant at 60s {service_name="svc-json"} | json | status = "500"
  {env="prod", service_name="svc-json", status="500", method="GET"} 0s {"status":500,"method":"GET"}

eval instant at 60s count_over_time({service_name="svc-json"} | json | status = "500" [30m])
  {env="prod", service_name="svc-json", status="500", method="GET"} 2

clear
```

- **Directives** (column 0, blank-line separated): `clear`, `load`,
  `eval instant at <T> <query>`, `eval_ordered instant at <T> <query>`
  (sort/sort_desc — ordered compare), `eval_fail instant at <T> <query>`
  (followed by `msg: <substring>`).
- **Stream header:** `{labelset} [service=<name>]`. Without `service=` the
  `service_name` label supplies it. **Sample line:** `<offset> <body>`
  (offset is a duration from T0).
- **Streams expected line:** `{labelset} <ts> <line>` (compared as a SET;
  label order is irrelevant, the runner sorts).
- **Metric expected line:** `{labelset} <value>` (exact f64). A bare
  scalar result is a single bare `<value>` line. Ordered evals compare the
  vector positionally.
- **Dataset == query scope:** an eval replays every loaded line through the
  query pipeline (the stream selector is pushed to SQL in the real engine and
  is not re-applied here). Author each `load`…`clear` section so its streams
  are exactly the scope the query selects; use `clear` to switch service.

## Capturing a NEW case's expected value (the ONLY reason to touch the container)

This is done **once** per case, offline; CI runs only the hermetic runner.

1. Start the pinned reference (digest-pinned to `grafana/loki:3.7.4`):

   ```
   podman run --rm -p 3100:3100 grafana/loki:3.7.4
   ```

   **`approx_topk` capture delta (issue #221):** the bare container above
   returns 500 `approx_topk is not enabled. See -limits.shard_aggregations`
   for EVERY `approx_topk` query. Capturing `b10_approx_topk.test` requires
   mounting a config (`-v <cfg>:/etc/loki/local-config.yaml:ro
   -config.file=/etc/loki/local-config.yaml`) that adds BOTH of these to the
   default config:

   ```yaml
   limits_config:
     shard_aggregations:
       - approx_topk
   frontend:
     encoding: protobuf
   ```

   The `limits_config` entry enables the construct; the protobuf frontend
   encoding makes the frontend ship the serialized query PLAN downstream —
   without it the `approx_topk` rewrite's internal `__count_min_sketch__`
   subquery is re-parsed from its string form by the querier and every
   query 400s with `parse error at line 1, col 1: syntax error: unexpected
   IDENTIFIER` (loki `pkg/loki/config_compat.go` documents the coupling).
   If ingestion fails on a tight disk, add `--tmpfs /loki:rw,size=512m`.

   **`variants` capture delta (issue #221):** capturing `b13_variants.test`
   additionally requires `limits_config.enable_multi_variant_queries: true`
   — the bare container returns 400 `multi variant queries are disabled
   for this instance` for EVERY `variants(...) of (...)` query (verified
   on both the v3.7.4 capture image and the then-v3.7.3 differential
   oracle image; the oracle has since been re-pinned to this same v3.7.4
   image, so the two are now one). One config serves every capture: `ci/logql/config.yaml`
   carries all three deltas. Transcription: the container attaches
   `detected_level`/`detected_level_extracted` labels to variants results
   that PulsusDB's label model has no analogue for — DROP them from
   expected label sets (values are unaffected).

2. Push the `load` dataset at the exact timestamps. For an `eval instant at
   T`, an entry written at offset `Δ` uses wall-clock `now - (T - Δ)` so the
   sample lands `Δ` before the query instant (Loki keys entries by
   nanosecond timestamp):

   ```
   curl -s -H 'Content-Type: application/json' http://localhost:3100/loki/api/v1/push \
     -d '{"streams":[{"stream":{"service_name":"svc-json"},
                      "values":[["<ts_ns>","<body>"]]}]}'
   ```

3. Run the query at `T` and read the exact value back:

   ```
   # metric (instant vector): resultType "vector", read result[].value[1]
   curl -s 'http://localhost:3100/loki/api/v1/query' \
     --data-urlencode 'query=count_over_time({service_name="svc-json"} | json | status = "500" [30m])' \
     --data-urlencode 'time=<T_ns>'

   # streams: resultType "streams", read result[].stream (labels) + values ([ts, line])
   ```

4. Paste the value **verbatim** as the expected line. For a float, write the
   shortest round-tripping decimal (Rust's `{}` form of the captured `f64`);
   the runner parses it back to the identical bits. Record the container
   digest / date in the `.test` file's header comment if the value is
   non-obvious (e.g. the `rate_counter` ns/ms extrapolation captures).

## `b15_wide_aggregation.test` — the issue #236 high-cardinality capture

- **Image:** `grafana/loki:3.7.4`, digest
  `sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`,
  **default single-binary config** (no `ci/logql/config.yaml` deltas — none
  of these constructs needs one). Captured 2026-07-30.
- **Dataset:** 507 lines / 501 distinct `| logfmt` `id` groups on one
  stream, spread 100 ms apart so a sliding range eval sees a different
  subset at each grid point; `bucket = id % 5` supplies a
  low-cardinality grouping label.
- **Anchoring:** the DSL's `at 60s` was mapped to a wall clock instant on
  an exact 30 s boundary. That matters only for the `eval range` case:
  Loki's `/query_range` grid is anchored on absolute time, so an
  unaligned anchor puts the container's grid points between the DSL's and
  the captured values do not correspond to any DSL instant. (Observed
  before the alignment: 485/322/22 where the aligned capture gives
  507/201.)
- **Boundary verified, not assumed:** the same dataset was captured at
  **499, 500 and 501** groups. Everything is served at 499 and 500; the
  rejections begin at exactly 501.

### The `topk(3, …)` row is BLOCKED, not captured

Plan v14 AC 4 lists `topk(3, …)` among the cases to capture as SERVED.
**The reference rejects it at 501 groups** — HTTP 400, the same
`maximum number of series (500) reached…` body as the `eval_fail` rows.
It is therefore absent from the file rather than captured from a
narrower dataset: a row asserting provenance it does not have is worse
than a missing row (the #240 discipline).

Measured at exactly 501 inner groups on the pinned image:

| served (200) | rejected (400) |
|---|---|
| `sum`, `count`, `min`, `max`, `avg` (bare and `by(bucket)`), `sum by (bucket)`, `sum(sum by (id) (…))` | `topk(k)`, `bottomk(k)`, `stddev`, `stdvar`, `sort`, `sum by (id)`, the bare leaf, `sum(topk(600, …))`, `count(topk(3, …))` |

The split is **shardability**: Loki's frontend rewrites the associative
aggregations into per-shard sub-queries, so the wide inner vector never
materialises in one evaluator; the others materialise it and trip
`max_query_series` on that intermediate. `sum(sum by (id) (…))` being
served while bare `sum by (id)` is rejected is the same mechanism, and it
is the case plan v14 AC 11 exists for.

**This contradicts plan v14 §1's live probe**, which recorded
`stddev(…)`, `topk(3, …)`, `approx_topk(3, …)` and `sum(topk(600, …))`
as 200 over 600 groups. Registered as a divergence in
`docs/benchmarks/logs-differential-ledger.md` (issue #236 entry (f)).

## Batch 0 seed provenance

Batch 0 ports the **instant-eval subset** of the 39 differential cases in
`test/fixtures/logs/differential.json`. The differential harness stores no
literal expected values (it is a live store-vs-store set comparison over a
seeded dataset); the seed here uses small **controlled** datasets whose exact
results are:

- **Streams cases** — deterministic pipeline output, already pinned
  byte-exact by `logql_pipeline_golden.rs` and validated against the
  reference by the differential harness (labels, lines, `__error__` /
  `__error_details__` strings).
- **Metric reducer / vector-agg / matching cases** — hand-derived from
  integer counts and exactly-representable durations (`250ms = 0.25s`), so
  every division/sum is a single exact IEEE operation.
- **`rate_counter` cases** — the bit-exact reference captures already pinned
  in `logql_metric_agg_golden.rs` (the #218 ns/ms `extrapolatedRate` factor,
  captured against the container); the datasets and values are ported
  directly.

Instant-eval values are stable 3.4.2 → 3.7.4 (verified: only cosmetic
range-vector reducer changes), so no re-capture was needed for the seed.

**Deferred to later batches / out of scope** (7 of 39): the 5 `metric_*range`
cases (superseded by issue #227 — range queries are now Loki-exact sliding
windows, covered by `b9_range_sliding.test` and the `eval range` directive);
`scope_structured_metadata`
(needs per-entry structured-metadata modelling, not yet in the DSL);
`fetch_until_limit_paged` (keyset paging / result-limit — an exec/SQL concern,
not the pure value path).

## Issue #230 — template-engine corpus (`t1…t6_*.test`)

The `t*` files pin the `line_format`/`label_format` template engine —
688 directives: 678 `eval` (t1 60 + t2 228 + t3 34 + t4 29 + t5 258 +
t6 69) plus 10 `eval_fail` reject-parity cases (all in t1) — every
value AND execution-error string captured verbatim from
`grafana/loki:3.7.4`, never hand-authored. (Re-derive with
`grep -c '^eval ' / '^eval_fail'` per file; an earlier "676 cases"
claim mixed the two directive kinds without saying so.) **Toolchain of record:**
the pinned image's binary is built with **go1.26.5** (`go version -m`
on the extracted binary) — semantics citations against an older local
Go tree are advisory only; on any disagreement the container capture
wins. Capture deltas from the base procedure above:

1. **Stock-environment precondition (load-bearing).** Run the container
   with **no `TZ`** and **no host zoneinfo mounted**, so Go's
   `initLocal` degenerates `Local` to the all-nil "UTC" form — the
   `t5` zone goldens (`Location` prints, `%d` struct dumps, MarshalBinary
   offsets) depend on it. The hermetic runner mirrors this by pinning
   `CompiledPipeline::with_template_env(TemplateEnv::default())`.
2. **Disable ingest-side level detection.** The stock config must gain
   `limits_config: { discover_log_levels: false }` (mounted like the
   `approx_topk` delta above) — otherwise the distributor injects a
   `detected_level` stream label into every captured labelset that the
   hermetic store would not reproduce.
3. **Capture through `query_range`.** v3.7.4 rejects instant queries on
   log selectors ("log queries are not supported as an instant query
   type"); capture with `/loki/api/v1/query_range` over a window
   covering the pushed entries. The `.test` replays as `eval instant`
   (the hermetic runner applies no window).
4. **Absolute-ns `load` offsets.** Entries are pushed at wall-clock ns
   and the `.test` records those exact stamps
   (`1785119131123456789ns`-style), so `__timestamp__` goldens replay
   hermetically at the captured instants.
5. **Representable outputs only.** A stream expected line cannot carry
   newlines, a leading `#`, leading/trailing whitespace, or be empty —
   templates whose output has any of these render into `label_format`
   values (escaped) or are bracketed (`[{{ … }}]`). The capture driver
   hard-fails on unrepresentable output instead of quietly mangling it.
6. **Exclusion classes stay out of the corpus.** The pinned-address and
   tzdata-table printf cells (ledger `template-pinned-address-cells` /
   `template-tzdata-table-cells`) and the rust-regex compile-error
   wordings are gated hermetically in `tests/logql_template_engine.rs`;
   a grep-gate there proves no `t*` golden contains a pinned-address
   token. `cargo run -p xtask -- template-audit` re-derives the
   address-cell evidence (dual-container diff + address-token scan) on
   any reference bump.
7. **`eval_fail` msg lines** follow the `b8_reject_parity` convention:
   the substring is of PULSUSDB's reject `Display` (whose inner text is
   the Go parse error verbatim); the capture proved the container 400s
   the same query before emitting each case.

## Issue #240 — error-body identity and rejection-status probes

The corpus rows whose produced error is a `ReadError::PipelineInvalid`
stringification AND whose committed prose claims reference-body identity are
pinned **byte-exactly** (`msg_exact:`, gated both directions by
`tests/logqltest_provenance.rs` checks A/B). Sources: `wave0 <date>` is a
fresh capture against the pinned `grafana/loki:3.7.4` container (the
`ci/logql/config.yaml` capture config — `shard_aggregations`, protobuf
frontend encoding, `enable_multi_variant_queries`); a `probe:` URL must
resolve to a published comment showing the capture. A row that can be
neither captured nor URL-cited is **BLOCKED** — reported to the
task-manager, never invented and never substituted from a different
query's capture. Three rows qualify (B1–B3); the fourth candidate is
blocked:

**B4 — BLOCKED (issue #240 AC10, reported).** The candidate row
(`b13_variants.test`,
`variants(sum_over_time({service_name="rj"}[5m])) of ({service_name="rj"}[5m])`)
has NO applicable reference capture: probing that exact query against the
pinned v3.7.4 container returns a 500 nil-pointer panic (`runtime error:
invalid memory address or nil pointer dereference` — exactly as the
b13 ledger entry records), so no reference body for the variants form
exists. The non-variants probe (`sum_over_time({service_name="rj"}[5m])`
→ `parse error : invalid aggregation sum_over_time without unwrap`)
captures a DIFFERENT query and must not be substituted as B4's
provenance. The corpus row therefore stays on a substring `msg:` gate
and claims no reference-derived byte identity.

Capture commands (2026-07-27, container digest per §Pinned reference; push
the row's `load` dataset first, exactly as §2 above):

```
# B1/B2: the two vector-matching runtime errors (HTTP 500 on the
# reference — the ledgered matching-error-status divergence):
curl -sw '\n%{http_code}\n' 'http://localhost:3100/loki/api/v1/query' \
  --data-urlencode 'query=sum by (method, status) (count_over_time({service_name="svc-json"} | json [30m])) / on(status) sum by (status) (count_over_time({service_name="svc-json"} | json [30m]))' \
  --data-urlencode 'time=<T_ns>'
# -> multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)  [500]
#    (B2: the same with `/ on(status) group_left sum by (method, status) (...)`
#     -> found duplicate series on the right hand-side;many-to-many matching
#        not allowed: matching labels must be unique on one side  [500])
# B3: any approx_topk on query_range:
#  -> count min sketches are only supported on instant queries  [500]
# B4 (BLOCKED — see above): the variants-form probe itself:
curl -sw '\n%{http_code}\n' 'http://localhost:3100/loki/api/v1/query' \
  --data-urlencode 'query=variants(sum_over_time({service_name="rj"}[5m])) of ({service_name="rj"}[5m])' \
  --data-urlencode 'time=<T_ns>'
# -> runtime error: invalid memory address or nil pointer dereference  [500]
#    (a nil panic, not a body — no applicable capture exists, so B4 has
#     no pulsus-240-bodies row and the corpus row stays on `msg:`)
```

```pulsus-240-bodies
| id | corpus-file | source | value |
| B1 | differential_vector_matching.test | wave0 2026-07-27 | multiple matches for labels: many-to-one matching must be explicit (group_left/group_right) |
| B2 | differential_vector_matching.test | wave0 2026-07-27 | found duplicate series on the right hand-side;many-to-many matching not allowed: matching labels must be unique on one side |
| B3 | b10_approx_topk.test | wave0 2026-07-27 | count min sketches are only supported on instant queries |
```

Rejection-status probes (issue #240 AC7(h) — the reference's status for an
uncompilable pushed-down regex is PROBED, not assumed; both 400, so the
plan-time rejection ships). Captured 2026-07-27; the same probes on the
other route each also returned 400 (`|~ "("` on `index/stats` is refused
earlier with `only label matchers are supported`, still 400):

```pulsus-240-status
| id | query | surface | reference-status |
| S1 | {service_name="svc-json"} \|~ "(" | /loki/api/v1/query_range | 400 |
| S2 | {app=~"("} | /loki/api/v1/index/stats | 400 |
```

Query-text-cap boundary capture (issue #279 AC9 — the source-derived
boundary is CONFIRMED on the wire: `maxInputSize = 131072`,
`pkg/logql/syntax/parser.go:42`, compared `>=` at `:86`, so the bound is
an exclusive maximum and the longest accepted query is 131,071 bytes).
Captured 2026-07-29 against `grafana/loki:3.7.4`
(`sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`),
`curl --data-urlencode query@<file>` (POST form) on `/loki/api/v1/query`;
query text built as `count_over_time({app="a…a"}[1m])` padded to the exact
byte length (and, corroborating, the bare-selector shape `{app="a…a"}`).
The 400 body is exactly 51 bytes with **no trailing newline** (`od -c`
verified), and the headers were `Content-Type: text/plain; charset=utf-8`
+ `X-Content-Type-Options: nosniff` — PulsusDB's JSON envelope container
divergence is #264's, not this row's.

```pulsus-279-cap
| id | query-bytes | shape | surface | reference-status | body |
| C1 | 131071 | count_over_time(...[1m]) | /loki/api/v1/query | 200 | {"status":"success",...} (empty vector) |
| C2 | 131072 | count_over_time(...[1m]) | /loki/api/v1/query | 400 | parse error : input size too long (131072 > 131072) |
| C3 | 131071 | bare selector | /loki/api/v1/query | 400 | log queries are not supported as an instant query type, ... (parser ACCEPTED the text; the rejection is the instant-log-query type check downstream of parse) |
| C4 | 131072 | bare selector | /loki/api/v1/query | 400 | parse error : input size too long (131072 > 131072) |
```

## Issue #244 — `/detected_fields` corpus (`b14_detected_fields.test`)

### The `eval detected` directive

```
eval detected at <T> [line_limit=<N>] [field_limit=<N>] {selector} [| pipeline]
  <label> <type> <cardinality> <parsers>
```

- Only the plain `eval` verb (`eval_ordered detected` / `eval_fail
  detected` are grammar errors, file-fatal). `<T>` is a duration offset;
  the runner is hermetic and applies no time bounds, so `<T>` is grammar
  symmetry with the other evals.
- `line_limit=` / `field_limit=` default to 100 / 1000 (the server's
  `DEFAULT_LINE_LIMIT`/`DEFAULT_FIELD_LIMIT`, the reference's defaults).
  A repeated key, non-numeric value, `0`, or a value `> 5000` is a
  grammar error. The query must start with `{`.
- One expected line per field, in the engine's label-sorted order,
  compared as an ORDERED list: exactly four whitespace-delimited tokens;
  `<parsers>` is `-` (no attribution) or a comma-separated list of
  `json`/`logfmt` in encounter order.
- **Selector scoping:** matchers are NOT applied — the section's loaded
  streams ARE the query scope (the real engine resolves matchers in
  stage-1 SQL, which the hermetic runner never executes); only the
  pipeline runs. A runner unit test locks this so it cannot be silently
  "fixed" into matcher filtering.
- A duplicate loaded timestamp anywhere in the section is a grammar
  error (rule A2 below); `retention_capped == true` is a case failure
  ("retention capped — a corpus case must not depend on the byte
  budget"), so no corpus expectation can be budget-dependent. The probe
  runs the PRODUCTION `MAX_DETECTED_FIELD_BYTES`.

Replay steps (the runner's `evaluate_detected`):

| step | operation | applies matchers? |
|---|---|---|
| a | `parse(query)`; a metric expression is a case failure with the engine's own text | — |
| b | `CompiledPipeline::compile`; a compile error is a case failure | — |
| c | flatten EVERY loaded stream's samples to `(fingerprint, timestamp_ns, body)` | **no** |
| d | duplicate-timestamp validation over the whole unfiltered sequence → grammar error | **no** |
| e | sort NEWEST-first by `timestamp_ns` DESC, ties `(fingerprint, body)` ASC | — |
| f | `probe.add_stream(fp, &base)` for EVERY loaded stream | **no** |
| g | feed in the order of (e); `feed_row` applies the `matched >= line_limit` gate | — |
| h | `probe.finish()`; `retention_capped == true` is a case failure | — |

Ordering justification: the engine's stage-3 `ORDER BY timestamp_ns
DESC` under `Direction::Backward`, matching the reference's
`Direction: logproto.BACKWARD` (`detected_fields.go:201`).

### Authoring rules

- **A0** — author each section so its loaded streams are exactly the
  scope its eval selects (the DSL's dataset == scope contract).
- **A1** — one stream per section unless the outcome is
  order-insensitive.
- **A2** — distinct timestamps across every loaded sample
  (grammar-enforced).
- **A3** — the `field_limit` case introduces each field on a DIFFERENT
  entry: the reference iterates Go maps per entry, so which fields
  survive `limit` is random when one entry carries more keys than the
  limit — a same-entry case must not be added.
- **A4** — label-sorted expected blocks; transcribe the captured fields
  as a set (the reference's order is Go map order); drop the reference's
  `jsonPath` key (#254 — PulsusDB does not emit it).
- **A5** — a case disagreeing for any reason other than the registered
  cardinality divergence is DROPPED AND FILED, never worked around.

### Reference citations (the #244 plan §0 block)

Pinned reference: `grafana/loki` v3.7.4, tag `v3.7.4` = commit
`b318f2829f0ae2094ab3a1e90780450e9e4b03be`; container
`grafana/loki:3.7.4`. `pkg/querier/queryrange/detected_fields.go` at
that SHA (482 lines):

| symbol | line | what it establishes |
|---|---|---|
| `parsedFields{sketch *hyperloglog.Sketch, …}` | `:226-231` | per-field state is a sketch; **no value bytes are retained** |
| `newParsedFields` → `hyperloglog.New()` | `:233-240` | the sketch constructor |
| `Insert(value)` → `p.sketch.Insert([]byte(value))` | `:242-244` | values are hashed and dropped |
| `Estimate() uint64` | `:246-248` | wire `Cardinality` is an **estimate**, consumed at `:69` |
| `parseDetectedFields` | `:282-360` | per-**entry** `map[string][]string`, dropped per entry; Go map order |
| `detectType := true` per (entry, key) | `:346-355` | type re-detected **per entry**, last processed wins |
| `Direction: logproto.BACKWARD, Limit: req.GetLineLimit()` | `:201-202` | the downstream sample is the **newest** `line_limit` entries |
| `validateMaxEntriesLimits` | `:189` | `line_limit` validated against `max_entries_limit_per_query` (default 5000) |
| `parseDetectedFieldValues` … `if len(values) >= int(limit) { break }` | `:134`, `:143` | the separate `/detected_field/{name}/values` path truncates silently; PulsusDB does not mount it |

`vendor/github.com/axiomhq/hyperloglog/hyperloglog.go`: `New()` `:27` →
`New14()` `:30` → `NewSketch(14, true)` `:46` — **p14, starts sparse**;
`Insert` `:148`; `Estimate` `:161`. The reference retains **no**
distinct values on `/detected_fields` — no per-field cap, no global cap,
no truncation signal, because there is nothing to truncate. Recorded
live probe (issue #244 §0): 200 × 64 KiB values at `line_limit=100` /
`5000` returns HTTP 200 with body `{}` — silently empty (the
querier↔ingester 4 MiB gRPC message cap); the reference's large-set
response is its ordinary zero-field response, not an error surface.

### Corpus capture procedure and transcript (2026-07-29)

Container: `grafana/loki:3.7.4` started with `ci/logql/config.yaml`
plus one delta — `limits_config.discover_log_levels: false` (matches
PulsusDB's model: no `detected_level` injection; the same setting every
prior detected-fields capture used). Push each case's `load` dataset at
`now - (T - Δ)` wall-clock timestamps (the §"Capturing a NEW case"
procedure), then:

```
GET /loki/api/v1/detected_fields?query=<selector>&start=<now-1h>&end=<now>[&line_limit=N][&limit=N]
```

Captured responses as a **NORMALIZED TRANSCRIPTION — not verbatim**.
The complete set of normalizations applied to each response body:

1. **Envelope unwrapped** — the body is `{"fields":[…],"limit":N}`; the
   `fields` array is transcribed inline and the envelope's own `limit`
   is recorded as the trailing `limit=N`.
2. **Object keys sorted alphabetically**, not kept in the response's own
   key order (hence `cardinality` first).
3. **`jsonPath` omitted** (A4; #254 — PulsusDB does not emit it).
4. **Fields transcribed as a SET** (A4) — the line order carries NO
   information, the reference's is Go map order (visible in C1, where
   `uid` precedes `lvl`).

Nothing else is altered: every label, type, parser list and cardinality
below is the captured value. The request side is likewise abbreviated to
the case id, selector and non-default params — the full request is the
URL template above.

```pulsus-244-detected-capture
C1 {app="c1"} (defaults)      -> [{"cardinality":3,"label":"uid","parsers":["json"],"type":"string"},{"cardinality":2,"label":"lvl","parsers":["json"],"type":"string"}] limit=1000
C2 {app="c2"} line_limit=2    -> [{"cardinality":1,"label":"uid","parsers":["json"],"type":"string"}] limit=1000
C3 {app="c3"} limit=2         -> [{"cardinality":2,"label":"alpha","parsers":["json"],"type":"int"},{"cardinality":2,"label":"beta","parsers":["json"],"type":"string"}] limit=2
C4 {app="c4"} limit=2         -> [{"cardinality":1,"label":"a_new","parsers":["json"],"type":"string"},{"cardinality":1,"label":"b_mid","parsers":["json"],"type":"string"}] limit=2
C5 {app="c5"} (defaults)      -> [{"cardinality":2,"label":"v","parsers":["json","logfmt"],"type":"string"}] limit=1000
C6 {app="c6"} (defaults)      -> [{"cardinality":1,"label":"k","parsers":["json"],"type":"string"}] limit=1000
```

Every captured cardinality is <= 100, where the reference's p14 estimate
equals the exact count (first divergence N = 5328) — pure hard-gated
parity, not divergences.

### Sketch-estimate capture procedure and transcript (2026-07-29)

`crates/pulsus-read/tests/golden/detected_cardinality/reference_divergence.tsv`
was captured by a THROWAWAY Go program placed inside the reference
checkout at `b318f2829f0ae2094ab3a1e90780450e9e4b03be` (so it links the
VENDORED `github.com/axiomhq/hyperloglog` at exactly the pinned
revision), run with `GOFLAGS=-mod=vendor`, then deleted — the checkout
verified byte-clean afterwards (`git status --porcelain` empty). Per
`n`, a fresh `hyperloglog.New()` sketch receives `Insert("v0")` …
`Insert("v{n-1}")` and reports `Estimate()`:

```pulsus-244-sketch-capture
5327  -> 5327   (exact; the last agreeing point)
5328  -> 5327   (first divergence — sparse-key collision "v2888"/"v5327", key 52686402)
7708  -> 7719   (first dense n for this family)
8192  -> 8230
10000 -> 10049
20000 -> 20155
50000 -> 49894
```

All five pre-committed additional points disagree, so all are kept
(the keep-iff-disagrees rule); the `5328` row is mandatory.
