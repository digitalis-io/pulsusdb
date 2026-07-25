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
tumbling cases (range/step, divergent by design); `scope_structured_metadata`
(needs per-entry structured-metadata modelling, not yet in the DSL);
`fetch_until_limit_paged` (keyset paging / result-limit — an exec/SQL concern,
not the pure value path).
