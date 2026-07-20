# Metrics read-path perf: range binop reclaim, evaluate offload, M6-08 finalize reclaim

> **This report is Tier-1-gated evidence plus recorded wall-time context**
> for the three read-path optimizations of
> [#93](https://github.com/digitalis-io/pulsusdb/issues/93). Correctness is
> pinned by `crates/pulsus-promql/tests/promqltest_corpus.rs` (the vendored
> Prometheus v3.13 corpus + the hand-authored proof files) and the e2e
> metrics differential — both stay green. No generated SQL changed, so the
> `explain_indexes.rs` / `query_log_gates.rs` pruning gates are untouched.
> Wall times here are **recorded context only** — never a CI assert
> (#16/#34/#35 methodology; full-tier latency is
> [#25](https://github.com/digitalis-io/pulsusdb/issues/25)).

All numbers below are CH-free, in-memory `SeriesData`, **debug** (`cargo
test`, unoptimized) builds — treat them as **relative** A/B ratios, not
absolute latencies. Each is warmup-discarded with per-rep distributions.

## What is Tier-1 gated (hermetic, in `cargo test --workspace`)

| Gate | Test | Proves |
|---|---|---|
| Range-binop allocation bound | `pulsus-promql` `tests/binop_range_alloc.rs` | allocations-per-(series × step) for the `group_right` range shape stay ≤ 25/cell |
| M6-08 finalize reclaim | `pulsus-promql` `eval::tests::finalize_skips_the_matrix_merge_pass_when_no_series_is_drop_marked` | `EvalCounts::finalize_matrix_merge_passes == 0` for a `drop_name`-free range, `> 0` for a `drop_name` case |
| Evaluate offload | `pulsus-read` `metrics::exec::tests::offloaded_evaluate_does_not_starve_the_reactor` | on a `current_thread` runtime a concurrent task makes progress DURING an offloaded eval; the inline contrast arm starves (failure-mode proof) |

The wall-time measurement harness is the `#[ignore]`-by-default
`pulsus-promql` `tests/binop_range_bench.rs` (asserts nothing about time).

---

## Finding 1 — range vector-vector binop allocation reclaim (profile-first)

### Profile (committed breakdown)

Method: a counting global allocator (the `logql_pipeline_alloc.rs`
pattern) over one warmup-excluded `evaluate()` of a `group_right` range
query — `foo / on(g) group_right bar`, 4 one-side (`foo`) series and 32
many-side (`bar`, 4-label) series, 100 steps (3,200 output cells). Each
suspect allocation site was neutralized in turn and the total re-measured;
the drop attributes that site's share.

| Allocation site (per matched pair, per step) | allocs/cell | share |
|---|---|---|
| **`MatchState::many_matched` duplicate-detection set** — clones the full `(Labels, Option<String>)` output identity into a `HashSet` every step | **12.4** | **34%** |
| Output labels `ls.labels.clone()` (the many side's labels, materialized into the output `InstantSample`) | 9.1 | 25% |
| Remainder — `matching_key`/`one_by_key` (`Labels::only`/`without`), `metric_name` clones, the range accumulator, finalize | 14.6 | 41% |
| **Total** | **36.1** | 100% |

**Hotspot identified: the `many_matched` duplicate-detection set** — the
single largest source, and a per-(step × many-side series) full deep label
clone that is rebuilt every step even though the identity set is stable
across steps. This is NOT the plan's leading hypothesis
(`matching_key`/`one_by_key`, which the breakdown shows is a minor ~3/cell
term) — profile-first paid off.

### Fix (scoped to `eval/binop.rs`)

The profiled hotspot is the **inner** many-to-one output-identity set,
which cloned the full `(Labels, Option<String>)` identity every matched
pair. `MatchState` now keys that inner set on the 64-bit
`identity_hash` of the output identity (`many_matched: HashMap<MatchKey,
HashSet<u64>>`) — the exact analogue of upstream's inner `insertSig :=
metric.Hash()` (a `uint64` where a collision is likewise accepted;
engine.go @ 40af9c2 L3258).

**The SIGNATURE-level dedup stays COLLISION-FREE, matching upstream
exactly** (round-2 review correction). Upstream computes each series'
join **signature ordinal** once in `rangeEval` (`signatureToOrdinal
map[string]int` keyed on the signature *bytes*, L1478 — collision-free)
and indexes `matchedSigsPresent []bool` / the OUTER slot of `matchedSigs`
by that ordinal. We mirror that by keying `one_to_one_matched` and the
OUTER key of `many_matched` on the full `MatchKey` (collision-free
equality). The `MatchKey` is cloned only on the **first sight of each
distinct signature** (a handful per step), never per matched pair, so the
reclaim holds without introducing a signature-level hash-collision risk
that upstream does not have. Only the inner output-identity hash inherits
upstream's accepted 64-bit collision semantics. The corpus + e2e
differential prove the reclaim outcome-neutral; a targeted unit test
(`eval::binop::tests::many_to_one_signature_dedup_is_collision_free`)
pins the collision-free outer / hashed-inner split.

### Tier-1 alloc gate

`tests/binop_range_alloc.rs`, `group_right` range (4×8 series, 100 steps,
3-label many side):

| | allocs/cell |
|---|---|
| Pre-#93 (full-identity clone, both outer + inner) | 30.06 |
| Post-#93 (collision-free `MatchKey` outer + hashed inner identity) | 20.44 |

Bound: **≤ 25/cell** — post-fix passes with margin, the pre-fix clone
fails (verified by reverting `binop.rs`), and any per-step operand rebuild
/ super-linear regression fails by orders of magnitude. (The collision-free
outer `MatchKey` — cloned only per distinct signature — costs ~0.4/cell
over a fully-hashed outer, a negligible fraction of the ~10/cell reclaimed
from the inner output-identity clone.)

### Wall-time A/B (recorded, `binop_range_bench.rs`)

**In-binary interleaved A/B of the reclaimed operation** (`dedup_hotspot_ab`
— A = pre-#93 clone-into-`HashSet`, B = post-#93 hash-into-`HashSet<u64>`,
the per-step dup-detection set for 32 identities × 200 steps, 60 reps,
rotated order):

| | min | median | p90 | max |
|---|---|---|---|---|
| A (base) | 6.25 | 7.01 | 8.11 | 9.58 |
| B (opt) | 3.62 | 4.35 | 4.67 | 6.62 |

(µs) — **median B/A = 0.62 (≈ 38% faster)** on the isolated hotspot.

**End-to-end `evaluate()`** (`evaluate_range_shapes`, `group_right` range,
40 reps, median): pre-#93 **32.4 ms → post-#93 25.9 ms (≈ 20% faster)**.
(`group_right` arithmetic output is `drop_name`-free, so it also benefits
from the Finding 3 finalize short-circuit; the two reclaims compound here.)

> **Round-2 correction (collision-free signature dedup):** the outer
> signature key was reverted from a 64-bit hash to the collision-free full
> `MatchKey`. This is allocation-negligible — the deterministic alloc gate
> moved 20.06 → 20.44/cell (the `MatchKey` is cloned only per distinct
> signature, not per matched pair), so the reclaim above is unchanged in
> character. The reclaimed inner output-identity hash (the profiled
> hotspot, isolated by `dedup_hotspot_ab`) is untouched by the correction.

---

## Finding 3 — M6-08 count_values-range finalize reclaim

`finalize_metadata_labels`'s `Matrix` arm ran a full per-series clone +
`HashMap` merge pass on **every** range result, including the common case
where no series is `drop_name`. In that case it is provably a no-op: the
range accumulator already deduped on the identical `(metric_name, Labels)`
identity and pre-sorted, and both metadata strips are guarded on
`drop_name`. The arm now short-circuits (returns the matrix verbatim) when
no element is `drop_name`.

### Attribution + gate (`EvalCounts::finalize_matrix_merge_passes`)

The new internal `EvalCounts` field counts how many times the full pass
ran. `finalize_skips_the_matrix_merge_pass_when_no_series_is_drop_marked`
asserts it is **0** for a `drop_name`-free `count_values("v", …)` range and
**> 0** for a `rate(…)` (`drop_name`) range — both branches covered, values
asserted unchanged so the short-circuit is proven outcome-neutral.

### Wall-time (recorded)

`evaluate_range_shapes`, `count_values("v", bar)` range (32 series, 200
steps, 40 reps, median): full finalize pass **7.21 ms → short-circuit
6.76 ms (≈ 6% faster)**. `count_values` does not use vector-vector
matching, so this delta isolates the finalize reclaim. The finalize pass
is therefore one attributable component of the M6-08 count_values overhead;
the residual (GroupKey / delayed-model churn) is a redesign, out of this
"reclaim what is provably cheap" issue's scope.

---

## Finding 2 — `pulsus_promql::evaluate` offloaded off the reactor

`MetricsEngine::query_inner` now runs the CPU-bound, multi-hundred-ms-at-
scale `evaluate` via `tokio::task::spawn_blocking` (extracted as
`evaluate_offloaded`), so a heavy range eval no longer pins a tokio worker.
`plan`/`data` are owned + `Send + 'static` and moved into the closure; every
`ChRowStream` was drained in `fetch_rows` before this point, so no pooled-
connection lease crosses the offload.

### Cancellation / concurrency bound (accurate)

Tokio does **not** cancel a `spawn_blocking` task when its awaiter is
dropped — but this changes nothing versus the pre-#93 synchronous path:

- **Concurrently executing** evals are bounded by tokio's
  `max_blocking_threads` (default **512**) — a bounded constant, the
  offloaded analogue of the old synchronous ceiling of `worker_threads`.
- **Queued** (not-yet-started) evals are bounded only by request arrival
  rate / the upstream `TimeoutLayer` — which is **already true today**:
  pending query tasks queue on the tokio scheduler identically.
- A request future can only be dropped **between** polls, and the
  synchronous `evaluate` ran inside a single await-free poll, so on client
  disconnect or the 408 timeout it **already ran to completion** before the
  drop was observed. `spawn_blocking` does not introduce this.

`spawn_blocking`'s only genuine deltas are (i) evals no longer pin a
reactor worker (the latency win) and (ii) the executing ceiling rises from
`worker_threads` to `max_blocking_threads` — a larger constant, still
bounded.

Issue #101 tightened this: the read path now carries a process-wide
eval-concurrency permit (`EvalGate`, an owned `Arc<Semaphore>` in
`AppState`, sized by `reader.query_eval_concurrency`, **default 256**).
`evaluate_offloaded` acquires the permit **after** the ClickHouse fetch has
fully drained into owned `SeriesData` and moves the owned permit **into**
the `spawn_blocking` closure, so it is released only when the blocking eval
actually finishes — bounding both in-flight and queued evals, including
evals for already-disconnected clients (which tokio will not cancel). The
uncontended path is a single lock-free `try_acquire_owned` (no clock, no
atomic, no waker), so a query that fits under the limit is never serialized
or slowed; the default 256 sits below tokio's 512 blocking-pool ceiling so
evals can never monopolize the pool, yet above realistic heavy-query
fan-in. Exhaustion is a bounded wait (`acquire().await`) bounded by the
existing per-request `TimeoutLayer` (`408`, `query_timeout`) — no new
429/503 status and no new timeout knob (this deliberately differs from the
tail slot's fail-fast `429`, which holds a slot for a connection's whole
lifetime). The gate exports six `pulsus_query_eval_*` metrics on
`/metrics`: gauges `pulsus_query_eval_permits_limit`,
`pulsus_query_eval_permits_available`, `pulsus_query_eval_in_flight`,
`pulsus_query_eval_waiting`; counters `pulsus_query_eval_contended_total`,
`pulsus_query_eval_wait_nanoseconds_total`. Hermetic counting/identity gates
(`crates/pulsus-read/src/eval_gate.rs`, plus AC6 in `metrics/exec.rs`)
prove the bound is tight, the fast path is uninstrumented, and the permit
spans the whole blocking closure without any wall-time assert.

The only reachable `JoinError` is a **panic** in `evaluate` (we own and
directly await the handle, never cancel); it is re-raised via
`std::panic::resume_unwind` to preserve panic-on-bug behavior exactly — no
new `ReadError` variant (a panic is not a domain error).

---

## Reproduce

```text
# Tier-1 gates (hermetic, no ClickHouse):
cargo test -p pulsus-promql --test binop_range_alloc
cargo test -p pulsus-promql eval::tests::finalize_skips_the_matrix_merge_pass_when_no_series_is_drop_marked
cargo test -p pulsus-read metrics::exec::tests::offloaded_evaluate_does_not_starve_the_reactor

# Wall-time A/B evidence (never in CI):
cargo test -p pulsus-promql --test binop_range_bench -- --ignored --nocapture
# End-to-end pre/post: run the same on the parent commit vs this one.
```
