# ADR 0001: ClickHouse client crate

Status: **Accepted** (2026-07-11)
Issue: [#3](https://github.com/digitalis-io/pulsusdb/issues/3)

## Context

[architecture.md §1.2](../architecture.md) left the ClickHouse client as an M0
decision between two disjoint-transport candidates:

- **`clickhouse`** (crates.io `clickhouse` 0.15.1) — HTTP interface, RowBinary
  wire format.
- **`klickhouse`** (crates.io `klickhouse` 0.15.3) — native TCP protocol.

[configuration.md §2](../configuration.md) requires columnar bulk-insert/fetch
performance and reliable DDL + `INSERT ... SELECT` maintenance; which
transport serves which statement class was left as an implementation detail
for this spike to settle with evidence (task-manager resolution on issue #3).

## Methodology

A dedicated `xtask` workspace crate (`xtask/src/ch_bench/`) implements a
`CrateUnderTest` trait once per candidate and runs identical scenarios
against both over a local ClickHouse 24.8 instance (the schemas.md §8
documented minimum supported version), so every number below is a real,
measured wall-clock result from this run, not an estimate.

**Hardware / environment (measured on):** 12th Gen Intel Core i7-1260P (16
logical CPUs), Linux 5.15 (WSL2), `rustc`/`cargo` 1.93.0. ClickHouse
24.8.14.39 (official build) run via `podman run --rm -p 9000:9000 -p
8123:8123 clickhouse/clickhouse-server:24.8`, no resource limits applied
(shares the host with other processes — see the row-count deviation below).

**Row shapes and codecs are byte-identical to the authoritative DDL**
(architect amendment, issue #3 Codex re-review finding 1):

- Metric-shaped rows/table: `docs/schemas.md §2.1` `metric_samples` —
  `metric_name LowCardinality(String)`, `fingerprint UInt64 CODEC(Delta(8),
  ZSTD(1))`, `unix_milli Int64 CODEC(DoubleDelta, ZSTD(1))`, `value Float64
  CODEC(Gorilla, ZSTD(1))`. Row generator uses **500 distinct `metric_name`
  values** (realistic `LowCardinality` cardinality) and includes a
  `fingerprint` value `> 2^63` in every rep.
- Log-shaped rows/table: `docs/schemas.md §3.1` `log_samples` —
  `service LowCardinality(String)`, `fingerprint UInt64`, `timestamp_ns Int64
  CODEC(DoubleDelta, ZSTD(1))`, `severity Int8`, `body String CODEC(ZSTD(1))`.
  **50 distinct `service` values**, nanosecond timestamps (not ms), ~200 B
  bodies. (Skip indexes from the full §3.1 DDL are a write-path concern, not
  a client-crate axis, and are excluded from the throughput measurement.)
- Aggregate-state tier: `docs/schemas.md §2.2` `metric_samples_5m` /
  `metric_samples_5m_mv` verbatim (`AggregatingMergeTree`,
  `SimpleAggregateFunction`, `AggregateFunction(argMin/argMax, ...)`), read
  via the exact `docs/schemas.md §2.3` shape
  (`finalizeAggregation(argMinMergeState(first_value))`, `sum()` over
  `SimpleAggregateFunction(sum, UInt64)`).

**Scenarios run through the shared `CrateUnderTest` trait, against both
candidates identically:** `insert`, `fetch`, `aggstate` (blocking correctness
gate), `ddl` (through **both** candidates' own transport — issue #3 Codex
re-review finding 2), `pool`, `tls`.

**Timing:** hand-rolled wall-clock (rows/s, MiB/s, p50/p95 over 5 reps), not
criterion — these are multi-second, network-bound bulk transfers, not
nanosecond microbenchmarks.

### Deviation from the architect plan: row count

The plan specified **N = 10,000,000 rows/shape**. This benchmark instead uses
**N = 1,000,000 rows/shape** (block size 200,000, 5 reps), because this
machine had ~1–1.4 GiB free RAM and a fully saturated swap file at the time
of the run (`free -h`, confirmed before and during the run) — it is a shared
development host running several concurrent, unrelated workloads, not a
dedicated benchmark machine. 10M rows/shape × 2 shapes × 2 candidates ×
5 reps risked OOM-killing the benchmark or degrading the host. 1M
rows/shape is still large enough to force multiple insert blocks (5× 200k)
and multiple parts per table, and the decision axis is **relative** rows/s
between the two candidates on identical hardware/data, which a 10×
row-count reduction does not change in kind. This is a disclosed deviation,
not a silent one; the full command to reproduce at N=10,000,000 on a
dedicated machine is:

```text
cargo run -p xtask --release -- ch-bench --scenario all \
    --rows 10000000 --block-rows 1000000 --reps 5 --pool-size 8 \
    --out /tmp/ch-bench-full.json
```

## Results (measured)

### 0. `LowCardinality` + codec fidelity gate (blocking)

**PASS for both.** Both candidates created the exact `metric_samples` /
`log_samples` DDL above (including `LowCardinality(String)`, `Delta(8)`,
`DoubleDelta`, `Gorilla`, `T64`, `ZSTD(1)` codecs) without error, inserted
1,000,000 rows/shape without silent type coercion, and round-tripped
`LowCardinality(String)` correctly on read (verified by the `fetch` scenario
reading `metric_name`-filtered projections and the `aggstate` scenario
grouping by `metric_name`).

### 1. Correctness gate (blocking)

**PASS for both.** A deterministic dataset (`fingerprint =
18446744073709551601`, i.e. `0xFFFF_FFFF_FFFF_FFF1 > 2^63`; 1,000 samples,
`value` strictly increasing 1.0..1000.0 within one 5-minute bucket) was
inserted, aggregated by the real `metric_samples_5m_mv`, and read back via
`finalizeAggregation(argMinMergeState(first_value))` /
`finalizeAggregation(argMaxMergeState(last_value))` and `sum(val_count)`
(`SimpleAggregateFunction(sum, UInt64)`):

| Crate | fingerprint round-trips | val_count (UInt64) | first_value | last_value |
|---|---|---|---|---|
| `clickhouse` | `18446744073709551601` (exact) | `1000` | `1.0` | `1000.0` |
| `klickhouse` | `18446744073709551601` (exact) | `1000` | `1.0` | `1000.0` |

Both bit-exact. Neither crate is disqualified by this gate.

### 2. Insert throughput (highest weight)

5 reps, N = 1,000,000 rows/shape, block = 200,000 rows, single connection:

| Shape | Crate | p50 | p95 | rows/s (p50) | MiB/s (p50) | parts after |
|---|---|---|---|---|---|---|
| metric | **clickhouse** | 2549.5 ms | 2715.6 ms | **392,229** | **15.3** | 6 |
| metric | klickhouse | 3714.8 ms | 4208.1 ms | 269,191 | 10.5 | 6 |
| log | **clickhouse** | 1169.5 ms | 1738.1 ms | **855,087** | **189.6** | 5 |
| log | klickhouse | 1783.3 ms | 1954.7 ms | 560,743 | 124.3 | 5 |

`clickhouse` is **46% faster on metric rows** and **53% faster on log rows**
(p50 rows/s) than `klickhouse`, on both documented row shapes, with
identical codecs and cardinality.

### 3. Streaming fetch

The §2.3 hot-path projection `SELECT fingerprint, unix_milli, value FROM
metric_samples PREWHERE metric_name = ...` (2,000 matching rows out of the
1,000,000-row table), 5 reps, run in an isolated process (no prior-scenario
memory in the same process) so peak RSS reflects only the fetch:

| Crate | p50 | rows/s (p50) | peak RSS |
|---|---|---|---|
| clickhouse | 8.1 ms | 245,633 | **7.5 MiB** |
| klickhouse | 10.0 ms | 199,922 | 8.4 MiB |

Both round-trip an identical checksum over the fetched rows (order-independent
XOR fold of `fingerprint`/`unix_milli`/`value.to_bits()`), confirming they
read the same data. Both stream — peak RSS scales with the ~2,000-row result
set, not the 1,000,000-row table, for both crates; neither materializes the
full result client-side. `clickhouse` is marginally faster and lower-RSS
here; the gap is not decision-significant on its own.

### 4. Aggregate-state read ergonomics

Both crates decode the §2.3 shape correctly (see gate 1). One ergonomic
asymmetry worth recording: `klickhouse` surfaces server errors as a
structured `KlickhouseError::ServerException { code: i32, name, message,
stack_trace }`, whereas `clickhouse` only exposes `Error::BadResponse(String)`
with the numeric exception code embedded in text (`"Code: 60. DB::Exception:
..."`), which `pulsus-clickhouse`'s `ChError::server_from_bad_response` must
parse out. This is a minor ergonomic edge to `klickhouse`, noted as a
tie-breaker input, not a correctness issue — `pulsus-clickhouse`'s parser is
unit-tested (`error.rs`) against both a well-formed and an unparseable
response.

### 5. TLS + pooling

**TLS:** a self-signed CA was generated (`xtask/docker/gen-certs.sh`) and a
ClickHouse instance with the secure ports enabled (native TLS 9440, HTTPS
8443, `xtask/docker/config.d/tls.xml`) was run locally. Both candidates
connected and round-tripped one insert + one fetch, in both verify modes:

| Crate | verify mode | insert | fetch |
|---|---|---|---|
| clickhouse | skip-verify | ok | ok |
| klickhouse | skip-verify | ok | ok |
| clickhouse | verified (self-signed CA) | ok | ok |
| klickhouse | verified (self-signed CA) | ok | ok |

Both crates' `rustls` integration works end-to-end for both verify modes;
TLS is not a differentiator between them. (Two environment-specific setup
issues were hit and fixed while standing this up — container-mounted key
file permissions, and installing an explicit process-wide `rustls`
`CryptoProvider` when more than one provider feature is reachable in the
dependency graph — both are one-line fixes now encoded in
`xtask/src/ch_bench/tls.rs` and `xtask/src/main.rs`, not open issues.)

**Pooling:** `pool_size = 8` concurrent inserters vs. one connection, N =
1,000,000 rows total (rows_per_conn = 125,000):

| Crate | single-conn rows/s | 8-conn rows/s | speedup |
|---|---|---|---|
| **clickhouse** | 1,605,525 | **2,160,148** | **1.35×** |
| klickhouse | 1,565,082 | 1,753,907 | 1.12× |

Both scale sub-linearly at 8 connections against a single local ClickHouse
instance (the server's own merge/insert-handling threads are the likely
bottleneck at this scale, not the client), but `clickhouse` extracts more
headroom from the same pool size.

### 6. DDL over the crate's own transport (decides one-crate vs. two-crate topology)

Per the task-manager's single-crate-preference ruling, the `ddl` scenario was
run through **both** candidates, each over its own transport — `clickhouse`
over HTTP, `klickhouse` over native TCP (issue #3 Codex re-review finding 2)
— issuing the exact §2.2 `CREATE TABLE metric_samples_5m` (`AggregatingMergeTree`
with `SimpleAggregateFunction`/`AggregateFunction` columns), the exact §2.2
`CREATE MATERIALIZED VIEW ... TO ... AS SELECT ... argMinState/argMaxState
...`, and a 4-chunk `INSERT ... SELECT` backfill (`WHERE cityHash64(fingerprint)
% 4 = i`) from the populated raw table:

| Crate | CREATE TABLE | CREATE MATERIALIZED VIEW | backfill chunks | reliable |
|---|---|---|---|---|
| clickhouse (HTTP) | ok | ok | 4/4 | **yes** |
| klickhouse (native) | ok | ok | 4/4 | **yes** |

**Both transports reliably serve DDL, MV creation, and chunked `INSERT ...
SELECT`.** This resolves the open question from configuration.md §2: the
"HTTP for DDL" split is not required — either crate's own transport is
sufficient by itself. Combined with `clickhouse` winning the throughput axis
(§2 above), **a single crate suffices for everything** (bulk insert,
streaming fetch, DDL, and maintenance): no second HTTP-only fallback path is
needed at this time.

## Decision rubric (scored)

| # | Axis | Weight | clickhouse | klickhouse |
|---|---|---|---|---|
| 0 | LowCardinality/codec fidelity | gate | PASS | PASS |
| 1 | Correctness (UInt64 > 2^63, agg-state) | gate | PASS | PASS |
| 2 | Insert throughput | highest | **wins** (+46%/+53% rows/s) | loses |
| 3 | Streaming fetch | high | **wins** (marginal) | loses (marginal) |
| 4 | Aggregate-state ergonomics | medium | tie (correctness); loses (error-code ergonomics) | tie (correctness); wins (error-code ergonomics) |
| 5 | TLS + pooling | medium | tie (TLS); **wins** (pooling 1.35× vs 1.12×) | tie (TLS); loses (pooling) |
| 6 | DDL over own transport | decides topology | reliable | reliable — but moot, since axis 2 already picks one crate |

Tie-breakers (both gates passed, so these did not need to be decisive): (a)
maintenance activity — both crates are on recent, actively released 0.15.x
lines; no material difference observed; (b) edge-type ergonomics —
`klickhouse`'s structured server-exception code is a minor ergonomic win,
outweighed by `clickhouse`'s wins on the two highest-weighted axes.

## Decision

**Use `clickhouse` (HTTP + RowBinary) for everything**: bulk columnar
insert, streaming fetch, DDL, and `INSERT ... SELECT` maintenance, all over
its own HTTP transport. `klickhouse` (native TCP) is **rejected**, primarily
on **insert throughput** (the highest-weighted axis and the one most
directly tied to configuration.md §2's "columnar bulk-insert/fetch
performance" hard requirement): 46–53% slower p50 rows/s than `clickhouse`
on both documented row shapes, at identical row cardinality and codecs. It
was not eliminated by the correctness or DDL gates — it passed both — and
remains a documented fallback (below).

Per the task-manager's evidence-based ruling (issue #3), no second
HTTP-only DDL crate is added: `clickhouse`'s own HTTP transport already
handles DDL/MV/backfill reliably (§6 above).

`pulsus-clickhouse` (`crates/pulsus-clickhouse`) is implemented against
`clickhouse` 0.15, with `rustls` (`ring` provider) for TLS,
`hyper-rustls`/`hyper-util` for the custom skip-verify connector, `thiserror`
for `ChError`, and `tokio`.

## Fallback path

If `clickhouse`'s HTTP transport later proves inadequate for a
decision-critical requirement not exercised by this spike (e.g. a hard
native-binary-protocol requirement is added to configuration.md, or an
HTTP-specific operational issue surfaces — a proxy/load balancer that does
not handle streaming RowBinary responses correctly, for instance), the
fallback is `klickhouse` (native TCP): it passed every correctness and DDL
gate in this spike and is already proven end-to-end (insert, fetch, DDL,
TLS, pooling), at the cost of the measured throughput gap. Because
`pulsus-clickhouse`'s public surface (`ChConnConfig`, `ChPool`, `ChClient`,
`ChError`, `QuerySettings`, `Idempotency`) is deliberately crate-agnostic —
only `client.rs`, `pool.rs`, and `settings.rs` import `clickhouse` directly —
swapping the winner is a wrapper-internals change, not a downstream-API
change. `klickhouse` is retained in `xtask`'s own `[dependencies]` (not
workspace-wide) specifically so this fallback path stays a runnable
regression harness (`xtask ch-bench`) rather than a from-scratch spike.

## Raw results

Full machine-readable output from this run: `insert`/`fetch`/`aggstate`/`ddl`/`pool`
scenarios captured together, plus the isolated fetch (peak-RSS) run and the
TLS run, are reproducible via:

```text
podman run -d --rm -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server:24.8
cargo run -p xtask --release -- ch-bench --scenario all \
    --rows 1000000 --block-rows 200000 --reps 5 --pool-size 8 \
    --out /tmp/ch-bench-full.json
cargo run -p xtask --release -- ch-bench --scenario fetch \
    --rows 1000000 --reps 5 --out /tmp/ch-bench-fetch-isolated.json

xtask/docker/gen-certs.sh
podman run -d --rm -p 9440:9440 -p 8443:8443 \
    -v "$PWD/xtask/docker/certs:/certs:ro" \
    -v "$PWD/xtask/docker/config.d/tls.xml:/etc/clickhouse-server/config.d/tls.xml:ro" \
    clickhouse/clickhouse-server:24.8
cargo run -p xtask --release -- ch-bench --scenario tls \
    --https-url https://127.0.0.1:8443 --native-tls-addr 127.0.0.1:9440 \
    --tls-server-name localhost --tls-ca-cert xtask/docker/certs/ca.crt \
    --out /tmp/ch-bench-tls.json
```
