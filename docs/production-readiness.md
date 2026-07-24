# Production-readiness checklist

M7 sub-area H1 (#177): an enumeration of operational-readiness areas, each item
mapped to concrete, in-tree evidence — a hermetic test, a live CI gate, a
committed doc section, or a code reference. The mapping is kept honest by a
hermetic anti-rot guard, `crates/pulsus-server/tests/production_readiness_doc.rs`
(`cargo test -p pulsus-server --test production_readiness_doc`), which parses
every citation token below and fails if the referenced test suite, code file,
test function, CI step name, or doc section no longer exists. A rename or
deletion of any cited artifact turns that guard red.

## Evidence model

Two tiers, per `schemas.md §9`:

- **Tier-1 (scale-invariant):** hermetic unit/integration tests and live CI
  gates that assert structure, correctness, and pruning/EXPLAIN shape. Every row
  below whose Status is ✅ is Tier-1 evidenced.
- **Tier-2 (scale-dependent):** throughput/latency claims — out of scope here;
  they belong to the benchmark harness, not this checklist. No wall-time
  assertion appears in a Tier-1 gate.

Evidence tokens use fixed, machine-checked forms so the guard can resolve them:

- integration suite — `` `cargo test -p <crate> --test <suite>` `` → `crates/<crate>/tests/<suite>.rs` exists.
- lib unit test / function ref — `` `crates/<crate>/src/<file>.rs::<fn>` `` → file exists and contains `fn <fn>`.
- plain code ref — `` `crates/<crate>/src/<file>.rs` `` → file exists.
- CI step — `ci.yml: "<step name>"` → that literal step name appears after a `name:` key in `.github/workflows/ci.yml`.
- doc section — `` `<doc>.md §N[.M]` `` → `docs/<doc>.md` has a heading line for that exact section number.

Status legend: ✅ implemented + evidenced · 🚫 explicit non-goal (recorded, not a blocker).

## 1. Config surface & `/config`

| Item | Evidence (test / CI step / doc § / code ref) | Status |
| --- | --- | --- |
| Typed config model, all scalars flat | `crates/pulsus-config/src/model.rs` · `configuration.md §1` | ✅ |
| Environment-variable overrides (allow-listed) | `crates/pulsus-config/src/env.rs` · `cargo test -p pulsus-config --test env_matrix` (rides `ci.yml: "Test"`) | ✅ |
| Fail-closed validation (e.g. TLS cert/key pairing) | `crates/pulsus-config/src/validate.rs` · `configuration.md §9` | ✅ |
| `/config` dumps effective config with secrets redacted | `crates/pulsus-server/src/ops.rs::config_handler_redacts_the_password` · `configuration.md §1` | ✅ |

## 2. TLS — inbound & ClickHouse

| Item | Evidence (test / CI step / doc § / code ref) | Status |
| --- | --- | --- |
| Inbound HTTPS listener (cert/key load, bounded handshake accept loop) | `crates/pulsus-server/src/tls.rs` · `tls_cert`/`tls_key` in `crates/pulsus-config/src/model.rs` | ✅ |
| Live inbound-TLS handshake suite | `cargo test -p pulsus-server --test tls_live` · `ci.yml: "Live inbound TLS suite"` · `api.md §7` | ✅ |
| ClickHouse-side TLS (`ChProto::Https`, `tls_skip_verify`) | `crates/pulsus-clickhouse/src/tls.rs` · `configuration.md §2` | ✅ |

## 3. Retention / TTL

| Item | Evidence (test / CI step / doc § / code ref) | Status |
| --- | --- | --- |
| TTL statements rendered from `retention_days` (saturating `DateTime`, incl. `u32::MAX`) | `crates/pulsus-schema/src/controller.rs::apply_ttl_all_statements_render_the_saturating_datetime_expression` | ✅ |
| TTL applied at startup and on the rotation timer | `crates/pulsus-server/src/serve.rs` · `crates/pulsus-schema/src/controller.rs` | ✅ |
| Retention/rotation/storage-policy config surface | `retention_days`/`rotation_interval`/`storage_policy` in `crates/pulsus-config/src/model.rs` · `configuration.md §3` | ✅ |
| Live schema TTL gates (logs/metrics + traces) | `ci.yml: "Live schema tests (single-node, migrations + MVs + crash/retry)"` · `ci.yml: "Live traces schema tests (tables + MV + EXPLAIN gates)"` · `architecture.md §3.6` | ✅ |

## 4. Backpressure & admission control

| Item | Evidence (test / CI step / doc § / code ref) | Status |
| --- | --- | --- |
| Queued-bytes reservation against a hard limit | `crates/pulsus-write/src/writer/mod.rs` · `queue_bytes_limit` in `crates/pulsus-write/src/writer/config.rs` | ✅ |
| Over-limit reservation rolls back and increments `backpressure_total` | `crates/pulsus-write/src/writer/mod.rs` · `pulsus_ingest_backpressure_total` on `/metrics` | ✅ |
| Ingest admission → HTTP 429 on backpressure | `crates/pulsus-server/src/traces_api/error.rs` · `architecture.md §4` · `api.md §1` | ✅ |

## 5. Error taxonomy

| Item | Evidence (test / CI step / doc § / code ref) | Status |
| --- | --- | --- |
| Per-surface error enums (logs / metrics / traces query APIs) | `crates/pulsus-server/src/logs_api/error.rs` · `crates/pulsus-server/src/prom_api/error.rs` · `crates/pulsus-server/src/traces_api/error.rs` | ✅ |
| Ingest & config error domains | `crates/pulsus-write/src/error.rs` · `crates/pulsus-config/src/error.rs` | ✅ |
| Status-code contract (400 `bad_data`, 422 `query_too_broad`, 429, 503) | `cargo test -p pulsus-server --test api_conformance` · `ci.yml: "API conformance matrix"` · `api.md §1` | ✅ |

## 6. Self-observability (`/ready`, `/metrics`, `/config`, `/buildinfo`)

| Item | Evidence (test / CI step / doc § / code ref) | Status |
| --- | --- | --- |
| `/ready` gates on ClickHouse ping (bounded deadline) + reader label-cache warmup | `crates/pulsus-server/src/ops.rs` · `architecture.md §8` | ✅ |
| Auth matrix: `/ready`+`/metrics` unauth, `/config`+`/buildinfo` gated | `crates/pulsus-server/src/app.rs::ops_auth_matrix_exempts_ready_and_metrics_but_gates_config_and_buildinfo` | ✅ |
| Ingest metrics bridged onto `/metrics` (fan-out entry) | `crates/pulsus-server/src/ops.rs::record_ingest_metrics` · `api.md §7` | ✅ |
| Per-table ingest counters/gauge (`pulsus_ingest_{rows,bytes,flushes,flush_latency_nanoseconds,retries,spool_write_failures}_total`, `pulsus_ingest_inflight`) | `crates/pulsus-server/src/ops.rs::record_table_metrics` · `architecture.md §8` | ✅ |
| Per-signal ingest series (`pulsus_ingest_queue_bytes`, `pulsus_ingest_{backpressure,spool_poison,spool_uncertain,rejected}_total`) exercised for logs/metrics/traces | `crates/pulsus-server/src/ops.rs::log_ingest_snapshot_exports_named_series` · `crates/pulsus-server/src/ops.rs::metric_ingest_snapshot_exports_named_series` · `crates/pulsus-server/src/ops.rs::trace_ingest_snapshot_exports_named_series` | ✅ |
| Registration-backfill series (`pulsus_ingest_backfill_*`) | `crates/pulsus-server/src/ops.rs::record_backfill_metrics` | ✅ |
| End-to-end scrape emits ingest series only for populated writer slots | `crates/pulsus-server/src/ops.rs::metrics_handler_exports_ingest_series_for_populated_writer_slots` · `crates/pulsus-server/src/ops.rs::metrics_handler_omits_ingest_series_when_no_writer` | ✅ |
| Label-cache (`pulsus_label_cache_*`) and query eval-gate (`pulsus_query_eval_*`) families | `crates/pulsus-server/src/ops.rs` · `architecture.md §8` | ✅ |
| `/config` secret redaction | `crates/pulsus-server/src/ops.rs::config_handler_redacts_the_password` | ✅ |
| On-disk spool **size / file-count / backlog** gauges (only `pulsus_ingest_spool_*_total` counters exist today) | `architecture.md §8` (recorded there under "Not yet exposed") — see §9 | 🚫 non-goal |

## 7. Clustering & distributed reads

| Item | Evidence (test / CI step / doc § / code ref) | Status |
| --- | --- | --- |
| Single-cluster `_dist` reader targeting via `PULSUS_DIST_SUFFIX` (a query-only cluster fronting storage with `_dist` wrapper tables) — the M7 "cross-cluster reads" deliverable | `dist_suffix` in `crates/pulsus-config/src/model.rs` · `crates/pulsus-config/src/env.rs` · `crates/pulsus-server/src/chconfig.rs::engine_config_from_uses_dist_table_names_when_clustered` · `configuration.md §4` · `architecture.md §7` | ✅ |
| Live cluster gate (`_dist` write/read-back, sharding identity) | `ci.yml: "Live cluster tests (_dist write/read-back, sharding identity)"` | ✅ |
| Multi-cluster **federation** — readers that fan out and merge across *multiple* clusters (#175 spike / #176) | `architecture.md §7` — descoped, both issues closed | 🚫 non-goal (will-not-implement) |

The implemented capability (single-cluster `_dist` reader targeting) is distinct
from the federation non-goal and must not be conflated with it: `_dist` reads
target one cluster's distributed tables; federation across multiple clusters is
the descoped piece.

## 8. Documentation coverage

| Item | Evidence (test / CI step / doc § / code ref) | Status |
| --- | --- | --- |
| Configuration reference | `configuration.md §1` · `configuration.md §5` | ✅ |
| API surface (ingest, query, operational endpoints) | `api.md §1` · `api.md §7` | ✅ |
| Architecture (ingestion, clustering, self-observability, retention) | `architecture.md §4` · `architecture.md §7` · `architecture.md §8` | ✅ |
| Storage schemas & distributed layout | `schemas.md §7` · `schemas.md §9` | ✅ |
| This checklist is anti-rot-guarded | `cargo test -p pulsus-server --test production_readiness_doc` (rides `ci.yml: "Test"`) | ✅ |

## 9. Known gaps & non-goals

No open production-readiness gap remains for the areas above; the ingest-metrics
bridge that this checklist's earlier draft flagged landed in #214 and is now
fully evidenced in §6.

The following are **bounded, explicitly-recorded non-goals** — enumerated so the
checklist reflects reality, not blockers:

- **On-disk spool size / file-count / backlog gauges (🚫 non-goal).** Only the
  `pulsus_ingest_spool_*_total` class counters (write-failure, poison, uncertain)
  are exported; there is no spool-size instrumentation. Recorded in `architecture.md §8`
  under "Not yet exposed". If operational demand appears, this is a candidate
  follow-up, not a shipping blocker.
- **Additional not-yet-exposed `/metrics` dimensions (🚫 non-goal).** Per
  `architecture.md §8`: per-*protocol* ingest error attribution (today errors are
  per signal + error-class), per-API / per-planner-stage query latencies,
  tier-router segment decisions, and tail-session counters.
- **Multi-cluster federation (#175/#176) (🚫 will-not-implement).** See §7; the
  implemented single-cluster `_dist` reader targeting satisfies the M7
  cross-cluster-reads deliverable.
