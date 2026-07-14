# e2e fixtures

Fixture files consumed by `pulsus-e2e` scenarios (`e2e/src/scenarios.rs`).
Layout: `test/fixtures/<area>/<name>.*` — one file per fixture. `<area>`
groups fixtures by the subsystem they exercise (`ops` for the M0 skeleton;
later milestones add `logs`, `metrics`, `traces`, `profiles`).

## Adding a scenario

1. Add one fixture file under `test/fixtures/<area>/<name>.*`, in whatever
   shape the assertion needs (JSON here; later fixtures may differ).
2. Add one assertion fn in `e2e/src/scenarios.rs` that loads it and asserts
   against the running stack over HTTP (`Ctx::http` / `Ctx::url`).
3. Add one entry to the `SCENARIOS` registry, naming the variants it runs
   under (`Variant::Single`, `Variant::Cluster`, or both).

No other wiring is required — `pulsus-e2e --variant <v>` runs every
scenario registered for `<v>` automatically.

## Current fixtures

- `ops/buildinfo.fields.json` — the field names `GET /buildinfo` must
  return, all non-empty (docs/api.md §7).
- `logs/roundtrip.json` — the M1 collector-to-query round-trip fixture
  (issue #15, `e2e/src/scenarios.rs`'s `logs_roundtrip` scenario): an array
  of `streams`, each `{ service, scope_name?, scope_version?,
  resource_attrs{}, scope_attrs{}, lines: [{ ts_offset_ns, body }] }`.
  Covers 4 services, otel scope identity labels, a `resource_attrs`/
  `scope_attrs` key collision (`billing`'s `env`), and a non-ASCII/
  JSON-ish body (`checkout`'s `café ☕` line). Timestamps are
  `base_ns + ts_offset_ns`, with `base_ns` computed at scenario run time —
  never a fixed past date, so the fixture stays inside
  `PULSUS_RETENTION_DAYS` and the query window brackets it regardless of
  when the suite runs.
- `metrics/differential.json` — the M2 differential-accuracy corpus/query-
  matrix fixture (issue #33, `e2e/src/corpus.rs` + `e2e/src/metrics.rs`'s
  `metrics_differential` scenario): a `seed`/`step_ms`/`sample_count`/
  `histogram_bounds` shared by both tiers, per-family series counts for
  the `ci` (~1k series, gates every PR) and `full` (~10k series, the
  docs/features.md §7 acceptance criterion) tiers, and the pinned
  `query_matrix` (`{R}` substituted with the run's `run_id` at execution
  time) every entry runs in `instant` and/or `range` mode against both
  PulsusDB and a reference Prometheus.
