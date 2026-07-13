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
