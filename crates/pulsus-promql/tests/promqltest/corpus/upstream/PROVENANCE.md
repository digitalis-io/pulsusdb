# Corpus provenance — Prometheus v3.13 promqltest scenario files

Issue: [#64](https://github.com/digitalis-io/pulsusdb/issues/64) (M6-01,
PromQL `.test` corpus driver). Prometheus is a public standard PulsusDB
openly targets for PromQL compatibility (docs/architecture.md §5.1) —
naming it here is correct, matching the precedent of
`crates/pulsus-promql/tests/corpus/PROVENANCE.md` (#29, the parser
corpus, which this eval corpus is fully namespaced away from and never
touches).

## Source

- Repository: <https://github.com/prometheus/prometheus>
- Directory: `promql/promqltest/testdata/`
- Tag: `v3.13.0`
- Commit SHA: `40af9c2cdc0eda00f3622e867a27f6359f7295f3`
- Fetched: 2026-07-16, via
  `https://raw.githubusercontent.com/prometheus/prometheus/<sha>/promql/promqltest/testdata/<file>`

## Contents

The upstream directory holds **21** `.test` scenario files. **20 are
vendored here byte-verbatim**; `native_histograms.test` is the sole
exclusion — native histograms are M7 (issue #22), which vendors and
activates that file (recorded machine-readably in
`upstream-manifest.json`'s `excluded` array, never a silent omission).

`upstream-manifest.json` pins every vendored file's SHA-256 and line
count plus the tag/SHA above. `tests/promqltest_corpus.rs` re-verifies
all of it (both directions: a file on disk missing from the manifest is
as fatal as a manifest entry missing on disk) before replaying a single
case — the #29 F1 integrity pattern.

## Re-vendor rule

On a Prometheus reference-version bump: re-fetch all files at the new
SHA, regenerate `upstream-manifest.json` (sha256 + line counts), update
this file, then re-run
`cargo test -p pulsus-promql --test promqltest_corpus` and re-classify
whatever the skip-manifest drift gate and the divergence gates surface.
The upstream driver grammar (directive regexes in
`promql/promqltest/test.go`) must be re-checked against
`tests/promqltest/grammar.rs`'s executed subset on every bump.

## Driver semantics pinned against upstream at this SHA

- Directive regexes: `patLoad`, `patEvalInstant`, `patEvalRange`
  (`promql/promqltest/test.go:52-54`).
- Base epoch `T0 = 0 ms` (`testStartTime = time.Unix(0,0).UTC()`).
- Series-value grammar: `promql/parser/generated_parser.y`
  (`series_item` productions — `vxN` is N+1 values, `_xN` is N gaps) and
  the series-mode hex prohibition (`promql/parser/lex.go::scanNumber`:
  "Disallow hexadecimal in series descriptions").
- Value comparison: `defaultEpsilon = 1e-6` relative error
  (`promql/promqltest/test.go`, `util/almost/almost.go::Equal`) with
  `NaN == NaN` for testing — the tolerance the files' expected values
  are written to.
- Lookback: 5m (upstream `LookbackDelta` default), matching
  `pulsus_promql::DEFAULT_LOOKBACK_MS`.

## Known fixture-comment slip: `at_modifier.test:159`'s subquery grid

The inline comment above `at_modifier.test:159` ("inner subquery: at
905=90+89, at 915=91+90", etc.) mis-states the subquery inner-step grid.
The compiled engine at the pinned SHA (`promql/engine.go::runSubquery`)
computes an **epoch-anchored ascending** grid — the multiples of `step`
in the left-open window `(mint, maxt]`, `subqStart = step *
floor(mint/step)` corrected up one step on the boundary — which for that
case emits `{900s, 910s}`, not the comment's `{905s, 915s}`; the
asserted aggregate (360) coincidentally matches both (issue #83 plan v2
Δ1, proven by instrumenting the engine). Do **not** re-derive an
end-anchored decrement grid from that comment:
`proof/m6_08a_at_subquery.test`'s
`sum_over_time(vector(time())[10s:3s] @ 25) = 63` golden exposes the
exact inner timestamps and fails any end-anchored port (which yields
66). The grid helper itself (`src/eval/mod.rs::subquery_grid_start`)
carries the same note.

## Executed vs skipped at vendor time

7 files execute fully under the M6-01 grammar subset (`at_modifier`,
`duration_expression`, `fill-modifier`, `selectors`, `staleness`,
`trig_functions`, `type_and_unit`); 13 use deferred directives (`expect`
assertion lines, `{{…}}` native-histogram sample literals,
`load_with_nhcb`, `@st` start-timestamp lines) and are listed — loudly,
wholesale, with activation issues — in `../skip-manifest.json`. Residual
divergences of the executed files that the coverage-manifest oracle
cannot attribute to a scheduled/deferred construct are classified
per-case in `../eval-divergences.jsonl` (105 entries at vendor time; see
each entry's `construct`/`reason`).
