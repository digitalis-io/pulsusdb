# Corpus provenance — Prometheus v3.13 promqltest scenario files

Issue: [#64](https://github.com/digitalis-io/pulsusdb/issues/64) (M6-01,
PromQL `.test` corpus driver); sourcing changed by
[#156](https://github.com/digitalis-io/pulsusdb/issues/156) (fetched at
test time, no longer vendored). Prometheus is a public standard PulsusDB
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

## Sourcing: fetched at test time, checksum-pinned (issue #156)

The **21** upstream `.test` scenario files are **not vendored in this
repo**. The shared test driver (`../../fetch.rs`) fetches each file on
cache miss from

```
https://raw.githubusercontent.com/prometheus/prometheus/<commit-sha>/promql/promqltest/testdata/<name>
```

addressed by the **commit SHA** above (never the tag name — a re-tag
cannot move a commit), verifies its SHA-256 and line count against the
committed `upstream-manifest.json` (the trust anchor; byte-frozen), and
installs it atomically into the local cache at

```
$PULSUSDB_PROMQLTEST_CACHE_DIR            (override, if set)
$XDG_CACHE_HOME/pulsusdb/promqltest/<commit-sha>/   (else, if set)
$HOME/.cache/pulsusdb/promqltest/<commit-sha>/      (else)
```

A warm cache is verified in place — no network process is ever spawned.
A corrupted cache entry self-heals by refetching once; a persistent
mismatch against the manifest fails loudly with the URL and both hashes
(the truncation/tamper/re-tag guard). Pre-warm command (also the
fast integrity gate):

```sh
cargo test -p pulsus-promql --test promqltest_corpus upstream_corpus_matches_its_integrity_manifest
```

CI restores the cache via `actions/cache` keyed on the manifest hash, so
warm CI runs are fully hermetic. `cargo build` / packaging never runs
tests and therefore never fetches.

## License and attribution

The upstream corpus files are © The Prometheus Authors, licensed under
the **Apache License, Version 2.0**
(<https://github.com/prometheus/prometheus/blob/main/LICENSE>). This
attribution covers the files as fetched into the local cache at test
time, and equally the byte-verbatim copies that remain in this
repository's git history from the vendoring era (issues #64–#124, before
#156 removed them from the tree). The files are used unmodified as test
fixtures.

## Re-pin procedure

On a Prometheus reference-version bump: update the tag and commit SHA
here and in `upstream-manifest.json`, regenerate the per-file entries,
then re-run the replay and re-classify whatever the skip-manifest drift
gate and the divergence gates surface. Per-file regeneration one-liner
(run in a scratch dir; `SHA` is the new commit):

```sh
for f in aggregators at_modifier collision duration_expression extended_vectors \
         fill-modifier functions histograms info limit literals \
         name_label_dropping native_histograms operators range_queries \
         selectors staleness start_timestamps subquery trig_functions \
         type_and_unit; do
  curl -sSfLo "$f.test" "https://raw.githubusercontent.com/prometheus/prometheus/$SHA/promql/promqltest/testdata/$f.test"
  printf '%s sha256=%s lines=%s\n' "$f.test" "$(sha256sum "$f.test" | cut -d' ' -f1)" "$(wc -l < "$f.test")"
done
```

(Confirm the upstream directory's file *set* against the tag's tree
listing first — a bump can add or remove files; the manifest, the count
assertion, and the pinned name-set list (`UPSTREAM_FILE_NAMES` in
`../../mod.rs`, the duplicate/rename/omission guard) must follow.)
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

## Executed vs skipped at pin time (M6-01)

7 files execute fully under the M6-01 grammar subset (`at_modifier`,
`duration_expression`, `fill-modifier`, `selectors`, `staleness`,
`trig_functions`, `type_and_unit`); 13 use deferred directives (`expect`
assertion lines, `{{…}}` native-histogram sample literals,
`load_with_nhcb`, and — until issue #155 activated them — `@st`
start-timestamp lines) and are listed — loudly,
wholesale, with activation issues — in `../skip-manifest.json`. Residual
divergences of the executed files that the coverage-manifest oracle
cannot attribute to a scheduled/deferred construct are classified
per-case in `../eval-divergences.jsonl` (105 entries at M6-01 time; see
each entry's `construct`/`reason`).

## M7-A6 update (issue #124)

The `{{…}}` native-histogram sample-literal grammar and the block-form
`expect warn|no_warn|info|no_info` annotation directives landed
(`../histogram_literal.rs`, `../grammar.rs`, `../runner.rs`), and
`native_histograms.test` joined the corpus (previously the sole
exclusion, tracked in `upstream-manifest.json`'s now-empty `excluded`
array). This incidentally cleared every deferred directive from four
already-skip-manifested files (`extended_vectors.test`, `info.test`,
`limit.test`, `subquery.test`); `subquery.test` now replays 100% green
and executes unlisted, the other three fail on gaps unrelated to native
histograms and are deferred via the non-directive `manual_skip` lever
(see `../skip-manifest.json`'s top-level comment and issue #130).
`load_with_nhcb` (a distinct classic-bucket-to-NHCB `load` conversion,
never a `{{…}}` literal) and the block `expect ordered`/`expect range
vector` forms remain deferred directives —
`histograms.test`/`operators.test`/
`aggregators.test`/`functions.test`/`range_queries.test` stay
skip-manifested on those grounds. `@st` start-timestamp lines became
executable in issue #155 (loader grammar + the rate/irate/increase/
resets ST semantics), de-listing `start_timestamps.test` — the file now
replays fully green.
