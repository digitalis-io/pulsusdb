# Registry provenance — Prometheus v3.13 PromQL function registry

Issue: [#64](https://github.com/digitalis-io/pulsusdb/issues/64) (M6-01,
function-coverage manifest). Prometheus is a public standard PulsusDB
openly targets (docs/architecture.md §5.1) — naming it here is correct.

## Source

- Repository: <https://github.com/prometheus/prometheus>
- Files:
  - `promql/parser/functions.go` — the function registry (**89**
    functions at this SHA, **17** of them `Experimental: true`);
  - `promql/parser/lex.go` — the aggregation-operator keywords (**14**:
    the keyword map's "Aggregators." block) and the experimental pair
    (`IsExperimentalAggregator`: `limitk`, `limit_ratio`).
- Tag: `v3.13.0`
- Commit SHA: `40af9c2cdc0eda00f3622e867a27f6359f7295f3`
- Fetched: 2026-07-16 by `extract-registry.py` (run-once, human-invoked,
  **not** wired into CI — CI is Rust-only; `tests/function_coverage.rs`
  only ever reads the files the script wrote, re-verifying
  `registry-v3.13.json`'s SHA-256 against `registry-manifest.json`
  first).

The 89 is the **function-only** count — the same 89 features.md §7 cites
(#64 Q1 adjudication: the pinned functions.go registers exactly 89
functions with no aggregation operators folded in; the 14 aggregation
operators are keyword tokens, tracked as a separate manifest dimension).
The vendored `promql-parser` 0.10.0 `FUNCTIONS` table mirrors the same
89 names and 17 experimental flags (verified during #64 planning).

## Files

- `registry-v3.13.json` — the vendored authoritative registry:
  `{name, arg_types, variadic, return_type, experimental}` per function,
  `{name, experimental}` per aggregation operator, sorted by name.
- `registry-manifest.json` — SHA-256 of `registry-v3.13.json` plus the
  three counts (89 / 17 / 14), asserted by `function_coverage.rs` before
  any coverage check runs.
- `function-coverage.json` — the coverage manifest itself: every
  function/operator/tracked language feature with a
  `status ∈ {implemented, scheduled, deferred}`; `scheduled` carries the
  owning M6 issue, `deferred` a rationale, `implemented` a `witness`
  pointer at a concrete proof-corpus case (plan v2 Δ3). This file is the
  machine-checked authority features.md §7 cross-references; it amends
  the #21 decomposition's per-issue lists per the #64 Q3 adjudication
  (`first_over_time` + the four `ts_of_*_over_time` → M6-04;
  `max_of`/`min_of` → M6-02; `start`/`end`/`step`/`range` → M6-08;
  `histogram_quantiles` → deferred to #22).

## Re-vendor rule

On a Prometheus reference-version bump:

```console
$ python3 crates/pulsus-promql/tests/promqltest/coverage/extract-registry.py \
    --tag <new-tag> --sha <new-sha>
```

then reconcile `function-coverage.json` (new/renamed functions fail the
identity gate loudly), update this file, and re-run
`cargo test -p pulsus-promql --test function_coverage`.
