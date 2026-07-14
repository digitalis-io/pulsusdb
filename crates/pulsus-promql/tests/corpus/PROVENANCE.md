# Corpus provenance — Prometheus v3.13 PromQL parser test suite

Issue: [#29](https://github.com/digitalis-io/pulsusdb/issues/29) (M2
`promql-parser` validation spike). Prometheus is a public standard
PulsusDB openly targets for PromQL compatibility
(docs/architecture.md §5.1) — naming it here is correct, unlike the
never-name rule that applies to the (different) source project this
codebase is a clean-room rebuild of.

## Source

- Repository: <https://github.com/prometheus/prometheus>
- File: `promql/parser/parse_test.go`
- Tag: `v3.13.0`
- Commit SHA: `40af9c2cdc0eda00f3622e867a27f6359f7295f3`
- Fetched: 2026-07-14, via
  `https://raw.githubusercontent.com/prometheus/prometheus/<sha>/promql/parser/parse_test.go`

## Extraction

`extract-upstream-cases.py` in this directory is a **run-once, human-invoked**
script — **not** wired into CI (CI is Rust-only; the harness
`crates/pulsus-promql/tests/upstream_parser_corpus.rs` only ever reads the
files this script wrote). Re-run it on a Prometheus reference-version bump
or a `promql-parser` version bump (docs/architecture.md §5.1's "re-vendor on
bump" rule):

```console
$ python3 crates/pulsus-promql/tests/corpus/extract-upstream-cases.py \
    --tag v3.13.0 \
    --sha 40af9c2cdc0eda00f3622e867a27f6359f7295f3 \
    --promql-parser-version 0.10.0
```

It extracts the `testExpr` table that upstream's `TestParseExpressions`
iterates (the only test table in `parse_test.go` shaped as accept/reject +
error-message parser cases — the file's other `Test*` functions test series
description parsing, histogram series syntax, and internals, not the
parser's `Expr` grammar). `testExpr` is parsed upstream by a parser
constructed with `EnableExperimentalFunctions: true`,
`ExperimentalDurationExpr: true`, `EnableExtendedRangeSelectors: true` (see
`TestParseExpressions`'s `optsParser`) — i.e. the vendored corpus reflects
Prometheus's **experimental/extended** grammar, a superset of the stable
v3.13 grammar. This matters for classification: several real divergences
found by the harness are the crate correctly *not* implementing an
experimental-only construct (duration expressions, `@`-timestamp arithmetic,
`info()`) — those are bucketed `irrelevant_to_m2` in
`expected-divergences.jsonl`, not counted against the M2 subset.

### Why a text extractor, not `go test`

The `testExpr` slice is an unexported symbol inside a `_test.go` file — Go's
build/test tooling does not expose it to any program outside `go test` of
that exact package, so there is no way to programmatically dump the table
by *running* Go. The extractor is instead a hand-rolled, comment/string-
literal-aware scanner over the Go source text (not a line-oriented regex
grep), specifically because a naive grep silently corrupts the corpus on:

- embedded PromQL braces inside Go string literals (e.g.
  `` ` +{"some_metric"}` `` — a brace that is *not* Go structural syntax);
- raw (backtick) vs. interpreted (double-quoted) Go string literals, which
  decode differently (only the latter applies escape sequences);
- `input:` fields built by concatenating multiple literals with `+`,
  including two uses of `strings.Repeat(<literal>, N)`.

## Counts and checksum

- **351** top-level `{ ... }` items in the `testExpr` slice literal
  (`SEGMENTED_CASE_COUNT` in the extractor — asserted after every
  extraction so a botched scan is loud, not silent).
- **2 cases excluded** from the corpus: both construct an invalid UTF-8 byte
  via a Go `\xff` escape (`some_metric{a="\xff"}` and
  `` label_replace(a, `b`, `c\xff`, `d`, `.*`) ``), specifically to test that
  Prometheus's lexer *rejects* invalid UTF-8. `promql_parser::parser::parse`
  takes `&str`, and Rust's type system already guarantees `&str` is valid
  UTF-8 — invalid-UTF-8 input cannot even be constructed to pass to it, so
  this Prometheus lexer-level check has no meaningful Rust equivalent to
  test. Excluding them (rather than mis-decoding `\xff` as the *different*,
  valid code point U+00FF) avoids fabricating a false divergence — see the
  extractor's `InvalidUtf8Literal` docstring for the full reasoning.
- **349** cases in the committed corpus (`EXPECTED_CASE_COUNT`), of which
  **143** have `should_fail: true`.
- SHA-256 of `prometheus-v3.13-parse-cases.jsonl`'s raw bytes:
  `911ff24d4e270e3e193b151849915bd68d092b90bacd4b8e92faceaf5353bac0`

These same values are in `manifest.json` (machine-readable, read by
`upstream_parser_corpus.rs` at test time — plan amendment F1: the harness
recomputes the corpus file's SHA-256 and line count and asserts both against
`manifest.json` *before* running any case, so a truncated/edited/reordered
committed corpus fails loudly instead of silently producing wrong pass-rate
numbers).

## Two known-good hand-verified decoding edge cases

- `fmt.Sprintf(` foo @ %f`, float64(math.MaxInt64)+1)` /
  `float64(math.MinInt64)-1` (2 cases, both `@`-modifier edge cases): the
  extractor cannot evaluate arbitrary Go expressions, so these two exact
  source strings are matched literally and mapped to their pre-computed
  Go-`%f`-formatted values (`KNOWN_SPRINTF_VALUES` in the extractor). Any
  other `fmt.Sprintf` usage introduced by a future re-vendor is a loud,
  unhandled-pattern error, not a silent mis-decode.
- One case builds its `input` via `strings.Repeat("-{}-1", 10000)` +
  `strings.Repeat("[1m:]", 1000)` (a ~55 KB pathological-nesting/subquery
  stress test the upstream comment says exists "to test that we are not
  re-rendering the expression string for each error, which would
  timeout"). The extractor evaluates `strings.Repeat` directly (it's a pure,
  deterministic string op) rather than excluding the case. Manually verified
  safe to include: `promql_parser::parser::parse` rejects it in ~12 ms
  (`vector selector must contain at least one non-empty matcher`), no stack
  overflow or hang.
