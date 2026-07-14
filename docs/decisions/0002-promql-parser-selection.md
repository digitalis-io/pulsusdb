# ADR 0002: `promql-parser` validation spike (M2 PromQL parser)

Status: **Superseded by [0003](0003-promql-parser-vendor-patch.md)** (2026-07-14) — the port-fallback trigger below fired mechanically on volume (12 patchable inputs > the ≤5 bar), but ADR 0003 found the gap concentrated in 5 leaf-level root causes touching zero grammar productions and ratified patch-and-vendor over a from-scratch port. This ADR's measurements (corpus pass rate, divergence buckets, round-trip results) stand as the evidence trail; its *decision* (port) does not.
Issue: [#29](https://github.com/digitalis-io/pulsusdb/issues/29)

## Context

[docs/architecture.md §5.1](../architecture.md) commits PulsusDB to full
PromQL compliance against a pinned Prometheus reference release, and names
`promql-parser` as the parser crate, with an explicit escape hatch: *"if the
crate cannot track the reference grammar ..., the fallback is porting the
upstream parser — the grammar is not negotiable."* This spike is the
risk-retirement work that decides which side of that line `promql-parser
0.10.0` (latest on crates.io, exact-pinned in `[workspace.dependencies]`)
falls on, **before** #31 builds the M2 engine on top of it. It is a spike
only: no engine code, no planner IR, no evaluation semantics — see the
architect plan's "out of scope" list on issue #29.

Two mechanically-separable questions, per the approved plan:

1. **Accept/reject fidelity** — does the crate accept and reject exactly
   what Prometheus v3.13 does, over the upstream's own parser test corpus?
2. **AST shape fidelity for the M2 proof subset** — does the crate's AST
   carry exactly the fields the #31 planner needs (matcher op/name/value,
   offset, range, aggregation modifier + grouping, binop matching), for
   every M2-subset construct
   ([docs/features.md §3](../features.md))?

The Go `expected` AST in the upstream test table is a Go struct literal and
not portable to Rust — the crate's AST is upstream-*shaped* but not
byte-identical — so these two questions are answered by two separate,
mechanical assets (accept/reject corpus + round-trip invariant vs.
human-authored AST goldens), not one.

## Methodology

### Corpus (accept/reject + round-trip)

`crates/pulsus-promql/tests/corpus/prometheus-v3.13-parse-cases.jsonl`,
extracted once by `extract-upstream-cases.py` from Prometheus
`promql/parser/parse_test.go` at tag `v3.13.0` (commit
`40af9c2cdc0eda00f3622e867a27f6359f7295f3`) — full provenance, checksum, and
the two documented exclusions (invalid-UTF-8 cases not representable as a
Rust `&str`) in `tests/corpus/PROVENANCE.md`. **349 cases**, of which 143
`should_fail`.

`crates/pulsus-promql/tests/upstream_parser_corpus.rs` (the CI-wired
harness, runs under the existing `ci` job's `cargo test --workspace`, no new
job):

1. **Corpus integrity gate (plan amendment F1):** recomputes the corpus
   file's SHA-256 and line count and asserts both against the
   extractor-written `manifest.json` before running a single case.
2. **Accept/reject replay:** `promql_parser::parser::parse(&case.input)`
   vs. `case.should_fail`, for every case. Comparison is **accept/reject
   only** — `promql-parser`'s error text never matches Prometheus's
   verbatim, so `err_substr` is carried informationally (printed in the
   per-case table) and never gates.
3. **Round-trip invariant (plan amendment F2):** for every case the crate
   *accepts*, asserts `parse(input) -> to_string() -> parse` yields an
   `Expr`-`PartialEq`-equal AST. This is the mechanical check for
   precedence/associativity/grouping/modifier-attachment bugs beyond the
   hand-picked goldens, without needing the (non-portable) Go AST.
4. **Gate rule:** every divergence (either axis) must have a matching entry
   in `tests/corpus/expected-divergences.jsonl` (53 entries), classified
   `irrelevant_to_m2` / `patchable` / `requires_fallback`, else the test
   fails ("never a silent skip"); every allowlisted entry must still
   reproduce, else the test fails ("stale allowlist / drift").

### AST golden matrix

`crates/pulsus-promql/tests/m2_subset_ast.rs` + `tests/golden/*.txt` (47
files): one committed `{:#?}` snapshot per M2 construct × modifier cell
(plan amendment F2's systematic matrix — every matcher op incl. `__name__`
(both via the bare metric-name prefix and as an explicit brace matcher,
regex and negative forms — code review finding 3); `offset` on vector and
matrix selectors; each range fn and `*_over_time` fn; each aggregation ×
`by`/`without`; binary ops × {vector-scalar, vector-vector} ×
{arithmetic, comparison} × {plain, bool, `on(...)`, `ignoring(...)`};
`histogram_quantile`; 3 precedence cases), asserted with plain `assert_eq!`
(no `insta`, matching this workspace's
established golden-file convention).

**Environment:** `rustc`/`cargo` 1.93.0, Linux 5.15 (WSL2), `promql-parser
0.10.0`, offline (pure parsing — no ClickHouse, no network at test time).

## Results (measured)

### 1. Corpus replay

| | Count | % of corpus |
|---|---|---|
| Total cases | 349 | 100% |
| Agree (accept/reject matches upstream) | 308 | **88.3%** |
| Divergent | 41 | 11.7% |

Divergences by bucket:

| Bucket | Count |
|---|---|
| `irrelevant_to_m2` | 34 |
| `patchable` | 7 |
| `requires_fallback` | **0** |

All 7 `patchable` accept/reject divergences are M2-subset-relevant:

| Input(s) | Construct | Root cause |
|---|---|---|
| `anchored{job="test"}`, `smoothed{job="test"}`, `sum by (anchored)(some_metric)`, `sum by (smoothed)(some_metric)` | bare metric selector / `by()` grouping | `promql-parser` unconditionally reserves the identifiers `anchored`/`smoothed` as keyword tokens (`token.rs`'s `KEYWORDS` map, declared `%expect-unused` in `promql.y` — forward-reserved for a not-yet-implemented feature), so they cannot be used as ordinary metric/label names, even though Prometheus v3.13 does not reserve them. **Leaf lexing fix**: drop them from the keyword map. |
| `` `\a\b\f\n\r\t\v\\\"\' - \xFF\377ሴ\U00010111\U0001011111☺` `` | string-literal lexing | The lexer raises "unknown escape sequence" for a literal backslash-quote inside a **backtick (raw) string literal**, even though backtick strings apply no escape processing at all — confirmed directly: `` `a\nb` `` round-trips as the literal two characters `\n`, but `` `a\"b` `` errors. **Leaf lexing fix**: raw-string scanning state bug, not a grammar production. |
| `foo offset 9.5e10`, `foo[9.5e10]` | offset / matrix-selector-range duration literal | `promql-parser` accepts a **bare number with no unit suffix** as a duration wherever Prometheus's base grammar requires one — confirmed directly: `foo offset 5` parses as 5s, `foo[5]` as a 5s range. Upstream's rejection here (`duration out of range`) is only reached via its *experimental* duration-expression parser, but the crate's unconditional bare-number acceptance is a real, always-on divergence, reachable through `offset` and `[...]`, both directly in the M2 subset. **Leaf lexing fix**: require a unit suffix in the duration-literal scan rule. |

The 34 `irrelevant_to_m2` divergences fall into named out-of-M2-subset
constructs: duration-expression arithmetic/builtins (`step()`, `range()`,
`max_of()`, `min_of()`, `+`/`-`/`*`/`/`/`^` inside `offset`/`[...]`) — 27
cases, all gated in upstream behind
`ExperimentalDurationExpr: true` (the corpus's own `testParser` options,
see `PROVENANCE.md`) and explicitly M6-only per
[features.md §3](../features.md); the `@` modifier (M3-only) — 2 cases;
the `info()` function (not in the M2 function list) — 2 cases; UTF-8-quoted
label *names* — 1 case; numeric-literal overflow — 1 case; a subquery
combined with duration-expression arithmetic — 1 case. Full
per-input classification with reasons: `tests/corpus/expected-divergences.jsonl`.

### 2. Round-trip invariant

Of the 179 cases the crate accepts, **12 fail** the round-trip invariant:

| Bucket | Count |
|---|---|
| `irrelevant_to_m2` | 7 |
| `patchable` | 5 |
| `requires_fallback` | **0** |

The 5 `patchable` round-trip failures are M2-subset-relevant:

- **4** cases (`foo{a="b", foo!="bar", test=~"test", bar!~"baz"}` and 3
  variants — trailing comma, `{"name"}` shorthand, all-`__name__`
  matchers): `Display` re-serializes a selector's `Matchers` list in
  **alphabetical order**, not parse-preserved order, so round-tripping
  through `Display` + `parse` changes matcher order and fails strict
  `Expr::PartialEq` (an order-sensitive `Vec`) — even though the *set* of
  matchers is unchanged. Every M2 selector with 2+ matchers of mixed types
  hits this. **Leaf `Display` fix**, not a grammar production.
- **1** case (`sum by ()(some_metric)`): `Display` collapses an *explicit
  empty* `by()` grouping clause to no modifier at all, so round-trip parse
  produces `AggregateExpr.modifier = None` instead of
  `Some(Include([]))` — an AST-shape difference on the M2-subset's own
  `by`/`without` construct. **Leaf `Display` fix**.

The 7 `irrelevant_to_m2` round-trip failures are all `@`-modifier /
subquery / `info()` / UTF-8-label-name constructs, all out of the M2
subset for the same reasons as their accept/reject counterparts.

### 3. AST golden matrix

**47 / 47 golden cases pass** (100%) — every M2 construct × modifier cell
from plan amendment F2's enumeration, plus the two explicit `__name__`
brace-matcher forms added for code review finding 3
(`selector_name_matcher_re.txt`, `selector_name_matcher_neq.txt`), produces
the exact `{:#?}` AST shape committed in `tests/golden/*.txt`. Every
M2-relevant field the #31 planner needs is present and correctly populated
in every cell: matcher `op`/`name`/`value`, both via the bare metric-name
prefix (`VectorSelector.name`) and as an explicit `__name__` `Matcher`
entry (`selector_matcher_*.txt`, `selector_name_matcher_*.txt`), `offset`
on both selector kinds (`offset_*.txt`), matrix `range`
(`matrix_selector_range.txt`, `range_fn_*.txt`, `over_time_*.txt`),
aggregation `param` + `modifier: Include/Exclude` (`agg_*.txt`),
`BinModifier{ card, matching, return_bool }` for every binop combination
(`binop_*.txt`), and correct precedence/associativity (`precedence_*.txt` —
`a * b + c` nests `a * b` as the `+`'s LHS, confirming `*` binds tighter
than `+`; `-a ^ b` confirms `^` binds tighter than unary `-`, matching
Prometheus's documented precedence table). No golden failures, no
exclusions.

One readability note, not a shape problem: `TokenType`'s derived `Debug`
renders the lexer's internal numeric token ID (e.g. `TokenType(24)`), not
an operator name — the #31 planner is expected to compare `op` against
`promql_parser::parser::token::T_*` named constants, not the golden file's
printed numeral.

## Decision rubric (plan amendment F3 — mechanical, not a judgment call)

Port fallback fires iff **any** of:

| # | Trigger | Measured | Fired? |
|---|---|---|---|
| 1 | Any M2-subset construct classified `requires_fallback` | 0 (both axes) | **No** |
| 2 | `patchable` M2-subset corpus inputs `> 5` | **12** distinct inputs (7 accept/reject + 5 round-trip; no overlap) | **YES** |
| 3 | Any required patch modifies a grammar production (vs. leaf lexing) | 0 — all 5 root causes are leaf lexer/`Display` fixes (`token.rs`'s keyword map, the raw-string scan rule, the duration-literal scan rule, `Matchers`/`AggregateExpr` `Display` impls); none touch `promql.y`'s production/precedence rules | No |
| 4 | Any round-trip-invariant failure on an M2-subset case that is not itself `patchable` under 1–3 | 0 — every M2-subset round-trip failure is `patchable` (none are unclassifiable/non-patchable) | No |

Criterion 2 fires: **12 distinct M2-subset corpus inputs require a
patch, which is more than the ≤5 threshold for patch-and-upstream.**

## Decision

**Port fallback is triggered**, per the plan's own measurable rubric
(criterion 2). This is recorded as the ADR's outcome even though — and the
distinction matters for how #31 should read this result — none of the
*other* three triggers fired: there is no M2-subset construct the crate
fundamentally cannot represent, every identified patch is a narrow,
leaf-level lexer or `Display` fix (none touch a grammar production), and
every round-trip failure is already accounted for as either out-of-scope or
patchable. The trigger fired purely on **volume**: 5 distinct root causes
happened to manifest across 12 corpus inputs, one more than twice the
≤5-patch threshold for patch-and-upstream.

Per [architecture.md §5.1](../architecture.md), the fallback is porting the
upstream Prometheus parser rather than patching/forking `promql-parser`.
This ADR does **not** implement that port (out of scope for this spike, per
the architect plan) — it hands #31 an unambiguous, evidence-backed
condition to act on, and records the following for whoever scopes the port:

- The AST **shape** `promql-parser` produces is correct and
  upstream-matching wherever the crate actually accepts an M2-subset
  input (§3 above, 47/47 golden cells) — a ported parser should target the
  same upstream-shaped `Expr` (matchers, offset, range, aggregation
  modifier, binop matching), since that shape is already proven fit for
  the #31 planner.
- The gap is concentrated, not diffuse: two lexer-keyword-table bugs
  (`anchored`/`smoothed`), one raw-string-escape lexer bug, one
  duration-literal unit-suffix gap, and two `Display`-formatting bugs
  (matcher ordering, empty `by()` collapse) account for all 12 patchable
  inputs. A from-scratch port inherits none of these five specific bugs by
  construction (they are this crate's own implementation gaps, not
  grammar ambiguities), but a port is a materially larger undertaking than
  five bugfixes — the task-manager/architect scoping #31 should weigh that
  against the mechanical trigger when deciding whether to actually port,
  patch this crate's five root causes and re-run this spike, or accept the
  gap as a documented, bounded set of known-diverging inputs. This ADR's
  job is to make that trade-off visible with real numbers, not to make it.
- 100% of the M2 proof subset that *is* representable in the current
  corpus agrees with upstream once these 5 root causes are set aside — the
  case for "use as-is with a documented patch set" is not weak, it simply
  doesn't clear the plan's own ≤5-patch bar.

## Reproduction

```console
$ cargo test -p pulsus-promql --test upstream_parser_corpus -- --nocapture
$ cargo test -p pulsus-promql --test m2_subset_ast
```

Both run under the existing `ci` job's `cargo test --workspace` — no new CI
job, no ClickHouse, no network at test time (the corpus is a committed
fixture; `extract-upstream-cases.py` is the only network-touching piece,
and it is run-once/human-invoked, not part of CI — see
`tests/corpus/PROVENANCE.md`).

To re-vendor on a Prometheus or `promql-parser` version bump
(docs/architecture.md §5.1's re-vendor rule):

```console
$ python3 crates/pulsus-promql/tests/corpus/extract-upstream-cases.py \
    --tag <new-tag> --sha <new-sha> --promql-parser-version <new-version>
```
