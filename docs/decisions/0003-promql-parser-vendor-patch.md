# ADR 0003: patch-and-vendor `promql-parser` (supersedes 0002's port fallback)

Status: **Accepted** (2026-07-14); **Amended** (2026-07-16, issue #84 —
first grammar-production patch, see the Amendment section)
Issue: [#31](https://github.com/digitalis-io/pulsusdb/issues/31)
Supersedes: [0002](0002-promql-parser-selection.md)

## Context

[ADR 0002](0002-promql-parser-selection.md) (issue #29's validation spike)
measured `promql-parser 0.10.0` against the upstream Prometheus v3.13
parser test corpus and concluded, per its own decision rubric:

> Criterion 2 fires: **12 distinct M2-subset corpus inputs require a
> patch, which is more than the ≤5 threshold for patch-and-upstream.**
> ...
> Port fallback is triggered, per the plan's own measurable rubric
> (criterion 2). ... The trigger fired purely on **volume**: 5 distinct
> root causes happened to manifest across 12 corpus inputs, one more than
> twice the ≤5-patch threshold for patch-and-upstream.

ADR 0002 also recorded, in the same decision, that none of the rubric's
other three triggers fired:

> | # | Trigger | Measured | Fired? |
> |---|---|---|---|
> | 1 | Any M2-subset construct classified `requires_fallback` | 0 (both axes) | **No** |
> | 3 | Any required patch modifies a grammar production (vs. leaf lexing) | 0 — all 5 root causes are leaf lexer/`Display` fixes ... none touch `promql.y`'s production/precedence rules | No |
> | 4 | Any round-trip-invariant failure on an M2-subset case that is not itself `patchable` under 1–3 | 0 | No |

i.e. the crate accepts and shapes the AST exactly right wherever it
accepts an M2-subset input (47/47 AST goldens), there is no construct it
fundamentally cannot represent, and every gap is a narrow, leaf-level
lexer or `Display` bug — the trigger fired on the *count* of patchable
inputs (12 > 5), not on the *depth* of what needed patching. This issue
(#31, the first issue to actually build on the parser) is where that
trade-off — named but explicitly not resolved by ADR 0002 — had to be
made.

## Decision

**Patch-and-vendor**, not a from-scratch port. Ratified by task-manager on
issue #31 (see the issue's "Ratification + open questions resolved"
comment) under these conditions, all met by this ADR:

- (a) this ADR quotes the trigger it supersedes and the evidence
  (above) — explicit, not implied.
- (b) `docs/architecture.md` §5.1's fallback wording is amended in the
  same change (docs-first) — done.
- (c) each vendored fix carries its upstream PR link once filed, tracked
  in the vendored copy's own README/`PATCHES.md`.

### Why patch-and-vendor over a full port

A full port is ~3–5k lines of grammar (yacc/precedence rules, a lexer
state machine, an AST layer) to own **forever** — a permanent maintenance
tax with re-derivation risk (subtly wrong precedence/associativity is
exactly the kind of bug the corpus+golden gate exists to catch, and a
from-scratch reimplementation is the highest-risk way to introduce one).
Patch-and-vendor is 5 narrow, individually-upstreamable bugfixes over a
copy whose grammar is *already proven exact* wherever it accepts (47/47
M2 AST goldens, ADR 0002 §3). The decisive point: **the corpus + golden
gate (349 accept/reject cases + round-trip invariant + 47 AST cells,
CI-wired) is the regression net either way** — it catches drift against
Prometheus v3.13 regardless of which parser sits underneath — so the
choice reduces to maintenance-cost + correctness-risk economics, and a
shrinking 5-fix patch set with an upstreaming path wins decisively over a
permanent from-scratch grammar to maintain.

"Use as-is" (ADR 0002's third option) is re-rejected for the same reason
ADR 0002 rejected it: silent divergence on `anchored`/`smoothed`,
raw-string escapes, and bare-duration overflow is exactly the "no silent
wrong answer" failure mode this project exists to avoid.

## The 5 root causes, patched

All 5 are leaf-level: a lexer state-machine bug, a semantic action routed
through an already-existing (elsewhere-used) checked function, or a
`Display` impl — **zero** touch a `promql.y` grammar production's tokens,
alternatives, or precedence declarations. (This zero-grammar-productions
invariant held as stated at acceptance time; as of issue #84 it no longer
holds — see the Amendment section below.) Full diffs, rationale, and
per-fix corpus inputs are in
[`vendor/promql-parser/PATCHES.md`](../../vendor/promql-parser/PATCHES.md);
summary:

1. **Reserved-keyword lexing** (`src/parser/token.rs`) — `anchored`/
   `smoothed` were unconditionally reserved keywords (forward-declared for
   a not-yet-implemented feature) even though Prometheus v3.13 does not
   reserve them; dropped from the runtime keyword lookup table.
2. **Backtick raw-string escapes** (`src/parser/lex.rs`) — backtick
   strings are PromQL's raw-string literals (no escape processing at all,
   mirroring Go); the lexer unconditionally entered escape-processing
   state on any `\` regardless of delimiter. Fixed to skip escape
   processing when the delimiter is `` ` ``.
3. **Duration overflow bound** (`src/util/duration.rs`,
   `src/parser/promql.y`) — a bare-number duration (no unit suffix) had no
   overflow bound, unlike Prometheus's Go `time.Duration` (`i64`
   nanoseconds, ~292.47 years). Added the matching bound check and routed
   the grammar's `duration -> NUMBER` action through it.
4. **`Matchers` `Display` order** (`src/label/matcher.rs`) — re-serialized
   a selector's matchers alphabetically rather than in parse order,
   breaking the `parse -> Display -> parse` round-trip invariant. Fixed to
   preserve insertion order.
5. **`AggregateExpr` `Display` empty `by()`** (`src/parser/ast.rs`) — an
   explicit empty `by()` collapsed to no modifier at all on Display,
   losing the AST's `Some(Include([]))` shape on round-trip. Fixed to
   render `by ()` explicitly, symmetric with `without()`'s existing
   behavior. This is a deliberate, documented divergence from upstream
   Prometheus's own `String()` (which also collapses it) — traded for
   round-trip fidelity, which is the property this project's own gate
   requires; parsed semantics are unaffected either way.

## Mechanism

- `vendor/promql-parser/` — a patched copy of `promql-parser 0.10.0`
  (crate name unchanged), with exactly these 5 fixes applied to an
  otherwise-unmodified upstream source tree.
- Root `Cargo.toml`'s `[patch.crates-io]` redirects the
  `promql-parser = "=0.10.0"` dependency to `vendor/promql-parser/` — every
  `promql_parser::...` import path across the workspace (and the #29
  corpus/golden tests) is unchanged.
- `vendor/promql-parser/` is **not** a workspace member (not listed in the
  root `Cargo.toml`'s `members`) — `cargo test --workspace` never runs its
  own internal `#[cfg(test)]` modules; those are validated standalone via
  `cargo test --manifest-path vendor/promql-parser/Cargo.toml` (119/119
  passing after the 6 internal-test expectation updates the `Display`
  fixes required — see `PATCHES.md`).

## Re-vendor rule

On any `promql-parser` version bump or Prometheus reference-version bump:

1. Re-run the #29 corpus + golden gate (`cargo test -p pulsus-promql`)
   against the new upstream version before accepting the bump.
2. For each of the 5 patches: if upstream has independently fixed it,
   drop that patch from the vendored copy. Once all 5 are fixed upstream,
   delete `vendor/promql-parser/` entirely and revert to a plain
   `promql-parser = "..."` dependency (no `[patch.crates-io]`).
3. If the new version introduces *new* M2-subset divergences, re-run this
   ADR's own decision rubric (ADR 0002 §"Decision rubric") against the
   new evidence — do not assume patch-and-vendor remains the right call
   without re-measuring.

## Consequences

- `pulsus-promql`'s `parser` module re-exports the vendored crate's parse
  entry point unchanged; #31's planner and #13's HTTP layer never see the
  vendoring — it is purely a build-time redirection.
- The vendored copy carries a maintenance cost (5 patches to keep current
  against upstream `promql-parser` releases) that a plain dependency would
  not — accepted, per the economics argument above, and bounded by the
  re-vendor rule's delete-on-upstream-fix path.
- Upstream PR filing for the 5 fixes is tracked as a follow-up in
  `vendor/promql-parser/PATCHES.md` (not filed from this sandboxed
  implementation environment, which has no outbound network access to
  open a GitHub PR).

## Amendment (2026-07-16, issue #84 / M6-08b): the first grammar-production patch

The original decision's "zero grammar productions touched" invariant no
longer holds. Issue #84 (Prometheus v3.13 **duration expressions** —
`[26m+4m]`, `[step()+1]`, `offset -min_of(step(),1s)`, `range()`) is the
vendored fork's first **grammar-production patch** (`PATCHES.md` patch
G1): new productions (`duration_expr`, `offset_duration_expr`,
`positive_duration_expr`, `paren_duration_expr`,
`number_duration_literal`, `max_of_min_of`, `unary_op`), three new tokens
(`RANGE`, `MAX_OF`, `MIN_OF`) plus keyword-ization of `step`, a rewritten
bracket-interior lexer mode (upstream `lexDurationExpr`), a
`DurationExpr` AST type with `*_expr` fields on the selector nodes, and
reconciled `%expect`/`%expect-rr` conflict counts.

**Why a grammar patch is unavoidable here:** the feature *is* a grammar —
upstream implements it as a set of new productions over new tokens with
deliberate precedence-splitting (`offset_duration_expr` exists solely so
`foo offset -2^2` parses as `(foo offset -2)^2`). There is no leaf
lexer/`Display`/action seam that can express it; the alternative would be
a from-scratch parser port, which this ADR already rejected on
maintenance-cost/correctness-risk economics that only strengthen here
(the corpus + golden + round-trip gate remains the regression net either
way, and now also pins the 51 upstream `duration_expression.test` eval
cases and the 26 formerly-allowlisted parser-corpus inputs as passing).

Consequences for this ADR's standing rules:

- The patch class taxonomy is now two-tier (leaf fixes 1–5 + grammar
  patch G1), recorded in `PATCHES.md`. Leaf fix 3's `promql.y` half
  (the `duration -> NUMBER` action) is superseded by G1's
  `number_duration_literal` literal guards; its `parse_duration` bound
  remains.
- The re-vendor rule gains a caveat: G1 has no delete-on-upstream-fix
  path short of upstream `promql-parser` itself implementing v3.13
  duration expressions — on any bump, re-run the corpus gate and re-port
  G1 rather than assuming it drops.
- Upstream's `--enable-feature=promql-duration-expr` gate (OFF by
  default at the pinned conformance SHA) is deliberately **not** in the
  vendored parser (no options plumbing exists in `parse()`); PulsusDB
  enforces it at plan time via `PlanParams::experimental_functions`
  (`pulsus-promql::plan`, issue #84), with upstream's "experimental
  duration expression is not enabled" rejection text carried verbatim.

## Reproduction

```console
$ cargo test -p pulsus-promql --test upstream_parser_corpus -- --nocapture
$ cargo test -p pulsus-promql --test m2_subset_ast
$ cargo test -p pulsus-promql --test promqltest_corpus
$ cargo test --manifest-path vendor/promql-parser/Cargo.toml
```
