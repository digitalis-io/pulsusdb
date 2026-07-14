# Patches applied to `promql-parser 0.10.0`

This is a patched, vendored copy of [`promql-parser`
0.10.0](https://github.com/GreptimeTeam/promql-parser), wired into the
workspace via `[patch.crates-io]` (root `Cargo.toml`) so every
`promql_parser::...` import path is unchanged. See
[`docs/decisions/0003-promql-parser-vendor-patch.md`](../../docs/decisions/0003-promql-parser-vendor-patch.md)
for the decision this vendored copy implements, and
[`docs/decisions/0002-promql-parser-selection.md`](../../docs/decisions/0002-promql-parser-selection.md)
for the validation spike that found these 5 root causes across 12
M2-subset corpus inputs.

**Re-vendor rule:** on any `promql-parser` version bump or Prometheus
reference-version bump, re-run the #29 corpus + golden gate
(`cargo test -p pulsus-promql`) before accepting the bump — if upstream has
independently fixed any of the 5 root causes below, drop the corresponding
patch and (once all 5 are upstream) delete this vendored copy entirely,
reverting to a plain `promql-parser = "..."` dependency.

All 5 fixes are leaf-level (a lexer state-machine bug, a semantic action
routing to an already-existing checked path, or a `Display` impl) — none
touch a `promql.y` grammar production's tokens, alternatives, or precedence
declarations.

## 1. Reserved-keyword lexing: `anchored`/`smoothed`

- **File:** `src/parser/token.rs`
- **Bug:** `anchored`/`smoothed` were unconditionally reserved as keyword
  tokens (`T_ANCHORED`/`T_SMOOTHED`, forward-reserved in `promql.y` for a
  not-yet-implemented feature, `%expect-unused`), so they could not be used
  as ordinary metric/label names even though Prometheus v3.13 does not
  reserve them.
- **Fix:** dropped both entries from the runtime `KEYWORDS` lookup table.
  The grammar's token declarations are untouched.
- **Corpus inputs fixed:** `anchored{job="test"}`, `smoothed{job="test"}`,
  `sum by (anchored)(some_metric)`, `sum by (smoothed)(some_metric)`.
- **Upstream PR:** not yet filed (follow-up — see "Upstream PR status"
  below).

## 2. Backtick raw-string escape processing

- **File:** `src/parser/lex.rs` (`Lexer::accept_string`)
- **Bug:** backtick (`` ` ``) strings are PromQL's raw string literals
  (mirroring Go's raw strings) — no escape processing should apply inside
  them at all. The lexer's string-scanning state machine unconditionally
  entered `Escape` state on any `\`, regardless of delimiter, so
  `` `a\"b` `` raised "unknown escape sequence" even though backslash has no
  special meaning in a backtick string.
- **Fix:** `accept_string` only transitions to `Escape` state when the
  delimiter is not `` ` ``.
- **Corpus input fixed:** `` `\a\b\f\n\r\t\v\\\"\' - \xFF\377ሴ\U00010111\U0001011111☺` ``
- **Upstream PR:** not yet filed.

## 3. Duration overflow bound (bare-number durations)

- **Files:** `src/util/duration.rs` (`parse_duration`), `src/parser/promql.y`
  (`duration -> NUMBER` action)
- **Bug:** a bare-number duration (no unit suffix, e.g. the `9.5e10` in
  `foo offset 9.5e10` or `foo[9.5e10]`) was converted straight to a
  `Duration` via `Duration::from_secs_f64` with no bound check. Prometheus's
  Go implementation represents a duration as an `i64` nanosecond count and
  rejects a value that would overflow it ("duration out of range",
  confirmed against the corpus's own `err_substr` for both inputs); this
  crate's own `std::time::Duration` (backed by `u64` seconds) does not
  overflow at that magnitude, so no error was ever raised — a real,
  always-on divergence reachable through `offset` and the matrix-selector
  range `[...]`, both in the M2 proof subset.
- **Fix:** added `MAX_DURATION_SECS = i64::MAX as f64 / 1e9` (~292.47
  years, Go's `time.Duration` bound) and a bound check in
  `parse_duration`'s plain-float-seconds branch; routed the `duration ->
  NUMBER` grammar alternative's semantic action through `parse_duration`
  (the same function the `DURATION`-token alternative already used)
  instead of an independent, unchecked `Duration::from_secs_f64` call. The
  production's tokens/alternatives are unchanged — only the action code for
  the pre-existing `NUMBER` alternative was edited.
- **Corpus inputs fixed:** `foo offset 9.5e10`, `foo[9.5e10]`.
- **Upstream PR:** not yet filed.

## 4. `Matchers` `Display` — preserve parse order

- **File:** `src/label/matcher.rs` (`impl Display for Matchers`)
- **Bug:** `Display` re-serialized a selector's matcher list in
  alphabetical-by-rendered-text order rather than parse-preserved order, so
  `parse -> Display -> parse` changed matcher order and failed strict
  `Expr` `PartialEq` (an order-sensitive `Vec`) — even though the *set* of
  matchers was unchanged. Every M2 selector with 2+ matchers of mixed types
  hit this.
- **Fix:** `join_vector(simple_matchers, ",", false)` — insertion order
  instead of a sort.
- **Corpus inputs fixed:** `foo{a="b", foo!="bar", test=~"test",
  bar!~"baz"}` (and 3 variants: `{"name"}` shorthand, trailing comma, an
  all-`__name__`-matchers selector).
- **Upstream PR:** not yet filed.

## 5. `AggregateExpr` `Display` — explicit empty `by()`

- **File:** `src/parser/ast.rs` (`AggregateExpr::get_op_string`)
- **Bug:** `Display` collapsed an *explicit* empty `by()` grouping clause
  (`modifier: Some(Include([]))`) to no modifier at all, so round-trip
  parsing produced `modifier: None` instead — an AST-shape difference on
  one of the M2 subset's own constructs (aggregations with `by`/`without`).
  `without()`'s empty form already rendered explicitly; `by()` did not.
- **Fix:** made `by`'s `Include` arm unconditional, symmetric with
  `Exclude`, rather than guarding on `!ls.is_empty()`.
- **Corpus input fixed:** `sum by ()(some_metric)`.
- **Deliberate divergence from upstream Display:** upstream Prometheus's
  own `String()` also collapses explicit empty `by()` — this patch
  intentionally diverges from that one upstream Display convention to
  restore `parse -> Display -> parse` AST round-trip fidelity, which is the
  property PulsusDB's own corpus gate requires. The *parsed* semantics
  (`by()` groups every series into one, identical to no modifier) are
  unaffected either way.
- **Upstream PR:** not yet filed.

## Upstream PR status

None of the 5 fixes above have been filed as upstream PRs against
[`GreptimeTeam/promql-parser`](https://github.com/GreptimeTeam/promql-parser)
yet — filing them was scoped as a follow-up in the architect plan ("file
the upstream PRs only if trivially possible from the sandbox — otherwise
record the patch descriptions and mark PR filing as follow-up") and this
sandboxed implementation environment has no outbound network access to
open a GitHub PR. The patch descriptions above are the record; each
`Upstream PR:` line should be updated with the PR link once filed.
