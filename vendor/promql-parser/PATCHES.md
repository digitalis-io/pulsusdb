# Patches applied to `promql-parser 0.10.0`

This is a patched, vendored copy of [`promql-parser`
0.10.0](https://github.com/GreptimeTeam/promql-parser), wired into the
workspace via `[patch.crates-io]` (root `Cargo.toml`) so every
`promql_parser::...` import path is unchanged. See
[`docs/decisions/0003-promql-parser-vendor-patch.md`](../../docs/decisions/0003-promql-parser-vendor-patch.md)
for the decision this vendored copy implements, and
[`docs/decisions/0002-promql-parser-selection.md`](../../docs/decisions/0002-promql-parser-selection.md)
for the validation spike that found the original 5 root causes across 12
M2-subset corpus inputs.

**Re-vendor rule:** on any `promql-parser` version bump or Prometheus
reference-version bump, re-run the #29 corpus + golden gate
(`cargo test -p pulsus-promql`) before accepting the bump — if upstream has
independently fixed any of the leaf root causes below, drop the
corresponding patch. The grammar-production patch class (G1 below) has no
delete-on-upstream-fix path short of upstream `promql-parser` itself
implementing Prometheus v3.13 duration expressions; re-measure against the
corpus gate on every bump.

The patches fall into two classes:

- **Leaf fixes 1–5:** a lexer state-machine bug, a semantic action routing
  to an already-existing checked path, or a `Display` impl — none touch a
  `promql.y` grammar production's tokens, alternatives, or precedence
  declarations.
- **Grammar-production patch G1** (issue #84, M6-08b): the first — and so
  far only — patch that adds grammar productions, tokens, and lexer modes.
  ADR 0003's original "zero grammar productions touched" invariant no
  longer holds; see the ADR's amendment for the rationale (Prometheus
  v3.13 duration expressions are structurally unimplementable as a leaf
  fix: the upstream feature is itself a grammar of new productions).

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

## 6. `info()` second-argument empty-matcher bypass (issue #82, M6-05b)

- **Files:** `src/parser/ast.rs` (`check_ast_for_vector_selector`, new
  `check_no_empty_selectors`), `src/parser/parse.rs` (`parse`)
- **Bug:** Prometheus v3.13.0 exempts exactly one context from the
  "vector selector must contain at least one non-empty matcher" rule —
  `info()`'s second argument (`VectorSelector.BypassEmptyMatcherCheck`,
  `parse.go` checkAST), a label-selector-only position where an
  all-empty-matching selector like `{data=~".*"}` or
  `{__name__!="target_info"}` is legal (several `info.test` corpus cases
  use exactly these). This crate ran the rejection at the selector's own
  grammar reduction, before any enclosing call is known, so those
  queries failed to parse.
- **Fix:** the rejection for a selector *carrying* matchers moved to a
  post-parse iterative walk (`check_no_empty_selectors`, run by
  `parse()`) that skips `info()`'s second argument; a rejected tree is
  then dismantled iteratively (`ast::dismantle`) so the deferred path
  adds no stack use beyond the generated parser's own recursion. The
  literal `{}` (zero matchers) keeps failing at reduction time: the
  pinned deep fuzz-regression input (`(-{}-1…` ×10k) relies on that
  first-reduction short-circuit — the generated LR parser's OWN
  recursion (a pre-existing, patch-independent property) overflows a
  2 MiB stack at ~9k `-{…}-1` units for *any* input it fully builds,
  valid `-m-1` chains included (measured; instrumentation showed the
  overflow fires inside the grammar before any patch code runs), so
  deferring `{}` would turn that pinned `Err` into an abort. Within the
  grammar's surviving depth (~8k units) the deferred rejection is
  proven stack-safe by regression tests here (`test_corner_fail_cases`)
  and in `pulsus-promql`
  (`deep_all_empty_matcher_input_is_rejected_and_destroyed_without_overflow`,
  which rides the workspace CI gauntlet — this vendored crate is its
  own cargo workspace and is not exercised by `cargo test --workspace`).
- **Deliberate divergence from upstream:** `info(m, {})` stays rejected
  ("vector selector must contain at least one non-empty matcher") where
  upstream's bypass admits it — the zero-matcher form appears in no
  corpus case and semantically adds nothing over `info(m)`.
- **Corpus inputs fixed:** `info(metric, {data=~".*"})`,
  `info(metric, {non_existent=~".*"})`,
  `info(metric, {__name__!="target_info"})`,
  `info(metric, {__name__!~".+_info", data=~".*"})` (the vendored
  `info.test` shapes, exercised by `proof/m6_05b_info.test`).
- **Upstream PR:** not yet filed (targets v3.13 semantics past the
  crate's v2.45 baseline, like G1 — recorded here as the divergence
  ledger instead).

## G1. Grammar-production patch: duration expressions (issue #84, M6-08b)

- **Files:** `src/parser/promql.y`, `src/parser/token.rs`,
  `src/parser/lex.rs`, `src/parser/ast.rs`, `src/parser/mod.rs`,
  `src/util/duration.rs`
- **What:** Prometheus v3.13.0's duration expressions — arithmetic
  (`+ - * / % ^`), unary `+`/`-`, parentheses, `step()`, `range()`, and
  `min_of`/`max_of` in the range-selector, subquery range/step, and
  `offset` positions (`http_requests[26m+4m]`, `m[step()+1]`,
  `m offset -min_of(step(),1s)`). Ported from `generated_parser.y`,
  `lex.go`, and `parse.go` at PulsusDB's pinned v3.13.0 conformance SHA
  (`40af9c2`), where the feature is gated behind
  `--enable-feature=promql-duration-expr` (OFF by default).
- **Grammar (`promql.y`):** new productions
  `number_duration_literal` / `duration_expr` / `paren_duration_expr` /
  `positive_duration_expr` / `offset_duration_expr` / `max_of_min_of` /
  `unary_op`; `matrix_selector`/`subquery_expr` rewired to
  `positive_duration_expr` and `offset_expr` to `offset_duration_expr`
  (the old `duration`/`maybe_duration` productions are gone);
  `STEP`/`RANGE`/`MAX_OF`/`MIN_OF` added to
  `metric_identifier`/`maybe_label` (still usable as metric/label names)
  and `function_call` gains `max_of_min_of function_call_body` (the
  already-implemented `max_of`/`min_of` scalar calls keep parsing after
  keyword-ization). `%expect 11`/`%expect-rr 207`: the reduce/reduce
  conflicts are the deliberate `offset_duration_expr`/`duration_expr`
  overlap, resolved by grmtools' earlier-production rule to terminate a
  bare offset before a trailing operator (`foo offset 100 + 2` ==
  `(foo offset 100) + 2`, `foo offset -2^2` == `(foo offset -2)^2` —
  upstream's split-non-terminal precedence semantics, pinned by the
  corpus and the crate's own `test_duration_expr`).
- **Tokens/lexer:** `range`/`max_of`/`min_of` token declarations
  (`step` already existed, previously `%expect-unused`); all four join
  the runtime `KEYWORDS` table (upstream `lex.go`'s `key` map at the
  pin). The bracket-interior lexer mode (`inside_brackets`) is rewritten
  to upstream's `lexDurationExpr`: operators, parentheses, comma, the
  four duration keywords (case-insensitive), and upstream's
  got-duration-before-colon subquery rule, with upstream's error texts.
- **AST (`ast.rs`):** a self-contained `DurationExpr` tree (NOT an
  `Expr` variant — it only appears in duration positions), carried as
  `range_expr`/`step_expr`/`offset_expr: Option<DurationExpr>` next to
  the existing concrete fields (upstream's dual `Range`+`RangeExpr`
  model). A plain or sign-folded literal still resolves to the concrete
  field at parse time (`*_expr: None`); `Display` renders the expression
  form when present, and a `Wrapped` variant preserves parentheses so
  `parse -> Display -> parse` round-trips exactly. Unary `+` is kept
  uniformly as a `Pos` node (and displayed) — upstream folds it away on
  some paths and keeps an `ADD`-with-nil-LHS node on others; uniformity
  is what keeps the round-trip invariant exact. Resolution to concrete
  durations (upstream `promql/durations.go`) deliberately does NOT live
  in this crate — the parser has no step/range context; PulsusDB's
  planner (`pulsus-promql::plan`) folds the tree at plan time.
- **`util/duration.rs`:** `parse_duration` no longer rejects zero
  durations — upstream `model.ParseDuration` parity; positivity is a
  grammar-position rule (`positive_duration_expr`), not a lexical one.
  Without this, `foo[5s/0d]` failed with the positivity message instead
  of upstream's "division by zero", and upstream-valid forms like
  `foo offset 0s`/`[30m+0s]` were rejected. This also supersedes the
  `promql.y` half of leaf fix 3 above: the `duration -> NUMBER` action it
  routed through `parse_duration` no longer exists; the bare-number
  out-of-range bound now lives in the duration-expression literal guards
  ("duration out of range", upstream's `durationLiteralOutOfRange`
  placement). Fix 3's `parse_duration` bound itself still stands for
  unit-suffixed lexemes.
- **Experimental gate:** deliberately NOT in this crate. The parser
  parses duration expressions unconditionally (it has no options
  plumbing); PulsusDB gates at plan time on
  `PlanParams::experimental_functions` via `*_expr.is_some()` presence
  checks, with upstream parse.go's "experimental duration expression is
  not enabled" carried verbatim in the rejection. The corpus-visible
  behaviour class is identical (a gated query is rejected with the
  upstream message before any data is touched).
- **Corpus inputs fixed:** the 26 duration-expression rows formerly in
  `crates/pulsus-promql/tests/corpus/expected-divergences.jsonl`
  (`foo[11s+10s-5*2^2]`, `foo[step()]`, `foo offset -min_of(5s,step()+8s)`,
  `foo[4s+4s:1s*2] offset (5s-8)`, ...) and the 51
  `duration_expression.test` eval rows formerly in
  `tests/promqltest/corpus/eval-divergences.jsonl`.
- **Upstream PR:** not applicable as a `GreptimeTeam/promql-parser` PR in
  its current shape (it targets Prometheus v3.13 grammar, several
  versions past that crate's v2.45 baseline); recorded here as the
  divergence ledger instead.

## Upstream PR status

None of the 5 leaf fixes above have been filed as upstream PRs against
[`GreptimeTeam/promql-parser`](https://github.com/GreptimeTeam/promql-parser)
yet — filing them was scoped as a follow-up in the architect plan ("file
the upstream PRs only if trivially possible from the sandbox — otherwise
record the patch descriptions and mark PR filing as follow-up") and this
sandboxed implementation environment has no outbound network access to
open a GitHub PR. The patch descriptions above are the record; each
`Upstream PR:` line should be updated with the PR link once filed.
