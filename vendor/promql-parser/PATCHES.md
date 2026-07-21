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

- **Files:** `src/parser/ast.rs` (`check_ast`, `check_ast_for_vector_selector`,
  `check_ast_for_unary`, `check_ast_for_binary_expr`,
  `check_ast_for_subquery`, `check_ast_for_aggregate_expr`,
  `check_ast_for_call`, new `reject_empty_operand`/
  `is_bare_empty_selector`/`check_ast_for_matrix_selector`, existing
  `check_no_empty_selectors`), `src/parser/parse.rs` (`parse`,
  `test_issue_82_*`)
- **Bug:** Prometheus v3.13.0 exempts exactly one context from the
  "vector selector must contain at least one non-empty matcher" rule —
  `info()`'s second argument (`VectorSelector.BypassEmptyMatcherCheck`,
  `parse.go:851-859`), a label-selector-only position where an
  all-empty-matching selector like `{data=~".*"}`, or the literal `{}`
  itself, is legal. This crate ran the rejection at the selector's own
  grammar reduction, before any enclosing call is known, so `info(m,
  {})` and several `info.test` corpus shapes failed to parse.
- **First fix (superseded within this same patch):** the rejection
  moved wholesale to a post-parse iterative walk
  (`check_no_empty_selectors`) that skips `info()`'s second argument,
  keeping the literal `{}` rejected at the selector's own reduction as a
  load-bearing stack-overflow guard. A retroactive re-review (issue #82)
  found this left `info(m, {})` itself rejected (the one case upstream's
  bypass exists for) and — separately — left the per-step `info()` info-
  family fetch unbounded before materialization (see the reader-side fix
  tracked on issue #82, not part of this vendored patch).
- **Current fix:** the eager reject is RELOCATED off the selector leaf
  onto every depth-adding reduction one level up
  (`reject_empty_operand`, called from `check_ast_for_unary`/
  `check_ast_for_binary_expr`/`check_ast_for_subquery`/
  `check_ast_for_aggregate_expr`/the `Expr::Paren` arm of `check_ast`;
  plus a dedicated `check_ast_for_matrix_selector`, since a
  `MatrixSelector` reduces through `check_ast` directly — `promql.y:194`
  — with no operand-level check otherwise seeing it, letting a
  range-wrapped empty selector like `rate({}[5m])` hide past every
  `VectorSelector`-only guard). `check_ast_for_call` applies the same
  guard to every call argument EXCEPT `info()`'s SECOND argument (index
  1, 0-indexed — matching the existing deferred bypass's own
  `bypass_second && i == 1`); `info()`'s own first argument is NOT
  exempt (`info({})` still rejects). `check_no_empty_selectors` is
  unchanged and stays the backstop for the one shape with no wrapping
  reduction to be eager on: a bare top-level `{}` (which cannot
  overflow — nothing nests it).
  - **Measured, not assumed (debug, 2 MiB thread — the round-2/round-3
    plan reviews required reproducible evidence before this landed):**
    the pre-existing overflow bound is in the generated LR grammar
    itself, not the empty-matcher check — fully VALID input (`(-m-1)×N`)
    overflows at N=9000 regardless of this patch. The eager relocation
    is *more* overflow-safe than both the original leaf check and the
    deferred walk: `(-{}-1)×N` never overflows up to N=11000 (the
    shallow leftmost `{}` short-circuits via `check_ast_for_unary`
    before the outer nesting is ever built), and
    `abs(×10000 rate({}[5m]) )×10000` returns a clean `Err` with no
    overflow (the `MatrixSelector` arm fires at the innermost reduction,
    long before the 10000 wrapping `abs()` calls could matter). The
    pinned fuzz case (`parse.rs` `test_corner_fail_cases`,
    `(-{}-1…`×10k + `[1m:]`×1000) is **unchanged** — same input, same
    `Err` message, still stack-safe. Full vendored suite: 121→123/123
    green in debug (0.24s total — the eager short-circuit is strictly
    faster than either prior version, which had to build the deep tree
    before rejecting).
  - **Paren-wrapped empty selector in `info()`'s arg-1 stays rejected —
    this is parity, not a regression.** `info(m, ({}))` REJECTS under
    this patch (the eager `Expr::Paren` guard fires on the inner `{}`
    before the enclosing `info()` call is known) — and this matches BOTH
    the shipped deferred walk (`check_no_empty_selectors`'s `Expr::Paren`
    arm resets `bypass` to `false` unconditionally, so a paren-wrapped
    inner selector was never exempt even before this patch) AND upstream
    Prometheus v3.13.0, whose bypass type-asserts arg 1 directly to
    `*VectorSelector` (`parse.go:851-859`); a `*ParenExpr` fails that
    assertion and errors `expected label selectors only` — parens are
    not transparent for `info()`'s selector argument upstream either.
    Propagating the exemption through parens would make this crate
    ACCEPT `info(m, ({}))`, diverging FROM upstream rather than closing
    a gap — not implemented. No divergence-ledger entry for this shape.
- **Divergence from upstream (unrelated, pre-existing — since fixed by
  entry #7 below):** at the time of this patch the crate had no analogue
  of upstream's "arg-1 must be a **direct** label-selector" type
  assertion, so it accepted a paren-wrapped **non-empty**
  `info(m, ({job="x"}))` where upstream rejects it
  (`expected label selectors only`) — orthogonal to the empty-matcher
  fix above; now closed by entry #7 (issue #132).
- **Corpus inputs fixed:** `info(metric, {data=~".*"})`,
  `info(metric, {non_existent=~".*"})`,
  `info(metric, {__name__!="target_info"})`,
  `info(metric, {__name__!~".+_info", data=~".*"})` (the vendored
  `info.test` shapes) plus, as of this revision, `info(metric, {})`
  itself and its in-place `@`/`offset` field-modifier forms
  (`info(m, {}@5)`, `info(m, {} offset 5m)`).
- **Upstream PR:** not yet filed (targets v3.13 semantics past the
  crate's v2.45 baseline, like G1 — recorded here as the divergence
  ledger instead).

## 7. `info()` second-argument direct-selector check (issue #132)

- **Files:** `src/parser/ast.rs` (`check_ast_for_call`),
  `src/parser/parse.rs` (`test_issue_132_info_direct_selector_parity`)
- **Bug:** Prometheus v3.13.0 (`parse.go:846-859` at PulsusDB's pinned
  conformance SHA `40af9c2`) requires `info()`'s second argument, when
  vector-typed, to be a **direct** `*VectorSelector`: the bypass block
  type-asserts `n.Args[1].(*VectorSelector)`; a named selector errors
  `expected label selectors only, got vector selector instead`
  (parse.go:852) and any other node — `*ParenExpr`, `*Call`,
  `*BinaryExpr`, ... — takes the `else` path and errors
  `expected label selectors only` (parse.go:858). This crate had no
  analogue (entry #6's recorded divergence), so
  `info(m, ({job="x"}))`, `info(m, target_info{job="x"})`,
  `info(m, sum(x))`, `info(m, m2 + m3)`, and `info(m, rate(x[5m]))` all
  parsed Ok where upstream rejects.
- **Fix:** `check_ast_for_call` gains the direct-selector check for
  `name == "info"`, gated on `args[1].value_type() == ValueType::Vector`
  (a non-vector arg falls through to `check_args_match_types`, so
  `info(m, 1)` keeps its type error as the FIRST error — upstream emits
  the parse.go:848 type error before the assertion error), placed BEFORE
  the entry-#6 empty-operand loop (upstream's info block runs before its
  generic per-arg checks, so `info({}, sum(x))` reports
  `expected label selectors only` first — oracle-verified ordering). A
  direct name-less `VectorSelector` passes (in-place `@`/`offset` are
  selector fields, not wrapper nodes, so `info(m, {job="x"} @ 5)` /
  `... offset 5m` stay accepted); a named one rejects with the
  parse.go:852 message; anything else with the parse.go:858 message
  (both verbatim, no position prefix — crate convention).
  `info(m, ({}))` is unchanged: entry #6's eager `Expr::Paren`
  empty-matcher guard fires bottom-up before the enclosing call is
  known, so it keeps rejecting with
  `vector selector must contain at least one non-empty matcher`
  (reject/reject parity with upstream; message-text difference
  pre-adjudicated in entry #6 — the eager guard is the load-bearing
  stack-overflow guard and must not be deferred).
- **Corpus rows retired:** the two `accept_reject` allowlist rows in
  `crates/pulsus-promql/tests/corpus/expected-divergences.jsonl` for
  `info(rate(http_request_counter_total{}[5m]), target_info{foo="bar"})`
  and `info(http_request_counter_total{namespace="zzz"}, {foo="bar"} == 1)`
  (corpus rows 303/305) — both now match upstream and the rows are
  deleted (the stale-allowlist guard enforces this).
- **Upstream PR:** not yet filed (targets v3.13 semantics past the
  crate's v2.45 baseline, like entry #6 and G1 — recorded here as the
  divergence ledger instead).

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

## G2. Grammar-production patch: native-histogram trim operators (issue #129)

- **Files:** `src/parser/promql.y`, `src/parser/token.rs`,
  `src/parser/lex.rs`
- **What:** Prometheus v3.13.0's experimental native-histogram *trim*
  operators, `</` (`TRIM_UPPER`) and `>/` (`TRIM_LOWER`). Ported from
  `generated_parser.y`/`lex.go` at PulsusDB's pinned v3.13.0 conformance
  SHA (`40af9c2`); no experimental feature gate exists for this pair
  upstream.
- **Grammar (`promql.y`):** `TRIM_UPPER`/`TRIM_LOWER` tokens added inside
  the `OPERATORS_START..OPERATORS_END` block (keeps `is_operator()`'s
  range check correct), immediately after `GTR` — matching
  `generated_parser.y`'s own token order. Precedence line extended to
  `%left EQLC GTE GTR LSS LTE NEQ TRIM_UPPER TRIM_LOWER` (comparison
  precedence, left-assoc, matching upstream). Two new `binary_expr`
  productions, clones of the `LSS`/`GTR` arms. `%expect-rr` rises from
  207 to 225 (each new comparison-precedence operator overlaps the same
  G1 `offset_duration_expr`/`duration_expr` ambiguity); `%expect`
  (shift/reduce) is unchanged at 11.
- **Lexer (`lex.rs`):** the `'<'`/`'>'` arms gain a `peek() == '/'` guard
  (checked before the existing `=` guard's fallthrough, mirroring
  upstream's `if/else if/else` chain) emitting `T_TRIM_UPPER`/
  `T_TRIM_LOWER`.
- **`token.rs`:** `token_display` gains the two display strings (`"</"`,
  `">/"`). `is_comparison_operator` is **deliberately unchanged** — it is
  what makes `bool` parse-reject on trim operators
  (`"bool modifier can only be used on comparison operators"`, `ast.rs`)
  and what exempts scalar operands of trim from the BOOL-modifier
  requirement, matching upstream's `IsComparisonOperator` (`lex.go:82-90`)
  excluding `TRIM_UPPER`/`TRIM_LOWER`.
- **Corpus inputs fixed:** the 110 native-histogram trim rows formerly in
  `crates/pulsus-promql/tests/promqltest/corpus/eval-divergences.jsonl`
  (`native_histograms.test`, e.g. `h_test </ 3`, `h_test >/ -Inf`,
  `cbh_for_join >/ on (label) float_for_join`).
- **Upstream PR:** not applicable — as with G1, this targets Prometheus
  v3.13 grammar past the crate's v2.45 baseline; recorded here as the
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
