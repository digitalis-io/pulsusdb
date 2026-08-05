# TraceQL parser accept-surface audit — provenance and findings

Issue #335. The deliverable is the **comparison**, not a patch: the two
gaps that prompted it (`{ !name = "foo" }`, `{ .a = span:duration }`) were
both found sideways, which is the signal that this area had never been
checked systematically.

## Why the construct registry could not have found these

`tests/conformance/` enumerates *documented constructs* and gives each one
a disposition. It cannot see a precedence level placed wrongly (both sides
accept, the meanings differ) and it cannot see an operand position never
wired up (no construct is missing — the construct is supported, just not
*there*). This audit enumerates from the **reference's grammar** instead:
operator precedence and associativity, and the set of expressions legal in
each operand position.

## Method

Black-box against an unmodified `grafana/tempo:3.0.2` container, the same
posture as `tempo_differential.rs` — no Tempo source is read, fetched or
vendored; our error text stays our own. Three observation channels:

1. **Accept/reject** — `2xx` = accept, `400` = reject over `/api/search`
   (or `/api/metrics/query_range` for metrics-form queries). Any other
   status is *inconclusive* and is excluded rather than scored, see
   `excluded_inconclusive` in `matrix.json`.
2. **The reference's echoed parse — the load-bearing part.** When the
   reference rejects on a *type* error it prints its own fully
   parenthesised parse of the offending expression. Appending a deliberate
   type error (`… && "x"`) to an otherwise valid query therefore makes the
   reference *state its own grouping*, which is why the precedence table
   below is **read off the reference rather than inferred** from accept /
   reject statuses — those cannot separate two groupings that both parse.
   `{ .a = 1 || .b = 2 && "x" }` answers `((.a = 1) || (.b = 2)) && \`x\``;
   `{ .a = 2 ^ 3 ^ 2 && "x" }` answers `(.a = 64)`, constant-folded — but
   **that value does NOT settle `^` associativity**, and reading it that
   way produced a wrong entry that misled two people (issue #335 Stage B).
   A folded value settles grouping only if the operator's own semantics
   are known, and this one's are not what they appear: the reference's
   INTEGER `^` swaps its operands. Grouping was settled structurally
   instead — `2 ^ 3 ^ 2` equals `2 ^ 8` and not `9 ^ 2`, using the
   reference's own folds of the subexpressions — which needs no model of
   the operator. **A folded value pins a value; use two of them against
   each candidate grouping to pin a grouping.** Every capture is recorded verbatim
   in `reference_parse`. Treat a precedence claim here as unverified
   unless it carries one.
3. **Result differential**, for the one level with no error channel
   (spanset `&&`/`||`): push spans over OTLP, then compare the returned
   trace/span sets of the bare form against both explicit parenthesisations.

Verdicts were captured twice — against an empty store and against a
populated one — and only stable ones were kept.

## Headline

Audit capture: 221 probes — **176 agree, 45 diverge** on accept/reject,
plus **7 probes both implementations accept and parse differently** (the
quiet half). Eleven structural divergence classes.

**First fix wave (this commit)** closed five of them — D2, D8, D9, D10,
D11 — leaving **196 agree, 25 diverge, 3 meaning divergences**. The
arithmetic: D2 carried twenty accept-surface probes, all of which flipped
reject→accept (45 − 20 = 25 diverging, 176 + 20 = 196 agreeing), and no
agreement became a divergence; D8/D9/D10/D11 are meaning-only and carry no
accept-surface probe, so they move only the meaning count (7 − 4 = 3).
`accept_surface.rs` asserts that arithmetic rather than narrating it.

**Corpus impact of the fix wave: two renderings, no dispositions.** Every
committed corpus case and registry probe was dumped through the parser
before and after and diffed. `accept/field_precedence` and
`accept/spanset_precedence` re-group (they are the only committed cases
mixing `&&` with `||`) and their goldens were regenerated; nothing else in
the corpus, and no registry probe, changed either its outcome or its
rendering.

**Still open — the field-expression regrammar (D1, D3–D7).** Not a
collection of patches: see the root cause below.

## Stage B re-pin (issue #335, 2026-08-03)

The grammar collapse: one precedence-climbing routine replaces the layered
field-expression parser, so every operand position takes the same grammar
and `!` becomes a field-level prefix operator.

**Parse axis (`parse ∘ validate`): 19 probes moved, every one
`diverge → agree`, none the other way.** `AGREE` 198 → 217,
`DIVERGE` 23 → 4. The direction is checked in the re-pin commit's diff —
19 changed `"verdict"` lines, all `diverge` → `agree` — not inferred from
the totals, because two errors that cancel leave the totals right.

| class | probes | direction | why it is an improvement |
|---|---|---|---|
| D1 | 5 | 4 accept→reject, 1 reject→accept | `!` binds tighter than `=`, so its operand is the intrinsic. `{ !name = "foo" }` is a reference **400** (`illegal operation for the given type: !name`) and is now ours too, with the same message; `{ !.a = !.b }` is a reference 200 we refused. |
| D3 | 7 | reject→accept | unary `-` is legal in every operand position, not just the RHS. |
| D4 | 4 | reject→accept | a literal is legal at the comparison LHS. |
| D5 | 2 | reject→accept | a parenthesised expression is legal at the LHS. |
| D6 | 1 | reject→accept | comparison is an ordinary left-associative binary level, so it chains. |

The 4 residual divergences are all **D7** — `avg(<field expr>)` still
takes a restricted argument grammar. Stage C.

**Wire axis (`parse → validate → plan`): 16 probes moved, every one
`diverge → agree`.** `WIRE_AGREE_BASELINE` 161 → 177,
`WIRE_DIVERGE_BASELINE` 60 → 44.

The two axes disagree on purpose and the gap is the point: parse-axis 4
vs wire-axis 44. A query can parse and validate and still be a planner
400, and the wire is where users live. Stage B itself produced four such
regressions — `{ !.a = 1 }` and friends validate but had no planner arm —
and they were caught by this baseline, not by the parse-axis count, which
read a clean 4 throughout. See `traces/filter.rs`'s `BoolMatch`.

**Every parse-axis flip is accounted for.** 70 probes now parse and are
rejected by `validate`; each is attributable to a rule on the
parse→validate class list in `validate_field_expr`'s doc comment, asserted
by `every_parse_axis_flip_is_explained_by_the_class_list`. That test
replaces Stage A's `stage_a_flipped_exactly_the_two_recorded_d1_probes`,
which named the flip set as two exact queries — right while two rejections
were movable, unusable at 70.

**Issue #359 closed by this change, not deferred by it.** It recorded five
value forms we 400'd and the reference answers (`{ duration = 5 }`,
`{ duration > 5 }`, `{ .a = ok }`, `{ .a = server }`, `{ .a = unset }`)
and expected Stage B to preserve those rejections behind a pointer. The
context-free atom grammar accepts all five instead: value typing stopped
being the parser's job, which was exactly #359's predicted mechanism, so
the fix arrived with the collapse rather than after it. Re-measured
against the pinned digest — all five plus `{ .a = error }`,
`{ .a = client }`, `{ duration = 5.5 }`, `{ span:duration = 5 }`,
`{ duration > -2s }` are reference 200s and ours accept, ten for ten.

**They are NOT probes in this matrix**, which stays at 221 through Stage B
by ruling. Adding the class as probes is follow-up work, and until then
this class remains unmeasured by the scoreboard — which is what let it go
unnoticed in the first place.

## Stage C re-pin (issue #335, 2026-08-04) — the aggregate argument

D7's mechanism: `parse_aggregate` took a bare `Field` and screened it
against a hand-maintained aggregatable-intrinsic allowlist. The reference
parses an ORDINARY FIELD EXPRESSION there and decides legality in its
validator. Stage C makes the argument a `FieldExpr` and ports that check
as `validate.rs` rule 11 — the argument's implied type must be numeric
or attribute, and it must reference the span.

**Parse axis: 4 probes moved, every one `diverge → agree`.**
`AGREE` 217 → 221, `DIVERGE` 4 → **0**. Two both-reject probes
(`{ true } | avg(1) > 1`, `{ true } | avg("x") > 1`) keep their verdict
and move their rejection from the parser to the validator, so the
parse-axis flip set goes 70 → 72.

Rule 11 is two `rule_id`s because the reference words its two halves
differently and the ORDER between them is observable (measured, pinned
digest, shadow `query=` route):

```
{} | avg("x") > 1  ->  400 aggregate field expressions must resolve to a number type: avg(`x`)
{} | avg(1) > 1    ->  400 aggregate field expressions must reference the span: avg(1)
{} | avg(!1) > 1   ->  400 illegal operation for the given type: !1   (the inner rules run first)
```

The whole `AggregateOp × Intrinsic` product was re-measured for the
type half: the numeric intrinsics (`duration`, `span:duration`,
`span:childCount`, `traceDuration`/`trace:duration`,
`event:timeSinceStart`, `nestedSetParent|Left|Right`) and the two
`instrumentation:` ones (the mirrored attribute-typing quirk) are
reference 200s; every string/status/kind intrinsic is a 400. `count(x)`
is a parse error on both sides. `validate.rs`'s
`aggregate_over_every_intrinsic_matches_the_reference_type_rule` asserts
that product cell by cell and replaces the D4 tier-2 parse guard the
rule retires — a blanket parse rejection could not have told
`max(span:childCount)` (a 200) from `avg(rootName)` (a 400), which is
why the decision had to move rather than be re-tuned.

**Wire axis: D7 stays OPEN, and the parse-axis zero does not say
otherwise.** `avg(span:childCount)` and `avg(trace:duration)` have no
numeric aggregation path in the planner and `avg(.a + 1)` is a composite
source; all three are still planner 400s against a reference 2xx. The
class row carries `wire_status: "open"` and its note, asserted by
`accept_surface.rs::a_class_open_on_the_wire_has_a_probe_still_diverging_there`.

**One probe moved on the wire, and the freeze gate gained a direction
rather than a waiver.** `{ true } | avg((.a)) > 1` is now a wire accept:
parentheses group without surviving into the AST, so it IS `avg(.a)` —
the same plan the planner has always served.

*Correction, and it matters because the re-pin was allowed on this
ground:* an earlier version of this section — and the review round that
accepted it — said the flip was reached "without touching planner code".
**That is false.** `search_plan.rs` DOES change here: the aggregate
source match and `aggregate_threshold` destructure a `FieldExpr` instead
of a `Field`, and a rejection arm is added for composite sources. What
is true is narrower and was measured rather than argued, on both trees:

```
49cff9a (pre-Stage-C)   { true } | avg(.a) > 1     -> plans OK
                        { true } | avg((.a)) > 1   -> PARSE ERROR (never reaches the planner)
this tree               { true } | avg(.a) > 1     -> plans OK, SearchPlan byte-identical to 49cff9a's
                        { true } | avg((.a)) > 1   -> the SAME plan, byte for byte
```

So the flip is **parser-caused**: the query previously died at parse, and
the plan it now reaches is one the planner already produced, unchanged by
this change. The planner edits are type-mechanical for the shapes that
could already exist, plus a rejection for shapes that could not; they do
not move any probe's wire disposition.

`wire_baseline.json` is re-pinned accordingly:
`WIRE_AGREE_BASELINE` 195 → **196**, `WIRE_DIVERGE_BASELINE` 26 → **25**,
one `pulsus_wire` field `reject` → `accept`.

Rather than merge that "by human override", the `wire-baseline-freeze`
job now states the rule it was always enforcing badly: **the baseline
may move only toward fewer divergences.** Per probe, nothing that agreed
on `origin/main` may diverge here; and neither the divergence count nor
the probe set may grow. Both "before" sides — the baseline and the
reference column it is scored against — are read from `origin/main`, so
nothing in a PR decides its own verdict. A decrease cannot express a
poisoned baseline (it is strictly more agreement with an oracle the PR
cannot move); an increase stays forbidden, which is the freeze. Verified
against real git states: the honest decrease passes, while a regressing
probe, a compensating swap that leaves the count unchanged, an added
already-diverging probe, a divergence retired by deleting its probe, and
a PR that rewrites its own `reference` column to match all fail.

## Wire-axis ownership: `owning_issue` (issue #335 follow-up, 2026-08-05)

A probe that agrees on the parse axis and is a planner 400 against a
reference 2xx names the issue that owns the gap, in an `owning_issue`
field on the probe. Ten probes do today, and it moved no verdict, no
count and no wire disposition — the field is metadata plus the gate that
requires it.

| probes | owner | evidence |
|---|---|---|
| `avg(span:childCount)`, `avg(trace:duration)`, `avg(.a + 1)` | **#335** | the refusing arm is Stage C's own (`search_plan.rs`, the `PipelineStage::Aggregate` source match, comment "the executable subset is decided HERE"); D7 is a #335 class (`closed_by: 335`) and its `wire_status: "open"` note already names exactly these three; they route to `/api/search`, not the metrics planner |
| `rate() by(.a\|span.a\|name\|span:id)`, `rate() by(.a, .b)`, `quantile_over_time(.a, .5)`, `max_over_time(.a)` | **#182** | the planner's own 400 names it — `by() currently supports grouping by resource.service.name only (issue #182)`, `by() currently supports a single grouping key (issue #182)`, `<func>() currently supports the duration target only (issue #182)` (`metrics_plan.rs`), which is #351's "#182 follow-up" row made machine-readable |

An owner is required while the probe diverges on the wire and forbidden
once it agrees, so closing a gap deletes its pointer in the same change
that re-pins the baseline — the `closed_by` posture, one axis over.

**Why it needed its own gate.**
`a_class_open_on_the_wire_has_a_probe_still_diverging_there` quantifies
over CLASSES that declare a `wire_status` and reaches a probe only
through `class`/`closed_class`. A probe that diverges on the wire while
agreeing on parse has neither field by construction — an agreement may
not carry `class` — so all ten were invisible to it: replayed against
the pre-change data, that test is green while
`a_wire_divergence_the_parse_axis_cannot_see_names_its_owning_issue`
names all ten. The parse axis needs no such field because its
divergences are owned by construction (every class is declared here and
this matrix's own `owning_issue` is the audit issue); a wire divergence
belongs to whichever planner refuses the query, which is not knowable
from the class.

**The join both wire gates read through is validated first** (review
round, `wire_dispositions`): duplicate `query` keys in
`wire_baseline.json` fail, and every baseline entry must name a matrix
probe. A duplicate is not cosmetic here — with the earlier
`.iter().find(...)` lookup an *earlier* copy reading `accept` hid a real
divergence, and the ownership gate then did not require an owner for it;
measured, the pre-fix tree is 12/12 green on exactly that mutant. This is
the Rust spelling of the weakness `wire-baseline-freeze` rejects in every
file it builds a join from.

## Operator precedence and associativity

Tightest first. `=` marks agreement from the audit capture, `✔` a
divergence this commit closed, `≠` one still open.

| level | reference | this parser | |
|---|---|---|---|
| 1 | `^`, **right**-associative (`2^3^2` ≡ `2^8`; the folded value 64 alone does not establish this — see the method note) | same since D8 was fixed; the grouping agrees and the OPERATOR diverges deliberately (`traceql-pow-integer-operand-swap`) | ✔ |
| 2 | `* / %`, left-associative | same | = |
| 3 | unary `!`, unary `-` (so `-2^2` = -4, `-.a*2` = `-(.a*2)`) | unary `-` matches since D9 was fixed; unary `!` is still not at this level | ✔ / ≠ D1 |
| 4 | `+ -`, left-associative | same | = |
| 5 | `= != < <= > >= =~ !~`, left-associative and **chainable** | one comparison only; a second is a grammar error | ≠ D6 |
| 6 | `&&` and `||` — **one level**, left-associative | same since D10 was fixed | ✔ |
| — | spanset structural `> >> < << ~` (and `!`/`&` forms) bind tighter than the spanset logical operators, left-associative | same | = |
| — | spanset `&&` / `||` — one level, left-associative (established by a result differential, not by status) | same since D11 was fixed | ✔ |
| — | arithmetic binds tighter than comparison | same | = |

Unary sitting *between* `* / %` and `+ -` is the reference's real
behaviour, not a transcription slip: `!.a * 2` groups as `!(.a * 2)` while
`!.a + 2` groups as `(!.a) + 2`, and the same for unary `-`.

## Operand positions

| position | reference accepts | agreement |
|---|---|---|
| comparison RHS — dotted attribute, every scope | yes | = |
| comparison RHS — bare intrinsic (all 11) | yes | = |
| comparison RHS — colon-scoped intrinsic (all 18) | yes | ✔ since D2 was fixed |
| comparison RHS — arithmetic, parentheses, unary `-` | yes | = |
| comparison RHS — unary `!` | yes | **≠ D1** |
| comparison LHS — attribute / intrinsic / arithmetic | yes | = |
| comparison LHS — colon-scoped intrinsic | yes | = |
| comparison LHS — literal, or literal-only comparison | yes | **≠ D4** |
| comparison LHS — parenthesised expression | yes | **≠ D5** |
| comparison LHS — unary `-` / `!` | yes | **≠ D1, D3** |
| aggregate argument (`avg/min/max/sum`) — attribute, `duration` | yes | = |
| aggregate argument — colon intrinsic, arithmetic, parentheses | yes | ✔ since D7 was fixed (parse axis; three of the four are still planner 400s — see the Stage C section) |
| aggregate argument — literal, empty, two arguments; `count(x)` | no | = |
| aggregate comparison RHS — attribute or another aggregate | no | = |
| `select(...)` — attribute, bare intrinsic, colon intrinsic, list | yes | = |
| `select(...)` — literal, arithmetic, unary, parentheses, empty, comparison | no | = (14/14) |
| `by(...)` — attribute, scoped attribute, intrinsic, colon intrinsic, list | yes | = |
| `by(...)` — literal, arithmetic, unary, parentheses, empty | no | = (11/11) |
| structural operands — spanset on both sides | yes | = |
| bare attribute (existence), `= nil` / `!= nil` | yes | = |
| bare intrinsic with no comparison | no | = |

`select()` and `by()` agree completely — 25 probes, no divergence. That
result is as much the audit's output as the divergences are: those two
positions do not need work.

## Divergence classes

`D1` unary `!` binds looser than comparison (accept-surface **and** quiet
meaning) · `D2` colon-scoped intrinsic rejected as a comparison RHS ·
`D3` unary `-` rejected outside the RHS · `D4` literal rejected at the LHS ·
`D5` parenthesised expression rejected at the LHS · `D6` chained comparison
rejected · `D7` aggregate argument must be a bare field · `D8` `^`
associativity · `D9` unary `-` versus `^` · `D10` field-level `&&`/`||`
precedence · `D11` spanset-level `&&`/`||` precedence.

**Closed by this commit:** `D2`, `D8`, `D9`, `D10`, `D11`. Each carries
`"status": "closed"` in `matrix.json`, and the harness *asserts* closure —
a closed class with any probe still diverging fails, and each closed
meaning probe is checked by parsing the bare query and the reference's
grouping written with explicit parentheses and comparing the ASTs.

**Still open:** `D1`, `D3`–`D7`. `D1`–`D7` share one root cause: this parser's field expression is not an
expression grammar. `parse_field_primary` parses a *field*, then peeks and
dispatches on what follows through three ad-hoc predicates
(`rhs_begins_field`, `rhs_begins_arith`, and the LHS arithmetic peek). The
reference has a single operand grammar with comparison as an ordinary
binary level, which is why every operand position is uniform there and
patchy here. `D8`–`D11` are precedence-table entries placed differently.

**State after Stage C (2026-08-04).** The two paragraphs above are the
first fix wave's record and are kept as written; they are not the
current state. Every accept-surface class D1–D11 now carries
`"status": "closed"` on the PARSE axis — the collapse (Stage B) closed
D1 and D3–D6, Stage C closed D7, and `DIVERGE` is 0 over all 221
probes. That is a statement about the grammar; `D12` (the meaning class
the Stage B capture opened) stays open on its six meaning probes. On
the WIRE axis `wire_baseline.json` is where the
answer lives, and D7 is recorded `wire_status: "open"` there and in its
class row: three of its four probes are still planner 400s the reference
answers.

## Regenerating

Re-record only with a deliberate grammar change, in the same commit:

1. edit the `pulsus` field of the affected probes (and `pulsus_parse` for
   meaning probes) to the new behaviour,
2. lower `DIVERGE` and raise `AGREE` in `accept_surface.rs` by the number
   closed,
3. run the live leg to confirm the `reference` side is unchanged:
   `PULSUSDB_TEMPO_DIFF_URL=http://localhost:13201 cargo test -p pulsus-traceql --test accept_surface`.

Adding a probe: append it with both sides recorded and bump the counts.

Re-pinning the wire baseline: a probe that leaves the divergence list
loses its `owning_issue` in the same change, and one that arrives on it
gains one (see *Wire-axis ownership* above) — the gate fails either way
round.

## The `!` / absence capture (Stage B, AC 4)

Captured 2026-08-03 against the pinned digest, **before** the Stage B
de-conflation commit (its own commit, so the ordering is a fact in the
history rather than a claim). Channel: result differential — spans pushed
over OTLP with attribute batteries `a = {true, false, absent, "x",
resource-only true}` and boolean-only `c = {true, false, absent}`, each
spelling queried over `/api/search` and scored by matched span set,
stable across three rounds.

Findings, wider than the four spellings the ruling named (the sweep was
built claim-first: every spelling our tree maps onto `Exists` /
`Not(Exists)`):

| spelling | reference (measured) | this tree today |
|---|---|---|
| `{ .a }` | truthiness — only `a == true` | presence |
| `{ .a != nil }` | presence | presence (agrees) |
| `{ !.a }` | boolean NOT — only `a == false`; absent never matches; non-boolean is an evaluation error | absence |
| `{ .a = nil }` | stable but inscrutable sets matching no simple predicate | absence |

Scoped (`span.`) variants behave identically at span scope. Two
operational notes: the reference's `!` over a non-boolean value fails the
whole query on the live-store path (`expression (!.a) expected a
boolean`), so the oracle acceptance leg for these probes needs a
container without such data; and the `= nil` sets, while deterministic,
correspond to no absence/presence reading — a decision (follow, or keep
our coherent absence semantics as a ledgered divergence) is required at
the de-conflation design point and is deliberately NOT taken by this
capture.
