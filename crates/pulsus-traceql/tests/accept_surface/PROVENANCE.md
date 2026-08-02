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
   `{ .a = 2 ^ 3 ^ 2 && "x" }` answers `(.a = 64)`, constant-folded, which
   settles `^` associativity by value. Every capture is recorded verbatim
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

## Operator precedence and associativity

Tightest first. `=` marks agreement from the audit capture, `✔` a
divergence this commit closed, `≠` one still open.

| level | reference | this parser | |
|---|---|---|---|
| 1 | `^`, left-associative (`2^3^2` = 64) | same since D8 was fixed | ✔ |
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
| aggregate argument — colon intrinsic, arithmetic, parentheses | yes | **≠ D7** |
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

## Regenerating

Re-record only with a deliberate grammar change, in the same commit:

1. edit the `pulsus` field of the affected probes (and `pulsus_parse` for
   meaning probes) to the new behaviour,
2. lower `DIVERGE` and raise `AGREE` in `accept_surface.rs` by the number
   closed,
3. run the live leg to confirm the `reference` side is unchanged:
   `PULSUSDB_TEMPO_DIFF_URL=http://localhost:13201 cargo test -p pulsus-traceql --test accept_surface`.

Adding a probe: append it with both sides recorded and bump the counts.
