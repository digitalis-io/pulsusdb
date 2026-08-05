# Traces differential divergence ledger

The M4 traces differential (`e2e/src/traces.rs`, issue #60) gates every
corpus-computable TraceQL case in
`test/fixtures/traces/differential.json` against both the corpus's
by-construction expectation and the pinned reference Tempo
(`grafana/tempo:3.0.2`, digest-pinned in
`deploy/e2e/compose.single.yaml`). **The exclusion list starts empty.**

A case moves from `mode: "gated"` to `mode: "informational"` only via
the #33 triage discipline:

1. an **observed live divergence** (a failed gated run with its dumped
   repro artifact from `target/e2e-artifacts/traces-diff/`),
2. triaged **fix-our-bug vs ratify-documented-difference**, and
3. recorded here as an entry whose id the fixture case's `ledger` field
   references (a hermetic unit test in `e2e/src/traces.rs` enforces the
   fixture↔ledger link both ways).

Entries are append-only; re-gating a case removes its `ledger` reference
but keeps the entry for history.

## Entries

### 2026-07-16-negation-matches-missing-key

- **Case:** `neg_attr_missing_key` — `{ resource.run_id = "{R}" &&
  resource.env != "prod" }`, where a deterministic subset of the corpus
  (`trace_idx % 5 == 4`) carries no `env` resource attribute at all.
- **Observed divergence (live run, 2026-07-16):** gated run against
  `grafana/tempo:3.0.2` failed with
  `tempo_vs_corpus`: expected 14 traces, PulsusDB returned all 14, Tempo
  returned 10 — missing exactly the 4 traces whose resources lack the
  `env` key (repro artifact
  `traces-diff/single/search-mismatch-5e98104cd2edb55c.json`; query
  `{ resource.run_id = "e2e-traces-diff-2a977e5fd55b1e36" &&
  resource.env != "prod" }`).
- **Triage:** ratify-documented-difference, not a PulsusDB bug.
  PulsusDB's behavior is the committed contract — docs/api.md §4.2:
  "`!=`/`!~` on an attribute match spans **lacking the key entirely** as
  well as spans whose value differs" (the negation rule ratified on
  issue #57 and exercised by the frozen part-(a) golden
  `negated_attr.sql`). Tempo's TraceQL evaluates a comparison against a
  missing attribute as non-matching, so its `!=` excludes absent-key
  spans. Both stores agree on `!=`/`!~` whenever every span carries the
  key (`neg_attr_key_on_all` / `neg_regex_key_on_all` remain GATED and
  pass three-way).
- **Disposition:** `neg_attr_missing_key` moves to
  `mode: "informational"`. PulsusDB stays hard-gated against the
  corpus expectation under our documented rule (a PulsusDB regression
  on this case still fails the scenario); only the Tempo comparison is
  reported as an informational artifact.

### `traceql-validate-re2-unknown-residual` (issues #328, #336)

- **What:** the #328 semantic validator's regex check
  (`pulsus_traceql::validate` → `pulsus_re2::re2_verdict`) is
  three-valued, and `Unknown` is ACCEPTED — deciding "RE2 rejects this"
  in-process needs an RE2-syntax parser this codebase deliberately does
  not have (the root fix is **#336**; closing #336 retires this row).
- **The `Unknown` surface, enumerated from the code** (fix round 1: the
  first version of this row listed three probed classes; the honest
  enumeration is every `Unknown` return site in
  `crates/pulsus-re2/src/lib.rs`, pinned representative-by-site by
  `every_unknown_return_site_has_a_named_class_representative` and
  covered vector-by-class in `validate-vectors.json`). Per class, the
  PINNED REFERENCE's verdict
  (`grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad58
  700aa96880653c3d8f7`, measured on the validation-only shadow route):
  - **`lookaround`** (`(?=`, `(?!`, `(?<=`, `(?<!`) — reference **400**
    on all four (vectors `rx-u7`–`rx-u10`). The commonest member of the
    residual by far.
  - **`unicode-property`** (`\p`/`\P`, bare or inside a class) —
    verdict varies BY MEMBER: properties in RE2's fixed table are 200
    (`\p{L}`, `rx-u17`), the rest 400 (`\p{Alphabetic}` bare and in
    class position, `rx-u4`/`rx-u16`).
  - **`rust-only-escape`** (`\u`, `\U`) — reference **400**
    (`rx-u12`/`rx-u13`).
  - **`trailing-backslash`** (`a\`) — reference **400** (`rx-u11`;
    jointly rejected in fact — the Rust crate rejects it too — but the
    scan defers before compiling, by design; deciding it is #336's).
  - **`nonportable-group-head`** (`(?x`, `(?u`, `(?#`, …) — reference
    **400** (`rx-u18`–`rx-u20`).
  - **`over-max-repeat`** (`{n}` above `kMaxRepeat = 1000`) — reference
    **400** (`rx-u5`).
  - **`repetition-of-repetition`** (`a**`, `a{2}{3}`, `a*??`) —
    reference **400** (`rx-u6`/`rx-u21`/`rx-u22`).
  - **`compiled-too-big`** (`regex::Error::CompiledTooBig` — the Rust
    crate's compiled-size budget, which is not RE2's; per-member the
    verdict depends on the two engines' budgets) — the measured member
    is a reference **400** (`rx-u23`).
  - **Agreement classes** — `Unknown` here AND accepted by the
    reference, so no consumer-visible divergence: `named-group`
    (`(?P<n>`, `(?<n>` — plan v3 wrongly listed this as rejected;
    re-measured 200, `rx-u3`), `literal-quoting` (`\Q…\E`, `rx-u1`),
    `octal-escape` (`rx-u2`), `boundary-escape` (`\<`/`\>`/`\b{…}` —
    the reference reads them as literals/boundary-plus-braces and
    accepts, `rx-u14`/`rx-u15`; the MEANING differs, which is the
    screen's original subject and precisely why they defer).
- **Which paths the residual reaches:** on search's `q=` and on the
  metrics query parameter the expression is planned and EXECUTED, so
  ClickHouse's RE2 rejects the diverging members at execution
  (`Code: 427` → #280's classifier → `400`) and only the error's origin
  and timing differ from the reference. On search's shadow `query=`,
  which is validated and never executed, **nothing catches them**: the
  request is a `200` where the reference is a `400` — for every
  reference-400 class above, lookarounds included.
- **Gates:** `pulsus-traceql/tests/validate_corpus.rs` asserts this row
  id and the vectors reference each other both ways and that every
  class above carries a vector;
  `tempo_differential.rs::validate_vectors_match_the_live_reference`
  re-measures every vector against the pinned digest, fail-closed.

### `traceql-validate-nil-spelling-conflation` (issue #328) — **RETIRED by #335 Stage B**

- **Retired 2026-08-03.** The row's own exit condition was "the fix rides
  whenever the AST next changes shape", and the Stage B grammar collapse
  is that change: `= nil` / `!= nil` now fold to a single `Exists { field,
  negated }` node carrying the polarity as a flag, so `{ name = nil }` and
  `{ !(name != nil) }` are DIFFERENT ASTs and the validator can tell them
  apart. Re-measured against the same pinned digest after the collapse:
  `{ name = nil }` 400 / reject, `{ !(name != nil) }` 200 / accept,
  `{ name != nil }` 200 / accept, `{ .a = nil }` 200 / accept,
  `{ !(.a != nil) }` 200 / accept,
  `{ !(resource.service.name != nil) }` 200 / accept — six for six.
  The over-rejection is gone; no divergence remains to record.
- **Gate:** `validate::tests::the_nil_spellings_are_no_longer_conflated`
  now pins the DISTINCTION (both the AST inequality and both verdicts), so
  a regression to the conflated shape fails rather than quietly restoring
  the divergence this row described. The `nil-d1`/`nil-d2` vectors were
  re-recorded with the matching verdicts in the same change.

The original entry, kept for the trail:

- **What:** the parser normalizes `x = nil` to `Not(Exists(x))` — and
  the double-negation spelling `!(x != nil)` produces the SAME node, so
  the two are indistinguishable to the AST-based validator. Measured at
  the pinned digest: `{ name = nil }` is a `400` (`intrinsics cannot be
  nil`) and `{ !(name != nil) }` is a `200`. PulsusDB rejects BOTH
  (vectors `nil-d1`/`nil-d2`: `tempo: accept`, `pulsus: reject`) — the
  canonical `= nil` rejection matches the reference, and the exotic
  double-negation spelling is over-rejected as the price.
- **Why deliberate (for now):** distinguishing the spellings needs a
  parser/AST change (a dedicated nil-comparison node), which churns the
  frozen corpus goldens and sits outside #328's file list; plan v1's
  premise that `Not(Exists(intrinsic))` "can only come from `= nil`"
  was measured false during implementation. Recorded here rather than
  silently absorbed; the fix rides whenever the AST next changes shape.
- **Gates:** the same vectors↔ledger link as above;
  `validate::tests::the_nil_double_negation_spelling_is_conflated_and_rejected`
  pins the conflation itself, so the row cannot outlive the AST that
  causes it.

### `traceql-pow-integer-operand-swap` (issue #335 Stage B; source located #351)

- **Where a reader meets it in the reference's source** (added on #351,
  after the v3.0.2 tree was read directly): `pkg/traceql/ast_execute.go`
  defines `intPow(base, exp int)` at `:940-942` and CALLS it as
  `intPow(rhsN, lhsN)` at `:486` (and again in the array-element path at
  `:741`) for `lhs ^ rhs` — the arguments are transposed at the call
  site, not in the helper. The float catch-all at `:652` is
  `math.Pow(lhs.Float(), rhs.Float())`, in the correct order, which is
  exactly why the condition below is load-bearing: the two paths
  disagree, and the literal's spelling picks the path. The measurements
  came first; this citation explains why they look the way they do and
  generalises past the inputs that were probed.

- **Reference behaviour (measured, `grafana/tempo@sha256:aa8df8d0…`,
  v3.0.2):** the `^` operator **swaps its operands on the INTEGER path**.
  Measured, with the folded constant read from the reference's own
  type-error message:

  | query | reference | `lhs^rhs` would be |
  |---|---|---|
  | `2 ^ 10` | 100 | 1024 |
  | `10 ^ 2` | 1024 | 100 |
  | `2 ^ 3` | 9 | 8 |
  | `5 ^ 0` | 0 | 1 |
  | `0 ^ 5` | 1 | 0 |
  | `2 ^ -1` | 1 | 0.5 |

- **The condition is load-bearing — this is NOT a blanket swap.** One
  float operand takes a correct path:

  | query | reference |
  |---|---|
  | `3 ^ 4` | **64** (swapped) |
  | `3.0 ^ 4` | **81.0** (correct) |
  | `2 ^ 10` | 100 (swapped) |
  | `2.0 ^ 10`, `2 ^ 10.0`, `2.0 ^ 10.0` | 1024.0 (correct) |
  | `4 ^ 0.5` | 2.0 (correct) |

  The same mathematical operands give opposite results, decided by the
  literal's spelling. Anyone writing "the reference's `^` swaps its
  operands" without the condition states something false for
  `2.0 ^ 10`, so the row would read as wrong and take the ledger's
  credibility with it.

- **PulsusDB behaviour:** `lhs ^ rhs` on every path. `2 ^ 10` is 1024.

- **Why deliberate:** this is a defect, not a convention. A result that
  depends on whether the user typed `3` or `3.0` cannot be relied on, and
  a user writing `2 ^ 10` means 1024. Under the standing mandate we match
  the reference except where it is wrong.

- **Consequence, stated plainly because it looks like a regression:**
  `{ .a = 2 ^ 3 ^ 2 }` is **512** for PulsusDB and **64** for the
  reference. Grouping AGREES — both are right-associative (established
  structurally; see `FieldOp::is_right_assoc`). Only the operator
  diverges.

  The merged #335 audit previously made PulsusDB answer 64 by making `^`
  **left**-associative with a correct operator: `(2^3)^2` = 8² = 64. The
  reference reaches 64 by right grouping with a swapped operator:
  `2^(3^2)` = `2^8` → 8² = 64. **Two independent errors cancelled for
  that one input.** Fixing the grammar without copying the defect
  necessarily breaks the agreement, and the agreement was never evidence
  of a shared model — a single value can only ever pin a value. That
  finding is not a criticism of the audit: seeing it took a three-term
  structural probe plus a characterisation of the operator, neither of
  which a single-value comparison can motivate.

### `traceql-event-link-operand-any-match` (issue #351)

**Owner ruling, 2026-08-05.** The row exists so the next reader can
re-decide from the evidence rather than re-derive it.

- **What PulsusDB does.** When a span-event or span-link intrinsic
  (`event:name`, `event:timeSinceStart`, `link:spanID`, `link:traceID`)
  is compared against another FIELD, the span matches if **any** of its
  events (or links) satisfies the comparison; `!=` matches only when
  **every** one does, so a span with no events at all satisfies it. Six
  probes: `{ .a = event:name }` and the three other intrinsics, plus the
  two reverse-order spellings.

- **What the reference does.** It answers the same queries from the
  **FIRST event only**. Measured against the pinned container
  (`grafana/tempo@sha256:aa8df8d0…`, v3.0.2) with a discriminating
  fixture — one positive example cannot tell "any" from "first" from
  "all", so the fixture varies WHICH event matches:

  | events on the span | `.a` holds | reference | PulsusDB |
  |---|---|---|---|
  | `evX, evY, evZ` | `evZ` (last) | no match | **match** |
  | `evP, evQ, evR` | `evP` (first) | **match** | **match** |
  | `ev1, evM, ev2` | `evM` (middle) | no match | **match** |
  | `ev7, ev8` | `evNope` | no match | no match |

  Each of those event names is individually queryable there
  (`{ event:name = "evZ" }` returns its span), so the data is fully
  indexed and present; the field-vs-field form simply consults the first
  entry. Stable across three runs.

  The negated form was measured on the same store and agrees with that
  reading from the other side: `{ .a != event:name }` returns the
  `evZ`-last and `evM`-middle spans there (their FIRST event differs, so
  the reference keeps them) and not the `evP`-first span. Ours excludes
  all three, because in each of them SOME event matches. Two independent
  confirmations of the same divergence, in opposite directions.

  **One edge the reference and PulsusDB already agree on:** its `!=`
  also returns spans with NO events at all (measured — the link-only
  spans in the same fixture come back), which is the empty-set rule we
  implement. So the disagreement is confined to spans that HAVE events
  and where the matching one is not the first.

- **The reference's own behaviour VARIES BY ROUTE, which is why this is
  not a contract to copy.** Three readers of the same span disagree, and
  each is reachable from user queries:

  | route | which event | citation (v3.0.2) |
  |---|---|---|
  | pushdown / fetch conditions (`{ event:name = "evZ" }`) | **any** | the parquet condition iterator matches any event row — measured: the span whose LAST event is `evZ` is returned |
  | `AttributeFor` (the field-vs-field path) | **first** | `tempodb/encoding/vparquet4/block_traceql.go:128-152` — `find` returns the first entry whose attribute matches; reached for intrinsics at `:227-241`; one entry per event is appended at `:3683-3691` |
  | `AllAttributes` (response projection) | **last** | `tempodb/encoding/vparquet4/block_traceql.go:65-104` — a `map[Attribute]Static`, so the last write wins |

  The first-event answer is a property of a linear first-match scan over
  a flat per-event list, not a designed rule. It is also indefensible on
  its own terms from the user's side: adding an OLDER event to a span
  would change whether it matches, though the event asked about did not
  change.

- **Ours is the reference's own DESIGNED multi-value rule.**
  `pkg/traceql/ast_execute.go:535-627` @ v3.0.2 compares a scalar against
  an array elementwise: `matchAll` is set for `OpNotEqual`/`OpNotRegex`
  and the result is `matchCount == elemCount`, otherwise `matchCount > 0`.
  We implement that arithmetic exactly, which is where three of our
  edge-case answers come from — an empty set satisfies `!=`
  (`0 == 0`), a cross-type element makes `!=` false rather than true,
  and an absent scalar operand never matches (issue #183's rule,
  unchanged).

- **The migration copying the reference would require.** Not
  implementable on this storage without a breaking schema change plus a
  rebuild of stored data:

  - event/link rows in `trace_attrs_idx` carry the **span's**
    `timestamp_ns`, with no event ordinal
    (`crates/pulsus-write/src/protocols/otlp_traces.rs`, the span-event
    and span-link fan-out);
  - the table is a `ReplacingMergeTree` ordered by
    `(key, val, scope, timestamp_ns, trace_id, span_id)`, so two events
    with the SAME name on one span collapse into one row — the ordering
    information is destroyed by construction, not merely unrecorded;
  - so "first event" needs a new indexed ordinal column (migration +
    write-path change + backfill of existing data), or a per-span
    payload decode in the Phase-2 hot loop.

  Paying that to reproduce a self-inconsistent artefact was judged the
  wrong trade for a query shape that is rare next to the literal form
  `{ event:name = "timeout" }`, which is common and already agrees.

- **Where it is enforced.** `filter::LeafEval::EventSetCompare` +
  `search_eval::eval_event_set_compare` (the rule), the hermetic
  `an_event_set_comparison_matches_any_event_not_the_first` /
  `an_event_set_negation_is_all_match_and_an_empty_set_satisfies_it`
  (the fixture table above, span for span), and the live
  `event_and_link_comparisons_match_any_event_over_real_clickhouse`,
  which runs the co-load against a real ClickHouse — the hermetic tests
  cannot execute the per-batch value read the semantics depend on.

- **Accept-surface effect:** the six probes move `reject → accept` on
  the wire axis. The reference's own verdict is unchanged (it always
  accepted them), so the matrix's ORACLE column is untouched; only what
  we return differs, and only for multi-event spans.
