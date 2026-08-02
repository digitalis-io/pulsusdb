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

### `traceql-validate-nil-spelling-conflation` (issue #328)

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
