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
