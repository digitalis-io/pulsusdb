# LogQL conformance foundation — provenance

## What this is

The clean-room conformance foundation for the `pulsus-logql` parser
(issue #191, M8-LQ0). It enumerates the entire *documented* LogQL surface
for the pinned language target and gives every construct exactly one
machine-checked disposition, so coverage gaps surface as RED tests rather
than silent skips.

Files:

- `registry-logql-v3.7.3.json` — the construct registry. One entry per
  documented construct: `id`, `category`, `syntax`, `doc` (public-docs
  URL with a real page path + anchor), `probe` (a canonical clean-room
  example query).
- `registry-manifest.json` — the integrity pin: SHA-256 of the registry
  file bytes, the pinned `language`/`target`, the total `construct_count`,
  and per-`category` counts. Any edit to the registry must be deliberate
  and re-pin this file.
- `dispositions.json` — one entry per registry construct (bijection
  enforced) recording its `status`, the black-box `oracle` verdict, and the
  `interim_count_pin`.
- `coverage-map.json` — the e2e slice: one `e2e-differential` source per
  observed case in `test/fixtures/logs/differential.json`, each mapping to
  the registry ids that case's query exercises (AST-presence gated by
  `conformance.rs::e2e_cases_exercise_their_mapped_constructs`).
- `seed-ledger.json` — the generic-failure ledger for the observed e2e
  cases (monotone shrink; empty at LQ0).
- `conformance.rs` (in `tests/`) — the hermetic harness.
- `logql_differential.rs` (in `tests/`) — the env-gated black-box leg.

## Oracle & language pin

The language target is **LogQL v3.7.3**. "Pinning v3.7.3" here means a
documented *version reference for the language*, **not** pinning, fetching,
adapting, or vendoring any reference-implementation source.

The oracle is:

1. **The published LogQL language documentation** rooted at
   `https://grafana.com/docs/loki/` — cited per registry entry in the `doc`
   field (the harness asserts every citation lives under that root and
   carries a real page-path segment plus a non-empty `#`-anchor). Citations
   target the documentation as retrieved on **2026-07-24**; the URL is
   authoritative and section anchors are best-effort against the headings
   live on that date.
2. **Observed query behaviour** — the real emitted query strings in the
   committed e2e differential corpus (`test/fixtures/logs/differential.json`,
   treated like observed HTTP requests), plus black-box replay against an
   unmodified, digest-pinned reference container (the env-gated
   `tests/logql_differential.rs` differential leg, which observes HTTP
   accept/reject only and copies no source).

The digest-pinned reference container image used by the differential leg
and the CI `schema-it` job is the functional coordinate
`docker.io/grafana/loki@sha256:70b9f699fc9bb868b62f1cfd4f787dfa50242f1fd92e6089787d5d7daea75fe8`
(tag `3.7.3`). It is used purely at runtime as a black-box syntax oracle;
no source from it is read, copied, or vendored.

Every conformance case and every expected result is authored by us from the
published documentation and observed behaviour.

**Docs-vs-binary conflict policy:** where the published docs disagree with
observed v3.7.3 behaviour, observed behaviour wins; the divergence is
recorded here and escalated to the owner if material. Two such observed
verdicts are already recorded as `oracle: reject` (both-reject agreements):
`stage.ip` (no standalone `ip` pipeline stage exists) and `stage.distinct`
(no `distinct` pipeline stage exists in v3.7.3, in any argument form).

## Clean-room / licensing statement (grep-checkable)

No upstream reference-implementation source, grammar file, lexer, AST, Go
enum, error string, parser table, or test corpus — and no wire-protocol,
`.proto`, or generated result-model code — is copied, fetched, adapted, or
vendored into this repository, regardless of upstream license. There is no
build-time fetch or cache step for any such material. The `pulsus-logql`
lexer, parser, and error messages are independently authored from the
published LogQL documentation plus observed query behaviour. The construct
registry and its probes are clean-room authored the same way; the observed
seed is composed of real emitted query strings (observed inputs), never
lifted from any upstream test file.

**License split (why status-only is sufficient at LQ0):** the reference
query-language engine (grammar/parser/AST/eval) is copyleft — it is used
clean-room ONLY, and the differential observes HTTP accept/reject status
only, so it needs neither to read that engine nor to reference the
permissively-licensed wire-protocol / HTTP-API / result-model packages. A
later LQ issue adding a body-level (result-value) differential is the only
point at which those permissive shapes could be referenced (with
attribution, referenced not committed); LQ0 does not.

## Disposition statuses

Every construct has exactly one `status`:

- `supported` — the probe parses `Ok` today (and the reference accepts it).
- `interim-named` — the probe yields `LogQlError::NotYetSupported` whose
  `construct` equals `error_construct` and whose `Display` names it (never a
  bare generic error for a real construct). Requires an `owning_issue` in the
  harness `VALID_ISSUES` allowlist.
- `interim-generic` — the probe yields a non-`NotYetSupported` error today
  (a documented construct the parser can only reject generically). Requires
  an `owning_issue`. When the owning issue lands and the construct starts
  parsing or names a boundary, this probe turns RED, forcing the disposition
  to be flipped deliberately.
- `divergence` — an owner-escalated, intentional deviation. Requires a
  non-empty `justification`, an `oracle_citation` (a
  `https://grafana.com/docs/loki/` URL for the expected behaviour), an
  `owner_escalation` (a `https://github.com/digitalis-io/pulsusdb/`
  adjudication URL), and an `owning_issue`. **Pinned to zero at LQ0.**

`interim_count_pin` pins the exact count of interim (named + generic)
dispositions. Every LQ-1..n PR lowers it deliberately; LQ-closeout drives it
to 0 and flips the pin into a strict gate.

## The `oracle` field and the differential (no ad-hoc exemptions)

Every disposition records `oracle` ∈ {`accept`, `reject`}: the measured
black-box verdict of the v3.7.3 reference for the construct's probe (2xx =
accept, HTTP 400 = reject; any other status is inconclusive and fails the
leg). It makes the differential **disposition-driven** —
`tests/logql_differential.rs` replays each registry probe and asserts the
live oracle still returns the recorded verdict, so there is no separate
allowlist that could silently suppress a gap. Each construct is exactly one
of:

- **agreement** — `supported` ∧ `oracle=accept` (both accept), or interim ∧
  `oracle=reject` (both reject the probe).
- **tracked interim gap** — interim ∧ `oracle=accept`: a real compatibility
  gap the reference supports and we do not yet. It is visible in the
  registry (with its public-doc citation) and carries an owning issue — a gap
  is surfaced and tracked, never allowlisted away.
- **unescalated divergence** — `supported` ∧ `oracle=reject` (we more
  permissive than the oracle): disallowed at LQ0; the categories test fails
  if one appears. A genuine, owner-ruled divergence goes through the
  `divergence` disposition status instead (pinned to 0 at LQ0).

`differential_categories_are_pinned` pins the exact category counts
(supported / tracked-interim / both-reject agreement) and asserts zero
`supported ∧ reject`, so a status or oracle flip must be re-pinned
deliberately.

## Revision workflow

1. **Add / change a construct:** edit `registry-logql-v3.7.3.json`, then
   re-pin `registry-manifest.json` (SHA-256 over the exact registry bytes +
   the new counts). Add the matching `dispositions.json` entry (the bijection
   test fails otherwise).
2. **Flip a disposition** (an LQ-1..n landing): change the `status`, and lower
   `interim_count_pin` by the number flipped off interim. The
   probe-vs-status test proves the flip is real; the differential proves the
   flipped `supported` claim agrees with the oracle.
3. **Record a divergence:** only with an owner ruling; fill all four required
   fields; bump the `divergence_count_is_zero_at_lq0` pin when that guard is
   relaxed by a future task.
4. **Shrink the ledger:** when an owning issue lands, an e2e case stops
   failing generically and its ledger entry turns stale (RED); drop it. The
   ledger only ever shrinks.
