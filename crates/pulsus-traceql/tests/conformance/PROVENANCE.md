# TraceQL conformance foundation — provenance

## What this is

The clean-room conformance foundation for the `pulsus-traceql` parser
(issue #180, M7-TQ1). It enumerates the entire *documented* TraceQL surface
for the pinned language target and gives every construct exactly one
machine-checked disposition, so coverage gaps surface as RED tests rather
than silent skips.

Files:

- `registry-traceql-v3.0.2.json` — the construct registry. One entry per
  documented construct: `id`, `category`, `syntax`, `doc` (public-docs
  URL), `probe` (a canonical clean-room example query).
- `registry-manifest.json` — the integrity pin: SHA-256 of the registry
  file bytes, the total `construct_count`, and per-`category` counts. Any
  edit to the registry must be deliberate and re-pin this file.
- `dispositions.json` — one entry per registry construct (bijection
  enforced) recording its `status`, the black-box `tempo` verdict, and the
  `interim_count_pin`.
- `replay-ledger.json` — the generic-failure ledger for observed
  `grafana/` corpus queries (monotone shrink).
- `conformance.rs` (in `tests/`) — the harness.

## Oracle & language pin

The language target is **TraceQL as shipped in Tempo v3.0.2**. "Pinning
Tempo v3.0.2" here means a documented *version reference for the language*,
**not** pinning, fetching, adapting, or vendoring any Tempo source.

The oracle is:

1. **The published TraceQL language documentation** at
   `https://grafana.com/docs/tempo/` — cited per registry entry in the
   `doc` field (the harness asserts every citation begins with that
   prefix). Citations target the documentation as retrieved on
   **2026-07-23**; the URL is authoritative and section anchors are
   best-effort against the headings live on that date.
2. **Observed query behavior** — real Grafana-emitted query strings (the
   `grafana/` corpus class, treated like observed HTTP requests), plus
   black-box replay against an unmodified `grafana/tempo:v3.0.2` container
   (the env-gated `tests/tempo_differential.rs` differential leg, which
   observes HTTP accept/reject only and copies no source).

Every conformance case and every expected result is authored by us from
the spec and observed behavior.

**Docs-vs-binary conflict policy:** where the published docs disagree with
observed v3.0.2 behavior, observed behavior wins; the divergence is
recorded here and escalated to the owner if material.

## Clean-room / licensing statement (grep-checkable)

No Tempo, Grafana, or Loki source, grammar file, lexer, AST, Go enum, error
string, parser table, or test corpus — and no `pkg/tempopb`, `.proto`, or
generated wire code — is copied, fetched, adapted, or vendored into this
repository, regardless of upstream license. There is no build-time fetch or
cache step for any such material. The `pulsus-traceql` lexer, parser, and
error messages are independently authored from the published TraceQL
documentation plus observed query behavior. The construct registry and its
probes are clean-room authored the same way; the `grafana/` seed corpus is
composed of real emitted query strings (observed inputs), never lifted from
any upstream test file.

## Disposition statuses

Every construct has exactly one `status`:

- `supported` — the probe parses `Ok` today. (Optional `evidence` points at
  `accept/` corpus cases that must parse.)
- `interim-named` — the probe yields `TraceQlError::NotYetSupported` whose
  `construct` equals `error_construct` and whose `Display` names it (never a
  bare generic error for a real construct). Requires an `owning_issue` in
  {181,182,183,184} and `evidence` pointing at the `unsupported/` boundary
  case.
- `interim-generic` — the probe yields a non-`NotYetSupported` error today
  (an unregistered construct the parser can only reject generically).
  Requires an `owning_issue`. When the owning issue lands and the construct
  starts parsing or names a boundary, this probe turns RED, forcing the
  disposition to be flipped deliberately.
- `divergence` — an owner-escalated, intentional deviation from Tempo
  parity. Requires a non-empty `justification`, an `oracle_citation`
  (a `https://grafana.com/docs/tempo/` URL for the expected behavior), an
  `owner_escalation` (a `https://github.com/digitalis-io/pulsusdb/`
  adjudication URL), and an `owning_issue`. **Pinned to zero at T1.**

`interim_count_pin` pins the exact count of interim (named + generic)
dispositions. Every T2–T5 PR lowers it deliberately; #185 drives it to 0
and flips the pin into a strict gate.

## The `tempo` field and the differential (no ad-hoc exemptions)

Every disposition records `tempo` ∈ {`accept`, `reject`}: the measured
black-box verdict of Tempo v3.0.2 for the construct's probe (2xx = accept,
HTTP 400 = reject; any other status is inconclusive and fails the leg). It
makes the differential **disposition-driven** — `tests/tempo_differential.rs`
replays each registry probe and asserts the live oracle still returns the
recorded verdict, so there is no separate allowlist that could silently
suppress a gap. Each construct is exactly one of:

- **agreement** — `supported` ∧ `tempo=accept` (we and Tempo both accept),
  or interim ∧ `tempo=reject` (we and Tempo both reject the probe).
- **tracked interim gap** — interim ∧ `tempo=accept`: a real compatibility
  gap Tempo supports and we do not yet. It is visible in the registry (with
  its public-doc citation) and carries an owning sub-issue (#181–#184) — a
  gap is surfaced and tracked, never allowlisted away. Example:
  `comparison.rhs_attribute` (`{ .a = .b }`, RHS attribute-vs-attribute
  comparison), owned by #183 (operator/field-expression-shaped per the #179
  routing).
- **unescalated divergence** — `supported` ∧ `tempo=reject` (we more
  permissive than the oracle): disallowed at T1; the categories test fails
  if one appears. A genuine, owner-ruled divergence goes through the
  `divergence` disposition status instead (pinned to 0 at T1).

`differential_categories_are_pinned` pins the exact category counts
(supported / tracked-interim / both-reject agreement), so a status or oracle
flip must be re-pinned deliberately.

Note on scope: PulsusDB is deliberately a *stricter* subset in a few areas
verified differentially at **T8** (the in-house duration grammar, nesting
depth caps, byte-escape handling — all recorded in
`tests/corpus/PROVENANCE.md`). Those are properties of malformed *inputs*,
not registry constructs, so the construct-level differential above does not
replay the `reject/` corpus and does not need to exempt them.

## Revision workflow

1. **Add / change a construct:** edit `registry-traceql-v3.0.2.json`, then
   re-pin `registry-manifest.json` (SHA-256 over the exact registry bytes +
   the new counts). Add the matching `dispositions.json` entry (the
   bijection test fails otherwise).
2. **Flip a disposition** (a T2–T5 landing): change the `status`, update
   `evidence`, and lower `interim_count_pin` by the number flipped off
   interim. The probe-vs-status test proves the flip is real.
3. **Record a divergence:** only with an owner ruling; fill all four
   required fields; bump the `divergence_count_is_zero_at_t1` pin when that
   guard is relaxed by a future task.
4. **Shrink the ledger:** when an owning issue lands, a `grafana/` case
   stops failing generically and its ledger entry turns stale (RED); drop
   it. The ledger only ever shrinks.
