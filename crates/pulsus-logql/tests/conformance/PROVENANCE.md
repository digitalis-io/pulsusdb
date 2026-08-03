# LogQL conformance foundation — provenance

## What this is

The clean-room conformance foundation for the `pulsus-logql` parser
(issue #191, M8-LQ0). It enumerates the entire *documented* LogQL surface
for the pinned language target and gives every construct exactly one
machine-checked disposition, so coverage gaps surface as RED tests rather
than silent skips.

Files:

- `registry-logql-v3.7.4.json` — the construct registry. One entry per
  documented construct: `id`, `category`, `syntax`, `doc` (public-docs
  URL with a real page path + anchor), `probe` (a canonical clean-room
  example query), and — for constructs that are instant-only in the
  reference — an optional `endpoint: instant` marker (issue #221:
  `approx_topk` returns 500 on `query_range` in every configuration, so
  only the `/loki/api/v1/query` alias yields a conclusive 2xx/400 syntax
  verdict; the differential-leg oracle container must also enable it via
  `limits_config.shard_aggregations` + `frontend.encoding: protobuf`,
  see `ci/logql/config.yaml`). `func.variants` (issue #221) needs a
  config delta too — `limits_config.enable_multi_variant_queries: true`
  in `ci/logql/config.yaml` — but NO `endpoint` marker: with the flag the
  probe returns 2xx at the default `query_range` endpoint (verified on
  the pinned oracle image; without the flag every variants probe is a
  400 `multi variant queries are disabled for this instance`, a false
  REJECT verdict).
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

The language target is **LogQL v3.7.4**, and the differential container in
this directory is digest-pinned to the same version. "Pinning a version"
here means a documented *version reference for the language*, **not**
pinning, fetching, adapting, or vendoring any reference-implementation
source.

> **Four things carry the version and they move together.** The container
> digest in `.github/workflows/ci.yml`, `registry-logql-v3.7.4.json`'s
> `target` field, `registry-manifest.json`'s `target`, and
> `conformance.rs`'s `EXPECTED_TARGET` — the harness cross-checks them, so
> correcting one alone reddens rather than drifts. They were briefly out
> of step with the language target while the bump was scheduled; that gap
> is closed.
>
> **Why the digest is the capture image's.** It is the same image
> `crates/pulsus-read/tests/logqltest/` captures against
> (`sha256:87f0a067…`, recorded in that directory's PROVENANCE), so the
> status oracle and the value corpus now speak for one version. Issue #339
> is the reason this matters: an adjudication was made there from a probe
> against the older oracle and concluded PulsusDB was over-accepting on
> byte-size literals, which it was not. A version disagreement is visible
> only from a probe **and** a capture together, so a probe against an
> oracle that lags the capture corpus can look authoritative and be wrong.
> Where an oracle probe and a capture disagree, **the capture wins**.

### What the oracle actually compares — status vs values

A construct probe checks **acceptance only**. Two reference versions can
agree on every accept/reject verdict and still return different *values*,
and no registry probe would see it. Stated plainly, because it bounds what
an oracle bump can be said to have verified:

| leg | container | compares | breadth |
|---|---|---|---|
| `logql_differential.rs` | live | **HTTP status only** (2xx/400) | all 103 registry constructs |
| `case_folding.rs` live leg | live | **HTTP status only** | the keyword-folding table |
| `logql_json_key_sanitization.rs` | live | **values** (derived label names) | 51 probes, `\| json` key derivation only |
| `logqltest` corpus (`logqltest_corpus.rs`) | none — replays a committed capture | **values and error strings** | 911 `eval`/`eval_fail` directives across 31 batch files |
| `conformance.rs` | none | registry integrity, no reference at all | 103 constructs |

So **live value comparison is thin**: one leg, one construct family. Broad
value coverage exists, but hermetically, against a frozen capture rather
than against the running container — it proves we still match what the
reference returned *when the corpus was captured*, not what it returns now.
There is also no mechanical construct→captured-value map: the corpus is
organised by feature batch, not by registry id, so "which constructs have
value coverage" cannot be answered by a query today.

**Consequence for a version bump.** Moving the oracle re-verifies the
status surface completely and the value surface barely. That is acceptable
for the v3.7.3→v3.7.4 move specifically, because the corpus was already
captured at v3.7.4 — the bump removed a version mismatch rather than
introducing one, and the one live value leg (whose regeneration gate
already demanded v3.7.4) now runs against the image it was captured from.
It would **not** be acceptable for a bump to a version the corpus has not
been captured against: that needs a corpus regeneration, not just a digest
change.

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
`docker.io/grafana/loki@sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`
(tag `3.7.4`; it reports `"version":"3.7.4"` from
`/loki/api/v1/status/buildinfo`, and is the same image
`crates/pulsus-read/tests/logqltest/` captures its values against). It is
used purely at runtime as a black-box syntax oracle; no source from it is
read, copied, or vendored.

Every conformance case and every expected result is authored by us from the
published documentation and observed behaviour.

**Docs-vs-binary conflict policy:** where the published docs disagree with
observed v3.7.4 behaviour, observed behaviour wins; the divergence is
recorded here and escalated to the owner if material. Two such observed
verdicts are recorded as `oracle: reject` and dispositioned `reject-parity`
(#203 closeout): `stage.ip` (no standalone `ip` pipeline stage exists) and
`stage.distinct` (no `distinct` pipeline stage exists in v3.7.4, in any
argument form). We reject both with a construct-named `NotYetSupported` and so
does the reference — a terminal parity, not a tracked gap.

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
- `reject-parity` — the probe yields `LogQlError::NotYetSupported` naming its
  `error_construct` (like `interim-named`) AND the pinned reference *also*
  rejects it (`oracle: reject`): parity, not a compatibility gap. It is a
  terminal state — **no `owning_issue`** and not counted by
  `interim_count_pin`. The residual `stage.distinct`/`stage.ip` constructs
  (no such pipeline stage exists in v3.7.4) are the two members. The live
  differential separately confirms the reference still rejects; a flip to
  Accept (either side) is RED.
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
black-box verdict of the v3.7.4 reference for the construct's probe (2xx =
accept, HTTP 400 = reject; any other status is inconclusive and fails the
leg). It makes the differential **disposition-driven** —
`tests/logql_differential.rs` replays each registry probe and asserts the
live oracle still returns the recorded verdict, so there is no separate
allowlist that could silently suppress a gap. Each construct is exactly one
of:

- **agreement** — `supported` ∧ `oracle=accept` (both accept), or
  `reject-parity` ∧ `oracle=reject` (both reject the probe).
- **tracked interim gap** — interim ∧ `oracle=accept`: a real compatibility
  gap the reference supports and we do not yet. It is visible in the
  registry (with its public-doc citation) and carries an owning issue — a gap
  is surfaced and tracked, never allowlisted away. Empty after the #203
  closeout (`interim_count_pin == 0`).
- **unescalated divergence** — `supported` ∧ `oracle=reject` (we more
  permissive than the oracle), or `reject-parity` ∧ `oracle=accept` (we
  reject and the reference does not): both disallowed; the categories test
  fails if one appears. A genuine, owner-ruled divergence goes through the
  `divergence` disposition status instead (pinned to 0 at LQ0).

`differential_categories_are_pinned` pins the exact category counts
(supported / tracked-interim / reject-parity agreement) and asserts zero
`supported ∧ reject` and zero `reject-parity ∧ accept`, so a status or oracle
flip must be re-pinned deliberately.

## `grouping.range_agg` — what `supported` does and does not claim (issue #344)

`status: supported` in this file means exactly what `check_status` checks:
**the probe parses**. For `grouping.range_agg` — a `by`/`without` clause on
a range aggregation — that is the whole truth of the language surface and
nothing more: the grammar accepts it on the eight ops the reference admits
it on, and `grouping.range_agg_disallowed` pins the reject-parity twin for
the seven it refuses by name. **Execution is not implemented**: the planner
refuses a range-aggregation grouping with a named error, so an end-to-end
query still fails. Recorded here rather than left to be inferred from the
word "supported".

Both rows were captured against **grafana/loki v3.7.4**. When they landed,
this directory's registry filename still said `v3.7.3` and the note here
recorded the mismatch as pending on a concurrent re-pin branch; that branch
has since merged, so the registry, the manifest, `EXPECTED_TARGET` and the
container digest all name v3.7.4 and there is no mismatch left to flag. The
container's identity was read from the running process —
`/loki/api/v1/status/buildinfo` answered
`{"version":"3.7.4","revision":"b318f282"}` — rather than trusted from any
committed header; that is the same `b318f282` the pinned digest resolves
to, so these rows and the differential oracle are one image. The captured semantics and the full
accept/reject split live in
`crates/pulsus-read/tests/logqltest/corpus/b18_range_agg_grouping.test`.

## Revision workflow

1. **Add / change a construct:** edit `registry-logql-v3.7.4.json`, then
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
