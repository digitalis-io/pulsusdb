# TraceQL golden corpus — provenance

## What this is

The byte-frozen semantic gate for the `pulsus-traceql` parser (M4 search
subset). Three case classes:

- `accept/<case>.traceql` — a query the M4 grammar must parse; its
  `.golden` sibling pins the exact `{:#?}` Debug rendering of the parsed
  `Query` AST.
- `reject/<case>.traceql` — a malformed query; its `.golden` pins the
  exact `{:#?}` rendering of the `TraceQlError` (variant + byte spans).
- `unsupported/<case>.traceql` — a recognized-but-out-of-M4 construct
  (the scope boundary); its `.golden` pins the `NotYetSupported` error.
  These cases map one-to-one onto the frozen registry
  `pulsus_traceql::BOUNDARY_CONSTRUCTS` — both directions are asserted
  mechanically by `tests/corpus.rs`, so scope drift either way fails CI.
- `validate_reject/<case>.traceql` — a query the grammar PARSES and the
  semantic pass then rejects (issue #335 Stage B). Its `.golden` pins the
  parsed AST, because the parse shape is the fact worth freezing; the
  rejection itself is pinned by `tests/corpus.rs`, which asserts
  `validate` returns an error. This class exists so `accept/` can keep
  meaning "a query PulsusDB SERVES" — `tests/validate_corpus.rs` asserts
  every `accept/` case validates `Ok`, and widening that to "merely
  parses" would have retired a real gate to make room for three cases.
- `grafana/<case>.traceql` — a real, observed Grafana-emitted TraceQL
  query (issue #180), captured like an observed HTTP request. Its `.golden`
  pins today's outcome, whatever it is — `Ok`, a named `NotYetSupported`
  boundary, or (today) a generic error at an unregistered construct. The
  outcome class is intentionally *unconstrained* by `tests/corpus.rs` (the
  golden-match still applies); the class invariant for these cases —
  generic failures must be recorded in `tests/conformance/replay-ledger.json`
  and that ledger only ever shrinks — lives in `tests/conformance.rs`,
  keeping `corpus.rs` std-only. These queries are observed *inputs*, never
  lifted from any upstream test file (see `tests/conformance/PROVENANCE.md`
  for the clean-room statement).

### `grafana/` capture provenance

Each `grafana/` case is captured from a real Grafana request; record its
provenance in this file when adding one:

- `explore_root_rate_by_service` — Grafana Explore Traces "Rate by service"
  root-span panel; the `nestedSetParent<0` root-filter idiom Grafana emits
  for root-only rate series, grouped `by(resource.service.name)`.
- `explore_root_rate_sample` — the same root-rate idiom with the
  `with(sample=true)` query hint Grafana appends when sampling is enabled.

Both hit `nestedSetParent` (a nested-set intrinsic, #181) before any named
boundary, so today they can only surface a generic parser error; both are
ledgered in `replay-ledger.json` with `owning_issues` {181, 182}.

## Issue #335 Stage B regeneration (2026-08-03)

The grammar collapse changed `FieldExpr`'s shape, so every `.golden` is a
new byte string. A single count would hide what matters, so each changed
case is classified by CAUSE. Method: dump `parse(q)`'s `Display` (or its
error message) for all 170 cases before and after the collapse and diff —
a case whose rendering is byte-identical had no behavioural change, only a
`Debug` shape change. **157 of 170 renderings are identical.** The
remaining 13, in full:

**A. Nil spellings now render faithfully (3).** `accept/existence_eq_nil`,
`accept/existence_neq_nil`, `grafana/explore_root_rate_by_service`. The
old AST folded `= nil`/`!= nil` into the truthiness node, so `{ .a = nil }`
rendered as `{ !(.a) }`, `{ .a != nil }` as `{ .a }`, and the Grafana query
lost its `!= nil` entirely on the way out. `Exists { field, negated }`
keeps the polarity, so all three now render what was written. This is the
same defect the retired ledger row `traceql-validate-nil-spelling-conflation`
described, seen from the rendering side.

**B. (was: arithmetic gained parens — FIXED, no longer a class.)** The
collapse briefly rendered `{ .a = 2 - 1 }` as `{ .a = (2 - 1) }`, changing
six goldens for nothing. `fmt_comparison_operand` restores the
pre-collapse behaviour: comparison binds looser than every arithmetic
operator, so an operand's outermost level needs no wrapping parens to
reparse. Exactly one level is stripped, as `fmt_operand_bare` did. Kept as
a numbered entry because the six goldens are unchanged and a future reader
comparing case counts should see why.

**C. Rejection moved parse → validate; case reclassified `accept/` (5).**
`bare_intrinsic_word` (`{ name }`), `chained_comparison` (`{ .a = 1 = 2 }`),
`duration_signed_minus` (`{ duration > -2s }`), `duration_unitless`
(`{ duration > 100 }`), `nested_set_regex_string`
(`{ nestedSetLeft =~ "x" }`). All five now parse, which is what `accept/`
asserts. Their wire verdicts, re-measured against the pinned digest:
`{ duration > -2s }` and `{ duration > 100 }` are reference **200s** —
those two were divergences of ours that the collapse CLOSED. The other
three are reference 400s and our validator rejects them, covered by
`conformance/validate-vectors.json` (`bool-r1` and the `type-mismatch`
rows) rather than here.

**D. Parse error MESSAGE changed, still a parse error (5).**
`reject/attr_bare_keyword_value`, `reject/kind_invalid_keyword`,
`reject/status_invalid_keyword`, `reject/missing_value`,
`reject/duration_signed_plus`. Each said "expected a status" / "expected a
duration literal" and now says "expected a field, a literal, or `(`". This
is the **stated price of the context-free atom grammar**: the old
`parse_value(cursor, &field)` typed its operand from the LHS, so it could
name the expected type; an atom parser has no left context by
construction. The reference is no more specific here — it answers
`unknown identifier: bogus` — and every one of the five remains a 400 on
both sides.

Class C's five cases split by their WIRE verdict, which is why they did
not all land in the same place: `duration_signed_minus` and
`duration_unitless` are reference 200s and validate `Ok`, so they are
`accept/`; `bare_intrinsic_word`, `chained_comparison` and
`nested_set_regex_string` are reference 400s that our validator also
rejects, so they are `validate_reject/`. Sorting all five into `accept/`
on the strength of "they parse now" is the mistake this split corrects —
it broke `validate_corpus.rs`'s accept-case invariant, which is how it
was caught.

`MANIFEST` was also re-sorted. It documents `LC_ALL=C sort` order and was
not in it (the `accept/arith_*` block sat after `accept/attr_*`); the
regeneration wrote byte order, which is that rule. No entry was added or
lost — 170 before, 170 after, no duplicates either side.

`MANIFEST` is the declared newline list of every `<class>/<stem>`;
`tests/corpus.rs` compares it against `read_dir` output before any case
runs, so an orphan file, an unlisted case, or a missing `.golden`
sibling fails loudly.

## File format

- `.traceql` files hold the query plus a single trailing newline (POSIX
  text files); the harness strips exactly one trailing `\n` — queries
  themselves never end in a newline. `reject/empty.traceql` is therefore
  a file containing only `\n` (the empty query).
- `.golden` files hold the pretty Debug output plus a trailing newline.

## What the vectors are derived from

The committed M4 surface, not any external parser:

- docs/features.md §4 (M4 TraceQL coverage line) — selectors,
  intrinsics, operators, aggregate filters, `select()`.
- docs/schemas.md §4.2 — the worked example
  (`accept/field_and_worked_example`).
- docs/api.md §4.2 — the normative in-house duration-literal grammar
  (unsigned decimal, single unit from `ns/us/µs/ms/s/m/h`, no sign, no
  compound, exact whole-nanosecond fractional conversion). Conformance
  against real Tempo behavior is verified differentially at T8's e2e
  gate, not here.
- The #56 architect plan (v3, as amended) — the scope-boundary registry
  and the required accept/reject vector lists.
- Double-quoted strings use the full Go escape grammar (`\a \b \f \n \r
  \t \v \\ \"`, `\xHH`, `\NNN` octal, `\uXXXX`, `\UXXXXXXXX`; unknown or
  malformed escapes are positioned errors; a raw newline in the literal
  is an error, pinned by `reject/string_raw_newline`) with one loud
  divergence, ruled intended by the task-manager (round-2 review) and
  pinned by `reject/string_escape_non_ascii_byte` and
  `reject/string_escape_octal_out_of_range`: byte escapes above `0x7F`
  are rejected rather than decoded, **including sequences that would
  compose into valid UTF-8 in Go — canonically `"\xc3\xa9"`, Go's
  byte-level spelling of `"é"`** — because a Rust `String` cannot hold
  the intermediate lone bytes and a byte-buffer decode path is not
  worth it; use `\uXXXX`. If T8's differential gate against real Tempo
  surfaces such usage, the ruling is revisited (see
  `src/lexer.rs::scan_double_quoted`).
- Boolean-chain limits: `&&`/`||` nodes are charged against a
  query-wide budget of `MAX_DEPTH` (64) shared across the spanset and
  field levels; `reject/field_chain_over_limit` and
  `reject/spanset_chain_over_limit` pin the boundary.
- An attribute path is a single unbroken token: no whitespace on either
  side of any `.` separator, for every scope (`.attr`, `span.`,
  `resource.`, `parent.`, `event.`, `link.`, `instrumentation.`, and each
  `.`-separated key segment). Pinned by `reject/attr_dot_space_after`,
  `reject/attr_dot_space_after_scope` and
  `reject/attr_dot_space_before_scope` (issue #327); enforced once in
  `src/lexer.rs::reject_split_attribute_path`, which carries the observed
  accept/reject vectors. Derived from observed behavior, not docs: the
  black-box oracle (`grafana/tempo:3.0.2` over `/api/search`) answers
  `400` to every gap spelling and `200` to the tight one.
- A colon-scoped intrinsic (`span:id`, `trace:duration`, `event:name`,
  `link:spanID`, `instrumentation:version`) binds its scope keyword to
  the `:` **on the left only** — `{ span :id }` is an error, `{ span: id }`
  is valid. That asymmetry with `.` is genuine reference behavior, not an
  oversight: the same oracle answers `400` to every pre-colon gap and
  `200` to every post-colon gap (spaces, tabs and newlines alike), for
  every colon scope and every operand position. Pinned in BOTH directions
  by `reject/intrinsic_colon_space_before` and
  `accept/intrinsic_colon_space_after` (issue #327); enforced in
  `src/lexer.rs::reject_split_scoped_intrinsic`, whose doc comment says
  the asymmetry must not be "fixed" into consistency.

## Regenerating

Goldens are authored by running the parser once and committing its
output. After an *intentional* AST or error-message change:

```
PULSUS_TRACEQL_REGEN=1 cargo test -p pulsus-traceql --test corpus -- --ignored regenerate_goldens
```

then review the diff and commit the `.golden` changes together with the
parser change. Adding a case = add the `.traceql` file, add its stem to
`MANIFEST` (sorted, `LC_ALL=C sort`), regenerate, review, commit. The
drift, round-trip, token-coverage, and registry-mapping tests are the
freeze — there is no checksum manifest.
