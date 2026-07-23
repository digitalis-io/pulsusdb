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
