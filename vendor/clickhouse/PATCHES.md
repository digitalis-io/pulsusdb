# Patches applied to `clickhouse 0.15.1`

This is a patched, vendored copy of [`clickhouse`
0.15.1](https://github.com/ClickHouse/clickhouse-rs), wired into the workspace
via `[patch.crates-io]` (root `Cargo.toml`) so every `clickhouse::...` import
path is unchanged. See
[`docs/decisions/0007-clickhouse-vendor-patch.md`](../../docs/decisions/0007-clickhouse-vendor-patch.md)
for the decision this copy implements, and issue #382 for the measurements.

**One patch, in one function.** Nothing else is modified. The vendored tree
drops upstream's `examples/`, `tests/`, `benches/`, CI and toolchain files and
their `[[example]]`/`[[test]]`/`[[bench]]` target declarations; `src/`,
`Cargo.toml`'s dependency and feature sets, `Cargo.lock`, `README.md`,
`CHANGELOG.md` and both licences are upstream's.

**Re-vendor rule:** on any `clickhouse` version bump, check whether upstream
has started surfacing the exception code (a numeric field on
`Error::BadResponse`, or any accessor for `X-ClickHouse-Exception-Code` on the
error path). If it has, drop this patch and read the typed value instead. The
gate that proves the patch is still doing its job is
`pulsus-clickhouse`'s live test
`a_result_limit_tripped_after_output_has_been_written_carries_its_code`
**run against ClickHouse 26.3 or newer** — on 24.8 it passes either way.

## 1. `collect_bad_response` keeps the header-derived exception code

`src/response.rs`, in `collect_bad_response` (upstream `:127-163`).

### What upstream does

`collect_response` reads `X-ClickHouse-Exception-Code` (`:85`) and hands it to
`collect_bad_response` as `Option<String>` holding `"Code: {n}"` (`:111-113`).
That value is then used **only** by the `reason()` fallbacks — body could not
be collected (`:142`), body empty (`:145`), body not UTF-8 (`:161`). When the
body *does* decode, `:158-163` returns the body and the header code is
discarded. `Error` has 20 variants and none carries a numeric code, so
`BadResponse(String)` is the only channel a caller has.

### Why that is a defect for us

When a query has already written output, ClickHouse emits the exception into
the same response stream, so the decoded body is
`<already-written result bytes><exception>` and the code is not at byte 0.
Measured through `pulsus-clickhouse`'s client against ClickHouse 26.3.17.110,
the code sat anywhere from byte 10 to byte 1,262,489.

Recovering it from that text is not possible soundly, and both halves of the
body are why:

- **The result bytes are tenant data.** A stored log line or span name can
  contain a whole forged `Code: 210. DB::Exception: …`, so a
  first-occurrence rule returns the forgery.
- **The exception description echoes the failing SQL**, and `pulsus-read`
  renders tenant regexes into `match()` predicates
  (`crates/pulsus-read/src/metrics/series_where.rs`,
  `crates/pulsus-read/src/logql/plan.rs`). So a last-occurrence rule returns
  the forgery too. Measured: a tenant literal in `match()` plus a late
  `intDiv` failure gave a real code of 153 and a parsed code of 210 — and 210
  is in `pulsus-clickhouse`'s `RETRYABLE_SERVER_CODES` while 153 is not.

The user-visible consequence was a 500 `internal` where `docs/api.md`'s
contract requires a 422 `query_too_broad`, and a retry decision that stored
data could steer.

### The change

When the body decodes and the header supplied a code, emit that code first and
the decoded body after it:

```text
Code: 396\n<already-written result bytes>Code: 396. DB::Exception: …
```

The prefix is added **only** when the body does not already start with the same
`Code: N`, so every response whose exception was not preceded by output — the
overwhelmingly common case, and every response taking a `reason()` fallback —
is byte-identical to upstream. The decoded body is kept whole, so callers still
read the description (`pulsus-read`'s 427 handling parses
`cannot compile re2: …` out of it) and operators still see the full message.

Additive, no public API change, no `Error` variant added or altered, no
semver impact. The transport, compression, decoding and pooling paths are
untouched.

### What this does NOT fix

**ClickHouse 24.8 remains forgeable on its streaming path, and no patch can
fix it — here or upstream.** When output has already reached the socket the
response is HTTP 200 with no exception-code header, and upstream anchors the
message with `rfind(b"Code:")` (`extract_exception_old`, `src/response.rs`
`:368-377`), so a tenant literal echoed into the description becomes byte 0 of
the message. Measured on 24.8.14.39: real code 153, delivered message begins
`Code: 210. DB::Exception: forged…`. There is no header to fall back on and
the exception boundary is genuinely unrecoverable from the text.

That is **issue #412**. It is live on `main`, it predates #382, and it closes
with the move to ClickHouse 26.3 (**#376**), whose streaming path frames the
exception with a length the client trusts (`extract_exception_new`,
`:382-419`) rather than searching for it.

**So this patch alone does not make the read path sound; it is sound only
together with #376.** Do not read the vendored fix as complete.
