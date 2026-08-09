# ADR 0007: vendor-and-patch `clickhouse` to keep the exception code off the wire text

Status: **Accepted** (2026-08-09)
Issue: [#382](https://github.com/digitalis-io/pulsusdb/issues/382) (a ClickHouse error after the first flush is parsed as code 0)
Related: [ADR 0003](0003-promql-parser-vendor-patch.md) and [ADR 0004](0004-opentelemetry-proto-vendor-patch.md) establish the vendor+patch discipline this ADR reuses; [ADR 0001](0001-clickhouse-client.md) selected the client.
Depends on: [#376](https://github.com/digitalis-io/pulsusdb/issues/376) (ClickHouse LTS move) — see "What this does not decide".
Spawned: [#412](https://github.com/digitalis-io/pulsusdb/issues/412) (the 24.8 streaming path, pre-existing on `main`).

## Context

`ChError::Server { code, .. }` is the single value the whole read path
classifies errors by: `traces::exec::map_trace_read_error` (158/307/396),
`map_trace_metrics_error` (191), `map_trace_generator_error` (241),
`logql::exec::map_read_error` (307), `metrics::dispatch::map_metrics_read_error`
(427), `schema::bookkeeping::is_missing_table` (60/81) and
`ChError::is_retryable`. There is exactly one non-test producer of that value in
the workspace — `ChError::server_from_bad_response` — so its correctness is the
correctness of every one of those.

The `clickhouse` crate does not give us the code as a typed value. It reads
`X-ClickHouse-Exception-Code` off the response (`src/response.rs:85` @ 0.15.1)
and then discards it whenever the body decodes as UTF-8 (`:158-163`), leaving
`Error::BadResponse(String)` — one of 20 variants, none carrying a number — as
the only channel.

That is survivable while the exception is the whole body. It is not survivable
once a query has written output before failing, because ClickHouse then emits
the exception into the same response stream and the body becomes
`<already-written result bytes><exception>`. Measured against ClickHouse
26.3.17.110 through our own client, the code sat between byte 10 and byte
1,262,489.

**No parse of that text is sound**, and this is the finding that decided the
ADR:

- the result bytes are tenant data, so a first-occurrence rule loses to a
  stored log line carrying a forged `Code: N. DB::Exception:`;
- the exception description echoes the failing SQL, and we render tenant
  regexes into `match()` predicates (`metrics/series_where.rs`,
  `logql/plan.rs`), so a last-occurrence rule loses to the description.
  Measured: tenant literal in `match()` + a late `intDiv` failure gave real
  code 153, parsed code 210 — and 210 is retryable while 153 is not.

The user-visible consequence was a 500 `internal` where `docs/api.md` requires
a 422 `query_too_broad`, plus a retry decision stored data could steer.

## Options considered

1. **A cleverer text rule.** Rejected on the measurement above: the server's
   code is the first marker of the *exception segment*, and nothing in the text
   identifies where that segment begins. Two rules were implemented and both
   were defeated by a real body.
2. **A route inside the crate as published.** None exists. Response headers are
   read in three places, all private. The only injection point,
   `Client::with_http_client`, takes a **sealed** trait (`sealed::Sealed` in a
   private module) that is not even nameable outside the crate and is
   implemented only for `hyper_util`'s legacy client, so we can vary the
   connector — a byte stream — and nothing else.
3. **Fork.** The same edit with publishing overhead and no benefit.
4. **Our own HTTP path for reads.** Requires reimplementing ClickHouse's native
   LZ4 block framing, `RowBinaryWithNamesAndTypes` decoding, streaming
   exception detection and pooling — most of the crate, on the hot read path,
   which is the product. Rejected.
5. **Vendor and patch.** Chosen.

## Decision

Vendor `clickhouse 0.15.1` to `vendor/clickhouse` and apply one patch, in one
function: `collect_bad_response` emits the header-derived `Code: N` at byte 0
ahead of the decoded body, and only when the body does not already start with
the same code. `vendor/clickhouse/PATCHES.md` carries the exhaustive change and
the re-vendor rule.

`ChError::server_from_bad_response` then reads byte 0 and nothing else. What it
reads came from a response header that no query result can influence.

Consequences:

- Every response whose exception was not preceded by output is byte-identical
  to upstream, so no currently-correct classification moves.
- The description is preserved after the code, so
  `metrics::dispatch::re2_reject_detail` still finds `cannot compile re2: …`
  and operators still see the whole message.
- No public API change, no `Error` variant added or altered, no semver impact,
  and no change to transport, compression, decoding or pooling.
- We own one more vendored crate at each dependency bump. The re-vendor rule
  says to drop the patch the moment upstream surfaces the code itself.

## What this does not decide

**This patch does not make the read path sound on ClickHouse 24.8**, the
version currently pinned in CI. On its streaming path — output already on the
socket — the response is HTTP 200 with no exception-code header, and the crate
anchors the message with `rfind(b"Code:")` (`extract_exception_old`,
`src/response.rs:368-377` upstream, `:392-401` in `vendor/clickhouse/`), so a
tenant literal echoed into the description
becomes byte 0 of the message we receive. Measured on 24.8.14.39: real code
153, message delivered beginning `Code: 210. DB::Exception: forged…`. There is
no header to fall back on, and the exception boundary is genuinely
unrecoverable from the text, so neither this patch nor an upstream one can fix
it.

That is **#412**. It is live on `main` and predates #382 — the pre-#382
`strip_prefix` read returns the same forged 210. It closes with **#376**, the
move to ClickHouse 26.3, whose streaming path frames the exception with a
length the client trusts (`extract_exception_new`) instead of searching for it.

**Soundness is this ADR *and* #376.** The limitation is pinned by
`pulsus_clickhouse::error`'s
`on_24_8_a_streamed_forgery_reaches_byte_zero_and_is_read_issue_412`, on a
verbatim 24.8 capture, so it fails loudly if a future crate or server version
changes the shape.

## The shortest true summary of this issue

Six times over this issue's review — in the parse rule twice, in the
version-dependence, in a retyped fixture, in a paraphrased one, and in the
stated cause of a reproduction's negative control — an explanation was adopted
that fitted the observed outcome and was not its cause. Every outcome was
right; every mechanism was wrong until it was measured. The last of them was
caught only because the *summary* still carried the superseded reason after the
paragraph above it had been corrected.

The resolution was the same move every time: **stop reasoning about the text
and go and get the authoritative value.** Count the rows the predicate actually
admits. Capture the bytes rather than retyping them. Open the cited line rather
than computing it.

That move is also, exactly, what this ADR does. The exception code was
recoverable from the response text right up until it was not, and every rule
that tried was defeated by a body an attacker could shape. The patch stops
reading the text and takes the value ClickHouse already sent in a header. When
a future reader wonders whether a cleverer parse would have done, the answer is
in the two that were implemented, measured and deleted.
