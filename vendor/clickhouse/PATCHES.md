# Patches applied to `clickhouse 0.15.1`

This is a patched, vendored copy of [`clickhouse`
0.15.1](https://github.com/ClickHouse/clickhouse-rs), wired into the workspace
via `[patch.crates-io]` (root `Cargo.toml`) so every `clickhouse::...` import
path is unchanged. See
[`docs/decisions/0007-clickhouse-vendor-patch.md`](../../docs/decisions/0007-clickhouse-vendor-patch.md)
for the decision this copy implements, and issue #382 for the measurements.

**Two patches, in two functions**, both in `src/response.rs`: §1
(`collect_bad_response`, issue #382) and §2 (`extract_exception` and
`DetectDbException`, issue #412). Nothing else is modified. The vendored tree
drops upstream's `examples/`, `tests/`, `benches/`, CI and toolchain files and
their `[[example]]`/`[[test]]`/`[[bench]]` target declarations; `src/`,
`Cargo.toml`'s dependency and feature sets, `Cargo.lock`, `README.md`,
`CHANGELOG.md` and both licences are upstream's. Upstream's two `#[test]`s in
`response.rs` are untouched and still pass.

**Re-vendor rule (§1):** on any `clickhouse` version bump, check whether upstream
has started surfacing the exception code (a numeric field on
`Error::BadResponse`, or any accessor for `X-ClickHouse-Exception-Code` on the
error path). If it has, drop this patch and read the typed value instead. The
gate that proves the patch is still doing its job is
`pulsus-clickhouse`'s live test
`a_result_limit_tripped_after_output_has_been_written_carries_its_code`
**run against ClickHouse 26.3 or newer** — on 24.8 it passed either way. Since
issue #376 moved the supported floor to 26.3 LTS, 26.3 is the only version we
run, so that gate is live in every CI job and on every developer machine
rather than conditional on which server happened to be up.

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

### What this patch does not reach

This patch is the **buffered** path (HTTP 500 with a code header). The
**streaming** path — HTTP 200, output already on the socket, no code header —
is a different function and a different defect; §2 below is its fix. On
ClickHouse 24.8 that path was unfixable by any patch, here or upstream: there
is no header and no trailer, so the exception's boundary is genuinely
unrecoverable from the text. That is why issue #376 moved the supported floor
to 26.3, which frames a streamed exception with a declared length.

## 2. `extract_exception` only searches result bytes when the server declared no exception tag

`src/response.rs`, in `extract_exception` and `DetectDbException` (issue
#412).

### What upstream does

```rust
if let Some(tag) = tag && chunk.ends_with(b"__exception__\r\n") {
    extract_exception_new(chunk, tag)      // slices by declared length
} else if chunk.ends_with(b"))\n") {
    extract_exception_old(chunk)           // rfind(b"Code:") -- forgeable
} else { None }
```

`extract_exception` runs **per chunk**, inside `DetectDbException::poll_next`.
On 26.3 the tag is `Some` for the whole stream, but the length-slicing arm
*additionally* requires the current chunk to end with `__exception__\r\n` —
which only the chunk carrying the trailer does. Every other chunk falls through
to the `))\n` test and, if it matches, into the searching extractor.

### Why that is a defect for us

**Result bytes are tenant data.** A single row ending `))\n` is enough:

| scenario | before | after |
|---|---|---|
| one row, `SELECT concat('Code: 210. DB::Exception: forged (FAKE) (version 26.3.17.110 (official build))', '\n')` | `rows=0`, `Code: 210` | `OK rows=1` |
| trace point read column order (`crates/pulsus-read/src/traces/sql.rs`, `payload` last), one span | `rows=0`, `Code: 210` | `OK rows=1` |
| LogQL row shape, 30 000 rows, mid-block cut (pads 15 and 48 of 64) | `rows=0`, `Code: 210` | `OK rows=30000` |
| **control**: real exception after 2.5 M rows streamed | `Code: 395` | `Code: 395` |

So a query ClickHouse **completed** came back as zero rows and a fabricated
server error. It was not merely a mislabelled failure: `DetectDbException`
returns `Err` for the chunk it was handed, so already-delivered rows are
discarded too. And 210 is in `pulsus-clickhouse`'s `RETRYABLE_SERVER_CODES`,
so the fabricated failure was retried and reached
`PooledConn::report_transport_failure` → `mark_unhealthy`: tenant data could
demote a healthy ClickHouse endpoint out of the pool.

The realistic route is not a contrived fixture. **A log aggregator storing
ClickHouse's own error text** is one of the things people point a log database
at, and a stored ClickHouse error ends `…)` and contains the exact prefix the
old search looks for. On the LogQL row shape the `\n` is not even in the log
line: it is the **RowBinary varint length prefix of the following column**,
which is `0x0A` whenever that column is exactly 10 bytes.

### The change

The tag, not the shape of the current chunk, decides which channel is
believed:

- **Tagged response.** Result bytes are never searched. A tagged exception
  frame is *reassembled*, anchored on its **opening**, and then sliced by its
  server-declared length exactly as upstream does.
- **Untagged response.** Unchanged, byte for byte: `ends_with(b"))\n")` →
  `extract_exception_old`. See "Why the fallback arm stays" below.

The frame's two ends are in **opposite field orders**. Captured from a live
26.3.17.110 HTTP-200 stream (body 15,960,999 bytes, tag `zgnglmkjouifsqby`):

```text
opening   \r\n__exception__\r\nzgnglmkjouifsqby\r\nCode: 395. DB::Exception: …
closing   …0 (official build))\n288 zgnglmkjouifsqby\r\n__exception__\r\n
```

`extract_exception_new`'s `strip_suffix` chain parses the **closing** sequence
only, so it establishes that order and says nothing about the opening. The
anchor targets the opening — `EXC_OPEN ++ tag`, built once per response in
`Chunks::new`, never per chunk.

**The anchor is scanned for at ANY offset**, not just the chunk start.
`starts_with` was the first design and carried the same shape as the defect
being fixed: a check that inspects one position. Measured on the hermetic
mock, a frame opening appended after result data in one chunk and closing in
the next was not recognised by it (4 garbage rows decoded out of the frame
bytes, then `not enough data…`). A `straddle_len` suffix check covers the
anchor being cut across a boundary.

**Withheld bytes — three cases, all deliberate:**

1. Bytes **before** the anchor are emitted as a data chunk, so rows already
   produced still reach the caller. (Upstream's `ends_with` arm discards its
   whole chunk; that asymmetry is upstream's and is left alone.)
2. Bytes **from the anchor onward** are withheld until the frame closes. If
   the stream ends first they are surfaced, not dropped: `Error::BadResponse`
   built from them with the anchor and its `\r\n` stripped, so byte 0 is the
   server's own `Code: N`. A started frame never ends as `Ok`. The tail of a
   partial closing trailer stays attached to the description — lossy there,
   exact in the code.
3. Past `EXC_FRAME_CAP` (16 MiB) the stream fails with `Error::Other` and the
   buffer **is dropped**. That is a memory bound, not data preservation.

### Why the fallback arm stays

Do not delete `extract_exception_old` and do not narrow its search. Measured
on 24.8.14.39 (`tag=None`), a real exception after 2 M rows streamed:

- **gated** (`None => extract_exception_old`): `Code: 395` — correct.
- **deleted**: `not enough data, probably a row type mismatches a database
  schema` — a `Decode` error, i.e. a wrong diagnosis of a real server
  exception, and non-retryable where the truth may be transient.

A tag-absent server is reachable on `main` despite the 26.3 floor: the floor is
enforced inside `pulsus_schema::run_init`
(`crates/pulsus-schema/src/controller.rs:84-100`), which
`crates/pulsus-server/src/serve.rs:645-685` skips entirely when
`PULSUS_SKIP_DDL` is set. A header-stripping proxy in front of a 26.3 server
produces the same `tag=None`. On that arm the parsed code remains forgeable —
that is the documented out-of-support path, and the failure mode is the status
quo rather than a new one.

### Assumptions this rests on, written as assumptions

The anchor cannot be matched without the response's tag, so tenant bytes
cannot open a frame. Two things are assumed, and neither is established:

1. **Non-reuse over the lifetime of stored data.** Rests on an observational
   census: 200 consecutive responses on 26.3.17.110 gave 200 distinct 16-byte
   `a–z` tags. That bounds nothing about reuse across servers, restarts, or a
   retention window, and 26.3 documents no uniqueness or non-reuse contract.
   An earlier argument — "the tag is chosen after the request arrives, so
   stored bytes cannot contain it" — is **withdrawn**: it stops same-request
   adaptation only, and a tenant who observed a tag on an earlier response
   could store it.
2. **A tenant cannot observe a tag through PulsusDB.** `extract_exception_new`
   returns only the declared-length message, and no measured 26.3 response
   body carried the tag outside the frame. This one is checkable, so it is a
   test rather than a claim:
   `an_exception_tag_never_reaches_a_client_visible_message`.

If both failed, the consequence is bounded to what this patch already fixes
for everyone else — a fabricated error on that tenant's own query, plus loss of
the withheld bytes — and it is strictly narrower than today, where no tag is
needed at all.

### Measured limits, stated rather than generalised

- **Header presence.** On ClickHouse 26.3.17.110 reached **directly**,
  `X-ClickHouse-Exception-Tag` was present on all eight request shapes probed:
  plain success, `compress=1`, a 3 M-row success, `wait_end_of_query=1`,
  `enable_http_compression=1`, an instant 404, a late HTTP-200 exception and a
  `JSON` format response. That is eight shapes on one build, not "every
  response from a supported server". Anything that removes the header lands on
  the `tag == None` arm by construction.
- **Frame cap.** `EXC_FRAME_CAP` = 16 MiB is a memory bound on a frame that
  never terminates, roughly 167x the largest exception body measured here
  (100,334 bytes, from `SELECT throwIf(1, repeat('x', 100000))`). Nothing
  measured supports a protocol bound on exception size.
- **Split frames were not observed on the wire.** Under `Compression::Lz4` the
  trailer arrived as its own decompressed block and under `Compression::None`
  as its own chunked piece, in every capture. Nothing measured shows ClickHouse
  emitting `<data><frame opening>` in one chunk. The reassembly is gated
  against constructed shapes precisely because the parser must not depend on a
  framing coincidence.

### Cost on the read path

The scan is unconditional, so it is a real cost and is stated as one. Release
build, mean of 2 000 reps over 1 MiB of splitmix64-seeded RowBinary log lines
(varint length prefix + a structured log body), `bstr` 1.12.3, `lz4_flex`
0.11.6, on the same machine, in one process:

| | per 1 MiB chunk |
|---|---|
| anchor scan (`find`, 33-byte needle) | **44.7 µs** |
| the `rfind(b"Code:")` this removes from `))\n` chunks | 223.4 µs |
| LZ4 compression of the same MiB (scale reference) | 998.9 µs |

So ≈45 ms per GiB of result bytes streamed, sub-millisecond for any result the
read path returns in one query, against removing an up-to-223 µs backwards
scan on every chunk ending `))\n` — and against the ≈999 µs the same MiB costs
to compress, which the server pays on the way out. On the steady-state data path — a chunk that
is neither a frame nor a prefix of one — the added cost is one `ends_with` of
16 bytes plus that scan, with no allocation. A chunk that matches the anchor is
buffered, so a frame spanning chunks costs one allocation growing to the
frame's size; a chunk that is a proper prefix of the anchor buffers at most 32
bytes and copies the following chunk once when it resolves. Buffering happens
only once a response has already failed, or for at most 32 bytes. No SQL,
projection, index or round trip changes.

### Gates

Neither the vendored crate's own `#[test]`s nor a new CI step for them exist —
`clickhouse` is a `[patch.crates-io]` path source, not a workspace member, so
`cargo test --workspace` never compiles them. The gates therefore live in
`pulsus-clickhouse`'s test suites, exactly as §1's gate does:

- **Live** (`tests/live_clickhouse.rs`, the `schema-it` job's
  `Live ClickHouse client suite` step): the three defect shapes
  (`a_successful_read_whose_last_row_ends_in_close_parens_is_not_an_error`,
  `a_trace_shaped_read_whose_payload_ends_in_close_parens_delivers_its_span`,
  `a_log_shaped_read_survives_every_block_boundary_alignment`), the control
  (`a_streamed_exception_after_output_still_carries_its_real_code`), and
  `the_mock_frame_layout_matches_a_real_streamed_exception`, which replays the
  shared `frame_bytes` builder against a frame captured from the live server
  on every CI run and **fails rather than skips** if the response comes back
  buffered.
- **Hermetic client-parser gates** (`tests/mock_clickhouse.rs`, the `ci` job's
  `cargo test --workspace`): a raw-TCP mock whose LZ4 block boundaries — and
  therefore the decompressed chunk boundaries the parser sees — are chosen
  rather than hoped for. They establish what our parser does with a given byte
  sequence and split; they do **not** establish that ClickHouse emits that
  framing, which is what AC13 above is for. The mock depends on
  `clickhouse::_priv::lz4_compress`, which is `#[doc(hidden)]` and
  semver-exempt — acceptable only because this crate is vendored and pinned.

**Re-vendor rule (§2):** on any `clickhouse` version bump, check whether
upstream has gated the searching extractor on the tag **and** reassembles a
split frame (their tracker is cited in the source,
`https://github.com/ClickHouse/clickhouse-rs/issues/359`). If it has, drop
this patch and take theirs.

### Reproducing a streamed exception

The failing expression must depend on the row. `intDiv(1, 0)` with literal
arguments is constant-folded, so it fails before any output and the response is
a buffered 500 with no trailer at all. Measured on one 26.3.17.110, same server
and request otherwise:

| query | HTTP | body | trailer |
|---|---|---|---|
| `SELECT number, intDiv(1,0) FROM numbers(5000000)` | 500 | 162 B | **absent** |
| `SELECT number, intDiv(1, toInt64(number) - 4000000) FROM numbers(5000000)` | 200 | 35 909 895 B | present |
| `SELECT concat(toString(number), toString(throwIf(number=2500000,'boom'))) AS v FROM numbers(3000000)` | 200 | 20 670 447 B | present |

Add `?default_format=RowBinary` and the last two stream on a stock 26.3
container.
