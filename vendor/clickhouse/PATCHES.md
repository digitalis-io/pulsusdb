# Patches applied to `clickhouse 0.15.1`

This is a patched, vendored copy of [`clickhouse`
0.15.1](https://github.com/ClickHouse/clickhouse-rs), wired into the workspace
via `[patch.crates-io]` (root `Cargo.toml`) so every `clickhouse::...` import
path is unchanged. See
[`docs/decisions/0007-clickhouse-vendor-patch.md`](../../docs/decisions/0007-clickhouse-vendor-patch.md)
for the decision this copy implements, and issue #382 for the measurements.

**Four patches, in four functions across two files**: §1
(`collect_bad_response`, issue #382) and §2 (`extract_exception` and
`DetectDbException`, issue #412), both in `src/response.rs`, and §3 and §4
(`validate_impl`, issue #585) in `src/rowbinary/validation.rs` — §3 in its
`SerdeType::Seq(_)` arm and §4 in its `SerdeType::Tuple(len)` arm. One
test-support export joins them in `src/lib.rs`'s `_priv` module and is
named in §3; it is not a change to the driver's behaviour. Nothing else is
modified. The vendored tree
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
**run against ClickHouse 26.3 or newer** — on 24.8 it passed either way. The
supported floor is 26.8 LTS (issue #376 moved it to 26.3, issue #624 to 26.8),
and 26.8 is the only version we run, so that gate is live in every CI job and
on every developer machine rather than conditional on which server happened
to be up.

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
## 3. `validate_impl` accepts a `Vec` against a `JSON` column

`src/rowbinary/validation.rs`, in `validate_impl`'s `SerdeType::Seq(_)` arm.

### What upstream does

The arm matches `Array`, `Map`, `Ring`, `Polygon`, `MultiPolygon`,
`LineString` and `MultiLineString`, and falls to `err_on_schema_mismatch` for
everything else. The `SerdeType::Str | SerdeType::String` arm is what matches
`DataTypeNode::JSON`. So a `Vec` against a `JSON` column is
`Error::SchemaMismatch`.

### Why that is a defect for us

A `JSON` column's RowBinary form is a **path count, then per path a
length-prefixed path string and the value as a binary-encoded `Dynamic`** — a
type tag then the value's own bytes, with no length prefix on the column as a
whole. Captured off ClickHouse 26.3.29.7 with
`SELECT CAST('<text>' AS JSON) FORMAT RowBinary`:

```text
{"a":1}                              0101610a0100000000000000
{"k":["a",1]}                        01016b1e2b20021501610a0100000000000000
{"k":{}}                             00
```

The only shape a Rust type can take to produce that is a sequence of
`(path, tagged value)` pairs. Writing the column as a Rust `String` instead is
accepted and is the **JSON-as-string** form, which the server reads only at
`input_format_binary_read_json_as_string = 1` and which cannot carry a
non-finite double at all: text JSON refuses one on this engine — four
attempts, each `SELECT toJSONString(CAST('<text>' AS JSON))` on 26.3.29.7,
with `{"k":NaN}`, `{"k":Inf}`, `{"k":Infinity}` and `{"k":1e400}` each
answering `Code: 117. DB::Exception: Cannot parse JSON object here`. A span
attribute is a client-chosen `double`, so a path that cannot carry `±Inf` or
`NaN` cannot carry what a sender sends.

There is no length-prefix-free byte route either: `serialize_bytes` and
`serialize_str` both write a LEB128 prefix, and the one exception,
`WithoutLenPrefix` in `serialize_newtype_struct`, is gated on
`name.starts_with(int256::MODULE_PATH)` and validates `Bytes(32)`.

**No table in this workspace had a `JSON` column before this change**:
`git grep -n 'JSON  *CODEC\|  JSON,' 53c2518e -- crates` returns nothing, and
the three JSON-ish columns in the shipped schema (`log_streams.labels`,
`log_landing.labels`, `trace_attrs_idx.val`) are `String`.

### The change

One match arm:

```rust
DataTypeNode::JSON => Ok(None),
```

`Option<InnerDataTypeValidator>::validate` returns `Ok(None)` the moment the
validator is `None`, so nothing inside the sequence is validated and the
writer owns every byte. The arm applies at **any depth**, which is what covers
the `attrs` inside an `Array(Tuple(…, attrs JSON, …))` column.

Additive, no public API change, no `Error` variant added or altered, no
semver impact. Every other arm is untouched, so every other column type is
validated exactly as before.

### What this patch does not reach

**Nothing inside the sequence is validated**, so a wrong type tag reaches the
server rather than the driver, and the error is then a server exception on the
insert rather than a `SchemaMismatch` before it. That is the patch's stated
limit. What stands in for it is the byte-exact cases in
`crates/pulsus-write/src/writer/trace_json.rs`, which reproduce from this
workspace's own encoder all eleven captured frames in that file's own module
header — three of them are quoted above — and whose case loop asserts
`cases.len() == 11` before comparing any of them.

### The test-support export in `_priv`

`src/lib.rs`, `_priv::serialize_row_unvalidated`. The crate's own
`serialize_row_binary` is `pub(crate)` and nothing public reaches a row's
bytes without a client and a server, so the byte-exact cases above had no
door. It serializes one row with no column metadata and so no validation, it
is called from no production path, and the validating serializer the client
uses is untouched.

`_priv::deserialize_row_unvalidated` is its mirror, added when the read side
of this column gained a decoder: `deserialize_row` is `pub(crate)` too, so a
hermetic case could assert the bytes a value produces and had no way to
assert the value a frame produces. The same three properties — no column
metadata, no production caller, the validating deserializer untouched — and
the same gate location: the eleven frames are decoded back in
`crates/pulsus-clickhouse/src/json_column.rs`'s own cases, against the same
literals the encoding loop compares.

### Gates

Neither the vendored crate's own `#[test]`s nor a new CI step for them exist —
`clickhouse` is a `[patch.crates-io]` path source, not a workspace member, so
`cargo test --workspace` never compiles them, and a `#[test]` added there
would be a case that cannot fail. The gate therefore lives in
`pulsus-clickhouse`'s suites, exactly as §1's and §2's do:

- **Live** (`tests/live_clickhouse.rs`, the `schema-it` job's
  `Live ClickHouse client suite` step):
  `a_vec_against_a_json_column_is_accepted` inserts a sequence into a `JSON`
  column, reads the stored path back, and checks that an `Array(Tuple(…))`
  column still admits a sequence and a `UInt8` column still refuses one.
  Measured with the arm removed: the insert fails
  `Decode("schema mismatch: … attempting to (de)serialize ClickHouse type
  JSON as Vec<T>")`.

**Re-vendor rule (§3):** on any `clickhouse` version bump, check whether
upstream's `SerdeType::Seq(_)` arm admits `DataTypeNode::JSON`. If it does,
drop this patch and take theirs; if it admits it with **validation** of the
sequence's contents, read what it validates against before taking it — this
workspace writes the column's bytes itself and a validator that expects
another shape refuses every trace push.


## 4. `validate_impl` accepts a Rust tuple against a **named** tuple column

`src/rowbinary/validation.rs`, in `validate_impl`'s `SerdeType::Tuple(len)`
arm (issue #585).

### What upstream does

The arm matches `FixedString`, `Tuple`, `Array`, `IPv6`, `UUID` and `Point`,
and falls to `err_on_schema_mismatch` for everything else. `DataTypeNode` has
no named-tuple variant upstream, so there is nothing to match — the whole
defect is one layer down, in `clickhouse-types`, where the type parser cannot
read `Tuple(a Int64, b String)` at all.

### Why that is a defect for us

`vendor/clickhouse-types` is patched to read a named tuple, into a new
`DataTypeNode::NamedTuple` variant (see its `PATCHES.md`). The variant's
arrival is **silent everywhere in this crate**: `validate_impl` is a chain of
arms each ending in a fallthrough, and `DataTypeNode` is `#[non_exhaustive]`,
so nothing here fails to compile and nothing warns. Without this arm the
variant reaches the arm's own `_ =>` and the insert is refused with
`attempting to (de)serialize nested ClickHouse type … as a tuple or sequence
with length N` — a client-side refusal for a column the server is perfectly
happy with. `trace_landing` and `spans` both declare `events` and `links`
with named tuple elements, and `TraceEventTuple` and `TraceLinkTuple` reach
this arm through `serialize_tuple`.

### The change

One match arm, beside the positional `Tuple` one:

```rust
DataTypeNode::NamedTuple(elements) => Ok(Some(InnerDataTypeValidator {
    root,
    kind: InnerDataTypeValidatorKind::Tuple(elements.types()),
})),
```

A named tuple's wire form **is** its positional one: the names are metadata the
server renders into the type string (`DataTypeTuple::doGetName`) and the
serialization does not carry them, so validation walks the element types
exactly as for `Tuple`. `elements.types()` is the `&'caller [DataTypeNode]`
that `InnerDataTypeValidatorKind::Tuple` already takes, with the lifetime
coming from `column_data_type`, so the existing `split_first` cursor and
`check_tuple_fully_validated` are reused unchanged and the element count is
left to them.

**Accepted here and nowhere else.** It is in the `SerdeType::Tuple(len)` arm
only, so a Rust **sequence** against a named-tuple column stays a mismatch,
exactly as it is for a positional tuple. Every other arm is untouched, and so
is `null_encoding_for`'s `_ => None`, which is correct: a named tuple carries
no null marker.

Additive, no public API change, no `Error` variant added or altered, no
semver impact.

### What this patch does not reach

**Too few element fields is not refused on the insert path.**
`check_tuple_fully_validated` — the only producer of
`tuple was not fully (de)serialized` — has exactly one caller in the crate,
`rowbinary/de.rs`'s `next_element_seed`, so it runs when the last element of a
sequence has been **de**serialized and never on the way out. Measured against
26.3.29.7 with a one-field Rust tuple against `Array(Tuple(a Int64, b String))`:
the driver accepts the short tuple, sends the block, and the server answers
`Code: 32 … Attempt to read after eof`. The too-MANY direction **is** refused
on the insert path, by the same cursor's `None` branch
(`attempting to (de)serialize … while no more elements are allowed`). This is
upstream's asymmetry, unchanged by this patch, and it is the same on a
positional tuple.

### Gates

Neither the vendored crate's own `#[test]`s nor a CI step for them exists —
`clickhouse` is a `[patch.crates-io]` path source, not a workspace member, so
`cargo test --workspace` never compiles them. The gates live in
`pulsus-clickhouse`'s and `pulsus-write`'s suites, as §§1-3's do:

- **Live** (`pulsus-clickhouse/tests/live_named_tuple.rs`, the `schema-it`
  job's `Live named-tuple insert and read-back` step):
  `l1_an_insert_into_a_named_tuple_column_succeeds` inserts into
  `Array(Tuple(a Int64, b String))` and reads the named element back;
  `l4_a_wrong_element_type_is_refused_by_the_element_it_is_wrong_for` shows the
  validator **descended into** the tuple rather than refusing it whole — the
  message names `Int64`, the element type, not the column type;
  `l3_an_element_tuple_short_of_a_field_is_refused` is the read-path half, and
  `l2_an_insert_into_a_positional_tuple_column_still_succeeds` is the pin that
  a positional tuple is unaffected.
- **Live** (`pulsus-write/tests/trace_rows_v2.rs`, the `schema-it` job's
  `Trace landing rows and the JSON column` step):
  `two_events_and_two_links_in_one_span_all_land` is the one that needs the
  cursor to be fresh per array element — a shared cursor would validate the
  second event's first field against the first event's second type.

**Re-vendor rule (§4):** on any `clickhouse` version bump, check whether
upstream's `SerdeType::Tuple(len)` arm admits a named tuple. It can only do so
once `clickhouse-types` has a variant for one, so this entry is paired with
that crate's re-vendor rule: if upstream takes named tuples there, drop that
patch and this arm together.
