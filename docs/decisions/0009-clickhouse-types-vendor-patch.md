# ADR 0009: vendor-and-patch `clickhouse-types` for a named tuple column, and the driver's validator for a `JSON` column

Status: **Accepted** (2026-10-01)
Issue: **none.** The owner declined a new issue for this work. It is a prerequisite of [#584](https://github.com/digitalis-io/pulsusdb/issues/584), [#585](https://github.com/digitalis-io/pulsusdb/issues/585) and [#586](https://github.com/digitalis-io/pulsusdb/issues/586), on pull request [#610](https://github.com/digitalis-io/pulsusdb/pull/610); the measurements were taken under #585, which is what both `PATCHES.md` files and the root `Cargo.toml` patch entries point at.
Covers: `vendor/clickhouse-types/PATCHES.md` §§1-5 (the type parser) and `vendor/clickhouse/PATCHES.md` §3 (a `Vec` against a `JSON` column) and §4 (a Rust tuple against a named tuple column).
Related: [ADR 0003](0003-promql-parser-vendor-patch.md) and [ADR 0004](0004-opentelemetry-proto-vendor-patch.md) establish the vendor+patch discipline this ADR reuses; [ADR 0007](0007-clickhouse-vendor-patch.md) is the first patch to this driver and owns the two patches beside the two recorded here; [ADR 0001](0001-clickhouse-client.md) selected the client.
Spawned: no issue. The two upstream reports are not filed — see "What this does not decide".

## Context

The approved trace schema declares two columns whose tuple elements are
**named** (`crates/pulsus-schema/src/catalog.rs`: migration 71's
`trace_landing` at `:1648` and `:1650`, the `spans` record at `:1491` and
`:1493`):

```text
events  Array(Tuple(time_ns UInt64, name LowCardinality(String), attrs JSON, attrs_other String, dropped_attrs UInt32))
links   Array(Tuple(trace_id String, span_id String, trace_state String, flags UInt32, attrs JSON, attrs_other String, dropped_attrs UInt32))
```

**Issue #587 widened both tuples** — each element gained its own
`attrs_other`, the event time became unsigned and the two link ids became
byte buffers. **The decision below is unaffected**: it is about the
vendored type parser reading a named tuple at all, and a wider named tuple
is the same decision.

Every insert into `trace_landing` failed before a byte left the client:

```text
Decode("error while parsing columns header from the response: \
  type parsing error: Unknown data type: \n    time_ns Int64")
```

**Where that happens, and why the message misnames it.** `get_insert_metadata`
(`vendor/clickhouse/src/lib.rs:607`) runs `DESCRIBE TABLE` and calls
`clickhouse_types::DataTypeNode::new` on the `type` string of every row it
reads back. The string that fails is therefore a row of the **metadata
response** — not a column header the server echoed back to us. The message
says otherwise because the conversion has one target:
`From<TypesError> for Error` (`vendor/clickhouse/src/error.rs:54-58`) maps
every type-parse failure onto `Error::InvalidColumnsHeader`
(`vendor/clickhouse/src/error.rs:44-45`, "error while parsing columns header
from the response"), and the inner enum has one
variant for every failure inside the parser, `TypesError::TypeParsingError`
("type parsing error: {0}"). The same text is accurate on the read path, where
`parse_rbwnat_columns_header` does parse a response header. On the insert path
no columns header was parsed, so the message names a step that did not run.

**What the parser cannot read.** Its `Tuple` arm hands each whole argument to
the dispatch, and `a Int64` matches no arm. Measured by calling the library
directly on `clickhouse-types` 0.1.2, the release `Cargo.lock` pins:

```text
Tuple(a Int64, b String)   -> type parsing error: Unknown data type: a Int64
Tuple(Int64, String)       -> OK
```

0.1.3, the latest published, carries the same defect, established by source
rather than by a second run: `data_types.rs` differs between the two releases
by 261 diff lines, but `parse_tuple`, `parse_inner_types`, the `Tuple` dispatch
arm and `Display`'s `Tuple` arm are each byte-identical.

**Why the names must be carried rather than discarded.** On an insert in
`RowBinaryWithNamesAndTypes` the client sends each column's name and its type
as a string. The server resolves each sent name against the table and throws
`Unknown field found in format header` on a miss, so the header is matched by
name and not by position; it then compares each sent type string with that
column's own `getName()` **byte for byte** and refuses the insert with
`Type of 'events' must be …, not …` on any difference. Both checks are on by default
(`input_format_with_names_use_header` and `input_format_with_types_use_header`,
each `true`). The string the client sends is `Display`'s output:
`put_rbwnat_columns_header` writes `column.data_type.to_string()`
(`vendor/clickhouse-types/src/lib.rs:72`). So whatever the parser drops, the
renderer cannot put back, and a parser that read a named tuple as a positional
one would send a header the server refuses.

**The pretty-versus-compact asymmetry**, which a future reader is least likely
to guess. `DESCRIBE TABLE` returns the type through `getPrettyName()`, not
`getName()` — `print_pretty_type_names` defaults to `true` — and a named
tuple's pretty form is multi-line, each element behind four spaces of indent.
That is where the newline and the four spaces in the error above come from. The
SELECT response header carries the **compact** form instead: it is written as
`type->getName()`, with the binary-encoded alternative off by default. So the
parser has to accept both forms, and `Display` has to emit the compact one,
because the compact one is what an insert header is compared against. The
round trip is therefore not the identity on a `DESCRIBE` string: pretty in,
compact out.

**The second defect, reached by fixing the first.** Thirteen parse functions
slice their input at fixed byte offsets without checking either end is a
character boundary, and nothing in `data_types.rs` is checked at all. Reading
a named tuple means handing `DataTypeNode::new` the type half of one argument,
which the unpatched code never does, so it opens a route from a tuple argument
to every one of those sites. Measured over a 1,073-input corpus — a two-byte
and a three-byte character inserted at every character boundary of twenty-one
complete types, each type truncated at every byte, plus three named `JSON(..)`
inputs — run through the library directly: **103 inputs abort the caller, at 16
distinct sites.** The panic unwinds out of `DataTypeNode::new` through the
awaiting task rather than returning, so a caller has nothing to handle. The
function's domain is bytes off the wire, not strings the library produced.

**The `JSON` column, on the same insert and through the same validator.** A
`JSON` column's RowBinary form is a path count, then per path a
length-prefixed path string and the value as a binary-encoded `Dynamic` — a
type tag then the value's own bytes — with no length prefix on the column as a
whole. Captured off ClickHouse 26.3.29.7 with
`SELECT CAST('<text>' AS JSON) FORMAT RowBinary`. Two of the eleven captured
frames, all of which are pasted in
`crates/pulsus-write/src/writer/trace_json.rs`'s module header:

```text
{"a":1}        0101610a0100000000000000
{"k":{}}       00
```

The only Rust shape that writes that is a sequence of (path, tagged value)
pairs, and upstream's validator does not admit one: `validate_impl`'s
`SerdeType::Seq(_)` arm matches `Array`, `Map`, `Ring`, `Polygon`,
`MultiPolygon`, `LineString` and `MultiLineString`, and `JSON` falls to
`err_on_schema_mismatch`. The arm that does admit `JSON` is
`SerdeType::Str | SerdeType::String`, which is the JSON-as-string form: the
server reads that only at `input_format_binary_read_json_as_string = 1`, off by
default, and text JSON on this engine refuses a non-finite double at all —
`{"k":NaN}`, `{"k":Inf}`, `{"k":Infinity}` and `{"k":1e400}` each answer
`Code: 117. DB::Exception: Cannot parse JSON object here`. A span attribute is
a sender-chosen `double`, so the string form cannot carry what a sender sends.

That `JSON` arm reached the tree with no decision record of its own. It is
recorded here rather than separately because the two patches share their
alternative: option 2 below removes the need for both, and nothing else does.

## Options considered

1. **Drop the element names from the DDL.** A positional tuple parses today —
   `Tuple(Int64, String)` → OK, above — so this unblocks the insert with no
   patch. The cost is that every statement over those columns then refers to
   tuple fields by number, `events.2` rather than `events.name`: inserting a
   field in the middle of the tuple silently changes what each existing
   expression means, and nothing fails loudly when it does. No statement reads
   those fields yet —
   `git grep -n 'events\.name\|events\.time_ns\|links\.span_id' -- 'crates/*/src'
   'docs/TraceQL/measure'` returns nothing — so the cost falls on every
   statement the read path will carry. The approved DDL and
   `docs/TraceQL/measure/schema.sql:35,37` both move as well. Rejected.
2. **Turn off insert-side schema validation** (`Client::with_validation(false)`,
   `vendor/clickhouse/src/lib.rs:549`). This also unblocks the insert with no
   patch, and it is the one option that removes the need for **both** patches
   recorded here. The cost is that it is one setting on one client, so the
   check goes for metrics and logs too — and it is not only the check: with
   validation off the client sends plain `RowBinary`
   (`vendor/clickhouse/src/lib.rs:457`), so no names and
   no types go in the request at all, the server matches columns by position,
   and a row that no longer matches its table becomes a server exception on
   the insert, or a wrong value in the wrong column, instead of a refusal
   before anything is sent. Rejected.
3. **Vendor and patch the type crate.** Keeps the schema and keeps the
   validation; costs a second vendored crate from the same upstream to carry at
   every bump. **Chosen by the owner on 2026-10-01.**

## Decision

Vendor `clickhouse-types 0.1.2` — the release `Cargo.lock` already pins, so no
resolved version moves — to `vendor/clickhouse-types`, wired through the root
`[patch.crates-io]` so every `clickhouse_types::...` import path is unchanged,
and apply five patches in one file, `src/data_types.rs`. Add two arms to
`validate_impl` in `vendor/clickhouse`: one for the new variant and one for a
`JSON` column. **The schema does not change.** Each `PATCHES.md` carries its
own exhaustive change list, measurements and re-vendor rule.

The parser change is not a rule over the text. `parse_tuple` tries a named
reading first and accepts it only when each element's type, parsed on its own,
renders back to that element's type text byte for byte; every failure inside
the named reading returns `None`, and on `None` the untouched existing code
runs and its result is returned. Two mechanism-level alternatives were rejected
on measurements, both written out in `vendor/clickhouse-types/PATCHES.md`: a
text rule that decides whether an element is named (eleven of the sixteen
prefix dispatch arms slice their payload one byte past the key and drop the
final byte unchecked, so `"<key> <tail>"` parses exactly when `<tail>` minus
its last byte parses; composed with the `Time` arm's unanchored `starts_with`
that gives five infinite families of inputs which parse today,
`Tuple(Array Timex, UInt8)` among them), and guarding the panicking slices one
at a time (two rounds each closed the single site a counterexample named and a
third counterexample came from another site, so the patch sweeps the file's
indexing instead, which is a closed set and re-runnable on a bump).

The named reading goes **first** rather than second. `Tuple(Timestamp UInt64)`
parses today as `Tuple(Time)` — the `Time` arm's `starts_with` is unanchored —
and a column whose tuple element is named `Timestamp` is ordinary; reading it
as a positional `Time` sends the server a header it refuses.

### The property, so a future bump can be checked against it

> For every input string, the patched `DataTypeNode::new` returns one of
> exactly three things: the unpatched result — the same value, or the same
> error with the same bytes; `Ok(NamedTuple(..))`; or an **error**, on an input
> where the unpatched code panics. Nothing returns a different value and
> nothing panics.

and, for the second of those three:

> Write an accepted input as `Tuple( w1 a1 , w2 a2 , … , wk ak )`, where each
> `wj` is the maximal run of ASCII whitespace before argument `j` and each `aj`
> begins with a non-whitespace byte. Every `aj` is reproduced **byte for byte**
> in `Display`'s output, in order, and the only difference from the input is
> that `w1` is dropped and every later `wj` becomes one space.

Those two give the insert path the two things the Context requires: on any
string the server's `getName()` produced the round trip is the identity, and on
the pretty form `DESCRIBE TABLE` returns it is the pretty-to-compact
conversion. Byte identity on an arbitrary accepted input is **not** claimed and
is false — `Tuple(a Int64,b String)` is accepted and renders
`Tuple(a Int64, b String)`. One panic class is outside the first statement and
is unchanged by the patch — the stack-overflow item under "What this does not
decide".

What that buys for the two approved columns is the insert itself: both forms of
each column type parse, and `Display` renders the compact form the insert
header is compared against. The cases that check it, hermetic and against a
server, are named in each `PATCHES.md`'s Gates section rather than restated
here.

Consequences:

- Every string the unpatched parser accepts keeps its value, and every error it
  raises keeps its bytes. The one behaviour change outside named tuples is the
  103 measured inputs, from an abort to an error — an error and never a value.
- We now carry **two vendored crates from the same upstream repository**,
  `vendor/clickhouse` and `vendor/clickhouse-types`, with two `PATCHES.md`
  files and two re-vendor rules, out of four vendored crates in all. The two
  rules are **paired**: the validator arm for the new variant can only be
  dropped when the parser patch is, because it exists only for the variant the
  parser patch adds.
- The new variant's arrival is **silent** everywhere in the driver.
  `DataTypeNode` is `#[non_exhaustive]` and `validate_impl` is a chain of arms
  each ending in a fallthrough, so nothing fails to compile and nothing warns.
  The one reader the compiler does report is `Display`, whose match has no
  wildcard. A future bump that re-applies the parser patch and forgets the
  validator arm therefore gets no diagnostic; what it gets is a client-side
  refusal on every trace insert.
- No partial-failure mode is added. The patch changes which type strings parse
  and changes no request count, no block count and no abort path. A type string
  that does not parse fails inside `get_insert_metadata`, before any request,
  so nothing is stored; a row that fails client-side validation is refused
  while the client buffer is being filled, and the request is then dropped
  without `end()`, so the server commits nothing. The one-block-per-push
  guarantee is `crates/pulsus-clickhouse/src/settings.rs`'s `landing_insert`
  set and is stated there. A push still stores everything or nothing.
- One diagnostic is worse than it could be. Because the named reading cannot
  raise an error, ``Tuple(`attrs` Flurble)`` reports
  ``Unknown data type: `attrs` Flurble`` rather than naming `Flurble` alone.
  That message resembles the defect this patch fixes, so a future reader may
  look for a parser bug that is not there. It is the price of the property
  above.
- The parse cost is exponential in the nesting depth of one constructed family:
  an element named exactly `Array`, `Nullable`, `LowCardinality`, `Tuple`,
  `Variant` or `Map` at **every** level, over an innermost type the crate
  cannot parse. Each level roughly doubles the work. Counted on a simulation of
  the two dispatch arms that family uses, in units of simulated parses: 4 at
  depth 1, 2,930 at depth 10 and 8,212,991 at depth 22, against one chain
  linear in the depth before. Any other element name makes the fallback a
  single dispatch miss and the cost linear, the approved schema's nesting depth
  is one, and a type string is parsed once per request and never per row.
- `vendor/clickhouse-types/Cargo.toml` carries an added empty `[workspace]`
  table, which `vendor/clickhouse`'s does not. Without it cargo refuses to
  resolve `--manifest-path vendor/clickhouse-types/Cargo.toml`, and the CI step
  that runs the crate's own 42 cases could not exist. That step is the pin on
  the checked-index patch.
- A `JSON` column's bytes are now written by this workspace rather than by the
  driver, and the driver validates nothing inside one.

## What this does not decide

**The upstream reports are not filed, so neither patch has an end date.** Two
separable reports, written out in `vendor/clickhouse-types/PATCHES.md`'s
closing section — the named tuple, and the unchecked byte indices, the second
being the stronger because it needs no judgement call and the function's inputs
come off the wire. The owner is filing them himself. Each re-vendor rule's
delete-on-upstream-fix path is what ends the patch, as ADR 0003's does for its
five fixes; until a report is filed and taken, there is no such date.

**The parser is not total against a stack overflow.** `DataTypeNode::new` is
recursive, so a deeply enough nested input overflows the stack, before and
after, and a stack overflow is an abort rather than a catchable panic. No case
can pin it.

**An element whose type text the crate re-renders differently is not read as
named**, and the tuple then takes the unpatched path, which errors. Measured
members: `Enum8('b' = 2, 'a' = 1)` → `Enum8('a' = 1, 'b' = 2)`,
`JSON(max_dynamic_paths=8)` → `JSON`, `Time64(3, 'UTC')` → `Time64(3)`. Nothing
that worked before is lost: an insert whose header carried `Display`'s string
for such a type would be refused by the server anyway, so this turns a server
refusal into a client-side parse error, and on the read path it is a refusal to
read a column that does not read without the patch either.

**The pretty form of a tuple nested inside a named tuple is not read.** The
inner newlines and indentation survive into the element's type text, so the
re-rendering test fails. The compact form of the same type reads normally, so
only `DESCRIBE TABLE` on such a column is affected, and no approved table has
one: `git grep -n 'Tuple(' -- crates/pulsus-schema/src/catalog.rs` returns four
lines, all `Array(Tuple(<scalars>))`.

**A type whose canonical name begins with an identifier and a space** would be
read as a name and a type. What rules it out is the element-name identifier
test, and the search behind it is bounded by the three routes the server's
`getName()` has rather than by a file count: two names carry a top-level space,
`Function(T -> U)` and `Nested(<name> <type>)`, and both fail the test because
the byte run before the first space contains `(`. If a future release does
carry one, the insert header is still byte-correct by the property above and
the only difference is which validator shape the client picks.

**A `JSON` nested in a named tuple stops element validation.** The `JSON` arm
returns `Ok(None)`, and `Option<InnerDataTypeValidator>::validate` returns
`Ok(None)` the moment the validator is `None`, so a wrong type tag inside
`events[i].attrs` reaches the server rather than the driver and the error is a
server exception on the insert rather than a refusal before it. What stands in
for it is the byte-exact cases in
`crates/pulsus-write/src/writer/trace_json.rs`, which reproduce every captured
frame from this workspace's own encoder.

**Too few element fields is not refused on the insert path.**
`check_tuple_fully_validated` — the only producer of `tuple was not fully
(de)serialized` — has one caller, in the deserializer, so it runs on the way in
and never on the way out. Measured against 26.3.29.7: a one-field Rust tuple
against `Array(Tuple(a Int64, b String))` is accepted by the driver, the block
is sent, and the server answers `Code: 32 … Attempt to read after eof`. The
too-many direction **is** refused on the insert path. The asymmetry is
upstream's, is the same for a positional tuple, and this patch does not change
it.

**Nothing here runs against a cluster.** A clustered deployment is the
production case; this change is client-side type parsing with no node-local
state, so there is nothing a second replica can disagree about — but nothing
measured here establishes that.
