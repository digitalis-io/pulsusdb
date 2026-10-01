# ADR 0009: vendor-and-patch `clickhouse-types` so a named tuple column can be written and read

Status: **Accepted** (2026-10-01)
Issue: [#585](https://github.com/digitalis-io/pulsusdb/issues/585) (the span row encoder and the `JSON` column through the driver)
Related: [ADR 0003](0003-promql-parser-vendor-patch.md), [ADR 0004](0004-opentelemetry-proto-vendor-patch.md) and [ADR 0007](0007-clickhouse-vendor-patch.md) establish the vendor+patch discipline this ADR reuses; [ADR 0001](0001-clickhouse-client.md) selected the client.

## Context

The approved trace schema declares two columns with **named** tuple elements
(`crates/pulsus-schema/src/catalog.rs`, migration 71 and the `spans` record):

```text
events   Array(Tuple(time_ns Int64, name LowCardinality(String), attrs JSON, dropped_attrs UInt32))
links    Array(Tuple(trace_id FixedString(16), span_id FixedString(8), trace_state String, flags UInt32, attrs JSON, dropped_attrs UInt32))
```

`clickhouse`'s insert path reads the column types before sending anything:
`get_insert_metadata` runs `DESCRIBE TABLE` and calls
`clickhouse_types::DataTypeNode::new` on every row. That parser has no
named-tuple form at all — its `Tuple` arm hands each whole argument to the
dispatch, and `a Int64` matches no arm. Measured against `clickhouse-types`
0.1.2, the release `Cargo.lock` pins, and 0.1.3, the latest published:

```text
Tuple(a Int64, b String)   -> type parsing error: Unknown data type: a Int64
Tuple(Int64, String)       -> OK
```

So **every** insert into `trace_landing` failed before a byte was sent, and
`insert_block` is the only client door into that table:

```text
Decode("error while parsing columns header from the response: \
  type parsing error: Unknown data type: \n    time_ns Int64")
```

The newline and the four spaces are in that message because `DESCRIBE TABLE`
returns the **pretty** name — `print_pretty_type_names` defaults to `true` —
while a SELECT response header carries the compact one. Both forms reach the
parser, on the two different paths, so both have to be read; and the string
`Display` emits is compared against the server's own `getName()` **byte for
byte** when an insert header arrives, so the rendering must be the compact one.

A second defect sits beside the first and is reached by fixing it. Thirteen
parse functions slice the input at fixed byte offsets without checking either
end is a character boundary, and nothing in `data_types.rs` is checked at all.
Measured over a 1,073-input corpus built by inserting a two-byte and a
three-byte character at every character boundary of twenty-one complete types
and truncating each at every byte, plus three named `JSON(..)` inputs: **103
inputs abort the caller, at 16 distinct sites.** The panic unwinds out of
`DataTypeNode::new` through the awaiting task rather than returning, so there
is nothing for a caller to handle. **The function's domain is bytes off the
wire**, not strings the library produced.

## Options considered

1. **Rename the columns to positional tuples.** Measured: it unblocks the
   whole suite, and the cost is that a read says `events.2` rather than
   `events.name`, and that the approved DDL and
   `docs/TraceQL/measure/schema.sql` both move. Rejected: the schema is
   approved and the names are what the read path will compile against.
2. **`Client::with_validation(false)`.** Measured: it unblocks the suite too,
   and the cost is that insert-side schema validation disappears for **every**
   insert of **every** signal, and `vendor/clickhouse`'s own §3 patch — the
   `JSON` column arm — has nothing left to do. Rejected.
3. **Guard the panicking slices one at a time.** Two rounds did exactly that,
   each closing the one site a counterexample named; a third counterexample
   then arrived from a fourteenth. Rejected in favour of a sweep over the
   file's indexing, which is a closed set and re-runnable on a bump.
4. **A text rule that decides whether a tuple element is named.** Rejected on
   a measurement: eleven of the sixteen prefix dispatch arms slice their
   payload one byte past the key and drop the final byte unchecked, so
   `"<key> <tail>"` parses exactly when `<tail>` minus its last byte parses.
   Composed with the `Time` arm's unanchored `starts_with`, five of those arms
   each generate an **infinite** family of inputs that parse today —
   `Tuple(Array Timex, UInt8)` is one — so writing the rule down means writing
   down the language. Two rounds proposed such a rule and each was refuted by a
   member of a family it had not enumerated.
5. **Vendor and patch.** Chosen.

## Decision

Vendor `clickhouse-types 0.1.2` — the release the lockfile already pins, so no
resolved version moves — to `vendor/clickhouse-types`, wired through the root
`[patch.crates-io]` so every `clickhouse_types::...` import path is unchanged,
and apply five patches in one file. `vendor/clickhouse-types/PATCHES.md`
carries the exhaustive change list, the measurements and the re-vendor rule.
**The schema does not change.**

The mechanism is not a classifier. `parse_tuple` tries a named reading first,
accepts it only when the element type's **own rendering reproduces the
argument's type text byte for byte**, and otherwise falls back to the
untouched existing code. The named reading returns `Option` and every failure
in it returns `None`, so:

> For every input string, the patched `DataTypeNode::new` returns one of
> exactly three things: the unpatched result, with the same value or the same
> error bytes; `Ok(NamedTuple(..))`; or an **error**, on an input where the
> unpatched code panics. Nothing returns a different value and nothing panics.

The named reading goes **first** rather than second, which is the one place
the patch is not purely additive in order. `Tuple(Timestamp UInt64)` parses
today as `Tuple(Time)` — the `Time` arm's `starts_with` is unanchored — and a
column whose tuple element is named `Timestamp` is ordinary; reading it as a
positional `Time` sends the server a header it refuses.

The insert-side validator needs one arm of its own, in the other vendored
crate: `vendor/clickhouse/PATCHES.md` §4. Its arrival is **silent** —
`DataTypeNode` is `#[non_exhaustive]` and `validate_impl` is a chain of arms
each ending in a fallthrough — so nothing fails to compile and nothing warns.
The only reader the compiler does report is `Display`, whose match has no
wildcard.

Consequences:

- Every string that parses today keeps today's value, and every error today
  raises keeps its bytes. The one behaviour change outside named tuples is the
  103 measured inputs, from an abort to an error — an error and never a value.
- One diagnostic is worse than it could be: because the named reading cannot
  raise an error, ``Tuple(`attrs` Flurble)`` reports
  ``Unknown data type: `attrs` Flurble`` rather than naming `Flurble` alone.
  That is the price of the property above.
- The parse cost is exponential in the nesting depth of one constructed family
  — an element named exactly `Array`, `Nullable`, `LowCardinality`, `Tuple`,
  `Variant` or `Map` at **every** level, over an innermost type the crate
  cannot parse. Counted on a simulation: 4 parse units at depth 1 and
  8,212,991 at depth 22, a factor of 1.95 per level. Any other element name
  makes the fallback a single dispatch miss and the cost linear, the approved
  schema's depth is one, and a type string is parsed once per request and never
  per row.
- We own one more vendored crate at each dependency bump. The re-vendor rule
  says to drop the patch the moment upstream takes named tuples, and to re-run
  the indexing **sweep** — not the corpus — over the new release's own file.
- `vendor/clickhouse-types/Cargo.toml` carries an added empty `[workspace]`
  table, which `vendor/clickhouse`'s does not. Without it
  `cargo metadata --manifest-path vendor/clickhouse-types/Cargo.toml` fails to
  resolve, and the CI step that runs the crate's own 42 cases could not exist.
  That step is the pin on the checked-index patch.

## What this does not decide

**The parser is not made total against a stack overflow.**
`DataTypeNode::new` is recursive, so a deeply enough nested input overflows the
stack, before and after, and a stack overflow is an abort rather than a
catchable panic. No case can pin it.

**A `JSON` nested in a named tuple still stops element validation.**
`vendor/clickhouse`'s §3 returns `Ok(None)` for `DataTypeNode::JSON`, so a
wrong type tag inside `events[i].attrs` reaches the server rather than the
driver. That is §3's stated limit and this change neither widens nor narrows
it.

**Too few element fields is not refused on the insert path.** The message
`tuple was not fully (de)serialized` has one producer and one caller, and the
caller is the deserializer, so it is a read-path refusal. Measured: a one-field
Rust tuple against `Array(Tuple(a Int64, b String))` is accepted by the driver
and answered `Code: 32 … Attempt to read after eof` by the server. The
too-many direction **is** refused on the insert path. That asymmetry is
upstream's, is the same for a positional tuple, and is recorded in
`vendor/clickhouse/PATCHES.md` §4.

**A type whose canonical name begins with an identifier and a space** would be
read as a name and a type. What rules it out is the element-name identifier
test, and the search behind that is bounded by the three routes the server's
`getName()` has rather than by a file count: two names carry a top-level space,
`Function(T -> U)` and `Nested(<name> <type>)`, and both fail the test because
the byte run before the first space contains `(`. If a future type does carry
one, the insert header is still byte-correct and the only difference is which
validator shape the client picks.

**No case runs against a cluster.** The change is client-side type parsing
with no node-local state, so there is nothing a second replica can disagree
about — but nothing here establishes that.
