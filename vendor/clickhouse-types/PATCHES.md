# Patches applied to `clickhouse-types 0.1.2`

This is a patched, vendored copy of [`clickhouse-types`
0.1.2](https://github.com/ClickHouse/clickhouse-rs), wired into the workspace
via `[patch.crates-io]` (root `Cargo.toml`) so every `clickhouse_types::...`
import path is unchanged. See
[`docs/decisions/0009-clickhouse-types-vendor-patch.md`](../../docs/decisions/0009-clickhouse-types-vendor-patch.md)
for the decision this copy implements, and issue #585 for the measurements.

**Five patches, in one file**, `src/data_types.rs`: §1 `split_type_arguments`,
§2 `split_element_name` with `parse_named_tuple` and the `NamedTuple`
representation, §3 the `parse_tuple` call site, §4 the nineteen checked byte
indices, and §5 `Display`'s new arm. Nothing else is modified. `src/` is
otherwise upstream's byte for byte; `Cargo.toml` is the registry-normalized
manifest with **an empty `[workspace]` table added**, which is what makes
`cargo test --manifest-path vendor/clickhouse-types/Cargo.toml` resolve at all
— without it cargo refuses, asking for the package to be a workspace member or
to carry that table. `Cargo.lock` is as published. Upstream's
`Cargo.toml.orig` and `release.toml` are not copied. Both licences are copied
from `vendor/clickhouse/`: both manifests declare
`license = "MIT OR Apache-2.0"` and the same
`repository = "https://github.com/ClickHouse/clickhouse-rs"`, so the two
crates are the same upstream repository under the same terms.

The insert-side validator needs one arm of its own for the new variant, and
that is in the other vendored crate: `vendor/clickhouse/PATCHES.md` §4.

## Gates

The crate's own 42 `#[test]`s run in CI, in the `ci` job's
`Vendored type-parser crate's own suite` step. **That step is a pin on §4**: a
checked slice must return the same slice wherever the unchecked one did not
panic, and upstream's compound-parser cases assert exact values and exact
messages. Not every one of the 42 runs an edited line — an input matched by one
of the thirty exact dispatch arms, or by `Time`, or by `Interval`, reaches no
edited line, and neither does a case that only asserts a rendering.

The named reading's own cases are
`crates/pulsus-clickhouse/tests/named_tuple_types.rs` (hermetic, 24 cases) and
`crates/pulsus-clickhouse/tests/live_named_tuple.rs` (5 cases, against a
server, both paths). They live in a workspace crate because a
`[patch.crates-io]` path source is not a workspace member, so a `#[test]`
written in here would never be compiled by `cargo test --workspace`.

## Re-vendor rule

On any `clickhouse-types` bump:

- **Re-read `parse_tuple`, `parse_inner_types`, the `starts_with("Tuple")`
  dispatch arm and `Display`'s `Tuple` arm.** If upstream has taken named
  tuples, drop §§1-3 and §5 and take theirs. §3 calls `parse_inner_types` and
  does not edit it, so a change to that function is a behaviour change this
  patch inherits rather than a merge conflict — which is why it is on this
  list.
- **Re-run §4's sweep, not §4's corpus.** The sweep is
  `/usr/bin/grep -n 'input\[\|input_bytes\[\|map\[\|str\[\|inner\[\|parsed\[\|\.get('
  src/data_types.rs` over the new release's own file; every line it lists must
  be in the checked set or have its bound beside it. The corpus does not
  substitute: a bump that adds an unchecked index in a shape the corpus does
  not generate leaves the corpus case green, which is exactly how `parse_json`'s
  index would have been missed. `named_tuple_types.rs`'s per-site case rows are
  **re-derived from the new release's own sites**, because a bump moves them and
  may change which error each arm returns. Re-check the import list at the top
  of the file too: that is what bounds the sweep to one file.
- **Re-read §1's closed form.** It is a statement about the slicing in eleven
  prefix arms; a bump that anchors any of them, or that validates the final byte
  is `)`, narrows the counterexample families and makes some of the pinned
  inputs errors rather than values. The property below still holds — the pins
  would then be pinning a different today — so what must be re-run is the three
  pin cases, with their expected values taken from the new release's own output.

Measured between 0.1.2 and 0.1.3: `data_types.rs` changes by 261 diff lines and
`leb128.rs` is rewritten, but `parse_tuple`, `parse_inner_types`, the dispatch
arm and the `Display` arm are each byte-identical, and `lib.rs`, `decoders.rs`
and `error.rs` are identical whole. So the defect is in the latest published
release too, and only the tuple code is common ground between the two.

## The property all five patches preserve

For every input string, the patched `DataTypeNode::new` returns one of exactly
three things:

1. **upstream's exact result** — the same value, or the same error with the same
   message;
2. `Ok(NamedTuple(..))`, of which the next section holds;
3. an **error**, on an input where upstream panics.

Nothing returns a different value and nothing panics. (1) and (2) hold because
`parse_named_tuple` returns `Option` and **every** failure in it returns
`None` — the splitter, a missing name token, a name that is not an identifier, a
type that does not parse, a type whose text the crate re-renders differently, an
empty argument list — and on `None` `parse_tuple` calls the untouched
`parse_inner_types` and returns its result. `NamedTupleElements::new`'s only
reachable rejection is the empty list: the two vectors are pushed in lockstep,
so unequal lengths cannot arise. (3) is the set §4 converts.

One panic class is left and this patch does not change it: `DataTypeNode::new`
is recursive, so a deeply enough nested input overflows the stack, before and
after, and a stack overflow is an abort rather than a catchable panic.

### What the result is where it changes

Write an accepted input as `Tuple( w1 a1 , w2 a2 , ... , wk ak )` where each
`wj` is the maximal run of ASCII whitespace before argument `j` and each `aj`
begins with a non-whitespace byte. Then every `aj` is reproduced **byte for
byte** in `Display`'s output, in order, and the only difference from the input
is that `w1` is dropped and every later `wj` becomes one space. So the output
equals the input exactly when `w1` is empty and every later `wj` is a single
space — which is the form `DataTypeTuple::doGetName` emits. **On any string the
server's `getName()` produced, the round trip is the identity**, which is what
an insert header needs. On the pretty form `DESCRIBE TABLE` returns, where each
`wj` is a newline and four spaces, it is the pretty-to-compact conversion.

**Byte identity on an arbitrary accepted input is not claimed, and is false**:
`Tuple(a Int64,b String)` is accepted and renders `Tuple(a Int64, b String)`.
Whitespace **inside** an argument is not canonicalised either; it is refused,
and the tuple takes upstream's path, because the type half carries the
whitespace and then either fails to parse or re-renders without it.

### What is not read as named, and what that costs

- **An element whose type text the crate re-renders differently.** Measured
  members: `Enum8('b' = 2, 'a' = 1)` → `Enum8('a' = 1, 'b' = 2)` (`Display`
  sorts by index), `JSON(max_dynamic_paths=8)` → `JSON` (`parse_json` drops
  parameters), `Time64(3, 'UTC')` → `Time64(3)` (`parse_time64` keeps only the
  precision). This costs nothing that worked before: an insert whose header
  carried `Display`'s string for such a type would be refused by the server, so
  the patch turns a server refusal into a client-side parse error, and on the
  read path it is a refusal to read a column that does not read today either.
- **The pretty form of a tuple nested inside a named tuple.** The inner
  newlines survive into the element's type text, so the re-rendering test fails.
  The compact form of the same type reads normally, so only `DESCRIBE TABLE` on
  such a column is affected.
- **One diagnostic is worse than it could be.** Because the named reading
  cannot raise an error, ``Tuple(`attrs` Flurble)`` reports
  ``Unknown data type: `attrs` Flurble`` rather than naming `Flurble` alone.
  Reporting the named reading's error instead would buy the better message at
  the cost of a second error path, and the property above is worth more.
- **The parse cost is exponential in the nesting depth of one constructed
  family.** When an argument's name token is itself a dispatch key whose arm
  re-enters `DataTypeNode::new` — `Array`, `Nullable`, `LowCardinality`,
  `Tuple`, `Variant`, `Map` — the named reading parses the type half and, on
  failure, the fallback parses `<key> <type half>`, which re-enters the parser
  on nearly the same text. Each level then roughly doubles the work: counted
  on a simulation of the two arms the family uses, in units of simulated
  parses, 4 at depth 1, 2,930 at depth 10 and 8,212,991 at depth 22, against
  one chain linear in the depth before. Reaching it needs a column whose tuple
  element is named exactly one of
  those six keys at **every** level and whose innermost type the crate cannot
  parse; any other element name makes the fallback a single dispatch miss and
  the cost linear. A type string is parsed once per request and never per row.

## 1. `split_type_arguments` — a new splitter, reached only by the named reading

`src/data_types.rs`.

### What upstream does

`parse_inner_types` tracks parentheses, single quotes and backslash escapes.
It does **not** track back quotes, so an element name containing `,`, `(` or
`'` breaks it. Measured on 0.1.2:

```text
Tuple(`a,b` String)            -> Err("Unknown data type: `a")
Tuple(`a(b` String)            -> Err("Expected at least one inner element in a Tuple from input Tuple(`a(b` String)")
Tuple(`a'b` String, c UInt8)   -> Err("Unknown data type: `a'b` String, c UInt8")
```

All three names are legal: `DataTypeTuple` forbids only empty and duplicate
names.

### Why that is a defect for us

An element name is whatever the sender called a span attribute, and the server
back-quotes any name that is not a bare identifier. A name with a comma in it
is ordinary, and the type string it produces has to parse.

### The change

A new private function, reached only from `parse_named_tuple`. It is upstream's
state machine with three changes — a single-quoted region is opaque only while
no back quote is open, a back-quoted region is opaque the same way, and the
parenthesis counter and the top-level comma are read only outside both — and at
a top-level comma it pushes a trimmed `&str` slice of the input instead of
calling `DataTypeNode::new` on it. The final push stays conditional on every
parenthesis being closed, as upstream's is. Upstream's `", "` skip is dropped
and each argument is `trim_start`-ed of ASCII whitespace instead, which is what
reads the pretty form, whose separator is a comma, a newline and four spaces.
Trailing whitespace is **not** trimmed.

### Why `parse_inner_types` is not edited instead

**Because making the shared scanner back-quote aware changes the value of an
input the crate accepts today.** A tuple holding an unpaired back quote
outside any element name — ``Tuple(Array Time`x, UInt8)`` — answers
`Ok("Tuple(Array(Time), UInt8)")` on 0.1.2. An unpaired back quote outside any
quoted region sets the scanner's flag and nothing clears it, so the top-level
comma after `x` stops being a split point; the single argument is then
``Array Time`x, UInt8``, and **that argument parses** — its `Array` payload
slice is ``Time`x, UInt`` and the `Time` arm's `starts_with` is unanchored — so
the value becomes `Ok("Tuple(Array(Time))")`, one element short, silently. A
changed value rather than an error, which is the worse of the two. Upstream's
scanner is therefore untouched; its only change is §4's two checked indices,
which are on lines that never run.

What that buys, exactly: `parse_map` and `parse_variant` call
`parse_inner_types` and nothing else, so **no `Map` or `Variant` argument list
is split by the new function** — a property of the call graph. It does not
follow that a `Map` or `Variant` result cannot change: the value type of
`Map(String, Tuple(a Int8))` is a `Tuple` string, which re-enters the dispatch
and does reach the new splitter. **Every `Tuple(...)` string reaches it, at any
depth.**

## 2. `split_element_name`, `parse_named_tuple` and the `NamedTuple` representation

`src/data_types.rs`.

### What upstream does

`DataTypeNode` has `Tuple(Vec<DataTypeNode>)` and nothing else, and the
dispatch's `Tuple` arm hands the whole argument list to `parse_inner_types`,
which calls `DataTypeNode::new` on each argument whole. `a Int64` matches no
arm, so the error is `Unknown data type: a Int64`.

### Why that is a defect for us

`RowBinaryWithNamesAndTypes` insert-side validation reads the column types from
`DESCRIBE TABLE` before sending anything, so a table with a named-tuple column
cannot be inserted into at all. `trace_landing` and `spans` both declare
`events` and `links` with named elements.

### The change

`NamedTupleElements` holds two private `Vec`s and a checking constructor that
answers `None` unless the two lengths are equal and non-zero, with `names()`
and `types()` as the only readers. The invariant is by construction rather than
by a doc comment: `Display` zips the two, and a shorter name list would
silently drop elements from the string the server compares the insert header
against. `types()` hands out the `&[DataTypeNode]` slice the insert-side
validator's tuple cursor already takes, which a `Vec<(String, DataTypeNode)>`
would not — that would force a second validator kind and a second cursor on the
hot path.

**The names are stored as the type string spells them** — back-quoted and
backslash-escaped exactly when the server back-quoted them. That makes
`Display` a concatenation and the byte-exactness above a property of the data
rather than of a re-quoting routine, which would otherwise have to reimplement
the server's `isValidIdentifier`, its four keyword exclusions and its escaping
and keep them in step. Nothing in the driver reads a tuple element name. A
consumer wanting `a b` from `` `a b` `` must unescape; nothing does.

**The rule, stated once:** an argument is an element name, one space, and a
type whose own rendering reproduces the argument's type text byte for byte;
anything else and the whole argument list falls back to the untouched
`parse_inner_types`. `DataTypeTuple::doGetName` writes
`backQuoteIfNeed(name) << ' ' << elems[i]->getName()`, so that is the shape,
and the re-rendering test is what confirms the type half was read whole rather
than sliced.

A back-quoted name is scanned to the next back quote not preceded by a
backslash, the byte after the closing quote must be an ASCII space, and the
name token keeps both quotes and every escape. A bare name is the argument up
to its first ASCII space and must pass the server's own identifier test: the
first byte an ASCII letter or `_`, every later byte ASCII alphanumeric or `_`.
Neither the server's own refusal of `null` nor its four keyword exclusions —
`distinct`, `all`, `table`, `select`, case-insensitively — is replicated: the
server back-quotes all five, so such a name arrives through the first branch,
and refusing them here would only refuse a hand-written string the existing
code refuses anyway. The identifier test narrows the accepted set to the names
the server emits unquoted; **it is not what makes the reading correct.** A
mistake in it can only accept or refuse a candidate: a refusal falls back, and
an acceptance still has to pass the re-rendering test.

### Why this is not a rule over the text

**A rule that decides from the text alone whether an element is named cannot be
checked, and the reason is measured.** Eleven of the sixteen prefix dispatch
arms slice their payload at a fixed offset one past the key and drop the final
byte without checking it is `)`. So for every one of them, `"<key> <tail>"` has
the key's own payload slice equal to **the tail with its last byte removed**,
because the slice starts one byte past the key and the space takes that byte.
For the five whose payload is itself a type string or a one-element list,
`<key> <tail>` therefore parses exactly when `<tail>` minus its last byte
parses. Compose that with the `Time` arm's unanchored `starts_with("Time")`,
which accepts any input beginning with `Time`, and each of the five yields an
**infinite family** of inputs that parse today. Measured, one member of each:

```text
Tuple(Array Timex, UInt8)             -> Ok("Tuple(Array(Time), UInt8)")
Tuple(Nullable Timex, UInt8)          -> Ok("Tuple(Nullable(Time), UInt8)")
Tuple(LowCardinality Timex, UInt8)    -> Ok("Tuple(LowCardinality(Time), UInt8)")
Tuple(Tuple Timex, UInt8)             -> Ok("Tuple(Tuple(Time), UInt8)")
Tuple(Variant Timex, UInt8)           -> Ok("Tuple(Variant(Time), UInt8)")
Array Int64                           -> Err("Unknown data type: Int6")
```

The last line is the shape of the trap, and it is why a probe over a fixed list
of tails reports these arms as rejecting tails: `Array Int64` is an **error**
while `Array Int64x` is `Ok("Array(Int64)")`. The accepted-tail set of each arm
is defined by re-entering `DataTypeNode::new`, so writing the enumeration down
means writing down the language.

Two further measurements, because they are the ones that choose the mechanism:

```text
Tuple(Timestamp UInt64)                 -> Ok("Tuple(Time)")      on 0.1.2
Tuple(TimeOfDay LowCardinality(String)) -> Ok("Tuple(Time)")      on 0.1.2
```

Both are ordinary named tuples the server can emit, and both parse today as
something else. So a mechanism that preferred upstream's reading whenever
upstream's succeeds would be wrong on them.

One residual follows from the rule rather than from the arms: **if the server
ever has a type whose canonical name begins with an identifier, a space, and
then something that parses**, a tuple all of whose elements have that shape
would be read as named rather than positional. What rules it out is the
identifier test — if a canonical name contains a space at all, the byte run
before the first space contains `(`. Checked over all three routes `getName()`
has: the custom-name route's three implementors (ten fixed names, all bare
words, plus `SimpleAggregateFunction(` and `Nested(`), the nineteen
`doGetName` overrides, and the inherited `getFamilyName()` default, whose 28
overrides each return a literal or a stringified type name with no space in any
of them. Two names carry a top-level space, `Function(T -> U)` and
`Nested(<name> <type>)`, and both fail the identifier test for the same reason.
If a future type does carry one, the insert header is still byte-correct by the
property above, and the only difference is which validator shape the client
picks.

## 3. The `parse_tuple` call site

`src/data_types.rs`, in `parse_tuple`.

### The change

Inside the existing `input.len() > 7` guard, after the payload slice and before
anything else: try the named reading, and on `Some` return
`Ok(DataTypeNode::NamedTuple(..))`. Everything from there to the end of the
function is upstream's, byte for byte, so `parse_tuple` returns exactly
upstream's errors and no others.

Two decisions in that shape:

- **The named reading goes first.** Trying upstream's reading first and falling
  back to the named one would be a stronger no-regression property and the wrong
  answer: `Tuple(Timestamp UInt64)` parses today as `Tuple(Time)`, and a column
  whose tuple element is named `Timestamp` is ordinary. Reading it as a
  positional `Time` sends the server a header it refuses.
- **The named reading cannot raise an error.** That is what makes the property
  above hold, and it costs the one diagnostic recorded there.

`Tuple()` is **not** how an empty argument list arises — the `len > 7` guard
refuses it first, with `Invalid Tuple format`. `Tuple(()` is the input that
reaches it: the splitter's final push is conditional on every parenthesis being
closed, so it yields no argument, and the answer is upstream's
`Expected at least one inner element in a Tuple from input Tuple(()`.

## 4. Nineteen checked byte indices

`src/data_types.rs`, in seventeen functions.

### What upstream does

Thirteen parse functions slice the input at fixed byte offsets without checking
either end is a character boundary, and nothing in the file is checked at all:
`/usr/bin/grep -n 'input\[\|input_bytes\[\|map\[\|str\[\|inner\[\|parsed\[\|\.get('
src/data_types.rs` lists 40 lines on 0.1.2 and **no `.get(` among them**.

### Why that is a defect for us

**`DataTypeNode::new`'s domain is bytes off the wire**, not strings the library
produced: it is called on every column type in a `DESCRIBE TABLE` answer and on
every column type in a response header. A multibyte character at either end
puts an offset inside a character and the slice panics, and the panic unwinds
out of the function through the awaiting task rather than returning, so there is
nothing for the caller to handle.

Measured over a corpus built by inserting one two-byte and one three-byte
character at every character boundary of twenty-one complete types, by
truncating each of them at every byte, and by adding three named `JSON(..)`
inputs:

```text
corpus size: 1073
panicking inputs: 103
distinct panic sites: 16
```

and this patch's §2 is what makes it urgent rather than merely untidy: the named
reading hands `DataTypeNode::new` the **type half** of a tuple argument, which
upstream's code never does. Measured, **none** of those 1,073 inputs panics
today as `Tuple(a <member>)`, so today the sites are reachable only from a
top-level call and the named reading opens a route from a tuple argument to all
sixteen.

### The change

Nineteen indexing expressions become checked. **A checked slice returns the
same slice wherever the unchecked one did not panic, so no accepted value
changes**; what changes is 103 measured inputs, from an abort to an error — an
error and never a value.

| Sites | What they are | What the patch does |
|---|---|---|
| `parse_fixed_string`, `parse_array`, `parse_enum`, `parse_datetime`, `parse_decimal`, `parse_datetime64`, `parse_time64`, `parse_low_cardinality`, `parse_simple_aggregate_function`, `parse_nullable`, `parse_map`, `parse_tuple`, `parse_variant` — one each | a fixed-offset `&input[k..len-1]`, `[k..len-2]` or `[prefix.len()..len-1]` | `input.get(a..b).ok_or_else(\|\| <the function's own trailing error>)?`. `str::get` answers `None` for a non-boundary **and** for `start > end`, which is `parse_simple_aggregate_function`'s second panic: it has no length guard, so `SimpleAggregateFunction(` on its own slices `[24..23]` |
| `parse_datetime64`'s timezone slice | `str[3..str.len()-1]` on a scanner remainder | `str.get(3..str.len() - 1)` and **the arm's error on `None`, not a `map`**. The slice is the timezone, so a `None` folded into `maybe_tz = None` would answer `Ok("DateTime64(3)")` with the timezone silently dropped — a type the server never sent. Seven corpus members reach this slice; all seven return the arm's error |
| `parse_json`'s `map[1]` | after `column.split(' ')` | a `let Some(..) = map.get(1) else { return Err(..) }` guard. `map[0]` stays: `split(' ')` yields at least one element on any string, the empty one included |
| `parse_inner_types` ×2, `parse_enum_values_map` ×1 | a `&str` slice **inside an error message**, formatted after `String::from_utf8` has just failed on the same bytes | `String::from_utf8_lossy` of the bytes already in hand, same extent. The `parse_enum_values_map` one is measured panicking; the two in `parse_inner_types` are dead — the byte slice is cut at an ASCII comma and so never fails `from_utf8` |
| `remove_json_header`'s `input[5..]` | | `input.get(5..).ok_or_else(..)?`. No input reaches the `None` branch: it is in bounds and on a boundary because its one caller is reached by `starts_with("JSON(")`, five ASCII bytes — but the function's own guard tests four, so its safety is a property of its caller. Checked anyway |

**Which error a checked slice returns, so that it is not a judgement.** "The
function's own trailing error" is the message its own trailing `Err` carries —
the one it returns today when its length guard fails — and the same message
serves every `None` that function's new `get` can produce. Each of the thirteen
in the first row ends in exactly one trailing `Err` whatever else it raises on
the way; `parse_datetime64`'s precision message is not it.
`parse_simple_aggregate_function` has no length guard and one message of any
kind, so it takes that.

### Why no further panic can remain

Three parts, and none of them is a reading of the arms.

1. **The reachable code is closed by the import list.** `data_types.rs` imports
   `crate::error::TypesError`, `std::collections::HashMap` and `std::fmt`, so
   everything `DataTypeNode::new` can reach is that file, `error.rs` and `std`.
   `error.rs` holds no index, no arithmetic and no panicking call.
2. **In safe Rust the panic sources are a closed set, and each one is a sweep
   over the file.** Indexing and slicing: the command above, which over the
   **patched** file also lists this patch's own new lines. Each of those is
   either a `get` or bounded in the same expression or by the enclosing loop
   condition: `bytes[i]` and `input_bytes[i]` sit inside
   `while i < <that slice>.len()`; `bytes[0]` is behind `!bytes.is_empty() &&`
   in the same expression and `bytes[1..]` behind the same test; and the two
   `&input_bytes[from..]` in the splitter's error closure take a start that is
   a comma's index plus one, so at most the length, where a byte slice is
   empty rather than out of bounds. `unwrap` /
   `expect` / `panic!` / `assert*!` / `unreachable!` / `todo!` /
   `unimplemented!` / `unsafe`: a grep over the non-test lines returns
   **nothing**. Division and remainder: **nothing**. Arithmetic overflow, which
   panics under `debug-assertions`: `/usr/bin/grep -nE '\+=|-=' ` over the
   non-test lines lists **fourteen** sites, nine upstream's and five this
   patch's two new functions'; each is a `+=` or `-=` executed at most once per
   input byte, so an overflow needs an input of at least 2^31 bytes.
   What is left is `std`, where `String::from_utf8` and `str::parse` return
   `Result` and `Vec::push` and `format!` abort on allocation failure rather
   than panic.
3. **The corpus is the check, not the enumeration.** It reaches 16 of the 19
   changed sites; the generated part alone reaches 15, because no truncation or
   insertion of a valid `JSON(a Int8)` has a parameter without a space in it,
   which is why `parse_json`'s three inputs are named rather than generated. A
   finite corpus cannot enumerate; the sweep carries the claim, which is why the
   re-vendor rule above names the sweep.

## 5. `Display`'s `NamedTuple` arm

`src/data_types.rs`, beside the `Tuple` arm.

### The change

`Tuple(`, then each name, a space and the element's own rendering, joined by
`", "`, then `)`. Compact by construction, because that is what the server's
own `getName()` emits and the insert header is compared against it byte for
byte: a mismatch is refused with `Code: 117`, `Type of 'events' must be …, not
…`, before any row is read. `DESCRIBE TABLE` returns the **pretty** name
instead — `print_pretty_type_names` defaults to `true` — so the round trip is
deliberately not an identity on a `DESCRIBE` string: pretty in, compact out.

This arm is the one edit the compiler demands. `Display`'s match has no
wildcard; every other reader of the enum either names specific variants with a
fallthrough or is a `==`/`matches!` test that is simply false for a new variant,
and `#[non_exhaustive]` plus a fallthrough means no error and no warning. The
one that needed a change and said nothing is the insert-side validator, in the
other vendored crate.

## What to say upstream

Two separable reports, and the second is the stronger one.

**The named tuple.** The type parser cannot parse a named tuple at any depth
(`Tuple(a Int64, b String)` → `Unknown data type: a Int64`, on 0.1.2 and on
0.1.3), so `RowBinaryWithNamesAndTypes` insert-side validation fails for any
table with a named-tuple column, and `DESCRIBE TABLE` returns the pretty form so
the element strings arrive with a leading newline and indentation. Do **not**
offer a grammar rule as the fix; offer the shape — try the named reading, accept
it only when the element type's own rendering reproduces its text, fall back to
the existing code otherwise — with `Tuple(Array Timex, UInt8)` and
`Tuple(Timestamp UInt64)` as the two inputs that settle why. The verbatim
versus unescaped names of §2 is the one judgement call; upstream may prefer the
logical name with a re-quoting renderer, which is a larger change and owes
`isValidIdentifier`, the four keyword exclusions and the escaping.

**The checked indices.** `DataTypeNode::new` aborts its caller on 103 inputs out
of a 1,073-input corpus, at 16 distinct sites, with `Array(UInt8)é` and
`JSON(x)` as two-word reproductions. The inputs arrive from a response header,
so the function's domain is bytes off the wire rather than strings the library
produced. That is worth sending on its own, before and independently of named
tuples, and it needs no judgement call.
