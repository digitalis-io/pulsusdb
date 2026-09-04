# Where the reference implementations are wrong, and what we do instead

PulsusDB is built to answer LogQL, PromQL and TraceQL queries the way
Grafana Loki, Prometheus and Grafana Tempo answer them. The standing rule
is parity **except where the reference is wrong**. This file is the list
of those exceptions: every place we found the reference behaving
incorrectly on its own terms and deliberately did not copy it.

**This is not the list of all our differences.** Most of the ways
PulsusDB differs from the three reference systems come from running Rust
instead of Go, from the defaults of the libraries we use, or from storing
data in ClickHouse instead of in their own file formats. Those are
recorded in the divergence ledgers and do not belong here.

## What "wrong" means here

A behaviour is listed below only if it fails at least one of these four
tests. Each entry names the test or tests it meets.

| code | test |
|---|---|
| **A** | **Internally inconsistent** — it works something out one way and then uses it as if it had been worked out another way, or two of its own paths answer the same question differently. |
| **B** | **Contradicts its own documentation or its own code comments.** |
| **C** | **Silently corrupts or fabricates data** — stores a value nobody sent, wraps a counter round, or drops part of an input without saying so. |
| **D** | **Produces a result that no reading of its stated intent supports.** |

Being coarser, slower, differently designed, or simply another reasonable
choice is **not** wrong, and none of that is here.

## What was read, and at which version

Every claim below was checked against the reference's own source at the
version PulsusDB is built against, not taken from the ledger row that
records it. File and line numbers are from these trees.

| product | version | commit | how it is pinned here |
|---|---|---|---|
| Grafana Loki | v3.7.4 | `b318f2829f0ae2094ab3a1e90780450e9e4b03be` | `.github/workflows/ci.yml`, image `grafana/loki@sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc` |
| Grafana Loki (end-to-end oracle) | 3.4.2 | `4fa045d3807f4de0543b06e6ce79b89afb741adc` | `deploy/e2e/compose.single.yaml`, image `grafana/loki:3.4.2@sha256:58a6c186ce78ba04d58bfe2a927eff296ba733a430df09645d56cdc158f3ba08` |
| Grafana Tempo | v3.0.2 | `0c4b926d09234186de39833e9c7ecb5b7614c8b9` | `deploy/e2e/compose.single.yaml`, image `grafana/tempo:3.0.2@sha256:cda87c212d8c584dc0b89e337e7ed648a5100feb657e5d528480ee4fa03dbbe3` |
| Prometheus | v3.13.0 | `40af9c2cdc0eda00f3622e867a27f6359f7295f3` | `deploy/e2e/compose.single.yaml`, image `prom/prometheus:v3.13.0` |

Where a line number below sits in a `vendor/` path, it is a library the
reference ships inside its own binary, so it is the reference's behaviour
even though it is not the reference's code.

**This file explains and indexes. The ledgers hold the evidence.** Every
entry links to the ledger row that carries the full measurement, the
fixtures and the tests. Nothing is moved out of a ledger, and nothing here
replaces one.

## The list

| # | product | surface | test | what goes wrong for a user |
|---|---|---|---|---|
| [1](#1-a-query-with-a-typo-in-it-answers-no-logs-matched) | Loki | queries | A, D | A query with a mistake in it answers "no logs matched" instead of "your query is broken", if the time range ends more than three hours ago. |
| [2](#2-asking-for-more-fields-returns-fewer-fields) | Loki | queries | C | Asking for 4,294,967,296 fields returns zero fields. Asking for one returns one. |
| [3](#3-a-response-key-appears-or-disappears-depending-on-how-many-shards-answered) | Loki | queries | A | The `jsonPath` key is in the answer on a one-machine deployment and gone on a bigger one, for the same query and the same data. |
| [4](#4-a-range-query-returns-points-outside-the-range-it-was-asked-for) | Loki | queries | A | A graph gets data points from before the start time and after the end time the client asked for. |
| [5](#5-a-size-threshold-is-compared-at-display-precision) | Loki | queries | A | `\| size >= 1KiB` is parsed as 1024 bytes and then compares against 1000. |
| [6](#6-a-large-offset-moves-the-window-to-an-unrelated-instant) | Loki | queries | C | A very large `offset` silently relocates the query window instead of failing. |
| [7](#7-a-variants-query-reports-a-number-that-is-simply-wrong) | Loki | queries | D | A `variants(...)` query reports 2 where the true answer is 58. |
| [8](#8-a-range-variants-query-at-exactly-the-limit-loses-the-whole-variant) | Loki | queries | A | At exactly 500 series a range `variants(...)` query drops a whole variant that the same query as an instant query serves. |
| [9](#9-a-label-name-the-parser-accepts-is-reprinted-as-a-query-the-parser-refuses) | Loki | queries | A | A query using a non-English label name is accepted, then fails inside the system with a parse error, or never answers at all. |
| [10](#10-one-encoder-repairs-a-character-the-next-encoder-refuses) | Loki | queries | A | The same valid result is served on one route and returns 500 on another, because two response encoders disagree about one character. |
| [11](#11-the-same-rejected-name-is-a-500-on-one-endpoint-and-a-400-on-the-other) | Loki | ingest | A | A log shipper retries forever on a body that can never be accepted. |
| [12](#12-a-logfmt-extraction-is-skipped-because-the-check-is-applied-to-the-wrong-name) | Loki | queries | A | `sum by (a) (... \| logfmt a="b_c" ...)` reports an empty `a` for a line that carries `b.c=4`. |
| [13](#13-when-two-extractions-name-one-source-key-a-coin-flip-decides-which-gets-the-value) | Loki | queries | D | `\| logfmt a="x", b="x"` puts the value in `a` on one run and in `b` on the next, from the same line. |
| [14](#14-a-live-tail-with-no-filter-silently-drops-a-stream-label) | Loki | queries | A, C | Live tail rows are missing a label that the same query's catch-up rows keep. |
| [15](#15-whether-a-push-is-accepted-depends-on-how-the-client-split-it-into-tcp-writes) | Loki | ingest | D | The same bytes are accepted from one client and rejected from another. |
| [16](#16-the-operator-swaps-its-operands-on-the-integer-path) | Tempo | queries | A, D | `2 ^ 10` answers 100. `2.0 ^ 10` answers 1024. |
| [17](#17-three-parts-of-the-same-system-read-a-spans-events-three-different-ways) | Tempo | queries | A | A span matches or does not match depending on which of its events happens to be first. |
| [18](#18-a-traces-duration-wraps-round-past-497-days) | Tempo | queries | C | A 60-day trace is reported as 10.3 days — a plausible number that is simply wrong. |
| [19](#19-quantile-exemplars-are-placed-against-numbers-the-series-never-carries) | Tempo | queries | A, B | An exemplar dot can be attached to a percentile line whose drawn value is nowhere near it. |
| [20](#20-an-otlp-point-with-no-value-is-stored-as-zero) | Prometheus | ingest | C | A data point that carries no value is stored as `0`, which reads as a real measurement. |
| [21](#21-a-histogram-is-stored-whose-buckets-cannot-add-up-to-its-own-count) | Prometheus | ingest | C | A histogram is stored with one bucket count silently thrown away and a total that contradicts the buckets. |
| [22](#22-a-fault-that-can-never-be-fixed-by-retrying-is-answered-500) | Prometheus | ingest | B | An OTLP exporter retries a payload forever that can never be accepted. |
| [23](#23-a-grouping-after-a-select-puts-every-span-into-one-group-called-nil) | Tempo | queries | A, D | A query grouping four named spans by their name answers one group called `nil`, in a response that prints each span's name beside it. |
| [24](#24-a-groups-sum-and-avg-over-one-attribute-give-different-answers-depending-on-which-span-arrived-first) | Tempo | queries | C, D | A group's `sum` and `avg` over one attribute give different answers depending on which span arrived first. |

---

## 1. A query with a typo in it answers "no logs matched"

**Loki v3.7.4 · queries · tests A, D**

### What the reference does

Loki decides whether a query is valid in two stages. A plain syntax
error is caught when the query text is parsed. A different class of
mistake — a bad `ip()` pattern, an unterminated `line_format` template, a
duplicated `label_format` target, an unparseable `| json` or `| logfmt`
extraction expression — is only caught later, when the pipeline of
processing stages is **built**.

That build step sits behind a short circuit. When the end of the
requested time range is older than `querier.query_ingesters_within`
(three hours by default), the live ingesters leave the query's path
(`pkg/querier/intervals.go:32-40`) and only the stored chunks are read.
The store then returns an empty iterator as soon as no chunk matches:

```go
// pkg/storage/store.go, v3.7.4
491:	if len(lazyChunks) == 0 {
492:		return iter.NoopEntryIterator, nil
493:	}
...
500:	pipeline, err := expr.Pipeline()
```

The return at line 491 happens before the pipeline is built at line 500,
so the mistake in the query is never noticed.

```
                     3 hours ago                              now
   ---------------------|--------------------------------------|-->
                        |                                      |
   window ends here:    |  window ends here:                   |
   ingesters not asked  |  ingesters asked                     |
   store returns empty  |  pipeline is built                   |
   pipeline never built |  error is raised                     |
        =>  200, no results         =>  400, "invalid pattern"
```

### Why that is wrong rather than different

The same query text gets two different verdicts from the same server
depending only on which dates the user picked (**A**). And an empty
`200` is read by every client and every human as "there is no data",
which is a statement the server has not established and has no reason to
believe (**D**). Loki's own answer to the identical query over a recent
window proves it knows the query is broken.

### What PulsusDB does

`plan()` and `CompiledPipeline::compile` both run before any I/O, so a
malformed query is a `400` in every window. Owner ruling, 2026-08-06.

### Evidence

Measured over six windows on the pinned v3.7.4 container, and over
seventeen query shapes that split into the two classes. Ledger row:
`malformed-query-refused-in-every-window` in
docs/benchmarks/logs-differential-ledger.md. User-facing statement:
docs/features.md.

---

## 2. Asking for more fields returns fewer fields

**Loki v3.7.4 · queries · test C**

### What the reference does

`/loki/api/v1/detected_fields` reads its `limit` as a machine-word
integer, checks it is positive, and then converts it to a 32-bit number
with no range check:

```go
// pkg/loghttp/params.go, v3.7.4
49:func detectedFieldsLimit(r *http.Request) (uint32, error) {
...
61:	if l <= 0 {
62:		return 0, errors.New("limit must be a positive value")
63:	}
64:	return uint32(l), nil
```

A value above 4,294,967,295 keeps only its bottom 32 bits.
`lineLimit` at `:38-46` has the same shape.

### Why that is wrong rather than different

The user asks for more and is given less, with a `200` and no warning
(**C**). Measured on a fixture with 41 fields: `limit=4294967295`
returns 41 fields, `limit=4294967296` returns **0**, and
`limit=4294967297` returns **1**. The same wrap on `line_limit` means
`line_limit=4294967396` is *served*, at the wrapped value, after
`line_limit=4294967295` was rejected as too large.

### What PulsusDB does

The field limit saturates: anything above the maximum runs at the
maximum. `line_limit` refuses before any conversion. Nothing wraps, at
any magnitude. Owner ruling on #253: "someone who asks for more and
receives nothing has been given a wrong answer, not a different one."

### Evidence

Ledger row: `detected-fields-limit-saturates-not-wraps`. Gated by
`parse_field_limit_saturates_where_the_reference_wraps` and
`parse_line_limit_matches_the_reference_atoi_surface` in
`crates/pulsus-server/src/logs_api/params.rs`.

---

## 3. A response key appears or disappears depending on how many shards answered

**Loki v3.7.4 · queries · test A**

### What the reference does

A `/detected_fields` response carries a `jsonPath` for each field. When
one machine answers, the field object is built with it:

```go
// pkg/querier/queryrange/detected_fields.go, v3.7.4
66:					fields[fieldCount] = &logproto.DetectedField{
67:						Label:       k,
68:						Type:        v.fieldType,
69:						Cardinality: v.Estimate(),
70:						Parsers:     p,
71:						JsonPath:    v.jsonPath,
72:					}
```

When several shards answer, the merge rebuilds every field and the key is
not set:

```go
// pkg/storage/detected/fields.go, v3.7.4
92:		detectedField := &logproto.DetectedField{
93:			Label:       field.Label,
94:			Type:        field.Type,
95:			Cardinality: field.Sketch.Estimate(),
96:			Parsers:     field.Parsers,
97:			Sketch:      nil,
98:		}
```

The key then vanishes from the JSON entirely, because it is marked
`omitempty`.

### Why that is wrong rather than different

The same query over the same data returns a different response shape
depending on the size of the deployment (**A**). Nothing marks it
intentional: the neighbouring field, `Parsers`, *is* carried across the
merge, so `JsonPath` reads as an omission from when it was added.

### What PulsusDB does

`jsonPath` is emitted on every response, for every JSON-flattened field,
however the answer was assembled. Dropping a documented, usable field on
some responses and not others would be worse for the client than the
parity break: `jsonPath` is exactly what lets a client turn a detected
field into a working `| json <expr>` selector.

### Evidence

Ledger row: `detected-fields-jsonpath-survives-merge`. Gated by
`logs_detected_live.rs`'s per-field `jsonPath` assertions.

---

## 4. A range query returns points outside the range it was asked for

**Loki v3.7.4 · queries · test A**

### What the reference does

Loki's query frontend cuts a range query into hour-long pieces so it can
run them in parallel. Those pieces have to line up on a grid, so before
the engine sees the request the frontend rewrites the range: the start is
rounded **down** and the end is rounded **up** to whole multiples of
`step`, counted from the Unix epoch.

```go
// pkg/querier/queryrange/splitters.go, v3.7.4
236:	start, end := s.alignStartEnd(r.GetStep(), lokiReq.StartTs, lokiReq.EndTs)
...
308:func (s *metricQuerySplitter) alignStartEnd(step int64, start, end time.Time) (time.Time, time.Time) {
309:	// step align start and end time of the query. Start time is rounded down and end time is rounded up.
```

It is unconditional — it runs even when the query produces a single
piece. The result is handed back as-is; nothing puts it back on the
timestamps the caller asked for.

```
   requested:        |<---------------------------->|
                   start                            end

   grid the frontend uses:
       ...----+----------+----------+----------+----------+----...
              |          |          |          |          |
        floor(start)                                  ceil(end)
              ^                                          ^
              |                                          |
      a point BEFORE the caller's start          a point AFTER the caller's end
```

### Why that is wrong rather than different

Loki's own query engine, `pkg/logql`, evaluates on a grid anchored at the
requested start. The frontend's rewrite contradicts it, and one tenant
setting (`split_queries_by_interval: 0`) switches the rewrite off — so
the same binary answers the same request two different ways depending on
configuration (**A**). Returning samples outside `[start, end]` also
breaks the Prometheus `query_range` contract that this endpoint mirrors.

### What PulsusDB does

Points are emitted on the grid `{start + k·step ≤ end}`, so every point
lies inside the window the caller requested. The step derivation itself
is the reference's own.

### Evidence

Measured on the pinned image: a 501-second window returned **252 points
ending 502 s after the start** there against our **251 ending at 500 s**.
Measured again at the client: Grafana 13.2.0 with datasource plugin
13.1.0 over a six-hour panel sends a step-aligned start, so the only
difference a real user sees is one extra trailing point 7.2 s past the
requested end, drawn off the edge of the panel. Ledger rows:
`frontend-step-alignment` and `range-step-grid-start-anchored`. Owner
ruling 2026-08-12, upheld on challenge.

---

## 5. A size threshold is compared at display precision

**Loki v3.7.4 · queries · test A**

### What the reference does

`| size >= 1KiB` parses `1KiB` as 1024 bytes — `KiB` is one of the units
Loki's own reference lists (`docs/sources/query/log_queries/_index.md:232`
@ v3.7.4), and `humanize.ParseBytes` gives the binary prefix its usual
value. The comparison in
`BytesLabelFilter.Process` uses the parsed value directly
(`pkg/logql/log/label_filter.go:190-202`). But the printed form of that
same filter goes through a human-readable formatter:

```go
// pkg/logql/log/label_filter.go, v3.7.4
217:func (d *BytesLabelFilter) String() string {
218:	b := strings.Map(func(r rune) rune {
...
223:	}, humanize.Bytes(d.Value)) // TODO: discuss whether this should just be bytes, B, to be more accurate.
224:	return fmt.Sprintf("%s%s%s", d.Name, d.Type, b)
```

`humanize.Bytes` formats in powers of a **thousand**, with suffixes
`B, kB, MB, ...`, to one decimal place below ten
(`vendor/github.com/dustin/go-humanize/bytes.go:68-91 @ v3.7.4`). So
`humanize.Bytes(1024)` is `"1.0 kB"`, which parses back as **1000**, and
`humanize.Bytes(1536)` is `"1.5 kB"`, which parses back as **1500**.
Those are exactly the quantization steps that were measured. The query
frontend does print the parsed query and send the text onward
(`pkg/querier/queryrange/shard_resolver.go:268` and `:275`).

### Why that is wrong rather than different

Loki parses `1KiB` as 1024 and then measurably compares against 1000, so
the parse and the comparison disagree about the same value (**A**).
Measured: a 1000-byte line passes
`size >= 1KiB`; `1024B` and `1025B` behave as 1000; `1536B` behaves as
1500; round decimal values like `3kB` compare exactly. Zero-valued
literals are unserveable, and the error names `0B`, a spelling the query
never contained. The formatter's own source comment
("discuss whether this should just be bytes, B, to be more accurate")
records the unease.

The round-trip through `String()` reproduces every measured step and
every observed error spelling arithmetically, but it is **not traced end
to end** — the ledger says so, and this file repeats it. The
measurements are the pinned facts; the round trip is the reading they
support.

### What PulsusDB does

Every accepted literal compares at its parsed value. `1KiB` is 1024.

### Evidence

Ledger row: `byte-literal-render-quantization`. Pinned by
`b8_byte_parity.test`'s 1KiB boundary row, marked as a pinned divergence
rather than a captured container answer.

---

## 6. A large `offset` moves the window to an unrelated instant

**Loki v3.7.4 · queries · test C**

### What the reference does

`offset` shifts the evaluation window once, in plain 64-bit integer
nanoseconds:

```go
// pkg/logql/range_vector.go, v3.7.4
50:	if offset != 0 {
51:		start = start - offset
52:		end = end - offset
53:	}
```

and inverts the shift when emitting (`:195`). Go's signed integer
subtraction wraps round on overflow rather than failing, so a shift that
leaves the representable range lands on an unrelated instant.

### Why that is wrong rather than different

The query is then evaluated over a time window nobody asked for and the
answer is returned with a `200` (**C**). Nothing in the response says the
window moved.

### What PulsusDB does

The same one-shift-at-the-boundary structure, evaluated with checked
arithmetic. When the shift leaves the representable range the query
answers **empty**. It neither wraps nor clamps onto the end of the range.
No new rejection is introduced: a large, negative or out-of-range offset
is a `200` answering nothing, never a `400`.

### Evidence

Ledger row: `offset-domain-edge-exact-arithmetic`, which also enumerates
the residual cases where our empty answer differs from the exact one, and
the five-year span cap that made all but one of them unreachable. Pinned
by one test per branch in
`crates/pulsus-read/tests/logql_metric_agg_golden.rs`.

---

## 7. A `variants(...)` query reports a number that is simply wrong

**Loki v3.7.4 · queries · test D**

### What the reference does

A `variants(...)` query runs several aggregations over one read. Each
result sample is tagged with which variant produced it, by **adding** a
`__variant__` label:

```go
// pkg/logql/log/consolidated_variant_extractor.go, v3.7.4
76:func appendVariantLabel(lbls LabelsResult, variantIndex int) LabelsResult {
77:	newLblsBuilder := labels.NewScratchBuilder(lbls.Stream().Len() + 1)
78:
79:	lbls.Stream().Range(func(l labels.Label) {
80:		newLblsBuilder.Add(l.Name, l.Value)
81:	})
82:
83:	newLblsBuilder.Add(constants.VariantLabel, strconv.Itoa(variantIndex))
84:	newLblsBuilder.Sort()
```

`Add` appends; it does not replace. If the query's common pipeline
already set `__variant__` — with `| label_format __variant__="..."`,
which is ordinary user input — the label set now carries **two**
`__variant__` entries. Samples are later routed to a variant by reading
that label back as a single string
(`p.Metric.Get(constants.VariantLabel)`, `pkg/logql/engine.go:489`).

### Why that is wrong rather than different

The reference answers `200` with a number that is not the answer to the
question (**D**). Measured: a `bytes_over_time` variant reported **2**
where the true value was **58**. A non-integer collision value gives an
empty `200`. Both are stable within a run, so a client has no way to
notice.

### What PulsusDB does

`append_variant_label` **overrides** — the variant index wins — so the
label is single-valued and the values are always correct. The corpus
commits in advance (in `b13_variants.test`'s header, not afterwards) to
setting no `__variant__` in a common pipeline, and the override is gated
without a container.

### Evidence

Ledger row: `variants-label-collision-and-fanout-bounds`, part (a).

---

## 8. A range `variants(...)` query at exactly the limit loses the whole variant

**Loki v3.7.4 · queries · test A**

### What the reference does

Loki has two nearly identical functions that fold sample vectors into
series under a limit. The plain one checks the limit **only when the
series is new**:

```go
// pkg/logql/engine.go, v3.7.4 — vectorsToSeriesWithLimit
449:		series, ok = sm[hash]
450:
451:		// create a new series if under the limit
452:		if !ok && !limitExceeded {
453:			// Check if adding a new series would exceed the limit
454:			if maxSeries > 0 && len(sm) >= maxSeries {
```

Its variants sibling tests the limit **before** looking the series up at
all, and deletes the entire variant when it trips:

```go
// pkg/logql/engine.go, v3.7.4 — multiVariantVectorsToSeries
500:		if len(sm[variantLabel]) >= maxSeries {
501:			skippedVariants[variantLabel] = struct{}{}
...
505:			delete(sm, variantLabel)
...
507:			continue
508:		}
509:
510:		series, ok = sm[variantLabel][hash]
```

So a point belonging to a series that has **already been counted**
deletes the whole variant. That needs at least two grid points, because
the delete can only fire when a later point revisits a counted series.

```
   grid point 1:  500 distinct series arrive -> variant now holds 500
   grid point 2:  the first of those series comes back
                  len(sm[variant]) >= 500  ->  variant deleted
                  (the sibling function would have found it already present
                   and appended to it)

   instant query = one grid point   ->  500 series served
   range query   = two grid points  ->  variant skipped with a warning
```

### Why that is wrong rather than different

The reference's own instant path serves 500 series and its own range path
skips them, from the same limit, on the same data (**A**), and the guard
that would fix it is present in the sibling function immediately above it
in the same file.

### What PulsusDB does

Applies "more than 500" uniformly, instant and range alike. Every query
the reference serves, we serve; we additionally serve a range variant of
exactly 500 series. That is an over-acceptance in the safe direction.

### Evidence

Measured on the pinned container over 501 groups: at 500 series, instant
is served and `range 60s→120s step 30s` is skipped, while
`range 60s→60s step 30s` — a single grid point — is served. Ledger row:
`variants-label-collision-and-fanout-bounds`, part (d). Gated by
`range_variant_at_the_cap_is_served_where_the_reference_skips_it`.

---

## 9. A label name the parser accepts is reprinted as a query the parser refuses

**Loki v3.7.4 · queries · test A**

### What the reference does

Loki's LogQL scanner accepts any Unicode letter in an identifier, so
`{éx="m"}` tokenises. The query frontend then prints the parsed query
back out as text and passes that text onward
(`pkg/querier/queryrange/shard_resolver.go:268`, `:275`). The printer for
a label matcher quotes any name outside `[A-Za-z_][A-Za-z0-9_]*`:

```go
// vendor/github.com/prometheus/prometheus/model/labels/matcher.go, v3.7.4
86:	if m.shouldQuoteName() {
87:		b.Write(strconv.AppendQuote(b.AvailableBuffer(), m.Name))
...
97:func (m *Matcher) shouldQuoteName() bool {
98:	for i, c := range m.Name {
99:		if c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (i > 0 && c >= '0' && c <= '9') {
100:			continue
101:		}
102:		return true
```

That produces `{"éx"="m"}`, and LogQL's grammar has no production for a
quoted matcher name.

### Why that is wrong rather than different

A system that prints its own parsed query into text its own parser
refuses is inconsistent with itself (**A**). The proof that it is the
round trip and not the lexer is that `{"éx"="m"}` — the printed form,
typed by hand — returns the byte-identical error at the identical column
as `{éx="m"}`: `parse error at line 1, col 2: syntax error: unexpected
STRING, expecting IDENTIFIER or }`.

Two further positions in the same family were measured:

- `{app="x"} | éx="v"` — the rewritten query reaches the querier, returns
  `500`, and the frontend retries it. Across five probes: one `500` after
  28 s and four with no HTTP status at all after ~37 s each, with the
  container log showing `try=0` through `try=4` and then
  `failed to enqueue request`.
- `{app="x"} | logfmt éx="b"` with a matching stream — a `500`,
  `could not write JSON response: 1:75: parse error: unexpected
  character inside braces: 'é'`, because the response encoder re-parses
  the label set it has just rendered. See entry 10, which is the same
  class at a different call site.

The extraction destination itself is fine on both sides: the reference
validates the identifier rather than sanitising it
(`pkg/logql/log/parser.go:518`), and PulsusDB matches that.

### What PulsusDB does

Serves all three. Owner ruling, 2026-08-10: adopting a
`shouldQuoteName`-shaped refusal would be reproducing a defect on purpose.

### Evidence

Ledger row: `lexer-identifier-charset`, residuals 1 and 2 and the third
recorded behaviour. Censused as `over-acceptance` rows in
`crates/pulsus-logql/tests/case_folding.rs`.

---

## 10. One encoder repairs a character the next encoder refuses

**Loki v3.7.4 · queries · test A**

### What the reference does

Loki has two paths that turn a stream's labels into a response. One
replaces the Unicode replacement character (U+FFFD) with a space, and
says why in its own comment:

```go
// pkg/util/marshal/query.go, v3.7.4
25:var (
26:	// The rune error replacement is rejected by Prometheus hence replacing them with space.
27:	removeInvalidUtf = func(r rune) rune {
28:		if r == utf8.RuneError {
29:			return 32 // rune value for space
30:		}
31:		return r
32:	}
33:)
...
87:func NewStreams(s logqlmodel.Streams) (loghttp.Streams, error) {
...
92:		if strings.ContainsRune(stream.Labels, utf8.RuneError) {
93:			stream.Labels = string(bytes.Map(removeInvalidUtf, []byte(stream.Labels)))
94:		}
```

The other re-parses the label string with the Prometheus metric parser
and fails:

```go
// pkg/util/marshal/query.go, v3.7.4 — encodeStream
416:	lbls, err := parser.NewParser(parser.Options{}).ParseMetric(stream.Labels)
417:	if err != nil {
418:		return err
419:	}
```

### Why that is wrong rather than different

The same valid query over the same data is served or refused depending on
which encoder runs (**A**), and one of the two encoders already knows the
repair is needed. Measured:

- `| json o="o"` over `{"o":{"k":"x\u{FFFD}y"}}` is `HTTP 500 could not
  write JSON response: 1:4: parse error: invalid UTF-8 rune` under the
  default frontend encoding, and `200` with the character mapped to a
  space under protobuf frontend encoding. One setting, two answers, and
  neither is a rejection the user could have anticipated.
- On `/loki/api/v1/tail`, a U+FFFD in a stream label produces **no
  frame**: WebSocket close `1011`, `could not write JSON tail response:
  1:41: parse error: invalid UTF-8 rune`. The stream is killed.
- At ingest, `{a_b="1", a_b="p�"}` is a `204` push whose read is a
  `500`: `failed to parse series labels to categorize labels: 1:6: parse
  error: invalid UTF-8 rune`. The write path stores what its own read
  path cannot serve. (This third site is measured; its call site was not
  read.)

### What PulsusDB does

Answers `200` with the value in every case. We follow the repairing
branch — the one whose own comment states the reason — on the query
response, and keep tail label bytes verbatim.

### Evidence

Ledger rows: `template-output-budget` (the `query_range` and `tail`
tables), `json-nonvalidating-scan-residual` (the configuration split),
and `structured-metadata-collision-resolution` residual B (the ingest
case). Pinned by
`crates/pulsus-server/tests/logs_utf8_substitution_live.rs`.

---

## 11. The same rejected name is a 500 on one endpoint and a 400 on the other

**Loki v3.7.4 · ingest · test A**

### What the reference does

A structured-metadata name that normalises to nothing is refused. On
`POST /otlp/v1/logs` that is a `400`. On `POST /loki/api/v1/push` it is a
`500`, for the identical input.

The reason is visible in the source. The normalisation error escapes the
per-stream validation loop as a bare error with no attached HTTP status:

```go
// pkg/distributor/distributor.go, v3.7.4
693:					validationErrors.Add(err)
694:					continue
...
702:				for _, lbl := range entry.StructuredMetadata {
703:					normalized, err = labelNamer.Build(lbl.Name)
704:					if err != nil {
705:						return err
706:					}
```

Lines 693 and 694 are a **sibling** failure in the same loop: it is
collected into `validationErrors`, which the distributor classifies as a
client error and answers `400`. Line 705 returns the error straight out
of the loop instead, carrying no status of any kind.

The HTTP layer then cannot classify it:

```go
// pkg/distributor/http.go, v3.7.4
162:	resp, ok := httpgrpc.HTTPResponseFromError(err)
163:	if ok {
...
172:		errorWriter(w, body, int(resp.Code), logger)
173:	} else {
...
181:		errorWriter(w, err.Error(), http.StatusInternalServerError, logger)
182:	}
```

### Why that is wrong rather than different

Two of the reference's own transports answer the same condition
differently (**A**), and the `500` is accidental rather than designed:
every sibling failure in that same loop goes through `validationErrors`
and comes out a `400`. The provenance agrees — the `if err != nil {
return err }` arrived in a mechanical dependency bump, adding the minimum
needed to compile once `Build` grew an error return. Before it, the same
input produced no error at all.

The practical cost is real: `5xx` is exactly the class log agents retry,
so a well-behaved shipper retries an unfixable body forever and books it
against server-error budgets.

### What PulsusDB does

`400` on both transports. The response body is byte-identical to the
reference's, terminating newline included.

### Evidence

Ledger row: `inadmissible-label-name-status`. Gated by
`loki_inadmissible_structured_metadata_name_is_400_on_both_encodings` and
`otlp_inadmissible_attribute_key_returns_400_with_status_code_3`.

---

## 12. A logfmt extraction is skipped because the check is applied to the wrong name

**Loki v3.7.4 · queries · test A**

### What the reference does

`| logfmt a="b_c"` means "read the line key `b_c` and call the result
`a`". Loki keeps a map from the **new** name to the **source** key, and
seeds the new name with an empty value before reading the line:

```go
// pkg/logql/log/parser.go, v3.7.4
544:	keys := make(map[string]string, len(l.expressions))
545:	for id, paths := range l.expressions {
546:		keys[id] = fmt.Sprintf("%v", paths...)
547:		if !lbs.BaseHas(id) && !lbs.HasInCategory(id, StructuredMetadataLabel) {
548:			lbs.Set(ParsedLabel, id, "")
549:		}
550:	}
552:	// alwaysExtract checks whether a key should be extracted regardless of other
553:	// conditions.
554:	alwaysExtract := func(key string) bool {
555:		// Any key in the expression list should always be extracted.
556:		_, ok := keys[key]
557:		return ok
558:	}
```

As an optimisation, a key is only read if the rest of the query needs it:

```go
// pkg/logql/log/parser.go, v3.7.4
580:			if !alwaysExtract(sanitized) && !lbs.ParserLabelHints().ShouldExtract(sanitized) {
581:				return "", false
582:			}
```

and the rename from source key to new name happens **after** that check:

```go
// pkg/logql/log/parser.go, v3.7.4
594:		for id, orig := range keys {
595:			if key == orig {
596:				key = id
597:				break
598:			}
599:		}
```

`ShouldExtract` answers true only for a label the rest of the query
requires (`pkg/logql/log/parser_hints.go:73-85`). `alwaysExtract` looks
its argument up in `keys`, which is keyed by the **new** name. So the
line key `b_c` matches neither: it is not a new name, and it is not
itself required — only `a` is.

### Why that is wrong rather than different

The optimisation asks "does the query need `b_c`?" when the question it
must answer is "does the query need what `b_c` becomes?" (**A**). The
seeded empty value then survives to the result, so the query returns a
label with an empty value for a line that carries the data.

Measured on the pinned container, line `b=1 a-b=2 x=3 b.c=4`:

| query | reference | PulsusDB |
|---|---|---|
| `sum by (a) (count_over_time(… \| logfmt a="b_c" [5m]))` | `{a=""}` | `{a="4"}` |
| `sum by (a,x) (count_over_time(… \| logfmt a="x" [5m]))` | `{a="3"}` | `{a="3"}` |

The second row is the control: adding the source key to the `by` clause
makes it required, `ShouldExtract` returns true, and the reference then
reads and renames it correctly. So the divergence is exactly the hint,
not the rename, not the seed and not the sanitiser.

### What PulsusDB does

Extracts every key, always, so the value is present whatever the rest of
the query needs.

### Evidence

Ledger row: `logfmt-expression-parser-hints-unmodelled`, which also
records what closing it on our side would cost (threading the required
label set into every parser stage, which changes the per-row cost model
for all of them).

---

## 13. When two extractions name one source key, a coin flip decides which gets the value

**Loki v3.7.4 · queries · test D**

### What the reference does

`| logfmt a="x", b="x"` asks for the same source key under two names. The
reference picks one by walking a Go map:

```go
// pkg/logql/log/parser.go, v3.7.4
594:		for id, orig := range keys {
595:			if key == orig {
596:				key = id
597:				break
598:			}
599:		}
```

Go deliberately randomises map iteration order on every pass.

### Why that is wrong rather than different

This is not an unspecified *order* in a list — it decides **which label
holds the data** (**D**). The same query against the same line answers
`{a="3", b=""}` sometimes and `{a="", b="3"}` other times, both `200`,
with nothing to indicate the answer is one draw of two.

Measured on the pinned container, one line `x=3 y=4`, each query issued
30 times:

| query | outcome A | outcome B | split |
|---|---|---|---|
| `a="x", b="x"` | `{a="3", b=""}` | `{a="", b="3"}` | 29 / 1 |
| `b="x", a="x"` | `{a="", b="3"}` | `{a="3", b=""}` | 21 / 9 |
| `a="x", b="y", c="x"` | `{a="3", b="4", c=""}` | `{a="", b="4", c="3"}` | 23 / 7 |

The splits do not converge across runs and are not the point. The point
is that both outcomes occur for every shape.

A repeated *identifier* (`a="x", a="y"`) is a different case: the
reference resolves it deterministically at construction
(`parser.go:521`, last declaration wins) and PulsusDB copies that exactly.

### What PulsusDB does

Query order. The first-declared name whose source key matches wins, so
the answer is predictable from the text the user typed and is the same on
every repeat, every replica and every version.

### Evidence

Ledger row: `logfmt-expression-duplicate-source-key-tiebreak`. Gated by
`two_identifiers_sharing_a_source_key_are_broken_by_query_order`.

---

## 14. A live tail with no filter silently drops a stream label

**Loki v3.7.4 · queries · tests A, C**

### What the reference does

When a tail query carries no filtering pipeline, the ingester skips
processing entirely and hands back the raw stream:

```go
// pkg/ingester/tailer.go, v3.7.4
181:func (t *tailer) processStream(stream logproto.Stream, lbs labels.Labels) []*logproto.Stream {
182:	// Optimization: skip filtering entirely, if no filter is set
183:	if log.IsNoopPipeline(t.pipeline) {
184:		return []*logproto.Stream{&stream}
185:	}
```

That skip also skips the step that renames a metadata key colliding with
a stream label. So the rendered frame loses the stream label and carries
the colliding metadata key unrenamed.

### Why that is wrong rather than different

The condition is exactly "no pipeline **and** delivered live". The same
query, delivered as catch-up rather than live, keeps the label and
renames the metadata key correctly — measured in all eight cells of the
pipeline × delivery-path × header grid. So the reference contradicts half
its own behaviour (**A**), and the difference is a label being dropped
from the answer with nothing to say so (**C**).

Adding one line filter to the query restores the label, which is what
isolates the pipeline as the variable rather than the delivery path.

### What PulsusDB does

Categorises uniformly on every tail query and every row age, so we answer
what the reference's own catch-up path answers. A response whose shape
changes because of an unrelated detail of the query is not a contract.

### Evidence

Ledger row: `categorize-tail-noop-pipeline`, with its witness, contrast
and control probes named. Close condition: the reference stops
short-circuiting the no-pipeline case on the live path.

---

## 15. Whether a push is accepted depends on how the client split it into TCP writes

**Loki v3.7.4 · ingest · test D**

### What the reference does

A JSON push body may contain values under keys the decoder does not read.
Those are skipped. For a run of digits, the skip is a scan over the
decoder's **current read buffer** only:

```go
// vendor/github.com/json-iterator/go/iter_skip_strict.go, jsoniter v1.1.12
24:func (iter *Iterator) trySkipNumber() bool {
25:	dotFound := false
26:	for i := iter.head; i < iter.tail; i++ {
...
57:	}
58:	return false
```

If the number runs past the end of that buffer the function returns
`false`, and the caller then *parses* the number and range-checks it
against a 64-bit float:

```go
// vendor/github.com/json-iterator/go/iter_skip_strict.go, jsoniter v1.1.12
10:func (iter *Iterator) skipNumber() {
11:	if !iter.trySkipNumber() {
12:		iter.unreadByte()
...
16:		iter.ReadFloat64()
```

The buffer is 512 bytes. Where its boundary falls depends on the byte
offset of the token in the body and on how the client chunked its writes.

```
   read buffer:  [.................512 bytes.................]
                                                     |
   case 1   ... "k": 99999999999999999999 ...        |     token ends inside
                 -> trySkipNumber succeeds -> skipped, never evaluated -> 204

   case 2   ... "k": 9999999999999999999999999999999|999... token crosses
                 -> trySkipNumber fails -> parsed -> out of range -> 400

   Same bytes. Different TCP framing. Different verdict.
```

### Why that is wrong rather than different

A JSON decoder's verdict on a document must be a function of the
document. Here it is a function of the transport (**D**). Measured on the
pinned container with the identical bytes: 400 digits written in one
piece is `204`, in 512-byte chunks is `204`, and in 256-byte chunks is
**`400`**. Moving the same run from byte offset 111 to 112 flips it the
same way.

### What PulsusDB does

A run of digits with at most one dot, under a key we do not read, is
accepted whatever its length. Our acceptance surface stays a function of
the request.

**The boundary of this entry is worth stating.** There is a sibling quirk
in the same file — a token whose first byte is `0` is range-checked
against a 32-bit float rather than a 64-bit one
(`vendor/github.com/json-iterator/go/iter_skip.go:83-87`), so `0.35e39`
is refused and `3.5e38` and `-0.35e39`, the same magnitude, are accepted.
That one is deterministic on the request, so PulsusDB **copies** it
rather than listing it here. Only the framing-dependent half is a defect
we decline.

### Evidence

Ledger row: `ingest-label-bounds`, residual 10, with an eleven-row
framing and offset table and a raw-socket probe harness
(`number_route_probe.py`) that writes each body in four framings at three
offsets and exits non-zero on a cell that moves.

---

## 16. The `^` operator swaps its operands on the integer path

**Tempo v3.0.2 · queries · tests A, D**

### What the reference does

TraceQL's power operator has two implementations. The integer one
transposes its arguments at the call site:

```go
// pkg/traceql/ast_execute.go, v3.0.2
485:		case OpPower:
486:			return NewStaticInt(intPow(rhsN, lhsN)), nil
...
940:func intPow(base, exp int) int {
941:	return int(math.Pow(float64(base), float64(exp)))
942:}
```

(the same transposition again in the array-element path at `:741`). The
float catch-all does not:

```go
// pkg/traceql/ast_execute.go, v3.0.2
651:	case OpPower:
652:		result = NewStaticFloat(math.Pow(lhs.Float(), rhs.Float()))
```

Which path runs is decided by how the operands are **spelled**.

### Why that is wrong rather than different

Two paths of the same operator disagree (**A**), and a user writing
`2 ^ 10` means 1024 (**D**). Measured on the pinned container:

| query | reference | `lhs ^ rhs` would be |
|---|---|---|
| `2 ^ 10` | 100 | 1024 |
| `10 ^ 2` | 1024 | 100 |
| `5 ^ 0` | 0 | 1 |
| `0 ^ 5` | 1 | 0 |
| `3 ^ 4` | 64 | 81 |
| `3.0 ^ 4` | **81.0** | 81 |
| `2.0 ^ 10`, `2 ^ 10.0`, `2.0 ^ 10.0` | 1024.0 | 1024 |

The condition matters: this is not a blanket swap, and writing it as one
would be false for `2.0 ^ 10`.

### What PulsusDB does

`lhs ^ rhs` on every path. `2 ^ 10` is 1024.

One consequence looks like a regression and is not: `{ .a = 2 ^ 3 ^ 2 }`
is **512** here and **64** there. Grouping agrees — both are
right-associative — and only the operator diverges. An earlier PulsusDB
version answered 64 by combining a *left*-associative `^` with a correct
operator; two independent errors cancelled for that one input.

### Evidence

Ledger row: `traceql-pow-integer-operand-swap`. User-facing statement:
docs/api.md §4.2 operator precedence.

---

## 17. Three parts of the same system read a span's events three different ways

**Tempo v3.0.2 · queries · test A**

### What the reference does

A span can carry many events. Three different readers of the same span
answer "what is `event:name`?" differently:

| reader | which event | source, v3.0.2 |
|---|---|---|
| the storage condition iterator (`{ event:name = "evZ" }`) | **any** | matches any event row — measured |
| `AttributeFor` (used when a field is compared against another field) | **the first** | `tempodb/encoding/vparquet4/block_traceql.go:128-152`, a first-match scan, reached for intrinsics at `:228-246` |
| `AllAttributes` (the response projection) | **the last** | `block_traceql.go:66-104`, a map, so the last write wins |

One event is appended per event row (`block_traceql.go:3681-3692`), so
the "first" is a property of a linear scan over a flat list.

### Why that is wrong rather than different

All three are reachable from ordinary user queries, and they disagree
(**A**). The first-event answer is also indefensible from the user's
side: adding an **older** event to a span would change whether it
matches, though the event asked about did not change.

Measured on the pinned container with a fixture that varies *which* event
matches — one positive example cannot tell "any" from "first" from "all":

| events on the span | the matching one | reference | PulsusDB |
|---|---|---|---|
| `evX, evY, evZ` | last | no match | **match** |
| `evP, evQ, evR` | first | **match** | **match** |
| `ev1, evM, ev2` | middle | no match | **match** |
| `ev7, ev8` | none | no match | no match |

The negated form confirms it from the other side, and every event name is
individually queryable, so the data is present and indexed.

### What PulsusDB does

The reference's **own designed** multi-value rule, the one it applies to
arrays elsewhere: `matchAll` is set for `!=` and `!~` and the result is
`matchCount == elemCount`; otherwise it is `matchCount > 0`
(`pkg/traceql/ast_execute.go:553-627`). So a span matches if **any** of
its events satisfies the comparison, and `!=` matches only when every one
does.

Copying the first-event behaviour is also not implementable on our
storage without a breaking schema change: event rows carry the span's
timestamp with no event ordinal, and the table collapses two events with
the same name on one span into one row, so the ordering information is
destroyed by construction rather than merely unrecorded.

### Evidence

Ledger row: `traceql-event-link-operand-any-match`. Owner ruling,
2026-08-05.

---

## 18. A trace's duration wraps round past 49.7 days

**Tempo v3.0.2 · queries · test C**

### What the reference does

A search result's `durationMs` is a 32-bit unsigned integer on the wire
(`pkg/tempopb/tempo.proto:139`), filled with an unchecked conversion:

```go
// pkg/traceql/engine.go, v3.0.2
295:		DurationMs:        uint32(spanset.DurationNanos / 1_000_000),
```

A trace longer than about 49.7 days keeps only the bottom 32 bits.

### Why that is wrong rather than different

The reported duration is a plausible-looking number that is not the
duration, with no indication (**C**). A 60-day trace reports
`889032704` ms — 10.3 days, indistinguishable from a genuinely short
trace.

Two captured inputs make the wrap visible:

| trace width | reference | PulsusDB |
|---|---|---|
| 9,007,199,254,740,993 ns | `417264662` | `4294967295` |
| 9,223,372,036,854,775,807 ns | `2077252342` | `4294967295` |

The reference's two values differ; ours are equal, which is the whole
discriminator between a wrapping renderer and a saturating one.

### What PulsusDB does

Saturates. Below zero renders zero; above the maximum renders the
maximum. Saturation preserves a lower bound and wrapping does not: a
consumer can act on "at least 49.7 days", and there is nothing to do with
a plausible number that is simply wrong. The same rule covers the three
other integers the response carries.

The wider reason this divergence is worth having is the rest of the same
change: a strict protobuf-JSON client decodes the search body with no
per-field recovery, so one out-of-range integer returns an error instead
of results and **one bad trace discards every trace in that response**.

### Evidence

Ledger row: `traceql-search-duration-ms-saturates-not-wraps`. This is the
same rule, for the same reason, as entry 2 above — which is what makes it
a rule rather than a one-off call.

---

## 19. Quantile exemplars are placed against numbers the series never carries

**Tempo v3.0.2 · queries · tests A, B**

### What the reference does

A `quantile_over_time` query returns one line per requested percentile
(`p=0.5`, `p=0.9`, …), each with a value per time interval. Exemplars —
individual spans shown as dots on the graph — have to be attached to one
of those lines.

The reference computes the **placement targets** from a distribution
pooled across every series and every interval in the window:

```go
// pkg/traceql/engine_metrics.go, v3.0.2
1933:	// Aggregate buckets across all series and time intervals for better quantile calculation
1934:	aggregatedBuckets := make(map[float64]int) // bucketMax -> totalCount
1936:	for _, in := range h.ss {
1938:		for _, hist := range in.hist {
1939:			for _, bucket := range hist.Buckets {
1940:				aggregatedBuckets[bucket.Max] += bucket.Count
...
1960:	for i, q := range h.qs {
1961:		quantileValues[i] = Log2Quantile(q, buckets)
```

but the value each line actually **carries** is computed per interval:

```go
// pkg/traceql/engine_metrics.go, v3.0.2
1993:				ts.Values[i] = Log2Quantile(q, in.hist[i].Buckets)
```

and the placement then compares the exemplar against the pooled numbers:

```go
// pkg/traceql/engine_metrics.go, v3.0.2
1997:			for _, exemplar := range in.exemplars {
1998:				if h.assignExemplarToQuantile(exemplar.Value, quantileValues, buckets) == qIdx {
```

```
   window:      interval 1        interval 2        interval 3
                (quiet)           (quiet)           (busy)

   p=0.9 line   drawn: 12 ms      drawn: 11 ms      drawn: 900 ms
                   ^                 ^                 ^
                   |                 |                 |
   placement basis:  ONE pooled number for the whole window
                     (dominated by interval 3)  ~  850 ms

   an exemplar of 12 ms in interval 1 is compared against 850 ms,
   not against the 12 ms the line is drawing beside it.
```

### Why that is wrong rather than different

It chooses which line an exemplar belongs to using numbers it never draws
(**A**). Where load varies across the window, an exemplar is attached to
a line whose value at that timestamp is nowhere near the exemplar's own
duration. There is no reading of "attach the exemplar to the nearest
line" under which the comparison basis should be numbers that are not
that line's values.

Second, the placement function does not do what its own comment says
(**B**):

```go
// pkg/traceql/engine_metrics.go, v3.0.2
2010:// assignExemplarToQuantile determines which quantile (if any) an exemplar should be assigned to.
2011:// Returns the quantile index, or -1 if the exemplar doesn't fit any quantile reasonably well.
2012:// This uses a simple closest-match strategy with reasonable bucket validation.
2013:func (h *HistogramAggregator) assignExemplarToQuantile(exemplarValue float64, quantileValues []float64, buckets []HistogramBucket) int {
2014:	if len(quantileValues) == 0 || len(buckets) == 0 {
2015:		return -1
2016:	}
```

There is no "doesn't fit reasonably well" return — the only `-1` is the
empty-input guard — and there is no bucket validation: the `buckets`
argument is never used again after the emptiness check at line 2014. The
path reads as unfinished rather than designed.

### What PulsusDB does

Per-bucket placement. Each exemplar is compared against the quantile
values of **its own bucket** — the numbers the panel draws beside it —
and attaches to the nearest, with ties going to the lowest `p`.

Where one bucket holds a single span, every quantile of that bucket
equals that span's duration, so every candidate ties and the lowest `p`
wins. That is a property of degenerate input, not a defect.

### Evidence

Issue #477. Owner ruling, comment 5506170373, reversing an earlier ruling
that had told us to match the reference. This entry's ledger row is being
written on that issue; the source citations above were re-read directly
from the v3.0.2 tree for this document.

Related: `2026-08-05-traceql-quantile-over-time-tdigest` already
establishes that our `p=` values are computed differently from the
reference's. Our placement being coherent with our own values follows
from that ruling and is not a new decision.

---

## 20. An OTLP point with no value is stored as zero

**Prometheus v3.13.0 · ingest · test C**

### What the reference does

OTLP number data points carry a value type. Prometheus switches on it
with no default arm:

```go
// storage/remote/otlptranslator/prometheusremotewrite/number_data_points.go, v3.13.0
54:		var val float64
55:		switch pt.ValueType() {
56:		case pmetric.NumberDataPointValueTypeInt:
57:			val = float64(pt.IntValue())
58:		case pmetric.NumberDataPointValueTypeDouble:
59:			val = pt.DoubleValue()
60:		}
...
66:		if _, err = c.appender.Append(0, labels, st, ts, val, nil, nil, appOpts); err != nil {
```

A point whose value type is neither leaves `val` at its zero and appends
`0`.

### Why that is wrong rather than different

A value nobody sent is stored, and reads back as an ordinary measurement
(**C**). Measured: `noval_g{job="novalcase"}` = `0`. A zero is a
particularly bad guess — it silently drags down rates, averages and
alert thresholds, and a caller cannot tell it apart from a real zero.

### What PulsusDB does

Rejects that data point and answers `200` with an OTLP partial-success
message naming it. Every other data point in the request is stored.

### Evidence

Metrics ledger, `## Fault classification`, row `value-less-number-point`,
in docs/benchmarks/metrics-differential-ledger.md.

---

## 21. A histogram is stored whose buckets cannot add up to its own count

**Prometheus v3.13.0 · ingest · test C**

### What the reference does

An OTLP explicit-bucket histogram carries a list of boundaries, a list of
bucket counts, and a total count. OTLP's own data model says there is one
more bucket count than boundary. Prometheus converts it to cumulative
buckets like this:

```go
// storage/remote/otlptranslator/prometheusremotewrite/helper.go, v3.13.0
271:		var cumulativeCount uint64
...
274:		for i := 0; i < pt.ExplicitBounds().Len() && i < pt.BucketCounts().Len(); i++ {
...
280:			cumulativeCount += pt.BucketCounts().At(i)
...
293:			val := float64(cumulativeCount)
...
299:			if _, err := c.appender.Append(0, bucketLabels, startTimestamp, timestamp, val, nil, nil, appOpts); err != nil {
...
305:		// Add le=+Inf bucket.
306:		val = float64(pt.Count())
```

Three things follow, and there is no check for any of them.

```
   sent:   bounds       = [1]
           bucketCounts = [4, 6]
           count        = 99
           sum          = 5

   loop bound: i < len(bounds) == 1, so only i = 0 runs.

   stored:  _bucket{le="1"}    = 4       <- from bucketCounts[0]
            _bucket{le="+Inf"} = 99      <- from count, not from the buckets
            _count             = 99
            _sum               = 5

   the 6 is never read.  4 + 6 != 99 is never noticed.
```

1. The loop is bounded by the *shorter* of the two lists, so a trailing
   bucket count is read by nobody and reported nowhere.
2. The `+Inf` bucket is taken from the sent `count`, not from the sum of
   the buckets, so the stored histogram can be arithmetically impossible.
3. `cumulativeCount` is an unchecked unsigned 64-bit addition, which
   wraps round in Go rather than failing, and is then converted to a
   64-bit float at line 293, which loses precision above 2^53. Measured
   with `bucketCounts=[18446744073709551615, 1]` and `count=0`:
   `_bucket{le="1"}` reads back as `18446744073709552000`, and `_count`
   as `0`.

### Why that is wrong rather than different

The stored histogram is corrupt on its own terms — its buckets cannot sum
to its count — and part of the input has been discarded with no error
(**C**). A caller gets a number rather than a rejection, and every
percentile computed from that histogram afterwards is wrong in a way
nobody can detect.

### What PulsusDB does

Rejects the data point and answers `200` with an OTLP partial-success
message. Every other point in the request is stored. A wrong value a
client cannot detect loses to a refusal it can.

### Evidence

Metrics ledger, `## Fault classification`, rows
`inconsistent-classic-histogram` and `u64-bucket-overflow`, with the
reasoning under "Why we keep the strict side of the four `accept` rows".

---

## 22. A fault that can never be fixed by retrying is answered 500

**Prometheus v3.13.0 · ingest · test B**

### What the reference does

Prometheus has one OTLP metrics admission path. Every error rolls the
whole request back, and one switch decides the HTTP status:

```go
// storage/remote/write_otlp_handler.go, v3.13.0
177:	switch {
178:	case err == nil:
179:	case errors.Is(err, storage.ErrOutOfOrderSample), errors.Is(err, storage.ErrOutOfBounds), errors.Is(err, storage.ErrDuplicateSampleForTimestamp), errors.Is(err, storage.ErrTooOldSample):
180:		// Indicated an out of order sample is a bad request to prevent retries.
181:		http.Error(w, err.Error(), http.StatusBadRequest)
182:		return
183:	default:
184:		h.logger.Error("Error appending remote write", "err", err.Error())
185:		http.Error(w, err.Error(), http.StatusInternalServerError)
186:		return
187:	}
```

Four storage conditions get `400`. Everything else gets `500` — including
a metric name that normalises to nothing, an empty label name, a label
name that normalises to an invalid one, and a delta-temporality metric.

### Why that is wrong rather than different

The comment at line 180 states the rule the branch exists for: a bad
request is answered `400` **to prevent retries**. Every condition listed
above is entirely client-controlled and permanent — a delta-temporality
metric never becomes cumulative, and a name that normalises to nothing
never normalises to something. None of them can succeed on retry, and all
of them fall to the `default` arm and get `500` (**B**).

`5xx` is precisely the class OTLP exporters retry. So the reference asks
for an unbounded retry of a payload that can never be accepted, while
also discarding every valid metric sent alongside it.

### What PulsusDB does

A fault in the request's shape or naming is a whole-request `400` with
`google.rpc.Status { code: 3 }` and the reference's own message text. A
fault in one data point's data is a `200` carrying OTLP partial success,
naming the rejected points, with every valid sibling stored. The dividing
rule is published to clients in docs/api.md §1.1.

### Evidence

Metrics ledger rows `otlp-name-reject-status-400`,
`otlp-request-atomic-faults` and `otlp-delta-partial-success` in
docs/benchmarks/metrics-differential-ledger.md. Adjudication on issue
#259, applied verbatim. Note that Loki has the same defect on one of its
own push endpoints, from a different mechanism — entry 11 above.

---

## 23. A grouping after a `select()` puts every span into one group called nil

**Tempo v3.0.2 · queries · tests A, D**

### What the reference does

A `| by(<key>)` stage puts each span into the group named by that key's
value. Written after a `| select(...)`, it puts **every** span into one
group whose key is the string `nil` — while the same response prints each
span's real value for that very key.

Measured 2026-09-04 against
`grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad58700aa96880653c3d8f7`
(the digest `.github/workflows/ci.yml` pins), started for that run with
`ci/tempo/tempo-compare.yaml` unmodified, over a four-span trace whose
spans are named `a`, `a`, `a`, `b`:

```
{ resource.service.name = "grp492" } | select(name) | by(name)
  -> 200, ONE spanSet
     attributes: [{"key":"by(name)","value":{"stringValue":"nil"}}]
     matched: 4, spans: 01 name=a, 02 name=a, 03 name=a, 04 name=b

{ resource.service.name = "grp492" } | by(name) | select(name)
  -> 200, TWO spanSets
     by(name)=a over spans 01,02,03   and   by(name)=b over span 04
```

The mechanism is an ordering one. A `select()` switches every LATER
pipeline element to the second pass:

```go
// pkg/traceql/ast.go, v3.0.2
198:func (p Pipeline) extractConditions(req *FetchSpansRequest) {
199:	forceSecondPass := false
201:	for _, element := range p.Elements {
202:		if forceSecondPass {
203:			extractToSecondPass(req, element)
...
211:		if _, ok := element.(SelectOperation); ok {
212:			forceSecondPass = true
```

and the second-pass columns are read only **after** the pipeline has run
— the fetch builds the first-pass iterator, wraps it in the bridge that
calls the pipeline, and only then creates the iterator that reads the
second-pass columns:

```go
// tempodb/encoding/vparquet4/block_traceql.go, v3.0.2
1601:	iter, err := createAllIterator(ctx, nil, req.Conditions, req.AllConditions, ...)
1606:	if req.SecondPass != nil {
1607:		iter = newBridgeIterator(newRebatchIterator(iter), req.SecondPass)
1609:		iter, err = createAllIterator(ctx, iter, req.SecondPassConditions, ...)
```

(`req.SecondPass` is the whole pipeline: `pkg/traceql/engine.go:98-129`.)
So the grouping executes its key expression against a span whose key has
not been read yet (`GroupOperation.evaluate`,
`pkg/traceql/ast_execute.go:14-55`), every span answers nil, and they all
land in the one group that nil maps to.

### Why that is wrong rather than different

The response contradicts itself: it says the group key is `nil` for four
spans and prints `a`, `a`, `a`, `b` as those spans' values for that same
key, in the same body (**A**). And no reading of "group these spans by
their name" supports one group called nil when the spans have names
(**D**) — the reference's own grouping code copies the resolved value
into the group attribute, so the label it publishes is the value it
grouped on, and that value is wrong rather than merely unavailable.

Nothing about the query is ambiguous or unsupported: written in the other
order the same instance answers correctly, from the same data, in the same
request shape.

### What PulsusDB does

Answers by the key, whichever side of the `select()` it is written on:
two spanSets, `by(name)=a` over spans 1–3 and `by(name)=b` over span 4 —
the same answer as the query without the `select()`. Our projection is
resolved from values already fetched for the matched spans, so no pipeline
stage can be evaluated against an unread column. The rule is published in
docs/api.md §4.2.

### Evidence

Ledger row `traceql-select-before-by-nil-group-key` in
docs/benchmarks/traces-differential-ledger.md carries the capture, both
orderings and our own measured answer. The ordered-fold semantics this
entry contrasts with are pinned by
`crates/pulsus-read/src/traces/search_eval.rs::tests::nested_by_stages_accumulate_attributes`
and by the live two-system differential
`crates/pulsus-read/tests/traces_search_grouping_differential.rs`
(issue #492 item 2).

---

## 24. A group's `sum` and `avg` over one attribute give different answers depending on which span arrived first

**Grafana Tempo v3.0.2 · queries · tests C, D**

### What the reference does

A TraceQL pipeline aggregate — `sum(.v)`, `avg(.v)` — walks the matched
spans and folds each span's value for the key into a running total. The
values arrive as typed cells, and the fold keeps ONE type: whichever the
FIRST contributor had. A later value of a different type is skipped
without a word. `avg` then divides that partial sum by the count of
**all** contributors, including the ones whose values were dropped.

Nothing checks that the values agree in type, and nothing reports that
some were discarded.

```
   stored:  span 01  v = int 2
            span 02  v = double 3.5

   sum(.v):  running total starts as an int at 2.
             3.5 is a double, so it is skipped.
             answer: intValue "2"

   avg(.v):  the same partial sum, 2, divided by 2 contributors.
             answer: doubleValue 1

   now push the SAME two values in the other order:

   sum(.v):  running total starts as a double at 3.5.
             2 is an int, so it is skipped.
             answer: doubleValue 3.5

   avg(.v):  3.5 / 2.
             answer: doubleValue 1.75
```

Four different answers for two numbers, decided by which span the
collector happened to see first.

### Why that is wrong rather than different

Two tests are met.

**C — part of the input is discarded with no error.** One of the two
values never reaches the answer and the response says nothing about it.
The number the user gets is not a coarser answer or a differently
rounded one; it is an answer computed over a subset of the data that
nobody chose and nobody is told about.

**D — no reading of the stated intent supports it.** `sum` over a set of
numbers means the total of those numbers. There is no interpretation of
"sum" under which `2 + 3.5 = 2`, and none under which the same two
values sum to `2` one way round and `3.5` the other. The mean is worse
again: it divides a partial numerator by a complete denominator, so it
is not the mean of anything — not of the values it kept, and not of the
values it was given.

The order dependence is what makes it undetectable in practice. A user
who re-runs the query gets the same answer, because the stored order
does not change; only a different ingest run gives a different number,
and by then there is nothing to compare it with.

### What PulsusDB does

Sums every numeric contributor, whatever type it was stored as, and
divides by the same count it summed. Both push orders give `5.5` and
`2.75`. A value that is not numeric contributes nothing, on either
system.

The wire ARM still follows the stored types — `sum(.attr)` is an
`intValue` only when EVERY contributor was stored `int` — so a mixed-type
sum is reported as the double it is rather than as an integer it is not.

### Evidence

Traces ledger, `traceql-spanset-aggregate-mixed-type-attribute` in
docs/benchmarks/traces-differential-ledger.md, with both orders measured
on a SEPARATE reference instance each — one order cannot show order
dependence. Exercised live in both orders by the fixtures
`mixed_type_int_first_sum`, `mixed_type_float_first_sum`,
`mixed_type_int_first_avg` and `mixed_type_float_first_avg` in
`crates/pulsus-read/tests/traces_search_grouping_differential.rs`, whose
own hermetic guard fails if either order is deleted.

---

## Two mechanisms that produce several of these

Worth stating because a reader meeting one entry will meet the others.

**Printing a parsed query and reading it back.** Loki's query frontend
turns the parsed query back into text and passes that text onward
(`pkg/querier/queryrange/shard_resolver.go:268`, `:275`). Two entries are
that round trip failing to round-trip: entry 9, where the printed form of
a label name is text the same grammar refuses; and entry 5, where the
printed form of a byte threshold is a different number from the one that
was parsed. Anything whose printed form is not a faithful, re-readable
form of itself is a candidate for the same failure.

**Re-parsing a label set that has already been rendered.** Loki has
several places that take a rendered label string and hand it back to a
parser: the response encoder (entry 10), the tail frame encoder (entry
10), and the categorize-labels read path (entry 10's third site). Each is
a chance for the writer and the reader to disagree about what is a legal
label, and they do.

## What is deliberately not in this list

These are differences from the reference that are **not** defects, and
they are named so nobody adds them later.

- **Unspecified order.** Go leaves map iteration order unspecified and
  randomises it, so several reference answers have no reproducible order:
  the `fields` array of `/detected_fields`, tag and scope lists on
  Tempo's discovery routes, the members of an equal-value run in a
  `sort`, `compare()`'s top-N survivors at a tie, the projected
  attributes of a matched span, and the echoed `encodingFlags` array.
  PulsusDB pins a deterministic order in every one of those, which is a
  refinement, not a correction — there is nothing to match. Entry 13 is
  the exception, and it is here because the map walk changes **which
  label holds a value**, not the order of a list.
- **Estimates and coarser answers.** Loki reports `/detected_fields` and
  `/detected_labels` cardinality from a HyperLogLog sketch where we count
  exactly; Tempo computes `quantile_over_time` from a log2 histogram, so
  its answer is one of about 64 values. Both are less exact than ours.
  Neither is wrong: they are consistent with their own model, and the
  traces ledger says so explicitly.
- **Missing limits.** The reference has no bound on template output, on
  JSON key expansion, on `label_replace` amplification, or on regular
  expression compilation cost, and can be made to exhaust memory. Adding
  a bound where there is none is our choice, not their error.
- **Status codes we prefer.** We answer `400` where Loki answers `500`
  for a query-time vector-matching failure, `422` where it answers `400`
  for a result-size cap, and `429` where Tempo answers `500` for
  backpressure. These are judgements about which code describes the
  condition better. They are recorded in the ledgers and are not defects
  on the reference's part. Entries 11 and 22 are different: each has a
  self-contradiction or a violated stated intent behind it, not just a
  code we would have chosen differently.
- **Everything that follows from our architecture.** Storing spans and
  log lines as ClickHouse rows rather than in the reference's own file
  formats decides a great deal — which parameters are meaningful, what
  can be pushed into the scan, what a window prunes. None of it is the
  reference behaving incorrectly.
- **Our own gaps and our own bugs.** The ledgers record several places
  where the reference is right and we are not yet, or where we refuse
  something it serves. Those belong there and not here.
- **`otlp-target-info-span-accepted-points-only`, examined and rejected.**
  The metrics ledger row used to read as a Prometheus defect: it computes
  the `target_info` emission span before validation, so a point it later
  rejects still stretches the span. The first half is true
  (`metrics_to_prw.go:217` runs ahead of the checks at `:218-233` and
  `:235-239`), but on any rejection the deferred block at
  `write_otlp_handler.go:132-138` rolls the appender back and stores
  nothing at all — so the widened span has no consumer and there is no
  defect to decline. The row was corrected on 2026-09-02 and keeps the
  superseded sentence beside the correction.

## Two claims recorded elsewhere that this file deliberately does not make

Both are real measurements and both are in the ledgers. Neither is here,
because in each case the reference's behaviour was observed but its
mechanism was never located in the reference's source, and this file only
lists defects that were confirmed against the source.

- **A grouped `avg_over_time` answers differently from an ungrouped one.**
  Measured on the pinned Loki container over the same four samples in the
  same order: the ungrouped form answers `54.24999999999999` (the range
  reducer's own recurrence) and every grouped form answers `54.25`
  (`sum/count`). Stable over 25 consecutive runs, so not a map walk, and
  not a fold-order effect — `stdvar_over_time` does not move. The
  ledger records the mechanism as unidentified. PulsusDB answers the
  recurrence either way. Row:
  `grouped-avg-over-time-unexplained` in the logs ledger.
- **Tempo under-reports a narrowed tag-value list.** Measured on the
  captured corpus, the reference's own three answers contradict each
  other: a value present on a matching span was absent from the narrowed
  list. Reproduced on two independent container runs. The ledger states
  that the mechanism was not established. Row:
  `traceql-tag-values-narrowed-set-complete-here` in the traces ledger.
