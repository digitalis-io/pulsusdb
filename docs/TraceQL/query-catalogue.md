# The TraceQL query catalogue

Every query in the repository's TraceQL corpus, with the SQL it compiles to
against the schema of `docs/TraceQL/sql-schema.md` and the literal answer it
gives, or — where the query is refused — the status and the reason.

The corpus is `crates/pulsus-traceql/tests/corpus/`: 141 queries the
parser and the validator accept (139 under `accept/`,
2 under `grafana/`, which are what
the dashboard client actually sends) and 47 they refuse
(40 `reject/`,
3 `unsupported/`,
4 `validate_reject/`). It
came from the reference's own suite and from captured client traffic, so it
covers shapes nobody writes by hand.

| | |
|---|---:|
| queries the parser and the validator accept | **141** |
| of those, the API refuses at plan time, `400` | **3** |
| queries the API serves | **138** |
| of those, with a statement that runs | **138** |
| whose **answer** equals the independent check | **138** |
| whose **membership** equals the independent check | **138** |
| queries the parser or the validator refuses, `400` | **47** |
| queries added here, which the corpus does not contain | **12** |
| of those, served, with a statement that runs | **12**, **12** |
| whose **answer** and **membership** equal the check | **12**, **12** |

- `docs/TraceQL/query-catalogue-accepted.md` — the 150
  served queries, each with its membership SQL and its answer: the
  138 of the corpus and the 12 added here.
- `docs/TraceQL/query-catalogue-refused.md` — the
  50 refusals:
  47 the parser or the validator turns away and
  3 the planner does, each with the status and the
  reason.
- `docs/TraceQL/measure/catalogue-sql/` — two statements per served query, both
  run exactly as the file stands: `<name>.sql` is **the statement the route
  issues**, returning the answer a client receives; `<name>.membership.sql`
  returns the ids of the matching spans, which is this catalogue's answer
  column. Each file ends with the settings it needs, so it can be pasted into a
  client as it is.

## How this was produced, and what the check proves

`docs/TraceQL/measure/catalogue.py` reads the corpus, renders each query with
the rules of `docs/TraceQL/server-implementation.md` §3.2 written out once each
(`catalogue_render.py`), **runs both statements** against ClickHouse, and
compares each with an interpreter that walks the same parsed query over the
fixture rows (`catalogue_interp.py`).

**The two sides share the parser (`catalogue_parse.py`) and nothing else.** Not
the status or kind codes, not the order an unscoped read tries the scopes in,
not the climb's depth bound, not the shape of an answer: each is written once on
each side, from the document or the standard that defines it.
`measure/perturb_check.py` is the check on that claim — it changes one rule at a
time, on one side at a time, and requires the comparison to go red for each;
`results/perturbations.tsv` is its output, and `docs/TraceQL/measure/README.md`
says how to run it.

**A wrong rule in the shared parser would move both answers together**, so the
comparison cannot see one, and the parser is checked a second way instead:
`measure/parse_vectors.tsv` holds one line per rule it decides — an escape, the
precedence, a keyword spelling, `minInt`, the unit a duration is written with —
with the tree that rule must produce, written out by hand from the grammar. This
run found **0** trees that differ. What a unit
is worth in nanoseconds is no longer in that module at all: the parser hands over
the digits and the spelling, and each side multiplies with its own table, because
changing the shared `ms` multiplier from 10⁶ to 10³ left every answer agreeing
(review round 5).

**The disposition is checked against the shipped planner, not asserted.**
`catalogue_render.refusal` implements §3.2's `400` rows;
`measure/planner_dispositions.tsv` holds the shipped parser, validator and
planner's own answer for all 141 of these queries, captured by the
probe `measure/README.md` prints. This run found
**0 disagreements** between the two.

**The reference is not consulted here, and that is deliberate.** The corpus's
keys are synthetic — `.a`, `.b`, `span.retried` — so a second store would answer
every one of them with an empty set, which proves nothing. The authority for an
answer is the fixture data itself, read by the interpreter. Where the two stores
*can* be compared on the same spans, they are, in
`docs/TraceQL/functional-requirements.md` §6.1 and §6.2.

The fixture is `docs/TraceQL/measure/fixture/make_catalogue_fixture.py`:
34 spans in 6 traces carrying every key, scope, intrinsic and value
type the corpus mentions, so an answer is a list of spans rather than a uniform
empty set. Answers name spans by the last four hex digits of their id.

Three of its six traces exist so that a wrong rule and a right one differ: a
chain that alternates the two sides of an operator (`P -> A2 -> B1 -> A1`)
beside a sibling pair under a parent of neither side; a descendant three links
below its nearest matching ancestor, so a direct-child rule and a bounded climb
answer `>>` differently; a span whose parent is not stored; a two-span cycle;
and one resource carrying a key its own span also carries with a different
value, so the unscoped lookup **order** decides the answer.

Reproduce with:

```
fixture/make_catalogue_fixture.py $WORK/cat 1790000000
load_staging.sh   $CH tqd_cat cat
apply_schema.py   $CH tqd_cat
catalogue.py $CH tqd_cat crates/pulsus-traceql/tests/corpus $WORK/cat/spans.jsonl \
             docs/TraceQL/measure 1790000000000000000 1790000060000000000
```

## What the design cannot serve

Of the 141 queries the parser and the validator accept, the API
refuses **3** at plan time and this design keeps
every one of those refusals. They are not defects in the schema — the storage
answers all three — they are behaviour the shipped planner declines, each with
its own refusal site:

| query | TraceQL | status | the reason |
|---|---|---|---|
| `by_expression_key` | `{ .a = 1 } \| by(.b + .c)` | `400` | a grouping key must resolve to a single per-span value, so it must be an attribute or an intrinsic |
| `duration_unitless` | `{ duration > 100 }` | `400` | duration requires a duration literal |
| `pipeline_spanset_operation` | `{ .a = 1 } \| { .b = 2 } && { .c = 3 }` | `400` | a `\|` stage must be a single { ... } filter, not a cross-spanset or structural operation |

Of the 150 queries it serves, 0
failed to run and 0 returned an
answer other than the independent check's. Two limits of the design are stated
rather than counted, because they are properties of the rules rather than of any
query:

1. **A nested-set comparison other than the three shapes §3.2 answers directly**
   needs two statements (`server-implementation.md` §3.5). No corpus query asks
   for one: the corpus's three nested-set queries are `nestedSetParent < 0`,
   `nestedSetLeft > 0` and `nestedSetRight >= 1`, which compile to the root
   anti-join and to `true`.
2. **An unscoped read in VALUE position** — inside `select()`, `by()` or a
   field-against-field comparison — resolves the span scope and then the
   instrumentation scope, and stops. In predicate position it resolves all five
   scopes in the documented order. A resource, event or link value in value
   position needs a join or an array read; no corpus query asks for one.

### The 12 queries the corpus does not contain

The corpus belongs to the parser and was written to cover the grammar, so some
rules of `server-implementation.md` §3.2 have no query that reaches them — and a
rule no query reaches is a rule this catalogue does not check, whatever its
agreement count says. `measure/perturb_check.py` names them: it changes one rule
at a time and reports the ones the comparison cannot tell apart. Round 5 of the
review found eight, and one of them was hiding a query the route could not serve
at all: `by(<an attribute>)` put the attribute's `Dynamic` value straight into
`GROUP BY`, which ClickHouse refuses (code 44), so an accepted, documented search
failed against the database. These queries are the answer to that: one per rule,
run and compared exactly as the corpus's are.

| query | TraceQL | the rule it decides |
|---|---|---|
| `by_span_attribute` | `{} \| by(span.http.method)` | a search grouped by an ATTRIBUTE, which 3.2 says is served; the corpus groups only by `name` and `resource.service.name`, both String columns, so no corpus query renders a group key read from the JSON |
| `by_missing_attribute` | `{} \| by(span.missing)` | a group key no span carries: every span is dropped by the presence rule and the search answers no traces, rather than the statement failing |
| `by_attribute_types` | `{} \| by(span.a)` | a group key whose value has the same TEXT at two stored types on one trace — `1` as an integer on one span and `1.0` as a double on another — so a statement that groups on the label alone merges two groups the API keeps apart (`docs/api.md` §4.2: an attribute by-key renders in the arm the sender stored it as) |
| `by_name_order` | `{} \| by(name)` | a trace with more than one group, whose first-appearance order differs from the group values' own order (trace six: `Az` starts before `A\n`) |
| `truthiness_span_scoped` | `{ span.success }` | the SPAN-SCOPED branch of the truthiness rule; the corpus's only truthiness query is unscoped |
| `coalesce_after_by` | `{ .a = 1 } \| by(resource.service.name) \| coalesce()` | `\| coalesce()` with a group key to drop; the corpus's `\| coalesce()` query has no `by()` before it |
| `topk_two_series` | `{} \| rate() by(resource.service.name) \| topk(1)` | `topk` over more than one series; the corpus's topk query has one series on this fixture, so the two orders keep the same one |
| `bottomk_two_series` | `{} \| rate() by(resource.service.name) \| bottomk(1)` | the same, in the other direction |
| `span_id_present` | `{ span:id = "0000000000000001" }` | a `span:id` comparison with an id a fixture span HAS; the corpus's three compare a sixteen-character id with a four-character literal, so their answer is empty whichever column the rule reads |
| `compare_both_sides` | `{} \| compare({ .b = 2 }, 3)` | a comparison whose selection and baseline are BOTH non-empty, over a resource carried by spans on both sides, with a `topN` small enough that applying it per key and per side differs from applying it once |
| `compare_selection_scopes` | `{} \| compare({ .a = 1 }, 3)` | the other side of the comparison above: its selection carries the spans with events, links and instrumentation attributes, which `compare_both_sides` has only on its baseline — between them the five scopes appear on both sides, so a rule that puts one scope's rows on the wrong side cannot hide in a scope that only ever has one |
| `exemplars_off` | `{} \| rate() with(exemplars=false)` | `with(exemplars=false)`, the one hint that changes a read; no corpus query turns exemplars off |

### What this check cannot detect

Five things, stated because the counts above do not say them. **This list is a
judgement, and it is the one claim on this page nothing checks.** The numbers
and sets `measure/claims.tsv` registers are derived from the thing they count
and compared with the document by `measure/claims_check.py`, which fails the run
when they differ; a number nobody registered is compared only if it is written
in one of the two shapes that file sweeps for, and otherwise it is not compared
at all. No program can tell you that a limit is missing from a list of limits.
Read the list below as what somebody thought of, not as a closed set.

1. **A rule both sides get wrong the same way.** The renderer is this design's
   statement and the interpreter is this design's meaning, so the comparison
   asks whether the statement says what the design means — not whether the
   design is right. Two other checks cover part of that: the disposition rule is
   checked against the shipped planner for every query
   (`measure/planner_dispositions.tsv`), and `functional-requirements.md` §6.1
   and §6.2 compare answers with the reference on the same spans.
2. **A rule nothing here names.** `measure/perturb_check.py` changes the rules
   `measure/perturbations.tsv` lists, one at a time, and nothing else: it does
   not enumerate the renderer's branches or the interpreter's, so the count of
   rules is a count of what somebody wrote down. A branch that no query here
   reaches **and** that no row names is invisible, and its absence looks exactly
   like a rule that was checked. The shared parser has the same shape:
   `measure/parse_vectors.tsv` is a list, not a proof of completeness, and a
   token shape none of its queries uses moves both sides together unseen.
3. **A value shape the fixture has no instance of.** The fixture carries string,
   integer, double and boolean attributes; it carries **no array-valued
   attribute**, so the array branch of every typed rule is unexercised. The
   worked fixture of `functional-requirements.md` §6.1 has one
   (`span.app.tags`), and F7 is its answer.
4. **Everything above the database.** Both sides are compared by sending SQL
   straight to the server and reading the rows back; **no HTTP request is made
   anywhere in this check**. The route, the response serialisation and the
   envelopes of `docs/api.md` §4.2 and §4.4 are therefore outside it — including
   the two parts of `compare()` that are deliberately the response layer's: the
   fixed 25-key well-known set, which emits `key=nil` for a key **absent** from
   the data, and the `*_total` denominators (`server-implementation.md` §3.2).
   What is checked is that neither can lose a stored count, because the
   statement's own `topN` is per key and per side.
5. **Anything at corpus scale, and anything timed.** The statements in
   `measure/sql/` are a different producer (`measure/make_sql.py`) and are not
   compared with the interpreter at all; they are checked against the corpus and
   the reference in §6.2. The `ms` column here exists to show the statement ran.

### The rules no query can tell apart

These are the rules that are left: a perturbation of one changes no answer on any
query here, and each says why.

| perturbation | the rule | why no query can tell it apart |
|---|---|---|
| — | — | *(none: every perturbation is noticed by some query here)* |

### The queries whose right answer is empty

An empty answer on both sides establishes nothing, so each one is named here
with the reason it cannot be made non-empty on any fixture. This run found
**0** empty answers that are not on this list.

| query | TraceQL | why no fixture can answer it |
|---|---|---|
| `intrinsic_colon_space_after` | `{ span: id = "0a1b" }` | the same query written `span: id`; the literal is two bytes |
| `intrinsic_span_id` | `{ span:id = "0a1b" }` | a stored span id is eight bytes, so `lower(hex(span_id))` is sixteen characters and never equals the two-byte literal `"0a1b"` |
| `intrinsic_span_parent_id` | `{ span:parentID = "0a1b" }` | a stored parent span id is eight bytes; the literal is two |
| `intrinsic_trace_id` | `{ trace:id = "0a1b" }` | a stored trace id is sixteen bytes, so its hex is thirty-two characters and never equals `"0a1b"` |

Four behaviours are worth stating plainly, because they are properties of the
design rather than of a query:

1. **`resource.service.name` is answered from the span's `service` column**, for
   comparison, regex and presence alike. The resource row does not repeat the
   service name (R1), so a presence test against the resource JSON would answer
   `false` for every span. The catalogue found this: `explore_root_rate_by_service`
   returned no series until the rule was written down.
2. **`compare()` groups by value *and stored type*.** An integer `1` and a
   double `1.0` render as the same text in ClickHouse, so grouping on text alone
   merges them; the API's own rule keeps a value distinct per type. The
   statement therefore carries the type from `JSONAllPathsWithTypes`.
3. **`| select()`, `| coalesce()` and `with(...)` do not change which spans
   match**; `| by()` does not either, but it changes the shape of the answer,
   and `<name>.sql` shows that shape. The catalogue's answer column is
   membership, so those queries share their answer with the filter they wrap.
   `with(sample=true)` changes no read at all — the shipped planner accepts it
   and returns the exact superset (`metrics_plan.rs:1093`).
4. **The transitive structural operators run bounded.** Every `>>`/`<<` statement
   carries `depth < PULSUS_TRACEQL_MAX_DEPTH` and reports what it could not
   resolve, as `sql-schema.md` §5.8 describes.

## What the corpus covers

| family | queries |
|---|---:|
| intrinsics | 33 |
| attribute comparison | 18 |
| metrics | 17 |
| pipeline stages | 14 |
| structural operators | 13 |
| the rules the corpus does not reach | 12 |
| duration literals | 9 |
| arithmetic | 7 |
| field expressions | 7 |
| static keywords | 6 |
| spanset operations | 4 |
| string escapes | 4 |
| existence and truthiness | 2 |
| hints | 2 |
| client-generated | 2 |

## Shapes that differ only in a literal

20 groups of queries compile to the same membership statement with a
different literal; the largest are listed here, and every member still has its
own SQL file and its own answer.

| statement shape | queries |
|---|---|
| 12 queries | `arith_minus`, `arith_mod`, `arith_plus`, `arith_pow`, `arith_slash`, `arith_star`, `coalesce_after_by`, `hints_most_recent`, `hints_repeated`, `pipeline_by`, `pipeline_coalesce`, `select_single` |
| 6 queries | `intrinsic_kind_eq_server`, `intrinsic_span_kind`, `static_kind_client`, `static_kind_consumer`, `static_kind_internal`, `static_kind_unspecified` |
| 6 queries | `intrinsic_name_eq`, `intrinsic_span_name`, `string_escape_hex`, `string_escape_octal`, `string_escape_unicode`, `string_escapes_short` |
| 5 queries | `by_attribute_types`, `by_missing_attribute`, `by_name_order`, `by_span_attribute`, `match_all` |
| 4 queries | `intrinsic_span_status`, `intrinsic_status_eq_error`, `intrinsic_status_eq_unset`, `select_multi` |
| 3 queries | `bare_bool_true`, `intrinsic_nested_set_left_gt`, `intrinsic_nested_set_right_gte` |
| 3 queries | `duration_frac_half`, `duration_gt`, `intrinsic_span_duration` |
| 3 queries | `intrinsic_colon_space_after`, `intrinsic_span_id`, `span_id_present` |
| 3 queries | `exemplars_off`, `metrics_count_over_time`, `metrics_rate` |
| 3 queries | `pipeline_spanset_filter`, `pipeline_spanset_filter_first`, `pipeline_spanset_filter_parens` |
| 2 queries | `arith_unary_neg`, `static_min_int` |
| 2 queries | `duration_frac_leading_dot`, `duration_lt` |
