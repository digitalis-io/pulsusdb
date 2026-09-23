# Reproducing the numbers in docs/TraceQL/

Every figure in the TraceQL documents comes from one of these scripts.
They take hosts, ports and directories as arguments; nothing is hard-coded.
`run_all.sh` goes from a clean checkout to `results/` in one command.

| script | what it produces |
|---|---|
| `run_all.sh` | corpus, both stores loaded, schema, layouts, the whole suite and every drill |
| `gen_corpus.py` | corpus g1: OTLP/JSON request bodies **and** the storage rows, from one seeded stream |
| `push_otlp.py` | posts those bodies to any OTLP/HTTP receiver |
| `otlp_to_rows.py` | the write path's mapping: OTLP bodies → storage rows |
| `staging.sql`, `load_staging.sh` | the staging table every layout is built from, and its load |
| `schema.sql` | the proposed schema: five tables, one view, and the catalog writes |
| `apply_schema.py` | applies `schema.sql` to one database, substituting its name |
| `claims_check.py`, `claims.tsv`, `counted-shapes.tsv`, `results-inventory.tsv` | every counted claim named in `claims.tsv`, derived from the thing it counts and compared with what the document says — every occurrence of it, not the first. `counted-shapes.tsv` and `results-inventory.tsv` are the two classifications a search cannot make |
| `claim_domains.py`, `claim-domains.txt` | how many numbers each candidate sweep rule would face, and the rules themselves — the measurement behind the design decision below. `claims_check.py` fails when the committed file and the documents disagree |
| `reproduce_check.py` | runs the catalogue a second time and compares every file that run wrote against a revision, allowing a difference only in the `ms` column. The list of files is the run's own |
| `layouts.sql` | the layout alternatives compared in `sql-schema.md` §8 |
| `make_sql.py` | writes `sql/*.sql`: the statement each query shape compiles to |
| `run_query.sh` | runs one statement warm or cold; rows, bytes, memory, time |
| `search_sliced.py` | the newest-slice-first search loop and its statement count |
| `http_bench.py` | the same queries over HTTP against the reference or PulsusDB |
| `fetch_compare.py` | trace by id, interleaved between the two stores, as a distribution |
| `agreement.py`, `ground_truth.py` | per-filter counts from both stores, and from the corpus file |
| `boundary.sh` | the window rule at both edges and across a day bound |
| `retention.sh` | dropping a day: time, parts, mutations, merges |
| `replication_bytes.sh` | what a span costs on the wire between replicas |
| `json_paths_default.sh` | what `max_dynamic_paths` defaults to, read at the boundary from the database itself, with a declared-parameter control |
| `offload.sh` | the object-storage measurement kept in `functional-requirements.md` §3 after the requirement was withdrawn |
| `fixture/make_fixture.py`, `fixture/fixture_answers.py` | the worked fixture and both stores' answers |
| `fixture/structural_answers.py` | the fifteen structural forms' answers on that fixture |
| `fixture/make_catalogue_fixture.py` | the catalogue fixture: 34 spans in six traces, carrying every key and type the corpus names and the shapes that tell a wrong rule from a right one |
| `catalogue_parse.py` | the TraceQL parser the catalogue's two sides share, and the only thing they share |
| `parse_vectors.tsv` | one line per rule that parser decides, with the tree it must produce, written out by hand — the check on the one module a comparison cannot check |
| `catalogue-extra.tsv` | the queries the corpus does not contain, one per rule of §3.2 no corpus query reaches, each with the rule it decides |
| `catalogue_render.py` | the hand-written TraceQL-to-SQL rules: the statement each route issues, and the membership query |
| `catalogue_interp.py` | the independent side: the fixture rows and an interpreter that walks the same parsed query over them |
| `catalogue.py` | runs both statements for every corpus query and writes `docs/TraceQL/query-catalogue*.md` and `catalogue-sql/` |
| `perturb_check.py`, `perturbations.tsv` | changes one rule at a time, on one side at a time, and requires the catalogue to notice; `results/perturbations.tsv` is the run |
| `planner_dispositions.tsv` | the shipped parser, validator and planner's own disposition for each of the 141 accepted corpus queries |
| `baseline/g1-today.tsv` | today's PulsusDB on the same corpus — an INPUT, with its date and command in the file's own header |
| `recursion_bound.sh`, `nested_set_check.sh`, `shared_span_edges.sh` | the climb's bound, the numbering on a known tree, and shared-span edges |
| `validate_messages.tsv` | the message the semantic pass returns for each of the four `validate_reject/` queries, captured from the validator itself |
| `reference.yaml`, `offload-server.xml` | the configurations the measurements ran against |

Results as measured are in `results/`; `results/comparison.tsv` is the
side-by-side of this design, the reference and today's PulsusDB.

## What a second run reproduces, and what it cannot

`results/` holds only what `run_all.sh` writes, and **every file there is
written by exactly one step**. The only input from outside the run is
`baseline/g1-today.tsv` — today's PulsusDB, which this run has no server of the
old design to measure — with the date and the command that produced it in the
file's own header.

**Which files under `results/` anything reads — and why this file no longer says.**
The inventory is `measure/results-inventory.tsv`: one row per mention of that
directory in every tracked script under `measure/`, with its file, its line,
whether it reads or writes there, and what it does. `measure/claims_check.py`
re-derives those LOCATIONS from the tree and fails when the table carries one
that is gone or misses one that is there, so a mention nobody has classified
stops the run.

The same file carries every other counted claim: `measure/claims.tsv` names **76
claims**, each with the document it is made in, the pattern that finds it, and
the derivation that answers it from the tree — the corpus totals against the catalogue's own
result, the case count and each family of cases, the guard list as a set, the
design queries, the layouts, the statement inventory, the file counts of the
kept and replaced trees. Every occurrence of a claim is compared, not the first,
and what the check prints is the number of rows it ran.

That is deliberate, and it replaces a sentence. This file used to assert the
inventory in prose, and the assertion was wrong in three successive reviews —
five readers that were twelve, then seven that were eight — each time because
the search behind it was narrower than the claim. A sentence cannot be narrower
than the tree when the tree is what produces it.

### The claim nobody registered, and the sweep that now finds it

A registry answers "is this claim right". It cannot answer "is there a claim
here nobody wrote down", and on this branch there were two. The kept table of
`server-implementation.md` said `the trace test tree (379 files)`, naming no
tree; the directory it meant, `crates/pulsus-traceql/tests/`, has 396 files and
has never had 379. The replaced table said `(42 trace migrations)`. Both were
wrong from the first commit of this branch, and neither was in `claims.tsv`, so
nothing compared them. A review found the first by reading; the second was found
by the sweep written to close it.

The migration count then took two more corrections, and both are the same
lesson twice:

```
  42   asserted, never derived                          wrong
  36   derived: `family: Some(Family::Traces),`         right family, wrong SET
                counted over the whole file             -- and it also reaches
                                                           four test assertions
  38   derived: every MIGRATIONS record whose NAME      the set the document means
                is a trace table, at a named revision
```

`trace_tag_catalog` has two migrations, ids 18 and 41, and both carry
`family: None`. A rule keyed on the family leaves them out, and a coder given
that rule would leave them in the catalogue while deleting the table they
manage. Keying on the name, inside the parsed `MIGRATIONS` array, is the set
`server-implementation.md` §6 names — and the same parse gives the four trace
materialized views in `MVS`, which the family rule could not see at all.

The other correction is where the count is read. Review round 10 counted the
trace migrations in a working copy two commits behind the default branch, got
32, and reported the design wrong; the design was right. So every source-tree
count is now read at the revision in `measure/source-revision.txt` with
`git show` and `git ls-tree`, and a clone that does not have that revision
fails the run instead of answering something else.

So `claims_check.py` now sweeps the documents for the two forms a count takes
when it qualifies a path or a set — a parenthesised count `(N word…)` and
`N files` — and requires **every** occurrence to be one of two things:

```
  a number some claims.tsv row already compared        -> nothing to do
  a row of measure/counted-shapes.tsv saying what      -> classified by hand
    it is instead
  neither                                              -> the run stops
```

`measure/counted-shapes.tsv` says what the other **3** are: a wall-clock
reading, and the two wrong numbers quoted in the paragraph above, which are
history rather than claims. It held a fourth — the database's default for
`max_dynamic_paths`, classified as a figure this repository neither sets nor
counts. That was true and it was the wrong answer: `json_paths_default.sh` now
reads the default out of the database at the boundary, so the number is derived
like the rest. A row of this table is a number nothing in this repository can
be asked for, and the first question about one is always whether that is so. The sweep is keyed on the number's position,
so a claim found by both forms is one occurrence, and the run prints how many it
swept.

**What the sweep does not reach.** A count written in any other shape — *the
parser's eight modules*, *one hundred and forty-one queries*, a number in a
table cell with no parentheses — is outside both forms, and is compared only if
somebody registers it. The sweep narrows the judgement; it does not remove it.

### The check, deliberately broken

A check nobody has watched fail is not a check. Each edit below was applied to a
clean tree, `claims_check.py` was run, and the tree was restored from a copy
before the next one; the last line is the tree as it stands.

```text
  the migration count put back to 36                exit 1  says 36, the tree says 38
  the migration count put back to 42                exit 1  says 42, the tree says 38
  the trace materialized-view count changed         exit 1  says 5, the tree says 4
  the test-tree count put back to 379               exit 1  says 379, the tree says 396
  source-revision.txt moved to a stale head         exit 1  says 38, that tree says 34
  source-revision.txt set to a missing revision     exit 1  refuses to derive anything
  one of the pair for one served query removed      exit 1  expected one value, the tree has 1 and 2
  BOTH of that pair removed                         exit 1  a served query with no statement file
  the statements-per-served-query count changed     exit 1  says three, the tree says 2
  the marker-entry count changed                    exit 1  says 3, the tree says 2
  one cell of sql-schema.md section 2 changed       exit 1  the cell against results/storage.tsv
  one of the five resource-row counts changed       exit 1  says 69, the tree says 68
  a registry row AND its derivation deleted         exit 1  registry size 76 against 75
  a parenthesised count added to a heading          exit 1  unclassified, unregistered
  a classification row deleted                      exit 1  table size 3 against 2
  the max_dynamic_paths measurement changed         exit 1  says 1,024, that result says 2048
  a document gains a number, populations stale      exit 1  claim-domains.txt no longer matches
  claim-domains.txt edited by hand                  exit 1  same
  the tree as it stands                             exit 0  172 claims, 0 failing
```

Two of these are the defects of earlier rounds, put back: the fourth is the
count a review found by reading, and the fifth is the reading that made a
correct document look wrong. One is not reachable from here — removing a marker
entry from `crates/pulsus-model/tests/doc_verification_markers.rs`, because that
file is outside this directory and carries other uncommitted work. The claim
side of that comparison was broken instead, and the derivation is
`count(working file) − count(same file at the baseline)`, so either entry going
missing moves the first term to 1 and fails the same comparison.

### Why the judgement is a file, and not a tag in the prose

Three designs were considered for where "this number is a count of a set" is
written down.

The populations are in **`measure/claim-domains.txt`** — one line per rule, the
rule printed beside its count, written by `measure/claim_domains.py --write`.
They are not restated here, and not in any document, for the reason that file's
header gives: a sentence counting the numbers in the document it sits in changes
itself when it is written. `claims_check.py` re-runs the rules and fails when
that file and the documents disagree, so the populations cannot go stale behind
an edit.

| design | what it would face | why not, or why |
|---|---|---|
| sweep every number and classify the rest | the first three rules of `claim-domains.txt` | each of those domains is mostly measurements, so the hand-classified part becomes a table of that size — the same judgement, one level up, and much larger |
| a tag around each number in the prose, the registry derived from the tags | one tag per registry row | not taken. An earlier round of this file reported a trial of it and the trial's results; nothing was kept that would let anybody re-run it, so that account is withdrawn — see below. What can be judged from the code as it stands is the cost: `claims_check.py` finds the guard sentence with `The \d+ guards are …`, so markup wrapped round that number makes the expression find nothing and the run fails on the rule rather than on the claim. Every other rule that reads a sentence holding a tagged number has the same problem |
| **a registry file, plus a sweep of the two count-shaped forms** | the fourth rule of `claim-domains.txt`, and the rows of `claims.tsv` and `counted-shapes.tsv` | **taken.** The narrow sweep is what closes the hole above; given it, the tag buys locality only, and costs the prose |

The two designs that keep a hand-written judgement fail the same way when a
number nobody classified appears: an unregistered number and an untagged number
are both unchecked. So the choice between them was never what closed the hole.

**And the sweep does not close it either — it narrows it.** What the sweep
reaches is the two forms the two wrong numbers happened to be written in. A
count written as a plain figure in a sentence is outside both, and four that the
documents rely on were exactly that until this revision: *two statements per
served query*, and the resource, tag-name and tag-value row counts that carry
the R1 storage invariant. They are registered now, by hand, because a hand is
what registers them. The sweep makes the unregistered count harder to write; it
does not make it impossible, and no design in the table above does.

**Withdrawn.** Two figures reported in an earlier round — a domain of 240
occurrences and one of 117 — came from a search that was not recorded and cannot
be reproduced; `claim-domains.txt` replaces them. The tag trial reported in the
same round is withdrawn on the same ground: it was described in prose, and no
patch, tagged tree, location list or comparison output was kept, so a reader has
only the description. The decision above does not rest on it. It rests on the
argument — a tag is written by the same hand as a registry row, so an untagged
number is exactly as unchecked as an unregistered one — and on the cost that is
visible in the retained code.

What stays here is the part no search can decide: **the order**. Every file under
`results/` is written before anything reads it, and one pair has to be in that
order on purpose — `perturb_check.py` writes `results/perturbations.tsv` and
`catalogue.py` then quotes it for the rules no query can tell apart, so
`run_all.sh` runs the perturbation suite **before** the catalogue step. A run
that took those two the other way round would describe the previous run's rules.
The corpus summary is the same shape at the other end of the run: it is written
first because the population check reads it before anything is measured.

`reproduce_check.py` checks the first row of the table below, and takes the same
arguments `catalogue.py` does. **It runs the catalogue itself**, so the run is
part of the check rather than something assumed to have happened earlier; then it
compares every file that run wrote — the list is the run's own
`results/catalogue-outputs.txt`, written as each file is written, not a path list
kept here — against a git revision, allowing a difference only in the `ms`
column, masked in the text so that every other byte — indentation and key order
included — still has to match. It **deletes that list before starting the
catalogue**, so a run that writes nothing leaves nothing to mistake for success;
it fails if the list is then missing or empty, if a listed file is absent, if
anything differs beyond the timing column, or if git reports a file modified
under `docs/TraceQL/` that the run did not write — so it wants a clean tree.
Four failure modes it exists to close are from earlier rounds: a hand-written
path list that omitted `query-catalogue-accepted.md`, reporting success when it
had compared nothing, a back-dated list letting a no-op run pass, and a parsed
and re-encoded JSON accepting a change of indentation as timing-only.

It needs the documents **committed**. Both of its sides are git — the earlier
bytes come from `git show REV:path`, and the did-not-write rule reads
`git status` — so while `docs/TraceQL/` is untracked it has nothing to compare
against and cannot run. The equivalent by hand is a copy of the directory taken
before the run and a difference taken after, with the `ms` column masked; that
is what was done for this revision, and it is weaker in one way worth naming:
the copy is one this revision made, so it shows the run reproduces itself, not
that it reproduces a state somebody else can fetch.

| what | a second run |
|---|---|
| `query-catalogue*.md`, `catalogue-sql/`, `results/catalogue.json`, `results/catalogue-outputs.txt` | **byte-identical**, except the `ms` column, which is one wall-clock reading per statement (`reproduce_check.py`) |
| `results/perturbations.tsv` | identical: every verdict is a comparison, not a timing |
| `results/fixture-answers.tsv`, `fixture-structural.tsv`, `edge-checks-summary.txt`, `g1-ground-truth.tsv`, `g1-agreement.tsv`, `r6-denominators.tsv`, `json-paths-default.tsv` | identical: these are answers and counts |
| `results/g1-new-design.tsv`, `g1-new-design-sliced.tsv`, `g1-reference-warm.tsv`, `g1-fetch-compare.tsv`, `comparison.tsv`, `layout-comparison.tsv`, `drills.txt`, `storage.tsv` | **the answers and row counts repeat; the milliseconds do not.** They are wall-clock medians on a shared machine |

The run also refuses to compare two different populations: before it measures
anything it requires the reference to report **exactly** `spans + dup_spans`
from `results/corpus-summary.json` — 2,000,064 + 19,669 = 2,019,733 for corpus
g1, the second figure being the spans inside the 40 bodies the corpus sends
twice, which the reference keeps and this design collapses. Anything larger is
an earlier run's leftovers and stops the run. Start the reference with an empty
data directory.

The corpus generator is seeded, so it is the easiest thing to check that on: a
second `gen_corpus.py $WORK/g1 1790095601 2000000` reproduces every figure in
`results/corpus-summary.json`.

To reproduce the catalogue alone — no reference needed, and the part that
carries the correctness claims. `$WORK` is the ClickHouse server's
`user_files_path` and `tqd_cat` is the database the committed answers were
produced in; a different name produces the same answers with the name
substituted into every generated statement.

```
fixture/make_catalogue_fixture.py $WORK/cat 1790000000
load_staging.sh  $CH tqd_cat cat
apply_schema.py  $CH tqd_cat
perturb_check.py $CH tqd_cat crates/pulsus-traceql/tests/corpus $WORK/cat/spans.jsonl \
               $WORK/perturb 1790000000000000000 1790000060000000000
catalogue.py   $CH tqd_cat crates/pulsus-traceql/tests/corpus $WORK/cat/spans.jsonl \
               docs/TraceQL/measure 1790000000000000000 1790000060000000000
claims_check.py
```

`catalogue.py` exits non-zero if any statement fails, any answer disagrees, any
query is answered empty on both sides without a recorded reason, any query in
`parse_vectors.tsv` parses to a tree other than the one written there, or its
disposition rule disagrees with the shipped planner. `claims_check.py` exits
non-zero if any number or set the documents state differs from the thing
it counts; it runs last because one of its rules reads the catalogue's own
result. `perturb_check.py` exits
non-zero if any rule can be changed without the run noticing — and it runs
first, because it is what writes `results/perturbations.tsv`, which the
catalogue document quotes.

## What this design changes outside `docs/TraceQL/`

One file: `crates/pulsus-model/tests/doc_verification_markers.rs`, which gains
**2 marker entries** and loses none.

That test walks every Markdown document under `docs/`, collects the lines
carrying one of two marker phrases, and requires each collected line to match an
entry of its `MARKERS` array — the entry recording where that statement's
expectation comes from. Two statements in `sql-schema.md` carry such a phrase,
so without the two entries the test fails on a count it cannot account for.

**What that test does not do**, stated because an earlier description of this
change said that it did: it does not require every verification statement in
`docs/**/*.md` to be registered. It sees a statement only when the statement
carries one of the two phrases, and its own header names two statements in this
repository that were false, carried no marker, and were invisible to it. So the
two entries record provenance for two sentences. They are not a check that these
documents' verification claims are complete, and nothing here is.

The two entries went missing from a working copy once, while a branch was being
deleted, and nothing in this directory could see that either. `claims_check.py`
now derives the count as the difference between the working file and the same
file at the baseline revision, so dropping either entry fails the run.

## What this revision re-derived, and what it carries forward

Nothing at corpus scale was re-measured for this revision. The distinction
matters to a reader deciding how much a figure is worth, and the run that
produced the corpus-scale figures is not the run that produced this text.

| | |
|---|---|
| re-derived here, against the tree | every row of `claims.tsv`, the sweep, the results inventory, the domain populations, the storage table of `sql-schema.md` §2 against `results/storage.tsv`, the `max_dynamic_paths` boundary, and the whole catalogue — `perturb_check.py` and `catalogue.py` over the catalogue fixture |
| carried forward unchanged | every figure measured over corpus g1 — storage, the query timings, the layout comparison, the reference comparison, the drills. Their producer is `run_all.sh` and their output is `results/` |
| not run at all for this revision | `reproduce_check.py`, which needs the documents committed — see below; and the repository's own test suites, which this directory changes no crate of |

**What no figure here can see.** The corpus-scale results come from one run on
one machine. `results/` records the answers and the row counts, which repeat,
and the milliseconds, which do not — the table above under "What a second run
reproduces" says which is which, column by column. Three carried-forward figures
in `functional-requirements.md` §1 come from a run older still, against a server
of the old design, and that document says so where it prints them.

## The three stores the run needs, and how they were started

`run_all.sh` takes them as arguments; nothing about them is hard-coded. What it
needs is one ClickHouse for the design, one reference at the pinned build, and
(for the replication figure only) two ClickHouse replicas of one cluster with a
keeper. The run this branch's numbers come from used, with `$WORK` the corpus
directory and every port bound to the loopback address:

```
# the design's store: user_files_path must be $WORK, so file('g1/spans.jsonl') resolves
podman run -d --name <ch> --network <net> -p 127.0.0.1:<port>:8123 \
  -v $WORK:/corpus:ro -v <conf>/tqd.xml:/etc/clickhouse-server/config.d/tqd.xml:ro \
  -e CLICKHOUSE_DO_NOT_CHOWN=1 clickhouse/clickhouse-server:26.3
# tqd.xml sets <user_files_path>/corpus/</user_files_path> and listen_host 0.0.0.0

# the reference, at the build deploy/e2e/compose.single.yaml pins, with reference.yaml
podman run -d --name <ref> -p 127.0.0.1:<http>:3200 -p 127.0.0.1:<otlp>:4318 \
  -v $PWD/reference.yaml:/etc/tempo.yaml:ro -v <refdata>:/var/tempo <the pinned image> \
  -config.file=/etc/tempo.yaml

# the replica pair, for replication_bytes.sh only: a keeper and two servers on one
# network, each with <macros><replica>, a <zookeeper> node and a <remote_servers>
# entry naming the cluster `tqd`
```

Then:

```
run_all.sh $WORK <ch-url> <ref-url> <ref-otlp-url> <replica1-url> <replica2-url> <src-host:9000>
```

`<src-host:9000>` is a native-protocol address from which the replicas can read
the loaded corpus. Given no replica pair, the run prints "replication bytes:
skipped" and every other number is unaffected.

## Regenerating `planner_dispositions.tsv`

The catalogue's disposition rule (`catalogue_render.refusal`) says which queries
the API answers `400` at plan time. This file is the same question asked of the
shipped code, so the rule is checked rather than asserted; `catalogue.py` fails
if the two disagree on any query. It covers the corpus and the queries of
`catalogue-extra.tsv` alike. Put this in
`crates/pulsus-read/tests/tqd_probe_dispositions.rs`, run it, keep the `PROBE`
lines with the leading column removed, and delete the file again — it is a
probe, not a test the repository keeps:

```rust
//! The disposition the shipped parser, validator and planner give each query
//! in crates/pulsus-traceql/tests/corpus/ and each row of
//! docs/TraceQL/measure/catalogue-extra.tsv.
use std::path::Path;

fn is_metrics(q: &pulsus_traceql::Query) -> bool {
    q.pipeline.iter().any(|s| {
        matches!(
            s,
            pulsus_traceql::PipelineStage::Metric(_)
                | pulsus_traceql::PipelineStage::Compare { .. }
        )
    })
}

fn disposition(name: &str, src: &str) {
    let q = match pulsus_traceql::parse(src) {
        Ok(q) => q,
        Err(e) => {
            println!("PROBE\t{name}\tparse_error\t{e}");
            return;
        }
    };
    if let Err(e) = pulsus_traceql::validate(&q) {
        println!("PROBE\t{name}\tvalidate_error\t{e}");
        return;
    }
    let route = if is_metrics(&q) { "metrics" } else { "search" };
    let outcome = if route == "metrics" {
        let ctx = pulsus_read::MetricsCtx {
            filter: pulsus_read::SpanFilterCtx {
                spans_table: "trace_spans",
                attrs_table: "trace_attrs_idx",
            },
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            distributed: false,
            skip_unavailable_shards: false,
        };
        let params = pulsus_read::MetricsParams {
            start_ns: 1_790_000_000_000_000_000,
            end_ns: 1_790_000_060_000_000_000,
            step_ms: 60_000,
            exemplars: None,
        };
        match pulsus_read::plan_trace_metrics(&q, &params, &ctx) {
            Ok(_) => "served".to_string(),
            Err(e) => format!("plan_error\t{e}"),
        }
    } else {
        let ctx = pulsus_read::SearchCtx {
            filter: pulsus_read::SpanFilterCtx {
                spans_table: "trace_spans",
                attrs_table: "trace_attrs_idx",
            },
            recent_table: "trace_recent",
            errors_table: "trace_error_spans",
            max_candidates: 100,
            max_series: 1_000,
            distributed: false,
        };
        let params = pulsus_read::SearchParams {
            start_ns: 1_790_000_000_000_000_000,
            end_ns: 1_790_000_060_000_000_000,
            limit: 20,
            spss: 3,
        };
        match pulsus_read::plan_search(&q, &params, &ctx) {
            Ok(_) => "served".to_string(),
            Err(e) => format!("plan_error\t{e}"),
        }
    };
    println!("PROBE\t{name}\t{route}\t{outcome}");
}

#[test]
fn print_dispositions() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let root = crates.join("pulsus-traceql/tests/corpus");
    let mut files: Vec<_> = ["accept", "grafana"]
        .iter()
        .flat_map(|d| {
            std::fs::read_dir(root.join(d))
                .unwrap()
                .filter_map(|e| {
                    let p = e.unwrap().path();
                    (p.extension().map(|x| x == "traceql").unwrap_or(false)).then_some(p)
                })
                .collect::<Vec<_>>()
        })
        .collect();
    files.sort();
    for f in files {
        let name = f.file_stem().unwrap().to_string_lossy().to_string();
        let src = std::fs::read_to_string(&f).unwrap();
        disposition(&name, src.trim());
    }
    let extra = crates
        .parent()
        .unwrap()
        .join("docs/TraceQL/measure/catalogue-extra.tsv");
    for line in std::fs::read_to_string(&extra).unwrap().lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let mut cols = line.split('\t');
        let name = cols.next().unwrap();
        let src = cols.next().unwrap();
        disposition(name, src.trim());
    }
}
```

```
cargo test -p pulsus-read --test tqd_probe_dispositions -- --nocapture \
  | grep '^PROBE' | cut -f2- > docs/TraceQL/measure/planner_dispositions.tsv
```

The route is chosen the way a client chooses one: a query whose pipeline carries
a metric function or `compare()` goes to `/metrics/query_range`, and every other
query goes to `/search`.

## Regenerating `validate_messages.tsv`

The four `validate_reject/` corpus cases parse, so their `.golden` files hold a
parsed query rather than a message; the message comes from the semantic pass.
Put this in `crates/pulsus-traceql/tests/tqd_probe_messages.rs`, run it, keep
the `PROBE` lines with the leading column removed, and delete the file again —
it is a probe, not a test the repository keeps:

```rust
#[test]
fn print_validate_messages() {
    for q in [
        "{} | avg(name) > 1",
        "{ name }",
        "{ .a = 1 = 2 }",
        "{ nestedSetLeft =~ \"x\" }",
    ] {
        let ast = pulsus_traceql::parse(q).expect("parses");
        match pulsus_traceql::validate(&ast) {
            Ok(()) => println!("PROBE\t{q}\tOK"),
            Err(e) => println!("PROBE\t{q}\t{e}"),
        }
    }
}
```

```
cargo test -p pulsus-traceql --test tqd_probe_messages -- --nocapture \
  | grep '^PROBE' | cut -f2- > docs/TraceQL/measure/validate_messages.tsv
```
