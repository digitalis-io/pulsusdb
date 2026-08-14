# ClickHouse 24.8 → 26.3: what moved in the query plans (issue #376)

One row per gated shape. A shape's verdict is one of:

- **unchanged** — the gate's assertion is byte-identical on both servers and
  the suite passes on 26.3 with no edit.
- **moved-correct** — the assertion changed, and the change is (i) no worse
  in net granule/row/byte selection than the pre-bump number for the same
  shape on the same fixture, and (ii) attributed to a named 26.x change with
  its `system.settings_changes` row or a probe.
- **moved-better** — as moved-correct, but the new number is strictly better.
  Recorded rather than accepted silently.
- **moved-regression** — halts the commit and goes back to the owner. **No
  row below carries this verdict.**

Servers: `docker.io/clickhouse/clickhouse-server:24.8` →
`24.8.14.39` (digest `sha256:1ffa82edee000a42c09313bd9f1293d94c570aee74babc1b3ca9983a35fa597b`)
and `:26.3` → `26.3.17.110`
(digest `sha256:2ef11bbe2e44ab7022f37ff3019b3f2125ed09e919ea6194660be6130b7ca4b7`).
Both digests are recorded so a silent retag of the floating minor tag is
detectable. Digests from `podman images --digests | grep clickhouse-server`.

---

## 1. The shapes that moved

Two of the 79 gated shapes, plus one live fixture outside that set (row 3).

| # | shape | 24.8.14.39 | 26.3.17.110 | verdict | the in-run identity that proves it |
|---|---|---|---|---|---|
| 1 | `explain_indexes :: stage3_not_contains_line_filter_uses_the_primary_key_and_the_token_skip_index` | `Skip` blocks: `idx_body_tokens`, `idx_body_ngrams`. No `<Combined skip indexes>` entry. Net granules 12/12 | same two `Skip` blocks, **plus** a third block `Name: <Combined skip indexes>`. Net granules 12/12 | **moved-correct** | `assert_prunes_at_least(gated, control = SETTINGS use_skip_indexes = 0, k = 1)` in the test itself, plus the 100k-corpus A/B below: the extra block changes no granule on either server |
| 2 | `query_log_gates :: body_search_skip_index_prunes_most_granules` | `SelectedMarks` 1/14; `read_rows` 8_192; `read_bytes` 1_556_440 | `SelectedMarks` **13/14**; `read_rows` 8_192; `read_bytes` **1_382_508** | **moved-correct** (and the byte number is **moved-better**) | `assert_index_pruning_by_bytes` — gated vs a same-server `use_skip_indexes = 0` control, both measured in the run: 16_384 rows / 1_572_824 B against 108_192 rows / 19_188_850 B = **6.60× rows, 12.20× bytes**, against pre-committed floors of 3× and 4× |

### Row 1 — the `<Combined skip indexes>` block

**What it is.** 26.x emits an extra pseudo-block when a filter mixes AND and
OR over skip-indexed columns. At the time of this study `!=` rendered
`NOT (hasToken(body,…) AND hasToken(body,…) AND position(body,…) > 0)` — a
negated conjunction, i.e. a disjunction — which is exactly that shape. The
other three stage-3 line filters (`|=`, `|~`, `!~`) did not produce it.

> **Superseded for `!=` by issue #450 (2026-08-13), which does not change
> this study's conclusion.** The `hasToken` conjunction returned wrong rows
> and was deleted; `!=` now renders `NOT (body LIKE '%…%')`, a single
> negated predicate, and no longer carries the pseudo-block. The shape that
> mixes AND and OR over `body` under the new rendering is the `or` group
> (`((body LIKE …) OR (body LIKE …))`), and that is where
> `explain_indexes.rs` now asserts `CombinedSkip::Present`. Row 1's finding
> — the block is a reporting addition that changes no granule — is what
> carried over; the table below is the 2026-07 measurement of the old
> rendering, kept as the record of it.

**Why it is not a regression.** Measured on the real `log_samples` DDL with a
100 000-row corpus, `EXPLAIN indexes = 1`, all four line-filter shapes on
both servers:

| shape | 24.8 net granules | 26.3 net granules | combined block on 26.3 |
|---|---|---|---|
| `\|=` (contains) | 4 / 12 | 4 / 12 | absent |
| `!=` (not contains) | 12 / 12 | 12 / 12 | **present** |
| `\|~` (regex literal) | 4 / 12 | 4 / 12 | absent |
| `!~` (regex metachar) | 12 / 12 | 12 / 12 | absent |

A bloom filter cannot rule a granule out for a negated predicate, so the two
negated shapes select every granule on both versions — the block is a
reporting addition on a shape that already pruned nothing.

**What changed in the gate, and why it is not weaker.** The stage-3
expectation used to be a `Vec<String>` compared with `assert_eq!`, which
asserted the *order* of the two `Skip` blocks. It now asserts:

- the `MinMax`/`Partition`/`PrimaryKey` prefix as a **committed, ordered**
  literal, key names included;
- the `Skip` blocks as a **committed SET** of `Name:` + `Description:`
  (`<type> GRANULARITY <n>`) pairs — the set is what drops the order, the
  `Description:` is what pins each block's identity;
- the `<Combined skip indexes>` presence as an explicit per-shape boolean,
  never folded into the block set;
- an in-run pruning identity against a same-server control.

**A first attempt at this was wrong and code review caught it.** The key
list and the skip-index names were briefly *derived* from
`system.tables.sorting_key` and `system.data_skipping_indices`. The planner
reads the same catalog, so an expectation derived from it is tautological:
removing `idx_body_ngrams` from the DDL moved both sides together and the
gate **passed on a table that had lost an index**, where the old literal
would have failed. A moved plan must never be replaced by something that
would pass on a worse configuration. The expectation is now written down,
where only a human edit moves it, and three DDL breaks were run to prove it:

| break | before (derived) | now (committed) |
|---|---|---|
| remove `idx_body_ngrams` from `log_samples` | passed 1/1 | **fails 5/5** stage-3 shapes |
| keep `idx_body_tokens`'s NAME, change its type/granularity | not detectable | **fails** — this is what `Description:` buys |
| reorder `ORDER BY (service, fingerprint, timestamp_ns)` | passed | **fails** on the prefix |
| leave the index declared, correct and granular, but make it rule out NO granule | passed | **fails 3/3** shapes that are supposed to prune |

The last row is round 2's finding and needed two changes. First, a skip
block's `Condition:` is now captured, and its absence recorded explicitly as
`Condition: <none>` — but for the bloom-filter family ClickHouse emits none at
all, which is a fact about the server rather than an omission. Measured on
26.3.17.110 over five index types on one table:

| index type | `Condition:` under its `Skip` block |
|---|---|
| `minmax` | `Condition: (severity in [4, +Inf))` |
| `set` | `Condition: (fingerprint in 2-element set)` |
| `tokenbf_v1` | **none emitted** |
| `ngrambf_v1` | **none emitted** |
| `bloom_filter` | **none emitted** |

Stage 3's two indexes are the two bloom filters, so there is no condition text
to pin for them and there never was — the pre-#376 literal did not carry one
either. So, second: the fixture now seeds a **100 000-row corpus** for the
line-filter shapes, because on the previous two-row fixture every index
trivially selects 1/1 and a dead one is indistinguishable from a working one.
With a corpus, whether the filter rules granules out is observable, and each
shape declares how much it must prune against a same-server
`use_skip_indexes = 0` control — `Strongly` (4x floor; measured 14 → 2
granules) for `|=`/`|~`, `NotAtAll` (never worse) for `!=`/`!~`, which is the
honest claim for a negated predicate on either server. The break above makes
every row carry the needle: names, types and granularities stay identical, the
filters rule out nothing, gated and control both select 14, and the three
positive shapes red.

Order is dropped deliberately, and it is the one part of the derivation that
was right. **On 24.8 the `Skip` block order follows the DDL declaration
order; on 26.3 the planner chooses it** — a two-table probe differing only in
declaration order gives `tokens, ngrams` then `ngrams, tokens` on 24.8, and
the *same* order on 26.3 for both tables. It is also data-dependent on 26.3:
a 50k fixture reports `ngrams, tokens` where 24.8 reports `tokens, ngrams`
for the identical query and DDL, while a 100k fixture with a different body
reports `tokens, ngrams` like 24.8. Net granule selection is identical in
every one of those cases. So order was never a correctness property, and on
26.3 it is not even a property of the schema — asserting it would redden on
data volume alone.

**Note on the plan's prediction.** The plan recorded a skip-index order flip
between the versions on the stage-3 shape and predicted the
`<Combined skip indexes>` block would be absent on every currently-gated
shape. On the suite's own two-row fixture neither holds: the order is
`tokens, ngrams` on both servers, and the combined block **is** present on
the gated `!=` shape. The flip is real on other fixtures, as the paragraph
above measures — it is fixture-dependent, which is itself the argument for a
set.

### Row 2 — `SelectedMarks` stopped measuring pruning

**Cause.** `use_skip_indexes_on_data_read`, default-enabled at **26.1**
(`SELECT version, changes FROM system.settings_changes` on 26.3.17.110:
`26.1 … ('use_skip_indexes_on_data_read','1','1','Default enable')`). It
moves skip filtering out of mark selection and into the data read, so all
marks are selected and the filtering happens inside them.

**The old assertion FAILS, it does not drift.** `SelectedMarks/total_marks
<= 0.5` evaluates 13/14 = 0.929 on 26.3 against 1/14 = 0.071 on 24.8, for
the identical query on the identical corpus.

**The read did not get worse.** Same query, same corpus shape, both servers:
`read_rows` is 8_192 on both, and `read_bytes` is 1_556_440 on 24.8 against
1_382_508 on 26.3 — **11% fewer bytes**. Only the instrument moved.

**The replacement is an identity, not a re-tuned constant.** The gate now
runs the same SQL twice against the same server in the same test, once
gated and once with `SETTINGS use_skip_indexes = 0`, and requires the gated
form to read at least 4× fewer bytes and 3× fewer rows. Both numbers come
from `system.query_log` in that run, so a pasted expectation cannot satisfy
it. Measured margin on the run that introduced it: 12.20× bytes, 6.60× rows.

### Row 3 — the label-cache sweep's memory ceiling

| # | shape | 24.8.14.39 | 26.3.17.110 | verdict | evidence |
|---|---|---|---|---|---|
| 3 | `live_metrics_cache :: a_memory_bounded_sweep_failure_retains_the_last_good_snapshot` | a 10-row sweep fits under a **1 KiB** `max_memory_usage`; a 200 010-row sweep breaches it | the 10-row sweep needs **1.17 MiB** before it reads anything, so it breaches 1 KiB and the test's first half fails | **moved-correct** (a fixture constant, re-derived) | the sweep run at candidate ceilings over both corpora on both servers — table in the test |

Not in the 79, because it is a `system.query_log`-free behaviour test rather
than a plan gate; recorded here because it moved on the bump and required an
edit. 26.3 reserves more memory per query before reading anything, so the
test's deliberately tiny ceiling had to move from 1 KiB to 4 MiB. Its
discriminating property is intact — small sweep succeeds, widened sweep
returns `241` — with 3.4x headroom below and a widened corpus that asks for
20.01 MiB at a 16 MiB ceiling. **`reader.promql_read_max_memory_bytes`'s
product default is untouched**; only the fixture moved.

---

## 2. The 77 shapes that did not move

Every one is **unchanged**: the suite passes on 26.3.17.110 with no edit to
its expectation. The enumeration is the test binaries', not a hand list —
`cargo test -p <crate> --test <suite> -- --list | grep ': test$'`.

| suite | gated shapes | verdict |
|---|---|---|
| `pulsus-read :: explain_indexes` | 30 (of which 1 moved, row 1 above; 5 more re-use the same rebuilt expectation) | unchanged except row 1 |
| `pulsus-read :: patterns_explain` | 3 | unchanged |
| `pulsus-read :: traces_point_read` | 2 | unchanged |
| `pulsus-read :: traces_search_explain` | 1 | unchanged |
| `pulsus-read :: traces_metrics_explain` | 1 | unchanged |
| `pulsus-read :: traces_tags_explain` | 2 | unchanged |
| `pulsus-read :: traces_graph_explain` | 1 | unchanged |
| `pulsus-read :: query_log_gates` | 12 (of which 1 moved, row 2 above) | unchanged except row 2 |
| `pulsus-schema :: live_traces` | 12 | unchanged |
| `pulsus-schema :: live_hist_schema` | 5 | unchanged |
| `pulsus-schema :: live_schema` | 10 | unchanged (`check_version_accepts_the_live_test_servers_reported_version` now requires >= 26.3, which is the floor move, not a plan move) |
| **total** | **79** | **2 moved, 77 unchanged, 0 regressions** |

`live_traces::narrow_time_window_prunes_granules_within_a_fixed_key_val_prefix`
was checked specifically, because its `last_granules` helper takes the LAST
`Granules:` line and 26.x can append a block: the trace shapes it gates carry
no disjunction over a skip-indexed column, no `<Combined skip indexes>` block
appears, and the suite passes unchanged.

**Two new `EXPLAIN indexes = 1` lines** appear on 26.3 and are outside the
extract by construction: `Search Algorithm: binary search` /
`generic exclusion search` under `PrimaryKey`, and `Ranges: N`. Neither is a
block title, neither starts `Condition:`/`Name:`, and both carry a `:` so
they also close a `Keys:` run — verified line by line, and by the fact that
the only extract difference the suite reported was the combined block.

---

## 3. The two settings decisions

Each rule was fixed **before** its run.

### 3.1 `use_skip_indexes_on_data_read` — **adopt 26.3's default (`1`)**

> **Pre-committed rule.** Adopt the 26.3 default unless the `=1` leg's
> `read_bytes` on the gate corpus exceeds the `=0` leg's by more than 5%.

Same server (26.3.17.110), same query, `use_query_condition_cache = 0`, one
warmup pass discarded, then 5 reps taken interleaved (`=1`, `=0`, `=1`, …):

| rep | `=1` read_bytes / read_rows / marks | `=0` read_bytes / read_rows / marks |
|---|---|---|
| 1 | 1_382_508 / 8_192 / 13 | 1_382_508 / 8_192 / 1 |
| 2 | 1_382_508 / 8_192 / 13 | 1_382_508 / 8_192 / 1 |
| 3 | 1_382_508 / 8_192 / 13 | 1_382_508 / 8_192 / 1 |
| 4 | 1_382_508 / 8_192 / 13 | 1_382_508 / 8_192 / 1 |
| 5 | 1_382_508 / 8_192 / 13 | 1_382_508 / 8_192 / 1 |

**Zero difference in bytes and rows, on every rep** — the distribution has no
spread at all. `SelectedMarks` is the only thing that moves, which is what
disqualified it as the gate's metric (§1 row 2). 0% ≤ 5%, so the rule adopts
the default: nothing is pinned, and the read path inherits it.

### 3.2 `async_insert` — **pin `0`**

ClickHouse flipped this `0 → 1` at **26.2**
(`system.settings_changes`: `('async_insert','0','1','Enable async inserts by
default.')`). `wait_for_async_insert` is `1` on both versions, so
read-your-write is preserved either way.

> **Pre-committed rule.** Pin `0` unless the default-on leg is ≥10% faster
> across `pulsus-write`'s live suites **and** `logs_tail_live` passes on both
> legs with no deadline change. Prior, to be overturned only by the number:
> pin `0`.

Two 26.3.17.110 containers differing **only** in `async_insert` (a
`users.d` profile override on one), five `pulsus-write` live suites
(`live_metric_writer`, `live_log_stream_backfill`, `ingest_fidelity`,
`live_metric_hist_writer`, `trace_ingest_roundtrip`), compiled before timing,
one warmup pass per leg discarded, then 5 reps taken interleaved:

| rep | `async_insert = 1` (26.3 default) | `async_insert = 0` | both pass? |
|---|---|---|---|
| warmup | 22.35 s | 12.69 s | — |
| 1 | 23.51 s | 12.77 s | yes |
| 2 | 22.52 s | 12.69 s | yes |
| 3 | 23.98 s | 13.38 s | yes |
| 4 | 22.66 s | 13.72 s | yes |
| 5 | 23.92 s | 14.51 s | yes |

Median 23.51 s against 13.38 s — the default-on leg is **1.76× slower**, with
**no overlap between the two distributions**. It is not ≥10% faster; it is
76% slower. `logs_tail_live` passes on both legs with no deadline change
(10/10 on each). The rule pins `0`, and the prior stands.

**Why the default is a loss here.** Async insert exists to coalesce many
small inserts. `ChClient::insert_block` already sends one columnar block per
batch, so the default adds only the adaptive busy timeout (50–200 ms) on the
way to a flush the batch did not need.

**Where the pin applies, and how its domain was corrected.** It started on
`ChClient::insert_settings_of` alone, which covers `insert_block`. Reading
`system.query_log` back showed that left `ChClient::execute`'s INSERTs
inheriting the new default — in production that is `pulsus-schema`'s
`schema_migrations`/`mv_checksums` bookkeeping, which writes a row and later
reads it back, i.e. read-your-write.

The consequence was measurable, and it is what found the gap:
`logs_tail_live`, whose fixtures seed through `execute`, failed **3 of 6
runs** against a stock 26.3 server and passed **6 of 6** against an
otherwise identical 26.3 server whose profile pinned `async_insert = 0`.
That is a discriminating measurement, not an inference: the only difference
between the two servers was that setting.

The pin now sits on **both** paths. Verified from the server rather than
argued: after a fresh `logs_tail_live` run,
`SELECT Settings['async_insert'], count() FROM system.query_log WHERE
query_kind = 'Insert'` over the run's window reports **2 709 inserts, all
carrying `'0'`, and none without it** — the claimed domain ("we do not use
async inserts") and the checked domain are the same set. With that coverage
`logs_tail_live` passed 10 of 10 consecutive runs against the stock 26.3
server.

### 3.3 `use_query_condition_cache` — pinned to `0` in the gate harness only

Not a product decision: the query-condition cache (default-on from **25.4**)
can only *reduce* `read_rows`/`read_bytes`/`SelectedMarks`, so it cannot
redden a gate wrongly — but it can make one pass by having cached rather than
by having pruned. `query_log_gates::run_and_capture` pins it to `0` and reads
the value back out of `system.query_log.Settings`, so deleting the pin
reddens the suite. The product read path is unchanged and keeps the default.

---

## 4. Two things code review found that this document had claimed closed

**The version leak had a third path.** The hermetic fixture-derived check and
the live `SELECT version()` check each covered their own route (427 / 422),
and a route fell between them: `ReadError::Clickhouse(ChError::Server { .. })`
is rendered with `e.to_string()` by the logs, Prometheus **and** traces
surfaces, which preserved ClickHouse's `(version X.Y.Z.W (official build))`
tail verbatim. Fixing it at those three call sites would have left a fourth
surface free to leak, so the redaction sits on `ChError::Server`'s `Display` —
the one place a server message becomes a rendered string. The `message` field
is untouched, so every parser and archived-capture test is unaffected. Each of
the three surfaces now has its own test over a real 26.3 body, plus four unit
tests on the redactor (single banner, three nested banners, field untouched,
no-banner pass-through).

**The `async_insert` pin missed the benchmark clients.** `ChClient` covers the
product; `xtask/src/ch_bench` builds its own HTTP and native clients and
inherited 26.3's `async_insert = 1`. Not a correctness defect — it is harness
code — but a **measurement-validity** one: a benchmark that informs a
read-path decision has to run the settings the shipped system runs, or its
numbers describe a system we do not ship, and on this setting the gap measured
1.76x. Pinned at each client's construction rather than per call site, so it
covers inserts, `INSERT … SELECT`, DDL and fetches alike. A census of every
`clickhouse::Client::` / `klickhouse::Client::` construction in the tree found
a fourth site the review had not named — the TLS HTTP candidate — which is
pinned too. Verified from the server: after a bench run,
`system.query_log` reports `async_insert = '0'` on **both** interfaces
(native and HTTP) for every bench insert.

## 5. Scale-dependent effects that are NOT claimed here

Routed to **#25** by name, because CI cannot reach the scale where they
matter: lazy materialization's benefit at 1 TB, the query-condition cache's
hit rate under a real workload, `use_skip_indexes_on_data_read` at part
counts CI cannot produce, and `async_insert`'s throughput effect on a
replicated cluster. Every number above is a scale-invariant ratio or an
identity measured against a same-server control.
