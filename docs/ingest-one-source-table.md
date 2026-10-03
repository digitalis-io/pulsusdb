# One push, one insert into one table

**What this is.** The implementation design of the metrics ingest write path as it stands
on `main` after issue #603: one landing table, one insert per push, and one materialized
view per target table. It states the decisions, the guarantees, and for each guarantee
what holds it up.

**Who it is for.** Someone about to do the same thing for logs or for traces. §10 is the
only part that differs per signal; everything above it is the pattern, and nothing in it
has to be decided again.

**Nothing has ever shipped.** No users, no deployments, no stored data. There is no
migration and no compatibility obligation anywhere in this document.

**How it cites.** A claim points at a path and a symbol name, never at a line number: a
name is searchable and a line number rots. An absolute claim — "the only place", "nothing
reads it" — either names the check that holds it (§11) or carries the search that supports
it. No figure here was measured against a running server: every one is a constant in the
tree, and the arithmetic over them is shown where it is used.

---

## 1. The shape

**One push becomes one `INSERT` of one block into `metric_landing`.** Four materialized
views maintain `metric_samples`, `metric_hist_samples`, `metric_series` and
`metric_metadata` from that one table. The writer inserts into none of the four and
names none of them.

**This is the shape the engine is built for**: one source table, one materialized view
per derived table, and the fan-out performed by the server as part of processing that one
insert. So there is no window in which some of a push's tables are written and others are
not because the process stopped. **What it does not give:** the fan-out across the four
targets is not a transaction — §9 D1 says what an observer sees when one view throws.

Three things depart from the plainest form of that shape, each stated where it is
decided: one source table feeds four targets with different column sets, so it carries a
discriminating column (below); the writer leaves the identity column out of the insert so
the server's own default fills it (§1.1); and every setting that could divide the request
into more than one block is pinned on the insert rather than inherited from the server's
profile (§2.1).

`kind` is the discriminating column. A row sets its kind's columns and leaves the rest
at the column type's default.

| `kind` | the landed event | its target | its view |
|---|---|---|---|
| 0 | a float sample | `metric_samples` | `metric_samples_mv` |
| 1 | a native-histogram sample | `metric_hist_samples` | `metric_hist_samples_mv` |
| 2 | a series registration | `metric_series` | `metric_series_mv` |
| 3 | a metadata descriptor | `metric_metadata` | `metric_metadata_mv` |

The four target tables' columns, keys and engines did not change. What changed is who
writes them.

### 1.1 The landing table

`crates/pulsus-schema/src/catalog.rs`, `MIGRATIONS` id 64, `Ddl::Static`,
`MigrationScope::Checksum`, `Replication::PerShard`. Every statement quoted in §1 and
§1.3 is the template as it stands in the tree, token for token, re-indented for reading;
`{{db}}` and `{{on_cluster}}` are substituted at render time:

```sql
CREATE TABLE IF NOT EXISTS {{db}}.metric_landing{{on_cluster}} (
    event_id                 UUID DEFAULT generateUUIDv7(),
    received_ms              Int64  CODEC(DoubleDelta, ZSTD(1)),
    kind                     UInt8  CODEC(ZSTD(1)),
    metric_name              LowCardinality(String),
    fingerprint              UInt128  CODEC(Delta(8), ZSTD(1)),
    unix_milli               Int64  CODEC(DoubleDelta, ZSTD(1)),
    value                    Float64  CODEC(Gorilla, ZSTD(1)),
    labels                   String  CODEC(ZSTD(5)),
    value_type               UInt8  CODEC(ZSTD(1)),
    metric_type              LowCardinality(String),
    help                     String  CODEC(ZSTD(1)),
    unit                     String  CODEC(ZSTD(1)),
    updated_ns               Int64  CODEC(DoubleDelta, ZSTD(1)),
    hist_schema              Int8  CODEC(ZSTD(1)),
    hist_zero_threshold      Float64  CODEC(Gorilla, ZSTD(1)),
    hist_zero_count          UInt64  CODEC(T64, ZSTD(1)),
    hist_count               UInt64  CODEC(T64, ZSTD(1)),
    hist_sum                 Float64  CODEC(Gorilla, ZSTD(1)),
    hist_pos_span_offsets    Array(Int32)  CODEC(ZSTD(1)),
    hist_pos_span_lengths    Array(UInt32)  CODEC(ZSTD(1)),
    hist_pos_bucket_deltas   Array(Int64)  CODEC(ZSTD(1)),
    hist_neg_span_offsets    Array(Int32)  CODEC(ZSTD(1)),
    hist_neg_span_lengths    Array(UInt32)  CODEC(ZSTD(1)),
    hist_neg_bucket_deltas   Array(Int64)  CODEC(ZSTD(1)),
    hist_custom_values       Array(Float64)  CODEC(ZSTD(1)),
    hist_counter_reset_hint  UInt8  CODEC(ZSTD(1))
) ENGINE = MergeTree
PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))
ORDER BY (kind, metric_name, fingerprint, unix_milli)
SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600;
```

Decisions inside that statement, each of which a second signal has to make again:

- **The sorting key starts with the discriminating column**, so each view's `WHERE kind
  = k` is a primary-key range rather than a scan.
- **The partition key is the receive stamp**, floored to the hour. Every row of one push
  carries the same `received_ms` (`MetricWriter::admit_batch` takes it once), so a
  block lies in one partition — which is what keeps `max_partitions_per_insert_block`
  out of the settings a push has to reason about (§2.1).
- **`event_id` is the landed event's identity and the writer never sets it.**
  `MetricLandingRow` declares the other 25 columns, and the insert's column list is
  exactly that row type's `COLUMN_NAMES`, so the server fills the column from its own
  default. A row type carrying the column would store whatever the writer put there.
  `event_id` is not the retry mechanism (§3) and no target holds it.
- **The two engine settings that are fixed sit here.** The delete-TTL and the
  deduplication window carry configuration values, so they are applied at run time
  instead (§1.3): migration identity is checksummed over the rendered template, so a
  configurable value inside this `CREATE` would read as schema drift the first time a
  deployment changed it.
- **No `Ddl::Dist` sibling.** This table has no distributed wrapper, so
  `chconfig::metric_writer_tables_from` returns the bare name in every mode, clustered
  or not. The four targets keep their wrappers for reads.

### 1.2 The four views

`crates/pulsus-schema/src/catalog.rs`, `MVS`. Each projection lists the target's
columns **in the target's own column order**, aliased to the target's column names, so
the view is correct whether the server matches by position or by name.

```sql
CREATE MATERIALIZED VIEW {{db}}.metric_samples_mv{{on_cluster}} TO {{db}}.metric_samples AS
SELECT metric_name AS metric_name, fingerprint AS fingerprint,
       unix_milli AS unix_milli, value AS value
FROM {{db}}.metric_landing WHERE kind = 0;

CREATE MATERIALIZED VIEW {{db}}.metric_hist_samples_mv{{on_cluster}} TO {{db}}.metric_hist_samples AS
SELECT metric_name AS metric_name, fingerprint AS fingerprint,
       unix_milli AS unix_milli, hist_schema AS schema,
       hist_zero_threshold AS zero_threshold, hist_zero_count AS zero_count,
       hist_count AS count, hist_sum AS sum,
       hist_pos_span_offsets AS pos_span_offsets,
       hist_pos_span_lengths AS pos_span_lengths,
       hist_pos_bucket_deltas AS pos_bucket_deltas,
       hist_neg_span_offsets AS neg_span_offsets,
       hist_neg_span_lengths AS neg_span_lengths,
       hist_neg_bucket_deltas AS neg_bucket_deltas,
       hist_custom_values AS custom_values,
       hist_counter_reset_hint AS counter_reset_hint
FROM {{db}}.metric_landing WHERE kind = 1;

CREATE MATERIALIZED VIEW {{db}}.metric_series_mv{{on_cluster}} TO {{db}}.metric_series AS
SELECT metric_name AS metric_name, fingerprint AS fingerprint,
       unix_milli AS unix_milli, labels AS labels, value_type AS value_type
FROM {{db}}.metric_landing WHERE kind = 2;

CREATE MATERIALIZED VIEW {{db}}.metric_metadata_mv{{on_cluster}} TO {{db}}.metric_metadata AS
SELECT metric_name AS metric_name, metric_type AS metric_type, help AS help,
       unit AS unit, updated_ns AS updated_ns
FROM {{db}}.metric_landing WHERE kind = 3;
```

`unix_milli` on a kind-2 row is the series activity-bucket floor, not a sample time —
the same contract the target row type carried before.

### 1.3 The statements applied at run time

`controller::METRIC_LANDING_STMTS`, appended to the statements `apply_ttl` itself runs,
so `run_init`, the rotation task and the server's rotation tick all reapply them with no
new wiring and no call site left to miss:

```sql
ALTER TABLE {{db}}.metric_samples{{on_cluster}} MODIFY SETTING non_replicated_deduplication_window = {{metrics_dedup_window}};
ALTER TABLE {{db}}.metric_series{{on_cluster}} MODIFY SETTING non_replicated_deduplication_window = {{metrics_dedup_window}};
ALTER TABLE {{db}}.metric_metadata{{on_cluster}} MODIFY SETTING non_replicated_deduplication_window = {{metrics_dedup_window}};
ALTER TABLE {{db}}.metric_hist_samples{{on_cluster}} MODIFY SETTING non_replicated_deduplication_window = {{metrics_dedup_window}};
ALTER TABLE {{db}}.metric_landing{{on_cluster}} MODIFY TTL toDateTime(least(intDiv(received_ms, 1000) + {{metrics_landing_retention_hours}} * 3600, 4294967295)) DELETE;
ALTER TABLE {{db}}.metric_landing{{on_cluster}} MODIFY SETTING non_replicated_deduplication_window = {{metrics_dedup_window}};
```

- **One window per table, on all five.** The landing table's own window is what
  recognises the writer's resend of a block (§3). Each of the four targets needs one of
  its own because a view's insert into its target carries a block id derived from the
  source block, and only a table with a window recognises the repeat.
- **How many windows there are depends on the engine, and the setting's name does too**
  (issue #603). A **non-replicated** table has one: `non_replicated_deduplication_window`,
  counted in blocks, with no seconds-based counterpart — so a block is remembered until
  that many newer blocks have arrived and is never forgotten on a timer. Running the
  startup check of §2.2 is what established that: the seconds name a design had assumed
  does not exist for this engine, and sending it would have refused every startup. A
  **replicated** table has two, and both have to be right:
  `replicated_deduplication_window` (blocks, server default 10000) and
  `replicated_deduplication_window_seconds` (seconds, server default 3600), whose own
  description says hash sums older than it are removed "even if they are less than
  `replicated_deduplication_window`", timed from the most recent record rather than from
  the wall clock. A deployment that lowered the seconds window below the landing budget
  would have a token forgotten while the writer was still entitled to resend under it.
  So the statements above render `{{dedup_window_setting}}` — the replicated name when
  `PULSUS_CLUSTER` is set, the non-replicated one when it is not, from the same context
  field that renders the engine — and a clustered deployment additionally gets
  `controller::CLUSTER_DEDUP_SECONDS_STMTS`, one
  `MODIFY SETTING replicated_deduplication_window_seconds = 3600` per write-path table.
  **3600 is a constant, not a key**: the precondition of the defect is a deployment
  setting it too small, it is the server's own default so pinning changes nothing where
  the server keeps it, and it is thirty times the 120-second landing budget that bounds
  the one thing the window guards — the writer resending its own block. A clustered
  deployment is the production case, so this is the shape that matters; the
  non-replicated window is the development variant.
- **`apply_ttl` stops at its first failing statement, so the order is a dependency
  order.** These come after the shipped `TTL_STMTS`, and the two naming the landing
  table come last within them, so a schema managed by hand without that table stops
  nothing that does not name it.
- **The landing table is the replay window.** A view never reconciles against its
  source; it reacts to inserts. So a target that ends up wrong can be rebuilt only from
  landed rows that are still there, and `PULSUS_METRICS_LANDING_RETENTION_HOURS` is how
  long that is. `pulsusdb rebuild-metrics` replays a window of the landing table through
  the same projection the view applies, read out of the view's own rendered statement
  (`pulsus_schema::mv_projection`) so the two cannot drift. It is a command run by hand
  and it has no tests (§9 D15). **Traces have the same thing**:
  `replay_trace_window`, bounded by `PULSUS_TRACE_LANDING_RETENTION_HOURS`,
  replays a window of `trace_landing` through all five of its views'
  projections — also run by hand, also untested, and **nothing detects that
  it is needed** (`docs/TraceQL/server-implementation.md` §2.5).

---

## 2. One push, one block

A push's rows are never spread over two inserts, and an insert never carries two
pushes. Three things together make that true: the block cannot be divided by the
server (§2.1), a push too large for one block is refused whole rather than split
(§2.3), and a server that might not honour one of the pinned settings is refused before
it is sent one (§2.2).

### 2.1 The settings every landing insert pins

`QuerySettings::landing_insert(token, max_rows)` in
`crates/pulsus-clickhouse/src/settings.rs`. Ten settings, and that constructor's own
doc comment is the owning passage for the catalogue wording behind each value, for the
two searches the class was derived from, and for each candidate setting that needs no
pin. `max_rows` is `PULSUS_METRICS_LANDING_MAX_ROWS`.

| setting | pinned to | why it is in the set |
|---|---|---|
| `max_insert_block_size` | `max_rows` | the maximum pair's row half: either maximum reached emits a block |
| `max_insert_block_size_bytes` | `0` | the maximum pair's byte half; `0` is the value its own entry calls not participating |
| `min_insert_block_size_rows` | `max_rows` | the minimum pair's row half: the pair emits when both halves are reached |
| `min_insert_block_size_bytes` | `0` | the minimum pair's byte half, likewise not participating |
| `input_format_max_block_size_bytes` | `0` | the byte limit on the blocks the input format forms; `0` is no limit |
| `input_format_max_block_wait_ms` | `0` | otherwise a block can be emitted because time passed |
| `input_format_connection_handling` | `0` | enabling it makes deduplication impossible, which defeats the token rather than splitting the push |
| `insert_deduplication_token` | the block's minted token | §3 |
| `deduplicate_insert` | `enable` | it overrides `insert_deduplicate` and `async_insert_deduplicate`, so one pin closes all three |
| `deduplicate_blocks_in_dependent_materialized_views` | `1` | each view's insert then carries a block id derived from the source block, so the targets' own windows see the repeat |

Every byte limit and the wait are pinned to the value that takes no part; the
connection handling to the value that leaves deduplication possible; both row counts to
the ceiling admission has already refused a larger push against. A row count is then
the only thing left that can end a block, and no admitted push reaches it.

**They are pinned rather than inherited.** Four of them default to where this pins them,
but a server profile may set any of them, and then a push well inside the row ceiling
becomes several blocks and a prefix of it can commit alone. What pinning them off
costs: a deployment that set a byte limit to bound the memory one insert takes does not
get it on this insert — `PULSUS_BATCH_BYTES` refuses the push whole instead (§2.3) — and
one that enabled connection handling to salvage a broken upload's buffered rows does not
get that either; a broken upload is a failed attempt the writer resends under the same
token.

### 2.2 A server missing one of those names is refused before it is sent it

`pulsus_schema::REQUIRED_SERVER_NAMES` lists every setting and function name this build
sends that was not already somewhere in `crates/` before this work, each with the
`system` catalogue that answers whether it exists: the ten settings of §2.1 less the two
that predate this work, `merge_with_ttl_timeout` from the `MergeTree` settings catalogue,
and the functions `generateUUIDv7`, `toStartOfHour` and `tupleElement`. Twelve names.

- **One statement per catalogue, not one joined statement.** A deployment's database
  user may hold `SELECT` on some of these `system` tables and not others, and a joined
  statement fails whole on the one it cannot read, leaving a readable catalogue
  unchecked because of an unreadable one.
- **A catalogue the user cannot read is unchecked, not a refusal,** and nothing wider
  than that: `controller::catalogue_read_is_unchecked` tolerates the access-denied code
  alone. A timeout, a transport fault or any other server exception is returned, because
  it says nothing about grants and continuing would let startup send a name nothing
  checked. A name a readable catalogue reports absent still refuses.
- **The check runs before any DDL.** In `--mode init` it runs before `run_init`, so a
  refusal leaves no database behind, and the process exits non-zero naming the absent
  names. In a serving process it is the first step of `ensure_schema_then_connect`, and
  outside the `PULSUS_SKIP_DDL` branch, because such a deployment reaches no DDL and
  still inserts landing blocks. A missing name there is terminal rather than transient:
  the reconnect loop logs every absent name and returns, no pool and no writer publish,
  and readiness stays at its failure status.
- **The two lists are held against each other, both ways.** A pin added to
  `landing_insert` without a `REQUIRED_SERVER_NAMES` row, or a row without a pin, fails
  the check named in §11.

### 2.3 A push that does not fit one block is refused whole

Both ceilings are counted over all four kinds and decided before either branch of
admission queues anything:

- **rows** `>= PULSUS_METRICS_LANDING_MAX_ROWS` — a push at or above that count is not
  strictly under the count both pinned row limits carry, and the server would end a
  block at it.
- **estimated bytes** `> PULSUS_BATCH_BYTES`, where the estimate is the whole charge of
  §6, the block's own overhead included.

Either is `AdmitRefusal::PushTooLarge { rows, row_limit, bytes, byte_limit }`, answered
`413` on both metrics transports with a message naming the push's own size and both
limits. Nothing is stored, no bytes stay reserved, and the un-sealed suppression guard
is dropped — which removes the claim, so a client's retry of an unstored push is stored
rather than suppressed. A push the suppression index recognises as a repeat is refused
the same way and for the same reason: it does not fit one block, so storing any part of
it would leave a `413` that stored something.

**A valid push with no rows of any kind makes no block and no insert**, and is charged
for none: one block's fixed overhead exceeds the smallest accepted value of either byte
limit, so charging an empty push for a block that is never created would have refused it
as too large. Its claim seals with no targets and completes at admission, and the caller
is answered a success.

---

## 3. A resend stores nothing twice

Two independent layers, answering different questions.

**The client's re-push** is issue #494's suppression index (`writer::push_dedup`): a
push whose identity the index already holds is suppressed before the insert, and the
caller is answered with the original push's outcome, with the one exception below.
Everything about a client's re-push — its suppression, its responses and its index — is as
it shipped before this work. One detail is this path's: a suppressed push stores no sample,
series or histogram row and **still sends its descriptors once their own reservation is
granted**, as its own landing insert of kind-3 rows carrying no claim and no waiter, which
runs §4's loop like any other block — so a failure after it was sent leaves whether they
landed unknown. That block takes a byte reservation of its own (§6), and a queue with no
room for it refuses the push `429` instead: no descriptor row is built, queued or sent, and
the caller gets that refusal rather than the original push's outcome (§9 D16).

**The writer's own resend** — the same block sent again after an attempt whose fate is
unknown — is the deduplication token:

- The writer **mints** one token per sealed block (`mint_landing_token`): a time-ordered
  UUID string, fresh, **never derived from the block's content**, because two
  byte-identical metric blocks can be two genuine pushes. Minting takes one lock per
  admitted push, because a token drawn from the clock alone would repeat for two pushes
  inside one millisecond and the second would be dropped as a resend of the first.
- The token is built once, into the block's own settings, and sent byte-identical on
  every resend. The block's rows are not rebuilt between attempts.
- A resend of a block the server already committed is then recognised by that table's
  own deduplication window (§1.3) and stores nothing a second time. Resending an insert
  whose fate is unknown is new behaviour; the token is what makes it safe.

**What the window does not cover.** It counts blocks, never seconds. A window of `W`
blocks covers a steady rate of `W / 120` pushes a second — 120 s being the landing
budget of §4 — which is 83 at the default `W` of 10,000 and 8,333 at the maximum of
1,000,000, rounding down. Above that rate a token can be evicted before its resend
arrives and the block is stored twice. Nothing checks it: a push rate is not
configuration (§9 D17).

---

## 4. The insert loop, and what an observer sees

One worker takes a sealed block off the queue and runs it to exactly one ending.
`writer::metric`'s `run_block` is the loop; `PULSUS_METRICS_LANDING_INSERTERS` workers
run it concurrently against one queue.

**The classification is a value the loop carries, not a decision each ending makes.**
`LandingFate` is either `NeverSent(msg)` or `Uncertain(msg)`. Two methods write it and
there is no third: `saw_pre_send` is what an ending whose own knowledge is "this did not
send" calls, and it **cannot** reverse an earlier `Uncertain` — the argument is dropped
in that case; `saw_uncertain` is what an ending that may have sent calls. One method
reads it, `LandingFate::settle`, giving `(Poison, NotCommitted, msg)` for `NeverSent`
and `(Uncertain, Uncertain, msg)` for `Uncertain`. It is the only place this path names a
spool directory or a non-committed outcome, so no ending picks its own.

A shutdown is not a fate. A block abandoned at the shutdown deadline may have committed
exactly as one abandoned at the budget may, so the cause travels beside the fate and
decides the waiting caller's error and nothing else.

| the ending | the fate it writes | then |
|---|---|---|
| the insert returned `Ok` | — | the flush is recorded, the block's new series are promoted into the registration cache, the reservation is released, a waiting caller is answered success |
| `ChError::InsertUncertain` | `saw_uncertain` | resends the identical block under the identical token after a sleep, while resends are left and the budget has time; otherwise settles |
| a retryable error | `saw_pre_send` | resends as above; otherwise settles |
| any other error | `saw_pre_send` | settles |
| the budget spent with the block still queued | `saw_pre_send` | settles; no attempt ever ran |
| the budget spent between attempts | `saw_pre_send` | settles, keeping whatever the attempts left |
| the budget elapsed inside an attempt | `saw_uncertain` | settles (§9 D6) |
| the shutdown deadline, attempt in flight | `saw_uncertain` | the attempt is abandoned; settles; the caller's error is "shutting down" |
| the shutdown deadline, block still queued | `saw_pre_send` | never sent; settles; the caller's error is "shutting down" |

Every error other than `InsertUncertain` reaching this loop is pre-send, because
`ChClient::insert_block_with` downgrades every failure from the first write onward to
`InsertUncertain` whatever its class, and the only failures before that are the
connection checkout's and the target's column-metadata read's.

**Two bounds, and the loop owns one of them.** The landing budget —
`WriterRuntime::landing_budget`, 120 s, measured from the push's admission — bounds the
queue wait, every attempt and every sleep. It is recomputed after every attempt, so a
sleep can never carry a block past it, and the check at the top of the loop is what ends
a block whose sleep spent what was left. The budget is derived rather than configured:
the claim deadline already reserves it, so the loop settles strictly before an open claim
could age into a tombstone. The other bound is the shutdown deadline, which is §5's and
which this loop neither reads nor reasons about.

**Every ending but a commit spools the block and answers a waiting sync caller `500`.**
The spool copy is written before the reservation is released, on the spool write's own
error path too, and a failed spool write is logged and counted and changes no outcome.
Success means the landing block committed: `204` on the remote-write route and `200` on
the export route (docs/api.md). An async-mode push is answered `202`, which means only
that the push was taken in — an async push that later fails reaches nobody.

**A spool file is an audit copy for a person to read, and nothing replays it.** The
`uncertain/` directory carries a note saying so beside the data. Nothing in this tree
reads a spool file back outside that module's own tests: `git grep -n
'read_dir\|File::open\|read_to_string\|fs::read' -- crates` at the merged revision
returns eight call sites under the spool root and all eight are inside
`crates/pulsus-write/src/writer/spool.rs`'s test module. There is no recovery path here
and none is designed — replaying a block whose fate is unknown is what the token exists
to make unnecessary while the window holds it, and past that it is a decision a person
makes.

---

## 5. Shutdown

`crates/pulsus-write/src/writer/drain.rs` owns the whole of it, and the landing path
states none of it again: **no attempt is authorized after the announced deadline, and
nothing the writer spawned is abandoned at it.**

**The ordering point is one lock, and the decision is inside the same critical section as
the act it decides.** `Deadline::publish` takes the lock to write the announced instant.
`Deadline::authorize` takes the same lock to read it **and calls the caller's constructor
inside that same section**. So every execution puts one strictly before the other: either
the publication went first and the authorization reads what it published, or the
authorization went first and the publication waits for the constructor to return. Nothing
acts on a stale read, because no read is separated from the act it decides. An attempt is
only ever created by this module calling that constructor, so authorization is the only
way work begins.

Creating an attempt and carrying it on are two different acts, and the deadline bounds
both: the request leaves on whichever poll of the attempt gets past the connection
checkout, so `DrainWatch::attempt` polls it only while no deadline is announced and
prefers the announcement to it in every selection that can park. The deadline arm
compares the clock against the announced instant on each poll rather than trusting a
timer to have fired. Every wait between attempts is `DrainWatch::sleep`, which ends at
the deadline whatever its own length; a wait that ended late would still start nothing,
because the next attempt is authorized afresh.

**Admission runs inside an `AdmissionPass`**, and `DrainBoundary::shutdown` waits for
every outstanding pass *before* it announces the deadline and *before* it joins the
settlement tasks. So no block reaches a closed queue from outside, and a settlement task
— which only a pass can spawn — cannot appear after the set of them has been joined.
However many callers ask for it, the drain is one operation under one mutex with one
deadline, the first announced, and each caller returns only once it has finished.

**A block reaching the deadline has one of the two fates of §4 and no third.** It was
never sent — the queue still held it, or the deadline refused the next attempt — or an
attempt was in flight and its fate is uncertain. The second is never downgraded to the
first, so a caller is told "unknown", never "not stored", for a block that might be
stored. That is the direction a claim has to err in, and every over-report in §9 errs the
same way.

**What the deadline does not bound is settling a block.** The drain finishes the audit
copy of §4 rather than dropping it at the deadline, and overruns by what that write
costs. A block is never lost to save time.

---

## 6. The memory accounting

`PULSUS_INGEST_QUEUE_BYTES` bounds what queued pushes hold. The rule is: **a charge
covers the memory it prices for that memory's whole life.**

- **The charge is taken before the rows are built.** Admission prices the push from the
  parsed batch, reserves that many bytes atomically, and only then materializes the
  rows. There are two such takes, both through `writer::reserve_queued_bytes`, and no
  early return between a take and the queue. Every refusal but backpressure is decided
  before any charge exists, and backpressure is that function's own: it reserves first
  and rolls back on overflow, so the gauge never under-reserves.
- **What is charged is the whole block, not its rows.** `LANDING_BLOCK_OVERHEAD_BYTES`
  prices everything a sealed block holds besides its rows — the block and the queue slot
  it is moved into, the ten owned settings pairs by capacity, the claim ticket's first
  allocation, and an allowance for the waiting caller's response channel — and
  `landing_charge` adds it to the rows' own estimate. One figure prices a push: the byte
  ceiling refuses against it, the reservation takes it, the release gives it back and the
  flush counter reports it, so no two halves of the accounting can drift apart.
- **So a block may hold nothing whose size grows with the push except its rows.** A
  field that did would need its own per-push term and this figure would stop bounding
  it. That is why the keys a commit promotes into the registration cache are derived
  from the block's own kind-2 rows rather than carried beside them.
- **One function releases, and it owns the order.** `LandingBlock::release` consumes the
  block: drop the rows, drop the settings, report the claim — which is where the
  ticket's own charged vector goes — subtract, and only then return the waiter for the
  caller to answer last. Eight terminal paths reach it. An ending that subtracted for
  itself would hand the allowance to a new admission with its own rows still in memory,
  and with the rows of every worker waiting behind the registration mutex too, so the
  configured ceiling would permit more than it names.
- The commit path promotes into the registration cache while still charged; the
  settlement path writes its audit copy while still charged. Both then release.
- **A census over the module's own source asserts that `release` is the only function
  that subtracts the gauge**, so an exit path nobody has written yet either goes through
  that function or the build's tests name it. Its limits are D9.

---

## 7. The bounded encoder

**The invariant, stated on the sink: nothing in the encoding path holds a copy that
grows with the input.** `SpoolSink` in `crates/pulsus-write/src/writer/spool.rs` holds
one chunk of at most `SPOOL_CHUNK_BYTES`, allocated once and never grown for the sink's
life. It matters because the reservation of §6 is held while the file is written, so
anything the encoder holds on top of the rows is memory the ingest byte bound does not
know about — and one accepted native histogram may carry
`MAX_BUCKETS_PER_HISTOGRAM_SIDE` bucket bounds, 65,536, in a single row.

Four routes in, and the sink's own comment tables each with what bounds it:

| route | what it takes | how it stays bounded |
|---|---|---|
| `put_bounded` | punctuation | the caller's literal, checked against the piece bound |
| `put_scalar` | a `SpoolScalar` | the type promises its text fits one piece |
| `put_str` | any `&str` | escaped and written a fixed number of input bytes at a time |
| `put_value` | any JSON value | walked on an explicit stack, every leaf through one of the three above |

**The enforcement is the compiler.** `field` and `array` take only `SpoolScalar`, whose
supertrait is nameable only inside the spool module. A `String`, a `Vec`, a nested value
or a scalar type nobody has added yet does not compile through the bounded route and has
to take a streaming one. Two run-time checks stand behind the seal: `put_bounded`
refuses an over-long piece in every build rather than only a debug one, and a case holds
each scalar type's widest value against the piece bound. What the types cannot catch is a
new scalar type whose author declares it bounded wrongly (§9 D14).

The metrics landing row overrides the default encoder and writes its fields, arrays and
text straight into the sink, so it builds no value of its own. Its keys are written in
sorted order, because that is the order the declared shape serialises in. The other
shipped row shapes keep the default, which builds one value tree first (§9 D13).

**A spool write flushes before it renames.** Handing bytes to a blocking task returns
before that task runs, so a rename issued straight after a write has no ordering against
it and a reader could open the renamed file and find it short — which is the one thing
the rename exists to prevent. `SpoolSink::finish` waits for the write, then the file's own
buffer, and only then is the file renamed into place.

---

## 8. Configuration

Five keys, each `PULSUS_` plus the field name upper-cased, rejected outside its range at
config load. Ranges and the reason for each bound live in
`crates/pulsus-config/src/validate.rs` beside the constants.

| key | carrier | range | default | what it does |
|---|---|---|---|---|
| `PULSUS_METRICS_LANDING_RETENTION_HOURS` | root | 1..=168 | 6 | the landing table's delete-TTL: the replay window of §1.3 |
| `PULSUS_METRICS_DEDUP_WINDOW` | root | 1..=1000000 | 10000 | blocks each of the five tables remembers (§1.3, §3) |
| `PULSUS_METRICS_LANDING_RETRIES` | `writer` | 0..=10 | 3 | resends of a failed landing insert |
| `PULSUS_METRICS_LANDING_INSERTERS` | `writer` | 1..=64 | 4 | insert workers on the landing queue |
| `PULSUS_METRICS_LANDING_MAX_ROWS` | `writer` | 1000..=10000000 | 1048576 | the per-push row ceiling, and the value both pinned row limits carry |

Two existing keys change meaning on this path: `PULSUS_BATCH_BYTES` becomes the per-push
byte ceiling, and `PULSUS_BATCH_MS` no longer triggers a metrics flush — there is no
flush loop left here, so the suppression index's ageing tick has a task of its own at
that cadence. `PULSUS_INGEST_QUEUE_BYTES` is unchanged and is now reserved against
landing rows. The landing budget is derived, not configured (§4).

The rotation tick reapplies the values this process loaded at startup — the schema
parameters are built from the process's own `Config` — so changing either root key takes
a restart.

---

## 9. What is disclosed rather than solved

Each of these was accepted by review rather than fixed. An implementer must not assume
any of them is covered.

| | precondition | effect |
|---|---|---|
| **D1** | a view throws while the server processes the insert | the insert fails after the block was sent, so it is §4's uncertain ending and takes that row's course rather than one of its own: at a `PULSUS_METRICS_LANDING_RETRIES` of 0, or with the budget spent, the first failure settles and spools; with resends left it is sent again, and if the view no longer throws that attempt commits, with no audit copy written at all. **Which of the targets kept the block's rows is not recorded anywhere** at any of those endings. `docs/schemas.md` §4.1, "What a failing view leaves behind", reports that outcome measured over 300 trials of exactly this shape — one throwing view and three healthy siblings over one source table: the throwing view's own target held nothing every time, the source rows were present in nearly every trial, and each healthy sibling committed in some of them. Nothing makes the fan-out a transaction and no machinery is built for this |
| **D2** | a clustered deployment | the landing table and the four targets render as `Replicated*` engines, where the window the server applies is `replicated_deduplication_window`. `METRIC_LANDING_STMTS` sets only the non-replicated name, so `PULSUS_METRICS_DEDUP_WINDOW` does not change the window in force there and the server's own default governs. The statements themselves apply without error — the clustered schema suite runs `run_init`, which runs them. `git grep -ln metric_landing` over the whole tree at the merged revision returns 23 files, and `crates/pulsus-schema/tests/live_cluster.rs` — the only suite that builds a clustered schema — is not among them, so no check covers the clustered window. **This one is a finding of this document's own and has not been through review** |
| **D3** | an attempt polled before the deadline and still in flight at it | abandoned, which is the uncertain ending. A send already on the wire cannot be recalled |
| **D4** | a deadline already expired when it was published, which needs a shutdown grace of zero | the constructor returns a future and the request goes out on its first poll, one store and one call after the lock is released; a publication landing in that gap means a request issued a few instructions after the deadline. With any positive grace the deadline is in the future at publication and the request is well inside it. Closing it would mean holding the deadline's lock across the insert's own polls, where a shutdown would wait on the network and the workers would stop overlapping |
| **D5** | an attempt that finished but was not polled before the deadline | reported abandoned rather than by its own outcome — an over-report of uncertainty, needing a task left unscheduled across the whole grace |
| **D6** | the budget elapsing inside an attempt | the budget encloses the connection checkout and the health ping as well as the send and cannot tell which of the three it interrupted, so it reports the fate as unknown for a block that may never have left the process |
| **D7** | a worker task cancelled or panicking mid-block | the charge stays counted, so the queue refuses pushes it could have taken and never exceeds its ceiling; **and the block's rows are dropped with no audit copy** while an async caller may already hold its acceptance. A sync caller gets a closed-wait failure. Review found no shipped path that aborts those handles, no request-controlled panic on the worker, insert, commit or audit path, and normal shutdown awaits every tracked task |
| **D8** | a sync caller waiting | its response channel is jointly owned from creation, so it crosses the release and no ordering here can cover its lifetime. It is a fixed term of the block's overhead and holds no rows. The term is an allowance: the channel's own bookkeeping is private to another crate, so a channel larger than the allowance is the one thing the charge would not catch |
| **D9** | a future exit path | the release census is lexical over one module's source. It would not see a release through an alias, a helper in another module, another spelling, an atomic update, or a drop that subtracts nothing at all, and it does not prove the matching call subtracts this block's charge |
| **D10** | a setting that ends a block without using any of the words the two searches looked for | not closed by those searches. The only closure against it is the server's own emit rule for format parsing, quoted in `landing_insert`'s comment |
| **D11** | a database user without the grant for the `MergeTree` settings catalogue | that catalogue's names go unchecked, with a warning, rather than refusing startup — refusing would stop every least-privilege deployment from starting (§2.2) |
| **D12** | a deployment that had set a byte limit, or enabled connection handling, deliberately | it does not get either on this insert (§2.1) |
| **D13** | the other shipped log, trace and per-target row shapes | they keep the collect-then-write encoder, which builds one value tree per row. Nothing is claimed about their peak, and it was not measured |
| **D14** | a new scalar type declared bounded wrongly | the piece refusal turns it into an error and the widest-value case into a failing test, rather than silent growth — but the compiler cannot catch it |
| **D15** | `pulsusdb rebuild-metrics` | deliberately without tests: there is no environment in this tree that would exercise it faithfully. It refuses a replay into the three append-only targets unless the caller also asks for their partitions to be dropped first, because a replay without that stores every row twice |
| **D16** | a suppressed push carrying descriptors | its descriptor-only insert carries no claim and no waiter, and it runs the same loop as every other block (§4): a failure before the block was sent stored nothing, one after it leaves whether the descriptors landed unknown, and a non-commit ending spools it like any other block. What the suppression leaves unchanged, once that block's own byte reservation is granted, is the caller's answer — the original push's outcome; a queue with no room for that reservation refuses the repeat `429` and nothing of its descriptors is built, queued or sent (§3). Nothing gates, caches or promotes a descriptor, so the next push carrying those descriptors emits them again |
| **D17** | a steady push rate above the rate §3 derives from `PULSUS_METRICS_DEDUP_WINDOW` | a token can be evicted before its resend arrives and the block is stored twice. Sizing the window is a deployment matter and nothing checks it |
| **D18** | the landing table's TTL | it floors `received_ms` to the second, so expiry can fall up to 999 ms before the exact instant and never after it; and a part's drop waits for a TTL merge, so this design states no instant at which landed rows stop occupying storage |
| **D19** | a landed event's identity | `event_id` lives only in the landing table and only while retention keeps it. No target holds it |

---

## 10. What differs per signal

Everything above §10 is signal-independent except the names. What a second signal
changes is the target list, the discriminating values, and its own landing table, row
type, config keys and window statements. Nothing is shared between two signals' landing
tables.

**What is reusable as it stands**, all inside `pulsus-write`: `writer::landing` — the
generic module the second signal extracted, holding `LandingBlock<R>`, its single
reservation-release point, `landing_charge::<R>` (§6) and the insert loop's two fates
(§4), each over the signal's own row type; `writer::drain`'s boundary (§5),
`writer::spool`'s sink and seal (§7), `writer::reserve_queued_bytes` (§6),
`QuerySettings::landing_insert` (§2.1) and `pulsus_schema::REQUIRED_SERVER_NAMES` (§2.2).
The settings constructor takes the row ceiling as an argument, so a second signal passes
its own.

**This document names no other signal's target tables, deliberately.** A signal's target
set — and which of those targets its own writer writes rather than a view — is decided in
that signal's own design, and a copy of the list here would state that decision twice and
date it. Logs: `docs/schemas.md` §3. Traces: `docs/TraceQL/sql-schema.md` §1 and
`docs/TraceQL/server-implementation.md` §2, whose accepted design replaces every trace
table now in `crates/pulsus-schema/src/catalog.rs`, so a trace target list read off the
catalogue today aims this work at tables that are going. The catalogue,
`WriterTables::logs_default` and `TraceWriterTables::traces_default` answer what is in the
tree now, which is a different question.

**The discriminating values are the implementer's to choose: one per landed event shape,
and one view per target — several views may read one value.** Metrics used `kind`
`UInt8`, values 0..3, one per target, declared as `MetricLandingRow::KIND_*` constants
and read by the views as `WHERE kind = k`. **Logs is why the rule is stated that way**
and not as one kind per target: it has three kinds and five views, because
`log_streams_idx_mv` is an `ARRAY JOIN` over the very same kind-1 row `log_streams_mv`
takes. A kind of its own for the index would make the writer land the same canonical
label blob twice per stream — the one blob is the widest column either target needs —
so the second view reads the first's kind instead. What a second signal keeps whichever
way it goes: one value per landed row shape, the discriminator first in the sorting key
(§1.1), a row type that is the union of its targets' columns less the server-filled
identity column, and a projection per view that names the target's columns in the
target's own order under the target's own names.

**A second-level view is that signal's own question, and there is a shape that does not
ask it.** Another signal may keep a target that a view maintains off another target
rather than off the landing table — four trace tables off two others. Neither the metrics
path nor the logs path has such a case any more — every one of their targets is one view
away from its landing table, and repointing `log_streams_idx_mv` and
`log_metrics_<res>_mv` is what removed the last two — and no insert in this tree sends a
block through two levels of view: every view in `MVS` reads a table the writer itself
inserts into. For `log_metrics_<res>` that is load-bearing rather than tidy: its columns
are `SimpleAggregateFunction(sum, UInt64)`, so a second-level view firing as well as a
first-level one would double every count. So **nothing here establishes what a
view does when its own source is written by a view**, and no sentence in this document
should be read as establishing it. What this document does establish is the other shape:
a kind and a view of its own per target, off the landing table. A signal that keeps a
second-level view instead rests on behaviour this design never exercised, and whether it
does is part of deciding that signal's target set, above.

Two things follow for the rest of this document, whichever way that goes. The row type is
the union of the kinds' columns, so a signal with more kinds holds a wider row per landed
event and §6's per-row term grows with it. And each target needs the window statement of
§1.3, whatever level of view writes it.

**Two things a second signal removes as well.** Metrics lost its registration backfill —
both backlogs, both tasks, their counters and their healed hooks — because what it
repaired no longer exists: a registration row rides the samples' own block, and the
registration cache is promoted only when that block commits, so a push whose insert
failed emits its registration rows again next time rather than leaving an orphan to
heal. Logs then did the same to the `log_streams` backlog (issue #134), for the same reason.
What `writer::backfill` and `WriterRuntime`'s remaining backfill field serve is the
`trace_attrs_idx` backlog (issue #139), and the same will hold for it when traces moves. And the per-table ingest counters collapse to one table
label, the landing table's, with the backfill series gone
(`crates/pulsus-server/src/ops.rs`).

---

## 11. What holds each guarantee up

Every entry is a test function name, findable with `git grep -n 'fn <name>'`. A file name
holds every name in the run directly before it — whether that file stands in brackets or
in the sentence, and both spellings occur below — back to whichever comes first of the
previous file, the previous semicolon, and the start of the cell. A name outside such a
run is in `crates/pulsus-write/tests/metric_writer.rs`. An entry marked live runs only
against a server.

| guarantee | what holds it up |
|---|---|
| one push is one insert into the landing table | `one_push_is_one_insert_into_the_landing_table`, `two_pushes_are_two_inserts` |
| every landing column carries the value its kind was built from | `every_landing_column_is_the_value_its_kind_was_built_from`; live: `a_push_lands_one_block_carrying_every_kind` (`crates/pulsus-write/tests/live_metric_writer.rs`) |
| the insert omits the identity column so the server fills it | `the_insert_omits_event_id_so_the_server_fills_it` — in `crates/pulsus-write/src/writer/rows.rs` for the column list, and live in `crates/pulsus-write/tests/live_metric_writer.rs` for what the server stores |
| every limit that forms a block, or disables deduplication, is pinned exactly | `the_landing_insert_pins_every_limit_that_forms_a_block`, `both_row_limits_follow_the_deployments_own_ceiling` (`crates/pulsus-clickhouse/src/settings.rs`) |
| the pinned set and the startup name list are the same set, both ways | `the_settings_read_back_at_startup_are_the_ones_the_landing_insert_sends` (`crates/pulsus-schema/src/controller.rs`) |
| the pinned settings reach the wire | `the_production_inserter_sends_the_landing_settings_on_the_wire`, `the_calls_settings_win_over_the_inserters_own` (`crates/pulsus-write/tests/landing_insert_settings.rs`) |
| a missing name refuses before any DDL | `a_missing_server_name_refuses_init_mode_before_any_ddl` (`crates/pulsus-server/src/schema_init.rs`); live: `required_names_are_read_from_the_server`, `a_catalogue_the_user_cannot_read_is_unchecked_not_a_refusal` (`crates/pulsus-schema/tests/live_schema.rs`) |
| the landing table, its views, its TTL and the five windows exist as configured | live: `metric_landing_and_its_views_exist_after_init`, `an_existing_landing_table_is_adopted_by_a_rerun`, `run_init_installs_the_landing_ttl_at_the_configured_hours`, `dedup_settings_reach_the_landing_table_and_all_four_targets` (`crates/pulsus-schema/tests/live_schema.rs`) |
| a push too large is refused whole, before any reservation | `a_push_at_a_ceiling_is_refused_whole`, `a_suppressed_copy_of_an_oversized_push_stores_nothing`, `the_push_too_large_message_names_the_size_and_both_limits`; `a_push_too_large_is_413_on_both_metric_transports` (`crates/pulsus-write/src/ingest/http.rs`) |
| an empty push is a success and is charged for nothing | `an_empty_push_is_a_success_at_the_smallest_accepted_byte_limits` |
| a token is minted per block and repeated byte-identically on resend | `a_token_is_minted_per_sealed_block_and_repeated_on_resend` |
| a class is preserved before the send and downgraded after it | `a_server_exception_after_the_block_was_sent_is_uncertain`, `a_retryable_failure_before_the_block_was_sent_keeps_its_own_class`, `a_client_deadline_during_the_metadata_read_is_a_pre_send_failure`, `a_client_deadline_after_the_insert_was_opened_is_uncertain` (`crates/pulsus-write/tests/landing_insert_settings.rs`) |
| the fate never walks back from uncertain, and each ending is the one named in §4 | `the_fate_never_walks_back_from_uncertain`, `the_insert_loop_endings` |
| the budget bounds the queue wait, every attempt and every sleep | `the_landing_budget_bounds_the_loop`, `a_retry_sleep_never_carries_a_block_past_the_budget`, `a_block_whose_budget_expired_while_queued_never_starts_an_insert`, `the_budget_expiring_inside_an_attempt_reports_an_unknown_fate` |
| no attempt is created or carried past the shutdown deadline | `an_attempt_is_never_created_once_the_deadline_has_passed`, `an_attempt_is_never_created_by_a_read_the_deadline_overtook` (`crates/pulsus-write/src/writer/drain.rs`), `a_retry_sleep_never_starts_an_attempt_after_the_drain_deadline` |
| shutdown accounts for every push it admitted, and files the two fates differently | `the_drain_accounts_for_every_push_it_admitted`, `shutdown_files_an_inflight_block_and_a_queued_block_differently`, `a_block_sent_after_the_queue_closed_is_settled_by_the_admitting_task` |
| the charge covers everything a queued block holds | `the_charge_covers_everything_a_queued_block_holds` (`crates/pulsus-write/src/writer/metric.rs`), `the_queue_charge_covers_the_landing_rows_it_holds`, `the_queue_charge_covers_the_escaped_labels_it_holds`, `the_queue_byte_allowance_is_aggregate_across_pushes` |
| the charge outlives the memory it prices, and one function releases it | `a_committing_block_stays_charged_until_its_rows_are_released`, `the_landing_reservation_is_released_in_one_place` (`crates/pulsus-write/src/writer/metric.rs`), `a_failed_blocks_reservation_is_held_until_its_spool_copy_is_written` |
| spooling a block holds no copy that grows with the push | `spooling_a_block_holds_no_copy_of_the_push` (`crates/pulsus-write/tests/spool_stream_alloc.rs`) |
| the streamed encoding is byte-for-byte the declared shape | `every_landing_row_kind_streams_the_shape_it_declares`, `a_streamed_string_escapes_exactly_as_serde_json_does`, `every_spool_scalar_fits_one_bounded_piece` (`crates/pulsus-write/src/writer/spool.rs`) |
| a failed spool write changes no outcome | `a_failed_spool_write_is_counted_and_changes_no_outcome` |
| registration stays success-only and emits one row per touched bucket | `same_bucket_second_sample_is_suppressed_by_the_series_lru`, `new_bucket_for_an_already_registered_series_emits_a_new_registration`, `transition_bucket_registers_both_float_and_histogram_series_rows`, `registered_series_row_carries_the_bucket_floored_timestamp_not_the_raw_sample`; live: `same_bucket_samples_land_exactly_one_registration_row` (`crates/pulsus-write/tests/live_metric_writer.rs`), `cross_request_float_and_histogram_register_both_value_type_rows` (`crates/pulsus-write/tests/live_metric_hist_writer.rs`) |
| every push emits its descriptors, and a tie serves one descriptor whole | `every_push_emits_its_descriptors`; live: `a_tie_on_updated_ns_serves_one_whole_descriptor`, `metadata_collapses_to_the_latest_write` (`crates/pulsus-read/tests/live_metrics_engine.rs`) |
| a retried client push still stores one copy | live: `a_retried_remote_write_stores_one_copy`, `a_concurrent_descriptor_race_leaves_one_visible_row` (`crates/pulsus-server/tests/push_dedup_live.rs`) |
| the configured dials are accepted at both ends of their ranges and rejected outside | `the_metrics_landing_dials_reject_both_sides_and_accept_both_ends` (`crates/pulsus-config/src/validate.rs`) |

**What no entry above covers**, stated here rather than left to be found: D2's clustered
window, D7's cancelled worker, and `pulsusdb rebuild-metrics` (D15). None of the three
has a test in this tree.
