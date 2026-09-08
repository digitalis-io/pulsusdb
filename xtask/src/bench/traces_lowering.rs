//! `bench traces-lowering` (issue #492 part 8) — the §9.2 re-measurement
//! of `docs/query-lowering.md`.
//!
//! **Why it exists.** §9.2 publishes a per-stage cost table for one
//! TraceQL request in two forms — as the engine drives it today, and as
//! a lowered request would — and the corpus those figures were taken on
//! was committed nowhere. Nobody could re-derive a single number in that
//! section from this repository. This scenario builds §9.1's corpus C1
//! from a deterministic generator, drives both forms through the
//! **product planner's own SQL**, and retains **one row per statement**
//! (never a total) in `docs/benchmarks/data/traces-lowering-92.json`, so
//! the document's figures can be checked against the rows they came from
//! by `crates/pulsus-read/tests/query_lowering_doc_gate.rs`.
//!
//! **The two forms come from one plan.** `plan_search` renders the
//! phase-1 generator twice: `generator_sqls[0]` carries the spanset
//! aggregate's `HAVING` (issue #492 part 4), and
//! `SearchPlan::generator_fallback_sql` is the byte-identical statement
//! without it (issue #492 part 5). The unlowered form is the second: its
//! generator returns every candidate the selector matches, so phase 2
//! walks candidates in `bound_ts DESC` order until `limit` of them
//! qualify. The lowered form is the first: the generator returns only
//! qualifying candidates, so phase 2 stops after one batch. Nothing else
//! differs — the hydration, membership and root statements are the same
//! builders in both forms.
//!
//! **Scenario-local evidence structs only.** [`super::query_log::
//! QueryLogTotals`] derives `Row` and is a frozen artifact; it is not
//! touched here.
//!
//! ```text
//! podman run -d --name pulsus-lowering-ch -p 18923:8123 \
//!     docker.io/clickhouse/clickhouse-server:26.3
//! cargo run -p xtask -- bench traces-lowering \
//!     --http-url http://127.0.0.1:18923 --database pulsus_lowering_bench \
//!     --out docs/benchmarks/data/traces-lowering-92.json
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::traces::exec::{TRACE_MAX_RESULT_BYTES, TRACE_READ_BYTES_BUDGET};
use pulsus_read::traces::rows::{CandidateRow, HydrationRow, MembershipRow, RootRow};
use pulsus_read::traces::search_plan::{SearchCtx, SearchParams, plan_search};
use pulsus_read::{SearchPlan, SpanFilterCtx, TRACE_SEARCH_MAX_BLOCK_ROWS};
use pulsus_schema::{RenderCtx, run_init};

use super::query_log::{flush_logs, tagged_settings};
use super::{BenchArgs, parse_http_url};

// ---------------------------------------------------------------------
// Corpus C1, exactly as `docs/query-lowering.md` §9.1 describes it.
//
// Every constant below is a number that section states. The generator is
// deterministic — `numbers_mt()` over an index, no PRNG — so the corpus
// is a function of these constants and nothing else, and a second run on
// a second machine builds the same rows.
// ---------------------------------------------------------------------

/// 1,000,000 traces of 10 spans each — §9.1's 10,000,000 spans.
const TRACES: u64 = 1_000_000;
const SPANS_PER_TRACE: u64 = 10;
const SPANS: u64 = TRACES * SPANS_PER_TRACE;
/// §9.1's five attribute rows per span — 50,000,000 `trace_attrs_idx`
/// rows.
const ATTRS_PER_SPAN: u64 = 5;
const ATTR_ROWS: u64 = SPANS * ATTRS_PER_SPAN;
/// §9.1's 50 services, 45 of them `service.namespace = "prod"`, so
/// `TRACES * 45 / 50` = 900,000 traces match the selector.
const SERVICES: u64 = 50;
const PROD_SERVICES: u64 = 45;
/// §9.1's 2,500 span names, the first six of them the awkward ones.
const SPAN_NAMES: u64 = 2_500;
/// §9.1's five whole days.
const DAYS: i64 = 5;
const DAY_NS: i64 = 24 * 3_600 * 1_000_000_000;
const WINDOW_NS: i64 = DAYS * DAY_NS;
/// The gap between consecutive spans: the window divided by the span
/// count, so the corpus fills exactly five whole day partitions.
const SPAN_SPACING_NS: i64 = WINDOW_NS / SPANS as i64;
/// §9.1's "exactly 1,000 traces carry one 2.001 s span, 200 on each of
/// the five days": one trace in every `LONG_TRACE_EVERY`, and
/// `TRACES / DAYS` traces land in each day partition.
const LONG_TRACE_EVERY: u64 = TRACES / 1_000;
const LONG_SPAN_NS: i64 = 2_001_000_000;
/// The ordinary span duration band, §9.1's 1.000001–2.0 ms.
const SHORT_SPAN_MIN_NS: i64 = 1_000_001;
const SHORT_SPAN_MAX_NS: i64 = 2_000_000;

/// The §9.2 request, verbatim.
const QUERY: &str = r#"{ .service.namespace = "prod" } | max(duration) > 1s"#;
const LIMIT: u32 = 20;
const SPSS: u32 = 3;
/// `reader.traceql_max_candidates` / `reader.traceql_max_series` at their
/// production defaults, so the planned statements are the shipped ones.
const MAX_CANDIDATES: u64 = 100_000;
const MAX_SERIES: u64 = 1_000;
/// [`pulsus_read::traces::exec::BATCH_TRACES`] is `pub`, but the constant
/// is re-read here through the public path so the batch width the harness
/// walks is the one the engine walks.
const BATCH_TRACES: usize = pulsus_read::BATCH_TRACES;

/// The two `system.query_log` byte counters §9.2's `off file system †`
/// column could be. **Both are recorded and both are asserted present**:
/// a `Map` lookup for an absent key returns `0` and says nothing, so a
/// counter this server does not emit would otherwise be written as a
/// zero and totalled as one. §9.2 names which of the two its column is.
const COUNTER_COMPRESSED: &str = "ReadCompressedBytes";
const COUNTER_FD: &str = "ReadBufferFromFileDescriptorReadBytes";
const COUNTER_MARKS: &str = "SelectedMarks";

// ---------------------------------------------------------------------
// The retained artefact.
// ---------------------------------------------------------------------

/// Which request a statement belongs to.
///
/// Adding a form is a build decision: every match over this enum in the
/// gate has no `_` arm. `stage` names the KIND of read and `form` names
/// the request, because one string carrying both facts cannot express
/// the lowered membership read — it would have to be counted under the
/// unlowered form's stage name or dropped from the artefact.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Form {
    Current,
    Lowered,
}

/// Which KIND of read a statement is. Four kinds, in both forms.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Stage {
    Generator,
    Hydration,
    Membership,
    RootRead,
}

/// One statement. **One row per `query_id`, never a summary** — §9.2's
/// existing total is unreproducible precisely because only totals were
/// kept.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub(crate) struct LoweringStageRow {
    pub(crate) form: Form,
    pub(crate) stage: Stage,
    pub(crate) query_id: String,
    /// 0-based within `(form, stage)`; contiguous, asserted per pair.
    pub(crate) seq: u32,
    pub(crate) read_rows: u64,
    /// `query_log.read_bytes` — uncompressed bytes read from tables,
    /// §9.2's `decoded †` column.
    pub(crate) read_bytes: u64,
    /// `ProfileEvents['ReadCompressedBytes']`, written only when
    /// `has(...)` is 1; a 0 aborts the run.
    pub(crate) read_compressed_bytes: u64,
    /// `ProfileEvents['ReadBufferFromFileDescriptorReadBytes']`, same
    /// presence rule. §9.2 states which of the two its
    /// `off file system †` column is.
    pub(crate) fd_read_bytes: u64,
    pub(crate) result_bytes: u64,
    pub(crate) selected_marks: u64,
    pub(crate) memory_usage: u64,
    /// What the statement was SENT with
    /// ([`TRACE_SEARCH_MAX_BLOCK_ROWS`]).
    pub(crate) max_block_size_submitted: u64,
    /// `Settings['max_block_size']` — what the SERVER recorded. Empty
    /// when the submitted value was the server default, because the log
    /// records only settings that differ from it.
    pub(crate) settings_max_block_size_logged: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub(crate) struct LoweringEvidence {
    pub(crate) clickhouse_version: String,
    pub(crate) corpus: String,
    pub(crate) query: String,
    pub(crate) limit: u32,
    pub(crate) rows: Vec<LoweringStageRow>,
}

// ---------------------------------------------------------------------
// Corpus generation.
// ---------------------------------------------------------------------

/// The span-name expression: `SPAN_NAMES` distinct names, the first six
/// of them §9.1's awkward ones (the empty string, a single character, a
/// slashed name, a braced name, a non-ASCII name and a shouted one).
fn name_expr(span_index: &str) -> String {
    format!(
        "multiIf(\
           {span_index} % {SPAN_NAMES} = 0, '', \
           {span_index} % {SPAN_NAMES} = 1, 'a', \
           {span_index} % {SPAN_NAMES} = 2, 'a/b/c', \
           {span_index} % {SPAN_NAMES} = 3, '{{braced}}', \
           {span_index} % {SPAN_NAMES} = 4, 'café-op', \
           {span_index} % {SPAN_NAMES} = 5, 'GETTING_STARTED', \
           concat('op-', leftPad(toString({span_index} % {SPAN_NAMES}), 4, '0')))"
    )
}

/// The duration expression: §9.1's one 2.001 s span in one trace of every
/// [`LONG_TRACE_EVERY`], every other span spread evenly over
/// 1.000001–2.0 ms.
fn duration_expr(span_index: &str) -> String {
    let trace = format!("intDiv({span_index}, {SPANS_PER_TRACE})");
    let span_in_trace = format!("{span_index} % {SPANS_PER_TRACE}");
    let band = SHORT_SPAN_MAX_NS - SHORT_SPAN_MIN_NS;
    format!(
        "toInt64(if({trace} % {LONG_TRACE_EVERY} = 0 AND {span_in_trace} = 5, \
           {LONG_SPAN_NS}, \
           {SHORT_SPAN_MIN_NS} + intDiv({band} * ({span_index} % 1000), 999)))"
    )
}

/// The timestamp expression.
///
/// `+ 1` nanosecond so the first span lands strictly inside the request
/// window, whose lower bound the generator applies as
/// `timestamp_ns > start_ns`; and the offset is `index * spacing` rather
/// than `(index + 1) * spacing` so the LAST span lands strictly inside
/// the fifth day rather than exactly on the midnight that opens the
/// sixth. One span in a sixth partition would make the corpus 6 parts
/// where §9.1 says 5.
fn timestamp_expr(base_ns: i64, span_index: &str) -> String {
    format!("toInt64({base_ns} + toInt64({span_index}) * {SPAN_SPACING_NS} + 1)")
}

/// `service.namespace` — `'prod'` for the first [`PROD_SERVICES`] of
/// [`SERVICES`], so exactly `TRACES * PROD_SERVICES / SERVICES` traces
/// match the selector.
fn namespace_expr(trace_index: &str) -> String {
    format!("if({trace_index} % {SERVICES} < {PROD_SERVICES}, 'prod', 'staging')")
}

async fn exec(client: &ChClient, sql: &str) -> anyhow::Result<()> {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await?;
    Ok(())
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

async fn count_of(client: &ChClient, sql: &str) -> anyhow::Result<u64> {
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await?;
    match stream.next().await {
        Some(row) => Ok(row?.n),
        None => Ok(0),
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct StringRow {
    s: String,
}

async fn string_of(client: &ChClient, sql: &str) -> anyhow::Result<String> {
    let mut stream = client
        .query_stream::<StringRow>(sql, &QuerySettings::new())
        .await?;
    match stream.next().await {
        Some(row) => Ok(row?.s),
        None => anyhow::bail!("no row for {sql}"),
    }
}

/// Builds corpus C1. Inserted in day-sized slices so a single statement
/// never has to hold ten million rows' worth of sort buffers, and so
/// progress is visible on a run that takes minutes.
async fn build_corpus(client: &ChClient, db: &str, base_ns: i64) -> anyhow::Result<()> {
    let spans_per_day = SPANS / DAYS as u64;
    for day in 0..DAYS as u64 {
        let lo = day * spans_per_day;
        let hi = lo + spans_per_day;
        let n = "number";
        let trace = format!("intDiv({n}, {SPANS_PER_TRACE})");
        eprintln!("  trace_spans day {}/{DAYS}: rows [{lo}, {hi})", day + 1);
        exec(
            client,
            &format!(
                "INSERT INTO {db}.trace_spans \
                 (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
                  status_code, kind, payload_type, payload) \
                 SELECT \
                   sipHash128(toUInt64({trace})), \
                   toFixedString(unhex(leftPad(lower(hex({n})), 16, '0')), 8), \
                   toFixedString(unhex('0000000000000000'), 8), \
                   {name}, \
                   concat('svc-', leftPad(toString({trace} % {SERVICES}), 2, '0')), \
                   {ts}, {dur}, 0, 1, 0, '' \
                 FROM numbers_mt({lo}, {span_count})",
                name = name_expr(n),
                ts = timestamp_expr(base_ns, n),
                dur = duration_expr(n),
                span_count = hi - lo,
            ),
        )
        .await?;
    }

    let attrs_per_day = ATTR_ROWS / DAYS as u64;
    for day in 0..DAYS as u64 {
        let lo = day * attrs_per_day;
        let hi = lo + attrs_per_day;
        // `number` indexes an ATTRIBUTE row here; `s` is the span it
        // belongs to and `t` the trace, so each attribute row carries the
        // span's own timestamp and duration.
        let s = format!("intDiv(number, {ATTRS_PER_SPAN})");
        let t = format!("intDiv(intDiv(number, {ATTRS_PER_SPAN}), {SPANS_PER_TRACE})");
        let k = format!("number % {ATTRS_PER_SPAN}");
        let ts = timestamp_expr(base_ns, &s);
        eprintln!(
            "  trace_attrs_idx day {}/{DAYS}: rows [{lo}, {hi})",
            day + 1
        );
        exec(
            client,
            &format!(
                "INSERT INTO {db}.trace_attrs_idx \
                 (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
                 SELECT \
                   toDate(fromUnixTimestamp64Nano({ts})), \
                   multiIf({k} = 0, 'service.namespace', {k} = 1, 'service.name', \
                           {k} = 2, 'http.status_code', {k} = 3, 'http.method', \
                           'deployment.environment'), \
                   multiIf({k} = 0, {ns}, \
                           {k} = 1, concat('svc-', leftPad(toString({t} % {SERVICES}), 2, '0')), \
                           {k} = 2, if({s} % 20 = 0, '500', '200'), \
                           {k} = 3, if({s} % 4 = 0, 'POST', 'GET'), \
                           {ns}), \
                   multiIf({k} = 0, 'resource', {k} = 1, 'resource', 'span'), \
                   if({k} = 2, toNullable(if({s} % 20 = 0, 500., 200.)), NULL), \
                   {ts}, \
                   sipHash128(toUInt64({t})), \
                   toFixedString(unhex(leftPad(lower(hex({s})), 16, '0')), 8), \
                   {dur} \
                 FROM numbers_mt({lo}, {attr_count})",
                ns = namespace_expr(&t),
                dur = duration_expr(&s),
                attr_count = hi - lo,
            ),
        )
        .await?;
    }

    for table in ["trace_spans", "trace_attrs_idx"] {
        eprintln!("  OPTIMIZE {db}.{table} FINAL");
        exec(client, &format!("OPTIMIZE TABLE {db}.{table} FINAL")).await?;
    }

    let spans = count_of(
        client,
        &format!("SELECT count() AS n FROM {db}.trace_spans"),
    )
    .await?;
    let attrs = count_of(
        client,
        &format!("SELECT count() AS n FROM {db}.trace_attrs_idx"),
    )
    .await?;
    anyhow::ensure!(
        spans == SPANS && attrs == ATTR_ROWS,
        "corpus C1 is the wrong size: {spans} spans (want {SPANS}), {attrs} attribute rows \
         (want {ATTR_ROWS})"
    );
    Ok(())
}

/// The part/granule shape §9.1 publishes, printed after the merge so the
/// corpus can be compared with the section that describes it.
async fn print_corpus_shape(
    client: &ChClient,
    db: &str,
) -> anyhow::Result<Vec<(String, u64, u64)>> {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct ShapeRow {
        table: String,
        parts: u64,
        granules: u64,
    }
    let sql = format!(
        "SELECT table, count() AS parts, sum(marks) AS granules FROM system.parts \
         WHERE database = '{db}' AND active AND table IN ('trace_spans', 'trace_attrs_idx') \
         GROUP BY table ORDER BY table"
    );
    let mut stream = client
        .query_stream::<ShapeRow>(&sql, &QuerySettings::new())
        .await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row?;
        eprintln!(
            "  {}: {} parts / {} granules",
            row.table, row.parts, row.granules
        );
        out.push((row.table, row.parts, row.granules));
    }
    Ok(out)
}

// ---------------------------------------------------------------------
// Driving the two forms.
// ---------------------------------------------------------------------

/// The settings every statement carries, mirroring
/// `traces/exec.rs::search_settings` (`:2826`) — the shipped read
/// budgets and, the one that decides a figure,
/// [`TRACE_SEARCH_MAX_BLOCK_ROWS`] as `max_block_size`.
/// `use_query_condition_cache = 0` is added on top: the cache is on by
/// default in ClickHouse 26.3 and would make the second read of a
/// predicate cheaper than the first, which is a property of the run
/// order rather than of the design.
fn statement_settings(query_id: &str, generator_memory: Option<u64>) -> QuerySettings {
    let settings = QuerySettings::new()
        .set("max_rows_to_read", 50_000_000u64)
        .set("max_bytes_to_read", TRACE_READ_BYTES_BUDGET)
        .set("read_overflow_mode", "throw")
        .set("max_result_bytes", TRACE_MAX_RESULT_BYTES)
        .set("result_overflow_mode", "throw")
        .set("max_block_size", TRACE_SEARCH_MAX_BLOCK_ROWS)
        .set("max_bytes_before_external_group_by", 0u64)
        .set("use_query_condition_cache", 0u64)
        .set(
            "max_memory_usage",
            generator_memory.unwrap_or(8 * 1024 * 1024 * 1024),
        );
    tagged_settings(settings, query_id, None)
}

/// Drains one statement to completion, returning its decoded rows.
async fn collect<R: pulsus_clickhouse::ChRow>(
    client: &ChClient,
    sql: &str,
    settings: &QuerySettings,
) -> anyhow::Result<Vec<R>> {
    let sql = sql.replace('?', "??");
    let mut stream = client.query_stream::<R>(&sql, settings).await?;
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row?);
    }
    Ok(rows)
}

/// One statement's identity in the artefact, before its `system.query_log`
/// row is read back.
struct Issued {
    form: Form,
    stage: Stage,
    seq: u32,
    query_id: String,
}

/// Drives one form of the request and returns the statements it issued,
/// in issue order.
///
/// The walk is the engine's: phase 1 returns candidates in `bound_ts DESC`
/// order; phase 2 consumes them in batches of [`BATCH_TRACES`], hydrating
/// and taking the selector's membership set for each, and stops once
/// `limit` traces have qualified; the winners' root read is last. The two
/// forms differ in exactly one statement — which generator SQL phase 1
/// sends — and everything downstream follows from how many candidates
/// that statement returns.
async fn drive(
    client: &ChClient,
    plan: &SearchPlan,
    form: Form,
    nonce: u128,
) -> anyhow::Result<Vec<Issued>> {
    let generator_sql = match form {
        Form::Lowered => plan.generator_sqls[0].clone(),
        Form::Current => plan
            .generator_fallback_sql()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "the plan pushed no HAVING, so there is no unlowered generator to drive: \
                     the two forms would be the same statement"
                )
            })?
            .to_string(),
    };
    let form_tag = match form {
        Form::Current => "current",
        Form::Lowered => "lowered",
    };
    let mut issued = Vec::new();
    let next_id = |stage: Stage, seq: u32| -> String {
        let stage_tag = match stage {
            Stage::Generator => "generator",
            Stage::Hydration => "hydration",
            Stage::Membership => "membership",
            Stage::RootRead => "root_read",
        };
        format!("pulsus-lowering-{nonce}-{form_tag}-{stage_tag}-{seq}")
    };

    let query_id = next_id(Stage::Generator, 0);
    let candidates: Vec<CandidateRow> = collect(
        client,
        &generator_sql,
        &statement_settings(&query_id, Some(536_870_912)),
    )
    .await?;
    issued.push(Issued {
        form,
        stage: Stage::Generator,
        seq: 0,
        query_id,
    });
    anyhow::ensure!(
        !candidates.is_empty(),
        "the {form_tag} generator returned no candidates — the corpus does not match the query"
    );
    eprintln!("  {form_tag} generator: {} candidates", candidates.len());

    let mut winners: Vec<[u8; 16]> = Vec::new();
    let mut batch_seq = 0u32;
    for batch in candidates.chunks(BATCH_TRACES) {
        if winners.len() >= LIMIT as usize {
            break;
        }
        let ids: Vec<[u8; 16]> = batch.iter().map(|c| c.trace_id).collect();

        let query_id = next_id(Stage::Hydration, batch_seq);
        let spans: Vec<HydrationRow> = collect(
            client,
            &plan.hydration_sql_for(&ids),
            &statement_settings(&query_id, None),
        )
        .await?;
        issued.push(Issued {
            form,
            stage: Stage::Hydration,
            seq: batch_seq,
            query_id,
        });

        let query_id = next_id(Stage::Membership, batch_seq);
        let members: Vec<MembershipRow> = collect(
            client,
            &plan.membership_sql_for(0, &ids),
            &statement_settings(&query_id, None),
        )
        .await?;
        issued.push(Issued {
            form,
            stage: Stage::Membership,
            seq: batch_seq,
            query_id,
        });

        // `max(duration) > 1s` over the selector's MATCHED spans — the
        // same scope the evaluator uses (§9.3), which is why the
        // membership set is read at all.
        let matched: BTreeSet<([u8; 16], [u8; 8])> =
            members.iter().map(|m| (m.trace_id, m.span_id)).collect();
        let mut max_by_trace: BTreeMap<[u8; 16], i64> = BTreeMap::new();
        for span in &spans {
            if matched.contains(&(span.trace_id, span.span_id)) {
                let slot = max_by_trace.entry(span.trace_id).or_insert(i64::MIN);
                *slot = (*slot).max(span.duration_ns);
            }
        }
        for candidate in batch {
            if winners.len() >= LIMIT as usize {
                break;
            }
            if max_by_trace
                .get(&candidate.trace_id)
                .is_some_and(|d| *d > 1_000_000_000)
            {
                winners.push(candidate.trace_id);
            }
        }
        batch_seq += 1;
    }
    anyhow::ensure!(
        winners.len() == LIMIT as usize,
        "the {form_tag} form found {} qualifying traces, not the requested {LIMIT}: the corpus \
         does not carry a full page of answers",
        winners.len()
    );
    eprintln!(
        "  {form_tag}: {batch_seq} phase-2 batches, {} winners",
        winners.len()
    );

    let query_id = next_id(Stage::RootRead, 0);
    let roots: Vec<RootRow> = collect(
        client,
        &plan.root_sql_for(&winners),
        &statement_settings(&query_id, None),
    )
    .await?;
    anyhow::ensure!(
        !roots.is_empty(),
        "the {form_tag} winners' root read returned nothing"
    );
    issued.push(Issued {
        form,
        stage: Stage::RootRead,
        seq: 0,
        query_id,
    });
    Ok(issued)
}

// ---------------------------------------------------------------------
// Reading the evidence back.
// ---------------------------------------------------------------------

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct QueryLogRow {
    query_id: String,
    read_rows: u64,
    read_bytes: u64,
    read_compressed_bytes: u64,
    fd_read_bytes: u64,
    has_compressed: u8,
    has_fd: u8,
    has_marks: u8,
    result_bytes: u64,
    selected_marks: u64,
    memory_usage: u64,
    settings_max_block_size_logged: String,
}

/// Reads back every statement's `QueryFinish` row.
///
/// `SYSTEM FLUSH LOGS` is not a barrier — a row can still be in flight
/// when it returns — so this polls until every issued `query_id` has a
/// row, and a `query_id` still missing at the deadline is a **hard
/// error**. A missing row read as a zero would be a figure this
/// measurement cannot tell from a real one.
async fn read_evidence(
    client: &ChClient,
    issued: &[Issued],
    nonce: u128,
) -> anyhow::Result<Vec<LoweringStageRow>> {
    let sql = format!(
        "SELECT query_id, read_rows, read_bytes, \
           ProfileEvents['{COUNTER_COMPRESSED}'] AS read_compressed_bytes, \
           ProfileEvents['{COUNTER_FD}'] AS fd_read_bytes, \
           toUInt8(has(ProfileEvents, '{COUNTER_COMPRESSED}')) AS has_compressed, \
           toUInt8(has(ProfileEvents, '{COUNTER_FD}')) AS has_fd, \
           toUInt8(has(ProfileEvents, '{COUNTER_MARKS}')) AS has_marks, \
           result_bytes, ProfileEvents['{COUNTER_MARKS}'] AS selected_marks, memory_usage, \
           Settings['max_block_size'] AS settings_max_block_size_logged \
         FROM system.query_log \
         WHERE type = 'QueryFinish' AND query_id LIKE 'pulsus-lowering-{nonce}-%'"
    );
    let deadline = Instant::now() + Duration::from_secs(300);
    let mut by_id: BTreeMap<String, QueryLogRow> = BTreeMap::new();
    loop {
        flush_logs(client).await?;
        by_id.clear();
        let mut stream = client
            .query_stream::<QueryLogRow>(&sql, &QuerySettings::new())
            .await?;
        while let Some(row) = stream.next().await {
            let row = row?;
            by_id.insert(row.query_id.clone(), row);
        }
        if issued.iter().all(|i| by_id.contains_key(&i.query_id)) {
            break;
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "{} of {} statements never produced a QueryFinish row in system.query_log; a missing \
             row is a hard error, never a zero",
            issued
                .iter()
                .filter(|i| !by_id.contains_key(&i.query_id))
                .count(),
            issued.len()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let mut rows = Vec::with_capacity(issued.len());
    for item in issued {
        let log = by_id
            .get(&item.query_id)
            .expect("every query_id has a row by now");
        anyhow::ensure!(
            log.has_compressed == 1,
            "{}: ProfileEvents carries no {COUNTER_COMPRESSED}; a Map miss returns 0 and would be \
             recorded as a measured zero",
            item.query_id
        );
        anyhow::ensure!(
            log.has_fd == 1,
            "{}: ProfileEvents carries no {COUNTER_FD}",
            item.query_id
        );
        anyhow::ensure!(
            log.has_marks == 1,
            "{}: ProfileEvents carries no {COUNTER_MARKS}",
            item.query_id
        );
        rows.push(LoweringStageRow {
            form: item.form,
            stage: item.stage,
            query_id: item.query_id.clone(),
            seq: item.seq,
            read_rows: log.read_rows,
            read_bytes: log.read_bytes,
            read_compressed_bytes: log.read_compressed_bytes,
            fd_read_bytes: log.fd_read_bytes,
            result_bytes: log.result_bytes,
            selected_marks: log.selected_marks,
            memory_usage: log.memory_usage,
            max_block_size_submitted: TRACE_SEARCH_MAX_BLOCK_ROWS,
            settings_max_block_size_logged: log.settings_max_block_size_logged.clone(),
        });
    }
    Ok(rows)
}

// ---------------------------------------------------------------------
// The scenario.
// ---------------------------------------------------------------------

/// `pulsus_traceql::parse` with anyhow context.
fn parse_query(q: &str) -> anyhow::Result<pulsus_traceql::Query> {
    pulsus_traceql::parse(q).map_err(|e| anyhow::anyhow!("parse {q:?}: {e}"))
}

/// The plan both forms read their SQL from — the product planner's, not a
/// statement written beside it. Pure, so the unit test below builds the
/// same plan the run does.
fn lowering_plan(start_ns: i64, end_ns: i64) -> anyhow::Result<SearchPlan> {
    let ctx = SearchCtx {
        filter: SpanFilterCtx {
            spans_table: "trace_spans",
            attrs_table: "trace_attrs_idx",
        },
        max_candidates: MAX_CANDIDATES,
        max_series: MAX_SERIES,
        distributed: false,
    };
    let params = SearchParams {
        start_ns,
        end_ns,
        limit: LIMIT,
        spss: SPSS,
    };
    let plan = plan_search(&parse_query(QUERY)?, &params, &ctx)
        .map_err(|e| anyhow::anyhow!("plan_search failed: {e}"))?;
    anyhow::ensure!(
        plan.generator_sqls.len() == 1 && plan.probes_len() == 1,
        "the §9.2 request must plan one generator and one membership probe"
    );
    anyhow::ensure!(
        plan.aggregate_pushed(),
        "the §9.2 request's aggregate no longer compiles into the generator, so this scenario \
         has no lowered form to measure"
    );
    Ok(plan)
}

/// Midnight UTC, `DAYS` days before the midnight that opens today — so
/// the corpus fills exactly five whole day partitions and the run is
/// never within a partition the table's TTL is about to drop.
fn corpus_base_ns() -> i64 {
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64");
    let midnight = (now / DAY_NS) * DAY_NS;
    midnight - WINDOW_NS
}

pub async fn run(args: BenchArgs) -> anyhow::Result<()> {
    let (server, http_port) = parse_http_url(&args.http_url)?;
    let admin_cfg = ChConnConfig {
        server,
        http_port,
        database: "default".to_string(),
        user: args.user.clone(),
        password: args.password.clone(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(3600),
        ..ChConnConfig::default()
    };
    let admin = ChClient::new(admin_cfg.clone()).await?;
    let version = string_of(&admin, "SELECT version() AS s").await?;
    eprintln!("=== ClickHouse {version} ===");

    let base_ns = corpus_base_ns();
    let end_ns = base_ns + WINDOW_NS;

    let existing = count_of(
        &admin,
        &format!(
            "SELECT count() AS n FROM system.tables WHERE database = '{}' \
             AND name = 'trace_attrs_idx'",
            args.database
        ),
    )
    .await?;
    let mut data_cfg = admin_cfg.clone();
    data_cfg.database = args.database.clone();

    if existing == 0 {
        eprintln!("=== initializing schema (db={}) ===", args.database);
        exec(
            &admin,
            &format!("DROP DATABASE IF EXISTS {} SYNC", args.database),
        )
        .await?;
        run_init(
            &admin,
            &RenderCtx {
                db: args.database.clone(),
                cluster: None,
                dist_suffix: "_dist".to_string(),
                storage_policy: None,
                retention_days: 7,
                log_rollup: Duration::from_secs(5),
            },
        )
        .await?;
        let client = ChClient::new(data_cfg.clone()).await?;
        eprintln!("=== building corpus C1 ({SPANS} spans / {ATTR_ROWS} attribute rows) ===");
        let started = Instant::now();
        build_corpus(&client, &args.database, base_ns).await?;
        eprintln!(
            "=== corpus built in {:.0}s ===",
            started.elapsed().as_secs_f64()
        );
    } else {
        eprintln!(
            "=== reusing the corpus already in {} (drop the database to rebuild) ===",
            args.database
        );
    }
    let client = ChClient::new(data_cfg).await?;
    print_corpus_shape(&client, &args.database).await?;

    let plan = lowering_plan(base_ns, end_ns)?;
    eprintln!("=== pushed HAVING: {:?} ===", plan.pushed_having());

    // `system.query_log` outlives databases, so a deterministic query_id
    // would aggregate rows across runs.
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();

    let mut issued = Vec::new();
    for form in [Form::Current, Form::Lowered] {
        let started = Instant::now();
        issued.extend(drive(&client, &plan, form, nonce).await?);
        eprintln!("  form finished in {:.0}s", started.elapsed().as_secs_f64());
    }
    eprintln!(
        "=== {} statements issued; reading system.query_log ===",
        issued.len()
    );

    let rows = read_evidence(&client, &issued, nonce).await?;
    let evidence = LoweringEvidence {
        clickhouse_version: version,
        corpus: "C1".to_string(),
        query: QUERY.to_string(),
        limit: LIMIT,
        rows,
    };
    summarise(&evidence);

    match &args.out {
        Some(out) => {
            std::fs::write(
                out,
                format!("{}\n", serde_json::to_string_pretty(&evidence)?),
            )?;
            eprintln!("wrote {out}");
        }
        None => eprintln!("no --out given; nothing written"),
    }
    Ok(())
}

/// Prints the per-`(form, stage)` totals the document's tables carry, so
/// a run can be read without opening the artefact.
fn summarise(evidence: &LoweringEvidence) {
    println!(
        "{:<8} {:<11} {:>6} {:>16} {:>15} {:>15} {:>10} {:>13}",
        "form", "stage", "reads", "read_rows", "read_bytes", "fd_bytes", "granules", "result_bytes"
    );
    for form in [Form::Current, Form::Lowered] {
        for stage in [
            Stage::Generator,
            Stage::Hydration,
            Stage::Membership,
            Stage::RootRead,
        ] {
            let rows: Vec<&LoweringStageRow> = evidence
                .rows
                .iter()
                .filter(|r| r.form == form && r.stage == stage)
                .collect();
            if rows.is_empty() {
                continue;
            }
            println!(
                "{:<8} {:<11} {:>6} {:>16} {:>15} {:>15} {:>10} {:>13}",
                format!("{form:?}").to_lowercase(),
                format!("{stage:?}").to_lowercase(),
                rows.len(),
                rows.iter().map(|r| r.read_rows).sum::<u64>(),
                rows.iter().map(|r| r.read_bytes).sum::<u64>(),
                rows.iter().map(|r| r.fd_read_bytes).sum::<u64>(),
                rows.iter().map(|r| r.selected_marks).sum::<u64>(),
                rows.iter().map(|r| r.result_bytes).sum::<u64>(),
            );
        }
    }
    println!(
        "peak memory_usage over all rows: {}",
        evidence
            .rows
            .iter()
            .map(|r| r.memory_usage)
            .max()
            .unwrap_or(0)
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The corpus constants are §9.1's, and they compose to §9.1's
    /// numbers rather than to numbers that happen to be near them.
    #[test]
    fn the_corpus_constants_are_the_ones_section_9_1_publishes() {
        assert_eq!(SPANS, 10_000_000);
        assert_eq!(ATTR_ROWS, 50_000_000);
        assert_eq!(TRACES * PROD_SERVICES / SERVICES, 900_000);
        assert_eq!(TRACES / LONG_TRACE_EVERY, 1_000);
        assert_eq!(TRACES / LONG_TRACE_EVERY / DAYS as u64, 200);
        // Five whole day partitions: the spacing divides the window
        // exactly, and a day holds a whole number of traces.
        assert_eq!(SPAN_SPACING_NS * SPANS as i64, WINDOW_NS);
        assert_eq!(SPANS % DAYS as u64, 0);
        assert_eq!((SPANS / DAYS as u64) % SPANS_PER_TRACE, 0);
    }

    /// The two forms are two different statements from ONE plan — the
    /// pushed generator and its no-`HAVING` fallback — and the three
    /// downstream builders are shared. Pure: no server.
    #[test]
    fn the_two_forms_differ_in_exactly_the_generator_statement() {
        let base = 1_700_000_000_000_000_000i64;
        let plan = lowering_plan(base, base + WINDOW_NS).expect("the §9.2 request plans");
        let pushed = &plan.generator_sqls[0];
        let fallback = plan
            .generator_fallback_sql()
            .expect("the aggregate pushed, so a fallback exists");
        assert_ne!(pushed, fallback, "the two forms must send different SQL");
        assert!(
            pushed.contains("HAVING max(duration_ns) > 1000000000"),
            "the lowered generator carries the aggregate's HAVING:\n{pushed}"
        );
        assert!(
            !fallback.contains("HAVING"),
            "the unlowered generator carries no HAVING:\n{fallback}"
        );
        assert_eq!(
            pushed.replace("HAVING max(duration_ns) > 1000000000\n", ""),
            *fallback,
            "the two generators must differ ONLY by the HAVING line"
        );
    }
}
