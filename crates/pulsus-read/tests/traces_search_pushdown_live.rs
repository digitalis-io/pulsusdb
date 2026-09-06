//! Issue #492 part 4: the spanset aggregate compiled into the phase-1
//! generator, checked against a running ClickHouse.
//!
//! The hermetic suites pin the SQL TEXT. They cannot say what the text
//! returns, and that is what this suite is for:
//!
//! ```text
//!   the answer      the winners the engine returns equal an expectation
//!                   computed from the rows THIS TEST wrote, not from
//!                   the planner
//!   the cost        the pushed statement selects the same parts, granules
//!                   and ranges as the same statement with its HAVING line
//!                   removed — the clause filters, it does not read more
//!   the change      a broad selector with a selective aggregate returns
//!                   twenty traces marked complete where it returned none
//!                   marked incomplete
//!   the safety      a replayed index row moves neither a pushed min, max
//!                   nor count, and the aggregates it WOULD move are
//!                   refused
//!   the refusal     the grouping state a pushed aggregate adds is bounded
//!                   by the ceiling the generator statement already
//!                   carries, and it refuses with 422 rather than spilling
//! ```
//!
//! **The control side of every comparison is built by string surgery on
//! the production statement**, so the two differ in the `HAVING` line and
//! in nothing else by construction.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, like the sibling live suites.
//! To run:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_search_pushdown_live
//! podman rm -f pulsus-ch-test
//! ```

use std::collections::BTreeSet;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::logql::{ReadError, TooBroadReason};
use pulsus_read::traces::search_plan::{SearchParams, plan_search};
use pulsus_read::{SearchPlan, TraceEngine, TraceReadConfig};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

/// `true` when the gated half of this suite should run. Skips cleanly on
/// a developer machine with no container; **panics** rather than skipping
/// when the gate is absent in a live CI job, so a lost `env:` block
/// reddens the build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/traces_search_pushdown_live.rs for setup)"
            );
            return;
        }
    };
}

fn conn(database: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(120),
        ..ChConnConfig::default()
    }
}

fn schema_ctx(db: &str) -> SchemaParams {
    RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    }
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

/// Drops and re-creates the suite database. Every test owns its own, so
/// two of them can run at once and neither sees the other's rows.
async fn fresh_db(db: &str) -> ChClient {
    let bootstrap = ChClient::new(conn("default"))
        .await
        .expect("connect (bootstrap)");
    exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
    run_init(&bootstrap, &schema_ctx(db))
        .await
        .expect("run_init");
    ChClient::new(conn(db)).await.expect("connect (test db)")
}

fn engine_config(max_candidates: u64, generator_max_memory_bytes: u64) -> TraceReadConfig {
    TraceReadConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
        edges_table: "trace_edges".to_string(),
        max_candidates,
        scan_budget_rows: 50_000_000,
        max_series: 1_000,
        generator_max_memory_bytes,
        distributed: false,
        skip_unavailable_shards: false,
    }
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

/// The window every plan below is built with: the seeded corpus sits
/// inside it, and it is derived from the clock so the tables' seven-day
/// delete-TTL never silently empties the fixture.
fn params(base_ns: i64, span_ns: i64) -> SearchParams {
    SearchParams {
        start_ns: base_ns - 1,
        end_ns: base_ns + span_ns,
        limit: 20,
        spss: 3,
    }
}

fn plan_for(engine: &TraceEngine, q: &str, p: &SearchParams) -> SearchPlan {
    let query = pulsus_traceql::parse(q).unwrap_or_else(|e| panic!("{q}: {e}"));
    plan_search(&query, p, &engine.search_ctx()).unwrap_or_else(|e| panic!("{q}: {e:?}"))
}

fn hex32(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

/// A trace id whose byte order is the ascending order of `n`, so
/// "ascending trace id" and "ascending n" are the same tie-break.
fn trace_id(n: u32) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[12..].copy_from_slice(&n.to_be_bytes());
    id
}

fn span_id(n: u32, j: u32) -> [u8; 8] {
    let mut id = [0u8; 8];
    id[0..4].copy_from_slice(&n.to_be_bytes());
    id[4..].copy_from_slice(&j.to_be_bytes());
    id
}

// ---------------------------------------------------------------------
// Corpus Q — the corpus the answer is computed from
// ---------------------------------------------------------------------

/// One seeded span, and everything a test needs to compute the answer
/// from it without asking the planner.
#[derive(Debug, Clone, Copy)]
struct Seeded {
    trace: u32,
    span: u32,
    ts_ns: i64,
    duration_ns: i64,
    /// Carries `http.method = 'GET'` at `scope = 'span'`.
    method_attr: bool,
    /// Carries the unscoped `k = 'v'` attribute.
    k_attr: bool,
}

const Q_TRACES: u32 = 200;
/// One millisecond between traces; every span of a trace sits inside its
/// own microsecond, so ordering by the maximum matched timestamp is
/// ordering by the trace's own index.
const Q_TRACE_STEP_NS: i64 = 1_000_000;

/// The number of `http.method = GET` spans trace `n` carries: 1, 2, 3, 4,
/// cycling — so `count() > 2` is a genuinely different set from the
/// duration ones.
fn q_matched_spans(n: u32) -> u32 {
    1 + (n % 4)
}

/// The first matched span's duration, and the rest's. Two independent
/// cycles, so `min` and `max` do not select the same traces: `max` is
/// large when either is, `min` only when both are.
fn q_durations(n: u32) -> Vec<i64> {
    let first = if n.is_multiple_of(2) {
        1_100_000_000
    } else {
        400_000_000
    };
    let rest = if n.is_multiple_of(3) {
        2_000_000_000
    } else {
        500_000_000
    };
    let mut out = vec![first];
    out.extend(std::iter::repeat_n(rest, (q_matched_spans(n) - 1) as usize));
    out
}

/// Corpus Q: `Q_TRACES` traces, each with 1..4 matched spans plus one
/// UNMATCHED five-second span whose timestamp is LATER than every matched
/// one — so a sort key computed over all spans rather than over the
/// matched ones would order the answer differently.
///
/// The unscoped `k = 'v'` attribute sits on the LAST matched span of each
/// trace, which gives the fourth query its own qualifying set.
fn corpus_q(base_ns: i64) -> Vec<Seeded> {
    let mut out = Vec::new();
    for n in 0..Q_TRACES {
        let ts0 = base_ns + i64::from(n) * Q_TRACE_STEP_NS;
        let durations = q_durations(n);
        let last = durations.len() - 1;
        for (j, duration_ns) in durations.iter().copied().enumerate() {
            out.push(Seeded {
                trace: n,
                span: j as u32,
                ts_ns: ts0 + (j as i64) * 1_000,
                duration_ns,
                method_attr: true,
                k_attr: j == last,
            });
        }
        out.push(Seeded {
            trace: n,
            span: 100,
            ts_ns: ts0 + 500_000,
            duration_ns: 5_000_000_000,
            method_attr: false,
            k_attr: false,
        });
    }
    out
}

/// Writes corpus Q into `trace_spans` and `trace_attrs_idx`.
/// `attr_repeats` is how many times each attribute row is written: `1` is
/// a clean corpus and `2` is the at-least-once replay a
/// `ReplacingMergeTree` read without `FINAL` can see.
///
/// **Merges are stopped on the attribute index before the second pass**,
/// and that is what makes a replay corpus reproducible rather than a
/// race. A `ReplacingMergeTree` merge collapses two identical rows on the
/// ordering key, so a background merge between the write and the read
/// deletes the very thing the test is about — measured, one run counted
/// 1,000 of the 1,400 rows it had just written. The window before the
/// merge is the state a read without `FINAL` can genuinely observe, and
/// stopping merges holds the corpus in it.
async fn seed(client: &ChClient, db: &str, rows: &[Seeded], attr_repeats: usize) {
    if attr_repeats > 1 {
        exec(client, &format!("SYSTEM STOP MERGES {db}.trace_attrs_idx")).await;
    }
    for chunk in rows.chunks(200) {
        let values: Vec<String> = chunk
            .iter()
            .map(|r| {
                format!(
                    "(unhex('{}'), unhex('{}'), unhex('0000000000000000'), 'op', 'svc', {}, {}, \
                     0, 1, 1, '')",
                    hex32(&trace_id(r.trace)),
                    span_id(r.trace, r.span)
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>(),
                    r.ts_ns,
                    r.duration_ns,
                )
            })
            .collect();
        exec(
            client,
            &format!(
                "INSERT INTO {db}.trace_spans (trace_id, span_id, parent_id, name, service, \
                 timestamp_ns, duration_ns, status_code, kind, payload_type, payload) VALUES {}",
                values.join(", ")
            ),
        )
        .await;
    }
    let mut attrs: Vec<String> = Vec::new();
    for r in rows {
        for (key, val, scope, present) in [
            ("http.method", "GET", "span", r.method_attr),
            ("k", "v", "resource", r.k_attr),
        ] {
            if !present {
                continue;
            }
            attrs.push(format!(
                "(toDate(fromUnixTimestamp64Nano({ts})), '{key}', '{val}', '{scope}', NULL, \
                 {ts}, unhex('{tid}'), unhex('{sid}'), {dur})",
                ts = r.ts_ns,
                tid = hex32(&trace_id(r.trace)),
                sid = span_id(r.trace, r.span)
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
                dur = r.duration_ns,
            ));
        }
    }
    for _ in 0..attr_repeats {
        for chunk in attrs.chunks(200) {
            exec(
                client,
                &format!(
                    "INSERT INTO {db}.trace_attrs_idx (date, key, val, scope, val_num, \
                     timestamp_ns, trace_id, span_id, duration_ns) VALUES {}",
                    chunk.join(", ")
                ),
            )
            .await;
        }
    }
}

/// One TraceQL query, the fragment it must compile to, and the predicate
/// deciding which traces qualify — evaluated over the SEEDED rows, never
/// over anything the planner produced.
struct Qualifier {
    q: &'static str,
    fragment: &'static str,
    /// The seeded spans of one trace that this query's SELECTOR matches.
    selects: fn(&Seeded) -> bool,
    /// Does the aggregate admit a trace, given its selected spans?
    admits: fn(&[Seeded]) -> bool,
}

fn qualifiers() -> Vec<Qualifier> {
    vec![
        Qualifier {
            q: r#"{ span.http.method = "GET" } | max(duration) > 1s"#,
            fragment: "max(duration_ns) > 1000000000",
            selects: |s| s.method_attr,
            admits: |spans| spans.iter().map(|s| s.duration_ns).max().unwrap_or(0) > 1_000_000_000,
        },
        // Issue #492 part 5: `min` reads LOW over the generator's rows,
        // so `<` and `<=` are the safe operators and `>=` refuses. This
        // row was `min(duration) >= 1s` at `ddb48c96`, which pushed and
        // could lose a trace.
        Qualifier {
            // The threshold is `1s` and not `2s` because corpus Q's
            // per-trace minima are 0.4s, 0.5s and 1.1s: at `2s` every
            // trace qualifies, the `HAVING` filters nothing, and the
            // suite's own "the corpus cannot tell the two apart" guard
            // reddens. Measured — 200 rows with the HAVING and 200
            // without.
            q: r#"{ span.http.method = "GET" } | min(duration) < 1s"#,
            fragment: "min(duration_ns) < 1000000000",
            selects: |s| s.method_attr,
            admits: |spans| {
                !spans.is_empty()
                    && spans
                        .iter()
                        .map(|s| s.duration_ns)
                        .min()
                        .unwrap_or(i64::MAX)
                        < 1_000_000_000
            },
        },
        Qualifier {
            q: r#"{ span.http.method = "GET" } | count() > 2"#,
            fragment: "uniqExact(span_id) > 2",
            selects: |s| s.method_attr,
            admits: |spans| spans.len() > 2,
        },
        Qualifier {
            q: r#"{ .k = "v" } | max(duration) > 1s"#,
            fragment: "max(duration_ns) > 1000000000",
            selects: |s| s.k_attr,
            admits: |spans| spans.iter().map(|s| s.duration_ns).max().unwrap_or(0) > 1_000_000_000,
        },
    ]
}

/// The trace ids this query must return, in the order it must return
/// them, computed from `rows` alone: qualifying traces ordered by the
/// maximum SELECTED span timestamp descending, then trace id ascending,
/// capped at the request limit.
fn expected_winners(rows: &[Seeded], qualifier: &Qualifier, limit: usize) -> Vec<[u8; 16]> {
    let mut per_trace: std::collections::BTreeMap<u32, Vec<Seeded>> = Default::default();
    for r in rows.iter().filter(|r| (qualifier.selects)(r)) {
        per_trace.entry(r.trace).or_default().push(*r);
    }
    let mut winners: Vec<(i64, [u8; 16])> = per_trace
        .into_iter()
        .filter(|(_, spans)| (qualifier.admits)(spans))
        .map(|(n, spans)| {
            (
                spans.iter().map(|s| s.ts_ns).max().expect("non-empty"),
                trace_id(n),
            )
        })
        .collect();
    winners.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    winners.truncate(limit);
    winners.into_iter().map(|(_, id)| id).collect()
}

/// The generator statement with its `HAVING` line removed — the control
/// against which "the pushdown filters and reads no more" is measured.
fn without_having(sql: &str) -> String {
    let out: Vec<&str> = sql.lines().filter(|l| !l.starts_with("HAVING ")).collect();
    let out = out.join("\n");
    assert_ne!(out, sql, "the statement carries no HAVING line:\n{sql}");
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize)]
struct IdRow {
    trace_id: [u8; 16],
    /// The generator projects two columns; the decoder needs both even
    /// though only the first is read here.
    bound_ts: i64,
}

/// The trace ids one generator statement returns, in statement order.
async fn generator_ids(client: &ChClient, sql: &str) -> Vec<[u8; 16]> {
    let mut stream = client
        .query_stream::<IdRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("generator query failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode generator row").trace_id);
    }
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize)]
struct ScalarRow {
    v: u64,
}

async fn scalar(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<ScalarRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("scalar query failed: {e}\nSQL:\n{sql}"));
    let row = stream
        .next()
        .await
        .unwrap_or_else(|| panic!("no row for:\n{sql}"))
        .expect("decode scalar row");
    row.v
}

#[derive(Row, serde::Serialize, serde::Deserialize)]
struct ExplainRow {
    #[serde(with = "serde_bytes")]
    explain: Vec<u8>,
}

async fn explain_indexes(client: &ChClient, sql: &str) -> String {
    let full = format!("EXPLAIN indexes = 1 {}", sql.replace('?', "??"));
    let mut stream = client
        .query_stream::<ExplainRow>(
            &full,
            &QuerySettings::new().set("use_query_condition_cache", 0u64),
        )
        .await
        .unwrap_or_else(|e| panic!("explain failed: {e}\nSQL:\n{full}"));
    let mut out = String::new();
    while let Some(row) = stream.next().await {
        out.push_str(&String::from_utf8_lossy(
            &row.expect("decode explain row").explain,
        ));
        out.push('\n');
    }
    out
}

/// The lines of an `EXPLAIN indexes = 1` render that report what was
/// READ: parts, granules and ranges. Everything else — the node list, the
/// expressions — is what the two plans are allowed to differ in.
fn read_lines(raw: &str) -> Vec<String> {
    raw.lines()
        .map(str::trim)
        .filter(|l| {
            l.starts_with("Parts:") || l.starts_with("Granules:") || l.starts_with("Ranges:")
        })
        .map(str::to_string)
        .collect()
}

/// Corpus G, for the index-selection identity ONLY.
///
/// **Corpus Q cannot decide that identity and this one can.** Q holds
/// about 900 attribute rows, which is one granule, so every statement
/// over it reads `Granules: 1/1` — with the predicate, without the
/// predicate, with the `HAVING` and without. An identity between two
/// renders that read the same single granule is true for a reason that
/// has nothing to do with the clause under test. Measured: with corpus Q,
/// deleting the `key`/`val`/`scope` predicate from the control left the
/// two index blocks identical.
///
/// G is 500,000 rows over three contiguous `(key, val)` prefixes, which
/// is about 62 granules at the default `index_granularity` of 8,192, so a
/// prefix predicate prunes to a strict subset and the test can tell a
/// pruning statement from a scanning one.
const G_METHOD_ROWS: u64 = 200_000;
const G_DECOY_ROWS: u64 = 200_000;
const G_K_ROWS: u64 = 100_000;
const G_STEP_NS: i64 = 1_000;

async fn seed_granule_corpus(client: &ChClient, db: &str, base_ns: i64) {
    for (key, val, scope, rows, offset) in [
        ("http.method", "GET", "span", G_METHOD_ROWS, 0),
        ("zzz.decoy", "x", "span", G_DECOY_ROWS, G_METHOD_ROWS),
        ("k", "v", "resource", G_K_ROWS, G_METHOD_ROWS + G_DECOY_ROWS),
    ] {
        exec(
            client,
            &format!(
                "INSERT INTO {db}.trace_attrs_idx (date, key, val, scope, val_num, \
                 timestamp_ns, trace_id, span_id, duration_ns) SELECT \
                   toDate(fromUnixTimestamp64Nano({base_ns} + toInt64(number) * {G_STEP_NS})), \
                   '{key}', '{val}', '{scope}', NULL, \
                   {base_ns} + toInt64(number) * {G_STEP_NS}, \
                   toFixedString(unhex(leftPad(lower(hex(number % 100000)), 32, '0')), 16), \
                   toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
                   if(number % 3 = 0, 1500000000, 500000000) \
                 FROM numbers({offset}, {rows})"
            ),
        )
        .await;
    }
}

/// The generator statement with its `HAVING` line AND its leaf predicate
/// removed — the discriminator that proves the corpus can tell a pruning
/// statement from a scanning one.
fn without_having_or_predicate(sql: &str) -> String {
    let out: Vec<&str> = sql
        .lines()
        .filter(|l| !l.starts_with("HAVING ") && !l.trim_start().starts_with("AND (key"))
        .collect();
    let out = out.join("\n");
    assert_ne!(
        out, sql,
        "the statement carries no predicate to remove:\n{sql}"
    );
    out
}

// ---------------------------------------------------------------------
// Criterion 11 — the answer
// ---------------------------------------------------------------------

/// **The pushed statement returns exactly the traces that qualify, in the
/// order the contract states.**
///
/// This is the answer to "what establishes that the new SQL returns the
/// same rows as the old", and it is a command that can fail: change the
/// pushed comparison from `>` to `>=`, or `max` to `min`, in
/// `compile::aggregate_having_sql`, and the expectation this test
/// computes from its own rows stops matching what the engine returns.
///
/// The expectation is computed from the seeded rows, not from the
/// planner, so the two sides are independent producers.
#[tokio::test]
async fn the_pushed_statement_returns_exactly_the_qualifying_traces() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_pushdown_answer");
    let client = fresh_db(db).await;
    let base = now_ns() - i64::from(Q_TRACES) * Q_TRACE_STEP_NS - 3_600_000_000_000;
    let rows = corpus_q(base);
    seed(&client, db, &rows, 1).await;

    let engine = TraceEngine::new(
        ChClient::new(conn(db)).await.expect("connect (engine)"),
        engine_config(100_000, 536_870_912),
    );
    let p = params(base, i64::from(Q_TRACES) * Q_TRACE_STEP_NS + 1_000_000_000);

    for qualifier in qualifiers() {
        let plan = plan_for(&engine, qualifier.q, &p);
        assert_eq!(
            plan.pushed_having(),
            Some(qualifier.fragment),
            "{}: the aggregate must have compiled into the generator",
            qualifier.q
        );

        let want = expected_winners(&rows, &qualifier, p.limit as usize);
        // Not vacuous: each of the four aggregates admits far more than
        // the request limit on this corpus, so the answer is a genuine
        // top-20 and an empty expectation cannot be met by an empty
        // result.
        assert_eq!(
            want.len(),
            p.limit as usize,
            "{}: the corpus must admit at least the request limit",
            qualifier.q
        );
        let out = engine
            .search(&plan)
            .await
            .unwrap_or_else(|e| panic!("{}: {e:?}", qualifier.q));
        let got: Vec<[u8; 16]> = out.traces.iter().map(|t| t.trace_id).collect();
        assert_eq!(
            got.iter().map(hex32).collect::<Vec<_>>(),
            want.iter().map(hex32).collect::<Vec<_>>(),
            "{}: expected {:?}, got {:?}",
            qualifier.q,
            want.iter().map(hex32).collect::<Vec<_>>(),
            got.iter().map(hex32).collect::<Vec<_>>()
        );

        // ...and the pushed statement's own rows are a SUBSET of the same
        // statement without its HAVING line, strictly smaller here because
        // every one of the four queries is selective on this corpus.
        let pushed = &plan.generator_sqls[0];
        let pushed_ids: BTreeSet<[u8; 16]> =
            generator_ids(&client, pushed).await.into_iter().collect();
        let all_ids: BTreeSet<[u8; 16]> = generator_ids(&client, &without_having(pushed))
            .await
            .into_iter()
            .collect();
        assert!(
            pushed_ids.is_subset(&all_ids),
            "{}: the HAVING must filter the same statement's rows, not change them",
            qualifier.q
        );
        assert!(
            pushed_ids.len() < all_ids.len(),
            "{}: {} rows with the HAVING and {} without — the corpus cannot tell the two apart",
            qualifier.q,
            pushed_ids.len(),
            all_ids.len()
        );
    }
    exec(&client, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

// ---------------------------------------------------------------------
// Criterion 12 — the cost
// ---------------------------------------------------------------------

/// **The `HAVING` adds no granule reading.**
///
/// `EXPLAIN indexes = 1` with `use_query_condition_cache = 0` on the
/// pushed statement and on the same statement with the `HAVING` line
/// removed: identical `Parts:`, `Granules:` and `Ranges:` lines, and the
/// pushed plan carries one extra `Filter (HAVING)` node.
///
/// A pinned granule count would be the wrong gate — it moves with the
/// corpus. An identity between two renders of one statement is
/// scale-invariant.
///
/// **The third render is what stops the identity being vacuous.** The
/// same statement with the leaf predicate ALSO removed reads more
/// granules, so on this corpus "identical index blocks" is a fact about
/// the `HAVING` and not about a corpus too small to prune.
#[tokio::test]
async fn the_pushed_statement_reads_the_same_granules_as_the_unpushed_one() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_pushdown_granules");
    let client = fresh_db(db).await;
    let rows = G_METHOD_ROWS + G_DECOY_ROWS + G_K_ROWS;
    let base = now_ns() - (rows as i64) * G_STEP_NS - 3_600_000_000_000;
    seed_granule_corpus(&client, db, base).await;

    let engine = TraceEngine::new(
        ChClient::new(conn(db)).await.expect("connect (engine)"),
        engine_config(100_000, 536_870_912),
    );
    let p = SearchParams {
        start_ns: base - 1,
        end_ns: base + (rows as i64) * G_STEP_NS,
        limit: 20,
        spss: 3,
    };

    for qualifier in qualifiers() {
        let plan = plan_for(&engine, qualifier.q, &p);
        let pushed = plan.generator_sqls[0].clone();
        let control = without_having(&pushed);
        let scan = without_having_or_predicate(&pushed);

        let with = explain_indexes(&client, &pushed).await;
        let without = explain_indexes(&client, &control).await;
        let unpruned = explain_indexes(&client, &scan).await;
        assert_eq!(
            read_lines(&with),
            read_lines(&without),
            "{}: the HAVING moved index selection.\nwith:\n{with}\nwithout:\n{without}",
            qualifier.q
        );
        assert!(
            !read_lines(&with).is_empty(),
            "{}: no Parts/Granules/Ranges lines in:\n{with}",
            qualifier.q
        );
        assert_ne!(
            read_lines(&with),
            read_lines(&unpruned),
            "{}: dropping the leaf predicate read the SAME granules, so this corpus cannot tell a \
             pruning statement from a scanning one and the identity above proves nothing.\n\
             pruned:\n{with}\nunpruned:\n{unpruned}",
            qualifier.q
        );
        assert!(
            with.contains("Filter (HAVING)"),
            "{}: the pushed plan must carry a HAVING filter node:\n{with}",
            qualifier.q
        );
        assert!(
            !without.contains("Filter (HAVING)"),
            "{}: the control must not:\n{without}",
            qualifier.q
        );
    }
    exec(&client, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

// ---------------------------------------------------------------------
// Criterion 13a — the answer that changes
// ---------------------------------------------------------------------

/// **A broad selector with a selective aggregate completes where it used
/// to truncate.**
///
/// 1,000 traces of two spans each; one span per trace carries
/// `http.method = GET`, and that span lasts 1.5 s in the 333 traces with
/// the OLDEST timestamps and 0.5 s in the other 667. The other span is
/// 5 s and carries no attribute. The qualifying traces are deliberately
/// the OLDEST, because the candidate generator is ranked newest-first and
/// a corpus whose qualifying traces are also the newest cannot tell the
/// two paths apart.
///
/// ```text
///   max_candidates = 400
///
///   before   generator returns 401 rows (the cap probe fired) — all of
///            them 0.5 s traces, none qualifies -> 0 traces, partial
///   after    generator returns 333 rows, every one qualifies
///                                                 -> 20 traces, complete
/// ```
///
/// The "before" half is measured rather than remembered: the same
/// statement with its `HAVING` line removed is executed, its first 401
/// rows are taken, and the test asserts that none of those traces has a
/// matched span over one second.
#[tokio::test]
async fn a_broad_selector_with_a_selective_aggregate_completes_where_it_used_to_truncate() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_pushdown_truncation");
    let client = fresh_db(db).await;

    const TRACES: u32 = 1_000;
    const QUALIFYING: u32 = 333;
    const MAX_CANDIDATES: u64 = 400;
    let base = now_ns() - i64::from(TRACES) * Q_TRACE_STEP_NS - 3_600_000_000_000;
    let mut rows = Vec::new();
    for n in 0..TRACES {
        let ts0 = base + i64::from(n) * Q_TRACE_STEP_NS;
        rows.push(Seeded {
            trace: n,
            span: 0,
            ts_ns: ts0,
            duration_ns: if n < QUALIFYING {
                1_500_000_000
            } else {
                500_000_000
            },
            method_attr: true,
            k_attr: false,
        });
        rows.push(Seeded {
            trace: n,
            span: 1,
            ts_ns: ts0 + 500_000,
            duration_ns: 5_000_000_000,
            method_attr: false,
            k_attr: false,
        });
    }
    seed(&client, db, &rows, 1).await;

    let engine = TraceEngine::new(
        ChClient::new(conn(db)).await.expect("connect (engine)"),
        engine_config(MAX_CANDIDATES, 536_870_912),
    );
    let p = params(base, i64::from(TRACES) * Q_TRACE_STEP_NS + 1_000_000_000);
    let plan = plan_for(
        &engine,
        r#"{ span.http.method = "GET" } | max(duration) > 1s"#,
        &p,
    );
    assert_eq!(plan.pushed_having(), Some("max(duration_ns) > 1000000000"));

    // The candidate list the OLD path consumed: the same statement with
    // no HAVING, ranked newest-first, capped at `max_candidates + 1`.
    let before = generator_ids(&client, &without_having(&plan.generator_sqls[0])).await;
    assert_eq!(
        before.len(),
        MAX_CANDIDATES as usize + 1,
        "the unpushed generator must return the cap probe row, or nothing was truncated"
    );
    let qualifying_ids: BTreeSet<[u8; 16]> = (0..QUALIFYING).map(trace_id).collect();
    let qualifying_among_them = before
        .iter()
        .filter(|id| qualifying_ids.contains(*id))
        .count();
    assert_eq!(
        qualifying_among_them, 0,
        "the 401 newest candidates must contain none of the {QUALIFYING} qualifying traces, or \
         this corpus does not reproduce the truncation"
    );

    // The candidate list the NEW path consumes, and the answer.
    let after = generator_ids(&client, &plan.generator_sqls[0]).await;
    assert_eq!(
        after.len(),
        QUALIFYING as usize,
        "the pushed generator must return exactly the qualifying traces"
    );
    let out = engine.search(&plan).await.expect("search");
    assert_eq!(out.traces.len(), 20, "{:?}", out.partial);
    assert!(
        !out.partial,
        "no bound engaged: the response is complete where it used to be marked incomplete"
    );
    // The winners are the twenty NEWEST qualifying traces, which are the
    // oldest 333 of the corpus — the ordering the old path never reached.
    let want: Vec<[u8; 16]> = (QUALIFYING - 20..QUALIFYING).rev().map(trace_id).collect();
    assert_eq!(
        out.traces
            .iter()
            .map(|t| hex32(&t.trace_id))
            .collect::<Vec<_>>(),
        want.iter().map(hex32).collect::<Vec<_>>()
    );
    exec(&client, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

// ---------------------------------------------------------------------
// Criterion 13b — duplicate index rows
// ---------------------------------------------------------------------

/// **A replayed index row moves neither a pushed `min`, `max` nor
/// `count`, and the aggregates it would move are refused.**
///
/// `trace_attrs_idx` is a `ReplacingMergeTree` read without `FINAL`, so
/// an at-least-once replay leaves two rows for one span until a merge
/// collapses them. This seeds exactly that shape — every attribute row
/// written twice — and asserts the three pushable families return the
/// same trace set as the clean corpus.
///
/// The last assertion is the reason the other three are safe: over the
/// replayed corpus `count()` and `uniqExact(span_id)` disagree, so an
/// aggregate built on `count()` would move. `sum` and `avg` are refused
/// for the same reason, and the test states that as a fact about the
/// planner rather than measuring an aggregate we do not send.
#[tokio::test]
async fn duplicate_index_rows_do_not_move_a_pushed_min_max_or_count() {
    skip_unless_live!();
    let clean_db = &pulsus_testkit::test_db("pulsus_read_it_pushdown_clean");
    let replay_db = &pulsus_testkit::test_db("pulsus_read_it_pushdown_replay");
    let base = now_ns() - i64::from(Q_TRACES) * Q_TRACE_STEP_NS - 3_600_000_000_000;
    let rows = corpus_q(base);
    let p = params(base, i64::from(Q_TRACES) * Q_TRACE_STEP_NS + 1_000_000_000);

    let clean = fresh_db(clean_db).await;
    seed(&clean, clean_db, &rows, 1).await;
    let replay = fresh_db(replay_db).await;
    seed(&replay, replay_db, &rows, 2).await;

    // The replay is visible: the index holds twice the rows for the same
    // spans. Without this the rest of the test would prove nothing.
    let one = scalar(
        &clean,
        &format!("SELECT count() AS v FROM {clean_db}.trace_attrs_idx"),
    )
    .await;
    let two = scalar(
        &replay,
        &format!("SELECT count() AS v FROM {replay_db}.trace_attrs_idx"),
    )
    .await;
    assert_eq!(
        two,
        2 * one,
        "the replayed corpus must hold every row twice"
    );

    let clean_engine = TraceEngine::new(
        ChClient::new(conn(clean_db)).await.expect("connect"),
        engine_config(100_000, 536_870_912),
    );
    let replay_engine = TraceEngine::new(
        ChClient::new(conn(replay_db)).await.expect("connect"),
        engine_config(100_000, 536_870_912),
    );
    // Issue #492 part 5: `count() < 3` and `min(duration) >= 1s` no
    // longer push — `count()` and `min` read the wrong way over the
    // generator's rows under those operators, so the six-cell rule
    // refuses them. They stay in the list as the UNPUSHED half: their
    // answers must still not move over a replay, and a query that pushes
    // nothing is the control for a query that pushes something.
    for (q, must_push) in [
        (r#"{ span.http.method = "GET" } | max(duration) > 1s"#, true),
        (r#"{ span.http.method = "GET" } | min(duration) < 1s"#, true),
        (r#"{ span.http.method = "GET" } | count() > 2"#, true),
        (
            r#"{ span.http.method = "GET" } | min(duration) >= 1s"#,
            false,
        ),
        (r#"{ span.http.method = "GET" } | count() < 3"#, false),
    ] {
        let a = plan_for(&clean_engine, q, &p);
        let b = plan_for(&replay_engine, q, &p);
        assert_eq!(a.aggregate_pushed(), must_push, "{q} (clean)");
        assert_eq!(b.aggregate_pushed(), must_push, "{q} (replay)");
        let ids = |o: pulsus_read::SearchOutput| -> Vec<String> {
            o.traces.iter().map(|t| hex32(&t.trace_id)).collect()
        };
        let want = ids(clean_engine.search(&a).await.expect("clean search"));
        let got = ids(replay_engine.search(&b).await.expect("replay search"));
        assert!(!want.is_empty(), "{q}: the clean corpus returns nothing");
        assert_eq!(got, want, "{q}: the replay moved the answer");

        // **What `uniqExact` buys, now that correctness no longer
        // depends on it.** Under the containment rule a plain `count()`
        // would also be sound for `>`/`>=` — phase 2 re-filters — so the
        // ANSWER cannot tell the two apart on this corpus. The CANDIDATE
        // set can: `count()` inflates over a replayed row and admits
        // traces the clean corpus's statement does not. Measured here on
        // the statements themselves rather than on the answers.
        if !must_push {
            continue;
        }
        let clean_candidates = generator_ids(&clean, &a.generator_sqls[0]).await;
        let replay_candidates = generator_ids(&replay, &b.generator_sqls[0]).await;
        assert!(
            !clean_candidates.is_empty(),
            "{q}: the clean statement returns no candidate"
        );
        assert_eq!(
            replay_candidates.iter().map(hex32).collect::<Vec<_>>(),
            clean_candidates.iter().map(hex32).collect::<Vec<_>>(),
            "{q}: the replay moved the CANDIDATE set — which is what an aggregate over rows \
             rather than over distinct span ids would do"
        );
    }

    // What makes the three safe, and what makes the other two unsafe: on
    // the replayed corpus the two counts disagree.
    let predicate = "key = 'http.method' AND val = 'GET' AND scope = 'span'";
    let rows_seen = scalar(
        &replay,
        &format!("SELECT count() AS v FROM {replay_db}.trace_attrs_idx WHERE {predicate}"),
    )
    .await;
    let spans_seen = scalar(
        &replay,
        &format!(
            "SELECT uniqExact(span_id) AS v FROM {replay_db}.trace_attrs_idx WHERE {predicate}"
        ),
    )
    .await;
    assert_eq!(
        rows_seen,
        2 * spans_seen,
        "count() must inflate over the replay while uniqExact(span_id) does not — that \
         difference is why count() pushes as uniqExact and sum/avg do not push at all"
    );
    for q in [
        r#"{ span.http.method = "GET" } | sum(duration) > 1s"#,
        r#"{ span.http.method = "GET" } | avg(duration) > 1s"#,
    ] {
        assert_eq!(
            plan_for(&replay_engine, q, &p).pushed_having(),
            None,
            "{q}: a duplicate-sensitive aggregate must not compile into the generator"
        );
    }
    exec(&clean, &format!("DROP DATABASE IF EXISTS {clean_db}")).await;
    exec(&replay, &format!("DROP DATABASE IF EXISTS {replay_db}")).await;
}

// ---------------------------------------------------------------------
// Criteria 10' and 11' — the ceiling that refuses
// ---------------------------------------------------------------------

/// Corpus M1: one `trace_attrs_idx` row per distinct trace id, all
/// carrying `http.method = GET` at `scope = 'span'`, plus `SPANS` rows in
/// `trace_spans` for the trace ids with the LARGEST timestamps — so the
/// control query below fills its heap on the first batch and stops on the
/// threshold rule instead of walking every candidate two statements at a
/// time.
const M1_ROWS: u64 = 1_000_000;
const M1_SPANS: u64 = 64;
const M1_STEP_NS: i64 = 1_000;
/// 320 MiB. Measured on ClickHouse 26.3: the pushed statement's grouping
/// state over corpus M1 peaks at about 534 MB and the unpushed one at
/// about 193 MB, so this ceiling sits between them and tells the two
/// apart. It is a fixture value, not the shipped default.
const M1_CEILING_BYTES: u64 = 335_544_320;
const M1_MAX_CANDIDATES: u64 = 1_000;
/// 16 MiB — below the BARE generator's own peak over corpus M1, whose
/// `GROUP BY trace_id` holds one aggregation state per distinct trace id
/// across 1,000,000 of them. At this ceiling there is no statement that
/// answers, which is the only boundary a `422` is owed at (issue #492
/// part 5).
const M1_FLOOR_CEILING_BYTES: u64 = 16_777_216;

async fn seed_m1(client: &ChClient, db: &str, base_ns: i64) {
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_attrs_idx (date, key, val, scope, val_num, timestamp_ns, \
             trace_id, span_id, duration_ns) SELECT \
               toDate(fromUnixTimestamp64Nano({base_ns} + toInt64(number) * {M1_STEP_NS})), \
               'http.method', 'GET', 'span', NULL, \
               {base_ns} + toInt64(number) * {M1_STEP_NS}, \
               toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               1500000000 \
             FROM numbers({M1_ROWS})"
        ),
    )
    .await;
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_spans (trace_id, span_id, parent_id, name, service, \
             timestamp_ns, duration_ns, status_code, kind, payload_type, payload) SELECT \
               toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               toFixedString(unhex('0000000000000000'), 8), \
               'op', 'svc', \
               {base_ns} + toInt64(number) * {M1_STEP_NS}, \
               1500000000, 0, 1, 1, '' \
             FROM numbers({}, {M1_SPANS})",
            M1_ROWS - M1_SPANS
        ),
    )
    .await;
}

fn m1_params(base_ns: i64) -> SearchParams {
    SearchParams {
        start_ns: base_ns - 1,
        end_ns: base_ns + (M1_ROWS as i64) * M1_STEP_NS,
        limit: 20,
        spss: 3,
    }
}

/// **Issue #492 part 5 criterion 29: a query that answers `200` without
/// the pushdown answers `200` with it.**
///
/// # What this replaces, and why
///
/// At `ddb48c96` this test was
/// `the_pushed_aggregate_outgrows_the_generator_memory_ceiling_and_refuses`,
/// and it asserted `422` for exactly this query, corpus and ceiling.
/// **Part 5 withdraws that expectation.** Its sibling below proves the
/// bare statement answers under the SAME ceiling, so the `422` was a
/// query we could answer correctly and chose not to — a capability
/// regression, and one whose refusal point moves with `max_threads`
/// because it sits close to the ceiling.
///
/// The fallback is what removes it. When the pushed statement raises
/// ClickHouse code 241 — the only thing `generator_settings`'
/// `max_memory_usage` raises — the executor releases the failed
/// attempt's byte charge, records an explain entry, and runs
/// `SearchPlan::generator_fallback_sql()`: the same generator with no
/// `HAVING`, which is byte-identical to the statement this query sends
/// with nothing pushed.
///
/// ```text
///    phase 1, generator 0
///
///      pushed statement ──► 200 ─────────────────────────► candidates (narrow)
///             │
///             └─ code 241 ─► release the charge ─► fallback ──► candidates (wide, = today)
///                                                     │
///                                                     └─ code 241 ─► 422, as before
/// ```
///
/// # What each assertion is for
///
/// The answer alone would not say the fallback ran — a build that never
/// pushed at all would also answer `200` here. So the plan's own
/// `pushed_having()` is asserted first, and the explain list must carry
/// a `phase1_candidate_generator_fallback` entry whose SQL is the
/// fallback statement. The control at the same ceiling with no aggregate
/// has no such entry.
#[tokio::test]
async fn the_pushed_aggregate_outgrows_the_ceiling_and_the_fallback_answers() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_pushdown_ceiling");
    let client = fresh_db(db).await;
    let base = now_ns() - (M1_ROWS as i64) * M1_STEP_NS - 3_600_000_000_000;
    seed_m1(&client, db, base).await;

    let engine = TraceEngine::new(
        ChClient::new(conn(db)).await.expect("connect (engine)"),
        engine_config(M1_MAX_CANDIDATES, M1_CEILING_BYTES),
    );
    let p = m1_params(base);
    let plan = plan_for(&engine, r#"{ span.http.method = "GET" } | count() > 0"#, &p);
    assert_eq!(
        plan.pushed_having(),
        Some("uniqExact(span_id) > 0"),
        "the aggregate must have compiled into the generator, or this test measures nothing"
    );
    let fallback = plan
        .generator_fallback_sql()
        .expect("a pushed plan carries a fallback")
        .to_string();
    assert!(
        !fallback.contains("HAVING"),
        "the fallback is the generator with no HAVING:\n{fallback}"
    );

    let (out, explain) = engine
        .search_explained(&plan)
        .await
        .expect("the fallback must answer where the pushed statement cannot");
    assert_eq!(out.traces.len(), 20);
    assert!(
        out.partial,
        "1,000,000 candidates against a cap of {M1_MAX_CANDIDATES}: the depth bound engaged"
    );
    let entry = explain
        .stages
        .iter()
        .find(|st| st.name == "phase1_candidate_generator_fallback")
        .unwrap_or_else(|| {
            panic!(
                "the explain list must record the fallback; it holds {:?}",
                explain.stages.iter().map(|st| st.name).collect::<Vec<_>>()
            )
        });
    assert_eq!(
        entry.sql, fallback,
        "the entry carries the fallback statement"
    );
    assert_eq!(
        entry.note.as_deref(),
        Some("reasongenerator memory ceiling"),
        "the entry says why it ran"
    );

    // The control: the same corpus and the same ceiling with no
    // aggregate records NO fallback entry, so the entry above is a
    // statement about this query and not about the suite.
    let bare = plan_for(&engine, r#"{ span.http.method = "GET" }"#, &p);
    assert_eq!(bare.pushed_having(), None);
    let (_, bare_explain) = engine
        .search_explained(&bare)
        .await
        .expect("the control must answer");
    assert!(
        !bare_explain
            .stages
            .iter()
            .any(|st| st.name == "phase1_candidate_generator_fallback"),
        "a request that did not fall back must carry no fallback entry"
    );

    exec(&client, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

/// **The refusal keeps a test, at the honest boundary.**
///
/// Below the BARE statement's own peak there is no way to answer: the
/// pushed statement breaches, the fallback breaches, and the request is
/// refused exactly as it was before part 5. That is the boundary we are
/// entitled to refuse at — not "there is a way and we chose not to take
/// it".
///
/// Its break is one character in `exec.rs`: with
/// `max_bytes_before_external_group_by` non-zero the identical statement
/// spills and answers `200`. The hermetic half of that break is
/// `traces::exec::tests::generator_settings_pin_the_memory_ceiling_and_throw_not_spill`.
#[tokio::test]
async fn a_ceiling_below_the_bare_statements_own_peak_still_refuses() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_pushdown_floor");
    let client = fresh_db(db).await;
    let base = now_ns() - (M1_ROWS as i64) * M1_STEP_NS - 3_600_000_000_000;
    seed_m1(&client, db, base).await;

    let engine = TraceEngine::new(
        ChClient::new(conn(db)).await.expect("connect (engine)"),
        engine_config(M1_MAX_CANDIDATES, M1_FLOOR_CEILING_BYTES),
    );
    let p = m1_params(base);
    // BOTH statements must breach, and the second half is what makes
    // this the honest boundary rather than a repeat of the withdrawn
    // expectation: the bare statement is refused here too.
    for (q, pushes) in [
        (r#"{ span.http.method = "GET" } | count() > 0"#, true),
        (r#"{ span.http.method = "GET" }"#, false),
    ] {
        let plan = plan_for(&engine, q, &p);
        assert_eq!(plan.aggregate_pushed(), pushes, "{q}");
        match engine.search(&plan).await {
            Err(ReadError::QueryTooBroad(TooBroadReason::TraceGeneratorMemory {
                budget_bytes,
            })) => {
                assert_eq!(budget_bytes, M1_FLOOR_CEILING_BYTES, "{q}");
            }
            other => panic!(
                "{q}: expected the generator memory ceiling to refuse; got {:?}",
                other.map(|o| (o.traces.len(), o.partial))
            ),
        }
    }
    exec(&client, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

/// The control for the test above: **the same corpus, the same ceiling
/// and the same selector, differing only in the pushed `HAVING`, answers
/// `200`.**
///
/// Without it, the refusal could be a statement about a corpus that is
/// simply too big for any query. With it, the refusal is a statement
/// about the aggregate.
///
/// The corpus is generated by the same function from the same constants
/// into its own database, so the two tests can run at the same time
/// without one dropping the other's rows.
#[tokio::test]
async fn the_same_corpus_without_the_pushed_aggregate_answers_two_hundred() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_pushdown_ceiling_control");
    let client = fresh_db(db).await;
    let base = now_ns() - (M1_ROWS as i64) * M1_STEP_NS - 3_600_000_000_000;
    seed_m1(&client, db, base).await;

    let engine = TraceEngine::new(
        ChClient::new(conn(db)).await.expect("connect (engine)"),
        engine_config(M1_MAX_CANDIDATES, M1_CEILING_BYTES),
    );
    let p = m1_params(base);
    let plan = plan_for(&engine, r#"{ span.http.method = "GET" }"#, &p);
    assert_eq!(
        plan.pushed_having(),
        None,
        "the control carries no aggregate, so nothing may compile into its generator"
    );
    let out = engine.search(&plan).await.expect("the control must answer");
    assert_eq!(out.traces.len(), 20);
    assert!(
        out.partial,
        "1,000,000 candidates against a cap of {M1_MAX_CANDIDATES}: the depth bound engaged"
    );
    exec(&client, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

// ---------------------------------------------------------------------
// Round 1 — the two paths agree at the precision boundary
// ---------------------------------------------------------------------

/// Corpus B: one trace per duration, one matched span each, all carrying
/// `http.method = 'GET'`.
///
/// The durations straddle 2^53 ns, which is the only region where the
/// pushed path's exact reading of a span and the evaluator's `f64` one
/// can put that span on different sides of a threshold. Below it both
/// readings are the same integer. The exact reading here is
/// `max(duration_ns)`, an `Int64`; this corpus runs `max(duration)` and
/// no other aggregate, and the third pushed family, `uniqExact(span_id)`,
/// is a `UInt64`.
///
/// ```text
///           2^53-1     2^53     2^53+1     2^53+3
///   exact   ...991    ...992    ...993     ...995
///   f64     ...991    ...992    ...992     ...996     <- rounds both ways
/// ```
///
/// `...993` rounds DOWN and `...995` rounds UP (ties-to-even at a step of
/// 2), so a threshold sitting between two of these traces separates them
/// differently under the two readings, in both directions.
const B_DURATIONS: [i64; 5] = [
    1_000_000_000,
    9_007_199_254_740_991,
    9_007_199_254_740_992,
    9_007_199_254_740_993,
    9_007_199_254_740_995,
];

/// The thresholds every operator is run against: the same four boundary
/// values plus one ordinary one, as they would be typed.
const B_THRESHOLDS: [&str; 5] = [
    "1000000000",
    "9007199254740991",
    "9007199254740992",
    "9007199254740993",
    "9007199254740995",
];

const B_OPS: [&str; 6] = ["=", "!=", ">", ">=", "<", "<="];

fn corpus_b(base_ns: i64) -> Vec<Seeded> {
    B_DURATIONS
        .iter()
        .enumerate()
        .map(|(n, &duration_ns)| Seeded {
            trace: n as u32,
            span: 0,
            ts_ns: base_ns + (n as i64) * Q_TRACE_STEP_NS,
            duration_ns,
            method_attr: true,
            k_attr: false,
        })
        .collect()
}

/// The traces the UNPUSHED path admits, computed from `B_DURATIONS` by
/// the rule the evaluator applies — `f64(max duration) <op> f64(t)` —
/// in the order a search returns them: newest matched span first, and the
/// corpus puts trace `n`'s span later than trace `n-1`'s.
fn b_expected(op: &str, threshold: &str) -> Vec<[u8; 16]> {
    let t: f64 = threshold.parse().expect("a threshold literal parses");
    let mut ids: Vec<[u8; 16]> = B_DURATIONS
        .iter()
        .enumerate()
        .filter(|&(_, &d)| {
            let v = d as f64;
            match op {
                "=" => v == t,
                "!=" => v != t,
                ">" => v > t,
                ">=" => v >= t,
                "<" => v < t,
                "<=" => v <= t,
                other => panic!("no such operator: {other}"),
            }
        })
        .map(|(n, _)| trace_id(n as u32))
        .collect();
    ids.reverse();
    ids
}

/// **The pushed path and the unpushed path return the same traces at the
/// precision boundary, under every comparison operator** (issue #492
/// part 4, round 1).
///
/// The two paths are the same query with two selectors: `=` on the
/// attribute compiles the aggregate into the generator's `HAVING`, and
/// `=~` on the same value does not, because a regex selector is not an
/// exact generator (`NotExact::ValuePredIsNotEquality`). Both select the
/// same span — the pattern is anchored, so `=~ "GET"` matches `GET` and
/// nothing else — so the ONLY difference between the two runs is where
/// the aggregate was evaluated.
///
/// **This test carries the boundary rather than one number.** Round 1
/// found the defect on `| max(duration) = 9007199254740993`, which
/// returned 0 traces pushed and 1 unpushed. A build that special-cased
/// that literal would pass a test naming only it. The thresholds here run
/// from `2^53 - 1` (still pushed, and still exact) through `2^53`,
/// `2^53 + 1` and `2^53 + 3`, and the corpus holds a span at each of
/// those four durations, so a threshold and a span duration meet at every
/// point where the two readings begin to differ.
///
/// Each of the six operators is run against each of the five thresholds:
/// a rounded threshold LOSES a qualifying trace on some operators and
/// ADMITS a non-qualifying one on others, so an operator-by-operator
/// answer would be six answers where the threshold rule is one.
///
/// The third producer is `b_expected`, which computes the answer from the
/// seeded durations and never asks the planner anything; without it "the
/// two agree" could be two empty lists.
#[tokio::test]
async fn the_two_paths_agree_at_the_precision_boundary_under_every_operator() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_pushdown_boundary");
    let client = fresh_db(db).await;
    let base = now_ns() - (B_DURATIONS.len() as i64) * Q_TRACE_STEP_NS - 3_600_000_000_000;
    let rows = corpus_b(base);
    seed(&client, db, &rows, 1).await;

    let engine = TraceEngine::new(
        ChClient::new(conn(db)).await.expect("connect (engine)"),
        engine_config(100_000, 536_870_912),
    );
    let p = params(
        base,
        (B_DURATIONS.len() as i64) * Q_TRACE_STEP_NS + 1_000_000_000,
    );

    // How many (operator, threshold) cases each answer shape occurred in,
    // so the agreement cannot be an agreement about nothing.
    let (mut empty, mut full, mut between) = (0usize, 0usize, 0usize);
    // Every case that failed, not the first: on a build that pushes a
    // rounded threshold several operators are wrong at once and in
    // OPPOSITE directions, and the whole set is the finding.
    let mut wrong: Vec<String> = Vec::new();
    for threshold in B_THRESHOLDS {
        // Every threshold at or past 2^53 refuses to push; the one below
        // it still does. Asserted per threshold rather than described,
        // because "they agree" is also true when nothing pushes at all.
        let in_range = threshold.parse::<i64>().expect("an integer literal") < (1i64 << 53);
        for op in B_OPS {
            // Issue #492 part 5: TWO rules compose here and neither
            // subsumes the other. `max(duration)` reads HIGH over the
            // generator's rows, so only `>` and `>=` are safe; the other
            // four refuse whatever the literal is. The threshold rule is
            // then what decides the remaining two.
            let pushes = in_range && matches!(op, ">" | ">=");
            let pushed_q =
                format!(r#"{{ span.http.method = "GET" }} | max(duration) {op} {threshold}"#);
            let plain_q =
                format!(r#"{{ span.http.method =~ "GET" }} | max(duration) {op} {threshold}"#);
            let pushed_plan = plan_for(&engine, &pushed_q, &p);
            let plain_plan = plan_for(&engine, &plain_q, &p);
            if pushed_plan.pushed_having().is_some() != pushes {
                wrong.push(format!(
                    "{pushed_q}: the aggregate {} have compiled into the generator, and it {}",
                    if pushes { "must" } else { "must not" },
                    match pushed_plan.pushed_having() {
                        Some(f) => format!("rendered {f:?}"),
                        None => "did not".to_string(),
                    }
                ));
            }
            assert_eq!(
                plain_plan.pushed_having(),
                None,
                "{plain_q}: the control must evaluate the aggregate in the engine"
            );

            let pushed_out = engine
                .search(&pushed_plan)
                .await
                .unwrap_or_else(|e| panic!("{pushed_q}: {e:?}"));
            let plain_out = engine
                .search(&plain_plan)
                .await
                .unwrap_or_else(|e| panic!("{plain_q}: {e:?}"));
            let got: Vec<String> = pushed_out
                .traces
                .iter()
                .map(|t| hex32(&t.trace_id))
                .collect();
            let control: Vec<String> = plain_out
                .traces
                .iter()
                .map(|t| hex32(&t.trace_id))
                .collect();
            let want: Vec<String> = b_expected(op, threshold).iter().map(hex32).collect();

            if got != control {
                wrong.push(format!(
                    "{pushed_q}: pushed returned {} trace(s) {got:?} and the same query unpushed \
                     returned {} {control:?}",
                    got.len(),
                    control.len()
                ));
            }
            if control != want {
                wrong.push(format!(
                    "{plain_q}: the unpushed path returned {control:?} where the seeded durations \
                     give {want:?}"
                ));
            }

            match want.len() {
                0 => empty += 1,
                n if n == B_DURATIONS.len() => full += 1,
                _ => between += 1,
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} cases failed:\n{}",
        wrong.len(),
        B_THRESHOLDS.len() * B_OPS.len(),
        wrong.join("\n")
    );
    assert!(
        empty > 0 && full > 0 && between > 0,
        "the matrix must contain an empty, a full and a partial answer — it held {empty}, {full} \
         and {between}"
    );
    exec(&client, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

// ---------------------------------------------------------------------
// Issue #492 part 5 — corpus U: a repeated `span_id` whose rows disagree
// ---------------------------------------------------------------------

/// One seeded row of corpus U. A `span` listed twice inside one trace is
/// **two rows for one `(trace_id, span_id)`** at different
/// `timestamp_ns` — a re-send of one span whose second delivery
/// disagrees with the first. The evaluator keeps the FIRST row per
/// `span_id` in `(trace_id, timestamp_ns, span_id)` order
/// (`exec::group_hydrated_rows` over `search_sql::hydration_sql`'s
/// ordering) and applies the selector to that survivor; the generator
/// statement aggregates every row.
#[derive(Debug, Clone)]
struct URow {
    trace: u32,
    span: u32,
    duration_ns: i64,
    name: String,
    service: String,
}

fn u(trace: u32, span: u32, duration_s: i64, service: &str) -> URow {
    URow {
        trace,
        span,
        duration_ns: duration_s * 1_000_000_000,
        name: "op".to_string(),
        service: service.to_string(),
    }
}

/// Corpus U, exactly as issue #492 part 5's plan defines it.
///
/// ```text
///   trace  rows                                    what disagrees  the engine's deduped spans
///   …0001  S1 1s          S2 5s                    nothing         1s, 5s
///   …0002  S1 3s                                   nothing         3s
///   …0003  S1 3s          S2 3s      S3 3s         nothing         3s, 3s, 3s
///   …000a  S1 5s (first)  S1 1s                    duration        5s
///   …000b  S1 1s (first)  S1 5s                    duration        1s
///   …000d  S1 5s (first)  S1 1s      S2 3s         duration        5s, 3s
///   …000e  S1 3s svc=other (first)   S1 3s svc     the SELECTOR    3s  (S1 is dropped;
///                          S2 3s svc                                    only S2 survives)
/// ```
///
/// `…000e` is the one whose surviving row does not satisfy the selector,
/// so under `{ resource.service.name = "svc" }` the evaluator counts ONE
/// span there and the generator's rows hold two. That is the shape in
/// which `R ⊇ D` is a strict containment on the span table and not only
/// on the index.
fn corpus_u() -> Vec<URow> {
    vec![
        u(1, 1, 1, "svc"),
        u(1, 2, 5, "svc"),
        u(2, 1, 3, "svc"),
        u(3, 1, 3, "svc"),
        u(3, 2, 3, "svc"),
        u(3, 3, 3, "svc"),
        u(10, 1, 5, "svc"),
        u(10, 1, 1, "svc"),
        u(11, 1, 1, "svc"),
        u(11, 1, 5, "svc"),
        u(13, 1, 5, "svc"),
        u(13, 1, 1, "svc"),
        u(13, 2, 3, "svc"),
        u(14, 1, 3, "other"),
        u(14, 1, 3, "svc"),
        u(14, 2, 3, "svc"),
    ]
}

/// Writes rows into `trace_spans` and one `http.method = 'GET'`
/// attribute-index row per span row.
///
/// `ts_ns` is assigned from each row's position: trace index first, then
/// the row's position within its trace. So the list ORDER is the
/// timestamp order, which is what makes "the first row of a repeated
/// `span_id`" the row written first in [`corpus_u`].
///
/// `repeats` is how many times every row is written. `2` is the
/// byte-identical at-least-once redelivery; merges are stopped on the
/// attribute index first, because a `ReplacingMergeTree` merge collapses
/// two identical rows on the ordering key and would delete the very
/// thing the test is about.
async fn seed_u(client: &ChClient, db: &str, base_ns: i64, rows: &[URow], repeats: usize) {
    if repeats > 1 {
        exec(client, &format!("SYSTEM STOP MERGES {db}.trace_attrs_idx")).await;
        exec(client, &format!("SYSTEM STOP MERGES {db}.trace_spans")).await;
    }
    let mut seen: std::collections::BTreeMap<u32, i64> = Default::default();
    let mut spans = Vec::new();
    let mut attrs = Vec::new();
    for r in rows {
        let j = seen.entry(r.trace).or_insert(0);
        let ts = base_ns + i64::from(r.trace) * Q_TRACE_STEP_NS + *j * 1_000;
        *j += 1;
        let tid = hex32(&trace_id(r.trace));
        let sid: String = span_id(r.trace, r.span)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        spans.push(format!(
            "(unhex('{tid}'), unhex('{sid}'), unhex('0000000000000000'), '{}', '{}', {ts}, {}, \
             0, 1, 1, '')",
            r.name, r.service, r.duration_ns
        ));
        attrs.push(format!(
            "(toDate(fromUnixTimestamp64Nano({ts})), 'http.method', 'GET', 'span', NULL, {ts}, \
             unhex('{tid}'), unhex('{sid}'), {})",
            r.duration_ns
        ));
    }
    for _ in 0..repeats {
        for chunk in spans.chunks(200) {
            exec(
                client,
                &format!(
                    "INSERT INTO {db}.trace_spans (trace_id, span_id, parent_id, name, service, \
                     timestamp_ns, duration_ns, status_code, kind, payload_type, payload) VALUES \
                     {}",
                    chunk.join(", ")
                ),
            )
            .await;
        }
        for chunk in attrs.chunks(200) {
            exec(
                client,
                &format!(
                    "INSERT INTO {db}.trace_attrs_idx (date, key, val, scope, val_num, \
                     timestamp_ns, trace_id, span_id, duration_ns) VALUES {}",
                    chunk.join(", ")
                ),
            )
            .await;
        }
    }
}

/// The eighteen (aggregate, operator) cells, and which six the six-cell
/// rule admits.
///
/// `count()` and `max(duration)` read HIGH over the generator's rows and
/// `min(duration)` reads LOW, so `>`/`>=` are safe for the first two and
/// `<`/`<=` for the third. The other twelve LOSE a trace and must send
/// no fragment at all.
const U_CELLS: [(&str, bool); 18] = [
    ("count() > 1", true),
    ("count() >= 2", true),
    ("count() < 2", false),
    ("count() <= 1", false),
    ("count() = 1", false),
    ("count() != 2", false),
    ("min(duration) < 2s", true),
    ("min(duration) <= 1s", true),
    ("min(duration) > 2s", false),
    ("min(duration) >= 5s", false),
    ("min(duration) = 5s", false),
    ("min(duration) != 1s", false),
    ("max(duration) > 2s", true),
    ("max(duration) >= 5s", true),
    ("max(duration) < 2s", false),
    ("max(duration) <= 1s", false),
    ("max(duration) = 1s", false),
    ("max(duration) != 5s", false),
];

fn u_params(base_ns: i64) -> SearchParams {
    params(base_ns, 20 * Q_TRACE_STEP_NS)
}

/// **Issue #492 part 5 criterion 24: every one of the eighteen cells
/// gives the same answer pushed and unpushed, on both exact families,
/// over a corpus where a repeated `span_id`'s rows disagree.**
///
/// At `ddb48c96` twelve of these thirty-six cells returned FEWER traces
/// than the query matches, and phase 2 cannot put a lost trace back.
/// This is the corpus those losses were measured on.
///
/// # The three producers
///
/// * the **pushed** plan — `{ selector } | <cell>`, whose generator
///   statement carries the fragment;
/// * the **unpushed** plan — `{ selector } && { selector } | <cell>`,
///   which matches the same spans (`eval_spanset`'s `And` takes the
///   union of two identical sets) and is refused by
///   `generator_exactness`'s one-leaf condition, so its aggregate runs
///   in the evaluator;
/// * three **literal** answers, written from the seeded rows rather than
///   read off either plan, so "the two agree" cannot be two wrong lists.
///
/// # The superset is asserted separately
///
/// A criterion comparing only answers cannot see the mechanism the six
/// surviving cells rest on. `max(duration) > 2s` returns six traces and
/// its statement returns SEVEN candidate rows — `…000b`, whose two rows
/// for one span id read 1s and 5s, is admitted by the statement and
/// dropped by phase 2. So the candidate rows are asserted to be a
/// superset of the answer, per cell.
#[tokio::test]
async fn every_ungrouped_cell_agrees_with_the_unpushed_plan_on_a_conflicting_corpus() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_pushdown_conflict");
    let client = fresh_db(db).await;
    let base = now_ns() - 20 * Q_TRACE_STEP_NS - 3_600_000_000_000;
    seed_u(&client, db, base, &corpus_u(), 1).await;

    let engine = TraceEngine::new(
        ChClient::new(conn(db)).await.expect("connect (engine)"),
        engine_config(100_000, 536_870_912),
    );
    let p = u_params(base);

    let mut wrong: Vec<String> = Vec::new();
    for selector in [
        r#"{ span.http.method = "GET" }"#,
        r#"{ resource.service.name = "svc" }"#,
    ] {
        for (cell, pushes) in U_CELLS {
            let pushed_q = format!("{selector} | {cell}");
            let unpushed_q = format!("{selector} && {selector} | {cell}");
            let pushed = plan_for(&engine, &pushed_q, &p);
            let unpushed = plan_for(&engine, &unpushed_q, &p);
            if pushed.aggregate_pushed() != pushes {
                wrong.push(format!(
                    "{pushed_q}: expected the fragment to {}, and it {}",
                    if pushes { "render" } else { "be refused" },
                    match pushed.pushed_having() {
                        Some(f) => format!("rendered {f:?}"),
                        None => "was not".to_string(),
                    }
                ));
            }
            assert!(
                !unpushed.aggregate_pushed(),
                "{unpushed_q}: the control must evaluate the aggregate in the engine"
            );

            let ids = |o: &pulsus_read::SearchOutput| -> Vec<String> {
                o.traces.iter().map(|t| hex32(&t.trace_id)).collect()
            };
            let got = ids(&engine
                .search(&pushed)
                .await
                .unwrap_or_else(|e| panic!("{pushed_q}: {e:?}")));
            let want = ids(&engine
                .search(&unpushed)
                .await
                .unwrap_or_else(|e| panic!("{unpushed_q}: {e:?}")));
            if got != want {
                wrong.push(format!(
                    "{pushed_q}: pushed returned {got:?} and unpushed returned {want:?}"
                ));
            }

            // The candidate rows are a SUPERSET of the answer: the
            // mechanism the six surviving cells rest on, per cell.
            let candidates: BTreeSet<String> = generator_ids(&client, &pushed.generator_sqls[0])
                .await
                .iter()
                .map(hex32)
                .collect();
            let missing: Vec<&String> = got.iter().filter(|id| !candidates.contains(*id)).collect();
            if !missing.is_empty() {
                wrong.push(format!(
                    "{pushed_q}: the statement did not return {missing:?}, which the answer holds"
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} cases failed:\n{}",
        wrong.len(),
        2 * U_CELLS.len(),
        wrong.join("\n")
    );

    // The three literal answers, written from the seeded rows. Without
    // them the agreement above could be an agreement about nothing.
    async fn answer(engine: &TraceEngine, p: &SearchParams, q: &str) -> Vec<String> {
        let plan = plan_for(engine, q, p);
        engine
            .search(&plan)
            .await
            .unwrap_or_else(|e| panic!("{q}: {e:?}"))
            .traces
            .iter()
            .map(|t| hex32(&t.trace_id))
            .collect()
    }
    let id = |n: u32| hex32(&trace_id(n));
    assert_eq!(
        answer(
            &engine,
            &p,
            r#"{ span.http.method = "GET" } | max(duration) < 2s"#
        )
        .await,
        vec![id(11)],
        "…000b's two rows read 1s then 5s; the evaluator keeps the first, so its max is 1s. At \
         ddb48c96 this answered with no traces at all"
    );
    assert_eq!(
        answer(
            &engine,
            &p,
            r#"{ span.http.method = "GET" } | min(duration) > 2s"#
        )
        .await,
        vec![id(14), id(13), id(10), id(3), id(2)],
        "…000d and …000a each keep a 5s first row; at ddb48c96 this answered with three traces"
    );
    let six = answer(
        &engine,
        &p,
        r#"{ span.http.method = "GET" } | max(duration) > 2s"#,
    )
    .await;
    assert_eq!(
        six,
        vec![id(14), id(13), id(10), id(3), id(2), id(1)],
        "the surviving cell's answer does not move"
    );
    let candidates: Vec<String> = generator_ids(
        &client,
        &plan_for(
            &engine,
            r#"{ span.http.method = "GET" } | max(duration) > 2s"#,
            &p,
        )
        .generator_sqls[0],
    )
    .await
    .iter()
    .map(hex32)
    .collect();
    assert_eq!(
        candidates.len(),
        7,
        "seven candidates against six answers: …000b is admitted by the statement and removed by \
         phase 2 — {candidates:?}"
    );
    assert!(
        candidates.contains(&id(11)),
        "…000b is the admitted one: {candidates:?}"
    );

    exec(&client, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

/// **Issue #492 part 5 criterion 25: a trace past the hydration cap does
/// not lose its `count()` cells.**
///
/// `search_sql::hydration_sql` carries `LIMIT MAX_SPANS_PER_TRACE + 1 BY
/// trace_id` (10 000) and `exec::group_hydrated_rows` drops the overflow
/// probe row, so a trace with 10 002 matched spans is evaluated on
/// 10 000 of them while `uniqExact(span_id)` counts all 10 002. That is
/// the witness that `count()` is not exempt from the containment rule —
/// on corpus U all four of its anti-monotone cells happen to agree, and
/// "the defect lives in the exemption" is exactly the shape that hides.
///
/// One trace, 10 002 spans, five cells, each asserted against the same
/// query planned unpushed.
#[tokio::test]
async fn a_trace_past_the_hydration_cap_does_not_lose_its_count_cells() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_pushdown_cap");
    let client = fresh_db(db).await;
    /// Two more than `exec::MAX_SPANS_PER_TRACE`, which is the narrowest
    /// separation this mechanism admits: one more would be inside the
    /// `+ 1` overflow probe.
    const CAP_SPANS: u64 = 10_002;
    let base = now_ns() - (CAP_SPANS as i64) * 1_000 - 3_600_000_000_000;
    exec(
        &client,
        &format!(
            "INSERT INTO {db}.trace_spans (trace_id, span_id, parent_id, name, service, \
             timestamp_ns, duration_ns, status_code, kind, payload_type, payload) SELECT \
               toFixedString(unhex('{tid}'), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               toFixedString(unhex('0000000000000000'), 8), 'op', 'svc', \
               {base} + toInt64(number) * 1000, 1000000000, 0, 1, 1, '' \
             FROM numbers({CAP_SPANS})",
            tid = hex32(&trace_id(1)),
        ),
    )
    .await;
    exec(
        &client,
        &format!(
            "INSERT INTO {db}.trace_attrs_idx (date, key, val, scope, val_num, timestamp_ns, \
             trace_id, span_id, duration_ns) SELECT \
               toDate(fromUnixTimestamp64Nano({base} + toInt64(number) * 1000)), \
               'http.method', 'GET', 'span', NULL, {base} + toInt64(number) * 1000, \
               toFixedString(unhex('{tid}'), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), 1000000000 \
             FROM numbers({CAP_SPANS})",
            tid = hex32(&trace_id(1)),
        ),
    )
    .await;

    let engine = TraceEngine::new(
        ChClient::new(conn(db)).await.expect("connect (engine)"),
        engine_config(100_000, 536_870_912),
    );
    let p = params(base, (CAP_SPANS as i64) * 1_000 + 1_000_000_000);

    let mut wrong: Vec<String> = Vec::new();
    for (cell, pushes) in [
        ("count() < 10001", false),
        ("count() <= 10000", false),
        ("count() = 10000", false),
        ("count() != 10002", false),
        ("count() > 10001", true),
    ] {
        let pushed_q = format!(r#"{{ span.http.method = "GET" }} | {cell}"#);
        let unpushed_q =
            format!(r#"{{ span.http.method = "GET" }} && {{ span.http.method = "GET" }} | {cell}"#);
        let pushed = plan_for(&engine, &pushed_q, &p);
        let unpushed = plan_for(&engine, &unpushed_q, &p);
        // COLLECTED, not asserted: an `assert_eq!` here would fire
        // before the answers were compared, so a build that admitted an
        // anti-monotone cell would redden on the push STATUS and never
        // reach the claim this criterion is about, which is that the
        // ANSWER does not move.
        if pushed.aggregate_pushed() != pushes {
            wrong.push(format!(
                "{pushed_q}: expected the fragment to {}, and it {}",
                if pushes { "render" } else { "be refused" },
                match pushed.pushed_having() {
                    Some(f) => format!("rendered {f:?}"),
                    None => "was not".to_string(),
                }
            ));
        }
        assert!(!unpushed.aggregate_pushed(), "{unpushed_q}");
        let ids = |o: pulsus_read::SearchOutput| -> Vec<String> {
            o.traces.iter().map(|t| hex32(&t.trace_id)).collect()
        };
        let got = ids(engine
            .search(&pushed)
            .await
            .unwrap_or_else(|e| panic!("{pushed_q}: {e:?}")));
        let want = ids(engine
            .search(&unpushed)
            .await
            .unwrap_or_else(|e| panic!("{unpushed_q}: {e:?}")));
        if got != want {
            wrong.push(format!(
                "{pushed_q}: pushed returned {got:?} and unpushed returned {want:?}"
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} cell(s) failed:\n{}",
        wrong.len(),
        wrong.join("\n")
    );

    // The literal answer, and it is the one that moved: at `ddb48c96`
    // this query returned no traces at all.
    let plan = plan_for(
        &engine,
        r#"{ span.http.method = "GET" } | count() < 10001"#,
        &p,
    );
    let out = engine.search(&plan).await.expect("search");
    assert_eq!(
        out.traces
            .iter()
            .map(|t| hex32(&t.trace_id))
            .collect::<Vec<_>>(),
        vec![hex32(&trace_id(1))],
        "the trace the evaluator counts 10 000 spans in and the statement counts 10 002"
    );
    exec(&client, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

// ---------------------------------------------------------------------
// Issue #492 part 5 — the grouped arm
// ---------------------------------------------------------------------

fn named(trace: u32, span: u32, duration_s: i64, name: &str) -> URow {
    URow {
        trace,
        span,
        duration_ns: duration_s * 1_000_000_000,
        name: name.to_string(),
        service: "svc".to_string(),
    }
}

/// **Issue #492 part 5 criteria 6 and 7: the two written orders separate
/// the narrowest pair, and the grouped answer is the unpushed answer.**
///
/// Corpus C3b is two traces that are identical in every respect the
/// query can see except how their four spans divide between two names:
///
/// ```text
///   …0001   a a a b     the biggest group holds THREE spans
///   …0002   a a b b     the biggest group holds TWO
/// ```
///
/// ```text
///   { resource.service.name = "svc" } | count() > 2 | by(name)   -> 0001, 0002
///   { resource.service.name = "svc" } | by(name) | count() > 2   -> 0001
/// ```
///
/// Both spellings answer `200`. For `…0002` the first returns two span
/// sets and four spans; the second returns **no trace at all**, because
/// an aggregate that empties the span-set list ends the trace
/// (`docs/api.md` §4.2).
///
/// The second half is criterion 6: the grouped spelling's answer is the
/// answer the SAME query gives with nothing pushed, so the statement
/// filtered and did not decide.
#[tokio::test]
async fn the_grouped_and_ungrouped_spellings_separate_the_narrowest_pair() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_pushdown_c3b");
    let client = fresh_db(db).await;
    let base = now_ns() - 20 * Q_TRACE_STEP_NS - 3_600_000_000_000;
    let rows = vec![
        named(1, 1, 1, "a"),
        named(1, 2, 1, "a"),
        named(1, 3, 1, "a"),
        named(1, 4, 1, "b"),
        named(2, 1, 1, "a"),
        named(2, 2, 1, "a"),
        named(2, 3, 1, "b"),
        named(2, 4, 1, "b"),
    ];
    seed_u(&client, db, base, &rows, 1).await;

    let engine = TraceEngine::new(
        ChClient::new(conn(db)).await.expect("connect (engine)"),
        engine_config(100_000, 536_870_912),
    );
    let p = u_params(base);
    let ids = |o: pulsus_read::SearchOutput| -> Vec<String> {
        o.traces.iter().map(|t| hex32(&t.trace_id)).collect()
    };

    let ungrouped_q = r#"{ resource.service.name = "svc" } | count() > 2 | by(name)"#;
    let grouped_q = r#"{ resource.service.name = "svc" } | by(name) | count() > 2"#;
    let ungrouped = plan_for(&engine, ungrouped_q, &p);
    let grouped = plan_for(&engine, grouped_q, &p);
    assert_eq!(
        ungrouped.pushed_having(),
        Some("uniqExact(span_id) > 2"),
        "the aggregate at its written position filters the whole matched set"
    );
    assert_eq!(
        grouped.pushed_having(),
        Some(
            "arrayMax(mapValues(uniqExactMap(map(if(length(name) <= 8192, name, \
             substringUTF8(name, 1, 2048)), span_id)))) > 2"
        ),
        "the aggregate after a by() filters GROUPS"
    );

    assert_eq!(
        ids(engine.search(&ungrouped).await.expect("ungrouped search")),
        vec![hex32(&trace_id(2)), hex32(&trace_id(1))],
        "{ungrouped_q}: both traces have four matched spans"
    );
    assert_eq!(
        ids(engine.search(&grouped).await.expect("grouped search")),
        vec![hex32(&trace_id(1))],
        "{grouped_q}: …0002's biggest group holds two spans, so its span-set list empties and \
         the trace ends"
    );

    // Criterion 6: the grouped answer is the answer with nothing pushed.
    // The unpushed spelling is the same pipeline behind a selector the
    // one-leaf exactness condition refuses, so the ONLY difference is
    // where the aggregate ran.
    let unpushed = plan_for(
        &engine,
        r#"{ resource.service.name = "svc" } && { resource.service.name = "svc" } | by(name) | count() > 2"#,
        &p,
    );
    assert!(!unpushed.aggregate_pushed());
    assert_eq!(
        ids(engine.search(&grouped).await.expect("grouped search")),
        ids(engine.search(&unpushed).await.expect("unpushed search")),
        "the grouped statement filtered; it did not decide"
    );
    exec(&client, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

/// **Issue #492 part 5 criterion 10: a string group key groups by the
/// BYTE-CAPPED expression.**
///
/// The evaluator groups the capped hydrated `name`
/// (`search_sql::hydration_sql` projects
/// `if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS
/// name`), so the statement must group the same expression. Grouping the
/// raw column is a FINER partition and loses the trace.
///
/// The corpus is the narrowest pair that can show it: `length(col) <=
/// 8192` is inclusive, so 8192 bytes caps to itself and **8193 is the
/// first that truncates**. Two names of 8193 bytes sharing their first
/// 2048 code points cap to the same value and differ raw:
///
/// ```text
///   '𝄞' x 2048 + '0'   8193 bytes, 2049 code points
///   '𝄞' x 2048 + '1'   8193 bytes, 2049 code points
///
///   GROUP BY the raw column     -> groups of 2 and 1, count() > 2 DROPS the trace
///   GROUP BY the capped column  -> one group of 3,    count() > 2 KEEPS it
/// ```
///
/// There is no ingest length cap on `name`
/// (`pulsus-write/src/protocols/otlp_traces.rs` binds it unbounded), so
/// this is reachable through our own write path.
#[tokio::test]
async fn a_string_group_key_groups_by_the_capped_expression() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_pushdown_capkey");
    let client = fresh_db(db).await;
    let base = now_ns() - 20 * Q_TRACE_STEP_NS - 3_600_000_000_000;
    let long = "\u{1D11E}".repeat(2048);
    let a = format!("{long}0");
    let b = format!("{long}1");
    assert_eq!(a.len(), 8193, "the first length that truncates");
    assert_eq!(a.chars().count(), 2049);
    let rows = vec![named(1, 1, 1, &a), named(1, 2, 1, &a), named(1, 3, 1, &b)];
    seed_u(&client, db, base, &rows, 1).await;

    // The corpus really does hold two distinct raw names that cap to
    // one, or the assertion below would be about nothing.
    assert_eq!(
        scalar(
            &client,
            &format!("SELECT uniqExact(name) AS v FROM {db}.trace_spans"),
        )
        .await,
        2,
        "two distinct raw names"
    );
    assert_eq!(
        scalar(
            &client,
            &format!("SELECT uniqExact(substringUTF8(name, 1, 2048)) AS v FROM {db}.trace_spans"),
        )
        .await,
        1,
        "one capped name"
    );

    let engine = TraceEngine::new(
        ChClient::new(conn(db)).await.expect("connect (engine)"),
        engine_config(100_000, 536_870_912),
    );
    let p = u_params(base);
    let plan = plan_for(
        &engine,
        r#"{ resource.service.name = "svc" } | by(name) | count() > 2"#,
        &p,
    );
    let frag = plan.pushed_having().expect("the grouped fragment renders");
    assert!(
        frag.contains("substringUTF8(name, 1, 2048)"),
        "the group key must be the capped expression: {frag}"
    );
    let out = engine.search(&plan).await.expect("search");
    assert_eq!(
        out.traces
            .iter()
            .map(|t| hex32(&t.trace_id))
            .collect::<Vec<_>>(),
        vec![hex32(&trace_id(1))],
        "three spans capping to one name: the capped grouping keeps the trace"
    );

    // The break, run in the test rather than described: the same
    // statement with the cap removed from the map key returns nothing,
    // because three spans become groups of two and one.
    let raw = plan.generator_sqls[0].replace(
        "if(length(name) <= 8192, name, substringUTF8(name, 1, 2048))",
        "name",
    );
    assert_ne!(raw, plan.generator_sqls[0], "the control must differ");
    assert!(
        generator_ids(&client, &raw).await.is_empty(),
        "grouping the RAW column must lose the trace — which is what makes the cap load-bearing"
    );
    exec(&client, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

/// **Issue #492 part 5 criteria 6b and 6c: the six grouped cells return
/// a superset the engine re-filters, and a byte-identical redelivery
/// moves neither side.**
///
/// The corpus is C3b plus the three conflicting shapes — a trace whose
/// repeated `span_id` carries two different NAMES (so the map assigns
/// one span to two group keys where the evaluator assigns it to one),
/// and two whose repeated `span_id` carries two different durations.
///
/// ```text
///   …0001  a a a b                       four spans, biggest group three
///   …0002  a a b b                       four spans, biggest group two
///   …0009  S1 name a, S1 name b          one span id, two rows, names SWAPPED
///          S2 name b, S2 name a
///   …000a  S1 5s (first), S1 1s          one span id, two rows, durations disagree
///   …000b  S1 1s (first), S1 5s
/// ```
///
/// **Criterion 6c is what licenses pushing at all.** If a BYTE-IDENTICAL
/// at-least-once redelivery could move either side, the map form could
/// not be used on a plain `MergeTree` at all. It cannot: the same rows
/// written twice give the same answer and the same candidate set for
/// every cell, because the aggregate is over DISTINCT span ids and the
/// duplicated rows carry the same key and the same value.
#[tokio::test]
async fn the_grouped_cells_return_a_superset_and_a_replay_moves_neither_side() {
    skip_unless_live!();
    let clean_db = &pulsus_testkit::test_db("pulsus_read_it_grouped_clean");
    let replay_db = &pulsus_testkit::test_db("pulsus_read_it_grouped_replay");
    let base = now_ns() - 20 * Q_TRACE_STEP_NS - 3_600_000_000_000;
    let rows = vec![
        named(1, 1, 1, "a"),
        named(1, 2, 1, "a"),
        named(1, 3, 1, "a"),
        named(1, 4, 1, "b"),
        named(2, 1, 1, "a"),
        named(2, 2, 1, "a"),
        named(2, 3, 1, "b"),
        named(2, 4, 1, "b"),
        named(9, 1, 1, "a"),
        named(9, 1, 1, "b"),
        named(9, 2, 1, "b"),
        named(9, 2, 1, "a"),
        named(10, 1, 5, "a"),
        named(10, 1, 1, "a"),
        named(11, 1, 1, "a"),
        named(11, 1, 5, "a"),
    ];
    let clean = fresh_db(clean_db).await;
    seed_u(&clean, clean_db, base, &rows, 1).await;
    let replay = fresh_db(replay_db).await;
    seed_u(&replay, replay_db, base, &rows, 2).await;

    // The replay is visible, or the rest proves nothing.
    let one = scalar(
        &clean,
        &format!("SELECT count() AS v FROM {clean_db}.trace_spans"),
    )
    .await;
    let two = scalar(
        &replay,
        &format!("SELECT count() AS v FROM {replay_db}.trace_spans"),
    )
    .await;
    assert_eq!(two, 2 * one, "the replayed corpus holds every row twice");

    let clean_engine = TraceEngine::new(
        ChClient::new(conn(clean_db)).await.expect("connect"),
        engine_config(100_000, 536_870_912),
    );
    let replay_engine = TraceEngine::new(
        ChClient::new(conn(replay_db)).await.expect("connect"),
        engine_config(100_000, 536_870_912),
    );
    let p = u_params(base);

    let mut wrong: Vec<String> = Vec::new();
    for (cell, pushes) in U_CELLS {
        let pushed_q = format!(r#"{{ resource.service.name = "svc" }} | by(name) | {cell}"#);
        let unpushed_q = format!(
            r#"{{ resource.service.name = "svc" }} && {{ resource.service.name = "svc" }} | by(name) | {cell}"#
        );
        let pushed = plan_for(&clean_engine, &pushed_q, &p);
        let unpushed = plan_for(&clean_engine, &unpushed_q, &p);
        if pushed.aggregate_pushed() != pushes {
            wrong.push(format!(
                "{pushed_q}: expected the grouped fragment to {}, and it {}",
                if pushes { "render" } else { "be refused" },
                match pushed.pushed_having() {
                    Some(f) => format!("rendered {f:?}"),
                    None => "was not".to_string(),
                }
            ));
        }
        assert!(!unpushed.aggregate_pushed(), "{unpushed_q}");

        let ids = |o: &pulsus_read::SearchOutput| -> Vec<String> {
            o.traces.iter().map(|t| hex32(&t.trace_id)).collect()
        };
        let got = ids(&clean_engine
            .search(&pushed)
            .await
            .unwrap_or_else(|e| panic!("{pushed_q}: {e:?}")));
        let want = ids(&clean_engine
            .search(&unpushed)
            .await
            .unwrap_or_else(|e| panic!("{unpushed_q}: {e:?}")));
        if got != want {
            wrong.push(format!(
                "{pushed_q}: pushed returned {got:?} and unpushed returned {want:?}"
            ));
        }

        // (i) the candidate rows CONTAIN the engine's answer.
        let candidates: BTreeSet<String> = generator_ids(&clean, &pushed.generator_sqls[0])
            .await
            .iter()
            .map(hex32)
            .collect();
        let missing: Vec<&String> = want.iter().filter(|id| !candidates.contains(*id)).collect();
        if !missing.is_empty() {
            wrong.push(format!(
                "{pushed_q}: the statement did not return {missing:?}, which the answer holds"
            ));
        }

        // (ii) criterion 6c: the byte-identical replay moves neither the
        // answer nor the candidate set.
        let replayed = plan_for(&replay_engine, &pushed_q, &p);
        let replayed_answer = ids(&replay_engine
            .search(&replayed)
            .await
            .unwrap_or_else(|e| panic!("{pushed_q} (replay): {e:?}")));
        if replayed_answer != got {
            wrong.push(format!(
                "{pushed_q}: the replay moved the answer, {got:?} -> {replayed_answer:?}"
            ));
        }
        let replay_candidates: BTreeSet<String> =
            generator_ids(&replay, &replayed.generator_sqls[0])
                .await
                .iter()
                .map(hex32)
                .collect();
        if replay_candidates != candidates {
            wrong.push(format!(
                "{pushed_q}: the replay moved the CANDIDATE set, {candidates:?} -> \
                 {replay_candidates:?}"
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} cases failed:\n{}",
        wrong.len(),
        U_CELLS.len(),
        wrong.join("\n")
    );

    // --- the flagship query, at the threshold that discriminates -----
    //
    // The loop above runs `count()` at thresholds 1 and 2, and on this
    // corpus every trace's biggest group holds at least two ROWS, so a
    // map that counted rows rather than distinct span ids would give the
    // same candidate set on both corpora and the loop could not see it.
    // At `> 2` it can: with `uniqExactMap` the clean and replayed
    // corpora both admit `…0001` alone, and with a row-counting map the
    // replay admits all five traces, because every row is written twice.
    //
    // This is `| by(name) | count() > 2` — the query part 5 exists for.
    {
        let q = r#"{ resource.service.name = "svc" } | by(name) | count() > 2"#;
        let clean_plan = plan_for(&clean_engine, q, &p);
        let replay_plan = plan_for(&replay_engine, q, &p);
        assert!(
            clean_plan.aggregate_pushed() && replay_plan.aggregate_pushed(),
            "{q}"
        );
        let clean_ids: Vec<String> = generator_ids(&clean, &clean_plan.generator_sqls[0])
            .await
            .iter()
            .map(hex32)
            .collect();
        let replay_ids: Vec<String> = generator_ids(&replay, &replay_plan.generator_sqls[0])
            .await
            .iter()
            .map(hex32)
            .collect();
        assert_eq!(
            clean_ids,
            vec![hex32(&trace_id(1))],
            "{q}: only …0001's biggest group holds three DISTINCT span ids"
        );
        assert_eq!(
            replay_ids, clean_ids,
            "{q}: the byte-identical replay must not move the candidate set — a map counting \
             ROWS rather than distinct span ids would admit all five traces here"
        );
    }

    // --- the superset, with a witness -------------------------------
    //
    // `…000b`'s two rows for one span id read 1s then 5s. The evaluator
    // keeps the FIRST, so its only group's maximum is 1s; the map
    // aggregates ROWS, so its maximum is 5s. Under `max(duration) >= 5s`
    // the statement admits the trace and phase 2 drops it — a superset,
    // and one that is not empty on this corpus.
    let eleven = hex32(&trace_id(11));
    let surviving = plan_for(
        &clean_engine,
        r#"{ resource.service.name = "svc" } | by(name) | max(duration) >= 5s"#,
        &p,
    );
    assert!(surviving.aggregate_pushed());
    let candidates: Vec<String> = generator_ids(&clean, &surviving.generator_sqls[0])
        .await
        .iter()
        .map(hex32)
        .collect();
    let answer: Vec<String> = clean_engine
        .search(&surviving)
        .await
        .expect("search")
        .traces
        .iter()
        .map(|t| hex32(&t.trace_id))
        .collect();
    assert!(
        candidates.contains(&eleven),
        "the map maximises over ROWS, so …000b's 5s second row admits it: {candidates:?}"
    );
    assert!(
        !answer.contains(&eleven),
        "the evaluator keeps the FIRST row per span id, so …000b's maximum is 1s: {answer:?}"
    );

    // --- and the loss the twelve refused cells are refused for -------
    //
    // `…0009` has two span ids and four rows with the NAMES SWAPPED. The
    // map assigns each repeated span id to BOTH keys before
    // deduplication, so every group reads 2; the evaluator deduplicates
    // first and sees two groups of one.
    //
    //   the engine   a:{S1}     b:{S2}      -> min 1  -> `count() < 2` RETURNS it
    //   the map      a:{S1,S2}  b:{S1,S2}   -> min 2  -> `count() < 2` LOSES it
    //
    // So `count() < 2` refuses. This runs the fragment it WOULD have
    // rendered — by string surgery on the production statement, so the
    // two differ in the `HAVING` and in nothing else — and shows the
    // loss. Without it the refusal is a rule nothing measures.
    let nine = hex32(&trace_id(9));
    let refused = plan_for(
        &clean_engine,
        r#"{ resource.service.name = "svc" } | by(name) | count() < 2"#,
        &p,
    );
    assert_eq!(
        refused.pushed_having(),
        None,
        "an anti-monotone grouped cell must send no fragment"
    );
    let answer: Vec<String> = clean_engine
        .search(&refused)
        .await
        .expect("search")
        .traces
        .iter()
        .map(|t| hex32(&t.trace_id))
        .collect();
    assert!(
        answer.contains(&nine),
        "…0009's deduplicated groups hold one span each: {answer:?}"
    );
    let would_be = refused.generator_sqls[0].replace(
        "\nGROUP BY trace_id\n",
        "\nGROUP BY trace_id\nHAVING arrayMin(mapValues(uniqExactMap(map(if(length(name) <= \
         8192, name, substringUTF8(name, 1, 2048)), span_id)))) < 2\n",
    );
    assert_ne!(
        would_be, refused.generator_sqls[0],
        "the control must differ"
    );
    let would_be_candidates: Vec<String> = generator_ids(&clean, &would_be)
        .await
        .iter()
        .map(hex32)
        .collect();
    assert!(
        !would_be_candidates.contains(&nine),
        "if `count() < 2` pushed, the statement would lose …0009 and phase 2 could not put it \
         back: {would_be_candidates:?}"
    );
    exec(&clean, &format!("DROP DATABASE IF EXISTS {clean_db}")).await;
    exec(&replay, &format!("DROP DATABASE IF EXISTS {replay_db}")).await;
}
