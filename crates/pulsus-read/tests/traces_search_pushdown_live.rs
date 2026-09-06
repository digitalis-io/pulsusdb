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
        Qualifier {
            q: r#"{ span.http.method = "GET" } | min(duration) >= 1s"#,
            fragment: "min(duration_ns) >= 1000000000",
            selects: |s| s.method_attr,
            admits: |spans| {
                !spans.is_empty()
                    && spans.iter().map(|s| s.duration_ns).min().unwrap_or(0) >= 1_000_000_000
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
    for q in [
        r#"{ span.http.method = "GET" } | max(duration) > 1s"#,
        r#"{ span.http.method = "GET" } | min(duration) >= 1s"#,
        r#"{ span.http.method = "GET" } | count() > 2"#,
        // **The `<` form is the one that can lose a trace, and it is why
        // this list is not three `>` queries.** A duplicated index row
        // can only INFLATE a count, so a pushed `count() > n` over the
        // replay admits too many traces and the evaluator — which
        // deduplicates by `span_id` — drops them again: the answer does
        // not move, and a corpus of `>` queries alone cannot tell a
        // correct pushdown from `count()`. With `<` the inflation drops
        // traces the database never returns, and nothing downstream can
        // put them back. Measured: with `count()` substituted for
        // `uniqExact(span_id)` this row returns 50 traces where the
        // clean corpus returns 100.
        r#"{ span.http.method = "GET" } | count() < 3"#,
    ] {
        let a = plan_for(&clean_engine, q, &p);
        let b = plan_for(&replay_engine, q, &p);
        assert!(a.aggregate_pushed() && b.aggregate_pushed(), "{q}");
        let ids = |o: pulsus_read::SearchOutput| -> Vec<String> {
            o.traces.iter().map(|t| hex32(&t.trace_id)).collect()
        };
        let want = ids(clean_engine.search(&a).await.expect("clean search"));
        let got = ids(replay_engine.search(&b).await.expect("replay search"));
        assert!(!want.is_empty(), "{q}: the clean corpus returns nothing");
        assert_eq!(got, want, "{q}: the replay moved the answer");
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

/// **The grouping state a pushed aggregate adds is bounded, and the bound
/// refuses.**
///
/// Ruling `5556416192` chose the ceiling the generator statement already
/// carries — `max_memory_usage` with `max_bytes_before_external_group_by
/// = 0` — over adding `max_rows_to_group_by`, because the latter's
/// refusal point moves with `max_threads` and the former's does not. That
/// choice made "already bounded" the load-bearing sentence, and prose has
/// no failure mode. This is the check.
///
/// Its break is one character in `exec.rs`: with
/// `max_bytes_before_external_group_by` non-zero the identical statement
/// spills and answers `200`, and this test then sees `Ok(SearchOutput)`.
/// The hermetic half of that break is
/// `traces::exec::tests::generator_settings_pin_the_memory_ceiling_and_throw_not_spill`.
#[tokio::test]
async fn the_pushed_aggregate_outgrows_the_generator_memory_ceiling_and_refuses() {
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
    match engine.search(&plan).await {
        Err(ReadError::QueryTooBroad(TooBroadReason::TraceGeneratorMemory { budget_bytes })) => {
            assert_eq!(budget_bytes, M1_CEILING_BYTES);
        }
        other => panic!(
            "expected the generator memory ceiling to refuse; got {:?}",
            other.map(|o| (o.traces.len(), o.partial))
        ),
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
/// pushed path's exact `Int64` reading of a span and the evaluator's
/// `f64` one can put that span on different sides of a threshold. Below
/// it both readings are the same integer.
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
        let pushes = threshold.parse::<i64>().expect("an integer literal") < (1i64 << 53);
        for op in B_OPS {
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
