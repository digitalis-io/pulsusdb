//! `cargo run -p xtask -- bench logql-metadata-filter` (issue #544) — the
//! measurement behind the structured-metadata label filter's figures.
//!
//! # What it measures, and why it is a script rather than a paragraph
//!
//! The filter's whole value is the statement count and the bytes on the
//! metered hop. Both are read out of `system.query_log`, which means a
//! figure can come from a **repeated** statement served partly from
//! ClickHouse's query-condition cache — on by default since 25.4 — and a
//! cached read can report 8,192 rows where a first execution reports
//! 3,000,000. A figure taken that way would be wrong by three orders of
//! magnitude and would look perfectly ordinary in a table.
//!
//! So every measured statement here is issued **once**, under a
//! `log_comment` no earlier statement used, **against a predicate literal
//! no earlier statement used**, with the condition cache dropped before
//! each route runs, and **the route's FIRST statement required to be
//! cold**: zero `ProfileEvents['QueryConditionCacheHits']` and at least
//! `ROWS` rows examined. A re-run fails that, because on a re-run the
//! first statement is the one that hits.
//!
//! **A row count cannot make that check, measured rather than argued.**
//! The earlier form required the route's summed `read_rows` to exceed a
//! million. Run twice on the same literal, once with the cache dropped and
//! once against the cache the first run left:
//!
//! ```text
//!                             read_rows       first statement    route
//!                             (route total)   hits / rows        hits / misses
//!   page loop, cache dropped   4,541,308,032    0 / 3,001,696    3,327 / 3,349
//!   page loop, cache warm      4,541,308,032   10 / 3,001,696    3,351 / 3,353
//!   one statement, cache warm          8,192   20 /     8,192       20 /     0
//! ```
//!
//! On the page loop the row count is **the same number** cold and warm, so
//! no threshold over it can tell the two apart; the hit counter moves from
//! 0 to 10 on the statement that decides. On the lowered route a repeat
//! collapses to 8,192 rows, which is where the 8,192 figure comes from.
//!
//! **And a zero-hit rule over the whole route would be wrong**, which the
//! same table shows: on a first execution with the cache dropped
//! immediately before it, the page loop still records 3,327 hits across
//! its 3,001 statements. Every one is on the cache the route's OWN earlier
//! statements filled — those statements share one predicate and differ
//! only by the keyset cursor. That is the route's behaviour under the
//! server defaults this script reports, not a stale figure, so the totals
//! below include it and the per-route counters are printed beside them.
//!
//! # The seven settings every figure carries
//!
//! Four are what our reader SENDS, so `system.query_log` records them per
//! statement and they are evidence: `max_query_size`, `max_bytes_to_read`,
//! `max_execution_time`, `max_memory_usage`. Three are server defaults in
//! force that the reader does not send, so the statement log does not
//! record them and they have to be read off `system.settings`:
//! `max_block_size`, `max_threads`, `use_query_condition_cache`. Printing
//! the second three beside the figures is what lets someone else
//! reproduce them.

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_logql::parse;
use pulsus_read::logql::{Direction, QueryParams, QuerySpec};
use pulsus_read::{EngineConfig, LogQlEngine};
use pulsus_schema::{RenderCtx, run_init};

use super::{BenchArgs, parse_http_url};

/// The corpus the design record's figures were taken on.
const ROWS: u64 = 3_000_000;
const FP: u64 = 18_374_000_000_000_000_777;
const SERVICE: &str = "checkout";

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedRow {
    service: String,
    fingerprint: u64,
    timestamp_ns: i64,
    severity: i8,
    body: String,
    structured_metadata: String,
}

/// One route's totals, summed over every statement it issued. **A
/// scenario-local struct**: the shared `QueryLogTotals` is byte-frozen by
/// the committed evidence JSONs and must not grow a field.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
struct RouteTotals {
    statements: u64,
    read_rows: u64,
    read_bytes: u64,
    result_bytes: u64,
    query_duration_ms: u64,
    /// `ProfileEvents['QueryConditionCacheHits']`, summed over the
    /// route's statements. **The cache check** — see the assertion below.
    cache_hits: u64,
    /// Its companion, printed so a zero-hit route is visibly a route that
    /// USED the cache and missed, not one the cache never saw.
    cache_misses: u64,
}

/// One server default in force, read off `system.settings`.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
struct ServerSetting {
    name: String,
    value: String,
    changed: u8,
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A 32-character lowercase-hexadecimal identifier, deterministic in `i`.
fn trace_id(i: u64) -> String {
    format!("{:016x}{:016x}", splitmix64(i), splitmix64(i ^ 0x5bf0_3635))
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
        query_timeout: Duration::from_secs(1800),
        ..ChConnConfig::default()
    };
    let admin = ChClient::new(admin_cfg.clone()).await?;
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {}", args.database),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await?;
    let schema = RenderCtx {
        db: args.database.clone(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
        metrics_landing_retention_hours: 6,
        metrics_dedup_window: 10_000,
    };
    run_init(&admin, &schema).await?;
    let mut data_cfg = admin_cfg.clone();
    data_cfg.database = args.database.clone();
    let client = ChClient::new(data_cfg.clone()).await?;

    let ts = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
    )? - (ROWS as i64) * 1_000_000;
    eprintln!("=== seeding {ROWS} rows on one stream ===");
    client
        .execute(
            &format!(
                "INSERT INTO {}.log_streams (month, fingerprint, service, labels, updated_ns) \
                 VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({ts}))), {FP}, \
                 '{SERVICE}', '{{\"service_name\":\"{SERVICE}\"}}', 0)",
                args.database
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await?;
    let mut written = 0u64;
    while written < ROWS {
        let batch: Vec<SeedRow> = (written..(written + 100_000).min(ROWS))
            .map(|i| SeedRow {
                service: SERVICE.to_string(),
                fingerprint: FP,
                timestamp_ns: ts + (i as i64) * 1_000_000,
                severity: 0,
                body: format!("GET /api/v1/orders/{i} status=200"),
                structured_metadata: format!(r#"{{"trace_id":"{}"}}"#, trace_id(i)),
            })
            .collect();
        written += batch.len() as u64;
        client.insert_block("log_samples", &batch).await?;
    }
    eprintln!("=== seeded {written} rows ===");

    // The three server defaults in force. Read once: they are properties
    // of the server, not of a statement, and `system.query_log` does not
    // record a setting the client did not send.
    let defaults = server_defaults(&admin).await?;
    println!("server defaults in force (read from system.settings):");
    for s in &defaults {
        println!("  {:<26} {:<12} changed={}", s.name, s.value, s.changed);
    }

    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts - 1_000_000_000,
            end_ns: ts + (ROWS as i64) * 1_000_000 + 1_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };

    // **The two routes run the SAME query**, and differ in one thing:
    // whether the fragment lowers. The budget knob decides that, which is
    // what makes this a comparison of routes rather than of queries — an
    // earlier version put a regular-expression operator on the second
    // route, so the two differed in the filter as well as in the route.
    //
    // **Each route gets its own predicate literal** — the row at index 7
    // and the row at index 11 — so no literal is issued twice and no
    // figure below can be a query-condition-cache hit.
    //
    // **And its own `log_comment`, which is SENT.** Attribution is by that
    // tag, not by matching the statement text: the fallback route's
    // statements carry no predicate literal at all, because the fragment
    // is exactly what they lack.
    let lowered_value = trace_id(7);
    let paged_value = trace_id(11);
    let run = trace_id(99);
    let routes = [
        (
            "one statement",
            format!(r#"{{service_name="{SERVICE}"}} | trace_id="{lowered_value}""#),
            format!("issue544-one-statement-{run}"),
            None,
        ),
        (
            "the page loop",
            format!(r#"{{service_name="{SERVICE}"}} | trace_id="{paged_value}""#),
            format!("issue544-page-loop-{run}"),
            Some(1usize),
        ),
    ];

    let mut measured: Vec<(&str, RouteTotals, SentSettings)> = Vec::new();
    for (name, query, tag, budget) in &routes {
        // Dropped before each route, so the cache-hit counter below reads
        // zero for a reason and not by luck of what ran before.
        admin
            .execute(
                "SYSTEM DROP QUERY CONDITION CACHE",
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await?;
        let engine = route_engine(&data_cfg, &args.database, tag, *budget).await?;
        let expr = parse(query)?;
        let started = now_micros(&admin).await?;
        let (_result, _warnings) = engine.query(&expr, &params).await?;
        super::query_log::flush_logs(&admin).await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        super::query_log::flush_logs(&admin).await?;
        // **Attributed by something TRANSMITTED** (code review round 1).
        // An earlier version printed a `log_comment` tag it never sent and
        // then attributed every sample statement since a timestamp, which
        // is not attribution at all — a second route running in the same
        // second would land in the first one's figures. The route's own
        // predicate literal IS in the statement the server received and
        // recorded, it is unique to the route, and no other statement can
        // carry it.
        let totals = route_totals(&admin, &args.database, started, tag).await?;
        let first = first_statement(&admin, &args.database, started, tag).await?;
        let sent = sent_settings(&admin, &args.database, started, tag).await?;
        println!("\n=== {name} — {query} ===");
        println!("  attributed by       log_comment = {tag}");
        println!("  statements          {}", totals.statements);
        println!("  read_rows           {}", totals.read_rows);
        println!("  read_bytes          {}", totals.read_bytes);
        println!("  result_bytes        {}", totals.result_bytes);
        println!("  query_duration_ms   {}", totals.query_duration_ms);
        println!(
            "  condition cache     {} hit(s), {} miss(es) over the route; its FIRST \
             statement {} hit(s), {} rows",
            totals.cache_hits, totals.cache_misses, first.cache_hits, first.read_rows
        );
        println!(
            "  settings THESE {} statements recorded (sent by the reader):",
            totals.statements
        );
        println!("    max_query_size      {}", sent.max_query_size());
        println!("    max_bytes_to_read   {}", sent.max_bytes_to_read());
        println!("    max_execution_time  {}", sent.max_execution_time());
        println!("    max_memory_usage    {}", sent.max_memory_usage());
        println!("  settings in force but NOT recorded (server defaults):");
        for s in &defaults {
            println!("    {:<18} {}", s.name, s.value);
        }
        anyhow::ensure!(
            totals.statements >= 1,
            "{name}: no statement carried log_comment = {tag} — the figures below would be \
             about nothing"
        );
        // **The cache check: the route's FIRST statement has to be cold**
        // (code review round 2). Two forms were measured and rejected
        // before this one, and both failures are worth keeping:
        //
        //   - the route's SUMMED `read_rows` over a threshold. Measured,
        //     the page loop reads 4,541,308,032 rows whether its cache was
        //     dropped first or left warm by an identical earlier run — the
        //     same number, because the read orders the window either way.
        //     No threshold over that number separates the two.
        //   - the route's SUMMED cache hits being zero. Measured, a page
        //     loop whose cache was dropped immediately before it still
        //     records 3,327 hits: its 3,001 statements share one predicate
        //     and differ only by the cursor, so each fills the cache the
        //     next one reads. A zero-sum rule forbids a first execution.
        //
        // What separates a first execution from a re-run is the FIRST
        // statement: cold, it misses and examines the whole window; on a
        // re-run it hits at once. The cache is dropped before each route,
        // so the only cache a first statement could hit is another run's.
        anyhow::ensure!(
            first.cache_hits == 0,
            "{name}: the route's FIRST statement records {} query-condition-cache hit(s), so \
             the figures are a re-run's, not a first execution's. The cache is dropped before \
             each route and each route uses a predicate literal no earlier statement used — \
             a hit here means neither held.",
            first.cache_hits
        );
        anyhow::ensure!(
            first.read_rows >= ROWS,
            "{name}: the route's FIRST statement examined {} rows, fewer than the {ROWS} \
             seeded — a cold first statement reads the whole window, so this is either a \
             cached read or a corpus that is not what this script assumes.",
            first.read_rows
        );
        measured.push((*name, totals, sent));
    }

    println!("\n=== the comparison ===");
    println!("| route | statements | result_bytes | read_rows | read_bytes |");
    println!("|---|---|---|---|---|");
    for (name, t, _) in &measured {
        println!(
            "| {name} | {} | {} | {} | {} |",
            t.statements, t.result_bytes, t.read_rows, t.read_bytes
        );
    }
    Ok(())
}

async fn now_micros(admin: &ChClient) -> anyhow::Result<u64> {
    #[derive(Row, serde::Serialize, serde::Deserialize)]
    struct N {
        n: u64,
    }
    let mut stream = admin
        .query_stream::<N>(
            "SELECT toUInt64(toUnixTimestamp64Micro(now64(6))) AS n",
            &QuerySettings::new(),
        )
        .await?;
    Ok(stream.next().await.transpose()?.map(|r| r.n).unwrap_or(0))
}

/// Every sample statement one request issued, summed — the ones the
/// server recorded under that route's own `log_comment`, and no others.
///
/// The tag is compared against the recorded `log_comment` COLUMN, not
/// searched for in the statement text, so this counting statement cannot
/// match itself however it spells the tag.
async fn route_totals(
    admin: &ChClient,
    db: &str,
    since_us: u64,
    tag: &str,
) -> anyhow::Result<RouteTotals> {
    let sql = format!(
        "SELECT count() AS statements, sum(read_rows) AS read_rows, sum(read_bytes) AS \
         read_bytes, sum(result_bytes) AS result_bytes, sum(query_duration_ms) AS \
         query_duration_ms, \
         sum(ProfileEvents['QueryConditionCacheHits']) AS cache_hits, \
         sum(ProfileEvents['QueryConditionCacheMisses']) AS cache_misses \
         FROM system.query_log \
         WHERE type = 'QueryFinish' AND query_kind = 'Select' \
           AND toUInt64(toUnixTimestamp64Micro(event_time_microseconds)) >= {since_us} \
           AND has(tables, '{db}.log_samples') \
           AND NOT has(tables, 'system.query_log') \
           AND log_comment = '{tag}'"
    );
    let mut stream = admin
        .query_stream::<RouteTotals>(&sql, &QuerySettings::new())
        .await?;
    Ok(stream.next().await.transpose()?.unwrap_or_default())
}

/// The route's FIRST statement in event order — the one that decides
/// whether the figures are a first execution's or a re-run's.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
struct FirstStatement {
    cache_hits: u64,
    read_rows: u64,
}

async fn first_statement(
    admin: &ChClient,
    db: &str,
    since_us: u64,
    tag: &str,
) -> anyhow::Result<FirstStatement> {
    let sql = format!(
        "SELECT ProfileEvents['QueryConditionCacheHits'] AS cache_hits, read_rows \
         FROM system.query_log \
         WHERE type = 'QueryFinish' AND query_kind = 'Select' \
           AND toUInt64(toUnixTimestamp64Micro(event_time_microseconds)) >= {since_us} \
           AND has(tables, '{db}.log_samples') \
           AND NOT has(tables, 'system.query_log') \
           AND log_comment = '{tag}' \
         ORDER BY query_start_time_microseconds ASC, event_time_microseconds ASC \
         LIMIT 1"
    );
    let mut stream = admin
        .query_stream::<FirstStatement>(&sql, &QuerySettings::new())
        .await?;
    Ok(stream.next().await.transpose()?.unwrap_or_default())
}

/// The four settings the reader sent, over the statements THIS route
/// issued.
///
/// **Reported as a distinct count with its range, not as one row's
/// value.** A route that issues one statement has one value per setting; a
/// page loop has 3,001, and `max_bytes_to_read` is DIFFERENT on each
/// because it carries the budget the pages before it have not spent. An
/// earlier version read the newest row and printed its value as though it
/// were the route's, which is a figure that is true of one statement and
/// false of the other three thousand.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
struct SentSettings {
    qs_n: u64,
    qs_lo: String,
    qs_hi: String,
    br_n: u64,
    br_lo: String,
    br_hi: String,
    et_n: u64,
    et_lo: String,
    et_hi: String,
    mm_n: u64,
    mm_lo: String,
    mm_hi: String,
}

impl SentSettings {
    fn max_query_size(&self) -> SettingSpread {
        SettingSpread::new(self.qs_n, &self.qs_lo, &self.qs_hi)
    }
    fn max_bytes_to_read(&self) -> SettingSpread {
        SettingSpread::new(self.br_n, &self.br_lo, &self.br_hi)
    }
    fn max_execution_time(&self) -> SettingSpread {
        SettingSpread::new(self.et_n, &self.et_lo, &self.et_hi)
    }
    fn max_memory_usage(&self) -> SettingSpread {
        SettingSpread::new(self.mm_n, &self.mm_lo, &self.mm_hi)
    }
}

/// One setting over a route's statements: how many distinct values, and
/// the lowest and highest.
struct SettingSpread {
    distinct: u64,
    low: String,
    high: String,
}

impl SettingSpread {
    fn new(distinct: u64, low: &str, high: &str) -> Self {
        Self {
            distinct,
            low: low.to_string(),
            high: high.to_string(),
        }
    }
}

impl std::fmt::Display for SettingSpread {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.distinct {
            0 => f.write_str("(no statement)"),
            1 => f.write_str(&self.low),
            n => write!(f, "{n} distinct, {} … {}", self.low, self.high),
        }
    }
}

async fn sent_settings(
    admin: &ChClient,
    db: &str,
    since_us: u64,
    tag: &str,
) -> anyhow::Result<SentSettings> {
    let spread = |name: &str, short: &str| {
        format!(
            "uniqExact(Settings['{name}']) AS {short}_n, min(Settings['{name}']) AS {short}_lo, \
             max(Settings['{name}']) AS {short}_hi"
        )
    };
    let sql = format!(
        "SELECT {}, {}, {}, {} FROM system.query_log \
         WHERE type = 'QueryFinish' AND query_kind = 'Select' \
           AND toUInt64(toUnixTimestamp64Micro(event_time_microseconds)) >= {since_us} \
           AND has(tables, '{db}.log_samples') \
           AND NOT has(tables, 'system.query_log') \
           AND log_comment = '{tag}'",
        spread("max_query_size", "qs"),
        spread("max_bytes_to_read", "br"),
        spread("max_execution_time", "et"),
        spread("max_memory_usage", "mm"),
    );
    let mut stream = admin
        .query_stream::<SentSettings>(&sql, &QuerySettings::new())
        .await?;
    Ok(stream.next().await.transpose()?.unwrap_or_default())
}

/// The three server defaults in force that no statement records.
async fn server_defaults(admin: &ChClient) -> anyhow::Result<Vec<ServerSetting>> {
    let sql = "SELECT name, value, changed FROM system.settings \
               WHERE name IN ('max_block_size', 'max_threads', 'use_query_condition_cache') \
               ORDER BY name";
    let mut stream = admin
        .query_stream::<ServerSetting>(sql, &QuerySettings::new())
        .await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?);
    }
    Ok(out)
}

/// One engine per route: its own `log_comment`, and — for the fallback
/// route — a metadata fragment budget of one byte, which is what puts the
/// same query on today's route.
async fn route_engine(
    data_cfg: &ChConnConfig,
    db: &str,
    tag: &str,
    budget: Option<usize>,
) -> anyhow::Result<LogQlEngine> {
    let engine = LogQlEngine::new(
        ChClient::new(data_cfg.clone()).await?,
        EngineConfig {
            read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
            db: db.to_string(),
            streams_idx: "log_streams_idx".to_string(),
            streams: "log_streams".to_string(),
            samples: "log_samples".to_string(),
            rollup_table: "log_metrics_5s".to_string(),
            patterns_table: "log_patterns".to_string(),
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 500 * 1024 * 1024 * 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
            distributed: false,
        },
    )
    .with_query_log_comment(tag);
    Ok(match budget {
        Some(bytes) => engine.with_metadata_fragment_budget(bytes),
        None => engine,
    })
}
