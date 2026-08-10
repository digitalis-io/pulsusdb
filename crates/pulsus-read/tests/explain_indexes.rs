//! `EXPLAIN indexes = 1` snapshot assertions against a live ClickHouse
//! (docs/schemas.md §9's regression harness: "a query silently losing its
//! primary-index prefix or skip-index usage fails the build"). Gated
//! behind `PULSUS_TEST_CLICKHOUSE=1`, reusing the #5 harness verbatim
//! (`crates/pulsus-schema/tests/live_schema.rs`'s connection/setup
//! pattern) — the CI `schema-it` job runs this after the live schema
//! tests, against the same ClickHouse 24.8 container.
//!
//! **Coverage (fix-plan amendment §4, code review FAIL):** every canonical
//! query shape from `tests/sql_snapshots.rs`'s matrix gets its own
//! `EXPLAIN indexes = 1` case here — stage 1 (single-eq / multi-eq / regex
//! / mixed positive+negative), stage 2 hydration, every stage-3 line-filter
//! op, and metric reads (rollup-served and the raw fallback, range and
//! instant). Direction/limit variants are deliberately **not** duplicated
//! here: they affect `ORDER BY`/`Sorting`, not index selection, and are
//! already exercised as pure SQL-generation snapshots in
//! `sql_snapshots.rs` — this file's job is index *usage*, not SQL text.
//!
//! **Assertion strength (round-2 review disposition):** raw `EXPLAIN` text
//! embeds volatile `Parts:`/`Granules:` counts (vary with data volume/
//! merges) and, since fixture timestamps must be wall-clock-recent (see
//! `now_ns()` below), literal nanosecond values that differ every run.
//! [`index_usage`] reduces the raw text to its stable, index-relevant lines
//! (block titles, `Keys:` + key names, `Condition:`, skip-index `Name:`)
//! and [`normalize_numbers`] collapses every digit run to `#`, producing a
//! deterministic `Vec<String>`. **Every case below `assert_eq!`s the
//! *complete* extract** against a captured expectation — not a
//! property-subset helper (a prior revision used `block_columns`/
//! `skip_index_names` picks, which can miss a real regression: a skip
//! index still listed but with `Condition: true` pruning nothing, a block
//! silently appearing/vanishing, or block order changing). A full-extract
//! `assert_eq!` catches all of those; docs/schemas.md §9's "snapshot-
//! tested" names the equality-comparison mechanism, not "capture raw
//! EXPLAIN text" (which would be non-deterministic here and fail on
//! non-regressions — see the architect's round-2 disposition on issue #11).
//!
//! Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test explain_indexes
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_logql::parse;
use pulsus_read::logql::sql::{self, TimeWindow};
use pulsus_read::logql::{Direction, Plan, PlanCtx, QueryParams, QuerySpec, plan};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

fn test_ctx(db: &str) -> SchemaParams {
    RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    }
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/explain_indexes.rs for setup)"
            );
            return;
        }
    };
}

async fn drop_database(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainRow {
    explain: String,
}

async fn explain_raw(client: &ChClient, sql: &str) -> String {
    // The `clickhouse` crate's own query builder treats a bare `?` in SQL
    // text as an unbound bind-argument placeholder; a regex matcher's own
    // `(?:...)` anchoring syntax (`escape::ch_regex_anchored`) always
    // contains one. Double it here exactly as `LogQlEngine::query_stream`
    // does internally — this test file calls `ChClient` directly, bypassing
    // that wrapper, so it must apply the same fix.
    let full = format!("EXPLAIN indexes = 1 {sql}").replace('?', "??");
    let mut stream = client
        .query_stream::<ExplainRow>(&full, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("explain query failed: {e}\nSQL:\n{full}"));
    let mut out = String::new();
    while let Some(row) = stream.next().await {
        out.push_str(&row.expect("decode explain row").explain);
        out.push('\n');
    }
    out
}

/// Collapses every run of ASCII digits in `s` to a single `#`, so a
/// deterministic-but-dynamic value (a fixture's wall-clock nanosecond
/// timestamp, a literal fingerprint) doesn't defeat an `assert_eq!`
/// snapshot.
fn normalize_numbers(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() {
            out.push('#');
            while matches!(chars.peek(), Some(d) if d.is_ascii_digit()) {
                chars.next();
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// ClickHouse's `EXPLAIN indexes = 1` block titles this crate's tables ever
/// produce (`MinMax`/`Partition`/`PrimaryKey` for `ORDER BY`/`PARTITION BY`
/// analysis, `Skip` per `tokenbf_v1`/`ngrambf_v1`/`minmax` secondary
/// index) — kept as an explicit allow-list so [`index_usage`]'s extract is
/// self-describing (which *kind* of index, not just position-in-list).
const INDEX_BLOCK_TITLES: &[&str] = &["MinMax", "Partition", "PrimaryKey", "Skip"];

/// Reduces raw `EXPLAIN indexes = 1` text to its stable, index-relevant
/// lines: block titles (`PrimaryKey`/`Skip`/...), `Keys:` plus the
/// key-name lines under it, `Condition:`, and skip-index `Name:` lines.
/// Drops everything else (`Parts:`/`Granules:` row/mark counts — the
/// volatile detail docs/schemas.md §9 doesn't care about; what it cares
/// about is which columns and which skip indexes are in play) and
/// collapses digit runs via [`normalize_numbers`] so a fixture's
/// wall-clock-dependent timestamp literals don't defeat `assert_eq!`.
fn index_usage(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut capturing_keys = false;
    for line in raw.lines() {
        let trimmed = line.trim();
        if INDEX_BLOCK_TITLES.contains(&trimmed) {
            out.push(trimmed.to_string());
            capturing_keys = false;
            continue;
        }
        if trimmed == "Keys:" {
            out.push(trimmed.to_string());
            capturing_keys = true;
            continue;
        }
        if capturing_keys {
            // Bare key-name lines carry no `:`; the first line that does
            // (or a blank line) ends the `Keys:` block.
            if !trimmed.is_empty() && !trimmed.contains(':') {
                out.push(normalize_numbers(trimmed));
                continue;
            }
            capturing_keys = false;
        }
        if trimmed.starts_with("Condition:") || trimmed.starts_with("Name:") {
            out.push(normalize_numbers(trimmed));
        }
    }
    out
}

async fn explain(client: &ChClient, sql: &str) -> Vec<String> {
    index_usage(&explain_raw(client, sql).await)
}

/// The FIRST `Parts: m/n` count in raw `EXPLAIN indexes = 1` text —
/// deliberately not part of [`index_usage`]'s extract, which drops these
/// counts as volatile (issue #11's round-2 disposition). Issue #399 needs
/// them for exactly one claim the extract cannot make: that a `Condition:`
/// on `bucket_ns` actually *prunes* parts rather than merely appearing.
/// The first block ClickHouse prints for these queries is `MinMax`, whose
/// `Parts:` line is the partition-level prune the assertion is about.
fn parts_selected(raw: &str) -> Option<(u64, u64)> {
    for line in raw.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("Parts: ")
            && let Some((m, n)) = rest.split_once('/')
        {
            return Some((m.trim().parse().ok()?, n.trim().parse().ok()?));
        }
    }
    None
}

/// The rollup resolution `test_ctx` renders (`log_rollup: 5s`).
const ROLLUP_RES_NS: u64 = 5_000_000_000;

fn plan_ctx(db: &str) -> PlanCtx<'_> {
    PlanCtx {
        db,
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    }
}

// One fixture stream, `service_name="checkout", env="prod"`, plus two
// `log_samples` rows — enough for every canonical shape's EXPLAIN to run
// genuine primary-key/skip-index analysis (ClickHouse's index-usage
// analysis is query/schema-driven, not row-content-driven: it needs *some*
// data in the queried partition/time-range so the optimizer doesn't
// short-circuit to a `NullSource`, not a literal match on the query's
// specific predicate values).
const FP_PROD: u64 = 18_374_000_000_000_000_001;

/// Nanoseconds since the Unix epoch, right now. Fixture timestamps must be
/// wall-clock-recent (not a fixed historical constant): `log_samples`'s
/// `ttl_only_drop_parts = 1` retention (docs/schemas.md §3.1) makes an
/// already-expired part eligible for background deletion almost
/// immediately, which would flake a fixed-date fixture the same way
/// `live_schema.rs`'s smoke insert documents (issue #5).
fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

async fn seed(client: &ChClient, db: &str, ts_ns: i64) {
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) VALUES \
                 (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({ts_ns}))), {FP_PROD}, 'checkout', \
                 '{{\"env\":\"prod\",\"service_name\":\"checkout\"}}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");

    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) VALUES \
                 ('checkout', {FP_PROD}, {ts_ns}, 9, 'connection refused'), \
                 ('checkout', {FP_PROD}, {ts_plus}, 0, 'request completed')",
                ts_plus = ts_ns + 1_000_000_000
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_samples");
}

/// Sets up a fresh database, seeds fixture data around `ts_ns`, and returns
/// a client bound directly to that database.
async fn setup(db: &str, ts_ns: i64) -> ChClient {
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let data_client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");
    seed(&data_client, db, ts_ns).await;
    data_client
}

/// A `[now - 6h, now]` window bracketing `ts_ns` (the seeded samples'
/// timestamp), matching docs/schemas.md §3.2's canonical "last 6h" example
/// shape.
fn range_params(ts_ns: i64) -> QueryParams {
    QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts_ns - 6 * 3_600_000_000_000,
            end_ns: ts_ns + 3_600_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    }
}

fn streams_plan(query: &str, params: &QueryParams, db: &str) -> pulsus_read::logql::StreamsPlan {
    let expr = parse(query).expect("parse");
    match plan(&expr, params, &plan_ctx(db)).expect("plan") {
        Plan::Streams(sp) => sp,
        Plan::Metric(_) | Plan::MetricBinary(_) => panic!("expected a Streams plan"),
    }
}

fn metric_plan(query: &str, params: &QueryParams, db: &str) -> pulsus_read::logql::MetricPlan {
    let expr = parse(query).expect("parse");
    match plan(&expr, params, &plan_ctx(db)).expect("plan") {
        Plan::Metric(mp) => mp,
        Plan::Streams(_) | Plan::MetricBinary(_) => panic!("expected a Metric plan"),
    }
}

/// Qualifies a bare (unqualified) table name in `sql` with `db.` — plan-
/// generated SQL targets the connection's default database, but these
/// tests connect via a shared-port client without a fixed default.
fn qualify(sql: &str, table: &str, db: &str) -> String {
    sql.replacen(table, &format!("{db}.{table}"), 1)
}

// ---------------------------------------------------------------------
// Stage 1 — matcher normalization shapes.
// ---------------------------------------------------------------------

/// Builds a `Vec<String>` expectation literal concisely for the
/// `assert_eq!`s below.
fn v(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

async fn stage1_usage(db: &str, ts_ns: i64, client: &ChClient, query: &str) -> Vec<String> {
    let sp = streams_plan(query, &range_params(ts_ns), db);
    let sql = qualify(&sp.stage1_sql, "log_streams_idx", db);
    explain(client, &sql).await
}

#[tokio::test]
async fn stage1_single_equality_uses_the_key_val_primary_key() {
    skip_unless_live!();
    let db = "pulsus_read_it_s1_single";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = stage1_usage(db, ts_ns, &client, r#"{service_name="checkout"}"#).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "Partition",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "PrimaryKey",
            "Keys:",
            "key",
            "val",
            "Condition: and((val in ['checkout', 'checkout']), (key in ['service_name', 'service_name']))",
        ])
    );
}

#[tokio::test]
async fn stage1_multi_equality_uses_the_key_val_primary_key() {
    skip_unless_live!();
    let db = "pulsus_read_it_s1_multi";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = stage1_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout", env="prod"}"#,
    )
    .await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "Partition",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "PrimaryKey",
            "Keys:",
            "key",
            "val",
            "Condition: or(and((val in ['prod', 'prod']), (key in ['env', 'env'])), and((val in ['checkout', 'checkout']), (key in ['service_name', 'service_name'])))",
        ])
    );
}

#[tokio::test]
async fn stage1_regex_matcher_uses_the_key_primary_key_prefix() {
    skip_unless_live!();
    let db = "pulsus_read_it_s1_regex";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    // `match(val, ...)` isn't sargable — ClickHouse's key-condition
    // analyzer can only narrow the primary-key range on the plain
    // equality (`key = 'env'`); `val`'s regex condition still applies as
    // a residual filter, just not via primary-key pruning. This is
    // exactly docs/schemas.md §3.2's "regex matchers evaluated within one
    // key's index prefix — a scan over the distinct values of that key".
    let usage = stage1_usage(db, ts_ns, &client, r#"{env=~"prod|staging"}"#).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "Partition",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "PrimaryKey",
            "Keys:",
            "key",
            "Condition: (key in ['env', 'env'])",
        ])
    );
}

#[tokio::test]
async fn stage1_mixed_positive_and_negative_matchers_uses_the_key_val_primary_key() {
    skip_unless_live!();
    let db = "pulsus_read_it_s1_mixed";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = stage1_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout", team!="qa"}"#,
    )
    .await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "Partition",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "PrimaryKey",
            "Keys:",
            "key",
            "val",
            "Condition: or(and((val in ['qa', 'qa']), (key in ['team', 'team'])), and((val in ['checkout', 'checkout']), (key in ['service_name', 'service_name'])))",
        ])
    );
}

// ---------------------------------------------------------------------
// Stage 2 — hydration.
// ---------------------------------------------------------------------

#[tokio::test]
async fn stage2_hydration_uses_the_fingerprint_primary_key() {
    skip_unless_live!();
    let db = "pulsus_read_it_s2";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let table = format!("{db}.log_streams");
    let sql = sql::stage2(&table, &[FP_PROD]);

    let usage = explain(&client, &sql).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Condition: true",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "fingerprint",
            "Condition: (fingerprint in #-element set)",
        ])
    );
}

// ---------------------------------------------------------------------
// Stage 3 — samples, every line-filter op. All four line-filter ops below
// produce the byte-identical index-usage extract: the `service`/
// `fingerprint`/`timestamp_ns` primary-key `Condition:` only reflects
// those three columns (not `body`, which isn't part of the primary key),
// and both `body` skip indexes are always listed as considered whenever
// any predicate references `body` — the ops differ in generated SQL
// (`sql_snapshots.rs`'s job) but not in which indexes ClickHouse consults.
// ---------------------------------------------------------------------

async fn stage3_usage(db: &str, ts_ns: i64, client: &ChClient, query: &str) -> Vec<String> {
    let sp = streams_plan(query, &range_params(ts_ns), db);
    let table = format!("{db}.log_samples");
    let sql = sql::stage3(
        &table,
        &["'checkout'".to_string()],
        &[FP_PROD],
        TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        },
        &sp.line_filters,
        sp.direction,
        sp.scan_limit,
    );
    explain(client, &sql).await
}

/// The `(service, fingerprint, timestamp_ns)` primary key + both `body`
/// skip indexes — the shared expectation every stage-3 line-filter case
/// below asserts (see the section comment for why they coincide).
fn expected_stage3_line_filter_usage() -> Vec<String> {
    v(&[
        "MinMax",
        "Keys:",
        "timestamp_ns",
        "Condition: and((timestamp_ns in (-Inf, #]), (timestamp_ns in [#, +Inf)))",
        "Partition",
        "Condition: true",
        "PrimaryKey",
        "Keys:",
        "service",
        "fingerprint",
        "timestamp_ns",
        "Condition: and(and((timestamp_ns in (-Inf, #]), and((timestamp_ns in [#, +Inf)), (fingerprint in #-element set))), (service in ['checkout', 'checkout']))",
        "Skip",
        "Name: idx_body_tokens",
        "Skip",
        "Name: idx_body_ngrams",
    ])
}

#[tokio::test]
async fn stage3_contains_line_filter_uses_the_primary_key_and_the_token_skip_index() {
    skip_unless_live!();
    let db = "pulsus_read_it_s3_contains";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = stage3_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout"} |= "connection refused""#,
    )
    .await;
    assert_eq!(usage, expected_stage3_line_filter_usage());
}

#[tokio::test]
async fn stage3_not_contains_line_filter_uses_the_primary_key_and_the_token_skip_index() {
    skip_unless_live!();
    let db = "pulsus_read_it_s3_not_contains";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = stage3_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout"} != "connection refused""#,
    )
    .await;
    assert_eq!(usage, expected_stage3_line_filter_usage());
}

#[tokio::test]
async fn stage3_regex_line_filter_over_a_plain_literal_uses_the_token_skip_index() {
    skip_unless_live!();
    let db = "pulsus_read_it_s3_regex";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = stage3_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout"} |~ "connection refused""#,
    )
    .await;
    assert_eq!(usage, expected_stage3_line_filter_usage());
}

#[tokio::test]
async fn stage3_not_regex_line_filter_over_a_metacharacter_pattern_still_lists_the_body_skip_indexes()
 {
    skip_unless_live!();
    let db = "pulsus_read_it_s3_not_regex";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    // Not a plain literal (`.*` is a regex metacharacter sequence) — no
    // `hasToken` prefilter is generated (`plan::is_plain_literal`), only
    // `match(body, ...)`. ClickHouse's `EXPLAIN indexes = 1` still lists
    // both `body` skip indexes as *considered* (any predicate referencing
    // `body` surfaces every skip index declared on that column) — the
    // `Parts:`/`Granules:` counts this file deliberately drops are what
    // would show whether either one actually pruned anything.
    let usage = stage3_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout"} !~ "err.*""#,
    )
    .await;
    assert_eq!(usage, expected_stage3_line_filter_usage());
}

/// Issue M6-09 AC4 (Tier-1, the named perf gate): a line filter followed
/// by parser/label-filter stages keeps the stage-3 `EXPLAIN indexes = 1`
/// extract EXACTLY equal to the plain line-filter expectation — the
/// `json`/`status` stages are pure post-fetch and add nothing to the SQL,
/// so the `tokenbf_v1` skip index stays engaged for `|= "refused"`.
#[tokio::test]
async fn stage3_line_filter_before_a_parser_keeps_the_exact_skip_index_usage() {
    skip_unless_live!();
    let db = "pulsus_read_it_s3_parser_pushdown";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = stage3_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout"} |= "connection refused" | json | status = "500""#,
    )
    .await;
    assert_eq!(usage, expected_stage3_line_filter_usage());
}

// ---------------------------------------------------------------------
// Issue #90 — the fetch-until-limit keyset PAGE (a later `After` page)
// must keep the primary index engaged in BOTH directions. The composite
// tuple comparison alone does not prune granules; the redundant
// `timestamp_ns` bound (`>= ts` Forward / `<= ts` Backward) is what keeps
// `PrimaryKey` on `timestamp_ns` in play — proving no per-page full scan.
// ---------------------------------------------------------------------

async fn keyset_page_usage(
    db: &str,
    ts_ns: i64,
    client: &ChClient,
    direction: Direction,
) -> Vec<String> {
    let table = format!("{db}.log_samples");
    let sql = sql::stage3_keyset(
        &table,
        &["'checkout'".to_string()],
        &[FP_PROD],
        TimeWindow {
            start_ns: ts_ns - 6 * 3_600_000_000_000,
            end_ns: ts_ns + 3_600_000_000_000,
        },
        sql::KeysetLower::After {
            tuple: (ts_ns, FP_PROD, 42),
            offset: 1,
        },
        direction,
        &[],
        500,
    );
    explain(client, &sql).await
}

#[tokio::test]
async fn keyset_forward_page_keeps_the_primary_key_engaged_via_the_redundant_time_bound() {
    skip_unless_live!();
    let db = "pulsus_read_it_keyset_fwd";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = keyset_page_usage(db, ts_ns, &client, Direction::Forward).await;
    // The PrimaryKey block must be present with `timestamp_ns` among its
    // keys and a `Condition:` that references it (granule pruning), not a
    // `Condition: true` full scan.
    assert!(
        usage.iter().any(|l| l == "PrimaryKey"),
        "forward keyset page must engage the PrimaryKey: {usage:?}"
    );
    let pk_pos = usage.iter().position(|l| l == "PrimaryKey").unwrap();
    assert!(
        usage[pk_pos..].iter().any(|l| l == "timestamp_ns"),
        "timestamp_ns must be a PrimaryKey column: {usage:?}"
    );
    assert!(
        usage[pk_pos..]
            .iter()
            .any(|l| l.starts_with("Condition:") && l.contains("timestamp_ns")),
        "the PrimaryKey Condition must prune on timestamp_ns (redundant bound): {usage:?}"
    );
}

#[tokio::test]
async fn keyset_backward_page_keeps_the_primary_key_engaged_via_the_redundant_time_bound() {
    skip_unless_live!();
    let db = "pulsus_read_it_keyset_bwd";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = keyset_page_usage(db, ts_ns, &client, Direction::Backward).await;
    assert!(
        usage.iter().any(|l| l == "PrimaryKey"),
        "backward keyset page must engage the PrimaryKey: {usage:?}"
    );
    let pk_pos = usage.iter().position(|l| l == "PrimaryKey").unwrap();
    assert!(
        usage[pk_pos..].iter().any(|l| l == "timestamp_ns"),
        "timestamp_ns must be a PrimaryKey column: {usage:?}"
    );
    assert!(
        usage[pk_pos..]
            .iter()
            .any(|l| l.starts_with("Condition:") && l.contains("timestamp_ns")),
        "the PrimaryKey Condition must prune on timestamp_ns (redundant bound): {usage:?}"
    );
}

// ---------------------------------------------------------------------
// Metric reads — rollup-served vs raw fallback, range vs instant.
// ---------------------------------------------------------------------

/// Issue #227 Tier-1 gate: a RANGE metric read slides raw off `log_samples`
/// (the rollup fast-path is retired for range) and its PK-ordered sliding
/// scan (`metric_raw_samples_sliding`) engages the `(service, fingerprint,
/// timestamp_ns)` primary key — the same prune as every raw `log_samples`
/// read (the `ORDER BY` change to `optimize_read_in_order` shape does not
/// alter the index prune). Never a full scan (the query-performance mandate).
#[tokio::test]
async fn metric_range_slides_raw_and_prunes_on_the_service_fingerprint_timestamp_primary_key() {
    skip_unless_live!();
    let db = "pulsus_read_it_metric_range_sliding";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let mp = metric_plan(r#"rate({env="prod"}[5m])"#, &range_params(ts_ns), db);
    assert!(
        !mp.rollup,
        "issue #227: a range query slides raw, never rollup"
    );
    assert!(mp.client.is_some());
    assert_eq!(mp.table, "log_samples");
    let table = format!("{db}.log_samples");
    let sql = sql::metric_raw_samples_sliding(
        &table,
        &["'checkout'".to_string()],
        &[FP_PROD],
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
    );
    assert!(
        sql.contains("ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC"),
        "PK read order (optimize_read_in_order), no body/global-ts sort: {sql}"
    );
    let usage = explain(&client, &sql).await;
    assert_eq!(usage, expected_metric_instant_raw_usage());
}

/// Issue #169 Tier-1 gate: the `/volume` rollup aggregation carries the
/// identical `(fingerprint IN, bucket_ns > s AND <= e)` predicate family
/// as the rollup metric reads, so its `EXPLAIN indexes = 1` extract must
/// equal [`expected_metric_rollup_usage`] in full — MinMax prune on
/// `bucket_ns` plus the `(fingerprint, bucket_ns)` primary key with both
/// predicates in its `Condition:` — and reference no `service`/body
/// column anywhere (primary-key pruning, never a full scan; the
/// query-performance mandate).
#[tokio::test]
async fn volume_rollup_read_uses_the_fingerprint_bucket_primary_key() {
    skip_unless_live!();
    let db = "pulsus_read_it_volume_rollup";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let table = format!("{db}.log_metrics_5s");
    let sql = sql::log_volume_rollup(
        &table,
        &[FP_PROD],
        TimeWindow {
            start_ns: ts_ns - 6 * 3_600_000_000_000,
            end_ns: ts_ns + 3_600_000_000_000,
        },
    );

    let usage = explain(&client, &sql).await;
    assert_eq!(usage, expected_metric_rollup_usage());
    assert!(!usage.iter().any(|l| l.contains("service")));
}

/// Issue #170 Tier-1 gate: the `/detected_labels` aggregation is ONE
/// `log_streams_idx` scan with the month partition pruned (MinMax +
/// Partition on `month` — the same scan class as the shipped `/labels`
/// discovery query) and never references `log_samples`/body anywhere.
/// The `(key, val, fingerprint)` primary key legitimately reports
/// `Condition: true` — the aggregation groups over every key, so the
/// pruning story is the partition, not the PK prefix.
///
/// `/detected_fields` deliberately has NO case here: it adds no new SQL
/// shape — its fast path is the byte-identical `sql::stage3` builder and
/// its paged path is `sql::stage3_keyset`, both already full-extract-
/// gated above (`stage3_*`/`keyset_*` cases); `sql_snapshots.rs` pins the
/// text and `logs_detected_live.rs` adds the endpoint-level pushdown
/// evidence.
#[tokio::test]
async fn detected_labels_aggregation_prunes_on_the_month_partition() {
    skip_unless_live!();
    let db = "pulsus_read_it_detected_labels";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let params = range_params(ts_ns);
    let (start_ns, end_ns) = match params.spec {
        QuerySpec::Range {
            start_ns, end_ns, ..
        } => (start_ns, end_ns),
        QuerySpec::Instant { .. } => unreachable!("range_params builds a Range spec"),
    };
    let months = pulsus_read::logql::plan::months_overlapping(start_ns, end_ns);
    let table = format!("{db}.log_streams_idx");
    let sql = sql::detected_labels(
        &table,
        &months,
        None,
        &format!("{db}.log_metrics_5s"),
        TimeWindow { start_ns, end_ns },
        ROLLUP_RES_NS,
    );
    assert!(!sql.contains("log_samples"), "never touches log_samples");

    let usage = explain(&client, &sql).await;
    assert_eq!(
        usage,
        // The outer `log_streams_idx` scan is unchanged by issue #399 —
        // same `month` MinMax/Partition pruning — and additionally
        // engages the index primary key's TRAILING column, replacing the
        // pre-#399 `PrimaryKey / Condition: true`: the activity semi-join
        // materializes a `fingerprint` set before the outer scan reads.
        //
        // `EXPLAIN indexes = 1` reports index analysis for the OUTER
        // table only — the subquery is executed to build the set, not
        // planned inline — so the rollup's own `bucket_ns` blocks are
        // absent here (measured, ClickHouse 24.8.14.39) and are pinned
        // separately, on the subquery's standalone form, by
        // `detected_labels_activity_subquery_prunes_the_rollup_by_bucket_range`.
        v(&[
            "MinMax",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "Partition",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "PrimaryKey",
            "Keys:",
            "fingerprint",
            "Condition: (fingerprint in #-element set)",
        ])
    );
}

/// Issue #399 AC5 — the activity semi-join's own scan prunes the rollup
/// on `bucket_ns`, which is the partition key's only input column
/// (`PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))`), so a
/// window narrower than the corpus reads strictly fewer parts. The
/// `Parts: m/n` check is the one that carries the claim: `index_usage`
/// deliberately drops those counts, so a `Condition:` that pruned nothing
/// would still match the extract.
#[tokio::test]
async fn detected_labels_activity_subquery_prunes_the_rollup_by_bucket_range() {
    skip_unless_live!();
    let db = "pulsus_read_it_activity_subquery";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    // Four distinct DAY partitions of rollup rows, one bucket each. The
    // window below covers only the newest, so at least three parts must
    // be pruned away.
    const DAY_NS: i64 = 86_400_000_000_000;
    for day in 1..=4i64 {
        let bucket = (ts_ns - day * DAY_NS) / ROLLUP_RES_NS as i64 * ROLLUP_RES_NS as i64;
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_metrics_5s (fingerprint, bucket_ns, count, bytes) \
                     VALUES ({FP_PROD}, {bucket}, 1, 10)"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed rollup day partition");
    }

    let table = format!("{db}.log_metrics_5s");
    let window = TimeWindow {
        start_ns: ts_ns - DAY_NS - 3_600_000_000_000,
        end_ns: ts_ns - DAY_NS + 3_600_000_000_000,
    };

    let unscoped = sql::active_fingerprints(&table, None, window, ROLLUP_RES_NS);
    let raw = explain_raw(&client, &unscoped).await;
    assert_eq!(
        index_usage(&raw),
        v(&[
            "MinMax",
            "Keys:",
            "bucket_ns",
            "Condition: and((bucket_ns in (-Inf, #]), (bucket_ns in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "bucket_ns",
            "Condition: and((bucket_ns in (-Inf, #]), (bucket_ns in [#, +Inf)))",
        ])
    );
    let (selected, total) = parts_selected(&raw).expect("a MinMax Parts: m/n line");
    assert!(
        total >= 4,
        "the fixture must offer at least four day partitions to prune from, saw {total}\n{raw}"
    );
    assert!(
        selected < total,
        "the bucket range must prune parts: {selected}/{total}\n{raw}"
    );

    // Scoped: the caller's fingerprint list is pushed inside, so the
    // primary key's LEADING column joins the condition too.
    let scoped = sql::active_fingerprints(&table, Some(&[FP_PROD]), window, ROLLUP_RES_NS);
    assert_eq!(
        explain(&client, &scoped).await,
        v(&[
            "MinMax",
            "Keys:",
            "bucket_ns",
            "Condition: and((bucket_ns in (-Inf, #]), (bucket_ns in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "fingerprint",
            "bucket_ns",
            "Condition: and((bucket_ns in (-Inf, #]), and((bucket_ns in [#, +Inf)), (fingerprint in #-element set)))",
        ])
    );
}

/// Issue #406 Part A — the two statements `/series` with no `match[]`
/// renders. The branch introduces NO new SQL shape: statement one is
/// `sql::active_fingerprints(rollup, None, …)`, already production SQL for
/// `/labels`, `/label/{name}/values` and `/detected_labels`, and statement
/// two is the byte-pinned `sql::stage2`. What this case adds over the
/// pieces' existing coverage is the PAIRING — the unmatched path engages
/// the rollup's `bucket_ns` MinMax/PrimaryKey and then `log_streams`'
/// `fingerprint` primary key, with no `log_streams_idx` stage 1 between
/// them and no `log_samples` read at all.
///
/// The `Parts: m/n` check on the first statement is what carries the
/// pruning claim: `index_usage` drops those counts, so a `Condition:` that
/// pruned nothing would still match the extract.
#[tokio::test]
async fn series_without_a_selector_prunes_the_rollup_and_hits_the_streams_primary_key() {
    skip_unless_live!();
    let db = "pulsus_read_it_series_all";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    // Four distinct DAY partitions of rollup rows; the window covers one.
    const DAY_NS: i64 = 86_400_000_000_000;
    for day in 1..=4i64 {
        let bucket = (ts_ns - day * DAY_NS) / ROLLUP_RES_NS as i64 * ROLLUP_RES_NS as i64;
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_metrics_5s (fingerprint, bucket_ns, count, bytes) \
                     VALUES ({FP_PROD}, {bucket}, 1, 10)"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed rollup day partition");
    }

    let rollup = format!("{db}.log_metrics_5s");
    let window = TimeWindow {
        start_ns: ts_ns - DAY_NS - 3_600_000_000_000,
        end_ns: ts_ns - DAY_NS + 3_600_000_000_000,
    };

    // Statement 1 — the UNSCOPED activity scan (`fingerprints: None`),
    // exactly what `all_active_fingerprints` dispatches.
    let activity = sql::active_fingerprints(&rollup, None, window, ROLLUP_RES_NS);
    let raw = explain_raw(&client, &activity).await;
    assert_eq!(
        index_usage(&raw),
        v(&[
            "MinMax",
            "Keys:",
            "bucket_ns",
            "Condition: and((bucket_ns in (-Inf, #]), (bucket_ns in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "bucket_ns",
            "Condition: and((bucket_ns in (-Inf, #]), (bucket_ns in [#, +Inf)))",
        ])
    );
    let (selected, total) = parts_selected(&raw).expect("a MinMax Parts: m/n line");
    assert!(
        total >= 4,
        "the fixture must offer at least four day partitions to prune from, saw {total}\n{raw}"
    );
    assert!(
        selected < total,
        "the unmatched /series activity scan must prune parts: {selected}/{total}\n{raw}"
    );

    // Statement 2 — hydration over whatever statement 1 returned, on
    // `log_streams`' `ORDER BY fingerprint` primary key.
    let streams = format!("{db}.log_streams");
    assert_eq!(
        explain(&client, &sql::stage2(&streams, &[FP_PROD])).await,
        v(&[
            "MinMax",
            "Condition: true",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "fingerprint",
            "Condition: (fingerprint in #-element set)",
        ])
    );
}

/// Issue #399 AC15 — `/labels` and `/label/{name}/values` keep their
/// month partition pruning and gain the same activity semi-join.
#[tokio::test]
async fn label_discovery_scans_prune_on_the_month_partition_and_the_activity_bucket_range() {
    skip_unless_live!();
    let db = "pulsus_read_it_label_discovery_window";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let params = range_params(ts_ns);
    let (start_ns, end_ns) = match params.spec {
        QuerySpec::Range {
            start_ns, end_ns, ..
        } => (start_ns, end_ns),
        QuerySpec::Instant { .. } => unreachable!("range_params builds a Range spec"),
    };
    let months = pulsus_read::logql::plan::months_overlapping(start_ns, end_ns);
    let idx = format!("{db}.log_streams_idx");
    let rollup = format!("{db}.log_metrics_5s");
    let window = TimeWindow { start_ns, end_ns };

    // `EXPLAIN indexes = 1` reports the OUTER table's analysis only (see
    // `detected_labels_aggregation_prunes_on_the_month_partition`), so
    // both extracts are month blocks plus that builder's own primary-key
    // condition. The subquery's rollup pruning is pinned on its
    // standalone form by
    // `detected_labels_activity_subquery_prunes_the_rollup_by_bucket_range`.
    let month_blocks = [
        "MinMax",
        "Keys:",
        "month",
        "Condition: (month in [#, #])",
        "Partition",
        "Keys:",
        "month",
        "Condition: (month in [#, #])",
    ];

    let names_sql = sql::label_names(&idx, &months, &rollup, window, ROLLUP_RES_NS);
    let mut expected: Vec<&str> = month_blocks.to_vec();
    expected.extend([
        "PrimaryKey",
        "Keys:",
        "fingerprint",
        "Condition: (fingerprint in #-element set)",
    ]);
    assert_eq!(explain(&client, &names_sql).await, v(&expected));

    let values_sql = sql::label_values(&idx, &months, "'env'", &rollup, window, ROLLUP_RES_NS);
    let mut expected: Vec<&str> = month_blocks.to_vec();
    expected.extend([
        "PrimaryKey",
        "Keys:",
        "key",
        "fingerprint",
        // Operand order is ClickHouse's, MEASURED (24.8.14.39), not the
        // order the predicate is written in: `label_values` renders `key
        // = 'env'` before the `fingerprint IN`, and the analyser reports
        // the reverse. The issue #399 plan predicted the written order;
        // the plan was not wrong about WHICH conditions engage the
        // primary key, only about the order the extract prints them in.
        "Condition: and((fingerprint in #-element set), (key in ['env', 'env']))",
    ]);
    assert_eq!(explain(&client, &values_sql).await, v(&expected));

    // What embedding the semi-join must NOT cost: the outer scan's month
    // partition pruning. `index_usage` drops `Parts:` counts, so assert
    // it on the raw text.
    let raw = explain_raw(&client, &names_sql).await;
    let (selected, total) = parts_selected(&raw).expect("a MinMax Parts: m/n line");
    assert!(
        selected <= total,
        "sanity: parts selected must not exceed parts total ({selected}/{total})\n{raw}"
    );
}

/// The `(fingerprint, bucket_ns)` primary key on `log_metrics_5s` — shared
/// by the range and instant rollup cases, which differ only in `SELECT`/
/// `GROUP BY` shape (`sql_snapshots.rs`'s job), not in the `WHERE`
/// predicates over `fingerprint`/`bucket_ns` that drive index usage.
fn expected_metric_rollup_usage() -> Vec<String> {
    v(&[
        "MinMax",
        "Keys:",
        "bucket_ns",
        "Condition: and((bucket_ns in (-Inf, #]), (bucket_ns in [#, +Inf)))",
        "Partition",
        "Condition: true",
        "PrimaryKey",
        "Keys:",
        "fingerprint",
        "bucket_ns",
        "Condition: and((bucket_ns in (-Inf, #]), and((bucket_ns in [#, +Inf)), (fingerprint in #-element set)))",
    ])
}

/// Renamed from `metric_rollup_instant_read_uses_the_fingerprint_bucket_primary_key`
/// (issue #12 behaviour change from #11): an instant metric query has no
/// step to test against the rollup resolution (an unaligned `[at-range,
/// at]` window would silently diverge from raw at bucket edges —
/// task-manager resolution #1 on issue #12), so it now always routes raw.
#[tokio::test]
async fn metric_instant_read_routes_to_raw_and_uses_the_service_fingerprint_timestamp_primary_key()
{
    skip_unless_live!();
    let db = "pulsus_read_it_metric_instant_raw";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let params = QueryParams {
        spec: QuerySpec::Instant { at_ns: ts_ns },
        limit: 100,
        direction: Direction::Backward,
    };
    let mp = metric_plan(r#"rate({env="prod"}[5m])"#, &params, db);
    assert!(!mp.rollup);
    assert_eq!(
        mp.routing.reason, "raw: instant query",
        "instant queries must name the routing reason"
    );
    assert!(mp.step_ns.is_none());
    let table = format!("{db}.log_samples");
    let sql = sql::metric_instant(
        sql::MetricSource {
            table: &table,
            bucket_col: mp.bucket_col,
            agg_expr: mp.agg_expr,
        },
        &["'checkout'".to_string()],
        &[FP_PROD],
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
    );

    let usage = explain(&client, &sql).await;
    assert_eq!(usage, expected_metric_instant_raw_usage());
}

/// The `(service, fingerprint, timestamp_ns)` primary key on `log_samples`
/// — the same key condition [`expected_stage3_line_filter_usage`] asserts
/// (a `body` predicate never factors into `PrimaryKey`'s `Condition:`, only
/// into whether the `Skip` blocks are listed at all), minus the two `Skip`
/// entries: an instant metric read carries no line filter, so it never
/// references `body` and neither skip index is ever considered.
fn expected_metric_instant_raw_usage() -> Vec<String> {
    v(&[
        "MinMax",
        "Keys:",
        "timestamp_ns",
        "Condition: and((timestamp_ns in (-Inf, #]), (timestamp_ns in [#, +Inf)))",
        "Partition",
        "Condition: true",
        "PrimaryKey",
        "Keys:",
        "service",
        "fingerprint",
        "timestamp_ns",
        "Condition: and(and((timestamp_ns in (-Inf, #]), and((timestamp_ns in [#, +Inf)), (fingerprint in #-element set))), (service in ['checkout', 'checkout']))",
    ])
}

// ---------------------------------------------------------------------
// PromQL metric reads (issue #83, M6-08a) — the @-fixed and the
// subquery-widened fetch windows must keep the `(metric_name,
// fingerprint, unix_milli)` primary index on `metric_samples`: both plan
// to exactly one bounded `sample_fetch` whose `EXPLAIN indexes = 1`
// extract matches the plain raw-fetch expectation (no index loss from
// the fixed/widened bounds).
// ---------------------------------------------------------------------

const MFP: u64 = 18_374_000_000_000_000_002;

async fn seed_metric_samples(client: &ChClient, db: &str, now_ms: i64) {
    // A few samples in the last minute — enough for genuine index
    // analysis (recent so `ttl_only_drop_parts` retention can't race it,
    // the same rule as `now_ns()`'s doc).
    let values: Vec<String> = (0..6)
        .map(|k| format!("('mq', {MFP}, {}, {k}.0)", now_ms - k * 10_000))
        .collect();
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_samples (metric_name, fingerprint, unix_milli, value) \
                 VALUES {}",
                values.join(", ")
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed metric_samples");
}

/// Plans `query` (single-selector by construction), computes its fetch
/// window, and renders the real `sample_fetch` SQL against
/// `{db}.metric_samples` — the same builder `MetricsEngine` executes.
fn promql_sample_fetch_sql(query: &str, params: pulsus_promql::PlanParams, db: &str) -> String {
    let expr = pulsus_promql::parse(query).expect("parse");
    let plan = pulsus_promql::plan(&expr, params).expect("plan");
    assert_eq!(
        plan.selectors.len(),
        1,
        "{query}: one bounded sample fetch, never per-inner-step fetches"
    );
    let (lower_excl, upper_incl) = plan.selectors[0].fetch_window(&params);
    let table = format!("{db}.metric_samples");
    pulsus_read::metrics::sample_sql::sample_fetch(
        &table,
        plan.selectors[0]
            .metric_name
            .as_deref()
            .expect("these cases use concrete-name selectors"),
        &[MFP],
        lower_excl,
        upper_incl,
    )
}

/// The `(metric_name, fingerprint, unix_milli)` primary key on
/// `metric_samples` — the shared raw-fetch expectation both PromQL cases
/// below assert against: the full three-column key condition plus MinMax
/// time pruning (the `toDate(...)` partition analysis reports
/// `Condition: true` here — time-range partition pruning surfaces through
/// the MinMax block instead).
fn expected_metric_samples_fetch_usage() -> Vec<String> {
    v(&[
        "MinMax",
        "Keys:",
        "unix_milli",
        "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
        "Partition",
        "Condition: true",
        "PrimaryKey",
        "Keys:",
        "metric_name",
        "fingerprint",
        "unix_milli",
        "Condition: and(and((fingerprint in #-element set), and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))), (metric_name in ['mq', 'mq']))",
    ])
}

fn promql_params(start_ms: i64, end_ms: i64, step_ms: i64) -> pulsus_promql::PlanParams {
    pulsus_promql::PlanParams {
        start_ms,
        end_ms,
        step_ms,
        lookback_ms: pulsus_promql::DEFAULT_LOOKBACK_MS,
        experimental_functions: false,
    }
}

#[tokio::test]
async fn promql_at_fixed_metric_read_stays_on_the_metric_samples_primary_key() {
    skip_unless_live!();
    let db = "pulsus_read_it_promql_at";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_samples(&client, db, now_ms).await;

    // `@` fixed at (roughly) now; the fetch window is invariant across
    // eval spans (the hermetic plan.rs gate) — asserted here against two
    // spans before the live EXPLAIN, tying AC4 to AC3.
    let at_s = now_ms / 1000;
    let query = format!("mq @ {at_s}");
    let span_a = promql_params(now_ms, now_ms, 0);
    let span_b = promql_params(now_ms - 86_400_000, now_ms, 60_000);
    let sql_a = promql_sample_fetch_sql(&query, span_a, db);
    let sql_b = promql_sample_fetch_sql(&query, span_b, db);
    assert_eq!(
        sql_a, sql_b,
        "@-fixed fetch SQL must not track the eval span"
    );

    let usage = explain(&client, &sql_a).await;
    assert_eq!(usage, expected_metric_samples_fetch_usage());
}

#[tokio::test]
async fn promql_subquery_widened_metric_read_stays_on_the_metric_samples_primary_key() {
    skip_unless_live!();
    let db = "pulsus_read_it_promql_subq";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_samples(&client, db, now_ms).await;

    // One widened window for the whole inner grid — exactly one fetch,
    // lower bound widened by exactly the subquery range vs the bare
    // selector's.
    let params = promql_params(now_ms, now_ms, 0);
    let subq_sql = promql_sample_fetch_sql("max_over_time(mq[1h:5m])", params, db);
    let bare_expr = pulsus_promql::parse("mq").expect("parse");
    let bare_plan = pulsus_promql::plan(&bare_expr, params).expect("plan");
    let (bare_lower, bare_upper) = bare_plan.selectors[0].fetch_window(&params);
    assert!(subq_sql.contains(&format!("unix_milli > {}", bare_lower - 3_600_000)));
    assert!(subq_sql.contains(&format!("unix_milli <= {bare_upper}")));

    let usage = explain(&client, &subq_sql).await;
    assert_eq!(usage, expected_metric_samples_fetch_usage());
}

/// Issue #85 (M6-08c) — the name-less/regex-`__name__` fan-out gate: the
/// flat `sample_fetch_multi` SQL (one query, `PREWHERE metric_name IN
/// (…)` + `fingerprint IN (…)`) must engage BOTH components of the
/// `(metric_name, fingerprint, unix_milli)` primary key in the live
/// ClickHouse plan (round-4 adjudication item 2) — the `IN`-set prune is
/// what makes the fan-out bounded instead of a name-less full scan.
/// Concrete-name selectors' plan stays byte-identical to the existing
/// single-eq expectation (no regression from adding the multi shape).
#[tokio::test]
async fn promql_multi_metric_fanout_prunes_on_both_metric_name_and_fingerprint_keys() {
    skip_unless_live!();
    let db = "pulsus_read_it_promql_multi";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    // Series across TWO metric names (`mq` via the shared seeder, `mq2`
    // here) — the fan-out shape is only meaningful over >= 2 metrics.
    seed_metric_samples(&client, db, now_ms).await;
    const MFP2: u64 = 18_374_000_000_000_000_003;
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_samples (metric_name, fingerprint, unix_milli, value) \
                 VALUES ('mq2', {MFP2}, {now_ms}, 1.0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed mq2 samples");

    // The real fetch SQL a name-less selector's resolved (name, fp) set
    // produces — the same builder `MetricsEngine::plan_multi_metric_fetch`
    // renders.
    let params = promql_params(now_ms, now_ms, 0);
    let expr = pulsus_promql::parse(r#"{__name__=~"mq.*"}"#).expect("parse");
    let plan = pulsus_promql::plan(&expr, params).expect("plan");
    assert_eq!(plan.selectors[0].metric_name, None, "name-less selector");
    let (lower_excl, upper_incl) = plan.selectors[0].fetch_window(&params);
    let table = format!("{db}.metric_samples");
    let sql = pulsus_read::metrics::sample_sql::sample_fetch_multi(
        &table,
        &["mq".to_string(), "mq2".to_string()],
        &[MFP, MFP2],
        lower_excl,
        upper_incl,
    );

    let usage = explain(&client, &sql).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "metric_name",
            "fingerprint",
            "unix_milli",
            "Condition: and(and((fingerprint in #-element set), and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))), (metric_name in #-element set))",
        ]),
        "both the metric_name IN and fingerprint IN components must engage the primary key"
    );

    // Control: a concrete-name selector's plan is unchanged by the multi
    // shape's existence — the exact pre-#85 extract.
    let single_sql = promql_sample_fetch_sql("mq", params, db);
    let single_usage = explain(&client, &single_sql).await;
    assert_eq!(single_usage, expected_metric_samples_fetch_usage());
}

/// Issue #82 (retroactive re-review, Finding 1) — the Tier-1 "bounded
/// info() fetch" gate: (a) `info(mq)`'s synthetic `target_info` selector
/// PK-prunes on `metric_name` in the live plan exactly like any other
/// concrete-name `metric_samples` fetch (no new/looser SQL shape — see
/// `expected_metric_samples_fetch_usage`, same shape, `target_info`
/// literal); (b) the degraded-path series-RESOLUTION probe
/// (`info_series_cardinality_probe`, `metrics/sql.rs`) both carries a
/// `LIMIT cap+1` in its rendered SQL text AND still PK-prunes on
/// `metric_series`'s leading `metric_name` component in the live plan —
/// the resolution stage is bounded BEFORE the sample fetch, not a
/// looser scan.
#[tokio::test]
async fn info_selector_fetch_prunes_on_metric_name_and_its_resolution_probe_is_limit_bounded() {
    skip_unless_live!();
    let db = "pulsus_read_it_promql_info";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_samples(&client, db, now_ms).await;

    const INFO_FP: u64 = 18_374_000_000_000_000_006;
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_samples (metric_name, fingerprint, unix_milli, value) \
                 VALUES ('target_info', {INFO_FP}, {now_ms}, 1.0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed target_info sample");
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_series (metric_name, fingerprint, unix_milli, labels) \
                 VALUES ('target_info', {INFO_FP}, {now_ms}, '{{\"instance\":\"a\",\"job\":\"1\"}}')"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed target_info series");

    // (a) The planned info() selector: `metric_name = Some("target_info")`
    // (AC2's PK-pruned single-metric fast path), `info_family = true`.
    let params = pulsus_promql::PlanParams {
        experimental_functions: true,
        ..promql_params(now_ms, now_ms, 0)
    };
    let expr = pulsus_promql::parse("info(mq)").expect("parse");
    let plan = pulsus_promql::plan(&expr, params).expect("plan");
    assert_eq!(plan.selectors.len(), 2);
    let info_sel = &plan.selectors[1];
    assert_eq!(info_sel.metric_name.as_deref(), Some("target_info"));
    assert!(
        info_sel.info_family,
        "the synthetic selector must be marked info_family"
    );

    let (lower_excl, upper_incl) = info_sel.fetch_window(&params);
    let samples_table = format!("{db}.metric_samples");
    let fetch_sql = pulsus_read::metrics::sample_sql::sample_fetch(
        &samples_table,
        "target_info",
        &[INFO_FP],
        lower_excl,
        upper_incl,
    );
    let usage = explain(&client, &fetch_sql).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "metric_name",
            "fingerprint",
            "unix_milli",
            "Condition: and(and((fingerprint in #-element set), and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))), (metric_name in ['target_info', 'target_info']))",
        ]),
        "the info() sample fetch must PK-prune on metric_name exactly like any concrete-name fetch"
    );

    // (b) The degraded-path resolution probe: a `LIMIT cap+1` bound in
    // the rendered SQL text (the cardinality cap, checked BEFORE the
    // sample fetch above ever runs), applied over a `SELECT DISTINCT
    // fingerprint` (the #82 code-review over-count fix — the cap counts
    // distinct SERIES, never per-activity-bucket `metric_series` rows),
    // and the probe query itself still PK-prunes on `metric_series`'s
    // leading `metric_name` component.
    let series_table = format!("{db}.metric_series");
    let window = pulsus_read::metrics::DataWindow {
        start_ms: now_ms - 3_600_000,
        end_ms: now_ms,
    };
    let series_sql = pulsus_read::metrics::sql::historical_series_subquery(
        &series_table,
        "target_info",
        window,
        1,
        &[],
    );
    let cap = 100_000u64;
    let probe_sql = pulsus_read::metrics::sql::info_series_cardinality_probe(&series_sql, cap);
    assert!(
        probe_sql.starts_with("SELECT DISTINCT fingerprint"),
        "the probe must count DISTINCT series, not activity-bucket rows: {probe_sql}"
    );
    assert!(
        probe_sql.ends_with("LIMIT 100001"),
        "the probe must bound resolution at cap+1: {probe_sql}"
    );

    let probe_usage = explain(&client, &probe_sql).await;
    assert_eq!(
        probe_usage,
        v(&[
            "MinMax",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "metric_name",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), and((unix_milli in [#, +Inf)), (metric_name in ['target_info', 'target_info'])))",
        ]),
        "the LIMIT-bounded resolution probe must still PK-prune on metric_name"
    );
}

// A pair of `metric_series` rows across TWO metric names sharing one
// activity bucket — the discovery-side fan-out shape is only meaningful
// over >= 2 metric names, and the flat `IN`×`IN` prune needs genuine data
// in the queried partition/time-range so the optimizer keeps a real
// `ReadFromMergeTree` (not a short-circuited `NullSource`).
const SFP1: u64 = 18_374_000_000_000_000_004;
const SFP2: u64 = 18_374_000_000_000_000_005;

async fn seed_metric_series(client: &ChClient, db: &str, now_ms: i64) {
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_series (metric_name, fingerprint, unix_milli, labels) \
                 VALUES ('sv', {SFP1}, {now_ms}, '{{\"job\":\"api\"}}'), \
                        ('sv2', {SFP2}, {now_ms}, '{{\"job\":\"api\"}}')"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed metric_series");
}

/// Issue #89 (discovery-path selector parity) — the regex/negated-
/// `__name__` discovery fan-out gate: the flat `discovery_fetch_multi` SQL
/// (one query, `metric_name IN (…)` + `fingerprint IN (…)`) must engage
/// BOTH components of the `(metric_name, fingerprint, unix_milli)` primary
/// key on `metric_series` in the live ClickHouse plan — the same Tier-1
/// evidence class as #85's `sample_fetch_multi` gate on `metric_samples`,
/// carried onto the discovery table the `/series`+`/labels` name-matcher
/// selector resolves against. The `IN`-set prune is what keeps the
/// cache-resolved fan-out bounded instead of a name-less full scan.
#[tokio::test]
async fn discovery_multi_metric_fanout_prunes_on_both_metric_name_and_fingerprint_keys() {
    skip_unless_live!();
    let db = "pulsus_read_it_discovery_multi";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_series(&client, db, now_ms).await;

    // The real fetch SQL a name-matcher discovery filter's resolved
    // (name, fp) set produces — the same builder
    // `MetricsEngine::discovery_multi_sql` renders. `bucket_ms = 1` floors
    // to the exact bounds (the flooring itself is unit-tested in `sql.rs`),
    // so the seeded now-stamped rows stay inside the queried window and the
    // primary-key analysis runs against a populated part.
    let window = pulsus_read::metrics::DataWindow {
        start_ms: now_ms - 3_600_000,
        end_ms: now_ms,
    };
    let table = format!("{db}.metric_series");
    let sql = pulsus_read::metrics::sql::discovery_fetch_multi(
        &table,
        &["sv".to_string(), "sv2".to_string()],
        &[SFP1, SFP2],
        window,
        1,
    );

    let usage = explain(&client, &sql).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "metric_name",
            "fingerprint",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), and((unix_milli in [#, +Inf)), and((fingerprint in #-element set), (metric_name in #-element set))))",
        ]),
        "both the metric_name IN and fingerprint IN components must engage the metric_series primary key"
    );
}

/// Issue #96 (degraded-cache discovery fallback) — the probe-derived
/// **fetch** gate: `discovery_fetch_by_names` (`metric_name IN (…)` + the
/// `unix_milli` window, label matchers in SQL, NO `fingerprint IN`) must
/// engage the leading `metric_name` component of the `(metric_name,
/// fingerprint, unix_milli)` primary key on `metric_series` in the live
/// plan — the same Tier-1 evidence class as #89's `discovery_fetch_multi`
/// gate above, minus the fingerprint component (the degraded route resolves
/// NAMES only; the label matchers narrow within each pruned metric). This
/// is what keeps the degraded fallback's dominant-cost stage PK-pruned, not
/// a name-less full scan. The PROBE itself is deliberately NOT gated here
/// (a regex `metric_name` predicate can't range-prune the leading PK
/// column — its bound, not index engagement, is the perf gate; see
/// `live_discovery_fallback.rs`).
#[tokio::test]
async fn discovery_fetch_by_names_prunes_on_the_metric_name_primary_key_component() {
    skip_unless_live!();
    let db = "pulsus_read_it_discovery_by_names";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_series(&client, db, now_ms).await;

    let window = pulsus_read::metrics::DataWindow {
        start_ms: now_ms - 3_600_000,
        end_ms: now_ms,
    };
    let table = format!("{db}.metric_series");
    // The exact fetch a degraded-cache name-matcher discovery filter's
    // probe produces (`MetricsEngine::discovery_series` wave 2): the probed
    // names IN-set, with a label matcher applied in SQL. `bucket_ms = 1`
    // floors to the exact bounds so the seeded rows stay in-window.
    let sql = pulsus_read::metrics::sql::discovery_fetch_by_names(
        &table,
        &["sv".to_string(), "sv2".to_string()],
        &[pulsus_read::metrics::LabelMatcher {
            key: "job".to_string(),
            op: pulsus_read::metrics::MatchOp::Eq,
            value: "api".to_string(),
        }],
        window,
        1,
    );

    let usage = explain(&client, &sql).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "metric_name",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), and((unix_milli in [#, +Inf)), (metric_name in #-element set)))",
        ]),
        "the metric_name IN component must engage the metric_series primary key"
    );
}

/// Issue #315 — the compile probe must be **free**. The probe is a
/// constant `match()` over an empty subject, spliced into the bucket-floored
/// lower bound (`metrics::sql::re2_compile_probe`) so ClickHouse folds it
/// during query analysis. This gate is the evidence that the folding
/// happens *before* index analysis: the `metric_series` fallback subquery
/// with a regex matcher must engage exactly the same MinMax/Partition/
/// PrimaryKey conditions as the same subquery with an `Eq` matcher, digits
/// normalised — a fold that arrived too late would drop `unix_milli` from
/// the primary-key condition and turn the fallback into a wider scan.
///
/// A standalone `AND <constant>` conjunct was rejected for the same reason
/// in reverse: it preserves these conditions but stops the matcher
/// predicate from moving *fully* into PREWHERE, leaving a `Filter` step
/// that re-evaluates `JSONExtractString` + `match` on every surviving row
/// (measured with `EXPLAIN actions=1` on 24.8.14.39).
#[tokio::test]
async fn the_re2_compile_probe_costs_the_metric_series_fallback_no_index_engagement() {
    skip_unless_live!();
    let db = "pulsus_read_it_metrics_compile_probe";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_series(&client, db, now_ms).await;

    let window = pulsus_read::metrics::DataWindow {
        start_ms: now_ms - 3_600_000,
        end_ms: now_ms,
    };
    let table = format!("{db}.metric_series");
    let matcher = |op| pulsus_read::metrics::LabelMatcher {
        key: "job".to_string(),
        op,
        value: "api".to_string(),
    };
    let subquery = |op| {
        pulsus_read::metrics::sql::historical_series_subquery(
            &table,
            "sv",
            window,
            1,
            &[matcher(op)],
        )
    };

    let with_probe = subquery(pulsus_read::metrics::MatchOp::Re);
    let without_probe = subquery(pulsus_read::metrics::MatchOp::Eq);
    // Premise: the two SQL texts genuinely differ, and only the regex one
    // carries a probe — otherwise this compares a query with itself.
    assert!(
        with_probe.contains("+ 0 * (match('', "),
        "the regex subquery must carry the probe: {with_probe}"
    );
    assert!(
        !without_probe.contains("match("),
        "the Eq subquery must carry no regex at all: {without_probe}"
    );

    assert_eq!(
        explain(&client, &with_probe).await,
        explain(&client, &without_probe).await,
        "the compile probe changed the metric_series index analysis"
    );

    // And the absolute shape, so "identical" cannot mean "both degraded".
    assert_eq!(
        explain(&client, &with_probe).await,
        v(&[
            "MinMax",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "metric_name",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), and((unix_milli in [#, +Inf)), (metric_name in ['sv', 'sv'])))",
        ]),
        "the bucket-floored window must still prune on unix_milli and metric_name"
    );
}

// ---------------------------------------------------------------------
// Issue M6-10 (AC3, the launch's named rollup-vs-raw gate): an un-piped
// `count_over_time` stays rollup-served (`log_metrics_<res>`); an
// unwrapped `sum_over_time` is client-aggregated and reads `log_samples`
// raw — two distinct table targets, both index-served.
// ---------------------------------------------------------------------

/// Issue #227: an un-piped range `count_over_time` slides raw (the rollup
/// fast-path is retired for range reads) and prunes on the `log_samples`
/// primary key.
#[tokio::test]
async fn m6_10_unpiped_count_over_time_range_slides_raw() {
    skip_unless_live!();
    let db = "pulsus_read_it_m610_range_raw";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let mp = metric_plan(
        r#"count_over_time({env="prod"}[5m])"#,
        &range_params(ts_ns),
        db,
    );
    assert!(!mp.rollup, "issue #227: a range count slides raw");
    assert!(mp.client.is_some());
    assert_eq!(mp.table, "log_samples");
    let table = format!("{db}.log_samples");
    let sql = sql::metric_raw_samples_sliding(
        &table,
        &["'checkout'".to_string()],
        &[FP_PROD],
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
    );
    let usage = explain(&client, &sql).await;
    assert_eq!(usage, expected_metric_instant_raw_usage());
}

#[tokio::test]
async fn m6_10_unwrapped_sum_over_time_reads_log_samples_raw_on_the_primary_key() {
    skip_unless_live!();
    let db = "pulsus_read_it_m610_client_raw";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let mp = metric_plan(
        r#"sum_over_time({env="prod"} | logfmt | unwrap duration(took) [5m])"#,
        &range_params(ts_ns),
        db,
    );
    assert!(!mp.rollup);
    assert!(mp.client.is_some(), "unwrap forces the client-agg mode");
    assert_eq!(mp.table, "log_samples");
    assert_eq!(
        mp.routing.reason,
        "raw: client-side pipeline/unwrap aggregation"
    );
    let table = format!("{db}.log_samples");
    let sql = sql::metric_raw_samples(
        &table,
        &["'checkout'".to_string()],
        &[FP_PROD],
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
    );
    assert!(!sql.contains("LIMIT"), "aggregations never truncate: {sql}");
    let usage = explain(&client, &sql).await;
    // Same `(service, fingerprint, timestamp_ns)` primary-key engagement
    // as every raw log_samples read; no body predicate, so no Skip
    // blocks are consulted.
    assert_eq!(usage, expected_metric_instant_raw_usage());
}

/// Issue #344 — a range-aggregation grouping costs the SCAN nothing.
///
/// The clause is a per-row label projection inside the client
/// aggregator; it never reaches SQL. So a grouped query must plan the
/// **byte-identical** statement to its ungrouped twin, engage the same
/// primary key, and issue the same ONE scan — never one scan per group,
/// and never an extra round trip to enumerate the groups. Asserted as
/// SQL equality plus the shared `EXPLAIN indexes=1` usage, so a future
/// change that pushed grouping into SQL would have to justify itself
/// here rather than land unnoticed.
#[tokio::test]
async fn a_grouped_range_aggregation_plans_the_same_single_raw_scan_as_its_ungrouped_twin() {
    skip_unless_live!();
    let db = "pulsus_read_it_344_grouped_scan";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let table = format!("{db}.log_samples");
    let build = |query: &str| {
        let mp = metric_plan(query, &range_params(ts_ns), db);
        assert!(!mp.rollup, "{query}");
        assert!(mp.client.is_some(), "{query}: unwrap forces client-agg");
        assert_eq!(mp.table, "log_samples", "{query}");
        assert_eq!(
            mp.routing.reason, "raw: client-side pipeline/unwrap aggregation",
            "{query}: grouping must not change the routing decision"
        );
        let sql = sql::metric_raw_samples(
            &table,
            &["'checkout'".to_string()],
            &[FP_PROD],
            TimeWindow {
                start_ns: mp.start_ns,
                end_ns: mp.end_ns,
            },
            mp.scan_lower,
            &mp.extra_predicates,
        );
        assert!(!sql.contains("LIMIT"), "{query}: {sql}");
        (mp, sql)
    };

    let (plain, plain_sql) =
        build(r#"max_over_time({env="prod"} | logfmt | unwrap duration(took) [5m])"#);
    for grouped in [
        r#"max_over_time({env="prod"} | logfmt | unwrap duration(took) [5m]) by (env)"#,
        r#"max_over_time({env="prod"} | logfmt | unwrap duration(took) [5m]) without (env)"#,
        r#"max_over_time({env="prod"} | logfmt | unwrap duration(took) [5m]) by ()"#,
    ] {
        let (mp, sql) = build(grouped);
        assert_eq!(sql, plain_sql, "{grouped}: the emitted SQL must not move");
        assert_eq!(
            (mp.start_ns, mp.end_ns, mp.step_ns),
            (plain.start_ns, plain.end_ns, plain.step_ns),
            "{grouped}: the scan window must not move"
        );
        assert_eq!(
            mp.probes.len(),
            plain.probes.len(),
            "{grouped}: no extra round trip"
        );
        assert!(
            mp.client.as_ref().expect("client").grouping.is_some(),
            "{grouped}: the clause must be planned, not dropped"
        );
        let usage = explain(&client, &sql).await;
        assert_eq!(usage, expected_metric_instant_raw_usage(), "{grouped}");
    }
}

#[tokio::test]
async fn metric_raw_fallback_uses_the_service_fingerprint_timestamp_primary_key() {
    skip_unless_live!();
    let db = "pulsus_read_it_metric_raw";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    // A line filter forces the raw fallback (`plan::metric_plan`: the
    // rollup table has no `body` column to filter on).
    let mp = metric_plan(
        r#"count_over_time({env="prod"} |= "refused" [5m])"#,
        &range_params(ts_ns),
        db,
    );
    assert!(!mp.rollup);
    let table = format!("{db}.log_samples");
    // Issue #227: a range read with a line filter slides raw
    // (`metric_raw_samples_sliding`), the filter pushed down as a predicate.
    let sql = sql::metric_raw_samples_sliding(
        &table,
        &["'checkout'".to_string()],
        &[FP_PROD],
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
    );

    let usage = explain(&client, &sql).await;
    assert_eq!(usage, expected_stage3_line_filter_usage());
}
