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

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
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
        Plan::Metric(_) => panic!("expected a Streams plan"),
    }
}

fn metric_plan(query: &str, params: &QueryParams, db: &str) -> pulsus_read::logql::MetricPlan {
    let expr = parse(query).expect("parse");
    match plan(&expr, params, &plan_ctx(db)).expect("plan") {
        Plan::Metric(mp) => mp,
        Plan::Streams(_) => panic!("expected a Metric plan"),
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
        sp.limit,
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

// ---------------------------------------------------------------------
// Metric reads — rollup-served vs raw fallback, range vs instant.
// ---------------------------------------------------------------------

#[tokio::test]
async fn metric_rollup_range_read_uses_the_fingerprint_bucket_primary_key() {
    skip_unless_live!();
    let db = "pulsus_read_it_metric_rollup_range";
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let mp = metric_plan(r#"rate({env="prod"}[5m])"#, &range_params(ts_ns), db);
    assert!(mp.rollup, "fixture query must be rollup-eligible");
    let table = format!("{db}.log_metrics_5s");
    let sql = sql::metric_range(
        sql::MetricSource {
            table: &table,
            bucket_col: mp.bucket_col,
            agg_expr: mp.agg_expr,
        },
        &[],
        &[FP_PROD],
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.step_ns.expect("range spec"),
        &mp.extra_predicates,
    );

    let usage = explain(&client, &sql).await;
    assert_eq!(usage, expected_metric_rollup_usage());
    assert!(!usage.iter().any(|l| l.contains("service")));
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
    let sql = sql::metric_range(
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
        mp.step_ns.expect("range spec"),
        &mp.extra_predicates,
    );

    let usage = explain(&client, &sql).await;
    assert_eq!(usage, expected_stage3_line_filter_usage());
}
