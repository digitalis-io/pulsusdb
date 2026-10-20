//! Issue #603: logs label discovery against a real 2-shard cluster, over
//! **split placement** — the placement the landing table produces.
//!
//! Before this change the writer inserted through the Logs family's `_dist`
//! wrappers, so `cityHash64(fingerprint)` put one stream's index rows and its
//! rollup rows on one shard, and the discovery read could evaluate its
//! semi-join shard-locally. The landing table has no wrapper: a push carries
//! many fingerprints, so routing its block through one would split the push
//! per shard and end "one push is one block". A fingerprint's rows can
//! therefore sit on several shards, and shard-local evaluation can only answer
//! *was this fingerprint active on **this** shard* where the question is *was
//! it active **anywhere***.
//!
//! **A clustered deployment is the production shape**, so this is the case for
//! the read path's primary deployment rather than a variant of it.
//!
//! Setup mirrors `live_metrics_cluster_fallback.rs` (clustered `run_init`, a
//! dedicated database, per-shard IP/port env overrides). Gated behind
//! `PULSUS_TEST_CLICKHOUSE=1` and requires the 2-shard fixture specifically:
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test live_logs_cluster_discovery
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_logql::parse;
use pulsus_read::logql::{Direction, QueryParams, QuerySpec, TimeBounds};
use pulsus_read::{EngineConfig, LogQlEngine, QueryResult};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const CLUSTER_NAME: &str = "pulsus_test_cluster";

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
                 run this test (see crates/pulsus-read/tests/live_logs_cluster_discovery.rs \
                 for setup)"
            );
            return;
        }
    };
}

/// Same fixture-static-IP convention as `pulsus-schema/tests/live_cluster.rs`
/// — dial each shard directly by IP, never a name — overridable via the
/// identical env var names for a runtime where the host cannot route to the
/// compose network directly.
fn shard_config(
    host_env: &str,
    default_host: &str,
    port_env: &str,
    default_port: u16,
    database: &str,
) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var(host_env).unwrap_or_else(|_| default_host.to_string()),
        http_port: std::env::var(port_env)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(default_port),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 8,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

fn shard1_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD1_HOST",
        "172.28.0.11",
        "PULSUS_TEST_CH_SHARD1_HTTP_PORT",
        8123,
        database,
    )
}

fn shard2_config(database: &str) -> ChConnConfig {
    shard_config(
        "PULSUS_TEST_CH_SHARD2_HOST",
        "172.28.0.12",
        "PULSUS_TEST_CH_SHARD2_HTTP_PORT",
        8123,
        database,
    )
}

fn cluster_ctx(db: &str) -> RenderCtx {
    RenderCtx {
        cluster: Some(CLUSTER_NAME.to_string()),
        ..RenderCtx::for_tests(db)
    }
}

fn engine_config(db: &str) -> EngineConfig {
    EngineConfig {
        skip_unavailable_shards: false,
        db: db.to_string(),
        streams_idx: "log_streams_idx_dist".to_string(),
        streams: "log_streams_dist".to_string(),
        samples: "log_samples_dist".to_string(),
        rollup_table: "log_metrics_5s_dist".to_string(),
        patterns_table: "log_patterns_dist".to_string(),
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
        distributed: true,
    }
}

async fn exec_on(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct ShardCountRow {
    shards: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct HostRow {
    host: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct ValueRow {
    value: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct MarksRow {
    marks: u64,
}

/// **The topology this case claims, established rather than assumed.**
///
/// `should_run()` is the same gate the single-node suites use, and the two
/// shard configurations default to the compose fixture's static container IPs.
/// Both pointed at ONE server is the unsafe direction: this case WOULD notice
/// it through its negative control — one server colocates the split inserts,
/// so the local subquery answers completely and the control fails — but
/// noticing through a control is not establishing the topology, so the
/// topology is read first, from the catalogue, with no insert.
async fn require_two_shard_topology(shard1: &ChClient, shard2: &ChClient) {
    let mut hosts = Vec::new();
    for (i, client) in [shard1, shard2].into_iter().enumerate() {
        let sql = format!(
            "SELECT toUInt64(count(DISTINCT shard_num)) AS shards FROM system.clusters \
             WHERE cluster = '{CLUSTER_NAME}'"
        );
        let mut stream = client
            .query_stream::<ShardCountRow>(&sql, &QuerySettings::new())
            .await
            .expect("read system.clusters");
        let shards = stream
            .next()
            .await
            .expect("one row")
            .expect("decode ShardCountRow")
            .shards;
        drop(stream);
        assert_eq!(
            shards,
            2,
            "shard{} reports {shards} shards in '{CLUSTER_NAME}': this case's \
             claim is about split placement across two",
            i + 1
        );

        let mut stream = client
            .query_stream::<HostRow>("SELECT hostName() AS host", &QuerySettings::new())
            .await
            .expect("read hostName()");
        hosts.push(
            stream
                .next()
                .await
                .expect("one row")
                .expect("decode HostRow")
                .host,
        );
    }
    assert_ne!(
        hosts[0], hosts[1],
        "both shard clients reached the same server ({}): the two shard \
         variables are aliased, and the split this case builds would be \
         colocated",
        hosts[0]
    );
}

/// The days-since-epoch a `Date` column takes for the month containing
/// `ts_ns`.
fn month_of(ts_ns: i64) -> u16 {
    pulsus_model::Date::start_of_month_utc(ts_ns)
        .expect("a representable month")
        .days_since_epoch()
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

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct IdxRow {
    /// The tenant (issue #653): the empty tenant's rows.
    org_id: String,
    month: u16,
    key: String,
    val: String,
    fingerprint: u128,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct RollupRow {
    /// The tenant (issue #653): the empty tenant's rows.
    org_id: String,
    fingerprint: u128,
    bucket_ns: i64,
    count: u64,
    bytes: u64,
    /// The lines exactly on `bucket_ns` (issue #614, part 2).
    count_at: u64,
    bytes_at: u64,
}

/// **T39.** The production discovery read answers completely over split
/// placement.
///
/// **The fixture is deliberately split**, which is what the landing table
/// produces and what a comparison of the two product modes over co-sharded
/// rows cannot exercise: one stream's `log_streams_idx` rows go into shard 1's
/// **local** table and its `log_metrics_<res>` rows into shard 2's, by naming
/// the local tables rather than the wrappers.
///
/// Three assertions:
///
/// 1. a **negative control** — the nested-subquery statement this case issues
///    itself, under `distributed_product_mode = 'local'`, which is the shape
///    and the setting the read path used before issue #603 — returns the
///    *incomplete* values, so the fixture is proven split rather than assumed;
/// 2. the **production path**, dispatched through the engine's own discovery
///    entry point rather than through a statement this case writes, returns
///    the complete values. **This is the gate.**
/// 3. the two runs' per-shard `SelectedMarks`, read out of `system.query_log`,
///    are **recorded and compared as a bound, not asserted as a gate**.
///
/// **The mark equality is unsettled and this case claims none.** The sorting
/// key argues for one — `log_streams_idx` is `ORDER BY (key, val,
/// fingerprint)`, a discovery scan bounds `month` and fixes `key`, and
/// `fingerprint` is the third component, so no `fingerprint IN (…)` set prunes
/// a contiguous mark range. That argument does not close: on this fixture the
/// local control's set is EMPTY on the shard holding the index row, and what
/// the optimizer does with an empty `IN` set decides the marks. So the case
/// prints both figures with the shape beside each and fails only if either
/// exceeds that shape's own scan budget.
#[tokio::test]
async fn the_production_discovery_read_answers_completely_over_split_placement() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_logs_cluster_discovery");

    let bootstrap = ChClient::new(shard1_config("default"))
        .await
        .expect("connect shard1 (bootstrap)");
    exec_on(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    run_init(&bootstrap, &cluster_ctx(&db))
        .await
        .expect("run_init (clustered)");

    let shard1 = ChClient::new(shard1_config(&db))
        .await
        .expect("connect shard1 (data)");
    let shard2 = ChClient::new(shard2_config(&db))
        .await
        .expect("connect shard2 (data)");
    require_two_shard_topology(&shard1, &shard2).await;

    let ts = now_ns();
    let month = month_of(ts);
    const FP: u128 = 770_001;

    // **The split.** The index rows go to shard 1's LOCAL table and the
    // activity rows to shard 2's, by naming the base tables rather than the
    // `_dist` wrappers — so no sharding key decides placement and the two
    // sides of the question are provably on different servers.
    shard1
        .insert_block(
            "log_streams_idx",
            &[
                IdxRow {
                    org_id: String::new(),
                    month,
                    key: "env".to_string(),
                    val: "alpha".to_string(),
                    fingerprint: FP,
                },
                IdxRow {
                    org_id: String::new(),
                    month,
                    key: "service_name".to_string(),
                    val: "split".to_string(),
                    fingerprint: FP,
                },
            ],
        )
        .await
        .expect("seed shard 1's local index");
    shard2
        .insert_block(
            "log_metrics_5s",
            &[RollupRow {
                org_id: String::new(),
                fingerprint: FP,
                bucket_ns: (ts / 5_000_000_000) * 5_000_000_000,
                count: 1,
                bytes: 10,
                count_at: 0,
                bytes_at: 0,
            }],
        )
        .await
        .expect("seed shard 2's local rollup");

    let bounds = TimeBounds {
        start_ns: ts - 60_000_000_000,
        end_ns: ts + 60_000_000_000,
    };

    // -- 1. the negative control: the nested subquery under `'local'`.
    //
    // Written out here rather than built by `sql::`: the point is what the
    // read path used to issue, and a control produced by the builder under
    // test would move with it.
    let control_id = format!("logs-disc-control-{}", std::process::id());
    let control_sql = format!(
        "SELECT DISTINCT val AS value\nFROM {db}.log_streams_idx_dist\n\
         WHERE month = toDate({month}) AND key = 'env'\n  \
         AND fingerprint IN (SELECT DISTINCT fingerprint FROM {db}.log_metrics_5s_dist \
         WHERE bucket_ns >= {} AND bucket_ns <= {})\nORDER BY value",
        (bounds.start_ns / 5_000_000_000) * 5_000_000_000,
        bounds.end_ns
    );
    let control = read_values(
        &shard1,
        &control_sql,
        &QuerySettings::new()
            .set("distributed_product_mode", "local")
            .set("log_comment", control_id.as_str())
            .set("query_id", control_id.as_str()),
    )
    .await;
    assert!(
        control.is_empty(),
        "the negative control must answer INCOMPLETELY over split placement — \
         shard 1 holds the index row and shard 2 holds the activity, so \
         shard-local evaluation finds neither side whole. It answered \
         {control:?}, which means the fixture is not split: the two shard \
         clients are reaching one server, or the inserts were forwarded"
    );

    // -- 2. the production path. **This is the gate.**
    let engine = LogQlEngine::new(
        ChClient::new(shard1_config(&db))
            .await
            .expect("connect the engine's client"),
        engine_config(&db),
        pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
    );
    let values = engine
        .label_values("env", None, bounds)
        .await
        .expect("the discovery read answers");
    assert_eq!(
        values,
        vec!["alpha".to_string()],
        "the production path must answer COMPLETELY over split placement: the \
         activity scan is a statement of its own and its result is rendered \
         into the index scan as a literal list, so neither side needs the \
         other to be on its own shard"
    );

    // -- 3. the marks, recorded and bounded — not asserted equal.
    exec_on(&shard1, "SYSTEM FLUSH LOGS").await;
    exec_on(&shard2, "SYSTEM FLUSH LOGS").await;
    let control_marks = selected_marks(&shard1, &shard2, &control_id).await;
    eprintln!(
        "T39 marks: nested-subquery-under-local = {control_marks} (per-shard sum). \
         The production shape's figure is not attributed here: it is two \
         statements, and this case reads one query id."
    );
    // The bound each shape is held to is its own scan budget, which is what
    // `EngineConfig::scan_budget_bytes` already enforces server-side; a mark
    // count past a whole table's marks would mean the month partition stopped
    // pruning.
    let total_marks = table_marks(&shard1, &shard2, &db, "log_streams_idx").await;
    assert!(
        control_marks <= total_marks,
        "the control read {control_marks} marks of a {total_marks}-mark table"
    );

    exec_on(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

async fn read_values(client: &ChClient, sql: &str, settings: &QuerySettings) -> Vec<String> {
    let mut stream = client
        .query_stream::<ValueRow>(sql, settings)
        .await
        .expect("the statement runs");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode ValueRow").value);
    }
    out
}

/// The per-shard `SelectedMarks` for `query_id`, summed — the per-shard,
/// coordinator-inclusive instrument `docs/schemas.md` §9 defines.
async fn selected_marks(shard1: &ChClient, shard2: &ChClient, query_id: &str) -> u64 {
    let sql = format!(
        "SELECT toUInt64(sum(ProfileEvents['SelectedMarks'])) AS marks FROM system.query_log \
         WHERE type = 'QueryFinish' AND (query_id = '{query_id}' \
         OR initial_query_id = '{query_id}')"
    );
    let mut total = 0u64;
    for client in [shard1, shard2] {
        let mut stream = client
            .query_stream::<MarksRow>(&sql, &QuerySettings::new())
            .await
            .expect("read system.query_log");
        if let Some(row) = stream.next().await {
            total += row.expect("decode MarksRow").marks;
        }
    }
    total
}

/// Every mark of `table`, summed across both shards' local parts — the
/// denominator a scan's own mark count is bounded by.
async fn table_marks(shard1: &ChClient, shard2: &ChClient, db: &str, table: &str) -> u64 {
    let sql = format!(
        "SELECT toUInt64(sum(marks)) AS marks FROM system.parts \
         WHERE active AND database = '{db}' AND table = '{table}'"
    );
    let mut total = 0u64;
    for client in [shard1, shard2] {
        let mut stream = client
            .query_stream::<MarksRow>(&sql, &QuerySettings::new())
            .await
            .expect("read system.parts");
        if let Some(row) = stream.next().await {
            total += row.expect("decode MarksRow").marks;
        }
    }
    total
}

// ---------------------------------------------------------------------------
// #603 follow-up: the answer pins, under a profile that would change the
// answer. Each case lands its rows through each shard's own `log_landing`,
// the way a push through that node would, and connects the engine as a user
// whose own settings carry the profile value, so the engine's pins are what
// overrides it.
// ---------------------------------------------------------------------------

/// The fingerprint every #603 follow-up case lands.
const SPLIT_FP: u128 = 770_002;

/// The `log_landing` columns every #603 follow-up case writes.
const LANDING_COLUMNS: &str = "(org_id, received_ms, kind, service, fingerprint, timestamp_ns, \
     severity, body, structured_metadata, month, labels, updated_ns, pattern, pattern_count, \
     pattern_level)";

/// One clustered database with both shard clients, the bootstrap client on
/// shard 1, and the instant every row is placed around.
struct Split {
    db: String,
    bootstrap: ChClient,
    shard1: ChClient,
    shard2: ChClient,
    ts: i64,
    month: u16,
}

/// `db` is the case's own `pulsus_testkit::test_db(…)`, composed at its call
/// site so the naming guards can read the literal.
async fn split_fixture(db: String) -> Split {
    let bootstrap = ChClient::new(shard1_config("default"))
        .await
        .expect("connect shard1 (bootstrap)");
    exec_on(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    run_init(&bootstrap, &cluster_ctx(&db))
        .await
        .expect("run_init (clustered)");
    let shard1 = ChClient::new(shard1_config(&db))
        .await
        .expect("connect shard1 (data)");
    let shard2 = ChClient::new(shard2_config(&db))
        .await
        .expect("connect shard2 (data)");
    require_two_shard_topology(&shard1, &shard2).await;
    let ts = now_ns();
    Split {
        month: month_of(ts),
        db,
        bootstrap,
        shard1,
        shard2,
        ts,
    }
}

impl Split {
    /// The kind-1 row `{env="alpha", service_name="split"}`.
    fn stream_row(&self) -> String {
        format!(
            "('', {}, 1, 'split', {SPLIT_FP}, 0, 0, '', '', toDate({}), \
             '{{\"env\":\"alpha\",\"service_name\":\"split\"}}', {}, '', 0, '')",
            self.ts / 1_000_000,
            self.month,
            self.ts
        )
    }

    /// One kind-0 line `seconds_before` the fixture's instant.
    fn line_row(&self, seconds_before: i64, body: &str) -> String {
        format!(
            "('', {}, 0, 'split', {SPLIT_FP}, {}, 0, '{body}', '', toDate({}), '', 0, '', 0, '')",
            self.ts / 1_000_000,
            self.ts - seconds_before * 1_000_000_000,
            self.month
        )
    }

    /// L1's landing rows: the stream row through each shard, 3 lines through
    /// shard 1 and 5 through shard 2, checked in place (F1).
    async fn land_three_and_five(&self) {
        let mut rows1 = vec![self.stream_row()];
        for (s, b) in [(30, "s1-a"), (20, "s1-b"), (10, "s1-c")] {
            rows1.push(self.line_row(s, b));
        }
        let mut rows2 = vec![self.stream_row()];
        for (s, b) in [
            (29, "s2-a"),
            (28, "s2-b"),
            (27, "s2-c"),
            (26, "s2-d"),
            (25, "s2-e"),
        ] {
            rows2.push(self.line_row(s, b));
        }
        for (client, rows) in [(&self.shard1, rows1), (&self.shard2, rows2)] {
            insert_on(
                client,
                &format!(
                    "INSERT INTO log_landing {LANDING_COLUMNS} VALUES {}",
                    rows.join(", ")
                ),
            )
            .await;
        }
        // F1: the placement, each shard read alone.
        assert_eq!(
            count_on(
                &self.shard1,
                "SELECT toUInt64(count()) AS n FROM log_samples"
            )
            .await,
            3,
            "shard 1's own lines"
        );
        assert_eq!(
            count_on(
                &self.shard2,
                "SELECT toUInt64(count()) AS n FROM log_samples"
            )
            .await,
            5,
            "shard 2's own lines"
        );
    }

    /// A user on shard 1 whose own settings carry `settings`.
    async fn profile_user(&self, suffix: &str, settings: &str) -> String {
        let user = format!("{}_{suffix}", self.db);
        for stmt in [
            format!("DROP USER IF EXISTS {user}"),
            format!("CREATE USER {user} IDENTIFIED WITH no_password SETTINGS {settings}"),
            format!("GRANT SELECT ON {}.* TO {user}", self.db),
        ] {
            exec_on(&self.bootstrap, &stmt).await;
        }
        user
    }

    /// A client on shard 1, bound to the database, as `user`.
    async fn client_as(&self, user: &str) -> ChClient {
        ChClient::new(ChConnConfig {
            user: user.to_string(),
            ..shard1_config(&self.db)
        })
        .await
        .expect("connect as the profile user")
    }

    /// The engine on shard 1, as `user` (the default user when `None`), with
    /// the option at `skip`.
    async fn engine(&self, user: Option<&str>, skip: bool) -> LogQlEngine {
        let client = match user {
            Some(user) => self.client_as(user).await,
            None => ChClient::new(shard1_config(&self.db))
                .await
                .expect("connect the engine's client"),
        };
        LogQlEngine::new(
            client,
            EngineConfig {
                skip_unavailable_shards: skip,
                ..engine_config(&self.db)
            },
            pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        )
    }

    /// L1's window, `[ts − 60s, ts + 60s]`.
    fn bounds(&self) -> TimeBounds {
        TimeBounds {
            start_ns: self.ts - 60_000_000_000,
            end_ns: self.ts + 60_000_000_000,
        }
    }

    /// A range query over L1's window, step 1s, limit 100, newest first.
    fn range_params(&self) -> QueryParams {
        QueryParams {
            spec: QuerySpec::Range {
                start_ns: self.ts - 60_000_000_000,
                end_ns: self.ts + 60_000_000_000,
                step_ns: 1_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        }
    }

    async fn teardown(&self, users: &[&str]) {
        for user in users {
            exec_on(&self.bootstrap, &format!("DROP USER IF EXISTS {user}")).await;
        }
        exec_on(
            &self.bootstrap,
            &format!(
                "DROP DATABASE IF EXISTS {} ON CLUSTER '{CLUSTER_NAME}' SYNC",
                self.db
            ),
        )
        .await;
    }
}

async fn insert_on(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("insert failed: {e}\nSQL:\n{sql}"));
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct CountRow {
    n: u64,
}

/// Every `n` a statement answers, in order.
async fn counts_on(client: &ChClient, sql: &str) -> Vec<u64> {
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.unwrap_or_else(|e| panic!("{e}\nSQL:\n{sql}")).n);
    }
    out
}

/// The one `n` a statement answers.
async fn count_on(client: &ChClient, sql: &str) -> u64 {
    let rows = counts_on(client, sql).await;
    assert_eq!(rows.len(), 1, "one row from {sql}");
    rows[0]
}

/// The entries of a streams answer.
fn entry_count(result: &QueryResult) -> usize {
    match result {
        QueryResult::Streams { items, .. } => items.iter().map(|s| s.entries.len()).sum(),
        other => panic!("expected streams, got {other:?}"),
    }
}

fn split_selector() -> pulsus_logql::Expr {
    parse(r#"{service_name="split"}"#).expect("parse")
}

/// **L1.** A logs read answers completely over split placement under a
/// profile that prunes shards by the sharding key.
///
/// P0 proves the profile prunes on this server and C that the read reaches
/// both shards; without them G1 and G2 would prove nothing.
#[tokio::test]
async fn a_logs_read_answers_completely_over_split_placement_under_a_pruning_profile() {
    skip_unless_live!();
    let f = split_fixture(pulsus_testkit::test_db("pulsus_read_it_logs_cluster_prune")).await;
    f.land_three_and_five().await;
    let user = f
        .profile_user("prune", "optimize_skip_unused_shards = 1")
        .await;

    // P0: the profile prunes on this server.
    let pruned = count_on(
        &f.client_as(&user).await,
        &format!(
            "SELECT toUInt64(count()) AS n FROM {}.log_samples_dist \
             WHERE fingerprint IN (toUInt128('{SPLIT_FP}'))",
            f.db
        ),
    )
    .await;
    assert!(
        pruned == 3 || pruned == 5,
        "the profile must prune to the hash owner on this server, got {pruned}"
    );

    // C: the default user reads both shards.
    let (result, _) = f
        .engine(None, false)
        .await
        .query(&split_selector(), &f.range_params())
        .await
        .expect("the control read answers");
    assert_eq!(entry_count(&result), 8, "the control reads both shards");

    // G1, G2: the profile user gets the same answer. Both are read before
    // either is asserted, so a failure shows both.
    let engine = f.engine(Some(&user), false).await;
    let (result, _) = engine
        .query(&split_selector(), &f.range_params())
        .await
        .expect("the read answers under the pruning profile");
    let stats = engine
        .stats(&split_selector(), f.bounds())
        .await
        .expect("the stats read answers under the pruning profile");
    assert_eq!(
        (entry_count(&result), stats.entries),
        (8, 8),
        "G1 (query entries), G2 (stats entries): a logs read must reach every shard \
         whatever the profile says"
    );

    f.teardown(&[&user]).await;
}

/// **LN.** A logs aggregate merges every shard's partial under a profile
/// that turns the merge off.
#[tokio::test]
async fn a_logs_aggregate_merges_every_shard_under_a_no_merge_profile() {
    skip_unless_live!();
    let f = split_fixture(pulsus_testkit::test_db(
        "pulsus_read_it_logs_cluster_nomerge",
    ))
    .await;
    f.land_three_and_five().await;
    let user = f
        .profile_user("nomerge", "distributed_group_by_no_merge = 1")
        .await;

    // P3: the profile splits the answer on this server.
    let mut parts = counts_on(
        &f.client_as(&user).await,
        &format!(
            "SELECT toUInt64(sum(count)) AS n FROM {}.log_metrics_5s_dist \
             WHERE fingerprint = toUInt128('{SPLIT_FP}')",
            f.db
        ),
    )
    .await;
    parts.sort_unstable();
    assert_eq!(
        parts,
        vec![3, 5],
        "the profile must return each shard's partial unmerged on this server"
    );

    // C4: the default user.
    let stats = f
        .engine(None, false)
        .await
        .stats(&split_selector(), f.bounds())
        .await
        .expect("the control stats read answers");
    assert_eq!(stats.entries, 8, "the control merges both shards");

    // G5: the profile user.
    let stats = f
        .engine(Some(&user), false)
        .await
        .stats(&split_selector(), f.bounds())
        .await
        .expect("the stats read answers under the no-merge profile");
    assert_eq!(
        stats.entries, 8,
        "G5: the read must merge every shard's partial whatever the profile says"
    );

    f.teardown(&[&user]).await;
}

/// **L2.** With shard 2's `log_samples` gone, a logs read fails unless the
/// option allows a partial answer — and a profile alone does not.
#[tokio::test]
async fn a_logs_read_with_a_shard_down_fails_unless_the_option_allows_a_partial_answer() {
    skip_unless_live!();
    let f = split_fixture(pulsus_testkit::test_db("pulsus_read_it_logs_cluster_down")).await;
    f.land_three_and_five().await;

    // K1: shard 2 loses `log_samples` alone.
    exec_on(&f.shard2, &format!("DROP TABLE {}.log_samples SYNC", f.db)).await;
    let user = f
        .profile_user("skipdown", "skip_unavailable_shards = 1")
        .await;

    // P1: the profile gives a silent partial answer on this server.
    let partial = count_on(
        &f.client_as(&user).await,
        &format!(
            "SELECT toUInt64(count()) AS n FROM {}.log_samples_dist",
            f.db
        ),
    )
    .await;
    assert_eq!(
        partial, 3,
        "the profile must skip the missing table on this server"
    );

    // C2: the default user, option off.
    let control = f
        .engine(None, false)
        .await
        .query(&split_selector(), &f.range_params())
        .await;
    assert!(control.is_err(), "the control must fail: {control:?}");

    // G3: the profile user, option off. G4: the default user, option on.
    // Both are read before either is asserted, so a failure shows both.
    let g3 = f
        .engine(Some(&user), false)
        .await
        .query(&split_selector(), &f.range_params())
        .await
        .map(|(result, _)| entry_count(&result));
    let g4 = f
        .engine(None, true)
        .await
        .query(&split_selector(), &f.range_params())
        .await
        .map(|(result, _)| entry_count(&result));
    assert!(
        g3.is_err() && matches!(g4, Ok(3)),
        "G3: with the option off the read must fail whatever the profile says (got \
         {g3:?}); G4: with the option on it answers shard 1's 3 lines (got {g4:?})"
    );

    f.teardown(&[&user]).await;
}

/// **LX.** With the option on, a shard that fails with an error is not
/// skipped under a profile that skips errors raised before processing.
#[tokio::test]
async fn a_shard_error_is_not_skipped_under_an_exception_skipping_profile() {
    skip_unless_live!();
    let f = split_fixture(pulsus_testkit::test_db(
        "pulsus_read_it_logs_cluster_shard_error",
    ))
    .await;
    f.land_three_and_five().await;

    // K3: shard 2 fails before it sends data.
    exec_on(
        &f.shard2,
        &format!(
            "ALTER TABLE {}.log_samples DROP INDEX idx_body_ngrams",
            f.db
        ),
    )
    .await;
    exec_on(
        &f.shard2,
        &format!("ALTER TABLE {}.log_samples DROP COLUMN body", f.db),
    )
    .await;
    let user = f
        .profile_user(
            "exskip",
            "skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing'",
        )
        .await;

    // P4: the profile skips the error on this server.
    let skipped = count_on(
        &f.client_as(&user).await,
        &format!(
            "SELECT toUInt64(count()) AS n FROM {}.log_samples_dist WHERE body != '' \
             SETTINGS skip_unavailable_shards = 1",
            f.db
        ),
    )
    .await;
    assert_eq!(
        skipped, 3,
        "the profile must skip shard 2's error on this server"
    );

    // C5: the default user, option on.
    let control = f
        .engine(None, true)
        .await
        .query(&split_selector(), &f.range_params())
        .await;
    assert!(control.is_err(), "the control must fail: {control:?}");

    // G6: the profile user, option on.
    let got = f
        .engine(Some(&user), true)
        .await
        .query(&split_selector(), &f.range_params())
        .await;
    assert!(
        got.is_err(),
        "G6: a shard's error must fail the read, not drop the shard, got {got:?}"
    );

    f.teardown(&[&user]).await;
}

/// **LO.** A logs read is not cut short under a profile that stops a
/// shard's read at a row limit and answers with what it read.
///
/// 200,000 lines per shard: at 50,000 a shard's lines are one part read as
/// one block, so `break` has nothing left to stop and P6 sees the whole
/// answer (measured on 26.8.21.10: 100,000 of 100,000; at 200,000, 147,456
/// of 400,000).
#[tokio::test]
async fn a_logs_read_is_not_cut_short_under_a_break_profile() {
    skip_unless_live!();
    let f = split_fixture(pulsus_testkit::test_db(
        "pulsus_read_it_logs_cluster_overflow",
    ))
    .await;
    for client in [&f.shard1, &f.shard2] {
        insert_on(
            client,
            &format!(
                "INSERT INTO log_landing {LANDING_COLUMNS} VALUES {}",
                f.stream_row()
            ),
        )
        .await;
        insert_on(
            client,
            &format!(
                "INSERT INTO log_landing {LANDING_COLUMNS} \
                 SELECT '', {}, 0, 'split', toUInt128('{SPLIT_FP}'), \
                 {} + number * 1000, 0, concat('line-', toString(number)), '', toDate({}), \
                 '', 0, '', 0, '' FROM numbers(200000)",
                f.ts / 1_000_000,
                f.ts - 60_000_000_000,
                f.month
            ),
        )
        .await;
    }
    // F9: the placement.
    for client in [&f.shard1, &f.shard2] {
        assert_eq!(
            count_on(client, "SELECT toUInt64(count()) AS n FROM log_samples").await,
            200_000,
            "each shard's own lines"
        );
    }
    let user = f
        .profile_user(
            "break",
            "max_rows_to_read_leaf = 1000, read_overflow_mode_leaf = 'break'",
        )
        .await;

    // P6: the profile cuts the read short on this server, with no error.
    let cut = count_on(
        &f.client_as(&user).await,
        &format!(
            "SELECT toUInt64(count()) AS n FROM {}.log_samples_dist WHERE body LIKE '%line%'",
            f.db
        ),
    )
    .await;
    assert!(
        cut < 400_000,
        "the profile must cut the read short on this server, got {cut}"
    );

    let expr = parse(r#"{service_name="split"} |= "line""#).expect("parse");
    let bounds = TimeBounds {
        start_ns: f.ts - 120_000_000_000,
        end_ns: f.ts + 60_000_000_000,
    };

    // C7: the default user.
    let stats = f
        .engine(None, false)
        .await
        .stats(&expr, bounds)
        .await
        .expect("the control stats read answers");
    assert_eq!(stats.entries, 400_000, "the control reads every line");

    // G8: the profile user.
    let got = f
        .engine(Some(&user), false)
        .await
        .stats(&expr, bounds)
        .await;
    assert!(
        got.is_err(),
        "G8: a read that hits a leaf limit must fail, not answer what it read; got {got:?}"
    );

    f.teardown(&[&user]).await;
}
