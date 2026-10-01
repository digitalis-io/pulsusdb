//! The trace landing table, its five views and the five tables they
//! maintain, against a real ClickHouse server (issues #584 to #586).
//!
//! `live_traces.rs` is the **old** two-table trace schema's suite and stays
//! green: the reads that have not moved still answer from `trace_spans` and
//! `trace_attrs_idx`, and no case here compares the two stores.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, mirroring `live_traces.rs`:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-schema --test live_traces_v2
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Each test uses its own dedicated database (dropped at the start of the
//! test) so tests can run concurrently against the same server.

use std::collections::BTreeSet;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

/// The six tables on the trace write path: the landing table and the five
/// the views maintain.
const WRITE_PATH_TABLES: [&str; 6] = [
    "trace_landing",
    "spans",
    "resources",
    "traces",
    "tag_names",
    "tag_values",
];

/// The five tables the views maintain, as a `table IN (…)` list. The
/// landing table is not one of them.
const TARGET_TABLE_LIST: &str = "'spans', 'traces', 'resources', 'tag_names', 'tag_values'";

/// The five views, one per target.
const TRACE_MVS: [&str; 5] = [
    "spans_mv",
    "resources_mv",
    "traces_mv",
    "tag_names_mv",
    "tag_values_mv",
];

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
        query_timeout: Duration::from_secs(60),
        ..ChConnConfig::default()
    }
}

fn test_ctx(db: &str) -> SchemaParams {
    RenderCtx::for_tests(db)
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-schema/tests/live_traces_v2.rs for setup)"
            );
            return;
        }
    };
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct NameRow {
    name: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TextRow {
    s: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct NameTypeRow {
    name: String,
    #[serde(rename = "type")]
    ty: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TableColumnCodecRow {
    table: String,
    name: String,
    compression_codec: String,
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

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .unwrap_or_else(|e| panic!("statement failed: {e}\nSQL:\n{sql}"));
}

async fn count(client: &ChClient, sql: &str) -> u64 {
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("count query failed: {e}\nSQL:\n{sql}"));
    stream.next().await.expect("one row").expect("decode").n
}

/// The single `s` column of a one-row query.
async fn scalar(client: &ChClient, sql: &str) -> String {
    let mut stream = client
        .query_stream::<TextRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("scalar query failed: {e}\nSQL:\n{sql}"));
    stream
        .next()
        .await
        .expect("one row")
        .expect("decode TextRow")
        .s
}

async fn names(client: &ChClient, sql: &str) -> Vec<String> {
    let mut stream = client
        .query_stream::<NameRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("name query failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode NameRow").name);
    }
    out
}

/// `system.tables.engine_full` for one object.
async fn engine_full(client: &ChClient, db: &str, name: &str) -> String {
    scalar(
        client,
        &format!(
            "SELECT engine_full AS s FROM system.tables \
             WHERE database = '{db}' AND name = '{name}'"
        ),
    )
    .await
}

/// `(name, type)` for every column of one table, ordered by name.
async fn columns(client: &ChClient, db: &str, table: &str) -> Vec<(String, String)> {
    let sql = format!(
        "SELECT name, type FROM system.columns \
         WHERE database = '{db}' AND table = '{table}' ORDER BY name"
    );
    let mut stream = client
        .query_stream::<NameTypeRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("column query failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode NameTypeRow");
        out.push((row.name, row.ty));
    }
    out
}

/// Now, in epoch milliseconds. The fixtures below take their stamps from the
/// clock rather than from a literal: `ttl_only_drop_parts = 1` makes a whole
/// already-expired part eligible for deletion right after the insert, so a
/// fixed past instant would leave a case reading an empty table.
fn now_unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after the epoch")
        .as_millis() as i64
}

/// `count` kind-0 landing rows in **one** statement, so one block and one
/// part — a hundred single-row inserts price a hundred parts' overhead
/// rather than what one push's rows cost.
///
/// Written as literal SQL so this suite stays inside `pulsus-schema`'s own
/// dependency set: it does not depend on `pulsus-write`.
fn landing_span_block_sql(db: &str, received_ms: i64, base_ns: i64, count: u64) -> String {
    format!(
        "INSERT INTO {db}.trace_landing \
         (received_ms, row_kind, trace_id, span_id, parent_span_id, start_ns, duration_ns, \
          resource_id, name, kind, status_code, service, attrs, scope_attrs) \
         SELECT {received_ms}, 0, \
          reinterpretAsFixedString(toUInt128(0x1000 + number)), \
          reinterpretAsFixedString(toUInt64(0x2000 + number)), \
          toFixedString('', 8), {base_ns} + number * 1000000, 1000000, 1, 'GET /api', 2, 0, \
          'checkout', \
          CAST('{{\"http%2Erequest%2Emethod\":\"GET\",\"http%2Eresponse%2Estatus_code\":200}}' AS JSON), \
          CAST('{{}}' AS JSON) \
         FROM numbers({count})"
    )
}

/// One kind-0 landing row's `INSERT`, written as literal SQL for the same
/// reason [`landing_span_block_sql`] gives. The span's own columns only;
/// every other column of every other kind defaults.
fn landing_span_sql(
    db: &str,
    received_ms: i64,
    trace_hex: &str,
    span_hex: &str,
    start_ns: i64,
) -> String {
    format!(
        "INSERT INTO {db}.trace_landing \
         (received_ms, row_kind, trace_id, span_id, parent_span_id, start_ns, duration_ns, \
          resource_id, name, kind, status_code, service, attrs, scope_attrs) \
         SELECT {received_ms}, 0, unhex('{trace_hex}'), unhex('{span_hex}'), \
          toFixedString('', 8), {start_ns}, 1000000, 1, 'GET /api', 2, 0, 'checkout', \
          CAST('{{\"http%2Erequest%2Emethod\":\"GET\",\"http%2Eresponse%2Estatus_code\":200}}' AS JSON), \
          CAST('{{}}' AS JSON)"
    )
}

/// The six tables and the five views exist after one `run_init`, and the
/// five views' sources are recorded against the landing table.
#[tokio::test]
async fn trace_landing_and_its_views_exist_after_init() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_exist");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let quoted = WRITE_PATH_TABLES
        .iter()
        .map(|t| format!("'{t}'"))
        .collect::<Vec<_>>()
        .join(", ");
    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM system.tables \
                 WHERE database = '{db}' AND name IN ({quoted})"
            )
        )
        .await,
        6,
        "the landing table and its five targets"
    );

    let quoted = TRACE_MVS
        .iter()
        .map(|t| format!("'{t}'"))
        .collect::<Vec<_>>()
        .join(", ");
    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM system.tables \
                 WHERE database = '{db}' AND name IN ({quoted})"
            )
        )
        .await,
        5,
        "five views, five targets"
    );

    // The server's own record of what feeds what.
    // The filter sits in a subquery: an outer `AS name` shadows
    // `system.tables.name`, and the predicate would then read the projected
    // value rather than the table's own name.
    let deps = names(
        &client,
        &format!(
            "SELECT mv AS name FROM (\
               SELECT arrayJoin(dependencies_table) AS mv FROM system.tables \
               WHERE database = '{db}' AND name = 'trace_landing'\
             ) ORDER BY name"
        ),
    )
    .await;
    let mut want: Vec<String> = TRACE_MVS.iter().map(|s| (*s).to_string()).collect();
    want.sort();
    assert_eq!(deps, want, "every view's source is the landing table");

    drop_database(&client, db).await;
}

/// **The `MergeTree` `async_insert` pin is on the stored definition, not
/// only on the rendered template.** §7.3 of the design showed a setting the
/// server accepts on a `CREATE` and then discards, leaving no trace in this
/// column, so the two are separate assertions:
/// `the_trace_landing_migration_is_a_base_only_mergetree` asserts the
/// template and this asserts what the server kept.
#[tokio::test]
async fn the_landing_table_carries_the_async_insert_setting() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_async");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let engine = engine_full(&client, db, "trace_landing").await;
    assert!(
        engine.contains("async_insert = 0"),
        "the server must keep the table setting: {engine}"
    );

    drop_database(&client, db).await;
}

/// The three sorting keys this design states, read back off the server. A
/// per-column Boolean cannot carry an expression or an order, so the
/// assertion is on `system.tables.sorting_key` — the pattern
/// `live_traces.rs` already uses.
#[tokio::test]
async fn the_sorting_key_is_the_one_this_design_states() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_sortkey");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    for (table, sorting_key, granularity) in [
        (
            "spans",
            "intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind",
            Some("index_granularity = 2048"),
        ),
        ("traces", "trace_id", Some("index_granularity = 1024")),
        ("resources", "service, resource_id", None),
    ] {
        let got = scalar(
            &client,
            &format!(
                "SELECT sorting_key AS s FROM system.tables \
                 WHERE database = '{db}' AND name = '{table}'"
            ),
        )
        .await;
        assert_eq!(got, sorting_key, "{table}'s sorting key");
        let primary = scalar(
            &client,
            &format!(
                "SELECT primary_key AS s FROM system.tables \
                 WHERE database = '{db}' AND name = '{table}'"
            ),
        )
        .await;
        assert_eq!(primary, sorting_key, "{table}'s primary key");
        if let Some(granularity) = granularity {
            let engine = engine_full(&client, db, table).await;
            assert!(
                engine.contains(granularity),
                "{table} must declare {granularity}: {engine}"
            );
        }
    }

    drop_database(&client, db).await;
}

/// **The function inside each of the five aggregate declarations, compared,
/// not the outer type name alone.** `system.columns.type` prints the
/// declared function, so a column transcribed as
/// `SimpleAggregateFunction(any, …)` reddens here — and nowhere else: that
/// declaration changes an answer only once two rows for one `trace_id`
/// merge, and no read in this change can be made to depend on a merge.
#[tokio::test]
async fn the_per_trace_aggregate_columns_carry_the_functions_this_design_names() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_aggcols");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let got = columns(&client, db, "traces").await;
    let want: Vec<(String, String)> = [
        ("day", "Date"),
        ("end_ns", "SimpleAggregateFunction(max, Int64)"),
        (
            "root_name",
            "SimpleAggregateFunction(max, LowCardinality(String))",
        ),
        (
            "root_service",
            "SimpleAggregateFunction(max, LowCardinality(String))",
        ),
        (
            "services",
            "SimpleAggregateFunction(groupUniqArrayArray, Array(String))",
        ),
        ("start_ns", "SimpleAggregateFunction(min, Int64)"),
        ("trace_id", "FixedString(16)"),
    ]
    .iter()
    .map(|(n, t)| ((*n).to_string(), (*t).to_string()))
    .collect();
    assert_eq!(got, want, "the per-trace table's seven declarations");

    drop_database(&client, db).await;
}

/// **T-S5.** Every column of the five target tables carries a compression
/// codec — none empty in `system.columns`.
///
/// `trace_landing` is deliberately outside the domain: its `event_id`
/// carries no codec, and a presence test with one carve-out is where the
/// next bare column appears.
#[tokio::test]
async fn every_target_table_column_carries_a_codec() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_codecs");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    // The denominator first: an empty column set would pass the check below
    // while establishing nothing.
    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM system.columns \
                 WHERE database = '{db}' AND table IN ({TARGET_TABLE_LIST})"
            ),
        )
        .await,
        43,
        "the five target tables' own column count (23 + 7 + 7 + 2 + 4), so \
         the codec check below has a non-empty domain"
    );
    let bare = names(
        &client,
        &format!(
            "SELECT concat(table, '.', name) AS name FROM system.columns \
             WHERE database = '{db}' AND table IN ({TARGET_TABLE_LIST}) \
             AND compression_codec = '' \
             ORDER BY name"
        ),
    )
    .await;
    assert!(bare.is_empty(), "columns with no codec: {bare:?}");

    drop_database(&client, db).await;
}

/// **T-S5b.** The exact codec spelling of every column of the five target
/// tables, in declaration order within each table. A presence test cannot
/// tell `ZSTD(1)` from `ZSTD(22)`.
///
/// The spellings are the server's normalised ones rather than the `CREATE`'s
/// text, for the reason `live_schema.rs`'s `LANDING_COLUMNS` doc comment
/// records: `spans.start_ns` is declared `CODEC(Delta, ZSTD(1))` and the
/// argument is resolved from the column type, and `duration_ns`'s `T64`
/// takes none.
///
/// The column **type** is not pinned here — the per-table `(name, type)`
/// cases above own that.
const TARGET_TABLE_CODECS: [(&str, &str, &str); 43] = [
    ("resources", "day", "CODEC(ZSTD(1))"),
    ("resources", "resource_id", "CODEC(ZSTD(1))"),
    ("resources", "service", "CODEC(ZSTD(1))"),
    ("resources", "attrs", "CODEC(ZSTD(1))"),
    ("resources", "attrs_other", "CODEC(ZSTD(1))"),
    ("resources", "dropped_attrs", "CODEC(ZSTD(1))"),
    ("resources", "schema_url", "CODEC(ZSTD(1))"),
    ("spans", "trace_id", "CODEC(ZSTD(1))"),
    ("spans", "span_id", "CODEC(ZSTD(1))"),
    ("spans", "parent_span_id", "CODEC(ZSTD(1))"),
    ("spans", "start_ns", "CODEC(Delta(8), ZSTD(1))"),
    ("spans", "duration_ns", "CODEC(T64, ZSTD(1))"),
    ("spans", "service", "CODEC(ZSTD(1))"),
    ("spans", "resource_id", "CODEC(ZSTD(1))"),
    ("spans", "name", "CODEC(ZSTD(1))"),
    ("spans", "kind", "CODEC(ZSTD(1))"),
    ("spans", "status_code", "CODEC(ZSTD(1))"),
    ("spans", "status_message", "CODEC(ZSTD(1))"),
    ("spans", "trace_state", "CODEC(ZSTD(1))"),
    ("spans", "flags", "CODEC(ZSTD(1))"),
    ("spans", "scope_name", "CODEC(ZSTD(1))"),
    ("spans", "scope_version", "CODEC(ZSTD(1))"),
    ("spans", "scope_attrs", "CODEC(ZSTD(1))"),
    ("spans", "attrs", "CODEC(ZSTD(1))"),
    ("spans", "attrs_other", "CODEC(ZSTD(1))"),
    ("spans", "dropped_attrs", "CODEC(ZSTD(1))"),
    ("spans", "events", "CODEC(ZSTD(1))"),
    ("spans", "dropped_events", "CODEC(ZSTD(1))"),
    ("spans", "links", "CODEC(ZSTD(1))"),
    ("spans", "dropped_links", "CODEC(ZSTD(1))"),
    ("tag_names", "scope", "CODEC(ZSTD(1))"),
    ("tag_names", "key", "CODEC(ZSTD(1))"),
    ("tag_values", "scope", "CODEC(ZSTD(1))"),
    ("tag_values", "key", "CODEC(ZSTD(1))"),
    ("tag_values", "value", "CODEC(ZSTD(1))"),
    ("tag_values", "val_type", "CODEC(ZSTD(1))"),
    ("traces", "day", "CODEC(ZSTD(1))"),
    ("traces", "trace_id", "CODEC(ZSTD(1))"),
    ("traces", "start_ns", "CODEC(ZSTD(1))"),
    ("traces", "end_ns", "CODEC(ZSTD(1))"),
    ("traces", "root_service", "CODEC(ZSTD(1))"),
    ("traces", "root_name", "CODEC(ZSTD(1))"),
    ("traces", "services", "CODEC(ZSTD(1))"),
];

#[tokio::test]
async fn the_target_tables_carry_the_codecs_this_design_names() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_codec_text");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let sql = format!(
        "SELECT table, name, compression_codec FROM system.columns \
         WHERE database = '{db}' AND table IN ({TARGET_TABLE_LIST}) \
         ORDER BY table, position"
    );
    let mut stream = client
        .query_stream::<TableColumnCodecRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("codec query failed: {e}\nSQL:\n{sql}"));
    let mut seen: Vec<TableColumnCodecRow> = Vec::new();
    while let Some(row) = stream.next().await {
        seen.push(row.expect("decode TableColumnCodecRow"));
    }
    drop(stream);

    assert_eq!(
        seen.len(),
        TARGET_TABLE_CODECS.len(),
        "the five target tables' column count"
    );
    for (got, (table, name, codec)) in seen.iter().zip(TARGET_TABLE_CODECS) {
        assert_eq!(got.table, table, "table order");
        assert_eq!(got.name, name, "{table}'s column order");
        assert_eq!(got.compression_codec, codec, "{table}.{name}'s codec");
    }

    drop_database(&client, db).await;
}

/// **T-S6.** Applying the schema twice is a no-op: no error, and the column
/// set of each of the six tables is identical after both runs.
#[tokio::test]
async fn applying_the_schema_twice_is_a_no_op() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_twice");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    let ctx = test_ctx(db);
    run_init(&client, &ctx).await.expect("first run_init");

    let mut before = Vec::new();
    for table in WRITE_PATH_TABLES {
        let cols = columns(&client, db, table).await;
        assert!(
            !cols.is_empty(),
            "{table} has no columns after the first run_init, so the comparison \
             below would compare two empty sets"
        );
        before.push(cols);
    }

    run_init(&client, &ctx).await.expect("second run_init");

    for (i, table) in WRITE_PATH_TABLES.iter().enumerate() {
        assert_eq!(
            columns(&client, db, table).await,
            before[i],
            "{table}'s column set moved between two run_init calls"
        );
    }

    drop_database(&client, db).await;
}

/// The two tag catalogs carry a deduplication window and **no TTL**, because
/// `docs/api.md` §4.3 requires catalog entries to outlive span retention;
/// the other four each carry one. It is what catches a TTL statement added
/// for a catalog.
#[tokio::test]
async fn the_catalogs_carry_no_ttl_and_the_other_four_do() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_ttlset");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    for catalog in ["tag_names", "tag_values"] {
        let engine = engine_full(&client, db, catalog).await;
        assert!(
            !engine.contains("TTL"),
            "{catalog} must carry no TTL: {engine}"
        );
    }
    for retained in ["spans", "resources", "traces", "trace_landing"] {
        let engine = engine_full(&client, db, retained).await;
        assert!(
            engine.contains("TTL"),
            "{retained} must carry a TTL: {engine}"
        );
    }

    drop_database(&client, db).await;
}

/// The configured window reaches **all six** write-path tables, read off
/// each table's own stored definition rather than off the statements that
/// set it.
#[tokio::test]
async fn dedup_settings_reach_all_six_trace_tables() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_dedupwin");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    ctx.trace_dedup_window = 4242;
    run_init(&client, &ctx).await.expect("run_init");

    for table in WRITE_PATH_TABLES {
        let engine = engine_full(&client, db, table).await;
        assert!(
            engine.contains("non_replicated_deduplication_window = 4242"),
            "{table} must carry the configured window: {engine}"
        );
    }

    drop_database(&client, db).await;
}

/// The landing TTL is installed at the configured hours, in the saturating
/// form, read off the stored definition.
#[tokio::test]
async fn the_landing_ttl_is_installed_at_the_configured_hours() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_landingttl");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    ctx.trace_landing_retention_hours = 3;
    run_init(&client, &ctx).await.expect("run_init");

    // The expected text is the server's own rendering, not the statement
    // this build sends: 26.3.29.7 re-prints the expression with its own
    // parentheses and drops the default `DELETE` action.
    // `the_rendered_ttl_statements_are_byte_exact` owns the statement text.
    let engine = engine_full(&client, db, "trace_landing").await;
    assert!(
        engine
            .contains("TTL toDateTime(least(intDiv(received_ms, 1000) + (3 * 3600), 4294967295))"),
        "the landing TTL must carry the configured hours: {engine}"
    );

    drop_database(&client, db).await;
}

/// **T-R2, the clamp half.** The three stored TTLs carry the clamp, and at
/// the top of the admitted domain they answer one instant.
///
/// **Both expressions are read off the server's own stored definition**, not
/// retyped: a case carrying its own copy of the expression would answer the
/// clamped instant whatever this change installed.
///
/// **What the answer alone cannot show, measured rather than assumed.** On
/// ClickHouse 26.3.29.7 `toDateTime` saturates by itself:
/// `SELECT toDateTime(intDiv(4294943999000000000, 1000000000) + 7 * 86400)`
/// answers `2106-02-07 06:28:15`, the same instant as the clamped form, so
/// removing `least(…, 4294967295)` moves no answer on this version and the
/// read below cannot discriminate it. The first assertion is therefore on
/// the clamp being **in the stored text**, which is what a later version
/// without that saturation would need, and the read is what shows the
/// expression's value at the boundary rather than its shape.
#[tokio::test]
async fn the_ttl_clamps_at_the_top_of_the_admitted_domain() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_ttlclamp");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    /// The TTL expression out of one table's stored definition. The stored
    /// text carries no `DELETE` — 26.3.29.7 drops the default action — so
    /// the expression runs to the `SETTINGS` clause.
    fn ttl_expr(engine_full: &str, table: &str) -> String {
        let (_, after) = engine_full
            .split_once("TTL ")
            .unwrap_or_else(|| panic!("{table} carries no TTL: {engine_full}"));
        let (expr, _) = after
            .split_once(" SETTINGS")
            .unwrap_or_else(|| panic!("{table}'s TTL has no SETTINGS after it: {engine_full}"));
        expr.to_string()
    }

    // The clamp is in the stored text of all three retained tables.
    for (table, column) in [
        ("spans", "intDiv(start_ns, 1000000000)"),
        ("traces", "(toUInt32(day) * 86400)"),
        ("resources", "(toUInt32(day) * 86400)"),
    ] {
        let engine = engine_full(&client, db, table).await;
        let want = format!("TTL toDateTime(least({column} + (7 * 86400), 4294967295))");
        assert!(
            engine.contains(&want),
            "{table}'s stored TTL must carry the clamp: wanted {want} in {engine}"
        );
    }

    // The top of the admitted domain: 2106-02-06T23:59:59Z.
    let from_ns = scalar(
        &client,
        &format!(
            "SELECT toString({}) AS s FROM (SELECT toInt64(4294943999000000000) AS start_ns)",
            ttl_expr(&engine_full(&client, db, "spans").await, "spans")
        ),
    )
    .await;
    assert_eq!(
        from_ns, "2106-02-07 06:28:15",
        "the span TTL clamps rather than wrapping to 1970"
    );

    let from_day = scalar(
        &client,
        &format!(
            "SELECT toString({}) AS s FROM (SELECT toDate('2106-02-06') AS day)",
            ttl_expr(&engine_full(&client, db, "traces").await, "traces")
        ),
    )
    .await;
    assert_eq!(
        from_day, from_ns,
        "the Date form clamps to the same instant as the nanosecond one"
    );

    drop_database(&client, db).await;
}

/// **T-R1.** Dropping the older of two days' partitions drops no row of the
/// other, and does so without a mutation or a merge.
#[tokio::test]
async fn dropping_a_day_drops_no_rows_of_another() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_droppart");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    // Two spans, one per UTC day, inside the retention window: the landing
    // insert's own views place them in `spans`.
    let now_ms = now_unix_millis();
    let day_ns = 86_400_000_000_000i64;
    let newer_ns = now_ms * 1_000_000;
    let older_ns = newer_ns - day_ns;
    exec(
        &client,
        &landing_span_sql(db, now_ms, &"aa".repeat(16), &"11".repeat(8), older_ns),
    )
    .await;
    exec(
        &client,
        &landing_span_sql(db, now_ms, &"bb".repeat(16), &"22".repeat(8), newer_ns),
    )
    .await;

    assert_eq!(
        count(&client, &format!("SELECT count() AS n FROM {db}.spans")).await,
        2,
        "both spans landed through the view"
    );

    // `apply_ttl`'s own `MODIFY TTL` leaves one `MATERIALIZE TTL` row behind
    // at init, so the property is that the DROP adds none rather than that
    // the table has none. Measured on 26.3.29.7: one row, `(MATERIALIZE
    // TTL)`, `parts_to_do = 0`, before any insert.
    let mutations_before = count(
        &client,
        &format!(
            "SELECT count() AS n FROM system.mutations \
             WHERE database = '{db}' AND table = 'spans'"
        ),
    )
    .await;

    let older_day = scalar(
        &client,
        &format!("SELECT toString(toDate(fromUnixTimestamp64Nano({older_ns}))) AS s"),
    )
    .await;
    exec(
        &client,
        &format!("ALTER TABLE {db}.spans DROP PARTITION '{older_day}'"),
    )
    .await;

    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM {db}.spans \
                 WHERE toDate(fromUnixTimestamp64Nano(start_ns)) = toDate('{older_day}')"
            )
        )
        .await,
        0,
        "the dropped day is gone"
    );
    assert_eq!(
        count(&client, &format!("SELECT count() AS n FROM {db}.spans")).await,
        1,
        "the other day is untouched"
    );
    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM system.mutations \
                 WHERE database = '{db}' AND table = 'spans'"
            )
        )
        .await,
        mutations_before,
        "a partition drop is not a mutation"
    );
    assert_eq!(
        count(
            &client,
            &format!(
                "SELECT count() AS n FROM system.merges \
                 WHERE database = '{db}' AND table = 'spans'"
            )
        )
        .await,
        0,
        "nor a merge"
    );

    drop_database(&client, db).await;
}

/// **T-S1.** The columns that hold attribute values are exactly these. The
/// landing table's six are new to this case, which is why the expected set
/// is restated here rather than cited.
#[tokio::test]
async fn the_attribute_columns_are_exactly_these() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_attrcols");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    // Every column of the six write-path tables whose declared type carries
    // a JSON value, plus the two text columns that carry attribute values —
    // `attrs_other` (a protobuf `KeyValueList`) and the catalogs' own value
    // column.
    let sql = format!(
        "SELECT concat(table, '.', name) AS name FROM (\
           SELECT table, name, type FROM system.columns \
           WHERE database = '{db}' \
             AND table IN ('trace_landing', 'spans', 'resources', 'tag_names', 'tag_values', \
                           'traces') \
             AND (position(type, 'JSON') > 0 \
                  OR name IN ('attrs_other', 'value', 'tag_value'))\
         ) ORDER BY name"
    );
    let got: BTreeSet<String> = names(&client, &sql).await.into_iter().collect();
    let want: BTreeSet<String> = [
        "resources.attrs",
        "resources.attrs_other",
        "spans.attrs",
        "spans.attrs_other",
        "spans.events",
        "spans.links",
        "spans.scope_attrs",
        "tag_values.value",
        "trace_landing.attrs",
        "trace_landing.attrs_other",
        "trace_landing.events",
        "trace_landing.links",
        "trace_landing.scope_attrs",
        "trace_landing.tag_value",
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect();
    assert_eq!(got, want, "the attribute-bearing columns");

    drop_database(&client, db).await;
}

/// **T-S3b.** The landing table is priced separately from the five retained
/// ones: `docs/TraceQL/functional-requirements.md` §8.1's bound is over
/// those five, and a denominator including this sixth table would fail
/// against a bound that never meant to include it.
///
/// The ceiling is wide enough that a fixture's part overhead does not fail
/// it and narrow enough to catch a row shape that grew by an order of
/// magnitude. It guards the design's derivation rather than proving it;
/// nothing here establishes the corpus-scale figures.
#[tokio::test]
async fn the_landed_storage_is_priced() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_priced");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let now_ms = now_unix_millis();
    let base_ns = now_ms * 1_000_000;
    exec(&client, &landing_span_block_sql(db, now_ms, base_ns, 100)).await;
    assert_eq!(
        count(
            &client,
            &format!("SELECT count() AS n FROM {db}.trace_landing")
        )
        .await,
        100,
        "the fixture landed"
    );

    let bytes = count(
        &client,
        &format!(
            "SELECT sum(bytes_on_disk) AS n FROM system.parts \
             WHERE database = '{db}' AND table = 'trace_landing' AND active"
        ),
    )
    .await;
    let per_span = bytes / 100;
    assert!(
        per_span < 200,
        "the landing table holds {per_span} B/span, over the 200 B/span ceiling \
         this case guards ({bytes} bytes over 100 spans)"
    );

    drop_database(&client, db).await;
}
