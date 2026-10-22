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
//!     clickhouse/clickhouse-server:26.8.21.10
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
use pulsus_schema::{RenderCtx, SchemaParams};
use pulsus_schema_testkit::run_init;

/// The nine tables on the trace write path: the landing table and the
/// eight the views maintain.
const WRITE_PATH_TABLES: [&str; 9] = [
    "trace_landing",
    "spans",
    "resources",
    "traces",
    "tag_names",
    "tag_values",
    "error_spans",
    "tag_bucket_values",
    "service_bucket_resources",
];

/// The eight tables the views maintain, as a `table IN (…)` list. The
/// landing table is not one of them.
const TARGET_TABLE_LIST: &str = "'spans', 'traces', 'resources', 'tag_names', 'tag_values', \
                                 'error_spans', 'tag_bucket_values', 'service_bucket_resources'";

/// The eight views, one per target.
const TRACE_MVS: [&str; 8] = [
    "spans_mv",
    "resources_mv",
    "traces_mv",
    "tag_names_mv",
    "tag_values_mv",
    "error_spans_mv",
    "tag_bucket_values_mv",
    "service_bucket_resources_mv",
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

/// The TTL expression out of one table's stored definition. The stored
/// text carries no `DELETE` — 26.3.29.7 drops the default action — so the
/// expression runs to the `SETTINGS` clause.
fn ttl_expr(engine_full: &str, table: &str) -> String {
    let (_, after) = engine_full
        .split_once("TTL ")
        .unwrap_or_else(|| panic!("{table} carries no TTL: {engine_full}"));
    let (expr, _) = after
        .split_once(" SETTINGS")
        .unwrap_or_else(|| panic!("{table}'s TTL has no SETTINGS after it: {engine_full}"));
    expr.to_string()
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
///
/// **`W-15` (issue #587): every field the amended table added or widened
/// carries a non-default value here**, because a compressed default column
/// is nearly free and a populated one is not — so the existing per-row
/// ceiling has never been measured against the amended row shape, and a
/// fixture that left any of them at a default would pass the ceiling while
/// representative rows exceeded it. That includes the **widened integers**
/// and not only the new byte fields: a `kind` outside `0..=5`, a
/// `status_code` outside `0..=255`, a non-zero `end_ns`, a non-zero
/// `scope_dropped_attrs` and a non-zero event time are what price four
/// bytes rather than one.
///
/// **`traces.buckets` is not here, and must not be**: it is a column on
/// `traces`, which `T-S3` prices, not on `trace_landing`.
fn landing_span_block_sql(db: &str, received_ms: i64, base_ns: i64, count: u64) -> String {
    format!(
        "INSERT INTO {db}.trace_landing \
         (received_ms, row_kind, trace_id, span_id, parent_span_id, start_ns, duration_ns, \
          resource_id, name, kind, status_code, service, attrs, scope_attrs, \
          scope_schema_url, scope_dropped_attrs, scope_attrs_other, end_ns, entity_refs, \
          service_type, events, links) \
         SELECT {received_ms}, 0, \
          reinterpretAsFixedString(toUInt128(0x1000 + number)), \
          reinterpretAsFixedString(toUInt64(0x2000 + number)), \
          toFixedString('', 8), {base_ns} + number * 1000000, 1000000, 1, 'GET /api', 300, 300, \
          'checkout', \
          CAST('{{\"http%2Erequest%2Emethod\":\"GET\",\"http%2Eresponse%2Estatus_code\":200}}' AS JSON), \
          CAST('{{}}' AS JSON), \
          'https://opentelemetry.invalid/schemas/1.21.0', \
          1 + number % 7, \
          concat('scope.blob.', leftPad(toString(number), 20, '0')), \
          toUInt64({base_ns} + number * 1000000 + 1000000), \
          concat('entity.refs.', leftPad(toString(number), 28, '0')), \
          'string', \
          [( \
            toUInt64({base_ns} + number * 1000000 + 500000), 'exception', \
            CAST('{{\"exception%2Etype\":\"IOError\"}}' AS JSON), \
            concat('event.blob.', leftPad(toString(number), 20, '0')), 1 \
          )], \
          [( \
            concat('lnk', leftPad(toString(number), 17, '0')), \
            concat('s', leftPad(toString(number), 7, '0')), \
            'congo=t61rcWkgMzE', 256, \
            CAST('{{\"link%2Ekind\":\"follows\"}}' AS JSON), \
            concat('link.blob.', leftPad(toString(number), 20, '0')), 2 \
          )] \
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
          resource_id, name, kind, status_code, service, service_type, attrs, scope_attrs) \
         SELECT {received_ms}, 0, unhex('{trace_hex}'), unhex('{span_hex}'), \
          toFixedString('', 8), {start_ns}, 1000000, 1, 'GET /api', 2, 0, 'checkout', 'string', \
          CAST('{{\"http%2Erequest%2Emethod\":\"GET\",\"http%2Eresponse%2Estatus_code\":200}}' AS JSON), \
          CAST('{{}}' AS JSON)"
    )
}

/// The nine tables and the eight views exist after one `run_init`, and the
/// eight views' sources are recorded against the landing table.
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
        9,
        "the landing table and its eight targets"
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
        8,
        "eight views, eight targets"
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
    // In NAME order, which is `columns`' own `ORDER BY`.
    let want: Vec<(String, String)> = [
        // Issue #587: the stored span-bucket set part 2's span read is a
        // point read over.
        //
        // **The cap is applied in three places, and which ones depends on
        // the engine** — measured on 26.3.29.7, one request each:
        //
        //   CAST(range(5000) AS SimpleAggregateFunction(
        //        groupUniqArrayArray(4096), Array(Int64)))        -> 5000
        //   INSERT of a 5,000-element array into a column of that
        //        type on a plain MergeTree                        -> 5000
        //   the same INSERT on an AggregatingMergeTree            -> 4096
        //
        // So the TYPE caps nothing and the FUNCTION does, and this table
        // is an `AggregatingMergeTree`, so the function named here is
        // applied when the part is written and again when two rows of one
        // key merge. Measured on the same engine: a 10-element array
        // carrying each value twice stores as **five** distinct here and
        // as **ten** on a plain `MergeTree`.
        //
        // `traces_mv` applies `groupUniqArray(4096)(...)` per push beside
        // it. **Both govern a single push's stored array; only the one
        // named here governs a MERGE, and only the one named here decides
        // which function is applied at all** — so a view that emitted
        // `groupArray` would still store a deduplicated, capped set.
        // **This case is therefore the only one that can see the wrong
        // declared function**: `W-17`
        // (`crates/pulsus-write/tests/trace_landing.rs`) reads the stored
        // set back through a deduplicating aggregate and cannot.
        (
            "buckets",
            "SimpleAggregateFunction(groupUniqArrayArray(4096), Array(Int64))",
        ),
        ("day", "Date"),
        ("end_ns", "SimpleAggregateFunction(max, Int64)"),
        // Issue #587: the per-trace LATEST start, which part 2 bounds the
        // resource-day range with. `max`, not `min`: `start_ns` above is
        // the minimum and this is the maximum, and a view that wrote `min`
        // here would narrow that bound silently.
        ("last_start_ns", "SimpleAggregateFunction(max, Int64)"),
        // Issue #591 part 1, decision 4: the root is ONE span, the least
        // `(not a root, start_ns, span_id, service, name)`.
        (
            "root",
            "SimpleAggregateFunction(min, Tuple(UInt8, Int64, FixedString(8), String, String))",
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
    assert_eq!(got, want, "the per-trace table's nine declarations");

    drop_database(&client, db).await;
}

/// **T-S5.** Every column of the eight target tables carries a compression
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
        70,
        "the eight target tables' own column count (28 + 8 + 8 + 2 + 4 + 12 \
         + 5 + 3), so the codec check below has a non-empty domain. Issue #587 \
         added four columns to `spans`, two to `traces` and one to \
         `resources`; issue #589 added one to `spans`; issue #591 part 1 made \
         `traces`' two root columns one; issue #601 part 4b added \
         `error_spans` and `tag_bucket_values`; part 4c added \
         `service_bucket_resources`"
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

/// **T-S5b.** The exact codec spelling of every column of the eight target
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
const TARGET_TABLE_CODECS: [(&str, &str, &str); 70] = [
    ("error_spans", "trace_id", "CODEC(ZSTD(1))"),
    ("error_spans", "span_id", "CODEC(ZSTD(1))"),
    ("error_spans", "start_ns", "CODEC(Delta(8), ZSTD(1))"),
    ("error_spans", "duration_ns", "CODEC(T64, ZSTD(1))"),
    ("error_spans", "end_ns", "CODEC(Delta(8), ZSTD(1))"),
    ("error_spans", "service", "CODEC(ZSTD(1))"),
    ("error_spans", "service_type", "CODEC(ZSTD(1))"),
    ("error_spans", "resource_id", "CODEC(ZSTD(1))"),
    ("error_spans", "name", "CODEC(ZSTD(1))"),
    ("error_spans", "kind", "CODEC(ZSTD(1))"),
    ("error_spans", "status_code", "CODEC(ZSTD(1))"),
    ("error_spans", "status_message", "CODEC(ZSTD(1))"),
    ("resources", "day", "CODEC(ZSTD(1))"),
    ("resources", "resource_id", "CODEC(ZSTD(1))"),
    ("resources", "service", "CODEC(ZSTD(1))"),
    ("resources", "attrs", "CODEC(ZSTD(1))"),
    ("resources", "attrs_other", "CODEC(ZSTD(1))"),
    ("resources", "dropped_attrs", "CODEC(ZSTD(1))"),
    ("resources", "schema_url", "CODEC(ZSTD(1))"),
    ("resources", "entity_refs", "CODEC(ZSTD(1))"),
    ("service_bucket_resources", "bucket", "CODEC(ZSTD(1))"),
    ("service_bucket_resources", "service", "CODEC(ZSTD(1))"),
    ("service_bucket_resources", "resource_id", "CODEC(ZSTD(1))"),
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
    ("spans", "scope_schema_url", "CODEC(ZSTD(1))"),
    ("spans", "scope_dropped_attrs", "CODEC(ZSTD(1))"),
    ("spans", "scope_attrs_other", "CODEC(ZSTD(1))"),
    ("spans", "end_ns", "CODEC(Delta(8), ZSTD(1))"),
    ("spans", "service_type", "CODEC(ZSTD(1))"),
    ("tag_bucket_values", "bucket", "CODEC(ZSTD(1))"),
    ("tag_bucket_values", "service", "CODEC(ZSTD(1))"),
    ("tag_bucket_values", "scope", "CODEC(ZSTD(1))"),
    ("tag_bucket_values", "key", "CODEC(ZSTD(1))"),
    ("tag_bucket_values", "vals", "CODEC(ZSTD(1))"),
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
    ("traces", "root", "CODEC(ZSTD(1))"),
    ("traces", "services", "CODEC(ZSTD(1))"),
    ("traces", "last_start_ns", "CODEC(Delta(8), ZSTD(1))"),
    ("traces", "buckets", "CODEC(ZSTD(1))"),
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
        "the eight target tables' column count"
    );
    for (got, (table, name, codec)) in seen.iter().zip(TARGET_TABLE_CODECS) {
        assert_eq!(got.table, table, "table order");
        assert_eq!(got.name, name, "{table}'s column order");
        assert_eq!(got.compression_codec, codec, "{table}.{name}'s codec");
    }

    drop_database(&client, db).await;
}

/// **T-S6.** Applying the schema to a database that already holds it fails
/// at the first materialized view: the file holds only `CREATE`
/// statements, so nothing in it restates an object, and
/// `schema/schema.sh` drops the database before applying it. The columns
/// the first application built are untouched by the failed second one.
#[tokio::test]
async fn applying_the_schema_to_a_built_database_fails() {
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

    let second = run_init(&client, &ctx).await;

    let mut after = Vec::new();
    for table in WRITE_PATH_TABLES {
        after.push(columns(&client, db, table).await);
    }
    drop_database(&client, db).await;
    let err = second.expect_err("a second application to a built database must fail");
    assert!(
        err.to_string().contains("already exists"),
        "the second application fails on an existing object: {err}"
    );
    assert_eq!(
        after, before,
        "the failed second application moved a column"
    );
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

/// The configured window reaches **all nine** write-path tables, read off
/// each table's own stored definition rather than off the statements that
/// set it.
#[tokio::test]
async fn dedup_settings_reach_all_nine_trace_tables() {
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

    // The clamp is in the stored text of all three retained tables.
    for (table, column) in [
        ("spans", "intDiv(start_ns, 1000000000)"),
        ("traces", "intDiv(last_start_ns, 1000000000)"),
        ("resources", "((toUInt32(day) + 1) * 86400)"),
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
            ttl_expr(&engine_full(&client, db, "resources").await, "resources")
        ),
    )
    .await;
    assert_eq!(
        from_day, from_ns,
        "the resource row's Date form clamps to the same instant as the span's"
    );

    let from_last_start = scalar(
        &client,
        &format!(
            "SELECT toString({}) AS s \
             FROM (SELECT toInt64(4294943999000000000) AS last_start_ns)",
            ttl_expr(&engine_full(&client, db, "traces").await, "traces")
        ),
    )
    .await;
    assert_eq!(
        from_last_start, from_ns,
        "the per-trace row's TTL clamps to the same instant as the span's"
    );

    drop_database(&client, db).await;
}

/// **T-R3.** Every resource row and every per-trace row outlives the spans
/// it covers: the gap, the dependency's TTL minus the span's, is never
/// negative.
///
/// All three expressions are read off the server's stored definitions. The
/// resource case runs every second of 2026-10-01 .. 2026-10-07, each at the
/// last nanosecond of that second, against the resource row of the span's
/// own UTC day. The per-trace case runs one block per minute of 2026-10-04,
/// whose earliest span files the row under its day, with the block's latest
/// span up to three days later.
///
/// `min = 1` for resources is the row outliving its day's last span by one
/// second; one day less gives `-86399`. `max = 86400` is a span at the
/// start of its day, which the day-grained row must also cover. The gaps do
/// not depend on the retention.
#[tokio::test]
async fn every_resource_and_trace_row_outlives_the_spans_it_covers() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_ttlgap");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    let mut ctx = test_ctx(db);
    ctx.retention_days = 1;
    run_init(&client, &ctx).await.expect("run_init");

    let span_ttl = ttl_expr(&engine_full(&client, db, "spans").await, "spans");
    let resource_ttl = ttl_expr(&engine_full(&client, db, "resources").await, "resources");
    let trace_ttl = ttl_expr(&engine_full(&client, db, "traces").await, "traces");

    let resource_gap = scalar(
        &client,
        &format!(
            "SELECT toString((min(g), max(g), count())) AS s FROM ( \
               SELECT toInt64(toUInt32({resource_ttl})) - toInt64(toUInt32({span_ttl})) AS g \
               FROM (SELECT start_ns, toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') AS day \
                     FROM (SELECT toInt64((1790812800 + number) * 1000000000 + 999999999) \
                                  AS start_ns \
                           FROM numbers(604800))))"
        ),
    )
    .await;
    assert_eq!(
        resource_gap, "(1,86400,604800)",
        "(min, max, count) of a resource row's TTL minus its span's, in seconds"
    );

    let trace_gap = scalar(
        &client,
        &format!(
            "SELECT toString((min(g), max(g), count())) AS s FROM ( \
               SELECT toInt64(toUInt32({trace_ttl})) - toInt64(toUInt32({span_ttl})) AS g \
               FROM (SELECT toDate(fromUnixTimestamp64Nano(s1), 'UTC') AS day, \
                            s2 AS last_start_ns, s2 AS start_ns \
                     FROM (SELECT toInt64((1791072000 + a.number * 60) * 1000000000) AS s1, \
                                  s1 + toInt64(b.number * 60 * 1000000000) AS s2 \
                           FROM numbers(1440) AS a CROSS JOIN numbers(4320) AS b)))"
        ),
    )
    .await;
    assert_eq!(
        trace_gap, "(0,0,6220800)",
        "(min, max, count) of a per-trace row's TTL minus its latest span's, in seconds"
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

/// **T-S1, whole.** Every table of the database that is not a view and not
/// of the logs or metrics family is one of the ten trace tables, and every
/// column of theirs whose type can hold text is listed below as holding
/// attribute values or as not holding them. The columns that hold
/// attribute values are exactly [`ATTRIBUTE_COLUMNS`].
#[tokio::test]
async fn the_attribute_columns_are_exactly_these() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_attrcols");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let trace_tables: BTreeSet<String> = names(
        &client,
        &format!(
            "SELECT name FROM system.tables WHERE database = '{db}' \
             AND engine != 'MaterializedView' \
             AND NOT startsWith(name, 'log_') AND NOT startsWith(name, 'metric_') \
             ORDER BY name"
        ),
    )
    .await
    .into_iter()
    .collect();
    let want_tables: BTreeSet<String> = TRACE_TABLES.iter().map(|s| (*s).to_string()).collect();

    let got: BTreeSet<String> = names(
        &client,
        &format!(
            "SELECT concat(table, '.', name) AS name FROM system.columns \
             WHERE database = '{db}' AND table IN ({}) \
               AND match(type, 'String|JSON|Map|Tuple|Array|AggregateFunction') \
             ORDER BY name",
            trace_tables
                .iter()
                .map(|t| format!("'{t}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    )
    .await
    .into_iter()
    .collect();
    let attrs: BTreeSet<String> = ATTRIBUTE_COLUMNS.iter().map(|s| (*s).to_string()).collect();
    let others: BTreeSet<String> = OTHER_TEXT_COLUMNS
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    let want: BTreeSet<String> = attrs.union(&others).cloned().collect();
    let unlisted: Vec<&String> = got.difference(&want).collect();
    let absent: Vec<&String> = want.difference(&got).collect();
    drop_database(&client, db).await;

    assert_eq!(trace_tables, want_tables, "the trace tables");
    assert!(
        attrs.is_disjoint(&others),
        "a column is listed twice: {:?}",
        attrs.intersection(&others).collect::<Vec<_>>()
    );
    assert!(
        unlisted.is_empty() && absent.is_empty(),
        "unlisted columns {unlisted:?}; listed but absent {absent:?}"
    );
}

/// The ten trace tables: the nine on the write path and `service_edges`.
const TRACE_TABLES: [&str; 10] = [
    "error_spans",
    "resources",
    "service_bucket_resources",
    "service_edges",
    "spans",
    "tag_bucket_values",
    "tag_names",
    "tag_values",
    "trace_landing",
    "traces",
];

/// The columns that hold attribute values.
const ATTRIBUTE_COLUMNS: [&str; 17] = [
    "resources.attrs",
    "resources.attrs_other",
    "spans.attrs",
    "spans.attrs_other",
    "spans.events",
    "spans.links",
    "spans.scope_attrs",
    // Issue #587 row 3: the scope's own keys whose values no JSON path
    // can hold.
    "spans.scope_attrs_other",
    // Issue #601 part 4b: each 5-minute bucket's span-attribute values.
    "tag_bucket_values.vals",
    "tag_values.value",
    "trace_landing.attrs",
    "trace_landing.attrs_other",
    "trace_landing.events",
    "trace_landing.links",
    "trace_landing.scope_attrs",
    "trace_landing.scope_attrs_other",
    "trace_landing.tag_value",
];

/// Every other text-capable column of the ten tables. `entity_refs`
/// carries attribute **keys** that must exist in the resource's
/// attributes, not attribute values.
const OTHER_TEXT_COLUMNS: [&str; 56] = [
    "error_spans.name",
    "error_spans.service",
    "error_spans.service_type",
    "error_spans.span_id",
    "error_spans.status_message",
    "error_spans.trace_id",
    "resources.entity_refs",
    "resources.schema_url",
    "resources.service",
    "service_bucket_resources.service",
    "service_edges.client",
    "service_edges.conn_type",
    "service_edges.q",
    "service_edges.server",
    "spans.name",
    "spans.parent_span_id",
    "spans.scope_name",
    "spans.scope_schema_url",
    "spans.scope_version",
    "spans.service",
    "spans.service_type",
    "spans.span_id",
    "spans.status_message",
    "spans.trace_id",
    "spans.trace_state",
    "tag_bucket_values.key",
    "tag_bucket_values.scope",
    "tag_bucket_values.service",
    "tag_names.key",
    "tag_names.scope",
    "tag_values.key",
    "tag_values.scope",
    "tag_values.val_type",
    "trace_landing.entity_refs",
    "trace_landing.name",
    "trace_landing.parent_span_id",
    "trace_landing.schema_url",
    "trace_landing.scope_name",
    "trace_landing.scope_schema_url",
    "trace_landing.scope_version",
    "trace_landing.service",
    "trace_landing.service_type",
    "trace_landing.span_id",
    "trace_landing.status_message",
    "trace_landing.tag_key",
    "trace_landing.tag_scope",
    "trace_landing.tag_type",
    "trace_landing.trace_id",
    "trace_landing.trace_state",
    "traces.buckets",
    "traces.end_ns",
    "traces.last_start_ns",
    "traces.root",
    "traces.services",
    "traces.start_ns",
    "traces.trace_id",
];

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
    // **Recorded, because issue #587 populated every added and widened
    // field and the figure had never been measured against that row
    // shape.** The ceiling is unchanged; if a later row shape exceeds it
    // that is a finding for the owner, not a licence to raise it.
    eprintln!("the landing table holds {per_span} B/row over 100 populated rows (recorded)");
    assert!(
        per_span < 200,
        "the landing table holds {per_span} B/span, over the 200 B/span ceiling \
         this case guards ({bytes} bytes over 100 spans)"
    );

    drop_database(&client, db).await;
}

/// Issue #595: `spans` carries exactly three skip indexes, a
/// `bloom_filter(0.01)` of granularity 1 over `name`, `resource_id` and
/// `service`, and no other table of the five carries one.
#[tokio::test]
async fn the_span_table_carries_three_bloom_indexes() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_span_indexes");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");
    let got = names(
        &client,
        &format!(
            "SELECT concat(table, ' ', name, ' ', expr, ' ', type_full, ' ', toString(granularity)) AS name \
             FROM system.data_skipping_indices \
             WHERE database = '{db}' AND table IN ({TARGET_TABLE_LIST}) ORDER BY table, name"
        ),
    )
    .await;
    assert_eq!(
        got,
        vec![
            "spans idx_name name bloom_filter(0.01) 1",
            "spans idx_resource resource_id bloom_filter(0.01) 1",
            "spans idx_service service bloom_filter(0.01) 1",
        ],
        "the span table's skip indexes"
    );
    drop_database(&client, db).await;
}

/// Issue #595 part 2: each configured attribute is one bloom-filter index
/// on `spans`, numbered in list order, over the expression
/// `pulsus_clickhouse::json_column` renders for its scope.
#[tokio::test]
async fn the_span_table_carries_one_index_per_named_attribute() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_attr_indexes");
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    let ctx = SchemaParams {
        trace_indexed_attributes: vec!["span.app.request.id".into(), "event.exception.type".into()],
        ..test_ctx(db)
    };
    run_init(&client, &ctx).await.expect("run_init");
    let got = names(
        &client,
        &format!(
            "SELECT concat(name, ' ', expr, ' ', type_full, ' ', toString(granularity)) AS name \
             FROM system.data_skipping_indices \
             WHERE database = '{db}' AND table = 'spans' AND name LIKE 'idx_attr_%' ORDER BY name"
        ),
    )
    .await;
    drop_database(&client, db).await;
    assert_eq!(
        got,
        vec![
            "idx_attr_1 arrayMap(x -> ifNull(x, ''), arrayConcat(attrs.`app%2Erequest%2Eid`.:`Array(Nullable(String))`, [attrs.`app%2Erequest%2Eid`.:String])) bloom_filter(0.01) 1".to_string(),
            "idx_attr_2 arrayMap(x -> ifNull(x, ''), arrayConcat(events.attrs.`exception%2Etype`.:String, arrayFlatten(events.attrs.`exception%2Etype`.:`Array(Nullable(String))`))) bloom_filter(0.01) 1".to_string(),
        ],
        "the named attributes' indexes"
    );
}

/// One object's `system.tables` row, as T2 reads it.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ShapeRow {
    engine: String,
    sorting_key: String,
    partition_key: String,
    engine_full: String,
    as_select: String,
}

async fn shape(client: &ChClient, db: &str, name: &str) -> ShapeRow {
    let sql = format!(
        "SELECT engine, sorting_key, partition_key, engine_full, as_select \
         FROM system.tables WHERE database = '{db}' AND name = '{name}'"
    );
    let mut stream = client
        .query_stream::<ShapeRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("shape query failed: {e}\nSQL:\n{sql}"));
    let row = stream.next().await;
    assert!(row.is_some(), "{db}.{name} does not exist");
    row.expect("one row").expect("decode ShapeRow")
}

/// **T2 (issue #601 part 4b).** The two derived tables, their wrapper and
/// their views as the design writes them, read back off the server.
///
/// Single-node always. With `PULSUS_TEST_CH_CLUSTER` set — this suite's
/// connection then points at a node of that cluster — the clustered schema
/// is checked too: the replica paths, the wrapper's sharding key and the
/// view writing `error_spans` through it.
#[tokio::test]
async fn the_two_4b_tables_have_the_planned_shape() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let mut modes = vec![None];
    if let Ok(cluster) = std::env::var("PULSUS_TEST_CH_CLUSTER") {
        modes.push(Some(cluster));
    }
    for cluster in modes {
        let tag = if cluster.is_some() { "c" } else { "s" };
        let db = &pulsus_testkit::test_db(&format!("pulsus_trace_landing_it_4b_{tag}"));
        let on = cluster
            .as_ref()
            .map(|c| format!(" ON CLUSTER {c} SYNC"))
            .unwrap_or_default();
        exec(&client, &format!("DROP DATABASE IF EXISTS {db}{on}")).await;
        let ctx = SchemaParams {
            cluster: cluster.clone(),
            ..test_ctx(db)
        };
        run_init(&client, &ctx).await.expect("run_init");
        let (dedup, replicated) = if cluster.is_some() {
            (
                format!(
                    "replicated_deduplication_window = 10000, \
                     replicated_deduplication_window_seconds = {}",
                    pulsus_schema::DEDUP_WINDOW_SECONDS
                ),
                "Replicated",
            )
        } else {
            (
                "non_replicated_deduplication_window = 10000".to_string(),
                "",
            )
        };

        let tbv = shape(&client, db, "tag_bucket_values").await;
        assert_eq!(tbv.engine, format!("{replicated}AggregatingMergeTree"));
        assert_eq!(tbv.sorting_key, "service, scope, key, bucket");
        assert_eq!(
            tbv.partition_key,
            "toDate(fromUnixTimestamp64Nano(bucket * 300000000000), 'UTC')"
        );
        assert_eq!(
            ttl_expr(&tbv.engine_full, "tag_bucket_values"),
            "toDateTime(least(intDiv(bucket * 300000000000, 1000000000) + (7 * 86400), 4294967295))"
        );
        assert!(
            tbv.engine_full.ends_with(&format!(
                "SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, {dedup}"
            )),
            "tag_bucket_values' settings: {}",
            tbv.engine_full
        );

        let es = shape(&client, db, "error_spans").await;
        assert_eq!(es.engine, format!("{replicated}ReplacingMergeTree"));
        assert_eq!(
            es.sorting_key,
            "intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind"
        );
        assert_eq!(
            es.partition_key,
            "toDate(fromUnixTimestamp64Nano(start_ns), 'UTC')"
        );
        assert_eq!(
            ttl_expr(&es.engine_full, "error_spans"),
            "toDateTime(least(intDiv(start_ns, 1000000000) + (7 * 86400), 4294967295))"
        );
        assert!(
            es.engine_full.ends_with(&format!(
                "SETTINGS ttl_only_drop_parts = 1, index_granularity = 2048, \
                 min_columns_to_activate_adaptive_write_buffer = 1, {dedup}"
            )),
            "error_spans' settings: {}",
            es.engine_full
        );

        let es_mv = shape(&client, db, "error_spans_mv").await;
        assert!(
            es_mv.as_select.ends_with(&format!(
                "FROM {db}.trace_landing WHERE (row_kind = 0) AND (status_code = 2)"
            )),
            "error_spans_mv: {}",
            es_mv.as_select
        );
        let tbv_mv = shape(&client, db, "tag_bucket_values_mv").await;
        assert!(
            tbv_mv.as_select.ends_with(
                "WHERE (row_kind = 0) AND (service_type = 'string') \
                 GROUP BY bucket, service, scope, key"
            ),
            "tag_bucket_values_mv: {}",
            tbv_mv.as_select
        );

        if let Some(cluster) = &cluster {
            assert!(
                es.engine_full.starts_with(&format!(
                    "ReplicatedReplacingMergeTree('/clickhouse/tables/{{shard}}/{db}.error_spans', '{{replica}}')"
                )),
                "error_spans' replica path: {}",
                es.engine_full
            );
            assert!(
                tbv.engine_full.starts_with(&format!(
                    "ReplicatedAggregatingMergeTree('/clickhouse/tables/all/{db}.tag_bucket_values', '{{replica}}')"
                )),
                "tag_bucket_values' replica path: {}",
                tbv.engine_full
            );
            let wrapper = shape(&client, db, "error_spans_dist").await;
            assert_eq!(
                wrapper.engine_full,
                format!(
                    "Distributed('{cluster}', '{db}', 'error_spans', cityHash64(trace_id)) \
                     SETTINGS fsync_after_insert = 1, fsync_directories = 1"
                )
            );
            let mv = scalar(
                &client,
                &format!(
                    "SELECT create_table_query AS s FROM system.tables \
                     WHERE database = '{db}' AND name = 'error_spans_mv'"
                ),
            )
            .await;
            assert!(
                mv.contains(&format!("TO {db}.error_spans_dist ")),
                "error_spans_mv writes through the wrapper: {mv}"
            );
        } else {
            assert!(
                shape_absent(&client, db, "error_spans_dist").await,
                "single-node has no wrapper"
            );
        }
        exec(&client, &format!("DROP DATABASE IF EXISTS {db}{on}")).await;
    }
}

/// **T6 (issue #601 part 4c).** `service_bucket_resources` and its view
/// as the design writes them, read back off the server: a
/// `ReplacingMergeTree` ordered for the service-narrowed read, partitioned
/// and expired by its bucket, filled from the landing table's span rows
/// whose `service.name` is a string.
///
/// Single-node always. With `PULSUS_TEST_CH_CLUSTER` set the clustered
/// schema is checked too: one cluster-wide replica set, and no wrapper.
#[tokio::test]
async fn the_4c_table_has_the_planned_shape() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let mut modes = vec![None];
    if let Ok(cluster) = std::env::var("PULSUS_TEST_CH_CLUSTER") {
        modes.push(Some(cluster));
    }
    for cluster in modes {
        let tag = if cluster.is_some() { "c" } else { "s" };
        let db = &pulsus_testkit::test_db(&format!("pulsus_trace_landing_it_4c_{tag}"));
        let on = cluster
            .as_ref()
            .map(|c| format!(" ON CLUSTER {c} SYNC"))
            .unwrap_or_default();
        exec(&client, &format!("DROP DATABASE IF EXISTS {db}{on}")).await;
        let ctx = SchemaParams {
            cluster: cluster.clone(),
            ..test_ctx(db)
        };
        run_init(&client, &ctx).await.expect("run_init");
        let (dedup, replicated) = if cluster.is_some() {
            (
                format!(
                    "replicated_deduplication_window = 10000, \
                     replicated_deduplication_window_seconds = {}",
                    pulsus_schema::DEDUP_WINDOW_SECONDS
                ),
                "Replicated",
            )
        } else {
            (
                "non_replicated_deduplication_window = 10000".to_string(),
                "",
            )
        };

        let sbr = shape(&client, db, "service_bucket_resources").await;
        assert_eq!(sbr.engine, format!("{replicated}ReplacingMergeTree"));
        assert_eq!(sbr.sorting_key, "service, bucket, resource_id");
        assert_eq!(
            sbr.partition_key,
            "toDate(fromUnixTimestamp64Nano(bucket * 300000000000), 'UTC')"
        );
        assert_eq!(
            ttl_expr(&sbr.engine_full, "service_bucket_resources"),
            "toDateTime(least(intDiv(bucket * 300000000000, 1000000000) + (7 * 86400), 4294967295))"
        );
        assert!(
            sbr.engine_full.ends_with(&format!(
                "SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, {dedup}"
            )),
            "service_bucket_resources' settings: {}",
            sbr.engine_full
        );
        let mv = shape(&client, db, "service_bucket_resources_mv").await;
        assert!(
            mv.as_select.contains(&format!("FROM {db}.trace_landing "))
                && mv.as_select.ends_with(
                    "WHERE (row_kind = 0) AND (service_type = 'string') \
                     GROUP BY bucket, service, resource_id"
                ),
            "service_bucket_resources_mv: {}",
            mv.as_select
        );

        if cluster.is_some() {
            assert!(
                sbr.engine_full.starts_with(&format!(
                    "ReplicatedReplacingMergeTree('/clickhouse/tables/all/{db}.service_bucket_resources', '{{replica}}')"
                )),
                "service_bucket_resources' replica path: {}",
                sbr.engine_full
            );
        }
        assert!(
            shape_absent(&client, db, "service_bucket_resources_dist").await,
            "no wrapper: one replica set, read locally"
        );
        exec(&client, &format!("DROP DATABASE IF EXISTS {db}{on}")).await;
    }
}

async fn shape_absent(client: &ChClient, db: &str, name: &str) -> bool {
    count(
        client,
        &format!(
            "SELECT count() AS n FROM system.tables WHERE database = '{db}' AND name = '{name}'"
        ),
    )
    .await
        == 0
}

/// One raw HTTP request to ClickHouse's own interface: the status and the
/// body, whatever the status is. `ChClient` turns a failed insert into an
/// error value; the case below is about what the CALLER is told, so it
/// reads the wire.
fn clickhouse_http_post(body: &str) -> (u16, String) {
    use std::io::{Read, Write};
    let host = std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string());
    let http = test_config().http_port;
    let mut stream = std::net::TcpStream::connect((host.as_str(), http)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .expect("read timeout");
    let request = format!(
        "POST / HTTP/1.0\r\nHost: {host}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).expect("write request");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read response");
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no HTTP status in {text:?}"));
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

/// The `Code: <n>` number in a ClickHouse error body.
fn error_code(body: &str) -> Option<u32> {
    let at = body.find("Code: ")? + "Code: ".len();
    body[at..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()
}

/// The `docs/schemas.md` paragraph beginning with the failing-view lead,
/// up to the next blank line, every whitespace run collapsed.
fn failing_view_paragraph() -> Option<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/schemas.md");
    let doc = std::fs::read_to_string(&path).expect("read docs/schemas.md");
    let lead = "**What a failing view leaves behind.**";
    let mut lines = doc.lines();
    let first = lines.by_ref().find(|l| l.trim_start().starts_with(lead))?;
    let mut out = vec![first.to_string()];
    for l in lines {
        if l.trim().is_empty() {
            break;
        }
        out.push(l.to_string());
    }
    Some(
        out.join("\n")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
    )
}

/// A view that throws on a landing block fails the insert, and
/// `docs/schemas.md` §4.1 says what the caller is told. Only the outcomes
/// that held on every one of the design record's 300 trials are asserted
/// — the status, the code and the throwing view's empty target. The other
/// targets' counts are printed, never asserted: they vary run to run
/// (`docs/traceql-schema-migration.md` §6.3).
#[tokio::test]
async fn a_throwing_view_fails_the_landing_insert_and_the_documents_say_so() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_trace_landing_it_throwing_view");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let second_span = "0000000000000002";
    exec(
        &client,
        &format!(
            "CREATE TABLE {db}.throw_target (trace_id FixedString(16)) \
             ENGINE = MergeTree ORDER BY trace_id"
        ),
    )
    .await;
    exec(
        &client,
        &format!(
            "CREATE MATERIALIZED VIEW {db}.throw_mv TO {db}.throw_target AS \
             SELECT trace_id FROM {db}.trace_landing \
             WHERE throwIf(span_id = unhex('{second_span}'), 'a throwing view') = 0"
        ),
    )
    .await;

    // Two kind-0 rows in one statement, so one block: the second throws.
    let received_ms = now_unix_millis();
    let start_ns = received_ms * 1_000_000 - 3_600_000_000_000;
    let insert = format!(
        "INSERT INTO {db}.trace_landing \
         (received_ms, row_kind, trace_id, span_id, parent_span_id, start_ns, duration_ns, \
          resource_id, name, kind, status_code, service, service_type, attrs, scope_attrs) \
         SELECT {received_ms}, 0, unhex('00000000000000000000000000000560'), \
          unhex(if(number = 0, '0000000000000001', '{second_span}')), \
          toFixedString('', 8), {start_ns} + number * 1000, 1000, 1, 'op', 2, 2, 'svc', \
          'string', CAST('{{}}' AS JSON), CAST('{{}}' AS JSON) \
         FROM numbers(2)"
    );
    let (status, body) = clickhouse_http_post(&insert);
    let code = error_code(&body);

    let mut printed = Vec::new();
    for table in ["trace_landing", "spans", "traces", "error_spans"] {
        let n = count(&client, &format!("SELECT count() AS n FROM {db}.{table}")).await;
        printed.push(format!("{table}={n}"));
    }
    let throw_target = count(
        &client,
        &format!("SELECT count() AS n FROM {db}.throw_target"),
    )
    .await;
    eprintln!(
        "throwing view: status {status}, code {code:?}, {}, throw_target={throw_target}",
        printed.join(", ")
    );

    let mut failures = Vec::new();
    if status != 500 {
        failures.push(format!("status {status}, expected 500; body {body:?}"));
    }
    if code != Some(395) {
        failures.push(format!("code {code:?}, expected 395; body {body:?}"));
    }
    if throw_target != 0 {
        failures.push(format!(
            "throw_target holds {throw_target} rows, expected 0"
        ));
    }
    let sentence = format!(
        "The insert fails: the caller receives `HTTP {status}` with `Code: {code}`,",
        code = code.unwrap_or(0)
    );
    match failing_view_paragraph() {
        None => failures.push(
            "docs/schemas.md has no paragraph beginning \
             `**What a failing view leaves behind.**`"
                .to_string(),
        ),
        Some(p) => {
            if !p.contains(&sentence) {
                failures.push(format!(
                    "the failing-view paragraph does not say {sentence:?}:\n{p}"
                ));
            }
        }
    }

    drop_database(&client, db).await;
    assert!(
        failures.is_empty(),
        "{} failure(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}
