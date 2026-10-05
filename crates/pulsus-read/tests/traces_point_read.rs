//! The trace fetch's engine-level gates (issue #587): the eight binary
//! fields' decoding, each statement's reported column types, the capped
//! bucket union across day rows, the `final = 1` collapse, and the plain
//! fetch over a seeded corpus.
//!
//! **Every case here is below the HTTP surface and below the assembler**,
//! which is the whole reason the file exists: three of the quantities
//! these cases move are invisible from a response body.
//!
//! * **the eight byte fields** — a field left on serde's sequence path
//!   fails with the driver's `SchemaMismatch` naming the column, and a
//!   nested element left as a derived struct fails the whole row. Two
//!   distinct messages for two defect classes, where a live HTTP case
//!   would only say "the fetch broke";
//! * **the reported header types** — read with no row read at all;
//! * **the capped union** — the number of elements the statement's own
//!   aggregate produces, which no response exposes;
//! * **the collapse** — the number of rows the ENGINE returns. Asserting
//!   "4 rows stored, 2 spans in the response" through HTTP passes with
//!   `final = 1` removed, because the assembler's `(span_id, kind)` map
//!   collapses the duplicates in Rust.
//!
//! **What this file no longer carries, and why.** Issue #55's
//! `EXPLAIN indexes = 1` gate pinned the OLD point read's primary-index
//! behaviour on `trace_spans`. No production statement looks like that
//! after this change, and the three new statements' index story is argued
//! from the catalogue's sort keys and the statements' literal predicates
//! rather than asserted by a case: a mark count is a property of parts, so
//! the instrument needs a populated table, and the figures this design
//! rests on come from the measurement corpus rather than from a test
//! fixture. The gate went with the statement it gated.
//!
//! Live half gated behind `PULSUS_TEST_CLICKHOUSE=1`, same podman harness
//! as the other live suites.

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_read::traces::spans::fetch::{BUCKET_NS, BUCKET_SET_CAP};
use pulsus_read::traces::spans::predicate::{
    PredicateCtx, compile_span_predicate_in, span_membership_sql,
};
use pulsus_read::traces::spans::rows::{FetchRoute, FetchWindow};
use pulsus_read::traces::window_sql::WindowSql;
use pulsus_read::{TraceEngine, TraceReadConfig};
use pulsus_schema::{RenderCtx, SchemaParams};
use pulsus_schema_testkit::run_init;

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
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

fn test_ctx(db: &str) -> SchemaParams {
    RenderCtx::for_tests(db)
}

/// A connection on the built-in `default` database, for DDL and for the
/// `system` reads.
async fn admin() -> ChClient {
    ChClient::new(test_config()).await.expect("connect admin")
}

/// A connection on `db`.
async fn data(db: &str) -> ChClient {
    let mut cfg = test_config();
    cfg.database = db.to_string();
    ChClient::new(cfg).await.expect("connect data client")
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("statement failed: {e}\nSQL:\n{sql}"));
}

/// Creates a nonce'd database with the migrations applied, and returns its
/// name.
///
/// **Takes the composed name**, not the bare one: the per-checkout prefix
/// comes from `pulsus_testkit::test_db`, and the source scan that checks
/// every live-test object name goes through it reads the call site, not
/// this helper.
async fn fresh_db(db: String) -> String {
    let admin = admin().await;
    exec(&admin, &format!("DROP DATABASE IF EXISTS {db}")).await;
    run_init(&admin, &test_ctx(&db)).await.expect("run_init");
    hold_the_fetch_tables(&admin, &db).await;
    db
}

/// A fixed past instant and a delete TTL are not compatible without this.
///
/// **Most fixtures here are anchored on a CALENDAR instant** rather than
/// on the clock, because the derived values each case asserts — the UTC
/// days a row files under, the buckets a span occupies — were computed
/// from those literals. The three fetch tables carry a delete TTL at the
/// configured retention with `ttl_only_drop_parts = 1`, and **a part
/// whose rows are all already expired is dropped by a BACKGROUND
/// operation**: measured on this server, a single row dated 2023-11-14
/// inserted into a table with that TTL was gone within three seconds. The
/// case would then assert against an empty table.
///
/// `SYSTEM STOP MERGES` is what holds them, because a TTL part drop is a
/// merge. **Table-scoped, which is the whole of why no restart is
/// needed**: the scoped form resolves a storage object, so the stop is
/// held against the table and dropping the database destroys the holder.
/// The argument-less form is server-wide and no drop undoes it.
///
/// It also gives the one case that asserts a PHYSICAL row count a stable
/// state to assert — but that is a second reason, and this one applies to
/// every case in the file.
async fn hold_the_fetch_tables(admin: &ChClient, db: &str) {
    for table in ["spans", "traces", "resources"] {
        exec(admin, &format!("SYSTEM STOP MERGES {db}.{table}")).await;
    }
}

async fn drop_db(db: &str) {
    exec(&admin().await, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
struct CountRow {
    n: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
struct U32Row {
    n: u32,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TextRow {
    s: String,
}

/// `DESCRIBE`'s first two columns. The statement returns seven, so the
/// row carries all of them positionally and the case reads the first two.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct DescribeRow {
    name: String,
    r#type: String,
    default_type: String,
    default_expression: String,
    comment: String,
    codec_expression: String,
    ttl_expression: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TraceStateRow {
    buckets: Vec<i64>,
    start_ns: i64,
    last_start_ns: i64,
}

async fn one_row<R>(client: &ChClient, sql: &str) -> R
where
    R: pulsus_clickhouse::ChRow,
{
    let mut stream = client
        .query_stream::<R>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    stream
        .next()
        .await
        .unwrap_or_else(|| panic!("no row for:\n{sql}"))
        .unwrap_or_else(|e| panic!("decode row: {e}\nSQL:\n{sql}"))
}

async fn scalar_u64(client: &ChClient, sql: &str) -> u64 {
    one_row::<CountRow>(client, sql).await.n
}

async fn scalar_u32(client: &ChClient, sql: &str) -> u32 {
    one_row::<U32Row>(client, sql).await.n
}

/// A `TraceReadConfig` pointing at the three fetch tables by their bare
/// names, with the production defaults for every budget.
fn fetch_config() -> TraceReadConfig {
    TraceReadConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
        edges_table: "trace_edges".to_string(),
        recent_table: "trace_recent".to_string(),
        errors_table: "trace_error_spans".to_string(),
        spans_v2_table: "spans".to_string(),
        traces_table: "traces".to_string(),
        resources_table: "resources".to_string(),
        max_candidates: 100_000,
        scan_budget_rows: 50_000_000,
        event_set_max_values: 1_000_000,
        max_series: 1_000,
        generator_max_memory_bytes: 536_870_912,
        distributed: false,
        skip_unavailable_shards: false,
    }
}

async fn engine_on(db: &str, prefix: &str) -> TraceEngine {
    TraceEngine::new(data(db).await, fetch_config()).with_statement_prefix(prefix)
}

/// `SYSTEM FLUSH LOGS`, then poll until `query_id` appears — the
/// `query_log` is asynchronous and asserting on it without a flush and a
/// wait is a known flake here.
async fn recorded_query(admin: &ChClient, db: &str, query_id: &str) -> Option<String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        exec(admin, "SYSTEM FLUSH LOGS").await;
        let sql = format!(
            "SELECT query AS s FROM system.query_log \
             WHERE query_id = '{query_id}' AND type = 'QueryFinish' \
               AND current_database = '{db}' LIMIT 1"
        );
        let mut stream = admin
            .query_stream::<TextRow>(&sql, &QuerySettings::new())
            .await
            .unwrap_or_else(|e| panic!("query_log read failed: {e}"));
        if let Some(row) = stream.next().await {
            return Some(row.expect("decode query_log row").s);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// A fresh statement-id prefix per run, so repeat runs against one server
/// cannot read each other's `query_log` rows.
fn prefix() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// =====================================================================
// F-15 — the eight binary fields, all of them, byte for byte.
// =====================================================================

/// The eight blobs, each a distinct byte string so a crossed wire is a
/// failed comparison rather than a coincidence. They are **arbitrary
/// bytes** on purpose: this case is about the reader's decoding, and a
/// field left on serde's sequence path fails before any content matters.
const SPAN_ATTRS_OTHER: &[u8] = &[0x01, 0x02, 0x03, 0xff];
const SCOPE_ATTRS_OTHER: &[u8] = &[0x11, 0x00, 0x12];
const EVENT_ATTRS_OTHER: &[u8] = &[0x21, 0x22];
const LINK_ATTRS_OTHER: &[u8] = &[0x31, 0x32, 0x33, 0x34, 0x35];
const LINK_TRACE_ID: &[u8] = &[0xaa, 0xbb, 0xcc, 0xdd];
const LINK_SPAN_ID: &[u8] = &[0x11, 0x22, 0x33];
const RESOURCE_ATTRS_OTHER: &[u8] = &[0x41, 0xfe, 0x43];
const RESOURCE_ENTITY_REFS: &[u8] = &[0x51, 0x52, 0x00, 0x54, 0x55, 0x56];

/// `F-15`: **all eight of the binary fields, read back byte for byte.**
///
/// A direct insert, because the subject is the reader's decoding: the
/// view's correctness is the write part's, and a direct insert makes the
/// state in three statements with nothing to settle.
///
/// **The fixture inserts a per-trace row as well, and that is not
/// optional.** The indexed statement reads its key set from that table;
/// with no row there the set defaults to the empty array, the span read
/// matches nothing, and the fetch answers empty before decoding anything —
/// so none of the eight assertions would be reached. All three of the
/// fields the statements read are supplied: the bucket set, which is what
/// makes the key read find the span, and the two extent bounds, which are
/// what makes the resource-day bound include the resource's own day.
/// Fields 7 and 8 come off the resource array and fail with the extent
/// left at zero.
#[tokio::test(flavor = "multi_thread")]
async fn the_eight_binary_fields_decode_byte_for_byte() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = fresh_db(pulsus_testkit::test_db("pulsus_fetch_bytes_it")).await;
    let client = data(&db).await;

    let trace_id = [0xf1u8; 16];
    let span_id = [0x01u8; 8];
    let start_ns: i64 = 1_700_000_000_000_000_000;
    let bucket = start_ns / BUCKET_NS;
    let resource_id: u128 = 7;

    exec(
        &client,
        &format!(
            "INSERT INTO {db}.spans \
             (trace_id, span_id, parent_span_id, start_ns, end_ns, service, resource_id, \
              name, kind, status_code, attrs_other, scope_attrs_other, events, links) VALUES \
             (unhex('{tid}'), unhex('{sid}'), toFixedString('', 8), {start_ns}, \
              {end_ns}, 'checkout', {resource_id}, 'op', 2, 0, \
              unhex('{span_other}'), unhex('{scope_other}'), \
              [({ev_time}, 'ev', '{{}}', unhex('{ev_other}'), 2)], \
              [(unhex('{link_trace}'), unhex('{link_span}'), 'ls=1', 2, '{{}}', \
                unhex('{link_other}'), 4)])",
            tid = hex(&trace_id),
            sid = hex(&span_id),
            end_ns = start_ns + 1_000_000,
            span_other = hex(SPAN_ATTRS_OTHER),
            scope_other = hex(SCOPE_ATTRS_OTHER),
            ev_time = start_ns + 100_000,
            ev_other = hex(EVENT_ATTRS_OTHER),
            link_trace = hex(LINK_TRACE_ID),
            link_span = hex(LINK_SPAN_ID),
            link_other = hex(LINK_ATTRS_OTHER),
        ),
    )
    .await;

    exec(
        &client,
        &format!(
            "INSERT INTO {db}.resources \
             (day, resource_id, service, attrs, attrs_other, dropped_attrs, schema_url, \
              entity_refs) VALUES \
             (toDate(fromUnixTimestamp64Nano({start_ns}), 'UTC'), {resource_id}, 'checkout', \
              '{{}}', unhex('{res_other}'), 7, 'https://example.invalid/res', \
              unhex('{refs}'))",
            res_other = hex(RESOURCE_ATTRS_OTHER),
            refs = hex(RESOURCE_ENTITY_REFS),
        ),
    )
    .await;

    exec(
        &client,
        &format!(
            "INSERT INTO {db}.traces (day, trace_id, start_ns, last_start_ns, buckets) VALUES \
             (toDate(fromUnixTimestamp64Nano({start_ns}), 'UTC'), unhex('{tid}'), \
              {start_ns}, {start_ns}, [{bucket}])",
            tid = hex(&trace_id),
        ),
    )
    .await;

    let fetched = engine_on(&db, &prefix())
        .await
        .fetch_by_id(&hex(&trace_id), None)
        .await
        .expect("the fetch executes");

    assert_eq!(fetched.spans.len(), 1, "one inserted span comes back");
    let span = &fetched.spans[0];
    assert_eq!(span.attrs_other.as_slice(), SPAN_ATTRS_OTHER, "field 1");
    assert_eq!(
        span.scope_attrs_other.as_slice(),
        SCOPE_ATTRS_OTHER,
        "field 2"
    );

    assert_eq!(span.events.len(), 1, "one event");
    assert_eq!(
        span.events[0].attrs_other.as_slice(),
        EVENT_ATTRS_OTHER,
        "field 3"
    );

    assert_eq!(span.links.len(), 1, "one link");
    assert_eq!(
        span.links[0].attrs_other.as_slice(),
        LINK_ATTRS_OTHER,
        "field 4"
    );
    assert_eq!(
        span.links[0].trace_id.as_slice(),
        LINK_TRACE_ID,
        "field 5 — four bytes, not sixteen"
    );
    assert_eq!(
        span.links[0].span_id.as_slice(),
        LINK_SPAN_ID,
        "field 6 — three bytes, not eight"
    );

    assert_eq!(
        fetched.resources.len(),
        1,
        "the resource array is what fields 7 and 8 come off, and it is empty if the \
         per-trace row's extent is left at zero"
    );
    let resource = &fetched.resources[0];
    assert_eq!(
        resource.attrs_other.as_slice(),
        RESOURCE_ATTRS_OTHER,
        "field 7"
    );
    assert_eq!(
        resource.entity_refs.as_slice(),
        RESOURCE_ENTITY_REFS,
        "field 8"
    );
    assert_eq!(resource.resource_id, resource_id);
    assert_eq!(resource.schema_url, "https://example.invalid/res");
    assert_eq!(resource.dropped_attrs, 7);

    drop_db(&db).await;
}

// =====================================================================
// F-16 — the reported column types of each of the three statements.
// =====================================================================

/// `F-16`: **each builder's reported column type list, column by column**,
/// against a literal list in this case.
///
/// Run against an EMPTY database, so no row is read and the only thing
/// compared is what the statement says it returns.
///
/// `UInt32` for `bucket_count` is what the statement reports because it
/// projects `toUInt32(length(bk))`: the bare `length` returns `UInt64`, so
/// dropping the cast reports a type the row refuses and every indexed
/// fetch ends in a schema mismatch. The two scalar members are read
/// through `ifNull`, which is why neither is `Nullable`; the composite
/// members cannot be `Nullable` at all, which the engine refuses rather
/// than this case assuming.
#[tokio::test(flavor = "multi_thread")]
async fn each_statements_reported_column_types_are_the_row_types() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = fresh_db(pulsus_testkit::test_db("pulsus_fetch_header_it")).await;
    let client = data(&db).await;
    const HEX: &str = "50fb0cd99260ac2a15d0a6f208126742";

    let window = FetchWindow {
        start_ns: 1_699_999_999_000_000_000,
        end_ns: 1_700_000_002_000_000_000,
    };
    let cases: Vec<(&str, String, usize)> = vec![
        (
            "indexed",
            pulsus_read::traces::spans::fetch::indexed_fetch_sql(
                "traces",
                "spans",
                "resources",
                HEX,
            ),
            4,
        ),
        (
            "wide",
            pulsus_read::traces::spans::fetch::wide_fetch_sql("traces", "spans", "resources", HEX),
            2,
        ),
        (
            "fallback",
            pulsus_read::traces::spans::fetch::fallback_fetch_sql(
                "spans",
                "resources",
                HEX,
                window,
            ),
            2,
        ),
    ];
    assert_eq!(cases.len(), 3, "one sub-case per builder");

    // **The server's own report, taken once and frozen.** Two things in
    // it are the engine's and neither is this statement's choice, so both
    // are written out rather than guessed at: the aggregate ERASES
    // `LowCardinality` from every element it carries, and the two NESTED
    // tuples keep their declared element names where the outer one — built
    // from anonymous expressions — does not.
    const SPAN_TUPLE: &str = "Array(Tuple(FixedString(8), FixedString(8), Int64, UInt64, \
         String, UInt128, String, Int32, Int32, String, String, UInt32, String, String, JSON, \
         JSON, String, UInt32, Array(Tuple(\n    time_ns UInt64,\n    name String,\n    \
         attrs JSON,\n    attrs_other String,\n    dropped_attrs UInt32)), UInt32, \
         Array(Tuple(\n    trace_id String,\n    span_id String,\n    trace_state String,\n    \
         flags UInt32,\n    attrs JSON,\n    attrs_other String,\n    dropped_attrs UInt32)), \
         UInt32, String, UInt32, String))";
    const RESOURCE_TUPLE: &str = "Array(Tuple(UInt128, JSON, String, UInt32, String, String))";

    for (label, sql, want_columns) in cases {
        // `DESCRIBE (<statement>)` and not a wrapping `SELECT … FROM
        // (DESCRIBE …)`: a `WITH` prelude inside a subquery is a syntax
        // error, and the describe's own first two columns are the name
        // and the type.
        let describe = format!("DESCRIBE ({sql})");
        let mut stream = client
            .query_stream::<DescribeRow>(&describe, &QuerySettings::new())
            .await
            .unwrap_or_else(|e| panic!("{label}: describe failed: {e}\nSQL:\n{sql}"));
        let mut got = Vec::new();
        while let Some(row) = stream.next().await {
            let row = row.expect("decode a describe row");
            got.push(format!("{} {}", row.name, row.r#type));
        }
        assert_eq!(
            got.len(),
            want_columns,
            "{label}: the statement must project {want_columns} columns, got {got:?}"
        );
        let want: Vec<String> = if label == "indexed" {
            vec![
                "index_rows UInt64".to_string(),
                "bucket_count UInt32".to_string(),
                format!("spans {SPAN_TUPLE}"),
                format!("resources {RESOURCE_TUPLE}"),
            ]
        } else {
            vec![
                format!("spans {SPAN_TUPLE}"),
                format!("resources {RESOURCE_TUPLE}"),
            ]
        };
        assert_eq!(got, want, "{label}: the reported column type list");
    }

    drop_db(&db).await;
}

// =====================================================================
// F-20 — the capped union across the trace's day rows.
// =====================================================================

/// The first stored row's bucket set starts here: 4,096 distinct buckets,
/// so the row sits exactly at the per-row cap and may have truncated.
const F20_FIRST_BUCKET: i64 = 5_666_666;
/// A further bucket, one past the first row's last — so the union of the
/// two rows is 4,097 elements, and in a DIFFERENT UTC day, so the two rows
/// are in different partitions and no merge can combine their parts.
const F20_FURTHER_BUCKET: i64 = F20_FIRST_BUCKET + BUCKET_SET_CAP as i64;

/// `F-20`: **the capped union across a trace's day rows.**
///
/// Four assertions, and the fourth is the only one that can see the cap.
/// Under the fetch's own settings the default
/// `do_not_merge_across_partitions_select_final = 0` lets `FINAL` collapse
/// the two partition rows **before** the statement's aggregate runs, and
/// the collapse applies the column's own capped merge — so over one
/// already-capped row the capped and uncapped expressions return the same
/// 4,096 and (a) to (c) cannot distinguish them. (d) re-issues the
/// recorded text with cross-partition final merging **disabled**, where
/// the statement sees two rows and the union becomes visible.
#[tokio::test(flavor = "multi_thread")]
async fn the_outer_bucket_union_is_capped_across_the_traces_day_rows() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = fresh_db(pulsus_testkit::test_db("pulsus_fetch_cap_it")).await;
    let client = data(&db).await;
    let admin = admin().await;

    let trace_id = [0x0au8; 16];
    // Row 1: the 4,096 buckets from `F20_FIRST_BUCKET`, with the extent
    // spanning them — the first nanosecond of the first bucket and the
    // last nanosecond of the 4,096th.
    let row1_lo = F20_FIRST_BUCKET * BUCKET_NS;
    let row1_hi = (F20_FIRST_BUCKET + BUCKET_SET_CAP as i64) * BUCKET_NS - 1;
    // Row 2: the further bucket alone.
    let row2_ns = F20_FURTHER_BUCKET * BUCKET_NS;

    exec(
        &client,
        &format!(
            "INSERT INTO {db}.traces (day, trace_id, start_ns, last_start_ns, buckets) VALUES \
             (toDate(fromUnixTimestamp64Nano({row1_lo}), 'UTC'), unhex('{tid}'), \
              {row1_lo}, {row1_hi}, range({F20_FIRST_BUCKET}, {first_past}))",
            tid = hex(&trace_id),
            first_past = F20_FIRST_BUCKET + BUCKET_SET_CAP as i64,
        ),
    )
    .await;
    exec(
        &client,
        &format!(
            "INSERT INTO {db}.traces (day, trace_id, start_ns, last_start_ns, buckets) VALUES \
             (toDate(fromUnixTimestamp64Nano({row2_ns}), 'UTC'), unhex('{tid}'), \
              {row2_ns}, {row2_ns}, [{F20_FURTHER_BUCKET}])",
            tid = hex(&trace_id),
        ),
    )
    .await;

    // One span, in the FURTHER bucket — the bucket a set truncated at the
    // cap cannot name. It comes back only because the complete-predicate
    // statement carries no bucket condition at all.
    exec(
        &client,
        &format!(
            "INSERT INTO {db}.spans \
             (trace_id, span_id, parent_span_id, start_ns, end_ns, service, resource_id, \
              name, kind, status_code) VALUES \
             (unhex('{tid}'), unhex('0000000000000001'), toFixedString('', 8), {row2_ns}, \
              {end_ns}, 'checkout', 5, 'op', 2, 0)",
            tid = hex(&trace_id),
            end_ns = row2_ns + 1_000_000,
        ),
    )
    .await;
    exec(
        &client,
        &format!(
            "INSERT INTO {db}.resources (day, resource_id, service, attrs, schema_url) VALUES \
             (toDate(fromUnixTimestamp64Nano({row2_ns}), 'UTC'), 5, 'checkout', '{{}}', '')"
        ),
    )
    .await;

    // What (d) depends on: two PHYSICAL rows. Different partitions, so no
    // merge can combine their parts and the count is stable.
    assert_eq!(
        scalar_u64(
            &client,
            &format!(
                "SELECT count() AS n FROM {db}.traces WHERE trace_id = unhex('{tid}')",
                tid = hex(&trace_id)
            )
        )
        .await,
        2,
        "two per-trace rows, in two day partitions"
    );

    let p = prefix();
    let fetched = engine_on(&db, &p)
        .await
        .fetch_by_id(&hex(&trace_id), None)
        .await
        .expect("the fetch executes");

    // (b) the route.
    assert_eq!(
        fetched.route,
        FetchRoute::TruncatedSet,
        "a stored set at the cap takes the complete-predicate route"
    );
    assert_eq!(fetched.statements, 2);

    // (c) THE ANSWER: the span in the further bucket is returned, which it
    // is only because the complete-predicate statement ran.
    assert_eq!(
        fetched.spans.iter().map(|s| s.span_id).collect::<Vec<_>>(),
        vec![[0u8, 0, 0, 0, 0, 0, 0, 1]],
        "the span in the bucket a truncated set cannot name"
    );

    // (a) the statement's own `bucket_count`, off the recorded text.
    let recorded = recorded_query(&admin, &db, &format!("{p}-1"))
        .await
        .unwrap_or_else(|| panic!("statement 1 must be recorded as {p}-1"));
    assert_eq!(
        scalar_u32(
            &client,
            &format!("SELECT bucket_count AS n FROM ({recorded}) SETTINGS final = 1")
        )
        .await,
        BUCKET_SET_CAP,
        "(a) under the production settings the union is already capped"
    );

    // (d) THE CAP CONTROL, with cross-partition final merging disabled so
    // the statement sees both rows.
    assert_eq!(
        scalar_u32(
            &client,
            &format!(
                "SELECT bucket_count AS n FROM ({recorded}) \
                 SETTINGS final = 1, do_not_merge_across_partitions_select_final = 1"
            )
        )
        .await,
        BUCKET_SET_CAP,
        "(d) with the cap, two rows union to the cap"
    );
    let uncapped = recorded.replace(
        "groupUniqArrayArray(4096)(buckets)",
        "groupUniqArrayArray(buckets)",
    );
    assert_ne!(
        uncapped, recorded,
        "the outer cap must be in the recorded text for this control to mean anything"
    );
    assert_eq!(
        scalar_u32(
            &client,
            &format!(
                "SELECT bucket_count AS n FROM ({uncapped}) \
                 SETTINGS final = 1, do_not_merge_across_partitions_select_final = 1"
            )
        )
        .await,
        BUCKET_SET_CAP + 1,
        "(d) without the cap, the same two rows union to one past it — which is why \
         the branch tests `>=` and not `==`"
    );

    drop_db(&db).await;
}

// =====================================================================
// The `final = 1` collapse.
// =====================================================================

/// **A retried span is not doubled before assembly.**
///
/// The quantity that moves when `final = 1` goes missing is the number of
/// rows the ENGINE returns, which is observable only below the assembler:
/// asserting "4 rows stored, 2 spans in the response" through HTTP passes
/// either way, because the assembler's `(span_id, kind)` map collapses the
/// duplicates in Rust.
///
/// **Merges are stopped around the inserts, and that is not optional.**
/// The two inserts write four rows with IDENTICAL sort keys into ONE
/// partition, so a background merge between the second insert and
/// assertion (1) collapses them to two and (1) goes red while the case's
/// subject is unaffected — assertion (3) reads through `final = 1` and
/// returns 2 either way. A flaky red on correct code. Weakening (1) to
/// `>= 2` instead would be worse: it would pass against a single insert,
/// which is the state this case exists to rule out.
///
/// The stop is **table-scoped**, which is the whole of why no restart is
/// needed: the scoped form resolves a storage object, so the stop is held
/// against the table and dropping the database destroys the holder. The
/// argument-less form is server-wide and no drop undoes it.
///
/// **No resource row is inserted**, deliberately: the fetch's resource
/// array is then empty and every rendered resource is whatever the service
/// name reconstructs. Stated because this case's assertions do not look at
/// it, and a later assertion on a resource field would fail here for a
/// reason that is not this case's subject.
#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_stored_span_is_collapsed_by_final_before_the_reader_sees_it() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = fresh_db(pulsus_testkit::test_db("pulsus_fetch_final_it")).await;
    let client = data(&db).await;

    let trace_id = [0x06u8; 16];
    let first_ns: i64 = 1_700_000_000_000_000_000;
    let second_ns: i64 = 1_700_000_000_001_000_000;
    let bucket = first_ns / BUCKET_NS;
    assert_eq!(
        bucket,
        second_ns / BUCKET_NS,
        "both spans must share one bucket, or the fixture's stored set is wrong"
    );

    // Merges are already stopped on all three tables by `fresh_db`, which
    // is where that device and its reason live.

    let insert = format!(
        "INSERT INTO {db}.spans \
         (trace_id, span_id, parent_span_id, start_ns, end_ns, service, resource_id, \
          name, kind, status_code) \
         SETTINGS insert_deduplication_token = '{{token}}' VALUES \
         (unhex('{tid}'), unhex('0000000000000001'), toFixedString('', 8), {first_ns}, \
          {first_end}, 'checkout', 0, 'op', 2, 0), \
         (unhex('{tid}'), unhex('0000000000000002'), toFixedString('', 8), {second_ns}, \
          {second_end}, 'checkout', 0, 'op', 2, 0)",
        tid = hex(&trace_id),
        first_end = first_ns + 1_000_000,
        second_end = second_ns + 1_000_000,
    );
    // TWO separate statements, so the rows land in two parts and no
    // block-level collapse applies.
    //
    // **Each carries its own deduplication token, and that is not a
    // convenience.** The table carries a block-deduplication window, so
    // two byte-identical inserts are ONE block and the second is
    // suppressed — the fixture would make two rows, not four, and the
    // case would be red for a reason that is not its subject. Two
    // distinct tokens is what a genuine at-least-once duplicate from two
    // different pushes looks like: the same span, two blocks, and the
    // read's own `final = 1` is the only thing that collapses them.
    for token in ["fetch-final-fixture-a", "fetch-final-fixture-b"] {
        // `SETTINGS` goes BEFORE `VALUES` in an insert; after it the
        // value parser reads the clause as another row.
        exec(&client, &insert.replace("{token}", token)).await;
    }

    exec(
        &client,
        &format!(
            "INSERT INTO {db}.traces (day, trace_id, start_ns, last_start_ns, buckets) VALUES \
             (toDate(fromUnixTimestamp64Nano({first_ns}), 'UTC'), unhex('{tid}'), \
              {first_ns}, {second_ns}, [{bucket}])",
            tid = hex(&trace_id),
        ),
    )
    .await;

    // (1) the fixture made the state — four PHYSICAL rows, deterministic
    // because merges are stopped.
    assert_eq!(
        scalar_u64(
            &client,
            &format!(
                "SELECT count() AS n FROM {db}.spans WHERE trace_id = unhex('{tid}')",
                tid = hex(&trace_id)
            )
        )
        .await,
        4,
        "two spans inserted twice are four rows until a merge or `final` collapses them"
    );

    // (2) the fixture made the KEY state and the EXTENT — without this the
    // next assertion can fail for the wrong reason.
    let state = one_row::<TraceStateRow>(
        &client,
        &format!(
            "SELECT buckets, start_ns, last_start_ns FROM {db}.traces \
             WHERE trace_id = unhex('{tid}')",
            tid = hex(&trace_id)
        ),
    )
    .await;
    assert_eq!(state.buckets, vec![bucket]);
    assert_eq!(state.start_ns, first_ns);
    assert_eq!(state.last_start_ns, second_ns);

    // (3) the subject: the ENGINE returns two rows, not four.
    let fetched = engine_on(&db, &prefix())
        .await
        .fetch_by_id(&hex(&trace_id), None)
        .await
        .expect("the fetch executes");
    assert_eq!(
        fetched.spans.len(),
        2,
        "`final = 1` is what collapses the duplicates before the reader sees them"
    );

    drop_db(&db).await;
}

// =====================================================================
// The plain fetch over a seeded corpus.
// =====================================================================

const CORPUS_TRACES: u64 = 10_000;
/// An arbitrary mid-corpus trace to fetch.
const TARGET: u64 = 5_432;

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

/// `CORPUS_TRACES` single-span traces with distinct ids, in one
/// server-side `INSERT ... SELECT` into each of the three tables —
/// timestamps wall-clock-recent so retention can never drop the part
/// underfoot.
async fn seed_corpus(client: &ChClient, db: &str) {
    let now = now_ns();
    exec(
        client,
        &format!(
            "INSERT INTO {db}.spans \
             (trace_id, span_id, parent_span_id, start_ns, end_ns, service, resource_id, \
              name, kind, status_code) \
             SELECT \
               toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               toFixedString('', 8), {now} + toInt64(number), \
               toUInt64({now} + toInt64(number) + 1000), 'gate-svc', 1, 'gate-span', 1, 0 \
             FROM numbers({CORPUS_TRACES})"
        ),
    )
    .await;
    exec(
        client,
        &format!(
            "INSERT INTO {db}.traces (day, trace_id, start_ns, last_start_ns, buckets) \
             SELECT \
               toDate(fromUnixTimestamp64Nano({now} + toInt64(number)), 'UTC'), \
               toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
               {now} + toInt64(number), {now} + toInt64(number), \
               [intDiv({now} + toInt64(number), {BUCKET_NS})] \
             FROM numbers({CORPUS_TRACES})"
        ),
    )
    .await;
    exec(
        client,
        &format!(
            "INSERT INTO {db}.resources (day, resource_id, service, attrs, schema_url) VALUES \
             (toDate(fromUnixTimestamp64Nano({now}), 'UTC'), 1, 'gate-svc', '{{}}', '')"
        ),
    )
    .await;
}

/// The engine's own fetch over a many-trace corpus: one span back for a
/// seeded id, with its service and its resource, and an EMPTY answer for
/// an id outside the corpus — which is one statement and never an error.
#[tokio::test(flavor = "multi_thread")]
async fn a_seeded_trace_fetches_its_span_and_an_absent_one_fetches_empty() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = fresh_db(pulsus_testkit::test_db("pulsus_fetch_corpus_it")).await;
    let client = data(&db).await;
    seed_corpus(&client, &db).await;

    let engine = engine_on(&db, &prefix()).await;
    let hex32 = format!("{TARGET:032x}");
    let fetched = engine
        .fetch_by_id(&hex32, None)
        .await
        .expect("the fetch executes");
    assert_eq!(
        fetched.spans.len(),
        1,
        "exactly one stored span for {hex32}"
    );
    assert_eq!(fetched.spans[0].span_id, TARGET.to_be_bytes());
    assert_eq!(fetched.spans[0].kind, 1);
    assert_eq!(fetched.spans[0].service, "gate-svc");
    assert_eq!(
        fetched.statements, 1,
        "an indexed fetch costs one statement"
    );
    assert_eq!(fetched.route, FetchRoute::Indexed);
    assert_eq!(fetched.resources.len(), 1, "the one seeded resource");

    let absent = format!("{:032x}", CORPUS_TRACES + 17);
    let empty = engine
        .fetch_by_id(&absent, None)
        .await
        .expect("the absent fetch executes");
    assert!(empty.spans.is_empty(), "an absent trace fetches zero spans");
    assert!(empty.resources.is_empty());
    assert_eq!(
        empty.statements, 1,
        "an absent trace costs one statement, not a full-retention scan"
    );

    drop_db(&db).await;
}

// =====================================================================
// T-R4 — at the retention edge, a live span keeps its resource and its
// per-trace row
// =====================================================================

/// One nanosecond-per-day constant for the retention-edge case.
const DAY_NS: i64 = 86_400_000_000_000;

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct IdRow {
    id: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
struct ResourceDayRow {
    r: u64,
    d: u32,
}

/// One `trace_landing` row of the retention-edge fixture, as a `SELECT`
/// arm of the one `INSERT`.
fn landing_arm(
    received_ms: i64,
    row_kind: u8,
    id_bytes: (&str, &str),
    start_ns: i64,
    resource_id: u64,
    day: &str,
    attrs: &str,
) -> String {
    let (trace_hex, span_hex) = id_bytes;
    let service_type = if row_kind == 0 { "string" } else { "" };
    format!(
        "SELECT toInt64({received_ms}), toUInt8({row_kind}), \
           toFixedString(unhex('{trace}'), 16), toFixedString(unhex('{span}'), 8), \
           toInt64({start_ns}), toUInt128({resource_id}), 'checkout', '{service_type}', \
           {day}, CAST('{attrs}' AS JSON)",
        trace = trace_hex.repeat(16),
        span = span_hex.repeat(8),
    )
}

/// `T-R4`: **with a one-day retention, a span inside retention still finds
/// its resource row and its per-trace row once every expired part has
/// gone**, through the engine's own fetch and the span-scope membership
/// statement.
///
/// `Y` is yesterday's UTC midnight. `aa`'s one span is at the last
/// nanosecond of yesterday; `cc`'s block starts half a day before `Y`, so
/// its per-trace row files under the day before yesterday while its latest
/// span `c2` is yesterday's second-to-last nanosecond; `bb`'s one span is
/// the day before yesterday's last nanosecond, already expired. Resource
/// rows are filed under the day of the spans they describe: `101` and the
/// second `103` under yesterday, `102` and the first `103` under the day
/// before.
///
/// **Merges are NOT stopped here** — no `fresh_db` — because the case needs
/// the TTL drops. It polls until no active part of the three tables holds
/// a part wholly past its TTL.
///
/// **The UTC day must not change under the case.** Span `a1` expires at
/// the end of today, so a run straddling midnight would read a different
/// state. The case waits out the last five minutes of a day before it
/// starts, and re-reads the day after every read and before any assertion:
/// a changed day panics with the reason rather than asserting or passing.
///
/// `resources` is read **without `FINAL`**: its replacing key omits `day`,
/// so `FINAL` folds two days' rows for one resource into one and would hide
/// a resource row kept a day too long.
#[tokio::test(flavor = "multi_thread")]
async fn a_retention_edge_trace_keeps_its_resource_and_per_trace_rows() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_fetch_retention_edge_it");
    let admin = admin().await;
    exec(&admin, &format!("DROP DATABASE IF EXISTS {db}")).await;
    let mut ctx = test_ctx(&db);
    ctx.retention_days = 1;
    run_init(&admin, &ctx).await.expect("run_init");

    // The late-night guard: never start inside a day's last 300 s.
    let guard_ns = 300 * 1_000_000_000;
    let into_day = now_ns().rem_euclid(DAY_NS);
    if into_day > DAY_NS - guard_ns {
        let wait = DAY_NS - into_day + 1_000_000_000;
        eprintln!(
            "T-R4: {} s before UTC midnight; waiting for the next day",
            (DAY_NS - into_day) / 1_000_000_000
        );
        tokio::time::sleep(Duration::from_nanos(u64::try_from(wait).expect("positive"))).await;
    }
    let today = now_ns().div_euclid(DAY_NS);
    let y = (today - 1) * DAY_NS;
    let yd = format!("toDate(fromUnixTimestamp64Nano(toInt64({y})), 'UTC')");
    let yd_less_1 = format!("({yd} - 1)");
    let epoch = "toDate('1970-01-01')";
    let received_ms = now_ns() / 1_000_000;

    let no_attrs = "{}";
    let k_r = r#"{"k":"r"}"#;
    let arm = |kind, ids, start_ns, resource_id, day: &str, attrs| {
        landing_arm(received_ms, kind, ids, start_ns, resource_id, day, attrs)
    };
    let arms = [
        arm(0, ("aa", "a1"), y + DAY_NS - 1, 101, epoch, no_attrs),
        arm(0, ("bb", "b1"), y - 1, 102, epoch, no_attrs),
        arm(0, ("cc", "c1"), y - DAY_NS / 2, 103, epoch, no_attrs),
        arm(0, ("cc", "c2"), y + DAY_NS - 2, 103, epoch, no_attrs),
        arm(1, ("00", "00"), 0, 101, &yd, k_r),
        arm(1, ("00", "00"), 0, 102, &yd_less_1, k_r),
        arm(1, ("00", "00"), 0, 103, &yd_less_1, k_r),
        arm(1, ("00", "00"), 0, 103, &yd, k_r),
    ];
    exec(
        &admin,
        &format!(
            "INSERT INTO {db}.trace_landing \
             (received_ms, row_kind, trace_id, span_id, start_ns, resource_id, service, \
              service_type, day, attrs) \
             {}",
            arms.join(" UNION ALL ")
        ),
    )
    .await;

    // Wait until no active part of the three tables is past its TTL.
    let expired_parts = format!(
        "SELECT count() AS n FROM system.parts \
         WHERE database = '{db}' AND table IN ('spans', 'resources', 'traces') \
           AND active AND delete_ttl_info_max > 0 AND delete_ttl_info_max <= now()"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        if scalar_u64(&admin, &expired_parts).await == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "expired parts were still active after 120 s in {db}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Every read first; the assertions come after the day re-check.
    let engine = engine_on(&db, &prefix()).await;
    let mut fetched = Vec::new();
    for t in ["aa", "cc", "bb"] {
        let f = engine
            .fetch_by_id(&t.repeat(16), None)
            .await
            .unwrap_or_else(|e| panic!("the {t} fetch executes: {e}"));
        let spans: Vec<String> = f.spans.iter().map(|s| hex(&s.span_id)).collect();
        let resources: Vec<u128> = f.resources.iter().map(|r| r.resource_id).collect();
        fetched.push((t, spans, resources));
    }

    let client = data(&db).await;
    let resource_days_sql = "SELECT DISTINCT toUInt64(resource_id) AS r, toUInt32(day) AS d \
                             FROM resources ORDER BY r, d";
    let mut stream = client
        .query_stream::<ResourceDayRow>(resource_days_sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("resource read failed: {e}\nSQL:\n{resource_days_sql}"));
    let mut resource_days = Vec::new();
    while let Some(row) = stream.next().await {
        resource_days.push(row.expect("decode ResourceDayRow"));
    }

    let w = WindowSql::start_closed_end_open(y - DAY_NS, now_ns() + 1_000_000_000);
    let query = r#"{ resource.k = "r" }"#;
    let parsed =
        pulsus_traceql::parse(query).unwrap_or_else(|e| panic!("{query} must parse: {e:?}"));
    let body = match parsed.spanset {
        pulsus_traceql::SpansetExpr::Filter(pulsus_traceql::SpansetFilter { body: Some(b) }) => b,
        other => panic!("{query}: expected one filter with a body, got {other}"),
    };
    let predicate = compile_span_predicate_in(
        &body,
        &PredicateCtx {
            window: w,
            resources_table: "resources",
        },
    )
    .unwrap_or_else(|e| panic!("{query} must compile: {e}"));
    let membership_sql = span_membership_sql("spans", w, &predicate);
    let mut stream = client
        .query_stream::<IdRow>(
            &membership_sql.replace('?', "??"),
            &QuerySettings::new().set("final", 1),
        )
        .await
        .unwrap_or_else(|e| panic!("membership read failed: {e}\nSQL:\n{membership_sql}"));
    let mut members = Vec::new();
    while let Some(row) = stream.next().await {
        members.push(row.expect("decode IdRow").id);
    }

    let after = now_ns().div_euclid(DAY_NS);
    assert_eq!(
        after, today,
        "T-R4 crossed UTC midnight between its insert and its last read, so the state it read \
         is not the one it asserts on: it cannot assert, rerun it"
    );

    let a1 = "a1".repeat(8);
    let c2 = "c2".repeat(8);
    assert_eq!(
        fetched,
        vec![
            ("aa", vec![a1.clone()], vec![101u128]),
            ("cc", vec![c2.clone()], vec![103u128]),
            ("bb", vec![], vec![]),
        ],
        "(trace, span ids, resource ids) for each fetch"
    );
    let yd_num = u32::try_from(today - 1).expect("a day number");
    assert_eq!(
        resource_days,
        vec![
            ResourceDayRow { r: 101, d: yd_num },
            ResourceDayRow { r: 103, d: yd_num },
        ],
        "the resource rows left, read without FINAL"
    );
    assert_eq!(
        members,
        vec![a1, c2],
        "the spans {query} matches in the window"
    );

    drop_db(&db).await;
}
