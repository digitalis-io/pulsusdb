//! Issue #738, T-R: a tenant's trace reads read the rows `main` reads.
//!
//! One corpus — 10,000 traces of 5 spans and their 20 services' resources,
//! from yesterday 00:00 UTC — is landed twice: into a database built from
//! the schema as it stood before the tenant column
//! (`fixtures/schema_f9ae32f5.sql`, rendered by `schema/schema.sh`), and
//! under `tenant-3` into one built from today's schema. The engine, as
//! `tenant-3`, fetches one trace and runs four searches over the second.
//! Each statement it sent, read back from `system.query_log`, runs again on
//! that database, and with every `org_id = 'tenant-3' AND ` removed (the
//! statement `main` sends) on the first: the two answer the same and read
//! the same number of rows.
//!
//! Single-node always. With `PULSUS_TEST_CH_CLUSTER` set — the connection
//! then points at a node of that cluster — the clustered schema too.
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test traces_tenant_rows
//! ```

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_read::{
    SearchParams, StatementTables, TraceEngine, TraceReadConfig, plan_search_request,
};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const TENANT: &str = "tenant-3";
const TERM: &str = "org_id = 'tenant-3' AND ";
const DAY_NS: i64 = 86_400_000_000_000;
const SECOND_NS: i64 = 1_000_000_000;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
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

async fn exec(client: &ChClient, sql: &str) {
    exec_with(client, sql, &QuerySettings::new()).await;
}

async fn exec_with(client: &ChClient, sql: &str, settings: &QuerySettings) {
    client
        .execute(
            &sql.replace('?', "??"),
            settings,
            Idempotency::NonIdempotent,
        )
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

async fn strings(client: &ChClient, sql: &str, settings: &QuerySettings) -> Vec<String> {
    client
        .query_strings(&sql.replace('?', "??"), settings)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
}

fn ctx(db: &str, cluster: Option<&str>) -> RenderCtx {
    RenderCtx {
        cluster: cluster.map(str::to_string),
        ..RenderCtx::for_tests(db, 7)
    }
}

/// The statements `schema/schema.sh --print` renders from the schema as it
/// stood before the tenant column, for `db`.
fn main_schema(db: &str, cluster: Option<&str>) -> Vec<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let dir =
        std::env::temp_dir().join(format!("pulsus-t738-schema-{}-{}", std::process::id(), db));
    std::fs::create_dir_all(&dir).expect("a scratch directory");
    std::fs::copy(root.join("schema/schema.sh"), dir.join("schema.sh")).expect("copy schema.sh");
    std::fs::copy(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/schema_f9ae32f5.sql"),
        dir.join("schema.sql"),
    )
    .expect("copy the fixture");
    let mut cmd = std::process::Command::new("sh");
    cmd.arg(dir.join("schema.sh"))
        .arg("--print")
        .arg("--mode")
        .arg(if cluster.is_some() {
            "cluster"
        } else {
            "single"
        })
        .env("CLICKHOUSE_DB", db)
        .env("PULSUS_RETENTION_DAYS", "7")
        .env_remove("PULSUS_TRACEQL_INDEXED_ATTRIBUTES")
        .env_remove("PULSUS_STORAGE_POLICY")
        .env_remove("PULSUS_DIST_SUFFIX");
    match cluster {
        Some(c) => cmd.env("PULSUS_CLUSTER", c),
        None => cmd.env_remove("PULSUS_CLUSTER"),
    };
    let out = cmd.output().expect("run schema.sh");
    assert!(
        out.status.success(),
        "schema.sh --print: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    std::fs::remove_dir_all(&dir).ok();
    let text = String::from_utf8(out.stdout).expect("UTF-8");
    let mut stmts = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        current.push_str(line);
        current.push('\n');
        if line.ends_with(';') {
            stmts.push(current.trim().to_string());
            current.clear();
        }
    }
    stmts
}

/// The corpus's two `INSERT … SELECT`s into `trace_landing`: 10,000 traces
/// of 5 spans over two hours from `t0`, 20 services and 50 routes, one
/// error per 97 traces; one resource per service and day. `org` is the
/// column and the value, or nothing.
fn corpus(t0: i64, received_ms: i64, org: Option<(&str, &str)>) -> [String; 2] {
    let (oc, ov) = org.unwrap_or(("", ""));
    [
        format!(
            "INSERT INTO trace_landing ({oc}received_ms, row_kind, trace_id, span_id, parent_span_id, \
             start_ns, duration_ns, resource_id, name, kind, status_code, service, attrs, day, \
             service_type, end_ns) \
             SELECT {ov}{received_ms}, 0, \
               toFixedString(unhex(concat(hex(cityHash64(3, i)), hex(toUInt64(i)))), 16), \
               toFixedString(unhex(hex(toUInt64(j + 1))), 8), \
               if(j = 0, toFixedString('', 8), toFixedString(unhex(hex(toUInt64(1))), 8)), \
               st + j * 1000000, 50000000 - j, toUInt128(cityHash64(3, i % 20)), \
               concat('op-', toString(j)), if(j = 0, 2, 1), if(i % 97 = 0 AND j = 1, 2, 0), \
               concat('svc-', toString(i % 20)), \
               CAST(concat('{{\"http%2Eroute\":\"/r', toString(i % 50), '\"}}') AS JSON), \
               toUInt16(intDiv(st, 86400000000000)), 'string', toUInt64(st + j * 1000000 + 50000000 - j) \
             FROM (SELECT intDiv(number, 5) AS i, number % 5 AS j, \
                          toInt64({t0}) + toInt64((i * 7919) % 7200) * 1000000000 AS st \
                   FROM numbers(50000))"
        ),
        format!(
            "INSERT INTO trace_landing ({oc}received_ms, row_kind, resource_id, service, attrs, day) \
             SELECT {ov}{received_ms}, 1, toUInt128(cityHash64(3, s)), concat('svc-', toString(s)), \
               CAST(concat('{{\"service%2Ename\":\"svc-', toString(s), '\"}}') AS JSON), \
               toUInt16(intDiv(toInt64({t0}) + d * 86400000000000, 86400000000000)) \
             FROM (SELECT intDiv(number, 2) AS s, number % 2 AS d FROM numbers(40))"
        ),
    ]
}

/// Builds `db` (today's schema, or the fixture's when `main`) and lands the
/// corpus in it.
async fn build(client: &ChClient, db: &str, cluster: Option<&str>, main: bool, t0: i64) {
    let on = cluster
        .map(|c| format!(" ON CLUSTER '{c}'"))
        .unwrap_or_default();
    exec(client, &format!("DROP DATABASE IF EXISTS {db}{on} SYNC")).await;
    if main {
        for stmt in main_schema(db, cluster) {
            exec(client, &stmt).await;
        }
    } else {
        run_init(client, &ctx(db, cluster)).await.expect("run_init");
    }
    let target = ChClient::new(conn(db)).await.expect("connect");
    let received_ms = chrono_now_ns() / 1_000_000;
    let org = (!main).then_some(("org_id, ", "'tenant-3', "));
    let settings = QuerySettings::new()
        .set("max_memory_usage", 4_294_967_296_u64)
        .set("distributed_foreground_insert", 1);
    for stmt in corpus(t0, received_ms, org) {
        exec_with(&target, &stmt, &settings).await;
    }
    for t in ["spans", "traces", "resources", "error_spans"] {
        exec(&target, &format!("OPTIMIZE TABLE {db}.{t}{on} FINAL")).await;
    }
}

fn chrono_now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("now fits in i64")
}

fn read_config(cluster: bool) -> TraceReadConfig {
    let dist = if cluster { "_dist" } else { "" };
    TraceReadConfig {
        read_max_memory_bytes: 4 * 1024 * 1024 * 1024,
        spans_table: format!("spans{dist}"),
        error_spans_table: format!("error_spans{dist}"),
        traces_table: format!("traces{dist}"),
        resources_table: "resources".to_string(),
        scan_budget_rows: 50_000_000,
        max_depth: 64,
        max_series: 1_000,
        distributed: cluster,
        skip_unavailable_shards: false,
    }
}

/// `sql` run on `db` under `settings`: a digest of its answer, and the rows
/// it read.
async fn answer_and_rows(
    client: &ChClient,
    db: &str,
    sql: &str,
    settings: &[(String, String)],
    tag: &str,
) -> (String, u64) {
    let target = ChClient::new(conn(db)).await.expect("connect");
    let mut s = QuerySettings::new().set("log_comment", tag);
    for (k, v) in settings {
        if k != "log_comment" {
            s = s.set(k, v);
        }
    }
    let digest = strings(
        &target,
        &format!(
            "SELECT toString(cityHash64(arraySort(groupArray(toString(tuple(*)))))) AS s FROM ({sql})"
        ),
        &s,
    )
    .await;
    // The query log is written behind the answer: flush until the row is
    // there, for at most 30 s.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let rows = loop {
        exec(client, "SYSTEM FLUSH LOGS").await;
        let rows = strings(
            client,
            &format!(
                "SELECT toString(read_rows) AS s FROM system.query_log \
                 WHERE type = 'QueryFinish' AND is_initial_query AND log_comment = '{tag}'"
            ),
            &QuerySettings::new(),
        )
        .await;
        if !rows.is_empty() || std::time::Instant::now() > deadline {
            break rows;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(rows.len(), 1, "{tag}: one finished statement: {rows:?}");
    (digest.join(","), rows[0].parse().expect("a row count"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_and_fetch_read_the_rows_main_reads() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let mut modes: Vec<Option<String>> = vec![None];
    if let Ok(cluster) = std::env::var("PULSUS_TEST_CH_CLUSTER") {
        modes.push(Some(cluster));
    }
    let client = ChClient::new(conn("default")).await.expect("connect");
    let t0 = (chrono_now_ns().div_euclid(DAY_NS) - 1) * DAY_NS;
    for cluster in modes {
        let tag = if cluster.is_some() { "c" } else { "s" };
        let d_main = pulsus_testkit::test_db(&format!("pulsus_read_it_t738_rows_main_{tag}"));
        let d = pulsus_testkit::test_db(&format!("pulsus_read_it_t738_rows_{tag}"));
        build(&client, &d_main, cluster.as_deref(), true, t0).await;
        build(&client, &d, cluster.as_deref(), false, t0).await;

        let prefix = pulsus_testkit::test_ident(&format!("t738_rows_{tag}_{}", chrono_now_ns()));
        let config = read_config(cluster.is_some());
        let tenant =
            pulsus_model::Tenant::from_header(Some(&http::HeaderValue::from_static(TENANT)), false)
                .expect("a valid tenant");
        let engine = TraceEngine::new(
            ChClient::new(conn(&d)).await.expect("connect"),
            config.clone(),
            tenant.clone(),
        )
        .with_statement_prefix(&prefix);
        let hex = strings(
            &client,
            "SELECT lower(hex(toFixedString(unhex(concat(hex(cityHash64(3, 14)), \
             hex(toUInt64(14)))), 16))) AS s",
            &QuerySettings::new(),
        )
        .await
        .remove(0);
        let fetched = engine.fetch_by_id(&hex, None).await.expect("the fetch");
        assert_eq!(fetched.spans.len(), 5, "{tag}: the fetched trace's spans");
        let term = pulsus_read::traces::tenant_scope::TenantTerm::of(&tenant);
        let tables = StatementTables {
            spans: &config.spans_table,
            error_spans: &config.error_spans_table,
            resources: &config.resources_table,
            traces: &config.traces_table,
            indexed: &[],
            tenant: &term,
        };
        for q in [
            r#"{ resource.service.name = "svc-3" && span.http.route = "/r3" }"#,
            "{ status = error }",
            "{}",
            r#"{ name = "op-3" && duration > 49ms }"#,
        ] {
            let request = plan_search_request(
                &pulsus_traceql::parse(q).expect("parses"),
                &SearchParams {
                    start_ns: t0,
                    end_ns: t0 + 7200 * SECOND_NS,
                    limit: 20,
                    spss: 3,
                },
                &tables,
                64,
            )
            .expect("plans");
            let out = engine.search_routed(&request).await.expect("the search");
            assert!(!out.traces.is_empty(), "{tag}: {q} finds traces");
        }

        exec(&client, "SYSTEM FLUSH LOGS").await;
        let sent = strings(
            &client,
            &format!(
                "SELECT concat(query_id, '\\t', \
                   arrayStringConcat(arrayMap((k, v) -> concat(k, '=', v), \
                     mapKeys(Settings), mapValues(Settings)), '\\n'), '\\t', \
                   base64Encode(query)) AS s \
                 FROM system.query_log \
                 WHERE type = 'QueryFinish' AND is_initial_query AND log_comment = '{prefix}' \
                 ORDER BY event_time_microseconds"
            ),
            &QuerySettings::new(),
        )
        .await;
        assert!(
            sent.len() >= 5,
            "{tag}: the engine's statements are in the query log: {}",
            sent.len()
        );
        let mut failures = Vec::new();
        for (n, row) in sent.iter().enumerate() {
            let mut parts = row.splitn(3, '\t');
            let id = parts.next().expect("an id").to_string();
            let settings: Vec<(String, String)> = parts
                .next()
                .expect("settings")
                .lines()
                .filter_map(|kv| kv.split_once('='))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let encoded = parts.next().expect("the query");
            let decoded = strings(
                &client,
                &format!("SELECT base64Decode('{encoded}') AS s"),
                &QuerySettings::new(),
            )
            .await
            .remove(0);
            // The statement `main` sends: the tenant's term removed (§4.1).
            let unscoped = decoded.replace(TERM, "");
            let (got, got_rows) =
                answer_and_rows(&client, &d, &decoded, &settings, &format!("{prefix}_d_{n}")).await;
            let (want, want_rows) = answer_and_rows(
                &client,
                &d_main,
                &unscoped,
                &settings,
                &format!("{prefix}_main_{n}"),
            )
            .await;
            eprintln!("{tag} {id}: rows {got_rows} as {TENANT}, {want_rows} on main");
            if got != want || got_rows != want_rows {
                failures.push(format!(
                    "{id}: answer {got} / {want}, rows {got_rows} / {want_rows} on main"
                ));
            }
        }
        let on = cluster
            .as_deref()
            .map(|c| format!(" ON CLUSTER '{c}'"))
            .unwrap_or_default();
        for db in [&d, &d_main] {
            exec(&client, &format!("DROP DATABASE IF EXISTS {db}{on} SYNC")).await;
        }
        assert!(
            failures.is_empty(),
            "{tag}: each statement reads what main reads: {failures:#?}"
        );
    }
}
