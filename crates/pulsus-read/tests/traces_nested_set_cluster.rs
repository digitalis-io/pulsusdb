//! Issue #594 part 2: a search on a nested-set intrinsic over the
//! **distributed** span and per-trace tables, against the 2-shard cluster.
//!
//! The statement nests a read of `spans_dist` inside a read of
//! `spans_dist` (the numbering a leaf compares against) and of
//! `traces_dist` inside `traces_dist` (the key scalar that finds the
//! traces to number). ClickHouse's default `distributed_product_mode =
//! 'deny'` refuses both; the search sends `'local'` for a statement that
//! reads the numbering, which is exact because both tables are sharded by
//! `cityHash64(trace_id)`. The negative control pins the refusal (code 288)
//! on the same statement without the setting; the positive cases pin the
//! answers, written from the fixture, over traces split across the shards.
//!
//! ```text
//! docker compose -f ci/clickhouse-cluster/compose.yaml up -d
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test traces_nested_set_cluster
//! docker compose -f ci/clickhouse-cluster/compose.yaml down -v
//! ```

use std::time::Duration;

use futures::StreamExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use pulsus_clickhouse::{
    ChClient, ChConnConfig, ChError, ChProto, Idempotency, QuerySettings, Row,
};
use pulsus_read::logql::error::ReadError;
use pulsus_read::traces::spans::rows::SearchTraceRow;
use pulsus_read::traces::spans::search::plan_statement_sliced;
use pulsus_read::{SearchParams, StatementTables, plan_search_request};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;
use pulsus_write::{TraceLandingRow, parse_trace_landing};

const CLUSTER_NAME: &str = "pulsus_test_cluster";
const S: i64 = 1_000_000_000;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with the 2-shard cluster fixture up to \
                 run this test (see this file's module doc)"
            );
            return;
        }
    };
}

fn shard_config(host_env: &str, default_host: &str, database: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var(host_env).unwrap_or_else(|_| default_host.to_string()),
        http_port: 8123,
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 8,
        query_timeout: Duration::from_secs(60),
        ..ChConnConfig::default()
    }
}

fn shard1_config(database: &str) -> ChConnConfig {
    shard_config("PULSUS_TEST_CH_SHARD1_HOST", "172.28.0.11", database)
}

fn shard2_config(database: &str) -> ChConnConfig {
    shard_config("PULSUS_TEST_CH_SHARD2_HOST", "172.28.0.12", database)
}

fn cluster_ctx(db: &str) -> RenderCtx {
    RenderCtx {
        db: db.to_string(),
        cluster: Some(CLUSTER_NAME.to_string()),
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
        metrics_landing_retention_hours: 6,
        metrics_dedup_window: 10_000,
        log_landing_retention_hours: 6,
        log_dedup_window: 10_000,
        trace_landing_retention_hours: 6,
        trace_dedup_window: 10_000,
        trace_indexed_attributes: Vec::new(),
        graph_job_max_rows: pulsus_schema::GRAPH_JOB_MAX_ROWS,
    }
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("now fits in i64")
}

fn id(n: u8) -> Vec<u8> {
    let mut v = vec![0u8; 8];
    v[7] = n;
    v
}

fn span(t: u8, n: u8, parent: u8, at_ns: i64) -> Span {
    Span {
        trace_id: vec![t; 16],
        span_id: id(n),
        parent_span_id: if parent == 0 { Vec::new() } else { id(parent) },
        name: "op".to_string(),
        kind: 2,
        start_time_unix_nano: at_ns as u64,
        end_time_unix_nano: (at_ns + S / 10) as u64,
        ..Span::default()
    }
}

fn request(spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("svc".to_string())),
                    }),
                    ..KeyValue::default()
                }],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope::default()),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Twenty traces `10…` to `23…`, trace `k` starting `k` seconds after
/// `base`: root `01`, `02←01`, `03←02` one and two milliseconds later, and
/// `04` whose parent `ff` is not stored, three milliseconds later. By the
/// reference's rule `01 1/6/-1`, `02 2/5/1`, `03 3/4/2`, `04 0/0/0`.
fn bodies(base: i64) -> Vec<ExportTraceServiceRequest> {
    (0x10..=0x23u8)
        .map(|t| {
            let at = base + i64::from(t - 0x10) * S;
            request(vec![
                span(t, 1, 0, at),
                span(t, 2, 1, at + 1_000_000),
                span(t, 3, 2, at + 2_000_000),
                span(t, 4, 0xff, at + 3_000_000),
            ])
        })
        .collect()
}

async fn land(client: &ChClient, req: &ExportTraceServiceRequest, token: &str) {
    let parsed = parse_trace_landing(req, now_ns()).expect("the landing decode");
    let received_ms = now_ns() / 1_000_000;
    let mut rows: Vec<TraceLandingRow> = Vec::new();
    rows.extend(
        parsed
            .spans
            .iter()
            .cloned()
            .map(|s| TraceLandingRow::span(received_ms, s)),
    );
    rows.extend(
        parsed
            .resources
            .iter()
            .cloned()
            .map(|r| TraceLandingRow::resource(received_ms, r)),
    );
    client
        .insert_block_with(
            "trace_landing",
            &rows,
            &QuerySettings::trace_landing_insert(token, 1_048_576),
        )
        .await
        .expect("the landing insert");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

async fn count(client: &ChClient, sql: &str) -> u64 {
    let mut st = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await
        .expect("count");
    st.next().await.expect("a row").expect("decode").n
}

fn config() -> pulsus_read::TraceReadConfig {
    pulsus_read::TraceReadConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        spans_table: "trace_spans_dist".to_string(),
        attrs_table: "trace_attrs_idx_dist".to_string(),
        edges_table: "trace_edges_dist".to_string(),
        recent_table: "trace_recent_dist".to_string(),
        errors_table: "trace_error_spans_dist".to_string(),
        spans_v2_table: "spans_dist".to_string(),
        traces_table: "traces_dist".to_string(),
        resources_table: "resources".to_string(),
        scan_budget_rows: 50_000_000,
        max_depth: 64,
        max_series: 1_000,
        distributed: true,
        skip_unavailable_shards: false,
    }
}

/// `tt [ss v, …]` per trace, ids by their last byte, each span's projected
/// integers.
fn numbers_of(out: &pulsus_read::traces::SearchOutput) -> String {
    use pulsus_read::traces::GroupValue;
    out.traces
        .iter()
        .map(|t| {
            let spans: Vec<String> = t
                .spans
                .iter()
                .map(|s| {
                    let v: Vec<String> = s
                        .attributes
                        .iter()
                        .map(|a| match a.value() {
                            GroupValue::Int(i) => i.to_string(),
                            other => format!("{other:?}"),
                        })
                        .collect();
                    format!("{:02x} {}", s.span_id[7], v.join("/"))
                })
                .collect();
            format!("{:02x} [{}]", t.trace_id[15], spans.join(", "))
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// `tt {v [ids]; …}` per trace with groups, `tt [ids]` without; `v` is
/// `int:` and the number, or the text.
fn groups_of(out: &pulsus_read::traces::SearchOutput) -> String {
    use pulsus_read::traces::GroupValue;
    let ids = |spans: &[pulsus_read::traces::SpanSummary]| -> String {
        spans
            .iter()
            .map(|s| format!("{:02x}", s.span_id[7]))
            .collect::<Vec<_>>()
            .join(", ")
    };
    out.traces
        .iter()
        .map(|t| match &t.groups {
            None => format!("{:02x} [{}]", t.trace_id[15], ids(&t.spans)),
            Some(groups) => {
                let groups: Vec<String> = groups
                    .iter()
                    .map(|g| {
                        let v = match g.attributes.first().map(|(_, v)| v) {
                            Some(GroupValue::Int(i)) => format!("int:{i}"),
                            Some(GroupValue::Str(s)) => s.clone(),
                            other => format!("{other:?}"),
                        };
                        format!("{v} [{}]", ids(&g.spans))
                    })
                    .collect();
                format!("{:02x} {{{}}}", t.trace_id[15], groups.join("; "))
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// `{ … }`'s answer for every trace `10…` to `23…`, newest first, each
/// span rendered by `each`.
fn every_trace(each: &dyn Fn(u8) -> String) -> String {
    (0x10..=0x23u8)
        .rev()
        .map(|t| format!("{t:02x} [{}]", each(t)))
        .collect::<Vec<_>>()
        .join("; ")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_numbered_search_answers_over_two_shards() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t594p2_cluster");
    let bootstrap = ChClient::new(shard1_config("default"))
        .await
        .expect("connect shard1");
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    run_init(&bootstrap, &cluster_ctx(&db))
        .await
        .expect("run_init");
    let shard1 = ChClient::new(shard1_config(&db))
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config(&db))
        .await
        .expect("connect shard2");

    let base = (now_ns() / S - 120) * S;
    for (i, req) in bodies(base).iter().enumerate() {
        land(&shard1, req, &format!("t594p2-cluster-{i}")).await;
    }
    for t in ["spans_dist", "traces_dist"] {
        exec(&shard1, &format!("SYSTEM FLUSH DISTRIBUTED {db}.{t}")).await;
    }
    let local = (
        count(&shard1, "SELECT count() AS n FROM spans").await,
        count(&shard2, "SELECT count() AS n FROM spans").await,
    );
    assert_eq!(local.0 + local.1, 80, "all spans landed: {local:?}");
    assert!(
        local.0 > 0 && local.1 > 0,
        "the traces split across the shards: {local:?}"
    );

    let engine = pulsus_read::TraceEngine::new(
        ChClient::new(shard1_config(&db))
            .await
            .expect("connect the engine"),
        config(),
    );
    let plan = |query: &str, start: i64, end: i64, limit: u32| {
        plan_search_request(
            &pulsus_traceql::parse(query).expect("parses"),
            &SearchParams {
                start_ns: start,
                end_ns: end,
                limit,
                spss: 10,
            },
            &StatementTables {
                spans: "spans_dist",
                resources: "resources",
                traces: "traces_dist",
                indexed: &[],
            },
            64,
        )
        .expect("plans")
    };
    let window = (base, base + 60 * S);

    // The negative control: the statement, without the setting, is refused.
    let p = plan("{ nestedSetLeft > 0 }", window.0, window.1, 30);
    let stmt = plan_statement_sliced(&p, "spans_dist", "traces_dist", "resources", 64, &[], None)
        .expect("covered");
    let settings = QuerySettings::clustered_reader(false)
        .set("final", 1)
        .set("do_not_merge_across_partitions_select_final", 1);
    let refused = async {
        let mut st = shard1
            .query_stream::<SearchTraceRow>(&stmt.sql().replace('?', "??"), &settings)
            .await?;
        while let Some(row) = st.next().await {
            row?;
        }
        Ok::<(), ChError>(())
    }
    .await;
    match refused {
        Err(ChError::Server { code, .. }) => {
            assert_eq!(code, 288, "DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED")
        }
        other => panic!("expected code 288 without the setting, got {other:?}"),
    }

    let mut wrong = Vec::new();
    for (query, (start, end), limit, want) in [
        (
            "{ nestedSetLeft > 0 }",
            window,
            30,
            every_trace(&|_| "01 1, 02 2, 03 3".to_string()),
        ),
        (
            "{ nestedSetParent = 0 }",
            window,
            30,
            every_trace(&|_| "04 0".to_string()),
        ),
        (
            "{ nestedSetRight = 4 }",
            window,
            30,
            every_trace(&|_| "03 4".to_string()),
        ),
        // The newest slice of a three-hour window holds all twenty traces,
        // so the first sliced statement answers.
        (
            "{ nestedSetParent = 2 }",
            (base + 60 * S - 10_800 * S, base + 60 * S),
            2,
            "23 [03 2]; 22 [03 2]".to_string(),
        ),
    ] {
        let got = engine.search_routed(&plan(query, start, end, limit)).await;
        let got = match got {
            Ok(out) => numbers_of(&out),
            Err(ReadError::Clickhouse(e)) => format!("Err({e})"),
            Err(e) => format!("Err({e})"),
        };
        if got != want {
            wrong.push(format!("{query}\n  want: {want}\n  got:  {got}"));
        }
    }
    // Issue #594 part 3: the seven as by() keys and a per-trace value as an
    // operand, over the same fixture. Each trace's extent is 103 ms.
    for (query, each) in [
        (
            "{ } | by(nestedSetParent)",
            "{int:-1 [01]; int:1 [02]; int:2 [03]; int:0 [04]}",
        ),
        (
            "{ } | by(span:childCount)",
            "{int:1 [01, 02]; int:0 [03, 04]}",
        ),
        ("{ } | by(trace:duration)", "{103ms [01, 02, 03, 04]}"),
        ("{ duration * 2 > trace:duration }", "[01, 02, 03, 04]"),
    ] {
        let want = (0x10..=0x23u8)
            .rev()
            .map(|t| format!("{t:02x} {each}"))
            .collect::<Vec<_>>()
            .join("; ");
        let got = match engine
            .search_routed(&plan(query, window.0, window.1, 30))
            .await
        {
            Ok(out) => groups_of(&out),
            Err(e) => format!("Err({e})"),
        };
        if got != want {
            wrong.push(format!("{query}\n  want: {want}\n  got:  {got}"));
        }
    }
    // Issue #594 part 4: a child count or a number as an operand, a
    // numbered comparison before an aggregate, and select() of a number.
    for (query, numbers, each) in [
        // Issue #602 part 2b: a structural search reads `spans_dist`
        // inside `spans_dist`, sent shard-local as the numbering is.
        ("{ } > { }", false, "[02, 03]"),
        ("{ } >> { }", false, "[02, 03]"),
        ("{ } << { }", false, "[01, 02]"),
        ("{ nestedSetRight - nestedSetLeft = 1 }", false, "[03]"),
        ("{ span:childCount * 1s > duration }", false, "[01, 02]"),
        (
            "{ nestedSetLeft > 0 } | count() > 2",
            false,
            "{int:3 [01, 02, 03]}",
        ),
        (
            "{ } | select(nestedSetParent)",
            true,
            "[01 -1, 02 1, 03 2, 04 0]",
        ),
    ] {
        let want = (0x10..=0x23u8)
            .rev()
            .map(|t| format!("{t:02x} {each}"))
            .collect::<Vec<_>>()
            .join("; ");
        let got = match engine
            .search_routed(&plan(query, window.0, window.1, 30))
            .await
        {
            Ok(out) if numbers => numbers_of(&out),
            Ok(out) => groups_of(&out),
            Err(e) => format!("Err({e})"),
        };
        if got != want {
            wrong.push(format!("{query}\n  want: {want}\n  got:  {got}"));
        }
    }
    // Issue #594 part 5: the seven in a metrics filter, one statement over
    // the distributed tables, instant over the minute holding the traces.
    let tables = pulsus_read::StatementTables {
        spans: "spans_dist",
        resources: "resources",
        traces: "traces_dist",
        indexed: &[],
    };
    for (query, value) in [
        ("{ nestedSetLeft > 0 } | count_over_time()", 60.0),
        ("{ nestedSetParent = 0 } | count_over_time()", 20.0),
        ("{ span:childCount = 1 } | count_over_time()", 40.0),
        ("{ trace:duration > 100ms } | count_over_time()", 80.0),
    ] {
        let routed = pulsus_read::plan_trace_metrics_routed(
            &pulsus_traceql::parse(query).expect("parses"),
            &pulsus_read::MetricsParams {
                start_ns: window.0,
                end_ns: window.1,
                step_ms: 60_000,
                exemplars: None,
            },
            &engine.metrics_ctx(),
            &tables,
        );
        let routed = match routed {
            Ok(r) if r.statement().is_some() => r,
            Ok(_) => {
                wrong.push(format!("{query}: no statement"));
                continue;
            }
            Err(e) => {
                wrong.push(format!("{query}: refused: {e}"));
                continue;
            }
        };
        let want = format!("[{value:?}]");
        let got = match engine.metrics_instant_routed(&routed).await {
            Ok(out) => format!(
                "{:?}",
                out.series
                    .iter()
                    .flat_map(|s| s.samples.iter().map(|(_, v)| *v))
                    .collect::<Vec<f64>>()
            ),
            Err(e) => format!("Err({e})"),
        };
        if got != want {
            wrong.push(format!("{query}\n  want: {want}\n  got:  {got}"));
        }
    }
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// One span of service `svc` carrying `http.route`.
fn routed_request(
    svc: &str,
    trace: u8,
    spans: Vec<(u8, u8, &str, i64, i64)>,
    route: &str,
) -> ExportTraceServiceRequest {
    let kv = |k: &str, v: &str| KeyValue {
        key: k.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(v.to_string())),
        }),
        ..KeyValue::default()
    };
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", svc)],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope::default()),
                spans: spans
                    .into_iter()
                    .map(|(n, parent, name, at, dur)| Span {
                        trace_id: vec![trace; 16],
                        span_id: id(n),
                        parent_span_id: if parent == 0 { Vec::new() } else { id(parent) },
                        name: name.to_string(),
                        kind: 2,
                        start_time_unix_nano: at as u64,
                        end_time_unix_nano: (at + dur) as u64,
                        attributes: vec![kv("http.route", route)],
                        ..Span::default()
                    })
                    .collect(),
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// R14 (issue #598 part 2): over the two shards, each shard's stopped
/// read is its newest one, and a trace-level condition reads the per-trace
/// set across the shards (`GLOBAL IN`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_tag_read_is_the_newest_on_every_shard() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db(&format!("pulsus_read_it_t598p2_cluster_{}", now_ns()));
    let bootstrap = ChClient::new(shard1_config("default"))
        .await
        .expect("connect shard1");
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    run_init(&bootstrap, &cluster_ctx(&db))
        .await
        .expect("run_init");
    let shard1 = ChClient::new(shard1_config(&db))
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config(&db))
        .await
        .expect("connect shard2");
    let now = now_ns();
    const MIN: i64 = 60 * S;
    for (client, svc) in [(&shard1, "a"), (&shard2, "b")] {
        for (age, label, from, spread) in [
            ("old", 0, now - 120 * MIN, MIN),
            ("new", 1, now - 2 * MIN, 2 * MIN - 1),
        ] {
            exec(
                client,
                &format!(
                    "INSERT INTO spans (trace_id, span_id, start_ns, duration_ns, service, name, \
                     kind, attrs, end_ns, service_type) \
                     SELECT sipHash128(number, '{svc}{label}'), \
                     reinterpretAsFixedString(cityHash64(number, '{svc}{label}')), \
                     {from} + intDiv(toInt64(number) * {spread}, 100000), 1000, '{svc}', \
                     'op.{age}-{svc}', 2, \
                     CAST('{{\"http%2Eroute\":\"/{age}-{svc}\"}}', 'JSON'), \
                     toUInt64({from} + intDiv(toInt64(number) * {spread}, 100000) + 1000), 'string' \
                     FROM numbers(100000)"
                ),
            )
            .await;
        }
    }
    let req = pulsus_read::TagValuesRequest {
        q: Some(r#"{resource.service.name="a" || resource.service.name="b"}"#),
        start_ns: now - 180 * MIN,
        end_ns: now,
    };
    let prefix = pulsus_testkit::test_ident(&format!("t598p2_cluster_{}", now_ns()));
    let engine = pulsus_read::TraceEngine::new(
        ChClient::new(shard1_config(&db))
            .await
            .expect("connect the engine"),
        config(),
    )
    .with_tag_read_rows(10_000)
    .with_statement_prefix(&prefix);
    let values = engine
        .list_tag_values("http.route", Some("span"), req)
        .await
        .expect("the values read answers");
    assert_eq!(
        values
            .values
            .iter()
            .map(|v| v.val.as_str())
            .collect::<Vec<_>>(),
        ["/new-a", "/new-b"]
    );
    let names = engine
        .list_span_name_values(req)
        .await
        .expect("the names read answers");
    assert_eq!(
        names
            .values
            .iter()
            .map(|v| v.val.as_str())
            .collect::<Vec<_>>(),
        ["op.new-a", "op.new-b"]
    );
    // Shard1's read is proven by the answer: `/new-a` and `op.new-a` are stored only there.
    exec(&shard2, "SYSTEM FLUSH LOGS").await;
    let remote = count(
        &shard2,
        &format!(
            "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' \
             AND is_initial_query = 0 AND startsWith(initial_query_id, '{prefix}-')"
        ),
    )
    .await;
    assert!(remote > 0, "the read reached shard2: {remote}");

    // The trace-level condition, over the per-trace table of both shards.
    land(
        &shard1,
        &routed_request(
            "a",
            0x5a,
            vec![
                (1, 0, "slow.root", now - MIN, S / 1000),
                (2, 1, "slow.child", now - MIN + 2 * S, S / 1000),
            ],
            "/slow-a",
        ),
        "t598p2-slow",
    )
    .await;
    land(
        &shard1,
        &routed_request(
            "b",
            0x5b,
            vec![(1, 0, "fast.root", now - MIN, S / 100)],
            "/fast-b",
        ),
        "t598p2-fast",
    )
    .await;
    for t in ["spans_dist", "traces_dist"] {
        exec(&shard1, &format!("SYSTEM FLUSH DISTRIBUTED {db}.{t}")).await;
    }
    let engine = pulsus_read::TraceEngine::new(
        ChClient::new(shard1_config(&db))
            .await
            .expect("connect the engine"),
        config(),
    );
    let slow = engine
        .list_tag_values(
            "http.route",
            Some("span"),
            pulsus_read::TagValuesRequest {
                q: Some("{traceDuration > 1s}"),
                start_ns: now - 180 * MIN,
                end_ns: now_ns() + MIN,
            },
        )
        .await
        .expect("the trace-level read answers");
    assert_eq!(
        slow.values
            .iter()
            .map(|v| v.val.as_str())
            .collect::<Vec<_>>(),
        ["/slow-a"]
    );
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

/// R14's uneven case (issue #598 part 2a): one cut for the whole
/// statement, not one per shard. Shard `a` holds only new spans and shard
/// `b` only old ones; a cut taken per shard would list `/old-b`, shard
/// `b`'s own newest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_cut_holds_for_every_shard() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db(&format!("pulsus_read_it_t598p2a_uneven_{}", now_ns()));
    let bootstrap = ChClient::new(shard1_config("default"))
        .await
        .expect("connect shard1");
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    run_init(&bootstrap, &cluster_ctx(&db))
        .await
        .expect("run_init");
    let shard1 = ChClient::new(shard1_config(&db))
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config(&db))
        .await
        .expect("connect shard2");
    let now = now_ns();
    const MIN: i64 = 60 * S;
    for (client, svc, age, from, spread) in [
        (&shard1, "a", "new", now - 2 * MIN, 2 * MIN - 1),
        (&shard2, "b", "old", now - 60 * MIN, MIN),
    ] {
        exec(
            client,
            &format!(
                "INSERT INTO spans (trace_id, span_id, start_ns, duration_ns, service, name, \
                 kind, attrs, end_ns, service_type) \
                 SELECT sipHash128(number, '{svc}'), \
                 reinterpretAsFixedString(cityHash64(number, '{svc}')), \
                 {from} + intDiv(toInt64(number) * {spread}, 100000), 1000, '{svc}', \
                 'op.{age}-{svc}', 2, \
                 CAST('{{\"http%2Eroute\":\"/{age}-{svc}\"}}', 'JSON'), \
                 toUInt64({from} + intDiv(toInt64(number) * {spread}, 100000) + 1000), 'string' \
                 FROM numbers(100000)"
            ),
        )
        .await;
    }
    let engine = pulsus_read::TraceEngine::new(
        ChClient::new(shard1_config(&db))
            .await
            .expect("connect the engine"),
        config(),
    )
    .with_tag_read_rows(10_000);
    let values = engine
        .list_tag_values(
            "http.route",
            Some("span"),
            pulsus_read::TagValuesRequest {
                q: Some(r#"{resource.service.name="a" || resource.service.name="b"}"#),
                start_ns: now - 180 * MIN,
                end_ns: now,
            },
        )
        .await
        .expect("the values read answers");
    assert_eq!(
        values
            .values
            .iter()
            .map(|v| v.val.as_str())
            .collect::<Vec<_>>(),
        ["/new-a"]
    );
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

/// C1 (issue #597): `compare()`'s `topN` is global. Shard a holds `v = "p"`
/// ×5 and `"q"` ×4, shard b `"q"` ×3 and `"r"` ×6, one root span a trace;
/// with `topN` 1 the baseline keeps `q`, 7, which neither shard ranks first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_compare_top_n_is_ranked_over_every_shard() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db(&format!("pulsus_read_it_t597_topn_{}", now_ns()));
    let bootstrap = ChClient::new(shard1_config("default"))
        .await
        .expect("connect shard1");
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    run_init(&bootstrap, &cluster_ctx(&db))
        .await
        .expect("run_init");
    let shard1 = ChClient::new(shard1_config(&db))
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config(&db))
        .await
        .expect("connect shard2");
    let now = now_ns();
    let at = now - 120 * S;
    for (client, shard, values) in [
        (&shard1, "a", [("p", 5u64), ("q", 4)]),
        (&shard2, "b", [("q", 3), ("r", 6)]),
    ] {
        for (v, n) in values {
            exec(
                client,
                &format!(
                    "INSERT INTO spans (trace_id, span_id, start_ns, duration_ns, service, name, \
                     kind, attrs, end_ns, service_type) \
                     SELECT sipHash128(number, '{shard}{v}'), \
                     reinterpretAsFixedString(cityHash64(number, '{shard}{v}')), \
                     {at} + toInt64(number), 1000, 'svc-{shard}', 'op', 2, \
                     CAST('{{\"v\":\"{v}\"}}', 'JSON'), toUInt64({at} + toInt64(number) + 1000), \
                     'string' FROM numbers({n})"
                ),
            )
            .await;
        }
    }
    let engine = pulsus_read::TraceEngine::new(
        ChClient::new(shard1_config(&db))
            .await
            .expect("connect the engine"),
        config(),
    );
    let read = config();
    let tables = StatementTables {
        spans: &read.spans_v2_table,
        resources: &read.resources_table,
        traces: &read.traces_table,
        indexed: &[],
    };
    let q = "{} | compare({ status = error }, 1)";
    let routed = pulsus_read::plan_trace_metrics_routed(
        &pulsus_traceql::parse(q).expect("parse"),
        &pulsus_read::MetricsParams {
            start_ns: (now / S - 600) * S,
            end_ns: (now / S) * S,
            step_ms: 60_000,
            exemplars: None,
        },
        &engine.metrics_ctx(),
        &tables,
    )
    .expect("planned");
    assert!(routed.statement().is_some(), "{q}: no statement");
    let out = engine
        .metrics_range_routed(&routed)
        .await
        .expect("the comparison answers");
    let mut baseline = std::collections::BTreeMap::new();
    for s in &out.series {
        let meta = s.labels.iter().find(|l| l.key == "__meta_type");
        let key = s.labels.iter().find(|l| l.key == "span.v");
        if let (Some(meta), Some(key)) = (meta, key)
            && meta.value == pulsus_read::MetricLabelValue::Str("baseline".to_string())
            && key.value != pulsus_read::MetricLabelValue::Str("nil".to_string())
        {
            let n: f64 = s.samples.iter().map(|(_, v)| *v).sum();
            baseline.insert(format!("{:?}", key.value), n);
        }
    }
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    assert_eq!(
        baseline,
        [("Str(\"q\")".to_string(), 7.0)]
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>(),
        "baseline span.v"
    );
}

// =====================================================================
// Issue #599 part A: the service graph over two shards
// =====================================================================
//
// The job runs on each shard's own node into that shard's `service_edges`,
// and the route reads each shard through `cluster('<c>', view(…))`, so a
// shard computes its coverage from its own markers. The helpers address one
// node each, as the cases name them.

const MINUTE: i64 = 60 * S;
const BUCKET: i64 = 5 * MINUTE;

fn floor_to(ns: i64, unit: i64) -> i64 {
    ns.div_euclid(unit) * unit
}

async fn strings(client: &ChClient, sql: &str) -> Vec<String> {
    client
        .query_strings(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// The node's job stopped, and no run in flight.
async fn stop_edges_view(client: &ChClient) {
    exec(client, "SYSTEM STOP VIEW service_edges_mv").await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let status = strings(
            client,
            "SELECT status AS s FROM system.view_refreshes WHERE database = currentDatabase() \
             AND view = 'service_edges_mv'",
        )
        .await;
        if status == ["Disabled"] {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the edge job did not stop: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Part A §4's `start_edges` on one node.
async fn start_edges(client: &ChClient, marker_ns: i64) {
    stop_edges_view(client).await;
    exec(client, "TRUNCATE TABLE service_edges").await;
    exec(
        client,
        &format!(
            "INSERT INTO service_edges SELECT {t}, {t}, '', '', '', 0, 0, \
             arrayReduce('quantilesTDigestState(0.5, 0.95, 0.99)', [toInt64(0)]), 1",
            t = format_args!("toDateTime({}, 'UTC')", marker_ns / S)
        ),
    )
    .await;
    exec(client, "SYSTEM START VIEW service_edges_mv").await;
}

/// The end of the node's complete prefix of markers, §3.3's `last`.
async fn prefix_end(client: &ChClient) -> i64 {
    strings(
        client,
        "SELECT toString(toUnixTimestamp(ms[arrayFirstIndex(i -> i = length(ms) \
         OR ms[i + 1] - ms[i] != 300, arrayEnumerate(ms))])) AS s \
         FROM (SELECT arraySort(groupArray(minute)) AS ms FROM service_edges FINAL \
         WHERE conn_type = '')",
    )
    .await
    .first()
    .map(|s| s.parse::<i64>().expect("a timestamp") * S)
    .unwrap_or(0)
}

/// Part A §4's `run_edges_until` on one node.
async fn run_edges_until(client: &ChClient, until_ns: i64) {
    exec(client, "SYSTEM START VIEW service_edges_mv").await;
    let mut runs = 0;
    while prefix_end(client).await < until_ns {
        assert!(runs < 400, "the edge job did not reach {until_ns}");
        exec(client, "SYSTEM REFRESH VIEW service_edges_mv").await;
        exec(client, "SYSTEM WAIT VIEW service_edges_mv").await;
        runs += 1;
    }
}

/// Part A §4's `process_edges` on one node.
async fn process_edges(client: &ChClient, marker_ns: i64, until_ns: i64) {
    start_edges(client, marker_ns).await;
    run_edges_until(client, until_ns).await;
}

/// `calls` calls of `svc-a → server`, both halves stored on this node's
/// own `spans`, from `at` on.
async fn place_calls(client: &ChClient, tag: &str, server: &str, calls: u64, at: i64) {
    exec(
        client,
        &format!(
            "INSERT INTO spans (trace_id, span_id, parent_span_id, start_ns, duration_ns, \
             service, name, kind, attrs, end_ns, service_type) \
             SELECT sipHash128(number, '{tag}'), \
                    reinterpretAsFixedString(cityHash64(number, '{tag}', k)), \
                    if(k = 2, reinterpretAsFixedString(cityHash64(number, '{tag}', toUInt8(3))), \
                       toFixedString('', 8)), \
                    {at} + toInt64(number) * 1000000 AS st, 1000000, \
                    if(k = 3, 'svc-a', '{server}'), 'op', k, CAST('{{}}', 'JSON'), \
                    toUInt64(st + 1000000), 'string' \
             FROM numbers({calls}) ARRAY JOIN [toUInt8(3), toUInt8(2)] AS k"
        ),
    )
    .await;
}

/// C1's placement, #598 part 2a's R14 shape: one shard alone holds
/// `svc-x`, the other alone `svc-z`, and both hold some `svc-y`.
async fn place_c1(shard1: &ChClient, shard2: &ChClient, l: i64, only_shard2: bool) {
    if !only_shard2 {
        place_calls(shard1, "c1x", "svc-x", 2, l + 10 * S).await;
        place_calls(shard1, "c1y", "svc-y", 1, l + 20 * S).await;
    }
    place_calls(shard2, "c2y", "svc-y", 3, l + 30 * S).await;
    place_calls(shard2, "c2z", "svc-z", 1, l + 40 * S).await;
}

/// `(client, server, calls)` of a graph, in order.
fn calls_of(graph: &pulsus_read::ServiceGraph) -> Vec<(String, String, u64)> {
    graph
        .edges
        .iter()
        .map(|e| (e.client.clone(), e.server.clone(), e.calls))
        .collect()
}

fn c1_edges() -> Vec<(String, String, u64)> {
    [("svc-y", 4), ("svc-x", 2), ("svc-z", 1)]
        .into_iter()
        .map(|(s, n)| ("svc-a".to_string(), s.to_string(), n))
        .collect()
}

/// C1, C3 and C2 (issue #599 part A §5.3): the graph over the two shards
/// from spans and from totals, each shard reading its own coverage; a
/// reader whose routing table names the storage cluster; and a shard with
/// no coverage answering from its spans beside one answering from totals.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_service_graph_answers_over_two_shards() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db(&format!("pulsus_read_it_t599_cluster_{}", now_ns()));
    let reader_db = format!("{db}_q");
    let bootstrap = ChClient::new(shard1_config("default"))
        .await
        .expect("connect shard1");
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {reader_db} SYNC"),
    )
    .await;
    run_init(&bootstrap, &cluster_ctx(&db))
        .await
        .expect("run_init");
    let shard1 = ChClient::new(shard1_config(&db))
        .await
        .expect("connect shard1");
    let shard2 = ChClient::new(shard2_config(&db))
        .await
        .expect("connect shard2");
    let l = floor_to(now_ns() - 30 * MINUTE, BUCKET);
    for c in [&shard1, &shard2] {
        exec(c, "SYSTEM STOP MERGES spans").await;
    }
    place_c1(&shard1, &shard2, l, false).await;
    let window = pulsus_read::GraphWindow {
        start_ns: l,
        end_ns: l + 5 * MINUTE,
    };
    let engine = |prefix: &str| {
        let prefix = prefix.to_string();
        let db = db.clone();
        async move {
            pulsus_read::TraceEngine::new(
                ChClient::new(shard1_config(&db))
                    .await
                    .expect("connect the engine"),
                config(),
            )
            .with_statement_prefix(&prefix)
        }
    };

    // C1, from spans: a fresh schema's markers are all after L.
    let graph = engine(&pulsus_testkit::test_ident(&format!(
        "t599c1s_{}",
        now_ns()
    )))
    .await
    .service_graph(window)
    .await
    .expect("C1 from spans");
    assert_eq!(calls_of(&graph), c1_edges(), "C1 from spans");

    // C1, shard 2 one bucket behind shard 1: the answer is unchanged.
    process_edges(&shard1, l - 6 * MINUTE, l + 9 * MINUTE).await;
    process_edges(&shard2, l - 6 * MINUTE, l + 4 * MINUTE).await;
    stop_edges_view(&shard2).await;
    let wide = pulsus_read::GraphWindow {
        start_ns: l - 5 * MINUTE,
        end_ns: l + 10 * MINUTE,
    };
    let graph = engine(&pulsus_testkit::test_ident(&format!(
        "t599c1b_{}",
        now_ns()
    )))
    .await
    .service_graph(wide)
    .await
    .expect("C1 one bucket behind");
    assert_eq!(
        calls_of(&graph),
        c1_edges(),
        "C1, shard 2 one bucket behind"
    );

    // C3: a reader database holding only a `spans_q` routing table onto the
    // storage database. Two engines over one target cache, as the server
    // builds one engine per request: the first reads the target, the
    // second reuses it, and both answer.
    exec(&shard1, &format!("CREATE DATABASE {reader_db}")).await;
    exec(
        &shard1,
        &format!(
            "CREATE TABLE {reader_db}.spans_q AS {db}.spans \
             ENGINE = Distributed('{CLUSTER_NAME}', '{db}', 'spans', cityHash64(trace_id))"
        ),
    )
    .await;
    let cache = pulsus_read::GraphTargetCache::default();
    let mut lookups = Vec::new();
    for n in 0..2 {
        let prefix = pulsus_testkit::test_ident(&format!("t599c3_{n}_{}", now_ns()));
        let reader = pulsus_read::TraceEngine::new(
            ChClient::new(shard1_config(&reader_db))
                .await
                .expect("connect the reader"),
            pulsus_read::TraceReadConfig {
                spans_v2_table: "spans_q".to_string(),
                ..config()
            },
        )
        .with_graph_target_cache(cache.clone())
        .with_statement_prefix(&prefix);
        let graph = reader.service_graph(window).await.expect("C3 answers");
        assert_eq!(calls_of(&graph), c1_edges(), "C3 request {n}");
        exec(&shard1, "SYSTEM FLUSH LOGS").await;
        lookups.push(
            count(
                &shard1,
                &format!(
                    "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' \
                     AND is_initial_query AND startsWith(query_id, '{prefix}-') \
                     AND position(query, 'engine_full') > 0"
                ),
            )
            .await,
        );
    }
    assert_eq!(lookups, vec![1, 0], "C3: the target is read once and kept");

    // C1, from totals.
    run_edges_until(&shard2, l + 9 * MINUTE).await;
    for c in [&shard1, &shard2] {
        exec(c, "TRUNCATE TABLE spans").await;
    }
    let prefix = pulsus_testkit::test_ident(&format!("t599c1t_{}", now_ns()));
    let graph = engine(&prefix)
        .await
        .service_graph(window)
        .await
        .expect("C1 from totals");
    assert_eq!(calls_of(&graph), c1_edges(), "C1 from totals");
    exec(&shard2, "SYSTEM FLUSH LOGS").await;
    let remote = count(
        &shard2,
        &format!(
            "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' \
             AND is_initial_query = 0 AND startsWith(initial_query_id, '{prefix}-') \
             AND has(tables, '{db}.service_edges')"
        ),
    )
    .await;
    assert!(
        remote > 0,
        "C1: shard 2 read its own service_edges: {remote}"
    );

    // C2: shard 2 with no coverage at all pairs from its spans while shard
    // 1 sums its totals (its spans are gone).
    place_c1(&shard1, &shard2, l, true).await;
    stop_edges_view(&shard2).await;
    exec(&shard2, "TRUNCATE TABLE service_edges").await;
    let graph = engine(&pulsus_testkit::test_ident(&format!("t599c2_{}", now_ns())))
        .await
        .service_graph(window)
        .await
        .expect("C2");
    assert_eq!(calls_of(&graph), c1_edges(), "C2");

    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {reader_db} SYNC"),
    )
    .await;
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}
