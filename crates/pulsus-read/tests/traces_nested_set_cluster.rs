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
/// `svc-x`, the other alone `svc-z`, and both hold some `svc-y`. `again`
/// places shard 2's calls a second time under new ids: a byte-identical
/// block would be dropped by the replicated table's block deduplication,
/// which `TRUNCATE` does not reset.
async fn place_c1(shard1: &ChClient, shard2: &ChClient, l: i64, again: bool) {
    let tag = if again { "again" } else { "first" };
    if !again {
        place_calls(shard1, "c1x", "svc-x", 2, l + 10 * S).await;
        place_calls(shard1, "c1y", "svc-y", 1, l + 20 * S).await;
    }
    place_calls(shard2, &format!("c2y{tag}"), "svc-y", 3, l + 30 * S).await;
    place_calls(shard2, &format!("c2z{tag}"), "svc-z", 1, l + 40 * S).await;
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

/// C1, C3, C2 and C4 (issue #599 part A §5.3, part A2 §6): the graph over the two shards
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

    // C4 (part A2): each shard's span read keeps its primary-key pruning.
    // A million spans per shard in the window's UTC day but outside its
    // buckets; the request reads its few granules, not the partition, on
    // every shard including the initiator's.
    let day = floor_to(l, 24 * 60 * MINUTE);
    let (from, until) = if l - 5 * MINUTE - day >= 5 * MINUTE {
        (day, l - 5 * MINUTE)
    } else {
        (l + 10 * MINUTE, day + 24 * 60 * MINUTE)
    };
    for c in [&shard1, &shard2] {
        exec(
            c,
            &format!(
                "INSERT INTO spans (trace_id, span_id, parent_span_id, start_ns, duration_ns, \
                 service, name, kind, attrs, end_ns, service_type) \
                 SELECT sipHash128(number, 'c4'), reinterpretAsFixedString(cityHash64(number, 'c4')), \
                        toFixedString('', 8), \
                        {from} + intDiv(toInt64(number) * ({until} - {from}), 1000000) AS st, \
                        1000, 'svc-bulk', 'op', 1, CAST('{{}}', 'JSON'), toUInt64(st + 1000), 'string' \
                 FROM numbers(1000000)"
            ),
        )
        .await;
    }
    let prefix = pulsus_testkit::test_ident(&format!("t599c4_{}", now_ns()));
    let graph = engine(&prefix)
        .await
        .service_graph(window)
        .await
        .expect("C4");
    assert_eq!(calls_of(&graph), c1_edges(), "C4");
    let mut read = 0;
    for c in [&shard1, &shard2] {
        exec(c, "SYSTEM FLUSH LOGS").await;
        read += count(
            c,
            &format!(
                "SELECT sum(read_rows) AS n FROM system.query_log WHERE type = 'QueryFinish' \
                 AND startsWith(initial_query_id, '{prefix}-')"
            ),
        )
        .await;
        let statements = count(
            c,
            &format!(
                "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' \
                 AND startsWith(initial_query_id, '{prefix}-') AND has(tables, '{db}.spans')"
            ),
        )
        .await;
        assert!(
            statements > 0,
            "C4: a statement of the request read spans on every node"
        );
    }
    assert!(
        read <= 50_000,
        "C4: the request reads a few granules on each shard, not the day's partition: {read}"
    );

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

// Issue #599 part B: shard 1 with two replicas
// =====================================================================
//
// Shard 1 has two replicas, `ch-shard1` (the first listed) and `ch-shard1b`;
// every replica runs the job into the one shared `service_edges`, and a run
// pairs a bucket only when its replica holds every part the shard has
// announced for `spans` and `service_edges`. Each case below names the node
// it acts on. Every view is stopped while a case builds its state and is
// started only for the refresh the case names, so no scheduled run acts
// first; merges are stopped on both tables of both replicas, so neither a
// merge nor its replication-log entry changes what a run or a check reads.

fn shard1b_config(database: &str) -> ChConnConfig {
    shard_config("PULSUS_TEST_CH_SHARD1B_HOST", "172.28.0.13", database)
}

/// A replica's replication state for one table, from `system.replicas`.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
struct ReplicaState {
    inserts_in_queue: u64,
    log_pointer: u64,
    log_max_index: u64,
}

impl ReplicaState {
    /// No announced part still to fetch, and every log entry copied.
    fn synced(&self) -> bool {
        self.inserts_in_queue == 0 && self.log_pointer > self.log_max_index
    }
}

async fn replica_state(client: &ChClient, db: &str, table: &str) -> ReplicaState {
    let sql = format!(
        "SELECT toUInt64(inserts_in_queue) AS inserts_in_queue, log_pointer, log_max_index \
         FROM system.replicas WHERE database = '{db}' AND table = '{table}'"
    );
    let mut st = client
        .query_stream::<ReplicaState>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    st.next()
        .await
        .unwrap_or_else(|| panic!("{db}.{table} is replicated on this node"))
        .expect("decode a replica state")
}

/// Polls `db.table`'s replica state until `want` holds, for up to two
/// minutes; the test fails, rather than goes on, if it never does.
async fn wait_state(
    client: &ChClient,
    db: &str,
    table: &str,
    what: &str,
    want: impl Fn(&ReplicaState) -> bool,
) -> ReplicaState {
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let state = replica_state(client, db, table).await;
        if want(&state) {
            return state;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: {db}.{table} never reached the state: {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Both tables of the job synced on every client given.
async fn wait_synced(clients: &[&ChClient], db: &str, what: &str) {
    for c in clients {
        for table in ["spans", "service_edges"] {
            wait_state(c, db, table, what, ReplicaState::synced).await;
        }
    }
}

/// The node's markers under `FINAL`, oldest first, in ns.
async fn markers(client: &ChClient) -> Vec<i64> {
    strings(
        client,
        "SELECT toString(toUnixTimestamp(minute)) AS s FROM service_edges FINAL \
         WHERE conn_type = '' ORDER BY minute",
    )
    .await
    .into_iter()
    .map(|s| s.parse::<i64>().expect("a timestamp") * S)
    .collect()
}

/// The server's clock, in microseconds.
async fn server_now_us(client: &ChClient) -> i64 {
    strings(
        client,
        "SELECT toString(toUnixTimestamp64Micro(now64(6))) AS s",
    )
    .await[0]
        .parse()
        .expect("a timestamp")
}

/// Every view of the database stopped on every node, then `start_edges`'s
/// state on `client` without its start: no marker but one at `marker_ns`.
async fn seed_edges(clients: &[&ChClient], client: &ChClient, marker_ns: i64) {
    for c in clients {
        stop_edges_view(c).await;
    }
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
}

/// One refresh of the node's job: the view started for it, a run finished
/// after the start (its `system.query_log` row), no refresh exception, and
/// the view stopped again.
async fn refresh_on(client: &ChClient, db: &str, what: &str) {
    let t0 = server_now_us(client).await;
    exec(client, "SYSTEM START VIEW service_edges_mv").await;
    exec(client, "SYSTEM REFRESH VIEW service_edges_mv").await;
    exec(client, "SYSTEM WAIT VIEW service_edges_mv").await;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        exec(client, "SYSTEM FLUSH LOGS").await;
        let finished = count(
            client,
            &format!(
                "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' \
                 AND log_comment = 'refresh of {db}.service_edges_mv' \
                 AND event_time_microseconds >= fromUnixTimestamp64Micro(toInt64({t0}))"
            ),
        )
        .await;
        if finished > 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: no refresh finished on the node"
        );
        exec(client, "SYSTEM WAIT VIEW service_edges_mv").await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    exec(client, "SYSTEM WAIT VIEW service_edges_mv").await;
    let failed = strings(
        client,
        "SELECT exception AS s FROM system.view_refreshes WHERE database = currentDatabase() \
         AND view = 'service_edges_mv'",
    )
    .await;
    assert_eq!(failed, [""], "{what}: the refresh succeeded");
    stop_edges_view(client).await;
    // Part B §4: each run's duration, the barrier's read included.
    let durations = strings(
        client,
        &format!(
            "SELECT toString(query_duration_ms) AS s FROM system.query_log \
             WHERE type = 'QueryFinish' AND log_comment = 'refresh of {db}.service_edges_mv' \
             AND event_time_microseconds >= fromUnixTimestamp64Micro(toInt64({t0})) \
             ORDER BY event_time_microseconds"
        ),
    )
    .await;
    println!("{what}: refresh durations, ms: {durations:?}");
}

/// Refreshes on `client` until its prefix reaches `until_ns`, at most
/// `tries` times; the view stopped after.
async fn refresh_until(client: &ChClient, db: &str, until_ns: i64, tries: u32, what: &str) {
    for _ in 0..tries {
        if prefix_end(client).await >= until_ns {
            return;
        }
        refresh_on(client, db, what).await;
    }
    assert!(
        prefix_end(client).await >= until_ns,
        "{what}: the job did not reach {until_ns}: markers {:?}",
        markers(client).await
    );
}

/// `NewPart` events of `service_edges` on the node since `t_us`.
async fn new_edge_parts_since(client: &ChClient, db: &str, t_us: i64) -> u64 {
    exec(client, "SYSTEM FLUSH LOGS").await;
    count(
        client,
        &format!(
            "SELECT count() AS n FROM system.part_log WHERE database = '{db}' \
             AND table = 'service_edges' AND event_type = 'NewPart' \
             AND event_time_microseconds >= fromUnixTimestamp64Micro(toInt64({t_us}))"
        ),
    )
    .await
}

/// One span of fixture G: trace, span id, parent, kind, start, duration,
/// error, `zipkin.shared = "true"`.
type GSpan = (
    [u8; 16],
    [u8; 8],
    Option<[u8; 8]>,
    i32,
    i64,
    i64,
    bool,
    bool,
);

const MILLI: i64 = 1_000_000;
const SERVER: i32 = 2;
const CLIENT: i32 = 3;
const PRODUCER: i32 = 4;
const CONSUMER: i32 = 5;
const INTERNAL: i32 = 1;

fn g_trace(group: u8, n: u8) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = 0x59;
    id[2] = group;
    id[3] = n;
    id[15] = 1;
    id
}

fn g_span_id(role: u8, n: u8) -> [u8; 8] {
    [0x5a, 0, role, n, 0, 0, 0, 1]
}

/// Part A §5.2's fixture G at `l`: its eight pushes P1–P8, each one
/// service's spans, every span starting in `[l + 1 s, l + 70 s)`.
fn fixture_g(l: i64) -> Vec<(&'static str, Vec<GSpan>)> {
    let (t, s) = (g_trace, g_span_id);
    let c1: GSpan = (t(1, 1), s(1, 1), None, CLIENT, l + 58 * S, S, false, false);
    let d = |i: u8| -> GSpan {
        let at = l + 30 * S + i64::from(i) * 500 * MILLI;
        (t(2, i), s(2, i), None, CLIENT, at, S, false, false)
    };
    let e = |i: u8| -> GSpan {
        let at = l + 30 * S + i64::from(i) * 500 * MILLI - 10 * MILLI;
        (
            t(2, i),
            s(4, i),
            Some(s(2, i)),
            SERVER,
            at,
            i64::from(i) * MILLI,
            false,
            false,
        )
    };
    let mut p1 = vec![
        c1,
        (t(1, 2), s(1, 2), None, CLIENT, l + 10 * S, S, false, false),
        (
            t(1, 3),
            s(1, 3),
            None,
            PRODUCER,
            l + 20 * S,
            S,
            false,
            false,
        ),
    ];
    p1.extend((1..=20).map(d));
    let p2 = vec![
        (
            t(1, 1),
            s(3, 1),
            Some(s(1, 1)),
            SERVER,
            l + 61 * S,
            10 * MILLI,
            false,
            false,
        ),
        (
            t(1, 3),
            s(3, 2),
            Some(s(1, 3)),
            CONSUMER,
            l + 21 * S,
            20 * MILLI,
            false,
            false,
        ),
    ];
    // The shared server span: the client's own span id, no parent.
    let p3 = vec![(
        t(1, 2),
        s(1, 2),
        None,
        SERVER,
        l + 11 * S,
        30 * MILLI,
        true,
        true,
    )];
    let p4: Vec<GSpan> = (1..=20).map(e).collect();
    let mut p5 = p4.clone();
    p5.push((
        t(3, 1),
        s(5, 1),
        None,
        INTERNAL,
        l + 5 * S,
        MILLI,
        false,
        false,
    ));
    let p6 = vec![
        c1,
        (t(1, 4), s(1, 4), None, CLIENT, l + 40 * S, S, true, false),
        (
            t(1, 5),
            s(1, 5),
            None,
            PRODUCER,
            l + 45 * S,
            S,
            false,
            false,
        ),
        (t(1, 6), s(1, 6), None, CLIENT, l + 50 * S, S, false, false),
    ];
    let p7 = vec![
        (
            t(1, 4),
            s(6, 1),
            Some(s(1, 4)),
            SERVER,
            l + 41 * S,
            40 * MILLI,
            false,
            false,
        ),
        (
            t(1, 4),
            s(6, 2),
            Some(s(1, 4)),
            SERVER,
            l + 41 * S,
            40 * MILLI,
            false,
            false,
        ),
    ];
    // Cross-kind halves: neither pair is an edge.
    let p8 = vec![
        (
            t(1, 5),
            s(7, 1),
            Some(s(1, 5)),
            SERVER,
            l + 46 * S,
            MILLI,
            false,
            false,
        ),
        (
            t(1, 6),
            s(7, 2),
            Some(s(1, 6)),
            CONSUMER,
            l + 51 * S,
            MILLI,
            false,
            false,
        ),
    ];
    vec![
        ("svc-a", p1),
        ("svc-b", p2),
        ("svc-c", p3),
        ("svc-d", p4),
        ("svc-d", p5),
        ("svc-a", p6),
        ("svc-e", p7),
        ("svc-f", p8),
    ]
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Pushes `range` of fixture G (0 is P1) into the node's own `spans`, one
/// insert, so one part, each.
async fn push_g(client: &ChClient, l: i64, range: std::ops::Range<usize>) {
    for (service, spans) in fixture_g(l).into_iter().skip(range.start).take(range.len()) {
        let rows: Vec<String> = spans
            .iter()
            .map(|(t, id, parent, kind, at, dur, error, shared)| {
                format!(
                    "('{}', '{}', '{}', {at}, {dur}, {kind}, {}, '{}')",
                    hex(t),
                    hex(id),
                    parent.map(|p| hex(&p)).unwrap_or_default(),
                    if *error { 2 } else { 0 },
                    if *shared {
                        r#"{"zipkin%2Eshared":"true"}"#
                    } else {
                        "{}"
                    }
                )
            })
            .collect();
        exec(
            client,
            &format!(
                "INSERT INTO spans (trace_id, span_id, parent_span_id, start_ns, duration_ns, \
                 service, name, kind, status_code, attrs, end_ns, service_type) \
                 SELECT unhex(t), unhex(sp), unhex(p), st, d, '{service}', 'op', k, sc, \
                        CAST(a, 'JSON'), toUInt64(st + d), 'string' \
                 FROM values('t String, sp String, p String, st Int64, d Int64, k Int32, \
                              sc Int32, a String', {})",
                rows.join(", ")
            ),
        )
        .await;
    }
}

/// Fixture G's answer (part A §5.2): `(client, server, type, calls,
/// failed, quantiles)`, in order.
/// One edge of an answer: client, server, type, calls, failed, quantiles.
type Edge = (&'static str, &'static str, &'static str, u64, u64, [f64; 3]);

fn g1_rows() -> Vec<Edge> {
    vec![
        (
            "svc-a",
            "svc-d",
            "rpc",
            20,
            0,
            [10_000_000.0, 19_000_000.0, 20_000_000.0],
        ),
        ("svc-a", "svc-e", "rpc", 2, 2, [40_000_000.0; 3]),
        ("svc-a", "svc-b", "messaging", 1, 0, [20_000_000.0; 3]),
        ("svc-a", "svc-b", "rpc", 1, 0, [10_000_000.0; 3]),
        ("svc-a", "svc-c", "rpc", 1, 1, [30_000_000.0; 3]),
    ]
}

/// `edges` are fixture G's answer, every field, the quantiles within the
/// digest's 1 % (part A L4).
fn assert_g1(edges: &[pulsus_read::GraphEdgeRow], what: &str) {
    let got: Vec<(String, String, String, u64, u64)> = edges
        .iter()
        .map(|e| {
            (
                e.client.clone(),
                e.server.clone(),
                e.conn_type.clone(),
                e.calls,
                e.failed,
            )
        })
        .collect();
    let want: Vec<(String, String, String, u64, u64)> = g1_rows()
        .into_iter()
        .map(|(c, s, t, n, f, _)| (c.to_string(), s.to_string(), t.to_string(), n, f))
        .collect();
    assert_eq!(got, want, "{what}: fixture G's edges, in order");
    for (e, (_, _, _, _, _, q)) in edges.iter().zip(g1_rows()) {
        for (i, want) in q.iter().enumerate() {
            assert!(
                (e.quantiles_ns[i] - want).abs() <= want * 0.01,
                "{what}: {} -> {} {} quantile {i}: got {:?}, want {want}",
                e.client,
                e.server,
                e.conn_type,
                e.quantiles_ns
            );
        }
    }
}

/// Shard 1's two replicas and shard 2's node over a fresh clustered
/// database, every view stopped, one marker at `L − 1 min`, merges stopped
/// on both job tables of shard 1, both replicas synced: the state every case
/// but K6 starts from.
struct Replicas {
    db: String,
    l: i64,
    s1: ChClient,
    s1b: ChClient,
    s2: ChClient,
}

impl Replicas {
    async fn new(name: &str) -> Self {
        let db = pulsus_testkit::test_db(&format!("pulsus_read_it_t599b_{name}_{}", now_ns()));
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
        let s1 = ChClient::new(shard1_config(&db))
            .await
            .expect("connect shard1");
        let s1b = ChClient::new(shard1b_config(&db))
            .await
            .expect("connect shard1b");
        let s2 = ChClient::new(shard2_config(&db))
            .await
            .expect("connect shard2");
        let l = floor_to(now_ns() - 30 * MINUTE, BUCKET);
        let r = Replicas { db, l, s1, s1b, s2 };
        for c in [&r.s1, &r.s1b] {
            exec(c, "SYSTEM STOP MERGES spans").await;
            exec(c, "SYSTEM STOP MERGES service_edges").await;
        }
        seed_edges(&[&r.s1, &r.s1b, &r.s2], &r.s1, l - MINUTE).await;
        for c in [&r.s1, &r.s1b] {
            wait_state(
                c,
                &r.db,
                "service_edges",
                "the seeded marker",
                ReplicaState::synced,
            )
            .await;
            assert_eq!(markers(c).await, vec![l - MINUTE], "the seeded marker");
        }
        r
    }

    /// G's window, `[L, L + 5 min)`.
    fn window(&self) -> pulsus_read::GraphWindow {
        pulsus_read::GraphWindow {
            start_ns: self.l,
            end_ns: self.l + 5 * MINUTE,
        }
    }

    /// The route over G's window, initiated on `ch-shard1`, its statements
    /// named `prefix`.
    async fn graph(&self, prefix: &str) -> Vec<pulsus_read::GraphEdgeRow> {
        pulsus_read::TraceEngine::new(
            ChClient::new(shard1_config(&self.db))
                .await
                .expect("connect the engine"),
            config(),
        )
        .with_statement_prefix(prefix)
        .service_graph(self.window())
        .await
        .expect("the graph answers")
        .edges
    }

    /// The route answers fixture G once, after both replicas are synced.
    async fn assert_route_g1(&self, what: &str) {
        wait_synced(&[&self.s1, &self.s1b], &self.db, what).await;
        let prefix = pulsus_testkit::test_ident(&format!("t599b_{}", now_ns()));
        assert_g1(&self.graph(&prefix).await, what);
    }

    async fn drop(self) {
        exec(
            &self.s1,
            &format!(
                "DROP DATABASE IF EXISTS {} ON CLUSTER '{CLUSTER_NAME}' SYNC",
                self.db
            ),
        )
        .await;
    }
}

/// The address and native port shard 1's replica `n` has in the cluster
/// definition, as the nodes reach each other.
async fn shard1_replica_address(client: &ChClient, n: u32) -> String {
    strings(
        client,
        &format!(
            "SELECT concat(host_address, ':', toString(port)) AS s FROM system.clusters \
             WHERE cluster = '{CLUSTER_NAME}' AND shard_num = 1 AND replica_num = {n}"
        ),
    )
    .await
    .into_iter()
    .next()
    .unwrap_or_else(|| panic!("shard 1 has a replica {n}"))
}

/// K1, K10 and K7 (issue #599 part B §3): both replicas write one shared
/// `ReplicatedReplacingMergeTree`, so a bucket both run is kept once; the
/// read takes a shard's totals and markers on one replica; a replica that
/// has not received the other's run of a bucket does not pair it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_replicas_share_one_edge_table() {
    skip_unless_live!();
    let r = Replicas::new("k1").await;
    let (db, l) = (r.db.clone(), r.l);

    // K1: the engine on both replicas.
    for (c, node) in [(&r.s1, "ch-shard1"), (&r.s1b, "ch-shard1b")] {
        let engine = strings(
            c,
            "SELECT engine AS s FROM system.tables WHERE database = currentDatabase() \
             AND name = 'service_edges'",
        )
        .await;
        assert_eq!(
            engine,
            ["ReplicatedReplacingMergeTree"],
            "K1: {node}'s target"
        );
    }
    // K1: ch-shard1 runs G's bucket; ch-shard1b's view is stopped.
    push_g(&r.s1, l, 0..8).await;
    wait_synced(&[&r.s1, &r.s1b], &db, "K1: G on both replicas").await;
    refresh_until(&r.s1, &db, l + 4 * MINUTE, 4, "K1: ch-shard1's run").await;
    wait_synced(&[&r.s1, &r.s1b], &db, "K1: ch-shard1's run on both").await;
    let written = count(
        &r.s1,
        "SELECT count() AS n FROM service_edges WHERE ver > 1",
    )
    .await;
    assert!(
        written > 5,
        "K1: ch-shard1's run wrote G's rows and a marker: {written}"
    );
    // The same rows, read back from ch-shard1, written on ch-shard1b one
    // version later, as ch-shard1b running the same bucket later would.
    let source = shard1_replica_address(&r.s1b, 1).await;
    r.s1b
        .execute(
            &format!(
                "INSERT INTO service_edges SELECT minute, client_minute, client, server, \
                 conn_type, calls, failed, q, ver + 1 \
                 FROM remote('{source}', '{db}', 'service_edges') WHERE ver > 1"
            ),
            &QuerySettings::new().set("insert_deduplicate", 0),
            Idempotency::NonIdempotent,
        )
        .await
        .expect("K1: the second run's rows");
    wait_synced(&[&r.s1, &r.s1b], &db, "K1: the second run on both").await;
    let mut finals = Vec::new();
    for (c, node) in [(&r.s1, "ch-shard1"), (&r.s1b, "ch-shard1b")] {
        let held = count(c, "SELECT count() AS n FROM service_edges WHERE ver > 1").await;
        assert_eq!(held, 2 * written, "K1: {node} holds both runs' rows");
        let repeated = count(
            c,
            "SELECT countIf(c > 1) AS n FROM (SELECT count() AS c FROM service_edges FINAL \
             GROUP BY conn_type, minute, client_minute, client, server)",
        )
        .await;
        assert_eq!(repeated, 0, "K1: every key once under FINAL on {node}");
        finals.push(
            strings(
                c,
                "SELECT concat(conn_type, ' ', toString(minute), ' ', toString(client_minute), \
                 ' ', client, ' ', server, ' ', toString(calls), ' ', toString(failed), ' ', \
                 toString(ver)) AS s FROM service_edges FINAL ORDER BY s",
            )
            .await,
        );
    }
    assert_eq!(
        finals[0], finals[1],
        "K1: both replicas hold the same totals"
    );
    r.assert_route_g1("K1").await;

    // K10: every request reads shard 1 on one replica, the one that served
    // its marker fan-out, and it does so because the route's statement and
    // every part of it it sent carry `load_balancing = 'in_order'`: the
    // logged setting is what fails without it, whatever replicas a random
    // choice happened to pick.
    for n in 0..8 {
        let prefix = pulsus_testkit::test_ident(&format!("t599b_k10_{n}_{}", now_ns()));
        assert_g1(&r.graph(&prefix).await, &format!("K10 request {n}"));
        exec(&r.s1, "SYSTEM FLUSH LOGS").await;
        let graph_statements = count(
            &r.s1,
            &format!(
                "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' \
                 AND is_initial_query AND startsWith(query_id, '{prefix}-') \
                 AND has(tables, '{db}.service_edges') AND has(tables, '{db}.spans') \
                 AND Settings['load_balancing'] = 'in_order'"
            ),
        )
        .await;
        assert_eq!(
            graph_statements, 1,
            "K10 request {n}: the route's statement ran with load_balancing = 'in_order'"
        );
        let mut hosts = Vec::new();
        for (c, node) in [(&r.s1, "ch-shard1"), (&r.s1b, "ch-shard1b")] {
            exec(c, "SYSTEM FLUSH LOGS").await;
            let rows = count(
                c,
                &format!(
                    "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' \
                     AND startsWith(initial_query_id, '{prefix}-')"
                ),
            )
            .await;
            let fan_out = count(
                c,
                &format!(
                    "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' \
                     AND is_initial_query = 0 AND startsWith(initial_query_id, '{prefix}-') \
                     AND has(tables, '{db}.service_edges') AND NOT has(tables, '{db}.spans')"
                ),
            )
            .await;
            let unordered = count(
                c,
                &format!(
                    "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' \
                     AND is_initial_query = 0 AND startsWith(initial_query_id, '{prefix}-') \
                     AND has(tables, '{db}.service_edges') \
                     AND Settings['load_balancing'] != 'in_order'"
                ),
            )
            .await;
            assert_eq!(
                unordered, 0,
                "K10 request {n}: every part {node} ran carried load_balancing = 'in_order'"
            );
            if rows > 0 {
                hosts.push((node, fan_out));
            }
        }
        assert_eq!(
            hosts.len(),
            1,
            "K10 request {n}: shard 1 read on one host: {hosts:?}"
        );
        assert!(
            hosts[0].1 > 0,
            "K10 request {n}: the host that answered served the marker fan-out: {hosts:?}"
        );
    }

    // K7: ch-shard1b has every span but not ch-shard1's run of the next
    // bucket; its run must not pair that bucket again.
    let newest = *markers(&r.s1).await.last().expect("a marker");
    exec(&r.s1b, "SYSTEM STOP FETCHES service_edges").await;
    let stopped = server_now_us(&r.s1b).await;
    refresh_until(&r.s1, &db, newest + BUCKET, 4, "K7: ch-shard1's run").await;
    wait_state(
        &r.s1b,
        &db,
        "spans",
        "K7: spans synced on ch-shard1b",
        ReplicaState::synced,
    )
    .await;
    wait_state(
        &r.s1b,
        &db,
        "service_edges",
        "K7: ch-shard1's run queued on ch-shard1b",
        |s| s.inserts_in_queue > 0,
    )
    .await;
    refresh_on(&r.s1b, &db, "K7: ch-shard1b's run").await;
    assert_eq!(
        new_edge_parts_since(&r.s1b, &db, stopped).await,
        0,
        "K7: ch-shard1b's run wrote nothing"
    );
    exec(&r.s1b, "SYSTEM START FETCHES service_edges").await;
    wait_synced(&[&r.s1, &r.s1b], &db, "K7: fetched").await;
    assert_eq!(
        markers(&r.s1).await,
        markers(&r.s1b).await,
        "K7: both replicas hold the same markers"
    );
    for (c, node) in [(&r.s1, "ch-shard1"), (&r.s1b, "ch-shard1b")] {
        let repeated = count(
            c,
            "SELECT countIf(c > 1) AS n FROM (SELECT count() AS c FROM service_edges FINAL \
             GROUP BY conn_type, minute, client_minute, client, server)",
        )
        .await;
        assert_eq!(repeated, 0, "K7: every key once under FINAL on {node}");
    }
    r.drop().await;
}

/// K2 (issue #599 part B §3): a replica that has copied the log entries of
/// G's parts but cannot fetch them writes no marker; once it has them, its
/// run pairs G.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replica_with_parts_still_to_fetch_writes_nothing() {
    skip_unless_live!();
    let r = Replicas::new("k2").await;
    let (db, l) = (r.db.clone(), r.l);
    exec(&r.s1b, "SYSTEM STOP FETCHES spans").await;
    push_g(&r.s1, l, 0..8).await;
    // The entries are copied, so a barrier on the log alone would pass.
    let state = wait_state(&r.s1b, &db, "spans", "K2: G queued on ch-shard1b", |s| {
        s.inserts_in_queue > 0 && s.log_pointer > s.log_max_index
    })
    .await;
    wait_state(&r.s1b, &db, "service_edges", "K2", ReplicaState::synced).await;
    refresh_on(&r.s1b, &db, "K2: ch-shard1b's run").await;
    assert_eq!(
        markers(&r.s1b).await,
        vec![l - MINUTE],
        "K2: ch-shard1b's run, spans {state:?}, wrote no marker"
    );
    exec(&r.s1b, "SYSTEM START FETCHES spans").await;
    wait_synced(&[&r.s1b], &db, "K2: fetched").await;
    refresh_until(
        &r.s1b,
        &db,
        l + 4 * MINUTE,
        4,
        "K2: ch-shard1b's run after the fetch",
    )
    .await;
    r.assert_route_g1("K2").await;
    r.drop().await;
}

/// K3 (issue #599 part B §3): G's pushes split across the two replicas,
/// neither able to fetch the other's: neither writes a marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pushes_split_across_replicas_wait_for_both() {
    skip_unless_live!();
    let r = Replicas::new("k3").await;
    let (db, l) = (r.db.clone(), r.l);
    for c in [&r.s1, &r.s1b] {
        exec(c, "SYSTEM STOP FETCHES spans").await;
    }
    push_g(&r.s1, l, 0..2).await;
    push_g(&r.s1b, l, 2..8).await;
    for (c, node) in [(&r.s1, "ch-shard1"), (&r.s1b, "ch-shard1b")] {
        wait_state(
            c,
            &db,
            "spans",
            &format!("K3: the other half queued on {node}"),
            |s| s.inserts_in_queue > 0 && s.log_pointer > s.log_max_index,
        )
        .await;
        wait_state(c, &db, "service_edges", "K3", ReplicaState::synced).await;
    }
    for (c, node) in [(&r.s1, "ch-shard1"), (&r.s1b, "ch-shard1b")] {
        refresh_on(c, &db, &format!("K3: {node}'s run")).await;
        for (d, other) in [(&r.s1, "ch-shard1"), (&r.s1b, "ch-shard1b")] {
            assert_eq!(
                markers(d).await,
                vec![l - MINUTE],
                "K3: after {node}'s run, {other} holds no new marker"
            );
        }
    }
    for c in [&r.s1, &r.s1b] {
        exec(c, "SYSTEM START FETCHES spans").await;
    }
    wait_synced(&[&r.s1, &r.s1b], &db, "K3: fetched").await;
    refresh_until(&r.s1, &db, l + 4 * MINUTE, 4, "K3: the run after the fetch").await;
    r.assert_route_g1("K3").await;
    r.drop().await;
}

/// K5 (issue #599 part B §3): a rebuilt replica, its tables empty and its
/// fetches unable to run, writes nothing; once it has fetched the shard it
/// holds the shard's totals without pairing them again, and answers from
/// them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rebuilt_replica_waits_for_the_shard() {
    skip_unless_live!();
    let r = Replicas::new("k5").await;
    let (db, l) = (r.db.clone(), r.l);
    push_g(&r.s1, l, 0..8).await;
    wait_synced(&[&r.s1, &r.s1b], &db, "K5: G on both replicas").await;
    refresh_until(&r.s1, &db, l + 4 * MINUTE, 4, "K5: ch-shard1's run").await;
    wait_synced(&[&r.s1, &r.s1b], &db, "K5: ch-shard1's run on both").await;
    let totals_sql = "SELECT concat(conn_type, ' ', toString(minute), ' ', \
                      toString(client_minute), ' ', client, ' ', server, ' ', toString(calls), \
                      ' ', toString(failed), ' ', toString(ver)) AS s \
                      FROM service_edges FINAL ORDER BY s";
    let totals = strings(&r.s1, totals_sql).await;

    // The rebuild: ch-shard1 sends nothing, ch-shard1b's two tables dropped
    // and created again by the schema's statements.
    for table in ["spans", "service_edges"] {
        exec(&r.s1, &format!("SYSTEM STOP REPLICATED SENDS {table}")).await;
    }
    let rebuilt = server_now_us(&r.s1b).await;
    for table in ["spans", "service_edges"] {
        exec(&r.s1b, &format!("DROP TABLE {table} SYNC")).await;
    }
    let on_cluster = format!(" ON CLUSTER '{CLUSTER_NAME}'");
    for table in ["spans", "service_edges"] {
        let head = format!("CREATE TABLE IF NOT EXISTS {db}.{table}{on_cluster}\n");
        let statement = pulsus_schema::rendered_statements(&cluster_ctx(&db))
            .into_iter()
            .find(|s| s.starts_with(&head))
            .unwrap_or_else(|| panic!("the schema creates {table}"));
        exec(&r.s1b, &statement.replacen(&on_cluster, "", 1)).await;
        exec(&r.s1b, &format!("SYSTEM STOP MERGES {table}")).await;
    }
    for table in ["spans", "service_edges"] {
        wait_state(
            &r.s1b,
            &db,
            table,
            "K5: the rebuilt replica has parts to fetch",
            |s| s.inserts_in_queue > 0,
        )
        .await;
    }
    refresh_on(&r.s1b, &db, "K5: the rebuilt replica's run").await;
    assert_eq!(
        count(&r.s1b, "SELECT count() AS n FROM service_edges").await,
        0,
        "K5: the rebuilt replica's run wrote no row and no marker"
    );

    for table in ["spans", "service_edges"] {
        exec(&r.s1, &format!("SYSTEM START REPLICATED SENDS {table}")).await;
    }
    wait_synced(&[&r.s1b], &db, "K5: the rebuilt replica fetched the shard").await;
    assert_eq!(
        strings(&r.s1b, totals_sql).await,
        totals,
        "K5: the rebuilt replica holds the shard's totals"
    );
    assert_eq!(
        new_edge_parts_since(&r.s1b, &db, rebuilt).await,
        0,
        "K5: the rebuilt replica paired nothing again"
    );

    // The route's statement with ch-shard1b listed first, in order.
    let first = shard1_replica_address(&r.s1, 2).await;
    let second = shard1_replica_address(&r.s1, 1).await;
    let target = pulsus_read::GraphTarget {
        cluster: CLUSTER_NAME.to_string(),
        database: db.clone(),
    };
    let sql = pulsus_read::spans_graph_sql(
        r.window(),
        Some(&target),
        pulsus_read::SERVICE_GRAPH_MAX_EDGES,
    );
    let from = format!("cluster('{CLUSTER_NAME}', view(");
    assert_eq!(
        sql.matches(&from).count(),
        2,
        "the read and its fan-out: {sql}"
    );
    let sql = sql.replace(&from, &format!("remote('{first}|{second}', view("));
    let tag = pulsus_testkit::test_ident(&format!("t599b_k5_{}", now_ns()));
    let settings = QuerySettings::new()
        .set("load_balancing", "in_order")
        .set("prefer_localhost_replica", 0)
        .set("log_comment", &tag);
    let mut edges = Vec::new();
    {
        let mut stream =
            r.s1.query_stream::<pulsus_read::GraphEdgeRow>(&sql, &settings)
                .await
                .expect("K5: the read");
        while let Some(row) = stream.next().await {
            edges.push(row.expect("K5: an edge"));
        }
    }
    assert_g1(&edges, "K5: read on the rebuilt replica");
    for (c, node, want) in [(&r.s1b, "ch-shard1b", true), (&r.s1, "ch-shard1", false)] {
        exec(c, "SYSTEM FLUSH LOGS").await;
        let remote = count(
            c,
            &format!(
                "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' \
                 AND is_initial_query = 0 AND log_comment = '{tag}'"
            ),
        )
        .await;
        assert_eq!(remote > 0, want, "K5: {node} served the read: {remote}");
    }
    r.drop().await;
}

/// K8 (issue #599 part B §3): a replica that has not copied the log entries
/// of G's parts has nothing queued, and still writes no marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replica_behind_on_the_log_writes_nothing() {
    skip_unless_live!();
    let r = Replicas::new("k8").await;
    let (db, l) = (r.db.clone(), r.l);
    exec(&r.s1b, "SYSTEM STOP PULLING REPLICATION LOG spans").await;
    push_g(&r.s1, l, 0..8).await;
    let behind = |s: &ReplicaState| {
        s.inserts_in_queue == 0 && s.log_max_index > 0 && s.log_pointer <= s.log_max_index
    };
    // ch-shard1 has announced all eight parts.
    wait_state(&r.s1, &db, "spans", "K8: G announced", |s| {
        s.log_max_index >= 7
    })
    .await;
    for when in ["before", "after"] {
        let spans = replica_state(&r.s1b, &db, "spans").await;
        assert!(
            behind(&spans),
            "K8: {when} the run, ch-shard1b's spans log: {spans:?}"
        );
        let edges = replica_state(&r.s1b, &db, "service_edges").await;
        assert!(
            edges.synced(),
            "K8: {when} the run, service_edges: {edges:?}"
        );
        if when == "before" {
            refresh_on(&r.s1b, &db, "K8: ch-shard1b's run").await;
        }
    }
    assert_eq!(
        markers(&r.s1b).await,
        vec![l - MINUTE],
        "K8: ch-shard1b's run wrote no marker"
    );
    exec(&r.s1b, "SYSTEM START PULLING REPLICATION LOG spans").await;
    wait_synced(&[&r.s1b], &db, "K8: pulled and fetched").await;
    refresh_until(
        &r.s1b,
        &db,
        l + 4 * MINUTE,
        4,
        "K8: ch-shard1b's run after the pull",
    )
    .await;
    r.assert_route_g1("K8").await;
    r.drop().await;
}

/// K9 (issue #599 part B §3): another database's replication queue does not
/// hold this database's job back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn another_database_s_queue_does_not_stop_the_job() {
    skip_unless_live!();
    let r = Replicas::new("k9").await;
    let (db, l) = (r.db.clone(), r.l);
    push_g(&r.s1, l, 0..8).await;
    let other = format!("{db}_other");
    run_init(&r.s1, &cluster_ctx(&other))
        .await
        .expect("run_init the other database");
    exec(&r.s1b, &format!("SYSTEM STOP FETCHES {other}.spans")).await;
    exec(
        &r.s1,
        &format!(
            "INSERT INTO {other}.spans (trace_id, span_id, parent_span_id, start_ns, \
             duration_ns, service, name, kind, attrs, end_ns, service_type) \
             SELECT sipHash128('k9'), reinterpretAsFixedString(cityHash64('k9')), \
                    toFixedString('', 8), {at}, 1000, 'svc-k9', 'op', 2, CAST('{{}}', 'JSON'), \
                    toUInt64({at} + 1000), 'string'",
            at = l + 10 * S
        ),
    )
    .await;
    wait_state(
        &r.s1b,
        &other,
        "spans",
        "K9: the other database's part queued",
        |s| s.inserts_in_queue > 0,
    )
    .await;
    wait_synced(&[&r.s1, &r.s1b], &db, "K9: G on both replicas").await;
    refresh_until(&r.s1b, &db, l + 4 * MINUTE, 4, "K9: ch-shard1b's run").await;
    r.assert_route_g1("K9").await;
    exec(&r.s1b, &format!("SYSTEM START FETCHES {other}.spans")).await;
    exec(
        &r.s1,
        &format!("DROP DATABASE IF EXISTS {other} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    r.drop().await;
}

/// K6 (issue #599 part B §3): on a fresh cluster no table has a log entry,
/// and every node's run writes the first marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fresh_cluster_writes_its_first_markers() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db(&format!("pulsus_read_it_t599b_k6_{}", now_ns()));
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
    let nodes = [
        (
            "ch-shard1",
            ChClient::new(shard1_config(&db))
                .await
                .expect("connect shard1"),
        ),
        (
            "ch-shard1b",
            ChClient::new(shard1b_config(&db))
                .await
                .expect("connect shard1b"),
        ),
        (
            "ch-shard2",
            ChClient::new(shard2_config(&db))
                .await
                .expect("connect shard2"),
        ),
    ];
    for (node, c) in &nodes {
        exec(c, "SYSTEM REFRESH VIEW service_edges_mv").await;
        exec(c, "SYSTEM WAIT VIEW service_edges_mv").await;
        let state = strings(
            c,
            "SELECT concat(exception, '|', toString(isNotNull(last_success_time))) AS s \
             FROM system.view_refreshes WHERE database = currentDatabase() \
             AND view = 'service_edges_mv'",
        )
        .await;
        assert_eq!(state, ["|1"], "K6: {node}'s refresh succeeded");
    }
    for (node, c) in &nodes {
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        loop {
            let n = count(
                c,
                "SELECT count() AS n FROM service_edges FINAL WHERE conn_type = ''",
            )
            .await;
            if n >= 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "K6: {node} holds no marker"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}
