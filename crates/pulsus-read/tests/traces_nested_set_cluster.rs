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

/// A shard's connection: its host from `host_env`, its HTTP port from the
/// same name with `_HOST` replaced by `_HTTP_PORT`; CI reaches each node on
/// its fixture address at the server's own port, a local copy on mapped ones.
fn shard_config(host_env: &str, default_host: &str, database: &str) -> ChConnConfig {
    let http_var = host_env.replace("_HOST", "_HTTP_PORT");
    ChConnConfig {
        server: std::env::var(host_env).unwrap_or_else(|_| default_host.to_string()),
        http_port: std::env::var(&http_var)
            .map(|p| {
                p.parse()
                    .unwrap_or_else(|_| panic!("{http_var}: a port, got {p:?}"))
            })
            .unwrap_or(8123),
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
        rollup_levels: pulsus_schema::DEFAULT_ROLLUP_LEVELS.to_vec(),
        rollup_ranges: pulsus_schema::DEFAULT_ROLLUP_RANGES.to_vec(),
        rollup_counter_buckets: pulsus_schema::DEFAULT_ROLLUP_COUNTER_BUCKETS,
        cold_volume: None,
        metrics_hot_days: None,
        logs_hot_days: None,
        traces_hot_days: None,
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
    let parsed = parse_trace_landing(req).expect("the landing decode");
    let received_ms = now_ns() / 1_000_000;
    let mut rows: Vec<TraceLandingRow> = Vec::new();
    rows.extend(
        parsed
            .spans
            .iter()
            .cloned()
            .map(|s| TraceLandingRow::span(&std::sync::Arc::from(""), received_ms, s)),
    );
    rows.extend(
        parsed
            .resources
            .iter()
            .cloned()
            .map(|r| TraceLandingRow::resource(&std::sync::Arc::from(""), received_ms, r)),
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
        spans_table: "spans_dist".to_string(),
        error_spans_table: "error_spans_dist".to_string(),
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
        empty_tenant(),
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
                error_spans: "error_spans_dist",
                resources: "resources",
                traces: "traces_dist",
                indexed: &[],
                tenant: empty_term(),
            },
            64,
        )
        .expect("plans")
    };
    let window = (base, base + 60 * S);

    // The negative control: the statement, without the setting, is refused.
    let p = plan("{ nestedSetLeft > 0 }", window.0, window.1, 30);
    let stmt = plan_statement_sliced(
        empty_term(),
        &p,
        "spans_dist",
        "traces_dist",
        "resources",
        64,
        &[],
        None,
    )
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
        // Issue #600 part 2: a child-count filter reads `spans_dist` inside
        // `spans_dist`, sent shard-local as the numbering is.
        ("{ span:childCount > 0 }", "[01, 02]"),
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
        error_spans: "error_spans_dist",
        resources: "resources",
        traces: "traces_dist",
        indexed: &[],
        tenant: empty_term(),
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
                floor_ns: i64::MIN,
            },
            &engine.metrics_ctx(),
            &tables,
        );
        let routed = match routed {
            Ok(r) => r,
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

/// Issue #600 part 2: a resource attribute's value over two shards. The
/// lookup is a scalar the initiator evaluates and sends to each shard as a
/// literal: one holding `Dynamic` values was refused when empty (code 36)
/// and re-typed when not, so a stored integer `200` rendered as a string.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resource_value_keeps_its_type_over_two_shards() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t600p2_cluster");
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
    for (i, mut req) in bodies(base).into_iter().enumerate() {
        req.resource_spans[0]
            .resource
            .as_mut()
            .expect("a resource")
            .attributes
            .push(KeyValue {
                key: "http.status_code".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(200)),
                }),
                ..KeyValue::default()
            });
        land(&shard1, &req, &format!("t600p2-cluster-{i}")).await;
    }
    for t in ["spans_dist", "traces_dist"] {
        exec(&shard1, &format!("SYSTEM FLUSH DISTRIBUTED {db}.{t}")).await;
    }
    // `resources` is replicated to every node; the `IN` form reads it on the shard.
    exec(&shard2, &format!("SYSTEM SYNC REPLICA {db}.resources")).await;
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
        empty_tenant(),
    );
    let plan = |query: &str| {
        plan_search_request(
            &pulsus_traceql::parse(query).expect("parses"),
            &SearchParams {
                start_ns: base,
                end_ns: base + 60 * S,
                limit: 30,
                spss: 10,
            },
            &StatementTables {
                spans: "spans_dist",
                error_spans: "error_spans_dist",
                resources: "resources",
                traces: "traces_dist",
                indexed: &[],
                tenant: empty_term(),
            },
            64,
        )
        .expect("plans")
    };
    let by_status = (0x10..=0x23u8)
        .rev()
        .map(|t| format!("{t:02x} {{int:200 [01, 02, 03, 04]}}"))
        .collect::<Vec<_>>()
        .join("; ");
    let render_numbers: fn(&pulsus_read::traces::SearchOutput) -> String = numbers_of;
    let render_groups: fn(&pulsus_read::traces::SearchOutput) -> String = groups_of;
    let mut wrong = Vec::new();
    for (query, render, want) in [
        (
            "{ resource.http.status_code = 200 }",
            render_numbers,
            every_trace(&|_| "01 200, 02 200, 03 200, 04 200".to_string()),
        ),
        (
            "{ resource.no.such.key = 1 }",
            render_numbers,
            String::new(),
        ),
        (
            "{ } | by(resource.http.status_code)",
            render_groups,
            by_status,
        ),
    ] {
        let got = match engine.search_routed(&plan(query)).await {
            Ok(out) => render(&out),
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
        floor_ns: i64::MIN,
    };
    let prefix = pulsus_testkit::test_ident(&format!("t598p2_cluster_{}", now_ns()));
    let engine = pulsus_read::TraceEngine::new(
        ChClient::new(shard1_config(&db))
            .await
            .expect("connect the engine"),
        config(),
        empty_tenant(),
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
        empty_tenant(),
    );
    let slow = engine
        .list_tag_values(
            "http.route",
            Some("span"),
            pulsus_read::TagValuesRequest {
                q: Some("{traceDuration > 1s}"),
                start_ns: now - 180 * MIN,
                end_ns: now_ns() + MIN,
                floor_ns: i64::MIN,
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
        empty_tenant(),
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
                floor_ns: i64::MIN,
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
        empty_tenant(),
    );
    let read = config();
    let tables = StatementTables {
        spans: &read.spans_table,
        error_spans: &read.error_spans_table,
        resources: &read.resources_table,
        traces: &read.traces_table,
        indexed: &[],
        tenant: empty_term(),
    };
    let q = "{} | compare({ status = error }, 1)";
    let routed = pulsus_read::plan_trace_metrics_routed(
        &pulsus_traceql::parse(q).expect("parse"),
        &pulsus_read::MetricsParams {
            start_ns: (now / S - 600) * S,
            end_ns: (now / S) * S,
            step_ms: 60_000,
            exemplars: None,
            floor_ns: i64::MIN,
        },
        &engine.metrics_ctx(),
        &tables,
    )
    .expect("planned");
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
            "INSERT INTO service_edges SELECT '', {t}, {t}, '', '', '', 0, 0, \
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
                empty_tenant(),
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
                spans_table: "spans_q".to_string(),
                error_spans_table: "error_spans_q".to_string(),
                ..config()
            },
            empty_tenant(),
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
// first; merges are stopped on both tables of every node and no replica
// assigns one (`max_replicated_merges_in_queue = 0`), so no merge runs and no
// merge entry is queued unless a case's `OPTIMIZE` assigns it.

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

/// `db.table` on this replica holds every part announced so far:
/// `SYSTEM SYNC REPLICA … LIGHTWEIGHT` pulls the log, then waits for the
/// fetches queued at that moment. A `system.replicas` read cannot be this
/// barrier, because it takes `inserts_in_queue` and `log_pointer` at two
/// moments and a pull landing between them reads as synced (#704).
///
/// Two minutes for the whole operation, each call inside a timeout of the
/// time left. Each call's engine wait is at most 30 s, below the client's
/// 60 s cut, and the call is never retried by the client: a
/// `TIMEOUT_EXCEEDED` (159) is re-issued here, which pulls the log again.
async fn sync_replica(client: &ChClient, db: &str, table: &str, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let sql = format!("SYSTEM SYNC REPLICA {db}.{table} LIGHTWEIGHT");
    loop {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            !left.is_zero(),
            "{what}: {db}.{table} not synced within 120 s"
        );
        let wait = left.min(Duration::from_secs(30)).as_secs().max(1);
        let settings = QuerySettings::new().set("receive_timeout", wait);
        match tokio::time::timeout(
            left,
            client.execute(&sql, &settings, Idempotency::NonIdempotent),
        )
        .await
        {
            Err(_elapsed) => panic!("{what}: {db}.{table} not synced within 120 s"),
            Ok(Ok(())) => return,
            Ok(Err(ChError::Server { code: 159, .. })) => {}
            Ok(Err(e)) => panic!("{what}: {sql}: {e}"),
        }
    }
}

/// Both tables of the job synced on every client given.
async fn wait_synced(clients: &[&ChClient], db: &str, what: &str) {
    for c in clients {
        for table in ["spans", "service_edges"] {
            sync_replica(c, db, table, what).await;
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
            "INSERT INTO service_edges SELECT '', {t}, {t}, '', '', '', 0, 0, \
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
    push_g_into(client, "spans", l, range).await;
}

/// [`push_g`] into `table`, a table with `spans`' columns.
async fn push_g_into(client: &ChClient, table: &str, l: i64, range: std::ops::Range<usize>) {
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
                "INSERT INTO {table} (trace_id, span_id, parent_span_id, start_ns, duration_ns, \
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
    /// `L` 30 minutes back, its interval eligible long ago.
    async fn new(name: &str) -> Self {
        Self::build(name, false).await
    }

    /// Part C: `L` the start of the interval in progress, so an insert now
    /// queues its fetches before the interval becomes eligible; the marker
    /// at `L − 1 min` seeded on shard 1 and on `ch-shard2`.
    async fn new_current(name: &str) -> Self {
        Self::build(name, true).await
    }

    async fn build(name: &str, current: bool) -> Self {
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
        let l = if current {
            floor_to(server_now_us(&s1).await * 1000, BUCKET)
        } else {
            floor_to(now_ns() - 30 * MINUTE, BUCKET)
        };
        let r = Replicas { db, l, s1, s1b, s2 };
        // `STOP MERGES` stops only the running of merges: a replica still
        // assigns merge entries unless its queue limit for them is 0. The
        // setting is per replica and queues nothing; an `OPTIMIZE` still
        // assigns one.
        for c in [&r.s1, &r.s1b, &r.s2] {
            for table in ["spans", "service_edges"] {
                exec(c, &format!("SYSTEM STOP MERGES {table}")).await;
                exec(
                    c,
                    &format!(
                        "ALTER TABLE {table} MODIFY SETTING max_replicated_merges_in_queue = 0"
                    ),
                )
                .await;
            }
        }
        seed_edges(&[&r.s1, &r.s1b, &r.s2], &r.s1, l - MINUTE).await;
        for (c, node) in [(&r.s1, "s1"), (&r.s1b, "s1b")] {
            sync_replica(c, &r.db, "service_edges", "the seeded marker").await;
            assert_eq!(
                markers(c).await,
                vec![l - MINUTE],
                "the seeded marker on {node}"
            );
        }
        if current {
            seed_edges(&[&r.s2], &r.s2, l - MINUTE).await;
            sync_replica(&r.s2, &r.db, "service_edges", "the seeded marker").await;
            assert_eq!(
                markers(&r.s2).await,
                vec![l - MINUTE],
                "the seeded marker on ch-shard2"
            );
        }
        r
    }

    /// Part C: waits until `[L, L + 5 min)` has been eligible for 11 s by
    /// the server's clock, `L + 8 min + 11 s`: at most 8 min 11 s after
    /// [`Self::new_current`].
    async fn wait_eligible(&self) {
        let at_us = (self.l + 8 * MINUTE + 11 * S) / 1000;
        let deadline = std::time::Instant::now() + Duration::from_secs(8 * 60 + 41);
        loop {
            let now = server_now_us(&self.s1).await;
            if now >= at_us {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the interval at {} never became eligible: the server's clock at {now} us",
                self.l
            );
            let left = u64::try_from(at_us - now).expect("a positive wait");
            tokio::time::sleep(Duration::from_micros(left.min(5_000_000))).await;
        }
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
            empty_tenant(),
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

    /// The database dropped on every node, sent once on a client with a
    /// 10-minute cut: a large database can take longer than the shared
    /// client's 60 s, and a second `DROP … ON CLUSTER` racing the first can
    /// fail with "does not exist" despite `IF EXISTS`.
    async fn drop(self) {
        let client = ChClient::new(ChConnConfig {
            query_timeout: Duration::from_secs(600),
            ..shard1_config("default")
        })
        .await
        .expect("connect shard1");
        let sql = format!(
            "DROP DATABASE IF EXISTS {} ON CLUSTER '{CLUSTER_NAME}' SYNC",
            self.db
        );
        client
            .execute(&sql, &QuerySettings::new(), Idempotency::NonIdempotent)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
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
                "INSERT INTO service_edges SELECT org_id, minute, client_minute, client, server, \
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
        // The initiator logs the statement's `cluster(…)` table function,
        // not the tables its parts read. Its row can reach the log after the
        // answer does, so the log is read once the row is there, whatever
        // its settings; the settings are then checked.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            exec(&r.s1, "SYSTEM FLUSH LOGS").await;
            let logged = count(
                &r.s1,
                &format!(
                    "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' \
                     AND is_initial_query AND startsWith(query_id, '{prefix}-') \
                     AND has(tables, '_table_function.cluster')"
                ),
            )
            .await;
            if logged > 0 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "K10 request {n}: the route's statement never reached the query log"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let graph_statements = count(
            &r.s1,
            &format!(
                "SELECT count() AS n FROM system.query_log WHERE type = 'QueryFinish' \
                 AND is_initial_query AND startsWith(query_id, '{prefix}-') \
                 AND has(tables, '_table_function.cluster') \
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
    sync_replica(&r.s1b, &db, "spans", "K7: spans synced on ch-shard1b").await;
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
    let r = Replicas::new_current("k2").await;
    let (db, l) = (r.db.clone(), r.l);
    exec(&r.s1b, "SYSTEM STOP FETCHES spans").await;
    push_g(&r.s1, l, 0..8).await;
    // The entries are copied, so a barrier on the log alone would pass.
    let state = wait_state(&r.s1b, &db, "spans", "K2: G queued on ch-shard1b", |s| {
        s.inserts_in_queue > 0 && s.log_pointer > s.log_max_index
    })
    .await;
    sync_replica(&r.s1b, &db, "service_edges", "K2").await;
    r.wait_eligible().await;
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
    let r = Replicas::new_current("k3").await;
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
        sync_replica(c, &db, "service_edges", "K3").await;
    }
    r.wait_eligible().await;
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
        empty_term(),
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
    let r = Replicas::new_current("k8").await;
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
    // Part C: G's entries, committed before its interval became eligible,
    // hold the run though they are not copied.
    r.wait_eligible().await;
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

// Issue #599 part C: a barrier read from Keeper
// =====================================================================
//
// The job's barrier reads each replica's queue from Keeper, entry by
// entry, after the replication log is copied and the replica is not under
// repair. The cases below build a queue state, check it in Keeper, and
// refresh. The cases that write a node through `INSERT INTO
// system.zookeeper` need `allow_zookeeper_write`, which the fixture's
// `config.d/keeper_write.xml` sets.

/// `db.table`'s replica path in Keeper on this node.
async fn replica_path(client: &ChClient, db: &str, table: &str) -> String {
    strings(
        client,
        &format!(
            "SELECT replica_path AS s FROM system.replicas WHERE database = '{db}' \
             AND table = '{table}'"
        ),
    )
    .await
    .into_iter()
    .next()
    .unwrap_or_else(|| panic!("{db}.{table} is replicated on this node"))
}

/// This node's replica name, the `source replica` its inserts write.
async fn replica_name(client: &ChClient) -> String {
    strings(client, "SELECT getMacro('replica') AS s").await[0].clone()
}

/// The values of `db.table`'s queue nodes in Keeper on this node, by name.
async fn queue_values(client: &ChClient, db: &str, table: &str) -> Vec<String> {
    let path = replica_path(client, db, table).await;
    strings(
        client,
        &format!(
            "SELECT value AS s FROM system.zookeeper WHERE path = '{path}/queue' \
             ORDER BY name"
        ),
    )
    .await
}

/// The number of `db.table`'s queue nodes in Keeper on this node.
async fn queue_size(client: &ChClient, db: &str, table: &str) -> u64 {
    let path = replica_path(client, db, table).await;
    count(
        client,
        &format!(
            "SELECT toUInt64(numChildren) AS n FROM system.zookeeper WHERE path = '{path}' \
             AND name = 'queue'"
        ),
    )
    .await
}

/// Polls until `db.table`'s queue in Keeper satisfies `want`, for up to
/// two minutes; the test fails, rather than goes on, if it never does.
async fn wait_queue(
    client: &ChClient,
    db: &str,
    table: &str,
    what: &str,
    want: impl Fn(&[Entry]) -> bool,
) -> Vec<Entry> {
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let entries: Vec<Entry> = queue_values(client, db, table)
            .await
            .iter()
            .map(|v| Entry::parse(v))
            .collect();
        if want(&entries) {
            return entries;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: {db}.{table}'s queue never reached the state: {entries:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The fields of a queued entry the barrier reads, as the engine writes
/// them: `format version`, `create_time`, `source replica`, `block_id`, an
/// optional `log_entry_id`, the type, then (for a `get` or an `attach`) the
/// part.
#[derive(Debug, Clone, PartialEq)]
struct Entry {
    create_time: String,
    source: String,
    kind: String,
    part: String,
}

impl Entry {
    fn parse(value: &str) -> Entry {
        let field = |key: &str| {
            value
                .lines()
                .find_map(|l| l.strip_prefix(key))
                .unwrap_or_default()
                .to_string()
        };
        let mut lines = value.lines().skip_while(|l| !l.starts_with("block_id: "));
        lines.next();
        let mut kind = lines.next().unwrap_or_default();
        if kind.starts_with("log_entry_id: ") {
            kind = lines.next().unwrap_or_default();
        }
        let part = if kind == "get" || kind == "attach" {
            lines.next().unwrap_or_default()
        } else {
            ""
        };
        Entry {
            create_time: field("create_time: "),
            source: field("source replica: "),
            kind: kind.to_string(),
            part: part.to_string(),
        }
    }
}

/// The active parts of `db.spans` on this node.
async fn active_spans_parts(client: &ChClient, db: &str) -> Vec<String> {
    strings(
        client,
        &format!(
            "SELECT name AS s FROM system.parts WHERE database = '{db}' AND table = 'spans' \
             AND active ORDER BY name"
        ),
    )
    .await
}

/// `secs` as the engine writes an entry's `create_time`: the server's
/// zone, `YYYY-MM-DD hh:mm:ss`.
async fn log_time(client: &ChClient, secs: i64) -> String {
    strings(
        client,
        &format!("SELECT toString(toDateTime({secs}, serverTimezone())) AS s"),
    )
    .await[0]
        .clone()
}

/// `value` as a quoted ClickHouse string literal.
fn quoted(value: &str) -> String {
    let mut out = String::from("'");
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// Writes, or rewrites, `<replica path>/queue/queue-9000000000` of
/// `db.table` on this node: an existing node is set.
async fn queue_node(client: &ChClient, db: &str, table: &str, value: &str) {
    let path = replica_path(client, db, table).await;
    exec(
        client,
        &format!(
            "INSERT INTO system.zookeeper (name, path, value) \
             VALUES ('queue-9000000000', '{path}/queue', {})",
            quoted(value)
        ),
    )
    .await;
}

/// A queued entry's text: `format version: 4`, its `create_time`, its
/// `source replica`, an empty `block_id`, then `rest` (the type and part).
fn entry_text(create_time: &str, source: &str, rest: &str) -> String {
    format!(
        "format version: 4\ncreate_time: {create_time}\nsource replica: {source}\nblock_id: \n{rest}"
    )
}

/// `G`'s partition id in `table` on this node: one, since G's spans lie in
/// `[L + 1 s, L + 70 s)`.
async fn g_partition(client: &ChClient, db: &str, table: &str) -> String {
    let ids = strings(
        client,
        &format!(
            "SELECT DISTINCT partition_id AS s FROM system.parts WHERE database = '{db}' \
             AND table = '{table}' AND active"
        ),
    )
    .await;
    assert_eq!(ids.len(), 1, "G lies in one partition of {table}: {ids:?}");
    ids[0].clone()
}

/// The node's markers include `L + 4 min`: its run covered `[L, L + 5 min)`.
async fn assert_covers_g(client: &ChClient, l: i64, what: &str) {
    let m = markers(client).await;
    assert!(
        m.contains(&(l + 4 * MINUTE)),
        "{what}: the marker covers [L, L + 5 min): {m:?}"
    );
}

/// The node holds no marker after `L − 1 min`.
async fn assert_no_new_marker(client: &ChClient, l: i64, what: &str) {
    assert_eq!(
        markers(client).await,
        vec![l - MINUTE],
        "{what}: no marker after L - 1 min"
    );
}

/// KC1: a replica whose queue nodes and `log_pointer` are in Keeper, its
/// memory not yet holding them, writes nothing. The pull's own `multi` is
/// written by hand, its memory step never run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pulled_entry_not_yet_in_memory_holds_the_run() {
    skip_unless_live!();
    let r = Replicas::new("kc1").await;
    let (db, l) = (r.db.clone(), r.l);
    exec(&r.s1b, "SYSTEM STOP PULLING REPLICATION LOG spans").await;
    push_g(&r.s1, l, 0..8).await;
    wait_state(&r.s1, &db, "spans", "KC1: G announced", |s| {
        s.log_max_index >= 7
    })
    .await;
    let path = replica_path(&r.s1b, &db, "spans").await;
    let zk_path = strings(
        &r.s1b,
        &format!(
            "SELECT zookeeper_path AS s FROM system.replicas WHERE database = '{db}' \
             AND table = 'spans'"
        ),
    )
    .await[0]
        .clone();
    let pointer = replica_state(&r.s1b, &db, "spans").await.log_pointer;
    let log = strings(
        &r.s1b,
        &format!(
            "SELECT concat(name, '\\t', value) AS s FROM system.zookeeper \
             WHERE path = '{zk_path}/log' ORDER BY name"
        ),
    )
    .await;
    let early = log_time(&r.s1b, (l + MINUTE) / S).await;
    let mut rows = Vec::new();
    let mut next = pointer;
    for entry in &log {
        let (name, value) = entry.split_once('\t').expect("a name and a value");
        let index: u64 = name
            .strip_prefix("log-")
            .expect("a log node")
            .parse()
            .expect("a log index");
        if index < pointer {
            continue;
        }
        let value: String = value
            .lines()
            .map(|line| {
                if line.starts_with("create_time: ") {
                    format!("create_time: {early}\n")
                } else {
                    format!("{line}\n")
                }
            })
            .collect();
        rows.push(format!(
            "('queue-{:010}', '{path}/queue', {})",
            9_000_000_000u64 + index,
            quoted(&value)
        ));
        next = next.max(index + 1);
    }
    rows.push(format!("('log_pointer', '{path}', '{next}')"));
    exec(
        &r.s1b,
        &format!(
            "INSERT INTO system.zookeeper (name, path, value) VALUES {}",
            rows.join(", ")
        ),
    )
    .await;
    let state = replica_state(&r.s1b, &db, "spans").await;
    assert!(
        state.inserts_in_queue == 0 && state.log_pointer > state.log_max_index,
        "KC1: the memory holds no entry and the log reads copied: {state:?}"
    );
    let queued = queue_size(&r.s1b, &db, "spans").await;
    assert!(queued >= 8, "KC1: G's entries in Keeper's queue: {queued}");
    sync_replica(&r.s1b, &db, "service_edges", "KC1").await;
    refresh_on(&r.s1b, &db, "KC1: ch-shard1b's run").await;
    assert_no_new_marker(&r.s1b, l, "KC1: ch-shard1b's run").await;
    exec(&r.s1b, "SYSTEM RESTART REPLICA spans").await;
    exec(&r.s1b, "SYSTEM START PULLING REPLICATION LOG spans").await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC1: fetched").await;
    refresh_until(
        &r.s1b,
        &db,
        l + 4 * MINUTE,
        4,
        "KC1: the run after the fetch",
    )
    .await;
    r.assert_route_g1("KC1").await;
    r.drop().await;
}

/// KC4: a queued `REPLACE_RANGE` holds the run, though `system.replicas`
/// counts no insert and reads the log copied.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pending_replace_range_holds_the_run() {
    skip_unless_live!();
    let r = Replicas::new("kc4").await;
    let (db, l) = (r.db.clone(), r.l);
    exec(&r.s1b, "SYSTEM STOP FETCHES spans").await;
    exec(
        &r.s1,
        "CREATE TABLE spans_g AS spans ENGINE = MergeTree \
         PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') \
         ORDER BY (org_id, intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)",
    )
    .await;
    push_g_into(&r.s1, "spans_g", l, 0..8).await;
    let id = g_partition(&r.s1, &db, "spans_g").await;
    exec(
        &r.s1,
        &format!("ALTER TABLE spans REPLACE PARTITION ID '{id}' FROM spans_g"),
    )
    .await;
    wait_queue(&r.s1b, &db, "spans", "KC4: the replace queued", |q| {
        q.iter().any(|e| e.kind == "REPLACE_RANGE")
    })
    .await;
    let state = wait_state(&r.s1b, &db, "spans", "KC4: the log copied", |s| {
        s.log_pointer > s.log_max_index
    })
    .await;
    assert_eq!(
        state.inserts_in_queue, 0,
        "KC4: system.replicas counts no insert: {state:?}"
    );
    sync_replica(&r.s1b, &db, "service_edges", "KC4").await;
    refresh_on(&r.s1b, &db, "KC4: ch-shard1b's run").await;
    assert_no_new_marker(&r.s1b, l, "KC4: ch-shard1b's run").await;
    exec(&r.s1b, "SYSTEM START FETCHES spans").await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC4: fetched").await;
    refresh_until(
        &r.s1b,
        &db,
        l + 4 * MINUTE,
        4,
        "KC4: the run after the fetch",
    )
    .await;
    r.assert_route_g1("KC4").await;
    r.drop().await;
}

/// KC3: the queue cap. A queue of 1,000 fetches an insert queued after
/// eligibility lets the run through; 1,001 holds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queue_cap_holds_the_run() {
    skip_unless_live!();
    let r = Replicas::new("kc3").await;
    let (db, l) = (r.db.clone(), r.l);
    for c in [&r.s1, &r.s1b] {
        exec(c, "SYSTEM STOP MERGES spans").await;
    }
    exec(&r.s1b, "SYSTEM STOP FETCHES spans").await;
    let blocks = |tag: &str, n: u64| {
        format!(
            "INSERT INTO spans (trace_id, span_id, parent_span_id, start_ns, duration_ns, \
             service, name, kind, attrs, end_ns, service_type) \
             SELECT sipHash128(number, '{tag}'), reinterpretAsFixedString(cityHash64(number, '{tag}')), \
                    toFixedString('', 8), {at} + toInt64(number) * 1000000 AS st, 1000, 'svc-kc3', \
                    'op', 1, CAST('{{}}', 'JSON'), toUInt64(st + 1000), 'string' \
             FROM numbers({n}) \
             SETTINGS max_block_size = 1, max_insert_block_size = 1, min_insert_block_size_rows = 0, \
                      min_insert_block_size_bytes = 0, max_threads = 1, max_insert_threads = 1",
            at = l + 100 * S
        )
    };
    // One insert of 1,000 parts can take longer than the shared client's
    // 60 s cut on a busy runner: a client of its own, with 10 minutes.
    let slow = ChClient::new(ChConnConfig {
        query_timeout: Duration::from_secs(600),
        ..shard1_config(&db)
    })
    .await
    .expect("connect shard1");
    slow.execute(
        &blocks("kc3a", 1000),
        &QuerySettings::new().set("max_execution_time", 600),
        Idempotency::NonIdempotent,
    )
    .await
    .expect("KC3: 1,000 one-row blocks");
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let n = queue_size(&r.s1b, &db, "spans").await;
        if n == 1000 {
            break;
        }
        if n > 1000 {
            let mut kinds: Vec<(String, String)> = queue_values(&r.s1b, &db, "spans")
                .await
                .iter()
                .map(|v| {
                    let e = Entry::parse(v);
                    (e.kind, e.source)
                })
                .collect();
            kinds.sort();
            kinds.dedup();
            panic!("KC3: ch-shard1b's queue holds 1,000 entries: {n}, kinds {kinds:?}");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "KC3: ch-shard1b's queue holds 1,000 entries: {n}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    sync_replica(&r.s1b, &db, "service_edges", "KC3").await;
    refresh_on(&r.s1b, &db, "KC3: the run over 1,000 entries").await;
    assert_covers_g(&r.s1b, l, "KC3: the run over 1,000 entries").await;
    let before = markers(&r.s1b).await;
    exec(&r.s1, &blocks("kc3b", 1)).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let n = queue_size(&r.s1b, &db, "spans").await;
        if n == 1001 {
            break;
        }
        assert!(
            n < 1001 && std::time::Instant::now() < deadline,
            "KC3: ch-shard1b's queue holds 1,001 entries: {n}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    sync_replica(&r.s1b, &db, "service_edges", "KC3: the first run's part").await;
    assert_eq!(
        queue_size(&r.s1b, &db, "service_edges").await,
        0,
        "KC3: ch-shard1b's service_edges queue is empty"
    );
    refresh_on(&r.s1b, &db, "KC3: the run over 1,001 entries").await;
    assert_eq!(
        markers(&r.s1b).await,
        before,
        "KC3: the run over 1,001 entries wrote no marker"
    );
    exec(&r.s1b, "SYSTEM START FETCHES spans").await;
    r.drop().await;
}

/// KC5: a replica whose `spans` queue holds only a merge writes its marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queued_merge_alone_does_not_hold_the_run() {
    skip_unless_live!();
    let r = Replicas::new("kc5").await;
    let (db, l) = (r.db.clone(), r.l);
    push_g(&r.s1, l, 0..8).await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC5: G on both replicas").await;
    exec(&r.s1b, "SYSTEM STOP MERGES spans").await;
    exec(&r.s1, "SYSTEM START MERGES spans").await;
    let id = g_partition(&r.s1, &db, "spans").await;
    exec(
        &r.s1,
        &format!("OPTIMIZE TABLE spans PARTITION ID '{id}' FINAL"),
    )
    .await;
    let q = wait_queue(&r.s1b, &db, "spans", "KC5: the merge queued", |q| {
        !q.is_empty() && q.iter().all(|e| e.kind == "merge")
    })
    .await;
    sync_replica(&r.s1b, &db, "service_edges", "KC5").await;
    refresh_on(&r.s1b, &db, "KC5: ch-shard1b's run").await;
    assert_covers_g(&r.s1b, l, &format!("KC5: ch-shard1b's run over {q:?}")).await;
    r.assert_route_g1("KC5").await;
    r.drop().await;
}

/// KC8: a replica whose `service_edges` queue holds only a merge writes the
/// next interval's marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queued_totals_merge_alone_does_not_hold_the_run() {
    skip_unless_live!();
    let r = Replicas::new("kc8").await;
    let (db, l) = (r.db.clone(), r.l);
    push_g(&r.s1, l, 0..8).await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC8: G on both replicas").await;
    refresh_until(&r.s1, &db, l + 4 * MINUTE, 4, "KC8: ch-shard1's run").await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC8: ch-shard1's run on both").await;
    exec(&r.s1, "SYSTEM START MERGES service_edges").await;
    exec(&r.s1, "OPTIMIZE TABLE service_edges FINAL").await;
    let q = wait_queue(&r.s1b, &db, "service_edges", "KC8: the merge queued", |q| {
        !q.is_empty() && q.iter().all(|e| e.kind == "merge")
    })
    .await;
    let spans_queue = queue_values(&r.s1b, &db, "spans").await;
    assert!(
        spans_queue.is_empty(),
        "KC8: ch-shard1b's spans queue is empty: {spans_queue:?}"
    );
    refresh_on(&r.s1b, &db, "KC8: ch-shard1b's run").await;
    let m = markers(&r.s1b).await;
    assert!(
        m.contains(&(l + 9 * MINUTE)),
        "KC8: ch-shard1b's run over {q:?} wrote the next interval's marker: {m:?}"
    );
    r.drop().await;
}

/// KC6: a replica's own inserts, queued as fetches of parts it holds, do
/// not hold its run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_replica_s_own_parts_do_not_hold_the_run() {
    skip_unless_live!();
    let r = Replicas::new_current("kc6").await;
    let (db, l) = (r.db.clone(), r.l);
    exec(&r.s2, "SYSTEM STOP REPLICATION QUEUES spans").await;
    push_g(&r.s2, l, 0..8).await;
    let me = replica_name(&r.s2).await;
    let eligible = log_time(&r.s2, (l + 8 * MINUTE) / S).await;
    let active = active_spans_parts(&r.s2, &db).await;
    let q = wait_queue(&r.s2, &db, "spans", "KC6: G's entries queued", |q| {
        q.len() == 8
    })
    .await;
    for e in &q {
        assert!(
            e.kind == "get"
                && e.source == me
                && active.contains(&e.part)
                && e.create_time < eligible,
            "KC6: every entry is a get from {me} of a part active here, created before \
             {eligible}: {e:?}, active {active:?}"
        );
    }
    r.wait_eligible().await;
    sync_replica(&r.s2, &db, "service_edges", "KC6").await;
    refresh_on(&r.s2, &db, "KC6: ch-shard2's run").await;
    assert_covers_g(&r.s2, l, "KC6: ch-shard2's run").await;
    exec(&r.s2, "SYSTEM START REPLICATION QUEUES spans").await;
    wait_synced(&[&r.s2], &db, "KC6: the queue run").await;
    // From the totals: no span is left to pair.
    exec(&r.s2, "TRUNCATE TABLE spans").await;
    r.assert_route_g1("KC6").await;
    r.drop().await;
}

/// KC18: a replica's own insert into `service_edges`, queued as a fetch of
/// a part it holds, does not hold its run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_replica_s_own_marker_does_not_hold_the_run() {
    skip_unless_live!();
    let r = Replicas::new_current("kc18").await;
    let (db, l) = (r.db.clone(), r.l);
    push_g(&r.s2, l, 0..8).await;
    exec(&r.s2, "SYSTEM STOP FETCHES service_edges").await;
    // The seeded marker again, at `ver` 2 so it is a new block.
    exec(
        &r.s2,
        &format!(
            "INSERT INTO service_edges SELECT '', {t}, {t}, '', '', '', 0, 0, \
             arrayReduce('quantilesTDigestState(0.5, 0.95, 0.99)', [toInt64(0)]), 2",
            t = format_args!("toDateTime({}, 'UTC')", (l - MINUTE) / S)
        ),
    )
    .await;
    let me = replica_name(&r.s2).await;
    let active = strings(
        &r.s2,
        &format!(
            "SELECT name AS s FROM system.parts WHERE database = '{db}' \
             AND table = 'service_edges' AND active ORDER BY name"
        ),
    )
    .await;
    let q = wait_queue(
        &r.s2,
        &db,
        "service_edges",
        "KC18: the insert queued",
        |q| q.len() == 1,
    )
    .await;
    assert!(
        q[0].kind == "get" && q[0].source == me && active.contains(&q[0].part),
        "KC18: the one entry is a get from {me} of a part active here: {q:?}, active {active:?}"
    );
    r.wait_eligible().await;
    sync_replica(&r.s2, &db, "spans", "KC18").await;
    refresh_on(&r.s2, &db, "KC18: ch-shard2's run").await;
    assert_covers_g(&r.s2, l, "KC18: ch-shard2's run").await;
    exec(&r.s2, "SYSTEM START FETCHES service_edges").await;
    r.drop().await;
}

/// KC19: a replica's own inserts, queued as fetches of parts a local merge
/// has since covered, do not hold its run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_replica_s_merged_own_parts_do_not_hold_the_run() {
    skip_unless_live!();
    let r = Replicas::new_current("kc19").await;
    let (db, l) = (r.db.clone(), r.l);
    exec(&r.s2, "SYSTEM STOP FETCHES spans").await;
    exec(&r.s2, "SYSTEM START MERGES spans").await;
    push_g(&r.s2, l, 0..8).await;
    exec(&r.s2, "OPTIMIZE TABLE spans FINAL").await;
    let me = replica_name(&r.s2).await;
    let eligible = log_time(&r.s2, (l + 8 * MINUTE) / S).await;
    let active = active_spans_parts(&r.s2, &db).await;
    let q = wait_queue(&r.s2, &db, "spans", "KC19: G's entries queued", |q| {
        q.len() == 8 && q.iter().all(|e| e.kind == "get")
    })
    .await;
    assert_eq!(
        active.len(),
        1,
        "KC19: one active part after the merge: {active:?}"
    );
    for e in &q {
        assert!(
            e.source == me && !active.contains(&e.part) && e.create_time < eligible,
            "KC19: every entry is a get from {me} of a part merged away here, created before \
             {eligible}: {e:?}, active {active:?}"
        );
    }
    r.wait_eligible().await;
    sync_replica(&r.s2, &db, "service_edges", "KC19").await;
    refresh_on(&r.s2, &db, "KC19: ch-shard2's run").await;
    assert_covers_g(&r.s2, l, "KC19: ch-shard2's run").await;
    exec(&r.s2, "SYSTEM START FETCHES spans").await;
    r.drop().await;
}

/// The state KC9 and KC11 share: G on both replicas, its partition detached
/// and attached again on `ch-shard1` with the queues of `stopped` stopped.
async fn attach_g(r: &Replicas, stopped: &ChClient, what: &str) {
    push_g(&r.s1, r.l, 0..8).await;
    wait_synced(&[&r.s1, &r.s1b], &r.db, what).await;
    let id = g_partition(&r.s1, &r.db, "spans").await;
    exec(
        &r.s1,
        &format!("ALTER TABLE spans DETACH PARTITION ID '{id}'"),
    )
    .await;
    wait_synced(&[&r.s1, &r.s1b], &r.db, what).await;
    exec(stopped, "SYSTEM STOP REPLICATION QUEUES spans").await;
    exec(
        &r.s1,
        &format!("ALTER TABLE spans ATTACH PARTITION ID '{id}'"),
    )
    .await;
}

/// KC9: an `attach` of parts the replica holds does not hold its run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_attach_does_not_hold_the_run() {
    skip_unless_live!();
    let r = Replicas::new("kc9").await;
    let (db, l) = (r.db.clone(), r.l);
    attach_g(&r, &r.s1, "KC9").await;
    let me = replica_name(&r.s1).await;
    let active = active_spans_parts(&r.s1, &db).await;
    let q = wait_queue(&r.s1, &db, "spans", "KC9: the attach queued", |q| {
        !q.is_empty()
    })
    .await;
    for e in &q {
        assert!(
            e.kind == "attach" && e.source == me && active.contains(&e.part),
            "KC9: every entry is an attach from {me} of a part active here: {e:?}, \
             active {active:?}"
        );
    }
    sync_replica(&r.s1, &db, "service_edges", "KC9").await;
    refresh_on(&r.s1, &db, "KC9: ch-shard1's run").await;
    assert_covers_g(&r.s1, l, "KC9: ch-shard1's run").await;
    exec(&r.s1, "SYSTEM START REPLICATION QUEUES spans").await;
    r.assert_route_g1("KC9").await;
    r.drop().await;
}

/// KC11: an `attach` queued after eligibility, of parts the replica does
/// not hold, holds its run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_attach_holds_the_run() {
    skip_unless_live!();
    let r = Replicas::new("kc11").await;
    let (db, l) = (r.db.clone(), r.l);
    attach_g(&r, &r.s1b, "KC11").await;
    let source = replica_name(&r.s1).await;
    let late = log_time(&r.s1b, (l + 8 * MINUTE + 10 * S) / S).await;
    let q = wait_queue(&r.s1b, &db, "spans", "KC11: the attach queued", |q| {
        !q.is_empty()
    })
    .await;
    let active = active_spans_parts(&r.s1b, &db).await;
    for e in &q {
        assert!(
            e.kind == "attach"
                && e.source == source
                && e.create_time > late
                && !active.contains(&e.part),
            "KC11: every entry is an attach from {source}, created after {late}, of a part \
             not active here: {e:?}, active {active:?}"
        );
    }
    sync_replica(&r.s1b, &db, "service_edges", "KC11").await;
    refresh_on(&r.s1b, &db, "KC11: ch-shard1b's run").await;
    assert_no_new_marker(&r.s1b, l, "KC11: ch-shard1b's run").await;
    exec(&r.s1b, "SYSTEM START REPLICATION QUEUES spans").await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC11: attached").await;
    refresh_until(
        &r.s1b,
        &db,
        l + 4 * MINUTE,
        4,
        "KC11: the run after the attach",
    )
    .await;
    r.assert_route_g1("KC11").await;
    r.drop().await;
}

/// `node`'s two queues in Keeper hold only `value`, in `spans`.
async fn assert_only_entry(r: &Replicas, value: &str, what: &str) {
    assert_eq!(
        queue_values(&r.s1b, &r.db, "spans").await,
        vec![value.to_string()],
        "{what}: the crafted node is ch-shard1b's only spans entry"
    );
    assert_eq!(
        queue_size(&r.s1b, &r.db, "service_edges").await,
        0,
        "{what}: ch-shard1b's service_edges queue is empty"
    );
}

/// KC10: a fetch queued 9 s after eligibility holds the run; one queued
/// 10 s after does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_late_cut_off_is_ten_seconds_after_eligibility() {
    skip_unless_live!();
    let r = Replicas::new("kc10").await;
    let (db, l) = (r.db.clone(), r.l);
    push_g(&r.s1, l, 0..8).await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC10: G on both replicas").await;
    let source = replica_name(&r.s1).await;
    for (secs, passes) in [(9, false), (10, true)] {
        let t = log_time(&r.s1b, (l + 8 * MINUTE) / S + secs).await;
        let value = entry_text(&t, &source, "get\ncrafted_9_9_0\n");
        queue_node(&r.s1b, &db, "spans", &value).await;
        let what = format!("KC10: a fetch created at eligibility + {secs} s");
        assert_only_entry(&r, &value, &what).await;
        refresh_on(&r.s1b, &db, &what).await;
        if passes {
            assert_covers_g(&r.s1b, l, &what).await;
        } else {
            assert_no_new_marker(&r.s1b, l, &what).await;
        }
    }
    r.assert_route_g1("KC10").await;
    r.drop().await;
}

/// KC7: an entry whose type, time or part is not read holds the run, and
/// so does a fetch with no source replica (a repair's); a merge, the
/// control, does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreadable_or_repair_entry_holds_the_run() {
    skip_unless_live!();
    let r = Replicas::new("kc7").await;
    let (db, l) = (r.db.clone(), r.l);
    push_g(&r.s1, l, 0..8).await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC7: G on both replicas").await;
    let source = replica_name(&r.s1).await;
    let p = active_spans_parts(&r.s1b, &db)
        .await
        .into_iter()
        .next()
        .expect("KC7: a part active on ch-shard1b");
    let t = log_time(&r.s1b, server_now_us(&r.s1b).await / 1_000_000).await;
    let rows = [
        ("1, garbage", "garbage".to_string()),
        (
            "2, time not read",
            entry_text("x", &source, &format!("get\n{p}\n")),
        ),
        ("3, part not read", entry_text(&t, &source, "get\n")),
        (
            "4, no source, a part not held",
            entry_text(&t, "", "get\ncrafted_9_9_0\n"),
        ),
        (
            "5, no source, a part held",
            entry_text(&t, "", &format!("get\n{p}\n")),
        ),
        (
            "6, a merge",
            entry_text(&t, &source, &format!("merge\n{p}\ninto\ncrafted_9_9_1\n")),
        ),
    ];
    for (row, value) in &rows {
        queue_node(&r.s1b, &db, "spans", value).await;
        let what = format!("KC7 row {row}");
        assert_only_entry(&r, value, &what).await;
        refresh_on(&r.s1b, &db, &what).await;
        if row.starts_with('6') {
            assert_covers_g(&r.s1b, l, &what).await;
        } else {
            assert_no_new_marker(&r.s1b, l, &what).await;
        }
    }
    r.assert_route_g1("KC7").await;
    r.drop().await;
}

/// KC12: a replica marked lost, being rebuilt, writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replica_under_repair_holds_the_run() {
    skip_unless_live!();
    let r = Replicas::new("kc12").await;
    let (db, l) = (r.db.clone(), r.l);
    push_g(&r.s1, l, 0..8).await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC12: G on both replicas").await;
    let path = replica_path(&r.s1b, &db, "spans").await;
    for (lost, passes) in [("1", false), ("0", true)] {
        exec(
            &r.s1b,
            &format!(
                "INSERT INTO system.zookeeper (name, path, value) \
                 VALUES ('is_lost', '{path}', '{lost}')"
            ),
        )
        .await;
        let what = format!("KC12: is_lost '{lost}'");
        for table in ["spans", "service_edges"] {
            assert_eq!(
                queue_size(&r.s1b, &db, table).await,
                0,
                "{what}: ch-shard1b's {table} queue is empty"
            );
            let state = replica_state(&r.s1b, &db, table).await;
            assert!(
                state.log_pointer > state.log_max_index,
                "{what}: ch-shard1b's {table} log is copied: {state:?}"
            );
        }
        refresh_on(&r.s1b, &db, &what).await;
        if passes {
            assert_covers_g(&r.s1b, l, &what).await;
        } else {
            assert_no_new_marker(&r.s1b, l, &what).await;
        }
    }
    r.assert_route_g1("KC12").await;
    r.drop().await;
}

/// One push of LV1's load: `pairs` client and server pairs starting now,
/// their ids from `seq`.
fn load_push(seq: u64, pairs: u64) -> ExportTraceServiceRequest {
    let at = u64::try_from(now_ns()).expect("a post-epoch clock");
    let id = |n: u64, bytes: usize| -> Vec<u8> {
        let mut v = vec![0u8; bytes];
        v[0] = 0x6c;
        v[bytes - 8..].copy_from_slice(&n.to_be_bytes());
        v
    };
    let mut clients = Vec::new();
    let mut servers = Vec::new();
    for i in 0..pairs {
        let n = seq * pairs + i + 1;
        let span = |span_id: Vec<u8>, parent: Vec<u8>, kind: i32| Span {
            trace_id: id(n, 16),
            span_id,
            parent_span_id: parent,
            name: "op".to_string(),
            kind,
            start_time_unix_nano: at,
            end_time_unix_nano: at + 1_000_000,
            ..Span::default()
        };
        clients.push(span(id(2 * n, 8), Vec::new(), CLIENT));
        servers.push(span(id(2 * n + 1, 8), id(2 * n, 8), SERVER));
    }
    let resource = |service: &str, spans: Vec<Span>| ResourceSpans {
        resource: Some(Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(service.to_string())),
                }),
                ..KeyValue::default()
            }],
            dropped_attributes_count: 0,
            entity_refs: Vec::new(),
        }),
        scope_spans: vec![ScopeSpans {
            scope: None,
            spans,
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    };
    ExportTraceServiceRequest {
        resource_spans: vec![
            resource("lv-client", clients),
            resource("lv-server", servers),
        ],
    }
}

/// The product's trace writer over one node, with the product's settings.
async fn node_writer(config: ChConnConfig) -> pulsus_write::writer::TraceWriter {
    use pulsus_write::writer::{ChBlockInserter, TraceWriter, TraceWriterTables};
    use std::sync::Arc;
    let client = Arc::new(ChClient::new(config).await.expect("connect the writer"));
    TraceWriter::with_inserters_with_tables(
        Arc::new(ChBlockInserter::new(client)),
        &pulsus_config::WriterConfig::default(),
        TraceWriterTables::traces_default(),
    )
}

/// The node's job's last successful run, as text; empty before the first.
async fn last_success(client: &ChClient) -> String {
    strings(
        client,
        "SELECT ifNull(toString(last_success_time), '') AS s FROM system.view_refreshes \
         WHERE database = currentDatabase() AND view = 'service_edges_mv'",
    )
    .await
    .into_iter()
    .next()
    .unwrap_or_default()
}

/// LV1: under steady ingest on every node, with merges and fetches running
/// and every node's job on its schedule, each shard finalizes every interval
/// within one scheduled run of catching up with it (issue #602).
///
/// **The bound is relative to replication, not to the clock.** A run
/// finalizes an interval only once its replica holds every span insert made
/// before the cut-off (`docs/api.md` §4.5), so how soon that happens is the
/// host's replication speed, which no product statement bounds. For interval
/// `i` and a shard, `T` is the later of: the first sample at or after the
/// cut-off at which a replica of the shard passes [`caught_up_sql`]; and the
/// first sample at which interval `i - 1` was covered on the shard (the load
/// start for the first interval), because a run finalizes only the oldest
/// unfinalized interval. The interval must be covered by `T + 60 s + D +
/// 15 s`: one schedule period, `D` the longest run seen on the shard in
/// whole seconds rounded up, and 15 s for the gap between a scheduled start
/// and the statement plus one loop iteration. An interval with no `T`
/// fails: replication did not catch up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_job_keeps_up_under_steady_ingest() {
    use pulsus_write::{PushHeaders, TraceSink};
    skip_unless_live!();
    let r = Replicas::new("lv1").await;
    let db = r.db.clone();
    for c in [&r.s1, &r.s1b, &r.s2] {
        for table in ["spans", "service_edges"] {
            exec(c, &format!("SYSTEM START MERGES {table}")).await;
            exec(
                c,
                &format!("ALTER TABLE {table} RESET SETTING max_replicated_merges_in_queue"),
            )
            .await;
        }
    }
    let newest_eligible = |now_us: i64| floor_to(now_us * 1000 - 3 * MINUTE, BUCKET) - MINUTE;
    let newest = newest_eligible(server_now_us(&r.s1).await);
    run_edges_until(&r.s1, newest).await;
    run_edges_until(&r.s2, newest).await;
    exec(&r.s1b, "SYSTEM START VIEW service_edges_mv").await;

    // Start just after an interval became eligible.
    let deadline = std::time::Instant::now() + Duration::from_secs(330);
    loop {
        let into = (server_now_us(&r.s1).await / 1_000_000).rem_euclid(300);
        if (180..200).contains(&into) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "LV1: no start in [180 s, 200 s) of an interval"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let start_us = server_now_us(&r.s1).await;
    let load_end_us = start_us + 16 * 60 * 1_000_000;
    // The intervals that become eligible during the load, by eligibility.
    let mut watched: Vec<(i64, i64)> = Vec::new();
    let mut b = floor_to(start_us * 1000 - 8 * MINUTE, BUCKET) + BUCKET;
    while (b + 8 * MINUTE) / 1000 <= load_end_us {
        watched.push((b, (b + 8 * MINUTE) / 1000));
        b += BUCKET;
    }
    assert!(watched.len() >= 3, "LV1: three intervals: {watched:?}");

    // The load: one writer per node, 5 pushes a second together, each of
    // 1,000 spans. Twice that rate was more than a four-core CI runner's
    // three-node cluster could replicate: its replication queues passed the
    // job's 1,000-entry hold and the sync answers queued past 60 s (issue
    // #602 part 3b-1). At this rate the run still keeps the queues busy
    // enough to hold the job if its catch-up rule is removed.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let failures = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let first_failure = std::sync::Arc::new(std::sync::Mutex::new(None::<String>));
    let mut tasks = Vec::new();
    for (k, config) in [shard1_config(&db), shard1b_config(&db), shard2_config(&db)]
        .into_iter()
        .enumerate()
    {
        let writer = std::sync::Arc::new(node_writer(config).await);
        let stop = stop.clone();
        let failures = failures.clone();
        let first_failure = first_failure.clone();
        tasks.push(tokio::spawn(async move {
            // Each answered push's admit-to-answer time, in ms; a push not
            // answered within 60 s is recorded as 60,000.
            let answer_ms = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
            let mut tick = tokio::time::interval(Duration::from_millis(600));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut seq = k as u64;
            let mut waits = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                tick.tick().await;
                let req = load_push(seq, 500);
                seq += 3;
                let landing = pulsus_write::parse_trace_landing(&req).expect("the landing decode");
                let admitted_at = std::time::Instant::now();
                match writer.admit_flush(&empty_tenant(), landing, PushHeaders::default()) {
                    Ok(wait) => {
                        let failures = failures.clone();
                        let first_failure = first_failure.clone();
                        let answer_ms = answer_ms.clone();
                        waits.push(tokio::spawn(async move {
                            let answered =
                                tokio::time::timeout(Duration::from_secs(60), wait).await;
                            answer_ms
                                .lock()
                                .expect("the lock")
                                .push(admitted_at.elapsed().as_millis().min(60_000) as u64);
                            let failed = match answered {
                                Ok(Ok(())) => None,
                                Ok(Err(e)) => Some(e.to_string()),
                                Err(_) => Some("no answer within 60 s".to_string()),
                            };
                            if let Some(e) = failed {
                                failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                first_failure.lock().expect("the lock").get_or_insert(e);
                            }
                        }));
                    }
                    Err(e) => {
                        failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        first_failure
                            .lock()
                            .expect("the lock")
                            .get_or_insert(format!("refused: {e:?}"));
                    }
                }
            }
            for w in waits {
                w.await.expect("a push's wait");
            }
            writer.shutdown(Duration::from_secs(30)).await;
            let mut ms = answer_ms.lock().expect("the lock").clone();
            ms.sort_unstable();
            let at = |q: f64| ms.get(((ms.len().max(1) - 1) as f64 * q) as usize).copied();
            let line = format!(
                "writer {k}: {} answers, p50 {:?} ms, p99 {:?} ms, max {:?} ms",
                ms.len(),
                at(0.5),
                at(0.99),
                ms.last()
            );
            (k, line, at(0.99))
        }));
    }

    // The samples: each node's two queues every second, each shard's
    // prefix end after every completed run on its nodes.
    let nodes = [
        ("ch-shard1", &r.s1),
        ("ch-shard1b", &r.s1b),
        ("ch-shard2", &r.s2),
    ];
    let mut last: Vec<String> = Vec::new();
    for (_, c) in &nodes {
        last.push(last_success(c).await);
    }
    // Server seconds throughout: when each interval was covered on each
    // shard, and when each node first passed the catch-up check for it.
    let mut covered: Vec<[Option<i64>; 2]> = vec![[None, None]; watched.len()];
    let mut caught: Vec<[Option<i64>; 3]> = vec![[None, None, None]; watched.len()];
    let mut longest_ms = [0i64; 2];
    let mut lags: Vec<String> = Vec::new();
    let mut queue_samples: Vec<String> = Vec::new();
    let start_s = start_us / 1_000_000;
    let give_up_us = load_end_us + 5 * 60 * 1_000_000;
    loop {
        let now_us = server_now_us(&r.s1).await;
        let now_s = now_us / 1_000_000;
        if now_us >= load_end_us {
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let all_covered = covered.iter().all(|c| c.iter().all(Option::is_some));
        if now_us >= load_end_us && (all_covered || now_us >= give_up_us) {
            break;
        }
        let mut sample = format!("{}", (now_us - start_us) / 1_000_000);
        for (node, c) in &nodes {
            sample.push_str(&format!(
                " {node} {}/{}",
                queue_size(c, &db, "spans").await,
                queue_size(c, &db, "service_edges").await
            ));
        }
        queue_samples.push(sample);
        for (n, (_, c)) in nodes.iter().enumerate() {
            let check = caught_up(c, &db).await;
            for (i, (_, at_us)) in watched.iter().enumerate() {
                let cutoff_s = at_us / 1_000_000 + 10;
                if caught[i][n].is_none() && now_s >= cutoff_s && check.passes(cutoff_s) {
                    caught[i][n] = Some(now_s);
                }
            }
        }
        // Covered on a shard: the largest prefix end over its replicas.
        let ends = [
            prefix_end(&r.s1).await.max(prefix_end(&r.s1b).await),
            prefix_end(&r.s2).await,
        ];
        for (i, (b, _)) in watched.iter().enumerate() {
            for shard in 0..2 {
                if covered[i][shard].is_none() && ends[shard] >= b + 4 * MINUTE {
                    covered[i][shard] = Some(now_s);
                }
            }
        }
        for (i, (node, c)) in nodes.iter().enumerate() {
            let now_last = last_success(c).await;
            if now_last != last[i] {
                last[i] = now_last;
                let end = if *node == "ch-shard2" {
                    ends[1]
                } else {
                    ends[0]
                };
                let lag = newest_eligible(server_now_us(c).await) - end;
                let shard = usize::from(*node == "ch-shard2");
                longest_ms[shard] = longest_ms[shard].max(last_duration_ms(c).await);
                lags.push(format!(
                    "{node} at {}: {} s",
                    (now_us - start_us) / 1_000_000,
                    lag / S
                ));
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut p99s = Vec::new();
    for t in tasks {
        let (k, latency, p99) = t.await.expect("a writer task");
        println!("LV1 answer latency, {latency}");
        p99s.push((k, p99));
    }
    println!("LV1 queue samples (s since start, spans/service_edges per node):");
    for s in &queue_samples {
        println!("  {s}");
    }
    println!("LV1 lag after each completed run: {lags:?}");
    println!("LV1 intervals {watched:?}; covered at (server s): {covered:?}");
    println!("LV1 caught up at (server s, per node): {caught:?}; longest run (ms): {longest_ms:?}");
    assert_eq!(
        failures.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "LV1: every push was stored; the first failure: {:?}",
        first_failure.lock().expect("the lock")
    );
    // The OTLP exporter's own default timeout is 10 s: a sync answer slower
    // than that is a client retry, whatever the 60 s check above allows.
    for (k, p99) in &p99s {
        assert!(
            p99.is_some_and(|ms| ms <= 10_000),
            "LV1: writer {k}'s answer p99 is within 10 s: {p99:?} ms"
        );
    }
    let shard_nodes: [&[usize]; 2] = [&[0, 1], &[2]];
    let mut under_load = [false; 2];
    let mut wrong = Vec::new();
    for (i, ((b, at_us), shards)) in watched.iter().zip(&covered).enumerate() {
        for (shard, nodes_of) in shard_nodes.iter().enumerate() {
            let first = nodes_of.iter().filter_map(|&n| caught[i][n]).min();
            let previous = if i == 0 {
                Some(start_s)
            } else {
                covered[i - 1][shard]
            };
            let Some(t) = first.zip(previous).map(|(a, p)| a.max(p)) else {
                wrong.push(format!(
                    "the interval at {b} on shard {}: replication did not catch up",
                    shard + 1
                ));
                continue;
            };
            under_load[shard] |= t * 1_000_000 < load_end_us;
            let bound = t + 60 + (longest_ms[shard] + 999) / 1000 + 15;
            if shards[shard].is_none_or(|c| c > bound) {
                wrong.push(format!(
                    "the interval at {b} on shard {}: caught up {} s after eligibility, \
                     covered {:?}, bound {} s",
                    shard + 1,
                    t - at_us / 1_000_000,
                    shards[shard].map(|c| c - at_us / 1_000_000),
                    bound - at_us / 1_000_000
                ));
            }
        }
    }
    assert!(
        under_load.iter().all(|&x| x),
        "LV1: each shard caught up with an interval while the load ran: {under_load:?}"
    );
    assert!(wrong.is_empty(), "LV1:\n{}", wrong.join("\n"));
    for c in [&r.s1, &r.s1b, &r.s2] {
        stop_edges_view(c).await;
    }
    r.drop().await;
}

/// One node's replication state, read by [`caught_up_sql`].
struct CaughtUp {
    spans_queue: i64,
    spans_oldest: i64,
    spans_copied: bool,
    spans_pulled: i64,
    edges_holding: i64,
    edges_queue: i64,
    edges_copied: bool,
}

impl CaughtUp {
    /// Whether this node has caught up with the interval whose cut-off is
    /// `cutoff_s`: every span insert from before it is here, and nothing in
    /// `service_edges` but merges and fetches of parts already here waits.
    /// Stricter than the job (it counts own fetches of merged parts, which
    /// the job passes), so it can report catch-up late and never early.
    fn passes(&self, cutoff_s: i64) -> bool {
        self.spans_queue <= 1000
            && (self.spans_oldest == 0 || self.spans_oldest >= cutoff_s)
            && (self.spans_copied || self.spans_pulled >= cutoff_s)
            && self.edges_holding == 0
            && self.edges_queue <= 1000
            && self.edges_copied
    }
}

/// The catch-up check LV1 reads on each node once per loop iteration, its
/// seven fields joined by spaces.
fn caught_up_sql(db: &str) -> String {
    format!(
        "SELECT concat(toString(s.queue_size), ' ', \
         toString(toUnixTimestamp(s.inserts_oldest_time)), ' ', \
         toString(s.log_pointer > s.log_max_index OR (s.log_pointer = 0 AND s.log_max_index = 0)), ' ', \
         toString(toUnixTimestamp(s.last_queue_update)), ' ', \
         toString(ifNull((SELECT count() FROM system.replication_queue \
           WHERE database = '{db}' AND table = 'service_edges' AND type != 'MERGE_PARTS' \
           AND NOT (type IN ('GET_PART', 'ATTACH_PART') AND source_replica != '' \
           AND new_part_name IN (SELECT name FROM system.parts \
             WHERE database = '{db}' AND table = 'service_edges' AND active))), 0)), ' ', \
         toString(e.queue_size), ' ', \
         toString(e.log_pointer > e.log_max_index OR (e.log_pointer = 0 AND e.log_max_index = 0))) AS s \
         FROM system.replicas AS s, system.replicas AS e \
         WHERE s.database = '{db}' AND s.table = 'spans' \
           AND e.database = '{db}' AND e.table = 'service_edges'"
    )
}

/// This node's [`caught_up_sql`] row.
async fn caught_up(client: &ChClient, db: &str) -> CaughtUp {
    let row = strings(client, &caught_up_sql(db)).await;
    let v: Vec<i64> = row
        .first()
        .unwrap_or_else(|| panic!("LV1: the catch-up check returned no row on {db}"))
        .split(' ')
        .map(|x| x.parse().expect("a number"))
        .collect();
    CaughtUp {
        spans_queue: v[0],
        spans_oldest: v[1],
        spans_copied: v[2] == 1,
        spans_pulled: v[3],
        edges_holding: v[4],
        edges_queue: v[5],
        edges_copied: v[6] == 1,
    }
}

/// The node's job's last successful run's duration, in ms; 0 before the
/// first. The column is `Nullable`, so it is read through `ifNull`.
async fn last_duration_ms(client: &ChClient) -> i64 {
    strings(
        client,
        "SELECT toString(ifNull(last_success_duration_ms, 0)) AS s FROM system.view_refreshes \
         WHERE database = currentDatabase() AND view = 'service_edges_mv'",
    )
    .await
    .first()
    .and_then(|x| x.parse().ok())
    .unwrap_or(0)
}

/// One entry of `db.spans`' replication log at or past this node's
/// pointer: its index, its Keeper commit time (Unix seconds) and its fields.
async fn uncopied_entries(client: &ChClient, db: &str) -> Vec<(u64, i64, Entry)> {
    let zk_path = strings(
        client,
        &format!(
            "SELECT zookeeper_path AS s FROM system.replicas WHERE database = '{db}' \
             AND table = 'spans'"
        ),
    )
    .await[0]
        .clone();
    let pointer = replica_state(client, db, "spans").await.log_pointer;
    strings(
        client,
        &format!(
            "SELECT concat(substring(name, 5), '|', toString(toUnixTimestamp(ctime)), '|', \
             value) AS s FROM system.zookeeper WHERE path = '{zk_path}/log' ORDER BY name"
        ),
    )
    .await
    .iter()
    .filter_map(|row| {
        let mut parts = row.splitn(3, '|');
        let index: u64 = parts.next()?.parse().expect("a log index");
        let ctime: i64 = parts.next()?.parse().expect("a commit time");
        let value = parts.next().unwrap_or_default();
        (index >= pointer).then(|| (index, ctime, Entry::parse(value)))
    })
    .collect()
}

/// Polls until `db.spans`' uncopied entries on this node satisfy `want`,
/// for up to `secs`; the test fails, rather than goes on, if they never do.
async fn wait_uncopied(
    client: &ChClient,
    db: &str,
    what: &str,
    secs: u64,
    want: impl Fn(&[(u64, i64, Entry)]) -> bool,
) -> Vec<(u64, i64, Entry)> {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let entries = uncopied_entries(client, db).await;
        if want(&entries) {
            return entries;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: the uncopied log never reached the state: {} entries, the first {:?}",
            entries.len(),
            entries.first()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// `n` one-row inserts into the node's `spans`, each starting now.
async fn one_row_inserts(client: &ChClient, tag: &str, n: u32) {
    for i in 0..n {
        exec(
            client,
            &format!(
                "INSERT INTO spans (trace_id, span_id, parent_span_id, start_ns, duration_ns, \
                 service, name, kind, attrs, end_ns, service_type) \
                 SELECT sipHash128('{tag}', {i}), reinterpretAsFixedString(cityHash64('{tag}', {i})), \
                        toFixedString('', 8), toInt64(toUnixTimestamp64Nano(now64(9))) AS st, 1000, \
                        'svc-{tag}', 'op', 1, CAST('{{}}', 'JSON'), toUInt64(st + 1000), 'string'"
            ),
        )
        .await;
    }
}

/// The node's totals over `[L, L + 5 min)` are fixture G's answer, read
/// from `service_edges` on that node alone.
async fn assert_totals_g1(client: &ChClient, l: i64, what: &str) {
    let got = strings(
        client,
        &format!(
            "SELECT concat(client, ' ', server, ' ', conn_type, ' ', toString(sum(calls)), ' ', \
                    toString(sum(failed))) AS s \
             FROM service_edges FINAL WHERE conn_type != '' \
             AND minute >= toDateTime({lo}, 'UTC') AND minute < toDateTime({hi}, 'UTC') \
             GROUP BY client, server, conn_type ORDER BY s",
            lo = l / S,
            hi = (l + 5 * MINUTE) / S
        ),
    )
    .await;
    let mut want: Vec<String> = g1_rows()
        .into_iter()
        .map(|(c, s, t, n, f, _)| format!("{c} {s} {t} {n} {f}"))
        .collect();
    want.sort();
    assert_eq!(got, want, "{what}: fixture G's edges in the node's totals");
}

/// KC13: a replica whose copier is behind, with only late insert fetches
/// uncopied, still finalizes the interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replica_copying_its_log_late_still_finalizes() {
    skip_unless_live!();
    let r = Replicas::new("kc13").await;
    let (db, l) = (r.db.clone(), r.l);
    push_g(&r.s1, l, 0..8).await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC13: G on both replicas").await;
    exec(&r.s1b, "SYSTEM STOP PULLING REPLICATION LOG spans").await;
    one_row_inserts(&r.s1, "kc13", 5).await;
    let late = wait_uncopied(&r.s1b, &db, "KC13: five entries uncopied", 120, |e| {
        e.len() >= 5
    })
    .await;
    let state = replica_state(&r.s1b, &db, "spans").await;
    assert!(
        state.log_pointer <= state.log_max_index,
        "KC13: ch-shard1b's log reads not copied: {state:?}"
    );
    refresh_on(&r.s1b, &db, "KC13: ch-shard1b's run").await;
    assert_covers_g(&r.s1b, l, &format!("KC13: the run over {late:?}")).await;
    assert_totals_g1(&r.s1b, l, "KC13").await;
    exec(&r.s1b, "SYSTEM START PULLING REPLICATION LOG spans").await;
    r.drop().await;
}

/// KC14: an uncopied merge does not hold the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_uncopied_merge_does_not_hold_the_run() {
    skip_unless_live!();
    let r = Replicas::new("kc14").await;
    let (db, l) = (r.db.clone(), r.l);
    push_g(&r.s1, l, 0..8).await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC14: G on both replicas").await;
    exec(&r.s1b, "SYSTEM STOP PULLING REPLICATION LOG spans").await;
    exec(&r.s1, "SYSTEM START MERGES spans").await;
    let id = g_partition(&r.s1, &db, "spans").await;
    exec(
        &r.s1,
        &format!("OPTIMIZE TABLE spans PARTITION ID '{id}' FINAL"),
    )
    .await;
    let uncopied = wait_uncopied(&r.s1b, &db, "KC14: the merge uncopied", 120, |e| {
        !e.is_empty() && e.iter().all(|(_, _, x)| x.kind == "merge")
    })
    .await;
    refresh_on(&r.s1b, &db, "KC14: ch-shard1b's run").await;
    assert_covers_g(&r.s1b, l, &format!("KC14: the run over {uncopied:?}")).await;
    assert_totals_g1(&r.s1b, l, "KC14").await;
    exec(&r.s1b, "SYSTEM START PULLING REPLICATION LOG spans").await;
    r.drop().await;
}

/// KC15: an uncopied attach, committed after eligibility, holds the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_uncopied_late_attach_holds_the_run() {
    skip_unless_live!();
    let r = Replicas::new("kc15").await;
    let (db, l) = (r.db.clone(), r.l);
    push_g(&r.s1, l, 0..8).await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC15: G on both replicas").await;
    let id = g_partition(&r.s1, &db, "spans").await;
    exec(
        &r.s1,
        &format!("ALTER TABLE spans DETACH PARTITION ID '{id}'"),
    )
    .await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC15: detached").await;
    exec(&r.s1b, "SYSTEM STOP PULLING REPLICATION LOG spans").await;
    exec(
        &r.s1,
        &format!("ALTER TABLE spans ATTACH PARTITION ID '{id}'"),
    )
    .await;
    let uncopied = wait_uncopied(&r.s1b, &db, "KC15: the attach uncopied", 120, |e| {
        !e.is_empty() && e.iter().all(|(_, _, x)| x.kind == "attach")
    })
    .await;
    refresh_on(&r.s1b, &db, "KC15: ch-shard1b's run").await;
    assert_no_new_marker(&r.s1b, l, &format!("KC15: the run over {uncopied:?}")).await;
    exec(&r.s1b, "SYSTEM START PULLING REPLICATION LOG spans").await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC15: attached").await;
    refresh_until(
        &r.s1b,
        &db,
        l + 4 * MINUTE,
        4,
        "KC15: the run after the copy",
    )
    .await;
    r.assert_route_g1("KC15").await;
    r.drop().await;
}

/// KC16: uncopied entries committed within 10 s of a later interval's
/// eligibility are judged against the eligibility of the interval being
/// finalized, long past, and pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncopied_entries_are_judged_against_the_interval_being_finalized() {
    skip_unless_live!();
    let r = Replicas::new("kc16").await;
    let (db, l) = (r.db.clone(), r.l);
    push_g(&r.s1, l, 0..8).await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC16: G on both replicas").await;
    exec(&r.s1b, "SYSTEM STOP PULLING REPLICATION LOG spans").await;
    // An eligibility: 180 s into an interval, at most 5 min away.
    let deadline = std::time::Instant::now() + Duration::from_secs(330);
    let eligibility = loop {
        // A window of 3 s, not the one second: under load one poll can take
        // longer than a second and step over it.
        let now = server_now_us(&r.s1).await / 1_000_000;
        let into = now.rem_euclid(300);
        if (180..183).contains(&into) {
            break now - (into - 180);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "KC16: no eligibility reached"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    one_row_inserts(&r.s1, "kc16", 5).await;
    let uncopied = wait_uncopied(&r.s1b, &db, "KC16: five entries uncopied", 60, |e| {
        e.len() >= 5
    })
    .await;
    for (index, ctime, e) in &uncopied {
        assert!(
            *ctime < eligibility + 10,
            "KC16: entry {index} ({e:?}) committed at {ctime}, before the eligibility at \
             {eligibility} + 10 s"
        );
    }
    refresh_on(&r.s1b, &db, "KC16: ch-shard1b's run").await;
    let after = server_now_us(&r.s1b).await / 1_000_000;
    assert!(
        after < eligibility + 300,
        "KC16: the run came before the next eligibility: {after}"
    );
    assert_covers_g(&r.s1b, l, "KC16: ch-shard1b's run").await;
    assert_totals_g1(&r.s1b, l, "KC16").await;
    exec(&r.s1b, "SYSTEM START PULLING REPLICATION LOG spans").await;
    r.drop().await;
}

/// KC17: a log with more than 10,000 uncopied entries holds the run; once
/// copied, the interval is finalized.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_log_over_the_cap_holds_the_run() {
    skip_unless_live!();
    let r = Replicas::new("kc17").await;
    let (db, l) = (r.db.clone(), r.l);
    push_g(&r.s1, l, 0..8).await;
    wait_synced(&[&r.s1, &r.s1b], &db, "KC17: G on both replicas").await;
    exec(&r.s1b, "SYSTEM STOP PULLING REPLICATION LOG spans").await;
    // 101 inserts of 100 rows, each row in its own day of the next 100:
    // one part per day per insert.
    for i in 0..101 {
        exec(
            &r.s1,
            &format!(
                "INSERT INTO spans (trace_id, span_id, parent_span_id, start_ns, duration_ns, \
                 service, name, kind, attrs, end_ns, service_type) \
                 SELECT sipHash128('kc17', {i}, number), \
                        reinterpretAsFixedString(cityHash64('kc17', {i}, number)), \
                        toFixedString('', 8), \
                        toInt64(toUnixTimestamp64Nano(now64(9))) + toInt64(number + 1) * 86400000000000 AS st, \
                        1000, 'svc-kc17', 'op', 1, CAST('{{}}', 'JSON'), toUInt64(st + 1000), 'string' \
                 FROM numbers(100)"
            ),
        )
        .await;
    }
    let n = wait_uncopied(&r.s1b, &db, "KC17: over 10,000 uncopied", 120, |e| {
        e.len() > 10_000
    })
    .await
    .len();
    refresh_on(&r.s1b, &db, "KC17: ch-shard1b's run over the cap").await;
    assert_no_new_marker(&r.s1b, l, &format!("KC17: the run over {n} uncopied")).await;
    exec(&r.s1b, "SYSTEM START PULLING REPLICATION LOG spans").await;
    wait_synced(&[&r.s1b], &db, "KC17: copied").await;
    refresh_on(&r.s1b, &db, "KC17: ch-shard1b's run after the copy").await;
    assert_covers_g(&r.s1b, l, "KC17: the run after the copy").await;
    r.drop().await;
}

// =====================================================================
// Issue #601 part 4a: a retried push read once over two shards (T29)
// =====================================================================

/// A 16-byte trace id whose last byte is `n`: `b1` and `b2` hash to
/// different shards.
fn tid4a(n: u8) -> Vec<u8> {
    let mut v = vec![0u8; 16];
    v[15] = n;
    v
}

/// One request under `service`'s resource.
fn service_request(service: &str, spans: Vec<Span>) -> ExportTraceServiceRequest {
    let mut req = request(spans);
    req.resource_spans[0].resource = Some(Resource {
        attributes: vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(service.to_string())),
            }),
            ..KeyValue::default()
        }],
        dropped_attributes_count: 0,
        entity_refs: Vec::new(),
    });
    req
}

/// `traces_query_v2_live.rs`'s fixture Q of part 4a: trace A (`b1`) — `r`
/// the frontend root, `p1` and `s` payment children of `r`, `p2` a payment
/// child of `p1`, `c` a payment child of `p2`, all four in error, 100, 10,
/// 20, 30 and 40 ms — and trace B (`b2`), `b1` a payment root, ok, 50 ms.
fn retried_bodies(base: i64) -> (ExportTraceServiceRequest, ExportTraceServiceRequest) {
    use opentelemetry_proto::tonic::trace::v1::Status;
    let ms = 1_000_000i64;
    let at = base + 1_000 * ms;
    let one =
        |t: u8, sid: u8, parent: Option<u8>, name: &str, start: i64, dur: i64, error: bool| Span {
            trace_id: tid4a(t),
            span_id: id(sid),
            parent_span_id: parent.map(id).unwrap_or_default(),
            name: name.to_string(),
            kind: 2,
            start_time_unix_nano: start as u64,
            end_time_unix_nano: (start + dur) as u64,
            status: error.then(|| Status {
                message: "boom".to_string(),
                code: 2,
            }),
            ..Span::default()
        };
    let mut a = service_request(
        "frontend",
        vec![one(0xb1, 1, None, "r", at, 100 * ms, false)],
    );
    a.resource_spans.extend(
        service_request(
            "payment",
            vec![
                one(0xb1, 2, Some(1), "p1", at + 10 * ms, 10 * ms, true),
                one(0xb1, 3, Some(1), "s", at + 20 * ms, 20 * ms, true),
                one(0xb1, 4, Some(2), "p2", at + 30 * ms, 30 * ms, true),
                one(0xb1, 5, Some(4), "c", at + 40 * ms, 40 * ms, true),
            ],
        )
        .resource_spans,
    );
    let b = service_request(
        "payment",
        vec![one(0xb2, 6, None, "b1", at + 1_000 * ms, 50 * ms, false)],
    );
    (a, b)
}

/// The cluster's `SELECT`s of `db` finished since `t0_ns` and how many ran
/// with `final = 1`, over every replica, read until two reads a second
/// apart agree on at least one.
async fn cluster_final_statements(nodes: &[&ChClient], db: &str, t0_ns: i64) -> (u64, u64) {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone, Copy)]
    struct Logged {
        n: u64,
        f: u64,
    }
    let read = || async {
        for node in nodes {
            exec(node, "SYSTEM FLUSH LOGS").await;
        }
        let sql = format!(
            "SELECT count() AS n, countIf(Settings['final'] = '1') AS f \
             FROM clusterAllReplicas('{CLUSTER_NAME}', system.query_log) \
             WHERE type = 'QueryFinish' AND query_kind = 'Select' AND has(databases, '{db}') \
             AND event_time_microseconds >= toDateTime64({}, 6)",
            t0_ns as f64 / 1e9
        );
        let mut st = nodes[0]
            .query_stream::<Logged>(&sql, &QuerySettings::new())
            .await
            .expect("read the query log");
        st.next().await.expect("one row").expect("decode")
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let a = read().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let b = read().await;
        if a == b && a.n >= 1 {
            return (a.n, a.f);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the query log did not settle within 60 s: {a:?} then {b:?}"
        );
    }
}

/// T29 (issue #601 part 4a): **a retried push stored twice is read once
/// over two shards**, with merges stopped on all three replicas: the
/// search's detail read and fold (T16, T17), a count and a comparison
/// (T22, T25) and the fetch (T26), none of them under `final`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_push_is_counted_once_over_two_shards() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t601p4a_cluster");
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
    let shard1b = ChClient::new(shard1b_config(&db))
        .await
        .expect("connect shard1b");
    let shard2 = ChClient::new(shard2_config(&db))
        .await
        .expect("connect shard2");
    for node in [&shard1, &shard1b, &shard2] {
        exec(node, &format!("SYSTEM STOP MERGES {db}.spans")).await;
    }
    let base = (now_ns() / S - 600) * S;
    let (a, b) = retried_bodies(base);
    land(&shard1, &a, "q0").await;
    land(&shard1, &b, "q1").await;
    land(&shard1, &a, "q2").await;
    for t in ["spans_dist", "traces_dist"] {
        exec(&shard1, &format!("SYSTEM FLUSH DISTRIBUTED {db}.{t}")).await;
    }
    let local = (
        count(&shard1, "SELECT count() AS n FROM spans").await,
        count(&shard2, "SELECT count() AS n FROM spans").await,
    );
    assert!(
        local.0 > 0 && local.1 > 0 && local.0 + local.1 == 11,
        "A twice and B once, split across the shards: {local:?}"
    );
    assert_eq!(
        count(
            &shard1,
            "SELECT uniqExact(trace_id, start_ns, span_id, kind) AS n FROM spans_dist"
        )
        .await,
        6,
        "six span identities"
    );

    let engine = pulsus_read::TraceEngine::new(
        ChClient::new(shard1_config(&db))
            .await
            .expect("connect the engine"),
        config(),
        empty_tenant(),
    );
    let tables = StatementTables {
        spans: "spans_dist",
        error_spans: "error_spans_dist",
        resources: "resources",
        traces: "traces_dist",
        indexed: &[],
        tenant: empty_term(),
    };
    let nodes = [&shard1, &shard1b, &shard2];
    let mut wrong = Vec::new();
    let mut judge = |case: &str, got: String, want: &str, finals: (u64, u64)| {
        eprintln!(
            "{case}: {got}; {} of {} statements with final",
            finals.1, finals.0
        );
        if got != want {
            wrong.push(format!("{case} (a): {got:?}, expected {want:?}"));
        }
        if finals.1 != 0 {
            wrong.push(format!(
                "{case} (b): {} of {} statements ran with final, expected 0",
                finals.1, finals.0
            ));
        }
    };

    for (case, q, want) in [
        (
            "T16",
            r#"{ resource.service.name = "payment" && status = error }"#,
            "b1 4 [02, 03, 04, 05]",
        ),
        (
            "T17a",
            r#"{ resource.service.name = "payment" } | count() > 3"#,
            "b1 4 [02, 03, 04, 05]",
        ),
        (
            "T17b",
            r#"{ resource.service.name = "payment" } | count() > 4"#,
            "",
        ),
    ] {
        let request = plan_search_request(
            &pulsus_traceql::parse(q).expect("parses"),
            &SearchParams {
                start_ns: base,
                end_ns: base + 10 * S,
                limit: 20,
                spss: 10,
            },
            &tables,
            64,
        )
        .expect("plans");
        let t0 = now_ns();
        let out = engine
            .search_routed(&request)
            .await
            .expect("the search answers");
        let got = out
            .traces
            .iter()
            .map(|t| {
                let ids: Vec<String> = t
                    .spans
                    .iter()
                    .map(|s| format!("{:02x}", s.span_id[7]))
                    .collect();
                format!("{:02x} {} [{}]", t.trace_id[15], t.matched, ids.join(", "))
            })
            .collect::<Vec<_>>()
            .join("; ");
        judge(
            case,
            got,
            want,
            cluster_final_statements(&nodes, &db, t0).await,
        );
    }

    let metrics = |q: &str| {
        pulsus_read::plan_trace_metrics_routed(
            &pulsus_traceql::parse(q).expect("parses"),
            &pulsus_read::MetricsParams {
                start_ns: base,
                end_ns: base + 10 * S,
                step_ms: 10_000,
                exemplars: None,
                floor_ns: i64::MIN,
            },
            &engine.metrics_ctx(),
            &tables,
        )
        .expect("planned")
    };
    let sums = |out: &pulsus_read::TraceMetricsResult| {
        let mut m = std::collections::BTreeMap::new();
        for s in &out.series {
            let name = s
                .labels
                .iter()
                .map(|l| match &l.value {
                    pulsus_read::MetricLabelValue::Str(v) => format!("{}={v}", l.key),
                    other => format!("{}={other:?}", l.key),
                })
                .collect::<Vec<_>>()
                .join(",");
            *m.entry(name).or_insert(0.0) += s.samples.iter().map(|(_, v)| *v).sum::<f64>();
        }
        m
    };

    let routed = metrics("{ status = error } | count_over_time()");
    let t0 = now_ns();
    let out = engine
        .metrics_range_routed(&routed)
        .await
        .expect("the count answers");
    let got = sums(&out)
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(" ");
    judge(
        "T22",
        got,
        "__name__=count_over_time=4",
        cluster_final_statements(&nodes, &db, t0).await,
    );

    let routed = metrics("{ } | compare({ status = error })");
    let t0 = now_ns();
    let out = engine
        .metrics_range_routed(&routed)
        .await
        .expect("the comparison answers");
    let s = sums(&out);
    let got = [
        "__meta_type=selection,resource.service.name=payment",
        "__meta_type=selection,rootServiceName=frontend",
    ]
    .iter()
    .map(|k| format!("{k} {}", s.get(*k).copied().unwrap_or(-1.0)))
    .collect::<Vec<_>>()
    .join("; ");
    judge(
        "T25",
        got,
        "__meta_type=selection,resource.service.name=payment 4; \
         __meta_type=selection,rootServiceName=frontend 4",
        cluster_final_statements(&nodes, &db, t0).await,
    );

    let t0 = now_ns();
    let fetched = engine
        .fetch_by_id(&("00".repeat(15) + "b1"), None)
        .await
        .expect("the fetch answers");
    let ids: std::collections::BTreeSet<String> = fetched
        .spans
        .iter()
        .map(|s| format!("{:02x}/{}", s.span_id[7], s.kind))
        .collect();
    judge(
        "T26",
        ids.into_iter().collect::<Vec<_>>().join(" "),
        "01/2 02/2 03/2 04/2 05/2",
        cluster_final_statements(&nodes, &db, t0).await,
    );

    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

// =====================================================================
// Issue #601 part 4b: the bucket catalogue and the error table over two
// shards (T11)
// =====================================================================

/// One span of trace `t` for part 4b: kind, duration in ms, error or not,
/// and an `http.route` when given.
#[allow(clippy::too_many_arguments)]
fn span_4b(
    t: u8,
    sid: u16,
    parent: Option<u16>,
    name: &str,
    kind: i32,
    at: i64,
    dur_ms: i64,
    error: bool,
    route: Option<&str>,
) -> Span {
    use opentelemetry_proto::tonic::trace::v1::Status;
    let sid_bytes = |s: u16| {
        let mut v = vec![0u8; 8];
        v[0] = 0x4b;
        v[6..8].copy_from_slice(&s.to_be_bytes());
        v
    };
    Span {
        trace_id: tid4a(t),
        span_id: sid_bytes(sid),
        parent_span_id: parent.map(sid_bytes).unwrap_or_default(),
        name: name.to_string(),
        kind,
        start_time_unix_nano: at as u64,
        end_time_unix_nano: (at + dur_ms * 1_000_000) as u64,
        attributes: route
            .map(|r| {
                vec![KeyValue {
                    key: "http.route".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(r.to_string())),
                    }),
                    ..KeyValue::default()
                }]
            })
            .unwrap_or_default(),
        status: error.then(|| Status {
            message: "boom".to_string(),
            code: 2,
        }),
        ..Span::default()
    }
}

/// One finished statement of the engine, on the node that ran it.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct Stmt4b {
    tables: Vec<String>,
    read_rows: u64,
}

/// The statements whose `query_id` starts with `prefix`, as the initiating
/// node logged them, read until two reads a second apart agree on one.
async fn statements_4b(client: &ChClient, prefix: &str) -> Vec<Stmt4b> {
    let read = || async {
        exec(client, "SYSTEM FLUSH LOGS").await;
        let sql = format!(
            "SELECT tables, read_rows FROM system.query_log \
             WHERE type = 'QueryFinish' AND is_initial_query AND startsWith(query_id, '{prefix}-') \
             ORDER BY event_time_microseconds"
        );
        let mut st = client
            .query_stream::<Stmt4b>(&sql, &QuerySettings::new())
            .await
            .expect("read the query log");
        let mut out = Vec::new();
        while let Some(row) = st.next().await {
            out.push(row.expect("decode"));
        }
        out
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let a = read().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let b = read().await;
        if a == b && !a.is_empty() {
            return a;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the query log did not settle: {a:?} then {b:?}"
        );
    }
}

/// The engine on shard 1 whose error table is `errors`, stamping its
/// statements with `prefix` when given.
async fn engine_4b(db: &str, prefix: Option<String>, errors: &str) -> pulsus_read::TraceEngine {
    let engine = pulsus_read::TraceEngine::new(
        ChClient::new(shard1_config(db))
            .await
            .expect("connect the engine"),
        pulsus_read::TraceReadConfig {
            error_spans_table: errors.to_string(),
            ..config()
        },
        empty_tenant(),
    );
    match prefix {
        Some(p) => engine.with_statement_prefix(&p),
        None => engine,
    }
}

fn read_table_4b(stmts: &[Stmt4b], db: &str, table: &str) -> bool {
    let name = format!("{db}.{table}");
    stmts.iter().any(|s| s.tables.contains(&name))
}

/// T11 (issue #601 part 4b): **the catalogue and the error table answer
/// over two shards.** Part 4b's fixtures C4 and E land through one node and
/// split across the shards: the service-narrowed values read answers in one
/// statement from `tag_bucket_values` and `spans_dist`, reading fewer rows
/// than the window holds; the admitted metrics and search reads answer as
/// the same reads forced to `spans_dist`, from `error_spans_dist` and not
/// `spans_dist`; and `tag_bucket_values` holds the same groups on all three
/// replicas.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_catalogue_and_the_error_table_answer_over_two_shards() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("pulsus_read_it_t601p4b_cluster");
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
    let shard1 = ChClient::new(shard1_config(&db)).await.expect("shard1");
    let shard1b = ChClient::new(shard1b_config(&db)).await.expect("shard1b");
    let shard2 = ChClient::new(shard2_config(&db)).await.expect("shard2");
    exec(
        &shard1,
        &format!("SYSTEM STOP MERGES ON CLUSTER '{CLUSTER_NAME}' {db}.spans"),
    )
    .await;

    // Fixture C4: `checkout` and `other` across three buckets, each bucket
    // and service its own push, the inner bucket's 400 spans over 40 traces.
    let b0 = now_ns() / BUCKET - 8;
    let at = |b: i64, s: i64| (b0 + b) * BUCKET + s * S;
    let route = |t: u8, sid: u16, when: i64, r: &str| {
        span_4b(t, sid, None, "op", 2, when, 1, false, Some(r))
    };
    let mut pushes = vec![
        service_request(
            "checkout",
            vec![
                route(0xc1, 1, at(0, 10), "/early"),
                route(0xc1, 2, at(0, 200), "/a"),
            ],
        ),
        service_request("other", vec![route(0xc2, 3, at(0, 200), "/other")]),
        service_request(
            "checkout",
            (0..400u16)
                .map(|i| {
                    route(
                        0xd0 + (i % 40) as u8,
                        100 + i,
                        at(1, 10) + i64::from(i) * 100_000_000,
                        &format!("/r{}", i % 5),
                    )
                })
                .chain([route(0xc1, 99, at(1, 200), "/b")])
                .collect(),
        ),
        service_request(
            "other",
            (0..10u16)
                .map(|i| route(0xc2, 600 + i, at(1, 20 + i64::from(i)), "/x"))
                .collect(),
        ),
        service_request(
            "checkout",
            vec![
                route(0xc1, 4, at(2, 100), "/c"),
                route(0xc1, 5, at(2, 200), "/late"),
            ],
        ),
    ];
    // Fixture E, in the ten seconds from `base`.
    let base = (now_ns() / S - 600) * S;
    let ms = 1_000_000i64;
    let e = |t: u8,
             sid: u16,
             parent: Option<u16>,
             name: &str,
             kind: i32,
             off: i64,
             dur: i64,
             err: bool| {
        span_4b(t, sid, parent, name, kind, base + off * ms, dur, err, None)
    };
    pushes.extend([
        service_request(
            "frontend",
            vec![e(0xe1, 1, None, "GET /", 2, 1_000, 100, false)],
        ),
        service_request(
            "payment",
            vec![
                e(0xe1, 2, Some(1), "charge", 3, 1_010, 20, true),
                e(0xe1, 3, Some(1), "db", 1, 1_020, 5, true),
                e(0xe2, 4, None, "pay", 2, 3_000, 50, true),
                e(0xe2, 5, Some(4), "pay2", 2, 3_010, 30, true),
            ],
        ),
        service_request(
            "checkout",
            vec![
                e(0xe2, 6, Some(4), "cart", 3, 3_020, 10, false),
                e(0xe3, 8, Some(7), "x", 2, 5_010, 30, true),
            ],
        ),
        service_request(
            "frontend",
            vec![e(0xe3, 7, None, "home", 2, 5_000, 80, false)],
        ),
    ]);
    for (i, req) in pushes.iter().enumerate() {
        land(&shard1, req, &format!("t601p4b-cluster-{i}")).await;
    }
    for t in ["spans_dist", "traces_dist"] {
        exec(&shard1, &format!("SYSTEM FLUSH DISTRIBUTED {db}.{t}")).await;
    }
    let flushed = shard1
        .execute(
            &format!("SYSTEM FLUSH DISTRIBUTED {db}.error_spans_dist"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .map_err(|e| e.to_string());
    let mut local = Vec::new();
    for node in [&shard1, &shard2] {
        local.push(
            node.query_strings(
                "SELECT toString(count()) AS s FROM error_spans",
                &QuerySettings::new(),
            )
            .await
            .map_err(|e| e.to_string()),
        );
    }
    let ok = |r: &Result<Vec<String>, String>| -> u64 {
        r.as_ref()
            .ok()
            .and_then(|v| v.first())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0)
    };
    assert!(
        flushed.is_ok()
            && ok(&local[0]) > 0
            && ok(&local[1]) > 0
            && ok(&local[0]) + ok(&local[1]) == 5,
        "the five error spans split across the shards: {flushed:?} {local:?}"
    );

    let mut wrong = Vec::new();

    // T4 through the cluster.
    let (s, e_ns) = (b0 * BUCKET + BUCKET / 2, (b0 + 2) * BUCKET + BUCKET / 2);
    let prefix = pulsus_testkit::test_ident(&format!("t601p4b_c4_{}", now_ns()));
    let values = engine_4b(&db, Some(prefix.clone()), "error_spans_dist")
        .await
        .list_tag_values(
            "http.route",
            Some("span"),
            pulsus_read::TagValuesRequest {
                q: Some(r#"{ resource.service.name = "checkout" }"#),
                start_ns: s,
                end_ns: e_ns,
                floor_ns: i64::MIN,
            },
        )
        .await
        .map(|v| {
            v.values
                .iter()
                .map(|v| v.val.clone())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .map_err(|e| e.to_string());
    let stmts = statements_4b(&shard1, &prefix).await;
    let in_window = count(
        &shard1,
        &format!("SELECT count() AS n FROM spans_dist WHERE start_ns >= {s} AND start_ns < {e_ns}"),
    )
    .await;
    eprintln!("T4: {values:?} in {stmts:?}; {in_window} spans in the window");
    if values != Ok("/a /b /c /r0 /r1 /r2 /r3 /r4".to_string()) {
        wrong.push(format!("T4: {values:?}"));
    }
    if stmts.len() != 1
        || !read_table_4b(&stmts, &db, "tag_bucket_values")
        || stmts[0].read_rows >= in_window
    {
        wrong.push(format!(
            "T4: expected one statement reading the catalogue and fewer than {in_window} rows: {stmts:?}"
        ));
    }

    // T7 through the cluster.
    let tables = |errors: &'static str| StatementTables {
        spans: "spans_dist",
        error_spans: errors,
        resources: "resources",
        traces: "traces_dist",
        indexed: &[],
        tenant: empty_term(),
    };
    let base_s = base / S;
    for (q, metrics) in [
        (
            "{ status = error } | count_over_time() by(resource.service.name)",
            true,
        ),
        ("{ status = error } | rate()", true),
        (
            "{ status = error } | quantile_over_time(duration, .5) by(name)",
            true,
        ),
        (
            r#"{ resource.service.name = "payment" && status = error }"#,
            false,
        ),
        ("{ status = error && kind = server } | count() > 1", false),
    ] {
        let query = pulsus_traceql::parse(q).expect("parses");
        let prefix = pulsus_testkit::test_ident(&format!("t601p4b_c7_{}", now_ns()));
        let mut answers = Vec::new();
        for (p, errors) in [
            (Some(prefix.clone()), "error_spans_dist"),
            (None, "spans_dist"),
        ] {
            let engine = engine_4b(&db, p, errors).await;
            let text = if metrics {
                let routed = pulsus_read::plan_trace_metrics_routed(
                    &query,
                    &pulsus_read::MetricsParams {
                        start_ns: base_s * S,
                        end_ns: (base_s + 10) * S,
                        step_ms: 1_000,
                        exemplars: None,
                        floor_ns: i64::MIN,
                    },
                    &engine.metrics_ctx(),
                    &tables(if errors == "spans_dist" {
                        "spans_dist"
                    } else {
                        "error_spans_dist"
                    }),
                )
                .expect("plans");
                match engine.metrics_range_routed(&routed).await {
                    Err(e) => format!("Err({e})"),
                    Ok(out) => {
                        let mut series: Vec<String> = out
                            .series
                            .iter()
                            .map(|s| format!("{:?} {:?}", s.labels, s.samples))
                            .collect();
                        series.sort();
                        series.join("; ")
                    }
                }
            } else {
                let plan = plan_search_request(
                    &query,
                    &SearchParams {
                        start_ns: base,
                        end_ns: base + 10 * S,
                        limit: 20,
                        spss: 10,
                    },
                    &tables("error_spans_dist"),
                    64,
                )
                .expect("plans");
                match engine.search_routed(&plan).await {
                    Err(e) => format!("Err({e})"),
                    Ok(out) => out
                        .traces
                        .iter()
                        .map(|t| {
                            format!(
                                "{:02x} {} [{}]",
                                t.trace_id[15],
                                t.matched,
                                t.spans
                                    .iter()
                                    .map(|s| format!("{:02x}", s.span_id[7]))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("; "),
                }
            };
            answers.push(text);
        }
        let stmts = statements_4b(&shard1, &prefix).await;
        eprintln!("T7 {q}: {} | forced {} | {stmts:?}", answers[0], answers[1]);
        if answers[0] != answers[1] || answers[0].is_empty() || answers[0].starts_with("Err") {
            wrong.push(format!("T7 {q}: {} against {}", answers[0], answers[1]));
        }
        if !read_table_4b(&stmts, &db, "error_spans_dist")
            || read_table_4b(&stmts, &db, "spans_dist")
        {
            wrong.push(format!(
                "T7 {q}: expected error_spans_dist and not spans_dist: {stmts:?}"
            ));
        }
    }

    // The catalogue is one replica set: every node holds the same groups.
    let groups = "SELECT concat(service, ' ', key, ' ', toString(bucket), ' ', \
                  arrayStringConcat(arrayMap(x -> x.1, arraySort(groupUniqArrayMerge(101)(vals))), ',')) \
                  AS s FROM tag_bucket_values GROUP BY service, key, bucket ORDER BY service, key, bucket";
    let mut held = Vec::new();
    for node in [&shard1, &shard1b, &shard2] {
        let synced = node
            .execute(
                &format!("SYSTEM SYNC REPLICA {db}.tag_bucket_values"),
                &QuerySettings::new(),
                Idempotency::NonIdempotent,
            )
            .await;
        let rows = node.query_strings(groups, &QuerySettings::new()).await;
        held.push(format!("{synced:?} {rows:?}"));
    }
    eprintln!("tag_bucket_values per node: {held:?}");
    if held[0] != held[1] || held[0] != held[2] || !held[0].contains("checkout") {
        wrong.push(format!("the three replicas differ: {held:?}"));
    }

    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

// =====================================================================
// Issue #601 part 4c: a resource label and the resource catalogue over
// two shards (T5, T11)
// =====================================================================

fn kv_4c(key: &str, value: any_value::Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        ..KeyValue::default()
    }
}

/// One request under a resource holding `attributes`, its spans each
/// carrying `span_attrs`.
fn resource_request_4c(
    attributes: Vec<KeyValue>,
    spans: Vec<Span>,
    span_attrs: &[KeyValue],
) -> ExportTraceServiceRequest {
    let mut req = request(
        spans
            .into_iter()
            .map(|mut s| {
                s.attributes = span_attrs.to_vec();
                s
            })
            .collect(),
    );
    req.resource_spans[0].resource = Some(Resource {
        attributes,
        dropped_attributes_count: 0,
        entity_refs: Vec::new(),
    });
    req
}

/// A fresh clustered database, merges stopped on `spans`, and its three
/// nodes.
async fn cluster_db_4c(test: &str) -> (String, ChClient, [ChClient; 3]) {
    let db = pulsus_testkit::test_db(&format!("pulsus_read_it_t601p4c_{test}"));
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
    let shard1 = ChClient::new(shard1_config(&db)).await.expect("shard1");
    let shard1b = ChClient::new(shard1b_config(&db)).await.expect("shard1b");
    let shard2 = ChClient::new(shard2_config(&db)).await.expect("shard2");
    exec(
        &shard1,
        &format!("SYSTEM STOP MERGES ON CLUSTER '{CLUSTER_NAME}' {db}.spans"),
    )
    .await;
    (db, bootstrap, [shard1, shard1b, shard2])
}

/// T5 (issue #601 part 4c), a guard: **a resource key labels alike over two
/// shards.** `traces_query_v2_live.rs`'s fixture R through one node, its
/// eight traces split across the shards: the scoped and the unscoped pod
/// name label as that file's T4 writes by hand.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resource_key_labels_alike_over_two_shards() {
    skip_unless_live!();
    use any_value::Value as V;
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList};
    let (db, bootstrap, [shard1, _, shard2]) = cluster_db_4c("t5").await;
    let b = (now_ns() / S / 60 - 5) * 60;
    let str_v = |s: &str| V::StringValue(s.to_string());
    let pods: Vec<(Option<V>, Vec<KeyValue>)> = vec![
        (Some(str_v("p1")), Vec::new()),
        (Some(V::IntValue(7)), Vec::new()),
        (Some(V::DoubleValue(1e21)), Vec::new()),
        (Some(V::BoolValue(true)), Vec::new()),
        (
            Some(V::ArrayValue(ArrayValue {
                values: ["a", "b"]
                    .iter()
                    .map(|v| AnyValue {
                        value: Some(str_v(v)),
                    })
                    .collect(),
            })),
            Vec::new(),
        ),
        (
            Some(V::KvlistValue(KeyValueList {
                values: vec![kv_4c("x", str_v("y"))],
            })),
            Vec::new(),
        ),
        (None, Vec::new()),
        (None, vec![kv_4c("k8s.pod.name", str_v("s9"))]),
    ];
    let mut push = ExportTraceServiceRequest {
        resource_spans: Vec::new(),
    };
    for (i, (pod, span_attrs)) in pods.into_iter().enumerate() {
        let i = u8::try_from(i).expect("eight resources");
        let mut attributes = vec![
            kv_4c("service.name", str_v("r")),
            kv_4c("fixture.i", V::IntValue(i64::from(i))),
        ];
        if let Some(pod) = pod {
            attributes.push(kv_4c("k8s.pod.name", pod));
        }
        let at = (b + 10) * S + i64::from(i) * 2 * S;
        let spans = (0..2u8)
            .map(|j| {
                let mut sp = span(0xe0 + i, 2 * i + j + 1, 0, at + i64::from(j) * S);
                sp.trace_id = tid4a(0xe0 + i);
                sp
            })
            .collect();
        push.resource_spans
            .extend(resource_request_4c(attributes, spans, &span_attrs).resource_spans);
    }
    land(&shard1, &push, "t601p4c-t5").await;
    exec(
        &shard1,
        &format!("SYSTEM FLUSH DISTRIBUTED {db}.spans_dist"),
    )
    .await;
    let local = (
        count(&shard1, "SELECT count() AS n FROM spans").await,
        count(&shard2, "SELECT count() AS n FROM spans").await,
    );
    assert!(
        local.0 > 0 && local.1 > 0 && local.0 + local.1 == 16,
        "sixteen spans split across the shards: {local:?}"
    );

    let engine = pulsus_read::TraceEngine::new(
        ChClient::new(shard1_config(&db))
            .await
            .expect("connect the engine"),
        config(),
        empty_tenant(),
    );
    let tables = StatementTables {
        spans: "spans_dist",
        error_spans: "error_spans_dist",
        resources: "resources",
        traces: "traces_dist",
        indexed: &[],
        tenant: empty_term(),
    };
    let mut answers = Vec::new();
    for key in ["resource.k8s.pod.name", ".k8s.pod.name"] {
        let q = format!("{{ }} | count_over_time() by ({key})");
        let routed = pulsus_read::plan_trace_metrics_routed(
            &pulsus_traceql::parse(&q).expect("parses"),
            &pulsus_read::MetricsParams {
                start_ns: b * S,
                end_ns: (b + 60) * S,
                step_ms: 60_000,
                exemplars: None,
                floor_ns: i64::MIN,
            },
            &engine.metrics_ctx(),
            &tables,
        )
        .expect("plans");
        let text = match engine.metrics_instant_routed(&routed).await {
            Err(e) => vec![format!("Err({e})")],
            Ok(out) => {
                let mut v: Vec<String> = out
                    .series
                    .iter()
                    .map(|s| {
                        let labels: Vec<String> = s
                            .labels
                            .iter()
                            .map(|l| format!("{}={:?}", l.key, l.value))
                            .collect();
                        let total: f64 = s.samples.iter().map(|(_, v)| v).sum();
                        format!("{} {total}", labels.join(","))
                    })
                    .collect();
                v.sort();
                v
            }
        };
        answers.push(text);
    }
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    let want = |k: &str, own: &[(&str, f64)]| -> Vec<String> {
        let mut v: Vec<String> = [
            ("Str(\"p1\")", 2.0),
            ("Int(7)", 2.0),
            ("Double(1e21)", 2.0),
            ("Bool(true)", 2.0),
            ("Str(\"nil\")", 2.0),
        ]
        .iter()
        .chain(own)
        .map(|(v, n)| format!("{k}={v} {n}"))
        .collect();
        v.sort();
        v
    };
    assert_eq!(
        answers[0],
        want("resource.k8s.pod.name", &[("Str(\"nil\")", 6.0)]),
        "scoped"
    );
    assert_eq!(
        answers[1],
        want(
            ".k8s.pod.name",
            &[("Str(\"s9\")", 2.0), ("Str(\"nil\")", 4.0)]
        ),
        "unscoped"
    );
}

/// T11 (issue #601 part 4c): **the resource catalogue answers over two
/// shards.** `traces_query_v2_live.rs`'s fixture B through one node: the
/// service-narrowed resource values read lists `ra1` and `ra2` in one
/// statement reading `service_bucket_resources`, and the table holds the
/// same five rows on all three replicas.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_resource_catalogue_answers_over_two_shards() {
    skip_unless_live!();
    use any_value::Value as V;
    let (db, bootstrap, nodes) = cluster_db_4c("t11").await;
    let k0 = now_ns() / BUCKET - 8;
    let at = |s: i64| k0 * BUCKET + s * S;
    let resource = |service: &str, pod: &str| {
        vec![
            kv_4c("service.name", V::StringValue(service.to_string())),
            kv_4c("k8s.pod.name", V::StringValue(pod.to_string())),
        ]
    };
    let one = |service: &str, pod: &str, sid: u16, s: i64| {
        resource_request_4c(
            resource(service, pod),
            vec![span_4b(
                0xb0 + (sid as u8),
                sid,
                None,
                "op",
                2,
                at(s),
                1,
                false,
                None,
            )],
            &[],
        )
    };
    let mut first = one("a", "ra2", 1, 150);
    first
        .resource_spans
        .extend(one("a", "ra3", 2, 50).resource_spans);
    let mut second = resource_request_4c(
        resource("a", "ra1"),
        (0..3_000u16)
            .map(|i| {
                span_4b(
                    0xd0 + (i % 40) as u8,
                    1_000 + i,
                    None,
                    "op",
                    2,
                    at(400),
                    1,
                    false,
                    None,
                )
            })
            .collect(),
        &[],
    );
    second
        .resource_spans
        .extend(one("b", "rb1", 3, 450).resource_spans);
    for (i, req) in [first, second, one("a", "ra4", 4, 1_050)]
        .iter()
        .enumerate()
    {
        land(&nodes[0], req, &format!("t601p4c-t11-{i}")).await;
    }
    exec(
        &nodes[0],
        &format!("SYSTEM FLUSH DISTRIBUTED {db}.spans_dist"),
    )
    .await;

    let mut wrong = Vec::new();
    let (s, e) = (at(100), at(1_000));
    let prefix = pulsus_testkit::test_ident(&format!("t601p4c_t11_{}", now_ns()));
    let values = engine_4b(&db, Some(prefix.clone()), "error_spans_dist")
        .await
        .list_tag_values(
            "k8s.pod.name",
            Some("resource"),
            pulsus_read::TagValuesRequest {
                q: Some(r#"{ resource.service.name = "a" }"#),
                start_ns: s,
                end_ns: e,
                floor_ns: i64::MIN,
            },
        )
        .await
        .map(|v| {
            v.values
                .iter()
                .map(|v| format!("{}/{}", v.val, v.val_type))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .map_err(|e| e.to_string());
    let stmts = statements_4b(&nodes[0], &prefix).await;
    eprintln!("T11: {values:?} in {stmts:?}");
    if values != Ok("ra1/string ra2/string".to_string()) {
        wrong.push(format!("T8 over the cluster: {values:?}"));
    }
    if stmts.len() != 1 || !read_table_4b(&stmts, &db, "service_bucket_resources") {
        wrong.push(format!(
            "expected one statement reading the catalogue: {stmts:?}"
        ));
    }

    let rows = format!(
        "SELECT concat(toString(bucket - {k0}), ' ', service, ' ', hex(resource_id)) AS s \
         FROM service_bucket_resources ORDER BY s"
    );
    let mut held = Vec::new();
    for node in &nodes {
        let synced = node
            .execute(
                &format!("SYSTEM SYNC REPLICA {db}.service_bucket_resources"),
                &QuerySettings::new(),
                Idempotency::NonIdempotent,
            )
            .await
            .map_err(|e| e.to_string());
        held.push((
            synced,
            node.query_strings(&rows, &QuerySettings::new())
                .await
                .map_err(|e| e.to_string()),
        ));
    }
    eprintln!("service_bucket_resources per node: {held:?}");
    let five = |h: &(Result<(), String>, Result<Vec<String>, String>)| {
        h.0.is_ok() && h.1.as_ref().is_ok_and(|r| r.len() == 5)
    };
    if !held.iter().all(five) || held[0] != held[1] || held[0] != held[2] {
        wrong.push(format!(
            "the three replicas do not hold the same five rows: {held:?}"
        ));
    }

    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// **A retry after a server-declared failure is stored once the fault
/// clears, on the cluster** (issue #602). The landing insert goes to shard 1
/// under its bare name; a view there throws, so the insert fails after the
/// block was sent. The trace claim is replay-safe, so the same push sent
/// again through the same writer is a new block, and once the view is gone
/// every span and trace is readable through the distributed tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_after_a_throwing_view_is_stored_once_the_fault_clears_on_the_cluster() {
    use pulsus_write::{PushHeaders, TraceSink};
    skip_unless_live!();
    let db = pulsus_testkit::test_db(&format!("pulsus_read_it_retry_throw_{}", now_ns()));
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
    exec(
        &s1,
        "CREATE TABLE zz_fault (x UInt8) ENGINE = MergeTree ORDER BY x",
    )
    .await;
    exec(
        &s1,
        "CREATE MATERIALIZED VIEW zz_fault_mv TO zz_fault AS \
         SELECT throwIf(row_kind = 0, 'injected fault') AS x FROM trace_landing",
    )
    .await;

    let writer = node_writer(shard1_config(&db)).await;
    let req = load_push(0, 3);
    let landing = || pulsus_write::parse_trace_landing(&req).expect("the landing decode");
    let first = writer
        .admit_flush(&empty_tenant(), landing(), PushHeaders::default())
        .expect("admitted")
        .await;
    assert!(
        first.is_err(),
        "the throwing view fails the push: {first:?}"
    );

    exec(&s1, "DROP VIEW zz_fault_mv").await;
    let second = writer
        .admit_flush(&empty_tenant(), landing(), PushHeaders::default())
        .expect("the retry is admitted, not suppressed")
        .await;
    assert!(
        second.is_ok(),
        "the retry commits once the fault is gone: {second:?}"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let spans = count(
            &s1,
            "SELECT count() AS n FROM (SELECT DISTINCT trace_id, span_id, kind FROM spans_dist)",
        )
        .await;
        let traces = count(
            &s1,
            "SELECT count() AS n FROM (SELECT DISTINCT trace_id FROM traces_dist)",
        )
        .await;
        if (spans, traces) == (6, 3) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the retried push is readable: {spans} spans, {traces} traces"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    writer.shutdown(Duration::from_secs(5)).await;
    exec(
        &bootstrap,
        &format!("DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER_NAME}' SYNC"),
    )
    .await;
}

/// The empty tenant (issue #738): every fixture here is pushed with no
/// `X-Scope-OrgID`.
#[allow(dead_code)]
fn empty_tenant() -> pulsus_model::Tenant {
    pulsus_model::Tenant::from_header(None, false).expect("the empty tenant")
}

/// [`empty_tenant`]'s term, for a statement built outside an engine.
#[allow(dead_code)]
fn empty_term() -> &'static pulsus_read::traces::tenant_scope::TenantTerm {
    static TERM: std::sync::LazyLock<pulsus_read::traces::tenant_scope::TenantTerm> =
        std::sync::LazyLock::new(|| {
            pulsus_read::traces::tenant_scope::TenantTerm::of(&empty_tenant())
        });
    &TERM
}
