//! `xtask traceql-catalogue` (issue #600): every catalogue query through the
//! shipped planner and engine, one JSON line per query.
//!
//! The queries are `<corpus>/accept/*.traceql`, then `<corpus>/grafana/`,
//! each sorted by name, then every row of `catalogue-extra.tsv`. Each one is
//! parsed, validated, planned and run as the search and metrics routes do,
//! against a database holding the catalogue fixture
//! (`docs/TraceQL/measure/catalogue.sh`). The line carries the disposition,
//! the answer as the response layer renders it, typed, and the statements
//! each request issued with the rows each returned, read from
//! `system.query_log` through the request's own statement prefix.
//! `docs/TraceQL/measure/catalogue.py` compares the answers with the
//! interpreter.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChPool, ChProto, Idempotency, QuerySettings};
use pulsus_read::traces::spans::attr_index::IndexedAttr;
use pulsus_read::{
    GroupValue, MetricLabel, MetricLabelValue, MetricsCtx, MetricsParams, SearchOutput,
    SearchParams, SpanFilterCtx, SpanSummary, StatementTables, TraceEngine, TraceMetricsResult,
    TraceReadConfig, plan_search_request, plan_trace_metrics_routed,
};
use pulsus_traceql::PipelineStage;
use serde_json::{Value, json};

/// The command line.
#[derive(clap::Parser)]
pub struct Args {
    /// The ClickHouse server's host.
    #[arg(long)]
    host: String,
    /// Its HTTP port.
    #[arg(long)]
    port: u16,
    /// The database holding the fixture.
    #[arg(long)]
    database: String,
    /// `crates/pulsus-traceql/tests/corpus`.
    #[arg(long)]
    corpus: String,
    /// `docs/TraceQL/measure/catalogue-extra.tsv`.
    #[arg(long)]
    extra: String,
    /// The window's start, inclusive, in Unix nanoseconds.
    #[arg(long)]
    start_ns: i64,
    /// The window's end, exclusive, in Unix nanoseconds.
    #[arg(long)]
    end_ns: i64,
    /// The cluster the schema was built for (`PULSUS_CLUSTER`): the run reads
    /// through the `_dist` tables, as the server does. Absent on one node.
    #[arg(long)]
    cluster: Option<String>,
}

/// The most statements one request may issue (`T-Q2`).
const MAX_STATEMENTS: usize = 100;

/// The client's timeout.
const TIMEOUT: Duration = Duration::from_secs(60);

/// The search requests, each `(name, limit, spss)`: the answer, a top-K cut
/// that decides something on seven traces, and every matching span.
const SEARCH_REQUESTS: [(&str, u32, u32); 3] =
    [("answer", 20, 3), ("cut", 2, 3), ("full", 20, 1000)];

/// The metrics step.
const STEP_MS: i64 = 60_000;

/// One catalogue query: its name, its group and its text.
struct Case {
    name: String,
    group: String,
    traceql: String,
}

/// The corpus's two accepted directories, each sorted by name, then each
/// non-comment row of the extra table.
fn cases(corpus: &str, extra: &str) -> anyhow::Result<Vec<Case>> {
    let mut out = Vec::new();
    for group in ["accept", "grafana"] {
        let dir = format!("{corpus}/{group}");
        let mut files = std::fs::read_dir(&dir)
            .with_context(|| format!("read {dir}"))?
            .map(|e| e.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;
        files.retain(|p| p.extension().is_some_and(|x| x == "traceql"));
        files.sort();
        for path in files {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("read {}", path.display()))?;
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .context("a corpus file name")?;
            out.push(Case {
                name: name.to_string(),
                group: group.to_string(),
                // `pulsus-traceql/tests/corpus.rs`: the file format ends
                // every input in one `\n`, which is not part of the query.
                traceql: raw.strip_suffix('\n').unwrap_or(&raw).to_string(),
            });
        }
    }
    let table = std::fs::read_to_string(extra).with_context(|| format!("read {extra}"))?;
    for line in table.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let mut cols = line.split('\t');
        let (Some(name), Some(traceql)) = (cols.next(), cols.next()) else {
            anyhow::bail!("{extra}: a row without a query: {line:?}");
        };
        out.push(Case {
            name: name.to_string(),
            group: "design".to_string(),
            traceql: traceql.to_string(),
        });
    }
    Ok(out)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A number as JSON, a non-finite one as its `to_string()`.
fn num(f: f64) -> Value {
    if f.is_finite() {
        json!(f)
    } else {
        json!(f.to_string())
    }
}

/// A typed response value as the renderer writes it: `[arm, text]` from
/// `wire_arm`, so a missing key is `["stringValue", "nil"]`.
fn gv(v: &GroupValue) -> Value {
    let (arm, text) = pulsus_read::wire_arm(v);
    json!([arm, text])
}

fn span(s: &SpanSummary) -> Value {
    let attrs: Vec<Value> = s
        .attributes
        .iter()
        .map(|a| json!([a.key(), gv(a.value())]))
        .collect();
    json!({
        "id": hex(&s.span_id),
        "start": s.start_ns,
        "dur": s.duration_ns,
        "name": s.name(),
        "attrs": attrs,
    })
}

fn search_json(o: &SearchOutput) -> Value {
    let traces: Vec<Value> = o
        .traces
        .iter()
        .map(|t| {
            let groups = t.groups.as_ref().map(|gs| {
                gs.iter()
                    .map(|g| {
                        let attrs: Vec<Value> = g
                            .attributes
                            .iter()
                            .map(|(k, v)| json!([k, gv(v)]))
                            .collect();
                        let spans: Vec<Value> = g.spans.iter().map(span).collect();
                        json!({"attrs": attrs, "matched": g.matched, "spans": spans})
                    })
                    .collect::<Vec<_>>()
            });
            let spans: Vec<Value> = t.spans.iter().map(span).collect();
            json!({
                "id": hex(&t.trace_id),
                "root_service": t.root.service,
                "root_name": t.root.name,
                "start": t.trace_start_ns,
                "dur": t.trace_duration_ns,
                "matched": t.matched,
                "spans": spans,
                "groups": groups,
            })
        })
        .collect();
    json!({"partial": o.partial, "traces": traces})
}

fn labels(ls: &[MetricLabel]) -> Value {
    ls.iter()
        .map(|l| {
            let v = match &l.value {
                MetricLabelValue::Str(s) => json!(["string", s]),
                MetricLabelValue::Double(f) => json!(["double", num(*f)]),
                MetricLabelValue::Int(i) => json!(["int", i]),
                MetricLabelValue::Bool(b) => json!(["bool", b]),
            };
            json!([l.key, v])
        })
        .collect()
}

fn metrics_json(r: &TraceMetricsResult) -> Value {
    let series: Vec<Value> = r
        .series
        .iter()
        .map(|s| {
            let samples: Vec<Value> = s.samples.iter().map(|(t, v)| json!([t, num(*v)])).collect();
            let exemplars: Vec<Value> = s
                .exemplars
                .iter()
                .map(|e| json!({"labels": labels(&e.labels), "value": num(e.value), "ts": e.timestamp_ms}))
                .collect();
            json!({"labels": labels(&s.labels), "samples": samples, "exemplars": exemplars})
        })
        .collect();
    json!({ "series": series })
}

/// The first `MAX_STATEMENTS` of a request's statements, and whether the
/// log held more: the read asks for one row past the ceiling.
fn cap_statements(mut rows: Vec<String>) -> (Vec<String>, bool) {
    let over = rows.len() > MAX_STATEMENTS;
    rows.truncate(MAX_STATEMENTS);
    (rows, over)
}

/// A request's statements: the first `MAX_STATEMENTS + 1` rows the log
/// holds under its prefix, the last of them the sentinel.
fn statements_sql(prefix: &str) -> String {
    format!(
        "SELECT toJSONString((query_id, result_rows, read_rows, query)) AS s \
         FROM system.query_log \
         WHERE log_comment = '{prefix}' AND type = 'QueryFinish' AND is_initial_query \
         ORDER BY event_time_microseconds \
         LIMIT {}",
        MAX_STATEMENTS + 1
    )
}

/// A query refused before it ran, with the error's `Display`.
fn refused(line: &mut Value, disposition: &str, message: String) {
    line["disposition"] = json!(disposition);
    line["message"] = json!(message);
}

pub async fn run(a: Args) -> anyhow::Result<()> {
    // As the search and metrics handlers build them.
    let config = pulsus_config::Config {
        cluster: a.cluster.clone(),
        ..pulsus_config::Config::default()
    };
    let rc = TraceReadConfig::from_config(&config);
    let indexed = IndexedAttr::from_config(&config.traceql_indexed_attributes);
    let tables = StatementTables {
        spans: &rc.spans_v2_table,
        resources: &rc.resources_table,
        traces: &rc.traces_table,
        indexed: &indexed,
    };
    let ctx = MetricsCtx {
        filter: SpanFilterCtx {
            spans_table: &rc.spans_table,
            attrs_table: &rc.attrs_table,
        },
        scan_budget_rows: rc.scan_budget_rows,
        max_series: rc.max_series,
        distributed: rc.distributed,
        skip_unavailable_shards: rc.skip_unavailable_shards,
    };
    let pool = Arc::new(
        ChPool::connect(ChConnConfig {
            server: a.host.clone(),
            http_port: a.port,
            database: a.database.clone(),
            proto: ChProto::Http,
            pool_size: 4,
            query_timeout: TIMEOUT,
            ..ChConnConfig::default()
        })
        .await?,
    );
    let start = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let engine = |index: usize, request: &str| {
        let prefix = format!("catalogue-{start}-{index}-{request}");
        let engine = TraceEngine::new(
            ChClient::from_shared_pool(pool.clone(), TIMEOUT),
            rc.clone(),
        )
        .with_statement_prefix(&prefix);
        (engine, prefix)
    };

    // Each line, with the prefix of every request it made.
    let mut lines: Vec<(Value, Vec<(&str, String)>)> = Vec::new();
    for (index, case) in cases(&a.corpus, &a.extra)?.into_iter().enumerate() {
        let mut line = json!({"name": case.name, "group": case.group, "traceql": case.traceql});
        let query = match pulsus_traceql::parse(&case.traceql) {
            Ok(q) => q,
            Err(e) => {
                refused(&mut line, "parse_error", e.to_string());
                lines.push((line, Vec::new()));
                continue;
            }
        };
        if let Err(e) = pulsus_traceql::validate(&query) {
            refused(&mut line, "validate_error", e.to_string());
            lines.push((line, Vec::new()));
            continue;
        }
        let metrics = query
            .pipeline
            .iter()
            .any(|s| matches!(s, PipelineStage::Metric(_) | PipelineStage::Compare { .. }));
        let mut prefixes = Vec::new();
        let result = if metrics {
            let params = MetricsParams {
                start_ns: a.start_ns,
                end_ns: a.end_ns,
                step_ms: STEP_MS,
                exemplars: None,
                floor_ns: i64::MIN,
            };
            let plan = match plan_trace_metrics_routed(&query, &params, &ctx, &tables) {
                Ok(plan) => plan,
                Err(e) => {
                    refused(&mut line, "plan_error", e.to_string());
                    lines.push((line, Vec::new()));
                    continue;
                }
            };
            line["route"] = json!("metrics");
            line["engine"] = json!(if plan.statement().is_some() {
                "statement"
            } else {
                "previous"
            });
            let (range_engine, prefix) = engine(index, "answer");
            prefixes.push(("answer", prefix));
            let range = range_engine
                .metrics_range_routed(&plan)
                .await
                .map(|r| metrics_json(&r))
                .map_err(|e| e.to_string());
            // Issue #600 part 1b-2: the instant route's answer to the same
            // plan, as the instant handler issues it.
            let (instant_engine, prefix) = engine(index, "instant");
            prefixes.push(("instant", prefix));
            match instant_engine.metrics_instant_routed(&plan).await {
                Ok(r) => line["instant"] = metrics_json(&r),
                Err(e) => line["instant_error"] = json!(e.to_string()),
            }
            range
        } else {
            let mut requests = Vec::new();
            let mut refusal = None;
            for (name, limit, spss) in SEARCH_REQUESTS {
                let params = SearchParams {
                    start_ns: a.start_ns,
                    end_ns: a.end_ns,
                    limit,
                    spss,
                };
                match plan_search_request(&query, &params, &tables, rc.max_depth) {
                    Ok(request) => requests.push((name, request)),
                    Err(e) => {
                        refusal = Some(e.to_string());
                        break;
                    }
                }
            }
            if let Some(message) = refusal {
                refused(&mut line, "plan_error", message);
                lines.push((line, Vec::new()));
                continue;
            }
            line["route"] = json!("search");
            let mut answers = json!({});
            let mut failed = None;
            for (name, request) in requests {
                let (engine, prefix) = engine(index, name);
                prefixes.push((name, prefix));
                match engine.search_routed(&request).await {
                    Ok(out) => answers[name] = search_json(&out),
                    Err(e) => {
                        failed = Some(e.to_string());
                        break;
                    }
                }
            }
            failed.map_or(Ok(answers), Err)
        };
        line["disposition"] = json!("served");
        match result {
            Ok(v) => line["result"] = v,
            Err(e) => line["error"] = json!(e),
        }
        lines.push((line, prefixes));
    }

    let log = ChClient::from_shared_pool(pool.clone(), TIMEOUT);
    log.execute(
        "SYSTEM FLUSH LOGS",
        &QuerySettings::new(),
        Idempotency::Idempotent,
    )
    .await
    .context("SYSTEM FLUSH LOGS")?;
    let mut stdout = std::io::stdout().lock();
    for (mut line, prefixes) in lines {
        if line["disposition"] == "served" {
            let mut statements = json!({});
            let mut overflow = json!({});
            for (request, prefix) in prefixes {
                let rows = log
                    .query_strings(&statements_sql(&prefix), &QuerySettings::new())
                    .await
                    .with_context(|| format!("system.query_log for {prefix}"))?;
                let (kept, over) = cap_statements(rows);
                let kept = kept
                    .iter()
                    .map(|s| serde_json::from_str::<Value>(s))
                    .collect::<Result<Vec<_>, _>>()
                    .with_context(|| format!("a statement row of {prefix}"))?;
                statements[request] = Value::Array(kept);
                overflow[request] = json!(over);
            }
            line["statements"] = statements;
            line["overflow"] = overflow;
        }
        writeln!(stdout, "{line}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("statement {i}")).collect()
    }

    /// `T-Q2`'s ceiling: a request with exactly `MAX_STATEMENTS` statements
    /// keeps them all and is not over.
    #[test]
    fn a_request_at_the_ceiling_keeps_every_statement() {
        let (kept, over) = cap_statements(rows(MAX_STATEMENTS));
        assert_eq!(kept, rows(MAX_STATEMENTS));
        assert!(!over);
    }

    /// The sentinel row: one more than the ceiling keeps the first
    /// `MAX_STATEMENTS` and reports the request over.
    #[test]
    fn the_sentinel_row_marks_the_request_over() {
        let (kept, over) = cap_statements(rows(MAX_STATEMENTS + 1));
        assert_eq!(kept, rows(MAX_STATEMENTS));
        assert!(over);
    }

    /// The read that feeds `cap_statements` asks for the sentinel: 101
    /// rows, one past the ceiling of 100, under the request's own prefix.
    /// Asking for 100 would never return the row that marks a request over.
    #[test]
    fn the_statement_read_asks_for_the_sentinel_row() {
        let sql = statements_sql("catalogue-1-2-answer");
        assert!(
            sql.contains("WHERE log_comment = 'catalogue-1-2-answer' AND"),
            "{sql}"
        );
        assert_eq!(
            sql.rsplit_once("LIMIT ").map(|(_, n)| n),
            Some("101"),
            "{sql}"
        );
    }
}
