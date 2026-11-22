//! `profileql_frontend` (issue #832): the profiles datasource and the
//! exploration app, provisioned in the `profileql-frontend` service of
//! `deploy/e2e/compose.single.yaml`, driven end to end against PulsusDB.
//!
//! The scenario pushes a small fixture straight to PulsusDB (§4.1 of the
//! plan), checks the datasource through the front end's HTTP API (F1–F6),
//! then loads the app's pages and the trace view in a headless browser in
//! the `profileql-browser` service (`e2e/profileql/pages.mjs`). The script
//! records every `/api/` response of each page; every judgement is made
//! here, by [`page_failures`].

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::harness::poll_until;
use crate::scenarios::Ctx;

mod parse;
use parse::Parsed;

/// The front end's published address (`ports` of `profileql-frontend`).
const FRONTEND_URL: &str = "http://127.0.0.1:3002";
/// The front end's address inside the stack's network, as the browser
/// reaches it.
const FRONTEND_IN_NETWORK: &str = "http://172.29.0.18:3000";
const BROWSER_SERVICE: &str = "profileql-browser";

const DS_T1: &str = "pulsus-profileql-t1";
const DS_T2: &str = "pulsus-profileql-t2";
const DS_TRACES: &str = "pulsus-traceql-t1";
const DS_TYPE: &str = "grafana-pyroscope-datasource";
const APP_ID: &str = "grafana-pyroscope-app";
const DS_VERSION: &str = "13.0.10";
const APP_VERSION: &str = "3.2.0";

const PROCESS_CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const CPU: &str = "cpu:cpu:nanoseconds:cpu:nanoseconds";
const OTLP_SELECTOR: &str = r#"{service_name="svc-otlp"}"#;

const TRACE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SPAN_WORK: &str = "0102030405060708";
const SPAN_IO: &str = "0a0b0c0d0e0f1011";
const SPAN_UNPROFILED: &str = "1111111111111111";

const READY_TIMEOUT: Duration = Duration::from_secs(90);
const VISIBLE_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const QUERY_TIMEOUT: Duration = Duration::from_secs(60);

const RECORDING_RULES: &str = "/settings.v1.RecordingRulesService/ListRecordingRules";

// ---------------------------------------------------------------------
// What the browser is asked to do, and what it reports
// ---------------------------------------------------------------------

/// One request a page must send: a recorded path ending in `path`, whose
/// request body contains every string in `req`.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct Expect {
    pub path: String,
    pub req: Vec<String>,
}

fn expect(path: &str) -> Expect {
    Expect {
        path: path.to_string(),
        req: Vec::new(),
    }
}

fn expect_req(path: &str, req: &[&str]) -> Expect {
    Expect {
        path: path.to_string(),
        req: req.iter().map(|s| s.to_string()).collect(),
    }
}

/// Requests that must not be recorded, and texts that must not be on the
/// page.
#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct Absent {
    pub requests: Vec<Expect>,
    pub texts: Vec<String>,
}

impl Absent {
    fn is_empty(&self) -> bool {
        self.requests.is_empty() && self.texts.is_empty()
    }
}

/// A text read from the page: in the last element with test id `scope`,
/// or in the page body when `scope` is absent.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct Text {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub text: String,
}

fn text_in(scope: &str, text: &str) -> Text {
    Text {
        scope: Some(scope.to_string()),
        text: text.to_string(),
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Upload {
    pub file: String,
    pub base64: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Save {
    pub max_nodes: u32,
}

/// The trace view: wait for `rows` span rows, click the `index`-th, then
/// follow `link` when given.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Row {
    pub rows: usize,
    pub index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
}

/// What a page's answers must hold (R4).
#[derive(Debug, Clone, Default)]
pub(crate) enum Check {
    #[default]
    None,
    /// Every `Series` answer's `service_name` values, together, are exactly
    /// these.
    Services(Vec<&'static str>),
    /// Every `/api/ds/query` answer holding a flame graph has this root
    /// value, and there is at least one.
    FlameRoot(i64),
    /// The span heatmap: the counts' sum and the exemplars' span ids and
    /// values.
    Heatmap {
        count: i64,
        exemplars: Vec<(&'static str, i64)>,
    },
    /// The diff page's `render-diff` answer: both sides' totals.
    Diff { left: i64, right: i64 },
    /// The ad hoc upload's flame graph total.
    Upload(i64),
    /// Every settings `Get` answer's `maxNodes`.
    Settings(i64),
    /// The span's two flame graphs, the inline one and the opened pane's.
    Span {
        span: &'static str,
        value: i64,
        labels: Vec<&'static str>,
    },
}

/// One page, as the browser is asked to load it.
#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct Spec {
    pub name: String,
    pub url: String,
    pub expect: Vec<Expect>,
    #[serde(skip_serializing_if = "Absent::is_empty")]
    pub absent: Absent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload: Option<Upload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub save: Option<Save>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row: Option<Row>,
    pub visible: Vec<Text>,
    pub invisible: Vec<Text>,
    /// A CSS selector hidden before the texts are read. Break checks only;
    /// no committed spec sets it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hide: Option<String>,
    #[serde(skip)]
    pub check: Check,
}

/// One recorded `/api/` response.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Record {
    pub status: u16,
    pub method: String,
    pub path: String,
    pub request: String,
    pub body: String,
    /// The browser could not read the body; the page did not abort it.
    pub body_error: Option<String>,
    /// The page aborted the request (`requestfailed`): not an answer it
    /// used, so it is not judged as one.
    pub aborted: bool,
}

/// Whether a [`Text`] was visible.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct Shown {
    #[serde(default)]
    pub scope: Option<String>,
    pub text: String,
    pub shown: bool,
}

/// One page, as the browser reports it.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PageRecord {
    pub name: String,
    pub complete: bool,
    #[serde(default)]
    pub error: Option<String>,
    pub records: Vec<Record>,
    #[serde(default)]
    pub absent_found: Vec<String>,
    #[serde(default)]
    pub visible: Vec<Shown>,
    #[serde(default)]
    pub invisible: Vec<Shown>,
}

// ---------------------------------------------------------------------
// The judgement
// ---------------------------------------------------------------------

/// Every rule R1–R6 a page breaks, one line each.
pub(crate) fn page_failures(spec: &Spec, page: &PageRecord) -> Vec<String> {
    let name = &spec.name;
    let mut out = Vec::new();
    let browser = page
        .error
        .as_deref()
        .map(|e| format!("; the browser: {e}"))
        .unwrap_or_default();

    // The answers the page used: the requests it aborted are not.
    let live: Vec<Record> = page
        .records
        .iter()
        .filter(|r| !r.aborted)
        .cloned()
        .collect();

    // R1: every expected request answered 200, and the wait met.
    let mut expected = spec.expect.clone();
    if spec.save.is_some() {
        expected.push(expect(SETTINGS_SET));
    }
    let mut missing = false;
    for e in &expected {
        if !live.iter().any(|r| r.status == 200 && matches(r, e)) {
            missing = true;
            let answers: Vec<u16> = live
                .iter()
                .filter(|r| matches(r, e))
                .map(|r| r.status)
                .collect();
            out.push(format!(
                "{name}: R1 no 200 answer to {} {:?} (answers {answers:?}){browser}",
                e.path, e.req
            ));
        }
    }
    if !page.complete && !missing {
        out.push(format!(
            "{name}: R1 the expected requests did not all arrive in time{browser}"
        ));
    }

    // R2: every datasource request answered exactly 200 with a body the
    // browser read, except the one accepted error.
    for r in &live {
        let judged = [DS_QUERY, "/api/datasources/proxy/", "/api/datasources/uid/"]
            .iter()
            .any(|prefix| r.path.starts_with(prefix));
        if !judged {
            continue;
        }
        if let Some(e) = &r.body_error {
            out.push(format!(
                "{name}: R2 {} {}: body not read: {e}",
                r.method, r.path
            ));
            continue;
        }
        if r.status == 200 {
            continue;
        }
        let unimplemented = parse::connect_code(&r.body).is_ok_and(|code| code == "unimplemented");
        if r.path.ends_with(RECORDING_RULES) && r.status == 501 && unimplemented {
            continue;
        }
        out.push(format!(
            "{name}: R2 {} {} answered {}: {:.300}",
            r.method, r.path, r.status, r.body
        ));
    }

    // R3: every query answer parses: each result status 200, no error,
    // every frame well formed.
    for r in live
        .iter()
        .filter(|r| r.path == DS_QUERY && r.status == 200 && r.body_error.is_none())
    {
        if let Err(e) = parse::answer(&r.body) {
            out.push(format!("{name}: R3 /api/ds/query: {e}"));
        }
    }

    // R4
    out.extend(
        value_failures(&spec.check, &live)
            .into_iter()
            .map(|f| format!("{name}: R4 {f}")),
    );

    // R5
    for e in &spec.absent.requests {
        if page.records.iter().any(|r| matches(r, e)) {
            out.push(format!("{name}: R5 {} {:?} was sent", e.path, e.req));
        }
    }
    for text in &page.absent_found {
        out.push(format!("{name}: R5 {text:?} is on the page"));
    }

    // R6
    let shown = |list: &[Shown], t: &Text| {
        list.iter()
            .any(|s| s.scope == t.scope && s.text == t.text && s.shown)
    };
    for t in &spec.visible {
        if !shown(&page.visible, t) {
            out.push(format!(
                "{name}: R6 not visible in {}: {:?}",
                t.scope.as_deref().unwrap_or("the page"),
                t.text
            ));
        }
    }
    for t in &spec.invisible {
        if shown(&page.invisible, t) {
            out.push(format!(
                "{name}: R6 visible in {}: {:?}",
                t.scope.as_deref().unwrap_or("the page"),
                t.text
            ));
        }
    }
    out
}

fn matches(r: &Record, e: &Expect) -> bool {
    r.path.ends_with(&e.path) && e.req.iter().all(|s| r.request.contains(s.as_str()))
}

/// Every 200 answer whose path ends in `path`, through `parser`: the
/// parsed answers, and one failure for each that does not parse.
fn parsed<T>(
    records: &[Record],
    path: &str,
    parser: impl Fn(&str) -> Parsed<T>,
    out: &mut Vec<String>,
) -> Vec<T> {
    let mut good = Vec::new();
    for r in records
        .iter()
        .filter(|r| r.status == 200 && r.path.ends_with(path))
    {
        match parser(&r.body) {
            Ok(v) => good.push(v),
            Err(e) => out.push(format!("{path}: {e}")),
        }
    }
    good
}

/// R4: what [`Check`] asks of the answers. Every answer the check reads is
/// parsed strictly; one that does not parse fails the check.
fn value_failures(check: &Check, records: &[Record]) -> Vec<String> {
    let mut out = Vec::new();
    match check {
        Check::None => {}
        Check::Services(want) => {
            let sets = parsed(records, SERIES, parse::series_answer, &mut out);
            let got: BTreeSet<String> = sets
                .iter()
                .flatten()
                .filter_map(|labels| labels.get("service_name").cloned())
                .collect();
            let want: BTreeSet<String> = want.iter().map(|s| s.to_string()).collect();
            if sets.is_empty() || got != want {
                out.push(format!("Series lists {got:?}, expected {want:?}"));
            }
        }
        Check::FlameRoot(want) => {
            let mut roots = Vec::new();
            for results in parsed(records, DS_QUERY, parse::answer, &mut out) {
                for (ref_id, result) in &results {
                    for (i, frame) in result.frames.iter().enumerate() {
                        match parse::kind(frame) {
                            Ok(parse::Kind::Flame(g)) => roots.push(g.root()),
                            Ok(_) => {}
                            Err(e) => out.push(format!("result {ref_id} frame {i}: {e}")),
                        }
                    }
                }
            }
            if roots.is_empty() || roots.iter().any(|root| root != want) {
                out.push(format!(
                    "flame graph root values {roots:?}, expected {want}"
                ));
            }
        }
        Check::Heatmap { count, exemplars } => {
            let answers = parsed(
                records,
                "/querier.v1.QuerierService/SelectHeatmap",
                parse::heatmap_answer,
                &mut out,
            );
            if answers.is_empty() {
                out.push("no SelectHeatmap answer".to_string());
            }
            let mut want: Vec<(String, i64)> =
                exemplars.iter().map(|(s, v)| (s.to_string(), *v)).collect();
            want.sort();
            for (got_count, mut got) in answers {
                got.sort();
                if got_count != *count || got != want {
                    out.push(format!(
                        "heatmap counts sum {got_count}, exemplars {got:?}; expected {count}, {want:?}"
                    ));
                }
            }
        }
        Check::Diff { left, right } => {
            let answers = parsed(
                records,
                "/pyroscope/render-diff",
                parse::diff_answer,
                &mut out,
            );
            if answers.is_empty() {
                out.push("no render-diff answer".to_string());
            }
            for got in answers {
                if got != (*left, *right) {
                    out.push(format!(
                        "render-diff totals {got:?}, expected {left}, {right}"
                    ));
                }
            }
        }
        Check::Upload(want) => {
            let ticks = parsed(
                records,
                "/adhocprofiles.v1.AdHocProfileService/Upload",
                parse::upload_answer,
                &mut out,
            );
            if ticks.is_empty() || ticks.iter().any(|t| t != want) {
                out.push(format!("upload totals {ticks:?}, expected {want}"));
            }
        }
        Check::Settings(max_nodes) => {
            let settings = parsed(records, SETTINGS_GET, parse::plugin_settings, &mut out);
            if settings.is_empty()
                || settings
                    .iter()
                    .any(|s| s["maxNodes"].as_i64() != Some(*max_nodes))
            {
                out.push(format!(
                    "settings maxNodes {:?}, expected {max_nodes}",
                    settings
                        .iter()
                        .map(|s| s["maxNodes"].clone())
                        .collect::<Vec<_>>()
                ));
            }
        }
        Check::Span {
            span,
            value,
            labels,
        } => {
            for ref_id in ["span-flamegraph-refId", "profile"] {
                let mut seen = false;
                for r in records
                    .iter()
                    .filter(|r| r.status == 200 && r.path == DS_QUERY)
                {
                    let queries = match parse::request_queries(&r.request) {
                        Ok(q) => q,
                        Err(e) => {
                            out.push(format!("a /api/ds/query request: {e}"));
                            continue;
                        }
                    };
                    let asked = queries.iter().any(|q| {
                        q.get("refId") == Some(&json!(ref_id))
                            && q.get("spanSelector") == Some(&json!([span]))
                            && q.get("labelSelector") == Some(&json!(OTLP_SELECTOR))
                            && q.get("profileTypeId") == Some(&json!(CPU))
                    });
                    if !asked {
                        continue;
                    }
                    seen = true;
                    let graph = parse::answer(&r.body).and_then(|results| {
                        results
                            .get(ref_id)
                            .ok_or_else(|| format!("no result {ref_id}"))
                            .and_then(parse::only_flame)
                    });
                    match graph {
                        Ok(g) if g.root() == *value && g.labels == *labels => {}
                        Ok(g) => out.push(format!(
                            "{ref_id} for span {span}: root {}, labels {:?}; expected {value}, {labels:?}",
                            g.root(),
                            g.labels
                        )),
                        Err(e) => out.push(format!("{ref_id} for span {span}: {e}")),
                    }
                }
                if !seen {
                    out.push(format!("no {ref_id} query for span {span}"));
                }
            }
        }
    }
    out
}

// ---------------------------------------------------------------------
// The scenario
// ---------------------------------------------------------------------

pub(crate) async fn profileql_frontend(ctx: &Ctx) -> Result<()> {
    let now_s = unix_seconds()?;
    let t0 = now_s - 600;
    let p_ns = (now_s - 120) * 1_000_000_000;

    push_fixture(ctx, t0, p_ns).await?;
    wait_for_pulsusdb(ctx).await?;

    let http = reqwest::Client::new();
    wait_for_frontend(&http).await?;

    let mut failures = datasource_failures(&http, now_s).await?;

    let export = export_bytes(ctx, t0, now_s).await?;
    let specs = page_specs(t0, &export);
    let pages = run_pages(ctx, &specs).await?;
    for spec in &specs {
        match pages.iter().find(|p| p.name == spec.name) {
            Some(page) => failures.extend(page_failures(spec, page)),
            None => failures.push(format!("{}: the browser reported no page", spec.name)),
        }
    }

    if !failures.is_empty() {
        bail!(
            "profileql_frontend: {} failure(s):\n  {}",
            failures.len(),
            failures.join("\n  ")
        );
    }
    Ok(())
}

fn unix_seconds() -> Result<i64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs() as i64)
}

// -- Pushes (§4.1) -----------------------------------------------------

async fn post(ctx: &Ctx, path: &str, tenant: &str, content_type: &str, body: String) -> Result<()> {
    let res = ctx
        .http
        .post(ctx.url(path))
        .header("X-Scope-OrgID", tenant)
        .header("Content-Type", content_type)
        .timeout(QUERY_TIMEOUT)
        .body(body)
        .send()
        .await
        .with_context(|| format!("POST {path} as {tenant}"))?;
    let status = res.status();
    if status != 200 {
        let text = res.text().await.unwrap_or_default();
        bail!("POST {path} as {tenant}: {status}, expected 200: {text}");
    }
    Ok(())
}

fn folded_path(name: &str, from: i64) -> String {
    format!(
        "/api/profiles/v1/ingest?name={}&format=folded&from={from}&until={}&sampleRate=100",
        encode(name),
        from + 10
    )
}

async fn push_fixture(ctx: &Ctx, t0: i64, p_ns: i64) -> Result<()> {
    for (tenant, scale) in [("t1", 1_i64), ("t2", 100)] {
        for i in 0..6 {
            let from = t0 + 60 * i;
            let a = format!(
                "main;a;b {}\nmain;a;c {}\nmain;d {}\n",
                10 * scale,
                5 * scale,
                3 * scale
            );
            post(
                ctx,
                &folded_path("svc-a.cpu{env=prod}", from),
                tenant,
                "text/plain",
                a,
            )
            .await?;
            let b = format!("main;x {}\n", 7 * scale);
            post(
                ctx,
                &folded_path("svc-b.cpu{env=dev}", from),
                tenant,
                "text/plain",
                b,
            )
            .await?;
        }
        post(
            ctx,
            "/v1development/profiles",
            tenant,
            "application/json",
            otlp_profile(p_ns, scale).to_string(),
        )
        .await?;
    }
    post(
        ctx,
        &folded_path("svc-t2only.cpu", t0),
        "t2",
        "text/plain",
        "main;z 1\n".to_string(),
    )
    .await?;
    post(
        ctx,
        "/v1/traces",
        "t1",
        "application/json",
        otlp_trace(p_ns).to_string(),
    )
    .await?;
    Ok(())
}

/// Push O: `svc-otlp`, one profile at `p_ns`, two of its three stacks
/// linked to a span of the trace.
fn otlp_profile(p_ns: i64, scale: i64) -> Value {
    let value = |v: i64| (v * scale).to_string();
    json!({
        "dictionary": {
            "stringTable": ["", "cpu", "nanoseconds", "main", "work", "mallocgc", "io"],
            "mappingTable": [{}],
            "functionTable": [{}, {"nameStrindex": 3}, {"nameStrindex": 4},
                              {"nameStrindex": 5}, {"nameStrindex": 6}],
            "locationTable": [{}, {"lines": [{"functionIndex": 1}]},
                              {"lines": [{"functionIndex": 2}]},
                              {"lines": [{"functionIndex": 3}]},
                              {"lines": [{"functionIndex": 4}]}],
            "stackTable": [{}, {"locationIndices": [2, 1]},
                           {"locationIndices": [3, 2, 1]},
                           {"locationIndices": [4, 2, 1]}],
            "linkTable": [{}, {"traceId": TRACE_ID, "spanId": SPAN_WORK},
                          {"traceId": TRACE_ID, "spanId": SPAN_IO}],
            "attributeTable": [{}],
        },
        "resourceProfiles": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "svc-otlp"}}]},
            "scopeProfiles": [{"profiles": [{
                "sampleType": {"typeStrindex": 1, "unitStrindex": 2},
                "periodType": {"typeStrindex": 1, "unitStrindex": 2},
                "period": "10000000",
                "timeUnixNano": p_ns.to_string(),
                "durationNano": "10000000000",
                "samples": [
                    {"stackIndex": 1, "values": [value(30_000_000)]},
                    {"stackIndex": 2, "values": [value(10_000_000)], "linkIndex": 1},
                    {"stackIndex": 3, "values": [value(20_000_000)], "linkIndex": 2},
                ],
            }]}],
        }],
    })
}

/// Push T: the trace, `work` and `io` carrying `pyroscope.profile.id`.
fn otlp_trace(p_ns: i64) -> Value {
    let end = (p_ns + 1_000_000_000).to_string();
    let profiled =
        |id: &str| json!([{"key": "pyroscope.profile.id", "value": {"stringValue": id}}]);
    json!({
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "svc-otlp"}}]},
            "scopeSpans": [{"spans": [
                {"traceId": TRACE_ID, "spanId": SPAN_WORK, "name": "work", "kind": 2,
                 "startTimeUnixNano": p_ns.to_string(), "endTimeUnixNano": end,
                 "attributes": profiled(SPAN_WORK)},
                {"traceId": TRACE_ID, "spanId": SPAN_IO, "parentSpanId": SPAN_WORK,
                 "name": "io", "kind": 1,
                 "startTimeUnixNano": (p_ns + 100_000_000).to_string(), "endTimeUnixNano": end,
                 "attributes": profiled(SPAN_IO)},
                {"traceId": TRACE_ID, "spanId": SPAN_UNPROFILED, "parentSpanId": SPAN_WORK,
                 "name": "unprofiled", "kind": 1,
                 "startTimeUnixNano": (p_ns + 200_000_000).to_string(), "endTimeUnixNano": end},
            ]}],
        }],
    })
}

/// Polls `Series` until each tenant lists its services, then the trace
/// until it has its three spans.
async fn wait_for_pulsusdb(ctx: &Ctx) -> Result<()> {
    let now_ms = unix_seconds()? * 1000;
    for (tenant, want) in [
        ("t1", vec!["svc-a", "svc-b", "svc-otlp"]),
        ("t2", vec!["svc-a", "svc-b", "svc-otlp", "svc-t2only"]),
    ] {
        let want: BTreeSet<String> = want.into_iter().map(String::from).collect();
        poll_until(VISIBLE_TIMEOUT, POLL_INTERVAL, || async {
            let res = ctx
                .http
                .post(ctx.url("/api/profiles/v1/querier.v1.QuerierService/Series"))
                .header("X-Scope-OrgID", tenant)
                .json(&json!({
                    "labelNames": ["service_name"],
                    "start": (now_ms - 3_600_000).to_string(),
                    "end": (now_ms + 60_000).to_string(),
                }))
                .send()
                .await?;
            if res.status() != 200 {
                return Ok(None);
            }
            let sets = match parse::series_answer(&res.text().await?) {
                Ok(sets) => sets,
                Err(_) => return Ok(None),
            };
            let got: BTreeSet<String> = sets
                .iter()
                .filter_map(|labels| labels.get("service_name").cloned())
                .collect();
            Ok((got == want).then_some(()))
        })
        .await
        .with_context(|| format!("{tenant}'s services never listed {want:?}"))?;
    }
    poll_until(VISIBLE_TIMEOUT, POLL_INTERVAL, || async {
        let res = ctx
            .http
            .get(ctx.url(&format!("/api/traces/v1/trace/{TRACE_ID}")))
            .header("X-Scope-OrgID", "t1")
            .send()
            .await?;
        if res.status() != 200 {
            return Ok(None);
        }
        let body: Value = res.json().await?;
        Ok((span_count(&body) == 3).then_some(()))
    })
    .await
    .context("the trace never had its three spans")?;
    Ok(())
}

fn span_count(body: &Value) -> usize {
    ["resourceSpans", "batches"]
        .into_iter()
        .filter_map(|key| body[key].as_array())
        .flatten()
        .filter_map(|rs| rs["scopeSpans"].as_array())
        .flatten()
        .filter_map(|ss| ss["spans"].as_array())
        .map(Vec::len)
        .sum()
}

// -- The front end -------------------------------------------------------

async fn frontend_get(http: &reqwest::Client, path: &str) -> Result<(u16, String)> {
    let res = http
        .get(format!("{FRONTEND_URL}{path}"))
        .timeout(QUERY_TIMEOUT)
        .send()
        .await
        .with_context(|| format!("GET {path} on the front end"))?;
    let status = res.status().as_u16();
    let text = res.text().await.context("reading a front end answer")?;
    Ok((status, text))
}

/// `/api/health`, both plugins at their pinned versions with the app
/// enabled, then both datasources provisioned.
async fn wait_for_frontend(http: &reqwest::Client) -> Result<()> {
    let ok = |path: String| async move {
        Ok(match frontend_get(http, &path).await {
            Ok((200, text)) => parse::json(&text).ok(),
            _ => None,
        })
    };
    poll_until(READY_TIMEOUT, POLL_INTERVAL, || ok("/api/health".into()))
        .await
        .context("the front end's /api/health never answered 200")?;
    for (id, version) in [(DS_TYPE, DS_VERSION), (APP_ID, APP_VERSION)] {
        let path = format!("/api/plugins/{id}/settings");
        let body = poll_until(READY_TIMEOUT, POLL_INTERVAL, || ok(path.clone()))
            .await
            .with_context(|| format!("{path} never answered 200"))?;
        if body["info"]["version"] != version {
            bail!(
                "plugin {id}: version {} is not {version}",
                body["info"]["version"]
            );
        }
        if id == APP_ID && body["enabled"] != true {
            bail!("plugin {id} is not enabled: {body}");
        }
    }
    for uid in [DS_T1, DS_T2] {
        let path = format!("/api/datasources/uid/{uid}");
        poll_until(READY_TIMEOUT, POLL_INTERVAL, || ok(path.clone()))
            .await
            .with_context(|| format!("datasource {uid} was never provisioned"))?;
    }
    Ok(())
}

/// One `/api/ds/query` of the profiles datasource: the answer must be
/// exactly 200 and parse strictly, with the one result `A`.
async fn ds_query(
    http: &reqwest::Client,
    uid: &str,
    query: Value,
    now_s: i64,
) -> Parsed<parse::QueryResult> {
    let mut query = query;
    query["refId"] = json!("A");
    query["datasource"] = json!({"type": DS_TYPE, "uid": uid});
    let body = json!({
        "queries": [query],
        "from": ((now_s - 3600) * 1000).to_string(),
        "to": ((now_s + 60) * 1000).to_string(),
    });
    let res = http
        .post(format!("{FRONTEND_URL}/api/ds/query"))
        .timeout(QUERY_TIMEOUT)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("POST /api/ds/query: {e}"))?;
    let status = res.status();
    let text = res
        .text()
        .await
        .map_err(|e| format!("reading /api/ds/query: {e}"))?;
    if status != 200 {
        return Err(format!("/api/ds/query answered {status}: {text:.500}"));
    }
    let mut results = parse::answer(&text)?;
    match (results.remove("A"), results.is_empty()) {
        (Some(a), true) => Ok(a),
        _ => Err(format!(
            "/api/ds/query: results are not exactly A: {text:.500}"
        )),
    }
}

/// F1–F6 (§4.3). Every answer is parsed strictly; one that does not parse
/// fails the check that asked for it.
async fn datasource_failures(http: &reqwest::Client, now_s: i64) -> Result<Vec<String>> {
    let mut failures = Vec::new();
    let start = (now_s - 3600) * 1000;
    let end = (now_s + 60) * 1000;

    for (uid, services, total, otlp_scale, metric_services, profiles) in [
        (
            DS_T1,
            vec!["svc-a", "svc-b", "svc-otlp"],
            1_500_000_000_i64,
            1_i64,
            vec!["svc-a", "svc-b"],
            12_i64,
        ),
        (
            DS_T2,
            vec!["svc-a", "svc-b", "svc-otlp", "svc-t2only"],
            150_010_000_000,
            100,
            vec!["svc-a", "svc-b", "svc-t2only"],
            13,
        ),
    ] {
        let mut check = |what: String, outcome: Parsed<()>| {
            if let Err(e) = outcome {
                failures.push(format!("{what} {uid}: {e}"));
            }
        };

        // F1
        let (status, text) =
            frontend_get(http, &format!("/api/datasources/uid/{uid}/health")).await?;
        check(
            "F1".into(),
            if status == 200 {
                parse::health(&text)
            } else {
                Err(format!("health answered {status}: {text:.300}"))
            },
        );

        // F2
        let (status, text) = frontend_get(
            http,
            &format!(
                "/api/datasources/uid/{uid}/resources/labelValues?label=service_name&query={}&start={start}&end={end}",
                encode("{}")
            ),
        )
        .await?;
        let mut want: Vec<String> = services.iter().map(|s| s.to_string()).collect();
        want.sort();
        check(
            "F2".into(),
            if status == 200 {
                parse::strings(&text).and_then(|mut got| {
                    got.sort();
                    if got == want {
                        Ok(())
                    } else {
                        Err(format!("labelValues {got:?}, expected {want:?}"))
                    }
                })
            } else {
                Err(format!("labelValues answered {status}: {text:.300}"))
            },
        );

        // F3
        let result = ds_query(
            http,
            uid,
            json!({"queryType": "profile", "profileTypeId": PROCESS_CPU,
                   "labelSelector": "{}", "groupBy": []}),
            now_s,
        )
        .await;
        check(
            "F3".into(),
            result.and_then(|r| parse::only_flame(&r)).and_then(|g| {
                if g.root() == total {
                    Ok(())
                } else {
                    Err(format!("root value {}, expected {total}", g.root()))
                }
            }),
        );

        // F4: every frame a time series, one per service, each labelled.
        let result = ds_query(
            http,
            uid,
            json!({"queryType": "metrics", "profileTypeId": PROCESS_CPU,
                   "labelSelector": "{}", "groupBy": ["service_name"]}),
            now_s,
        )
        .await;
        check(
            "F4".into(),
            result.and_then(|r| {
                let mut labels = r
                    .frames
                    .iter()
                    .map(|f| {
                        let series = parse::series(f)?;
                        if series.times.is_empty() || series.values.is_empty() {
                            return Err(format!("an empty series: {:?}", series.labels));
                        }
                        series.labels.get("service_name").cloned().ok_or_else(|| {
                            format!("a series without service_name: {:?}", series.labels)
                        })
                    })
                    .collect::<Parsed<Vec<_>>>()?;
                labels.sort();
                let mut want: Vec<String> = metric_services.iter().map(|s| s.to_string()).collect();
                want.sort();
                if labels == want {
                    Ok(())
                } else {
                    Err(format!(
                        "frames labelled {labels:?}, expected one each of {want:?}"
                    ))
                }
            }),
        );

        // F5
        for (span, value) in [
            (Some(SPAN_WORK), 10_000_000),
            (Some(SPAN_IO), 20_000_000),
            (None, 60_000_000),
            (Some(SPAN_UNPROFILED), 0),
        ] {
            let mut query = json!({"queryType": "profile", "profileTypeId": CPU,
                                   "labelSelector": OTLP_SELECTOR, "groupBy": []});
            if let Some(span) = span {
                query["spanSelector"] = json!([span]);
            }
            let want = value * otlp_scale;
            let result = ds_query(http, uid, query, now_s).await;
            // For the unprofiled span the answer must be exactly the empty
            // profile, the root alone at 0.
            check(
                format!("F5 span {span:?}"),
                result.and_then(|r| parse::only_flame(&r)).and_then(|g| {
                    if g.root() != want {
                        Err(format!("root value {}, expected {want}", g.root()))
                    } else if want == 0 && !g.is_empty_profile() {
                        Err(format!("not the empty profile: {g:?}"))
                    } else {
                        Ok(())
                    }
                }),
            );
        }

        // F6: the heatmap frame's counts, then the exemplar frame's rows.
        let result = ds_query(
            http,
            uid,
            json!({"queryType": "metrics", "profileTypeId": PROCESS_CPU,
                   "labelSelector": "{}", "groupBy": [],
                   "includeHeatmap": true, "heatmapType": "individual"}),
            now_s,
        )
        .await;
        check(
            "F6 heatmap".into(),
            result.and_then(|r| {
                let counts = kinds(&r)?
                    .into_iter()
                    .filter_map(|k| match k {
                        parse::Kind::Heatmap(c) => Some(c),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                match &counts[..] {
                    [c] if c.iter().sum::<i64>() == profiles => Ok(()),
                    [c] => Err(format!(
                        "heatmap count {}, expected {profiles}",
                        c.iter().sum::<i64>()
                    )),
                    other => Err(format!("{} heatmap frames, expected one", other.len())),
                }
            }),
        );
        if uid == DS_T1 {
            let result = ds_query(
                http,
                uid,
                json!({"queryType": "metrics", "profileTypeId": PROCESS_CPU,
                       "labelSelector": "{}", "groupBy": [],
                       "includeExemplars": true}),
                now_s,
            )
            .await;
            check(
                "F6 exemplars".into(),
                result.and_then(|r| {
                    let rows = kinds(&r)?
                        .into_iter()
                        .filter_map(|k| match k {
                            parse::Kind::Exemplars(n) => Some(n),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    match &rows[..] {
                        [6] => Ok(()),
                        other => Err(format!(
                            "exemplar frames with rows {other:?}, expected one with 6"
                        )),
                    }
                }),
            );
        }
    }
    Ok(failures)
}

/// Every frame of a result, each parsed as its kind.
fn kinds(r: &parse::QueryResult) -> Parsed<Vec<parse::Kind>> {
    r.frames.iter().map(parse::kind).collect()
}

// -- The pages (§4.4) --------------------------------------------------

/// P8's upload: the pprof export of `svc-a`'s profiles, as `t1`.
async fn export_bytes(ctx: &Ctx, t0: i64, now_s: i64) -> Result<Vec<u8>> {
    let path = format!(
        "/api/profiles/v1/export?query={}&from={}&until={now_s}",
        encode(&format!(r#"{PROCESS_CPU}{{service_name="svc-a"}}"#)),
        t0 - 60
    );
    let res = ctx
        .http
        .get(ctx.url(&path))
        .header("X-Scope-OrgID", "t1")
        .timeout(QUERY_TIMEOUT)
        .send()
        .await
        .context("GET the export")?;
    let status = res.status();
    if !status.is_success() {
        bail!("GET {path}: {status}");
    }
    Ok(res.bytes().await?.to_vec())
}

fn app_url(page: &str, uid: &str, rest: &str) -> String {
    format!("/a/{APP_ID}/{page}var-dataSource={uid}{rest}")
}

fn svc(service: &str, type_id: &str) -> String {
    format!("&var-serviceName={service}&var-profileMetricId={type_id}")
}

const LAST_HOUR: &str = "&from=now-1h&to=now";
const STATS: &str = "/querier.v1.QuerierService/GetProfileStats";
const SETTINGS_GET: &str = "/settings.v1.SettingsService/Get";
const SETTINGS_SET: &str = "/settings.v1.SettingsService/Set";
const SERIES: &str = "/querier.v1.QuerierService/Series";
const DS_QUERY: &str = "/api/ds/query";
const FLAME_PANEL: &str = "flame-graph-panel";

fn page(name: &str, url: String, expect: Vec<Expect>, check: Check) -> Spec {
    Spec {
        name: name.to_string(),
        url,
        expect,
        check,
        ..Spec::default()
    }
}

fn panel(service: &str) -> String {
    format!("data-testid Panel header {service}")
}

fn cores(service: &str, v: &str) -> Text {
    text_in(
        &panel(service),
        &format!("avg cpu = {v} cores / max = {v} cores"),
    )
}

fn trace_url() -> String {
    let panes = json!({"x": {
        "datasource": DS_TRACES,
        "queries": [{"refId": "A", "datasource": {"type": "tempo", "uid": DS_TRACES},
                     "queryType": "traceql", "query": TRACE_ID}],
        "range": {"from": "now-1h", "to": "now"},
    }});
    format!(
        "/explore?schemaVersion=1&panes={}",
        encode(&panes.to_string())
    )
}

fn span_spec(name: &str, index: usize, span: &'static str, value: i64, leaf: &'static str) -> Spec {
    let selector = format!(r#""spanSelector":["{span}"]"#);
    let pane = "data-testid explorer scroll view";
    let shown = format!(
        "{} ms | {} Mil samples (Time)",
        value / 1_000_000,
        value / 1_000_000
    );
    Spec {
        name: name.to_string(),
        url: trace_url(),
        expect: vec![
            expect_req(DS_QUERY, &[&selector, r#""refId":"span-flamegraph-refId""#]),
            expect_req(DS_QUERY, &[&selector, r#""refId":"profile""#]),
        ],
        row: Some(Row {
            rows: 3,
            index,
            link: Some("Profiles for this span".to_string()),
        }),
        visible: vec![
            text_in("span-detail-component", &shown),
            text_in(pane, &format!("Span ID: {span}")),
            text_in(pane, &shown),
        ],
        check: Check::Span {
            span,
            value,
            labels: vec!["total", "main", "work", leaf],
        },
        ..Spec::default()
    }
}

/// Every page, in the order they run: P7 last, as it changes `maxNodes`.
fn page_specs(t0: i64, export: &[u8]) -> Vec<Spec> {
    let ms = |s: i64| (s * 1000).to_string();
    let all = |uid: &str| app_url("explore?explorationType=all&", uid, LAST_HOUR);
    let services_expect = || {
        vec![
            expect(STATS),
            expect(SETTINGS_GET),
            expect(SERIES),
            expect(DS_QUERY),
        ]
    };
    let svc_a = svc("svc-a", PROCESS_CPU);

    let mut p1 = page(
        "P1 services t1",
        all(DS_T1),
        services_expect(),
        Check::Services(vec!["svc-a", "svc-b", "svc-otlp"]),
    );
    p1.visible = vec![cores("svc-a", "0.0120"), cores("svc-b", "0.00467")];
    p1.invisible = vec![text_in("sceneBody", "svc-t2only")];

    let mut p9 = page(
        "P9 services t2",
        all(DS_T2),
        services_expect(),
        Check::Services(vec!["svc-a", "svc-b", "svc-otlp", "svc-t2only"]),
    );
    p9.visible = vec![
        cores("svc-a", "1.20"),
        cores("svc-b", "0.467"),
        cores("svc-t2only", "0.000667"),
    ];

    let mut p2 = page(
        "P2 profile types",
        app_url(
            "explore?explorationType=profiles&",
            DS_T1,
            &format!("{svc_a}{LAST_HOUR}"),
        ),
        vec![expect(STATS), expect(SERIES), expect(DS_QUERY)],
        Check::None,
    );
    p2.visible = vec![text_in(
        &panel("cpu (process_cpu)"),
        "avg cpu = 0.0120 cores / max = 0.0120 cores",
    )];
    let mut p3 = page(
        "P3 labels",
        app_url(
            "explore?explorationType=labels&",
            DS_T1,
            &format!("{svc_a}{LAST_HOUR}"),
        ),
        vec![
            expect("/querier.v1.QuerierService/LabelNames"),
            expect("/querier.v1.QuerierService/LabelValues"),
            expect(DS_QUERY),
        ],
        Check::None,
    );
    p3.visible = vec![text_in(
        "sceneBody",
        "avg prod = 0.0120 cores / max = 0.0120 cores",
    )];
    let mut p4 = page(
        "P4 flame graph",
        app_url(
            "explore?explorationType=flame-graph&",
            DS_T1,
            &format!("{svc_a}{LAST_HOUR}"),
        ),
        vec![expect(SETTINGS_GET), expect(DS_QUERY)],
        Check::FlameRoot(1_080_000_000),
    );
    p4.visible = vec![text_in(FLAME_PANEL, "1.08 s | 1.08 Bil samples (Time)")];
    let mut p5 = page(
        "P5 span heatmap",
        app_url(
            "explore?explorationType=flame-graph&showSpanHeatmap=true&",
            DS_T1,
            &format!("{}{LAST_HOUR}", svc("svc-otlp", CPU)),
        ),
        vec![expect("/querier.v1.QuerierService/SelectHeatmap")],
        Check::Heatmap {
            count: 2,
            exemplars: vec![(SPAN_WORK, 10_000_000), (SPAN_IO, 20_000_000)],
        },
    );
    p5.visible = vec![
        text_in("span-heatmap-panel", SPAN_WORK),
        text_in("span-heatmap-panel", SPAN_IO),
        text_in(FLAME_PANEL, "60 ms | 60 Mil samples (Time)"),
    ];
    let (from, to) = (ms(t0 - 60), ms(t0 + 360));
    let mut p6 = page(
        "P6 diff",
        app_url(
            "explore?explorationType=diff-flame-graph&",
            DS_T1,
            &format!(
                "{svc_a}&from={from}&to={to}&from-2={from}&to-2={to}&from-3={from}&to-3={to}\
                 &diffFrom={}&diffTo={}&diffFrom-2={}&diffTo-2={}",
                ms(t0 - 1),
                ms(t0 + 121),
                ms(t0 + 179),
                ms(t0 + 301)
            ),
        ),
        vec![expect("/pyroscope/render-diff")],
        Check::Diff {
            left: 540_000_000,
            right: 540_000_000,
        },
    );
    // Both sides' totals together.
    p6.visible = vec![text_in(
        "diff-flame-graph-panel",
        "1.08 s | 1.08 Bil samples (Time)",
    )];
    let mut p8 = page(
        "P8 ad hoc",
        app_url("ad-hoc?", DS_T1, ""),
        vec![expect("/adhocprofiles.v1.AdHocProfileService/Upload")],
        Check::Upload(1_080_000_000),
    );
    p8.upload = Some(Upload {
        file: "svc-a.pb.gz".to_string(),
        base64: base64(export),
    });

    let t1 = span_spec("T1 span work", 0, SPAN_WORK, 10_000_000, "mallocgc");
    let t2 = span_spec("T2 span io", 1, SPAN_IO, 20_000_000, "io");
    let t3 = Spec {
        name: "T3 span unprofiled".to_string(),
        url: trace_url(),
        row: Some(Row {
            rows: 3,
            index: 2,
            link: None,
        }),
        absent: Absent {
            requests: vec![expect_req(DS_QUERY, &[r#""spanSelector""#])],
            texts: vec!["Profiles for this span".to_string()],
        },
        ..Spec::default()
    };

    let mut p7 = page(
        "P7 settings",
        app_url("settings?", DS_T1, ""),
        vec![expect(SETTINGS_GET)],
        Check::None,
    );
    p7.save = Some(Save { max_nodes: 12345 });
    let p7b = page(
        "P7b settings reload",
        app_url("settings?", DS_T1, ""),
        vec![expect(SETTINGS_GET)],
        Check::Settings(12345),
    );

    vec![p1, p9, p2, p3, p4, p5, p6, p8, t1, t2, t3, p7, p7b]
}

async fn run_pages(ctx: &Ctx, specs: &[Spec]) -> Result<Vec<PageRecord>> {
    let specs_json = serde_json::to_string(specs)?;
    let compose = ctx.compose.clone();
    let out = tokio::task::spawn_blocking(move || {
        compose.exec(
            BROWSER_SERVICE,
            &["sh", "/src/run.sh", FRONTEND_IN_NETWORK, &specs_json],
        )
    })
    .await
    .context("the browser task panicked")?
    .context("the browser script failed")?;
    serde_json::from_str(&out)
        .with_context(|| format!("the browser's output is not the expected JSON: {out:.2000}"))
}

// -- Helpers -------------------------------------------------------------

/// Percent-encodes everything but the unreserved characters.
fn encode(v: &str) -> String {
    let mut out = String::new();
    for b in v.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0_u32, |n, (i, b)| n | (u32::from(*b) << (16 - 8 * i)));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(status: u16, path: &str, request: &str, body: &str) -> Record {
        Record {
            status,
            method: "POST".to_string(),
            path: path.to_string(),
            request: request.to_string(),
            body: body.to_string(),
            body_error: None,
            aborted: false,
        }
    }

    const DS_OK: &str = r#"{"results":{"A":{"status":200,"frames":[]}}}"#;
    const PROXY: &str = "/api/datasources/proxy/uid/pulsus-profileql-t1";

    /// A flame graph answer whose root value is `root`.
    fn flame(root: i64) -> String {
        json!({"results": {"A": {"status": 200, "frames": [parse::tests::flame_frame(root)]}}})
            .to_string()
    }

    fn spec() -> Spec {
        page(
            "P2",
            "/a/x".to_string(),
            vec![expect(STATS), expect(DS_QUERY)],
            Check::None,
        )
    }

    fn records() -> Vec<Record> {
        vec![
            rec(200, &format!("{PROXY}{STATS}"), "{}", "{}"),
            rec(200, DS_QUERY, "{}", DS_OK),
        ]
    }

    fn record(records: Vec<Record>) -> PageRecord {
        PageRecord {
            name: "P2".to_string(),
            complete: true,
            records,
            ..PageRecord::default()
        }
    }

    #[test]
    fn u1_every_request_answered_is_no_failure() {
        assert_eq!(
            page_failures(&spec(), &record(records())),
            Vec::<String>::new()
        );
    }

    #[test]
    fn u2_a_proxy_404_is_one_failure_naming_its_path() {
        let mut r = records();
        let path = format!("{PROXY}/pyroscope/render-diff");
        r.push(rec(404, &path, "", "404 page not found"));
        let failures = page_failures(&spec(), &record(r));
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains(&path), "{failures:?}");
    }

    #[test]
    fn u3_a_query_error_inside_a_200_is_one_failure() {
        let mut r = records();
        r[1].body = r#"{"results":{"A":{"status":500,"error":"boom"}}}"#.to_string();
        let failures = page_failures(&spec(), &record(r));
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains("boom"), "{failures:?}");
    }

    #[test]
    fn u4_only_list_recording_rules_may_answer_501_unimplemented() {
        let body = r#"{"code":"unimplemented","message":"not served"}"#;
        let mut r = records();
        r.push(rec(501, &format!("{PROXY}{RECORDING_RULES}"), "{}", body));
        assert_eq!(page_failures(&spec(), &record(r)), Vec::<String>::new());

        let mut r = records();
        r.push(rec(
            404,
            &format!("{PROXY}{RECORDING_RULES}"),
            "{}",
            "not found",
        ));
        let failures = page_failures(&spec(), &record(r));
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains(RECORDING_RULES), "{failures:?}");

        let mut r = records();
        let other = format!("{PROXY}/vcs.v1.VCSService/GetFile");
        r.push(rec(501, &other, "{}", body));
        let failures = page_failures(&spec(), &record(r));
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains(&other), "{failures:?}");
    }

    #[test]
    fn u5_an_expected_request_never_sent_is_one_failure_naming_it() {
        let mut page = record(vec![rec(200, DS_QUERY, "{}", DS_OK)]);
        page.complete = false;
        let failures = page_failures(&spec(), &page);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains(STATS), "{failures:?}");
    }

    #[test]
    fn u6_the_front_ends_own_paths_are_not_judged() {
        let mut r = records();
        r.push(rec(500, "/api/plugins/x/settings", "", "boom"));
        assert_eq!(page_failures(&spec(), &record(r)), Vec::<String>::new());
    }

    #[test]
    fn u7_the_flame_graph_root_value_is_checked() {
        let p4 = |root: i64| {
            let spec = page(
                "P4",
                "/a/x".to_string(),
                vec![expect(DS_QUERY)],
                Check::FlameRoot(1_080_000_000),
            );
            let mut page = record(vec![rec(200, DS_QUERY, "{}", &flame(root))]);
            page.name = "P4".to_string();
            page_failures(&spec, &page)
        };
        assert_eq!(p4(1_080_000_000), Vec::<String>::new());
        let failures = p4(1_079_999_999);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains("1079999999"), "{failures:?}");
    }

    /// Review r3: a flame graph whose root is missing fails the check even
    /// when another answer has the expected root.
    #[test]
    fn u7b_a_flame_graph_with_no_root_fails_beside_a_good_one() {
        let spec = page(
            "P4",
            "/a/x".to_string(),
            vec![expect(DS_QUERY)],
            Check::FlameRoot(1_080_000_000),
        );
        let mut broken: Value = serde_json::from_str(&flame(1_080_000_000)).unwrap();
        broken["results"]["A"]["frames"][0]["data"]["values"][1] = json!([]);
        let mut page = record(vec![
            rec(200, DS_QUERY, "{}", &flame(1_080_000_000)),
            rec(200, DS_QUERY, "{}", &broken.to_string()),
        ]);
        page.name = "P4".to_string();
        let failures = page_failures(&spec, &page);
        // R3 refuses the answer, and so does the root check that reads it.
        assert_eq!(failures.len(), 2, "{failures:?}");
        assert!(
            failures[0].contains("R3") && failures[0].contains("column"),
            "{failures:?}"
        );
        assert!(
            failures[1].contains("R4") && failures[1].contains("column"),
            "{failures:?}"
        );
    }

    #[test]
    fn u8_a_span_query_on_the_unprofiled_span_is_one_failure() {
        let t3 = page_specs(0, b"")
            .into_iter()
            .find(|s| s.name.starts_with("T3"))
            .expect("T3");
        let request = r#"{"queries":[{"refId":"span-flamegraph-refId","spanSelector":["1111111111111111"]}]}"#;
        let mut page = record(vec![rec(200, DS_QUERY, request, DS_OK)]);
        page.name = t3.name.clone();
        let failures = page_failures(&t3, &page);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains("spanSelector"), "{failures:?}");
    }

    #[test]
    fn u9_a_text_not_visible_is_one_failure_naming_its_scope_and_text() {
        let mut spec = spec();
        spec.visible = vec![cores("svc-a", "0.0120")];
        let mut page = record(records());
        page.visible = vec![Shown {
            scope: Some(panel("svc-a")),
            text: "avg cpu = 0.0120 cores / max = 0.0120 cores".to_string(),
            shown: false,
        }];
        let failures = page_failures(&spec, &page);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains(&panel("svc-a")), "{failures:?}");
        assert!(
            failures[0].contains("avg cpu = 0.0120 cores"),
            "{failures:?}"
        );
    }

    #[test]
    fn base64_matches_the_standard_alphabet() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"foob"), "Zm9vYg==");
    }
}
