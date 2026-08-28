//! Issue #461 — the OTLP metrics naming differential.
//!
//! One OTLP/JSON body is pushed **byte-identically** to PulsusDB's
//! `POST /v1/metrics` and to the reference Prometheus's
//! `POST /api/v1/otlp/v1/metrics`, and the series each backend stores are
//! compared as sets. This is the only leg that reaches PulsusDB's OTLP
//! **metrics receiver** at all: the existing `metrics_differential`
//! (`crate::metrics`) fans metrics out through two `prometheusremotewrite`
//! exporters (`deploy/e2e/otel-config.single.yaml`), so both backends
//! there receive already-translated remote write and `otlp_metrics::parse`
//! is never invoked. That is why the naming bug survived a green
//! differential.
//!
//! **What this leg covers**, of the eleven transformations Prometheus
//! v3.13.0's OTLP receiver applies: metric-name escaping, the unit suffix,
//! `_total`, `_ratio`, label-name sanitization with the `key`/`key_`
//! prefix, the `;` collision merge, empty-value delete, `job`/`instance`
//! synthesis, resource attributes NOT promoted, `target_info`, and scope
//! metadata NOT promoted. All eleven are visible in the shipped payload.
//!
//! **What it does not cover, and what does instead.** The six whole-request
//! name/label rejections — the reference answers `500` with a text body, so
//! a series-set comparison cannot express them; `pulsus-write`'s
//! `otlp_prom_translation::reference_rejections_are_whole_request_400`
//! owns those. The three non-default translation strategies and scope
//! promotion — each needs its own Prometheus configuration and this stack
//! runs one container; the captured corpus owns those. `target_info`
//! cadence over a multi-interval span, the 4096-sample cap, duplicate raw
//! attribute keys, the leading-digit rule and the token-drop rule — kept
//! out to keep the PR-tier payload small; the corpus owns those too.
//! Metadata `unit` translation has **no live oracle at all**: the
//! reference's `/api/v1/metadata` answers `{"status":"success","data":{}}`
//! for OTLP-ingested metrics, so that rule is asserted hermetically
//! against its source citation and nothing here implies otherwise.
//!
//! **Three gate properties this leg must have, each of which a real false
//! pass has required** (issue #461 plan Δ4/Δ10):
//!
//! 1. **Push status is asserted on both backends.** Posting to a base URL
//!    instead of the receiver path answers `405` on Prometheus and `404`
//!    on PulsusDB, after which comparing nothing to nothing is "equal".
//! 2. **The validity gate is direction-neutral, an EQUALITY, and reported
//!    as its own verdict** before the set comparison. A floor passes the
//!    exact loss it exists to catch when both sides lose the same series;
//!    measured, two references with one metric removed from both give
//!    `A=8 B=8 sets_equal=true`, which a floor of 8 admits.
//! 3. **Run scoping is by a data-point attribute, not by `job`.** `job` is
//!    `shop/<service>`, so a `{job=~"<run>.*"}` selector is fully anchored
//!    on both engines and matches nothing. `target_info` is resource-level
//!    and carries no `run_id`, so it is gated separately — as its own
//!    verdict, because its *absence* is the defect this issue is about.
//!
//! **The reference's admission window is head-relative**: it refuses a
//! sample older than `head.maxTime - 60 min`, so every timestamp here is
//! computed from run time and the whole fixture spans 11 minutes. The
//! resource's `service.name` varies per run for the same reason — see
//! `otlp-reference-admission-window` in
//! `docs/benchmarks/metrics-differential-ledger.md`.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::corpus::Scale;
use crate::harness::{poll_until, query_request_timeout};
use crate::metrics::unique_id;
use crate::scenarios::Ctx;

const FIXTURE_PATH: &str = "metrics/otlp-naming-differential.json";

/// This leg is single-variant and PR-tier only — one push and three reads
/// against a fixed 11-minute fixture — so it always uses the strict
/// [`Scale::Ci`] request budget. `PULSUS_E2E_METRICS_SCALE=full` scales
/// `metrics_differential`'s corpus, not this one.
const SCALE: Scale = Scale::Ci;

/// Both backends store synchronously here (PulsusDB's `/v1/metrics` is
/// sync without `X-Pulsus-Async`; Prometheus appends before answering),
/// but the read side still polls: PulsusDB's series are read back through
/// ClickHouse, whose `_dist` fan-out on the cluster variant is eventually
/// consistent.
const VISIBILITY_POLL_TIMEOUT: Duration = Duration::from_secs(90);
const VISIBILITY_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// 15 s between data points — the step the Grafana panel on issue #461
/// used, and the step the `rate_query` expectation was captured at.
const STEP_S: i64 = 15;
/// The queried window, in seconds. Ten minutes keeps the whole fixture,
/// including the extra minute of history below, inside the reference's
/// 60-minute head-relative admission floor.
const WINDOW_S: i64 = 600;
/// One extra `[1m]` of history before `start`, so the first step of the
/// range query already has a full rate window.
const PREROLL_S: i64 = 60;

#[derive(Debug, Deserialize)]
struct Fixture {
    expected_metric_series: Vec<BTreeMap<String, String>>,
    expected_target_info: Vec<BTreeMap<String, String>>,
    rate_query: RateQuery,
}

#[derive(Debug, Deserialize)]
struct RateQuery {
    expr: String,
    step: i64,
    series: usize,
    metric: BTreeMap<String, String>,
    points: usize,
    value: String,
}

fn load_fixture(ctx: &Ctx) -> Result<Fixture> {
    let path = ctx.fixtures_dir.join(FIXTURE_PATH);
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read fixture {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("fixture {} was not valid JSON", path.display()))
}

fn substitute(template: &BTreeMap<String, String>, run_id: &str, service: &str) -> Value {
    let mapped: serde_json::Map<String, Value> = template
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                Value::String(v.replace("{R}", run_id).replace("{SVC}", service)),
            )
        })
        .collect();
    Value::Object(mapped)
}

fn attr(key: &str, value: &str) -> Value {
    json!({"key": key, "value": {"stringValue": value}})
}

/// The shared payload. Every metric here is chosen because its stored name
/// or label set differs from the wire under Prometheus v3.13.0's default
/// translation; the manifest beside this file records what each becomes.
fn payload(run_id: &str, service: &str, first_ms: i64, last_ms: i64) -> Value {
    let point_count = ((last_ms - first_ms) / (STEP_S * 1000)) + 1;
    let counter_points: Vec<Value> = (0..point_count)
        .map(|i| {
            let ms = first_ms + i * STEP_S * 1000;
            json!({
                // The counter advances by exactly 1 per second, so
                // `rate()` over it is 1/s before extrapolation.
                "asDouble": ((ms - first_ms) / 1000) as f64,
                "timeUnixNano": (ms * 1_000_000).to_string(),
                "attributes": [attr("region", "eu-west-1"), attr("run_id", run_id)],
            })
        })
        .collect();
    let last_ns = (last_ms * 1_000_000).to_string();
    let run_attr = [attr("run_id", run_id)];

    json!({"resourceMetrics": [{
        "resource": {"attributes": [
            attr("service.name", service),
            attr("service.namespace", "shop"),
            attr("service.instance.id", "pod-7"),
            attr("deployment.environment", "prod"),
            attr("host.arch", "amd64"),
        ]},
        "scopeMetrics": [{
            "scope": {"name": "gen", "version": "1.2.3"},
            "metrics": [
                {"name": "app.checkout.request.count", "unit": "1", "sum": {
                    "dataPoints": counter_points,
                    "aggregationTemporality": 2, "isMonotonic": true}},
                {"name": "queue.bytes.sent", "unit": "By", "sum": {
                    "dataPoints": [{"asDouble": 7.0, "timeUnixNano": last_ns,
                                    "attributes": run_attr}],
                    "aggregationTemporality": 2, "isMonotonic": true}},
                {"name": "http.server.duration", "unit": "s", "histogram": {
                    "dataPoints": [{"timeUnixNano": last_ns, "attributes": run_attr,
                                    "count": "3", "sum": 1.5,
                                    "bucketCounts": ["1", "1", "1"],
                                    "explicitBounds": [0.1, 0.5]}],
                    "aggregationTemporality": 2}},
                {"name": "cpu.utilization", "unit": "1", "gauge": {
                    "dataPoints": [{"asDouble": 0.25, "timeUnixNano": last_ns,
                                    "attributes": run_attr}]}},
                {"name": "lbl.a", "unit": "", "gauge": {
                    "dataPoints": [{"asDouble": 1.0, "timeUnixNano": last_ns, "attributes": [
                        attr("9lives", "cat"), attr("_priv", "p"),
                        attr("a.b", "dot"), attr("a_b", "under"), attr("a-b", "dash"),
                        attr("dropme", ""), attr("run_id", run_id),
                    ]}]}},
            ],
        }],
    }]})
}

async fn push(ctx: &Ctx, base_url: &str, path: &str, body: &Value) -> Result<()> {
    let url = format!("{base_url}{path}");
    let response = ctx
        .http
        .post(&url)
        .json(body)
        .timeout(query_request_timeout(SCALE))
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if status != reqwest::StatusCode::OK {
        bail!("POST {url} answered {status}, body {text:?} — expected 200");
    }
    Ok(())
}

/// `GET /api/v1/series` on either backend, returning each series as a
/// sorted label map (with `__name__` kept, since it is exactly what this
/// leg is about).
async fn series(
    ctx: &Ctx,
    base_url: &str,
    selector: &str,
    start_s: i64,
    end_s: i64,
) -> Result<Vec<Value>> {
    let url = format!("{base_url}/api/v1/series");
    let response = ctx
        .http
        .get(&url)
        .query(&[
            ("match[]", selector.to_string()),
            ("start", start_s.to_string()),
            ("end", end_s.to_string()),
        ])
        .timeout(query_request_timeout(SCALE))
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .with_context(|| format!("GET {url} did not answer JSON"))?;
    if status != reqwest::StatusCode::OK {
        bail!("GET {url} {selector} answered {status}: {body}");
    }
    let mut out: Vec<Value> = body["data"]
        .as_array()
        .with_context(|| format!("GET {url} answered no data array: {body}"))?
        .clone();
    out.sort_by_key(|v| v.to_string());
    Ok(out)
}

fn render(set: &[Value]) -> String {
    set.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\n    ")
}

/// The naming differential. See this module's doc comment.
pub async fn otlp_metrics_naming_differential(ctx: &Ctx) -> Result<()> {
    let fixture = load_fixture(ctx)?;
    let unique = format!("{:x}", unique_id()?);
    let run_id = format!("e2e461{unique}");
    // Vary the resource per run: `target_info` is resource-level and
    // carries no `run_id`, so a fixed `service.name` makes the second run
    // of the day collide with the first as `400 out of order sample`.
    let service = format!("firehose-{unique}");

    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs() as i64;
    // Anchor a minute behind now and snap to the step, so every point is
    // in the past on both backends and the range query's grid lines up.
    let end_s = ((now_s - 60) / STEP_S) * STEP_S;
    let start_s = end_s - WINDOW_S;
    let first_ms = (start_s - PREROLL_S) * 1000;
    let last_ms = end_s * 1000;

    let body = payload(&run_id, &service, first_ms, last_ms);

    // (1) Push status on BOTH backends, asserted. A 404/405 here would
    // otherwise present as two empty, equal result sets.
    push(ctx, &ctx.prometheus_url, "/api/v1/otlp/v1/metrics", &body).await?;
    push(ctx, &ctx.base_url, "/v1/metrics", &body).await?;

    let expected_metric: Vec<Value> = {
        let mut v: Vec<Value> = fixture
            .expected_metric_series
            .iter()
            .map(|t| substitute(t, &run_id, &service))
            .collect();
        v.sort_by_key(|x| x.to_string());
        v
    };
    let expected_target: Vec<Value> = {
        let mut v: Vec<Value> = fixture
            .expected_target_info
            .iter()
            .map(|t| substitute(t, &run_id, &service))
            .collect();
        v.sort_by_key(|x| x.to_string());
        v
    };

    let metric_selector = format!("{{run_id=\"{run_id}\"}}");
    let target_selector = format!("{{__name__=\"target_info\", job=\"shop/{service}\"}}");
    let read_start = start_s - PREROLL_S - 60;
    let read_end = end_s + 60;

    // (2) The direction-neutral validity gate, an EQUALITY against the
    // fixture manifest, evaluated per backend and reported as its own
    // verdict before anything is compared across backends.
    let manifest_count = expected_metric.len();
    let (prom_metric, pulsus_metric) = poll_until(
        VISIBILITY_POLL_TIMEOUT,
        VISIBILITY_POLL_INTERVAL,
        || async {
            let prom = series(
                ctx,
                &ctx.prometheus_url,
                &metric_selector,
                read_start,
                read_end,
            )
            .await?;
            let pulsus = series(ctx, &ctx.base_url, &metric_selector, read_start, read_end).await?;
            Ok(
                (prom.len() == manifest_count && pulsus.len() == manifest_count)
                    .then_some((prom, pulsus)),
            )
        },
    )
    .await
    .with_context(|| {
        format!(
            "[validity gate] each backend must independently store exactly {manifest_count} \
             metric series carrying run_id={run_id}"
        )
    })?;

    let (prom_target, pulsus_target) = poll_until(
        VISIBILITY_POLL_TIMEOUT,
        VISIBILITY_POLL_INTERVAL,
        || async {
            let prom = series(
                ctx,
                &ctx.prometheus_url,
                &target_selector,
                read_start,
                read_end,
            )
            .await?;
            let pulsus = series(ctx, &ctx.base_url, &target_selector, read_start, read_end).await?;
            Ok(
                (prom.len() == expected_target.len() && pulsus.len() == expected_target.len())
                    .then_some((prom, pulsus)),
            )
        },
    )
    .await
    .with_context(|| {
        format!(
            "[target_info gate] each backend must store exactly {} target_info series for \
             job=shop/{service} — its ABSENCE on PulsusDB is the defect issue #461 reports",
            expected_target.len()
        )
    })?;

    // (3) The set comparisons, only now that both sides are known complete.
    if prom_metric != pulsus_metric {
        bail!(
            "metric series sets differ\n  only prometheus:\n    {}\n  only pulsusdb:\n    {}",
            render(
                &prom_metric
                    .iter()
                    .filter(|s| !pulsus_metric.contains(s))
                    .cloned()
                    .collect::<Vec<_>>()
            ),
            render(
                &pulsus_metric
                    .iter()
                    .filter(|s| !prom_metric.contains(s))
                    .cloned()
                    .collect::<Vec<_>>()
            )
        );
    }
    if prom_metric != expected_metric {
        bail!(
            "both backends agree but disagree with the fixture manifest\n  stored:\n    {}\n  \
             manifest:\n    {}",
            render(&prom_metric),
            render(&expected_metric)
        );
    }
    if prom_target != pulsus_target || prom_target != expected_target {
        bail!(
            "target_info sets differ\n  prometheus:\n    {}\n  pulsusdb:\n    {}\n  manifest:\n    {}",
            render(&prom_target),
            render(&pulsus_target),
            render(&expected_target)
        );
    }

    // (4) The query the Grafana panel on issue #461 sent, verbatim.
    //
    // Polled, unlike the series reads: PulsusDB's PromQL planner resolves
    // series through the time-aware label cache, which lags the write by
    // its refresh cadence, so a range query can answer an empty matrix for
    // series `/api/v1/series` already reports. Measured against a debug
    // binary on a private ClickHouse 26.3.17: the reference answered on
    // the first attempt, PulsusDB on the 25th (one attempt per second).
    // The poll makes that a latency, not a failure; a bare single-shot
    // request here would be a flake generator.
    let expr = fixture.rate_query.expr.replace("{R}", &run_id);
    let metric = substitute(&fixture.rate_query.metric, &run_id, &service);
    for (label, base_url) in [
        ("prometheus", ctx.prometheus_url.as_str()),
        ("pulsusdb", ctx.base_url.as_str()),
    ] {
        poll_until(
            VISIBILITY_POLL_TIMEOUT,
            VISIBILITY_POLL_INTERVAL,
            || async {
                let url = format!("{base_url}/api/v1/query_range");
                let response = ctx
                    .http
                    .post(&url)
                    .form(&[
                        ("query", expr.clone()),
                        ("start", start_s.to_string()),
                        ("end", end_s.to_string()),
                        ("step", fixture.rate_query.step.to_string()),
                    ])
                    .timeout(query_request_timeout(SCALE))
                    .send()
                    .await
                    .with_context(|| format!("POST {url} on {label}"))?;
                let status = response.status();
                let body: Value = response
                    .json()
                    .await
                    .with_context(|| format!("POST {url} on {label} did not answer JSON"))?;
                if status != reqwest::StatusCode::OK {
                    bail!("POST {url} on {label} answered {status}: {body}");
                }
                let result = body["data"]["result"]
                    .as_array()
                    .with_context(|| format!("{label}: no result array in {body}"))?;
                if result.len() != fixture.rate_query.series {
                    // Not yet visible to the planner — retried, and reported
                    // with the whole body once the deadline passes.
                    return Ok(None);
                }
                if result[0]["metric"] != metric {
                    bail!(
                        "{label}: expected metric {metric}, got {}",
                        result[0]["metric"]
                    );
                }
                let values = result[0]["values"]
                    .as_array()
                    .with_context(|| format!("{label}: no values array in {body}"))?;
                if values.len() != fixture.rate_query.points {
                    return Ok(None);
                }
                let distinct: std::collections::BTreeSet<&str> =
                    values.iter().filter_map(|v| v[1].as_str()).collect();
                if distinct != std::collections::BTreeSet::from([fixture.rate_query.value.as_str()])
                {
                    bail!(
                        "{label}: expected every point of {expr} to be {:?}, got {distinct:?}",
                        fixture.rate_query.value
                    );
                }
                Ok(Some(()))
            },
        )
        .await
        .with_context(|| {
            format!(
                "[rate gate] {label}: {expr} must answer {} series of {} points",
                fixture.rate_query.series, fixture.rate_query.points
            )
        })?;
    }

    Ok(())
}
