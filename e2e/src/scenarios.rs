//! Scenario registry (issue #7 architect plan). `test/fixtures/README.md`
//! documents the contract: adding a scenario is one
//! `test/fixtures/<area>/<name>.*` fixture file, one assertion fn here,
//! and one `SCENARIOS` entry. The M0 skeleton shipped two ops-only
//! scenarios; `logs_roundtrip` (issue #15) is the M1 milestone gate: known
//! OTLP logs pushed through a real collector, then asserted back through
//! every native `/api/logs/v1` endpoint.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use serde::Deserialize;

use crate::engine::Compose;
use crate::harness::poll_until;

/// Which compose variant a [`Scenario`] runs under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Variant {
    Single,
    Cluster,
}

/// Per-run context handed to every scenario: an HTTP client bound to the
/// stack's published `:3100`, which variant is running, and the fixtures
/// directory scenarios load expected data from. `collector_url` and
/// `compose` (issue #15 architect plan): the collector's published
/// OTLP/HTTP base URL for pushing ingest fixtures, and a handle onto the
/// running compose stack for the cluster leg's `compose exec`-based
/// shard-local sanity check.
pub struct Ctx {
    pub http: reqwest::Client,
    pub base_url: String,
    pub collector_url: String,
    /// The reference Prometheus's published base URL (issue #33 architect
    /// plan) — `crate::metrics::metrics_differential`'s oracle backend.
    pub prometheus_url: String,
    /// The reference Tempo's published base URL (issue #60 architect
    /// plan) — `crate::traces::traces_differential`'s oracle backend,
    /// single-variant only (the cluster overlay ships no `tempo`
    /// service; only single-variant scenarios dereference this).
    pub tempo_url: String,
    pub variant: Variant,
    pub fixtures_dir: PathBuf,
    pub compose: Compose,
}

impl Ctx {
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

/// A scenario's entry point: a plain fn pointer returning a boxed future.
/// No trait, no `async-trait` — a bare fn pointer is enough for a
/// `&'static [Scenario]` registry. Named as a type alias purely to keep
/// `Scenario` readable (same type the architect plan's interface
/// specifies, `fn(&Ctx) -> Pin<Box<dyn Future<Output = Result<()>> + '_>>`).
pub type ScenarioFn = fn(&Ctx) -> Pin<Box<dyn Future<Output = Result<()>> + '_>>;

/// One scenario: a name (for logging/diagnostics), the variants it applies
/// to, and its [`ScenarioFn`].
pub struct Scenario {
    pub name: &'static str,
    pub variants: &'static [Variant],
    pub run: ScenarioFn,
}

pub const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "readiness",
        variants: &[Variant::Single, Variant::Cluster],
        run: |ctx| Box::pin(readiness(ctx)),
    },
    Scenario {
        name: "buildinfo_roundtrip",
        variants: &[Variant::Single, Variant::Cluster],
        run: |ctx| Box::pin(buildinfo_roundtrip(ctx)),
    },
    Scenario {
        name: "logs_roundtrip",
        // Both variants (issue #15 architect plan): the cluster leg adds a
        // shard-local sanity check on top of the same fixtures/assertions.
        // Registered before `grafana_loki_compat` so the single-variant
        // leg's Loki-compat check below has real data to query.
        variants: &[Variant::Single, Variant::Cluster],
        run: |ctx| Box::pin(logs_roundtrip(ctx)),
    },
    Scenario {
        name: "grafana_loki_compat",
        // Single-variant only (architect plan): the `grafana` service and
        // its Loki-datasource provisioning ship only in
        // `deploy/e2e/compose.single.yaml`.
        variants: &[Variant::Single],
        run: |ctx| Box::pin(grafana_loki_compat(ctx)),
    },
    Scenario {
        name: "metrics_differential",
        // Both variants (issue #33 architect plan): the cluster leg's
        // `poll_until`-based completeness pre-check absorbs `_dist`
        // eventual-consistency lag the same way `logs_roundtrip` does —
        // no separate cluster-only assertions needed. Registered after
        // `logs_roundtrip`/`grafana_loki_compat`: independent data via its
        // own `run_id`, so ordering relative to them is immaterial.
        variants: &[Variant::Single, Variant::Cluster],
        run: |ctx| Box::pin(crate::metrics::metrics_differential(ctx)),
    },
    Scenario {
        name: "traces_roundtrip",
        // Both variants (issue #60 architect plan, mirroring
        // `logs_roundtrip`): the M4 DoD's "collector traces pipeline
        // lands and is searchable" through every native endpoint, with
        // the cluster leg adding the shard-local `trace_spans`
        // count-sum check. Independent data via its own `run_id`.
        variants: &[Variant::Single, Variant::Cluster],
        run: |ctx| Box::pin(crate::traces::traces_roundtrip(ctx)),
    },
    Scenario {
        name: "traces_differential",
        // Single-variant only (issue #60 task-manager adjudication 1):
        // set-equivalence correctness is topology-invariant, multi-shard
        // fan-out is already Tier-1-gated by #57's cluster evidence plus
        // the cluster `traces_roundtrip` leg above, and the reference
        // Tempo container ships only in `deploy/e2e/compose.single.yaml`.
        variants: &[Variant::Single],
        run: |ctx| Box::pin(crate::traces::traces_differential(ctx)),
    },
];

/// `GET /ready` is already gated on by the harness's own polling
/// (`harness::wait_ready`) before any scenario runs; this scenario
/// re-asserts 200 as the skeleton milestone's trivially-green per-variant
/// case (docs/api.md §7).
async fn readiness(ctx: &Ctx) -> Result<()> {
    println!("pulsus-e2e:   readiness check for {:?}", ctx.variant);
    let res = ctx
        .http
        .get(ctx.url("/ready"))
        .send()
        .await
        .context("GET /ready failed")?;
    if !res.status().is_success() {
        bail!("GET /ready returned {}", res.status());
    }
    Ok(())
}

/// `GET /buildinfo` (docs/api.md §7): 200, plus every field named in
/// `test/fixtures/ops/buildinfo.fields.json` present and non-empty —
/// exercises the fixture-file contract itself, not just the endpoint.
async fn buildinfo_roundtrip(ctx: &Ctx) -> Result<()> {
    let fields = load_fixture_fields(&ctx.fixtures_dir.join("ops/buildinfo.fields.json"))?;

    let res = ctx
        .http
        .get(ctx.url("/buildinfo"))
        .send()
        .await
        .context("GET /buildinfo failed")?;
    if !res.status().is_success() {
        bail!("GET /buildinfo returned {}", res.status());
    }
    let body: serde_json::Value = res
        .json()
        .await
        .context("GET /buildinfo body was not JSON")?;

    for field in &fields {
        let present = body
            .get(field)
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty());
        if !present {
            bail!("GET /buildinfo missing or empty field {field:?} in {body}");
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// logs_roundtrip (issue #15): collector -> pulsusdb -> query round trip.
// ---------------------------------------------------------------------

/// `test/fixtures/logs/roundtrip.json`'s shape (see
/// `test/fixtures/README.md`): one entry per stream, each carrying its
/// resource/scope identity and a handful of log lines timestamped
/// `base_ns + ts_offset_ns` (`base_ns` computed at run time, per the
/// architect plan — never a fixed past date, so the fixture never races
/// `PULSUS_RETENTION_DAYS`).
#[derive(Debug, Deserialize)]
struct RoundtripFixture {
    streams: Vec<RoundtripStream>,
}

#[derive(Debug, Deserialize)]
struct RoundtripStream {
    service: String,
    #[serde(default)]
    scope_name: Option<String>,
    #[serde(default)]
    scope_version: Option<String>,
    #[serde(default)]
    resource_attrs: BTreeMap<String, String>,
    #[serde(default)]
    scope_attrs: BTreeMap<String, String>,
    lines: Vec<RoundtripLine>,
}

#[derive(Debug, Deserialize)]
struct RoundtripLine {
    ts_offset_ns: i64,
    body: String,
}

const ROUNDTRIP_FIXTURE: &str = "logs/roundtrip.json";
const ROUNDTRIP_POLL_TIMEOUT: Duration = Duration::from_secs(60);
const ROUNDTRIP_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Collector readiness poll bounds (CI regression fix, issue #15: same
/// class of bug as `grafana_loki_compat`'s missing readiness wait — this
/// scenario runs right after `pulsus /ready` (gated by the harness), but
/// nothing waited for the **collector**'s own OTLP/HTTP receiver to start
/// listening, so a slow-starting collector on a loaded CI runner hit
/// "connection reset by peer" on the very first `POST /v1/logs`). Matches
/// [`GRAFANA_READY_POLL_TIMEOUT`]'s magnitude (90s) — the same
/// poll-until discipline the harness uses everywhere else.
const COLLECTOR_READY_POLL_TIMEOUT: Duration = Duration::from_secs(90);
const COLLECTOR_READY_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// The query window every assertion below brackets `base_ns` with — wide
/// enough that none of the fixture's `ts_offset_ns` values (all small,
/// recent offsets) can fall outside it.
const ROUNDTRIP_WINDOW_NS: i64 = 3_600_000_000_000; // 1h
/// The database the schema controller creates by default
/// (`pulsus_config::ClickHouseConfig::database`'s default) — neither
/// compose overlay overrides `CLICKHOUSE_DB`, so this is exactly what both
/// variants' `pulsusdb` targets.
const PULSUS_DB: &str = "pulsus";

/// The M1 milestone gate (docs/features.md §7): pushes the round-trip
/// fixture as an OTLP/HTTP **JSON** `ExportLogsServiceRequest` into the
/// collector's `otlp` receiver, then polls-until-visible and asserts every
/// native `/api/logs/v1` endpoint round-trips it exactly — ns timestamps
/// verbatim, bodies byte-exact, labels normalized, `service` extracted.
/// The collector re-exports as protobuf (`otlphttp` exporter) to
/// `POST /v1/logs`, so this also exercises the real wire path issue #15
/// wires up, not just the parser (already covered by `pulsus-write`'s own
/// fixture tests).
async fn logs_roundtrip(ctx: &Ctx) -> Result<()> {
    let fixture = load_roundtrip_fixture(ctx)?;
    let base_ns = now_unix_nanos()?;
    // A per-run marker embedded in every line's body (code review finding,
    // issue #15 re-review): `base_ns` is nanosecond-precision and computed
    // fresh per invocation, so tagging every body with it makes this run's
    // rows structurally distinguishable from anything a prior/concurrent
    // run (or leftover, un-torn-down stack data) could have left in the
    // same time window — poll predicates below key off the marker, not a
    // bare entry *count*, so a stale/unrelated row with a coincidentally
    // matching count can never be mistaken for this run's data.
    let marker = run_marker(base_ns);

    let payload = build_otlp_export_request(&fixture, base_ns, &marker);
    // Poll-until-listening (CI regression fix, issue #15): retries on
    // transport-level failures only (connection refused/reset — the
    // collector's OTLP/HTTP receiver isn't listening yet), stopping the
    // instant *any* HTTP response comes back, success or not. Safe to
    // resend the identical payload on a transport failure: a connection
    // that was never established processed zero bytes server-side, so
    // this can never double-ingest. `res.status()` is still checked below
    // exactly as before — reaching the collector at all doesn't imply the
    // export itself succeeded.
    let res = poll_until(
        COLLECTOR_READY_POLL_TIMEOUT,
        COLLECTOR_READY_POLL_INTERVAL,
        || post_otlp_logs(ctx, &payload),
    )
    .await
    .context("collector otlp/v1/logs endpoint never accepted a connection")?;
    if !res.status().is_success() {
        bail!("collector otlp/v1/logs export returned {}", res.status());
    }

    let start_ns = base_ns - ROUNDTRIP_WINDOW_NS;
    let end_ns = base_ns + ROUNDTRIP_WINDOW_NS;

    let mut total_lines = 0usize;
    for stream in &fixture.streams {
        total_lines += stream.lines.len();
        assert_stream_roundtrip(ctx, stream, base_ns, &marker, start_ns, end_ns).await?;
    }

    assert_labels_and_series(ctx, &fixture, base_ns, &marker, start_ns, end_ns).await?;

    if ctx.variant == Variant::Cluster {
        assert_shard_local_row_counts(ctx, total_lines).await?;
    }

    Ok(())
}

/// This run's unique body marker (code review finding, issue #15
/// re-review) — see [`logs_roundtrip`]'s doc comment for why.
fn run_marker(base_ns: i64) -> String {
    format!("e2e-run={base_ns}")
}

/// Appends [`run_marker`]'s tag to `body`, the same way on both the
/// ingest side ([`build_otlp_export_request`]) and the assertion side
/// ([`assert_stream_roundtrip`]) — a single shared function so the two
/// can never drift.
fn tagged_body(body: &str, marker: &str) -> String {
    format!("{body} [{marker}]")
}

fn load_roundtrip_fixture(ctx: &Ctx) -> Result<RoundtripFixture> {
    let path = ctx.fixtures_dir.join(ROUNDTRIP_FIXTURE);
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read fixture {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("fixture {} was not valid JSON", path.display()))
}

fn now_unix_nanos() -> Result<i64> {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(dur.as_nanos()).context("current time does not fit in i64 nanoseconds")
}

/// One `POST {collector_url}/v1/logs` attempt (CI regression fix, issue
/// #15): `Ok(Some(response))` once the request reaches the collector at
/// all (any HTTP response, success or not — the caller still checks
/// `response.status()`), `Ok(None)` on a transport-level failure (the
/// collector's OTLP/HTTP receiver isn't listening yet) — the [`poll_until`]
/// condition [`logs_roundtrip`] polls on. Neither `deploy/e2e/otel-config
/// .single.yaml` nor `.cluster.yaml` wires the `health_check` extension
/// (checked: no `extensions:`/`service.extensions` block in either), so
/// there is no cheaper dedicated readiness endpoint to poll instead — the
/// real OTLP endpoint is the correct, only signal available.
async fn post_otlp_logs(
    ctx: &Ctx,
    payload: &serde_json::Value,
) -> Result<Option<reqwest::Response>> {
    // `?` (not `.ok()`-and-discard): `poll_until` tolerates an `Err` return
    // exactly like `Ok(None)` (retried until the deadline), but surfaces
    // the underlying `reqwest::Error` in the final failure's context if
    // the collector never comes up — preserves the transport-failure
    // detail rather than collapsing it to a generic timeout message.
    let res = ctx
        .http
        .post(format!("{}/v1/logs", ctx.collector_url))
        .json(payload)
        .send()
        .await?;
    Ok(Some(res))
}

fn otlp_key_value(key: &str, value: &str) -> serde_json::Value {
    serde_json::json!({ "key": key, "value": { "stringValue": value } })
}

/// The normalized label name [`run_marker`]'s value is injected under, on
/// every stream (code review finding, issue #15 second re-review): unlike
/// log bodies, `series`/`labels`/metric results carry no body text to
/// substring-filter — a run-scoped **label**, present on every one of this
/// run's streams and matched directly in the LogQL selector, is the
/// equivalent isolation primitive for those endpoints. Already a valid
/// normalized key (`[a-zA-Z0-9_]`), so [`normalize_label_key`] is a no-op
/// on it — no collision risk with any existing fixture label.
const RUN_ID_LABEL: &str = "run_id";

/// Builds an OTLP/HTTP-**JSON** `ExportLogsServiceRequest` (the collector's
/// `otlp` receiver accepts JSON per the OTLP/HTTP spec) — one `resourceLogs`
/// entry per fixture stream, each with exactly one `scopeLogs` (`scope`
/// present iff the fixture set `scope_name`, matching
/// `pulsus-write::protocols::otlp_logs`'s "absent scopes emit nothing"
/// contract) carrying every fixture line as a `logRecords` entry, its body
/// tagged with [`run_marker`] (code review finding, issue #15 re-review).
/// Every stream also gets a [`RUN_ID_LABEL`] resource attribute carrying
/// `marker` verbatim (second re-review finding), so label-only endpoints
/// (`series`, metric `count_over_time`) can isolate this run's data by
/// selector, not just by body content.
fn build_otlp_export_request(
    fixture: &RoundtripFixture,
    base_ns: i64,
    marker: &str,
) -> serde_json::Value {
    let resource_logs: Vec<serde_json::Value> = fixture
        .streams
        .iter()
        .map(|stream| {
            let mut resource_attrs = vec![
                otlp_key_value("service.name", &stream.service),
                otlp_key_value(RUN_ID_LABEL, marker),
            ];
            for (key, value) in &stream.resource_attrs {
                resource_attrs.push(otlp_key_value(key, value));
            }

            let log_records: Vec<serde_json::Value> = stream
                .lines
                .iter()
                .map(|line| {
                    serde_json::json!({
                        "timeUnixNano": (base_ns + line.ts_offset_ns).to_string(),
                        "body": { "stringValue": tagged_body(&line.body, marker) },
                    })
                })
                .collect();

            let mut scope_logs = serde_json::json!({ "logRecords": log_records });
            if let Some(name) = &stream.scope_name {
                let mut scope = serde_json::json!({
                    "name": name,
                    "version": stream.scope_version.clone().unwrap_or_default(),
                });
                if !stream.scope_attrs.is_empty() {
                    let attrs: Vec<serde_json::Value> = stream
                        .scope_attrs
                        .iter()
                        .map(|(key, value)| otlp_key_value(key, value))
                        .collect();
                    scope["attributes"] = serde_json::Value::Array(attrs);
                }
                scope_logs["scope"] = scope;
            }

            serde_json::json!({
                "resource": { "attributes": resource_attrs },
                "scopeLogs": [scope_logs],
            })
        })
        .collect();

    serde_json::json!({ "resourceLogs": resource_logs })
}

/// Normalizes a label key the same way the writer does
/// (`pulsus_model::LabelSet::from_normalized`, docs/architecture.md §2.3):
/// characters outside `[a-zA-Z0-9_]` become `_`.
fn normalize_label_key(key: &str) -> String {
    key.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// One `query_range` attempt for `selector`: `Ok(Some(body))` once the
/// count of entries whose body carries `marker` reaches `expected_entries`,
/// `Ok(None)` otherwise — the [`poll_until`] condition every per-stream
/// round-trip assertion below polls on. Counting *marker-tagged* entries
/// rather than trusting `data.stats.entries` verbatim is load-bearing
/// (code review finding, issue #15 re-review): `stats.entries` reflects
/// *every* matched row in the window, so a stale/unrelated row from a
/// prior run (or a stack left up via `--keep`) could otherwise satisfy the
/// target count without this run's actual data ever having landed.
async fn query_range_entries(
    ctx: &Ctx,
    selector: &str,
    marker: &str,
    start_ns: i64,
    end_ns: i64,
    expected_entries: usize,
) -> Result<Option<serde_json::Value>> {
    let start_s = start_ns.to_string();
    let end_s = end_ns.to_string();
    let res = ctx
        .http
        .get(ctx.url("/api/logs/v1/query_range"))
        .query(&[
            ("query", selector),
            ("start", start_s.as_str()),
            ("end", end_s.as_str()),
            ("limit", "1000"),
        ])
        .send()
        .await
        .context("GET query_range failed")?;
    if !res.status().is_success() {
        bail!("query_range returned {}", res.status());
    }
    let body: serde_json::Value = res.json().await.context("query_range body was not JSON")?;
    let marker_entries = body["data"]["result"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|result| result["values"].as_array())
        .flatten()
        .filter(|entry| entry[1].as_str().is_some_and(|line| line.contains(marker)))
        .count();
    Ok((marker_entries == expected_entries).then_some(body))
}

/// Polls `query_range` for `stream.service` until this run's marker-tagged
/// entries are visible, then asserts round-trip integrity (architect
/// plan): ns timestamps verbatim, bodies exact (compared as an unordered
/// set, restricted to this run's marker-tagged entries — code review
/// finding, issue #15 re-review: any incidental foreign row in the same
/// window is excluded from the comparison rather than causing a spurious
/// pass *or* fail), normalized labels present, and `service` extraction.
/// The `billing` collision-stream fixture entry exercises
/// `ParsedLogs.collisions` without crashing — this only asserts the stream
/// is ingested/queryable, never which collision value won the tie-break
/// (that is `LabelSet::from_normalized`'s frozen rule, not this scenario's
/// contract).
async fn assert_stream_roundtrip(
    ctx: &Ctx,
    stream: &RoundtripStream,
    base_ns: i64,
    marker: &str,
    start_ns: i64,
    end_ns: i64,
) -> Result<()> {
    let selector = format!(r#"{{service_name="{}"}}"#, stream.service);
    let expected_entries = stream.lines.len();

    let body = poll_until(ROUNDTRIP_POLL_TIMEOUT, ROUNDTRIP_POLL_INTERVAL, || {
        query_range_entries(ctx, &selector, marker, start_ns, end_ns, expected_entries)
    })
    .await
    .with_context(|| {
        format!(
            "stream for service {:?} never reached {expected_entries} marker-tagged entries",
            stream.service
        )
    })?;

    let results = body["data"]["result"]
        .as_array()
        .context("query_range result was not an array")?;
    // Filter to the marker-bearing stream(s) *before* enforcing the
    // one-stream cardinality check (code review re-review finding): a
    // foreign row sharing `service_name` but carrying different labels
    // (e.g. a different `env`) surfaces as its own, separate stream entry
    // in `results` — checking cardinality against the *unfiltered* array
    // would fail this run even though its own data round-tripped
    // perfectly. Every subsequent assertion below (entries, labels, scope)
    // reads from this marker-filtered set, never the raw `results`.
    let marker_results: Vec<&serde_json::Value> = results
        .iter()
        .filter(|r| {
            r["values"].as_array().is_some_and(|values| {
                values
                    .iter()
                    .any(|entry| entry[1].as_str().is_some_and(|line| line.contains(marker)))
            })
        })
        .collect();
    if marker_results.len() != 1 {
        bail!(
            "expected exactly one matched stream carrying this run's marker for service {:?}, \
             got {} (out of {} total matched streams before marker filtering)",
            stream.service,
            marker_results.len(),
            results.len()
        );
    }
    let result = marker_results[0];

    let expected: std::collections::BTreeSet<(String, String)> = stream
        .lines
        .iter()
        .map(|line| {
            (
                (base_ns + line.ts_offset_ns).to_string(),
                tagged_body(&line.body, marker),
            )
        })
        .collect();
    // Restricted to this run's marker-tagged entries (code review finding):
    // any foreign row sharing the window/selector is excluded here rather
    // than corrupting the comparison either way.
    let actual: std::collections::BTreeSet<(String, String)> = result["values"]
        .as_array()
        .context("stream result missing a values array")?
        .iter()
        .map(|entry| {
            let ts = entry[0].as_str().unwrap_or_default().to_string();
            let line = entry[1].as_str().unwrap_or_default().to_string();
            (ts, line)
        })
        .filter(|(_, line)| line.contains(marker))
        .collect();
    if actual != expected {
        bail!(
            "stream for service {:?}: round-tripped entries diverged from the fixture\n\
             expected: {expected:?}\nactual:   {actual:?}",
            stream.service
        );
    }

    let labels = result["stream"]
        .as_object()
        .context("stream result missing a stream label object")?;
    if labels.get("service_name").and_then(|v| v.as_str()) != Some(stream.service.as_str()) {
        bail!(
            "stream for service {:?}: service_name label missing/mismatched: {labels:?}",
            stream.service
        );
    }
    if let Some(name) = &stream.scope_name
        && labels.get("otel_scope_name").and_then(|v| v.as_str()) != Some(name.as_str())
    {
        bail!(
            "stream for service {:?}: otel_scope_name label missing/mismatched: {labels:?}",
            stream.service
        );
    }
    // otel scope *version* (code review finding, issue #15 re-review):
    // previously only `otel_scope_name` was asserted, leaving
    // `otel_scope_version` — a distinct label `pulsus-write` emits
    // whenever a scope is present — unverified.
    if stream.scope_name.is_some() {
        let expected_version = stream.scope_version.clone().unwrap_or_default();
        if labels.get("otel_scope_version").and_then(|v| v.as_str())
            != Some(expected_version.as_str())
        {
            bail!(
                "stream for service {:?}: otel_scope_version label missing/mismatched \
                 (expected {expected_version:?}): {labels:?}",
                stream.service
            );
        }
    }
    for key in stream.resource_attrs.keys() {
        let normalized = normalize_label_key(key);
        if !labels.contains_key(&normalized) {
            bail!(
                "stream for service {:?}: expected normalized resource-attr label {normalized:?} \
                 missing from {labels:?}",
                stream.service
            );
        }
    }
    // scope-attribute normalization (code review finding, issue #15
    // re-review): the fixture's `scope_attrs` (e.g. `payments`'s `team`)
    // were never previously checked — only `resource_attrs` were.
    for key in stream.scope_attrs.keys() {
        let normalized = normalize_label_key(key);
        if !labels.contains_key(&normalized) {
            bail!(
                "stream for service {:?}: expected normalized scope-attr label {normalized:?} \
                 missing from {labels:?}",
                stream.service
            );
        }
    }

    Ok(())
}

/// Asserts the remaining three native endpoints (`labels`,
/// `label/{name}/values`, `series`) plus the metric shapes of
/// `query_range`/`query` (matrix/vector `count_over_time`), all against the
/// fixture's first stream and the full fixture's service set. By the time
/// this runs every stream has already round-tripped through
/// [`assert_stream_roundtrip`], so no further polling is needed here.
///
/// **Run isolation (code review finding, issue #15 second re-review):**
/// `series` results and metric points carry no body text, so unlike
/// [`assert_stream_roundtrip`]'s marker-substring filtering this function
/// scopes every `series`/metric selector with `run_id="<marker>"`
/// ([`RUN_ID_LABEL`]) directly — a foreign stream sharing `service_name`
/// (but not this run's `run_id`) can neither add a spurious series to a
/// `series.len() == 1` check nor shift a metric result's index, because it
/// is excluded by the selector itself before the response is ever built,
/// not filtered client-side after the fact.
async fn assert_labels_and_series(
    ctx: &Ctx,
    fixture: &RoundtripFixture,
    base_ns: i64,
    marker: &str,
    start_ns: i64,
    end_ns: i64,
) -> Result<()> {
    let start_s = start_ns.to_string();
    let end_s = end_ns.to_string();
    let representative = fixture
        .streams
        .first()
        .context("fixture has no streams to build a representative selector from")?;

    let res = ctx
        .http
        .get(ctx.url("/api/logs/v1/labels"))
        .query(&[("start", start_s.as_str()), ("end", end_s.as_str())])
        .send()
        .await
        .context("GET labels failed")?;
    if !res.status().is_success() {
        bail!("labels returned {}", res.status());
    }
    let body: serde_json::Value = res.json().await.context("labels body was not JSON")?;
    let names: Vec<&str> = body["data"]
        .as_array()
        .context("labels data was not an array")?
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    if !names.contains(&"service_name") {
        bail!("labels response missing service_name: {names:?}");
    }

    let res = ctx
        .http
        .get(ctx.url("/api/logs/v1/label/service_name/values"))
        .query(&[("start", start_s.as_str()), ("end", end_s.as_str())])
        .send()
        .await
        .context("GET label/service_name/values failed")?;
    if !res.status().is_success() {
        bail!("label/service_name/values returned {}", res.status());
    }
    let body: serde_json::Value = res
        .json()
        .await
        .context("label/service_name/values body was not JSON")?;
    let values: Vec<&str> = body["data"]
        .as_array()
        .context("label values data was not an array")?
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    for stream in &fixture.streams {
        if !values.contains(&stream.service.as_str()) {
            bail!(
                "label/service_name/values missing fixture service {:?}: {values:?}",
                stream.service
            );
        }
    }

    let selector = format!(
        r#"{{service_name="{}", {RUN_ID_LABEL}="{marker}"}}"#,
        representative.service
    );
    let res = ctx
        .http
        .get(ctx.url("/api/logs/v1/series"))
        .query(&[
            ("match[]", selector.as_str()),
            ("start", start_s.as_str()),
            ("end", end_s.as_str()),
        ])
        .send()
        .await
        .context("GET series failed")?;
    if !res.status().is_success() {
        bail!("series returned {}", res.status());
    }
    let body: serde_json::Value = res.json().await.context("series body was not JSON")?;
    // Safe to assume at most one match and index `series[0]` below: the
    // selector above is `run_id`-scoped (see this fn's doc comment), so a
    // foreign stream sharing `service_name` is excluded server-side, never
    // just filtered after the fact.
    let series = body["data"]
        .as_array()
        .context("series data was not an array")?;
    if series.len() != 1 {
        bail!(
            "expected exactly one series for {:?}, got {}",
            representative.service,
            series.len()
        );
    }
    if series[0].get("service_name").and_then(|v| v.as_str())
        != Some(representative.service.as_str())
    {
        bail!(
            "series result missing service_name for {:?}: {:?}",
            representative.service,
            series[0]
        );
    }

    // otel scope name/version + scope-attr normalization, via `series`
    // (code review finding, issue #15 re-review): `assert_stream_roundtrip`
    // already checks these on `query_range`'s per-stream labels; this
    // covers the same fixture facts through the `series` endpoint too,
    // against a stream that actually carries `scope_attrs`
    // (`representative`/`checkout` carries none).
    if let Some(scope_stream) = fixture
        .streams
        .iter()
        .find(|s| s.scope_name.is_some() && !s.scope_attrs.is_empty())
    {
        let scope_selector = format!(
            r#"{{service_name="{}", {RUN_ID_LABEL}="{marker}"}}"#,
            scope_stream.service
        );
        let res = ctx
            .http
            .get(ctx.url("/api/logs/v1/series"))
            .query(&[
                ("match[]", scope_selector.as_str()),
                ("start", start_s.as_str()),
                ("end", end_s.as_str()),
            ])
            .send()
            .await
            .context("GET series (scope check) failed")?;
        if !res.status().is_success() {
            bail!("series (scope check) returned {}", res.status());
        }
        let body: serde_json::Value = res
            .json()
            .await
            .context("series (scope check) body was not JSON")?;
        // `scope_selector` is `run_id`-scoped too — same "safe to index
        // series[0]" reasoning as the representative check above.
        let series = body["data"]
            .as_array()
            .context("series (scope check) data was not an array")?;
        if series.len() != 1 {
            bail!(
                "expected exactly one series for {:?}, got {}",
                scope_stream.service,
                series.len()
            );
        }
        let expected_version = scope_stream.scope_version.clone().unwrap_or_default();
        if series[0].get("otel_scope_name").and_then(|v| v.as_str())
            != scope_stream.scope_name.as_deref()
            || series[0].get("otel_scope_version").and_then(|v| v.as_str())
                != Some(expected_version.as_str())
        {
            bail!(
                "series result for {:?} missing/mismatched otel scope labels: {:?}",
                scope_stream.service,
                series[0]
            );
        }
        for key in scope_stream.scope_attrs.keys() {
            let normalized = normalize_label_key(key);
            if series[0].get(&normalized).is_none() {
                bail!(
                    "series result for {:?}: expected normalized scope-attr label \
                     {normalized:?} missing from {:?}",
                    scope_stream.service,
                    series[0]
                );
            }
        }
    }

    // The metric selector is scoped by `run_id` too (code review finding,
    // issue #15 second re-review): `count_over_time` has no body to
    // substring-filter, so without `run_id` a foreign stream sharing
    // `service_name` could contribute a second matrix series, making the
    // positional `result[0]` read below unsound. Scoped this way, at most
    // one series can ever match — asserted explicitly rather than assumed.
    let metric_selector = format!(
        r#"count_over_time({{service_name="{}", {RUN_ID_LABEL}="{marker}"}}[1h])"#,
        representative.service
    );
    let expected_count = representative.lines.len();

    let res = ctx
        .http
        .get(ctx.url("/api/logs/v1/query_range"))
        .query(&[
            ("query", metric_selector.as_str()),
            ("start", start_s.as_str()),
            ("end", end_s.as_str()),
            ("step", "3600s"),
        ])
        .send()
        .await
        .context("GET query_range (metric) failed")?;
    if !res.status().is_success() {
        bail!("query_range (metric) returned {}", res.status());
    }
    let body: serde_json::Value = res
        .json()
        .await
        .context("query_range (metric) body was not JSON")?;
    if body["data"]["resultType"] != "matrix" {
        bail!("query_range (metric) resultType was not matrix: {body}");
    }
    let matrix_result = body["data"]["result"]
        .as_array()
        .context("query_range (metric) result was not an array")?;
    if matrix_result.len() != 1 {
        bail!(
            "expected exactly one matrix series for the run_id-scoped selector, got {}: {body}",
            matrix_result.len()
        );
    }
    let total: f64 = matrix_result[0]["values"]
        .as_array()
        .context("matrix result missing a values array")?
        .iter()
        .filter_map(|point| point[1].as_str())
        .filter_map(|s| s.parse::<f64>().ok())
        .sum();
    if total as usize != expected_count {
        bail!(
            "query_range (metric) total count {total} did not match {expected_count} ingested lines"
        );
    }

    let time_s = base_ns.to_string();
    let res = ctx
        .http
        .get(ctx.url("/api/logs/v1/query"))
        .query(&[
            ("query", metric_selector.as_str()),
            ("time", time_s.as_str()),
        ])
        .send()
        .await
        .context("GET query (instant) failed")?;
    if !res.status().is_success() {
        bail!("query (instant) returned {}", res.status());
    }
    let body: serde_json::Value = res
        .json()
        .await
        .context("query (instant) body was not JSON")?;
    if body["data"]["resultType"] != "vector" {
        bail!("query (instant) resultType was not vector: {body}");
    }
    // Cardinality, not just non-emptiness (code review finding, issue #15
    // second re-review): `metric_selector` is `run_id`-scoped above, so
    // exactly one vector sample can ever match — asserted explicitly, and
    // its value read from that single element rather than assumed present
    // at an arbitrary position.
    let vector_result = body["data"]["result"]
        .as_array()
        .context("query (instant) result was not an array")?;
    if vector_result.len() != 1 {
        bail!(
            "expected exactly one vector sample for the run_id-scoped selector, got {}: {body}",
            vector_result.len()
        );
    }
    let instant_value: f64 = vector_result[0]["value"][1]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .context("query (instant) vector sample value was not a parseable number")?;
    if instant_value as usize != expected_count {
        bail!(
            "query (instant) value {instant_value} did not match {expected_count} ingested lines"
        );
    }

    Ok(())
}

/// Cluster-only shard-local sanity (issue #15 architect plan, task-manager
/// resolution #3: `compose exec ... clickhouse-client`, no extra published
/// ports). **Poll-until-deadline** (code review finding, issue #15
/// re-review): a single one-shot check raced the very eventual-consistency
/// docs/architecture.md's writer section documents — `_dist` Distributed
/// forwarding to shards is asynchronous even for sync-mode writes, so the
/// per-shard local counts can lag the confirmed `/v1/logs` response by
/// more than an instant. This polls the same way every other
/// visibility-dependent assertion in this scenario does. Robust against
/// sharding-distribution flakiness with few distinct fingerprints: the
/// condition is `sum(per-shard local counts) == total`, not "both shards
/// non-empty" (which a small fixture cannot guarantee probabilistically) —
/// and, since both counts are non-negative, that sum condition alone
/// already proves neither shard exceeds the total (no cross-shard
/// duplication).
async fn assert_shard_local_row_counts(ctx: &Ctx, expected_total: usize) -> Result<()> {
    let compose = ctx.compose.clone();
    let (shard1, shard2) = poll_until(ROUNDTRIP_POLL_TIMEOUT, ROUNDTRIP_POLL_INTERVAL, || {
        shard_local_row_counts(compose.clone(), expected_total)
    })
    .await
    .with_context(|| {
        format!(
            "shard-local log_samples row counts never summed to the {expected_total} ingested \
             rows within the poll deadline"
        )
    })?;

    let total = shard1 + shard2;
    debug_assert_eq!(
        total, expected_total,
        "poll_until only returns Some once the sum condition holds"
    );
    Ok(())
}

/// One shard-local-count attempt: `Ok(Some((shard1, shard2)))` once their
/// sum reaches `expected_total`, `Ok(None)` otherwise — the [`poll_until`]
/// condition [`assert_shard_local_row_counts`] polls on.
async fn shard_local_row_counts(
    compose: Compose,
    expected_total: usize,
) -> Result<Option<(usize, usize)>> {
    // `Compose::exec` shells out synchronously (`std::process::Command`);
    // offloaded to the blocking pool so it never stalls the async executor,
    // matching `harness::run`'s own `compose.up()` discipline.
    let (shard1, shard2) = tokio::task::spawn_blocking(move || -> Result<(usize, usize)> {
        let shard1 = shard_local_log_samples_count(&compose, "ch-shard1")?;
        let shard2 = shard_local_log_samples_count(&compose, "ch-shard2")?;
        Ok((shard1, shard2))
    })
    .await
    .context("shard-local row-count task panicked")??;

    Ok((shard1 + shard2 == expected_total).then_some((shard1, shard2)))
}

fn shard_local_log_samples_count(compose: &Compose, shard_service: &str) -> Result<usize> {
    let output = compose
        .exec(
            shard_service,
            &[
                "clickhouse-client",
                "--query",
                &format!("SELECT count() FROM {PULSUS_DB}.log_samples"),
            ],
        )
        .with_context(|| format!("compose exec {shard_service} clickhouse-client failed"))?;
    output
        .trim()
        .parse::<usize>()
        .with_context(|| format!("shard {shard_service} row count {output:?} was not a number"))
}

/// Grafana's own published base URL for this stack (deploy/e2e/
/// compose.single.yaml's `grafana` service, `ports: ["3000:3000"]`) —
/// distinct from `ctx.base_url` (pulsusdb's `:3100`), so this scenario
/// builds its own client/URL rather than using `Ctx::url`.
const GRAFANA_BASE_URL: &str = "http://127.0.0.1:3000";
/// Grafana readiness poll bounds (CI regression fix: the harness's own
/// `/ready` poll — `harness::READY_POLL_TIMEOUT`/`READY_POLL_INTERVAL` —
/// only covers `pulsusdb`; nothing previously waited for the separate
/// `grafana` container to finish booting before this scenario's first
/// `/api/ds/query` call, so a slow-starting Grafana on a loaded CI runner
/// hit "connection reset" a few seconds after `compose up`). Matches
/// `harness::READY_POLL_TIMEOUT`'s magnitude (90s) — consistent with the
/// rest of the harness's poll-until discipline.
const GRAFANA_READY_POLL_TIMEOUT: Duration = Duration::from_secs(90);
const GRAFANA_READY_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// The provisioned Loki datasource's fixed `uid`
/// (`deploy/e2e/grafana/provisioning/datasources/loki.yaml`) — shared
/// between the readiness probe below and the query request body.
const GRAFANA_LOKI_DATASOURCE_UID: &str = "pulsus-loki";

/// M1 log-query compat alias check via a real Loki datasource (issue #14,
/// docs/api.md §8.1; task-manager-approved option A on the architect
/// plan's open question). Drives Grafana's datasource proxy
/// (`POST /api/ds/query`) with an M1 `query_range` against the
/// `pulsus-loki` datasource provisioned in
/// `deploy/e2e/grafana/provisioning/datasources/loki.yaml`, which points
/// at pulsusdb's `/loki/api/v1/*` compat surface
/// (`PULSUS_COMPAT_ENDPOINTS=true` in the single-variant compose overlay).
/// Asserts a well-formed Loki envelope with no query error — proving alias
/// routing and Loki-datasource wire compatibility end to end.
///
/// **Non-empty frames (issue #15 upgrade, pinned during #14):** this
/// scenario is registered after `logs_roundtrip` (same SCENARIOS order),
/// which has already ingested a `checkout` stream through the collector by
/// the time this runs — the selector below (`{service_name="checkout"}`)
/// is chosen to match it, so a well-formed *but empty* envelope is now a
/// failure, not the expected M0/M1-pre-ingest state.
///
/// **Grafana readiness (CI regression fix, issue #15):** unlike
/// `pulsusdb` (gated by `harness::wait_ready` before any scenario runs),
/// nothing previously waited for the separate `grafana` container to
/// finish booting — a slow-starting Grafana on a loaded CI runner hit
/// "connection reset by peer" seconds after `compose up`, since this was
/// the very first request this scenario (or any scenario) ever sent it.
/// [`wait_for_grafana_ready`] polls `/api/health` (process up) and then
/// the specific `pulsus-loki` datasource's own endpoint (config
/// provisioning applied — Grafana provisions datasources asynchronously
/// after `/api/health` already reports healthy) before the first
/// `/api/ds/query` call below.
async fn grafana_loki_compat(_ctx: &Ctx) -> Result<()> {
    let http = reqwest::Client::new();
    wait_for_grafana_ready(&http).await?;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    let from_ms = now_ms.saturating_sub(3_600_000);

    let request_body = serde_json::json!({
        "queries": [{
            "refId": "A",
            "datasource": { "type": "loki", "uid": GRAFANA_LOKI_DATASOURCE_UID },
            "expr": r#"{service_name="checkout"}"#,
            "queryType": "range",
            "maxLines": 100,
        }],
        "from": from_ms.to_string(),
        "to": now_ms.to_string(),
    });

    let res = http
        .post(format!("{GRAFANA_BASE_URL}/api/ds/query"))
        .json(&request_body)
        .send()
        .await
        .context("POST /api/ds/query failed")?;
    if !res.status().is_success() {
        bail!("POST /api/ds/query returned {}", res.status());
    }
    let payload: serde_json::Value = res
        .json()
        .await
        .context("POST /api/ds/query body was not JSON")?;

    let result_a = payload
        .get("results")
        .and_then(|results| results.get("A"))
        .with_context(|| format!("no results.A in ds/query response: {payload}"))?;

    if let Some(error) = result_a.get("error") {
        bail!("Loki query_range through the compat alias errored: {error}");
    }
    let frames = result_a
        .get("frames")
        .and_then(|f| f.as_array())
        .with_context(|| {
            format!("results.A missing a frames array in ds/query response: {result_a}")
        })?;
    if frames.is_empty() {
        bail!(
            "results.A frames was empty — expected checkout-stream data seeded by the \
             logs_roundtrip scenario to be visible through the Loki-compat proxy: {result_a}"
        );
    }
    Ok(())
}

/// Poll-until-visible for Grafana itself (CI regression fix, issue #15):
/// first `/api/health` (the process is up and answering HTTP at all),
/// then the specific `pulsus-loki` datasource's own endpoint (config
/// provisioning has applied — this can lag `/api/health` by a beat, since
/// Grafana provisions datasources from disk during startup rather than
/// before opening its listener). Both stages tolerate connection errors
/// (`Ok(None)` on any request failure, not just a non-2xx status) — a
/// listener that is not yet accepting connections must retry exactly like
/// a non-2xx response, not abort the poll early.
async fn wait_for_grafana_ready(http: &reqwest::Client) -> Result<()> {
    poll_until(
        GRAFANA_READY_POLL_TIMEOUT,
        GRAFANA_READY_POLL_INTERVAL,
        || grafana_get_ok(http, "/api/health"),
    )
    .await
    .context("grafana /api/health never returned 200")?;

    let datasource_path = format!("/api/datasources/uid/{GRAFANA_LOKI_DATASOURCE_UID}");
    poll_until(
        GRAFANA_READY_POLL_TIMEOUT,
        GRAFANA_READY_POLL_INTERVAL,
        || grafana_get_ok(http, &datasource_path),
    )
    .await
    .context("grafana's pulsus-loki datasource was never provisioned")?;

    Ok(())
}

/// One `GET {GRAFANA_BASE_URL}{path}` attempt: `Ok(Some(()))` on any 2xx,
/// `Ok(None)` otherwise (including connection failures — see
/// [`wait_for_grafana_ready`]'s doc comment) — the [`poll_until`]
/// condition both of its stages share.
async fn grafana_get_ok(http: &reqwest::Client, path: &str) -> Result<Option<()>> {
    let outcome = http.get(format!("{GRAFANA_BASE_URL}{path}")).send().await;
    Ok(match outcome {
        Ok(res) if res.status().is_success() => Some(()),
        _ => None,
    })
}

fn load_fixture_fields(path: &Path) -> Result<Vec<String>> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read fixture {}", path.display()))?;
    let fields: Vec<String> = serde_json::from_str(&raw)
        .with_context(|| format!("fixture {} was not a JSON array of strings", path.display()))?;
    if fields.is_empty() {
        bail!("fixture {} listed no fields", path.display());
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenarios_is_non_empty_per_variant() {
        for variant in [Variant::Single, Variant::Cluster] {
            assert!(
                SCENARIOS.iter().any(|s| s.variants.contains(&variant)),
                "no scenarios registered for {variant:?}"
            );
        }
    }

    #[test]
    fn load_fixture_fields_reads_the_shipped_buildinfo_fixture() {
        let root = crate::engine::workspace_root();
        let fields =
            load_fixture_fields(&root.join("test/fixtures/ops/buildinfo.fields.json")).unwrap();
        assert_eq!(fields, vec!["version", "revision", "builtAt", "rustc"]);
    }

    #[test]
    fn load_fixture_fields_rejects_an_empty_list() {
        let dir = std::env::temp_dir().join("pulsus-e2e-test-empty-fixture");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("empty.json");
        std::fs::write(&path, "[]").unwrap();
        assert!(load_fixture_fields(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn shipped_roundtrip_fixture() -> RoundtripFixture {
        let root = crate::engine::workspace_root();
        let raw =
            std::fs::read_to_string(root.join("test/fixtures").join(ROUNDTRIP_FIXTURE)).unwrap();
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn roundtrip_fixture_covers_at_least_three_services_with_a_scope_and_a_collision() {
        let fixture = shipped_roundtrip_fixture();
        assert!(fixture.streams.len() >= 3);
        assert!(fixture.streams.iter().any(|s| s.scope_name.is_some()));
        // The `billing` stream's `env` collides between resource_attrs and
        // scope_attrs by design (normalized-key collision coverage).
        let billing = fixture
            .streams
            .iter()
            .find(|s| s.service == "billing")
            .expect("fixture ships a collision-case stream");
        assert_eq!(
            billing.resource_attrs.get("env").map(String::as_str),
            Some("resource-env")
        );
        assert_eq!(
            billing.scope_attrs.get("env").map(String::as_str),
            Some("scope-env")
        );
    }

    #[test]
    fn normalize_label_key_replaces_non_alphanumeric_characters_with_underscore() {
        assert_eq!(normalize_label_key("service.name"), "service_name");
        assert_eq!(normalize_label_key("k8s.pod.name"), "k8s_pod_name");
        assert_eq!(normalize_label_key("already_ok_123"), "already_ok_123");
    }

    #[test]
    fn build_otlp_export_request_emits_one_resource_log_per_stream_with_scope_iff_configured() {
        let fixture = shipped_roundtrip_fixture();
        let base_ns = 1_700_000_000_000_000_000i64;
        let marker = run_marker(base_ns);
        let payload = build_otlp_export_request(&fixture, base_ns, &marker);

        let resource_logs = payload["resourceLogs"].as_array().unwrap();
        assert_eq!(resource_logs.len(), fixture.streams.len());

        for (stream, entry) in fixture.streams.iter().zip(resource_logs) {
            let resource_attrs = entry["resource"]["attributes"].as_array().unwrap();
            let service_name_present = resource_attrs.iter().any(|kv| {
                kv["key"] == "service.name" && kv["value"]["stringValue"] == stream.service
            });
            assert!(service_name_present, "missing service.name for {stream:?}");

            let run_id_present = resource_attrs
                .iter()
                .any(|kv| kv["key"] == RUN_ID_LABEL && kv["value"]["stringValue"] == marker);
            assert!(
                run_id_present,
                "missing {RUN_ID_LABEL} resource attribute for {stream:?}"
            );

            let scope_logs = &entry["scopeLogs"][0];
            assert_eq!(scope_logs["scope"].is_null(), stream.scope_name.is_none());

            let log_records = scope_logs["logRecords"].as_array().unwrap();
            assert_eq!(log_records.len(), stream.lines.len());
            for (line, record) in stream.lines.iter().zip(log_records) {
                assert_eq!(
                    record["timeUnixNano"],
                    (base_ns + line.ts_offset_ns).to_string()
                );
                assert_eq!(
                    record["body"]["stringValue"],
                    tagged_body(&line.body, &marker)
                );
            }
        }
    }

    #[test]
    fn run_marker_is_distinct_across_different_base_ns_values() {
        assert_ne!(run_marker(1), run_marker(2));
    }

    #[test]
    fn tagged_body_embeds_the_marker_and_preserves_the_original_body() {
        let tagged = tagged_body("hello", "e2e-run=42");
        assert!(tagged.contains("hello"));
        assert!(tagged.contains("e2e-run=42"));
    }
}
