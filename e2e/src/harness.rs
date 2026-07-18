//! One end-to-end run for a single variant: `up` the stack, poll `/ready`
//! until it's live, run every scenario registered for the variant, and
//! tear down (via `ComposeGuard`, so teardown also fires on panic or an
//! early scenario failure). On any readiness-timeout or scenario failure,
//! dumps diagnostic service logs before returning the error.

use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::corpus::Scale;
use crate::engine::{Compose, ComposeGuard, EngineKind, workspace_root};
use crate::scenarios::{Ctx, SCENARIOS, Variant};

const READY_POLL_TIMEOUT: Duration = Duration::from_secs(90);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Bounds a single `/ready` attempt (review fix): a connection that is
/// accepted but never answered must not stall the poll loop past this.
/// `wait_ready` wraps every attempt in `tokio::time::timeout` with (at
/// most) this budget in addition to the client's own request timeout
/// (`run`, below), so the overall `READY_POLL_TIMEOUT` deadline is honored
/// regardless of which layer actually cuts a stuck attempt off.
const READY_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Bounds a single scenario *query* (issue #92): the shared client's
/// [`READY_REQUEST_TIMEOUT`] is a readiness-probe budget, and letting it
/// bound matrix queries broke the nightly `Full`-tier run — a
/// `count_values` range query needing ~0.55s locally exceeds 5s on a
/// shared 4-vCPU runner (~10-15x slower on this path). Applied per
/// request via `RequestBuilder::timeout` at the query call sites
/// (`metrics::query_get_raw`), which *replaces* the client-level total
/// timeout for that request (reqwest semantics), so readiness polling
/// keeps its tight 5s budget untouched.
pub const QUERY_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// The full-tier per-request query budget (issue #106). The nightly
/// full-tier lane saturates a single 2-vCPU runner (7 heavy containers +
/// ~10k series), where a heavy range query (e.g. `count_values` over the
/// full corpus) can exceed the strict [`QUERY_REQUEST_TIMEOUT`] client
/// timeout and abort mid-body — the 07-17 `count_values` symptom. Full
/// tier only; per-PR/ci tiers stay strict on the 60s budget.
const QUERY_REQUEST_TIMEOUT_FULL: Duration = Duration::from_secs(120);

/// Tier-aware per-request query timeout (issue #106): the strict 60s
/// [`QUERY_REQUEST_TIMEOUT`] on `Scale::Ci`, the relaxed
/// [`QUERY_REQUEST_TIMEOUT_FULL`] on `Scale::Full`. Resolved at the query
/// call site from the corpus/scenario `Scale` (the shared reqwest client
/// is built before any scenario resolves its scale, so the budget is
/// applied per request via `RequestBuilder::timeout`, not the client
/// default).
pub fn query_request_timeout(scale: Scale) -> Duration {
    match scale {
        Scale::Ci => QUERY_REQUEST_TIMEOUT,
        Scale::Full => QUERY_REQUEST_TIMEOUT_FULL,
    }
}

/// The ci-tier bounded-completeness poll deadline (issue #15 precedent):
/// absorbs collector-export and store-visibility lag without fixed sleeps.
const COMPLETENESS_POLL_TIMEOUT_CI: Duration = Duration::from_secs(180);
/// The full-tier bounded-completeness poll deadline (issue #106): the same
/// single-node saturation that stretches query latency also stretches
/// export/flush convergence, so the full tier gets a wider budget while
/// per-PR/ci stays strict at [`COMPLETENESS_POLL_TIMEOUT_CI`]. The paired
/// on-timeout diagnostic (per-store counts + missing/extra records) means a
/// wider budget can never mask a real convergence bug — the run either goes
/// green or emits the exact shortfall.
const COMPLETENESS_POLL_TIMEOUT_FULL: Duration = Duration::from_secs(600);

/// Tier-aware completeness-poll deadline (issue #106), shared by the logs
/// and traces completeness gates.
pub fn completeness_poll_timeout(scale: Scale) -> Duration {
    match scale {
        Scale::Ci => COMPLETENESS_POLL_TIMEOUT_CI,
        Scale::Full => COMPLETENESS_POLL_TIMEOUT_FULL,
    }
}

/// Service names dumped on failure — present under these exact names in
/// both variants' overlays. `prometheus` (issue #33): the reference
/// backend the `metrics_differential` scenario diffs against — present
/// under this name in both `deploy/e2e/compose.{single,cluster}.yaml`, so
/// a failed differential run dumps its logs alongside `pulsusdb`'s own.
const DIAGNOSTIC_SERVICES: &[&str] = &["pulsusdb", "otel-collector", "prometheus"];
/// Extra services dumped on the single-node leg only (issue #60):
/// `tempo`, the traces differential's reference backend, ships only in
/// `deploy/e2e/compose.single.yaml` (task-manager adjudication 1 —
/// single-node differential topology).
const SINGLE_DIAGNOSTIC_SERVICES: &[&str] = &["tempo"];
/// Extra services dumped on the cluster leg only (issue #15): the 2-shard
/// ClickHouse containers `ci/clickhouse-cluster/compose.yaml` defines,
/// absent from the single-node variant.
const CLUSTER_DIAGNOSTIC_SERVICES: &[&str] = &["ch-shard1", "ch-shard2"];

/// Every service to dump logs for on a failed run of `variant` —
/// [`DIAGNOSTIC_SERVICES`] plus the per-variant extras
/// ([`SINGLE_DIAGNOSTIC_SERVICES`] / [`CLUSTER_DIAGNOSTIC_SERVICES`]).
fn diagnostic_services(variant: Variant) -> Vec<&'static str> {
    let mut services = DIAGNOSTIC_SERVICES.to_vec();
    match variant {
        Variant::Single => services.extend_from_slice(SINGLE_DIAGNOSTIC_SERVICES),
        Variant::Cluster => services.extend_from_slice(CLUSTER_DIAGNOSTIC_SERVICES),
    }
    services
}

pub struct RunOptions {
    pub variant: Variant,
    pub engine: EngineKind,
    pub keep: bool,
    pub base_url: String,
    /// The collector's published OTLP/HTTP base URL (issue #15 architect
    /// plan) — both compose variants publish the same host port
    /// (`deploy/e2e/compose.{single,cluster}.yaml`, `:4318`), so this is a
    /// fixed value like `base_url`, not derived per-variant.
    pub collector_url: String,
    /// The reference Prometheus's published base URL (issue #33 architect
    /// plan, task-manager resolution #4: "threaded through
    /// `RunOptions`/`Ctx`, mirrors `collector_url`") — both compose
    /// variants publish the same host port
    /// (`deploy/e2e/compose.{single,cluster}.yaml`, `:9090`), so this is
    /// fixed like `collector_url`, not derived per-variant.
    pub prometheus_url: String,
    /// The reference Tempo's published base URL (issue #60 architect
    /// plan, mirrors `prometheus_url`) — published by the single-node
    /// overlay only (`deploy/e2e/compose.single.yaml`, `:3200`); a fixed
    /// value regardless, since only single-variant scenarios dereference
    /// it.
    pub tempo_url: String,
    /// The reference log store's published base URL (issue M6-09,
    /// mirrors `tempo_url`) — published by the single-node overlay only
    /// (`deploy/e2e/compose.single.yaml`, `:3101`); a fixed value
    /// regardless, since only single-variant scenarios dereference it.
    pub loki_url: String,
}

/// The `-f` file list + project name for one variant (architect plan:
/// single overrides the root `docker-compose.yaml` quickstart; cluster
/// overlays issue #5's `ci/clickhouse-cluster/compose.yaml` — no
/// ClickHouse/Keeper duplication).
fn compose_for(variant: Variant, engine: EngineKind) -> Compose {
    match variant {
        Variant::Single => Compose::new(
            engine,
            vec!["docker-compose.yaml", "deploy/e2e/compose.single.yaml"],
            "pulsus-e2e-single",
        ),
        Variant::Cluster => Compose::new(
            engine,
            vec![
                "ci/clickhouse-cluster/compose.yaml",
                "deploy/e2e/compose.cluster.yaml",
            ],
            "pulsus-e2e-cluster",
        ),
    }
}

pub async fn run(opts: RunOptions) -> Result<()> {
    let compose = compose_for(opts.variant, opts.engine);
    let guard = ComposeGuard::new(compose.clone(), opts.keep);

    println!(
        "pulsus-e2e: bringing up the {:?} stack ({:?})",
        opts.variant, opts.engine
    );
    let up_compose = compose.clone();
    // `Command::output` blocks the calling thread for as long as the
    // images take to build/pull and the containers take to start —
    // offloaded to the blocking pool so it never stalls the async
    // executor (this binary has no other concurrent work, but the
    // discipline costs nothing and stays correct if that changes).
    tokio::task::spawn_blocking(move || up_compose.up())
        .await
        .context("compose up task panicked")??;

    let http = reqwest::Client::builder()
        // Belt-and-suspenders with `wait_ready`'s own per-attempt
        // `tokio::time::timeout` wrap (review fix): a client-level request
        // timeout means a stalled `/ready` (or scenario) request fails on
        // its own, without relying solely on the wrapper. Strictly a
        // *readiness/default* budget (issue #92): the scenario query paths
        // override it per request with `QUERY_REQUEST_TIMEOUT` via
        // `RequestBuilder::timeout`, so heavyweight matrix queries are
        // never cut off at 5s on a slow shared runner.
        .timeout(READY_REQUEST_TIMEOUT)
        .build()
        .context("failed to build the HTTP client")?;

    println!("pulsus-e2e: polling {}/ready", opts.base_url);
    if let Err(err) = wait_ready(&http, &opts.base_url, READY_POLL_TIMEOUT).await {
        dump_logs(&compose, opts.variant);
        return Err(err);
    }

    let ctx = Ctx {
        http,
        base_url: opts.base_url.clone(),
        collector_url: opts.collector_url.clone(),
        prometheus_url: opts.prometheus_url.clone(),
        tempo_url: opts.tempo_url.clone(),
        loki_url: opts.loki_url.clone(),
        variant: opts.variant,
        fixtures_dir: workspace_root().join("test/fixtures"),
        compose: compose.clone(),
    };

    let scenarios: Vec<_> = SCENARIOS
        .iter()
        .filter(|s| s.variants.contains(&opts.variant))
        .collect();
    if scenarios.is_empty() {
        bail!("no scenarios registered for variant {:?}", opts.variant);
    }

    for scenario in scenarios {
        println!("pulsus-e2e: running scenario {:?}", scenario.name);
        if let Err(err) = (scenario.run)(&ctx).await {
            dump_logs(&compose, opts.variant);
            return Err(err.context(format!("scenario {:?} failed", scenario.name)));
        }
    }

    println!("pulsus-e2e: all scenarios passed for {:?}", opts.variant);
    drop(guard); // explicit: makes the successful-teardown point visible.
    Ok(())
}

/// Polls `GET {base_url}/ready` until it returns 2xx or `timeout` elapses.
/// The harness polls itself rather than trusting compose's
/// `service_healthy` condition, whose semantics differ across Docker and
/// Podman compose implementations (architect plan edge case).
///
/// Every attempt is wrapped in its own `tokio::time::timeout` (bounded by
/// whatever is left of `timeout`, capped at `READY_REQUEST_TIMEOUT`) so a
/// connection that is accepted but never answered cannot stall this loop
/// past the overall deadline — checking `timeout` only after `send().await`
/// returns is not enough, since that `.await` can itself hang forever
/// (review fix).
pub async fn wait_ready(http: &reqwest::Client, base_url: &str, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    let url = format!("{base_url}/ready");
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            bail!("{url} did not become ready within {timeout:?}");
        }
        let remaining = deadline - now;
        let attempt_budget = remaining.min(READY_REQUEST_TIMEOUT);
        match tokio::time::timeout(attempt_budget, http.get(&url).send()).await {
            Ok(Ok(res)) if res.status().is_success() => return Ok(()),
            _ => {}
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            bail!("{url} did not become ready within {timeout:?}");
        }
        tokio::time::sleep(READY_POLL_INTERVAL.min(deadline - now)).await;
    }
}

fn dump_logs(compose: &Compose, variant: Variant) {
    for service in diagnostic_services(variant) {
        eprintln!("=== logs: {service} ===");
        eprintln!("{}", compose.logs(service));
    }
}

/// Polls `attempt` every `interval` until it returns `Ok(Some(value))` or
/// `timeout` elapses, returning `value` — the collector-to-query e2e's
/// **poll-until-visible** primitive (issue #15 architect plan): cluster-
/// mode `_dist` writes are eventually consistent even in sync mode
/// (docs/architecture.md's writer section), so scenarios asserting
/// ingested data is queryable poll for it rather than assuming immediate
/// visibility. `Ok(None)` ("not visible yet") and `Err` (a transient
/// request failure) are both treated as "not yet" and retried — only the
/// final, post-deadline attempt's error (if any) is surfaced, so a
/// scenario failure reports the *last* observed state, not the first.
/// Mirrors [`wait_ready`]'s bounded-poll shape, generalized over the
/// condition being polled for.
pub async fn poll_until<F, Fut, T>(
    timeout: Duration,
    interval: Duration,
    mut attempt: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let outcome = attempt().await;
        let now = tokio::time::Instant::now();
        match outcome {
            Ok(Some(value)) => return Ok(value),
            Ok(None) if now >= deadline => {
                bail!("condition not met within {timeout:?}");
            }
            Err(err) if now >= deadline => {
                return Err(err.context(format!("condition not met within {timeout:?}")));
            }
            Ok(None) | Err(_) => {}
        }
        tokio::time::sleep(interval.min(deadline - now)).await;
    }
}

/// True iff a reqwest transport error is a connection-establishment
/// failure — zero request bytes were processed server-side, so resending
/// the identical body cannot double-ingest. Delegates to
/// [`reqwest::Error::is_connect`] (0.12: walks the source chain for a hyper
/// connect error). A post-connect failure (request-write, post-send
/// timeout, response-read) returns false and MUST NOT be retried.
pub fn push_retry_is_safe(err: &reqwest::Error) -> bool {
    err.is_connect()
}

/// Maps one non-idempotent POST `send()` outcome to the [`poll_until`]
/// producer contract shared by every harness push helper (issue #105):
///  * `Ok(Some(Ok(res)))` — an HTTP response arrived; stop polling (the
///    caller checks `res.status()`).
///  * `Err(e)`            — connection-phase failure; `poll_until` retries
///    the identical body (the endpoint-not-listening readiness signal these
///    polls exist for) and surfaces `e` on the deadline.
///  * `Ok(Some(Err(e)))`  — post-connect failure; stop polling and fail
///    fast — the server may have ingested the body, so resending would
///    double-ingest.
pub fn classify_push_send(
    outcome: reqwest::Result<reqwest::Response>,
) -> Result<Option<Result<reqwest::Response>>> {
    match outcome {
        Ok(res) => Ok(Some(Ok(res))),
        Err(e) if push_retry_is_safe(&e) => Err(e.into()),
        Err(e) => Ok(Some(Err(anyhow::Error::new(e).context(
            "POST failed after the connection was established — the request may have reached \
             the server; the identical body is NOT retried (idempotency guard, issue #105)",
        )))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_request_timeout_is_strict_on_ci_and_relaxed_on_full() {
        // Issue #106: full tier gets 120s (the count_values saturation
        // fix), ci stays strict at 60s.
        assert_eq!(query_request_timeout(Scale::Ci), Duration::from_secs(60));
        assert_eq!(query_request_timeout(Scale::Full), Duration::from_secs(120));
    }

    #[test]
    fn completeness_poll_timeout_is_strict_on_ci_and_relaxed_on_full() {
        // Issue #106: full tier gets 600s, ci stays strict at 180s.
        assert_eq!(
            completeness_poll_timeout(Scale::Ci),
            Duration::from_secs(180)
        );
        assert_eq!(
            completeness_poll_timeout(Scale::Full),
            Duration::from_secs(600)
        );
    }

    #[test]
    fn compose_for_covers_both_variants_with_distinct_projects() {
        let single = compose_for(Variant::Single, EngineKind::Docker);
        let cluster = compose_for(Variant::Cluster, EngineKind::Docker);
        assert_ne!(single.project(), cluster.project());
    }

    #[tokio::test]
    async fn wait_ready_times_out_against_an_unreachable_url() {
        let http = reqwest::Client::new();
        // Port 1 is reserved/unroutable in practice, so the connect
        // itself fails fast; a short timeout keeps the test quick.
        let res = wait_ready(&http, "http://127.0.0.1:1", Duration::from_millis(200)).await;
        assert!(res.is_err());
    }

    /// Reproduces a `pulsusdb` that accepts the TCP connection but never
    /// answers (wedged, not down) — the load-bearing review-fix case:
    /// without the per-attempt `tokio::time::timeout` wrap, the stalled
    /// `send().await` would hang past `wait_ready`'s own deadline forever.
    #[tokio::test]
    async fn wait_ready_times_out_against_a_stalled_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                // Hold every accepted connection open without ever writing
                // a response back.
                tokio::spawn(async move {
                    let _socket = socket;
                    tokio::time::sleep(Duration::from_secs(60)).await;
                });
            }
        });

        let http = reqwest::Client::new();
        let deadline = Duration::from_millis(500);
        let start = std::time::Instant::now();
        let res = wait_ready(&http, &format!("http://{addr}"), deadline).await;
        let elapsed = start.elapsed();

        accept_task.abort();
        assert!(res.is_err());
        assert!(
            elapsed < Duration::from_secs(3),
            "wait_ready took {elapsed:?}, expected to be bounded by the {deadline:?} deadline"
        );
    }

    /// Issue #105: a connection that never established (nothing listening on
    /// `127.0.0.1:1`) is a `is_connect()` failure — zero bytes reached the
    /// server, so it is safe to resend. `classify_push_send` maps it to
    /// `Err` so `poll_until` keeps polling (readiness signal) and surfaces
    /// the reqwest detail on the deadline.
    #[tokio::test]
    async fn classify_push_send_retries_a_connect_refused_failure() {
        let http = reqwest::Client::new();
        let outcome = http
            .post("http://127.0.0.1:1/x")
            .json(&serde_json::json!({ "probe": true }))
            .send()
            .await;

        let err = outcome
            .as_ref()
            .expect_err("POST to 127.0.0.1:1 must fail — nothing is listening");
        assert!(
            err.is_connect(),
            "expected a connect-phase error, got: {err:?}"
        );

        // Err arm -> poll_until retries the identical body.
        assert!(
            classify_push_send(outcome).is_err(),
            "a connect-refused failure must classify as the retry (Err) arm"
        );
    }

    /// Issue #105 (load-bearing proof): a POST that connects, is read, then
    /// has its connection dropped mid-response yields a genuine post-connect
    /// (`!is_connect()`) error — the server may have ingested the body.
    /// `classify_push_send` maps it to `Ok(Some(Err(_)))` so `poll_until`
    /// STOPS and does NOT resend. A classifier that retried post-connect
    /// errors would fail this test's `matches!` assertion.
    #[tokio::test]
    async fn classify_push_send_does_not_retry_a_post_connect_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_task = tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                // Read the request bytes off the wire (so the client's
                // request-write completes and the connection is fully
                // established), then drop the socket without answering —
                // the client's response-read then fails post-connect.
                use tokio::io::AsyncReadExt;
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                drop(socket);
            }
        });

        let http = reqwest::Client::new();
        let outcome = http
            .post(format!("http://{addr}/x"))
            .json(&serde_json::json!({ "probe": true }))
            .send()
            .await;

        accept_task.abort();

        let err = outcome
            .as_ref()
            .expect_err("the dropped connection must surface as a send() error");
        assert!(
            !err.is_connect(),
            "expected a post-connect error (connection was established), got: {err:?}"
        );

        // Ok(Some(Err(_))) arm -> poll_until stops, no resend.
        assert!(
            matches!(classify_push_send(outcome), Ok(Some(Err(_)))),
            "a post-connect failure must classify as the terminal (fail-fast) arm"
        );
    }
}
