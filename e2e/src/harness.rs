//! One end-to-end run for a single variant: `up` the stack, poll `/ready`
//! until it's live, run every scenario registered for the variant, and
//! tear down (via `ComposeGuard`, so teardown also fires on panic or an
//! early scenario failure). On any readiness-timeout or scenario failure,
//! dumps diagnostic service logs before returning the error.

use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result, bail};

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

/// Service names dumped on failure — present under these exact names in
/// both variants' overlays.
const DIAGNOSTIC_SERVICES: &[&str] = &["pulsusdb", "otel-collector"];
/// Extra services dumped on the cluster leg only (issue #15): the 2-shard
/// ClickHouse containers `ci/clickhouse-cluster/compose.yaml` defines,
/// absent from the single-node variant.
const CLUSTER_DIAGNOSTIC_SERVICES: &[&str] = &["ch-shard1", "ch-shard2"];

/// Every service to dump logs for on a failed run of `variant` —
/// [`DIAGNOSTIC_SERVICES`] plus, on the cluster leg,
/// [`CLUSTER_DIAGNOSTIC_SERVICES`].
fn diagnostic_services(variant: Variant) -> Vec<&'static str> {
    let mut services = DIAGNOSTIC_SERVICES.to_vec();
    if variant == Variant::Cluster {
        services.extend_from_slice(CLUSTER_DIAGNOSTIC_SERVICES);
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
        // its own, without relying solely on the wrapper.
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
