//! `serve::run`: process entry point for every *serving* mode (`all`,
//! `writer`, `reader` — never `init`, which `main.rs` dispatches to
//! `schema_init::run` and exits before this module is ever reached).
//! Initializes tracing, installs the Prometheus recorder, spawns the
//! background ClickHouse reconnect loop, builds the router, and serves it
//! with graceful shutdown.
//!
//! Data flow: req → CORS → gzip → TraceLayer(span) → [ops-authed group:
//! TimeoutLayer(query_timeout) → auth(opt) → subsystem/compat routes], with
//! `/ready`/`/metrics` mounted outside the bracketed group entirely (see
//! `app::build_router`).

use std::future::Future;
use std::process::ExitCode;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use pulsus_clickhouse::{ChClient, ChError, ChPool};
use pulsus_config::{Config, LogLevel, Mode};
use pulsus_read::LabelCache;
use pulsus_schema::SchemaError;
use pulsus_write::{LogWriter, MetricWriter};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;

use crate::app::{self, AppState, BuildInfo};
use crate::chconfig::{
    bootstrap_conn_config_from, build_label_cache, conn_config_from, metric_writer_tables_from,
    schema_params_from, writer_tables_from,
};
use crate::ingest::{MetricWriterSink, WriterSink};

/// Startup-time failures specific to the HTTP server layer (config-load and
/// schema-controller failures are handled separately, in `main.rs` /
/// `schema_init.rs`, before this module is reached).
#[derive(Debug, Error)]
pub(crate) enum ServeError {
    #[error("PULSUS_CORS_ORIGIN {0:?} is not a valid HTTP header value")]
    InvalidCorsOrigin(String),
    #[error("failed to bind {addr}: {source}")]
    Bind {
        addr: String,
        source: std::io::Error,
    },
}

/// The bound on [`LogWriter::shutdown`]'s drain, run between graceful HTTP
/// shutdown and background-task/pool teardown (issue #15 architect plan).
/// A documented constant for now, not a `PULSUS_*` var (task-manager
/// resolution: promote to config only when a deployment needs to tune it —
/// the same precedent `writer::config::WriterRuntime`'s own documented
/// constants set).
const WRITER_DRAIN_DEADLINE: Duration = Duration::from_secs(10);

/// Runs one serving process (`all`/`writer`/`reader`) to completion: init
/// tracing, install the metrics recorder, spawn the reconnect loop (which
/// also starts the recurring TTL-rotation task once the schema is ready),
/// build the router, and serve with graceful shutdown. Shutdown ordering
/// (architect plan amendment, extended to rotation by the issue #6 review
/// fix, and to the writer by issue #15): graceful stop (accept loop drains
/// in-flight requests — including a sync-ingest request's held-open
/// `FlushWait`) → drain the writer (`LogWriter::shutdown`, bounded by
/// [`WRITER_DRAIN_DEADLINE`]) → abort the reconnect task, then join it →
/// abort the rotation task (if one was ever started), then join it → only
/// then does `pool_slot` (and the `ChPool` it may hold) drop, so pool
/// teardown never races an in-flight `connect`/`ping`/rotation tick *or* a
/// still-draining writer flush.
pub async fn run(config: Config) -> ExitCode {
    init_tracing(config.log_level);

    let metrics = install_metrics_recorder();
    let config = Arc::new(config);
    let pool_slot: Arc<RwLock<Option<Arc<ChPool>>>> = Arc::new(RwLock::new(None));
    // Async-filled at most once by the reconnect loop, same shape as
    // `pool_slot` but a `OnceLock` (no readers before the first write ever
    // race a write — `WriterSink::admit`/`admit_flush` just see `None` and
    // return `Backpressure`, no lock needed) — set *before* `pool_slot` so
    // `/ready`=200 implies the ingest route is live too (issue #15
    // architect plan).
    let writer_slot: Arc<OnceLock<Arc<LogWriter>>> = Arc::new(OnceLock::new());
    // `MetricWriter`'s lifecycle-parity counterpart (issue #26 architect
    // plan): constructed + shutdown-drained alongside `LogWriter`. Wired
    // into `AppState` (via `MetricWriterSink`) and `/v1/metrics` below
    // (issue #27); `/api/v1/write` (Prometheus remote write) still lands
    // in #28. Its flush tasks simply idle (never admitted to) until this
    // slot is filled by the reconnect loop.
    let metric_writer_slot: Arc<OnceLock<Arc<MetricWriter>>> = Arc::new(OnceLock::new());
    // The label cache's async-filled slot (issue #30 architect plan): same
    // shape as `writer_slot`/`metric_writer_slot`, constructed by the
    // reconnect loop only in reader-enabled modes (see `reader_enabled`).
    // `ops::ready` gates on `label_cache.get().is_some_and(|c| c.is_warm())`.
    let label_cache_slot: Arc<OnceLock<Arc<LabelCache>>> = Arc::new(OnceLock::new());
    // The reconnect loop is a one-shot bootstrap (see its own doc comment):
    // it runs exactly once per process and, on success, spawns at most one
    // rotation task — handed back over this oneshot channel so `run` can
    // abort it at shutdown too. No duplication/leak risk follows from that
    // single-spawn invariant.
    let (rotation_tx, rotation_rx) = oneshot::channel();
    // The label cache's refresh-loop handle, handed back the same way
    // (issue #30 architect plan: "abort/join the refresh handle in the
    // shutdown ordering next to rotation").
    let (label_cache_refresh_tx, label_cache_refresh_rx) = oneshot::channel();
    let reconnect_handle = spawn_reconnect_loop(
        Arc::clone(&pool_slot),
        Arc::clone(&writer_slot),
        Arc::clone(&metric_writer_slot),
        Arc::clone(&label_cache_slot),
        Arc::clone(&config),
        rotation_tx,
        label_cache_refresh_tx,
    );

    let state = AppState {
        pool: Arc::clone(&pool_slot),
        config: Arc::clone(&config),
        metrics,
        build: BuildInfo::from_build_env(),
        writer: Arc::new(WriterSink::new(Arc::clone(&writer_slot))),
        metric_writer: Arc::new(MetricWriterSink::new(Arc::clone(&metric_writer_slot))),
        label_cache: Arc::clone(&label_cache_slot),
    };

    let router = match app::build_router(state, &config) {
        Ok(router) => router,
        Err(err) => {
            eprintln!("pulsusdb: {err}");
            shutdown_background_tasks(reconnect_handle, rotation_rx, label_cache_refresh_rx).await;
            return ExitCode::FAILURE;
        }
    };

    let addr = format!("{}:{}", config.host, config.port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(source) => {
            eprintln!("pulsusdb: {}", ServeError::Bind { addr, source });
            shutdown_background_tasks(reconnect_handle, rotation_rx, label_cache_refresh_rx).await;
            return ExitCode::FAILURE;
        }
    };

    tracing::info!(%addr, mode = ?config.mode, "pulsusdb listening");

    if let Err(err) = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        tracing::error!(error = %err, "server exited with an error");
    }

    // Drain any still-buffered/in-flight writer generations (async-mode
    // admits that never had a request-scoped `FlushWait` to hold graceful
    // shutdown open for) before the reconnect/rotation tasks stop and
    // `pool_slot` drops (issue #15 architect plan). A no-op when this
    // process never mounted the writer (`writer_slot` stays empty).
    if let Some(writer) = writer_slot.get() {
        writer.shutdown(WRITER_DRAIN_DEADLINE).await;
    }
    // Same drain, same deadline, for `MetricWriter` (issue #26) — a no-op
    // when this process never mounted the writer subsystem.
    if let Some(metric_writer) = metric_writer_slot.get() {
        metric_writer.shutdown(WRITER_DRAIN_DEADLINE).await;
    }

    shutdown_background_tasks(reconnect_handle, rotation_rx, label_cache_refresh_rx).await;

    ExitCode::SUCCESS
}

/// Stops the reconnect task, then (if one was ever spawned) the rotation
/// task, then (if one was ever spawned) the label cache refresh task — in
/// that order, and always joined (never just aborted-and-dropped) so
/// callers never race `pool_slot`'s teardown against an in-flight
/// `connect`/`ping`/rotation tick/refresh sweep. Shared by the two
/// pre-listen startup failure paths above and the normal end-of-`run`
/// shutdown path, so the ordering contract lives in exactly one place.
async fn shutdown_background_tasks(
    reconnect_handle: JoinHandle<()>,
    rotation_rx: oneshot::Receiver<JoinHandle<()>>,
    label_cache_refresh_rx: oneshot::Receiver<JoinHandle<()>>,
) {
    reconnect_handle.abort();
    let _ = reconnect_handle.await;

    // By now the reconnect task's fate (finished, or aborted mid-flight) is
    // sealed, so the sender side of `rotation_rx`/`label_cache_refresh_rx`
    // has either already sent (schema ready, task running) or been dropped
    // (skip_ddl, writer-only/no-reader mode, or aborted before ever
    // reconciling) — these `.await`s therefore resolve immediately either
    // way, never hanging.
    if let Ok(rotation_handle) = rotation_rx.await {
        rotation_handle.abort();
        let _ = rotation_handle.await;
    }
    if let Ok(refresh_handle) = label_cache_refresh_rx.await {
        refresh_handle.abort();
        let _ = refresh_handle.await;
    }
}

/// Initial backoff before retrying a failed `ChPool::connect`.
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
/// Cap on the reconnect backoff so a persistently-unreachable ClickHouse
/// still gets retried at a bounded interval.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// A failure from either half of [`ensure_schema_then_connect`] — the
/// reconnect loop only needs to log and retry the whole sequence, never
/// branch on kind.
#[derive(Debug, Error)]
enum StartupError {
    #[error("bootstrap connect failed: {0}")]
    Bootstrap(ChError),
    #[error("schema reconcile failed: {0}")]
    Schema(SchemaError),
    #[error("clickhouse pool connect failed: {0}")]
    Pool(ChError),
}

/// Background task: repeatedly ensures the schema exists (unless
/// `PULSUS_SKIP_DDL`) and then connects the serving `ChPool`, retrying the
/// whole sequence with capped exponential backoff until it succeeds, then
/// stores the pool, starts the recurring TTL-rotation task (unless
/// `PULSUS_SKIP_DDL` — same gate as the reconcile: skip_ddl means the
/// schema controller is fully off, rotation included), and exits. A serving
/// process (`all`/`writer`/`reader`) must create the database/tables and
/// keep the TTL current just like `--mode init` + a standing schema
/// controller would (docs/architecture.md §1: `all` mounts "writer +
/// reader + ruler + **schema controller**"; docs/configuration.md:
/// `CLICKHOUSE_DB` is "created by init/startup unless `PULSUS_SKIP_DDL=1`",
/// and `PULSUS_ROTATION_INTERVAL` is "how often the schema controller
/// re-applies TTL/rotation") — otherwise a fresh ClickHouse with no
/// pre-existing database leaves `/ready` at 503 forever, and a changed
/// `PULSUS_RETENTION_DAYS` never propagates without a restart.
/// `ChPool::connect` pings fail-fast on any failure, so a cold or
/// unreachable ClickHouse must never block startup — `/ready` reports 503
/// while `pool_slot` is `None` and reflects live ping results once this
/// loop's first successful pass lands.
fn spawn_reconnect_loop(
    pool_slot: Arc<RwLock<Option<Arc<ChPool>>>>,
    writer_slot: Arc<OnceLock<Arc<LogWriter>>>,
    metric_writer_slot: Arc<OnceLock<Arc<MetricWriter>>>,
    label_cache_slot: Arc<OnceLock<Arc<LabelCache>>>,
    config: Arc<Config>,
    rotation_tx: oneshot::Sender<JoinHandle<()>>,
    label_cache_refresh_tx: oneshot::Sender<JoinHandle<()>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = INITIAL_BACKOFF;
        loop {
            match ensure_schema_then_connect(&config).await {
                Ok(pool) => {
                    tracing::info!("clickhouse schema ready; pool established");
                    let pool = Arc::new(pool);
                    // Construct and store the writer(s) *before* the pool
                    // (issue #15 architect plan): `/ready`=200 (which gates
                    // on `pool_slot`) must imply the ingest route is live
                    // too, not just that the reader's pool exists.
                    if writer_enabled(&config) {
                        let client = Arc::new(ChClient::from_shared_pool(
                            Arc::clone(&pool),
                            config.query_timeout.0,
                        ));
                        let writer = Arc::new(LogWriter::new_with_tables(
                            client.clone(),
                            &config.writer,
                            writer_tables_from(&config),
                        ));
                        // `spawn_reconnect_loop` is a one-shot bootstrap
                        // that `return`s on this, its first successful
                        // pass (see this fn's own doc comment) — the slot
                        // can therefore never already be set, so a `set`
                        // failure is unreachable.
                        let _ = writer_slot.set(writer);

                        // `MetricWriter` (issue #26 architect plan): same
                        // client, same lifecycle gate — shares one
                        // ClickHouse connection pool with `LogWriter`
                        // rather than opening a second one.
                        let bucket_ms = config.reader.series_activity_bucket.0.as_millis() as i64;
                        let metric_writer = Arc::new(MetricWriter::new_with_tables(
                            client,
                            &config.writer,
                            bucket_ms,
                            metric_writer_tables_from(&config),
                        ));
                        let _ = metric_writer_slot.set(metric_writer);
                    }
                    // The label cache (issue #30 architect plan; code-review
                    // round-1 fix): built and stored *before* `pool_slot` is
                    // published, mirroring the writer-before-pool precedent
                    // above (issue #15) for the identical reason —
                    // `/ready`'s pool check runs first, but on the
                    // multi-threaded runtime another task can observe
                    // `pool_slot = Some` the instant this line below runs,
                    // with nothing forcing it to also observe
                    // `label_cache_slot` already set; `label_cache_ready`
                    // maps an unset slot to 200 (the correct behavior for
                    // writer/init modes), so a `None` slot here would let a
                    // concurrent `/ready` probe pass before the cache even
                    // exists. Publishing the slot first closes that window:
                    // `pool_slot = Some` now implies `label_cache_slot =
                    // Some(cache)` by construction, so `/ready` only ever
                    // needs `label_cache_ready` + `LabelCache::is_warm` to
                    // gate the rest (cold-cache "label cache warming" 503
                    // for the whole first sweep).
                    if reader_enabled(&config) {
                        let cache = Arc::new(build_label_cache(Arc::clone(&pool), &config));
                        let _ = label_cache_slot.set(Arc::clone(&cache));
                        let refresh_handle =
                            pulsus_read::spawn_refresh_loop(cache, config.reader.cache_ttl.0);
                        let _ = label_cache_refresh_tx.send(refresh_handle);
                    }
                    *pool_slot.write().await = Some(Arc::clone(&pool));
                    if rotation_enabled(&config) {
                        let handle = spawn_rotation_task(Arc::clone(&config));
                        // The receiver may already be gone (e.g. `run` is
                        // tearing down after a pre-listen failure); in that
                        // case there is nothing left to hand the handle to,
                        // so drop it — the task itself keeps running
                        // detached until the process exits.
                        let _ = rotation_tx.send(handle);
                    }
                    return;
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        backoff_ms = backoff.as_millis() as u64,
                        "clickhouse startup step failed; retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
                }
            }
        }
    })
}

/// Whether the reconnect loop should start the TTL-rotation task after a
/// successful schema-ensure step — the same gate as the reconcile step
/// itself (`PULSUS_SKIP_DDL`): skip_ddl means the schema controller,
/// rotation included, is fully off. Pure so the gate is unit-tested without
/// touching the network.
fn rotation_enabled(config: &Config) -> bool {
    !config.skip_ddl
}

/// Whether this process mounts the writer subsystem (`docs/architecture.md
/// §1`'s mode table: `all`/`writer` mount ingestion APIs, `reader` does
/// not) — the reconnect loop's gate on constructing a `LogWriter` at all
/// (issue #15 architect plan). Pure so the gate is unit-tested without
/// touching the network, mirroring [`rotation_enabled`]'s idiom.
fn writer_enabled(cfg: &Config) -> bool {
    matches!(cfg.mode, Mode::All | Mode::Writer)
}

/// Whether this process mounts the reader subsystem (`docs/architecture.md
/// §1`'s mode table: `all`/`reader` mount query APIs, `writer` does not) —
/// the reconnect loop's gate on constructing a [`LabelCache`] at all (issue
/// #30 architect plan). Pure so the gate is unit-tested without touching
/// the network, mirroring [`writer_enabled`]'s idiom.
fn reader_enabled(cfg: &Config) -> bool {
    matches!(cfg.mode, Mode::All | Mode::Reader)
}

/// Spawns the recurring TTL-rotation task in a self-healing shape (issue #6
/// review finding: a one-shot `ChClient::new` before the task ever started
/// meant a single transient connect failure at spawn time permanently
/// disabled rotation for the rest of the process's life, even though
/// `/ready` was already 200). Unlike `pulsus_schema::spawn_rotation` (which
/// takes an already-built client and is left connection-agnostic on
/// purpose), this task (re)builds its own `ChClient` lazily, on the
/// interval tick itself: a failed connect or a failed `apply_ttl` both just
/// log a warning and try again next tick — nothing here is ever permanent,
/// so this always returns a live `JoinHandle` immediately, never an
/// `Option`. Bound to the *target* database (unlike the bootstrap
/// connection `reconcile_schema` uses — rotation's `ALTER TABLE` statements
/// are `{{db}}.`-qualified against the real, by-now-created database).
fn spawn_rotation_task(config: Arc<Config>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let params = schema_params_from(&config);
        let mut interval = tokio::time::interval(config.rotation_interval.0);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut client: Option<ChClient> = None;
        loop {
            interval.tick().await;
            rotation_tick(
                &mut client,
                || ChClient::new(conn_config_from(&config)),
                |c| {
                    let params = &params;
                    async move {
                        let result = pulsus_schema::apply_ttl(&c, params).await;
                        (c, result)
                    }
                },
            )
            .await;
        }
    })
}

/// One rotation tick's self-healing core: (re)connects lazily only when
/// `client` is `None`, then applies TTL through it. A failed connect leaves
/// `client` as `None` (retried next tick); a failed apply also leaves
/// `client` as `None` afterwards (forces a fresh connection next tick, in
/// case the cached one is the reason the apply failed). `apply` takes and
/// hands back ownership of `T` (rather than borrowing it) purely so this
/// stays free of higher-ranked-lifetime gymnastics on the closure's return
/// type; the effect is identical to borrowing. Generic over both effects so
/// the retry/self-healing state machine is unit-tested with fake, in-memory
/// failures — no live ClickHouse required to prove recovery (issue #6
/// review finding).
async fn rotation_tick<T, EC, EA, FC, FA>(
    client: &mut Option<T>,
    connect: impl FnOnce() -> FC,
    apply: impl FnOnce(T) -> FA,
) where
    FC: Future<Output = Result<T, EC>>,
    FA: Future<Output = (T, Result<(), EA>)>,
    EC: std::fmt::Display,
    EA: std::fmt::Display,
{
    let active = match client.take() {
        Some(c) => c,
        None => match connect().await {
            Ok(c) => c,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "rotation: failed to (re)connect to clickhouse; retrying next tick"
                );
                return;
            }
        },
    };
    let (active, result) = apply(active).await;
    match result {
        Ok(()) => *client = Some(active),
        Err(err) => {
            tracing::warn!(error = %err, "rotation: failed to reapply TTL; will retry next tick");
            // Deliberately do not restore `client`: a failed apply forces a
            // fresh connection attempt on the next tick.
        }
    }
}

/// One attempt at "ensure the schema, then connect the serving pool" — the
/// unit the reconnect loop retries as a whole. Retrying the pair together
/// (rather than caching a one-off "schema is done" flag) keeps this
/// self-healing: if the pool connect step fails after a successful
/// reconcile, the next attempt simply reconciles again, which is a
/// idempotent no-op (`pulsus_schema::run_init`'s contract).
async fn ensure_schema_then_connect(config: &Config) -> Result<ChPool, StartupError> {
    if !config.skip_ddl {
        reconcile_schema(config).await?;
    }
    ChPool::connect(conn_config_from(config))
        .await
        .map_err(StartupError::Pool)
}

/// Runs the same version-gate → reconcile → apply-TTL pipeline `--mode init`
/// runs (`schema_init::run`), against a bootstrap connection to
/// ClickHouse's built-in `default` database (the target database does not
/// exist yet on a fresh server — see `chconfig::bootstrap_conn_config_from`).
async fn reconcile_schema(config: &Config) -> Result<(), StartupError> {
    let client = ChClient::new(bootstrap_conn_config_from(config))
        .await
        .map_err(StartupError::Bootstrap)?;
    let params = schema_params_from(config);
    pulsus_schema::run_init(&client, &params)
        .await
        .map_err(StartupError::Schema)
}

/// Initializes the global `tracing` subscriber from `cfg.log_level`
/// (`PULSUS_LOG_LEVEL`, already parsed by `pulsus-config` — the single
/// source for that env var). `tracing`'s global subscriber is a
/// process-global singleton, so a second call (e.g. across tests in one
/// binary) is expected to fail; that failure is ignored rather than
/// propagated, matching the metrics recorder's same caveat below.
fn init_tracing(log_level: LogLevel) {
    let directive = match log_level {
        LogLevel::Error => "error",
        LogLevel::Warn => "warn",
        LogLevel::Info => "info",
        LogLevel::Debug => "debug",
        LogLevel::Trace => "trace",
    };
    let filter = EnvFilter::try_new(directive).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

/// Installs the process-global Prometheus recorder. The recorder is a
/// process-global singleton (`metrics::set_global_recorder`); a second
/// install attempt within the same process is expected to fail (multiple
/// tests in one binary), in which case a standalone, unlinked handle is
/// returned instead of a hard startup error — `/metrics` still renders
/// (empty) rather than the process refusing to start.
fn install_metrics_recorder() -> PrometheusHandle {
    match PrometheusBuilder::new().install_recorder() {
        Ok(handle) => handle,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "prometheus recorder already installed for this process; using a standalone handle"
            );
            PrometheusBuilder::new().build_recorder().handle()
        }
    }
}

/// Waits for SIGINT (Ctrl+C) or, on Unix, SIGTERM — whichever arrives
/// first. `axum::serve(...).with_graceful_shutdown` awaits this future
/// before draining in-flight requests and returning.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(err) => {
                tracing::error!(error = %err, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!("shutdown signal received; draining in-flight requests");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reconnect_loop_handle_is_abortable_before_it_ever_connects() {
        let mut cfg = Config::default();
        cfg.clickhouse.http_port = 1; // nothing listens here
        cfg.clickhouse.pool_size = 1;
        let (rotation_tx, _rotation_rx) = oneshot::channel();
        let (label_cache_refresh_tx, _label_cache_refresh_rx) = oneshot::channel();
        let handle = spawn_reconnect_loop(
            Arc::new(RwLock::new(None)),
            Arc::new(OnceLock::new()),
            Arc::new(OnceLock::new()),
            Arc::new(OnceLock::new()),
            Arc::new(cfg),
            rotation_tx,
            label_cache_refresh_tx,
        );
        assert!(!handle.is_finished());
        handle.abort();
        let result = handle.await;
        assert!(result.is_err_and(|e| e.is_cancelled()));
    }

    #[test]
    fn rotation_enabled_follows_skip_ddl() {
        let mut cfg = Config::default();
        assert!(rotation_enabled(&cfg));
        cfg.skip_ddl = true;
        assert!(!rotation_enabled(&cfg));
    }

    #[test]
    fn writer_enabled_follows_the_mode_table() {
        use pulsus_config::Mode;

        for mode in [Mode::All, Mode::Writer] {
            let cfg = Config {
                mode,
                ..Config::default()
            };
            assert!(writer_enabled(&cfg), "{mode:?} must mount the writer");
        }
        for mode in [Mode::Reader, Mode::Init] {
            let cfg = Config {
                mode,
                ..Config::default()
            };
            assert!(!writer_enabled(&cfg), "{mode:?} must not mount the writer");
        }
    }

    #[test]
    fn reader_enabled_follows_the_mode_table() {
        use pulsus_config::Mode;

        for mode in [Mode::All, Mode::Reader] {
            let cfg = Config {
                mode,
                ..Config::default()
            };
            assert!(reader_enabled(&cfg), "{mode:?} must mount the reader");
        }
        for mode in [Mode::Writer, Mode::Init] {
            let cfg = Config {
                mode,
                ..Config::default()
            };
            assert!(!reader_enabled(&cfg), "{mode:?} must not mount the reader");
        }
    }

    /// Load-bearing regression test for the round-3 review finding: a
    /// connect failure on one tick must not permanently disable rotation —
    /// the very next tick must retry and succeed. Uses fake, in-memory
    /// connect/apply effects (no live ClickHouse) so the self-healing state
    /// machine itself is proven deterministically.
    #[tokio::test]
    async fn rotation_tick_recovers_after_an_initial_connect_failure() {
        let mut client: Option<u32> = None;

        // Tick 1: connect fails — must not panic, must leave `client` unset.
        rotation_tick(
            &mut client,
            || async { Err::<u32, &'static str>("connect refused") },
            |c: u32| async move { (c, Ok::<(), &'static str>(())) },
        )
        .await;
        assert!(client.is_none());

        // Tick 2: connect now succeeds — this is the recovery under test.
        rotation_tick(
            &mut client,
            || async { Ok::<u32, &'static str>(42) },
            |c: u32| async move { (c, Ok::<(), &'static str>(())) },
        )
        .await;
        assert_eq!(client, Some(42));
    }

    /// Stand-in "connect" for tests where a cached client must make the
    /// connect step unreachable; a plain fn item (rather than a closure)
    /// gives the compiler a concrete, non-`!` return type to infer against.
    async fn unreachable_connect() -> Result<u32, &'static str> {
        panic!("connect must not be attempted when a client is already cached")
    }

    #[tokio::test]
    async fn rotation_tick_forces_a_fresh_connect_after_a_failed_apply() {
        let mut client = Some(7u32);
        rotation_tick(&mut client, unreachable_connect, |c: u32| async move {
            (c, Err::<(), &'static str>("ttl apply failed"))
        })
        .await;
        assert!(
            client.is_none(),
            "a failed apply must force a fresh connect attempt next tick"
        );
    }

    #[tokio::test]
    async fn rotation_tick_leaves_a_healthy_cached_client_in_place() {
        let mut client = Some(7u32);
        rotation_tick(&mut client, unreachable_connect, |c: u32| async move {
            (c, Ok::<(), &'static str>(()))
        })
        .await;
        assert_eq!(client, Some(7));
    }

    /// The real (non-fake) task must always return a live handle
    /// immediately, even against an unreachable ClickHouse — the whole
    /// point of the round-3 fix is that there is no more fallible,
    /// pre-spawn connection attempt for `Option<JoinHandle>` to be `None`
    /// from.
    #[tokio::test]
    async fn spawn_rotation_task_always_returns_a_handle_even_when_clickhouse_is_unreachable() {
        let cfg = Config {
            clickhouse: pulsus_config::ClickHouseConfig {
                http_port: 1, // nothing listens here
                ..Config::default().clickhouse
            },
            rotation_interval: pulsus_config::HumanDuration(Duration::from_millis(20)),
            ..Config::default()
        };
        let handle = spawn_rotation_task(Arc::new(cfg));
        assert!(!handle.is_finished());
        handle.abort();
        let result = handle.await;
        assert!(result.is_err_and(|e| e.is_cancelled()));
    }

    /// Regression test for the review finding: the rotation task must be
    /// stopped (aborted and joined) at shutdown whenever one was started.
    #[tokio::test]
    async fn shutdown_background_tasks_stops_a_pending_rotation_task_too() {
        let reconnect_handle = tokio::spawn(async {}); // already-finished stand-in
        let (rotation_tx, rotation_rx) = oneshot::channel();
        let rotation_handle = tokio::spawn(std::future::pending::<()>());
        rotation_tx
            .send(rotation_handle)
            .unwrap_or_else(|_| panic!("receiver must still be open"));
        // No label cache refresh task in this scenario — drop the sender
        // immediately so its receiver resolves to `Err` right away, rather
        // than hanging on a sender that is merely unused-but-still-alive
        // for the rest of this test's scope.
        let (label_cache_refresh_tx, label_cache_refresh_rx) = oneshot::channel::<JoinHandle<()>>();
        drop(label_cache_refresh_tx);

        // Bounded so a regression that hangs shutdown fails this test
        // instead of the whole suite.
        tokio::time::timeout(
            Duration::from_secs(5),
            shutdown_background_tasks(reconnect_handle, rotation_rx, label_cache_refresh_rx),
        )
        .await
        .expect("shutdown_background_tasks must not hang on a pending rotation task");
    }

    /// The label cache refresh task's shutdown-ordering counterpart to
    /// `shutdown_background_tasks_stops_a_pending_rotation_task_too` (issue
    /// #30 architect plan: "abort/join the refresh handle in the shutdown
    /// ordering next to rotation").
    #[tokio::test]
    async fn shutdown_background_tasks_stops_a_pending_label_cache_refresh_task_too() {
        let reconnect_handle = tokio::spawn(async {});
        // No rotation task in this scenario — drop the sender immediately
        // (same reasoning as the mirrored fix in
        // `shutdown_background_tasks_stops_a_pending_rotation_task_too`).
        let (rotation_tx, rotation_rx) = oneshot::channel::<JoinHandle<()>>();
        drop(rotation_tx);
        let (label_cache_refresh_tx, label_cache_refresh_rx) = oneshot::channel();
        let refresh_handle = tokio::spawn(std::future::pending::<()>());
        label_cache_refresh_tx
            .send(refresh_handle)
            .unwrap_or_else(|_| panic!("receiver must still be open"));

        tokio::time::timeout(
            Duration::from_secs(5),
            shutdown_background_tasks(reconnect_handle, rotation_rx, label_cache_refresh_rx),
        )
        .await
        .expect("shutdown_background_tasks must not hang on a pending label cache refresh task");
    }

    /// `PULSUS_SKIP_DDL=1` (or a reconnect loop aborted before ever
    /// reconciling): no rotation task was ever started, so shutdown must be
    /// a clean no-op rather than hanging on an empty channel. Same for the
    /// label cache refresh task in writer-only/init modes.
    #[tokio::test]
    async fn shutdown_background_tasks_is_a_no_op_when_no_rotation_was_ever_started() {
        let reconnect_handle = tokio::spawn(async {});
        let (rotation_tx, rotation_rx) = oneshot::channel::<JoinHandle<()>>();
        drop(rotation_tx);
        let (label_cache_refresh_tx, label_cache_refresh_rx) = oneshot::channel::<JoinHandle<()>>();
        drop(label_cache_refresh_tx);

        tokio::time::timeout(
            Duration::from_secs(5),
            shutdown_background_tasks(reconnect_handle, rotation_rx, label_cache_refresh_rx),
        )
        .await
        .expect("shutdown_background_tasks must not hang when rotation was never started");
    }

    /// Load-bearing regression test for the review finding: by default
    /// (`skip_ddl = false`) startup must attempt the schema-bootstrap
    /// connection first, not jump straight to the serving pool — otherwise a
    /// fresh ClickHouse (missing database) never becomes ready.
    #[tokio::test]
    async fn ensure_schema_then_connect_reconciles_schema_first_by_default() {
        let mut cfg = Config::default();
        cfg.clickhouse.http_port = 1; // nothing listens here
        // `ChPool` is not `Debug` (pulsus-clickhouse), so match manually
        // instead of `.expect_err`/`.unwrap_err`.
        let err = match ensure_schema_then_connect(&cfg).await {
            Err(err) => err,
            Ok(_) => panic!("nothing listens on port 1"),
        };
        assert!(matches!(err, StartupError::Bootstrap(_)));
    }

    /// `PULSUS_SKIP_DDL=1` must skip the reconcile step entirely and go
    /// straight to the serving pool connect (schema assumed pre-existing).
    #[tokio::test]
    async fn ensure_schema_then_connect_skips_reconcile_when_skip_ddl_is_set() {
        let mut cfg = Config::default();
        cfg.clickhouse.http_port = 1; // nothing listens here
        cfg.skip_ddl = true;
        let err = match ensure_schema_then_connect(&cfg).await {
            Err(err) => err,
            Ok(_) => panic!("nothing listens on port 1"),
        };
        assert!(matches!(err, StartupError::Pool(_)));
    }

    #[test]
    fn install_metrics_recorder_does_not_panic_when_called_twice() {
        let _ = install_metrics_recorder();
        // Second call in the same process: must fall back gracefully
        // ("ignore already-set in tests" per the architect plan), not panic.
        let _ = install_metrics_recorder();
    }

    #[test]
    fn init_tracing_does_not_panic_when_called_twice() {
        init_tracing(LogLevel::Debug);
        init_tracing(LogLevel::Debug);
    }
}
