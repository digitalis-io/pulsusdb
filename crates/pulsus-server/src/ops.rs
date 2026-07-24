//! `/ready`, `/metrics`, `/config`, `/buildinfo` (docs/api.md §7). Split
//! into a public sub-router (`/ready`, `/metrics`) and an authed sub-router
//! (`/config`, `/buildinfo`) per the architect plan amendment — see
//! `app::build_router` for how the two are composed with the rest of the
//! middleware stack.

use std::future::Future;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use pulsus_clickhouse::ChError;
use pulsus_read::LabelCache;
use pulsus_write::writer::{
    BackfillMetricsSnapshot, MetricWriterMetricsSnapshot, TableMetricsSnapshot,
    TraceWriterMetricsSnapshot, WriterMetricsSnapshot,
};

use crate::app::AppState;

/// `/ready` and `/metrics` — always unauthenticated (probes/scrapers must
/// work credential-free) and never subject to the generic query timeout
/// (amendment F1/F2 — enforced by composition in `app::build_router`, not
/// here).
pub(crate) fn ops_public_router() -> Router<AppState> {
    Router::new()
        .route("/ready", get(ready))
        .route("/metrics", get(metrics_handler))
}

/// `/config` and `/buildinfo` — inside auth (when configured) and the
/// generic timeout.
pub(crate) fn ops_authed_router() -> Router<AppState> {
    Router::new()
        .route("/config", get(config_handler))
        .route("/buildinfo", get(buildinfo_handler))
}

/// The hard deadline `/ready` gives ClickHouse's `ping` before mapping the
/// attempt to 503 — independent of `PULSUS_QUERY_TIMEOUT` (amendment F2): a
/// slow or hanging ClickHouse must still produce the documented 503, never
/// the generic timeout's 408/504.
const READY_PING_TIMEOUT: Duration = Duration::from_secs(2);

/// `GET /ready` (docs/api.md §7): 200 only after a live, successful
/// ClickHouse ping — and, in reader-enabled modes, a warm label cache
/// (issue #30 architect plan) — 503 (with a short reason body) for "pool
/// not yet established", "ping failed", "ping exceeded 2s", and "label
/// cache warming" alike. The pool `Option` is cloned out from behind the
/// lock with the guard dropped before the `.await` on the ping itself, so
/// the lock is never held across an await point; the label cache check is
/// a lock-free `OnceLock::get` + `LabelCache::is_warm` (itself lock-free,
/// see its own doc comment), so no lock is ever held across an `.await`
/// here either.
async fn ready(State(state): State<AppState>) -> Response {
    let pool = {
        let guard = state.pool.read().await;
        guard.clone()
    };
    let Some(pool) = pool else {
        return unavailable("clickhouse pool not yet established");
    };
    let ping = ready_from_ping(async move { pool.ping().await }).await;
    if ping.status() != StatusCode::OK {
        return ping;
    }
    label_cache_ready(state.label_cache.get())
}

/// The label-cache half of [`ready`]'s gate, decoupled from `AppState` so
/// the "unset slot" branch is unit-testable without a `LabelCache`
/// (constructing one always needs a live `ChClient`, unlike `ChPool`'s own
/// [`ready_from_ping`] decoupling). `None` covers both "not yet constructed
/// by the reconnect loop" (a reader-enabled process still warming up) and
/// "this process never mounts the reader subsystem" (writer/init modes) —
/// the latter is permanent, the former resolves once the reconnect loop's
/// first pass completes; either way, a *present* cache that is not yet warm
/// is the only branch gated here (issue #30 architect plan).
fn label_cache_ready(cache: Option<&std::sync::Arc<LabelCache>>) -> Response {
    match cache {
        Some(cache) if !cache.is_warm() => unavailable("label cache warming"),
        _ => StatusCode::OK.into_response(),
    }
}

/// The 503-mapping core of [`ready`], decoupled from `ChPool` so the
/// "ping exceeds the deadline" branch is unit-testable without a live (or
/// even fake-hanging) ClickHouse connection.
async fn ready_from_ping<F>(ping: F) -> Response
where
    F: Future<Output = Result<(), ChError>>,
{
    match tokio::time::timeout(READY_PING_TIMEOUT, ping).await {
        Ok(Ok(())) => StatusCode::OK.into_response(),
        Ok(Err(err)) => unavailable(&format!("clickhouse ping failed: {err}")),
        Err(_elapsed) => unavailable("clickhouse ping exceeded 2s"),
    }
}

fn unavailable(reason: &str) -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, reason.to_string()).into_response()
}

/// `GET /metrics`: Prometheus exposition of PulsusDB internals. In
/// reader-enabled modes (`state.label_cache.get()` is `Some`), first
/// bridges the label cache's counters/gauges through the `metrics` facade
/// (issue #30 AC: "cache hit/size/age metrics on `/metrics`" — code-review
/// round-2 fix; not deferred the way `pulsus-write`'s `WriterMetrics` is)
/// so `state.metrics.render()` picks up freshly-set values in the very
/// same scrape, never a value from a prior request.
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    if let Some(cache) = state.label_cache.get() {
        record_label_cache_metrics(cache);
    }
    record_eval_gate_metrics(&state.eval_gate);
    record_ingest_metrics(&state);
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
}

/// Bridges [`pulsus_read::CacheMetricsSnapshot`] (plus the scrape-time-
/// derived [`LabelCache::age_ms`]) through the `metrics` facade. Counters
/// use `.absolute()`, not `.increment()`: this crate does not own the
/// underlying atomics, it just mirrors their current value on every
/// scrape, so setting the absolute count each time is the correct
/// operation (an `.increment()` here would double-count against the
/// exporter's own running total). `misses_total` carries one `reason`
/// label per [`pulsus_read::FallbackReason`] variant, matching Prometheus's
/// labelled-counter idiom rather than five separate metric names.
fn record_label_cache_metrics(cache: &LabelCache) {
    let snap = cache.metrics();
    metrics::gauge!("pulsus_label_cache_series_count").set(snap.series_count as f64);
    metrics::gauge!("pulsus_label_cache_oversize").set(if snap.oversize { 1.0 } else { 0.0 });
    if let Some(age_ms) = cache.age_ms() {
        metrics::gauge!("pulsus_label_cache_age_ms").set(age_ms as f64);
    }
    metrics::counter!("pulsus_label_cache_hits_total").absolute(snap.hits_total);
    metrics::counter!("pulsus_label_cache_misses_total", "reason" => "cold")
        .absolute(snap.miss_cold_total);
    metrics::counter!("pulsus_label_cache_misses_total", "reason" => "stale")
        .absolute(snap.miss_stale_total);
    metrics::counter!("pulsus_label_cache_misses_total", "reason" => "out_of_window")
        .absolute(snap.miss_out_of_window_total);
    metrics::counter!("pulsus_label_cache_misses_total", "reason" => "over_cardinality")
        .absolute(snap.miss_over_cardinality_total);
    metrics::counter!("pulsus_label_cache_misses_total", "reason" => "regex_unsupported")
        .absolute(snap.miss_regex_unsupported_total);
    metrics::counter!("pulsus_label_cache_misses_total", "reason" => "scan_budget")
        .absolute(snap.miss_scan_budget_total);
    metrics::counter!("pulsus_label_cache_refreshes_total").absolute(snap.refreshes_total);
    metrics::counter!("pulsus_label_cache_refresh_failures_total")
        .absolute(snap.refresh_failures_total);
}

/// Issue #101: bridges the read path's [`pulsus_read::EvalGateSnapshot`]
/// through the `metrics` facade on scrape (same snapshot→pull model as
/// [`record_label_cache_metrics`], so the read path never touches the
/// `metrics` facade in its hot loop). Thin wrapper over
/// [`record_eval_gate_snapshot`] — the split exists so the exposition can be
/// unit-tested with pinned `EvalGateSnapshot` values instead of driving a
/// real gate through contention.
fn record_eval_gate_metrics(gate: &pulsus_read::EvalGate) {
    record_eval_gate_snapshot(&gate.snapshot());
}

/// Issue #101 (re-review comment 5011870282): gauges use `.set()`; the
/// counters use `.absolute()` (this crate mirrors the gate's own atomics, it
/// does not own the exporter's running total). `wait_nanos_total` is
/// exported verbatim as `pulsus_query_eval_wait_nanoseconds_total` — the
/// prior `pulsus_query_eval_wait_seconds_total` divided by
/// `1_000_000_000` with integer division, so it read `0` until a full
/// cumulative *second* of contended waiting had ever accrued (eval-gate
/// waits are typically milliseconds); the `metrics` 0.24 `Counter` facade
/// is `u64`-only, so sub-second seconds are unrepresentable under that
/// name/type. The source atomic is already exact (and, since #101's
/// saturating-accumulator hardening, saturation-capped at `u64::MAX` —
/// `eval_gate.rs`'s `add_wait_nanos`), so this exports it directly with no
/// division.
fn record_eval_gate_snapshot(snap: &pulsus_read::EvalGateSnapshot) {
    metrics::gauge!("pulsus_query_eval_permits_limit").set(snap.limit as f64);
    metrics::gauge!("pulsus_query_eval_permits_available").set(snap.available as f64);
    metrics::gauge!("pulsus_query_eval_in_flight").set(snap.in_flight as f64);
    metrics::gauge!("pulsus_query_eval_waiting").set(snap.waiting as f64);
    metrics::counter!("pulsus_query_eval_contended_total").absolute(snap.contended_total);
    metrics::counter!("pulsus_query_eval_wait_nanoseconds_total").absolute(snap.wait_nanos_total);
}

/// Issue #214: bridges the writers' ingest atomics onto `/metrics` as the
/// `pulsus_ingest_*` series on scrape — the same snapshot→pull model as
/// [`record_label_cache_metrics`]/[`record_eval_gate_metrics`], so the
/// ingest hot path never touches the `metrics` facade. Each sink's
/// `metrics()` returns `Some` only when its `OnceLock` writer slot is filled
/// (a `writer`/`all` role); a reader-only process leaves every slot empty,
/// so `None` short-circuits and NO `pulsus_ingest_*` series appear. The
/// snapshot reads are read-only `load(Relaxed)` on already-maintained
/// atomics — no new per-sample work, no ClickHouse round-trip.
fn record_ingest_metrics(state: &AppState) {
    if let Some(snap) = state.writer.metrics() {
        record_log_ingest_snapshot(&snap);
    }
    if let Some(snap) = state.metric_writer.metrics() {
        record_metric_ingest_snapshot(&snap);
    }
    if let Some(snap) = state.trace_writer.metrics() {
        record_trace_ingest_snapshot(&snap);
    }
}

/// Emits the per-table `pulsus_ingest_*` series (label `table`) shared by all
/// three signals. Counters use `.absolute()` — this crate mirrors the
/// writer's own atomics on every scrape, it never owns the exporter's
/// running total (an `.increment()` would double-count). `flush_latency` is
/// exported in **nanoseconds verbatim** (`pulsus_ingest_flush_latency_nanoseconds_total`),
/// not divided to seconds: the `metrics` 0.24 counter facade is `u64`-only,
/// so a `/1e9` would truncate sub-second flush sums to `0` — the same
/// reasoning as `pulsus_query_eval_wait_nanoseconds_total`.
fn record_table_metrics(table: &'static str, t: &TableMetricsSnapshot) {
    metrics::counter!("pulsus_ingest_rows_total", "table" => table).absolute(t.rows_total);
    metrics::counter!("pulsus_ingest_bytes_total", "table" => table).absolute(t.bytes_total);
    metrics::counter!("pulsus_ingest_flushes_total", "table" => table).absolute(t.flushes_total);
    metrics::counter!("pulsus_ingest_flush_latency_nanoseconds_total", "table" => table)
        .absolute(t.flush_latency_sum_ns);
    metrics::counter!("pulsus_ingest_retries_total", "table" => table).absolute(t.retries_total);
    metrics::gauge!("pulsus_ingest_inflight", "table" => table).set(t.inflight as f64);
    metrics::counter!("pulsus_ingest_spool_write_failures_total", "table" => table)
        .absolute(t.spool_write_failures_total);
}

/// The log writer's `pulsus_ingest_*` series: per-table (`log_samples`/
/// `log_streams`/`log_patterns`), per-signal (`signal="logs"`),
/// registration-cache, and backfill (`backlog="log_streams"`).
fn record_log_ingest_snapshot(s: &WriterMetricsSnapshot) {
    record_table_metrics("log_samples", &s.samples);
    record_table_metrics("log_streams", &s.streams);
    record_table_metrics("log_patterns", &s.patterns);

    metrics::gauge!("pulsus_ingest_queue_bytes", "signal" => "logs").set(s.queue_bytes as f64);
    metrics::counter!("pulsus_ingest_backpressure_total", "signal" => "logs")
        .absolute(s.backpressure_total);
    metrics::counter!("pulsus_ingest_spool_poison_total", "signal" => "logs")
        .absolute(s.spool_poison_total);
    metrics::counter!("pulsus_ingest_spool_uncertain_total", "signal" => "logs")
        .absolute(s.spool_uncertain_total);
    metrics::counter!("pulsus_ingest_rejected_total", "signal" => "logs")
        .absolute(s.rejected_total);

    metrics::counter!("pulsus_ingest_registrations_total", "signal" => "logs")
        .absolute(s.stream_registrations_total);
    metrics::counter!("pulsus_ingest_registration_cache_hits_total", "signal" => "logs")
        .absolute(s.lru_hits_total);
    metrics::counter!("pulsus_ingest_registration_cache_misses_total", "signal" => "logs")
        .absolute(s.lru_misses_total);
    metrics::counter!("pulsus_ingest_collisions_total", "signal" => "logs")
        .absolute(s.collisions_total);
    metrics::counter!("pulsus_ingest_patterns_dropped_total", "signal" => "logs")
        .absolute(s.patterns_dropped_total);

    record_backfill_metrics(
        "log_streams",
        &BackfillMetricsSnapshot {
            enqueued_total: s.backfill_enqueued_total,
            dropped_total: s.backfill_dropped_total,
            retries_total: s.backfill_retries_total,
            healed_total: s.backfill_healed_total,
            abandoned_total: s.backfill_abandoned_total,
            pending: s.backfill_pending,
        },
    );
}

/// The metric writer's `pulsus_ingest_*` series: per-table (`metric_samples`/
/// `metric_series`/`metric_metadata`/`metric_hist_samples`), per-signal
/// (`signal="metrics"`), registration-cache, `metadata_upserts`, and backfill
/// (`backlog="metric_series"`/`metric_metadata"`).
fn record_metric_ingest_snapshot(s: &MetricWriterMetricsSnapshot) {
    record_table_metrics("metric_samples", &s.samples);
    record_table_metrics("metric_series", &s.series);
    record_table_metrics("metric_metadata", &s.metadata);
    record_table_metrics("metric_hist_samples", &s.hist_samples);

    metrics::gauge!("pulsus_ingest_queue_bytes", "signal" => "metrics").set(s.queue_bytes as f64);
    metrics::counter!("pulsus_ingest_backpressure_total", "signal" => "metrics")
        .absolute(s.backpressure_total);
    metrics::counter!("pulsus_ingest_spool_poison_total", "signal" => "metrics")
        .absolute(s.spool_poison_total);
    metrics::counter!("pulsus_ingest_spool_uncertain_total", "signal" => "metrics")
        .absolute(s.spool_uncertain_total);
    metrics::counter!("pulsus_ingest_rejected_total", "signal" => "metrics")
        .absolute(s.rejected_total);

    metrics::counter!("pulsus_ingest_registrations_total", "signal" => "metrics")
        .absolute(s.series_registrations_total);
    metrics::counter!("pulsus_ingest_registration_cache_hits_total", "signal" => "metrics")
        .absolute(s.series_lru_hits_total);
    metrics::counter!("pulsus_ingest_registration_cache_misses_total", "signal" => "metrics")
        .absolute(s.series_lru_misses_total);
    metrics::counter!("pulsus_ingest_collisions_total", "signal" => "metrics")
        .absolute(s.collisions_total);
    metrics::counter!("pulsus_ingest_metadata_upserts_total", "signal" => "metrics")
        .absolute(s.metadata_upserts_total);

    record_backfill_metrics("metric_series", &s.series_backfill);
    record_backfill_metrics("metric_metadata", &s.metadata_backfill);
}

/// The trace writer's `pulsus_ingest_*` series: per-table (`trace_spans`/
/// `trace_attrs_idx`), per-signal (`signal="traces"`), and backfill
/// (`backlog="trace_attrs_idx"`). Traces have no registration-cache or
/// collision counters (no label sets / LRU).
fn record_trace_ingest_snapshot(s: &TraceWriterMetricsSnapshot) {
    record_table_metrics("trace_spans", &s.spans);
    record_table_metrics("trace_attrs_idx", &s.attrs);

    metrics::gauge!("pulsus_ingest_queue_bytes", "signal" => "traces").set(s.queue_bytes as f64);
    metrics::counter!("pulsus_ingest_backpressure_total", "signal" => "traces")
        .absolute(s.backpressure_total);
    metrics::counter!("pulsus_ingest_spool_poison_total", "signal" => "traces")
        .absolute(s.spool_poison_total);
    metrics::counter!("pulsus_ingest_spool_uncertain_total", "signal" => "traces")
        .absolute(s.spool_uncertain_total);
    metrics::counter!("pulsus_ingest_rejected_total", "signal" => "traces")
        .absolute(s.rejected_total);

    record_backfill_metrics("trace_attrs_idx", &s.attrs_backfill);
}

/// Emits the `pulsus_ingest_backfill_*` registration-backfill series for one
/// backlog (label `backlog`). Counters `.absolute()`, `pending` a gauge.
fn record_backfill_metrics(backlog: &'static str, b: &BackfillMetricsSnapshot) {
    metrics::counter!("pulsus_ingest_backfill_enqueued_total", "backlog" => backlog)
        .absolute(b.enqueued_total);
    metrics::counter!("pulsus_ingest_backfill_dropped_total", "backlog" => backlog)
        .absolute(b.dropped_total);
    metrics::counter!("pulsus_ingest_backfill_retries_total", "backlog" => backlog)
        .absolute(b.retries_total);
    metrics::counter!("pulsus_ingest_backfill_healed_total", "backlog" => backlog)
        .absolute(b.healed_total);
    metrics::counter!("pulsus_ingest_backfill_abandoned_total", "backlog" => backlog)
        .absolute(b.abandoned_total);
    metrics::gauge!("pulsus_ingest_backfill_pending", "backlog" => backlog).set(b.pending as f64);
}

/// `GET /config`: effective configuration, secrets redacted
/// (`Config::to_redacted_yaml`).
async fn config_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.config.to_redacted_yaml() {
        Ok(yaml) => (StatusCode::OK, yaml).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "failed to render redacted config");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to render configuration",
            )
                .into_response()
        }
    }
}

/// `GET /buildinfo`: `{"version","revision","builtAt","rustc"}`.
async fn buildinfo_handler(State(state): State<AppState>) -> impl IntoResponse {
    axum::Json(state.build.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use crate::app::BuildInfo;
    use crate::ingest::{MetricWriterSink, TraceWriterSink, WriterSink};
    use pulsus_config::Config;

    fn test_state() -> AppState {
        AppState {
            pool: Arc::new(RwLock::new(None)),
            config: Arc::new(Config::default()),
            metrics: metrics_exporter_prometheus::PrometheusBuilder::new()
                .build_recorder()
                .handle(),
            build: BuildInfo::from_build_env(),
            writer: Arc::new(WriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            metric_writer: Arc::new(MetricWriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            trace_writer: Arc::new(TraceWriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            label_cache: Arc::new(std::sync::OnceLock::new()),
            eval_gate: Arc::new(pulsus_read::EvalGate::new(
                pulsus_config::Config::default()
                    .reader
                    .query_eval_concurrency,
            )),
            started_at: std::time::SystemTime::now(),
            tail: std::sync::Arc::new(crate::app::TailRuntime::for_tests()),
        }
    }

    #[tokio::test]
    async fn ready_is_503_when_the_pool_is_not_yet_established() {
        let res = ready(State(test_state())).await;
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// `label_cache_ready`'s only pure-testable branch (constructing a
    /// `LabelCache` at all needs a live `ChClient`): an unset slot must
    /// never gate readiness — covers both "writer/init mode, no reader
    /// subsystem" (permanently `None`) and "reader mode, reconnect loop
    /// hasn't constructed the cache yet" (transiently `None`).
    #[test]
    fn label_cache_ready_is_a_pass_through_when_the_slot_is_unset() {
        assert_eq!(label_cache_ready(None).status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn ready_from_ping_is_200_on_a_successful_ping() {
        let res = ready_from_ping(async { Ok(()) }).await;
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn ready_from_ping_is_503_on_a_failed_ping() {
        let res = ready_from_ping(async { Err(ChError::Connect("refused".to_string())) }).await;
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn ready_from_ping_is_503_not_408_or_504_when_the_ping_hangs() {
        // A ping future that never resolves stands in for a hung ClickHouse
        // connection (amendment F2's load-bearing case): the 2s internal
        // deadline must still map to 503, never a generic-timeout-style
        // 408/504.
        let hang = std::future::pending::<Result<(), ChError>>();
        let res = ready_from_ping(hang).await;
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_ne!(res.status(), StatusCode::REQUEST_TIMEOUT);
        assert_ne!(res.status(), StatusCode::GATEWAY_TIMEOUT);
    }

    #[tokio::test]
    async fn metrics_handler_renders_the_prometheus_handle() {
        let res = metrics_handler(State(test_state())).await.into_response();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn config_handler_redacts_the_password() {
        let mut cfg = Config::default();
        cfg.clickhouse.auth.password = pulsus_config::Secret::new("s3cret");
        let state = AppState {
            pool: Arc::new(RwLock::new(None)),
            config: Arc::new(cfg),
            metrics: metrics_exporter_prometheus::PrometheusBuilder::new()
                .build_recorder()
                .handle(),
            build: BuildInfo::from_build_env(),
            writer: Arc::new(WriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            metric_writer: Arc::new(MetricWriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            trace_writer: Arc::new(TraceWriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            label_cache: Arc::new(std::sync::OnceLock::new()),
            eval_gate: Arc::new(pulsus_read::EvalGate::new(
                pulsus_config::Config::default()
                    .reader
                    .query_eval_concurrency,
            )),
            started_at: std::time::SystemTime::now(),
            tail: std::sync::Arc::new(crate::app::TailRuntime::for_tests()),
        };
        let res = config_handler(State(state)).await.into_response();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(!text.contains("s3cret"));
    }

    /// Issue #101 (plan v1/v2): pins `record_eval_gate_snapshot`'s exact
    /// exposition — a fresh recorder per value (`Counter::absolute` is
    /// monotonic-max within one recorder, so reusing one across values would
    /// silently keep the max instead of proving each round-trips). `1`,
    /// `999_999_999` (mutation-sensitive: the old truncating `/
    /// 1_000_000_000` code rendered `0`), `1_500_000_000` (old code rendered
    /// `1`), and `u64::MAX` (the saturating-accumulator boundary, now
    /// genuinely reachable per `eval_gate.rs`'s `add_wait_nanos`) all
    /// round-trip exactly with no division. Also pins the rename: the old
    /// `pulsus_query_eval_wait_seconds_total` name must never appear.
    #[test]
    fn eval_gate_snapshot_exports_exact_wait_nanoseconds_with_no_division() {
        for wait_nanos_total in [1u64, 999_999_999, 1_500_000_000, u64::MAX] {
            let snap = pulsus_read::EvalGateSnapshot {
                limit: 256,
                available: 200,
                in_flight: 56,
                waiting: 3,
                contended_total: 7,
                wait_nanos_total,
            };
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            metrics::with_local_recorder(&recorder, || record_eval_gate_snapshot(&snap));
            let rendered = handle.render();

            let exported = rendered
                .lines()
                .find_map(|line| line.strip_prefix("pulsus_query_eval_wait_nanoseconds_total "))
                .unwrap_or_else(|| {
                    panic!(
                        "missing pulsus_query_eval_wait_nanoseconds_total sample in:\n{rendered}"
                    )
                })
                .trim()
                .parse::<u64>()
                .expect("exported sample must parse as a u64");
            assert_eq!(
                exported, wait_nanos_total,
                "exact u64 round-trip for wait_nanos_total={wait_nanos_total}"
            );
            assert!(
                !rendered.contains("pulsus_query_eval_wait_seconds_total"),
                "the stale metric name must never be emitted"
            );
        }
    }

    // --- Issue #214: pulsus_ingest_* bridge ---

    /// Finds the exact-value of a fully-qualified `name{labels}` sample in a
    /// Prometheus exposition body and asserts it. Parses as `f64` so it works
    /// for both counters (rendered as integers) and gauges.
    fn assert_sample(rendered: &str, key: &str, expected: f64) {
        let prefix = format!("{key} ");
        let value = rendered
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .unwrap_or_else(|| panic!("missing sample {key:?} in:\n{rendered}"))
            .trim()
            .parse::<f64>()
            .unwrap_or_else(|_| panic!("sample {key:?} did not parse as a number"));
        assert_eq!(value, expected, "sample {key:?}");
    }

    /// Asserts the `# TYPE <name> <kind>` header line is present.
    fn assert_type(rendered: &str, name: &str, kind: &str) {
        let line = format!("# TYPE {name} {kind}");
        assert!(
            rendered.lines().any(|l| l == line),
            "missing TYPE line {line:?} in:\n{rendered}"
        );
    }

    fn render_local(f: impl FnOnce()) -> String {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, f);
        handle.render()
    }

    /// A `TableMetricsSnapshot` with every *exported* field set to a distinct
    /// nonzero value derived from `base` (offsets 1..=7). Distinct-per-table
    /// bases make a mis-wired `table` label (a value landing on the wrong
    /// series) fail. `flush_latency_count` is intentionally left zero: it is
    /// not exported, so it carries no series to assert.
    fn table_snap(base: u64) -> TableMetricsSnapshot {
        TableMetricsSnapshot {
            rows_total: base + 1,
            bytes_total: base + 2,
            flushes_total: base + 3,
            flush_latency_sum_ns: base + 4,
            flush_latency_count: 0,
            retries_total: base + 5,
            inflight: base + 6,
            spool_write_failures_total: base + 7,
        }
    }

    /// Asserts all seven `record_table_metrics` series for `table` carry their
    /// exact `table_snap(base)` values.
    fn assert_table_series(r: &str, table: &str, base: u64) {
        for (name, off) in [
            ("pulsus_ingest_rows_total", 1u64),
            ("pulsus_ingest_bytes_total", 2),
            ("pulsus_ingest_flushes_total", 3),
            ("pulsus_ingest_flush_latency_nanoseconds_total", 4),
            ("pulsus_ingest_retries_total", 5),
            ("pulsus_ingest_inflight", 6),
            ("pulsus_ingest_spool_write_failures_total", 7),
        ] {
            assert_sample(
                r,
                &format!(r#"{name}{{table="{table}"}}"#),
                (base + off) as f64,
            );
        }
    }

    /// Asserts the `# TYPE` header for every per-table metric name (one line
    /// per name regardless of the number of `table` label values).
    fn assert_table_types(r: &str) {
        assert_type(r, "pulsus_ingest_rows_total", "counter");
        assert_type(r, "pulsus_ingest_bytes_total", "counter");
        assert_type(r, "pulsus_ingest_flushes_total", "counter");
        assert_type(
            r,
            "pulsus_ingest_flush_latency_nanoseconds_total",
            "counter",
        );
        assert_type(r, "pulsus_ingest_retries_total", "counter");
        assert_type(r, "pulsus_ingest_inflight", "gauge");
        assert_type(r, "pulsus_ingest_spool_write_failures_total", "counter");
    }

    /// A `BackfillMetricsSnapshot` with every field distinct-nonzero from
    /// `base` (offsets 1..=6).
    fn backfill_snap(base: u64) -> BackfillMetricsSnapshot {
        BackfillMetricsSnapshot {
            enqueued_total: base + 1,
            dropped_total: base + 2,
            retries_total: base + 3,
            healed_total: base + 4,
            abandoned_total: base + 5,
            pending: base + 6,
        }
    }

    /// Asserts all six `record_backfill_metrics` series for `backlog`.
    fn assert_backfill_series(r: &str, backlog: &str, base: u64) {
        for (name, off) in [
            ("pulsus_ingest_backfill_enqueued_total", 1u64),
            ("pulsus_ingest_backfill_dropped_total", 2),
            ("pulsus_ingest_backfill_retries_total", 3),
            ("pulsus_ingest_backfill_healed_total", 4),
            ("pulsus_ingest_backfill_abandoned_total", 5),
            ("pulsus_ingest_backfill_pending", 6),
        ] {
            assert_sample(
                r,
                &format!(r#"{name}{{backlog="{backlog}"}}"#),
                (base + off) as f64,
            );
        }
    }

    /// Asserts the `# TYPE` header for every backfill metric name.
    fn assert_backfill_types(r: &str) {
        assert_type(r, "pulsus_ingest_backfill_enqueued_total", "counter");
        assert_type(r, "pulsus_ingest_backfill_dropped_total", "counter");
        assert_type(r, "pulsus_ingest_backfill_retries_total", "counter");
        assert_type(r, "pulsus_ingest_backfill_healed_total", "counter");
        assert_type(r, "pulsus_ingest_backfill_abandoned_total", "counter");
        assert_type(r, "pulsus_ingest_backfill_pending", "gauge");
    }

    /// Asserts the `# TYPE` header for the five per-`signal` series shared by
    /// all three writers.
    fn assert_signal_types(r: &str) {
        assert_type(r, "pulsus_ingest_queue_bytes", "gauge");
        assert_type(r, "pulsus_ingest_backpressure_total", "counter");
        assert_type(r, "pulsus_ingest_spool_poison_total", "counter");
        assert_type(r, "pulsus_ingest_spool_uncertain_total", "counter");
        assert_type(r, "pulsus_ingest_rejected_total", "counter");
    }

    /// AC-2 (logs): `record_log_ingest_snapshot` emits every per-table,
    /// per-signal, registration-cache, and backfill `pulsus_ingest_*` series.
    /// Exhaustive: EVERY emitted series is asserted for both its exact seeded
    /// value AND its `# TYPE` header. The struct literal is fully spelled out
    /// (no `..Default::default()`) so a newly added exported field breaks this
    /// test's compile until it is seeded and asserted. Distinct-nonzero seeds
    /// (per-table bases 10/20/30, backfill base 40, per-signal 51..=59) catch a
    /// mis-wired name, `table`/`signal` label, or counter/gauge type.
    #[test]
    fn log_ingest_snapshot_exports_named_series() {
        let snap = WriterMetricsSnapshot {
            samples: table_snap(10),
            streams: table_snap(20),
            patterns: table_snap(30),
            patterns_dropped_total: 51,
            queue_bytes: 1000,
            backpressure_total: 52,
            spool_poison_total: 53,
            spool_uncertain_total: 54,
            stream_registrations_total: 55,
            lru_hits_total: 56,
            lru_misses_total: 57,
            collisions_total: 58,
            rejected_total: 59,
            backfill_enqueued_total: 41,
            backfill_dropped_total: 42,
            backfill_retries_total: 43,
            backfill_healed_total: 44,
            backfill_abandoned_total: 45,
            backfill_pending: 46,
        };
        let r = render_local(|| record_log_ingest_snapshot(&snap));

        // # TYPE header for every emitted metric name.
        assert_table_types(&r);
        assert_signal_types(&r);
        assert_backfill_types(&r);
        assert_type(&r, "pulsus_ingest_registrations_total", "counter");
        assert_type(&r, "pulsus_ingest_registration_cache_hits_total", "counter");
        assert_type(
            &r,
            "pulsus_ingest_registration_cache_misses_total",
            "counter",
        );
        assert_type(&r, "pulsus_ingest_collisions_total", "counter");
        assert_type(&r, "pulsus_ingest_patterns_dropped_total", "counter");

        // Per-table values (7 series each).
        assert_table_series(&r, "log_samples", 10);
        assert_table_series(&r, "log_streams", 20);
        assert_table_series(&r, "log_patterns", 30);

        // Per-signal values.
        assert_sample(&r, r#"pulsus_ingest_queue_bytes{signal="logs"}"#, 1000.0);
        assert_sample(
            &r,
            r#"pulsus_ingest_backpressure_total{signal="logs"}"#,
            52.0,
        );
        assert_sample(
            &r,
            r#"pulsus_ingest_spool_poison_total{signal="logs"}"#,
            53.0,
        );
        assert_sample(
            &r,
            r#"pulsus_ingest_spool_uncertain_total{signal="logs"}"#,
            54.0,
        );
        assert_sample(&r, r#"pulsus_ingest_rejected_total{signal="logs"}"#, 59.0);

        // Registration-cache values.
        assert_sample(
            &r,
            r#"pulsus_ingest_registrations_total{signal="logs"}"#,
            55.0,
        );
        assert_sample(
            &r,
            r#"pulsus_ingest_registration_cache_hits_total{signal="logs"}"#,
            56.0,
        );
        assert_sample(
            &r,
            r#"pulsus_ingest_registration_cache_misses_total{signal="logs"}"#,
            57.0,
        );
        assert_sample(&r, r#"pulsus_ingest_collisions_total{signal="logs"}"#, 58.0);
        assert_sample(
            &r,
            r#"pulsus_ingest_patterns_dropped_total{signal="logs"}"#,
            51.0,
        );

        // Backfill values (6 series).
        assert_backfill_series(&r, "log_streams", 40);

        // Logs have no per-metrics metadata-upsert series.
        assert!(!r.contains("pulsus_ingest_metadata_upserts_total"));
    }

    /// AC-2 (metrics): the metric-writer snapshot emits its four tables, the
    /// `signal="metrics"` series (incl. `metadata_upserts`), and both series/
    /// metadata backfills. Exhaustive: EVERY emitted series is asserted for its
    /// exact seeded value AND its `# TYPE`. Fully-spelled struct literal.
    /// Distinct-nonzero seeds (per-table bases 100/110/120/130, backfill bases
    /// 140/150, per-signal 61..=69) catch a mis-wired name/label/type.
    #[test]
    fn metric_ingest_snapshot_exports_named_series() {
        let snap = MetricWriterMetricsSnapshot {
            samples: table_snap(100),
            series: table_snap(110),
            metadata: table_snap(120),
            hist_samples: table_snap(130),
            queue_bytes: 2000,
            backpressure_total: 61,
            spool_poison_total: 62,
            spool_uncertain_total: 63,
            series_registrations_total: 64,
            series_lru_hits_total: 65,
            series_lru_misses_total: 66,
            metadata_upserts_total: 67,
            collisions_total: 68,
            rejected_total: 69,
            series_backfill: backfill_snap(140),
            metadata_backfill: backfill_snap(150),
        };
        let r = render_local(|| record_metric_ingest_snapshot(&snap));

        // # TYPE header for every emitted metric name.
        assert_table_types(&r);
        assert_signal_types(&r);
        assert_backfill_types(&r);
        assert_type(&r, "pulsus_ingest_registrations_total", "counter");
        assert_type(&r, "pulsus_ingest_registration_cache_hits_total", "counter");
        assert_type(
            &r,
            "pulsus_ingest_registration_cache_misses_total",
            "counter",
        );
        assert_type(&r, "pulsus_ingest_collisions_total", "counter");
        assert_type(&r, "pulsus_ingest_metadata_upserts_total", "counter");

        // Per-table values (4 tables × 7 series).
        assert_table_series(&r, "metric_samples", 100);
        assert_table_series(&r, "metric_series", 110);
        assert_table_series(&r, "metric_metadata", 120);
        assert_table_series(&r, "metric_hist_samples", 130);

        // Per-signal values.
        assert_sample(&r, r#"pulsus_ingest_queue_bytes{signal="metrics"}"#, 2000.0);
        assert_sample(
            &r,
            r#"pulsus_ingest_backpressure_total{signal="metrics"}"#,
            61.0,
        );
        assert_sample(
            &r,
            r#"pulsus_ingest_spool_poison_total{signal="metrics"}"#,
            62.0,
        );
        assert_sample(
            &r,
            r#"pulsus_ingest_spool_uncertain_total{signal="metrics"}"#,
            63.0,
        );
        assert_sample(
            &r,
            r#"pulsus_ingest_rejected_total{signal="metrics"}"#,
            69.0,
        );

        // Registration-cache + metadata-upsert values.
        assert_sample(
            &r,
            r#"pulsus_ingest_registrations_total{signal="metrics"}"#,
            64.0,
        );
        assert_sample(
            &r,
            r#"pulsus_ingest_registration_cache_hits_total{signal="metrics"}"#,
            65.0,
        );
        assert_sample(
            &r,
            r#"pulsus_ingest_registration_cache_misses_total{signal="metrics"}"#,
            66.0,
        );
        assert_sample(
            &r,
            r#"pulsus_ingest_collisions_total{signal="metrics"}"#,
            68.0,
        );
        assert_sample(
            &r,
            r#"pulsus_ingest_metadata_upserts_total{signal="metrics"}"#,
            67.0,
        );

        // Backfill values (2 backlogs × 6 series).
        assert_backfill_series(&r, "metric_series", 140);
        assert_backfill_series(&r, "metric_metadata", 150);

        // Metrics writer has no pattern-drop series.
        assert!(!r.contains("pulsus_ingest_patterns_dropped_total"));
    }

    /// AC-2 (traces): the trace-writer snapshot emits its two tables, the
    /// `signal="traces"` series, and the attrs backfill — and NO
    /// registration-cache/collision/metadata/pattern series (traces track
    /// none). Exhaustive: EVERY emitted series is asserted for its exact
    /// seeded value AND its `# TYPE`. Fully-spelled struct literal.
    /// Distinct-nonzero seeds (per-table bases 200/210, backfill base 220,
    /// per-signal 71..=74) catch a mis-wired name/label/type.
    #[test]
    fn trace_ingest_snapshot_exports_named_series() {
        let snap = TraceWriterMetricsSnapshot {
            spans: table_snap(200),
            attrs: table_snap(210),
            queue_bytes: 500,
            backpressure_total: 71,
            spool_poison_total: 72,
            spool_uncertain_total: 73,
            rejected_total: 74,
            attrs_backfill: backfill_snap(220),
        };
        let r = render_local(|| record_trace_ingest_snapshot(&snap));

        // # TYPE header for every emitted metric name.
        assert_table_types(&r);
        assert_signal_types(&r);
        assert_backfill_types(&r);

        // Per-table values (2 tables × 7 series).
        assert_table_series(&r, "trace_spans", 200);
        assert_table_series(&r, "trace_attrs_idx", 210);

        // Per-signal values.
        assert_sample(&r, r#"pulsus_ingest_queue_bytes{signal="traces"}"#, 500.0);
        assert_sample(
            &r,
            r#"pulsus_ingest_backpressure_total{signal="traces"}"#,
            71.0,
        );
        assert_sample(
            &r,
            r#"pulsus_ingest_spool_poison_total{signal="traces"}"#,
            72.0,
        );
        assert_sample(
            &r,
            r#"pulsus_ingest_spool_uncertain_total{signal="traces"}"#,
            73.0,
        );
        assert_sample(&r, r#"pulsus_ingest_rejected_total{signal="traces"}"#, 74.0);

        // Backfill values (6 series).
        assert_backfill_series(&r, "trace_attrs_idx", 220);

        // Traces track no registration-cache/collision/metadata/pattern series.
        assert!(!r.contains("pulsus_ingest_registrations_total"));
        assert!(!r.contains("pulsus_ingest_registration_cache_hits_total"));
        assert!(!r.contains("pulsus_ingest_registration_cache_misses_total"));
        assert!(!r.contains("pulsus_ingest_collisions_total"));
        assert!(!r.contains("pulsus_ingest_metadata_upserts_total"));
        assert!(!r.contains("pulsus_ingest_patterns_dropped_total"));
    }

    /// AC-3 / reader-only guard: `metrics_handler` with empty writer slots
    /// (a reader-only or not-yet-warm process) renders a body with NO line
    /// starting `pulsus_ingest_` — the `Option::None` short-circuit holds
    /// through the real handler.
    #[tokio::test]
    async fn metrics_handler_omits_ingest_series_when_no_writer() {
        let res = metrics_handler(State(test_state())).await.into_response();
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            !text.lines().any(|l| l.starts_with("pulsus_ingest_")),
            "reader-only /metrics must expose no ingest series:\n{text}"
        );
    }

    /// AC-4: each sink's `metrics()` is `None` on an empty slot.
    #[test]
    fn sink_metrics_are_none_on_an_empty_slot() {
        assert!(
            WriterSink::new(Arc::new(std::sync::OnceLock::new()))
                .metrics()
                .is_none()
        );
        assert!(
            MetricWriterSink::new(Arc::new(std::sync::OnceLock::new()))
                .metrics()
                .is_none()
        );
        assert!(
            TraceWriterSink::new(Arc::new(std::sync::OnceLock::new()))
                .metrics()
                .is_none()
        );
    }

    /// AC-7 (the round-1 fix): drives the REAL `metrics_handler` end-to-end
    /// with all three writer slots populated (via the `#[cfg(test)]`
    /// snapshot seam) and asserts the rendered `/metrics` body carries the
    /// seeded `pulsus_ingest_*` values + labels for every sink. `metrics_handler`
    /// has no yield point between its `counter!`/`gauge!` emissions and
    /// `state.metrics.render()`, so `futures::executor::block_on` polls it
    /// inline and the `with_local_recorder` thread-local stays installed for
    /// both — and `state.metrics` is the SAME recorder's handle, so the
    /// emitted samples land where `render()` reads.
    #[test]
    fn metrics_handler_exports_ingest_series_for_populated_writer_slots() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        let log = WriterMetricsSnapshot {
            samples: TableMetricsSnapshot {
                rows_total: 7,
                bytes_total: 512,
                ..Default::default()
            },
            streams: TableMetricsSnapshot {
                rows_total: 3,
                ..Default::default()
            },
            queue_bytes: 1024,
            backpressure_total: 2,
            ..Default::default()
        };
        let metric = MetricWriterMetricsSnapshot {
            samples: TableMetricsSnapshot {
                rows_total: 71,
                ..Default::default()
            },
            queue_bytes: 2048,
            ..Default::default()
        };
        let trace = TraceWriterMetricsSnapshot {
            spans: TableMetricsSnapshot {
                rows_total: 91,
                ..Default::default()
            },
            queue_bytes: 256,
            ..Default::default()
        };

        let mut state = test_state();
        state.metrics = handle.clone();
        state.writer = Arc::new(WriterSink::with_metrics_snapshot(log));
        state.metric_writer = Arc::new(MetricWriterSink::with_metrics_snapshot(metric));
        state.trace_writer = Arc::new(TraceWriterSink::with_metrics_snapshot(trace));

        let body = metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let res = metrics_handler(State(state)).await.into_response();
                axum::body::to_bytes(res.into_body(), usize::MAX)
                    .await
                    .unwrap()
            })
        });
        let text = String::from_utf8(body.to_vec()).unwrap();

        // All three sinks bridged through the real handler with their seeds.
        assert_sample(
            &text,
            r#"pulsus_ingest_rows_total{table="log_samples"}"#,
            7.0,
        );
        assert_sample(
            &text,
            r#"pulsus_ingest_rows_total{table="log_streams"}"#,
            3.0,
        );
        assert_sample(&text, r#"pulsus_ingest_queue_bytes{signal="logs"}"#, 1024.0);
        assert_sample(
            &text,
            r#"pulsus_ingest_backpressure_total{signal="logs"}"#,
            2.0,
        );
        assert_sample(
            &text,
            r#"pulsus_ingest_rows_total{table="metric_samples"}"#,
            71.0,
        );
        assert_sample(
            &text,
            r#"pulsus_ingest_queue_bytes{signal="metrics"}"#,
            2048.0,
        );
        assert_sample(
            &text,
            r#"pulsus_ingest_rows_total{table="trace_spans"}"#,
            91.0,
        );
        assert_sample(
            &text,
            r#"pulsus_ingest_queue_bytes{signal="traces"}"#,
            256.0,
        );
    }

    #[tokio::test]
    async fn buildinfo_handler_has_four_non_empty_fields() {
        let res = buildinfo_handler(State(test_state())).await.into_response();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        for field in ["version", "revision", "builtAt", "rustc"] {
            assert!(
                json.get(field)
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| !s.is_empty()),
                "missing or empty field {field:?} in {json}"
            );
        }
    }
}
