//! The admin UI (issue #746 part B): status, database and configuration
//! pages under `/admin/`, and their JSON API under `/admin/api/v1/`. Each
//! page and its endpoint are built from one view value (`views`). Mounted
//! only with the admin pair; `app::build_router` merges it outside the CORS
//! layer.

use axum::Router;
use axum::extract::Request;
use axum::http::{HeaderValue, header};
use axum::middleware::Next;
use axum::response::Response;
use axum::routing::get;
use pulsus_config::Config;

use crate::app::AppState;

mod api;
pub(crate) mod html;
mod pages;
pub(crate) mod views;

/// The admin routes, or `None` unless both admin fields are set. The pages
/// refuse with the plain `401`, the API with the JSON one; every response
/// carries the two headers `headers` sets.
pub(crate) fn router(config: &Config) -> Option<Router<AppState>> {
    let page_auth = crate::middleware::admin_auth_layer(config, false)?;
    let api_auth = crate::middleware::admin_auth_layer(config, true)?;
    let pages = Router::new()
        .route("/admin/", get(pages::status))
        .route("/admin/database", get(pages::database))
        .route("/admin/config", get(pages::config))
        .route("/admin/admin.css", get(pages::css))
        .layer(page_auth);
    let api = Router::new()
        .route("/admin/api/v1/status", get(api::status))
        .route("/admin/api/v1/database", get(api::database))
        .route("/admin/api/v1/config", get(api::config))
        .layer(api_auth);
    Some(
        pages
            .merge(api)
            .layer(axum::middleware::from_fn(headers))
            .layer(axum::middleware::from_fn(crate::request_id::stamp))
            .layer(crate::middleware::trace_layer()),
    )
}

/// `Content-Security-Policy: default-src 'self'` and `Cache-Control:
/// no-store` on every admin response, a `401` included.
async fn headers(req: Request, next: Next) -> Response {
    let mut res = next.run(req).await;
    let headers = res.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'self'"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    res
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, OnceLock};
    use std::time::{Duration, SystemTime};

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use pulsus_config::{Config, Mode, Secret};
    use pulsus_read::metrics::rollup::{Level, LevelSlot, Levels};
    use serde_json::{Value, json};
    use tokio::sync::RwLock;
    use tower::ServiceExt;

    use super::html::html_text;
    use super::views::{DatabaseView, StatusView};
    use crate::app::{AppState, BuildInfo, TailRuntime, build_router};
    use crate::ingest::{MetricWriterSink, TraceWriterSink, WriterSink};

    const NOT_ESTABLISHED: &str = "clickhouse pool not yet established";

    /// `app.rs`'s `test_state()`, with `cfg` as the configuration, the
    /// admin pair `a`/`s3cret` added, and retention unset.
    fn state(mut cfg: Config) -> AppState {
        cfg.admin_user = Some("a".to_string());
        cfg.admin_password = Some(Secret::new("s3cret"));
        AppState {
            pool: Arc::new(RwLock::new(None)),
            retention_days: Arc::new(OnceLock::new()),
            config: Arc::new(cfg),
            metrics: metrics_exporter_prometheus::PrometheusBuilder::new()
                .build_recorder()
                .handle(),
            build: BuildInfo::from_build_env(),
            writer: Arc::new(WriterSink::new(Arc::new(OnceLock::new()))),
            metric_writer: Arc::new(MetricWriterSink::new(Arc::new(OnceLock::new()))),
            trace_writer: Arc::new(TraceWriterSink::new(Arc::new(OnceLock::new()))),
            label_cache: Arc::new(OnceLock::new()),
            rollup_levels: Arc::new(LevelSlot::new()),
            graph_target: pulsus_read::GraphTargetCache::default(),
            eval_gate: Arc::new(pulsus_read::EvalGate::new(
                Config::default().reader.query_eval_concurrency,
            )),
            started_at: SystemTime::now(),
            tail: Arc::new(TailRuntime::for_tests()),
        }
    }

    fn router(state: &AppState) -> axum::Router {
        build_router(state.clone(), &state.config.clone()).expect("router builds")
    }

    fn admin_header() -> String {
        format!("Basic {}", crate::middleware::base64_encode(b"a:s3cret"))
    }

    /// `(status, content type, body)` of `GET path`.
    async fn get(router: &axum::Router, path: &str, auth: bool) -> (StatusCode, String, String) {
        let mut builder = Request::builder().uri(path);
        if auth {
            builder = builder.header(axum::http::header::AUTHORIZATION, admin_header());
        }
        let res = router
            .clone()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = res.status();
        let content_type = res
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            content_type,
            String::from_utf8(bytes.to_vec()).unwrap(),
        )
    }

    /// The JSON body of an admin API route, which must answer `200`.
    async fn get_json(router: &axum::Router, path: &str) -> Value {
        let (status, _, body) = get(router, path, true).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("{path}: {e}: {body}"))
    }

    /// The page of an admin HTML route, which must answer `200`.
    async fn get_page(router: &axum::Router, path: &str) -> String {
        let (status, _, body) = get(router, path, true).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        body
    }

    /// The text of the `<dd>` the page shows under `label`, still escaped.
    fn row<'p>(page: &'p str, label: &str) -> &'p str {
        let open = format!("<dt>{label}</dt><dd>");
        let start = page
            .find(&open)
            .unwrap_or_else(|| panic!("no row {label:?} in {page}"))
            + open.len();
        let len = page[start..].find("</dd>").expect("row closes");
        &page[start..start + len]
    }

    /// The page's text for a JSON value: the string itself, `—` for
    /// `null`, the JSON text otherwise.
    fn value_text(v: &Value) -> String {
        match v {
            Value::String(s) => s.clone(),
            Value::Null => "—".to_string(),
            other => serde_json::to_string(other).unwrap(),
        }
    }

    fn b6_levels(shards: u32, counter_buckets: u32) -> Levels {
        Levels {
            levels: vec![
                Level {
                    res_ms: 60_000,
                    table: "metric_samples_60s".to_string(),
                    from_ms: 0,
                },
                Level {
                    res_ms: 720_000,
                    table: "metric_samples_720s".to_string(),
                    from_ms: 86_400_000,
                },
            ],
            shards,
            counter_buckets,
        }
    }

    fn b11_levels_json() -> Value {
        json!([
            {"table": "metric_samples_60s", "resMs": 60000, "fromMs": 0},
            {"table": "metric_samples_720s", "resMs": 720000, "fromMs": 86400000}
        ])
    }

    /// A ClickHouse stand-in on loopback: answers the first connection's
    /// request `200`, then either drops (`hold == false`) or holds open
    /// without answering (`hold == true`) every later connection.
    async fn stub_clickhouse(hold: bool) -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            // Read the whole request: the head, then its body by length or
            // to the last chunk.
            loop {
                let n = first.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&buf).to_ascii_lowercase();
                let Some(head_end) = text.find("\r\n\r\n") else {
                    continue;
                };
                let body = buf.len() - head_end - 4;
                let length = text[..head_end]
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse::<usize>().ok());
                let done = match length {
                    Some(len) => body >= len,
                    None if text[..head_end].contains("transfer-encoding: chunked") => {
                        text.ends_with("0\r\n\r\n")
                    }
                    None => true,
                };
                if done {
                    break;
                }
            }
            first
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            first.shutdown().await.ok();
            drop(first);
            let mut held = Vec::new();
            loop {
                let Ok((later, _)) = listener.accept().await else {
                    return;
                };
                if hold {
                    held.push(later);
                } else {
                    drop(later);
                }
            }
        });
        port
    }

    /// The not-ready text `/ready`, the status page and the status API each
    /// give, in that order; the latter two must answer `200` and say
    /// `ready` false.
    async fn three_reasons(router: &axum::Router) -> (String, String, String) {
        let (status, _, ready) = get(router, "/ready", false).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "/ready: {ready}");
        let page = get_page(router, "/admin/").await;
        assert_eq!(row(&page, "Ready"), "false");
        let page_reason = row(&page, "Not ready because").to_string();
        let api = get_json(router, "/admin/api/v1/status").await;
        assert_eq!(api["ready"], json!(false));
        let api_reason = api["notReadyReason"]
            .as_str()
            .unwrap_or_else(|| panic!("notReadyReason is a string: {api}"))
            .to_string();
        (ready, page_reason, api_reason)
    }

    /// B5 (i): no pool.
    #[tokio::test]
    async fn status_reports_the_missing_pool_as_ready_does() {
        let st = state(Config::default());
        let (ready, page, api) = three_reasons(&router(&st)).await;
        assert_eq!(ready, NOT_ESTABLISHED);
        assert_eq!(page, html_text(NOT_ESTABLISHED));
        assert_eq!(api, NOT_ESTABLISHED);
    }

    async fn state_with_stub_pool(hold: bool) -> AppState {
        let port = stub_clickhouse(hold).await;
        let mut cfg = Config::default();
        cfg.clickhouse.server = "127.0.0.1".to_string();
        cfg.clickhouse.http_port = port;
        let pool = pulsus_clickhouse::ChPool::connect(crate::chconfig::conn_config_from(&cfg))
            .await
            .expect("the stub answers the connect ping");
        let st = state(cfg);
        *st.pool.write().await = Some(Arc::new(pool));
        st
    }

    /// B5 (ii): the ping fails.
    #[tokio::test]
    async fn status_reports_a_failed_ping_as_ready_does() {
        let st = state_with_stub_pool(false).await;
        let (ready, page, api) = three_reasons(&router(&st)).await;
        // Each request pings afresh; the three failures give one text.
        assert!(ready.starts_with("clickhouse ping failed: "), "{ready}");
        assert_eq!(api, ready);
        assert_eq!(page, html_text(&ready));
    }

    /// B5 (iii): the ping hangs.
    #[tokio::test]
    async fn status_reports_a_hung_ping_as_ready_does() {
        let st = state_with_stub_pool(true).await;
        let (ready, page, api) = three_reasons(&router(&st)).await;
        assert_eq!(ready, "clickhouse ping exceeded 2s");
        assert_eq!(page, "clickhouse ping exceeded 2s");
        assert_eq!(api, "clickhouse ping exceeded 2s");
    }

    /// B6: the database page shows the levels and the retention.
    #[tokio::test]
    async fn database_page_shows_levels_and_retention() {
        let st = state(Config::default());
        st.rollup_levels.set(b6_levels(1, 5));
        st.retention_days.set(30).unwrap();
        let page = get_page(&router(&st), "/admin/database").await;
        for needle in [
            "metric_samples_60s",
            "metric_samples_720s",
            "86400000",
            "<dt>Retention (days)</dt><dd>30</dd>",
        ] {
            assert!(page.contains(needle), "{needle:?} not in {page}");
        }
    }

    fn both_pairs() -> Config {
        Config {
            auth_user: Some("alice".to_string()),
            auth_password: Some(Secret::new("hunter2")),
            ..Config::default()
        }
    }

    /// B7: the configuration page redacts both passwords.
    #[tokio::test]
    async fn config_page_redacts_both_passwords() {
        let mut cfg = both_pairs();
        cfg.clickhouse.database = "pulsus_b7".to_string();
        let st = state(cfg);
        let page = get_page(&router(&st), "/admin/config").await;
        assert!(!page.contains("hunter2"), "{page}");
        assert!(!page.contains("s3cret"), "{page}");
        // The configuration is rendered, not an empty page.
        assert!(page.contains("database: pulsus_b7"), "{page}");
        assert!(page.contains("admin_user: a"), "{page}");
        assert!(page.contains("***"), "{page}");
    }

    /// B12: the configuration API redacts both passwords; an API refusal is
    /// the JSON `401`, a page refusal the plain one.
    #[tokio::test]
    async fn config_api_redacts_and_refuses_in_json() {
        let st = state(both_pairs());
        let r = router(&st);
        let api = get_json(&r, "/admin/api/v1/config").await;
        assert_eq!(api["auth_password"], json!("***"));
        assert_eq!(api["admin_password"], json!("***"));
        assert_eq!(api["auth_user"], json!("alice"));
        let text = api.to_string();
        assert!(
            !text.contains("hunter2") && !text.contains("s3cret"),
            "{text}"
        );

        let (status, content_type, body) = get(&r, "/admin/api/v1/config", false).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(content_type, "application/json");
        assert_eq!(
            body,
            r#"{"errors":[{"field":null,"message":"unauthorized"}]}"#
        );
        let (status, _, body) = get(&r, "/admin/", false).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body, "unauthorized");
    }

    /// B11: every field of both views is bound to state, and the page shows
    /// exactly the JSON's fields.
    #[tokio::test]
    async fn status_and_database_views_bind_every_field_and_the_pages_show_each() {
        for retention in [None, Some(30u32)] {
            let mut cfg = Config {
                mode: Mode::Reader,
                cluster: Some("prod".to_string()),
                ..Config::default()
            };
            cfg.clickhouse.database = "pulsus_b11".to_string();
            let mut st = state(cfg);
            st.started_at = SystemTime::now() - Duration::from_secs(90);
            st.rollup_levels.set(b6_levels(3, 7));
            if let Some(days) = retention {
                st.retention_days.set(days).unwrap();
            }
            let r = router(&st);

            let status = get_json(&r, "/admin/api/v1/status").await;
            let build = BuildInfo::from_build_env();
            let uptime = status["uptimeSeconds"]
                .as_u64()
                .expect("uptime is a number");
            assert!((90..=150).contains(&uptime), "uptime {uptime}");
            assert_eq!(
                status,
                json!({
                    "version": build.version,
                    "revision": build.revision,
                    "builtAt": build.built_at,
                    "rustc": build.rustc,
                    "mode": "reader",
                    "uptimeSeconds": uptime,
                    "ready": false,
                    "notReadyReason": NOT_ESTABLISHED,
                })
            );

            let database = get_json(&r, "/admin/api/v1/database").await;
            assert_eq!(
                database,
                json!({
                    "database": "pulsus_b11",
                    "cluster": "prod",
                    "retentionDays": retention,
                    "levels": b11_levels_json(),
                    "levelsNote": null,
                    "shards": 3,
                    "counterBuckets": 7,
                    "discoveryFailures": 0,
                })
            );

            for (fields, json_value, page_path) in [
                (StatusView::FIELDS, &status, "/admin/"),
                (DatabaseView::FIELDS, &database, "/admin/database"),
            ] {
                let object = json_value.as_object().expect("an object");
                let mut keys: Vec<&str> = fields.iter().map(|(k, _)| *k).collect();
                keys.sort_unstable();
                let mut json_keys: Vec<&str> = object.keys().map(String::as_str).collect();
                json_keys.sort_unstable();
                assert_eq!(keys, json_keys, "{page_path}: FIELDS against the JSON");

                let page = get_page(&r, page_path).await;
                for (key, label) in fields {
                    // Uptime ticks between the two requests; its row is
                    // checked for presence and range instead.
                    if *key == "uptimeSeconds" {
                        let shown: u64 = row(&page, label).parse().expect("a number");
                        assert!((90..=150).contains(&shown), "page uptime {shown}");
                        continue;
                    }
                    let want = format!(
                        "<dt>{label}</dt><dd>{}</dd>",
                        html_text(&value_text(&object[*key]))
                    );
                    assert!(page.contains(&want), "{page_path}: {want:?} not in {page}");
                }
            }
        }
    }

    /// B13: a writer reads no levels, and says so.
    #[tokio::test]
    async fn writer_mode_reports_levels_as_not_read() {
        let st = state(Config {
            mode: Mode::Writer,
            ..Config::default()
        });
        st.rollup_levels.set(b6_levels(1, 5));
        let r = router(&st);
        let database = get_json(&r, "/admin/api/v1/database").await;
        assert_eq!(database["levels"], Value::Null);
        assert_eq!(database["levelsNote"], json!("not read in writer mode"));
        assert_eq!(database["shards"], Value::Null);
        assert_eq!(database["counterBuckets"], Value::Null);
        let page = get_page(&r, "/admin/database").await;
        assert_eq!(row(&page, "Levels note"), "not read in writer mode");
    }

    /// B15: levels are `null` until the first discovery, then shown.
    #[tokio::test]
    async fn levels_appear_once_discovered() {
        let st = state(Config::default());
        let r = router(&st);
        let before = get_json(&r, "/admin/api/v1/database").await;
        assert_eq!(before["levels"], Value::Null);
        assert_eq!(before["levelsNote"], json!("not yet discovered"));
        st.rollup_levels.set(b6_levels(1, 5));
        let after = get_json(&r, "/admin/api/v1/database").await;
        assert_eq!(after["levels"], b11_levels_json());
        assert_eq!(after["levelsNote"], Value::Null);
    }

    /// B17: a level's table name is escaped in the level table.
    #[tokio::test]
    async fn level_table_cells_are_escaped() {
        let st = state(Config::default());
        st.rollup_levels.set(Levels {
            levels: vec![Level {
                res_ms: 60_000,
                table: "t<b>&".to_string(),
                from_ms: 0,
            }],
            shards: 1,
            counter_buckets: 5,
        });
        let page = get_page(&router(&st), "/admin/database").await;
        assert!(page.contains("<td>t&lt;b&gt;&amp;</td>"), "{page}");
        assert!(!page.contains("<b>"), "{page}");
    }
}
