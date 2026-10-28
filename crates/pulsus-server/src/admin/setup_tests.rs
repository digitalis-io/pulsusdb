//! The setup form's pages and API (issue #746 part C): tests C10, C13,
//! C14 and C18, and B16, which runs the V5 check of a generated
//! `setup.md` against a server.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use pulsus_config::Config;
use serde_json::{Value, json};
use tower::ServiceExt;

use super::html::html_text;
use super::tests::{admin_header, b6_levels, router, state};

type Pairs = Vec<(String, String)>;

fn with(mut pairs: Pairs, overrides: &[(&str, &str)]) -> Pairs {
    for (name, value) in overrides {
        pairs.retain(|(n, _)| n != name);
        pairs.push((name.to_string(), value.to_string()));
    }
    pairs
}

fn example() -> Pairs {
    pulsus_setup::example_pairs()
}

/// `s` percent-encoded for a form body: every byte but `[A-Za-z0-9]`.
fn form_text(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn form_body(pairs: &Pairs) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", form_text(k), form_text(v)))
        .collect::<Vec<_>>()
        .join("&")
}

fn json_body(pairs: &Pairs) -> String {
    let object: BTreeMap<&str, &str> = pairs
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    serde_json::to_string(&object).unwrap()
}

struct Answer {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: String,
}

async fn post(router: &axum::Router, path: &str, content_type: &str, body: String) -> Answer {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header(header::AUTHORIZATION, admin_header())
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap();
    let res = router.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    Answer {
        status,
        headers,
        body: String::from_utf8(bytes.to_vec()).unwrap(),
    }
}

async fn post_form(router: &axum::Router, path: &str, pairs: &Pairs) -> Answer {
    post(
        router,
        path,
        "application/x-www-form-urlencoded",
        form_body(pairs),
    )
    .await
}

async fn post_json(router: &axum::Router, path: &str, body: String) -> Answer {
    post(router, path, "application/json", body).await
}

fn admin_router() -> axum::Router {
    router(&state(Config::default()))
}

/// The page's file list: each download button's `value`, unescaped.
fn page_files(page: &str) -> Vec<String> {
    let marker = "name=\"file\" value=\"";
    page.match_indices(marker)
        .map(|(at, _)| {
            let rest = &page[at + marker.len()..];
            let value = &rest[..rest.find('"').expect("the value closes")];
            value.replace("&amp;", "&")
        })
        .collect()
}

/// The page's error list, as `(field, message)`, still escaped.
fn page_errors(page: &str) -> Vec<(String, String)> {
    let Some(start) = page.find("<ul class=\"errors\">") else {
        return Vec::new();
    };
    let list = &page[start..];
    let list = &list[..list.find("</ul>").expect("the list closes")];
    let marker = "<li data-field=\"";
    list.match_indices(marker)
        .map(|(at, _)| {
            let rest = &list[at + marker.len()..];
            let close = rest.find("\">").expect("the attribute closes");
            let field = rest[..close].to_string();
            let item = &rest[close + 2..];
            let item = &item[..item.find("</li>").expect("the item closes")];
            let open = "<span class=\"msg\">";
            let text = &item[item.find(open).expect("the message opens") + open.len()..];
            let message = text[..text.find("</span>").expect("the message closes")].to_string();
            (field, message)
        })
        .collect()
}

/// The page's list of what the schema keeps, still escaped: the files'
/// contents below it repeat the lines in `setup.md`.
fn totals(page: &str) -> &str {
    let start = page
        .find("<ul class=\"totals\">")
        .unwrap_or_else(|| panic!("no totals in {page}"));
    let list = &page[start..];
    &list[..list.find("</ul>").expect("the list closes")]
}

/// The API's bundle of `pairs`: `(totals, [(path, content)])`.
async fn api_bundle(router: &axum::Router, pairs: &Pairs) -> (Value, Vec<(String, String)>) {
    let res = post_json(router, "/admin/api/v1/setup/bundle", json_body(pairs)).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    assert_eq!(
        res.headers.get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let json: Value = serde_json::from_str(&res.body).unwrap();
    let files = json["files"]
        .as_array()
        .expect("files")
        .iter()
        .map(|f| {
            (
                f["path"].as_str().expect("path").to_string(),
                f["content"].as_str().expect("content").to_string(),
            )
        })
        .collect();
    (json["totals"].clone(), files)
}

/// Each file the page offers, downloaded through `POST /admin/setup/file`.
async fn page_bundle(router: &axum::Router, pairs: &Pairs) -> Vec<(String, String)> {
    let page = post_form(router, "/admin/setup", pairs).await;
    assert_eq!(page.status, StatusCode::OK, "{}", page.body);
    let mut out = Vec::new();
    for path in page_files(&page.body) {
        let mut request = pairs.clone();
        request.push(("file".to_string(), path.clone()));
        let res = post_form(router, "/admin/setup/file", &request).await;
        assert_eq!(res.status, StatusCode::OK, "{path}: {}", res.body);
        let disposition = res
            .headers
            .get(header::CONTENT_DISPOSITION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            disposition.starts_with("attachment"),
            "{path}: {disposition}"
        );
        out.push((path, res.body));
    }
    out
}

/// One level: `example` with levels 2-4 blank.
fn one_level() -> Pairs {
    with(
        example(),
        &[("l2_width", ""), ("l3_width", ""), ("l4_width", "")],
    )
}

/// Eight levels, each 365 days on local disk.
fn eight_levels() -> Pairs {
    let widths = ["5", "10", "20", "60", "120", "240", "720", "86400"];
    let froms = [
        "60", "3600", "7200", "14400", "28800", "86400", "604800", "7776000",
    ];
    let mut overrides: Vec<(String, String)> = Vec::new();
    for (i, width) in widths.iter().enumerate() {
        let k = i + 1;
        overrides.push((format!("l{k}_width"), width.to_string()));
        overrides.push((format!("l{k}_local"), "365".to_string()));
        overrides.push((format!("l{k}_from"), froms[i].to_string()));
    }
    let refs: Vec<(&str, &str)> = overrides
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    with(example(), &refs)
}

const NO_MOVE: &str =
    "kept at least 365 days; no move rule; on local disk while the local disk has room";

/// **C10: the page answers each fixture with its files and its lines, and
/// a file downloads as an attachment.**
#[tokio::test]
async fn c10_the_setup_page_shows_files_and_totals() {
    let r = admin_router();
    let form = {
        let req = Request::builder()
            .uri("/admin/setup")
            .header(header::AUTHORIZATION, admin_header())
            .body(Body::empty())
            .unwrap();
        let res = r.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    };
    for k in 1..=pulsus_schema::MAX_LEVELS {
        assert!(form.contains(&format!("name=\"l{k}_width\"")), "row {k}");
    }
    assert!(form.contains("value=\"pulsus_cluster\""), "{form}");

    let page = post_form(&r, "/admin/setup", &example()).await;
    assert_eq!(page.status, StatusCode::OK, "{}", page.body);
    let (_, api_files) = api_bundle(&r, &example()).await;
    let shown = page_files(&page.body);
    let want: Vec<String> = api_files.iter().map(|(p, _)| p.clone()).collect();
    assert_eq!(shown, want);
    assert!(totals(&page.body).contains("kept at least 97 days; at least 7 on local disk"));
    assert_eq!(
        totals(&page.body).matches(NO_MOVE).count(),
        4,
        "{}",
        page.body
    );
    assert!(totals(&page.body).contains("no row is due after 2106-02-07 06:28:15 UTC"));

    let mixed = post_form(
        &r,
        "/admin/setup",
        &with(example(), &[("l1_local", "30"), ("l1_remote", "335")]),
    )
    .await;
    assert_eq!(mixed.status, StatusCode::OK, "{}", mixed.body);
    assert!(totals(&mixed.body).contains("kept at least 365 days; at least 30 on local disk"));
    assert_eq!(totals(&mixed.body).matches(NO_MOVE).count(), 3);

    for (pairs, n) in [(one_level(), 1), (eight_levels(), 8)] {
        let page = post_form(&r, "/admin/setup", &pairs).await;
        assert_eq!(page.status, StatusCode::OK, "{n}: {}", page.body);
        assert_eq!(totals(&page.body).matches(NO_MOVE).count(), n, "{n}");
        assert_eq!(
            totals(&page.body).matches("kept at least").count(),
            n + 1,
            "{n}"
        );
    }

    let mut request = example();
    request.push(("file".to_string(), "schema.env".to_string()));
    let res = post_form(&r, "/admin/setup/file", &request).await;
    assert_eq!(res.status, StatusCode::OK);
    let disposition = res
        .headers
        .get(header::CONTENT_DISPOSITION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(disposition.starts_with("attachment"), "{disposition}");
    assert_eq!(
        res.body,
        "CLICKHOUSE_DB=pulsus\nPULSUS_CLUSTER=pulsus_cluster\nPULSUS_COLD_VOLUME=cold\n\
         PULSUS_LOGS_HOT_DAYS=7\nPULSUS_METRICS_HOT_DAYS=7\nPULSUS_RETENTION_DAYS=97\n\
         PULSUS_ROLLUP_LEVELS=60,720,7200,86400\nPULSUS_ROLLUP_RANGES=3600,86400,604800,7776000\n\
         PULSUS_ROLLUP_RETENTION_DAYS=365,365,365,365\nPULSUS_STORAGE_POLICY=pulsus_tiered\n\
         PULSUS_TRACES_HOT_DAYS=7\n"
    );
}

/// **C13: the API's bundle and the page's downloads are the same files.**
#[tokio::test]
async fn c13_api_and_page_give_the_same_files() {
    let r = admin_router();
    let fixtures = [
        ("example", example()),
        (
            "zero levels",
            with(
                example(),
                &[
                    ("l1_width", ""),
                    ("l2_width", ""),
                    ("l3_width", ""),
                    ("l4_width", ""),
                ],
            ),
        ),
        (
            "mixed",
            with(example(), &[("l1_local", "30"), ("l1_remote", "335")]),
        ),
        (
            "no remote",
            with(example(), &[("remote_url", ""), ("raw_remote_days", "")]),
        ),
        (
            "no tls",
            with(
                example(),
                &[("tls_cert", ""), ("tls_key", ""), ("tls_ca", "")],
            ),
        ),
    ];
    for (name, pairs) in fixtures {
        let (totals, api) = api_bundle(&r, &pairs).await;
        assert!(api.len() > 10, "{name}");
        let page = page_bundle(&r, &pairs).await;
        assert_eq!(page, api, "{name}");
        let validated = post_json(&r, "/admin/api/v1/setup/validate", json_body(&pairs)).await;
        assert_eq!(
            validated.status,
            StatusCode::OK,
            "{name}: {}",
            validated.body
        );
        let validated: Value = serde_json::from_str(&validated.body).unwrap();
        assert_eq!(validated, json!({"totals": totals}), "{name}");
        if name == "example" {
            assert_eq!(totals, json!({"raw": 97, "levels": [365, 365, 365, 365]}));
        }
    }
}

/// C4's inputs, as overrides of `example`.
fn c4_inputs() -> Vec<Vec<(&'static str, &'static str)>> {
    vec![
        vec![("hosts", "ch1\nch_2\nch3\nch4\nch5\nch6")],
        vec![("hosts", "ch1\nch1\nch3\nch4\nch5\nch6")],
        vec![("shards", "0")],
        vec![("replicas", "1.5")],
        vec![("replicas", "51")],
        vec![("hosts", "ch1\nch2\nch3\nch4\nch5")],
        vec![("local_path", "var/lib/clickhouse/")],
        vec![("remote_url", "https://bucket.example/pulsus/?a")],
        vec![("tls_key", "")],
        vec![("tls_ca", "etc/ch/ca.crt")],
        vec![("cores", "0")],
        vec![("l3_width", "")],
        vec![("keepers", "kp1\nkp1\nkp3")],
        vec![("keepers", "kp1\nkp2\nch1")],
        vec![("keepers", "kp1\nkp2\nkp_3")],
        vec![("keepers", "kp1\nkp2")],
        vec![("raw_local_days", "0")],
        vec![("l1_remote", "x")],
        vec![("remote_url", "")],
        vec![("cluster", "1c")],
        vec![("l1_local", "30")],
        // Several at once: the list keeps the order.
        vec![
            ("cores", "0"),
            ("hosts", "ch1"),
            ("cluster", "1c"),
            ("disk_gib", ""),
        ],
    ]
}

/// **C14: the API and the page refuse with the same list.**
#[tokio::test]
async fn c14_api_and_page_refuse_alike() {
    let r = admin_router();
    for overrides in c4_inputs() {
        let pairs = with(example(), &overrides);
        let api = post_json(&r, "/admin/api/v1/setup/validate", json_body(&pairs)).await;
        assert_eq!(
            api.status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{overrides:?}: {}",
            api.body
        );
        assert_eq!(
            api.headers.get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let json: Value = serde_json::from_str(&api.body).unwrap();
        let api_errors: Vec<(String, String)> = json["errors"]
            .as_array()
            .expect("errors")
            .iter()
            .map(|e| {
                (
                    e["field"].as_str().expect("field").to_string(),
                    e["message"].as_str().expect("message").to_string(),
                )
            })
            .collect();
        assert!(!api_errors.is_empty(), "{overrides:?}");
        let bundle = post_json(&r, "/admin/api/v1/setup/bundle", json_body(&pairs)).await;
        assert_eq!(
            bundle.status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{overrides:?}"
        );
        assert_eq!(bundle.body, api.body, "{overrides:?}");

        let page = post_form(&r, "/admin/setup", &pairs).await;
        assert_eq!(page.status, StatusCode::OK, "{overrides:?}");
        let escaped: Vec<(String, String)> = api_errors
            .iter()
            .map(|(f, m)| (html_text(f), html_text(m)))
            .collect();
        assert_eq!(page_errors(&page.body), escaped, "{overrides:?}");
        assert!(page_files(&page.body).is_empty(), "{overrides:?}");
    }

    let schema = with(example(), &[("l1_local", "30")]);
    let api = post_json(&r, "/admin/api/v1/setup/validate", json_body(&schema)).await;
    assert_eq!(
        serde_json::from_str::<Value>(&api.body).unwrap(),
        json!({"errors": [{
            "field": "levels",
            "message": "PULSUS_ROLLUP_RETENTION_DAYS: 30 for the 60s level is not a whole number of days from 97 to 4294967295"
        }]})
    );

    for body in [r#"{"hosts": 6}"#, "not json", r#"["hosts"]"#] {
        for path in ["/admin/api/v1/setup/validate", "/admin/api/v1/setup/bundle"] {
            let res = post_json(&r, path, body.to_string()).await;
            assert_eq!(
                res.status,
                StatusCode::BAD_REQUEST,
                "{path} {body}: {}",
                res.body
            );
            let json: Value = serde_json::from_str(&res.body).unwrap();
            assert_eq!(json["errors"][0]["field"], Value::Null, "{body}");
            assert!(
                json["errors"][0]["message"]
                    .as_str()
                    .is_some_and(|m| !m.is_empty())
            );
        }
    }
}

/// **C18: a hundred hosts and nine Keepers.**
#[tokio::test]
async fn c18_a_hundred_hosts() {
    let r = admin_router();
    let hosts = (1..=100)
        .map(|i| format!("h{i}"))
        .collect::<Vec<_>>()
        .join("\n");
    let keepers = (1..=9)
        .map(|i| format!("k{i}"))
        .collect::<Vec<_>>()
        .join("\n");
    let pairs = with(
        example(),
        &[
            ("shards", "10"),
            ("replicas", "10"),
            ("hosts", &hosts),
            ("keepers", &keepers),
        ],
    );
    let (_, api) = api_bundle(&r, &pairs).await;
    assert_eq!(api.len(), 100 * 5 + 9 + 6);
    let page = page_bundle(&r, &pairs).await;
    assert_eq!(page, api);
}

/// The text between `# V5 begin` and `# V5 end`, both included.
fn v5_block(setup_md: &str) -> String {
    let start = setup_md.find("# V5 begin").expect("the V5 block");
    let end = setup_md[start..].find("# V5 end").expect("the block's end") + start;
    setup_md[start..end + "# V5 end".len()].to_string()
}

/// Runs `block` with `sh`, against `url`, with the test's admin pair.
async fn run_v5(block: String, url: String, env: &[(&str, &str)]) -> (Option<i32>, String, String) {
    let env: Vec<(String, String)> = env
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    tokio::task::spawn_blocking(move || {
        let out = std::process::Command::new("sh")
            .arg("-c")
            .arg(block)
            .env("PULSUS_URL", url)
            .env("PULSUS_ADMIN_USER", "a")
            .env("PULSUS_ADMIN_PASSWORD", "s3cret")
            .envs(env)
            .output()
            .expect("run sh");
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stdout).to_string(),
            String::from_utf8_lossy(&out.stderr).to_string(),
        )
    })
    .await
    .unwrap()
}

/// **B16: V5 waits for the levels, and gives up when they never come.**
#[tokio::test(flavor = "multi_thread")]
async fn b16_v5_polls_until_the_levels_are_discovered() {
    let two = with(example(), &[("l3_width", ""), ("l4_width", "")]);
    let validated = pulsus_setup::validate(&pulsus_setup::Inputs::from_pairs(two.clone()))
        .expect("two levels are accepted");
    let files = pulsus_setup::bundle(&validated);
    let setup_md = &files
        .iter()
        .find(|f| f.path == "setup.md")
        .expect("setup.md")
        .content;
    let block = v5_block(setup_md);

    for (late, env, code) in [
        (true, vec![], Some(0)),
        (false, vec![("V5_TRIES", "2"), ("V5_PAUSE", "1")], Some(1)),
    ] {
        let st = state(Config::default());
        let app = router(&st);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        if late {
            let slot = st.rollup_levels.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(7)).await;
                slot.set(b6_levels(1, 5));
            });
        }
        let started = Instant::now();
        let (got, out, err) = run_v5(block.clone(), url, &env).await;
        assert_eq!(got, code, "late={late}: {out} {err}");
        if late {
            assert_eq!(out, "V5: 2 levels\n");
            assert!(
                started.elapsed() >= Duration::from_secs(7),
                "no empty poll first"
            );
        } else {
            assert_eq!(err, "V5: got null, want 2 levels\n");
        }
        server.abort();
    }
}

/// **Review r1 finding 4: with no levels, V5 passes only on an empty
/// list: not before discovery (`null`), and not on a refused request.**
#[tokio::test(flavor = "multi_thread")]
async fn v5_for_no_levels_needs_an_answered_empty_list() {
    let none = with(
        example(),
        &[
            ("l1_width", ""),
            ("l2_width", ""),
            ("l3_width", ""),
            ("l4_width", ""),
        ],
    );
    let validated = pulsus_setup::validate(&pulsus_setup::Inputs::from_pairs(none))
        .expect("no levels are accepted");
    let files = pulsus_setup::bundle(&validated);
    let block = v5_block(
        &files
            .iter()
            .find(|f| f.path == "setup.md")
            .expect("setup.md")
            .content,
    );
    let quick = [("V5_TRIES", "1"), ("V5_PAUSE", "0")];
    for (case, set, password, code, out_want, err_want) in [
        (
            "before discovery",
            false,
            "s3cret",
            Some(1),
            "",
            "V5: got null, want 0 levels\n",
        ),
        (
            "refused",
            true,
            "wrong",
            Some(1),
            "",
            "V5: got an error, want 0 levels\n",
        ),
        (
            "discovered, none",
            true,
            "s3cret",
            Some(0),
            "V5: 0 levels\n",
            "",
        ),
    ] {
        let st = state(Config::default());
        if set {
            st.rollup_levels.set(pulsus_read::metrics::rollup::Levels {
                levels: Vec::new(),
                shards: 1,
                counter_buckets: 5,
            });
        }
        let app = router(&st);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut env = quick.to_vec();
        env.push(("PULSUS_ADMIN_PASSWORD", password));
        let (got, out, err) = run_v5(block.clone(), url, &env).await;
        assert_eq!(
            (got, out.as_str(), err.as_str()),
            (code, out_want, err_want),
            "{case}"
        );
        server.abort();
    }
}
