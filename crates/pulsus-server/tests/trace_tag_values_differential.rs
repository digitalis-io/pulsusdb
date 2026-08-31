//! Issue #478: the ledger link (hermetic) and the oracle leg.
//!
//! # The two tests, and why they are in one file
//!
//! Both read `tests/fixtures/reference-tag-values.json`, and neither
//! compares PulsusDB against PulsusDB: the hermetic test compares the
//! fixture against the committed ledger, and the oracle leg compares the
//! fixture against the pinned reference build. Our own answers are
//! asserted in `traces_tag_values_narrow_live.rs`, against the same
//! fixture — so a driver that mangled a value fails its own leg rather
//! than agreeing with the other one.
//!
//! # Why the oracle leg needs TWO endpoints
//!
//! The fixture carries two corpora captured by two issues, and they
//! cannot share one reference instance: **measured on the pinned build,
//! its tag-value time filtering is block-granular rather than
//! span-granular** — two corpora pushed 120 s apart were both returned by
//! a window covering only the later one, so a window cannot separate
//! them. The `#476` sections therefore replay against their own endpoint,
//! against their own corpus, exactly as they were captured.
//!
//! The `#478` sections replay against the other endpoint in the order
//! they were captured: push C10, replay the `q` matrix and the range
//! shapes, then push C4 and replay the span-name section over the union.
//! Re-running the leg against an already-populated instance is NOT
//! supported — CI starts a fresh container per job.
//!
//! # Gate
//!
//! Skips unless all four endpoint variables are set; **fail-closed**, so
//! a job that dropped the `env:` block reddens instead of reporting
//! green. No ClickHouse is needed — this leg never touches PulsusDB.
//!
//! ```text
//! podman run -d --name pulsus-tempo-478a -p 13478:3200 -p 14478:4318 \
//!     -v $PWD/ci/tempo/tempo-compare.yaml:/etc/tempo/tempo.yaml:ro \
//!     grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad58700aa96880653c3d8f7 \
//!     -config.file=/etc/tempo/tempo.yaml
//! podman run -d --name pulsus-tempo-478b -p 13479:3200 -p 14479:4318 \
//!     -v $PWD/ci/tempo/tempo-compare.yaml:/etc/tempo/tempo.yaml:ro \
//!     grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad58700aa96880653c3d8f7 \
//!     -config.file=/etc/tempo/tempo.yaml
//! PULSUSDB_TAG_VALUES_DIFF_URL=http://localhost:13478 \
//!   PULSUSDB_TAG_VALUES_OTLP_URL=http://localhost:14478 \
//!   PULSUSDB_TAG_VALUES_476_DIFF_URL=http://localhost:13479 \
//!   PULSUSDB_TAG_VALUES_476_OTLP_URL=http://localhost:14479 \
//!   cargo test -p pulsus-server --test trace_tag_values_differential -- --nocapture
//! ```
//!
//! Clean-room: no reference source is read. The corpora are our own and
//! the reference's answers are black-box runtime output.

#[path = "support/tag_values_corpus.rs"]
mod corpus;

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::process::Command;
use std::time::{Duration, Instant};

use prost::Message;
use serde_json::Value;

fn fixture() -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/reference-tag-values.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", path.display()))
}

fn ledger() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/benchmarks/traces-differential-ledger.md");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The ledger, parsed into `heading id -> row body`. A row starts at its
/// own `### \`<id>\`` heading and ends at the next `### `.
fn ledger_rows(text: &str) -> BTreeMap<String, String> {
    let mut rows = BTreeMap::new();
    let mut current: Option<(String, String)> = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("### ") {
            if let Some((id, body)) = current.take() {
                rows.insert(id, body);
            }
            let id = rest
                .split('`')
                .nth(1)
                .unwrap_or_default()
                .trim()
                .to_string();
            current = Some((id, String::new()));
            continue;
        }
        if let Some((_, body)) = current.as_mut() {
            body.push_str(line);
            body.push('\n');
        }
    }
    if let Some((id, body)) = current {
        rows.insert(id, body);
    }
    rows
}

/// The case ids a row's `- **Cases.**` bullet names, as backticked
/// tokens. Empty when the row has no such bullet.
fn row_cases(body: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let Some(start) = body.find("- **Cases.**") else {
        return out;
    };
    // The bullet runs to the next blank line — a `Cases` bullet may wrap.
    let tail = &body[start..];
    let bullet = match tail.find("\n\n") {
        Some(end) => &tail[..end],
        None => tail,
    };
    let mut in_tick = false;
    let mut token = String::new();
    for ch in bullet.chars() {
        if ch == '`' {
            if in_tick {
                out.insert(token.clone());
                token.clear();
            }
            in_tick = !in_tick;
        } else if in_tick {
            token.push(ch);
        }
    }
    out
}

/// One fixture case: its id, the section it came from, and the two
/// answers.
struct Case {
    id: String,
    reference: Value,
    pulsus: Value,
    ledger: Option<String>,
}

/// Every case in every section that carries both answers.
fn cases(fx: &Value) -> Vec<Case> {
    let mut out = Vec::new();
    for section in ["q_matrix", "range_faults", "range_accepted", "span_names"] {
        let Some(map) = fx[section].as_object() else {
            panic!("fixture section {section} is missing");
        };
        for (id, case) in map {
            if case["reference"].is_null() {
                // A route the reference does not serve: ours alone, so
                // there is nothing to differ from.
                continue;
            }
            out.push(Case {
                id: id.clone(),
                reference: case["reference"].clone(),
                pulsus: case["pulsus"].clone(),
                ledger: case["ledger"].as_str().map(str::to_string),
            });
        }
    }
    assert!(!out.is_empty(), "the fixture carries no comparable case");
    out
}

/// Normalises an answer for the DIFFER decision: the reference does not
/// sort its tag values (ledger `traceql-tag-discovery-ordering`), so a
/// value list is compared as a sorted multiset. Everything else compares
/// as it stands.
fn normalised(answer: &Value) -> Value {
    let mut copy = answer.clone();
    if let Some(values) = copy.get_mut("values").and_then(|v| v.as_array_mut()) {
        values.sort_by_key(|v| v.to_string());
    }
    copy
}

fn differs(case: &Case) -> bool {
    normalised(&case.reference) != normalised(&case.pulsus)
}

/// Every ledger id THIS PLAN introduces or extends. A literal, so a row
/// that stops being referenced fails rather than quietly rotting.
const PLAN_LEDGER_IDS: [&str; 9] = [
    "traceql-tag-values-q-lenient-parse-not-reproduced",
    "traceql-tag-values-q-partial-pushdown",
    "traceql-tag-values-unscoped-attr-narrows-here",
    "traceql-tag-values-requested-tag-condition-applied",
    "traceql-tag-values-window-is-day-granular",
    "traceql-tag-values-span-name-byte-cap",
    "traceql-tag-values-range-error-text",
    "traceql-tag-values-narrowed-set-complete-here",
    "traceql-v1-tag-values-statics-unimplemented",
];

/// Issue #478, criterion 9. **The case-to-row binding is MUTUAL.**
///
/// A presence test — "the id appears somewhere in the document" — passes
/// when a case points at the wrong row, because both ids exist. So does a
/// route-based check, when the two cases are on the SAME route. The four
/// assertions below bind a case to a row and the row back to that case,
/// so exchanging the `ledger` ids of two same-route divergences fails
/// assertion 3 on both of them.
#[test]
fn every_divergence_names_its_own_ledger_row() {
    let fx = fixture();
    let text = ledger();
    let rows = ledger_rows(&text);
    let cases = cases(&fx);

    let mut used: BTreeSet<String> = BTreeSet::new();
    let mut differing = 0usize;
    for case in &cases {
        if !differs(case) {
            assert!(
                case.ledger.is_none(),
                "case {} carries a ledger id but its two answers agree",
                case.id
            );
            continue;
        }
        differing += 1;
        // 1. the case names a ledger id.
        let Some(id) = case.ledger.as_deref() else {
            panic!(
                "case {} diverges ({} vs {}) and names no ledger row",
                case.id, case.reference, case.pulsus
            );
        };
        // 2. a row exists whose HEADING id is exactly that — not a
        //    substring match anywhere in the document.
        let Some(body) = rows.get(id) else {
            panic!(
                "case {} names ledger row `{id}`, which has no heading",
                case.id
            );
        };
        // 3. that row's Cases bullet names this case's own id.
        let named = row_cases(body);
        assert!(
            named.contains(&case.id),
            "assertion 3 failed for {}: it names row {id}, whose Cases bullet names {:?}",
            case.id,
            named
        );
        used.insert(id.to_string());
    }
    assert!(
        differing >= 20,
        "only {differing} differing cases — the fixture has stopped carrying the divergences \
         this check exists for"
    );
    // 4. every ledger id this plan introduces is named by at least one
    //    differing case.
    for id in PLAN_LEDGER_IDS {
        assert!(
            rows.contains_key(id),
            "ledger row `{id}` is missing from docs/benchmarks/traces-differential-ledger.md"
        );
        assert!(
            used.contains(id),
            "ledger row `{id}` is named by no differing fixture case — either the divergence \
             stopped being produced and the row was not retired, or a case lost its id"
        );
    }
}

// =====================================================================
// The oracle leg.
// =====================================================================

struct CurlResponse {
    status: u16,
    content_type: String,
    body: Vec<u8>,
}

fn curl(args: &[&str], url: &str, ctx: &str) -> CurlResponse {
    let dir = std::env::temp_dir().join(format!("pulsus478-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let body_path = dir.join("body");
    let out = Command::new("curl")
        .args(["-s", "--max-time", "30"])
        .args(args)
        .args(["-o", body_path.to_str().expect("utf8 path")])
        .args(["-w", "%{http_code}\n%{content_type}"])
        .arg(url)
        .output()
        .expect("curl on PATH");
    let meta = String::from_utf8_lossy(&out.stdout);
    let mut lines = meta.lines();
    let status: u16 = lines
        .next()
        .unwrap_or_default()
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("{ctx}: curl reported no HTTP status ({e}) for {url}"));
    let content_type = lines.next().unwrap_or_default().trim().to_string();
    CurlResponse {
        status,
        content_type,
        body: std::fs::read(&body_path).unwrap_or_default(),
    }
}

fn push(
    otlp_base: &str,
    req: &opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
    ctx: &str,
) {
    let dir = std::env::temp_dir().join(format!("pulsus478-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let path = dir.join("push.bin");
    let mut f = std::fs::File::create(&path).expect("create push body");
    f.write_all(&req.encode_to_vec()).expect("write push body");
    drop(f);
    let url = format!("{}/v1/traces", otlp_base.trim_end_matches('/'));
    let res = curl(
        &[
            "-X",
            "POST",
            "-H",
            "Content-Type: application/x-protobuf",
            "--data-binary",
            &format!("@{}", path.to_str().expect("utf8 path")),
        ],
        &url,
        ctx,
    );
    assert_eq!(res.status, 200, "{ctx}: OTLP push to {url}");
}

/// The reference cuts a live-store block a few seconds after a push, so
/// the first read after ingest can legitimately be short.
fn wait_for_names(api_base: &str, query: &str, want: usize, ctx: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let res = curl(
            &[],
            &format!(
                "{}/api/v2/search/tag/name/values?{query}",
                api_base.trim_end_matches('/')
            ),
            ctx,
        );
        let body_text = String::from_utf8_lossy(&res.body).to_string();
        if res.status == 200 {
            let body: Value = serde_json::from_str(&body_text).unwrap_or(Value::Null);
            if body["tagValues"].as_array().map(Vec::len).unwrap_or(0) >= want {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "{ctx}: the corpus never became visible within 120s (last body {body_text})"
        );
        std::thread::sleep(Duration::from_millis(1_000));
    }
}

/// The `(type, value)` pairs of a tag-values body, in whatever order the
/// server sent them. The v1 flat shape's bare strings are read as
/// `string`-typed.
fn entries(body: &Value) -> Vec<(String, String)> {
    body["tagValues"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|e| match e {
                    Value::String(s) => ("string".to_string(), s.clone()),
                    _ => (
                        e["type"].as_str().unwrap_or_default().to_string(),
                        e["value"].as_str().unwrap_or_default().to_string(),
                    ),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn sorted(mut v: Vec<(String, String)>) -> Vec<(String, String)> {
    v.sort();
    v
}

fn expected_entries(answer: &Value) -> Vec<(String, String)> {
    answer["values"]
        .as_array()
        .expect("values array")
        .iter()
        .map(|pair| {
            let a = pair.as_array().expect("pair");
            (
                a[0].as_str().unwrap_or_default().to_string(),
                a[1].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

#[test]
fn the_committed_capture_matches_the_live_reference() {
    for var in [
        "PULSUSDB_TAG_VALUES_DIFF_URL",
        "PULSUSDB_TAG_VALUES_OTLP_URL",
        "PULSUSDB_TAG_VALUES_476_DIFF_URL",
        "PULSUSDB_TAG_VALUES_476_OTLP_URL",
    ] {
        pulsus_testkit::require_live_endpoint_gate(var);
    }
    let (Ok(api), Ok(otlp), Ok(api476), Ok(otlp476)) = (
        std::env::var("PULSUSDB_TAG_VALUES_DIFF_URL"),
        std::env::var("PULSUSDB_TAG_VALUES_OTLP_URL"),
        std::env::var("PULSUSDB_TAG_VALUES_476_DIFF_URL"),
        std::env::var("PULSUSDB_TAG_VALUES_476_OTLP_URL"),
    ) else {
        eprintln!(
            "skipping the tag-values oracle leg — set PULSUSDB_TAG_VALUES_DIFF_URL, \
             PULSUSDB_TAG_VALUES_OTLP_URL, PULSUSDB_TAG_VALUES_476_DIFF_URL and \
             PULSUSDB_TAG_VALUES_476_OTLP_URL"
        );
        return;
    };
    let fx = fixture();

    replay_478_sections(&fx, &api, &otlp);
    replay_476_sections(&fx, &api476, &otlp476);
}

/// The sections issue #478 captured, replayed in capture order.
fn replay_478_sections(fx: &Value, api: &str, otlp: &str) {
    let base = corpus::base_ns();
    let start = base / 1_000_000_000 - 3_600;
    let end = base / 1_000_000_000 + 600;
    let window = format!("start={start}&end={end}");

    push(otlp, &corpus::c10_request(base), "C10 push");
    wait_for_names(api, &window, corpus::C10.len(), "C10 visibility");

    let mut checked = 0usize;
    for (id, case) in fx["q_matrix"].as_object().expect("q_matrix") {
        if case["reference"].is_null() {
            continue;
        }
        let route = case["route"].as_str().expect("route");
        // The zero-width window case carries its own params.
        let mut url = if case.get("params").is_some() {
            format!(
                "{}{route}?start={start}&end={start}",
                api.trim_end_matches('/')
            )
        } else {
            format!("{}{route}?{window}", api.trim_end_matches('/'))
        };
        if let Some(q) = case["q"].as_str() {
            url.push_str(&format!("&q={}", urlencode(q)));
        }
        let res = curl(&[], &url, id);
        let want_status = case["reference"]["status"].as_u64().unwrap_or(200) as u16;
        assert_eq!(
            res.status,
            want_status,
            "{id}: {url} — body {}",
            String::from_utf8_lossy(&res.body)
        );
        let body: Value = serde_json::from_slice(&res.body)
            .unwrap_or_else(|e| panic!("{id}: reference body is not JSON: {e}"));
        assert_eq!(
            sorted(entries(&body)),
            sorted(expected_entries(&case["reference"])),
            "{id}: the reference's answer has drifted from the committed capture"
        );
        checked += 1;
    }
    assert!(checked >= 40, "only {checked} q-matrix cases replayed");

    for (id, case) in fx["range_faults"]
        .as_object()
        .expect("range_faults")
        .iter()
        .chain(fx["range_accepted"].as_object().expect("range_accepted"))
    {
        let route = case["route"].as_str().expect("route");
        let shape = case["shape"].as_str().expect("shape");
        let query = match shape {
            "malformed_start" => format!("start=abc&end={end}"),
            "malformed_end" => format!("start={start}&end=abc"),
            "half_start" => format!("start={start}"),
            "half_end" => format!("end={end}"),
            "zero_start" => format!("start=0&end={end}"),
            "zero_end" => format!("start={start}&end=0"),
            "inverted" => format!("start={end}&end={start}"),
            "both_zero" => "start=0&end=0".to_string(),
            "zero_width" => format!("start={start}&end={start}"),
            other => panic!("{id}: unknown range shape {other}"),
        };
        let res = curl(
            &[],
            &format!("{}{route}?{query}", api.trim_end_matches('/')),
            id,
        );
        assert_eq!(
            res.status,
            case["reference"]["status"].as_u64().unwrap_or(0) as u16,
            "{id}: status"
        );
        assert_eq!(
            res.content_type,
            case["reference"]["content_type"]
                .as_str()
                .unwrap_or_default(),
            "{id}: content type"
        );
        // The reference's 400 BODIES are deliberately not compared: one
        // ends in its runtime's own integer-parse error and one names a
        // configured maximum-window-width setting we do not have. Our own
        // bodies are asserted in `traces_tag_values_narrow_live.rs`.
    }

    // Phase 3: the typing corpus, over the union.
    push(otlp, &corpus::c4_request(base), "C4 push");
    let union = corpus::C10.len() + corpus::c4_rows().len();
    wait_for_names(api, &window, union, "C4 visibility");
    let res = curl(
        &[],
        &format!(
            "{}/api/v2/search/tag/name/values?{window}",
            api.trim_end_matches('/')
        ),
        "span_names",
    );
    assert_eq!(res.status, 200, "span_names: status");
    let body: Value = serde_json::from_slice(&res.body).expect("span_names body");
    let got = entries(&body);
    for (ty, val) in expected_entries(&fx["span_names"]["T-TYPES"]["reference"]) {
        assert!(
            got.contains(&(ty.clone(), val.clone())),
            "span_names T-TYPES: the reference no longer reports {val:?} as {ty}"
        );
    }
    let cap = &fx["span_names"]["T-CAP"];
    let want_len = cap["reference"]["value_len"].as_u64().expect("value_len") as usize;
    let ch = cap["repeated_char"].as_str().expect("repeated_char");
    let long = got
        .iter()
        .find(|(_, v)| v.len() >= 1_000 && v.starts_with(ch))
        .unwrap_or_else(|| panic!("span_names T-CAP: no over-cap name in the reference's answer"));
    assert_eq!(
        long.1.chars().count(),
        want_len,
        "span_names T-CAP: the reference no longer returns the whole name"
    );
}

/// The sections issue #476 captured, replayed against their own endpoint
/// and their own corpus. Nothing re-ran these before issue #478 folded
/// the two artifacts into one file.
fn replay_476_sections(fx: &Value, api: &str, otlp: &str) {
    let base = corpus::base_ns();
    push(otlp, &corpus::ac_476_request(base), "#476 corpus push");
    let start = base / 1_000_000_000 - 3_600;
    let end = base / 1_000_000_000 + 600;
    wait_for_names(
        api,
        &format!("start={start}&end={end}"),
        2,
        "#476 visibility",
    );

    let keys = fx["v2_tag_values"].as_object().expect("v2_tag_values");
    assert_eq!(
        keys.len(),
        11,
        "the #476 section must cover every corpus key"
    );
    for (key, want) in keys {
        let url = format!(
            "{}/api/v2/search/tag/{key}/values?start={start}&end={end}",
            api.trim_end_matches('/')
        );
        let res = curl(&[], &url, key);
        assert_eq!(res.status, 200, "#476 {key}: status");
        let body: Value = serde_json::from_slice(&res.body).expect("body");
        assert_eq!(
            sorted(entries(&body)),
            sorted(entries(want)),
            "#476 {key}: the reference's answer has drifted from the committed capture"
        );
    }
    let url = format!(
        "{}/api/search/tag/port/values?start={start}&end={end}",
        api.trim_end_matches('/')
    );
    let res = curl(&[], &url, "v1 flat port");
    assert_eq!(res.status, 200, "#476 v1 flat port: status");
    let body: Value = serde_json::from_slice(&res.body).expect("body");
    assert_eq!(
        sorted(entries(&body)),
        sorted(entries(&fx["v1_flat_port_values"])),
        "#476 v1 flat port: drift"
    );
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
