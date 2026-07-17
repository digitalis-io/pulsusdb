//! Deterministic OTLP-logs corpus generator for the M6-09 LogQL-pipeline
//! differential (`e2e/src/logs.rs`), mirroring `traces_corpus.rs`'s
//! shape: every semantic feature of a log record is a pure function of
//! its `log_idx`, so the corpus — and each case's expected result set —
//! is byte-reproducible from a [`LogCorpusSpec`] alone. No PRNG at all:
//! log records carry no ids, so unlike the traces corpus there is
//! nothing left to seed (the fixture `seed` is carried for shape parity
//! and future use).
//!
//! **Three body shapes, keyed by service** so each case's parser only
//! ever sees bodies built for it: `svc-json` (JSON objects, incl. a
//! nested member), `svc-logfmt` (`k=v` pairs with duration/bytes
//! fields), `svc-plain` (space-delimited text for `regexp`/`pattern`).
//! Resource attributes are exactly `service.name` + `run_id` — both
//! promoted to stream labels by both stores (PulsusDB flattens all
//! resource attrs; the oracle promotes `service.name` by default and
//! `run_id` via its OTLP config) — and no scope/severity/record
//! attributes are emitted, so neither store grows extra stream labels
//! or structured metadata and stream-label-set equality is mechanically
//! valid (plan v3 delta 3).
//!
//! **Two independent oracles (the circularity breaker):**
//! [`case_projection`] derives each case's verdict + final
//! `(labels, line)` from the record's **typed feature fields**;
//! [`naive_matches`] re-derives every case predicate by parsing the
//! generated **body text** with its own tiny parsers, never reading the
//! feature fields or the by-construction labels. A hermetic unit test
//! asserts they agree for every case × record.

use std::collections::{BTreeMap, BTreeSet};

use crate::corpus::Scale;

/// The resource attribute isolating one run's streams (the
/// `scenarios::RUN_ID_LABEL` / `traces_corpus::RUN_ATTR` precedent).
pub const RUN_ATTR: &str = "run_id";

/// Every committed differential case id, in fixture order —
/// `test/fixtures/logs/differential.json` `cases[]` must match exactly
/// (hermetic unit test in `logs.rs`).
pub const CASE_IDS: &[&str] = &[
    "json_string_filter",
    "json_label_filter_regex",
    "logfmt_string_filter",
    "regexp_extract_filter",
    "pattern_extract_filter",
    "numeric_number_filter",
    "numeric_duration_filter",
    "numeric_bytes_filter",
    "line_format_rewrite",
    "label_format_rename",
];

pub const SVC_JSON: &str = "svc-json";
pub const SVC_LOGFMT: &str = "svc-logfmt";
pub const SVC_PLAIN: &str = "svc-plain";

/// Generation parameters for one corpus.
#[derive(Debug, Clone)]
pub struct LogCorpusSpec {
    pub scale: Scale,
    pub record_count: usize,
    pub step_ns: i64,
    pub base_ns: i64,
    pub run_id: String,
}

/// One generated log record: the typed feature fields (the
/// by-construction oracle's inputs) plus the rendered body.
#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedRecord {
    pub service: &'static str,
    pub ts_ns: i64,
    pub body: String,
    // svc-json fields.
    pub method: &'static str,
    pub status: i64,
    pub req_path: &'static str,
    // svc-logfmt fields.
    pub level: &'static str,
    pub took_ms: i64,
    pub size_kb: i64,
    pub msg_idx: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogCorpus {
    pub run_id: String,
    pub records: Vec<GeneratedRecord>,
    pub first_ts_ns: i64,
    pub last_ts_ns: i64,
    pub scale: Scale,
}

/// One case's expected result shape: final stream label set → the
/// `(timestamp_ns, line)` entry set — exactly what both stores'
/// `query_range` responses normalize to in `logs.rs`.
pub type ExpectedResult = BTreeMap<BTreeMap<String, String>, BTreeSet<(i64, String)>>;

// ---------------------------------------------------------------------
// Per-record feature assignment: pure functions of `log_idx`.
// ---------------------------------------------------------------------

fn service_of(i: usize) -> &'static str {
    [SVC_JSON, SVC_LOGFMT, SVC_PLAIN][i % 3]
}
fn method_of(i: usize) -> &'static str {
    ["GET", "PUT", "DELETE"][(i / 3) % 3]
}
fn status_of(i: usize) -> i64 {
    match (i / 3) % 4 {
        0 => 500,
        1 => 503,
        _ => 200,
    }
}
fn req_path_of(i: usize) -> &'static str {
    if (i / 3).is_multiple_of(2) {
        "/api/items"
    } else {
        "/api/users"
    }
}
fn level_of(i: usize) -> &'static str {
    if (i / 3).is_multiple_of(3) {
        "error"
    } else {
        "info"
    }
}
fn took_ms_of(i: usize) -> i64 {
    100 + ((i / 3) % 5) as i64 * 100 // 100..500ms
}
fn size_kb_of(i: usize) -> i64 {
    1 + ((i / 3) % 10) as i64 // 1..10kb
}

fn render_body(r: &GeneratedRecord) -> String {
    match r.service {
        SVC_JSON => format!(
            r#"{{"method":"{}","status":{},"took_ms":{},"req":{{"path":"{}"}}}}"#,
            r.method, r.status, r.took_ms, r.req_path
        ),
        SVC_LOGFMT => format!(
            r#"level={} took={}ms size={}kb msg="op {}""#,
            r.level, r.took_ms, r.size_kb, r.msg_idx
        ),
        _ => format!("{} {} {} {}ms", r.method, r.req_path, r.status, r.took_ms),
    }
}

/// Generates the corpus: every value is a pure function of
/// `(spec, log_idx)` — byte-reproducible (unit-tested).
pub fn generate(spec: &LogCorpusSpec) -> LogCorpus {
    let mut records = Vec::with_capacity(spec.record_count);
    for i in 0..spec.record_count {
        let mut r = GeneratedRecord {
            service: service_of(i),
            ts_ns: spec.base_ns + spec.step_ns * i as i64,
            body: String::new(),
            method: method_of(i),
            status: status_of(i),
            req_path: req_path_of(i),
            level: level_of(i),
            took_ms: took_ms_of(i),
            size_kb: size_kb_of(i),
            msg_idx: i,
        };
        r.body = render_body(&r);
        records.push(r);
    }
    let last_ts_ns = records.last().map_or(spec.base_ns, |r| r.ts_ns);
    LogCorpus {
        run_id: spec.run_id.clone(),
        records,
        first_ts_ns: spec.base_ns,
        last_ts_ns,
        scale: spec.scale,
    }
}

impl LogCorpus {
    /// Base stream labels both stores expose for `record` (plan v3 delta
    /// 3: resource attrs promoted identically on both sides).
    fn base_labels(&self, r: &GeneratedRecord) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("service_name".to_string(), r.service.to_string()),
            (RUN_ATTR.to_string(), self.run_id.clone()),
        ])
    }

    /// The by-construction expected result set for one committed case.
    pub fn expected_case_result(&self, case_id: &str) -> ExpectedResult {
        let mut out = ExpectedResult::new();
        for r in &self.records {
            let Some((extracted, line)) = case_projection(case_id, r) else {
                continue;
            };
            let mut labels = self.base_labels(r);
            labels.extend(extracted);
            out.entry(labels).or_default().insert((r.ts_ns, line));
        }
        out
    }

    /// Which service a case's selector scopes to (from the case id's
    /// shape family) — used by `logs.rs` only for logging.
    pub fn total_records(&self) -> usize {
        self.records.len()
    }

    /// All `(labels, entries)` for the run-scoped completeness query
    /// (`{run_id="R"}` with no pipeline): every record under its base
    /// label set.
    pub fn expected_all_records(&self) -> ExpectedResult {
        let mut out = ExpectedResult::new();
        for r in &self.records {
            out.entry(self.base_labels(r))
                .or_default()
                .insert((r.ts_ns, r.body.clone()));
        }
        out
    }
}

// ---------------------------------------------------------------------
// The by-construction oracle: verdict + final (extracted labels, line)
// per case, derived from the TYPED feature fields.
// ---------------------------------------------------------------------

/// `Some((extracted labels, final line))` when `record` satisfies
/// `case_id`'s pipeline, else `None`. The extracted labels are the
/// parser-added (and format-adjusted) labels only — base stream labels
/// are layered on by [`LogCorpus::expected_case_result`].
pub fn case_projection(
    case_id: &str,
    r: &GeneratedRecord,
) -> Option<(BTreeMap<String, String>, String)> {
    let json_labels = |r: &GeneratedRecord| {
        BTreeMap::from([
            ("method".to_string(), r.method.to_string()),
            ("status".to_string(), r.status.to_string()),
            ("took_ms".to_string(), r.took_ms.to_string()),
            ("req_path".to_string(), r.req_path.to_string()),
        ])
    };
    let logfmt_labels = |r: &GeneratedRecord| {
        BTreeMap::from([
            ("level".to_string(), r.level.to_string()),
            ("took".to_string(), format!("{}ms", r.took_ms)),
            ("size".to_string(), format!("{}kb", r.size_kb)),
            ("msg".to_string(), format!("op {}", r.msg_idx)),
        ])
    };
    let plain_labels = |r: &GeneratedRecord| {
        BTreeMap::from([
            ("method".to_string(), r.method.to_string()),
            ("path".to_string(), r.req_path.to_string()),
            ("status".to_string(), r.status.to_string()),
        ])
    };

    match case_id {
        "json_string_filter" => {
            (r.service == SVC_JSON && r.status == 500).then(|| (json_labels(r), r.body.clone()))
        }
        "json_label_filter_regex" => (r.service == SVC_JSON
            && (r.method == "GET" || r.method == "DELETE"))
            .then(|| (json_labels(r), r.body.clone())),
        "logfmt_string_filter" => (r.service == SVC_LOGFMT && r.level == "error")
            .then(|| (logfmt_labels(r), r.body.clone())),
        "regexp_extract_filter" => {
            (r.service == SVC_PLAIN && r.status == 503).then(|| (plain_labels(r), r.body.clone()))
        }
        "pattern_extract_filter" => (r.service == SVC_PLAIN && r.method == "PUT").then(|| {
            // `pattern "<method> <path> <status> <took>"` also captures
            // the trailing took token.
            let mut labels = plain_labels(r);
            labels.insert("took".to_string(), format!("{}ms", r.took_ms));
            (labels, r.body.clone())
        }),
        "numeric_number_filter" => {
            (r.service == SVC_JSON && r.status >= 500).then(|| (json_labels(r), r.body.clone()))
        }
        "numeric_duration_filter" => {
            (r.service == SVC_LOGFMT && r.took_ms > 250).then(|| (logfmt_labels(r), r.body.clone()))
        }
        "numeric_bytes_filter" => (r.service == SVC_LOGFMT && r.size_kb * 1000 > 5_000)
            .then(|| (logfmt_labels(r), r.body.clone())),
        "line_format_rewrite" => (r.service == SVC_JSON && r.status == 500)
            .then(|| (json_labels(r), format!("{} {}", r.method, r.req_path))),
        "label_format_rename" => (r.service == SVC_LOGFMT && r.level == "error").then(|| {
            let mut labels = logfmt_labels(r);
            let level = labels.remove("level").unwrap_or_default();
            labels.insert("lvl".to_string(), level);
            (labels, r.body.clone())
        }),
        other => panic!("case_projection: unknown case id {other:?}"),
    }
}

// ---------------------------------------------------------------------
// OTLP export: one ExportLogsServiceRequest for the whole corpus,
// grouped by service — resource attrs exactly {service.name, run_id},
// scope omitted, no severity/attributes (see the module doc comment).
// ---------------------------------------------------------------------

pub fn to_otlp_export_request(c: &LogCorpus) -> serde_json::Value {
    let mut groups: Vec<(&str, Vec<&GeneratedRecord>)> = Vec::new();
    for r in &c.records {
        match groups.iter_mut().find(|(svc, _)| *svc == r.service) {
            Some((_, records)) => records.push(r),
            None => groups.push((r.service, vec![r])),
        }
    }
    let resource_logs: Vec<serde_json::Value> = groups
        .into_iter()
        .map(|(service, records)| {
            let log_records: Vec<serde_json::Value> = records
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "timeUnixNano": r.ts_ns.to_string(),
                        "body": { "stringValue": r.body },
                    })
                })
                .collect();
            serde_json::json!({
                "resource": { "attributes": [
                    { "key": "service.name", "value": { "stringValue": service } },
                    { "key": RUN_ATTR, "value": { "stringValue": c.run_id } },
                ]},
                "scopeLogs": [{ "logRecords": log_records }],
            })
        })
        .collect();
    serde_json::json!({ "resourceLogs": resource_logs })
}

// ---------------------------------------------------------------------
// The independent naive evaluator: re-derives every case predicate by
// parsing the generated BODY TEXT — never reads the typed feature
// fields. Test-only (the AC8 hermetic oracle).
// ---------------------------------------------------------------------

#[cfg(test)]
fn body_json(r: &GeneratedRecord) -> Option<serde_json::Value> {
    serde_json::from_str(&r.body).ok()
}

#[cfg(test)]
fn body_logfmt(r: &GeneratedRecord) -> BTreeMap<String, String> {
    // Independent tiny logfmt reader: k=v tokens, quoted values.
    let mut out = BTreeMap::new();
    let mut rest = r.body.as_str();
    while !rest.is_empty() {
        rest = rest.trim_start();
        let Some(eq) = rest.find('=') else { break };
        let key = &rest[..eq];
        rest = &rest[eq + 1..];
        let value = if let Some(q) = rest.strip_prefix('"') {
            let end = q.find('"').unwrap_or(q.len());
            let v = &q[..end];
            rest = &q[(end + 1).min(q.len())..];
            v.to_string()
        } else {
            let end = rest.find(' ').unwrap_or(rest.len());
            let v = &rest[..end];
            rest = &rest[end..];
            v.to_string()
        };
        out.insert(key.to_string(), value);
    }
    out
}

#[cfg(test)]
fn plain_fields(r: &GeneratedRecord) -> Option<(String, String, i64, i64)> {
    // "<method> <path> <status> <took>ms"
    let parts: Vec<&str> = r.body.split(' ').collect();
    if parts.len() != 4 {
        return None;
    }
    let status: i64 = parts[2].parse().ok()?;
    let took: i64 = parts[3].strip_suffix("ms")?.parse().ok()?;
    Some((parts[0].to_string(), parts[1].to_string(), status, took))
}

/// Evaluates one committed case's predicate over the record's body text
/// (plus the service scope its selector pins) — the independent oracle.
#[cfg(test)]
pub fn naive_matches(case_id: &str, r: &GeneratedRecord) -> bool {
    match case_id {
        "json_string_filter" | "line_format_rewrite" => {
            r.service == SVC_JSON && body_json(r).is_some_and(|v| v["status"].as_i64() == Some(500))
        }
        "json_label_filter_regex" => {
            r.service == SVC_JSON
                && body_json(r)
                    .is_some_and(|v| matches!(v["method"].as_str(), Some("GET") | Some("DELETE")))
        }
        "logfmt_string_filter" | "label_format_rename" => {
            r.service == SVC_LOGFMT
                && body_logfmt(r).get("level").map(String::as_str) == Some("error")
        }
        "regexp_extract_filter" => {
            r.service == SVC_PLAIN && plain_fields(r).is_some_and(|(_, _, status, _)| status == 503)
        }
        "pattern_extract_filter" => {
            r.service == SVC_PLAIN
                && plain_fields(r).is_some_and(|(method, _, _, _)| method == "PUT")
        }
        "numeric_number_filter" => {
            r.service == SVC_JSON
                && body_json(r).is_some_and(|v| v["status"].as_i64().is_some_and(|s| s >= 500))
        }
        "numeric_duration_filter" => {
            r.service == SVC_LOGFMT
                && body_logfmt(r)
                    .get("took")
                    .and_then(|v| v.strip_suffix("ms").and_then(|n| n.parse::<f64>().ok()))
                    .is_some_and(|ms| ms / 1000.0 > 0.25)
        }
        "numeric_bytes_filter" => {
            r.service == SVC_LOGFMT
                && body_logfmt(r)
                    .get("size")
                    .and_then(|v| v.strip_suffix("kb").and_then(|n| n.parse::<f64>().ok()))
                    .is_some_and(|kb| kb * 1000.0 > 5_000.0)
        }
        other => panic!("naive_matches: unknown case id {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(record_count: usize) -> LogCorpusSpec {
        LogCorpusSpec {
            scale: Scale::Ci,
            record_count,
            step_ns: 1_000_000_000,
            base_ns: 1_700_000_000_000_000_000,
            run_id: "e2e-logs-test-run".to_string(),
        }
    }

    #[test]
    fn generate_is_deterministic_for_the_same_spec() {
        let a = generate(&spec(60));
        let b = generate(&spec(60));
        assert_eq!(a, b);
        assert_eq!(to_otlp_export_request(&a), to_otlp_export_request(&b));
    }

    /// The circularity breaker: the body-text-parsing oracle agrees with
    /// the typed-field projection for every case × record, at both tier
    /// sizes.
    #[test]
    fn naive_evaluator_agrees_with_the_by_construction_projection() {
        for count in [60, 300] {
            let corpus = generate(&spec(count));
            for (idx, r) in corpus.records.iter().enumerate() {
                for case in CASE_IDS {
                    assert_eq!(
                        naive_matches(case, r),
                        case_projection(case, r).is_some(),
                        "case {case:?} disagrees on log_idx {idx} (count {count})"
                    );
                }
            }
        }
    }

    /// Set comparisons are only well-defined unclipped: every case must
    /// match at least one record and stay strictly below the request
    /// limit, at both tiers.
    #[test]
    fn every_case_result_is_non_empty_and_below_the_differential_limit() {
        const DIFFERENTIAL_LIMIT: usize = 1_000; // fixture `limit`
        for count in [60, 300] {
            let corpus = generate(&spec(count));
            for case in CASE_IDS {
                let expected = corpus.expected_case_result(case);
                let entries: usize = expected.values().map(BTreeSet::len).sum();
                assert!(entries > 0, "case {case:?} is vacuous at count {count}");
                assert!(
                    entries < DIFFERENTIAL_LIMIT,
                    "case {case:?} has {entries} entries at count {count} — not below the limit"
                );
            }
        }
    }

    /// At least one case must be a strict subset of its service's
    /// records (a gate where every survivor set is the whole stream
    /// proves no filtering).
    #[test]
    fn case_results_actually_filter() {
        let corpus = generate(&spec(60));
        let json_records = corpus
            .records
            .iter()
            .filter(|r| r.service == SVC_JSON)
            .count();
        let survivors: usize = corpus
            .expected_case_result("json_string_filter")
            .values()
            .map(BTreeSet::len)
            .sum();
        assert!(survivors > 0 && survivors < json_records);
    }

    #[test]
    fn line_format_case_rewrites_lines_and_label_format_case_renames() {
        let corpus = generate(&spec(60));
        for entries in corpus.expected_case_result("line_format_rewrite").values() {
            for (_, line) in entries {
                assert!(
                    !line.starts_with('{'),
                    "line_format output must not be the raw JSON body: {line}"
                );
            }
        }
        for labels in corpus.expected_case_result("label_format_rename").keys() {
            assert!(labels.contains_key("lvl") && !labels.contains_key("level"));
        }
    }

    #[test]
    fn export_request_groups_by_service_with_only_the_two_resource_attrs() {
        let corpus = generate(&spec(60));
        let req = to_otlp_export_request(&corpus);
        let resources = req["resourceLogs"].as_array().unwrap();
        assert_eq!(resources.len(), 3, "one resource group per service");
        for res in resources {
            let attrs = res["resource"]["attributes"].as_array().unwrap();
            let keys: Vec<&str> = attrs.iter().filter_map(|a| a["key"].as_str()).collect();
            assert_eq!(keys, vec!["service.name", RUN_ATTR]);
            // Scope deliberately absent: no otel_scope_name label on
            // either store.
            assert!(res["scopeLogs"][0].get("scope").is_none());
        }
    }
}
