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
//! `run_id` via its OTLP config). No severity/record attributes are
//! emitted. The regular corpus carries no scope; a single [`SVC_SCOPE`]
//! witness (issue #109) carries a collision-bearing `InstrumentationScope`
//! that BOTH stores route into per-entry structured metadata (Loki 3.4.2
//! parity, live-probe-pinned) — never indexed stream labels — so
//! stream-label-set equality stays mechanically valid and the flattened
//! structured-metadata output is asserted identical across stores.
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
    // Issue #99 streams error-detail cases (append-only): each errored
    // pipeline returns a stream carrying both __error__ and the byte-exact
    // __error_details__, compared set-equal against grafana/loki:3.4.2.
    "json_error_details",
    "json_error_kept_by_error_filter",
    "labelfilter_number_error_details",
    // Issue #100 fetch-until-limit ordered-limited case (append-only):
    // a heavily-dropping json + double numeric-filter pipeline whose
    // earliest-`limit` survivors span >= 2 keyset pages, compared as an
    // ORDERED prefix (not set-equal) at full tier. See `run_streams_limited_case`.
    "fetch_until_limit_paged",
    // Issue #109 scope-in-structured-metadata case (append-only): a
    // `| scope_name="…"` structured-metadata pipeline filter selects the
    // collision-bearing SVC_SCOPE witness; the flattened response (last-write-
    // wins-resolved SM) is set-equal PulsusDB==Loki, and the `{scope_name="…"}`
    // STREAM selector returns empty on both stores (scope is not indexed).
    "scope_structured_metadata",
];

/// The committed METRIC differential case ids (issue M6-10), in fixture
/// order — appended after [`CASE_IDS`] in
/// `test/fixtures/logs/differential.json` (the id-set lock covers the
/// concatenation of both lists).
pub const METRIC_CASE_IDS: &[&str] = &[
    "metric_filtered_count",
    "metric_unwrap_sum",
    "metric_vector_agg",
    "metric_binary_scalar",
    "metric_rate_tumbling",
    "metric_unwrap_error",
    // Issue #91 vector-matching modifiers (instant; gated).
    "metric_match_on",
    "metric_match_ignoring",
    "metric_match_group_left",
    "metric_match_group_right",
    // Issue #91 vector-matching modifiers (range; informational — the
    // same tumbling-vs-sliding window divergence as metric_rate_tumbling).
    "metric_match_on_range",
    "metric_match_ignoring_range",
    "metric_match_group_left_range",
    "metric_match_group_right_range",
    // Issue #91 matching runtime errors (both stores fail the query).
    "metric_match_multiple_err",
    "metric_match_duplicate_err",
];

pub const SVC_JSON: &str = "svc-json";
pub const SVC_LOGFMT: &str = "svc-logfmt";
pub const SVC_PLAIN: &str = "svc-plain";

/// The M6-10 D1 witness service: exactly ONE synthetic record whose
/// `took` value can never convert to a duration, isolated under its own
/// service so no other case's pipeline ever touches it. The
/// `metric_unwrap_error` case unwraps it and asserts BOTH stores fail
/// the query (HTTP 400, `SampleExtractionErr`) — the genuine
/// conversion-error differential obligation.
pub const SVC_BADUNIT: &str = "svc-badunit";

/// The witness record's body — `took=abc` fails `duration(took)`.
pub const BADUNIT_BODY: &str = "took=abc x=1";

/// Issue #99 streams error-detail witness services, each isolating a
/// single synthetic record whose pipeline errors a distinct stage, so no
/// regular case's projection ever touches them.
pub const SVC_BADJSON: &str = "svc-badjson";
/// A top-level non-object line — `| json` is a `JSONParserErr`.
pub const BADJSON_BODY: &str = "not a json line";
pub const SVC_BADNUM: &str = "svc-badnum";
/// `| logfmt | n > 5` fails the numeric conversion (`LabelFilterErr`).
pub const BADNUM_BODY: &str = "n=oops";

/// The issue #109 scope witness service: exactly ONE synthetic record
/// carrying a collision-bearing `InstrumentationScope`, isolated under its
/// own service so no other case touches it. BOTH stores route its scope
/// into per-entry structured metadata (never indexed labels), so the
/// `scope_structured_metadata` case can assert PulsusDB==Loki on the
/// last-write-wins-resolved SM output and prove placement via an empty
/// `{scope_name="…"}` stream selector.
pub const SVC_SCOPE: &str = "svc-scope";
/// The witness record's body (no scope signal — the scope is metadata).
pub const SCOPE_WITNESS_BODY: &str = "scope collision witness";
/// The witness scope name — non-empty, so it appears as `scope_name` and
/// (via last-write-wins identity precedence) overrides a colliding
/// `scope.name` attribute.
pub const SCOPE_WITNESS_NAME: &str = "coll-scope";
/// The witness scope version — non-empty, so it appears as `scope_version`.
pub const SCOPE_WITNESS_VERSION: &str = "1.0";
/// The witness scope attributes, in wire order — exercising Loki's
/// collision rules (live-probe-pinned): `dup.key`/`dup_key` both sanitize
/// to `dup_key` (last wins -> `v_us`), `scope.name` sanitizes onto the
/// identity key (dropped -> identity `coll-scope` wins), `emptyattr` is an
/// empty-valued attribute (kept verbatim).
pub const SCOPE_WITNESS_ATTRS: &[(&str, &str)] = &[
    ("dup.key", "v_dot"),
    ("dup_key", "v_us"),
    ("scope.name", "LOSE"),
    ("emptyattr", ""),
];

/// Sanitizes a label key the way `pulsus_model::canonicalize_label_key` /
/// `LabelSet::from_normalized` does (`[^a-zA-Z0-9_]` -> `_`) — the corpus's
/// own independent copy, never calling the crate under test.
fn sanitize_label_key(key: &str) -> String {
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

/// The witness scope's flattened structured-metadata labels, resolved by
/// Loki's last-write-wins rule over the ordered list
/// `[attributes in wire order …, scope_name, scope_version]` (issue #109):
/// `dup_key="v_us"` (last write), `scope_name="coll-scope"` (identity
/// precedence — `scope.name="LOSE"` dropped), `scope_version="1.0"`,
/// `emptyattr=""` (kept). The corpus's independent oracle, computed here
/// with its own loop — never by calling `pulsus-write`.
pub fn scope_witness_sm_labels() -> BTreeMap<String, String> {
    let mut ordered: Vec<(String, String)> = SCOPE_WITNESS_ATTRS
        .iter()
        .map(|(k, v)| (sanitize_label_key(k), (*v).to_string()))
        .collect();
    // Identity appended last (both non-empty) so it overrides a colliding
    // attribute.
    ordered.push(("scope_name".to_string(), SCOPE_WITNESS_NAME.to_string()));
    ordered.push((
        "scope_version".to_string(),
        SCOPE_WITNESS_VERSION.to_string(),
    ));
    let mut resolved: Vec<(String, String)> = Vec::with_capacity(ordered.len());
    for (key, value) in ordered {
        if let Some(slot) = resolved.iter_mut().find(|(k, _)| *k == key) {
            slot.1 = value;
        } else {
            resolved.push((key, value));
        }
    }
    resolved.into_iter().collect()
}

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

/// One case's earliest-`limit` ordered entries: `(labels, ts_ns, line)`
/// in ascending-ts order — the fetch-until-limit ordered comparison shape
/// (issue #100), distinct from [`ExpectedResult`]'s set-collapsed form.
pub type OrderedEntries = Vec<(BTreeMap<String, String>, i64, String)>;

/// An instant metric case's expected shape: series label set → value
/// (issue M6-10; values compared with a tight relative tolerance in
/// `logs.rs` — both stores compute the same f64 operations, the
/// tolerance only absorbs summation-order ulps).
pub type MetricVector = BTreeMap<BTreeMap<String, String>, f64>;

/// A range metric case's expected shape: series label set → bucket
/// timestamp (ns) → value.
pub type MetricMatrix = BTreeMap<BTreeMap<String, String>, BTreeMap<i64, f64>>;

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
/// `(spec, log_idx)` — byte-reproducible (unit-tested). One synthetic
/// [`SVC_BADUNIT`] witness record (issue M6-10 D1) is appended after the
/// regular records; its dedicated service keeps every other case's
/// projection untouched.
pub fn generate(spec: &LogCorpusSpec) -> LogCorpus {
    let mut records = Vec::with_capacity(spec.record_count + 4);
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
    // Witness records: one per synthetic-error service, feature fields
    // unused (each case's projection is service-guarded). `svc-badunit`
    // stays LAST so `last_ts_ns` (the metric-witness window margin) pins to
    // it, unchanged by the issue #99 additions appended before it.
    let witness = |offset: usize, service: &'static str, body: &str| GeneratedRecord {
        service,
        ts_ns: spec.base_ns + spec.step_ns * (spec.record_count + offset) as i64,
        body: body.to_string(),
        method: "GET",
        status: 0,
        req_path: "/",
        level: "info",
        took_ms: 0,
        size_kb: 0,
        msg_idx: spec.record_count + offset,
    };
    records.push(witness(0, SVC_BADJSON, BADJSON_BODY));
    records.push(witness(1, SVC_BADNUM, BADNUM_BODY));
    // Issue #109 scope witness — appended BEFORE `svc-badunit` so the latter
    // stays LAST and `last_ts_ns` keeps pinning to it. Its scope identity is
    // implied by `service == SVC_SCOPE` (see `to_otlp_export_request` /
    // `base_labels`), so it needs no extra `GeneratedRecord` fields.
    records.push(witness(2, SVC_SCOPE, SCOPE_WITNESS_BODY));
    records.push(witness(3, SVC_BADUNIT, BADUNIT_BODY));
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
        let mut labels = BTreeMap::from([
            ("service_name".to_string(), r.service.to_string()),
            (RUN_ATTR.to_string(), self.run_id.clone()),
        ]);
        // The scope witness (issue #109) carries per-entry structured metadata
        // that BOTH stores flatten into the response label set (the #97 SM
        // merge — no key here collides with `service_name`/`run_id`). Every
        // flattened response for this record — bare `{run_id}` completeness or a
        // `| scope_name=` pipeline — therefore carries these keys.
        if r.service == SVC_SCOPE {
            labels.extend(scope_witness_sm_labels());
        }
        labels
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

    /// The earliest-`limit` matching `(labels, ts_ns, line)` for one
    /// committed case, in ascending-ts order (issue #100). Records are
    /// generated in index order and `ts_ns = base_ns + step_ns · i` is
    /// injective in the record index (`generate`), so iterating in index
    /// order IS ascending-ts order and the first `limit` matches are the
    /// earliest `limit` by timestamp — a UNIQUE ordered prefix with no
    /// boundary tie (every timestamp is globally distinct). Used by the
    /// fetch-until-limit ordered comparison, which requires exactly
    /// `limit` entries on both stores.
    pub fn expected_ordered_limited(&self, case_id: &str, limit: u32) -> OrderedEntries {
        let mut out = Vec::new();
        for r in &self.records {
            let Some((extracted, line)) = case_projection(case_id, r) else {
                continue;
            };
            let mut labels = self.base_labels(r);
            labels.extend(extracted);
            out.push((labels, r.ts_ns, line));
            if out.len() == limit as usize {
                break;
            }
        }
        out
    }

    /// Which service a case's selector scopes to (from the case id's
    /// shape family) — used by `logs.rs` only for logging.
    pub fn total_records(&self) -> usize {
        self.records.len()
    }

    /// The by-construction expected VECTOR for one committed instant
    /// metric case (issue M6-10). Every value is derived from the typed
    /// feature fields with the same f64 operations the engine performs
    /// (duration ms → seconds is `ms * 1e-3`; sums accumulate in
    /// timestamp order).
    pub fn expected_metric_vector(&self, case_id: &str) -> MetricVector {
        let json_labels = |r: &GeneratedRecord| {
            let mut labels = self.base_labels(r);
            labels.insert("method".to_string(), r.method.to_string());
            labels.insert("status".to_string(), r.status.to_string());
            labels.insert("took_ms".to_string(), r.took_ms.to_string());
            labels.insert("req_path".to_string(), r.req_path.to_string());
            labels
        };
        let mut out = MetricVector::new();
        match case_id {
            "metric_filtered_count" => {
                for r in &self.records {
                    if r.service == SVC_JSON && r.status == 500 {
                        *out.entry(json_labels(r)).or_insert(0.0) += 1.0;
                    }
                }
            }
            "metric_binary_scalar" => {
                for (labels, v) in self.expected_metric_vector("metric_filtered_count") {
                    out.insert(labels, v * 2.0);
                }
            }
            "metric_unwrap_sum" => {
                // `| logfmt took | unwrap duration(took)`: the targeted
                // extraction adds only `took`, which the unwrap then
                // deletes — every record collapses onto the base label
                // set, one series, values summed in timestamp order.
                let mut sum = 0.0f64;
                let mut any = false;
                for r in &self.records {
                    if r.service == SVC_LOGFMT {
                        sum += r.took_ms as f64 * 1e-3;
                        any = true;
                    }
                }
                if any {
                    let base = BTreeMap::from([
                        ("service_name".to_string(), SVC_LOGFMT.to_string()),
                        (RUN_ATTR.to_string(), self.run_id.clone()),
                    ]);
                    out.insert(base, sum);
                }
            }
            "metric_vector_agg" => {
                // `sum by (level) (...)`: output labels are exactly
                // `level`.
                for r in &self.records {
                    if r.service == SVC_LOGFMT {
                        let labels = BTreeMap::from([("level".to_string(), r.level.to_string())]);
                        *out.entry(labels).or_insert(0.0) += 1.0;
                    }
                }
            }
            // Issue #91 vector-matching modifiers over svc-json counts.
            "metric_match_on" => {
                // one-to-one `on(method)`: total(method) / count200(method).
                let joined = join_by_construction(
                    MatchOp::Div,
                    true,
                    &["method"],
                    MatchG::OneToOne,
                    &svc_json_counts(&self.records, &["method"], None),
                    &svc_json_counts(&self.records, &["method"], Some(200)),
                );
                out.extend(joined);
            }
            "metric_match_ignoring" => {
                // one-to-one `ignoring(status)`: total(method) / count503(method).
                let joined = join_by_construction(
                    MatchOp::Div,
                    false,
                    &["status"],
                    MatchG::OneToOne,
                    &svc_json_counts(&self.records, &["method"], None),
                    &svc_json_counts(&self.records, &["method"], Some(503)),
                );
                out.extend(joined);
            }
            "metric_match_group_left" => {
                // many-to-one `on(status) group_left`: the many (lhs) side
                // {method,status} passes through whole; value =
                // count(method,status) / total(status).
                let joined = join_by_construction(
                    MatchOp::Div,
                    true,
                    &["status"],
                    MatchG::Left,
                    &svc_json_counts(&self.records, &["method", "status"], None),
                    &svc_json_counts(&self.records, &["status"], None),
                );
                out.extend(joined);
            }
            "metric_match_group_right" => {
                // one-to-many `on(status) group_right`: the many (rhs) side
                // {method,status} passes through whole; value =
                // total(status) * count(method,status).
                let joined = join_by_construction(
                    MatchOp::Mul,
                    true,
                    &["status"],
                    MatchG::Right,
                    &svc_json_counts(&self.records, &["status"], None),
                    &svc_json_counts(&self.records, &["method", "status"], None),
                );
                out.extend(joined);
            }
            other => panic!("expected_metric_vector: unknown case id {other:?}"),
        }
        out
    }

    /// The by-construction expected MATRIX for the range metric case
    /// (issue M6-10): PulsusDB's documented tumbling semantics —
    /// epoch-aligned `floor(ts/step)*step` buckets, `rate` = count/step
    /// seconds, non-empty buckets only.
    pub fn expected_metric_matrix(&self, case_id: &str, step_ns: i64) -> MetricMatrix {
        if case_id == "metric_rate_tumbling" {
            let mut buckets: BTreeMap<i64, f64> = BTreeMap::new();
            for r in &self.records {
                if r.service == SVC_JSON {
                    let bucket = r.ts_ns.div_euclid(step_ns) * step_ns;
                    *buckets.entry(bucket).or_insert(0.0) += 1.0;
                }
            }
            let step_seconds = step_ns as f64 / 1e9;
            let points: BTreeMap<i64, f64> = buckets
                .into_iter()
                .map(|(b, n)| (b, n / step_seconds))
                .collect();
            let mut out = MetricMatrix::new();
            if !points.is_empty() {
                let base = BTreeMap::from([
                    ("service_name".to_string(), SVC_JSON.to_string()),
                    (RUN_ATTR.to_string(), self.run_id.clone()),
                ]);
                out.insert(base, points);
            }
            return out;
        }

        // Issue #91 vector-matching modifiers, RANGE path: an INDEPENDENT
        // per-bucket instant join over epoch-aligned `count_over_time`
        // buckets (the same shape the shipped engine produces per step).
        // These match the four instant cases' queries, run as
        // range queries — the join is applied fresh per shared bucket.
        let (op, on, keys, group, lhs_spec, rhs_spec) = match case_id {
            "metric_match_on_range" => (
                MatchOp::Div,
                true,
                &["method"][..],
                MatchG::OneToOne,
                (&["method"][..], None),
                (&["method"][..], Some(200)),
            ),
            "metric_match_ignoring_range" => (
                MatchOp::Div,
                false,
                &["status"][..],
                MatchG::OneToOne,
                (&["method"][..], None),
                (&["method"][..], Some(503)),
            ),
            "metric_match_group_left_range" => (
                MatchOp::Div,
                true,
                &["status"][..],
                MatchG::Left,
                (&["method", "status"][..], None),
                (&["status"][..], None),
            ),
            "metric_match_group_right_range" => (
                MatchOp::Mul,
                true,
                &["status"][..],
                MatchG::Right,
                (&["status"][..], None),
                (&["method", "status"][..], None),
            ),
            other => panic!("expected_metric_matrix: unknown case id {other:?}"),
        };
        let lhs = svc_json_bucket_counts(&self.records, lhs_spec.0, lhs_spec.1, step_ns);
        let rhs = svc_json_bucket_counts(&self.records, rhs_spec.0, rhs_spec.1, step_ns);
        let mut buckets: BTreeSet<i64> = BTreeSet::new();
        buckets.extend(lhs.keys().copied());
        buckets.extend(rhs.keys().copied());
        let mut out = MetricMatrix::new();
        let empty = Vec::new();
        for b in buckets {
            let l = lhs.get(&b).unwrap_or(&empty);
            let r = rhs.get(&b).unwrap_or(&empty);
            for (labels, value) in join_by_construction(op, on, keys, group, l, r) {
                out.entry(labels).or_default().insert(b, value);
            }
        }
        out
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
// Issue #91: an ENGINE-INDEPENDENT by-construction oracle for the
// vector-matching join (the corpus never calls `pulsus-read` — the
// circularity breaker). Mirrors the shipped join semantics
// (`instant_join`) but computed from the typed feature fields.
// ---------------------------------------------------------------------

type Labels = BTreeMap<String, String>;

/// One matching arithmetic operator used by the #91 differential cases.
#[derive(Clone, Copy)]
pub enum MatchOp {
    Div,
    Mul,
}

/// The grouping side, mirroring the AST's `MatchGroup` (include lists are
/// empty for every committed #91 case).
#[derive(Clone, Copy)]
pub enum MatchG {
    OneToOne,
    Left,
    Right,
}

/// The reduced match signature: `on` keeps only the listed keys,
/// `ignoring` (on = false) drops them.
fn match_sig(labels: &Labels, on: bool, keys: &[&str]) -> Labels {
    labels
        .iter()
        .filter(|(k, _)| on == keys.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// The engine-independent instant join for one step (or one instant):
/// one-to-one output = the reduced signature; `group_left`/`group_right`
/// output = the many side's full labels (no include copies in the
/// committed cases). Panics on a duplicate one-side signature — the
/// committed success cases never trigger it (the error cases are asserted
/// live, not by-construction).
fn join_by_construction(
    op: MatchOp,
    on: bool,
    keys: &[&str],
    group: MatchG,
    lhs: &[(Labels, f64)],
    rhs: &[(Labels, f64)],
) -> Vec<(Labels, f64)> {
    if lhs.is_empty() || rhs.is_empty() {
        return Vec::new();
    }
    let (many, one, swapped) = match group {
        MatchG::OneToOne | MatchG::Left => (lhs, rhs, false),
        MatchG::Right => (rhs, lhs, true),
    };
    let one_to_one = matches!(group, MatchG::OneToOne);
    let mut one_by_sig: BTreeMap<Labels, f64> = BTreeMap::new();
    for (labels, v) in one {
        let sig = match_sig(labels, on, keys);
        assert!(
            one_by_sig.insert(sig, *v).is_none(),
            "by-construction join: duplicate one-side signature (a success case must be \
             genuinely 1:1 or many-to-one)"
        );
    }
    let mut out: Vec<(Labels, f64)> = Vec::new();
    let mut seen: BTreeSet<Labels> = BTreeSet::new();
    for (labels, mv) in many {
        let sig = match_sig(labels, on, keys);
        let Some(ov) = one_by_sig.get(&sig) else {
            continue;
        };
        let (l, r) = if swapped { (*ov, *mv) } else { (*mv, *ov) };
        let value = match op {
            MatchOp::Div => l / r,
            MatchOp::Mul => l * r,
        };
        let out_labels = if one_to_one { sig } else { labels.clone() };
        if one_to_one {
            assert!(
                seen.insert(out_labels.clone()),
                "by-construction join: one-to-one signature matched twice"
            );
        }
        out.push((out_labels, value));
    }
    out
}

/// Counts svc-json records grouped by the requested label keys (subset of
/// `{method, status}`), optionally filtered to a single status — the
/// instant `count_over_time` value per group.
fn svc_json_counts(
    records: &[GeneratedRecord],
    group: &[&str],
    status: Option<i64>,
) -> Vec<(Labels, f64)> {
    let mut acc: BTreeMap<Labels, f64> = BTreeMap::new();
    for r in records {
        if r.service != SVC_JSON {
            continue;
        }
        if let Some(s) = status
            && r.status != s
        {
            continue;
        }
        let mut labels = Labels::new();
        for k in group {
            match *k {
                "method" => {
                    labels.insert("method".to_string(), r.method.to_string());
                }
                "status" => {
                    labels.insert("status".to_string(), r.status.to_string());
                }
                other => panic!("svc_json_counts: unsupported group key {other:?}"),
            }
        }
        *acc.entry(labels).or_insert(0.0) += 1.0;
    }
    acc.into_iter().collect()
}

/// The per-tumbling-bucket variant of [`svc_json_counts`] — the range
/// `count_over_time` value per group per epoch-aligned bucket.
fn svc_json_bucket_counts(
    records: &[GeneratedRecord],
    group: &[&str],
    status: Option<i64>,
    step_ns: i64,
) -> BTreeMap<i64, Vec<(Labels, f64)>> {
    let mut per_bucket: BTreeMap<i64, BTreeMap<Labels, f64>> = BTreeMap::new();
    for r in records {
        if r.service != SVC_JSON {
            continue;
        }
        if let Some(s) = status
            && r.status != s
        {
            continue;
        }
        let bucket = r.ts_ns.div_euclid(step_ns) * step_ns;
        let mut labels = Labels::new();
        for k in group {
            match *k {
                "method" => {
                    labels.insert("method".to_string(), r.method.to_string());
                }
                "status" => {
                    labels.insert("status".to_string(), r.status.to_string());
                }
                other => panic!("svc_json_bucket_counts: unsupported group key {other:?}"),
            }
        }
        *per_bucket
            .entry(bucket)
            .or_default()
            .entry(labels)
            .or_insert(0.0) += 1.0;
    }
    per_bucket
        .into_iter()
        .map(|(b, m)| (b, m.into_iter().collect()))
        .collect()
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
        // Issue #100: `| json | status = "503" | took_ms = "500"` — two
        // dropping string label filters after `| json`. Matches svc-json
        // records at `j%4==1` (status 503) AND `j%5==4` (took_ms 500),
        // i.e. `j ≡ 9 (mod 20)` (j = record_index/3). No extra labels vs
        // the json parser (both filtered keys are already json labels).
        "fetch_until_limit_paged" => (r.service == SVC_JSON && r.status == 503 && r.took_ms == 500)
            .then(|| (json_labels(r), r.body.clone())),
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
        // Issue #99: a `| json` over a non-object line errors — no
        // extracted labels, just the error class + its byte-exact detail
        // (the `!= ""` variant keeps the same errored stream). Both
        // grafana/loki:3.4.2 and PulsusDB produce this pair.
        "json_error_details" | "json_error_kept_by_error_filter" => (r.service == SVC_BADJSON)
            .then(|| {
                let labels = BTreeMap::from([
                    ("__error__".to_string(), "JSONParserErr".to_string()),
                    (
                        "__error_details__".to_string(),
                        "Value looks like object, but can't find closing '}' symbol".to_string(),
                    ),
                ]);
                (labels, r.body.clone())
            }),
        // Issue #99: `| logfmt | n > 5` fails the numeric conversion —
        // logfmt still extracts `n`, then LabelFilterErr + the Go
        // strconv.ParseFloat detail (value verbatim).
        "labelfilter_number_error_details" => (r.service == SVC_BADNUM).then(|| {
            let labels = BTreeMap::from([
                ("n".to_string(), "oops".to_string()),
                ("__error__".to_string(), "LabelFilterErr".to_string()),
                (
                    "__error_details__".to_string(),
                    r#"strconv.ParseFloat: parsing "oops": invalid syntax"#.to_string(),
                ),
            ]);
            (labels, r.body.clone())
        }),
        // Issue #109: `{run_id="R"} | scope_name="coll-scope"` selects the
        // scope witness by its structured-metadata `scope_name` label. The
        // pipeline extracts no NEW labels (the SM keys are already in
        // `base_labels` for this record via the #97 flatten), so the extracted
        // map is empty; the response entry is the witness body under its full
        // flattened label set.
        "scope_structured_metadata" => {
            (r.service == SVC_SCOPE).then(|| (BTreeMap::new(), r.body.clone()))
        }
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
            let mut scope_logs = serde_json::json!({ "logRecords": log_records });
            // Issue #109: the scope witness group carries a collision-bearing
            // `InstrumentationScope`; every other group omits scope entirely.
            if service == SVC_SCOPE {
                let attributes: Vec<serde_json::Value> = SCOPE_WITNESS_ATTRS
                    .iter()
                    .map(|(key, value)| {
                        serde_json::json!({ "key": key, "value": { "stringValue": value } })
                    })
                    .collect();
                scope_logs["scope"] = serde_json::json!({
                    "name": SCOPE_WITNESS_NAME,
                    "version": SCOPE_WITNESS_VERSION,
                    "attributes": attributes,
                });
            }
            serde_json::json!({
                "resource": { "attributes": [
                    { "key": "service.name", "value": { "stringValue": service } },
                    { "key": RUN_ATTR, "value": { "stringValue": c.run_id } },
                ]},
                "scopeLogs": [scope_logs],
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
        // Issue #100: independent body-text re-derivation of the double
        // string filter (status 503 AND took_ms 500).
        "fetch_until_limit_paged" => {
            r.service == SVC_JSON
                && body_json(r).is_some_and(|v| {
                    v["status"].as_i64() == Some(503) && v["took_ms"].as_i64() == Some(500)
                })
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
        // Issue #99: the errored-line membership (the detail STRING is the
        // projection's, not tested here — this only re-derives survival).
        "json_error_details" | "json_error_kept_by_error_filter" => {
            r.service == SVC_BADJSON && body_json(r).is_none()
        }
        "labelfilter_number_error_details" => {
            r.service == SVC_BADNUM
                && body_logfmt(r)
                    .get("n")
                    .is_some_and(|v| v.parse::<f64>().is_err())
        }
        // Issue #109: the scope witness carries no body signal (scope is
        // metadata), so the independent oracle re-derives survival purely from
        // the service isolation — every SVC_SCOPE record has `scope_name` =
        // `coll-scope` by construction, so `| scope_name="coll-scope"` selects
        // exactly it.
        "scope_structured_metadata" => r.service == SVC_SCOPE,
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

    /// The M6-10 D1 witness: exactly one `svc-badunit` record, carrying
    /// the never-convertible body, isolated from every other service's
    /// projections.
    #[test]
    fn the_badunit_witness_record_is_appended_exactly_once() {
        let corpus = generate(&spec(60));
        let witnesses: Vec<&GeneratedRecord> = corpus
            .records
            .iter()
            .filter(|r| r.service == SVC_BADUNIT)
            .collect();
        assert_eq!(witnesses.len(), 1);
        assert_eq!(witnesses[0].body, BADUNIT_BODY);
        assert_eq!(corpus.last_ts_ns, witnesses[0].ts_ns);
        // No committed streams case projects it.
        for case in CASE_IDS {
            assert!(
                case_projection(case, witnesses[0]).is_none(),
                "case {case:?} must not project the witness record"
            );
        }
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
        assert_eq!(
            resources.len(),
            7,
            "one resource group per service (svc-json/logfmt/plain + the \
             issue #99 svc-badjson/svc-badnum, the issue #109 svc-scope, and the \
             M6-10 svc-badunit witnesses)"
        );
        for res in resources {
            let attrs = res["resource"]["attributes"].as_array().unwrap();
            let keys: Vec<&str> = attrs.iter().filter_map(|a| a["key"].as_str()).collect();
            // Resource attributes are still exactly the two isolation labels on
            // every group — scope is a separate `scopeLogs.scope` object.
            assert_eq!(keys, vec!["service.name", RUN_ATTR]);
            let service = attrs[0]["value"]["stringValue"].as_str().unwrap();
            let scope = res["scopeLogs"][0].get("scope");
            if service == SVC_SCOPE {
                // The scope witness group carries the collision-bearing scope.
                let scope = scope.expect("svc-scope group carries a scope");
                assert_eq!(scope["name"], SCOPE_WITNESS_NAME);
                assert_eq!(scope["version"], SCOPE_WITNESS_VERSION);
                let attrs = scope["attributes"].as_array().unwrap();
                assert_eq!(attrs.len(), SCOPE_WITNESS_ATTRS.len());
            } else {
                // Every other group omits scope (routes to no SM on either store).
                assert!(scope.is_none());
            }
        }
    }

    /// The issue #109 scope witness: exactly one `svc-scope` record whose
    /// collision-bearing scope resolves (last-write-wins) to the pinned
    /// structured-metadata label set on both stores, and which no OTHER
    /// committed case projects.
    #[test]
    fn the_scope_witness_record_resolves_its_collisions_and_is_isolated() {
        let corpus = generate(&spec(60));
        let witnesses: Vec<&GeneratedRecord> = corpus
            .records
            .iter()
            .filter(|r| r.service == SVC_SCOPE)
            .collect();
        assert_eq!(witnesses.len(), 1);
        assert_eq!(witnesses[0].body, SCOPE_WITNESS_BODY);
        // svc-badunit is still LAST (the scope witness was inserted before it).
        assert_eq!(corpus.records.last().unwrap().service, SVC_BADUNIT);

        // Last-write-wins collision resolution, live-Loki-3.4.2-pinned:
        // dup_key=v_us (last write), scope_name=coll-scope (identity beats the
        // colliding scope.name=LOSE), scope_version=1.0, emptyattr="" (kept).
        let sm = scope_witness_sm_labels();
        assert_eq!(
            sm,
            BTreeMap::from([
                ("dup_key".to_string(), "v_us".to_string()),
                ("emptyattr".to_string(), String::new()),
                ("scope_name".to_string(), "coll-scope".to_string()),
                ("scope_version".to_string(), "1.0".to_string()),
            ])
        );

        // The witness's completeness labels include the resolved SM.
        let expected = corpus.expected_case_result("scope_structured_metadata");
        assert_eq!(expected.len(), 1, "one flattened stream for the witness");
        let (labels, entries) = expected.iter().next().unwrap();
        assert_eq!(
            labels.get("scope_name").map(String::as_str),
            Some("coll-scope")
        );
        assert_eq!(labels.get("dup_key").map(String::as_str), Some("v_us"));
        assert_eq!(labels.get("emptyattr").map(String::as_str), Some(""));
        assert!(!labels.contains_key("scope.name"));
        assert_eq!(entries.len(), 1);

        // No other committed case projects the scope witness.
        for case in CASE_IDS
            .iter()
            .filter(|c| **c != "scope_structured_metadata")
        {
            assert!(
                case_projection(case, witnesses[0]).is_none(),
                "case {case:?} must not project the scope witness record"
            );
        }
    }
}
